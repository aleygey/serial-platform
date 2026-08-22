//! `seriald` runtime library.

pub mod api;
pub mod config;
pub mod control;
pub mod journal;
pub mod monitor;
pub mod registry;
pub mod ring;
pub mod runtime;
pub mod slot;

use crate::api::AppState;
use crate::config::{ConfigStore, LoadedConfig};
use crate::journal::{JournalConfig, JournalManager};
use crate::registry::SlotRegistry;
use crate::runtime::{ActiveEndpoint, ActiveInstance};
use anyhow::Context as _;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::AsyncReadExt as _;

pub async fn serve(
    store: ConfigStore,
    loaded: LoadedConfig,
    bind_override: Option<SocketAddr>,
    managed: bool,
    mut instance: ActiveInstance,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let bind = bind_override.unwrap_or(loaded.config.bind);
    let server_id = loaded.config.server_id;
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind seriald to {bind}"))?;
    let actual_bind = listener
        .local_addr()
        .context("read the bound seriald address")?;
    let mut journal_config = JournalConfig::new(loaded.paths.journal_dir.clone());
    journal_config.max_total_bytes = loaded.config.logging.max_total_bytes;
    journal_config.cleanup_low_watermark =
        f64::from(loaded.config.logging.retention_target_percent) / 100.0;
    journal_config.max_segment_bytes = loaded.config.logging.segment_max_bytes;
    journal_config.max_segment_age = Duration::from_secs(60 * 60);
    let journal = JournalManager::open(journal_config).context("open serial journal")?;
    let registry = SlotRegistry::new(
        loaded.daemon_epoch,
        started,
        journal.handle(),
        loaded.config.ports.clone(),
        loaded.config.transport_profiles.clone(),
        loaded.config.model_profiles.clone(),
        loaded.config.control.limits(),
    );
    let state = AppState::try_new(
        store,
        loaded.config,
        registry,
        journal.handle(),
        loaded.daemon_epoch,
        started,
    )
    .context("open durable Monitor state")?;
    let endpoint = ActiveEndpoint::new(
        actual_bind,
        server_id,
        loaded.daemon_epoch,
        std::process::id(),
    );
    instance
        .publish(&endpoint)
        .context("publish the active seriald endpoint")?;
    tracing::info!(requested_bind = %bind, actual_bind = %actual_bind, endpoint = %endpoint.endpoint, epoch = %loaded.daemon_epoch, "seriald listening");
    let server = axum::serve(listener, api::router(state.clone()))
        .with_graceful_shutdown(shutdown_signal(managed));
    let result = server.await.context("seriald HTTP server failed");
    state.shutdown().await;
    journal.shutdown().await.context("close serial journal")?;
    instance.clear_marker();
    tracing::info!("seriald shutdown complete");
    result
}

async fn shutdown_signal(managed: bool) {
    if managed {
        let mut stdin = tokio::io::stdin();
        let mut buffer = [0_u8; 256];
        loop {
            match stdin.read(&mut buffer).await {
                Ok(0) => return,
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(%error, "failed to read the managed shutdown pipe");
                    return;
                }
            }
        }
    } else if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install Ctrl-C handler");
    }
}
