//! `seriald` runtime library.

pub mod api;
pub mod auth;
pub mod config;
pub mod control;
pub mod journal;
pub mod registry;
pub mod ring;
pub mod slot;

use crate::api::AppState;
use crate::config::{ConfigStore, LoadedConfig};
use crate::journal::{JournalConfig, JournalManager};
use crate::registry::SlotRegistry;
use anyhow::Context as _;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

pub async fn serve(
    store: ConfigStore,
    loaded: LoadedConfig,
    bind_override: Option<SocketAddr>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let bind = bind_override.unwrap_or(loaded.config.bind);
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
        loaded.config.slots.clone(),
    );
    let state = AppState::new(
        store,
        loaded.config,
        registry,
        journal.handle(),
        loaded.daemon_epoch,
        started,
    );
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("bind seriald to {bind}"))?;
    tracing::info!(%bind, epoch = %loaded.daemon_epoch, "seriald listening");
    let server =
        axum::serve(listener, api::router(state.clone())).with_graceful_shutdown(shutdown_signal());
    let result = server.await.context("seriald HTTP server failed");
    state.shutdown().await;
    journal.shutdown().await.context("close serial journal")?;
    result
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "failed to install Ctrl-C handler");
    }
}
