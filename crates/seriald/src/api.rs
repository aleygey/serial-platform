use crate::config::{ConfigError, ConfigStore, DaemonConfig};
use crate::journal::{JournalError, JournalHandle};
use crate::monitor::{MonitorError, MonitorManager};
use crate::registry::{RegistryError, RegistryRollbackError, SlotRegistry};
use crate::slot::{AttachState, SlotError, SlotHandle};
use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    Actor, ActorKind, ArchiveListResponse, ClientMessage, CommandResult,
    ConfigureModelProfilesRequest, ConfigureModelProfilesResponse, ConfigurePortsRequest,
    ConfigurePortsResponse, ConfigureTransportProfilesRequest, ConfigureTransportProfilesResponse,
    CreateMonitorRequest, Cursor, DaemonDiagnosticsResponse, ErrorCode, EventQuery,
    EventQueryResponse, GapRange, HealthResponse, ModelProfileListResponse,
    MonitorIncidentListResponse, MonitorIncidentResponse, MonitorListResponse, MonitorResponse,
    MonitorStatus, PROTOCOL_VERSION, PortDescriptor, ServerMessage, SlotDiagnostics,
    StatusResponse, StorageDiagnosticsResponse, TransportProfileListResponse, UpdateMonitorRequest,
    encode_control, encode_event,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore, broadcast, mpsc};
use tokio::task::JoinHandle;
use uuid::Uuid;

const OUTBOUND_QUEUE: usize = 512;
const MAX_WS_INCOMING_BYTES: usize = 64 * 1024;
const MAX_WS_CONNECTIONS: usize = 256;

#[derive(Clone)]
pub struct AppState {
    inner: Arc<AppStateInner>,
}

struct AppStateInner {
    config_store: ConfigStore,
    config: RwLock<DaemonConfig>,
    config_updates: Mutex<()>,
    registry: SlotRegistry,
    journal: JournalHandle,
    monitors: MonitorManager,
    daemon_epoch: Uuid,
    started: Instant,
    ws_connections: Arc<Semaphore>,
}

impl AppState {
    pub fn new(
        config_store: ConfigStore,
        config: DaemonConfig,
        registry: SlotRegistry,
        journal: JournalHandle,
        daemon_epoch: Uuid,
        started: Instant,
    ) -> Self {
        Self::try_new(
            config_store,
            config,
            registry,
            journal,
            daemon_epoch,
            started,
        )
        .expect("AppState requires valid Monitor storage")
    }

    pub fn try_new(
        config_store: ConfigStore,
        config: DaemonConfig,
        registry: SlotRegistry,
        journal: JournalHandle,
        daemon_epoch: Uuid,
        started: Instant,
    ) -> Result<Self, MonitorError> {
        let monitors = MonitorManager::open(
            config_store.paths().monitor_state_file.clone(),
            registry.clone(),
            daemon_epoch,
            config.server_id,
        )?;
        Ok(Self {
            inner: Arc::new(AppStateInner {
                config_store,
                config: RwLock::new(config),
                config_updates: Mutex::new(()),
                registry,
                journal,
                monitors,
                daemon_epoch,
                started,
                ws_connections: Arc::new(Semaphore::new(MAX_WS_CONNECTIONS)),
            }),
        })
    }

    pub async fn shutdown(&self) {
        let _update = self.inner.config_updates.lock().await;
        self.inner.monitors.shutdown().await;
        self.inner.registry.shutdown().await;
    }

    async fn configure_ports_transaction(
        &self,
        requested: Vec<serial_protocol::SlotConfig>,
        source: String,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<serial_protocol::SlotSnapshot>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_ports(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .apply_replacement_with_source(
                staged.ports.clone(),
                staged.transport_profiles.clone(),
                staged.model_profiles.clone(),
                source,
            )
            .await?;

        match self.inner.config_store.save(&staged) {
            Ok(()) => {
                let snapshots = match applied.commit().await {
                    Ok(snapshots) => snapshots,
                    Err(commit) => {
                        return Err(compensate_commit_failure(
                            &self.inner.config_store,
                            &current,
                            commit,
                        ));
                    }
                };
                let revision = staged.config_revision;
                *self.inner.config.write().await = staged;
                Ok((snapshots, revision))
            }
            Err(save) => match applied.rollback().await {
                Ok(()) => Err(ApiError::Config(save)),
                Err(rollback) => Err(ApiError::ConfigRollback { save, rollback }),
            },
        }
    }

    /// Validates and stages every affected actor before persistence. Staging
    /// is inert, so an unavailable actor or save failure can roll back without
    /// publishing a mixed runtime catalog or changing the in-memory config.
    async fn configure_model_profiles_transaction(
        &self,
        requested: Vec<serial_protocol::ModelProfile>,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<serial_protocol::ModelProfile>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_model_profiles(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .stage_model_profiles(staged.model_profiles.clone())
            .await?;

        match self.inner.config_store.save(&staged) {
            Ok(()) => {
                if let Err(commit) = applied.commit().await {
                    return Err(compensate_commit_failure(
                        &self.inner.config_store,
                        &current,
                        commit,
                    ));
                }
                let revision = staged.config_revision;
                *self.inner.config.write().await = staged.clone();
                Ok((staged.model_profiles, revision))
            }
            Err(save) => match applied.rollback().await {
                Ok(()) => Err(ApiError::Config(save)),
                Err(rollback) => Err(ApiError::ConfigRollback { save, rollback }),
            },
        }
    }

    async fn configure_transport_profiles_transaction(
        &self,
        requested: Vec<serial_protocol::TransportProfile>,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<serial_protocol::TransportProfile>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_transport_profiles(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .apply_replacement_with_source(
                staged.ports.clone(),
                staged.transport_profiles.clone(),
                staged.model_profiles.clone(),
                "system:transport-profile".to_owned(),
            )
            .await?;

        match self.inner.config_store.save(&staged) {
            Ok(()) => {
                if let Err(commit) = applied.commit().await {
                    return Err(compensate_commit_failure(
                        &self.inner.config_store,
                        &current,
                        commit,
                    ));
                }
                let revision = staged.config_revision;
                *self.inner.config.write().await = staged.clone();
                Ok((staged.transport_profiles, revision))
            }
            Err(save) => match applied.rollback().await {
                Ok(()) => Err(ApiError::Config(save)),
                Err(rollback) => Err(ApiError::ConfigRollback { save, rollback }),
            },
        }
    }
}

fn ensure_expected_revision(expected: Option<u64>, actual: u64) -> Result<(), ApiError> {
    if let Some(expected) = expected
        && expected != actual
    {
        return Err(ApiError::ConfigRevisionMismatch { expected, actual });
    }
    Ok(())
}

fn compensate_commit_failure(
    store: &ConfigStore,
    previous: &DaemonConfig,
    commit: RegistryError,
) -> ApiError {
    match store.save(previous) {
        Ok(()) => ApiError::Registry(commit),
        Err(restore) => ApiError::ConfigCommitRestore {
            commit: Box::new(commit),
            restore,
        },
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/status", get(status))
        .route("/api/v1/ports", get(ports))
        .route("/api/v1/config/ports", put(configure_ports))
        .route(
            "/api/v1/config/transport-profiles",
            get(list_transport_profiles).put(configure_transport_profiles),
        )
        .route(
            "/api/v1/config/model-profiles",
            get(list_model_profiles).put(configure_model_profiles),
        )
        .route("/api/v1/archives", get(archives))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/diagnostics/storage", get(storage_diagnostics))
        .route("/api/v1/ports/{port}/diagnostics", get(port_diagnostics))
        .route("/api/v1/ports/{port}/tail", get(live_tail))
        .route("/api/v1/ports/{port}/recent-activity", get(recent_activity))
        .route("/api/v1/ports/{port}/events", get(events))
        .route("/api/v1/monitors", get(list_monitors).post(create_monitor))
        .route(
            "/api/v1/monitors/{monitor_id}",
            get(get_monitor).put(update_monitor).delete(stop_monitor),
        )
        .route(
            "/api/v1/monitors/{monitor_id}/incidents",
            get(list_monitor_incidents),
        )
        .route(
            "/api/v1/monitors/{monitor_id}/incidents/{incident_id}/ack",
            post(acknowledge_monitor_incident),
        )
        .route("/api/v1/ws", get(websocket))
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let config = state.inner.config.read().await;
    Ok(Json(HealthResponse {
        status: "ok".into(),
        server_id: config.server_id,
        daemon_epoch: state.inner.daemon_epoch,
        uptime_ms: state
            .inner
            .started
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        protocol_version: PROTOCOL_VERSION,
    }))
}

async fn status(State(state): State<AppState>) -> Result<Json<StatusResponse>, ApiError> {
    let config = state.inner.config.read().await;
    Ok(Json(StatusResponse {
        server_id: config.server_id,
        daemon_epoch: state.inner.daemon_epoch,
        protocol_version: PROTOCOL_VERSION,
        config_revision: config.config_revision,
        sequence_write_precondition_supported: true,
        serial_context_precondition_supported: true,
        ports: state.inner.registry.snapshots().await,
    }))
}

async fn ports() -> Result<Json<Vec<PortDescriptor>>, ApiError> {
    let ports = tokio::task::spawn_blocking(serialport::available_ports)
        .await
        .map_err(|_| ApiError::Internal("serial enumeration task failed".into()))?
        .map_err(|error| ApiError::Internal(format!("serial enumeration failed: {error}")))?;
    Ok(Json(
        ports
            .into_iter()
            .map(|port| {
                let (port_type, manufacturer, product, serial_number) = match port.port_type {
                    serialport::SerialPortType::UsbPort(info) => (
                        "usb".to_owned(),
                        info.manufacturer,
                        info.product,
                        info.serial_number,
                    ),
                    serialport::SerialPortType::BluetoothPort => {
                        ("bluetooth".to_owned(), None, None, None)
                    }
                    serialport::SerialPortType::PciPort => ("pci".to_owned(), None, None, None),
                    serialport::SerialPortType::Unknown => ("unknown".to_owned(), None, None, None),
                };
                PortDescriptor {
                    name: port.port_name,
                    port_type,
                    manufacturer,
                    product,
                    serial_number,
                }
            })
            .collect(),
    ))
}

async fn configure_ports(
    State(state): State<AppState>,
    Json(request): Json<ConfigurePortsRequest>,
) -> Result<Json<ConfigurePortsResponse>, ApiError> {
    validate_source(&request.source)?;
    // Keep the transaction alive even if the HTTP request is cancelled after
    // physical actors were staged. The spawned task must either commit all
    // three views or run the compensating rollback.
    let transaction = state.clone();
    let (ports, config_revision) = tokio::spawn(async move {
        transaction
            .configure_ports_transaction(request.ports, request.source, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigurePortsResponse {
        ports,
        config_revision,
    }))
}

fn validate_source(source: &str) -> Result<(), ApiError> {
    if source.is_empty()
        || source.len() > 128
        || source != source.trim()
        || source.chars().any(char::is_control)
    {
        return Err(ApiError::BadRequest(
            "source must contain 1-128 trimmed, non-control UTF-8 bytes".into(),
        ));
    }
    Ok(())
}

async fn list_transport_profiles(
    State(state): State<AppState>,
) -> Result<Json<TransportProfileListResponse>, ApiError> {
    let config = state.inner.config.read().await;
    Ok(Json(TransportProfileListResponse {
        profiles: config.transport_profiles.clone(),
        config_revision: config.config_revision,
    }))
}

async fn configure_transport_profiles(
    State(state): State<AppState>,
    Json(request): Json<ConfigureTransportProfilesRequest>,
) -> Result<Json<ConfigureTransportProfilesResponse>, ApiError> {
    let transaction = state.clone();
    let (profiles, config_revision) = tokio::spawn(async move {
        transaction
            .configure_transport_profiles_transaction(request.profiles, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureTransportProfilesResponse {
        profiles,
        config_revision,
    }))
}

async fn list_model_profiles(
    State(state): State<AppState>,
) -> Result<Json<ModelProfileListResponse>, ApiError> {
    let config = state.inner.config.read().await;
    Ok(Json(ModelProfileListResponse {
        profiles: config.model_profiles.clone(),
        config_revision: config.config_revision,
    }))
}

async fn configure_model_profiles(
    State(state): State<AppState>,
    Json(request): Json<ConfigureModelProfilesRequest>,
) -> Result<Json<ConfigureModelProfilesResponse>, ApiError> {
    // Mirror the port transaction: the spawned task completes the validate /
    // persist / publish sequence even if the HTTP request is cancelled.
    let transaction = state.clone();
    let (profiles, config_revision) = tokio::spawn(async move {
        transaction
            .configure_model_profiles_transaction(request.profiles, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureModelProfilesResponse {
        profiles,
        config_revision,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct ArchiveListQuery {
    port: Option<String>,
}

async fn archives(
    State(state): State<AppState>,
    Query(query): Query<ArchiveListQuery>,
) -> Result<Json<ArchiveListResponse>, ApiError> {
    Ok(Json(state.inner.journal.list_archives(query.port).await?))
}

async fn diagnostics(
    State(state): State<AppState>,
) -> Result<Json<DaemonDiagnosticsResponse>, ApiError> {
    let config = state.inner.config.read().await.clone();
    let handles = state.inner.registry.handles().await;
    let ports = handles
        .into_iter()
        .map(|handle| SlotDiagnostics {
            snapshot: handle.snapshot(),
            subscriber_count: handle.subscriber_count(),
            subscriber_lag_events: handle.subscriber_lag_events(),
        })
        .collect::<Vec<_>>();
    let mut journal = state.inner.journal.diagnostics().await?;
    if ports
        .iter()
        .any(|slot| slot.snapshot.logging == serial_protocol::LoggingState::Degraded)
    {
        journal.logging = serial_protocol::LoggingState::Degraded;
    }
    Ok(Json(DaemonDiagnosticsResponse {
        server_id: config.server_id,
        daemon_epoch: state.inner.daemon_epoch,
        uptime_ms: state
            .inner
            .started
            .elapsed()
            .as_millis()
            .min(u64::MAX as u128) as u64,
        config_revision: config.config_revision,
        websocket_connections: MAX_WS_CONNECTIONS
            .saturating_sub(state.inner.ws_connections.available_permits()),
        websocket_limit: MAX_WS_CONNECTIONS,
        journal,
        ports,
    }))
}

async fn storage_diagnostics(
    State(state): State<AppState>,
) -> Result<Json<StorageDiagnosticsResponse>, ApiError> {
    let mut journal = state.inner.journal.diagnostics().await?;
    if state
        .inner
        .registry
        .snapshots()
        .await
        .iter()
        .any(|slot| slot.logging == serial_protocol::LoggingState::Degraded)
    {
        journal.logging = serial_protocol::LoggingState::Degraded;
    }
    Ok(Json(StorageDiagnosticsResponse { journal }))
}

async fn port_diagnostics(
    State(state): State<AppState>,
    Path(port): Path<String>,
) -> Result<Json<SlotDiagnostics>, ApiError> {
    let handle = state
        .inner
        .registry
        .get(&port)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("unknown port {port}")))?;
    Ok(Json(SlotDiagnostics {
        snapshot: handle.snapshot(),
        subscriber_count: handle.subscriber_count(),
        subscriber_lag_events: handle.subscriber_lag_events(),
    }))
}

async fn events(
    State(state): State<AppState>,
    Path(port): Path<String>,
    Query(mut query): Query<EventQuery>,
) -> Result<Json<EventQueryResponse>, ApiError> {
    // Normal history reads are scoped to this daemon run so an omitted epoch
    // can never surface a matching log from an earlier test cycle. Archived
    // history remains available by explicitly supplying its epoch.
    query.epoch.get_or_insert(state.inner.daemon_epoch);
    Ok(Json(state.inner.journal.query(port, query).await?))
}

#[derive(Debug, serde::Deserialize)]
struct LiveTailQuery {
    tail_events: Option<usize>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
}

/// Returns a bounded snapshot from the port's in-memory replay ring. This is
/// the low-latency current-tail path; unlike `/events`, its work is independent
/// of retained journal epochs and the size of the active `.open` segment.
async fn live_tail(
    State(state): State<AppState>,
    Path(port): Path<String>,
    Query(query): Query<LiveTailQuery>,
) -> Result<Json<EventQueryResponse>, ApiError> {
    let cursor = match (query.epoch, query.after_seq) {
        (Some(epoch), Some(after_seq)) => Some(Cursor { epoch, after_seq }),
        (None, None) => None,
        _ => {
            return Err(ApiError::BadRequest(
                "live tail cursor requires both epoch and after_seq".into(),
            ));
        }
    };
    let tail_events = query.tail_events.unwrap_or(200).clamp(1, 2_000);
    let handle = state
        .inner
        .registry
        .get(&port)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("unknown port {port}")))?;
    // Cursor pagination must begin at the oldest still-retained event after
    // the cursor. If EventRing received only `tail_events` here, RingEvicted
    // recovery would select the newest N events and silently skip an older,
    // still-retained portion. The ring is independently bounded to 20k
    // events / 4 MiB; clone that bounded window and page it below.
    let replay_events = if cursor.is_some() {
        usize::MAX
    } else {
        tail_events
    };
    let attach = handle.attach(cursor.as_ref(), replay_events).await?;
    Ok(Json(bounded_live_response(
        attach,
        cursor.as_ref(),
        tail_events,
    )))
}

fn bounded_live_response(
    attach: AttachState,
    cursor: Option<&Cursor>,
    tail_events: usize,
) -> EventQueryResponse {
    let mut events = attach.replay.events;
    let window_limited = cursor.is_some() && events.len() > tail_events;
    if window_limited {
        events.truncate(tail_events);
    }
    let next_after_seq = if window_limited {
        events
            .last()
            .map(|event| event.seq)
            .unwrap_or(attach.snapshot.head_seq)
    } else {
        attach.snapshot.head_seq
    };
    let gaps = attach
        .replay
        .gap
        .as_ref()
        .map(|gap| {
            let first_seq = gap.requested_after_seq.unwrap_or(0).saturating_add(1);
            let last_seq = gap
                .first_available_seq
                .map(|first_available| first_available.saturating_sub(1))
                .unwrap_or(attach.snapshot.head_seq)
                .max(first_seq);
            GapRange {
                epoch: cursor
                    .as_ref()
                    .map_or(attach.snapshot.daemon_epoch, |cursor| cursor.epoch),
                first_seq,
                last_seq,
                reason: gap.reason,
            }
        })
        .into_iter()
        .collect();
    let first_available_seq = attach.snapshot.ring_oldest_seq;
    EventQueryResponse {
        events,
        next_cursor: Some(Cursor {
            epoch: attach.snapshot.daemon_epoch,
            after_seq: next_after_seq,
        }),
        truncated: window_limited || attach.replay.gap.is_some(),
        first_available_seq,
        gaps,
    }
}

#[derive(Debug, serde::Deserialize)]
struct RecentActivityQuery {
    epoch: Uuid,
    after_seq: u64,
    through_seq: u64,
}

/// Returns only serial ownership/interference evidence from the bounded live
/// ring. It is used between two MCP operations and intentionally excludes RX,
/// so a noisy target cannot inflate normal tool results or force a disk scan.
async fn recent_activity(
    State(state): State<AppState>,
    Path(port): Path<String>,
    Query(query): Query<RecentActivityQuery>,
) -> Result<Json<EventQueryResponse>, ApiError> {
    if query.after_seq > query.through_seq {
        return Err(ApiError::BadRequest(
            "after_seq must not exceed through_seq".into(),
        ));
    }
    let handle = state
        .inner
        .registry
        .get(&port)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("unknown port {port}")))?;
    let attach = handle
        .attach(
            Some(&serial_protocol::Cursor {
                epoch: query.epoch,
                after_seq: query.after_seq,
            }),
            2_000,
        )
        .await?;
    let mut relevant = attach
        .replay
        .events
        .into_iter()
        .filter(|event| {
            event.seq <= query.through_seq
                && (event.direction == serial_protocol::Direction::Tx
                    || matches!(
                        event.kind,
                        serial_protocol::EventKind::ControlRevoked
                            | serial_protocol::EventKind::ControlExpired
                            | serial_protocol::EventKind::RunAborted
                            | serial_protocol::EventKind::PortReconfigured
                            | serial_protocol::EventKind::PortRemoved
                    ))
        })
        .collect::<Vec<_>>();
    let truncated = relevant.len() > 32 || attach.replay.gap.is_some();
    if relevant.len() > 32 {
        relevant.drain(..relevant.len() - 32);
    }
    Ok(Json(EventQueryResponse {
        events: relevant,
        next_cursor: Some(serial_protocol::Cursor {
            epoch: attach.snapshot.daemon_epoch,
            after_seq: query.through_seq.min(attach.snapshot.head_seq),
        }),
        truncated,
        first_available_seq: attach.snapshot.ring_oldest_seq,
        gaps: Vec::new(),
    }))
}

#[derive(Debug, serde::Deserialize)]
struct MonitorListQuery {
    port: Option<String>,
    status: Option<MonitorStatus>,
}

async fn create_monitor(
    State(state): State<AppState>,
    Json(request): Json<CreateMonitorRequest>,
) -> Result<Json<MonitorResponse>, ApiError> {
    Ok(Json(state.inner.monitors.create(request).await?))
}

async fn list_monitors(
    State(state): State<AppState>,
    Query(query): Query<MonitorListQuery>,
) -> Result<Json<MonitorListResponse>, ApiError> {
    Ok(Json(
        state
            .inner
            .monitors
            .list(query.port.as_deref(), query.status)
            .await,
    ))
}

async fn get_monitor(
    State(state): State<AppState>,
    Path(monitor_id): Path<Uuid>,
) -> Result<Json<MonitorResponse>, ApiError> {
    Ok(Json(state.inner.monitors.get(monitor_id).await?))
}

async fn update_monitor(
    State(state): State<AppState>,
    Path(monitor_id): Path<Uuid>,
    Json(request): Json<UpdateMonitorRequest>,
) -> Result<Json<MonitorResponse>, ApiError> {
    Ok(Json(
        state.inner.monitors.update(monitor_id, request).await?,
    ))
}

async fn stop_monitor(
    State(state): State<AppState>,
    Path(monitor_id): Path<Uuid>,
    Query(query): Query<MonitorMutationQuery>,
) -> Result<Json<MonitorResponse>, ApiError> {
    Ok(Json(
        state
            .inner
            .monitors
            .stop(monitor_id, query.expected_revision)
            .await?,
    ))
}

#[derive(Debug, serde::Deserialize)]
struct MonitorMutationQuery {
    expected_revision: u64,
}

#[derive(Debug, serde::Deserialize)]
struct MonitorIncidentQuery {
    after_incident_seq: Option<u64>,
    limit: Option<usize>,
    #[serde(default)]
    include_acked: bool,
}

async fn list_monitor_incidents(
    State(state): State<AppState>,
    Path(monitor_id): Path<Uuid>,
    Query(query): Query<MonitorIncidentQuery>,
) -> Result<Json<MonitorIncidentListResponse>, ApiError> {
    Ok(Json(
        state
            .inner
            .monitors
            .incidents(
                monitor_id,
                query.after_incident_seq,
                query.limit,
                query.include_acked,
            )
            .await?,
    ))
}

async fn acknowledge_monitor_incident(
    State(state): State<AppState>,
    Path((monitor_id, incident_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<MonitorIncidentResponse>, ApiError> {
    Ok(Json(MonitorIncidentResponse {
        incident: state
            .inner
            .monitors
            .acknowledge_incident(monitor_id, incident_id)
            .await?,
    }))
}

async fn websocket(
    State(state): State<AppState>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let connection_permit = state
        .inner
        .ws_connections
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::TooManyConnections)?;
    Ok(upgrade
        .max_message_size(MAX_WS_INCOMING_BYTES)
        .max_frame_size(MAX_WS_INCOMING_BYTES)
        .on_upgrade(move |socket| async move {
            let _connection_permit = connection_permit;
            serve_socket(socket, state).await;
        })
        .into_response())
}

async fn serve_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();
    let (outbound, mut outbound_rx) = mpsc::channel::<Message>(OUTBOUND_QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if sink.send(frame).await.is_err() {
                break;
            }
        }
    });

    let actor = match receive_hello(&mut stream, &outbound, &state).await {
        Ok(actor) => actor,
        Err(()) => {
            drop(outbound);
            let _ = writer.await;
            return;
        }
    };

    let mut subscriptions: HashMap<String, JoinHandle<()>> = HashMap::new();
    while let Some(incoming) = stream.next().await {
        let message = match incoming {
            Ok(Message::Binary(bytes)) => serial_protocol::decode_client_control(&bytes),
            Ok(Message::Text(text)) => serde_json::from_str::<ClientMessage>(&text)
                .map_err(serial_protocol::ProtocolError::from),
            Ok(Message::Ping(payload)) => {
                let _ = outbound.send(Message::Pong(payload)).await;
                continue;
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(Message::Pong(_)) => continue,
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                send_error(
                    &outbound,
                    None,
                    ErrorCode::BadRequest,
                    error.to_string(),
                    false,
                )
                .await;
                continue;
            }
        };
        let request_id = message.request_id();
        if let Err(error) =
            dispatch_message(message, &actor, &state, &outbound, &mut subscriptions).await
        {
            let (code, retryable) = error.protocol_code();
            send_error(
                &outbound,
                Some(request_id),
                code,
                error.to_string(),
                retryable,
            )
            .await;
        }
    }

    for subscription in subscriptions.into_values() {
        subscription.abort();
    }
    state.inner.registry.disconnect_actor(&actor.id).await;
    drop(outbound);
    let _ = writer.await;
}

async fn receive_hello(
    stream: &mut futures_util::stream::SplitStream<WebSocket>,
    outbound: &mpsc::Sender<Message>,
    state: &AppState,
) -> Result<Actor, ()> {
    let message = match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
        Ok(Some(Ok(Message::Binary(bytes)))) => {
            serial_protocol::decode_client_control(&bytes).map_err(|_| ())?
        }
        Ok(Some(Ok(Message::Text(text)))) => {
            serde_json::from_str::<ClientMessage>(&text).map_err(|_| ())?
        }
        _ => return Err(()),
    };
    let ClientMessage::Hello {
        request_id,
        protocol_version,
        client_name,
        actor_kind,
    } = message
    else {
        send_error(
            outbound,
            Some(message.request_id()),
            ErrorCode::BadRequest,
            "hello must be the first message".into(),
            false,
        )
        .await;
        return Err(());
    };
    if protocol_version != PROTOCOL_VERSION {
        send_error(
            outbound,
            Some(request_id),
            ErrorCode::Conflict,
            format!(
                "protocol version {protocol_version} is unsupported; expected {PROTOCOL_VERSION}"
            ),
            false,
        )
        .await;
        return Err(());
    }
    let actor = match issue_actor(actor_kind, &client_name) {
        Ok(actor) => actor,
        Err(error) => {
            send_error(
                outbound,
                Some(request_id),
                ErrorCode::BadRequest,
                error.to_string(),
                false,
            )
            .await;
            return Err(());
        }
    };
    let server_id = state.inner.config.read().await.server_id;
    send_control(
        outbound,
        ServerMessage::Welcome {
            server_id,
            daemon_epoch: state.inner.daemon_epoch,
            protocol_version: PROTOCOL_VERSION,
            actor: actor.clone(),
        },
    )
    .await?;
    send_control(
        outbound,
        ServerMessage::Result {
            request_id,
            result: CommandResult::HelloAccepted {
                actor: actor.clone(),
            },
        },
    )
    .await?;
    Ok(actor)
}

fn issue_actor(kind: ActorKind, requested_label: &str) -> Result<Actor, &'static str> {
    if kind == ActorKind::System {
        return Err("the system actor kind is reserved for seriald");
    }
    let label = requested_label.trim();
    if label.is_empty() || label.len() > 128 || label.chars().any(char::is_control) {
        return Err("actor label must contain 1-128 non-control UTF-8 bytes");
    }
    let prefix = match kind {
        ActorKind::Human => "human",
        ActorKind::Agent => "agent",
        ActorKind::Script => "script",
        ActorKind::System => unreachable!(),
    };
    Ok(Actor {
        id: format!("{prefix}:{}", Uuid::new_v4().simple()),
        label: label.to_owned(),
        kind,
    })
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_message(
    message: ClientMessage,
    actor: &Actor,
    state: &AppState,
    outbound: &mpsc::Sender<Message>,
    subscriptions: &mut HashMap<String, JoinHandle<()>>,
) -> Result<(), WsError> {
    match message {
        ClientMessage::Hello { .. } => {
            Err(WsError::BadRequest("hello may only be sent once".into()))
        }
        ClientMessage::Attach {
            request_id,
            subscriptions: requested,
        } => {
            let mut attached = Vec::new();
            for request in requested {
                let handle = state
                    .inner
                    .registry
                    .get(&request.port)
                    .await
                    .ok_or_else(|| WsError::NotFound(request.port.clone()))?;
                if let Some(old) = subscriptions.remove(&request.port) {
                    old.abort();
                }
                let attach = handle
                    .attach(request.cursor.as_ref(), request.tail_events)
                    .await?;
                send_attach(outbound, &handle, &attach).await?;
                let port = request.port;
                subscriptions.insert(
                    port.clone(),
                    spawn_live_forwarder(outbound.clone(), handle, attach),
                );
                attached.push(port);
            }
            send_result(
                outbound,
                request_id,
                CommandResult::Attached { ports: attached },
            )
            .await
        }
        ClientMessage::Detach { request_id, ports } => {
            let mut detached = Vec::new();
            for slot in ports {
                if let Some(task) = subscriptions.remove(&slot) {
                    task.abort();
                    detached.push(slot);
                }
            }
            send_result(
                outbound,
                request_id,
                CommandResult::Detached { ports: detached },
            )
            .await
        }
        ClientMessage::Ping { request_id } => {
            send_result(
                outbound,
                request_id,
                CommandResult::Pong {
                    server_wall_time_ns: wall_time_ns(),
                },
            )
            .await
        }
        other => {
            let port = command_slot(&other)
                .ok_or_else(|| WsError::BadRequest("message has no port".into()))?;
            let handle = state
                .inner
                .registry
                .get(port)
                .await
                .ok_or_else(|| WsError::NotFound(port.into()))?;
            let (request_id, result) = dispatch_slot_command(other, handle, actor.clone()).await?;
            send_result(outbound, request_id, result).await
        }
    }
}

async fn dispatch_slot_command(
    message: ClientMessage,
    handle: SlotHandle,
    actor: Actor,
) -> Result<(Uuid, CommandResult), WsError> {
    let request_id = message.request_id();
    let result = match message {
        ClientMessage::AcquireControl { mode, ttl_ms, .. } => {
            handle
                .acquire_control(request_id, actor, mode, ttl_ms)
                .await?
        }
        ClientMessage::RenewControl {
            control_id,
            fence,
            ttl_ms,
            ..
        } => {
            handle
                .renew_control(request_id, actor, control_id, fence, ttl_ms)
                .await?
        }
        ClientMessage::ReleaseControl {
            control_id, fence, ..
        } => {
            handle
                .release_control(request_id, actor, control_id, fence)
                .await?
        }
        ClientMessage::CancelAcquire { control_id, .. } => {
            handle.cancel_acquire(request_id, actor, control_id).await?
        }
        ClientMessage::Write {
            control_id,
            fence,
            data,
            operation_id,
            expected_run_id,
            pacing,
            description,
            command_capture_matchers,
            command_sequence,
            sequence_precondition,
            cooperative,
            ..
        } => {
            handle
                .write(
                    request_id,
                    actor,
                    control_id,
                    fence,
                    data,
                    operation_id,
                    expected_run_id,
                    pacing,
                    description,
                    command_capture_matchers,
                    command_sequence,
                    sequence_precondition,
                    cooperative,
                )
                .await?
        }
        ClientMessage::SendBreak {
            control_id,
            fence,
            duration_ms,
            operation_id,
            expected_run_id,
            sequence_precondition,
            ..
        } => {
            handle
                .send_break(
                    request_id,
                    actor,
                    control_id,
                    fence,
                    duration_ms,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                )
                .await?
        }
        ClientMessage::TriggerStart {
            control_id,
            fence,
            daemon_epoch,
            generation,
            operation_id,
            expected_run_id,
            sequence_precondition,
            spec,
            ..
        } => {
            handle
                .start_trigger(
                    request_id,
                    actor,
                    control_id,
                    fence,
                    daemon_epoch,
                    generation,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                    spec,
                )
                .await?
        }
        ClientMessage::TriggerStatus {
            daemon_epoch,
            generation,
            trigger_id,
            ..
        } => {
            handle
                .trigger_status(request_id, actor, daemon_epoch, generation, trigger_id)
                .await?
        }
        ClientMessage::TriggerCancel {
            control_id,
            fence,
            daemon_epoch,
            generation,
            trigger_id,
            ..
        } => {
            handle
                .cancel_trigger(
                    request_id,
                    actor,
                    control_id,
                    fence,
                    daemon_epoch,
                    generation,
                    trigger_id,
                )
                .await?
        }
        ClientMessage::StartRun {
            control_id,
            fence,
            label,
            metadata,
            ..
        } => {
            handle
                .start_run(request_id, actor, control_id, fence, label, metadata)
                .await?
        }
        ClientMessage::EndRun {
            control_id,
            fence,
            run_id,
            ..
        } => {
            handle
                .end_run(request_id, actor, control_id, fence, run_id)
                .await?
        }
        ClientMessage::Checkpoint {
            control_id,
            fence,
            label,
            ..
        } => {
            handle
                .checkpoint(request_id, actor, control_id, fence, label)
                .await?
        }
        _ => return Err(WsError::BadRequest("unsupported command".into())),
    };
    Ok((request_id, result))
}

async fn send_attach(
    outbound: &mpsc::Sender<Message>,
    handle: &SlotHandle,
    attach: &AttachState,
) -> Result<(), WsError> {
    send_control(
        outbound,
        ServerMessage::Snapshot {
            port: Box::new(attach.snapshot.clone()),
        },
    )
    .await
    .map_err(|_| WsError::Closed)?;
    if let Some(gap) = &attach.replay.gap {
        send_control(
            outbound,
            ServerMessage::Gap {
                port: handle.id().into(),
                requested_after_seq: gap.requested_after_seq,
                first_available_seq: gap.first_available_seq,
                head_seq: attach.snapshot.head_seq,
                reason: gap.reason,
            },
        )
        .await
        .map_err(|_| WsError::Closed)?;
    }
    if let (Some(first), Some(last)) = (attach.replay.events.first(), attach.replay.events.last()) {
        send_control(
            outbound,
            ServerMessage::ReplayBegin {
                port: handle.id().into(),
                from_seq: first.seq,
                through_seq: last.seq,
            },
        )
        .await
        .map_err(|_| WsError::Closed)?;
    }
    for event in &attach.replay.events {
        outbound
            .send(Message::Binary(
                encode_event(event, true)
                    .map_err(|error| WsError::Codec(error.to_string()))?
                    .into(),
            ))
            .await
            .map_err(|_| WsError::Closed)?;
    }
    send_control(
        outbound,
        ServerMessage::Ready {
            port: handle.id().into(),
            head_seq: attach.snapshot.head_seq,
        },
    )
    .await
    .map_err(|_| WsError::Closed)
}

fn spawn_live_forwarder(
    outbound: mpsc::Sender<Message>,
    handle: SlotHandle,
    mut attach: AttachState,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_seq = attach.snapshot.head_seq;
        loop {
            match attach.live.recv().await {
                Ok(event) => {
                    if event.daemon_epoch != attach.snapshot.daemon_epoch || event.seq <= last_seq {
                        continue;
                    }
                    last_seq = event.seq;
                    let Ok(frame) = encode_event(&event, false) else {
                        break;
                    };
                    if outbound.send(Message::Binary(frame.into())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    handle.record_subscriber_lag(skipped);
                    let head = handle.snapshot().head_seq;
                    let message = ServerMessage::Lagged {
                        port: handle.id().into(),
                        from_seq: last_seq.saturating_add(1),
                        to_seq: head.max(last_seq.saturating_add(skipped)),
                    };
                    if send_control(&outbound, message).await.is_err() {
                        break;
                    }
                    // Detach only this port. The caller can recover via the
                    // history endpoint and attach again with a cursor.
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    })
}

fn command_slot(message: &ClientMessage) -> Option<&str> {
    match message {
        ClientMessage::AcquireControl { port, .. }
        | ClientMessage::RenewControl { port, .. }
        | ClientMessage::ReleaseControl { port, .. }
        | ClientMessage::CancelAcquire { port, .. }
        | ClientMessage::Write { port, .. }
        | ClientMessage::SendBreak { port, .. }
        | ClientMessage::TriggerStart { port, .. }
        | ClientMessage::TriggerStatus { port, .. }
        | ClientMessage::TriggerCancel { port, .. }
        | ClientMessage::StartRun { port, .. }
        | ClientMessage::EndRun { port, .. }
        | ClientMessage::Checkpoint { port, .. } => Some(port),
        _ => None,
    }
}

async fn send_result(
    outbound: &mpsc::Sender<Message>,
    request_id: Uuid,
    result: CommandResult,
) -> Result<(), WsError> {
    send_control(outbound, ServerMessage::Result { request_id, result })
        .await
        .map_err(|_| WsError::Closed)
}

async fn send_control(outbound: &mpsc::Sender<Message>, message: ServerMessage) -> Result<(), ()> {
    let frame = encode_control(&message).map_err(|_| ())?;
    outbound
        .send(Message::Binary(frame.into()))
        .await
        .map_err(|_| ())
}

async fn send_error(
    outbound: &mpsc::Sender<Message>,
    request_id: Option<Uuid>,
    code: ErrorCode,
    message: String,
    retryable: bool,
) {
    let _ = send_control(
        outbound,
        ServerMessage::Error {
            request_id,
            code,
            message,
            retryable,
        },
    )
    .await;
}

#[derive(Debug, thiserror::Error)]
enum WsError {
    #[error("request is invalid: {0}")]
    BadRequest(String),
    #[error("unknown port {0}")]
    NotFound(String),
    #[error("connection output is closed")]
    Closed,
    #[error("wire codec failed: {0}")]
    Codec(String),
    #[error(transparent)]
    Slot(#[from] SlotError),
}

impl WsError {
    fn protocol_code(&self) -> (ErrorCode, bool) {
        match self {
            Self::BadRequest(_) | Self::Codec(_) => (ErrorCode::BadRequest, false),
            Self::NotFound(_) => (ErrorCode::NotFound, false),
            Self::Closed => (ErrorCode::Internal, true),
            Self::Slot(error) => match error {
                SlotError::PortOffline | SlotError::Closed | SlotError::ReplyDropped => {
                    (ErrorCode::PortOffline, true)
                }
                SlotError::Control(crate::control::ControlError::NotOwner) => {
                    (ErrorCode::ControlRequired, false)
                }
                SlotError::Control(_) => (ErrorCode::StaleFence, false),
                SlotError::CursorAhead => (ErrorCode::CursorAhead, false),
                SlotError::RunAlreadyActive
                | SlotError::NoActiveRun
                | SlotError::RunMismatch
                | SlotError::WriteRunMissing { .. }
                | SlotError::WriteRunMismatch { .. }
                | SlotError::WriteRunNotOwner { .. }
                | SlotError::PartialWrite { .. }
                | SlotError::TriggerActive
                | SlotError::TriggerNotOwner { .. }
                | SlotError::TriggerEpochMismatch
                | SlotError::TriggerGenerationMismatch
                | SlotError::RequestIdReused => (ErrorCode::Conflict, false),
                SlotError::SequenceBoundaryChanged { .. } => {
                    (ErrorCode::SequenceBoundaryChanged, false)
                }
                SlotError::ProfileChangeBusy => (ErrorCode::ProfileChangeBusy, false),
                SlotError::WriteLeaseTooShort { .. } => (ErrorCode::Conflict, true),
                SlotError::WriteResultExpired => (ErrorCode::IdempotencyExpired, false),
                SlotError::WriteIdempotencyCapacity => (ErrorCode::ResourceExhausted, false),
                SlotError::ControlQueueFull => (ErrorCode::ResourceExhausted, true),
                SlotError::TriggerNotFound { .. } => (ErrorCode::NotFound, false),
                SlotError::BreakUnsupported => (ErrorCode::BreakUnsupported, false),
                SlotError::BreakFailed { .. } => (ErrorCode::PortIo, false),
                SlotError::WriteTooLarge
                | SlotError::EmptyWrite
                | SlotError::InvalidCommandDescription
                | SlotError::InvalidCommandSequenceAudit
                | SlotError::WriteDeadlineExceeded { .. }
                | SlotError::InvalidBreakDuration
                | SlotError::InvalidTriggerAction
                | SlotError::TriggerInitialWriteTooLarge
                | SlotError::InvalidTriggerInterval
                | SlotError::InvalidTriggerTimeout
                | SlotError::InvalidTriggerMaxFires
                | SlotError::InvalidTriggerPatterns
                | SlotError::TriggerTotalBytesTooLarge
                | SlotError::InvalidLabel
                | SlotError::RunMetadataTooManyKeys { .. }
                | SlotError::RunMetadataTooLarge { .. } => (ErrorCode::BadRequest, false),
                SlotError::SlotIdChanged => (ErrorCode::Internal, false),
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("configuration persistence failed ({save}); runtime rollback also failed ({rollback})")]
    ConfigRollback {
        save: ConfigError,
        rollback: RegistryRollbackError,
    },
    #[error(
        "runtime commit failed ({commit}); restoring the previous persisted configuration also failed ({restore})"
    )]
    ConfigCommitRestore {
        commit: Box<RegistryError>,
        restore: ConfigError,
    },
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Monitor(#[from] MonitorError),
    #[error(transparent)]
    Slot(#[from] SlotError),
    #[error("{0}")]
    NotFound(String),
    #[error("request is invalid: {0}")]
    BadRequest(String),
    #[error("the seriald WebSocket connection limit has been reached")]
    TooManyConnections,
    #[error("configuration revision mismatch: expected {expected}, current {actual}")]
    ConfigRevisionMismatch { expected: u64, actual: u64 },
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Self::Journal(JournalError::QueryBudgetExceeded {
            phase,
            scanned_bytes,
            elapsed_ms,
        }) = &self
        {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "code": ErrorCode::QueryBudgetExceeded,
                    "message": self.to_string(),
                    "retryable": true,
                    "phase": phase,
                    "scanned_bytes": scanned_bytes,
                    "elapsed_ms": elapsed_ms,
                    "retry_hint": "Retry from the last returned epoch/cursor with a smaller window or narrower filter; do not restart from sequence zero."
                })),
            )
                .into_response();
        }
        let (status, code) = match &self {
            Self::Config(ConfigError::Validation(_)) => {
                (StatusCode::BAD_REQUEST, ErrorCode::BadRequest)
            }
            Self::Registry(
                RegistryError::InvalidConfig(_) | RegistryError::IdentityLimit { .. },
            ) => (StatusCode::BAD_REQUEST, ErrorCode::BadRequest),
            Self::Registry(RegistryError::Slot(SlotError::ProfileChangeBusy)) => {
                (StatusCode::CONFLICT, ErrorCode::ProfileChangeBusy)
            }
            Self::Registry(RegistryError::Shutdown | RegistryError::Degraded) => {
                (StatusCode::SERVICE_UNAVAILABLE, ErrorCode::Unavailable)
            }
            Self::Journal(JournalError::InvalidConfig(_) | JournalError::InvalidPortId) => {
                (StatusCode::BAD_REQUEST, ErrorCode::BadRequest)
            }
            Self::Journal(JournalError::InvalidRegex(_)) => {
                (StatusCode::BAD_REQUEST, ErrorCode::RegexInvalid)
            }
            Self::Journal(JournalError::QueryBudgetExceeded { .. }) => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::QueryBudgetExceeded,
            ),
            Self::Monitor(MonitorError::NotFound(_) | MonitorError::UnknownSlot(_)) => {
                (StatusCode::NOT_FOUND, ErrorCode::NotFound)
            }
            Self::Monitor(
                MonitorError::RequestIdReused(_) | MonitorError::RevisionMismatch { .. },
            ) => (StatusCode::CONFLICT, ErrorCode::Conflict),
            Self::Monitor(MonitorError::Capacity | MonitorError::ActiveCapacity) => {
                (StatusCode::TOO_MANY_REQUESTS, ErrorCode::ResourceExhausted)
            }
            Self::Monitor(MonitorError::InvalidSpec(_) | MonitorError::CursorAhead) => {
                (StatusCode::BAD_REQUEST, ErrorCode::BadRequest)
            }
            Self::NotFound(_) => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, ErrorCode::BadRequest),
            Self::TooManyConnections => {
                (StatusCode::TOO_MANY_REQUESTS, ErrorCode::ResourceExhausted)
            }
            Self::ConfigRevisionMismatch { .. } => {
                (StatusCode::CONFLICT, ErrorCode::ConfigRevisionMismatch)
            }
            Self::Config(_)
            | Self::Registry(_)
            | Self::ConfigRollback { .. }
            | Self::ConfigCommitRestore { .. }
            | Self::Journal(_)
            | Self::Monitor(_)
            | Self::Slot(_)
            | Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal),
        };
        (
            status,
            Json(serde_json::json!({ "code": code, "message": self.to_string() })),
        )
            .into_response()
    }
}

fn wall_time_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_else(|| {
        chrono::Utc::now()
            .timestamp_millis()
            .saturating_mul(1_000_000)
    })
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::Path,
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn encoded_macos_device_path_round_trips_through_port_route() {
        async fn echo_port(Path(port): Path<String>) -> String {
            port
        }

        let app = Router::new().route("/api/v1/ports/{port}/events", get(echo_port));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/ports/%2Fdev%2Fcu.usbserial-210/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            to_bytes(response.into_body(), 1024).await.unwrap(),
            "/dev/cu.usbserial-210"
        );
    }
}
