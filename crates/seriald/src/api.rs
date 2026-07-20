use crate::auth::{AuthError, Principal, role_allows};
use crate::config::{ConfigError, ConfigStore, DaemonConfig};
use crate::journal::{JournalError, JournalHandle};
use crate::registry::{RegistryError, RegistryRollbackError, SlotRegistry};
use crate::slot::{AttachState, SlotError, SlotHandle};
use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    Actor, ArchiveListResponse, ClientMessage, CommandResult, ConfigureDeviceProfilesRequest,
    ConfigureDeviceProfilesResponse, ConfigureSlotsRequest, ConfigureSlotsResponse,
    DeviceProfileListResponse, ErrorCode, EventQuery, EventQueryResponse, HealthResponse,
    PROTOCOL_VERSION, PortDescriptor, Role, ServerMessage, StatusResponse, encode_control,
    encode_event,
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
        Self {
            inner: Arc::new(AppStateInner {
                config_store,
                config: RwLock::new(config),
                config_updates: Mutex::new(()),
                registry,
                journal,
                daemon_epoch,
                started,
                ws_connections: Arc::new(Semaphore::new(MAX_WS_CONNECTIONS)),
            }),
        }
    }

    pub async fn shutdown(&self) {
        let _update = self.inner.config_updates.lock().await;
        self.inner.registry.shutdown().await;
    }

    async fn configure_slots_transaction(
        &self,
        requested: Vec<serial_protocol::SlotConfig>,
    ) -> Result<Vec<serial_protocol::SlotSnapshot>, ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        let staged = current
            .staged_with_slots(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .apply_replacement(staged.slots.clone(), staged.device_profiles.clone())
            .await?;

        match self.inner.config_store.save(&staged) {
            Ok(()) => {
                let snapshots = applied.commit().await?;
                *self.inner.config.write().await = staged;
                Ok(snapshots)
            }
            Err(save) => match applied.rollback().await {
                Ok(()) => Err(ApiError::Config(save)),
                Err(rollback) => Err(ApiError::ConfigRollback { save, rollback }),
            },
        }
    }

    /// Validates, persists, and then publishes a device profile catalog. The
    /// runtime effect is limited to snapshots resolving prompts from the new
    /// profiles, so a persistence failure simply leaves every view unchanged.
    async fn configure_device_profiles_transaction(
        &self,
        requested: Vec<serial_protocol::DeviceProfile>,
    ) -> Result<Vec<serial_protocol::DeviceProfile>, ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        let staged = current
            .staged_with_device_profiles(requested)
            .map_err(ConfigError::from)?;
        self.inner.config_store.save(&staged)?;
        *self.inner.config.write().await = staged.clone();
        self.inner
            .registry
            .apply_device_profiles(staged.device_profiles.clone())
            .await;
        Ok(staged.device_profiles)
    }

    async fn authenticate(
        &self,
        headers: &HeaderMap,
        required: Role,
    ) -> Result<Principal, ApiError> {
        let authorization = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        let config = self.inner.config.read().await;
        let principal = config
            .auth
            .authenticate_authorization(authorization)
            .map_err(ApiError::Auth)?;
        principal.require_role(required).map_err(ApiError::Auth)?;
        Ok(principal)
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/status", get(status))
        .route("/api/v1/ports", get(ports))
        .route("/api/v1/config/slots", put(configure_slots))
        .route(
            "/api/v1/config/device-profiles",
            get(list_device_profiles).put(configure_device_profiles),
        )
        .route("/api/v1/archives", get(archives))
        .route("/api/v1/slots/{slot_id}/events", get(events))
        .route("/api/v1/ws", get(websocket))
        .with_state(state)
}

async fn health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<HealthResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
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
    }))
}

async fn status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StatusResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await;
    Ok(Json(StatusResponse {
        server_id: config.server_id,
        daemon_epoch: state.inner.daemon_epoch,
        slots: state.inner.registry.snapshots().await,
    }))
}

async fn ports(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<PortDescriptor>>, ApiError> {
    state.authenticate(&headers, Role::Admin).await?;
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

async fn configure_slots(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConfigureSlotsRequest>,
) -> Result<Json<ConfigureSlotsResponse>, ApiError> {
    state.authenticate(&headers, Role::Admin).await?;
    // Keep the transaction alive even if the HTTP request is cancelled after
    // physical actors were staged. The spawned task must either commit all
    // three views or run the compensating rollback.
    let transaction = state.clone();
    let slots =
        tokio::spawn(async move { transaction.configure_slots_transaction(request.slots).await })
            .await
            .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureSlotsResponse { slots }))
}

async fn list_device_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DeviceProfileListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await;
    Ok(Json(DeviceProfileListResponse {
        profiles: config.device_profiles.clone(),
    }))
}

async fn configure_device_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConfigureDeviceProfilesRequest>,
) -> Result<Json<ConfigureDeviceProfilesResponse>, ApiError> {
    state.authenticate(&headers, Role::Admin).await?;
    // Mirror the Slot transaction: the spawned task completes the validate /
    // persist / publish sequence even if the HTTP request is cancelled.
    let transaction = state.clone();
    let profiles = tokio::spawn(async move {
        transaction
            .configure_device_profiles_transaction(request.profiles)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureDeviceProfilesResponse { profiles }))
}

#[derive(Debug, serde::Deserialize)]
struct ArchiveListQuery {
    slot_id: Option<String>,
}

async fn archives(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ArchiveListQuery>,
) -> Result<Json<ArchiveListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    Ok(Json(
        state.inner.journal.list_archives(query.slot_id).await?,
    ))
}

async fn events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slot_id): Path<String>,
    Query(mut query): Query<EventQuery>,
) -> Result<Json<EventQueryResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    // Normal history reads are scoped to this daemon run so an omitted epoch
    // can never surface a matching log from an earlier test cycle. Archived
    // history remains available by explicitly supplying its epoch.
    query.epoch.get_or_insert(state.inner.daemon_epoch);
    Ok(Json(state.inner.journal.query(slot_id, query).await?))
}

async fn websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    let principal = state.authenticate(&headers, Role::Observer).await?;
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
            serve_socket(socket, state, principal).await;
        })
        .into_response())
}

async fn serve_socket(socket: WebSocket, state: AppState, principal: Principal) {
    let (mut sink, mut stream) = socket.split();
    let (outbound, mut outbound_rx) = mpsc::channel::<Message>(OUTBOUND_QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            if sink.send(frame).await.is_err() {
                break;
            }
        }
    });

    let actor = match receive_hello(&mut stream, &outbound, &state, principal).await {
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
        if let Err(error) = dispatch_message(
            message,
            &actor,
            principal,
            &state,
            &outbound,
            &mut subscriptions,
        )
        .await
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
    principal: Principal,
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
    let authenticated = match principal.issue_actor(actor_kind, &client_name) {
        Ok(authenticated) => authenticated,
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
    let actor = authenticated.actor().clone();
    let server_id = state.inner.config.read().await.server_id;
    send_control(
        outbound,
        ServerMessage::Welcome {
            server_id,
            daemon_epoch: state.inner.daemon_epoch,
            protocol_version: PROTOCOL_VERSION,
            actor: actor.clone(),
            role: principal.role(),
        },
    )
    .await?;
    send_control(
        outbound,
        ServerMessage::Result {
            request_id,
            result: CommandResult::HelloAccepted {
                actor: actor.clone(),
                role: principal.role(),
            },
        },
    )
    .await?;
    Ok(actor)
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_message(
    message: ClientMessage,
    actor: &Actor,
    principal: Principal,
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
                    .get(&request.slot_id)
                    .await
                    .ok_or_else(|| WsError::NotFound(request.slot_id.clone()))?;
                if let Some(old) = subscriptions.remove(&request.slot_id) {
                    old.abort();
                }
                let attach = handle
                    .attach(request.cursor.as_ref(), request.tail_events)
                    .await?;
                send_attach(outbound, &handle, &attach).await?;
                let slot_id = request.slot_id;
                subscriptions.insert(
                    slot_id.clone(),
                    spawn_live_forwarder(outbound.clone(), handle, attach),
                );
                attached.push(slot_id);
            }
            send_result(
                outbound,
                request_id,
                CommandResult::Attached { slots: attached },
            )
            .await
        }
        ClientMessage::Detach { request_id, slots } => {
            let mut detached = Vec::new();
            for slot in slots {
                if let Some(task) = subscriptions.remove(&slot) {
                    task.abort();
                    detached.push(slot);
                }
            }
            send_result(
                outbound,
                request_id,
                CommandResult::Detached { slots: detached },
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
            if !role_allows(principal.role(), Role::Operator) {
                return Err(WsError::Forbidden);
            }
            let slot_id = command_slot(&other)
                .ok_or_else(|| WsError::BadRequest("message has no Slot".into()))?;
            let handle = state
                .inner
                .registry
                .get(slot_id)
                .await
                .ok_or_else(|| WsError::NotFound(slot_id.into()))?;
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
            pacing,
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
                    pacing,
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
            slot: Box::new(attach.snapshot.clone()),
        },
    )
    .await
    .map_err(|_| WsError::Closed)?;
    if let Some(gap) = &attach.replay.gap {
        send_control(
            outbound,
            ServerMessage::Gap {
                slot_id: handle.id().into(),
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
                slot_id: handle.id().into(),
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
            slot_id: handle.id().into(),
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
                    let head = handle.snapshot().head_seq;
                    let message = ServerMessage::Lagged {
                        slot_id: handle.id().into(),
                        from_seq: last_seq.saturating_add(1),
                        to_seq: head.max(last_seq.saturating_add(skipped)),
                    };
                    if send_control(&outbound, message).await.is_err() {
                        break;
                    }
                    // Detach only this Slot. The caller can recover via the
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
        ClientMessage::AcquireControl { slot_id, .. }
        | ClientMessage::RenewControl { slot_id, .. }
        | ClientMessage::ReleaseControl { slot_id, .. }
        | ClientMessage::CancelAcquire { slot_id, .. }
        | ClientMessage::Write { slot_id, .. }
        | ClientMessage::StartRun { slot_id, .. }
        | ClientMessage::EndRun { slot_id, .. }
        | ClientMessage::Checkpoint { slot_id, .. } => Some(slot_id),
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
    #[error("the authenticated role may not write serial data")]
    Forbidden,
    #[error("unknown Slot {0}")]
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
            Self::Forbidden => (ErrorCode::Forbidden, false),
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
                | SlotError::PartialWrite { .. }
                | SlotError::RequestIdReused => (ErrorCode::Conflict, false),
                SlotError::WriteResultExpired => (ErrorCode::IdempotencyExpired, false),
                SlotError::WriteIdempotencyCapacity => (ErrorCode::ResourceExhausted, false),
                SlotError::ControlQueueFull => (ErrorCode::ResourceExhausted, true),
                SlotError::WriteTooLarge
                | SlotError::EmptyWrite
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
    Auth(#[from] AuthError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
    #[error("configuration persistence failed ({save}); runtime rollback also failed ({rollback})")]
    ConfigRollback {
        save: ConfigError,
        rollback: RegistryRollbackError,
    },
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Slot(#[from] SlotError),
    #[error("{0}")]
    NotFound(String),
    #[error("the seriald WebSocket connection limit has been reached")]
    TooManyConnections,
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Auth(AuthError::Forbidden) => StatusCode::FORBIDDEN,
            Self::Auth(_) => StatusCode::UNAUTHORIZED,
            Self::Config(ConfigError::Validation(_)) => StatusCode::BAD_REQUEST,
            Self::Registry(
                RegistryError::InvalidConfig(_) | RegistryError::IdentityLimit { .. },
            ) => StatusCode::BAD_REQUEST,
            Self::Registry(RegistryError::Shutdown | RegistryError::Degraded) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Journal(JournalError::InvalidConfig(_) | JournalError::InvalidSlotId) => {
                StatusCode::BAD_REQUEST
            }
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::TooManyConnections => StatusCode::TOO_MANY_REQUESTS,
            Self::Config(_)
            | Self::Registry(_)
            | Self::ConfigRollback { .. }
            | Self::Journal(_)
            | Self::Slot(_)
            | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let code = match status {
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::BAD_REQUEST => "bad_request",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::TOO_MANY_REQUESTS => "resource_exhausted",
            StatusCode::SERVICE_UNAVAILABLE => "unavailable",
            _ => "internal_error",
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
    use super::*;
    use crate::config::{ConfigPaths, ConfigStore};
    use crate::control::ControlLimits;
    use crate::journal::{JournalConfig, JournalManager};
    use serial_protocol::{SerialSettings, SlotConfig};

    fn disabled_slot(id: &str, display_name: &str, port: &str) -> SlotConfig {
        SlotConfig {
            id: id.into(),
            display_name: display_name.into(),
            port: port.into(),
            profile: "generic-115200".into(),
            device_profile: None,
            enabled: false,
            settings: SerialSettings {
                auto_open: false,
                ..SerialSettings::default()
            },
        }
    }

    #[tokio::test]
    async fn successful_slot_update_commits_runtime_disk_and_memory() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            loaded.config.slots.clone(),
            loaded.config.device_profiles.clone(),
            ControlLimits::default(),
        );
        let state = AppState::new(
            store.clone(),
            loaded.config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );
        let requested = vec![disabled_slot("slot-1", "Slot 1", "COM3")];

        let snapshots = state
            .configure_slots_transaction(requested.clone())
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].config, requested[0]);
        assert_eq!(state.inner.config.read().await.slots, requested);
        assert_eq!(store.load().unwrap().slots, requested);

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn invalid_slot_update_changes_no_authoritative_view() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            Vec::new(),
            Vec::new(),
            ControlLimits::default(),
        );
        let state = AppState::new(
            store.clone(),
            loaded.config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );

        let error = state
            .configure_slots_transaction(vec![
                disabled_slot("slot-1", "One", "COM3"),
                disabled_slot("slot-1", "Duplicate", "COM4"),
            ])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApiError::Config(ConfigError::Validation(_))
        ));
        assert!(state.inner.registry.snapshots().await.is_empty());
        assert!(state.inner.config.read().await.slots.is_empty());
        assert!(store.load().unwrap().slots.is_empty());

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_slot_updates_commit_as_whole_transactions() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            Vec::new(),
            Vec::new(),
            ControlLimits::default(),
        );
        let state = AppState::new(
            store.clone(),
            loaded.config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );
        let first_state = state.clone();
        let second_state = state.clone();
        let first = async move {
            first_state
                .configure_slots_transaction(vec![disabled_slot("slot-1", "First", "COM3")])
                .await
        };
        let second = async move {
            second_state
                .configure_slots_transaction(vec![disabled_slot("slot-1", "Second", "COM4")])
                .await
        };
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();

        let memory = state.inner.config.read().await.slots.clone();
        let disk = store.load().unwrap().slots;
        let runtime = state
            .inner
            .registry
            .snapshots()
            .await
            .into_iter()
            .map(|snapshot| snapshot.config)
            .collect::<Vec<_>>();
        assert_eq!(memory, disk);
        assert_eq!(memory, runtime);
        assert_eq!(
            state
                .inner
                .registry
                .get("slot-1")
                .await
                .unwrap()
                .snapshot()
                .head_seq,
            1
        );

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn save_failure_restores_runtime_and_keeps_disk_and_memory_old() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut loaded = store.load_or_create().unwrap();
        let old_slots = vec![disabled_slot("slot-old", "Old", "COM3")];
        store
            .update_slots(&mut loaded.config, old_slots.clone())
            .unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            old_slots.clone(),
            Vec::new(),
            ControlLimits::default(),
        );
        let state = AppState::new(
            store.clone(),
            loaded.config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );
        let old_handle = state.inner.registry.get("slot-old").await.unwrap();
        let mut old_live = old_handle.attach(None, 10).await.unwrap().live;
        store.set_save_failure(true);

        let error = state
            .configure_slots_transaction(vec![disabled_slot("slot-new", "New", "COM4")])
            .await
            .unwrap_err();
        assert!(matches!(&error, ApiError::Config(ConfigError::Io { .. })));
        assert_eq!(
            error.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(state.inner.config.read().await.slots, old_slots);
        assert_eq!(store.load().unwrap().slots, old_slots);
        assert_eq!(
            state
                .inner
                .registry
                .get("slot-old")
                .await
                .unwrap()
                .snapshot()
                .config,
            old_slots[0]
        );
        assert!(state.inner.registry.get("slot-new").await.is_none());
        assert_eq!(old_handle.snapshot().config, old_slots[0]);
        assert_eq!(old_handle.snapshot().head_seq, 0);
        assert!(matches!(
            old_live.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        store.set_save_failure(false);
        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    fn sigmastar_profile() -> serial_protocol::DeviceProfile {
        serial_protocol::DeviceProfile {
            name: "sigmastar-evb".into(),
            shell_prompt: Some("root@sigmastar:/# ".into()),
            uboot_prompt: Some("SigmaStar =>".into()),
            write_eol: None,
            echo: None,
        }
    }

    #[tokio::test]
    async fn device_profile_update_commits_memory_disk_and_live_snapshots() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let mut referencing = disabled_slot("slot-1", "Slot 1", "COM3");
        referencing.device_profile = Some("sigmastar-evb".into());
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            vec![referencing.clone()],
            vec![sigmastar_profile()],
            ControlLimits::default(),
        );
        // The catalog must be present in memory for validation to pass; the
        // registry was built with it directly above.
        let mut config = loaded.config.clone();
        config.slots = vec![referencing];
        config.device_profiles = vec![sigmastar_profile()];
        store.save(&config).unwrap();
        let state = AppState::new(
            store.clone(),
            config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );

        let snapshot = state.inner.registry.get("slot-1").await.unwrap().snapshot();
        assert_eq!(
            snapshot.effective_shell_prompt.as_deref(),
            Some("root@sigmastar:/# ")
        );
        assert_eq!(
            snapshot.effective_uboot_prompt.as_deref(),
            Some("SigmaStar =>")
        );

        // Replacing the catalog validates against existing Slots, persists,
        // and refreshes live snapshots without touching ports.
        let mut updated = sigmastar_profile();
        updated.uboot_prompt = Some("SigmaStar #".into());
        let profiles = state
            .configure_device_profiles_transaction(vec![updated.clone()])
            .await
            .unwrap();
        assert_eq!(profiles, vec![updated.clone()]);
        assert_eq!(
            state.inner.config.read().await.device_profiles,
            vec![updated.clone()]
        );
        assert_eq!(store.load().unwrap().device_profiles, vec![updated]);
        let snapshot = state.inner.registry.get("slot-1").await.unwrap().snapshot();
        assert_eq!(
            snapshot.effective_uboot_prompt.as_deref(),
            Some("SigmaStar #")
        );

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn orphaned_device_profile_update_is_rejected_everywhere() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let mut referencing = disabled_slot("slot-1", "Slot 1", "COM3");
        referencing.device_profile = Some("sigmastar-evb".into());
        let mut config = loaded.config.clone();
        config.slots = vec![referencing.clone()];
        config.device_profiles = vec![sigmastar_profile()];
        store.save(&config).unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            vec![referencing],
            vec![sigmastar_profile()],
            ControlLimits::default(),
        );
        let state = AppState::new(
            store.clone(),
            config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );

        // Deleting a profile that a Slot still references fails validation.
        let error = state
            .configure_device_profiles_transaction(Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApiError::Config(ConfigError::Validation(
                crate::config::ConfigValidationError::UnknownDeviceProfile { .. }
            ))
        ));
        assert_eq!(
            state.inner.config.read().await.device_profiles,
            vec![sigmastar_profile()]
        );
        assert_eq!(
            store.load().unwrap().device_profiles,
            vec![sigmastar_profile()]
        );

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn slot_without_device_profile_keeps_builtin_prompt_defaults() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            vec![disabled_slot("slot-1", "Slot 1", "COM3")],
            Vec::new(),
            ControlLimits::default(),
        );
        let snapshot = registry.get("slot-1").await.unwrap().snapshot();
        assert!(snapshot.effective_shell_prompt.is_none());
        assert_eq!(
            snapshot.effective_uboot_prompt.as_deref(),
            Some(serial_protocol::DEFAULT_UBOOT_PROMPT)
        );

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }
}
