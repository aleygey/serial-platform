use crate::auth::{AuthError, Principal, role_allows};
use crate::config::{ConfigError, ConfigStore, DaemonConfig};
use crate::journal::{JournalError, JournalHandle};
use crate::monitor::{MonitorError, MonitorManager};
use crate::registry::{RegistryError, RegistryRollbackError, SlotRegistry};
use crate::slot::{AttachState, SlotError, SlotHandle};
use axum::Json;
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    Actor, ArchiveListResponse, ClientMessage, CommandResult, ConfigureDeviceModelsRequest,
    ConfigureDeviceModelsResponse, ConfigureDeviceProfilesRequest, ConfigureDeviceProfilesResponse,
    ConfigureSlotsRequest, ConfigureSlotsResponse, ConfigureTransportProfilesRequest,
    ConfigureTransportProfilesResponse, CreateMonitorRequest, DaemonDiagnosticsResponse,
    DeviceModel, DeviceModelListResponse, DeviceProfileListResponse, ErrorCode, EventQuery,
    EventQueryResponse, HealthResponse, MonitorIncidentListResponse, MonitorIncidentResponse,
    MonitorListResponse, MonitorOutboxEventResponse, MonitorOutboxListResponse, MonitorResponse,
    MonitorStatus, PROTOCOL_VERSION, PortDescriptor, Role, ServerMessage,
    SetSlotDeviceModelRequest, SetSlotDeviceModelResponse, SlotDiagnostics, SlotModelBinding,
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
        let mut sink = config.monitor_event_sink.clone();
        if let Some(path) = sink.token_file.as_mut()
            && path.is_relative()
        {
            *path = config_store.paths().config_dir.join(&*path);
        }
        let monitors = MonitorManager::open(
            config_store.paths().monitor_state_file.clone(),
            registry.clone(),
            daemon_epoch,
            config.server_id,
            sink,
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

    async fn configure_slots_transaction(
        &self,
        requested: Vec<serial_protocol::SlotConfig>,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<serial_protocol::SlotSnapshot>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_slots(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .apply_replacement(
                staged.slots.clone(),
                staged.transport_profiles.clone(),
                staged.device_profiles.clone(),
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
    async fn configure_device_profiles_transaction(
        &self,
        requested: Vec<serial_protocol::DeviceProfile>,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<serial_protocol::DeviceProfile>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_device_profiles(requested)
            .map_err(ConfigError::from)?;
        let applied = self
            .inner
            .registry
            .stage_device_profiles(staged.device_profiles.clone())
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
                Ok((staged.device_profiles, revision))
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
            .apply_replacement(
                staged.slots.clone(),
                staged.transport_profiles.clone(),
                staged.device_profiles.clone(),
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

    async fn configure_device_models_transaction(
        &self,
        requested: Vec<DeviceModel>,
        expected_revision: Option<u64>,
    ) -> Result<(Vec<DeviceModel>, Vec<SlotModelBinding>, u64), ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(expected_revision, current.config_revision)?;
        let staged = current
            .staged_with_device_models(requested)
            .map_err(ConfigError::from)?;
        self.inner.config_store.save(&staged)?;
        let revision = staged.config_revision;
        let models = staged.device_models.clone();
        let bindings = staged.slot_model_bindings.clone();
        *self.inner.config.write().await = staged;
        Ok((models, bindings, revision))
    }

    async fn set_slot_device_model_transaction(
        &self,
        slot_id: String,
        request: SetSlotDeviceModelRequest,
    ) -> Result<SetSlotDeviceModelResponse, ApiError> {
        let _update = self.inner.config_updates.lock().await;
        let current = self.inner.config.read().await.clone();
        ensure_expected_revision(request.expected_revision, current.config_revision)?;
        if !current.slots.iter().any(|slot| slot.id == slot_id) {
            return Err(ApiError::NotFound(format!("unknown Slot {slot_id:?}")));
        }
        if request.source.is_empty()
            || request.source.len() > 128
            || request.source != request.source.trim()
            || request.source.chars().any(char::is_control)
        {
            return Err(ApiError::BadRequest(
                "source must be non-empty, trimmed text of at most 128 bytes".into(),
            ));
        }

        let current_model = current
            .slot_model_bindings
            .iter()
            .find(|binding| binding.slot_id == slot_id)
            .map(|binding| binding.model_id.clone());
        let has_expected_revision = request.expected_revision.is_some();
        let expected_current_guard = request.expected_current.clone();
        if let Some(expected) = request.expected_current.clone() {
            if expected.as_deref() != current_model.as_deref() {
                return Err(ApiError::ModelBindingMismatch {
                    expected,
                    actual: current_model,
                });
            }
        }

        let SetSlotDeviceModelRequest {
            model_id,
            create_if_missing,
            update_existing,
            name,
            parent_id,
            clear_parent,
            aliases,
            clear_aliases,
            confirmation_method,
            note,
            source,
            expected_revision: _,
            expected_current: _,
        } = request;
        let mut models = current.device_models.clone();
        let mut bindings = current.slot_model_bindings.clone();
        bindings.retain(|binding| binding.slot_id != slot_id);

        if create_if_missing && update_existing {
            return Err(ApiError::BadRequest(
                "create_if_missing and update_existing are mutually exclusive".into(),
            ));
        }

        let (binding, model, created) = match model_id {
            None => {
                if create_if_missing
                    || update_existing
                    || name.is_some()
                    || parent_id.is_some()
                    || clear_parent
                    || !aliases.is_empty()
                    || clear_aliases
                    || confirmation_method.is_some()
                    || note.is_some()
                {
                    return Err(ApiError::BadRequest(
                        "detaching a Slot cannot include model definition, confirmation, or note fields"
                            .into(),
                    ));
                }
                (None, None, false)
            }
            Some(model_id) => {
                let existing_index = models.iter().position(|model| model.id == model_id);
                let existing = existing_index.map(|index| models[index].clone());
                let (model, created) = match existing {
                    Some(existing) => {
                        if create_if_missing {
                            if clear_parent || clear_aliases {
                                return Err(ApiError::BadRequest(
                                    "clear_parent and clear_aliases require update_existing".into(),
                                ));
                            }
                            let candidate = DeviceModel {
                                id: model_id.clone(),
                                name: name.clone().ok_or_else(|| {
                                    ApiError::BadRequest(
                                        "create_if_missing requires the model name".into(),
                                    )
                                })?,
                                parent_id: parent_id.clone(),
                                aliases: aliases.clone(),
                            };
                            if candidate != existing {
                                return Err(ApiError::ModelDefinitionConflict { model_id });
                            }
                            (existing, false)
                        } else if update_existing {
                            if !has_expected_revision
                                || expected_current_guard.as_ref() != Some(&Some(model_id.clone()))
                                || current_model.as_deref() != Some(model_id.as_str())
                            {
                                return Err(ApiError::BadRequest(
                                    "update_existing requires expected_revision and expected_current equal to the model currently bound to this Slot"
                                        .into(),
                                ));
                            }
                            if parent_id.is_some() && clear_parent {
                                return Err(ApiError::BadRequest(
                                    "parent_id and clear_parent are mutually exclusive".into(),
                                ));
                            }
                            if !aliases.is_empty() && clear_aliases {
                                return Err(ApiError::BadRequest(
                                    "aliases and clear_aliases are mutually exclusive".into(),
                                ));
                            }
                            if name.is_none()
                                && parent_id.is_none()
                                && !clear_parent
                                && aliases.is_empty()
                                && !clear_aliases
                            {
                                return Err(ApiError::BadRequest(
                                    "update_existing requires at least one model field change"
                                        .into(),
                                ));
                            }
                            let mut updated = existing;
                            if let Some(name) = name {
                                updated.name = name;
                            }
                            if let Some(parent_id) = parent_id {
                                updated.parent_id = Some(parent_id);
                            } else if clear_parent {
                                updated.parent_id = None;
                            }
                            if !aliases.is_empty() {
                                updated.aliases = aliases;
                            } else if clear_aliases {
                                updated.aliases.clear();
                            }
                            models[existing_index.expect("existing index is present")] =
                                updated.clone();
                            (updated, false)
                        } else if name.is_some()
                            || parent_id.is_some()
                            || clear_parent
                            || !aliases.is_empty()
                            || clear_aliases
                        {
                            return Err(ApiError::BadRequest(
                                "model definition fields require create_if_missing or update_existing"
                                    .into(),
                            ));
                        } else {
                            (existing, false)
                        }
                    }
                    None if create_if_missing => {
                        if clear_parent || clear_aliases {
                            return Err(ApiError::BadRequest(
                                "clear_parent and clear_aliases require update_existing".into(),
                            ));
                        }
                        let candidate = DeviceModel {
                            id: model_id.clone(),
                            name: name.ok_or_else(|| {
                                ApiError::BadRequest(
                                    "create_if_missing requires the model name".into(),
                                )
                            })?,
                            parent_id,
                            aliases,
                        };
                        models.push(candidate.clone());
                        (candidate, true)
                    }
                    None if update_existing => {
                        return Err(ApiError::NotFound(format!(
                            "cannot update unknown device model {model_id:?}"
                        )));
                    }
                    None => {
                        return Err(ApiError::NotFound(format!(
                            "unknown device model {model_id:?}"
                        )));
                    }
                };
                let confirmation_method = confirmation_method.ok_or_else(|| {
                    ApiError::BadRequest(
                        "attaching a device model requires confirmation_method".into(),
                    )
                })?;
                let binding = SlotModelBinding {
                    slot_id,
                    model_id: model.id.clone(),
                    confirmation_method,
                    note,
                    updated_wall_time_ns: wall_time_ns(),
                    source,
                };
                bindings.push(binding.clone());
                (Some(binding), Some(model), created)
            }
        };

        let staged = current
            .staged_with_model_state(models, bindings)
            .map_err(ConfigError::from)?;
        self.inner.config_store.save(&staged)?;
        let config_revision = staged.config_revision;
        let mut affected_slots = model.as_ref().map_or_else(Vec::new, |model| {
            staged
                .slot_model_bindings
                .iter()
                .filter(|binding| binding.model_id == model.id)
                .map(|binding| binding.slot_id.clone())
                .collect::<Vec<_>>()
        });
        affected_slots.sort();
        *self.inner.config.write().await = staged;
        Ok(SetSlotDeviceModelResponse {
            binding,
            model,
            created,
            affected_slots,
            config_revision,
        })
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
        let principal = if config.auth_required {
            config
                .auth
                .as_ref()
                .ok_or(ApiError::Auth(AuthError::MissingAuthorization))?
                .authenticate_authorization(authorization)
                .map_err(ApiError::Auth)?
        } else {
            Principal::trusted_local_admin()
        };
        principal.require_role(required).map_err(ApiError::Auth)?;
        Ok(principal)
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
        .route("/api/v1/config/slots", put(configure_slots))
        .route(
            "/api/v1/config/transport-profiles",
            get(list_transport_profiles).put(configure_transport_profiles),
        )
        .route(
            "/api/v1/config/device-profiles",
            get(list_device_profiles).put(configure_device_profiles),
        )
        .route(
            "/api/v1/config/device-models",
            get(list_device_models).put(configure_device_models),
        )
        .route(
            "/api/v1/slots/{slot_id}/device-model",
            put(set_slot_device_model),
        )
        .route("/api/v1/archives", get(archives))
        .route("/api/v1/diagnostics", get(diagnostics))
        .route("/api/v1/diagnostics/storage", get(storage_diagnostics))
        .route("/api/v1/slots/{slot_id}/diagnostics", get(slot_diagnostics))
        .route("/api/v1/slots/{slot_id}/events", get(events))
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
        .route("/api/v1/monitor-events", get(list_monitor_outbox))
        .route(
            "/api/v1/monitor-events/{outbox_seq}/ack",
            post(acknowledge_monitor_outbox),
        )
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
        protocol_version: PROTOCOL_VERSION,
        auth_required: config.auth_required,
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
        protocol_version: PROTOCOL_VERSION,
        config_revision: config.config_revision,
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
    let (slots, config_revision) = tokio::spawn(async move {
        transaction
            .configure_slots_transaction(request.slots, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureSlotsResponse {
        slots,
        config_revision,
    }))
}

async fn list_transport_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<TransportProfileListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await;
    Ok(Json(TransportProfileListResponse {
        profiles: config.transport_profiles.clone(),
        config_revision: config.config_revision,
    }))
}

async fn configure_transport_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConfigureTransportProfilesRequest>,
) -> Result<Json<ConfigureTransportProfilesResponse>, ApiError> {
    state.authenticate(&headers, Role::Admin).await?;
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

async fn list_device_profiles(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DeviceProfileListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await;
    Ok(Json(DeviceProfileListResponse {
        profiles: config.device_profiles.clone(),
        config_revision: config.config_revision,
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
    let (profiles, config_revision) = tokio::spawn(async move {
        transaction
            .configure_device_profiles_transaction(request.profiles, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("configuration transaction task failed".into()))??;
    Ok(Json(ConfigureDeviceProfilesResponse {
        profiles,
        config_revision,
    }))
}

async fn list_device_models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DeviceModelListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await;
    Ok(Json(DeviceModelListResponse {
        models: config.device_models.clone(),
        bindings: config.slot_model_bindings.clone(),
        config_revision: config.config_revision,
    }))
}

async fn configure_device_models(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ConfigureDeviceModelsRequest>,
) -> Result<Json<ConfigureDeviceModelsResponse>, ApiError> {
    state.authenticate(&headers, Role::Admin).await?;
    let transaction = state.clone();
    let (models, bindings, config_revision) = tokio::spawn(async move {
        transaction
            .configure_device_models_transaction(request.models, request.expected_revision)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("device model transaction task failed".into()))??;
    Ok(Json(ConfigureDeviceModelsResponse {
        models,
        bindings,
        config_revision,
    }))
}

async fn set_slot_device_model(
    State(state): State<AppState>,
    Path(slot_id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<SetSlotDeviceModelRequest>,
) -> Result<Json<SetSlotDeviceModelResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
    let transaction = state.clone();
    let response = tokio::spawn(async move {
        transaction
            .set_slot_device_model_transaction(slot_id, request)
            .await
    })
    .await
    .map_err(|_| ApiError::Internal("Slot model binding transaction task failed".into()))??;
    Ok(Json(response))
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

async fn diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<DaemonDiagnosticsResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let config = state.inner.config.read().await.clone();
    let handles = state.inner.registry.handles().await;
    let slots = handles
        .into_iter()
        .map(|handle| SlotDiagnostics {
            snapshot: handle.snapshot(),
            subscriber_count: handle.subscriber_count(),
            subscriber_lag_events: handle.subscriber_lag_events(),
        })
        .collect::<Vec<_>>();
    let mut journal = state.inner.journal.diagnostics().await?;
    if slots
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
        slots,
    }))
}

async fn storage_diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<StorageDiagnosticsResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
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

async fn slot_diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(slot_id): Path<String>,
) -> Result<Json<SlotDiagnostics>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    let handle = state
        .inner
        .registry
        .get(&slot_id)
        .await
        .ok_or_else(|| ApiError::NotFound(format!("unknown Slot {slot_id}")))?;
    Ok(Json(SlotDiagnostics {
        snapshot: handle.snapshot(),
        subscriber_count: handle.subscriber_count(),
        subscriber_lag_events: handle.subscriber_lag_events(),
    }))
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

#[derive(Debug, serde::Deserialize)]
struct MonitorListQuery {
    slot_id: Option<String>,
    status: Option<MonitorStatus>,
}

async fn create_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateMonitorRequest>,
) -> Result<Json<MonitorResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
    Ok(Json(state.inner.monitors.create(request).await?))
}

async fn list_monitors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MonitorListQuery>,
) -> Result<Json<MonitorListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    Ok(Json(
        state
            .inner
            .monitors
            .list(query.slot_id.as_deref(), query.status)
            .await,
    ))
}

async fn get_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(monitor_id): Path<Uuid>,
) -> Result<Json<MonitorResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    Ok(Json(state.inner.monitors.get(monitor_id).await?))
}

async fn update_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(monitor_id): Path<Uuid>,
    Json(request): Json<UpdateMonitorRequest>,
) -> Result<Json<MonitorResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
    Ok(Json(
        state.inner.monitors.update(monitor_id, request).await?,
    ))
}

async fn stop_monitor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(monitor_id): Path<Uuid>,
    Query(query): Query<MonitorMutationQuery>,
) -> Result<Json<MonitorResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
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
    headers: HeaderMap,
    Path(monitor_id): Path<Uuid>,
    Query(query): Query<MonitorIncidentQuery>,
) -> Result<Json<MonitorIncidentListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
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
    headers: HeaderMap,
    Path((monitor_id, incident_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<MonitorIncidentResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
    Ok(Json(MonitorIncidentResponse {
        incident: state
            .inner
            .monitors
            .acknowledge_incident(monitor_id, incident_id)
            .await?,
    }))
}

#[derive(Debug, serde::Deserialize)]
struct MonitorOutboxQuery {
    after_outbox_seq: Option<u64>,
    limit: Option<usize>,
}

async fn list_monitor_outbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<MonitorOutboxQuery>,
) -> Result<Json<MonitorOutboxListResponse>, ApiError> {
    state.authenticate(&headers, Role::Observer).await?;
    Ok(Json(
        state
            .inner
            .monitors
            .outbox(query.after_outbox_seq, query.limit)
            .await,
    ))
}

async fn acknowledge_monitor_outbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(outbox_seq): Path<u64>,
) -> Result<Json<MonitorOutboxEventResponse>, ApiError> {
    state.authenticate(&headers, Role::Operator).await?;
    Ok(Json(MonitorOutboxEventResponse {
        event: state.inner.monitors.acknowledge_outbox(outbox_seq).await?,
    }))
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
            expected_run_id,
            pacing,
            description,
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
                    handle.record_subscriber_lag(skipped);
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
        | ClientMessage::SendBreak { slot_id, .. }
        | ClientMessage::TriggerStart { slot_id, .. }
        | ClientMessage::TriggerStatus { slot_id, .. }
        | ClientMessage::TriggerCancel { slot_id, .. }
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
                | SlotError::WriteRunMissing { .. }
                | SlotError::WriteRunMismatch { .. }
                | SlotError::WriteRunNotOwner { .. }
                | SlotError::PartialWrite { .. }
                | SlotError::TriggerActive
                | SlotError::TriggerNotOwner { .. }
                | SlotError::TriggerEpochMismatch
                | SlotError::TriggerGenerationMismatch
                | SlotError::RequestIdReused => (ErrorCode::Conflict, false),
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
    #[error("Slot model binding changed: expected {expected:?}, current {actual:?}")]
    ModelBindingMismatch {
        expected: Option<String>,
        actual: Option<String>,
    },
    #[error("device model {model_id:?} already exists with a different definition")]
    ModelDefinitionConflict { model_id: String },
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::Auth(AuthError::Forbidden) => (StatusCode::FORBIDDEN, ErrorCode::Forbidden),
            Self::Auth(_) => (StatusCode::UNAUTHORIZED, ErrorCode::Unauthorized),
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
            Self::Journal(JournalError::InvalidConfig(_) | JournalError::InvalidSlotId) => {
                (StatusCode::BAD_REQUEST, ErrorCode::BadRequest)
            }
            Self::Journal(JournalError::InvalidRegex(_)) => {
                (StatusCode::BAD_REQUEST, ErrorCode::RegexInvalid)
            }
            Self::Journal(JournalError::QueryBudgetExceeded { .. }) => (
                StatusCode::TOO_MANY_REQUESTS,
                ErrorCode::QueryBudgetExceeded,
            ),
            Self::Monitor(
                MonitorError::NotFound(_)
                | MonitorError::OutboxNotFound(_)
                | MonitorError::UnknownSlot(_),
            ) => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
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
            Self::ModelBindingMismatch { .. } | Self::ModelDefinitionConflict { .. } => {
                (StatusCode::CONFLICT, ErrorCode::Conflict)
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
    use super::*;
    use crate::config::{ConfigPaths, ConfigStore};
    use crate::control::ControlLimits;
    use crate::journal::{JournalConfig, JournalManager};
    use serial_protocol::{ModelConfirmationMethod, SerialSettings, SlotConfig, TransportProfile};

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

    fn transport_profile(name: &str, baud_rate: u32) -> TransportProfile {
        let settings = SerialSettings::default();
        TransportProfile {
            name: name.into(),
            baud_rate,
            data_bits: settings.data_bits,
            parity: settings.parity,
            stop_bits: settings.stop_bits,
            flow_control: settings.flow_control,
            dtr: settings.dtr,
            rts: settings.rts,
            auto_open: settings.auto_open,
        }
    }

    #[test]
    fn insufficient_write_lease_is_a_retryable_conflict() {
        let error = WsError::Slot(SlotError::WriteLeaseTooShort {
            remaining_ms: 2_099,
            write_ms: 2_000,
            margin_ms: 100,
        });
        assert_eq!(error.protocol_code(), (ErrorCode::Conflict, true));
        assert!(error.to_string().contains("renew control"));
    }

    #[test]
    fn expected_run_write_rejection_is_a_definite_nonretryable_conflict() {
        let run_id = Uuid::new_v4();
        let error = WsError::Slot(SlotError::WriteRunMissing {
            expected_run_id: run_id,
        });
        assert_eq!(error.protocol_code(), (ErrorCode::Conflict, false));
        assert!(error.to_string().contains(&run_id.to_string()));
        assert!(error.to_string().contains("(no bytes were written)"));
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
        let initial_revision = loaded.config.config_revision;
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            loaded.config.slots.clone(),
            loaded.config.transport_profiles.clone(),
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

        let (snapshots, revision) = state
            .configure_slots_transaction(requested.clone(), None)
            .await
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].config, requested[0]);
        assert_eq!(revision, initial_revision + 1);
        assert_eq!(state.inner.config.read().await.slots, requested);
        assert_eq!(store.load().unwrap().slots, requested);

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_configuration_revision_is_rejected_before_any_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let revision = loaded.config.config_revision;
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
            .configure_slots_transaction(
                vec![disabled_slot("slot-1", "Slot 1", "COM3")],
                Some(revision + 1),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApiError::ConfigRevisionMismatch {
                expected,
                actual
            } if expected == revision + 1 && actual == revision
        ));
        assert!(state.inner.registry.snapshots().await.is_empty());
        assert_eq!(state.inner.config.read().await.config_revision, revision);
        assert_eq!(store.load().unwrap().config_revision, revision);

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn transport_profile_update_publishes_effective_uart_settings_and_revision() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut loaded = store.load_or_create().unwrap();
        let configured_slot = disabled_slot("slot-1", "Slot 1", "COM3");
        store
            .update_slots(&mut loaded.config, vec![configured_slot.clone()])
            .unwrap();
        let revision = loaded.config.config_revision;
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            vec![configured_slot],
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
        let profile = transport_profile("generic-115200", 230_400);

        let (profiles, updated_revision) = state
            .configure_transport_profiles_transaction(vec![profile.clone()], Some(revision))
            .await
            .unwrap();
        assert_eq!(profiles, vec![profile.clone()]);
        assert_eq!(updated_revision, revision + 1);
        assert_eq!(
            store.load().unwrap().transport_profiles,
            vec![profile.clone()]
        );
        let snapshot = state.inner.registry.get("slot-1").await.unwrap().snapshot();
        assert_eq!(
            snapshot.effective_transport.unwrap().baud_rate,
            profile.baud_rate
        );
        // The Slot snapshot remains backward compatible; the effective bundle
        // is authoritative when a transport catalog is attached.
        assert_eq!(snapshot.config.settings.baud_rate, 115_200);

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn model_creation_and_slot_binding_commit_atomically_and_honor_current_guard() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut loaded = store.load_or_create().unwrap();
        let configured_slot = disabled_slot("slot-1", "Slot 1", "COM3");
        store
            .update_slots(&mut loaded.config, vec![configured_slot.clone()])
            .unwrap();
        let revision = loaded.config.config_revision;
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            vec![configured_slot],
            loaded.config.transport_profiles.clone(),
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

        let response = state
            .set_slot_device_model_transaction(
                "slot-1".into(),
                SetSlotDeviceModelRequest {
                    model_id: Some("tl-as7230-w".into()),
                    create_if_missing: true,
                    update_existing: false,
                    name: Some("TL-AS7230-W".into()),
                    parent_id: None,
                    clear_parent: false,
                    aliases: vec!["7230W".into()],
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Serial),
                    note: Some("confirmed with show version".into()),
                    source: "human:test".into(),
                    expected_revision: Some(revision),
                    expected_current: Some(None),
                },
            )
            .await
            .unwrap();
        assert!(response.created);
        assert_eq!(response.config_revision, revision + 1);
        assert_eq!(response.model.unwrap().id, "tl-as7230-w");
        assert_eq!(response.binding.unwrap().model_id, "tl-as7230-w");

        let persisted = store.load().unwrap();
        assert_eq!(persisted.device_models.len(), 1);
        assert_eq!(persisted.slot_model_bindings.len(), 1);
        assert_eq!(
            state.inner.config.read().await.slot_model_bindings,
            persisted.slot_model_bindings
        );

        let error = state
            .set_slot_device_model_transaction(
                "slot-1".into(),
                SetSlotDeviceModelRequest {
                    model_id: None,
                    create_if_missing: false,
                    update_existing: false,
                    name: None,
                    parent_id: None,
                    clear_parent: false,
                    aliases: Vec::new(),
                    clear_aliases: false,
                    confirmation_method: None,
                    note: None,
                    source: "human:test".into(),
                    expected_revision: Some(response.config_revision),
                    expected_current: Some(Some("different-model".into())),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ApiError::ModelBindingMismatch { .. }));
        assert_eq!(
            state.inner.config.read().await.config_revision,
            response.config_revision
        );
        assert_eq!(
            store.load().unwrap().config_revision,
            response.config_revision
        );

        let malformed_detach = state
            .set_slot_device_model_transaction(
                "slot-1".into(),
                SetSlotDeviceModelRequest {
                    model_id: None,
                    create_if_missing: false,
                    update_existing: false,
                    name: None,
                    parent_id: None,
                    clear_parent: false,
                    aliases: Vec::new(),
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Human),
                    note: Some("irrelevant on detach".into()),
                    source: "human:test".into(),
                    expected_revision: Some(response.config_revision),
                    expected_current: Some(Some("tl-as7230-w".into())),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(malformed_detach, ApiError::BadRequest(_)));
        assert_eq!(
            store.load().unwrap().config_revision,
            response.config_revision
        );

        let updated = state
            .set_slot_device_model_transaction(
                "slot-1".into(),
                SetSlotDeviceModelRequest {
                    model_id: Some("tl-as7230-w".into()),
                    create_if_missing: false,
                    update_existing: true,
                    name: Some("TL-AS7230-W rev2".into()),
                    parent_id: None,
                    clear_parent: false,
                    aliases: vec!["7230-W".into()],
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Web),
                    note: Some("confirmed in device web UI".into()),
                    source: "agent:test".into(),
                    expected_revision: Some(response.config_revision),
                    expected_current: Some(Some("tl-as7230-w".into())),
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.model.as_ref().unwrap().name, "TL-AS7230-W rev2");
        assert_eq!(updated.model.as_ref().unwrap().aliases, ["7230-W"]);
        assert_eq!(updated.affected_slots, ["slot-1"]);
        assert!(!updated.created);

        let cycle = state
            .set_slot_device_model_transaction(
                "slot-1".into(),
                SetSlotDeviceModelRequest {
                    model_id: Some("tl-as7230-w".into()),
                    create_if_missing: false,
                    update_existing: true,
                    name: None,
                    parent_id: Some("tl-as7230-w".into()),
                    clear_parent: false,
                    aliases: Vec::new(),
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Serial),
                    note: None,
                    source: "agent:test".into(),
                    expected_revision: Some(updated.config_revision),
                    expected_current: Some(Some("tl-as7230-w".into())),
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            cycle,
            ApiError::Config(ConfigError::Validation(
                crate::config::ConfigValidationError::DeviceModelCycle { .. }
            ))
        ));
        assert_eq!(
            store.load().unwrap().config_revision,
            updated.config_revision
        );

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
            .configure_slots_transaction(
                vec![
                    disabled_slot("slot-1", "One", "COM3"),
                    disabled_slot("slot-1", "Duplicate", "COM4"),
                ],
                None,
            )
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
                .configure_slots_transaction(vec![disabled_slot("slot-1", "First", "COM3")], None)
                .await
        };
        let second = async move {
            second_state
                .configure_slots_transaction(vec![disabled_slot("slot-1", "Second", "COM4")], None)
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
            .configure_slots_transaction(vec![disabled_slot("slot-new", "New", "COM4")], None)
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

    #[test]
    fn runtime_commit_failure_restores_previous_persisted_configuration() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let previous = loaded.config;
        let staged = previous
            .staged_with_slots(vec![disabled_slot("slot-1", "Slot 1", "COM3")])
            .unwrap();
        store.save(&staged).unwrap();
        assert_eq!(
            store.load().unwrap().config_revision,
            staged.config_revision
        );

        let error = compensate_commit_failure(&store, &previous, RegistryError::Shutdown);
        assert!(matches!(error, ApiError::Registry(RegistryError::Shutdown)));
        let restored = store.load().unwrap();
        assert_eq!(restored.config_revision, previous.config_revision);
        assert!(restored.slots.is_empty());
    }

    fn sigmastar_profile() -> serial_protocol::DeviceProfile {
        serial_protocol::DeviceProfile {
            name: "sigmastar-evb".into(),
            shell_prompt: Some("root@sigmastar:/# ".into()),
            uboot_prompt: Some("SigmaStar =>".into()),
            write_eol: Some("\n".into()),
            echo: Some(serial_protocol::EchoMode::Off),
            write_chunk_size: None,
            write_chunk_delay_ms: None,
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
            Vec::new(),
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

        let handle = state.inner.registry.get("slot-1").await.unwrap();
        let mut live = handle.attach(None, 0).await.unwrap().live;
        let snapshot = handle.snapshot();
        assert_eq!(
            snapshot.effective_shell_prompt.as_deref(),
            Some("root@sigmastar:/# ")
        );
        assert_eq!(
            snapshot.effective_uboot_prompt.as_deref(),
            Some("SigmaStar =>")
        );
        assert_eq!(snapshot.effective_write_eol.as_deref(), Some("\n"));
        assert_eq!(
            snapshot.effective_echo,
            Some(serial_protocol::EchoMode::Off)
        );

        // Replacing the catalog validates against existing Slots, persists,
        // and refreshes live snapshots without touching ports.
        let mut updated = sigmastar_profile();
        updated.uboot_prompt = Some("SigmaStar #".into());
        let (profiles, revision) = state
            .configure_device_profiles_transaction(vec![updated.clone()], None)
            .await
            .unwrap();
        assert_eq!(profiles, vec![updated.clone()]);
        assert_eq!(revision, state.inner.config.read().await.config_revision);
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
        let event = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .expect("attached consumer should receive the profile refresh")
            .unwrap();
        assert_eq!(event.kind, serial_protocol::EventKind::SlotReconfigured);
        assert_eq!(
            event
                .metadata
                .get("profile_only")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        let effective: serial_protocol::ResolvedDeviceSettings = serde_json::from_value(
            event
                .metadata
                .get("effective")
                .cloned()
                .expect("effective settings metadata"),
        )
        .unwrap();
        assert_eq!(effective.uboot_prompt.as_deref(), Some("SigmaStar #"));

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn device_profile_stage_failure_leaves_every_committed_view_unchanged() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let started = Instant::now();
        let journal =
            JournalManager::open(JournalConfig::new(temporary.path().join("runtime-journal")))
                .unwrap();
        let mut healthy = disabled_slot("slot-a", "Healthy", "COM3");
        healthy.device_profile = Some("sigmastar-evb".into());
        let mut stopped = disabled_slot("slot-z", "Stopped", "COM4");
        stopped.device_profile = Some("sigmastar-evb".into());
        let slots = vec![healthy, stopped];
        let old_profile = sigmastar_profile();
        let registry = SlotRegistry::new(
            loaded.daemon_epoch,
            started,
            journal.handle(),
            slots.clone(),
            Vec::new(),
            vec![old_profile.clone()],
            ControlLimits::default(),
        );
        let mut config = loaded.config.clone();
        config.slots = slots;
        config.device_profiles = vec![old_profile.clone()];
        store.save(&config).unwrap();
        let state = AppState::new(
            store.clone(),
            config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );

        let healthy = state.inner.registry.get("slot-a").await.unwrap();
        let mut live = healthy.attach(None, 0).await.unwrap().live;
        state
            .inner
            .registry
            .get("slot-z")
            .await
            .unwrap()
            .shutdown()
            .await;
        let mut updated = old_profile.clone();
        updated.uboot_prompt = Some("SigmaStar #".into());

        let error = state
            .configure_device_profiles_transaction(vec![updated], None)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ApiError::Registry(RegistryError::Slot(SlotError::Closed))
        ));
        assert_eq!(
            state.inner.config.read().await.device_profiles,
            vec![old_profile.clone()]
        );
        assert_eq!(
            store.load().unwrap().device_profiles,
            vec![old_profile.clone()]
        );
        let snapshot = healthy.snapshot();
        assert_eq!(
            snapshot.effective_uboot_prompt,
            old_profile.uboot_prompt.clone()
        );
        assert_eq!(snapshot.head_seq, 0);
        assert!(matches!(
            live.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

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
            Vec::new(),
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
            .configure_device_profiles_transaction(Vec::new(), None)
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
    async fn slot_without_device_profile_does_not_guess_device_prompts() {
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
            Vec::new(),
            ControlLimits::default(),
        );
        let snapshot = registry.get("slot-1").await.unwrap().snapshot();
        assert!(snapshot.effective_shell_prompt.is_none());
        assert!(snapshot.effective_uboot_prompt.is_none());

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn diagnostics_are_read_only_for_slots_and_journal() {
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
            Vec::new(),
            ControlLimits::default(),
        );
        let state = AppState::new(
            store,
            loaded.config,
            registry,
            journal.handle(),
            loaded.daemon_epoch,
            started,
        );
        let handle = state.inner.registry.get("slot-1").await.unwrap();
        let before = handle.snapshot();
        let headers = HeaderMap::new();

        let daemon = diagnostics(State(state.clone()), headers.clone())
            .await
            .unwrap()
            .0;
        let storage = storage_diagnostics(State(state.clone()), headers.clone())
            .await
            .unwrap()
            .0;
        let slot = slot_diagnostics(State(state.clone()), headers, Path("slot-1".to_owned()))
            .await
            .unwrap()
            .0;

        assert_eq!(daemon.slots.len(), 1);
        assert_eq!(
            daemon.config_revision,
            state.inner.config.read().await.config_revision
        );
        assert_eq!(daemon.journal, storage.journal);
        assert_eq!(slot.snapshot, before);
        assert_eq!(slot.subscriber_count, 0);
        assert_eq!(slot.subscriber_lag_events, 0);
        assert_eq!(handle.snapshot(), before);

        state.shutdown().await;
        journal.shutdown().await.unwrap();
    }
}
