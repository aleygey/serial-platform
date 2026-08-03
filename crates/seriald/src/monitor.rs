//! Durable, device-agnostic live RX Monitor Jobs.
//!
//! Monitor matching happens on independent broadcast receivers after Slot
//! actors publish timeline events. Neither persistence nor webhook delivery
//! can block the serial reader or the Slot actor.

use crate::config::{MonitorEventSinkConfig, atomic_write};
use crate::registry::SlotRegistry;
use regex::bytes::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serial_protocol::{
    CreateMonitorRequest, Cursor, Direction, EventKind, MonitorCloudEvent, MonitorIncident,
    MonitorIncidentListResponse, MonitorListResponse, MonitorOutboxEvent,
    MonitorOutboxListResponse, MonitorOutboxStatus, MonitorResponse, MonitorSpec, MonitorStatus,
    MonitorView, TimelineEvent, UpdateMonitorRequest,
};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, broadcast, watch};
use tokio::task::JoinHandle;
use uuid::Uuid;

const STATE_SCHEMA_VERSION: u32 = 1;
const MAX_STATE_FILE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_MONITORS: usize = 128;
const MAX_ACTIVE_MONITORS: usize = 64;
const MAX_INCIDENTS_PER_MONITOR: usize = 512;
const MAX_INCIDENTS_TOTAL: usize = 1_024;
const MAX_OUTBOX_EVENTS: usize = 512;
const MAX_PATTERN_BYTES: usize = 4_096;
// Keep the daemon contract aligned with the MCP schema. More importantly,
// this participates in the persisted-state budget below because descriptions
// are copied into both retained Incidents and notification events.
const MAX_DESCRIPTION_BYTES: usize = 1_024;
const MAX_PREVIEW_BYTES: usize = 1_024;
const MAX_MATCH_WINDOW_BYTES: usize = 64 * 1024;
const MAX_REGEX_COMPILED_BYTES: usize = 2 * 1024 * 1024;
const MAX_DEBOUNCE_MS: u64 = 60_000;
const MAX_COOLDOWN_MS: u64 = 24 * 60 * 60 * 1_000;
const MAX_DURATION_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const MAX_EVENT_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
const DEFAULT_PAGE: usize = 100;
const MAX_PAGE: usize = 200;
/// This bounds disk work on the Monitor worker; the Slot RX actor never waits
/// for a checkpoint and broadcast/ring replay cover a worker that falls behind.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);

// Conservative JSON-size envelopes include worst-case JSON escaping for all
// bounded strings plus fixed field/pretty-print overhead. Keep the legal state
// comfortably below the hard file limit so reaching a documented retention
// bound cannot itself make the next atomic persistence fail.
const ESTIMATED_MONITOR_AND_CHECKPOINT_BYTES: u64 = 64 * 1024;
const ESTIMATED_INCIDENT_BYTES: u64 = 24 * 1024;
const ESTIMATED_OUTBOX_EVENT_BYTES: u64 = 40 * 1024;
const ESTIMATED_STATE_FIXED_BYTES: u64 = 1024 * 1024;
const MAX_ESTIMATED_STATE_BYTES: u64 = ESTIMATED_STATE_FIXED_BYTES
    + MAX_MONITORS as u64 * ESTIMATED_MONITOR_AND_CHECKPOINT_BYTES
    + MAX_INCIDENTS_TOTAL as u64 * ESTIMATED_INCIDENT_BYTES
    + MAX_OUTBOX_EVENTS as u64 * ESTIMATED_OUTBOX_EVENT_BYTES;
const _: () = assert!(MAX_ESTIMATED_STATE_BYTES < MAX_STATE_FILE_BYTES);

#[derive(Clone)]
pub struct MonitorManager {
    inner: Arc<MonitorInner>,
}

struct MonitorInner {
    path: PathBuf,
    registry: SlotRegistry,
    server_id: Uuid,
    state: RwLock<PersistedState>,
    mutation: Mutex<()>,
    workers: Mutex<HashMap<Uuid, WorkerHandle>>,
    sink: MonitorEventSinkConfig,
    startup_task: StdMutex<Option<JoinHandle<()>>>,
    sink_task: Mutex<Option<JoinHandle<()>>>,
    shutdown: watch::Sender<bool>,
    #[cfg(test)]
    fail_persists: std::sync::atomic::AtomicBool,
}

struct WorkerHandle {
    token: Uuid,
    revision: u64,
    cancel: watch::Sender<bool>,
    /// `None` is a reservation installed before the task is spawned. Stop or
    /// replacement can therefore cancel a worker even in the small window
    /// between its authoritative state check and `tokio::spawn`.
    task: Option<JoinHandle<()>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PersistedState {
    schema_version: u32,
    #[serde(default)]
    monitors: BTreeMap<Uuid, MonitorView>,
    #[serde(default)]
    incidents: BTreeMap<Uuid, VecDeque<MonitorIncident>>,
    /// The replay-safe cursor and cooldown barrier. `MonitorView.current_cursor`
    /// is live status and can be newer than this durable checkpoint.
    #[serde(default)]
    checkpoints: BTreeMap<Uuid, MonitorCheckpoint>,
    #[serde(default)]
    outbox: VecDeque<StoredOutboxEvent>,
    #[serde(default = "default_next_outbox_seq")]
    next_outbox_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MonitorCheckpoint {
    cursor: Cursor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cooldown_until_wall_time_ns: Option<i64>,
    /// Debounced evidence is durable so a restart or generation boundary can
    /// form an Incident rather than silently discarding a matched candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending: Option<PendingIncident>,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            monitors: BTreeMap::new(),
            incidents: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            outbox: VecDeque::new(),
            next_outbox_seq: default_next_outbox_seq(),
        }
    }
}

const fn default_next_outbox_seq() -> u64 {
    1
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredOutboxEvent {
    public: MonitorOutboxEvent,
    next_attempt_wall_time_ns: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum MonitorError {
    #[error("Monitor Job {0} was not found")]
    NotFound(Uuid),
    #[error("Monitor outbox event {0} was not found")]
    OutboxNotFound(u64),
    #[error("unknown Slot {0}")]
    UnknownSlot(String),
    #[error("Monitor request_id {0} was reused with a different specification")]
    RequestIdReused(Uuid),
    #[error("Monitor revision mismatch (expected {expected}, current {actual})")]
    RevisionMismatch { expected: u64, actual: u64 },
    #[error("Monitor capacity is exhausted (maximum {MAX_MONITORS} retained Jobs)")]
    Capacity,
    #[error("active Monitor capacity is exhausted (maximum {MAX_ACTIVE_MONITORS})")]
    ActiveCapacity,
    #[error("invalid Monitor specification: {0}")]
    InvalidSpec(String),
    #[error("Monitor cursor is ahead of the current Slot timeline")]
    CursorAhead,
    #[error("Monitor state is corrupt or incompatible: {0}")]
    InvalidState(String),
    #[error("Monitor state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Monitor state serialization failed")]
    Serialization,
    #[error("Monitor task failed: {0}")]
    Runtime(String),
}

impl MonitorManager {
    pub fn open(
        path: PathBuf,
        registry: SlotRegistry,
        _daemon_epoch: Uuid,
        server_id: Uuid,
        sink: MonitorEventSinkConfig,
    ) -> Result<Self, MonitorError> {
        let state = load_state(&path)?;
        validate_loaded_state(&state)?;
        let (shutdown, _) = watch::channel(false);
        let manager = Self {
            inner: Arc::new(MonitorInner {
                path,
                registry,
                server_id,
                state: RwLock::new(state),
                mutation: Mutex::new(()),
                workers: Mutex::new(HashMap::new()),
                sink,
                startup_task: StdMutex::new(None),
                sink_task: Mutex::new(None),
                shutdown,
                #[cfg(test)]
                fail_persists: std::sync::atomic::AtomicBool::new(false),
            }),
        };
        let startup = manager.clone();
        let task = tokio::spawn(async move {
            if startup.is_shutting_down() {
                return;
            }
            startup.resume_workers().await;
            if !startup.is_shutting_down() {
                startup.start_sink_worker().await;
            }
        });
        *manager
            .inner
            .startup_task
            .lock()
            .expect("Monitor startup task lock poisoned") = Some(task);
        Ok(manager)
    }

    pub async fn create(
        &self,
        request: CreateMonitorRequest,
    ) -> Result<MonitorResponse, MonitorError> {
        validate_spec(&request.spec)?;
        // A retry of an already durable create must not depend on current
        // hardware presence. Check the idempotency key before consulting the
        // live Slot registry, while holding the mutation gate so two creates
        // with the same request ID cannot race.
        let _mutation = self.inner.mutation.lock().await;
        if let Some(existing) = self
            .inner
            .state
            .read()
            .await
            .monitors
            .get(&request.request_id)
        {
            return if same_create_spec(&existing.spec, &request.spec) {
                Ok(MonitorResponse {
                    monitor: existing.clone(),
                })
            } else {
                Err(MonitorError::RequestIdReused(request.request_id))
            };
        }
        let handle = self
            .inner
            .registry
            .get(&request.spec.slot_id)
            .await
            .ok_or_else(|| MonitorError::UnknownSlot(request.spec.slot_id.clone()))?;
        let snapshot = handle.snapshot();
        let mut spec = request.spec;
        let cursor = resolve_start_cursor(&spec, snapshot.daemon_epoch, snapshot.head_seq)?;
        spec.start_cursor = Some(cursor.clone());
        let previous = self.inner.state.read().await.clone();
        let active_count = previous
            .monitors
            .values()
            .filter(|monitor| monitor.status == MonitorStatus::Running)
            .count();
        if active_count >= MAX_ACTIVE_MONITORS {
            return Err(MonitorError::ActiveCapacity);
        }

        let now = wall_time_ns();
        let expires_wall_time_ns = duration_deadline(now, spec.duration_ms);
        let monitor = MonitorView {
            id: request.request_id,
            revision: 1,
            spec,
            status: MonitorStatus::Running,
            created_wall_time_ns: now,
            started_wall_time_ns: now,
            expires_wall_time_ns,
            stopped_wall_time_ns: None,
            current_cursor: Some(cursor.clone()),
            incident_count: 0,
            unacked_incident_count: 0,
            gap_count: 0,
            last_error: None,
        };
        {
            let mut state = self.inner.state.write().await;
            prune_monitor_capacity(&mut state, now);
            if state.monitors.len() >= MAX_MONITORS {
                return Err(MonitorError::Capacity);
            }
            state.monitors.insert(monitor.id, monitor.clone());
            state.incidents.entry(monitor.id).or_default();
            state.checkpoints.insert(
                monitor.id,
                MonitorCheckpoint {
                    cursor: cursor.clone(),
                    cooldown_until_wall_time_ns: None,
                    pending: None,
                },
            );
        }
        if let Err(error) = self.persist().await {
            *self.inner.state.write().await = previous;
            return Err(error);
        }
        drop(_mutation);
        self.spawn_worker(monitor.id, monitor.revision).await;
        Ok(MonitorResponse { monitor })
    }

    pub async fn update(
        &self,
        monitor_id: Uuid,
        request: UpdateMonitorRequest,
    ) -> Result<MonitorResponse, MonitorError> {
        validate_spec(&request.spec)?;
        let handle = self
            .inner
            .registry
            .get(&request.spec.slot_id)
            .await
            .ok_or_else(|| MonitorError::UnknownSlot(request.spec.slot_id.clone()))?;
        let snapshot = handle.snapshot();
        let mut spec = request.spec;
        let cursor = resolve_start_cursor(&spec, snapshot.daemon_epoch, snapshot.head_seq)?;
        spec.start_cursor = Some(cursor.clone());
        let expected_revision = request.expected_revision;
        let current = self.get_view(monitor_id).await?;
        if current.revision != expected_revision {
            return Err(MonitorError::RevisionMismatch {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        self.stop_worker(monitor_id, expected_revision).await;
        let now = wall_time_ns();
        let previous = current;
        let mut updated = previous.clone();
        updated.revision = updated.revision.saturating_add(1);
        updated.spec = spec;
        updated.status = MonitorStatus::Running;
        updated.started_wall_time_ns = now;
        updated.expires_wall_time_ns = duration_deadline(now, updated.spec.duration_ms);
        updated.stopped_wall_time_ns = None;
        updated.current_cursor = Some(cursor.clone());
        updated.last_error = None;
        if let Err(error) = self
            .mutate_and_persist(|state| {
                let actual = state
                    .monitors
                    .get(&monitor_id)
                    .ok_or(MonitorError::NotFound(monitor_id))?
                    .revision;
                if actual != expected_revision {
                    return Err(MonitorError::RevisionMismatch {
                        expected: expected_revision,
                        actual,
                    });
                }
                let active = state
                    .monitors
                    .values()
                    .filter(|monitor| monitor.status == MonitorStatus::Running)
                    .count();
                let was_running = state
                    .monitors
                    .get(&monitor_id)
                    .is_some_and(|monitor| monitor.status == MonitorStatus::Running);
                if !was_running && active >= MAX_ACTIVE_MONITORS {
                    return Err(MonitorError::ActiveCapacity);
                }
                state.monitors.insert(monitor_id, updated.clone());
                state.checkpoints.insert(
                    monitor_id,
                    MonitorCheckpoint {
                        cursor: cursor.clone(),
                        cooldown_until_wall_time_ns: None,
                        pending: None,
                    },
                );
                Ok(())
            })
            .await
        {
            self.spawn_worker(monitor_id, expected_revision).await;
            return Err(error);
        }
        self.spawn_worker(monitor_id, updated.revision).await;
        Ok(MonitorResponse { monitor: updated })
    }

    pub async fn stop(
        &self,
        monitor_id: Uuid,
        expected_revision: u64,
    ) -> Result<MonitorResponse, MonitorError> {
        let current = self.get_view(monitor_id).await?;
        if current.revision != expected_revision {
            return Err(MonitorError::RevisionMismatch {
                expected: expected_revision,
                actual: current.revision,
            });
        }
        self.stop_worker(monitor_id, expected_revision).await;
        let now = wall_time_ns();
        if let Err(error) = self
            .mutate_and_persist(|state| {
                let monitor = state
                    .monitors
                    .get_mut(&monitor_id)
                    .ok_or(MonitorError::NotFound(monitor_id))?;
                if monitor.revision != expected_revision {
                    return Err(MonitorError::RevisionMismatch {
                        expected: expected_revision,
                        actual: monitor.revision,
                    });
                }
                if monitor.status == MonitorStatus::Running {
                    monitor.status = MonitorStatus::Stopped;
                    monitor.stopped_wall_time_ns = Some(now);
                    monitor.revision = monitor.revision.saturating_add(1);
                }
                Ok(())
            })
            .await
        {
            // `mutate_and_persist` restored the durable Running state. Put its
            // worker back before surfacing the failed stop so state and runtime
            // cannot diverge after a transient storage error.
            self.spawn_worker(monitor_id, expected_revision).await;
            return Err(error);
        }
        // Close the race with a startup/recovery spawn that observed the old
        // Running revision immediately before this stop committed.
        self.stop_worker(monitor_id, expected_revision).await;
        Ok(MonitorResponse {
            monitor: self.get_view(monitor_id).await?,
        })
    }

    pub async fn get(&self, monitor_id: Uuid) -> Result<MonitorResponse, MonitorError> {
        Ok(MonitorResponse {
            monitor: self.get_view(monitor_id).await?,
        })
    }

    pub async fn list(
        &self,
        slot_id: Option<&str>,
        status: Option<MonitorStatus>,
    ) -> MonitorListResponse {
        let mut monitors = self
            .inner
            .state
            .read()
            .await
            .monitors
            .values()
            .filter(|monitor| slot_id.is_none_or(|slot| monitor.spec.slot_id == slot))
            .filter(|monitor| status.is_none_or(|value| monitor.status == value))
            .cloned()
            .collect::<Vec<_>>();
        monitors.sort_by_key(|monitor| (monitor.created_wall_time_ns, monitor.id));
        MonitorListResponse { monitors }
    }

    pub async fn incidents(
        &self,
        monitor_id: Uuid,
        after_incident_seq: Option<u64>,
        limit: Option<usize>,
        include_acked: bool,
    ) -> Result<MonitorIncidentListResponse, MonitorError> {
        let state = self.inner.state.read().await;
        let monitor = state
            .monitors
            .get(&monitor_id)
            .ok_or(MonitorError::NotFound(monitor_id))?;
        let high_water = monitor.incident_count;
        let all = state
            .incidents
            .get(&monitor_id)
            .cloned()
            .unwrap_or_default();
        let limit = limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
        let first_available_incident_seq = all.front().map(|incident| incident.incident_seq);
        let retention_gap = after_incident_seq.is_some_and(|after| {
            first_available_incident_seq
                .map_or(after < high_water, |first| after < first.saturating_sub(1))
        });
        let eligible = all
            .into_iter()
            .filter(|incident| include_acked || incident.acked_wall_time_ns.is_none())
            .collect::<Vec<_>>();
        let eligible_len = eligible.len();
        let selected = if let Some(after) = after_incident_seq {
            eligible
                .into_iter()
                .filter(|incident| incident.incident_seq > after)
                .collect::<Vec<_>>()
        } else {
            let skip = eligible.len().saturating_sub(limit);
            eligible.into_iter().skip(skip).collect::<Vec<_>>()
        };
        let truncated = after_incident_seq.is_some() && selected.len() > limit
            || after_incident_seq.is_none() && eligible_len > limit;
        let incidents = selected.into_iter().take(limit).collect::<Vec<_>>();
        // `next_cursor` is also the observed high-water mark. In particular,
        // an empty page caused by filtering ACKed Incidents must still advance
        // a poller instead of making it scan the same filtered range forever.
        let next_cursor = if after_incident_seq.is_none() {
            Some(high_water).filter(|cursor| *cursor > 0)
        } else if truncated {
            incidents.last().map(|incident| incident.incident_seq)
        } else {
            Some(after_incident_seq.unwrap_or_default().max(high_water))
        };
        Ok(MonitorIncidentListResponse {
            incidents,
            next_cursor,
            truncated,
            first_available_incident_seq,
            retention_gap,
        })
    }

    pub async fn acknowledge_incident(
        &self,
        monitor_id: Uuid,
        incident_id: Uuid,
    ) -> Result<MonitorIncident, MonitorError> {
        let now = wall_time_ns();
        self.mutate_and_persist(|state| {
            let incidents = state
                .incidents
                .get_mut(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            let incident = incidents
                .iter_mut()
                .find(|incident| incident.id == incident_id)
                .ok_or(MonitorError::NotFound(incident_id))?;
            if incident.acked_wall_time_ns.is_none() {
                incident.acked_wall_time_ns = Some(now);
                if let Some(monitor) = state.monitors.get_mut(&monitor_id) {
                    monitor.unacked_incident_count =
                        monitor.unacked_incident_count.saturating_sub(1);
                }
                for entry in &mut state.outbox {
                    if entry.public.event.id == incident_id.to_string()
                        && entry.public.status == MonitorOutboxStatus::Pending
                    {
                        entry.public.status = MonitorOutboxStatus::Acknowledged;
                    }
                }
            }
            Ok(())
        })
        .await?;
        let state = self.inner.state.read().await;
        state
            .incidents
            .get(&monitor_id)
            .and_then(|incidents| incidents.iter().find(|incident| incident.id == incident_id))
            .cloned()
            .ok_or(MonitorError::NotFound(incident_id))
    }

    async fn get_view(&self, monitor_id: Uuid) -> Result<MonitorView, MonitorError> {
        self.inner
            .state
            .read()
            .await
            .monitors
            .get(&monitor_id)
            .cloned()
            .ok_or(MonitorError::NotFound(monitor_id))
    }

    async fn mutate_and_persist(
        &self,
        mutate: impl FnOnce(&mut PersistedState) -> Result<(), MonitorError>,
    ) -> Result<(), MonitorError> {
        let _mutation = self.inner.mutation.lock().await;
        let previous = self.inner.state.read().await.clone();
        {
            let mut state = self.inner.state.write().await;
            mutate(&mut state)?;
        }
        if let Err(error) = self.persist().await {
            *self.inner.state.write().await = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn persist(&self) -> Result<(), MonitorError> {
        #[cfg(test)]
        if self
            .inner
            .fail_persists
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(MonitorError::Runtime(
                "injected Monitor persistence failure".into(),
            ));
        }
        let state = self.inner.state.read().await.clone();
        let encoded = serde_json::to_vec_pretty(&state).map_err(|_| MonitorError::Serialization)?;
        if encoded.len() as u64 > MAX_STATE_FILE_BYTES {
            return Err(MonitorError::InvalidState(format!(
                "encoded state exceeds {MAX_STATE_FILE_BYTES} bytes"
            )));
        }
        let path = self.inner.path.clone();
        tokio::task::spawn_blocking(move || atomic_write(&path, &encoded))
            .await
            .map_err(|_| MonitorError::Runtime("state writer task failed".into()))??;
        Ok(())
    }

    async fn resume_workers(&self) {
        let ids = self
            .inner
            .state
            .read()
            .await
            .monitors
            .values()
            .filter(|monitor| monitor.status == MonitorStatus::Running)
            .map(|monitor| (monitor.id, monitor.revision))
            .collect::<Vec<_>>();
        for (id, revision) in ids.into_iter().take(MAX_ACTIVE_MONITORS) {
            if self.is_shutting_down() {
                return;
            }
            self.spawn_worker(id, revision).await;
        }
    }

    async fn spawn_worker(&self, monitor_id: Uuid, revision: u64) {
        if *self.inner.shutdown.borrow() {
            return;
        }
        let Ok(monitor) = self.get_view(monitor_id).await else {
            return;
        };
        if monitor.status != MonitorStatus::Running || monitor.revision != revision {
            return;
        }
        let (cancel, cancel_rx) = watch::channel(false);
        let token = Uuid::new_v4();
        let previous = {
            let mut workers = self.inner.workers.lock().await;
            if workers
                .get(&monitor_id)
                .is_some_and(|worker| worker.revision >= revision)
            {
                return;
            }
            workers.insert(
                monitor_id,
                WorkerHandle {
                    token,
                    revision,
                    cancel: cancel.clone(),
                    task: None,
                },
            )
        };
        if let Some(mut previous) = previous {
            let _ = previous.cancel.send(true);
            if let Some(task) = previous.task.take() {
                let _ = task.await;
            }
        }

        // A stop/update can commit after the first read but before the worker
        // reservation is installed. Re-check authoritative state before a
        // task can observe or persist any serial event.
        let still_authoritative = self.get_view(monitor_id).await.is_ok_and(|current| {
            current.status == MonitorStatus::Running && current.revision == revision
        });
        if self.is_shutting_down() || !still_authoritative {
            let _ = cancel.send(true);
            self.clear_worker_if_token(monitor_id, token).await;
            return;
        }

        let manager = self.clone();
        let mut workers = self.inner.workers.lock().await;
        let Some(worker) = workers
            .get_mut(&monitor_id)
            .filter(|worker| worker.token == token)
        else {
            return;
        };
        let task = tokio::spawn(async move {
            let result = run_monitor_worker(manager.clone(), monitor, cancel_rx).await;
            manager.clear_worker_if_token(monitor_id, token).await;
            if let Err(error) = result
                && !manager.is_shutting_down()
            {
                // `fail_monitor` restores supervision itself when persistence
                // rolls the authoritative state back to Running.
                let _ = manager
                    .fail_monitor(monitor_id, revision, error.to_string())
                    .await;
            }
        });
        worker.task = Some(task);
    }

    async fn stop_worker(&self, monitor_id: Uuid, revision: u64) {
        let worker = {
            let mut workers = self.inner.workers.lock().await;
            if workers
                .get(&monitor_id)
                .is_some_and(|worker| worker.revision == revision)
            {
                workers.remove(&monitor_id)
            } else {
                None
            }
        };
        if let Some(mut worker) = worker {
            let _ = worker.cancel.send(true);
            if let Some(task) = worker.task.take() {
                let _ = task.await;
            }
        }
    }

    async fn clear_worker_if_token(&self, monitor_id: Uuid, token: Uuid) {
        let mut workers = self.inner.workers.lock().await;
        if workers
            .get(&monitor_id)
            .is_some_and(|worker| worker.token == token)
        {
            workers.remove(&monitor_id);
        }
    }

    // Worker-failure recovery can re-enter `spawn_worker`. Erasing this future
    // type breaks that recursive async type while retaining the Send bound
    // required by the owning Tokio task.
    fn ensure_current_worker(
        &self,
        monitor_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            if let Ok(monitor) = self.get_view(monitor_id).await
                && monitor.status == MonitorStatus::Running
            {
                self.spawn_worker(monitor_id, monitor.revision).await;
            }
        })
    }

    async fn fail_monitor(
        &self,
        monitor_id: Uuid,
        revision: u64,
        error: String,
    ) -> Result<(), MonitorError> {
        let now = wall_time_ns();
        let result = self
            .mutate_and_persist(|state| {
                let monitor = state
                    .monitors
                    .get_mut(&monitor_id)
                    .ok_or(MonitorError::NotFound(monitor_id))?;
                if monitor.revision == revision && monitor.status == MonitorStatus::Running {
                    monitor.status = MonitorStatus::Failed;
                    monitor.stopped_wall_time_ns = Some(now);
                    monitor.last_error = Some(truncate_text(&error, MAX_DESCRIPTION_BYTES));
                }
                Ok(())
            })
            .await;
        if result.is_err() {
            // The state mutation rolls back on a write failure. Ensure that a
            // durable Running revision is never left without a supervisor.
            self.ensure_current_worker(monitor_id).await;
        }
        result
    }

    async fn complete_monitor(
        &self,
        monitor_id: Uuid,
        expected_revision: u64,
    ) -> Result<(), MonitorError> {
        let now = wall_time_ns();
        self.mutate_and_persist(|state| {
            let monitor = state
                .monitors
                .get_mut(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            if monitor.status == MonitorStatus::Running && monitor.revision == expected_revision {
                monitor.status = MonitorStatus::Completed;
                monitor.stopped_wall_time_ns = Some(now);
            }
            Ok(())
        })
        .await
    }

    async fn update_progress(&self, monitor_id: Uuid, expected_revision: u64, cursor: Cursor) {
        if let Some(monitor) = self
            .inner
            .state
            .write()
            .await
            .monitors
            .get_mut(&monitor_id)
            .filter(|monitor| {
                monitor.status == MonitorStatus::Running && monitor.revision == expected_revision
            })
        {
            monitor.current_cursor = Some(cursor);
        }
    }

    async fn checkpoint_progress(
        &self,
        monitor_id: Uuid,
        expected_revision: u64,
        checkpoint: MonitorCheckpoint,
    ) -> Result<(), MonitorError> {
        let _mutation = self.inner.mutation.lock().await;
        {
            let state = self.inner.state.read().await;
            let monitor = state
                .monitors
                .get(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            if monitor.status != MonitorStatus::Running || monitor.revision != expected_revision {
                return Ok(());
            }
            if state.checkpoints.get(&monitor_id) == Some(&checkpoint) {
                return Ok(());
            }
        }

        let previous = self.inner.state.read().await.clone();
        self.inner
            .state
            .write()
            .await
            .checkpoints
            .insert(monitor_id, checkpoint);
        if let Err(error) = self.persist().await {
            *self.inner.state.write().await = previous;
            return Err(error);
        }
        Ok(())
    }

    async fn checkpoint_for(&self, monitor: &MonitorView, fallback: Cursor) -> MonitorCheckpoint {
        self.inner
            .state
            .read()
            .await
            .checkpoints
            .get(&monitor.id)
            .cloned()
            .unwrap_or(MonitorCheckpoint {
                cursor: monitor.current_cursor.clone().unwrap_or(fallback),
                cooldown_until_wall_time_ns: None,
                pending: None,
            })
    }

    async fn record_gap(
        &self,
        monitor_id: Uuid,
        expected_revision: u64,
        message: String,
    ) -> Result<(), MonitorError> {
        self.mutate_and_persist(|state| {
            let monitor = state
                .monitors
                .get_mut(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            if monitor.status != MonitorStatus::Running || monitor.revision != expected_revision {
                return Ok(());
            }
            monitor.gap_count = monitor.gap_count.saturating_add(1);
            monitor.last_error = Some(truncate_text(&message, MAX_DESCRIPTION_BYTES));
            Ok(())
        })
        .await
    }

    async fn record_incident(
        &self,
        monitor_id: Uuid,
        expected_revision: u64,
        pending: PendingIncident,
        checkpoint: MonitorCheckpoint,
    ) -> Result<(), MonitorError> {
        let now = wall_time_ns();
        let server_id = self.inner.server_id;
        self.mutate_and_persist(|state| {
            prune_expired_metadata(state, now);
            let monitor = state
                .monitors
                .get_mut(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            if monitor.status != MonitorStatus::Running || monitor.revision != expected_revision {
                return Ok(());
            }
            if state
                .incidents
                .get(&monitor_id)
                .is_some_and(|incidents| incidents.len() >= MAX_INCIDENTS_PER_MONITOR)
            {
                prune_one_monitor_incident(state, monitor_id);
            }
            if state.incidents.values().map(VecDeque::len).sum::<usize>() >= MAX_INCIDENTS_TOTAL {
                prune_one_global_incident(state);
            }
            let monitor = state
                .monitors
                .get_mut(&monitor_id)
                .ok_or(MonitorError::NotFound(monitor_id))?;
            let incidents = state.incidents.entry(monitor_id).or_default();
            let incident_seq = monitor.incident_count.saturating_add(1);
            let incident_id = Uuid::new_v4();
            let expires = now.saturating_add(ms_to_ns(monitor.spec.event_ttl_ms));
            let cursor = Cursor {
                epoch: pending.daemon_epoch,
                after_seq: pending.seq_start.saturating_sub(1),
            };
            let incident = MonitorIncident {
                id: incident_id,
                incident_seq,
                monitor_id,
                slot_id: monitor.spec.slot_id.clone(),
                daemon_epoch: pending.daemon_epoch,
                seq_start: pending.seq_start,
                seq_end: pending.seq_end,
                wall_time_start_ns: pending.wall_time_start_ns,
                wall_time_end_ns: pending.wall_time_end_ns,
                severity: monitor.spec.severity,
                description: monitor.spec.description.clone(),
                preview: truncate_text(&pending.preview, MAX_PREVIEW_BYTES),
                evidence_cursor: cursor,
                evidence_ref: format!(
                    "serial://{server_id}/slots/{}/events?epoch={}&after_seq={}&through_seq={}",
                    monitor.spec.slot_id,
                    pending.daemon_epoch,
                    pending.seq_start.saturating_sub(1),
                    pending.seq_end
                ),
                created_wall_time_ns: now,
                expires_wall_time_ns: expires,
                acked_wall_time_ns: None,
            };
            monitor.incident_count = incident_seq;
            monitor.unacked_incident_count = monitor.unacked_incident_count.saturating_add(1);
            incidents.push_back(incident.clone());
            state.checkpoints.insert(monitor_id, checkpoint);
            enqueue_outbox(state, server_id, &incident, now);
            recompute_unacked_counts(state);
            Ok(())
        })
        .await
    }

    pub async fn outbox(
        &self,
        after_outbox_seq: Option<u64>,
        limit: Option<usize>,
    ) -> MonitorOutboxListResponse {
        // Standalone deployments have no sink loop. Expire stale entries before
        // exposing a pull page so consumers never mistake them for deliverable.
        if let Err(error) = self.expire_outbox(wall_time_ns()).await {
            tracing::warn!(%error, "failed to expire Monitor outbox entries before read");
        }
        let state = self.inner.state.read().await;
        let limit = limit.unwrap_or(DEFAULT_PAGE).clamp(1, MAX_PAGE);
        let eligible = state
            .outbox
            .iter()
            .filter(|entry| after_outbox_seq.is_none_or(|after| entry.public.outbox_seq > after))
            .map(|entry| entry.public.clone())
            .collect::<Vec<_>>();
        let truncated = eligible.len() > limit;
        let events = eligible.into_iter().take(limit).collect::<Vec<_>>();
        let next_cursor = events.last().map(|event| event.outbox_seq);
        MonitorOutboxListResponse {
            events,
            next_cursor,
            truncated,
        }
    }

    pub async fn acknowledge_outbox(
        &self,
        outbox_seq: u64,
    ) -> Result<MonitorOutboxEvent, MonitorError> {
        self.mutate_and_persist(|state| {
            let event = state
                .outbox
                .iter_mut()
                .find(|entry| entry.public.outbox_seq == outbox_seq)
                .ok_or(MonitorError::OutboxNotFound(outbox_seq))?;
            if event.public.status == MonitorOutboxStatus::Pending {
                event.public.status = MonitorOutboxStatus::Acknowledged;
            }
            Ok(())
        })
        .await?;
        self.inner
            .state
            .read()
            .await
            .outbox
            .iter()
            .find(|entry| entry.public.outbox_seq == outbox_seq)
            .map(|entry| entry.public.clone())
            .ok_or(MonitorError::OutboxNotFound(outbox_seq))
    }

    pub async fn shutdown(&self) {
        // `send` drops the new value when there are no receivers. That is a
        // real startup race here: an immediately shut down manager may not
        // have spawned a Monitor or sink receiver yet, and its startup task
        // would then observe `false` and create a sink task that shutdown waits
        // on forever. `send_replace` makes shutdown authoritative even with no
        // current subscribers; later subscribers also observe the terminal
        // state before starting work.
        self.inner.shutdown.send_replace(true);
        let startup = self
            .inner
            .startup_task
            .lock()
            .expect("Monitor startup task lock poisoned")
            .take();
        if let Some(task) = startup {
            let _ = task.await;
        }
        let workers = {
            let mut workers = self.inner.workers.lock().await;
            std::mem::take(&mut *workers)
        };
        for (_, worker) in &workers {
            let _ = worker.cancel.send(true);
        }
        for (_, mut worker) in workers {
            if let Some(task) = worker.task.take() {
                let _ = task.await;
            }
        }
        if let Some(task) = self.inner.sink_task.lock().await.take() {
            let _ = task.await;
        }
        if let Err(error) = self.persist().await {
            tracing::error!(%error, "failed to persist Monitor state during shutdown");
        }
    }

    fn is_shutting_down(&self) -> bool {
        *self.inner.shutdown.borrow()
    }

    #[cfg(test)]
    fn set_persist_failure(&self, fail: bool) {
        self.inner
            .fail_persists
            .store(fail, std::sync::atomic::Ordering::SeqCst);
    }
}

impl MonitorManager {
    async fn start_sink_worker(&self) {
        if self.is_shutting_down() {
            return;
        }
        let endpoint = self.inner.sink.endpoint.clone();
        let client = match endpoint.as_ref() {
            Some(_) => match reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
            {
                Ok(client) => Some(client),
                Err(error) => {
                    tracing::error!(%error, "failed to construct Monitor webhook client");
                    return;
                }
            },
            None => None,
        };
        let manager = self.clone();
        let token_file = self.inner.sink.token_file.clone();
        let mut shutdown = self.inner.shutdown.subscribe();
        let task = tokio::spawn(async move {
            loop {
                if *shutdown.borrow() {
                    break;
                }
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
                let now = wall_time_ns();
                if let Err(error) = manager.expire_outbox(now).await {
                    tracing::warn!(%error, "failed to expire Monitor outbox entries");
                    continue;
                }
                let (Some(client), Some(endpoint)) = (client.as_ref(), endpoint.as_deref()) else {
                    continue;
                };
                let Some(entry) = manager.next_due_outbox(now).await else {
                    continue;
                };
                let result =
                    send_cloud_event(client, endpoint, token_file.as_deref(), &entry.public.event)
                        .await;
                if let Err(error) = manager
                    .record_delivery_result(entry.public.outbox_seq, result)
                    .await
                {
                    tracing::warn!(%error, "failed to persist Monitor webhook result");
                }
            }
        });
        if self.is_shutting_down() {
            task.abort();
            return;
        }
        *self.inner.sink_task.lock().await = Some(task);
    }

    async fn next_due_outbox(&self, now: i64) -> Option<StoredOutboxEvent> {
        self.inner
            .state
            .read()
            .await
            .outbox
            .iter()
            .find(|entry| {
                entry.public.status == MonitorOutboxStatus::Pending
                    && entry.public.expires_wall_time_ns > now
                    && entry.next_attempt_wall_time_ns <= now
            })
            .cloned()
    }

    async fn expire_outbox(&self, now: i64) -> Result<(), MonitorError> {
        let state = self.inner.state.read().await;
        let needs_change = state.outbox.iter().any(|entry| {
            entry.public.status == MonitorOutboxStatus::Pending
                && entry.public.expires_wall_time_ns <= now
        });
        drop(state);
        if !needs_change {
            return Ok(());
        }
        self.mutate_and_persist(|state| {
            prune_expired_metadata(state, now);
            Ok(())
        })
        .await
    }

    async fn record_delivery_result(
        &self,
        outbox_seq: u64,
        result: Result<(), String>,
    ) -> Result<(), MonitorError> {
        let now = wall_time_ns();
        let retry_min_ms = self.inner.sink.retry_min_ms;
        let retry_max_ms = self.inner.sink.retry_max_ms;
        self.mutate_and_persist(|state| {
            let entry = state
                .outbox
                .iter_mut()
                .find(|entry| entry.public.outbox_seq == outbox_seq)
                .ok_or_else(|| MonitorError::Runtime("outbox entry disappeared".into()))?;
            if entry.public.status != MonitorOutboxStatus::Pending {
                return Ok(());
            }
            entry.public.attempts = entry.public.attempts.saturating_add(1);
            match result {
                Ok(()) => {
                    entry.public.status = MonitorOutboxStatus::Delivered;
                    entry.public.last_error = None;
                }
                Err(error) => {
                    entry.public.last_error = Some(truncate_text(&error, MAX_DESCRIPTION_BYTES));
                    let shift = entry.public.attempts.saturating_sub(1).min(20);
                    let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
                    let delay_ms = retry_min_ms.saturating_mul(multiplier).min(retry_max_ms);
                    entry.next_attempt_wall_time_ns = now.saturating_add(ms_to_ns(delay_ms));
                }
            }
            Ok(())
        })
        .await
    }
}

async fn send_cloud_event(
    client: &reqwest::Client,
    endpoint: &str,
    token_file: Option<&Path>,
    event: &MonitorCloudEvent,
) -> Result<(), String> {
    let mut request = client
        .post(endpoint)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/cloudevents+json",
        )
        .json(event);
    if let Some(path) = token_file {
        let token = tokio::fs::read_to_string(path)
            .await
            .map_err(|_| "could not read Monitor sink token file".to_owned())?;
        let token = token.trim();
        if token.is_empty() {
            return Err("Monitor sink token file is empty".into());
        }
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("webhook request failed: {error}"))?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("webhook returned HTTP {}", response.status()))
    }
}

fn prune_expired_metadata(state: &mut PersistedState, now: i64) {
    for entry in &mut state.outbox {
        if entry.public.status == MonitorOutboxStatus::Pending
            && entry.public.expires_wall_time_ns <= now
        {
            entry.public.status = MonitorOutboxStatus::Expired;
        }
    }
    while state.outbox.len() >= MAX_OUTBOX_EVENTS {
        let Some(index) = state
            .outbox
            .iter()
            .position(|entry| entry.public.status != MonitorOutboxStatus::Pending)
        else {
            break;
        };
        state.outbox.remove(index);
    }
    recompute_unacked_counts(state);
}

fn recompute_unacked_counts(state: &mut PersistedState) {
    let counts = state
        .incidents
        .iter()
        .map(|(monitor_id, incidents)| {
            (
                *monitor_id,
                incidents
                    .iter()
                    .filter(|incident| incident.acked_wall_time_ns.is_none())
                    .count() as u64,
            )
        })
        .collect::<BTreeMap<_, _>>();
    for monitor in state.monitors.values_mut() {
        monitor.unacked_incident_count = counts.get(&monitor.id).copied().unwrap_or(0);
    }
}

fn enqueue_outbox(
    state: &mut PersistedState,
    server_id: Uuid,
    incident: &MonitorIncident,
    now: i64,
) {
    prune_expired_metadata(state, now);
    if state.outbox.len() >= MAX_OUTBOX_EVENTS {
        if let Some(monitor) = state.monitors.get_mut(&incident.monitor_id) {
            monitor.gap_count = monitor.gap_count.saturating_add(1);
            monitor.last_error =
                Some("notification outbox is full; Incident remains queryable".into());
        }
        return;
    }
    let outbox_seq = state.next_outbox_seq;
    state.next_outbox_seq = state.next_outbox_seq.saturating_add(1).max(1);
    let created = rfc3339_ns(incident.created_wall_time_ns);
    let expires = rfc3339_ns(incident.expires_wall_time_ns);
    let cloud_event = MonitorCloudEvent {
        specversion: "1.0".into(),
        id: incident.id.to_string(),
        source: format!("serial://{server_id}/{}", incident.slot_id),
        event_type: "io.openchamber.serial.monitor.incident.detected.v1".into(),
        subject: format!("monitors/{}/incidents/{}", incident.monitor_id, incident.id),
        time: created,
        datacontenttype: "application/json".into(),
        expiresat: expires,
        data: serde_json::json!({
            "text": format!(
                "Serial monitor {} detected {:?} output on {} (seq {}-{}): {}",
                incident.monitor_id,
                incident.severity,
                incident.slot_id,
                incident.seq_start,
                incident.seq_end,
                incident.preview
            ),
            "incident": incident,
        }),
    };
    state.outbox.push_back(StoredOutboxEvent {
        public: MonitorOutboxEvent {
            outbox_seq,
            event: cloud_event,
            status: MonitorOutboxStatus::Pending,
            created_wall_time_ns: now,
            expires_wall_time_ns: incident.expires_wall_time_ns,
            attempts: 0,
            last_error: None,
        },
        next_attempt_wall_time_ns: now,
    });
}

fn rfc3339_ns(value: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(value)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn truncate_text(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &value[..end])
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingIncident {
    daemon_epoch: Uuid,
    seq_start: u64,
    seq_end: u64,
    wall_time_start_ns: i64,
    wall_time_end_ns: i64,
    preview: String,
}

enum CompiledMatcher {
    Literal(Vec<u8>),
    Regex(Regex),
}

struct StreamMatcher {
    matcher: CompiledMatcher,
    bytes: Vec<u8>,
    chunks: VecDeque<MatchChunk>,
    base_offset: u64,
    next_offset: u64,
    scanned_through: u64,
}

struct MatchChunk {
    end: u64,
    seq: u64,
    wall_time_ns: i64,
}

impl StreamMatcher {
    fn compile(spec: &MonitorSpec) -> Result<Self, MonitorError> {
        let matcher = if let Some(literal) = spec.contains.as_ref() {
            CompiledMatcher::Literal(literal.as_bytes().to_vec())
        } else {
            let expression = spec
                .regex
                .as_deref()
                .ok_or_else(|| MonitorError::InvalidSpec("missing matcher".into()))?;
            CompiledMatcher::Regex(
                RegexBuilder::new(expression)
                    .size_limit(MAX_REGEX_COMPILED_BYTES)
                    .dfa_size_limit(MAX_REGEX_COMPILED_BYTES)
                    .build()
                    .map_err(|error| {
                        MonitorError::InvalidSpec(format!("invalid regex: {error}"))
                    })?,
            )
        };
        Ok(Self {
            matcher,
            bytes: Vec::with_capacity(MAX_MATCH_WINDOW_BYTES),
            chunks: VecDeque::new(),
            base_offset: 0,
            next_offset: 0,
            scanned_through: 0,
        })
    }

    fn reset(&mut self) {
        self.bytes.clear();
        self.chunks.clear();
        self.base_offset = self.next_offset;
        self.scanned_through = self.next_offset;
    }

    fn push(&mut self, event: &TimelineEvent) -> Option<PendingIncident> {
        if event.data.is_empty() {
            return None;
        }
        let event_start = event.stream_offset_start.unwrap_or(self.next_offset);
        let event_end = event
            .stream_offset_end
            .unwrap_or_else(|| event_start.saturating_add(event.data.len() as u64));
        if !self.bytes.is_empty() && event_start != self.next_offset {
            self.bytes.clear();
            self.chunks.clear();
            self.base_offset = event_start;
            self.scanned_through = event_start;
        }
        if self.bytes.is_empty() {
            self.base_offset = event_start;
        }
        self.bytes.extend_from_slice(&event.data);
        self.chunks.push_back(MatchChunk {
            end: event_end,
            seq: event.seq,
            wall_time_ns: event.wall_time_ns,
        });
        self.next_offset = event_end;

        let previous_scan = self.scanned_through;
        let match_range = match &self.matcher {
            CompiledMatcher::Literal(pattern) => self
                .bytes
                .windows(pattern.len())
                .enumerate()
                .find(|(start, window)| {
                    *window == pattern.as_slice()
                        && self
                            .base_offset
                            .saturating_add((*start + pattern.len()) as u64)
                            > previous_scan
                })
                .map(|(start, _)| (start, start + pattern.len())),
            CompiledMatcher::Regex(regex) => regex.find_iter(&self.bytes).find_map(|found| {
                let absolute_end = self.base_offset.saturating_add(found.end() as u64);
                (absolute_end > previous_scan).then_some((found.start(), found.end()))
            }),
        };
        self.scanned_through = event_end;
        let incident = match_range.and_then(|(start, end)| self.incident_for(start, end, event));
        self.trim();
        incident
    }

    fn incident_for(
        &self,
        start: usize,
        end: usize,
        event: &TimelineEvent,
    ) -> Option<PendingIncident> {
        let absolute_start = self.base_offset.saturating_add(start as u64);
        let absolute_end = self.base_offset.saturating_add(end as u64);
        let first = self
            .chunks
            .iter()
            .find(|chunk| chunk.end > absolute_start)?;
        let last = self
            .chunks
            .iter()
            .find(|chunk| chunk.end >= absolute_end)
            .unwrap_or(first);
        let preview_start = start.saturating_sub(128);
        let preview_end = end.saturating_add(128).min(self.bytes.len());
        Some(PendingIncident {
            daemon_epoch: event.daemon_epoch,
            seq_start: first.seq,
            seq_end: last.seq,
            wall_time_start_ns: first.wall_time_ns,
            wall_time_end_ns: last.wall_time_ns,
            preview: sanitize_preview(&self.bytes[preview_start..preview_end]),
        })
    }

    fn trim(&mut self) {
        if self.bytes.len() <= MAX_MATCH_WINDOW_BYTES {
            return;
        }
        let remove = self.bytes.len() - MAX_MATCH_WINDOW_BYTES;
        self.bytes.drain(..remove);
        self.base_offset = self.base_offset.saturating_add(remove as u64);
        while self
            .chunks
            .front()
            .is_some_and(|chunk| chunk.end <= self.base_offset)
        {
            self.chunks.pop_front();
        }
    }
}

fn sanitize_preview(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut sanitized = String::with_capacity(text.len().min(MAX_PREVIEW_BYTES));
    for character in text.chars() {
        if sanitized.len() >= MAX_PREVIEW_BYTES {
            break;
        }
        match character {
            '\r' => {}
            '\n' | '\t' => sanitized.push(character),
            value if value.is_control() => sanitized.push('�'),
            value => sanitized.push(value),
        }
    }
    truncate_text(&sanitized, MAX_PREVIEW_BYTES)
}

struct WorkerRuntime {
    matcher: StreamMatcher,
    pending: Option<(PendingIncident, Instant)>,
    cooldown_until_wall_time_ns: Option<i64>,
    debounce: Duration,
    cooldown: Duration,
    generation: Option<u64>,
}

impl WorkerRuntime {
    fn new(
        spec: &MonitorSpec,
        cooldown_until_wall_time_ns: Option<i64>,
        pending: Option<PendingIncident>,
    ) -> Result<Self, MonitorError> {
        Ok(Self {
            matcher: StreamMatcher::compile(spec)?,
            pending: pending.map(|incident| (incident, Instant::now())),
            cooldown_until_wall_time_ns,
            debounce: Duration::from_millis(spec.debounce_ms),
            cooldown: Duration::from_millis(spec.cooldown_ms),
            generation: None,
        })
    }

    fn reset_for_gap(&mut self) -> Option<PendingIncident> {
        self.matcher.reset();
        self.generation = None;
        self.pending.take().map(|value| value.0)
    }

    fn reset_for_boundary(&mut self, generation: u64) -> Option<PendingIncident> {
        self.matcher.reset();
        self.generation = Some(generation);
        self.pending.take().map(|value| value.0)
    }

    fn observes_boundary(&mut self, event: &TimelineEvent) -> Option<PendingIncident> {
        let lifecycle_boundary = matches!(
            event.kind,
            EventKind::SerialOpened
                | EventKind::SerialClosed
                | EventKind::SlotReconfigured
                | EventKind::SlotRemoved
        );
        let generation_changed = self
            .generation
            .is_some_and(|value| value != event.generation);
        if lifecycle_boundary || generation_changed {
            return self.reset_for_boundary(event.generation);
        }
        self.generation.get_or_insert(event.generation);
        None
    }

    fn accept_match(
        &mut self,
        candidate: PendingIncident,
        now: Instant,
        now_wall_time_ns: i64,
    ) -> Option<PendingIncident> {
        if self
            .cooldown_until_wall_time_ns
            .is_some_and(|deadline| deadline > now_wall_time_ns)
        {
            return None;
        }
        if self.debounce.is_zero() {
            self.cooldown_until_wall_time_ns =
                Some(now_wall_time_ns.saturating_add(ms_to_ns(self.cooldown.as_millis() as u64)));
            return Some(candidate);
        }
        if let Some((pending, _)) = self.pending.as_mut() {
            pending.seq_end = candidate.seq_end;
            pending.wall_time_end_ns = candidate.wall_time_end_ns;
            if pending.preview.len() < MAX_PREVIEW_BYTES {
                pending.preview.push('\n');
                pending.preview.push_str(&candidate.preview);
                pending.preview = truncate_text(&pending.preview, MAX_PREVIEW_BYTES);
            }
        } else {
            self.pending = Some((candidate, now + self.debounce));
        }
        None
    }

    fn take_due(&mut self, now: Instant, now_wall_time_ns: i64) -> Option<PendingIncident> {
        if !self
            .pending
            .as_ref()
            .is_some_and(|(_, deadline)| *deadline <= now)
        {
            return None;
        }
        let (incident, _) = self.pending.take()?;
        self.cooldown_until_wall_time_ns =
            Some(now_wall_time_ns.saturating_add(ms_to_ns(self.cooldown.as_millis() as u64)));
        Some(incident)
    }

    fn take_recovered_pending(
        &mut self,
        cursor: &mut Cursor,
        now_wall_time_ns: i64,
    ) -> Option<PendingIncident> {
        let (incident, _) = self.pending.take()?;
        self.cooldown_until_wall_time_ns =
            Some(now_wall_time_ns.saturating_add(ms_to_ns(self.cooldown.as_millis() as u64)));
        // The durable pending checkpoint deliberately rewinds before the
        // match. Once that pending evidence is committed as an Incident, move
        // the same atomic checkpoint through its final byte so replay cannot
        // form the same Incident a second time.
        cursor.epoch = incident.daemon_epoch;
        cursor.after_seq = incident.seq_end;
        Some(incident)
    }

    fn checkpoint(&self, cursor: &Cursor) -> MonitorCheckpoint {
        let cursor = self.pending.as_ref().map_or_else(
            || cursor.clone(),
            |(pending, _)| Cursor {
                epoch: pending.daemon_epoch,
                after_seq: pending.seq_start.saturating_sub(1),
            },
        );
        MonitorCheckpoint {
            cursor,
            cooldown_until_wall_time_ns: self.cooldown_until_wall_time_ns,
            pending: self.pending.as_ref().map(|(pending, _)| pending.clone()),
        }
    }
}

async fn run_monitor_worker(
    manager: MonitorManager,
    monitor: MonitorView,
    mut cancel: watch::Receiver<bool>,
) -> Result<(), MonitorError> {
    let mut shutdown = manager.inner.shutdown.subscribe();
    if *shutdown.borrow() {
        return Ok(());
    }
    let handle = manager
        .inner
        .registry
        .get(&monitor.spec.slot_id)
        .await
        .ok_or_else(|| MonitorError::UnknownSlot(monitor.spec.slot_id.clone()))?;
    let snapshot = handle.snapshot();
    let fallback = monitor.current_cursor.clone().unwrap_or(Cursor {
        epoch: snapshot.daemon_epoch,
        after_seq: snapshot.head_seq,
    });
    let checkpoint = manager.checkpoint_for(&monitor, fallback).await;
    let mut runtime = WorkerRuntime::new(
        &monitor.spec,
        checkpoint.cooldown_until_wall_time_ns,
        checkpoint.pending,
    )?;
    let mut cursor = checkpoint.cursor;
    // A persisted debounced match belongs to its original epoch. Form it
    // before moving the replay cursor to a new epoch so evidence never spans
    // daemon sessions or vanishes on restart.
    if let Some(incident) = runtime.take_recovered_pending(&mut cursor, wall_time_ns()) {
        manager
            .record_incident(
                monitor.id,
                monitor.revision,
                incident,
                runtime.checkpoint(&cursor),
            )
            .await?;
    }
    if cursor.epoch != snapshot.daemon_epoch {
        manager
            .record_gap(
                monitor.id,
                monitor.revision,
                format!(
                    "daemon epoch changed from {} to {}; matching resumed from the retained current-epoch ring",
                    cursor.epoch, snapshot.daemon_epoch
                ),
            )
            .await?;
        cursor = Cursor {
            epoch: snapshot.daemon_epoch,
            after_seq: 0,
        };
        let _ = runtime.reset_for_gap();
    }
    if cursor.after_seq > snapshot.head_seq {
        manager
            .record_gap(
                monitor.id,
                monitor.revision,
                "saved cursor was ahead of the current head; matching resumed at head".into(),
            )
            .await?;
        cursor.after_seq = snapshot.head_seq;
    }
    manager
        .update_progress(monitor.id, monitor.revision, cursor.clone())
        .await;
    let mut last_checkpoint = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(25));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    'reattach: loop {
        if is_expired(monitor.expires_wall_time_ns) {
            if let Some(incident) = runtime.pending.take().map(|value| value.0) {
                manager
                    .record_incident(
                        monitor.id,
                        monitor.revision,
                        incident,
                        runtime.checkpoint(&cursor),
                    )
                    .await?;
            }
            manager
                .complete_monitor(monitor.id, monitor.revision)
                .await?;
            return Ok(());
        }
        let attach = handle
            .attach(Some(&cursor), 1)
            .await
            .map_err(|error| MonitorError::Runtime(error.to_string()))?;
        if let Some(gap) = attach.replay.gap {
            manager
                .record_gap(
                    monitor.id,
                    monitor.revision,
                    format!(
                        "Monitor replay gap: {:?}, requested after {:?}, first available {:?}",
                        gap.reason, gap.requested_after_seq, gap.first_available_seq
                    ),
                )
                .await?;
            if let Some(incident) = runtime.reset_for_gap() {
                manager
                    .record_incident(
                        monitor.id,
                        monitor.revision,
                        incident,
                        runtime.checkpoint(&cursor),
                    )
                    .await?;
            }
        }
        for event in attach.replay.events {
            process_monitor_event(
                &manager,
                monitor.id,
                monitor.revision,
                &mut runtime,
                &mut cursor,
                event,
            )
            .await?;
            checkpoint_if_due(
                &manager,
                monitor.id,
                monitor.revision,
                &runtime,
                &cursor,
                &mut last_checkpoint,
            )
            .await?;
        }
        let ready_head = attach.snapshot.head_seq;
        let mut live = attach.live;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        manager
                            .checkpoint_progress(
                                monitor.id,
                                monitor.revision,
                                runtime.checkpoint(&cursor),
                            )
                            .await?;
                        return Ok(());
                    }
                }
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        manager
                            .checkpoint_progress(
                                monitor.id,
                                monitor.revision,
                                runtime.checkpoint(&cursor),
                            )
                            .await?;
                        return Ok(());
                    }
                }
                _ = tick.tick() => {
                    let now = Instant::now();
                    let now_wall_time_ns = wall_time_ns();
                    if let Some(incident) = runtime.take_due(now, now_wall_time_ns) {
                        manager
                            .record_incident(
                                monitor.id,
                                monitor.revision,
                                incident,
                                runtime.checkpoint(&cursor),
                            )
                            .await?;
                    }
                    checkpoint_if_due(
                        &manager,
                        monitor.id,
                        monitor.revision,
                        &runtime,
                        &cursor,
                        &mut last_checkpoint,
                    )
                    .await?;
                    if is_expired(monitor.expires_wall_time_ns) {
                        if let Some(incident) = runtime.pending.take().map(|value| value.0) {
                            manager
                                .record_incident(
                                    monitor.id,
                                    monitor.revision,
                                    incident,
                                    runtime.checkpoint(&cursor),
                                )
                                .await?;
                        }
                        manager
                            .complete_monitor(monitor.id, monitor.revision)
                            .await?;
                        return Ok(());
                    }
                }
                received = live.recv() => match received {
                    Ok(event) => {
                        if event.seq <= ready_head || event.seq <= cursor.after_seq {
                            continue;
                        }
                        process_monitor_event(
                            &manager,
                            monitor.id,
                            monitor.revision,
                            &mut runtime,
                            &mut cursor,
                            event,
                        ).await?;
                        checkpoint_if_due(
                            &manager,
                            monitor.id,
                            monitor.revision,
                            &runtime,
                            &cursor,
                            &mut last_checkpoint,
                        )
                        .await?;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Reattach from the exact last processed cursor. The
                        // ring recovers ordinary subscriber lag; only an
                        // actual ring gap increments the Monitor gap counter.
                        continue 'reattach;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        manager
                            .checkpoint_progress(
                                monitor.id,
                                monitor.revision,
                                runtime.checkpoint(&cursor),
                            )
                            .await?;
                        return Err(MonitorError::Runtime("Slot event stream closed".into()));
                    }
                }
            }
        }
    }
}

async fn process_monitor_event(
    manager: &MonitorManager,
    monitor_id: Uuid,
    expected_revision: u64,
    runtime: &mut WorkerRuntime,
    cursor: &mut Cursor,
    event: TimelineEvent,
) -> Result<(), MonitorError> {
    cursor.epoch = event.daemon_epoch;
    cursor.after_seq = event.seq;
    manager
        .update_progress(monitor_id, expected_revision, cursor.clone())
        .await;
    if event.kind == EventKind::Gap {
        if let Some(incident) = runtime.reset_for_gap() {
            manager
                .record_incident(
                    monitor_id,
                    expected_revision,
                    incident,
                    runtime.checkpoint(cursor),
                )
                .await?;
        }
        manager
            .record_gap(
                monitor_id,
                expected_revision,
                "timeline reported an RX/logging gap; matcher reset".into(),
            )
            .await?;
        return Ok(());
    }
    if let Some(incident) = runtime.observes_boundary(&event) {
        manager
            .record_incident(
                monitor_id,
                expected_revision,
                incident,
                runtime.checkpoint(cursor),
            )
            .await?;
    }
    if event.kind != EventKind::Rx || event.direction != Direction::Rx {
        return Ok(());
    }
    let Some(candidate) = runtime.matcher.push(&event) else {
        return Ok(());
    };
    let had_pending = runtime.pending.is_some();
    if let Some(incident) = runtime.accept_match(candidate, Instant::now(), wall_time_ns()) {
        manager
            .record_incident(
                monitor_id,
                expected_revision,
                incident,
                runtime.checkpoint(cursor),
            )
            .await?;
    } else if !had_pending && runtime.pending.is_some() {
        // Persist the first debounced match immediately. The cursor stored in
        // this checkpoint deliberately rewinds to just before the match so a
        // crash cannot lose an incident even if the Slot ring belongs to a new
        // daemon epoch on restart.
        manager
            .checkpoint_progress(monitor_id, expected_revision, runtime.checkpoint(cursor))
            .await?;
    }
    Ok(())
}

async fn checkpoint_if_due(
    manager: &MonitorManager,
    monitor_id: Uuid,
    expected_revision: u64,
    runtime: &WorkerRuntime,
    cursor: &Cursor,
    last_checkpoint: &mut Instant,
) -> Result<(), MonitorError> {
    if last_checkpoint.elapsed() < CHECKPOINT_INTERVAL {
        return Ok(());
    }
    manager
        .checkpoint_progress(monitor_id, expected_revision, runtime.checkpoint(cursor))
        .await?;
    *last_checkpoint = Instant::now();
    Ok(())
}

fn is_expired(deadline: Option<i64>) -> bool {
    deadline.is_some_and(|deadline| deadline <= wall_time_ns())
}

fn load_state(path: &Path) -> Result<PersistedState, MonitorError> {
    if !path.exists() {
        return Ok(PersistedState::default());
    }
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_STATE_FILE_BYTES {
        return Err(MonitorError::InvalidState(format!(
            "state file exceeds {MAX_STATE_FILE_BYTES} bytes"
        )));
    }
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).map_err(|_| MonitorError::InvalidState("invalid JSON".into()))
}

fn same_create_spec(existing: &MonitorSpec, requested: &MonitorSpec) -> bool {
    let mut comparable = existing.clone();
    if requested.start_cursor.is_none() {
        comparable.start_cursor = None;
    }
    comparable == *requested
}

fn prune_monitor_capacity(state: &mut PersistedState, _now: i64) {
    while state.monitors.len() >= MAX_MONITORS {
        let candidate = state
            .monitors
            .values()
            .filter(|monitor| monitor.status != MonitorStatus::Running)
            // Prefer a fully acknowledged Job, but never let missing ACKs
            // permanently exhaust the catalog. At the hard bound the oldest
            // stopped Job yields to newer work; raw evidence remains in the
            // serial journal even when its Monitor summary is pruned.
            .min_by_key(|monitor| {
                let has_unacked = state.incidents.get(&monitor.id).is_some_and(|incidents| {
                    incidents
                        .iter()
                        .any(|incident| incident.acked_wall_time_ns.is_none())
                });
                (
                    has_unacked,
                    monitor.stopped_wall_time_ns,
                    monitor.created_wall_time_ns,
                )
            })
            .map(|monitor| monitor.id);
        let Some(id) = candidate else {
            break;
        };
        state.monitors.remove(&id);
        state.incidents.remove(&id);
        state.checkpoints.remove(&id);
    }
}

fn mark_incident_retention_gap(state: &mut PersistedState, monitor_id: Uuid, message: &str) {
    if let Some(monitor) = state.monitors.get_mut(&monitor_id) {
        monitor.gap_count = monitor.gap_count.saturating_add(1);
        monitor.last_error = Some(message.into());
    }
}

fn prune_one_monitor_incident(state: &mut PersistedState, monitor_id: Uuid) {
    let removed = state
        .incidents
        .get_mut(&monitor_id)
        .and_then(VecDeque::pop_front);
    if removed.is_some() {
        mark_incident_retention_gap(
            state,
            monitor_id,
            "oldest retained Incident was pruned at the per-Monitor capacity",
        );
    }
}

fn prune_one_global_incident(state: &mut PersistedState) {
    let candidate = state
        .incidents
        .iter()
        .flat_map(|(monitor_id, incidents)| {
            incidents
                .iter()
                .enumerate()
                .map(move |(index, incident)| (*monitor_id, index, incident.created_wall_time_ns))
        })
        .min_by_key(|(_, _, created)| *created);
    if let Some((monitor_id, index, _)) = candidate
        && let Some(incidents) = state.incidents.get_mut(&monitor_id)
    {
        incidents.remove(index);
        mark_incident_retention_gap(
            state,
            monitor_id,
            "oldest retained Incident was pruned at the global capacity",
        );
    }
}

fn validate_loaded_state(state: &PersistedState) -> Result<(), MonitorError> {
    if state.schema_version != STATE_SCHEMA_VERSION {
        return Err(MonitorError::InvalidState(format!(
            "unsupported schema version {}",
            state.schema_version
        )));
    }
    if state.monitors.len() > MAX_MONITORS
        || state
            .monitors
            .values()
            .filter(|monitor| monitor.status == MonitorStatus::Running)
            .count()
            > MAX_ACTIVE_MONITORS
        || state.outbox.len() > MAX_OUTBOX_EVENTS
        || state.incidents.values().map(VecDeque::len).sum::<usize>() > MAX_INCIDENTS_TOTAL
        || state
            .incidents
            .values()
            .any(|incidents| incidents.len() > MAX_INCIDENTS_PER_MONITOR)
    {
        return Err(MonitorError::InvalidState(
            "retention bound exceeded".into(),
        ));
    }
    for monitor in state.monitors.values() {
        validate_spec(&monitor.spec)?;
    }
    Ok(())
}

fn validate_spec(spec: &MonitorSpec) -> Result<(), MonitorError> {
    if spec.slot_id.is_empty() || spec.slot_id.len() > 64 {
        return Err(MonitorError::InvalidSpec("invalid slot_id".into()));
    }
    match (spec.contains.as_deref(), spec.regex.as_deref()) {
        (Some(literal), None) if !literal.is_empty() && literal.len() <= MAX_PATTERN_BYTES => {}
        (None, Some(expression))
            if !expression.is_empty() && expression.len() <= MAX_PATTERN_BYTES =>
        {
            let regex = RegexBuilder::new(expression)
                .size_limit(MAX_REGEX_COMPILED_BYTES)
                .dfa_size_limit(MAX_REGEX_COMPILED_BYTES)
                .build()
                .map_err(|error| MonitorError::InvalidSpec(format!("invalid regex: {error}")))?;
            if regex.is_match(b"") {
                return Err(MonitorError::InvalidSpec(
                    "regex must not match an empty byte stream".into(),
                ));
            }
        }
        _ => {
            return Err(MonitorError::InvalidSpec(
                "exactly one non-empty contains or regex is required".into(),
            ));
        }
    }
    if spec
        .description
        .as_ref()
        .is_some_and(|value| value.len() > MAX_DESCRIPTION_BYTES)
    {
        return Err(MonitorError::InvalidSpec("description is too large".into()));
    }
    if spec.debounce_ms > MAX_DEBOUNCE_MS {
        return Err(MonitorError::InvalidSpec("debounce_ms is too large".into()));
    }
    if spec.cooldown_ms > MAX_COOLDOWN_MS {
        return Err(MonitorError::InvalidSpec("cooldown_ms is too large".into()));
    }
    if spec
        .duration_ms
        .is_some_and(|value| value == 0 || value > MAX_DURATION_MS)
    {
        return Err(MonitorError::InvalidSpec(
            "duration_ms is out of range".into(),
        ));
    }
    if spec.event_ttl_ms == 0 || spec.event_ttl_ms > MAX_EVENT_TTL_MS {
        return Err(MonitorError::InvalidSpec(
            "event_ttl_ms is out of range".into(),
        ));
    }
    Ok(())
}

fn resolve_start_cursor(
    spec: &MonitorSpec,
    daemon_epoch: Uuid,
    head_seq: u64,
) -> Result<Cursor, MonitorError> {
    let Some(cursor) = spec.start_cursor.clone() else {
        return Ok(Cursor {
            epoch: daemon_epoch,
            after_seq: head_seq,
        });
    };
    if cursor.epoch == daemon_epoch && cursor.after_seq > head_seq {
        return Err(MonitorError::CursorAhead);
    }
    Ok(cursor)
}

fn duration_deadline(now: i64, duration_ms: Option<u64>) -> Option<i64> {
    duration_ms.map(|duration| now.saturating_add(ms_to_ns(duration)))
}

fn ms_to_ns(value: u64) -> i64 {
    value.saturating_mul(1_000_000).min(i64::MAX as u64) as i64
}

fn wall_time_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or_else(|| {
        chrono::Utc::now()
            .timestamp_millis()
            .saturating_mul(1_000_000)
    })
}

#[cfg(test)]
mod matcher_tests {
    use super::*;
    use serial_protocol::MonitorSeverity;

    fn literal_spec(literal: &str) -> MonitorSpec {
        MonitorSpec {
            slot_id: "slot-1".into(),
            contains: Some(literal.into()),
            regex: None,
            start_cursor: None,
            severity: MonitorSeverity::Warning,
            description: None,
            debounce_ms: 0,
            cooldown_ms: 0,
            duration_ms: None,
            event_ttl_ms: 60_000,
        }
    }

    fn rx_event(epoch: Uuid, seq: u64, start: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(start),
            stream_offset_end: Some(start + data.len() as u64),
            data: data.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    #[test]
    fn literal_matcher_finds_repeated_matches_before_window_trim() {
        let epoch = Uuid::new_v4();
        let mut matcher = StreamMatcher::compile(&literal_spec("ERROR")).unwrap();
        let first = matcher.push(&rx_event(epoch, 1, 0, b"ERROR ok\n")).unwrap();
        let second = matcher
            .push(&rx_event(epoch, 2, 9, b"again ERROR\n"))
            .unwrap();
        assert_eq!((first.seq_start, first.seq_end), (1, 1));
        assert_eq!((second.seq_start, second.seq_end), (2, 2));
    }

    #[test]
    fn literal_matcher_spans_contiguous_rx_events() {
        let epoch = Uuid::new_v4();
        let mut matcher = StreamMatcher::compile(&literal_spec("panic")).unwrap();
        assert!(matcher.push(&rx_event(epoch, 7, 0, b"pa")).is_none());
        let incident = matcher
            .push(&rx_event(epoch, 8, 2, b"nic: boom\n"))
            .unwrap();
        assert_eq!((incident.seq_start, incident.seq_end), (7, 8));
        assert!(incident.preview.contains("panic"));
    }

    #[test]
    fn generation_and_serial_lifecycle_boundaries_never_join_rx_bytes() {
        let epoch = Uuid::new_v4();
        let spec = literal_spec("panic");
        let mut runtime = WorkerRuntime::new(&spec, None, None).unwrap();
        let first = rx_event(epoch, 1, 0, b"pa");
        assert!(runtime.observes_boundary(&first).is_none());
        assert!(runtime.matcher.push(&first).is_none());

        let mut changed = rx_event(epoch, 2, 2, b"nic");
        changed.generation = 2;
        assert!(runtime.observes_boundary(&changed).is_none());
        assert!(runtime.matcher.push(&changed).is_none());

        let mut opened = rx_event(epoch, 3, 5, b"");
        opened.kind = EventKind::SerialOpened;
        assert!(runtime.observes_boundary(&opened).is_none());
        let tail = rx_event(epoch, 4, 5, b"nic");
        assert!(runtime.matcher.push(&tail).is_none());
    }

    #[test]
    fn generation_boundary_flushes_pending_debounce_evidence() {
        let epoch = Uuid::new_v4();
        let mut spec = literal_spec("panic");
        spec.debounce_ms = 1_000;
        let mut runtime = WorkerRuntime::new(&spec, None, None).unwrap();
        let first = rx_event(epoch, 1, 0, b"panic");
        assert!(runtime.observes_boundary(&first).is_none());
        let candidate = runtime.matcher.push(&first).unwrap();
        assert!(
            runtime
                .accept_match(candidate, Instant::now(), wall_time_ns())
                .is_none()
        );
        let mut next_generation = rx_event(epoch, 2, 5, b"");
        next_generation.generation = 2;
        let incident = runtime.observes_boundary(&next_generation).unwrap();
        assert_eq!((incident.seq_start, incident.seq_end), (1, 1));
        assert!(runtime.pending.is_none());
    }

    #[test]
    fn regex_validation_rejects_empty_stream_matches() {
        let mut spec = literal_spec("unused");
        spec.contains = None;
        spec.regex = Some(".*".into());
        assert!(matches!(
            validate_spec(&spec),
            Err(MonitorError::InvalidSpec(_))
        ));
    }

    #[test]
    fn truncation_preserves_utf8_boundaries() {
        assert_eq!(truncate_text("甲乙丙", 4), "甲…");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ControlLimits;
    use crate::journal::{JournalConfig, JournalManager};
    use serial_protocol::{MonitorSeverity, SerialSettings, SlotConfig};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn spec(slot_id: &str) -> MonitorSpec {
        MonitorSpec {
            slot_id: slot_id.into(),
            contains: Some("kernel panic".into()),
            regex: None,
            start_cursor: None,
            severity: MonitorSeverity::Error,
            description: Some("watch the DUT".into()),
            debounce_ms: 0,
            cooldown_ms: 0,
            duration_ms: None,
            event_ttl_ms: 60_000,
        }
    }

    fn rx_event(epoch: Uuid, seq: u64, start: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(start),
            stream_offset_end: Some(start + data.len() as u64),
            data: data.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    fn retained_incident(monitor_id: Uuid, epoch: Uuid, incident_seq: u64) -> MonitorIncident {
        MonitorIncident {
            id: Uuid::new_v4(),
            incident_seq,
            monitor_id,
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            seq_start: incident_seq,
            seq_end: incident_seq,
            wall_time_start_ns: incident_seq as i64,
            wall_time_end_ns: incident_seq as i64,
            severity: MonitorSeverity::Error,
            description: Some("watch the DUT".into()),
            preview: format!("kernel panic {incident_seq}"),
            evidence_cursor: Cursor {
                epoch,
                after_seq: incident_seq.saturating_sub(1),
            },
            evidence_ref: format!("serial://test/{incident_seq}"),
            created_wall_time_ns: incident_seq as i64,
            expires_wall_time_ns: i64::MAX,
            acked_wall_time_ns: None,
        }
    }

    fn stopped_monitor(id: Uuid, epoch: Uuid, created_wall_time_ns: i64) -> MonitorView {
        let mut monitor_spec = spec("slot-1");
        monitor_spec.start_cursor = Some(Cursor {
            epoch,
            after_seq: 0,
        });
        MonitorView {
            id,
            revision: 2,
            spec: monitor_spec,
            status: MonitorStatus::Stopped,
            created_wall_time_ns,
            started_wall_time_ns: created_wall_time_ns,
            expires_wall_time_ns: None,
            stopped_wall_time_ns: Some(created_wall_time_ns),
            current_cursor: Some(Cursor {
                epoch,
                after_seq: 0,
            }),
            incident_count: 1,
            unacked_incident_count: 1,
            gap_count: 0,
            last_error: None,
        }
    }

    #[test]
    fn literal_and_regex_match_across_rx_chunks_without_repeating_old_matches() {
        let epoch = Uuid::new_v4();
        let mut literal = StreamMatcher::compile(&spec("slot-1")).unwrap();
        assert!(literal.push(&rx_event(epoch, 1, 0, b"kernel ")).is_none());
        let first = literal.push(&rx_event(epoch, 2, 7, b"panic one")).unwrap();
        assert_eq!((first.seq_start, first.seq_end), (1, 2));
        assert!(literal.push(&rx_event(epoch, 3, 16, b" noise ")).is_none());
        let second = literal
            .push(&rx_event(epoch, 4, 23, b"kernel panic two"))
            .unwrap();
        assert_eq!((second.seq_start, second.seq_end), (4, 4));

        let mut regex_spec = spec("slot-1");
        regex_spec.contains = None;
        regex_spec.regex = Some(r"panic\s+code=E[0-9]+".into());
        let mut regex = StreamMatcher::compile(&regex_spec).unwrap();
        assert!(regex.push(&rx_event(epoch, 5, 0, b"panic ")).is_none());
        assert!(regex.push(&rx_event(epoch, 6, 6, b"code=E42")).is_some());
    }

    async fn fixture(temp: &TempDir) -> (MonitorManager, SlotRegistry, JournalManager, Uuid) {
        let epoch = Uuid::new_v4();
        let journal =
            JournalManager::open(JournalConfig::new(temp.path().join("journal"))).unwrap();
        let slot = SlotConfig {
            id: "slot-1".into(),
            display_name: "Slot 1".into(),
            port: "TEST0".into(),
            profile: "generic-115200".into(),
            device_profile: None,
            enabled: false,
            settings: SerialSettings {
                auto_open: false,
                ..SerialSettings::default()
            },
        };
        let registry = SlotRegistry::new(
            epoch,
            Instant::now(),
            journal.handle(),
            vec![slot],
            Vec::new(),
            Vec::new(),
            ControlLimits::default(),
        );
        let manager = MonitorManager::open(
            temp.path().join("monitors.json"),
            registry.clone(),
            epoch,
            Uuid::new_v4(),
            MonitorEventSinkConfig::default(),
        )
        .unwrap();
        (manager, registry, journal, epoch)
    }

    #[tokio::test]
    async fn shutdown_before_startup_subscribes_is_bounded() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;

        tokio::time::timeout(Duration::from_secs(1), manager.shutdown())
            .await
            .expect("Monitor shutdown must retain its signal without receivers");

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn first_debounced_match_is_checkpointed_immediately() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let mut monitor_spec = spec("slot-1");
        monitor_spec.debounce_ms = 60_000;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: monitor_spec.clone(),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(created.id, created.revision).await;

        let mut runtime = WorkerRuntime::new(&monitor_spec, None, None).unwrap();
        let mut cursor = created.current_cursor.clone().unwrap();
        process_monitor_event(
            &manager,
            created.id,
            created.revision,
            &mut runtime,
            &mut cursor,
            rx_event(epoch, 1, 0, b"kernel panic"),
        )
        .await
        .unwrap();

        let persisted = load_state(&temp.path().join("monitors.json")).unwrap();
        let checkpoint = persisted.checkpoints.get(&created.id).unwrap();
        assert_eq!(checkpoint.cursor.after_seq, 0);
        let pending = checkpoint.pending.as_ref().unwrap();
        assert_eq!((pending.seq_start, pending.seq_end), (1, 1));

        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn pending_restart_records_exactly_one_incident_and_advances_checkpoint() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let mut monitor_spec = spec("slot-1");
        monitor_spec.debounce_ms = 60_000;
        monitor_spec.cooldown_ms = 30_000;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: monitor_spec.clone(),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(created.id, created.revision).await;

        let mut runtime = WorkerRuntime::new(&monitor_spec, None, None).unwrap();
        let mut cursor = created.current_cursor.clone().unwrap();
        process_monitor_event(
            &manager,
            created.id,
            created.revision,
            &mut runtime,
            &mut cursor,
            rx_event(epoch, 1, 0, b"kernel panic"),
        )
        .await
        .unwrap();
        manager.shutdown().await;

        let reopened = MonitorManager::open(
            temp.path().join("monitors.json"),
            registry.clone(),
            epoch,
            Uuid::new_v4(),
            MonitorEventSinkConfig::default(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = reopened
                    .incidents(created.id, Some(0), Some(20), true)
                    .await
                    .unwrap();
                if page.incidents.len() == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovered pending Incident");

        let state = reopened.inner.state.read().await;
        assert_eq!(state.incidents.get(&created.id).unwrap().len(), 1);
        let checkpoint = state.checkpoints.get(&created.id).unwrap();
        assert_eq!(checkpoint.cursor.epoch, epoch);
        assert_eq!(checkpoint.cursor.after_seq, 1);
        assert!(checkpoint.pending.is_none());
        assert!(
            checkpoint
                .cooldown_until_wall_time_ns
                .is_some_and(|deadline| deadline > wall_time_ns())
        );
        drop(state);

        reopened.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_worker_token_cannot_clear_replacement() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let token = Uuid::new_v4();
        let stale_token = Uuid::new_v4();
        let (cancel, mut cancelled) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _ = cancelled.changed().await;
        });
        manager.inner.workers.lock().await.insert(
            monitor_id,
            WorkerHandle {
                token,
                revision: 2,
                cancel,
                task: Some(task),
            },
        );

        manager.clear_worker_if_token(monitor_id, stale_token).await;
        assert_eq!(
            manager
                .inner
                .workers
                .lock()
                .await
                .get(&monitor_id)
                .map(|worker| worker.token),
            Some(token)
        );

        manager.stop_worker(monitor_id, 2).await;
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_spawns_cannot_resurrect_a_stopped_revision() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        let monitor_id = created.id;
        let revision = created.revision;
        manager.stop_worker(monitor_id, revision).await;

        let mut spawns = Vec::new();
        for _ in 0..32 {
            let manager = manager.clone();
            spawns.push(tokio::spawn(async move {
                manager.spawn_worker(monitor_id, revision).await;
            }));
        }
        let stopping = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.stop(monitor_id, revision).await })
        };
        for spawn in spawns {
            spawn.await.unwrap();
        }
        let stopped = stopping.await.unwrap().unwrap().monitor;

        assert_eq!(stopped.status, MonitorStatus::Stopped);
        assert!(!manager.inner.workers.lock().await.contains_key(&monitor_id));

        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn create_is_idempotent_starts_at_head_and_stop_survives_reopen() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let request = CreateMonitorRequest {
            request_id: Uuid::new_v4(),
            spec: spec("slot-1"),
        };
        let created = manager.create(request.clone()).await.unwrap().monitor;
        assert_eq!(created.current_cursor.as_ref().unwrap().after_seq, 0);
        assert_eq!(
            manager.create(request).await.unwrap().monitor.id,
            created.id
        );
        assert_eq!(
            manager
                .stop(created.id, created.revision)
                .await
                .unwrap()
                .monitor
                .status,
            MonitorStatus::Stopped
        );
        manager.shutdown().await;

        let reopened = MonitorManager::open(
            temp.path().join("monitors.json"),
            registry.clone(),
            registry.daemon_epoch(),
            Uuid::new_v4(),
            MonitorEventSinkConfig::default(),
        )
        .unwrap();
        assert_eq!(
            reopened.get(created.id).await.unwrap().monitor.status,
            MonitorStatus::Stopped
        );
        reopened.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn incidents_are_bounded_tail_page_and_ack_cancels_pending_notification() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let revision = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor
            .revision;
        for seq in 1..=3 {
            manager
                .record_incident(
                    monitor_id,
                    revision,
                    PendingIncident {
                        daemon_epoch: epoch,
                        seq_start: seq,
                        seq_end: seq,
                        wall_time_start_ns: seq as i64,
                        wall_time_end_ns: seq as i64,
                        preview: format!("panic {seq}"),
                    },
                    MonitorCheckpoint {
                        cursor: Cursor {
                            epoch,
                            after_seq: seq,
                        },
                        cooldown_until_wall_time_ns: None,
                        pending: None,
                    },
                )
                .await
                .unwrap();
        }
        let tail = manager
            .incidents(monitor_id, None, Some(2), false)
            .await
            .unwrap();
        assert_eq!(tail.incidents.len(), 2);
        assert!(tail.truncated);
        assert_eq!(tail.incidents[0].incident_seq, 2);
        let incident = tail.incidents[0].clone();
        manager
            .acknowledge_incident(monitor_id, incident.id)
            .await
            .unwrap();
        let outbox = manager.outbox(None, None).await;
        assert_eq!(
            outbox
                .events
                .iter()
                .find(|event| event.event.id == incident.id.to_string())
                .unwrap()
                .status,
            MonitorOutboxStatus::Acknowledged
        );
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn filtered_empty_incident_page_advances_to_the_observed_high_water() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let created = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(monitor_id, created.revision).await;
        {
            let mut state = manager.inner.state.write().await;
            let incidents = (1..=3)
                .map(|seq| {
                    let mut incident = retained_incident(monitor_id, epoch, seq);
                    incident.acked_wall_time_ns = Some(seq as i64);
                    incident
                })
                .collect::<VecDeque<_>>();
            state.incidents.insert(monitor_id, incidents);
            let monitor = state.monitors.get_mut(&monitor_id).unwrap();
            monitor.incident_count = 3;
            monitor.unacked_incident_count = 0;
        }

        let page = manager
            .incidents(monitor_id, Some(0), Some(20), false)
            .await
            .unwrap();
        assert!(page.incidents.is_empty());
        assert_eq!(page.next_cursor, Some(3));
        assert!(!page.truncated);

        let beyond_head = manager
            .incidents(monitor_id, Some(99), Some(20), false)
            .await
            .unwrap();
        assert!(beyond_head.incidents.is_empty());
        assert_eq!(beyond_head.next_cursor, Some(99));

        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn unacked_incident_capacity_evicts_oldest_and_retains_newest_match() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let created = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(monitor_id, created.revision).await;
        {
            let mut state = manager.inner.state.write().await;
            state.incidents.insert(
                monitor_id,
                (1..=MAX_INCIDENTS_PER_MONITOR as u64)
                    .map(|seq| retained_incident(monitor_id, epoch, seq))
                    .collect(),
            );
            let monitor = state.monitors.get_mut(&monitor_id).unwrap();
            monitor.incident_count = MAX_INCIDENTS_PER_MONITOR as u64;
            monitor.unacked_incident_count = MAX_INCIDENTS_PER_MONITOR as u64;
        }

        let newest_seq = MAX_INCIDENTS_PER_MONITOR as u64 + 1;
        manager
            .record_incident(
                monitor_id,
                created.revision,
                PendingIncident {
                    daemon_epoch: epoch,
                    seq_start: newest_seq,
                    seq_end: newest_seq,
                    wall_time_start_ns: newest_seq as i64,
                    wall_time_end_ns: newest_seq as i64,
                    preview: "newest kernel panic".into(),
                },
                MonitorCheckpoint {
                    cursor: Cursor {
                        epoch,
                        after_seq: newest_seq,
                    },
                    cooldown_until_wall_time_ns: None,
                    pending: None,
                },
            )
            .await
            .unwrap();

        let state = manager.inner.state.read().await;
        let incidents = state.incidents.get(&monitor_id).unwrap();
        assert_eq!(incidents.len(), MAX_INCIDENTS_PER_MONITOR);
        assert_eq!(incidents.front().unwrap().incident_seq, 2);
        assert_eq!(incidents.back().unwrap().incident_seq, newest_seq);
        assert!(
            incidents
                .iter()
                .all(|incident| incident.acked_wall_time_ns.is_none())
        );
        let monitor = state.monitors.get(&monitor_id).unwrap();
        assert_eq!(monitor.incident_count, newest_seq);
        assert_eq!(
            monitor.unacked_incident_count,
            MAX_INCIDENTS_PER_MONITOR as u64
        );
        assert_eq!(monitor.gap_count, 1);
        drop(state);

        let page = manager
            .incidents(monitor_id, Some(0), Some(20), true)
            .await
            .unwrap();
        assert!(page.retention_gap);
        assert_eq!(page.first_available_incident_seq, Some(2));

        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[test]
    fn hard_retention_bounds_do_not_depend_on_acknowledgement() {
        let epoch = Uuid::new_v4();
        let oldest_monitor = Uuid::new_v4();
        let newer_monitor = Uuid::new_v4();
        let mut global = PersistedState::default();
        global
            .monitors
            .insert(oldest_monitor, stopped_monitor(oldest_monitor, epoch, 1));
        global
            .monitors
            .insert(newer_monitor, stopped_monitor(newer_monitor, epoch, 2));
        global.incidents.insert(
            oldest_monitor,
            VecDeque::from([retained_incident(oldest_monitor, epoch, 1)]),
        );
        global.incidents.insert(
            newer_monitor,
            VecDeque::from([retained_incident(newer_monitor, epoch, 2)]),
        );

        prune_one_global_incident(&mut global);
        assert!(global.incidents[&oldest_monitor].is_empty());
        assert_eq!(global.incidents[&newer_monitor].len(), 1);
        assert_eq!(global.monitors[&oldest_monitor].gap_count, 1);

        let mut catalog = PersistedState::default();
        let mut ids = Vec::new();
        for index in 0..MAX_MONITORS {
            let id = Uuid::new_v4();
            ids.push(id);
            catalog
                .monitors
                .insert(id, stopped_monitor(id, epoch, index as i64));
            catalog
                .incidents
                .insert(id, VecDeque::from([retained_incident(id, epoch, 1)]));
        }
        prune_monitor_capacity(&mut catalog, wall_time_ns());
        assert_eq!(catalog.monitors.len(), MAX_MONITORS - 1);
        assert!(!catalog.monitors.contains_key(&ids[0]));
        assert!(catalog.monitors.contains_key(ids.last().unwrap()));
    }

    #[test]
    fn legal_retention_bounds_fit_the_atomic_state_file_budget() {
        assert!(MAX_ESTIMATED_STATE_BYTES < MAX_STATE_FILE_BYTES);
        assert_eq!(MAX_DESCRIPTION_BYTES, 1_024);
        assert_eq!(MAX_INCIDENTS_PER_MONITOR, 512);
        assert_eq!(MAX_INCIDENTS_TOTAL, 1_024);
        assert_eq!(MAX_OUTBOX_EVENTS, 512);
    }

    #[tokio::test]
    async fn incident_page_marks_a_cursor_before_retained_history_as_a_gap() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let revision = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor
            .revision;
        for seq in 1..=2 {
            manager
                .record_incident(
                    monitor_id,
                    revision,
                    PendingIncident {
                        daemon_epoch: epoch,
                        seq_start: seq,
                        seq_end: seq,
                        wall_time_start_ns: seq as i64,
                        wall_time_end_ns: seq as i64,
                        preview: format!("panic {seq}"),
                    },
                    MonitorCheckpoint {
                        cursor: Cursor {
                            epoch,
                            after_seq: seq,
                        },
                        cooldown_until_wall_time_ns: None,
                        pending: None,
                    },
                )
                .await
                .unwrap();
        }
        manager
            .inner
            .state
            .write()
            .await
            .incidents
            .get_mut(&monitor_id)
            .unwrap()
            .pop_front();
        let page = manager
            .incidents(monitor_id, Some(0), Some(20), true)
            .await
            .unwrap();
        assert!(page.retention_gap);
        assert_eq!(page.first_available_incident_seq, Some(2));
        assert_eq!(page.incidents[0].incident_seq, 2);
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_survives_restart_with_its_cooldown_barrier() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let created = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        let checkpoint = MonitorCheckpoint {
            cursor: Cursor {
                epoch,
                after_seq: 42,
            },
            cooldown_until_wall_time_ns: Some(wall_time_ns().saturating_add(ms_to_ns(30_000))),
            pending: None,
        };
        // Isolate the persistence contract under test. A live worker owns its
        // checkpoint and is expected to persist its own cursor when cancelled;
        // it must not race this deliberately injected checkpoint.
        manager.stop_worker(monitor_id, created.revision).await;
        manager
            .checkpoint_progress(monitor_id, created.revision, checkpoint.clone())
            .await
            .unwrap();
        manager.shutdown().await;

        let reopened = MonitorManager::open(
            temp.path().join("monitors.json"),
            registry.clone(),
            epoch,
            Uuid::new_v4(),
            MonitorEventSinkConfig::default(),
        )
        .unwrap();
        let recovered = reopened
            .checkpoint_for(
                &created,
                Cursor {
                    epoch,
                    after_seq: 0,
                },
            )
            .await;
        assert_eq!(recovered.cursor, checkpoint.cursor);
        assert_eq!(
            recovered.cooldown_until_wall_time_ns,
            checkpoint.cooldown_until_wall_time_ns
        );
        reopened.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn identical_checkpoint_skips_disk_write_but_a_rewind_is_persisted() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(created.id, created.revision).await;
        let forward = MonitorCheckpoint {
            cursor: Cursor {
                epoch,
                after_seq: 42,
            },
            cooldown_until_wall_time_ns: Some(wall_time_ns().saturating_add(ms_to_ns(30_000))),
            pending: None,
        };
        manager
            .checkpoint_progress(created.id, created.revision, forward.clone())
            .await
            .unwrap();

        manager.set_persist_failure(true);
        manager
            .checkpoint_progress(created.id, created.revision, forward.clone())
            .await
            .expect("an unchanged checkpoint must not touch persistence");
        let mut rewound = forward.clone();
        rewound.cursor.after_seq = 10;
        assert!(
            manager
                .checkpoint_progress(created.id, created.revision, rewound.clone())
                .await
                .is_err(),
            "a changed rewind must not be mistaken for a monotonic no-op"
        );

        manager.set_persist_failure(false);
        manager
            .checkpoint_progress(created.id, created.revision, rewound.clone())
            .await
            .unwrap();
        assert_eq!(
            manager
                .checkpoint_for(
                    &created,
                    Cursor {
                        epoch,
                        after_seq: 0,
                    },
                )
                .await,
            rewound
        );

        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn notification_ttl_does_not_delete_retained_incident_evidence() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, epoch) = fixture(&temp).await;
        let monitor_id = Uuid::new_v4();
        let revision = manager
            .create(CreateMonitorRequest {
                request_id: monitor_id,
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor
            .revision;
        manager
            .record_incident(
                monitor_id,
                revision,
                PendingIncident {
                    daemon_epoch: epoch,
                    seq_start: 1,
                    seq_end: 1,
                    wall_time_start_ns: 1,
                    wall_time_end_ns: 1,
                    preview: "kernel panic".into(),
                },
                MonitorCheckpoint {
                    cursor: Cursor {
                        epoch,
                        after_seq: 1,
                    },
                    cooldown_until_wall_time_ns: None,
                    pending: None,
                },
            )
            .await
            .unwrap();
        {
            let mut state = manager.inner.state.write().await;
            prune_expired_metadata(&mut state, i64::MAX);
        }
        assert_eq!(
            manager
                .get(monitor_id)
                .await
                .unwrap()
                .monitor
                .unacked_incident_count,
            1
        );
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn stale_revision_cannot_stop_a_restarted_monitor() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        let stopped = manager
            .stop(created.id, created.revision)
            .await
            .unwrap()
            .monitor;
        let error = manager
            .stop(created.id, created.revision)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            MonitorError::RevisionMismatch { actual, .. } if actual == stopped.revision
        ));
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_stop_persistence_restores_the_running_worker() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        manager.set_persist_failure(true);
        assert!(manager.stop(created.id, created.revision).await.is_err());
        manager.set_persist_failure(false);

        assert_eq!(
            manager.get(created.id).await.unwrap().monitor.status,
            MonitorStatus::Running
        );
        assert!(manager.inner.workers.lock().await.contains_key(&created.id));
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_worker_state_persist_keeps_a_running_revision_supervised() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        let created = manager
            .create(CreateMonitorRequest {
                request_id: Uuid::new_v4(),
                spec: spec("slot-1"),
            })
            .await
            .unwrap()
            .monitor;
        manager.stop_worker(created.id, created.revision).await;
        manager.set_persist_failure(true);
        assert!(
            manager
                .fail_monitor(created.id, created.revision, "injected worker error".into())
                .await
                .is_err()
        );
        manager.set_persist_failure(false);
        assert_eq!(
            manager.get(created.id).await.unwrap().monitor.status,
            MonitorStatus::Running
        );
        assert!(manager.inner.workers.lock().await.contains_key(&created.id));
        manager.shutdown().await;
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_waits_for_startup_before_draining_background_tasks() {
        let temp = TempDir::new().unwrap();
        let (manager, registry, journal, _) = fixture(&temp).await;
        manager.shutdown().await;
        assert!(manager.inner.workers.lock().await.is_empty());
        assert!(manager.inner.sink_task.lock().await.is_none());
        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[test]
    fn recovered_cooldown_rejects_replayed_match() {
        let epoch = Uuid::new_v4();
        let mut monitor_spec = spec("slot-1");
        monitor_spec.debounce_ms = 0;
        monitor_spec.cooldown_ms = 60_000;
        let mut first = WorkerRuntime::new(&monitor_spec, None, None).unwrap();
        let candidate = first
            .matcher
            .push(&rx_event(epoch, 1, 0, b"kernel panic"))
            .unwrap();
        let now = Instant::now();
        let now_wall_time_ns = wall_time_ns();
        assert!(
            first
                .accept_match(candidate, now, now_wall_time_ns)
                .is_some()
        );
        let checkpoint = first.checkpoint(&Cursor {
            epoch,
            after_seq: 1,
        });

        let mut recovered = WorkerRuntime::new(
            &monitor_spec,
            checkpoint.cooldown_until_wall_time_ns,
            checkpoint.pending,
        )
        .unwrap();
        let replayed = recovered
            .matcher
            .push(&rx_event(epoch, 2, 12, b"kernel panic"))
            .unwrap();
        assert!(
            recovered
                .accept_match(replayed, Instant::now(), wall_time_ns())
                .is_none()
        );
    }
}
