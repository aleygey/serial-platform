use crate::control::{AcquireOutcome, ControlError, ControlState, ReleaseOutcome};
use crate::journal::{JournalError, JournalHandle};
use crate::ring::{EventRing, ReplayError, ReplayWindow};
use chrono::Utc;
use serde_json::{Value, json};
use serial_protocol::{
    Actor, ActorKind, CommandResult, ControlMode, Cursor, DataBits, Direction, EventKind,
    FlowControl, LoggingState, Parity, RunInfo, RunStatus, SerialSettings, SessionState,
    SlotConfig, SlotSnapshot, StopBits, TargetActivity, TimelineEvent, WritePacing,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot, watch};
use tokio_serial::{
    DataBits as TokioDataBits, FlowControl as TokioFlowControl, Parity as TokioParity, SerialPort,
    SerialPortBuilderExt, SerialStream, StopBits as TokioStopBits,
};
use uuid::Uuid;

const COMMAND_QUEUE: usize = 256;
const PORT_EVENT_QUEUE: usize = 4_096;
const PORT_WRITE_QUEUE: usize = 128;
const BROADCAST_QUEUE: usize = 2_048;
const RING_EVENTS: usize = 20_000;
const RING_BYTES: usize = 4 * 1024 * 1024;
const RX_BUFFER_BYTES: usize = 4 * 1024;
const RX_COALESCE_WINDOW: Duration = Duration::from_millis(4);
const MAX_WRITE_BYTES: usize = 4 * 1024;
const MAX_LABEL_BYTES: usize = 256;
const MAX_RUN_METADATA_BYTES: usize = 16 * 1024;
const MAX_RUN_METADATA_KEYS: usize = 64;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const JOURNAL_ACK_TIMEOUT: Duration = Duration::from_millis(100);
const IDEMPOTENCY_ENTRIES: usize = 2_048;
const WRITE_IDEMPOTENCY_HISTORY_ENTRIES: usize = 262_144;
const ACTIVE_WINDOW: Duration = Duration::from_secs(5);
const OPEN_BACKOFF_MIN: Duration = Duration::from_millis(500);
const OPEN_BACKOFF_MAX: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct SlotHandle {
    slot_id: Arc<str>,
    commands: mpsc::Sender<SlotCommand>,
    snapshot: watch::Receiver<SlotSnapshot>,
    events: broadcast::Sender<TimelineEvent>,
    ring: Arc<Mutex<EventRing>>,
}

pub struct AttachState {
    pub snapshot: SlotSnapshot,
    pub replay: ReplayWindow,
    pub live: broadcast::Receiver<TimelineEvent>,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SlotError {
    #[error("slot command queue is closed")]
    Closed,
    #[error("serial port is offline")]
    PortOffline,
    #[error(
        "serial write failed before all bytes were accepted ({written}/{total}); generation={generation}, tx_event={event_seq:?}, operation={operation_id:?}: {message}"
    )]
    PartialWrite {
        written: usize,
        total: usize,
        generation: u64,
        event_seq: Option<u64>,
        operation_id: Option<Uuid>,
        message: String,
    },
    #[error("{0}")]
    Control(#[from] ControlError),
    #[error("an active Run already exists")]
    RunAlreadyActive,
    #[error("there is no active Run")]
    NoActiveRun,
    #[error("the Run id does not match the active Run")]
    RunMismatch,
    #[error("cursor is ahead of the current timeline")]
    CursorAhead,
    #[error("slot actor stopped before replying")]
    ReplyDropped,
    #[error("slot id cannot change while reconfiguring an existing slot")]
    SlotIdChanged,
    #[error("serial write exceeds the {MAX_WRITE_BYTES}-byte request limit")]
    WriteTooLarge,
    #[error("serial write must contain at least one byte")]
    EmptyWrite,
    #[error("request_id was already used with different request content")]
    RequestIdReused,
    #[error(
        "request_id was executed earlier in this daemon epoch, but its result is no longer cached; the write was not repeated"
    )]
    WriteResultExpired,
    #[error(
        "the Slot has reached its bounded write idempotency history for this daemon epoch; restart seriald before accepting more writes"
    )]
    WriteIdempotencyCapacity,
    #[error("the write-control wait queue is full; retry after another waiter leaves")]
    ControlQueueFull,
    #[error(
        "label must be non-empty, trimmed, at most {MAX_LABEL_BYTES} bytes, and contain no control characters"
    )]
    InvalidLabel,
    #[error("Run metadata contains {actual} keys; the maximum is {MAX_RUN_METADATA_KEYS}")]
    RunMetadataTooManyKeys { actual: usize },
    #[error("Run metadata encodes to {actual} bytes; the maximum is {MAX_RUN_METADATA_BYTES}")]
    RunMetadataTooLarge { actual: usize },
}

impl From<ReplayError> for SlotError {
    fn from(_: ReplayError) -> Self {
        Self::CursorAhead
    }
}

impl SlotHandle {
    pub fn spawn(
        config: SlotConfig,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
    ) -> Self {
        Self::spawn_inner(config, daemon_epoch, daemon_started, journal, false)
    }

    /// Creates a candidate Slot actor that cannot open its port until the
    /// surrounding configuration transaction has been persisted and commits.
    pub(crate) fn spawn_staged(
        config: SlotConfig,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
    ) -> Self {
        Self::spawn_inner(config, daemon_epoch, daemon_started, journal, true)
    }

    fn spawn_inner(
        config: SlotConfig,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
        staged: bool,
    ) -> Self {
        let initial = initial_snapshot(config.clone(), daemon_epoch, staged);
        let (commands, command_rx) = mpsc::channel(COMMAND_QUEUE);
        let (events, _) = broadcast::channel(BROADCAST_QUEUE);
        let (snapshot_tx, snapshot) = watch::channel(initial);
        let ring = Arc::new(Mutex::new(EventRing::new(RING_EVENTS, RING_BYTES)));
        let handle = Self {
            slot_id: Arc::from(config.id.as_str()),
            commands,
            snapshot,
            events: events.clone(),
            ring: Arc::clone(&ring),
        };
        tokio::spawn(
            SlotActor {
                config,
                daemon_epoch,
                daemon_started,
                journal,
                commands: command_rx,
                events,
                snapshot: snapshot_tx,
                ring,
                seq: 0,
                generation: 0,
                rx_offset: 0,
                tx_offset: 0,
                endpoint_present: false,
                session_state: if staged {
                    SessionState::Disabled
                } else {
                    SessionState::WaitingForPort
                },
                state_reason: staged.then(|| "slot configuration pending persistence".into()),
                target_activity: TargetActivity::Unknown,
                last_rx_wall_time_ns: None,
                last_rx_instant: None,
                logging: LoggingState::Healthy,
                control: ControlState::new(daemon_epoch, 0),
                active_run: None,
                port: None,
                port_events: None,
                administratively_paused: staged,
                pending_reconfiguration: staged.then_some(PendingReconfiguration::Add),
                retry_at: Instant::now(),
                retry_delay: OPEN_BACKOFF_MIN,
                request_cache: HashMap::new(),
                request_order: VecDeque::new(),
                write_request_cache: HashMap::new(),
                write_request_order: VecDeque::new(),
                executed_write_ids: ExecutedWriteIds::new(WRITE_IDEMPOTENCY_HISTORY_ENTRIES),
            }
            .run(),
        );
        handle
    }

    pub fn id(&self) -> &str {
        &self.slot_id
    }

    pub fn snapshot(&self) -> SlotSnapshot {
        self.snapshot.borrow().clone()
    }

    pub async fn attach(
        &self,
        cursor: Option<&Cursor>,
        tail_events: usize,
    ) -> Result<AttachState, SlotError> {
        // Subscribe before taking the snapshot. The caller filters live events
        // through snapshot.head_seq, closing the attach race without stopping RX.
        let live = self.events.subscribe();
        let snapshot = self.snapshot();
        let replay = self.ring.lock().await.replay(
            snapshot.daemon_epoch,
            cursor,
            snapshot.head_seq,
            tail_events,
        )?;
        Ok(AttachState {
            snapshot,
            replay,
            live,
        })
    }

    pub async fn acquire_control(
        &self,
        request_id: Uuid,
        actor: Actor,
        mode: ControlMode,
        ttl_ms: u64,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::Acquire {
            request_id,
            actor,
            mode,
            ttl_ms,
            reply,
        })
        .await
    }

    pub async fn renew_control(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::Renew {
            request_id,
            actor,
            control_id,
            fence,
            ttl_ms,
            reply,
        })
        .await
    }

    pub async fn release_control(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::Release {
            request_id,
            actor,
            control_id,
            fence,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        pacing: Option<WritePacing>,
    ) -> Result<CommandResult, SlotError> {
        if data.len() > MAX_WRITE_BYTES {
            return Err(SlotError::WriteTooLarge);
        }
        self.request(|reply| SlotCommand::Write {
            request_id,
            actor,
            control_id,
            fence,
            data,
            operation_id,
            pacing,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_run(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
        metadata: BTreeMap<String, Value>,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::StartRun {
            request_id,
            actor,
            control_id,
            fence,
            label,
            metadata,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn end_run(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        run_id: Uuid,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::EndRun {
            request_id,
            actor,
            control_id,
            fence,
            run_id,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn checkpoint(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::Checkpoint {
            request_id,
            actor,
            control_id,
            fence,
            label,
            reply,
        })
        .await
    }

    pub async fn disconnect_actor(&self, actor_id: String) {
        let _ = self
            .commands
            .send(SlotCommand::DisconnectActor { actor_id })
            .await;
    }

    /// Stages a candidate config without publishing it. `resume_on_rollback`
    /// distinguishes an active Slot from an already-retired actor.
    pub(crate) async fn stage_reconfiguration(
        &self,
        config: SlotConfig,
        resume_on_rollback: bool,
    ) -> Result<(), SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SlotCommand::StageReconfiguration {
                config,
                resume_on_rollback,
                reply,
            })
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }

    pub(crate) async fn stage_removal(&self) -> Result<(), SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SlotCommand::StageRemoval { reply })
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }

    pub(crate) async fn commit_staged_reconfiguration(&self) -> Result<(), SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SlotCommand::CommitStagedReconfiguration { reply })
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }

    pub(crate) async fn rollback_staged_reconfiguration(&self) -> Result<(), SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SlotCommand::RollbackStagedReconfiguration { reply })
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }

    pub async fn shutdown(&self) {
        let (reply, wait) = oneshot::channel();
        if self
            .commands
            .send(SlotCommand::Shutdown { reply })
            .await
            .is_ok()
        {
            let _ = wait.await;
        }
    }

    async fn request(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<CommandResult, SlotError>>) -> SlotCommand,
    ) -> Result<CommandResult, SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(make(reply))
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }
}

enum SlotCommand {
    Acquire {
        request_id: Uuid,
        actor: Actor,
        mode: ControlMode,
        ttl_ms: u64,
        reply: Reply,
    },
    Renew {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
        reply: Reply,
    },
    Release {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        reply: Reply,
    },
    Write {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        pacing: Option<WritePacing>,
        reply: Reply,
    },
    StartRun {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
        metadata: BTreeMap<String, Value>,
        reply: Reply,
    },
    EndRun {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        run_id: Uuid,
        reply: Reply,
    },
    Checkpoint {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
        reply: Reply,
    },
    DisconnectActor {
        actor_id: String,
    },
    StageReconfiguration {
        config: SlotConfig,
        resume_on_rollback: bool,
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    StageRemoval {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    CommitStagedReconfiguration {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    RollbackStagedReconfiguration {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

type Reply = oneshot::Sender<Result<CommandResult, SlotError>>;

#[derive(Clone)]
struct CachedResult {
    fingerprint: Vec<u8>,
    result: Result<CommandResult, SlotError>,
}

#[derive(Debug)]
struct ExecutedWriteIds {
    ids: HashSet<Uuid>,
    limit: usize,
}

impl ExecutedWriteIds {
    fn new(limit: usize) -> Self {
        Self {
            ids: HashSet::new(),
            limit,
        }
    }

    /// Returns true when this request was executed but its detailed result has
    /// fallen out of the smaller result cache. IDs are never evicted within a
    /// daemon epoch, so an old retry is rejected instead of reaching the port.
    fn was_executed_or_reserveable(&self, request_id: Uuid) -> Result<bool, SlotError> {
        if self.ids.contains(&request_id) {
            return Ok(true);
        }
        if self.ids.len() >= self.limit {
            return Err(SlotError::WriteIdempotencyCapacity);
        }
        Ok(false)
    }

    fn remember(&mut self, request_id: Uuid) {
        let inserted = self.ids.insert(request_id);
        debug_assert!(inserted, "executed write request IDs are inserted once");
    }
}

enum PortCommand {
    Write {
        data: Vec<u8>,
        pacing: WritePacing,
        reply: oneshot::Sender<PortWriteOutcome>,
    },
}

struct PortWriteOutcome {
    written: usize,
    error: Option<String>,
    cancelled: bool,
}

enum PortEvent {
    Rx(Vec<u8>),
    Overflow { dropped_bytes: u64 },
    Closed(String),
}

struct PortWorker {
    commands: mpsc::Sender<PortCommand>,
    cancel: watch::Sender<bool>,
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
}

enum PendingReconfiguration {
    /// A newly-created actor. Its candidate config is already stored in
    /// `config`, but it starts paused and is not present in the active map.
    Add,
    /// A replacement config held entirely inside the actor until commit.
    Replace {
        config: Box<SlotConfig>,
        resume_on_rollback: bool,
    },
    /// An active Slot that will move to the retired map on commit.
    Remove,
}

struct SlotActor {
    config: SlotConfig,
    daemon_epoch: Uuid,
    daemon_started: Instant,
    journal: JournalHandle,
    commands: mpsc::Receiver<SlotCommand>,
    events: broadcast::Sender<TimelineEvent>,
    snapshot: watch::Sender<SlotSnapshot>,
    ring: Arc<Mutex<EventRing>>,
    seq: u64,
    generation: u64,
    rx_offset: u64,
    tx_offset: u64,
    endpoint_present: bool,
    session_state: SessionState,
    state_reason: Option<String>,
    target_activity: TargetActivity,
    last_rx_wall_time_ns: Option<i64>,
    last_rx_instant: Option<Instant>,
    logging: LoggingState,
    control: ControlState,
    active_run: Option<RunInfo>,
    port: Option<PortWorker>,
    port_events: Option<mpsc::Receiver<PortEvent>>,
    administratively_paused: bool,
    pending_reconfiguration: Option<PendingReconfiguration>,
    retry_at: Instant,
    retry_delay: Duration,
    request_cache: HashMap<(String, Uuid), CachedResult>,
    request_order: VecDeque<(String, Uuid)>,
    // Write idempotency intentionally outlives one WebSocket actor. A
    // reconnect receives a new server-issued actor ID, but retrying the same
    // request_id must not write the bytes to the physical port twice.
    write_request_cache: HashMap<Uuid, CachedResult>,
    write_request_order: VecDeque<Uuid>,
    executed_write_ids: ExecutedWriteIds,
}

impl SlotActor {
    async fn run(mut self) {
        if !self.config.enabled || !self.config.settings.auto_open {
            self.session_state = SessionState::Disabled;
            self.publish_snapshot().await;
        }
        let mut maintenance = tokio::time::interval(Duration::from_millis(250));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let port_event = async {
                match self.port_events.as_mut() {
                    Some(events) => events.recv().await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                command = self.commands.recv() => {
                    let Some(command) = command else { break };
                    if self.handle_command(command).await { break; }
                }
                event = port_event => {
                    if let Some(event) = event {
                        self.handle_port_event(event).await;
                    } else if self.port.is_some() {
                        self.handle_port_closed("serial worker stopped".into()).await;
                    }
                }
                _ = maintenance.tick() => self.maintain().await,
            }
        }

        self.stop_port().await;
        self.session_state = SessionState::Disabled;
        self.publish_snapshot().await;
    }

    async fn maintain(&mut self) {
        self.expire_control().await;

        if self.target_activity == TargetActivity::Active
            && self
                .last_rx_instant
                .is_some_and(|last| last.elapsed() >= ACTIVE_WINDOW)
        {
            self.target_activity = TargetActivity::Silent;
            self.publish_snapshot().await;
        }

        if self.port.is_none()
            && !self.administratively_paused
            && self.config.enabled
            && self.config.settings.auto_open
            && Instant::now() >= self.retry_at
        {
            self.try_open().await;
        }
    }

    async fn try_open(&mut self) {
        self.endpoint_present = endpoint_present(&self.config.port);
        self.session_state = SessionState::Opening;
        self.state_reason = None;
        self.publish_snapshot().await;
        self.emit(
            EventKind::SerialOpening,
            Direction::None,
            Vec::new(),
            Some(system_actor()),
            None,
            metadata([("port", json!(self.config.port))]),
        )
        .await;

        match open_port(&self.config.port, &self.config.settings) {
            Ok(stream) => {
                self.endpoint_present = true;
                self.generation = self.generation.saturating_add(1);
                if let Some(released) =
                    self.control
                        .change_generation(self.generation, wall_time_ns(), Instant::now())
                {
                    self.abort_run(
                        "serial generation changed",
                        Some(released.released.owner.clone()),
                    )
                    .await;
                    self.emit_release(released, EventKind::ControlRevoked).await;
                }
                let (worker, events) = spawn_port_worker(stream);
                self.port = Some(worker);
                self.port_events = Some(events);
                self.session_state = SessionState::Online;
                self.state_reason = None;
                self.target_activity = TargetActivity::Unknown;
                self.retry_delay = OPEN_BACKOFF_MIN;
                self.publish_snapshot().await;
                self.emit(
                    EventKind::SerialOpened,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    metadata([
                        ("port", json!(self.config.port)),
                        ("baud_rate", json!(self.config.settings.baud_rate)),
                    ]),
                )
                .await;
            }
            Err(error) => {
                self.session_state = SessionState::Backoff;
                self.state_reason = Some(error.to_string());
                self.schedule_retry();
                self.publish_snapshot().await;
                self.emit(
                    EventKind::SerialOpenFailed,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    metadata([("error", json!(error.to_string()))]),
                )
                .await;
            }
        }
    }

    async fn handle_port_event(&mut self, event: PortEvent) {
        match event {
            PortEvent::Rx(data) => {
                self.last_rx_wall_time_ns = Some(wall_time_ns());
                self.last_rx_instant = Some(Instant::now());
                self.target_activity = TargetActivity::Active;
                self.emit(
                    EventKind::Rx,
                    Direction::Rx,
                    data,
                    Some(device_actor()),
                    None,
                    BTreeMap::new(),
                )
                .await;
            }
            PortEvent::Overflow { dropped_bytes } => {
                self.rx_offset = self.rx_offset.saturating_add(dropped_bytes);
                self.emit(
                    EventKind::Gap,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    metadata([
                        ("reason", json!("serial receive queue overflow")),
                        ("dropped_bytes", json!(dropped_bytes)),
                    ]),
                )
                .await;
            }
            PortEvent::Closed(reason) => self.handle_port_closed(reason).await,
        }
    }

    async fn handle_port_closed(&mut self, reason: String) {
        self.stop_port().await;
        self.session_state = SessionState::Backoff;
        self.state_reason = Some(reason.clone());
        self.target_activity = TargetActivity::Unknown;
        if let Some(released) =
            self.control
                .change_generation(self.generation, wall_time_ns(), Instant::now())
        {
            self.abort_run(
                "serial port disconnected",
                Some(released.released.owner.clone()),
            )
            .await;
            self.emit_release(released, EventKind::ControlRevoked).await;
        } else {
            self.abort_run("serial port disconnected", None).await;
        }
        self.schedule_retry();
        self.publish_snapshot().await;
        self.emit(
            EventKind::SerialClosed,
            Direction::None,
            Vec::new(),
            Some(system_actor()),
            None,
            metadata([("reason", json!(reason))]),
        )
        .await;
    }

    async fn handle_command(&mut self, command: SlotCommand) -> bool {
        let (key, request, reply) = match command.into_request() {
            CommandDisposition::Request {
                key,
                request,
                reply,
            } => (key, request, reply),
            CommandDisposition::Disconnect { actor_id } => {
                if let Some(released) =
                    self.control
                        .disconnect(&actor_id, wall_time_ns(), Instant::now())
                {
                    self.abort_run(
                        "controlling client disconnected",
                        Some(released.released.owner.clone()),
                    )
                    .await;
                    self.emit_release(released, EventKind::ControlReleased)
                        .await;
                }
                return false;
            }
            CommandDisposition::StageReconfiguration {
                config,
                resume_on_rollback,
                reply,
            } => {
                let result = self.stage_reconfiguration(config, resume_on_rollback).await;
                let _ = reply.send(result);
                return false;
            }
            CommandDisposition::StageRemoval { reply } => {
                let result = self.stage_removal().await;
                let _ = reply.send(result);
                return false;
            }
            CommandDisposition::CommitStagedReconfiguration { reply } => {
                let result = self.commit_staged_reconfiguration().await;
                let _ = reply.send(result);
                return false;
            }
            CommandDisposition::RollbackStagedReconfiguration { reply } => {
                let result = self.rollback_staged_reconfiguration().await;
                let _ = reply.send(result);
                return false;
            }
            CommandDisposition::Shutdown { reply } => {
                self.prepare_shutdown().await;
                let _ = reply.send(());
                return true;
            }
        };
        if let Err(error) = request.validate_business_fields() {
            let _ = reply.send(Err(error));
            return false;
        }

        if let Some(fingerprint) = request.write_fingerprint() {
            // Expire/promote first so cache hits are authorized against the
            // current lease, not against the actor or fence from the original
            // connection.
            self.expire_control().await;
            if let Err(error) = request.validate_write_control(&self.control) {
                let _ = reply.send(Err(error));
                return false;
            }

            let request_id = key.1;
            if let Some(cached) = self.write_request_cache.get(&request_id) {
                let result = if cached.fingerprint == fingerprint {
                    cached.result.clone()
                } else {
                    Err(SlotError::RequestIdReused)
                };
                let _ = reply.send(result);
                return false;
            }
            match self
                .executed_write_ids
                .was_executed_or_reserveable(request_id)
            {
                Ok(true) => {
                    let _ = reply.send(Err(SlotError::WriteResultExpired));
                    return false;
                }
                Ok(false) => {}
                Err(error) => {
                    let _ = reply.send(Err(error));
                    return false;
                }
            }

            let result = self.execute(request).await;
            if is_cacheable_write_result(&result) {
                self.cache_write_result(request_id, fingerprint, result.clone());
            }
            let _ = reply.send(result);
            return false;
        }

        let fingerprint = request.fingerprint();
        if let Some(cached) = self.request_cache.get(&key) {
            let result = if cached.fingerprint == fingerprint {
                cached.result.clone()
            } else {
                Err(SlotError::RequestIdReused)
            };
            let _ = reply.send(result);
            return false;
        }

        let result = self.execute(request).await;
        self.cache_result(key, fingerprint, result.clone());
        let _ = reply.send(result);
        false
    }

    async fn execute(&mut self, command: SlotRequest) -> Result<CommandResult, SlotError> {
        self.expire_control().await;
        match command {
            SlotRequest::Acquire {
                actor,
                mode,
                ttl_ms,
                ..
            } => {
                if self.port.is_none() {
                    return Err(SlotError::PortOffline);
                }
                match self.control.acquire(
                    actor.clone(),
                    mode,
                    ttl_ms,
                    wall_time_ns(),
                    Instant::now(),
                ) {
                    AcquireOutcome::Granted(lease) => {
                        self.emit_control_granted(&lease).await;
                        Ok(CommandResult::ControlGranted { lease })
                    }
                    AcquireOutcome::AlreadyHeld(lease) => {
                        Ok(CommandResult::ControlGranted { lease })
                    }
                    AcquireOutcome::Queued { position } => {
                        Ok(CommandResult::ControlQueued { position })
                    }
                    AcquireOutcome::QueueFull => Err(SlotError::ControlQueueFull),
                    AcquireOutcome::TakenOver { revoked, granted } => {
                        self.abort_run("human takeover", Some(revoked.owner.clone()))
                            .await;
                        self.emit(
                            EventKind::ControlRevoked,
                            Direction::None,
                            Vec::new(),
                            Some(actor.clone()),
                            None,
                            metadata([(
                                "lease",
                                serde_json::to_value(&revoked).unwrap_or(Value::Null),
                            )]),
                        )
                        .await;
                        self.emit_control_granted(&granted).await;
                        Ok(CommandResult::ControlGranted { lease: granted })
                    }
                }
            }
            SlotRequest::Renew {
                actor,
                control_id,
                fence,
                ttl_ms,
                ..
            } => {
                let lease = self.control.renew(
                    &actor.id,
                    control_id,
                    fence,
                    ttl_ms,
                    wall_time_ns(),
                    Instant::now(),
                )?;
                self.publish_snapshot().await;
                Ok(CommandResult::ControlRenewed { lease })
            }
            SlotRequest::Release {
                actor,
                control_id,
                fence,
                ..
            } => {
                let released = self.control.release(
                    &actor.id,
                    control_id,
                    fence,
                    wall_time_ns(),
                    Instant::now(),
                )?;
                self.abort_run("control released", Some(actor)).await;
                self.emit_release(released, EventKind::ControlReleased)
                    .await;
                Ok(CommandResult::ControlReleased)
            }
            SlotRequest::Write {
                actor,
                control_id,
                fence,
                data,
                operation_id,
                pacing,
                ..
            } => {
                if data.len() > MAX_WRITE_BYTES {
                    return Err(SlotError::WriteTooLarge);
                }
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                if data.is_empty() {
                    return Err(SlotError::EmptyWrite);
                }
                let Some(port) = &self.port else {
                    return Err(SlotError::PortOffline);
                };
                let total = data.len();
                let pacing = WritePacing::resolve(pacing, &self.config.settings);
                let (reply, result) = oneshot::channel();
                port.commands
                    .send(PortCommand::Write {
                        data: data.clone(),
                        pacing,
                        reply,
                    })
                    .await
                    .map_err(|_| SlotError::PortOffline)?;
                // Once accepted by the port worker queue, a lost reply cannot
                // prove that zero bytes reached the driver. Surface an
                // uncertain partial outcome so the request_id is retained in
                // the cross-connection write cache and a blind retry cannot
                // duplicate a possibly completed command.
                let outcome = result.await.map_err(|_| SlotError::PartialWrite {
                    written: 0,
                    total,
                    generation: self.generation,
                    event_seq: None,
                    operation_id,
                    message: "serial writer stopped before confirming the outcome; the physical write may have occurred".into(),
                })?;
                let event_seq = if outcome.written > 0 {
                    Some(
                        self.emit(
                            EventKind::Tx,
                            Direction::Tx,
                            data[..outcome.written].to_vec(),
                            Some(actor),
                            operation_id,
                            metadata([("partial", json!(outcome.written != total))]),
                        )
                        .await,
                    )
                } else {
                    None
                };
                if outcome.written != total || outcome.error.is_some() {
                    return Err(SlotError::PartialWrite {
                        written: outcome.written,
                        total,
                        generation: self.generation,
                        event_seq,
                        operation_id,
                        message: outcome.error.unwrap_or_else(|| "short serial write".into()),
                    });
                }
                Ok(CommandResult::WriteAccepted {
                    event_seq: event_seq.expect("full non-empty write emits TX"),
                })
            }
            SlotRequest::StartRun {
                actor,
                control_id,
                fence,
                label,
                metadata: run_metadata,
                ..
            } => {
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                if self.active_run.is_some() {
                    return Err(SlotError::RunAlreadyActive);
                }
                let run = RunInfo {
                    id: Uuid::new_v4(),
                    owner: actor.clone(),
                    label,
                    status: RunStatus::Active,
                    start_seq: self.seq.saturating_add(1),
                    end_seq: None,
                    metadata: run_metadata,
                };
                self.active_run = Some(run.clone());
                self.emit(
                    EventKind::RunStarted,
                    Direction::None,
                    Vec::new(),
                    Some(actor),
                    None,
                    metadata([("run", serde_json::to_value(&run).unwrap_or(Value::Null))]),
                )
                .await;
                Ok(CommandResult::RunStarted { run })
            }
            SlotRequest::EndRun {
                actor,
                control_id,
                fence,
                run_id,
                ..
            } => {
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                let active = self.active_run.as_ref().ok_or(SlotError::NoActiveRun)?;
                if active.id != run_id {
                    return Err(SlotError::RunMismatch);
                }
                let mut ended = self.active_run.take().expect("checked above");
                ended.status = RunStatus::Completed;
                ended.end_seq = Some(self.seq.saturating_add(1));
                self.emit_with_run(
                    EventKind::RunEnded,
                    Some(ended.id),
                    Some(actor),
                    metadata([("run", serde_json::to_value(&ended).unwrap_or(Value::Null))]),
                )
                .await;
                Ok(CommandResult::RunEnded { run: ended })
            }
            SlotRequest::Checkpoint {
                actor,
                control_id,
                fence,
                label,
                ..
            } => {
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                if self.active_run.is_none() {
                    return Err(SlotError::NoActiveRun);
                }
                let seq = self
                    .emit(
                        EventKind::Checkpoint,
                        Direction::None,
                        Vec::new(),
                        Some(actor),
                        None,
                        metadata([("label", json!(label))]),
                    )
                    .await;
                Ok(CommandResult::CheckpointCreated { event_seq: seq })
            }
        }
    }

    async fn emit_control_granted(&mut self, lease: &serial_protocol::ControlLease) {
        self.emit(
            EventKind::ControlGranted,
            Direction::None,
            Vec::new(),
            Some(lease.owner.clone()),
            None,
            metadata([("lease", serde_json::to_value(lease).unwrap_or(Value::Null))]),
        )
        .await;
    }

    async fn expire_control(&mut self) {
        let Some(released) = self.control.expire(wall_time_ns(), Instant::now()) else {
            return;
        };
        self.abort_run(
            "control lease expired",
            Some(released.released.owner.clone()),
        )
        .await;
        self.emit_release(released, EventKind::ControlExpired).await;
    }

    async fn emit_release(&mut self, outcome: ReleaseOutcome, kind: EventKind) {
        self.emit(
            kind,
            Direction::None,
            Vec::new(),
            Some(outcome.released.owner),
            None,
            BTreeMap::new(),
        )
        .await;
        if let Some(promoted) = outcome.promoted {
            self.emit_control_granted(&promoted).await;
        }
    }

    async fn abort_run(&mut self, reason: &str, actor: Option<Actor>) {
        let Some(mut run) = self.active_run.take() else {
            return;
        };
        run.status = RunStatus::Aborted;
        run.end_seq = Some(self.seq.saturating_add(1));
        self.emit_with_run(
            EventKind::RunAborted,
            Some(run.id),
            actor.or_else(|| Some(system_actor())),
            metadata([
                ("reason", json!(reason)),
                ("run", serde_json::to_value(&run).unwrap_or(Value::Null)),
            ]),
        )
        .await;
    }

    async fn emit_with_run(
        &mut self,
        kind: EventKind,
        run_id: Option<Uuid>,
        actor: Option<Actor>,
        metadata: BTreeMap<String, Value>,
    ) -> u64 {
        self.emit_inner(
            kind,
            Direction::None,
            Vec::new(),
            actor,
            None,
            run_id,
            metadata,
        )
        .await
    }

    async fn emit(
        &mut self,
        kind: EventKind,
        direction: Direction,
        data: Vec<u8>,
        actor: Option<Actor>,
        operation_id: Option<Uuid>,
        metadata: BTreeMap<String, Value>,
    ) -> u64 {
        let run_id = self.active_run.as_ref().map(|run| run.id);
        self.emit_inner(kind, direction, data, actor, operation_id, run_id, metadata)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_inner(
        &mut self,
        kind: EventKind,
        direction: Direction,
        data: Vec<u8>,
        actor: Option<Actor>,
        operation_id: Option<Uuid>,
        run_id: Option<Uuid>,
        metadata: BTreeMap<String, Value>,
    ) -> u64 {
        self.seq = self.seq.saturating_add(1);
        let event_seq = self.seq;
        let (start, end) = match direction {
            Direction::Rx => {
                let start = self.rx_offset;
                self.rx_offset = self.rx_offset.saturating_add(data.len() as u64);
                (Some(start), Some(self.rx_offset))
            }
            Direction::Tx => {
                let start = self.tx_offset;
                self.tx_offset = self.tx_offset.saturating_add(data.len() as u64);
                (Some(start), Some(self.tx_offset))
            }
            Direction::None => (None, None),
        };
        let event = TimelineEvent {
            slot_id: self.config.id.clone(),
            daemon_epoch: self.daemon_epoch,
            seq: event_seq,
            generation: self.generation,
            wall_time_ns: wall_time_ns(),
            monotonic_time_ns: self
                .daemon_started
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            kind,
            direction,
            actor,
            run_id,
            operation_id,
            stream_offset_start: start,
            stream_offset_end: end,
            data,
            metadata,
            durable: false,
        };

        let mut degradation = None;
        let event = match self.journal.try_append(event.clone()) {
            Ok(pending) if self.logging == LoggingState::Healthy => {
                match tokio::time::timeout(JOURNAL_ACK_TIMEOUT, pending.wait()).await {
                    Ok(Ok(durable)) => durable,
                    Ok(Err(error)) => {
                        if self.mark_logging_degraded(&error) {
                            degradation = Some(error.to_string());
                        }
                        event
                    }
                    Err(_) => {
                        let error = "journal acknowledgement timed out; continuing live delivery";
                        if self.mark_logging_degraded_message(error) {
                            degradation = Some(error.into());
                        }
                        event
                    }
                }
            }
            Ok(_pending) => event,
            Err(error) => {
                if self.mark_logging_degraded(&error) {
                    degradation = Some(error.to_string());
                }
                event
            }
        };
        self.ring.lock().await.push(event.clone());
        self.publish_snapshot().await;
        let _ = self.events.send(event);
        if let Some(error) = degradation {
            self.publish_nondurable_logging_event(error).await;
        }
        event_seq
    }

    fn mark_logging_degraded(&mut self, error: &JournalError) -> bool {
        self.mark_logging_degraded_message(&error.to_string())
    }

    fn mark_logging_degraded_message(&mut self, error: &str) -> bool {
        let changed = self.logging != LoggingState::Degraded;
        self.logging = LoggingState::Degraded;
        self.state_reason = Some(format!("journal degraded: {error}"));
        changed
    }

    async fn publish_nondurable_logging_event(&mut self, error: String) {
        self.seq = self.seq.saturating_add(1);
        let event = TimelineEvent {
            slot_id: self.config.id.clone(),
            daemon_epoch: self.daemon_epoch,
            seq: self.seq,
            generation: self.generation,
            wall_time_ns: wall_time_ns(),
            monotonic_time_ns: self
                .daemon_started
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            kind: EventKind::LoggingDegraded,
            direction: Direction::None,
            actor: Some(system_actor()),
            run_id: self.active_run.as_ref().map(|run| run.id),
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: Vec::new(),
            metadata: metadata([("error", json!(error))]),
            durable: false,
        };
        self.ring.lock().await.push(event.clone());
        self.publish_snapshot().await;
        let _ = self.events.send(event);
    }

    async fn publish_snapshot(&self) {
        let oldest = self.ring.lock().await.oldest_seq();
        self.snapshot.send_replace(SlotSnapshot {
            config: self.config.clone(),
            daemon_epoch: self.daemon_epoch,
            head_seq: self.seq,
            ring_oldest_seq: oldest,
            generation: self.generation,
            endpoint_present: self.endpoint_present,
            session_state: self.session_state,
            state_reason: self.state_reason.clone(),
            target_activity: self.target_activity,
            last_rx_wall_time_ns: self.last_rx_wall_time_ns,
            rx_offset: self.rx_offset,
            tx_offset: self.tx_offset,
            control: self.control.current().cloned(),
            active_run: self.active_run.clone(),
            logging: self.logging,
        });
    }

    fn schedule_retry(&mut self) {
        self.retry_at = Instant::now() + self.retry_delay;
        self.retry_delay = (self.retry_delay * 2).min(OPEN_BACKOFF_MAX);
    }

    async fn pause_for_reconfigure(&mut self) -> Result<(), SlotError> {
        if self.administratively_paused {
            return Ok(());
        }
        self.administratively_paused = true;
        let was_online = self.port.is_some();
        self.stop_port().await;
        if let Some(released) =
            self.control
                .change_generation(self.generation, wall_time_ns(), Instant::now())
        {
            self.abort_run(
                "slot reconfiguration",
                Some(released.released.owner.clone()),
            )
            .await;
            self.emit_release(released, EventKind::ControlRevoked).await;
        } else {
            self.abort_run("slot reconfiguration", None).await;
        }
        self.session_state = SessionState::Disabled;
        self.state_reason = Some("slot reconfiguration in progress".into());
        self.target_activity = TargetActivity::Unknown;
        if was_online {
            self.emit(
                EventKind::SerialClosed,
                Direction::None,
                Vec::new(),
                Some(system_actor()),
                None,
                metadata([("reason", json!("slot reconfiguration"))]),
            )
            .await;
        } else {
            self.publish_snapshot().await;
        }
        Ok(())
    }

    async fn stage_reconfiguration(
        &mut self,
        config: SlotConfig,
        resume_on_rollback: bool,
    ) -> Result<(), SlotError> {
        if config.id != self.config.id {
            return Err(SlotError::SlotIdChanged);
        }
        debug_assert!(self.pending_reconfiguration.is_none());
        self.pause_for_reconfigure().await?;
        self.pending_reconfiguration = Some(PendingReconfiguration::Replace {
            config: Box::new(config),
            resume_on_rollback,
        });
        Ok(())
    }

    async fn stage_removal(&mut self) -> Result<(), SlotError> {
        debug_assert!(self.pending_reconfiguration.is_none());
        self.pause_for_reconfigure().await?;
        self.pending_reconfiguration = Some(PendingReconfiguration::Remove);
        Ok(())
    }

    async fn commit_staged_reconfiguration(&mut self) -> Result<(), SlotError> {
        let Some(pending) = self.pending_reconfiguration.take() else {
            debug_assert!(false, "commit requires a staged Slot change");
            return Err(SlotError::ReplyDropped);
        };
        match pending {
            PendingReconfiguration::Add => {
                self.resume_current_config();
                self.publish_snapshot().await;
            }
            PendingReconfiguration::Replace { config, .. } => {
                self.apply_committed_reconfiguration(*config).await;
            }
            PendingReconfiguration::Remove => {
                self.emit(
                    EventKind::SlotRemoved,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    metadata([("reason", json!("slot removed from active configuration"))]),
                )
                .await;
            }
        }
        Ok(())
    }

    async fn rollback_staged_reconfiguration(&mut self) -> Result<(), SlotError> {
        let Some(pending) = self.pending_reconfiguration.take() else {
            debug_assert!(false, "rollback requires a staged Slot change");
            return Err(SlotError::ReplyDropped);
        };
        match pending {
            PendingReconfiguration::Add => {
                // New candidate actors are shut down by the Registry after
                // rollback; keep the physical port parked until then.
            }
            PendingReconfiguration::Replace {
                resume_on_rollback, ..
            } => {
                if resume_on_rollback {
                    self.resume_current_config();
                    self.publish_snapshot().await;
                }
            }
            PendingReconfiguration::Remove => {
                self.resume_current_config();
                self.publish_snapshot().await;
            }
        }
        Ok(())
    }

    async fn apply_committed_reconfiguration(&mut self, config: SlotConfig) {
        let previous = std::mem::replace(&mut self.config, config);
        self.resume_current_config();
        self.emit(
            EventKind::SlotReconfigured,
            Direction::None,
            Vec::new(),
            Some(system_actor()),
            None,
            metadata([
                (
                    "previous",
                    serde_json::to_value(previous).unwrap_or(Value::Null),
                ),
                (
                    "current",
                    serde_json::to_value(&self.config).unwrap_or(Value::Null),
                ),
            ]),
        )
        .await;
    }

    fn resume_current_config(&mut self) {
        self.endpoint_present = false;
        self.last_rx_instant = None;
        self.last_rx_wall_time_ns = None;
        self.target_activity = TargetActivity::Unknown;
        self.retry_at = Instant::now();
        self.retry_delay = OPEN_BACKOFF_MIN;
        self.administratively_paused = false;
        if self.config.enabled && self.config.settings.auto_open {
            self.session_state = SessionState::WaitingForPort;
            self.state_reason = None;
        } else {
            self.session_state = SessionState::Disabled;
            self.state_reason = None;
        }
    }

    async fn prepare_shutdown(&mut self) {
        self.administratively_paused = true;
        let was_online = self.port.is_some();
        self.stop_port().await;
        if let Some(released) =
            self.control
                .change_generation(self.generation, wall_time_ns(), Instant::now())
        {
            self.abort_run("slot shutdown", Some(released.released.owner.clone()))
                .await;
            self.emit_release(released, EventKind::ControlRevoked).await;
        } else {
            self.abort_run("slot shutdown", None).await;
        }
        self.session_state = SessionState::Disabled;
        self.state_reason = Some("slot stopped".into());
        self.target_activity = TargetActivity::Unknown;
        if was_online {
            self.emit(
                EventKind::SerialClosed,
                Direction::None,
                Vec::new(),
                Some(system_actor()),
                None,
                metadata([("reason", json!("slot shutdown"))]),
            )
            .await;
        } else {
            self.publish_snapshot().await;
        }
    }

    fn cache_result(
        &mut self,
        key: (String, Uuid),
        fingerprint: Vec<u8>,
        result: Result<CommandResult, SlotError>,
    ) {
        if self.request_cache.contains_key(&key) {
            return;
        }
        self.request_cache.insert(
            key.clone(),
            CachedResult {
                fingerprint,
                result,
            },
        );
        self.request_order.push_back(key);
        while self.request_order.len() > IDEMPOTENCY_ENTRIES {
            if let Some(oldest) = self.request_order.pop_front() {
                self.request_cache.remove(&oldest);
            }
        }
    }

    fn cache_write_result(
        &mut self,
        request_id: Uuid,
        fingerprint: Vec<u8>,
        result: Result<CommandResult, SlotError>,
    ) {
        if self.write_request_cache.contains_key(&request_id) {
            return;
        }
        self.write_request_cache.insert(
            request_id,
            CachedResult {
                fingerprint,
                result,
            },
        );
        self.executed_write_ids.remember(request_id);
        self.write_request_order.push_back(request_id);
        while self.write_request_order.len() > IDEMPOTENCY_ENTRIES {
            if let Some(oldest) = self.write_request_order.pop_front() {
                self.write_request_cache.remove(&oldest);
            }
        }
    }

    async fn stop_port(&mut self) {
        if let Some(port) = self.port.take() {
            let _ = port.cancel.send(true);
            drop(port.commands);
            let _ = port.reader.await;
            let _ = port.writer.await;
        }
        self.port_events = None;
    }
}

enum SlotRequest {
    Acquire {
        actor: Actor,
        mode: ControlMode,
        ttl_ms: u64,
    },
    Renew {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
    },
    Release {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
    },
    Write {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        pacing: Option<WritePacing>,
    },
    StartRun {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
        metadata: BTreeMap<String, Value>,
    },
    EndRun {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        run_id: Uuid,
    },
    Checkpoint {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        label: String,
    },
}

impl SlotRequest {
    fn validate_business_fields(&self) -> Result<(), SlotError> {
        match self {
            Self::StartRun {
                label, metadata, ..
            } => {
                validate_label(label)?;
                if metadata.len() > MAX_RUN_METADATA_KEYS {
                    return Err(SlotError::RunMetadataTooManyKeys {
                        actual: metadata.len(),
                    });
                }
                let encoded_bytes = serde_json::to_vec(metadata)
                    .expect("serde_json::Value metadata is serializable")
                    .len();
                if encoded_bytes > MAX_RUN_METADATA_BYTES {
                    return Err(SlotError::RunMetadataTooLarge {
                        actual: encoded_bytes,
                    });
                }
                Ok(())
            }
            Self::Checkpoint { label, .. } => validate_label(label),
            _ => Ok(()),
        }
    }

    /// Fingerprint fields that describe the intended physical write. Actor,
    /// lease ID, and fence are deliberately excluded because those are
    /// connection-scoped authorization data and change after reconnect.
    fn write_fingerprint(&self) -> Option<Vec<u8>> {
        let Self::Write {
            data,
            operation_id,
            pacing,
            ..
        } = self
        else {
            return None;
        };
        Some(
            serde_json::to_vec(&(data, operation_id, pacing))
                .expect("write request fields are serializable"),
        )
    }

    fn validate_write_control(&self, control: &ControlState) -> Result<(), SlotError> {
        let Self::Write {
            actor,
            control_id,
            fence,
            ..
        } = self
        else {
            return Ok(());
        };
        control.validate(&actor.id, *control_id, *fence, Instant::now())?;
        Ok(())
    }

    fn fingerprint(&self) -> Vec<u8> {
        match self {
            Self::Acquire {
                actor,
                mode,
                ttl_ms,
            } => serde_json::to_vec(&("acquire", &actor.id, mode, ttl_ms)),
            Self::Renew {
                actor,
                control_id,
                fence,
                ttl_ms,
            } => serde_json::to_vec(&("renew", &actor.id, control_id, fence, ttl_ms)),
            Self::Release {
                actor,
                control_id,
                fence,
            } => serde_json::to_vec(&("release", &actor.id, control_id, fence)),
            Self::Write {
                actor,
                control_id,
                fence,
                data,
                operation_id,
                pacing,
            } => serde_json::to_vec(&(
                "write",
                &actor.id,
                control_id,
                fence,
                data,
                operation_id,
                pacing,
            )),
            Self::StartRun {
                actor,
                control_id,
                fence,
                label,
                metadata,
            } => serde_json::to_vec(&("start_run", &actor.id, control_id, fence, label, metadata)),
            Self::EndRun {
                actor,
                control_id,
                fence,
                run_id,
            } => serde_json::to_vec(&("end_run", &actor.id, control_id, fence, run_id)),
            Self::Checkpoint {
                actor,
                control_id,
                fence,
                label,
            } => serde_json::to_vec(&("checkpoint", &actor.id, control_id, fence, label)),
        }
        .expect("Slot request fields are serializable")
    }
}

fn validate_label(label: &str) -> Result<(), SlotError> {
    if label.is_empty()
        || label != label.trim()
        || label.len() > MAX_LABEL_BYTES
        || label.chars().any(char::is_control)
    {
        Err(SlotError::InvalidLabel)
    } else {
        Ok(())
    }
}

fn is_cacheable_write_result(result: &Result<CommandResult, SlotError>) -> bool {
    matches!(
        result,
        Ok(CommandResult::WriteAccepted { .. }) | Err(SlotError::PartialWrite { .. })
    )
}

enum CommandDisposition {
    Request {
        key: (String, Uuid),
        request: SlotRequest,
        reply: Reply,
    },
    Disconnect {
        actor_id: String,
    },
    StageReconfiguration {
        config: SlotConfig,
        resume_on_rollback: bool,
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    StageRemoval {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    CommitStagedReconfiguration {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    RollbackStagedReconfiguration {
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

impl SlotCommand {
    fn into_request(self) -> CommandDisposition {
        match self {
            SlotCommand::Acquire {
                request_id,
                actor,
                reply,
                mode,
                ttl_ms,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Acquire {
                    actor,
                    mode,
                    ttl_ms,
                },
                reply,
            },
            SlotCommand::Renew {
                request_id,
                actor,
                reply,
                control_id,
                fence,
                ttl_ms,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Renew {
                    actor,
                    control_id,
                    fence,
                    ttl_ms,
                },
                reply,
            },
            SlotCommand::Release {
                request_id,
                actor,
                reply,
                control_id,
                fence,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Release {
                    actor,
                    control_id,
                    fence,
                },
                reply,
            },
            SlotCommand::Write {
                request_id,
                actor,
                reply,
                control_id,
                fence,
                data,
                operation_id,
                pacing,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Write {
                    actor,
                    control_id,
                    fence,
                    data,
                    operation_id,
                    pacing,
                },
                reply,
            },
            SlotCommand::StartRun {
                request_id,
                actor,
                reply,
                control_id,
                fence,
                label,
                metadata,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::StartRun {
                    actor,
                    control_id,
                    fence,
                    label,
                    metadata,
                },
                reply,
            },
            SlotCommand::EndRun {
                request_id,
                actor,
                reply,
                control_id,
                fence,
                run_id,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::EndRun {
                    actor,
                    control_id,
                    fence,
                    run_id,
                },
                reply,
            },
            SlotCommand::Checkpoint {
                request_id,
                actor,
                reply,
                control_id,
                fence,
                label,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Checkpoint {
                    actor,
                    control_id,
                    fence,
                    label,
                },
                reply,
            },
            SlotCommand::DisconnectActor { actor_id } => {
                CommandDisposition::Disconnect { actor_id }
            }
            SlotCommand::StageReconfiguration {
                config,
                resume_on_rollback,
                reply,
            } => CommandDisposition::StageReconfiguration {
                config,
                resume_on_rollback,
                reply,
            },
            SlotCommand::StageRemoval { reply } => CommandDisposition::StageRemoval { reply },
            SlotCommand::CommitStagedReconfiguration { reply } => {
                CommandDisposition::CommitStagedReconfiguration { reply }
            }
            SlotCommand::RollbackStagedReconfiguration { reply } => {
                CommandDisposition::RollbackStagedReconfiguration { reply }
            }
            SlotCommand::Shutdown { reply } => CommandDisposition::Shutdown { reply },
        }
    }
}

fn initial_snapshot(config: SlotConfig, daemon_epoch: Uuid, staged: bool) -> SlotSnapshot {
    let state = if staged {
        SessionState::Disabled
    } else if config.enabled && config.settings.auto_open {
        SessionState::WaitingForPort
    } else {
        SessionState::Disabled
    };
    SlotSnapshot {
        config,
        daemon_epoch,
        head_seq: 0,
        ring_oldest_seq: None,
        generation: 0,
        endpoint_present: false,
        session_state: state,
        state_reason: staged.then(|| "slot configuration pending persistence".into()),
        target_activity: TargetActivity::Unknown,
        last_rx_wall_time_ns: None,
        rx_offset: 0,
        tx_offset: 0,
        control: None,
        active_run: None,
        logging: LoggingState::Healthy,
    }
}

fn open_port(port_name: &str, settings: &SerialSettings) -> Result<SerialStream, String> {
    let builder = tokio_serial::new(port_name, settings.baud_rate)
        .data_bits(match settings.data_bits {
            DataBits::Five => TokioDataBits::Five,
            DataBits::Six => TokioDataBits::Six,
            DataBits::Seven => TokioDataBits::Seven,
            DataBits::Eight => TokioDataBits::Eight,
        })
        .parity(match settings.parity {
            Parity::None => TokioParity::None,
            Parity::Odd => TokioParity::Odd,
            Parity::Even => TokioParity::Even,
        })
        .stop_bits(match settings.stop_bits {
            StopBits::One => TokioStopBits::One,
            StopBits::Two => TokioStopBits::Two,
        })
        .dtr_on_open(settings.dtr)
        .flow_control(match settings.flow_control {
            FlowControl::None => TokioFlowControl::None,
            FlowControl::Software => TokioFlowControl::Software,
            FlowControl::Hardware => TokioFlowControl::Hardware,
        });
    let mut stream = builder
        .open_native_async()
        .map_err(|error| error.to_string())?;
    stream
        .write_data_terminal_ready(settings.dtr)
        .map_err(|error| error.to_string())?;
    // With hardware flow control the driver owns RTS. Manually forcing the
    // line can defeat CTS/RTS negotiation and may reset some target boards.
    if settings.flow_control != FlowControl::Hardware {
        stream
            .write_request_to_send(settings.rts)
            .map_err(|error| error.to_string())?;
    }
    Ok(stream)
}

fn spawn_port_worker(stream: SerialStream) -> (PortWorker, mpsc::Receiver<PortEvent>) {
    let (commands, command_rx) = mpsc::channel(PORT_WRITE_QUEUE);
    let (events, event_rx) = mpsc::channel(PORT_EVENT_QUEUE);
    let (cancel, cancel_rx) = watch::channel(false);
    let (reader_half, writer_half) = tokio::io::split(stream);
    let reader = tokio::spawn(run_port_reader(
        reader_half,
        events.clone(),
        cancel_rx.clone(),
    ));
    let writer = tokio::spawn(run_port_writer(writer_half, command_rx, events, cancel_rx));
    (
        PortWorker {
            commands,
            cancel,
            reader,
            writer,
        },
        event_rx,
    )
}

async fn run_port_reader(
    mut reader: tokio::io::ReadHalf<SerialStream>,
    events: mpsc::Sender<PortEvent>,
    mut cancel: watch::Receiver<bool>,
) {
    let mut buffer = vec![0_u8; RX_BUFFER_BYTES];
    let mut pending = Vec::with_capacity(RX_BUFFER_BYTES);
    let mut dropped_bytes = 0_u64;
    let mut flush_deadline = None;

    loop {
        let deadline = flush_deadline;
        let flush = async move {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
            }
            _ = flush => {
                enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                flush_deadline = (!pending.is_empty() || dropped_bytes > 0)
                    .then(|| tokio::time::Instant::now() + RX_COALESCE_WINDOW);
            }
            read = reader.read(&mut buffer) => match read {
                Ok(0) => {
                    enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    send_port_closed(
                        &events,
                        &mut cancel,
                        "serial port reached EOF".into(),
                    )
                    .await;
                    return;
                }
                Ok(count) => {
                    pending.extend_from_slice(&buffer[..count]);
                    if pending.len() >= RX_BUFFER_BYTES {
                        enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    }
                    flush_deadline = Some(tokio::time::Instant::now() + RX_COALESCE_WINDOW);
                }
                Err(error) => {
                    enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    send_port_closed(&events, &mut cancel, error.to_string()).await;
                    return;
                }
            }
        }
    }
}

fn enqueue_rx(events: &mpsc::Sender<PortEvent>, pending: &mut Vec<u8>, dropped_bytes: &mut u64) {
    if *dropped_bytes > 0 {
        match events.try_send(PortEvent::Overflow {
            dropped_bytes: *dropped_bytes,
        }) {
            Ok(()) => *dropped_bytes = 0,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                pending.clear();
                return;
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                *dropped_bytes = dropped_bytes.saturating_add(pending.len() as u64);
                pending.clear();
                return;
            }
        }
    }
    if pending.is_empty() {
        return;
    }
    let data = std::mem::take(pending);
    let length = data.len() as u64;
    match events.try_send(PortEvent::Rx(data)) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            *dropped_bytes = dropped_bytes.saturating_add(length);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

async fn run_port_writer(
    mut writer: tokio::io::WriteHalf<SerialStream>,
    mut command_rx: mpsc::Receiver<PortCommand>,
    events: mpsc::Sender<PortEvent>,
    mut cancel: watch::Receiver<bool>,
) {
    loop {
        let command = tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return;
                }
                continue;
            }
            command = command_rx.recv() => command,
        };
        let Some(PortCommand::Write {
            data,
            pacing,
            reply,
        }) = command
        else {
            return;
        };
        let outcome = write_with_pacing(&mut writer, &data, pacing, &mut cancel).await;
        let failed = outcome.error.is_some();
        let cancelled = outcome.cancelled;
        let message = outcome.error.clone();
        let _ = reply.send(outcome);
        if cancelled {
            return;
        }
        if failed {
            send_port_closed(
                &events,
                &mut cancel,
                message.unwrap_or_else(|| "serial write failed".into()),
            )
            .await;
            return;
        }
    }
}

/// Writes `data` to the driver in `pacing.chunk_size` byte chunks, sleeping
/// `pacing.chunk_delay_ms` between chunks (never after the final chunk) so a
/// slow target UART is not overrun. A zero chunk delay keeps the original
/// full-speed path with no sleeps. A chunk size of zero is treated as one
/// byte. The deadline is the fixed write timeout extended to twice the
/// estimated pacing duration, so large paced writes cannot time out merely
/// because pacing made them slower.
async fn write_with_pacing<W>(
    writer: &mut W,
    data: &[u8],
    pacing: WritePacing,
    cancel: &mut watch::Receiver<bool>,
) -> PortWriteOutcome
where
    W: AsyncWriteExt + Unpin,
{
    let chunk_size = (pacing.chunk_size as usize).max(1);
    let chunk_delay = Duration::from_millis(pacing.chunk_delay_ms);
    let deadline =
        tokio::time::Instant::now() + write_deadline(data.len(), chunk_size, chunk_delay);
    let mut written = 0;
    let mut error = None;
    let mut cancelled = false;
    'chunks: while written < data.len() {
        let chunk_end = written.saturating_add(chunk_size).min(data.len());
        // One chunk can still need several driver calls when a write is
        // accepted only partially.
        while written < chunk_end {
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        error = Some("serial write cancelled because the port is closing".into());
                        cancelled = true;
                        break;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    error = Some("serial write timed out; port state is uncertain".into());
                    break;
                }
                result = writer.write(&data[written..chunk_end]) => match result {
                    Ok(0) => {
                        error = Some("serial driver accepted zero bytes".into());
                        break;
                    }
                    Ok(count) => written += count,
                    Err(write_error) => {
                        error = Some(write_error.to_string());
                        break;
                    }
                }
            }
            if error.is_some() {
                break 'chunks;
            }
        }
        if written >= data.len() || chunk_delay.is_zero() {
            continue;
        }
        tokio::select! {
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    error = Some("serial write cancelled because the port is closing".into());
                    cancelled = true;
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                error = Some("serial write timed out; port state is uncertain".into());
            }
            _ = tokio::time::sleep(chunk_delay) => {}
        }
        if error.is_some() {
            break;
        }
    }
    PortWriteOutcome {
        written,
        error,
        cancelled,
    }
}

/// Deadline budget for one paced write: at least the fixed write timeout,
/// otherwise twice the estimated inter-chunk pacing duration plus the fixed
/// timeout as slack for driver latency.
fn write_deadline(total_bytes: usize, chunk_size: usize, chunk_delay: Duration) -> Duration {
    if chunk_delay.is_zero() {
        return WRITE_TIMEOUT;
    }
    let chunk_count = total_bytes.div_ceil(chunk_size.max(1)) as u128;
    let pacing_millis = chunk_count
        .saturating_sub(1)
        .saturating_mul(chunk_delay.as_millis())
        .min(u64::MAX as u128) as u64;
    let estimated = Duration::from_millis(pacing_millis);
    WRITE_TIMEOUT.max(estimated.saturating_mul(2).saturating_add(WRITE_TIMEOUT))
}

async fn send_port_closed(
    events: &mpsc::Sender<PortEvent>,
    cancel: &mut watch::Receiver<bool>,
    reason: String,
) {
    tokio::select! {
        _ = cancel.changed() => {}
        _ = events.send(PortEvent::Closed(reason)) => {}
    }
}

fn endpoint_present(port_name: &str) -> bool {
    serialport::available_ports().is_ok_and(|ports| {
        ports
            .iter()
            .any(|port| port.port_name.eq_ignore_ascii_case(port_name))
    })
}

fn wall_time_ns() -> i64 {
    Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis().saturating_mul(1_000_000))
}

fn metadata<const N: usize>(entries: [(&str, Value); N]) -> BTreeMap<String, Value> {
    entries
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

fn system_actor() -> Actor {
    Actor {
        id: "system:seriald".into(),
        label: "seriald".into(),
        kind: ActorKind::System,
    }
}

fn device_actor() -> Actor {
    Actor {
        id: "device".into(),
        label: "device".into(),
        kind: ActorKind::System,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::EchoMode;

    #[test]
    fn default_serial_settings_are_no_flow_control() {
        let settings = SerialSettings::default();
        assert_eq!(settings.flow_control, FlowControl::None);
        assert!(!settings.dtr);
        assert!(!settings.rts);
        assert_eq!(settings.write_eol, "\r");
        assert_eq!(settings.echo, EchoMode::On);
    }

    #[test]
    fn request_fingerprint_detects_reused_id_with_different_write_bytes() {
        let actor = Actor {
            id: "human:test".into(),
            label: "test".into(),
            kind: ActorKind::Human,
        };
        let control_id = Uuid::new_v4();
        let first = SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"first".to_vec(),
            operation_id: None,
            pacing: None,
        };
        let same = SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"first".to_vec(),
            operation_id: None,
            pacing: None,
        };
        let different = SlotRequest::Write {
            actor,
            control_id,
            fence: 7,
            data: b"second".to_vec(),
            operation_id: None,
            pacing: None,
        };
        assert_eq!(first.fingerprint(), same.fingerprint());
        assert_ne!(first.fingerprint(), different.fingerprint());
    }

    #[test]
    fn write_fingerprint_includes_the_pacing_override() {
        let actor = Actor {
            id: "human:test".into(),
            label: "test".into(),
            kind: ActorKind::Human,
        };
        let control_id = Uuid::new_v4();
        let unpaced = SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"reboot\r".to_vec(),
            operation_id: None,
            pacing: None,
        };
        let paced = SlotRequest::Write {
            actor,
            control_id,
            fence: 7,
            data: b"reboot\r".to_vec(),
            operation_id: None,
            pacing: Some(WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 5,
            }),
        };
        assert_ne!(unpaced.write_fingerprint(), paced.write_fingerprint());
    }

    #[test]
    fn write_idempotency_fingerprint_survives_server_actor_reissue() {
        let operation_id = Some(Uuid::new_v4());
        let original = SlotRequest::Write {
            actor: Actor {
                id: "agent:first-connection".into(),
                label: "worker".into(),
                kind: ActorKind::Agent,
            },
            control_id: Uuid::new_v4(),
            fence: 3,
            data: b"reboot\r".to_vec(),
            operation_id,
            pacing: None,
        };
        let reconnected = SlotRequest::Write {
            actor: Actor {
                id: "agent:reconnected".into(),
                label: "worker".into(),
                kind: ActorKind::Agent,
            },
            control_id: Uuid::new_v4(),
            fence: 9,
            data: b"reboot\r".to_vec(),
            operation_id,
            pacing: None,
        };

        assert_ne!(original.fingerprint(), reconnected.fingerprint());
        assert_eq!(
            original.write_fingerprint(),
            reconnected.write_fingerprint()
        );
    }

    #[test]
    fn run_and_checkpoint_fields_are_bounded_before_execution() {
        let actor = Actor {
            id: "human:test".into(),
            label: "test".into(),
            kind: ActorKind::Human,
        };
        let invalid_checkpoint = SlotRequest::Checkpoint {
            actor: actor.clone(),
            control_id: Uuid::new_v4(),
            fence: 1,
            label: " trailing ".into(),
        };
        assert_eq!(
            invalid_checkpoint.validate_business_fields(),
            Err(SlotError::InvalidLabel)
        );

        let too_many_keys = (0..=MAX_RUN_METADATA_KEYS)
            .map(|index| (format!("key-{index}"), json!(index)))
            .collect();
        let invalid_run = SlotRequest::StartRun {
            actor: actor.clone(),
            control_id: Uuid::new_v4(),
            fence: 1,
            label: "bounded run".into(),
            metadata: too_many_keys,
        };
        assert!(matches!(
            invalid_run.validate_business_fields(),
            Err(SlotError::RunMetadataTooManyKeys { .. })
        ));

        let invalid_run = SlotRequest::StartRun {
            actor,
            control_id: Uuid::new_v4(),
            fence: 1,
            label: "bounded run".into(),
            metadata: metadata([("payload", json!("x".repeat(MAX_RUN_METADATA_BYTES)))]),
        };
        assert!(matches!(
            invalid_run.validate_business_fields(),
            Err(SlotError::RunMetadataTooLarge { .. })
        ));
    }

    #[test]
    fn definite_pre_execution_write_errors_are_not_cached() {
        assert!(!is_cacheable_write_result(&Err(SlotError::EmptyWrite)));
        assert!(!is_cacheable_write_result(&Err(SlotError::PortOffline)));
        assert!(is_cacheable_write_result(&Err(SlotError::PartialWrite {
            written: 0,
            total: 4,
            generation: 1,
            event_seq: None,
            operation_id: None,
            message: "outcome unknown".into(),
        })));
    }

    #[test]
    fn executed_write_ids_are_never_forgotten_within_the_bounded_epoch_history() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut history = ExecutedWriteIds::new(1);

        assert_eq!(history.was_executed_or_reserveable(first), Ok(false));
        history.remember(first);
        assert_eq!(history.was_executed_or_reserveable(first), Ok(true));
        assert_eq!(
            history.was_executed_or_reserveable(second),
            Err(SlotError::WriteIdempotencyCapacity)
        );
    }

    #[test]
    fn full_rx_queue_becomes_an_explicit_overflow_count() {
        let (events, mut receiver) = mpsc::channel(1);
        assert!(events.try_send(PortEvent::Rx(vec![0])).is_ok());
        let mut pending = vec![1, 2, 3];
        let mut dropped = 0;
        enqueue_rx(&events, &mut pending, &mut dropped);
        assert_eq!(dropped, 3);
        assert!(pending.is_empty());

        assert!(matches!(receiver.try_recv(), Ok(PortEvent::Rx(_))));
        enqueue_rx(&events, &mut pending, &mut dropped);
        assert_eq!(dropped, 0);
        assert!(matches!(
            receiver.try_recv(),
            Ok(PortEvent::Overflow { dropped_bytes: 3 })
        ));
    }

    #[test]
    fn write_deadline_scales_with_the_estimated_pacing_duration() {
        // The full-speed path keeps the fixed two-second timeout.
        assert_eq!(
            write_deadline(4_096, 1, Duration::ZERO),
            Duration::from_secs(2)
        );
        // 4 KiB at 1 byte/1 ms paces for ~4.1 s, so the deadline doubles the
        // estimate and adds the fixed timeout instead of expiring mid-write.
        assert_eq!(
            write_deadline(4_096, 1, Duration::from_millis(1)),
            Duration::from_millis(4_095 * 2 + 2_000)
        );
        assert_eq!(
            write_deadline(35, 1, Duration::from_millis(1)),
            Duration::from_millis(34 * 2 + 2_000)
        );
        // A pacing estimate shorter than the fixed timeout never shrinks it.
        assert_eq!(
            write_deadline(1, 16, Duration::from_millis(1)),
            Duration::from_secs(2)
        );
        // Extreme pacing settings saturate instead of overflowing.
        let _ = write_deadline(usize::MAX, 1, Duration::from_millis(u64::MAX));
    }

    struct RecordingWriter {
        calls: Vec<(usize, tokio::time::Instant)>,
        max_accept: usize,
        never_accept: bool,
    }

    impl RecordingWriter {
        fn new(max_accept: usize) -> Self {
            Self {
                calls: Vec::new(),
                max_accept,
                never_accept: false,
            }
        }
    }

    impl tokio::io::AsyncWrite for RecordingWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.never_accept {
                return std::task::Poll::Pending;
            }
            let accepted = buffer.len().min(self.max_accept);
            self.calls.push((accepted, tokio::time::Instant::now()));
            std::task::Poll::Ready(Ok(accepted))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn paced_write_chunks_bytes_and_sleeps_between_chunks() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(usize::MAX);
        let pacing = WritePacing {
            chunk_size: 2,
            chunk_delay_ms: 5,
        };
        let outcome = write_with_pacing(&mut writer, b"abcde", pacing, &mut cancel).await;
        assert_eq!(outcome.written, 5);
        assert_eq!(outcome.error, None);
        assert!(!outcome.cancelled);

        let sizes = writer
            .calls
            .iter()
            .map(|(size, _)| *size)
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![2, 2, 1]);
        let first = writer.calls[0].1;
        // Two inter-chunk sleeps of 5 ms; no sleep after the final chunk.
        assert_eq!(writer.calls[1].1 - first, Duration::from_millis(5));
        assert_eq!(writer.calls[2].1 - first, Duration::from_millis(10));
    }

    #[tokio::test(start_paused = true)]
    async fn paced_write_accepts_partial_driver_writes_inside_one_chunk() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(1);
        let pacing = WritePacing {
            chunk_size: 3,
            chunk_delay_ms: 7,
        };
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, &mut cancel).await;
        assert_eq!(outcome.written, 4);
        assert_eq!(outcome.error, None);
        let sizes = writer
            .calls
            .iter()
            .map(|(size, _)| *size)
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![1, 1, 1, 1]);
        let first = writer.calls[0].1;
        // The first three one-byte calls form one chunk; only one 7 ms gap.
        assert_eq!(writer.calls[1].1 - first, Duration::ZERO);
        assert_eq!(writer.calls[2].1 - first, Duration::ZERO);
        assert_eq!(writer.calls[3].1 - first, Duration::from_millis(7));
    }

    #[tokio::test(start_paused = true)]
    async fn zero_delay_pacing_keeps_the_full_speed_path_without_sleeps() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(usize::MAX);
        let start = tokio::time::Instant::now();
        let pacing = WritePacing {
            chunk_size: 2,
            chunk_delay_ms: 0,
        };
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, &mut cancel).await;
        assert_eq!(outcome.written, 4);
        assert_eq!(outcome.error, None);
        assert_eq!(writer.calls.len(), 2);
        assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn paced_write_times_out_when_the_driver_stops_accepting() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(usize::MAX);
        writer.never_accept = true;
        let start = tokio::time::Instant::now();
        let pacing = WritePacing {
            chunk_size: 2,
            chunk_delay_ms: 5,
        };
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, &mut cancel).await;
        assert_eq!(outcome.written, 0);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("timed out"))
        );
        // 4 bytes in 2-byte chunks pace for 5 ms, so the deadline is
        // max(2 s, 2 * 5 ms + 2 s).
        assert_eq!(
            tokio::time::Instant::now() - start,
            Duration::from_millis(2_010)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn paced_write_stops_when_the_port_is_closing() {
        let (cancel_tx, mut cancel) = watch::channel(false);
        cancel_tx.send(true).unwrap();
        // A driver that never accepts leaves cancellation as the only ready
        // branch, keeping the outcome deterministic.
        let mut writer = RecordingWriter::new(usize::MAX);
        writer.never_accept = true;
        let pacing = WritePacing {
            chunk_size: 1,
            chunk_delay_ms: 1,
        };
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, &mut cancel).await;
        assert_eq!(outcome.written, 0);
        assert!(outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("cancelled"))
        );
    }
}
