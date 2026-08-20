use crate::control::{AcquireOutcome, ControlError, ControlLimits, ControlState, ReleaseOutcome};
use crate::journal::{JournalError, JournalHandle, PendingAppend};
use crate::ring::{EventRing, ReplayError, ReplayWindow};
use base64::Engine as _;
use chrono::Utc;
use serde_json::{Value, json};
use serial_protocol::{
    Actor, ActorKind, CommandResult, CommandSequenceAuditContext, ControlMode, Cursor, DataBits,
    DeviceProfile, Direction, ErrorCode, EventKind, FlowControl, LoggingState,
    MAX_BREAK_DURATION_MS, MAX_COMMAND_DESCRIPTION_BYTES, MAX_PHYSICAL_WRITE_TIMEOUT_MS,
    MAX_TRIGGER_ACTION_BYTES, MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES,
    MAX_TRIGGER_INTERVAL_MS, MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS,
    MAX_TRIGGER_TIMEOUT_MS, MAX_TRIGGER_TOTAL_BYTES, MIN_BREAK_DURATION_MS,
    MIN_TRIGGER_INTERVAL_MS, MIN_TRIGGER_TIMEOUT_MS, Parity, RunInfo, RunStatus,
    SequenceWritePrecondition, SerialSettings, SessionState, SlotConfig, SlotSnapshot, StopBits,
    TargetActivity, TimelineEvent, TransportProfile, TriggerInfo, TriggerSpec, TriggerStatus,
    WritePacing, apply_transport_profile, resolve_device_settings, resolve_transport_settings,
};
#[cfg(windows)]
use serialport::COMPort;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
#[cfg(windows)]
use std::io::Read as _;
use std::sync::Arc;
#[cfg(windows)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
#[cfg(not(windows))]
use std::{
    pin::Pin,
    task::{Context, Poll},
};
#[cfg(any(not(windows), test))]
use tokio::io::AsyncWriteExt;
#[cfg(not(windows))]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::sync::{Mutex, Semaphore, broadcast, mpsc, oneshot, watch};
use tokio_serial::{
    DataBits as TokioDataBits, FlowControl as TokioFlowControl, Parity as TokioParity, SerialPort,
    StopBits as TokioStopBits,
};
#[cfg(not(windows))]
use tokio_serial::{SerialPortBuilderExt, SerialStream};
use uuid::Uuid;

const COMMAND_QUEUE: usize = 256;
const PORT_EVENT_QUEUE: usize = 4_096;
const PORT_WRITE_QUEUE: usize = 128;
const PORT_READER_COMMAND_QUEUE: usize = 8;
const JOURNAL_ACK_QUEUE: usize = 1_024;
const BROADCAST_QUEUE: usize = 2_048;
const RING_EVENTS: usize = 20_000;
const RING_BYTES: usize = 4 * 1024 * 1024;
const RX_BUFFER_BYTES: usize = 4 * 1024;
const MAX_TRIGGER_BUFFERED_RX_BYTES: usize = 1024 * 1024;
const RX_COALESCE_WINDOW: Duration = Duration::from_millis(4);
const MAX_WRITE_BYTES: usize = 4 * 1024;
const MAX_LABEL_BYTES: usize = 256;
const MAX_RUN_METADATA_BYTES: usize = 16 * 1024;
const MAX_RUN_METADATA_KEYS: usize = 64;
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
// Windows commonly rounds a short pacing sleep plus the next asynchronous
// serial write to roughly one 15.6 ms scheduler tick. Budget a little more
// than that for every additional paced chunk instead of assuming the
// requested sleep is the only per-chunk cost.
const WRITE_CHUNK_OVERHEAD_ALLOWANCE: Duration = Duration::from_millis(20);
// Leave enough monotonic lease time for the accepted request to enter the
// port worker and report its bounded outcome without racing exact expiry.
const WRITE_LEASE_SAFETY_MARGIN: Duration = Duration::from_millis(100);
// Bound one physical write well below the first-party human/Agent lease
// durations. A request whose estimated pacing budget exceeds this limit is
// rejected before it enters the port worker, rather than starting a write that
// is already guaranteed to hit a clamped deadline partway through.
const MAX_WRITE_TIMEOUT: Duration = Duration::from_millis(MAX_PHYSICAL_WRITE_TIMEOUT_MS);
const IDEMPOTENCY_ENTRIES: usize = 2_048;
const WRITE_IDEMPOTENCY_HISTORY_ENTRIES: usize = 262_144;
const ACTIVE_WINDOW: Duration = Duration::from_secs(5);
const OPEN_BACKOFF_MIN: Duration = Duration::from_millis(500);
const OPEN_BACKOFF_MAX: Duration = Duration::from_secs(10);
const TERMINAL_TRIGGER_HISTORY: usize = 128;
#[cfg(windows)]
const WINDOWS_PORT_POLL_INTERVAL: Duration = Duration::from_millis(4);

#[derive(Clone)]
pub struct SlotHandle {
    slot_id: Arc<str>,
    commands: mpsc::Sender<SlotCommand>,
    snapshot: watch::Receiver<SlotSnapshot>,
    events: broadcast::Sender<TimelineEvent>,
    ring: Arc<Mutex<EventRing>>,
    subscriber_lag_events: Arc<std::sync::atomic::AtomicU64>,
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
    #[error(
        "serial write expected active Run {expected_run_id}, but no Run is active (no bytes were written)"
    )]
    WriteRunMissing { expected_run_id: Uuid },
    #[error(
        "serial write expected active Run {expected_run_id}, but the Slot's active Run is {active_run_id} (no bytes were written)"
    )]
    WriteRunMismatch {
        expected_run_id: Uuid,
        active_run_id: Uuid,
    },
    #[error(
        "serial write expected active Run {expected_run_id}, but that Run is owned by another actor (no bytes were written)"
    )]
    WriteRunNotOwner { expected_run_id: Uuid },
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
    #[error(
        "command description must be non-empty, trimmed, at most {MAX_COMMAND_DESCRIPTION_BYTES} UTF-8 bytes, and contain no control characters"
    )]
    InvalidCommandDescription,
    #[error(
        "command sequence audit requires a non-nil sequence id, a valid overall description, 1-8 steps, a zero-based step index within that count, and a per-step command description"
    )]
    InvalidCommandSequenceAudit,
    #[error("command sequence boundary changed before write: {reason} (no bytes were written)")]
    SequenceBoundaryChanged { reason: String },
    #[error(
        "serial write pacing requires an estimated {required_ms} ms, exceeding the {maximum_ms} ms request limit; increase chunk_size, reduce chunk_delay_ms, or split the write (no bytes were written)"
    )]
    WriteDeadlineExceeded { required_ms: u64, maximum_ms: u64 },
    #[error(
        "control lease has only {remaining_ms} ms remaining, but this serial write requires {write_ms} ms plus a {margin_ms} ms scheduling margin; renew control or shorten the write and retry (no bytes were written)"
    )]
    WriteLeaseTooShort {
        remaining_ms: u64,
        write_ms: u64,
        margin_ms: u64,
    },
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
    #[error("a Trigger Job is already active on this Slot (no bytes were written)")]
    TriggerActive,
    #[error("Trigger Job {trigger_id} was not found")]
    TriggerNotFound { trigger_id: Uuid },
    #[error("Trigger Job {trigger_id} belongs to another actor")]
    TriggerNotOwner { trigger_id: Uuid },
    #[error("Trigger daemon epoch does not match this daemon process")]
    TriggerEpochMismatch,
    #[error("Trigger generation does not match the current serial session")]
    TriggerGenerationMismatch,
    #[error("Trigger action must contain between 1 and {MAX_TRIGGER_ACTION_BYTES} bytes")]
    InvalidTriggerAction,
    #[error(
        "Trigger initial_write exceeds the {MAX_TRIGGER_INITIAL_WRITE_BYTES}-byte request limit"
    )]
    TriggerInitialWriteTooLarge,
    #[error(
        "Trigger interval_ms must be between {MIN_TRIGGER_INTERVAL_MS} and {MAX_TRIGGER_INTERVAL_MS}"
    )]
    InvalidTriggerInterval,
    #[error(
        "Trigger timeout_ms must be between {MIN_TRIGGER_TIMEOUT_MS} and {MAX_TRIGGER_TIMEOUT_MS}"
    )]
    InvalidTriggerTimeout,
    #[error("Trigger max_fires must be between 1 and {MAX_TRIGGER_FIRES}")]
    InvalidTriggerMaxFires,
    #[error(
        "Trigger byte patterns must be non-empty, at most {MAX_TRIGGER_PATTERN_BYTES} bytes each, and stop_contains may contain at most {MAX_TRIGGER_PATTERNS} patterns"
    )]
    InvalidTriggerPatterns,
    #[error("Trigger's bounded write plan exceeds {MAX_TRIGGER_TOTAL_BYTES} total bytes")]
    TriggerTotalBytesTooLarge,
    #[error(
        "BREAK duration_ms must be between {MIN_BREAK_DURATION_MS} and {MAX_BREAK_DURATION_MS}"
    )]
    InvalidBreakDuration,
    #[error("UART BREAK is not supported by this serial backend")]
    BreakUnsupported,
    #[error("UART BREAK failed and the physical port state is uncertain: {message}")]
    BreakFailed { message: String },
    #[error("device profile cannot change while a Run or Trigger Job is active")]
    ProfileChangeBusy,
}

impl From<ReplayError> for SlotError {
    fn from(_: ReplayError) -> Self {
        Self::CursorAhead
    }
}

impl SlotHandle {
    pub fn spawn(
        config: SlotConfig,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        control_limits: ControlLimits,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
    ) -> Self {
        Self::spawn_inner(
            config,
            transport_profile,
            device_profile,
            control_limits,
            daemon_epoch,
            daemon_started,
            journal,
            false,
        )
    }

    /// Creates a candidate Slot actor that cannot open its port until the
    /// surrounding configuration transaction has been persisted and commits.
    pub(crate) fn spawn_staged(
        config: SlotConfig,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        control_limits: ControlLimits,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
    ) -> Self {
        Self::spawn_inner(
            config,
            transport_profile,
            device_profile,
            control_limits,
            daemon_epoch,
            daemon_started,
            journal,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_inner(
        config: SlotConfig,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        control_limits: ControlLimits,
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
        staged: bool,
    ) -> Self {
        let initial = initial_snapshot(
            config.clone(),
            transport_profile.clone(),
            device_profile.clone(),
            daemon_epoch,
            staged,
        );
        let (commands, command_rx) = mpsc::channel(COMMAND_QUEUE);
        let (trigger_write_results, trigger_write_result_rx) = mpsc::channel(1);
        let (journal_ack_results, journal_ack_result_rx) = mpsc::channel(JOURNAL_ACK_QUEUE);
        let (events, _) = broadcast::channel(BROADCAST_QUEUE);
        let (snapshot_tx, snapshot) = watch::channel(initial);
        let ring = Arc::new(Mutex::new(EventRing::new(RING_EVENTS, RING_BYTES)));
        let subscriber_lag_events = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let handle = Self {
            slot_id: Arc::from(config.id.as_str()),
            commands,
            snapshot,
            events: events.clone(),
            ring: Arc::clone(&ring),
            subscriber_lag_events: Arc::clone(&subscriber_lag_events),
        };
        tokio::spawn(
            SlotActor {
                config,
                transport_profile,
                device_profile,
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
                rx_overflow_bytes: 0,
                endpoint_present: false,
                session_state: if staged {
                    SessionState::Disabled
                } else {
                    SessionState::WaitingForPort
                },
                state_reason: staged.then(|| "slot configuration pending persistence".into()),
                state_code: None,
                target_activity: TargetActivity::Unknown,
                last_rx_wall_time_ns: None,
                last_rx_instant: None,
                logging: LoggingState::Healthy,
                control: ControlState::new(daemon_epoch, 0, control_limits),
                active_run: None,
                port: None,
                port_events: None,
                active_trigger: None,
                terminal_triggers: HashMap::new(),
                terminal_trigger_order: VecDeque::new(),
                trigger_write_results,
                trigger_write_result_rx,
                journal_ack_results,
                journal_ack_result_rx,
                journal_ack_permits: Arc::new(Semaphore::new(JOURNAL_ACK_QUEUE)),
                trigger_arming: false,
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

    pub fn subscriber_count(&self) -> usize {
        self.events.receiver_count()
    }

    pub fn subscriber_lag_events(&self) -> u64 {
        self.subscriber_lag_events
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn record_subscriber_lag(&self, skipped: u64) {
        self.subscriber_lag_events
            .fetch_add(skipped, std::sync::atomic::Ordering::Relaxed);
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

    /// Cancels a queued acquire request. `control_id` is part of the wire
    /// contract for forward compatibility; the actor matches the queued
    /// waiter by actor identity because waiters hold no lease yet.
    pub async fn cancel_acquire(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::CancelAcquire {
            request_id,
            actor,
            control_id,
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
        expected_run_id: Option<Uuid>,
        pacing: Option<WritePacing>,
        description: Option<String>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        cooperative: bool,
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
            expected_run_id,
            pacing,
            description,
            command_sequence,
            sequence_precondition,
            cooperative,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_break(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        duration_ms: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::SendBreak {
            request_id,
            actor,
            control_id,
            fence,
            duration_ms,
            operation_id,
            expected_run_id,
            sequence_precondition,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn start_trigger(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        spec: TriggerSpec,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::StartTrigger {
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
            reply,
        })
        .await
    }

    pub async fn trigger_status(
        &self,
        request_id: Uuid,
        actor: Actor,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::TriggerStatus {
            request_id,
            actor,
            daemon_epoch,
            generation,
            trigger_id,
            reply,
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn cancel_trigger(
        &self,
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| SlotCommand::CancelTrigger {
            request_id,
            actor,
            control_id,
            fence,
            daemon_epoch,
            generation,
            trigger_id,
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
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        resume_on_rollback: bool,
    ) -> Result<(), SlotError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(SlotCommand::StageReconfiguration {
                config: Box::new(config),
                transport_profile,
                device_profile,
                resume_on_rollback,
                reply,
            })
            .await
            .map_err(|_| SlotError::Closed)?;
        result.await.map_err(|_| SlotError::ReplyDropped)?
    }

    /// Stages a device-profile refresh without changing the live snapshot,
    /// sequence, control/Run state, or physical port. The boolean is false for
    /// an exact no-op, in which case there is nothing to commit or roll back.
    pub(crate) async fn stage_device_profile(
        &self,
        device_profile: Option<DeviceProfile>,
    ) -> Result<bool, SlotError> {
        let (reply, done) = oneshot::channel();
        self.commands
            .send(SlotCommand::StageDeviceProfile {
                device_profile,
                reply,
            })
            .await
            .map_err(|_| SlotError::Closed)?;
        done.await.map_err(|_| SlotError::ReplyDropped)?
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
    CancelAcquire {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        reply: Reply,
    },
    Write {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        pacing: Option<WritePacing>,
        description: Option<String>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        cooperative: bool,
        reply: Reply,
    },
    SendBreak {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        duration_ms: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        reply: Reply,
    },
    StartTrigger {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        spec: TriggerSpec,
        reply: Reply,
    },
    TriggerStatus {
        request_id: Uuid,
        actor: Actor,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        reply: Reply,
    },
    CancelTrigger {
        request_id: Uuid,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
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
        config: Box<SlotConfig>,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        resume_on_rollback: bool,
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    StageDeviceProfile {
        device_profile: Option<DeviceProfile>,
        reply: oneshot::Sender<Result<bool, SlotError>>,
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
        deadline: tokio::time::Instant,
        reply: oneshot::Sender<PortWriteOutcome>,
    },
    Break {
        duration: Duration,
        reply: oneshot::Sender<Result<(), PortBreakFailure>>,
    },
}

struct PortWriteOutcome {
    written: usize,
    error: Option<String>,
    cancelled: bool,
}

struct PortBreakOutcome {
    error: Option<PortBreakFailure>,
    cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PortBreakFailure {
    Unsupported(String),
    Failed(String),
}

impl std::fmt::Display for PortBreakFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(message) | Self::Failed(message) => formatter.write_str(message),
        }
    }
}

fn classify_break_failure(phase: &str, error: impl std::fmt::Display) -> PortBreakFailure {
    let message = format!("{phase}: {error}");
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("not supported")
        || normalized.contains("unsupported")
        || normalized.contains("not implemented")
    {
        PortBreakFailure::Unsupported(message)
    } else {
        PortBreakFailure::Failed(message)
    }
}

fn break_failure_closes_port(error: Option<&PortBreakFailure>) -> bool {
    matches!(error, Some(PortBreakFailure::Failed(_)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriggerWriteKind {
    Initial,
    Action { fire_index: u32 },
}

struct TriggerWriteResult {
    trigger_id: Uuid,
    kind: TriggerWriteKind,
    data: Vec<u8>,
    outcome: Result<PortWriteOutcome, String>,
}

enum JournalAckOutcome {
    Durable,
    Failed(String),
    TimedOut,
}

struct JournalAckResult {
    seq: u64,
    outcome: JournalAckOutcome,
}

enum PortEvent {
    Rx(Vec<u8>),
    Overflow {
        dropped_bytes: u64,
    },
    Closed {
        reason: String,
        dropped_bytes: u64,
    },
    /// Ordered marker emitted by the sole reader after all earlier RX has
    /// been flushed ahead of a Trigger's live observation boundary.
    ReaderBarrier {
        id: Uuid,
    },
}

#[derive(Debug)]
struct LiteralMatcher {
    patterns: Vec<Vec<u8>>,
    tail: Vec<u8>,
    maximum_pattern_len: usize,
}

impl LiteralMatcher {
    fn new(patterns: Vec<Vec<u8>>) -> Self {
        let maximum_pattern_len = patterns.iter().map(Vec::len).max().unwrap_or(0);
        Self {
            patterns,
            tail: Vec::new(),
            maximum_pattern_len,
        }
    }

    fn push(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        if self.patterns.is_empty() {
            return None;
        }
        let mut window = Vec::with_capacity(self.tail.len().saturating_add(data.len()));
        window.extend_from_slice(&self.tail);
        window.extend_from_slice(data);
        let matched = self
            .patterns
            .iter()
            .find(|pattern| contains_bytes(&window, pattern))
            .cloned();
        let tail_len = self.maximum_pattern_len.saturating_sub(1).min(window.len());
        self.tail.clear();
        self.tail
            .extend_from_slice(&window[window.len().saturating_sub(tail_len)..]);
        matched
    }
}

#[derive(Default)]
struct TriggerRxAuditBuffer {
    events: VecDeque<PortEvent>,
    bytes: usize,
    dropped_bytes: u64,
}

impl TriggerRxAuditBuffer {
    fn push_rx(&mut self, data: Vec<u8>) -> bool {
        self.push_rx_with_limit(data, MAX_TRIGGER_BUFFERED_RX_BYTES)
    }

    fn push_rx_with_limit(&mut self, data: Vec<u8>, limit: usize) -> bool {
        if self.dropped_bytes == 0 && self.bytes.saturating_add(data.len()) <= limit {
            self.bytes = self.bytes.saturating_add(data.len());
            self.events.push_back(PortEvent::Rx(data));
            false
        } else {
            self.dropped_bytes = self.dropped_bytes.saturating_add(data.len() as u64);
            true
        }
    }

    fn add_gap(&mut self, dropped_bytes: u64) {
        self.dropped_bytes = self.dropped_bytes.saturating_add(dropped_bytes);
    }

    fn take(&mut self) -> (VecDeque<PortEvent>, u64) {
        self.bytes = 0;
        (
            std::mem::take(&mut self.events),
            std::mem::take(&mut self.dropped_bytes),
        )
    }
}

struct ActiveTrigger {
    info: TriggerInfo,
    bound_run_id: Option<Uuid>,
    deadline: Instant,
    next_write_at: Option<Instant>,
    initial_pending: bool,
    start_seen: bool,
    start_matcher: Option<LiteralMatcher>,
    stop_matcher: LiteralMatcher,
    write_in_flight: Option<TriggerWriteKind>,
    buffered_rx: TriggerRxAuditBuffer,
    pending_terminal: Option<(TriggerStatus, Option<Vec<u8>>)>,
}

impl ActiveTrigger {
    fn next_deadline(&self) -> Option<Instant> {
        if self.pending_terminal.is_some() {
            return None;
        }
        Some(
            self.next_write_at
                .filter(|_| self.write_in_flight.is_none())
                .map_or(self.deadline, |write| write.min(self.deadline)),
        )
    }

    fn status_snapshot(&self) -> TriggerInfo {
        self.info.clone()
    }

    fn observe_rx(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        if let Some(matched) = self.stop_matcher.push(data) {
            return Some(matched);
        }
        if !self.start_seen
            && self
                .start_matcher
                .as_mut()
                .is_some_and(|matcher| matcher.push(data).is_some())
        {
            self.start_seen = true;
            self.start_matcher = None;
            if !self.initial_pending && self.pending_terminal.is_none() {
                self.info.status = TriggerStatus::Running;
                self.next_write_at = Some(Instant::now());
            }
        }
        None
    }

    fn deadline_status(&self) -> TriggerStatus {
        if self.info.fires_confirmed >= self.info.spec.max_fires {
            TriggerStatus::MaxFiresReached
        } else {
            TriggerStatus::TimedOut
        }
    }

    /// Completes the kickoff phase without inventing an RX start gate.
    ///
    /// An omitted `start_contains` sets `start_seen` when the Trigger is
    /// created, so a confirmed kickoff makes the first action immediately
    /// eligible. An explicit start matcher keeps the Trigger waiting until its
    /// literal is observed. Keeping this transition here makes the two wire-
    /// compatible modes explicit and independently testable.
    fn confirm_initial_write(&mut self, now: Instant) {
        self.initial_pending = false;
        if self.pending_terminal.is_some() {
            return;
        }
        if self.start_seen {
            self.info.status = TriggerStatus::Running;
            self.next_write_at = Some(now);
        } else {
            self.info.status = TriggerStatus::WaitingForStart;
            self.next_write_at = None;
        }
    }

    /// Records one fully-confirmed action write. Reaching the send budget is
    /// not necessarily terminal: when a stop matcher exists, the Trigger keeps
    /// observing RX until its original deadline so output caused by the final
    /// write can still prove completion.
    fn confirm_action_write(&mut self, now: Instant) -> Option<TriggerStatus> {
        self.info.fires_confirmed = self.info.fires_confirmed.saturating_add(1);
        if self.info.fires_confirmed < self.info.spec.max_fires {
            if self.pending_terminal.is_none() {
                self.next_write_at = Some(now + Duration::from_millis(self.info.spec.interval_ms));
            }
            return None;
        }

        self.next_write_at = None;
        self.info
            .spec
            .stop_contains
            .is_empty()
            .then_some(TriggerStatus::MaxFiresReached)
    }
}

#[derive(Default)]
struct ReaderTail {
    pending: Vec<u8>,
    dropped_bytes: u64,
}

struct PortWorker {
    commands: mpsc::Sender<PortCommand>,
    reader_commands: mpsc::Sender<PortReaderCommand>,
    cancel: watch::Sender<bool>,
    reader: tokio::task::JoinHandle<ReaderTail>,
    writer: tokio::task::JoinHandle<()>,
}

enum PortReaderCommand {
    Barrier { id: Uuid },
}

enum PendingReconfiguration {
    /// A newly-created actor. Its candidate config is already stored in
    /// `config`, but it starts paused and is not present in the active map.
    Add,
    /// A replacement config held entirely inside the actor until commit.
    Replace {
        config: Box<SlotConfig>,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        resume_on_rollback: bool,
        reopened: bool,
    },
    /// An active Slot that will move to the retired map on commit.
    Remove,
    /// A device-profile catalog refresh. Staging it is deliberately inert:
    /// only commit changes the resolved behavior and publishes an event.
    DeviceProfile {
        device_profile: Option<DeviceProfile>,
    },
}

struct SlotActor {
    config: SlotConfig,
    transport_profile: Option<TransportProfile>,
    device_profile: Option<DeviceProfile>,
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
    rx_overflow_bytes: u64,
    endpoint_present: bool,
    session_state: SessionState,
    state_reason: Option<String>,
    state_code: Option<ErrorCode>,
    target_activity: TargetActivity,
    last_rx_wall_time_ns: Option<i64>,
    last_rx_instant: Option<Instant>,
    logging: LoggingState,
    control: ControlState,
    active_run: Option<RunInfo>,
    port: Option<PortWorker>,
    port_events: Option<mpsc::Receiver<PortEvent>>,
    active_trigger: Option<ActiveTrigger>,
    terminal_triggers: HashMap<Uuid, TriggerInfo>,
    terminal_trigger_order: VecDeque<Uuid>,
    trigger_write_results: mpsc::Sender<TriggerWriteResult>,
    trigger_write_result_rx: mpsc::Receiver<TriggerWriteResult>,
    journal_ack_results: mpsc::Sender<JournalAckResult>,
    journal_ack_result_rx: mpsc::Receiver<JournalAckResult>,
    journal_ack_permits: Arc<Semaphore>,
    trigger_arming: bool,
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
        if !self.config.enabled || !self.effective_serial_settings().auto_open {
            self.session_state = SessionState::Disabled;
            self.publish_snapshot().await;
        }
        let mut maintenance = tokio::time::interval(Duration::from_millis(250));
        maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let trigger_deadline = self
                .active_trigger
                .as_ref()
                .and_then(ActiveTrigger::next_deadline);
            let port_event = async {
                match self.port_events.as_mut() {
                    Some(events) => events.recv().await,
                    None => std::future::pending().await,
                }
            };
            let trigger_timer = async move {
                match trigger_deadline {
                    Some(deadline) => {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                    }
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
                result = self.trigger_write_result_rx.recv() => {
                    if let Some(result) = result {
                        self.handle_trigger_write_result(result).await;
                    }
                }
                result = self.journal_ack_result_rx.recv() => {
                    if let Some(result) = result {
                        self.handle_journal_ack_result(result).await;
                    }
                }
                _ = trigger_timer => {
                    if self.handle_trigger_timer().await {
                        break;
                    }
                }
                _ = maintenance.tick() => self.maintain().await,
            }
        }

        self.request_trigger_stop(TriggerStatus::PortClosed, None)
            .await;
        self.stop_port().await;
        self.settle_trigger_write_after_port_stop().await;
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
            && self.effective_serial_settings().auto_open
            && Instant::now() >= self.retry_at
        {
            self.try_open().await;
        }
    }

    async fn try_open(&mut self) {
        self.endpoint_present = endpoint_present(&self.config.port);
        self.session_state = SessionState::Opening;
        self.state_reason = None;
        self.state_code = None;
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

        let effective_settings = self.effective_serial_settings();
        match open_port(&self.config.port, &effective_settings) {
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
                self.state_code = None;
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
                        ("baud_rate", json!(effective_settings.baud_rate)),
                        ("backend", json!(serial_backend_name())),
                    ]),
                )
                .await;
            }
            Err(error) => {
                self.session_state = SessionState::Backoff;
                self.state_reason = Some(error.to_string());
                self.state_code = Some(error.code);
                self.schedule_retry();
                self.publish_snapshot().await;
                self.emit(
                    EventKind::SerialOpenFailed,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    metadata([
                        ("error", json!(error.to_string())),
                        ("error_code", json!(error.code)),
                    ]),
                )
                .await;
            }
        }
    }

    async fn handle_port_event(&mut self, event: PortEvent) {
        match event {
            PortEvent::Rx(data) => {
                self.handle_rx_data(data).await;
            }
            PortEvent::Overflow { dropped_bytes } => {
                self.handle_rx_overflow(dropped_bytes).await;
            }
            PortEvent::Closed {
                reason,
                dropped_bytes,
            } => {
                if dropped_bytes > 0 {
                    self.handle_rx_overflow(dropped_bytes).await;
                }
                self.handle_port_closed(reason).await;
            }
            PortEvent::ReaderBarrier { .. } => {}
        }
    }

    async fn handle_rx_data(&mut self, data: Vec<u8>) {
        self.last_rx_wall_time_ns = Some(wall_time_ns());
        self.last_rx_instant = Some(Instant::now());
        self.target_activity = TargetActivity::Active;
        let write_was_in_flight = self
            .active_trigger
            .as_ref()
            .is_some_and(|trigger| trigger.write_in_flight.is_some());
        let matched = self.observe_trigger_rx(&data);
        if let Some(pattern) = matched {
            self.mark_trigger_stopping(TriggerStatus::Matched, Some(pattern));
        }
        if write_was_in_flight {
            let buffer_overflowed = self
                .active_trigger
                .as_mut()
                .is_some_and(|trigger| trigger.buffered_rx.push_rx(data));
            if buffer_overflowed {
                self.mark_trigger_stopping(TriggerStatus::RxGap, None);
            }
            return;
        }
        self.emit(
            EventKind::Rx,
            Direction::Rx,
            data,
            Some(device_actor()),
            None,
            BTreeMap::new(),
        )
        .await;
        self.finish_stopped_trigger_if_idle().await;
    }

    async fn handle_rx_overflow(&mut self, dropped_bytes: u64) {
        if dropped_bytes == 0 {
            return;
        }
        self.rx_overflow_bytes = self.rx_overflow_bytes.saturating_add(dropped_bytes);
        let write_in_flight = self
            .active_trigger
            .as_ref()
            .is_some_and(|trigger| trigger.write_in_flight.is_some());
        self.mark_trigger_stopping(TriggerStatus::RxGap, None);
        if write_in_flight {
            if let Some(trigger) = self.active_trigger.as_mut() {
                trigger.buffered_rx.add_gap(dropped_bytes);
            }
            return;
        }
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
        self.finish_stopped_trigger_if_idle().await;
    }

    async fn handle_port_closed(&mut self, reason: String) {
        self.mark_trigger_stopping(TriggerStatus::PortClosed, None);
        self.stop_port().await;
        self.settle_trigger_write_after_port_stop().await;
        self.finish_stopped_trigger_if_idle().await;
        self.session_state = SessionState::Backoff;
        self.state_reason = Some(reason.clone());
        self.state_code = Some(ErrorCode::PortIo);
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
                if self
                    .active_trigger
                    .as_ref()
                    .is_some_and(|trigger| trigger.info.owner.id == actor_id)
                {
                    self.request_trigger_stop(TriggerStatus::ControlLost, None)
                        .await;
                }
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
                transport_profile,
                device_profile,
                resume_on_rollback,
                reply,
            } => {
                let result = self
                    .stage_reconfiguration(
                        *config,
                        transport_profile,
                        device_profile,
                        resume_on_rollback,
                    )
                    .await;
                let _ = reply.send(result);
                return false;
            }
            CommandDisposition::StageDeviceProfile {
                device_profile,
                reply,
            } => {
                let result = self.stage_device_profile(device_profile);
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
            if let Err(error) =
                request.validate_write_authorization(&self.control, self.active_run.as_ref())
            {
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
                        if self
                            .active_trigger
                            .as_ref()
                            .is_some_and(|trigger| trigger.info.owner.id == revoked.owner.id)
                        {
                            self.request_trigger_stop(TriggerStatus::ControlLost, None)
                                .await;
                        }
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
                self.request_trigger_stop(TriggerStatus::ControlLost, None)
                    .await;
                self.abort_run("control released", Some(actor)).await;
                self.emit_release(released, EventKind::ControlReleased)
                    .await;
                Ok(CommandResult::ControlReleased)
            }
            // The wire `control_id` is ignored: a queued waiter holds no lease
            // yet, so the request is matched by actor identity.
            SlotRequest::CancelAcquire { actor, .. } => Ok(CommandResult::AcquireCancelled {
                removed: self.control.cancel(&actor.id),
            }),
            SlotRequest::Write {
                actor,
                control_id,
                fence,
                data,
                operation_id,
                expected_run_id,
                pacing,
                description,
                command_sequence,
                sequence_precondition,
                cooperative,
                ..
            } => {
                if self.active_trigger.is_some() {
                    return Err(SlotError::TriggerActive);
                }
                if cooperative {
                    validate_cooperative_write(
                        &actor,
                        expected_run_id,
                        &self.control,
                        self.active_run.as_ref(),
                    )?;
                } else {
                    validate_expected_write_run(expected_run_id, &actor, self.active_run.as_ref())?;
                }
                if data.len() > MAX_WRITE_BYTES {
                    return Err(SlotError::WriteTooLarge);
                }
                if data.is_empty() {
                    return Err(SlotError::EmptyWrite);
                }
                let total = data.len();
                let effective_settings = self.effective_serial_settings();
                let pacing = WritePacing::resolve(pacing, &effective_settings);
                let write_timeout = write_deadline(
                    total,
                    pacing.chunk_size as usize,
                    Duration::from_millis(pacing.chunk_delay_ms),
                )
                .map_err(|required_ms| SlotError::WriteDeadlineExceeded {
                    required_ms,
                    maximum_ms: duration_millis_saturating(MAX_WRITE_TIMEOUT),
                })?;
                let authorization_now = Instant::now();
                let lease_remaining = if cooperative {
                    self.control.current_remaining_ttl(authorization_now)?
                } else {
                    self.control
                        .remaining_ttl(&actor.id, control_id, fence, authorization_now)?
                };
                ensure_lease_covers_write(lease_remaining, write_timeout)?;
                if let Some(precondition) = sequence_precondition.as_ref() {
                    let ring = self.ring.lock().await;
                    validate_sequence_write_precondition(
                        precondition,
                        self.daemon_epoch,
                        self.generation,
                        self.tx_offset,
                        self.seq,
                        &ring,
                    )?;
                }
                let Some(port) = &self.port else {
                    return Err(SlotError::PortOffline);
                };
                let (reply, result) = oneshot::channel();
                port.commands
                    .send(PortCommand::Write {
                        data: data.clone(),
                        pacing,
                        deadline: tokio::time::Instant::from_std(authorization_now + write_timeout),
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
                    let mut event_metadata = write_event_metadata(
                        outcome.written != total,
                        cooperative,
                        description,
                        command_sequence,
                    );
                    if cooperative && let Some(run) = self.active_run.as_ref() {
                        event_metadata.insert("interfered_run_id".into(), json!(run.id));
                        event_metadata.insert(
                            "interfered_run_owner".into(),
                            serde_json::to_value(&run.owner).unwrap_or(Value::Null),
                        );
                    }
                    Some(
                        self.emit(
                            EventKind::Tx,
                            Direction::Tx,
                            data[..outcome.written].to_vec(),
                            Some(actor),
                            operation_id,
                            event_metadata,
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
            SlotRequest::SendBreak {
                actor,
                control_id,
                fence,
                duration_ms,
                operation_id,
                expected_run_id,
                sequence_precondition,
            } => {
                if self.active_trigger.is_some() {
                    return Err(SlotError::TriggerActive);
                }
                validate_expected_write_run(expected_run_id, &actor, self.active_run.as_ref())?;
                if !(MIN_BREAK_DURATION_MS..=MAX_BREAK_DURATION_MS).contains(&duration_ms) {
                    return Err(SlotError::InvalidBreakDuration);
                }
                let authorization_now = Instant::now();
                let signal_duration = Duration::from_millis(duration_ms);
                let lease_remaining =
                    self.control
                        .remaining_ttl(&actor.id, control_id, fence, authorization_now)?;
                ensure_lease_covers_write(lease_remaining, signal_duration)?;
                if let Some(precondition) = sequence_precondition.as_ref() {
                    let ring = self.ring.lock().await;
                    validate_sequence_write_precondition(
                        precondition,
                        self.daemon_epoch,
                        self.generation,
                        self.tx_offset,
                        self.seq,
                        &ring,
                    )?;
                }
                let Some(port) = &self.port else {
                    return Err(SlotError::PortOffline);
                };
                let (reply, result) = oneshot::channel();
                port.commands
                    .send(PortCommand::Break {
                        duration: signal_duration,
                        reply,
                    })
                    .await
                    .map_err(|_| SlotError::PortOffline)?;
                result
                    .await
                    .map_err(|_| SlotError::BreakFailed {
                        message: "serial writer stopped before confirming that BREAK was cleared"
                            .into(),
                    })?
                    .map_err(|error| match error {
                        PortBreakFailure::Unsupported(_) => SlotError::BreakUnsupported,
                        PortBreakFailure::Failed(message) => SlotError::BreakFailed { message },
                    })?;
                let event_seq = self
                    .emit(
                        EventKind::Break,
                        Direction::None,
                        Vec::new(),
                        Some(actor),
                        operation_id,
                        metadata([("duration_ms", json!(duration_ms))]),
                    )
                    .await;
                Ok(CommandResult::BreakSent { event_seq })
            }
            SlotRequest::StartTrigger {
                actor,
                control_id,
                fence,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                sequence_precondition,
                spec,
            } => {
                if self.active_trigger.is_some() {
                    return Err(SlotError::TriggerActive);
                }
                if daemon_epoch != self.daemon_epoch {
                    return Err(SlotError::TriggerEpochMismatch);
                }
                if generation != self.generation {
                    return Err(SlotError::TriggerGenerationMismatch);
                }
                if self.port.is_none() {
                    return Err(SlotError::PortOffline);
                }
                let effective_settings = self.effective_serial_settings();
                validate_trigger_pacing(&spec, &effective_settings)?;
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                validate_expected_write_run(expected_run_id, &actor, self.active_run.as_ref())?;
                self.trigger_arming = true;
                let flush_result = self.flush_pretrigger_rx().await;
                self.trigger_arming = false;
                flush_result?;
                // The ordered reader barrier can surface a port close, and a
                // large pre-existing backlog can consume lease time. Recheck
                // every physical-write boundary before arming the matcher.
                if self.port.is_none() {
                    return Err(SlotError::PortOffline);
                }
                if generation != self.generation {
                    return Err(SlotError::TriggerGenerationMismatch);
                }
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                validate_expected_write_run(expected_run_id, &actor, self.active_run.as_ref())?;
                if let Some(precondition) = sequence_precondition.as_ref() {
                    let ring = self.ring.lock().await;
                    validate_sequence_write_precondition(
                        precondition,
                        self.daemon_epoch,
                        self.generation,
                        self.tx_offset,
                        self.seq,
                        &ring,
                    )?;
                }

                let bound_run_id =
                    expected_run_id.or_else(|| self.active_run.as_ref().map(|run| run.id));
                let initial_pending = spec.initial_write.is_some();
                let start_seen = spec.start_contains.is_none();
                let status = if initial_pending {
                    TriggerStatus::Armed
                } else if start_seen {
                    TriggerStatus::Running
                } else {
                    TriggerStatus::WaitingForStart
                };
                let trigger_id = Uuid::new_v4();
                let start_seq = self.seq.saturating_add(1);
                let now = Instant::now();
                let info = TriggerInfo {
                    id: trigger_id,
                    owner: actor.clone(),
                    daemon_epoch,
                    generation,
                    control_id,
                    fence,
                    operation_id,
                    expected_run_id,
                    spec: spec.clone(),
                    status,
                    start_seq,
                    end_seq: None,
                    last_write_seq: None,
                    fires_confirmed: 0,
                    tx_bytes_confirmed: 0,
                    matched_pattern: None,
                };
                self.active_trigger = Some(ActiveTrigger {
                    info: info.clone(),
                    bound_run_id,
                    deadline: now + Duration::from_millis(spec.timeout_ms),
                    next_write_at: (initial_pending || start_seen).then_some(now),
                    initial_pending,
                    start_seen,
                    start_matcher: spec
                        .start_contains
                        .clone()
                        .map(|pattern| LiteralMatcher::new(vec![pattern])),
                    stop_matcher: LiteralMatcher::new(spec.stop_contains.clone()),
                    write_in_flight: None,
                    buffered_rx: TriggerRxAuditBuffer::default(),
                    pending_terminal: None,
                });
                self.emit_inner(
                    EventKind::TriggerStarted,
                    Direction::None,
                    Vec::new(),
                    Some(actor),
                    operation_id,
                    bound_run_id,
                    trigger_event_metadata(&info),
                )
                .await;
                Ok(CommandResult::TriggerStarted {
                    trigger: Box::new(
                        self.active_trigger
                            .as_ref()
                            .expect("Trigger remains active after its start event")
                            .status_snapshot(),
                    ),
                })
            }
            SlotRequest::TriggerStatus {
                actor: _,
                daemon_epoch,
                generation,
                trigger_id,
            } => {
                let trigger = self.lookup_trigger(trigger_id)?;
                validate_trigger_identity(trigger, daemon_epoch, generation)?;
                Ok(CommandResult::TriggerStatus {
                    trigger: Box::new(trigger.clone()),
                })
            }
            SlotRequest::CancelTrigger {
                actor,
                control_id,
                fence,
                daemon_epoch,
                generation,
                trigger_id,
            } => {
                let trigger = self.lookup_trigger(trigger_id)?;
                validate_trigger_identity(trigger, daemon_epoch, generation)?;
                if trigger.status.is_terminal() {
                    return Ok(CommandResult::TriggerCancelled {
                        trigger: Box::new(trigger.clone()),
                    });
                }
                if trigger.owner.id != actor.id {
                    return Err(SlotError::TriggerNotOwner { trigger_id });
                }
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                if trigger.control_id != control_id || trigger.fence != fence {
                    return Err(SlotError::Control(ControlError::StaleFence));
                }
                self.request_trigger_stop(TriggerStatus::Cancelled, None)
                    .await;
                let trigger = self.lookup_trigger(trigger_id)?.clone();
                Ok(CommandResult::TriggerCancelled {
                    trigger: Box::new(trigger),
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
                if self.active_trigger.is_some() {
                    return Err(SlotError::TriggerActive);
                }
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
                self.request_trigger_stop(TriggerStatus::RunLost, None)
                    .await;
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
        self.request_trigger_stop(TriggerStatus::ControlLost, None)
            .await;
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
        if let Some(run_id) = self.active_run.as_ref().map(|run| run.id)
            && self
                .active_trigger
                .as_ref()
                .is_some_and(|trigger| trigger.bound_run_id == Some(run_id))
        {
            self.request_trigger_stop(TriggerStatus::RunLost, None)
                .await;
        }
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

    async fn flush_pretrigger_rx(&mut self) -> Result<(), SlotError> {
        let barrier_id = Uuid::new_v4();
        let reader_commands = self
            .port
            .as_ref()
            .map(|port| port.reader_commands.clone())
            .ok_or(SlotError::PortOffline)?;
        reader_commands
            .send(PortReaderCommand::Barrier { id: barrier_id })
            .await
            .map_err(|_| SlotError::PortOffline)?;

        loop {
            let event = match self.port_events.as_mut() {
                Some(events) => events.recv().await,
                None => return Err(SlotError::PortOffline),
            }
            .ok_or(SlotError::PortOffline)?;
            match event {
                PortEvent::ReaderBarrier { id } if id == barrier_id => return Ok(()),
                PortEvent::ReaderBarrier { .. } => {
                    // A Slot actor issues barriers serially. An unrelated
                    // marker is stale and has no timeline meaning.
                }
                event => {
                    self.handle_port_event(event).await;
                    if self.port.is_none() {
                        return Err(SlotError::PortOffline);
                    }
                }
            }
        }
    }

    fn lookup_trigger(&self, trigger_id: Uuid) -> Result<&TriggerInfo, SlotError> {
        if let Some(trigger) = self
            .active_trigger
            .as_ref()
            .filter(|trigger| trigger.info.id == trigger_id)
        {
            return Ok(&trigger.info);
        }
        self.terminal_triggers
            .get(&trigger_id)
            .ok_or(SlotError::TriggerNotFound { trigger_id })
    }

    fn observe_trigger_rx(&mut self, data: &[u8]) -> Option<Vec<u8>> {
        self.active_trigger.as_mut()?.observe_rx(data)
    }

    fn trigger_write_is_due(&self) -> bool {
        let now = Instant::now();
        self.active_trigger.as_ref().is_some_and(|trigger| {
            trigger_write_due_at(
                now,
                trigger.deadline,
                trigger.next_write_at,
                trigger.pending_terminal.is_some(),
                trigger.write_in_flight.is_some(),
            )
        })
    }

    async fn handle_trigger_timer(&mut self) -> bool {
        // A stop byte and a timer can become ready in the same scheduler turn.
        // Drain already-queued RX and control requests first so a due timer
        // cannot schedule one avoidable extra write ahead of stop/cancel.
        let mut port_events_empty = false;
        for _ in 0..PORT_EVENT_QUEUE {
            let event = match self.port_events.as_mut().map(mpsc::Receiver::try_recv) {
                Some(Ok(event)) => event,
                Some(Err(mpsc::error::TryRecvError::Empty)) | None => {
                    port_events_empty = true;
                    break;
                }
                Some(Err(mpsc::error::TryRecvError::Disconnected)) => {
                    self.handle_port_closed("serial worker stopped".into())
                        .await;
                    return false;
                }
            };
            self.handle_port_event(event).await;
            if self.active_trigger.is_none() {
                return false;
            }
        }
        if !port_events_empty {
            // Bound one actor turn. The next ready timer turn drains again,
            // and no write is scheduled while unread RX might contain a stop.
            return false;
        }

        let mut commands_empty = false;
        for _ in 0..COMMAND_QUEUE {
            let command = match self.commands.try_recv() {
                Ok(command) => command,
                Err(mpsc::error::TryRecvError::Empty) => {
                    commands_empty = true;
                    break;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.request_trigger_stop(TriggerStatus::PortClosed, None)
                        .await;
                    return true;
                }
            };
            if self.handle_command(command).await {
                return true;
            }
            if self.active_trigger.is_none() {
                return false;
            }
        }
        if !commands_empty {
            return false;
        }

        self.expire_control().await;
        let Some(trigger) = self.active_trigger.as_ref() else {
            return false;
        };
        if trigger.pending_terminal.is_some() {
            self.finish_stopped_trigger_if_idle().await;
            return false;
        }
        if Instant::now() >= trigger.deadline {
            let status = trigger.deadline_status();
            self.request_trigger_stop(status, None).await;
            return false;
        }
        if self.trigger_write_is_due() {
            self.begin_trigger_write().await;
        }
        false
    }

    async fn begin_trigger_write(&mut self) {
        if let Some(status) = self.active_trigger.as_ref().and_then(|trigger| {
            (Instant::now() >= trigger.deadline).then(|| trigger.deadline_status())
        }) {
            self.request_trigger_stop(status, None).await;
            return;
        }
        let Some(trigger) = self.active_trigger.as_ref() else {
            return;
        };
        if trigger.pending_terminal.is_some()
            || trigger.write_in_flight.is_some()
            || trigger
                .next_write_at
                .is_none_or(|deadline| Instant::now() < deadline)
        {
            return;
        }

        let trigger_id = trigger.info.id;
        let trigger_deadline = trigger.deadline;
        if trigger.info.daemon_epoch != self.daemon_epoch
            || trigger.info.generation != self.generation
        {
            self.request_trigger_stop(TriggerStatus::GenerationChanged, None)
                .await;
            return;
        }
        if self.port.is_none() {
            self.request_trigger_stop(TriggerStatus::PortClosed, None)
                .await;
            return;
        }
        if self
            .control
            .validate(
                &trigger.info.owner.id,
                trigger.info.control_id,
                trigger.info.fence,
                Instant::now(),
            )
            .is_err()
        {
            self.request_trigger_stop(TriggerStatus::ControlLost, None)
                .await;
            return;
        }
        if validate_expected_write_run(
            trigger.info.expected_run_id,
            &trigger.info.owner,
            self.active_run.as_ref(),
        )
        .is_err()
        {
            self.request_trigger_stop(TriggerStatus::RunLost, None)
                .await;
            return;
        }

        let (kind, data) = if trigger.initial_pending {
            let Some(data) = trigger.info.spec.initial_write.clone() else {
                self.request_trigger_stop(TriggerStatus::WriteFailed, None)
                    .await;
                return;
            };
            (TriggerWriteKind::Initial, data)
        } else {
            if !trigger.start_seen {
                return;
            }
            if trigger.info.fires_confirmed >= trigger.info.spec.max_fires {
                if trigger.info.spec.stop_contains.is_empty() {
                    self.request_trigger_stop(TriggerStatus::MaxFiresReached, None)
                        .await;
                } else if let Some(trigger) = self.active_trigger.as_mut() {
                    // Defensive repair for any stale schedule: after the send
                    // budget is exhausted, only the original deadline and RX
                    // matcher can wake this Trigger.
                    trigger.next_write_at = None;
                }
                return;
            }
            (
                TriggerWriteKind::Action {
                    fire_index: trigger.info.fires_confirmed.saturating_add(1),
                },
                trigger.info.spec.action.clone(),
            )
        };
        let effective_settings = self.effective_serial_settings();
        let pacing = WritePacing::resolve(trigger.info.spec.pacing, &effective_settings);
        let Ok(write_timeout) = write_deadline(
            data.len(),
            pacing.chunk_size as usize,
            Duration::from_millis(pacing.chunk_delay_ms),
        ) else {
            self.request_trigger_stop(TriggerStatus::WriteFailed, None)
                .await;
            return;
        };
        let authorization_now = Instant::now();
        let lease_remaining = match self.control.remaining_ttl(
            &trigger.info.owner.id,
            trigger.info.control_id,
            trigger.info.fence,
            authorization_now,
        ) {
            Ok(remaining) => remaining,
            Err(_) => {
                self.request_trigger_stop(TriggerStatus::ControlLost, None)
                    .await;
                return;
            }
        };
        if ensure_lease_covers_write(lease_remaining, write_timeout).is_err() {
            self.request_trigger_stop(TriggerStatus::ControlLost, None)
                .await;
            return;
        }
        // The validation and pacing calculations above are intentionally
        // bounded, but the deadline is authoritative at the final enqueue
        // boundary too: no new physical write may start after it.
        if Instant::now() >= trigger_deadline {
            let status = self
                .active_trigger
                .as_ref()
                .map_or(TriggerStatus::TimedOut, ActiveTrigger::deadline_status);
            self.request_trigger_stop(status, None).await;
            return;
        }

        let Some(port_commands) = self.port.as_ref().map(|port| port.commands.clone()) else {
            self.request_trigger_stop(TriggerStatus::PortClosed, None)
                .await;
            return;
        };
        let (reply, result) = oneshot::channel();
        let command = PortCommand::Write {
            data: data.clone(),
            pacing,
            deadline: tokio::time::Instant::from_std(authorization_now + write_timeout),
            reply,
        };
        match port_commands.try_send(command) {
            Ok(()) => {
                let trigger = self
                    .active_trigger
                    .as_mut()
                    .expect("Trigger is still active while scheduling its write");
                trigger.write_in_flight = Some(kind);
                trigger.next_write_at = None;
                if matches!(kind, TriggerWriteKind::Action { .. }) {
                    trigger.info.status = TriggerStatus::Running;
                }
                let completed = self.trigger_write_results.clone();
                tokio::spawn(async move {
                    let outcome = result.await.map_err(|_| {
                        "serial writer stopped before confirming the Trigger write; the physical outcome is uncertain".to_owned()
                    });
                    let _ = completed
                        .send(TriggerWriteResult {
                            trigger_id,
                            kind,
                            data,
                            outcome,
                        })
                        .await;
                });
                self.publish_snapshot().await;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.request_trigger_stop(TriggerStatus::PortClosed, None)
                    .await;
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.request_trigger_stop(TriggerStatus::WriteFailed, None)
                    .await;
            }
        }
    }

    async fn handle_trigger_write_result(&mut self, result: TriggerWriteResult) {
        let Some(trigger) = self.active_trigger.as_ref() else {
            return;
        };
        if trigger.info.id != result.trigger_id || trigger.write_in_flight != Some(result.kind) {
            return;
        }

        self.active_trigger
            .as_mut()
            .expect("checked above")
            .write_in_flight = None;
        let (written, write_error) = match result.outcome {
            Ok(outcome) => (
                outcome.written,
                outcome.error.or_else(|| {
                    outcome
                        .cancelled
                        .then(|| "Trigger write was cancelled while the port was closing".into())
                }),
            ),
            Err(error) => (0, Some(error)),
        };

        if written > 0 {
            let (actor, operation_id, run_id, trigger_id) = {
                let trigger = self.active_trigger.as_ref().expect("checked above");
                (
                    trigger.info.owner.clone(),
                    trigger.info.operation_id,
                    trigger.bound_run_id,
                    trigger.info.id,
                )
            };
            let mut tx_metadata = metadata([
                ("partial", json!(written != result.data.len())),
                ("trigger_id", json!(trigger_id)),
                (
                    "trigger_write_kind",
                    json!(match result.kind {
                        TriggerWriteKind::Initial => "initial",
                        TriggerWriteKind::Action { .. } => "action",
                    }),
                ),
            ]);
            if let TriggerWriteKind::Action { fire_index } = result.kind {
                tx_metadata.insert("fire_index".into(), json!(fire_index));
            }
            let event_seq = self
                .emit_inner(
                    EventKind::Tx,
                    Direction::Tx,
                    result.data[..written].to_vec(),
                    Some(actor),
                    operation_id,
                    run_id,
                    tx_metadata,
                )
                .await;
            let trigger = self.active_trigger.as_mut().expect("checked above");
            trigger.info.last_write_seq = Some(event_seq);
            trigger.info.tx_bytes_confirmed = trigger
                .info
                .tx_bytes_confirmed
                .saturating_add(written as u64);
        }

        let full_write = written == result.data.len() && write_error.is_none();
        if full_write {
            let now = Instant::now();
            let terminal_status = match result.kind {
                TriggerWriteKind::Initial => {
                    let trigger = self.active_trigger.as_mut().expect("checked above");
                    trigger.confirm_initial_write(now);
                    None
                }
                TriggerWriteKind::Action { .. } => {
                    let trigger = self.active_trigger.as_mut().expect("checked above");
                    trigger.confirm_action_write(now)
                }
            };
            if let Some(status) = terminal_status {
                self.mark_trigger_stopping(status, None);
            }
        } else {
            self.mark_trigger_stopping(TriggerStatus::WriteFailed, None);
        }

        let (buffered, buffered_dropped_bytes, buffered_run_id) = self
            .active_trigger
            .as_mut()
            .map(|trigger| {
                let (events, dropped_bytes) = trigger.buffered_rx.take();
                (events, dropped_bytes, trigger.bound_run_id)
            })
            .unwrap_or_default();
        for event in buffered {
            match event {
                PortEvent::Rx(data) => {
                    self.emit_inner(
                        EventKind::Rx,
                        Direction::Rx,
                        data,
                        Some(device_actor()),
                        None,
                        buffered_run_id,
                        BTreeMap::new(),
                    )
                    .await;
                }
                PortEvent::Overflow { dropped_bytes } => {
                    self.rx_offset = self.rx_offset.saturating_add(dropped_bytes);
                    self.emit_inner(
                        EventKind::Gap,
                        Direction::None,
                        Vec::new(),
                        Some(system_actor()),
                        None,
                        buffered_run_id,
                        metadata([
                            ("reason", json!("serial receive queue overflow")),
                            ("dropped_bytes", json!(dropped_bytes)),
                        ]),
                    )
                    .await;
                }
                PortEvent::Closed { .. } => {}
                PortEvent::ReaderBarrier { .. } => {}
            }
        }
        if buffered_dropped_bytes > 0 {
            self.rx_offset = self.rx_offset.saturating_add(buffered_dropped_bytes);
            self.emit_inner(
                EventKind::Gap,
                Direction::None,
                Vec::new(),
                Some(system_actor()),
                None,
                buffered_run_id,
                metadata([
                    (
                        "reason",
                        json!("Trigger RX audit buffer exceeded its bounded capacity"),
                    ),
                    ("dropped_bytes", json!(buffered_dropped_bytes)),
                ]),
            )
            .await;
        }
        self.finish_stopped_trigger_if_idle().await;
        self.publish_snapshot().await;
    }

    fn mark_trigger_stopping(&mut self, status: TriggerStatus, matched_pattern: Option<Vec<u8>>) {
        debug_assert!(status.is_terminal());
        let Some(trigger) = self.active_trigger.as_mut() else {
            return;
        };
        let replace = trigger
            .pending_terminal
            .as_ref()
            .is_none_or(|(current, _)| {
                trigger_terminal_priority(status) > trigger_terminal_priority(*current)
            });
        if replace {
            trigger.pending_terminal = Some((status, matched_pattern));
        }
        trigger.info.status = TriggerStatus::Stopping;
        trigger.next_write_at = None;
    }

    async fn request_trigger_stop(
        &mut self,
        status: TriggerStatus,
        matched_pattern: Option<Vec<u8>>,
    ) {
        self.mark_trigger_stopping(status, matched_pattern);
        if self
            .active_trigger
            .as_ref()
            .is_some_and(|trigger| trigger.write_in_flight.is_some())
        {
            self.publish_snapshot().await;
            return;
        }
        self.finish_stopped_trigger_if_idle().await;
    }

    async fn finish_stopped_trigger_if_idle(&mut self) {
        let should_finish = self.active_trigger.as_ref().is_some_and(|trigger| {
            trigger.write_in_flight.is_none() && trigger.pending_terminal.is_some()
        });
        if !should_finish {
            return;
        }
        let mut trigger = self.active_trigger.take().expect("checked above");
        let (status, matched_pattern) = trigger.pending_terminal.take().expect("checked above");
        trigger.info.status = status;
        trigger.info.matched_pattern = matched_pattern;
        trigger.info.end_seq = Some(self.seq.saturating_add(1));
        let event_kind = match status {
            TriggerStatus::Matched | TriggerStatus::TimedOut | TriggerStatus::MaxFiresReached => {
                EventKind::TriggerCompleted
            }
            TriggerStatus::Cancelled => EventKind::TriggerCancelled,
            TriggerStatus::ControlLost
            | TriggerStatus::RunLost
            | TriggerStatus::GenerationChanged
            | TriggerStatus::PortClosed
            | TriggerStatus::WriteFailed
            | TriggerStatus::RxGap => EventKind::TriggerFailed,
            TriggerStatus::Armed
            | TriggerStatus::WaitingForStart
            | TriggerStatus::Running
            | TriggerStatus::Stopping => {
                debug_assert!(false, "only terminal Trigger states can be finalized");
                EventKind::TriggerFailed
            }
        };
        let event_seq = self
            .emit_inner(
                event_kind,
                Direction::None,
                Vec::new(),
                Some(trigger.info.owner.clone()),
                trigger.info.operation_id,
                trigger.bound_run_id,
                trigger_event_metadata(&trigger.info),
            )
            .await;
        trigger.info.end_seq = Some(event_seq);
        let trigger_id = trigger.info.id;
        self.terminal_triggers
            .insert(trigger_id, trigger.info.clone());
        self.terminal_trigger_order.push_back(trigger_id);
        while self.terminal_trigger_order.len() > TERMINAL_TRIGGER_HISTORY {
            if let Some(oldest) = self.terminal_trigger_order.pop_front() {
                self.terminal_triggers.remove(&oldest);
            }
        }
        self.publish_snapshot().await;
    }

    async fn drain_trigger_write_results(&mut self) {
        while let Ok(result) = self.trigger_write_result_rx.try_recv() {
            self.handle_trigger_write_result(result).await;
        }
    }

    async fn settle_trigger_write_after_port_stop(&mut self) {
        if self
            .active_trigger
            .as_ref()
            .is_some_and(|trigger| trigger.write_in_flight.is_some())
        {
            // `stop_port` has already joined the writer, so its per-write
            // oneshot is guaranteed to have produced a value or closed.
            // Yield until the tiny forwarding task delivers that outcome;
            // otherwise shutdown could drop a confirmed prefix without TX
            // audit merely because the forwarding task had not been polled.
            if let Some(result) = self.trigger_write_result_rx.recv().await {
                self.handle_trigger_write_result(result).await;
            }
        }
        self.drain_trigger_write_results().await;
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

        let wait_for_journal = self.active_trigger.is_none() && !self.trigger_arming;
        let mut degradation = None;
        let event = match self.journal.try_append(event.clone()) {
            Ok(pending) if self.logging == LoggingState::Healthy && wait_for_journal => {
                match tokio::time::timeout(self.journal.ack_timeout(), pending.wait()).await {
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
            Ok(pending) if self.logging == LoggingState::Healthy => {
                // Trigger matching and scheduling are real-time paths. Enqueue
                // the record in sequence order, deliver the live event as
                // durable=false, and observe the acknowledgement out of band.
                // This prevents a healthy-but-slow disk flush from stretching a
                // 20 ms Trigger interval toward the journal's 100 ms budget.
                if let Err(error) = self.track_journal_ack(event_seq, pending)
                    && self.mark_logging_degraded_message(error)
                {
                    degradation = Some(error.into());
                }
                event
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

    fn track_journal_ack(&self, seq: u64, pending: PendingAppend) -> Result<(), &'static str> {
        let permit = self
            .journal_ack_permits
            .clone()
            .try_acquire_owned()
            .map_err(
                |_| "journal acknowledgement tracker is saturated; continuing live delivery",
            )?;
        let timeout = self.journal.ack_timeout();
        let completed = self.journal_ack_results.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let outcome = match tokio::time::timeout(timeout, pending.wait()).await {
                Ok(Ok(_durable)) => JournalAckOutcome::Durable,
                Ok(Err(error)) => JournalAckOutcome::Failed(error.to_string()),
                Err(_) => JournalAckOutcome::TimedOut,
            };
            let _ = completed.send(JournalAckResult { seq, outcome }).await;
        });
        Ok(())
    }

    async fn handle_journal_ack_result(&mut self, result: JournalAckResult) {
        match result.outcome {
            JournalAckOutcome::Durable => {
                self.ring.lock().await.mark_durable(result.seq);
            }
            JournalAckOutcome::Failed(error) => {
                if self.mark_logging_degraded_message(&error) {
                    self.publish_nondurable_logging_event(error).await;
                }
            }
            JournalAckOutcome::TimedOut => {
                let error = format!(
                    "journal acknowledgement for event {} timed out; continuing live delivery",
                    result.seq
                );
                if self.mark_logging_degraded_message(&error) {
                    self.publish_nondurable_logging_event(error).await;
                }
            }
        }
    }

    fn mark_logging_degraded(&mut self, error: &JournalError) -> bool {
        self.mark_logging_degraded_message(&error.to_string())
    }

    fn mark_logging_degraded_message(&mut self, error: &str) -> bool {
        let changed = self.logging != LoggingState::Degraded;
        self.logging = LoggingState::Degraded;
        self.state_reason = Some(format!("journal degraded: {error}"));
        self.state_code = Some(ErrorCode::Internal);
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
        let resolved = resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        let transport =
            resolve_transport_settings(&self.config.settings, self.transport_profile.as_ref());
        let effective_settings = self.effective_serial_settings();
        self.snapshot.send_replace(SlotSnapshot {
            config: self.config.clone(),
            daemon_epoch: self.daemon_epoch,
            head_seq: self.seq,
            ring_oldest_seq: oldest,
            generation: self.generation,
            endpoint_present: self.endpoint_present,
            session_state: self.session_state,
            state_reason: self.state_reason.clone(),
            state_code: self.state_code,
            target_activity: self.target_activity,
            last_rx_wall_time_ns: self.last_rx_wall_time_ns,
            rx_offset: self.rx_offset,
            tx_offset: self.tx_offset,
            rx_overflow_bytes: self.rx_overflow_bytes,
            control: self.control.current().cloned(),
            active_run: self.active_run.clone(),
            active_trigger: self
                .active_trigger
                .as_ref()
                .map(ActiveTrigger::status_snapshot),
            logging: self.logging,
            effective_shell_prompt: resolved.shell_prompt,
            effective_uboot_prompt: resolved.uboot_prompt,
            effective_write_eol: Some(resolved.write_eol),
            effective_echo: Some(resolved.echo),
            effective_transport: Some(transport),
            effective_write_pacing: Some(WritePacing {
                chunk_size: effective_settings.write_chunk_size,
                chunk_delay_ms: effective_settings.write_chunk_delay_ms,
            }),
        });
    }

    fn effective_serial_settings(&self) -> SerialSettings {
        let mut effective =
            apply_transport_profile(&self.config.settings, self.transport_profile.as_ref());
        let device = resolve_device_settings(&effective, self.device_profile.as_ref());
        effective.write_chunk_size = device.write_pacing.chunk_size;
        effective.write_chunk_delay_ms = device.write_pacing.chunk_delay_ms;
        effective
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
        self.mark_trigger_stopping(TriggerStatus::GenerationChanged, None);
        self.stop_port().await;
        self.settle_trigger_write_after_port_stop().await;
        self.finish_stopped_trigger_if_idle().await;
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
        self.state_code = None;
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
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        resume_on_rollback: bool,
    ) -> Result<(), SlotError> {
        if config.id != self.config.id {
            return Err(SlotError::SlotIdChanged);
        }
        debug_assert!(self.pending_reconfiguration.is_none());
        let previous_transport =
            resolve_transport_settings(&self.config.settings, self.transport_profile.as_ref());
        let next_transport =
            resolve_transport_settings(&config.settings, transport_profile.as_ref());
        let previous_device =
            resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        let next_device = resolve_device_settings(&config.settings, device_profile.as_ref());
        let reopened = self.config.port != config.port
            || self.config.enabled != config.enabled
            || previous_transport != next_transport;
        let device_changed = self.device_profile != device_profile
            || self.config.device_profile != config.device_profile
            || previous_device != next_device;
        if profile_change_requires_idle(
            reopened,
            device_changed,
            self.active_run.is_some(),
            self.active_trigger.is_some(),
        ) {
            return Err(SlotError::ProfileChangeBusy);
        }
        if reopened {
            self.pause_for_reconfigure().await?;
        }
        self.pending_reconfiguration = Some(PendingReconfiguration::Replace {
            config: Box::new(config),
            transport_profile,
            device_profile,
            resume_on_rollback,
            reopened,
        });
        Ok(())
    }

    async fn stage_removal(&mut self) -> Result<(), SlotError> {
        debug_assert!(self.pending_reconfiguration.is_none());
        self.pause_for_reconfigure().await?;
        self.pending_reconfiguration = Some(PendingReconfiguration::Remove);
        Ok(())
    }

    fn stage_device_profile(
        &mut self,
        device_profile: Option<DeviceProfile>,
    ) -> Result<bool, SlotError> {
        debug_assert!(self.pending_reconfiguration.is_none());
        if self.device_profile == device_profile {
            return Ok(false);
        }
        if self.active_run.is_some() || self.active_trigger.is_some() {
            return Err(SlotError::ProfileChangeBusy);
        }
        self.pending_reconfiguration =
            Some(PendingReconfiguration::DeviceProfile { device_profile });
        Ok(true)
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
            PendingReconfiguration::Replace {
                config,
                transport_profile,
                device_profile,
                reopened,
                ..
            } => {
                self.apply_committed_reconfiguration(
                    *config,
                    transport_profile,
                    device_profile,
                    reopened,
                )
                .await;
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
            PendingReconfiguration::DeviceProfile { device_profile } => {
                self.apply_committed_device_profile(device_profile).await;
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
                resume_on_rollback,
                reopened,
                ..
            } => {
                if reopened && resume_on_rollback {
                    self.resume_current_config();
                    self.publish_snapshot().await;
                }
            }
            PendingReconfiguration::Remove => {
                self.resume_current_config();
                self.publish_snapshot().await;
            }
            PendingReconfiguration::DeviceProfile { .. } => {
                // Staging a profile is inert, so dropping the candidate fully
                // restores the pre-transaction state without an event.
            }
        }
        Ok(())
    }

    async fn apply_committed_device_profile(&mut self, device_profile: Option<DeviceProfile>) {
        let previous_effective =
            resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        self.device_profile = device_profile;
        let effective =
            resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        self.emit(
            EventKind::SlotReconfigured,
            Direction::None,
            Vec::new(),
            Some(system_actor()),
            None,
            metadata([
                (
                    "current",
                    serde_json::to_value(&self.config).unwrap_or(Value::Null),
                ),
                (
                    "device_profile",
                    serde_json::to_value(
                        self.device_profile
                            .as_ref()
                            .map(|profile| profile.name.as_str()),
                    )
                    .unwrap_or(Value::Null),
                ),
                (
                    "previous_effective",
                    serde_json::to_value(previous_effective).unwrap_or(Value::Null),
                ),
                (
                    "effective",
                    serde_json::to_value(effective).unwrap_or(Value::Null),
                ),
                ("profile_only", json!(true)),
            ]),
        )
        .await;
    }

    async fn apply_committed_reconfiguration(
        &mut self,
        config: SlotConfig,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        reopened: bool,
    ) {
        let previous_effective =
            resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        let previous_transport =
            resolve_transport_settings(&self.config.settings, self.transport_profile.as_ref());
        let previous = std::mem::replace(&mut self.config, config);
        self.transport_profile = transport_profile;
        self.device_profile = device_profile;
        if reopened {
            self.resume_current_config();
        }
        let effective =
            resolve_device_settings(&self.config.settings, self.device_profile.as_ref());
        let effective_transport =
            resolve_transport_settings(&self.config.settings, self.transport_profile.as_ref());
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
                (
                    "device_profile",
                    serde_json::to_value(
                        self.device_profile
                            .as_ref()
                            .map(|profile| profile.name.as_str()),
                    )
                    .unwrap_or(Value::Null),
                ),
                (
                    "previous_effective",
                    serde_json::to_value(previous_effective).unwrap_or(Value::Null),
                ),
                (
                    "effective",
                    serde_json::to_value(effective).unwrap_or(Value::Null),
                ),
                (
                    "previous_transport",
                    serde_json::to_value(previous_transport).unwrap_or(Value::Null),
                ),
                (
                    "effective_transport",
                    serde_json::to_value(effective_transport).unwrap_or(Value::Null),
                ),
                ("transport_reopened", json!(reopened)),
                ("profile_only", json!(!reopened)),
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
        if self.config.enabled && self.effective_serial_settings().auto_open {
            self.session_state = SessionState::WaitingForPort;
            self.state_reason = None;
            self.state_code = None;
        } else {
            self.session_state = SessionState::Disabled;
            self.state_reason = None;
            self.state_code = None;
        }
    }

    async fn prepare_shutdown(&mut self) {
        self.administratively_paused = true;
        let was_online = self.port.is_some();
        self.mark_trigger_stopping(TriggerStatus::PortClosed, None);
        self.stop_port().await;
        self.settle_trigger_write_after_port_stop().await;
        self.finish_stopped_trigger_if_idle().await;
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
        self.state_code = None;
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
        let mut reader_tail = ReaderTail::default();
        if let Some(port) = self.port.take() {
            let _ = port.cancel.send(true);
            drop(port.commands);
            drop(port.reader_commands);
            if let Ok(tail) = port.reader.await {
                reader_tail = tail;
            }
            let _ = port.writer.await;
        }
        if let Some(mut events) = self.port_events.take() {
            while let Ok(event) = events.try_recv() {
                let (data, dropped_bytes) = drained_port_event_parts(event);
                if let Some(data) = data {
                    self.handle_rx_data(data).await;
                }
                if dropped_bytes > 0 {
                    self.handle_rx_overflow(dropped_bytes).await;
                }
            }
        }
        if reader_tail.dropped_bytes > 0 {
            self.handle_rx_overflow(reader_tail.dropped_bytes).await;
        }
        if !reader_tail.pending.is_empty() {
            self.handle_rx_data(reader_tail.pending).await;
        }
    }
}

fn drained_port_event_parts(event: PortEvent) -> (Option<Vec<u8>>, u64) {
    match event {
        PortEvent::Rx(data) => (Some(data), 0),
        PortEvent::Overflow { dropped_bytes } => (None, dropped_bytes),
        // The caller already owns the authoritative close reason, so a
        // duplicate worker-close notification must not recurse into
        // `stop_port`. Its overflow accounting is still authoritative:
        // a reader that successfully queued this event returns an empty tail.
        PortEvent::Closed { dropped_bytes, .. } => (None, dropped_bytes),
        PortEvent::ReaderBarrier { .. } => (None, 0),
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
    CancelAcquire {
        actor: Actor,
        control_id: Uuid,
    },
    Write {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        pacing: Option<WritePacing>,
        description: Option<String>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        cooperative: bool,
    },
    SendBreak {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        duration_ms: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
    },
    StartTrigger {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Option<Uuid>,
        expected_run_id: Option<Uuid>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        spec: TriggerSpec,
    },
    TriggerStatus {
        actor: Actor,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    },
    CancelTrigger {
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
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
            Self::Write {
                description,
                command_sequence,
                ..
            } => {
                if let Some(description) = description {
                    validate_command_description(description)?;
                }
                if let Some(command_sequence) = command_sequence {
                    if description.is_none() {
                        return Err(SlotError::InvalidCommandSequenceAudit);
                    }
                    validate_command_sequence_audit(command_sequence)?;
                }
                Ok(())
            }
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
            Self::StartTrigger { spec, .. } => validate_trigger_spec(spec),
            Self::SendBreak { duration_ms, .. }
                if !(MIN_BREAK_DURATION_MS..=MAX_BREAK_DURATION_MS).contains(duration_ms) =>
            {
                Err(SlotError::InvalidBreakDuration)
            }
            _ => Ok(()),
        }
    }

    /// Fingerprint fields that describe the intended physical write. Actor,
    /// lease ID, and fence are deliberately excluded because those are
    /// connection-scoped authorization data and change after reconnect.
    fn write_fingerprint(&self) -> Option<Vec<u8>> {
        match self {
            Self::Write {
                data,
                operation_id,
                expected_run_id,
                pacing,
                description,
                command_sequence,
                sequence_precondition,
                cooperative,
                ..
            } => Some(
                serde_json::to_vec(&(
                    "write",
                    data,
                    operation_id,
                    expected_run_id,
                    pacing,
                    description,
                    command_sequence,
                    sequence_precondition,
                    cooperative,
                ))
                .expect("write request fields are serializable"),
            ),
            Self::StartTrigger {
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                sequence_precondition,
                spec,
                ..
            } => Some(
                serde_json::to_vec(&(
                    "trigger_start",
                    daemon_epoch,
                    generation,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                    spec,
                ))
                .expect("Trigger request fields are serializable"),
            ),
            Self::SendBreak {
                duration_ms,
                operation_id,
                expected_run_id,
                sequence_precondition,
                ..
            } => Some(
                serde_json::to_vec(&(
                    "send_break",
                    duration_ms,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                ))
                .expect("BREAK request fields are serializable"),
            ),
            _ => None,
        }
    }

    fn validate_write_authorization(
        &self,
        control: &ControlState,
        active_run: Option<&RunInfo>,
    ) -> Result<(), SlotError> {
        match self {
            Self::Write {
                actor,
                control_id,
                fence,
                expected_run_id,
                pacing,
                cooperative,
                ..
            } => {
                if *cooperative {
                    if pacing.is_some() {
                        return Err(ControlError::NotOwner.into());
                    }
                    validate_cooperative_write(actor, *expected_run_id, control, active_run)
                } else {
                    control.validate(&actor.id, *control_id, *fence, Instant::now())?;
                    validate_expected_write_run(*expected_run_id, actor, active_run)
                }
            }
            Self::StartTrigger {
                actor,
                control_id,
                fence,
                expected_run_id,
                ..
            }
            | Self::SendBreak {
                actor,
                control_id,
                fence,
                expected_run_id,
                ..
            } => {
                control.validate(&actor.id, *control_id, *fence, Instant::now())?;
                validate_expected_write_run(*expected_run_id, actor, active_run)?;
                Ok(())
            }
            _ => Ok(()),
        }
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
            Self::CancelAcquire { actor, control_id } => {
                serde_json::to_vec(&("cancel_acquire", &actor.id, control_id))
            }
            Self::Write {
                actor,
                control_id,
                fence,
                data,
                operation_id,
                expected_run_id,
                pacing,
                description,
                command_sequence,
                sequence_precondition,
                cooperative,
            } => serde_json::to_vec(&(
                "write",
                &actor.id,
                control_id,
                fence,
                data,
                operation_id,
                expected_run_id,
                pacing,
                description,
                command_sequence,
                sequence_precondition,
                cooperative,
            )),
            Self::StartTrigger {
                actor,
                control_id,
                fence,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                sequence_precondition,
                spec,
            } => serde_json::to_vec(&(
                "trigger_start",
                &actor.id,
                control_id,
                fence,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                sequence_precondition,
                spec,
            )),
            Self::SendBreak {
                actor,
                control_id,
                fence,
                duration_ms,
                operation_id,
                expected_run_id,
                sequence_precondition,
            } => serde_json::to_vec(&(
                "send_break",
                &actor.id,
                control_id,
                fence,
                duration_ms,
                operation_id,
                expected_run_id,
                sequence_precondition,
            )),
            Self::TriggerStatus {
                actor,
                daemon_epoch,
                generation,
                trigger_id,
            } => serde_json::to_vec(&(
                "trigger_status",
                &actor.id,
                daemon_epoch,
                generation,
                trigger_id,
            )),
            Self::CancelTrigger {
                actor,
                control_id,
                fence,
                daemon_epoch,
                generation,
                trigger_id,
            } => serde_json::to_vec(&(
                "trigger_cancel",
                &actor.id,
                control_id,
                fence,
                daemon_epoch,
                generation,
                trigger_id,
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

fn validate_expected_write_run(
    expected_run_id: Option<Uuid>,
    actor: &Actor,
    active_run: Option<&RunInfo>,
) -> Result<(), SlotError> {
    let Some(expected_run_id) = expected_run_id else {
        return Ok(());
    };
    let active_run = active_run.ok_or(SlotError::WriteRunMissing { expected_run_id })?;
    if active_run.id != expected_run_id {
        return Err(SlotError::WriteRunMismatch {
            expected_run_id,
            active_run_id: active_run.id,
        });
    }
    if active_run.owner.id != actor.id {
        return Err(SlotError::WriteRunNotOwner { expected_run_id });
    }
    Ok(())
}

/// Authorizes the explicit Human/Agent cooperative-write escape hatch.
///
/// Cooperative writes never borrow the Agent's fence and never transfer
/// ownership. They are accepted only while the current lease and active Run
/// belong to the same Agent, so an ordinary nil/stale lease cannot silently
/// turn into an unfenced write. The resulting foreign TX remains visible to
/// Agent capture and therefore downgrades command confidence as interference.
fn validate_cooperative_write(
    actor: &Actor,
    expected_run_id: Option<Uuid>,
    control: &ControlState,
    active_run: Option<&RunInfo>,
) -> Result<(), SlotError> {
    if actor.kind != ActorKind::Human {
        return Err(ControlError::NotOwner.into());
    }
    let expected_run_id = expected_run_id.ok_or(ControlError::NotOwner)?;
    let lease = control.current().ok_or(ControlError::NotOwner)?;
    let run = active_run.ok_or(SlotError::WriteRunMissing { expected_run_id })?;
    if run.id != expected_run_id {
        return Err(SlotError::WriteRunMismatch {
            expected_run_id,
            active_run_id: run.id,
        });
    }
    if lease.owner.kind != ActorKind::Agent
        || run.owner.kind != ActorKind::Agent
        || lease.owner.id != run.owner.id
    {
        return Err(ControlError::NotOwner.into());
    }
    Ok(())
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

fn validate_command_description(description: &str) -> Result<(), SlotError> {
    if description.is_empty()
        || description != description.trim()
        || description.len() > MAX_COMMAND_DESCRIPTION_BYTES
        || description.chars().any(char::is_control)
    {
        Err(SlotError::InvalidCommandDescription)
    } else {
        Ok(())
    }
}

fn validate_command_sequence_audit(audit: &CommandSequenceAuditContext) -> Result<(), SlotError> {
    if audit.sequence_id.is_nil()
        || validate_command_description(&audit.description).is_err()
        || !(1..=8).contains(&audit.step_count)
        || audit.step_index >= audit.step_count
    {
        Err(SlotError::InvalidCommandSequenceAudit)
    } else {
        Ok(())
    }
}

/// Checks a dependent-write boundary against the authoritative Slot state.
///
/// This is called by the Slot actor after authorization and immediately before
/// the port command is enqueued. The actor cannot process another write while
/// this check and enqueue are in progress. RX is intentionally allowed: it may
/// arrive between a prompt match and the planned reply without changing the
/// dependent TX history. Any TX or any kind of evidence gap fails closed.
fn validate_sequence_write_precondition(
    precondition: &SequenceWritePrecondition,
    daemon_epoch: Uuid,
    generation: u64,
    tx_offset: u64,
    head_seq: u64,
    ring: &EventRing,
) -> Result<(), SlotError> {
    let changed = |reason: String| SlotError::SequenceBoundaryChanged { reason };
    if precondition.cursor.epoch != daemon_epoch {
        return Err(changed("daemon epoch changed".into()));
    }
    if precondition.expected_generation != generation {
        return Err(changed(format!(
            "serial generation changed from {} to {generation}",
            precondition.expected_generation
        )));
    }
    if precondition.expected_tx_offset != tx_offset {
        return Err(changed(format!(
            "TX offset changed from {} to {tx_offset}",
            precondition.expected_tx_offset
        )));
    }
    let replay = ring
        .replay(
            daemon_epoch,
            Some(&precondition.cursor),
            head_seq,
            RING_EVENTS,
        )
        .map_err(|error| changed(error.to_string()))?;
    if let Some(gap) = replay.gap {
        return Err(changed(format!("timeline replay gap: {:?}", gap.reason)));
    }
    if let Some(event) = replay
        .events
        .iter()
        .find(|event| event.kind == EventKind::Gap || event.direction == Direction::Tx)
    {
        return Err(changed(format!(
            "timeline {:?} event at sequence {} crossed the boundary",
            event.kind, event.seq
        )));
    }
    Ok(())
}

fn write_event_metadata(
    partial: bool,
    cooperative: bool,
    description: Option<String>,
    command_sequence: Option<CommandSequenceAuditContext>,
) -> BTreeMap<String, Value> {
    let mut event_metadata = metadata([
        ("partial", json!(partial)),
        ("cooperative", json!(cooperative)),
    ]);
    if let Some(description) = description {
        event_metadata.insert("command_description".into(), json!(description));
    }
    if let Some(command_sequence) = command_sequence {
        event_metadata.insert(
            "command_sequence_id".into(),
            json!(command_sequence.sequence_id),
        );
        event_metadata.insert(
            "command_sequence_description".into(),
            json!(command_sequence.description),
        );
        event_metadata.insert(
            "command_sequence_step_index".into(),
            json!(command_sequence.step_index),
        );
        event_metadata.insert(
            "command_sequence_step_count".into(),
            json!(command_sequence.step_count),
        );
    }
    event_metadata
}

fn profile_change_requires_idle(
    transport_reopened: bool,
    device_changed: bool,
    run_active: bool,
    trigger_active: bool,
) -> bool {
    !transport_reopened && device_changed && (run_active || trigger_active)
}

fn validate_trigger_spec(spec: &TriggerSpec) -> Result<(), SlotError> {
    if spec.action.is_empty() || spec.action.len() > MAX_TRIGGER_ACTION_BYTES {
        return Err(SlotError::InvalidTriggerAction);
    }
    if let Some(initial) = &spec.initial_write {
        if initial.is_empty() {
            return Err(SlotError::EmptyWrite);
        }
        if initial.len() > MAX_TRIGGER_INITIAL_WRITE_BYTES {
            return Err(SlotError::TriggerInitialWriteTooLarge);
        }
    }
    if !(MIN_TRIGGER_INTERVAL_MS..=MAX_TRIGGER_INTERVAL_MS).contains(&spec.interval_ms) {
        return Err(SlotError::InvalidTriggerInterval);
    }
    if !(MIN_TRIGGER_TIMEOUT_MS..=MAX_TRIGGER_TIMEOUT_MS).contains(&spec.timeout_ms) {
        return Err(SlotError::InvalidTriggerTimeout);
    }
    if !(1..=MAX_TRIGGER_FIRES).contains(&spec.max_fires) {
        return Err(SlotError::InvalidTriggerMaxFires);
    }
    if spec.stop_contains.len() > MAX_TRIGGER_PATTERNS
        || spec
            .start_contains
            .iter()
            .chain(spec.stop_contains.iter())
            .any(|pattern| pattern.is_empty() || pattern.len() > MAX_TRIGGER_PATTERN_BYTES)
    {
        return Err(SlotError::InvalidTriggerPatterns);
    }
    let planned_action_bytes = spec
        .action
        .len()
        .checked_mul(spec.max_fires as usize)
        .ok_or(SlotError::TriggerTotalBytesTooLarge)?;
    let planned_total = spec
        .initial_write
        .as_ref()
        .map_or(0, Vec::len)
        .checked_add(planned_action_bytes)
        .ok_or(SlotError::TriggerTotalBytesTooLarge)?;
    if planned_total > MAX_TRIGGER_TOTAL_BYTES {
        return Err(SlotError::TriggerTotalBytesTooLarge);
    }
    Ok(())
}

fn validate_trigger_pacing(spec: &TriggerSpec, settings: &SerialSettings) -> Result<(), SlotError> {
    let pacing = WritePacing::resolve(spec.pacing, settings);
    for length in spec
        .initial_write
        .iter()
        .map(Vec::len)
        .chain(std::iter::once(spec.action.len()))
    {
        write_deadline(
            length,
            pacing.chunk_size as usize,
            Duration::from_millis(pacing.chunk_delay_ms),
        )
        .map_err(|required_ms| SlotError::WriteDeadlineExceeded {
            required_ms,
            maximum_ms: duration_millis_saturating(MAX_WRITE_TIMEOUT),
        })?;
    }
    Ok(())
}

fn validate_trigger_identity(
    trigger: &TriggerInfo,
    daemon_epoch: Uuid,
    generation: u64,
) -> Result<(), SlotError> {
    if trigger.daemon_epoch != daemon_epoch {
        return Err(SlotError::TriggerEpochMismatch);
    }
    if trigger.generation != generation {
        return Err(SlotError::TriggerGenerationMismatch);
    }
    Ok(())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn trigger_write_due_at(
    now: Instant,
    deadline: Instant,
    next_write_at: Option<Instant>,
    stopping: bool,
    write_in_flight: bool,
) -> bool {
    !stopping
        && !write_in_flight
        && now < deadline
        && next_write_at.is_some_and(|write_at| now >= write_at)
}

fn trigger_terminal_priority(status: TriggerStatus) -> u8 {
    match status {
        TriggerStatus::RxGap => 100,
        TriggerStatus::WriteFailed
        | TriggerStatus::PortClosed
        | TriggerStatus::GenerationChanged => 90,
        TriggerStatus::ControlLost | TriggerStatus::RunLost => 80,
        TriggerStatus::Matched => 70,
        TriggerStatus::Cancelled => 60,
        TriggerStatus::TimedOut => 20,
        TriggerStatus::MaxFiresReached => 10,
        TriggerStatus::Armed
        | TriggerStatus::WaitingForStart
        | TriggerStatus::Running
        | TriggerStatus::Stopping => 0,
    }
}

fn trigger_event_metadata(info: &TriggerInfo) -> BTreeMap<String, Value> {
    let mut values = metadata([
        ("trigger_id", json!(info.id)),
        ("status", json!(info.status)),
        ("fires_confirmed", json!(info.fires_confirmed)),
        ("tx_bytes_confirmed", json!(info.tx_bytes_confirmed)),
        ("trigger", serde_json::to_value(info).unwrap_or(Value::Null)),
    ]);
    if let Some(pattern) = &info.matched_pattern {
        values.insert(
            "matched_pattern_base64".into(),
            json!(base64::engine::general_purpose::STANDARD.encode(pattern)),
        );
    }
    values
}

fn is_cacheable_write_result(result: &Result<CommandResult, SlotError>) -> bool {
    matches!(
        result,
        Ok(CommandResult::WriteAccepted { .. })
            | Ok(CommandResult::BreakSent { .. })
            | Ok(CommandResult::TriggerStarted { .. })
            | Err(SlotError::PartialWrite { .. })
            | Err(SlotError::BreakFailed { .. })
    )
}

// This enum is only a short-lived dispatch value. Boxing the common request
// path would add one heap allocation to every Slot command without reducing
// any retained queue.
#[allow(clippy::large_enum_variant)]
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
        config: Box<SlotConfig>,
        transport_profile: Option<TransportProfile>,
        device_profile: Option<DeviceProfile>,
        resume_on_rollback: bool,
        reply: oneshot::Sender<Result<(), SlotError>>,
    },
    StageDeviceProfile {
        device_profile: Option<DeviceProfile>,
        reply: oneshot::Sender<Result<bool, SlotError>>,
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
            SlotCommand::CancelAcquire {
                request_id,
                actor,
                reply,
                control_id,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::CancelAcquire { actor, control_id },
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
                expected_run_id,
                pacing,
                description,
                command_sequence,
                sequence_precondition,
                cooperative,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::Write {
                    actor,
                    control_id,
                    fence,
                    data,
                    operation_id,
                    expected_run_id,
                    pacing,
                    description,
                    command_sequence,
                    sequence_precondition,
                    cooperative,
                },
                reply,
            },
            SlotCommand::SendBreak {
                request_id,
                actor,
                control_id,
                fence,
                duration_ms,
                operation_id,
                expected_run_id,
                sequence_precondition,
                reply,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::SendBreak {
                    actor,
                    control_id,
                    fence,
                    duration_ms,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                },
                reply,
            },
            SlotCommand::StartTrigger {
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
                reply,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::StartTrigger {
                    actor,
                    control_id,
                    fence,
                    daemon_epoch,
                    generation,
                    operation_id,
                    expected_run_id,
                    sequence_precondition,
                    spec,
                },
                reply,
            },
            SlotCommand::TriggerStatus {
                request_id,
                actor,
                daemon_epoch,
                generation,
                trigger_id,
                reply,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::TriggerStatus {
                    actor,
                    daemon_epoch,
                    generation,
                    trigger_id,
                },
                reply,
            },
            SlotCommand::CancelTrigger {
                request_id,
                actor,
                control_id,
                fence,
                daemon_epoch,
                generation,
                trigger_id,
                reply,
            } => CommandDisposition::Request {
                key: (actor.id.clone(), request_id),
                request: SlotRequest::CancelTrigger {
                    actor,
                    control_id,
                    fence,
                    daemon_epoch,
                    generation,
                    trigger_id,
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
                transport_profile,
                device_profile,
                resume_on_rollback,
                reply,
            } => CommandDisposition::StageReconfiguration {
                config,
                transport_profile,
                device_profile,
                resume_on_rollback,
                reply,
            },
            SlotCommand::StageDeviceProfile {
                device_profile,
                reply,
            } => CommandDisposition::StageDeviceProfile {
                device_profile,
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

fn initial_snapshot(
    config: SlotConfig,
    transport_profile: Option<TransportProfile>,
    device_profile: Option<DeviceProfile>,
    daemon_epoch: Uuid,
    staged: bool,
) -> SlotSnapshot {
    let state = if staged {
        SessionState::Disabled
    } else if config.enabled
        && resolve_transport_settings(&config.settings, transport_profile.as_ref()).auto_open
    {
        SessionState::WaitingForPort
    } else {
        SessionState::Disabled
    };
    let resolved = resolve_device_settings(&config.settings, device_profile.as_ref());
    let transport = resolve_transport_settings(&config.settings, transport_profile.as_ref());
    SlotSnapshot {
        config,
        daemon_epoch,
        head_seq: 0,
        ring_oldest_seq: None,
        generation: 0,
        endpoint_present: false,
        session_state: state,
        state_reason: staged.then(|| "slot configuration pending persistence".into()),
        state_code: None,
        target_activity: TargetActivity::Unknown,
        last_rx_wall_time_ns: None,
        rx_offset: 0,
        tx_offset: 0,
        rx_overflow_bytes: 0,
        control: None,
        active_run: None,
        active_trigger: None,
        logging: LoggingState::Healthy,
        effective_shell_prompt: resolved.shell_prompt,
        effective_uboot_prompt: resolved.uboot_prompt,
        effective_write_eol: Some(resolved.write_eol),
        effective_echo: Some(resolved.echo),
        effective_transport: Some(transport),
        effective_write_pacing: Some(resolved.write_pacing),
    }
}

#[cfg(windows)]
fn serial_backend_name() -> &'static str {
    "windows_blocking_com"
}

#[cfg(not(windows))]
fn serial_backend_name() -> &'static str {
    "tokio_serial"
}

#[derive(Debug)]
struct PortOpenError {
    code: ErrorCode,
    message: String,
}

impl PortOpenError {
    fn from_serial(error: serialport::Error) -> Self {
        use serialport::ErrorKind as SerialErrorKind;
        let message = error.to_string();
        let normalized = message.to_ascii_lowercase();
        let explicitly_busy = normalized.contains("busy")
            || normalized.contains("in use")
            || normalized.contains("sharing violation")
            || normalized.contains("access is denied")
            || message.contains("拒绝访问");
        let code = if explicitly_busy {
            ErrorCode::PortBusy
        } else {
            match error.kind() {
                SerialErrorKind::NoDevice => ErrorCode::PortNotFound,
                SerialErrorKind::Io(std::io::ErrorKind::NotFound) => ErrorCode::PortNotFound,
                SerialErrorKind::Io(std::io::ErrorKind::PermissionDenied) => {
                    #[cfg(windows)]
                    {
                        ErrorCode::PortBusy
                    }
                    #[cfg(not(windows))]
                    {
                        ErrorCode::PortAccessDenied
                    }
                }
                SerialErrorKind::Io(std::io::ErrorKind::WouldBlock)
                | SerialErrorKind::Io(std::io::ErrorKind::AlreadyExists) => ErrorCode::PortBusy,
                SerialErrorKind::InvalidInput => ErrorCode::BadRequest,
                _ => ErrorCode::PortIo,
            }
        };
        Self { code, message }
    }
}

impl std::fmt::Display for PortOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

#[cfg(not(windows))]
fn open_port(port_name: &str, settings: &SerialSettings) -> Result<SerialStream, PortOpenError> {
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
        .map_err(PortOpenError::from_serial)?;
    stream
        .write_data_terminal_ready(settings.dtr)
        .map_err(PortOpenError::from_serial)?;
    // With hardware flow control the driver owns RTS. Manually forcing the
    // line can defeat CTS/RTS negotiation and may reset some target boards.
    if settings.flow_control != FlowControl::Hardware {
        stream
            .write_request_to_send(settings.rts)
            .map_err(PortOpenError::from_serial)?;
    }
    Ok(stream)
}

#[cfg(windows)]
struct WindowsSerialPort {
    reader: COMPort,
    writer: COMPort,
}

/// Tokio's generic split hides the underlying `SerialPort` methods. Keep a
/// small shared wrapper so the writer task can assert/clear BREAK while the
/// same nonblocking stream continues to serve RX.
#[cfg(not(windows))]
#[derive(Clone)]
struct SharedSerialStream {
    inner: Arc<std::sync::Mutex<SerialStream>>,
}

#[cfg(not(windows))]
impl SharedSerialStream {
    fn new(stream: SerialStream) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(stream)),
        }
    }

    fn set_break(&self) -> Result<(), String> {
        self.inner
            .lock()
            .map_err(|_| "serial stream lock was poisoned".to_owned())?
            .set_break()
            .map_err(|error| error.to_string())
    }

    fn clear_break(&self) -> Result<(), String> {
        self.inner
            .lock()
            .map_err(|_| "serial stream lock was poisoned".to_owned())?
            .clear_break()
            .map_err(|error| error.to_string())
    }
}

#[cfg(not(windows))]
impl AsyncRead for SharedSerialStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut stream = match self.inner.lock() {
            Ok(stream) => stream,
            Err(_) => {
                return Poll::Ready(Err(std::io::Error::other(
                    "serial stream lock was poisoned",
                )));
            }
        };
        Pin::new(&mut *stream).poll_read(context, buffer)
    }
}

#[cfg(not(windows))]
impl AsyncWrite for SharedSerialStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let mut stream = match self.inner.lock() {
            Ok(stream) => stream,
            Err(_) => {
                return Poll::Ready(Err(std::io::Error::other(
                    "serial stream lock was poisoned",
                )));
            }
        };
        Pin::new(&mut *stream).poll_write(context, data)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut stream = match self.inner.lock() {
            Ok(stream) => stream,
            Err(_) => {
                return Poll::Ready(Err(std::io::Error::other(
                    "serial stream lock was poisoned",
                )));
            }
        };
        Pin::new(&mut *stream).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let mut stream = match self.inner.lock() {
            Ok(stream) => stream,
            Err(_) => {
                return Poll::Ready(Err(std::io::Error::other(
                    "serial stream lock was poisoned",
                )));
            }
        };
        Pin::new(&mut *stream).poll_shutdown(context)
    }
}

#[cfg(windows)]
fn open_port(
    port_name: &str,
    settings: &SerialSettings,
) -> Result<WindowsSerialPort, PortOpenError> {
    // `tokio-serial` implements Windows COM I/O through Tokio's named-pipe
    // backend. Some serial-card drivers never complete an overlapped write;
    // mio then deliberately keeps that write (and the exclusive COM handle)
    // alive while dropping the stream. Use native synchronous COM handles on
    // Windows so a bounded WriteFile has settled before either handle drops.
    let builder = serialport::new(port_name, settings.baud_rate)
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
        })
        // The reader checks bytes_to_read before calling ReadFile, so this
        // primarily bounds a synchronous WriteFile that stops making progress.
        .timeout(WRITE_TIMEOUT);
    let mut reader = builder.open_native().map_err(PortOpenError::from_serial)?;
    reader
        .write_data_terminal_ready(settings.dtr)
        .map_err(PortOpenError::from_serial)?;
    if settings.flow_control != FlowControl::Hardware {
        reader
            .write_request_to_send(settings.rts)
            .map_err(PortOpenError::from_serial)?;
    }
    let writer = reader
        .try_clone_native()
        .map_err(PortOpenError::from_serial)?;
    Ok(WindowsSerialPort { reader, writer })
}

#[cfg(not(windows))]
fn spawn_port_worker(stream: SerialStream) -> (PortWorker, mpsc::Receiver<PortEvent>) {
    let (commands, command_rx) = mpsc::channel(PORT_WRITE_QUEUE);
    let (reader_commands, reader_command_rx) = mpsc::channel(PORT_READER_COMMAND_QUEUE);
    let (events, event_rx) = mpsc::channel(PORT_EVENT_QUEUE);
    let (cancel, cancel_rx) = watch::channel(false);
    let shared = SharedSerialStream::new(stream);
    let break_control = shared.clone();
    let (reader_half, writer_half) = tokio::io::split(shared);
    let reader = tokio::spawn(run_port_reader(
        reader_half,
        events.clone(),
        reader_command_rx,
        cancel_rx.clone(),
    ));
    let writer = tokio::spawn(run_port_writer(
        writer_half,
        break_control,
        command_rx,
        events,
        cancel_rx,
    ));
    (
        PortWorker {
            commands,
            reader_commands,
            cancel,
            reader,
            writer,
        },
        event_rx,
    )
}

#[cfg(windows)]
fn spawn_port_worker(stream: WindowsSerialPort) -> (PortWorker, mpsc::Receiver<PortEvent>) {
    let (commands, command_rx) = mpsc::channel(PORT_WRITE_QUEUE);
    let (reader_commands, reader_command_rx) = mpsc::channel(PORT_READER_COMMAND_QUEUE);
    let (events, event_rx) = mpsc::channel(PORT_EVENT_QUEUE);
    let (cancel, cancel_rx) = watch::channel(false);
    let writer_failed = Arc::new(AtomicBool::new(false));
    let reader = tokio::task::spawn_blocking({
        let events = events.clone();
        let cancel = cancel_rx.clone();
        let writer_failed = writer_failed.clone();
        move || {
            run_windows_port_reader(
                stream.reader,
                events,
                reader_command_rx,
                cancel,
                writer_failed,
            )
        }
    });
    let writer = tokio::task::spawn_blocking(move || {
        run_windows_port_writer(stream.writer, command_rx, events, cancel_rx, writer_failed)
    });
    (
        PortWorker {
            commands,
            reader_commands,
            cancel,
            reader,
            writer,
        },
        event_rx,
    )
}

#[cfg(not(windows))]
async fn run_port_reader(
    mut reader: tokio::io::ReadHalf<SharedSerialStream>,
    events: mpsc::Sender<PortEvent>,
    mut commands: mpsc::Receiver<PortReaderCommand>,
    mut cancel: watch::Receiver<bool>,
) -> ReaderTail {
    let mut buffer = vec![0_u8; RX_BUFFER_BYTES];
    let mut pending = Vec::with_capacity(RX_BUFFER_BYTES);
    let mut dropped_bytes = 0_u64;
    let mut flush_deadline = None;

    loop {
        if pending.len() >= RX_BUFFER_BYTES {
            enqueue_rx(&events, &mut pending, &mut dropped_bytes);
            flush_deadline = arm_rx_flush_deadline(
                None,
                tokio::time::Instant::now(),
                !pending.is_empty() || dropped_bytes > 0,
            );
        }
        let read_capacity = rx_read_capacity(pending.len());
        let deadline = flush_deadline;
        let flush = async move {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            command = commands.recv() => {
                let Some(PortReaderCommand::Barrier { id }) = command else {
                    return ReaderTail {
                        pending,
                        dropped_bytes,
                    };
                };
                enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                if dropped_bytes > 0 {
                    let dropped = dropped_bytes;
                    let sent = tokio::select! {
                        _ = cancel.changed() => false,
                        result = events.send(PortEvent::Overflow { dropped_bytes: dropped }) => {
                            result.is_ok()
                        }
                    };
                    if !sent {
                        return ReaderTail {
                            pending,
                            dropped_bytes,
                        };
                    }
                    dropped_bytes = 0;
                }
                let sent = tokio::select! {
                    _ = cancel.changed() => false,
                    result = events.send(PortEvent::ReaderBarrier { id }) => result.is_ok(),
                };
                if !sent {
                    return ReaderTail {
                        pending,
                        dropped_bytes,
                    };
                }
                flush_deadline = None;
            }
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return ReaderTail {
                        pending,
                        dropped_bytes,
                    };
                }
            }
            _ = flush => {
                enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                flush_deadline = arm_rx_flush_deadline(
                    None,
                    tokio::time::Instant::now(),
                    !pending.is_empty() || dropped_bytes > 0,
                );
            }
            read = reader.read(&mut buffer[..read_capacity]) => match read {
                Ok(0) => {
                    enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    let sent = send_port_closed(
                        &events,
                        &mut cancel,
                        "serial port reached EOF".into(),
                        dropped_bytes,
                    )
                    .await;
                    return if sent {
                        ReaderTail::default()
                    } else {
                        ReaderTail {
                            pending,
                            dropped_bytes,
                        }
                    };
                }
                Ok(count) => {
                    pending.extend_from_slice(&buffer[..count]);
                    if pending.len() >= RX_BUFFER_BYTES {
                        enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    }
                    // The coalescing window is a hard maximum measured from
                    // the first pending byte. A continuous stream must not
                    // keep extending it until the 4 KiB buffer fills.
                    flush_deadline = arm_rx_flush_deadline(
                        flush_deadline,
                        tokio::time::Instant::now(),
                        !pending.is_empty() || dropped_bytes > 0,
                    );
                }
                Err(error) => {
                    enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    let sent = send_port_closed(
                        &events,
                        &mut cancel,
                        error.to_string(),
                        dropped_bytes,
                    )
                    .await;
                    return if sent {
                        ReaderTail::default()
                    } else {
                        ReaderTail {
                            pending,
                            dropped_bytes,
                        }
                    };
                }
            }
        }
    }
}

#[cfg(windows)]
fn run_windows_port_reader(
    mut reader: COMPort,
    events: mpsc::Sender<PortEvent>,
    mut commands: mpsc::Receiver<PortReaderCommand>,
    cancel: watch::Receiver<bool>,
    writer_failed: Arc<AtomicBool>,
) -> ReaderTail {
    let mut buffer = vec![0_u8; RX_BUFFER_BYTES];
    let mut pending = Vec::with_capacity(RX_BUFFER_BYTES);
    let mut dropped_bytes = 0_u64;
    let mut flush_deadline = None;

    loop {
        if write_cancelled(&cancel) || writer_failed.load(Ordering::Acquire) {
            return ReaderTail {
                pending,
                dropped_bytes,
            };
        }

        if pending.len() >= RX_BUFFER_BYTES {
            enqueue_rx(&events, &mut pending, &mut dropped_bytes);
            flush_deadline = None;
        }
        if flush_deadline.is_some_and(|deadline: Instant| Instant::now() >= deadline) {
            enqueue_rx(&events, &mut pending, &mut dropped_bytes);
            flush_deadline = None;
        }

        loop {
            match commands.try_recv() {
                Ok(PortReaderCommand::Barrier { id }) => {
                    // Include the bytes already queued by the Windows driver
                    // in the pre-barrier side. Without this snapshot drain, a
                    // quiet reader could emit the marker first and let stale
                    // boot output satisfy a newly-installed Trigger matcher.
                    if let Err(reason) = drain_windows_rx_snapshot(
                        &mut reader,
                        &events,
                        &mut buffer,
                        &mut pending,
                        &mut dropped_bytes,
                    ) {
                        return finish_windows_reader(
                            &events,
                            &cancel,
                            &mut pending,
                            dropped_bytes,
                            reason,
                        );
                    }
                    enqueue_rx(&events, &mut pending, &mut dropped_bytes);
                    if dropped_bytes > 0 {
                        let dropped = dropped_bytes;
                        if !send_windows_port_event(
                            &events,
                            PortEvent::Overflow {
                                dropped_bytes: dropped,
                            },
                            &cancel,
                        ) {
                            return ReaderTail {
                                pending,
                                dropped_bytes,
                            };
                        }
                        dropped_bytes = 0;
                    }
                    if !send_windows_port_event(&events, PortEvent::ReaderBarrier { id }, &cancel) {
                        return ReaderTail {
                            pending,
                            dropped_bytes,
                        };
                    }
                    flush_deadline = None;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let available = match reader.bytes_to_read() {
            Ok(available) => available as usize,
            Err(error) => {
                return finish_windows_reader(
                    &events,
                    &cancel,
                    &mut pending,
                    dropped_bytes,
                    error.to_string(),
                );
            }
        };
        if available == 0 {
            let sleep = flush_deadline
                .map(|deadline: Instant| deadline.saturating_duration_since(Instant::now()))
                .unwrap_or(WINDOWS_PORT_POLL_INTERVAL)
                .min(WINDOWS_PORT_POLL_INTERVAL);
            if !sleep.is_zero() {
                std::thread::sleep(sleep);
            }
            continue;
        }

        let capacity = rx_read_capacity(pending.len()).min(available);
        match reader.read(&mut buffer[..capacity]) {
            Ok(0) => {
                return finish_windows_reader(
                    &events,
                    &cancel,
                    &mut pending,
                    dropped_bytes,
                    "serial port reached EOF".into(),
                );
            }
            Ok(count) => {
                pending.extend_from_slice(&buffer[..count]);
                flush_deadline.get_or_insert_with(|| Instant::now() + RX_COALESCE_WINDOW);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) => {}
            Err(error) => {
                return finish_windows_reader(
                    &events,
                    &cancel,
                    &mut pending,
                    dropped_bytes,
                    error.to_string(),
                );
            }
        }
    }
}

#[cfg(windows)]
fn drain_windows_rx_snapshot(
    reader: &mut COMPort,
    events: &mpsc::Sender<PortEvent>,
    buffer: &mut [u8],
    pending: &mut Vec<u8>,
    dropped_bytes: &mut u64,
) -> Result<(), String> {
    let mut remaining = reader.bytes_to_read().map_err(|error| error.to_string())? as usize;
    while remaining > 0 {
        if pending.len() >= RX_BUFFER_BYTES {
            enqueue_rx(events, pending, dropped_bytes);
        }
        let capacity = rx_read_capacity(pending.len())
            .min(remaining)
            .min(buffer.len());
        match reader.read(&mut buffer[..capacity]) {
            Ok(0) => {
                return Err(format!(
                    "serial driver reported {remaining} queued byte(s) but returned zero while establishing the reader barrier"
                ));
            }
            Ok(count) => {
                pending.extend_from_slice(&buffer[..count]);
                remaining = remaining.saturating_sub(count);
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                return Err(format!(
                    "serial read timed out with {remaining} queued byte(s) while establishing the reader barrier"
                ));
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn finish_windows_reader(
    events: &mpsc::Sender<PortEvent>,
    cancel: &watch::Receiver<bool>,
    pending: &mut Vec<u8>,
    mut dropped_bytes: u64,
    reason: String,
) -> ReaderTail {
    enqueue_rx(events, pending, &mut dropped_bytes);
    if send_windows_port_event(
        events,
        PortEvent::Closed {
            reason,
            dropped_bytes,
        },
        cancel,
    ) {
        ReaderTail::default()
    } else {
        ReaderTail {
            pending: std::mem::take(pending),
            dropped_bytes,
        }
    }
}

#[cfg(windows)]
fn send_windows_port_event(
    events: &mpsc::Sender<PortEvent>,
    mut event: PortEvent,
    cancel: &watch::Receiver<bool>,
) -> bool {
    loop {
        if write_cancelled(cancel) {
            return false;
        }
        match events.try_send(event) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(returned)) => {
                event = returned;
                std::thread::sleep(WINDOWS_PORT_POLL_INTERVAL);
            }
        }
    }
}

fn rx_read_capacity(pending_len: usize) -> usize {
    RX_BUFFER_BYTES.saturating_sub(pending_len)
}

#[cfg(any(not(windows), test))]
fn arm_rx_flush_deadline(
    current: Option<tokio::time::Instant>,
    now: tokio::time::Instant,
    has_pending_work: bool,
) -> Option<tokio::time::Instant> {
    has_pending_work.then(|| current.unwrap_or(now + RX_COALESCE_WINDOW))
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
    let data = std::mem::replace(pending, Vec::with_capacity(RX_BUFFER_BYTES));
    let length = data.len() as u64;
    match events.try_send(PortEvent::Rx(data)) {
        Ok(()) => {}
        Err(mpsc::error::TrySendError::Full(_)) => {
            *dropped_bytes = dropped_bytes.saturating_add(length);
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {}
    }
}

#[cfg(not(windows))]
async fn run_port_writer(
    mut writer: tokio::io::WriteHalf<SharedSerialStream>,
    break_control: SharedSerialStream,
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
        let Some(command) = command else {
            return;
        };
        let (failed, cancelled, message) = match command {
            PortCommand::Write {
                data,
                pacing,
                deadline,
                reply,
            } => {
                let outcome =
                    write_with_pacing(&mut writer, &data, pacing, deadline, &mut cancel).await;
                let state = (
                    outcome.error.is_some(),
                    outcome.cancelled,
                    outcome.error.clone(),
                );
                let _ = reply.send(outcome);
                state
            }
            PortCommand::Break { duration, reply } => {
                let outcome = send_break_async(&break_control, duration, &mut cancel).await;
                let result = outcome.error.clone().map_or(Ok(()), Err);
                let state = (
                    break_failure_closes_port(outcome.error.as_ref()),
                    outcome.cancelled,
                    outcome.error.as_ref().map(ToString::to_string),
                );
                let _ = reply.send(result);
                state
            }
        };
        if cancelled {
            return;
        }
        if failed {
            send_port_closed(
                &events,
                &mut cancel,
                message.unwrap_or_else(|| "serial write failed".into()),
                0,
            )
            .await;
            return;
        }
    }
}

#[cfg(not(windows))]
async fn send_break_async(
    port: &SharedSerialStream,
    duration: Duration,
    cancel: &mut watch::Receiver<bool>,
) -> PortBreakOutcome {
    if let Err(error) = port.set_break() {
        return PortBreakOutcome {
            error: Some(classify_break_failure("failed to assert BREAK", error)),
            cancelled: false,
        };
    }
    let mut cancelled = false;
    tokio::select! {
        changed = cancel.changed() => {
            cancelled = changed.is_err() || *cancel.borrow();
        }
        _ = tokio::time::sleep(duration) => {}
    }
    let clear = port
        .clear_break()
        .map_err(|error| classify_break_failure("failed to clear BREAK", error));
    let error = match (cancelled, clear.err()) {
        (false, None) => None,
        (false, Some(error)) => Some(error),
        (true, None) => Some(PortBreakFailure::Failed(
            "BREAK was cancelled because the port is closing".into(),
        )),
        (true, Some(error)) => Some(PortBreakFailure::Failed(format!(
            "BREAK was cancelled because the port is closing; {error}"
        ))),
    };
    PortBreakOutcome { error, cancelled }
}

#[cfg(windows)]
trait BlockingBreakSignal {
    fn assert_break(&self) -> Result<(), String>;
    fn clear_break_signal(&self) -> Result<(), String>;
}

#[cfg(windows)]
impl BlockingBreakSignal for COMPort {
    fn assert_break(&self) -> Result<(), String> {
        SerialPort::set_break(self).map_err(|error| error.to_string())
    }

    fn clear_break_signal(&self) -> Result<(), String> {
        SerialPort::clear_break(self).map_err(|error| error.to_string())
    }
}

#[cfg(windows)]
fn send_break_blocking<W>(
    port: &W,
    duration: Duration,
    cancel: &watch::Receiver<bool>,
) -> PortBreakOutcome
where
    W: BlockingBreakSignal,
{
    if let Err(error) = port.assert_break() {
        return PortBreakOutcome {
            error: Some(classify_break_failure("failed to assert BREAK", error)),
            cancelled: false,
        };
    }
    let deadline = Instant::now() + duration;
    let mut cancelled = false;
    while Instant::now() < deadline {
        if write_cancelled(cancel) {
            cancelled = true;
            break;
        }
        std::thread::sleep(
            deadline
                .saturating_duration_since(Instant::now())
                .min(WINDOWS_PORT_POLL_INTERVAL),
        );
    }
    let clear = port
        .clear_break_signal()
        .map_err(|error| classify_break_failure("failed to clear BREAK", error));
    let error = match (cancelled, clear.err()) {
        (false, None) => None,
        (false, Some(error)) => Some(error),
        (true, None) => Some(PortBreakFailure::Failed(
            "BREAK was cancelled because the port is closing".into(),
        )),
        (true, Some(error)) => Some(PortBreakFailure::Failed(format!(
            "BREAK was cancelled because the port is closing; {error}"
        ))),
    };
    PortBreakOutcome { error, cancelled }
}

#[cfg(windows)]
fn run_windows_port_writer<W>(
    mut writer: W,
    mut command_rx: mpsc::Receiver<PortCommand>,
    events: mpsc::Sender<PortEvent>,
    cancel: watch::Receiver<bool>,
    writer_failed: Arc<AtomicBool>,
) where
    W: std::io::Write + BlockingBreakSignal,
{
    loop {
        if write_cancelled(&cancel) {
            return;
        }
        let Some(command) = command_rx.blocking_recv() else {
            return;
        };
        let (failed, cancelled, message) = match command {
            PortCommand::Write {
                data,
                pacing,
                deadline,
                reply,
            } => {
                let outcome = write_with_blocking_pacing(
                    &mut writer,
                    &data,
                    pacing,
                    deadline.into_std(),
                    &cancel,
                );
                let state = (
                    outcome.error.is_some(),
                    outcome.cancelled,
                    outcome.error.clone(),
                );
                let _ = reply.send(outcome);
                state
            }
            PortCommand::Break { duration, reply } => {
                let outcome = send_break_blocking(&writer, duration, &cancel);
                let result = outcome.error.clone().map_or(Ok(()), Err);
                let state = (
                    break_failure_closes_port(outcome.error.as_ref()),
                    outcome.cancelled,
                    outcome.error.as_ref().map(ToString::to_string),
                );
                let _ = reply.send(result);
                state
            }
        };
        if failed && !cancelled {
            // Stop the RX producer before reserving space for the authoritative
            // close event. Under sustained RX this prevents a try-send loop
            // from being starved forever by the reader refilling every slot.
            writer_failed.store(true, Ordering::Release);
        }
        if cancelled {
            return;
        }
        if failed {
            let _ = send_windows_port_event(
                &events,
                PortEvent::Closed {
                    reason: message.unwrap_or_else(|| "serial write failed".into()),
                    dropped_bytes: 0,
                },
                &cancel,
            );
            return;
        }
    }
}

#[cfg(windows)]
fn write_with_blocking_pacing<W>(
    writer: &mut W,
    data: &[u8],
    pacing: WritePacing,
    deadline: Instant,
    cancel: &watch::Receiver<bool>,
) -> PortWriteOutcome
where
    W: std::io::Write,
{
    let chunk_size = (pacing.chunk_size as usize).max(1);
    let chunk_delay = Duration::from_millis(pacing.chunk_delay_ms);
    let mut written = 0;
    let mut error = None;
    let mut cancelled = false;

    'chunks: while written < data.len() {
        let chunk_end = written.saturating_add(chunk_size).min(data.len());
        while written < chunk_end {
            if write_cancelled(cancel) {
                error = Some("serial write cancelled because the port is closing".into());
                cancelled = true;
                break 'chunks;
            }
            if Instant::now() >= deadline {
                error = Some("serial write timed out; port state is uncertain".into());
                break 'chunks;
            }
            match writer.write(&data[written..chunk_end]) {
                Ok(0) => {
                    error = Some("serial driver accepted zero bytes".into());
                    break 'chunks;
                }
                Ok(count) => written += count,
                Err(write_error) => {
                    error = Some(write_error.to_string());
                    break 'chunks;
                }
            }
        }
        if written >= data.len() || chunk_delay.is_zero() {
            continue;
        }
        match wait_windows_pacing(chunk_delay, deadline, cancel) {
            WindowsPacingWait::Elapsed => {}
            WindowsPacingWait::Cancelled => {
                error = Some("serial write cancelled because the port is closing".into());
                cancelled = true;
                break;
            }
            WindowsPacingWait::TimedOut => {
                error = Some("serial write timed out; port state is uncertain".into());
                break;
            }
        }
    }

    PortWriteOutcome {
        written,
        error,
        cancelled,
    }
}

#[cfg(windows)]
enum WindowsPacingWait {
    Elapsed,
    Cancelled,
    TimedOut,
}

#[cfg(windows)]
fn wait_windows_pacing(
    delay: Duration,
    deadline: Instant,
    cancel: &watch::Receiver<bool>,
) -> WindowsPacingWait {
    let delay_deadline = Instant::now() + delay;
    loop {
        if write_cancelled(cancel) {
            return WindowsPacingWait::Cancelled;
        }
        let now = Instant::now();
        if now >= deadline {
            return WindowsPacingWait::TimedOut;
        }
        if now >= delay_deadline {
            return WindowsPacingWait::Elapsed;
        }
        std::thread::sleep(
            delay_deadline
                .saturating_duration_since(now)
                .min(deadline.saturating_duration_since(now))
                .min(WINDOWS_PORT_POLL_INTERVAL),
        );
    }
}

/// Writes `data` to the driver in `pacing.chunk_size` byte chunks, sleeping
/// `pacing.chunk_delay_ms` between chunks (never after the final chunk) so a
/// slow target UART is not overrun. A zero chunk delay keeps the original
/// full-speed path with no sleeps. A chunk size of zero is treated as one
/// byte. The hard request deadline includes configured pacing plus a
/// per-chunk scheduler/driver allowance, while each individual driver write
/// still has the fixed no-progress timeout. The caller must reject a pacing
/// plan that exceeds [`MAX_WRITE_TIMEOUT`] before enqueueing it and supplies
/// the resulting absolute deadline here.
#[cfg(any(not(windows), test))]
async fn write_with_pacing<W>(
    writer: &mut W,
    data: &[u8],
    pacing: WritePacing,
    deadline: tokio::time::Instant,
    cancel: &mut watch::Receiver<bool>,
) -> PortWriteOutcome
where
    W: AsyncWriteExt + Unpin,
{
    let chunk_size = (pacing.chunk_size as usize).max(1);
    let chunk_delay = Duration::from_millis(pacing.chunk_delay_ms);
    let mut written = 0;
    let mut error = None;
    let mut cancelled = false;
    'chunks: while written < data.len() {
        let chunk_end = written.saturating_add(chunk_size).min(data.len());
        // One chunk can still need several driver calls when a write is
        // accepted only partially.
        while written < chunk_end {
            if write_cancelled(cancel) {
                error = Some("serial write cancelled because the port is closing".into());
                cancelled = true;
                break 'chunks;
            }
            if tokio::time::Instant::now() >= deadline {
                error = Some("serial write timed out; port state is uncertain".into());
                break 'chunks;
            }
            let no_progress_deadline = tokio::time::Instant::now() + WRITE_TIMEOUT;
            tokio::select! {
                biased;
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
                _ = tokio::time::sleep_until(no_progress_deadline) => {
                    error = Some("serial driver write timed out after making no progress; port state is uncertain".into());
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
        if write_cancelled(cancel) {
            error = Some("serial write cancelled because the port is closing".into());
            cancelled = true;
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            error = Some("serial write timed out; port state is uncertain".into());
            break;
        }
        tokio::select! {
            biased;
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

fn write_cancelled(cancel: &watch::Receiver<bool>) -> bool {
    *cancel.borrow() || cancel.has_changed().is_err()
}

/// Deadline budget for one paced write.
///
/// The fixed timeout covers the first chunk. Every later planned chunk adds
/// twice its configured pacing delay plus a conservative allowance for timer
/// quantization and asynchronous driver scheduling. Windows commonly turns a
/// nominal 1 ms typewriter cadence into roughly 15 ms per byte, so counting
/// only the requested sleeps is not a safe estimate. An over-budget plan is
/// rejected in full before any bytes are written; it is never clamped into a
/// deadline that guarantees a partial write. The per-driver-call no-progress
/// timeout in [`write_with_pacing`] remains a separate, shorter bound.
fn write_deadline(
    total_bytes: usize,
    chunk_size: usize,
    chunk_delay: Duration,
) -> Result<Duration, u64> {
    if chunk_delay.is_zero() {
        return Ok(WRITE_TIMEOUT);
    }
    let chunk_count = total_bytes.div_ceil(chunk_size.max(1)) as u128;
    let additional_chunks = chunk_count.saturating_sub(1);
    let pacing_millis = additional_chunks.saturating_mul(chunk_delay.as_millis());
    let scheduling_millis =
        additional_chunks.saturating_mul(WRITE_CHUNK_OVERHEAD_ALLOWANCE.as_millis());
    let budget_millis = WRITE_TIMEOUT
        .as_millis()
        .saturating_add(pacing_millis.saturating_mul(2))
        .saturating_add(scheduling_millis);
    let budget_millis = budget_millis.min(u64::MAX as u128) as u64;
    if budget_millis > duration_millis_saturating(MAX_WRITE_TIMEOUT) {
        return Err(budget_millis);
    }
    Ok(Duration::from_millis(budget_millis))
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn ensure_lease_covers_write(
    lease_remaining: Duration,
    write_timeout: Duration,
) -> Result<(), SlotError> {
    let required = write_timeout.saturating_add(WRITE_LEASE_SAFETY_MARGIN);
    if lease_remaining < required {
        return Err(SlotError::WriteLeaseTooShort {
            remaining_ms: duration_millis_saturating(lease_remaining),
            write_ms: duration_millis_saturating(write_timeout),
            margin_ms: duration_millis_saturating(WRITE_LEASE_SAFETY_MARGIN),
        });
    }
    Ok(())
}

#[cfg(any(not(windows), test))]
async fn send_port_closed(
    events: &mpsc::Sender<PortEvent>,
    cancel: &mut watch::Receiver<bool>,
    reason: String,
    dropped_bytes: u64,
) -> bool {
    tokio::select! {
        _ = cancel.changed() => false,
        result = events.send(PortEvent::Closed { reason, dropped_bytes }) => result.is_ok(),
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
    fn serial_open_failures_have_stable_diagnostic_codes() {
        let missing = PortOpenError::from_serial(serialport::Error::new(
            serialport::ErrorKind::NoDevice,
            "missing",
        ));
        assert_eq!(missing.code, ErrorCode::PortNotFound);

        let busy = PortOpenError::from_serial(serialport::Error::new(
            serialport::ErrorKind::Io(std::io::ErrorKind::WouldBlock),
            "temporarily unavailable",
        ));
        assert_eq!(busy.code, ErrorCode::PortBusy);

        let denied = PortOpenError::from_serial(serialport::Error::new(
            serialport::ErrorKind::Io(std::io::ErrorKind::PermissionDenied),
            "permission denied",
        ));
        #[cfg(windows)]
        assert_eq!(denied.code, ErrorCode::PortBusy);
        #[cfg(not(windows))]
        assert_eq!(denied.code, ErrorCode::PortAccessDenied);
    }

    fn trigger_spec(action: &[u8]) -> TriggerSpec {
        TriggerSpec {
            initial_write: None,
            start_contains: None,
            action: action.to_vec(),
            interval_ms: 20,
            stop_contains: vec![b"ready>".to_vec()],
            timeout_ms: 5_000,
            max_fires: 250,
            pacing: None,
        }
    }

    fn active_trigger_for_test(spec: TriggerSpec, now: Instant) -> ActiveTrigger {
        let stop_matcher = LiteralMatcher::new(spec.stop_contains.clone());
        ActiveTrigger {
            info: TriggerInfo {
                id: Uuid::new_v4(),
                owner: Actor {
                    id: "agent:test".into(),
                    label: "test".into(),
                    kind: ActorKind::Agent,
                },
                daemon_epoch: Uuid::new_v4(),
                generation: 1,
                control_id: Uuid::new_v4(),
                fence: 1,
                operation_id: None,
                expected_run_id: None,
                spec,
                status: TriggerStatus::Running,
                start_seq: 1,
                end_seq: None,
                last_write_seq: None,
                fires_confirmed: 0,
                tx_bytes_confirmed: 0,
                matched_pattern: None,
            },
            bound_run_id: None,
            deadline: now + Duration::from_secs(5),
            next_write_at: Some(now),
            initial_pending: false,
            start_seen: true,
            start_matcher: None,
            stop_matcher,
            write_in_flight: None,
            buffered_rx: TriggerRxAuditBuffer::default(),
            pending_terminal: None,
        }
    }

    #[test]
    fn trigger_observes_a_late_stop_match_after_reaching_one_fire() {
        let now = Instant::now();
        let mut spec = trigger_spec(b"slp\r");
        spec.max_fires = 1;
        spec.stop_contains = vec![b"SigmaStar #".to_vec()];
        let mut trigger = active_trigger_for_test(spec, now);

        assert_eq!(trigger.confirm_action_write(now), None);
        assert_eq!(trigger.info.fires_confirmed, 1);
        assert_eq!(trigger.next_write_at, None);
        assert_eq!(trigger.deadline_status(), TriggerStatus::MaxFiresReached);

        // The prompt can be emitted only after the final write completes and
        // can cross serial read boundaries. It must still prove completion.
        assert_eq!(trigger.observe_rx(b"SigmaStar "), None);
        assert_eq!(trigger.observe_rx(b"#"), Some(b"SigmaStar #".to_vec()));
    }

    #[test]
    fn trigger_without_a_stop_matcher_finishes_at_the_fire_limit() {
        let now = Instant::now();
        let mut spec = trigger_spec(b"x");
        spec.max_fires = 1;
        spec.stop_contains.clear();
        let mut trigger = active_trigger_for_test(spec, now);

        assert_eq!(
            trigger.confirm_action_write(now),
            Some(TriggerStatus::MaxFiresReached)
        );
        assert_eq!(trigger.info.fires_confirmed, 1);
        assert_eq!(trigger.next_write_at, None);
    }

    #[test]
    fn kickoff_without_an_explicit_start_gate_immediately_enables_actions() {
        let now = Instant::now();
        let mut spec = trigger_spec(b"action");
        spec.initial_write = Some(b"kickoff\r".to_vec());
        spec.start_contains = None;
        let mut trigger = active_trigger_for_test(spec, now);
        trigger.info.status = TriggerStatus::Armed;
        trigger.initial_pending = true;
        trigger.start_seen = true;
        trigger.next_write_at = Some(now);

        let after_kickoff = now + Duration::from_millis(1);
        trigger.confirm_initial_write(after_kickoff);

        assert!(!trigger.initial_pending);
        assert_eq!(trigger.info.status, TriggerStatus::Running);
        assert_eq!(trigger.next_write_at, Some(after_kickoff));
        assert!(trigger.start_matcher.is_none());
    }

    #[test]
    fn kickoff_with_an_explicit_start_gate_waits_for_live_rx() {
        let now = Instant::now();
        let mut spec = trigger_spec(b"action");
        spec.initial_write = Some(b"kickoff\r".to_vec());
        spec.start_contains = Some(b"go>".to_vec());
        let mut trigger = active_trigger_for_test(spec.clone(), now);
        trigger.info.status = TriggerStatus::Armed;
        trigger.initial_pending = true;
        trigger.start_seen = false;
        trigger.start_matcher = spec
            .start_contains
            .map(|pattern| LiteralMatcher::new(vec![pattern]));
        trigger.next_write_at = Some(now);

        trigger.confirm_initial_write(now + Duration::from_millis(1));
        assert_eq!(trigger.info.status, TriggerStatus::WaitingForStart);
        assert_eq!(trigger.next_write_at, None);

        assert_eq!(trigger.observe_rx(b"g"), None);
        assert_eq!(trigger.info.status, TriggerStatus::WaitingForStart);
        assert_eq!(trigger.observe_rx(b"o>"), None);
        assert!(trigger.start_seen);
        assert_eq!(trigger.info.status, TriggerStatus::Running);
        assert!(trigger.next_write_at.is_some());
        assert!(trigger.start_matcher.is_none());
    }

    #[test]
    fn trigger_deadline_distinguishes_timeout_from_an_exhausted_fire_budget() {
        let now = Instant::now();
        let mut spec = trigger_spec(b"x");
        spec.max_fires = 2;
        let mut trigger = active_trigger_for_test(spec, now);

        assert_eq!(trigger.deadline_status(), TriggerStatus::TimedOut);
        assert_eq!(trigger.confirm_action_write(now), None);
        assert_eq!(trigger.deadline_status(), TriggerStatus::TimedOut);
        assert_eq!(trigger.confirm_action_write(now), None);
        assert_eq!(trigger.deadline_status(), TriggerStatus::MaxFiresReached);
    }

    #[test]
    fn literal_matcher_matches_across_real_chunks_without_self_replaying_one_chunk() {
        let mut single_observation = LiteralMatcher::new(vec![b"abcabc".to_vec()]);
        assert_eq!(single_observation.push(b"abc"), None);

        // Buffered RX is matched when it first reaches the actor. Flushing
        // that exact chunk after TX audit must only emit it, never feed it to
        // the matcher a second time: doing so would create this false match.
        assert_eq!(single_observation.tail, b"abc");

        let mut real_two_chunk_stream = LiteralMatcher::new(vec![b"abcabc".to_vec()]);
        assert_eq!(real_two_chunk_stream.push(b"abc"), None);
        assert_eq!(real_two_chunk_stream.push(b"abc"), Some(b"abcabc".to_vec()));
    }

    #[test]
    fn trigger_spec_is_device_agnostic_and_strictly_bounded() {
        let mut valid = trigger_spec(&[0x00, 0xff, b'x']);
        valid.initial_write = Some(b"reset\r".to_vec());
        valid.start_contains = Some(vec![0x80, b'B']);
        valid.stop_contains = vec![vec![0x00, 0xfe], b"arbitrary prompt".to_vec()];
        assert_eq!(validate_trigger_spec(&valid), Ok(()));

        let mut too_many_bytes = trigger_spec(&vec![0; MAX_TRIGGER_ACTION_BYTES]);
        too_many_bytes.max_fires = 257;
        assert_eq!(
            validate_trigger_spec(&too_many_bytes),
            Err(SlotError::TriggerTotalBytesTooLarge)
        );

        let mut empty_pattern = trigger_spec(b"x");
        empty_pattern.stop_contains = vec![Vec::new()];
        assert_eq!(
            validate_trigger_spec(&empty_pattern),
            Err(SlotError::InvalidTriggerPatterns)
        );
    }

    #[test]
    fn break_request_is_bounded_and_idempotency_fingerprint_covers_duration() {
        let actor = Actor {
            id: "agent:test".into(),
            label: "test".into(),
            kind: ActorKind::Agent,
        };
        let request = |duration_ms| SlotRequest::SendBreak {
            actor: actor.clone(),
            control_id: Uuid::new_v4(),
            fence: 7,
            duration_ms,
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(Uuid::new_v4()),
            sequence_precondition: None,
        };
        assert_eq!(
            request(MIN_BREAK_DURATION_MS).validate_business_fields(),
            Ok(())
        );
        assert_eq!(
            request(MAX_BREAK_DURATION_MS).validate_business_fields(),
            Ok(())
        );
        assert_eq!(
            request(0).validate_business_fields(),
            Err(SlotError::InvalidBreakDuration)
        );

        let operation_id = Some(Uuid::new_v4());
        let expected_run_id = Some(Uuid::new_v4());
        let make = |duration_ms| SlotRequest::SendBreak {
            actor: actor.clone(),
            control_id: Uuid::new_v4(),
            fence: 99,
            duration_ms,
            operation_id,
            expected_run_id,
            sequence_precondition: None,
        };
        assert_ne!(make(100).write_fingerprint(), make(200).write_fingerprint());
        let unguarded = make(100);
        let mut guarded = make(100);
        if let SlotRequest::SendBreak {
            sequence_precondition,
            ..
        } = &mut guarded
        {
            *sequence_precondition = Some(SequenceWritePrecondition {
                cursor: Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 5,
                },
                expected_generation: 1,
                expected_tx_offset: 3,
            });
        }
        assert_ne!(unguarded.write_fingerprint(), guarded.write_fingerprint());
        assert!(matches!(
            classify_break_failure("assert BREAK", "operation not supported"),
            PortBreakFailure::Unsupported(_)
        ));
        assert!(!break_failure_closes_port(Some(
            &PortBreakFailure::Unsupported("unsupported".into())
        )));
        assert!(break_failure_closes_port(Some(&PortBreakFailure::Failed(
            "uncertain".into()
        ))));
    }

    #[test]
    fn device_profile_changes_require_idle_without_a_transport_reopen() {
        assert!(profile_change_requires_idle(false, true, true, false));
        assert!(profile_change_requires_idle(false, true, false, true));
        assert!(!profile_change_requires_idle(false, false, true, true));
        assert!(!profile_change_requires_idle(true, true, true, true));
    }

    #[test]
    fn trigger_due_predicate_excludes_the_deadline_and_blocked_states() {
        let now = Instant::now();
        let deadline = now + Duration::from_millis(20);
        assert!(trigger_write_due_at(now, deadline, Some(now), false, false));
        assert!(!trigger_write_due_at(
            deadline,
            deadline,
            Some(now),
            false,
            false
        ));
        assert!(!trigger_write_due_at(now, deadline, Some(now), true, false));
        assert!(!trigger_write_due_at(now, deadline, Some(now), false, true));
    }

    #[test]
    fn trigger_rx_audit_buffer_keeps_a_gap_at_the_dropped_suffix() {
        let mut buffer = TriggerRxAuditBuffer::default();
        assert!(!buffer.push_rx_with_limit(vec![1, 2], 3));
        assert!(buffer.push_rx_with_limit(vec![3, 4], 3));
        // Once a suffix has been dropped, later smaller chunks cannot jump
        // ahead of the eventual Gap event.
        assert!(buffer.push_rx_with_limit(vec![5], 3));

        let (events, dropped_bytes) = buffer.take();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events.front(),
            Some(PortEvent::Rx(data)) if data == &[1, 2]
        ));
        assert_eq!(dropped_bytes, 3);
        assert_eq!(buffer.bytes, 0);
        assert_eq!(buffer.dropped_bytes, 0);
    }

    #[test]
    fn uncertain_trigger_failures_override_success_and_timer_bounds() {
        assert!(
            trigger_terminal_priority(TriggerStatus::RxGap)
                > trigger_terminal_priority(TriggerStatus::Matched)
        );
        assert!(
            trigger_terminal_priority(TriggerStatus::WriteFailed)
                > trigger_terminal_priority(TriggerStatus::Matched)
        );
        assert!(
            trigger_terminal_priority(TriggerStatus::Matched)
                > trigger_terminal_priority(TriggerStatus::TimedOut)
        );
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
            expected_run_id: None,
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        let same = SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"first".to_vec(),
            operation_id: None,
            expected_run_id: None,
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        let different = SlotRequest::Write {
            actor,
            control_id,
            fence: 7,
            data: b"second".to_vec(),
            operation_id: None,
            expected_run_id: None,
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
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
            expected_run_id: None,
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        let paced = SlotRequest::Write {
            actor,
            control_id,
            fence: 7,
            data: b"reboot\r".to_vec(),
            operation_id: None,
            expected_run_id: None,
            pacing: Some(WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 5,
            }),
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        assert_ne!(unpaced.write_fingerprint(), paced.write_fingerprint());
    }

    #[test]
    fn command_description_is_validated_and_bound_to_write_idempotency() {
        let actor = Actor {
            id: "agent:test".into(),
            label: "test".into(),
            kind: ActorKind::Agent,
        };
        let operation_id = Some(Uuid::new_v4());
        let control_id = Uuid::new_v4();
        let request = |description: Option<&str>| SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"cat /proc/meminfo\r".to_vec(),
            operation_id,
            expected_run_id: None,
            pacing: None,
            description: description.map(str::to_string),
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };

        assert!(request(None).validate_business_fields().is_ok());
        assert!(
            request(Some("查看样机内存"))
                .validate_business_fields()
                .is_ok()
        );
        for invalid in ["", " 前导空格", "尾随空格 ", "包含\n换行"] {
            assert_eq!(
                request(Some(invalid)).validate_business_fields(),
                Err(SlotError::InvalidCommandDescription)
            );
        }
        let oversized = "界".repeat(MAX_COMMAND_DESCRIPTION_BYTES / "界".len() + 1);
        assert_eq!(
            request(Some(&oversized)).validate_business_fields(),
            Err(SlotError::InvalidCommandDescription)
        );

        assert_ne!(
            request(Some("查看样机内存")).write_fingerprint(),
            request(Some("查看样机负载")).write_fingerprint()
        );
        assert_ne!(
            request(Some("查看样机内存")).fingerprint(),
            request(Some("查看样机负载")).fingerprint()
        );
    }

    #[test]
    fn command_sequence_audit_is_validated_and_bound_to_write_idempotency() {
        let sequence_id = Uuid::new_v4();
        let audit = CommandSequenceAuditContext {
            sequence_id,
            description: "登录样机".into(),
            step_index: 0,
            step_count: 2,
        };
        let control_id = Uuid::new_v4();
        let operation_id = Some(Uuid::new_v4());
        let request = |command_sequence| SlotRequest::Write {
            actor: Actor {
                id: "agent:test".into(),
                label: "test".into(),
                kind: ActorKind::Agent,
            },
            control_id,
            fence: 7,
            data: b"admin\r".to_vec(),
            operation_id,
            expected_run_id: None,
            pacing: None,
            description: Some("输入账号".into()),
            command_sequence,
            sequence_precondition: None,
            cooperative: false,
        };
        assert!(
            request(Some(audit.clone()))
                .validate_business_fields()
                .is_ok()
        );
        let mut missing_step_description = request(Some(audit.clone()));
        if let SlotRequest::Write { description, .. } = &mut missing_step_description {
            *description = None;
        }
        assert_eq!(
            missing_step_description.validate_business_fields(),
            Err(SlotError::InvalidCommandSequenceAudit)
        );
        for invalid in [
            CommandSequenceAuditContext {
                sequence_id: Uuid::nil(),
                ..audit.clone()
            },
            CommandSequenceAuditContext {
                step_count: 0,
                ..audit.clone()
            },
            CommandSequenceAuditContext {
                step_index: 2,
                ..audit.clone()
            },
            CommandSequenceAuditContext {
                description: " 未修剪".into(),
                ..audit.clone()
            },
        ] {
            assert_eq!(
                request(Some(invalid)).validate_business_fields(),
                Err(SlotError::InvalidCommandSequenceAudit)
            );
        }
        let first = request(Some(audit.clone()));
        let precondition = SequenceWritePrecondition {
            cursor: Cursor {
                epoch: Uuid::new_v4(),
                after_seq: 41,
            },
            expected_generation: 3,
            expected_tx_offset: 17,
        };
        let mut guarded = request(Some(audit.clone()));
        if let SlotRequest::Write {
            sequence_precondition,
            ..
        } = &mut guarded
        {
            *sequence_precondition = Some(precondition.clone());
        }
        assert!(guarded.validate_business_fields().is_ok());
        assert_ne!(first.write_fingerprint(), guarded.write_fingerprint());
        assert_ne!(first.fingerprint(), guarded.fingerprint());

        let mut missing_audit = request(None);
        if let SlotRequest::Write {
            sequence_precondition,
            ..
        } = &mut missing_audit
        {
            *sequence_precondition = Some(precondition);
        }
        assert!(missing_audit.validate_business_fields().is_ok());

        let different_sequence = request(Some(CommandSequenceAuditContext {
            sequence_id: Uuid::new_v4(),
            ..audit
        }));
        assert_ne!(
            first.write_fingerprint(),
            different_sequence.write_fingerprint()
        );
    }

    fn sequence_boundary_event(
        epoch: Uuid,
        seq: u64,
        kind: EventKind,
        direction: Direction,
        data: &[u8],
        actor: Option<Actor>,
    ) -> TimelineEvent {
        TimelineEvent {
            slot_id: "bench".into(),
            daemon_epoch: epoch,
            seq,
            generation: 3,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind,
            direction,
            actor,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(0),
            stream_offset_end: Some(data.len() as u64),
            data: data.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    #[test]
    fn sequence_boundary_allows_pure_rx_but_rejects_gap_or_eviction() {
        let epoch = Uuid::new_v4();
        let precondition = SequenceWritePrecondition {
            cursor: Cursor {
                epoch,
                after_seq: 0,
            },
            expected_generation: 3,
            expected_tx_offset: 11,
        };
        let mut rx_ring = EventRing::new(8, 64 * 1024);
        rx_ring.push(sequence_boundary_event(
            epoch,
            1,
            EventKind::Rx,
            Direction::Rx,
            b"late output",
            None,
        ));
        assert!(
            validate_sequence_write_precondition(&precondition, epoch, 3, 11, 1, &rx_ring).is_ok(),
            "RX after the cursor is permitted and cannot itself interleave TX"
        );

        let mut gap_ring = EventRing::new(8, 64 * 1024);
        gap_ring.push(sequence_boundary_event(
            epoch,
            1,
            EventKind::Gap,
            Direction::Rx,
            &[],
            None,
        ));
        assert!(matches!(
            validate_sequence_write_precondition(&precondition, epoch, 3, 11, 1, &gap_ring),
            Err(SlotError::SequenceBoundaryChanged { .. })
        ));

        let mut evicted_ring = EventRing::new(1, 64 * 1024);
        evicted_ring.push(sequence_boundary_event(
            epoch,
            1,
            EventKind::Rx,
            Direction::Rx,
            b"old",
            None,
        ));
        evicted_ring.push(sequence_boundary_event(
            epoch,
            2,
            EventKind::Rx,
            Direction::Rx,
            b"new",
            None,
        ));
        assert!(matches!(
            validate_sequence_write_precondition(&precondition, epoch, 3, 11, 2, &evicted_ring),
            Err(SlotError::SequenceBoundaryChanged { .. })
        ));

        assert!(matches!(
            validate_sequence_write_precondition(&precondition, epoch, 4, 11, 1, &rx_ring),
            Err(SlotError::SequenceBoundaryChanged { .. })
        ));
    }

    #[test]
    fn foreign_or_cooperative_tx_rejects_next_step_before_zero_byte_enqueue() {
        let epoch = Uuid::new_v4();
        let precondition = SequenceWritePrecondition {
            cursor: Cursor {
                epoch,
                after_seq: 0,
            },
            expected_generation: 3,
            expected_tx_offset: 11,
        };
        let human = Actor {
            id: "human:operator".into(),
            label: "operator".into(),
            kind: ActorKind::Human,
        };
        let mut ring = EventRing::new(8, 64 * 1024);
        let mut cooperative = sequence_boundary_event(
            epoch,
            1,
            EventKind::Tx,
            Direction::Tx,
            b"status\r",
            Some(human),
        );
        cooperative
            .metadata
            .insert("cooperative".into(), json!(true));
        ring.push(cooperative);

        let mut physical_write = Vec::new();
        let authorized =
            validate_sequence_write_precondition(&precondition, epoch, 3, 18, 1, &ring);
        if authorized.is_ok() {
            physical_write.extend_from_slice(b"password\r");
        }
        let error = authorized.unwrap_err();
        assert!(matches!(&error, SlotError::SequenceBoundaryChanged { .. }));
        assert!(error.to_string().contains("no bytes were written"));
        assert!(
            physical_write.is_empty(),
            "the next step must enqueue 0 bytes"
        );

        // The event replay check independently rejects a TX even if an
        // inconsistent caller claims that the offset did not move.
        assert!(matches!(
            validate_sequence_write_precondition(&precondition, epoch, 3, 11, 1, &ring),
            Err(SlotError::SequenceBoundaryChanged { .. })
        ));
    }

    #[test]
    fn confirmed_tx_metadata_retains_command_and_sequence_descriptions() {
        let described = write_event_metadata(false, false, Some("查看样机内存".into()), None);
        assert_eq!(described["command_description"], json!("查看样机内存"));
        assert_eq!(described["partial"], json!(false));
        assert_eq!(described["cooperative"], json!(false));

        let legacy = write_event_metadata(true, false, None, None);
        assert!(!legacy.contains_key("command_description"));
        assert_eq!(legacy["partial"], json!(true));

        let sequence_id = Uuid::new_v4();
        let grouped = write_event_metadata(
            false,
            false,
            Some("输入账号".into()),
            Some(CommandSequenceAuditContext {
                sequence_id,
                description: "登录样机".into(),
                step_index: 0,
                step_count: 2,
            }),
        );
        assert_eq!(grouped["command_description"], json!("输入账号"));
        assert_eq!(grouped["command_sequence_id"], json!(sequence_id));
        assert_eq!(grouped["command_sequence_description"], json!("登录样机"));
        assert_eq!(grouped["command_sequence_step_index"], json!(0));
        assert_eq!(grouped["command_sequence_step_count"], json!(2));
    }

    #[test]
    fn agent_write_fingerprint_and_authorization_bind_the_expected_run() {
        let actor = Actor {
            id: "agent:test".into(),
            label: "test".into(),
            kind: ActorKind::Agent,
        };
        let control_id = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let other_run_id = Uuid::new_v4();
        let request = |expected_run_id| SlotRequest::Write {
            actor: actor.clone(),
            control_id,
            fence: 7,
            data: b"version\r".to_vec(),
            operation_id: None,
            expected_run_id,
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        assert_ne!(
            request(Some(run_id)).write_fingerprint(),
            request(Some(other_run_id)).write_fingerprint()
        );
        assert_ne!(
            request(None).write_fingerprint(),
            request(Some(run_id)).write_fingerprint()
        );

        let active_run = RunInfo {
            id: run_id,
            owner: actor.clone(),
            label: "task".into(),
            status: RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        };
        assert!(validate_expected_write_run(None, &actor, None).is_ok());
        assert!(validate_expected_write_run(Some(run_id), &actor, Some(&active_run)).is_ok());

        let missing = validate_expected_write_run(Some(run_id), &actor, None).unwrap_err();
        assert!(missing.to_string().contains("(no bytes were written)"));
        assert!(matches!(
            missing,
            SlotError::WriteRunMissing {
                expected_run_id
            } if expected_run_id == run_id
        ));

        assert!(matches!(
            validate_expected_write_run(Some(other_run_id), &actor, Some(&active_run)),
            Err(SlotError::WriteRunMismatch {
                expected_run_id,
                active_run_id,
            }) if expected_run_id == other_run_id && active_run_id == run_id
        ));

        let foreign_run = RunInfo {
            owner: Actor {
                id: "agent:other".into(),
                label: "other".into(),
                kind: ActorKind::Agent,
            },
            ..active_run
        };
        assert!(matches!(
            validate_expected_write_run(Some(run_id), &actor, Some(&foreign_run)),
            Err(SlotError::WriteRunNotOwner {
                expected_run_id
            }) if expected_run_id == run_id
        ));
    }

    #[test]
    fn cooperative_write_requires_a_human_and_matching_agent_lease_and_run() {
        let daemon_epoch = Uuid::new_v4();
        let mut control = ControlState::new(daemon_epoch, 1, ControlLimits::default());
        let agent = Actor {
            id: "agent:owner".into(),
            label: "owner".into(),
            kind: ActorKind::Agent,
        };
        let human = Actor {
            id: "human:operator".into(),
            label: "operator".into(),
            kind: ActorKind::Human,
        };
        let lease =
            match control.acquire(agent.clone(), ControlMode::Queue, 30_000, 1, Instant::now()) {
                AcquireOutcome::Granted(lease) => lease,
                other => panic!("expected granted lease, got {other:?}"),
            };
        let run = RunInfo {
            id: Uuid::new_v4(),
            owner: agent.clone(),
            label: "inspect DUT".into(),
            status: RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        };
        let request = SlotRequest::Write {
            actor: human.clone(),
            control_id: Uuid::nil(),
            fence: 0,
            data: b"status\r".to_vec(),
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(run.id),
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: true,
        };
        assert!(
            request
                .validate_write_authorization(&control, Some(&run))
                .is_ok()
        );

        let mut non_cooperative = request;
        if let SlotRequest::Write {
            cooperative,
            control_id,
            fence,
            ..
        } = &mut non_cooperative
        {
            *cooperative = false;
            *control_id = lease.id;
            *fence = lease.fence;
        }
        assert!(
            non_cooperative
                .validate_write_authorization(&control, Some(&run))
                .is_err(),
            "a Human must not borrow the Agent's ordinary fenced lease"
        );
        assert!(validate_cooperative_write(&agent, Some(run.id), &control, Some(&run)).is_err());
        assert!(validate_cooperative_write(&human, Some(run.id), &control, None).is_err());
        assert!(validate_cooperative_write(&human, None, &control, Some(&run)).is_err());
        assert!(matches!(
            validate_cooperative_write(&human, Some(Uuid::new_v4()), &control, Some(&run),),
            Err(SlotError::WriteRunMismatch { .. })
        ));
    }

    #[test]
    fn cooperative_write_retry_is_bound_to_the_expected_agent_run() {
        let actor = Actor {
            id: "human:operator".into(),
            label: "operator".into(),
            kind: ActorKind::Human,
        };
        let operation_id = Some(Uuid::new_v4());
        let old_run_id = Uuid::new_v4();
        let new_run_id = Uuid::new_v4();
        let request = |expected_run_id| SlotRequest::Write {
            actor: actor.clone(),
            control_id: Uuid::nil(),
            fence: 0,
            data: b"status\r".to_vec(),
            operation_id,
            expected_run_id: Some(expected_run_id),
            pacing: None,
            description: None,
            command_sequence: None,
            sequence_precondition: None,
            cooperative: true,
        };

        let old = request(old_run_id).write_fingerprint().unwrap();
        let retry_in_new_run = request(new_run_id).write_fingerprint().unwrap();
        assert_ne!(old, retry_in_new_run);

        let cached = CachedResult {
            fingerprint: old,
            result: Ok(CommandResult::WriteAccepted { event_seq: 41 }),
        };
        assert_ne!(cached.fingerprint, retry_in_new_run);
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
            expected_run_id: None,
            pacing: None,
            description: Some("重启样机".into()),
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
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
            expected_run_id: None,
            pacing: None,
            description: Some("重启样机".into()),
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
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
        assert!(!is_cacheable_write_result(&Err(
            SlotError::WriteDeadlineExceeded {
                required_ms: 15_002,
                maximum_ms: 15_000,
            }
        )));
        assert!(!is_cacheable_write_result(&Err(
            SlotError::WriteLeaseTooShort {
                remaining_ms: 5_000,
                write_ms: 8_116,
                margin_ms: 100,
            }
        )));
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

    #[tokio::test]
    async fn reader_close_carries_overflow_that_could_not_be_enqueued_first() {
        let (events, mut receiver) = mpsc::channel(1);
        assert!(events.try_send(PortEvent::Rx(vec![0])).is_ok());
        let mut pending = vec![1, 2, 3];
        let mut dropped = 0;
        enqueue_rx(&events, &mut pending, &mut dropped);
        assert_eq!(dropped, 3);

        let (_cancel, cancel) = watch::channel(false);
        let close = tokio::spawn(async move {
            let mut cancel = cancel;
            send_port_closed(&events, &mut cancel, "EOF".into(), dropped).await
        });
        assert!(matches!(receiver.recv().await, Some(PortEvent::Rx(_))));
        assert!(close.await.unwrap());
        assert!(matches!(
            receiver.recv().await,
            Some(PortEvent::Closed {
                dropped_bytes: 3,
                ..
            })
        ));
    }

    #[test]
    fn drained_reader_close_preserves_its_overflow_count() {
        let (data, dropped_bytes) = drained_port_event_parts(PortEvent::Closed {
            reason: "EOF".into(),
            dropped_bytes: 17,
        });

        assert!(data.is_none());
        assert_eq!(dropped_bytes, 17);
    }

    #[test]
    fn drained_reader_barrier_has_no_serial_payload_or_gap() {
        let (data, dropped_bytes) =
            drained_port_event_parts(PortEvent::ReaderBarrier { id: Uuid::new_v4() });
        assert!(data.is_none());
        assert_eq!(dropped_bytes, 0);
    }

    #[test]
    fn continuous_rx_does_not_extend_the_first_byte_flush_deadline() {
        let first_byte_at = tokio::time::Instant::now();
        let first_deadline = arm_rx_flush_deadline(None, first_byte_at, true).unwrap();
        let later_read_at = first_byte_at + Duration::from_millis(3);

        assert_eq!(
            arm_rx_flush_deadline(Some(first_deadline), later_read_at, true),
            Some(first_deadline)
        );
        assert_eq!(
            first_deadline.duration_since(first_byte_at),
            RX_COALESCE_WINDOW
        );
        assert_eq!(
            arm_rx_flush_deadline(Some(first_deadline), later_read_at, false),
            None
        );
    }

    #[test]
    fn rx_driver_read_is_bounded_by_the_remaining_event_capacity() {
        assert_eq!(rx_read_capacity(0), RX_BUFFER_BYTES);
        assert_eq!(rx_read_capacity(1), RX_BUFFER_BYTES - 1);
        assert_eq!(rx_read_capacity(RX_BUFFER_BYTES - 17), 17);
        assert_eq!(rx_read_capacity(RX_BUFFER_BYTES), 0);
    }

    #[test]
    fn write_deadline_scales_with_the_estimated_pacing_duration() {
        // The full-speed path keeps the fixed two-second timeout.
        assert_eq!(
            write_deadline(4_096, 1, Duration::ZERO),
            Ok(Duration::from_secs(2))
        );
        // A syntactically valid but impractically slow request is rejected in
        // full; the daemon must not clamp it and then fail partway through.
        assert_eq!(
            write_deadline(4_096, 1, Duration::from_millis(1)),
            Err(2_000 + 4_095 * (2 + 20))
        );
        assert_eq!(
            write_deadline(35, 1, Duration::from_millis(1)),
            Ok(Duration::from_millis(2_000 + 34 * (2 + 20)))
        );
        // The fixed timeout entirely covers a single chunk.
        assert_eq!(
            write_deadline(1, 16, Duration::from_millis(1)),
            Ok(Duration::from_secs(2))
        );
        // The boundary is inclusive, while one more planned chunk is rejected
        // before the port worker can observe the request.
        assert_eq!(
            write_deadline(591, 1, Duration::from_millis(1)),
            Ok(Duration::from_millis(14_980))
        );
        assert_eq!(
            write_deadline(592, 1, Duration::from_millis(1)),
            Err(15_002)
        );
        // Extreme pacing settings report a saturated required duration rather
        // than overflowing or silently accepting a clamped budget.
        assert_eq!(
            write_deadline(usize::MAX, 1, Duration::from_millis(u64::MAX)),
            Err(u64::MAX)
        );
    }

    #[test]
    fn minimum_and_near_expiry_leases_reject_writes_that_cannot_finish() {
        let fast_write = write_deadline(4_096, 4_096, Duration::ZERO).unwrap();
        assert_eq!(fast_write, WRITE_TIMEOUT);
        assert!(
            ensure_lease_covers_write(Duration::from_secs(5), fast_write).is_ok(),
            "a full five-second lease safely covers a two-second full-speed write"
        );

        let default_279_byte_write = write_deadline(279, 1, Duration::from_millis(1)).unwrap();
        assert_eq!(default_279_byte_write, Duration::from_millis(8_116));
        assert!(matches!(
            ensure_lease_covers_write(Duration::from_secs(5), default_279_byte_write),
            Err(SlotError::WriteLeaseTooShort {
                remaining_ms: 5_000,
                write_ms: 8_116,
                margin_ms: 100,
            })
        ));

        let exact_required = WRITE_TIMEOUT + WRITE_LEASE_SAFETY_MARGIN;
        assert!(ensure_lease_covers_write(exact_required, WRITE_TIMEOUT).is_ok());
        assert!(matches!(
            ensure_lease_covers_write(exact_required - Duration::from_millis(1), WRITE_TIMEOUT),
            Err(SlotError::WriteLeaseTooShort {
                remaining_ms: 2_099,
                write_ms: 2_000,
                margin_ms: 100,
            })
        ));
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

    struct DelayedOneByteWriter {
        delay: Duration,
        pending: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
        calls: usize,
    }

    impl DelayedOneByteWriter {
        fn new(delay: Duration) -> Self {
            Self {
                delay,
                pending: None,
                calls: 0,
            }
        }
    }

    impl tokio::io::AsyncWrite for DelayedOneByteWriter {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            context: &mut std::task::Context<'_>,
            buffer: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if self.pending.is_none() {
                self.pending = Some(Box::pin(tokio::time::sleep(self.delay)));
            }
            let timer_ready = {
                let timer = self.pending.as_mut().expect("timer was just installed");
                std::future::Future::poll(timer.as_mut(), context).is_ready()
            };
            if !timer_ready {
                return std::task::Poll::Pending;
            }
            self.pending = None;
            self.calls += 1;
            std::task::Poll::Ready(Ok(buffer.len().min(1)))
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
    async fn default_pacing_budget_covers_repeated_async_write_overhead() {
        const COMMAND_BYTES: usize = 279;
        const DRIVER_DELAY_MS: u64 = 10;
        let old_budget = Duration::from_millis(2_000 + (COMMAND_BYTES as u64 - 1) * 2);
        let modeled_elapsed = Duration::from_millis(
            COMMAND_BYTES as u64 * DRIVER_DELAY_MS + (COMMAND_BYTES as u64 - 1),
        );
        assert!(
            modeled_elapsed > old_budget,
            "the regression model must exceed the previous pacing-only deadline"
        );
        let write_timeout = write_deadline(COMMAND_BYTES, 1, Duration::from_millis(1))
            .expect("the real command must fit the bounded write budget");
        assert!(write_timeout > modeled_elapsed);

        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = DelayedOneByteWriter::new(Duration::from_millis(DRIVER_DELAY_MS));
        let data = vec![b'x'; COMMAND_BYTES];
        let start = tokio::time::Instant::now();
        let outcome = write_with_pacing(
            &mut writer,
            &data,
            WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 1,
            },
            start + write_timeout,
            &mut cancel,
        )
        .await;

        assert_eq!(outcome.written, COMMAND_BYTES);
        assert_eq!(outcome.error, None);
        assert!(!outcome.cancelled);
        assert_eq!(writer.calls, COMMAND_BYTES);
        assert_eq!(tokio::time::Instant::now() - start, modeled_elapsed);
    }

    #[tokio::test(start_paused = true)]
    async fn paced_write_chunks_bytes_and_sleeps_between_chunks() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(usize::MAX);
        let pacing = WritePacing {
            chunk_size: 2,
            chunk_delay_ms: 5,
        };
        let deadline = tokio::time::Instant::now()
            + write_deadline(
                b"abcde".len(),
                pacing.chunk_size as usize,
                Duration::from_millis(pacing.chunk_delay_ms),
            )
            .unwrap();
        let outcome = write_with_pacing(&mut writer, b"abcde", pacing, deadline, &mut cancel).await;
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
        let deadline = tokio::time::Instant::now()
            + write_deadline(
                b"abcd".len(),
                pacing.chunk_size as usize,
                Duration::from_millis(pacing.chunk_delay_ms),
            )
            .unwrap();
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, deadline, &mut cancel).await;
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
        let deadline = tokio::time::Instant::now()
            + write_deadline(
                b"abcd".len(),
                pacing.chunk_size as usize,
                Duration::from_millis(pacing.chunk_delay_ms),
            )
            .unwrap();
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, deadline, &mut cancel).await;
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
        let write_timeout = write_deadline(
            b"abcd".len(),
            pacing.chunk_size as usize,
            Duration::from_millis(pacing.chunk_delay_ms),
        )
        .unwrap();
        let outcome = write_with_pacing(
            &mut writer,
            b"abcd",
            pacing,
            start + write_timeout,
            &mut cancel,
        )
        .await;
        assert_eq!(outcome.written, 0);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("timed out"))
        );
        // An individual driver call that makes no progress retains the
        // two-second bound even though the overall paced request has a larger
        // budget. Tokio may resume the task just after the exact timer tick,
        // so assert the semantic bounds rather than an exact wake instant.
        let elapsed = tokio::time::Instant::now() - start;
        assert!(elapsed >= WRITE_TIMEOUT);
        assert!(
            elapsed
                < write_deadline(
                    b"abcd".len(),
                    pacing.chunk_size as usize,
                    Duration::from_millis(pacing.chunk_delay_ms),
                )
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn already_cancelled_write_wins_over_a_ready_driver() {
        let (cancel_tx, mut cancel) = watch::channel(false);
        cancel_tx.send(true).unwrap();
        // The writer is immediately ready. Cancellation must still win before
        // the first byte reaches the driver.
        let mut writer = RecordingWriter::new(usize::MAX);
        let pacing = WritePacing {
            chunk_size: 1,
            chunk_delay_ms: 1,
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let outcome = write_with_pacing(&mut writer, b"abcd", pacing, deadline, &mut cancel).await;
        assert_eq!(outcome.written, 0);
        assert!(writer.calls.is_empty());
        assert!(outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("cancelled"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn exact_total_deadline_wins_over_a_ready_driver() {
        let (_cancel_tx, mut cancel) = watch::channel(false);
        let mut writer = RecordingWriter::new(usize::MAX);
        let pacing = WritePacing {
            chunk_size: 1,
            chunk_delay_ms: 1,
        };

        let outcome = write_with_pacing(
            &mut writer,
            b"abcd",
            pacing,
            tokio::time::Instant::now(),
            &mut cancel,
        )
        .await;

        assert_eq!(outcome.written, 0);
        assert!(writer.calls.is_empty());
        assert!(!outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("timed out"))
        );
    }

    #[cfg(windows)]
    struct BlockingRecordingWriter {
        maximum_accept: usize,
        calls: Vec<usize>,
    }

    #[cfg(windows)]
    impl std::io::Write for BlockingRecordingWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.calls.push(buffer.len());
            Ok(buffer.len().min(self.maximum_accept))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(windows)]
    struct BlockingCancellingWriter {
        cancel: watch::Sender<bool>,
        calls: usize,
    }

    #[cfg(windows)]
    impl std::io::Write for BlockingCancellingWriter {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            self.cancel
                .send(true)
                .expect("test cancellation receiver must remain open");
            Ok(buffer.len().min(1))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_writer_preserves_chunking_and_partial_progress() {
        let (_cancel_tx, cancel) = watch::channel(false);
        let mut writer = BlockingRecordingWriter {
            maximum_accept: 1,
            calls: Vec::new(),
        };
        let outcome = write_with_blocking_pacing(
            &mut writer,
            b"abcd",
            WritePacing {
                chunk_size: 3,
                chunk_delay_ms: 0,
            },
            Instant::now() + Duration::from_secs(1),
            &cancel,
        );

        assert_eq!(outcome.written, 4);
        assert_eq!(outcome.error, None);
        assert!(!outcome.cancelled);
        assert_eq!(writer.calls, vec![3, 2, 1, 1]);
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_writer_stops_after_cancellation_with_partial_progress() {
        let (cancel_tx, cancel) = watch::channel(false);
        let mut writer = BlockingCancellingWriter {
            cancel: cancel_tx,
            calls: 0,
        };
        let outcome = write_with_blocking_pacing(
            &mut writer,
            b"abcd",
            WritePacing {
                chunk_size: 4,
                chunk_delay_ms: 0,
            },
            Instant::now() + Duration::from_secs(1),
            &cancel,
        );

        assert_eq!(outcome.written, 1);
        assert!(outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("cancelled"))
        );
        assert_eq!(writer.calls, 1);
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_writer_does_not_touch_driver_after_deadline() {
        let (_cancel_tx, cancel) = watch::channel(false);
        let mut writer = BlockingRecordingWriter {
            maximum_accept: usize::MAX,
            calls: Vec::new(),
        };
        let outcome = write_with_blocking_pacing(
            &mut writer,
            b"abcd",
            WritePacing {
                chunk_size: 4,
                chunk_delay_ms: 0,
            },
            Instant::now(),
            &cancel,
        );

        assert_eq!(outcome.written, 0);
        assert!(!outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("timed out"))
        );
        assert!(writer.calls.is_empty());
    }

    #[cfg(windows)]
    struct BlockingTimedOutWriter;

    #[cfg(windows)]
    impl std::io::Write for BlockingTimedOutWriter {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "simulated synchronous COM timeout",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[cfg(windows)]
    impl BlockingBreakSignal for BlockingTimedOutWriter {
        fn assert_break(&self) -> Result<(), String> {
            Ok(())
        }

        fn clear_break_signal(&self) -> Result<(), String> {
            Ok(())
        }
    }

    #[cfg(windows)]
    #[derive(Default)]
    struct RecordingBreakSignal {
        asserted: std::sync::atomic::AtomicUsize,
        cleared: std::sync::atomic::AtomicUsize,
    }

    #[cfg(windows)]
    impl BlockingBreakSignal for RecordingBreakSignal {
        fn assert_break(&self) -> Result<(), String> {
            self.asserted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn clear_break_signal(&self) -> Result<(), String> {
            self.cleared.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_break_is_asserted_for_a_bounded_interval_and_always_cleared() {
        let signal = RecordingBreakSignal::default();
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let outcome = send_break_blocking(&signal, Duration::from_millis(1), &cancel_rx);
        assert_eq!(outcome.error, None);
        assert!(!outcome.cancelled);
        assert_eq!(signal.asserted.load(Ordering::SeqCst), 1);
        assert_eq!(signal.cleared.load(Ordering::SeqCst), 1);
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_writer_reports_failure_and_closes_worker() {
        let (command_tx, command_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::channel(1);
        let (_cancel_tx, cancel_rx) = watch::channel(false);
        let (reply_tx, mut reply_rx) = oneshot::channel();
        command_tx
            .try_send(PortCommand::Write {
                data: b"x".to_vec(),
                pacing: WritePacing {
                    chunk_size: 1,
                    chunk_delay_ms: 0,
                },
                deadline: tokio::time::Instant::from_std(Instant::now() + Duration::from_secs(1)),
                reply: reply_tx,
            })
            .unwrap();

        run_windows_port_writer(
            BlockingTimedOutWriter,
            command_rx,
            event_tx,
            cancel_rx,
            Arc::new(AtomicBool::new(false)),
        );

        let outcome = reply_rx.try_recv().expect("write outcome");
        assert_eq!(outcome.written, 0);
        assert!(!outcome.cancelled);
        assert!(
            outcome
                .error
                .as_deref()
                .is_some_and(|message| message.contains("synchronous COM timeout"))
        );
        assert!(matches!(
            event_rx.try_recv(),
            Ok(PortEvent::Closed { reason, .. })
                if reason.contains("synchronous COM timeout")
        ));
    }

    #[cfg(windows)]
    #[test]
    fn windows_blocking_writer_never_touches_driver_after_cancellation() {
        let (cancel_tx, cancel) = watch::channel(false);
        cancel_tx.send(true).unwrap();
        let mut writer = BlockingRecordingWriter {
            maximum_accept: usize::MAX,
            calls: Vec::new(),
        };
        let outcome = write_with_blocking_pacing(
            &mut writer,
            b"abcd",
            WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 0,
            },
            Instant::now() + Duration::from_secs(1),
            &cancel,
        );

        assert_eq!(outcome.written, 0);
        assert!(outcome.cancelled);
        assert!(writer.calls.is_empty());
    }
}
