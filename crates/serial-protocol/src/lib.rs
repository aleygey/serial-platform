//! Stable, transport-independent contracts shared by `seriald` and its clients.
//!
//! WebSocket binary frames use a small envelope:
//! `[tag: u8][header_len: u32 big-endian][JSON header][raw bytes]`.
//! Control messages use tag `0x01`; device RX and confirmed TX use `0x02`
//! and `0x03`. Raw serial bytes are never converted to text by this crate.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub const PROTOCOL_VERSION: u16 = 1;
pub const CONTROL_FRAME_TAG: u8 = 0x01;
pub const RX_FRAME_TAG: u8 = 0x02;
pub const TX_FRAME_TAG: u8 = 0x03;
pub const WRITE_FRAME_TAG: u8 = 0x04;
pub const MAX_HEADER_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Observer,
    Operator,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorKind {
    Human,
    Agent,
    Script,
    System,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    pub id: String,
    pub label: String,
    pub kind: ActorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataBits {
    Five,
    Six,
    Seven,
    Eight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Parity {
    None,
    Odd,
    Even,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopBits {
    One,
    Two,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControl {
    None,
    Software,
    Hardware,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EchoMode {
    On,
    Off,
    Auto,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerialSettings {
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
    pub stop_bits: StopBits,
    pub flow_control: FlowControl,
    pub dtr: bool,
    pub rts: bool,
    pub write_eol: String,
    pub echo: EchoMode,
    pub shell_prompt: Option<String>,
    pub uboot_prompt: Option<String>,
    /// Bytes written to the driver per paced chunk. The daemon defaults to a
    /// typewriter-style one byte per chunk because slow target UARTs drop
    /// characters when a full write is pushed at once.
    #[serde(default = "default_write_chunk_size")]
    pub write_chunk_size: u32,
    /// Delay between paced write chunks. `0` disables pacing and writes at
    /// full speed.
    #[serde(default = "default_write_chunk_delay_ms")]
    pub write_chunk_delay_ms: u64,
    pub auto_open: bool,
    pub probe: Option<ProbeConfig>,
}

fn default_write_chunk_size() -> u32 {
    1
}

fn default_write_chunk_delay_ms() -> u64 {
    1
}

/// Per-write pacing override carried by [`ClientMessage::Write`].
///
/// The daemon writes at most `chunk_size` bytes per driver call and sleeps
/// `chunk_delay_ms` between chunks so slow target UARTs are not overrun. A
/// zero delay selects the full-speed write path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WritePacing {
    pub chunk_size: u32,
    pub chunk_delay_ms: u64,
}

impl WritePacing {
    /// Resolves the effective pacing for one write request: an explicit
    /// per-request override wins over the Slot settings.
    pub fn resolve(override_pacing: Option<Self>, settings: &SerialSettings) -> Self {
        override_pacing.unwrap_or(Self {
            chunk_size: settings.write_chunk_size,
            chunk_delay_ms: settings.write_chunk_delay_ms,
        })
    }
}

impl Default for SerialSettings {
    fn default() -> Self {
        Self {
            baud_rate: 115_200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            write_eol: "\r".into(),
            echo: EchoMode::On,
            shell_prompt: None,
            // Defaults to None so an attached device profile is not shadowed;
            // resolution falls back to DEFAULT_UBOOT_PROMPT.
            uboot_prompt: None,
            write_chunk_size: default_write_chunk_size(),
            write_chunk_delay_ms: default_write_chunk_delay_ms(),
            auto_open: true,
            probe: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeConfig {
    #[serde(with = "base64_bytes")]
    pub request: Vec<u8>,
    pub response_pattern: String,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotConfig {
    pub id: String,
    pub display_name: String,
    pub port: String,
    pub profile: String,
    /// Name of the device-model profile this Slot is attached to. Prompts and
    /// similar device behavior belong to the device model; Slot settings
    /// remain per-station overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device_profile: Option<String>,
    pub enabled: bool,
    pub settings: SerialSettings,
}

/// A reusable device-model profile. Prompt and line-ending defaults describe
/// the device connected behind any number of Slots, so they are configured
/// once per model instead of being embedded in every Slot's settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceProfile {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uboot_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_eol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub echo: Option<EchoMode>,
}

/// U-Boot prompt assumed when neither the Slot settings nor the attached
/// device profile provide one.
pub const DEFAULT_UBOOT_PROMPT: &str = "SigmaStar #";

/// Device-interaction settings after layering the Slot settings over the
/// attached device profile and the built-in defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedDeviceSettings {
    pub shell_prompt: Option<String>,
    pub uboot_prompt: Option<String>,
    pub write_eol: String,
    pub echo: EchoMode,
}

/// Resolves the effective device behavior for one Slot. Every field follows
/// the order: explicit Slot setting, then the device profile, then the
/// built-in default.
///
/// `shell_prompt`/`uboot_prompt` are optional in the Slot settings, so a
/// profile can supply them when the Slot does not. `write_eol` and `echo`
/// are concrete Slot fields that always materialize today, so they always
/// override the profile; the profile values are retained for clients and for
/// a future optional form of the Slot fields.
pub fn resolve_device_settings(
    settings: &SerialSettings,
    device_profile: Option<&DeviceProfile>,
) -> ResolvedDeviceSettings {
    let shell_prompt = settings
        .shell_prompt
        .clone()
        .or_else(|| device_profile.and_then(|profile| profile.shell_prompt.clone()));
    let uboot_prompt = settings
        .uboot_prompt
        .clone()
        .or_else(|| device_profile.and_then(|profile| profile.uboot_prompt.clone()))
        .or_else(|| Some(DEFAULT_UBOOT_PROMPT.to_owned()));
    ResolvedDeviceSettings {
        shell_prompt,
        uboot_prompt,
        write_eol: settings.write_eol.clone(),
        echo: settings.echo,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortDescriptor {
    pub name: String,
    pub port_type: String,
    pub manufacturer: Option<String>,
    pub product: Option<String>,
    pub serial_number: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Disabled,
    WaitingForPort,
    Opening,
    Online,
    Backoff,
    Stopping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetActivity {
    Unknown,
    Active,
    Silent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoggingState {
    Healthy,
    Degraded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlLease {
    pub id: Uuid,
    pub owner: Actor,
    pub epoch: Uuid,
    pub generation: u64,
    pub fence: u64,
    pub issued_wall_time_ns: i64,
    pub expires_wall_time_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Active,
    Completed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunInfo {
    pub id: Uuid,
    pub owner: Actor,
    pub label: String,
    pub status: RunStatus,
    pub start_seq: u64,
    pub end_seq: Option<u64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotSnapshot {
    pub config: SlotConfig,
    pub daemon_epoch: Uuid,
    pub head_seq: u64,
    pub ring_oldest_seq: Option<u64>,
    pub generation: u64,
    pub endpoint_present: bool,
    pub session_state: SessionState,
    pub state_reason: Option<String>,
    pub target_activity: TargetActivity,
    pub last_rx_wall_time_ns: Option<i64>,
    pub rx_offset: u64,
    pub tx_offset: u64,
    pub control: Option<ControlLease>,
    pub active_run: Option<RunInfo>,
    pub logging: LoggingState,
    /// Prompts after layering Slot settings over the attached device profile
    /// and the built-in defaults. Omitted on the wire when unset so older
    /// clients keep decoding snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_uboot_prompt: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Rx,
    Tx,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Rx,
    Tx,
    SerialOpening,
    SerialOpened,
    SerialOpenFailed,
    SerialClosed,
    SlotReconfigured,
    SlotRemoved,
    ControlGranted,
    ControlReleased,
    ControlRevoked,
    ControlExpired,
    RunStarted,
    RunEnded,
    RunAborted,
    Checkpoint,
    LoggingDegraded,
    Gap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub slot_id: String,
    pub daemon_epoch: Uuid,
    pub seq: u64,
    pub generation: u64,
    pub wall_time_ns: i64,
    pub monotonic_time_ns: u64,
    pub kind: EventKind,
    pub direction: Direction,
    pub actor: Option<Actor>,
    pub run_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub stream_offset_start: Option<u64>,
    pub stream_offset_end: Option<u64>,
    #[serde(default, with = "base64_bytes")]
    pub data: Vec<u8>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default)]
    pub durable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlMode {
    Queue,
    Takeover,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub epoch: Uuid,
    pub after_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscription {
    pub slot_id: String,
    pub cursor: Option<Cursor>,
    #[serde(default = "default_tail_events")]
    pub tail_events: usize,
}

fn default_tail_events() -> usize {
    200
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Hello {
        request_id: Uuid,
        protocol_version: u16,
        client_name: String,
        /// Client-declared audit source. The server always issues the actor ID
        /// and rejects [`ActorKind::System`].
        actor_kind: ActorKind,
    },
    Attach {
        request_id: Uuid,
        subscriptions: Vec<Subscription>,
    },
    Detach {
        request_id: Uuid,
        slots: Vec<String>,
    },
    AcquireControl {
        request_id: Uuid,
        slot_id: String,
        mode: ControlMode,
        ttl_ms: u64,
    },
    RenewControl {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
    },
    ReleaseControl {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
    },
    CancelAcquire {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
    },
    Write {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        /// Per-write pacing override. Older clients omit the field and keep
        /// using the Slot's configured pacing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pacing: Option<WritePacing>,
    },
    StartRun {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        label: String,
        #[serde(default)]
        metadata: BTreeMap<String, Value>,
    },
    EndRun {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        run_id: Uuid,
    },
    Checkpoint {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        label: String,
    },
    Ping {
        request_id: Uuid,
    },
}

impl ClientMessage {
    pub fn request_id(&self) -> Uuid {
        match self {
            Self::Hello { request_id, .. }
            | Self::Attach { request_id, .. }
            | Self::Detach { request_id, .. }
            | Self::AcquireControl { request_id, .. }
            | Self::RenewControl { request_id, .. }
            | Self::ReleaseControl { request_id, .. }
            | Self::CancelAcquire { request_id, .. }
            | Self::Write { request_id, .. }
            | Self::StartRun { request_id, .. }
            | Self::EndRun { request_id, .. }
            | Self::Checkpoint { request_id, .. }
            | Self::Ping { request_id } => *request_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandResult {
    HelloAccepted { actor: Actor, role: Role },
    Attached { slots: Vec<String> },
    Detached { slots: Vec<String> },
    ControlGranted { lease: ControlLease },
    ControlQueued { position: usize },
    ControlRenewed { lease: ControlLease },
    ControlReleased,
    AcquireCancelled { removed: bool },
    WriteAccepted { event_seq: u64 },
    RunStarted { run: RunInfo },
    RunEnded { run: RunInfo },
    CheckpointCreated { event_seq: u64 },
    Pong { server_wall_time_ns: i64 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Welcome {
        server_id: Uuid,
        daemon_epoch: Uuid,
        protocol_version: u16,
        actor: Actor,
        role: Role,
    },
    Snapshot {
        slot: Box<SlotSnapshot>,
    },
    ReplayBegin {
        slot_id: String,
        from_seq: u64,
        through_seq: u64,
    },
    Ready {
        slot_id: String,
        head_seq: u64,
    },
    Timeline {
        event: TimelineEvent,
        replay: bool,
    },
    Result {
        request_id: Uuid,
        result: CommandResult,
    },
    Error {
        request_id: Option<Uuid>,
        code: ErrorCode,
        message: String,
        retryable: bool,
    },
    Gap {
        slot_id: String,
        requested_after_seq: Option<u64>,
        first_available_seq: Option<u64>,
        head_seq: u64,
        reason: GapReason,
    },
    Lagged {
        slot_id: String,
        from_seq: u64,
        to_seq: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    ControlRequired,
    StaleFence,
    PortOffline,
    CursorAhead,
    ResourceExhausted,
    IdempotencyExpired,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    EpochChanged,
    RingEvicted,
    Retention,
    Corruption,
    LoggingFault,
    /// Adjacent retained records prove that one or more sequence numbers are
    /// absent, but the query cannot safely attribute the loss to retention,
    /// corruption, or a known writer failure.
    SequenceDiscontinuity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    pub uptime_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    pub slots: Vec<SlotSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigureSlotsRequest {
    pub slots: Vec<SlotConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigureSlotsResponse {
    pub slots: Vec<SlotSnapshot>,
}

/// Read model for the configured device-model profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceProfileListResponse {
    pub profiles: Vec<DeviceProfile>,
}

/// Full replacement of the device-model profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceProfilesRequest {
    pub profiles: Vec<DeviceProfile>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceProfilesResponse {
    pub profiles: Vec<DeviceProfile>,
}

/// One discoverable, retained Slot/daemon-epoch journal archive.
///
/// Segment timestamps describe when the first and last retained segments were
/// created. Event timestamps remain available from the bounded event query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveSummary {
    pub slot_id: String,
    pub epoch: Uuid,
    pub first_seq: u64,
    pub last_seq: u64,
    pub first_segment_wall_time_ns: i64,
    pub last_segment_wall_time_ns: i64,
    pub segment_count: u64,
    pub total_bytes: u64,
    pub has_open_segment: bool,
}

/// Bounded archive catalog returned by `seriald`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveListResponse {
    pub archives: Vec<ArchiveSummary>,
    /// More retained archives exist than fit in this response.
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventQuery {
    pub epoch: Option<Uuid>,
    pub after_seq: Option<u64>,
    pub before_wall_time_ns: Option<i64>,
    pub after_wall_time_ns: Option<i64>,
    pub direction: Option<Direction>,
    pub kind: Option<EventKind>,
    pub actor_id: Option<String>,
    pub run_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub contains: Option<String>,
    pub limit_events: Option<usize>,
    pub limit_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventQueryResponse {
    pub events: Vec<TimelineEvent>,
    pub next_cursor: Option<Cursor>,
    pub truncated: bool,
    pub first_available_seq: Option<u64>,
    pub gaps: Vec<GapRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapRange {
    pub epoch: Uuid,
    pub first_seq: u64,
    pub last_seq: u64,
    pub reason: GapReason,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataFrameHeader {
    pub protocol_version: u16,
    pub slot_id: String,
    pub daemon_epoch: Uuid,
    pub seq: u64,
    pub generation: u64,
    pub wall_time_ns: i64,
    pub monotonic_time_ns: u64,
    pub kind: EventKind,
    pub direction: Direction,
    pub actor: Option<Actor>,
    pub run_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub stream_offset_start: Option<u64>,
    pub stream_offset_end: Option<u64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub durable: bool,
    #[serde(default)]
    pub replay: bool,
}

impl From<&TimelineEvent> for DataFrameHeader {
    fn from(event: &TimelineEvent) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            slot_id: event.slot_id.clone(),
            daemon_epoch: event.daemon_epoch,
            seq: event.seq,
            generation: event.generation,
            wall_time_ns: event.wall_time_ns,
            monotonic_time_ns: event.monotonic_time_ns,
            kind: event.kind,
            direction: event.direction,
            actor: event.actor.clone(),
            run_id: event.run_id,
            operation_id: event.operation_id,
            stream_offset_start: event.stream_offset_start,
            stream_offset_end: event.stream_offset_end,
            metadata: event.metadata.clone(),
            durable: event.durable,
            replay: false,
        }
    }
}

impl DataFrameHeader {
    pub fn into_event(self, data: Vec<u8>) -> TimelineEvent {
        TimelineEvent {
            slot_id: self.slot_id,
            daemon_epoch: self.daemon_epoch,
            seq: self.seq,
            generation: self.generation,
            wall_time_ns: self.wall_time_ns,
            monotonic_time_ns: self.monotonic_time_ns,
            kind: self.kind,
            direction: self.direction,
            actor: self.actor,
            run_id: self.run_id,
            operation_id: self.operation_id,
            stream_offset_start: self.stream_offset_start,
            stream_offset_end: self.stream_offset_end,
            data,
            metadata: self.metadata,
            durable: self.durable,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum WireFrame {
    Control(ServerMessage),
    Rx(DataFrameHeader, Vec<u8>),
    Tx(DataFrameHeader, Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame is too short")]
    TooShort,
    #[error("unknown frame tag {0:#04x}")]
    UnknownTag(u8),
    #[error("header is too large: {0} bytes")]
    HeaderTooLarge(usize),
    #[error("payload is too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("frame header length is invalid")]
    InvalidHeaderLength,
    #[error("JSON codec error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("event kind does not match frame tag")]
    DirectionMismatch,
}

pub fn encode_control(message: &ServerMessage) -> Result<Vec<u8>, ProtocolError> {
    encode_json_envelope(CONTROL_FRAME_TAG, message, &[])
}

pub fn decode_client_control(bytes: &[u8]) -> Result<ClientMessage, ProtocolError> {
    let (tag, header, payload) = split_envelope(bytes)?;
    if tag != CONTROL_FRAME_TAG || !payload.is_empty() {
        return Err(ProtocolError::UnknownTag(tag));
    }
    Ok(serde_json::from_slice(header)?)
}

pub fn encode_client_control(message: &ClientMessage) -> Result<Vec<u8>, ProtocolError> {
    encode_json_envelope(CONTROL_FRAME_TAG, message, &[])
}

pub fn decode_control(bytes: &[u8]) -> Result<ServerMessage, ProtocolError> {
    let (tag, header, payload) = split_envelope(bytes)?;
    if tag != CONTROL_FRAME_TAG || !payload.is_empty() {
        return Err(ProtocolError::UnknownTag(tag));
    }
    Ok(serde_json::from_slice(header)?)
}

pub fn encode_event(event: &TimelineEvent, replay: bool) -> Result<Vec<u8>, ProtocolError> {
    let tag = match event.direction {
        Direction::Rx => RX_FRAME_TAG,
        Direction::Tx => TX_FRAME_TAG,
        Direction::None => {
            return encode_control(&ServerMessage::Timeline {
                event: event.clone(),
                replay,
            });
        }
    };
    let mut header = DataFrameHeader::from(event);
    header.replay = replay;
    encode_json_envelope(tag, &header, &event.data)
}

pub fn decode_wire_frame(bytes: &[u8]) -> Result<WireFrame, ProtocolError> {
    let (tag, header, payload) = split_envelope(bytes)?;
    match tag {
        CONTROL_FRAME_TAG => Ok(WireFrame::Control(serde_json::from_slice(header)?)),
        RX_FRAME_TAG | TX_FRAME_TAG => {
            let decoded: DataFrameHeader = serde_json::from_slice(header)?;
            let expected = if tag == RX_FRAME_TAG {
                Direction::Rx
            } else {
                Direction::Tx
            };
            if decoded.direction != expected {
                return Err(ProtocolError::DirectionMismatch);
            }
            if tag == RX_FRAME_TAG {
                Ok(WireFrame::Rx(decoded, payload.to_vec()))
            } else {
                Ok(WireFrame::Tx(decoded, payload.to_vec()))
            }
        }
        other => Err(ProtocolError::UnknownTag(other)),
    }
}

fn encode_json_envelope<T: Serialize>(
    tag: u8,
    header: &T,
    payload: &[u8],
) -> Result<Vec<u8>, ProtocolError> {
    let header = serde_json::to_vec(header)?;
    if header.len() > MAX_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge(header.len()));
    }
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge(payload.len()));
    }
    let mut frame = Vec::with_capacity(5 + header.len() + payload.len());
    frame.push(tag);
    frame.extend_from_slice(&(header.len() as u32).to_be_bytes());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn split_envelope(bytes: &[u8]) -> Result<(u8, &[u8], &[u8]), ProtocolError> {
    if bytes.len() < 5 {
        return Err(ProtocolError::TooShort);
    }
    let tag = bytes[0];
    let header_len = u32::from_be_bytes(bytes[1..5].try_into().expect("fixed length")) as usize;
    if header_len > MAX_HEADER_BYTES {
        return Err(ProtocolError::HeaderTooLarge(header_len));
    }
    let header_end = 5usize
        .checked_add(header_len)
        .ok_or(ProtocolError::InvalidHeaderLength)?;
    if header_end > bytes.len() {
        return Err(ProtocolError::InvalidHeaderLength);
    }
    let payload = &bytes[header_end..];
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge(payload.len()));
    }
    Ok((tag, &bytes[5..header_end], payload))
}

mod base64_bytes {
    use super::*;
    use serde::{Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        BASE64.decode(encoded).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(direction: Direction, kind: EventKind, data: Vec<u8>) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: Uuid::new_v4(),
            seq: 42,
            generation: 3,
            wall_time_ns: 123,
            monotonic_time_ns: 456,
            kind,
            direction,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(8),
            stream_offset_end: Some(8 + data.len() as u64),
            data,
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    #[test]
    fn raw_rx_bytes_round_trip_without_utf8_conversion() {
        let original = event(Direction::Rx, EventKind::Rx, (0..=255).collect());
        let encoded = encode_event(&original, true).unwrap();
        let WireFrame::Rx(header, bytes) = decode_wire_frame(&encoded).unwrap() else {
            panic!("expected RX frame");
        };
        assert!(header.replay);
        assert_eq!(header.into_event(bytes), original);
    }

    #[test]
    fn control_round_trip() {
        let message = ServerMessage::Ready {
            slot_id: "slot-1".into(),
            head_seq: 9,
        };
        assert_eq!(
            decode_control(&encode_control(&message).unwrap()).unwrap(),
            message
        );
    }

    #[test]
    fn rejects_truncated_header() {
        let frame = [CONTROL_FRAME_TAG, 0, 0, 0, 10, b'{'];
        assert!(matches!(
            decode_control(&frame),
            Err(ProtocolError::InvalidHeaderLength)
        ));
    }

    #[test]
    fn default_profile_matches_station_decisions() {
        let settings = SerialSettings::default();
        assert_eq!(settings.baud_rate, 115_200);
        assert_eq!(settings.flow_control, FlowControl::None);
        assert!(!settings.dtr);
        assert!(!settings.rts);
        assert_eq!(settings.write_eol, "\r");
        assert_eq!(settings.echo, EchoMode::On);
        assert!(settings.shell_prompt.is_none());
        // The built-in U-Boot prompt moved behind resolution so a device
        // profile is not shadowed by the Slot default.
        assert!(settings.uboot_prompt.is_none());
        assert_eq!(settings.write_chunk_size, 1);
        assert_eq!(settings.write_chunk_delay_ms, 1);
        assert!(settings.probe.is_none());
    }

    fn device_profile() -> DeviceProfile {
        DeviceProfile {
            name: "sigmastar-evb".into(),
            shell_prompt: Some("root@sigmastar:/# ".into()),
            uboot_prompt: Some("SigmaStar =>".into()),
            write_eol: Some("\n".into()),
            echo: Some(EchoMode::Off),
        }
    }

    #[test]
    fn device_settings_resolve_slot_then_profile_then_builtin() {
        let profile = device_profile();
        // Profile supplies everything the Slot leaves unset.
        let resolved = resolve_device_settings(&SerialSettings::default(), Some(&profile));
        assert_eq!(resolved.shell_prompt.as_deref(), Some("root@sigmastar:/# "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("SigmaStar =>"));
        // write_eol/echo are concrete Slot fields: the profile never shadows
        // them in v1.
        assert_eq!(resolved.write_eol, "\r");
        assert_eq!(resolved.echo, EchoMode::On);

        // An explicit Slot setting always wins over the profile.
        let settings = SerialSettings {
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("U-Boot> ".into()),
            write_eol: "\r\n".into(),
            echo: EchoMode::Auto,
            ..SerialSettings::default()
        };
        let resolved = resolve_device_settings(&settings, Some(&profile));
        assert_eq!(resolved.shell_prompt.as_deref(), Some("/ # "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("U-Boot> "));
        assert_eq!(resolved.write_eol, "\r\n");
        assert_eq!(resolved.echo, EchoMode::Auto);
    }

    #[test]
    fn device_settings_without_profile_match_legacy_behavior() {
        // Regression: a configuration without device profiles resolves to the
        // same effective values as before profiles existed.
        let resolved = resolve_device_settings(&SerialSettings::default(), None);
        assert!(resolved.shell_prompt.is_none());
        assert_eq!(resolved.uboot_prompt.as_deref(), Some(DEFAULT_UBOOT_PROMPT));
        assert_eq!(resolved.write_eol, "\r");
        assert_eq!(resolved.echo, EchoMode::On);
    }

    #[test]
    fn legacy_slot_config_without_device_profile_still_decodes() {
        let legacy = serde_json::json!({
            "id": "slot-1",
            "display_name": "Slot 1",
            "port": "COM3",
            "profile": "generic-115200",
            "enabled": true,
            "settings": SerialSettings::default(),
        });
        let slot: SlotConfig = serde_json::from_value(legacy).unwrap();
        assert!(slot.device_profile.is_none());
    }

    #[test]
    fn snapshot_omits_unset_effective_prompts_on_the_wire() {
        let json = serde_json::to_value(SlotSnapshot {
            config: SlotConfig {
                id: "slot-1".into(),
                display_name: "Slot 1".into(),
                port: "COM3".into(),
                profile: "generic-115200".into(),
                device_profile: None,
                enabled: true,
                settings: SerialSettings::default(),
            },
            daemon_epoch: Uuid::new_v4(),
            head_seq: 0,
            ring_oldest_seq: None,
            generation: 0,
            endpoint_present: false,
            session_state: SessionState::Disabled,
            state_reason: None,
            target_activity: TargetActivity::Unknown,
            last_rx_wall_time_ns: None,
            rx_offset: 0,
            tx_offset: 0,
            control: None,
            active_run: None,
            logging: LoggingState::Healthy,
            effective_shell_prompt: None,
            effective_uboot_prompt: None,
        })
        .unwrap();
        let object = json.as_object().unwrap();
        assert!(!object.contains_key("effective_shell_prompt"));
        assert!(!object.contains_key("effective_uboot_prompt"));
        // ...and an older daemon's snapshot without the keys still decodes.
        let decoded: SlotSnapshot = serde_json::from_value(json).unwrap();
        assert!(decoded.effective_shell_prompt.is_none());
        assert!(decoded.effective_uboot_prompt.is_none());
    }

    #[test]
    fn write_pacing_round_trips_through_the_control_frame() {
        let message = ClientMessage::Write {
            request_id: Uuid::new_v4(),
            slot_id: "slot-1".into(),
            control_id: Uuid::new_v4(),
            fence: 7,
            data: b"reboot\r".to_vec(),
            operation_id: Some(Uuid::new_v4()),
            pacing: Some(WritePacing {
                chunk_size: 4,
                chunk_delay_ms: 10,
            }),
        };
        let frame = encode_client_control(&message).unwrap();
        assert_eq!(decode_client_control(&frame).unwrap(), message);
    }

    #[test]
    fn legacy_write_message_without_pacing_still_decodes() {
        // A pre-pacing client serializes the Write variant without the
        // optional pacing key; the daemon must keep accepting that shape.
        let request_id = Uuid::new_v4();
        let control_id = Uuid::new_v4();
        let legacy = serde_json::json!({
            "type": "write",
            "request_id": request_id,
            "slot_id": "slot-1",
            "control_id": control_id,
            "fence": 3,
            "data": BASE64.encode(b"reboot\r"),
            "operation_id": null,
        });
        let header = serde_json::to_vec(&legacy).unwrap();
        let mut frame = vec![CONTROL_FRAME_TAG];
        frame.extend_from_slice(&(header.len() as u32).to_be_bytes());
        frame.extend_from_slice(&header);
        assert_eq!(
            decode_client_control(&frame).unwrap(),
            ClientMessage::Write {
                request_id,
                slot_id: "slot-1".into(),
                control_id,
                fence: 3,
                data: b"reboot\r".to_vec(),
                operation_id: None,
                pacing: None,
            }
        );
    }

    #[test]
    fn pacing_resolution_prefers_the_request_override() {
        let settings = SerialSettings {
            write_chunk_size: 8,
            write_chunk_delay_ms: 5,
            ..SerialSettings::default()
        };
        assert_eq!(
            WritePacing::resolve(None, &settings),
            WritePacing {
                chunk_size: 8,
                chunk_delay_ms: 5,
            }
        );
        let override_pacing = WritePacing {
            chunk_size: 2,
            chunk_delay_ms: 0,
        };
        assert_eq!(
            WritePacing::resolve(Some(override_pacing), &settings),
            override_pacing
        );
    }

    #[test]
    fn cancel_acquire_round_trips_through_the_control_frame() {
        let request_id = Uuid::new_v4();
        let message = ClientMessage::CancelAcquire {
            request_id,
            slot_id: "slot-1".into(),
            control_id: Uuid::new_v4(),
        };
        assert_eq!(message.request_id(), request_id);
        let frame = encode_client_control(&message).unwrap();
        assert_eq!(decode_client_control(&frame).unwrap(), message);
    }

    #[test]
    fn acquire_cancelled_result_uses_the_snake_case_wire_tag() {
        let result = CommandResult::AcquireCancelled { removed: true };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["type"], "acquire_cancelled");
        assert_eq!(json["removed"], true);
        assert_eq!(
            serde_json::from_value::<CommandResult>(json).unwrap(),
            result
        );
    }
}
