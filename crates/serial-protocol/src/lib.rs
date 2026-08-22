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

/// Shared protocol generation for HTTP DTOs, WebSocket control/timeline
/// frames, and cross-component compatibility checks.
pub const PROTOCOL_VERSION: u16 = 5;
pub const CONTROL_FRAME_TAG: u8 = 0x01;
pub const RX_FRAME_TAG: u8 = 0x02;
pub const TX_FRAME_TAG: u8 = 0x03;
pub const WRITE_FRAME_TAG: u8 = 0x04;
pub const MAX_HEADER_BYTES: usize = 256 * 1024;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_TRIGGER_INTERVAL_MS: u64 = 20;
pub const DEFAULT_TRIGGER_TIMEOUT_MS: u64 = 5_000;
pub const DEFAULT_TRIGGER_MAX_FIRES: u32 = 250;
pub const MAX_TRIGGER_INITIAL_WRITE_BYTES: usize = 4 * 1024;
pub const MAX_TRIGGER_ACTION_BYTES: usize = 256;
pub const MAX_TRIGGER_PATTERN_BYTES: usize = 256;
pub const MAX_TRIGGER_PATTERNS: usize = 8;
pub const MIN_TRIGGER_INTERVAL_MS: u64 = 5;
pub const MAX_TRIGGER_INTERVAL_MS: u64 = 1_000;
pub const MIN_TRIGGER_TIMEOUT_MS: u64 = 100;
pub const MAX_TRIGGER_TIMEOUT_MS: u64 = 30_000;
pub const MAX_TRIGGER_FIRES: u32 = 1_000;
pub const MAX_TRIGGER_TOTAL_BYTES: usize = 64 * 1024;
pub const MIN_BREAK_DURATION_MS: u64 = 1;
pub const MAX_BREAK_DURATION_MS: u64 = 5_000;
/// Upper bound for one physical write accepted by `seriald`.
///
/// A Trigger timeout stops new scheduling, but an already accepted write may
/// need this long to settle and be audited before the Trigger becomes terminal.
pub const MAX_PHYSICAL_WRITE_TIMEOUT_MS: u64 = 15_000;
/// Maximum UTF-8 size of the human-readable purpose attached to one Agent
/// command. The purpose is durable audit metadata, not serial payload.
pub const MAX_COMMAND_DESCRIPTION_BYTES: usize = 256;

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

/// Reusable physical UART configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportProfile {
    pub name: String,
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
    pub stop_bits: StopBits,
    pub flow_control: FlowControl,
    pub dtr: bool,
    pub rts: bool,
    pub auto_open: bool,
}

/// Physical UART settings after resolving a port's transport-profile binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTransportSettings {
    pub baud_rate: u32,
    pub data_bits: DataBits,
    pub parity: Parity,
    pub stop_bits: StopBits,
    pub flow_control: FlowControl,
    pub dtr: bool,
    pub rts: bool,
    pub auto_open: bool,
}

pub fn resolve_transport_settings(
    settings: &SerialSettings,
    transport_profile: Option<&TransportProfile>,
) -> ResolvedTransportSettings {
    match transport_profile {
        Some(profile) => ResolvedTransportSettings {
            baud_rate: profile.baud_rate,
            data_bits: profile.data_bits,
            parity: profile.parity,
            stop_bits: profile.stop_bits,
            flow_control: profile.flow_control,
            dtr: profile.dtr,
            rts: profile.rts,
            auto_open: profile.auto_open,
        },
        None => ResolvedTransportSettings {
            baud_rate: settings.baud_rate,
            data_bits: settings.data_bits,
            parity: settings.parity,
            stop_bits: settings.stop_bits,
            flow_control: settings.flow_control,
            dtr: settings.dtr,
            rts: settings.rts,
            auto_open: settings.auto_open,
        },
    }
}

/// Applies only physical UART fields and preserves model interaction settings.
pub fn apply_transport_profile(
    settings: &SerialSettings,
    transport_profile: Option<&TransportProfile>,
) -> SerialSettings {
    let resolved = resolve_transport_settings(settings, transport_profile);
    SerialSettings {
        baud_rate: resolved.baud_rate,
        data_bits: resolved.data_bits,
        parity: resolved.parity,
        stop_bits: resolved.stop_bits,
        flow_control: resolved.flow_control,
        dtr: resolved.dtr,
        rts: resolved.rts,
        auto_open: resolved.auto_open,
        ..settings.clone()
    }
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

/// Optional grouping metadata for one physical write that belongs to a known
/// dependent command sequence. This is durable audit context only; it does
/// not claim that the DUT executed either the step or the whole sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSequenceAuditContext {
    pub sequence_id: Uuid,
    /// Human-readable purpose of the complete sequence. Individual step
    /// purpose remains in `ClientMessage::Write::description`.
    pub description: String,
    /// Zero-based index of this physical write within the sequence.
    pub step_index: u8,
    pub step_count: u8,
}

/// The concrete receive boundary used for one command write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandCaptureMatcherKind {
    Contains,
    Regex,
    ShellPrompt,
    UbootPrompt,
}

/// A matcher persisted with the authoritative TX event so user interfaces can
/// recover the exact command/output region without guessing from current
/// profile settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandCaptureMatcher {
    pub kind: CommandCaptureMatcherKind,
    pub value: String,
}

/// Optional fail-closed boundary for one Agent physical serial action.
///
/// The daemon validates this inside the port actor immediately before the
/// write, BREAK, or Trigger enters the physical action boundary. RX and
/// ordinary control events after `cursor` are allowed, but a changed serial
/// generation, a changed TX offset, an explicit Gap event, or an evicted
/// replay window rejects the action with a definite zero-byte outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceWritePrecondition {
    pub cursor: Cursor,
    pub expected_generation: u64,
    pub expected_tx_offset: u64,
}

impl WritePacing {
    /// Resolves the effective pacing for one write request: an explicit
    /// per-request override wins over the port settings.
    pub fn resolve(override_pacing: Option<Self>, settings: &SerialSettings) -> Self {
        override_pacing.unwrap_or(Self {
            chunk_size: settings.write_chunk_size,
            chunk_delay_ms: settings.write_chunk_delay_ms,
        })
    }
}

/// One bounded, device-agnostic reaction to the live serial byte stream.
///
/// The daemon arms its literal-byte matchers before performing the optional
/// initial write. It then sends `action` at the requested interval, stopping
/// when a `stop_contains` pattern is observed or either hard bound is reached.
/// Model-specific meaning and higher-level workflows deliberately remain in
/// clients; every byte field here is encoded as base64 on the JSON wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerSpec {
    /// Optional one-time write after the live RX matchers have been armed.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_base64_bytes"
    )]
    pub initial_write: Option<Vec<u8>>,
    /// Optional advanced live-RX gate for the first action write. When this is
    /// absent, the first action becomes eligible immediately after a confirmed
    /// initial write, or immediately after arming when there is no initial
    /// write.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_base64_bytes"
    )]
    pub start_contains: Option<Vec<u8>>,
    /// Raw bytes sent once per fire.
    #[serde(with = "base64_bytes")]
    pub action: Vec<u8>,
    /// Delay between completed action writes. The daemon validates a safe
    /// non-zero lower bound before accepting the Job.
    #[serde(default = "default_trigger_interval_ms")]
    pub interval_ms: u64,
    /// Live RX literals that terminate the Job successfully. Matching spans
    /// serial read-chunk boundaries.
    #[serde(default, with = "base64_byte_patterns")]
    pub stop_contains: Vec<Vec<u8>>,
    /// Wall-clock deadline after which the daemon schedules no new writes.
    /// One already accepted bounded write is allowed to settle and be audited
    /// before the Job enters its authoritative terminal state.
    #[serde(default = "default_trigger_timeout_ms")]
    pub timeout_ms: u64,
    /// Hard bound on confirmed action writes. The initial write is not a fire.
    #[serde(default = "default_trigger_max_fires")]
    pub max_fires: u32,
    /// Optional pacing applied to both the initial write and every action
    /// write. When absent, the port's effective write pacing is used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pacing: Option<WritePacing>,
}

fn default_trigger_interval_ms() -> u64 {
    DEFAULT_TRIGGER_INTERVAL_MS
}

fn default_trigger_timeout_ms() -> u64 {
    DEFAULT_TRIGGER_TIMEOUT_MS
}

fn default_trigger_max_fires() -> u32 {
    DEFAULT_TRIGGER_MAX_FIRES
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
    /// Operating-system serial port name and the sole identity of this
    /// logical connection (for example `COM4` or `/dev/cu.usbserial-210`).
    pub port: String,
    /// Optional reusable physical UART profile. No profile means 115200 8N1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_profile: Option<String>,
    /// Optional model profile describing the connected device and its prompt
    /// and input behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile: Option<String>,
    /// Optional concrete product name within the bound model profile. The
    /// profile owns reusable interaction behavior; this field identifies the
    /// exact device attached to this port for humans and Agents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_name: Option<String>,
    pub enabled: bool,
}

/// Maximum concrete product names in one model-family profile.
pub const MAX_MODEL_NAMES_PER_PROFILE: usize = 128;

/// Reusable interaction behavior for one model family.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfile {
    pub name: String,
    /// Concrete product names belonging to this family, displayed as the
    /// second level of the model selector.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub model_names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uboot_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_eol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub echo: Option<EchoMode>,
    /// Optional target-specific UART write pacing. These fields belong to the
    /// DUT because they compensate for how quickly that target consumes input,
    /// not for a property of the host COM port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_chunk_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_chunk_delay_ms: Option<u64>,
}

/// Device-interaction settings after applying the attached model profile.
/// Generic defaults deliberately do not guess Shell or U-Boot prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedModelSettings {
    pub shell_prompt: Option<String>,
    pub uboot_prompt: Option<String>,
    pub write_eol: String,
    pub echo: EchoMode,
    pub write_pacing: WritePacing,
}

/// Resolves effective model behavior. A bound profile owns Shell/U-Boot
/// prompts and any provided EOL, echo, and pacing overrides.
pub fn resolve_model_settings(
    settings: &SerialSettings,
    model_profile: Option<&ModelProfile>,
) -> ResolvedModelSettings {
    let shell_prompt = match model_profile {
        Some(profile) => profile.shell_prompt.clone(),
        None => settings.shell_prompt.clone(),
    };
    let uboot_prompt = match model_profile {
        Some(profile) => profile.uboot_prompt.clone(),
        None => settings.uboot_prompt.clone(),
    };
    ResolvedModelSettings {
        shell_prompt,
        uboot_prompt,
        write_eol: model_profile
            .and_then(|profile| profile.write_eol.clone())
            .unwrap_or_else(|| settings.write_eol.clone()),
        echo: model_profile
            .and_then(|profile| profile.echo)
            .unwrap_or(settings.echo),
        write_pacing: WritePacing {
            chunk_size: model_profile
                .and_then(|profile| profile.write_chunk_size)
                .unwrap_or(settings.write_chunk_size),
            chunk_delay_ms: model_profile
                .and_then(|profile| profile.write_chunk_delay_ms)
                .unwrap_or(settings.write_chunk_delay_ms),
        },
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerStatus {
    Armed,
    WaitingForStart,
    Running,
    /// A terminal cause has been selected, but one short driver write may
    /// still need to finish and be audited before the Job is fully stopped.
    Stopping,
    Matched,
    TimedOut,
    MaxFiresReached,
    Cancelled,
    ControlLost,
    RunLost,
    GenerationChanged,
    PortClosed,
    WriteFailed,
    RxGap,
}

impl TriggerStatus {
    pub const fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Armed | Self::WaitingForStart | Self::Running | Self::Stopping
        )
    }

    pub const fn is_matched(self) -> bool {
        matches!(self, Self::Matched)
    }
}

/// Authoritative state for one daemon-owned Trigger Job.
///
/// `fires_confirmed` counts action writes only. `tx_bytes_confirmed` includes
/// both the confirmed initial-write bytes and confirmed action bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub id: Uuid,
    pub owner: Actor,
    pub daemon_epoch: Uuid,
    pub generation: u64,
    pub control_id: Uuid,
    pub fence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_run_id: Option<Uuid>,
    pub spec: TriggerSpec,
    pub status: TriggerStatus,
    pub start_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_write_seq: Option<u64>,
    #[serde(default)]
    pub fires_confirmed: u32,
    #[serde(default)]
    pub tx_bytes_confirmed: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "option_base64_bytes"
    )]
    pub matched_pattern: Option<Vec<u8>>,
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
    /// Stable classification for `state_reason`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_code: Option<ErrorCode>,
    pub target_activity: TargetActivity,
    pub last_rx_wall_time_ns: Option<i64>,
    pub rx_offset: u64,
    pub tx_offset: u64,
    /// Total reader bytes dropped during this daemon epoch for this port.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub rx_overflow_bytes: u64,
    pub control: Option<ControlLease>,
    pub active_run: Option<RunInfo>,
    /// Current daemon-owned Trigger Job, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_trigger: Option<TriggerInfo>,
    pub logging: LoggingState,
    /// Authoritative prompts after resolving the bound model profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_uboot_prompt: Option<String>,
    /// Effective line ending and echo policy after applying the model profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_write_eol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_echo: Option<EchoMode>,
    /// Authoritative physical UART settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_transport: Option<ResolvedTransportSettings>,
    /// Authoritative target-aware write pacing after model-profile overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_write_pacing: Option<WritePacing>,
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

const fn is_false(value: &bool) -> bool {
    !*value
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
    PortReconfigured,
    PortRemoved,
    ControlGranted,
    ControlReleased,
    ControlRevoked,
    ControlExpired,
    RunStarted,
    RunEnded,
    RunAborted,
    TriggerStarted,
    TriggerCompleted,
    TriggerCancelled,
    TriggerFailed,
    Break,
    Checkpoint,
    LoggingDegraded,
    Gap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub port: String,
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
    pub port: String,
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
        ports: Vec<String>,
    },
    AcquireControl {
        request_id: Uuid,
        port: String,
        mode: ControlMode,
        ttl_ms: u64,
    },
    RenewControl {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
    },
    ReleaseControl {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
    },
    CancelAcquire {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
    },
    Write {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        #[serde(with = "base64_bytes")]
        data: Vec<u8>,
        operation_id: Option<Uuid>,
        /// Optional optimistic Run boundary for one physical write. Agent
        /// adapters set this to the Run they own. A
        /// cooperative Human write must set it to the current Agent Run so its
        /// authorization and cross-connection idempotency remain Run-scoped.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_run_id: Option<Uuid>,
        /// Per-write pacing override. Omission uses configured pacing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pacing: Option<WritePacing>,
        /// Optional human-readable purpose for this physical write. Agent
        /// adapters attach this to command writes so operators can review a
        /// Run by intent before expanding the exact serial payload.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        /// Receive boundaries used by the command capture that follows this
        /// physical write. Empty for raw input and quiet-only commands.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        command_capture_matchers: Vec<CommandCaptureMatcher>,
        /// Optional durable grouping metadata for `command_sequence` writes.
        /// Ordinary commands and Human writes omit this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        command_sequence: Option<CommandSequenceAuditContext>,
        /// Optional daemon-enforced, fail-closed serial-context boundary.
        /// Agent adapters use it for ordinary commands and dependent sequence
        /// writes. Human clients omit it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_precondition: Option<SequenceWritePrecondition>,
        /// Explicit Human-only injection while an Agent owns control. This
        /// bypasses takeover but does not transfer or revoke the Agent lease.
        /// Daemons must reject the flag for non-Human actors, a missing or
        /// mismatched `expected_run_id`, and every other ownership situation.
        #[serde(default, skip_serializing_if = "is_false")]
        cooperative: bool,
    },
    /// Assert the UART BREAK condition. This is a physical line signal, not a
    /// control byte; Ctrl-C/Ctrl-D/Ctrl-Z remain ordinary write payloads.
    SendBreak {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        duration_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_run_id: Option<Uuid>,
        /// Optional daemon-enforced, fail-closed serial-context boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_precondition: Option<SequenceWritePrecondition>,
    },
    TriggerStart {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        /// Explicit daemon identity prevents a delayed request from crossing a
        /// daemon restart.
        daemon_epoch: Uuid,
        /// Explicit physical-session identity prevents a delayed request from
        /// crossing a serial close/reopen boundary.
        generation: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<Uuid>,
        /// Agent adapters bind a Trigger to the Run they own. Human/script
        /// clients may omit this and retain lease-only authorization.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_run_id: Option<Uuid>,
        /// Optional daemon-enforced, fail-closed serial-context boundary.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_precondition: Option<SequenceWritePrecondition>,
        spec: TriggerSpec,
    },
    TriggerStatus {
        request_id: Uuid,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    },
    TriggerCancel {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    },
    StartRun {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        label: String,
        #[serde(default)]
        metadata: BTreeMap<String, Value>,
    },
    EndRun {
        request_id: Uuid,
        port: String,
        control_id: Uuid,
        fence: u64,
        run_id: Uuid,
    },
    Checkpoint {
        request_id: Uuid,
        port: String,
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
            | Self::SendBreak { request_id, .. }
            | Self::TriggerStart { request_id, .. }
            | Self::TriggerStatus { request_id, .. }
            | Self::TriggerCancel { request_id, .. }
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
    HelloAccepted { actor: Actor },
    Attached { ports: Vec<String> },
    Detached { ports: Vec<String> },
    ControlGranted { lease: ControlLease },
    ControlQueued { position: usize },
    ControlRenewed { lease: ControlLease },
    ControlReleased,
    AcquireCancelled { removed: bool },
    WriteAccepted { event_seq: u64 },
    BreakSent { event_seq: u64 },
    TriggerStarted { trigger: Box<TriggerInfo> },
    TriggerStatus { trigger: Box<TriggerInfo> },
    TriggerCancelled { trigger: Box<TriggerInfo> },
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
    },
    Snapshot {
        port: Box<SlotSnapshot>,
    },
    ReplayBegin {
        port: String,
        from_seq: u64,
        through_seq: u64,
    },
    Ready {
        port: String,
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
        port: String,
        requested_after_seq: Option<u64>,
        first_available_seq: Option<u64>,
        head_seq: u64,
        reason: GapReason,
    },
    Lagged {
        port: String,
        from_seq: u64,
        to_seq: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    NotFound,
    Conflict,
    ControlRequired,
    StaleFence,
    PortOffline,
    CursorAhead,
    SequenceBoundaryChanged,
    ResourceExhausted,
    IdempotencyExpired,
    ConfigRevisionMismatch,
    ProfileChangeBusy,
    PortNotFound,
    PortBusy,
    PortAccessDenied,
    PortIo,
    BreakUnsupported,
    RegexInvalid,
    QueryBudgetExceeded,
    Unavailable,
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
    /// Shared component protocol generation served by this daemon.
    #[serde(default)]
    pub protocol_version: u16,
}

/// Local identity exposed by the HTTP MCP adapter so the unified launcher can
/// distinguish the matching adapter from an unrelated listener on its fixed
/// loopback port.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpHealthResponse {
    pub status: String,
    pub service: String,
    pub protocol_version: u16,
    pub pid: u32,
    pub seriald_endpoint: String,
    pub seriald_server_id: Uuid,
    pub seriald_daemon_epoch: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    /// Shared component protocol generation served by this daemon.
    #[serde(default)]
    pub protocol_version: u16,
    #[serde(default)]
    pub config_revision: u64,
    /// True when guarded `command_sequence` writes are enforced atomically by
    /// the port actor.
    #[serde(default, skip_serializing_if = "is_false")]
    pub sequence_write_precondition_supported: bool,
    /// True when `sequence_precondition` is enforced atomically for every
    /// Agent physical action: ordinary Write, BREAK, and Trigger start.
    #[serde(default, skip_serializing_if = "is_false")]
    pub serial_context_precondition_supported: bool,
    pub ports: Vec<SlotSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurePortsRequest {
    pub ports: Vec<SlotConfig>,
    /// Audit label for the local UI making the change, for example
    /// `human:serialctl` or `human:desktop`.
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurePortsResponse {
    pub ports: Vec<SlotSnapshot>,
    #[serde(default)]
    pub config_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportProfileListResponse {
    pub profiles: Vec<TransportProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureTransportProfilesRequest {
    pub profiles: Vec<TransportProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureTransportProfilesResponse {
    pub profiles: Vec<TransportProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Read model for the configured model-profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelProfileListResponse {
    pub profiles: Vec<ModelProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Full replacement of the model-profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureModelProfilesRequest {
    pub profiles: Vec<ModelProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureModelProfilesResponse {
    pub profiles: Vec<ModelProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

/// One discoverable, retained port/daemon-epoch journal archive.
///
/// Segment timestamps describe when the first and last retained segments were
/// created. Event timestamps remain available from the bounded event query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveSummary {
    pub port: String,
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
    /// Retained port/epoch summaries, newest archive first. The daemon orders
    /// by `last_segment_wall_time_ns`; clients may preserve this order as a
    /// stable rank when sequence numbers cannot be compared across epochs.
    pub archives: Vec<ArchiveSummary>,
    /// More retained archives exist than fit in this response.
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventQuery {
    pub epoch: Option<Uuid>,
    pub after_seq: Option<u64>,
    /// Inclusive upper sequence bound. Together with `after_seq`, this selects
    /// the exact half-open/closed interval `(after_seq, through_seq]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through_seq: Option<u64>,
    pub before_wall_time_ns: Option<i64>,
    pub after_wall_time_ns: Option<i64>,
    pub direction: Option<Direction>,
    pub kind: Option<EventKind>,
    pub actor_id: Option<String>,
    pub run_id: Option<Uuid>,
    pub operation_id: Option<Uuid>,
    pub contains: Option<String>,
    /// Bounded UTF-8 regular expression. Mutually exclusive with `contains`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    pub limit_events: Option<usize>,
    pub limit_bytes: Option<usize>,
}

/// Maximum number of OR-ed conditions in one Monitor Job.
pub const MAX_MONITOR_MATCHERS: usize = 16;
/// Maximum UTF-8 bytes in one Monitor condition.
pub const MAX_MONITOR_PATTERN_BYTES: usize = 4_096;
/// Maximum UTF-8 bytes across all conditions in one Monitor Job.
pub const MAX_MONITOR_TOTAL_PATTERN_BYTES: usize = 16_384;

/// One condition in a long-lived Monitor Job. Conditions are evaluated over
/// contiguous live RX bytes and all conditions in a Job have OR semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MonitorMatcher {
    Contains(String),
    Regex(String),
}

/// Identifies the configured condition that contributed to one Incident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorMatch {
    pub index: usize,
    pub matcher: MonitorMatcher,
}

/// A long-lived, daemon-owned OR matcher over one port's live RX stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSpec {
    pub port: String,
    pub matchers: Vec<MonitorMatcher>,
    /// First event considered is strictly after this cursor. When omitted,
    /// seriald resolves it to the port head at creation/update time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_cursor: Option<Cursor>,
    #[serde(default)]
    pub severity: MonitorSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Fixed grouping window beginning at the first match. Further matches in
    /// the window expand the same Incident instead of creating more turns.
    #[serde(default = "default_monitor_debounce_ms")]
    pub debounce_ms: u64,
    /// Minimum delay after an Incident before another may be emitted.
    #[serde(default = "default_monitor_cooldown_ms")]
    pub cooldown_ms: u64,
    /// Optional wall-clock lifetime of the Monitor Job.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

fn default_monitor_debounce_ms() -> u64 {
    250
}

fn default_monitor_cooldown_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MonitorSeverity {
    Info,
    #[default]
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorStatus {
    Running,
    Completed,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorView {
    pub id: Uuid,
    pub revision: u64,
    pub spec: MonitorSpec,
    pub status: MonitorStatus,
    pub created_wall_time_ns: i64,
    pub started_wall_time_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_wall_time_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_wall_time_ns: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_cursor: Option<Cursor>,
    #[serde(default)]
    pub incident_count: u64,
    #[serde(default)]
    pub unacked_incident_count: u64,
    #[serde(default)]
    pub gap_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateMonitorRequest {
    /// Idempotency key and stable Monitor ID. Repeating the same request
    /// returns the existing Monitor; reusing it with a different spec fails.
    pub request_id: Uuid,
    pub spec: MonitorSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateMonitorRequest {
    pub spec: MonitorSpec,
    /// Optimistic concurrency guard from `MonitorView.revision`. A stale
    /// replacement must not silently revive a Monitor another operator stopped
    /// or overwrite a newer matcher.
    pub expected_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorResponse {
    pub monitor: MonitorView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorListResponse {
    pub monitors: Vec<MonitorView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorIncident {
    pub id: Uuid,
    /// Stable, monotonically increasing cursor within one Monitor Job.
    pub incident_seq: u64,
    pub monitor_id: Uuid,
    pub port: String,
    pub daemon_epoch: Uuid,
    pub seq_start: u64,
    pub seq_end: u64,
    pub wall_time_start_ns: i64,
    pub wall_time_end_ns: i64,
    pub severity: MonitorSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Distinct configured conditions observed while this Incident was
    /// grouped. Their indexes refer to `MonitorSpec.matchers`.
    pub matches: Vec<MonitorMatch>,
    pub preview: String,
    pub evidence_cursor: Cursor,
    pub evidence_ref: String,
    pub created_wall_time_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acked_wall_time_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorIncidentResponse {
    pub incident: MonitorIncident,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorIncidentListResponse {
    pub incidents: Vec<MonitorIncident>,
    /// Decimal incident high-water sequence; pass it as
    /// `after_incident_seq`. This advances across filtered ACKed Incidents and
    /// can therefore be present even when `incidents` is empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u64>,
    pub truncated: bool,
    /// Oldest retained sequence for this Monitor, when any Incident remains.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_available_incident_seq: Option<u64>,
    /// True when `after_incident_seq` precedes retained history and results
    /// begin after an irrecoverable retention gap.
    #[serde(default)]
    pub retention_gap: bool,
}

/// Read-only journal health and retention metrics. Gathering this information
/// never probes a target, opens a port, or writes serial bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalDiagnostics {
    pub usage_bytes: u64,
    pub max_bytes: u64,
    pub retention_target_bytes: u64,
    pub segment_max_bytes: u64,
    pub writer_queue_capacity: usize,
    pub writer_queue_remaining: usize,
    pub archive_count: usize,
    pub logging: LoggingState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlotDiagnostics {
    pub snapshot: SlotSnapshot,
    pub subscriber_count: usize,
    pub subscriber_lag_events: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonDiagnosticsResponse {
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    pub uptime_ms: u64,
    pub config_revision: u64,
    pub websocket_connections: usize,
    pub websocket_limit: usize,
    pub journal: JournalDiagnostics,
    pub ports: Vec<SlotDiagnostics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageDiagnosticsResponse {
    pub journal: JournalDiagnostics,
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
    pub port: String,
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
            port: event.port.clone(),
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
            port: self.port,
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

mod option_base64_bytes {
    use super::*;
    use serde::{Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match bytes {
            Some(bytes) => serializer.serialize_some(&BASE64.encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = Option::<String>::deserialize(deserializer)?;
        encoded
            .map(|encoded| BASE64.decode(encoded).map_err(D::Error::custom))
            .transpose()
    }
}

mod base64_byte_patterns {
    use super::*;
    use serde::{Deserializer, Serializer, de::Error as _};

    pub fn serialize<S>(patterns: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded: Vec<String> = patterns
            .iter()
            .map(|pattern| BASE64.encode(pattern))
            .collect();
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|encoded| BASE64.decode(encoded).map_err(D::Error::custom))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(direction: Direction, kind: EventKind, data: Vec<u8>) -> TimelineEvent {
        TimelineEvent {
            port: "slot-1".into(),
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
            port: "slot-1".into(),
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
        // Generic settings do not guess a model-specific U-Boot prompt.
        assert!(settings.uboot_prompt.is_none());
        assert_eq!(settings.write_chunk_size, 1);
        assert_eq!(settings.write_chunk_delay_ms, 1);
        assert!(settings.probe.is_none());
    }

    fn model_profile() -> ModelProfile {
        ModelProfile {
            name: "sigmastar-evb".into(),
            model_names: vec!["SigmaStar EVB 1.0".into()],
            shell_prompt: Some("root@sigmastar:/# ".into()),
            uboot_prompt: Some("SigmaStar =>".into()),
            write_eol: Some("\n".into()),
            echo: Some(EchoMode::Off),
            write_chunk_size: Some(2),
            write_chunk_delay_ms: Some(3),
        }
    }

    #[test]
    fn model_profile_overrides_generic_behavior_baseline() {
        let profile = model_profile();
        // The model profile supplies all target-specific behavior.
        let resolved = resolve_model_settings(&SerialSettings::default(), Some(&profile));
        assert_eq!(resolved.shell_prompt.as_deref(), Some("root@sigmastar:/# "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("SigmaStar =>"));
        assert_eq!(resolved.write_eol, "\n");
        assert_eq!(resolved.echo, EchoMode::Off);
        assert_eq!(
            resolved.write_pacing,
            WritePacing {
                chunk_size: 2,
                chunk_delay_ms: 3,
            }
        );

        // Once attached, the model owns prompts as well as line ending and
        // echo behavior, so changing the bound model cannot retain stale
        // prompt behavior from the previous device.
        let settings = SerialSettings {
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("U-Boot> ".into()),
            write_eol: "\r\n".into(),
            echo: EchoMode::Auto,
            ..SerialSettings::default()
        };
        let resolved = resolve_model_settings(&settings, Some(&profile));
        assert_eq!(resolved.shell_prompt.as_deref(), Some("root@sigmastar:/# "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("SigmaStar =>"));
        assert_eq!(resolved.write_eol, "\n");
        assert_eq!(resolved.echo, EchoMode::Off);

        // Attaching a model makes that profile authoritative for prompt
        // presence too. An omitted prompt means "not configured", rather
        // than inheriting a stale prompt from the previously attached model.
        let promptless = ModelProfile {
            name: "promptless".into(),
            model_names: Vec::new(),
            shell_prompt: None,
            uboot_prompt: None,
            write_eol: None,
            echo: None,
            write_chunk_size: None,
            write_chunk_delay_ms: None,
        };
        let resolved = resolve_model_settings(&settings, Some(&promptless));
        assert!(resolved.shell_prompt.is_none());
        assert!(resolved.uboot_prompt.is_none());
        assert_eq!(resolved.write_eol, "\r\n");
        assert_eq!(resolved.echo, EchoMode::Auto);
    }

    #[test]
    fn transport_profile_overrides_only_physical_uart_fields() {
        let settings = SerialSettings {
            baud_rate: 9_600,
            data_bits: DataBits::Seven,
            parity: Parity::Odd,
            stop_bits: StopBits::Two,
            flow_control: FlowControl::Software,
            dtr: true,
            rts: true,
            write_eol: "\n".into(),
            echo: EchoMode::Off,
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("U-Boot> ".into()),
            write_chunk_size: 3,
            write_chunk_delay_ms: 7,
            auto_open: false,
            probe: None,
        };
        let profile = TransportProfile {
            name: "station-fast".into(),
            baud_rate: 921_600,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        };

        let resolved = resolve_transport_settings(&settings, Some(&profile));
        assert_eq!(resolved.baud_rate, 921_600);
        assert_eq!(resolved.data_bits, DataBits::Eight);
        assert!(resolved.auto_open);

        let applied = apply_transport_profile(&settings, Some(&profile));
        assert_eq!(applied.baud_rate, 921_600);
        assert_eq!(applied.write_eol, "\n");
        assert_eq!(applied.echo, EchoMode::Off);
        assert_eq!(applied.shell_prompt.as_deref(), Some("/ # "));
        assert_eq!(applied.uboot_prompt.as_deref(), Some("U-Boot> "));
        assert_eq!(applied.write_chunk_size, 3);
        assert_eq!(applied.write_chunk_delay_ms, 7);
    }

    #[test]
    fn device_settings_without_profile_use_the_generic_baseline() {
        let settings = SerialSettings {
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("boot=> ".into()),
            ..SerialSettings::default()
        };
        let resolved = resolve_model_settings(&settings, None);
        assert_eq!(resolved.shell_prompt.as_deref(), Some("/ # "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("boot=> "));
        assert_eq!(resolved.write_eol, "\r");
        assert_eq!(resolved.echo, EchoMode::On);
    }

    #[test]
    fn slot_config_uses_the_port_as_its_only_identity() {
        let value = serde_json::json!({
            "port": "COM3",
            "transport_profile": "generic-115200",
            "model_profile": "TL-AS7230",
            "model_name": "TL-AS7230-W 1.0",
            "enabled": true,
        });
        let slot: SlotConfig = serde_json::from_value(value).unwrap();
        assert_eq!(slot.port, "COM3");
        assert_eq!(slot.model_profile.as_deref(), Some("TL-AS7230"));
        assert_eq!(slot.model_name.as_deref(), Some("TL-AS7230-W 1.0"));
    }

    #[test]
    fn snapshot_omits_unset_effective_prompts_on_the_wire() {
        let json = serde_json::to_value(SlotSnapshot {
            config: SlotConfig {
                port: "COM3".into(),
                transport_profile: Some("generic-115200".into()),
                model_profile: None,
                model_name: None,
                enabled: true,
            },
            daemon_epoch: Uuid::new_v4(),
            head_seq: 0,
            ring_oldest_seq: None,
            generation: 0,
            endpoint_present: false,
            session_state: SessionState::Disabled,
            state_reason: None,
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
            effective_shell_prompt: None,
            effective_uboot_prompt: None,
            effective_write_eol: None,
            effective_echo: None,
            effective_transport: None,
            effective_write_pacing: None,
        })
        .unwrap();
        let object = json.as_object().unwrap();
        assert!(!object.contains_key("effective_shell_prompt"));
        assert!(!object.contains_key("effective_uboot_prompt"));
        assert!(!object.contains_key("effective_write_eol"));
        assert!(!object.contains_key("effective_echo"));
        let decoded: SlotSnapshot = serde_json::from_value(json).unwrap();
        assert!(decoded.effective_shell_prompt.is_none());
        assert!(decoded.effective_uboot_prompt.is_none());
        assert!(decoded.effective_write_eol.is_none());
        assert!(decoded.effective_echo.is_none());
    }

    #[test]
    fn write_pacing_round_trips_through_the_control_frame() {
        let expected_run_id = Uuid::new_v4();
        let message = ClientMessage::Write {
            request_id: Uuid::new_v4(),
            port: "slot-1".into(),
            control_id: Uuid::new_v4(),
            fence: 7,
            data: b"reboot\r".to_vec(),
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(expected_run_id),
            pacing: Some(WritePacing {
                chunk_size: 4,
                chunk_delay_ms: 10,
            }),
            description: Some("重启样机".into()),
            command_capture_matchers: Vec::new(),
            command_sequence: None,
            sequence_precondition: None,
            cooperative: false,
        };
        let frame = encode_client_control(&message).unwrap();
        assert_eq!(decode_client_control(&frame).unwrap(), message);
    }

    #[test]
    fn command_capture_sequence_context_and_precondition_round_trip() {
        let sequence_id = Uuid::new_v4();
        let message = ClientMessage::Write {
            request_id: Uuid::new_v4(),
            port: "slot-1".into(),
            control_id: Uuid::new_v4(),
            fence: 7,
            data: b"admin\r".to_vec(),
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(Uuid::new_v4()),
            pacing: None,
            description: Some("输入登录账号".into()),
            command_capture_matchers: vec![CommandCaptureMatcher {
                kind: CommandCaptureMatcherKind::Contains,
                value: "Password:".into(),
            }],
            command_sequence: Some(CommandSequenceAuditContext {
                sequence_id,
                description: "登录样机".into(),
                step_index: 0,
                step_count: 2,
            }),
            sequence_precondition: Some(SequenceWritePrecondition {
                cursor: Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 41,
                },
                expected_generation: 3,
                expected_tx_offset: 17,
            }),
            cooperative: false,
        };
        let encoded = serde_json::to_value(&message).unwrap();
        assert_eq!(
            encoded["command_sequence"]["sequence_id"],
            serde_json::json!(sequence_id)
        );
        assert_eq!(encoded["command_sequence"]["step_index"], 0);
        assert_eq!(encoded["command_capture_matchers"][0]["kind"], "contains");
        assert_eq!(encoded["command_capture_matchers"][0]["value"], "Password:");
        assert_eq!(encoded["sequence_precondition"]["expected_generation"], 3);
        assert_eq!(encoded["sequence_precondition"]["expected_tx_offset"], 17);
        assert_eq!(encoded["sequence_precondition"]["cursor"]["after_seq"], 41);
        assert_eq!(
            serde_json::from_value::<ClientMessage>(encoded).unwrap(),
            message
        );
    }

    #[test]
    fn status_uses_ports_and_defaults_optional_capabilities() {
        let status: StatusResponse = serde_json::from_value(serde_json::json!({
            "server_id": Uuid::new_v4(),
            "daemon_epoch": Uuid::new_v4(),
            "protocol_version": PROTOCOL_VERSION,
            "ports": []
        }))
        .unwrap();
        assert!(!status.sequence_write_precondition_supported);
        assert!(!status.serial_context_precondition_supported);
    }

    #[test]
    fn uart_break_round_trips_with_run_and_operation_boundaries() {
        let request_id = Uuid::new_v4();
        let message = ClientMessage::SendBreak {
            request_id,
            port: "slot-1".into(),
            control_id: Uuid::new_v4(),
            fence: 7,
            duration_ms: 250,
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(Uuid::new_v4()),
            sequence_precondition: Some(SequenceWritePrecondition {
                cursor: Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 9,
                },
                expected_generation: 2,
                expected_tx_offset: 17,
            }),
        };
        assert_eq!(message.request_id(), request_id);
        let frame = encode_client_control(&message).unwrap();
        assert_eq!(decode_client_control(&frame).unwrap(), message);

        let result = CommandResult::BreakSent { event_seq: 91 };
        let json = serde_json::to_value(&result).unwrap();
        assert_eq!(json["type"], "break_sent");
        assert_eq!(
            serde_json::from_value::<CommandResult>(json).unwrap(),
            result
        );
    }

    #[test]
    fn cooperative_write_round_trips() {
        let request_id = Uuid::new_v4();
        let control_id = Uuid::new_v4();
        let cooperative = ClientMessage::Write {
            request_id,
            port: "slot-1".into(),
            control_id,
            fence: 3,
            data: b"status\r".to_vec(),
            operation_id: None,
            expected_run_id: Some(Uuid::new_v4()),
            pacing: None,
            description: None,
            command_capture_matchers: Vec::new(),
            command_sequence: None,
            sequence_precondition: None,
            cooperative: true,
        };
        let encoded = serde_json::to_value(&cooperative).unwrap();
        assert_eq!(encoded["cooperative"], true);
        assert!(encoded["expected_run_id"].is_string());
        assert_eq!(
            serde_json::from_value::<ClientMessage>(encoded).unwrap(),
            cooperative
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
            port: "slot-1".into(),
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

    fn trigger_spec() -> TriggerSpec {
        TriggerSpec {
            initial_write: Some(vec![b'r', b'e', b'b', b'o', b'o', b't', b'\r']),
            start_contains: Some(vec![0x00, 0xff, b'B']),
            action: vec![b's', b'l', b'p'],
            interval_ms: 17,
            stop_contains: vec![b"U-Boot> ".to_vec(), vec![0x00, 0x80, 0xff]],
            timeout_ms: 4_000,
            max_fires: 123,
            pacing: Some(WritePacing {
                chunk_size: 3,
                chunk_delay_ms: 0,
            }),
        }
    }

    fn trigger_info(status: TriggerStatus) -> TriggerInfo {
        TriggerInfo {
            id: Uuid::new_v4(),
            owner: Actor {
                id: "agent:test".into(),
                label: "test adapter".into(),
                kind: ActorKind::Agent,
            },
            daemon_epoch: Uuid::new_v4(),
            generation: 9,
            control_id: Uuid::new_v4(),
            fence: 11,
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(Uuid::new_v4()),
            spec: trigger_spec(),
            status,
            start_seq: 41,
            end_seq: status.is_terminal().then_some(73),
            last_write_seq: Some(70),
            fires_confirmed: 12,
            tx_bytes_confirmed: 43,
            matched_pattern: status.is_matched().then(|| b"U-Boot> ".to_vec()),
        }
    }

    #[test]
    fn protocol_v5_exposes_multi_match_monitor_and_model_name_contracts() {
        assert_eq!(PROTOCOL_VERSION, 5);
    }

    #[test]
    fn trigger_spec_uses_base64_for_every_raw_byte_field() {
        let spec = trigger_spec();
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(
            json["initial_write"],
            BASE64.encode(spec.initial_write.as_ref().unwrap())
        );
        assert_eq!(
            json["start_contains"],
            BASE64.encode(spec.start_contains.as_ref().unwrap())
        );
        assert_eq!(json["action"], BASE64.encode(&spec.action));
        assert_eq!(
            json["stop_contains"],
            serde_json::json!([
                BASE64.encode(&spec.stop_contains[0]),
                BASE64.encode(&spec.stop_contains[1])
            ])
        );
        assert_eq!(serde_json::from_value::<TriggerSpec>(json).unwrap(), spec);
    }

    #[test]
    fn trigger_spec_defaults_are_bounded() {
        let decoded: TriggerSpec = serde_json::from_value(serde_json::json!({
            "action": BASE64.encode([0x00, 0xff]),
        }))
        .unwrap();
        assert!(decoded.initial_write.is_none());
        assert!(decoded.start_contains.is_none());
        assert_eq!(decoded.action, vec![0x00, 0xff]);
        assert_eq!(decoded.interval_ms, DEFAULT_TRIGGER_INTERVAL_MS);
        assert!(decoded.stop_contains.is_empty());
        assert_eq!(decoded.timeout_ms, DEFAULT_TRIGGER_TIMEOUT_MS);
        assert_eq!(decoded.max_fires, DEFAULT_TRIGGER_MAX_FIRES);
        assert!(decoded.pacing.is_none());
    }

    #[test]
    fn trigger_control_messages_round_trip() {
        let request_id = Uuid::new_v4();
        let daemon_epoch = Uuid::new_v4();
        let control_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let expected_run_id = Uuid::new_v4();
        let start = ClientMessage::TriggerStart {
            request_id,
            port: "slot-1".into(),
            control_id,
            fence: 17,
            daemon_epoch,
            generation: 5,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            sequence_precondition: Some(SequenceWritePrecondition {
                cursor: Cursor {
                    epoch: daemon_epoch,
                    after_seq: 9,
                },
                expected_generation: 5,
                expected_tx_offset: 17,
            }),
            spec: trigger_spec(),
        };
        assert_eq!(start.request_id(), request_id);
        assert_eq!(
            decode_client_control(&encode_client_control(&start).unwrap()).unwrap(),
            start
        );

        let trigger_id = Uuid::new_v4();
        let status = ClientMessage::TriggerStatus {
            request_id: Uuid::new_v4(),
            port: "slot-1".into(),
            daemon_epoch,
            generation: 5,
            trigger_id,
        };
        assert_eq!(
            decode_client_control(&encode_client_control(&status).unwrap()).unwrap(),
            status
        );

        let cancel = ClientMessage::TriggerCancel {
            request_id: Uuid::new_v4(),
            port: "slot-1".into(),
            control_id,
            fence: 17,
            daemon_epoch,
            generation: 5,
            trigger_id,
        };
        assert_eq!(
            decode_client_control(&encode_client_control(&cancel).unwrap()).unwrap(),
            cancel
        );
    }

    #[test]
    fn trigger_info_and_results_round_trip_with_base64_match() {
        let trigger = trigger_info(TriggerStatus::Matched);
        let json = serde_json::to_value(&trigger).unwrap();
        assert_eq!(
            json["matched_pattern"],
            BASE64.encode(trigger.matched_pattern.as_ref().unwrap())
        );
        assert_eq!(
            serde_json::from_value::<TriggerInfo>(json).unwrap(),
            trigger
        );

        for result in [
            CommandResult::TriggerStarted {
                trigger: Box::new(trigger.clone()),
            },
            CommandResult::TriggerStatus {
                trigger: Box::new(trigger.clone()),
            },
            CommandResult::TriggerCancelled {
                trigger: Box::new(trigger.clone()),
            },
        ] {
            let json = serde_json::to_value(&result).unwrap();
            assert_eq!(
                serde_json::from_value::<CommandResult>(json).unwrap(),
                result
            );
        }
    }

    #[test]
    fn trigger_status_and_lifecycle_event_tags_are_stable() {
        assert!(!TriggerStatus::Armed.is_terminal());
        assert!(!TriggerStatus::WaitingForStart.is_terminal());
        assert!(!TriggerStatus::Running.is_terminal());
        assert!(!TriggerStatus::Stopping.is_terminal());
        assert!(TriggerStatus::Matched.is_terminal());
        assert!(TriggerStatus::Matched.is_matched());
        assert!(TriggerStatus::TimedOut.is_terminal());
        assert!(!TriggerStatus::TimedOut.is_matched());

        for (kind, expected) in [
            (EventKind::TriggerStarted, "trigger_started"),
            (EventKind::TriggerCompleted, "trigger_completed"),
            (EventKind::TriggerCancelled, "trigger_cancelled"),
            (EventKind::TriggerFailed, "trigger_failed"),
        ] {
            assert_eq!(
                serde_json::to_value(kind).unwrap(),
                serde_json::json!(expected)
            );
        }
    }
}
