//! Stable, transport-independent contracts shared by `seriald` and its clients.
//!
//! WebSocket binary frames use a small envelope:
//! `[tag: u8][header_len: u32 big-endian][JSON header][raw bytes]`.
//! Control messages use tag `0x01`; device RX and confirmed TX use `0x02`
//! and `0x03`. Raw serial bytes are never converted to text by this crate.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

/// WebSocket wire generation. v3 adds physical BREAK, stable serial error
/// codes, effective Transport/Device settings, and bounded regex queries.
/// Those enum additions are intentionally rejected by v2 peers instead of
/// allowing a mixed-version connection to fail later while debugging.
pub const PROTOCOL_VERSION: u16 = 3;
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

/// Reusable physical UART configuration. `SlotConfig::settings` remains a
/// complete compatibility snapshot, while a matching catalog entry is the
/// authoritative source for these transport fields.
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

/// Physical UART settings after resolving a Slot's transport-profile binding.
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

/// Applies only physical UART fields, preserving the Slot's legacy
/// device-behavior and write-pacing snapshot.
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

/// One bounded, device-agnostic reaction to the live serial byte stream.
///
/// The daemon arms its literal-byte matchers before performing the optional
/// initial write. It then sends `action` at the requested interval, stopping
/// when a `stop_contains` pattern is observed or either hard bound is reached.
/// Device-model meaning and higher-level workflows deliberately remain in
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
    /// write. When absent, the Slot's effective write pacing is used.
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
    pub id: String,
    pub display_name: String,
    pub port: String,
    pub profile: String,
    /// Name of the device-model profile this Slot is attached to. Prompts and
    /// similar device behavior belong to the device model and override the
    /// generic/legacy behavior baseline stored in Slot settings.
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
    /// Optional target-specific UART write pacing. These fields belong to the
    /// DUT because they compensate for how quickly that target consumes input,
    /// not for a property of the host COM port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_chunk_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_chunk_delay_ms: Option<u64>,
}

/// One node in the station-owned DUT model catalog.
///
/// Model identity is deliberately separate from [`DeviceProfile`]. A model
/// name describes the hardware connected to a Slot, while a Device Profile
/// changes prompts, line endings, echo handling, and write pacing. Keeping the
/// two catalogs independent lets Human and Agent clients correct model
/// identity without implicitly changing serial behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceModel {
    /// Stable catalog identity used by bindings and parent references.
    pub id: String,
    /// Human-readable model or family name. Names may repeat at different
    /// levels (for example a family and its base variant can share a label).
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

/// How the connected DUT's model identity was confirmed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelConfirmationMethod {
    Serial,
    Telnet,
    Web,
    Human,
    Other,
}

/// Persisted assignment of one configured Slot to one catalog model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotModelBinding {
    pub slot_id: String,
    pub model_id: String,
    pub confirmation_method: ModelConfirmationMethod,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Wall-clock time of the latest assignment, in Unix nanoseconds.
    pub updated_wall_time_ns: i64,
    /// Bounded caller/audit label such as `human:serialctl` or
    /// `agent:serial-mcp`. Authentication still comes from the bearer role;
    /// this field records the declared workflow source only.
    pub source: String,
}

/// Device-interaction settings after applying the attached device profile to
/// the Slot's generic/legacy baseline. Generic transport defaults deliberately
/// do not guess device-specific Shell or U-Boot prompts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedDeviceSettings {
    pub shell_prompt: Option<String>,
    pub uboot_prompt: Option<String>,
    pub write_eol: String,
    pub echo: EchoMode,
    pub write_pacing: WritePacing,
}

/// Resolves the effective device behavior for one Slot. An attached
/// device-model profile owns Shell/U-Boot prompt presence as well as any
/// provided EOL/echo overrides. Without a profile, old Slot prompt values
/// remain compatible.
///
/// `shell_prompt`/`uboot_prompt` are optional in the Slot settings, so a
/// profile can supply them when the Slot does not. `write_eol` and `echo`
/// remain concrete in the Slot for backward-compatible generic defaults.
pub fn resolve_device_settings(
    settings: &SerialSettings,
    device_profile: Option<&DeviceProfile>,
) -> ResolvedDeviceSettings {
    let shell_prompt = match device_profile {
        Some(profile) => profile.shell_prompt.clone(),
        None => settings.shell_prompt.clone(),
    };
    let uboot_prompt = match device_profile {
        Some(profile) => profile.uboot_prompt.clone(),
        None => settings.uboot_prompt.clone(),
    };
    ResolvedDeviceSettings {
        shell_prompt,
        uboot_prompt,
        write_eol: device_profile
            .and_then(|profile| profile.write_eol.clone())
            .unwrap_or_else(|| settings.write_eol.clone()),
        echo: device_profile
            .and_then(|profile| profile.echo)
            .unwrap_or(settings.echo),
        write_pacing: WritePacing {
            chunk_size: device_profile
                .and_then(|profile| profile.write_chunk_size)
                .unwrap_or(settings.write_chunk_size),
            chunk_delay_ms: device_profile
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
    /// Stable classification for `state_reason`. Older daemons only expose
    /// the human-readable text.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_code: Option<ErrorCode>,
    pub target_activity: TargetActivity,
    pub last_rx_wall_time_ns: Option<i64>,
    pub rx_offset: u64,
    pub tx_offset: u64,
    /// Total reader bytes dropped during this daemon epoch for this Slot.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub rx_overflow_bytes: u64,
    pub control: Option<ControlLease>,
    pub active_run: Option<RunInfo>,
    /// Current daemon-owned Trigger Job, if any. Older snapshots omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_trigger: Option<TriggerInfo>,
    pub logging: LoggingState,
    /// Authoritative prompts after resolving the attached device profile (or
    /// legacy Slot values when no model is attached). Omitted on the wire when
    /// unset; current clients use effective EOL/echo presence to distinguish
    /// an authoritative `None` from an older daemon lacking this bundle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_uboot_prompt: Option<String>,
    /// Effective line ending and echo policy after applying the attached
    /// device-model profile. Optional on the wire for older-daemon
    /// compatibility; current daemons always publish both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_write_eol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_echo: Option<EchoMode>,
    /// Authoritative physical UART settings. Present on current daemons;
    /// optional on the wire so old persisted/test snapshots still decode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_transport: Option<ResolvedTransportSettings>,
    /// Authoritative target-aware write pacing after Device Profile overrides.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_write_pacing: Option<WritePacing>,
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

const fn is_false(value: &bool) -> bool {
    !*value
}

/// Deserializes an optional JSON property while preserving whether it was
/// present with a `null` value. Serde normally maps both a missing property
/// and an explicit `null` to `None` for `Option<Option<T>>`.
fn deserialize_present_option<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
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
        /// Optional optimistic Run boundary for one physical write. New Agent
        /// adapters set this to the Run they own; ordinary human and legacy
        /// clients may omit it and retain lease-only write authorization. A
        /// cooperative Human write must set it to the current Agent Run so its
        /// authorization and cross-connection idempotency remain Run-scoped.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_run_id: Option<Uuid>,
        /// Per-write pacing override. Older clients omit the field and keep
        /// using the Slot's configured pacing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pacing: Option<WritePacing>,
        /// Optional human-readable purpose for this physical write. Agent
        /// adapters attach this to command writes so operators can review a
        /// Run by intent before expanding the exact serial payload. Older
        /// clients omit it and retain the original wire behavior.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
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
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        duration_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected_run_id: Option<Uuid>,
    },
    TriggerStart {
        request_id: Uuid,
        slot_id: String,
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
        spec: TriggerSpec,
    },
    TriggerStatus {
        request_id: Uuid,
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    },
    TriggerCancel {
        request_id: Uuid,
        slot_id: String,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
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
    HelloAccepted { actor: Actor, role: Role },
    Attached { slots: Vec<String> },
    Detached { slots: Vec<String> },
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
    /// WebSocket wire generation served by this daemon. Missing on pre-0.4
    /// HTTP responses and decoded as zero by current clients.
    #[serde(default)]
    pub protocol_version: u16,
    /// Whether clients must send a bearer credential. Missing on older HTTP
    /// responses and therefore decoded as `true` to preserve their secure
    /// behavior.
    #[serde(default = "default_auth_required")]
    pub auth_required: bool,
}

const fn default_auth_required() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusResponse {
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    /// WebSocket wire generation served by this daemon.
    #[serde(default)]
    pub protocol_version: u16,
    #[serde(default)]
    pub config_revision: u64,
    pub slots: Vec<SlotSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigureSlotsRequest {
    pub slots: Vec<SlotConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigureSlotsResponse {
    pub slots: Vec<SlotSnapshot>,
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

/// Read model for the configured device-model profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceProfileListResponse {
    pub profiles: Vec<DeviceProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Full replacement of the device-model profile catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceProfilesRequest {
    pub profiles: Vec<DeviceProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceProfilesResponse {
    pub profiles: Vec<DeviceProfile>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Authoritative model tree plus the current per-Slot assignments.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceModelListResponse {
    pub models: Vec<DeviceModel>,
    pub bindings: Vec<SlotModelBinding>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Full replacement of the model catalog. Existing bindings are retained and
/// therefore prevent deletion of an assigned model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceModelsRequest {
    pub models: Vec<DeviceModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigureDeviceModelsResponse {
    pub models: Vec<DeviceModel>,
    pub bindings: Vec<SlotModelBinding>,
    #[serde(default)]
    pub config_revision: u64,
}

/// Atomically attach, replace, or remove one Slot's model assignment.
///
/// `model_id = null` detaches. When `create_if_missing` is true, the remaining
/// model fields describe a catalog leaf to create in the same configuration
/// transaction before it is bound. `update_existing` instead patches the
/// existing node currently bound to this Slot; it requires exact revision and
/// binding guards. `expected_current` is intentionally a
/// nested Option: an omitted key disables this guard, JSON `null` expects an
/// unbound Slot, and a string expects that exact current model ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetSlotDeviceModelRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub create_if_missing: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub update_existing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub clear_parent: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub clear_aliases: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmation_method: Option<ModelConfirmationMethod>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_current: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetSlotDeviceModelResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding: Option<SlotModelBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<DeviceModel>,
    #[serde(default)]
    pub created: bool,
    /// Slots whose bindings refer to the returned model after this transaction.
    /// Updating one shared catalog node changes its metadata for every listed
    /// Slot even though only the path Slot authorizes the mutation.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub affected_slots: Vec<String>,
    #[serde(default)]
    pub config_revision: u64,
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

/// A long-lived, daemon-owned matcher over one Slot's live RX stream.
///
/// `contains` and `regex` are mutually exclusive. They are strings rather
/// than device-profile fields because monitors describe one observation job,
/// not the DUT's shell protocol. Matching uses the byte representation of the
/// UTF-8 strings and spans contiguous RX timeline events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitorSpec {
    pub slot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
    /// First event considered is strictly after this cursor. When omitted,
    /// seriald resolves it to the Slot head at creation/update time.
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
    /// Notification freshness deadline. Expired outbox entries remain
    /// inspectable but are never delivered to a webhook sink.
    #[serde(default = "default_monitor_event_ttl_ms")]
    pub event_ttl_ms: u64,
}

fn default_monitor_debounce_ms() -> u64 {
    250
}

fn default_monitor_cooldown_ms() -> u64 {
    30_000
}

fn default_monitor_event_ttl_ms() -> u64 {
    10 * 60 * 1_000
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
    pub slot_id: String,
    pub daemon_epoch: Uuid,
    pub seq_start: u64,
    pub seq_end: u64,
    pub wall_time_start_ns: i64,
    pub wall_time_end_ns: i64,
    pub severity: MonitorSeverity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub preview: String,
    pub evidence_cursor: Cursor,
    pub evidence_ref: String,
    pub created_wall_time_ns: i64,
    /// Notification freshness deadline. This only expires webhook/outbox
    /// delivery; the Incident itself is retained until bounded retention
    /// removes it. At a hard bound, the oldest summary yields to newer evidence
    /// regardless of ACK.
    pub expires_wall_time_ns: i64,
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

/// Strict CloudEvents-shaped notification retained by seriald until ACK,
/// webhook delivery, or TTL expiry. The serial byte stream remains in the
/// journal and is referenced through the Incident evidence fields.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorCloudEvent {
    pub specversion: String,
    pub id: String,
    pub source: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub subject: String,
    pub time: String,
    pub datacontenttype: String,
    /// CloudEvents extension attribute. Consumers must not start stale work
    /// after this RFC3339 timestamp.
    pub expiresat: String,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonitorOutboxStatus {
    Pending,
    Delivered,
    Acknowledged,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorOutboxEvent {
    pub outbox_seq: u64,
    pub event: MonitorCloudEvent,
    pub status: MonitorOutboxStatus,
    pub created_wall_time_ns: i64,
    pub expires_wall_time_ns: i64,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorOutboxListResponse {
    pub events: Vec<MonitorOutboxEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<u64>,
    pub truncated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorOutboxEventResponse {
    pub event: MonitorOutboxEvent,
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
    pub slots: Vec<SlotDiagnostics>,
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

    #[test]
    fn legacy_health_response_defaults_to_authentication_required() {
        let health: HealthResponse = serde_json::from_value(serde_json::json!({
            "status": "ok",
            "server_id": Uuid::nil(),
            "daemon_epoch": Uuid::nil(),
            "uptime_ms": 1,
            "protocol_version": PROTOCOL_VERSION
        }))
        .unwrap();
        assert!(health.auth_required);
    }

    fn device_profile() -> DeviceProfile {
        DeviceProfile {
            name: "sigmastar-evb".into(),
            shell_prompt: Some("root@sigmastar:/# ".into()),
            uboot_prompt: Some("SigmaStar =>".into()),
            write_eol: Some("\n".into()),
            echo: Some(EchoMode::Off),
            write_chunk_size: Some(2),
            write_chunk_delay_ms: Some(3),
        }
    }

    #[test]
    fn device_profile_overrides_generic_behavior_baseline() {
        let profile = device_profile();
        // Profile supplies everything the Slot leaves unset.
        let resolved = resolve_device_settings(&SerialSettings::default(), Some(&profile));
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
        // echo behavior. This prevents a stale legacy Slot prompt from
        // surviving when the physical station changes device models.
        let settings = SerialSettings {
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("U-Boot> ".into()),
            write_eol: "\r\n".into(),
            echo: EchoMode::Auto,
            ..SerialSettings::default()
        };
        let resolved = resolve_device_settings(&settings, Some(&profile));
        assert_eq!(resolved.shell_prompt.as_deref(), Some("root@sigmastar:/# "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("SigmaStar =>"));
        assert_eq!(resolved.write_eol, "\n");
        assert_eq!(resolved.echo, EchoMode::Off);

        // Attaching a model makes that profile authoritative for prompt
        // presence too. An omitted prompt means "not configured", rather
        // than inheriting a stale prompt from the previously attached model.
        let promptless = DeviceProfile {
            name: "promptless".into(),
            shell_prompt: None,
            uboot_prompt: None,
            write_eol: None,
            echo: None,
            write_chunk_size: None,
            write_chunk_delay_ms: None,
        };
        let resolved = resolve_device_settings(&settings, Some(&promptless));
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
    fn device_settings_without_profile_match_legacy_behavior() {
        // Regression: a configuration without device profiles resolves to the
        // same effective values as before profiles existed.
        let settings = SerialSettings {
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("legacy=> ".into()),
            ..SerialSettings::default()
        };
        let resolved = resolve_device_settings(&settings, None);
        assert_eq!(resolved.shell_prompt.as_deref(), Some("/ # "));
        assert_eq!(resolved.uboot_prompt.as_deref(), Some("legacy=> "));
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
        // ...and an older daemon's snapshot without the keys still decodes.
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
            slot_id: "slot-1".into(),
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
            cooperative: false,
        };
        let frame = encode_client_control(&message).unwrap();
        assert_eq!(decode_client_control(&frame).unwrap(), message);
    }

    #[test]
    fn uart_break_round_trips_with_run_and_operation_boundaries() {
        let request_id = Uuid::new_v4();
        let message = ClientMessage::SendBreak {
            request_id,
            slot_id: "slot-1".into(),
            control_id: Uuid::new_v4(),
            fence: 7,
            duration_ms: 250,
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: Some(Uuid::new_v4()),
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
                expected_run_id: None,
                pacing: None,
                description: None,
                cooperative: false,
            }
        );
    }

    #[test]
    fn cooperative_write_is_additive_and_legacy_writes_default_to_false() {
        let request_id = Uuid::new_v4();
        let control_id = Uuid::new_v4();
        let legacy = serde_json::json!({
            "type": "write",
            "request_id": request_id,
            "slot_id": "slot-1",
            "control_id": control_id,
            "fence": 3,
            "data": BASE64.encode(b"status\r"),
            "operation_id": null,
        });
        let decoded: ClientMessage = serde_json::from_value(legacy).unwrap();
        assert!(matches!(
            decoded,
            ClientMessage::Write {
                cooperative: false,
                ..
            }
        ));

        let cooperative = ClientMessage::Write {
            request_id,
            slot_id: "slot-1".into(),
            control_id,
            fence: 3,
            data: b"status\r".to_vec(),
            operation_id: None,
            expected_run_id: Some(Uuid::new_v4()),
            pacing: None,
            description: None,
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
    fn slot_model_expected_current_preserves_three_states() {
        let base = serde_json::json!({
            "model_id": "tl-as7230-w",
            "source": "human:serialctl"
        });
        let omitted: SetSlotDeviceModelRequest = serde_json::from_value(base.clone()).unwrap();
        assert_eq!(omitted.expected_current, None);
        assert!(
            !serde_json::to_value(&omitted)
                .unwrap()
                .as_object()
                .unwrap()
                .contains_key("expected_current")
        );

        let mut unbound = base.clone();
        unbound["expected_current"] = serde_json::Value::Null;
        let unbound: SetSlotDeviceModelRequest = serde_json::from_value(unbound).unwrap();
        assert_eq!(unbound.expected_current, Some(None));
        assert!(serde_json::to_value(&unbound).unwrap()["expected_current"].is_null());

        let mut bound = base;
        bound["expected_current"] = serde_json::json!("tl-as7230");
        let bound: SetSlotDeviceModelRequest = serde_json::from_value(bound).unwrap();
        assert_eq!(bound.expected_current, Some(Some("tl-as7230".into())));
        assert_eq!(
            serde_json::to_value(&bound).unwrap()["expected_current"],
            serde_json::json!("tl-as7230")
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
    fn protocol_v3_exposes_trigger_contracts() {
        assert_eq!(PROTOCOL_VERSION, 3);
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
    fn trigger_spec_defaults_are_bounded_and_backward_friendly() {
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
            slot_id: "slot-1".into(),
            control_id,
            fence: 17,
            daemon_epoch,
            generation: 5,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
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
            slot_id: "slot-1".into(),
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
            slot_id: "slot-1".into(),
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
