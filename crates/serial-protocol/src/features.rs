//! Version-eight human-history and Macro Script contracts.

use super::*;

pub const MACRO_LANGUAGE_VERSION: u16 = 1;
pub const MAX_MACRO_SOURCE_BYTES: usize = 64 * 1024;
pub const MAX_MACRO_TIMEOUT_SECONDS: u64 = 120;

/// Present only for a command submitted from a Human LINE editor. Raw keys,
/// signals, Agent writes and macro expansion must never manufacture this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanLineInput {
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanCommandHistoryEntry {
    pub id: Uuid,
    pub command: String,
    pub port: String,
    pub wall_time_ns: i64,
    pub revision: u64,
    pub uses: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HumanCommandHistoryQuery {
    pub prefix: Option<String>,
    pub contains: Option<String>,
    pub before_revision: Option<u64>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HumanCommandHistoryResponse {
    pub server_id: Uuid,
    pub revision: u64,
    /// Recent first. Clients reverse when using oldest-first editor history.
    pub entries: Vec<HumanCommandHistoryEntry>,
    pub next_before_revision: Option<u64>,
    pub warning: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MacroParameterType {
    String,
    Integer,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacroParameter {
    #[serde(rename = "type")]
    pub value_type: MacroParameterType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacroApplicability {
    pub model_family: String,
    #[serde(default)]
    pub model_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MacroDefinition {
    pub id: String,
    pub name: String,
    pub description: String,
    pub language_version: u16,
    pub revision: u64,
    pub parameters: BTreeMap<String, MacroParameter>,
    pub script: String,
    pub shared: bool,
    pub applies_to: Option<MacroApplicability>,
    pub updated_at_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MacroSummary {
    pub id: String,
    pub name: String,
    pub description: String,
    pub language_version: u16,
    pub revision: u64,
    pub parameters: BTreeMap<String, MacroParameter>,
    pub shared: bool,
    pub applies_to: Option<MacroApplicability>,
}

impl From<&MacroDefinition> for MacroSummary {
    fn from(value: &MacroDefinition) -> Self {
        Self {
            id: value.id.clone(),
            name: value.name.clone(),
            description: value.description.clone(),
            language_version: value.language_version,
            revision: value.revision,
            parameters: value.parameters.clone(),
            shared: value.shared,
            applies_to: value.applies_to.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MacroListQuery {
    pub id: Option<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub include_drafts: bool,
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroListResponse {
    pub catalog_revision: u64,
    pub macros: Vec<MacroSummary>,
    pub definition: Option<MacroDefinition>,
    pub total: usize,
    pub next_offset: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacroSaveRequest {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub parameters: BTreeMap<String, MacroParameter>,
    pub script: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub shared: Option<bool>,
    #[serde(default)]
    pub applies_to: Option<MacroApplicability>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MacroSaveResponse {
    pub catalog_revision: u64,
    pub definition: MacroDefinition,
}

/// The public MCP call translates its opaque Run into the authenticated
/// control fields on MacroStart. Humans use their own ordinary control lease.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MacroRunSpec {
    #[serde(default)]
    pub macro_id: Option<String>,
    #[serde(default)]
    pub revision: Option<u64>,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub args: BTreeMap<String, Value>,
    #[serde(default = "default_macro_timeout")]
    pub timeout_seconds: u64,
}

fn default_macro_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MacroStatus {
    Running,
    Stopping,
    Succeeded,
    TimedOut,
    Cancelled,
    InterruptedByUser,
    Failed,
}

impl MacroStatus {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Stopping)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MacroExecutionInfo {
    pub id: Uuid,
    pub port: String,
    pub daemon_epoch: Uuid,
    pub generation: u64,
    pub owner: Actor,
    pub run_id: Option<Uuid>,
    pub macro_id: Option<String>,
    pub revision: Option<u64>,
    pub description: String,
    pub status: MacroStatus,
    pub started_at_ns: i64,
    pub completed_at_ns: Option<i64>,
    pub line: usize,
    pub column: usize,
    pub writes: u64,
    #[serde(default)]
    pub input_verified_writes: u64,
    #[serde(default)]
    pub send_only_writes: u64,
    pub bytes_written: u64,
    pub first_seq: u64,
    pub through_seq: u64,
    pub message: Option<String>,
    pub outcome_uncertain: bool,
}
