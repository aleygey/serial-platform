//! Macro Script v1. This crate performs no I/O and never appends serial EOL.
//!
//! The driver registers watches before acknowledging `EffectKind::Watch`, binds
//! them to the next command's authoritative TX boundary, and appends the active
//! profile's EOL to `Command`. It feeds fresh, trustworthy RX matches back through
//! [`Vm::mark_matched`]. Every effect must be acknowledged before execution resumes.
//! In particular, an `expect` effect requires the driver to check evidence integrity
//! as well as matching; it must fail the effect on gaps, interference or uncertainty.
mod compiler;
mod syntax;
mod vm;

use std::collections::BTreeMap;
use std::fmt;

pub use vm::Vm;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Span {
    pub offset: usize,
    pub end_offset: usize,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub code: &'static str,
    pub message: String,
    pub span: Span,
}

impl Error {
    pub(crate) fn new(code: &'static str, message: impl Into<String>, span: Span) -> Self {
        Self {
            code,
            message: message.into(),
            span,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at {}:{}: {}",
            self.code, self.span.line, self.span.column, self.message
        )
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String,
    Integer,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    String(String),
    Integer(i64),
    Boolean(bool),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::String(_) => ValueType::String,
            Self::Integer(_) => ValueType::Integer,
            Self::Boolean(_) => ValueType::Boolean,
        }
    }
}

/// Compilation and execution limits, independent of the driver's wall clock.
/// The driver must additionally enforce the total run deadline and physical TX
/// budget including EOL. `max_duration_ms` limits each wait/delay expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_source_bytes: usize,
    pub max_string_bytes: usize,
    pub max_nesting: usize,
    pub max_variables: usize,
    pub max_watchers: usize,
    pub max_instructions: u64,
    pub instructions_per_yield: usize,
    pub max_duration_ms: u64,
    pub max_total_command_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_source_bytes: 65_536,
            max_string_bytes: 4_096,
            max_nesting: 64,
            max_variables: 256,
            max_watchers: 256,
            max_instructions: 100_000,
            instructions_per_yield: 256,
            max_duration_ms: 300_000,
            max_total_command_bytes: 1_048_576,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Program {
    pub(crate) code: Vec<compiler::Instruction>,
    pub(crate) parameters: BTreeMap<String, ValueType>,
    pub(crate) prompts: BTreeMap<String, Span>,
    pub(crate) variables: usize,
    pub(crate) limits: Limits,
}

impl Program {
    /// All static profile prompt references, including those in conditional code.
    pub fn required_prompts(&self) -> impl Iterator<Item = &str> {
        self.prompts.keys().map(String::as_str)
    }

    /// Validate all supplied arguments and required profile prompts before the
    /// driver can receive a command effect. Apply parameter defaults in the caller.
    pub fn start(
        &self,
        args: BTreeMap<String, Value>,
        prompts: BTreeMap<String, String>,
    ) -> Result<Vm, Error> {
        Vm::new(self.clone(), args, prompts)
    }
}

pub fn compile(
    source: &str,
    parameters: &BTreeMap<String, ValueType>,
    limits: Limits,
) -> Result<Program, Error> {
    compiler::compile(source, parameters, limits)
}

pub type WatcherId = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Effect {
    pub id: u64,
    pub span: Span,
    pub kind: EffectKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectKind {
    Command {
        text: String,
        /// False only for an explicit cmd(text, "send_only") call.
        verify_echo: bool,
    },
    Watch {
        watcher: WatcherId,
        pattern: String,
    },
    Wait {
        watcher: WatcherId,
        timeout_ms: u64,
        strict: bool,
    },
    Delay {
        duration_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectResult {
    Done,
    Matched(bool),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Effect(Effect),
    Yielded,
    Complete,
}

/// Commands are line-oriented. Signal/control bytes must use the existing signal
/// path, never a script string. Empty text is valid and means one profile EOL.
pub fn validate_command(text: &str, max_bytes: usize, span: Span) -> Result<(), Error> {
    if text.len() > max_bytes {
        return Err(Error::new(
            "string_limit",
            format!("command exceeds {max_bytes} UTF-8 bytes"),
            span,
        ));
    }
    if text.chars().any(char::is_control) {
        return Err(Error::new(
            "command_control_character",
            "cmd text must not contain control characters or embedded EOL",
            span,
        ));
    }
    Ok(())
}
