use chrono::{DateTime, Local, SecondsFormat, Utc};
use ratatui::style::{Color, Modifier, Style};
use serial_protocol::{ActorKind, Direction, EventKind, TimelineEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SemanticKind {
    Normal,
    Prompt,
    Debug,
    Info,
    Success,
    Warning,
    Error,
    System,
    Gap,
}

#[derive(Debug, Clone)]
pub struct DisplayLine {
    pub seq: u64,
    pub source: String,
    pub text: String,
    pub source_style: Style,
    pub semantic_style: Style,
    pub bytes: usize,
}

/// The bounded amount of one unterminated terminal row retained in memory.
/// Long binary/no-newline streams are committed in readable chunks instead of
/// allowing a single Slot to grow without limit.
const MAX_STREAM_LINE_CHARS: usize = 16 * 1024;
const MAX_CSI_PARAMETER_BYTES: usize = 64;

/// The result of feeding one timeline event into [`TerminalStreamParser`].
///
/// `completed` contains immutable rows that may be appended to the scrollback.
/// `pending` is the authoritative current unterminated row: callers should
/// replace their previous pending row with this value (including clearing it
/// when it is `None`). This is what makes carriage-return progress output and
/// prompts update in place without duplicating a row for every serial chunk.
#[derive(Debug, Default)]
pub struct StreamDisplayBatch {
    pub completed: Vec<DisplayLine>,
    pub pending: Option<DisplayLine>,
}

/// Incremental, per-Slot terminal-to-text projection.
///
/// Keep one instance for each Slot and feed timeline events in sequence order.
/// The parser deliberately does not execute remote terminal controls. It
/// strips CSI/OSC/DCS/SOS/PM/APC sequences while preserving enough single-row
/// cursor semantics for CR, backspace, tabs, and common CSI erase/cursor
/// operations. UTF-8 and escape sequences may span any number of events.
#[derive(Debug, Default)]
pub struct TerminalStreamParser {
    terminal: TerminalTextState,
    context: Option<LineContext>,
}

impl TerminalStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Projects the next event. Byte-bearing RX/TX events participate in the
    /// stream; control/system events form their own committed rows. Changing
    /// source, daemon epoch, or physical generation commits the old partial
    /// row so attribution never leaks across actors or serial sessions.
    pub fn push_event(
        &mut self,
        event: &TimelineEvent,
        shell_prompt: Option<&str>,
        uboot_prompt: Option<&str>,
    ) -> StreamDisplayBatch {
        let mut completed = Vec::new();

        if event.direction == Direction::None {
            completed.extend(self.flush(shell_prompt, uboot_prompt));
            completed.extend(event_to_lines(event, shell_prompt, uboot_prompt));
            return StreamDisplayBatch {
                completed,
                pending: None,
            };
        }

        let incoming = LineContext::from_event(event);
        if self
            .context
            .as_ref()
            .is_some_and(|current| current.identity != incoming.identity)
        {
            completed.extend(self.flush(shell_prompt, uboot_prompt));
        }

        match &mut self.context {
            Some(current) => current.refresh(event),
            None => self.context = Some(incoming),
        }

        let rows = self.terminal.push(&event.data);
        if let Some(context) = self.context.as_ref() {
            completed.extend(
                rows.into_iter()
                    .map(|text| context.display_line(text, shell_prompt, uboot_prompt)),
            );
        }

        StreamDisplayBatch {
            completed,
            pending: self.pending_line(shell_prompt, uboot_prompt),
        }
    }

    /// Commits an unterminated row and resets all decoder/escape state.
    /// A truncated UTF-8 scalar is rendered as U+FFFD; an unfinished escape
    /// sequence is discarded because replaying it would be unsafe.
    pub fn flush(
        &mut self,
        shell_prompt: Option<&str>,
        uboot_prompt: Option<&str>,
    ) -> Vec<DisplayLine> {
        let completed_rows = self.terminal.finish_input();
        let mut lines = Vec::new();
        if let Some(context) = self.context.as_ref() {
            lines.extend(
                completed_rows
                    .into_iter()
                    .map(|text| context.display_line(text, shell_prompt, uboot_prompt)),
            );
            if let Some(text) = self.terminal.take_pending() {
                lines.push(context.display_line(text, shell_prompt, uboot_prompt));
            }
        }
        self.terminal.reset();
        self.context = None;
        lines
    }

    /// Drops all buffered text and decoder state without producing output.
    /// Use this when an authoritative snapshot invalidates the old stream.
    pub fn reset(&mut self) {
        self.terminal.reset();
        self.context = None;
    }

    pub fn pending_line(
        &self,
        shell_prompt: Option<&str>,
        uboot_prompt: Option<&str>,
    ) -> Option<DisplayLine> {
        let context = self.context.as_ref()?;
        self.terminal
            .pending_text()
            .map(|text| context.display_line(text, shell_prompt, uboot_prompt))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamIdentity {
    daemon_epoch: uuid::Uuid,
    generation: u64,
    direction: Direction,
    actor_id: Option<String>,
    actor_kind: Option<ActorKind>,
}

#[derive(Debug, Clone)]
struct LineContext {
    identity: StreamIdentity,
    seq: u64,
    source: String,
    source_style: Style,
    direction: Direction,
    kind: EventKind,
}

impl LineContext {
    fn from_event(event: &TimelineEvent) -> Self {
        Self {
            identity: StreamIdentity {
                daemon_epoch: event.daemon_epoch,
                generation: event.generation,
                direction: event.direction,
                actor_id: event.actor.as_ref().map(|actor| actor.id.clone()),
                actor_kind: event.actor.as_ref().map(|actor| actor.kind),
            },
            seq: event.seq,
            source: source_label(event),
            source_style: source_style(event),
            direction: event.direction,
            kind: event.kind,
        }
    }

    fn refresh(&mut self, event: &TimelineEvent) {
        self.seq = event.seq;
        self.kind = event.kind;
    }

    fn display_line(
        &self,
        text: String,
        shell_prompt: Option<&str>,
        uboot_prompt: Option<&str>,
    ) -> DisplayLine {
        let semantic = classify_parts(&text, self.direction, self.kind, shell_prompt, uboot_prompt);
        DisplayLine {
            seq: self.seq,
            source: self.source.clone(),
            bytes: text.len() + self.source.len() + 16,
            semantic_style: semantic_style(semantic),
            source_style: self.source_style,
            text,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum EscapeState {
    #[default]
    Ground,
    Escape,
    EscapeIntermediate,
    Csi,
    ControlString,
    ControlStringEscape,
}

#[derive(Debug, Default)]
struct TerminalTextState {
    line: Vec<char>,
    cursor: usize,
    touched: bool,
    utf8: Vec<u8>,
    escape: EscapeState,
    csi_parameters: Vec<u8>,
}

impl TerminalTextState {
    fn push(&mut self, bytes: &[u8]) -> Vec<String> {
        let mut rows = Vec::new();
        for &byte in bytes {
            self.consume(byte, &mut rows);
        }
        rows
    }

    fn consume(&mut self, byte: u8, rows: &mut Vec<String>) {
        if self.escape != EscapeState::Ground {
            self.consume_escape(byte);
            return;
        }

        // ASCII controls cannot continue UTF-8. Finalize a malformed prefix
        // before applying the control to keep both states deterministic.
        if !self.utf8.is_empty() && byte < 0x80 {
            self.drain_utf8(true, rows);
        }
        if !self.utf8.is_empty() {
            self.utf8.push(byte);
            self.drain_utf8(false, rows);
            return;
        }

        match byte {
            0x1b => self.escape = EscapeState::Escape,
            // Eight-bit C1 forms are accepted only when no UTF-8 scalar is in
            // progress, so continuation bytes inside valid UTF-8 are safe.
            0x9b => self.start_csi(),
            0x90 | 0x98 | 0x9d | 0x9e | 0x9f => self.escape = EscapeState::ControlString,
            0x9c => {}
            b'\n' => self.commit_row(rows),
            b'\r' => {
                self.cursor = 0;
                self.touched |= !self.line.is_empty();
            }
            0x08 => {
                self.cursor = self.cursor.saturating_sub(1);
                self.touched |= !self.line.is_empty();
            }
            b'\t' => {
                let next_tab = ((self.cursor / 8) + 1) * 8;
                while self.cursor < next_tab {
                    self.write_char(' ', rows);
                }
            }
            0x00..=0x1f | 0x7f => {}
            0x20..=0x7e => self.write_char(char::from(byte), rows),
            _ => {
                self.utf8.push(byte);
                self.drain_utf8(false, rows);
            }
        }
    }

    fn consume_escape(&mut self, byte: u8) {
        match self.escape {
            EscapeState::Ground => unreachable!("ground escapes are handled by consume"),
            EscapeState::Escape => match byte {
                b'[' => self.start_csi(),
                b']' | b'P' | b'X' | b'^' | b'_' => self.escape = EscapeState::ControlString,
                0x20..=0x2f => self.escape = EscapeState::EscapeIntermediate,
                0x1b => {}
                _ => self.escape = EscapeState::Ground,
            },
            EscapeState::EscapeIntermediate => {
                if byte == 0x1b {
                    self.escape = EscapeState::Escape;
                } else if (0x30..=0x7e).contains(&byte) {
                    self.escape = EscapeState::Ground;
                }
            }
            EscapeState::Csi => {
                if byte == 0x1b {
                    self.csi_parameters.clear();
                    self.escape = EscapeState::Escape;
                } else if (0x40..=0x7e).contains(&byte) {
                    let parameters = std::mem::take(&mut self.csi_parameters);
                    self.escape = EscapeState::Ground;
                    self.apply_csi(byte, &parameters);
                } else if self.csi_parameters.len() < MAX_CSI_PARAMETER_BYTES {
                    self.csi_parameters.push(byte);
                }
            }
            EscapeState::ControlString => match byte {
                0x07 | 0x9c => self.escape = EscapeState::Ground,
                0x1b => self.escape = EscapeState::ControlStringEscape,
                _ => {}
            },
            EscapeState::ControlStringEscape => match byte {
                b'\\' | 0x9c => self.escape = EscapeState::Ground,
                0x1b => {}
                _ => self.escape = EscapeState::ControlString,
            },
        }
    }

    fn start_csi(&mut self) {
        self.csi_parameters.clear();
        self.escape = EscapeState::Csi;
    }

    fn apply_csi(&mut self, final_byte: u8, parameters: &[u8]) {
        let first = csi_parameter(parameters, 0, 0);
        match final_byte {
            // EL: preserving this common operation avoids stale suffixes in
            // `CR + erase-line + progress` output while all styling remains
            // deliberately stripped.
            b'K' => match first {
                0 => self.line.truncate(self.cursor.min(self.line.len())),
                1 => {
                    let through = self.cursor.saturating_add(1).min(self.line.len());
                    self.line[..through].fill(' ');
                }
                2 => self.line.clear(),
                _ => {}
            },
            // CHA/HPA, CUF, CUB, and the column component of CUP/HVP.
            b'G' | b'`' => {
                self.cursor = first
                    .max(1)
                    .saturating_sub(1)
                    .min(MAX_STREAM_LINE_CHARS - 1)
            }
            b'C' | b'a' => {
                self.cursor = self
                    .cursor
                    .saturating_add(first.max(1))
                    .min(MAX_STREAM_LINE_CHARS - 1)
            }
            b'D' => self.cursor = self.cursor.saturating_sub(first.max(1)),
            b'H' | b'f' => {
                let column = csi_parameter(parameters, 1, 1);
                self.cursor = column
                    .max(1)
                    .saturating_sub(1)
                    .min(MAX_STREAM_LINE_CHARS - 1);
            }
            _ => {}
        }
    }

    fn drain_utf8(&mut self, finalize: bool, rows: &mut Vec<String>) {
        loop {
            match std::str::from_utf8(&self.utf8) {
                Ok(text) => {
                    let text = text.to_string();
                    self.utf8.clear();
                    for character in text.chars() {
                        self.write_char(character, rows);
                    }
                    return;
                }
                Err(error) => {
                    let valid_up_to = error.valid_up_to();
                    if valid_up_to > 0 {
                        let valid = String::from_utf8(self.utf8[..valid_up_to].to_vec())
                            .expect("from_utf8 reported this prefix as valid");
                        self.utf8.drain(..valid_up_to);
                        for character in valid.chars() {
                            self.write_char(character, rows);
                        }
                        continue;
                    }
                    if let Some(error_len) = error.error_len() {
                        self.utf8.drain(..error_len);
                        self.write_char('\u{fffd}', rows);
                        continue;
                    }
                    if finalize {
                        self.utf8.clear();
                        self.write_char('\u{fffd}', rows);
                    }
                    return;
                }
            }
        }
    }

    fn write_char(&mut self, character: char, rows: &mut Vec<String>) {
        if self.cursor >= MAX_STREAM_LINE_CHARS {
            self.commit_row(rows);
        }
        while self.line.len() < self.cursor {
            self.line.push(' ');
        }
        if self.cursor < self.line.len() {
            self.line[self.cursor] = character;
        } else {
            self.line.push(character);
        }
        self.cursor += 1;
        self.touched = true;
    }

    fn commit_row(&mut self, rows: &mut Vec<String>) {
        self.drain_utf8(true, rows);
        rows.push(self.line.iter().collect());
        self.line.clear();
        self.cursor = 0;
        self.touched = false;
    }

    fn finish_input(&mut self) -> Vec<String> {
        let mut completed = Vec::new();
        self.drain_utf8(true, &mut completed);
        self.escape = EscapeState::Ground;
        self.csi_parameters.clear();
        completed
    }

    fn pending_text(&self) -> Option<String> {
        (self.touched || !self.line.is_empty()).then(|| self.line.iter().collect())
    }

    fn take_pending(&mut self) -> Option<String> {
        let text = self.pending_text()?;
        self.line.clear();
        self.cursor = 0;
        self.touched = false;
        Some(text)
    }

    fn reset(&mut self) {
        *self = Self::default();
    }
}

fn csi_parameter(parameters: &[u8], index: usize, default: usize) -> usize {
    let Some(value) = parameters.split(|byte| *byte == b';').nth(index) else {
        return default;
    };
    if value.is_empty() {
        return default;
    }
    value.iter().fold(0usize, |number, byte| {
        if byte.is_ascii_digit() {
            number
                .saturating_mul(10)
                .saturating_add(usize::from(*byte - b'0'))
        } else {
            number
        }
    })
}

pub fn event_to_lines(
    event: &TimelineEvent,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> Vec<DisplayLine> {
    let source = source_label(event);
    let source_style = source_style(event);
    let text = sanitize_terminal_bytes(&event.data);
    let text = normalize_newlines(&text);
    let mut lines = text.split('\n').map(str::to_string).collect::<Vec<_>>();
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    if lines.is_empty() {
        lines.push(system_event_text(event));
    }

    lines
        .into_iter()
        .map(|text| {
            let semantic = classify(&text, event, shell_prompt, uboot_prompt);
            DisplayLine {
                seq: event.seq,
                source: source.clone(),
                bytes: text.len() + source.len() + 16,
                semantic_style: semantic_style(semantic),
                source_style,
                text,
            }
        })
        .collect()
}

pub fn gap_line(seq: u64, text: impl Into<String>) -> DisplayLine {
    let text = text.into();
    DisplayLine {
        seq,
        source: "GAP".into(),
        bytes: text.len() + 20,
        source_style: Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        semantic_style: semantic_style(SemanticKind::Gap),
        text,
    }
}

pub fn format_event_plain(event: &TimelineEvent) -> String {
    let source = audit_source_label(event);
    let payload = normalize_newlines(&sanitize_terminal_bytes(&event.data))
        .replace('\n', "\\n")
        .replace('\t', "\\t");
    let payload = if payload.is_empty() {
        system_event_text(event)
    } else {
        payload
    };
    format!(
        "{}  seq={:<10} gen={:<6} {}/{:<8} {}",
        format_wall_time_local(event.wall_time_ns),
        event.seq,
        event.generation,
        event_kind_label(event.kind),
        source,
        payload
    )
}

pub fn format_wall_time_local(wall_time_ns: i64) -> String {
    let seconds = wall_time_ns.div_euclid(1_000_000_000);
    let nanos = wall_time_ns.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanos)
        .map(|timestamp| {
            timestamp
                .with_timezone(&Local)
                .to_rfc3339_opts(SecondsFormat::Millis, false)
        })
        .unwrap_or_else(|| format!("{wall_time_ns}ns"))
}

/// Makes daemon/config/user-provided labels safe for a single terminal row.
/// It intentionally removes every escape sequence, not only known-dangerous
/// ones, because Ratatui must never replay remote terminal controls.
pub fn safe_inline(value: &str) -> String {
    normalize_newlines(&sanitize_terminal_bytes(value.as_bytes())).replace(['\n', '\t'], " ")
}

pub fn sanitize_terminal_bytes(bytes: &[u8]) -> String {
    let mut clean = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == 0x1b {
            index = skip_escape_sequence(bytes, index);
            continue;
        }
        match byte {
            b'\n' | b'\r' | b'\t' => clean.push(byte),
            0x08 => {
                while clean.last().is_some_and(|last| (*last & 0xc0) == 0x80) {
                    clean.pop();
                }
                clean.pop();
            }
            0x00..=0x1f | 0x7f => {}
            _ => clean.push(byte),
        }
        index += 1;
    }
    String::from_utf8_lossy(&clean).into_owned()
}

fn skip_escape_sequence(bytes: &[u8], escape_index: usize) -> usize {
    let Some(&kind) = bytes.get(escape_index + 1) else {
        return bytes.len();
    };
    match kind {
        // CSI: parameters/intermediates ending in a final byte.
        b'[' => {
            let mut index = escape_index + 2;
            while index < bytes.len() {
                if (0x40..=0x7e).contains(&bytes[index]) {
                    return index + 1;
                }
                index += 1;
            }
            bytes.len()
        }
        // OSC, DCS, SOS, PM and APC: terminate at BEL or ST. This removes
        // clipboard (OSC 52), hyperlinks, title updates and device queries.
        b']' | b'P' | b'X' | b'^' | b'_' => {
            let mut index = escape_index + 2;
            while index < bytes.len() {
                if bytes[index] == 0x07 {
                    return index + 1;
                }
                if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
                    return index + 2;
                }
                index += 1;
            }
            bytes.len()
        }
        // All remaining two-byte escape sequences are display control and are
        // deliberately not replayed into the user's terminal.
        _ => (escape_index + 2).min(bytes.len()),
    }
}

fn normalize_newlines(value: &str) -> String {
    value.replace("\r\n", "\n").replace('\r', "\n")
}

fn source_label(event: &TimelineEvent) -> String {
    match event.direction {
        Direction::Rx => "DEV".into(),
        Direction::Tx => event
            .actor
            .as_ref()
            .map(|actor| compact_actor_label(actor, true))
            .unwrap_or_else(|| "TX>".into()),
        Direction::None => event
            .actor
            .as_ref()
            .map(|actor| compact_actor_label(actor, false))
            .unwrap_or_else(|| "SYSTEM".into()),
    }
}

fn audit_source_label(event: &TimelineEvent) -> String {
    match event.direction {
        Direction::Rx => "DEV".into(),
        Direction::Tx => event
            .actor
            .as_ref()
            .map(|actor| full_actor_label(actor, true))
            .unwrap_or_else(|| "TX>".into()),
        Direction::None => event
            .actor
            .as_ref()
            .map(|actor| full_actor_label(actor, false))
            .unwrap_or_else(|| "SYSTEM".into()),
    }
}

fn actor_kind_label(kind: ActorKind) -> &'static str {
    match kind {
        ActorKind::Human => "HUMAN",
        ActorKind::Agent => "AGENT",
        ActorKind::Script => "SCRIPT",
        ActorKind::System => "SYSTEM",
    }
}

fn compact_actor_label(actor: &serial_protocol::Actor, write: bool) -> String {
    let label = truncate_inline(&actor.label, 12);
    let id = safe_inline(&actor.id);
    let short_id = id.chars().rev().take(8).collect::<String>();
    let short_id = short_id.chars().rev().collect::<String>();
    format!(
        "{}:{}[{}]{}",
        actor_kind_label(actor.kind),
        label,
        short_id,
        if write { ">" } else { "" }
    )
}

fn full_actor_label(actor: &serial_protocol::Actor, write: bool) -> String {
    format!(
        "{}:{}[{}]{}",
        actor_kind_label(actor.kind),
        safe_inline(&actor.label),
        safe_inline(&actor.id),
        if write { ">" } else { "" }
    )
}

fn truncate_inline(value: &str, max_chars: usize) -> String {
    let clean = safe_inline(value);
    if clean.chars().count() <= max_chars {
        return clean;
    }
    let mut truncated = clean
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn event_kind_label(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Rx => "rx",
        EventKind::Tx => "tx",
        EventKind::SerialOpening => "serial_opening",
        EventKind::SerialOpened => "serial_opened",
        EventKind::SerialOpenFailed => "serial_open_failed",
        EventKind::SerialClosed => "serial_closed",
        EventKind::SlotReconfigured => "slot_reconfigured",
        EventKind::SlotRemoved => "slot_removed",
        EventKind::ControlGranted => "control_granted",
        EventKind::ControlReleased => "control_released",
        EventKind::ControlRevoked => "control_revoked",
        EventKind::ControlExpired => "control_expired",
        EventKind::RunStarted => "run_started",
        EventKind::RunEnded => "run_ended",
        EventKind::RunAborted => "run_aborted",
        EventKind::Checkpoint => "checkpoint",
        EventKind::LoggingDegraded => "logging_degraded",
        EventKind::Gap => "gap",
    }
}

fn source_style(event: &TimelineEvent) -> Style {
    let color = match event.direction {
        Direction::Rx => Color::Cyan,
        Direction::None => Color::DarkGray,
        Direction::Tx => match event.actor.as_ref().map(|actor| actor.kind) {
            Some(ActorKind::Human) => Color::Green,
            Some(ActorKind::Agent) => Color::Magenta,
            Some(ActorKind::Script) => Color::Yellow,
            Some(ActorKind::System) | None => Color::Blue,
        },
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

fn classify(
    text: &str,
    event: &TimelineEvent,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> SemanticKind {
    classify_parts(
        text,
        event.direction,
        event.kind,
        shell_prompt,
        uboot_prompt,
    )
}

fn classify_parts(
    text: &str,
    direction: Direction,
    kind: EventKind,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> SemanticKind {
    if direction == Direction::None {
        return match kind {
            EventKind::Gap | EventKind::LoggingDegraded | EventKind::SerialOpenFailed => {
                SemanticKind::Error
            }
            _ => SemanticKind::System,
        };
    }

    let configured_prompt = [shell_prompt, uboot_prompt]
        .into_iter()
        .flatten()
        .any(|prompt| !prompt.is_empty() && text.ends_with(prompt));
    let trimmed = text.trim_end();
    if configured_prompt
        || trimmed.ends_with(" #")
        || trimmed.ends_with(" $")
        || trimmed.ends_with(" >")
    {
        return SemanticKind::Prompt;
    }

    let lowercase = text.to_ascii_lowercase();
    if contains_any(
        &lowercase,
        &["error", "failed", "failure", "panic", "fatal", "assert"],
    ) {
        SemanticKind::Error
    } else if contains_any(&lowercase, &["warn", "timeout", "retry", "dropped"]) {
        SemanticKind::Warning
    } else if contains_any(&lowercase, &["success", "passed", " pass", "ready", "[ok]"]) {
        SemanticKind::Success
    } else if contains_any(&lowercase, &["debug", "trace"]) {
        SemanticKind::Debug
    } else if contains_any(&lowercase, &["info", "notice"]) {
        SemanticKind::Info
    } else {
        SemanticKind::Normal
    }
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

fn semantic_style(kind: SemanticKind) -> Style {
    match kind {
        SemanticKind::Normal => Style::default(),
        SemanticKind::Prompt => Style::default()
            .fg(Color::LightCyan)
            .add_modifier(Modifier::BOLD),
        SemanticKind::Debug => Style::default().fg(Color::DarkGray),
        SemanticKind::Info => Style::default().fg(Color::LightBlue),
        SemanticKind::Success => Style::default().fg(Color::LightGreen),
        SemanticKind::Warning => Style::default().fg(Color::Yellow),
        SemanticKind::Error | SemanticKind::Gap => Style::default()
            .fg(Color::LightRed)
            .add_modifier(Modifier::BOLD),
        SemanticKind::System => Style::default().fg(Color::DarkGray),
    }
}

fn system_event_text(event: &TimelineEvent) -> String {
    if let Some(message) = event
        .metadata
        .get("message")
        .and_then(|value| value.as_str())
    {
        return message.to_string();
    }
    format!("{:?}", event.kind)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serial_protocol::{Actor, ActorKind, Direction, EventKind};
    use uuid::Uuid;

    use super::*;

    fn event(data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: Uuid::nil(),
            seq: 1,
            generation: 1,
            wall_time_ns: 0,
            monotonic_time_ns: 0,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: data.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    fn event_at(seq: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent { seq, ..event(data) }
    }

    #[test]
    fn removes_sgr_and_dangerous_osc_sequences() {
        let bytes = b"safe\x1b[31m red\x1b[0m\x1b]52;c;secret\x07 end";
        assert_eq!(sanitize_terminal_bytes(bytes), "safe red end");
    }

    #[test]
    fn plain_log_output_includes_local_millisecond_time_and_event_identity() {
        let mut event = event(b"booted\r\n");
        event.seq = 42;
        event.generation = 7;
        event.wall_time_ns = 1_123_456_789;
        let rendered = format_event_plain(&event);
        assert!(rendered.contains(".123"));
        assert!(rendered.contains("seq=42"));
        assert!(rendered.contains("gen=7"));
        assert!(rendered.contains("rx/DEV"));
        assert!(rendered.ends_with("booted\\n"));
    }

    #[test]
    fn human_readable_sources_include_safe_actor_label_and_id() {
        let mut tx = event(b"reboot\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "agent:session-12345678".into(),
            label: "worker-a\u{1b}[31m".into(),
            kind: ActorKind::Agent,
        });

        let rendered = format_event_plain(&tx);
        assert!(rendered.contains("AGENT:worker-a[agent:session-12345678]>"));
        assert!(!rendered.contains("\u{1b}"));

        let line = event_to_lines(&tx, None, None).remove(0);
        assert_eq!(line.source, "AGENT:worker-a[12345678]>");
    }

    #[test]
    fn local_time_formatter_handles_negative_epoch_values() {
        let rendered = format_wall_time_local(-1);
        assert!(rendered.contains(".999"));
        assert!(!rendered.ends_with("ns"));
    }

    #[test]
    fn prompt_and_errors_get_semantic_styles() {
        let prompt = event(b"SigmaStar #");
        assert_eq!(
            classify("SigmaStar #", &prompt, None, Some("SigmaStar #")),
            SemanticKind::Prompt
        );
        assert_eq!(
            classify("FATAL: boot failed", &prompt, None, None),
            SemanticKind::Error
        );
    }

    #[test]
    fn stream_decodes_utf8_split_across_events() {
        let mut parser = TerminalStreamParser::new();
        let encoded = "启动".as_bytes();

        let first = parser.push_event(&event_at(1, &encoded[..2]), None, None);
        assert!(first.completed.is_empty());
        assert!(first.pending.is_none());

        let second = parser.push_event(&event_at(2, &encoded[2..5]), None, None);
        assert!(second.completed.is_empty());
        assert_eq!(
            second.pending.as_ref().map(|line| line.text.as_str()),
            Some("启")
        );

        let mut final_bytes = encoded[5..].to_vec();
        final_bytes.push(b'\n');
        let third = parser.push_event(&event_at(3, &final_bytes), None, None);
        assert_eq!(
            third
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["启动"]
        );
        assert!(third.pending.is_none());
    }

    #[test]
    fn stream_strips_split_csi_and_osc_sequences() {
        let mut parser = TerminalStreamParser::new();

        let first = parser.push_event(&event_at(1, b"safe \x1b[3"), None, None);
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe ")
        );

        let second = parser.push_event(&event_at(2, b"1mred\x1b[0m \x1b]52;c;sec"), None, None);
        assert_eq!(
            second.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe red ")
        );

        let third = parser.push_event(&event_at(3, b"ret\x1b"), None, None);
        assert_eq!(
            third.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe red ")
        );
        let fourth = parser.push_event(&event_at(4, b"\\end\n"), None, None);
        assert_eq!(fourth.completed.len(), 1);
        assert_eq!(fourth.completed[0].text, "safe red end");
    }

    #[test]
    fn stream_applies_cr_erase_and_backspace_across_events() {
        let mut parser = TerminalStreamParser::new();
        let progress = parser.push_event(&event_at(1, b"download 100%\r42%\x1b[K\n"), None, None);
        assert_eq!(progress.completed[0].text, "42%");

        let first = parser.push_event(&event_at(2, b"abc\x08"), None, None);
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("abc")
        );
        let second = parser.push_event(&event_at(3, b"\x08XY\n"), None, None);
        assert_eq!(second.completed[0].text, "aXY");

        let overwrite = parser.push_event(&event_at(4, b"abc\rXY\n"), None, None);
        assert_eq!(overwrite.completed[0].text, "XYc");
    }

    #[test]
    fn source_change_commits_partial_row_and_preserves_styles() {
        let mut parser = TerminalStreamParser::new();
        let first = parser.push_event(&event_at(1, b"Sigma"), None, Some("SigmaStar #"));
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("Sigma")
        );

        let prompt = parser.push_event(&event_at(2, b"Star #"), None, Some("SigmaStar #"));
        let prompt = prompt
            .pending
            .expect("prompt should remain visible without newline");
        assert_eq!(prompt.source, "DEV");
        assert_eq!(prompt.semantic_style, semantic_style(SemanticKind::Prompt));

        let mut tx = event_at(3, b"reboot\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "human-1".into(),
            label: "operator".into(),
            kind: ActorKind::Human,
        });
        let switched = parser.push_event(&tx, None, Some("SigmaStar #"));
        assert_eq!(switched.completed.len(), 1);
        assert_eq!(switched.completed[0].source, "DEV");
        assert_eq!(switched.completed[0].text, "SigmaStar #");
        assert_eq!(
            switched.pending.as_ref().map(|line| line.source.as_str()),
            Some("HUMAN:operator[human-1]>")
        );
        assert_eq!(
            switched.pending.as_ref().map(|line| line.source_style),
            Some(
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            )
        );
        assert_eq!(
            switched.pending.as_ref().map(|line| line.text.as_str()),
            Some("reboot")
        );
    }

    #[test]
    fn streamed_rows_match_shell_and_uboot_profile_prompts() {
        let mut parser = TerminalStreamParser::new();
        let shell_prompt = Some("root@dut:/tmp# ");
        let uboot_prompt = Some("SigmaStar =>");

        let shell_prefix =
            parser.push_event(&event_at(1, b"root@dut:/"), shell_prompt, uboot_prompt);
        assert_eq!(
            shell_prefix.pending.as_ref().map(|line| line.text.as_str()),
            Some("root@dut:/")
        );
        let shell = parser.push_event(&event_at(2, b"tmp# "), shell_prompt, uboot_prompt);
        assert_eq!(
            shell.pending.as_ref().map(|line| line.semantic_style),
            Some(semantic_style(SemanticKind::Prompt))
        );

        let uboot_prefix = parser.push_event(&event_at(3, b"\nSigma"), shell_prompt, uboot_prompt);
        assert_eq!(
            uboot_prefix
                .completed
                .first()
                .map(|line| line.semantic_style),
            Some(semantic_style(SemanticKind::Prompt))
        );
        assert_eq!(
            uboot_prefix.pending.as_ref().map(|line| line.text.as_str()),
            Some("Sigma")
        );
        let uboot = parser.push_event(&event_at(4, b"Star =>"), shell_prompt, uboot_prompt);
        assert_eq!(
            uboot.pending.as_ref().map(|line| line.semantic_style),
            Some(semantic_style(SemanticKind::Prompt))
        );
    }

    #[test]
    fn semantic_classification_uses_the_complete_streamed_row() {
        let mut parser = TerminalStreamParser::new();
        parser.push_event(&event_at(1, b"FAT"), None, None);
        let completed = parser.push_event(&event_at(2, b"AL: boot failed\n"), None, None);

        assert_eq!(completed.completed.len(), 1);
        assert_eq!(completed.completed[0].text, "FATAL: boot failed");
        assert_eq!(
            completed.completed[0].semantic_style,
            semantic_style(SemanticKind::Error)
        );
    }

    #[test]
    fn flush_handles_truncated_utf8_and_drops_incomplete_escape() {
        let mut parser = TerminalStreamParser::new();
        parser.push_event(&event_at(1, b"ok\xe5\x90"), None, None);
        let flushed = parser.flush(None, None);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].text, "ok\u{fffd}");

        parser.push_event(&event_at(2, b"safe\x1b]52;c;secret"), None, None);
        let flushed = parser.flush(None, None);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].text, "safe");
    }

    #[test]
    fn stream_bounds_unterminated_rows() {
        let mut parser = TerminalStreamParser::new();
        let bytes = vec![b'x'; MAX_STREAM_LINE_CHARS + 1];
        let batch = parser.push_event(&event_at(1, &bytes), None, None);

        assert_eq!(batch.completed.len(), 1);
        assert_eq!(batch.completed[0].text.len(), MAX_STREAM_LINE_CHARS);
        assert_eq!(
            batch.pending.as_ref().map(|line| line.text.as_str()),
            Some("x")
        );
    }
}
