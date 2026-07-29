use std::collections::VecDeque;

use chrono::{DateTime, Local, SecondsFormat, Utc};
use ratatui::style::{Color, Modifier, Style};
use serial_protocol::{ActorKind, Direction, EventKind, TimelineEvent, TriggerStatus};
use unicode_width::UnicodeWidthChar;

use crate::i18n::{tr, trf};

/// Pads or truncates `value` to exactly `width` terminal display columns.
/// CJK characters count as two columns; zero-width characters count as zero.
pub fn pad_display(value: &str, width: usize) -> String {
    let mut output = String::with_capacity(width);
    let mut used = 0;
    for character in value.chars() {
        let char_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if used + char_width > width {
            break;
        }
        output.push(character);
        used += char_width;
    }
    while used < width {
        output.push(' ');
        used += 1;
    }
    output
}

#[derive(Debug, Clone)]
pub struct DisplayLine {
    pub seq: u64,
    pub source: String,
    pub text: String,
    pub source_style: Style,
    /// Color of the leading "●" marker for TX/actor-attributed rows. `None`
    /// renders a two-space indent instead (device RX rows, system rows, gaps).
    pub marker_color: Option<Color>,
    /// Whole-line style for system/gap rows. `None` selects inline keyword and
    /// prompt highlighting (stream rows) at render time.
    pub solid_style: Option<Style>,
    /// Run lifecycle rows are rendered as full-width scope boundaries while
    /// the projected terminal cursor remains untouched.
    pub run_boundary: Option<RunBoundary>,
    /// Set when the device echo of this TX row was received and merged: the
    /// renderer switches the leading marker from "●" to "✓".
    pub echoed: bool,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunBoundary {
    Started,
    Ended,
    Aborted,
}

/// The bounded amount of one unterminated terminal row retained in memory.
/// Long binary/no-newline streams are committed in readable chunks instead of
/// allowing a single Slot to grow without limit.
const MAX_STREAM_LINE_CHARS: usize = 16 * 1024;
const MAX_CSI_PARAMETER_BYTES: usize = 64;
/// A malformed remote control string must not hide an unbounded amount of
/// later UART output. Legitimate OSC/DCS/SOS/PM/APC sequences are normally
/// short and explicitly terminated; a larger one is discarded only up to
/// this bound before ordinary text projection resumes.
const MAX_CONTROL_STRING_BYTES: usize = 4 * 1024;
const MAX_ESCAPE_INTERMEDIATE_BYTES: usize = 16;
/// Maximum confirmed TX prefix retained while waiting for an exact device
/// echo. This is bounded independently from the scrollback and write queues.
const MAX_EXPECTED_ECHO_BYTES: usize = 64 * 1024;
const MAX_EXPECTED_ECHO_SEGMENTS: usize = 1_024;
const ERASE_ECHO: &[u8] = b"\x08 \x08";
/// Some target TTYs insert this sequence while echoing a command that crosses
/// their configured terminal column. It is presentation-only: the bytes were
/// not part of the confirmed TX and may be ignored only while the surrounding
/// RX still matches that exact TX expectation.
const ECHO_HARD_WRAP: &[u8] = b"\r\r\n";

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
    /// True only when the previously pending terminal row was committed.
    /// Standalone audit annotations can add a completed display row while
    /// deliberately preserving the live device cursor.
    pub pending_committed: bool,
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
    reconcile_echo: bool,
    expected_echoes: VecDeque<EchoExpectation>,
    expected_echo_bytes: usize,
    /// RX bytes that tentatively matched a TX prefix. They are discarded only
    /// after a complete expectation (or logical line segment) matches; a
    /// mismatch replays them so real device output can never be lost.
    echo_candidate: Vec<u8>,
    /// Candidate bytes abandoned when reconciliation is disabled between
    /// events. They are replayed before the next byte-bearing event.
    abandoned_rx: Vec<u8>,
    /// RX attribution for speculative/abandoned echo bytes. In particular,
    /// this keeps a partial echo separate from the locally projected TX row
    /// when a physical-session boundary forces the stream to flush.
    echo_candidate_context: Option<LineContext>,
    /// A matched TX `\r` may be echoed by the target as `\r\n`. The optional
    /// LF must not be mistaken for the start of the next pasted command.
    swallow_optional_echo_lf: bool,
    /// A locally projected TX line ended with CR but has not yet received the
    /// device newline. If the target responds with text directly, commit the
    /// visible command before drawing that text instead of overwriting it
    /// from column zero.
    tx_line_end_pending: bool,
}

impl TerminalStreamParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables terminal-style local TX projection with exact RX echo
    /// reconciliation. TX bytes are shown immediately in the current terminal
    /// row; a matching device echo is consumed byte-for-byte so echo-on
    /// targets do not render the command twice.
    pub fn set_echo_reconciliation(&mut self, enabled: bool) {
        if self.reconcile_echo != enabled {
            self.reconcile_echo = enabled;
            if !enabled {
                self.abandoned_rx.append(&mut self.echo_candidate);
            }
            self.expected_echoes.clear();
            self.expected_echo_bytes = 0;
            self.swallow_optional_echo_lf = false;
        }
    }

    /// Projects the next event. Byte-bearing RX/TX events participate in one
    /// terminal stream. Physical-session boundaries form committed system
    /// rows; bookkeeping events such as control and Run changes stay in the
    /// audit journal without moving the projected terminal cursor.
    ///
    /// Only daemon epoch or physical-generation changes force an
    /// unterminated row to commit. Direction and actor changes update the row
    /// attribution without splitting the visible terminal line.
    pub fn push_event(&mut self, event: &TimelineEvent) -> StreamDisplayBatch {
        let mut completed = Vec::new();
        let had_pending = self.pending_line().is_some();

        if event.direction == Direction::None {
            if terminal_boundary(event.kind) {
                completed.extend(self.flush());
                completed.extend(event_to_lines(event));
                return StreamDisplayBatch {
                    completed,
                    pending: None,
                    pending_committed: had_pending,
                };
            }
            if visible_terminal_annotation(event.kind) {
                completed.extend(event_to_lines(event));
            }
            return StreamDisplayBatch {
                completed,
                pending: self.pending_line(),
                pending_committed: false,
            };
        }

        let incoming = LineContext::from_event(event);
        if self
            .context
            .as_ref()
            .is_some_and(|current| current.identity != incoming.identity)
        {
            completed.extend(self.flush());
        }

        if !self.abandoned_rx.is_empty() {
            let replay = std::mem::take(&mut self.abandoned_rx);
            self.replay_buffered_rx(replay, &incoming, &mut completed);
        }

        let has_pending = self.terminal.pending_text().is_some();
        match &mut self.context {
            Some(current) if has_pending => {
                current.seq = event.seq;
                if event.direction == Direction::Tx {
                    current.adopt_tx(&incoming);
                }
            }
            _ => self.context = Some(incoming.clone()),
        }

        if event.direction == Direction::Tx && self.reconcile_echo && !event.data.is_empty() {
            let fits_byte_budget = self.expected_echo_bytes.saturating_add(event.data.len())
                <= MAX_EXPECTED_ECHO_BYTES;
            let can_extend_tail = self
                .expected_echoes
                .back()
                .is_some_and(|expectation| expectation.can_append(&event.data));
            if fits_byte_budget
                && (can_extend_tail || self.expected_echoes.len() < MAX_EXPECTED_ECHO_SEGMENTS)
            {
                self.expected_echo_bytes += event.data.len();
                if can_extend_tail {
                    self.expected_echoes
                        .back_mut()
                        .expect("an appendable echo tail exists")
                        .append(&event.data);
                } else {
                    self.expected_echoes
                        .push_back(EchoExpectation::new(&event.data));
                }
            } else {
                // Do not retain a partial expectation: it could consume an
                // unrelated RX prefix after a large write.
                self.abandoned_rx.append(&mut self.echo_candidate);
                self.expected_echoes.clear();
                self.expected_echo_bytes = 0;
                self.swallow_optional_echo_lf = false;
            }
        }

        for &byte in &event.data {
            if event.direction == Direction::Rx {
                match self.reconcile_rx_byte(byte, &incoming) {
                    EchoDisposition::Suppressed {
                        expectation_complete,
                    } => {
                        if expectation_complete && let Some(context) = self.context.as_mut() {
                            context.echoed = true;
                        }
                        continue;
                    }
                    EchoDisposition::Visible(bytes) => {
                        for visible in bytes {
                            self.feed_visible_byte(visible, &incoming, &mut completed);
                        }
                    }
                }
                continue;
            }

            if matches!(byte, 0x08 | 0x7f) {
                // RAW input is projected optimistically. Model a local
                // destructive backspace so both a literal DEL echo and the
                // common Linux TTY BS-space-BS echo can be suppressed without
                // applying the edit twice.
                for erase_byte in ERASE_ECHO {
                    self.feed_visible_byte(*erase_byte, &incoming, &mut completed);
                }
            } else {
                self.feed_visible_byte(byte, &incoming, &mut completed);
            }
        }

        StreamDisplayBatch {
            pending_committed: had_pending && !completed.is_empty(),
            completed,
            pending: self.pending_line(),
        }
    }

    fn reconcile_rx_byte(&mut self, byte: u8, incoming: &LineContext) -> EchoDisposition {
        if !self.reconcile_echo {
            return EchoDisposition::Visible(vec![byte]);
        }
        if self.swallow_optional_echo_lf {
            self.swallow_optional_echo_lf = false;
            if byte == b'\n' {
                return EchoDisposition::Suppressed {
                    expectation_complete: false,
                };
            }
        }
        let Some(expectation) = self.expected_echoes.front_mut() else {
            return EchoDisposition::Visible(vec![byte]);
        };

        self.echo_candidate.push(byte);
        self.echo_candidate_context = Some(incoming.clone());
        expectation.search_started = true;
        match expectation.match_candidate(&self.echo_candidate) {
            EchoCandidateMatch::Full => {
                let matched_cr = expectation.ends_in_cr();
                let matched_len = expectation.bytes.len();
                self.echo_candidate.clear();
                self.echo_candidate_context = None;
                self.expected_echoes.pop_front();
                self.expected_echo_bytes = self.expected_echo_bytes.saturating_sub(matched_len);
                let expectation_complete = self.expected_echoes.is_empty();
                let next_expectation_starts_in_lf = self
                    .expected_echoes
                    .front()
                    .is_some_and(EchoExpectation::starts_in_lf);
                self.swallow_optional_echo_lf = matched_cr
                    && !next_expectation_starts_in_lf
                    && (!expectation_complete || !self.tx_line_end_pending);
                return EchoDisposition::Suppressed {
                    expectation_complete,
                };
            }
            EchoCandidateMatch::Prefix => {
                return EchoDisposition::Suppressed {
                    expectation_complete: false,
                };
            }
            EchoCandidateMatch::Mismatch => {}
        }

        // Echo suppression is deliberately adjacency-biased. Once the first
        // RX bytes prove that the oldest expectation is not the immediate
        // device echo, replay the whole speculative candidate and abandon all
        // pending expectations. A later echo may therefore remain visible,
        // but boot logs, password prompts, and other device data can never be
        // searched for matching bytes and silently consumed.
        let visible = std::mem::take(&mut self.echo_candidate);
        self.echo_candidate_context = None;
        self.expected_echoes.clear();
        self.expected_echo_bytes = 0;
        self.swallow_optional_echo_lf = false;
        EchoDisposition::Visible(visible)
    }

    fn feed_visible_byte(
        &mut self,
        byte: u8,
        incoming: &LineContext,
        completed: &mut Vec<DisplayLine>,
    ) {
        if self.tx_line_end_pending && !matches!(byte, b'\r' | b'\n') {
            self.commit_pending_row(incoming, completed);
        }

        let mut rows = Vec::new();
        self.terminal.consume(byte, &mut rows);
        if byte == b'\n' {
            self.tx_line_end_pending = false;
        } else if incoming.direction == Direction::Tx && byte == b'\r' {
            self.tx_line_end_pending = true;
        }
        if rows.is_empty() {
            return;
        }

        let context = self
            .context
            .as_ref()
            .cloned()
            .unwrap_or_else(|| incoming.clone());
        completed.extend(rows.drain(..).map(|text| context.display_line(text)));
        // A newline ended any prior mixed RX/TX row. Remaining bytes in this
        // event belong to the incoming event's source.
        self.context = Some(incoming.clone());
    }

    fn commit_pending_row(&mut self, incoming: &LineContext, completed: &mut Vec<DisplayLine>) {
        self.tx_line_end_pending = false;
        let mut rows = Vec::new();
        self.terminal.consume(b'\n', &mut rows);
        let context = self
            .context
            .as_ref()
            .cloned()
            .unwrap_or_else(|| incoming.clone());
        completed.extend(rows.into_iter().map(|text| context.display_line(text)));
        self.context = Some(incoming.clone());
    }

    fn replay_echo_candidate(&mut self, completed: &mut Vec<DisplayLine>) {
        let mut replay = std::mem::take(&mut self.abandoned_rx);
        replay.append(&mut self.echo_candidate);
        if replay.is_empty() {
            return;
        }
        let fallback = self.context.clone();
        let Some(fallback) = fallback.as_ref() else {
            debug_assert!(false, "echo candidates always have a stream context");
            return;
        };
        self.replay_buffered_rx(replay, fallback, completed);
    }

    fn replay_buffered_rx(
        &mut self,
        replay: Vec<u8>,
        fallback: &LineContext,
        completed: &mut Vec<DisplayLine>,
    ) {
        let incoming = self
            .echo_candidate_context
            .take()
            .unwrap_or_else(|| fallback.clone());
        if self
            .context
            .as_ref()
            .is_some_and(|current| current.direction == Direction::Tx)
            && incoming.direction == Direction::Rx
            && self.terminal.pending_text().is_some()
        {
            self.commit_pending_row(&incoming, completed);
        }
        for byte in replay {
            self.feed_visible_byte(byte, &incoming, completed);
        }
    }

    /// Commits an unterminated row and resets all decoder/escape state.
    /// A truncated UTF-8 scalar is rendered as U+FFFD; an unfinished escape
    /// sequence is discarded because replaying it would be unsafe.
    pub fn flush(&mut self) -> Vec<DisplayLine> {
        let mut lines = Vec::new();
        self.replay_echo_candidate(&mut lines);
        let completed_rows = self.terminal.finish_input();
        if let Some(context) = self.context.as_ref() {
            lines.extend(
                completed_rows
                    .into_iter()
                    .map(|text| context.display_line(text)),
            );
            if let Some(text) = self.terminal.take_pending() {
                lines.push(context.display_line(text));
            }
        }
        self.terminal.reset();
        self.context = None;
        self.expected_echoes.clear();
        self.expected_echo_bytes = 0;
        self.echo_candidate.clear();
        self.abandoned_rx.clear();
        self.echo_candidate_context = None;
        self.swallow_optional_echo_lf = false;
        self.tx_line_end_pending = false;
        lines
    }

    /// Drops all buffered text and decoder state without producing output.
    /// Use this when an authoritative snapshot invalidates the old stream.
    pub fn reset(&mut self) {
        self.terminal.reset();
        self.context = None;
        self.expected_echoes.clear();
        self.expected_echo_bytes = 0;
        self.echo_candidate.clear();
        self.abandoned_rx.clear();
        self.echo_candidate_context = None;
        self.swallow_optional_echo_lf = false;
        self.tx_line_end_pending = false;
    }

    pub fn pending_line(&self) -> Option<DisplayLine> {
        let context = self.context.as_ref()?;
        self.terminal
            .pending_text()
            .map(|text| context.display_line(text))
    }
}

fn terminal_boundary(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::SerialOpening
            | EventKind::SerialOpened
            | EventKind::SerialOpenFailed
            | EventKind::SerialClosed
            | EventKind::SlotRemoved
            | EventKind::Gap
    )
}

fn visible_terminal_annotation(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::RunStarted
            | EventKind::RunEnded
            | EventKind::RunAborted
            | EventKind::Break
            | EventKind::TriggerStarted
            | EventKind::TriggerCompleted
            | EventKind::TriggerCancelled
            | EventKind::TriggerFailed
    )
}

#[derive(Debug)]
struct EchoExpectation {
    bytes: Vec<u8>,
    /// Linux TTYs commonly echo a transmitted DEL as BS-space-BS.
    accept_erase_echo: bool,
    /// Once RX has been compared with this expectation, later TX must form a
    /// new expectation rather than extending the byte sequence being matched.
    search_started: bool,
}

impl EchoExpectation {
    fn new(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.to_vec(),
            accept_erase_echo: bytes == [0x7f],
            search_started: false,
        }
    }

    fn can_append(&self, bytes: &[u8]) -> bool {
        !self.search_started
            && !self.accept_erase_echo
            && !matches!(self.bytes.last(), Some(b'\r' | b'\n'))
            && !bytes.iter().any(|byte| matches!(byte, 0x08 | 0x7f))
    }

    fn append(&mut self, bytes: &[u8]) {
        debug_assert!(self.can_append(bytes));
        self.bytes.extend_from_slice(bytes);
    }

    fn match_candidate(&self, candidate: &[u8]) -> EchoCandidateMatch {
        if self.accept_erase_echo {
            if candidate == self.bytes || candidate == b"\x08" || candidate == ERASE_ECHO {
                return EchoCandidateMatch::Full;
            }
            if self.bytes.starts_with(candidate) || ERASE_ECHO.starts_with(candidate) {
                return EchoCandidateMatch::Prefix;
            }
            return EchoCandidateMatch::Mismatch;
        }

        let mut expected = 0usize;
        let mut actual = 0usize;
        while actual < candidate.len() {
            if expected < self.bytes.len() && candidate[actual] == self.bytes[expected] {
                expected += 1;
                actual += 1;
                continue;
            }

            // This exact sequence was observed from the target TTY at its
            // configured column boundary. Accept a complete or split sequence
            // only between two expected TX bytes. It stays speculative until
            // the rest of the exact TX matches, so a later mismatch replays
            // every byte and cannot hide real output.
            if expected > 0 && expected < self.bytes.len() {
                let remaining = &candidate[actual..];
                if ECHO_HARD_WRAP.starts_with(remaining) {
                    return EchoCandidateMatch::Prefix;
                }
                if remaining.starts_with(ECHO_HARD_WRAP) {
                    actual += ECHO_HARD_WRAP.len();
                    continue;
                }
            }
            return EchoCandidateMatch::Mismatch;
        }

        if expected == self.bytes.len() {
            EchoCandidateMatch::Full
        } else {
            EchoCandidateMatch::Prefix
        }
    }

    fn ends_in_cr(&self) -> bool {
        self.bytes.last().copied() == Some(b'\r')
    }

    fn starts_in_lf(&self) -> bool {
        self.bytes.first().copied() == Some(b'\n')
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EchoCandidateMatch {
    Full,
    Prefix,
    Mismatch,
}

enum EchoDisposition {
    Suppressed { expectation_complete: bool },
    Visible(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StreamIdentity {
    daemon_epoch: uuid::Uuid,
    generation: u64,
}

#[derive(Debug, Clone)]
struct LineContext {
    identity: StreamIdentity,
    seq: u64,
    source: String,
    source_style: Style,
    direction: Direction,
    kind: EventKind,
    actor_kind: Option<ActorKind>,
    echoed: bool,
}

impl LineContext {
    fn from_event(event: &TimelineEvent) -> Self {
        Self {
            identity: StreamIdentity {
                daemon_epoch: event.daemon_epoch,
                generation: event.generation,
            },
            seq: event.seq,
            source: source_label(event),
            source_style: source_style(event),
            direction: event.direction,
            kind: event.kind,
            actor_kind: event.actor.as_ref().map(|actor| actor.kind),
            echoed: false,
        }
    }

    fn adopt_tx(&mut self, incoming: &Self) {
        self.seq = incoming.seq;
        self.source.clone_from(&incoming.source);
        self.source_style = incoming.source_style;
        self.direction = incoming.direction;
        self.kind = incoming.kind;
        self.actor_kind = incoming.actor_kind;
        self.echoed = false;
    }

    fn display_line(&self, text: String) -> DisplayLine {
        DisplayLine {
            seq: self.seq,
            source: self.source.clone(),
            bytes: text.len() + self.source.len() + 16,
            source_style: self.source_style,
            marker_color: marker_color(self.direction, self.actor_kind),
            solid_style: solid_style(self.direction, self.kind),
            run_boundary: None,
            echoed: self.echoed,
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
    escape_payload_bytes: usize,
}

impl TerminalTextState {
    fn consume(&mut self, byte: u8, rows: &mut Vec<String>) {
        if self.escape != EscapeState::Ground {
            self.consume_escape(byte, rows);
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
            b'\n' => self.commit_row(rows),
            b'\r' => {
                self.cursor = 0;
                self.touched |= !self.line.is_empty();
            }
            0x08 | 0x7f => {
                self.cursor = self.cursor.saturating_sub(1);
                self.touched |= !self.line.is_empty();
            }
            b'\t' => {
                let next_tab = ((self.cursor / 8) + 1) * 8;
                while self.cursor < next_tab {
                    self.write_char(' ', rows);
                }
            }
            0x00..=0x1f => {}
            0x20..=0x7e => self.write_char(char::from(byte), rows),
            _ => {
                self.utf8.push(byte);
                self.drain_utf8(false, rows);
            }
        }
    }

    fn consume_escape(&mut self, byte: u8, rows: &mut Vec<String>) {
        // ECMA-48 CAN/SUB cancel any in-progress escape or control string.
        // They are safe synchronization points for a noisy UART stream.
        if matches!(byte, 0x18 | 0x1a) {
            self.reset_escape();
            return;
        }
        match self.escape {
            EscapeState::Ground => unreachable!("ground escapes are handled by consume"),
            EscapeState::Escape => match byte {
                b'[' => self.start_csi(),
                b']' | b'P' | b'X' | b'^' | b'_' => self.start_control_string(),
                0x20..=0x2f => {
                    self.escape_payload_bytes = 1;
                    self.escape = EscapeState::EscapeIntermediate;
                }
                0x1b => self.escape_payload_bytes = 0,
                _ => self.reset_escape(),
            },
            EscapeState::EscapeIntermediate => match byte {
                0x1b => {
                    self.escape_payload_bytes = 0;
                    self.escape = EscapeState::Escape;
                }
                b'\r' | b'\n' => self.recover_escape_with(byte, rows),
                0x20..=0x2f => {
                    self.escape_payload_bytes += 1;
                    if self.escape_payload_bytes > MAX_ESCAPE_INTERMEDIATE_BYTES {
                        self.reset_escape();
                    }
                }
                0x30..=0x7e => self.reset_escape(),
                _ => self.recover_escape_with(byte, rows),
            },
            EscapeState::Csi => {
                if byte == 0x1b {
                    self.reset_escape();
                    self.escape = EscapeState::Escape;
                } else if matches!(byte, b'\r' | b'\n') {
                    self.recover_escape_with(byte, rows);
                } else if (0x40..=0x7e).contains(&byte) {
                    let parameters = std::mem::take(&mut self.csi_parameters);
                    self.reset_escape();
                    self.apply_csi(byte, &parameters);
                } else if (0x20..=0x3f).contains(&byte)
                    && self.csi_parameters.len() < MAX_CSI_PARAMETER_BYTES
                {
                    self.csi_parameters.push(byte);
                } else if (0x20..=0x3f).contains(&byte) {
                    self.reset_escape();
                } else {
                    self.recover_escape_with(byte, rows);
                }
            }
            EscapeState::ControlString => match byte {
                0x07 | 0x9c => self.reset_escape(),
                b'\r' | b'\n' => self.recover_escape_with(byte, rows),
                0x1b => {
                    if self.bump_control_string() {
                        self.escape = EscapeState::ControlStringEscape;
                    }
                }
                _ => {
                    self.bump_control_string();
                }
            },
            EscapeState::ControlStringEscape => match byte {
                b'\\' | 0x9c => self.reset_escape(),
                b'\r' | b'\n' => self.recover_escape_with(byte, rows),
                0x1b => {
                    self.bump_control_string();
                }
                _ => {
                    if self.bump_control_string() {
                        self.escape = EscapeState::ControlString;
                    }
                }
            },
        }
    }

    fn start_csi(&mut self) {
        self.csi_parameters.clear();
        self.escape_payload_bytes = 0;
        self.escape = EscapeState::Csi;
    }

    fn start_control_string(&mut self) {
        self.escape_payload_bytes = 0;
        self.escape = EscapeState::ControlString;
    }

    fn bump_control_string(&mut self) -> bool {
        self.escape_payload_bytes += 1;
        if self.escape_payload_bytes > MAX_CONTROL_STRING_BYTES {
            self.reset_escape();
            false
        } else {
            true
        }
    }

    fn recover_escape_with(&mut self, byte: u8, rows: &mut Vec<String>) {
        self.reset_escape();
        self.consume(byte, rows);
    }

    fn reset_escape(&mut self) {
        self.escape = EscapeState::Ground;
        self.csi_parameters.clear();
        self.escape_payload_bytes = 0;
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
        self.reset_escape();
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

pub fn event_to_lines(event: &TimelineEvent) -> Vec<DisplayLine> {
    let source = source_label(event);
    let source_style = source_style(event);
    let marker_color = marker_color(
        event.direction,
        event.actor.as_ref().map(|actor| actor.kind),
    );
    let solid_style = solid_style(event.direction, event.kind);
    let run_boundary = match event.kind {
        EventKind::RunStarted => Some(RunBoundary::Started),
        EventKind::RunEnded => Some(RunBoundary::Ended),
        EventKind::RunAborted => Some(RunBoundary::Aborted),
        _ => None,
    };
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
        .map(|text| DisplayLine {
            seq: event.seq,
            source: source.clone(),
            bytes: text.len() + source.len() + 16,
            source_style,
            marker_color,
            solid_style,
            run_boundary,
            echoed: false,
            text,
        })
        .collect()
}

pub fn gap_line(seq: u64, text: impl Into<String>) -> DisplayLine {
    let text = text.into();
    DisplayLine {
        seq,
        source: tr("d.gap").into(),
        bytes: text.len() + 20,
        source_style: Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        marker_color: None,
        solid_style: Some(
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        ),
        run_boundary: None,
        echoed: false,
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
        Direction::Rx => tr("d.dev").into(),
        Direction::Tx => event
            .actor
            .as_ref()
            .map(|actor| compact_actor_label(actor, true))
            .unwrap_or_else(|| tr("d.tx").into()),
        Direction::None => event
            .actor
            .as_ref()
            .map(|actor| compact_actor_label(actor, false))
            .unwrap_or_else(|| tr("d.system").into()),
    }
}

fn audit_source_label(event: &TimelineEvent) -> String {
    match event.direction {
        Direction::Rx => tr("d.dev").into(),
        Direction::Tx => event
            .actor
            .as_ref()
            .map(|actor| full_actor_label(actor, true))
            .unwrap_or_else(|| tr("d.tx").into()),
        Direction::None => event
            .actor
            .as_ref()
            .map(|actor| full_actor_label(actor, false))
            .unwrap_or_else(|| tr("d.system").into()),
    }
}

fn actor_kind_label(kind: ActorKind) -> &'static str {
    match kind {
        ActorKind::Human => tr("d.kind.human"),
        ActorKind::Agent => tr("d.kind.agent"),
        ActorKind::Script => tr("d.kind.script"),
        ActorKind::System => tr("d.kind.system"),
    }
}

fn compact_actor_label(actor: &serial_protocol::Actor, write: bool) -> String {
    let label = truncate_inline(&actor.label, 12);
    let id = safe_inline(&actor.id);
    let short_id = id.chars().rev().take(8).collect::<String>();
    let short_id = short_id.chars().rev().collect::<String>();
    format!(
        "{}[{}]:{}{}",
        actor_kind_label(actor.kind),
        short_id,
        label,
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
        EventKind::Rx => tr("d.ev.rx"),
        EventKind::Tx => tr("d.ev.tx"),
        EventKind::SerialOpening => tr("d.ev.serial_opening"),
        EventKind::SerialOpened => tr("d.ev.serial_opened"),
        EventKind::SerialOpenFailed => tr("d.ev.serial_open_failed"),
        EventKind::SerialClosed => tr("d.ev.serial_closed"),
        EventKind::SlotReconfigured => tr("d.ev.slot_reconfigured"),
        EventKind::SlotRemoved => tr("d.ev.slot_removed"),
        EventKind::ControlGranted => tr("d.ev.control_granted"),
        EventKind::ControlReleased => tr("d.ev.control_released"),
        EventKind::ControlRevoked => tr("d.ev.control_revoked"),
        EventKind::ControlExpired => tr("d.ev.control_expired"),
        EventKind::RunStarted => tr("d.ev.run_started"),
        EventKind::RunEnded => tr("d.ev.run_ended"),
        EventKind::RunAborted => tr("d.ev.run_aborted"),
        EventKind::TriggerStarted => tr("d.ev.trigger_started"),
        EventKind::TriggerCompleted => tr("d.ev.trigger_completed"),
        EventKind::TriggerCancelled => tr("d.ev.trigger_cancelled"),
        EventKind::TriggerFailed => tr("d.ev.trigger_failed"),
        EventKind::Break => tr("d.ev.break"),
        EventKind::Checkpoint => tr("d.ev.checkpoint"),
        EventKind::LoggingDegraded => tr("d.ev.logging_degraded"),
        EventKind::Gap => tr("d.ev.gap"),
    }
}

/// Stable protocol spelling for one Trigger state in human-readable status
/// output. Labels stay device-agnostic and intentionally do not infer a
/// bootloader or flashing result from a matched literal.
pub fn trigger_status_label(status: TriggerStatus) -> &'static str {
    match status {
        TriggerStatus::Armed => "armed",
        TriggerStatus::WaitingForStart => "waiting_for_start",
        TriggerStatus::Running => "running",
        TriggerStatus::Stopping => "stopping",
        TriggerStatus::Matched => "matched",
        TriggerStatus::TimedOut => "timed_out",
        TriggerStatus::MaxFiresReached => "max_fires_reached",
        TriggerStatus::Cancelled => "cancelled",
        TriggerStatus::ControlLost => "control_lost",
        TriggerStatus::RunLost => "run_lost",
        TriggerStatus::GenerationChanged => "generation_changed",
        TriggerStatus::PortClosed => "port_closed",
        TriggerStatus::WriteFailed => "write_failed",
        TriggerStatus::RxGap => "rx_gap",
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

/// Whole-line style for non-stream rows. Stream rows return `None` so the
/// renderer applies inline keyword/prompt spans instead of one flat color.
fn solid_style(direction: Direction, kind: EventKind) -> Option<Style> {
    if direction != Direction::None {
        return None;
    }
    Some(match kind {
        EventKind::Gap | EventKind::LoggingDegraded | EventKind::SerialOpenFailed => {
            Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD)
        }
        _ => Style::default().fg(Color::DarkGray),
    })
}

/// Leading-dot color for TX/actor-attributed rows; device RX rows and rows
/// without an actor get no dot and are indented instead.
fn marker_color(direction: Direction, actor_kind: Option<ActorKind>) -> Option<Color> {
    match direction {
        Direction::Rx => None,
        Direction::Tx => Some(match actor_kind {
            Some(ActorKind::Human) => Color::Green,
            Some(ActorKind::Agent) => Color::Magenta,
            Some(ActorKind::Script) => Color::Yellow,
            Some(ActorKind::System) | None => Color::Blue,
        }),
        Direction::None => actor_kind.map(|_| Color::Blue),
    }
}

const ERROR_KEYWORDS: &[&str] = &["error", "failed", "failure", "panic", "fatal", "assert"];
const WARNING_KEYWORDS: &[&str] = &["warn", "timeout", "retry", "dropped"];
const SUCCESS_KEYWORDS: &[&str] = &["success", "passed", "pass", "ready", "[ok]"];
const INFO_KEYWORDS: &[&str] = &["info", "notice"];
const DEBUG_KEYWORDS: &[&str] = &["debug", "trace"];

/// MobaXterm-style inline highlight spans for one stream row.
///
/// Returns non-overlapping `(start_byte, end_byte, Style)` spans sorted by
/// start. Only the matched keyword itself is colored; the rest of the row
/// keeps the default foreground. Class priority is error over warning over
/// success over info over debug; within one class the longer keyword wins
/// overlaps. A trailing prompt (configured shell/U-Boot prompt, or the
/// fallback ` #`, ` $`, ` >` line endings) is colored LightCyan+Bold and
/// takes precedence over keywords.
pub fn highlight_spans(
    text: &str,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> Vec<(usize, usize, Style)> {
    let mut spans: Vec<(usize, usize, Style)> = Vec::new();
    if let Some((start, end)) = prompt_range(text, shell_prompt, uboot_prompt) {
        spans.push((start, end, prompt_style()));
    }

    // to_ascii_lowercase only remaps A-Z, so byte offsets stay valid for the
    // original text.
    let lowercase = text.to_ascii_lowercase();
    let mut candidates: Vec<(usize, usize, usize)> = Vec::new();
    for (rank, keywords) in [
        ERROR_KEYWORDS,
        WARNING_KEYWORDS,
        SUCCESS_KEYWORDS,
        INFO_KEYWORDS,
        DEBUG_KEYWORDS,
    ]
    .into_iter()
    .enumerate()
    {
        for keyword in keywords {
            let mut from = 0;
            while let Some(found) = lowercase[from..].find(keyword) {
                let start = from + found;
                let end = start + keyword.len();
                if keyword_has_boundaries(text, start, end) {
                    candidates.push((start, end, rank));
                }
                from = start + 1;
            }
        }
    }
    candidates.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .then((right.1 - right.0).cmp(&(left.1 - left.0)))
    });
    for (start, end, rank) in candidates {
        if spans
            .iter()
            .any(|(kept_start, kept_end, _)| start < *kept_end && *kept_start < end)
        {
            continue;
        }
        spans.push((start, end, keyword_style(rank)));
    }
    spans.sort_by_key(|(start, _, _)| *start);
    spans
}

/// Keywords are tokens, not arbitrary substrings. Underscore, hyphen,
/// punctuation, brackets, colons and whitespace are boundaries; only a
/// Unicode letter or digit suppresses a match on either side.
fn keyword_has_boundaries(text: &str, start: usize, end: usize) -> bool {
    let left_is_alphanumeric = text[..start]
        .chars()
        .next_back()
        .is_some_and(char::is_alphanumeric);
    let right_is_alphanumeric = text[end..]
        .chars()
        .next()
        .is_some_and(char::is_alphanumeric);
    !left_is_alphanumeric && !right_is_alphanumeric
}

fn prompt_range(
    text: &str,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> Option<(usize, usize)> {
    let configured = [shell_prompt, uboot_prompt]
        .into_iter()
        .flatten()
        .filter(|prompt| !prompt.is_empty())
        .filter_map(|prompt| {
            text.rfind(prompt)
                .map(|start| (start, start + prompt.len()))
        })
        .max_by_key(|(start, end)| (*end, end - start));
    if configured.is_some() {
        return configured;
    }
    let trimmed = text.trim_end();
    for marker in [" #", " $", " >"] {
        if trimmed.ends_with(marker) {
            return Some((trimmed.len() - 1, trimmed.len()));
        }
    }
    None
}

fn prompt_style() -> Style {
    Style::default()
        .fg(Color::LightCyan)
        .add_modifier(Modifier::BOLD)
}

fn keyword_style(rank: usize) -> Style {
    match rank {
        0 => Style::default()
            .fg(Color::LightRed)
            .add_modifier(Modifier::BOLD),
        1 => Style::default().fg(Color::Yellow),
        2 => Style::default().fg(Color::LightGreen),
        3 => Style::default().fg(Color::LightBlue),
        _ => Style::default().fg(Color::DarkGray),
    }
}

fn system_event_text(event: &TimelineEvent) -> String {
    if let Some(message) = event
        .metadata
        .get("message")
        .and_then(|value| value.as_str())
    {
        return safe_inline(message);
    }
    if matches!(
        event.kind,
        EventKind::RunStarted | EventKind::RunEnded | EventKind::RunAborted
    ) {
        let title = match event.kind {
            EventKind::RunStarted => tr("d.run.start"),
            EventKind::RunEnded => tr("d.run.end"),
            EventKind::RunAborted => tr("d.run.abort"),
            _ => unreachable!("guarded by matches"),
        };
        let run = event.metadata.get("run");
        let label = run
            .and_then(|value| value.get("label"))
            .and_then(|value| value.as_str())
            .map(safe_inline)
            .filter(|value| !value.is_empty());
        let short_id = run
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str())
            .map(|value| value.chars().take(8).collect::<String>());
        let reason = (event.kind == EventKind::RunAborted)
            .then(|| event.metadata.get("reason"))
            .flatten()
            .and_then(|value| value.as_str())
            .map(safe_inline)
            .filter(|value| !value.is_empty());
        return [Some(title.to_string()), label, short_id, reason]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
    }
    if event.kind == EventKind::Break {
        return event
            .metadata
            .get("duration_ms")
            .and_then(|value| value.as_u64())
            .map_or_else(
                || tr("d.ev.break").to_string(),
                |duration| trf("d.break.duration", &[&duration.to_string()]),
            );
    }
    if matches!(
        event.kind,
        EventKind::TriggerStarted
            | EventKind::TriggerCompleted
            | EventKind::TriggerCancelled
            | EventKind::TriggerFailed
    ) {
        let kind = event_kind_label(event.kind);
        event
            .metadata
            .get("status")
            .and_then(|value| serde_json::from_value::<TriggerStatus>(value.clone()).ok())
            .map_or_else(
                || kind.to_string(),
                |status| format!("{kind}: {}", trigger_status_label(status)),
            )
    } else {
        format!("{:?}", event.kind)
    }
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
        let _guard = crate::i18n::lang_test_lock();
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
        let _guard = crate::i18n::lang_test_lock();
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

        let line = event_to_lines(&tx).remove(0);
        assert_eq!(line.source, "AGENT[12345678]:worker-a>");
        assert_eq!(line.marker_color, Some(Color::Magenta));
    }

    #[test]
    fn trigger_tx_keeps_the_ordinary_agent_tx_presentation() {
        let _guard = crate::i18n::lang_test_lock();
        let mut tx = event(b"slp");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "agent:trigger-1".into(),
            label: "boot-window".into(),
            kind: ActorKind::Agent,
        });
        tx.metadata
            .insert("trigger_id".into(), serde_json::json!(Uuid::new_v4()));
        tx.metadata
            .insert("trigger_write_kind".into(), serde_json::json!("action"));
        tx.metadata
            .insert("fire_index".into(), serde_json::json!(7));

        let line = event_to_lines(&tx).remove(0);

        assert_eq!(line.source, "AGENT[rigger-1]:boot-window>");
        assert_eq!(line.marker_color, Some(Color::Magenta));
        assert!(line.solid_style.is_none());
        assert_eq!(line.text, "slp");
        assert!(format_event_plain(&tx).contains("tx/AGENT:boot-window[agent:trigger-1]>"));
    }

    #[test]
    fn trigger_lifecycle_labels_are_available_in_both_languages() {
        let _guard = crate::i18n::lang_test_lock();
        let mut started = event(&[]);
        started.direction = Direction::None;
        started.kind = EventKind::TriggerStarted;
        started
            .metadata
            .insert("status".into(), serde_json::json!("waiting_for_start"));

        assert!(format_event_plain(&started).contains("trigger_started: waiting_for_start"));
        assert_eq!(
            event_to_lines(&started).remove(0).text,
            "trigger_started: waiting_for_start"
        );
        crate::i18n::set_lang(crate::i18n::Lang::Zh);
        assert!(format_event_plain(&started).contains("触发任务已启动"));
        crate::i18n::set_lang(crate::i18n::Lang::En);
    }

    #[test]
    fn every_terminal_trigger_lifecycle_row_exposes_metadata_status() {
        let _guard = crate::i18n::lang_test_lock();
        for (kind, status) in [
            (EventKind::TriggerCompleted, TriggerStatus::Matched),
            (EventKind::TriggerCompleted, TriggerStatus::TimedOut),
            (EventKind::TriggerCompleted, TriggerStatus::MaxFiresReached),
            (EventKind::TriggerCancelled, TriggerStatus::Cancelled),
            (EventKind::TriggerFailed, TriggerStatus::ControlLost),
            (EventKind::TriggerFailed, TriggerStatus::WriteFailed),
        ] {
            let mut terminal = event(&[]);
            terminal.direction = Direction::None;
            terminal.kind = kind;
            terminal
                .metadata
                .insert("status".into(), serde_json::json!(status));

            let expected = trigger_status_label(status);
            assert!(format_event_plain(&terminal).contains(expected));
            assert!(event_to_lines(&terminal).remove(0).text.contains(expected));
        }
    }

    #[test]
    fn trigger_annotation_preserves_the_live_terminal_cursor() {
        let _guard = crate::i18n::lang_test_lock();
        let mut parser = TerminalStreamParser::new();
        let prompt = parser.push_event(&event_at(1, b"shell# "));
        assert_eq!(prompt.pending.unwrap().text, "shell# ");

        let mut completed = event(&[]);
        completed.seq = 2;
        completed.direction = Direction::None;
        completed.kind = EventKind::TriggerCompleted;
        completed
            .metadata
            .insert("status".into(), serde_json::json!("matched"));
        let annotation = parser.push_event(&completed);

        assert!(!annotation.pending_committed);
        assert_eq!(annotation.completed.len(), 1);
        assert_eq!(annotation.completed[0].text, "trigger_completed: matched");
        assert_eq!(annotation.pending.unwrap().text, "shell# ");
    }

    #[test]
    fn run_lifecycle_is_a_visible_boundary_without_committing_the_prompt() {
        let _guard = crate::i18n::lang_test_lock();
        let mut parser = TerminalStreamParser::new();
        let prompt = parser.push_event(&event_at(1, b"shell# "));
        assert_eq!(prompt.pending.unwrap().text, "shell# ");

        let run_id = Uuid::new_v4();
        let mut started = event(&[]);
        started.seq = 2;
        started.direction = Direction::None;
        started.kind = EventKind::RunStarted;
        started.metadata.insert(
            "run".into(),
            serde_json::json!({"id": run_id, "label": "network-test"}),
        );
        let boundary = parser.push_event(&started);

        assert!(!boundary.pending_committed);
        assert_eq!(boundary.completed.len(), 1);
        assert_eq!(
            boundary.completed[0].run_boundary,
            Some(RunBoundary::Started)
        );
        assert!(boundary.completed[0].text.contains("RUN START"));
        assert!(boundary.completed[0].text.contains("network-test"));
        assert_eq!(boundary.pending.unwrap().text, "shell# ");
    }

    #[test]
    fn trigger_status_uses_stable_protocol_spelling() {
        assert_eq!(
            trigger_status_label(TriggerStatus::WaitingForStart),
            "waiting_for_start"
        );
        assert_eq!(
            trigger_status_label(TriggerStatus::MaxFiresReached),
            "max_fires_reached"
        );
        assert_eq!(trigger_status_label(TriggerStatus::RxGap), "rx_gap");
    }

    #[test]
    fn local_time_formatter_handles_negative_epoch_values() {
        let rendered = format_wall_time_local(-1);
        assert!(rendered.contains(".999"));
        assert!(!rendered.ends_with("ns"));
    }

    #[test]
    fn prompt_and_error_keywords_get_inline_spans() {
        let spans = highlight_spans("SigmaStar #", None, Some("SigmaStar #"));
        assert_eq!(spans, vec![(0, 11, prompt_style())]);

        let spans = highlight_spans("FATAL: boot failed", None, None);
        assert_eq!(
            spans,
            vec![(0, 5, keyword_style(0)), (12, 18, keyword_style(0)),]
        );
    }

    #[test]
    fn keyword_priority_prefers_error_then_the_longer_match() {
        let spans = highlight_spans("ready pass", None, None);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0], (0, 5, keyword_style(2)));
        assert_eq!(spans[1], (6, 10, keyword_style(2)));

        let spans = highlight_spans("pass passed", None, None);
        assert_eq!(
            spans,
            vec![(0, 4, keyword_style(2)), (5, 11, keyword_style(2))]
        );

        let spans = highlight_spans("timeout error", None, None);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0], (0, 7, keyword_style(1)));
        assert_eq!(spans[1], (8, 13, keyword_style(0)));
    }

    #[test]
    fn keyword_highlighting_requires_non_alphanumeric_boundaries() {
        assert_eq!(
            highlight_spans("[INFO] level=info xxx_info", None, None),
            vec![
                (1, 5, keyword_style(3)),
                (13, 17, keyword_style(3)),
                (22, 26, keyword_style(3)),
            ]
        );
        assert_eq!(
            highlight_spans("cloud_com_error_to_log", None, None),
            vec![(10, 15, keyword_style(0))]
        );
        assert!(highlight_spans("xxxinfo information errorCounter", None, None).is_empty());
    }

    #[test]
    fn fallback_prompt_marks_only_the_trailing_symbol() {
        let spans = highlight_spans("root@dut #", None, None);
        assert_eq!(spans, vec![(9, 10, prompt_style())]);
        assert!(highlight_spans("no prompt here", None, None).is_empty());
    }

    #[test]
    fn configured_prompt_prefers_the_longest_match_at_the_latest_end() {
        let spans = highlight_spans("boot\nSigmaStar # ", Some("# "), Some("SigmaStar # "));
        assert_eq!(spans, vec![(5, 17, prompt_style())]);
    }

    #[test]
    fn stream_decodes_utf8_split_across_events() {
        let mut parser = TerminalStreamParser::new();
        let encoded = "启动".as_bytes();

        let first = parser.push_event(&event_at(1, &encoded[..2]));
        assert!(first.completed.is_empty());
        assert!(first.pending.is_none());

        let second = parser.push_event(&event_at(2, &encoded[2..5]));
        assert!(second.completed.is_empty());
        assert_eq!(
            second.pending.as_ref().map(|line| line.text.as_str()),
            Some("启")
        );

        let mut final_bytes = encoded[5..].to_vec();
        final_bytes.push(b'\n');
        let third = parser.push_event(&event_at(3, &final_bytes));
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

        let first = parser.push_event(&event_at(1, b"safe \x1b[3"));
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe ")
        );

        let second = parser.push_event(&event_at(2, b"1mred\x1b[0m \x1b]52;c;sec"));
        assert_eq!(
            second.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe red ")
        );

        let third = parser.push_event(&event_at(3, b"ret\x1b"));
        assert_eq!(
            third.pending.as_ref().map(|line| line.text.as_str()),
            Some("safe red ")
        );
        let fourth = parser.push_event(&event_at(4, b"\\end\n"));
        assert_eq!(fourth.completed.len(), 1);
        assert_eq!(fourth.completed[0].text, "safe red end");
    }

    #[test]
    fn field_invalid_utf8_cannot_poison_later_replay_or_live_rx() {
        // Exact RX payload from the field incident at seq=206. The two
        // isolated 0x98 bytes are part of an invalid UTF-8 filename. Older
        // clients treated the first one as an unterminated C1 SOS and silently
        // swallowed `meminfo`, the prompt, and every later replay/live event.
        let field_payload = [
            0x1b, 0x5b, 0x30, 0x3b, 0x30, 0x6d, 0x3f, 0xa0, 0x3f, 0x40, 0xf8, 0x36, 0x40, 0x38,
            0x3f, 0x40, 0x3f, 0x3f, 0x3f, 0x3f, 0x40, 0x40, 0x40, 0xd8, 0x3f, 0xd8, 0x3f, 0x3f,
            0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x98,
            0x3f, 0x98, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f, 0x3f,
            0x3f, 0x3f, 0x1b, 0x5b, 0x6d, 0x0d, 0x0a, 0x1b, 0x5b, 0x30, 0x3b, 0x30, 0x6d, 0x6d,
            0x65, 0x6d, 0x69, 0x6e, 0x66, 0x6f, 0x1b, 0x5b, 0x6d, 0x0d, 0x0a, 0x5b, 0x72, 0x6f,
            0x6f, 0x74, 0x40, 0x6c, 0x75, 0x63, 0x6b, 0x66, 0x6f, 0x78, 0x20, 0x72, 0x6f, 0x6f,
            0x74, 0x5d, 0x23, 0x20,
        ];
        let mut parser = TerminalStreamParser::new();

        let replay = parser.push_event(&event_at(206, &field_payload));
        assert!(
            replay.completed.iter().any(|line| line.text == "meminfo"),
            "valid text after the invalid filename must remain visible"
        );
        assert_eq!(
            replay.pending.as_ref().map(|line| line.text.as_str()),
            Some("[root@luckfox root]# ")
        );

        let live = parser.push_event(&event_at(207, b"echo still-live\r\n"));
        assert_eq!(
            live.completed.last().map(|line| line.text.as_str()),
            Some("[root@luckfox root]# echo still-live")
        );
    }

    #[test]
    fn isolated_eight_bit_c1_bytes_are_text_not_parser_state() {
        let mut parser = TerminalStreamParser::new();
        let batch = parser.push_event(&event_at(
            1,
            &[
                0x90, 0x98, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, b'v', b'i', b's', b'i', b'b', b'l', b'e',
                b'\n',
            ],
        ));

        assert_eq!(batch.completed.len(), 1);
        assert!(batch.completed[0].text.ends_with("visible"));
        assert_eq!(
            batch.completed[0]
                .text
                .chars()
                .filter(|character| *character == '\u{fffd}')
                .count(),
            7
        );
    }

    #[test]
    fn unterminated_control_sequences_recover_without_hiding_later_rx() {
        let mut parser = TerminalStreamParser::new();

        let newline_recovery =
            parser.push_event(&event_at(1, b"before\x1b]52;c;unterminated\r\nafter\n"));
        assert_eq!(
            newline_recovery
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["before", "after"]
        );

        let mut oversized_osc = b"prefix\x1b]".to_vec();
        oversized_osc.extend(std::iter::repeat_n(b'x', MAX_CONTROL_STRING_BYTES + 1));
        oversized_osc.extend_from_slice(b"visible\n");
        let bounded = parser.push_event(&event_at(2, &oversized_osc));
        assert!(
            bounded
                .completed
                .last()
                .is_some_and(|line| line.text.ends_with("visible"))
        );

        let mut oversized_csi = b"left\x1b[".to_vec();
        oversized_csi.extend(std::iter::repeat_n(b'1', MAX_CSI_PARAMETER_BYTES + 1));
        oversized_csi.extend_from_slice(b"right\n");
        let bounded = parser.push_event(&event_at(3, &oversized_csi));
        assert!(
            bounded
                .completed
                .last()
                .is_some_and(|line| line.text.ends_with("right"))
        );

        for (seq, cancel) in [(4, 0x18), (5, 0x1a)] {
            let cancelled = parser.push_event(&event_at(
                seq,
                &[
                    b'b', b'e', b'f', b'o', b'r', b'e', 0x1b, b'P', b'h', b'i', b'd', b'd', b'e',
                    b'n', cancel, b'a', b'f', b't', b'e', b'r', b'\n',
                ],
            ));
            assert_eq!(
                cancelled.completed.last().map(|line| line.text.as_str()),
                Some("beforeafter")
            );
        }
    }

    #[test]
    fn terminated_dcs_remains_safely_stripped() {
        let mut parser = TerminalStreamParser::new();
        let batch = parser.push_event(&event_at(1, b"left\x1bPprivate\x1b\\right\n"));

        assert_eq!(batch.completed.len(), 1);
        assert_eq!(batch.completed[0].text, "leftright");
    }

    #[test]
    fn stream_applies_cr_erase_and_backspace_across_events() {
        let mut parser = TerminalStreamParser::new();
        let progress = parser.push_event(&event_at(1, b"download 100%\r42%\x1b[K\n"));
        assert_eq!(progress.completed[0].text, "42%");

        let first = parser.push_event(&event_at(2, b"abc\x08"));
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("abc")
        );
        let second = parser.push_event(&event_at(3, b"\x08XY\n"));
        assert_eq!(second.completed[0].text, "aXY");

        let overwrite = parser.push_event(&event_at(4, b"abc\rXY\n"));
        assert_eq!(overwrite.completed[0].text, "XYc");

        let delete = parser.push_event(&event_at(5, b"abc\x7fX\n"));
        assert_eq!(delete.completed[0].text, "abX");
    }

    #[test]
    fn prompt_tx_and_exact_rx_echo_form_one_attributed_terminal_row() {
        let _guard = crate::i18n::lang_test_lock();
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        let first = parser.push_event(&event_at(1, b"Sigma"));
        assert_eq!(
            first.pending.as_ref().map(|line| line.text.as_str()),
            Some("Sigma")
        );

        let prompt = parser.push_event(&event_at(2, b"Star # "));
        let prompt = prompt
            .pending
            .expect("prompt should remain visible without newline");
        assert_eq!(prompt.source, "DEV");
        assert_eq!(prompt.marker_color, None);
        assert_eq!(prompt.solid_style, None);
        assert_eq!(
            highlight_spans(&prompt.text, None, Some("SigmaStar #")),
            vec![(0, 11, prompt_style())]
        );

        let mut tx = event_at(3, b"reboot\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "human-1".into(),
            label: "operator".into(),
            kind: ActorKind::Human,
        });
        let switched = parser.push_event(&tx);
        assert!(switched.completed.is_empty());
        assert_eq!(
            switched.pending.as_ref().map(|line| line.source.as_str()),
            Some("HUMAN[human-1]:operator>")
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
            switched.pending.as_ref().map(|line| line.marker_color),
            Some(Some(Color::Green))
        );
        assert_eq!(
            switched.pending.as_ref().map(|line| line.text.as_str()),
            Some("SigmaStar # reboot")
        );

        let mut control = event_at(4, &[]);
        control.direction = Direction::None;
        control.kind = EventKind::ControlGranted;
        let unchanged = parser.push_event(&control);
        assert!(unchanged.completed.is_empty());
        assert_eq!(
            unchanged.pending.as_ref().map(|line| line.text.as_str()),
            Some("SigmaStar # reboot")
        );

        let echoed = parser.push_event(&event_at(5, b"reboot\r\n"));
        assert_eq!(echoed.completed.len(), 1);
        assert_eq!(echoed.completed[0].text, "SigmaStar # reboot");
        assert_eq!(echoed.completed[0].source, "HUMAN[human-1]:operator>");
        assert_eq!(echoed.completed[0].marker_color, Some(Color::Green));
        assert!(echoed.completed[0].echoed);
        assert!(echoed.pending.is_none());
    }

    #[test]
    fn target_hard_wrap_inside_long_echo_is_reconciled_without_duplicate_rows() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"[root@luckfox tmp]# "));

        let command =
            b"printf 'abcdefghijklmnopqrstuvwxyz-0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZ'\r";
        let mut tx = event_at(2, command);
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "agent-1".into(),
            label: "debugger".into(),
            kind: ActorKind::Agent,
        });
        let projected = parser.push_event(&tx);
        assert!(projected.completed.is_empty());

        // Real target PTY capture: once its configured display column is
        // crossed, the line discipline injects CR CR LF into the echo. Split
        // it across RX events to cover arbitrary serial read boundaries.
        let split = 42;
        let first = parser.push_event(&event_at(3, &command[..split]));
        assert!(first.completed.is_empty());
        let second = parser.push_event(&event_at(4, b"\r"));
        assert!(second.completed.is_empty());
        let third = parser.push_event(&event_at(5, b"\r"));
        assert!(third.completed.is_empty());

        let mut tail = Vec::from(&b"\n"[..]);
        tail.extend_from_slice(&command[split..]);
        tail.push(b'\n');
        let echoed = parser.push_event(&event_at(6, &tail));

        assert_eq!(echoed.completed.len(), 1);
        assert_eq!(
            echoed.completed[0].text,
            "[root@luckfox tmp]# printf 'abcdefghijklmnopqrstuvwxyz-0123456789-ABCDEFGHIJKLMNOPQRSTUVWXYZ'"
        );
        assert_eq!(echoed.completed[0].source, "AGENT[agent-1]:debugger>");
        assert!(echoed.completed[0].echoed);
        assert!(echoed.pending.is_none());
    }

    #[test]
    fn hard_wrap_candidate_mismatch_replays_every_rx_byte() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"# "));

        let mut tx = event_at(2, b"abcdefghij\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        parser.push_event(&tx);

        let response = parser.push_event(&event_at(3, b"abcde\r\r\nXYZ\r\n"));
        assert_eq!(
            response
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["# abcdefghij", "abcde", "XYZ"]
        );
        assert!(response.pending.is_none());
    }

    #[test]
    fn serial_boundary_keeps_partial_rx_echo_separate_from_projected_tx() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"shell# "));

        let mut tx = event_at(2, b"long-command-with-arguments\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        tx.actor = Some(Actor {
            id: "agent-1".into(),
            label: "debugger".into(),
            kind: ActorKind::Agent,
        });
        parser.push_event(&tx);
        parser.push_event(&event_at(3, b"long-command"));

        let mut closed = event_at(4, &[]);
        closed.direction = Direction::None;
        closed.kind = EventKind::SerialClosed;
        let boundary = parser.push_event(&closed);

        assert_eq!(boundary.completed.len(), 3);
        assert_eq!(
            boundary.completed[0].text,
            "shell# long-command-with-arguments"
        );
        assert_eq!(boundary.completed[0].source, "AGENT[agent-1]:debugger>");
        assert_eq!(boundary.completed[1].text, "long-command");
        assert_eq!(boundary.completed[1].source, "DEV");
        assert_eq!(boundary.completed[2].source, "SYSTEM");
        assert!(boundary.pending.is_none());

        let mut reopened = event_at(5, &[]);
        reopened.direction = Direction::None;
        reopened.kind = EventKind::SerialOpened;
        let reopened = parser.push_event(&reopened);
        assert_eq!(reopened.completed.len(), 1);
        assert_eq!(reopened.completed[0].source, "SYSTEM");
        assert!(reopened.pending.is_none());

        let after = parser.push_event(&event_at(6, b"fresh boot\r\n"));
        assert_eq!(after.completed.len(), 1);
        assert_eq!(after.completed[0].text, "fresh boot");
        assert_eq!(after.completed[0].source, "DEV");
    }

    #[test]
    fn raw_character_echoes_extend_the_prompt_without_duplicate_rows() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"[root@luckfox ~]# "));

        for (seq, byte) in [(2, b'c'), (4, b'd')] {
            let mut tx = event_at(seq, &[byte]);
            tx.direction = Direction::Tx;
            tx.kind = EventKind::Tx;
            tx.actor = Some(Actor {
                id: "human-1".into(),
                label: "operator".into(),
                kind: ActorKind::Human,
            });
            let projected = parser.push_event(&tx);
            assert!(projected.completed.is_empty());
            let echoed = parser.push_event(&event_at(seq + 1, &[byte]));
            assert!(echoed.completed.is_empty());
        }

        assert_eq!(
            parser
                .pending_line()
                .as_ref()
                .map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# cd")
        );

        let mut enter = event_at(6, b"\r");
        enter.direction = Direction::Tx;
        enter.kind = EventKind::Tx;
        parser.push_event(&enter);
        let completed = parser.push_event(&event_at(7, b"\r\n"));
        assert_eq!(completed.completed.len(), 1);
        assert_eq!(completed.completed[0].text, "[root@luckfox ~]# cd");
        assert!(completed.pending.is_none());
    }

    #[test]
    fn raw_tx_events_can_all_precede_one_batched_rx_echo() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"[root@luckfox ~]# "));

        for (offset, byte) in b"pwd\r".iter().copied().enumerate() {
            let mut tx = event_at(2 + offset as u64, &[byte]);
            tx.direction = Direction::Tx;
            tx.kind = EventKind::Tx;
            tx.actor = Some(Actor {
                id: "human-1".into(),
                label: "operator".into(),
                kind: ActorKind::Human,
            });
            parser.push_event(&tx);
        }

        assert_eq!(
            parser
                .pending_line()
                .as_ref()
                .map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# pwd")
        );

        let received = parser.push_event(&event_at(6, b"pwd\r\n/oem\r\n[root@luckfox ~]# "));
        assert_eq!(
            received
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["[root@luckfox ~]# pwd", "/oem"]
        );
        assert!(received.completed[0].echoed);
        assert_eq!(
            received.pending.as_ref().map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# ")
        );
    }

    #[test]
    fn echo_off_commits_the_visible_command_before_direct_device_text() {
        let mut parser = TerminalStreamParser::new();
        parser.push_event(&event_at(1, b"[root@luckfox tmp]# "));

        let mut tx = event_at(2, b"cd\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        parser.push_event(&tx);

        let response = parser.push_event(&event_at(3, b"/tmp\r\n"));
        assert_eq!(
            response
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["[root@luckfox tmp]# cd", "/tmp"]
        );
        assert!(response.pending.is_none());
    }

    #[test]
    fn echo_mismatch_replays_the_complete_candidate_prefix() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"[root@luckfox tmp]# "));

        let mut tx = event_at(2, b"ready\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        parser.push_event(&tx);

        // "re" first matches the expected echo prefix, then "s" proves this
        // is real device output. The speculative prefix must be replayed.
        let response = parser.push_event(&event_at(3, b"result\r\n"));
        assert_eq!(
            response
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["[root@luckfox tmp]# ready", "result"]
        );
    }

    #[test]
    fn raw_no_echo_never_consumes_matching_bytes_from_later_boot_output() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);

        let mut tx = event_at(1, b"c");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        parser.push_event(&tx);

        let boot = b"Boot countdown c continues\r\n";
        let received = parser.push_event(&event_at(2, boot));
        let mut rendered = received
            .completed
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if let Some(pending) = received.pending.as_ref() {
            rendered.push_str(&pending.text);
        }

        assert_eq!(
            rendered, "cBoot countdown c continues",
            "the local TX projection may remain, but every RX byte must be visible"
        );
    }

    #[test]
    fn logging_degradation_does_not_break_pending_echo_reconciliation() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"SigmaStar # "));

        let mut tx = event_at(2, b"reboot\r");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        parser.push_event(&tx);

        let mut degraded = event_at(3, &[]);
        degraded.direction = Direction::None;
        degraded.kind = EventKind::LoggingDegraded;
        let unchanged = parser.push_event(&degraded);
        assert!(unchanged.completed.is_empty());
        assert_eq!(
            unchanged.pending.as_ref().map(|line| line.text.as_str()),
            Some("SigmaStar # reboot")
        );

        let echoed = parser.push_event(&event_at(4, b"reboot\r\n"));
        assert_eq!(echoed.completed.len(), 1);
        assert_eq!(echoed.completed[0].text, "SigmaStar # reboot");
        assert!(echoed.completed[0].echoed);
    }

    #[test]
    fn crlf_tx_and_exact_echo_commit_only_one_row() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"shell# "));

        let mut tx = event_at(2, b"version\r\n");
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        let projected = parser.push_event(&tx);
        assert_eq!(projected.completed.len(), 1);
        assert_eq!(projected.completed[0].text, "shell# version");

        let echoed = parser.push_event(&event_at(3, b"version\r\n"));
        assert!(echoed.completed.is_empty());
        assert!(echoed.pending.is_none());
    }

    #[test]
    fn crlf_echo_split_across_tx_events_does_not_consume_a_later_device_newline() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"shell# "));

        let mut command = event_at(2, b"a\r");
        command.direction = Direction::Tx;
        command.kind = EventKind::Tx;
        parser.push_event(&command);

        let mut split_lf = event_at(3, b"\n");
        split_lf.direction = Direction::Tx;
        split_lf.kind = EventKind::Tx;
        let projected = parser.push_event(&split_lf);
        assert_eq!(projected.completed[0].text, "shell# a");

        let echoed = parser.push_event(&event_at(4, b"a\r\n"));
        assert!(echoed.completed.is_empty());
        assert!(echoed.pending.is_none());

        let output = parser.push_event(&event_at(5, b"ok\r\n"));
        assert_eq!(output.completed.len(), 1);
        assert_eq!(output.completed[0].text, "ok");
        assert!(output.pending.is_none());
    }

    #[test]
    fn raw_del_and_linux_erase_echo_apply_backspace_only_once() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"ab"));

        let mut tx = event_at(2, &[0x7f]);
        tx.direction = Direction::Tx;
        tx.kind = EventKind::Tx;
        let projected = parser.push_event(&tx);
        assert_eq!(
            projected.pending.as_ref().map(|line| line.text.as_str()),
            Some("a ")
        );

        let echoed = parser.push_event(&event_at(3, ERASE_ECHO));
        assert!(echoed.completed.is_empty());
        assert_eq!(
            echoed.pending.as_ref().map(|line| line.text.as_str()),
            Some("a ")
        );
    }

    #[test]
    fn raw_del_with_single_bs_echo_does_not_block_later_character_echoes() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"abc"));

        let mut delete = event_at(2, &[0x7f]);
        delete.direction = Direction::Tx;
        delete.kind = EventKind::Tx;
        parser.push_event(&delete);
        parser.push_event(&event_at(3, b"\x08"));

        for (seq, byte) in [(4, b'd'), (6, b'e')] {
            let mut tx = event_at(seq, &[byte]);
            tx.direction = Direction::Tx;
            tx.kind = EventKind::Tx;
            parser.push_event(&tx);
            parser.push_event(&event_at(seq + 1, &[byte]));
        }

        assert_eq!(
            parser
                .pending_line()
                .as_ref()
                .map(|line| line.text.as_str()),
            Some("abde")
        );
    }

    #[test]
    fn intervening_output_abandons_delayed_echo_search_without_data_loss() {
        let mut parser = TerminalStreamParser::new();
        parser.set_echo_reconciliation(true);
        parser.push_event(&event_at(1, b"# "));

        let mut first = event_at(2, b"one\r");
        first.direction = Direction::Tx;
        first.kind = EventKind::Tx;
        parser.push_event(&first);

        let mut second = event_at(3, b"two\r");
        second.direction = Direction::Tx;
        second.kind = EventKind::Tx;
        let projected = parser.push_event(&second);
        assert_eq!(projected.completed[0].text, "# one");
        assert_eq!(
            projected.pending.as_ref().map(|line| line.text.as_str()),
            Some("two")
        );

        let first_echo = parser.push_event(&event_at(4, b"one\r\n"));
        assert!(first_echo.completed.is_empty());
        let output = parser.push_event(&event_at(5, b"result\r\n"));
        assert_eq!(
            output
                .completed
                .iter()
                .map(|line| line.text.as_str())
                .collect::<Vec<_>>(),
            ["two", "result"]
        );

        let second_echo = parser.push_event(&event_at(6, b"two\r\n"));
        assert_eq!(second_echo.completed.len(), 1);
        assert_eq!(second_echo.completed[0].text, "two");
        assert_eq!(second_echo.completed[0].source, "DEV");
        assert!(second_echo.pending.is_none());
    }

    #[test]
    fn streamed_rows_match_shell_and_uboot_profile_prompts() {
        let mut parser = TerminalStreamParser::new();
        let shell_prompt = Some("root@dut:/tmp# ");
        let uboot_prompt = Some("SigmaStar =>");

        let shell_prefix = parser.push_event(&event_at(1, b"root@dut:/"));
        assert_eq!(
            shell_prefix.pending.as_ref().map(|line| line.text.as_str()),
            Some("root@dut:/")
        );
        let shell = parser.push_event(&event_at(2, b"tmp# "));
        let shell = shell.pending.expect("shell prompt row");
        assert_eq!(
            highlight_spans(&shell.text, shell_prompt, uboot_prompt),
            vec![(0, 15, prompt_style())]
        );

        let uboot_prefix = parser.push_event(&event_at(3, b"\nSigma"));
        assert_eq!(
            uboot_prefix.completed.first().map(|line| highlight_spans(
                &line.text,
                shell_prompt,
                uboot_prompt
            )),
            Some(vec![(0, 15, prompt_style())])
        );
        assert_eq!(
            uboot_prefix.pending.as_ref().map(|line| line.text.as_str()),
            Some("Sigma")
        );
        let uboot = parser.push_event(&event_at(4, b"Star =>"));
        let uboot = uboot.pending.expect("U-Boot prompt row");
        assert_eq!(
            highlight_spans(&uboot.text, shell_prompt, uboot_prompt),
            vec![(0, 12, prompt_style())]
        );
    }

    #[test]
    fn keyword_highlighting_uses_the_complete_streamed_row() {
        let mut parser = TerminalStreamParser::new();
        parser.push_event(&event_at(1, b"FAT"));
        let completed = parser.push_event(&event_at(2, b"AL: boot failed\n"));

        assert_eq!(completed.completed.len(), 1);
        assert_eq!(completed.completed[0].text, "FATAL: boot failed");
        assert_eq!(
            highlight_spans(&completed.completed[0].text, None, None),
            vec![(0, 5, keyword_style(0)), (12, 18, keyword_style(0))]
        );
    }

    #[test]
    fn flush_handles_truncated_utf8_and_drops_incomplete_escape() {
        let mut parser = TerminalStreamParser::new();
        parser.push_event(&event_at(1, b"ok\xe5\x90"));
        let flushed = parser.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].text, "ok\u{fffd}");

        parser.push_event(&event_at(2, b"safe\x1b]52;c;secret"));
        let flushed = parser.flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].text, "safe");
    }

    #[test]
    fn stream_bounds_unterminated_rows() {
        let mut parser = TerminalStreamParser::new();
        let bytes = vec![b'x'; MAX_STREAM_LINE_CHARS + 1];
        let batch = parser.push_event(&event_at(1, &bytes));

        assert_eq!(batch.completed.len(), 1);
        assert_eq!(batch.completed[0].text.len(), MAX_STREAM_LINE_CHARS);
        assert_eq!(
            batch.pending.as_ref().map(|line| line.text.as_str()),
            Some("x")
        );
    }
}
