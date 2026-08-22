use std::{
    collections::{HashMap, VecDeque},
    io::{self, Write},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use crossterm::{
    cursor::Show,
    event::{
        DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseButton, MouseEvent,
        MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures_util::StreamExt;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Tabs, Wrap},
};
#[cfg(test)]
use serial_protocol::WritePacing;
use serial_protocol::{
    Actor, ArchiveSummary, ClientMessage, CommandCaptureMatcher, CommandCaptureMatcherKind,
    CommandResult, ControlLease, ControlMode, Cursor, DataBits, Direction, EchoMode, EventKind,
    EventQuery, FlowControl, GapRange, LoggingState, ModelProfile, MonitorIncident, MonitorMatcher,
    MonitorStatus, MonitorView, Parity, PortDescriptor, ResolvedModelSettings,
    ResolvedTransportSettings, RunInfo, RunStatus, ServerMessage, SessionState, SlotSnapshot,
    StopBits, TargetActivity, TimelineEvent, TransportProfile, TriggerInfo, TriggerStatus,
    WireFrame,
};
use tokio::sync::mpsc;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    config::{DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS, LoadedConfig, MIN_ORPHAN_RUN_TIMEOUT_SECONDS},
    display::{
        DisplayLine, RunBoundary, TerminalStreamParser, error_code_label, format_event_plain,
        format_wall_time_local, gap_line, gap_reason_label, highlight_spans, pad_display,
        safe_inline, trigger_status_label,
    },
    history::{StartupHistory, StartupHistoryTarget, load_startup_histories},
    i18n::{self, tr, trf},
    ws::{self, NetworkCommand, NetworkEvent},
};

const MAX_LINES_PER_SLOT: usize = 20_000;
const MAX_BYTES_PER_SLOT: usize = 4 * 1024 * 1024;
const MAX_PENDING_WRITES: usize = 16;
const MAX_PENDING_BYTES: usize = 64 * 1024;
const MAX_PASTE_BYTES: usize = 64 * 1024;
const MAX_OUTSTANDING_REQUESTS: usize = 512;
const MAX_WRITE_BYTES: usize = 4 * 1024;
const CONTROL_TTL_MS: u64 = 30_000;
const DEFAULT_HUMAN_IDLE_RELEASE_SECONDS: u64 = 60;
#[cfg(test)]
const ACTIVE_WINDOW_NS: i64 = 5_000_000_000;
const MOUSE_SELECTION_TIMEOUT: Duration = Duration::from_secs(5);
const DOUBLE_CLICK_INTERVAL: Duration = Duration::from_millis(500);
const SOFTWARE_CURSOR_BLINK_INTERVAL: Duration = Duration::from_millis(600);
const STATUS_NOTICE_DURATION: Duration = Duration::from_secs(4);
const MAX_RUN_HISTORY_PER_SLOT: usize = 20;
const MAX_COMMANDS_PER_RUN: usize = 64;
const MAX_RUN_COMMAND_BYTES: usize = 4 * 1024;
const MAX_MONITORS_PER_SLOT: usize = 32;
const MAX_INCIDENTS_PER_MONITOR: usize = 64;
const DEFAULT_AGENT_HISTORY_ROWS: u16 = 5;
const MIN_AGENT_HISTORY_ROWS: u16 = 3;
const MAX_AGENT_HISTORY_ROWS: u16 = 20;
/// Keep the command-history bar useful without taking the serial output below
/// its four-row minimum. Short terminals expose the same view as a focused
/// popup instead of permanently consuming scarce vertical space.
const RUN_HISTORY_BAR_MIN_TERMINAL_HEIGHT: u16 = 22;
const OUTPUT_SEARCH_LIMIT_EVENTS: usize = 200;
const OUTPUT_SEARCH_PAGE_EVENTS: usize = 10_000;
const OUTPUT_SEARCH_LIMIT_BYTES: usize = 16 * 1024 * 1024;
const OUTPUT_SEARCH_EVENT_WINDOW: u64 = 10_000;
const OUTPUT_SEARCH_ARCHIVE_LIMIT: usize = 4;
const OUTPUT_SEARCH_QUERY_BYTES: usize = 4_096;
const OUTPUT_SEARCH_HTTP_QUERY_LIMIT: usize = 8;
const OUTPUT_SEARCH_DEADLINE: Duration = Duration::from_secs(10);
const EXACT_EVIDENCE_PAGE_EVENTS: usize = 5_000;
const EXACT_EVIDENCE_PAGE_BYTES: usize = 1024 * 1024;
const EXACT_EVIDENCE_MAX_EVENTS: usize = 20_000;
const EXACT_EVIDENCE_MAX_BYTES: usize = 4 * 1024 * 1024;
const EXACT_EVIDENCE_HTTP_QUERY_LIMIT: usize = 8;
const EXACT_EVIDENCE_DEADLINE: Duration = Duration::from_secs(10);

type ClipboardCopyFn = fn(&str) -> Result<()>;

fn configured_agent_history_rows(value: Option<u16>) -> u16 {
    value
        .unwrap_or(DEFAULT_AGENT_HISTORY_ROWS)
        .clamp(MIN_AGENT_HISTORY_ROWS, MAX_AGENT_HISTORY_ROWS)
}

fn configured_orphan_run_timeout_seconds(value: Option<u64>) -> u64 {
    value.unwrap_or(DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS)
}

fn default_clipboard_copy(text: &str) -> Result<()> {
    crate::clipboard::copy_text(text)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Line,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneFocus {
    Queue,
    RunHistory,
    Input,
}

#[derive(Debug, Clone, Copy)]
struct ConsoleLayout {
    output_area: Rect,
    output_inner: Rect,
    input_area: Rect,
    run_history_area: Option<Rect>,
    run_history_inner: Option<Rect>,
}

/// A paused viewport is an immutable set of already wrapped terminal rows.
/// Live serial events continue to update the underlying Port, but they cannot
/// move these rows. Returning to offset zero drops the snapshot and resumes
/// the ordinary live projection.
#[derive(Debug, Clone)]
struct ScrollSnapshot {
    rows: Vec<Line<'static>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SelectionPoint {
    row: usize,
    column: u16,
}

#[derive(Debug, Clone)]
struct TextSelection {
    rows: Vec<Line<'static>>,
    plain_rows: Vec<String>,
    anchor: SelectionPoint,
    head: SelectionPoint,
    /// A double-click selects the lexical token even when it occupies one
    /// terminal cell, so completion must not depend on pointer movement.
    word_selected: bool,
    completed: bool,
    last_activity: Instant,
}

impl TextSelection {
    fn ordered_points(&self) -> (SelectionPoint, SelectionPoint) {
        if self.anchor <= self.head {
            (self.anchor, self.head)
        } else {
            (self.head, self.anchor)
        }
    }

    fn is_dragged(&self) -> bool {
        self.word_selected || self.anchor != self.head
    }

    fn selected_text(&self) -> String {
        let (start, end) = self.ordered_points();
        self.plain_rows
            .iter()
            .enumerate()
            .filter_map(|(row, text)| {
                selection_columns(start, end, row)
                    .map(|(from, through)| slice_display_columns(text, from, through))
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Clone, Copy)]
struct OutputClick {
    point: SelectionPoint,
    at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingWriteKind {
    Line,
    Raw,
}

#[derive(Debug, Clone)]
struct PendingWrite {
    data: Vec<u8>,
    operation_id: Option<Uuid>,
    kind: PendingWriteKind,
}

#[derive(Debug, Clone)]
struct QueuedLineOperation {
    operation_id: Option<Uuid>,
    start: usize,
    end: usize,
    data: Vec<u8>,
}

fn queued_line_operations(queue: &VecDeque<PendingWrite>) -> Vec<QueuedLineOperation> {
    let mut operations: Vec<QueuedLineOperation> = Vec::new();
    for (index, write) in queue.iter().enumerate() {
        if write.kind != PendingWriteKind::Line {
            continue;
        }
        if let Some(operation) = operations.last_mut()
            && operation.end == index
            && operation.operation_id == write.operation_id
        {
            operation.end += 1;
            operation.data.extend_from_slice(&write.data);
            continue;
        }
        operations.push(QueuedLineOperation {
            operation_id: write.operation_id,
            start: index,
            end: index + 1,
            data: write.data.clone(),
        });
    }
    operations
}

fn take_queued_line_operation(
    queue: &mut VecDeque<PendingWrite>,
    operation_index: usize,
) -> Option<QueuedLineOperation> {
    let operation = queued_line_operations(queue)
        .into_iter()
        .nth(operation_index)?;
    let removed = queue
        .drain(operation.start..operation.end)
        .flat_map(|write| write.data)
        .collect::<Vec<_>>();
    Some(QueuedLineOperation {
        data: removed,
        ..operation
    })
}

fn append_pending_write(
    queue: &mut VecDeque<PendingWrite>,
    data: &[u8],
    operation_id: Option<Uuid>,
    kind: PendingWriteKind,
) {
    let mut remaining = data;
    if kind == PendingWriteKind::Raw
        && let Some(last) = queue.back_mut()
        && last.kind == PendingWriteKind::Raw
        && last.operation_id == operation_id
        && last.data.len() < MAX_WRITE_BYTES
    {
        let append = remaining
            .len()
            .min(MAX_WRITE_BYTES.saturating_sub(last.data.len()));
        last.data.extend_from_slice(&remaining[..append]);
        remaining = &remaining[append..];
    }
    for chunk in remaining.chunks(MAX_WRITE_BYTES) {
        queue.push_back(PendingWrite {
            data: chunk.to_vec(),
            operation_id,
            kind,
        });
    }
}

fn queued_line_count(queue: &VecDeque<PendingWrite>) -> usize {
    queued_line_operations(queue).len()
}

/// Removes the newest complete LINE operation from a local, not-yet-sent
/// queue. One long line may occupy several physical chunks, but it remains
/// one editable queue entry. Each normalized command in a multiline paste has
/// its own operation ID and therefore its own entry.
#[cfg(test)]
fn pop_last_queued_line(queue: &mut VecDeque<PendingWrite>) -> Option<Vec<u8>> {
    let newest = queue.back()?;
    if newest.kind != PendingWriteKind::Line {
        return None;
    }
    let operation_id = newest.operation_id;
    let mut chunks = Vec::new();
    while queue.back().is_some_and(|write| {
        write.kind == PendingWriteKind::Line && write.operation_id == operation_id
    }) {
        chunks.push(queue.pop_back().expect("back was just checked").data);
    }
    chunks.reverse();
    Some(chunks.into_iter().flatten().collect())
}

#[derive(Debug)]
enum PendingRequest {
    Acquire {
        port: String,
        mode: ControlMode,
    },
    Renew {
        port: String,
    },
    Release {
        port: String,
    },
    CancelAcquire {
        port: String,
    },
    Write {
        port: String,
        operation_id: Option<Uuid>,
        cooperative: bool,
    },
}

impl PendingRequest {
    fn port(&self) -> &str {
        match self {
            Self::Acquire { port, .. }
            | Self::Renew { port }
            | Self::Release { port }
            | Self::CancelAcquire { port }
            | Self::Write { port, .. } => port,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct InFlightWrite {
    operation_id: Option<Uuid>,
    kind: PendingWriteKind,
    chunk_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SubscriptionPhase {
    Disconnected,
    Attaching,
    Replaying { from_seq: u64, through_seq: u64 },
    Ready { head_seq: u64 },
    Lagged { from_seq: u64, to_seq: u64 },
}

impl SubscriptionPhase {
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

#[derive(Debug)]
struct TriggerLiveProjection {
    trigger_id: Uuid,
    initial_pending: bool,
    /// `None` means a reconnect snapshot could not reveal whether a start
    /// literal was consumed while the one-time initial write was in flight.
    start_seen: Option<bool>,
    start_matcher: Option<LiteralProjectionMatcher>,
    stop_matcher: LiteralProjectionMatcher,
    /// Only live TriggerStarted events establish a trustworthy local timeout
    /// origin. A reconnect snapshot intentionally leaves this unset.
    deadline: Option<Instant>,
    status_known: bool,
}

impl TriggerLiveProjection {
    fn new(trigger: &TriggerInfo, live_start: bool) -> Self {
        let initial_pending =
            trigger.status == TriggerStatus::Armed && trigger.spec.initial_write.is_some();
        let start_seen = match trigger.status {
            TriggerStatus::WaitingForStart => Some(false),
            TriggerStatus::Running => Some(true),
            TriggerStatus::Armed => {
                if trigger.spec.start_contains.is_none() {
                    Some(true)
                } else if live_start {
                    Some(false)
                } else {
                    None
                }
            }
            TriggerStatus::Stopping => None,
            status if status.is_terminal() => None,
            _ => None,
        };
        let start_matcher = trigger
            .spec
            .start_contains
            .as_ref()
            .filter(|_| start_seen != Some(true))
            .map(|pattern| LiteralProjectionMatcher::new(std::slice::from_ref(pattern)));
        Self {
            trigger_id: trigger.id,
            initial_pending,
            start_seen,
            start_matcher,
            stop_matcher: LiteralProjectionMatcher::new(&trigger.spec.stop_contains),
            deadline: live_start
                .then(|| Instant::now() + Duration::from_millis(trigger.spec.timeout_ms)),
            status_known: true,
        }
    }
}

#[derive(Debug)]
struct LiteralProjectionMatcher {
    patterns: Vec<Vec<u8>>,
    tail: Vec<u8>,
    tail_limit: usize,
}

impl LiteralProjectionMatcher {
    fn new(patterns: &[Vec<u8>]) -> Self {
        Self {
            patterns: patterns.to_vec(),
            tail: Vec::new(),
            tail_limit: patterns
                .iter()
                .map(Vec::len)
                .max()
                .unwrap_or(0)
                .saturating_sub(1),
        }
    }

    fn push(&mut self, data: &[u8]) -> bool {
        if self.patterns.is_empty() {
            return false;
        }
        let mut window = Vec::with_capacity(self.tail.len().saturating_add(data.len()));
        window.extend_from_slice(&self.tail);
        window.extend_from_slice(data);
        let matched = self.patterns.iter().any(|pattern| {
            !pattern.is_empty()
                && window
                    .windows(pattern.len())
                    .any(|candidate| candidate == pattern)
        });
        let keep = self.tail_limit.min(window.len());
        self.tail.clear();
        self.tail
            .extend_from_slice(&window[window.len().saturating_sub(keep)..]);
        matched
    }
}

#[derive(Debug, Clone)]
struct RunCommandStep {
    daemon_epoch: Uuid,
    operation_id: Option<Uuid>,
    step_index: Option<usize>,
    first_seq: u64,
    last_seq: u64,
    data: Vec<u8>,
    capture_matchers: Vec<CommandCaptureMatcher>,
    truncated: bool,
}

#[derive(Debug, Clone)]
struct RunCommandRecord {
    daemon_epoch: Uuid,
    sequence_id: Option<Uuid>,
    first_seq: u64,
    last_seq: u64,
    first_wall_time_ns: i64,
    description: Option<String>,
    steps: Vec<RunCommandStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunCommandKey {
    run_id: Uuid,
    first_seq: u64,
}

impl RunCommandStep {
    fn capture_matchers(event: &TimelineEvent) -> Vec<CommandCaptureMatcher> {
        event
            .metadata
            .get("command_capture_matchers")
            .and_then(|value| {
                serde_json::from_value::<Vec<CommandCaptureMatcher>>(value.clone()).ok()
            })
            .unwrap_or_default()
    }

    fn from_event(event: &TimelineEvent) -> Self {
        let mut data = event.data.clone();
        let truncated = data.len() > MAX_RUN_COMMAND_BYTES;
        data.truncate(MAX_RUN_COMMAND_BYTES);
        Self {
            daemon_epoch: event.daemon_epoch,
            operation_id: event.operation_id,
            step_index: event
                .metadata
                .get("command_sequence_step_index")
                .and_then(serde_json::Value::as_u64)
                .and_then(|value| usize::try_from(value).ok()),
            first_seq: event.seq,
            last_seq: event.seq,
            data,
            capture_matchers: Self::capture_matchers(event),
            truncated,
        }
    }

    fn append_event(&mut self, event: &TimelineEvent) {
        self.first_seq = self.first_seq.min(event.seq);
        self.last_seq = self.last_seq.max(event.seq);
        let available = MAX_RUN_COMMAND_BYTES.saturating_sub(self.data.len());
        let append = available.min(event.data.len());
        self.data.extend_from_slice(&event.data[..append]);
        if self.capture_matchers.is_empty() {
            self.capture_matchers = Self::capture_matchers(event);
        }
        self.truncated |= append < event.data.len();
    }
}

impl RunCommandRecord {
    fn sequence_id(event: &TimelineEvent) -> Option<Uuid> {
        event.metadata.get("command_sequence_id").and_then(|value| {
            value
                .as_str()
                .and_then(|value| Uuid::parse_str(value).ok())
                .or_else(|| serde_json::from_value::<Uuid>(value.clone()).ok())
        })
    }

    fn description(event: &TimelineEvent) -> Option<String> {
        event
            .metadata
            .get("command_sequence_description")
            .or_else(|| event.metadata.get("command_description"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    }

    fn from_event(event: &TimelineEvent) -> Self {
        Self {
            daemon_epoch: event.daemon_epoch,
            sequence_id: Self::sequence_id(event),
            first_seq: event.seq,
            last_seq: event.seq,
            first_wall_time_ns: event.wall_time_ns,
            description: Self::description(event),
            steps: vec![RunCommandStep::from_event(event)],
        }
    }

    fn matches_event(&self, event: &TimelineEvent) -> bool {
        if self.daemon_epoch != event.daemon_epoch {
            return false;
        }
        match (self.sequence_id, Self::sequence_id(event)) {
            (Some(existing), Some(incoming)) => existing == incoming,
            (None, None) => event.operation_id.is_some_and(|operation_id| {
                self.steps.first().and_then(|step| step.operation_id) == Some(operation_id)
            }),
            _ => false,
        }
    }

    fn append_event(&mut self, event: &TimelineEvent) {
        self.first_seq = self.first_seq.min(event.seq);
        self.last_seq = self.last_seq.max(event.seq);
        self.first_wall_time_ns = self.first_wall_time_ns.min(event.wall_time_ns);
        if self.description.is_none() {
            self.description = Self::description(event);
        }
        let incoming_index = event
            .metadata
            .get("command_sequence_step_index")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok());
        let existing = self.steps.iter_mut().find(|step| {
            event.operation_id.is_some() && step.operation_id == event.operation_id
                || incoming_index.is_some() && step.step_index == incoming_index
        });
        if let Some(step) = existing {
            step.append_event(event);
        } else {
            self.steps.push(RunCommandStep::from_event(event));
            self.steps
                .sort_by_key(|step| (step.step_index.unwrap_or(usize::MAX), step.first_seq));
        }
    }
}

#[derive(Debug, Clone)]
struct RunHistoryEntry {
    id: Uuid,
    label: String,
    status: RunStatus,
    start_seq: u64,
    end_seq: Option<u64>,
    commands: VecDeque<RunCommandRecord>,
}

#[derive(Debug, Clone)]
struct MonitorHistoryEntry {
    monitor: MonitorView,
    incidents: VecDeque<MonitorIncident>,
    limited: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryActionKey {
    Command(RunCommandKey),
    Monitor(Uuid),
}

impl RunHistoryEntry {
    fn from_run(run: &RunInfo) -> Self {
        Self {
            id: run.id,
            label: run.label.clone(),
            status: run.status,
            start_seq: run.start_seq,
            end_seq: run.end_seq,
            commands: VecDeque::new(),
        }
    }

    fn update_from_run(&mut self, run: &RunInfo) {
        self.label.clone_from(&run.label);
        self.status = run.status;
        self.start_seq = run.start_seq;
        self.end_seq = run.end_seq;
    }

    /// Returns `(new_action, evicted)` for the bounded command projection.
    fn append_command(&mut self, event: &TimelineEvent) -> (bool, bool) {
        if let Some(command) = self
            .commands
            .iter_mut()
            .find(|command| command.matches_event(event))
        {
            command.append_event(event);
            return (false, false);
        }
        let evicted = self.commands.len() == MAX_COMMANDS_PER_RUN;
        if self.commands.len() == MAX_COMMANDS_PER_RUN {
            self.commands.pop_front();
        }
        self.commands.push_back(RunCommandRecord::from_event(event));
        (true, evicted)
    }
}

struct SlotView {
    snapshot: SlotSnapshot,
    /// Live-only projection of Trigger transitions that are not published as
    /// replacement snapshots on an attached WebSocket. The durable lifecycle
    /// events remain authoritative; this state only keeps the footer honest
    /// between TriggerStarted and the terminal event.
    trigger_projection: Option<TriggerLiveProjection>,
    subscription: SubscriptionPhase,
    lines: VecDeque<DisplayLine>,
    pending_line: Option<DisplayLine>,
    stream: TerminalStreamParser,
    buffered_bytes: usize,
    /// The bounded in-memory presentation has evicted at least one retained
    /// row. The durable journal remains authoritative; this bit keeps a
    /// synthetic warning visible at the oldest local boundary.
    local_history_truncated: bool,
    scroll_snapshot: Option<ScrollSnapshot>,
    /// Visual-row offset within `scroll_snapshot`. Recovery paths may retain
    /// this offset before a snapshot is rebuilt; interactive scrolling always
    /// creates a snapshot first.
    scroll_from_bottom: usize,
    unseen: usize,
    last_epoch: Option<Uuid>,
    last_seq: u64,
    /// Earliest event in the current raw-journal suffix whose sequence is
    /// known to be contiguous through `last_seq`. Display rows can merge many
    /// events, so their row sequences alone cannot prove a command capture is
    /// complete.
    local_contiguous_from_seq: Option<u64>,
    draft: Vec<char>,
    draft_cursor: usize,
    mode: InputMode,
    history: Vec<String>,
    history_search: Option<HistorySearch>,
    completion: Option<Completion>,
    last_manual_activity: Option<Instant>,
    /// Bounded, structured Run projection for the operator history bar. Timeline
    /// rows remain the durable audit source; this projection only groups their
    /// lifecycle and confirmed TX events for quick review.
    run_history: VecDeque<RunHistoryEntry>,
    monitor_history: VecDeque<MonitorHistoryEntry>,
    /// The bar is a bounded recent projection, not an assertion that the
    /// durable journal has been read from sequence one. Initial attach uses a
    /// tail, and any gap or local eviction keeps this conservative marker set.
    run_history_limited: bool,
    /// `None` follows the newest described Agent command at the bottom. Every new
    /// command action returns here; chunks and later steps of the same action do not.
    selected_run_command: Option<RunCommandKey>,
    expanded_run_command: Option<RunCommandKey>,
    /// Child command selected inside an expanded `command_sequence` action.
    /// `None` keeps navigation at the action level.
    selected_run_step: Option<usize>,
    selected_monitor: Option<Uuid>,
    expanded_monitor: Option<Uuid>,
    selected_monitor_matcher: Option<usize>,
    selected_monitor_incident: Option<Uuid>,
    run_detail_scroll: usize,
}

/// Ctrl-R incremental history search state (LINE mode).
#[derive(Debug)]
struct HistorySearch {
    query: String,
    saved_draft: Vec<char>,
    saved_cursor: usize,
    /// Index into `SlotView::history` of the current match.
    match_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSearchMatcher {
    Literal,
    Regex,
}

impl OutputSearchMatcher {
    fn toggled(self) -> Self {
        match self {
            Self::Literal => Self::Regex,
            Self::Regex => Self::Literal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSearchDirection {
    Both,
    Rx,
    Tx,
}

impl OutputSearchDirection {
    fn next(self) -> Self {
        match self {
            Self::Both => Self::Rx,
            Self::Rx => Self::Tx,
            Self::Tx => Self::Both,
        }
    }

    fn query_directions(self) -> &'static [Direction] {
        match self {
            Self::Both => &[Direction::Rx, Direction::Tx],
            Self::Rx => &[Direction::Rx],
            Self::Tx => &[Direction::Tx],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSearchScope {
    CurrentEpoch,
    Retained,
    CurrentRun,
}

#[derive(Debug, Clone, Copy)]
struct OutputSearchRun {
    id: Uuid,
    start_seq: u64,
    through_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSearchPhase {
    Editing,
    Loading(Uuid),
    Results,
}

#[derive(Debug)]
struct OutputSearchState {
    port: String,
    current_epoch: Uuid,
    head_seq: u64,
    current_run: Option<OutputSearchRun>,
    previous_focus: PaneFocus,
    query: Vec<char>,
    cursor: usize,
    matcher: OutputSearchMatcher,
    case_sensitive: bool,
    direction: OutputSearchDirection,
    scope: OutputSearchScope,
    phase: OutputSearchPhase,
    results: Vec<TimelineEvent>,
    selected: usize,
    detail_scroll: usize,
    gaps: Vec<GapRange>,
    partial: bool,
    scanned_archives: usize,
    error: Option<String>,
}

impl OutputSearchState {
    fn query_text(&self) -> String {
        self.query.iter().collect()
    }

    fn cycle_scope(&mut self) {
        self.scope = match (self.scope, self.current_run.is_some()) {
            (OutputSearchScope::CurrentEpoch, _) => OutputSearchScope::Retained,
            (OutputSearchScope::Retained, true) => OutputSearchScope::CurrentRun,
            (OutputSearchScope::Retained, false) | (OutputSearchScope::CurrentRun, _) => {
                OutputSearchScope::CurrentEpoch
            }
        };
    }

    fn begin_editing(&mut self) {
        self.phase = OutputSearchPhase::Editing;
        self.error = None;
        self.cursor = self.cursor.min(self.query.len());
    }
}

/// Tab completion state (LINE mode): newest-first deduplicated candidates.
#[derive(Debug)]
struct Completion {
    candidates: Vec<String>,
    current: usize,
}

impl SlotView {
    fn new(snapshot: SlotSnapshot) -> Self {
        let mut view = Self {
            last_epoch: Some(snapshot.daemon_epoch),
            last_seq: 0,
            snapshot,
            trigger_projection: None,
            subscription: SubscriptionPhase::Disconnected,
            lines: VecDeque::new(),
            pending_line: None,
            stream: TerminalStreamParser::new(),
            buffered_bytes: 0,
            local_history_truncated: false,
            scroll_snapshot: None,
            scroll_from_bottom: 0,
            unseen: 0,
            local_contiguous_from_seq: None,
            draft: Vec::new(),
            draft_cursor: 0,
            mode: InputMode::Line,
            history: Vec::new(),
            history_search: None,
            completion: None,
            last_manual_activity: None,
            run_history: VecDeque::new(),
            monitor_history: VecDeque::new(),
            run_history_limited: true,
            selected_run_command: None,
            expanded_run_command: None,
            selected_run_step: None,
            selected_monitor: None,
            expanded_monitor: None,
            selected_monitor_matcher: None,
            selected_monitor_incident: None,
            run_detail_scroll: 0,
        };
        view.sync_trigger_projection(false);
        view.sync_active_run_history();
        view
    }

    fn sync_active_run_history(&mut self) {
        let Some(run) = self.snapshot.active_run.clone() else {
            return;
        };
        self.upsert_run(&run);
    }

    fn clear_run_history(&mut self) {
        self.run_history.clear();
        self.run_history_limited = true;
        self.selected_run_command = None;
        self.expanded_run_command = None;
        self.selected_run_step = None;
        self.selected_monitor = None;
        self.expanded_monitor = None;
        self.selected_monitor_matcher = None;
        self.selected_monitor_incident = None;
        self.run_detail_scroll = 0;
    }

    fn forget_run_selection(&mut self, removed: Uuid) {
        if self
            .selected_run_command
            .is_some_and(|selected| selected.run_id == removed)
        {
            self.selected_run_command = None;
        }
        if self
            .expanded_run_command
            .is_some_and(|expanded| expanded.run_id == removed)
        {
            self.expanded_run_command = None;
            self.selected_run_step = None;
            self.run_detail_scroll = 0;
        }
    }

    fn sort_and_trim_run_history(&mut self) {
        // Stable ordering makes snapshot-seeded active Runs coexist with an
        // older replay irrespective of arrival order. The bounded projection
        // always drops the oldest start sequence, never the first insertion.
        self.run_history
            .make_contiguous()
            .sort_by_key(|entry| entry.start_seq);
        while self.run_history.len() > MAX_RUN_HISTORY_PER_SLOT {
            let Some(removed) = self.run_history.pop_front().map(|entry| entry.id) else {
                break;
            };
            self.run_history_limited = true;
            self.forget_run_selection(removed);
        }
    }

    fn upsert_run(&mut self, run: &RunInfo) {
        if run.owner.kind != serial_protocol::ActorKind::Agent {
            return;
        }
        if let Some(index) = self.run_history.iter().position(|entry| entry.id == run.id) {
            self.run_history[index].update_from_run(run);
            self.sort_and_trim_run_history();
            return;
        }
        self.run_history.push_back(RunHistoryEntry::from_run(run));
        self.sort_and_trim_run_history();
    }

    fn ensure_run_for_event(
        &mut self,
        event: &TimelineEvent,
        run_id: Uuid,
    ) -> Option<&mut RunHistoryEntry> {
        if let Some(index) = self.run_history.iter().position(|entry| entry.id == run_id) {
            return Some(&mut self.run_history[index]);
        }
        self.run_history.push_back(RunHistoryEntry {
            id: run_id,
            label: String::new(),
            status: RunStatus::Active,
            start_seq: event.seq,
            end_seq: None,
            commands: VecDeque::new(),
        });
        self.sort_and_trim_run_history();
        let index = self
            .run_history
            .iter()
            .position(|entry| entry.id == run_id)?;
        Some(&mut self.run_history[index])
    }

    fn observe_run_history(&mut self, event: &TimelineEvent) {
        match event.kind {
            EventKind::RunStarted | EventKind::RunEnded | EventKind::RunAborted => {
                let parsed = event
                    .metadata
                    .get("run")
                    .and_then(|value| serde_json::from_value::<RunInfo>(value.clone()).ok());
                let agent_run = parsed.as_ref().map_or_else(
                    || {
                        event
                            .actor
                            .as_ref()
                            .is_some_and(|actor| actor.kind == serial_protocol::ActorKind::Agent)
                    },
                    |run| run.owner.kind == serial_protocol::ActorKind::Agent,
                );
                if !agent_run {
                    return;
                }
                if let Some(run) = parsed.as_ref() {
                    self.upsert_run(run);
                    // A bounded projection may immediately discard a replayed
                    // Run whose authoritative start is older than everything
                    // retained. Do not reinsert it using this event's newer
                    // sequence as a misleading placeholder start.
                    if !self.run_history.iter().any(|entry| entry.id == run.id) {
                        return;
                    }
                }
                let run_id = parsed.as_ref().map(|run| run.id).or(event.run_id);
                let Some(run_id) = run_id else {
                    return;
                };
                let Some(entry) = self.ensure_run_for_event(event, run_id) else {
                    return;
                };
                match event.kind {
                    EventKind::RunStarted => entry.status = RunStatus::Active,
                    EventKind::RunEnded => {
                        entry.status = RunStatus::Completed;
                        entry.end_seq = Some(event.seq);
                    }
                    EventKind::RunAborted => {
                        entry.status = RunStatus::Aborted;
                        entry.end_seq = Some(event.seq);
                    }
                    _ => unreachable!("guarded by lifecycle match"),
                }
            }
            EventKind::Tx => {
                let Some(run_id) = event.run_id else {
                    return;
                };
                let described_agent_command = event.actor.as_ref().is_some_and(|actor| {
                    actor.kind == serial_protocol::ActorKind::Agent
                        && event
                            .metadata
                            .get("command_description")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(|description| !description.trim().is_empty())
                });
                if !described_agent_command {
                    return;
                }
                let (new_action, evicted) = {
                    let Some(entry) = self.ensure_run_for_event(event, run_id) else {
                        return;
                    };
                    entry.append_command(event)
                };
                if evicted {
                    self.run_history_limited = true;
                }
                if new_action {
                    self.selected_run_command = None;
                    self.expanded_run_command = None;
                    self.selected_run_step = None;
                    self.selected_monitor = None;
                    self.expanded_monitor = None;
                    self.selected_monitor_matcher = None;
                    self.selected_monitor_incident = None;
                    self.run_detail_scroll = 0;
                }
            }
            _ => {}
        }
    }

    fn run_command_keys(&self) -> Vec<RunCommandKey> {
        self.run_history_chronological()
            .into_iter()
            .flat_map(|run| {
                run.commands.iter().map(|command| RunCommandKey {
                    run_id: run.id,
                    first_seq: command.first_seq,
                })
            })
            .collect()
    }

    fn run_history_chronological(&self) -> Vec<&RunHistoryEntry> {
        let mut runs = self.run_history.iter().collect::<Vec<_>>();
        runs.sort_by_key(|run| run.start_seq);
        runs
    }

    fn selected_run_command_index(&self) -> Option<usize> {
        let keys = self.run_command_keys();
        (!keys.is_empty()).then(|| {
            self.selected_run_command
                .and_then(|selected| keys.iter().position(|key| *key == selected))
                .unwrap_or(keys.len() - 1)
        })
    }

    fn selected_run_command_key(&self) -> Option<RunCommandKey> {
        if self.selected_monitor.is_some() {
            return None;
        }
        let index = self.selected_run_command_index()?;
        self.run_command_keys().get(index).copied()
    }

    fn history_action_keys(&self) -> Vec<HistoryActionKey> {
        let mut actions = self
            .run_history_chronological()
            .into_iter()
            .flat_map(|run| {
                run.commands.iter().map(move |command| {
                    let key = RunCommandKey {
                        run_id: run.id,
                        first_seq: command.first_seq,
                    };
                    (
                        command.first_wall_time_ns,
                        command.first_seq,
                        run.id,
                        HistoryActionKey::Command(key),
                    )
                })
            })
            .chain(self.monitor_history.iter().map(|entry| {
                (
                    entry.monitor.created_wall_time_ns,
                    entry
                        .monitor
                        .spec
                        .start_cursor
                        .as_ref()
                        .map_or(0, |cursor| cursor.after_seq),
                    entry.monitor.id,
                    HistoryActionKey::Monitor(entry.monitor.id),
                )
            }))
            .collect::<Vec<_>>();
        actions.sort_by_key(|(wall_time, sequence, id, key)| {
            (
                *wall_time,
                *sequence,
                match key {
                    HistoryActionKey::Command(_) => 0u8,
                    HistoryActionKey::Monitor(_) => 1u8,
                },
                *id,
            )
        });
        actions.into_iter().map(|(_, _, _, key)| key).collect()
    }

    fn selected_history_action_index(&self) -> Option<usize> {
        let keys = self.history_action_keys();
        (!keys.is_empty()).then(|| {
            let selected = self
                .selected_monitor
                .map(HistoryActionKey::Monitor)
                .or_else(|| self.selected_run_command.map(HistoryActionKey::Command));
            selected
                .and_then(|selected| keys.iter().position(|key| *key == selected))
                .unwrap_or(keys.len() - 1)
        })
    }

    fn selected_history_action_key(&self) -> Option<HistoryActionKey> {
        let keys = self.history_action_keys();
        let index = self.selected_history_action_index()?;
        keys.get(index).copied()
    }

    fn select_history_action_index(&mut self, index: usize) {
        match self.history_action_keys().get(index).copied() {
            Some(HistoryActionKey::Command(key)) => {
                self.selected_run_command = Some(key);
                self.selected_monitor = None;
            }
            Some(HistoryActionKey::Monitor(id)) => {
                self.selected_run_command = None;
                self.selected_monitor = Some(id);
            }
            None => {
                self.selected_run_command = None;
                self.selected_monitor = None;
            }
        }
        self.expanded_run_command = None;
        self.selected_run_step = None;
        self.expanded_monitor = None;
        self.selected_monitor_matcher = None;
        self.selected_monitor_incident = None;
        self.run_detail_scroll = 0;
    }

    fn monitor(&self, id: Uuid) -> Option<&MonitorHistoryEntry> {
        self.monitor_history
            .iter()
            .find(|entry| entry.monitor.id == id)
    }

    fn monitor_incident_ids(&self, id: Uuid, matcher: usize) -> Vec<Uuid> {
        self.monitor(id)
            .map(|entry| {
                entry
                    .incidents
                    .iter()
                    .filter(|incident| incident.matches.iter().any(|item| item.index == matcher))
                    .map(|incident| incident.id)
                    .collect()
            })
            .unwrap_or_default()
    }

    fn selected_monitor_incident(&self) -> Option<&MonitorIncident> {
        let monitor = self.selected_monitor?;
        let incident = self.selected_monitor_incident?;
        self.monitor(monitor)?
            .incidents
            .iter()
            .find(|item| item.id == incident)
    }

    fn run_command(&self, key: RunCommandKey) -> Option<&RunCommandRecord> {
        self.run_history
            .iter()
            .find(|run| run.id == key.run_id)
            .and_then(|run| {
                run.commands
                    .iter()
                    .find(|command| command.first_seq == key.first_seq)
            })
    }

    fn next_run_command_seq(&self, key: RunCommandKey) -> Option<u64> {
        self.run_command_keys()
            .into_iter()
            .filter_map(|candidate| {
                (candidate != key && candidate.first_seq > key.first_seq)
                    .then_some(candidate.first_seq)
            })
            .min()
    }

    fn sync_trigger_projection(&mut self, live_start: bool) {
        self.trigger_projection = self
            .snapshot
            .active_trigger
            .as_ref()
            .map(|trigger| TriggerLiveProjection::new(trigger, live_start));
    }

    fn clear_trigger_projection(&mut self) {
        self.trigger_projection = None;
    }

    fn observe_trigger_rx(&mut self, data: &[u8]) {
        let (Some(trigger), Some(projection)) = (
            self.snapshot.active_trigger.as_mut(),
            self.trigger_projection.as_mut(),
        ) else {
            return;
        };
        if trigger.id != projection.trigger_id {
            self.sync_trigger_projection(false);
            return;
        }

        // seriald gives stop literals priority when the same RX chunk can
        // satisfy both start and stop. Mirror that exact ordering locally.
        if projection.stop_matcher.push(data) {
            trigger.status = TriggerStatus::Stopping;
            projection.status_known = true;
            return;
        }
        if projection
            .start_matcher
            .as_mut()
            .is_some_and(|matcher| matcher.push(data))
        {
            projection.start_seen = Some(true);
            projection.start_matcher = None;
            if !projection.initial_pending {
                trigger.status = TriggerStatus::Running;
                projection.status_known = true;
            }
        }
    }

    fn observe_trigger_tx(&mut self, event: &TimelineEvent) {
        let (Some(trigger), Some(projection)) = (
            self.snapshot.active_trigger.as_mut(),
            self.trigger_projection.as_mut(),
        ) else {
            return;
        };
        let trigger_id = event
            .metadata
            .get("trigger_id")
            .and_then(|value| serde_json::from_value::<Uuid>(value.clone()).ok());
        if trigger_id != Some(trigger.id) || projection.trigger_id != trigger.id {
            return;
        }

        trigger.last_write_seq = Some(event.seq);
        trigger.tx_bytes_confirmed = trigger
            .tx_bytes_confirmed
            .saturating_add(event.data.len() as u64);
        let full_write = !event
            .metadata
            .get("partial")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        if !full_write {
            trigger.status = TriggerStatus::Stopping;
            projection.status_known = true;
            return;
        }
        let was_stopping = trigger.status == TriggerStatus::Stopping;

        match event
            .metadata
            .get("trigger_write_kind")
            .and_then(serde_json::Value::as_str)
        {
            Some("initial") => {
                projection.initial_pending = false;
                if was_stopping {
                    return;
                }
                match projection.start_seen {
                    Some(true) => {
                        trigger.status = TriggerStatus::Running;
                        projection.status_known = true;
                    }
                    Some(false) => {
                        trigger.status = TriggerStatus::WaitingForStart;
                        projection.status_known = true;
                    }
                    None => {
                        // A reconnect snapshot can observe Armed after the
                        // start literal was already consumed but before the
                        // initial TX completes. Do not invent WaitingForStart
                        // in that ambiguous window.
                        projection.status_known = false;
                    }
                }
            }
            Some("action") => {
                projection.status_known = true;
                if let Some(fire_index) = event
                    .metadata
                    .get("fire_index")
                    .and_then(serde_json::Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok())
                {
                    trigger.fires_confirmed = trigger.fires_confirmed.max(fire_index);
                    // Reaching max_fires exhausts only the Trigger's write
                    // budget. When a stop matcher is configured, seriald
                    // continues observing RX until the original deadline so
                    // a prompt emitted after the final write can still match.
                    trigger.status = if was_stopping
                        || (fire_index >= trigger.spec.max_fires
                            && trigger.spec.stop_contains.is_empty())
                    {
                        TriggerStatus::Stopping
                    } else {
                        TriggerStatus::Running
                    };
                } else if !was_stopping {
                    trigger.status = TriggerStatus::Running;
                }
            }
            _ => {}
        }
    }

    fn mark_trigger_stopping(&mut self) {
        if let Some(trigger) = self.snapshot.active_trigger.as_mut() {
            trigger.status = TriggerStatus::Stopping;
        }
        if let Some(projection) = self.trigger_projection.as_mut() {
            projection.status_known = true;
        }
    }

    fn update_trigger_deadline(&mut self, now: Instant) -> bool {
        let Some(projection) = self.trigger_projection.as_mut() else {
            return false;
        };
        if projection.deadline.is_none_or(|deadline| now < deadline) {
            return false;
        }
        projection.deadline = None;
        let Some(trigger) = self.snapshot.active_trigger.as_mut() else {
            return false;
        };
        if trigger.status == TriggerStatus::Stopping {
            return false;
        }
        trigger.status = TriggerStatus::Stopping;
        projection.status_known = true;
        true
    }

    #[cfg(test)]
    fn trigger_status_text(&self) -> Option<&'static str> {
        let trigger = self.snapshot.active_trigger.as_ref()?;
        if self
            .trigger_projection
            .as_ref()
            .is_some_and(|projection| !projection.status_known)
        {
            Some(tr("trigger.status.active"))
        } else {
            Some(trigger_status_label(trigger.status))
        }
    }

    fn push_line(&mut self, line: DisplayLine, selected: bool) {
        self.buffered_bytes += line.bytes;
        self.lines.push_back(line);
        let mut evicted = 0usize;
        while self.lines.len() > MAX_LINES_PER_SLOT || self.buffered_bytes > MAX_BYTES_PER_SLOT {
            let Some(removed) = self.lines.pop_front() else {
                break;
            };
            self.buffered_bytes = self.buffered_bytes.saturating_sub(removed.bytes);
            evicted += 1;
        }
        let first_truncation = evicted > 0 && !self.local_history_truncated;
        self.local_history_truncated |= evicted > 0;
        self.run_history_limited |= evicted > 0;
        if self.scroll_snapshot.is_some() {
            // The paused viewport owns immutable visual rows. New output is
            // counted, but it must never alter the visual offset.
            self.unseen = self.unseen.saturating_add(1);
        } else if self.scroll_from_bottom > 0 {
            // Paused viewport: keep the same rows in view by pushing the
            // bottom-offset up with each appended row, and pulling it back
            // down for rows evicted from the front. The first eviction also
            // creates one synthetic truncation row at the front, so include
            // that row in the offset to keep the same retained row anchored.
            self.scroll_from_bottom = (self.scroll_from_bottom + 1)
                .saturating_sub(evicted)
                .saturating_add(usize::from(first_truncation));
            self.unseen = self.unseen.saturating_add(1);
        } else if !selected {
            self.unseen = self.unseen.saturating_add(1);
        }
    }

    fn push_event(&mut self, event: TimelineEvent, selected: bool) {
        if self.last_epoch == Some(event.daemon_epoch) && event.seq <= self.last_seq {
            return;
        }
        let follows_previous = self.last_epoch == Some(event.daemon_epoch)
            && self.last_seq.checked_add(1) == Some(event.seq);
        if self.last_epoch.is_some() && self.last_epoch != Some(event.daemon_epoch) {
            self.reset_stream();
            self.clear_run_history();
            self.push_line(gap_line(event.seq, tr("st.epoch.changed")), selected);
        }
        if event.kind == EventKind::Gap {
            self.local_contiguous_from_seq = None;
        } else if !follows_previous || self.local_contiguous_from_seq.is_none() {
            self.local_contiguous_from_seq = Some(event.seq);
        }
        if event.kind == EventKind::Gap {
            self.run_history_limited = true;
        }
        self.observe_run_history(&event);
        self.last_epoch = Some(event.daemon_epoch);
        self.last_seq = event.seq;
        // TX remains in the durable journal and the Agent command history,
        // but the serial pane represents bytes emitted by the device. If the
        // target echoes a command, its RX bytes appear naturally; echo-off
        // targets do not receive a synthetic local copy.
        if event.direction == Direction::Tx {
            return;
        }
        self.stream.set_echo_reconciliation(false);
        let had_pending = self.pending_line.is_some();
        let batch = self.stream.push_event(&event);
        let completed_pending = batch.pending_committed;
        for line in batch.completed {
            self.push_line(line, selected);
        }
        if completed_pending && (!selected || self.is_paused()) {
            // The unterminated row was already counted as unseen when it first
            // appeared; committing it must not count the same row twice.
            self.unseen = self.unseen.saturating_sub(1);
        }
        self.pending_line = batch.pending;
        if !had_pending && self.pending_line.is_some() && (!selected || self.is_paused()) {
            self.unseen = self.unseen.saturating_add(1);
        }
    }

    fn seed_startup_history(&mut self, history: StartupHistory, selected: bool) {
        if history.epoch != self.snapshot.daemon_epoch || history.port != self.snapshot.config.port
        {
            return;
        }

        self.local_history_truncated |= history.limited || history.error.is_some();
        self.run_history_limited |= history.limited || history.error.is_some();
        if let Some(error) = history.error.as_deref() {
            let marker_seq = history
                .events
                .first()
                .map_or(history.head_seq, |event| event.seq)
                .saturating_sub(1);
            self.push_line(
                gap_line(
                    marker_seq,
                    trf("history.startup.failed", &[&safe_inline(error)]),
                ),
                selected,
            );
        }

        let mut gaps = history.gaps.into_iter().peekable();
        for event in history.events {
            while gaps.peek().is_some_and(|gap| gap.first_seq <= event.seq) {
                let gap = gaps.next().expect("gap was just checked");
                self.push_gap(
                    gap.last_seq,
                    trf(
                        "m.logs.gap",
                        &[
                            &gap.first_seq.to_string(),
                            &gap.last_seq.to_string(),
                            gap_reason_label(gap.reason),
                            &gap.epoch.to_string(),
                        ],
                    ),
                    selected,
                );
            }
            self.push_event(event, selected);
        }
        for gap in gaps {
            self.push_gap(
                gap.last_seq,
                trf(
                    "m.logs.gap",
                    &[
                        &gap.first_seq.to_string(),
                        &gap.last_seq.to_string(),
                        gap_reason_label(gap.reason),
                        &gap.epoch.to_string(),
                    ],
                ),
                selected,
            );
        }
    }

    fn push_gap(&mut self, seq: u64, message: impl Into<String>, selected: bool) {
        self.run_history_limited = true;
        self.reset_stream();
        self.push_line(gap_line(seq, message), selected);
    }

    fn reset_stream(&mut self) {
        self.stream.reset();
        self.pending_line = None;
    }

    fn follow(&mut self) {
        self.scroll_snapshot = None;
        self.scroll_from_bottom = 0;
        self.unseen = 0;
    }

    fn is_paused(&self) -> bool {
        self.scroll_snapshot.is_some() || self.scroll_from_bottom > 0
    }

    fn active_agent_run(&self) -> Option<&RunInfo> {
        self.snapshot.active_run.as_ref().filter(|run| {
            run.owner.kind == serial_protocol::ActorKind::Agent
                && run.status == serial_protocol::RunStatus::Active
        })
    }

    fn logical_line_count(&self) -> usize {
        self.lines.len()
            + usize::from(self.pending_line.is_some())
            + usize::from(self.local_history_truncated)
    }

    fn local_truncation_line(&self) -> Option<DisplayLine> {
        self.local_history_truncated.then(|| {
            let seq = self
                .lines
                .front()
                .map_or(self.last_seq, |line| line.seq.saturating_sub(1));
            gap_line(seq, local_history_truncated_message())
        })
    }

    fn effective_echo(&self) -> EchoMode {
        self.snapshot.effective_echo.unwrap_or(EchoMode::On)
    }

    fn effective_write_eol(&self) -> &str {
        self.snapshot.effective_write_eol.as_deref().unwrap_or("\r")
    }

    fn effective_shell_prompt(&self) -> Option<&str> {
        self.snapshot.effective_shell_prompt.as_deref()
    }

    fn effective_uboot_prompt(&self) -> Option<&str> {
        self.snapshot.effective_uboot_prompt.as_deref()
    }
}

struct PendingPaste {
    port: String,
    bytes: Vec<u8>,
    raw: bool,
}

#[derive(Debug, Clone, Copy)]
struct QueuedControl {
    _position: usize,
    since: Instant,
}

#[derive(Debug, Clone)]
struct QueueSelection {
    port: String,
    selected: usize,
    detail_scroll: usize,
}

#[derive(Clone)]
struct MenuCatalog {
    ports: Vec<SlotSnapshot>,
    detected_ports: Vec<PortDescriptor>,
    config_revision: Option<u64>,
    transport_profiles: Vec<TransportProfile>,
    transport_revision: Option<u64>,
    model_profiles: Vec<ModelProfile>,
    model_profile_revision: Option<u64>,
}

#[derive(Clone)]
struct CurrentProfileEditor {
    original_port: String,
    port: String,
    original_transport_binding: Option<String>,
    transport_binding: Option<String>,
    original_transport: Option<TransportProfile>,
    transport: TransportProfile,
    original_model_profile_binding: Option<String>,
    model_profile_binding: Option<String>,
    original_model_name: Option<String>,
    model_name: Option<String>,
    original_device: Option<ModelProfile>,
    device: ModelProfile,
}

impl CurrentProfileEditor {
    fn new(view: &SlotView, catalog: &MenuCatalog) -> Self {
        let original_port = view.snapshot.config.port.clone();
        let port = original_port.clone();
        let original_transport_binding = view.snapshot.config.transport_profile.clone();
        let transport_binding = original_transport_binding.clone();
        let original_transport = catalog
            .transport_profiles
            .iter()
            .find(|profile| {
                view.snapshot.config.transport_profile.as_deref() == Some(profile.name.as_str())
            })
            .cloned();
        let transport = original_transport
            .clone()
            .unwrap_or_else(|| current_transport_template(view, catalog));
        let original_model_profile_binding = view.snapshot.config.model_profile.clone();
        let model_profile_binding = original_model_profile_binding.clone();
        let original_model_name = view.snapshot.config.model_name.clone();
        let model_name = original_model_name.clone();
        let original_device = view
            .snapshot
            .config
            .model_profile
            .as_deref()
            .and_then(|name| {
                catalog
                    .model_profiles
                    .iter()
                    .find(|profile| profile.name == name)
            })
            .cloned();
        let device = original_device
            .clone()
            .unwrap_or_else(|| current_model_profile_template(view));
        Self {
            original_port,
            port,
            original_transport_binding,
            transport_binding,
            original_transport,
            transport,
            original_model_profile_binding,
            model_profile_binding,
            original_model_name,
            model_name,
            original_device,
            device,
        }
    }

    fn port_update(&self) -> Option<String> {
        (self.port != self.original_port).then(|| self.port.clone())
    }

    fn transport_update(&self) -> Option<TransportProfile> {
        (self.transport_binding == self.original_transport_binding)
            .then_some(())
            .and(
                self.original_transport
                    .as_ref()
                    .filter(|original| *original != &self.transport)
                    .map(|_| self.transport.clone()),
            )
    }

    fn device_update(&self) -> Option<ModelProfile> {
        (self.model_profile_binding == self.original_model_profile_binding)
            .then_some(())
            .and(
                self.original_device
                    .as_ref()
                    .filter(|original| *original != &self.device)
                    .map(|_| self.device.clone()),
            )
    }

    fn transport_binding_update(&self) -> Option<Option<String>> {
        (self.transport_binding != self.original_transport_binding)
            .then(|| self.transport_binding.clone())
    }

    fn model_profile_binding_update(&self) -> Option<Option<String>> {
        (self.model_profile_binding != self.original_model_profile_binding)
            .then(|| self.model_profile_binding.clone())
    }

    fn model_name_update(&self) -> Option<Option<String>> {
        (self.model_name != self.original_model_name).then(|| self.model_name.clone())
    }

    fn changed(&self) -> bool {
        self.port_update().is_some()
            || self.transport_binding_update().is_some()
            || self.transport_update().is_some()
            || self.model_profile_binding_update().is_some()
            || self.model_name_update().is_some()
            || self.device_update().is_some()
    }

    fn device_is_bound(&self) -> bool {
        self.model_profile_binding.is_some()
    }
}

fn shared_profile_impacts(
    catalog: &MenuCatalog,
    transport: Option<&TransportProfile>,
    device: Option<&ModelProfile>,
) -> SharedProfileImpacts {
    fn matching_slots(
        catalog: &MenuCatalog,
        mut matches: impl FnMut(&SlotSnapshot) -> bool,
    ) -> Vec<(String, String)> {
        let mut ports = catalog
            .ports
            .iter()
            .filter(|slot| matches(slot))
            .map(|slot| (slot.config.port.clone(), slot.config.port.clone()))
            .collect::<Vec<_>>();
        ports.sort_by(|left, right| left.0.cmp(&right.0));
        ports
    }

    SharedProfileImpacts {
        transport: transport.map(|profile| SharedProfileImpact {
            profile_name: profile.name.clone(),
            ports: matching_slots(catalog, |slot| {
                slot.config.transport_profile.as_deref() == Some(profile.name.as_str())
            }),
        }),
        device: device.map(|profile| SharedProfileImpact {
            profile_name: profile.name.clone(),
            ports: matching_slots(catalog, |slot| {
                slot.config.model_profile.as_deref() == Some(profile.name.as_str())
            }),
        }),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MenuPage {
    Root,
    Profiles,
    CreateProfiles,
    CreateTransportProfile,
    CreateModelProfile,
    Settings,
    ModelFamilies,
    ModelNames,
    DisplaySettings,
    McpSettings,
    Help,
}

struct MenuState {
    page: MenuPage,
    selected: usize,
    stack: Vec<(MenuPage, usize)>,
    catalog: Option<MenuCatalog>,
    profile_editor: Option<CurrentProfileEditor>,
    create_transport: Option<TransportProfile>,
    create_model: Option<ModelProfile>,
    choice: Option<MenuChoice>,
    model_family: Option<String>,
    prompt: Option<MenuPrompt>,
    confirmation: Option<MenuConfirmation>,
    field_help: Option<String>,
    help_scroll: usize,
    busy: bool,
    message: String,
}

impl MenuState {
    fn new() -> Self {
        Self {
            page: MenuPage::Root,
            selected: 0,
            stack: Vec::new(),
            catalog: None,
            profile_editor: None,
            create_transport: None,
            create_model: None,
            choice: None,
            model_family: None,
            prompt: None,
            confirmation: None,
            field_help: None,
            help_scroll: 0,
            busy: false,
            message: tr("menu.loading").into(),
        }
    }

    fn push(&mut self, page: MenuPage) {
        self.stack.push((self.page, self.selected));
        self.page = page;
        self.selected = 0;
        self.choice = None;
        self.help_scroll = 0;
    }

    fn back(&mut self) -> bool {
        if let Some((page, selected)) = self.stack.pop() {
            self.page = page;
            self.selected = selected;
            self.choice = None;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
struct MenuChoice {
    purpose: MenuChoicePurpose,
    options: Vec<MenuChoiceOption>,
    selected: usize,
}

#[derive(Clone)]
struct MenuChoiceOption {
    label: String,
    value: MenuChoiceValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MenuChoicePurpose {
    CurrentPort,
    CurrentTransportProfile,
    CurrentModelProfile,
    CurrentBaudRate,
    CurrentDataBits,
    CurrentParity,
    CurrentStopBits,
    CurrentFlowControl,
    CurrentDtr,
    CurrentRts,
    CurrentAutoOpen,
    CurrentWriteEol,
    CurrentEcho,
    CreateTransportBaudRate,
    CreateTransportDataBits,
    CreateTransportParity,
    CreateTransportStopBits,
    CreateTransportFlowControl,
    CreateTransportDtr,
    CreateTransportRts,
    CreateTransportAutoOpen,
    CreateModelWriteEol,
    CreateModelEcho,
}

#[derive(Clone)]
enum MenuChoiceValue {
    Text(String),
    OptionalText(Option<String>),
    Number(u32),
    DataBits(DataBits),
    Parity(Parity),
    StopBits(StopBits),
    FlowControl(FlowControl),
    Bool(bool),
    Eol(Option<String>),
    Echo(Option<EchoMode>),
}

struct MenuConfirmation {
    title: String,
    lines: Vec<String>,
    scroll: usize,
    cancelled_message: String,
    action: MenuConfirmationAction,
}

enum MenuConfirmationAction {
    Mutation(MenuMutation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SharedProfileImpact {
    profile_name: String,
    ports: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SharedProfileImpacts {
    transport: Option<SharedProfileImpact>,
    device: Option<SharedProfileImpact>,
}

struct MenuPrompt {
    title: String,
    value: Vec<char>,
    cursor: usize,
    purpose: MenuPromptPurpose,
}

enum MenuPromptPurpose {
    CurrentProfile(CurrentProfilePromptField),
    CreateTransport(CreateTransportPromptField),
    CreateModel(CreateModelPromptField),
    AgentHistoryRows,
    OrphanRunTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurrentProfilePromptField {
    ShellPrompt,
    UbootPrompt,
    ChunkSize,
    ChunkDelay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateTransportPromptField {
    Name,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateModelPromptField {
    Name,
    ModelNames,
    ShellPrompt,
    UbootPrompt,
    ChunkSize,
    ChunkDelay,
}

enum MenuMutation {
    CreateTransport { profile: TransportProfile },
    CreateModelProfile { profile: ModelProfile },
    UpdateCurrentProfiles(Box<CurrentProfileUpdate>),
}

struct CurrentProfileUpdate {
    current_port: String,
    new_port: Option<String>,
    transport_binding: Option<Option<String>>,
    transport: Option<TransportProfile>,
    model_profile_binding: Option<Option<String>>,
    model_name: Option<Option<String>>,
    device: Option<ModelProfile>,
    revisions: CurrentProfileRevisions,
}

#[derive(Debug, Clone, Copy)]
struct CurrentProfileRevisions {
    config: Option<u64>,
    transport: Option<u64>,
    device: Option<u64>,
}

enum MenuIoCommand {
    Reload,
    Mutation { mutation: Box<MenuMutation> },
}

#[derive(Clone)]
enum MenuSuccess {
    Loaded,
    TransportCreated(String),
    ModelProfileCreated(String),
    ProfilesUpdated {
        previous_port: String,
        configured_port: String,
    },
}

enum MenuIoEvent {
    Completed {
        catalog: MenuCatalog,
        success: MenuSuccess,
    },
    Failed(String),
}

const CURRENT_PROFILE_ROW_COUNT: usize = 19;
const CREATE_TRANSPORT_ROW_COUNT: usize = 10;
const CREATE_MODEL_ROW_COUNT: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CurrentProfileRow {
    Port,
    TransportProfile,
    BaudRate,
    DataBits,
    Parity,
    StopBits,
    FlowControl,
    Dtr,
    Rts,
    AutoOpen,
    ModelProfile,
    ModelName,
    WriteEol,
    Echo,
    ShellPrompt,
    UbootPrompt,
    ChunkSize,
    ChunkDelay,
    Apply,
}

impl CurrentProfileRow {
    fn from_index(index: usize) -> Option<Self> {
        Some(match index {
            0 => Self::Port,
            1 => Self::TransportProfile,
            2 => Self::BaudRate,
            3 => Self::DataBits,
            4 => Self::Parity,
            5 => Self::StopBits,
            6 => Self::FlowControl,
            7 => Self::Dtr,
            8 => Self::Rts,
            9 => Self::AutoOpen,
            10 => Self::ModelProfile,
            11 => Self::ModelName,
            12 => Self::WriteEol,
            13 => Self::Echo,
            14 => Self::ShellPrompt,
            15 => Self::UbootPrompt,
            16 => Self::ChunkSize,
            17 => Self::ChunkDelay,
            18 => Self::Apply,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateTransportRow {
    Name,
    BaudRate,
    DataBits,
    Parity,
    StopBits,
    FlowControl,
    Dtr,
    Rts,
    AutoOpen,
    Save,
}

impl CreateTransportRow {
    fn from_index(index: usize) -> Option<Self> {
        Some(match index {
            0 => Self::Name,
            1 => Self::BaudRate,
            2 => Self::DataBits,
            3 => Self::Parity,
            4 => Self::StopBits,
            5 => Self::FlowControl,
            6 => Self::Dtr,
            7 => Self::Rts,
            8 => Self::AutoOpen,
            9 => Self::Save,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateModelRow {
    Name,
    ModelNames,
    WriteEol,
    Echo,
    ShellPrompt,
    UbootPrompt,
    ChunkSize,
    ChunkDelay,
    Save,
}

impl CreateModelRow {
    fn from_index(index: usize) -> Option<Self> {
        Some(match index {
            0 => Self::Name,
            1 => Self::ModelNames,
            2 => Self::WriteEol,
            3 => Self::Echo,
            4 => Self::ShellPrompt,
            5 => Self::UbootPrompt,
            6 => Self::ChunkSize,
            7 => Self::ChunkDelay,
            8 => Self::Save,
            _ => return None,
        })
    }
}

fn default_transport_profile(name: String) -> TransportProfile {
    TransportProfile {
        name,
        baud_rate: 115_200,
        data_bits: DataBits::Eight,
        parity: Parity::None,
        stop_bits: StopBits::One,
        flow_control: FlowControl::None,
        dtr: false,
        rts: false,
        auto_open: true,
    }
}

fn current_transport_template(view: &SlotView, catalog: &MenuCatalog) -> TransportProfile {
    if let Some(profile) = catalog.transport_profiles.iter().find(|profile| {
        view.snapshot.config.transport_profile.as_deref() == Some(profile.name.as_str())
    }) {
        return profile.clone();
    }
    let settings = view
        .snapshot
        .effective_transport
        .unwrap_or(ResolvedTransportSettings {
            baud_rate: 115_200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        });
    TransportProfile {
        name: view
            .snapshot
            .config
            .transport_profile
            .clone()
            .unwrap_or_else(|| format!("{}-uart", view.snapshot.config.port)),
        baud_rate: settings.baud_rate,
        data_bits: settings.data_bits,
        parity: settings.parity,
        stop_bits: settings.stop_bits,
        flow_control: settings.flow_control,
        dtr: settings.dtr,
        rts: settings.rts,
        auto_open: settings.auto_open,
    }
}

fn current_model_profile_template(view: &SlotView) -> ModelProfile {
    let pacing = view
        .snapshot
        .effective_write_pacing
        .unwrap_or(serial_protocol::WritePacing {
            chunk_size: 1,
            chunk_delay_ms: 1,
        });
    ModelProfile {
        name: String::new(),
        model_names: Vec::new(),
        shell_prompt: view.effective_shell_prompt().map(ToOwned::to_owned),
        uboot_prompt: view.effective_uboot_prompt().map(ToOwned::to_owned),
        write_eol: Some(view.effective_write_eol().to_owned()),
        echo: Some(view.effective_echo()),
        write_chunk_size: Some(pacing.chunk_size),
        write_chunk_delay_ms: Some(pacing.chunk_delay_ms),
    }
}

fn valid_menu_name(value: &str) -> bool {
    !value.is_empty()
        && value == value.trim()
        && value.len() <= 128
        && !value.chars().any(char::is_control)
}

fn menu_item_count(menu: &MenuState) -> usize {
    match menu.page {
        MenuPage::Root => 4,
        MenuPage::Profiles => CURRENT_PROFILE_ROW_COUNT,
        MenuPage::CreateProfiles | MenuPage::Settings => 2,
        MenuPage::CreateTransportProfile => CREATE_TRANSPORT_ROW_COUNT,
        MenuPage::CreateModelProfile => CREATE_MODEL_ROW_COUNT,
        MenuPage::ModelFamilies => menu.catalog.as_ref().map_or(0, |catalog| {
            catalog
                .model_profiles
                .iter()
                .filter(|profile| !profile.model_names.is_empty())
                .count()
        }),
        MenuPage::ModelNames => menu.catalog.as_ref().map_or(0, |catalog| {
            menu.model_family.as_deref().map_or(0, |family| {
                catalog
                    .model_profiles
                    .iter()
                    .find(|profile| profile.name == family)
                    .map_or(0, |profile| profile.model_names.len())
            })
        }),
        MenuPage::DisplaySettings => 1,
        MenuPage::McpSettings => 1,
        MenuPage::Help => 0,
    }
}

fn menu_success_message(success: &MenuSuccess) -> String {
    match success {
        MenuSuccess::Loaded => tr("menu.loaded").into(),
        MenuSuccess::TransportCreated(name) => trf("menu.transport.created", &[name]),
        MenuSuccess::ModelProfileCreated(name) => trf("menu.device.created", &[name]),
        MenuSuccess::ProfilesUpdated { .. } => tr("menu.profile.updated").into(),
    }
}

#[derive(Debug)]
struct OutputSearchRequest {
    request_id: Uuid,
    port: String,
    current_epoch: Uuid,
    head_seq: u64,
    current_run: Option<OutputSearchRun>,
    scope: OutputSearchScope,
    direction: OutputSearchDirection,
    contains: Option<String>,
    regex: Option<String>,
}

#[derive(Debug)]
enum OutputSearchIoCommand {
    Query(OutputSearchRequest),
    Cancel { request_id: Uuid },
}

#[derive(Debug)]
struct OutputSearchResponse {
    events: Vec<TimelineEvent>,
    gaps: Vec<GapRange>,
    partial: bool,
    scanned_archives: usize,
}

#[derive(Debug)]
enum OutputSearchIoEvent {
    Completed {
        request_id: Uuid,
        response: OutputSearchResponse,
    },
    Failed {
        request_id: Uuid,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct IncidentEvidenceTarget {
    incident_id: Uuid,
    port: String,
    daemon_epoch: Uuid,
    seq_start: u64,
    seq_end: u64,
}

impl From<&MonitorIncident> for IncidentEvidenceTarget {
    fn from(incident: &MonitorIncident) -> Self {
        Self {
            incident_id: incident.id,
            port: incident.port.clone(),
            daemon_epoch: incident.daemon_epoch,
            seq_start: incident.seq_start,
            seq_end: incident.seq_end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandEvidenceTarget {
    key: RunCommandKey,
    step_index: Option<usize>,
    port: String,
    daemon_epoch: Uuid,
    seq_start: u64,
    write_end_seq: u64,
    query_end_seq: u64,
    command: String,
    matchers: Vec<CommandCaptureMatcher>,
}

impl CommandEvidenceTarget {
    fn same_selection(&self, other: &Self) -> bool {
        self.key == other.key
            && self.step_index == other.step_index
            && self.port == other.port
            && self.daemon_epoch == other.daemon_epoch
            && self.seq_start == other.seq_start
            && self.write_end_seq == other.write_end_seq
            && self.command == other.command
            && self.matchers == other.matchers
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactEvidenceTarget {
    Incident(IncidentEvidenceTarget),
    Command(CommandEvidenceTarget),
}

impl ExactEvidenceTarget {
    fn port(&self) -> &str {
        match self {
            Self::Incident(target) => &target.port,
            Self::Command(target) => &target.port,
        }
    }

    fn daemon_epoch(&self) -> Uuid {
        match self {
            Self::Incident(target) => target.daemon_epoch,
            Self::Command(target) => target.daemon_epoch,
        }
    }

    fn seq_start(&self) -> u64 {
        match self {
            Self::Incident(target) => target.seq_start,
            Self::Command(target) => target.seq_start,
        }
    }

    fn query_end_seq(&self) -> u64 {
        match self {
            Self::Incident(target) => target.seq_end,
            Self::Command(target) => target.query_end_seq,
        }
    }
}

#[derive(Debug)]
struct ExactEvidenceRequest {
    request_id: Uuid,
    target: ExactEvidenceTarget,
}

#[derive(Debug)]
enum ExactEvidenceIoCommand {
    Query(ExactEvidenceRequest),
}

#[derive(Debug)]
struct ExactEvidenceResponse {
    target: ExactEvidenceTarget,
    events: Vec<TimelineEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExactEvidenceFailure {
    Gap(GapRange),
    Incomplete,
    LimitExceeded,
    QueryFailed(String),
}

#[derive(Debug)]
enum ExactEvidenceIoEvent {
    Completed {
        request_id: Uuid,
        response: ExactEvidenceResponse,
    },
    Failed {
        request_id: Uuid,
        target: ExactEvidenceTarget,
        failure: ExactEvidenceFailure,
    },
}

struct App {
    ports: Vec<SlotView>,
    selected: usize,
    prefix_pending: bool,
    /// The prefix key was pressed while dismissing the help overlay. The
    /// following `?` belongs to that same shortcut and must not enter the LINE
    /// draft or reopen help.
    help_dismiss_prefix: bool,
    help: bool,
    /// First visual row displayed by the grouped help popup. Help owns its
    /// own scroll state so narrow terminals do not overload serial output
    /// scrolling or close the popup when PageUp/PageDown is pressed.
    help_scroll: usize,
    detailed_timeline: bool,
    transport_connected: bool,
    hello_accepted: bool,
    connection_generation: Option<u64>,
    actor: Option<Actor>,
    status: String,
    /// The old permanent status strip mixed control/trigger noise with useful
    /// errors. Keep the existing status producers, but surface each changed
    /// message only briefly in the ordinary one-line footer.
    status_notice_source: String,
    status_notice_until: Option<Instant>,
    pending_paste: Option<PendingPaste>,
    pending_writes: HashMap<String, VecDeque<PendingWrite>>,
    /// Current physical chunk within the first queued operation for each Port.
    /// The complete operation stays in `pending_writes` until every chunk is
    /// acknowledged, so its UI card never disappears or shrinks in flight.
    inflight_writes: HashMap<String, InFlightWrite>,
    pending_requests: HashMap<Uuid, PendingRequest>,
    queued_controls: HashMap<String, QueuedControl>,
    queue_selection: Option<QueueSelection>,
    menu: Option<MenuState>,
    menu_commands: Option<mpsc::Sender<MenuIoCommand>>,
    output_search: Option<OutputSearchState>,
    output_search_commands: Option<mpsc::Sender<OutputSearchIoCommand>>,
    exact_evidence_commands: Option<mpsc::Sender<ExactEvidenceIoCommand>>,
    pending_exact_evidence: Option<(Uuid, ExactEvidenceTarget)>,
    uncertain_write_outcomes: usize,
    human_idle_release: Duration,
    mouse_capture: bool,
    run_panel_visible: bool,
    agent_history_rows: u16,
    orphan_run_timeout_seconds: u64,
    focus: PaneFocus,
    layout: Option<ConsoleLayout>,
    /// Only the currently active left-button drag keeps a stable visual
    /// snapshot. Once the drag finishes, the selected text moves to
    /// `selection_copy` so live output resumes immediately.
    selection: Option<TextSelection>,
    selection_copy: Option<String>,
    last_output_click: Option<OutputClick>,
    clipboard_copy: ClipboardCopyFn,
    config: Option<LoadedConfig>,
    /// Software cursor timing is independent of serial repaint frequency.
    /// Input resets the phase to visible; only the 600 ms phase transition
    /// requests another frame.
    software_cursor_blink_started: Instant,
    software_cursor_visible: bool,
    should_quit: bool,
    dirty: bool,
}

impl App {
    fn new(ports: Vec<SlotSnapshot>, initial_port: Option<&str>) -> Self {
        let ports = ports.into_iter().map(SlotView::new).collect::<Vec<_>>();
        let initial_status = tr("st.connecting").to_string();
        let selected = initial_port
            .and_then(|requested| {
                ports
                    .iter()
                    .position(|slot| slot.snapshot.config.port == requested)
            })
            .unwrap_or(0);
        Self {
            ports,
            selected,
            prefix_pending: false,
            help_dismiss_prefix: false,
            help: false,
            help_scroll: 0,
            detailed_timeline: false,
            transport_connected: false,
            hello_accepted: false,
            connection_generation: None,
            actor: None,
            status: initial_status.clone(),
            status_notice_source: initial_status,
            status_notice_until: None,
            pending_paste: None,
            pending_writes: HashMap::new(),
            inflight_writes: HashMap::new(),
            pending_requests: HashMap::new(),
            queued_controls: HashMap::new(),
            queue_selection: None,
            menu: None,
            menu_commands: None,
            output_search: None,
            output_search_commands: None,
            exact_evidence_commands: None,
            pending_exact_evidence: None,
            uncertain_write_outcomes: 0,
            human_idle_release: Duration::from_secs(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS),
            mouse_capture: true,
            run_panel_visible: true,
            agent_history_rows: DEFAULT_AGENT_HISTORY_ROWS,
            orphan_run_timeout_seconds: DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS,
            focus: PaneFocus::Input,
            layout: None,
            selection: None,
            selection_copy: None,
            last_output_click: None,
            clipboard_copy: default_clipboard_copy,
            config: None,
            software_cursor_blink_started: Instant::now(),
            software_cursor_visible: true,
            should_quit: false,
            dirty: true,
        }
    }

    fn software_cursor_active(&self) -> bool {
        if let Some(menu) = self.menu.as_ref() {
            return menu.confirmation.is_none() && menu.prompt.is_some();
        }
        if let Some(search) = self.output_search.as_ref() {
            return search.phase == OutputSearchPhase::Editing;
        }
        !self.help
            && self.queue_selection.is_none()
            && self.focus == PaneFocus::Input
            && self.current_mode() == InputMode::Line
            && self.current().history_search.is_none()
            && !(self.current().draft.is_empty() && self.current().active_agent_run().is_some())
    }

    fn reset_software_cursor_blink(&mut self, now: Instant) {
        self.software_cursor_blink_started = now;
        self.software_cursor_visible = true;
        self.dirty = true;
    }

    fn update_software_cursor_blink(&mut self, now: Instant) -> bool {
        if !self.software_cursor_active() {
            self.software_cursor_blink_started = now;
            let changed = !self.software_cursor_visible;
            self.software_cursor_visible = true;
            return changed;
        }
        let elapsed = now
            .checked_duration_since(self.software_cursor_blink_started)
            .unwrap_or_default();
        let interval = SOFTWARE_CURSOR_BLINK_INTERVAL.as_millis().max(1);
        let visible = (elapsed.as_millis() / interval).is_multiple_of(2);
        let changed = visible != self.software_cursor_visible;
        self.software_cursor_visible = visible;
        changed
    }

    fn apply_startup_history(&mut self, history: StartupHistory) -> Option<(String, Cursor)> {
        let resume = history
            .resume_cursor
            .clone()
            .map(|cursor| (history.port.clone(), cursor));
        let index = self.slot_index(&history.port)?;
        let selected = index == self.selected;
        self.ports[index].seed_startup_history(history, selected);
        resume
    }

    fn current(&self) -> &SlotView {
        &self.ports[self.selected]
    }

    fn current_mut(&mut self) -> &mut SlotView {
        &mut self.ports[self.selected]
    }

    fn selected_port(&self) -> String {
        self.current().snapshot.config.port.clone()
    }

    fn current_model_profile_name(&self) -> String {
        self.current()
            .snapshot
            .config
            .model_name
            .clone()
            .or_else(|| self.current().snapshot.config.model_profile.clone())
            .unwrap_or_else(|| tr("ui.output.model.unconfigured").into())
    }

    fn open_output_search(&mut self) {
        let view = self.current();
        let current_run = view.active_agent_run().map(|run| OutputSearchRun {
            id: run.id,
            start_seq: run.start_seq,
            through_seq: view.snapshot.head_seq,
        });
        self.output_search = Some(OutputSearchState {
            port: view.snapshot.config.port.clone(),
            current_epoch: view.snapshot.daemon_epoch,
            head_seq: view.snapshot.head_seq,
            current_run,
            previous_focus: self.focus,
            query: Vec::new(),
            cursor: 0,
            matcher: OutputSearchMatcher::Literal,
            case_sensitive: false,
            direction: OutputSearchDirection::Both,
            scope: OutputSearchScope::CurrentEpoch,
            phase: OutputSearchPhase::Editing,
            results: Vec::new(),
            selected: 0,
            detail_scroll: 0,
            gaps: Vec::new(),
            partial: false,
            scanned_archives: 0,
            error: None,
        });
        self.status = tr("st.output.search.open").into();
    }

    fn submit_output_search(&mut self, search: &mut OutputSearchState) {
        let query = search.query_text();
        if query.is_empty() {
            search.error = Some(tr("ui.output.search.empty").into());
            return;
        }
        let Some(view) = self
            .ports
            .iter()
            .find(|view| view.snapshot.config.port == search.port)
        else {
            search.error = Some(tr("ui.output.search.port.missing").into());
            return;
        };
        // The popup may remain open while live Snapshot/events continue to
        // advance. Bind every actual query (including retry) to the latest
        // authoritative Port view, never to the state captured when `/` was
        // first pressed.
        search.current_epoch = view.snapshot.daemon_epoch;
        search.head_seq = view.snapshot.head_seq;
        search.current_run = view.active_agent_run().map(|run| OutputSearchRun {
            id: run.id,
            start_seq: run.start_seq,
            through_seq: view.snapshot.head_seq,
        });
        let (contains, regex) = output_search_filter(&query, search.matcher, search.case_sensitive);
        if contains
            .as_ref()
            .or(regex.as_ref())
            .is_some_and(|filter| filter.len() > OUTPUT_SEARCH_QUERY_BYTES)
        {
            search.error = Some(trf(
                "ui.output.search.too.long",
                &[&OUTPUT_SEARCH_QUERY_BYTES.to_string()],
            ));
            return;
        }
        if search.scope == OutputSearchScope::CurrentRun && search.current_run.is_none() {
            search.error = Some(tr("ui.output.search.no.run").into());
            return;
        }
        let Some(commands) = self.output_search_commands.as_ref() else {
            search.error = Some(tr("ui.output.search.unavailable").into());
            return;
        };
        let request_id = Uuid::new_v4();
        let request = OutputSearchRequest {
            request_id,
            port: search.port.clone(),
            current_epoch: search.current_epoch,
            head_seq: search.head_seq,
            current_run: search.current_run,
            scope: search.scope,
            direction: search.direction,
            contains,
            regex,
        };
        match commands.try_send(OutputSearchIoCommand::Query(request)) {
            Ok(()) => {
                search.phase = OutputSearchPhase::Loading(request_id);
                search.error = None;
            }
            Err(_) => search.error = Some(tr("ui.output.search.busy").into()),
        }
    }

    fn handle_output_search_key(&mut self, key: KeyEvent) {
        let Some(mut search) = self.output_search.take() else {
            return;
        };
        let mut keep_open = true;
        match search.phase {
            OutputSearchPhase::Editing => match key.code {
                KeyCode::Esc => keep_open = false,
                KeyCode::Enter => self.submit_output_search(&mut search),
                KeyCode::F(2) | KeyCode::Tab => {
                    search.matcher = search.matcher.toggled();
                    search.error = None;
                }
                KeyCode::F(3) => {
                    search.case_sensitive = !search.case_sensitive;
                    search.error = None;
                }
                KeyCode::F(4) => {
                    search.direction = search.direction.next();
                    search.error = None;
                }
                KeyCode::F(5) => {
                    search.cycle_scope();
                    search.error = None;
                }
                KeyCode::Left => search.cursor = search.cursor.saturating_sub(1),
                KeyCode::Right => search.cursor = (search.cursor + 1).min(search.query.len()),
                KeyCode::Home => search.cursor = 0,
                KeyCode::End => search.cursor = search.query.len(),
                KeyCode::Backspace => {
                    if search.cursor > 0 {
                        search.cursor -= 1;
                        search.query.remove(search.cursor);
                    }
                    search.error = None;
                }
                KeyCode::Delete => {
                    if search.cursor < search.query.len() {
                        search.query.remove(search.cursor);
                    }
                    search.error = None;
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    keep_open = false;
                }
                KeyCode::Char(character)
                    if !key
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                        && !character.is_control() =>
                {
                    let mut candidate = search.query.clone();
                    candidate.insert(search.cursor, character);
                    if candidate.iter().collect::<String>().len() <= OUTPUT_SEARCH_QUERY_BYTES {
                        search.query = candidate;
                        search.cursor += 1;
                        search.error = None;
                    }
                }
                _ => {}
            },
            OutputSearchPhase::Loading(_) => match key.code {
                KeyCode::Esc => keep_open = false,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    keep_open = false
                }
                _ => {}
            },
            OutputSearchPhase::Results => match key.code {
                KeyCode::Esc => keep_open = false,
                KeyCode::Char('/') | KeyCode::Char('e' | 'E') => search.begin_editing(),
                KeyCode::Char('r' | 'R') => self.submit_output_search(&mut search),
                KeyCode::F(2) | KeyCode::Tab => {
                    search.matcher = search.matcher.toggled();
                    search.begin_editing();
                }
                KeyCode::F(3) => {
                    search.case_sensitive = !search.case_sensitive;
                    search.begin_editing();
                }
                KeyCode::F(4) => {
                    search.direction = search.direction.next();
                    search.begin_editing();
                }
                KeyCode::F(5) => {
                    search.cycle_scope();
                    search.begin_editing();
                }
                KeyCode::Up | KeyCode::Char('N') => {
                    search.selected = search.selected.saturating_sub(1);
                    search.detail_scroll = 0;
                }
                KeyCode::Down | KeyCode::Char('n') => {
                    search.selected =
                        (search.selected + 1).min(search.results.len().saturating_sub(1));
                    search.detail_scroll = 0;
                }
                KeyCode::PageUp => search.detail_scroll = search.detail_scroll.saturating_sub(5),
                KeyCode::PageDown => search.detail_scroll = search.detail_scroll.saturating_add(5),
                KeyCode::Home => {
                    search.selected = 0;
                    search.detail_scroll = 0;
                }
                KeyCode::End => {
                    search.selected = search.results.len().saturating_sub(1);
                    search.detail_scroll = 0;
                }
                _ => {}
            },
        }
        if keep_open {
            self.output_search = Some(search);
        } else {
            if let OutputSearchPhase::Loading(request_id) = search.phase
                && let Some(commands) = self.output_search_commands.as_ref()
            {
                let _ = commands.try_send(OutputSearchIoCommand::Cancel { request_id });
            }
            self.focus = search.previous_focus;
            self.status = tr("st.output.search.closed").into();
        }
    }

    fn handle_output_search_paste(&mut self, value: String) {
        let Some(search) = self.output_search.as_mut() else {
            return;
        };
        if search.phase != OutputSearchPhase::Editing {
            return;
        }
        for character in value.chars().filter(|character| !character.is_control()) {
            let mut candidate = search.query.clone();
            candidate.insert(search.cursor, character);
            if candidate.iter().collect::<String>().len() > OUTPUT_SEARCH_QUERY_BYTES {
                break;
            }
            search.query = candidate;
            search.cursor += 1;
        }
        search.error = None;
    }

    fn handle_output_search_io_event(&mut self, event: OutputSearchIoEvent) {
        let Some(search) = self.output_search.as_mut() else {
            return;
        };
        match event {
            OutputSearchIoEvent::Completed {
                request_id,
                response,
            } if search.phase == OutputSearchPhase::Loading(request_id) => {
                search.results = response.events;
                search.gaps = response.gaps;
                search.partial = response.partial;
                search.scanned_archives = response.scanned_archives;
                search.selected = 0;
                search.detail_scroll = 0;
                search.error = None;
                search.phase = OutputSearchPhase::Results;
            }
            OutputSearchIoEvent::Failed {
                request_id,
                message,
            } if search.phase == OutputSearchPhase::Loading(request_id) => {
                search.phase = OutputSearchPhase::Editing;
                search.error = Some(trf("ui.output.search.failed", &[&safe_inline(&message)]));
            }
            _ => return,
        }
        self.dirty = true;
    }

    fn handle_exact_evidence_io_event(&mut self, event: ExactEvidenceIoEvent) {
        let (request_id, event_target) = match &event {
            ExactEvidenceIoEvent::Completed {
                request_id,
                response,
            } => (*request_id, &response.target),
            ExactEvidenceIoEvent::Failed {
                request_id, target, ..
            } => (*request_id, target),
        };
        let Some((pending_id, pending_target)) = self.pending_exact_evidence.as_ref() else {
            return;
        };
        if *pending_id != request_id || pending_target != event_target {
            return;
        }
        let still_selected = self.focus == PaneFocus::RunHistory
            && match pending_target {
                ExactEvidenceTarget::Incident(target) => {
                    self.current()
                        .selected_monitor_incident()
                        .map(IncidentEvidenceTarget::from)
                        .as_ref()
                        == Some(target)
                }
                ExactEvidenceTarget::Command(target) => self
                    .command_evidence_target(target.key, target.step_index)
                    .is_some_and(|current| target.same_selection(&current)),
            };
        let pending_target = pending_target.clone();
        self.pending_exact_evidence = None;
        if !still_selected {
            return;
        }
        match event {
            ExactEvidenceIoEvent::Completed { response, .. } => {
                let Some(inner) = self.layout.map(|layout| layout.output_inner) else {
                    self.current_mut().follow();
                    self.status = match pending_target {
                        ExactEvidenceTarget::Incident(_) => {
                            tr("st.monitor.jump.query.unavailable").into()
                        }
                        ExactEvidenceTarget::Command(_) => {
                            tr("st.run.jump.query.unavailable").into()
                        }
                    };
                    self.dirty = true;
                    return;
                };
                let snapshot = match &response.target {
                    ExactEvidenceTarget::Incident(target) => incident_evidence_snapshot(
                        self.detailed_timeline,
                        target,
                        &response.events,
                        inner.width,
                    ),
                    ExactEvidenceTarget::Command(target) => command_evidence_snapshot(
                        self.detailed_timeline,
                        target,
                        &response.events,
                        inner.width,
                    ),
                };
                let Some(snapshot) = snapshot else {
                    self.current_mut().follow();
                    self.status = match &pending_target {
                        ExactEvidenceTarget::Incident(target) => trf(
                            "st.monitor.jump.incomplete",
                            &[&target.seq_start.to_string(), &target.seq_end.to_string()],
                        ),
                        ExactEvidenceTarget::Command(target) => trf(
                            "st.run.jump.incomplete",
                            &[
                                &target.seq_start.to_string(),
                                &target.query_end_seq.to_string(),
                            ],
                        ),
                    };
                    self.dirty = true;
                    return;
                };
                let scroll_from_bottom = snapshot
                    .rows
                    .len()
                    .saturating_sub(usize::from(inner.height).max(1));
                let view = self.current_mut();
                view.scroll_snapshot = Some(snapshot);
                view.scroll_from_bottom = scroll_from_bottom;
                view.unseen = 0;
                self.status = match pending_target {
                    ExactEvidenceTarget::Incident(target) => trf(
                        "st.monitor.jump.journal",
                        &[&target.seq_start.to_string(), &target.seq_end.to_string()],
                    ),
                    ExactEvidenceTarget::Command(target) => {
                        trf("st.run.jump.journal", &[&target.seq_start.to_string()])
                    }
                };
            }
            ExactEvidenceIoEvent::Failed { failure, .. } => {
                self.current_mut().follow();
                self.status = match (&pending_target, failure) {
                    (ExactEvidenceTarget::Incident(target), ExactEvidenceFailure::Gap(gap)) => trf(
                        "st.monitor.jump.gap",
                        &[
                            &target.seq_start.to_string(),
                            &target.seq_end.to_string(),
                            &gap.first_seq.to_string(),
                            &gap.last_seq.to_string(),
                            gap_reason_label(gap.reason),
                        ],
                    ),
                    (ExactEvidenceTarget::Incident(target), ExactEvidenceFailure::Incomplete) => {
                        trf(
                            "st.monitor.jump.incomplete",
                            &[&target.seq_start.to_string(), &target.seq_end.to_string()],
                        )
                    }
                    (
                        ExactEvidenceTarget::Incident(target),
                        ExactEvidenceFailure::LimitExceeded,
                    ) => trf(
                        "st.monitor.jump.limit",
                        &[&target.seq_start.to_string(), &target.seq_end.to_string()],
                    ),
                    (
                        ExactEvidenceTarget::Incident(_),
                        ExactEvidenceFailure::QueryFailed(message),
                    ) => trf("st.monitor.jump.query.failed", &[&safe_inline(&message)]),
                    (ExactEvidenceTarget::Command(target), ExactEvidenceFailure::Gap(gap)) => trf(
                        "st.run.jump.gap",
                        &[
                            &target.seq_start.to_string(),
                            &gap.first_seq.to_string(),
                            &gap.last_seq.to_string(),
                            gap_reason_label(gap.reason),
                        ],
                    ),
                    (ExactEvidenceTarget::Command(target), ExactEvidenceFailure::Incomplete) => {
                        trf(
                            "st.run.jump.incomplete",
                            &[
                                &target.seq_start.to_string(),
                                &target.query_end_seq.to_string(),
                            ],
                        )
                    }
                    (ExactEvidenceTarget::Command(target), ExactEvidenceFailure::LimitExceeded) => {
                        trf("st.run.jump.limit", &[&target.seq_start.to_string()])
                    }
                    (
                        ExactEvidenceTarget::Command(_),
                        ExactEvidenceFailure::QueryFailed(message),
                    ) => trf("st.run.jump.query.failed", &[&safe_inline(&message)]),
                };
            }
        }
        self.dirty = true;
    }

    fn sync_status_notice(&mut self, now: Instant) {
        if self.status_notice_source != self.status {
            self.status_notice_source.clone_from(&self.status);
            self.status_notice_until =
                (!self.status.is_empty()).then_some(now + STATUS_NOTICE_DURATION);
        }
    }

    fn expire_status_notice(&mut self, now: Instant) -> bool {
        if self
            .status_notice_until
            .is_some_and(|deadline| now >= deadline)
        {
            self.status_notice_until = None;
            true
        } else {
            false
        }
    }

    fn active_status_notice(&self, now: Instant) -> Option<&str> {
        self.status_notice_until
            .is_some_and(|deadline| now < deadline)
            .then_some(self.status_notice_source.as_str())
    }

    fn current_mode(&self) -> InputMode {
        self.current().mode
    }

    fn select(&mut self, index: usize) {
        if index < self.ports.len() {
            self.clear_text_selection();
            self.queue_selection = None;
            self.selected = index;
            self.current_mut().unseen = 0;
            let name = self.current().snapshot.config.port.clone();
            let port = self.current().snapshot.config.port.clone();
            self.status = trf("st.viewing", &[&name, &port]);
            self.dirty = true;
        }
    }

    fn handle_network(&mut self, event: NetworkEvent, commands: &mpsc::Sender<NetworkCommand>) {
        match event {
            NetworkEvent::TransportConnected { generation } => {
                self.transport_connected = true;
                self.hello_accepted = false;
                self.connection_generation = Some(generation);
                self.actor = None;
                for slot in &mut self.ports {
                    slot.subscription = SubscriptionPhase::Attaching;
                }
                self.status = tr("st.transport").into();
            }
            NetworkEvent::Disconnected { reason } => {
                let old_actor_id = self.actor.take().map(|actor| actor.id);
                let newly_uncertain = self
                    .pending_requests
                    .values()
                    .filter(|request| matches!(request, PendingRequest::Write { .. }))
                    .count();
                self.uncertain_write_outcomes = self
                    .uncertain_write_outcomes
                    .saturating_add(newly_uncertain);
                self.transport_connected = false;
                self.hello_accepted = false;
                self.connection_generation = None;
                self.pending_requests.clear();
                self.pending_writes.clear();
                self.inflight_writes.clear();
                self.queued_controls.clear();
                self.pending_paste = None;
                for slot in &mut self.ports {
                    slot.last_manual_activity = None;
                    if old_actor_id.as_ref().is_some_and(|actor_id| {
                        slot.snapshot
                            .control
                            .as_ref()
                            .is_some_and(|lease| &lease.owner.id == actor_id)
                    }) {
                        slot.snapshot.control = None;
                    }
                    if !matches!(slot.subscription, SubscriptionPhase::Lagged { .. }) {
                        slot.subscription = SubscriptionPhase::Disconnected;
                    }
                }
                self.status = if newly_uncertain == 0 {
                    trf("st.disconnected", &[&reason])
                } else {
                    trf(
                        "st.disconnected.uncertain",
                        &[&reason, &newly_uncertain.to_string()],
                    )
                };
            }
            NetworkEvent::SendRejected { reason } => {
                self.status = reason;
            }
            NetworkEvent::Frame(frame) => self.handle_frame(*frame, commands),
        }
        self.normalize_queue_selection();
        self.dirty = true;
    }

    fn handle_frame(&mut self, frame: WireFrame, commands: &mpsc::Sender<NetworkCommand>) {
        match frame {
            WireFrame::Rx(header, data) | WireFrame::Tx(header, data) => {
                let replay = header.replay;
                self.push_event(header.into_event(data), replay, commands);
            }
            WireFrame::Control(message) => self.handle_server_message(message, commands),
        }
    }

    fn handle_server_message(
        &mut self,
        message: ServerMessage,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        match message {
            ServerMessage::Welcome {
                actor,
                protocol_version,
                ..
            } => {
                self.actor = Some(actor);
                self.hello_accepted = true;
                self.status = trf("st.welcome", &[&protocol_version.to_string()]);
            }
            ServerMessage::Snapshot { port: slot } => {
                if let Some(index) = self
                    .ports
                    .iter()
                    .position(|view| view.snapshot.config.port == slot.config.port)
                {
                    let epoch_changed =
                        self.ports[index].snapshot.daemon_epoch != slot.daemon_epoch;
                    let generation_changed =
                        self.ports[index].snapshot.generation != slot.generation;
                    if epoch_changed || generation_changed {
                        self.invalidate_slot_pending(
                            &slot.config.port,
                            tr("st.session.changed.unsent"),
                        );
                        self.ports[index].reset_stream();
                        self.ports[index].local_contiguous_from_seq = None;
                    }
                    if epoch_changed {
                        self.ports[index].clear_run_history();
                    }
                    self.ports[index].snapshot = *slot;
                    self.ports[index].sync_trigger_projection(false);
                    self.ports[index].sync_active_run_history();
                    self.ports[index].subscription = SubscriptionPhase::Attaching;
                    if epoch_changed {
                        let selected = self.selected == index;
                        let seq = self.ports[index].snapshot.head_seq;
                        self.ports[index].push_gap(seq, tr("st.daemon.restarted"), selected);
                        self.ports[index].last_epoch =
                            Some(self.ports[index].snapshot.daemon_epoch);
                        self.ports[index].last_seq = 0;
                    }
                }
            }
            ServerMessage::Timeline { event, replay } => self.push_event(event, replay, commands),
            ServerMessage::Result { request_id, result } => {
                self.handle_result(request_id, result, commands)
            }
            ServerMessage::Error {
                request_id,
                code,
                message,
                retryable,
            } => {
                let mut discarded_suffix = String::new();
                let mut cooperative_slot = None;
                if let Some(request_id) = request_id {
                    match self.pending_requests.remove(&request_id) {
                        Some(PendingRequest::Acquire { port, .. })
                        | Some(PendingRequest::Write {
                            port,
                            cooperative: false,
                            ..
                        }) => {
                            self.queued_controls.remove(&port);
                            let discarded = self
                                .pending_writes
                                .remove(&port)
                                .map_or(0, |writes| writes.len());
                            self.inflight_writes.remove(&port);
                            if discarded > 0 {
                                discarded_suffix =
                                    trf("st.discarded.chunks", &[&port, &discarded.to_string()]);
                            }
                        }
                        Some(PendingRequest::Write {
                            port,
                            cooperative: true,
                            ..
                        }) => {
                            // Cooperative input never owns the queued Human
                            // suffix or its acquire request. A rejection (for
                            // example an Agent lease expiring at the boundary)
                            // ends only this one opportunistic write.
                            cooperative_slot = Some(port);
                        }
                        _ => {}
                    }
                }
                self.status = format!(
                    "{}：{}{discarded_suffix}{}",
                    error_code_label(code),
                    safe_inline(&message),
                    if retryable { tr("st.retryable") } else { "" }
                );
                if let Some(port) = cooperative_slot {
                    // A queue-mode acquire can be granted while the
                    // cooperative request is still in flight. That grant
                    // deliberately waits behind all writes; once this request
                    // is rejected, resume the untouched ordinary queue if the
                    // Human now owns the lease.
                    self.flush_pending_writes(&port, commands);
                }
            }
            ServerMessage::Gap {
                port,
                requested_after_seq,
                first_available_seq,
                head_seq,
                reason,
            } => {
                self.push_gap(
                    &port,
                    head_seq,
                    trf(
                        "st.history.gap",
                        &[
                            gap_reason_label(reason),
                            &optional_sequence_label(requested_after_seq),
                            &optional_sequence_label(first_available_seq),
                        ],
                    ),
                );
            }
            ServerMessage::Lagged {
                port,
                from_seq,
                to_seq,
            } => {
                if let Some(index) = self.slot_index(&port) {
                    self.ports[index].subscription = SubscriptionPhase::Lagged { from_seq, to_seq };
                }
                self.push_gap(
                    &port,
                    to_seq,
                    trf("st.lagged", &[&from_seq.to_string(), &to_seq.to_string()]),
                );
            }
            ServerMessage::ReplayBegin {
                port,
                from_seq,
                through_seq,
            } => {
                if let Some(index) = self.slot_index(&port) {
                    self.ports[index].subscription = SubscriptionPhase::Replaying {
                        from_seq,
                        through_seq,
                    };
                }
                self.status = trf(
                    "st.replaying",
                    &[&port, &from_seq.to_string(), &through_seq.to_string()],
                );
            }
            ServerMessage::Ready { port, head_seq } => {
                if let Some(index) = self.slot_index(&port) {
                    self.ports[index].subscription = SubscriptionPhase::Ready { head_seq };
                    if self.owns_control(index) {
                        self.flush_pending_writes(&port, commands);
                    }
                }
                self.status = trf("st.live", &[&port, &head_seq.to_string()]);
            }
        }
    }

    fn handle_result(
        &mut self,
        request_id: Uuid,
        result: CommandResult,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        let pending = self.pending_requests.remove(&request_id);
        match result {
            CommandResult::ControlGranted { lease } => {
                if let Some(PendingRequest::Acquire { port, mode }) = pending {
                    self.queued_controls.remove(&port);
                    self.install_lease(&port, lease);
                    self.status = match mode {
                        ControlMode::Queue => trf("st.granted", &[&port]),
                        ControlMode::Takeover => trf("st.takeover.granted", &[&port]),
                    };
                    self.flush_pending_writes(&port, commands);
                }
            }
            CommandResult::ControlQueued { position } => {
                if let Some(PendingRequest::Acquire { port, mode }) = pending {
                    self.queued_controls.insert(
                        port.clone(),
                        QueuedControl {
                            _position: position,
                            since: Instant::now(),
                        },
                    );
                    self.pending_requests
                        .insert(request_id, PendingRequest::Acquire { port, mode });
                }
                self.status = trf("st.queued", &[&position.to_string()]);
            }
            CommandResult::ControlRenewed { lease } => {
                if let Some(PendingRequest::Renew { port }) = pending {
                    self.install_lease(&port, lease);
                }
            }
            CommandResult::ControlReleased => {
                if let Some(PendingRequest::Release { port }) = pending {
                    if let Some(index) = self.slot_index(&port) {
                        self.ports[index].snapshot.control = None;
                        self.ports[index].last_manual_activity = None;
                    }
                    self.status = trf("st.released", &[&port]);
                }
            }
            CommandResult::AcquireCancelled { removed } => {
                if let Some(
                    PendingRequest::Acquire { port, .. } | PendingRequest::CancelAcquire { port },
                ) = pending
                {
                    self.queued_controls.remove(&port);
                    self.status = trf("st.acquire.cancelled", &[&port]);
                    // The queued waiter can be promoted just before its
                    // directed cancellation is processed. If that happened,
                    // release the now-idle Human lease immediately instead of
                    // holding it until the idle timer expires.
                    if !removed
                        && self
                            .slot_index(&port)
                            .is_some_and(|index| self.owns_control(index))
                        && !self.pending_writes.contains_key(&port)
                        && let Some(index) = self.slot_index(&port)
                        && let Some(lease) = self.ports[index].snapshot.control.clone()
                    {
                        self.release_slot_control(commands, port, lease, false);
                    }
                }
            }
            CommandResult::WriteAccepted { event_seq } => {
                if let Some(PendingRequest::Write {
                    port, cooperative, ..
                }) = pending
                {
                    self.status = trf("st.write.confirmed", &[&port, &event_seq.to_string()]);
                    if !cooperative {
                        self.acknowledge_inflight_write(&port);
                    }
                    self.flush_pending_writes(&port, commands);
                }
            }
            CommandResult::BreakSent { event_seq } => {
                self.status = trf("st.break.confirmed", &[&event_seq.to_string()]);
            }
            CommandResult::TriggerStarted { trigger }
            | CommandResult::TriggerStatus { trigger }
            | CommandResult::TriggerCancelled { trigger } => {
                self.status = trf(
                    "st.trigger.result",
                    &[
                        &trigger.id.to_string(),
                        trigger_status_label(trigger.status),
                        &trigger.fires_confirmed.to_string(),
                    ],
                );
            }
            CommandResult::HelloAccepted { actor } => {
                self.actor = Some(actor);
                self.hello_accepted = true;
                self.status = tr("st.session.ready").into();
            }
            CommandResult::Attached { ports } => {
                self.status = trf("st.watching", &[&ports.len().to_string()]);
            }
            CommandResult::Detached { ports } => {
                self.status = trf("st.detached", &[&ports.len().to_string()]);
            }
            CommandResult::Pong { .. } => {}
            CommandResult::RunStarted { run } => {
                self.status = trf("st.run.started", &[&run.label]);
            }
            CommandResult::RunEnded { run } => {
                self.status = trf("st.run.ended", &[&run.label]);
            }
            CommandResult::CheckpointCreated { event_seq } => {
                self.status = trf("st.checkpoint", &[&event_seq.to_string()]);
            }
        }
    }

    fn push_event(
        &mut self,
        event: TimelineEvent,
        replay: bool,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        if let Some(index) = self.slot_index(&event.port) {
            let port = event.port.clone();
            let selected = index == self.selected;
            if replay {
                self.ports[index].push_event(event, selected);
                return;
            }

            let generation_changed = self.ports[index].snapshot.generation != event.generation;
            let declared_profile_only = event
                .metadata
                .get("profile_only")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let unchanged_config = event
                .metadata
                .get("current")
                .and_then(|value| {
                    serde_json::from_value::<serial_protocol::SlotConfig>(value.clone()).ok()
                })
                .is_some_and(|current| current == self.ports[index].snapshot.config);
            let profile_only = event.kind == EventKind::PortReconfigured
                && declared_profile_only
                && unchanged_config;
            let physical_reconfiguration =
                event.kind == EventKind::PortReconfigured && !profile_only;
            if generation_changed
                || matches!(event.kind, EventKind::SerialClosed | EventKind::PortRemoved)
                || physical_reconfiguration
            {
                self.invalidate_slot_pending(&port, tr("st.session.changed.discarded"));
                self.ports[index].snapshot.active_trigger = None;
                self.ports[index].clear_trigger_projection();
            }
            self.apply_event_projection(index, &event);
            if event.kind == EventKind::RunAborted {
                let label = event
                    .metadata
                    .get("run")
                    .and_then(|value| value.get("label"))
                    .and_then(serde_json::Value::as_str)
                    .map(safe_inline)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| tr("menu.value.unbound").into());
                let reason = event
                    .metadata
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .map(safe_inline)
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| tr("menu.value.unbound").into());
                self.status = trf("st.run.aborted", &[&label, &reason]);
            }
            self.ports[index].push_event(event, selected);
            if self.ports[index].subscription.is_ready() && self.owns_control(index) {
                self.queued_controls.remove(&port);
                self.pending_requests.retain(|_, request| {
                    !matches!(request, PendingRequest::Acquire { port: pending, .. } if pending == &port)
                });
                self.flush_pending_writes(&port, commands);
            }
        }
    }

    fn apply_event_projection(&mut self, index: usize, event: &TimelineEvent) {
        let slot = &mut self.ports[index];
        let snapshot = &mut slot.snapshot;
        snapshot.head_seq = snapshot.head_seq.max(event.seq);
        snapshot.generation = event.generation;
        if let Some(end) = event.stream_offset_end {
            match event.direction {
                serial_protocol::Direction::Rx => snapshot.rx_offset = end,
                serial_protocol::Direction::Tx => snapshot.tx_offset = end,
                serial_protocol::Direction::None => {}
            }
        }
        match event.kind {
            EventKind::Rx => {
                snapshot.target_activity = TargetActivity::Active;
                snapshot.last_rx_wall_time_ns = Some(event.wall_time_ns);
                slot.observe_trigger_rx(&event.data);
            }
            EventKind::SerialOpening => snapshot.session_state = SessionState::Opening,
            EventKind::SerialOpened => {
                snapshot.endpoint_present = true;
                snapshot.session_state = SessionState::Online;
                snapshot.state_reason = None;
                snapshot.target_activity = TargetActivity::Unknown;
            }
            EventKind::SerialOpenFailed | EventKind::SerialClosed => {
                snapshot.session_state = SessionState::Backoff;
                snapshot.target_activity = TargetActivity::Unknown;
                snapshot.state_reason = event
                    .metadata
                    .get("error")
                    .or_else(|| event.metadata.get("reason"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
            }
            EventKind::ControlGranted => {
                if let Some(lease) = event
                    .metadata
                    .get("lease")
                    .and_then(|value| serde_json::from_value::<ControlLease>(value.clone()).ok())
                {
                    snapshot.control = Some(lease);
                }
            }
            EventKind::ControlReleased | EventKind::ControlRevoked | EventKind::ControlExpired => {
                snapshot.control = None;
                slot.mark_trigger_stopping();
            }
            EventKind::RunStarted => {
                snapshot.active_run = event
                    .metadata
                    .get("run")
                    .and_then(|value| serde_json::from_value::<RunInfo>(value.clone()).ok());
            }
            EventKind::RunEnded | EventKind::RunAborted => {
                snapshot.active_run = None;
                slot.mark_trigger_stopping();
            }
            EventKind::TriggerStarted => {
                snapshot.active_trigger = event
                    .metadata
                    .get("trigger")
                    .and_then(|value| serde_json::from_value::<TriggerInfo>(value.clone()).ok());
                slot.sync_trigger_projection(true);
            }
            EventKind::TriggerCompleted
            | EventKind::TriggerCancelled
            | EventKind::TriggerFailed => {
                snapshot.active_trigger = None;
                slot.clear_trigger_projection();
            }
            EventKind::LoggingDegraded => {
                snapshot.logging = LoggingState::Degraded;
            }
            EventKind::Gap => slot.mark_trigger_stopping(),
            EventKind::PortReconfigured => {
                if let Some(config) = event
                    .metadata
                    .get("current")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                {
                    snapshot.config = config;
                }
                if let Some(effective) = event.metadata.get("effective").and_then(|value| {
                    serde_json::from_value::<ResolvedModelSettings>(value.clone()).ok()
                }) {
                    snapshot.effective_shell_prompt = effective.shell_prompt;
                    snapshot.effective_uboot_prompt = effective.uboot_prompt;
                    snapshot.effective_write_eol = Some(effective.write_eol);
                    snapshot.effective_echo = Some(effective.echo);
                    snapshot.effective_write_pacing = Some(effective.write_pacing);
                }
                if let Some(effective_transport) =
                    event.metadata.get("effective_transport").and_then(|value| {
                        serde_json::from_value::<ResolvedTransportSettings>(value.clone()).ok()
                    })
                {
                    snapshot.effective_transport = Some(effective_transport);
                }
            }
            EventKind::PortRemoved => {
                snapshot.endpoint_present = false;
                snapshot.session_state = SessionState::Disabled;
                snapshot.state_reason = Some(tr("state.removed").into());
                snapshot.target_activity = TargetActivity::Unknown;
                snapshot.control = None;
                snapshot.active_run = None;
                snapshot.active_trigger = None;
                slot.clear_trigger_projection();
            }
            EventKind::Tx => slot.observe_trigger_tx(event),
            EventKind::Break | EventKind::Checkpoint => {}
        }
    }

    fn push_gap(&mut self, port: &str, seq: u64, message: String) {
        if let Some(index) = self.slot_index(port) {
            let selected = index == self.selected;
            self.ports[index].push_gap(seq, message, selected);
        }
    }

    fn slot_index(&self, port: &str) -> Option<usize> {
        self.ports
            .iter()
            .position(|slot| slot.snapshot.config.port == port)
    }

    fn all_slots_ready(&self) -> bool {
        !self.ports.is_empty() && self.ports.iter().all(|slot| slot.subscription.is_ready())
    }

    fn slot_ready(&self, index: usize) -> bool {
        self.ports[index].subscription.is_ready()
    }

    fn invalidate_slot_pending(&mut self, port: &str, reason: &str) {
        let discarded_writes = self
            .pending_writes
            .remove(port)
            .map_or(0, |writes| writes.len());
        self.inflight_writes.remove(port);
        let before = self.pending_requests.len();
        self.pending_requests
            .retain(|_, request| request.port() != port);
        self.queued_controls.remove(port);
        let discarded_requests = before.saturating_sub(self.pending_requests.len());
        if self
            .pending_paste
            .as_ref()
            .is_some_and(|paste| paste.port == port)
        {
            self.pending_paste = None;
        }
        if discarded_writes > 0 || discarded_requests > 0 {
            self.status = trf(
                "st.invalidated",
                &[
                    port,
                    reason,
                    &discarded_writes.to_string(),
                    &discarded_requests.to_string(),
                ],
            );
        }
    }

    fn owns_control(&self, index: usize) -> bool {
        let Some(actor) = &self.actor else {
            return false;
        };
        self.ports[index]
            .snapshot
            .control
            .as_ref()
            .is_some_and(|lease| lease.owner.id == actor.id)
    }

    fn install_lease(&mut self, port: &str, lease: ControlLease) {
        self.queued_controls.remove(port);
        if let Some(index) = self.slot_index(port) {
            self.ports[index].snapshot.control = Some(lease);
        }
    }

    fn send_message(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        message: ClientMessage,
        pending: Option<PendingRequest>,
    ) -> bool {
        if !self.transport_connected || !self.hello_accepted {
            self.status = tr("st.not.ready.queued").into();
            return false;
        }
        let Some(generation) = self.connection_generation else {
            self.status = tr("st.not.connected").into();
            return false;
        };
        let request_id = message.request_id();
        if pending.is_some() && self.pending_requests.len() >= MAX_OUTSTANDING_REQUESTS {
            self.status = tr("st.too.many").into();
            return false;
        }
        match commands.try_send(NetworkCommand::Send {
            generation,
            message,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.status = tr("st.outbound.full").into();
                return false;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.status = tr("st.network.stopped").into();
                return false;
            }
        }
        if let Some(pending) = pending {
            self.pending_requests.insert(request_id, pending);
        }
        true
    }

    fn request_write(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
    ) -> bool {
        self.request_write_batch_with_kind(
            commands,
            vec![data],
            operation_id,
            PendingWriteKind::Line,
        )
    }

    fn request_raw_write(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        data: Vec<u8>,
    ) -> bool {
        self.request_write_batch_with_kind(commands, vec![data], None, PendingWriteKind::Raw)
    }

    /// Sends an explicit Human/Agent cooperative write without acquiring or
    /// taking over the Agent's control lease. The daemon independently checks
    /// the same lease/Run relationship; this local gate keeps an accidental
    /// Alt+Enter from becoming an opaque rejected request.
    fn request_cooperative_write(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
    ) -> bool {
        if !self.transport_connected || !self.hello_accepted {
            self.status = tr("st.not.ready").into();
            return false;
        }
        if !self.slot_ready(self.selected) {
            self.status = trf("st.not.live", &[&self.selected_port()]);
            return false;
        }
        let human = self
            .actor
            .as_ref()
            .is_some_and(|actor| actor.kind == serial_protocol::ActorKind::Human);
        let matching_run_id = self
            .current()
            .snapshot
            .control
            .as_ref()
            .zip(self.current().active_agent_run())
            .and_then(|(lease, run)| {
                (lease.owner.kind == serial_protocol::ActorKind::Agent
                    && lease.owner.id == run.owner.id)
                    .then_some(run.id)
            });
        let Some(expected_run_id) = matching_run_id.filter(|_| human) else {
            self.status = tr("st.cooperative.unavailable").into();
            return false;
        };

        let port = self.selected_port();
        let sent = self.send_message(
            commands,
            ClientMessage::Write {
                request_id: Uuid::new_v4(),
                port: port.clone(),
                control_id: Uuid::nil(),
                fence: 0,
                data,
                operation_id,
                // Bind this exceptional write to the exact Agent Run that
                // justified cooperation. The daemon rejects delayed/replayed
                // input after that Run ends or a successor begins.
                expected_run_id: Some(expected_run_id),
                pacing: None,
                description: None,
                command_sequence: None,
                command_capture_matchers: Vec::new(),
                sequence_precondition: None,
                cooperative: true,
            },
            Some(PendingRequest::Write {
                port,
                operation_id,
                cooperative: true,
            }),
        );
        if sent {
            self.status = tr("st.cooperative.sent").into();
        }
        sent
    }

    fn request_write_batch(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        writes: Vec<Vec<u8>>,
    ) -> bool {
        let writes = writes
            .into_iter()
            .map(|write| (write, Some(Uuid::new_v4())))
            .collect();
        self.request_write_operations(commands, writes, PendingWriteKind::Line)
    }

    fn request_write_batch_with_kind(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        writes: Vec<Vec<u8>>,
        operation_id: Option<Uuid>,
        kind: PendingWriteKind,
    ) -> bool {
        self.request_write_operations(
            commands,
            writes
                .into_iter()
                .map(|write| (write, operation_id))
                .collect(),
            kind,
        )
    }

    fn request_write_operations(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        writes: Vec<(Vec<u8>, Option<Uuid>)>,
        kind: PendingWriteKind,
    ) -> bool {
        if writes.iter().all(|(write, _)| write.is_empty()) {
            return true;
        }
        if !self.transport_connected || !self.hello_accepted {
            self.status = tr("st.not.ready").into();
            return false;
        }
        if !self.slot_ready(self.selected) {
            self.status = trf("st.not.live", &[&self.selected_port()]);
            return false;
        }
        let port = self.selected_port();
        let total_new_bytes = writes.iter().fold(0usize, |total, (write, _)| {
            total.saturating_add(write.len())
        });
        let previous_slot_writes = self.pending_writes.get(&port).cloned().unwrap_or_default();
        let previous_slot_count = previous_slot_writes.len();
        let mut candidate_slot_writes = previous_slot_writes.clone();
        for (write, operation_id) in writes.iter().filter(|(write, _)| !write.is_empty()) {
            append_pending_write(&mut candidate_slot_writes, write, *operation_id, kind);
        }

        let total_pending = self
            .pending_writes
            .values()
            .map(VecDeque::len)
            .sum::<usize>();
        let total_bytes = self
            .pending_writes
            .values()
            .flat_map(|writes| writes.iter())
            .map(|write| write.data.len())
            .sum::<usize>();
        let candidate_total_pending = total_pending
            .saturating_sub(previous_slot_count)
            .saturating_add(candidate_slot_writes.len());
        let candidate_total_bytes = total_bytes.saturating_add(total_new_bytes);
        if candidate_total_pending > MAX_PENDING_WRITES || candidate_total_bytes > MAX_PENDING_BYTES
        {
            self.status = tr("st.writeq.full").into();
            return false;
        }
        self.pending_writes
            .insert(port.clone(), candidate_slot_writes);
        self.ports[self.selected].last_manual_activity = Some(Instant::now());

        if self.owns_control(self.selected) {
            let flushed = self.flush_pending_writes(&port, commands);
            // A saturated outbound channel leaves the complete operation in
            // the visible local queue. Treat that as accepted local enqueue so
            // Enter may clear the draft without risking a later duplicate.
            return flushed || self.pending_writes.contains_key(&port);
        }

        let acquire_already_pending = self.pending_requests.values().any(|request| {
            matches!(request, PendingRequest::Acquire { port: pending, .. } if pending == &port)
        });
        if !acquire_already_pending && !self.acquire_control(commands, ControlMode::Queue) {
            if previous_slot_writes.is_empty() {
                self.pending_writes.remove(&port);
            } else {
                self.pending_writes
                    .insert(port.clone(), previous_slot_writes);
            }
            return false;
        }
        true
    }

    fn acquire_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        mode: ControlMode,
    ) -> bool {
        if !self.transport_connected || !self.hello_accepted || !self.slot_ready(self.selected) {
            self.status = tr("st.not.ready.live").into();
            return false;
        }
        let port = self.selected_port();
        let message = ClientMessage::AcquireControl {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            mode,
            ttl_ms: CONTROL_TTL_MS,
        };
        if self.send_message(
            commands,
            message,
            Some(PendingRequest::Acquire {
                port: port.clone(),
                mode,
            }),
        ) {
            if mode == ControlMode::Takeover {
                self.ports[self.selected].last_manual_activity = Some(Instant::now());
            }
            self.status = match mode {
                ControlMode::Queue => trf("st.requesting.control", &[&port]),
                ControlMode::Takeover => trf("st.requesting.takeover", &[&port]),
            };
            true
        } else {
            false
        }
    }

    fn release_control(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        if !self.transport_connected || !self.hello_accepted || !self.slot_ready(self.selected) {
            self.status = tr("st.port.not.live").into();
            return;
        }
        let port = self.selected_port();
        if !self.owns_control(self.selected) && self.has_queued_control(&port) {
            self.cancel_queued_control(commands, &port, tr("st.cancel.reason"));
            return;
        }
        let Some(lease) = self.current().snapshot.control.clone() else {
            self.status = tr("st.no.control").into();
            return;
        };
        if !self.owns_control(self.selected) {
            self.status = trf("st.control.belongs", &[&lease.owner.label]);
            return;
        }
        self.pending_writes.remove(&port);
        self.inflight_writes.remove(&port);
        self.release_slot_control(commands, port, lease, false);
    }

    fn remove_last_queued_line(
        &mut self,
        restore_to_editor: bool,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        let port = self.selected_port();
        let count = self
            .pending_writes
            .get(&port)
            .map_or(0, |queue| queued_line_operations(queue).len());
        if count == 0 {
            self.status = if self
                .pending_writes
                .get(&port)
                .is_some_and(|queue| !queue.is_empty())
            {
                tr("st.queue.raw.only").into()
            } else {
                tr("st.queue.none").into()
            };
            return;
        }
        self.remove_queued_line_operation(count - 1, restore_to_editor, commands);
    }

    fn remove_queued_line_operation(
        &mut self,
        operation_index: usize,
        restore_to_editor: bool,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        let port = self.selected_port();
        let Some(operation) = self.pending_writes.get(&port).and_then(|queue| {
            queued_line_operations(queue)
                .into_iter()
                .nth(operation_index)
        }) else {
            self.status = tr("st.queue.none").into();
            return;
        };

        let sending = self.pending_requests.values().any(|request| match request {
            PendingRequest::Write {
                port: pending_slot,
                operation_id,
                ..
            } if pending_slot == &port => {
                operation.operation_id.is_none() || operation.operation_id == *operation_id
            }
            _ => false,
        });
        if sending {
            self.status = tr("st.queue.already.sending").into();
            return;
        }

        let Some(mut bytes) = self
            .pending_writes
            .get_mut(&port)
            .and_then(|queue| take_queued_line_operation(queue, operation_index))
            .map(|operation| operation.data)
        else {
            self.status = tr("st.queue.none").into();
            return;
        };
        let queue_empty = self
            .pending_writes
            .get(&port)
            .is_some_and(VecDeque::is_empty);
        if queue_empty {
            self.pending_writes.remove(&port);
        }
        if restore_to_editor {
            let eol = self.current().effective_write_eol().as_bytes().to_vec();
            if !eol.is_empty() && bytes.ends_with(&eol) {
                bytes.truncate(bytes.len() - eol.len());
            }
            let text = String::from_utf8_lossy(&bytes);
            let view = self.current_mut();
            view.mode = InputMode::Line;
            view.draft = text.chars().collect();
            view.draft_cursor = view.draft.len();
            view.history_search = None;
            view.completion = None;
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
            self.status = tr("st.queue.restored").into();
        } else {
            self.status = tr("st.queue.deleted").into();
            self.normalize_queue_selection();
        }
        if !self.pending_writes.contains_key(&port)
            && !self.owns_control(self.selected)
            && (self.queued_controls.contains_key(&port)
                || self.pending_requests.values().any(
                    |request| matches!(request, PendingRequest::Acquire { port: pending, .. } if pending == &port),
                ))
        {
            self.cancel_queued_control(commands, &port, tr("st.cancel.reason"));
        }
    }

    fn open_queue_selection(&mut self) {
        let port = self.selected_port();
        let count = self
            .pending_writes
            .get(&port)
            .map_or(0, |queue| queued_line_operations(queue).len());
        if count == 0 {
            self.status = tr("st.queue.none").into();
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
            return;
        }
        self.queue_selection = Some(QueueSelection {
            port,
            selected: 0,
            detail_scroll: 0,
        });
        self.focus = PaneFocus::Queue;
        self.status = tr("st.queue.select").into();
    }

    fn normalize_queue_selection(&mut self) {
        let Some(selection) = self.queue_selection.as_ref() else {
            return;
        };
        let count = self
            .pending_writes
            .get(&selection.port)
            .map_or(0, |queue| queued_line_operations(queue).len());
        if count == 0 {
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
        } else if let Some(selection) = self.queue_selection.as_mut() {
            selection.selected = selection.selected.min(count - 1);
        }
    }

    fn handle_queue_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        self.normalize_queue_selection();
        let Some(selection) = self.queue_selection.as_ref() else {
            return;
        };
        let port = selection.port.clone();
        let selected = selection.selected;
        let count = self
            .pending_writes
            .get(&port)
            .map_or(0, |queue| queued_line_operations(queue).len());
        match key.code {
            KeyCode::Up => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.selected = selected.saturating_sub(1);
                    selection.detail_scroll = 0;
                }
            }
            KeyCode::Down => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.selected = (selected + 1).min(count - 1);
                    selection.detail_scroll = 0;
                }
            }
            KeyCode::Home => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.selected = 0;
                    selection.detail_scroll = 0;
                }
            }
            KeyCode::End => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.selected = count - 1;
                    selection.detail_scroll = 0;
                }
            }
            KeyCode::PageUp => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.detail_scroll = selection.detail_scroll.saturating_sub(5);
                }
            }
            KeyCode::PageDown => {
                if let Some(selection) = self.queue_selection.as_mut() {
                    selection.detail_scroll = selection.detail_scroll.saturating_add(5);
                }
            }
            KeyCode::Char('d' | 'D') => {
                self.remove_queued_line_operation(selected, false, commands)
            }
            KeyCode::Char('e' | 'E') => self.remove_queued_line_operation(selected, true, commands),
            KeyCode::Esc | KeyCode::Enter | KeyCode::Char('u' | 'U') => {
                self.queue_selection = None;
                self.focus = PaneFocus::Input;
                self.status = tr("st.queue.select.closed").into();
            }
            _ => {}
        }
        self.normalize_queue_selection();
    }

    fn toggle_run_history_panel(&mut self) {
        if !self.run_panel_visible {
            self.run_panel_visible = true;
            self.focus = PaneFocus::RunHistory;
            self.status = tr("st.run.panel.focused").into();
        } else if self.focus == PaneFocus::RunHistory {
            self.run_panel_visible = false;
            self.focus = PaneFocus::Input;
            self.status = tr("st.run.panel.hidden").into();
        } else {
            self.focus = PaneFocus::RunHistory;
            self.status = tr("st.run.panel.focused").into();
        }
    }

    fn handle_run_history_key(&mut self, key: KeyEvent) {
        let count = self.current().history_action_keys().len();
        let selected = self
            .current()
            .selected_history_action_index()
            .unwrap_or_else(|| count.saturating_sub(1));
        let mut jump_to_selection = false;
        if let (Some(monitor_id), Some(matcher), Some(incident_id)) = (
            self.current().selected_monitor,
            self.current().selected_monitor_matcher,
            self.current().selected_monitor_incident,
        ) {
            let incidents = self.current().monitor_incident_ids(monitor_id, matcher);
            let selected_incident = incidents
                .iter()
                .position(|id| *id == incident_id)
                .unwrap_or_else(|| incidents.len().saturating_sub(1));
            match key.code {
                KeyCode::Up if !incidents.is_empty() => {
                    self.current_mut().selected_monitor_incident =
                        incidents.get(selected_incident.saturating_sub(1)).copied();
                    jump_to_selection = true;
                }
                KeyCode::Down if !incidents.is_empty() => {
                    self.current_mut().selected_monitor_incident = incidents
                        .get((selected_incident + 1).min(incidents.len() - 1))
                        .copied();
                    jump_to_selection = true;
                }
                KeyCode::Home if !incidents.is_empty() => {
                    self.current_mut().selected_monitor_incident = incidents.first().copied();
                    jump_to_selection = true;
                }
                KeyCode::End if !incidents.is_empty() => {
                    self.current_mut().selected_monitor_incident = incidents.last().copied();
                    jump_to_selection = true;
                }
                KeyCode::Right | KeyCode::Enter => jump_to_selection = true,
                KeyCode::Left | KeyCode::Esc => {
                    self.current_mut().selected_monitor_incident = None;
                }
                _ => {}
            }
            if jump_to_selection {
                self.jump_output_to_monitor_incident();
            }
            return;
        }
        if let (Some(monitor_id), Some(matcher)) = (
            self.current().selected_monitor,
            self.current().selected_monitor_matcher,
        ) {
            let matcher_count = self
                .current()
                .monitor(monitor_id)
                .map_or(0, |entry| entry.monitor.spec.matchers.len());
            match key.code {
                KeyCode::Up if matcher_count > 0 => {
                    self.current_mut().selected_monitor_matcher = Some(matcher.saturating_sub(1));
                }
                KeyCode::Down if matcher_count > 0 => {
                    self.current_mut().selected_monitor_matcher =
                        Some((matcher + 1).min(matcher_count - 1));
                }
                KeyCode::Right | KeyCode::Enter => {
                    let incidents = self.current().monitor_incident_ids(monitor_id, matcher);
                    self.current_mut().selected_monitor_incident = incidents.last().copied();
                    if self.current().selected_monitor_incident.is_some() {
                        self.jump_output_to_monitor_incident();
                    }
                }
                KeyCode::Left | KeyCode::Esc => {
                    self.current_mut().selected_monitor_matcher = None;
                    self.current_mut().expanded_monitor = None;
                }
                _ => {}
            }
            return;
        }
        if let Some(step) = self.current().selected_run_step {
            let step_count = self
                .current()
                .selected_run_command_key()
                .and_then(|key| self.current().run_command(key))
                .map_or(0, |record| record.steps.len());
            match key.code {
                KeyCode::Up if step_count > 0 => {
                    self.current_mut().selected_run_step = Some(step.saturating_sub(1));
                    jump_to_selection = true;
                }
                KeyCode::Down if step_count > 0 => {
                    self.current_mut().selected_run_step = Some((step + 1).min(step_count - 1));
                    jump_to_selection = true;
                }
                KeyCode::Home if step_count > 0 => {
                    self.current_mut().selected_run_step = Some(0);
                    jump_to_selection = true;
                }
                KeyCode::End if step_count > 0 => {
                    self.current_mut().selected_run_step = Some(step_count - 1);
                    jump_to_selection = true;
                }
                KeyCode::Right | KeyCode::Enter => jump_to_selection = true,
                KeyCode::Left | KeyCode::Esc => {
                    self.current_mut().selected_run_step = None;
                    self.current_mut().run_detail_scroll = 0;
                }
                KeyCode::PageUp => {
                    let maximum = self.max_run_detail_scroll();
                    self.current_mut().run_detail_scroll = self
                        .current()
                        .run_detail_scroll
                        .min(maximum)
                        .saturating_sub(5);
                }
                KeyCode::PageDown => {
                    let maximum = self.max_run_detail_scroll();
                    self.current_mut().run_detail_scroll = self
                        .current()
                        .run_detail_scroll
                        .min(maximum)
                        .saturating_add(5)
                        .min(maximum);
                }
                _ => {}
            }
            if jump_to_selection && let Some(key) = self.current().selected_run_command_key() {
                self.jump_output_to_run_command(key, self.current().selected_run_step);
            }
            return;
        }
        match key.code {
            KeyCode::Up if count > 0 => {
                self.current_mut()
                    .select_history_action_index(selected.saturating_sub(1));
                jump_to_selection = matches!(
                    self.current().selected_history_action_key(),
                    Some(HistoryActionKey::Command(_))
                );
            }
            KeyCode::Down if count > 0 => {
                self.current_mut()
                    .select_history_action_index((selected + 1).min(count - 1));
                jump_to_selection = matches!(
                    self.current().selected_history_action_key(),
                    Some(HistoryActionKey::Command(_))
                );
            }
            KeyCode::Home if count > 0 => {
                self.current_mut().select_history_action_index(0);
                jump_to_selection = matches!(
                    self.current().selected_history_action_key(),
                    Some(HistoryActionKey::Command(_))
                );
            }
            KeyCode::End if count > 0 => {
                self.current_mut().select_history_action_index(count - 1);
                jump_to_selection = matches!(
                    self.current().selected_history_action_key(),
                    Some(HistoryActionKey::Command(_))
                );
            }
            KeyCode::Right if count > 0 => {
                match self.current().selected_history_action_key() {
                    Some(HistoryActionKey::Command(selected_key)) => {
                        let step_count = self
                            .current()
                            .run_command(selected_key)
                            .map_or(0, |record| record.steps.len());
                        let view = self.current_mut();
                        view.selected_run_command = Some(selected_key);
                        view.expanded_run_command = Some(selected_key);
                        view.selected_run_step = (step_count > 1).then_some(0);
                        jump_to_selection = true;
                    }
                    Some(HistoryActionKey::Monitor(id)) => {
                        let matcher_count = self
                            .current()
                            .monitor(id)
                            .map_or(0, |entry| entry.monitor.spec.matchers.len());
                        let view = self.current_mut();
                        view.selected_monitor = Some(id);
                        view.expanded_monitor = Some(id);
                        view.selected_monitor_matcher = (matcher_count > 0).then_some(0);
                    }
                    None => {}
                }
                self.current_mut().run_detail_scroll = 0;
            }
            KeyCode::Left if count > 0 => {
                self.current_mut().expanded_run_command = None;
                self.current_mut().selected_run_step = None;
                self.current_mut().expanded_monitor = None;
                self.current_mut().selected_monitor_matcher = None;
                self.current_mut().selected_monitor_incident = None;
                self.current_mut().run_detail_scroll = 0;
            }
            KeyCode::PageUp => {
                if self.current().expanded_run_command.is_some() {
                    let maximum = self.max_run_detail_scroll();
                    let scroll = self
                        .current()
                        .run_detail_scroll
                        .min(maximum)
                        .saturating_sub(5);
                    self.current_mut().run_detail_scroll = scroll;
                } else if count > 0 {
                    let page = usize::from(self.agent_history_rows.saturating_sub(1)).max(1);
                    self.current_mut()
                        .select_history_action_index(selected.saturating_sub(page));
                    jump_to_selection = matches!(
                        self.current().selected_history_action_key(),
                        Some(HistoryActionKey::Command(_))
                    );
                }
            }
            KeyCode::PageDown => {
                if self.current().expanded_run_command.is_some() {
                    let maximum = self.max_run_detail_scroll();
                    let scroll = self
                        .current()
                        .run_detail_scroll
                        .min(maximum)
                        .saturating_add(5)
                        .min(maximum);
                    self.current_mut().run_detail_scroll = scroll;
                } else if count > 0 {
                    let page = usize::from(self.agent_history_rows.saturating_sub(1)).max(1);
                    self.current_mut()
                        .select_history_action_index(selected.saturating_add(page).min(count - 1));
                    jump_to_selection = matches!(
                        self.current().selected_history_action_key(),
                        Some(HistoryActionKey::Command(_))
                    );
                }
            }
            KeyCode::Esc => {
                self.focus = PaneFocus::Input;
                self.current_mut().follow();
                self.status = tr("st.run.panel.left").into();
            }
            _ => {}
        }
        if jump_to_selection && let Some(key) = self.current().selected_run_command_key() {
            self.jump_output_to_run_command(key, self.current().selected_run_step);
        }
    }

    fn max_run_detail_scroll(&self) -> usize {
        let view = self.current();
        let selected = view.selected_run_command_key();
        if selected.is_none() || view.expanded_run_command != selected {
            return 0;
        }
        let Some(inner) = self.layout.and_then(|layout| layout.run_history_inner) else {
            return 0;
        };
        let height = usize::from(inner.height);
        if height == 0 || inner.width == 0 {
            return 0;
        }
        let rows = run_history_rows(self, inner.width);
        let selected_row = rows
            .iter()
            .position(|row| {
                row.command == selected
                    && match view.selected_run_step {
                        Some(step) => row.step == Some(step),
                        None => row.step.is_none(),
                    }
            })
            .unwrap_or(0);
        let max_start = rows.len().saturating_sub(height);
        max_start.saturating_sub(selected_row.saturating_sub(2).min(max_start))
    }

    fn clamp_run_detail_scroll(&mut self) {
        let maximum = self.max_run_detail_scroll();
        let scroll = self.current().run_detail_scroll.min(maximum);
        self.current_mut().run_detail_scroll = scroll;
    }

    fn jump_output_to_run_command(
        &mut self,
        key: RunCommandKey,
        step_index: Option<usize>,
    ) -> bool {
        let Some(target) = self.command_evidence_target(key, step_index) else {
            return false;
        };
        let Some(inner) = self.layout.map(|layout| layout.output_inner) else {
            return false;
        };
        let entries = self
            .current()
            .lines
            .iter()
            .chain(self.current().pending_line.iter())
            .collect::<Vec<_>>();
        if target.matchers.is_empty() {
            if local_command_window_is_retained(self.current(), &target, &entries) {
                let rows = all_output_visual_rows(self, inner.width);
                if let Some(target_index) = rows.iter().position(|row| {
                    row.daemon_epoch == Some(target.daemon_epoch) && row.seq >= target.seq_start
                }) {
                    let height = usize::from(inner.height).max(1);
                    let start = target_index.saturating_sub(height / 3);
                    let end = start.saturating_add(height).min(rows.len());
                    let scroll_from_bottom = rows.len().saturating_sub(end);
                    let snapshot = ScrollSnapshot {
                        rows: rows.into_iter().map(|row| row.line).collect(),
                    };
                    let view = self.current_mut();
                    view.scroll_snapshot = Some(snapshot);
                    view.scroll_from_bottom = scroll_from_bottom;
                    view.unseen = 0;
                    self.pending_exact_evidence = None;
                    self.status = trf("st.run.jump.overlay", &[&target.seq_start.to_string()]);
                    return true;
                }
            }
            let snapshot = ScrollSnapshot {
                rows: wrap_command_fallback_line(&target.command, inner.width),
            };
            let view = self.current_mut();
            view.scroll_snapshot = Some(snapshot);
            view.scroll_from_bottom = 0;
            view.unseen = 0;
            self.pending_exact_evidence = None;
            self.status = trf("st.run.jump.overlay", &[&target.seq_start.to_string()]);
            return true;
        }
        let capture = command_capture_for_target(&target, &entries);
        if local_command_evidence_is_complete(self.current(), &target, &entries, &capture) {
            let rows = all_output_visual_rows(self, inner.width);
            let target_seq = capture
                .start
                .and_then(|index| entries.get(index))
                .map_or(target.seq_start, |entry| entry.seq);
            let Some(target_index) = rows.iter().position(|row| {
                row.daemon_epoch == Some(target.daemon_epoch) && row.seq >= target_seq
            }) else {
                return self.query_exact_evidence(ExactEvidenceTarget::Command(target));
            };
            let height = usize::from(inner.height).max(1);
            let start = target_index.saturating_sub(height / 3);
            let end = start.saturating_add(height).min(rows.len());
            let scroll_from_bottom = rows.len().saturating_sub(end);
            let snapshot = ScrollSnapshot {
                rows: rows.into_iter().map(|row| row.line).collect(),
            };
            let view = self.current_mut();
            view.scroll_snapshot = Some(snapshot);
            view.scroll_from_bottom = scroll_from_bottom;
            view.unseen = 0;
            self.pending_exact_evidence = None;
            self.status = trf("st.run.jump", &[&target.seq_start.to_string()]);
            return true;
        }
        self.query_exact_evidence(ExactEvidenceTarget::Command(target))
    }

    fn command_evidence_target(
        &self,
        key: RunCommandKey,
        step_index: Option<usize>,
    ) -> Option<CommandEvidenceTarget> {
        let view = self.current();
        let record = view.run_command(key)?;
        let step_index = step_index.filter(|index| *index < record.steps.len());
        let step = step_index.and_then(|index| record.steps.get(index));
        let seq_start = step.map_or(record.first_seq, |step| step.first_seq);
        let write_end_seq = step.map_or(record.last_seq, |step| step.last_seq);
        let daemon_epoch = step.map_or(record.daemon_epoch, |step| step.daemon_epoch);
        let next_command = step_index
            .and_then(|index| record.steps.get(index + 1).map(|step| step.first_seq))
            .or_else(|| view.next_run_command_seq(key));
        let query_end_seq = next_command
            .map(|sequence| sequence.saturating_sub(1))
            .unwrap_or(view.snapshot.head_seq.max(view.last_seq))
            .max(write_end_seq);
        Some(CommandEvidenceTarget {
            key,
            step_index,
            port: view.snapshot.config.port.clone(),
            daemon_epoch,
            seq_start,
            write_end_seq,
            query_end_seq,
            command: command_payload(record, step_index),
            matchers: command_capture_matchers(record, step_index),
        })
    }

    fn jump_output_to_monitor_incident(&mut self) -> bool {
        let Some(incident) = self.current().selected_monitor_incident().cloned() else {
            return false;
        };
        let Some(inner) = self.layout.map(|layout| layout.output_inner) else {
            return false;
        };
        let target = IncidentEvidenceTarget::from(&incident);
        if local_incident_entry_range(self.current(), &target).is_some() {
            let rows = all_output_visual_rows(self, inner.width);
            let Some(target_index) = rows.iter().position(|row| {
                row.daemon_epoch == Some(target.daemon_epoch) && row.seq == target.seq_start
            }) else {
                return self.query_exact_evidence(ExactEvidenceTarget::Incident(target));
            };
            let height = usize::from(inner.height).max(1);
            let start = target_index.saturating_sub(height / 3);
            let end = start.saturating_add(height).min(rows.len());
            let scroll_from_bottom = rows.len().saturating_sub(end);
            let snapshot = ScrollSnapshot {
                rows: rows.into_iter().map(|row| row.line).collect(),
            };
            let view = self.current_mut();
            view.scroll_snapshot = Some(snapshot);
            view.scroll_from_bottom = scroll_from_bottom;
            view.unseen = 0;
            self.pending_exact_evidence = None;
            self.status = trf(
                "st.monitor.jump",
                &[&target.seq_start.to_string(), &target.seq_end.to_string()],
            );
            return true;
        }
        self.query_exact_evidence(ExactEvidenceTarget::Incident(target))
    }

    fn query_exact_evidence(&mut self, target: ExactEvidenceTarget) -> bool {
        let Some(commands) = self.exact_evidence_commands.as_ref() else {
            self.current_mut().follow();
            self.pending_exact_evidence = None;
            self.status = match target {
                ExactEvidenceTarget::Incident(_) => tr("st.monitor.jump.query.unavailable").into(),
                ExactEvidenceTarget::Command(_) => tr("st.run.jump.query.unavailable").into(),
            };
            return false;
        };
        let request_id = Uuid::new_v4();
        let request = ExactEvidenceRequest {
            request_id,
            target: target.clone(),
        };
        match commands.try_send(ExactEvidenceIoCommand::Query(request)) {
            Ok(()) => {
                self.current_mut().follow();
                self.pending_exact_evidence = Some((request_id, target.clone()));
                self.status = match target {
                    ExactEvidenceTarget::Incident(target) => trf(
                        "st.monitor.jump.loading",
                        &[&target.seq_start.to_string(), &target.seq_end.to_string()],
                    ),
                    ExactEvidenceTarget::Command(target) => {
                        trf("st.run.jump.loading", &[&target.seq_start.to_string()])
                    }
                };
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.current_mut().follow();
                self.pending_exact_evidence = None;
                self.status = match target {
                    ExactEvidenceTarget::Incident(_) => tr("st.monitor.jump.query.busy").into(),
                    ExactEvidenceTarget::Command(_) => tr("st.run.jump.query.busy").into(),
                };
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.current_mut().follow();
                self.pending_exact_evidence = None;
                self.status = match target {
                    ExactEvidenceTarget::Incident(_) => {
                        tr("st.monitor.jump.query.unavailable").into()
                    }
                    ExactEvidenceTarget::Command(_) => tr("st.run.jump.query.unavailable").into(),
                };
                false
            }
        }
    }

    fn has_queued_control(&self, port: &str) -> bool {
        self.queued_controls.contains_key(port)
            || self.pending_writes.contains_key(port)
            || self.pending_requests.values().any(
                |request| matches!(request, PendingRequest::Acquire { port: pending, .. } if pending == port),
            )
    }

    fn cancel_queued_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        port: &str,
        reason: &str,
    ) {
        let message = ClientMessage::CancelAcquire {
            request_id: Uuid::new_v4(),
            port: port.to_owned(),
            // A queued waiter has no lease identity; seriald matches it by
            // actor identity and treats this field as wire context.
            control_id: Uuid::nil(),
        };
        if self.send_message(
            commands,
            message,
            Some(PendingRequest::CancelAcquire {
                port: port.to_owned(),
            }),
        ) {
            self.pending_writes.remove(port);
            self.inflight_writes.remove(port);
            self.queued_controls.remove(port);
            self.pending_requests.retain(|_, request| {
                !matches!(request, PendingRequest::Acquire { port: pending, .. } if pending == port)
            });
            if self
                .pending_paste
                .as_ref()
                .is_some_and(|paste| paste.port == port)
            {
                self.pending_paste = None;
            }
            self.status = trf("st.reconnect.reason", &[reason, port]);
        }
    }

    fn release_slot_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        port: String,
        lease: ControlLease,
        automatic: bool,
    ) {
        let release_pending = self.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Release { port: pending } if pending == &port),
        );
        if release_pending {
            return;
        }
        self.send_message(
            commands,
            ClientMessage::ReleaseControl {
                request_id: Uuid::new_v4(),
                port: port.clone(),
                control_id: lease.id,
                fence: lease.fence,
            },
            Some(PendingRequest::Release { port: port.clone() }),
        );
        if automatic {
            self.status = trf(
                "st.idle.release",
                &[&port, &self.human_idle_release.as_secs().to_string()],
            );
        }
    }

    fn maintain_controls(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        if !self.transport_connected || !self.hello_accepted {
            return;
        }
        self.dirty = true;
        let idle_release = self.human_idle_release;
        let expired_queue = self.queued_controls.iter().find_map(|(port, queued)| {
            let last_activity = self
                .slot_index(port)
                .and_then(|index| self.ports[index].last_manual_activity);
            let idle = last_activity
                .map(|activity| activity.elapsed())
                .unwrap_or_else(|| queued.since.elapsed());
            (idle >= idle_release).then(|| port.clone())
        });
        if let Some(port) = expired_queue {
            self.cancel_queued_control(
                commands,
                &port,
                &trf("st.queue.expired", &[&idle_release.as_secs().to_string()]),
            );
            return;
        }

        let actor_id = self.actor.as_ref().map(|actor| actor.id.clone());
        let leases = self
            .ports
            .iter()
            .filter_map(|slot| {
                if !slot.subscription.is_ready() {
                    return None;
                }
                let lease = slot.snapshot.control.as_ref()?;
                (Some(&lease.owner.id) == actor_id.as_ref())
                    .then(|| (slot.snapshot.config.port.clone(), lease.clone()))
            })
            .collect::<Vec<_>>();
        for (port, lease) in leases {
            let index = self.slot_index(&port).expect("lease came from this Port");
            // Retry a locally accepted operation whose previous outbound send
            // hit channel backpressure. Pending Write requests and active
            // Triggers are already guarded inside `flush_pending_writes`.
            self.flush_pending_writes(&port, commands);
            let operation_pending = self.pending_writes.contains_key(&port)
                || self.pending_requests.values().any(
                    |request| matches!(request, PendingRequest::Write { port: pending, .. } if pending == &port),
                );
            let recently_active = self.ports[index]
                .last_manual_activity
                .is_some_and(|activity| activity.elapsed() < idle_release);
            if !recently_active && !operation_pending {
                self.release_slot_control(commands, port, lease, true);
                continue;
            }
            let already_pending = self.pending_requests.values().any(|request| {
                matches!(request, PendingRequest::Renew { port: pending } if pending == &port)
            });
            if already_pending {
                continue;
            }
            self.send_message(
                commands,
                ClientMessage::RenewControl {
                    request_id: Uuid::new_v4(),
                    port: port.clone(),
                    control_id: lease.id,
                    fence: lease.fence,
                    ttl_ms: CONTROL_TTL_MS,
                },
                Some(PendingRequest::Renew { port }),
            );
        }
    }

    fn flush_pending_writes(
        &mut self,
        port: &str,
        commands: &mpsc::Sender<NetworkCommand>,
    ) -> bool {
        let Some(index) = self.slot_index(port) else {
            return false;
        };
        if !self.transport_connected
            || !self.hello_accepted
            || !self.slot_ready(index)
            || !self.owns_control(index)
            || self.ports[index].snapshot.active_trigger.is_some()
        {
            return true;
        }
        let write_already_pending = self.pending_requests.values().any(|request| {
            matches!(request, PendingRequest::Write { port: pending, .. } if pending == port)
        });
        if write_already_pending {
            return true;
        }
        let progress = self.inflight_writes.get(port).copied().or_else(|| {
            self.pending_writes
                .get(port)
                .and_then(|writes| writes.front())
                .map(|write| InFlightWrite {
                    operation_id: write.operation_id,
                    kind: write.kind,
                    chunk_index: 0,
                })
        });
        let write = progress.and_then(|progress| {
            self.pending_writes
                .get(port)
                .and_then(|writes| writes.get(progress.chunk_index))
                .filter(|write| {
                    write.operation_id == progress.operation_id && write.kind == progress.kind
                })
                .cloned()
                .map(|write| (progress, write))
        });
        if let Some((progress, write)) = write {
            self.inflight_writes.insert(port.to_owned(), progress);
            if !self.send_write_now(commands, port, write.data, write.operation_id) {
                self.inflight_writes.remove(port);
                return false;
            }
        }
        true
    }

    fn acknowledge_inflight_write(&mut self, port: &str) {
        let Some(mut progress) = self.inflight_writes.get(port).copied() else {
            return;
        };
        let next_index = progress.chunk_index.saturating_add(1);
        let same_operation_continues = self
            .pending_writes
            .get(port)
            .and_then(|writes| writes.get(next_index))
            .is_some_and(|write| {
                write.operation_id == progress.operation_id && write.kind == progress.kind
            });
        if same_operation_continues {
            progress.chunk_index = next_index;
            self.inflight_writes.insert(port.to_owned(), progress);
            return;
        }

        if let Some(writes) = self.pending_writes.get_mut(port) {
            writes.drain(..next_index.min(writes.len()));
            if writes.is_empty() {
                self.pending_writes.remove(port);
            }
        }
        self.inflight_writes.remove(port);
    }

    fn send_write_now(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        port: &str,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
    ) -> bool {
        let Some(index) = self.slot_index(port) else {
            return false;
        };
        let Some(lease) = self.ports[index].snapshot.control.clone() else {
            self.status = tr("st.write.disappeared").into();
            return false;
        };
        self.send_message(
            commands,
            ClientMessage::Write {
                request_id: Uuid::new_v4(),
                port: port.to_string(),
                control_id: lease.id,
                fence: lease.fence,
                data,
                operation_id,
                // Human writes are governed by the fenced control lease, not
                // by an Agent Run boundary.
                expected_run_id: None,
                pacing: None,
                description: None,
                command_sequence: None,
                command_capture_matchers: Vec::new(),
                sequence_precondition: None,
                cooperative: false,
            },
            Some(PendingRequest::Write {
                port: port.to_string(),
                operation_id,
                cooperative: false,
            }),
        )
    }

    fn handle_terminal_event(&mut self, event: Event, commands: &mpsc::Sender<NetworkCommand>) {
        match event {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                self.reset_software_cursor_blink(Instant::now());
                self.handle_key(key, commands)
            }
            Event::Paste(value) => {
                self.reset_software_cursor_blink(Instant::now());
                self.clear_text_selection();
                if self.output_search.is_some() {
                    self.handle_output_search_paste(value);
                } else if self.menu.is_some() {
                    self.handle_menu_paste(value);
                } else {
                    self.handle_paste(value, commands);
                }
            }
            Event::Mouse(mouse) => self.handle_mouse(mouse, commands),
            Event::Resize(_, _) => {
                self.clear_text_selection();
                for slot in &mut self.ports {
                    slot.follow();
                }
                self.queue_selection = None;
                self.focus = PaneFocus::Input;
                self.dirty = true;
            }
            Event::FocusLost => {
                self.clear_text_selection();
                self.dirty = true;
            }
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        self.clear_text_selection();
        if self.output_search.is_some() {
            self.handle_output_search_key(key);
            self.dirty = true;
            return;
        }
        if self.menu.is_some() {
            self.handle_menu_key(key);
            self.dirty = true;
            return;
        }
        if self.queue_selection.is_some() {
            self.handle_queue_key(key, commands);
            self.dirty = true;
            return;
        }
        if self.help {
            let page = self.layout.map_or(10, |layout| {
                usize::from(layout.output_area.height.max(3)).saturating_sub(2)
            });
            let max = help_lines(self).len().saturating_sub(page);
            match key.code {
                KeyCode::PageUp | KeyCode::Up => {
                    self.help_scroll = self.help_scroll.saturating_sub(page.max(1));
                    self.dirty = true;
                    return;
                }
                KeyCode::PageDown | KeyCode::Down => {
                    self.help_scroll = self.help_scroll.saturating_add(page.max(1)).min(max);
                    self.dirty = true;
                    return;
                }
                KeyCode::Home => {
                    self.help_scroll = 0;
                    self.dirty = true;
                    return;
                }
                KeyCode::End => {
                    self.help_scroll = max;
                    self.dirty = true;
                    return;
                }
                _ => {}
            }
            self.help = false;
            self.help_scroll = 0;
            if is_prefix(key) {
                self.prefix_pending = true;
                self.help_dismiss_prefix = true;
            }
            self.dirty = true;
            return;
        }
        if self.prefix_pending {
            self.prefix_pending = false;
            if self.help_dismiss_prefix && key.code == KeyCode::Char('?') {
                self.help_dismiss_prefix = false;
                self.dirty = true;
                return;
            }
            self.help_dismiss_prefix = false;
            self.handle_prefix_key(key, commands);
            self.dirty = true;
            return;
        }
        if is_prefix(key) {
            self.prefix_pending = true;
            self.help_dismiss_prefix = false;
            self.status = tr("st.prefix.hint").into();
            self.dirty = true;
            return;
        }
        if key.modifiers.contains(KeyModifiers::ALT)
            && let KeyCode::Char(digit @ '1'..='9') = key.code
        {
            self.select((digit as usize) - ('1' as usize));
            return;
        }

        match key.code {
            KeyCode::PageUp => {
                self.run_panel_visible = true;
                self.focus = PaneFocus::RunHistory;
                self.handle_run_history_key(key);
                self.dirty = true;
                return;
            }
            KeyCode::PageDown => {
                self.run_panel_visible = true;
                self.focus = PaneFocus::RunHistory;
                self.handle_run_history_key(key);
                self.dirty = true;
                return;
            }
            KeyCode::Up | KeyCode::Down | KeyCode::Left | KeyCode::Right => {
                self.run_panel_visible = true;
                self.focus = PaneFocus::RunHistory;
                self.handle_run_history_key(key);
                self.dirty = true;
                return;
            }
            KeyCode::Enter
            | KeyCode::Backspace
            | KeyCode::Delete
            | KeyCode::Tab
            | KeyCode::BackTab
            | KeyCode::Char(_)
                if !key.modifiers.contains(KeyModifiers::ALT) =>
            {
                let leaving_history = self.focus == PaneFocus::RunHistory;
                self.focus = PaneFocus::Input;
                if leaving_history {
                    self.current_mut().follow();
                }
                match self.current_mode() {
                    InputMode::Line => self.handle_line_key(key, commands),
                    InputMode::Raw => self.handle_raw_key(key, commands),
                }
                self.dirty = true;
                return;
            }
            _ => {}
        }

        if self.focus == PaneFocus::RunHistory {
            self.handle_run_history_key(key);
            self.dirty = true;
            return;
        }

        self.focus = PaneFocus::Input;
        match self.current_mode() {
            InputMode::Line => self.handle_line_key(key, commands),
            InputMode::Raw => self.handle_raw_key(key, commands),
        }
        self.dirty = true;
    }

    fn handle_mouse(&mut self, mouse: MouseEvent, commands: &mpsc::Sender<NetworkCommand>) {
        if matches!(
            mouse.kind,
            MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
        ) {
            self.last_output_click = None;
            self.clear_text_selection();
            self.run_panel_visible = true;
            self.focus = PaneFocus::RunHistory;
            self.handle_run_history_key(KeyEvent::new(
                if mouse.kind == MouseEventKind::ScrollUp {
                    KeyCode::PageUp
                } else {
                    KeyCode::PageDown
                },
                KeyModifiers::NONE,
            ));
            self.dirty = true;
            return;
        }
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => self.begin_mouse_selection(mouse),
            MouseEventKind::Drag(MouseButton::Left) => self.update_mouse_selection(mouse),
            MouseEventKind::Up(MouseButton::Left) => self.finish_mouse_selection(mouse),
            MouseEventKind::Down(MouseButton::Right) => {
                self.handle_right_click(mouse, commands);
            }
            _ => {}
        }
        self.dirty = true;
    }

    fn clear_text_selection(&mut self) {
        self.selection = None;
        self.selection_copy = None;
    }

    fn begin_mouse_selection(&mut self, mouse: MouseEvent) {
        let Some(layout) = self.layout else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if rect_contains(layout.input_area, position) {
            self.reset_software_cursor_blink(Instant::now());
            self.last_output_click = None;
            self.queue_selection = None;
            self.clear_text_selection();
            return;
        }
        if !rect_contains(layout.output_area, position) {
            self.last_output_click = None;
            return;
        }
        self.clear_text_selection();
        if !rect_contains(layout.output_inner, position) {
            return;
        }
        let rows = visible_output_lines(self, layout.output_inner);
        let Some(point) = selection_point(layout.output_inner, position, rows.len()) else {
            return;
        };
        let plain_rows = rows.iter().map(line_plain_text).collect::<Vec<_>>();
        let now = Instant::now();
        let double_click = self.last_output_click.is_some_and(|previous| {
            output_clicks_form_double_click(previous, point, &plain_rows, now)
        });
        self.last_output_click = (!double_click).then_some(OutputClick { point, at: now });
        let (anchor, head, word_selected) = if double_click {
            word_selection_points(&plain_rows, point)
                .map_or((point, point, false), |(anchor, head)| (anchor, head, true))
        } else {
            (point, point, false)
        };
        self.selection = Some(TextSelection {
            rows,
            plain_rows,
            anchor,
            head,
            word_selected,
            completed: false,
            last_activity: now,
        });
        if word_selected {
            self.complete_mouse_selection();
        }
    }

    fn update_mouse_selection(&mut self, mouse: MouseEvent) {
        let (Some(layout), Some(selection)) = (self.layout, self.selection.as_mut()) else {
            return;
        };
        if selection.completed {
            return;
        }
        let position = Position::new(mouse.column, mouse.row);
        if let Some(point) =
            selection_point_clamped(layout.output_inner, position, selection.plain_rows.len())
        {
            selection.head = point;
            selection.last_activity = Instant::now();
            if selection.anchor != point {
                self.last_output_click = None;
            }
        }
    }

    fn finish_mouse_selection(&mut self, mouse: MouseEvent) {
        self.update_mouse_selection(mouse);
        self.complete_mouse_selection();
    }

    fn complete_mouse_selection(&mut self) -> bool {
        let Some(selection) = self.selection.as_ref() else {
            return false;
        };
        if selection.completed {
            return true;
        }
        if !selection.is_dragged() {
            self.selection = None;
            self.selection_copy = None;
            return false;
        }
        let text = selection.selected_text();
        if text.is_empty() {
            self.selection = None;
            self.selection_copy = None;
            return false;
        }
        let characters = text.chars().count().to_string();
        self.status = match (self.clipboard_copy)(&text) {
            Ok(()) => trf("st.selection.copied", &[&characters]),
            Err(error) => trf("st.clipboard.copy.failed", &[&error.to_string()]),
        };
        if let Some(selection) = self.selection.as_mut() {
            selection.completed = true;
        }
        // Keep both the highlighted cells and the payload so right-click
        // remains an explicit retry/copy path.
        self.selection_copy = Some(text);
        true
    }

    fn expire_mouse_selection(&mut self, now: Instant) -> bool {
        if self.selection.as_ref().is_some_and(|selection| {
            !selection.completed
                && now.saturating_duration_since(selection.last_activity) >= MOUSE_SELECTION_TIMEOUT
        }) {
            return self.complete_mouse_selection();
        }
        false
    }

    fn take_selection_text(&mut self) -> Option<String> {
        let active_text = self
            .selection
            .take()
            .map(|selection| selection.selected_text())
            .filter(|text| !text.is_empty());
        let text = active_text.or_else(|| self.selection_copy.take());
        self.selection_copy = None;
        text
    }

    fn handle_right_click(&mut self, mouse: MouseEvent, commands: &mpsc::Sender<NetworkCommand>) {
        self.last_output_click = None;
        let Some(layout) = self.layout else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if rect_contains(layout.output_area, position) {
            let Some(text) = self.take_selection_text() else {
                return;
            };
            self.status = match (self.clipboard_copy)(&text) {
                Ok(()) => trf("st.clipboard.copied", &[&text.chars().count().to_string()]),
                Err(error) => trf("st.clipboard.copy.failed", &[&error.to_string()]),
            };
            return;
        }
        if !rect_contains(layout.input_area, position) {
            return;
        }
        self.queue_selection = None;
        self.clear_text_selection();
        match crate::clipboard::read_text() {
            Ok(Some(text)) => self.handle_paste(text, commands),
            Ok(None) => self.status = tr("st.clipboard.paste.shortcut").into(),
            Err(error) => {
                self.status = trf("st.clipboard.paste.failed", &[&error.to_string()]);
            }
        }
    }

    fn handle_prefix_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        match key.code {
            KeyCode::Char(digit @ '1'..='9') => {
                self.select((digit as usize) - ('1' as usize));
            }
            KeyCode::Char('s' | 'S') => self.select((self.selected + 1) % self.ports.len()),
            KeyCode::Char('l' | 'L') => {
                self.current_mut().mode = InputMode::Line;
                self.status = tr("st.line.mode").into();
            }
            KeyCode::Char('r' | 'R') => {
                self.current_mut().mode = InputMode::Raw;
                self.status = tr("st.raw.mode").into();
            }
            KeyCode::Char('f' | 'F') | KeyCode::End => {
                self.current_mut().follow();
                self.status = tr("st.follow").into();
            }
            KeyCode::Char('v' | 'V') => {
                for slot in &mut self.ports {
                    slot.follow();
                }
                self.detailed_timeline = !self.detailed_timeline;
                self.status = if self.detailed_timeline {
                    tr("st.detailed").into()
                } else {
                    tr("st.compact").into()
                };
            }
            KeyCode::Char('g' | 'G') => self.toggle_language(),
            KeyCode::Char('m' | 'M') => self.open_menu(),
            KeyCode::Char('o' | 'O') => self.open_profiles_menu(),
            KeyCode::Char('h' | 'H') => self.toggle_run_history_panel(),
            KeyCode::PageUp => self.scroll_up(10),
            KeyCode::PageDown => self.scroll_down(10),
            KeyCode::Char('t' | 'T') => {
                self.acquire_control(commands, ControlMode::Takeover);
            }
            KeyCode::Char('c' | 'C') => self.release_control(commands),
            KeyCode::Char('d' | 'D') => self.remove_last_queued_line(false, commands),
            KeyCode::Char('e' | 'E') => self.remove_last_queued_line(true, commands),
            KeyCode::Char('u' | 'U') => self.open_queue_selection(),
            KeyCode::Char('p' | 'P') => self.confirm_paste(commands),
            KeyCode::Char('/') => {
                self.open_output_search();
            }
            KeyCode::Char('?') => {
                self.help = true;
                self.help_scroll = 0;
            }
            KeyCode::Char('q' | 'Q') => self.should_quit = true,
            KeyCode::Char(']') => {
                self.request_raw_write(commands, vec![0x1d]);
            }
            _ => self.status = tr("st.unknown.prefix").into(),
        }
    }

    fn handle_line_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        if self.current().history_search.is_some() {
            self.handle_history_search_key(key);
            return;
        }
        // Any key other than Tab confirms the current completion candidate.
        if key.code != KeyCode::Tab && self.current().completion.is_some() {
            self.current_mut().completion = None;
        }
        match key.code {
            KeyCode::Enter => {
                let value = self.current().draft.iter().collect::<String>();
                if value.is_empty() {
                    self.current_mut().follow();
                    self.status = tr("st.agent.enter.follow").into();
                    return;
                }
                let mut bytes = value.as_bytes().to_vec();
                bytes.extend_from_slice(self.current().effective_write_eol().as_bytes());
                let operation_id = Some(Uuid::new_v4());
                let cooperative = key.modifiers.contains(KeyModifiers::ALT);
                let accepted = if cooperative {
                    self.request_cooperative_write(commands, bytes.clone(), operation_id)
                } else {
                    self.request_write(commands, bytes, operation_id)
                };
                if !accepted {
                    return;
                }
                {
                    let view = self.current_mut();
                    if !value.is_empty() {
                        view.history.push(value);
                        if view.history.len() > 500 {
                            view.history.remove(0);
                        }
                    }
                    view.draft.clear();
                    view.draft_cursor = 0;
                }
                // Sending returns the view to the live tail, like Ctrl-] f.
                self.current_mut().follow();
            }
            KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.start_history_search();
            }
            KeyCode::Tab => self.complete_draft(),
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                let view = self.current_mut();
                view.draft.insert(view.draft_cursor, character);
                view.draft_cursor += 1;
            }
            KeyCode::Backspace => {
                let view = self.current_mut();
                if view.draft_cursor > 0 {
                    view.draft_cursor -= 1;
                    view.draft.remove(view.draft_cursor);
                }
            }
            KeyCode::Delete => {
                let view = self.current_mut();
                if view.draft_cursor < view.draft.len() {
                    view.draft.remove(view.draft_cursor);
                }
            }
            KeyCode::Left => {
                let view = self.current_mut();
                view.draft_cursor = view.draft_cursor.saturating_sub(1);
            }
            KeyCode::Right => {
                let view = self.current_mut();
                view.draft_cursor = (view.draft_cursor + 1).min(view.draft.len());
            }
            KeyCode::Home => self.current_mut().draft_cursor = 0,
            KeyCode::End => {
                let length = self.current().draft.len();
                self.current_mut().draft_cursor = length;
            }
            KeyCode::PageUp => self.scroll_up(10),
            KeyCode::PageDown => self.scroll_down(10),
            KeyCode::Char('c' | 'C') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                {
                    let view = self.current_mut();
                    view.draft.clear();
                    view.draft_cursor = 0;
                    view.completion = None;
                }
                // Ctrl-C is an asynchronous remote TTY interrupt, not a LINE
                // command. Send ETX immediately without the Profile EOL or
                // any unsent local draft.
                self.request_raw_write(commands, vec![0x03]);
                self.current_mut().follow();
            }
            _ => {}
        }
    }

    fn handle_raw_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        if let Some(bytes) = raw_key_bytes(key) {
            self.request_raw_write(commands, bytes);
        }
    }

    fn handle_paste(&mut self, value: String, commands: &mpsc::Sender<NetworkCommand>) {
        if value.len() > MAX_PASTE_BYTES {
            self.status = trf(
                "st.paste.rejected",
                &[&value.len().to_string(), &MAX_PASTE_BYTES.to_string()],
            );
            self.dirty = true;
            return;
        }
        let dangerous = value.len() > 1024 || value.contains('\n') || value.contains('\r');
        if dangerous {
            self.pending_paste = Some(PendingPaste {
                port: self.selected_port(),
                bytes: value.into_bytes(),
                raw: self.current_mode() == InputMode::Raw,
            });
            self.status = tr("st.paste.blocked").into();
            self.dirty = true;
            return;
        }
        if self.current_mode() == InputMode::Raw {
            self.request_raw_write(commands, value.into_bytes());
        } else {
            let view = self.current_mut();
            // LINE mode is a visible command editor, so hidden terminal
            // controls belong in RAW mode and must not desynchronize display
            // width from the logical cursor.
            let visible = safe_inline(&value);
            for character in visible.chars() {
                view.draft.insert(view.draft_cursor, character);
                view.draft_cursor += 1;
            }
        }
        self.dirty = true;
    }

    fn confirm_paste(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        let Some(paste) = self.pending_paste.take() else {
            self.status = tr("st.paste.none").into();
            return;
        };
        let Some(index) = self.slot_index(&paste.port) else {
            self.status = tr("st.paste.gone").into();
            return;
        };
        let previous = self.selected;
        self.selected = index;
        let accepted = if paste.raw {
            self.request_raw_write(commands, paste.bytes)
        } else {
            let text = String::from_utf8_lossy(&paste.bytes);
            let eol = self.current().effective_write_eol().to_string();
            let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
            let writes = normalized
                .split_inclusive('\n')
                .map(|line| {
                    let visible = safe_inline(line.trim_end_matches('\n'));
                    let mut command = Vec::with_capacity(visible.len() + eol.len());
                    command.extend_from_slice(visible.as_bytes());
                    command.extend_from_slice(eol.as_bytes());
                    command
                })
                .collect::<Vec<_>>();
            self.request_write_batch(commands, writes)
        };
        self.selected = previous;
        if accepted {
            self.status = trf("st.paste.queued", &[&paste.port]);
        }
    }

    fn start_history_search(&mut self) {
        let view = self.current_mut();
        if view.history_search.is_some() {
            return;
        }
        view.history_search = Some(HistorySearch {
            query: String::new(),
            saved_draft: std::mem::take(&mut view.draft),
            saved_cursor: view.draft_cursor,
            match_index: None,
        });
        view.draft_cursor = 0;
    }

    fn handle_history_search_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                let view = self.current_mut();
                if let Some(search) = view.history_search.take() {
                    if let Some(index) = search.match_index {
                        view.draft = view.history[index].chars().collect();
                        view.draft_cursor = view.draft.len();
                    } else {
                        view.draft = search.saved_draft;
                        view.draft_cursor = search.saved_cursor;
                    }
                }
            }
            KeyCode::Esc => self.cancel_history_search(),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.cancel_history_search();
            }
            KeyCode::Char('g') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.cancel_history_search();
            }
            KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                // Repeat: find the next older match, cycling back to newest.
                let view = self.current_mut();
                if let Some(search) = &mut view.history_search {
                    search.match_index =
                        find_history_match(&view.history, &search.query, search.match_index)
                            .or_else(|| find_history_match(&view.history, &search.query, None));
                }
            }
            KeyCode::Backspace => {
                let view = self.current_mut();
                if let Some(search) = &mut view.history_search {
                    search.query.pop();
                    search.match_index = find_history_match(&view.history, &search.query, None);
                }
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                let view = self.current_mut();
                if let Some(search) = &mut view.history_search {
                    search.query.push(character);
                    search.match_index = find_history_match(&view.history, &search.query, None);
                }
            }
            _ => {}
        }
    }

    fn cancel_history_search(&mut self) {
        let view = self.current_mut();
        if let Some(search) = view.history_search.take() {
            view.draft = search.saved_draft;
            view.draft_cursor = search.saved_cursor;
        }
    }

    fn open_menu(&mut self) {
        self.open_menu_page(None);
    }

    fn open_profiles_menu(&mut self) {
        self.open_menu_page(Some(MenuPage::Profiles));
    }

    fn open_menu_page(&mut self, page: Option<MenuPage>) {
        self.queue_selection = None;
        self.focus = PaneFocus::Input;
        let mut menu = MenuState::new();
        if let Some(page) = page {
            menu.push(page);
        }
        self.submit_menu_command(&mut menu, MenuIoCommand::Reload);
        self.menu = Some(menu);
        self.status = if page == Some(MenuPage::Profiles) {
            tr("st.menu.profile.open").into()
        } else {
            tr("st.menu.open").into()
        };
    }

    fn submit_menu_command(&mut self, menu: &mut MenuState, command: MenuIoCommand) -> bool {
        if menu.busy {
            menu.message = tr("menu.busy").into();
            return false;
        }
        let Some(commands) = self.menu_commands.as_ref() else {
            menu.message = tr("menu.io.unavailable").into();
            self.status = menu.message.clone();
            return false;
        };
        match commands.try_send(command) {
            Ok(()) => {
                menu.busy = true;
                menu.message = tr("menu.loading").into();
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                menu.message = tr("menu.io.full").into();
                self.status = menu.message.clone();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                menu.message = tr("menu.io.unavailable").into();
                self.status = menu.message.clone();
                false
            }
        }
    }

    fn handle_menu_key(&mut self, key: KeyEvent) {
        let Some(mut menu) = self.menu.take() else {
            return;
        };
        if menu.field_help.take().is_some() {
            self.menu = Some(menu);
            return;
        }
        if let Some(confirmation) = menu.confirmation.take() {
            self.handle_menu_confirmation_key(&mut menu, confirmation, key);
            self.menu = Some(menu);
            return;
        }
        if let Some(prompt) = menu.prompt.take() {
            self.handle_menu_prompt_key(&mut menu, prompt, key);
            self.menu = Some(menu);
            return;
        }
        if let Some(choice) = menu.choice.take() {
            self.handle_menu_choice_key(&mut menu, choice, key);
            self.menu = Some(menu);
            return;
        }

        let mut keep_open = true;
        let count = menu_item_count(&menu);
        match key.code {
            KeyCode::Esc | KeyCode::Left => {
                if !menu.back() {
                    keep_open = false;
                    self.status = tr("st.menu.closed").into();
                }
            }
            KeyCode::Up if count > 0 => {
                menu.selected = menu.selected.saturating_sub(1);
            }
            KeyCode::Down if count > 0 => {
                menu.selected = (menu.selected + 1).min(count - 1);
            }
            KeyCode::PageUp if menu.page == MenuPage::Help => {
                menu.help_scroll = menu.help_scroll.saturating_sub(8);
            }
            KeyCode::PageDown if menu.page == MenuPage::Help => {
                menu.help_scroll = menu.help_scroll.saturating_add(8);
            }
            KeyCode::Home if menu.page == MenuPage::Help => menu.help_scroll = 0,
            KeyCode::End if menu.page == MenuPage::Help => menu.help_scroll = usize::MAX,
            KeyCode::Home if count > 0 => menu.selected = 0,
            KeyCode::End if count > 0 => menu.selected = count - 1,
            KeyCode::Char('r' | 'R') => {
                self.submit_menu_command(&mut menu, MenuIoCommand::Reload);
            }
            KeyCode::Char('?') => {
                menu.field_help = Some(menu_field_help(self, &menu));
            }
            KeyCode::Enter | KeyCode::Right => self.activate_menu_item(&mut menu),
            _ => {}
        }
        if keep_open {
            let count = menu_item_count(&menu);
            menu.selected = menu.selected.min(count.saturating_sub(1));
            self.menu = Some(menu);
        }
    }

    fn handle_menu_choice_key(
        &mut self,
        menu: &mut MenuState,
        mut choice: MenuChoice,
        key: KeyEvent,
    ) {
        match key.code {
            KeyCode::Esc | KeyCode::Left => {
                menu.message = tr("menu.choice.closed").into();
            }
            KeyCode::Up if !choice.options.is_empty() => {
                choice.selected = choice.selected.saturating_sub(1);
                menu.choice = Some(choice);
            }
            KeyCode::Down if !choice.options.is_empty() => {
                choice.selected = (choice.selected + 1).min(choice.options.len() - 1);
                menu.choice = Some(choice);
            }
            KeyCode::Home if !choice.options.is_empty() => {
                choice.selected = 0;
                menu.choice = Some(choice);
            }
            KeyCode::End if !choice.options.is_empty() => {
                choice.selected = choice.options.len() - 1;
                menu.choice = Some(choice);
            }
            KeyCode::Enter if !choice.options.is_empty() => {
                let option = choice.options[choice.selected].clone();
                self.apply_menu_choice(menu, choice.purpose, option.value);
            }
            KeyCode::Right => menu.choice = Some(choice),
            _ => menu.choice = Some(choice),
        }
    }

    fn handle_menu_confirmation_key(
        &mut self,
        menu: &mut MenuState,
        mut confirmation: MenuConfirmation,
        key: KeyEvent,
    ) {
        match key.code {
            KeyCode::Esc | KeyCode::Char('n' | 'N') => {
                menu.message = confirmation.cancelled_message;
            }
            KeyCode::Enter | KeyCode::Char('y' | 'Y') => match confirmation.action {
                MenuConfirmationAction::Mutation(mutation) => {
                    self.submit_menu_mutation(menu, mutation);
                }
            },
            KeyCode::Up => {
                confirmation.scroll = confirmation.scroll.saturating_sub(1);
                menu.confirmation = Some(confirmation);
            }
            KeyCode::Down => {
                confirmation.scroll =
                    (confirmation.scroll + 1).min(confirmation.lines.len().saturating_sub(1));
                menu.confirmation = Some(confirmation);
            }
            KeyCode::PageUp => {
                confirmation.scroll = confirmation.scroll.saturating_sub(8);
                menu.confirmation = Some(confirmation);
            }
            KeyCode::PageDown => {
                confirmation.scroll =
                    (confirmation.scroll + 8).min(confirmation.lines.len().saturating_sub(1));
                menu.confirmation = Some(confirmation);
            }
            KeyCode::Home => {
                confirmation.scroll = 0;
                menu.confirmation = Some(confirmation);
            }
            KeyCode::End => {
                confirmation.scroll = confirmation.lines.len().saturating_sub(1);
                menu.confirmation = Some(confirmation);
            }
            _ => menu.confirmation = Some(confirmation),
        }
    }

    fn handle_menu_prompt_key(
        &mut self,
        menu: &mut MenuState,
        mut prompt: MenuPrompt,
        key: KeyEvent,
    ) {
        match key.code {
            KeyCode::Esc => {
                menu.message = tr("menu.prompt.cancelled").into();
                return;
            }
            KeyCode::Left => prompt.cursor = prompt.cursor.saturating_sub(1),
            KeyCode::Right => prompt.cursor = (prompt.cursor + 1).min(prompt.value.len()),
            KeyCode::Home => prompt.cursor = 0,
            KeyCode::End => prompt.cursor = prompt.value.len(),
            KeyCode::Backspace => {
                if prompt.cursor > 0 {
                    prompt.cursor -= 1;
                    prompt.value.remove(prompt.cursor);
                }
            }
            KeyCode::Delete => {
                if prompt.cursor < prompt.value.len() {
                    prompt.value.remove(prompt.cursor);
                }
            }
            KeyCode::Char(character)
                if !key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
            {
                if prompt.value.len() < 512 && !character.is_control() {
                    prompt.value.insert(prompt.cursor, character);
                    prompt.cursor += 1;
                }
            }
            KeyCode::Enter => {
                let value = prompt.value.iter().collect::<String>();
                if matches!(&prompt.purpose, MenuPromptPurpose::AgentHistoryRows) {
                    let parsed = value.trim().parse::<u16>().ok().filter(|rows| {
                        (MIN_AGENT_HISTORY_ROWS..=MAX_AGENT_HISTORY_ROWS).contains(rows)
                    });
                    let Some(rows) = parsed else {
                        menu.message = trf(
                            "menu.display.history.invalid",
                            &[
                                &MIN_AGENT_HISTORY_ROWS.to_string(),
                                &MAX_AGENT_HISTORY_ROWS.to_string(),
                            ],
                        );
                        menu.prompt = Some(prompt);
                        return;
                    };
                    self.save_agent_history_rows(menu, rows);
                    return;
                }
                if matches!(&prompt.purpose, MenuPromptPurpose::OrphanRunTimeout) {
                    let parsed = value.trim().parse::<u64>().ok().filter(|seconds| {
                        *seconds == 0 || *seconds >= MIN_ORPHAN_RUN_TIMEOUT_SECONDS
                    });
                    let Some(seconds) = parsed else {
                        menu.message = trf(
                            "menu.run.timeout.invalid",
                            &[&MIN_ORPHAN_RUN_TIMEOUT_SECONDS.to_string()],
                        );
                        menu.prompt = Some(prompt);
                        return;
                    };
                    self.save_orphan_run_timeout(menu, seconds);
                    return;
                }
                if let MenuPromptPurpose::CurrentProfile(field) = &prompt.purpose {
                    let Some(editor) = menu.profile_editor.as_mut() else {
                        menu.message = tr("menu.catalog.unavailable").into();
                        return;
                    };
                    let valid = match field {
                        CurrentProfilePromptField::ShellPrompt => {
                            editor.device.shell_prompt =
                                (!value.is_empty()).then_some(value.clone());
                            true
                        }
                        CurrentProfilePromptField::UbootPrompt => {
                            editor.device.uboot_prompt =
                                (!value.is_empty()).then_some(value.clone());
                            true
                        }
                        CurrentProfilePromptField::ChunkSize => {
                            if value.trim().is_empty() {
                                editor.device.write_chunk_size = None;
                                true
                            } else {
                                value
                                    .trim()
                                    .parse::<u32>()
                                    .ok()
                                    .filter(|value| *value > 0)
                                    .map(|value| editor.device.write_chunk_size = Some(value))
                                    .is_some()
                            }
                        }
                        CurrentProfilePromptField::ChunkDelay => {
                            if value.trim().is_empty() {
                                editor.device.write_chunk_delay_ms = None;
                                true
                            } else {
                                value
                                    .trim()
                                    .parse::<u64>()
                                    .ok()
                                    .map(|value| editor.device.write_chunk_delay_ms = Some(value))
                                    .is_some()
                            }
                        }
                    };
                    if valid {
                        menu.message = tr("menu.current.modified").into();
                        return;
                    }
                    menu.message = tr("menu.current.value.invalid").into();
                    menu.prompt = Some(prompt);
                    return;
                }
                match prompt.purpose {
                    MenuPromptPurpose::CreateTransport(CreateTransportPromptField::Name) => {
                        if !valid_menu_name(&value) {
                            menu.message = tr("menu.name.invalid").into();
                            menu.prompt = Some(prompt);
                            return;
                        }
                        if let Some(profile) = menu.create_transport.as_mut() {
                            profile.name = value;
                            menu.message = tr("menu.current.modified").into();
                        }
                    }
                    MenuPromptPurpose::CreateModel(field) => {
                        let Some(profile) = menu.create_model.as_mut() else {
                            return;
                        };
                        let valid = match field {
                            CreateModelPromptField::Name => {
                                if valid_menu_name(&value) {
                                    profile.name = value;
                                    true
                                } else {
                                    false
                                }
                            }
                            CreateModelPromptField::ModelNames => {
                                let mut names = value
                                    .split([',', '，', ';', '；'])
                                    .map(str::trim)
                                    .filter(|name| !name.is_empty())
                                    .map(ToOwned::to_owned)
                                    .collect::<Vec<_>>();
                                names.dedup();
                                if names.iter().all(|name| valid_menu_name(name)) {
                                    profile.model_names = names;
                                    true
                                } else {
                                    false
                                }
                            }
                            CreateModelPromptField::ShellPrompt => {
                                profile.shell_prompt = (!value.is_empty()).then_some(value);
                                true
                            }
                            CreateModelPromptField::UbootPrompt => {
                                profile.uboot_prompt = (!value.is_empty()).then_some(value);
                                true
                            }
                            CreateModelPromptField::ChunkSize => {
                                if value.trim().is_empty() {
                                    profile.write_chunk_size = None;
                                    true
                                } else {
                                    value
                                        .trim()
                                        .parse::<u32>()
                                        .ok()
                                        .filter(|value| *value > 0)
                                        .map(|value| profile.write_chunk_size = Some(value))
                                        .is_some()
                                }
                            }
                            CreateModelPromptField::ChunkDelay => {
                                if value.trim().is_empty() {
                                    profile.write_chunk_delay_ms = None;
                                    true
                                } else {
                                    value
                                        .trim()
                                        .parse::<u64>()
                                        .ok()
                                        .map(|value| profile.write_chunk_delay_ms = Some(value))
                                        .is_some()
                                }
                            }
                        };
                        if !valid {
                            menu.message = tr("menu.current.value.invalid").into();
                            menu.prompt = Some(prompt);
                            return;
                        }
                        menu.message = tr("menu.current.modified").into();
                    }
                    MenuPromptPurpose::CurrentProfile(_)
                    | MenuPromptPurpose::AgentHistoryRows
                    | MenuPromptPurpose::OrphanRunTimeout => {
                        unreachable!("special configuration prompt handled before name validation")
                    }
                }
                return;
            }
            _ => {}
        }
        menu.prompt = Some(prompt);
    }

    fn handle_menu_paste(&mut self, value: String) {
        let Some(menu) = self.menu.as_mut() else {
            return;
        };
        let Some(prompt) = menu.prompt.as_mut() else {
            return;
        };
        for character in value.chars().filter(|character| !character.is_control()) {
            if prompt.value.len() >= 512 {
                break;
            }
            prompt.value.insert(prompt.cursor, character);
            prompt.cursor += 1;
        }
        self.dirty = true;
    }

    fn submit_menu_mutation(&mut self, menu: &mut MenuState, mutation: MenuMutation) {
        self.submit_menu_command(
            menu,
            MenuIoCommand::Mutation {
                mutation: Box::new(mutation),
            },
        );
    }

    fn begin_orphan_run_timeout_prompt(&self, menu: &mut MenuState) {
        let value = self.orphan_run_timeout_seconds.to_string();
        menu.prompt = Some(MenuPrompt {
            title: trf(
                "menu.run.timeout.prompt",
                &[&MIN_ORPHAN_RUN_TIMEOUT_SECONDS.to_string()],
            ),
            cursor: value.len(),
            value: value.chars().collect(),
            purpose: MenuPromptPurpose::OrphanRunTimeout,
        });
    }

    fn save_orphan_run_timeout(&mut self, menu: &mut MenuState, seconds: u64) {
        self.orphan_run_timeout_seconds = seconds;
        let label = orphan_run_timeout_label(seconds);
        if let Some(loaded) = &mut self.config {
            loaded.config.orphan_run_timeout_seconds = Some(self.orphan_run_timeout_seconds);
            match loaded.save() {
                Ok(()) => menu.message = trf("menu.run.timeout.saved", &[&label]),
                Err(error) => {
                    menu.message = trf(
                        "menu.run.timeout.save.failed",
                        &[&safe_inline(&error.to_string())],
                    );
                }
            }
        } else {
            menu.message = trf("menu.run.timeout.saved.session", &[&label]);
        }
        self.status = menu.message.clone();
    }

    fn save_agent_history_rows(&mut self, menu: &mut MenuState, rows: u16) {
        self.agent_history_rows = configured_agent_history_rows(Some(rows));
        let rows = self.agent_history_rows.to_string();
        if let Some(loaded) = &mut self.config {
            loaded.config.agent_history_rows = Some(self.agent_history_rows);
            match loaded.save() {
                Ok(()) => menu.message = trf("menu.display.saved", &[&rows]),
                Err(error) => {
                    menu.message = trf(
                        "menu.display.save.failed",
                        &[&safe_inline(&error.to_string())],
                    );
                }
            }
        } else {
            menu.message = trf("menu.display.saved.session", &[&rows]);
        }
        self.status = menu.message.clone();
    }

    fn refresh_current_profile_editor(&self, menu: &mut MenuState) {
        menu.profile_editor = menu
            .catalog
            .as_ref()
            .map(|catalog| CurrentProfileEditor::new(self.current(), catalog));
    }

    fn open_menu_choice(
        menu: &mut MenuState,
        purpose: MenuChoicePurpose,
        options: Vec<MenuChoiceOption>,
        selected: usize,
    ) {
        if options.is_empty() {
            menu.message = tr("menu.choice.empty").into();
            return;
        }
        menu.choice = Some(MenuChoice {
            purpose,
            selected: selected.min(options.len() - 1),
            options,
        });
        menu.message = tr("menu.choice.open").into();
    }

    fn apply_menu_choice(
        &mut self,
        menu: &mut MenuState,
        purpose: MenuChoicePurpose,
        value: MenuChoiceValue,
    ) {
        let changed = match purpose {
            MenuChoicePurpose::CurrentPort => {
                let (Some(editor), MenuChoiceValue::Text(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.port = value;
                true
            }
            MenuChoicePurpose::CurrentTransportProfile => {
                let MenuChoiceValue::OptionalText(binding) = value else {
                    return;
                };
                let Some(profile) = binding.as_deref().and_then(|name| {
                    menu.catalog
                        .as_ref()?
                        .transport_profiles
                        .iter()
                        .find(|profile| profile.name == name)
                        .cloned()
                }) else {
                    menu.message = tr("menu.choice.empty").into();
                    return;
                };
                let Some(editor) = menu.profile_editor.as_mut() else {
                    return;
                };
                editor.transport_binding = binding;
                editor.transport = profile;
                true
            }
            MenuChoicePurpose::CurrentModelProfile => {
                let MenuChoiceValue::OptionalText(binding) = value else {
                    return;
                };
                let profile = binding.as_deref().and_then(|name| {
                    menu.catalog
                        .as_ref()?
                        .model_profiles
                        .iter()
                        .find(|profile| profile.name == name)
                        .cloned()
                });
                let Some(editor) = menu.profile_editor.as_mut() else {
                    return;
                };
                editor.model_profile_binding = binding;
                if let Some(profile) = profile {
                    if !editor
                        .model_name
                        .as_ref()
                        .is_some_and(|name| profile.model_names.contains(name))
                    {
                        editor.model_name = None;
                    }
                    editor.device = profile;
                } else {
                    editor.model_name = None;
                }
                true
            }
            MenuChoicePurpose::CurrentBaudRate => {
                let (Some(editor), MenuChoiceValue::Number(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.transport.baud_rate = value;
                true
            }
            MenuChoicePurpose::CurrentDataBits => {
                let (Some(editor), MenuChoiceValue::DataBits(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.transport.data_bits = value;
                true
            }
            MenuChoicePurpose::CurrentParity => {
                let (Some(editor), MenuChoiceValue::Parity(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.transport.parity = value;
                true
            }
            MenuChoicePurpose::CurrentStopBits => {
                let (Some(editor), MenuChoiceValue::StopBits(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.transport.stop_bits = value;
                true
            }
            MenuChoicePurpose::CurrentFlowControl => {
                let (Some(editor), MenuChoiceValue::FlowControl(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.transport.flow_control = value;
                true
            }
            MenuChoicePurpose::CurrentDtr
            | MenuChoicePurpose::CurrentRts
            | MenuChoicePurpose::CurrentAutoOpen => {
                let (Some(editor), MenuChoiceValue::Bool(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                match purpose {
                    MenuChoicePurpose::CurrentDtr => editor.transport.dtr = value,
                    MenuChoicePurpose::CurrentRts => editor.transport.rts = value,
                    MenuChoicePurpose::CurrentAutoOpen => editor.transport.auto_open = value,
                    _ => unreachable!("grouped boolean choice"),
                }
                true
            }
            MenuChoicePurpose::CurrentWriteEol => {
                let (Some(editor), MenuChoiceValue::Eol(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.device.write_eol = value;
                true
            }
            MenuChoicePurpose::CurrentEcho => {
                let (Some(editor), MenuChoiceValue::Echo(value)) =
                    (menu.profile_editor.as_mut(), value)
                else {
                    return;
                };
                editor.device.echo = value;
                true
            }
            MenuChoicePurpose::CreateTransportBaudRate => {
                let (Some(profile), MenuChoiceValue::Number(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                profile.baud_rate = value;
                true
            }
            MenuChoicePurpose::CreateTransportDataBits => {
                let (Some(profile), MenuChoiceValue::DataBits(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                profile.data_bits = value;
                true
            }
            MenuChoicePurpose::CreateTransportParity => {
                let (Some(profile), MenuChoiceValue::Parity(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                profile.parity = value;
                true
            }
            MenuChoicePurpose::CreateTransportStopBits => {
                let (Some(profile), MenuChoiceValue::StopBits(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                profile.stop_bits = value;
                true
            }
            MenuChoicePurpose::CreateTransportFlowControl => {
                let (Some(profile), MenuChoiceValue::FlowControl(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                profile.flow_control = value;
                true
            }
            MenuChoicePurpose::CreateTransportDtr
            | MenuChoicePurpose::CreateTransportRts
            | MenuChoicePurpose::CreateTransportAutoOpen => {
                let (Some(profile), MenuChoiceValue::Bool(value)) =
                    (menu.create_transport.as_mut(), value)
                else {
                    return;
                };
                match purpose {
                    MenuChoicePurpose::CreateTransportDtr => profile.dtr = value,
                    MenuChoicePurpose::CreateTransportRts => profile.rts = value,
                    MenuChoicePurpose::CreateTransportAutoOpen => profile.auto_open = value,
                    _ => unreachable!("grouped boolean choice"),
                }
                true
            }
            MenuChoicePurpose::CreateModelWriteEol => {
                let (Some(profile), MenuChoiceValue::Eol(value)) =
                    (menu.create_model.as_mut(), value)
                else {
                    return;
                };
                profile.write_eol = value;
                true
            }
            MenuChoicePurpose::CreateModelEcho => {
                let (Some(profile), MenuChoiceValue::Echo(value)) =
                    (menu.create_model.as_mut(), value)
                else {
                    return;
                };
                profile.echo = value;
                true
            }
        };
        if changed {
            menu.message = tr("menu.current.modified").into();
        }
    }

    fn begin_current_profile_prompt(
        menu: &mut MenuState,
        field: CurrentProfilePromptField,
        title: &'static str,
        value: String,
    ) {
        let value = value.chars().collect::<Vec<_>>();
        menu.prompt = Some(MenuPrompt {
            title: title.into(),
            cursor: value.len(),
            value,
            purpose: MenuPromptPurpose::CurrentProfile(field),
        });
    }

    fn profile_editable(menu: &mut MenuState, transport: bool) -> bool {
        let editable = menu.profile_editor.as_ref().is_some_and(|editor| {
            if transport {
                editor.original_transport.is_some()
            } else {
                editor.device_is_bound()
            }
        });
        if !editable {
            menu.message = if transport {
                tr("menu.current.transport.missing").into()
            } else {
                tr("menu.current.device.unbound").into()
            };
        }
        editable
    }

    fn activate_current_profile_row(&mut self, menu: &mut MenuState) {
        let Some(row) = CurrentProfileRow::from_index(menu.selected) else {
            return;
        };
        if menu.profile_editor.is_none() {
            self.refresh_current_profile_editor(menu);
        }
        match row {
            CurrentProfileRow::Port => {
                let editor = menu.profile_editor.as_ref().expect("checked editor");
                let current = editor.port.clone();
                let original = editor.original_port.clone();
                let mut ports = menu
                    .catalog
                    .as_ref()
                    .map(|catalog| {
                        catalog
                            .detected_ports
                            .iter()
                            .filter(|port| {
                                port.name == original
                                    || !catalog.ports.iter().any(|configured| {
                                        configured.config.port == port.name
                                            && configured.config.port != original
                                    })
                            })
                            .map(|port| port.name.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                ports.push(current.clone());
                ports.sort();
                ports.dedup();
                let selected = ports.iter().position(|port| port == &current).unwrap_or(0);
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentPort,
                    ports
                        .into_iter()
                        .map(|port| MenuChoiceOption {
                            label: port.clone(),
                            value: MenuChoiceValue::Text(port),
                        })
                        .collect(),
                    selected,
                );
            }
            CurrentProfileRow::TransportProfile => {
                let current = menu
                    .profile_editor
                    .as_ref()
                    .and_then(|editor| editor.transport_binding.as_deref());
                let profiles = menu
                    .catalog
                    .as_ref()
                    .map(|catalog| catalog.transport_profiles.clone())
                    .unwrap_or_default();
                let selected = profiles
                    .iter()
                    .position(|profile| current == Some(profile.name.as_str()))
                    .unwrap_or(0);
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentTransportProfile,
                    profiles
                        .into_iter()
                        .map(|profile| MenuChoiceOption {
                            label: profile.name.clone(),
                            value: MenuChoiceValue::OptionalText(Some(profile.name)),
                        })
                        .collect(),
                    selected,
                );
            }
            CurrentProfileRow::BaudRate => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let current = menu
                    .profile_editor
                    .as_ref()
                    .expect("checked editor")
                    .transport
                    .baud_rate;
                let options = baud_rate_options(current);
                let selected = options
                    .iter()
                    .position(|value| *value == current)
                    .unwrap_or(0);
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentBaudRate,
                    options
                        .into_iter()
                        .map(|value| MenuChoiceOption {
                            label: value.to_string(),
                            value: MenuChoiceValue::Number(value),
                        })
                        .collect(),
                    selected,
                );
            }
            CurrentProfileRow::DataBits => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let current = menu.profile_editor.as_ref().unwrap().transport.data_bits;
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentDataBits,
                    data_bits_options(),
                    data_bits_index(current),
                );
            }
            CurrentProfileRow::Parity => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let current = menu.profile_editor.as_ref().unwrap().transport.parity;
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentParity,
                    parity_options(),
                    parity_index(current),
                );
            }
            CurrentProfileRow::StopBits => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let current = menu.profile_editor.as_ref().unwrap().transport.stop_bits;
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentStopBits,
                    stop_bits_options(),
                    stop_bits_index(current),
                );
            }
            CurrentProfileRow::FlowControl => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let current = menu.profile_editor.as_ref().unwrap().transport.flow_control;
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentFlowControl,
                    flow_control_options(),
                    flow_control_index(current),
                );
            }
            CurrentProfileRow::Dtr | CurrentProfileRow::Rts | CurrentProfileRow::AutoOpen => {
                if !Self::profile_editable(menu, true) {
                    return;
                }
                let transport = &menu.profile_editor.as_ref().unwrap().transport;
                let (purpose, current) = match row {
                    CurrentProfileRow::Dtr => (MenuChoicePurpose::CurrentDtr, transport.dtr),
                    CurrentProfileRow::Rts => (MenuChoicePurpose::CurrentRts, transport.rts),
                    CurrentProfileRow::AutoOpen => {
                        (MenuChoicePurpose::CurrentAutoOpen, transport.auto_open)
                    }
                    _ => unreachable!(),
                };
                Self::open_menu_choice(menu, purpose, bool_options(), usize::from(current));
            }
            CurrentProfileRow::ModelProfile => {
                let current = menu
                    .profile_editor
                    .as_ref()
                    .and_then(|editor| editor.model_profile_binding.as_deref());
                let profiles = menu
                    .catalog
                    .as_ref()
                    .map(|catalog| catalog.model_profiles.clone())
                    .unwrap_or_default();
                let mut options = vec![MenuChoiceOption {
                    label: tr("menu.value.unbound").into(),
                    value: MenuChoiceValue::OptionalText(None),
                }];
                options.extend(profiles.into_iter().map(|profile| MenuChoiceOption {
                    label: profile.name.clone(),
                    value: MenuChoiceValue::OptionalText(Some(profile.name)),
                }));
                let selected = current
                    .and_then(|name| options.iter().position(|option| option.label == name))
                    .unwrap_or(0);
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentModelProfile,
                    options,
                    selected,
                );
            }
            CurrentProfileRow::ModelName => menu.push(MenuPage::ModelFamilies),
            CurrentProfileRow::WriteEol => {
                if !Self::profile_editable(menu, false) {
                    return;
                }
                let current = menu
                    .profile_editor
                    .as_ref()
                    .unwrap()
                    .device
                    .write_eol
                    .clone();
                let options = eol_options();
                let selected = eol_index(current.as_deref());
                Self::open_menu_choice(menu, MenuChoicePurpose::CurrentWriteEol, options, selected);
            }
            CurrentProfileRow::Echo => {
                if !Self::profile_editable(menu, false) {
                    return;
                }
                let current = menu.profile_editor.as_ref().unwrap().device.echo;
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CurrentEcho,
                    echo_options(),
                    echo_index(current),
                );
            }
            CurrentProfileRow::ShellPrompt
            | CurrentProfileRow::UbootPrompt
            | CurrentProfileRow::ChunkSize
            | CurrentProfileRow::ChunkDelay => {
                if !Self::profile_editable(menu, false) {
                    return;
                }
                let editor = menu.profile_editor.as_ref().expect("checked editor");
                let (field, title, value) = match row {
                    CurrentProfileRow::ShellPrompt => (
                        CurrentProfilePromptField::ShellPrompt,
                        tr("menu.current.prompt.shell"),
                        editor.device.shell_prompt.clone().unwrap_or_default(),
                    ),
                    CurrentProfileRow::UbootPrompt => (
                        CurrentProfilePromptField::UbootPrompt,
                        tr("menu.current.prompt.uboot"),
                        editor.device.uboot_prompt.clone().unwrap_or_default(),
                    ),
                    CurrentProfileRow::ChunkSize => (
                        CurrentProfilePromptField::ChunkSize,
                        tr("menu.current.prompt.chunk"),
                        editor
                            .device
                            .write_chunk_size
                            .map_or_else(String::new, |value| value.to_string()),
                    ),
                    CurrentProfileRow::ChunkDelay => (
                        CurrentProfilePromptField::ChunkDelay,
                        tr("menu.current.prompt.delay"),
                        editor
                            .device
                            .write_chunk_delay_ms
                            .map_or_else(String::new, |value| value.to_string()),
                    ),
                    _ => unreachable!("grouped device prompt row"),
                };
                Self::begin_current_profile_prompt(menu, field, title, value);
            }
            CurrentProfileRow::Apply => self.submit_current_profile_updates(menu),
        }
    }

    fn begin_create_prompt(
        menu: &mut MenuState,
        title: &'static str,
        value: String,
        purpose: MenuPromptPurpose,
    ) {
        let value = value.chars().collect::<Vec<_>>();
        menu.prompt = Some(MenuPrompt {
            title: title.into(),
            cursor: value.len(),
            value,
            purpose,
        });
    }

    fn activate_create_transport_row(&mut self, menu: &mut MenuState) {
        let Some(row) = CreateTransportRow::from_index(menu.selected) else {
            return;
        };
        let Some(profile) = menu.create_transport.as_ref() else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        match row {
            CreateTransportRow::Name => Self::begin_create_prompt(
                menu,
                tr("menu.prompt.transport.name"),
                profile.name.clone(),
                MenuPromptPurpose::CreateTransport(CreateTransportPromptField::Name),
            ),
            CreateTransportRow::BaudRate => {
                let current = profile.baud_rate;
                let values = baud_rate_options(current);
                let selected = values
                    .iter()
                    .position(|value| *value == current)
                    .unwrap_or(0);
                Self::open_menu_choice(
                    menu,
                    MenuChoicePurpose::CreateTransportBaudRate,
                    values
                        .into_iter()
                        .map(|value| MenuChoiceOption {
                            label: value.to_string(),
                            value: MenuChoiceValue::Number(value),
                        })
                        .collect(),
                    selected,
                );
            }
            CreateTransportRow::DataBits => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateTransportDataBits,
                data_bits_options(),
                data_bits_index(profile.data_bits),
            ),
            CreateTransportRow::Parity => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateTransportParity,
                parity_options(),
                parity_index(profile.parity),
            ),
            CreateTransportRow::StopBits => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateTransportStopBits,
                stop_bits_options(),
                stop_bits_index(profile.stop_bits),
            ),
            CreateTransportRow::FlowControl => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateTransportFlowControl,
                flow_control_options(),
                flow_control_index(profile.flow_control),
            ),
            CreateTransportRow::Dtr | CreateTransportRow::Rts | CreateTransportRow::AutoOpen => {
                let (purpose, current) = match row {
                    CreateTransportRow::Dtr => (MenuChoicePurpose::CreateTransportDtr, profile.dtr),
                    CreateTransportRow::Rts => (MenuChoicePurpose::CreateTransportRts, profile.rts),
                    CreateTransportRow::AutoOpen => (
                        MenuChoicePurpose::CreateTransportAutoOpen,
                        profile.auto_open,
                    ),
                    _ => unreachable!(),
                };
                Self::open_menu_choice(menu, purpose, bool_options(), usize::from(current));
            }
            CreateTransportRow::Save => {
                let profile = profile.clone();
                if !valid_menu_name(&profile.name) {
                    menu.message = tr("menu.name.invalid").into();
                    return;
                }
                self.submit_menu_mutation(menu, MenuMutation::CreateTransport { profile });
            }
        }
    }

    fn activate_create_model_row(&mut self, menu: &mut MenuState) {
        let Some(row) = CreateModelRow::from_index(menu.selected) else {
            return;
        };
        let Some(profile) = menu.create_model.as_ref() else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        match row {
            CreateModelRow::Name => Self::begin_create_prompt(
                menu,
                tr("menu.prompt.device.name"),
                profile.name.clone(),
                MenuPromptPurpose::CreateModel(CreateModelPromptField::Name),
            ),
            CreateModelRow::ModelNames => Self::begin_create_prompt(
                menu,
                tr("menu.prompt.model.names"),
                profile.model_names.join(", "),
                MenuPromptPurpose::CreateModel(CreateModelPromptField::ModelNames),
            ),
            CreateModelRow::WriteEol => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateModelWriteEol,
                eol_options(),
                eol_index(profile.write_eol.as_deref()),
            ),
            CreateModelRow::Echo => Self::open_menu_choice(
                menu,
                MenuChoicePurpose::CreateModelEcho,
                echo_options(),
                echo_index(profile.echo),
            ),
            CreateModelRow::ShellPrompt
            | CreateModelRow::UbootPrompt
            | CreateModelRow::ChunkSize
            | CreateModelRow::ChunkDelay => {
                let (field, title, value) = match row {
                    CreateModelRow::ShellPrompt => (
                        CreateModelPromptField::ShellPrompt,
                        tr("menu.current.prompt.shell"),
                        profile.shell_prompt.clone().unwrap_or_default(),
                    ),
                    CreateModelRow::UbootPrompt => (
                        CreateModelPromptField::UbootPrompt,
                        tr("menu.current.prompt.uboot"),
                        profile.uboot_prompt.clone().unwrap_or_default(),
                    ),
                    CreateModelRow::ChunkSize => (
                        CreateModelPromptField::ChunkSize,
                        tr("menu.current.prompt.chunk"),
                        profile
                            .write_chunk_size
                            .map_or_else(String::new, |value| value.to_string()),
                    ),
                    CreateModelRow::ChunkDelay => (
                        CreateModelPromptField::ChunkDelay,
                        tr("menu.current.prompt.delay"),
                        profile
                            .write_chunk_delay_ms
                            .map_or_else(String::new, |value| value.to_string()),
                    ),
                    _ => unreachable!(),
                };
                Self::begin_create_prompt(
                    menu,
                    title,
                    value,
                    MenuPromptPurpose::CreateModel(field),
                );
            }
            CreateModelRow::Save => {
                let profile = profile.clone();
                if !valid_menu_name(&profile.name) {
                    menu.message = tr("menu.name.invalid").into();
                    return;
                }
                self.submit_menu_mutation(menu, MenuMutation::CreateModelProfile { profile });
            }
        }
    }

    fn begin_shared_profile_confirmation(
        &self,
        menu: &mut MenuState,
        impacts: SharedProfileImpacts,
        mutation: MenuMutation,
    ) {
        let mut lines = vec![tr("menu.profile.shared.warning").into()];
        if let Some(impact) = impacts.transport {
            lines.push(trf(
                "menu.profile.shared.transport",
                &[
                    &safe_inline(&impact.profile_name),
                    &impact.ports.len().to_string(),
                ],
            ));
            lines.extend(impact.ports.into_iter().map(|(port, display_name)| {
                trf(
                    "menu.profile.shared.port",
                    &[&safe_inline(&port), &safe_inline(&display_name)],
                )
            }));
        }
        if let Some(impact) = impacts.device {
            lines.push(trf(
                "menu.profile.shared.device",
                &[
                    &safe_inline(&impact.profile_name),
                    &impact.ports.len().to_string(),
                ],
            ));
            lines.extend(impact.ports.into_iter().map(|(port, display_name)| {
                trf(
                    "menu.profile.shared.port",
                    &[&safe_inline(&port), &safe_inline(&display_name)],
                )
            }));
        }
        lines.push(tr("menu.profile.shared.revision").into());
        menu.confirmation = Some(MenuConfirmation {
            title: tr("menu.profile.shared.title").into(),
            lines,
            scroll: 0,
            cancelled_message: tr("menu.profile.shared.cancelled").into(),
            action: MenuConfirmationAction::Mutation(mutation),
        });
        menu.message = tr("menu.profile.shared.pending").into();
    }

    fn submit_current_profile_updates(&mut self, menu: &mut MenuState) {
        let Some(editor) = menu.profile_editor.as_ref() else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        let port = editor.port_update();
        let transport_binding = editor.transport_binding_update();
        let transport = editor.transport_update();
        let model_profile_binding = editor.model_profile_binding_update();
        let model_name = editor.model_name_update();
        let device = editor.device_update();
        if port.is_none()
            && transport_binding.is_none()
            && transport.is_none()
            && model_profile_binding.is_none()
            && model_name.is_none()
            && device.is_none()
        {
            menu.message = tr("menu.current.no.changes").into();
            return;
        }
        let Some(catalog) = menu.catalog.as_ref() else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        let impacts = shared_profile_impacts(catalog, transport.as_ref(), device.as_ref());
        let has_shared_profile_update = transport.is_some() || device.is_some();
        let mutation = MenuMutation::UpdateCurrentProfiles(Box::new(CurrentProfileUpdate {
            current_port: self.selected_port(),
            new_port: port,
            transport_binding,
            transport,
            model_profile_binding,
            model_name,
            device,
            revisions: CurrentProfileRevisions {
                config: catalog.config_revision,
                transport: catalog.transport_revision,
                device: catalog.model_profile_revision,
            },
        }));
        if has_shared_profile_update {
            self.begin_shared_profile_confirmation(menu, impacts, mutation);
        } else {
            self.submit_menu_mutation(menu, mutation);
        }
    }

    fn activate_menu_item(&mut self, menu: &mut MenuState) {
        if menu.busy
            && !matches!(
                menu.page,
                MenuPage::Root
                    | MenuPage::Settings
                    | MenuPage::DisplaySettings
                    | MenuPage::McpSettings
            )
        {
            menu.message = tr("menu.busy").into();
            return;
        }
        match menu.page {
            MenuPage::Root => match menu.selected {
                0 => {
                    menu.push(MenuPage::Profiles);
                    self.refresh_current_profile_editor(menu);
                }
                1 => menu.push(MenuPage::CreateProfiles),
                2 => menu.push(MenuPage::Settings),
                3 => menu.push(MenuPage::Help),
                _ => {}
            },
            MenuPage::Profiles => self.activate_current_profile_row(menu),
            MenuPage::CreateProfiles => match menu.selected {
                0 => {
                    menu.create_transport = Some(default_transport_profile(String::new()));
                    menu.push(MenuPage::CreateTransportProfile);
                }
                1 => {
                    menu.create_model = Some(ModelProfile {
                        name: String::new(),
                        model_names: Vec::new(),
                        shell_prompt: None,
                        uboot_prompt: None,
                        write_eol: Some("\r".into()),
                        echo: Some(EchoMode::Auto),
                        write_chunk_size: None,
                        write_chunk_delay_ms: None,
                    });
                    menu.push(MenuPage::CreateModelProfile);
                }
                _ => {}
            },
            MenuPage::CreateTransportProfile => self.activate_create_transport_row(menu),
            MenuPage::CreateModelProfile => self.activate_create_model_row(menu),
            MenuPage::Settings => match menu.selected {
                0 => menu.push(MenuPage::DisplaySettings),
                1 => menu.push(MenuPage::McpSettings),
                _ => {}
            },
            MenuPage::ModelFamilies => {
                let profile = menu
                    .catalog
                    .as_ref()
                    .and_then(|catalog| {
                        catalog
                            .model_profiles
                            .iter()
                            .filter(|profile| !profile.model_names.is_empty())
                            .nth(menu.selected)
                    })
                    .map(|profile| profile.name.clone());
                if let Some(profile) = profile {
                    menu.model_family = Some(profile);
                    menu.push(MenuPage::ModelNames);
                }
            }
            MenuPage::ModelNames => {
                let selection = menu.catalog.as_ref().and_then(|catalog| {
                    let family = menu.model_family.as_deref()?;
                    let profile = catalog
                        .model_profiles
                        .iter()
                        .find(|profile| profile.name == family)?;
                    let name = profile.model_names.get(menu.selected)?.clone();
                    Some((profile.clone(), name))
                });
                if let Some((profile, name)) = selection
                    && let Some(editor) = menu.profile_editor.as_mut()
                {
                    editor.model_profile_binding = Some(profile.name.clone());
                    editor.device = profile;
                    editor.model_name = Some(name);
                    while menu.page != MenuPage::Profiles && menu.back() {}
                    menu.message = tr("menu.current.modified").into();
                }
            }
            MenuPage::DisplaySettings => {
                let value = self.agent_history_rows.to_string();
                menu.prompt = Some(MenuPrompt {
                    title: trf(
                        "menu.display.history.prompt",
                        &[
                            &MIN_AGENT_HISTORY_ROWS.to_string(),
                            &MAX_AGENT_HISTORY_ROWS.to_string(),
                        ],
                    ),
                    cursor: value.len(),
                    value: value.chars().collect(),
                    purpose: MenuPromptPurpose::AgentHistoryRows,
                });
            }
            MenuPage::McpSettings => self.begin_orphan_run_timeout_prompt(menu),
            MenuPage::Help => {}
        }
    }

    fn reconcile_configured_ports(&mut self, fresh: &[SlotSnapshot], preferred_port: &str) -> bool {
        let previous_ports = self
            .ports
            .iter()
            .map(|view| view.snapshot.config.port.clone())
            .collect::<Vec<_>>();
        let configured_ports = fresh
            .iter()
            .map(|slot| slot.config.port.clone())
            .collect::<Vec<_>>();
        let port_set_changed = previous_ports != configured_ports;
        let mut previous = std::mem::take(&mut self.ports);
        self.ports = fresh
            .iter()
            .cloned()
            .map(|slot| {
                let port = slot.config.port.clone();
                if let Some(index) = previous
                    .iter()
                    .position(|view| view.snapshot.config.port == port)
                {
                    let mut view = previous.swap_remove(index);
                    let configuration_changed = view.snapshot.config != slot.config;
                    view.snapshot = slot;
                    view.sync_trigger_projection(false);
                    view.sync_active_run_history();
                    if configuration_changed {
                        view.follow();
                    }
                    view
                } else {
                    SlotView::new(slot)
                }
            })
            .collect();
        self.selected = self
            .ports
            .iter()
            .position(|view| view.snapshot.config.port == preferred_port)
            .unwrap_or_else(|| self.selected.min(self.ports.len().saturating_sub(1)));
        let configured = configured_ports
            .iter()
            .collect::<std::collections::HashSet<_>>();
        self.pending_writes
            .retain(|port, _| configured.contains(port));
        self.inflight_writes
            .retain(|port, _| configured.contains(port));
        self.queued_controls
            .retain(|port, _| configured.contains(port));
        self.pending_requests.retain(|_, request| match request {
            PendingRequest::Acquire { port, .. }
            | PendingRequest::Renew { port }
            | PendingRequest::Release { port }
            | PendingRequest::CancelAcquire { port }
            | PendingRequest::Write { port, .. } => configured.contains(port),
        });
        port_set_changed
    }

    fn handle_menu_io_event(
        &mut self,
        event: MenuIoEvent,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        match event {
            MenuIoEvent::Completed { catalog, success } => {
                let current_port = self.selected_port();
                let preferred_port = match &success {
                    MenuSuccess::ProfilesUpdated {
                        previous_port,
                        configured_port,
                    } if current_port == *previous_port => configured_port.as_str(),
                    _ => current_port.as_str(),
                };
                if self.reconcile_configured_ports(&catalog.ports, preferred_port) {
                    let ports = self
                        .ports
                        .iter()
                        .map(|view| view.snapshot.config.port.clone())
                        .collect();
                    if commands
                        .try_send(NetworkCommand::Reconfigure { ports })
                        .is_err()
                    {
                        tracing::warn!("failed to reconnect after configured Port set changed");
                    }
                }
                let profile_editor = CurrentProfileEditor::new(self.current(), &catalog);
                let message = menu_success_message(&success);
                if let Some(menu) = self.menu.as_mut() {
                    menu.catalog = Some(catalog);
                    menu.profile_editor = Some(profile_editor);
                    menu.busy = false;
                    menu.message = message.clone();
                    let count = menu_item_count(menu);
                    menu.selected = menu.selected.min(count.saturating_sub(1));
                }
                self.status = message;
            }
            MenuIoEvent::Failed(error) => {
                let message = trf("menu.io.failed", &[&safe_inline(&error)]);
                if let Some(menu) = self.menu.as_mut() {
                    menu.busy = false;
                    menu.message = message.clone();
                }
                self.status = message;
            }
        }
        self.dirty = true;
    }

    fn handle_monitor_io_event(&mut self, event: MonitorIoEvent) {
        match event {
            MonitorIoEvent::Snapshot(entries) => {
                for view in &mut self.ports {
                    let mut matching = entries
                        .iter()
                        .filter(|entry| entry.monitor.spec.port == view.snapshot.config.port)
                        .cloned()
                        .collect::<VecDeque<_>>();
                    while matching.len() > MAX_MONITORS_PER_SLOT {
                        matching.pop_front();
                    }
                    view.monitor_history = matching;
                    if view
                        .selected_monitor
                        .is_some_and(|id| view.monitor(id).is_none())
                    {
                        view.selected_monitor = None;
                        view.expanded_monitor = None;
                        view.selected_monitor_matcher = None;
                        view.selected_monitor_incident = None;
                    }
                }
                self.dirty = true;
            }
            MonitorIoEvent::Failed(error) => {
                tracing::debug!(%error, "Monitor history refresh failed");
            }
        }
    }

    /// Ctrl-] g: switch between English and Chinese at runtime and persist
    /// the choice to the client config on a best-effort basis.
    fn toggle_language(&mut self) {
        for slot in &mut self.ports {
            slot.follow();
        }
        let next = i18n::lang().toggled();
        i18n::set_lang(next);
        if let Some(loaded) = &mut self.config {
            loaded.config.language = Some(next);
            if let Err(error) = loaded.save() {
                tracing::warn!(%error, "failed to persist the language preference");
            }
        }
        self.status = trf(
            "st.language",
            &[match next {
                i18n::Lang::En => "English",
                i18n::Lang::Zh => "中文",
            }],
        );
    }

    fn complete_draft(&mut self) {
        let view = self.current_mut();
        if let Some(completion) = &mut view.completion {
            completion.current = (completion.current + 1) % completion.candidates.len();
            let candidate = completion.candidates[completion.current].clone();
            view.draft = candidate.chars().collect();
            view.draft_cursor = view.draft.len();
            return;
        }
        let prefix = view.draft.iter().collect::<String>();
        let mut seen = std::collections::HashSet::new();
        let candidates = view
            .history
            .iter()
            .rev()
            .filter(|entry| entry.starts_with(&prefix))
            .filter(|entry| seen.insert((*entry).clone()))
            .cloned()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return;
        }
        let first = candidates[0].clone();
        view.completion = Some(Completion {
            candidates,
            current: 0,
        });
        view.draft = first.chars().collect();
        view.draft_cursor = view.draft.len();
    }

    fn scroll_up(&mut self, amount: usize) {
        if self.current().scroll_snapshot.is_none() {
            let Some(layout) = self.layout else {
                // Before the first draw there is no trustworthy visual width.
                // Retain the old bounded behavior for tests/recovery only.
                let max = self.current().logical_line_count().saturating_sub(1);
                let view = self.current_mut();
                view.scroll_from_bottom = (view.scroll_from_bottom + amount).min(max);
                return;
            };
            let rows = all_output_visual_lines(self, layout.output_inner.width);
            let max = rows
                .len()
                .saturating_sub(layout.output_inner.height as usize);
            if max == 0 {
                self.current_mut().follow();
                return;
            }
            let view = self.current_mut();
            view.scroll_snapshot = Some(ScrollSnapshot { rows });
            view.scroll_from_bottom = 0;
        }

        let visible_height = self
            .layout
            .map_or(0, |layout| layout.output_inner.height as usize);
        let max = self
            .current()
            .scroll_snapshot
            .as_ref()
            .map_or(0, |snapshot| {
                snapshot.rows.len().saturating_sub(visible_height)
            });
        let view = self.current_mut();
        view.scroll_from_bottom = (view.scroll_from_bottom + amount).min(max);
        if view.scroll_from_bottom == 0 {
            view.follow();
        }
    }

    fn scroll_down(&mut self, amount: usize) {
        let view = self.current_mut();
        view.scroll_from_bottom = view.scroll_from_bottom.saturating_sub(amount);
        if view.scroll_from_bottom == 0 {
            view.follow();
        }
    }
}

struct MenuIo {
    commands: mpsc::Sender<MenuIoCommand>,
    events: mpsc::Receiver<MenuIoEvent>,
}

struct MonitorIo {
    events: mpsc::Receiver<MonitorIoEvent>,
}

enum MonitorIoEvent {
    Snapshot(Vec<MonitorHistoryEntry>),
    Failed(String),
}

struct OutputSearchIo {
    commands: mpsc::Sender<OutputSearchIoCommand>,
    events: mpsc::Receiver<OutputSearchIoEvent>,
}

struct ExactEvidenceIo {
    commands: mpsc::Sender<ExactEvidenceIoCommand>,
    events: mpsc::Receiver<ExactEvidenceIoEvent>,
}

#[derive(Debug, Clone, Copy)]
struct OutputSearchArchive {
    epoch: Uuid,
    first_seq: u64,
    last_seq: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputSearchPageProgress {
    Complete,
    Continue(u64),
    Incomplete,
}

fn output_search_page_progress(
    truncated: bool,
    next_cursor: Option<&Cursor>,
    epoch: Uuid,
    after_seq: u64,
    through_seq: u64,
) -> OutputSearchPageProgress {
    let Some(cursor) =
        next_cursor.filter(|cursor| cursor.epoch == epoch && cursor.after_seq > after_seq)
    else {
        return OutputSearchPageProgress::Incomplete;
    };
    if cursor.after_seq >= through_seq {
        OutputSearchPageProgress::Complete
    } else if truncated {
        OutputSearchPageProgress::Continue(cursor.after_seq)
    } else {
        // A clean page ending before the requested Snapshot/archive head means
        // that tail is not currently queryable (for example H is live while
        // only K is durable, or an archive changed during discovery). Do not
        // poll the same empty tail; expose the result as partial instead.
        OutputSearchPageProgress::Incomplete
    }
}

fn begin_output_search_archive(query_count: usize, scanned_archives: &mut usize) -> bool {
    if query_count >= OUTPUT_SEARCH_HTTP_QUERY_LIMIT {
        return false;
    }
    *scanned_archives += 1;
    true
}

fn retain_newest_output_search_events(
    events: &mut Vec<TimelineEvent>,
    epoch_ranks: &HashMap<Uuid, usize>,
) -> bool {
    events.sort_by(|left, right| {
        let left_rank = epoch_ranks
            .get(&left.daemon_epoch)
            .copied()
            .unwrap_or(usize::MAX);
        let right_rank = epoch_ranks
            .get(&right.daemon_epoch)
            .copied()
            .unwrap_or(usize::MAX);
        left_rank.cmp(&right_rank).then_with(|| {
            if left.daemon_epoch == right.daemon_epoch {
                right.seq.cmp(&left.seq)
            } else {
                left.daemon_epoch.cmp(&right.daemon_epoch)
            }
        })
    });
    events.dedup_by(|left, right| left.daemon_epoch == right.daemon_epoch && left.seq == right.seq);
    let limited = events.len() > OUTPUT_SEARCH_LIMIT_EVENTS;
    events.truncate(OUTPUT_SEARCH_LIMIT_EVENTS);
    limited
}

impl From<&ArchiveSummary> for OutputSearchArchive {
    fn from(archive: &ArchiveSummary) -> Self {
        Self {
            epoch: archive.epoch,
            first_seq: archive.first_seq,
            last_seq: archive.last_seq,
        }
    }
}

fn spawn_output_search_io(api: ApiClient) -> OutputSearchIo {
    let (command_tx, mut command_rx) = mpsc::channel(4);
    let (event_tx, event_rx) = mpsc::channel(2);
    tokio::spawn(async move {
        let mut active: Option<(Uuid, tokio::task::JoinHandle<OutputSearchIoEvent>)> = None;
        loop {
            if let Some((active_id, mut task)) = active.take() {
                tokio::select! {
                    result = &mut task => {
                        let event = result.unwrap_or_else(|error| OutputSearchIoEvent::Failed {
                            request_id: active_id,
                            message: format!("history-search worker stopped: {error}"),
                        });
                        if event_tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    command = command_rx.recv() => match command {
                        Some(OutputSearchIoCommand::Query(request)) => {
                            task.abort();
                            let request_id = request.request_id;
                            active = Some((request_id, spawn_output_search_task(api.clone(), request)));
                        }
                        Some(OutputSearchIoCommand::Cancel { request_id }) if request_id == active_id => {
                            task.abort();
                        }
                        Some(OutputSearchIoCommand::Cancel { .. }) => {
                            active = Some((active_id, task));
                        }
                        None => {
                            task.abort();
                            break;
                        }
                    }
                }
                continue;
            }
            match command_rx.recv().await {
                Some(OutputSearchIoCommand::Query(request)) => {
                    let request_id = request.request_id;
                    active = Some((request_id, spawn_output_search_task(api.clone(), request)));
                }
                Some(OutputSearchIoCommand::Cancel { .. }) => {}
                None => break,
            }
        }
    });
    OutputSearchIo {
        commands: command_tx,
        events: event_rx,
    }
}

fn spawn_output_search_task(
    api: ApiClient,
    request: OutputSearchRequest,
) -> tokio::task::JoinHandle<OutputSearchIoEvent> {
    tokio::spawn(async move {
        let request_id = request.request_id;
        match tokio::time::timeout(OUTPUT_SEARCH_DEADLINE, execute_output_search(&api, request))
            .await
        {
            Ok(Ok(response)) => OutputSearchIoEvent::Completed {
                request_id,
                response,
            },
            Ok(Err(error)) => OutputSearchIoEvent::Failed {
                request_id,
                message: error.to_string(),
            },
            Err(_) => OutputSearchIoEvent::Failed {
                request_id,
                message: trf(
                    "ui.output.search.timeout",
                    &[&OUTPUT_SEARCH_DEADLINE.as_secs().to_string()],
                ),
            },
        }
    })
}

async fn execute_output_search(
    api: &ApiClient,
    request: OutputSearchRequest,
) -> Result<OutputSearchResponse> {
    let mut partial = false;
    let mut archives = match request.scope {
        OutputSearchScope::CurrentEpoch => vec![OutputSearchArchive {
            epoch: request.current_epoch,
            first_seq: 1,
            last_seq: request.head_seq,
        }],
        OutputSearchScope::CurrentRun => {
            let run = request
                .current_run
                .context("the selected Port no longer has an active Agent Run")?;
            vec![OutputSearchArchive {
                epoch: request.current_epoch,
                first_seq: run.start_seq,
                last_seq: run.through_seq,
            }]
        }
        OutputSearchScope::Retained => {
            let catalog = api.archives(Some(&request.port)).await?;
            partial |= catalog.truncated;
            let mut retained = catalog.archives;
            if retained.len() > OUTPUT_SEARCH_ARCHIVE_LIMIT {
                retained.truncate(OUTPUT_SEARCH_ARCHIVE_LIMIT);
                partial = true;
            }
            retained.iter().map(OutputSearchArchive::from).collect()
        }
    };
    // Synthetic current-epoch ranges have no records when the head is zero.
    archives.retain(|archive| archive.last_seq > 0 && archive.first_seq <= archive.last_seq);
    // Wall clocks can jump backwards. Archive catalog order establishes the
    // cross-epoch recency relation for this query; sequence number establishes
    // recency within one epoch.
    let epoch_ranks = archives
        .iter()
        .enumerate()
        .map(|(rank, archive)| (archive.epoch, rank))
        .collect::<HashMap<_, _>>();

    let mut events = Vec::new();
    let mut gaps = Vec::new();
    let archive_count = archives.len();
    let mut scanned_archives = 0usize;
    let mut query_count = 0usize;
    'archives: for (archive_index, archive) in archives.into_iter().enumerate() {
        // An archive is "scanned" only once its first HTTP request can
        // actually be issued. Reaching the global request cap between
        // archives must not count the untouched next archive.
        if !begin_output_search_archive(query_count, &mut scanned_archives) {
            partial = true;
            break;
        }
        let window_first = archive.first_seq.max(
            archive
                .last_seq
                .saturating_sub(OUTPUT_SEARCH_EVENT_WINDOW - 1),
        );
        partial |= window_first > archive.first_seq;
        // The journal API is forward-only. Page every selected direction to
        // the end of this bounded tail, then keep only the newest matches.
        // Round-robin paging prevents a chatty RX stream from starving TX.
        let mut directions = request
            .direction
            .query_directions()
            .iter()
            .copied()
            .map(|direction| (direction, window_first.saturating_sub(1), false))
            .collect::<Vec<_>>();
        while directions.iter().any(|(_, _, done)| !done) {
            for (direction, after_seq, done) in &mut directions {
                if *done {
                    continue;
                }
                if query_count >= OUTPUT_SEARCH_HTTP_QUERY_LIMIT {
                    partial = true;
                    break 'archives;
                }
                query_count += 1;
                let response = api
                    .events(
                        &request.port,
                        &EventQuery {
                            epoch: Some(archive.epoch),
                            after_seq: Some(*after_seq),
                            through_seq: Some(archive.last_seq),
                            before_wall_time_ns: None,
                            after_wall_time_ns: None,
                            direction: Some(*direction),
                            kind: None,
                            actor_id: None,
                            run_id: if request.scope == OutputSearchScope::CurrentRun {
                                request.current_run.map(|run| run.id)
                            } else {
                                None
                            },
                            operation_id: None,
                            contains: request.contains.clone(),
                            regex: request.regex.clone(),
                            limit_events: Some(OUTPUT_SEARCH_PAGE_EVENTS),
                            limit_bytes: Some(OUTPUT_SEARCH_LIMIT_BYTES),
                        },
                    )
                    .await?;
                for gap in response.gaps {
                    if !gaps.contains(&gap) {
                        gaps.push(gap);
                    }
                }
                events.extend(response.events);
                partial |= retain_newest_output_search_events(&mut events, &epoch_ranks);
                match output_search_page_progress(
                    response.truncated,
                    response.next_cursor.as_ref(),
                    archive.epoch,
                    *after_seq,
                    archive.last_seq,
                ) {
                    OutputSearchPageProgress::Complete => *done = true,
                    OutputSearchPageProgress::Continue(next) => *after_seq = next,
                    OutputSearchPageProgress::Incomplete => {
                        partial = true;
                        *done = true;
                    }
                }
            }
        }
        if events.len() >= OUTPUT_SEARCH_LIMIT_EVENTS {
            partial |= archive_index + 1 < archive_count;
            break;
        }
    }

    partial |= retain_newest_output_search_events(&mut events, &epoch_ranks);
    if gaps.len() > 32 {
        gaps.truncate(32);
        partial = true;
    }
    Ok(OutputSearchResponse {
        events,
        gaps,
        partial,
        scanned_archives,
    })
}

fn exact_evidence_is_complete(
    port: &str,
    daemon_epoch: Uuid,
    seq_start: u64,
    seq_end: u64,
    events: &[TimelineEvent],
) -> bool {
    if seq_start == 0 || seq_start > seq_end || events.is_empty() {
        return false;
    }
    if events.first().is_none_or(|event| {
        event.port != port || event.daemon_epoch != daemon_epoch || event.seq != seq_start
    }) || events.last().is_none_or(|event| {
        event.port != port || event.daemon_epoch != daemon_epoch || event.seq != seq_end
    }) {
        return false;
    }
    events.windows(2).all(|pair| {
        pair[0].port == port
            && pair[1].port == port
            && pair[0].daemon_epoch == daemon_epoch
            && pair[1].daemon_epoch == daemon_epoch
            && pair[0].seq.checked_add(1) == Some(pair[1].seq)
    })
}

fn incident_evidence_is_complete(
    target: &IncidentEvidenceTarget,
    events: &[TimelineEvent],
) -> bool {
    exact_evidence_is_complete(
        &target.port,
        target.daemon_epoch,
        target.seq_start,
        target.seq_end,
        events,
    )
}

fn command_evidence_end_seq(
    target: &CommandEvidenceTarget,
    events: &[TimelineEvent],
) -> Option<u64> {
    if target.matchers.is_empty() {
        return exact_evidence_is_complete(
            &target.port,
            target.daemon_epoch,
            target.seq_start,
            target.write_end_seq,
            events,
        )
        .then_some(target.write_end_seq);
    }
    let entries = project_incident_evidence(events);
    let entries = entries.iter().collect::<Vec<_>>();
    let capture = command_capture_for_target(target, &entries);
    capture
        .end
        .and_then(|index| entries.get(index))
        .map(|entry| entry.seq)
}

fn first_exact_evidence_gap(
    target: &ExactEvidenceTarget,
    through_seq: u64,
    events: &[TimelineEvent],
) -> Option<GapRange> {
    let mut expected = target.seq_start();
    for event in events.iter().take_while(|event| event.seq <= through_seq) {
        if event.seq > expected {
            return Some(GapRange {
                epoch: target.daemon_epoch(),
                first_seq: expected,
                last_seq: event.seq.saturating_sub(1),
                reason: serial_protocol::GapReason::SequenceDiscontinuity,
            });
        }
        expected = event.seq.saturating_add(1);
    }
    (expected <= through_seq).then_some(GapRange {
        epoch: target.daemon_epoch(),
        first_seq: expected,
        last_seq: through_seq,
        reason: serial_protocol::GapReason::SequenceDiscontinuity,
    })
}

fn spawn_exact_evidence_io(api: ApiClient) -> ExactEvidenceIo {
    let (command_tx, mut command_rx) = mpsc::channel(4);
    let (event_tx, event_rx) = mpsc::channel(2);
    tokio::spawn(async move {
        let mut active: Option<(
            Uuid,
            ExactEvidenceTarget,
            tokio::task::JoinHandle<ExactEvidenceIoEvent>,
        )> = None;
        loop {
            if let Some((active_id, active_target, mut task)) = active.take() {
                tokio::select! {
                    result = &mut task => {
                        let event = result.unwrap_or_else(|error| ExactEvidenceIoEvent::Failed {
                            request_id: active_id,
                            target: active_target,
                            failure: ExactEvidenceFailure::QueryFailed(
                                format!("Exact evidence worker stopped: {error}"),
                            ),
                        });
                        if event_tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    command = command_rx.recv() => match command {
                        Some(ExactEvidenceIoCommand::Query(request)) => {
                            task.abort();
                            let request_id = request.request_id;
                            let target = request.target.clone();
                            active = Some((request_id, target, spawn_exact_evidence_task(api.clone(), request)));
                        }
                        None => {
                            task.abort();
                            break;
                        }
                    }
                }
                continue;
            }
            match command_rx.recv().await {
                Some(ExactEvidenceIoCommand::Query(request)) => {
                    let request_id = request.request_id;
                    let target = request.target.clone();
                    active = Some((
                        request_id,
                        target,
                        spawn_exact_evidence_task(api.clone(), request),
                    ));
                }
                None => break,
            }
        }
    });
    ExactEvidenceIo {
        commands: command_tx,
        events: event_rx,
    }
}

fn spawn_exact_evidence_task(
    api: ApiClient,
    request: ExactEvidenceRequest,
) -> tokio::task::JoinHandle<ExactEvidenceIoEvent> {
    tokio::spawn(async move {
        let request_id = request.request_id;
        let target = request.target.clone();
        match tokio::time::timeout(
            EXACT_EVIDENCE_DEADLINE,
            execute_exact_evidence_query(&api, request.target),
        )
        .await
        {
            Ok(Ok(response)) => ExactEvidenceIoEvent::Completed {
                request_id,
                response,
            },
            Ok(Err(failure)) => ExactEvidenceIoEvent::Failed {
                request_id,
                target,
                failure,
            },
            Err(_) => ExactEvidenceIoEvent::Failed {
                request_id,
                target,
                failure: ExactEvidenceFailure::QueryFailed(format!(
                    "query exceeded {} seconds",
                    EXACT_EVIDENCE_DEADLINE.as_secs()
                )),
            },
        }
    })
}

async fn execute_exact_evidence_query(
    api: &ApiClient,
    target: ExactEvidenceTarget,
) -> std::result::Result<ExactEvidenceResponse, ExactEvidenceFailure> {
    let seq_start = target.seq_start();
    let query_end_seq = target.query_end_seq();
    if seq_start == 0 || seq_start > query_end_seq {
        return Err(ExactEvidenceFailure::Incomplete);
    }
    let mut events = Vec::new();
    let mut reported_gaps = Vec::new();
    let mut data_bytes = 0usize;
    let mut after_seq = seq_start.saturating_sub(1);
    for _ in 0..EXACT_EVIDENCE_HTTP_QUERY_LIMIT {
        let response = api
            .events(
                target.port(),
                &EventQuery {
                    epoch: Some(target.daemon_epoch()),
                    after_seq: Some(after_seq),
                    through_seq: Some(query_end_seq),
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: None,
                    kind: None,
                    actor_id: None,
                    run_id: None,
                    operation_id: None,
                    contains: None,
                    regex: None,
                    limit_events: Some(EXACT_EVIDENCE_PAGE_EVENTS),
                    limit_bytes: Some(EXACT_EVIDENCE_PAGE_BYTES),
                },
            )
            .await
            .map_err(|error| ExactEvidenceFailure::QueryFailed(error.to_string()))?;
        reported_gaps.extend(
            response
                .gaps
                .iter()
                .filter(|gap| {
                    gap.epoch == target.daemon_epoch()
                        && gap.first_seq <= query_end_seq
                        && gap.last_seq >= seq_start
                })
                .cloned(),
        );
        if let Some(first_available) = response.first_available_seq
            && first_available > seq_start
        {
            return Err(ExactEvidenceFailure::Gap(GapRange {
                epoch: target.daemon_epoch(),
                first_seq: seq_start,
                last_seq: first_available.saturating_sub(1).min(query_end_seq),
                reason: serial_protocol::GapReason::Retention,
            }));
        }
        for event in response.events {
            if event.port != target.port()
                || event.daemon_epoch != target.daemon_epoch()
                || event.seq <= after_seq
                || event.seq < seq_start
                || event.seq > query_end_seq
            {
                return Err(ExactEvidenceFailure::Incomplete);
            }
            if event.kind == EventKind::Gap {
                let reason = event
                    .metadata
                    .get("reason")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                    .unwrap_or(serial_protocol::GapReason::SequenceDiscontinuity);
                reported_gaps.push(GapRange {
                    epoch: target.daemon_epoch(),
                    first_seq: event.seq,
                    last_seq: event.seq,
                    reason,
                });
            }
            data_bytes = data_bytes.saturating_add(event.data.len());
            events.push(event);
        }
        events.sort_by_key(|event| event.seq);
        events.dedup_by_key(|event| event.seq);
        if events.len() > EXACT_EVIDENCE_MAX_EVENTS || data_bytes > EXACT_EVIDENCE_MAX_BYTES {
            return Err(ExactEvidenceFailure::LimitExceeded);
        }
        let completed_through = match &target {
            ExactEvidenceTarget::Incident(target) => {
                incident_evidence_is_complete(target, &events).then_some(target.seq_end)
            }
            ExactEvidenceTarget::Command(target) => command_evidence_end_seq(target, &events),
        };
        if let Some(completed_through) = completed_through {
            if let Some(gap) = reported_gaps
                .iter()
                .find(|gap| gap.first_seq <= completed_through && gap.last_seq >= seq_start)
            {
                return Err(ExactEvidenceFailure::Gap(gap.clone()));
            }
            let completed_len = events.partition_point(|event| event.seq <= completed_through);
            if !exact_evidence_is_complete(
                target.port(),
                target.daemon_epoch(),
                seq_start,
                completed_through,
                &events[..completed_len],
            ) {
                return Err(ExactEvidenceFailure::Gap(
                    first_exact_evidence_gap(&target, completed_through, &events).unwrap_or(
                        GapRange {
                            epoch: target.daemon_epoch(),
                            first_seq: seq_start,
                            last_seq: completed_through,
                            reason: serial_protocol::GapReason::SequenceDiscontinuity,
                        },
                    ),
                ));
            }
            events.retain(|event| event.seq <= completed_through);
            return Ok(ExactEvidenceResponse { target, events });
        }
        let Some(cursor) = response.next_cursor.filter(|cursor| {
            cursor.epoch == target.daemon_epoch()
                && cursor.after_seq > after_seq
                && cursor.after_seq < query_end_seq
        }) else {
            if let Some(gap) = reported_gaps.first() {
                return Err(ExactEvidenceFailure::Gap(gap.clone()));
            }
            return Err(ExactEvidenceFailure::Incomplete);
        };
        if !response.truncated {
            if let Some(gap) = reported_gaps.first() {
                return Err(ExactEvidenceFailure::Gap(gap.clone()));
            }
            return Err(ExactEvidenceFailure::Incomplete);
        }
        after_seq = cursor.after_seq;
    }
    Err(ExactEvidenceFailure::LimitExceeded)
}

fn spawn_menu_io(api: ApiClient) -> MenuIo {
    let (command_tx, mut command_rx) = mpsc::channel(8);
    let (event_tx, event_rx) = mpsc::channel(8);
    tokio::spawn(async move {
        while let Some(command) = command_rx.recv().await {
            let event = match execute_menu_io(&api, command).await {
                Ok((catalog, success)) => MenuIoEvent::Completed { catalog, success },
                Err(error) => MenuIoEvent::Failed(error.to_string()),
            };
            if event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
    MenuIo {
        commands: command_tx,
        events: event_rx,
    }
}

async fn execute_menu_io(
    api: &ApiClient,
    command: MenuIoCommand,
) -> Result<(MenuCatalog, MenuSuccess)> {
    let success = match command {
        MenuIoCommand::Reload => MenuSuccess::Loaded,
        MenuIoCommand::Mutation { mutation } => execute_menu_mutation(api, *mutation).await?,
    };
    Ok((load_menu_catalog(api).await?, success))
}

async fn execute_menu_mutation(api: &ApiClient, mutation: MenuMutation) -> Result<MenuSuccess> {
    match mutation {
        MenuMutation::CreateTransport { profile } => {
            let profile_name = profile.name.clone();
            let mut catalog = api.transport_profiles().await?;
            if catalog
                .profiles
                .iter()
                .any(|existing| existing.name == profile.name)
            {
                bail!(trf("menu.profile.exists", &[&profile.name]));
            }
            catalog.profiles.push(profile);
            api.configure_transport_profiles(catalog.profiles, catalog.config_revision)
                .await?;
            Ok(MenuSuccess::TransportCreated(profile_name))
        }
        MenuMutation::CreateModelProfile { profile } => {
            let profile_name = profile.name.clone();
            let mut catalog = api.model_profiles().await?;
            if catalog
                .profiles
                .iter()
                .any(|existing| existing.name == profile.name)
            {
                bail!(trf("menu.profile.exists", &[&profile.name]));
            }
            catalog.profiles.push(profile);
            api.configure_model_profiles(catalog.profiles, catalog.config_revision)
                .await?;
            Ok(MenuSuccess::ModelProfileCreated(profile_name))
        }
        MenuMutation::UpdateCurrentProfiles(update) => {
            let previous_port = update.current_port.clone();
            let configured_port = update
                .new_port
                .clone()
                .unwrap_or_else(|| previous_port.clone());
            update_current_profiles(api, *update).await?;
            Ok(MenuSuccess::ProfilesUpdated {
                previous_port,
                configured_port,
            })
        }
    }
}

async fn update_current_profiles(api: &ApiClient, update: CurrentProfileUpdate) -> Result<()> {
    let CurrentProfileUpdate {
        current_port,
        new_port,
        transport_binding,
        transport,
        model_profile_binding,
        model_name,
        device,
        revisions,
    } = update;
    if transport.is_some() {
        ensure!(
            revisions.transport == revisions.config,
            "{}",
            tr("menu.profile.revision.conflict")
        );
    }
    if device.is_some() {
        ensure!(
            revisions.device == revisions.config,
            "{}",
            tr("menu.profile.revision.conflict")
        );
    }
    let status = api.configuration_status().await?;
    ensure!(
        status.config_revision == revisions.config,
        "{}",
        tr("menu.profile.revision.conflict")
    );
    let slot = status
        .ports
        .iter()
        .find(|slot| slot.config.port == current_port)
        .with_context(|| trf("menu.port.missing", &[&current_port]))?;
    if let Some(profile) = transport.as_ref() {
        ensure!(
            slot.config.transport_profile.as_deref() == Some(profile.name.as_str()),
            "{}",
            tr("menu.profile.binding.changed")
        );
    }
    if let Some(profile) = device.as_ref() {
        ensure!(
            slot.config.model_profile.as_deref() == Some(profile.name.as_str()),
            "{}",
            tr("menu.profile.binding.changed")
        );
    }

    let mut known_revision = revisions.config;
    if let Some(profile) = transport.as_ref() {
        let mut catalog = api.transport_profiles().await?;
        ensure!(
            catalog.config_revision == revisions.transport
                && catalog.config_revision == known_revision,
            "{}",
            tr("menu.profile.revision.conflict")
        );
        let existing = catalog
            .profiles
            .iter_mut()
            .find(|existing| existing.name == profile.name)
            .with_context(|| trf("menu.transport.missing", &[&profile.name]))?;
        *existing = profile.clone();
        let updated = api
            .configure_transport_profiles(catalog.profiles, catalog.config_revision)
            .await?;
        known_revision = updated.config_revision;
    }
    if let Some(profile) = device.as_ref() {
        let mut catalog = api.model_profiles().await?;
        ensure!(
            catalog.config_revision == known_revision,
            "{}",
            tr("menu.profile.revision.conflict")
        );
        let existing = catalog
            .profiles
            .iter_mut()
            .find(|existing| existing.name == profile.name)
            .with_context(|| trf("menu.device.missing", &[&profile.name]))?;
        *existing = profile.clone();
        let updated = api
            .configure_model_profiles(catalog.profiles, catalog.config_revision)
            .await?;
        known_revision = updated.config_revision;
    }
    if new_port.is_some()
        || transport_binding.is_some()
        || model_profile_binding.is_some()
        || model_name.is_some()
    {
        let status = api.configuration_status().await?;
        ensure!(
            status.config_revision == known_revision,
            "{}",
            tr("menu.profile.revision.conflict")
        );
        let mut ports = status
            .ports
            .into_iter()
            .map(|slot| slot.config)
            .collect::<Vec<_>>();
        let slot = ports
            .iter_mut()
            .find(|slot| slot.port == current_port)
            .with_context(|| trf("menu.port.missing", &[&current_port]))?;
        if let Some(new_port) = new_port {
            slot.port = new_port;
        }
        if let Some(binding) = transport_binding {
            slot.transport_profile = binding;
        }
        if let Some(binding) = model_profile_binding {
            slot.model_profile = binding;
            if slot.model_profile.is_none() {
                slot.model_name = None;
            }
        }
        if let Some(model_name) = model_name {
            slot.model_name = model_name;
        }
        api.configure_ports(ports, status.config_revision).await?;
    }
    Ok(())
}

async fn load_menu_catalog(api: &ApiClient) -> Result<MenuCatalog> {
    let (status, detected_ports, transport, model) = tokio::try_join!(
        api.configuration_status(),
        api.ports(),
        api.transport_profiles(),
        api.model_profiles(),
    )?;
    Ok(MenuCatalog {
        ports: status.ports,
        detected_ports,
        config_revision: status.config_revision,
        transport_profiles: transport.profiles,
        transport_revision: transport.config_revision,
        model_profiles: model.profiles,
        model_profile_revision: model.config_revision,
    })
}

fn spawn_monitor_io(api: ApiClient) -> MonitorIo {
    let (event_tx, event_rx) = mpsc::channel(2);
    tokio::spawn(async move {
        let mut retained = HashMap::<Uuid, MonitorHistoryEntry>::new();
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let result = refresh_monitor_history(&api, &mut retained).await;
            let event = match result {
                Ok(entries) => MonitorIoEvent::Snapshot(entries),
                Err(error) => MonitorIoEvent::Failed(error.to_string()),
            };
            if event_tx.send(event).await.is_err() {
                break;
            }
        }
    });
    MonitorIo { events: event_rx }
}

async fn refresh_monitor_history(
    api: &ApiClient,
    retained: &mut HashMap<Uuid, MonitorHistoryEntry>,
) -> Result<Vec<MonitorHistoryEntry>> {
    let mut monitors = api.monitors(None).await?.monitors;
    monitors.sort_by_key(|monitor| monitor.created_wall_time_ns);
    let ids = monitors
        .iter()
        .map(|monitor| monitor.id)
        .collect::<Vec<_>>();
    retained.retain(|id, _| ids.contains(id));
    for monitor in monitors {
        let entry = retained
            .entry(monitor.id)
            .or_insert_with(|| MonitorHistoryEntry {
                monitor: monitor.clone(),
                incidents: VecDeque::new(),
                limited: false,
            });
        entry.monitor = monitor;
        let known = entry
            .incidents
            .back()
            .map_or(0, |incident| incident.incident_seq);
        if entry.monitor.incident_count > known {
            let page = api
                .monitor_incidents(
                    entry.monitor.id,
                    (known > 0).then_some(known),
                    MAX_INCIDENTS_PER_MONITOR,
                )
                .await?;
            entry.limited |= page.truncated || page.retention_gap;
            for incident in page.incidents {
                if entry
                    .incidents
                    .back()
                    .is_none_or(|known| known.id != incident.id)
                {
                    entry.incidents.push_back(incident);
                }
            }
            while entry.incidents.len() > MAX_INCIDENTS_PER_MONITOR {
                entry.incidents.pop_front();
                entry.limited = true;
            }
        }
    }
    let mut entries = retained.values().cloned().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.monitor.created_wall_time_ns);
    Ok(entries)
}

pub async fn run(
    api: ApiClient,
    loaded: LoadedConfig,
    initial_port: Option<String>,
    endpoint: String,
) -> Result<()> {
    let status = api
        .status()
        .await
        .context("cannot load Port status before opening the console")?;
    if status.ports.is_empty() {
        bail!(tr("st.no.port"));
    }
    let ports = status
        .ports
        .iter()
        .map(|slot| slot.config.port.clone())
        .collect::<Vec<_>>();
    let history_targets = status
        .ports
        .iter()
        .map(StartupHistoryTarget::from)
        .collect::<Vec<_>>();
    let mut app = App::new(status.ports, initial_port.as_deref());
    app.human_idle_release = Duration::from_secs(
        loaded
            .config
            .human_idle_release_seconds
            .unwrap_or(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS)
            .max(1),
    );
    app.config = Some(loaded.clone());
    app.mouse_capture = loaded.config.mouse_capture.unwrap_or(true);
    app.agent_history_rows = configured_agent_history_rows(loaded.config.agent_history_rows);
    app.orphan_run_timeout_seconds =
        configured_orphan_run_timeout_seconds(loaded.config.orphan_run_timeout_seconds);
    let mut initial_cursors = HashMap::new();
    load_startup_histories(api.clone(), history_targets, |history| {
        if let Some((port, cursor)) = app.apply_startup_history(history) {
            initial_cursors.insert(port, cursor);
        }
    })
    .await;
    let mut menu_io = spawn_menu_io(api.clone());
    app.menu_commands = Some(menu_io.commands.clone());
    let mut output_search_io = spawn_output_search_io(api.clone());
    app.output_search_commands = Some(output_search_io.commands.clone());
    let mut exact_evidence_io = spawn_exact_evidence_io(api.clone());
    app.exact_evidence_commands = Some(exact_evidence_io.commands.clone());
    let mut monitor_io = spawn_monitor_io(api.clone());
    let mut network = ws::spawn(endpoint, ports, initial_cursors);

    let mut terminal = enter_terminal(app.mouse_capture)?;
    let _guard = TerminalGuard {
        mouse_capture: app.mouse_capture,
    };
    let result = run_loop(
        &mut terminal,
        &mut app,
        &network.commands,
        RunLoopEvents {
            network: &mut network.events,
            menu: &mut menu_io.events,
            output_search: &mut output_search_io.events,
            exact_evidence: &mut exact_evidence_io.events,
            monitor: &mut monitor_io.events,
        },
    )
    .await;
    let _ = network.commands.try_send(NetworkCommand::Shutdown);

    // Runtime preferences are saved through `app.config`. Persist the last
    // selected Port using that latest copy so the startup snapshot cannot
    // overwrite a language or display change when the console exits.
    let mut latest_config = app.config.take().unwrap_or(loaded);
    latest_config.config.last_port = Some(app.selected_port());
    if let Err(error) = latest_config.save() {
        tracing::warn!(%error, "failed to persist the last selected Port");
    }
    result
}

struct RunLoopEvents<'a> {
    network: &'a mut mpsc::Receiver<NetworkEvent>,
    menu: &'a mut mpsc::Receiver<MenuIoEvent>,
    output_search: &'a mut mpsc::Receiver<OutputSearchIoEvent>,
    exact_evidence: &'a mut mpsc::Receiver<ExactEvidenceIoEvent>,
    monitor: &'a mut mpsc::Receiver<MonitorIoEvent>,
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    commands: &mpsc::Sender<NetworkCommand>,
    io_events: RunLoopEvents<'_>,
) -> Result<()> {
    let mut terminal_events = EventStream::new();
    let mut network_events_open = true;
    let mut output_search_events_open = true;
    let mut exact_evidence_events_open = true;
    let mut render_tick = tokio::time::interval(Duration::from_millis(33));
    render_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut renew_tick = tokio::time::interval(Duration::from_secs(10));
    renew_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut activity_tick = tokio::time::interval(Duration::from_secs(1));
    activity_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    terminal.draw(|frame| draw(frame, app))?;
    while !app.should_quit {
        tokio::select! {
            event = terminal_events.next() => match event {
                Some(Ok(event)) => app.handle_terminal_event(event, commands),
                Some(Err(error)) => return Err(error).context("terminal input failed"),
                None => return Ok(()),
            },
            event = io_events.network.recv(), if network_events_open => {
                network_events_open = handle_network_channel_event(app, event, commands);
            },
            event = io_events.menu.recv() => {
                if let Some(event) = event {
                    app.handle_menu_io_event(event, commands);
                }
            },
            event = io_events.output_search.recv(), if output_search_events_open => {
                if let Some(event) = event {
                    app.handle_output_search_io_event(event);
                } else {
                    output_search_events_open = false;
                    if let Some(search) = app.output_search.as_mut()
                        && matches!(search.phase, OutputSearchPhase::Loading(_))
                    {
                        search.phase = OutputSearchPhase::Editing;
                        search.error = Some(tr("ui.output.search.unavailable").into());
                        app.dirty = true;
                    }
                }
            },
            event = io_events.exact_evidence.recv(), if exact_evidence_events_open => {
                if let Some(event) = event {
                    app.handle_exact_evidence_io_event(event);
                } else {
                    exact_evidence_events_open = false;
                    if let Some((_, target)) = app.pending_exact_evidence.take() {
                        app.current_mut().follow();
                        app.status = match target {
                            ExactEvidenceTarget::Incident(_) => {
                                tr("st.monitor.jump.query.unavailable").into()
                            }
                            ExactEvidenceTarget::Command(_) => {
                                tr("st.run.jump.query.unavailable").into()
                            }
                        };
                        app.dirty = true;
                    }
                }
            },
            event = io_events.monitor.recv() => {
                if let Some(event) = event {
                    app.handle_monitor_io_event(event);
                }
            },
            _ = renew_tick.tick() => app.maintain_controls(commands),
            _ = activity_tick.tick() => {
                let now = Instant::now();
                let selection_changed = app.expire_mouse_selection(now);
                let status_notice_changed = app.expire_status_notice(now);
                let mut trigger_changed = false;
                for slot in &mut app.ports {
                    trigger_changed |= slot.update_trigger_deadline(now);
                }
                if selection_changed || status_notice_changed || trigger_changed || app.ports.iter().any(|slot| {
                    slot.snapshot.target_activity == TargetActivity::Active
                        && slot.snapshot.session_state == SessionState::Online
                }) {
                    app.dirty = true;
                }
            },
            _ = render_tick.tick() => {
                if app.update_software_cursor_blink(Instant::now()) {
                    app.dirty = true;
                }
                if app.dirty {
                    terminal.draw(|frame| draw(frame, app))?;
                    app.dirty = false;
                }
            }
        }
    }
    Ok(())
}

fn handle_network_channel_event(
    app: &mut App,
    event: Option<NetworkEvent>,
    commands: &mpsc::Sender<NetworkCommand>,
) -> bool {
    match event {
        Some(event) => {
            app.handle_network(event, commands);
            true
        }
        None => {
            app.transport_connected = false;
            app.hello_accepted = false;
            app.connection_generation = None;
            app.actor = None;
            for slot in &mut app.ports {
                slot.subscription = SubscriptionPhase::Disconnected;
            }
            app.status = tr("st.network.stopped").into();
            app.dirty = true;
            // A closed Tokio receiver is permanently ready. Disable this
            // select branch after observing closure once so terminal input and
            // rendering remain responsive instead of busy-spinning.
            false
        }
    }
}

fn enter_terminal(mouse_capture: bool) -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(stdout, EnterAlternateScreen, EnableBracketedPaste) {
        leave_terminal(false);
        return Err(error.into());
    }
    if mouse_capture && let Err(error) = execute!(stdout, EnableMouseCapture) {
        leave_terminal(true);
        return Err(error.into());
    }
    match Terminal::new(CrosstermBackend::new(stdout)) {
        Ok(terminal) => Ok(terminal),
        Err(error) => {
            leave_terminal(mouse_capture);
            Err(error.into())
        }
    }
}

struct TerminalGuard {
    mouse_capture: bool,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        leave_terminal(self.mouse_capture);
    }
}

fn leave_terminal(mouse_capture: bool) {
    if mouse_capture {
        let _ = execute!(io::stdout(), DisableMouseCapture);
    }
    let _ = execute!(
        io::stdout(),
        Show,
        DisableBracketedPaste,
        LeaveAlternateScreen
    );
    let _ = disable_raw_mode();
    let _ = io::stdout().flush();
}

#[cfg(test)]
fn displayed_target_activity_at(snapshot: &SlotSnapshot, now: i64) -> TargetActivity {
    if snapshot.session_state != SessionState::Online {
        return TargetActivity::Unknown;
    }
    if snapshot.target_activity != TargetActivity::Active {
        return snapshot.target_activity;
    }
    let Some(last_rx) = snapshot.last_rx_wall_time_ns else {
        return TargetActivity::Active;
    };
    if now.saturating_sub(last_rx) >= ACTIVE_WINDOW_NS {
        TargetActivity::Silent
    } else {
        TargetActivity::Active
    }
}

fn optional_sequence_label(sequence: Option<u64>) -> String {
    sequence.map_or_else(|| tr("value.none").into(), |value| value.to_string())
}

fn local_history_truncated_message() -> &'static str {
    tr("history.local.truncated")
}

struct QueueCard {
    operation_index: usize,
    sending: bool,
    header: String,
    command: String,
}

fn queue_cards(app: &App, inner_width: u16) -> Vec<QueueCard> {
    let port = app.selected_port();
    let Some(queue) = app.pending_writes.get(&port) else {
        return Vec::new();
    };
    let eol = app.current().effective_write_eol().as_bytes();
    queued_line_operations(queue)
        .into_iter()
        .enumerate()
        .map(|(operation_index, operation)| {
            let sending = app.pending_requests.values().any(|request| match request {
                PendingRequest::Write {
                    port: pending_slot,
                    operation_id,
                    ..
                } if pending_slot == &port => {
                    operation.operation_id.is_none() || operation.operation_id == *operation_id
                }
                _ => false,
            });
            let mut bytes = operation.data.as_slice();
            if !eol.is_empty() && bytes.ends_with(eol) {
                bytes = &bytes[..bytes.len() - eol.len()];
            }
            let command = safe_inline(&String::from_utf8_lossy(bytes));
            let command = if command.is_empty() {
                tr("ui.queue.empty").to_string()
            } else {
                command
            };
            let header = format!("{}.", operation_index + 1);
            QueueCard {
                operation_index,
                sending,
                header,
                command: truncate_display(&command, inner_width.saturating_sub(5).max(1) as usize),
            }
        })
        .collect()
}

fn wrap_queue_text(value: &str, width: u16) -> Vec<String> {
    let width = width.max(1) as usize;
    let mut rows = Vec::new();
    let mut row = String::new();
    let mut used = 0usize;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if character_width > 0 && !row.is_empty() && used.saturating_add(character_width) > width {
            rows.push(std::mem::take(&mut row));
            used = 0;
        }
        row.push(character);
        used = used.saturating_add(character_width);
        if used >= width {
            rows.push(std::mem::take(&mut row));
            used = 0;
        }
    }
    if !row.is_empty() || rows.is_empty() {
        rows.push(row);
    }
    rows
}

fn draw(frame: &mut Frame<'_>, app: &mut App) {
    app.sync_status_notice(Instant::now());
    let area = frame.area();
    let history_growth = app
        .agent_history_rows
        .saturating_sub(DEFAULT_AGENT_HISTORY_ROWS);
    let show_run_history_bar = app.run_panel_visible
        && area.height >= RUN_HISTORY_BAR_MIN_TERMINAL_HEIGHT.saturating_add(history_growth);
    let run_history_height = if show_run_history_bar {
        app.agent_history_rows
    } else {
        0
    };
    let separator_height = u16::from(show_run_history_bar);

    let queue_visual_rows = queue_cards(app, area.width.saturating_sub(2)).len();
    // Preserve the existing four-row minimum output pane. On a normal
    // terminal every queued operation gets one row; very short terminals use
    // a bounded queue viewport that follows the selected operation.
    let max_queue_height = area
        .height
        .saturating_sub(13)
        .saturating_sub(run_history_height);
    let queue_height = if queue_visual_rows == 0 {
        0
    } else {
        queue_visual_rows
            .saturating_add(2)
            .min(max_queue_height as usize) as u16
    };
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(4),
        Constraint::Length(separator_height),
        Constraint::Length(run_history_height),
        Constraint::Length(separator_height),
        Constraint::Length(queue_height),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .split(area);
    let output_area = chunks[1];
    let run_history_area = show_run_history_bar.then_some(chunks[3]);
    let input_area = chunks[6];
    app.layout = Some(ConsoleLayout {
        output_area,
        output_inner: inset_border(output_area),
        input_area,
        run_history_area,
        run_history_inner: run_history_area,
    });
    app.clamp_run_detail_scroll();

    draw_tabs(frame, app, chunks[0]);
    draw_output(frame, app, chunks[1]);
    if let Some(run_history_area) = run_history_area {
        draw_powerline_separator(
            frame,
            app,
            chunks[2],
            if app.current().run_history_limited {
                "ui.separator.agent.recent"
            } else {
                "ui.separator.agent"
            },
        );
        draw_run_history(frame, app, run_history_area, false);
        draw_powerline_separator(frame, app, chunks[4], "ui.separator.input");
    }
    if queue_height > 0 {
        draw_queue(frame, app, chunks[5]);
    }
    draw_input(frame, app, chunks[6]);
    draw_help_line(frame, app, chunks[7]);
    if run_history_area.is_none() && app.run_panel_visible && app.focus == PaneFocus::RunHistory {
        let popup = centered_rect(
            area.width.saturating_sub(4).clamp(1, 72),
            area.height.saturating_sub(4).max(1),
            area,
        );
        frame.render_widget(Clear, popup);
        if let Some(layout) = app.layout.as_mut() {
            layout.run_history_area = Some(popup);
            layout.run_history_inner = Some(inset_border(popup));
        }
        app.clamp_run_detail_scroll();
        draw_run_history(frame, app, popup, true);
    }
    if app.help {
        draw_help(frame, app, area);
    }
    if let Some(menu) = app.menu.as_ref() {
        draw_menu(frame, app, menu, area);
    }
    if let Some(search) = app.output_search.as_ref() {
        draw_output_search(frame, search, area, app.software_cursor_visible);
    }
}

fn output_search_matcher_label(matcher: OutputSearchMatcher) -> &'static str {
    match matcher {
        OutputSearchMatcher::Literal => tr("ui.output.search.matcher.literal"),
        OutputSearchMatcher::Regex => tr("ui.output.search.matcher.regex"),
    }
}

fn output_search_case_label(case_sensitive: bool) -> &'static str {
    if case_sensitive {
        tr("ui.output.search.case.sensitive")
    } else {
        tr("ui.output.search.case.insensitive")
    }
}

fn output_search_direction_label(direction: OutputSearchDirection) -> &'static str {
    match direction {
        OutputSearchDirection::Both => tr("ui.output.search.direction.both"),
        OutputSearchDirection::Rx => tr("ui.output.search.direction.rx"),
        OutputSearchDirection::Tx => tr("ui.output.search.direction.tx"),
    }
}

fn output_search_scope_label(scope: OutputSearchScope) -> &'static str {
    match scope {
        OutputSearchScope::CurrentEpoch => tr("ui.output.search.scope.epoch"),
        OutputSearchScope::Retained => tr("ui.output.search.scope.retained"),
        OutputSearchScope::CurrentRun => tr("ui.output.search.scope.run"),
    }
}

fn output_search_filters(search: &OutputSearchState) -> String {
    trf(
        "ui.output.search.filters",
        &[
            output_search_matcher_label(search.matcher),
            output_search_case_label(search.case_sensitive),
            output_search_direction_label(search.direction),
            output_search_scope_label(search.scope),
        ],
    )
}

fn output_search_target(search: &OutputSearchState) -> String {
    let epoch = search.current_epoch.to_string();
    let epoch = &epoch[..8];
    match search.scope {
        OutputSearchScope::Retained => tr("ui.output.search.target.retained").into(),
        OutputSearchScope::CurrentRun => search.current_run.map_or_else(
            || {
                trf(
                    "ui.output.search.target.epoch",
                    &[epoch, &search.head_seq.to_string()],
                )
            },
            |run| {
                let run_id = run.id.to_string();
                trf(
                    "ui.output.search.target.run",
                    &[epoch, &search.head_seq.to_string(), &run_id[..8]],
                )
            },
        ),
        OutputSearchScope::CurrentEpoch => trf(
            "ui.output.search.target.epoch",
            &[epoch, &search.head_seq.to_string()],
        ),
    }
}

fn output_search_event_summary(event: &TimelineEvent, width: u16) -> String {
    let direction = match event.direction {
        Direction::Rx => "RX",
        Direction::Tx => "TX",
        Direction::None => "-",
    };
    let epoch = event.daemon_epoch.to_string();
    let epoch = &epoch[..8];
    let payload = safe_inline(&String::from_utf8_lossy(&event.data));
    let payload = if payload.is_empty() {
        tr("ui.output.search.empty.event").into()
    } else {
        payload
    };
    truncate_display(
        &trf(
            "ui.output.search.row",
            &[
                &format_wall_time_local(event.wall_time_ns),
                direction,
                &event.seq.to_string(),
                epoch,
                &payload,
            ],
        ),
        width.max(1) as usize,
    )
}

#[derive(Debug, PartialEq, Eq)]
struct OutputSearchResultFooter {
    integrity: String,
    navigation: String,
    limits: Option<String>,
}

fn trim_output_search_footer_separator(value: String) -> String {
    value
        .trim_start_matches(|character: char| character.is_whitespace() || character == '·')
        .to_owned()
}

fn output_search_result_footer(search: &OutputSearchState) -> OutputSearchResultFooter {
    let position = if search.results.is_empty() {
        "0".to_owned()
    } else {
        (search.selected + 1).to_string()
    };
    let navigation = trf(
        "ui.output.search.position",
        &[
            &position,
            &search.results.len().to_string(),
            &search.scanned_archives.to_string(),
        ],
    );
    let mut warnings = Vec::new();
    if search.partial {
        warnings.push(tr("ui.output.search.integrity.partial").to_owned());
    }
    if !search.gaps.is_empty() {
        warnings.push(trim_output_search_footer_separator(trf(
            "ui.output.search.gaps",
            &[&search.gaps.len().to_string()],
        )));
    }
    let integrity = if warnings.is_empty() {
        tr("ui.output.search.integrity.complete").to_owned()
    } else {
        warnings.join(" · ")
    };
    let limits = search.partial.then(|| {
        // Keep the detailed bounded-query contract visible without making it
        // compete with the compact first-line integrity warning.
        trim_output_search_footer_separator(tr("ui.output.search.partial").to_owned())
    });
    OutputSearchResultFooter {
        integrity,
        navigation,
        limits,
    }
}

fn draw_output_search(
    frame: &mut Frame<'_>,
    search: &OutputSearchState,
    area: Rect,
    cursor_visible: bool,
) {
    let width = area.width.saturating_sub(4).clamp(1, 110);
    let height = area.height.saturating_sub(2).clamp(1, 34);
    let popup = centered_rect(width, height, area);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(tr("ui.output.search.title"))
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    if inner.width == 0 || inner.height == 0 {
        return;
    }

    match search.phase {
        OutputSearchPhase::Editing | OutputSearchPhase::Loading(_) => {
            let chunks = Layout::vertical([
                Constraint::Length(3),
                Constraint::Length(4),
                Constraint::Min(1),
                Constraint::Length(1),
            ])
            .split(inner);
            let (query, cursor_column) = line_input_projection(
                &search.query,
                search.cursor,
                chunks[0].width.saturating_sub(2),
            );
            frame.render_widget(
                Paragraph::new(if search.phase == OutputSearchPhase::Editing {
                    line_with_software_cursor(query, cursor_column, cursor_visible)
                } else {
                    Line::from(query)
                })
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(tr("ui.output.search.query")),
                ),
                chunks[0],
            );
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(output_search_filters(search)),
                    Line::from(output_search_target(search)),
                    Line::from(tr("ui.output.search.filter.keys")),
                ])
                .style(Style::default().fg(Color::LightCyan)),
                chunks[1],
            );
            let message = if let Some(error) = search.error.as_deref() {
                Line::from(Span::styled(
                    safe_inline(error),
                    Style::default().fg(Color::LightRed),
                ))
            } else if matches!(search.phase, OutputSearchPhase::Loading(_)) {
                Line::from(Span::styled(
                    tr("ui.output.search.loading"),
                    Style::default().fg(Color::Yellow),
                ))
            } else {
                Line::from(tr("ui.output.search.boundary.note"))
            };
            frame.render_widget(
                Paragraph::new(message).wrap(Wrap { trim: false }),
                chunks[2],
            );
            frame.render_widget(
                Paragraph::new(tr("ui.output.search.edit.footer"))
                    .alignment(Alignment::Center)
                    .style(Style::default().fg(Color::DarkGray)),
                chunks[3],
            );
        }
        OutputSearchPhase::Results => {
            let detail_height = inner.height.saturating_div(3).clamp(3, 8);
            let desired_footer_height = if search.partial { 5 } else { 2 };
            // On short terminals retain room for the result list and detail;
            // the compact integrity warning always remains the first row.
            let footer_height = desired_footer_height.min(inner.height.saturating_sub(9).max(2));
            let chunks = Layout::vertical([
                Constraint::Length(4),
                Constraint::Min(2),
                Constraint::Length(detail_height),
                Constraint::Length(footer_height),
            ])
            .split(inner);
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(trf(
                        "ui.output.search.result.query",
                        &[&safe_inline(&search.query_text())],
                    )),
                    Line::from(output_search_filters(search)),
                    Line::from(output_search_target(search)),
                    Line::from(Span::styled(
                        tr("ui.output.search.completion.block"),
                        Style::default().fg(Color::DarkGray),
                    )),
                ]),
                chunks[0],
            );
            let visible = chunks[1].height as usize;
            let start = search
                .selected
                .saturating_sub(visible.saturating_sub(1))
                .min(search.results.len().saturating_sub(visible));
            let rows = if search.results.is_empty() {
                vec![Line::from(Span::styled(
                    tr("ui.output.search.none"),
                    Style::default().fg(Color::DarkGray),
                ))]
            } else {
                search
                    .results
                    .iter()
                    .enumerate()
                    .skip(start)
                    .take(visible)
                    .map(|(index, event)| {
                        let marker = if index == search.selected {
                            "› "
                        } else {
                            "  "
                        };
                        Line::from(Span::styled(
                            format!(
                                "{marker}{}",
                                output_search_event_summary(
                                    event,
                                    chunks[1].width.saturating_sub(2)
                                )
                            ),
                            if index == search.selected {
                                Style::default()
                                    .fg(Color::LightCyan)
                                    .add_modifier(Modifier::BOLD)
                            } else {
                                Style::default()
                            },
                        ))
                    })
                    .collect()
            };
            frame.render_widget(Paragraph::new(rows), chunks[1]);
            let detail = search.results.get(search.selected).map_or_else(
                || tr("ui.output.search.no.detail").to_owned(),
                format_event_plain,
            );
            frame.render_widget(
                Paragraph::new(detail)
                    .block(
                        Block::default()
                            .borders(Borders::TOP)
                            .title(tr("ui.output.search.detail")),
                    )
                    .wrap(Wrap { trim: false })
                    .scroll((search.detail_scroll.min(u16::MAX as usize) as u16, 0)),
                chunks[2],
            );
            let footer = output_search_result_footer(search);
            let integrity_style = if search.partial || !search.gaps.is_empty() {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let mut footer_lines = vec![
                Line::from(Span::styled(footer.integrity, integrity_style)),
                Line::from(Span::styled(
                    footer.navigation,
                    Style::default().fg(Color::DarkGray),
                )),
            ];
            if let Some(limits) = footer.limits {
                footer_lines.push(Line::from(Span::styled(
                    limits,
                    Style::default().fg(Color::Yellow),
                )));
            }
            frame.render_widget(
                Paragraph::new(footer_lines)
                    .alignment(Alignment::Center)
                    .wrap(Wrap { trim: false }),
                chunks[3],
            );
        }
    }
}

fn session_state_label(state: SessionState) -> &'static str {
    match state {
        SessionState::Disabled => tr("state.disabled"),
        SessionState::WaitingForPort => tr("state.waiting"),
        SessionState::Opening => tr("state.opening"),
        SessionState::Online => tr("state.online"),
        SessionState::Backoff => tr("state.backoff"),
        SessionState::Stopping => tr("state.stopping"),
    }
}

fn draw_tabs(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let titles = app
        .ports
        .iter()
        .map(|slot| {
            let state = session_state_label(slot.snapshot.session_state);
            Line::from(format!(
                " {} · {} ",
                safe_inline(&slot.snapshot.config.port),
                state,
            ))
        })
        .collect::<Vec<_>>();
    let connection = if !app.transport_connected {
        tr("conn.reconnecting")
    } else if !app.hello_accepted {
        tr("conn.handshaking")
    } else if app.all_slots_ready() {
        tr("conn.live")
    } else {
        tr("conn.attaching")
    };
    let tabs = Tabs::new(titles)
        .select(app.selected)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!(" serialctl · {connection} ")),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider("│");
    frame.render_widget(tabs, area);
}

fn draw_output(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let title = output_title(app);
    let block = Block::default().borders(Borders::ALL).title(title);
    let inner = block.inner(area);
    let mut visual_lines = app.selection.as_ref().map_or_else(
        || visible_output_lines(app, inner),
        |selection| selection.rows.clone(),
    );
    if let Some(selection) = app.selection.as_ref() {
        let (start, end) = selection.ordered_points();
        for (row, line) in visual_lines.iter_mut().enumerate() {
            if let Some((from, through)) = selection_columns(start, end, row) {
                *line = line_with_selection(line.clone(), from, through);
            }
        }
    }
    frame.render_widget(Paragraph::new(visual_lines).block(block), area);
}

fn output_title(app: &App) -> String {
    format!(" {} ", safe_inline(&app.current_model_profile_name()))
}

fn visible_output_lines(app: &App, inner: Rect) -> Vec<Line<'static>> {
    let view = app.current();
    let visible_height = inner.height as usize;
    if let Some(snapshot) = view.scroll_snapshot.as_ref() {
        let max_scroll = snapshot.rows.len().saturating_sub(visible_height);
        let scroll = view.scroll_from_bottom.min(max_scroll);
        let end = snapshot.rows.len().saturating_sub(scroll);
        let start = end.saturating_sub(visible_height);
        return snapshot.rows[start..end].to_vec();
    }

    let truncation_line = view.local_truncation_line();
    let total_lines = view.logical_line_count();
    // Clamp the paused offset so a vanished pending row can never produce an
    // empty viewport; push_line already keeps the offset anchored on append
    // and front-eviction.
    let scroll = view.scroll_from_bottom.min(total_lines.saturating_sub(1));
    let end = total_lines.saturating_sub(scroll);
    // Every logical row occupies at least one wrapped visual row, so the last
    // `visible_height` logical rows before the requested boundary are a
    // sufficient suffix. Paragraph then scrolls inside that suffix by its
    // actual wrapped-line count, keeping the newest prompt visible at 80
    // columns without remeasuring the full 20,000-row scrollback on each draw.
    let start = end.saturating_sub(visible_height);
    let entries = truncation_line
        .iter()
        .chain(view.lines.iter().chain(view.pending_line.iter()))
        .skip(start)
        .take(end.saturating_sub(start))
        .collect::<Vec<_>>();
    // Ratatui's stable public API does not expose the rendered line count for
    // a wrapped Paragraph. Pre-wrap this small logical suffix using terminal
    // character widths, then retain its visual tail. This mirrors a serial
    // terminal's character wrapping and guarantees that one long row cannot
    // push the newest prompt below the viewport.
    let visual_lines = render_output_entries(app, &entries, inner.width)
        .into_iter()
        .map(|row| row.line)
        .collect::<Vec<_>>();
    let visual_start = visual_lines.len().saturating_sub(visible_height);
    visual_lines.into_iter().skip(visual_start).collect()
}

/// Renders the complete bounded local history into visual terminal rows. This
/// intentionally runs only when the operator first pauses output. The frozen
/// rows make subsequent live appends O(1) for viewport stability and avoid
/// mixing logical-row offsets with post-wrap visual-row offsets.
#[derive(Debug)]
struct OutputVisualRow {
    line: Line<'static>,
    daemon_epoch: Option<Uuid>,
    seq: u64,
}

#[derive(Debug)]
struct CommandCapture {
    start: Option<usize>,
    end: Option<usize>,
    command: String,
    highlight_available: bool,
    sequence: u64,
    incident_epoch: Option<Uuid>,
}

enum CommandBoundaryMatcher {
    Contains(String),
    Prompt(String),
    Regex(regex::Regex),
}

impl CommandBoundaryMatcher {
    fn matches(&self, text: &str) -> bool {
        match self {
            Self::Contains(value) => text.contains(value),
            // Prompt completion uses the same logical-line boundary as the
            // Agent capture. A shell prompt at the start of the device echo
            // (for example `dut# show status`) is not the command's final
            // prompt and must not truncate the highlighted response.
            Self::Prompt(value) => text.split('\n').any(|line| line.ends_with(value)),
            Self::Regex(value) => value.is_match(text),
        }
    }
}

fn command_capture_matchers(
    record: &RunCommandRecord,
    step_index: Option<usize>,
) -> Vec<CommandCaptureMatcher> {
    step_index
        .and_then(|index| record.steps.get(index))
        .or_else(|| record.steps.last())
        .map(|step| step.capture_matchers.as_slice())
        .unwrap_or_default()
        .to_vec()
}

fn command_boundary_matchers(matchers: &[CommandCaptureMatcher]) -> Vec<CommandBoundaryMatcher> {
    matchers
        .iter()
        .filter_map(|matcher| match matcher.kind {
            CommandCaptureMatcherKind::Regex => regex::Regex::new(&matcher.value)
                .ok()
                .map(CommandBoundaryMatcher::Regex),
            CommandCaptureMatcherKind::Contains => (!matcher.value.is_empty())
                .then(|| CommandBoundaryMatcher::Contains(matcher.value.clone())),
            CommandCaptureMatcherKind::ShellPrompt | CommandCaptureMatcherKind::UbootPrompt => {
                let value = matcher.value.replace("\r\n", "\n").replace('\r', "\n");
                let value = value.trim_end_matches('\n');
                (!value.is_empty()).then(|| CommandBoundaryMatcher::Prompt(value.to_owned()))
            }
        })
        .collect()
}

fn first_command_boundary(
    entries: &[&DisplayLine],
    start: usize,
    eligible: impl Fn(&&DisplayLine) -> bool,
    matchers: &[CommandBoundaryMatcher],
) -> Option<usize> {
    let mut received = String::new();
    for (index, entry) in entries.iter().enumerate().skip(start) {
        if !eligible(entry) {
            break;
        }
        received.push_str(&entry.text);
        received.push('\n');
        if matchers.iter().any(|matcher| matcher.matches(&received)) {
            return Some(index);
        }
    }
    None
}

fn command_payload(record: &RunCommandRecord, step_index: Option<usize>) -> String {
    record
        .steps
        .iter()
        .enumerate()
        .filter(|(index, _)| step_index.is_none_or(|selected| *index == selected))
        .map(|(_, step)| step)
        .map(|step| {
            String::from_utf8_lossy(&step.data)
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        })
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("  →  ")
}

fn command_capture_for_target(
    target: &CommandEvidenceTarget,
    entries: &[&DisplayLine],
) -> CommandCapture {
    let in_window = |entry: &&DisplayLine| {
        entry.daemon_epoch == Some(target.daemon_epoch)
            && entry.seq >= target.seq_start
            && entry.seq <= target.query_end_seq
    };
    let eligible = |entry: &&DisplayLine| {
        in_window(entry)
            && entry.event_kind == EventKind::Rx
            && entry.run_boundary.is_none()
            && entry.solid_style.is_none()
    };
    let first_in_window = entries.iter().position(in_window);
    let first_available = entries.iter().position(eligible);
    let start = first_available;
    let matchers = command_boundary_matchers(&target.matchers);
    let boundary_start = first_available.and_then(|start| {
        entries
            .iter()
            .enumerate()
            .skip(start)
            .take_while(|(_, entry)| eligible(entry))
            .find(|(_, entry)| entry.seq >= target.write_end_seq)
            .map(|(index, _)| index)
            .or(Some(start))
    });
    let end = if matchers.is_empty() || first_in_window != first_available {
        None
    } else {
        boundary_start.and_then(|index| {
            first_command_boundary(entries, index, eligible, &matchers)
                .filter(|end| start.is_some_and(|start| *end >= start))
        })
    };
    CommandCapture {
        start,
        end,
        command: target.command.clone(),
        highlight_available: start.is_some() && end.is_some(),
        sequence: target.seq_start,
        incident_epoch: Some(target.daemon_epoch),
    }
}

fn local_command_evidence_is_complete(
    view: &SlotView,
    target: &CommandEvidenceTarget,
    entries: &[&DisplayLine],
    capture: &CommandCapture,
) -> bool {
    if !local_command_window_is_retained(view, target, entries) || !capture.highlight_available {
        return false;
    }
    let Some((start, end)) = capture.start.zip(capture.end) else {
        return false;
    };
    if entries.get(end).is_none_or(|entry| {
        entry.daemon_epoch != Some(target.daemon_epoch)
            || entry.seq > target.query_end_seq
            || entry.event_kind != EventKind::Rx
    }) {
        return false;
    }
    !entries[start..=end]
        .iter()
        .any(|entry| entry.event_kind == EventKind::Gap)
}

fn local_command_window_is_retained(
    view: &SlotView,
    target: &CommandEvidenceTarget,
    entries: &[&DisplayLine],
) -> bool {
    if view.snapshot.config.port != target.port || view.snapshot.daemon_epoch != target.daemon_epoch
    {
        return false;
    }
    if view
        .local_contiguous_from_seq
        .is_none_or(|sequence| sequence > target.seq_start)
    {
        return false;
    }
    if view.local_history_truncated {
        let first_retained = entries
            .iter()
            .find(|entry| entry.daemon_epoch == Some(target.daemon_epoch))
            .map(|entry| entry.seq);
        if first_retained.is_none_or(|sequence| sequence > target.seq_start) {
            return false;
        }
    }
    true
}

fn command_capture(app: &App, entries: &[&DisplayLine]) -> Option<CommandCapture> {
    if app.focus != PaneFocus::RunHistory {
        return None;
    }
    let view = app.current();
    if let Some(incident) = view.selected_monitor_incident()
        && incident.daemon_epoch == view.snapshot.daemon_epoch
    {
        let target = IncidentEvidenceTarget::from(incident);
        local_incident_entry_range(view, &target)?;
        let start = entries.iter().position(|entry| {
            entry.daemon_epoch == Some(incident.daemon_epoch) && entry.seq == incident.seq_start
        });
        let end = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                entry.daemon_epoch == Some(incident.daemon_epoch) && entry.seq == incident.seq_end
            })
            .map(|(index, _)| index)
            .next_back();
        return Some(CommandCapture {
            start,
            end,
            command: incident.preview.clone(),
            highlight_available: start.is_some() && end.is_some(),
            sequence: incident.seq_start,
            incident_epoch: Some(incident.daemon_epoch),
        });
    }
    let key = view.selected_run_command_key()?;
    let target = app.command_evidence_target(key, view.selected_run_step)?;
    let full_entries = view
        .lines
        .iter()
        .chain(view.pending_line.iter())
        .collect::<Vec<_>>();
    let full_capture = command_capture_for_target(&target, &full_entries);
    if target.matchers.is_empty() && local_command_window_is_retained(view, &target, &full_entries)
    {
        return Some(command_capture_for_target(&target, entries));
    }
    if !local_command_evidence_is_complete(view, &target, &full_entries, &full_capture) {
        return Some(CommandCapture {
            start: None,
            end: None,
            command: target.command,
            highlight_available: false,
            sequence: target.seq_start,
            incident_epoch: Some(target.daemon_epoch),
        });
    }
    Some(command_capture_for_target(&target, entries))
}

const COMMAND_CAPTURE_BACKGROUND: Color = Color::Rgb(28, 53, 66);
const COMMAND_FALLBACK_BACKGROUND: Color = Color::LightCyan;

fn command_capture_line(mut line: Line<'static>) -> Line<'static> {
    let background = COMMAND_CAPTURE_BACKGROUND;
    line.style = line.style.patch(Style::default().bg(background));
    for span in &mut line.spans {
        span.style = span.style.patch(Style::default().bg(background));
    }
    line
}

fn command_fallback_line(command: &str) -> Line<'static> {
    Line::from(Span::styled(
        format!("› {}", safe_inline(command)),
        Style::default()
            .fg(Color::Black)
            .bg(COMMAND_FALLBACK_BACKGROUND)
            .add_modifier(Modifier::BOLD),
    ))
}

/// Pads one already-wrapped visual row to the output pane width. Ratatui only
/// paints cells occupied by text, so styling the `Line` and its existing spans
/// alone leaves the short-row tail on the terminal's default background.
fn fill_visual_row_background(
    mut line: Line<'static>,
    width: u16,
    background: Color,
) -> Line<'static> {
    let width = usize::from(width);
    let used = line
        .spans
        .iter()
        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
        .sum::<usize>();
    let remaining = width.saturating_sub(used);
    if remaining > 0 {
        line.spans.push(Span::styled(
            " ".repeat(remaining),
            Style::default().bg(background),
        ));
    }
    line
}

fn wrap_command_capture_line(line: Line<'static>, width: u16) -> Vec<Line<'static>> {
    wrap_timeline_line(command_capture_line(line), width)
        .into_iter()
        .map(|line| fill_visual_row_background(line, width, COMMAND_CAPTURE_BACKGROUND))
        .collect()
}

fn wrap_command_fallback_line(command: &str, width: u16) -> Vec<Line<'static>> {
    wrap_timeline_line(command_fallback_line(command), width)
        .into_iter()
        .map(|line| fill_visual_row_background(line, width, COMMAND_FALLBACK_BACKGROUND))
        .collect()
}

fn render_output_entries(app: &App, entries: &[&DisplayLine], width: u16) -> Vec<OutputVisualRow> {
    if width == 0 {
        return Vec::new();
    }
    let view = app.current();
    let capture = command_capture(app, entries);
    let shell_prompt = view.effective_shell_prompt();
    let uboot_prompt = view.effective_uboot_prompt();
    let source_width = detailed_source_width(width as usize);
    let mut rows = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        if capture
            .as_ref()
            .is_some_and(|capture| !capture.highlight_available && capture.start == Some(index))
        {
            let capture = capture.as_ref().expect("capture was just checked");
            rows.extend(
                wrap_command_fallback_line(&capture.command, width)
                    .into_iter()
                    .map(|line| OutputVisualRow {
                        line,
                        daemon_epoch: None,
                        seq: capture.sequence,
                    }),
            );
        }
        let highlighted = capture.as_ref().is_some_and(|capture| {
            capture.highlight_available
                && capture
                    .start
                    .zip(capture.end)
                    .is_some_and(|(start, end)| start <= index && index <= end)
                && capture.incident_epoch.is_none_or(|epoch| {
                    entry.daemon_epoch == Some(epoch) && entry.event_kind == EventKind::Rx
                })
        });
        let line = timeline_line(
            entry,
            app.detailed_timeline,
            source_width,
            shell_prompt,
            uboot_prompt,
            width as usize,
        );
        let visual_lines = if highlighted {
            wrap_command_capture_line(line, width)
        } else {
            wrap_timeline_line(line, width)
        };
        rows.extend(visual_lines.into_iter().map(|line| OutputVisualRow {
            line,
            daemon_epoch: entry.daemon_epoch,
            seq: entry.seq,
        }));
    }
    if let Some(capture) = capture.filter(|capture| capture.start.is_none()) {
        rows.extend(
            wrap_command_fallback_line(&capture.command, width)
                .into_iter()
                .map(|line| OutputVisualRow {
                    line,
                    daemon_epoch: None,
                    seq: capture.sequence,
                }),
        );
    }
    rows
}

fn all_output_visual_rows(app: &App, width: u16) -> Vec<OutputVisualRow> {
    if width == 0 {
        return Vec::new();
    }
    let view = app.current();
    let truncation_line = view.local_truncation_line();
    let entries = truncation_line
        .iter()
        .chain(view.lines.iter().chain(view.pending_line.iter()))
        .collect::<Vec<_>>();
    render_output_entries(app, &entries, width)
}

fn all_output_visual_lines(app: &App, width: u16) -> Vec<Line<'static>> {
    all_output_visual_rows(app, width)
        .into_iter()
        .map(|row| row.line)
        .collect()
}

fn local_incident_entry_range(
    view: &SlotView,
    target: &IncidentEvidenceTarget,
) -> Option<(usize, usize)> {
    if view.snapshot.config.port != target.port || view.snapshot.daemon_epoch != target.daemon_epoch
    {
        return None;
    }
    let entries = view
        .lines
        .iter()
        .chain(view.pending_line.iter())
        .collect::<Vec<_>>();
    let start = entries.iter().position(|entry| {
        entry.daemon_epoch == Some(target.daemon_epoch) && entry.seq == target.seq_start
    })?;
    let end = entries.iter().rposition(|entry| {
        entry.daemon_epoch == Some(target.daemon_epoch) && entry.seq == target.seq_end
    })?;
    if start > end {
        return None;
    }
    let relevant = &entries[start..=end];
    if relevant.iter().any(|entry| {
        entry.daemon_epoch != Some(target.daemon_epoch) || entry.event_kind == EventKind::Gap
    }) {
        return None;
    }
    let mut sequences = relevant.iter().map(|entry| entry.seq).collect::<Vec<_>>();
    sequences.dedup();
    if sequences.first().copied() != Some(target.seq_start)
        || sequences.last().copied() != Some(target.seq_end)
        || !sequences
            .windows(2)
            .all(|pair| pair[0].checked_add(1) == Some(pair[1]))
    {
        return None;
    }
    Some((start, end))
}

fn project_incident_evidence(events: &[TimelineEvent]) -> Vec<DisplayLine> {
    let mut stream = TerminalStreamParser::new();
    stream.set_echo_reconciliation(false);
    let mut lines = Vec::new();
    let mut pending = None;
    for event in events {
        if event.direction == Direction::Tx {
            continue;
        }
        let batch = stream.push_event(event);
        lines.extend(batch.completed);
        pending = batch.pending;
    }
    lines.extend(pending);
    lines
}

fn incident_evidence_snapshot(
    detailed_timeline: bool,
    target: &IncidentEvidenceTarget,
    events: &[TimelineEvent],
    width: u16,
) -> Option<ScrollSnapshot> {
    if width == 0 || !incident_evidence_is_complete(target, events) {
        return None;
    }
    let source_width = detailed_source_width(width as usize);
    let mut highlighted_rx = false;
    let mut rows = Vec::new();
    for entry in project_incident_evidence(events) {
        let highlighted = entry.daemon_epoch == Some(target.daemon_epoch)
            && entry.event_kind == EventKind::Rx
            && entry.seq >= target.seq_start
            && entry.seq <= target.seq_end;
        highlighted_rx |= highlighted;
        let line = timeline_line(
            &entry,
            detailed_timeline,
            source_width,
            None,
            None,
            width as usize,
        );
        if highlighted {
            rows.extend(wrap_command_capture_line(line, width));
        } else {
            rows.extend(wrap_timeline_line(line, width));
        }
    }
    (highlighted_rx && !rows.is_empty()).then_some(ScrollSnapshot { rows })
}

fn command_evidence_snapshot(
    detailed_timeline: bool,
    target: &CommandEvidenceTarget,
    events: &[TimelineEvent],
    width: u16,
) -> Option<ScrollSnapshot> {
    if width == 0 {
        return None;
    }
    let completed_through = command_evidence_end_seq(target, events)?;
    if !exact_evidence_is_complete(
        &target.port,
        target.daemon_epoch,
        target.seq_start,
        completed_through,
        events,
    ) {
        return None;
    }
    if target.matchers.is_empty() {
        return Some(ScrollSnapshot {
            rows: wrap_command_fallback_line(&target.command, width),
        });
    }

    let entries = project_incident_evidence(events);
    let references = entries.iter().collect::<Vec<_>>();
    let capture = command_capture_for_target(target, &references);
    let (start, end) = capture.start.zip(capture.end)?;
    if !capture.highlight_available || start > end {
        return None;
    }
    let source_width = detailed_source_width(width as usize);
    let mut rows = Vec::new();
    for (index, entry) in entries.iter().enumerate().take(end + 1) {
        let highlighted = index >= start
            && entry.daemon_epoch == Some(target.daemon_epoch)
            && entry.event_kind == EventKind::Rx;
        let line = timeline_line(
            entry,
            detailed_timeline,
            source_width,
            None,
            None,
            width as usize,
        );
        if highlighted {
            rows.extend(wrap_command_capture_line(line, width));
        } else {
            rows.extend(wrap_timeline_line(line, width));
        }
    }
    (!rows.is_empty()).then_some(ScrollSnapshot { rows })
}

fn wrap_timeline_line(line: Line<'static>, width: u16) -> Vec<Line<'static>> {
    let width = width as usize;
    if width == 0 {
        return Vec::new();
    }

    let line_style = line.style;
    let alignment = line.alignment;
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut used = 0usize;

    for span in line.spans {
        let span_style = span.style;
        let mut piece = String::new();
        for character in span.content.chars() {
            let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
            if character_width > 0 && used > 0 && used + character_width > width {
                if !piece.is_empty() {
                    row.push(Span::styled(std::mem::take(&mut piece), span_style));
                }
                rows.push(Line {
                    style: line_style,
                    alignment,
                    spans: std::mem::take(&mut row),
                });
                used = 0;
            }

            piece.push(character);
            used = used.saturating_add(character_width);
            if used >= width {
                row.push(Span::styled(std::mem::take(&mut piece), span_style));
                rows.push(Line {
                    style: line_style,
                    alignment,
                    spans: std::mem::take(&mut row),
                });
                used = 0;
            }
        }
        if !piece.is_empty() {
            row.push(Span::styled(piece, span_style));
        }
    }

    if !row.is_empty() || rows.is_empty() {
        rows.push(Line {
            style: line_style,
            alignment,
            spans: row,
        });
    }
    rows
}

fn inset_border(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}

fn rect_contains(area: Rect, position: Position) -> bool {
    position.x >= area.x
        && position.x < area.x.saturating_add(area.width)
        && position.y >= area.y
        && position.y < area.y.saturating_add(area.height)
}

fn selection_point(
    output_inner: Rect,
    position: Position,
    row_count: usize,
) -> Option<SelectionPoint> {
    rect_contains(output_inner, position)
        .then(|| selection_point_clamped(output_inner, position, row_count))
        .flatten()
}

fn selection_point_clamped(
    output_inner: Rect,
    position: Position,
    row_count: usize,
) -> Option<SelectionPoint> {
    if output_inner.width == 0 || output_inner.height == 0 || row_count == 0 {
        return None;
    }
    let last_x = output_inner
        .x
        .saturating_add(output_inner.width.saturating_sub(1));
    let last_y = output_inner
        .y
        .saturating_add(output_inner.height.saturating_sub(1));
    let x = position.x.clamp(output_inner.x, last_x);
    let y = position.y.clamp(output_inner.y, last_y);
    Some(SelectionPoint {
        row: usize::from(y.saturating_sub(output_inner.y)).min(row_count - 1),
        column: x.saturating_sub(output_inner.x),
    })
}

fn selection_columns(start: SelectionPoint, end: SelectionPoint, row: usize) -> Option<(u16, u16)> {
    if row < start.row || row > end.row {
        return None;
    }
    Some((
        if row == start.row { start.column } else { 0 },
        if row == end.row { end.column } else { u16::MAX },
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelectionTokenKind {
    Word,
    Whitespace,
    Punctuation(char),
}

fn selection_token_kind(character: char) -> SelectionTokenKind {
    if character.is_alphanumeric()
        || matches!(character, '_' | '-' | '.' | ':' | '/' | '\\' | '@' | '~')
    {
        SelectionTokenKind::Word
    } else if character.is_whitespace() {
        SelectionTokenKind::Whitespace
    } else {
        SelectionTokenKind::Punctuation(character)
    }
}

fn word_selection_points(
    rows: &[String],
    point: SelectionPoint,
) -> Option<(SelectionPoint, SelectionPoint)> {
    let text = rows.get(point.row)?;
    let mut column = 0u16;
    let cells = text
        .chars()
        .map(|character| {
            let start = column;
            let width = UnicodeWidthChar::width(character).unwrap_or(0).max(1) as u16;
            column = column.saturating_add(width);
            (
                start,
                column.saturating_sub(1),
                selection_token_kind(character),
            )
        })
        .collect::<Vec<_>>();
    let selected = cells
        .iter()
        .position(|(start, end, _)| *start <= point.column && point.column <= *end)?;
    let kind = cells[selected].2;
    let first = (0..=selected)
        .rev()
        .take_while(|index| cells[*index].2 == kind)
        .last()
        .unwrap_or(selected);
    let last = (selected..cells.len())
        .take_while(|index| cells[*index].2 == kind)
        .last()
        .unwrap_or(selected);
    Some((
        SelectionPoint {
            row: point.row,
            column: cells[first].0,
        },
        SelectionPoint {
            row: point.row,
            column: cells[last].1,
        },
    ))
}

fn output_clicks_form_double_click(
    previous: OutputClick,
    current: SelectionPoint,
    rows: &[String],
    now: Instant,
) -> bool {
    if previous.point.row != current.row
        || now.saturating_duration_since(previous.at) > DOUBLE_CLICK_INTERVAL
    {
        return false;
    }
    let same_token = word_selection_points(rows, previous.point)
        .zip(word_selection_points(rows, current))
        .is_some_and(|(previous, current)| previous == current);
    // Terminal mouse reports cell coordinates rather than pixel coordinates.
    // Keep the conventional double-click feel if two rapid clicks straddle a
    // cell boundary, while the same-token check above permits larger movement
    // anywhere inside one word.
    same_token || previous.point.column.abs_diff(current.column) <= 1
}

fn line_plain_text(line: &Line<'_>) -> String {
    let mut text = String::new();
    for span in &line.spans {
        text.push_str(span.content.as_ref());
    }
    text
}

fn slice_display_columns(text: &str, from: u16, through: u16) -> String {
    let mut column = 0usize;
    let from = usize::from(from);
    let through = usize::from(through);
    text.chars()
        .filter(|character| {
            let width = UnicodeWidthChar::width(*character).unwrap_or(0);
            let start = column;
            let end = column.saturating_add(width.max(1));
            column = column.saturating_add(width);
            start <= through && end > from
        })
        .collect()
}

fn line_with_selection(line: Line<'static>, from: u16, through: u16) -> Line<'static> {
    let mut column = 0usize;
    let from = usize::from(from);
    let through = usize::from(through);
    let spans = line
        .spans
        .into_iter()
        .flat_map(|span| {
            let base_style = span.style;
            span.content
                .chars()
                .map(|character| {
                    let width = UnicodeWidthChar::width(character).unwrap_or(0);
                    let start = column;
                    let end = column.saturating_add(width.max(1));
                    column = column.saturating_add(width);
                    let selected = start <= through && end > from;
                    Span::styled(
                        character.to_string(),
                        if selected {
                            base_style.add_modifier(Modifier::REVERSED)
                        } else {
                            base_style
                        },
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();
    Line {
        style: line.style,
        alignment: line.alignment,
        spans,
    }
}

/// Renders one scrollback row. Compact mode is `{marker}{text}` where the
/// two-column marker is a colored "●" for TX/actor-attributed rows and two
/// spaces otherwise; a TX row whose exact device echo was merged shows a
/// softer unbolded "✓" in the same actor color instead. Detailed mode
/// additionally shows the event sequence and source columns. Stream rows get
/// inline keyword/prompt highlighting; system and gap rows keep their
/// whole-line style.
fn timeline_line(
    entry: &DisplayLine,
    detailed: bool,
    detailed_source_width: usize,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
    inner_width: usize,
) -> Line<'static> {
    if let Some(boundary) = entry.run_boundary {
        let style = match boundary {
            RunBoundary::Started => Style::default()
                .fg(Color::LightBlue)
                .add_modifier(Modifier::BOLD),
            RunBoundary::Ended => Style::default()
                .fg(Color::LightGreen)
                .add_modifier(Modifier::BOLD),
            RunBoundary::Aborted => Style::default()
                .fg(Color::LightRed)
                .add_modifier(Modifier::BOLD),
        };
        let label = format!(" {} ", safe_inline(&entry.text));
        let label_width = UnicodeWidthStr::width(label.as_str());
        let text = if label_width >= inner_width {
            pad_display(&label, inner_width)
        } else {
            let remaining = inner_width - label_width;
            format!(
                "{}{}{}",
                "─".repeat(remaining / 2),
                label,
                "─".repeat(remaining - remaining / 2)
            )
        };
        return Line::from(Span::styled(text, style));
    }
    let mut spans = Vec::new();
    match entry.marker_color {
        Some(color) => {
            let (glyph, modifier) = if entry.echoed {
                ("✓ ", Modifier::empty())
            } else {
                ("● ", Modifier::BOLD)
            };
            spans.push(Span::styled(
                glyph,
                Style::default().fg(color).add_modifier(modifier),
            ));
        }
        None => spans.push(Span::raw("  ")),
    }
    if detailed {
        spans.push(Span::styled(
            format!("#{:<8} ", entry.seq),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            format!("{} ", pad_display(&entry.source, detailed_source_width)),
            entry.source_style,
        ));
    }
    if let Some(style) = entry.solid_style {
        spans.push(Span::styled(entry.text.clone(), style));
        return Line::from(spans);
    }
    let mut cursor = 0;
    for (start, end, style) in highlight_spans(&entry.text, shell_prompt, uboot_prompt) {
        if start > cursor {
            spans.push(Span::raw(entry.text[cursor..start].to_string()));
        }
        spans.push(Span::styled(entry.text[start..end].to_string(), style));
        cursor = end;
    }
    spans.push(Span::raw(entry.text[cursor..].to_string()));
    Line::from(spans)
}

/// Detailed rows reserve about 48 columns for payload on an ordinary terminal
/// and expand the actor/source column only when there is room. A fixed
/// 28-column source used almost half of an 80-column viewport once the marker
/// and sequence were included, hiding the useful tail of Agent commands.
fn detailed_source_width(inner_width: usize) -> usize {
    inner_width.saturating_sub(62).clamp(10, 28)
}

fn draw_powerline_separator(frame: &mut Frame<'_>, app: &App, area: Rect, label_key: &'static str) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let accent = if app.focus == PaneFocus::RunHistory {
        Color::Cyan
    } else {
        Color::DarkGray
    };
    let label = format!(" {} ", tr(label_key));
    let occupied = UnicodeWidthStr::width(label.as_str()).saturating_add(2);
    let fill = "─".repeat((area.width as usize).saturating_sub(occupied));
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("", Style::default().fg(accent)),
            Span::styled(
                label,
                Style::default()
                    .fg(Color::Black)
                    .bg(accent)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("", Style::default().fg(accent)),
            Span::styled(fill, Style::default().fg(Color::DarkGray)),
        ])),
        area,
    );
}

fn draw_queue(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(tr("ui.queue.title"))
        .border_style(if app.focus == PaneFocus::Queue {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        });
    let inner = block.inner(area);
    let cards = queue_cards(app, inner.width);
    if cards.is_empty() || inner.height == 0 {
        return;
    }
    let height = inner.height as usize;
    let selected = app.queue_selection.as_ref().and_then(|selection| {
        (selection.port == app.selected_port()).then_some(selection.selected.min(cards.len() - 1))
    });

    let style_for = |card: &QueueCard| {
        if selected == Some(card.operation_index) {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else if card.sending {
            Style::default().fg(Color::DarkGray)
        } else {
            Style::default().fg(Color::White)
        }
    };
    let row = |card: &QueueCard, is_selected: bool| {
        let marker = if is_selected { "▶ " } else { "" };
        Line::from(Span::styled(
            format!("{marker}{} {}", card.header, card.command),
            style_for(card),
        ))
    };
    let start = selected
        .map(|selected| {
            selected
                .saturating_add(1)
                .saturating_sub(height)
                .min(cards.len().saturating_sub(height))
        })
        .unwrap_or(0);
    let mut rows = cards
        .iter()
        .skip(start)
        .take(height)
        .map(|card| row(card, selected == Some(card.operation_index)))
        .collect::<Vec<_>>();
    if selected.is_none() && cards.len() > height && height > 0 {
        let hidden = cards.len().saturating_sub(height.saturating_sub(1));
        rows.truncate(height.saturating_sub(1));
        rows.push(Line::from(Span::styled(
            trf("ui.queue.more", &[&hidden.to_string()]),
            Style::default().fg(Color::Yellow),
        )));
    }
    frame.render_widget(Paragraph::new(rows).block(block), area);
}

struct RunPanelRow {
    line: Line<'static>,
    command: Option<RunCommandKey>,
    step: Option<usize>,
    monitor: Option<Uuid>,
    matcher: Option<usize>,
    incident: Option<Uuid>,
}

fn run_status_text(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Active => tr("ui.run.status.active"),
        RunStatus::Completed => tr("ui.run.status.completed"),
        RunStatus::Aborted => tr("ui.run.status.aborted"),
    }
}

fn monitor_status_text(status: MonitorStatus) -> &'static str {
    match status {
        MonitorStatus::Running => tr("ui.monitor.status.running"),
        MonitorStatus::Completed => tr("ui.monitor.status.completed"),
        MonitorStatus::Stopped => tr("ui.monitor.status.stopped"),
        MonitorStatus::Failed => tr("ui.monitor.status.failed"),
    }
}

fn monitor_matcher_text(matcher: &MonitorMatcher) -> String {
    match matcher {
        MonitorMatcher::Contains(value) => {
            trf("ui.monitor.matcher.contains", &[&safe_inline(value)])
        }
        MonitorMatcher::Regex(value) => trf("ui.monitor.matcher.regex", &[&safe_inline(value)]),
    }
}

fn push_command_history_rows(
    app: &App,
    run: &RunHistoryEntry,
    command: &RunCommandRecord,
    width: u16,
    rows: &mut Vec<RunPanelRow>,
) {
    let view = app.current();
    let key = RunCommandKey {
        run_id: run.id,
        first_seq: command.first_seq,
    };
    let is_selected = view.selected_run_command_key() == Some(key);
    let expanded = view.expanded_run_command == Some(key);
    let style = if is_selected && app.focus == PaneFocus::RunHistory {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else if is_selected {
        Style::default().fg(Color::LightCyan)
    } else {
        Style::default().fg(Color::White)
    };
    let marker = if is_selected { "▶" } else { " " };
    let disclosure = if expanded { "▾" } else { "▸" };
    let label = if run.label.trim().is_empty() {
        tr("ui.run.unknown").to_string()
    } else {
        safe_inline(&run.label)
    };
    let run_label = trf("ui.run.header", &[run_status_text(run.status), &label]);
    let description = command
        .description
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("ui.run.description.missing").into());
    let title = format!("{run_label} · {description}");
    let available = width.saturating_sub(4).max(1);
    for (line_index, text) in wrap_queue_text(&title, available).into_iter().enumerate() {
        rows.push(RunPanelRow {
            line: Line::from(Span::styled(
                if line_index == 0 {
                    format!("{marker} {disclosure} {text}")
                } else {
                    format!("    {text}")
                },
                style,
            )),
            command: Some(key),
            step: None,
            monitor: None,
            matcher: None,
            incident: None,
        });
    }
    if !expanded {
        return;
    }
    for (step_index, step) in command.steps.iter().enumerate() {
        let mut payload = safe_inline(&String::from_utf8_lossy(&step.data));
        if payload.is_empty() {
            payload = tr("ui.run.command.empty").into();
        }
        if step.truncated {
            payload.push('…');
        }
        let detail_width = usize::from(width);
        let indentation = detail_width.saturating_sub(1).min(4);
        let child_selected =
            is_selected && command.steps.len() > 1 && view.selected_run_step == Some(step_index);
        let first_prefix = if command.steps.len() > 1 {
            format!(
                "{}{} {}. ",
                " ".repeat(indentation),
                if child_selected { "▶" } else { " " },
                step_index + 1
            )
        } else {
            " ".repeat(indentation)
        };
        let prefix_width = UnicodeWidthStr::width(first_prefix.as_str());
        let continuation_prefix = " ".repeat(prefix_width);
        let payload_width = detail_width
            .saturating_sub(prefix_width)
            .max(1)
            .min(usize::from(u16::MAX)) as u16;
        for (line_index, text) in wrap_queue_text(&payload, payload_width)
            .into_iter()
            .enumerate()
        {
            let detail_style = if child_selected && app.focus == PaneFocus::RunHistory {
                Style::default().fg(Color::Black).bg(Color::LightCyan)
            } else {
                Style::default().fg(Color::Gray)
            };
            let line = if line_index == 0 {
                Line::from(Span::styled(format!("{first_prefix}{text}"), detail_style))
            } else {
                Line::from(Span::styled(
                    format!("{continuation_prefix}{text}"),
                    detail_style,
                ))
            };
            rows.push(RunPanelRow {
                line,
                command: Some(key),
                step: Some(step_index),
                monitor: None,
                matcher: None,
                incident: None,
            });
        }
    }
}

fn push_monitor_history_rows(
    app: &App,
    entry: &MonitorHistoryEntry,
    width: u16,
    rows: &mut Vec<RunPanelRow>,
) {
    let view = app.current();
    let available = width.saturating_sub(4).max(1);
    let id = entry.monitor.id;
    let is_selected = view.selected_monitor == Some(id);
    let expanded = view.expanded_monitor == Some(id);
    let description = entry
        .monitor
        .spec
        .description
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("ui.monitor.unnamed").into());
    let style = if is_selected && app.focus == PaneFocus::RunHistory {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else if is_selected {
        Style::default().fg(Color::LightCyan)
    } else {
        Style::default().fg(Color::White)
    };
    rows.push(RunPanelRow {
        line: Line::from(Span::styled(
            trf(
                "ui.monitor.header",
                &[
                    if is_selected { "▶" } else { " " },
                    if expanded { "▾" } else { "▸" },
                    monitor_status_text(entry.monitor.status),
                    &description,
                    &entry.monitor.incident_count.to_string(),
                ],
            ),
            style,
        )),
        command: None,
        step: None,
        monitor: Some(id),
        matcher: None,
        incident: None,
    });
    if !expanded {
        return;
    }
    for (matcher_index, matcher) in entry.monitor.spec.matchers.iter().enumerate() {
        let matcher_selected = is_selected
            && view.selected_monitor_matcher == Some(matcher_index)
            && view.selected_monitor_incident.is_none();
        let matcher_style = if matcher_selected && app.focus == PaneFocus::RunHistory {
            Style::default().fg(Color::Black).bg(Color::LightCyan)
        } else {
            Style::default().fg(Color::Gray)
        };
        rows.push(RunPanelRow {
            line: Line::from(Span::styled(
                format!(
                    "    {} {}. {}",
                    if matcher_selected { "▶" } else { " " },
                    matcher_index + 1,
                    monitor_matcher_text(matcher)
                ),
                matcher_style,
            )),
            command: None,
            step: None,
            monitor: Some(id),
            matcher: Some(matcher_index),
            incident: None,
        });
        if view.selected_monitor_matcher != Some(matcher_index)
            || view.selected_monitor_incident.is_none()
        {
            continue;
        }
        for incident in entry.incidents.iter().filter(|incident| {
            incident
                .matches
                .iter()
                .any(|item| item.index == matcher_index)
        }) {
            let incident_selected = view.selected_monitor_incident == Some(incident.id);
            let incident_style = if incident_selected && app.focus == PaneFocus::RunHistory {
                Style::default().fg(Color::Black).bg(Color::LightCyan)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            let preview = safe_inline(&incident.preview);
            for (line_index, text) in wrap_queue_text(&preview, available.saturating_sub(4))
                .into_iter()
                .enumerate()
            {
                rows.push(RunPanelRow {
                    line: Line::from(Span::styled(
                        if line_index == 0 {
                            format!(
                                "        {} #{} {text}",
                                if incident_selected { "▶" } else { " " },
                                incident.incident_seq
                            )
                        } else {
                            format!("          {text}")
                        },
                        incident_style,
                    )),
                    command: None,
                    step: None,
                    monitor: Some(id),
                    matcher: Some(matcher_index),
                    incident: Some(incident.id),
                });
            }
        }
    }
}

fn run_history_rows(app: &App, width: u16) -> Vec<RunPanelRow> {
    let view = app.current();
    let actions = view.history_action_keys();
    if actions.is_empty() {
        return vec![RunPanelRow {
            line: Line::from(Span::styled(
                tr("ui.run.none"),
                Style::default().fg(Color::DarkGray),
            )),
            command: None,
            step: None,
            monitor: None,
            matcher: None,
            incident: None,
        }];
    }
    let mut rows = Vec::new();
    for action in actions {
        match action {
            HistoryActionKey::Command(key) => {
                let Some(run) = view.run_history.iter().find(|run| run.id == key.run_id) else {
                    continue;
                };
                let Some(command) = run
                    .commands
                    .iter()
                    .find(|command| command.first_seq == key.first_seq)
                else {
                    continue;
                };
                push_command_history_rows(app, run, command, width, &mut rows);
            }
            HistoryActionKey::Monitor(id) => {
                let Some(entry) = view.monitor(id) else {
                    continue;
                };
                push_monitor_history_rows(app, entry, width, &mut rows);
            }
        }
    }
    rows
}

fn draw_run_history(frame: &mut Frame<'_>, app: &App, area: Rect, framed: bool) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(if app.current().run_history_limited {
            tr("ui.run.title.limited")
        } else {
            tr("ui.run.title")
        })
        .border_style(if app.focus == PaneFocus::RunHistory {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default().fg(Color::DarkGray)
        });
    let inner = if framed { block.inner(area) } else { area };
    if inner.height == 0 || inner.width == 0 {
        if framed {
            frame.render_widget(block, area);
        }
        return;
    }
    let rows = run_history_rows(app, inner.width);
    if rows.is_empty() {
        if framed {
            frame.render_widget(Paragraph::new(tr("ui.run.none")).block(block), area);
        } else {
            frame.render_widget(Paragraph::new(tr("ui.run.none")), area);
        }
        return;
    }
    let height = inner.height as usize;
    let selected_row = if let Some(monitor) = app.current().selected_monitor {
        rows.iter()
            .position(|row| {
                row.monitor == Some(monitor)
                    && match app.current().selected_monitor_incident {
                        Some(incident) => row.incident == Some(incident),
                        None => match app.current().selected_monitor_matcher {
                            Some(matcher) => row.matcher == Some(matcher) && row.incident.is_none(),
                            None => row.matcher.is_none() && row.incident.is_none(),
                        },
                    }
            })
            .unwrap_or(0)
    } else {
        app.current()
            .selected_run_command_key()
            .and_then(|selected| {
                rows.iter().position(|row| {
                    row.command == Some(selected)
                        && match app.current().selected_run_step {
                            Some(step) => row.step == Some(step),
                            None => row.step.is_none(),
                        }
                })
            })
            .unwrap_or(0)
    };
    let max_start = rows.len().saturating_sub(height);
    let start = selected_row
        .saturating_sub(2)
        .saturating_add(app.current().run_detail_scroll)
        .min(max_start);
    let paragraph = Paragraph::new(
        rows.into_iter()
            .skip(start)
            .take(height)
            .map(|row| row.line)
            .collect::<Vec<_>>(),
    );
    if framed {
        frame.render_widget(paragraph.block(block), area);
    } else {
        frame.render_widget(paragraph, area);
    }
}

fn draw_input(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if let Some(search) = &app.current().history_search {
        let matched = search
            .match_index
            .map(|index| safe_inline(&app.current().history[index]))
            .unwrap_or_default();
        let text = trf("ui.search.query", &[&search.query, &matched]);
        frame.render_widget(
            Paragraph::new(text).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(tr("ui.search.title")),
            ),
            area,
        );
        return;
    }
    let block =
        Block::default()
            .borders(Borders::ALL)
            .border_style(if app.focus == PaneFocus::Input {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            });
    let inner = block.inner(area);
    let agent_hint = app
        .current()
        .active_agent_run()
        .map(|run| trf("ui.input.agent", &[&safe_inline(&run.label)]));
    let (text, cursor_column, title) = match app.current_mode() {
        InputMode::Line => {
            if app.current().draft.is_empty()
                && let Some(agent_hint) = agent_hint
            {
                (
                    Line::from(Span::styled(
                        agent_hint,
                        Style::default().fg(Color::DarkGray),
                    )),
                    None,
                    input_title(app, InputMode::Line),
                )
            } else {
                let (text, cursor_column) = line_input_projection(
                    &app.current().draft,
                    app.current().draft_cursor,
                    inner.width,
                );
                (
                    Line::from(text),
                    Some(cursor_column),
                    input_title(app, InputMode::Line),
                )
            }
        }
        InputMode::Raw => {
            if let Some(agent_hint) = agent_hint {
                (
                    Line::from(Span::styled(
                        agent_hint,
                        Style::default().fg(Color::DarkGray),
                    )),
                    None,
                    input_title(app, InputMode::Raw),
                )
            } else {
                (
                    Line::from(format!("> {}", tr("ui.input.raw.text"))),
                    None,
                    input_title(app, InputMode::Raw),
                )
            }
        }
    };
    let text = match (app.focus == PaneFocus::Input, cursor_column) {
        (true, Some(cursor)) => {
            line_with_software_cursor(line_plain_text(&text), cursor, app.software_cursor_visible)
        }
        _ => text,
    };
    frame.render_widget(Paragraph::new(text).block(block.title(title)), area);
}

fn input_title(app: &App, mode: InputMode) -> String {
    let port = &app.current().snapshot.config.port;
    let Some(writes) = app
        .pending_writes
        .get(port)
        .filter(|writes| !writes.is_empty())
    else {
        return match mode {
            InputMode::Line => tr("ui.input.title.line").into(),
            InputMode::Raw => tr("ui.input.title.raw").into(),
        };
    };
    match mode {
        InputMode::Line => {
            let preview = writes
                .iter()
                .find(|write| write.kind == PendingWriteKind::Line)
                .map(|write| {
                    let text = safe_inline(&String::from_utf8_lossy(&write.data));
                    truncate_display(&text, 40)
                })
                .unwrap_or_else(|| {
                    let bytes = writes.iter().map(|write| write.data.len()).sum::<usize>();
                    trf("ui.input.queued.raw", &[&bytes.to_string()])
                });
            trf(
                "ui.input.title.line.queued",
                &[&queued_line_count(writes).to_string(), &preview],
            )
        }
        InputMode::Raw => {
            let bytes = writes.iter().map(|write| write.data.len()).sum::<usize>();
            trf("ui.input.title.raw.queued", &[&bytes.to_string()])
        }
    }
}

fn truncate_display(value: &str, max_width: usize) -> String {
    if UnicodeWidthStr::width(value) <= max_width {
        return value.trim().to_string();
    }
    let mut output = String::new();
    let mut width = 0usize;
    let content_width = max_width.saturating_sub(1);
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if width.saturating_add(character_width) > content_width {
            break;
        }
        output.push(character);
        width = width.saturating_add(character_width);
    }
    if max_width > 0 {
        output.push('…');
    }
    output.trim().to_string()
}

/// Returns one non-wrapping LINE-mode input row and the cursor column inside
/// the bordered input area's inner rectangle. The view follows the logical
/// cursor horizontally and counts CJK characters by terminal display width.
fn line_input_projection(draft: &[char], cursor: usize, inner_width: u16) -> (String, u16) {
    const PREFIX: &str = "> ";
    let prefix_width = UnicodeWidthStr::width(PREFIX) as u16;
    if inner_width <= prefix_width {
        return (PREFIX.chars().take(inner_width as usize).collect(), 0);
    }

    let cursor = cursor.min(draft.len());
    // Reserve one terminal cell for the visible cursor, including when it is
    // positioned just after the last character.
    let before_budget = inner_width.saturating_sub(prefix_width).saturating_sub(1) as usize;
    let mut start = cursor;
    let mut before_width = 0usize;
    while start > 0 {
        let width = unicode_width::UnicodeWidthChar::width(draft[start - 1]).unwrap_or(0);
        if before_width.saturating_add(width) > before_budget {
            break;
        }
        start -= 1;
        before_width += width;
    }

    let content_budget = inner_width.saturating_sub(prefix_width) as usize;
    let mut visible = String::new();
    let mut visible_width = 0usize;
    for &character in &draft[start..] {
        let width = unicode_width::UnicodeWidthChar::width(character).unwrap_or(0);
        if visible_width.saturating_add(width) > content_budget {
            break;
        }
        visible.push(character);
        visible_width += width;
    }

    (
        format!("{PREFIX}{}", safe_inline(&visible)),
        prefix_width.saturating_add(before_width as u16),
    )
}

/// Render the slow-blinking block cursor in the Ratatui buffer instead of
/// repeatedly moving the terminal emulator's hardware cursor. Live serial RX
/// can redraw at up to 30 FPS without restarting this independently scheduled
/// phase, which avoids the emulator's high-frequency blink/flicker artifact.
fn line_with_software_cursor(
    text: String,
    cursor_column: u16,
    cursor_visible: bool,
) -> Line<'static> {
    let mut before = String::new();
    let mut after = String::new();
    let mut cursor = None;
    let mut column = 0u16;
    for character in text.chars() {
        let width = UnicodeWidthChar::width(character).unwrap_or(0).max(1) as u16;
        if cursor.is_none() && column == cursor_column {
            cursor = Some(character.to_string());
        } else if cursor.is_some() {
            after.push(character);
        } else {
            before.push(character);
        }
        column = column.saturating_add(width);
    }
    let cursor = cursor.unwrap_or_else(|| " ".into());
    let cursor_style = if cursor_visible {
        Style::default().add_modifier(Modifier::REVERSED)
    } else {
        Style::default()
    };
    Line::from(vec![
        Span::raw(before),
        Span::styled(cursor, cursor_style),
        Span::raw(after),
    ])
}

fn draw_help_line(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if let Some(status) = app.active_status_notice(Instant::now()) {
        frame.render_widget(
            Paragraph::new(safe_inline(status))
                .alignment(Alignment::Center)
                .style(Style::default().fg(Color::Yellow)),
            area,
        );
        return;
    }
    let scroll = tr("ui.scroll.plain");
    frame.render_widget(
        Paragraph::new(trf("ui.helpline", &[scroll]))
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_help(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let width = area.width.saturating_sub(2).clamp(1, 92);
    let height = area.height.saturating_sub(2).clamp(1, 38);
    let popup = centered_rect(width, height, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(tr("help.title"));
    let inner = block.inner(popup);
    let lines = help_lines(app);
    let visible_height = usize::from(inner.height).max(1);
    let max_scroll = lines.len().saturating_sub(visible_height);
    let scroll = app.help_scroll.min(max_scroll);
    let footer = trf(
        "help.position",
        &[
            &(scroll.saturating_add(1)).min(lines.len()).to_string(),
            &(scroll.saturating_add(visible_height))
                .min(lines.len())
                .to_string(),
            &lines.len().to_string(),
        ],
    );
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .block(block.title_bottom(Line::from(footer).alignment(Alignment::Center)))
            .scroll((scroll.min(u16::MAX as usize) as u16, 0)),
        popup,
    );
}

fn help_heading(key: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        tr(key),
        Style::default()
            .fg(Color::LightCyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn help_shortcut(key: &'static str, description: &'static str) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            pad_display(tr(key), 28),
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(tr(description), Style::default().fg(Color::White)),
    ])
}

/// Builds explicit visual rows instead of joining one large string and
/// relying on Paragraph wrapping. Every shortcut remains a distinct row;
/// section gaps are preserved and the popup can scroll on narrow terminals.
fn help_lines(_app: &App) -> Vec<Line<'static>> {
    vec![
        help_heading("help.group.navigation"),
        help_shortcut("help.key.switch", "help.desc.switch"),
        help_shortcut("help.key.history.select", "help.desc.history.select"),
        help_shortcut("help.key.history.expand", "help.desc.history.expand"),
        help_shortcut("help.key.history.panel", "help.desc.history.panel"),
        help_shortcut("help.key.scroll", "help.desc.scroll"),
        help_shortcut("help.key.follow", "help.desc.follow"),
        help_shortcut("help.key.menu", "help.desc.menu"),
        help_shortcut("help.key.profile", "help.desc.profile"),
        help_shortcut("help.key.search.output", "help.desc.search.output"),
        Line::default(),
        help_heading("help.group.line"),
        help_shortcut("help.key.enter", "help.desc.enter"),
        help_shortcut("help.key.alt.enter", "help.desc.alt.enter"),
        help_shortcut("help.key.input.search", "help.desc.input.search"),
        help_shortcut("help.key.complete", "help.desc.complete"),
        help_shortcut("help.key.paste", "help.desc.paste"),
        Line::default(),
        help_heading("help.group.control"),
        help_shortcut("help.key.takeover", "help.desc.takeover"),
        help_shortcut("help.key.release", "help.desc.release"),
        help_shortcut("help.key.mode", "help.desc.mode"),
        help_shortcut("help.key.interrupt", "help.desc.interrupt"),
        help_shortcut("help.key.lang", "help.desc.lang"),
        help_shortcut("help.key.quit", "help.desc.quit"),
        Line::default(),
        Line::from(Span::styled(
            tr("help.close"),
            Style::default().fg(Color::DarkGray),
        )),
    ]
}

fn draw_menu(frame: &mut Frame<'_>, app: &App, menu: &MenuState, area: Rect) {
    let width = area.width.saturating_sub(4).clamp(1, 100);
    let height = area.height.saturating_sub(2).clamp(1, 34);
    let popup = centered_rect(width, height, area);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", menu_page_title(menu.page)))
        .border_style(Style::default().fg(Color::Cyan));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    let chunks = Layout::vertical([
        Constraint::Length(2),
        Constraint::Min(3),
        Constraint::Length(1),
    ])
    .split(inner);

    let port = safe_inline(&app.selected_port());
    let transport = app
        .current()
        .snapshot
        .config
        .transport_profile
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.unbound").into());
    let model = app
        .current()
        .snapshot
        .config
        .model_profile
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.generic").into());
    let model_name = app
        .current()
        .snapshot
        .config
        .model_name
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.unbound").into());
    let header = trf("menu.current", &[&port, &transport, &model, &model_name]);
    frame.render_widget(
        Paragraph::new(header).style(Style::default().fg(Color::LightCyan)),
        chunks[0],
    );

    let rows = menu_rows(app, menu);
    let viewport = chunks[1].height as usize;
    let selected_row = menu_selected_visual_row(menu);
    let max_start = rows.len().saturating_sub(viewport);
    let start = if menu.page == MenuPage::Help {
        menu.help_scroll.min(max_start)
    } else {
        selected_row
            .map(|selected| selected.saturating_add(1).saturating_sub(viewport))
            .unwrap_or(0)
            .min(max_start)
    };
    frame.render_widget(
        Paragraph::new(
            rows.into_iter()
                .skip(start)
                .take(viewport)
                .collect::<Vec<_>>(),
        ),
        chunks[1],
    );
    frame.render_widget(
        Paragraph::new(menu_footer(menu.page))
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::DarkGray)),
        chunks[2],
    );

    if let Some(confirmation) = menu.confirmation.as_ref() {
        draw_menu_confirmation(frame, confirmation, popup);
    } else if let Some(help) = menu.field_help.as_ref() {
        draw_menu_field_help(frame, help, popup);
    }
}

fn draw_menu_confirmation(frame: &mut Frame<'_>, confirmation: &MenuConfirmation, parent: Rect) {
    let width = parent.width.saturating_sub(4).clamp(1, 92);
    let desired_height = u16::try_from(confirmation.lines.len())
        .unwrap_or(u16::MAX)
        .saturating_add(4);
    let height = desired_height.min(parent.height.saturating_sub(2)).max(1);
    let popup = centered_rect(width, height, parent);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", confirmation.title))
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    frame.render_widget(block, popup);
    let chunks = Layout::vertical([Constraint::Min(1), Constraint::Length(1)]).split(inner);
    let viewport = chunks[0].height as usize;
    let scroll = confirmation
        .scroll
        .min(confirmation.lines.len().saturating_sub(viewport));
    let lines = confirmation
        .lines
        .iter()
        .skip(scroll)
        .cloned()
        .map(Line::from)
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .style(Style::default().fg(Color::White)),
        chunks[0],
    );
    frame.render_widget(
        Paragraph::new(tr("menu.profile.shared.footer"))
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::Yellow)),
        chunks[1],
    );
}

fn draw_menu_field_help(frame: &mut Frame<'_>, help: &str, parent: Rect) {
    let width = parent.width.saturating_sub(8).clamp(1, 72);
    let popup = centered_rect(width, 7.min(parent.height).max(1), parent);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", tr("menu.field.help.title")))
        .border_style(Style::default().fg(Color::Cyan));
    frame.render_widget(
        Paragraph::new(help.to_owned())
            .wrap(Wrap { trim: false })
            .block(block.title_bottom(
                Line::from(tr("menu.field.help.close")).alignment(Alignment::Center),
            )),
        popup,
    );
}

fn menu_page_title(page: MenuPage) -> &'static str {
    match page {
        MenuPage::Root => tr("menu.title"),
        MenuPage::Profiles => tr("menu.profile.title"),
        MenuPage::CreateProfiles => tr("menu.create.title"),
        MenuPage::CreateTransportProfile => tr("menu.create.transport.title"),
        MenuPage::CreateModelProfile => tr("menu.create.model.title"),
        MenuPage::Settings => tr("menu.settings.title"),
        MenuPage::ModelFamilies => tr("menu.model.family.title"),
        MenuPage::ModelNames => tr("menu.model.name.title"),
        MenuPage::DisplaySettings => tr("menu.display.title"),
        MenuPage::McpSettings => tr("menu.mcp.title"),
        MenuPage::Help => tr("menu.help.title"),
    }
}

fn menu_footer(page: MenuPage) -> &'static str {
    match page {
        MenuPage::Help => tr("menu.footer.help"),
        _ => tr("menu.footer"),
    }
}

fn selected_menu_line(index: usize, selected: usize, text: String) -> Line<'static> {
    let active = index == selected;
    let marker = if active { "▶" } else { " " };
    Line::from(Span::styled(
        format!("{marker} {text}"),
        if active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::White)
        },
    ))
}

fn indented_menu_line(index: usize, selected: usize, text: String) -> Line<'static> {
    selected_menu_line(index, selected, format!("    {text}"))
}

fn menu_section_heading(key: &'static str) -> Line<'static> {
    Line::from(Span::styled(
        tr(key),
        Style::default()
            .fg(Color::LightCyan)
            .add_modifier(Modifier::BOLD),
    ))
}

fn data_bits_label(value: DataBits) -> &'static str {
    match value {
        DataBits::Five => "5",
        DataBits::Six => "6",
        DataBits::Seven => "7",
        DataBits::Eight => "8",
    }
}

fn parity_label(value: Parity) -> &'static str {
    match value {
        Parity::None => tr("menu.detail.parity.none"),
        Parity::Odd => tr("menu.detail.parity.odd"),
        Parity::Even => tr("menu.detail.parity.even"),
    }
}

fn stop_bits_label(value: StopBits) -> &'static str {
    match value {
        StopBits::One => "1",
        StopBits::Two => "2",
    }
}

fn flow_control_label(value: FlowControl) -> &'static str {
    match value {
        FlowControl::None => tr("menu.detail.flow.none"),
        FlowControl::Software => tr("menu.detail.flow.software"),
        FlowControl::Hardware => tr("menu.detail.flow.hardware"),
    }
}

fn on_off(value: bool) -> &'static str {
    if value {
        tr("menu.value.on")
    } else {
        tr("menu.value.off")
    }
}

fn enabled_disabled(value: bool) -> &'static str {
    if value {
        tr("menu.value.enabled")
    } else {
        tr("menu.value.disabled")
    }
}

fn eol_label(value: Option<&str>) -> String {
    match value {
        Some("\r") => "CR".into(),
        Some("\n") => "LF".into(),
        Some("\r\n") => "CRLF".into(),
        Some("") => tr("menu.detail.eol.none").into(),
        Some(value) => safe_inline(value),
        None => tr("menu.detail.eol.inherit").into(),
    }
}

fn echo_label(value: Option<EchoMode>) -> &'static str {
    match value {
        Some(EchoMode::On) => tr("menu.detail.echo.on"),
        Some(EchoMode::Off) => tr("menu.detail.echo.off"),
        Some(EchoMode::Auto) => tr("menu.detail.echo.auto"),
        None => tr("menu.detail.eol.inherit"),
    }
}

fn optional_profile_value(value: Option<&str>) -> String {
    value
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.unbound").into())
}

fn optional_number<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(
        || tr("menu.detail.eol.inherit").into(),
        |value| value.to_string(),
    )
}

fn orphan_run_timeout_label(seconds: u64) -> String {
    if seconds == 0 {
        tr("menu.run.timeout.unlimited").into()
    } else {
        trf("menu.run.timeout.seconds", &[&seconds.to_string()])
    }
}

fn baud_rate_options(current: u32) -> Vec<u32> {
    let mut values = vec![
        9_600, 19_200, 38_400, 57_600, 115_200, 230_400, 460_800, 921_600,
    ];
    values.push(current);
    values.sort_unstable();
    values.dedup();
    values
}

fn data_bits_options() -> Vec<MenuChoiceOption> {
    [
        DataBits::Five,
        DataBits::Six,
        DataBits::Seven,
        DataBits::Eight,
    ]
    .into_iter()
    .map(|value| MenuChoiceOption {
        label: data_bits_label(value).into(),
        value: MenuChoiceValue::DataBits(value),
    })
    .collect()
}

fn data_bits_index(value: DataBits) -> usize {
    match value {
        DataBits::Five => 0,
        DataBits::Six => 1,
        DataBits::Seven => 2,
        DataBits::Eight => 3,
    }
}

fn parity_options() -> Vec<MenuChoiceOption> {
    [Parity::None, Parity::Even, Parity::Odd]
        .into_iter()
        .map(|value| MenuChoiceOption {
            label: parity_label(value).into(),
            value: MenuChoiceValue::Parity(value),
        })
        .collect()
}

fn parity_index(value: Parity) -> usize {
    match value {
        Parity::None => 0,
        Parity::Even => 1,
        Parity::Odd => 2,
    }
}

fn stop_bits_options() -> Vec<MenuChoiceOption> {
    [StopBits::One, StopBits::Two]
        .into_iter()
        .map(|value| MenuChoiceOption {
            label: stop_bits_label(value).into(),
            value: MenuChoiceValue::StopBits(value),
        })
        .collect()
}

fn stop_bits_index(value: StopBits) -> usize {
    match value {
        StopBits::One => 0,
        StopBits::Two => 1,
    }
}

fn flow_control_options() -> Vec<MenuChoiceOption> {
    [
        FlowControl::None,
        FlowControl::Software,
        FlowControl::Hardware,
    ]
    .into_iter()
    .map(|value| MenuChoiceOption {
        label: flow_control_label(value).into(),
        value: MenuChoiceValue::FlowControl(value),
    })
    .collect()
}

fn flow_control_index(value: FlowControl) -> usize {
    match value {
        FlowControl::None => 0,
        FlowControl::Software => 1,
        FlowControl::Hardware => 2,
    }
}

fn bool_options() -> Vec<MenuChoiceOption> {
    [false, true]
        .into_iter()
        .map(|value| MenuChoiceOption {
            label: on_off(value).into(),
            value: MenuChoiceValue::Bool(value),
        })
        .collect()
}

fn eol_options() -> Vec<MenuChoiceOption> {
    [
        (tr("menu.detail.eol.inherit").into(), None),
        ("CR".into(), Some("\r".into())),
        ("LF".into(), Some("\n".into())),
        ("CRLF".into(), Some("\r\n".into())),
        (tr("menu.detail.eol.none").into(), Some(String::new())),
    ]
    .into_iter()
    .map(|(label, value)| MenuChoiceOption {
        label,
        value: MenuChoiceValue::Eol(value),
    })
    .collect()
}

fn eol_index(value: Option<&str>) -> usize {
    match value {
        None => 0,
        Some("\r") => 1,
        Some("\n") => 2,
        Some("\r\n") => 3,
        Some(_) => 4,
    }
}

fn echo_options() -> Vec<MenuChoiceOption> {
    [
        (tr("menu.detail.eol.inherit"), None),
        (tr("menu.detail.echo.on"), Some(EchoMode::On)),
        (tr("menu.detail.echo.off"), Some(EchoMode::Off)),
        (tr("menu.detail.echo.auto"), Some(EchoMode::Auto)),
    ]
    .into_iter()
    .map(|(label, value)| MenuChoiceOption {
        label: label.into(),
        value: MenuChoiceValue::Echo(value),
    })
    .collect()
}

fn echo_index(value: Option<EchoMode>) -> usize {
    match value {
        None => 0,
        Some(EchoMode::On) => 1,
        Some(EchoMode::Off) => 2,
        Some(EchoMode::Auto) => 3,
    }
}

fn menu_rows(app: &App, menu: &MenuState) -> Vec<Line<'static>> {
    let mut rows = match menu.page {
        MenuPage::Root => [
            tr("menu.root.profile"),
            tr("menu.root.create"),
            tr("menu.root.settings"),
            tr("menu.root.help"),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, text)| selected_menu_line(index, menu.selected, text.into()))
        .collect(),
        MenuPage::Profiles => {
            let Some(editor) = menu.profile_editor.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let changed = editor.changed();
            let values = [
                trf("menu.current.row.port", &[&safe_inline(&editor.port)]),
                trf(
                    "menu.current.row.transport",
                    &[&editor
                        .transport_binding
                        .as_deref()
                        .map(safe_inline)
                        .unwrap_or_else(|| tr("menu.value.unbound").into())],
                ),
                trf(
                    "menu.current.row.baud",
                    &[&editor.transport.baud_rate.to_string()],
                ),
                trf(
                    "menu.current.row.data",
                    &[data_bits_label(editor.transport.data_bits)],
                ),
                trf(
                    "menu.current.row.parity",
                    &[parity_label(editor.transport.parity)],
                ),
                trf(
                    "menu.current.row.stop",
                    &[stop_bits_label(editor.transport.stop_bits)],
                ),
                trf(
                    "menu.current.row.flow",
                    &[flow_control_label(editor.transport.flow_control)],
                ),
                trf("menu.current.row.dtr", &[on_off(editor.transport.dtr)]),
                trf("menu.current.row.rts", &[on_off(editor.transport.rts)]),
                trf(
                    "menu.current.row.auto",
                    &[enabled_disabled(editor.transport.auto_open)],
                ),
                trf(
                    "menu.current.row.device",
                    &[&editor
                        .model_profile_binding
                        .as_deref()
                        .map(safe_inline)
                        .unwrap_or_else(|| tr("menu.value.unbound").into())],
                ),
                trf(
                    "menu.current.row.model.name",
                    &[&editor
                        .model_name
                        .as_deref()
                        .map(safe_inline)
                        .unwrap_or_else(|| tr("menu.value.unbound").into())],
                ),
                trf(
                    "menu.current.row.eol",
                    &[&eol_label(editor.device.write_eol.as_deref())],
                ),
                trf("menu.current.row.echo", &[echo_label(editor.device.echo)]),
                trf(
                    "menu.current.row.shell",
                    &[&optional_profile_value(
                        editor.device.shell_prompt.as_deref(),
                    )],
                ),
                trf(
                    "menu.current.row.uboot",
                    &[&optional_profile_value(
                        editor.device.uboot_prompt.as_deref(),
                    )],
                ),
                trf(
                    "menu.current.row.chunk",
                    &[&optional_number(editor.device.write_chunk_size)],
                ),
                trf(
                    "menu.current.row.delay",
                    &[&optional_number(editor.device.write_chunk_delay_ms)],
                ),
                if changed {
                    tr("menu.current.row.apply.changed").into()
                } else {
                    tr("menu.current.row.apply.clean").into()
                },
            ];
            let mut rows = Vec::with_capacity(values.len() + 4);
            rows.push(menu_section_heading("menu.current.section.serial"));
            rows.extend(
                values[..10]
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(index, text)| indented_menu_line(index, menu.selected, text)),
            );
            rows.push(menu_section_heading("menu.current.section.model"));
            rows.extend(
                values[10..18]
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(offset, text)| {
                        let index = offset + 10;
                        indented_menu_line(index, menu.selected, text)
                    }),
            );
            rows.push(Line::default());
            rows.push(menu_section_heading("menu.current.section.actions"));
            rows.push(indented_menu_line(18, menu.selected, values[18].clone()));
            rows
        }
        MenuPage::CreateProfiles => [tr("menu.create.transport"), tr("menu.create.model")]
            .into_iter()
            .enumerate()
            .map(|(index, text)| selected_menu_line(index, menu.selected, text.into()))
            .collect(),
        MenuPage::CreateTransportProfile => {
            let Some(profile) = menu.create_transport.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let values = [
                trf("menu.create.row.name", &[&safe_inline(&profile.name)]),
                trf("menu.current.row.baud", &[&profile.baud_rate.to_string()]),
                trf(
                    "menu.current.row.data",
                    &[data_bits_label(profile.data_bits)],
                ),
                trf("menu.current.row.parity", &[parity_label(profile.parity)]),
                trf(
                    "menu.current.row.stop",
                    &[stop_bits_label(profile.stop_bits)],
                ),
                trf(
                    "menu.current.row.flow",
                    &[flow_control_label(profile.flow_control)],
                ),
                trf("menu.current.row.dtr", &[on_off(profile.dtr)]),
                trf("menu.current.row.rts", &[on_off(profile.rts)]),
                trf(
                    "menu.current.row.auto",
                    &[enabled_disabled(profile.auto_open)],
                ),
                tr("menu.create.row.save").into(),
            ];
            values
                .into_iter()
                .enumerate()
                .map(|(index, text)| selected_menu_line(index, menu.selected, text))
                .collect()
        }
        MenuPage::CreateModelProfile => {
            let Some(profile) = menu.create_model.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let values = [
                trf("menu.create.row.name", &[&safe_inline(&profile.name)]),
                trf(
                    "menu.create.row.model.names",
                    &[&safe_inline(&profile.model_names.join(", "))],
                ),
                trf(
                    "menu.current.row.eol",
                    &[&eol_label(profile.write_eol.as_deref())],
                ),
                trf("menu.current.row.echo", &[echo_label(profile.echo)]),
                trf(
                    "menu.current.row.shell",
                    &[&optional_profile_value(profile.shell_prompt.as_deref())],
                ),
                trf(
                    "menu.current.row.uboot",
                    &[&optional_profile_value(profile.uboot_prompt.as_deref())],
                ),
                trf(
                    "menu.current.row.chunk",
                    &[&optional_number(profile.write_chunk_size)],
                ),
                trf(
                    "menu.current.row.delay",
                    &[&optional_number(profile.write_chunk_delay_ms)],
                ),
                tr("menu.create.row.save").into(),
            ];
            values
                .into_iter()
                .enumerate()
                .map(|(index, text)| selected_menu_line(index, menu.selected, text))
                .collect()
        }
        MenuPage::Settings => [tr("menu.root.display"), tr("menu.root.mcp")]
            .into_iter()
            .enumerate()
            .map(|(index, text)| selected_menu_line(index, menu.selected, text.into()))
            .collect(),
        MenuPage::ModelFamilies => menu
            .catalog
            .as_ref()
            .map(|catalog| {
                catalog
                    .model_profiles
                    .iter()
                    .filter(|profile| !profile.model_names.is_empty())
                    .enumerate()
                    .map(|(index, profile)| {
                        selected_menu_line(index, menu.selected, safe_inline(&profile.name))
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![Line::from(tr("menu.loading"))]),
        MenuPage::ModelNames => menu
            .catalog
            .as_ref()
            .and_then(|catalog| {
                let family = menu.model_family.as_deref()?;
                catalog
                    .model_profiles
                    .iter()
                    .find(|profile| profile.name == family)
            })
            .map(|profile| {
                profile
                    .model_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| {
                        selected_menu_line(index, menu.selected, safe_inline(name))
                    })
                    .collect()
            })
            .unwrap_or_else(|| vec![Line::from(tr("menu.loading"))]),
        MenuPage::DisplaySettings => vec![selected_menu_line(
            0,
            menu.selected,
            trf(
                "menu.display.history.rows",
                &[&app.agent_history_rows.to_string()],
            ),
        )],
        MenuPage::McpSettings => vec![selected_menu_line(
            0,
            menu.selected,
            trf(
                "menu.run.timeout.row",
                &[&orphan_run_timeout_label(app.orphan_run_timeout_seconds)],
            ),
        )],
        MenuPage::Help => help_lines(app),
    };

    if let Some(anchor) = menu_selected_visual_row_base(menu) {
        if let Some(choice) = menu.choice.as_ref() {
            let insert_at = (anchor + 1).min(rows.len());
            let options = choice
                .options
                .iter()
                .enumerate()
                .map(|(index, option)| {
                    let active = index == choice.selected;
                    Line::from(Span::styled(
                        format!("      {} {}", if active { "▶" } else { " " }, option.label),
                        if active {
                            Style::default().fg(Color::Black).bg(Color::LightCyan)
                        } else {
                            Style::default().fg(Color::Gray)
                        },
                    ))
                })
                .collect::<Vec<_>>();
            rows.splice(insert_at..insert_at, options);
        } else if let Some(prompt) = menu.prompt.as_ref() {
            let insert_at = (anchor + 1).min(rows.len());
            rows.insert(
                insert_at,
                inline_menu_prompt_line(prompt, app.software_cursor_visible),
            );
        }
    }
    rows
}

fn menu_selected_visual_row(menu: &MenuState) -> Option<usize> {
    let base = menu_selected_visual_row_base(menu)?;
    if let Some(choice) = menu.choice.as_ref() {
        Some(base + 1 + choice.selected)
    } else if menu.prompt.is_some() {
        Some(base + 1)
    } else {
        Some(base)
    }
}

fn menu_selected_visual_row_base(menu: &MenuState) -> Option<usize> {
    (menu.page != MenuPage::Help && menu_item_count(menu) > 0).then(|| match menu.page {
        MenuPage::Profiles if menu.selected < 10 => menu.selected + 1,
        MenuPage::Profiles if menu.selected < 18 => menu.selected + 2,
        MenuPage::Profiles => menu.selected + 4,
        _ => menu.selected,
    })
}

fn inline_menu_prompt_line(prompt: &MenuPrompt, cursor_visible: bool) -> Line<'static> {
    let value = prompt.value.iter().collect::<String>();
    let before = prompt.value[..prompt.cursor.min(prompt.value.len())]
        .iter()
        .collect::<String>();
    let prefix = format!("      {}: ", prompt.title);
    let cursor = UnicodeWidthStr::width(prefix.as_str()) + UnicodeWidthStr::width(before.as_str());
    line_with_software_cursor(
        format!("{prefix}{value} "),
        cursor.min(u16::MAX as usize) as u16,
        cursor_visible,
    )
}

fn menu_field_help(_app: &App, menu: &MenuState) -> String {
    match menu.page {
        MenuPage::Profiles => match CurrentProfileRow::from_index(menu.selected) {
            Some(CurrentProfileRow::Port) => tr("menu.help.field.port"),
            Some(CurrentProfileRow::TransportProfile) => tr("menu.help.field.transport"),
            Some(CurrentProfileRow::ModelProfile) => tr("menu.help.field.model.profile"),
            Some(CurrentProfileRow::ModelName) => tr("menu.help.field.model.name"),
            Some(CurrentProfileRow::ShellPrompt) => tr("menu.help.field.shell"),
            Some(CurrentProfileRow::UbootPrompt) => tr("menu.help.field.uboot"),
            Some(CurrentProfileRow::ChunkSize | CurrentProfileRow::ChunkDelay) => {
                tr("menu.help.field.pacing")
            }
            Some(CurrentProfileRow::Apply) => tr("menu.help.field.apply"),
            _ => tr("menu.help.field.serial"),
        },
        MenuPage::CreateProfiles
        | MenuPage::CreateTransportProfile
        | MenuPage::CreateModelProfile => tr("menu.help.field.create"),
        MenuPage::DisplaySettings => tr("menu.help.field.display"),
        MenuPage::McpSettings => tr("menu.help.field.mcp"),
        MenuPage::ModelFamilies | MenuPage::ModelNames => tr("menu.help.field.model.name"),
        _ => tr("menu.help.field.navigation"),
    }
    .into()
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

fn is_prefix(key: KeyEvent) -> bool {
    key.code == KeyCode::Char(']') && key.modifiers.contains(KeyModifiers::CONTROL)
}

/// Newest-first case-sensitive substring search over the command history.
/// `before` bounds the search to entries older than that history index.
fn find_history_match(history: &[String], query: &str, before: Option<usize>) -> Option<usize> {
    if query.is_empty() {
        return None;
    }
    let end = before.unwrap_or(history.len()).min(history.len());
    history[..end]
        .iter()
        .rposition(|entry| entry.contains(query))
}

fn output_search_filter(
    query: &str,
    matcher: OutputSearchMatcher,
    case_sensitive: bool,
) -> (Option<String>, Option<String>) {
    match (matcher, case_sensitive) {
        (OutputSearchMatcher::Literal, true) => (Some(query.to_owned()), None),
        (OutputSearchMatcher::Literal, false) => {
            (None, Some(format!("(?i:{})", regex::escape(query))))
        }
        (OutputSearchMatcher::Regex, true) => (None, Some(query.to_owned())),
        (OutputSearchMatcher::Regex, false) => (None, Some(format!("(?i:{query})"))),
    }
}

fn raw_key_bytes(key: KeyEvent) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(character) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let ascii = character.to_ascii_uppercase();
            if ascii.is_ascii_uppercase() {
                Some(vec![(ascii as u8) - b'A' + 1])
            } else {
                match character {
                    '@' | ' ' => Some(vec![0x00]),
                    '[' => Some(vec![0x1b]),
                    '\\' => Some(vec![0x1c]),
                    ']' => Some(vec![0x1d]),
                    '^' => Some(vec![0x1e]),
                    '_' => Some(vec![0x1f]),
                    '?' => Some(vec![0x7f]),
                    _ => None,
                }
            }
        }
        KeyCode::Char(character) => {
            let mut bytes = Vec::new();
            if key.modifiers.contains(KeyModifiers::ALT) {
                bytes.push(0x1b);
            }
            let mut encoded = [0; 4];
            bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            Some(bytes)
        }
        KeyCode::Enter => Some(vec![b'\r']),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::BackTab => Some(b"\x1b[Z".to_vec()),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Esc => Some(vec![0x1b]),
        KeyCode::Up => Some(b"\x1b[A".to_vec()),
        KeyCode::Down => Some(b"\x1b[B".to_vec()),
        KeyCode::Right => Some(b"\x1b[C".to_vec()),
        KeyCode::Left => Some(b"\x1b[D".to_vec()),
        KeyCode::Home => Some(b"\x1b[H".to_vec()),
        KeyCode::End => Some(b"\x1b[F".to_vec()),
        KeyCode::Delete => Some(b"\x1b[3~".to_vec()),
        KeyCode::Insert => Some(b"\x1b[2~".to_vec()),
        KeyCode::PageUp => Some(b"\x1b[5~".to_vec()),
        KeyCode::PageDown => Some(b"\x1b[6~".to_vec()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashSet},
        sync::Mutex,
    };

    use crossterm::event::KeyEvent;
    use ratatui::backend::TestBackend;
    use serial_protocol::{ActorKind, Direction, SlotConfig, TriggerSpec};

    use super::*;

    static TEST_CLIPBOARD: Mutex<Vec<String>> = Mutex::new(Vec::new());

    fn record_clipboard_copy(text: &str) -> Result<()> {
        TEST_CLIPBOARD
            .lock()
            .expect("test clipboard lock")
            .push(text.to_owned());
        Ok(())
    }

    fn accept_clipboard_copy(_text: &str) -> Result<()> {
        Ok(())
    }

    #[test]
    fn raw_ctrl_c_is_etx_and_arrows_are_xterm() {
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(vec![0x03])
        );
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::CONTROL)),
            Some(vec![0x04])
        );
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Char('z'), KeyModifiers::CONTROL)),
            Some(vec![0x1a])
        );
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)),
            Some(b"\x1b[A".to_vec())
        );
    }

    #[test]
    fn line_ctrl_c_sends_etx_to_cancel_remote_continuation() {
        let mut app = ready_app_with_control();
        app.ports[0].snapshot.effective_write_eol = Some("\r\n".into());
        app.ports[0].history.push("previous command".into());
        app.ports[0].draft = "echo 'unterminated".chars().collect();
        app.ports[0].draft_cursor = app.ports[0].draft.len();
        app.ports[0].scroll_from_bottom = 5;
        app.ports[0].unseen = 2;
        let control_id = app.ports[0]
            .snapshot
            .control
            .as_ref()
            .expect("test control")
            .id;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_key(
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &commands,
        );

        let (_, data, operation_id) = take_write(&mut received);
        assert_eq!(data, vec![0x03]);
        assert_eq!(operation_id, None);
        assert!(app.current().draft.is_empty());
        assert_eq!(app.current().draft_cursor, 0);
        assert_eq!(app.current().scroll_from_bottom, 0);
        assert_eq!(app.current().unseen, 0);
        assert_eq!(app.current_mode(), InputMode::Line);
        assert!(!app.should_quit);
        assert_eq!(
            app.current()
                .snapshot
                .control
                .as_ref()
                .expect("control remains held")
                .id,
            control_id
        );
    }

    #[test]
    fn raw_mode_ctrl_c_is_forwarded_as_etx() {
        let mut app = ready_app_with_control();
        app.ports[0].mode = InputMode::Raw;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_key(
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &commands,
        );

        let (_, data, operation_id) = take_write(&mut received);
        assert_eq!(data, vec![0x03]);
        assert_eq!(operation_id, None);
        assert_eq!(app.current_mode(), InputMode::Raw);
        assert!(!app.should_quit);
    }

    #[test]
    fn replay_is_displayed_without_overwriting_the_authoritative_snapshot() {
        let mut snapshot = snapshot();
        snapshot.target_activity = TargetActivity::Silent;
        snapshot.last_rx_wall_time_ns = Some(1);
        let mut app = App::new(vec![snapshot], None);
        let (commands, _) = mpsc::channel(4);

        let mut replay = event(EventKind::Rx, Direction::Rx, 1, b"boot\r\n");
        replay.daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        app.push_event(replay, true, &commands);

        assert_eq!(
            app.ports[0].snapshot.target_activity,
            TargetActivity::Silent
        );
        assert_eq!(app.ports[0].snapshot.last_rx_wall_time_ns, Some(1));
        assert!(!app.ports[0].lines.is_empty());
    }

    #[test]
    fn startup_history_is_seeded_before_ring_replay_and_deduplicates_the_boundary() {
        let mut snapshot = snapshot();
        snapshot.head_seq = 3;
        let epoch = snapshot.daemon_epoch;
        let mut old = event(EventKind::Rx, Direction::Rx, 1, b"old\r\n");
        old.daemon_epoch = epoch;
        let mut persisted_tail = event(EventKind::Rx, Direction::Rx, 2, b"persisted\r\n");
        persisted_tail.daemon_epoch = epoch;
        let history = StartupHistory {
            port: "COM3".into(),
            epoch,
            head_seq: 3,
            events: vec![old, persisted_tail.clone()],
            gaps: Vec::new(),
            resume_cursor: Some(Cursor {
                epoch,
                after_seq: 2,
            }),
            limited: false,
            error: None,
        };
        let mut app = App::new(vec![snapshot], None);

        let (_, cursor) = app
            .apply_startup_history(history)
            .expect("verified startup cursor");
        assert_eq!(cursor.after_seq, 2);
        assert_eq!(app.ports[0].last_seq, 2);
        assert_eq!(app.ports[0].lines.len(), 2);

        let (commands, _) = mpsc::channel(4);
        app.push_event(persisted_tail, true, &commands);
        assert_eq!(app.ports[0].lines.len(), 2);
        let mut non_durable_live_tail = event(EventKind::Rx, Direction::Rx, 3, b"live\r\n");
        non_durable_live_tail.daemon_epoch = epoch;
        non_durable_live_tail.durable = false;
        app.push_event(non_durable_live_tail, true, &commands);

        assert_eq!(app.ports[0].last_seq, 3);
        assert_eq!(app.ports[0].lines.len(), 3);
    }

    #[test]
    fn provisional_live_event_does_not_claim_logging_is_degraded() {
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        let mut provisional = event(EventKind::Rx, Direction::Rx, 1, b"live");
        provisional.daemon_epoch = daemon_epoch;
        provisional.durable = false;
        app.push_event(provisional, false, &commands);
        assert_eq!(app.ports[0].snapshot.logging, LoggingState::Healthy);

        let mut degraded = event(EventKind::LoggingDegraded, Direction::None, 2, &[]);
        degraded.daemon_epoch = daemon_epoch;
        degraded.durable = false;
        app.push_event(degraded, false, &commands);
        assert_eq!(app.ports[0].snapshot.logging, LoggingState::Degraded);
    }

    #[test]
    fn serial_close_discards_queued_control_and_input() {
        let mut app = App::new(vec![snapshot()], None);
        let port = app.selected_port();
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        app.ports[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry(port.clone())
            .or_default()
            .push_back(PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                port: port.clone(),
                mode: ControlMode::Queue,
            },
        );
        let (commands, _) = mpsc::channel(4);

        let mut closed = event(EventKind::SerialClosed, Direction::None, 1, &[]);
        closed.daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        app.push_event(closed, false, &commands);

        assert!(!app.pending_writes.contains_key(&port));
        assert!(app.pending_requests.is_empty());
        assert!(app.ports[0].snapshot.active_trigger.is_none());
    }

    #[test]
    fn trigger_lifecycle_projects_live_state_and_confirmed_fires() {
        // The product defaults to Chinese, while this assertion deliberately
        // verifies the stable English rendering. Serialize access to the
        // process-global locale so parallel localization tests cannot race it.
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Armed);
        let trigger_id = trigger.id;
        let (commands, _) = mpsc::channel(4);

        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started.actor = Some(trigger.owner.clone());
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        app.push_event(started, false, &commands);

        let projected = app.ports[0]
            .snapshot
            .active_trigger
            .as_ref()
            .expect("TriggerStarted projects the authoritative Trigger");
        assert_eq!(projected.id, trigger_id);
        assert_eq!(projected.status, TriggerStatus::Armed);
        assert_eq!(projected.fires_confirmed, 0);

        let mut fire = event(EventKind::Tx, Direction::Tx, 2, b"slp");
        fire.daemon_epoch = daemon_epoch;
        fire.actor = Some(trigger.owner.clone());
        fire.metadata
            .insert("trigger_id".into(), serde_json::json!(trigger_id));
        fire.metadata
            .insert("trigger_write_kind".into(), serde_json::json!("action"));
        fire.metadata
            .insert("fire_index".into(), serde_json::json!(1));
        fire.metadata
            .insert("partial".into(), serde_json::json!(false));
        app.push_event(fire, false, &commands);

        let projected = app.ports[0]
            .snapshot
            .active_trigger
            .as_ref()
            .expect("confirmed Trigger TX keeps the live projection");
        assert_eq!(projected.status, TriggerStatus::Running);
        assert_eq!(projected.fires_confirmed, 1);
        assert_eq!(projected.tx_bytes_confirmed, 3);
        assert_eq!(projected.last_write_seq, Some(2));

        let mut completed = event(EventKind::TriggerCompleted, Direction::None, 3, &[]);
        completed.daemon_epoch = daemon_epoch;
        completed
            .metadata
            .insert("status".into(), serde_json::json!("matched"));
        app.push_event(completed, false, &commands);
        assert!(app.ports[0].snapshot.active_trigger.is_none());
        assert!(
            app.ports[0]
                .lines
                .iter()
                .any(|line| line.text == "trigger_completed: matched")
        );
    }

    #[test]
    fn trigger_projection_matches_start_and_stop_literals_across_rx_events() {
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::WaitingForStart);
        trigger.spec.start_contains = Some(b"boot>".to_vec());
        trigger.spec.stop_contains = vec![b"SigmaStar #".to_vec()];
        let (commands, _) = mpsc::channel(4);

        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        app.push_event(started, false, &commands);

        for (seq, bytes) in [(2, b"bo".as_slice()), (3, b"ot>".as_slice())] {
            let mut rx = event(EventKind::Rx, Direction::Rx, seq, bytes);
            rx.daemon_epoch = daemon_epoch;
            app.push_event(rx, false, &commands);
        }
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Running
        );

        for (seq, bytes) in [(4, b"Sigma".as_slice()), (5, b"Star #".as_slice())] {
            let mut rx = event(EventKind::Rx, Direction::Rx, seq, bytes);
            rx.daemon_epoch = daemon_epoch;
            app.push_event(rx, false, &commands);
        }
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );

        // A confirmed write that was already in flight is audited before the
        // terminal lifecycle event; it must not regress the local projection
        // from Stopping back to Running.
        let mut in_flight = event(EventKind::Tx, Direction::Tx, 6, b"slp");
        in_flight.daemon_epoch = daemon_epoch;
        in_flight
            .metadata
            .insert("trigger_id".into(), serde_json::json!(trigger.id));
        in_flight
            .metadata
            .insert("trigger_write_kind".into(), serde_json::json!("action"));
        in_flight
            .metadata
            .insert("fire_index".into(), serde_json::json!(1));
        app.push_event(in_flight, false, &commands);
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );
    }

    #[test]
    fn max_fire_budget_keeps_observing_when_stop_literal_exists() {
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        trigger.spec.max_fires = 1;
        trigger.spec.timeout_ms = 1;
        let trigger_id = trigger.id;
        let (commands, _) = mpsc::channel(4);

        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        app.push_event(started, false, &commands);

        let mut fire = event(EventKind::Tx, Direction::Tx, 2, b"slp");
        fire.daemon_epoch = daemon_epoch;
        fire.metadata
            .insert("trigger_id".into(), serde_json::json!(trigger_id));
        fire.metadata
            .insert("trigger_write_kind".into(), serde_json::json!("action"));
        fire.metadata
            .insert("fire_index".into(), serde_json::json!(1));
        app.push_event(fire, false, &commands);
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Running
        );

        for (seq, bytes) in [(3, b"rea".as_slice()), (4, b"dy".as_slice())] {
            let mut rx = event(EventKind::Rx, Direction::Rx, seq, bytes);
            rx.daemon_epoch = daemon_epoch;
            app.push_event(rx, false, &commands);
        }
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );

        let mut no_match_app = App::new(vec![snapshot()], None);
        let daemon_epoch = no_match_app.ports[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&no_match_app.ports[0].snapshot, TriggerStatus::Running);
        trigger.spec.max_fires = 1;
        trigger.spec.stop_contains.clear();
        let trigger_id = trigger.id;
        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        no_match_app.push_event(started, false, &commands);
        let mut fire = event(EventKind::Tx, Direction::Tx, 2, b"slp");
        fire.daemon_epoch = daemon_epoch;
        fire.metadata
            .insert("trigger_id".into(), serde_json::json!(trigger_id));
        fire.metadata
            .insert("trigger_write_kind".into(), serde_json::json!("action"));
        fire.metadata
            .insert("fire_index".into(), serde_json::json!(1));
        no_match_app.push_event(fire, false, &commands);
        assert_eq!(
            no_match_app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );

        let mut timeout_app = App::new(vec![snapshot()], None);
        let daemon_epoch = timeout_app.ports[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&timeout_app.ports[0].snapshot, TriggerStatus::Running);
        trigger.spec.timeout_ms = 1;
        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        timeout_app.push_event(started, false, &commands);

        assert!(
            timeout_app.ports[0].update_trigger_deadline(Instant::now() + Duration::from_millis(2))
        );
        assert_eq!(
            timeout_app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );
    }

    #[test]
    fn reconnect_ambiguity_is_labeled_active_instead_of_inventing_a_state() {
        let _guard = crate::i18n::lang_test_lock();
        let mut snapshot = snapshot();
        let mut trigger = trigger_info(&snapshot, TriggerStatus::Armed);
        trigger.spec.initial_write = Some(b"reboot\r".to_vec());
        trigger.spec.start_contains = Some(b"boot>".to_vec());
        let trigger_id = trigger.id;
        snapshot.active_trigger = Some(trigger);
        let mut app = App::new(vec![snapshot], None);
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        let mut initial = event(EventKind::Tx, Direction::Tx, 1, b"reboot\r");
        initial.daemon_epoch = daemon_epoch;
        initial
            .metadata
            .insert("trigger_id".into(), serde_json::json!(trigger_id));
        initial
            .metadata
            .insert("trigger_write_kind".into(), serde_json::json!("initial"));
        app.push_event(initial, false, &commands);

        assert_eq!(
            app.ports[0].trigger_status_text(),
            Some(tr("trigger.status.active"))
        );
        assert!(app.ports[0].snapshot.active_trigger.is_some());
    }

    #[test]
    fn every_terminal_trigger_lifecycle_clears_live_state() {
        for (offset, kind) in [
            EventKind::TriggerCompleted,
            EventKind::TriggerCancelled,
            EventKind::TriggerFailed,
        ]
        .into_iter()
        .enumerate()
        {
            let mut app = App::new(vec![snapshot()], None);
            let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
            let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Stopping);
            app.ports[0].snapshot.active_trigger = Some(trigger);
            let (commands, _) = mpsc::channel(4);
            let mut terminal = event(kind, Direction::None, offset as u64 + 1, &[]);
            terminal.daemon_epoch = daemon_epoch;

            app.push_event(terminal, false, &commands);

            assert!(
                app.ports[0].snapshot.active_trigger.is_none(),
                "{kind:?} left a stale active Trigger"
            );
        }
    }

    #[test]
    fn human_write_waits_for_trigger_terminal_after_takeover() {
        let mut app = ready_app_with_control();
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Stopping);
        app.ports[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry("COM3".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, mut received) = mpsc::channel(4);

        assert!(app.flush_pending_writes("COM3", &commands));
        assert!(received.try_recv().is_err());
        assert_eq!(app.pending_writes["COM3"].len(), 1);

        let mut cancelled = event(EventKind::TriggerCancelled, Direction::None, 1, &[]);
        cancelled.daemon_epoch = daemon_epoch;
        app.push_event(cancelled, false, &commands);

        assert!(app.ports[0].snapshot.active_trigger.is_none());
        let (_, data, _) = take_write(&mut received);
        assert_eq!(data, b"version\r");
    }

    #[test]
    fn disconnect_keeps_sent_unacknowledged_write_warning_visible() {
        let mut app = App::new(vec![snapshot()], None);
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Write {
                port: "COM3".into(),
                operation_id: Some(Uuid::new_v4()),
                cooperative: false,
            },
        );
        let (commands, _) = mpsc::channel(4);

        app.handle_network(
            NetworkEvent::Disconnected {
                reason: "test disconnect".into(),
            },
            &commands,
        );
        app.handle_network(
            NetworkEvent::TransportConnected { generation: 2 },
            &commands,
        );

        assert_eq!(app.uncertain_write_outcomes, 1);
        assert!(app.pending_requests.is_empty());
    }

    #[test]
    fn input_is_rejected_until_the_selected_slot_is_ready() {
        let mut app = App::new(vec![snapshot()], None);
        app.transport_connected = true;
        app.hello_accepted = true;
        app.connection_generation = Some(1);
        let (commands, mut received) = mpsc::channel(4);

        app.request_write(&commands, b"help\r".to_vec(), None);

        assert!(received.try_recv().is_err());
        assert!(app.pending_writes.is_empty());
    }

    #[test]
    fn queued_line_operations_remain_one_editable_entry_across_chunks() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut queue = VecDeque::new();
        append_pending_write(
            &mut queue,
            &vec![b'a'; MAX_WRITE_BYTES + 7],
            Some(first),
            PendingWriteKind::Line,
        );
        append_pending_write(
            &mut queue,
            b"second\r",
            Some(second),
            PendingWriteKind::Line,
        );

        assert_eq!(queue.len(), 3);
        assert_eq!(queued_line_count(&queue), 2);
        assert_eq!(pop_last_queued_line(&mut queue), Some(b"second\r".to_vec()));
        assert_eq!(queue.len(), 2);
        assert_eq!(queued_line_count(&queue), 1);
        assert_eq!(
            pop_last_queued_line(&mut queue),
            Some(vec![b'a'; MAX_WRITE_BYTES + 7])
        );
        assert!(queue.is_empty());
    }

    #[test]
    fn queued_line_can_be_returned_to_the_editor_before_control_is_granted() {
        let mut app = ready_app_with_foreign_control();
        let (commands, _received) = mpsc::channel(4);
        assert!(app.request_write(&commands, b"echo queued\r".to_vec(), Some(Uuid::new_v4())));

        app.remove_last_queued_line(true, &commands);

        assert!(!app.pending_writes.contains_key("COM3"));
        assert_eq!(app.ports[0].draft.iter().collect::<String>(), "echo queued");
        assert_eq!(app.ports[0].draft_cursor, "echo queued".chars().count());
        assert_eq!(app.ports[0].mode, InputMode::Line);
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::CancelAcquire { port } if port == "COM3")
        ));
    }

    #[test]
    fn raw_queue_is_not_lossily_converted_into_a_line_draft() {
        let mut app = ready_app_with_foreign_control();
        let (commands, _received) = mpsc::channel(4);
        assert!(app.request_raw_write(&commands, vec![0x03]));

        app.remove_last_queued_line(true, &commands);

        assert_eq!(app.pending_writes["COM3"][0].data, vec![0x03]);
        assert!(app.ports[0].draft.is_empty());
        assert_eq!(app.status, tr("st.queue.raw.only"));
    }

    #[test]
    fn one_hundred_queued_raw_characters_coalesce_into_one_unsent_block() {
        let mut app = ready_app_with_foreign_control();
        let (commands, mut received) = mpsc::channel(4);
        let expected = (0..100)
            .map(|index| b'a' + (index % 26) as u8)
            .collect::<Vec<_>>();

        for &byte in &expected {
            assert!(app.request_raw_write(&commands, vec![byte]));
        }

        let queued = app.pending_writes.get("COM3").expect("queued RAW data");
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].data, expected);
        assert_eq!(queued[0].kind, PendingWriteKind::Raw);
        let NetworkCommand::Send { message, .. } =
            received.try_recv().expect("one queued control request")
        else {
            panic!("expected control request")
        };
        assert!(matches!(message, ClientMessage::AcquireControl { .. }));
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn raw_queue_capacity_rejects_the_whole_new_input_without_partial_append() {
        let mut app = ready_app_with_foreign_control();
        let (commands, _received) = mpsc::channel(4);

        assert!(
            app.request_raw_write(&commands, vec![b'x'; MAX_PENDING_BYTES]),
            "queue rejected its documented exact capacity: {}",
            app.status
        );
        let before = app
            .pending_writes
            .get("COM3")
            .expect("full bounded RAW queue")
            .iter()
            .map(|write| write.data.clone())
            .collect::<Vec<_>>();
        assert_eq!(before.len(), MAX_PENDING_WRITES);

        assert!(!app.request_raw_write(&commands, vec![b'y']));
        let after = app
            .pending_writes
            .get("COM3")
            .expect("previous accepted RAW queue remains authoritative")
            .iter()
            .map(|write| write.data.clone())
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        assert_eq!(app.status, tr("st.writeq.full"));
    }

    #[test]
    fn large_write_is_split_and_sent_one_chunk_at_a_time() {
        let mut app = ready_app_with_control();
        let operation_id = Uuid::new_v4();
        let (commands, mut received) = mpsc::channel(8);

        app.request_write(
            &commands,
            vec![0x5a; MAX_WRITE_BYTES * 2 + 17],
            Some(operation_id),
        );

        let (first_id, first_data, first_operation) = take_write(&mut received);
        assert_eq!(first_data.len(), MAX_WRITE_BYTES);
        assert_eq!(first_operation, Some(operation_id));
        assert_eq!(app.pending_writes["COM3"].len(), 3);
        assert_eq!(
            queued_line_operations(&app.pending_writes["COM3"])[0]
                .data
                .len(),
            MAX_WRITE_BYTES * 2 + 17
        );
        assert!(received.try_recv().is_err());

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (second_id, second_data, second_operation) = take_write(&mut received);
        assert_ne!(first_id, second_id);
        assert_eq!(second_data.len(), MAX_WRITE_BYTES);
        assert_eq!(second_operation, Some(operation_id));
        assert_eq!(app.pending_writes["COM3"].len(), 3);
        assert_eq!(
            queued_line_operations(&app.pending_writes["COM3"])[0]
                .data
                .len(),
            MAX_WRITE_BYTES * 2 + 17
        );

        app.handle_result(
            second_id,
            CommandResult::WriteAccepted { event_seq: 2 },
            &commands,
        );
        let (third_id, third_data, third_operation) = take_write(&mut received);
        assert_ne!(second_id, third_id);
        assert_eq!(third_data.len(), 17);
        assert_eq!(third_operation, Some(operation_id));
        assert_eq!(app.pending_writes["COM3"].len(), 3);
        app.handle_result(
            third_id,
            CommandResult::WriteAccepted { event_seq: 3 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("COM3"));
    }

    #[test]
    fn rejected_write_discards_remaining_chunks() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);

        app.request_write(
            &commands,
            vec![0x5a; MAX_WRITE_BYTES + 1],
            Some(Uuid::new_v4()),
        );
        let (request_id, first_data, _) = take_write(&mut received);
        assert_eq!(first_data.len(), MAX_WRITE_BYTES);
        assert_eq!(app.pending_writes["COM3"].len(), 2);

        app.handle_server_message(
            ServerMessage::Error {
                request_id: Some(request_id),
                code: serial_protocol::ErrorCode::PortOffline,
                message: "port went offline".into(),
                retryable: true,
            },
            &commands,
        );

        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn confirmed_line_paste_is_one_ordered_chunked_write() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            port: "COM3".into(),
            bytes: vec![b'x'; MAX_WRITE_BYTES + 1],
            raw: false,
        });

        app.confirm_paste(&commands);

        let (first_id, first_data, operation_id) = take_write(&mut received);
        assert_eq!(first_data, vec![b'x'; MAX_WRITE_BYTES]);
        let operation_id = operation_id.expect("line paste operation ID");
        assert_eq!(app.pending_writes["COM3"].len(), 2);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (second_id, second_data, second_operation) = take_write(&mut received);
        assert_ne!(first_id, second_id);
        assert_eq!(second_data, b"x\r");
        assert_eq!(second_operation, Some(operation_id));
        assert_eq!(app.pending_writes["COM3"].len(), 2);
        app.handle_result(
            second_id,
            CommandResult::WriteAccepted { event_seq: 2 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("COM3"));
    }

    #[test]
    fn confirmed_multiline_paste_assigns_each_command_a_distinct_operation() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            port: "COM3".into(),
            bytes: b"pwd\nversion\n".to_vec(),
            raw: false,
        });

        app.confirm_paste(&commands);

        let (first_id, first_data, first_operation) = take_write(&mut received);
        let first_operation = first_operation.expect("first line paste operation ID");
        assert_eq!(first_data, b"pwd\r");
        assert_eq!(app.pending_writes["COM3"].len(), 2);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (_, second_data, second_operation) = take_write(&mut received);
        assert_eq!(second_data, b"version\r");
        assert!(second_operation.is_some());
        assert_ne!(second_operation, Some(first_operation));
        assert_eq!(app.pending_writes["COM3"].len(), 1);
    }

    #[test]
    fn foreign_control_multiline_paste_creates_oldest_first_independent_cards() {
        let mut app = ready_app_with_foreign_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            port: "COM3".into(),
            bytes: b"first\nsecond\nthird\n".to_vec(),
            raw: false,
        });

        app.confirm_paste(&commands);

        let NetworkCommand::Send { message, .. } = received.try_recv().expect("queue-mode acquire")
        else {
            panic!("expected queue-mode acquire")
        };
        assert!(matches!(
            message,
            ClientMessage::AcquireControl {
                mode: ControlMode::Queue,
                ..
            }
        ));
        assert!(received.try_recv().is_err());

        let operations = queued_line_operations(&app.pending_writes["COM3"]);
        assert_eq!(operations.len(), 3);
        assert_eq!(operations[0].data, b"first\r");
        assert_eq!(operations[1].data, b"second\r");
        assert_eq!(operations[2].data, b"third\r");
        let ids = operations
            .iter()
            .map(|operation| operation.operation_id.expect("LINE operation ID"))
            .collect::<HashSet<_>>();
        assert_eq!(ids.len(), 3);

        app.remove_queued_line_operation(1, true, &commands);
        assert_eq!(app.current().draft.iter().collect::<String>(), "second");
        let remaining = queued_line_operations(&app.pending_writes["COM3"])
            .into_iter()
            .map(|operation| operation.data)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![b"first\r".to_vec(), b"third\r".to_vec()]);
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Acquire { port, .. } if port == "COM3")
        ));
    }

    #[test]
    fn confirmed_raw_paste_preserves_one_unmodified_burst() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            port: "COM3".into(),
            bytes: b"pwd\nversion\n".to_vec(),
            raw: true,
        });

        app.confirm_paste(&commands);

        let (_, data, operation_id) = take_write(&mut received);
        assert_eq!(data, b"pwd\nversion\n");
        assert_eq!(operation_id, None);
        assert!(app.pending_writes.contains_key("COM3"));
    }

    #[test]
    fn subscription_phase_tracks_attach_replay_ready_and_lag() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        app.handle_network(
            NetworkEvent::TransportConnected { generation: 1 },
            &commands,
        );
        assert!(matches!(
            app.ports[0].subscription,
            SubscriptionPhase::Attaching
        ));

        app.handle_server_message(
            ServerMessage::ReplayBegin {
                port: "COM3".into(),
                from_seq: 4,
                through_seq: 9,
            },
            &commands,
        );
        assert!(matches!(
            app.ports[0].subscription,
            SubscriptionPhase::Replaying {
                from_seq: 4,
                through_seq: 9
            }
        ));

        app.handle_server_message(
            ServerMessage::Ready {
                port: "COM3".into(),
                head_seq: 9,
            },
            &commands,
        );
        assert!(app.slot_ready(0));

        app.handle_server_message(
            ServerMessage::Lagged {
                port: "COM3".into(),
                from_seq: 10,
                to_seq: 20,
            },
            &commands,
        );
        assert!(matches!(
            app.ports[0].subscription,
            SubscriptionPhase::Lagged {
                from_seq: 10,
                to_seq: 20
            }
        ));
    }

    #[test]
    fn active_activity_is_derived_as_silent_without_mutating_snapshot() {
        let mut snapshot = snapshot();
        snapshot.target_activity = TargetActivity::Active;
        snapshot.last_rx_wall_time_ns = Some(10);

        assert_eq!(
            displayed_target_activity_at(&snapshot, 10 + ACTIVE_WINDOW_NS),
            TargetActivity::Silent
        );
        assert_eq!(snapshot.target_activity, TargetActivity::Active);
    }

    #[test]
    fn output_title_uses_exact_bound_model_name_without_slot_or_baud() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut current = snapshot();
        current.config.model_profile = Some("TL-AS7230 1.0".into());
        let mut app = App::new(vec![current], None);

        let title = output_title(&app);
        assert!(title.contains("TL-AS7230 1.0"));
        assert!(!title.contains(&app.current().snapshot.config.port));
        assert!(!title.contains("115200"));

        app.ports[0].snapshot.config.model_profile = None;
        let fallback = output_title(&app);
        assert!(fallback.contains(tr("ui.output.model.unconfigured")));
        assert!(!fallback.contains(&app.current().snapshot.config.port));
    }

    #[test]
    fn top_status_uses_only_port_name_and_session_state() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::En);
        let mut app = App::new(vec![snapshot()], None);
        app.ports[0].subscription = SubscriptionPhase::Ready { head_seq: 42 };
        app.ports[0].unseen = 99;
        let backend = TestBackend::new(80, 3);
        let mut terminal = Terminal::new(backend).expect("tab test terminal");

        terminal
            .draw(|frame| {
                let area = frame.area();
                draw_tabs(frame, &app, area);
            })
            .unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("COM3 · ONLINE"));
        assert!(!rendered.contains("Port 1"));
        assert!(!rendered.contains("LIVE#"));
        assert!(!rendered.contains("+99"));
    }

    #[test]
    fn live_profile_refresh_updates_effective_behavior_without_changing_config() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        let config = app.ports[0].snapshot.config.clone();
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        let trigger_id = trigger.id;
        app.ports[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry("COM3".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"queued".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let mut reconfigured = event(EventKind::PortReconfigured, Direction::None, 1, &[]);
        reconfigured.daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        reconfigured
            .metadata
            .insert("current".into(), serde_json::to_value(&config).unwrap());
        reconfigured.metadata.insert(
            "effective".into(),
            serde_json::to_value(ResolvedModelSettings {
                shell_prompt: Some("]# ".into()),
                uboot_prompt: Some("Luckfox #".into()),
                write_eol: "\n".into(),
                echo: EchoMode::Off,
                write_pacing: WritePacing {
                    chunk_size: 1,
                    chunk_delay_ms: 1,
                },
            })
            .unwrap(),
        );
        reconfigured
            .metadata
            .insert("profile_only".into(), serde_json::Value::Bool(true));

        app.push_event(reconfigured, false, &commands);

        assert_eq!(app.ports[0].snapshot.config, config);
        assert_eq!(
            app.ports[0].snapshot.effective_shell_prompt.as_deref(),
            Some("]# ")
        );
        assert_eq!(
            app.ports[0].snapshot.effective_uboot_prompt.as_deref(),
            Some("Luckfox #")
        );
        assert_eq!(
            app.ports[0].snapshot.effective_write_eol.as_deref(),
            Some("\n")
        );
        assert_eq!(app.ports[0].snapshot.effective_echo, Some(EchoMode::Off));
        assert_eq!(
            app.ports[0].snapshot.effective_write_pacing,
            Some(WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 1,
            })
        );
        assert_eq!(
            app.ports[0]
                .snapshot
                .active_trigger
                .as_ref()
                .map(|trigger| trigger.id),
            Some(trigger_id)
        );
        assert!(app.pending_writes.contains_key("COM3"));
    }

    #[test]
    fn physical_reconfigure_updates_config_even_if_metadata_claims_profile_only() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        app.ports[0].snapshot.active_trigger = Some(trigger);
        let mut config = app.ports[0].snapshot.config.clone();
        config.transport_profile = Some("uart-57600".into());
        app.pending_writes
            .entry("COM3".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"queued".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let mut reconfigured = event(EventKind::PortReconfigured, Direction::None, 1, &[]);
        reconfigured.daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        reconfigured
            .metadata
            .insert("current".into(), serde_json::to_value(&config).unwrap());
        reconfigured
            .metadata
            .insert("profile_only".into(), serde_json::Value::Bool(true));

        app.push_event(reconfigured, false, &commands);

        assert_eq!(app.ports[0].snapshot.config, config);
        assert!(app.ports[0].snapshot.active_trigger.is_none());
        assert!(!app.pending_writes.contains_key("COM3"));
    }

    #[test]
    fn removed_slot_projects_an_authoritative_disabled_state() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_control();
        let owner = app.actor.clone().unwrap();
        app.ports[0].snapshot.active_run = Some(RunInfo {
            id: Uuid::new_v4(),
            owner,
            label: "active run".into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        });
        let trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        app.ports[0].snapshot.active_trigger = Some(trigger);
        let (commands, _) = mpsc::channel(4);
        let mut removed = event(EventKind::PortRemoved, Direction::None, 2, &[]);
        removed.daemon_epoch = app.ports[0].snapshot.daemon_epoch;

        app.push_event(removed, false, &commands);

        let snapshot = &app.ports[0].snapshot;
        assert_eq!(snapshot.session_state, SessionState::Disabled);
        assert_eq!(snapshot.state_reason.as_deref(), Some(tr("state.removed")));
        assert_eq!(snapshot.target_activity, TargetActivity::Unknown);
        assert!(!snapshot.endpoint_present);
        assert!(snapshot.control.is_none());
        assert!(snapshot.active_run.is_none());
        assert!(snapshot.active_trigger.is_none());
    }

    #[test]
    fn queued_control_cancel_is_directed_and_preserves_other_slots() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_control();
        let port = app.selected_port();
        app.ports[0].snapshot.control = Some(ControlLease {
            owner: Actor {
                id: "agent:other".into(),
                label: "other-agent".into(),
                kind: ActorKind::Agent,
            },
            ..app.ports[0].snapshot.control.clone().expect("test lease")
        });
        app.pending_writes
            .entry(port.clone())
            .or_default()
            .push_back(PendingWrite {
                data: b"reboot\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        app.queued_controls.insert(
            port.clone(),
            QueuedControl {
                _position: 1,
                since: Instant::now(),
            },
        );
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                port: port.clone(),
                mode: ControlMode::Queue,
            },
        );
        let mut other = snapshot();
        other.config.port = "COM4".into();
        other.config.port = "Port 2".into();
        other.config.port = "COM4".into();
        let mut other = SlotView::new(other);
        other.subscription = SubscriptionPhase::Ready { head_seq: 0 };
        app.ports.push(other);
        app.pending_writes.insert(
            "COM4".into(),
            VecDeque::from([PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: Some(Uuid::new_v4()),
                kind: PendingWriteKind::Line,
            }]),
        );
        app.queued_controls.insert(
            "COM4".into(),
            QueuedControl {
                _position: 2,
                since: Instant::now(),
            },
        );
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                port: "COM4".into(),
                mode: ControlMode::Queue,
            },
        );
        let (commands, mut received) = mpsc::channel(4);

        app.release_control(&commands);

        let NetworkCommand::Send { message, .. } = received.try_recv().expect("directed cancel")
        else {
            panic!("expected directed cancel request")
        };
        assert!(matches!(
            message,
            ClientMessage::CancelAcquire { port, .. } if port == "COM3"
        ));
        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(app.pending_writes.contains_key("COM4"));
        assert!(!app.queued_controls.contains_key("COM3"));
        assert!(app.queued_controls.contains_key("COM4"));
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Acquire { port, .. } if port == "COM4")
        ));
    }

    #[test]
    fn idle_human_control_is_released_instead_of_renewed_forever() {
        let mut app = ready_app_with_control();
        app.ports[0].last_manual_activity =
            Some(Instant::now() - app.human_idle_release - Duration::from_secs(1));
        let (commands, mut received) = mpsc::channel(4);

        app.maintain_controls(&commands);

        let NetworkCommand::Send { message, .. } = received.try_recv().expect("release request")
        else {
            panic!("expected a release request");
        };
        assert!(matches!(message, ClientMessage::ReleaseControl { .. }));
    }

    #[test]
    fn recent_human_activity_renews_control() {
        let mut app = ready_app_with_control();
        app.ports[0].last_manual_activity = Some(Instant::now());
        let (commands, mut received) = mpsc::channel(4);

        app.maintain_controls(&commands);

        let NetworkCommand::Send { message, .. } = received.try_recv().expect("renew request")
        else {
            panic!("expected a renew request");
        };
        assert!(matches!(message, ClientMessage::RenewControl { .. }));
    }

    #[test]
    fn history_search_finds_newest_match_and_cycles_to_older() {
        let mut app = App::new(vec![snapshot()], None);
        {
            let view = &mut app.ports[0];
            view.history = vec![
                "show version".into(),
                "reboot".into(),
                "show interfaces".into(),
            ];
            view.draft = "partial".chars().collect();
            view.draft_cursor = 7;
        }

        app.start_history_search();
        for character in "show".chars() {
            app.handle_history_search_key(KeyEvent::new(
                KeyCode::Char(character),
                KeyModifiers::NONE,
            ));
        }
        assert_eq!(
            app.ports[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Ctrl-R cycles to the older match, then wraps back to the newest.
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        assert_eq!(
            app.ports[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(0))
        );
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        assert_eq!(
            app.ports[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Backspace edits the query and re-searches from newest.
        for _ in 0..4 {
            app.handle_history_search_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        assert_eq!(
            app.ports[0].history_search.as_ref().map(|s| s.match_index),
            Some(None)
        );
        for character in "int".chars() {
            app.handle_history_search_key(KeyEvent::new(
                KeyCode::Char(character),
                KeyModifiers::NONE,
            ));
        }
        assert_eq!(
            app.ports[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Enter accepts the current match into the draft.
        app.handle_history_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.ports[0].history_search.is_none());
        assert_eq!(
            app.ports[0].draft.iter().collect::<String>(),
            "show interfaces"
        );
    }

    #[test]
    fn history_search_escape_restores_the_original_draft() {
        let mut app = App::new(vec![snapshot()], None);
        {
            let view = &mut app.ports[0];
            view.history = vec!["reboot".into()];
            view.draft = "keep me".chars().collect();
            view.draft_cursor = 7;
        }
        app.start_history_search();
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));
        assert!(
            app.ports[0]
                .history_search
                .as_ref()
                .is_some_and(|s| s.match_index == Some(0))
        );

        app.handle_history_search_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(app.ports[0].history_search.is_none());
        assert_eq!(app.ports[0].draft.iter().collect::<String>(), "keep me");
        assert_eq!(app.ports[0].draft_cursor, 7);
    }

    #[test]
    fn output_search_filters_escape_literals_and_bound_case_mode() {
        assert_eq!(
            output_search_filter("a.b[0]", OutputSearchMatcher::Literal, true),
            (Some("a.b[0]".into()), None)
        );
        assert_eq!(
            output_search_filter("a.b[0]", OutputSearchMatcher::Literal, false),
            (None, Some(r"(?i:a\.b\[0\])".into()))
        );
        assert_eq!(
            output_search_filter("error|panic", OutputSearchMatcher::Regex, false),
            (None, Some("(?i:error|panic)".into()))
        );
    }

    #[test]
    fn output_search_footer_puts_integrity_before_navigation_and_keeps_limits() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.open_output_search();
        let search = app.output_search.as_mut().expect("search");
        search.results = vec![event(EventKind::Rx, Direction::Rx, 1, b"match")];
        search.partial = true;
        search.scanned_archives = 4;
        search.gaps.push(GapRange {
            epoch: search.current_epoch,
            first_seq: 10,
            last_seq: 12,
            reason: serial_protocol::GapReason::Retention,
        });

        let footer = output_search_result_footer(search);
        assert_eq!(footer.integrity, "⚠ PARTIAL · 1 journal gap(s)");
        assert!(footer.navigation.starts_with("1/1 · 4 archives"));
        assert!(!footer.navigation.contains("PARTIAL"));
        assert!(
            footer
                .limits
                .as_deref()
                .is_some_and(|limits| limits.contains("10,000-sequence window/archive"))
        );
    }

    #[test]
    fn output_search_integrity_warning_is_visible_before_navigation_at_80_columns() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.open_output_search();
        let search = app.output_search.as_mut().expect("search");
        search.query = "boot failure".chars().collect();
        search.phase = OutputSearchPhase::Results;
        search.results = vec![event(EventKind::Rx, Direction::Rx, 1, b"boot failure")];
        search.partial = true;
        search.scanned_archives = 4;
        search.gaps.push(GapRange {
            epoch: search.current_epoch,
            first_seq: 10,
            last_seq: 12,
            reason: serial_protocol::GapReason::Retention,
        });

        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        let integrity = rendered
            .find("PARTIAL · 1 journal gap(s)")
            .expect("compact partial/gap warning remains visible at 80 columns");
        let navigation = rendered
            .find("1/1 · 4 archives")
            .expect("navigation is rendered on its own following line");
        assert!(integrity < navigation);
    }

    #[test]
    fn output_search_archive_count_only_advances_when_a_request_can_start() {
        let mut scanned = 3;
        assert!(!begin_output_search_archive(
            OUTPUT_SEARCH_HTTP_QUERY_LIMIT,
            &mut scanned
        ));
        assert_eq!(scanned, 3);

        assert!(begin_output_search_archive(
            OUTPUT_SEARCH_HTTP_QUERY_LIMIT - 1,
            &mut scanned
        ));
        assert_eq!(scanned, 4);
    }

    #[test]
    fn output_search_pagination_must_advance_within_the_same_epoch() {
        let epoch = Uuid::new_v4();
        assert_eq!(
            output_search_page_progress(
                true,
                Some(&Cursor {
                    epoch,
                    after_seq: 120,
                }),
                epoch,
                100,
                200,
            ),
            OutputSearchPageProgress::Continue(120)
        );
        assert_eq!(
            output_search_page_progress(
                true,
                Some(&Cursor {
                    epoch,
                    after_seq: 200,
                }),
                epoch,
                120,
                200,
            ),
            OutputSearchPageProgress::Complete
        );
        assert_eq!(
            output_search_page_progress(
                true,
                Some(&Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 150,
                }),
                epoch,
                120,
                200,
            ),
            OutputSearchPageProgress::Incomplete
        );
        assert_eq!(
            output_search_page_progress(true, None, epoch, 120, 200),
            OutputSearchPageProgress::Incomplete
        );
        // Snapshot head H=12 can lead durable journal head K=10. A clean page
        // at K is still incomplete and must not silently claim a full search.
        assert_eq!(
            output_search_page_progress(
                false,
                Some(&Cursor {
                    epoch,
                    after_seq: 10,
                }),
                epoch,
                0,
                12,
            ),
            OutputSearchPageProgress::Incomplete
        );
        // A disappeared/empty archive with no authoritative scan cursor is
        // likewise partial, not a complete zero-match result.
        assert_eq!(
            output_search_page_progress(false, None, epoch, 0, 12),
            OutputSearchPageProgress::Incomplete
        );
        // No matching events is complete when the scan cursor still proves
        // the requested head was examined.
        assert_eq!(
            output_search_page_progress(
                false,
                Some(&Cursor {
                    epoch,
                    after_seq: 12,
                }),
                epoch,
                0,
                12,
            ),
            OutputSearchPageProgress::Complete
        );
    }

    #[test]
    fn output_search_keeps_the_newest_matches_while_forward_pages_arrive() {
        let epoch = Uuid::new_v4();
        let epoch_ranks = HashMap::from([(epoch, 0)]);
        let mut events = (1..=250)
            .map(|seq| {
                let mut event = event(EventKind::Rx, Direction::Rx, seq, b"match");
                event.daemon_epoch = epoch;
                // A backwards wall-clock jump must never make a higher
                // sequence look older within one daemon epoch.
                event.wall_time_ns = 1_000 - seq as i64;
                event
            })
            .collect::<Vec<_>>();
        assert!(retain_newest_output_search_events(
            &mut events,
            &epoch_ranks
        ));
        assert_eq!(events.len(), OUTPUT_SEARCH_LIMIT_EVENTS);
        assert_eq!(events.first().map(|event| event.seq), Some(250));
        assert_eq!(events.last().map(|event| event.seq), Some(51));

        events.extend((251..=275).map(|seq| {
            let mut event = event(EventKind::Rx, Direction::Rx, seq, b"later match");
            event.daemon_epoch = epoch;
            event.wall_time_ns = 1_000 - seq as i64;
            event
        }));
        assert!(retain_newest_output_search_events(
            &mut events,
            &epoch_ranks
        ));
        assert_eq!(events.first().map(|event| event.seq), Some(275));
        assert_eq!(events.last().map(|event| event.seq), Some(76));
    }

    #[test]
    fn output_search_cross_epoch_order_uses_archive_rank_not_wall_clock() {
        let newer_epoch = Uuid::new_v4();
        let older_epoch = Uuid::new_v4();
        let epoch_ranks = HashMap::from([(newer_epoch, 0), (older_epoch, 1)]);
        let mut events = Vec::new();
        events.extend((1..=150).map(|seq| {
            let mut event = event(EventKind::Rx, Direction::Rx, seq, b"new archive");
            event.daemon_epoch = newer_epoch;
            // Deliberately older-looking wall time.
            event.wall_time_ns = -10_000 - seq as i64;
            event
        }));
        events.extend((1_000..=1_149).map(|seq| {
            let mut event = event(EventKind::Rx, Direction::Rx, seq, b"old archive");
            event.daemon_epoch = older_epoch;
            event.wall_time_ns = 10_000 + seq as i64;
            event
        }));

        assert!(retain_newest_output_search_events(
            &mut events,
            &epoch_ranks
        ));
        assert_eq!(events.len(), OUTPUT_SEARCH_LIMIT_EVENTS);
        assert_eq!(
            events.first().map(|event| event.daemon_epoch),
            Some(newer_epoch)
        );
        assert_eq!(events.first().map(|event| event.seq), Some(150));
        assert_eq!(events.get(149).map(|event| event.seq), Some(1));
        assert_eq!(
            events.get(150).map(|event| event.daemon_epoch),
            Some(older_epoch)
        );
        assert_eq!(events.get(150).map(|event| event.seq), Some(1_149));
        assert_eq!(events.last().map(|event| event.seq), Some(1_100));
    }

    #[test]
    fn output_search_prefix_builds_run_scoped_query_and_plain_escape_cancels_loading() {
        let mut current = snapshot();
        current.head_seq = 80;
        let mut run = agent_run("inspect boot");
        run.start_seq = 20;
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        let (search_commands, mut received) = mpsc::channel(4);
        app.output_search_commands = Some(search_commands);
        let (network_commands, _network_rx) = mpsc::channel(1);

        app.handle_prefix_key(
            KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE),
            &network_commands,
        );
        for character in "Error".chars() {
            app.handle_output_search_key(KeyEvent::new(
                KeyCode::Char(character),
                KeyModifiers::NONE,
            ));
        }
        app.handle_output_search_key(KeyEvent::new(KeyCode::F(4), KeyModifiers::NONE));
        app.handle_output_search_key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        app.handle_output_search_key(KeyEvent::new(KeyCode::F(5), KeyModifiers::NONE));
        app.handle_output_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let OutputSearchIoCommand::Query(request) = received.try_recv().expect("journal query")
        else {
            panic!("expected a search query");
        };
        assert_eq!(request.scope, OutputSearchScope::CurrentRun);
        assert_eq!(request.direction, OutputSearchDirection::Rx);
        assert_eq!(request.current_run.map(|scope| scope.id), Some(run.id));
        assert_eq!(request.contains, None);
        assert_eq!(request.regex.as_deref(), Some("(?i:Error)"));
        assert!(matches!(
            app.output_search.as_ref().map(|search| search.phase),
            Some(OutputSearchPhase::Loading(id)) if id == request.request_id
        ));

        // The Esc arm must not inherit the Ctrl-C guard.
        app.handle_output_search_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(app.output_search.is_none());
        assert!(matches!(
            received.try_recv(),
            Ok(OutputSearchIoCommand::Cancel { request_id }) if request_id == request.request_id
        ));
    }

    #[test]
    fn output_search_submit_refreshes_the_current_epoch_head() {
        let mut current = snapshot();
        current.head_seq = 100;
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        app.open_output_search();
        let (commands, mut received) = mpsc::channel(2);
        app.output_search_commands = Some(commands);
        {
            let search = app.output_search.as_mut().expect("search");
            search.query = "needle".chars().collect();
            search.cursor = search.query.len();
        }
        app.ports[0].snapshot.head_seq = 101;

        app.handle_output_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let OutputSearchIoCommand::Query(request) = received.try_recv().expect("query") else {
            panic!("expected query");
        };
        assert_eq!(request.current_epoch, epoch);
        assert_eq!(request.head_seq, 101);
        let search = app.output_search.as_ref().expect("loading search");
        assert_eq!(search.head_seq, 101);
        assert!(output_search_target(search).contains("#101"));
    }

    #[test]
    fn output_search_submit_switches_to_the_new_authoritative_epoch() {
        let mut current = snapshot();
        current.head_seq = 100;
        let old_epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        app.open_output_search();
        let (commands, mut received) = mpsc::channel(2);
        app.output_search_commands = Some(commands);
        {
            let search = app.output_search.as_mut().expect("search");
            search.query = "boot".chars().collect();
            search.cursor = search.query.len();
        }
        let new_epoch = Uuid::new_v4();
        app.ports[0].snapshot.daemon_epoch = new_epoch;
        app.ports[0].snapshot.head_seq = 7;

        app.handle_output_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let OutputSearchIoCommand::Query(request) = received.try_recv().expect("query") else {
            panic!("expected query");
        };
        assert_ne!(request.current_epoch, old_epoch);
        assert_eq!(request.current_epoch, new_epoch);
        assert_eq!(request.head_seq, 7);
        let search = app.output_search.as_ref().expect("loading search");
        assert_eq!(search.current_epoch, new_epoch);
        assert!(output_search_target(search).contains(&new_epoch.to_string()[..8]));
    }

    #[test]
    fn output_search_run_scope_rebinds_to_the_active_run_on_every_submit() {
        let mut current = snapshot();
        current.head_seq = 100;
        let mut first_run = agent_run("first");
        first_run.start_seq = 10;
        current.active_run = Some(first_run.clone());
        let mut app = App::new(vec![current], None);
        app.open_output_search();
        let (commands, mut received) = mpsc::channel(2);
        app.output_search_commands = Some(commands);
        {
            let search = app.output_search.as_mut().expect("search");
            search.scope = OutputSearchScope::CurrentRun;
            search.query = "login".chars().collect();
            search.cursor = search.query.len();
        }
        let mut replacement = agent_run("replacement");
        replacement.start_seq = 105;
        app.ports[0].snapshot.active_run = Some(replacement.clone());
        app.ports[0].snapshot.head_seq = 120;

        app.handle_output_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        let OutputSearchIoCommand::Query(request) = received.try_recv().expect("query") else {
            panic!("expected query");
        };
        assert_ne!(replacement.id, first_run.id);
        assert_eq!(request.current_run.map(|run| run.id), Some(replacement.id));
        assert_eq!(request.current_run.map(|run| run.start_seq), Some(105));
        assert_eq!(request.current_run.map(|run| run.through_seq), Some(120));

        let mut without_run = snapshot();
        without_run.head_seq = 121;
        let mut app = App::new(vec![without_run], None);
        app.open_output_search();
        let (commands, mut no_query) = mpsc::channel(1);
        app.output_search_commands = Some(commands);
        {
            let search = app.output_search.as_mut().expect("search");
            search.scope = OutputSearchScope::CurrentRun;
            search.query = "login".chars().collect();
            search.cursor = search.query.len();
        }
        app.handle_output_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(no_query.try_recv().is_err());
        assert!(
            app.output_search
                .as_ref()
                .and_then(|search| search.error.as_deref())
                .is_some_and(|error| error == tr("ui.output.search.no.run"))
        );
    }

    #[test]
    fn output_search_results_navigate_records_and_scroll_selected_detail() {
        let mut app = App::new(vec![snapshot()], None);
        app.open_output_search();
        let request_id = Uuid::new_v4();
        app.output_search.as_mut().expect("search").phase = OutputSearchPhase::Loading(request_id);
        let first = event(EventKind::Rx, Direction::Rx, 2, b"newest match");
        let second = event(EventKind::Tx, Direction::Tx, 1, b"older match");
        app.handle_output_search_io_event(OutputSearchIoEvent::Completed {
            request_id,
            response: OutputSearchResponse {
                events: vec![first, second],
                gaps: Vec::new(),
                partial: true,
                scanned_archives: 2,
            },
        });

        app.handle_output_search_key(KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE));
        assert_eq!(
            app.output_search
                .as_ref()
                .map(|search| search.detail_scroll),
            Some(5)
        );
        app.handle_output_search_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        let search = app.output_search.as_ref().expect("results remain open");
        assert_eq!(search.selected, 1);
        assert_eq!(search.detail_scroll, 0);
        assert!(search.partial);
        assert_eq!(search.scanned_archives, 2);
    }

    #[test]
    fn tab_completion_cycles_deduplicated_newest_first_candidates() {
        let mut app = App::new(vec![snapshot()], None);
        {
            let view = &mut app.ports[0];
            view.history = vec![
                "show version".into(),
                "reset".into(),
                "show interfaces".into(),
                "show version".into(),
            ];
            view.draft = "sh".chars().collect();
            view.draft_cursor = 2;
        }
        let (commands, _) = mpsc::channel(4);

        for expected in ["show version", "show interfaces", "show version"] {
            app.handle_line_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), &commands);
            assert_eq!(app.ports[0].draft.iter().collect::<String>(), expected);
        }

        // Any other key confirms the candidate and leaves completion mode.
        app.handle_line_key(
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
            &commands,
        );
        assert!(app.ports[0].completion.is_none());
        assert_eq!(
            app.ports[0].draft.iter().collect::<String>(),
            "show version "
        );

        // An empty draft completes from the full history, newest first.
        app.ports[0].draft.clear();
        app.ports[0].draft_cursor = 0;
        app.handle_line_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), &commands);
        assert_eq!(
            app.ports[0].draft.iter().collect::<String>(),
            "show version"
        );
    }

    #[test]
    fn enter_send_returns_the_view_to_the_live_tail() {
        let mut app = ready_app_with_control();
        app.ports[0].scroll_from_bottom = 5;
        app.ports[0].unseen = 3;
        app.ports[0].draft = "version".chars().collect();
        app.ports[0].draft_cursor = 7;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert_eq!(app.ports[0].scroll_from_bottom, 0);
        assert_eq!(app.ports[0].unseen, 0);
        assert!(received.try_recv().is_ok());
    }

    #[test]
    fn empty_enter_follows_the_serial_tail_without_sending() {
        let mut app = ready_app_with_control();
        app.ports[0].scroll_from_bottom = 5;
        app.ports[0].unseen = 3;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert_eq!(app.focus, PaneFocus::Input);
        assert_eq!(app.current().scroll_from_bottom, 0);
        assert_eq!(app.current().unseen, 0);
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn line_send_uses_the_model_profiles_effective_eol() {
        let mut app = ready_app_with_control();
        app.ports[0].snapshot.effective_write_eol = Some("\n".into());
        app.ports[0].draft = "version".chars().collect();
        app.ports[0].draft_cursor = 7;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        let (_, bytes, _) = take_write(&mut received);
        assert_eq!(bytes, b"version\n");
    }

    #[test]
    fn input_mode_is_isolated_per_port() {
        let first = snapshot();
        let mut second = snapshot();
        second.config.port = "COM4".into();
        let mut app = App::new(vec![first, second], None);
        app.ports[0].mode = InputMode::Raw;

        app.select(1);
        assert_eq!(app.current_mode(), InputMode::Line);

        app.select(0);
        assert_eq!(app.current_mode(), InputMode::Raw);
    }

    fn snapshot() -> SlotSnapshot {
        SlotSnapshot {
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
            generation: 1,
            endpoint_present: true,
            session_state: SessionState::Online,
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
            effective_write_eol: Some("\r".into()),
            effective_echo: Some(EchoMode::On),
            effective_transport: None,
            effective_write_pacing: Some(WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 1,
            }),
        }
    }

    fn editable_profile_fixture() -> (SlotSnapshot, MenuCatalog) {
        let mut current = snapshot();
        current.config.model_profile = Some("dut-console".into());
        let transport = TransportProfile {
            name: current
                .config
                .transport_profile
                .clone()
                .expect("fixture transport profile"),
            baud_rate: 115_200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        };
        let device = ModelProfile {
            name: "dut-console".into(),
            model_names: vec!["DUT Console 1.0".into()],
            shell_prompt: Some("dut# ".into()),
            uboot_prompt: Some("dut=> ".into()),
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::On),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        };
        let catalog = MenuCatalog {
            ports: vec![current.clone()],
            detected_ports: Vec::new(),
            config_revision: Some(41),
            transport_profiles: vec![transport],
            transport_revision: Some(41),
            model_profiles: vec![device],
            model_profile_revision: Some(41),
        };
        (current, catalog)
    }

    fn trigger_info(snapshot: &SlotSnapshot, status: TriggerStatus) -> TriggerInfo {
        TriggerInfo {
            id: Uuid::new_v4(),
            owner: Actor {
                id: "agent:trigger-test".into(),
                label: "Trigger test Agent".into(),
                kind: ActorKind::Agent,
            },
            daemon_epoch: snapshot.daemon_epoch,
            generation: snapshot.generation,
            control_id: snapshot
                .control
                .as_ref()
                .map_or_else(Uuid::new_v4, |lease| lease.id),
            fence: snapshot.control.as_ref().map_or(1, |lease| lease.fence),
            operation_id: Some(Uuid::new_v4()),
            expected_run_id: None,
            spec: TriggerSpec {
                initial_write: None,
                start_contains: None,
                action: b"slp".to_vec(),
                interval_ms: 20,
                stop_contains: vec![b"ready".to_vec()],
                timeout_ms: 1_000,
                max_fires: 50,
                pacing: None,
            },
            status,
            start_seq: 1,
            end_seq: status.is_terminal().then_some(2),
            last_write_seq: None,
            fires_confirmed: 0,
            tx_bytes_confirmed: 0,
            matched_pattern: None,
        }
    }

    fn agent_run(label: &str) -> RunInfo {
        RunInfo {
            id: Uuid::new_v4(),
            owner: Actor {
                id: "agent:test".into(),
                label: "Test Agent".into(),
                kind: ActorKind::Agent,
            },
            label: label.into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        }
    }

    fn ready_app_with_control() -> App {
        let mut snapshot = snapshot();
        let actor = Actor {
            id: "human:test".into(),
            label: "Test operator".into(),
            kind: ActorKind::Human,
        };
        snapshot.control = Some(ControlLease {
            id: Uuid::new_v4(),
            owner: actor.clone(),
            epoch: snapshot.daemon_epoch,
            generation: snapshot.generation,
            fence: 1,
            issued_wall_time_ns: 1,
            expires_wall_time_ns: i64::MAX,
        });
        let mut app = App::new(vec![snapshot], None);
        app.transport_connected = true;
        app.hello_accepted = true;
        app.connection_generation = Some(1);
        app.actor = Some(actor);
        app.ports[0].subscription = SubscriptionPhase::Ready { head_seq: 0 };
        app
    }

    fn ready_app_with_foreign_control() -> App {
        let mut app = ready_app_with_control();
        app.ports[0]
            .snapshot
            .control
            .as_mut()
            .expect("test control")
            .owner = Actor {
            id: "agent:foreign".into(),
            label: "Foreign Agent".into(),
            kind: ActorKind::Agent,
        };
        app
    }

    fn take_write(received: &mut mpsc::Receiver<NetworkCommand>) -> (Uuid, Vec<u8>, Option<Uuid>) {
        let NetworkCommand::Send { message, .. } = received.try_recv().expect("write command")
        else {
            panic!("expected outbound write")
        };
        let ClientMessage::Write {
            request_id,
            data,
            operation_id,
            ..
        } = message
        else {
            panic!("expected outbound write")
        };
        (request_id, data, operation_id)
    }

    fn event(kind: EventKind, direction: Direction, seq: u64, data: &[u8]) -> TimelineEvent {
        TimelineEvent {
            port: "COM3".into(),
            daemon_epoch: Uuid::new_v4(),
            seq,
            generation: 1,
            wall_time_ns: 100,
            monotonic_time_ns: 100,
            kind,
            direction,
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

    fn monitor_incident(epoch: Uuid, seq_start: u64, seq_end: u64) -> MonitorIncident {
        let monitor_id = Uuid::new_v4();
        MonitorIncident {
            id: Uuid::new_v4(),
            incident_seq: 1,
            monitor_id,
            port: "COM3".into(),
            daemon_epoch: epoch,
            seq_start,
            seq_end,
            wall_time_start_ns: 100,
            wall_time_end_ns: 200,
            severity: serial_protocol::MonitorSeverity::Warning,
            description: Some("串口异常".into()),
            matches: vec![serial_protocol::MonitorMatch {
                index: 0,
                matcher: MonitorMatcher::Contains("alarm".into()),
            }],
            preview: "alarm".into(),
            evidence_cursor: Cursor {
                epoch,
                after_seq: seq_start.saturating_sub(1),
            },
            evidence_ref: format!("serial://COM3/{epoch}/{seq_start}-{seq_end}"),
            created_wall_time_ns: 200,
            acked_wall_time_ns: None,
        }
    }

    fn select_monitor_incident(app: &mut App, incident: MonitorIncident) {
        let monitor_id = incident.monitor_id;
        let incident_id = incident.id;
        app.current_mut()
            .monitor_history
            .push_back(MonitorHistoryEntry {
                monitor: MonitorView {
                    id: monitor_id,
                    revision: 1,
                    spec: serial_protocol::MonitorSpec {
                        port: incident.port.clone(),
                        matchers: vec![MonitorMatcher::Contains("alarm".into())],
                        start_cursor: None,
                        severity: serial_protocol::MonitorSeverity::Warning,
                        description: Some("串口异常".into()),
                        debounce_ms: 0,
                        cooldown_ms: 0,
                        duration_ms: None,
                    },
                    status: MonitorStatus::Running,
                    created_wall_time_ns: 100,
                    started_wall_time_ns: 100,
                    expires_wall_time_ns: None,
                    stopped_wall_time_ns: None,
                    current_cursor: None,
                    incident_count: 1,
                    unacked_incident_count: 1,
                    gap_count: 0,
                    last_error: None,
                },
                incidents: VecDeque::from([incident]),
                limited: false,
            });
        let view = app.current_mut();
        view.selected_monitor = Some(monitor_id);
        view.expanded_monitor = Some(monitor_id);
        view.selected_monitor_matcher = Some(0);
        view.selected_monitor_incident = Some(incident_id);
        app.focus = PaneFocus::RunHistory;
        app.layout = Some(ConsoleLayout {
            output_area: Rect::new(0, 0, 80, 20),
            output_inner: Rect::new(1, 1, 78, 18),
            input_area: Rect::new(0, 20, 80, 3),
            run_history_area: None,
            run_history_inner: None,
        });
    }

    fn focus_run_history_for_jump(app: &mut App) {
        app.focus = PaneFocus::RunHistory;
        app.layout = Some(ConsoleLayout {
            output_area: Rect::new(0, 0, 80, 20),
            output_inner: Rect::new(1, 1, 78, 18),
            input_area: Rect::new(0, 20, 80, 3),
            run_history_area: None,
            run_history_inner: None,
        });
    }

    fn described_agent_tx(
        run: &RunInfo,
        epoch: Uuid,
        seq: u64,
        data: &[u8],
        matcher: &str,
    ) -> TimelineEvent {
        let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata
            .insert("command_description".into(), serde_json::json!("读取状态"));
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "contains", "value": matcher}]),
        );
        tx
    }

    fn described_agent_prompt_tx(
        run: &RunInfo,
        epoch: Uuid,
        seq: u64,
        data: &[u8],
        description: &str,
        prompt: &str,
    ) -> TimelineEvent {
        let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata
            .insert("command_description".into(), serde_json::json!(description));
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "shell_prompt", "value": prompt}]),
        );
        tx
    }

    fn stream_row(seq: u64, direction: Direction, text: &str) -> DisplayLine {
        DisplayLine {
            daemon_epoch: None,
            seq,
            event_kind: match direction {
                Direction::Rx => EventKind::Rx,
                Direction::Tx => EventKind::Tx,
                Direction::None => EventKind::Checkpoint,
            },
            source: if direction == Direction::Tx {
                "HUMAN:test[abcd1234]>".into()
            } else {
                "DEV".into()
            },
            bytes: text.len() + 16,
            source_style: Style::default(),
            marker_color: (direction == Direction::Tx).then_some(Color::Green),
            solid_style: None,
            run_boundary: None,
            echoed: false,
            text: text.into(),
        }
    }

    #[test]
    fn detailed_source_column_adapts_to_common_terminal_widths() {
        // 80 outer columns -> 78 inner columns: keep 48+ payload columns
        // instead of spending a fixed 28 columns on the source.
        assert_eq!(detailed_source_width(78), 16);
        assert_eq!(detailed_source_width(118), 28);
        assert_eq!(detailed_source_width(58), 10);
    }

    #[test]
    fn run_boundary_fills_the_terminal_width() {
        let mut row = stream_row(1, Direction::None, "RUN START · smoke · 12345678");
        row.run_boundary = Some(RunBoundary::Started);
        let line = timeline_line(&row, false, 10, None, None, 64);
        let rendered = line
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert_eq!(UnicodeWidthStr::width(rendered.as_str()), 64);
        assert!(rendered.contains("RUN START · smoke · 12345678"));
        assert!(rendered.starts_with('─'));
        assert!(rendered.ends_with('─'));
    }

    #[test]
    fn wrapped_live_output_keeps_the_latest_prompt_visible_at_eighty_columns() {
        let mut app = App::new(vec![snapshot()], None);
        app.ports[0].push_line(stream_row(1, Direction::Rx, &"x".repeat(2_000)), true);
        app.ports[0].pending_line = Some(stream_row(2, Direction::Rx, "__LATEST_PROMPT__ "));
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("__LATEST_PROMPT__"));
    }

    #[test]
    fn visual_separators_do_not_repeat_trigger_or_control_details() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let mut trigger = trigger_info(&app.ports[0].snapshot, TriggerStatus::Running);
        trigger.fires_confirmed = 7;
        let short_id = trigger.id.to_string().chars().take(8).collect::<String>();
        app.ports[0].snapshot.active_trigger = Some(trigger);
        let backend = TestBackend::new(180, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains(tr("ui.separator.agent")));
        assert!(rendered.contains(tr("ui.separator.input")));
        assert!(!rendered.contains(&short_id));
        assert!(!rendered.contains("7 fire(s)"));
    }

    #[test]
    fn footer_shows_changed_status_temporarily_then_restores_help() {
        let mut app = App::new(vec![snapshot()], None);
        app.status = "CRITICAL_STATUS_NOTICE".into();
        let backend = TestBackend::new(120, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let notice = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(notice.contains("CRITICAL_STATUS_NOTICE"));

        assert!(app.expire_status_notice(Instant::now() + STATUS_NOTICE_DURATION));
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let restored = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!restored.contains("CRITICAL_STATUS_NOTICE"));
        assert!(restored.contains("Ctrl-]"));
    }

    #[test]
    fn help_shortcut_closes_help_without_inserting_question_mark() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(1);
        app.help = true;

        app.handle_key(
            KeyEvent::new(KeyCode::Char(']'), KeyModifiers::CONTROL),
            &commands,
        );
        assert!(!app.help);
        assert!(app.prefix_pending);

        app.handle_key(
            KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE),
            &commands,
        );
        assert!(!app.help);
        assert!(!app.prefix_pending);
        assert!(app.current().draft.is_empty());
    }

    #[test]
    fn grouped_help_uses_distinct_rows_and_scrolls_without_closing() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut app = App::new(vec![snapshot()], None);
        let lines = help_lines(&app);
        let plain = lines.iter().map(line_plain_text).collect::<Vec<_>>();
        let navigation = plain
            .iter()
            .position(|line| line == "导航与显示")
            .expect("navigation heading");
        let control = plain
            .iter()
            .position(|line| line == "控制权与 Agent 协作")
            .expect("control heading");
        assert!(control > navigation + 1);
        assert!(plain[navigation + 1].contains("Alt-1..9"));
        assert!(plain[navigation..control].iter().any(String::is_empty));
        assert!(plain.iter().any(|line| line.contains("接管当前串口")));

        app.help = true;
        app.layout = Some(ConsoleLayout {
            output_area: Rect::new(0, 0, 60, 8),
            output_inner: Rect::new(1, 1, 58, 6),
            input_area: Rect::new(0, 9, 60, 3),
            run_history_area: None,
            run_history_inner: None,
        });
        let (commands, _) = mpsc::channel(1);
        app.handle_key(
            KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
            &commands,
        );
        assert!(app.help);
        assert!(app.help_scroll > 0);
        app.handle_key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE), &commands);
        assert!(app.help);
        assert_eq!(app.help_scroll, 0);
        app.handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE), &commands);
        assert!(!app.help);
    }

    #[test]
    fn explicit_takeover_is_tracked_separately_from_an_ordinary_queue_acquire() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut app = ready_app_with_foreign_control();
        app.ports[0].snapshot.active_run = Some(agent_run("升级固件"));
        let (commands, mut received) = mpsc::channel(2);

        assert!(app.acquire_control(&commands, ControlMode::Takeover));
        let NetworkCommand::Send { message, .. } = received.try_recv().expect("takeover command")
        else {
            panic!("expected outbound takeover request")
        };
        let ClientMessage::AcquireControl {
            request_id, mode, ..
        } = message
        else {
            panic!("expected acquire-control message")
        };
        assert_eq!(mode, ControlMode::Takeover);
        assert!(matches!(
            app.pending_requests.get(&request_id),
            Some(PendingRequest::Acquire {
                mode: ControlMode::Takeover,
                ..
            })
        ));
        assert!(app.status.contains("当前 Agent 任务将被中止"));

        let lease = ControlLease {
            id: Uuid::new_v4(),
            owner: app.actor.clone().expect("human actor"),
            epoch: app.ports[0].snapshot.daemon_epoch,
            generation: app.ports[0].snapshot.generation,
            fence: 2,
            issued_wall_time_ns: 2,
            expires_wall_time_ns: i64::MAX,
        };
        app.handle_result(
            request_id,
            CommandResult::ControlGranted { lease },
            &commands,
        );
        assert!(app.status.contains("之前的 Agent 任务已被中止"));
    }

    #[test]
    fn run_aborted_event_surfaces_the_human_takeover_reason() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut app = ready_app_with_foreign_control();
        let run = agent_run("检查启动日志");
        app.ports[0].snapshot.active_run = Some(run.clone());
        let daemon_epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(2);
        let mut aborted = event(EventKind::RunAborted, Direction::None, 2, &[]);
        aborted.daemon_epoch = daemon_epoch;
        aborted
            .metadata
            .insert("run".into(), serde_json::to_value(&run).unwrap());
        aborted.metadata.insert(
            "reason".into(),
            serde_json::json!("human takeover requested from terminal"),
        );

        app.push_event(aborted, false, &commands);

        assert!(app.ports[0].snapshot.active_run.is_none());
        assert!(app.status.contains("Agent 任务已中止"));
        assert!(app.status.contains("检查启动日志"));
        assert!(app.status.contains("human takeover"));
    }

    #[test]
    fn run_history_keeps_only_described_agent_commands_and_merges_operation_chunks() {
        let mut view = SlotView::new(snapshot());
        let epoch = view.snapshot.daemon_epoch;
        let run = agent_run("检查系统版本");
        let mut started = event(EventKind::RunStarted, Direction::None, 1, &[]);
        started.daemon_epoch = epoch;
        started.actor = Some(run.owner.clone());
        started.run_id = Some(run.id);
        started
            .metadata
            .insert("run".into(), serde_json::to_value(&run).unwrap());
        view.push_event(started, true);

        let first_operation = Uuid::new_v4();
        for (seq, data) in [(2, b"show ".as_slice()), (3, b"version\r".as_slice())] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(first_operation);
            tx.metadata.insert(
                "command_description".into(),
                serde_json::json!("读取系统版本"),
            );
            tx.metadata
                .insert("partial".into(), serde_json::json!(seq == 3));
            view.push_event(tx.clone(), true);
            if seq == 2 {
                // Replayed duplicate sequence must not duplicate payload.
                view.push_event(tx, true);
            }
        }

        let mut second = event(EventKind::Tx, Direction::Tx, 4, b"uname -a\r");
        second.daemon_epoch = epoch;
        second.actor = Some(run.owner.clone());
        second.run_id = Some(run.id);
        second.operation_id = Some(Uuid::new_v4());
        second.metadata.insert(
            "command_description".into(),
            serde_json::json!("读取内核版本"),
        );
        view.push_event(second, true);

        let mut undescribed = event(EventKind::Tx, Direction::Tx, 5, b"raw bytes");
        undescribed.daemon_epoch = epoch;
        undescribed.actor = Some(run.owner.clone());
        undescribed.run_id = Some(run.id);
        view.push_event(undescribed, true);

        let mut human = event(EventKind::Tx, Direction::Tx, 6, b"human command\r");
        human.daemon_epoch = epoch;
        human.actor = Some(Actor {
            id: "human:test".into(),
            label: "Test operator".into(),
            kind: ActorKind::Human,
        });
        human.run_id = Some(run.id);
        human.metadata.insert(
            "command_description".into(),
            serde_json::json!("人工协作输入"),
        );
        view.push_event(human, true);

        let mut ended_run = run.clone();
        ended_run.status = RunStatus::Completed;
        ended_run.end_seq = Some(7);
        let mut ended = event(EventKind::RunEnded, Direction::None, 7, &[]);
        ended.daemon_epoch = epoch;
        ended.actor = Some(run.owner.clone());
        ended.run_id = Some(run.id);
        ended
            .metadata
            .insert("run".into(), serde_json::to_value(&ended_run).unwrap());
        view.push_event(ended, true);

        assert_eq!(view.run_history.len(), 1);
        let history = &view.run_history[0];
        assert_eq!(history.status, RunStatus::Completed);
        assert_eq!(history.commands.len(), 2);
        assert_eq!(history.commands[0].steps.len(), 1);
        assert_eq!(history.commands[0].steps[0].data, b"show version\r");
        assert_eq!(
            history.commands[0].description.as_deref(),
            Some("读取系统版本")
        );
        assert_eq!(history.commands[1].steps[0].data, b"uname -a\r");
        assert_eq!(view.run_command_keys()[0].first_seq, 2);
        assert_eq!(view.run_command_keys()[1].first_seq, 4);

        let mut human_run = run.clone();
        human_run.id = Uuid::new_v4();
        human_run.owner.kind = ActorKind::Human;
        let mut human_started = event(EventKind::RunStarted, Direction::None, 8, &[]);
        human_started.daemon_epoch = epoch;
        human_started.actor = Some(human_run.owner.clone());
        human_started.run_id = Some(human_run.id);
        human_started
            .metadata
            .insert("run".into(), serde_json::to_value(&human_run).unwrap());
        view.push_event(human_started, true);
        assert_eq!(
            view.run_history.len(),
            1,
            "Human Runs stay out of Agent history"
        );
    }

    #[test]
    fn run_history_groups_command_sequence_steps_under_one_purpose() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut current = snapshot();
        let run = agent_run("登录样机");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        let sequence_id = Uuid::new_v4();
        for (seq, index, purpose, command, partial) in [
            (2, 0, "输入账号", b"admin\r".as_slice(), false),
            (3, 1, "输入密码", b"password\r".as_slice(), true),
        ] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, command);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!(purpose));
            tx.metadata.insert(
                "command_sequence_id".into(),
                serde_json::json!(sequence_id.to_string()),
            );
            tx.metadata.insert(
                "command_sequence_description".into(),
                serde_json::json!("登录样机控制台"),
            );
            tx.metadata.insert(
                "command_sequence_step_index".into(),
                serde_json::json!(index),
            );
            tx.metadata
                .insert("command_sequence_step_count".into(), serde_json::json!(2));
            tx.metadata
                .insert("partial".into(), serde_json::json!(partial));
            app.ports[0].push_event(tx, true);
        }

        let history = &app.current().run_history[0];
        assert_eq!(history.commands.len(), 1);
        assert_eq!(history.commands[0].sequence_id, Some(sequence_id));
        assert_eq!(
            history.commands[0].description.as_deref(),
            Some("登录样机控制台")
        );
        assert_eq!(history.commands[0].steps.len(), 2);

        let key = app.current().selected_run_command_key().unwrap();
        app.current_mut().expanded_run_command = Some(key);
        let rendered = run_history_rows(&app, 80)
            .into_iter()
            .flat_map(|row| row.line.spans)
            .map(|span| span.content.into_owned())
            .collect::<String>();
        assert_eq!(rendered.matches("登录样机控制台").count(), 1);
        assert!(rendered.contains("admin"));
        assert!(rendered.contains("password"));
        assert!(!rendered.contains('\u{2705}'));
        assert!(!rendered.contains('\u{274c}'));
        assert!(!rendered.contains("已确认发送"));
    }

    #[test]
    fn command_and_monitor_actions_render_and_navigate_in_one_wall_time_order() {
        let mut current = snapshot();
        let run = agent_run("交错历史");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for (seq, wall_time, description) in [(2, 10, "命令一"), (4, 30, "命令二")] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, description.as_bytes());
            tx.daemon_epoch = epoch;
            tx.wall_time_ns = wall_time;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!(description));
            app.ports[0].push_event(tx, true);
        }
        let monitor_id = Uuid::new_v4();
        app.ports[0].monitor_history.push_back(MonitorHistoryEntry {
            monitor: MonitorView {
                id: monitor_id,
                revision: 1,
                spec: serial_protocol::MonitorSpec {
                    port: "COM3".into(),
                    matchers: vec![MonitorMatcher::Contains("alarm".into())],
                    start_cursor: None,
                    severity: serial_protocol::MonitorSeverity::Warning,
                    description: Some("监控一".into()),
                    debounce_ms: 250,
                    cooldown_ms: 30_000,
                    duration_ms: None,
                },
                status: MonitorStatus::Running,
                created_wall_time_ns: 20,
                started_wall_time_ns: 20,
                expires_wall_time_ns: None,
                stopped_wall_time_ns: None,
                current_cursor: None,
                incident_count: 0,
                unacked_incident_count: 0,
                gap_count: 0,
                last_error: None,
            },
            incidents: VecDeque::new(),
            limited: false,
        });
        app.focus = PaneFocus::RunHistory;

        let rendered = run_history_rows(&app, 100)
            .into_iter()
            .map(|row| line_plain_text(&row.line))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.find("命令一").unwrap() < rendered.find("监控一").unwrap());
        assert!(rendered.find("监控一").unwrap() < rendered.find("命令二").unwrap());

        app.handle_run_history_key(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE));
        assert!(matches!(
            app.current().selected_history_action_key(),
            Some(HistoryActionKey::Command(RunCommandKey {
                first_seq: 2,
                ..
            }))
        ));
        app.handle_run_history_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(
            app.current().selected_history_action_key(),
            Some(HistoryActionKey::Monitor(monitor_id))
        );
        app.handle_run_history_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert!(matches!(
            app.current().selected_history_action_key(),
            Some(HistoryActionKey::Command(RunCommandKey {
                first_seq: 4,
                ..
            }))
        ));
    }

    #[test]
    fn monitor_incident_jump_uses_a_complete_same_epoch_local_window() {
        let current = snapshot();
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for seq in 10..=12 {
            let mut row = stream_row(seq, Direction::Rx, &format!("local-{seq}"));
            row.daemon_epoch = Some(epoch);
            app.ports[0].push_line(row, true);
        }
        select_monitor_incident(&mut app, monitor_incident(epoch, 10, 12));
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_monitor_incident());
        assert!(received.try_recv().is_err());
        let snapshot = app
            .current()
            .scroll_snapshot
            .as_ref()
            .expect("complete local evidence snapshot");
        let rendered = snapshot
            .rows
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("local-10"));
        assert!(rendered.contains("local-12"));
        let pane_width = usize::from(app.layout.unwrap().output_inner.width);
        assert!(snapshot.rows.iter().all(|line| {
            line.spans
                .iter()
                .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                .sum::<usize>()
                == pane_width
                && line.style.bg == Some(COMMAND_CAPTURE_BACKGROUND)
                && line
                    .spans
                    .iter()
                    .all(|span| span.style.bg == Some(COMMAND_CAPTURE_BACKGROUND))
        }));
    }

    #[test]
    fn monitor_incident_jump_queries_when_the_local_window_is_fully_evicted() {
        let current = snapshot();
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for seq in 20..=22 {
            let mut row = stream_row(seq, Direction::Rx, &format!("newer-{seq}"));
            row.daemon_epoch = Some(epoch);
            app.ports[0].push_line(row, true);
        }
        let incident = monitor_incident(epoch, 10, 12);
        let expected = ExactEvidenceTarget::Incident(IncidentEvidenceTarget::from(&incident));
        select_monitor_incident(&mut app, incident);
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_monitor_incident());
        let ExactEvidenceIoCommand::Query(request) =
            received.try_recv().expect("journal fallback query");
        assert_eq!(request.target, expected);
        assert!(app.current().scroll_snapshot.is_none());
    }

    #[test]
    fn monitor_incident_jump_queries_when_only_half_the_local_range_remains() {
        let current = snapshot();
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for seq in 11..=12 {
            let mut row = stream_row(seq, Direction::Rx, &format!("partial-{seq}"));
            row.daemon_epoch = Some(epoch);
            app.ports[0].push_line(row, true);
        }
        let incident = monitor_incident(epoch, 10, 12);
        select_monitor_incident(&mut app, incident);
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_monitor_incident());
        let ExactEvidenceIoCommand::Query(request) = received
            .try_recv()
            .expect("partial range must query journal");
        assert!(matches!(
            request.target,
            ExactEvidenceTarget::Incident(IncidentEvidenceTarget {
                seq_start: 10,
                seq_end: 12,
                ..
            })
        ));
        assert!(app.current().scroll_snapshot.is_none());
    }

    #[test]
    fn monitor_incident_jump_queries_and_highlights_an_archived_epoch() {
        let current = snapshot();
        let current_epoch = current.daemon_epoch;
        let archived_epoch = Uuid::new_v4();
        let mut app = App::new(vec![current], None);
        for seq in 10..=35 {
            let mut row = stream_row(seq, Direction::Rx, &format!("current-{seq}"));
            row.daemon_epoch = Some(current_epoch);
            app.ports[0].push_line(row, true);
        }
        let incident = monitor_incident(archived_epoch, 10, 35);
        select_monitor_incident(&mut app, incident);
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_monitor_incident());
        let ExactEvidenceIoCommand::Query(request) =
            received.try_recv().expect("archived epoch query");
        let events = (10..=35)
            .map(|seq| {
                let mut event = if seq == 20 {
                    event(EventKind::SerialClosed, Direction::None, seq, &[])
                } else {
                    event(
                        EventKind::Rx,
                        Direction::Rx,
                        seq,
                        format!("archived-{seq}\r\n").as_bytes(),
                    )
                };
                event.daemon_epoch = archived_epoch;
                event
            })
            .collect::<Vec<_>>();
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Completed {
            request_id: request.request_id,
            response: ExactEvidenceResponse {
                target: request.target,
                events,
            },
        });

        assert_eq!(app.current().snapshot.daemon_epoch, current_epoch);
        let snapshot = app
            .current()
            .scroll_snapshot
            .as_ref()
            .expect("archived evidence snapshot");
        let rendered = snapshot
            .rows
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains("archived-10"));
        assert!(rendered.contains("archived-35"));
        let pane_width = usize::from(app.layout.unwrap().output_inner.width);
        assert!(
            snapshot
                .rows
                .iter()
                .filter(|line| line_plain_text(line).contains("archived-"))
                .all(|line| {
                    line.spans
                        .iter()
                        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                        .sum::<usize>()
                        == pane_width
                        && line.style.bg == Some(COMMAND_CAPTURE_BACKGROUND)
                        && line
                            .spans
                            .iter()
                            .all(|span| span.style.bg == Some(COMMAND_CAPTURE_BACKGROUND))
                })
        );
        assert!(snapshot.rows.iter().any(|line| {
            !line_plain_text(line).contains("archived-")
                && line.style.bg != Some(Color::Rgb(28, 53, 66))
                && line
                    .spans
                    .iter()
                    .all(|span| span.style.bg != Some(Color::Rgb(28, 53, 66)))
        }));
        let visible = visible_output_lines(&app, app.layout.unwrap().output_inner)
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(visible.contains("archived-10"));
        assert!(!visible.contains("archived-35"));
    }

    #[test]
    fn monitor_incident_gap_and_query_failure_never_leave_a_wrong_snapshot() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::En);
        let current = snapshot();
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        select_monitor_incident(&mut app, monitor_incident(epoch, 10, 12));
        let (commands, mut received) = mpsc::channel(4);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_monitor_incident());
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("gap query");
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Failed {
            request_id: request.request_id,
            target: request.target,
            failure: ExactEvidenceFailure::Gap(GapRange {
                epoch,
                first_seq: 10,
                last_seq: 11,
                reason: serial_protocol::GapReason::Retention,
            }),
        });
        assert!(app.current().scroll_snapshot.is_none());
        assert!(app.status.contains("journal gap #10-#11"));

        assert!(app.jump_output_to_monitor_incident());
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("failed query");
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Failed {
            request_id: request.request_id,
            target: request.target,
            failure: ExactEvidenceFailure::QueryFailed("test backend unavailable".into()),
        });
        assert!(app.current().scroll_snapshot.is_none());
        assert!(app.status.contains("test backend unavailable"));
    }

    #[test]
    fn truncated_local_command_tail_queries_journal_and_never_highlights_the_partial_tail() {
        let mut current = snapshot();
        let epoch = current.daemon_epoch;
        current.head_seq = 5;
        let run = agent_run("截断命令证据");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        app.ports[0].push_event(
            described_agent_tx(&run, epoch, 2, b"show status\r", "dut# "),
            true,
        );
        for (seq, text) in [(4, "partial tail"), (5, "dut# ")] {
            let mut row = stream_row(seq, Direction::Rx, text);
            row.daemon_epoch = Some(epoch);
            app.ports[0].push_line(row, true);
        }
        app.ports[0].local_history_truncated = true;
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, None));
        let ExactEvidenceIoCommand::Query(request) = received
            .try_recv()
            .expect("truncated local capture must query the journal");
        assert!(matches!(
            request.target,
            ExactEvidenceTarget::Command(CommandEvidenceTarget {
                daemon_epoch,
                seq_start: 2,
                query_end_seq: 5,
                ..
            }) if daemon_epoch == epoch
        ));
        assert!(app.current().scroll_snapshot.is_none());
        assert!(all_output_visual_rows(&app, 78).iter().all(|row| {
            row.line.style.bg != Some(Color::Rgb(28, 53, 66))
                && row
                    .line
                    .spans
                    .iter()
                    .all(|span| span.style.bg != Some(Color::Rgb(28, 53, 66)))
        }));
    }

    #[test]
    fn discontinuous_local_command_events_query_journal_instead_of_highlighting_merged_rows() {
        let mut current = snapshot();
        let epoch = current.daemon_epoch;
        current.head_seq = 5;
        let run = agent_run("缺序命令证据");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        app.ports[0].push_event(
            described_agent_tx(&run, epoch, 2, b"show status\r", "dut# "),
            true,
        );
        let mut output = event(EventKind::Rx, Direction::Rx, 3, b"value=ready\r\n");
        output.daemon_epoch = epoch;
        app.ports[0].push_event(output, true);
        let mut prompt = event(EventKind::Rx, Direction::Rx, 5, b"dut# \r\n");
        prompt.daemon_epoch = epoch;
        app.ports[0].push_event(prompt, true);
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, None));
        let ExactEvidenceIoCommand::Query(request) = received
            .try_recv()
            .expect("a silent raw sequence discontinuity must query the journal");
        assert!(matches!(request.target, ExactEvidenceTarget::Command(_)));
        assert!(app.current().scroll_snapshot.is_none());
    }

    #[test]
    fn command_journal_backfill_restores_complete_rx_only_highlight() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::En);
        let mut current = snapshot();
        let epoch = current.daemon_epoch;
        current.head_seq = 5;
        let run = agent_run("日志回填命令");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        let tx = described_agent_prompt_tx(&run, epoch, 2, b"show status\r", "读取状态", "dut# ");
        app.ports[0].push_event(tx.clone(), true);
        app.ports[0].local_history_truncated = true;
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, None));
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("journal query");
        let mut rx3 = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"dut# show status\r\nvalue=ready\r\n",
        );
        rx3.daemon_epoch = epoch;
        let mut rx4 = event(EventKind::Rx, Direction::Rx, 4, b"more output\r\n");
        rx4.daemon_epoch = epoch;
        let mut rx5 = event(EventKind::Rx, Direction::Rx, 5, b"dut# \r\n");
        rx5.daemon_epoch = epoch;
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Completed {
            request_id: request.request_id,
            response: ExactEvidenceResponse {
                target: request.target,
                events: vec![tx, rx3, rx4, rx5],
            },
        });

        let snapshot = app
            .current()
            .scroll_snapshot
            .as_ref()
            .expect("exact command evidence snapshot");
        let highlighted = snapshot
            .rows
            .iter()
            .filter(|line| line.style.bg == Some(COMMAND_CAPTURE_BACKGROUND))
            .map(line_plain_text)
            .collect::<String>();
        assert!(highlighted.contains("show status"));
        assert!(highlighted.contains("value=ready"));
        assert!(highlighted.contains("more output"));
        assert!(highlighted.contains("dut#"));
        assert_eq!(highlighted.matches("show status").count(), 1);
        let pane_width = usize::from(app.layout.unwrap().output_inner.width);
        assert!(
            snapshot
                .rows
                .iter()
                .filter(|line| line.style.bg == Some(COMMAND_CAPTURE_BACKGROUND))
                .all(|line| {
                    line.spans
                        .iter()
                        .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                        .sum::<usize>()
                        == pane_width
                })
        );
        assert!(
            app.status.contains("exact command evidence loaded"),
            "{}",
            app.status
        );
    }

    #[test]
    fn archived_command_epoch_is_queried_and_rendered_without_using_current_rows() {
        let mut current = snapshot();
        let archived_epoch = current.daemon_epoch;
        current.head_seq = 3;
        let run = agent_run("旧周期命令");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        let tx = described_agent_tx(&run, archived_epoch, 2, b"version\r", "old# ");
        app.ports[0].push_event(tx.clone(), true);
        let current_epoch = Uuid::new_v4();
        app.ports[0].snapshot.daemon_epoch = current_epoch;
        app.ports[0].last_epoch = Some(current_epoch);
        let mut current_row = stream_row(3, Direction::Rx, "current epoch output");
        current_row.daemon_epoch = Some(current_epoch);
        app.ports[0].push_line(current_row, true);
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, None));
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("archive query");
        assert_eq!(request.target.daemon_epoch(), archived_epoch);
        let mut rx = event(EventKind::Rx, Direction::Rx, 3, b"version 1.0\r\nold# \r\n");
        rx.daemon_epoch = archived_epoch;
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Completed {
            request_id: request.request_id,
            response: ExactEvidenceResponse {
                target: request.target,
                events: vec![tx, rx],
            },
        });

        let rendered = app
            .current()
            .scroll_snapshot
            .as_ref()
            .expect("archived command evidence")
            .rows
            .iter()
            .map(line_plain_text)
            .collect::<String>();
        assert!(rendered.contains("version 1.0"));
        assert!(!rendered.contains("current epoch output"));
        assert_eq!(app.current().snapshot.daemon_epoch, current_epoch);
    }

    #[test]
    fn command_gap_failure_returns_to_live_tail_without_partial_highlight() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::En);
        let mut current = snapshot();
        let epoch = current.daemon_epoch;
        current.head_seq = 5;
        let run = agent_run("缺口命令");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        app.ports[0].push_event(
            described_agent_tx(&run, epoch, 2, b"status\r", "dut# "),
            true,
        );
        app.ports[0].local_history_truncated = true;
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, None));
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("gap query");
        app.current_mut().scroll_snapshot = Some(ScrollSnapshot {
            rows: vec![Line::from("operator scrolled while the query was pending")],
        });
        app.current_mut().scroll_from_bottom = 1;
        app.handle_exact_evidence_io_event(ExactEvidenceIoEvent::Failed {
            request_id: request.request_id,
            target: request.target,
            failure: ExactEvidenceFailure::Gap(GapRange {
                epoch,
                first_seq: 3,
                last_seq: 4,
                reason: serial_protocol::GapReason::Retention,
            }),
        });

        assert!(app.current().scroll_snapshot.is_none());
        assert_eq!(app.current().scroll_from_bottom, 0);
        assert!(app.status.contains("journal gap #3-#4"));
    }

    #[test]
    fn command_sequence_step_queries_only_its_exact_matcher_range() {
        let mut current = snapshot();
        let epoch = current.daemon_epoch;
        current.head_seq = 5;
        let run = agent_run("登录序列");
        current.active_run = Some(run.clone());
        let mut app = App::new(vec![current], None);
        let sequence_id = Uuid::new_v4();
        for (seq, index, data, matcher) in [
            (2, 0, b"login\r".as_slice(), "Username:"),
            (4, 1, b"admin\r".as_slice(), "Password:"),
        ] {
            let mut tx = described_agent_tx(&run, epoch, seq, data, matcher);
            tx.metadata
                .insert("command_sequence_id".into(), serde_json::json!(sequence_id));
            tx.metadata.insert(
                "command_sequence_description".into(),
                serde_json::json!("登录设备"),
            );
            tx.metadata.insert(
                "command_sequence_step_index".into(),
                serde_json::json!(index),
            );
            app.ports[0].push_event(tx, true);
        }
        app.ports[0].local_history_truncated = true;
        focus_run_history_for_jump(&mut app);
        let key = app.current().selected_run_command_key().unwrap();
        app.current_mut().expanded_run_command = Some(key);
        app.current_mut().selected_run_step = Some(0);
        let (commands, mut received) = mpsc::channel(2);
        app.exact_evidence_commands = Some(commands);

        assert!(app.jump_output_to_run_command(key, Some(0)));
        let ExactEvidenceIoCommand::Query(request) = received.try_recv().expect("step query");
        let ExactEvidenceTarget::Command(target) = request.target else {
            panic!("expected command evidence target");
        };
        assert_eq!(target.step_index, Some(0));
        assert_eq!(
            (target.seq_start, target.write_end_seq, target.query_end_seq),
            (2, 2, 3)
        );
        assert_eq!(target.matchers[0].value, "Username:");
    }

    #[test]
    fn monitor_incident_journal_evidence_requires_both_endpoints_and_no_missing_sequence() {
        let epoch = Uuid::new_v4();
        let incident = monitor_incident(epoch, 10, 12);
        let target = IncidentEvidenceTarget::from(&incident);
        let events = (10..=12)
            .map(|seq| {
                let mut event = event(EventKind::Rx, Direction::Rx, seq, b"evidence\n");
                event.daemon_epoch = epoch;
                event
            })
            .collect::<Vec<_>>();

        assert!(incident_evidence_is_complete(&target, &events));
        assert!(!incident_evidence_is_complete(&target, &events[1..]));
        assert!(!incident_evidence_is_complete(
            &target,
            &events[..events.len() - 1]
        ));
        assert!(!incident_evidence_is_complete(
            &target,
            &[events[0].clone(), events[2].clone()]
        ));
        let mut wrong_epoch = events;
        wrong_epoch[1].daemon_epoch = Uuid::new_v4();
        assert!(!incident_evidence_is_complete(&target, &wrong_epoch));
    }

    #[test]
    fn run_history_bar_marks_tail_and_gap_history_as_recent_only() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::En);
        let mut app = App::new(vec![snapshot()], None);

        assert!(
            app.current().run_history_limited,
            "a first attach has only the bounded tail, not a sequence-one proof"
        );
        let row_text = run_history_rows(&app, 44)
            .into_iter()
            .flat_map(|row| row.line.spans)
            .map(|span| span.content.into_owned())
            .collect::<String>();
        assert!(!row_text.contains("Recent records only"));

        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains(tr("ui.separator.agent.recent")));

        app.current_mut().run_history_limited = false;
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let complete = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(complete.contains(tr("ui.separator.agent")));
        assert!(!complete.contains(tr("ui.separator.agent.recent")));

        app.current_mut()
            .push_gap(10, "test durable journal gap", true);
        assert!(app.current().run_history_limited);
    }

    #[test]
    fn agent_history_content_rows_are_configurable_and_bounded() {
        assert_eq!(configured_agent_history_rows(None), 5);
        assert_eq!(configured_agent_history_rows(Some(1)), 3);
        assert_eq!(configured_agent_history_rows(Some(12)), 12);
        assert_eq!(configured_agent_history_rows(Some(99)), 20);

        let mut app = App::new(vec![snapshot()], None);
        app.agent_history_rows = 12;
        let backend = TestBackend::new(100, 40);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        assert_eq!(
            app.layout
                .and_then(|layout| layout.run_history_area)
                .expect("inline Agent history")
                .height,
            12
        );
    }

    #[test]
    fn orphan_run_timeout_defaults_and_preserves_valid_configuration() {
        assert_eq!(configured_orphan_run_timeout_seconds(None), 1800);
        assert_eq!(configured_orphan_run_timeout_seconds(Some(0)), 0);
        assert_eq!(configured_orphan_run_timeout_seconds(Some(3600)), 3600);
        assert_eq!(
            configured_orphan_run_timeout_seconds(Some(u64::MAX)),
            u64::MAX
        );
    }

    #[test]
    fn display_menu_selects_and_persists_agent_history_height() {
        let temporary = std::env::temp_dir().join(format!(
            "serialctl-display-settings-test-{}",
            Uuid::new_v4().simple()
        ));
        let path = temporary.join("serialctl.toml");
        let mut app = App::new(vec![snapshot()], None);
        app.config = Some(LoadedConfig {
            path: path.clone(),
            config: crate::config::ClientConfig::default(),
        });
        let mut menu = MenuState::new();
        menu.busy = true;
        menu.selected = 2;

        app.activate_menu_item(&mut menu);
        assert_eq!(menu.page, MenuPage::Settings);
        assert_eq!(menu.selected, 0);
        app.activate_menu_item(&mut menu);
        assert_eq!(menu.page, MenuPage::DisplaySettings);
        assert_eq!(menu.selected, 0);

        app.activate_menu_item(&mut menu);
        let mut prompt = menu.prompt.take().expect("exact row-count prompt");
        prompt.value = "12".chars().collect();
        prompt.cursor = prompt.value.len();
        app.handle_menu_prompt_key(
            &mut menu,
            prompt,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert_eq!(app.agent_history_rows, 12);
        assert!(menu.message.contains("12"));

        let saved = LoadedConfig::load(Some(path)).expect("saved display settings");
        assert_eq!(saved.config.agent_history_rows, Some(12));
        std::fs::remove_dir_all(temporary).expect("remove display settings fixture");
    }

    #[test]
    fn current_profile_page_edits_fields_and_submits_observed_revisions() {
        let (current, mut catalog) = editable_profile_fixture();
        let mut transport_peer = current.clone();
        transport_peer.config.port = "COM4".into();
        transport_peer.config.model_profile = None;
        let mut device_peer = current.clone();
        device_peer.config.port = "COM5".into();
        device_peer.config.transport_profile = Some("other-uart".into());
        catalog
            .ports
            .extend([transport_peer.clone(), device_peer.clone()]);
        catalog.detected_ports.push(PortDescriptor {
            name: "/dev/ttyUSB7".into(),
            port_type: "usb".into(),
            manufacturer: None,
            product: None,
            serial_number: None,
        });
        let mut app = App::new(vec![current], None);
        let (menu_commands, mut received) = mpsc::channel(2);
        app.menu_commands = Some(menu_commands);
        let mut menu = MenuState::new();
        menu.page = MenuPage::Profiles;
        menu.catalog = Some(catalog);
        menu.busy = false;
        app.refresh_current_profile_editor(&mut menu);

        let rows = menu_rows(&app, &menu)
            .into_iter()
            .map(|line| line_plain_text(&line))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), CURRENT_PROFILE_ROW_COUNT + 4);
        assert_eq!(rows[0], tr("menu.current.section.serial"));
        assert_eq!(rows[11], tr("menu.current.section.model"));
        assert!(rows[1].starts_with("▶     "));
        assert!(rows[12].starts_with("      "));
        for expected in [
            "COM3",
            "115200",
            "DTR",
            "RTS",
            "dut-console",
            "dut#",
            "dut=>",
        ] {
            assert!(
                rows.iter().any(|row| row.contains(expected)),
                "complete current profile should show {expected}: {rows:?}"
            );
        }

        menu.selected = CurrentProfileRow::Port as usize;
        app.activate_current_profile_row(&mut menu);
        let mut choice = menu.choice.take().expect("serial-port choices");
        choice.selected = choice
            .options
            .iter()
            .position(|option| option.label == "/dev/ttyUSB7")
            .expect("new detected Port");
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        menu.selected = CurrentProfileRow::BaudRate as usize;
        app.activate_current_profile_row(&mut menu);
        let mut choice = menu.choice.take().expect("baud-rate choices");
        choice.selected = choice
            .options
            .iter()
            .position(|option| option.label == "921600")
            .expect("921600 choice");
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        menu.selected = CurrentProfileRow::DataBits as usize;
        app.activate_current_profile_row(&mut menu);
        let mut choice = menu.choice.take().expect("data-bit choices");
        choice.selected = 0;
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        menu.selected = CurrentProfileRow::Echo as usize;
        app.activate_current_profile_row(&mut menu);
        let mut choice = menu.choice.take().expect("echo choices");
        choice.selected = 2;
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        menu.selected = CurrentProfileRow::Apply as usize;
        app.activate_current_profile_row(&mut menu);

        assert!(
            received.try_recv().is_err(),
            "shared profile updates must not submit before explicit confirmation"
        );
        let confirmation = menu
            .confirmation
            .take()
            .expect("shared profile impact confirmation");
        let confirmation_text = confirmation.lines.join("\n");
        for expected in ["COM3", "COM4", "COM5"] {
            assert!(
                confirmation_text.contains(expected),
                "confirmation must list every affected Port: {confirmation_text}"
            );
        }
        app.handle_menu_confirmation_key(
            &mut menu,
            confirmation,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );

        let MenuIoCommand::Mutation { mutation } =
            received.try_recv().expect("profile update command")
        else {
            panic!("expected a trusted current-profile update")
        };
        let MenuMutation::UpdateCurrentProfiles(update) = *mutation else {
            panic!("expected a current-profile update")
        };
        let CurrentProfileUpdate {
            current_port,
            new_port: Some(new_port),
            transport: Some(transport),
            device: Some(device),
            revisions,
            ..
        } = *update
        else {
            panic!("expected a current-profile update")
        };
        assert_eq!(current_port, "COM3");
        assert_eq!(new_port, "/dev/ttyUSB7");
        assert_eq!(revisions.config, Some(41));
        assert_eq!(revisions.transport, Some(41));
        assert_eq!(revisions.device, Some(41));
        assert_eq!(transport.baud_rate, 921_600);
        assert_eq!(transport.data_bits, DataBits::Five);
        assert_eq!(device.echo, Some(EchoMode::Off));
    }

    #[test]
    fn port_choice_excludes_other_configured_ports_and_right_does_not_commit() {
        let (current, mut catalog) = editable_profile_fixture();
        let mut occupied = current.clone();
        occupied.config.port = "COM4".into();
        catalog.ports.push(occupied);
        catalog.detected_ports = ["COM3", "COM4", "COM7"]
            .into_iter()
            .map(|name| PortDescriptor {
                name: name.into(),
                port_type: "usb".into(),
                manufacturer: None,
                product: None,
                serial_number: None,
            })
            .collect();
        let mut app = App::new(vec![current], None);
        let mut menu = MenuState::new();
        menu.page = MenuPage::Profiles;
        menu.catalog = Some(catalog);
        app.refresh_current_profile_editor(&mut menu);

        menu.selected = CurrentProfileRow::Port as usize;
        app.activate_current_profile_row(&mut menu);
        let mut choice = menu.choice.take().expect("Port options");
        let labels = choice
            .options
            .iter()
            .map(|option| option.label.as_str())
            .collect::<Vec<_>>();
        assert_eq!(labels, vec!["COM3", "COM7"]);
        choice.selected = 1;
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Right, KeyModifiers::NONE),
        );
        assert_eq!(menu.profile_editor.as_ref().unwrap().port, "COM3");
        let choice = menu.choice.take().expect("Right keeps options expanded");
        app.handle_menu_choice_key(
            &mut menu,
            choice,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert_eq!(menu.profile_editor.as_ref().unwrap().port, "COM7");
        assert!(menu.choice.is_none());
    }

    #[test]
    fn successful_port_rename_rebuilds_view_and_reconnects_exact_port_set() {
        let (current, mut catalog) = editable_profile_fixture();
        let mut renamed = current.clone();
        renamed.config.port = "COM7".into();
        catalog.ports = vec![renamed];
        let mut app = App::new(vec![current], None);
        app.pending_writes.insert("COM3".into(), VecDeque::new());
        let (network_commands, mut received) = mpsc::channel(2);

        app.handle_menu_io_event(
            MenuIoEvent::Completed {
                catalog,
                success: MenuSuccess::ProfilesUpdated {
                    previous_port: "COM3".into(),
                    configured_port: "COM7".into(),
                },
            },
            &network_commands,
        );

        assert_eq!(app.ports.len(), 1);
        assert_eq!(app.selected_port(), "COM7");
        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(matches!(
            received.try_recv(),
            Ok(NetworkCommand::Reconfigure { ports }) if ports == vec!["COM7"]
        ));
    }

    #[test]
    fn model_name_navigation_selects_family_then_concrete_model() {
        let (current, catalog) = editable_profile_fixture();
        let mut app = App::new(vec![current], None);
        let mut menu = MenuState::new();
        menu.page = MenuPage::Profiles;
        menu.catalog = Some(catalog);
        app.refresh_current_profile_editor(&mut menu);

        menu.selected = CurrentProfileRow::ModelName as usize;
        app.activate_current_profile_row(&mut menu);
        assert_eq!(menu.page, MenuPage::ModelFamilies);
        app.activate_menu_item(&mut menu);
        assert_eq!(menu.page, MenuPage::ModelNames);
        app.activate_menu_item(&mut menu);

        assert_eq!(menu.page, MenuPage::Profiles);
        let editor = menu.profile_editor.as_ref().unwrap();
        assert_eq!(editor.model_profile_binding.as_deref(), Some("dut-console"));
        assert_eq!(editor.model_name.as_deref(), Some("DUT Console 1.0"));
    }

    #[test]
    fn shared_profile_impact_scans_transport_and_device_bindings_separately() {
        let (current, mut catalog) = editable_profile_fixture();
        let mut transport_peer = current.clone();
        transport_peer.config.port = "COM4".into();
        transport_peer.config.model_profile = None;
        let mut device_peer = current.clone();
        device_peer.config.port = "COM5".into();
        device_peer.config.transport_profile = Some("other-uart".into());
        catalog.ports.extend([transport_peer, device_peer]);

        let editor = CurrentProfileEditor::new(&SlotView::new(current), &catalog);
        let impacts =
            shared_profile_impacts(&catalog, Some(&editor.transport), Some(&editor.device));
        assert_eq!(
            impacts
                .transport
                .expect("transport impact")
                .ports
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec!["COM3", "COM4"]
        );
        assert_eq!(
            impacts
                .device
                .expect("device impact")
                .ports
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>(),
            vec!["COM3", "COM5"]
        );
    }

    #[test]
    fn run_settings_menu_validates_and_persists_the_orphan_timeout() {
        let _guard = crate::i18n::lang_test_lock();
        let temporary = std::env::temp_dir().join(format!(
            "serialctl-run-settings-test-{}",
            Uuid::new_v4().simple()
        ));
        let path = temporary.join("serialctl.toml");
        let mut app = App::new(vec![snapshot()], None);
        app.config = Some(LoadedConfig {
            path: path.clone(),
            config: crate::config::ClientConfig {
                capture_max_events: Some(8192),
                ..crate::config::ClientConfig::default()
            },
        });
        let mut menu = MenuState::new();
        menu.busy = true;
        menu.selected = 2;

        app.activate_menu_item(&mut menu);
        assert_eq!(menu.page, MenuPage::Settings);
        menu.selected = 1;
        app.activate_menu_item(&mut menu);
        assert_eq!(menu.page, MenuPage::McpSettings);
        app.activate_menu_item(&mut menu);
        let mut prompt = menu.prompt.take().expect("timeout prompt");
        prompt.value = "299".chars().collect();
        prompt.cursor = prompt.value.len();
        app.handle_menu_prompt_key(
            &mut menu,
            prompt,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert!(menu.prompt.is_some());
        assert_eq!(app.orphan_run_timeout_seconds, 1800);

        let mut prompt = menu.prompt.take().expect("retry timeout prompt");
        prompt.value = "3600".chars().collect();
        prompt.cursor = prompt.value.len();
        app.handle_menu_prompt_key(
            &mut menu,
            prompt,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert!(menu.prompt.is_none());
        assert_eq!(app.orphan_run_timeout_seconds, 3600);
        assert!(menu.message.contains("3600"));

        app.begin_orphan_run_timeout_prompt(&mut menu);
        let mut prompt = menu.prompt.take().expect("unlimited timeout prompt");
        prompt.value = "0".chars().collect();
        prompt.cursor = 1;
        app.handle_menu_prompt_key(
            &mut menu,
            prompt,
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
        );
        assert_eq!(app.orphan_run_timeout_seconds, 0);
        assert!(menu.message.contains(tr("menu.run.timeout.unlimited")));

        let saved = LoadedConfig::load(Some(path)).expect("saved Run settings");
        assert_eq!(saved.config.orphan_run_timeout_seconds, Some(0));
        assert_eq!(saved.config.capture_max_events, Some(8192));
        std::fs::remove_dir_all(temporary).expect("remove Run settings fixture");
    }

    #[test]
    fn out_of_order_replay_sorts_by_start_and_never_evicts_the_active_run() {
        let mut current = snapshot();
        let mut active = agent_run("当前任务");
        active.start_seq = 10_000;
        current.active_run = Some(active.clone());
        let epoch = current.daemon_epoch;
        let mut view = SlotView::new(current);
        view.run_history_limited = false;

        let historical_count = MAX_RUN_HISTORY_PER_SLOT + 5;
        for index in 0..historical_count {
            let mut run = agent_run(&format!("历史任务 {index}"));
            run.status = RunStatus::Completed;
            // Replay arrival is deliberately the inverse of authoritative
            // start order, exercising insertion-order independence.
            run.start_seq = (historical_count - index) as u64;
            run.end_seq = Some(100 + index as u64);
            let mut ended = event(
                EventKind::RunEnded,
                Direction::None,
                100 + index as u64,
                &[],
            );
            ended.daemon_epoch = epoch;
            ended.actor = Some(run.owner.clone());
            ended.run_id = Some(run.id);
            ended
                .metadata
                .insert("run".into(), serde_json::to_value(&run).unwrap());
            view.push_event(ended, true);
        }

        assert_eq!(view.run_history.len(), MAX_RUN_HISTORY_PER_SLOT);
        assert!(view.run_history_limited, "bounded eviction is disclosed");
        assert!(view.run_history.iter().any(|run| run.id == active.id));
        let starts = view
            .run_history
            .iter()
            .map(|run| run.start_seq)
            .collect::<Vec<_>>();
        assert!(starts.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(
            starts[0],
            (historical_count - MAX_RUN_HISTORY_PER_SLOT + 2) as u64,
            "the oldest start sequences, not the earliest insertions, are evicted"
        );
        assert_eq!(
            view.run_history_chronological().last().unwrap().id,
            active.id,
            "the newest authoritative Run stays at the bottom"
        );
    }

    #[test]
    fn run_history_bar_selects_commands_and_expands_only_the_confirmed_tx() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut current = snapshot();
        let run = agent_run("版本巡检");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for (seq, description, data) in [
            (2, "read system version", b"show version\r".as_slice()),
            (3, "read kernel version", b"uname -a\r".as_slice()),
        ] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!(description));
            tx.metadata
                .insert("partial".into(), serde_json::json!(seq == 2));
            app.ports[0].push_event(tx, true);
        }

        assert_eq!(app.current().selected_run_command_index(), Some(1));
        app.focus = PaneFocus::RunHistory;
        app.handle_run_history_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
        assert_eq!(app.current().selected_run_command_index(), Some(0));
        app.handle_run_history_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        assert_eq!(
            app.current().expanded_run_command,
            Some(RunCommandKey {
                run_id: run.id,
                first_seq: 2,
            })
        );

        let backend = TestBackend::new(140, 28);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("read system version"));
        assert!(rendered.contains("show version"));
        assert!(rendered.contains("read kernel version"));
        let row_text = run_history_rows(&app, 44)
            .into_iter()
            .flat_map(|row| row.line.spans)
            .map(|span| span.content.into_owned())
            .collect::<String>();
        assert!(row_text.contains("show version"));
        assert!(!row_text.contains('\u{2705}'));
        assert!(!row_text.contains('\u{274c}'));
        assert!(!row_text.contains("已确认发送"));

        app.focus = PaneFocus::Input;
        app.current_mut().expanded_run_command = None;
        let backend = TestBackend::new(90, 24);
        let mut narrow = Terminal::new(backend).expect("narrow test terminal");
        narrow.draw(|frame| draw(frame, &mut app)).unwrap();
        let horizontal = narrow
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(horizontal.contains("read system version"));
        let layout = app.layout.expect("horizontal history layout");
        let history_area = layout.run_history_area.expect("visible history bar");
        assert_eq!(history_area.x, layout.output_area.x);
        assert_eq!(history_area.width, layout.output_area.width);
        assert_eq!(
            history_area.y,
            layout.output_area.y + layout.output_area.height + 1
        );
        assert!(history_area.y + history_area.height <= layout.input_area.y);

        // Ctrl-] h focuses the bar first, then hides it when repeated while
        // focused. The shortcut can show it again without changing history.
        app.toggle_run_history_panel();
        assert_eq!(app.focus, PaneFocus::RunHistory);
        app.toggle_run_history_panel();
        assert!(!app.run_panel_visible);
        narrow.draw(|frame| draw(frame, &mut app)).unwrap();
        assert!(app.layout.unwrap().run_history_area.is_none());

        // A short terminal does not permanently sacrifice serial rows. Once
        // focused, the same history view is available as an on-demand popup.
        app.toggle_run_history_panel();
        let backend = TestBackend::new(90, 18);
        let mut short = Terminal::new(backend).expect("short test terminal");
        short.draw(|frame| draw(frame, &mut app)).unwrap();
        let popup = short
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(popup.contains("read system version"));
        assert!(
            app.layout
                .and_then(|layout| layout.run_history_area)
                .is_some()
        );
    }

    #[test]
    fn arrow_keys_browse_agent_history_and_printable_input_returns_to_editor() {
        let mut current = snapshot();
        let run = agent_run("方向键巡检");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for (seq, description, data) in [
            (2, "第一条", b"first\r".as_slice()),
            (3, "第二条", b"second\r".as_slice()),
        ] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!(description));
            app.ports[0].push_event(tx, true);
        }
        let (commands, _) = mpsc::channel(1);

        assert_eq!(app.current().selected_run_command_index(), Some(1));
        let history = run_history_rows(&app, 80)
            .into_iter()
            .map(|row| line_plain_text(&row.line))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(history.find("第一条").unwrap() < history.find("第二条").unwrap());
        app.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), &commands);
        assert_eq!(app.focus, PaneFocus::RunHistory);
        assert_eq!(app.current().selected_run_command_index(), Some(0));
        app.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE), &commands);
        assert_eq!(
            app.current().expanded_run_command,
            Some(RunCommandKey {
                run_id: run.id,
                first_seq: 2,
            })
        );

        app.handle_key(
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
            &commands,
        );
        assert_eq!(app.focus, PaneFocus::Input);
        assert_eq!(app.current().draft.iter().collect::<String>(), "x");
        assert!(app.current().scroll_snapshot.is_none());
    }

    #[test]
    fn selected_command_highlights_device_echo_through_the_prompt() {
        let mut current = snapshot();
        current.effective_shell_prompt = Some("dut# ".into());
        let run = agent_run("匹配输出");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);

        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"show version\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata.insert(
            "command_description".into(),
            serde_json::json!("读取系统版本"),
        );
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "shell_prompt", "value": "dut# "}]),
        );
        app.ports[0].push_event(tx, true);
        let mut rx = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"show version\r\nfirmware 1.0\r\ndut# \r\n",
        );
        rx.daemon_epoch = epoch;
        app.ports[0].push_event(rx, true);
        app.ports[0].push_line(stream_row(4, Direction::Rx, "later output"), true);
        app.focus = PaneFocus::RunHistory;

        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let rows = render_output_entries(&app, &entries, 80);
        let captured = rows
            .iter()
            .filter(|row| row.line.style.bg == Some(Color::Rgb(28, 53, 66)))
            .map(|row| line_plain_text(&row.line))
            .collect::<String>();
        assert!(captured.contains("show version"));
        assert!(captured.contains("firmware 1.0"));
        assert!(captured.contains("dut#"));
        assert!(!captured.contains("later output"));
        assert!(
            !rows
                .iter()
                .map(|row| line_plain_text(&row.line))
                .any(|line| line.starts_with('›'))
        );

        let rendered_lines = rows.iter().map(|row| row.line.clone()).collect::<Vec<_>>();
        let plain_lines = rendered_lines
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>();
        let height = rendered_lines.len().max(1) as u16;
        let backend = TestBackend::new(80, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| frame.render_widget(Paragraph::new(rendered_lines.clone()), frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer();
        for (row, line) in rendered_lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.style.bg == Some(COMMAND_CAPTURE_BACKGROUND))
        {
            assert_eq!(
                line.spans
                    .iter()
                    .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                    .sum::<usize>(),
                80
            );
            for column in 0..80 {
                assert_eq!(
                    buffer.content[row * 80 + column].bg,
                    COMMAND_CAPTURE_BACKGROUND,
                    "capture row {row} cell {column} lost its background"
                );
            }
        }
        let later_row = plain_lines
            .iter()
            .position(|line| line.contains("later output"))
            .expect("missing ordinary row after the capture");
        assert!((0..80).all(|column| {
            buffer.content[later_row * 80 + column].bg != COMMAND_CAPTURE_BACKGROUND
        }));
    }

    #[test]
    fn capture_highlight_fills_wrapped_wide_character_rows_and_preserves_foreground() {
        let width = 12;
        let source = Line::from(vec![
            Span::styled("device", Style::default().fg(Color::LightGreen)),
            Span::raw(" output "),
            Span::styled(
                "error",
                Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
            ),
            Span::raw(" abcdefghijklmnop"),
        ]);
        let mut rendered = wrap_command_capture_line(source, width);
        assert!(rendered.len() >= 3, "fixture must wrap across visual rows");
        assert!(rendered.iter().all(|line| {
            line.spans
                .iter()
                .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                .sum::<usize>()
                == usize::from(width)
        }));
        assert!(rendered.iter().flat_map(|line| &line.spans).any(|span| {
            span.content.contains("error")
                && span.style.fg == Some(Color::Red)
                && span.style.bg == Some(COMMAND_CAPTURE_BACKGROUND)
                && span.style.add_modifier.contains(Modifier::BOLD)
        }));
        let wide_rows = wrap_command_capture_line(Line::from("设备输出 abcdefghijklmnop"), width);
        assert!(wide_rows.len() >= 2);
        assert!(wide_rows.iter().all(|line| {
            line.spans
                .iter()
                .map(|span| UnicodeWidthStr::width(span.content.as_ref()))
                .sum::<usize>()
                == usize::from(width)
        }));

        let ordinary_row = rendered.len();
        rendered.extend(wrap_timeline_line(Line::from("未选中"), width));
        let backend = TestBackend::new(width, rendered.len() as u16);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| frame.render_widget(Paragraph::new(rendered.clone()), frame.area()))
            .unwrap();
        let buffer = terminal.backend().buffer();
        for row in 0..ordinary_row {
            for column in 0..usize::from(width) {
                assert_eq!(
                    buffer.content[row * usize::from(width) + column].bg,
                    COMMAND_CAPTURE_BACKGROUND,
                    "wrapped capture row {row} cell {column} lost its background"
                );
            }
        }
        assert!((0..usize::from(width)).all(|column| {
            buffer.content[ordinary_row * usize::from(width) + column].bg
                != COMMAND_CAPTURE_BACKGROUND
        }));
    }

    #[test]
    fn latest_of_multiple_commands_does_not_finish_at_the_echo_prompt_prefix() {
        let mut current = snapshot();
        current.effective_shell_prompt = Some("dut# ".into());
        let run = agent_run("连续读取状态");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);

        for mut event in [
            event(EventKind::Rx, Direction::Rx, 1, b"dut# "),
            described_agent_prompt_tx(&run, epoch, 2, b"show first\r", "第一条", "dut# "),
            event(
                EventKind::Rx,
                Direction::Rx,
                3,
                b"show first\r\nfirst output\r\ndut# ",
            ),
            described_agent_prompt_tx(&run, epoch, 4, b"show last\r", "最后一条", "dut# "),
            event(
                EventKind::Rx,
                Direction::Rx,
                5,
                b"show last\r\nlast output one\r\n",
            ),
            event(
                EventKind::Rx,
                Direction::Rx,
                6,
                b"last output two\r\n\x1b[32mdut# \x1b[0m",
            ),
        ] {
            event.daemon_epoch = epoch;
            app.ports[0].push_event(event, true);
        }
        app.focus = PaneFocus::RunHistory;

        let key = app.current().selected_run_command_key().unwrap();
        let target = app.command_evidence_target(key, None).unwrap();
        let entries = app.ports[0]
            .lines
            .iter()
            .chain(app.ports[0].pending_line.iter())
            .collect::<Vec<_>>();
        let capture = command_capture_for_target(&target, &entries);
        let (start, end) = capture
            .start
            .zip(capture.end)
            .expect("complete final command");
        let captured = entries[start..=end]
            .iter()
            .map(|entry| entry.text.as_str())
            .collect::<Vec<_>>();

        assert!(capture.highlight_available);
        assert_eq!(
            captured,
            [
                "dut# show last",
                "last output one",
                "last output two",
                "dut# "
            ]
        );
        assert!(!captured.iter().any(|line| line.contains("first output")));
    }

    #[test]
    fn command_capture_starts_at_first_device_result_without_echo() {
        let mut current = snapshot();
        let run = agent_run("无设备回显");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"show version\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata
            .insert("command_description".into(), serde_json::json!("读取版本"));
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "shell_prompt", "value": "dut# "}]),
        );
        app.ports[0].push_event(tx, true);
        let mut rx = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"firmware 2.0\r\ndut# \r\n",
        );
        rx.daemon_epoch = epoch;
        app.ports[0].push_event(rx, true);
        app.focus = PaneFocus::RunHistory;

        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let captured = render_output_entries(&app, &entries, 80)
            .into_iter()
            .filter(|row| row.line.style.bg == Some(Color::Rgb(28, 53, 66)))
            .map(|row| line_plain_text(&row.line))
            .collect::<String>();
        assert!(captured.contains("firmware 2.0"));
        assert!(captured.contains("dut#"));
        assert!(!captured.contains("show version"));
    }

    #[test]
    fn command_capture_does_not_cross_a_system_or_gap_row() {
        let mut current = snapshot();
        let run = agent_run("系统边界");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"status\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata
            .insert("command_description".into(), serde_json::json!("读取状态"));
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "shell_prompt", "value": "dut# "}]),
        );
        app.ports[0].push_event(tx, true);
        app.ports[0].push_line(gap_line(3, "journal gap"), true);
        app.ports[0].push_line(stream_row(4, Direction::Rx, "dut# "), true);
        app.focus = PaneFocus::RunHistory;

        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let rows = render_output_entries(&app, &entries, 80);
        assert!(
            rows.iter()
                .any(|row| line_plain_text(&row.line).trim_end() == "› status")
        );
        assert!(rows.iter().all(|row| {
            row.line.style.bg != Some(Color::Rgb(28, 53, 66))
                && row
                    .line
                    .spans
                    .iter()
                    .all(|span| span.style.bg != Some(Color::Rgb(28, 53, 66)))
        }));
    }

    #[test]
    fn explicit_regex_capture_matcher_overrides_an_earlier_profile_prompt() {
        let mut current = snapshot();
        current.effective_shell_prompt = Some("dut# ".into());
        let run = agent_run("等待显式结束条件");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);

        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"show status\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata
            .insert("command_description".into(), serde_json::json!("读取状态"));
        tx.metadata.insert(
            "command_capture_matchers".into(),
            serde_json::json!([{"kind": "regex", "value": "DONE\\s+[0-9]+"}]),
        );
        app.ports[0].push_event(tx, true);

        let mut rx = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"show status\r\ndut# early\r\nstill running\r\nDONE\r\n42\r\nafter\r\n",
        );
        rx.daemon_epoch = epoch;
        app.ports[0].push_event(rx, true);
        app.focus = PaneFocus::RunHistory;

        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let rows = render_output_entries(&app, &entries, 80);
        let captured = rows
            .iter()
            .filter(|row| row.line.style.bg == Some(Color::Rgb(28, 53, 66)))
            .map(|row| line_plain_text(&row.line))
            .collect::<Vec<_>>();
        assert!(captured.iter().any(|line| line.contains("dut# early")));
        assert!(captured.iter().any(|line| line.contains("still running")));
        assert!(captured.iter().any(|line| line.contains("42")));
        assert!(!captured.iter().any(|line| line.contains("after")));
        assert!(
            !rows
                .iter()
                .any(|row| line_plain_text(&row.line).starts_with('›'))
        );
    }

    #[test]
    fn command_sequence_capture_ends_at_the_last_steps_matcher() {
        let mut current = snapshot();
        let run = agent_run("登录设备");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let sequence_id = Uuid::new_v4();
        let mut app = App::new(vec![current], None);

        for (seq, index, command, matcher) in [
            (2, 0, b"login\r".as_slice(), "Username:"),
            (4, 1, b"admin\r".as_slice(), "Password:"),
        ] {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, command);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata.insert(
                "command_description".into(),
                serde_json::json!(format!("登录步骤 {}", index + 1)),
            );
            tx.metadata.insert(
                "command_sequence_description".into(),
                serde_json::json!("登录设备"),
            );
            tx.metadata
                .insert("command_sequence_id".into(), serde_json::json!(sequence_id));
            tx.metadata.insert(
                "command_sequence_step_index".into(),
                serde_json::json!(index),
            );
            tx.metadata.insert(
                "command_capture_matchers".into(),
                serde_json::json!([{"kind": "contains", "value": matcher}]),
            );
            app.ports[0].push_event(tx, true);

            let response = if index == 0 {
                b"login\r\nUsername:\r\n".as_slice()
            } else {
                b"admin\r\nPassword:\r\n".as_slice()
            };
            let mut rx = event(EventKind::Rx, Direction::Rx, seq + 1, response);
            rx.daemon_epoch = epoch;
            app.ports[0].push_event(rx, true);
        }
        app.ports[0].push_line(stream_row(6, Direction::Rx, "after login"), true);
        app.focus = PaneFocus::RunHistory;

        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let rows = render_output_entries(&app, &entries, 80);
        let captured = rows
            .iter()
            .filter(|row| row.line.style.bg == Some(Color::Rgb(28, 53, 66)))
            .map(|row| line_plain_text(&row.line))
            .collect::<String>();
        assert!(captured.contains("login"));
        assert!(captured.contains("Username:"));
        assert!(captured.contains("admin"));
        assert!(captured.contains("Password:"));
        assert!(!captured.contains("after login"));

        drop(entries);
        app.handle_run_history_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        assert_eq!(app.current().selected_run_step, Some(0));
        app.handle_run_history_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(app.current().selected_run_step, Some(1));
        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();
        let selected_step = render_output_entries(&app, &entries, 80)
            .into_iter()
            .filter(|row| row.line.style.bg == Some(Color::Rgb(28, 53, 66)))
            .map(|row| line_plain_text(&row.line))
            .collect::<String>();
        assert!(!selected_step.contains("Username:"));
        assert!(selected_step.contains("admin"));
        assert!(selected_step.contains("Password:"));
        drop(entries);
        app.handle_run_history_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE));
        assert!(app.current().selected_run_step.is_none());
    }

    #[test]
    fn command_sequence_last_step_does_not_finish_at_the_echo_prompt_prefix() {
        let mut current = snapshot();
        current.effective_shell_prompt = Some("dut# ".into());
        let run = agent_run("执行两步检查");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let sequence_id = Uuid::new_v4();
        let mut app = App::new(vec![current], None);

        let mut initial_prompt = event(EventKind::Rx, Direction::Rx, 1, b"dut# ");
        initial_prompt.daemon_epoch = epoch;
        app.ports[0].push_event(initial_prompt, true);
        for (tx_seq, index, command, response) in [
            (
                2,
                0,
                b"show first\r".as_slice(),
                b"show first\r\nfirst result\r\ndut# ".as_slice(),
            ),
            (
                4,
                1,
                b"show last\r".as_slice(),
                b"show last\r\nlast result one\r\nlast result two\r\ndut# ".as_slice(),
            ),
        ] {
            let mut tx = described_agent_prompt_tx(
                &run,
                epoch,
                tx_seq,
                command,
                &format!("步骤 {}", index + 1),
                "dut# ",
            );
            tx.metadata
                .insert("command_sequence_id".into(), serde_json::json!(sequence_id));
            tx.metadata.insert(
                "command_sequence_description".into(),
                serde_json::json!("两步检查"),
            );
            tx.metadata.insert(
                "command_sequence_step_index".into(),
                serde_json::json!(index),
            );
            app.ports[0].push_event(tx, true);

            let mut rx = event(EventKind::Rx, Direction::Rx, tx_seq + 1, response);
            rx.daemon_epoch = epoch;
            app.ports[0].push_event(rx, true);
        }
        app.focus = PaneFocus::RunHistory;

        let key = app.current().selected_run_command_key().unwrap();
        let target = app.command_evidence_target(key, Some(1)).unwrap();
        let entries = app.ports[0]
            .lines
            .iter()
            .chain(app.ports[0].pending_line.iter())
            .collect::<Vec<_>>();
        let capture = command_capture_for_target(&target, &entries);
        let (start, end) = capture.start.zip(capture.end).expect("complete last step");
        let captured = entries[start..=end]
            .iter()
            .map(|entry| entry.text.as_str())
            .collect::<Vec<_>>();

        assert!(capture.highlight_available);
        assert_eq!(
            captured,
            [
                "dut# show last",
                "last result one",
                "last result two",
                "dut# "
            ]
        );
        assert!(!captured.iter().any(|line| line.contains("first result")));
    }

    #[test]
    fn unmatched_command_uses_a_temporary_overlay_only_while_history_is_selected() {
        let mut current = snapshot();
        let run = agent_run("无回显输出");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);

        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"show version\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata.insert(
            "command_description".into(),
            serde_json::json!("读取系统版本"),
        );
        app.ports[0].push_event(tx, true);
        // A later Profile refresh must not retroactively reinterpret a command
        // whose TX persisted no capture matcher.
        app.ports[0].snapshot.effective_shell_prompt = Some("dut# ".into());
        let mut rx = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"show version\r\nfirmware 1.0\r\ndut# \r\n",
        );
        rx.daemon_epoch = epoch;
        app.ports[0].push_event(rx, true);
        let entries = app.ports[0].lines.iter().collect::<Vec<_>>();

        app.focus = PaneFocus::RunHistory;
        let selected_rows = render_output_entries(&app, &entries, 80);
        assert!(selected_rows.iter().all(|row| {
            row.line.style.bg != Some(Color::Rgb(28, 53, 66))
                && row
                    .line
                    .spans
                    .iter()
                    .all(|span| span.style.bg != Some(Color::Rgb(28, 53, 66)))
        }));
        let selected = selected_rows
            .into_iter()
            .map(|row| line_plain_text(&row.line))
            .collect::<Vec<_>>();
        assert!(
            selected
                .iter()
                .any(|line| line.trim_end() == "› show version")
        );

        app.focus = PaneFocus::Input;
        let ordinary = render_output_entries(&app, &entries, 80)
            .into_iter()
            .map(|row| line_plain_text(&row.line))
            .collect::<Vec<_>>();
        assert!(!ordinary.iter().any(|line| line.starts_with('›')));
        assert!(ordinary.iter().any(|line| line.contains("show version")));
    }

    #[test]
    fn expanding_command_history_jumps_serial_output_to_its_operation() {
        let mut current = snapshot();
        let run = agent_run("定位历史命令");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for seq in 1..30 {
            app.ports[0].push_line(
                stream_row(seq, Direction::Rx, &format!("before-{seq}")),
                true,
            );
        }
        let operation_id = Uuid::new_v4();
        let mut tx = event(EventKind::Tx, Direction::Tx, 30, b"show version\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(operation_id);
        tx.metadata.insert(
            "command_description".into(),
            serde_json::json!("读取系统版本"),
        );
        app.ports[0].push_event(tx, true);
        let mut echo = event(EventKind::Rx, Direction::Rx, 31, b"show version\r\n");
        echo.daemon_epoch = epoch;
        app.ports[0].push_event(echo, true);
        for seq in 32..70 {
            app.ports[0].push_line(
                stream_row(seq, Direction::Rx, &format!("after-{seq}")),
                true,
            );
        }

        let backend = TestBackend::new(100, 28);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        app.focus = PaneFocus::RunHistory;
        app.handle_run_history_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));

        assert!(app.current().scroll_snapshot.is_some());
        assert!(app.current().scroll_from_bottom > 0);
        let inner = app.layout.expect("console layout").output_inner;
        let visible = visible_output_lines(&app, inner)
            .iter()
            .map(line_plain_text)
            .collect::<String>();
        assert!(visible.contains("show version"), "jumped page: {visible}");
    }

    #[test]
    fn new_agent_command_returns_history_to_the_newest_action() {
        let mut current = snapshot();
        let run = agent_run("持续巡检");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);

        let described_tx = |seq: u64, description: &str, data: &[u8]| {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, data);
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!(description));
            tx
        };
        app.ports[0].push_event(described_tx(2, "第一条命令", b"first command"), true);
        assert!(app.current().selected_run_command.is_none());

        app.focus = PaneFocus::RunHistory;
        app.handle_run_history_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));
        let pinned = RunCommandKey {
            run_id: run.id,
            first_seq: 2,
        };
        assert_eq!(app.current().selected_run_command, Some(pinned));
        assert_eq!(app.current().expanded_run_command, Some(pinned));

        app.ports[0].push_event(described_tx(3, "第二条命令", b"second command"), true);
        let newest = RunCommandKey {
            run_id: run.id,
            first_seq: 3,
        };
        assert!(app.current().selected_run_command.is_none());
        assert_eq!(app.current().selected_run_command_key(), Some(newest));
        assert!(app.current().expanded_run_command.is_none());

        let backend = TestBackend::new(80, 28);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let area = app.layout.unwrap().run_history_area.unwrap();
        let (commands, _) = mpsc::channel(1);
        app.handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: area.x,
                row: area.y,
                modifiers: KeyModifiers::NONE,
            },
            &commands,
        );
        assert_eq!(app.current().selected_run_command_key(), Some(newest));
        assert!(app.current().expanded_run_command.is_none());
    }

    #[test]
    fn expanded_command_payload_wraps_without_clipping_ascii_cjk_or_emoji() {
        for payload in [
            "abcdefghijklmnopqrstuvwxyz0123456789",
            "中文样机命令参数甲乙丙丁戊己庚辛",
            "login-🔐-step-🔧-password-完成",
        ] {
            let mut current = snapshot();
            let run = agent_run("查看详情");
            current.active_run = Some(run.clone());
            let epoch = current.daemon_epoch;
            let mut app = App::new(vec![current], None);
            let mut tx = event(EventKind::Tx, Direction::Tx, 2, payload.as_bytes());
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata
                .insert("command_description".into(), serde_json::json!("用途"));
            app.ports[0].push_event(tx, true);
            app.focus = PaneFocus::RunHistory;
            app.handle_run_history_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE));

            let width = 18;
            let key = app.current().selected_run_command_key().unwrap();
            let command_rows = run_history_rows(&app, width)
                .into_iter()
                .filter(|row| row.command == Some(key) && row.step == Some(0))
                .map(|row| {
                    row.line
                        .spans
                        .into_iter()
                        .map(|span| span.content.into_owned())
                        .collect::<String>()
                })
                .collect::<Vec<_>>();
            assert!(command_rows.len() > 2, "payload should wrap: {payload}");
            assert!(
                command_rows
                    .iter()
                    .all(|row| { UnicodeWidthStr::width(row.as_str()) <= usize::from(width) })
            );
            let reconstructed = command_rows
                .iter()
                .map(|row| row.trim_start())
                .collect::<String>();
            assert_eq!(reconstructed, payload);
        }
    }

    #[test]
    fn mouse_wheel_scrolls_expanded_run_detail_without_collapsing_it() {
        let mut current = snapshot();
        let run = agent_run("读取长配置");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        let mut tx = event(EventKind::Tx, Direction::Tx, 2, &vec![b'x'; 2048]);
        tx.daemon_epoch = epoch;
        tx.actor = Some(run.owner.clone());
        tx.run_id = Some(run.id);
        tx.operation_id = Some(Uuid::new_v4());
        tx.metadata.insert(
            "command_description".into(),
            serde_json::json!("读取完整配置"),
        );
        app.ports[0].push_event(tx, true);
        app.focus = PaneFocus::RunHistory;
        let key = app.current().selected_run_command_key().unwrap();
        app.current_mut().expanded_run_command = Some(key);
        assert!(
            run_history_rows(&app, 20)
                .iter()
                .filter(|row| row.command == Some(key))
                .count()
                > 20,
            "expanded command bytes wrap into independently scrollable detail rows"
        );

        let backend = TestBackend::new(80, 28);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let area = app.layout.unwrap().run_history_area.unwrap();
        let (commands, _) = mpsc::channel(1);
        app.handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: area.x,
                row: area.y,
                modifiers: KeyModifiers::NONE,
            },
            &commands,
        );

        assert_eq!(app.current().expanded_run_command, Some(key));
        assert_eq!(app.current().selected_run_command_key(), Some(key));
        assert_eq!(app.current().run_detail_scroll, 5);

        let maximum = app.max_run_detail_scroll();
        assert!(maximum > 5);
        for _ in 0..100 {
            app.handle_mouse(
                MouseEvent {
                    kind: MouseEventKind::ScrollDown,
                    column: area.x,
                    row: area.y,
                    modifiers: KeyModifiers::NONE,
                },
                &commands,
            );
        }
        assert_eq!(app.current().run_detail_scroll, maximum);
        app.handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: area.x,
                row: area.y,
                modifiers: KeyModifiers::NONE,
            },
            &commands,
        );
        assert_eq!(
            app.current().run_detail_scroll,
            maximum.saturating_sub(5),
            "scrolling beyond the bottom must not create offset debt"
        );
    }

    #[test]
    fn scrolled_viewport_stays_anchored_when_new_lines_arrive() {
        let mut view = SlotView::new(snapshot());
        for seq in 0..20 {
            view.push_line(stream_row(seq, Direction::Rx, &format!("row-{seq}")), true);
        }
        view.scroll_from_bottom = 5;
        view.unseen = 0;
        // Anchor: the last visible row is index 20 - 5 - 1 = 14 ("row-14").
        for seq in 20..23 {
            view.push_line(stream_row(seq, Direction::Rx, &format!("row-{seq}")), true);
        }
        assert_eq!(view.scroll_from_bottom, 8);
        let end = view.lines.len() - view.scroll_from_bottom;
        assert_eq!(view.lines[end - 1].text, "row-14");
        assert_eq!(view.unseen, 3);
    }

    #[test]
    fn visual_scroll_does_nothing_when_history_fits_the_viewport() {
        let mut app = App::new(vec![snapshot()], None);
        for seq in 0..3 {
            app.ports[0].push_line(stream_row(seq, Direction::Rx, "short"), true);
        }
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");

        app.scroll_up(3);

        assert!(app.current().scroll_snapshot.is_none());
        assert_eq!(app.current().scroll_from_bottom, 0);
    }

    #[test]
    fn visual_scroll_can_move_inside_one_wrapped_logical_line() {
        let mut app = App::new(vec![snapshot()], None);
        app.ports[0].push_line(stream_row(1, Direction::Rx, &"x".repeat(2_000)), true);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");

        app.scroll_up(3);

        assert!(app.current().scroll_snapshot.is_some());
        assert_eq!(app.current().scroll_from_bottom, 3);
    }

    #[test]
    fn paused_visual_snapshot_does_not_drift_when_live_rows_arrive() {
        let mut app = App::new(vec![snapshot()], None);
        for seq in 0..30 {
            app.ports[0].push_line(stream_row(seq, Direction::Rx, &format!("row-{seq}")), true);
        }
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");
        let inner = app.layout.expect("layout").output_inner;
        app.scroll_up(3);
        let before = visible_output_lines(&app, inner)
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>();

        for seq in 30..35 {
            app.ports[0].push_line(stream_row(seq, Direction::Rx, &format!("live-{seq}")), true);
        }
        let after = visible_output_lines(&app, inner)
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>();

        assert_eq!(after, before);
        assert_eq!(app.current().scroll_from_bottom, 3);
        assert_eq!(app.current().unseen, 5);
        app.current_mut().follow();
        assert!(app.current().scroll_snapshot.is_none());
        assert!(
            visible_output_lines(&app, inner)
                .iter()
                .map(line_plain_text)
                .any(|line| line.contains("live-34"))
        );
    }

    #[test]
    fn detailed_view_change_releases_frozen_rows_for_every_slot() {
        let mut second = snapshot();
        second.config.port = "COM4".into();
        second.config.port = "Port 2".into();
        let mut app = App::new(vec![snapshot(), second], None);
        for slot in &mut app.ports {
            slot.scroll_snapshot = Some(ScrollSnapshot {
                rows: vec![Line::from("frozen")],
            });
            slot.scroll_from_bottom = 1;
            slot.unseen = 2;
        }
        let (commands, _) = mpsc::channel(1);

        app.handle_prefix_key(
            KeyEvent::new(KeyCode::Char('v'), KeyModifiers::NONE),
            &commands,
        );

        assert!(app.detailed_timeline);
        assert!(app.ports.iter().all(|slot| slot.scroll_snapshot.is_none()
            && slot.scroll_from_bottom == 0
            && slot.unseen == 0));
    }

    #[test]
    fn front_eviction_while_scrolled_keeps_the_offset_in_bounds() {
        let mut view = SlotView::new(snapshot());
        view.run_history_limited = false;
        for seq in 0..MAX_LINES_PER_SLOT as u64 {
            view.push_line(stream_row(seq, Direction::Rx, "row"), true);
        }
        view.scroll_from_bottom = 10;
        for seq in 0..5u64 {
            view.push_line(stream_row(20_000 + seq, Direction::Rx, "row"), true);
        }
        // Every append evicts one retained row. The first eviction also adds
        // the synthetic truncation boundary, so the virtual bottom offset is
        // one larger while the same retained anchor remains in view.
        assert_eq!(view.scroll_from_bottom, 11);
        assert!(view.scroll_from_bottom < view.logical_line_count());
        assert_eq!(view.lines.len(), MAX_LINES_PER_SLOT);
        assert!(view.local_history_truncated);
        assert!(view.run_history_limited);
        assert!(view.local_truncation_line().is_some());
    }

    #[test]
    fn local_history_eviction_keeps_an_authoritative_truncation_boundary() {
        let mut view = SlotView::new(snapshot());
        view.run_history_limited = false;
        for seq in 0..=MAX_LINES_PER_SLOT as u64 {
            view.push_line(stream_row(seq, Direction::Rx, "row"), true);
        }

        assert_eq!(view.lines.len(), MAX_LINES_PER_SLOT);
        assert!(view.local_history_truncated);
        assert!(view.run_history_limited);
        assert_eq!(view.logical_line_count(), MAX_LINES_PER_SLOT + 1);
        let boundary = view
            .local_truncation_line()
            .expect("synthetic local truncation boundary");
        assert!(boundary.text.contains("serialctl logs"));
        assert!(boundary.solid_style.is_some());

        // The warning is synthetic rather than part of the bounded deque, so
        // repeated front eviction cannot silently evict the warning itself.
        for seq in 0..100u64 {
            view.push_line(stream_row(30_000 + seq, Direction::Rx, "new"), true);
        }
        assert!(
            view.local_truncation_line()
                .expect("persistent truncation boundary")
                .text
                .contains("serialctl logs")
        );
    }

    #[test]
    fn serial_pane_ignores_tx_and_shows_only_the_device_echo() {
        let mut view = SlotView::new(snapshot());
        let epoch = view.snapshot.daemon_epoch;

        let mut prompt = event(EventKind::Rx, Direction::Rx, 1, b"[root@luckfox tmp]# ");
        prompt.daemon_epoch = epoch;
        view.push_event(prompt, true);

        let mut tx = event(EventKind::Tx, Direction::Tx, 2, b"cd\r");
        tx.daemon_epoch = epoch;
        tx.actor = Some(Actor {
            id: "human:test".into(),
            label: "Test operator".into(),
            kind: ActorKind::Human,
        });
        view.push_event(tx, true);
        assert_eq!(
            view.pending_line.as_ref().map(|line| line.text.as_str()),
            Some("[root@luckfox tmp]# ")
        );

        let mut echoed = event(EventKind::Rx, Direction::Rx, 3, b"cd\r\n");
        echoed.daemon_epoch = epoch;
        view.push_event(echoed, true);

        assert_eq!(view.lines.len(), 1);
        assert_eq!(view.lines[0].text, "[root@luckfox tmp]# cd");
        assert_eq!(view.lines[0].marker_color, None);
        assert!(!view.lines[0].echoed);
        assert!(view.pending_line.is_none());
    }

    #[test]
    fn raw_mode_also_shows_only_bytes_echoed_by_the_device() {
        let mut view = SlotView::new(snapshot());
        view.mode = InputMode::Raw;
        let epoch = view.snapshot.daemon_epoch;
        let mut seq = 1;

        let mut prompt = event(EventKind::Rx, Direction::Rx, seq, b"[root@luckfox ~]# ");
        prompt.daemon_epoch = epoch;
        view.push_event(prompt, true);
        for byte in *b"cd" {
            seq += 1;
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, &[byte]);
            tx.daemon_epoch = epoch;
            view.push_event(tx, true);
            seq += 1;
            let mut echoed = event(EventKind::Rx, Direction::Rx, seq, &[byte]);
            echoed.daemon_epoch = epoch;
            view.push_event(echoed, true);
        }

        assert!(view.lines.is_empty());
        assert_eq!(
            view.pending_line.as_ref().map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# cd")
        );
    }

    #[test]
    fn stale_replay_tx_does_not_duplicate_a_later_live_raw_echo() {
        let mut app = App::new(vec![snapshot()], None);
        let epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        app.handle_server_message(
            ServerMessage::ReplayBegin {
                port: "COM3".into(),
                from_seq: 1,
                through_seq: 3,
            },
            &commands,
        );

        let mut prompt = event(EventKind::Rx, Direction::Rx, 1, b"[root@luckfox ~]# ");
        prompt.daemon_epoch = epoch;
        app.push_event(prompt, true, &commands);

        let mut stale_tx = event(EventKind::Tx, Direction::Tx, 2, b"old-unmatched\r");
        stale_tx.daemon_epoch = epoch;
        stale_tx.actor = Some(Actor {
            id: "human:old".into(),
            label: "old operator".into(),
            kind: ActorKind::Human,
        });
        app.push_event(stale_tx, true, &commands);

        let mut unrelated = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"unrelated\r\n[root@luckfox ~]# ",
        );
        unrelated.daemon_epoch = epoch;
        app.push_event(unrelated, true, &commands);

        app.handle_server_message(
            ServerMessage::Ready {
                port: "COM3".into(),
                head_seq: 3,
            },
            &commands,
        );

        for (offset, byte) in b"pwd\r".iter().copied().enumerate() {
            let mut tx = event(EventKind::Tx, Direction::Tx, 4 + offset as u64, &[byte]);
            tx.daemon_epoch = epoch;
            tx.actor = Some(Actor {
                id: "human:live".into(),
                label: "live operator".into(),
                kind: ActorKind::Human,
            });
            app.push_event(tx, false, &commands);
        }

        let mut response = event(
            EventKind::Rx,
            Direction::Rx,
            8,
            b"pwd\r\n/oem\r\n[root@luckfox ~]# ",
        );
        response.daemon_epoch = epoch;
        app.push_event(response, false, &commands);

        let pwd_rows = app.ports[0]
            .lines
            .iter()
            .filter(|line| line.text.contains("pwd"))
            .collect::<Vec<_>>();
        assert_eq!(pwd_rows.len(), 1);
        assert_eq!(pwd_rows[0].text, "[root@luckfox ~]# pwd");
        assert!(!pwd_rows[0].echoed);
        assert!(app.ports[0].lines.iter().any(|line| line.text == "/oem"));
        assert_eq!(
            app.ports[0]
                .pending_line
                .as_ref()
                .map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# ")
        );
    }

    #[test]
    fn ready_does_not_project_an_in_flight_tx_into_rx_output() {
        let mut app = App::new(vec![snapshot()], None);
        let epoch = app.ports[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        let mut prompt = event(EventKind::Rx, Direction::Rx, 1, b"[root@luckfox ~]# ");
        prompt.daemon_epoch = epoch;
        app.push_event(prompt, true, &commands);

        let mut replay_tx = event(EventKind::Tx, Direction::Tx, 2, b"pwd\r");
        replay_tx.daemon_epoch = epoch;
        replay_tx.actor = Some(Actor {
            id: "agent:replay".into(),
            label: "replay agent".into(),
            kind: ActorKind::Agent,
        });
        app.push_event(replay_tx, true, &commands);

        app.handle_server_message(
            ServerMessage::Ready {
                port: "COM3".into(),
                head_seq: 2,
            },
            &commands,
        );

        let mut live_echo = event(
            EventKind::Rx,
            Direction::Rx,
            3,
            b"pwd\r\n/oem\r\n[root@luckfox ~]# ",
        );
        live_echo.daemon_epoch = epoch;
        app.push_event(live_echo, false, &commands);

        let pwd_rows = app.ports[0]
            .lines
            .iter()
            .filter(|line| line.text.contains("pwd"))
            .collect::<Vec<_>>();
        assert_eq!(pwd_rows.len(), 1);
        assert_eq!(pwd_rows[0].text, "[root@luckfox ~]# pwd");
        assert!(!pwd_rows[0].echoed);
        assert!(app.ports[0].lines.iter().any(|line| line.text == "/oem"));
    }

    #[test]
    fn line_input_cursor_uses_inner_columns_and_cjk_width() {
        let draft = "abc".chars().collect::<Vec<_>>();
        assert_eq!(line_input_projection(&draft, 3, 20), ("> abc".into(), 5));
        assert_eq!(line_input_projection(&draft, 1, 20), ("> abc".into(), 3));

        let cjk = "中a".chars().collect::<Vec<_>>();
        assert_eq!(line_input_projection(&cjk, 1, 20), ("> 中a".into(), 4));
        assert_eq!(line_input_projection(&cjk, 2, 20), ("> 中a".into(), 5));
    }

    #[test]
    fn software_cursor_marks_one_stable_display_cell() {
        let line = line_with_software_cursor("a中b".into(), 1, true);
        assert_eq!(line.spans.len(), 3);
        assert_eq!(line.spans[0].content, "a");
        assert_eq!(line.spans[1].content, "中");
        assert!(
            line.spans[1]
                .style
                .add_modifier
                .contains(Modifier::REVERSED)
        );
        assert_eq!(line.spans[2].content, "b");

        let end = line_with_software_cursor("a中b".into(), 4, true);
        assert_eq!(end.spans[1].content, " ");
        assert!(end.spans[1].style.add_modifier.contains(Modifier::REVERSED));

        let hidden = line_with_software_cursor("a中b".into(), 1, false);
        assert_eq!(hidden.spans[1].content, "中");
        assert!(
            !hidden.spans[1]
                .style
                .add_modifier
                .contains(Modifier::REVERSED)
        );
    }

    #[test]
    fn software_cursor_uses_a_slow_phase_and_input_resets_it_visible() {
        let mut app = App::new(vec![snapshot()], None);
        let start = Instant::now();
        app.reset_software_cursor_blink(start);

        assert!(app.software_cursor_visible);
        assert!(!app.update_software_cursor_blink(
            start + SOFTWARE_CURSOR_BLINK_INTERVAL - Duration::from_millis(1)
        ));
        assert!(app.update_software_cursor_blink(start + SOFTWARE_CURSOR_BLINK_INTERVAL));
        assert!(!app.software_cursor_visible);
        assert!(app.update_software_cursor_blink(start + SOFTWARE_CURSOR_BLINK_INTERVAL * 2));
        assert!(app.software_cursor_visible);

        let reset = start + SOFTWARE_CURSOR_BLINK_INTERVAL * 2 + Duration::from_millis(10);
        app.software_cursor_visible = false;
        app.reset_software_cursor_blink(reset);
        assert!(app.software_cursor_visible);
        assert!(!app.update_software_cursor_blink(
            reset + SOFTWARE_CURSOR_BLINK_INTERVAL - Duration::from_millis(1)
        ));
    }

    #[test]
    fn long_line_input_scrolls_horizontally_with_the_cursor() {
        let draft = "abcdef".chars().collect::<Vec<_>>();
        assert_eq!(line_input_projection(&draft, 6, 6), ("> def".into(), 5));
        assert_eq!(line_input_projection(&draft, 2, 6), ("> abcd".into(), 4));
    }

    #[test]
    fn empty_enter_during_foreign_agent_run_only_follows_live_output() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_foreign_control();
        app.ports[0].snapshot.active_run = Some(agent_run("diagnose boot"));
        app.ports[0].scroll_from_bottom = 5;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert!(received.try_recv().is_err());
        assert!(!app.pending_writes.contains_key("COM3"));
        assert_eq!(app.current().scroll_from_bottom, 0);
        assert!(app.status.contains("empty Enter"));
    }

    #[test]
    fn ordinary_enter_keeps_draft_when_local_enqueue_is_rejected() {
        let mut app = ready_app_with_foreign_control();
        app.transport_connected = false;
        app.ports[0].draft = "must survive".chars().collect();
        app.ports[0].draft_cursor = app.ports[0].draft.len();
        let (commands, mut received) = mpsc::channel(1);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert_eq!(
            app.current().draft.iter().collect::<String>(),
            "must survive"
        );
        assert!(app.current().history.is_empty());
        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn alt_enter_sends_matching_agent_cooperative_write_without_acquire() {
        let mut app = ready_app_with_foreign_control();
        let agent = app
            .current()
            .snapshot
            .control
            .as_ref()
            .unwrap()
            .owner
            .clone();
        let mut run = agent_run("diagnose boot");
        run.owner = agent;
        let run_id = run.id;
        app.ports[0].snapshot.active_run = Some(run);
        app.ports[0].snapshot.effective_write_eol = Some("\r\n".into());
        app.ports[0].draft = "show version".chars().collect();
        app.ports[0].draft_cursor = app.ports[0].draft.len();
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT), &commands);

        let NetworkCommand::Send { message, .. } = received.try_recv().expect("cooperative write")
        else {
            panic!("expected cooperative write")
        };
        let ClientMessage::Write {
            control_id,
            fence,
            data,
            operation_id,
            expected_run_id,
            pacing,
            cooperative,
            ..
        } = message
        else {
            panic!("expected Write, not AcquireControl")
        };
        assert!(cooperative);
        assert_eq!(control_id, Uuid::nil());
        assert_eq!(fence, 0);
        assert_eq!(data, b"show version\r\n");
        assert!(operation_id.is_some());
        assert_eq!(expected_run_id, Some(run_id));
        assert_eq!(pacing, None);
        assert!(received.try_recv().is_err());
        assert!(app.current().draft.is_empty());
        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(app.pending_requests.values().any(|request| matches!(
            request,
            PendingRequest::Write {
                port,
                cooperative: true,
                ..
            } if port == "COM3"
        )));
    }

    #[test]
    fn rejected_cooperative_write_preserves_ordinary_queue_and_acquire() {
        let mut app = ready_app_with_foreign_control();
        let agent = app
            .current()
            .snapshot
            .control
            .as_ref()
            .unwrap()
            .owner
            .clone();
        let mut run = agent_run("lease boundary");
        run.owner = agent;
        app.ports[0].snapshot.active_run = Some(run);

        let queued_operation = Uuid::new_v4();
        app.pending_writes.insert(
            "COM3".into(),
            VecDeque::from([PendingWrite {
                data: b"ordinary queued\r".to_vec(),
                operation_id: Some(queued_operation),
                kind: PendingWriteKind::Line,
            }]),
        );
        let acquire_request = Uuid::new_v4();
        app.pending_requests.insert(
            acquire_request,
            PendingRequest::Acquire {
                port: "COM3".into(),
                mode: ControlMode::Queue,
            },
        );
        app.queued_controls.insert(
            "COM3".into(),
            QueuedControl {
                _position: 2,
                since: Instant::now(),
            },
        );
        app.ports[0].draft = "cooperative at expiry".chars().collect();
        app.ports[0].draft_cursor = app.ports[0].draft.len();
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT), &commands);
        let NetworkCommand::Send { message, .. } = received.try_recv().expect("cooperative write")
        else {
            panic!("expected cooperative write")
        };
        let ClientMessage::Write {
            request_id,
            cooperative,
            ..
        } = message
        else {
            panic!("expected cooperative Write")
        };
        assert!(cooperative);

        app.handle_server_message(
            ServerMessage::Error {
                request_id: Some(request_id),
                code: serial_protocol::ErrorCode::ControlRequired,
                message: "Agent lease expired before cooperative write".into(),
                retryable: true,
            },
            &commands,
        );

        let queue = app
            .pending_writes
            .get("COM3")
            .expect("ordinary queue must survive");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].data, b"ordinary queued\r");
        assert_eq!(queue[0].operation_id, Some(queued_operation));
        assert!(matches!(
            app.pending_requests.get(&acquire_request),
            Some(PendingRequest::Acquire { port, .. }) if port == "COM3"
        ));
        assert_eq!(app.queued_controls["COM3"]._position, 2);
        assert!(!app.pending_requests.contains_key(&request_id));
    }

    #[test]
    fn agent_run_hint_tracks_empty_draft_and_active_run_without_sticky_dismissal() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.ports[0].snapshot.active_run = Some(agent_run("FIRST_AGENT_TASK"));
        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render Agent hint");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("FIRST_AGENT_TASK"));
        let layout = app.layout.expect("layout");
        let (commands, _) = mpsc::channel(1);

        // Focusing an empty editor must not permanently dismiss the hint.
        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: layout.input_area.x + 1,
                row: layout.input_area.y + 1,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render focused empty Agent hint");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("FIRST_AGENT_TASK"));

        // Draft content alone hides the placeholder.
        app.handle_key(
            KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
            &commands,
        );
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render non-empty draft");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("FIRST_AGENT_TASK"));
        assert!(rendered.contains("> x"));

        // Deleting the draft back to empty restores the same Run immediately.
        app.handle_key(
            KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE),
            &commands,
        );
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render restored Agent hint");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(app.current().draft.is_empty());
        assert!(rendered.contains("FIRST_AGENT_TASK"));

        // Once the Run is no longer active there is no placeholder to show.
        app.ports[0].snapshot.active_run = None;
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render after Agent Run ended");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("FIRST_AGENT_TASK"));
    }

    #[test]
    fn configuration_menu_shortcut_navigates_and_reload_uses_command_channel() {
        let mut app = App::new(vec![snapshot()], None);
        let (menu_commands, mut menu_received) = mpsc::channel(2);
        app.menu_commands = Some(menu_commands);
        let (network_commands, _) = mpsc::channel(1);

        app.handle_key(
            KeyEvent::new(KeyCode::Char(']'), KeyModifiers::CONTROL),
            &network_commands,
        );
        app.handle_key(
            KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE),
            &network_commands,
        );

        assert!(matches!(
            menu_received.try_recv(),
            Ok(MenuIoCommand::Reload)
        ));
        assert!(matches!(
            app.menu.as_ref().map(|menu| menu.page),
            Some(MenuPage::Root)
        ));
        app.handle_key(
            KeyEvent::new(KeyCode::Down, KeyModifiers::NONE),
            &network_commands,
        );
        app.handle_key(
            KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            &network_commands,
        );
        assert!(matches!(
            app.menu.as_ref().map(|menu| menu.page),
            Some(MenuPage::CreateProfiles)
        ));
        app.handle_key(
            KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
            &network_commands,
        );
        assert!(matches!(
            app.menu.as_ref().map(|menu| menu.page),
            Some(MenuPage::Root)
        ));
        app.handle_key(
            KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
            &network_commands,
        );
        assert!(app.menu.is_none());
    }

    #[test]
    fn profile_shortcut_opens_profile_page_and_remains_discoverable_in_help() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let mut app = App::new(vec![snapshot()], None);
        let (menu_commands, mut menu_received) = mpsc::channel(2);
        app.menu_commands = Some(menu_commands);
        let (network_commands, _) = mpsc::channel(1);

        app.handle_key(
            KeyEvent::new(KeyCode::Char(']'), KeyModifiers::CONTROL),
            &network_commands,
        );
        app.handle_key(
            KeyEvent::new(KeyCode::Char('o'), KeyModifiers::NONE),
            &network_commands,
        );

        assert!(matches!(
            menu_received.try_recv(),
            Ok(MenuIoCommand::Reload)
        ));
        let menu = app.menu.as_ref().expect("profile menu");
        assert_eq!(menu.page, MenuPage::Profiles);
        assert_eq!(menu.stack, vec![(MenuPage::Root, 0)]);
        assert!(!menu_rows(&app, menu).is_empty());
        let help = help_lines(&app)
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>();
        assert!(help.iter().any(|line| line.contains("Ctrl-] o")));
        assert!(help.iter().any(|line| line.contains("Ctrl-] h")));
    }

    #[test]
    fn current_model_profile_template_preserves_effective_behavior() {
        let mut current = snapshot();
        current.effective_shell_prompt = Some("dut# ".into());
        current.effective_uboot_prompt = Some("dut=> ".into());
        current.effective_write_eol = Some("\r\n".into());
        current.effective_echo = Some(EchoMode::Auto);
        current.effective_write_pacing = Some(WritePacing {
            chunk_size: 7,
            chunk_delay_ms: 13,
        });
        let view = SlotView::new(current);

        let cloned = current_model_profile_template(&view);
        assert_eq!(cloned.name, "");
        assert_eq!(cloned.shell_prompt.as_deref(), Some("dut# "));
        assert_eq!(cloned.uboot_prompt.as_deref(), Some("dut=> "));
        assert_eq!(cloned.write_eol.as_deref(), Some("\r\n"));
        assert_eq!(cloned.echo, Some(EchoMode::Auto));
        assert_eq!(cloned.write_chunk_size, Some(7));
        assert_eq!(cloned.write_chunk_delay_ms, Some(13));
    }

    #[test]
    fn queued_line_command_is_visible_in_the_input_title() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.pending_writes.insert(
            "COM3".into(),
            VecDeque::from([PendingWrite {
                data: b"reboot\r".to_vec(),
                operation_id: Some(Uuid::new_v4()),
                kind: PendingWriteKind::Line,
            }]),
        );

        let title = input_title(&app, InputMode::Line);
        assert!(title.contains("QUEUED 1"));
        assert!(title.contains("reboot"));
        assert!(title.contains("Ctrl-] d/e/c"));
    }

    #[test]
    fn queue_selection_can_restore_the_middle_operation() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let operations = ["first", "second", "third"];
        let queue = app.pending_writes.entry("COM3".into()).or_default();
        for command in operations {
            append_pending_write(
                queue,
                format!("{command}\r").as_bytes(),
                Some(Uuid::new_v4()),
                PendingWriteKind::Line,
            );
        }

        app.open_queue_selection();
        let (commands, _) = mpsc::channel(1);
        app.handle_queue_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), &commands);
        app.handle_queue_key(
            KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE),
            &commands,
        );

        assert_eq!(app.current().draft.iter().collect::<String>(), "second");
        assert!(app.queue_selection.is_none());
        let remaining = queued_line_operations(app.pending_writes.get("COM3").unwrap())
            .into_iter()
            .map(|operation| String::from_utf8(operation.data).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec!["first\r", "third\r"]);
    }

    #[test]
    fn queue_selector_is_reachable_from_the_prefix_shortcut() {
        let mut app = App::new(vec![snapshot()], None);
        let queue = app.pending_writes.entry("COM3".into()).or_default();
        for command in ["first", "second"] {
            append_pending_write(
                queue,
                format!("{command}\r").as_bytes(),
                Some(Uuid::new_v4()),
                PendingWriteKind::Line,
            );
        }
        let (commands, _) = mpsc::channel(1);

        app.handle_key(
            KeyEvent::new(KeyCode::Char(']'), KeyModifiers::CONTROL),
            &commands,
        );
        app.handle_key(
            KeyEvent::new(KeyCode::Char('u'), KeyModifiers::NONE),
            &commands,
        );

        assert_eq!(app.focus, PaneFocus::Queue);
        assert_eq!(
            app.queue_selection
                .as_ref()
                .map(|selection| selection.selected),
            Some(0)
        );
    }

    #[test]
    fn sending_operation_is_locked_but_a_later_operation_is_editable() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let sending_id = Uuid::new_v4();
        let queued_id = Uuid::new_v4();
        app.pending_writes.insert(
            "COM3".into(),
            VecDeque::from([
                PendingWrite {
                    data: b"sending\r".to_vec(),
                    operation_id: Some(sending_id),
                    kind: PendingWriteKind::Line,
                },
                PendingWrite {
                    data: b"editable\r".to_vec(),
                    operation_id: Some(queued_id),
                    kind: PendingWriteKind::Line,
                },
            ]),
        );
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Write {
                port: "COM3".into(),
                operation_id: Some(sending_id),
                cooperative: false,
            },
        );
        let (commands, _) = mpsc::channel(1);

        app.remove_queued_line_operation(0, true, &commands);
        assert!(app.current().draft.is_empty());
        assert_eq!(
            queued_line_count(app.pending_writes.get("COM3").unwrap()),
            2
        );
        assert_eq!(app.status, tr("st.queue.already.sending"));

        app.remove_queued_line_operation(1, true, &commands);
        assert_eq!(app.current().draft.iter().collect::<String>(), "editable");
        assert_eq!(
            queued_line_count(app.pending_writes.get("COM3").unwrap()),
            1
        );
    }

    #[test]
    fn real_flush_keeps_single_chunk_card_visible_and_locked_until_ack() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(4);

        assert!(app.request_write(&commands, b"REAL_INFLIGHT\r".to_vec(), Some(Uuid::new_v4()),));
        let (request_id, data, _) = take_write(&mut received);
        assert_eq!(data, b"REAL_INFLIGHT\r");
        assert_eq!(queued_line_count(&app.pending_writes["COM3"]), 1);

        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render in-flight queue card");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("REAL_INFLIGHT"));
        assert!(!rendered.contains("SENDING (locked)"));
        assert!(queue_cards(&app, 98)[0].sending);

        app.handle_result(
            request_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("COM3"));
        assert!(!app.inflight_writes.contains_key("COM3"));
    }

    #[test]
    fn control_tick_retries_queue_after_outbound_channel_backpressure() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(1);
        commands.try_send(NetworkCommand::Shutdown).unwrap();

        assert!(app.request_write(
            &commands,
            b"retry after full\r".to_vec(),
            Some(Uuid::new_v4()),
        ));
        assert_eq!(queued_line_count(&app.pending_writes["COM3"]), 1);
        assert!(app.inflight_writes.is_empty());
        assert!(matches!(received.try_recv(), Ok(NetworkCommand::Shutdown)));

        app.maintain_controls(&commands);

        let (_, data, _) = take_write(&mut received);
        assert_eq!(data, b"retry after full\r");
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Write { port, cooperative: false, .. } if port == "COM3")
        ));
        assert_eq!(queued_line_count(&app.pending_writes["COM3"]), 1);
    }

    #[test]
    fn queue_panel_renders_operations_oldest_first() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let queue = app.pending_writes.entry("COM3".into()).or_default();
        for command in ["FIRST_QUEUED", "SECOND_QUEUED", "THIRD_QUEUED"] {
            append_pending_write(
                queue,
                format!("{command}\r").as_bytes(),
                Some(Uuid::new_v4()),
                PendingWriteKind::Line,
            );
        }
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render queue panel");

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        let first = rendered.find("FIRST_QUEUED").unwrap();
        let second = rendered.find("SECOND_QUEUED").unwrap();
        let third = rendered.find("THIRD_QUEUED").unwrap();
        assert!(first < second && second < third);
    }

    #[test]
    fn queue_cards_keep_long_ascii_on_one_numbered_summary_row() {
        let mut app = App::new(vec![snapshot()], None);
        let command = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        append_pending_write(
            app.pending_writes.entry("COM3".into()).or_default(),
            format!("{command}\r").as_bytes(),
            Some(Uuid::new_v4()),
            PendingWriteKind::Line,
        );

        let cards = queue_cards(&app, 14);

        assert_eq!(cards.len(), 1);
        assert_eq!(cards[0].header, "1.");
        assert!(cards[0].command.ends_with('…'));
        assert!(UnicodeWidthStr::width(cards[0].command.as_str()) <= 9);
        assert_eq!(
            queued_line_operations(&app.pending_writes["COM3"])[0].data,
            format!("{command}\r").as_bytes()
        );
    }

    #[test]
    fn queue_cards_truncate_cjk_by_display_width() {
        let mut app = App::new(vec![snapshot()], None);
        let command = "中文样机命令参数甲乙丙丁戊己庚辛";
        append_pending_write(
            app.pending_writes.entry("COM3".into()).or_default(),
            format!("{command}\r").as_bytes(),
            Some(Uuid::new_v4()),
            PendingWriteKind::Line,
        );

        let cards = queue_cards(&app, 12);

        assert!(cards[0].command.ends_with('…'));
        assert!(UnicodeWidthStr::width(cards[0].command.as_str()) <= 7);
    }

    #[test]
    fn short_queue_viewport_keeps_selected_command_on_one_row() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let command = ["A"; 30]
            .into_iter()
            .chain(["B"; 30])
            .chain(["C"; 30])
            .chain(["D"; 30])
            .chain(["E"; 30])
            .chain(["F"; 30])
            .collect::<String>();
        append_pending_write(
            app.pending_writes.entry("COM3".into()).or_default(),
            format!("{command}\r").as_bytes(),
            Some(Uuid::new_v4()),
            PendingWriteKind::Line,
        );
        app.open_queue_selection();
        let (commands, _) = mpsc::channel(1);
        let backend = TestBackend::new(32, 16);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let first_page = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(first_page.contains("AAAAAAAA"));
        assert!(first_page.contains("▶ 1."));
        assert!(!first_page.contains("text rows"));

        app.handle_queue_key(
            KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
            &commands,
        );
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let unchanged = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(unchanged.contains("AAAAAAAA"));
        assert!(!unchanged.contains("text rows"));
    }

    #[test]
    fn display_column_selection_handles_wrapped_rows_and_cjk() {
        let selection = TextSelection {
            rows: vec![Line::from("  abc"), Line::from("中def")],
            plain_rows: vec!["  abc".into(), "中def".into()],
            anchor: SelectionPoint { row: 0, column: 2 },
            head: SelectionPoint { row: 1, column: 2 },
            word_selected: false,
            completed: false,
            last_activity: Instant::now(),
        };
        assert_eq!(selection.selected_text(), "abc\n中d");

        let reversed = TextSelection {
            head: selection.anchor,
            anchor: selection.head,
            ..selection
        };
        assert_eq!(reversed.selected_text(), "abc\n中d");
    }

    #[test]
    fn double_click_accepts_different_cells_inside_the_same_word() {
        let now = Instant::now();
        let rows = vec!["  serial-platform ready".to_owned()];
        let previous = OutputClick {
            point: SelectionPoint { row: 0, column: 3 },
            at: now - Duration::from_millis(100),
        };
        let current = SelectionPoint { row: 0, column: 12 };

        assert!(output_clicks_form_double_click(
            previous, current, &rows, now
        ));
        assert_eq!(
            word_selection_points(&rows, current),
            Some((
                SelectionPoint { row: 0, column: 2 },
                SelectionPoint { row: 0, column: 16 },
            ))
        );
        assert!(!output_clicks_form_double_click(
            previous,
            SelectionPoint { row: 0, column: 19 },
            &rows,
            now
        ));
    }

    #[test]
    fn double_click_word_selection_copies_the_whole_token() {
        let mut app = App::new(vec![snapshot()], None);
        app.clipboard_copy = accept_clipboard_copy;
        app.ports[0].push_line(stream_row(1, Direction::Rx, "abcdef"), true);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let layout = app.layout.expect("draw records console layout");
        let (commands, _) = mpsc::channel(1);

        for column in [layout.output_inner.x + 2, layout.output_inner.x + 5] {
            app.handle_terminal_event(
                Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Down(MouseButton::Left),
                    column,
                    row: layout.output_inner.y,
                    modifiers: KeyModifiers::NONE,
                }),
                &commands,
            );
            app.handle_terminal_event(
                Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Up(MouseButton::Left),
                    column,
                    row: layout.output_inner.y,
                    modifiers: KeyModifiers::NONE,
                }),
                &commands,
            );
        }

        assert!(
            app.selection
                .as_ref()
                .is_some_and(|selection| selection.completed)
        );
        assert_eq!(app.selection_copy.as_deref(), Some("abcdef"));
    }

    #[test]
    fn mouse_wheel_pages_agent_history_without_mouse_focus_routing() {
        let mut current = snapshot();
        let run = agent_run("滚轮巡检");
        current.active_run = Some(run.clone());
        let epoch = current.daemon_epoch;
        let mut app = App::new(vec![current], None);
        for seq in 0..20 {
            app.ports[0].push_line(stream_row(seq, Direction::Rx, "row"), true);
        }
        for seq in 21..27 {
            let mut tx = event(EventKind::Tx, Direction::Tx, seq, b"command\r");
            tx.daemon_epoch = epoch;
            tx.actor = Some(run.owner.clone());
            tx.run_id = Some(run.id);
            tx.operation_id = Some(Uuid::new_v4());
            tx.metadata.insert(
                "command_description".into(),
                serde_json::json!(format!("command-{seq}")),
            );
            app.ports[0].push_event(tx, true);
        }
        let (commands, _) = mpsc::channel(1);

        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: 1,
                row: 1,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );

        assert_eq!(app.focus, PaneFocus::RunHistory);
        assert_eq!(app.current().selected_run_command_index(), Some(1));
        assert_eq!(app.current().scroll_from_bottom, 0);
    }

    #[test]
    fn completed_selection_stays_visibly_highlighted_and_remains_copyable() {
        let mut app = App::new(vec![snapshot()], None);
        app.clipboard_copy = record_clipboard_copy;
        TEST_CLIPBOARD.lock().expect("test clipboard lock").clear();
        app.ports[0].push_line(stream_row(1, Direction::Rx, "abcdef"), true);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");
        let layout = app.layout.expect("draw records console layout");
        let (commands, _) = mpsc::channel(1);

        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: layout.output_inner.x + 2,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: layout.output_inner.x + 5,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Up(MouseButton::Left),
                column: layout.output_inner.x + 5,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );

        assert_eq!(app.focus, PaneFocus::Input);
        assert!(
            app.selection
                .as_ref()
                .is_some_and(|selection| selection.completed)
        );
        assert_eq!(app.selection_copy.as_deref(), Some("abcd"));
        assert_eq!(
            TEST_CLIPBOARD
                .lock()
                .expect("test clipboard lock")
                .as_slice(),
            ["abcd"]
        );

        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render completed selection");
        let selected_cells = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .filter(|cell| cell.modifier.contains(Modifier::REVERSED))
            .count();
        assert!(
            selected_cells >= 4,
            "selected text must remain visibly reversed"
        );
        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Right),
                column: layout.output_inner.x + 2,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        assert_eq!(
            TEST_CLIPBOARD
                .lock()
                .expect("test clipboard lock")
                .as_slice(),
            ["abcd", "abcd"]
        );
        assert!(app.selection.is_none());
        assert!(app.selection_copy.is_none());

        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: layout.input_area.x + 1,
                row: layout.input_area.y + 1,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        assert_eq!(app.focus, PaneFocus::Input);
        assert!(app.selection.is_none());
        assert!(app.selection_copy.is_none());
    }

    #[test]
    fn stale_mouse_drag_finishes_without_pinning_output_forever() {
        let mut app = App::new(vec![snapshot()], None);
        app.clipboard_copy = record_clipboard_copy;
        app.ports[0].push_line(stream_row(1, Direction::Rx, "abcdef"), true);
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).expect("test terminal");
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render TUI");
        let layout = app.layout.expect("draw records console layout");
        let (commands, _) = mpsc::channel(1);

        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: layout.output_inner.x + 2,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        app.handle_terminal_event(
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::Drag(MouseButton::Left),
                column: layout.output_inner.x + 5,
                row: layout.output_inner.y,
                modifiers: KeyModifiers::NONE,
            }),
            &commands,
        );
        let last_activity = app.selection.as_ref().expect("active drag").last_activity;

        assert!(app.expire_mouse_selection(last_activity + MOUSE_SELECTION_TIMEOUT));
        assert!(
            app.selection
                .as_ref()
                .is_some_and(|selection| selection.completed)
        );
        assert_eq!(app.selection_copy.as_deref(), Some("abcd"));
    }

    #[test]
    fn closed_network_event_channel_is_disabled_after_one_observation() {
        let mut app = App::new(vec![snapshot()], None);
        app.transport_connected = true;
        app.hello_accepted = true;
        app.connection_generation = Some(7);
        let (commands, _) = mpsc::channel(1);

        assert!(!handle_network_channel_event(&mut app, None, &commands));
        assert!(!app.transport_connected);
        assert!(!app.hello_accepted);
        assert!(app.connection_generation.is_none());
        assert!(matches!(
            app.ports[0].subscription,
            SubscriptionPhase::Disconnected
        ));
        assert!(app.dirty);
    }
}
