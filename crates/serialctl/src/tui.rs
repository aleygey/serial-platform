use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Write},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
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
    Actor, ClientMessage, CommandResult, ControlLease, ControlMode, DataBits, DeviceModel,
    DeviceModelListResponse, DeviceProfile, EchoMode, EventKind, FlowControl, LoggingState,
    ModelConfirmationMethod, Parity, ResolvedDeviceSettings, ResolvedTransportSettings, RunInfo,
    RunStatus, ServerMessage, SessionState, SetSlotDeviceModelRequest, SlotModelBinding,
    SlotSnapshot, StopBits, TargetActivity, TimelineEvent, TransportProfile, TriggerInfo,
    TriggerStatus, WireFrame, apply_transport_profile,
};
use tokio::sync::mpsc;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    config::LoadedConfig,
    display::{
        DisplayLine, RunBoundary, TerminalStreamParser, error_code_label, gap_line,
        gap_reason_label, highlight_spans, pad_display, role_label, safe_inline,
        trigger_status_label,
    },
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
const ACTIVE_WINDOW_NS: i64 = 5_000_000_000;
const MOUSE_SELECTION_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_RUN_HISTORY_PER_SLOT: usize = 20;
const MAX_COMMANDS_PER_RUN: usize = 64;
const MAX_RUN_COMMAND_BYTES: usize = 4 * 1024;
const RUN_SIDEBAR_MIN_TERMINAL_WIDTH: u16 = 110;
const RUN_SIDEBAR_MIN_WIDTH: u16 = 32;
const RUN_SIDEBAR_MAX_WIDTH: u16 = 44;

type ClipboardCopyFn = fn(&str) -> Result<()>;

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
    Output,
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
}

/// A paused viewport is an immutable set of already wrapped terminal rows.
/// Live serial events continue to update the underlying Slot, but they cannot
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
        self.anchor != self.head
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
        slot_id: String,
        mode: ControlMode,
    },
    Renew {
        slot_id: String,
    },
    Release {
        slot_id: String,
    },
    CancelAcquire {
        slot_id: String,
    },
    Write {
        slot_id: String,
        operation_id: Option<Uuid>,
        cooperative: bool,
    },
}

impl PendingRequest {
    fn slot_id(&self) -> &str {
        match self {
            Self::Acquire { slot_id, .. }
            | Self::Renew { slot_id }
            | Self::Release { slot_id }
            | Self::CancelAcquire { slot_id }
            | Self::Write { slot_id, .. } => slot_id,
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
    fn label(&self) -> String {
        match self {
            Self::Disconnected => tr("phase.off").into(),
            Self::Attaching => tr("phase.attach").into(),
            Self::Replaying {
                from_seq,
                through_seq,
            } => trf(
                "phase.replay",
                &[&from_seq.to_string(), &through_seq.to_string()],
            ),
            Self::Ready { head_seq } => trf("phase.live", &[&head_seq.to_string()]),
            Self::Lagged { from_seq, to_seq } => trf(
                "phase.lagged",
                &[&from_seq.to_string(), &to_seq.to_string()],
            ),
        }
    }

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
struct RunCommandRecord {
    operation_id: Option<Uuid>,
    first_seq: u64,
    last_seq: u64,
    description: Option<String>,
    actor_label: Option<String>,
    data: Vec<u8>,
    partial: bool,
    truncated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RunCommandKey {
    run_id: Uuid,
    first_seq: u64,
}

impl RunCommandRecord {
    fn from_event(event: &TimelineEvent) -> Self {
        let description = event
            .metadata
            .get("command_description")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        let mut data = event.data.clone();
        let partial = event
            .metadata
            .get("partial")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let truncated = data.len() > MAX_RUN_COMMAND_BYTES;
        data.truncate(MAX_RUN_COMMAND_BYTES);
        Self {
            operation_id: event.operation_id,
            first_seq: event.seq,
            last_seq: event.seq,
            description,
            actor_label: event.actor.as_ref().map(|actor| actor.label.clone()),
            data,
            partial,
            truncated,
        }
    }

    fn append_event(&mut self, event: &TimelineEvent) {
        self.last_seq = self.last_seq.max(event.seq);
        if self.description.is_none() {
            self.description = event
                .metadata
                .get("command_description")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
        }
        let available = MAX_RUN_COMMAND_BYTES.saturating_sub(self.data.len());
        let append = available.min(event.data.len());
        self.data.extend_from_slice(&event.data[..append]);
        self.partial |= event
            .metadata
            .get("partial")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        self.truncated |= append < event.data.len();
    }
}

#[derive(Debug, Clone)]
struct RunHistoryEntry {
    id: Uuid,
    label: String,
    owner_label: Option<String>,
    status: RunStatus,
    start_seq: u64,
    end_seq: Option<u64>,
    abort_reason: Option<String>,
    commands: VecDeque<RunCommandRecord>,
}

impl RunHistoryEntry {
    fn from_run(run: &RunInfo) -> Self {
        Self {
            id: run.id,
            label: run.label.clone(),
            owner_label: Some(run.owner.label.clone()),
            status: run.status,
            start_seq: run.start_seq,
            end_seq: run.end_seq,
            abort_reason: None,
            commands: VecDeque::new(),
        }
    }

    fn update_from_run(&mut self, run: &RunInfo) {
        self.label.clone_from(&run.label);
        self.owner_label = Some(run.owner.label.clone());
        self.status = run.status;
        self.start_seq = run.start_seq;
        self.end_seq = run.end_seq;
    }

    /// Returns whether an older described command had to be evicted.
    fn append_command(&mut self, event: &TimelineEvent) -> bool {
        if let Some(operation_id) = event.operation_id
            && let Some(command) = self
                .commands
                .iter_mut()
                .find(|command| command.operation_id == Some(operation_id))
        {
            command.append_event(event);
            return false;
        }
        let evicted = self.commands.len() == MAX_COMMANDS_PER_RUN;
        if self.commands.len() == MAX_COMMANDS_PER_RUN {
            self.commands.pop_front();
        }
        self.commands.push_back(RunCommandRecord::from_event(event));
        evicted
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
    /// Visual-row offset within `scroll_snapshot`. When no snapshot exists,
    /// this retains the legacy logical-row offset used by a few recovery paths
    /// and tests, but all interactive scrolling creates a snapshot first.
    scroll_from_bottom: usize,
    unseen: usize,
    last_epoch: Option<Uuid>,
    last_seq: u64,
    /// Reconcile confirmed TX bytes with an exact subsequent RX echo while
    /// building the terminal projection. The durable RX/TX audit events stay
    /// separate, and this applies equally to LINE and RAW input.
    merge_echo: bool,
    draft: Vec<char>,
    draft_cursor: usize,
    mode: InputMode,
    history: Vec<String>,
    history_cursor: Option<usize>,
    history_search: Option<HistorySearch>,
    completion: Option<Completion>,
    last_manual_activity: Option<Instant>,
    /// Bounded, structured Run projection for the operator sidebar. Timeline
    /// rows remain the durable audit source; this projection only groups their
    /// lifecycle and confirmed TX events for quick review.
    run_history: VecDeque<RunHistoryEntry>,
    /// The sidebar is a bounded recent projection, not an assertion that the
    /// durable journal has been read from sequence one. Initial attach uses a
    /// tail, and any gap or local eviction keeps this conservative marker set.
    run_history_limited: bool,
    /// `None` follows the newest described Agent command. A concrete key
    /// preserves an explicit operator selection when newer commands arrive.
    selected_run_command: Option<RunCommandKey>,
    expanded_run_command: Option<RunCommandKey>,
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
            merge_echo: true,
            draft: Vec::new(),
            draft_cursor: 0,
            mode: InputMode::Line,
            history: Vec::new(),
            history_cursor: None,
            history_search: None,
            completion: None,
            last_manual_activity: None,
            run_history: VecDeque::new(),
            run_history_limited: true,
            selected_run_command: None,
            expanded_run_command: None,
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
            owner_label: event.actor.as_ref().map(|actor| actor.label.clone()),
            status: RunStatus::Active,
            start_seq: event.seq,
            end_seq: None,
            abort_reason: None,
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
                        entry.abort_reason = event
                            .metadata
                            .get("reason")
                            .and_then(serde_json::Value::as_str)
                            .map(safe_inline)
                            .filter(|reason| !reason.is_empty());
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
                let Some(entry) = self.ensure_run_for_event(event, run_id) else {
                    return;
                };
                if entry.append_command(event) {
                    self.run_history_limited = true;
                }
            }
            _ => {}
        }
    }

    fn run_command_keys(&self) -> Vec<RunCommandKey> {
        self.run_history_newest_first()
            .into_iter()
            .flat_map(|run| {
                run.commands.iter().rev().map(|command| RunCommandKey {
                    run_id: run.id,
                    first_seq: command.first_seq,
                })
            })
            .collect()
    }

    fn run_history_newest_first(&self) -> Vec<&RunHistoryEntry> {
        let active_run_id = self.snapshot.active_run.as_ref().and_then(|run| {
            (run.owner.kind == serial_protocol::ActorKind::Agent).then_some(run.id)
        });
        let mut runs = self.run_history.iter().collect::<Vec<_>>();
        runs.sort_by(|left, right| {
            let left_active = Some(left.id) == active_run_id;
            let right_active = Some(right.id) == active_run_id;
            right_active
                .cmp(&left_active)
                .then_with(|| right.start_seq.cmp(&left.start_seq))
        });
        runs
    }

    fn selected_run_command_index(&self) -> Option<usize> {
        let keys = self.run_command_keys();
        (!keys.is_empty()).then(|| {
            self.selected_run_command
                .and_then(|selected| keys.iter().position(|key| *key == selected))
                .unwrap_or(0)
        })
    }

    fn select_run_command_index(&mut self, index: usize) {
        let key = self.run_command_keys().get(index).copied();
        self.selected_run_command = key;
        self.expanded_run_command = None;
        self.run_detail_scroll = 0;
    }

    fn selected_run_command_key(&self) -> Option<RunCommandKey> {
        let index = self.selected_run_command_index()?;
        self.run_command_keys().get(index).copied()
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
        if self.last_epoch.is_some() && self.last_epoch != Some(event.daemon_epoch) {
            self.reset_stream();
            self.clear_run_history();
            self.push_line(gap_line(event.seq, tr("st.epoch.changed")), selected);
        }
        if event.kind == EventKind::Gap {
            self.run_history_limited = true;
        }
        self.observe_run_history(&event);
        self.last_epoch = Some(event.daemon_epoch);
        self.last_seq = event.seq;
        // `Auto` is deliberately lossless: until the platform has an
        // authoritative echo probe it behaves like local projection without
        // suppression. Only an explicit `On` may discard an exact RX echo.
        let reconcile_echo = self.merge_echo && self.effective_echo() == EchoMode::On;
        self.stream.set_echo_reconciliation(reconcile_echo);
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
        self.snapshot
            .effective_echo
            .unwrap_or(self.snapshot.config.settings.echo)
    }

    fn effective_write_eol(&self) -> &str {
        self.snapshot
            .effective_write_eol
            .as_deref()
            .unwrap_or(&self.snapshot.config.settings.write_eol)
    }

    fn has_effective_device_settings(&self) -> bool {
        self.snapshot.effective_shell_prompt.is_some()
            || self.snapshot.effective_uboot_prompt.is_some()
            || self.snapshot.effective_write_eol.is_some()
            || self.snapshot.effective_echo.is_some()
    }

    fn effective_shell_prompt(&self) -> Option<&str> {
        if self.has_effective_device_settings() {
            self.snapshot.effective_shell_prompt.as_deref()
        } else {
            self.snapshot.config.settings.shell_prompt.as_deref()
        }
    }

    fn effective_uboot_prompt(&self) -> Option<&str> {
        if self.has_effective_device_settings() {
            self.snapshot.effective_uboot_prompt.as_deref()
        } else {
            self.snapshot.config.settings.uboot_prompt.as_deref()
        }
    }
}

struct PendingPaste {
    slot_id: String,
    bytes: Vec<u8>,
    raw: bool,
}

#[derive(Debug, Clone, Copy)]
struct QueuedControl {
    position: usize,
    since: Instant,
}

#[derive(Debug, Clone)]
struct QueueSelection {
    slot_id: String,
    selected: usize,
    detail_scroll: usize,
}

#[derive(Clone)]
struct MenuCatalog {
    auth_required: bool,
    slots: Vec<SlotSnapshot>,
    transport_profiles: Vec<TransportProfile>,
    device_profiles: Vec<DeviceProfile>,
    models: Vec<DeviceModel>,
    model_bindings: Vec<SlotModelBinding>,
    model_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MenuPage {
    Root,
    Profiles,
    TransportProfiles,
    DeviceProfiles,
    Models,
    ModelParents,
    SerialSettings,
    Help,
}

struct MenuState {
    page: MenuPage,
    selected: usize,
    stack: Vec<(MenuPage, usize)>,
    catalog: Option<MenuCatalog>,
    expanded_models: HashSet<String>,
    prompt: Option<MenuPrompt>,
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
            expanded_models: HashSet::new(),
            prompt: None,
            help_scroll: 0,
            busy: false,
            message: tr("menu.loading").into(),
        }
    }

    fn push(&mut self, page: MenuPage) {
        self.stack.push((self.page, self.selected));
        self.page = page;
        self.selected = 0;
        self.help_scroll = 0;
    }

    fn back(&mut self) -> bool {
        if let Some((page, selected)) = self.stack.pop() {
            self.page = page;
            self.selected = selected;
            true
        } else {
            false
        }
    }
}

struct MenuPrompt {
    title: String,
    value: Vec<char>,
    cursor: usize,
    secret: bool,
    purpose: MenuPromptPurpose,
}

enum MenuPromptPurpose {
    Admin(MenuAdminMutation),
    TransportName {
        slot_id: String,
        profile: TransportProfile,
    },
    DeviceName {
        slot_id: String,
        profile: DeviceProfile,
    },
    ModelName {
        slot_id: String,
        parent_id: Option<String>,
    },
}

enum MenuAdminMutation {
    BindTransport {
        slot_id: String,
        profile_name: String,
    },
    BindDevice {
        slot_id: String,
        profile_name: Option<String>,
    },
    CreateTransportAndBind {
        slot_id: String,
        profile: TransportProfile,
    },
    CreateDeviceAndBind {
        slot_id: String,
        profile: DeviceProfile,
    },
}

enum MenuIoCommand {
    Reload,
    Admin {
        token: Option<String>,
        mutation: MenuAdminMutation,
    },
    BindModel {
        slot_id: String,
        model_id: String,
        expected_revision: u64,
        expected_current: Option<String>,
    },
    CreateAndBindModel {
        slot_id: String,
        model_id: String,
        name: String,
        parent_id: Option<String>,
        expected_revision: u64,
        expected_current: Option<String>,
    },
}

#[derive(Clone)]
enum MenuSuccess {
    Loaded,
    TransportBound(String),
    DeviceBound(Option<String>),
    TransportCreated(String),
    DeviceCreated(String),
    ModelBound(String),
    ModelCreated(String),
}

enum MenuIoEvent {
    Completed {
        catalog: MenuCatalog,
        success: MenuSuccess,
    },
    Failed(String),
}

#[derive(Clone, Copy)]
enum TransportPreset {
    Baud(u32),
    EightNOne,
    EightEOne,
    EightOOne,
    EightNTwo,
    FlowNone,
    FlowHardware,
    ToggleDtr,
    ToggleRts,
    ToggleAutoOpen,
}

const TRANSPORT_PRESETS: &[TransportPreset] = &[
    TransportPreset::Baud(9_600),
    TransportPreset::Baud(57_600),
    TransportPreset::Baud(115_200),
    TransportPreset::Baud(230_400),
    TransportPreset::Baud(921_600),
    TransportPreset::EightNOne,
    TransportPreset::EightEOne,
    TransportPreset::EightOOne,
    TransportPreset::EightNTwo,
    TransportPreset::FlowNone,
    TransportPreset::FlowHardware,
    TransportPreset::ToggleDtr,
    TransportPreset::ToggleRts,
    TransportPreset::ToggleAutoOpen,
];

#[derive(Clone, Copy)]
enum DevicePreset {
    Echo(EchoMode),
    Eol(&'static str),
}

const DEVICE_PRESETS: &[DevicePreset] = &[
    DevicePreset::Echo(EchoMode::On),
    DevicePreset::Echo(EchoMode::Off),
    DevicePreset::Echo(EchoMode::Auto),
    DevicePreset::Eol("\r"),
    DevicePreset::Eol("\n"),
    DevicePreset::Eol("\r\n"),
];

impl DevicePreset {
    fn apply(self, profile: &mut DeviceProfile) {
        match self {
            Self::Echo(echo) => profile.echo = Some(echo),
            Self::Eol(eol) => profile.write_eol = Some(eol.into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Echo(EchoMode::On) => tr("menu.device.echo.on"),
            Self::Echo(EchoMode::Off) => tr("menu.device.echo.off"),
            Self::Echo(EchoMode::Auto) => tr("menu.device.echo.auto"),
            Self::Eol("\r") => tr("menu.device.eol.cr"),
            Self::Eol("\n") => tr("menu.device.eol.lf"),
            Self::Eol("\r\n") => tr("menu.device.eol.crlf"),
            Self::Eol(_) => tr("menu.device.eol.custom"),
        }
    }
}

impl TransportPreset {
    fn apply(self, profile: &mut TransportProfile) {
        match self {
            Self::Baud(baud_rate) => profile.baud_rate = baud_rate,
            Self::EightNOne => {
                profile.data_bits = DataBits::Eight;
                profile.parity = Parity::None;
                profile.stop_bits = StopBits::One;
            }
            Self::EightEOne => {
                profile.data_bits = DataBits::Eight;
                profile.parity = Parity::Even;
                profile.stop_bits = StopBits::One;
            }
            Self::EightOOne => {
                profile.data_bits = DataBits::Eight;
                profile.parity = Parity::Odd;
                profile.stop_bits = StopBits::One;
            }
            Self::EightNTwo => {
                profile.data_bits = DataBits::Eight;
                profile.parity = Parity::None;
                profile.stop_bits = StopBits::Two;
            }
            Self::FlowNone => profile.flow_control = FlowControl::None,
            Self::FlowHardware => profile.flow_control = FlowControl::Hardware,
            Self::ToggleDtr => profile.dtr = !profile.dtr,
            Self::ToggleRts => profile.rts = !profile.rts,
            Self::ToggleAutoOpen => profile.auto_open = !profile.auto_open,
        }
    }

    fn label(self) -> String {
        match self {
            Self::Baud(value) => trf("menu.serial.baud", &[&value.to_string()]),
            Self::EightNOne => tr("menu.serial.8n1").into(),
            Self::EightEOne => tr("menu.serial.8e1").into(),
            Self::EightOOne => tr("menu.serial.8o1").into(),
            Self::EightNTwo => tr("menu.serial.8n2").into(),
            Self::FlowNone => tr("menu.serial.flow.none").into(),
            Self::FlowHardware => tr("menu.serial.flow.hardware").into(),
            Self::ToggleDtr => tr("menu.serial.dtr").into(),
            Self::ToggleRts => tr("menu.serial.rts").into(),
            Self::ToggleAutoOpen => tr("menu.serial.auto").into(),
        }
    }
}

#[derive(Clone, Copy)]
struct ModelTreeRow {
    index: usize,
    depth: usize,
}

fn visible_model_rows(models: &[DeviceModel], expanded: &HashSet<String>) -> Vec<ModelTreeRow> {
    fn visit(
        models: &[DeviceModel],
        parent: Option<&str>,
        depth: usize,
        expanded: &HashSet<String>,
        rows: &mut Vec<ModelTreeRow>,
    ) {
        for (index, model) in models
            .iter()
            .enumerate()
            .filter(|(_, model)| model.parent_id.as_deref() == parent)
        {
            rows.push(ModelTreeRow { index, depth });
            if expanded.contains(&model.id) {
                visit(models, Some(&model.id), depth + 1, expanded, rows);
            }
        }
    }

    let mut rows = Vec::new();
    visit(models, None, 0, expanded, &mut rows);
    rows
}

fn all_model_rows(models: &[DeviceModel]) -> Vec<ModelTreeRow> {
    let expanded = models
        .iter()
        .map(|model| model.id.clone())
        .collect::<HashSet<_>>();
    visible_model_rows(models, &expanded)
}

fn model_has_children(models: &[DeviceModel], model_id: &str) -> bool {
    models
        .iter()
        .any(|model| model.parent_id.as_deref() == Some(model_id))
}

fn slot_model_binding<'a>(catalog: &'a MenuCatalog, slot_id: &str) -> Option<&'a SlotModelBinding> {
    catalog
        .model_bindings
        .iter()
        .find(|binding| binding.slot_id == slot_id)
}

fn normalize_model_id(name: &str, models: &[DeviceModel]) -> String {
    let base = name
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned();
    let base = if base.is_empty() {
        "model".to_owned()
    } else {
        base
    };
    if !models.iter().any(|model| model.id == base) {
        return base;
    }
    for suffix in 2..=9_999 {
        let candidate = format!("{base}-{suffix}");
        if !models.iter().any(|model| model.id == candidate) {
            return candidate;
        }
    }
    format!("{base}-{}", Uuid::new_v4().simple())
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
    if let Some(profile) = catalog
        .transport_profiles
        .iter()
        .find(|profile| profile.name == view.snapshot.config.profile)
    {
        return profile.clone();
    }
    let settings = view.snapshot.effective_transport.unwrap_or_else(|| {
        serial_protocol::resolve_transport_settings(&view.snapshot.config.settings, None)
    });
    TransportProfile {
        name: view.snapshot.config.profile.clone(),
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

fn current_device_template(view: &SlotView) -> DeviceProfile {
    let pacing = view
        .snapshot
        .effective_write_pacing
        .unwrap_or(serial_protocol::WritePacing {
            chunk_size: view.snapshot.config.settings.write_chunk_size,
            chunk_delay_ms: view.snapshot.config.settings.write_chunk_delay_ms,
        });
    DeviceProfile {
        name: String::new(),
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
        MenuPage::Profiles => 2,
        MenuPage::TransportProfiles => menu
            .catalog
            .as_ref()
            .map_or(0, |catalog| catalog.transport_profiles.len() + 1),
        MenuPage::DeviceProfiles => menu.catalog.as_ref().map_or(0, |catalog| {
            catalog.device_profiles.len() + 2 + DEVICE_PRESETS.len()
        }),
        MenuPage::Models => menu.catalog.as_ref().map_or(0, |catalog| {
            visible_model_rows(&catalog.models, &menu.expanded_models).len() + 2
        }),
        MenuPage::ModelParents => menu
            .catalog
            .as_ref()
            .map_or(0, |catalog| all_model_rows(&catalog.models).len()),
        MenuPage::SerialSettings => TRANSPORT_PRESETS.len(),
        MenuPage::Help => 0,
    }
}

fn menu_success_message(success: &MenuSuccess) -> String {
    match success {
        MenuSuccess::Loaded => tr("menu.loaded").into(),
        MenuSuccess::TransportBound(name) => trf("menu.transport.bound", &[name]),
        MenuSuccess::DeviceBound(Some(name)) => trf("menu.device.bound", &[name]),
        MenuSuccess::DeviceBound(None) => tr("menu.device.generic.bound").into(),
        MenuSuccess::TransportCreated(name) => trf("menu.transport.created", &[name]),
        MenuSuccess::DeviceCreated(name) => trf("menu.device.created", &[name]),
        MenuSuccess::ModelBound(name) => trf("menu.model.bound", &[name]),
        MenuSuccess::ModelCreated(name) => trf("menu.model.created", &[name]),
    }
}

struct App {
    slots: Vec<SlotView>,
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
    authenticated: bool,
    connection_generation: Option<u64>,
    actor: Option<Actor>,
    status: String,
    pending_paste: Option<PendingPaste>,
    pending_writes: HashMap<String, VecDeque<PendingWrite>>,
    /// Current physical chunk within the first queued operation for each Slot.
    /// The complete operation stays in `pending_writes` until every chunk is
    /// acknowledged, so its UI card never disappears or shrinks in flight.
    inflight_writes: HashMap<String, InFlightWrite>,
    pending_requests: HashMap<Uuid, PendingRequest>,
    queued_controls: HashMap<String, QueuedControl>,
    queue_selection: Option<QueueSelection>,
    menu: Option<MenuState>,
    menu_commands: Option<mpsc::Sender<MenuIoCommand>>,
    uncertain_write_outcomes: usize,
    human_idle_release: Duration,
    mouse_capture: bool,
    run_panel_visible: bool,
    focus: PaneFocus,
    layout: Option<ConsoleLayout>,
    /// Only the currently active left-button drag keeps a stable visual
    /// snapshot. Once the drag finishes, the selected text moves to
    /// `selection_copy` so live output resumes immediately.
    selection: Option<TextSelection>,
    selection_copy: Option<String>,
    clipboard_copy: ClipboardCopyFn,
    config: Option<LoadedConfig>,
    should_quit: bool,
    dirty: bool,
}

impl App {
    fn new(slots: Vec<SlotSnapshot>, initial_slot: Option<&str>) -> Self {
        let slots = slots.into_iter().map(SlotView::new).collect::<Vec<_>>();
        let selected = initial_slot
            .and_then(|requested| {
                slots.iter().position(|slot| {
                    slot.snapshot.config.id == requested
                        || slot.snapshot.config.display_name == requested
                })
            })
            .unwrap_or(0);
        Self {
            slots,
            selected,
            prefix_pending: false,
            help_dismiss_prefix: false,
            help: false,
            help_scroll: 0,
            detailed_timeline: false,
            transport_connected: false,
            authenticated: false,
            connection_generation: None,
            actor: None,
            status: tr("st.connecting").into(),
            pending_paste: None,
            pending_writes: HashMap::new(),
            inflight_writes: HashMap::new(),
            pending_requests: HashMap::new(),
            queued_controls: HashMap::new(),
            queue_selection: None,
            menu: None,
            menu_commands: None,
            uncertain_write_outcomes: 0,
            human_idle_release: Duration::from_secs(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS),
            mouse_capture: true,
            run_panel_visible: true,
            focus: PaneFocus::Input,
            layout: None,
            selection: None,
            selection_copy: None,
            clipboard_copy: default_clipboard_copy,
            config: None,
            should_quit: false,
            dirty: true,
        }
    }

    fn current(&self) -> &SlotView {
        &self.slots[self.selected]
    }

    fn current_mut(&mut self) -> &mut SlotView {
        &mut self.slots[self.selected]
    }

    fn selected_slot_id(&self) -> String {
        self.current().snapshot.config.id.clone()
    }

    fn current_mode(&self) -> InputMode {
        self.current().mode
    }

    fn select(&mut self, index: usize) {
        if index < self.slots.len() {
            self.clear_text_selection();
            self.queue_selection = None;
            self.selected = index;
            self.current_mut().unseen = 0;
            let name = self.current().snapshot.config.display_name.clone();
            let port = self.current().snapshot.config.port.clone();
            self.status = trf("st.viewing", &[&name, &port]);
            self.dirty = true;
        }
    }

    fn handle_network(&mut self, event: NetworkEvent, commands: &mpsc::Sender<NetworkCommand>) {
        match event {
            NetworkEvent::TransportConnected { generation } => {
                self.transport_connected = true;
                self.authenticated = false;
                self.connection_generation = Some(generation);
                self.actor = None;
                for slot in &mut self.slots {
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
                self.authenticated = false;
                self.connection_generation = None;
                self.pending_requests.clear();
                self.pending_writes.clear();
                self.inflight_writes.clear();
                self.queued_controls.clear();
                self.pending_paste = None;
                for slot in &mut self.slots {
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
                role,
                protocol_version,
                ..
            } => {
                self.actor = Some(actor);
                self.authenticated = true;
                self.status = trf(
                    "st.welcome",
                    &[role_label(role), &protocol_version.to_string()],
                );
            }
            ServerMessage::Snapshot { slot } => {
                if let Some(index) = self
                    .slots
                    .iter()
                    .position(|view| view.snapshot.config.id == slot.config.id)
                {
                    let epoch_changed =
                        self.slots[index].snapshot.daemon_epoch != slot.daemon_epoch;
                    let generation_changed =
                        self.slots[index].snapshot.generation != slot.generation;
                    if epoch_changed || generation_changed {
                        self.invalidate_slot_pending(
                            &slot.config.id,
                            tr("st.session.changed.unsent"),
                        );
                        self.slots[index].reset_stream();
                    }
                    if epoch_changed {
                        self.slots[index].clear_run_history();
                    }
                    self.slots[index].snapshot = *slot;
                    self.slots[index].sync_trigger_projection(false);
                    self.slots[index].sync_active_run_history();
                    self.slots[index].subscription = SubscriptionPhase::Attaching;
                    if epoch_changed {
                        let selected = self.selected == index;
                        let seq = self.slots[index].snapshot.head_seq;
                        self.slots[index].push_gap(seq, tr("st.daemon.restarted"), selected);
                        self.slots[index].last_epoch =
                            Some(self.slots[index].snapshot.daemon_epoch);
                        self.slots[index].last_seq = 0;
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
                        Some(PendingRequest::Acquire { slot_id, .. })
                        | Some(PendingRequest::Write {
                            slot_id,
                            cooperative: false,
                            ..
                        }) => {
                            self.queued_controls.remove(&slot_id);
                            let discarded = self
                                .pending_writes
                                .remove(&slot_id)
                                .map_or(0, |writes| writes.len());
                            self.inflight_writes.remove(&slot_id);
                            if discarded > 0 {
                                discarded_suffix =
                                    trf("st.discarded.chunks", &[&slot_id, &discarded.to_string()]);
                            }
                        }
                        Some(PendingRequest::Write {
                            slot_id,
                            cooperative: true,
                            ..
                        }) => {
                            // Cooperative input never owns the queued Human
                            // suffix or its acquire request. A rejection (for
                            // example an Agent lease expiring at the boundary)
                            // ends only this one opportunistic write.
                            cooperative_slot = Some(slot_id);
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
                if let Some(slot_id) = cooperative_slot {
                    // A queue-mode acquire can be granted while the
                    // cooperative request is still in flight. That grant
                    // deliberately waits behind all writes; once this request
                    // is rejected, resume the untouched ordinary queue if the
                    // Human now owns the lease.
                    self.flush_pending_writes(&slot_id, commands);
                }
            }
            ServerMessage::Gap {
                slot_id,
                requested_after_seq,
                first_available_seq,
                head_seq,
                reason,
            } => {
                self.push_gap(
                    &slot_id,
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
                slot_id,
                from_seq,
                to_seq,
            } => {
                if let Some(index) = self.slot_index(&slot_id) {
                    self.slots[index].subscription = SubscriptionPhase::Lagged { from_seq, to_seq };
                }
                self.push_gap(
                    &slot_id,
                    to_seq,
                    trf("st.lagged", &[&from_seq.to_string(), &to_seq.to_string()]),
                );
            }
            ServerMessage::ReplayBegin {
                slot_id,
                from_seq,
                through_seq,
            } => {
                if let Some(index) = self.slot_index(&slot_id) {
                    self.slots[index].subscription = SubscriptionPhase::Replaying {
                        from_seq,
                        through_seq,
                    };
                }
                self.status = trf(
                    "st.replaying",
                    &[&slot_id, &from_seq.to_string(), &through_seq.to_string()],
                );
            }
            ServerMessage::Ready { slot_id, head_seq } => {
                if let Some(index) = self.slot_index(&slot_id) {
                    self.slots[index].subscription = SubscriptionPhase::Ready { head_seq };
                    if self.owns_control(index) {
                        self.flush_pending_writes(&slot_id, commands);
                    }
                }
                self.status = trf("st.live", &[&slot_id, &head_seq.to_string()]);
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
                if let Some(PendingRequest::Acquire { slot_id, mode }) = pending {
                    self.queued_controls.remove(&slot_id);
                    self.install_lease(&slot_id, lease);
                    self.status = match mode {
                        ControlMode::Queue => trf("st.granted", &[&slot_id]),
                        ControlMode::Takeover => trf("st.takeover.granted", &[&slot_id]),
                    };
                    self.flush_pending_writes(&slot_id, commands);
                }
            }
            CommandResult::ControlQueued { position } => {
                if let Some(PendingRequest::Acquire { slot_id, mode }) = pending {
                    self.queued_controls.insert(
                        slot_id.clone(),
                        QueuedControl {
                            position,
                            since: Instant::now(),
                        },
                    );
                    self.pending_requests
                        .insert(request_id, PendingRequest::Acquire { slot_id, mode });
                }
                self.status = trf("st.queued", &[&position.to_string()]);
            }
            CommandResult::ControlRenewed { lease } => {
                if let Some(PendingRequest::Renew { slot_id }) = pending {
                    self.install_lease(&slot_id, lease);
                }
            }
            CommandResult::ControlReleased => {
                if let Some(PendingRequest::Release { slot_id }) = pending {
                    if let Some(index) = self.slot_index(&slot_id) {
                        self.slots[index].snapshot.control = None;
                        self.slots[index].last_manual_activity = None;
                    }
                    self.status = trf("st.released", &[&slot_id]);
                }
            }
            CommandResult::AcquireCancelled { removed } => {
                if let Some(
                    PendingRequest::Acquire { slot_id, .. }
                    | PendingRequest::CancelAcquire { slot_id },
                ) = pending
                {
                    self.queued_controls.remove(&slot_id);
                    self.status = trf("st.acquire.cancelled", &[&slot_id]);
                    // The queued waiter can be promoted just before its
                    // directed cancellation is processed. If that happened,
                    // release the now-idle Human lease immediately instead of
                    // holding it until the idle timer expires.
                    if !removed
                        && self
                            .slot_index(&slot_id)
                            .is_some_and(|index| self.owns_control(index))
                        && !self.pending_writes.contains_key(&slot_id)
                        && let Some(index) = self.slot_index(&slot_id)
                        && let Some(lease) = self.slots[index].snapshot.control.clone()
                    {
                        self.release_slot_control(commands, slot_id, lease, false);
                    }
                }
            }
            CommandResult::WriteAccepted { event_seq } => {
                if let Some(PendingRequest::Write {
                    slot_id,
                    cooperative,
                    ..
                }) = pending
                {
                    self.status = trf("st.write.confirmed", &[&slot_id, &event_seq.to_string()]);
                    if !cooperative {
                        self.acknowledge_inflight_write(&slot_id);
                    }
                    self.flush_pending_writes(&slot_id, commands);
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
            CommandResult::HelloAccepted { actor, role } => {
                self.actor = Some(actor);
                self.authenticated = true;
                self.status = trf("st.authenticated", &[role_label(role)]);
            }
            CommandResult::Attached { slots } => {
                self.status = trf("st.watching", &[&slots.len().to_string()]);
            }
            CommandResult::Detached { slots } => {
                self.status = trf("st.detached", &[&slots.len().to_string()]);
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
        if let Some(index) = self.slot_index(&event.slot_id) {
            let slot_id = event.slot_id.clone();
            let selected = index == self.selected;
            if replay {
                self.slots[index].push_event(event, selected);
                return;
            }

            let generation_changed = self.slots[index].snapshot.generation != event.generation;
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
                .is_some_and(|current| current == self.slots[index].snapshot.config);
            let profile_only = event.kind == EventKind::SlotReconfigured
                && declared_profile_only
                && unchanged_config;
            let physical_reconfiguration =
                event.kind == EventKind::SlotReconfigured && !profile_only;
            if generation_changed
                || matches!(event.kind, EventKind::SerialClosed | EventKind::SlotRemoved)
                || physical_reconfiguration
            {
                self.invalidate_slot_pending(&slot_id, tr("st.session.changed.discarded"));
                self.slots[index].snapshot.active_trigger = None;
                self.slots[index].clear_trigger_projection();
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
            self.slots[index].push_event(event, selected);
            if self.slots[index].subscription.is_ready() && self.owns_control(index) {
                self.queued_controls.remove(&slot_id);
                self.pending_requests.retain(|_, request| {
                    !matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == &slot_id)
                });
                self.flush_pending_writes(&slot_id, commands);
            }
        }
    }

    fn apply_event_projection(&mut self, index: usize, event: &TimelineEvent) {
        let slot = &mut self.slots[index];
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
            EventKind::SlotReconfigured => {
                if let Some(config) = event
                    .metadata
                    .get("current")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                {
                    snapshot.config = config;
                }
                if let Some(effective) = event.metadata.get("effective").and_then(|value| {
                    serde_json::from_value::<ResolvedDeviceSettings>(value.clone()).ok()
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
            EventKind::SlotRemoved => {
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

    fn push_gap(&mut self, slot_id: &str, seq: u64, message: String) {
        if let Some(index) = self.slot_index(slot_id) {
            let selected = index == self.selected;
            self.slots[index].push_gap(seq, message, selected);
        }
    }

    fn slot_index(&self, slot_id: &str) -> Option<usize> {
        self.slots
            .iter()
            .position(|slot| slot.snapshot.config.id == slot_id)
    }

    fn all_slots_ready(&self) -> bool {
        !self.slots.is_empty() && self.slots.iter().all(|slot| slot.subscription.is_ready())
    }

    fn slot_ready(&self, index: usize) -> bool {
        self.slots[index].subscription.is_ready()
    }

    fn invalidate_slot_pending(&mut self, slot_id: &str, reason: &str) {
        let discarded_writes = self
            .pending_writes
            .remove(slot_id)
            .map_or(0, |writes| writes.len());
        self.inflight_writes.remove(slot_id);
        let before = self.pending_requests.len();
        self.pending_requests
            .retain(|_, request| request.slot_id() != slot_id);
        self.queued_controls.remove(slot_id);
        let discarded_requests = before.saturating_sub(self.pending_requests.len());
        if self
            .pending_paste
            .as_ref()
            .is_some_and(|paste| paste.slot_id == slot_id)
        {
            self.pending_paste = None;
        }
        if discarded_writes > 0 || discarded_requests > 0 {
            self.status = trf(
                "st.invalidated",
                &[
                    slot_id,
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
        self.slots[index]
            .snapshot
            .control
            .as_ref()
            .is_some_and(|lease| lease.owner.id == actor.id)
    }

    fn install_lease(&mut self, slot_id: &str, lease: ControlLease) {
        self.queued_controls.remove(slot_id);
        if let Some(index) = self.slot_index(slot_id) {
            self.slots[index].snapshot.control = Some(lease);
        }
    }

    fn send_message(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        message: ClientMessage,
        pending: Option<PendingRequest>,
    ) -> bool {
        if !self.transport_connected || !self.authenticated {
            self.status = tr("st.not.auth.queued").into();
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
        if !self.transport_connected || !self.authenticated {
            self.status = tr("st.not.auth2").into();
            return false;
        }
        if !self.slot_ready(self.selected) {
            self.status = trf("st.not.live", &[&self.selected_slot_id()]);
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

        let slot_id = self.selected_slot_id();
        let sent = self.send_message(
            commands,
            ClientMessage::Write {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.clone(),
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
                cooperative: true,
            },
            Some(PendingRequest::Write {
                slot_id,
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
        if !self.transport_connected || !self.authenticated {
            self.status = tr("st.not.auth2").into();
            return false;
        }
        if !self.slot_ready(self.selected) {
            self.status = trf("st.not.live", &[&self.selected_slot_id()]);
            return false;
        }
        let slot_id = self.selected_slot_id();
        let total_new_bytes = writes.iter().fold(0usize, |total, (write, _)| {
            total.saturating_add(write.len())
        });
        let previous_slot_writes = self
            .pending_writes
            .get(&slot_id)
            .cloned()
            .unwrap_or_default();
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
            .insert(slot_id.clone(), candidate_slot_writes);
        self.slots[self.selected].last_manual_activity = Some(Instant::now());

        if self.owns_control(self.selected) {
            let flushed = self.flush_pending_writes(&slot_id, commands);
            // A saturated outbound channel leaves the complete operation in
            // the visible local queue. Treat that as accepted local enqueue so
            // Enter may clear the draft without risking a later duplicate.
            return flushed || self.pending_writes.contains_key(&slot_id);
        }

        let acquire_already_pending = self.pending_requests.values().any(|request| {
            matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == &slot_id)
        });
        if !acquire_already_pending && !self.acquire_control(commands, ControlMode::Queue) {
            if previous_slot_writes.is_empty() {
                self.pending_writes.remove(&slot_id);
            } else {
                self.pending_writes
                    .insert(slot_id.clone(), previous_slot_writes);
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
        if !self.transport_connected || !self.authenticated || !self.slot_ready(self.selected) {
            self.status = tr("st.not.auth.live").into();
            return false;
        }
        let slot_id = self.selected_slot_id();
        let message = ClientMessage::AcquireControl {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
            mode,
            ttl_ms: CONTROL_TTL_MS,
        };
        if self.send_message(
            commands,
            message,
            Some(PendingRequest::Acquire {
                slot_id: slot_id.clone(),
                mode,
            }),
        ) {
            if mode == ControlMode::Takeover {
                self.slots[self.selected].last_manual_activity = Some(Instant::now());
            }
            self.status = match mode {
                ControlMode::Queue => trf("st.requesting.control", &[&slot_id]),
                ControlMode::Takeover => trf("st.requesting.takeover", &[&slot_id]),
            };
            true
        } else {
            false
        }
    }

    fn release_control(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        if !self.transport_connected || !self.authenticated || !self.slot_ready(self.selected) {
            self.status = tr("st.slot.not.live").into();
            return;
        }
        let slot_id = self.selected_slot_id();
        if !self.owns_control(self.selected) && self.has_queued_control(&slot_id) {
            self.cancel_queued_control(commands, &slot_id, tr("st.cancel.reason"));
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
        self.pending_writes.remove(&slot_id);
        self.inflight_writes.remove(&slot_id);
        self.release_slot_control(commands, slot_id, lease, false);
    }

    fn remove_last_queued_line(
        &mut self,
        restore_to_editor: bool,
        commands: &mpsc::Sender<NetworkCommand>,
    ) {
        let slot_id = self.selected_slot_id();
        let count = self
            .pending_writes
            .get(&slot_id)
            .map_or(0, |queue| queued_line_operations(queue).len());
        if count == 0 {
            self.status = if self
                .pending_writes
                .get(&slot_id)
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
        let slot_id = self.selected_slot_id();
        let Some(operation) = self.pending_writes.get(&slot_id).and_then(|queue| {
            queued_line_operations(queue)
                .into_iter()
                .nth(operation_index)
        }) else {
            self.status = tr("st.queue.none").into();
            return;
        };

        let sending = self.pending_requests.values().any(|request| match request {
            PendingRequest::Write {
                slot_id: pending_slot,
                operation_id,
                ..
            } if pending_slot == &slot_id => {
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
            .get_mut(&slot_id)
            .and_then(|queue| take_queued_line_operation(queue, operation_index))
            .map(|operation| operation.data)
        else {
            self.status = tr("st.queue.none").into();
            return;
        };
        let queue_empty = self
            .pending_writes
            .get(&slot_id)
            .is_some_and(VecDeque::is_empty);
        if queue_empty {
            self.pending_writes.remove(&slot_id);
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
            view.history_cursor = None;
            view.history_search = None;
            view.completion = None;
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
            self.status = tr("st.queue.restored").into();
        } else {
            self.status = tr("st.queue.deleted").into();
            self.normalize_queue_selection();
        }
        if !self.pending_writes.contains_key(&slot_id)
            && !self.owns_control(self.selected)
            && (self.queued_controls.contains_key(&slot_id)
                || self.pending_requests.values().any(
                    |request| matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == &slot_id),
                ))
        {
            self.cancel_queued_control(commands, &slot_id, tr("st.cancel.reason"));
        }
    }

    fn open_queue_selection(&mut self) {
        let slot_id = self.selected_slot_id();
        let count = self
            .pending_writes
            .get(&slot_id)
            .map_or(0, |queue| queued_line_operations(queue).len());
        if count == 0 {
            self.status = tr("st.queue.none").into();
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
            return;
        }
        self.queue_selection = Some(QueueSelection {
            slot_id,
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
            .get(&selection.slot_id)
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
        let slot_id = selection.slot_id.clone();
        let selected = selection.selected;
        let count = self
            .pending_writes
            .get(&slot_id)
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
        let count = self.current().run_command_keys().len();
        let selected = self.current().selected_run_command_index().unwrap_or(0);
        match key.code {
            KeyCode::Up if count > 0 => {
                self.current_mut()
                    .select_run_command_index(selected.saturating_sub(1));
            }
            KeyCode::Down if count > 0 => {
                self.current_mut()
                    .select_run_command_index((selected + 1).min(count - 1));
            }
            KeyCode::Home if count > 0 => self.current_mut().select_run_command_index(0),
            KeyCode::End if count > 0 => {
                self.current_mut().select_run_command_index(count - 1);
            }
            KeyCode::Enter | KeyCode::Right if count > 0 => {
                let selected_key = self.current().selected_run_command_key();
                let view = self.current_mut();
                if view.expanded_run_command == selected_key && key.code == KeyCode::Enter {
                    view.expanded_run_command = None;
                } else {
                    view.expanded_run_command = selected_key;
                }
                view.run_detail_scroll = 0;
            }
            KeyCode::Left if count > 0 => {
                self.current_mut().expanded_run_command = None;
                self.current_mut().run_detail_scroll = 0;
            }
            KeyCode::PageUp => {
                let scroll = self.current().run_detail_scroll.saturating_sub(5);
                self.current_mut().run_detail_scroll = scroll;
            }
            KeyCode::PageDown => {
                let scroll = self.current().run_detail_scroll.saturating_add(5);
                self.current_mut().run_detail_scroll = scroll;
            }
            KeyCode::Esc => {
                self.focus = PaneFocus::Input;
                self.status = tr("st.run.panel.left").into();
            }
            _ => {}
        }
    }

    fn has_queued_control(&self, slot_id: &str) -> bool {
        self.queued_controls.contains_key(slot_id)
            || self.pending_writes.contains_key(slot_id)
            || self.pending_requests.values().any(
                |request| matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == slot_id),
            )
    }

    fn cancel_queued_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        slot_id: &str,
        reason: &str,
    ) {
        let message = ClientMessage::CancelAcquire {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.to_owned(),
            // A queued waiter has no lease identity; seriald intentionally
            // matches it by authenticated actor and treats this field as
            // forward-compatible wire context.
            control_id: Uuid::nil(),
        };
        if self.send_message(
            commands,
            message,
            Some(PendingRequest::CancelAcquire {
                slot_id: slot_id.to_owned(),
            }),
        ) {
            self.pending_writes.remove(slot_id);
            self.inflight_writes.remove(slot_id);
            self.queued_controls.remove(slot_id);
            self.pending_requests.retain(|_, request| {
                !matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == slot_id)
            });
            if self
                .pending_paste
                .as_ref()
                .is_some_and(|paste| paste.slot_id == slot_id)
            {
                self.pending_paste = None;
            }
            self.status = trf("st.reconnect.reason", &[reason, slot_id]);
        }
    }

    fn release_slot_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        slot_id: String,
        lease: ControlLease,
        automatic: bool,
    ) {
        let release_pending = self.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Release { slot_id: pending } if pending == &slot_id),
        );
        if release_pending {
            return;
        }
        self.send_message(
            commands,
            ClientMessage::ReleaseControl {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.clone(),
                control_id: lease.id,
                fence: lease.fence,
            },
            Some(PendingRequest::Release {
                slot_id: slot_id.clone(),
            }),
        );
        if automatic {
            self.status = trf(
                "st.idle.release",
                &[&slot_id, &self.human_idle_release.as_secs().to_string()],
            );
        }
    }

    fn maintain_controls(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        if !self.transport_connected || !self.authenticated {
            return;
        }
        self.dirty = true;
        let idle_release = self.human_idle_release;
        let expired_queue = self.queued_controls.iter().find_map(|(slot_id, queued)| {
            let last_activity = self
                .slot_index(slot_id)
                .and_then(|index| self.slots[index].last_manual_activity);
            let idle = last_activity
                .map(|activity| activity.elapsed())
                .unwrap_or_else(|| queued.since.elapsed());
            (idle >= idle_release).then(|| slot_id.clone())
        });
        if let Some(slot_id) = expired_queue {
            self.cancel_queued_control(
                commands,
                &slot_id,
                &trf("st.queue.expired", &[&idle_release.as_secs().to_string()]),
            );
            return;
        }

        let actor_id = self.actor.as_ref().map(|actor| actor.id.clone());
        let leases = self
            .slots
            .iter()
            .filter_map(|slot| {
                if !slot.subscription.is_ready() {
                    return None;
                }
                let lease = slot.snapshot.control.as_ref()?;
                (Some(&lease.owner.id) == actor_id.as_ref())
                    .then(|| (slot.snapshot.config.id.clone(), lease.clone()))
            })
            .collect::<Vec<_>>();
        for (slot_id, lease) in leases {
            let index = self
                .slot_index(&slot_id)
                .expect("lease came from this Slot");
            // Retry a locally accepted operation whose previous outbound send
            // hit channel backpressure. Pending Write requests and active
            // Triggers are already guarded inside `flush_pending_writes`.
            self.flush_pending_writes(&slot_id, commands);
            let operation_pending = self.pending_writes.contains_key(&slot_id)
                || self.pending_requests.values().any(
                    |request| matches!(request, PendingRequest::Write { slot_id: pending, .. } if pending == &slot_id),
                );
            let recently_active = self.slots[index]
                .last_manual_activity
                .is_some_and(|activity| activity.elapsed() < idle_release);
            if !recently_active && !operation_pending {
                self.release_slot_control(commands, slot_id, lease, true);
                continue;
            }
            let already_pending = self.pending_requests.values().any(|request| {
                matches!(request, PendingRequest::Renew { slot_id: pending } if pending == &slot_id)
            });
            if already_pending {
                continue;
            }
            self.send_message(
                commands,
                ClientMessage::RenewControl {
                    request_id: Uuid::new_v4(),
                    slot_id: slot_id.clone(),
                    control_id: lease.id,
                    fence: lease.fence,
                    ttl_ms: CONTROL_TTL_MS,
                },
                Some(PendingRequest::Renew { slot_id }),
            );
        }
    }

    fn flush_pending_writes(
        &mut self,
        slot_id: &str,
        commands: &mpsc::Sender<NetworkCommand>,
    ) -> bool {
        let Some(index) = self.slot_index(slot_id) else {
            return false;
        };
        if !self.transport_connected
            || !self.authenticated
            || !self.slot_ready(index)
            || !self.owns_control(index)
            || self.slots[index].snapshot.active_trigger.is_some()
        {
            return true;
        }
        let write_already_pending = self.pending_requests.values().any(|request| {
            matches!(request, PendingRequest::Write { slot_id: pending, .. } if pending == slot_id)
        });
        if write_already_pending {
            return true;
        }
        let progress = self.inflight_writes.get(slot_id).copied().or_else(|| {
            self.pending_writes
                .get(slot_id)
                .and_then(|writes| writes.front())
                .map(|write| InFlightWrite {
                    operation_id: write.operation_id,
                    kind: write.kind,
                    chunk_index: 0,
                })
        });
        let write = progress.and_then(|progress| {
            self.pending_writes
                .get(slot_id)
                .and_then(|writes| writes.get(progress.chunk_index))
                .filter(|write| {
                    write.operation_id == progress.operation_id && write.kind == progress.kind
                })
                .cloned()
                .map(|write| (progress, write))
        });
        if let Some((progress, write)) = write {
            self.inflight_writes.insert(slot_id.to_owned(), progress);
            if !self.send_write_now(commands, slot_id, write.data, write.operation_id) {
                self.inflight_writes.remove(slot_id);
                return false;
            }
        }
        true
    }

    fn acknowledge_inflight_write(&mut self, slot_id: &str) {
        let Some(mut progress) = self.inflight_writes.get(slot_id).copied() else {
            return;
        };
        let next_index = progress.chunk_index.saturating_add(1);
        let same_operation_continues = self
            .pending_writes
            .get(slot_id)
            .and_then(|writes| writes.get(next_index))
            .is_some_and(|write| {
                write.operation_id == progress.operation_id && write.kind == progress.kind
            });
        if same_operation_continues {
            progress.chunk_index = next_index;
            self.inflight_writes.insert(slot_id.to_owned(), progress);
            return;
        }

        if let Some(writes) = self.pending_writes.get_mut(slot_id) {
            writes.drain(..next_index.min(writes.len()));
            if writes.is_empty() {
                self.pending_writes.remove(slot_id);
            }
        }
        self.inflight_writes.remove(slot_id);
    }

    fn send_write_now(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        slot_id: &str,
        data: Vec<u8>,
        operation_id: Option<Uuid>,
    ) -> bool {
        let Some(index) = self.slot_index(slot_id) else {
            return false;
        };
        let Some(lease) = self.slots[index].snapshot.control.clone() else {
            self.status = tr("st.write.disappeared").into();
            return false;
        };
        self.send_message(
            commands,
            ClientMessage::Write {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.to_string(),
                control_id: lease.id,
                fence: lease.fence,
                data,
                operation_id,
                // Human writes are governed by the fenced control lease, not
                // by an Agent Run boundary.
                expected_run_id: None,
                pacing: None,
                description: None,
                cooperative: false,
            },
            Some(PendingRequest::Write {
                slot_id: slot_id.to_string(),
                operation_id,
                cooperative: false,
            }),
        )
    }

    fn handle_terminal_event(&mut self, event: Event, commands: &mpsc::Sender<NetworkCommand>) {
        match event {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                self.handle_key(key, commands)
            }
            Event::Paste(value) => {
                self.clear_text_selection();
                if self.menu.is_some() {
                    self.handle_menu_paste(value);
                } else {
                    self.handle_paste(value, commands);
                }
            }
            Event::Mouse(mouse) => self.handle_mouse(mouse, commands),
            Event::Resize(_, _) => {
                self.clear_text_selection();
                for slot in &mut self.slots {
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
        let position = Position::new(mouse.column, mouse.row);
        if self
            .layout
            .and_then(|layout| layout.run_history_area)
            .is_some_and(|area| rect_contains(area, position))
        {
            self.clear_text_selection();
            self.focus = PaneFocus::RunHistory;
            match mouse.kind {
                MouseEventKind::ScrollUp => {
                    self.handle_run_history_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE));
                }
                MouseEventKind::ScrollDown => {
                    self.handle_run_history_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
                }
                _ => {}
            }
            self.dirty = true;
            return;
        }
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.clear_text_selection();
                self.scroll_up(3);
            }
            MouseEventKind::ScrollDown => {
                self.clear_text_selection();
                self.scroll_down(3);
            }
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
            self.queue_selection = None;
            self.focus = PaneFocus::Input;
            self.clear_text_selection();
            return;
        }
        if !rect_contains(layout.output_area, position) {
            return;
        }
        self.focus = PaneFocus::Output;
        self.clear_text_selection();
        if !rect_contains(layout.output_inner, position) {
            return;
        }
        let rows = visible_output_lines(self, layout.output_inner);
        let Some(point) = selection_point(layout.output_inner, position, rows.len()) else {
            return;
        };
        let plain_rows = rows.iter().map(line_plain_text).collect();
        self.selection = Some(TextSelection {
            rows,
            plain_rows,
            anchor: point,
            head: point,
            last_activity: Instant::now(),
        });
    }

    fn update_mouse_selection(&mut self, mouse: MouseEvent) {
        let (Some(layout), Some(selection)) = (self.layout, self.selection.as_mut()) else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if let Some(point) =
            selection_point_clamped(layout.output_inner, position, selection.plain_rows.len())
        {
            selection.head = point;
            selection.last_activity = Instant::now();
        }
    }

    fn finish_mouse_selection(&mut self, mouse: MouseEvent) {
        self.update_mouse_selection(mouse);
        self.complete_mouse_selection();
    }

    fn complete_mouse_selection(&mut self) -> bool {
        let Some(selection) = self.selection.take() else {
            return false;
        };
        if !selection.is_dragged() {
            self.selection_copy = None;
            return false;
        }
        let text = selection.selected_text();
        if text.is_empty() {
            self.selection_copy = None;
            return false;
        }
        let characters = text.chars().count().to_string();
        self.status = match (self.clipboard_copy)(&text) {
            Ok(()) => trf("st.selection.copied", &[&characters]),
            Err(error) => trf("st.clipboard.copy.failed", &[&error.to_string()]),
        };
        // Keep the payload so right-click remains an explicit retry/copy path
        // even after the automatic copy has resumed live output.
        self.selection_copy = Some(text);
        true
    }

    fn expire_mouse_selection(&mut self, now: Instant) -> bool {
        if self.selection.as_ref().is_some_and(|selection| {
            now.saturating_duration_since(selection.last_activity) >= MOUSE_SELECTION_TIMEOUT
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
        let Some(layout) = self.layout else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if rect_contains(layout.output_area, position) {
            self.focus = PaneFocus::Output;
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
        self.focus = PaneFocus::Input;
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
            KeyCode::Char('s' | 'S') => self.select((self.selected + 1) % self.slots.len()),
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
                for slot in &mut self.slots {
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
                self.status = tr("st.logs.hint").into();
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
                if value.is_empty() && self.current().active_agent_run().is_some() {
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
                    view.history_cursor = None;
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
            KeyCode::Up => self.history_previous(),
            KeyCode::Down => self.history_next(),
            KeyCode::PageUp => self.scroll_up(10),
            KeyCode::PageDown => self.scroll_down(10),
            KeyCode::Char('c' | 'C') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                {
                    let view = self.current_mut();
                    view.draft.clear();
                    view.draft_cursor = 0;
                    view.history_cursor = None;
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
                slot_id: self.selected_slot_id(),
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
        let Some(index) = self.slot_index(&paste.slot_id) else {
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
            self.status = trf("st.paste.queued", &[&paste.slot_id]);
        }
    }

    fn history_previous(&mut self) {
        let view = self.current_mut();
        if view.history.is_empty() {
            return;
        }
        let index = view
            .history_cursor
            .map(|index| index.saturating_sub(1))
            .unwrap_or(view.history.len() - 1);
        view.history_cursor = Some(index);
        view.draft = view.history[index].chars().collect();
        view.draft_cursor = view.draft.len();
    }

    fn history_next(&mut self) {
        let view = self.current_mut();
        let Some(index) = view.history_cursor else {
            return;
        };
        if index + 1 < view.history.len() {
            view.history_cursor = Some(index + 1);
            view.draft = view.history[index + 1].chars().collect();
        } else {
            view.history_cursor = None;
            view.draft.clear();
        }
        view.draft_cursor = view.draft.len();
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
        if let Some(prompt) = menu.prompt.take() {
            self.handle_menu_prompt_key(&mut menu, prompt, key);
            self.menu = Some(menu);
            return;
        }

        let mut keep_open = true;
        let count = menu_item_count(&menu);
        match key.code {
            KeyCode::Esc => {
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
            KeyCode::Right if menu.page == MenuPage::Models => {
                self.expand_selected_model(&mut menu, true);
            }
            KeyCode::Left if menu.page == MenuPage::Models => {
                self.expand_selected_model(&mut menu, false);
            }
            KeyCode::Char('b' | 'B') if menu.page == MenuPage::Models => {
                self.bind_selected_model(&mut menu);
            }
            KeyCode::Enter => self.activate_menu_item(&mut menu),
            _ => {}
        }
        if keep_open {
            let count = menu_item_count(&menu);
            menu.selected = menu.selected.min(count.saturating_sub(1));
            self.menu = Some(menu);
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
                let limit = if prompt.secret { 512 } else { 128 };
                if prompt.value.len() < limit && !character.is_control() {
                    prompt.value.insert(prompt.cursor, character);
                    prompt.cursor += 1;
                }
            }
            KeyCode::Enter => {
                let value = prompt.value.iter().collect::<String>();
                if prompt.secret {
                    if value.trim().is_empty() {
                        menu.message = tr("menu.admin.required").into();
                        menu.prompt = Some(prompt);
                        return;
                    }
                } else if !valid_menu_name(&value) {
                    menu.message = tr("menu.name.invalid").into();
                    menu.prompt = Some(prompt);
                    return;
                }
                match prompt.purpose {
                    MenuPromptPurpose::Admin(mutation) => {
                        self.submit_menu_command(
                            menu,
                            MenuIoCommand::Admin {
                                token: Some(value.trim().to_owned()),
                                mutation,
                            },
                        );
                    }
                    MenuPromptPurpose::TransportName {
                        slot_id,
                        mut profile,
                    } => {
                        profile.name = value;
                        self.begin_admin_prompt(
                            menu,
                            MenuAdminMutation::CreateTransportAndBind { slot_id, profile },
                        );
                    }
                    MenuPromptPurpose::DeviceName {
                        slot_id,
                        mut profile,
                    } => {
                        profile.name = value;
                        self.begin_admin_prompt(
                            menu,
                            MenuAdminMutation::CreateDeviceAndBind { slot_id, profile },
                        );
                    }
                    MenuPromptPurpose::ModelName { slot_id, parent_id } => {
                        let Some((model_id, expected_revision, expected_current)) =
                            menu.catalog.as_ref().map(|catalog| {
                                (
                                    normalize_model_id(&value, &catalog.models),
                                    catalog.model_revision,
                                    slot_model_binding(catalog, &slot_id)
                                        .map(|binding| binding.model_id.clone()),
                                )
                            })
                        else {
                            menu.message = tr("menu.catalog.unavailable").into();
                            return;
                        };
                        self.submit_menu_command(
                            menu,
                            MenuIoCommand::CreateAndBindModel {
                                slot_id,
                                model_id,
                                name: value,
                                parent_id,
                                expected_revision,
                                expected_current,
                            },
                        );
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
        let limit = if prompt.secret { 512 } else { 128 };
        for character in value.chars().filter(|character| !character.is_control()) {
            if prompt.value.len() >= limit {
                break;
            }
            prompt.value.insert(prompt.cursor, character);
            prompt.cursor += 1;
        }
        self.dirty = true;
    }

    fn begin_admin_prompt(&mut self, menu: &mut MenuState, mutation: MenuAdminMutation) {
        if menu
            .catalog
            .as_ref()
            .is_some_and(|catalog| !catalog.auth_required)
        {
            self.submit_menu_command(
                menu,
                MenuIoCommand::Admin {
                    token: None,
                    mutation,
                },
            );
            if menu.busy {
                menu.message = tr("menu.admin.not.required").into();
            }
            return;
        }
        menu.prompt = Some(MenuPrompt {
            title: tr("menu.prompt.admin").into(),
            value: Vec::new(),
            cursor: 0,
            secret: true,
            purpose: MenuPromptPurpose::Admin(mutation),
        });
        menu.message = tr("menu.admin.memory").into();
    }

    fn begin_transport_name_prompt(
        &self,
        menu: &mut MenuState,
        slot_id: String,
        profile: TransportProfile,
        suggested_name: String,
    ) {
        let value = suggested_name.chars().collect::<Vec<_>>();
        menu.prompt = Some(MenuPrompt {
            title: tr("menu.prompt.transport.name").into(),
            cursor: value.len(),
            value,
            secret: false,
            purpose: MenuPromptPurpose::TransportName { slot_id, profile },
        });
    }

    fn begin_device_name_prompt(
        &self,
        menu: &mut MenuState,
        slot_id: String,
        profile: DeviceProfile,
    ) {
        let value = "device-profile".chars().collect::<Vec<_>>();
        menu.prompt = Some(MenuPrompt {
            title: tr("menu.prompt.device.name").into(),
            cursor: value.len(),
            value,
            secret: false,
            purpose: MenuPromptPurpose::DeviceName { slot_id, profile },
        });
    }

    fn begin_model_name_prompt(
        &self,
        menu: &mut MenuState,
        slot_id: String,
        parent_id: Option<String>,
    ) {
        menu.prompt = Some(MenuPrompt {
            title: if parent_id.is_some() {
                tr("menu.prompt.model.child").into()
            } else {
                tr("menu.prompt.model.root").into()
            },
            value: Vec::new(),
            cursor: 0,
            secret: false,
            purpose: MenuPromptPurpose::ModelName { slot_id, parent_id },
        });
    }

    fn activate_menu_item(&mut self, menu: &mut MenuState) {
        if menu.busy && menu.page != MenuPage::Root {
            menu.message = tr("menu.busy").into();
            return;
        }
        match menu.page {
            MenuPage::Root => match menu.selected {
                0 => menu.push(MenuPage::Profiles),
                1 => menu.push(MenuPage::Models),
                2 => menu.push(MenuPage::SerialSettings),
                3 => menu.push(MenuPage::Help),
                _ => {}
            },
            MenuPage::Profiles => match menu.selected {
                0 => menu.push(MenuPage::TransportProfiles),
                1 => menu.push(MenuPage::DeviceProfiles),
                _ => {}
            },
            MenuPage::TransportProfiles => {
                let Some((profiles_len, profile_name)) = menu.catalog.as_ref().map(|catalog| {
                    (
                        catalog.transport_profiles.len(),
                        catalog
                            .transport_profiles
                            .get(menu.selected)
                            .map(|profile| profile.name.clone()),
                    )
                }) else {
                    menu.message = tr("menu.catalog.unavailable").into();
                    return;
                };
                let slot_id = self.selected_slot_id();
                if let Some(profile_name) = profile_name {
                    self.begin_admin_prompt(
                        menu,
                        MenuAdminMutation::BindTransport {
                            slot_id,
                            profile_name,
                        },
                    );
                } else if menu.selected == profiles_len {
                    self.begin_transport_name_prompt(
                        menu,
                        slot_id,
                        default_transport_profile(String::new()),
                        "uart-115200-8n1".into(),
                    );
                }
            }
            MenuPage::DeviceProfiles => {
                let Some((profiles_len, profile_name)) = menu.catalog.as_ref().map(|catalog| {
                    (
                        catalog.device_profiles.len(),
                        menu.selected
                            .checked_sub(1)
                            .and_then(|index| catalog.device_profiles.get(index))
                            .map(|profile| profile.name.clone()),
                    )
                }) else {
                    menu.message = tr("menu.catalog.unavailable").into();
                    return;
                };
                let slot_id = self.selected_slot_id();
                if menu.selected == 0 {
                    self.begin_admin_prompt(
                        menu,
                        MenuAdminMutation::BindDevice {
                            slot_id,
                            profile_name: None,
                        },
                    );
                } else if let Some(profile_name) = profile_name {
                    self.begin_admin_prompt(
                        menu,
                        MenuAdminMutation::BindDevice {
                            slot_id,
                            profile_name: Some(profile_name),
                        },
                    );
                } else if menu.selected == profiles_len + 1 {
                    self.begin_device_name_prompt(
                        menu,
                        slot_id,
                        current_device_template(self.current()),
                    );
                } else if let Some(preset) = menu
                    .selected
                    .checked_sub(profiles_len + 2)
                    .and_then(|index| DEVICE_PRESETS.get(index))
                    .copied()
                {
                    let mut profile = current_device_template(self.current());
                    preset.apply(&mut profile);
                    self.begin_device_name_prompt(menu, slot_id, profile);
                }
            }
            MenuPage::Models => self.activate_model_item(menu),
            MenuPage::ModelParents => {
                let Some(parent_id) = menu.catalog.as_ref().and_then(|catalog| {
                    let rows = all_model_rows(&catalog.models);
                    rows.get(menu.selected)
                        .map(|row| catalog.models[row.index].id.clone())
                }) else {
                    menu.message = tr("menu.catalog.unavailable").into();
                    return;
                };
                self.begin_model_name_prompt(menu, self.selected_slot_id(), Some(parent_id));
            }
            MenuPage::SerialSettings => {
                let Some(mut profile) = menu
                    .catalog
                    .as_ref()
                    .map(|catalog| current_transport_template(self.current(), catalog))
                else {
                    menu.message = tr("menu.catalog.unavailable").into();
                    return;
                };
                let Some(preset) = TRANSPORT_PRESETS.get(menu.selected).copied() else {
                    return;
                };
                preset.apply(&mut profile);
                let suggested_name =
                    format!("{}-custom", self.current().snapshot.config.profile.trim());
                profile.name.clear();
                self.begin_transport_name_prompt(
                    menu,
                    self.selected_slot_id(),
                    profile,
                    suggested_name,
                );
            }
            MenuPage::Help => {}
        }
    }

    fn activate_model_item(&mut self, menu: &mut MenuState) {
        if menu.selected == 0 {
            self.begin_model_name_prompt(menu, self.selected_slot_id(), None);
            return;
        }
        if menu.selected == 1 {
            if menu
                .catalog
                .as_ref()
                .is_none_or(|catalog| catalog.models.is_empty())
            {
                menu.message = tr("menu.model.no.parent").into();
            } else {
                menu.push(MenuPage::ModelParents);
            }
            return;
        }
        let Some(catalog) = menu.catalog.as_ref() else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        let rows = visible_model_rows(&catalog.models, &menu.expanded_models);
        let Some(row) = rows.get(menu.selected - 2).copied() else {
            return;
        };
        let model_id = catalog.models[row.index].id.clone();
        let has_children = model_has_children(&catalog.models, &model_id);
        if has_children {
            if !menu.expanded_models.remove(&model_id) {
                menu.expanded_models.insert(model_id);
            }
        } else {
            self.submit_model_binding(menu, model_id);
        }
    }

    fn expand_selected_model(&mut self, menu: &mut MenuState, expand: bool) {
        if menu.selected < 2 {
            return;
        }
        let Some(catalog) = menu.catalog.as_ref() else {
            return;
        };
        let rows = visible_model_rows(&catalog.models, &menu.expanded_models);
        let Some(row) = rows.get(menu.selected - 2) else {
            return;
        };
        let model_id = catalog.models[row.index].id.clone();
        let has_children = model_has_children(&catalog.models, &model_id);
        if expand && has_children {
            menu.expanded_models.insert(model_id);
        } else if !expand {
            menu.expanded_models.remove(&model_id);
        }
    }

    fn bind_selected_model(&mut self, menu: &mut MenuState) {
        if menu.selected < 2 {
            return;
        }
        let model_id = menu.catalog.as_ref().and_then(|catalog| {
            let rows = visible_model_rows(&catalog.models, &menu.expanded_models);
            rows.get(menu.selected - 2)
                .map(|row| catalog.models[row.index].id.clone())
        });
        if let Some(model_id) = model_id {
            self.submit_model_binding(menu, model_id);
        }
    }

    fn submit_model_binding(&mut self, menu: &mut MenuState, model_id: String) {
        let Some((expected_revision, expected_current)) = menu.catalog.as_ref().map(|catalog| {
            let slot_id = self.selected_slot_id();
            (
                catalog.model_revision,
                slot_model_binding(catalog, &slot_id).map(|binding| binding.model_id.clone()),
            )
        }) else {
            menu.message = tr("menu.catalog.unavailable").into();
            return;
        };
        let slot_id = self.selected_slot_id();
        self.submit_menu_command(
            menu,
            MenuIoCommand::BindModel {
                slot_id,
                model_id,
                expected_revision,
                expected_current,
            },
        );
    }

    fn handle_menu_io_event(&mut self, event: MenuIoEvent) {
        match event {
            MenuIoEvent::Completed { catalog, success } => {
                for fresh in &catalog.slots {
                    if let Some(view) = self
                        .slots
                        .iter_mut()
                        .find(|view| view.snapshot.config.id == fresh.config.id)
                    {
                        let configuration_changed = view.snapshot.config != fresh.config;
                        view.snapshot = fresh.clone();
                        view.sync_trigger_projection(false);
                        view.sync_active_run_history();
                        if configuration_changed {
                            view.follow();
                        }
                    }
                }
                let message = menu_success_message(&success);
                if let Some(menu) = self.menu.as_mut() {
                    menu.catalog = Some(catalog);
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

    /// Ctrl-] g: switch between English and Chinese at runtime and persist
    /// the choice to the client config on a best-effort basis.
    fn toggle_language(&mut self) {
        for slot in &mut self.slots {
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
        MenuIoCommand::Admin { token, mutation } => {
            // `None` is the explicit trusted-loopback path advertised by
            // health.auth_required=false. A legacy/authenticated daemon still
            // reaches this branch with the one-time masked credential.
            if token.as_deref().is_some_and(str::is_empty) {
                bail!(tr("menu.admin.required"));
            }
            let admin = ApiClient::new(api.endpoint().to_owned(), token)?;
            execute_admin_menu_mutation(&admin, mutation).await?
        }
        MenuIoCommand::BindModel {
            slot_id,
            model_id,
            expected_revision,
            expected_current,
        } => {
            api.set_slot_device_model(
                &slot_id,
                &SetSlotDeviceModelRequest {
                    model_id: Some(model_id.clone()),
                    create_if_missing: false,
                    update_existing: false,
                    name: None,
                    parent_id: None,
                    clear_parent: false,
                    aliases: Vec::new(),
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Human),
                    note: Some(tr("menu.model.confirm.note").into()),
                    source: "human:serialctl-tui".into(),
                    expected_revision: Some(expected_revision),
                    expected_current: Some(expected_current),
                },
            )
            .await?;
            MenuSuccess::ModelBound(model_id)
        }
        MenuIoCommand::CreateAndBindModel {
            slot_id,
            model_id,
            name,
            parent_id,
            expected_revision,
            expected_current,
        } => {
            api.set_slot_device_model(
                &slot_id,
                &SetSlotDeviceModelRequest {
                    model_id: Some(model_id.clone()),
                    create_if_missing: true,
                    update_existing: false,
                    name: Some(name),
                    parent_id,
                    clear_parent: false,
                    aliases: Vec::new(),
                    clear_aliases: false,
                    confirmation_method: Some(ModelConfirmationMethod::Human),
                    note: Some(tr("menu.model.confirm.note").into()),
                    source: "human:serialctl-tui".into(),
                    expected_revision: Some(expected_revision),
                    expected_current: Some(expected_current),
                },
            )
            .await?;
            MenuSuccess::ModelCreated(model_id)
        }
    };
    Ok((load_menu_catalog(api).await?, success))
}

async fn execute_admin_menu_mutation(
    admin: &ApiClient,
    mutation: MenuAdminMutation,
) -> Result<MenuSuccess> {
    match mutation {
        MenuAdminMutation::BindTransport {
            slot_id,
            profile_name,
        } => {
            bind_transport_profile(admin, &slot_id, &profile_name).await?;
            Ok(MenuSuccess::TransportBound(profile_name))
        }
        MenuAdminMutation::BindDevice {
            slot_id,
            profile_name,
        } => {
            bind_device_profile(admin, &slot_id, profile_name.as_deref()).await?;
            Ok(MenuSuccess::DeviceBound(profile_name))
        }
        MenuAdminMutation::CreateTransportAndBind { slot_id, profile } => {
            let profile_name = profile.name.clone();
            let mut catalog = admin.transport_profiles().await?;
            if catalog
                .profiles
                .iter()
                .any(|existing| existing.name == profile.name)
            {
                bail!(trf("menu.profile.exists", &[&profile.name]));
            }
            catalog.profiles.push(profile);
            admin
                .configure_transport_profiles(catalog.profiles, catalog.config_revision)
                .await?;
            bind_transport_profile(admin, &slot_id, &profile_name).await?;
            Ok(MenuSuccess::TransportCreated(profile_name))
        }
        MenuAdminMutation::CreateDeviceAndBind { slot_id, profile } => {
            let profile_name = profile.name.clone();
            let mut catalog = admin.device_profiles().await?;
            if catalog
                .profiles
                .iter()
                .any(|existing| existing.name == profile.name)
            {
                bail!(trf("menu.profile.exists", &[&profile.name]));
            }
            catalog.profiles.push(profile);
            admin
                .configure_device_profiles(catalog.profiles, catalog.config_revision)
                .await?;
            bind_device_profile(admin, &slot_id, Some(&profile_name)).await?;
            Ok(MenuSuccess::DeviceCreated(profile_name))
        }
    }
}

async fn bind_transport_profile(api: &ApiClient, slot_id: &str, profile_name: &str) -> Result<()> {
    let status = api.configuration_status().await?;
    let catalog = api.transport_profiles().await?;
    let profile = catalog
        .profiles
        .iter()
        .find(|profile| profile.name == profile_name)
        .with_context(|| trf("menu.transport.missing", &[profile_name]))?;
    let mut slots = status
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let slot = slots
        .iter_mut()
        .find(|slot| slot.id == slot_id)
        .with_context(|| trf("menu.slot.missing", &[slot_id]))?;
    slot.settings = apply_transport_profile(&slot.settings, Some(profile));
    slot.profile = profile.name.clone();
    api.configure_slots(slots, status.config_revision).await?;
    Ok(())
}

async fn bind_device_profile(
    api: &ApiClient,
    slot_id: &str,
    profile_name: Option<&str>,
) -> Result<()> {
    let status = api.configuration_status().await?;
    if let Some(profile_name) = profile_name {
        let catalog = api.device_profiles().await?;
        if !catalog
            .profiles
            .iter()
            .any(|profile| profile.name == profile_name)
        {
            bail!(trf("menu.device.missing", &[profile_name]));
        }
    }
    let mut slots = status
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let slot = slots
        .iter_mut()
        .find(|slot| slot.id == slot_id)
        .with_context(|| trf("menu.slot.missing", &[slot_id]))?;
    slot.device_profile = profile_name.map(ToOwned::to_owned);
    api.configure_slots(slots, status.config_revision).await?;
    Ok(())
}

async fn load_menu_catalog(api: &ApiClient) -> Result<MenuCatalog> {
    let (health, status, transport, device, models): (_, _, _, _, DeviceModelListResponse) = tokio::try_join!(
        api.health(),
        api.configuration_status(),
        api.transport_profiles(),
        api.device_profiles(),
        api.device_models(),
    )?;
    Ok(MenuCatalog {
        auth_required: health.auth_required,
        slots: status.slots,
        transport_profiles: transport.profiles,
        device_profiles: device.profiles,
        models: models.models,
        model_bindings: models.bindings,
        model_revision: models.config_revision,
    })
}

pub async fn run(
    api: ApiClient,
    mut loaded: LoadedConfig,
    initial_slot: Option<String>,
    endpoint: String,
    token: Option<String>,
) -> Result<()> {
    let status = api
        .status()
        .await
        .context("cannot load Slot status before opening the console")?;
    if status.slots.is_empty() {
        bail!(tr("st.no.slot"));
    }
    let slot_ids = status
        .slots
        .iter()
        .map(|slot| slot.config.id.clone())
        .collect::<Vec<_>>();
    let mut app = App::new(status.slots, initial_slot.as_deref());
    app.human_idle_release = Duration::from_secs(
        loaded
            .config
            .human_idle_release_seconds
            .unwrap_or(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS)
            .max(1),
    );
    app.config = Some(loaded.clone());
    let merge_echo = loaded.config.merge_echo.unwrap_or(true);
    for view in &mut app.slots {
        view.merge_echo = merge_echo;
    }
    app.mouse_capture = loaded.config.mouse_capture.unwrap_or(true);
    let mut menu_io = spawn_menu_io(api.clone());
    app.menu_commands = Some(menu_io.commands.clone());
    let mut network = ws::spawn(endpoint, token, slot_ids);

    let mut terminal = enter_terminal(app.mouse_capture)?;
    let _guard = TerminalGuard {
        mouse_capture: app.mouse_capture,
    };
    let result = run_loop(
        &mut terminal,
        &mut app,
        &network.commands,
        &mut network.events,
        &mut menu_io.events,
    )
    .await;
    let _ = network.commands.try_send(NetworkCommand::Shutdown);

    loaded.config.last_slot = Some(app.selected_slot_id());
    if let Err(error) = loaded.save() {
        tracing::warn!(%error, "failed to persist the last selected Slot");
    }
    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    commands: &mpsc::Sender<NetworkCommand>,
    network_events: &mut mpsc::Receiver<NetworkEvent>,
    menu_events: &mut mpsc::Receiver<MenuIoEvent>,
) -> Result<()> {
    let mut terminal_events = EventStream::new();
    let mut network_events_open = true;
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
            event = network_events.recv(), if network_events_open => {
                network_events_open = handle_network_channel_event(app, event, commands);
            },
            event = menu_events.recv() => {
                if let Some(event) = event {
                    app.handle_menu_io_event(event);
                }
            },
            _ = renew_tick.tick() => app.maintain_controls(commands),
            _ = activity_tick.tick() => {
                let now = Instant::now();
                let selection_changed = app.expire_mouse_selection(now);
                let mut trigger_changed = false;
                for slot in &mut app.slots {
                    trigger_changed |= slot.update_trigger_deadline(now);
                }
                if selection_changed || trigger_changed || app.slots.iter().any(|slot| {
                    slot.snapshot.target_activity == TargetActivity::Active
                        && slot.snapshot.session_state == SessionState::Online
                }) {
                    app.dirty = true;
                }
            },
            _ = render_tick.tick() => {
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
            app.authenticated = false;
            app.connection_generation = None;
            app.actor = None;
            for slot in &mut app.slots {
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

fn displayed_target_activity(snapshot: &SlotSnapshot) -> TargetActivity {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_nanos().min(i64::MAX as u128) as i64
        });
    displayed_target_activity_at(snapshot, now)
}

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

fn local_history_truncated_title() -> &'static str {
    tr("history.local.truncated.title")
}

struct QueueCard {
    operation_index: usize,
    sending: bool,
    header: String,
    body: Vec<String>,
}

fn queue_cards(app: &App, inner_width: u16) -> Vec<QueueCard> {
    let slot_id = app.selected_slot_id();
    let Some(queue) = app.pending_writes.get(&slot_id) else {
        return Vec::new();
    };
    let eol = app.current().effective_write_eol().as_bytes();
    queued_line_operations(queue)
        .into_iter()
        .enumerate()
        .map(|(operation_index, operation)| {
            let sending = app.pending_requests.values().any(|request| match request {
                PendingRequest::Write {
                    slot_id: pending_slot,
                    operation_id,
                    ..
                } if pending_slot == &slot_id => {
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
            let header = format!(
                "{}.{}",
                operation_index + 1,
                if sending { tr("ui.queue.sending") } else { "" }
            );
            QueueCard {
                operation_index,
                sending,
                header,
                body: wrap_queue_text(&command, inner_width.saturating_sub(2).max(1)),
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
    let area = frame.area();
    let wide_run_panel = app.run_panel_visible && area.width >= RUN_SIDEBAR_MIN_TERMINAL_WIDTH;
    let (console_area, run_history_area) = if wide_run_panel {
        let run_width = (area.width / 3).clamp(RUN_SIDEBAR_MIN_WIDTH, RUN_SIDEBAR_MAX_WIDTH);
        let columns =
            Layout::horizontal([Constraint::Min(60), Constraint::Length(run_width)]).split(area);
        (columns[0], Some(columns[1]))
    } else {
        (area, None)
    };

    let queue_visual_rows = queue_cards(app, console_area.width.saturating_sub(2))
        .iter()
        .map(|card| card.body.len().saturating_add(1))
        .sum::<usize>();
    // Preserve the existing four-row minimum output pane. On a normal
    // terminal every queued operation gets one row; very short terminals use
    // a bounded queue viewport that follows the selected operation.
    let max_queue_height = console_area.height.saturating_sub(12);
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
        Constraint::Length(1),
        Constraint::Length(queue_height),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .split(console_area);
    let output_area = chunks[1];
    let input_area = chunks[4];
    app.layout = Some(ConsoleLayout {
        output_area,
        output_inner: inset_border(output_area),
        input_area,
        run_history_area,
    });

    draw_tabs(frame, app, chunks[0]);
    draw_output(frame, app, chunks[1]);
    draw_status(frame, app, chunks[2]);
    if queue_height > 0 {
        draw_queue(frame, app, chunks[3]);
    }
    draw_input(frame, app, chunks[4]);
    draw_help_line(frame, app, chunks[5]);
    if let Some(run_history_area) = run_history_area {
        draw_run_history(frame, app, run_history_area);
    } else if app.run_panel_visible && app.focus == PaneFocus::RunHistory {
        let popup = centered_rect(
            area.width.saturating_sub(4).clamp(1, 72),
            area.height.saturating_sub(4).max(1),
            area,
        );
        frame.render_widget(Clear, popup);
        draw_run_history(frame, app, popup);
        if let Some(layout) = app.layout.as_mut() {
            layout.run_history_area = Some(popup);
        }
    }
    if app.help {
        draw_help(frame, app, area);
    }
    if let Some(menu) = app.menu.as_ref() {
        draw_menu(frame, app, menu, area);
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

fn target_activity_label(activity: TargetActivity) -> &'static str {
    match activity {
        TargetActivity::Active => tr("activity.active"),
        TargetActivity::Silent => tr("activity.silent"),
        TargetActivity::Unknown => tr("activity.unknown"),
    }
}

fn draw_tabs(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let titles = app
        .slots
        .iter()
        .enumerate()
        .map(|(index, slot)| {
            let state = session_state_label(slot.snapshot.session_state);
            let activity = target_activity_label(displayed_target_activity(&slot.snapshot));
            let unseen = if slot.unseen > 0 {
                format!(" +{}", slot.unseen)
            } else {
                String::new()
            };
            Line::from(format!(
                " {} {} {}/{} {}{} ",
                index + 1,
                safe_inline(&slot.snapshot.config.display_name),
                state,
                activity,
                slot.subscription.label(),
                unseen
            ))
        })
        .collect::<Vec<_>>();
    let connection = if !app.transport_connected {
        tr("conn.reconnecting")
    } else if !app.authenticated {
        tr("conn.authenticating")
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
    let view = app.current();
    let baud_rate = view
        .snapshot
        .effective_transport
        .map(|transport| transport.baud_rate)
        .unwrap_or(view.snapshot.config.settings.baud_rate);
    let title = format!(
        " {} · {} · {}{}{} ",
        safe_inline(&view.snapshot.config.display_name),
        safe_inline(&view.snapshot.config.port),
        trf("ui.output.baud", &[&baud_rate.to_string()]),
        if view.is_paused() {
            tr("ui.paused")
        } else {
            ""
        },
        if view.local_history_truncated {
            local_history_truncated_title()
        } else {
            ""
        }
    );
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .border_style(if app.focus == PaneFocus::Output {
            Style::default().fg(Color::Cyan)
        } else {
            Style::default()
        });
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
    let shell_prompt = view.effective_shell_prompt();
    let uboot_prompt = view.effective_uboot_prompt();
    let detailed_source_width = detailed_source_width(inner.width as usize);
    let logical_lines = truncation_line
        .iter()
        .chain(view.lines.iter().chain(view.pending_line.iter()))
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|entry| {
            timeline_line(
                entry,
                app.detailed_timeline,
                detailed_source_width,
                shell_prompt,
                uboot_prompt,
                inner.width as usize,
            )
        })
        .collect::<Vec<_>>();
    // Ratatui's stable public API does not expose the rendered line count for
    // a wrapped Paragraph. Pre-wrap this small logical suffix using terminal
    // character widths, then retain its visual tail. This mirrors a serial
    // terminal's character wrapping and guarantees that one long row cannot
    // push the newest prompt below the viewport.
    let visual_lines = logical_lines
        .into_iter()
        .flat_map(|line| wrap_timeline_line(line, inner.width))
        .collect::<Vec<_>>();
    let visual_start = visual_lines.len().saturating_sub(visible_height);
    visual_lines.into_iter().skip(visual_start).collect()
}

/// Renders the complete bounded local history into visual terminal rows. This
/// intentionally runs only when the operator first pauses output. The frozen
/// rows make subsequent live appends O(1) for viewport stability and avoid
/// mixing logical-row offsets with post-wrap visual-row offsets.
fn all_output_visual_lines(app: &App, width: u16) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let view = app.current();
    let truncation_line = view.local_truncation_line();
    let shell_prompt = view.effective_shell_prompt();
    let uboot_prompt = view.effective_uboot_prompt();
    let source_width = detailed_source_width(width as usize);
    truncation_line
        .iter()
        .chain(view.lines.iter().chain(view.pending_line.iter()))
        .flat_map(|entry| {
            wrap_timeline_line(
                timeline_line(
                    entry,
                    app.detailed_timeline,
                    source_width,
                    shell_prompt,
                    uboot_prompt,
                    width as usize,
                ),
                width,
            )
        })
        .collect()
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
/// additionally shows the legacy `#seq` and source columns. Stream rows get
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

fn draw_status(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let control = app
        .current()
        .snapshot
        .control
        .as_ref()
        .map(|lease| safe_inline(&lease.owner.label))
        .unwrap_or_else(|| tr("ui.control.none").into());
    let mode = match app.current_mode() {
        InputMode::Line => "LINE",
        InputMode::Raw => "RAW",
    };
    let prefix = if app.prefix_pending {
        tr("ui.prefix")
    } else {
        ""
    };
    let uncertain = if app.uncertain_write_outcomes == 0 {
        String::new()
    } else {
        trf("ui.uncertain", &[&app.uncertain_write_outcomes.to_string()])
    };
    let slot_id = &app.current().snapshot.config.id;
    let queue = if let Some(queued) = app.queued_controls.get(slot_id) {
        let writes = app.pending_writes.get(slot_id).map_or(0, VecDeque::len);
        trf(
            "ui.queued",
            &[
                &queued.position.to_string(),
                &queued.since.elapsed().as_secs().to_string(),
                &writes.to_string(),
            ],
        )
    } else if app.pending_requests.values().any(
        |request| matches!(request, PendingRequest::Acquire { slot_id: pending, .. } if pending == slot_id),
    ) {
        tr("ui.control.pending").into()
    } else {
        String::new()
    };
    let idle = if app.owns_control(app.selected) {
        app.current()
            .last_manual_activity
            .map_or_else(String::new, |activity| {
                let remaining = app
                    .human_idle_release
                    .saturating_sub(activity.elapsed())
                    .as_secs();
                trf("ui.idle.release", &[&remaining.to_string()])
            })
    } else {
        String::new()
    };
    let trigger =
        app.current()
            .snapshot
            .active_trigger
            .as_ref()
            .map_or_else(String::new, |trigger| {
                let short_id = trigger.id.to_string().chars().take(8).collect::<String>();
                trf(
                    "ui.trigger",
                    &[
                        &short_id,
                        app.current()
                            .trigger_status_text()
                            .unwrap_or_else(|| trigger_status_label(trigger.status)),
                        &trigger.fires_confirmed.to_string(),
                    ],
                )
            });
    let content = format!(
        " {} · {mode}{prefix} · {} {control}{idle}{queue}{trigger} · {}{}",
        safe_inline(slot_id),
        tr("ui.status.control"),
        safe_inline(&app.status),
        uncertain
    );
    let style = if app.current_mode() == InputMode::Raw {
        Style::default().fg(Color::Black).bg(Color::Yellow)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    frame.render_widget(Paragraph::new(content).style(style), area);
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
        (selection.slot_id == app.selected_slot_id())
            .then_some(selection.selected.min(cards.len() - 1))
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
    let card_rows = |card: &QueueCard, header: String, body: &[String]| {
        let style = style_for(card);
        std::iter::once(Line::from(Span::styled(header, style)))
            .chain(
                body.iter()
                    .map(move |row| Line::from(Span::styled(format!("│ {row}"), style))),
            )
            .collect::<Vec<_>>()
    };

    let rows = if let Some(selected_index) = selected {
        let selected_card = &cards[selected_index];
        let total_visual_rows = cards
            .iter()
            .map(|card| card.body.len().saturating_add(1))
            .sum::<usize>();
        if selected_card.body.len().saturating_add(1) > height {
            let body_height = height.saturating_sub(1);
            let max_scroll = selected_card.body.len().saturating_sub(body_height);
            let scroll = app
                .queue_selection
                .as_ref()
                .map_or(0, |selection| selection.detail_scroll.min(max_scroll));
            let through = scroll
                .saturating_add(body_height)
                .min(selected_card.body.len());
            let header = trf(
                "ui.queue.page",
                &[
                    &selected_card.header,
                    &(scroll + 1).to_string(),
                    &through.to_string(),
                    &selected_card.body.len().to_string(),
                ],
            );
            card_rows(
                selected_card,
                format!("▶ {header}"),
                &selected_card.body[scroll..through],
            )
        } else if total_visual_rows > height {
            // In a short terminal, selection mode is an explicit detail view:
            // show the chosen card in full instead of clipping neighboring
            // commands mid-card. Up/Down changes which complete card is shown.
            card_rows(
                selected_card,
                format!("▶ {}", selected_card.header),
                &selected_card.body,
            )
        } else {
            let flattened = cards
                .iter()
                .flat_map(|card| {
                    let marker = if card.operation_index == selected_index {
                        "▶"
                    } else {
                        " "
                    };
                    card_rows(card, format!("{marker} {}", card.header), &card.body)
                })
                .collect::<Vec<_>>();
            let selected_start = cards
                .iter()
                .take(selected_index)
                .map(|card| card.body.len().saturating_add(1))
                .sum::<usize>();
            let selected_end = selected_start
                .saturating_add(selected_card.body.len())
                .saturating_add(1);
            let start = selected_end
                .saturating_sub(height)
                .min(flattened.len().saturating_sub(height));
            flattened.into_iter().skip(start).take(height).collect()
        }
    } else {
        let flattened = cards
            .iter()
            .flat_map(|card| card_rows(card, format!("  {}", card.header), &card.body))
            .collect::<Vec<_>>();
        if flattened.len() <= height {
            flattened
        } else {
            let hidden = flattened.len().saturating_sub(height.saturating_sub(1));
            let mut rows = flattened
                .into_iter()
                .take(height.saturating_sub(1))
                .collect::<Vec<_>>();
            rows.push(Line::from(Span::styled(
                trf("ui.queue.more", &[&hidden.to_string()]),
                Style::default().fg(Color::Yellow),
            )));
            rows
        }
    };
    frame.render_widget(Paragraph::new(rows).block(block), area);
}

struct RunPanelRow {
    line: Line<'static>,
    command: Option<RunCommandKey>,
}

fn run_status_text(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Active => tr("ui.run.status.active"),
        RunStatus::Completed => tr("ui.run.status.completed"),
        RunStatus::Aborted => tr("ui.run.status.aborted"),
    }
}

fn run_history_rows(app: &App, width: u16) -> Vec<RunPanelRow> {
    let view = app.current();
    let selected = view.selected_run_command_key();
    let available = width.saturating_sub(4).max(1);
    let mut rows = Vec::new();
    if view.run_history_limited {
        for text in wrap_queue_text(tr("ui.run.history.limited"), width.max(1)) {
            rows.push(RunPanelRow {
                line: Line::from(Span::styled(
                    text,
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                command: None,
            });
        }
        if !view.run_history.is_empty() {
            rows.push(RunPanelRow {
                line: Line::default(),
                command: None,
            });
        }
    }
    if view.run_history.is_empty() {
        rows.push(RunPanelRow {
            line: Line::from(Span::styled(
                tr("ui.run.none"),
                Style::default().fg(Color::DarkGray),
            )),
            command: None,
        });
        return rows;
    }
    for run in view.run_history_newest_first() {
        let label = if run.label.trim().is_empty() {
            tr("ui.run.unknown").to_string()
        } else {
            safe_inline(&run.label)
        };
        let owner = run
            .owner_label
            .as_deref()
            .map(safe_inline)
            .filter(|owner| !owner.is_empty())
            .unwrap_or_else(|| tr("ui.run.owner.unknown").into());
        let short_id = run.id.to_string().chars().take(8).collect::<String>();
        rows.push(RunPanelRow {
            line: Line::from(Span::styled(
                trf(
                    "ui.run.header",
                    &[run_status_text(run.status), &label, &owner, &short_id],
                ),
                Style::default()
                    .fg(match run.status {
                        RunStatus::Active => Color::LightBlue,
                        RunStatus::Completed => Color::LightGreen,
                        RunStatus::Aborted => Color::LightRed,
                    })
                    .add_modifier(Modifier::BOLD),
            )),
            command: None,
        });

        if run.commands.is_empty() {
            rows.push(RunPanelRow {
                line: Line::from(Span::styled(
                    format!("  {}", tr("ui.run.no.described.commands")),
                    Style::default().fg(Color::DarkGray),
                )),
                command: None,
            });
        }

        for command in run.commands.iter().rev() {
            let key = RunCommandKey {
                run_id: run.id,
                first_seq: command.first_seq,
            };
            let is_selected = selected == Some(key);
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
            let description = command
                .description
                .as_deref()
                .map(safe_inline)
                .unwrap_or_else(|| tr("ui.run.description.missing").into());
            for (line_index, text) in wrap_queue_text(&description, available)
                .into_iter()
                .enumerate()
            {
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
                });
            }

            if !expanded {
                continue;
            }
            let actor = command
                .actor_label
                .as_deref()
                .map(safe_inline)
                .filter(|actor| !actor.is_empty())
                .unwrap_or_else(|| tr("ui.run.owner.unknown").into());
            rows.push(RunPanelRow {
                line: Line::from(Span::styled(
                    format!(
                        "    {}",
                        trf(
                            if command.partial {
                                "ui.run.command.meta.partial"
                            } else {
                                "ui.run.command.meta"
                            },
                            &[
                                &command.first_seq.to_string(),
                                &command.last_seq.to_string(),
                                &actor,
                            ],
                        )
                    ),
                    Style::default().fg(Color::DarkGray),
                )),
                command: Some(key),
            });
            let payload = safe_inline(&String::from_utf8_lossy(&command.data));
            let payload = if payload.is_empty() {
                tr("ui.run.command.empty").into()
            } else {
                payload
            };
            for text in wrap_queue_text(&payload, available).into_iter() {
                rows.push(RunPanelRow {
                    line: Line::from(Span::styled(
                        format!("    │ {text}"),
                        Style::default().fg(Color::Gray),
                    )),
                    command: Some(key),
                });
            }
            if command.partial {
                rows.push(RunPanelRow {
                    line: Line::from(Span::styled(
                        format!("    {}", tr("ui.run.command.partial")),
                        Style::default().fg(Color::Yellow),
                    )),
                    command: Some(key),
                });
            }
            if command.truncated {
                rows.push(RunPanelRow {
                    line: Line::from(Span::styled(
                        format!("    {}", tr("ui.run.command.truncated")),
                        Style::default().fg(Color::Yellow),
                    )),
                    command: Some(key),
                });
            }
        }
        if let Some(reason) = run.abort_reason.as_deref() {
            rows.push(RunPanelRow {
                line: Line::from(Span::styled(
                    format!("  {}", trf("ui.run.abort.reason", &[&safe_inline(reason)])),
                    Style::default().fg(Color::LightRed),
                )),
                command: None,
            });
        }
        rows.push(RunPanelRow {
            line: Line::default(),
            command: None,
        });
    }
    rows
}

fn draw_run_history(frame: &mut Frame<'_>, app: &App, area: Rect) {
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
    let inner = block.inner(area);
    if inner.height == 0 || inner.width == 0 {
        frame.render_widget(block, area);
        return;
    }
    let rows = run_history_rows(app, inner.width);
    if rows.is_empty() {
        frame.render_widget(Paragraph::new(tr("ui.run.none")).block(block), area);
        return;
    }
    let height = inner.height as usize;
    let selected = app.current().selected_run_command_key();
    let selected_row = selected
        .and_then(|selected| rows.iter().position(|row| row.command == Some(selected)))
        .unwrap_or(0);
    let max_start = rows.len().saturating_sub(height);
    let start = selected_row
        .saturating_sub(2)
        .saturating_add(app.current().run_detail_scroll)
        .min(max_start);
    frame.render_widget(
        Paragraph::new(
            rows.into_iter()
                .skip(start)
                .take(height)
                .map(|row| row.line)
                .collect::<Vec<_>>(),
        )
        .block(block),
        area,
    );
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
    frame.render_widget(Paragraph::new(text).block(block.title(title)), area);
    if let Some(cursor_column) = cursor_column
        && app.focus == PaneFocus::Input
    {
        frame.set_cursor_position(Position::new(
            inner.x.saturating_add(cursor_column),
            inner.y,
        ));
    }
}

fn input_title(app: &App, mode: InputMode) -> String {
    let slot_id = &app.current().snapshot.config.id;
    let Some(writes) = app
        .pending_writes
        .get(slot_id)
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
    let mut output = String::new();
    let mut width = 0usize;
    for character in value.chars() {
        let character_width = UnicodeWidthChar::width(character).unwrap_or(0);
        if width.saturating_add(character_width) > max_width {
            output.push('…');
            break;
        }
        output.push(character);
        width = width.saturating_add(character_width);
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

fn draw_help_line(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let scroll = if app.current_mode() == InputMode::Raw {
        tr("ui.scroll.prefix")
    } else {
        tr("ui.scroll.plain")
    };
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

fn help_item(key: &'static str) -> Line<'static> {
    Line::from(Span::styled(tr(key), Style::default().fg(Color::White)))
}

/// Builds explicit visual rows instead of joining one large string and
/// relying on Paragraph wrapping. Every shortcut remains a distinct row;
/// section gaps are preserved and the popup can scroll on narrow terminals.
fn help_lines(app: &App) -> Vec<Line<'static>> {
    let mouse_key = if app.mouse_capture {
        "help.wheel"
    } else {
        "help.selection"
    };
    let idle_seconds = app.human_idle_release.as_secs().to_string();
    vec![
        help_heading("help.group.navigation"),
        help_item("help.switch"),
        help_item("help.next"),
        help_item("help.mode"),
        help_item("help.view"),
        help_item("help.lang"),
        help_item("help.scroll"),
        help_item(mouse_key),
        help_item("help.mouse.paste"),
        help_item("help.menu"),
        help_item("help.profile"),
        help_item("help.run.history"),
        help_item("help.run.keys"),
        Line::default(),
        help_heading("help.group.control"),
        help_item("help.takeover"),
        help_item("help.cooperative"),
        help_item("help.release"),
        Line::default(),
        help_heading("help.group.queue"),
        help_item("help.queue.behavior"),
        help_item("help.queue.select"),
        help_item("help.queue.delete"),
        help_item("help.queue.edit"),
        help_item("help.paste"),
        Line::default(),
        help_heading("help.group.line"),
        help_item("help.line1"),
        help_item("help.line2"),
        help_item("help.line3"),
        help_item("help.follow"),
        help_item("help.echo"),
        Line::default(),
        help_heading("help.group.raw"),
        help_item("help.raw1"),
        help_item("help.raw2"),
        help_item("help.byte"),
        help_item("help.interrupt"),
        Line::default(),
        help_heading("help.group.safety"),
        Line::from(tr("help.paste.note")),
        Line::from(trf("help.expire", &[&idle_seconds])),
        Line::from(tr("help.replay")),
        Line::from(tr("help.uncertain")),
        help_item("help.quit"),
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
        Constraint::Length(3),
        Constraint::Length(1),
        Constraint::Length(1),
    ])
    .split(inner);

    let current_model = menu
        .catalog
        .as_ref()
        .and_then(|catalog| slot_model_binding(catalog, &app.selected_slot_id()))
        .map(|binding| binding.model_id.as_str())
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.unbound").into());
    let slot_id = safe_inline(&app.selected_slot_id());
    let transport = safe_inline(&app.current().snapshot.config.profile);
    let device = app
        .current()
        .snapshot
        .config
        .device_profile
        .as_deref()
        .map(safe_inline)
        .unwrap_or_else(|| tr("menu.value.generic").into());
    let model = current_model;
    let header = trf("menu.current", &[&slot_id, &transport, &device, &model]);
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
        Paragraph::new(menu_detail(app, menu))
            .wrap(Wrap { trim: false })
            .style(Style::default().fg(Color::Gray)),
        chunks[2],
    );
    frame.render_widget(
        Paragraph::new(if menu.busy {
            format!("◐ {}", menu.message)
        } else {
            menu.message.clone()
        })
        .style(if menu.busy {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        }),
        chunks[3],
    );
    frame.render_widget(
        Paragraph::new(menu_footer(menu.page))
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::DarkGray)),
        chunks[4],
    );

    if let Some(prompt) = menu.prompt.as_ref() {
        draw_menu_prompt(frame, prompt, popup);
    }
}

fn draw_menu_prompt(frame: &mut Frame<'_>, prompt: &MenuPrompt, parent: Rect) {
    let width = parent.width.saturating_sub(6).clamp(1, 76);
    let popup = centered_rect(width, 5.min(parent.height).max(1), parent);
    frame.render_widget(Clear, popup);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", prompt.title))
        .border_style(Style::default().fg(Color::Yellow));
    let inner = block.inner(popup);
    let visible = if prompt.secret {
        vec!['•'; prompt.value.len()]
    } else {
        prompt.value.clone()
    };
    let (text, cursor) = line_input_projection(&visible, prompt.cursor, inner.width);
    frame.render_widget(Paragraph::new(text).block(block), popup);
    if inner.width > 0 && inner.height > 0 {
        frame.set_cursor_position(Position::new(
            inner
                .x
                .saturating_add(cursor.min(inner.width.saturating_sub(1))),
            inner.y,
        ));
    }
}

fn menu_page_title(page: MenuPage) -> &'static str {
    match page {
        MenuPage::Root => tr("menu.title"),
        MenuPage::Profiles => tr("menu.profile.title"),
        MenuPage::TransportProfiles => tr("menu.transport.title"),
        MenuPage::DeviceProfiles => tr("menu.device.title"),
        MenuPage::Models => tr("menu.model.title"),
        MenuPage::ModelParents => tr("menu.model.parent.title"),
        MenuPage::SerialSettings => tr("menu.serial.title"),
        MenuPage::Help => tr("menu.help.title"),
    }
}

fn menu_footer(page: MenuPage) -> &'static str {
    match page {
        MenuPage::Models => tr("menu.footer.models"),
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

fn menu_rows(app: &App, menu: &MenuState) -> Vec<Line<'static>> {
    match menu.page {
        MenuPage::Root => [
            tr("menu.root.profile"),
            tr("menu.root.model"),
            tr("menu.root.serial"),
            tr("menu.root.help"),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, text)| selected_menu_line(index, menu.selected, text.into()))
        .collect(),
        MenuPage::Profiles => [tr("menu.profile.transport"), tr("menu.profile.device")]
            .into_iter()
            .enumerate()
            .map(|(index, text)| selected_menu_line(index, menu.selected, text.into()))
            .collect(),
        MenuPage::TransportProfiles => {
            let Some(catalog) = menu.catalog.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let current = &app.current().snapshot.config.profile;
            catalog
                .transport_profiles
                .iter()
                .map(|profile| {
                    format!(
                        "{}{}",
                        if &profile.name == current {
                            "✓ "
                        } else {
                            "  "
                        },
                        safe_inline(&profile.name)
                    )
                })
                .chain(std::iter::once(tr("menu.transport.new").into()))
                .enumerate()
                .map(|(index, text)| selected_menu_line(index, menu.selected, text))
                .collect()
        }
        MenuPage::DeviceProfiles => {
            let Some(catalog) = menu.catalog.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let current = app.current().snapshot.config.device_profile.as_deref();
            std::iter::once(format!(
                "{}{}",
                if current.is_none() { "✓ " } else { "  " },
                tr("menu.device.generic")
            ))
            .chain(catalog.device_profiles.iter().map(|profile| {
                format!(
                    "{}{}",
                    if current == Some(profile.name.as_str()) {
                        "✓ "
                    } else {
                        "  "
                    },
                    safe_inline(&profile.name)
                )
            }))
            .chain(std::iter::once(tr("menu.device.new").into()))
            .chain(DEVICE_PRESETS.iter().map(|preset| preset.label().into()))
            .enumerate()
            .map(|(index, text)| selected_menu_line(index, menu.selected, text))
            .collect()
        }
        MenuPage::Models => {
            let Some(catalog) = menu.catalog.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            let bound = slot_model_binding(catalog, &app.selected_slot_id())
                .map(|binding| binding.model_id.as_str());
            let mut rows = vec![
                selected_menu_line(0, menu.selected, tr("menu.model.add.root").into()),
                selected_menu_line(1, menu.selected, tr("menu.model.add.child").into()),
            ];
            for (offset, row) in visible_model_rows(&catalog.models, &menu.expanded_models)
                .into_iter()
                .enumerate()
            {
                let model = &catalog.models[row.index];
                let children = model_has_children(&catalog.models, &model.id);
                let disclosure = if !children {
                    "  "
                } else if menu.expanded_models.contains(&model.id) {
                    "▾ "
                } else {
                    "▸ "
                };
                let text = format!(
                    "{}{}{}{} ({})",
                    "  ".repeat(row.depth),
                    disclosure,
                    if bound == Some(model.id.as_str()) {
                        "✓ "
                    } else {
                        "  "
                    },
                    safe_inline(&model.name),
                    safe_inline(&model.id),
                );
                rows.push(selected_menu_line(offset + 2, menu.selected, text));
            }
            rows
        }
        MenuPage::ModelParents => {
            let Some(catalog) = menu.catalog.as_ref() else {
                return vec![Line::from(tr("menu.loading"))];
            };
            all_model_rows(&catalog.models)
                .into_iter()
                .enumerate()
                .map(|(index, row)| {
                    selected_menu_line(
                        index,
                        menu.selected,
                        format!(
                            "{}{} ({})",
                            "  ".repeat(row.depth),
                            safe_inline(&catalog.models[row.index].name),
                            safe_inline(&catalog.models[row.index].id),
                        ),
                    )
                })
                .collect()
        }
        MenuPage::SerialSettings => TRANSPORT_PRESETS
            .iter()
            .copied()
            .enumerate()
            .map(|(index, preset)| selected_menu_line(index, menu.selected, preset.label()))
            .collect(),
        MenuPage::Help => menu_help_lines()
            .into_iter()
            .map(|line| match line {
                Some((text, true)) => Line::from(Span::styled(
                    text,
                    Style::default()
                        .fg(Color::LightCyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Some((text, false)) => {
                    Line::from(Span::styled(text, Style::default().fg(Color::White)))
                }
                None => Line::default(),
            })
            .collect(),
    }
}

fn menu_selected_visual_row(menu: &MenuState) -> Option<usize> {
    (menu.page != MenuPage::Help && menu_item_count(menu) > 0).then_some(menu.selected)
}

fn menu_detail(app: &App, menu: &MenuState) -> String {
    match menu.page {
        MenuPage::TransportProfiles => menu
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.transport_profiles.get(menu.selected))
            .map_or_else(
                || tr("menu.transport.new.detail").into(),
                transport_profile_detail,
            ),
        MenuPage::DeviceProfiles => {
            if menu.selected == 0 {
                tr("menu.device.generic.detail").into()
            } else {
                menu.catalog
                    .as_ref()
                    .and_then(|catalog| catalog.device_profiles.get(menu.selected - 1))
                    .map_or_else(
                        || tr("menu.device.clone.detail").into(),
                        device_profile_detail,
                    )
            }
        }
        MenuPage::Models | MenuPage::ModelParents => tr("menu.model.verify").into(),
        MenuPage::Profiles => tr("menu.profile.detail").into(),
        MenuPage::SerialSettings => {
            let Some(catalog) = menu.catalog.as_ref() else {
                return tr("menu.loading").into();
            };
            trf(
                "menu.serial.current",
                &[&transport_profile_detail(&current_transport_template(
                    app.current(),
                    catalog,
                ))],
            )
        }
        MenuPage::Help => String::new(),
        _ => tr("menu.root.detail").into(),
    }
}

fn transport_profile_detail(profile: &TransportProfile) -> String {
    let data_bits = match profile.data_bits {
        DataBits::Five => "5",
        DataBits::Six => "6",
        DataBits::Seven => "7",
        DataBits::Eight => "8",
    };
    let stop_bits = match profile.stop_bits {
        StopBits::One => "1",
        StopBits::Two => "2",
    };
    let parity = match profile.parity {
        Parity::None => tr("menu.detail.parity.none"),
        Parity::Odd => tr("menu.detail.parity.odd"),
        Parity::Even => tr("menu.detail.parity.even"),
    };
    let flow = match profile.flow_control {
        FlowControl::None => tr("menu.detail.flow.none"),
        FlowControl::Software => tr("menu.detail.flow.software"),
        FlowControl::Hardware => tr("menu.detail.flow.hardware"),
    };
    let baud = trf("menu.detail.baud", &[&profile.baud_rate.to_string()]);
    let data_bits = trf("menu.detail.data_bits", &[data_bits]);
    let stop_bits = trf("menu.detail.stop_bits", &[stop_bits]);
    trf(
        "menu.detail.transport",
        &[
            &baud,
            &data_bits,
            parity,
            &stop_bits,
            flow,
            if profile.dtr {
                tr("menu.value.on")
            } else {
                tr("menu.value.off")
            },
            if profile.rts {
                tr("menu.value.on")
            } else {
                tr("menu.value.off")
            },
            if profile.auto_open {
                tr("menu.value.enabled")
            } else {
                tr("menu.value.disabled")
            },
        ],
    )
}

fn device_profile_detail(profile: &DeviceProfile) -> String {
    let unset = tr("menu.value.unbound");
    let shell = trf(
        "menu.detail.prompt.shell",
        &[&safe_inline(
            profile.shell_prompt.as_deref().unwrap_or(unset),
        )],
    );
    let uboot = trf(
        "menu.detail.prompt.uboot",
        &[&safe_inline(
            profile.uboot_prompt.as_deref().unwrap_or(unset),
        )],
    );
    let eol_value = match profile.write_eol.as_deref() {
        Some("\r") => "CR",
        Some("\n") => "LF",
        Some("\r\n") => "CRLF",
        Some("") => tr("menu.detail.eol.none"),
        Some(_) => tr("menu.detail.eol.custom"),
        None => tr("menu.detail.eol.inherit"),
    };
    let eol = trf("menu.detail.eol", &[eol_value]);
    let echo = match profile.echo {
        Some(EchoMode::On) => tr("menu.detail.echo.on"),
        Some(EchoMode::Off) => tr("menu.detail.echo.off"),
        Some(EchoMode::Auto) => tr("menu.detail.echo.auto"),
        None => tr("menu.detail.eol.inherit"),
    };
    let chunk_size = profile.write_chunk_size.map_or_else(
        || tr("menu.detail.eol.inherit").into(),
        |value| value.to_string(),
    );
    let delay = profile.write_chunk_delay_ms.map_or_else(
        || tr("menu.detail.eol.inherit").into(),
        |value| value.to_string(),
    );
    let pacing = trf("menu.detail.pacing", &[&chunk_size, &delay]);
    trf("menu.detail.device", &[&shell, &uboot, &eol, echo, &pacing])
}

fn menu_help_lines() -> Vec<Option<(&'static str, bool)>> {
    vec![
        Some((tr("help.group.navigation"), true)),
        Some((tr("menu.help.menu"), false)),
        Some((tr("help.profile"), false)),
        Some((tr("help.run.history"), false)),
        Some((tr("help.run.keys"), false)),
        None,
        Some((tr("help.group.control"), true)),
        Some((tr("menu.help.enter"), false)),
        Some((tr("menu.help.cooperative"), false)),
        Some((tr("menu.help.takeover"), false)),
        None,
        Some((tr("help.group.queue"), true)),
        Some((tr("menu.help.queue"), false)),
        None,
        Some((tr("help.group.safety"), true)),
        Some((tr("menu.help.echo"), false)),
        Some((tr("menu.help.model"), false)),
        Some((tr("menu.help.token"), false)),
    ]
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
    use std::{collections::BTreeMap, sync::Mutex};

    use crossterm::event::KeyEvent;
    use ratatui::backend::TestBackend;
    use serial_protocol::{ActorKind, Direction, SerialSettings, SlotConfig, TriggerSpec};

    use super::*;

    static TEST_CLIPBOARD: Mutex<Vec<String>> = Mutex::new(Vec::new());

    fn record_clipboard_copy(text: &str) -> Result<()> {
        TEST_CLIPBOARD
            .lock()
            .expect("test clipboard lock")
            .push(text.to_owned());
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
        app.slots[0].snapshot.effective_write_eol = Some("\r\n".into());
        app.slots[0].history.push("previous command".into());
        app.slots[0].history_cursor = Some(0);
        app.slots[0].draft = "echo 'unterminated".chars().collect();
        app.slots[0].draft_cursor = app.slots[0].draft.len();
        app.slots[0].scroll_from_bottom = 5;
        app.slots[0].unseen = 2;
        let control_id = app.slots[0]
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
        assert_eq!(app.current().history_cursor, None);
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
        app.slots[0].mode = InputMode::Raw;
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
        replay.daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        app.push_event(replay, true, &commands);

        assert_eq!(
            app.slots[0].snapshot.target_activity,
            TargetActivity::Silent
        );
        assert_eq!(app.slots[0].snapshot.last_rx_wall_time_ns, Some(1));
        assert!(!app.slots[0].lines.is_empty());
    }

    #[test]
    fn provisional_live_event_does_not_claim_logging_is_degraded() {
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        let mut provisional = event(EventKind::Rx, Direction::Rx, 1, b"live");
        provisional.daemon_epoch = daemon_epoch;
        provisional.durable = false;
        app.push_event(provisional, false, &commands);
        assert_eq!(app.slots[0].snapshot.logging, LoggingState::Healthy);

        let mut degraded = event(EventKind::LoggingDegraded, Direction::None, 2, &[]);
        degraded.daemon_epoch = daemon_epoch;
        degraded.durable = false;
        app.push_event(degraded, false, &commands);
        assert_eq!(app.slots[0].snapshot.logging, LoggingState::Degraded);
    }

    #[test]
    fn serial_close_discards_queued_control_and_input() {
        let mut app = App::new(vec![snapshot()], None);
        let slot_id = app.selected_slot_id();
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
        app.slots[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry(slot_id.clone())
            .or_default()
            .push_back(PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                slot_id: slot_id.clone(),
                mode: ControlMode::Queue,
            },
        );
        let (commands, _) = mpsc::channel(4);

        let mut closed = event(EventKind::SerialClosed, Direction::None, 1, &[]);
        closed.daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        app.push_event(closed, false, &commands);

        assert!(!app.pending_writes.contains_key(&slot_id));
        assert!(app.pending_requests.is_empty());
        assert!(app.slots[0].snapshot.active_trigger.is_none());
    }

    #[test]
    fn trigger_lifecycle_projects_live_state_and_confirmed_fires() {
        // The product defaults to Chinese, while this assertion deliberately
        // verifies the stable English rendering. Serialize access to the
        // process-global locale so parallel localization tests cannot race it.
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Armed);
        let trigger_id = trigger.id;
        let (commands, _) = mpsc::channel(4);

        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started.actor = Some(trigger.owner.clone());
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        app.push_event(started, false, &commands);

        let projected = app.slots[0]
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

        let projected = app.slots[0]
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
        assert!(app.slots[0].snapshot.active_trigger.is_none());
        assert!(
            app.slots[0]
                .lines
                .iter()
                .any(|line| line.text == "trigger_completed: matched")
        );
    }

    #[test]
    fn trigger_projection_matches_start_and_stop_literals_across_rx_events() {
        let mut app = App::new(vec![snapshot()], None);
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::WaitingForStart);
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
            app.slots[0]
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
            app.slots[0]
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
            app.slots[0]
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
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
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
            app.slots[0]
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
            app.slots[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );

        let mut no_match_app = App::new(vec![snapshot()], None);
        let daemon_epoch = no_match_app.slots[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&no_match_app.slots[0].snapshot, TriggerStatus::Running);
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
            no_match_app.slots[0]
                .snapshot
                .active_trigger
                .as_ref()
                .unwrap()
                .status,
            TriggerStatus::Stopping
        );

        let mut timeout_app = App::new(vec![snapshot()], None);
        let daemon_epoch = timeout_app.slots[0].snapshot.daemon_epoch;
        let mut trigger = trigger_info(&timeout_app.slots[0].snapshot, TriggerStatus::Running);
        trigger.spec.timeout_ms = 1;
        let mut started = event(EventKind::TriggerStarted, Direction::None, 1, &[]);
        started.daemon_epoch = daemon_epoch;
        started
            .metadata
            .insert("trigger".into(), serde_json::to_value(&trigger).unwrap());
        timeout_app.push_event(started, false, &commands);

        assert!(
            timeout_app.slots[0].update_trigger_deadline(Instant::now() + Duration::from_millis(2))
        );
        assert_eq!(
            timeout_app.slots[0]
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
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
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
            app.slots[0].trigger_status_text(),
            Some(tr("trigger.status.active"))
        );
        assert!(app.slots[0].snapshot.active_trigger.is_some());
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
            let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
            let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Stopping);
            app.slots[0].snapshot.active_trigger = Some(trigger);
            let (commands, _) = mpsc::channel(4);
            let mut terminal = event(kind, Direction::None, offset as u64 + 1, &[]);
            terminal.daemon_epoch = daemon_epoch;

            app.push_event(terminal, false, &commands);

            assert!(
                app.slots[0].snapshot.active_trigger.is_none(),
                "{kind:?} left a stale active Trigger"
            );
        }
    }

    #[test]
    fn human_write_waits_for_trigger_terminal_after_takeover() {
        let mut app = ready_app_with_control();
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Stopping);
        app.slots[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry("slot-1".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        let (commands, mut received) = mpsc::channel(4);

        assert!(app.flush_pending_writes("slot-1", &commands));
        assert!(received.try_recv().is_err());
        assert_eq!(app.pending_writes["slot-1"].len(), 1);

        let mut cancelled = event(EventKind::TriggerCancelled, Direction::None, 1, &[]);
        cancelled.daemon_epoch = daemon_epoch;
        app.push_event(cancelled, false, &commands);

        assert!(app.slots[0].snapshot.active_trigger.is_none());
        let (_, data, _) = take_write(&mut received);
        assert_eq!(data, b"version\r");
    }

    #[test]
    fn disconnect_keeps_sent_unacknowledged_write_warning_visible() {
        let mut app = App::new(vec![snapshot()], None);
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Write {
                slot_id: "slot-1".into(),
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
        app.authenticated = true;
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

        assert!(!app.pending_writes.contains_key("slot-1"));
        assert_eq!(app.slots[0].draft.iter().collect::<String>(), "echo queued");
        assert_eq!(app.slots[0].draft_cursor, "echo queued".chars().count());
        assert_eq!(app.slots[0].mode, InputMode::Line);
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::CancelAcquire { slot_id } if slot_id == "slot-1")
        ));
    }

    #[test]
    fn raw_queue_is_not_lossily_converted_into_a_line_draft() {
        let mut app = ready_app_with_foreign_control();
        let (commands, _received) = mpsc::channel(4);
        assert!(app.request_raw_write(&commands, vec![0x03]));

        app.remove_last_queued_line(true, &commands);

        assert_eq!(app.pending_writes["slot-1"][0].data, vec![0x03]);
        assert!(app.slots[0].draft.is_empty());
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

        let queued = app.pending_writes.get("slot-1").expect("queued RAW data");
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
            .get("slot-1")
            .expect("full bounded RAW queue")
            .iter()
            .map(|write| write.data.clone())
            .collect::<Vec<_>>();
        assert_eq!(before.len(), MAX_PENDING_WRITES);

        assert!(!app.request_raw_write(&commands, vec![b'y']));
        let after = app
            .pending_writes
            .get("slot-1")
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
        assert_eq!(app.pending_writes["slot-1"].len(), 3);
        assert_eq!(
            queued_line_operations(&app.pending_writes["slot-1"])[0]
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
        assert_eq!(app.pending_writes["slot-1"].len(), 3);
        assert_eq!(
            queued_line_operations(&app.pending_writes["slot-1"])[0]
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
        assert_eq!(app.pending_writes["slot-1"].len(), 3);
        app.handle_result(
            third_id,
            CommandResult::WriteAccepted { event_seq: 3 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("slot-1"));
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
        assert_eq!(app.pending_writes["slot-1"].len(), 2);

        app.handle_server_message(
            ServerMessage::Error {
                request_id: Some(request_id),
                code: serial_protocol::ErrorCode::PortOffline,
                message: "port went offline".into(),
                retryable: true,
            },
            &commands,
        );

        assert!(!app.pending_writes.contains_key("slot-1"));
        assert!(received.try_recv().is_err());
    }

    #[test]
    fn confirmed_line_paste_is_one_ordered_chunked_write() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            slot_id: "slot-1".into(),
            bytes: vec![b'x'; MAX_WRITE_BYTES + 1],
            raw: false,
        });

        app.confirm_paste(&commands);

        let (first_id, first_data, operation_id) = take_write(&mut received);
        assert_eq!(first_data, vec![b'x'; MAX_WRITE_BYTES]);
        let operation_id = operation_id.expect("line paste operation ID");
        assert_eq!(app.pending_writes["slot-1"].len(), 2);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (second_id, second_data, second_operation) = take_write(&mut received);
        assert_ne!(first_id, second_id);
        assert_eq!(second_data, b"x\r");
        assert_eq!(second_operation, Some(operation_id));
        assert_eq!(app.pending_writes["slot-1"].len(), 2);
        app.handle_result(
            second_id,
            CommandResult::WriteAccepted { event_seq: 2 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("slot-1"));
    }

    #[test]
    fn confirmed_multiline_paste_assigns_each_command_a_distinct_operation() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            slot_id: "slot-1".into(),
            bytes: b"pwd\nversion\n".to_vec(),
            raw: false,
        });

        app.confirm_paste(&commands);

        let (first_id, first_data, first_operation) = take_write(&mut received);
        let first_operation = first_operation.expect("first line paste operation ID");
        assert_eq!(first_data, b"pwd\r");
        assert_eq!(app.pending_writes["slot-1"].len(), 2);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (_, second_data, second_operation) = take_write(&mut received);
        assert_eq!(second_data, b"version\r");
        assert!(second_operation.is_some());
        assert_ne!(second_operation, Some(first_operation));
        assert_eq!(app.pending_writes["slot-1"].len(), 1);
    }

    #[test]
    fn foreign_control_multiline_paste_creates_oldest_first_independent_cards() {
        let mut app = ready_app_with_foreign_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            slot_id: "slot-1".into(),
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

        let operations = queued_line_operations(&app.pending_writes["slot-1"]);
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
        let remaining = queued_line_operations(&app.pending_writes["slot-1"])
            .into_iter()
            .map(|operation| operation.data)
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![b"first\r".to_vec(), b"third\r".to_vec()]);
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Acquire { slot_id, .. } if slot_id == "slot-1")
        ));
    }

    #[test]
    fn confirmed_raw_paste_preserves_one_unmodified_burst() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            slot_id: "slot-1".into(),
            bytes: b"pwd\nversion\n".to_vec(),
            raw: true,
        });

        app.confirm_paste(&commands);

        let (_, data, operation_id) = take_write(&mut received);
        assert_eq!(data, b"pwd\nversion\n");
        assert_eq!(operation_id, None);
        assert!(app.pending_writes.contains_key("slot-1"));
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
            app.slots[0].subscription,
            SubscriptionPhase::Attaching
        ));

        app.handle_server_message(
            ServerMessage::ReplayBegin {
                slot_id: "slot-1".into(),
                from_seq: 4,
                through_seq: 9,
            },
            &commands,
        );
        assert!(matches!(
            app.slots[0].subscription,
            SubscriptionPhase::Replaying {
                from_seq: 4,
                through_seq: 9
            }
        ));

        app.handle_server_message(
            ServerMessage::Ready {
                slot_id: "slot-1".into(),
                head_seq: 9,
            },
            &commands,
        );
        assert!(app.slot_ready(0));

        app.handle_server_message(
            ServerMessage::Lagged {
                slot_id: "slot-1".into(),
                from_seq: 10,
                to_seq: 20,
            },
            &commands,
        );
        assert!(matches!(
            app.slots[0].subscription,
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
    fn live_profile_refresh_updates_effective_behavior_without_changing_config() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        let config = app.slots[0].snapshot.config.clone();
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
        let trigger_id = trigger.id;
        app.slots[0].snapshot.active_trigger = Some(trigger);
        app.pending_writes
            .entry("slot-1".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"queued".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let mut reconfigured = event(EventKind::SlotReconfigured, Direction::None, 1, &[]);
        reconfigured.daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        reconfigured
            .metadata
            .insert("current".into(), serde_json::to_value(&config).unwrap());
        reconfigured.metadata.insert(
            "effective".into(),
            serde_json::to_value(ResolvedDeviceSettings {
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

        assert_eq!(app.slots[0].snapshot.config, config);
        assert_eq!(
            app.slots[0].snapshot.effective_shell_prompt.as_deref(),
            Some("]# ")
        );
        assert_eq!(
            app.slots[0].snapshot.effective_uboot_prompt.as_deref(),
            Some("Luckfox #")
        );
        assert_eq!(
            app.slots[0].snapshot.effective_write_eol.as_deref(),
            Some("\n")
        );
        assert_eq!(app.slots[0].snapshot.effective_echo, Some(EchoMode::Off));
        assert_eq!(
            app.slots[0].snapshot.effective_write_pacing,
            Some(WritePacing {
                chunk_size: 1,
                chunk_delay_ms: 1,
            })
        );
        assert_eq!(
            app.slots[0]
                .snapshot
                .active_trigger
                .as_ref()
                .map(|trigger| trigger.id),
            Some(trigger_id)
        );
        assert!(app.pending_writes.contains_key("slot-1"));
    }

    #[test]
    fn physical_reconfigure_updates_config_even_if_metadata_claims_profile_only() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
        app.slots[0].snapshot.active_trigger = Some(trigger);
        let mut config = app.slots[0].snapshot.config.clone();
        config.display_name = "Renamed station".into();
        config.settings.baud_rate = 57_600;
        app.pending_writes
            .entry("slot-1".into())
            .or_default()
            .push_back(PendingWrite {
                data: b"queued".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        let mut reconfigured = event(EventKind::SlotReconfigured, Direction::None, 1, &[]);
        reconfigured.daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        reconfigured
            .metadata
            .insert("current".into(), serde_json::to_value(&config).unwrap());
        reconfigured
            .metadata
            .insert("profile_only".into(), serde_json::Value::Bool(true));

        app.push_event(reconfigured, false, &commands);

        assert_eq!(app.slots[0].snapshot.config, config);
        assert!(app.slots[0].snapshot.active_trigger.is_none());
        assert!(!app.pending_writes.contains_key("slot-1"));
    }

    #[test]
    fn removed_slot_projects_an_authoritative_disabled_state() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_control();
        let owner = app.actor.clone().unwrap();
        app.slots[0].snapshot.active_run = Some(RunInfo {
            id: Uuid::new_v4(),
            owner,
            label: "active run".into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        });
        let trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
        app.slots[0].snapshot.active_trigger = Some(trigger);
        let (commands, _) = mpsc::channel(4);
        let mut removed = event(EventKind::SlotRemoved, Direction::None, 2, &[]);
        removed.daemon_epoch = app.slots[0].snapshot.daemon_epoch;

        app.push_event(removed, false, &commands);

        let snapshot = &app.slots[0].snapshot;
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
        let slot_id = app.selected_slot_id();
        app.slots[0].snapshot.control = Some(ControlLease {
            owner: Actor {
                id: "agent:other".into(),
                label: "other-agent".into(),
                kind: ActorKind::Agent,
            },
            ..app.slots[0].snapshot.control.clone().expect("test lease")
        });
        app.pending_writes
            .entry(slot_id.clone())
            .or_default()
            .push_back(PendingWrite {
                data: b"reboot\r".to_vec(),
                operation_id: None,
                kind: PendingWriteKind::Line,
            });
        app.queued_controls.insert(
            slot_id.clone(),
            QueuedControl {
                position: 1,
                since: Instant::now(),
            },
        );
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                slot_id: slot_id.clone(),
                mode: ControlMode::Queue,
            },
        );
        let mut other = snapshot();
        other.config.id = "slot-2".into();
        other.config.display_name = "Slot 2".into();
        other.config.port = "COM4".into();
        let mut other = SlotView::new(other);
        other.subscription = SubscriptionPhase::Ready { head_seq: 0 };
        app.slots.push(other);
        app.pending_writes.insert(
            "slot-2".into(),
            VecDeque::from([PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: Some(Uuid::new_v4()),
                kind: PendingWriteKind::Line,
            }]),
        );
        app.queued_controls.insert(
            "slot-2".into(),
            QueuedControl {
                position: 2,
                since: Instant::now(),
            },
        );
        app.pending_requests.insert(
            Uuid::new_v4(),
            PendingRequest::Acquire {
                slot_id: "slot-2".into(),
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
            ClientMessage::CancelAcquire { slot_id, .. } if slot_id == "slot-1"
        ));
        assert!(!app.pending_writes.contains_key("slot-1"));
        assert!(app.pending_writes.contains_key("slot-2"));
        assert!(!app.queued_controls.contains_key("slot-1"));
        assert!(app.queued_controls.contains_key("slot-2"));
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Acquire { slot_id, .. } if slot_id == "slot-2")
        ));
    }

    #[test]
    fn idle_human_control_is_released_instead_of_renewed_forever() {
        let mut app = ready_app_with_control();
        app.slots[0].last_manual_activity =
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
        app.slots[0].last_manual_activity = Some(Instant::now());
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
            let view = &mut app.slots[0];
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
            app.slots[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Ctrl-R cycles to the older match, then wraps back to the newest.
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        assert_eq!(
            app.slots[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(0))
        );
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::CONTROL));
        assert_eq!(
            app.slots[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Backspace edits the query and re-searches from newest.
        for _ in 0..4 {
            app.handle_history_search_key(KeyEvent::new(KeyCode::Backspace, KeyModifiers::NONE));
        }
        assert_eq!(
            app.slots[0].history_search.as_ref().map(|s| s.match_index),
            Some(None)
        );
        for character in "int".chars() {
            app.handle_history_search_key(KeyEvent::new(
                KeyCode::Char(character),
                KeyModifiers::NONE,
            ));
        }
        assert_eq!(
            app.slots[0].history_search.as_ref().map(|s| s.match_index),
            Some(Some(2))
        );

        // Enter accepts the current match into the draft.
        app.handle_history_search_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert!(app.slots[0].history_search.is_none());
        assert_eq!(
            app.slots[0].draft.iter().collect::<String>(),
            "show interfaces"
        );
    }

    #[test]
    fn history_search_escape_restores_the_original_draft() {
        let mut app = App::new(vec![snapshot()], None);
        {
            let view = &mut app.slots[0];
            view.history = vec!["reboot".into()];
            view.draft = "keep me".chars().collect();
            view.draft_cursor = 7;
        }
        app.start_history_search();
        app.handle_history_search_key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));
        assert!(
            app.slots[0]
                .history_search
                .as_ref()
                .is_some_and(|s| s.match_index == Some(0))
        );

        app.handle_history_search_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(app.slots[0].history_search.is_none());
        assert_eq!(app.slots[0].draft.iter().collect::<String>(), "keep me");
        assert_eq!(app.slots[0].draft_cursor, 7);
    }

    #[test]
    fn tab_completion_cycles_deduplicated_newest_first_candidates() {
        let mut app = App::new(vec![snapshot()], None);
        {
            let view = &mut app.slots[0];
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
            assert_eq!(app.slots[0].draft.iter().collect::<String>(), expected);
        }

        // Any other key confirms the candidate and leaves completion mode.
        app.handle_line_key(
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
            &commands,
        );
        assert!(app.slots[0].completion.is_none());
        assert_eq!(
            app.slots[0].draft.iter().collect::<String>(),
            "show version "
        );

        // An empty draft completes from the full history, newest first.
        app.slots[0].draft.clear();
        app.slots[0].draft_cursor = 0;
        app.handle_line_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), &commands);
        assert_eq!(
            app.slots[0].draft.iter().collect::<String>(),
            "show version"
        );
    }

    #[test]
    fn enter_send_returns_the_view_to_the_live_tail() {
        let mut app = ready_app_with_control();
        app.slots[0].scroll_from_bottom = 5;
        app.slots[0].unseen = 3;
        app.slots[0].draft = "version".chars().collect();
        app.slots[0].draft_cursor = 7;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert_eq!(app.slots[0].scroll_from_bottom, 0);
        assert_eq!(app.slots[0].unseen, 0);
        assert!(received.try_recv().is_ok());
    }

    #[test]
    fn line_send_uses_the_device_profiles_effective_eol() {
        let mut app = ready_app_with_control();
        app.slots[0].snapshot.effective_write_eol = Some("\n".into());
        app.slots[0].draft = "version".chars().collect();
        app.slots[0].draft_cursor = 7;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        let (_, bytes, _) = take_write(&mut received);
        assert_eq!(bytes, b"version\n");
    }

    #[test]
    fn authoritative_promptless_profile_does_not_revive_legacy_slot_prompts() {
        let mut current = snapshot();
        current.config.settings.shell_prompt = Some("legacy# ".into());
        current.config.settings.uboot_prompt = Some("legacy=> ".into());
        let mut view = SlotView::new(current);

        assert_eq!(view.effective_shell_prompt(), None);
        assert_eq!(view.effective_uboot_prompt(), None);

        // An older daemon omits the entire effective bundle, so compatibility
        // fallback remains available only in that case.
        view.snapshot.effective_write_eol = None;
        view.snapshot.effective_echo = None;
        assert_eq!(view.effective_shell_prompt(), Some("legacy# "));
        assert_eq!(view.effective_uboot_prompt(), Some("legacy=> "));
    }

    #[test]
    fn input_mode_and_command_history_are_isolated_per_slot() {
        let first = snapshot();
        let mut second = snapshot();
        second.config.id = "slot-2".into();
        second.config.display_name = "Slot 2".into();
        second.config.port = "COM4".into();
        let mut app = App::new(vec![first, second], None);
        app.slots[0].mode = InputMode::Raw;
        app.slots[0].history.push("slot-one-command".into());

        app.select(1);
        assert_eq!(app.current_mode(), InputMode::Line);
        app.history_previous();
        assert!(app.current().draft.is_empty());

        app.select(0);
        assert_eq!(app.current_mode(), InputMode::Raw);
        app.history_previous();
        assert_eq!(
            app.current().draft.iter().collect::<String>(),
            "slot-one-command"
        );
    }

    fn snapshot() -> SlotSnapshot {
        SlotSnapshot {
            config: SlotConfig {
                id: "slot-1".into(),
                display_name: "Slot 1".into(),
                port: "COM3".into(),
                profile: "generic-115200".into(),
                enabled: true,
                settings: SerialSettings::default(),
                device_profile: None,
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
        app.authenticated = true;
        app.connection_generation = Some(1);
        app.actor = Some(actor);
        app.slots[0].subscription = SubscriptionPhase::Ready { head_seq: 0 };
        app
    }

    fn ready_app_with_foreign_control() -> App {
        let mut app = ready_app_with_control();
        app.slots[0]
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
            slot_id: "slot-1".into(),
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

    fn stream_row(seq: u64, direction: Direction, text: &str) -> DisplayLine {
        DisplayLine {
            seq,
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
        app.slots[0].push_line(stream_row(1, Direction::Rx, &"x".repeat(2_000)), true);
        app.slots[0].pending_line = Some(stream_row(2, Direction::Rx, "__LATEST_PROMPT__ "));
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
    fn footer_shows_the_active_trigger_state_and_confirmed_fires() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let mut trigger = trigger_info(&app.slots[0].snapshot, TriggerStatus::Running);
        trigger.fires_confirmed = 7;
        let short_id = trigger.id.to_string().chars().take(8).collect::<String>();
        app.slots[0].snapshot.active_trigger = Some(trigger);
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
        assert!(rendered.contains(&format!("trigger {short_id} running")));
        assert!(rendered.contains("7 fire(s)"));
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
        assert!(plain.iter().any(|line| line.contains("强制人工接管")));

        app.help = true;
        app.layout = Some(ConsoleLayout {
            output_area: Rect::new(0, 0, 60, 8),
            output_inner: Rect::new(1, 1, 58, 6),
            input_area: Rect::new(0, 9, 60, 3),
            run_history_area: None,
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
        app.slots[0].snapshot.active_run = Some(agent_run("升级固件"));
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
            epoch: app.slots[0].snapshot.daemon_epoch,
            generation: app.slots[0].snapshot.generation,
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
        app.slots[0].snapshot.active_run = Some(run.clone());
        let daemon_epoch = app.slots[0].snapshot.daemon_epoch;
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

        assert!(app.slots[0].snapshot.active_run.is_none());
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
        assert_eq!(history.commands[0].data, b"show version\r");
        assert!(
            history.commands[0].partial,
            "operation chunks preserve any partial-write outcome"
        );
        assert_eq!(
            history.commands[0].description.as_deref(),
            Some("读取系统版本")
        );
        assert_eq!(history.commands[1].data, b"uname -a\r");
        assert_eq!(view.run_command_keys()[0].first_seq, 4);
        assert_eq!(view.run_command_keys()[1].first_seq, 2);

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
    fn run_sidebar_marks_tail_and_gap_history_as_recent_only() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
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
        assert!(row_text.contains("这里只显示最近记录"));

        app.current_mut().run_history_limited = false;
        app.current_mut()
            .push_gap(10, "test durable journal gap", true);
        assert!(app.current().run_history_limited);
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
            view.run_history_newest_first()[0].id,
            active.id,
            "the authoritative active Run stays at the top"
        );
    }

    #[test]
    fn run_sidebar_selects_commands_and_expands_only_the_confirmed_tx() {
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
            app.slots[0].push_event(tx, true);
        }

        assert_eq!(app.current().selected_run_command_index(), Some(0));
        app.focus = PaneFocus::RunHistory;
        app.handle_run_history_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE));
        assert_eq!(app.current().selected_run_command_index(), Some(1));
        app.handle_run_history_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
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
        assert!(row_text.contains("仅部分字节确认发送"));

        app.focus = PaneFocus::Input;
        app.current_mut().expanded_run_command = None;
        let backend = TestBackend::new(90, 24);
        let mut narrow = Terminal::new(backend).expect("narrow test terminal");
        narrow.draw(|frame| draw(frame, &mut app)).unwrap();
        let hidden = narrow
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!hidden.contains("read system version"));
        assert!(
            app.layout
                .and_then(|layout| layout.run_history_area)
                .is_none()
        );
        app.toggle_run_history_panel();
        narrow.draw(|frame| draw(frame, &mut app)).unwrap();
        let popup = narrow
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
    fn chinese_profile_details_are_readable_without_debug_enum_names() {
        let _guard = crate::i18n::lang_test_lock();
        i18n::set_lang(i18n::Lang::Zh);
        let transport = TransportProfile {
            name: "hardware-test".into(),
            baud_rate: 230_400,
            data_bits: DataBits::Seven,
            parity: Parity::Even,
            stop_bits: StopBits::Two,
            flow_control: FlowControl::Hardware,
            dtr: true,
            rts: false,
            auto_open: true,
        };
        let transport_text = transport_profile_detail(&transport);
        assert!(transport_text.contains("波特率 230400"));
        assert!(transport_text.contains("7 数据位"));
        assert!(transport_text.contains("偶校验"));
        assert!(transport_text.contains("2 停止位"));
        assert!(transport_text.contains("硬件流控"));
        assert!(transport_text.contains("DTR 开启"));
        assert!(transport_text.contains("RTS 关闭"));
        assert!(transport_text.contains("自动打开 启用"));
        assert!(!transport_text.contains("Even"));
        assert!(!transport_text.contains("Hardware"));
        assert!(!transport_text.contains("true"));

        let device = DeviceProfile {
            name: "interaction-test".into(),
            shell_prompt: Some("dut# ".into()),
            uboot_prompt: Some("dut=> ".into()),
            write_eol: Some("\r\n".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(8),
            write_chunk_delay_ms: Some(10),
        };
        let device_text = device_profile_detail(&device);
        assert!(device_text.contains("Shell 提示符 dut#"));
        assert!(device_text.contains("U-Boot 提示符 dut=>"));
        assert!(device_text.contains("换行 CRLF"));
        assert!(device_text.contains("回显自动"));
        assert!(device_text.contains("分段发送：每段 8 字节，间隔 10 毫秒"));
        assert!(!device_text.contains("Auto"));
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
            app.slots[0].push_line(stream_row(seq, Direction::Rx, "short"), true);
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
        app.slots[0].push_line(stream_row(1, Direction::Rx, &"x".repeat(2_000)), true);
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
            app.slots[0].push_line(stream_row(seq, Direction::Rx, &format!("row-{seq}")), true);
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
            app.slots[0].push_line(stream_row(seq, Direction::Rx, &format!("live-{seq}")), true);
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
        second.config.id = "slot-2".into();
        second.config.display_name = "Slot 2".into();
        let mut app = App::new(vec![snapshot(), second], None);
        for slot in &mut app.slots {
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
        assert!(app.slots.iter().all(|slot| slot.scroll_snapshot.is_none()
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
    fn slot_view_keeps_prompt_command_and_echo_on_one_row() {
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
            Some("[root@luckfox tmp]# cd")
        );

        let mut echoed = event(EventKind::Rx, Direction::Rx, 3, b"cd\r\n");
        echoed.daemon_epoch = epoch;
        view.push_event(echoed, true);

        assert_eq!(view.lines.len(), 1);
        assert_eq!(view.lines[0].text, "[root@luckfox tmp]# cd");
        assert_eq!(view.lines[0].marker_color, Some(Color::Green));
        assert!(view.lines[0].echoed);
        assert!(view.pending_line.is_none());
    }

    #[test]
    fn raw_mode_uses_the_same_inline_echo_projection() {
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
        let epoch = app.slots[0].snapshot.daemon_epoch;
        let (commands, _) = mpsc::channel(4);

        app.handle_server_message(
            ServerMessage::ReplayBegin {
                slot_id: "slot-1".into(),
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
                slot_id: "slot-1".into(),
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

        let pwd_rows = app.slots[0]
            .lines
            .iter()
            .filter(|line| line.text.contains("pwd"))
            .collect::<Vec<_>>();
        assert_eq!(pwd_rows.len(), 1);
        assert_eq!(pwd_rows[0].text, "[root@luckfox ~]# pwd");
        assert!(pwd_rows[0].echoed);
        assert!(app.slots[0].lines.iter().any(|line| line.text == "/oem"));
        assert_eq!(
            app.slots[0]
                .pending_line
                .as_ref()
                .map(|line| line.text.as_str()),
            Some("[root@luckfox ~]# ")
        );
    }

    #[test]
    fn ready_preserves_an_in_flight_replay_echo() {
        let mut app = App::new(vec![snapshot()], None);
        let epoch = app.slots[0].snapshot.daemon_epoch;
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
                slot_id: "slot-1".into(),
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

        let pwd_rows = app.slots[0]
            .lines
            .iter()
            .filter(|line| line.text.contains("pwd"))
            .collect::<Vec<_>>();
        assert_eq!(pwd_rows.len(), 1);
        assert_eq!(pwd_rows[0].text, "[root@luckfox ~]# pwd");
        assert!(pwd_rows[0].echoed);
        assert!(app.slots[0].lines.iter().any(|line| line.text == "/oem"));
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
    fn long_line_input_scrolls_horizontally_with_the_cursor() {
        let draft = "abcdef".chars().collect::<Vec<_>>();
        assert_eq!(line_input_projection(&draft, 6, 6), ("> def".into(), 5));
        assert_eq!(line_input_projection(&draft, 2, 6), ("> abcd".into(), 4));
    }

    #[test]
    fn empty_enter_during_foreign_agent_run_only_follows_live_output() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = ready_app_with_foreign_control();
        app.slots[0].snapshot.active_run = Some(agent_run("diagnose boot"));
        app.slots[0].scroll_from_bottom = 5;
        let (commands, mut received) = mpsc::channel(4);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert!(received.try_recv().is_err());
        assert!(!app.pending_writes.contains_key("slot-1"));
        assert_eq!(app.current().scroll_from_bottom, 0);
        assert!(app.status.contains("empty Enter"));
    }

    #[test]
    fn ordinary_enter_keeps_draft_when_local_enqueue_is_rejected() {
        let mut app = ready_app_with_foreign_control();
        app.transport_connected = false;
        app.slots[0].draft = "must survive".chars().collect();
        app.slots[0].draft_cursor = app.slots[0].draft.len();
        let (commands, mut received) = mpsc::channel(1);

        app.handle_line_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &commands);

        assert_eq!(
            app.current().draft.iter().collect::<String>(),
            "must survive"
        );
        assert!(app.current().history.is_empty());
        assert!(!app.pending_writes.contains_key("slot-1"));
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
        app.slots[0].snapshot.active_run = Some(run);
        app.slots[0].snapshot.effective_write_eol = Some("\r\n".into());
        app.slots[0].draft = "show version".chars().collect();
        app.slots[0].draft_cursor = app.slots[0].draft.len();
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
        assert!(!app.pending_writes.contains_key("slot-1"));
        assert!(app.pending_requests.values().any(|request| matches!(
            request,
            PendingRequest::Write {
                slot_id,
                cooperative: true,
                ..
            } if slot_id == "slot-1"
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
        app.slots[0].snapshot.active_run = Some(run);

        let queued_operation = Uuid::new_v4();
        app.pending_writes.insert(
            "slot-1".into(),
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
                slot_id: "slot-1".into(),
                mode: ControlMode::Queue,
            },
        );
        app.queued_controls.insert(
            "slot-1".into(),
            QueuedControl {
                position: 2,
                since: Instant::now(),
            },
        );
        app.slots[0].draft = "cooperative at expiry".chars().collect();
        app.slots[0].draft_cursor = app.slots[0].draft.len();
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
            .get("slot-1")
            .expect("ordinary queue must survive");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].data, b"ordinary queued\r");
        assert_eq!(queue[0].operation_id, Some(queued_operation));
        assert!(matches!(
            app.pending_requests.get(&acquire_request),
            Some(PendingRequest::Acquire { slot_id, .. }) if slot_id == "slot-1"
        ));
        assert_eq!(app.queued_controls["slot-1"].position, 2);
        assert!(!app.pending_requests.contains_key(&request_id));
    }

    #[test]
    fn agent_run_hint_tracks_empty_draft_and_active_run_without_sticky_dismissal() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.slots[0].snapshot.active_run = Some(agent_run("FIRST_AGENT_TASK"));
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
        app.slots[0].snapshot.active_run = None;
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
            Some(MenuPage::Models)
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
        assert!(menu_detail(&app, menu).contains("上下键选择已有方案"));
        let help = help_lines(&app)
            .iter()
            .map(line_plain_text)
            .collect::<Vec<_>>();
        assert!(help.iter().any(|line| line.contains("Ctrl-] o")));
        assert!(help.iter().any(|line| line.contains("Ctrl-] h")));
    }

    #[test]
    fn administrator_token_prompt_is_rendered_only_as_masked_cells() {
        let secret = "ADMIN_TOKEN_MUST_NOT_RENDER";
        let mut app = App::new(vec![snapshot()], None);
        let mut menu = MenuState::new();
        menu.prompt = Some(MenuPrompt {
            title: tr("menu.prompt.admin").into(),
            value: secret.chars().collect(),
            cursor: secret.chars().count(),
            secret: true,
            purpose: MenuPromptPurpose::Admin(MenuAdminMutation::BindDevice {
                slot_id: "slot-1".into(),
                profile_name: None,
            }),
        });
        app.menu = Some(menu);
        let backend = TestBackend::new(100, 30);
        let mut terminal = Terminal::new(backend).expect("test terminal");

        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render masked administrator prompt");

        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains(secret));
        assert!(rendered.contains("••••"));
    }

    #[test]
    fn trusted_local_catalog_skips_the_administrator_prompt() {
        let mut app = App::new(vec![snapshot()], None);
        let (menu_commands, mut menu_received) = mpsc::channel(1);
        app.menu_commands = Some(menu_commands);
        let mut menu = MenuState::new();
        menu.catalog = Some(MenuCatalog {
            auth_required: false,
            slots: vec![snapshot()],
            transport_profiles: Vec::new(),
            device_profiles: Vec::new(),
            models: Vec::new(),
            model_bindings: Vec::new(),
            model_revision: 0,
        });

        app.begin_admin_prompt(
            &mut menu,
            MenuAdminMutation::BindDevice {
                slot_id: "slot-1".into(),
                profile_name: None,
            },
        );

        assert!(menu.prompt.is_none());
        assert!(menu.busy);
        assert!(matches!(
            menu_received.try_recv(),
            Ok(MenuIoCommand::Admin {
                token: None,
                mutation: MenuAdminMutation::BindDevice { .. },
            })
        ));
    }

    #[test]
    fn authenticated_catalog_still_requests_a_masked_one_time_token() {
        let mut app = App::new(vec![snapshot()], None);
        let mut menu = MenuState::new();
        menu.catalog = Some(MenuCatalog {
            auth_required: true,
            slots: vec![snapshot()],
            transport_profiles: Vec::new(),
            device_profiles: Vec::new(),
            models: Vec::new(),
            model_bindings: Vec::new(),
            model_revision: 0,
        });

        app.begin_admin_prompt(
            &mut menu,
            MenuAdminMutation::BindDevice {
                slot_id: "slot-1".into(),
                profile_name: None,
            },
        );

        assert!(menu.prompt.as_ref().is_some_and(|prompt| prompt.secret));
        assert!(!menu.busy);
    }

    #[test]
    fn new_device_profile_clones_effective_behavior_before_echo_or_eol_preset() {
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

        let cloned = current_device_template(&view);
        assert_eq!(cloned.name, "");
        assert_eq!(cloned.shell_prompt.as_deref(), Some("dut# "));
        assert_eq!(cloned.uboot_prompt.as_deref(), Some("dut=> "));
        assert_eq!(cloned.write_eol.as_deref(), Some("\r\n"));
        assert_eq!(cloned.echo, Some(EchoMode::Auto));
        assert_eq!(cloned.write_chunk_size, Some(7));
        assert_eq!(cloned.write_chunk_delay_ms, Some(13));

        let mut echo_off = cloned.clone();
        DevicePreset::Echo(EchoMode::Off).apply(&mut echo_off);
        assert_eq!(echo_off.echo, Some(EchoMode::Off));
        assert_eq!(echo_off.write_eol, cloned.write_eol);
        assert_eq!(echo_off.shell_prompt, cloned.shell_prompt);
        assert_eq!(echo_off.write_chunk_size, cloned.write_chunk_size);

        let mut line_feed = cloned.clone();
        DevicePreset::Eol("\n").apply(&mut line_feed);
        assert_eq!(line_feed.write_eol.as_deref(), Some("\n"));
        assert_eq!(line_feed.echo, cloned.echo);
        assert_eq!(line_feed.uboot_prompt, cloned.uboot_prompt);
    }

    #[test]
    fn model_tree_only_reveals_children_of_expanded_parents() {
        let models = vec![
            DeviceModel {
                id: "tl-as7230".into(),
                name: "TL-AS7230".into(),
                parent_id: None,
                aliases: Vec::new(),
            },
            DeviceModel {
                id: "tl-as7230-w".into(),
                name: "TL-AS7230-W".into(),
                parent_id: Some("tl-as7230".into()),
                aliases: Vec::new(),
            },
            DeviceModel {
                id: "tl-kdp712-d".into(),
                name: "TL-KDP712-D".into(),
                parent_id: Some("tl-as7230-w".into()),
                aliases: Vec::new(),
            },
            DeviceModel {
                id: "other".into(),
                name: "Other".into(),
                parent_id: None,
                aliases: Vec::new(),
            },
        ];

        let collapsed = visible_model_rows(&models, &HashSet::new());
        assert_eq!(
            collapsed
                .iter()
                .map(|row| models[row.index].id.as_str())
                .collect::<Vec<_>>(),
            vec!["tl-as7230", "other"]
        );

        let root_expanded = HashSet::from(["tl-as7230".to_string()]);
        let root_rows = visible_model_rows(&models, &root_expanded);
        assert_eq!(
            root_rows
                .iter()
                .map(|row| (models[row.index].id.as_str(), row.depth))
                .collect::<Vec<_>>(),
            vec![("tl-as7230", 0), ("tl-as7230-w", 1), ("other", 0)]
        );

        let fully_expanded = HashSet::from(["tl-as7230".to_string(), "tl-as7230-w".to_string()]);
        let all_rows = visible_model_rows(&models, &fully_expanded);
        assert_eq!(
            all_rows
                .iter()
                .map(|row| (models[row.index].id.as_str(), row.depth))
                .collect::<Vec<_>>(),
            vec![
                ("tl-as7230", 0),
                ("tl-as7230-w", 1),
                ("tl-kdp712-d", 2),
                ("other", 0),
            ]
        );
    }

    #[test]
    fn queued_line_command_is_visible_in_the_input_title() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        app.pending_writes.insert(
            "slot-1".into(),
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
        let queue = app.pending_writes.entry("slot-1".into()).or_default();
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
        let remaining = queued_line_operations(app.pending_writes.get("slot-1").unwrap())
            .into_iter()
            .map(|operation| String::from_utf8(operation.data).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec!["first\r", "third\r"]);
    }

    #[test]
    fn queue_selector_is_reachable_from_the_prefix_shortcut() {
        let mut app = App::new(vec![snapshot()], None);
        let queue = app.pending_writes.entry("slot-1".into()).or_default();
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
            "slot-1".into(),
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
                slot_id: "slot-1".into(),
                operation_id: Some(sending_id),
                cooperative: false,
            },
        );
        let (commands, _) = mpsc::channel(1);

        app.remove_queued_line_operation(0, true, &commands);
        assert!(app.current().draft.is_empty());
        assert_eq!(
            queued_line_count(app.pending_writes.get("slot-1").unwrap()),
            2
        );
        assert_eq!(app.status, tr("st.queue.already.sending"));

        app.remove_queued_line_operation(1, true, &commands);
        assert_eq!(app.current().draft.iter().collect::<String>(), "editable");
        assert_eq!(
            queued_line_count(app.pending_writes.get("slot-1").unwrap()),
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
        assert_eq!(queued_line_count(&app.pending_writes["slot-1"]), 1);

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
        assert!(rendered.contains("SENDING (locked)"));

        app.handle_result(
            request_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        assert!(!app.pending_writes.contains_key("slot-1"));
        assert!(!app.inflight_writes.contains_key("slot-1"));
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
        assert_eq!(queued_line_count(&app.pending_writes["slot-1"]), 1);
        assert!(app.inflight_writes.is_empty());
        assert!(matches!(received.try_recv(), Ok(NetworkCommand::Shutdown)));

        app.maintain_controls(&commands);

        let (_, data, _) = take_write(&mut received);
        assert_eq!(data, b"retry after full\r");
        assert!(app.pending_requests.values().any(
            |request| matches!(request, PendingRequest::Write { slot_id, cooperative: false, .. } if slot_id == "slot-1")
        ));
        assert_eq!(queued_line_count(&app.pending_writes["slot-1"]), 1);
    }

    #[test]
    fn queue_panel_renders_operations_oldest_first() {
        let _guard = crate::i18n::lang_test_lock();
        let mut app = App::new(vec![snapshot()], None);
        let queue = app.pending_writes.entry("slot-1".into()).or_default();
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
    fn queue_cards_wrap_long_ascii_without_losing_command_text() {
        let mut app = App::new(vec![snapshot()], None);
        let command = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        append_pending_write(
            app.pending_writes.entry("slot-1".into()).or_default(),
            format!("{command}\r").as_bytes(),
            Some(Uuid::new_v4()),
            PendingWriteKind::Line,
        );

        let cards = queue_cards(&app, 14);

        assert_eq!(cards.len(), 1);
        assert!(cards[0].body.len() > 1);
        assert_eq!(cards[0].body.concat(), command);
        assert!(
            cards[0]
                .body
                .iter()
                .all(|row| UnicodeWidthStr::width(row.as_str()) <= 12)
        );
    }

    #[test]
    fn queue_cards_wrap_cjk_by_display_width_without_losing_text() {
        let mut app = App::new(vec![snapshot()], None);
        let command = "中文样机命令参数甲乙丙丁戊己庚辛";
        append_pending_write(
            app.pending_writes.entry("slot-1".into()).or_default(),
            format!("{command}\r").as_bytes(),
            Some(Uuid::new_v4()),
            PendingWriteKind::Line,
        );

        let cards = queue_cards(&app, 12);

        assert_eq!(cards[0].body.concat(), command);
        assert!(
            cards[0]
                .body
                .iter()
                .all(|row| UnicodeWidthStr::width(row.as_str()) <= 10)
        );
    }

    #[test]
    fn short_queue_viewport_pages_selected_command_without_silent_truncation() {
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
            app.pending_writes.entry("slot-1".into()).or_default(),
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
        assert!(first_page.contains("text rows 1-"));

        app.handle_queue_key(
            KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
            &commands,
        );
        terminal.draw(|frame| draw(frame, &mut app)).unwrap();
        let later_page = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!later_page.contains("text rows 1-"));
        assert!(later_page.contains("EEEE") || later_page.contains("FFFF"));
    }

    #[test]
    fn display_column_selection_handles_wrapped_rows_and_cjk() {
        let selection = TextSelection {
            rows: vec![Line::from("  abc"), Line::from("中def")],
            plain_rows: vec!["  abc".into(), "中def".into()],
            anchor: SelectionPoint { row: 0, column: 2 },
            head: SelectionPoint { row: 1, column: 2 },
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
    fn mouse_wheel_scrolls_output_without_browsing_command_history() {
        let mut app = App::new(vec![snapshot()], None);
        for seq in 0..20 {
            app.slots[0].push_line(stream_row(seq, Direction::Rx, "row"), true);
        }
        app.slots[0].history = vec!["first".into(), "second".into()];
        app.slots[0].history_cursor = None;
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

        assert_eq!(app.slots[0].scroll_from_bottom, 3);
        assert_eq!(app.slots[0].history_cursor, None);
        assert!(app.slots[0].draft.is_empty());
    }

    #[test]
    fn completed_selection_resumes_live_output_and_remains_copyable() {
        let mut app = App::new(vec![snapshot()], None);
        app.clipboard_copy = record_clipboard_copy;
        TEST_CLIPBOARD.lock().expect("test clipboard lock").clear();
        app.slots[0].push_line(stream_row(1, Direction::Rx, "abcdef"), true);
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

        assert_eq!(app.focus, PaneFocus::Output);
        assert!(app.selection.is_none());
        assert_eq!(app.selection_copy.as_deref(), Some("abcd"));
        assert_eq!(
            TEST_CLIPBOARD
                .lock()
                .expect("test clipboard lock")
                .as_slice(),
            ["abcd"]
        );

        app.slots[0].push_line(stream_row(2, Direction::Rx, "__AFTER_SELECTION__"), true);
        terminal
            .draw(|frame| draw(frame, &mut app))
            .expect("render live output after selection");
        let rendered = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(rendered.contains("__AFTER_SELECTION__"));
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
        app.slots[0].push_line(stream_row(1, Direction::Rx, "abcdef"), true);
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
        assert!(app.selection.is_none());
        assert_eq!(app.selection_copy.as_deref(), Some("abcd"));
    }

    #[test]
    fn closed_network_event_channel_is_disabled_after_one_observation() {
        let mut app = App::new(vec![snapshot()], None);
        app.transport_connected = true;
        app.authenticated = true;
        app.connection_generation = Some(7);
        let (commands, _) = mpsc::channel(1);

        assert!(!handle_network_channel_event(&mut app, None, &commands));
        assert!(!app.transport_connected);
        assert!(!app.authenticated);
        assert!(app.connection_generation.is_none());
        assert!(matches!(
            app.slots[0].subscription,
            SubscriptionPhase::Disconnected
        ));
        assert!(app.dirty);
    }
}
