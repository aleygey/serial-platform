use std::{
    collections::{HashMap, VecDeque},
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
use serial_protocol::{
    Actor, ClientMessage, CommandResult, ControlLease, ControlMode, EchoMode, EventKind,
    LoggingState, ResolvedDeviceSettings, RunInfo, ServerMessage, SessionState, SlotSnapshot,
    TargetActivity, TimelineEvent, TriggerInfo, TriggerStatus, WireFrame,
};
use tokio::sync::mpsc;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    config::LoadedConfig,
    display::{
        DisplayLine, TerminalStreamParser, gap_line, highlight_spans, pad_display, safe_inline,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputMode {
    Line,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaneFocus {
    Output,
    Input,
}

#[derive(Debug, Clone, Copy)]
struct ConsoleLayout {
    output_area: Rect,
    output_inner: Rect,
    input_area: Rect,
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

#[derive(Debug)]
enum PendingRequest {
    Acquire { slot_id: String },
    Renew { slot_id: String },
    Release { slot_id: String },
    Write { slot_id: String },
}

impl PendingRequest {
    fn slot_id(&self) -> &str {
        match self {
            Self::Acquire { slot_id }
            | Self::Renew { slot_id }
            | Self::Release { slot_id }
            | Self::Write { slot_id } => slot_id,
        }
    }
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
        };
        view.sync_trigger_projection(false);
        view
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
                    trigger.status = if was_stopping || fire_index >= trigger.spec.max_fires {
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
            Some("active")
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
        if self.scroll_from_bottom > 0 {
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
            self.push_line(gap_line(event.seq, tr("st.epoch.changed")), selected);
        }
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
        if completed_pending && (!selected || self.scroll_from_bottom > 0) {
            // The unterminated row was already counted as unseen when it first
            // appeared; committing it must not count the same row twice.
            self.unseen = self.unseen.saturating_sub(1);
        }
        self.pending_line = batch.pending;
        if !had_pending && self.pending_line.is_some() && (!selected || self.scroll_from_bottom > 0)
        {
            self.unseen = self.unseen.saturating_add(1);
        }
    }

    fn push_gap(&mut self, seq: u64, message: impl Into<String>, selected: bool) {
        self.reset_stream();
        self.push_line(gap_line(seq, message), selected);
    }

    fn reset_stream(&mut self) {
        self.stream.reset();
        self.pending_line = None;
    }

    fn follow(&mut self) {
        self.scroll_from_bottom = 0;
        self.unseen = 0;
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

struct App {
    slots: Vec<SlotView>,
    selected: usize,
    prefix_pending: bool,
    /// The prefix key was pressed while dismissing the help overlay. The
    /// following `?` belongs to that same shortcut and must not enter the LINE
    /// draft or reopen help.
    help_dismiss_prefix: bool,
    help: bool,
    detailed_timeline: bool,
    transport_connected: bool,
    authenticated: bool,
    connection_generation: Option<u64>,
    actor: Option<Actor>,
    status: String,
    pending_paste: Option<PendingPaste>,
    pending_writes: HashMap<String, VecDeque<PendingWrite>>,
    pending_requests: HashMap<Uuid, PendingRequest>,
    queued_controls: HashMap<String, QueuedControl>,
    uncertain_write_outcomes: usize,
    human_idle_release: Duration,
    mouse_capture: bool,
    focus: PaneFocus,
    layout: Option<ConsoleLayout>,
    selection: Option<TextSelection>,
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
            detailed_timeline: false,
            transport_connected: false,
            authenticated: false,
            connection_generation: None,
            actor: None,
            status: tr("st.connecting").into(),
            pending_paste: None,
            pending_writes: HashMap::new(),
            pending_requests: HashMap::new(),
            queued_controls: HashMap::new(),
            uncertain_write_outcomes: 0,
            human_idle_release: Duration::from_secs(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS),
            mouse_capture: true,
            focus: PaneFocus::Input,
            layout: None,
            selection: None,
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
                    &[&format!("{role:?}"), &protocol_version.to_string()],
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
                    self.slots[index].snapshot = *slot;
                    self.slots[index].sync_trigger_projection(false);
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
                if let Some(request_id) = request_id {
                    match self.pending_requests.remove(&request_id) {
                        Some(PendingRequest::Acquire { slot_id })
                        | Some(PendingRequest::Write { slot_id }) => {
                            self.queued_controls.remove(&slot_id);
                            let discarded = self
                                .pending_writes
                                .remove(&slot_id)
                                .map_or(0, |writes| writes.len());
                            if discarded > 0 {
                                discarded_suffix =
                                    trf("st.discarded.chunks", &[&slot_id, &discarded.to_string()]);
                            }
                        }
                        _ => {}
                    }
                }
                self.status = format!(
                    "{:?}: {message}{discarded_suffix}{}",
                    code,
                    if retryable { tr("st.retryable") } else { "" }
                );
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
                            &format!("{reason:?}"),
                            &format!("{requested_after_seq:?}"),
                            &format!("{first_available_seq:?}"),
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
                if let Some(PendingRequest::Acquire { slot_id }) = pending {
                    self.queued_controls.remove(&slot_id);
                    self.install_lease(&slot_id, lease);
                    self.status = trf("st.granted", &[&slot_id]);
                    self.flush_pending_writes(&slot_id, commands);
                }
            }
            CommandResult::ControlQueued { position } => {
                if let Some(PendingRequest::Acquire { slot_id }) = pending {
                    self.queued_controls.insert(
                        slot_id.clone(),
                        QueuedControl {
                            position,
                            since: Instant::now(),
                        },
                    );
                    self.pending_requests
                        .insert(request_id, PendingRequest::Acquire { slot_id });
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
            CommandResult::AcquireCancelled { .. } => {
                if let Some(PendingRequest::Acquire { slot_id }) = pending {
                    self.queued_controls.remove(&slot_id);
                    self.status = trf("st.acquire.cancelled", &[&slot_id]);
                }
            }
            CommandResult::WriteAccepted { event_seq } => {
                if let Some(PendingRequest::Write { slot_id }) = pending {
                    self.status = trf("st.write.confirmed", &[&slot_id, &event_seq.to_string()]);
                    self.flush_pending_writes(&slot_id, commands);
                }
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
                self.status = trf("st.authenticated", &[&format!("{role:?}")]);
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
            self.slots[index].push_event(event, selected);
            if self.slots[index].subscription.is_ready() && self.owns_control(index) {
                self.queued_controls.remove(&slot_id);
                self.pending_requests.retain(|_, request| {
                    !matches!(request, PendingRequest::Acquire { slot_id: pending } if pending == &slot_id)
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
                }
            }
            EventKind::SlotRemoved => {
                snapshot.endpoint_present = false;
                snapshot.session_state = SessionState::Disabled;
                snapshot.state_reason = Some("removed from active configuration".into());
                snapshot.target_activity = TargetActivity::Unknown;
                snapshot.control = None;
                snapshot.active_run = None;
                snapshot.active_trigger = None;
                slot.clear_trigger_projection();
            }
            EventKind::Tx => slot.observe_trigger_tx(event),
            EventKind::Checkpoint => {}
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

    fn request_write_batch(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        writes: Vec<Vec<u8>>,
        operation_id: Option<Uuid>,
    ) -> bool {
        self.request_write_batch_with_kind(commands, writes, operation_id, PendingWriteKind::Line)
    }

    fn request_write_batch_with_kind(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        writes: Vec<Vec<u8>>,
        operation_id: Option<Uuid>,
        kind: PendingWriteKind,
    ) -> bool {
        if writes.iter().all(Vec::is_empty) {
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
        let total_new_bytes = writes
            .iter()
            .fold(0usize, |total, write| total.saturating_add(write.len()));
        let previous_slot_writes = self
            .pending_writes
            .get(&slot_id)
            .cloned()
            .unwrap_or_default();
        let previous_slot_count = previous_slot_writes.len();
        let mut candidate_slot_writes = previous_slot_writes;
        for write in writes.iter().filter(|write| !write.is_empty()) {
            append_pending_write(&mut candidate_slot_writes, write, operation_id, kind);
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
            return self.flush_pending_writes(&slot_id, commands);
        }

        let acquire_already_pending = self.pending_requests.values().any(|request| {
            matches!(request, PendingRequest::Acquire { slot_id: pending } if pending == &slot_id)
        });
        if !acquire_already_pending && !self.acquire_control(commands, ControlMode::Queue) {
            self.pending_writes.remove(&slot_id);
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
        self.release_slot_control(commands, slot_id, lease, false);
    }

    fn has_queued_control(&self, slot_id: &str) -> bool {
        self.queued_controls.contains_key(slot_id)
            || self.pending_writes.contains_key(slot_id)
            || self.pending_requests.values().any(
                |request| matches!(request, PendingRequest::Acquire { slot_id: pending } if pending == slot_id),
            )
    }

    fn cancel_queued_control(
        &mut self,
        commands: &mpsc::Sender<NetworkCommand>,
        slot_id: &str,
        reason: &str,
    ) {
        let reconnect_reason = trf("st.reconnect.reason", &[reason, slot_id]);
        match commands.try_send(NetworkCommand::Reconnect {
            reason: reconnect_reason.clone(),
        }) {
            Ok(()) => {
                self.pending_writes.clear();
                self.queued_controls.clear();
                self.pending_requests
                    .retain(|_, request| !matches!(request, PendingRequest::Acquire { .. }));
                self.pending_paste = None;
                self.status = reconnect_reason;
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.status = tr("st.cancel.full").into();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.status = tr("st.cancel.stopped").into();
            }
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
            let operation_pending = self.pending_writes.contains_key(&slot_id)
                || self.pending_requests.values().any(
                    |request| matches!(request, PendingRequest::Write { slot_id: pending } if pending == &slot_id),
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
            matches!(request, PendingRequest::Write { slot_id: pending } if pending == slot_id)
        });
        if write_already_pending {
            return true;
        }
        let write = self
            .pending_writes
            .get_mut(slot_id)
            .and_then(VecDeque::pop_front);
        if self
            .pending_writes
            .get(slot_id)
            .is_some_and(VecDeque::is_empty)
        {
            self.pending_writes.remove(slot_id);
        }
        if let Some(write) = write
            && !self.send_write_now(commands, slot_id, write.data, write.operation_id)
        {
            self.pending_writes.remove(slot_id);
            return false;
        }
        true
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
            },
            Some(PendingRequest::Write {
                slot_id: slot_id.to_string(),
            }),
        )
    }

    fn handle_terminal_event(&mut self, event: Event, commands: &mpsc::Sender<NetworkCommand>) {
        match event {
            Event::Key(key) if matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) => {
                self.handle_key(key, commands)
            }
            Event::Paste(value) => self.handle_paste(value, commands),
            Event::Mouse(mouse) => self.handle_mouse(mouse, commands),
            Event::Resize(_, _) => {
                self.selection = None;
                self.dirty = true;
            }
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        self.focus = PaneFocus::Input;
        self.selection = None;
        if self.help {
            self.help = false;
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

        match self.current_mode() {
            InputMode::Line => self.handle_line_key(key, commands),
            InputMode::Raw => self.handle_raw_key(key, commands),
        }
        self.dirty = true;
    }

    fn handle_mouse(&mut self, mouse: MouseEvent, commands: &mpsc::Sender<NetworkCommand>) {
        match mouse.kind {
            MouseEventKind::ScrollUp => {
                self.selection = None;
                self.scroll_up(3);
            }
            MouseEventKind::ScrollDown => {
                self.selection = None;
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

    fn begin_mouse_selection(&mut self, mouse: MouseEvent) {
        let Some(layout) = self.layout else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if rect_contains(layout.input_area, position) {
            self.focus = PaneFocus::Input;
            self.selection = None;
            return;
        }
        if !rect_contains(layout.output_area, position) {
            return;
        }
        self.focus = PaneFocus::Output;
        if !rect_contains(layout.output_inner, position) {
            self.selection = None;
            return;
        }
        let rows = visible_output_lines(self, layout.output_inner);
        let Some(point) = selection_point(layout.output_inner, position, rows.len()) else {
            self.selection = None;
            return;
        };
        let plain_rows = rows.iter().map(line_plain_text).collect();
        self.selection = Some(TextSelection {
            rows,
            plain_rows,
            anchor: point,
            head: point,
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
        }
    }

    fn finish_mouse_selection(&mut self, mouse: MouseEvent) {
        self.update_mouse_selection(mouse);
        if self
            .selection
            .as_ref()
            .is_some_and(|selection| !selection.is_dragged())
        {
            self.selection = None;
        }
    }

    fn handle_right_click(&mut self, mouse: MouseEvent, commands: &mpsc::Sender<NetworkCommand>) {
        let Some(layout) = self.layout else {
            return;
        };
        let position = Position::new(mouse.column, mouse.row);
        if rect_contains(layout.output_area, position) {
            self.focus = PaneFocus::Output;
            let Some(selection) = self.selection.take() else {
                return;
            };
            let text = selection.selected_text();
            if text.is_empty() {
                return;
            }
            self.status = match crate::clipboard::copy_text(&text) {
                Ok(()) => trf("st.clipboard.copied", &[&text.chars().count().to_string()]),
                Err(error) => trf("st.clipboard.copy.failed", &[&error.to_string()]),
            };
            return;
        }
        if !rect_contains(layout.input_area, position) {
            return;
        }
        self.focus = PaneFocus::Input;
        self.selection = None;
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
                self.detailed_timeline = !self.detailed_timeline;
                self.status = if self.detailed_timeline {
                    tr("st.detailed").into()
                } else {
                    tr("st.compact").into()
                };
            }
            KeyCode::Char('g' | 'G') => self.toggle_language(),
            KeyCode::PageUp => self.scroll_up(10),
            KeyCode::PageDown => self.scroll_down(10),
            KeyCode::Char('t' | 'T') => {
                self.acquire_control(commands, ControlMode::Takeover);
            }
            KeyCode::Char('c' | 'C') => self.release_control(commands),
            KeyCode::Char('p' | 'P') => self.confirm_paste(commands),
            KeyCode::Char('/') => {
                self.status = tr("st.logs.hint").into();
            }
            KeyCode::Char('?') => self.help = true,
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
                let mut bytes = value.as_bytes().to_vec();
                bytes.extend_from_slice(self.current().effective_write_eol().as_bytes());
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
                self.request_write(commands, bytes, Some(Uuid::new_v4()));
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
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.current_mut().draft.clear();
                self.current_mut().draft_cursor = 0;
                self.status = tr("st.input.cleared").into();
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
            self.request_write_batch(commands, writes, Some(Uuid::new_v4()))
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

    /// Ctrl-] g: switch between English and Chinese at runtime and persist
    /// the choice to the client config on a best-effort basis.
    fn toggle_language(&mut self) {
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
        let max = self.current().logical_line_count().saturating_sub(1);
        let view = self.current_mut();
        view.scroll_from_bottom = (view.scroll_from_bottom + amount).min(max);
    }

    fn scroll_down(&mut self, amount: usize) {
        let view = self.current_mut();
        view.scroll_from_bottom = view.scroll_from_bottom.saturating_sub(amount);
        if view.scroll_from_bottom == 0 {
            view.unseen = 0;
        }
    }
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
) -> Result<()> {
    let mut terminal_events = EventStream::new();
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
            event = network_events.recv() => match event {
                Some(event) => app.handle_network(event, commands),
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
                }
            },
            _ = renew_tick.tick() => app.maintain_controls(commands),
            _ = activity_tick.tick() => {
                let now = Instant::now();
                let mut trigger_changed = false;
                for slot in &mut app.slots {
                    trigger_changed |= slot.update_trigger_deadline(now);
                }
                if trigger_changed || app.slots.iter().any(|slot| {
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

fn local_history_truncated_message() -> &'static str {
    match i18n::lang() {
        i18n::Lang::En => {
            "Local display history was truncated; use `serialctl logs` for the complete history."
        }
        i18n::Lang::Zh => "本地显示历史已截断；完整历史请使用 `serialctl logs` 查询。",
    }
}

fn local_history_truncated_title() -> &'static str {
    match i18n::lang() {
        i18n::Lang::En => " · LOCAL HISTORY TRUNCATED",
        i18n::Lang::Zh => " · 本地历史已截断",
    }
}

fn draw(frame: &mut Frame<'_>, app: &mut App) {
    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(4),
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .split(area);
    let output_area = chunks[1];
    let input_area = chunks[3];
    app.layout = Some(ConsoleLayout {
        output_area,
        output_inner: inset_border(output_area),
        input_area,
    });

    draw_tabs(frame, app, chunks[0]);
    draw_output(frame, app, chunks[1]);
    draw_status(frame, app, chunks[2]);
    draw_input(frame, app, chunks[3]);
    draw_help_line(frame, app, chunks[4]);
    if app.help {
        draw_help(frame, app, area);
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
    let title = format!(
        " {} · {} · {} baud{}{} ",
        safe_inline(&view.snapshot.config.display_name),
        safe_inline(&view.snapshot.config.port),
        view.snapshot.config.settings.baud_rate,
        if view.scroll_from_bottom > 0 {
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
    let truncation_line = view.local_truncation_line();
    let total_lines = view.logical_line_count();
    // Clamp the paused offset so a vanished pending row can never produce an
    // empty viewport; push_line already keeps the offset anchored on append
    // and front-eviction.
    let scroll = view.scroll_from_bottom.min(total_lines.saturating_sub(1));
    let end = total_lines.saturating_sub(scroll);
    let visible_height = inner.height as usize;
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
) -> Line<'static> {
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
        |request| matches!(request, PendingRequest::Acquire { slot_id: pending } if pending == slot_id),
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

fn draw_input(frame: &mut Frame<'_>, app: &App, area: Rect) {
    if let Some(search) = &app.current().history_search {
        let matched = search
            .match_index
            .map(|index| safe_inline(&app.current().history[index]))
            .unwrap_or_default();
        let text = format!("(reverse-i-search)`{}': {matched}", search.query);
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
    let (text, cursor_column, title) = match app.current_mode() {
        InputMode::Line => {
            let (text, cursor_column) = line_input_projection(
                &app.current().draft,
                app.current().draft_cursor,
                inner.width,
            );
            (text, Some(cursor_column), tr("ui.input.title.line"))
        }
        InputMode::Raw => (
            format!("> {}", tr("ui.input.raw.text")),
            None,
            tr("ui.input.title.raw"),
        ),
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
    let width = area.width.min(76);
    let height = area.height.min(32);
    let popup = centered_rect(width, height, area);
    let idle_seconds = app.human_idle_release.as_secs().to_string();
    let mouse_help = if app.mouse_capture {
        tr("help.wheel")
    } else {
        tr("help.selection")
    };
    let help = [
        tr("help.all.modes").to_string(),
        tr("help.switch").to_string(),
        tr("help.next").to_string(),
        tr("help.mode").to_string(),
        tr("help.view").to_string(),
        tr("help.lang").to_string(),
        tr("help.scroll").to_string(),
        mouse_help.to_string(),
        tr("help.mouse.paste").to_string(),
        tr("help.takeover").to_string(),
        tr("help.release").to_string(),
        tr("help.follow").to_string(),
        tr("help.echo").to_string(),
        tr("help.paste").to_string(),
        tr("help.byte").to_string(),
        tr("help.quit").to_string(),
        String::new(),
        tr("help.line1").to_string(),
        tr("help.line2").to_string(),
        tr("help.line3").to_string(),
        tr("help.raw1").to_string(),
        tr("help.raw2").to_string(),
        tr("help.paste.note").to_string(),
        trf("help.expire", &[&idle_seconds]),
        tr("help.replay").to_string(),
        tr("help.uncertain").to_string(),
        String::new(),
        tr("help.close").to_string(),
    ];
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(help.join("\n"))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(tr("help.title")),
            )
            .wrap(Wrap { trim: false }),
        popup,
    );
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
    use std::collections::BTreeMap;

    use crossterm::event::KeyEvent;
    use ratatui::backend::TestBackend;
    use serial_protocol::{ActorKind, Direction, SerialSettings, SlotConfig, TriggerSpec};

    use super::*;

    #[test]
    fn raw_ctrl_c_is_etx_and_arrows_are_xterm() {
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Some(vec![0x03])
        );
        assert_eq!(
            raw_key_bytes(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)),
            Some(b"\x1b[A".to_vec())
        );
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
    fn max_fire_and_local_timeout_project_stopping_before_terminal_event() {
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

        assert_eq!(app.slots[0].trigger_status_text(), Some("active"));
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
        assert_eq!(app.pending_writes["slot-1"].len(), 2);
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
        assert_eq!(app.pending_writes["slot-1"].len(), 1);

        app.handle_result(
            second_id,
            CommandResult::WriteAccepted { event_seq: 2 },
            &commands,
        );
        let (third_id, third_data, third_operation) = take_write(&mut received);
        assert_ne!(second_id, third_id);
        assert_eq!(third_data.len(), 17);
        assert_eq!(third_operation, Some(operation_id));
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
        assert_eq!(app.pending_writes["slot-1"].len(), 1);

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
        assert_eq!(app.pending_writes["slot-1"].len(), 1);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (second_id, second_data, second_operation) = take_write(&mut received);
        assert_ne!(first_id, second_id);
        assert_eq!(second_data, b"x\r");
        assert_eq!(second_operation, Some(operation_id));
        assert!(!app.pending_writes.contains_key("slot-1"));
    }

    #[test]
    fn confirmed_multiline_paste_queues_distinct_commands_in_one_operation() {
        let mut app = ready_app_with_control();
        let (commands, mut received) = mpsc::channel(8);
        app.pending_paste = Some(PendingPaste {
            slot_id: "slot-1".into(),
            bytes: b"pwd\nversion\n".to_vec(),
            raw: false,
        });

        app.confirm_paste(&commands);

        let (first_id, first_data, operation_id) = take_write(&mut received);
        let operation_id = operation_id.expect("line paste operation ID");
        assert_eq!(first_data, b"pwd\r");
        assert_eq!(app.pending_writes["slot-1"].len(), 1);

        app.handle_result(
            first_id,
            CommandResult::WriteAccepted { event_seq: 1 },
            &commands,
        );
        let (_, second_data, second_operation) = take_write(&mut received);
        assert_eq!(second_data, b"version\r");
        assert_eq!(second_operation, Some(operation_id));
        assert!(!app.pending_writes.contains_key("slot-1"));
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
        assert!(!app.pending_writes.contains_key("slot-1"));
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
        assert_eq!(
            snapshot.state_reason.as_deref(),
            Some("removed from active configuration")
        );
        assert_eq!(snapshot.target_activity, TargetActivity::Unknown);
        assert!(!snapshot.endpoint_present);
        assert!(snapshot.control.is_none());
        assert!(snapshot.active_run.is_none());
        assert!(snapshot.active_trigger.is_none());
    }

    #[test]
    fn queued_control_can_be_cancelled_and_forces_actor_reconnect() {
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
            },
        );
        let (commands, mut received) = mpsc::channel(4);

        app.release_control(&commands);

        assert!(matches!(
            received.try_recv(),
            Ok(NetworkCommand::Reconnect { reason }) if reason.contains("cancelled queued input")
        ));
        assert!(app.pending_writes.is_empty());
        assert!(app.queued_controls.is_empty());
        assert!(
            app.pending_requests
                .values()
                .all(|request| !matches!(request, PendingRequest::Acquire { .. }))
        );
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
            target_activity: TargetActivity::Unknown,
            last_rx_wall_time_ns: None,
            rx_offset: 0,
            tx_offset: 0,
            control: None,
            active_run: None,
            active_trigger: None,
            logging: LoggingState::Healthy,
            effective_shell_prompt: None,
            effective_uboot_prompt: None,
            effective_write_eol: Some("\r".into()),
            effective_echo: Some(EchoMode::On),
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
    fn front_eviction_while_scrolled_keeps_the_offset_in_bounds() {
        let mut view = SlotView::new(snapshot());
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
        assert!(view.local_truncation_line().is_some());
    }

    #[test]
    fn local_history_eviction_keeps_an_authoritative_truncation_boundary() {
        let mut view = SlotView::new(snapshot());
        for seq in 0..=MAX_LINES_PER_SLOT as u64 {
            view.push_line(stream_row(seq, Direction::Rx, "row"), true);
        }

        assert_eq!(view.lines.len(), MAX_LINES_PER_SLOT);
        assert!(view.local_history_truncated);
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
    fn display_column_selection_handles_wrapped_rows_and_cjk() {
        let selection = TextSelection {
            rows: vec![Line::from("  abc"), Line::from("中def")],
            plain_rows: vec!["  abc".into(), "中def".into()],
            anchor: SelectionPoint { row: 0, column: 2 },
            head: SelectionPoint { row: 1, column: 2 },
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
    fn mouse_click_changes_focus_and_left_drag_selects_output_without_shift() {
        let mut app = App::new(vec![snapshot()], None);
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
        assert_eq!(
            app.selection
                .as_ref()
                .map(TextSelection::selected_text)
                .as_deref(),
            Some("abcd")
        );

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
    }
}
