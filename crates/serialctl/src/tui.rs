use std::{
    collections::{HashMap, VecDeque},
    io::{self, Write},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use crossterm::{
    event::{
        DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
        Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEventKind,
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
    Actor, ClientMessage, CommandResult, ControlLease, ControlMode, EventKind, LoggingState,
    RunInfo, ServerMessage, SessionState, SlotSnapshot, TargetActivity, TimelineEvent, WireFrame,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
    api::ApiClient,
    config::LoadedConfig,
    display::{DisplayLine, TerminalStreamParser, gap_line, highlight_spans, safe_inline},
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

#[derive(Debug)]
struct PendingWrite {
    data: Vec<u8>,
    operation_id: Option<Uuid>,
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
            Self::Disconnected => "OFF".into(),
            Self::Attaching => "ATTACH".into(),
            Self::Replaying {
                from_seq,
                through_seq,
            } => format!("REPLAY#{from_seq}-#{through_seq}"),
            Self::Ready { head_seq } => format!("LIVE#{head_seq}"),
            Self::Lagged { from_seq, to_seq } => format!("LAGGED#{from_seq}-#{to_seq}"),
        }
    }

    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready { .. })
    }
}

struct SlotView {
    snapshot: SlotSnapshot,
    subscription: SubscriptionPhase,
    lines: VecDeque<DisplayLine>,
    pending_line: Option<DisplayLine>,
    stream: TerminalStreamParser,
    buffered_bytes: usize,
    scroll_from_bottom: usize,
    unseen: usize,
    last_epoch: Option<Uuid>,
    last_seq: u64,
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
        Self {
            last_epoch: Some(snapshot.daemon_epoch),
            last_seq: 0,
            snapshot,
            subscription: SubscriptionPhase::Disconnected,
            lines: VecDeque::new(),
            pending_line: None,
            stream: TerminalStreamParser::new(),
            buffered_bytes: 0,
            scroll_from_bottom: 0,
            unseen: 0,
            draft: Vec::new(),
            draft_cursor: 0,
            mode: InputMode::Line,
            history: Vec::new(),
            history_cursor: None,
            history_search: None,
            completion: None,
            last_manual_activity: None,
        }
    }

    fn push_line(&mut self, line: DisplayLine, selected: bool) {
        self.buffered_bytes += line.bytes;
        self.lines.push_back(line);
        while self.lines.len() > MAX_LINES_PER_SLOT || self.buffered_bytes > MAX_BYTES_PER_SLOT {
            let Some(removed) = self.lines.pop_front() else {
                break;
            };
            self.buffered_bytes = self.buffered_bytes.saturating_sub(removed.bytes);
        }
        if !selected || self.scroll_from_bottom > 0 {
            self.unseen = self.unseen.saturating_add(1);
        }
    }

    fn push_event(&mut self, event: TimelineEvent, selected: bool) {
        if self.last_epoch == Some(event.daemon_epoch) && event.seq <= self.last_seq {
            return;
        }
        if self.last_epoch.is_some() && self.last_epoch != Some(event.daemon_epoch) {
            self.reset_stream();
            self.push_line(
                gap_line(
                    event.seq,
                    "daemon epoch changed; previous control leases and cursors are invalid",
                ),
                selected,
            );
        }
        self.last_epoch = Some(event.daemon_epoch);
        self.last_seq = event.seq;
        let had_pending = self.pending_line.is_some();
        let batch = self.stream.push_event(&event);
        let completed_pending = had_pending && !batch.completed.is_empty();
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
            help: false,
            detailed_timeline: false,
            transport_connected: false,
            authenticated: false,
            connection_generation: None,
            actor: None,
            status: "connecting…".into(),
            pending_paste: None,
            pending_writes: HashMap::new(),
            pending_requests: HashMap::new(),
            queued_controls: HashMap::new(),
            uncertain_write_outcomes: 0,
            human_idle_release: Duration::from_secs(DEFAULT_HUMAN_IDLE_RELEASE_SECONDS),
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
            self.status = format!(
                "viewing {} ({})",
                self.current().snapshot.config.display_name,
                self.current().snapshot.config.port
            );
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
                self.status = "transport connected; authenticating and attaching all Slots".into();
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
                    format!("disconnected: {reason}; reconnecting")
                } else {
                    format!(
                        "disconnected: {reason}; {newly_uncertain} sent write outcome(s) uncertain; inspect TX before retrying"
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
                self.status = format!("connected as {:?} (protocol v{protocol_version})", role);
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
                            "the serial session changed before queued input was sent",
                        );
                        self.slots[index].reset_stream();
                    }
                    self.slots[index].snapshot = *slot;
                    self.slots[index].subscription = SubscriptionPhase::Attaching;
                    if epoch_changed {
                        let selected = self.selected == index;
                        let seq = self.slots[index].snapshot.head_seq;
                        self.slots[index].push_gap(
                            seq,
                            "daemon restarted; old control leases were invalidated",
                            selected,
                        );
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
                                    format!("; {slot_id}: discarded {discarded} queued chunk(s)");
                            }
                        }
                        _ => {}
                    }
                }
                self.status = format!(
                    "{:?}: {message}{discarded_suffix}{}",
                    code,
                    if retryable { " (retryable)" } else { "" }
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
                    format!(
                        "history gap ({reason:?}); requested after {:?}, first available {:?}",
                        requested_after_seq, first_available_seq
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
                    format!(
                        "slow client missed live events {from_seq}..={to_seq}; reconnecting for journal replay"
                    ),
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
                self.status = format!("replaying {slot_id} #{from_seq}..=#{through_seq}");
            }
            ServerMessage::Ready { slot_id, head_seq } => {
                if let Some(index) = self.slot_index(&slot_id) {
                    self.slots[index].subscription = SubscriptionPhase::Ready { head_seq };
                    if self.owns_control(index) {
                        self.flush_pending_writes(&slot_id, commands);
                    }
                }
                self.status = format!("{slot_id} live at sequence {head_seq}");
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
                    self.status = format!("write control granted for {slot_id}");
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
                self.status =
                    format!("write control queued at position {position}; input is held locally");
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
                    self.status = format!("write control released for {slot_id}");
                }
            }
            CommandResult::WriteAccepted { event_seq } => {
                if let Some(PendingRequest::Write { slot_id }) = pending {
                    self.status = format!("{slot_id}: write confirmed at sequence {event_seq}");
                    self.flush_pending_writes(&slot_id, commands);
                }
            }
            CommandResult::HelloAccepted { actor, role } => {
                self.actor = Some(actor);
                self.authenticated = true;
                self.status = format!("authenticated as {:?}", role);
            }
            CommandResult::Attached { slots } => {
                self.status = format!("watching {} Slot(s)", slots.len());
            }
            CommandResult::Detached { slots } => {
                self.status = format!("detached {} Slot(s)", slots.len());
            }
            CommandResult::Pong { .. } => {}
            CommandResult::RunStarted { run } => {
                self.status = format!("run started: {}", run.label);
            }
            CommandResult::RunEnded { run } => {
                self.status = format!("run ended: {}", run.label);
            }
            CommandResult::CheckpointCreated { event_seq } => {
                self.status = format!("checkpoint created at sequence {event_seq}");
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
            if generation_changed
                || matches!(
                    event.kind,
                    EventKind::SerialClosed | EventKind::SlotReconfigured | EventKind::SlotRemoved
                )
            {
                self.invalidate_slot_pending(
                    &slot_id,
                    "the serial session changed; queued input was discarded",
                );
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
        let snapshot = &mut self.slots[index].snapshot;
        snapshot.head_seq = snapshot.head_seq.max(event.seq);
        snapshot.generation = event.generation;
        snapshot.logging = if event.durable {
            snapshot.logging
        } else {
            LoggingState::Degraded
        };
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
                snapshot.control = None
            }
            EventKind::RunStarted => {
                snapshot.active_run = event
                    .metadata
                    .get("run")
                    .and_then(|value| serde_json::from_value::<RunInfo>(value.clone()).ok());
            }
            EventKind::RunEnded | EventKind::RunAborted => snapshot.active_run = None,
            EventKind::LoggingDegraded | EventKind::Gap => {
                snapshot.logging = LoggingState::Degraded;
            }
            EventKind::SlotReconfigured => {
                if let Some(config) = event
                    .metadata
                    .get("current")
                    .and_then(|value| serde_json::from_value(value.clone()).ok())
                {
                    snapshot.config = config;
                }
            }
            EventKind::SlotRemoved => {
                snapshot.endpoint_present = false;
                snapshot.session_state = SessionState::Disabled;
                snapshot.state_reason = Some("removed from active configuration".into());
                snapshot.target_activity = TargetActivity::Unknown;
                snapshot.control = None;
                snapshot.active_run = None;
            }
            EventKind::Tx | EventKind::Checkpoint => {}
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
            self.status = format!(
                "{slot_id}: {reason} ({discarded_writes} write(s), {discarded_requests} request(s))"
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
            self.status = "connection is not authenticated; input was not queued".into();
            return false;
        }
        let Some(generation) = self.connection_generation else {
            self.status = "not connected; input was not queued".into();
            return false;
        };
        let request_id = message.request_id();
        if pending.is_some() && self.pending_requests.len() >= MAX_OUTSTANDING_REQUESTS {
            self.status = "too many outstanding daemon requests; input was not sent".into();
            return false;
        }
        match commands.try_send(NetworkCommand::Send {
            generation,
            message,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.status = "outbound queue is full; input was not sent".into();
                return false;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.status = "network worker stopped".into();
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
        if data.is_empty() {
            return true;
        }
        if !self.transport_connected || !self.authenticated {
            self.status = "not authenticated; input was not queued".into();
            return false;
        }
        if !self.slot_ready(self.selected) {
            self.status = format!(
                "{} is not live yet; input was not queued",
                self.selected_slot_id()
            );
            return false;
        }
        let slot_id = self.selected_slot_id();
        let chunks = data
            .chunks(MAX_WRITE_BYTES)
            .map(|chunk| PendingWrite {
                data: chunk.to_vec(),
                operation_id,
            })
            .collect::<Vec<_>>();

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
        if total_pending + chunks.len() > MAX_PENDING_WRITES
            || total_bytes + data.len() > MAX_PENDING_BYTES
        {
            self.status = "local write queue is full; input was not queued".into();
            return false;
        }
        self.pending_writes
            .entry(slot_id.clone())
            .or_default()
            .extend(chunks);
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
            self.status =
                "the selected Slot is not authenticated and live; control was not requested".into();
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
                ControlMode::Queue => format!("requesting write control for {slot_id}…"),
                ControlMode::Takeover => format!("requesting explicit takeover of {slot_id}…"),
            };
            true
        } else {
            false
        }
    }

    fn release_control(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        if !self.transport_connected || !self.authenticated || !self.slot_ready(self.selected) {
            self.status = "the selected Slot is not live; control was not released".into();
            return;
        }
        let slot_id = self.selected_slot_id();
        if !self.owns_control(self.selected) && self.has_queued_control(&slot_id) {
            self.cancel_queued_control(commands, &slot_id, "operator cancelled queued input");
            return;
        }
        let Some(lease) = self.current().snapshot.control.clone() else {
            self.status = "this Slot has no active write control".into();
            return;
        };
        if !self.owns_control(self.selected) {
            self.status = format!("write control belongs to {}", lease.owner.label);
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
        let reconnect_reason = format!(
            "{reason} for {slot_id}; reconnecting cancels this actor's queues and releases its controls on every Slot"
        );
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
                self.status = "cannot cancel queued control: outbound queue is full".into();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.status = "cannot cancel queued control: network worker stopped".into();
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
            self.status = format!(
                "{slot_id}: releasing idle human control after {} seconds",
                self.human_idle_release.as_secs()
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
                &format!(
                    "queued human input expired after {} seconds of inactivity",
                    idle_release.as_secs()
                ),
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
            self.status = "write control disappeared before send".into();
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
            Event::Mouse(mouse) => {
                match mouse.kind {
                    MouseEventKind::ScrollUp => self.scroll_up(3),
                    MouseEventKind::ScrollDown => self.scroll_down(3),
                    _ => {}
                }
                self.dirty = true;
            }
            Event::Resize(_, _) => self.dirty = true,
            _ => {}
        }
    }

    fn handle_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        if self.help {
            self.help = false;
            self.dirty = true;
            return;
        }
        if self.prefix_pending {
            self.prefix_pending = false;
            self.handle_prefix_key(key, commands);
            self.dirty = true;
            return;
        }
        if is_prefix(key) {
            self.prefix_pending = true;
            self.status = "command prefix: 1-9 Slot, l LINE, r RAW, PgUp/PgDn scroll, v detail, t takeover, c release/cancel, ? help".into();
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

    fn handle_prefix_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        match key.code {
            KeyCode::Char(digit @ '1'..='9') => {
                self.select((digit as usize) - ('1' as usize));
            }
            KeyCode::Char('s' | 'S') => self.select((self.selected + 1) % self.slots.len()),
            KeyCode::Char('l' | 'L') => {
                self.current_mut().mode = InputMode::Line;
                self.status = "LINE mode: Enter sends the line plus Profile EOL".into();
            }
            KeyCode::Char('r' | 'R') => {
                self.current_mut().mode = InputMode::Raw;
                self.status = "RAW mode: keystrokes are sent directly; Ctrl-] remains local".into();
            }
            KeyCode::Char('f' | 'F') | KeyCode::End => {
                self.current_mut().follow();
                self.status = "following live output".into();
            }
            KeyCode::Char('v' | 'V') => {
                self.detailed_timeline = !self.detailed_timeline;
                self.status = if self.detailed_timeline {
                    "detailed timeline: #seq and source columns shown".into()
                } else {
                    "compact timeline: markers and inline highlighting".into()
                };
            }
            KeyCode::PageUp => self.scroll_up(10),
            KeyCode::PageDown => self.scroll_down(10),
            KeyCode::Char('t' | 'T') => {
                self.acquire_control(commands, ControlMode::Takeover);
            }
            KeyCode::Char('c' | 'C') => self.release_control(commands),
            KeyCode::Char('p' | 'P') => self.confirm_paste(commands),
            KeyCode::Char('/') => {
                self.status =
                    "use `serialctl logs --contains TEXT` for durable history search".into();
            }
            KeyCode::Char('?') => self.help = true,
            KeyCode::Char('q' | 'Q') => self.should_quit = true,
            KeyCode::Char(']') => {
                self.request_write(commands, vec![0x1d], None);
            }
            _ => self.status = "unknown prefix command; Ctrl-] ? opens help".into(),
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
                bytes.extend_from_slice(
                    self.current().snapshot.config.settings.write_eol.as_bytes(),
                );
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
                self.status = "input cleared".into();
            }
            _ => {}
        }
    }

    fn handle_raw_key(&mut self, key: KeyEvent, commands: &mpsc::Sender<NetworkCommand>) {
        if let Some(bytes) = raw_key_bytes(key) {
            self.request_write(commands, bytes, None);
        }
    }

    fn handle_paste(&mut self, value: String, commands: &mpsc::Sender<NetworkCommand>) {
        if value.len() > MAX_PASTE_BYTES {
            self.status = format!(
                "paste rejected: {} bytes exceeds the {} byte interactive safety limit",
                value.len(),
                MAX_PASTE_BYTES
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
            self.status =
                "multi-line/large paste blocked; Ctrl-] p confirms for the original Slot".into();
            self.dirty = true;
            return;
        }
        if self.current_mode() == InputMode::Raw {
            self.request_write(commands, value.into_bytes(), None);
        } else {
            let view = self.current_mut();
            for character in value.chars() {
                view.draft.insert(view.draft_cursor, character);
                view.draft_cursor += 1;
            }
        }
        self.dirty = true;
    }

    fn confirm_paste(&mut self, commands: &mpsc::Sender<NetworkCommand>) {
        let Some(paste) = self.pending_paste.take() else {
            self.status = "no blocked paste to confirm".into();
            return;
        };
        let Some(index) = self.slot_index(&paste.slot_id) else {
            self.status = "the paste target Slot no longer exists".into();
            return;
        };
        let previous = self.selected;
        self.selected = index;
        let accepted = if paste.raw {
            self.request_write(commands, paste.bytes, None)
        } else {
            let text = String::from_utf8_lossy(&paste.bytes);
            let eol = self.current().snapshot.config.settings.write_eol.clone();
            let normalized = text.replace("\r\n", "\n").replace('\r', "\n");
            let mut data = Vec::with_capacity(normalized.len() + eol.len());
            for line in normalized.split_inclusive('\n') {
                data.extend_from_slice(line.trim_end_matches('\n').as_bytes());
                data.extend_from_slice(eol.as_bytes());
            }
            self.request_write(commands, data, Some(Uuid::new_v4()))
        };
        self.selected = previous;
        if accepted {
            self.status = format!("confirmed paste queued for {}", paste.slot_id);
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
        let max = (self.current().lines.len() + usize::from(self.current().pending_line.is_some()))
            .saturating_sub(1);
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
        bail!("no Slot is configured; run `serialctl init`");
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
    let mut network = ws::spawn(endpoint, token, slot_ids);

    let mut terminal = enter_terminal()?;
    let _guard = TerminalGuard;
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
                    app.status = "network worker stopped".into();
                    app.dirty = true;
                }
            },
            _ = renew_tick.tick() => app.maintain_controls(commands),
            _ = activity_tick.tick() => {
                if app.slots.iter().any(|slot| {
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

fn enter_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    if let Err(error) = execute!(
        stdout,
        EnterAlternateScreen,
        EnableBracketedPaste,
        EnableMouseCapture
    ) {
        let _ = disable_raw_mode();
        return Err(error.into());
    }
    match Terminal::new(CrosstermBackend::new(stdout)) {
        Ok(terminal) => Ok(terminal),
        Err(error) => {
            let _ = disable_raw_mode();
            let _ = execute!(
                io::stdout(),
                DisableMouseCapture,
                DisableBracketedPaste,
                LeaveAlternateScreen
            );
            Err(error.into())
        }
    }
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            DisableMouseCapture,
            DisableBracketedPaste,
            LeaveAlternateScreen
        );
        let _ = io::stdout().flush();
    }
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

fn draw(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(4),
        Constraint::Length(1),
        Constraint::Length(3),
        Constraint::Length(1),
    ])
    .split(area);

    draw_tabs(frame, app, chunks[0]);
    draw_output(frame, app, chunks[1]);
    draw_status(frame, app, chunks[2]);
    draw_input(frame, app, chunks[3]);
    draw_help_line(frame, app, chunks[4]);
    if app.help {
        draw_help(frame, app, area);
    }
}

fn draw_tabs(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let titles = app
        .slots
        .iter()
        .enumerate()
        .map(|(index, slot)| {
            let state = format!("{:?}", slot.snapshot.session_state).to_uppercase();
            let activity =
                format!("{:?}", displayed_target_activity(&slot.snapshot)).to_uppercase();
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
        "○ reconnecting"
    } else if !app.authenticated {
        "◐ authenticating"
    } else if app.all_slots_ready() {
        "● live"
    } else {
        "◐ attaching"
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
    let visible_height = area.height.saturating_sub(2) as usize;
    let total_lines = view.lines.len() + usize::from(view.pending_line.is_some());
    let end = total_lines.saturating_sub(view.scroll_from_bottom);
    let start = end.saturating_sub(visible_height);
    let shell_prompt = view.snapshot.config.settings.shell_prompt.as_deref();
    let uboot_prompt = view.snapshot.config.settings.uboot_prompt.as_deref();
    let lines = view
        .lines
        .iter()
        .chain(view.pending_line.iter())
        .skip(start)
        .take(end.saturating_sub(start))
        .map(|entry| timeline_line(entry, app.detailed_timeline, shell_prompt, uboot_prompt))
        .collect::<Vec<_>>();
    let title = format!(
        " {} · {} · {} baud{} ",
        safe_inline(&view.snapshot.config.display_name),
        safe_inline(&view.snapshot.config.port),
        view.snapshot.config.settings.baud_rate,
        if view.scroll_from_bottom > 0 {
            " · PAUSED"
        } else {
            ""
        }
    );
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().borders(Borders::ALL).title(title))
            .wrap(Wrap { trim: false }),
        area,
    );
}

/// Renders one scrollback row. Compact mode is `{marker}{text}` where the
/// two-column marker is a colored "●" for TX/actor-attributed rows and two
/// spaces otherwise; detailed mode additionally shows the legacy `#seq` and
/// source columns. Stream rows get inline keyword/prompt highlighting;
/// system and gap rows keep their whole-line style.
fn timeline_line(
    entry: &DisplayLine,
    detailed: bool,
    shell_prompt: Option<&str>,
    uboot_prompt: Option<&str>,
) -> Line<'static> {
    let mut spans = Vec::new();
    match entry.marker_color {
        Some(color) => spans.push(Span::styled(
            "● ",
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        )),
        None => spans.push(Span::raw("  ")),
    }
    if detailed {
        spans.push(Span::styled(
            format!("#{:<8} ", entry.seq),
            Style::default().fg(Color::DarkGray),
        ));
        spans.push(Span::styled(
            format!("{:<28} ", entry.source),
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

fn draw_status(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let control = app
        .current()
        .snapshot
        .control
        .as_ref()
        .map(|lease| safe_inline(&lease.owner.label))
        .unwrap_or_else(|| "none".into());
    let mode = match app.current_mode() {
        InputMode::Line => "LINE",
        InputMode::Raw => "RAW",
    };
    let prefix = if app.prefix_pending { " · PREFIX" } else { "" };
    let uncertain = if app.uncertain_write_outcomes == 0 {
        String::new()
    } else {
        format!(
            " · {} WRITE OUTCOME(S) UNCERTAIN: inspect TX before retrying",
            app.uncertain_write_outcomes
        )
    };
    let slot_id = &app.current().snapshot.config.id;
    let queue = if let Some(queued) = app.queued_controls.get(slot_id) {
        let writes = app.pending_writes.get(slot_id).map_or(0, VecDeque::len);
        format!(
            " · QUEUED #{} ({}s, {} chunk(s); Ctrl-] c cancels)",
            queued.position,
            queued.since.elapsed().as_secs(),
            writes
        )
    } else if app.pending_requests.values().any(
        |request| matches!(request, PendingRequest::Acquire { slot_id: pending } if pending == slot_id),
    ) {
        " · CONTROL REQUEST PENDING (Ctrl-] c cancels)".into()
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
                format!(" · idle release in {remaining}s")
            })
    } else {
        String::new()
    };
    let content = format!(
        " {} · {mode}{prefix} · control: {control}{idle}{queue} · {}{}",
        safe_inline(slot_id),
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
                    .title(" history search · Enter accepts · Esc cancels "),
            ),
            area,
        );
        return;
    }
    let (text, title) = match app.current_mode() {
        InputMode::Line => (
            safe_inline(&app.current().draft.iter().collect::<String>()),
            " command · Enter sends Profile EOL ",
        ),
        InputMode::Raw => (
            "Keystrokes are sent directly. Ctrl-C sends ETX; Ctrl-] opens local commands.".into(),
            " RAW direct transport ",
        ),
    };
    frame.render_widget(
        Paragraph::new(format!("> {text}"))
            .block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
    if app.current_mode() == InputMode::Line {
        let cursor = app.current().draft_cursor as u16;
        frame.set_cursor_position(Position::new(
            area.x.saturating_add(2).saturating_add(cursor),
            area.y.saturating_add(1),
        ));
    }
}

fn draw_help_line(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let scroll = if app.current_mode() == InputMode::Raw {
        "Ctrl-] PgUp/PgDn scroll"
    } else {
        "PgUp/PgDn scroll"
    };
    frame.render_widget(
        Paragraph::new(format!(
            " Ctrl-] ? help · Alt-1/2 switch · {scroll} · Ctrl-] q quit "
        ))
        .alignment(Alignment::Center)
        .style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_help(frame: &mut Frame<'_>, app: &App, area: Rect) {
    let width = area.width.min(76);
    let height = area.height.min(28);
    let popup = centered_rect(width, height, area);
    let help = [
        "All modes",
        "  Alt-1..9 / Ctrl-] 1..9   switch Slot",
        "  Ctrl-] s                 next Slot",
        "  Ctrl-] l / r             LINE / RAW mode",
        "  Ctrl-] v                 compact / detailed timeline",
        "  Ctrl-] PgUp / PgDn       local scroll (especially in RAW)",
        "  mouse wheel              scroll 3 lines (bottom resumes follow)",
        "  Ctrl-] t                 explicit human takeover",
        "  Ctrl-] c                 release control or cancel queued input",
        "  Ctrl-] f                 follow live output",
        "  Ctrl-] p                 confirm blocked paste",
        "  Ctrl-] Ctrl-]            send byte 0x1d",
        "  Ctrl-] q                 quit",
        "",
        "LINE: Enter sends the line plus the Profile EOL (default CR) and",
        "returns to the live tail. Up/Down browse history; Ctrl-R starts an",
        "incremental history search; Tab completes from history.",
        "RAW: keys are bytes; Ctrl-C is sent to the device and does not quit.",
        "RAW PageUp/PageDown go to the device; use the prefix for local scroll.",
        "Large or multi-line paste is always held for explicit confirmation.",
        &format!(
            "Queued input expires after {}s idle; cancel reconnects and releases this terminal's controls.",
            app.human_idle_release.as_secs()
        ),
        "Disconnected input is never replayed after reconnect.",
        "Sent writes without an acknowledgement are uncertain; inspect TX before retrying.",
        "",
        "Press any key to close help.",
    ];
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(help.join("\n"))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" serialctl help "),
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
    use serial_protocol::{ActorKind, Direction, SerialSettings, SlotConfig};

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
    fn serial_close_discards_queued_control_and_input() {
        let mut app = App::new(vec![snapshot()], None);
        let slot_id = app.selected_slot_id();
        app.pending_writes
            .entry(slot_id.clone())
            .or_default()
            .push_back(PendingWrite {
                data: b"version\r".to_vec(),
                operation_id: None,
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
    fn live_reconfigure_updates_the_authoritative_slot_config() {
        let mut app = App::new(vec![snapshot()], None);
        let (commands, _) = mpsc::channel(4);
        let mut config = app.slots[0].snapshot.config.clone();
        config.display_name = "Renamed station".into();
        config.profile = "custom-profile".into();
        config.settings.baud_rate = 57_600;
        let mut reconfigured = event(EventKind::SlotReconfigured, Direction::None, 1, &[]);
        reconfigured.daemon_epoch = app.slots[0].snapshot.daemon_epoch;
        reconfigured
            .metadata
            .insert("current".into(), serde_json::to_value(&config).unwrap());

        app.push_event(reconfigured, false, &commands);

        assert_eq!(app.slots[0].snapshot.config, config);
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
    }

    #[test]
    fn queued_control_can_be_cancelled_and_forces_actor_reconnect() {
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
            logging: LoggingState::Healthy,
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
}
