use std::{collections::VecDeque, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, EventKind, PROTOCOL_VERSION, ServerMessage, Subscription,
    TimelineEvent, WireFrame, decode_wire_frame, encode_client_control,
};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

use crate::{config::CaptureLimits, render::terminal_text, session::ensure_welcome_protocol};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct Capture {
    socket: Socket,
    slot_id: String,
    events: VecDeque<TimelineEvent>,
    retained_bytes: usize,
    truncated: bool,
    gaps: Vec<String>,
    limits: CaptureLimits,
    watched_run_id: Option<Uuid>,
}

pub struct CaptureOptions {
    pub timeout: Duration,
    pub quiet: Duration,
    pub patterns: Vec<CompletionPattern>,
    /// Regex matched against the rolling RX window; compiled once by the
    /// caller and reused for every poll until it completes the capture.
    pub until_regex: Option<regex::Regex>,
    /// Finish on a quiet interval. Exact prompt/literal captures disable this
    /// so a short scheduling gap cannot masquerade as their requested
    /// completion evidence.
    pub complete_on_quiet: bool,
    pub allow_empty_quiet: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionPattern {
    /// Exact normalized terminal text anywhere in the post-TX window.
    Literal(String),
    /// Exact normalized terminal text only at the end of a logical line.
    /// Device prompts use this form so a prompt-looking fragment in ordinary
    /// output cannot finish a command.
    Prompt(String),
}

pub struct CaptureResult {
    pub events: Vec<TimelineEvent>,
    pub truncated: bool,
    pub gaps: Vec<String>,
    pub completion: Completion,
    /// Highest sequence observed by the capture socket, including activity
    /// deliberately excluded from the command output boundary.
    pub through_seq: Option<u64>,
    pub command_boundary: Option<CommandBoundaryResult>,
}

/// The confirmed TX audit is the lower bound for one command capture. RX below
/// it is never command output. A complete configured echo is stripped and
/// raises confidence; exact post-TX evidence may still complete when that echo
/// is missing or incomplete, with the result explicitly downgraded.
pub struct CommandBoundary {
    pub tx_event_seq: u64,
    pub operation_id: Uuid,
    pub expected_echo: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureActivity {
    pub first_seq: Option<u64>,
    pub through_seq: Option<u64>,
    pub event_count: usize,
    pub rx_event_count: usize,
    pub rx_byte_count: usize,
    pub tx_event_count: usize,
}

impl CaptureActivity {
    fn observe(&mut self, event: &TimelineEvent) {
        self.first_seq.get_or_insert(event.seq);
        self.through_seq = Some(event.seq);
        self.event_count = self.event_count.saturating_add(1);
        match event.direction {
            serial_protocol::Direction::Rx => {
                self.rx_event_count = self.rx_event_count.saturating_add(1);
                self.rx_byte_count = self.rx_byte_count.saturating_add(event.data.len());
            }
            serial_protocol::Direction::Tx => {
                self.tx_event_count = self.tx_event_count.saturating_add(1);
            }
            serial_protocol::Direction::None => {}
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandBoundaryResult {
    pub prewrite_activity: CaptureActivity,
    pub tx_audit_observed: bool,
    pub echo_required: bool,
    pub echo_observed: bool,
    pub discarded_rx_event_count: usize,
    pub discarded_rx_byte_count: usize,
    pub interfered: bool,
}

impl CommandBoundaryResult {
    #[cfg(test)]
    pub fn confidence(&self, completion: &Completion) -> &'static str {
        if self.interfered {
            return if self.echo_observed {
                "echo_observed_interfered"
            } else {
                "interfered"
            };
        }
        if self.echo_required {
            if self.echo_observed {
                "echo_confirmed"
            } else if matches!(
                completion,
                Completion::Pattern(_) | Completion::Prompt(_) | Completion::Regex(_)
            ) {
                "post_tx_boundary_without_echo"
            } else {
                "echo_not_observed"
            }
        } else if self.tx_audit_observed {
            "tx_audit_observed"
        } else {
            // The write RPC's event_seq remains authoritative even when the
            // independent capture stream has not replayed the TX frame.
            "tx_event_seq"
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completion {
    Pattern(String),
    Prompt(String),
    Regex(String),
    Quiet,
    Signal(String),
    RunAborted { run_id: Uuid, reason: String },
    Timeout,
    Disconnected(String),
}

impl Completion {
    #[cfg(test)]
    pub fn label(&self) -> String {
        match self {
            Self::Pattern(pattern) => format!("pattern:{pattern}"),
            Self::Prompt(prompt) => format!("prompt:{prompt}"),
            Self::Regex(pattern) => format!("regex:{pattern}"),
            Self::Quiet => "quiet".into(),
            Self::Signal(signal) => format!("signal:{signal}"),
            Self::RunAborted { run_id, reason } => {
                format!("run_aborted:{run_id}:{reason}")
            }
            Self::Timeout => "timeout".into(),
            Self::Disconnected(reason) => format!("disconnected:{reason}"),
        }
    }

    #[cfg(test)]
    pub fn is_complete(&self) -> bool {
        matches!(
            self,
            Self::Pattern(_) | Self::Prompt(_) | Self::Regex(_) | Self::Quiet | Self::Signal(_)
        )
    }
}

impl Capture {
    pub async fn attach(
        endpoint: &str,
        token: &str,
        actor_label: &str,
        slot_id: String,
        cursor: Cursor,
        limits: CaptureLimits,
    ) -> Result<Self> {
        let mut request = ws_url(endpoint)?.into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {token}")
                .parse()
                .context("operator token cannot be encoded as an HTTP header")?,
        );
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
            .await
            .context("timed out connecting capture stream to seriald")??;

        let hello = ClientMessage::Hello {
            request_id: Uuid::new_v4(),
            protocol_version: PROTOCOL_VERSION,
            client_name: format!("{actor_label}-capture"),
            actor_kind: ActorKind::Agent,
        };
        send_control(&mut socket, &hello).await?;
        loop {
            match next_frame(&mut socket).await? {
                WireFrame::Control(ServerMessage::Welcome {
                    protocol_version, ..
                }) => {
                    ensure_welcome_protocol(protocol_version)?;
                    break;
                }
                WireFrame::Control(ServerMessage::Error { message, .. }) => {
                    bail!("seriald rejected capture hello: {message}")
                }
                _ => {}
            }
        }

        let attach_id = Uuid::new_v4();
        send_control(
            &mut socket,
            &ClientMessage::Attach {
                request_id: attach_id,
                subscriptions: vec![Subscription {
                    slot_id: slot_id.clone(),
                    cursor: Some(cursor),
                    tail_events: 0,
                }],
            },
        )
        .await?;

        let mut capture = Self {
            socket,
            slot_id,
            events: VecDeque::new(),
            retained_bytes: 0,
            truncated: false,
            gaps: Vec::new(),
            limits,
            watched_run_id: None,
        };
        loop {
            match capture.next().await? {
                Frame::Event(event) => capture.push(*event),
                Frame::Gap(gap) => capture.gaps.push(gap),
                Frame::Ready => return Ok(capture),
                Frame::Other => {}
            }
        }
    }

    /// End an observation as soon as the authoritative timeline says that the
    /// Run it belongs to was aborted. This is especially important for human
    /// takeover: otherwise a prompt/regex wait can sit until its ordinary
    /// timeout and hide the actual ownership change from the Agent.
    #[must_use]
    pub fn watch_run(mut self, run_id: Uuid) -> Self {
        self.watched_run_id = Some(run_id);
        self
    }

    pub async fn collect(self, options: CaptureOptions) -> CaptureResult {
        self.collect_inner(options, None).await
    }

    pub async fn collect_after_write(
        self,
        options: CaptureOptions,
        boundary: CommandBoundary,
    ) -> CaptureResult {
        self.collect_inner(options, Some(boundary)).await
    }

    /// Keep a pre-attached capture alive until an external operation reports
    /// its authoritative terminal sequence. Trigger status polling runs on
    /// the independent control session, so this socket can continue draining
    /// RX/TX without blocking lease renewal. When a gap prevents observing the
    /// requested sequence, return the bounded evidence with that gap instead
    /// of pretending the capture is complete.
    pub async fn collect_until_seq(
        mut self,
        mut terminal_seq: oneshot::Receiver<Option<u64>>,
        timeout: Duration,
    ) -> CaptureResult {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut signalled = false;
        let mut target_seq = None;
        let mut through_seq = self.events.back().map(|event| event.seq);

        loop {
            if signalled
                && (target_seq
                    .is_none_or(|target| through_seq.is_some_and(|through| through >= target))
                    || !self.gaps.is_empty())
            {
                return self.finish(Completion::Signal("trigger_terminal".into()), None);
            }

            tokio::select! {
                signal = &mut terminal_seq, if !signalled => {
                    match signal {
                        Ok(seq) => {
                            signalled = true;
                            target_seq = seq;
                        }
                        Err(_) => {
                            return self.finish(
                                Completion::Disconnected(
                                    "trigger status waiter ended without a terminal result".into()
                                ),
                                None,
                            );
                        }
                    }
                }
                frame = self.next() => {
                    match frame {
                        Ok(Frame::Event(event)) => {
                            let event = *event;
                            through_seq = Some(through_seq.map_or(event.seq, |through| through.max(event.seq)));
                            self.push(event);
                        }
                        Ok(Frame::Gap(gap)) => self.gaps.push(gap),
                        Ok(Frame::Ready | Frame::Other) => {}
                        Err(error) => {
                            return self.finish(Completion::Disconnected(error.to_string()), None);
                        }
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return self.finish(Completion::Timeout, None);
                }
            }
        }
    }

    async fn collect_inner(
        mut self,
        options: CaptureOptions,
        boundary: Option<CommandBoundary>,
    ) -> CaptureResult {
        let mut watcher = CompletionWatcher::new(options);
        let mut boundary =
            boundary.map(|boundary| CommandBoundaryTracker::new(boundary, self.limits));
        let mut rolling = String::new();

        let attached_events: Vec<_> = self.events.drain(..).collect();
        self.retained_bytes = 0;
        for event in attached_events {
            let run_aborted = run_abort_completion(&event, self.watched_run_id);
            let accepted = match &mut boundary {
                Some(boundary) => boundary.accept(event),
                None => AcceptedEvents::single(event),
            };
            self.observe_accepted(accepted, &mut watcher, &mut rolling);
            if let Some(completion) = run_aborted {
                return self.finish(completion, boundary);
            }
        }

        loop {
            let now = tokio::time::Instant::now();
            if let Some(completion) = watcher.poll(&rolling, now) {
                return self.finish(completion, boundary);
            }

            match tokio::time::timeout_at(watcher.wake_at(), self.next()).await {
                Ok(Ok(Frame::Event(event))) => {
                    let event = *event;
                    let run_aborted = run_abort_completion(&event, self.watched_run_id);
                    let accepted = match &mut boundary {
                        Some(boundary) => boundary.accept(event),
                        None => AcceptedEvents::single(event),
                    };
                    self.observe_accepted(accepted, &mut watcher, &mut rolling);
                    if let Some(completion) = run_aborted {
                        return self.finish(completion, boundary);
                    }
                }
                Ok(Ok(Frame::Gap(gap))) => self.gaps.push(gap),
                Ok(Ok(Frame::Ready | Frame::Other)) => {}
                Ok(Err(error)) => {
                    return self.finish(Completion::Disconnected(error.to_string()), boundary);
                }
                Err(_) => {
                    return self.finish(watcher.expired(tokio::time::Instant::now()), boundary);
                }
            }
        }
    }

    fn observe_accepted(
        &mut self,
        accepted: AcceptedEvents,
        watcher: &mut CompletionWatcher,
        rolling: &mut String,
    ) {
        let now = tokio::time::Instant::now();
        if accepted.reset_matching {
            // Echo reconciliation supplies a complete replacement view. Drop
            // the previous bytes so an echoed command or pre-echo backlog
            // cannot masquerade as the requested boundary.
            rolling.clear();
        }
        if accepted.observed_post_tx_rx {
            watcher.observe_rx(now);
        }
        if !accepted.matching_rx.is_empty() {
            append_rolling(rolling, &String::from_utf8_lossy(&accepted.matching_rx));
        }
        for event in accepted.events {
            self.push(event);
        }
    }

    async fn next(&mut self) -> Result<Frame> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Binary(bytes))) => match decode_wire_frame(&bytes)? {
                    WireFrame::Rx(header, data) | WireFrame::Tx(header, data) => {
                        if header.slot_id == self.slot_id {
                            return Ok(Frame::Event(Box::new(header.into_event(data))));
                        }
                    }
                    WireFrame::Control(ServerMessage::Timeline { event, .. }) => {
                        if event.slot_id == self.slot_id {
                            return Ok(Frame::Event(Box::new(event)));
                        }
                    }
                    WireFrame::Control(ServerMessage::Ready { slot_id, .. })
                        if slot_id == self.slot_id =>
                    {
                        return Ok(Frame::Ready);
                    }
                    WireFrame::Control(ServerMessage::Gap {
                        slot_id,
                        requested_after_seq,
                        first_available_seq,
                        head_seq,
                        reason,
                    }) if slot_id == self.slot_id => {
                        return Ok(Frame::Gap(format!(
                            "{reason:?}: requested_after={requested_after_seq:?}, first_available={first_available_seq:?}, head={head_seq}"
                        )));
                    }
                    WireFrame::Control(ServerMessage::Lagged {
                        slot_id,
                        from_seq,
                        to_seq,
                    }) if slot_id == self.slot_id => {
                        return Ok(Frame::Gap(format!("lagged:{from_seq}-{to_seq}")));
                    }
                    WireFrame::Control(ServerMessage::Error { message, .. }) => {
                        bail!("seriald capture error: {message}");
                    }
                    _ => return Ok(Frame::Other),
                },
                Some(Ok(Message::Ping(payload))) => {
                    self.socket.send(Message::Pong(payload)).await?
                }
                Some(Ok(Message::Close(frame))) => {
                    bail!("seriald capture stream closed: {frame:?}")
                }
                Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Frame(_))) => {}
                Some(Err(error)) => return Err(error.into()),
                None => bail!("seriald capture stream ended"),
            }
        }
    }

    fn push(&mut self, event: TimelineEvent) {
        self.retained_bytes = self.retained_bytes.saturating_add(event.data.len() + 256);
        self.events.push_back(event);
        while self.retained_bytes > self.limits.max_bytes
            || self.events.len() > self.limits.max_events
        {
            let Some(dropped) = self.events.pop_front() else {
                break;
            };
            self.retained_bytes = self.retained_bytes.saturating_sub(dropped.data.len() + 256);
            self.truncated = true;
        }
    }

    fn finish(
        self,
        completion: Completion,
        command_boundary: Option<CommandBoundaryTracker>,
    ) -> CaptureResult {
        let through_seq = command_boundary
            .as_ref()
            .and_then(|boundary| boundary.through_seq)
            .or_else(|| self.events.back().map(|event| event.seq));
        let boundary_truncated = command_boundary
            .as_ref()
            .is_some_and(|boundary| boundary.truncated);
        let (command_boundary, pending_rx) =
            command_boundary.map_or((None, Vec::new()), |boundary| {
                let (result, pending_rx) = boundary.finish();
                (Some(result), pending_rx)
            });
        let mut events: Vec<_> = self.events.into_iter().chain(pending_rx).collect();
        events.sort_by_key(|event| event.seq);
        let mut retained_bytes: usize = events
            .iter()
            .map(|event| event.data.len().saturating_add(256))
            .sum();
        let mut truncated = self.truncated || boundary_truncated;
        while retained_bytes > self.limits.max_bytes || events.len() > self.limits.max_events {
            let dropped = events.remove(0);
            retained_bytes = retained_bytes.saturating_sub(dropped.data.len().saturating_add(256));
            truncated = true;
        }
        CaptureResult {
            events,
            truncated,
            gaps: self.gaps,
            completion,
            through_seq,
            command_boundary,
        }
    }
}

fn run_abort_completion(event: &TimelineEvent, watched_run_id: Option<Uuid>) -> Option<Completion> {
    let run_id = event.run_id?;
    if event.kind != EventKind::RunAborted || watched_run_id != Some(run_id) {
        return None;
    }
    let reason = event
        .metadata
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unspecified")
        .to_string();
    Some(Completion::RunAborted { run_id, reason })
}

struct AcceptedEvents {
    events: Vec<TimelineEvent>,
    /// RX bytes visible to the completion matcher. Echo-required captures can
    /// expose post-TX bytes here before deciding whether a complete echo will
    /// arrive, while still retaining those events for lossless output.
    matching_rx: Vec<u8>,
    observed_post_tx_rx: bool,
    /// Replace, rather than append to, the rolling matcher view. Used both
    /// when a complete echo is stripped and while an incomplete echo's safe
    /// fallback view is recomputed.
    reset_matching: bool,
}

impl AcceptedEvents {
    fn single(event: TimelineEvent) -> Self {
        let observed_post_tx_rx = event.direction == serial_protocol::Direction::Rx;
        let matching_rx = if observed_post_tx_rx {
            event.data.clone()
        } else {
            Vec::new()
        };
        Self {
            events: vec![event],
            matching_rx,
            observed_post_tx_rx,
            reset_matching: false,
        }
    }

    fn none() -> Self {
        Self {
            events: Vec::new(),
            matching_rx: Vec::new(),
            observed_post_tx_rx: false,
            reset_matching: false,
        }
    }

    fn pending_rx(data: Vec<u8>) -> Self {
        Self {
            events: Vec::new(),
            matching_rx: data,
            observed_post_tx_rx: true,
            // Recomputed from the entire pending RX window so a partial echo
            // can retract bytes that must not be used as completion evidence.
            reset_matching: true,
        }
    }

    fn after_echo(events: Vec<TimelineEvent>) -> Self {
        let matching_rx = events
            .iter()
            .filter(|event| event.direction == serial_protocol::Direction::Rx)
            .flat_map(|event| event.data.iter().copied())
            .collect();
        Self {
            events,
            matching_rx,
            // The echo itself is authoritative post-TX RX evidence even when
            // it has no following output bytes.
            observed_post_tx_rx: true,
            reset_matching: true,
        }
    }
}

struct CommandBoundaryTracker {
    boundary: CommandBoundary,
    limits: CaptureLimits,
    armed: bool,
    pending_rx: VecDeque<TimelineEvent>,
    pending_bytes: usize,
    prewrite_activity: CaptureActivity,
    tx_audit_observed: bool,
    echo_observed: bool,
    discarded_rx_event_count: usize,
    discarded_rx_byte_count: usize,
    interfered: bool,
    through_seq: Option<u64>,
    truncated: bool,
}

impl CommandBoundaryTracker {
    fn new(boundary: CommandBoundary, limits: CaptureLimits) -> Self {
        let armed = boundary.expected_echo.is_none();
        let through_seq = Some(boundary.tx_event_seq);
        Self {
            boundary,
            limits,
            armed,
            pending_rx: VecDeque::new(),
            pending_bytes: 0,
            prewrite_activity: CaptureActivity::default(),
            tx_audit_observed: false,
            echo_observed: false,
            discarded_rx_event_count: 0,
            discarded_rx_byte_count: 0,
            interfered: false,
            // The write RPC authoritatively confirms this sequence even if
            // the independent capture socket has not received its TX frame.
            through_seq,
            truncated: false,
        }
    }

    fn accept(&mut self, event: TimelineEvent) -> AcceptedEvents {
        self.through_seq = Some(
            self.through_seq
                .map_or(event.seq, |through| through.max(event.seq)),
        );

        // Any foreign TX observed between attach and capture completion makes
        // causal attribution unsafe, including a TX before our own audit whose
        // delayed echo could arrive after it.
        if event.direction == serial_protocol::Direction::Tx
            && event.operation_id != Some(self.boundary.operation_id)
        {
            self.interfered = true;
        }

        if event.seq < self.boundary.tx_event_seq {
            self.prewrite_activity.observe(&event);
            return AcceptedEvents::none();
        }

        if event.seq == self.boundary.tx_event_seq {
            if event.direction == serial_protocol::Direction::Tx
                && event.operation_id == Some(self.boundary.operation_id)
            {
                self.tx_audit_observed = true;
            } else {
                // A mismatched event at the authoritative sequence is not
                // command output. Preserve it as prewrite/anomalous activity
                // and leave the RPC-provided lower bound intact.
                self.prewrite_activity.observe(&event);
            }
            return AcceptedEvents::none();
        }

        if self.armed || event.direction != serial_protocol::Direction::Rx {
            return AcceptedEvents::single(event);
        }

        self.pending_bytes = self
            .pending_bytes
            .saturating_add(event.data.len().saturating_add(256));
        self.pending_rx.push_back(event);
        self.bound_pending();

        let expected = self
            .boundary
            .expected_echo
            .as_deref()
            .expect("an unarmed command boundary always requires echo");
        let pending: Vec<u8> = self
            .pending_rx
            .iter()
            .flat_map(|event| event.data.iter().copied())
            .collect();
        let Some(echo_end) = find_echo_end(&pending, expected) else {
            // Sequence, not echo, is the causal lower bound. Exact
            // post-write evidence may therefore complete a command even when
            // an echo-on target loses or omits part of the echo. The pending
            // events stay buffered so they can either be stripped if the full
            // echo arrives later, or returned losslessly with a low-confidence
            // result if it never does.
            return AcceptedEvents::pending_rx(fallback_after_incomplete_echo(&pending, expected));
        };

        let events = self.discard_pending_prefix(echo_end);
        self.echo_observed = true;
        self.armed = true;
        AcceptedEvents::after_echo(events)
    }

    fn bound_pending(&mut self) {
        while self.pending_bytes > self.limits.max_bytes
            || self.pending_rx.len() > self.limits.max_events
        {
            let Some(dropped) = self.pending_rx.pop_front() else {
                break;
            };
            self.pending_bytes = self
                .pending_bytes
                .saturating_sub(dropped.data.len().saturating_add(256));
            self.discarded_rx_event_count = self.discarded_rx_event_count.saturating_add(1);
            self.discarded_rx_byte_count = self
                .discarded_rx_byte_count
                .saturating_add(dropped.data.len());
            self.truncated = true;
        }
    }

    fn discard_pending_prefix(&mut self, mut remaining: usize) -> Vec<TimelineEvent> {
        let mut accepted = Vec::new();
        while let Some(mut event) = self.pending_rx.pop_front() {
            self.pending_bytes = self
                .pending_bytes
                .saturating_sub(event.data.len().saturating_add(256));
            if remaining >= event.data.len() {
                remaining -= event.data.len();
                self.discarded_rx_event_count = self.discarded_rx_event_count.saturating_add(1);
                self.discarded_rx_byte_count = self
                    .discarded_rx_byte_count
                    .saturating_add(event.data.len());
                continue;
            }

            if remaining > 0 {
                self.discarded_rx_event_count = self.discarded_rx_event_count.saturating_add(1);
                self.discarded_rx_byte_count =
                    self.discarded_rx_byte_count.saturating_add(remaining);
                event.data.drain(..remaining);
                if let Some(start) = event.stream_offset_start.as_mut() {
                    *start = start.saturating_add(remaining as u64);
                }
                remaining = 0;
            }
            if !event.data.is_empty() {
                accepted.push(event);
            }
            accepted.extend(self.pending_rx.drain(..));
            self.pending_bytes = 0;
            break;
        }
        debug_assert_eq!(remaining, 0);
        accepted
    }

    fn finish(self) -> (CommandBoundaryResult, Vec<TimelineEvent>) {
        // Missing/incomplete echo is lower-confidence evidence, not a reason
        // to erase the post-TX bytes. Return the bounded pending events so the
        // caller and Agent can inspect the exact RX that led to completion.
        let pending_rx = self.pending_rx.into_iter().collect();
        let result = CommandBoundaryResult {
            prewrite_activity: self.prewrite_activity,
            tx_audit_observed: self.tx_audit_observed,
            echo_required: self.boundary.expected_echo.is_some(),
            echo_observed: self.echo_observed,
            discarded_rx_event_count: self.discarded_rx_event_count,
            discarded_rx_byte_count: self.discarded_rx_byte_count,
            interfered: self.interfered,
        };
        (result, pending_rx)
    }
}

/// Return the raw byte offset immediately after a complete command echo.
/// Some target TTYs inject CR CR LF while hard-wrapping long echoed input; that
/// exact sequence is tolerated only after matching has started. The caller
/// scans all possible starts because stale RX may precede the real echo.
fn find_echo_end(actual: &[u8], expected: &[u8]) -> Option<usize> {
    if expected.is_empty() {
        return Some(0);
    }

    for start in 0..actual.len() {
        if actual[start] != expected[0] {
            continue;
        }
        let mut actual_index = start;
        let mut expected_index = 0;
        while expected_index < expected.len() {
            if actual_index >= actual.len() {
                break;
            }
            if actual[actual_index] == expected[expected_index] {
                actual_index += 1;
                expected_index += 1;
                continue;
            }
            if expected_index > 0 && actual[actual_index..].starts_with(b"\r\r\n") {
                actual_index += 3;
                continue;
            }
            if expected[expected_index] == b'\n' && actual[actual_index..].starts_with(b"\r\n") {
                // A target TTY may expand a transmitted LF (or the LF half
                // of CRLF) to CRLF in its local echo.
                actual_index += 1;
                continue;
            }
            break;
        }
        if expected_index == expected.len() {
            // A transmitted CR is commonly echoed as CRLF or, on this target,
            // CR CR LF. Consume the remaining line-ending bytes when they are
            // already present so primary output does not start with a visual
            // blank line.
            if expected.last() == Some(&b'\r') {
                if actual[actual_index..].starts_with(b"\r\n") {
                    actual_index += 2;
                } else if actual[actual_index..].starts_with(b"\n") {
                    actual_index += 1;
                }
            }
            return Some(actual_index);
        }
    }
    None
}

/// Build the completion-matching view while a configured echo is incomplete.
///
/// A logical line that is identical or close to the expected echo is withheld
/// in full. In particular, bytes after the first damaged character on that
/// same line are never reclassified as command output. Unrelated post-TX lines
/// remain intact, even when they happen to contain the command's first byte.
fn fallback_after_incomplete_echo(actual: &[u8], expected: &[u8]) -> Vec<u8> {
    if actual.is_empty() || expected.is_empty() {
        return actual.to_vec();
    }

    let expected = expected
        .strip_suffix(b"\r\n")
        .or_else(|| expected.strip_suffix(b"\r"))
        .or_else(|| expected.strip_suffix(b"\n"))
        .unwrap_or(expected);
    if expected.is_empty() {
        return actual.to_vec();
    }

    let mut line_start = 0;
    while line_start < actual.len() {
        let (_, line_end) = logical_echo_line(actual, line_start);
        let mut candidates = 0;
        for start in line_start..line_end {
            let at_boundary =
                start == line_start || is_echo_candidate_boundary(actual[start.saturating_sub(1)]);
            if !at_boundary && actual[start] != expected[0] {
                continue;
            }
            let (candidate, candidate_end) = logical_echo_line(actual, start);
            if candidate.is_empty() {
                continue;
            }
            candidates += 1;
            if looks_like_damaged_echo(&candidate, expected) {
                return actual[candidate_end..].to_vec();
            }
            if candidates >= 128 {
                break;
            }
        }
        if line_end >= actual.len() {
            break;
        }
        line_start = line_end;
    }
    actual.to_vec()
}

fn logical_echo_line(actual: &[u8], start: usize) -> (Vec<u8>, usize) {
    let mut line = Vec::new();
    let mut index = start;
    while index < actual.len() {
        if actual[index..].starts_with(b"\r\r\n") {
            index += 3;
            continue;
        }
        if actual[index..].starts_with(b"\r\n") {
            return (line, index + 2);
        }
        if matches!(actual[index], b'\r' | b'\n') {
            return (line, index + 1);
        }
        line.push(actual[index]);
        index += 1;
    }
    (line, actual.len())
}

fn is_echo_candidate_boundary(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'#' | b'$' | b'>' | b':' | b']')
}

fn looks_like_damaged_echo(candidate: &[u8], expected: &[u8]) -> bool {
    if expected.starts_with(candidate) || candidate.starts_with(expected) {
        return true;
    }
    // Fuzzy matching is too permissive for one- to four-byte bootloader
    // commands (for example "?"). Only an exact partial/full prefix can
    // identify their echo reliably.
    if expected.len() <= 4 {
        return false;
    }
    let max_edits = (expected.len() / 4).clamp(1, 16);
    let candidate = if candidate.len() > expected.len() {
        &candidate[..expected.len()]
    } else {
        candidate
    };
    edit_distance_within(candidate, expected, max_edits)
}

/// Banded Levenshtein check. Echo comparison is bounded to at most 16 edits,
/// keeping this O(command length * edit bound) rather than quadratic.
fn edit_distance_within(left: &[u8], right: &[u8], max_edits: usize) -> bool {
    if left.len().abs_diff(right.len()) > max_edits {
        return false;
    }
    let infinity = max_edits + 1;
    let mut previous = vec![infinity; right.len() + 1];
    let mut current = vec![infinity; right.len() + 1];
    for (index, value) in previous
        .iter_mut()
        .enumerate()
        .take(right.len().min(max_edits) + 1)
    {
        *value = index;
    }

    for left_index in 1..=left.len() {
        let band_start = left_index.saturating_sub(max_edits);
        let band_end = right.len().min(left_index.saturating_add(max_edits));
        if band_start == 0 {
            current[0] = left_index;
        } else {
            // `current[band_start - 1]` feeds the first insertion candidate;
            // reset this one cell because the two row buffers are reused.
            current[band_start - 1] = infinity;
        }
        for right_index in band_start.max(1)..=band_end {
            let substitution = previous[right_index - 1]
                + if left[left_index - 1] == right[right_index - 1] {
                    0
                } else {
                    1
                };
            let deletion = previous[right_index].saturating_add(1);
            let insertion = current[right_index - 1].saturating_add(1);
            current[right_index] = substitution.min(deletion).min(insertion);
        }
        if band_end < right.len() {
            // The next row can grow the band by one; keep that newly visible
            // deletion cell authoritative instead of reusing stale data.
            current[band_end + 1] = infinity;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[right.len()] <= max_edits
}

enum Frame {
    Event(Box<TimelineEvent>),
    Gap(String),
    Ready,
    Other,
}

/// Completion decision for one bounded capture. Quiet is armed only when the
/// caller explicitly selected a quiet boundary (or had no exact boundary).
/// An exact prompt/literal request must match or reach the overall timeout;
/// otherwise a transient device or scheduler gap can return partial output.
struct CompletionWatcher {
    deadline: tokio::time::Instant,
    quiet: Option<Duration>,
    patterns: Vec<CompletionPattern>,
    until_regex: Option<regex::Regex>,
    last_activity: Option<tokio::time::Instant>,
}

impl CompletionWatcher {
    fn new(options: CaptureOptions) -> Self {
        // A supplied regex is the caller's sole authoritative boundary. Keep
        // this invariant in the capture core as well as in command argument
        // validation so future callers cannot accidentally let a prompt or a
        // quiet interval pre-empt a delayed regex marker.
        let regex_is_authoritative = options.until_regex.is_some();
        let quiet = (options.complete_on_quiet && !regex_is_authoritative).then_some(options.quiet);
        let patterns = if regex_is_authoritative {
            Vec::new()
        } else {
            options.patterns
        };
        Self {
            deadline: tokio::time::Instant::now() + options.timeout,
            quiet,
            patterns,
            until_regex: options.until_regex,
            last_activity: (quiet.is_some() && options.allow_empty_quiet)
                .then(tokio::time::Instant::now),
        }
    }

    fn observe_rx(&mut self, now: tokio::time::Instant) {
        if self.quiet.is_some() {
            self.last_activity = Some(now);
        }
    }

    fn quiet_deadline(&self) -> Option<tokio::time::Instant> {
        self.last_activity
            .zip(self.quiet)
            .map(|(last, quiet)| last + quiet)
    }

    /// Next instant at which the capture could finish without new input.
    fn wake_at(&self) -> tokio::time::Instant {
        self.quiet_deadline()
            .map_or(self.deadline, |quiet| quiet.min(self.deadline))
    }

    /// Decide whether the capture should finish before waiting for more input.
    fn poll(&self, rolling: &str, now: tokio::time::Instant) -> Option<Completion> {
        let normalized = terminal_text(rolling.as_bytes());
        if let Some(completion) = matched_pattern(&normalized, &self.patterns) {
            return Some(completion);
        }
        if let Some(regex) = &self.until_regex
            && regex.is_match(&normalized)
        {
            return Some(Completion::Regex(regex.as_str().to_string()));
        }
        if let Some(quiet) = self.quiet
            && let Some(last) = self.last_activity
            && now.duration_since(last) >= quiet
        {
            return Some(Completion::Quiet);
        }
        if now >= self.deadline {
            return Some(Completion::Timeout);
        }
        None
    }

    /// Decide the outcome once the scheduled wake-up elapsed.
    fn expired(&self, now: tokio::time::Instant) -> Completion {
        if self.quiet_deadline().is_some_and(|quiet| now >= quiet) {
            Completion::Quiet
        } else {
            Completion::Timeout
        }
    }
}

fn matched_pattern(text: &str, patterns: &[CompletionPattern]) -> Option<Completion> {
    let text = terminal_text(text.as_bytes());
    patterns.iter().find_map(|pattern| match pattern {
        CompletionPattern::Literal(pattern) => {
            let pattern = terminal_text(pattern.as_bytes());
            (!pattern.is_empty() && text.contains(pattern.as_str()))
                .then_some(Completion::Pattern(pattern))
        }
        CompletionPattern::Prompt(prompt) => {
            let prompt = terminal_text(prompt.as_bytes());
            let prompt = prompt.trim_end_matches('\n');
            (!prompt.is_empty() && text.split('\n').any(|line| line.ends_with(prompt)))
                .then(|| Completion::Prompt(prompt.to_string()))
        }
    })
}

fn append_rolling(rolling: &mut String, value: &str) {
    rolling.push_str(value);
    const MAX_ROLLING_CHARS: usize = 64 * 1024;
    if rolling.len() > MAX_ROLLING_CHARS {
        let mut start = rolling.len() - MAX_ROLLING_CHARS;
        while !rolling.is_char_boundary(start) {
            start += 1;
        }
        rolling.drain(..start);
    }
}

async fn send_control(socket: &mut Socket, message: &ClientMessage) -> Result<()> {
    socket
        .send(Message::Binary(encode_client_control(message)?.into()))
        .await?;
    Ok(())
}

async fn next_frame(socket: &mut Socket) -> Result<WireFrame> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => return Ok(decode_wire_frame(&bytes)?),
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(Message::Close(frame))) => bail!("seriald WebSocket closed: {frame:?}"),
            Some(Ok(Message::Text(_)))
            | Some(Ok(Message::Pong(_)))
            | Some(Ok(Message::Frame(_))) => {}
            Some(Err(error)) => return Err(error.into()),
            None => bail!("seriald WebSocket connection ended"),
        }
    }
}

fn ws_url(endpoint: &str) -> Result<String> {
    let rest = endpoint
        .strip_prefix("http://")
        .context("seriald endpoint is not an http:// origin")?;
    Ok(format!("ws://{rest}/api/v1/ws"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{Direction, EventKind};

    fn event(
        seq: u64,
        direction: Direction,
        data: &[u8],
        operation_id: Option<Uuid>,
    ) -> TimelineEvent {
        TimelineEvent {
            slot_id: "bench".into(),
            daemon_epoch: Uuid::nil(),
            seq,
            generation: 1,
            wall_time_ns: 0,
            monotonic_time_ns: 0,
            kind: match direction {
                Direction::Rx => EventKind::Rx,
                Direction::Tx => EventKind::Tx,
                Direction::None => EventKind::Checkpoint,
            },
            direction,
            actor: None,
            run_id: None,
            operation_id,
            stream_offset_start: None,
            stream_offset_end: None,
            data: data.to_vec(),
            metadata: Default::default(),
            durable: true,
        }
    }

    #[test]
    fn watched_run_abort_is_an_immediate_reasoned_completion() {
        let run_id = Uuid::new_v4();
        let mut aborted = event(42, Direction::None, b"", None);
        aborted.kind = EventKind::RunAborted;
        aborted.run_id = Some(run_id);
        aborted
            .metadata
            .insert("reason".into(), serde_json::json!("human takeover"));

        assert_eq!(
            run_abort_completion(&aborted, Some(run_id)),
            Some(Completion::RunAborted {
                run_id,
                reason: "human takeover".into(),
            })
        );
        let completion = Completion::RunAborted {
            run_id,
            reason: "human takeover".into(),
        };
        assert!(
            !completion.is_complete(),
            "an aborted Run is a failure, not successful capture completion"
        );
        assert_eq!(run_abort_completion(&aborted, Some(Uuid::new_v4())), None);
        assert_eq!(run_abort_completion(&aborted, None), None);
    }

    #[test]
    fn rolling_matcher_keeps_recent_utf8_at_a_character_boundary() {
        let mut rolling = "界".repeat(30_000);
        append_rolling(&mut rolling, "SigmaStar #");
        assert!(rolling.contains("SigmaStar #"));
        assert!(rolling.is_char_boundary(0));
        assert!(rolling.len() <= 64 * 1024);
    }

    #[test]
    fn pattern_matching_is_literal_and_deterministic() {
        assert_eq!(
            matched_pattern(
                "boot\nSigmaStar #",
                &[
                    CompletionPattern::Literal("$ ".into()),
                    CompletionPattern::Literal("SigmaStar #".into())
                ]
            ),
            Some(Completion::Pattern("SigmaStar #".into()))
        );
    }

    #[test]
    fn prompt_matching_requires_a_normalized_line_end() {
        let prompt = [CompletionPattern::Prompt("SigmaStar #".into())];
        assert_eq!(
            matched_pattern("boot\r\nSigmaStar #", &prompt),
            Some(Completion::Prompt("SigmaStar #".into()))
        );
        assert_eq!(
            matched_pattern("message mentions SigmaStar # but continues", &prompt),
            None
        );
    }

    fn watcher(
        patterns: &[&str],
        complete_on_quiet: bool,
        allow_empty_quiet: bool,
    ) -> CompletionWatcher {
        CompletionWatcher::new(CaptureOptions {
            timeout: Duration::from_secs(60),
            quiet: Duration::from_millis(300),
            patterns: patterns
                .iter()
                .map(|pattern| CompletionPattern::Literal((*pattern).to_string()))
                .collect(),
            until_regex: None,
            complete_on_quiet,
            allow_empty_quiet,
        })
    }

    #[test]
    fn until_regex_completes_on_the_first_window_match() {
        let mut regex_watcher = watcher(&["never this literal"], false, false);
        regex_watcher.until_regex = Some(regex::Regex::new(r"U-Boot \d+\.\d+").unwrap());
        assert_eq!(
            regex_watcher.poll("booting...", tokio::time::Instant::now()),
            None
        );
        assert_eq!(
            regex_watcher.poll("U-Boot 2023.10 ready", tokio::time::Instant::now()),
            Some(Completion::Regex(r"U-Boot \d+\.\d+".into()))
        );
    }

    #[test]
    fn regex_is_authoritative_when_a_background_command_returns_prompt_first() {
        let both = CompletionWatcher::new(CaptureOptions {
            timeout: Duration::from_secs(60),
            quiet: Duration::from_millis(300),
            patterns: vec![CompletionPattern::Literal("]# ".into())],
            until_regex: Some(regex::Regex::new(r"__BACKGROUND_DONE__=\d+").unwrap()),
            complete_on_quiet: true,
            allow_empty_quiet: true,
        });
        let now = tokio::time::Instant::now();
        assert_eq!(both.poll("[root@luckfox ~]# ", now), None);
        assert_eq!(
            both.poll(
                "[root@luckfox ~]# \n__BACKGROUND_DONE__=0",
                now + Duration::from_secs(1)
            ),
            Some(Completion::Regex(r"__BACKGROUND_DONE__=\d+".into()))
        );
        assert!(both.quiet.is_none());
        assert!(both.patterns.is_empty());
    }

    #[test]
    fn prompt_capture_ignores_transient_quiet_and_waits_for_the_prompt() {
        let mut watcher = watcher(&["]# "], false, false);
        let rx_at = tokio::time::Instant::now();
        watcher.observe_rx(rx_at);
        let after_gap = rx_at + Duration::from_millis(300);
        assert_eq!(watcher.poll("i=1;", after_gap), None);
        assert_eq!(watcher.wake_at(), watcher.deadline);

        assert_eq!(
            watcher.poll("i=1;\n...\n[root@luckfox tmp]# ", after_gap),
            Some(Completion::Pattern("]# ".into()))
        );
    }

    #[test]
    fn pattern_match_wins_over_quiet() {
        let mut watcher = watcher(&["SigmaStar #"], true, false);
        let rx_at = tokio::time::Instant::now();
        watcher.observe_rx(rx_at);
        let after_gap = rx_at + Duration::from_secs(1);
        assert_eq!(
            watcher.poll("boot done\nSigmaStar #", after_gap),
            Some(Completion::Pattern("SigmaStar #".into()))
        );
    }

    #[test]
    fn quiet_requires_rx_activity_unless_empty_quiet_is_allowed() {
        let armed_by_rx = watcher(&[], true, false);
        assert_eq!(armed_by_rx.poll("", tokio::time::Instant::now()), None);
        let empty_quiet = watcher(&[], true, true);
        let later = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(empty_quiet.poll("", later), Some(Completion::Quiet));
    }

    #[test]
    fn disabled_quiet_does_not_arm_even_when_empty_quiet_was_requested() {
        let no_quiet = watcher(&["ready"], false, true);
        let later = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(no_quiet.poll("", later), None);
        assert_eq!(no_quiet.wake_at(), no_quiet.deadline);
    }

    #[test]
    fn timeout_fires_when_neither_pattern_nor_quiet_does() {
        let mut options_watcher = watcher(&["never"], false, false);
        options_watcher.deadline = tokio::time::Instant::now() + Duration::from_millis(1);
        let later = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(
            options_watcher.poll("noise", later),
            Some(Completion::Timeout)
        );
        assert_eq!(options_watcher.expired(later), Completion::Timeout);
    }

    #[test]
    fn external_terminal_signal_is_an_explicit_complete_boundary() {
        let completion = Completion::Signal("trigger_terminal".into());
        assert_eq!(completion.label(), "signal:trigger_terminal");
        assert!(completion.is_complete());
    }

    #[test]
    fn pre_tx_prompt_is_excluded_and_full_echo_keeps_high_confidence() {
        let operation_id = Uuid::new_v4();
        let mut boundary = CommandBoundaryTracker::new(
            CommandBoundary {
                tx_event_seq: 12,
                operation_id,
                expected_echo: Some(b"help\r".to_vec()),
            },
            CaptureLimits::default(),
        );
        let mut prompt = watcher(&["]# "], false, false);
        let mut rolling = String::new();

        // This prompt was replayed by attach before the write.
        let accepted = boundary.accept(event(10, Direction::Rx, b"[root@luckfox ~]# ", None));
        assert!(accepted.events.is_empty());
        assert!(accepted.matching_rx.is_empty());
        assert_eq!(prompt.poll(&rolling, tokio::time::Instant::now()), None);

        // The write RPC's event_seq identifies the authoritative TX audit.
        let accepted = boundary.accept(event(12, Direction::Tx, b"help\r", Some(operation_id)));
        assert!(accepted.events.is_empty());
        assert!(boundary.tx_audit_observed);

        // The true command echo resets matching and only its following output
        // enters the high-confidence primary capture window.
        let accepted = boundary.accept(event(
            13,
            Direction::Rx,
            b"help\r\r\nreal output\r\n[root@luckfox ~]# ",
            None,
        ));
        assert!(accepted.reset_matching);
        rolling.clear();
        assert!(accepted.observed_post_tx_rx);
        prompt.observe_rx(tokio::time::Instant::now());
        append_rolling(
            &mut rolling,
            &String::from_utf8_lossy(&accepted.matching_rx),
        );
        for event in accepted.events {
            assert!(event.seq > 12);
        }
        assert!(!rolling.starts_with("[root@luckfox ~]# "));
        assert!(rolling.contains("real output"));
        assert_eq!(
            prompt.poll(&rolling, tokio::time::Instant::now()),
            Some(Completion::Pattern("]# ".into()))
        );

        let (result, pending) = boundary.finish();
        assert!(pending.is_empty());
        assert_eq!(result.prewrite_activity.event_count, 1);
        assert!(result.tx_audit_observed);
        assert!(result.echo_observed);
        assert_eq!(
            result.confidence(&Completion::Pattern("]# ".into())),
            "echo_confirmed"
        );
        assert!(!result.interfered);
    }

    #[test]
    fn explicit_post_tx_boundary_can_complete_with_missing_echo() {
        let operation_id = Uuid::new_v4();
        let mut boundary = CommandBoundaryTracker::new(
            CommandBoundary {
                tx_event_seq: 20,
                operation_id,
                expected_echo: Some(b"very-long-command\r".to_vec()),
            },
            CaptureLimits::default(),
        );
        let mut prompt = watcher(&["]# "], false, false);
        let mut rolling = String::new();

        // A prompt before the confirmed TX sequence remains excluded.
        let before = boundary.accept(event(19, Direction::Rx, b"old ]# ", None));
        assert!(before.matching_rx.is_empty());

        boundary.accept(event(
            20,
            Direction::Tx,
            b"very-long-command\r",
            Some(operation_id),
        ));
        // The target emitted only a partial echo before its exact prompt.
        let accepted = boundary.accept(event(
            21,
            Direction::Rx,
            b"very-long-com\r\nresult\r\n[root@luckfox ~]# ",
            None,
        ));
        assert!(accepted.reset_matching);
        assert!(accepted.observed_post_tx_rx);
        rolling.clear();
        prompt.observe_rx(tokio::time::Instant::now());
        append_rolling(
            &mut rolling,
            &String::from_utf8_lossy(&accepted.matching_rx),
        );
        let completion = prompt
            .poll(&rolling, tokio::time::Instant::now())
            .expect("post-TX prompt should complete the bounded capture");
        assert_eq!(completion, Completion::Pattern("]# ".into()));

        let (result, pending) = boundary.finish();
        assert!(!result.echo_observed);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].seq, 21);
        assert_eq!(
            result.confidence(&completion),
            "post_tx_boundary_without_echo"
        );
    }

    #[test]
    fn echo_boundary_tolerates_target_cr_cr_lf_hard_wraps() {
        let expected = b"printf 1234567890\r";
        let actual = b"stale prompt\r\nprintf 1234\r\r\n567890\r\r\nresult\r\n";
        let end = find_echo_end(actual, expected).expect("wrapped echo should match");
        assert_eq!(&actual[end..], b"result\r\n");
    }

    #[test]
    fn partial_echo_cannot_match_a_boundary_embedded_in_the_command() {
        let expected = b"printf __DONE__\r";
        assert!(
            fallback_after_incomplete_echo(b"printf __DONE__", expected).is_empty(),
            "a still-valid echo prefix must remain hidden from completion"
        );
        assert!(
            fallback_after_incomplete_echo(b"[root@luckfox ~]# printf __DONE__", expected)
                .is_empty(),
            "a stale prompt before a partial real echo must also remain hidden"
        );
        assert_eq!(
            fallback_after_incomplete_echo(
                b"printf __DON\r\nreal __DONE__\r\n[root@luckfox ~]# ",
                expected
            ),
            b"real __DONE__\r\n[root@luckfox ~]# "
        );
    }

    #[test]
    fn no_echo_prompt_containing_the_command_first_byte_is_preserved() {
        assert_eq!(
            fallback_after_incomplete_echo(b"root@dut#", b"reboot\r"),
            b"root@dut#"
        );
        assert_eq!(
            fallback_after_incomplete_echo(b"ready\r\nroot@dut#", b"reboot\r"),
            b"ready\r\nroot@dut#"
        );
    }

    #[test]
    fn short_commands_disable_fuzzy_echo_classification() {
        assert_eq!(
            fallback_after_incomplete_echo(b"root@dut#", b"?\r"),
            b"root@dut#"
        );
        assert_eq!(
            fallback_after_incomplete_echo(b"! ready\r\nroot@dut#", b"?\r"),
            b"! ready\r\nroot@dut#"
        );
        assert!(
            fallback_after_incomplete_echo(b"?", b"?\r").is_empty(),
            "an exact partial echo remains protected"
        );
    }

    #[test]
    fn banded_echo_distance_accepts_small_damage_only() {
        assert!(edit_distance_within(b"eXcho READY", b"echo READY", 2));
        assert!(!edit_distance_within(b"root@d", b"reboot", 1));
    }

    #[test]
    fn damaged_echo_same_line_remainder_cannot_complete_a_boundary() {
        assert!(fallback_after_incomplete_echo(b"eXcho READY\r", b"echo READY\r").is_empty());
        assert_eq!(
            fallback_after_incomplete_echo(
                b"[root@dut]# eXcho READY\r\nreal READY\r\n",
                b"echo READY\r"
            ),
            b"real READY\r\n"
        );
    }

    #[test]
    fn foreign_prewrite_tx_downgrades_an_identical_echo_boundary() {
        let operation_id = Uuid::new_v4();
        let mut boundary = CommandBoundaryTracker::new(
            CommandBoundary {
                tx_event_seq: 20,
                operation_id,
                expected_echo: Some(b"help\r".to_vec()),
            },
            CaptureLimits::default(),
        );

        boundary.accept(event(18, Direction::Tx, b"help\r", Some(Uuid::new_v4())));
        boundary.accept(event(20, Direction::Tx, b"help\r", Some(operation_id)));
        let accepted = boundary.accept(event(
            21,
            Direction::Rx,
            b"help\r\r\n[root@luckfox ~]# ",
            None,
        ));
        assert!(accepted.reset_matching);

        let (result, pending) = boundary.finish();
        assert!(pending.is_empty());
        assert!(result.interfered);
        assert_eq!(result.prewrite_activity.tx_event_count, 1);
        assert_eq!(
            result.confidence(&Completion::Pattern("]# ".into())),
            "echo_observed_interfered"
        );
    }

    #[test]
    fn quiet_capture_requires_post_tx_rx_evidence() {
        let mut quiet = watcher(&[], true, false);
        let after_gap = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(quiet.poll("", after_gap), None);
        quiet.observe_rx(after_gap);
        assert_eq!(
            quiet.poll("", after_gap + Duration::from_secs(1)),
            Some(Completion::Quiet)
        );
    }
}
