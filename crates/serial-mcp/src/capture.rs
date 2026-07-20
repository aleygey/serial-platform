use std::{collections::VecDeque, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, PROTOCOL_VERSION, ServerMessage, Subscription, TimelineEvent,
    WireFrame, decode_wire_frame, encode_client_control,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

use crate::config::CaptureLimits;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct Capture {
    socket: Socket,
    slot_id: String,
    events: VecDeque<TimelineEvent>,
    retained_bytes: usize,
    truncated: bool,
    gaps: Vec<String>,
    limits: CaptureLimits,
}

pub struct CaptureOptions {
    pub timeout: Duration,
    pub quiet: Duration,
    pub patterns: Vec<String>,
    pub allow_empty_quiet: bool,
}

pub struct CaptureResult {
    pub events: Vec<TimelineEvent>,
    pub truncated: bool,
    pub gaps: Vec<String>,
    pub completion: Completion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Completion {
    Pattern(String),
    Quiet,
    Timeout,
    Disconnected(String),
}

impl Completion {
    pub fn label(&self) -> String {
        match self {
            Self::Pattern(pattern) => format!("pattern:{pattern}"),
            Self::Quiet => "quiet".into(),
            Self::Timeout => "timeout".into(),
            Self::Disconnected(reason) => format!("disconnected:{reason}"),
        }
    }

    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Pattern(_) | Self::Quiet)
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
                WireFrame::Control(ServerMessage::Welcome { .. }) => break,
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

    pub async fn collect(mut self, options: CaptureOptions) -> CaptureResult {
        let mut watcher = CompletionWatcher::new(options);
        let mut rolling = self.rx_text();

        loop {
            let now = tokio::time::Instant::now();
            if let Some(completion) = watcher.poll(&rolling, now) {
                return self.finish(completion);
            }

            match tokio::time::timeout_at(watcher.wake_at(), self.next()).await {
                Ok(Ok(Frame::Event(event))) => {
                    let event = *event;
                    if event.direction == serial_protocol::Direction::Rx {
                        watcher.observe_rx(tokio::time::Instant::now());
                        append_rolling(&mut rolling, &String::from_utf8_lossy(&event.data));
                    }
                    self.push(event);
                }
                Ok(Ok(Frame::Gap(gap))) => self.gaps.push(gap),
                Ok(Ok(Frame::Ready | Frame::Other)) => {}
                Ok(Err(error)) => {
                    return self.finish(Completion::Disconnected(error.to_string()));
                }
                Err(_) => {
                    return self.finish(watcher.expired(tokio::time::Instant::now()));
                }
            }
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

    fn rx_text(&self) -> String {
        let bytes: Vec<u8> = self
            .events
            .iter()
            .filter(|event| event.direction == serial_protocol::Direction::Rx)
            .flat_map(|event| event.data.iter().copied())
            .collect();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn finish(self, completion: Completion) -> CaptureResult {
        CaptureResult {
            events: self.events.into_iter().collect(),
            truncated: self.truncated,
            gaps: self.gaps,
            completion,
        }
    }
}

enum Frame {
    Event(Box<TimelineEvent>),
    Gap(String),
    Ready,
    Other,
}

/// Completion decision for one bounded capture. Quiet is armed independently
/// of the prompt patterns: in auto/prompt modes a spammy device that never
/// shows its prompt still finishes on the first RX gap of `quiet`, whichever
/// condition is met first. Pattern matches win over quiet, quiet wins over
/// the overall timeout.
struct CompletionWatcher {
    deadline: tokio::time::Instant,
    quiet: Duration,
    patterns: Vec<String>,
    last_activity: Option<tokio::time::Instant>,
}

impl CompletionWatcher {
    fn new(options: CaptureOptions) -> Self {
        Self {
            deadline: tokio::time::Instant::now() + options.timeout,
            quiet: options.quiet,
            patterns: options.patterns,
            last_activity: options.allow_empty_quiet.then(tokio::time::Instant::now),
        }
    }

    fn observe_rx(&mut self, now: tokio::time::Instant) {
        self.last_activity = Some(now);
    }

    fn quiet_deadline(&self) -> Option<tokio::time::Instant> {
        self.last_activity.map(|last| last + self.quiet)
    }

    /// Next instant at which the capture could finish without new input.
    fn wake_at(&self) -> tokio::time::Instant {
        self.quiet_deadline()
            .map_or(self.deadline, |quiet| quiet.min(self.deadline))
    }

    /// Decide whether the capture should finish before waiting for more input.
    fn poll(&self, rolling: &str, now: tokio::time::Instant) -> Option<Completion> {
        if let Some(pattern) = matched_pattern(rolling, &self.patterns) {
            return Some(Completion::Pattern(pattern));
        }
        if let Some(last) = self.last_activity
            && now.duration_since(last) >= self.quiet
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

fn matched_pattern(text: &str, patterns: &[String]) -> Option<String> {
    patterns
        .iter()
        .find(|pattern| !pattern.is_empty() && text.contains(pattern.as_str()))
        .cloned()
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
            matched_pattern("boot\nSigmaStar #", &["$ ".into(), "SigmaStar #".into()]),
            Some("SigmaStar #".into())
        );
    }

    fn watcher(patterns: &[&str], allow_empty_quiet: bool) -> CompletionWatcher {
        CompletionWatcher::new(CaptureOptions {
            timeout: Duration::from_secs(60),
            quiet: Duration::from_millis(300),
            patterns: patterns.iter().map(|pattern| pattern.to_string()).collect(),
            allow_empty_quiet,
        })
    }

    #[test]
    fn quiet_stays_armed_when_prompt_patterns_are_configured() {
        // A spammy device with configured prompts must still finish on quiet:
        // the pattern never matches, but the first RX gap of `quiet` completes.
        let mut watcher = watcher(&["SigmaStar #"], false);
        let rx_at = tokio::time::Instant::now();
        watcher.observe_rx(rx_at);
        let during_burst = rx_at + Duration::from_millis(100);
        assert_eq!(watcher.poll("spam spam spam", during_burst), None);
        let after_gap = rx_at + Duration::from_millis(300);
        assert_eq!(
            watcher.poll("spam spam spam", after_gap),
            Some(Completion::Quiet)
        );
        assert_eq!(watcher.expired(after_gap), Completion::Quiet);
    }

    #[test]
    fn pattern_match_wins_over_quiet() {
        let mut watcher = watcher(&["SigmaStar #"], false);
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
        let armed_by_rx = watcher(&[], false);
        assert_eq!(armed_by_rx.poll("", tokio::time::Instant::now()), None);
        let empty_quiet = watcher(&[], true);
        let later = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(empty_quiet.poll("", later), Some(Completion::Quiet));
    }

    #[test]
    fn timeout_fires_when_neither_pattern_nor_quiet_does() {
        let mut options_watcher = watcher(&["never"], false);
        options_watcher.deadline = tokio::time::Instant::now() + Duration::from_millis(1);
        let later = tokio::time::Instant::now() + Duration::from_secs(1);
        assert_eq!(
            options_watcher.poll("noise", later),
            Some(Completion::Timeout)
        );
        assert_eq!(options_watcher.expired(later), Completion::Timeout);
    }
}
