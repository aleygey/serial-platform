use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, Direction, EventKind, EventQuery, LoggingState,
    PROTOCOL_VERSION, ServerMessage, SessionState, SlotSnapshot, Subscription, WireFrame,
    decode_wire_frame, encode_client_control,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};
use uuid::Uuid;

use crate::{
    api::{ApiClient, is_forbidden, is_not_found},
    cli::{DoctorSlotArgs, DoctorStreamArgs, OutputArgs},
    display::safe_inline,
};

#[derive(Debug, Serialize)]
struct PortReport {
    slot_id: String,
    port: String,
    port_discovered: bool,
    discovery_source: &'static str,
    enabled: bool,
    session_state: SessionState,
    endpoint_present: bool,
    state_code: Option<serial_protocol::ErrorCode>,
    state_reason: Option<String>,
    generation: u64,
    rx_offset: u64,
    tx_offset: u64,
    rx_overflow_bytes: u64,
    subscriber_count: Option<usize>,
    subscriber_lag_events: Option<u64>,
    assessment: &'static str,
    recent_control_events: Vec<serial_protocol::TimelineEvent>,
    recent_events_error: Option<String>,
}

pub async fn port(api: &ApiClient, args: DoctorSlotArgs) -> Result<()> {
    let status = api.status().await?;
    let snapshot = find_slot(&status.slots, &args.slot)?.clone();
    let (port_discovered, discovery_source) = match api.ports().await {
        Ok(ports) => (
            ports
                .iter()
                .any(|port| same_serial_port(&port.name, &snapshot.config.port)),
            "daemon_port_enumeration",
        ),
        Err(error) if is_forbidden(&error) || is_not_found(&error) => {
            (snapshot.endpoint_present, "authoritative_slot_snapshot")
        }
        Err(error) => return Err(error),
    };
    let diagnostics = match api.slot_diagnostics(&args.slot).await {
        Ok(diagnostics) => Some(diagnostics),
        Err(error) if is_not_found(&error) => None,
        Err(error) => return Err(error),
    };
    let (recent, recent_events_error) = match api
        .events(
            &args.slot,
            &EventQuery {
                epoch: Some(snapshot.daemon_epoch),
                after_seq: Some(snapshot.head_seq.saturating_sub(100)),
                through_seq: None,
                before_wall_time_ns: None,
                after_wall_time_ns: None,
                direction: Some(Direction::None),
                kind: None,
                actor_id: None,
                run_id: None,
                operation_id: None,
                contains: None,
                regex: None,
                limit_events: Some(100),
                limit_bytes: Some(256 * 1024),
            },
        )
        .await
    {
        Ok(response) => (response.events, None),
        Err(error) => (Vec::new(), Some(error.to_string())),
    };
    let assessment = assess_port(&snapshot);
    let report = PortReport {
        slot_id: args.slot,
        port: snapshot.config.port.clone(),
        port_discovered,
        discovery_source,
        enabled: snapshot.config.enabled,
        session_state: snapshot.session_state,
        endpoint_present: snapshot.endpoint_present,
        state_code: snapshot.state_code,
        state_reason: snapshot.state_reason.clone(),
        generation: snapshot.generation,
        rx_offset: snapshot.rx_offset,
        tx_offset: snapshot.tx_offset,
        rx_overflow_bytes: snapshot.rx_overflow_bytes,
        subscriber_count: diagnostics.as_ref().map(|value| value.subscriber_count),
        subscriber_lag_events: diagnostics
            .as_ref()
            .map(|value| value.subscriber_lag_events),
        assessment,
        recent_control_events: recent,
        recent_events_error,
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Slot       {}", safe_inline(&report.slot_id));
        println!("Port       {}", safe_inline(&report.port));
        println!(
            "Discovery  {} ({})",
            if report.port_discovered {
                "present"
            } else {
                "missing"
            },
            report.discovery_source
        );
        println!(
            "Session    {:?} (generation {})",
            report.session_state, report.generation
        );
        println!("Assessment {}", report.assessment);
        if let Some(code) = report.state_code {
            println!("State code {:?}", code);
        }
        if let Some(reason) = &report.state_reason {
            println!("Reason     {}", safe_inline(reason));
        }
        println!(
            "Counters   rx={} tx={} overflow={}",
            report.rx_offset, report.tx_offset, report.rx_overflow_bytes
        );
        if let (Some(subscribers), Some(lag)) =
            (report.subscriber_count, report.subscriber_lag_events)
        {
            println!("Consumers  {subscribers} attached, {lag} lagged event(s)");
        }
        if !report.recent_control_events.is_empty() {
            println!("Recent port lifecycle:");
            for event in report.recent_control_events.iter().rev().take(8).rev() {
                println!(
                    "  #{} {:?} {}",
                    event.seq,
                    event.kind,
                    safe_inline(&format!("{:?}", event.metadata))
                );
            }
        }
        if let Some(error) = &report.recent_events_error {
            println!("History    unavailable ({})", safe_inline(error));
        }
    }
    Ok(())
}

fn assess_port(snapshot: &SlotSnapshot) -> &'static str {
    if !snapshot.config.enabled {
        "slot_disabled"
    } else if !snapshot.endpoint_present {
        "port_not_present"
    } else {
        match snapshot.session_state {
            SessionState::Online => "online",
            SessionState::Opening => "opening",
            SessionState::Backoff => "open_failed_backoff",
            SessionState::WaitingForPort => "waiting_for_port",
            SessionState::Stopping => "stopping",
            SessionState::Disabled => "slot_disabled",
        }
    }
}

#[derive(Debug, Serialize)]
struct StorageFallback {
    source: &'static str,
    usage_bytes: u64,
    max_bytes: Option<u64>,
    archive_count: usize,
    catalog_truncated: bool,
    degraded_slots: Vec<String>,
    note: &'static str,
}

pub async fn storage(api: &ApiClient, args: OutputArgs) -> Result<()> {
    match api.storage_diagnostics().await {
        Ok(report) => {
            if args.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("Source     authoritative daemon diagnostics");
                println!(
                    "Usage      {} / {} bytes",
                    report.usage_bytes, report.max_bytes
                );
                println!(
                    "Retention  {} bytes (segment {} bytes)",
                    report.retention_target_bytes, report.segment_max_bytes
                );
                println!("Archives   {}", report.archive_count);
                println!(
                    "Writer     {}/{} queue entries free",
                    report.writer_queue_remaining, report.writer_queue_capacity
                );
                println!("Logging    {:?}", report.logging);
            }
            return Ok(());
        }
        Err(error) if is_not_found(&error) => {}
        Err(error) => return Err(error),
    }

    let archives = api.archives(None).await?;
    let status = api.status().await?;
    let report = StorageFallback {
        source: "archive_catalog_fallback",
        usage_bytes: archives
            .archives
            .iter()
            .map(|archive| archive.total_bytes)
            .sum(),
        max_bytes: None,
        archive_count: archives.archives.len(),
        catalog_truncated: archives.truncated,
        degraded_slots: status
            .slots
            .iter()
            .filter(|slot| slot.logging == LoggingState::Degraded)
            .map(|slot| slot.config.id.clone())
            .collect(),
        note: "upgrade seriald for authoritative quota and writer-queue metrics",
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Source     {}", report.source);
        println!("Usage      at least {} bytes", report.usage_bytes);
        println!("Archives   {}", report.archive_count);
        println!("Quota      unavailable on this seriald");
        if !report.degraded_slots.is_empty() {
            println!(
                "Degraded   {}",
                safe_inline(&report.degraded_slots.join(", "))
            );
        }
        println!("Note       {}", report.note);
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct StateFallback {
    snapshot: SlotSnapshot,
    subscriber_count: Option<usize>,
    subscriber_lag_events: Option<u64>,
    source: &'static str,
}

pub async fn state(api: &ApiClient, args: DoctorSlotArgs) -> Result<()> {
    let report = match api.slot_diagnostics(&args.slot).await {
        Ok(diagnostics) => StateFallback {
            snapshot: diagnostics.snapshot,
            subscriber_count: Some(diagnostics.subscriber_count),
            subscriber_lag_events: Some(diagnostics.subscriber_lag_events),
            source: "authoritative_slot_diagnostics",
        },
        Err(error) if is_not_found(&error) => {
            let status = api.status().await?;
            StateFallback {
                snapshot: find_slot(&status.slots, &args.slot)?.clone(),
                subscriber_count: None,
                subscriber_lag_events: None,
                source: "status_fallback",
            }
        }
        Err(error) => return Err(error),
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        let slot = &report.snapshot;
        println!("Source     {}", report.source);
        println!(
            "Slot       {} ({})",
            safe_inline(&slot.config.id),
            safe_inline(&slot.config.display_name)
        );
        println!(
            "Session    {:?}/{:?}, generation {}",
            slot.session_state, slot.target_activity, slot.generation
        );
        println!(
            "Stream     head={} rx={} tx={} overflow={}",
            slot.head_seq, slot.rx_offset, slot.tx_offset, slot.rx_overflow_bytes
        );
        println!(
            "Control    {}",
            safe_inline(
                slot.control
                    .as_ref()
                    .map(|lease| lease.owner.label.as_str())
                    .unwrap_or("none")
            )
        );
        let run = slot
            .active_run
            .as_ref()
            .map(|run| format!("{} {:?} {}", run.id, run.status, safe_inline(&run.label)))
            .unwrap_or_else(|| "none".into());
        println!("Run        {}", run);
        println!(
            "Trigger    {}",
            slot.active_trigger
                .as_ref()
                .map(|trigger| format!("{} {:?}", trigger.id, trigger.status))
                .unwrap_or_else(|| "none".into())
        );
        println!(
            "Profiles   transport={} device={}",
            safe_inline(&slot.config.profile),
            safe_inline(slot.config.device_profile.as_deref().unwrap_or("Generic"))
        );
        println!(
            "Effective  transport={:?} pacing={:?} EOL={:?} echo={:?}",
            slot.effective_transport,
            slot.effective_write_pacing,
            slot.effective_write_eol,
            slot.effective_echo
        );
        println!(
            "Prompts    shell={} uboot={}",
            slot.effective_shell_prompt
                .as_deref()
                .map(safe_inline)
                .unwrap_or_else(|| "none".into()),
            slot.effective_uboot_prompt
                .as_deref()
                .map(safe_inline)
                .unwrap_or_else(|| "none".into())
        );
        if let Some(count) = report.subscriber_count {
            println!(
                "Consumers  {} attached, {} lagged event(s)",
                count,
                report.subscriber_lag_events.unwrap_or(0)
            );
        }
        if let Some(reason) = &slot.state_reason {
            println!("Reason     {}", safe_inline(reason));
        }
        if let Some(code) = slot.state_code {
            println!("State code {:?}", code);
        }
    }
    Ok(())
}

#[derive(Debug, Default, Serialize)]
struct LiveObservation {
    ready: bool,
    snapshot_generation: Option<u64>,
    rx_frames: u64,
    rx_bytes: u64,
    tx_frames: u64,
    tx_bytes: u64,
    timeline_events: u64,
    last_seq: Option<u64>,
    gap_events: u64,
    lagged_events: u64,
}

#[derive(Debug, Serialize)]
struct StreamReport {
    slot_id: String,
    duration_seconds: u64,
    daemon_epoch_before: Uuid,
    daemon_epoch_after: Uuid,
    generation_before: u64,
    generation_after: u64,
    head_seq_before: u64,
    head_seq_after: u64,
    rx_offset_before: u64,
    rx_offset_after: u64,
    rx_offset_delta: u64,
    rx_overflow_delta: u64,
    live: LiveObservation,
    journal_rx_events: usize,
    journal_rx_bytes: u64,
    journal_truncated: bool,
    journal_gaps: usize,
    assessment: &'static str,
}

pub async fn stream(api: &ApiClient, args: DoctorStreamArgs) -> Result<()> {
    let before_status = api.status().await?;
    let before = find_slot(&before_status.slots, &args.slot)?.clone();
    let live = observe_live(
        api.endpoint(),
        api.token(),
        &args.slot,
        Cursor {
            epoch: before.daemon_epoch,
            after_seq: before.head_seq,
        },
        Duration::from_secs(args.duration),
    )
    .await?;
    let after_status = api.status().await?;
    let after = find_slot(&after_status.slots, &args.slot)?.clone();

    let journal = if before.daemon_epoch == after.daemon_epoch {
        Some(
            api.events(
                &args.slot,
                &EventQuery {
                    epoch: Some(before.daemon_epoch),
                    after_seq: Some(before.head_seq),
                    through_seq: Some(after.head_seq),
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: Some(Direction::Rx),
                    kind: Some(EventKind::Rx),
                    actor_id: None,
                    run_id: None,
                    operation_id: None,
                    contains: None,
                    regex: None,
                    limit_events: Some(10_000),
                    limit_bytes: Some(4 * 1024 * 1024),
                },
            )
            .await?,
        )
    } else {
        None
    };
    let journal_rx_events = journal
        .as_ref()
        .map(|response| response.events.len())
        .unwrap_or(0);
    let journal_rx_bytes = journal
        .as_ref()
        .map(|response| {
            response
                .events
                .iter()
                .map(|event| event.data.len() as u64)
                .sum()
        })
        .unwrap_or(0);
    let journal_truncated = journal.as_ref().is_some_and(|response| response.truncated);
    let journal_gaps = journal
        .as_ref()
        .map(|response| response.gaps.len())
        .unwrap_or(0);
    let assessment = assess_stream(&before, &after, &live, journal_rx_events, journal_gaps);
    let report = StreamReport {
        slot_id: args.slot,
        duration_seconds: args.duration,
        daemon_epoch_before: before.daemon_epoch,
        daemon_epoch_after: after.daemon_epoch,
        generation_before: before.generation,
        generation_after: after.generation,
        head_seq_before: before.head_seq,
        head_seq_after: after.head_seq,
        rx_offset_before: before.rx_offset,
        rx_offset_after: after.rx_offset,
        rx_offset_delta: after.rx_offset.saturating_sub(before.rx_offset),
        rx_overflow_delta: after
            .rx_overflow_bytes
            .saturating_sub(before.rx_overflow_bytes),
        live,
        journal_rx_events,
        journal_rx_bytes,
        journal_truncated,
        journal_gaps,
        assessment,
    };
    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("Slot       {}", safe_inline(&report.slot_id));
        println!("Duration   {}s", report.duration_seconds);
        println!(
            "Offsets    rx {} -> {} (+{}), head {} -> {}",
            report.rx_offset_before,
            report.rx_offset_after,
            report.rx_offset_delta,
            report.head_seq_before,
            report.head_seq_after
        );
        println!(
            "WebSocket  ready={} rx={} frame(s)/{} bytes tx={} frame(s)/{} bytes",
            report.live.ready,
            report.live.rx_frames,
            report.live.rx_bytes,
            report.live.tx_frames,
            report.live.tx_bytes
        );
        println!(
            "Journal    {} RX event(s)/{} bytes, gaps={}, truncated={}",
            report.journal_rx_events,
            report.journal_rx_bytes,
            report.journal_gaps,
            report.journal_truncated
        );
        println!("Overflow   +{} bytes", report.rx_overflow_delta);
        println!("Assessment {}", report.assessment);
    }
    Ok(())
}

fn assess_stream(
    before: &SlotSnapshot,
    after: &SlotSnapshot,
    live: &LiveObservation,
    journal_rx_events: usize,
    journal_gaps: usize,
) -> &'static str {
    if before.daemon_epoch != after.daemon_epoch || before.generation != after.generation {
        "inconclusive_session_changed"
    } else if !after.config.enabled {
        "slot_disabled"
    } else if !after.endpoint_present {
        "port_not_present"
    } else if after.session_state != SessionState::Online {
        match after.session_state {
            SessionState::Opening => "opening",
            SessionState::Backoff => "open_failed_backoff",
            SessionState::WaitingForPort => "waiting_for_port",
            SessionState::Stopping => "stopping",
            SessionState::Disabled => "slot_disabled",
            SessionState::Online => unreachable!("handled above"),
        }
    } else if !live.ready {
        "live_subscription_not_ready"
    } else if live.lagged_events > 0 {
        "subscriber_lagged"
    } else if live.gap_events > 0 || journal_gaps > 0 {
        "stream_gap_detected"
    } else if after.rx_offset == before.rx_offset {
        "target_silent_during_window"
    } else if live.rx_bytes > 0 {
        "healthy"
    } else if journal_rx_events > 0 {
        "live_delivery_fault"
    } else if after.logging == LoggingState::Degraded {
        "journal_degraded"
    } else {
        "ingestion_visibility_fault"
    }
}

async fn observe_live(
    endpoint: &str,
    token: Option<&str>,
    slot_id: &str,
    cursor: Cursor,
    duration: Duration,
) -> Result<LiveObservation> {
    let base = crate::api::normalize_endpoint(endpoint)?;
    let rest = base
        .strip_prefix("http://")
        .expect("normalized endpoint always uses http");
    let mut request = format!("ws://{rest}/api/v1/ws")
        .into_client_request()
        .context("invalid seriald WebSocket URL")?;
    if let Some(token) = token {
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("token contains invalid HTTP header characters")?,
        );
    }
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
        .await
        .context("independent WebSocket connection timed out")??;
    send_control(
        &mut socket,
        &ClientMessage::Hello {
            request_id: Uuid::new_v4(),
            protocol_version: PROTOCOL_VERSION,
            client_name: "serialctl-doctor-stream".into(),
            actor_kind: ActorKind::Human,
        },
    )
    .await?;
    send_control(
        &mut socket,
        &ClientMessage::Attach {
            request_id: Uuid::new_v4(),
            subscriptions: vec![Subscription {
                slot_id: slot_id.to_owned(),
                cursor: Some(cursor),
                tail_events: 0,
            }],
        },
    )
    .await?;

    let deadline = tokio::time::Instant::now() + duration;
    let mut observation = LiveObservation::default();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let message = match tokio::time::timeout(remaining, socket.next()).await {
            Ok(Some(Ok(message))) => message,
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(None) => break,
            Err(_) => break,
        };
        match message {
            Message::Binary(bytes) => match decode_wire_frame(&bytes)? {
                WireFrame::Rx(header, data) if header.slot_id == slot_id => {
                    observation.rx_frames += 1;
                    observation.rx_bytes += data.len() as u64;
                    observation.last_seq = Some(observation.last_seq.unwrap_or(0).max(header.seq));
                }
                WireFrame::Tx(header, data) if header.slot_id == slot_id => {
                    observation.tx_frames += 1;
                    observation.tx_bytes += data.len() as u64;
                    observation.last_seq = Some(observation.last_seq.unwrap_or(0).max(header.seq));
                }
                WireFrame::Control(ServerMessage::Snapshot { slot })
                    if slot.config.id == slot_id =>
                {
                    observation.snapshot_generation = Some(slot.generation);
                }
                WireFrame::Control(ServerMessage::Ready {
                    slot_id: ready_slot,
                    head_seq,
                }) if ready_slot == slot_id => {
                    observation.ready = true;
                    observation.last_seq = Some(observation.last_seq.unwrap_or(0).max(head_seq));
                }
                WireFrame::Control(ServerMessage::Timeline { event, .. })
                    if event.slot_id == slot_id =>
                {
                    observation.timeline_events += 1;
                    observation.last_seq = Some(observation.last_seq.unwrap_or(0).max(event.seq));
                }
                WireFrame::Control(ServerMessage::Gap {
                    slot_id: gap_slot, ..
                }) if gap_slot == slot_id => observation.gap_events += 1,
                WireFrame::Control(ServerMessage::Lagged {
                    slot_id: lagged_slot,
                    ..
                }) if lagged_slot == slot_id => observation.lagged_events += 1,
                WireFrame::Control(ServerMessage::Error { message, .. }) => {
                    bail!("seriald rejected the diagnostic subscription: {message}")
                }
                _ => {}
            },
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            Message::Text(_) => bail!("seriald sent unsupported text on the binary protocol"),
            _ => {}
        }
    }
    let _ = socket.close(None).await;
    Ok(observation)
}

async fn send_control<S>(socket: &mut S, message: &ClientMessage) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    socket
        .send(Message::Binary(encode_client_control(message)?.into()))
        .await?;
    Ok(())
}

fn find_slot<'a>(slots: &'a [SlotSnapshot], id: &str) -> Result<&'a SlotSnapshot> {
    slots
        .iter()
        .find(|slot| slot.config.id == id)
        .with_context(|| format!("unknown Slot {id:?}"))
}

fn same_serial_port(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    match (windows_com_name(left), windows_com_name(right)) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => false,
    }
}

fn windows_com_name(port: &str) -> Option<&str> {
    let port = port.strip_prefix(r"\\.\").unwrap_or(port);
    let bytes = port.as_bytes();
    (bytes.len() > 3
        && bytes[..3].eq_ignore_ascii_case(b"COM")
        && bytes[3..].iter().all(u8::is_ascii_digit))
    .then_some(port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_com_port_matching_is_case_insensitive_on_every_client_platform() {
        assert!(same_serial_port("COM3", "com3"));
        assert!(same_serial_port(r"\\.\COM3", "com3"));
        assert!(!same_serial_port("/dev/ttyUSB0", "/dev/ttyusb0"));
    }
}
