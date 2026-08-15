use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, DataBits, Direction, EchoMode, EventKind, EventQuery,
    FlowControl, LoggingState, PROTOCOL_VERSION, Parity, ResolvedTransportSettings, RunStatus,
    ServerMessage, SessionState, SlotSnapshot, StopBits, Subscription, WireFrame, WritePacing,
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
    display::{
        error_code_label, event_to_lines, pad_display, safe_inline, session_state_label,
        target_activity_label, trigger_status_label,
    },
    i18n::{tr, trf},
};

const FIELD_WIDTH: usize = 18;

fn print_field(key: &'static str, value: impl std::fmt::Display) {
    println!("{} {}", pad_display(tr(key), FIELD_WIDTH), value);
}

fn bool_label(value: bool) -> &'static str {
    if value {
        tr("doctor.value.yes")
    } else {
        tr("doctor.value.no")
    }
}

fn source_label(source: &str) -> String {
    match source {
        "daemon_port_enumeration" => tr("doctor.source.port.enumeration").into(),
        "authoritative_slot_snapshot" => tr("doctor.source.slot.snapshot").into(),
        "authoritative daemon diagnostics" => tr("doctor.source.storage.diagnostics").into(),
        "archive_catalog_fallback" => tr("doctor.source.archive.fallback").into(),
        "authoritative_slot_diagnostics" => tr("doctor.source.slot.diagnostics").into(),
        "status_fallback" => tr("doctor.source.status.fallback").into(),
        other => safe_inline(other),
    }
}

fn assessment_label(assessment: &str) -> String {
    let key = match assessment {
        "slot_disabled" => "doctor.assessment.slot_disabled",
        "port_not_present" => "doctor.assessment.port_not_present",
        "online" => "doctor.assessment.online",
        "opening" => "doctor.assessment.opening",
        "open_failed_backoff" => "doctor.assessment.open_failed_backoff",
        "waiting_for_port" => "doctor.assessment.waiting_for_port",
        "stopping" => "doctor.assessment.stopping",
        "inconclusive_session_changed" => "doctor.assessment.inconclusive_session_changed",
        "live_subscription_not_ready" => "doctor.assessment.live_subscription_not_ready",
        "subscriber_lagged" => "doctor.assessment.subscriber_lagged",
        "stream_gap_detected" => "doctor.assessment.stream_gap_detected",
        "target_silent_during_window" => "doctor.assessment.target_silent_during_window",
        "healthy" => "doctor.assessment.healthy",
        "live_delivery_fault" => "doctor.assessment.live_delivery_fault",
        "journal_degraded" => "doctor.assessment.journal_degraded",
        "ingestion_visibility_fault" => "doctor.assessment.ingestion_visibility_fault",
        other => return trf("doctor.assessment.unknown", &[&safe_inline(other)]),
    };
    tr(key).into()
}

fn logging_label(state: LoggingState) -> &'static str {
    match state {
        LoggingState::Healthy => tr("doctor.logging.healthy"),
        LoggingState::Degraded => tr("doctor.logging.degraded"),
    }
}

fn run_status_label(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Active => tr("ui.run.status.active"),
        RunStatus::Completed => tr("ui.run.status.completed"),
        RunStatus::Aborted => tr("ui.run.status.aborted"),
    }
}

fn transport_label(settings: Option<ResolvedTransportSettings>) -> String {
    let Some(settings) = settings else {
        return tr("doctor.value.unavailable").into();
    };
    let baud = settings.baud_rate.to_string();
    let data_bits = match settings.data_bits {
        DataBits::Five => "5",
        DataBits::Six => "6",
        DataBits::Seven => "7",
        DataBits::Eight => "8",
    };
    let parity = match settings.parity {
        Parity::None => tr("menu.detail.parity.none"),
        Parity::Odd => tr("menu.detail.parity.odd"),
        Parity::Even => tr("menu.detail.parity.even"),
    };
    let stop_bits = match settings.stop_bits {
        StopBits::One => "1",
        StopBits::Two => "2",
    };
    let flow = match settings.flow_control {
        FlowControl::None => tr("menu.detail.flow.none"),
        FlowControl::Software => tr("menu.detail.flow.software"),
        FlowControl::Hardware => tr("menu.detail.flow.hardware"),
    };
    trf(
        "doctor.value.transport",
        &[
            &baud,
            data_bits,
            parity,
            stop_bits,
            flow,
            bool_label(settings.dtr),
            bool_label(settings.rts),
            bool_label(settings.auto_open),
        ],
    )
}

fn pacing_label(pacing: Option<WritePacing>) -> String {
    pacing.map_or_else(
        || tr("doctor.value.unavailable").into(),
        |pacing| {
            trf(
                "doctor.value.pacing",
                &[
                    &pacing.chunk_size.to_string(),
                    &pacing.chunk_delay_ms.to_string(),
                ],
            )
        },
    )
}

fn eol_label(eol: Option<&str>) -> String {
    match eol {
        None => tr("doctor.value.unavailable").into(),
        Some("") => tr("doctor.value.eol.none").into(),
        Some("\r") => "CR (\\r)".into(),
        Some("\n") => "LF (\\n)".into(),
        Some("\r\n") => "CRLF (\\r\\n)".into(),
        Some(value) => trf(
            "doctor.value.eol.custom",
            &[&value
                .chars()
                .flat_map(char::escape_default)
                .collect::<String>()],
        ),
    }
}

fn echo_label(echo: Option<EchoMode>) -> &'static str {
    match echo {
        Some(EchoMode::On) => tr("menu.detail.echo.on"),
        Some(EchoMode::Off) => tr("menu.detail.echo.off"),
        Some(EchoMode::Auto) => tr("menu.detail.echo.auto"),
        None => tr("doctor.value.unavailable"),
    }
}

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
        print_field("doctor.field.slot", safe_inline(&report.slot_id));
        print_field("doctor.field.port", safe_inline(&report.port));
        print_field(
            "doctor.field.discovery",
            trf(
                "doctor.value.discovery",
                &[
                    if report.port_discovered {
                        tr("doctor.value.present")
                    } else {
                        tr("doctor.value.missing")
                    },
                    &source_label(report.discovery_source),
                ],
            ),
        );
        print_field(
            "doctor.field.session",
            trf(
                "doctor.value.session",
                &[
                    session_state_label(report.session_state),
                    &report.generation.to_string(),
                ],
            ),
        );
        print_field(
            "doctor.field.assessment",
            assessment_label(report.assessment),
        );
        if let Some(code) = report.state_code {
            print_field("doctor.field.state_code", error_code_label(code));
        }
        if let Some(reason) = &report.state_reason {
            print_field("doctor.field.reason", safe_inline(reason));
        }
        print_field(
            "doctor.field.counters",
            trf(
                "doctor.value.counters",
                &[
                    &report.rx_offset.to_string(),
                    &report.tx_offset.to_string(),
                    &report.rx_overflow_bytes.to_string(),
                ],
            ),
        );
        if let (Some(subscribers), Some(lag)) =
            (report.subscriber_count, report.subscriber_lag_events)
        {
            print_field(
                "doctor.field.consumers",
                trf(
                    "doctor.value.consumers",
                    &[&subscribers.to_string(), &lag.to_string()],
                ),
            );
        }
        if !report.recent_control_events.is_empty() {
            println!("{}", tr("doctor.heading.port_lifecycle"));
            for event in report.recent_control_events.iter().rev().take(8).rev() {
                for line in event_to_lines(event) {
                    println!("  #{} {}", event.seq, safe_inline(&line.text));
                }
            }
        }
        if let Some(error) = &report.recent_events_error {
            print_field(
                "doctor.field.history",
                trf("doctor.value.history_unavailable", &[&safe_inline(error)]),
            );
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
                print_field(
                    "doctor.field.source",
                    source_label("authoritative daemon diagnostics"),
                );
                print_field(
                    "doctor.field.usage",
                    trf(
                        "doctor.value.usage",
                        &[
                            &report.usage_bytes.to_string(),
                            &report.max_bytes.to_string(),
                        ],
                    ),
                );
                print_field(
                    "doctor.field.retention",
                    trf(
                        "doctor.value.retention",
                        &[
                            &report.retention_target_bytes.to_string(),
                            &report.segment_max_bytes.to_string(),
                        ],
                    ),
                );
                print_field("doctor.field.archives", report.archive_count);
                print_field(
                    "doctor.field.writer",
                    trf(
                        "doctor.value.writer",
                        &[
                            &report.writer_queue_remaining.to_string(),
                            &report.writer_queue_capacity.to_string(),
                        ],
                    ),
                );
                print_field("doctor.field.logging", logging_label(report.logging));
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
        print_field("doctor.field.source", source_label(report.source));
        print_field(
            "doctor.field.usage",
            trf(
                "doctor.value.usage_at_least",
                &[&report.usage_bytes.to_string()],
            ),
        );
        print_field("doctor.field.archives", report.archive_count);
        print_field("doctor.field.quota", tr("doctor.value.quota_unavailable"));
        if !report.degraded_slots.is_empty() {
            print_field(
                "doctor.field.degraded_slots",
                safe_inline(&report.degraded_slots.join(", ")),
            );
        }
        if report.catalog_truncated {
            print_field("doctor.field.catalog", tr("doctor.value.catalog_truncated"));
        }
        print_field("doctor.field.note", tr("doctor.note.upgrade_storage"));
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
        print_field("doctor.field.source", source_label(report.source));
        print_field(
            "doctor.field.slot",
            trf(
                "doctor.value.slot",
                &[
                    &safe_inline(&slot.config.id),
                    &safe_inline(&slot.config.display_name),
                ],
            ),
        );
        print_field(
            "doctor.field.session",
            trf(
                "doctor.value.session_activity",
                &[
                    session_state_label(slot.session_state),
                    target_activity_label(slot.target_activity),
                    &slot.generation.to_string(),
                ],
            ),
        );
        print_field(
            "doctor.field.stream",
            trf(
                "doctor.value.stream",
                &[
                    &slot.head_seq.to_string(),
                    &slot.rx_offset.to_string(),
                    &slot.tx_offset.to_string(),
                    &slot.rx_overflow_bytes.to_string(),
                ],
            ),
        );
        print_field(
            "doctor.field.control",
            slot.control.as_ref().map_or_else(
                || tr("value.none").into(),
                |lease| safe_inline(&lease.owner.label),
            ),
        );
        let run = slot
            .active_run
            .as_ref()
            .map(|run| {
                trf(
                    "doctor.value.run",
                    &[
                        &run.id.to_string(),
                        run_status_label(run.status),
                        &safe_inline(&run.label),
                    ],
                )
            })
            .unwrap_or_else(|| tr("value.none").into());
        print_field("doctor.field.run", run);
        print_field(
            "doctor.field.trigger",
            slot.active_trigger.as_ref().map_or_else(
                || tr("value.none").into(),
                |trigger| {
                    trf(
                        "doctor.value.trigger",
                        &[
                            &trigger.id.to_string(),
                            trigger_status_label(trigger.status),
                        ],
                    )
                },
            ),
        );
        print_field(
            "doctor.field.profiles",
            trf(
                "doctor.value.profiles",
                &[
                    &safe_inline(&slot.config.profile),
                    &slot.config.device_profile.as_ref().map_or_else(
                        || tr("menu.value.generic").into(),
                        |profile| safe_inline(profile),
                    ),
                ],
            ),
        );
        print_field(
            "doctor.field.transport",
            transport_label(slot.effective_transport),
        );
        print_field(
            "doctor.field.pacing",
            pacing_label(slot.effective_write_pacing),
        );
        print_field(
            "doctor.field.eol",
            eol_label(slot.effective_write_eol.as_deref()),
        );
        print_field("doctor.field.echo", echo_label(slot.effective_echo));
        print_field(
            "doctor.field.prompts",
            trf(
                "doctor.value.prompts",
                &[
                    &slot
                        .effective_shell_prompt
                        .as_deref()
                        .map(safe_inline)
                        .unwrap_or_else(|| tr("value.none").into()),
                    &slot
                        .effective_uboot_prompt
                        .as_deref()
                        .map(safe_inline)
                        .unwrap_or_else(|| tr("value.none").into()),
                ],
            ),
        );
        if let Some(count) = report.subscriber_count {
            print_field(
                "doctor.field.consumers",
                trf(
                    "doctor.value.consumers",
                    &[
                        &count.to_string(),
                        &report.subscriber_lag_events.unwrap_or(0).to_string(),
                    ],
                ),
            );
        }
        if let Some(reason) = &slot.state_reason {
            print_field("doctor.field.reason", safe_inline(reason));
        }
        if let Some(code) = slot.state_code {
            print_field("doctor.field.state_code", error_code_label(code));
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
        print_field("doctor.field.slot", safe_inline(&report.slot_id));
        print_field(
            "doctor.field.duration",
            trf(
                "doctor.value.duration",
                &[&report.duration_seconds.to_string()],
            ),
        );
        print_field(
            "doctor.field.offsets",
            trf(
                "doctor.value.offsets",
                &[
                    &report.rx_offset_before.to_string(),
                    &report.rx_offset_after.to_string(),
                    &report.rx_offset_delta.to_string(),
                    &report.head_seq_before.to_string(),
                    &report.head_seq_after.to_string(),
                ],
            ),
        );
        print_field(
            "doctor.field.websocket",
            trf(
                "doctor.value.websocket",
                &[
                    bool_label(report.live.ready),
                    &report.live.rx_frames.to_string(),
                    &report.live.rx_bytes.to_string(),
                    &report.live.tx_frames.to_string(),
                    &report.live.tx_bytes.to_string(),
                ],
            ),
        );
        print_field(
            "doctor.field.journal",
            trf(
                "doctor.value.journal",
                &[
                    &report.journal_rx_events.to_string(),
                    &report.journal_rx_bytes.to_string(),
                    &report.journal_gaps.to_string(),
                    bool_label(report.journal_truncated),
                ],
            ),
        );
        print_field(
            "doctor.field.overflow",
            trf(
                "doctor.value.overflow",
                &[&report.rx_overflow_delta.to_string()],
            ),
        );
        print_field(
            "doctor.field.assessment",
            assessment_label(report.assessment),
        );
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
        .context(tr("doctor.error.ws_url"))?;
    if let Some(token) = token {
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context(tr("doctor.error.token_header"))?,
        );
    }
    let connection = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
        .await
        .context(tr("doctor.error.ws_timeout"))?;
    let (mut socket, _) = connection.context(tr("doctor.error.ws_connect"))?;
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
                    bail!(
                        "{}",
                        trf(
                            "doctor.error.subscription_rejected",
                            &[&safe_inline(&message)]
                        )
                    )
                }
                _ => {}
            },
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(_) => break,
            Message::Text(_) => bail!("{}", tr("doctor.error.ws_text")),
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
        .with_context(|| trf("doctor.error.unknown_slot", &[&safe_inline(id)]))
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
    use crate::i18n::{Lang, lang_test_lock, set_lang};

    #[test]
    fn windows_com_port_matching_is_case_insensitive_on_every_client_platform() {
        assert!(same_serial_port("COM3", "com3"));
        assert!(same_serial_port(r"\\.\COM3", "com3"));
        assert!(!same_serial_port("/dev/ttyUSB0", "/dev/ttyusb0"));
    }

    #[test]
    fn human_assessments_localize_without_changing_json_machine_values() {
        let _guard = lang_test_lock();
        let report = PortReport {
            slot_id: "dut-1".into(),
            port: "COM3".into(),
            port_discovered: true,
            discovery_source: "daemon_port_enumeration",
            enabled: true,
            session_state: SessionState::Online,
            endpoint_present: true,
            state_code: None,
            state_reason: None,
            generation: 3,
            rx_offset: 10,
            tx_offset: 5,
            rx_overflow_bytes: 0,
            subscriber_count: Some(1),
            subscriber_lag_events: Some(0),
            assessment: "online",
            recent_control_events: Vec::new(),
            recent_events_error: None,
        };
        let json = serde_json::to_value(&report).expect("serializes");
        assert_eq!(json["assessment"], "online");
        assert_eq!(json["discovery_source"], "daemon_port_enumeration");

        set_lang(Lang::Zh);
        assert_eq!(
            assessment_label(report.assessment),
            "串口会话在线，未发现异常"
        );
        assert_eq!(source_label(report.discovery_source), "守护进程串口枚举");
        set_lang(Lang::En);
        assert_eq!(
            assessment_label(report.assessment),
            "the serial session is online"
        );
    }

    #[test]
    fn effective_serial_settings_use_natural_runtime_labels() {
        let _guard = lang_test_lock();
        let settings = ResolvedTransportSettings {
            baud_rate: 115_200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        };

        set_lang(Lang::Zh);
        let chinese = transport_label(Some(settings));
        assert!(chinese.contains("波特率 115200"));
        assert!(chinese.contains("8 数据位"));
        assert!(chinese.contains("无校验"));
        assert_eq!(eol_label(Some("\r\n")), "CRLF (\\r\\n)");
        assert_eq!(run_status_label(RunStatus::Active), "执行中");

        set_lang(Lang::En);
        let english = transport_label(Some(settings));
        assert!(english.contains("115200 baud"));
        assert!(english.contains("8 data bits"));
        assert_eq!(run_status_label(RunStatus::Aborted), "aborted");
    }
}
