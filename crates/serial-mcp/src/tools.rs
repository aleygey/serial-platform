use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use serial_protocol::{
    Cursor, DEFAULT_TRIGGER_INTERVAL_MS, DEFAULT_TRIGGER_MAX_FIRES, DEFAULT_TRIGGER_TIMEOUT_MS,
    Direction, EchoMode, EventQuery, MAX_PHYSICAL_WRITE_TIMEOUT_MS, MAX_TRIGGER_ACTION_BYTES,
    MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES, MAX_TRIGGER_INTERVAL_MS,
    MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS, MAX_TRIGGER_TIMEOUT_MS,
    MAX_TRIGGER_TOTAL_BYTES, MIN_TRIGGER_INTERVAL_MS, MIN_TRIGGER_TIMEOUT_MS, SessionState,
    SlotSnapshot, TriggerInfo, TriggerSpec, TriggerStatus, WritePacing,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    api::ApiClient,
    capture::{Capture, CaptureOptions, CommandBoundary, Completion},
    config::CaptureLimits,
    render::{MatchExcerptOptions, RenderOptions, render_events},
    session::SessionHandle,
};

const DEFAULT_TEXT_CHARS: usize = 16_000;
const MAX_TEXT_CHARS: usize = 64_000;
const MAX_WRITE_BYTES: usize = 4096;
const TRIGGER_STATUS_POLL: Duration = Duration::from_millis(50);
const TRIGGER_STATUS_MARGIN: Duration =
    Duration::from_millis(MAX_PHYSICAL_WRITE_TIMEOUT_MS + 5_000);
const TRIGGER_CANCEL_MARGIN: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AgentTools {
    api: ApiClient,
    session: SessionHandle,
    actor_label: String,
    capture_limits: CaptureLimits,
    live_cursors: Arc<Mutex<BTreeMap<String, Cursor>>>,
}

impl AgentTools {
    pub fn new(
        api: ApiClient,
        session: SessionHandle,
        actor_label: String,
        capture_limits: CaptureLimits,
    ) -> Self {
        Self {
            api,
            session,
            actor_label,
            capture_limits,
            live_cursors: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "devices" => self.devices(parse(arguments)?).await,
            "read" => self.read(parse(arguments)?).await,
            "command" => self.command(parse(arguments)?).await,
            "trigger" => self.trigger(parse(arguments)?).await,
            "wait" => self.wait(parse(arguments)?).await,
            "search" => self.search(parse(arguments)?).await,
            "run_start" => self.run_start(parse(arguments)?).await,
            "run_end" => self.run_end(parse(arguments)?).await,
            "release" => self.release(parse(arguments)?).await,
            _ => bail!("unknown serial tool {name:?}"),
        }
    }

    async fn devices(&self, args: DevicesArgs) -> Result<Value> {
        let status = self.api.status().await?;
        let mut slots: Vec<Value> = status
            .slots
            .iter()
            .filter(|slot| args.slot_id.as_ref().is_none_or(|id| &slot.config.id == id))
            .map(slot_summary)
            .collect();
        disambiguate_display_names(&mut slots);
        if let Some(slot_id) = args.slot_id
            && slots.is_empty()
        {
            bail!("unknown Slot {slot_id:?}");
        }
        Ok(json!({
            "daemon_epoch": status.daemon_epoch,
            "slots": slots,
            "selection_note": "Choose a Slot explicitly before writing. A Run isolates only its log/event interval; it does not reset, initialize, or otherwise isolate device state."
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
        validate_operation_filter(args.operation_id, args.direction)?;
        let slot = self.slot(&args.slot_id).await?;
        let (epoch, after_seq) = read_window(args.epoch, args.after_seq, args.tail_events, &slot)?;
        let response = self
            .api
            .events(
                &args.slot_id,
                &EventQuery {
                    epoch: Some(epoch),
                    after_seq,
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: args.direction,
                    kind: None,
                    actor_id: None,
                    run_id: None,
                    operation_id: args.operation_id,
                    contains: None,
                    limit_events: Some(args.limit_events.unwrap_or(1000).clamp(1, 2000)),
                    limit_bytes: Some(args.limit_bytes.unwrap_or(512 * 1024).clamp(1, 1024 * 1024)),
                },
            )
            .await?;
        Ok(render_response(
            &slot,
            epoch,
            response,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                echo: None,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
                match_excerpt: None,
            },
            "tail_or_cursor",
        ))
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        if args.contains.trim().is_empty() {
            bail!("contains must not be empty");
        }
        validate_operation_filter(args.operation_id, args.direction)?;
        let slot = self.slot(&args.slot_id).await?;
        let scope = args.scope.as_deref().unwrap_or("current_run");
        let (epoch, after_seq, run_id) = match scope {
            "current_run" => {
                let after_seq = current_run_after_seq(args.epoch, args.after_seq, &slot)?;
                let run = current_run_id(args.run_id, after_seq, &slot)?;
                (slot.daemon_epoch, after_seq, Some(run))
            }
            "current_cursor" => {
                let cursor = requested_cursor(args.epoch, args.after_seq, &slot)?
                    .context("scope=current_cursor requires epoch and after_seq")?;
                (cursor.epoch, Some(cursor.after_seq), args.run_id)
            }
            "archive" => {
                let epoch = match args.epoch {
                    Some(epoch) => epoch,
                    None => bail!("{}", self.archive_epoch_hint(&args.slot_id, &slot).await),
                };
                (epoch, args.after_seq, args.run_id)
            }
            _ => bail!("scope must be current_run, current_cursor, or archive"),
        };
        let response = self
            .api
            .events(
                &args.slot_id,
                &EventQuery {
                    epoch: Some(epoch),
                    after_seq,
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: args.direction,
                    kind: None,
                    actor_id: None,
                    run_id,
                    operation_id: args.operation_id,
                    contains: Some(args.contains.clone()),
                    limit_events: Some(args.limit_events.unwrap_or(200).clamp(1, 1000)),
                    limit_bytes: Some(args.limit_bytes.unwrap_or(512 * 1024).clamp(1, 1024 * 1024)),
                },
            )
            .await?;
        let no_matches = response.events.is_empty();
        let truncated = response.truncated;
        let mut output = render_response(
            &slot,
            epoch,
            response,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                echo: None,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
                match_excerpt: Some(MatchExcerptOptions {
                    literal: &args.contains,
                    context_lines: args.context_lines.unwrap_or(5).min(50),
                }),
            },
            scope,
        );
        if let Some(run_id) = run_id {
            output["run_id"] = json!(run_id);
        }
        if truncated {
            attach_search_continuation_guidance(&mut output, scope, run_id);
        } else if no_matches {
            self.attach_archive_guidance(&mut output, &args.slot_id, scope)
                .await;
        }
        Ok(output)
    }

    /// Error text for scope=archive without an epoch, carrying a concrete
    /// example value the caller can retry with.
    async fn archive_epoch_hint(&self, slot_id: &str, slot: &SlotSnapshot) -> String {
        let example = self
            .api
            .archives(Some(slot_id))
            .await
            .ok()
            .and_then(|list| list.archives.first().map(|archive| archive.epoch))
            .unwrap_or(slot.daemon_epoch);
        format!("scope=archive requires an explicit epoch, for example epoch={example}")
    }

    /// Point an empty search at wider scopes and the retained archive epochs.
    async fn attach_archive_guidance(&self, output: &mut Value, slot_id: &str, scope: &str) {
        match self.api.archives(Some(slot_id)).await {
            Ok(list) => {
                output["archive_epochs"] = json!({
                    "archives": list.archives.iter().map(|archive| json!({
                        "epoch": archive.epoch,
                        "first_seq": archive.first_seq,
                        "last_seq": archive.last_seq,
                    })).collect::<Vec<_>>(),
                    "truncated": list.truncated,
                });
                output["guidance"] = json!(format!(
                    "No events matched in scope={scope}. Widen the window: search scope=archive with an epoch from archive_epochs, or bracket the operation with run_start/run_end and search the new Run."
                ));
            }
            Err(error) => {
                output["guidance"] = json!(format!(
                    "No events matched in scope={scope}. Listing archives failed ({error}); retry scope=archive with a known epoch or bracket the operation with run_start/run_end and search the new Run."
                ));
            }
        }
    }

    async fn wait(&self, args: WaitArgs) -> Result<Value> {
        let slot = self.slot_online(&args.slot_id).await?;
        let complete_on_quiet = args.contains.is_none();
        let explicit_cursor = requested_cursor(args.epoch, args.after_seq, &slot)?;
        let remembered_cursor = if explicit_cursor.is_none() {
            self.live_cursor(&slot.config.id)
        } else {
            None
        };
        let (cursor, cursor_source) = select_wait_cursor(explicit_cursor, remembered_cursor, &slot);
        let started_epoch = cursor.epoch;
        let started_after_seq = cursor.after_seq;
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            args.slot_id,
            cursor,
            self.capture_limits,
        )
        .await?;
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: millis(args.quiet_ms, 300, 50, 5000),
                patterns: args.contains.into_iter().collect(),
                until_regex: None,
                complete_on_quiet,
                allow_empty_quiet: false,
            })
            .await;
        let with_events = args.include_events || args.include_raw;
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                echo: None,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
                match_excerpt: None,
            },
        );
        let last_seq = result.through_seq.unwrap_or(started_after_seq);
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: started_epoch,
                after_seq: last_seq,
            },
        );
        let mut output = json!({
            "slot_id": slot.config.id, "epoch": started_epoch, "after_seq": last_seq,
            "cursor_source": cursor_source.label(), "started_after_seq": started_after_seq,
            "completion": result.completion.label(), "complete": result.completion.is_complete(),
            "text": rendered.text,
            "capture_truncated": result.truncated, "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed, "gaps": result.gaps
        });
        if with_events {
            output["events"] = json!(rendered.events);
        }
        Ok(output)
    }

    async fn command(&self, args: CommandArgs) -> Result<Value> {
        let slot = self.slot_online(&args.slot_id).await?;
        let expected_run_id = slot
            .active_run
            .as_ref()
            .map(|run| run.id)
            .context("no active Run; call run_start before command")?;
        let operation_id = Uuid::new_v4();
        let capture_after_seq = slot.head_seq;
        let cursor = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: capture_after_seq,
        };
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            args.slot_id.clone(),
            cursor,
            self.capture_limits,
        )
        .await?;
        let bytes = compose_write_bytes(
            &args.command,
            args.eol.as_deref(),
            effective_write_eol(&slot),
        )?;

        let until_regex = compile_until_regex(args.until_regex.as_deref())?;
        let (patterns, completion_mode) = completion_patterns(&args, &slot)?;
        // completion_patterns makes an explicit regex the sole authoritative
        // boundary, so neither a configured prompt nor quiet can pre-empt it.
        let complete_on_quiet = completion_mode == "quiet";
        let pacing = write_pacing(args.chunk_size, args.inter_char_delay_ms, &slot);
        let echo_mode = effective_echo_mode(&slot);
        let expected_echo =
            (matches!(echo_mode, EchoMode::On) && !args.command.is_empty()).then(|| bytes.clone());
        let write = self
            .session
            .write(
                args.slot_id.clone(),
                bytes,
                operation_id,
                expected_run_id,
                pacing,
            )
            .await?;
        let result = capture
            .collect_after_write(
                CaptureOptions {
                    timeout: seconds(args.timeout_seconds, 10, 1, 120),
                    quiet: millis(args.quiet_ms, 300, 50, 5000),
                    patterns,
                    until_regex,
                    complete_on_quiet,
                    allow_empty_quiet: complete_on_quiet,
                },
                CommandBoundary {
                    tx_event_seq: write.event_seq,
                    operation_id,
                    expected_echo,
                },
            )
            .await;
        let with_events = args.include_events || args.include_raw;
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                // collect_after_write has already consumed the complete
                // authoritative echo while arming the completion watcher.
                echo: None,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
                match_excerpt: None,
            },
        );
        let boundary = result
            .command_boundary
            .as_ref()
            .context("command capture lost its authoritative write boundary")?;
        let interfered = boundary.interfered;
        let last_seq = result.through_seq.unwrap_or(write.event_seq);
        let rx_event_count = result
            .events
            .iter()
            .filter(|event| event.direction == Direction::Rx)
            .count();
        let rx_byte_count: usize = result
            .events
            .iter()
            .filter(|event| event.direction == Direction::Rx)
            .map(|event| event.data.len())
            .sum();
        let completion_evidence = completion_evidence(&result.completion);
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: last_seq,
            },
        );
        let mut output = json!({
            "slot_id": slot.config.id, "epoch": slot.daemon_epoch, "after_seq": last_seq,
            "request_id": write.request_id, "run_id": expected_run_id,
            "operation_id": operation_id, "tx_event_seq": write.event_seq,
            "actor": write.actor, "completion_mode": completion_mode,
            "completion": result.completion.label(), "complete": result.completion.is_complete(),
            "completion_evidence": completion_evidence, "execution_status": "unknown",
            "interfered": interfered, "text": rendered.text,
            "capture_window": {
                "attached_after_seq": capture_after_seq,
                "after_seq": write.event_seq, "through_seq": last_seq,
                "rx_event_count": rx_event_count, "rx_byte_count": rx_byte_count
            },
            "prewrite_activity": {
                "observed": boundary.prewrite_activity.event_count > 0,
                "first_seq": boundary.prewrite_activity.first_seq,
                "through_seq": boundary.prewrite_activity.through_seq,
                "event_count": boundary.prewrite_activity.event_count,
                "rx_event_count": boundary.prewrite_activity.rx_event_count,
                "rx_byte_count": boundary.prewrite_activity.rx_byte_count,
                "tx_event_count": boundary.prewrite_activity.tx_event_count
            },
            "boundary": {
                "confidence": boundary.confidence(),
                "tx_event_seq": write.event_seq,
                "tx_audit_observed_in_capture": boundary.tx_audit_observed,
                "echo_required": boundary.echo_required,
                "echo_observed": boundary.echo_observed,
                "discarded_rx_event_count": boundary.discarded_rx_event_count,
                "discarded_rx_byte_count": boundary.discarded_rx_byte_count
            },
            "capture_truncated": result.truncated, "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed, "gaps": result.gaps,
            "guidance": command_guidance(&result.completion, interfered)
        });
        if with_events {
            output["events"] = json!(rendered.events);
        }
        Ok(output)
    }

    async fn trigger(&self, args: TriggerArgs) -> Result<Value> {
        let slot = self.slot_online(&args.slot_id).await?;
        let expected_run_id = slot
            .active_run
            .as_ref()
            .map(|run| run.id)
            .context("no active Run; call run_start before trigger")?;
        if let Some(active) = &slot.active_trigger {
            bail!(
                "Slot already has Trigger {} in status {:?}; wait for it to finish or cancel it \
                 from its owning client",
                active.id,
                active.status
            );
        }

        let spec = trigger_spec(&args, &slot)?;
        let operation_id = Uuid::new_v4();
        let attached_after_seq = slot.head_seq;
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            args.slot_id.clone(),
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: attached_after_seq,
            },
            self.capture_limits,
        )
        .await?;

        // Capture uses an independent subscribed socket. The control session
        // only starts/statuses the daemon Job and stays available for its
        // normal periodic Run lease renewal.
        let capture_timeout = Duration::from_millis(spec.timeout_ms)
            .saturating_add(TRIGGER_STATUS_MARGIN)
            .saturating_add(TRIGGER_CANCEL_MARGIN);
        let (capture_stop, capture_terminal) = oneshot::channel();
        let capture_task =
            tokio::spawn(capture.collect_until_seq(capture_terminal, capture_timeout));

        let started = match self
            .session
            .trigger_start(
                args.slot_id.clone(),
                slot.daemon_epoch,
                slot.generation,
                operation_id,
                expected_run_id,
                spec,
            )
            .await
        {
            Ok(trigger) => trigger,
            Err(error) => {
                let _ = capture_stop.send(None);
                let _ = capture_task.await;
                return Err(error);
            }
        };
        let started_id = started.id;
        let terminal = match self
            .wait_trigger_terminal(&slot, expected_run_id, operation_id, started)
            .await
        {
            Ok(trigger) => trigger,
            Err(error) => {
                let cancel = self
                    .session
                    .trigger_cancel(
                        args.slot_id.clone(),
                        slot.daemon_epoch,
                        slot.generation,
                        started_id,
                        expected_run_id,
                    )
                    .await;
                let _ = capture_stop.send(None);
                let _ = capture_task.await;
                match cancel {
                    Ok(trigger) => bail!(
                        "{error}; best-effort cancellation returned status {:?}, but the \
                         original status/identity failure prevents presenting this as a trusted \
                         normal result. Inspect Trigger {} and the TX timeline before retrying.",
                        trigger.status,
                        trigger.id
                    ),
                    Err(cancel_error) => bail!(
                        "{error}; best-effort cancellation also failed ({cancel_error}). Trigger \
                         {started_id} has no authoritative terminal result at this client; its \
                         outcome is uncertain. Inspect active_trigger/TX timeline before retrying."
                    ),
                }
            }
        };
        let Some(terminal_end_seq) = terminal.end_seq else {
            let _ = capture_stop.send(None);
            let _ = capture_task.await;
            bail!(
                "seriald returned terminal Trigger {} without end_seq; its evidence boundary is \
                 not authoritative",
                terminal.id
            );
        };
        if terminal_end_seq < terminal.start_seq {
            let _ = capture_stop.send(None);
            let _ = capture_task.await;
            bail!(
                "seriald returned Trigger {} with invalid evidence range {}..={terminal_end_seq}",
                terminal.id,
                terminal.start_seq
            );
        }
        let _ = capture_stop.send(Some(terminal_end_seq));
        let capture = capture_task
            .await
            .context("trigger capture task stopped unexpectedly")?;
        let run_ownership_retained = self
            .session
            .run_ownership_retained(slot.config.id.clone(), expected_run_id)
            .await
            .unwrap_or(false);

        let pretrigger_events: Vec<_> = capture
            .events
            .iter()
            .filter(|event| event.seq < terminal.start_seq)
            .collect();
        let trigger_events: Vec<_> = capture
            .events
            .iter()
            .filter(|event| {
                trigger_evidence_contains(event.seq, terminal.start_seq, terminal_end_seq)
            })
            .cloned()
            .collect();
        let pretrigger_rx_bytes: usize = pretrigger_events
            .iter()
            .filter(|event| event.direction == Direction::Rx)
            .map(|event| event.data.len())
            .sum();
        let rendered = render_events(
            &trigger_events,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                echo: None,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
                match_excerpt: None,
            },
        );
        let observed_through_seq = capture.through_seq;
        let last_seq = terminal_end_seq;
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: last_seq,
            },
        );
        let matched_pattern = terminal
            .matched_pattern
            .as_deref()
            .map(|pattern| String::from_utf8_lossy(pattern).into_owned());
        let with_events = args.include_events || args.include_raw;
        let outcome = trigger_status_label(terminal.status);
        let capture_complete = !capture.truncated
            && capture.gaps.is_empty()
            && matches!(&capture.completion, Completion::Signal(_))
            && observed_through_seq.is_some_and(|through| through >= terminal_end_seq);
        let guidance = if run_ownership_retained {
            trigger_guidance(terminal.status).to_owned()
        } else {
            format!(
                "{} The MCP control connection no longer retains this Run/lease. Check devices \
                 until the old Run is no longer active, then call run_start and initialize device \
                 state explicitly before any further write.",
                trigger_guidance(terminal.status)
            )
        };
        let mut output = json!({
            "slot_id": slot.config.id,
            "epoch": slot.daemon_epoch,
            "after_seq": last_seq,
            "trigger_id": terminal.id,
            "operation_id": operation_id,
            "run_id": expected_run_id,
            "mcp_run_ownership_retained": run_ownership_retained,
            "outcome": outcome,
            "matched": terminal.status.is_matched(),
            "matched_pattern": matched_pattern,
            "fires_confirmed": terminal.fires_confirmed,
            "tx_bytes_confirmed": terminal.tx_bytes_confirmed,
            "start_seq": terminal.start_seq,
            "end_seq": terminal_end_seq,
            "last_write_seq": terminal.last_write_seq,
            "capture_window": {
                "attached_after_seq": attached_after_seq,
                "after_seq": terminal.start_seq.saturating_sub(1),
                "through_seq": terminal_end_seq,
                "observed_through_seq": observed_through_seq,
                "completion": capture.completion.label()
            },
            "pretrigger_activity": {
                "observed": !pretrigger_events.is_empty(),
                "event_count": pretrigger_events.len(),
                "rx_byte_count": pretrigger_rx_bytes,
                "first_seq": pretrigger_events.first().map(|event| event.seq),
                "through_seq": pretrigger_events.last().map(|event| event.seq)
            },
            "text": rendered.text,
            "capture_complete": capture_complete,
            "capture_truncated": capture.truncated,
            "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed,
            "gaps": capture.gaps,
            "guidance": guidance
        });
        if with_events {
            output["events"] = json!(rendered.events);
        }
        Ok(output)
    }

    async fn wait_trigger_terminal(
        &self,
        slot: &SlotSnapshot,
        expected_run_id: Uuid,
        operation_id: Uuid,
        mut trigger: TriggerInfo,
    ) -> Result<TriggerInfo> {
        let trigger_id = trigger.id;
        let deadline = tokio::time::Instant::now()
            + Duration::from_millis(trigger.spec.timeout_ms)
            + TRIGGER_STATUS_MARGIN;
        loop {
            validate_trigger_identity(&trigger, slot, expected_run_id, operation_id, trigger_id)?;
            if trigger.status.is_terminal() {
                return Ok(trigger);
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(TRIGGER_STATUS_POLL).await;
            trigger = self
                .session
                .trigger_status(
                    slot.config.id.clone(),
                    slot.daemon_epoch,
                    slot.generation,
                    trigger_id,
                )
                .await?;
        }

        // seriald should terminate at its own timeout. If it has not, request
        // cancellation rather than allowing a Job to outlive this tool call.
        trigger = self
            .session
            .trigger_cancel(
                slot.config.id.clone(),
                slot.daemon_epoch,
                slot.generation,
                trigger_id,
                expected_run_id,
            )
            .await
            .context("Trigger exceeded its status deadline and cancellation failed")?;
        let cancel_deadline = tokio::time::Instant::now() + TRIGGER_CANCEL_MARGIN;
        loop {
            validate_trigger_identity(&trigger, slot, expected_run_id, operation_id, trigger_id)?;
            if trigger.status.is_terminal() {
                return Ok(trigger);
            }
            if tokio::time::Instant::now() >= cancel_deadline {
                bail!(
                    "Trigger {trigger_id} remained {:?} after cancellation; no authoritative \
                     terminal outcome was received",
                    trigger.status
                );
            }
            tokio::time::sleep(TRIGGER_STATUS_POLL).await;
            trigger = self
                .session
                .trigger_status(
                    slot.config.id.clone(),
                    slot.daemon_epoch,
                    slot.generation,
                    trigger_id,
                )
                .await?;
        }
    }

    async fn run_start(&self, args: RunStartArgs) -> Result<Value> {
        let slot = self.slot_online(&args.slot_id).await?;
        if let Some(run) = slot.active_run {
            bail!("Slot already has active Run {} ({})", run.id, run.label);
        }
        let run = self
            .session
            .start_run(
                args.slot_id,
                args.label,
                args.metadata,
                seconds(args.control_wait_seconds, 15, 0, 15),
            )
            .await?;
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: run.start_seq,
            },
        );
        Ok(
            json!({"run": run, "guidance": "Initialize device state explicitly after starting the Run."}),
        )
    }

    async fn run_end(&self, args: RunEndArgs) -> Result<Value> {
        let slot = self.slot(&args.slot_id).await?;
        let run_id = args
            .run_id
            .or_else(|| slot.active_run.as_ref().map(|run| run.id))
            .context("no active Run; pass run_id explicitly")?;
        Ok(json!({
            "run": self.session.end_run(args.slot_id, run_id).await?,
            "control_renewal_stopped": true,
            "control_release": "best_effort",
            "serial_port_closed": false
        }))
    }

    async fn release(&self, args: ReleaseArgs) -> Result<Value> {
        let had_lease = self
            .session
            .release(args.slot_id.clone(), args.abort_run)
            .await?;
        Ok(json!({
            "slot_id": args.slot_id,
            "released": had_lease,
            "already_released": !had_lease,
            "serial_port_closed": false
        }))
    }

    async fn slot(&self, slot_id: &str) -> Result<SlotSnapshot> {
        self.api
            .status()
            .await?
            .slots
            .into_iter()
            .find(|slot| slot.config.id == slot_id)
            .with_context(|| format!("unknown Slot {slot_id:?}"))
    }

    async fn slot_online(&self, slot_id: &str) -> Result<SlotSnapshot> {
        let slot = self.slot(slot_id).await?;
        if slot.session_state != SessionState::Online {
            bail!(
                "Slot {slot_id:?} is {:?}: {}",
                slot.session_state,
                slot.state_reason.as_deref().unwrap_or("no reason reported")
            );
        }
        Ok(slot)
    }

    fn live_cursor(&self, slot_id: &str) -> Option<Cursor> {
        self.live_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(slot_id)
            .cloned()
    }

    fn remember_live_cursor(&self, slot_id: &str, cursor: Cursor) {
        remember_live_cursor(
            &mut self
                .live_cursors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            slot_id,
            cursor,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorSource {
    Explicit,
    SessionLiveCursor,
    CurrentHead,
}

impl CursorSource {
    fn label(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::SessionLiveCursor => "session_live_cursor",
            Self::CurrentHead => "current_head",
        }
    }
}

fn select_wait_cursor(
    explicit: Option<Cursor>,
    remembered: Option<Cursor>,
    slot: &SlotSnapshot,
) -> (Cursor, CursorSource) {
    if let Some(cursor) = explicit {
        return (cursor, CursorSource::Explicit);
    }
    if let Some(cursor) = remembered
        && cursor.epoch == slot.daemon_epoch
        && cursor.after_seq <= slot.head_seq
    {
        return (cursor, CursorSource::SessionLiveCursor);
    }
    (
        Cursor {
            epoch: slot.daemon_epoch,
            after_seq: slot.head_seq,
        },
        CursorSource::CurrentHead,
    )
}

fn remember_live_cursor(cursors: &mut BTreeMap<String, Cursor>, slot_id: &str, cursor: Cursor) {
    match cursors.get_mut(slot_id) {
        Some(current) if current.epoch == cursor.epoch => {
            current.after_seq = current.after_seq.max(cursor.after_seq);
        }
        Some(current) => *current = cursor,
        None => {
            cursors.insert(slot_id.to_string(), cursor);
        }
    }
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    serde_json::from_value(value).context("invalid tool arguments")
}

fn requested_cursor(
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    slot: &SlotSnapshot,
) -> Result<Option<Cursor>> {
    match (epoch, after_seq) {
        (None, None) => Ok(None),
        (Some(epoch), Some(after_seq)) => {
            if epoch != slot.daemon_epoch {
                bail!("cursor epoch changed; refresh devices/read before continuing");
            }
            if after_seq > slot.head_seq {
                bail!("cursor is ahead of Slot head_seq {}", slot.head_seq);
            }
            Ok(Some(Cursor { epoch, after_seq }))
        }
        _ => bail!("epoch and after_seq must be supplied together"),
    }
}

fn current_run_after_seq(
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    slot: &SlotSnapshot,
) -> Result<Option<u64>> {
    match (epoch, after_seq) {
        (None, None) => Ok(None),
        (Some(epoch), Some(after_seq)) => requested_cursor(Some(epoch), Some(after_seq), slot)
            .map(|cursor| cursor.map(|cursor| cursor.after_seq)),
        _ => bail!(
            "scope=current_run continuation requires epoch and after_seq together; use the \
             values returned by the previous truncated search page"
        ),
    }
}

fn current_run_id(
    requested: Option<Uuid>,
    continuation_after_seq: Option<u64>,
    slot: &SlotSnapshot,
) -> Result<Uuid> {
    match requested {
        Some(run_id) => Ok(run_id),
        None if continuation_after_seq.is_some() => bail!(
            "scope=current_run continuation requires the run_id returned by the previous page; \
             refusing to resolve a possibly different active Run"
        ),
        None => {
            slot.active_run.as_ref().map(|run| run.id).context(
                "no active Run; pass run_id or use scope=current_cursor/archive explicitly",
            )
        }
    }
}

/// Resolve the epoch and window for a read. An explicit epoch is honored
/// as-is so archived history stays reachable; only a cursor on the current
/// daemon epoch is validated against the live head.
fn read_window(
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    tail_events: Option<usize>,
    slot: &SlotSnapshot,
) -> Result<(Uuid, Option<u64>)> {
    match (epoch, after_seq) {
        (None, None) => {
            let tail = tail_events.unwrap_or(200).clamp(1, 2000) as u64;
            Ok((slot.daemon_epoch, Some(slot.head_seq.saturating_sub(tail))))
        }
        (Some(epoch), Some(after_seq)) => {
            if epoch == slot.daemon_epoch && after_seq > slot.head_seq {
                bail!("cursor is ahead of Slot head_seq {}", slot.head_seq);
            }
            Ok((epoch, Some(after_seq)))
        }
        (Some(epoch), None) => Ok((epoch, None)),
        (None, Some(_)) => bail!("after_seq requires an explicit epoch"),
    }
}

fn render_response(
    slot: &SlotSnapshot,
    query_epoch: Uuid,
    response: serial_protocol::EventQueryResponse,
    options: RenderOptions,
    scope: &str,
) -> Value {
    let with_events = options.include_events || options.include_raw;
    let rendered = render_events(&response.events, options);
    let after_seq = response
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.after_seq)
        .or_else(|| response.events.last().map(|event| event.seq))
        .unwrap_or(slot.head_seq);
    let mut output = json!({
        "slot_id": slot.config.id, "scope": scope, "epoch": response.next_cursor.as_ref().map(|c| c.epoch).unwrap_or(query_epoch),
        "after_seq": after_seq, "head_seq": slot.head_seq, "text": rendered.text,
        "truncated": response.truncated,
        "text_truncated": rendered.text_truncated, "repeated_lines_collapsed": rendered.repeated_lines_collapsed,
        "first_available_seq": response.first_available_seq, "gaps": response.gaps
    });
    if let Some(excerpt) = rendered.match_excerpt {
        output["match_excerpt"] = json!({
            "matched_lines": excerpt.matched_lines,
            "omitted_lines": excerpt.omitted_lines,
            "context_lines": excerpt.context_lines,
        });
    }
    if with_events {
        output["events"] = json!(rendered.events);
    }
    output
}

fn validate_operation_filter(
    operation_id: Option<Uuid>,
    direction: Option<Direction>,
) -> Result<()> {
    if operation_id.is_some() && direction != Some(Direction::Tx) {
        bail!(
            "operation_id identifies confirmed TX audit events only; device RX events have \
             operation_id=null because a shared serial byte stream has no reliable causal \
             assignment. Add direction=tx to inspect the exact write, or omit operation_id \
             and use command's capture_window, a Run, or an epoch/after_seq cursor for output."
        );
    }
    Ok(())
}

fn completion_evidence(completion: &Completion) -> Value {
    match completion {
        Completion::Pattern(pattern) => {
            json!({"kind": "literal", "matched": pattern, "boundary_observed": true})
        }
        Completion::Regex(pattern) => {
            json!({"kind": "regex", "matched": pattern, "boundary_observed": true})
        }
        Completion::Quiet => {
            json!({"kind": "quiet", "boundary_observed": true})
        }
        Completion::Signal(signal) => {
            json!({"kind": "signal", "signal": signal, "boundary_observed": true})
        }
        Completion::Timeout => {
            json!({"kind": "timeout", "boundary_observed": false})
        }
        Completion::Disconnected(reason) => {
            json!({"kind": "disconnected", "reason": reason, "boundary_observed": false})
        }
    }
}

fn command_guidance(completion: &Completion, interfered: bool) -> &'static str {
    if interfered {
        return "Another actor wrote during this capture window; do not treat the RX as isolated. \
                RX is window/Run scoped and never tagged with operation_id.";
    }
    match completion {
        Completion::Pattern(_) | Completion::Regex(_) => {
            "The requested completion boundary was observed, but serial-mcp did not inspect a \
             shell exit code; execution_status remains unknown. RX belongs to this capture \
             window/Run, not to operation_id."
        }
        Completion::Quiet => {
            "A quiet interval ended capture; this is not proof that the command finished or \
             succeeded. RX belongs to this capture window/Run, not to operation_id."
        }
        Completion::Timeout => {
            "The requested boundary was not observed before timeout. Continue from after_seq or \
             inspect the current Run; do not infer command success."
        }
        Completion::Disconnected(_) => {
            "Capture disconnected before a completion boundary. Inspect the TX timeline and \
             continue from the returned cursor before deciding whether to retry."
        }
        Completion::Signal(_) => {
            "An external operation boundary ended capture. This completion mode is not used by \
             ordinary serial_command calls."
        }
    }
}

fn attach_search_continuation_guidance(output: &mut Value, scope: &str, run_id: Option<Uuid>) {
    let mut continuation = json!({
        "scope": scope,
        "epoch": output["epoch"].clone(),
        "after_seq": output["after_seq"].clone(),
    });
    if let Some(run_id) = run_id {
        continuation["run_id"] = json!(run_id);
    }
    output["continuation"] = continuation;
    output["guidance"] = json!(if scope == "current_run" {
        "Search results are incomplete. Repeat search with the same contains/direction/operation \
         filters and the returned continuation fields before concluding that a match is absent. \
         Keep scope=current_run and run_id unchanged to avoid searching a different test cycle."
    } else {
        "Search results are incomplete. Repeat search with the same filters and the returned \
         continuation fields before concluding that a match is absent."
    });
}

/// Per-call write pacing override. Either side falls back to the Slot's
/// configured pacing (seriald itself defaults to one byte per chunk with a
/// 1 ms inter-chunk delay), and both absent means no override at all.
fn write_pacing(
    chunk_size: Option<u32>,
    inter_char_delay_ms: Option<u64>,
    slot: &SlotSnapshot,
) -> Option<WritePacing> {
    let chunk_size = chunk_size.filter(|size| *size > 0);
    if chunk_size.is_none() && inter_char_delay_ms.is_none() {
        return None;
    }
    Some(WritePacing {
        chunk_size: chunk_size.unwrap_or(slot.config.settings.write_chunk_size),
        chunk_delay_ms: inter_char_delay_ms.unwrap_or(slot.config.settings.write_chunk_delay_ms),
    })
}

/// Assemble the bytes for one write. An empty command is valid as long as the
/// effective EOL contributes bytes, which sends a bare Enter; only a fully
/// empty payload is rejected.
fn compose_write_bytes(
    command: &str,
    eol_override: Option<&str>,
    default_eol: &str,
) -> Result<Vec<u8>> {
    let eol = eol_override.unwrap_or(default_eol);
    if command.is_empty() && eol.is_empty() {
        bail!("command and EOL are both empty; nothing would be sent");
    }
    let mut bytes = command.as_bytes().to_vec();
    bytes.extend_from_slice(eol.as_bytes());
    if bytes.len() > MAX_WRITE_BYTES {
        bail!("command plus EOL exceeds {MAX_WRITE_BYTES} bytes");
    }
    Ok(bytes)
}

fn effective_write_eol(slot: &SlotSnapshot) -> &str {
    slot.effective_write_eol
        .as_deref()
        .unwrap_or(&slot.config.settings.write_eol)
}

fn effective_echo_mode(slot: &SlotSnapshot) -> EchoMode {
    slot.effective_echo.unwrap_or(slot.config.settings.echo)
}

fn effective_prompts(slot: &SlotSnapshot) -> (Option<String>, Option<String>) {
    // Current daemons always publish effective EOL/echo, even when both
    // effective prompts are authoritatively unset. Only snapshots with no
    // effective bundle at all come from an older daemon and may fall back to
    // legacy raw Slot prompt fields.
    if slot.effective_shell_prompt.is_some()
        || slot.effective_uboot_prompt.is_some()
        || slot.effective_write_eol.is_some()
        || slot.effective_echo.is_some()
    {
        (
            slot.effective_shell_prompt.clone(),
            slot.effective_uboot_prompt.clone(),
        )
    } else {
        (
            slot.config.settings.shell_prompt.clone(),
            slot.config.settings.uboot_prompt.clone(),
        )
    }
}

fn completion_patterns(args: &CommandArgs, slot: &SlotSnapshot) -> Result<(Vec<String>, String)> {
    if args.until_regex.is_some() {
        match args.completion.as_deref() {
            None | Some("auto") => {}
            Some(mode) => bail!(
                "until_regex is the sole completion boundary; omit completion or use \
                 completion=auto, not completion={mode}"
            ),
        }
        if args.until.is_some() {
            bail!("until cannot be combined with until_regex; choose one completion boundary");
        }
        return Ok((Vec::new(), "regex".into()));
    }

    let mode = args.completion.as_deref().unwrap_or("auto");
    let (shell_prompt, uboot_prompt) = effective_prompts(slot);
    let patterns = match mode {
        "auto" => [shell_prompt.clone(), uboot_prompt.clone()]
            .into_iter()
            .flatten()
            .collect(),
        "prompt" => {
            let patterns: Vec<String> = args
                .until
                .clone()
                .into_iter()
                .chain([shell_prompt, uboot_prompt].into_iter().flatten())
                .collect();
            if patterns.is_empty() {
                bail!("completion=prompt needs until or a configured Shell/U-Boot prompt");
            }
            patterns
        }
        "contains" => vec![
            args.until
                .clone()
                .context("completion=contains requires until")?,
        ],
        "quiet" => Vec::new(),
        _ => bail!("completion must be auto, prompt, contains, or quiet"),
    };
    let effective = if mode == "auto" && patterns.is_empty() {
        "quiet"
    } else {
        mode
    };
    Ok((patterns, effective.into()))
}

fn compile_until_regex(value: Option<&str>) -> Result<Option<regex::Regex>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        bail!("until_regex must not be empty");
    }
    regex::Regex::new(value)
        .map(Some)
        .context("until_regex is not a valid regex")
}

fn trigger_spec(args: &TriggerArgs, slot: &SlotSnapshot) -> Result<TriggerSpec> {
    let initial_write = args
        .initial_write
        .as_ref()
        .map(|write| trigger_write_bytes(write, "initial_write", MAX_TRIGGER_INITIAL_WRITE_BYTES))
        .transpose()?;
    let action = trigger_write_bytes(&args.action, "action", MAX_TRIGGER_ACTION_BYTES)?;
    let start_contains = args
        .start_contains
        .as_ref()
        .map(|pattern| trigger_pattern_bytes(pattern, "start_contains"))
        .transpose()?;
    if args.stop_contains.len() > MAX_TRIGGER_PATTERNS {
        bail!(
            "stop_contains has {} literals; at most {MAX_TRIGGER_PATTERNS} are allowed",
            args.stop_contains.len()
        );
    }
    let stop_contains = args
        .stop_contains
        .iter()
        .enumerate()
        .map(|(index, pattern)| trigger_pattern_bytes(pattern, &format!("stop_contains[{index}]")))
        .collect::<Result<Vec<_>>>()?;

    let interval_ms = args.interval_ms.unwrap_or(DEFAULT_TRIGGER_INTERVAL_MS);
    if !(MIN_TRIGGER_INTERVAL_MS..=MAX_TRIGGER_INTERVAL_MS).contains(&interval_ms) {
        bail!(
            "interval_ms must be between {MIN_TRIGGER_INTERVAL_MS} and \
             {MAX_TRIGGER_INTERVAL_MS}"
        );
    }
    let timeout_ms = args.timeout_ms.unwrap_or(DEFAULT_TRIGGER_TIMEOUT_MS);
    if !(MIN_TRIGGER_TIMEOUT_MS..=MAX_TRIGGER_TIMEOUT_MS).contains(&timeout_ms) {
        bail!("timeout_ms must be between {MIN_TRIGGER_TIMEOUT_MS} and {MAX_TRIGGER_TIMEOUT_MS}");
    }
    let max_fires = args.max_fires.unwrap_or(DEFAULT_TRIGGER_MAX_FIRES);
    if !(1..=MAX_TRIGGER_FIRES).contains(&max_fires) {
        bail!("max_fires must be between 1 and {MAX_TRIGGER_FIRES}");
    }
    let planned_bytes = initial_write
        .as_ref()
        .map_or(0, Vec::len)
        .saturating_add(action.len().saturating_mul(max_fires as usize));
    if planned_bytes > MAX_TRIGGER_TOTAL_BYTES {
        bail!(
            "initial_write plus action * max_fires plans {planned_bytes} bytes; the Trigger limit \
             is {MAX_TRIGGER_TOTAL_BYTES} bytes"
        );
    }

    Ok(TriggerSpec {
        initial_write,
        start_contains,
        action,
        interval_ms,
        stop_contains,
        timeout_ms,
        max_fires,
        pacing: write_pacing(args.chunk_size, args.inter_char_delay_ms, slot),
    })
}

fn trigger_write_bytes(write: &TriggerWriteArgs, field: &str, max_bytes: usize) -> Result<Vec<u8>> {
    let mut bytes = write.text.as_bytes().to_vec();
    bytes.extend_from_slice(write.eol.as_deref().unwrap_or("").as_bytes());
    if bytes.is_empty() {
        bail!("{field} text and EOL are both empty; omit initial_write or provide bytes");
    }
    if bytes.len() > max_bytes {
        bail!("{field} text plus EOL exceeds {max_bytes} UTF-8 bytes");
    }
    Ok(bytes)
}

fn trigger_pattern_bytes(pattern: &str, field: &str) -> Result<Vec<u8>> {
    if pattern.is_empty() {
        bail!("{field} must not be empty");
    }
    let bytes = pattern.as_bytes().to_vec();
    if bytes.len() > MAX_TRIGGER_PATTERN_BYTES {
        bail!("{field} exceeds {MAX_TRIGGER_PATTERN_BYTES} UTF-8 bytes");
    }
    Ok(bytes)
}

fn trigger_evidence_contains(seq: u64, start_seq: u64, end_seq: u64) -> bool {
    (start_seq..=end_seq).contains(&seq)
}

fn validate_trigger_identity(
    trigger: &TriggerInfo,
    slot: &SlotSnapshot,
    expected_run_id: Uuid,
    operation_id: Uuid,
    trigger_id: Uuid,
) -> Result<()> {
    if trigger.id != trigger_id {
        bail!(
            "seriald returned Trigger {} while waiting for {trigger_id}",
            trigger.id
        );
    }
    if trigger.daemon_epoch != slot.daemon_epoch {
        bail!("Trigger daemon epoch changed; its terminal outcome cannot be trusted");
    }
    if trigger.generation != slot.generation {
        bail!("Trigger serial generation changed; its terminal outcome cannot be trusted");
    }
    if trigger.expected_run_id != Some(expected_run_id) {
        bail!("Trigger is no longer bound to the adapter-owned Run {expected_run_id}");
    }
    if trigger.operation_id != Some(operation_id) {
        bail!("Trigger operation identity changed; refusing to merge unrelated evidence");
    }
    Ok(())
}

fn trigger_status_label(status: TriggerStatus) -> &'static str {
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

fn trigger_guidance(status: TriggerStatus) -> &'static str {
    match status {
        TriggerStatus::Matched => {
            "A caller-supplied stop literal was observed in live RX. This confirms only the \
             Trigger boundary, not that a later flashing or debug workflow succeeded."
        }
        TriggerStatus::TimedOut | TriggerStatus::MaxFiresReached => {
            "No stop literal was observed before the hard Trigger bound. Inspect this bounded \
             capture/current Run before deciding whether a different action is safe."
        }
        TriggerStatus::Cancelled => {
            "The Trigger was cancelled and reached an authoritative terminal state; no future \
             fires will be scheduled."
        }
        TriggerStatus::ControlLost
        | TriggerStatus::RunLost
        | TriggerStatus::GenerationChanged
        | TriggerStatus::PortClosed => {
            "The serial ownership/session boundary changed. Start a new Run and initialize \
             device state explicitly before any further write."
        }
        TriggerStatus::WriteFailed | TriggerStatus::RxGap => {
            "Trigger evidence is uncertain because a physical write failed or live RX had a \
             gap. Inspect the TX timeline and current device state before retrying."
        }
        TriggerStatus::Armed
        | TriggerStatus::WaitingForStart
        | TriggerStatus::Running
        | TriggerStatus::Stopping => {
            "The Trigger has not reached a terminal outcome; this response should not normally \
             be returned by serial_trigger."
        }
    }
}

fn seconds(value: Option<u64>, default: u64, min: u64, max: u64) -> Duration {
    Duration::from_secs(value.unwrap_or(default).clamp(min, max))
}
fn millis(value: Option<u64>, default: u64, min: u64, max: u64) -> Duration {
    Duration::from_millis(value.unwrap_or(default).clamp(min, max))
}
fn max_chars(value: Option<usize>) -> usize {
    value
        .unwrap_or(DEFAULT_TEXT_CHARS)
        .clamp(256, MAX_TEXT_CHARS)
}

fn slot_summary(slot: &SlotSnapshot) -> Value {
    json!({
        "slot_id": slot.config.id, "display_name": slot.config.display_name, "port": slot.config.port,
        "profile": slot.config.profile, "device_profile": slot.config.device_profile,
        "enabled": slot.config.enabled, "session_state": slot.session_state,
        "state_reason": slot.state_reason, "target_activity": slot.target_activity, "baud_rate": slot.config.settings.baud_rate,
        "write_eol": slot.config.settings.write_eol, "echo": slot.config.settings.echo,
        "shell_prompt": slot.config.settings.shell_prompt, "uboot_prompt": slot.config.settings.uboot_prompt,
        "effective_shell_prompt": slot.effective_shell_prompt, "effective_uboot_prompt": slot.effective_uboot_prompt,
        "effective_write_eol": slot.effective_write_eol, "effective_echo": slot.effective_echo,
        "epoch": slot.daemon_epoch, "head_seq": slot.head_seq, "generation": slot.generation,
        "control": slot.control, "active_run": slot.active_run, "active_trigger": slot.active_trigger,
        "logging": slot.logging
    })
}

/// Keep display names usable as identifiers: an empty name falls back to the
/// port, and names shared by several Slots on one daemon gain a port suffix.
fn disambiguate_display_names(slots: &mut [Value]) {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for slot in slots.iter() {
        let name = slot["display_name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        *counts.entry(name).or_default() += 1;
    }
    for slot in slots.iter_mut() {
        let name = slot["display_name"].as_str().unwrap_or_default();
        let port = slot["port"].as_str().unwrap_or_default();
        if name.is_empty() {
            slot["display_name"] = json!(format!("({port})"));
        } else if counts.get(name).copied().unwrap_or(0) > 1 {
            slot["display_name"] = json!(format!("{name} ({port})"));
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DevicesArgs {
    slot_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    slot_id: String,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    tail_events: Option<usize>,
    direction: Option<Direction>,
    operation_id: Option<Uuid>,
    limit_events: Option<usize>,
    limit_bytes: Option<usize>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    include_events: bool,
    #[serde(default = "default_true")]
    collapse_repeats: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    slot_id: String,
    contains: String,
    scope: Option<String>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    run_id: Option<Uuid>,
    direction: Option<Direction>,
    operation_id: Option<Uuid>,
    context_lines: Option<usize>,
    limit_events: Option<usize>,
    limit_bytes: Option<usize>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    include_events: bool,
    #[serde(default = "default_true")]
    collapse_repeats: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArgs {
    slot_id: String,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    contains: Option<String>,
    timeout_seconds: Option<u64>,
    quiet_ms: Option<u64>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    include_events: bool,
    #[serde(default = "default_true")]
    collapse_repeats: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandArgs {
    slot_id: String,
    command: String,
    eol: Option<String>,
    completion: Option<String>,
    until: Option<String>,
    until_regex: Option<String>,
    inter_char_delay_ms: Option<u64>,
    chunk_size: Option<u32>,
    timeout_seconds: Option<u64>,
    quiet_ms: Option<u64>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    include_events: bool,
    #[serde(default = "default_true")]
    collapse_repeats: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerWriteArgs {
    text: String,
    eol: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TriggerArgs {
    slot_id: String,
    initial_write: Option<TriggerWriteArgs>,
    start_contains: Option<String>,
    action: TriggerWriteArgs,
    #[serde(default)]
    stop_contains: Vec<String>,
    interval_ms: Option<u64>,
    timeout_ms: Option<u64>,
    max_fires: Option<u32>,
    inter_char_delay_ms: Option<u64>,
    chunk_size: Option<u32>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
    #[serde(default)]
    include_events: bool,
    #[serde(default = "default_true")]
    collapse_repeats: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStartArgs {
    slot_id: String,
    label: String,
    #[serde(default)]
    metadata: BTreeMap<String, Value>,
    control_wait_seconds: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunEndArgs {
    slot_id: String,
    run_id: Option<Uuid>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseArgs {
    slot_id: String,
    #[serde(default)]
    abort_run: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_command_is_allowed_when_eol_contributes_bytes() {
        assert_eq!(compose_write_bytes("", None, "\r").unwrap(), b"\r".to_vec());
        assert_eq!(
            compose_write_bytes("", Some("\r\n"), "\r").unwrap(),
            b"\r\n".to_vec()
        );
        assert_eq!(
            compose_write_bytes("help", Some(""), "\r").unwrap(),
            b"help".to_vec()
        );
    }

    #[test]
    fn fully_empty_write_is_rejected() {
        let error = compose_write_bytes("", Some(""), "\r").unwrap_err();
        assert!(error.to_string().contains("nothing would be sent"));
        let error = compose_write_bytes("", None, "").unwrap_err();
        assert!(error.to_string().contains("nothing would be sent"));
    }

    #[test]
    fn write_size_limit_counts_command_plus_eol() {
        let command = "x".repeat(MAX_WRITE_BYTES);
        assert!(compose_write_bytes(&command, Some(""), "\r").is_ok());
        assert!(compose_write_bytes(&command, None, "\r").is_err());
    }

    #[test]
    fn read_args_parse_direction_operation_id_and_collapse_default() {
        let args: ReadArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "direction": "rx",
            "operation_id": Uuid::nil(),
        }))
        .unwrap();
        assert_eq!(args.direction, Some(Direction::Rx));
        assert_eq!(args.operation_id, Some(Uuid::nil()));
        assert!(args.collapse_repeats);
        assert!(!args.include_raw);

        let args: ReadArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "direction": "none",
            "collapse_repeats": false,
        }))
        .unwrap();
        assert_eq!(args.direction, Some(Direction::None));
        assert!(!args.collapse_repeats);
    }

    #[test]
    fn search_and_wait_args_parse_new_optional_fields() {
        let search: SearchArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "contains": "ERROR",
            "operation_id": Uuid::nil(),
            "context_lines": 7,
            "collapse_repeats": false,
        }))
        .unwrap();
        assert_eq!(search.operation_id, Some(Uuid::nil()));
        assert_eq!(search.context_lines, Some(7));
        assert!(!search.collapse_repeats);

        let wait: WaitArgs = serde_json::from_value(json!({"slot_id": "bench"})).unwrap();
        assert!(wait.collapse_repeats);
    }

    #[test]
    fn wait_cursor_prefers_explicit_then_compatible_session_cursor() {
        let slot = test_slot();
        let explicit = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: 7,
        };
        let remembered = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: 21,
        };

        let (selected, source) =
            select_wait_cursor(Some(explicit.clone()), Some(remembered.clone()), &slot);
        assert_eq!(selected, explicit);
        assert_eq!(source, CursorSource::Explicit);

        let (selected, source) = select_wait_cursor(None, Some(remembered.clone()), &slot);
        assert_eq!(selected, remembered);
        assert_eq!(source, CursorSource::SessionLiveCursor);
        assert_eq!(source.label(), "session_live_cursor");
    }

    #[test]
    fn wait_cursor_falls_back_to_head_for_missing_or_stale_session_state() {
        let slot = test_slot();
        for remembered in [
            None,
            Some(Cursor {
                epoch: Uuid::new_v4(),
                after_seq: 21,
            }),
            Some(Cursor {
                epoch: slot.daemon_epoch,
                after_seq: slot.head_seq + 1,
            }),
        ] {
            let (selected, source) = select_wait_cursor(None, remembered, &slot);
            assert_eq!(
                selected,
                Cursor {
                    epoch: slot.daemon_epoch,
                    after_seq: slot.head_seq,
                }
            );
            assert_eq!(source, CursorSource::CurrentHead);
        }
    }

    #[test]
    fn remembered_live_cursor_is_monotonic_within_an_epoch_and_resets_across_epochs() {
        let mut cursors = BTreeMap::new();
        let first_epoch = Uuid::new_v4();
        remember_live_cursor(
            &mut cursors,
            "bench",
            Cursor {
                epoch: first_epoch,
                after_seq: 20,
            },
        );
        remember_live_cursor(
            &mut cursors,
            "bench",
            Cursor {
                epoch: first_epoch,
                after_seq: 10,
            },
        );
        assert_eq!(cursors["bench"].after_seq, 20);

        let next_epoch = Uuid::new_v4();
        remember_live_cursor(
            &mut cursors,
            "bench",
            Cursor {
                epoch: next_epoch,
                after_seq: 3,
            },
        );
        assert_eq!(
            cursors["bench"],
            Cursor {
                epoch: next_epoch,
                after_seq: 3,
            }
        );
    }

    #[test]
    fn command_args_accept_an_empty_command() {
        let args: CommandArgs =
            serde_json::from_value(json!({"slot_id": "bench", "command": ""})).unwrap();
        assert!(args.command.is_empty());
        assert!(args.collapse_repeats);
    }

    #[test]
    fn trigger_uses_only_explicit_call_text_and_eol() {
        let args: TriggerArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "initial_write": {"text": "reboot", "eol": "\r"},
            "start_contains": "Booting",
            "action": {"text": "slp"},
            "stop_contains": ["any caller literal"],
            "interval_ms": 20,
            "timeout_ms": 5000,
            "max_fires": 250,
            "chunk_size": 3,
            "inter_char_delay_ms": 0
        }))
        .unwrap();
        let spec = trigger_spec(&args, &test_slot()).unwrap();
        assert_eq!(spec.initial_write.as_deref(), Some(b"reboot\r".as_slice()));
        assert_eq!(spec.start_contains.as_deref(), Some(b"Booting".as_slice()));
        assert_eq!(spec.action, b"slp");
        assert_eq!(spec.stop_contains, vec![b"any caller literal".to_vec()]);
        assert_eq!(spec.interval_ms, 20);
        assert_eq!(spec.timeout_ms, 5000);
        assert_eq!(spec.max_fires, 250);
        assert_eq!(
            spec.pacing,
            Some(WritePacing {
                chunk_size: 3,
                chunk_delay_ms: 0,
            })
        );
    }

    #[test]
    fn trigger_allows_bounded_one_shot_without_a_stop_literal() {
        let args: TriggerArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "start_contains": "send ACK now",
            "action": {"text": "ACK", "eol": ""},
            "max_fires": 1
        }))
        .unwrap();
        let spec = trigger_spec(&args, &test_slot()).unwrap();
        assert!(spec.stop_contains.is_empty());
        assert_eq!(spec.max_fires, 1);
        assert_eq!(
            trigger_status_label(TriggerStatus::MaxFiresReached),
            "max_fires_reached"
        );
        assert!(!TriggerStatus::MaxFiresReached.is_matched());
    }

    #[test]
    fn trigger_rejects_empty_or_unbounded_payload_plans() {
        let empty: TriggerArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "action": {"text": "", "eol": ""}
        }))
        .unwrap();
        assert!(
            trigger_spec(&empty, &test_slot())
                .unwrap_err()
                .to_string()
                .contains("both empty")
        );

        let over_total: TriggerArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "initial_write": {"text": "x"},
            "action": {"text": "a".repeat(256)},
            "max_fires": 256
        }))
        .unwrap();
        assert!(
            trigger_spec(&over_total, &test_slot())
                .unwrap_err()
                .to_string()
                .contains("65536 bytes")
        );
    }

    #[test]
    fn trigger_stopping_is_not_a_terminal_outcome() {
        assert!(!TriggerStatus::Stopping.is_terminal());
        assert!(TriggerStatus::Cancelled.is_terminal());
        assert_eq!(trigger_status_label(TriggerStatus::Stopping), "stopping");
    }

    #[test]
    fn trigger_evidence_excludes_events_after_the_terminal_boundary() {
        assert!(!trigger_evidence_contains(40, 41, 73));
        assert!(trigger_evidence_contains(41, 41, 73));
        assert!(trigger_evidence_contains(73, 41, 73));
        assert!(!trigger_evidence_contains(74, 41, 73));
    }

    #[test]
    fn operation_filter_requires_an_explicit_tx_direction() {
        let operation_id = Some(Uuid::nil());
        let error = validate_operation_filter(operation_id, None).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("RX events have operation_id=null")
        );
        let error = validate_operation_filter(operation_id, Some(Direction::Rx)).unwrap_err();
        assert!(error.to_string().contains("direction=tx"));
        validate_operation_filter(operation_id, Some(Direction::Tx)).unwrap();
        validate_operation_filter(None, Some(Direction::Rx)).unwrap();
    }

    #[test]
    fn current_run_continuation_requires_an_exact_current_cursor_pair() {
        let slot = test_slot();
        assert_eq!(current_run_after_seq(None, None, &slot).unwrap(), None);
        assert_eq!(
            current_run_after_seq(Some(slot.daemon_epoch), Some(21), &slot).unwrap(),
            Some(21)
        );

        for (epoch, after_seq) in [(Some(slot.daemon_epoch), None), (None, Some(21))] {
            let error = current_run_after_seq(epoch, after_seq, &slot).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("requires epoch and after_seq together")
            );
        }

        let error = current_run_after_seq(Some(Uuid::new_v4()), Some(21), &slot).unwrap_err();
        assert!(error.to_string().contains("cursor epoch changed"));
        let error = current_run_after_seq(Some(slot.daemon_epoch), Some(slot.head_seq + 1), &slot)
            .unwrap_err();
        assert!(error.to_string().contains("cursor is ahead"));

        let run_id = Uuid::new_v4();
        assert_eq!(
            current_run_id(Some(run_id), Some(21), &slot).unwrap(),
            run_id
        );
        let error = current_run_id(None, Some(21), &slot).unwrap_err();
        assert!(error.to_string().contains("requires the run_id"));
    }

    #[test]
    fn truncated_current_run_search_guides_a_run_scoped_continuation() {
        let run_id = Uuid::new_v4();
        let epoch = Uuid::new_v4();
        let mut output = json!({"epoch": epoch, "after_seq": 1234});
        attach_search_continuation_guidance(&mut output, "current_run", Some(run_id));

        assert_eq!(output["continuation"]["scope"], "current_run");
        assert_eq!(output["continuation"]["epoch"], json!(epoch));
        assert_eq!(output["continuation"]["after_seq"], 1234);
        assert_eq!(output["continuation"]["run_id"], json!(run_id));
        assert!(output["guidance"].as_str().unwrap().contains("incomplete"));
        assert!(!output["guidance"].as_str().unwrap().contains("archive"));
    }

    #[test]
    fn completion_evidence_never_claims_command_success() {
        let evidence = completion_evidence(&Completion::Pattern("]# ".into()));
        assert_eq!(evidence["boundary_observed"], true);
        assert!(evidence.get("success").is_none());
        assert!(
            command_guidance(&Completion::Pattern("]# ".into()), false)
                .contains("execution_status remains unknown")
        );
        assert!(command_guidance(&Completion::Quiet, false).contains("not proof"));
    }

    #[test]
    fn display_names_are_disambiguated_by_port() {
        let mut slots = vec![
            json!({"display_name": "hawk", "port": "COM3"}),
            json!({"display_name": "hawk", "port": "COM7"}),
            json!({"display_name": "", "port": "COM9"}),
            json!({"display_name": "unique", "port": "COM11"}),
        ];
        disambiguate_display_names(&mut slots);
        assert_eq!(slots[0]["display_name"], "hawk (COM3)");
        assert_eq!(slots[1]["display_name"], "hawk (COM7)");
        assert_eq!(slots[2]["display_name"], "(COM9)");
        assert_eq!(slots[3]["display_name"], "unique");
    }

    fn test_slot() -> SlotSnapshot {
        let settings = serde_json::to_value(serial_protocol::SerialSettings::default()).unwrap();
        serde_json::from_value(json!({
            "config": {
                "id": "bench", "display_name": "Bench", "port": "COM3", "profile": "linux",
                "enabled": true, "settings": settings,
            },
            "daemon_epoch": Uuid::nil(),
            "head_seq": 42,
            "ring_oldest_seq": 1,
            "generation": 1,
            "endpoint_present": true,
            "session_state": "online",
            "state_reason": null,
            "target_activity": "active",
            "last_rx_wall_time_ns": null,
            "rx_offset": 0,
            "tx_offset": 0,
            "control": null,
            "active_run": null,
            "logging": "healthy"
        }))
        .unwrap()
    }

    #[test]
    fn pacing_is_unset_without_overrides_and_falls_back_per_field() {
        let slot = test_slot();
        assert_eq!(write_pacing(None, None, &slot), None);
        assert_eq!(
            write_pacing(Some(8), None, &slot),
            Some(WritePacing {
                chunk_size: 8,
                chunk_delay_ms: slot.config.settings.write_chunk_delay_ms,
            })
        );
        assert_eq!(
            write_pacing(None, Some(0), &slot),
            Some(WritePacing {
                chunk_size: slot.config.settings.write_chunk_size,
                chunk_delay_ms: 0,
            })
        );
        // A zero chunk size is meaningless, so it falls back like an unset one.
        assert_eq!(write_pacing(Some(0), None, &slot), None);
    }

    #[test]
    fn command_defaults_to_the_daemons_effective_device_profile_eol() {
        let mut slot = test_slot();
        assert_eq!(effective_write_eol(&slot), "\r");
        slot.effective_write_eol = Some("\n".into());
        assert_eq!(effective_write_eol(&slot), "\n");
        assert_eq!(
            compose_write_bytes("version", None, effective_write_eol(&slot)).unwrap(),
            b"version\n"
        );
    }

    #[test]
    fn command_echo_cleanup_uses_the_daemons_effective_device_profile_mode() {
        let mut slot = test_slot();
        slot.config.settings.echo = EchoMode::Off;
        assert_eq!(effective_echo_mode(&slot), EchoMode::Off);
        slot.effective_echo = Some(EchoMode::On);
        assert_eq!(effective_echo_mode(&slot), EchoMode::On);
    }

    #[test]
    fn completion_prefers_effective_device_profile_prompts() {
        let mut slot = test_slot();
        slot.config.settings.shell_prompt = Some("legacy# ".into());
        slot.effective_shell_prompt = Some("]# ".into());
        slot.effective_uboot_prompt = Some("SigmaStar #".into());
        let args: CommandArgs =
            serde_json::from_value(json!({"slot_id": "bench", "command": "version"})).unwrap();

        let (patterns, mode) = completion_patterns(&args, &slot).unwrap();
        assert_eq!(patterns, vec!["]# ".to_string(), "SigmaStar #".to_string()]);
        assert_eq!(mode, "auto");
    }

    #[test]
    fn promptless_effective_profile_does_not_fall_back_to_stale_slot_prompts() {
        let mut slot = test_slot();
        slot.config.settings.shell_prompt = Some("legacy# ".into());
        slot.config.settings.uboot_prompt = Some("legacy=> ".into());
        slot.effective_write_eol = Some("\r".into());
        slot.effective_echo = Some(EchoMode::On);
        let args: CommandArgs =
            serde_json::from_value(json!({"slot_id": "bench", "command": "version"})).unwrap();

        let (patterns, mode) = completion_patterns(&args, &slot).unwrap();
        assert!(patterns.is_empty());
        assert_eq!(mode, "quiet");

        slot.effective_write_eol = None;
        slot.effective_echo = None;
        let (legacy_patterns, _) = completion_patterns(&args, &slot).unwrap();
        assert_eq!(legacy_patterns, vec!["legacy# ", "legacy=> "]);
    }

    #[test]
    fn command_args_parse_regex_pacing_and_lean_rendering_fields() {
        let args: CommandArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "command": "boot",
            "until_regex": "U-Boot \\d+",
            "inter_char_delay_ms": 5,
            "chunk_size": 16,
        }))
        .unwrap();
        assert_eq!(args.until_regex.as_deref(), Some("U-Boot \\d+"));
        assert_eq!(args.inter_char_delay_ms, Some(5));
        assert_eq!(args.chunk_size, Some(16));
        assert!(!args.include_events);
        assert!(!args.include_raw);

        let args: ReadArgs =
            serde_json::from_value(json!({"slot_id": "bench", "include_events": true})).unwrap();
        assert!(args.include_events);
    }

    #[test]
    fn exact_regex_replaces_configured_prompts_and_quiet_with_regex_mode() {
        let mut slot = test_slot();
        slot.effective_shell_prompt = Some("]# ".into());
        slot.effective_uboot_prompt = Some("SigmaStar #".into());
        slot.effective_write_eol = Some("\r".into());
        slot.effective_echo = Some(EchoMode::On);
        let args: CommandArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "command": "boot",
            "until_regex": "U-Boot \\d+"
        }))
        .unwrap();

        let (patterns, completion_mode) = completion_patterns(&args, &slot).unwrap();
        let until_regex = args
            .until_regex
            .as_deref()
            .map(regex::Regex::new)
            .transpose()
            .unwrap();
        let complete_on_quiet = completion_mode == "quiet";

        assert!(patterns.is_empty());
        assert_eq!(completion_mode, "regex");
        assert!(until_regex.is_some());
        assert!(!complete_on_quiet);
    }

    #[test]
    fn regex_boundary_rejects_competing_literal_completion_parameters() {
        let slot = test_slot();
        for completion in ["prompt", "contains", "quiet"] {
            let args: CommandArgs = serde_json::from_value(json!({
                "slot_id": "bench",
                "command": "boot",
                "completion": completion,
                "until_regex": "__DONE__"
            }))
            .unwrap();
            let error = completion_patterns(&args, &slot).unwrap_err();
            assert!(error.to_string().contains("sole completion boundary"));
        }

        let args: CommandArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "command": "boot",
            "until": "literal",
            "until_regex": "__DONE__"
        }))
        .unwrap();
        let error = completion_patterns(&args, &slot).unwrap_err();
        assert!(error.to_string().contains("cannot be combined"));

        assert!(
            compile_until_regex(Some(""))
                .unwrap_err()
                .to_string()
                .contains("must not be empty")
        );
        assert!(
            compile_until_regex(Some("("))
                .unwrap_err()
                .to_string()
                .contains("not a valid regex")
        );
    }
}
