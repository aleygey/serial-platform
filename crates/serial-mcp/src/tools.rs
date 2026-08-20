use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use serial_protocol::{
    Actor, ActorKind, CreateMonitorRequest, Cursor, DEFAULT_TRIGGER_INTERVAL_MS,
    DEFAULT_TRIGGER_MAX_FIRES, DEFAULT_TRIGGER_TIMEOUT_MS, DeviceModelListResponse, Direction,
    EchoMode, EventKind, EventQuery, EventQueryResponse, MAX_BREAK_DURATION_MS,
    MAX_COMMAND_DESCRIPTION_BYTES, MAX_PHYSICAL_WRITE_TIMEOUT_MS, MAX_TRIGGER_ACTION_BYTES,
    MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES, MAX_TRIGGER_INTERVAL_MS,
    MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS, MAX_TRIGGER_TIMEOUT_MS,
    MAX_TRIGGER_TOTAL_BYTES, MIN_BREAK_DURATION_MS, MIN_TRIGGER_INTERVAL_MS,
    MIN_TRIGGER_TIMEOUT_MS, ModelConfirmationMethod, PROTOCOL_VERSION, SequenceWritePrecondition,
    SessionState, SetSlotDeviceModelRequest, SlotSnapshot, StatusResponse, TriggerInfo,
    TriggerSpec, TriggerStatus, WritePacing,
};
use tokio::sync::oneshot;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    capture::{Capture, CaptureOptions, CommandBoundary, Completion, CompletionPattern},
    config::CaptureLimits,
    render::{MatchExcerptOptions, MatchExcerptPattern, RenderOptions, render_events},
    session::{LocalControlState, SequenceBoundaryRejected, SessionHandle},
};

const DEFAULT_TEXT_CHARS: usize = 16_000;
const MAX_WRITE_BYTES: usize = 4096;
const MAX_REGEX_BYTES: usize = 4096;
const MAX_COMMAND_SEQUENCE_STEPS: usize = 8;
const MAX_COMMAND_SEQUENCE_TOTAL_WRITE_BYTES: usize = MAX_COMMAND_SEQUENCE_STEPS * MAX_WRITE_BYTES;
const MAX_COMMAND_SEQUENCE_TIMEOUT_SECONDS: u64 = 300;
const MAX_MONITOR_DESCRIPTION_BYTES: usize = 1024;
const TRIGGER_STATUS_POLL: Duration = Duration::from_millis(50);
const TRIGGER_STATUS_MARGIN: Duration =
    Duration::from_millis(MAX_PHYSICAL_WRITE_TIMEOUT_MS + 5_000);
const TRIGGER_CANCEL_MARGIN: Duration = Duration::from_secs(5);
const MODEL_BINDING_SOURCE: &str = "agent:serial-mcp";
const MODEL_IDENTITY_WARNING: &str = "A configured model name is an assignment, not evidence of the connected hardware. On first connection or whenever the identity is uncertain, confirm it via serial evidence, telnet, the device web UI, or a human.";

struct PreparedCommandStep {
    bytes: Vec<u8>,
    description: String,
    timeout: Duration,
    patterns: Vec<CompletionPattern>,
    until_regex: Option<regex::Regex>,
    complete_on_quiet: bool,
    expected_echo: Option<Vec<u8>>,
}

struct ExecutedCommandStep {
    output: Value,
    completion: Completion,
    cursor: Cursor,
    truncated: bool,
    gap: bool,
    interfered: bool,
    echo_missing: bool,
    no_rx: bool,
}

struct CommandStepFailure {
    phase: &'static str,
    error: anyhow::Error,
}

impl CommandStepFailure {
    fn is_sequence_boundary_rejection(&self) -> bool {
        self.error
            .downcast_ref::<SequenceBoundaryRejected>()
            .is_some()
    }
}

struct SequenceStop {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ContextChanged {
    recent_context: Value,
}

impl std::fmt::Display for ContextChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "serial context changed since the previous Agent operation; no bytes were written; inspect recent_context with read(scope=tail) or wait, then retry"
        )
    }
}

impl std::error::Error for ContextChanged {}

pub(crate) fn structured_tool_error(error: &anyhow::Error) -> Option<Value> {
    let changed = error.downcast_ref::<ContextChanged>()?;
    Some(json!({
        "error": {
            "code": "context_changed",
            "message": changed.to_string(),
            "no_bytes_written": true,
            "recent_context": changed.recent_context,
            "retry_hint": "Call read(scope=tail) or wait to confirm the new serial state, then retry the operation."
        }
    }))
}

#[derive(Clone)]
pub struct AgentTools {
    api: ApiClient,
    session: SessionHandle,
    actor_label: String,
    capture_limits: CaptureLimits,
    live_cursors: Arc<StdMutex<BTreeMap<String, Cursor>>>,
    operation_cursors: Arc<StdMutex<BTreeMap<String, Cursor>>>,
    pending_context: Arc<StdMutex<BTreeMap<String, Value>>>,
    write_locks: Arc<StdMutex<BTreeMap<String, Arc<AsyncMutex<()>>>>>,
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
            live_cursors: Arc::new(StdMutex::new(BTreeMap::new())),
            operation_cursors: Arc::new(StdMutex::new(BTreeMap::new())),
            pending_context: Arc::new(StdMutex::new(BTreeMap::new())),
            write_locks: Arc::new(StdMutex::new(BTreeMap::new())),
        }
    }

    pub async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        let mut output = match name {
            "devices" => self.devices(parse(arguments)?).await,
            "device_models" => self.device_models(parse(arguments)?).await,
            "device_model_set" => self.device_model_set(parse(arguments)?).await,
            "read" => self.read(parse(arguments)?).await,
            "command" => self.command(parse(arguments)?).await,
            "command_sequence" => self.command_sequence(parse(arguments)?).await,
            "input" => self.input(parse(arguments)?).await,
            "signal" => self.signal(parse(arguments)?).await,
            "trigger" => self.trigger(parse(arguments)?).await,
            "wait" => self.wait(parse(arguments)?).await,
            "search" => self.search(parse(arguments)?).await,
            "monitor_start" => self.monitor_start(parse(arguments)?).await,
            "monitor_list" => self.monitor_list(parse(arguments)?).await,
            "monitor_status" => self.monitor_status(parse(arguments)?).await,
            "monitor_incidents" => self.monitor_incidents(parse(arguments)?).await,
            "monitor_stop" => self.monitor_stop(parse(arguments)?).await,
            "run_start" => self.run_start(parse(arguments)?).await,
            "run_end" => self.run_end(parse(arguments)?).await,
            "release" => self.release(parse(arguments)?).await,
            _ => bail!("unknown serial tool {name:?}"),
        }?;
        self.attach_recent_context(name, &mut output).await;
        Ok(output)
    }

    /// Adds a compact activity summary only when another serial actor acted
    /// after this MCP's previous successful operation and before the current
    /// one completed. An incomplete bounded-ring answer is surfaced even when
    /// it contains no matching event: only a complete empty answer proves that
    /// no third party changed the serial context.
    async fn attach_recent_context(&self, tool_name: &str, output: &mut Value) {
        if !matches!(
            tool_name,
            "read"
                | "command"
                | "command_sequence"
                | "input"
                | "signal"
                | "trigger"
                | "wait"
                | "run_start"
        ) {
            return;
        }
        // Archived evidence can describe another daemon epoch and therefore
        // cannot acknowledge a pending change on the live physical session.
        if tool_name == "read" && output.get("scope").and_then(Value::as_str) == Some("archive") {
            return;
        }
        let Some(slot_id) = output.get("slot_id").and_then(Value::as_str) else {
            return;
        };
        let Some(after_seq) = output.pointer("/cursor/after_seq").and_then(Value::as_u64) else {
            return;
        };
        let Some(epoch) = output
            .pointer("/cursor/epoch")
            .cloned()
            .and_then(|value| serde_json::from_value::<Uuid>(value).ok())
        else {
            return;
        };
        let current = Cursor { epoch, after_seq };
        let is_observation = matches!(tool_name, "read" | "wait");
        let previous = self
            .operation_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(slot_id.to_owned(), current.clone());
        if tool_name == "run_start" {
            return;
        }
        let Some(previous) = previous else {
            if is_observation {
                self.clear_pending_context(slot_id);
            }
            return;
        };
        if previous.epoch != current.epoch || previous.after_seq >= current.after_seq {
            if is_observation {
                self.clear_pending_context(slot_id);
            }
            return;
        }
        let context = self
            .recent_context_between(slot_id, &previous, &current)
            .await;
        if is_observation {
            self.clear_pending_context(slot_id);
        } else if let Some(context) = context.as_ref() {
            self.pending_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(slot_id.to_owned(), context.clone());
        }
        if let Some(context) = context {
            output["recent_context"] = context;
        }
    }

    async fn recent_context_between(
        &self,
        slot_id: &str,
        previous: &Cursor,
        current: &Cursor,
    ) -> Option<Value> {
        if previous.epoch != current.epoch || previous.after_seq >= current.after_seq {
            return None;
        }
        let own_actor_id = self.session.actor_id().await.ok().flatten();
        match self
            .api
            .recent_activity(
                slot_id,
                current.epoch,
                previous.after_seq,
                current.after_seq,
            )
            .await
        {
            Ok(activity) => {
                summarize_recent_context(activity, own_actor_id.as_deref(), previous, current)
            }
            Err(error) => Some(json!({
                "interference": false,
                "complete": false,
                "after_seq": previous.after_seq,
                "through_seq": current.after_seq,
                "events": [],
                "truncated": true,
                "warning": format!("could not prove the serial context was unchanged: {error}"),
            })),
        }
    }

    /// Fails before a physical action whenever the bounded live history shows
    /// a third-party action, or cannot prove that none occurred. The caller
    /// deliberately does not advance the operation cursor on failure: a
    /// subsequent read/wait is the explicit acknowledgement boundary.
    async fn ensure_serial_context_unchanged(&self, slot: &SlotSnapshot) -> Result<()> {
        if let Some(recent_context) = self
            .pending_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&slot.config.id)
            .cloned()
        {
            return Err(ContextChanged { recent_context }.into());
        }
        let previous = self
            .operation_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&slot.config.id)
            .cloned();
        let Some(previous) = previous else {
            return Ok(());
        };
        let current = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: slot.head_seq,
        };
        if previous.epoch != current.epoch {
            let recent_context = json!({
                "interference": false,
                "complete": false,
                "after_seq": previous.after_seq,
                "through_seq": current.after_seq,
                "events": [],
                "truncated": true,
                "warning": "daemon epoch changed since the previous Agent operation",
            });
            self.pending_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(slot.config.id.clone(), recent_context.clone());
            return Err(ContextChanged { recent_context }.into());
        }
        if let Some(recent_context) = self
            .recent_context_between(&slot.config.id, &previous, &current)
            .await
        {
            self.pending_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(slot.config.id.clone(), recent_context.clone());
            return Err(ContextChanged { recent_context }.into());
        }
        Ok(())
    }

    fn clear_pending_context(&self, slot_id: &str) {
        self.pending_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(slot_id);
    }

    async fn context_changed_after_boundary(
        &self,
        slot: &SlotSnapshot,
        boundary_error: &anyhow::Error,
    ) -> anyhow::Error {
        let current_slot = self
            .slot(&slot.config.id)
            .await
            .unwrap_or_else(|_| slot.clone());
        let current = Cursor {
            epoch: current_slot.daemon_epoch,
            after_seq: current_slot.head_seq,
        };
        let previous = self
            .operation_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&slot.config.id)
            .cloned()
            .unwrap_or(Cursor {
                epoch: slot.daemon_epoch,
                after_seq: slot.head_seq,
            });
        let recent_context = if previous.epoch == current.epoch {
            self.recent_context_between(&slot.config.id, &previous, &current)
                .await
        } else {
            None
        }
        .unwrap_or_else(|| {
            json!({
                "interference": false,
                "complete": false,
                "after_seq": previous.after_seq,
                "through_seq": current.after_seq,
                "events": [],
                "truncated": true,
                "warning": format!("daemon rejected the serial-context boundary: {boundary_error}"),
            })
        });
        self.pending_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(slot.config.id.clone(), recent_context.clone());
        ContextChanged { recent_context }.into()
    }

    async fn devices(&self, args: DevicesArgs) -> Result<Value> {
        let status = self.status().await?;
        let model_catalog = self.api.device_models_if_supported().await?;
        let mut slots: Vec<Value> = status
            .slots
            .iter()
            .filter(|slot| args.slot_id.as_ref().is_none_or(|id| &slot.config.id == id))
            .map(|slot| {
                let mut summary = slot_summary(slot);
                summary["device_model"] = model_catalog.as_ref().map_or(Value::Null, |catalog| {
                    slot_device_model_summary(&slot.config.id, catalog)
                });
                summary
            })
            .collect();
        disambiguate_display_names(&mut slots);
        if let Some(slot_id) = args.slot_id
            && slots.is_empty()
        {
            bail!("unknown Slot {slot_id:?}");
        }
        Ok(json!({
            "daemon_epoch": status.daemon_epoch,
            "model_config_revision": model_catalog.as_ref().map(|catalog| catalog.config_revision),
            "device_model_capability": if model_catalog.is_some() { "available" } else { "unavailable" },
            "slots": slots,
            "selection_note": "Choose a Slot explicitly and confirm that its configured model matches the physical DUT before connecting or writing. Confirm by serial evidence, telnet, the device web UI, or a human; a configured name alone is not proof. A Run isolates only its log/event interval; it does not reset, initialize, or otherwise isolate device state."
        }))
    }

    async fn device_models(&self, args: DeviceModelsArgs) -> Result<Value> {
        if let Some(slot_id) = args.slot_id.as_deref() {
            // A missing binding is valid for a known Slot, so validate the
            // filter independently instead of treating an empty result as an
            // unknown Slot.
            self.slot(slot_id).await?;
        }
        let catalog = self.api.device_models().await?;
        let bindings = catalog
            .bindings
            .into_iter()
            .filter(|binding| {
                args.slot_id
                    .as_deref()
                    .is_none_or(|slot_id| binding.slot_id == slot_id)
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "config_revision": catalog.config_revision,
            "models": catalog.models,
            "bindings": bindings,
            "slot_id_filter": args.slot_id,
            "identity_warning": MODEL_IDENTITY_WARNING,
        }))
    }

    async fn device_model_set(&self, args: DeviceModelSetArgs) -> Result<Value> {
        let catalog = self.api.device_models().await?;
        let observed_previous_model_id = catalog
            .bindings
            .iter()
            .find(|binding| binding.slot_id == args.slot_id)
            .map(|binding| binding.model_id.clone());
        let request = build_device_model_set_request(&args, &catalog)?;
        let response = self
            .api
            .set_slot_device_model(&args.slot_id, &request)
            .await?;
        let shared_model_warning =
            (args.update_existing && response.affected_slots.len() > 1).then(|| {
            format!(
                "This model node is shared by {} Slots; updated name, parent, or aliases are visible to every affected Slot.",
                response.affected_slots.len()
            )
        });
        Ok(json!({
            "slot_id": args.slot_id,
            "previous_model_id": observed_previous_model_id,
            "binding": response.binding,
            "model": response.model,
            "created": response.created,
            "affected_slots": response.affected_slots,
            "shared_model_warning": shared_model_warning,
            "config_revision": response.config_revision,
            "identity_warning": MODEL_IDENTITY_WARNING,
            "next_step": "Before connecting or issuing commands, re-confirm the physical DUT whenever this is its first connection or its identity remains uncertain."
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
        let slot = self.slot(&args.slot_id).await?;
        let scope = args.scope.as_deref().unwrap_or("tail");
        if args.through_seq.is_some() && scope != "archive" {
            bail!("through_seq is only valid with scope=archive");
        }
        let (epoch, response) = match scope {
            "tail" => {
                let response = self.api.live_tail(&args.slot_id, 200, None).await?;
                let epoch = response
                    .next_cursor
                    .as_ref()
                    .map(|cursor| cursor.epoch)
                    .unwrap_or(slot.daemon_epoch);
                (epoch, response)
            }
            "continue" => {
                let cursor = self.live_cursor(&slot.config.id).unwrap_or(Cursor {
                    epoch: slot.daemon_epoch,
                    after_seq: slot.head_seq,
                });
                let response = self
                    .api
                    .live_tail(&args.slot_id, 1_000, Some(&cursor))
                    .await?;
                let response_epoch = response
                    .next_cursor
                    .as_ref()
                    .map_or(slot.daemon_epoch, |next| next.epoch);
                (response_epoch, response)
            }
            "archive" => {
                let epoch = args
                    .epoch
                    .context("scope=archive requires an explicit epoch")?;
                let response = self
                    .api
                    .events(
                        &args.slot_id,
                        &EventQuery {
                            epoch: Some(epoch),
                            after_seq: args.after_seq,
                            through_seq: args.through_seq,
                            before_wall_time_ns: None,
                            after_wall_time_ns: None,
                            direction: None,
                            kind: None,
                            actor_id: None,
                            run_id: None,
                            operation_id: None,
                            contains: None,
                            regex: None,
                            limit_events: Some(1000),
                            limit_bytes: Some(512 * 1024),
                        },
                    )
                    .await?;
                (epoch, response)
            }
            _ => bail!("scope must be tail, continue, or archive"),
        };
        let mut output = render_response(
            &slot,
            epoch,
            response,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
            scope,
        );
        if scope == "tail" {
            output["source"] = json!("live_ring");
            output["bounded_tail"] = json!(true);
            output["tail_events"] = json!(200);
        } else if scope == "continue" {
            output["source"] = json!("live_ring");
            output["bounded_continue"] = json!(true);
            output["limit_events"] = json!(1_000);
        }
        if output["cursor"]["epoch"] == json!(slot.daemon_epoch)
            && let Some(after_seq) = output["cursor"]["after_seq"].as_u64()
        {
            self.remember_live_cursor(
                &slot.config.id,
                Cursor {
                    epoch: slot.daemon_epoch,
                    after_seq,
                },
            );
        }
        Ok(output)
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        if args.query.trim().is_empty() {
            bail!("query must not be empty");
        }
        let compiled_regex = args
            .regex
            .then(|| compile_regex(&args.query, "query"))
            .transpose()?;
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
                    .or_else(|| self.live_cursor(&slot.config.id))
                    .context("scope=current_cursor has no remembered cursor; call read/run_start first or pass epoch and after_seq")?;
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
        let query = EventQuery {
            epoch: Some(epoch),
            after_seq,
            through_seq: None,
            before_wall_time_ns: None,
            after_wall_time_ns: None,
            direction: None,
            kind: None,
            actor_id: None,
            run_id,
            operation_id: None,
            contains: (!args.regex).then(|| args.query.clone()),
            regex: args.regex.then(|| args.query.clone()),
            limit_events: Some(1000),
            limit_bytes: Some(1024 * 1024),
        };
        let response = self.api.events(&args.slot_id, &query).await?;
        let no_matches = response.events.is_empty();
        let truncated = response.truncated;
        let mut output = render_response(
            &slot,
            epoch,
            response,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: if args.regex {
                    compiled_regex.as_ref().map(|regex| MatchExcerptOptions {
                        pattern: MatchExcerptPattern::Regex(regex),
                        context_lines: 5,
                    })
                } else {
                    Some(MatchExcerptOptions {
                        pattern: MatchExcerptPattern::Literal(&args.query),
                        context_lines: 5,
                    })
                },
            },
            scope,
        );
        output["matched"] = json!(!no_matches);
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

    async fn monitor_start(&self, args: MonitorStartArgs) -> Result<Value> {
        let request = create_monitor_request(args)?;
        let status = self.status().await?;
        if !status
            .slots
            .iter()
            .any(|slot| slot.config.id == request.spec.slot_id)
        {
            bail!("unknown Slot {:?}", request.spec.slot_id);
        }
        let response = self.api.create_monitor(&request).await?;
        let monitor = monitor_from_response(response)?;
        let mut output = compact_monitor(&monitor);
        output["persistent"] = json!(true);
        output["returns_immediately"] = json!(true);
        output["guidance"] = json!(
            "The Monitor runs in seriald after this MCP call ends. Call monitor_incidents without after for the recent tail, then continue with that tool's next_after cursor."
        );
        Ok(output)
    }

    async fn monitor_list(&self, args: MonitorListArgs) -> Result<Value> {
        self.status().await?;
        let response = serde_json::to_value(self.api.monitors(args.slot_id.as_deref()).await?)
            .context("seriald returned an invalid Monitor list")?;
        let monitors = response
            .get("monitors")
            .and_then(Value::as_array)
            .context("seriald Monitor list omitted monitors")?
            .iter()
            .map(compact_monitor)
            .collect::<Vec<_>>();
        let count = monitors.len();
        Ok(json!({"monitors": monitors, "count": count}))
    }

    async fn monitor_status(&self, args: MonitorIdArgs) -> Result<Value> {
        self.status().await?;
        let response = self.api.monitor(args.monitor_id).await?;
        Ok(compact_monitor(&monitor_from_response(response)?))
    }

    async fn monitor_incidents(&self, args: MonitorIncidentsArgs) -> Result<Value> {
        let requested_tail = args.after.is_none();
        let after = args
            .after
            .as_deref()
            .map(parse_monitor_cursor)
            .transpose()?;
        self.status().await?;
        let response =
            serde_json::to_value(self.api.monitor_incidents(args.monitor_id, after).await?)
                .context("seriald returned an invalid Monitor incident page")?;
        let incidents = response
            .get("incidents")
            .and_then(Value::as_array)
            .context("seriald Monitor incident page omitted incidents")?
            .iter()
            .map(compact_monitor_incident)
            .collect::<Vec<_>>();
        let count = incidents.len();
        let next_after = response
            .get("next_cursor")
            .and_then(Value::as_u64)
            .map(|cursor| cursor.to_string());
        let truncated = response
            .get("truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let first_available = response
            .get("first_available_incident_seq")
            .and_then(Value::as_u64)
            .map(|cursor| cursor.to_string());
        let retention_gap = response
            .get("retention_gap")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let guidance = match (requested_tail, truncated) {
            (true, true) => {
                "This is the recent tail; older retained incidents were omitted. Use after=\"0\" to page from the oldest retained incident, or next_after to poll only newer incidents."
            }
            (true, false) => {
                "This is the complete retained tail. Use next_after as after to poll only newer incidents."
            }
            (false, true) => {
                "More incidents are retained after the requested cursor; call monitor_incidents again with next_after as after."
            }
            (false, false) => {
                "This is the complete retained page after the requested cursor. Use next_after later to poll newer incidents."
            }
        };
        Ok(json!({
            "monitor_id": args.monitor_id,
            "started_after": args.after,
            "mode": if requested_tail { "recent_tail" } else { "forward" },
            "incidents": incidents,
            "count": count,
            "next_after": next_after,
            "truncated": truncated,
            "first_available_after": first_available,
            "retention_gap": retention_gap,
            "has_older_retained": requested_tail && truncated,
            "warning": retention_gap.then_some("The requested cursor predates retained Monitor incidents; some evidence has been pruned."),
            "guidance": guidance
        }))
    }

    async fn monitor_stop(&self, args: MonitorIdArgs) -> Result<Value> {
        let existing = monitor_from_response(self.api.monitor(args.monitor_id).await?)?;
        let revision = existing
            .get("revision")
            .and_then(Value::as_u64)
            .context("seriald Monitor response omitted revision")?;
        let response = self.api.stop_monitor(args.monitor_id, revision).await?;
        let mut output = compact_monitor(&monitor_from_response(response)?);
        output["stopped"] = json!(true);
        output["incidents_retained"] = json!(true);
        Ok(output)
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
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let slot = self.slot_online(&run_use.slot_id).await?;
        let active_run = matching_active_run(&slot, run_use.run_id, "wait")?;
        let watched_run = (active_run.id, active_run.start_seq);
        let (patterns, until_regex, completion_mode) =
            requested_completion(args.expect.as_deref(), args.regex.as_deref(), &slot, true)?;
        let complete_on_quiet = completion_mode == "quiet";
        let remembered_cursor = self.live_cursor(&slot.config.id);
        let (cursor, _) = select_wait_cursor(None, remembered_cursor, &slot);
        let started_epoch = cursor.epoch;
        let started_after_seq = cursor.after_seq;
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            run_use.slot_id.clone(),
            cursor,
            self.capture_limits,
        )
        .await?;
        let capture = capture.watch_run(watched_run.0);
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: Duration::from_millis(1_000),
                patterns,
                until_regex,
                complete_on_quiet,
                allow_empty_quiet: false,
            })
            .await;
        if let Completion::RunAborted { run_id, reason } = &result.completion {
            let last_seq = result.through_seq.unwrap_or(started_after_seq);
            self.remember_live_cursor(
                &slot.config.id,
                Cursor {
                    epoch: started_epoch,
                    after_seq: last_seq,
                },
            );
            let start_seq = if watched_run.0 == *run_id {
                watched_run.1
            } else {
                started_after_seq
            };
            return Err(self
                .run_abort_error(&slot, *run_id, start_seq, reason, true)
                .await);
        }
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
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
        let gap = !result.gaps.is_empty();
        let truncated = result.truncated || rendered.text_truncated;
        let confidence = capture_confidence(&result.completion, truncated, gap);
        let mut output = json!({
            "slot_id": slot.config.id,
            "run_handle": args.run_handle,
            "run_open": true,
            "capture": completion_kind(&result.completion),
            "confidence": confidence,
            "text": rendered.text,
            "truncated": truncated,
            "gap": gap,
            "cursor": {"epoch": started_epoch, "after_seq": last_seq}
        });
        attach_capture_warnings(
            &mut output,
            &result.completion,
            result.truncated,
            rendered.text_truncated,
            gap,
            false,
            false,
            result
                .events
                .iter()
                .all(|event| event.direction != Direction::Rx),
        );
        attach_omission(&mut output, &rendered);
        Ok(output)
    }

    async fn command(&self, args: CommandArgs) -> Result<Value> {
        validate_command_description(&args.description)?;
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.slot_id).await;
        let slot = self
            .slot_online_for_physical_action(&run_use.slot_id)
            .await?;
        let active_run = matching_active_run(&slot, run_use.run_id, "command")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        let expected_run_id = run_use.run_id;
        let run_start_seq = active_run.start_seq;
        let prepared = prepare_command_step(
            &args.command,
            args.description,
            args.expect.as_deref(),
            args.regex.as_deref(),
            seconds(args.timeout_seconds, 10, 1, 120),
            &slot,
        )?;
        let executed = match self
            .execute_command_step(
                &slot,
                expected_run_id,
                run_use.run_token,
                run_start_seq,
                slot.head_seq,
                prepared,
                None,
                Some(serial_context_precondition(&slot)),
            )
            .await
        {
            Ok(executed) => executed,
            Err(failure) if failure.is_sequence_boundary_rejection() => {
                return Err(self
                    .context_changed_after_boundary(&slot, &failure.error)
                    .await);
            }
            Err(failure) => return Err(failure.error),
        };
        if let Completion::RunAborted { run_id, reason } = &executed.completion {
            return Err(self
                .run_abort_error(&slot, *run_id, run_start_seq, reason, false)
                .await);
        }
        let mut output = executed.output;
        attach_run_state(&mut output, &args.run_handle, true);
        Ok(output)
    }

    async fn command_sequence(&self, args: CommandSequenceArgs) -> Result<Value> {
        let CommandSequenceArgs {
            run_handle,
            description,
            steps,
        } = args;
        validate_command_description(&description)?;
        validate_command_sequence_shape(&steps)?;

        // One Run pin and one process-local Slot write lock cover the entire
        // dependent interaction. No other call through this MCP can insert a
        // write between two sequence steps.
        let run_use = self.session.authorize_run_use(run_handle.clone()).await?;
        let slot_id = run_use.slot_id.clone();
        let run_id = run_use.run_id;
        let run_token = run_use.run_token;
        let _write_guard = self.write_guard(&slot_id).await;
        let status = self.status().await?;
        ensure_sequence_write_precondition_supported(&status)?;
        ensure_serial_context_precondition_supported(&status)?;
        let slot = status
            .slots
            .into_iter()
            .find(|slot| slot.config.id == slot_id)
            .with_context(|| format!("unknown Slot {slot_id:?}"))?;
        if slot.session_state != SessionState::Online {
            bail!(
                "Slot {slot_id:?} is {:?}: {}",
                slot.session_state,
                slot.state_reason.as_deref().unwrap_or("no reason reported")
            );
        }
        let active_run = matching_active_run(&slot, run_id, "command_sequence")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        let run_start_seq = active_run.start_seq;

        // This completes every validation that depends on the effective Slot
        // profile (notably the physical EOL byte count) before step 1 writes.
        let prepared_steps = prepare_command_sequence_steps(steps, &slot)?;
        let requested_steps = prepared_steps.len();
        let sequence_id = Uuid::new_v4();
        let mut capture_after_seq = slot.head_seq;
        let mut expected_tx_offset = slot.tx_offset;
        let mut completed_steps = 0usize;
        let mut step_outputs = Vec::with_capacity(requested_steps);

        for (step_index, prepared) in prepared_steps.into_iter().enumerate() {
            let has_next = step_index + 1 < requested_steps;
            let audit = serial_protocol::CommandSequenceAuditContext {
                sequence_id,
                description: description.clone(),
                step_index: step_index as u8,
                step_count: requested_steps as u8,
            };
            let precondition = SequenceWritePrecondition {
                cursor: Cursor {
                    epoch: slot.daemon_epoch,
                    after_seq: capture_after_seq,
                },
                expected_generation: slot.generation,
                expected_tx_offset,
            };
            let planned_write_bytes = prepared.bytes.len() as u64;
            let executed = match self
                .execute_command_step(
                    &slot,
                    run_id,
                    run_token,
                    run_start_seq,
                    capture_after_seq,
                    prepared,
                    Some(audit),
                    Some(precondition),
                )
                .await
            {
                Ok(executed) => executed,
                Err(failure) => {
                    let boundary_changed = failure.is_sequence_boundary_rejection();
                    if step_outputs.is_empty() {
                        if boundary_changed {
                            return Err(self
                                .context_changed_after_boundary(&slot, &failure.error)
                                .await);
                        }
                        return Err(failure.error);
                    }
                    let mut output = command_sequence_output(
                        &slot,
                        run_id,
                        sequence_id,
                        description,
                        requested_steps,
                        completed_steps,
                        step_outputs,
                        Some(json!({
                            "step_index": step_index,
                            "phase": failure.phase,
                            "code": if boundary_changed { "sequence_boundary_changed" } else { "step_error" },
                            "message": failure.error.to_string(),
                            "next_step_sent": false,
                        })),
                    );
                    let run_open = self
                        .session
                        .run_ownership_retained(slot_id.clone(), run_id, run_token)
                        .await
                        .unwrap_or(false);
                    attach_run_state(&mut output, &run_handle, run_open);
                    return Ok(output);
                }
            };
            expected_tx_offset = expected_tx_offset
                .checked_add(planned_write_bytes)
                .context("command_sequence TX offset overflowed")?;
            capture_after_seq = executed.cursor.after_seq;
            let mut stop = command_sequence_stop(&executed, has_next);
            if let Completion::RunAborted { run_id, reason } = &executed.completion {
                stop = Some(SequenceStop {
                    code: "run_aborted",
                    message: self
                        .run_abort_error(&slot, *run_id, run_start_seq, reason, false)
                        .await
                        .to_string(),
                });
            }

            let mut output = executed.output;
            output["sequence_id"] = json!(sequence_id);
            output["step_index"] = json!(step_index);
            output["step_count"] = json!(requested_steps);
            output["status"] = json!(if stop.is_none() {
                "completed"
            } else {
                "partial"
            });
            output["safe_to_advance"] = json!(has_next && stop.is_none());
            step_outputs.push(output);

            if let Some(stop) = stop {
                let run_open = if sequence_stop_forces_closed(&stop) {
                    false
                } else {
                    self.session
                        .run_ownership_retained(slot_id.clone(), run_id, run_token)
                        .await
                        .unwrap_or(false)
                };
                let mut output = command_sequence_output(
                    &slot,
                    run_id,
                    sequence_id,
                    description,
                    requested_steps,
                    completed_steps,
                    step_outputs,
                    Some(json!({
                        "step_index": step_index,
                        "phase": "capture",
                        "code": stop.code,
                        "message": stop.message,
                        "next_step_sent": false,
                    })),
                );
                attach_run_state(&mut output, &run_handle, run_open);
                return Ok(output);
            }
            completed_steps += 1;
        }

        let mut output = command_sequence_output(
            &slot,
            run_id,
            sequence_id,
            description,
            requested_steps,
            completed_steps,
            step_outputs,
            None,
        );
        let run_open = self
            .session
            .run_ownership_retained(slot_id, run_id, run_token)
            .await
            .unwrap_or(false);
        attach_run_state(&mut output, &run_handle, run_open);
        Ok(output)
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_command_step(
        &self,
        slot: &SlotSnapshot,
        expected_run_id: Uuid,
        run_token: Uuid,
        run_start_seq: u64,
        capture_after_seq: u64,
        prepared: PreparedCommandStep,
        sequence: Option<serial_protocol::CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
    ) -> std::result::Result<ExecutedCommandStep, CommandStepFailure> {
        let operation_id = Uuid::new_v4();
        let cursor = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: capture_after_seq,
        };
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            slot.config.id.clone(),
            cursor,
            self.capture_limits,
        )
        .await
        .map_err(|error| CommandStepFailure {
            phase: "attach",
            error,
        })?
        .watch_run(expected_run_id);

        let write = self
            .session
            .write(
                slot.config.id.clone(),
                prepared.bytes,
                operation_id,
                expected_run_id,
                run_token,
                effective_write_pacing(slot),
                Some(prepared.description.clone()),
                sequence,
                sequence_precondition,
            )
            .await
            .map_err(|error| CommandStepFailure {
                phase: "write",
                error,
            });
        let write = match write {
            Ok(write) => write,
            Err(mut failure) => {
                failure.error = self
                    .session_run_error(slot, expected_run_id, run_start_seq, failure.error, true)
                    .await;
                return Err(failure);
            }
        };

        let result = capture
            .collect_after_write(
                CaptureOptions {
                    timeout: prepared.timeout,
                    quiet: Duration::from_millis(1_000),
                    patterns: prepared.patterns,
                    until_regex: prepared.until_regex,
                    complete_on_quiet: prepared.complete_on_quiet,
                    // A quiet boundary needs post-TX RX evidence. In
                    // particular, an empty command window must not return
                    // "complete" merely because the timer elapsed.
                    allow_empty_quiet: false,
                },
                CommandBoundary {
                    tx_event_seq: write.event_seq,
                    operation_id,
                    expected_echo: prepared.expected_echo,
                },
            )
            .await;
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                // collect_after_write has already consumed the complete
                // authoritative echo while arming the completion watcher.
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
        );
        let boundary = result
            .command_boundary
            .as_ref()
            .ok_or_else(|| CommandStepFailure {
                phase: "capture",
                error: anyhow!("command capture lost its authoritative write boundary"),
            })?;
        let interfered = boundary.interfered;
        let echo_missing = boundary.echo_required && !boundary.echo_observed;
        let last_seq = result.through_seq.unwrap_or(write.event_seq);
        let rx_event_count = result
            .events
            .iter()
            .filter(|event| event.direction == Direction::Rx)
            .count();
        let gap = !result.gaps.is_empty();
        let truncated = result.truncated || rendered.text_truncated;
        let confidence = command_confidence(
            &result.completion,
            truncated,
            gap,
            interfered,
            echo_missing,
            rx_event_count,
        );
        let cursor = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: last_seq,
        };
        self.remember_live_cursor(&slot.config.id, cursor.clone());
        let mut output = json!({
            "slot_id": slot.config.id,
            "write": if echo_missing { "uncertain" } else { "confirmed" },
            "capture": completion_kind(&result.completion),
            "execution": "unknown",
            "confidence": confidence,
            "text": rendered.text,
            "truncated": truncated,
            "gap": gap,
            "interfered": interfered,
            "run_id": expected_run_id,
            "operation_id": operation_id,
            "event_seq": write.event_seq,
            "description": prepared.description,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": last_seq}
        });
        let no_rx = command_has_no_rx(rx_event_count, boundary.echo_observed);
        attach_capture_warnings(
            &mut output,
            &result.completion,
            result.truncated,
            rendered.text_truncated,
            gap,
            interfered,
            echo_missing,
            no_rx,
        );
        attach_omission(&mut output, &rendered);
        Ok(ExecutedCommandStep {
            output,
            completion: result.completion,
            cursor,
            truncated,
            gap,
            interfered,
            echo_missing,
            no_rx,
        })
    }

    async fn input(&self, args: InputArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.slot_id).await;
        let slot = self
            .slot_online_for_physical_action(&run_use.slot_id)
            .await?;
        matching_active_run(&slot, run_use.run_id, "input")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        let bytes = args.text.into_bytes();
        if bytes.is_empty() {
            bail!("input text must not be empty");
        }
        if bytes.len() > MAX_WRITE_BYTES {
            bail!("input text exceeds {MAX_WRITE_BYTES} UTF-8 bytes");
        }
        let mut output = self
            .write_raw(&slot, bytes, "input", run_use.run_id, run_use.run_token)
            .await?;
        attach_run_state(&mut output, &args.run_handle, true);
        Ok(output)
    }

    async fn signal(&self, args: SignalArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.slot_id).await;
        let slot = self
            .slot_online_for_physical_action(&run_use.slot_id)
            .await?;
        matching_active_run(&slot, run_use.run_id, "signal")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        if args.signal == "break" {
            let duration_ms = args.duration_ms.unwrap_or(250);
            if !(MIN_BREAK_DURATION_MS..=MAX_BREAK_DURATION_MS).contains(&duration_ms) {
                bail!(
                    "duration_ms must be between {MIN_BREAK_DURATION_MS} and \
                     {MAX_BREAK_DURATION_MS}"
                );
            }
            let mut output = self
                .send_break(&slot, duration_ms, run_use.run_id, run_use.run_token)
                .await?;
            attach_run_state(&mut output, &args.run_handle, true);
            return Ok(output);
        }
        if args.duration_ms.is_some() {
            bail!("duration_ms is valid only for signal=break");
        }
        let byte = control_signal_byte(&args.signal)
            .context("signal must be ctrl_c, ctrl_d, ctrl_z, or break")?;
        let mut output = self
            .write_raw(
                &slot,
                vec![byte],
                &args.signal,
                run_use.run_id,
                run_use.run_token,
            )
            .await?;
        attach_run_state(&mut output, &args.run_handle, true);
        Ok(output)
    }

    async fn send_break(
        &self,
        slot: &SlotSnapshot,
        duration_ms: u64,
        expected_run_id: Uuid,
        run_token: Uuid,
    ) -> Result<Value> {
        let active_run = matching_active_run(slot, expected_run_id, "signal")?;
        let operation_id = Uuid::new_v4();
        let sent = match self
            .session
            .send_break(
                slot.config.id.clone(),
                duration_ms,
                operation_id,
                expected_run_id,
                run_token,
                serial_context_precondition(slot),
            )
            .await
        {
            Ok(sent) => sent,
            Err(error) if error.downcast_ref::<SequenceBoundaryRejected>().is_some() => {
                return Err(self.context_changed_after_boundary(slot, &error).await);
            }
            Err(error) => {
                return Err(self
                    .session_run_error(slot, expected_run_id, active_run.start_seq, error, true)
                    .await);
            }
        };
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: sent.event_seq,
            },
        );
        Ok(json!({
            "slot_id": slot.config.id,
            "write": "confirmed",
            "kind": "break",
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": sent.event_seq}
        }))
    }

    async fn write_raw(
        &self,
        slot: &SlotSnapshot,
        bytes: Vec<u8>,
        label: &str,
        expected_run_id: Uuid,
        run_token: Uuid,
    ) -> Result<Value> {
        let active_run = matching_active_run(slot, expected_run_id, "input/signal")?;
        let operation_id = Uuid::new_v4();
        let byte_count = bytes.len();
        let write = match self
            .session
            .write(
                slot.config.id.clone(),
                bytes,
                operation_id,
                expected_run_id,
                run_token,
                effective_write_pacing(slot),
                None,
                None,
                Some(serial_context_precondition(slot)),
            )
            .await
        {
            Ok(write) => write,
            Err(error) if error.downcast_ref::<SequenceBoundaryRejected>().is_some() => {
                return Err(self.context_changed_after_boundary(slot, &error).await);
            }
            Err(error) => {
                return Err(self
                    .session_run_error(slot, expected_run_id, active_run.start_seq, error, true)
                    .await);
            }
        };
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: write.event_seq,
            },
        );
        Ok(json!({
            "slot_id": slot.config.id,
            "write": "confirmed",
            "kind": label,
            "bytes": byte_count,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": write.event_seq}
        }))
    }

    async fn trigger(&self, args: TriggerArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.slot_id).await;
        let slot = self
            .slot_online_for_physical_action(&run_use.slot_id)
            .await?;
        let active_run = matching_active_run(&slot, run_use.run_id, "trigger")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        let expected_run_id = run_use.run_id;
        if let Some(active) = &slot.active_trigger {
            bail!(
                "Slot already has Trigger {} in status {:?}; wait for it to finish or cancel it \
                 from its owning client",
                active.id,
                active.status
            );
        }

        let spec = trigger_spec(&args)?;
        let operation_id = Uuid::new_v4();
        let attached_after_seq = slot.head_seq;
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            run_use.slot_id.clone(),
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
                run_use.slot_id.clone(),
                slot.daemon_epoch,
                slot.generation,
                operation_id,
                expected_run_id,
                run_use.run_token,
                serial_context_precondition(&slot),
                spec,
            )
            .await
        {
            Ok(trigger) => trigger,
            Err(error) if error.downcast_ref::<SequenceBoundaryRejected>().is_some() => {
                let _ = capture_stop.send(None);
                let _ = capture_task.await;
                return Err(self.context_changed_after_boundary(&slot, &error).await);
            }
            Err(error) => {
                let _ = capture_stop.send(None);
                let _ = capture_task.await;
                return Err(self
                    .session_run_error(&slot, expected_run_id, active_run.start_seq, error, true)
                    .await);
            }
        };
        let started_id = started.id;
        let terminal = match self
            .wait_trigger_terminal(
                &slot,
                expected_run_id,
                run_use.run_token,
                operation_id,
                started,
            )
            .await
        {
            Ok(trigger) => trigger,
            Err(error) => {
                let cancel = self
                    .session
                    .trigger_cancel(
                        run_use.slot_id.clone(),
                        slot.daemon_epoch,
                        slot.generation,
                        started_id,
                        expected_run_id,
                        run_use.run_token,
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
            .run_ownership_retained(slot.config.id.clone(), expected_run_id, run_use.run_token)
            .await
            .unwrap_or(false);

        let trigger_events: Vec<_> = capture
            .events
            .iter()
            .filter(|event| {
                trigger_evidence_contains(event.seq, terminal.start_seq, terminal_end_seq)
            })
            .cloned()
            .collect();
        let rendered = render_events(
            &trigger_events,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                echo: None,
                collapse_repeats: true,
                include_events: false,
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
        let outcome = trigger_status_label(terminal.status);
        let send_budget_exhausted = trigger_send_budget_exhausted(&terminal);
        let capture_complete = !capture.truncated
            && capture.gaps.is_empty()
            && matches!(&capture.completion, Completion::Signal(_))
            && observed_through_seq.is_some_and(|through| through >= terminal_end_seq);
        let gap = !capture.gaps.is_empty();
        let truncated = capture.truncated || rendered.text_truncated;
        let confidence = if gap {
            "unreliable"
        } else if truncated || !capture_complete {
            "partial"
        } else {
            "high"
        };
        let takeover_diagnosis = if matches!(
            terminal.status,
            TriggerStatus::ControlLost | TriggerStatus::RunLost
        ) {
            self.diagnose_run_abort(&slot, expected_run_id, active_run.start_seq)
                .await
        } else {
            None
        };
        let mut output = json!({
            "slot_id": slot.config.id,
            "run_handle": args.run_handle,
            "run_open": run_ownership_retained,
            "outcome": outcome,
            "matched": terminal.status.is_matched(),
            "fires": terminal.fires_confirmed,
            "fire_budget": terminal.spec.max_fires,
            "send_budget_exhausted": send_budget_exhausted,
            "confidence": confidence,
            "text": rendered.text,
            "truncated": truncated,
            "gap": gap,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": last_seq}
        });
        if let Some(matched_pattern) = matched_pattern {
            output["matched_pattern"] = json!(matched_pattern);
        }
        let confirmed_human_takeover = takeover_diagnosis.as_ref().is_some_and(|diagnosis| {
            attach_trigger_takeover_diagnosis(&mut output, expected_run_id, diagnosis)
        });
        let mut warnings = Vec::new();
        if gap {
            warnings.push("RX gap; Trigger evidence is unreliable".to_string());
        }
        if capture.truncated {
            warnings.push("Trigger capture hit its hard limit".to_string());
        }
        if !capture_complete {
            warnings
                .push("Trigger terminal sequence was not fully observed by capture".to_string());
        }
        if !run_ownership_retained {
            warnings.push("MCP no longer owns the Run; start a new Run before writing".to_string());
        }
        if confirmed_human_takeover {
            warnings.push(
                "Human takeover aborted the Run after seriald accepted the Trigger Job; kickoff or action bytes may already have reached the physical DUT (no_bytes_written=false)"
                    .to_string(),
            );
        }
        if terminal.status.is_matched()
            && send_budget_exhausted
            && !terminal.spec.stop_contains.is_empty()
        {
            warnings.push(
                "The stop matcher remained armed after the final permitted action write; no extra action writes were scheduled"
                    .to_string(),
            );
        }
        if !terminal.status.is_matched() {
            warnings.push(trigger_guidance(&terminal).to_string());
        }
        if !warnings.is_empty() {
            output["warnings"] = json!(warnings);
        }
        attach_omission(&mut output, &rendered);
        Ok(output)
    }

    async fn wait_trigger_terminal(
        &self,
        slot: &SlotSnapshot,
        expected_run_id: Uuid,
        run_token: Uuid,
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
                run_token,
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
        let _write_guard = self.write_guard(&args.slot_id).await;
        let slot = self.slot_online(&args.slot_id).await?;
        if let Some(run) = slot.active_run {
            bail!("Slot already has active Run {} ({})", run.id, run.label);
        }
        let started = self
            .session
            .start_run_with_handle(
                args.slot_id.clone(),
                args.label,
                BTreeMap::new(),
                Duration::from_secs(15),
            )
            .await?;
        let run = started.run;
        self.remember_live_cursor(
            &slot.config.id,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: run.start_seq,
            },
        );
        Ok(json!({
            "slot_id": args.slot_id,
            "run_id": run.id,
            "run_handle": started.run_handle,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": run.start_seq},
            "cleanup_required": "Call run_end before the final reply unless deliberately handing this live Run to a continuing agent workflow."
        }))
    }

    async fn run_end(&self, args: RunEndArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.slot_id).await;
        let slot = self.slot(&run_use.slot_id).await?;
        matching_active_run(&slot, run_use.run_id, "run_end")?;
        let ended = self
            .session
            .end_run(run_use.slot_id.clone(), run_use.run_id, run_use.run_token)
            .await?;
        Ok(json!({
            "slot_id": run_use.slot_id,
            "run_id": ended.id,
            "run_handle": args.run_handle,
            "run_open": false,
            "control_release": "best_effort"
        }))
    }

    async fn release(&self, args: ReleaseArgs) -> Result<Value> {
        let (slot_id, run_use) = match args.run_handle.as_ref() {
            Some(handle) => {
                if !args.abort_run {
                    bail!(
                        "run_handle is valid for release only with abort_run=true; use run_end for normal completion"
                    );
                }
                if args.slot_id.is_some() {
                    bail!(
                        "aborting release needs only run_handle and abort_run=true; omit slot_id"
                    );
                }
                let authorized = self.session.authorize_run_use(handle.clone()).await?;
                (authorized.slot_id.clone(), Some(authorized))
            }
            None => {
                if args.abort_run {
                    bail!("abort_run=true requires run_handle from run_start");
                }
                let slot_id = args
                    .slot_id
                    .context("release requires slot_id, or run_handle with abort_run=true")?;
                (slot_id, None)
            }
        };
        let run_capability = run_use.as_ref().map(|run| (run.run_id, run.run_token));
        let _write_guard = self.write_guard(&slot_id).await;
        let local = self.session.local_control_state(slot_id.clone()).await?;
        if !local.has_lease {
            // Public status may show a foreign Run, but release controls only
            // this MCP connection. Avoid consulting or modifying that Run;
            // the local no-lease release also discards any stale owned_run.
            let had_lease = self
                .session
                .release(slot_id.clone(), false, None, true)
                .await?;
            return Ok(release_output(slot_id, had_lease));
        }

        let current = self.slot(&slot_id).await?;
        let decision = plan_release(
            local,
            current.active_run.as_ref().map(|run| run.id),
            args.abort_run,
            run_capability,
        )?;
        let ReleaseDecision::Release {
            authorize,
            allow_stale_cleanup,
        } = decision
        else {
            unreachable!("a checked local lease cannot produce AlreadyReleased")
        };
        let had_lease = self
            .session
            .release(
                slot_id.clone(),
                args.abort_run,
                authorize,
                allow_stale_cleanup,
            )
            .await?;
        Ok(release_output(slot_id, had_lease))
    }

    async fn slot(&self, slot_id: &str) -> Result<SlotSnapshot> {
        self.status()
            .await?
            .slots
            .into_iter()
            .find(|slot| slot.config.id == slot_id)
            .with_context(|| format!("unknown Slot {slot_id:?}"))
    }

    async fn status(&self) -> Result<StatusResponse> {
        let status = self.api.status().await?;
        ensure_protocol_compatible(&status)?;
        Ok(status)
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

    async fn slot_online_for_physical_action(&self, slot_id: &str) -> Result<SlotSnapshot> {
        let status = self.status().await?;
        ensure_serial_context_precondition_supported(&status)?;
        let slot = status
            .slots
            .into_iter()
            .find(|slot| slot.config.id == slot_id)
            .with_context(|| format!("unknown Slot {slot_id:?}"))?;
        if slot.session_state != SessionState::Online {
            bail!(
                "Slot {slot_id:?} is {:?}: {}",
                slot.session_state,
                slot.state_reason.as_deref().unwrap_or("no reason reported")
            );
        }
        Ok(slot)
    }

    async fn diagnose_run_abort(
        &self,
        slot: &SlotSnapshot,
        run_id: Uuid,
        start_seq: u64,
    ) -> Option<RunAbortDiagnosis> {
        let response = self
            .api
            .events(
                &slot.config.id,
                &EventQuery {
                    epoch: Some(slot.daemon_epoch),
                    after_seq: Some(start_seq.saturating_sub(1)),
                    through_seq: None,
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: None,
                    kind: Some(EventKind::RunAborted),
                    actor_id: None,
                    run_id: Some(run_id),
                    operation_id: None,
                    contains: None,
                    regex: None,
                    limit_events: Some(8),
                    limit_bytes: Some(64 * 1024),
                },
            )
            .await
            .ok()?;
        let aborted =
            response.events.iter().rev().find(|event| {
                event.kind == EventKind::RunAborted && event.run_id == Some(run_id)
            })?;
        let abort_seq = aborted.seq;
        let reason = aborted
            .metadata
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("unspecified")
            .to_string();
        let taken_over_by = if reason == "human takeover" {
            self.api
                .events(
                    &slot.config.id,
                    &EventQuery {
                        epoch: Some(slot.daemon_epoch),
                        after_seq: Some(abort_seq),
                        // Human takeover emits RunAborted, ControlRevoked, then
                        // ControlGranted in one serialized Slot transition.
                        through_seq: Some(abort_seq.saturating_add(4)),
                        before_wall_time_ns: None,
                        after_wall_time_ns: None,
                        direction: None,
                        kind: Some(EventKind::ControlRevoked),
                        actor_id: None,
                        run_id: None,
                        operation_id: None,
                        contains: None,
                        regex: None,
                        limit_events: Some(4),
                        limit_bytes: Some(32 * 1024),
                    },
                )
                .await
                .ok()
                .and_then(|response| {
                    response.events.into_iter().find_map(|event| {
                        event.actor.filter(|actor| actor.kind == ActorKind::Human)
                    })
                })
        } else {
            None
        };
        Some(RunAbortDiagnosis {
            reason,
            taken_over_by,
        })
    }

    async fn run_abort_error(
        &self,
        slot: &SlotSnapshot,
        run_id: Uuid,
        start_seq: u64,
        observed_reason: &str,
        no_bytes_written: bool,
    ) -> anyhow::Error {
        let diagnosis = self
            .diagnose_run_abort(slot, run_id, start_seq)
            .await
            .unwrap_or_else(|| RunAbortDiagnosis {
                reason: observed_reason.to_string(),
                taken_over_by: None,
            });
        anyhow!(format_run_abort_error(
            &slot.config.id,
            run_id,
            &diagnosis,
            no_bytes_written,
        ))
    }

    async fn session_run_error(
        &self,
        slot: &SlotSnapshot,
        run_id: Uuid,
        start_seq: u64,
        error: anyhow::Error,
        no_bytes_written: bool,
    ) -> anyhow::Error {
        if !error_indicates_run_or_control_loss(&error) {
            return error;
        }
        if let Some(diagnosis) = self.diagnose_run_abort(slot, run_id, start_seq).await {
            return anyhow!(format_run_abort_error(
                &slot.config.id,
                run_id,
                &diagnosis,
                no_bytes_written,
            ));
        }
        anyhow!(
            "human_takeover_or_control_revoked: Slot {:?} Run {} lost fenced serial control; \
             taken_over_by=unknown; run_id={}; no_bytes_written={}; start a new Run only after \
             the current owner releases control and the DUT model/state is reconfirmed: {}",
            slot.config.id,
            run_id,
            run_id,
            no_bytes_written,
            error
        )
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

    async fn write_guard(&self, slot_id: &str) -> OwnedMutexGuard<()> {
        let lock = self
            .write_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(slot_id.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        lock.lock_owned().await
    }
}

fn summarize_recent_context(
    activity: EventQueryResponse,
    own_actor_id: Option<&str>,
    previous: &Cursor,
    current: &Cursor,
) -> Option<Value> {
    let activity_truncated = activity.truncated || !activity.gaps.is_empty();
    let events = activity
        .events
        .into_iter()
        // Actor labels are intentionally not identities: two MCP processes
        // commonly use the same label. Only the server-issued actor ID for
        // this exact WebSocket can identify our own writes.
        .filter(|event| {
            event
                .actor
                .as_ref()
                .is_none_or(|actor| Some(actor.id.as_str()) != own_actor_id)
        })
        .map(|event| {
            let actor = event.actor.map(|actor| {
                json!({
                    "kind": actor.kind,
                    "label": actor.label,
                })
            });
            let mut summary = json!({
                "seq": event.seq,
                "kind": event.kind,
                "actor": actor,
            });
            if event.direction == Direction::Tx {
                summary["tx_bytes"] = json!(event.data.len());
                if let Some(description) = event
                    .metadata
                    .get("command_description")
                    .and_then(Value::as_str)
                {
                    summary["description"] = json!(description);
                }
                if let Some(description) = event
                    .metadata
                    .get("command_sequence_description")
                    .and_then(Value::as_str)
                {
                    summary["sequence_description"] = json!(description);
                }
            }
            if let Some(reason) = event.metadata.get("reason").and_then(Value::as_str) {
                summary["reason"] = json!(reason);
            }
            summary
        })
        .collect::<Vec<_>>();
    if events.is_empty() && !activity_truncated {
        return None;
    }
    Some(json!({
        "interference": !events.is_empty(),
        "complete": !activity_truncated,
        "after_seq": previous.after_seq,
        "through_seq": current.after_seq,
        "events": events,
        "truncated": activity_truncated,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunAbortDiagnosis {
    reason: String,
    taken_over_by: Option<Actor>,
}

fn format_run_abort_error(
    slot_id: &str,
    run_id: Uuid,
    diagnosis: &RunAbortDiagnosis,
    no_bytes_written: bool,
) -> String {
    let taken_over_by = diagnosis
        .taken_over_by
        .as_ref()
        .map(|actor| format!("{} ({})", actor.label, actor.id))
        .unwrap_or_else(|| "unknown".into());
    let code = if diagnosis.reason == "human takeover" {
        "human_takeover"
    } else {
        "run_aborted"
    };
    format!(
        "{code}: Slot {slot_id:?} Run {run_id} was aborted; reason={:?}; \
         taken_over_by={taken_over_by:?}; run_id={run_id}; \
         no_bytes_written={no_bytes_written}; start a new Run only after the current owner \
         releases control and the DUT model/state is reconfirmed",
        diagnosis.reason,
    )
}

/// Add a machine-readable diagnosis to an already accepted Trigger Job.
///
/// Unlike a rejected write request, a Trigger can have emitted its kickoff or
/// one or more actions before ownership loss became terminal. Consequently the
/// stable diagnostic must never claim that zero physical bytes were written.
fn attach_trigger_takeover_diagnosis(
    output: &mut Value,
    run_id: Uuid,
    diagnosis: &RunAbortDiagnosis,
) -> bool {
    if diagnosis.reason != "human takeover" {
        return false;
    }
    output["abort_diagnosis"] = json!({
        "code": "human_takeover",
        "reason": diagnosis.reason,
        "taken_over_by": diagnosis.taken_over_by.as_ref(),
        "run_id": run_id,
        "no_bytes_written": false,
    });
    true
}

fn error_indicates_run_or_control_loss(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    [
        "human_takeover_or_control_revoked",
        "ControlRequired",
        "StaleFence",
        "expected Run boundary is no longer valid",
        "does not own an active Run",
        "lost the control lease",
        "control renewal failed",
        "Run boundary is no longer valid",
        "can no longer be trusted",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorSource {
    Explicit,
    SessionLiveCursor,
    CurrentHead,
}

impl CursorSource {
    #[cfg(test)]
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

fn serial_context_precondition(slot: &SlotSnapshot) -> SequenceWritePrecondition {
    SequenceWritePrecondition {
        cursor: Cursor {
            epoch: slot.daemon_epoch,
            after_seq: slot.head_seq,
        },
        expected_generation: slot.generation,
        expected_tx_offset: slot.tx_offset,
    }
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

fn matching_active_run<'a>(
    slot: &'a SlotSnapshot,
    expected_run_id: Uuid,
    operation: &str,
) -> Result<&'a serial_protocol::RunInfo> {
    let active = slot
        .active_run
        .as_ref()
        .with_context(|| format!("no active Run; call run_start before {operation}"))?;
    if active.id != expected_run_id {
        bail!(
            "{operation} expected Run {expected_run_id}, but Slot has active Run {}; refusing to \
             adopt or modify another caller's Run",
            active.id
        );
    }
    Ok(active)
}

#[derive(Debug, PartialEq, Eq)]
enum ReleaseDecision {
    AlreadyReleased,
    Release {
        authorize: Option<(Uuid, Uuid)>,
        allow_stale_cleanup: bool,
    },
}

fn plan_release(
    local: LocalControlState,
    daemon_active_run_id: Option<Uuid>,
    abort_run: bool,
    run_capability: Option<(Uuid, Uuid)>,
) -> Result<ReleaseDecision> {
    if !local.has_lease {
        return Ok(ReleaseDecision::AlreadyReleased);
    }
    let Some(local_run_id) = local.owned_run_id else {
        return Ok(ReleaseDecision::Release {
            authorize: None,
            allow_stale_cleanup: false,
        });
    };
    let Some(active_run_id) = daemon_active_run_id else {
        return Ok(ReleaseDecision::Release {
            authorize: None,
            allow_stale_cleanup: true,
        });
    };
    if active_run_id != local_run_id {
        bail!(
            "local Run ownership changed: serial-mcp recorded Run {local_run_id}, but seriald \
             reports active Run {active_run_id}; refusing to release across that Run boundary"
        );
    }
    if !abort_run {
        bail!(
            "serial-mcp owns active Run {local_run_id}; use run_end, or set abort_run=true with \
             its run_handle"
        );
    }
    let capability = run_capability.context(
        "release would abort this MCP's active Run; pass run_handle from this caller's \
         run_start response",
    )?;
    if capability.0 != local_run_id {
        bail!(
            "release capability names Run {}, but serial-mcp owns Run {local_run_id}; refusing \
             to abort a different Run",
            capability.0
        );
    }
    Ok(ReleaseDecision::Release {
        authorize: Some(capability),
        allow_stale_cleanup: false,
    })
}

fn release_output(slot_id: String, had_lease: bool) -> Value {
    json!({
        "slot_id": slot_id,
        "released": had_lease,
        "already_released": !had_lease,
        "serial_port_closed": false
    })
}

fn attach_run_state(output: &mut Value, run_handle: &str, run_open: bool) {
    output["run_handle"] = json!(run_handle);
    output["run_open"] = json!(run_open);
}

fn validate_command_description(description: &str) -> Result<()> {
    if description.is_empty() {
        bail!("description must not be empty");
    }
    if description != description.trim() {
        bail!("description must be trimmed");
    }
    if description.len() > MAX_COMMAND_DESCRIPTION_BYTES {
        bail!("description must not exceed {MAX_COMMAND_DESCRIPTION_BYTES} UTF-8 bytes");
    }
    if description.chars().any(char::is_control) {
        bail!("description must not contain control characters");
    }
    Ok(())
}

fn validate_command_sequence_shape(steps: &[CommandSequenceStepArgs]) -> Result<()> {
    if steps.is_empty() || steps.len() > MAX_COMMAND_SEQUENCE_STEPS {
        bail!("steps must contain between 1 and {MAX_COMMAND_SEQUENCE_STEPS} commands");
    }

    let mut total_timeout_seconds = 0u64;
    for (index, step) in steps.iter().enumerate() {
        validate_command_description(&step.description)
            .with_context(|| format!("steps[{index}].description is invalid"))?;
        if step.command.len() > MAX_WRITE_BYTES {
            bail!("steps[{index}].command exceeds {MAX_WRITE_BYTES} UTF-8 bytes before adding EOL");
        }
        match (step.expect.as_deref(), step.regex.as_deref()) {
            (Some(_), Some(_)) => {
                bail!("steps[{index}].expect and steps[{index}].regex are alternatives; choose one")
            }
            (None, None) if index + 1 < steps.len() => {
                bail!("steps[{index}] is not final and must provide exactly one of expect or regex")
            }
            (Some(expect), None) => {
                if expect.is_empty() {
                    bail!("steps[{index}].expect must not be empty");
                }
                if expect.len() > MAX_REGEX_BYTES {
                    bail!("steps[{index}].expect must not exceed {MAX_REGEX_BYTES} UTF-8 bytes");
                }
            }
            (None, Some(pattern)) => {
                let compiled = compile_regex(pattern, &format!("steps[{index}].regex"))?;
                if compiled.is_match("") {
                    bail!("steps[{index}].regex must not match an empty serial stream");
                }
            }
            (None, None) => {}
        }

        let timeout_seconds = step.timeout_seconds.unwrap_or(10);
        if !(1..=120).contains(&timeout_seconds) {
            bail!("steps[{index}].timeout_seconds must be between 1 and 120");
        }
        total_timeout_seconds = total_timeout_seconds
            .checked_add(timeout_seconds)
            .context("command_sequence timeout total overflowed")?;
    }
    if total_timeout_seconds > MAX_COMMAND_SEQUENCE_TIMEOUT_SECONDS {
        bail!(
            "steps request {total_timeout_seconds}s of capture time; command_sequence allows at most {MAX_COMMAND_SEQUENCE_TIMEOUT_SECONDS}s"
        );
    }
    Ok(())
}

fn prepare_command_step(
    command: &str,
    description: String,
    expect: Option<&str>,
    regex: Option<&str>,
    timeout: Duration,
    slot: &SlotSnapshot,
) -> Result<PreparedCommandStep> {
    validate_command_description(&description)?;
    let bytes = compose_write_bytes(command, effective_write_eol(slot))?;
    let (patterns, until_regex, completion_mode) = requested_completion(expect, regex, slot, true)?;
    // An explicit matcher is the sole authoritative boundary. A configured
    // prompt or a brief quiet period must never pre-empt it.
    let complete_on_quiet = completion_mode == "quiet";
    let expected_echo = (matches!(effective_echo_mode(slot), EchoMode::On) && !command.is_empty())
        .then(|| bytes.clone());
    Ok(PreparedCommandStep {
        bytes,
        description,
        timeout,
        patterns,
        until_regex,
        complete_on_quiet,
        expected_echo,
    })
}

fn prepare_command_sequence_steps(
    steps: Vec<CommandSequenceStepArgs>,
    slot: &SlotSnapshot,
) -> Result<Vec<PreparedCommandStep>> {
    validate_command_sequence_shape(&steps)?;
    let mut total_write_bytes = 0usize;
    let mut prepared = Vec::with_capacity(steps.len());
    for (index, step) in steps.into_iter().enumerate() {
        let timeout_seconds = step.timeout_seconds.unwrap_or(10);
        let command = prepare_command_step(
            &step.command,
            step.description,
            step.expect.as_deref(),
            step.regex.as_deref(),
            Duration::from_secs(timeout_seconds),
            slot,
        )
        .with_context(|| format!("steps[{index}] is invalid"))?;
        total_write_bytes = total_write_bytes
            .checked_add(command.bytes.len())
            .context("command_sequence write byte total overflowed")?;
        prepared.push(command);
    }
    if total_write_bytes > MAX_COMMAND_SEQUENCE_TOTAL_WRITE_BYTES {
        bail!(
            "steps plan {total_write_bytes} physical bytes; command_sequence allows at most {MAX_COMMAND_SEQUENCE_TOTAL_WRITE_BYTES}"
        );
    }
    Ok(prepared)
}

fn command_sequence_stop(
    executed: &ExecutedCommandStep,
    requires_next_step: bool,
) -> Option<SequenceStop> {
    let stop = |code, message: &str| {
        Some(SequenceStop {
            code,
            message: message.into(),
        })
    };
    match &executed.completion {
        Completion::RunAborted { .. } => {
            return stop("run_aborted", "the active Run was aborted during this step");
        }
        Completion::Timeout => {
            return stop(
                "timeout",
                "the requested completion boundary was not observed before timeout",
            );
        }
        Completion::Disconnected(reason) => {
            return Some(SequenceStop {
                code: "disconnected",
                message: format!("capture disconnected before completion: {reason}"),
            });
        }
        _ => {}
    }
    if executed.gap {
        return stop("rx_gap", "RX evidence has a gap");
    }
    if executed.truncated {
        return stop("capture_truncated", "capture evidence was truncated");
    }
    if executed.interfered {
        return stop("interfered", "another actor wrote during this step");
    }
    if executed.echo_missing {
        return stop(
            "echo_uncertain",
            "the configured command echo was not observed completely",
        );
    }
    if executed.no_rx {
        return stop("no_rx", "no post-write RX was observed");
    }
    if requires_next_step
        && !matches!(
            executed.completion,
            Completion::Pattern(_) | Completion::Regex(_)
        )
    {
        return stop(
            "boundary_not_matched",
            "the explicit intermediate expect/regex boundary was not matched",
        );
    }
    None
}

fn sequence_stop_forces_closed(stop: &SequenceStop) -> bool {
    stop.code == "run_aborted"
}

fn command_has_no_rx(rx_event_count: usize, echo_observed: bool) -> bool {
    rx_event_count == 0 && !echo_observed
}

#[allow(clippy::too_many_arguments)]
fn command_sequence_output(
    slot: &SlotSnapshot,
    run_id: Uuid,
    sequence_id: Uuid,
    description: String,
    requested_steps: usize,
    completed_steps: usize,
    steps: Vec<Value>,
    failure: Option<Value>,
) -> Value {
    let cursor = steps
        .last()
        .and_then(|step| step.get("cursor"))
        .cloned()
        .unwrap_or_else(|| json!({"epoch": slot.daemon_epoch, "after_seq": slot.head_seq}));
    let sent_steps = steps.len();
    let mut output = json!({
        "slot_id": slot.config.id,
        "run_id": run_id,
        "sequence_id": sequence_id,
        "description": description,
        "status": if failure.is_some() { "partial" } else { "completed" },
        "execution": "unknown",
        "requested_steps": requested_steps,
        "sent_steps": sent_steps,
        "completed_steps": completed_steps,
        "steps": steps,
        "cursor": cursor,
    });
    if let Some(failure) = failure {
        output["failure"] = failure;
    }
    output
}

fn validate_monitor_matcher(contains: Option<&str>, regex: Option<&str>) -> Result<()> {
    match (contains, regex) {
        (Some(contains), None) => {
            if contains.is_empty() {
                bail!("contains must not be empty");
            }
            if contains.len() > MAX_REGEX_BYTES {
                bail!("contains must not exceed {MAX_REGEX_BYTES} UTF-8 bytes");
            }
            Ok(())
        }
        (None, Some(regex)) => {
            let compiled = compile_regex(regex, "regex")?;
            if compiled.is_match("") {
                bail!("regex must not match an empty serial stream");
            }
            Ok(())
        }
        (None, None) => bail!("provide exactly one of contains or regex"),
        (Some(_), Some(_)) => bail!("contains and regex are alternatives; choose exactly one"),
    }
}

fn create_monitor_request(args: MonitorStartArgs) -> Result<CreateMonitorRequest> {
    validate_monitor_matcher(args.contains.as_deref(), args.regex.as_deref())?;
    if let Some(description) = args.description.as_deref() {
        if description.is_empty() {
            bail!("description must not be empty when provided");
        }
        if description.len() > MAX_MONITOR_DESCRIPTION_BYTES {
            bail!("description must not exceed {MAX_MONITOR_DESCRIPTION_BYTES} UTF-8 bytes");
        }
    }
    serde_json::from_value(json!({
        "request_id": args.idempotency_key.unwrap_or_else(Uuid::new_v4),
        "spec": {
            "slot_id": args.slot_id,
            "contains": args.contains,
            "regex": args.regex,
            "description": args.description,
        }
    }))
    .context("failed to construct Monitor request")
}

fn parse_monitor_cursor(value: &str) -> Result<u64> {
    value.parse::<u64>().with_context(|| {
        format!("after must be the decimal cursor returned by seriald, got {value:?}")
    })
}

fn monitor_from_response(response: impl serde::Serialize) -> Result<Value> {
    let response =
        serde_json::to_value(response).context("seriald returned an invalid Monitor response")?;
    response
        .get("monitor")
        .cloned()
        .context("seriald Monitor response omitted monitor")
}

fn compact_monitor(monitor: &Value) -> Value {
    let spec = monitor.get("spec").unwrap_or(&Value::Null);
    let matcher = if let Some(value) = spec.get("contains").and_then(Value::as_str) {
        json!({"kind": "literal", "value": value})
    } else if let Some(value) = spec.get("regex").and_then(Value::as_str) {
        json!({"kind": "regex", "value": value})
    } else {
        Value::Null
    };
    json!({
        "monitor_id": monitor.get("id").cloned().unwrap_or(Value::Null),
        "slot_id": spec.get("slot_id").cloned().unwrap_or(Value::Null),
        "status": monitor.get("status").cloned().unwrap_or(Value::Null),
        "severity": spec.get("severity").cloned().unwrap_or(Value::Null),
        "description": spec.get("description").cloned().unwrap_or(Value::Null),
        "matcher": matcher,
        "current_cursor": monitor.get("current_cursor").cloned().unwrap_or(Value::Null),
        "incident_count": monitor.get("incident_count").cloned().unwrap_or(Value::Null),
        "unacked_incident_count": monitor.get("unacked_incident_count").cloned().unwrap_or(Value::Null),
        "gap_count": monitor.get("gap_count").cloned().unwrap_or(Value::Null),
        "expires_wall_time_ns": monitor.get("expires_wall_time_ns").cloned().unwrap_or(Value::Null),
        "last_error": monitor.get("last_error").cloned().unwrap_or(Value::Null)
    })
}

fn compact_monitor_incident(incident: &Value) -> Value {
    json!({
        "incident_id": incident.get("id").cloned().unwrap_or(Value::Null),
        "incident_seq": incident.get("incident_seq").cloned().unwrap_or(Value::Null),
        "slot_id": incident.get("slot_id").cloned().unwrap_or(Value::Null),
        "severity": incident.get("severity").cloned().unwrap_or(Value::Null),
        "description": incident.get("description").cloned().unwrap_or(Value::Null),
        "preview": incident.get("preview").cloned().unwrap_or(Value::Null),
        "serial_range": {
            "epoch": incident.get("daemon_epoch").cloned().unwrap_or(Value::Null),
            "seq_start": incident.get("seq_start").cloned().unwrap_or(Value::Null),
            "seq_end": incident.get("seq_end").cloned().unwrap_or(Value::Null)
        },
        "evidence_ref": incident.get("evidence_ref").cloned().unwrap_or(Value::Null),
        "evidence_cursor": incident.get("evidence_cursor").cloned().unwrap_or(Value::Null),
        "wall_time_start_ns": incident.get("wall_time_start_ns").cloned().unwrap_or(Value::Null),
        "wall_time_end_ns": incident.get("wall_time_end_ns").cloned().unwrap_or(Value::Null),
        "created_wall_time_ns": incident.get("created_wall_time_ns").cloned().unwrap_or(Value::Null),
        "acked": incident.get("acked_wall_time_ns").is_some_and(|value| !value.is_null())
    })
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

fn render_response(
    slot: &SlotSnapshot,
    query_epoch: Uuid,
    response: serial_protocol::EventQueryResponse,
    options: RenderOptions,
    scope: &str,
) -> Value {
    let rendered = render_events(&response.events, options);
    let after_seq = response
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.after_seq)
        .or_else(|| response.events.last().map(|event| event.seq))
        .unwrap_or(slot.head_seq);
    let epoch = response
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.epoch)
        .unwrap_or(query_epoch);
    let gap = !response.gaps.is_empty();
    let truncated = response.truncated || rendered.text_truncated;
    let mut output = json!({
        "slot_id": slot.config.id,
        "scope": scope,
        "confidence": if gap { "unreliable" } else if truncated { "partial" } else { "high" },
        "text": rendered.text,
        "truncated": truncated,
        "gap": gap,
        "cursor": {"epoch": epoch, "after_seq": after_seq}
    });
    if let Some(ref excerpt) = rendered.match_excerpt {
        output["matches"] = json!(excerpt.matched_lines);
    }
    let mut warnings = Vec::new();
    if gap {
        warnings.push(
            if matches!(scope, "tail" | "continue") {
                "live replay gap; returned text is incomplete"
            } else {
                "journal gap; returned text is incomplete"
            }
            .to_string(),
        );
    }
    if response.truncated {
        warnings.push("event page hit its hard limit; continue from cursor".to_string());
    }
    if !warnings.is_empty() {
        output["warnings"] = json!(warnings);
    }
    if gap {
        output["gaps"] = json!(response.gaps);
    }
    attach_omission(&mut output, &rendered);
    output
}

fn completion_kind(completion: &Completion) -> &'static str {
    match completion {
        Completion::Pattern(_) => "literal",
        Completion::Prompt(_) => "prompt",
        Completion::Regex(_) => "regex",
        Completion::Quiet => "quiet",
        Completion::Signal(_) => "signal",
        Completion::RunAborted { .. } => "run_aborted",
        Completion::Timeout => "timeout",
        Completion::Disconnected(_) => "disconnected",
    }
}

fn command_confidence(
    completion: &Completion,
    output_truncated: bool,
    has_gap: bool,
    interfered: bool,
    echo_missing: bool,
    rx_event_count: usize,
) -> &'static str {
    if has_gap
        || matches!(
            completion,
            Completion::Disconnected(_) | Completion::RunAborted { .. }
        )
    {
        "unreliable"
    } else if output_truncated {
        "partial"
    } else if interfered {
        "interfered"
    } else if matches!(completion, Completion::Timeout) {
        "incomplete"
    } else if matches!(completion, Completion::Quiet) {
        "low"
    } else if echo_missing {
        "medium"
    } else if rx_event_count == 0 {
        "low"
    } else {
        "high"
    }
}

fn capture_confidence(
    completion: &Completion,
    output_truncated: bool,
    has_gap: bool,
) -> &'static str {
    if has_gap
        || matches!(
            completion,
            Completion::Disconnected(_) | Completion::RunAborted { .. }
        )
    {
        "unreliable"
    } else if output_truncated {
        "partial"
    } else if matches!(completion, Completion::Timeout) {
        "incomplete"
    } else if matches!(completion, Completion::Quiet) {
        "low"
    } else {
        "high"
    }
}

#[allow(clippy::too_many_arguments)]
fn attach_capture_warnings(
    output: &mut Value,
    completion: &Completion,
    capture_truncated: bool,
    text_truncated: bool,
    gap: bool,
    interfered: bool,
    echo_missing: bool,
    no_rx: bool,
) {
    let mut warnings: Vec<String> = Vec::new();
    if gap {
        warnings.push("RX gap; evidence is incomplete".into());
    }
    if capture_truncated {
        warnings.push("capture hit its hard limit".into());
    }
    if text_truncated {
        warnings.push("text was summarized".into());
    }
    if interfered {
        warnings.push("another actor wrote during capture".into());
    }
    if echo_missing {
        warnings.push("configured echo missing; target delivery may be incomplete".into());
    }
    if no_rx {
        warnings.push("no post-boundary RX observed".into());
    }
    match completion {
        Completion::Quiet => warnings.push("quiet is not proof of command completion".into()),
        Completion::Timeout => {
            warnings.push("completion boundary not observed before timeout".into())
        }
        Completion::Disconnected(_) => {
            warnings.push("capture disconnected before completion".into())
        }
        Completion::RunAborted { reason, .. } => {
            warnings.push(format!("active Run was aborted: {reason}"))
        }
        _ => {}
    }
    if !warnings.is_empty() {
        output["warnings"] = json!(warnings);
    }
}

fn attach_omission(output: &mut Value, rendered: &crate::render::RenderedEvents) {
    if rendered.summary.omitted_chars > 0 || rendered.summary.omitted_lines > 0 {
        output["omitted"] = json!({
            "chars": rendered.summary.omitted_chars,
            "lines": rendered.summary.omitted_lines
        });
    }
}

fn attach_search_continuation_guidance(output: &mut Value, scope: &str, run_id: Option<Uuid>) {
    let mut continuation = json!({
        "scope": scope,
        "epoch": output["cursor"]["epoch"].clone(),
        "after_seq": output["cursor"]["after_seq"].clone(),
    });
    if let Some(run_id) = run_id {
        continuation["run_id"] = json!(run_id);
    }
    output["continuation"] = continuation;
    output["guidance"] = json!(if scope == "current_run" {
        "Search is incomplete; repeat the same query with continuation and unchanged run_id."
    } else {
        "Search is incomplete; repeat the same query with continuation."
    });
}

/// Assemble the bytes for one write. An empty command is valid as long as the
/// effective EOL contributes bytes, which sends a bare Enter; only a fully
/// empty payload is rejected.
fn compose_write_bytes(command: &str, default_eol: &str) -> Result<Vec<u8>> {
    if command.is_empty() && default_eol.is_empty() {
        bail!("command and EOL are both empty; nothing would be sent");
    }
    let mut bytes = command.as_bytes().to_vec();
    bytes.extend_from_slice(default_eol.as_bytes());
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

fn effective_write_pacing(slot: &SlotSnapshot) -> WritePacing {
    slot.effective_write_pacing
        .unwrap_or_else(|| WritePacing::resolve(None, &slot.config.settings))
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

fn requested_completion(
    expect: Option<&str>,
    regex: Option<&str>,
    slot: &SlotSnapshot,
    use_profile_prompts: bool,
) -> Result<(Vec<CompletionPattern>, Option<regex::Regex>, String)> {
    if expect.is_some() && regex.is_some() {
        bail!("expect and regex are alternative completion boundaries; choose one");
    }
    if let Some(regex) = regex {
        return Ok((
            Vec::new(),
            Some(compile_regex(regex, "regex")?),
            "regex".into(),
        ));
    }
    if let Some(expect) = expect {
        if expect.is_empty() {
            bail!("expect must not be empty");
        }
        return Ok((
            vec![CompletionPattern::Literal(expect.to_string())],
            None,
            "expect".into(),
        ));
    }

    let (shell_prompt, uboot_prompt) = effective_prompts(slot);
    let patterns: Vec<_> = if use_profile_prompts {
        [shell_prompt, uboot_prompt]
            .into_iter()
            .flatten()
            .map(CompletionPattern::Prompt)
            .collect()
    } else {
        Vec::new()
    };
    let mode = if patterns.is_empty() {
        "quiet"
    } else {
        "prompt"
    };
    Ok((patterns, None, mode.into()))
}

fn compile_regex(value: &str, field: &str) -> Result<regex::Regex> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    if value.len() > MAX_REGEX_BYTES {
        bail!("{field} must not exceed {MAX_REGEX_BYTES} UTF-8 bytes");
    }
    regex::Regex::new(value).with_context(|| format!("{field} is not a valid regex"))
}

fn trigger_spec(args: &TriggerArgs) -> Result<TriggerSpec> {
    let initial_write = args
        .initial_write
        .as_ref()
        .map(|write| trigger_write_bytes(write, "kickoff", MAX_TRIGGER_INITIAL_WRITE_BYTES))
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
            "kickoff plus action * max_fires plans {planned_bytes} bytes; the Trigger limit \
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
        // The Agent-facing Trigger intentionally cannot override physical
        // write pacing. `None` tells seriald to apply the Slot transport
        // settings to kickoff and action writes.
        pacing: None,
    })
}

fn trigger_write_bytes(write: &TriggerWriteArgs, field: &str, max_bytes: usize) -> Result<Vec<u8>> {
    let mut bytes = write.text.as_bytes().to_vec();
    bytes.extend_from_slice(write.eol.as_deref().unwrap_or("").as_bytes());
    if bytes.is_empty() {
        bail!("{field} text and EOL are both empty; omit kickoff or provide bytes");
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

fn trigger_send_budget_exhausted(trigger: &TriggerInfo) -> bool {
    trigger.fires_confirmed >= trigger.spec.max_fires
}

fn trigger_guidance(trigger: &TriggerInfo) -> &'static str {
    match trigger.status {
        TriggerStatus::Matched => {
            "A caller-supplied stop literal was observed in live RX. This confirms only the \
             Trigger boundary, not that a later flashing or debug workflow succeeded."
        }
        TriggerStatus::TimedOut => {
            "The observation deadline elapsed without a stop match. Confirmed TX proves only \
             that bytes were accepted by the serial driver, not that the target action failed. \
             Inspect this capture/current Run before changing parameters or retrying."
        }
        TriggerStatus::MaxFiresReached if !trigger.spec.stop_contains.is_empty() => {
            "The action send budget was exhausted, then seriald kept observing until the \
             original deadline without a stop match. Confirmed TX is not proof that the target \
             action failed; inspect TX/RX and current device state before any retry."
        }
        TriggerStatus::MaxFiresReached => {
            "No stop literal was configured, so exhausting the action send budget completed \
             this Trigger. That proves neither target success nor target failure; inspect the \
             resulting state instead of blindly retrying."
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

fn control_signal_byte(signal: &str) -> Option<u8> {
    match signal {
        "ctrl_c" => Some(0x03),
        "ctrl_d" => Some(0x04),
        "ctrl_z" => Some(0x1a),
        _ => None,
    }
}

fn ensure_protocol_compatible(status: &StatusResponse) -> Result<()> {
    if status.protocol_version != PROTOCOL_VERSION {
        bail!(
            "seriald protocol version {} is incompatible with serial-mcp protocol version {}; \
             install seriald and serial-mcp from the same release",
            status.protocol_version,
            PROTOCOL_VERSION
        );
    }
    Ok(())
}

fn ensure_sequence_write_precondition_supported(status: &StatusResponse) -> Result<()> {
    if !status.sequence_write_precondition_supported {
        bail!(
            "seriald does not advertise atomic command_sequence write boundaries; no bytes were written. Install seriald and serial-mcp from the same v0.7.0-or-newer build"
        );
    }
    Ok(())
}

fn ensure_serial_context_precondition_supported(status: &StatusResponse) -> Result<()> {
    if !status.serial_context_precondition_supported {
        bail!(
            "seriald does not advertise atomic serial-context boundaries for Write, BREAK, and Trigger; no bytes were written. Install seriald and serial-mcp from the same v0.7.0-or-newer build"
        );
    }
    Ok(())
}

fn slot_summary(slot: &SlotSnapshot) -> Value {
    let effective_transport = slot.effective_transport.unwrap_or_else(|| {
        serial_protocol::resolve_transport_settings(&slot.config.settings, None)
    });
    let (shell_prompt, uboot_prompt) = effective_prompts(slot);
    let control = slot.control.as_ref().map(|lease| {
        json!({
            "owner": actor_summary(&lease.owner),
            "expires_wall_time_ns": lease.expires_wall_time_ns
        })
    });
    let active_run = slot.active_run.as_ref().map(|run| {
        json!({
            "id": run.id,
            "label": run.label,
            "status": run.status,
            "start_seq": run.start_seq,
            "owner": actor_summary(&run.owner)
        })
    });
    let active_trigger = slot.active_trigger.as_ref().map(|trigger| {
        json!({
            "id": trigger.id,
            "status": trigger.status,
            "start_seq": trigger.start_seq,
            "end_seq": trigger.end_seq,
            "last_write_seq": trigger.last_write_seq,
            "fires_confirmed": trigger.fires_confirmed,
            "tx_bytes_confirmed": trigger.tx_bytes_confirmed,
            "owner": actor_summary(&trigger.owner)
        })
    });
    json!({
        "slot_id": slot.config.id,
        "display_name": slot.config.display_name,
        "port": slot.config.port,
        "enabled": slot.config.enabled,
        "transport_profile": slot.config.profile,
        "device_profile": slot.config.device_profile,
        "endpoint_present": slot.endpoint_present,
        "session_state": slot.session_state,
        "state_code": slot.state_code,
        "state_reason": slot.state_reason,
        "target_activity": slot.target_activity,
        "effective_transport": effective_transport,
        "effective_device": {
            "write_eol": effective_write_eol(slot),
            "echo": effective_echo_mode(slot),
            "shell_prompt": shell_prompt,
            "uboot_prompt": uboot_prompt,
            "write_pacing": effective_write_pacing(slot)
        },
        "cursor": {"epoch": slot.daemon_epoch, "after_seq": slot.head_seq},
        "generation": slot.generation,
        "control": control,
        "active_run": active_run,
        "active_trigger": active_trigger,
        "logging": slot.logging,
        "rx_overflow_bytes": slot.rx_overflow_bytes,
    })
}

fn slot_device_model_summary(slot_id: &str, catalog: &DeviceModelListResponse) -> Value {
    let Some(binding) = catalog
        .bindings
        .iter()
        .find(|binding| binding.slot_id == slot_id)
    else {
        return Value::Null;
    };
    let model = catalog
        .models
        .iter()
        .find(|model| model.id == binding.model_id);
    json!({
        "id": binding.model_id.as_str(),
        "name": model.map(|model| model.name.as_str()),
        "parent_id": model.and_then(|model| model.parent_id.as_deref()),
        "aliases": model.map(|model| model.aliases.as_slice()).unwrap_or(&[]),
        "confirmation_method": binding.confirmation_method,
        "confirmation_note": binding.note.as_deref(),
        "confirmed_wall_time_ns": binding.updated_wall_time_ns,
        "source": binding.source.as_str(),
    })
}

fn actor_summary(actor: &serial_protocol::Actor) -> Value {
    json!({"id": actor.id, "label": actor.label, "kind": actor.kind})
}

fn build_device_model_set_request(
    args: &DeviceModelSetArgs,
    catalog: &DeviceModelListResponse,
) -> Result<SetSlotDeviceModelRequest> {
    if args.model_id.is_empty() {
        bail!("model_id must not be empty");
    }
    if args.create_if_missing && args.update_existing {
        bail!("create_if_missing and update_existing are mutually exclusive");
    }
    let observed_current = catalog
        .bindings
        .iter()
        .find(|binding| binding.slot_id == args.slot_id)
        .map(|binding| binding.model_id.clone());
    if args.create_if_missing {
        if args.name.is_none() {
            bail!("name is required when create_if_missing=true");
        }
        if args.clear_parent || args.clear_aliases {
            bail!("clear_parent and clear_aliases require update_existing=true");
        }
    } else if args.update_existing {
        if !catalog.models.iter().any(|model| model.id == args.model_id) {
            bail!("cannot update unknown device model {:?}", args.model_id);
        }
        if observed_current.as_deref() != Some(args.model_id.as_str()) {
            bail!(
                "update_existing may only modify the model currently bound to Slot {:?}; observed {:?}",
                args.slot_id,
                observed_current
            );
        }
        if args.parent.is_some() && args.clear_parent {
            bail!("parent and clear_parent are mutually exclusive");
        }
        if !args.aliases.is_empty() && args.clear_aliases {
            bail!("aliases and clear_aliases are mutually exclusive");
        }
        if args.name.is_none()
            && args.parent.is_none()
            && !args.clear_parent
            && args.aliases.is_empty()
            && !args.clear_aliases
        {
            bail!(
                "update_existing requires at least one of name, parent, clear_parent, aliases, or clear_aliases"
            );
        }
    } else if args.name.is_some()
        || args.parent.is_some()
        || args.clear_parent
        || !args.aliases.is_empty()
        || args.clear_aliases
    {
        bail!("model definition fields require create_if_missing=true or update_existing=true");
    } else if !catalog.models.iter().any(|model| model.id == args.model_id) {
        bail!(
            "unknown device model {:?}; call device_models or set create_if_missing=true",
            args.model_id
        );
    }

    if let Some(expected_current) = args.expected_current.as_ref()
        && expected_current.as_deref() != observed_current.as_deref()
    {
        bail!(
            "Slot {:?} model binding changed: expected {:?}, observed {:?}; inspect device_models \
             and re-confirm the physical DUT before retrying",
            args.slot_id,
            expected_current,
            observed_current,
        );
    }
    // Even when the caller omits expected_current, guard the PUT with the
    // binding observed in the immediately preceding catalog read. Together
    // with expected_revision this prevents an Agent from overwriting a Human
    // correction made concurrently.
    let expected_current = args
        .expected_current
        .clone()
        .unwrap_or_else(|| observed_current.clone());
    Ok(SetSlotDeviceModelRequest {
        model_id: Some(args.model_id.clone()),
        create_if_missing: args.create_if_missing,
        update_existing: args.update_existing,
        name: args.name.clone(),
        parent_id: args.parent.clone(),
        clear_parent: args.clear_parent,
        aliases: args.aliases.clone(),
        clear_aliases: args.clear_aliases,
        confirmation_method: Some(args.confirmation_method),
        note: args.note.clone(),
        source: MODEL_BINDING_SOURCE.into(),
        expected_revision: Some(catalog.config_revision),
        expected_current: Some(expected_current),
    })
}

fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> std::result::Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
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

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DevicesArgs {
    slot_id: Option<String>,
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DeviceModelsArgs {
    slot_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceModelSetArgs {
    slot_id: String,
    model_id: String,
    #[serde(default)]
    create_if_missing: bool,
    #[serde(default)]
    update_existing: bool,
    name: Option<String>,
    parent: Option<String>,
    #[serde(default)]
    clear_parent: bool,
    #[serde(default)]
    aliases: Vec<String>,
    #[serde(default)]
    clear_aliases: bool,
    confirmation_method: ModelConfirmationMethod,
    note: Option<String>,
    #[serde(default, deserialize_with = "deserialize_present_option")]
    expected_current: Option<Option<String>>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    slot_id: String,
    scope: Option<String>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    through_seq: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    slot_id: String,
    query: String,
    #[serde(default)]
    regex: bool,
    scope: Option<String>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    run_id: Option<Uuid>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorStartArgs {
    slot_id: String,
    contains: Option<String>,
    regex: Option<String>,
    description: Option<String>,
    idempotency_key: Option<Uuid>,
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MonitorListArgs {
    slot_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorIdArgs {
    monitor_id: Uuid,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorIncidentsArgs {
    monitor_id: Uuid,
    after: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArgs {
    run_handle: String,
    expect: Option<String>,
    regex: Option<String>,
    timeout_seconds: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandArgs {
    run_handle: String,
    command: String,
    description: String,
    expect: Option<String>,
    regex: Option<String>,
    timeout_seconds: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSequenceArgs {
    run_handle: String,
    description: String,
    steps: Vec<CommandSequenceStepArgs>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandSequenceStepArgs {
    command: String,
    description: String,
    expect: Option<String>,
    regex: Option<String>,
    timeout_seconds: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputArgs {
    run_handle: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SignalArgs {
    run_handle: String,
    signal: String,
    duration_ms: Option<u64>,
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
    run_handle: String,
    #[serde(rename = "kickoff")]
    initial_write: Option<TriggerWriteArgs>,
    start_contains: Option<String>,
    action: TriggerWriteArgs,
    #[serde(default)]
    stop_contains: Vec<String>,
    interval_ms: Option<u64>,
    timeout_ms: Option<u64>,
    max_fires: Option<u32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStartArgs {
    slot_id: String,
    label: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunEndArgs {
    run_handle: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseArgs {
    slot_id: Option<String>,
    #[serde(default)]
    abort_run: bool,
    run_handle: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{GapRange, GapReason, TimelineEvent};

    const TEST_RUN_HANDLE: &str = "AAAAAAAAAAAAAAAAAAAAAA";

    fn activity_event(
        epoch: Uuid,
        seq: u64,
        actor_id: &str,
        actor_label: &str,
        metadata: BTreeMap<String, Value>,
    ) -> TimelineEvent {
        TimelineEvent {
            slot_id: "bench".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind: EventKind::Tx,
            direction: Direction::Tx,
            actor: Some(Actor {
                id: actor_id.into(),
                label: actor_label.into(),
                kind: ActorKind::Agent,
            }),
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(0),
            stream_offset_end: Some(4),
            data: b"test".to_vec(),
            metadata,
            durable: true,
        }
    }

    #[test]
    fn recent_context_filters_by_server_actor_id_not_shared_label_and_keeps_descriptions() {
        let epoch = Uuid::new_v4();
        let previous = Cursor {
            epoch,
            after_seq: 1,
        };
        let current = Cursor {
            epoch,
            after_seq: 3,
        };
        let own = activity_event(epoch, 2, "own-id", "serial-mcp", BTreeMap::new());
        let foreign = activity_event(
            epoch,
            3,
            "foreign-id",
            // Deliberately identical: labels are audit text, not identity.
            "serial-mcp",
            BTreeMap::from([
                ("command_description".into(), json!("查看样机内存")),
                (
                    "command_sequence_description".into(),
                    json!("登录并检查内存"),
                ),
            ]),
        );
        let context = summarize_recent_context(
            EventQueryResponse {
                events: vec![own, foreign],
                next_cursor: Some(current.clone()),
                truncated: false,
                first_available_seq: Some(1),
                gaps: Vec::new(),
            },
            Some("own-id"),
            &previous,
            &current,
        )
        .expect("foreign same-label actor must remain visible");

        assert_eq!(context["interference"], true);
        assert_eq!(context["complete"], true);
        assert_eq!(context["events"].as_array().unwrap().len(), 1);
        assert_eq!(context["events"][0]["actor"]["label"], "serial-mcp");
        assert_eq!(context["events"][0]["description"], "查看样机内存");
        assert_eq!(
            context["events"][0]["sequence_description"],
            "登录并检查内存"
        );
    }

    #[test]
    fn recent_context_surfaces_gap_even_when_no_relevant_event_survives() {
        let epoch = Uuid::new_v4();
        let previous = Cursor {
            epoch,
            after_seq: 1,
        };
        let current = Cursor {
            epoch,
            after_seq: 50,
        };
        let context = summarize_recent_context(
            EventQueryResponse {
                events: Vec::new(),
                next_cursor: Some(current.clone()),
                truncated: false,
                first_available_seq: Some(40),
                gaps: vec![GapRange {
                    epoch,
                    first_seq: 2,
                    last_seq: 39,
                    reason: GapReason::RingEvicted,
                }],
            },
            Some("own-id"),
            &previous,
            &current,
        )
        .expect("a gap must not be mistaken for proof of no interference");

        assert_eq!(context["interference"], false);
        assert_eq!(context["complete"], false);
        assert_eq!(context["truncated"], true);
        assert_eq!(context["events"], json!([]));
    }

    #[tokio::test]
    async fn pending_context_fails_closed_before_any_side_effect() {
        let api = ApiClient::new("http://127.0.0.1:1".into(), None).unwrap();
        let session = SessionHandle::spawn(
            "http://127.0.0.1:1".into(),
            None,
            "serial-mcp".into(),
            Some(Duration::from_secs(1_800)),
        );
        let tools = AgentTools::new(api, session, "serial-mcp".into(), CaptureLimits::default());
        tools.pending_context.lock().unwrap().insert(
            "bench".into(),
            json!({
                "interference": true,
                "complete": true,
                "after_seq": 40,
                "through_seq": 42,
                "events": [{"seq": 41, "kind": "tx"}],
                "truncated": false,
            }),
        );

        let error = tools
            .ensure_serial_context_unchanged(&test_slot())
            .await
            .unwrap_err();
        let structured = structured_tool_error(&error).expect("typed context_changed error");
        assert_eq!(structured["error"]["code"], "context_changed");
        assert_eq!(structured["error"]["no_bytes_written"], true);
        assert_eq!(structured["error"]["recent_context"]["interference"], true);
        assert!(
            structured["error"]["retry_hint"]
                .as_str()
                .unwrap()
                .contains("read(scope=tail)")
        );
    }

    fn device_model_catalog() -> DeviceModelListResponse {
        DeviceModelListResponse {
            models: vec![serial_protocol::DeviceModel {
                id: "tl-as7230".into(),
                name: "TL-AS7230".into(),
                parent_id: None,
                aliases: vec!["7230".into()],
            }],
            bindings: vec![serial_protocol::SlotModelBinding {
                slot_id: "bench".into(),
                model_id: "tl-as7230".into(),
                confirmation_method: ModelConfirmationMethod::Human,
                note: Some("operator checked label".into()),
                updated_wall_time_ns: 1,
                source: "human:serialctl".into(),
            }],
            config_revision: 9,
        }
    }

    #[test]
    fn device_summary_exposes_the_confirmed_model_binding() {
        let catalog = device_model_catalog();
        let summary = slot_device_model_summary("bench", &catalog);
        assert_eq!(summary["id"], "tl-as7230");
        assert_eq!(summary["name"], "TL-AS7230");
        assert_eq!(summary["aliases"], json!(["7230"]));
        assert_eq!(summary["confirmation_method"], "human");
        assert_eq!(summary["confirmation_note"], "operator checked label");
        assert!(slot_device_model_summary("unbound", &catalog).is_null());
    }

    #[test]
    fn device_model_set_preserves_expected_current_tristate() {
        let omitted: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "confirmation_method": "serial"
        }))
        .unwrap();
        assert_eq!(omitted.expected_current, None);

        let unbound: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "confirmation_method": "human",
            "expected_current": null
        }))
        .unwrap();
        assert_eq!(unbound.expected_current, Some(None));

        let exact: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "confirmation_method": "web",
            "expected_current": "old-model"
        }))
        .unwrap();
        assert_eq!(exact.expected_current, Some(Some("old-model".into())));
    }

    #[test]
    fn device_model_set_uses_observed_revision_and_binding_guards() {
        let catalog = device_model_catalog();
        let args: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "confirmation_method": "serial",
            "note": "confirmed from boot banner"
        }))
        .unwrap();
        let request = build_device_model_set_request(&args, &catalog).unwrap();
        assert_eq!(request.expected_revision, Some(9));
        assert_eq!(request.expected_current, Some(Some("tl-as7230".into())));
        assert_eq!(
            request.confirmation_method,
            Some(ModelConfirmationMethod::Serial)
        );
        assert_eq!(request.source, MODEL_BINDING_SOURCE);

        let mismatch: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "confirmation_method": "human",
            "expected_current": null
        }))
        .unwrap();
        let error = build_device_model_set_request(&mismatch, &catalog)
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected None"));
        assert!(error.contains("re-confirm the physical DUT"));
    }

    #[test]
    fn device_model_set_maps_create_fields_without_catalog_admin_access() {
        let catalog = device_model_catalog();
        let args: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench-2",
            "model_id": "tl-as7230-w",
            "create_if_missing": true,
            "name": "TL-AS7230-W",
            "parent": "tl-as7230",
            "aliases": ["7230-W"],
            "confirmation_method": "telnet",
            "expected_current": null,
            "note": "confirmed from telnet identity output"
        }))
        .unwrap();
        let request = build_device_model_set_request(&args, &catalog).unwrap();
        assert_eq!(request.parent_id.as_deref(), Some("tl-as7230"));
        assert_eq!(request.aliases, vec!["7230-W".to_string()]);
        assert_eq!(request.expected_current, Some(None));
        assert_eq!(request.expected_revision, Some(9));
        assert_eq!(request.model_id.as_deref(), Some("tl-as7230-w"));

        let serialized = serde_json::to_value(request).unwrap();
        assert!(serialized.get("models").is_none());
        assert_eq!(serialized["create_if_missing"], true);
    }

    #[test]
    fn device_model_set_updates_only_the_exact_current_model_with_guards() {
        let catalog = device_model_catalog();
        let args: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "model_id": "tl-as7230",
            "update_existing": true,
            "name": "TL-AS7230 rev2",
            "clear_aliases": true,
            "confirmation_method": "web"
        }))
        .unwrap();
        let request = build_device_model_set_request(&args, &catalog).unwrap();
        assert!(request.update_existing);
        assert_eq!(request.expected_revision, Some(9));
        assert_eq!(request.expected_current, Some(Some("tl-as7230".into())));
        assert_eq!(request.name.as_deref(), Some("TL-AS7230 rev2"));
        assert!(request.clear_aliases);

        let wrong_slot: DeviceModelSetArgs = serde_json::from_value(json!({
            "slot_id": "unbound",
            "model_id": "tl-as7230",
            "update_existing": true,
            "name": "wrong",
            "confirmation_method": "human"
        }))
        .unwrap();
        assert!(
            build_device_model_set_request(&wrong_slot, &catalog)
                .unwrap_err()
                .to_string()
                .contains("only modify the model currently bound")
        );
    }

    #[test]
    fn device_model_results_warn_that_configuration_is_not_evidence() {
        assert!(MODEL_IDENTITY_WARNING.contains("not evidence"));
        for method in ["serial", "telnet", "web UI", "human"] {
            assert!(MODEL_IDENTITY_WARNING.contains(method));
        }
    }

    #[test]
    fn empty_command_is_allowed_when_eol_contributes_bytes() {
        assert_eq!(compose_write_bytes("", "\r").unwrap(), b"\r".to_vec());
        assert_eq!(compose_write_bytes("", "\r\n").unwrap(), b"\r\n".to_vec());
    }

    #[test]
    fn fully_empty_write_is_rejected() {
        let error = compose_write_bytes("", "").unwrap_err();
        assert!(error.to_string().contains("nothing would be sent"));
    }

    #[test]
    fn write_size_limit_counts_command_plus_eol() {
        let command = "x".repeat(MAX_WRITE_BYTES);
        assert!(compose_write_bytes(&command, "").is_ok());
        assert!(compose_write_bytes(&command, "\r").is_err());
    }

    #[test]
    fn read_args_keep_only_scope_and_bounded_archive_cursor() {
        let args: ReadArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "scope": "archive",
            "epoch": Uuid::nil(),
            "after_seq": 42,
            "through_seq": 47,
        }))
        .unwrap();
        assert_eq!(args.scope.as_deref(), Some("archive"));
        assert_eq!(args.epoch, Some(Uuid::nil()));
        assert_eq!(args.after_seq, Some(42));
        assert_eq!(args.through_seq, Some(47));
        assert!(
            serde_json::from_value::<ReadArgs>(json!({"slot_id":"bench","include_raw":true}))
                .is_err()
        );
    }

    #[tokio::test]
    async fn legacy_v3_status_fails_the_common_physical_action_gate_closed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut legacy_status = serde_json::to_value(StatusResponse {
            server_id: Uuid::from_u128(10),
            daemon_epoch: Uuid::from_u128(11),
            protocol_version: PROTOCOL_VERSION,
            config_revision: 1,
            sequence_write_precondition_supported: true,
            serial_context_precondition_supported: true,
            slots: vec![test_slot()],
        })
        .unwrap();
        legacy_status
            .as_object_mut()
            .unwrap()
            .remove("serial_context_precondition_supported");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0_u8; 4096];
            let read = stream.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.starts_with("GET /api/v1/status HTTP/1.1"));
            let body = serde_json::to_string(&legacy_status).unwrap();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        });

        let endpoint = format!("http://{address}");
        let tools = AgentTools::new(
            ApiClient::new(endpoint.clone(), None).unwrap(),
            SessionHandle::spawn(
                endpoint,
                None,
                "test".into(),
                Some(Duration::from_secs(1_800)),
            ),
            "test".into(),
            CaptureLimits::default(),
        );
        let error = tools
            .slot_online_for_physical_action("bench")
            .await
            .unwrap_err()
            .to_string();
        server.await.unwrap();

        assert!(error.contains("Write, BREAK, and Trigger"));
        assert!(error.contains("no bytes were written"));
    }

    #[tokio::test]
    async fn read_tail_uses_bounded_live_ring_endpoint_instead_of_journal_events() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let slot = test_slot();
        let status = StatusResponse {
            server_id: Uuid::from_u128(10),
            daemon_epoch: slot.daemon_epoch,
            protocol_version: PROTOCOL_VERSION,
            config_revision: 1,
            sequence_write_precondition_supported: true,
            serial_context_precondition_supported: true,
            slots: vec![slot.clone()],
        };
        let timeline_event = serial_protocol::TimelineEvent {
            slot_id: "bench".into(),
            daemon_epoch: slot.daemon_epoch,
            seq: 42,
            generation: 1,
            wall_time_ns: 42,
            monotonic_time_ns: 42,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(0),
            stream_offset_end: Some(5),
            data: b"ready".to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        };
        let tail = serial_protocol::EventQueryResponse {
            events: vec![timeline_event],
            next_cursor: Some(Cursor {
                epoch: slot.daemon_epoch,
                after_seq: 42,
            }),
            truncated: false,
            first_available_seq: Some(42),
            gaps: Vec::new(),
        };
        let server = tokio::spawn(async move {
            for expected_path in ["/api/v1/status", "/api/v1/slots/bench/tail?tail_events=200"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 4096];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                assert!(
                    request.starts_with(&format!("GET {expected_path} HTTP/1.1")),
                    "unexpected request: {request}"
                );
                assert!(!request.starts_with("GET /api/v1/slots/bench/events"));
                let body = if expected_path == "/api/v1/status" {
                    serde_json::to_string(&status).unwrap()
                } else {
                    serde_json::to_string(&tail).unwrap()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let endpoint = format!("http://{address}");
        let tools = AgentTools::new(
            ApiClient::new(endpoint.clone(), None).unwrap(),
            SessionHandle::spawn(
                endpoint,
                None,
                "test".into(),
                Some(Duration::from_secs(1_800)),
            ),
            "test".into(),
            CaptureLimits::default(),
        );
        let output = tools
            .read(ReadArgs {
                slot_id: "bench".into(),
                scope: Some("tail".into()),
                epoch: None,
                after_seq: None,
                through_seq: None,
            })
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(output["text"], "ready");
        assert_eq!(output["cursor"]["after_seq"], 42);
    }

    #[tokio::test]
    async fn read_continue_uses_cursor_bounded_live_ring_and_surfaces_eviction() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let slot = test_slot();
        let status = StatusResponse {
            server_id: Uuid::from_u128(10),
            daemon_epoch: slot.daemon_epoch,
            protocol_version: PROTOCOL_VERSION,
            config_revision: 1,
            sequence_write_precondition_supported: true,
            serial_context_precondition_supported: true,
            slots: vec![slot.clone()],
        };
        let event = activity_event(slot.daemon_epoch, 42, "target", "target", BTreeMap::new());
        let live = EventQueryResponse {
            events: vec![serial_protocol::TimelineEvent {
                actor: None,
                kind: EventKind::Rx,
                direction: Direction::Rx,
                data: b"latest".to_vec(),
                ..event
            }],
            next_cursor: Some(Cursor {
                epoch: slot.daemon_epoch,
                after_seq: 42,
            }),
            truncated: true,
            first_available_seq: Some(42),
            gaps: vec![GapRange {
                epoch: slot.daemon_epoch,
                first_seq: 41,
                last_seq: 41,
                reason: GapReason::RingEvicted,
            }],
        };
        let expected_continue = format!(
            "/api/v1/slots/bench/tail?tail_events=1000&epoch={}&after_seq=40",
            slot.daemon_epoch
        );
        let server = tokio::spawn(async move {
            for expected_path in ["/api/v1/status".to_owned(), expected_continue] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0_u8; 4096];
                let read = stream.read(&mut request).await.unwrap();
                let request = String::from_utf8_lossy(&request[..read]);
                assert!(
                    request.starts_with(&format!("GET {expected_path} HTTP/1.1")),
                    "unexpected request: {request}"
                );
                assert!(!request.starts_with("GET /api/v1/slots/bench/events"));
                let body = if expected_path == "/api/v1/status" {
                    serde_json::to_string(&status).unwrap()
                } else {
                    serde_json::to_string(&live).unwrap()
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let endpoint = format!("http://{address}");
        let tools = AgentTools::new(
            ApiClient::new(endpoint.clone(), None).unwrap(),
            SessionHandle::spawn(
                endpoint,
                None,
                "test".into(),
                Some(Duration::from_secs(1_800)),
            ),
            "test".into(),
            CaptureLimits::default(),
        );
        tools.remember_live_cursor(
            "bench",
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: 40,
            },
        );
        let output = tools
            .read(ReadArgs {
                slot_id: "bench".into(),
                scope: Some("continue".into()),
                epoch: None,
                after_seq: None,
                through_seq: None,
            })
            .await
            .unwrap();
        server.await.unwrap();

        assert_eq!(output["source"], "live_ring");
        assert_eq!(output["bounded_continue"], true);
        assert_eq!(output["text"], "latest");
        assert_eq!(output["cursor"]["after_seq"], 42);
        assert_eq!(output["gap"], true);
        assert_eq!(output["truncated"], true);
        assert_eq!(output["gaps"][0]["reason"], "ring_evicted");
        assert_eq!(output["gaps"][0]["first_seq"], 41);
        assert_eq!(output["gaps"][0]["last_seq"], 41);
        assert!(
            output["warnings"][0]
                .as_str()
                .unwrap()
                .contains("live replay gap")
        );
    }

    #[test]
    fn search_and_wait_args_parse_new_optional_fields() {
        let search: SearchArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "query": "ERROR.*1006",
            "regex": true,
        }))
        .unwrap();
        assert_eq!(search.query, "ERROR.*1006");
        assert!(search.regex);

        let wait: WaitArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "expect": "ready"
        }))
        .unwrap();
        assert_eq!(wait.expect.as_deref(), Some("ready"));
    }

    #[test]
    fn monitor_arguments_are_small_and_matchers_are_unambiguous() {
        let literal: MonitorStartArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "contains": "kernel panic",
            "description": "intermittent crash"
        }))
        .unwrap();
        validate_monitor_matcher(literal.contains.as_deref(), literal.regex.as_deref()).unwrap();
        let request = create_monitor_request(literal).unwrap();
        assert!(!request.request_id.is_nil());
        assert_eq!(request.spec.slot_id, "bench");
        assert_eq!(request.spec.contains.as_deref(), Some("kernel panic"));
        assert_eq!(
            request.spec.description.as_deref(),
            Some("intermittent crash")
        );
        assert_eq!(request.spec.debounce_ms, 250);
        assert_eq!(request.spec.cooldown_ms, 30_000);

        let idempotency_key = Uuid::new_v4();
        let retry: MonitorStartArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "contains": "kernel panic",
            "idempotency_key": idempotency_key,
        }))
        .unwrap();
        assert_eq!(
            create_monitor_request(retry).unwrap().request_id,
            idempotency_key
        );

        let regex: MonitorStartArgs = serde_json::from_value(json!({
            "slot_id": "bench",
            "regex": "(?i)watchdog|panic"
        }))
        .unwrap();
        validate_monitor_matcher(regex.contains.as_deref(), regex.regex.as_deref()).unwrap();

        for invalid in [
            json!({"slot_id":"bench"}),
            json!({"slot_id":"bench","contains":"panic","regex":"panic"}),
            json!({"slot_id":"bench","contains":""}),
            json!({"slot_id":"bench","regex":".*"}),
        ] {
            let args: MonitorStartArgs = serde_json::from_value(invalid).unwrap();
            assert!(
                validate_monitor_matcher(args.contains.as_deref(), args.regex.as_deref()).is_err()
            );
        }
        assert!(
            serde_json::from_value::<MonitorStartArgs>(json!({
                "slot_id":"bench", "contains":"panic", "delivery_mode":"push"
            }))
            .is_err()
        );
        for description in [
            "".to_string(),
            "x".repeat(MAX_MONITOR_DESCRIPTION_BYTES + 1),
        ] {
            let args: MonitorStartArgs = serde_json::from_value(json!({
                "slot_id": "bench", "contains": "panic", "description": description
            }))
            .unwrap();
            assert!(create_monitor_request(args).is_err());
        }
    }

    #[test]
    fn monitor_incident_cursor_is_opaque_decimal_text() {
        let args: MonitorIncidentsArgs = serde_json::from_value(json!({
            "monitor_id": Uuid::nil(),
            "after": "18446744073709551615"
        }))
        .unwrap();
        assert_eq!(
            parse_monitor_cursor(args.after.as_deref().unwrap()).unwrap(),
            u64::MAX
        );
        for invalid in ["-1", " 1", "1.0", "next"] {
            assert!(parse_monitor_cursor(invalid).is_err());
        }
    }

    #[test]
    fn compact_monitor_incident_keeps_evidence_without_internal_policy() {
        let incident = json!({
            "id": Uuid::from_u128(1),
            "incident_seq": 7,
            "monitor_id": Uuid::from_u128(2),
            "slot_id": "bench",
            "daemon_epoch": Uuid::from_u128(3),
            "seq_start": 40,
            "seq_end": 43,
            "severity": "error",
            "description": "panic",
            "preview": "Kernel panic",
            "evidence_cursor": {"epoch": Uuid::from_u128(3), "after_seq": 39},
            "evidence_ref": "serial://bench/epochs/3/events?after_seq=39&through_seq=43",
            "wall_time_start_ns": 10,
            "wall_time_end_ns": 20,
            "created_wall_time_ns": 21,
            "acked_wall_time_ns": null,
            "internal_worker_token": "omit"
        });
        let compact = compact_monitor_incident(&incident);
        assert_eq!(compact["incident_id"], json!(Uuid::from_u128(1)));
        assert_eq!(compact["serial_range"]["seq_start"], 40);
        assert_eq!(compact["evidence_cursor"]["after_seq"], 39);
        assert_eq!(compact["acked"], false);
        assert!(compact.get("internal_worker_token").is_none());
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
        let args: CommandArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "command": "",
            "description": "发送回车"
        }))
        .unwrap();
        assert!(args.command.is_empty());
    }

    #[test]
    fn command_description_is_required_and_utf8_byte_bounded() {
        assert!(
            serde_json::from_value::<CommandArgs>(json!({
                "run_handle": TEST_RUN_HANDLE,
                "command": "cat /proc/meminfo"
            }))
            .is_err()
        );
        for invalid in ["", " 前导空格", "尾随空格 ", "包含\n换行"] {
            assert!(validate_command_description(invalid).is_err());
        }
        assert!(validate_command_description("查看样机内存").is_ok());
        let oversized = "界".repeat(MAX_COMMAND_DESCRIPTION_BYTES / "界".len() + 1);
        assert!(validate_command_description(&oversized).is_err());
    }

    fn sequence_step(value: Value) -> CommandSequenceStepArgs {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn command_sequence_args_are_strict_and_require_an_overall_description() {
        let args: CommandSequenceArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "description": "登录样机",
            "steps": [
                {"command":"admin", "description":"输入账号", "expect":"Password:"},
                {"command":"secret", "description":"输入密码", "regex":"[#>$]\\s*$"}
            ]
        }))
        .unwrap();
        assert_eq!(args.description, "登录样机");
        assert_eq!(args.steps.len(), 2);

        assert!(
            serde_json::from_value::<CommandSequenceArgs>(json!({
                "run_handle":TEST_RUN_HANDLE,
                "steps":[{"command":"admin","description":"输入账号"}]
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<CommandSequenceArgs>(json!({
                "run_handle":TEST_RUN_HANDLE,
                "description":"登录样机",
                "steps":[{"command":"admin","description":"输入账号","sensitive":true}]
            }))
            .is_err()
        );
    }

    #[test]
    fn command_sequence_prevalidates_every_dependent_boundary_and_timeout() {
        let valid = vec![
            sequence_step(json!({
                "command":"admin", "description":"输入账号", "expect":"Password:"
            })),
            sequence_step(json!({
                "command":"secret", "description":"输入密码", "regex":"[#>$]\\s*$",
                "timeout_seconds":120
            })),
        ];
        validate_command_sequence_shape(&valid).unwrap();

        for invalid in [
            vec![],
            vec![
                sequence_step(json!({"command":"admin","description":"输入账号"})),
                sequence_step(json!({"command":"secret","description":"输入密码"})),
            ],
            vec![
                sequence_step(json!({
                    "command":"admin", "description":"输入账号",
                    "expect":"Password:", "regex":"Password:"
                })),
                sequence_step(json!({"command":"secret","description":"输入密码"})),
            ],
            vec![sequence_step(json!({
                "command":"admin", "description":"输入账号", "regex":"("
            }))],
            vec![sequence_step(json!({
                "command":"admin", "description":"输入账号", "regex":".*"
            }))],
            vec![sequence_step(json!({
                "command":"admin", "description":"输入账号", "timeout_seconds":0
            }))],
            vec![sequence_step(json!({
                "command":"admin", "description":"输入账号", "timeout_seconds":121
            }))],
            vec![
                sequence_step(json!({
                    "command":"one", "description":"第一步", "expect":"1",
                    "timeout_seconds":120
                })),
                sequence_step(json!({
                    "command":"two", "description":"第二步", "expect":"2",
                    "timeout_seconds":120
                })),
                sequence_step(json!({
                    "command":"three", "description":"第三步", "timeout_seconds":61
                })),
            ],
        ] {
            assert!(validate_command_sequence_shape(&invalid).is_err());
        }

        let too_many = (0..=MAX_COMMAND_SEQUENCE_STEPS)
            .map(|index| {
                sequence_step(json!({
                    "command":format!("step-{index}"),
                    "description":format!("第{}步", index + 1),
                    "expect":"ready"
                }))
            })
            .collect::<Vec<_>>();
        assert!(validate_command_sequence_shape(&too_many).is_err());
    }

    #[test]
    fn command_sequence_physical_byte_budget_uses_each_effective_eol() {
        let mut slot = test_slot();
        slot.effective_write_eol = Some("\r".into());
        slot.effective_echo = Some(EchoMode::On);
        let max_command = "x".repeat(MAX_WRITE_BYTES - 1);
        let steps = (0..MAX_COMMAND_SEQUENCE_STEPS)
            .map(|index| CommandSequenceStepArgs {
                command: max_command.clone(),
                description: format!("第{}步", index + 1),
                expect: (index + 1 < MAX_COMMAND_SEQUENCE_STEPS).then(|| "ready".into()),
                regex: None,
                timeout_seconds: Some(1),
            })
            .collect::<Vec<_>>();
        let prepared = prepare_command_sequence_steps(steps, &slot).unwrap();
        assert_eq!(
            prepared.iter().map(|step| step.bytes.len()).sum::<usize>(),
            MAX_COMMAND_SEQUENCE_TOTAL_WRITE_BYTES
        );

        let oversized = vec![CommandSequenceStepArgs {
            command: "x".repeat(MAX_WRITE_BYTES),
            description: "超过物理写上限".into(),
            expect: None,
            regex: None,
            timeout_seconds: Some(1),
        }];
        assert!(prepare_command_sequence_steps(oversized, &slot).is_err());
    }

    #[test]
    fn command_sequence_advances_only_on_unambiguous_explicit_evidence() {
        let execution = |completion| ExecutedCommandStep {
            output: json!({}),
            completion,
            cursor: Cursor {
                epoch: Uuid::nil(),
                after_seq: 1,
            },
            truncated: false,
            gap: false,
            interfered: false,
            echo_missing: false,
            no_rx: false,
        };
        assert!(
            command_sequence_stop(&execution(Completion::Pattern("Password:".into())), true)
                .is_none()
        );
        assert!(
            command_sequence_stop(&execution(Completion::Regex("prompt".into())), true).is_none()
        );
        for completion in [
            Completion::Quiet,
            Completion::Prompt("# ".into()),
            Completion::Timeout,
            Completion::Disconnected("closed".into()),
        ] {
            assert!(command_sequence_stop(&execution(completion), true).is_some());
        }

        for mutate in ["gap", "truncated", "interfered", "echo_missing", "no_rx"] {
            let mut unsafe_step = execution(Completion::Pattern("Password:".into()));
            match mutate {
                "gap" => unsafe_step.gap = true,
                "truncated" => unsafe_step.truncated = true,
                "interfered" => unsafe_step.interfered = true,
                "echo_missing" => unsafe_step.echo_missing = true,
                "no_rx" => unsafe_step.no_rx = true,
                _ => unreachable!(),
            }
            assert!(command_sequence_stop(&unsafe_step, true).is_some());
        }

        let aborted = execution(Completion::RunAborted {
            run_id: Uuid::new_v4(),
            reason: "human takeover".into(),
        });
        let stop = command_sequence_stop(&aborted, true).expect("abort stops sequence");
        assert_eq!(stop.code, "run_aborted");
        assert!(sequence_stop_forces_closed(&stop));
    }

    #[test]
    fn complete_echo_counts_as_post_write_rx_after_echo_bytes_are_stripped() {
        assert!(!command_has_no_rx(0, true));
        assert!(command_has_no_rx(0, false));
        assert!(!command_has_no_rx(1, false));
    }

    #[test]
    fn sequence_boundary_failure_is_a_zero_next_step_partial_result() {
        let slot = test_slot();
        let run_id = Uuid::new_v4();
        let output = command_sequence_output(
            &slot,
            run_id,
            Uuid::new_v4(),
            "登录样机".into(),
            2,
            1,
            vec![json!({
                "step_index": 0,
                "status": "completed",
                "cursor": {"epoch": slot.daemon_epoch, "after_seq": 50}
            })],
            Some(json!({
                "step_index": 1,
                "phase": "write",
                "code": "sequence_boundary_changed",
                "next_step_sent": false
            })),
        );
        assert_eq!(output["status"], "partial");
        assert_eq!(output["sent_steps"], 1);
        assert_eq!(output["completed_steps"], 1);
        assert_eq!(output["failure"]["next_step_sent"], false);
        assert_eq!(output["failure"]["code"], "sequence_boundary_changed");
    }

    #[test]
    fn release_planning_uses_local_lease_ownership_and_fails_closed_on_run_races() {
        let local_run = Uuid::new_v4();
        let foreign_run = Uuid::new_v4();
        let run_token = Uuid::new_v4();

        // A fresh MCP must not require a token for, or interfere with, a Run
        // merely because that foreign Run is visible in daemon status.
        assert_eq!(
            plan_release(
                LocalControlState {
                    has_lease: false,
                    owned_run_id: None,
                },
                Some(foreign_run),
                false,
                None,
            )
            .unwrap(),
            ReleaseDecision::AlreadyReleased
        );

        // No authoritative active Run turns an old process-local Run entry
        // into cleanup state; default release needs neither abort nor token.
        assert_eq!(
            plan_release(
                LocalControlState {
                    has_lease: true,
                    owned_run_id: Some(local_run),
                },
                None,
                false,
                None,
            )
            .unwrap(),
            ReleaseDecision::Release {
                authorize: None,
                allow_stale_cleanup: true,
            }
        );
        // A caller can race with an already-authoritative external abort after
        // resolving its handle. The fresh daemon snapshot wins: stale cleanup
        // must discard that now-obsolete capability instead of panicking or
        // trying to authorize an abort of a Run that no longer exists.
        assert_eq!(
            plan_release(
                LocalControlState {
                    has_lease: true,
                    owned_run_id: Some(local_run),
                },
                None,
                true,
                Some((local_run, run_token)),
            )
            .unwrap(),
            ReleaseDecision::Release {
                authorize: None,
                allow_stale_cleanup: true,
            }
        );

        let active_local = LocalControlState {
            has_lease: true,
            owned_run_id: Some(local_run),
        };
        assert!(plan_release(active_local, Some(local_run), false, None).is_err());
        assert!(plan_release(active_local, Some(local_run), true, None).is_err());
        assert_eq!(
            plan_release(
                active_local,
                Some(local_run),
                true,
                Some((local_run, run_token)),
            )
            .unwrap(),
            ReleaseDecision::Release {
                authorize: Some((local_run, run_token)),
                allow_stale_cleanup: false,
            }
        );

        assert!(
            plan_release(
                active_local,
                Some(local_run),
                true,
                Some((foreign_run, run_token)),
            )
            .is_err()
        );
        assert!(
            plan_release(
                active_local,
                Some(foreign_run),
                true,
                Some((local_run, run_token)),
            )
            .is_err()
        );
    }

    #[test]
    fn control_signals_are_exact_single_bytes_without_eol() {
        assert_eq!(control_signal_byte("ctrl_c"), Some(0x03));
        assert_eq!(control_signal_byte("ctrl_d"), Some(0x04));
        assert_eq!(control_signal_byte("ctrl_z"), Some(0x1a));
        assert_eq!(control_signal_byte("break"), None);
    }

    #[test]
    fn agent_writes_reject_transport_pacing_overrides() {
        let command = serde_json::from_value::<CommandArgs>(json!({
            "run_handle": TEST_RUN_HANDLE,
            "command": "version",
            "description": "查看版本",
            "chunk_size": 64
        }))
        .err()
        .expect("command pacing override must be rejected");
        assert!(command.to_string().contains("unknown field"));

        let trigger = serde_json::from_value::<TriggerArgs>(json!({
            "run_handle": TEST_RUN_HANDLE,
            "action": {"text": "slp"},
            "inter_char_delay_ms": 0
        }))
        .err()
        .expect("trigger pacing override must be rejected");
        assert!(trigger.to_string().contains("unknown field"));
    }

    #[test]
    fn trigger_uses_only_explicit_call_text_and_eol() {
        let args: TriggerArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "kickoff": {"text": "reboot", "eol": "\r"},
            "start_contains": "Booting",
            "action": {"text": "slp"},
            "stop_contains": ["any caller literal"],
            "interval_ms": 20,
            "timeout_ms": 5000,
            "max_fires": 250
        }))
        .unwrap();
        let spec = trigger_spec(&args).unwrap();
        assert_eq!(spec.initial_write.as_deref(), Some(b"reboot\r".as_slice()));
        assert_eq!(spec.start_contains.as_deref(), Some(b"Booting".as_slice()));
        assert_eq!(spec.action, b"slp");
        assert_eq!(spec.stop_contains, vec![b"any caller literal".to_vec()]);
        assert_eq!(spec.interval_ms, 20);
        assert_eq!(spec.timeout_ms, 5000);
        assert_eq!(spec.max_fires, 250);
        assert_eq!(spec.pacing, None);
    }

    #[test]
    fn normal_kickoff_trigger_omits_the_optional_start_gate() {
        let args: TriggerArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "kickoff": {"text": "reboot", "eol": "\r"},
            "action": {"text": "slp"},
            "stop_contains": ["prompt>"]
        }))
        .unwrap();

        let spec = trigger_spec(&args).unwrap();
        assert_eq!(spec.initial_write.as_deref(), Some(b"reboot\r".as_slice()));
        assert!(spec.start_contains.is_none());
        assert_eq!(spec.action, b"slp");
    }

    #[test]
    fn trigger_allows_bounded_one_shot_without_a_stop_literal() {
        let args: TriggerArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "start_contains": "send ACK now",
            "action": {"text": "ACK", "eol": ""},
            "max_fires": 1
        }))
        .unwrap();
        let spec = trigger_spec(&args).unwrap();
        assert!(spec.stop_contains.is_empty());
        assert_eq!(spec.max_fires, 1);
        assert_eq!(
            trigger_status_label(TriggerStatus::MaxFiresReached),
            "max_fires_reached"
        );
        assert!(!TriggerStatus::MaxFiresReached.is_matched());
    }

    #[test]
    fn trigger_guidance_separates_send_budget_from_observation() {
        let with_stop = test_terminal_trigger(
            TriggerStatus::MaxFiresReached,
            DEFAULT_TRIGGER_MAX_FIRES,
            vec![b"prompt".to_vec()],
        );
        assert!(trigger_send_budget_exhausted(&with_stop));
        let guidance = trigger_guidance(&with_stop);
        assert!(guidance.contains("send budget was exhausted"));
        assert!(guidance.contains("kept observing until the original deadline"));
        assert!(guidance.contains("not proof"));

        let without_stop = test_terminal_trigger(
            TriggerStatus::MaxFiresReached,
            DEFAULT_TRIGGER_MAX_FIRES,
            Vec::new(),
        );
        let guidance = trigger_guidance(&without_stop);
        assert!(guidance.contains("No stop literal was configured"));
        assert!(guidance.contains("instead of blindly retrying"));

        let timed_out = test_terminal_trigger(TriggerStatus::TimedOut, 1, vec![b"ready".to_vec()]);
        assert!(!trigger_send_budget_exhausted(&timed_out));
        assert!(trigger_guidance(&timed_out).contains("observation deadline"));
    }

    #[test]
    fn trigger_rejects_empty_or_unbounded_payload_plans() {
        let empty: TriggerArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "action": {"text": "", "eol": ""}
        }))
        .unwrap();
        assert!(
            trigger_spec(&empty)
                .unwrap_err()
                .to_string()
                .contains("both empty")
        );

        let over_total: TriggerArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "kickoff": {"text": "x"},
            "action": {"text": "a".repeat(256)},
            "max_fires": 256
        }))
        .unwrap();
        assert!(
            trigger_spec(&over_total)
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
        let mut output = json!({"cursor": {"epoch": epoch, "after_seq": 1234}});
        attach_search_continuation_guidance(&mut output, "current_run", Some(run_id));

        assert_eq!(output["continuation"]["scope"], "current_run");
        assert_eq!(output["continuation"]["epoch"], json!(epoch));
        assert_eq!(output["continuation"]["after_seq"], 1234);
        assert_eq!(output["continuation"]["run_id"], json!(run_id));
        assert!(output["guidance"].as_str().unwrap().contains("incomplete"));
        assert!(!output["guidance"].as_str().unwrap().contains("archive"));
    }

    #[test]
    fn command_assessment_never_claims_execution_success() {
        assert_eq!(
            completion_kind(&Completion::Pattern("]# ".into())),
            "literal"
        );
        assert_eq!(completion_kind(&Completion::Prompt("]# ".into())), "prompt");
        assert_eq!(
            command_confidence(&Completion::Quiet, false, false, false, false, 1),
            "low"
        );
        assert_eq!(
            command_confidence(
                &Completion::Pattern("]# ".into()),
                false,
                false,
                false,
                true,
                1,
            ),
            "medium"
        );
    }

    #[test]
    fn human_takeover_error_has_stable_machine_readable_fields() {
        let run_id = Uuid::new_v4();
        let message = format_run_abort_error(
            "bench",
            run_id,
            &RunAbortDiagnosis {
                reason: "human takeover".into(),
                taken_over_by: Some(Actor {
                    id: "human:operator-1".into(),
                    label: "operator-1".into(),
                    kind: ActorKind::Human,
                }),
            },
            true,
        );
        assert!(message.starts_with("human_takeover:"));
        assert!(message.contains("taken_over_by=\"operator-1 (human:operator-1)\""));
        assert!(message.contains(&format!("run_id={run_id}")));
        assert!(message.contains("no_bytes_written=true"));
        assert!(message.contains("DUT model/state is reconfirmed"));

        for diagnostic in [
            "human_takeover_or_control_revoked: taken_over_by=unknown",
            "seriald StaleFence (retryable=false)",
            "seriald ControlRequired (retryable=false)",
        ] {
            assert!(error_indicates_run_or_control_loss(&anyhow!(diagnostic)));
        }
        assert!(!error_indicates_run_or_control_loss(&anyhow!(
            "ordinary serial I/O error"
        )));
    }

    #[test]
    fn accepted_trigger_takeover_has_structured_diagnosis_without_zero_write_claim() {
        let run_id = Uuid::new_v4();
        let actor = Actor {
            id: "human:operator-1".into(),
            label: "operator-1".into(),
            kind: ActorKind::Human,
        };
        let mut output = json!({"outcome": "control_lost"});
        assert!(attach_trigger_takeover_diagnosis(
            &mut output,
            run_id,
            &RunAbortDiagnosis {
                reason: "human takeover".into(),
                taken_over_by: Some(actor.clone()),
            },
        ));
        assert_eq!(output["abort_diagnosis"]["code"], "human_takeover");
        assert_eq!(output["abort_diagnosis"]["taken_over_by"], json!(actor));
        assert_eq!(output["abort_diagnosis"]["run_id"], json!(run_id));
        assert_eq!(output["abort_diagnosis"]["no_bytes_written"], false);

        let mut non_human = json!({"outcome": "run_lost"});
        assert!(!attach_trigger_takeover_diagnosis(
            &mut non_human,
            run_id,
            &RunAbortDiagnosis {
                reason: "control lease expired".into(),
                taken_over_by: None,
            },
        ));
        assert!(non_human.get("abort_diagnosis").is_none());
    }

    #[test]
    fn aborted_capture_is_never_reported_as_successful_evidence() {
        let completion = Completion::RunAborted {
            run_id: Uuid::new_v4(),
            reason: "human takeover".into(),
        };
        assert_eq!(completion_kind(&completion), "run_aborted");
        assert_eq!(capture_confidence(&completion, false, false), "unreliable");
        assert_eq!(
            command_confidence(&completion, false, false, false, false, 1),
            "unreliable"
        );
    }

    #[test]
    fn rendered_text_truncation_reduces_command_and_wait_confidence() {
        let rendered_text_truncated = true;
        let output_truncated = rendered_text_truncated;
        assert_eq!(
            command_confidence(
                &Completion::Prompt("]# ".into()),
                output_truncated,
                false,
                false,
                false,
                1,
            ),
            "partial"
        );
        assert_eq!(
            capture_confidence(&Completion::Prompt("]# ".into()), output_truncated, false),
            "partial"
        );
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

    #[test]
    fn slot_summary_is_compact_but_keeps_agent_decision_state() {
        let mut slot = test_slot();
        let owner = serial_protocol::Actor {
            id: "agent:one".into(),
            label: "agent-one".into(),
            kind: serial_protocol::ActorKind::Agent,
        };
        slot.config.device_profile = Some("luckfox".into());
        slot.control = Some(serial_protocol::ControlLease {
            id: Uuid::from_u128(1),
            owner: owner.clone(),
            epoch: slot.daemon_epoch,
            generation: slot.generation,
            fence: 7,
            issued_wall_time_ns: 10,
            expires_wall_time_ns: 20,
        });
        slot.active_run = Some(serial_protocol::RunInfo {
            id: Uuid::from_u128(2),
            owner: owner.clone(),
            label: "diagnose".into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 40,
            end_seq: None,
            metadata: BTreeMap::from([("large_internal_value".into(), json!("omit me"))]),
        });
        slot.active_trigger = Some(TriggerInfo {
            id: Uuid::from_u128(3),
            owner,
            daemon_epoch: slot.daemon_epoch,
            generation: slot.generation,
            control_id: Uuid::from_u128(1),
            fence: 7,
            operation_id: Some(Uuid::from_u128(4)),
            expected_run_id: Some(Uuid::from_u128(2)),
            spec: TriggerSpec {
                initial_write: Some(b"secret kickoff bytes".to_vec()),
                start_contains: Some(b"boot".to_vec()),
                action: b"slp".to_vec(),
                interval_ms: DEFAULT_TRIGGER_INTERVAL_MS,
                stop_contains: vec![b"prompt".to_vec()],
                timeout_ms: DEFAULT_TRIGGER_TIMEOUT_MS,
                max_fires: DEFAULT_TRIGGER_MAX_FIRES,
                pacing: None,
            },
            status: TriggerStatus::Running,
            start_seq: 41,
            end_seq: None,
            last_write_seq: Some(42),
            fires_confirmed: 2,
            tx_bytes_confirmed: 6,
            matched_pattern: None,
        });

        let summary = slot_summary(&slot);
        assert_eq!(summary["slot_id"], "bench");
        assert_eq!(summary["transport_profile"], "linux");
        assert_eq!(summary["device_profile"], "luckfox");
        assert_eq!(summary["session_state"], "online");
        assert_eq!(summary["cursor"]["epoch"], json!(Uuid::nil()));
        assert_eq!(summary["cursor"]["after_seq"], 42);
        assert_eq!(summary["effective_transport"]["baud_rate"], 115_200);
        assert_eq!(summary["effective_device"]["write_eol"], "\r");
        assert_eq!(summary["control"]["owner"]["id"], "agent:one");
        assert_eq!(summary["active_run"]["label"], "diagnose");
        assert_eq!(summary["active_trigger"]["fires_confirmed"], 2);

        for removed in [
            "config",
            "profile",
            "baud_rate",
            "write_eol",
            "effective_write_eol",
            "head_seq",
            "epoch",
        ] {
            assert!(
                summary.get(removed).is_none(),
                "legacy or duplicate field {removed:?} leaked into devices"
            );
        }
        assert!(summary["active_run"].get("metadata").is_none());
        assert!(summary["active_trigger"].get("spec").is_none());
        assert!(
            !serde_json::to_string(&summary)
                .unwrap()
                .contains("secret kickoff bytes")
        );
    }

    #[test]
    fn http_status_protocol_mismatch_fails_closed() {
        let mut status = StatusResponse {
            server_id: Uuid::from_u128(10),
            daemon_epoch: Uuid::from_u128(11),
            protocol_version: PROTOCOL_VERSION,
            config_revision: 1,
            sequence_write_precondition_supported: true,
            serial_context_precondition_supported: true,
            slots: vec![test_slot()],
        };
        ensure_protocol_compatible(&status).unwrap();
        ensure_sequence_write_precondition_supported(&status).unwrap();
        ensure_serial_context_precondition_supported(&status).unwrap();

        status.protocol_version = 0;
        let error = ensure_protocol_compatible(&status).unwrap_err().to_string();
        assert!(error.contains("protocol version 0"));
        assert!(error.contains("same release"));

        status.protocol_version = PROTOCOL_VERSION;
        status.sequence_write_precondition_supported = false;
        let error = ensure_sequence_write_precondition_supported(&status)
            .unwrap_err()
            .to_string();
        assert!(error.contains("atomic command_sequence"));
        assert!(error.contains("no bytes were written"));

        status.sequence_write_precondition_supported = true;
        status.serial_context_precondition_supported = false;
        let error = ensure_serial_context_precondition_supported(&status)
            .unwrap_err()
            .to_string();
        assert!(error.contains("Write, BREAK, and Trigger"));
        assert!(error.contains("no bytes were written"));
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

    fn test_terminal_trigger(
        status: TriggerStatus,
        fires_confirmed: u32,
        stop_contains: Vec<Vec<u8>>,
    ) -> TriggerInfo {
        TriggerInfo {
            id: Uuid::from_u128(30),
            owner: Actor {
                id: "agent:test".into(),
                label: "test".into(),
                kind: ActorKind::Agent,
            },
            daemon_epoch: Uuid::nil(),
            generation: 1,
            control_id: Uuid::from_u128(31),
            fence: 1,
            operation_id: Some(Uuid::from_u128(32)),
            expected_run_id: Some(Uuid::from_u128(33)),
            spec: TriggerSpec {
                initial_write: None,
                start_contains: None,
                action: b"x".to_vec(),
                interval_ms: DEFAULT_TRIGGER_INTERVAL_MS,
                stop_contains,
                timeout_ms: DEFAULT_TRIGGER_TIMEOUT_MS,
                max_fires: DEFAULT_TRIGGER_MAX_FIRES,
                pacing: None,
            },
            status,
            start_seq: 1,
            end_seq: Some(5),
            last_write_seq: Some(3),
            fires_confirmed,
            tx_bytes_confirmed: fires_confirmed as u64,
            matched_pattern: None,
        }
    }

    #[test]
    fn command_defaults_to_the_daemons_effective_device_profile_eol() {
        let mut slot = test_slot();
        assert_eq!(effective_write_eol(&slot), "\r");
        slot.effective_write_eol = Some("\n".into());
        assert_eq!(effective_write_eol(&slot), "\n");
        assert_eq!(
            compose_write_bytes("version", effective_write_eol(&slot)).unwrap(),
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
        let (patterns, regex, mode) = requested_completion(None, None, &slot, true).unwrap();
        assert_eq!(
            patterns,
            vec![
                CompletionPattern::Prompt("]# ".to_string()),
                CompletionPattern::Prompt("SigmaStar #".to_string())
            ]
        );
        assert!(regex.is_none());
        assert_eq!(mode, "prompt");
    }

    #[test]
    fn promptless_effective_profile_does_not_fall_back_to_stale_slot_prompts() {
        let mut slot = test_slot();
        slot.config.settings.shell_prompt = Some("legacy# ".into());
        slot.config.settings.uboot_prompt = Some("legacy=> ".into());
        slot.effective_write_eol = Some("\r".into());
        slot.effective_echo = Some(EchoMode::On);
        let (patterns, _, mode) = requested_completion(None, None, &slot, true).unwrap();
        assert!(patterns.is_empty());
        assert_eq!(mode, "quiet");

        slot.effective_write_eol = None;
        slot.effective_echo = None;
        let (legacy_patterns, _, _) = requested_completion(None, None, &slot, true).unwrap();
        assert_eq!(
            legacy_patterns,
            vec![
                CompletionPattern::Prompt("legacy# ".into()),
                CompletionPattern::Prompt("legacy=> ".into())
            ]
        );
    }

    #[test]
    fn command_args_parse_regex_and_lean_rendering_fields() {
        let args: CommandArgs = serde_json::from_value(json!({
            "run_handle": TEST_RUN_HANDLE,
            "command": "boot",
            "description": "启动样机",
            "regex": "U-Boot \\d+",
        }))
        .unwrap();
        assert_eq!(args.regex.as_deref(), Some("U-Boot \\d+"));
        assert!(
            serde_json::from_value::<CommandArgs>(json!({
                "run_handle":TEST_RUN_HANDLE,
                "command":"boot", "description":"启动样机", "include_events":true
            }))
            .is_err()
        );
    }

    #[test]
    fn exact_regex_replaces_configured_prompts_and_quiet_with_regex_mode() {
        let mut slot = test_slot();
        slot.effective_shell_prompt = Some("]# ".into());
        slot.effective_uboot_prompt = Some("SigmaStar #".into());
        slot.effective_write_eol = Some("\r".into());
        slot.effective_echo = Some(EchoMode::On);
        let (patterns, until_regex, completion_mode) =
            requested_completion(None, Some("U-Boot \\d+"), &slot, true).unwrap();
        let complete_on_quiet = completion_mode == "quiet";

        assert!(patterns.is_empty());
        assert_eq!(completion_mode, "regex");
        assert!(until_regex.is_some());
        assert!(!complete_on_quiet);
    }

    #[test]
    fn regex_boundary_rejects_competing_literal_completion_parameters() {
        let slot = test_slot();
        let error =
            requested_completion(Some("literal"), Some("__DONE__"), &slot, true).unwrap_err();
        assert!(error.to_string().contains("choose one"));

        assert!(
            compile_regex("", "regex")
                .unwrap_err()
                .to_string()
                .contains("must not be empty")
        );
        assert!(
            compile_regex("(", "regex")
                .unwrap_err()
                .to_string()
                .contains("not a valid regex")
        );
        assert!(
            compile_regex(&"界".repeat(1366), "regex")
                .unwrap_err()
                .to_string()
                .contains("4096 UTF-8 bytes")
        );

        for legacy in ["completion", "until", "eol", "quiet_ms"] {
            let mut value = json!({"slot_id":"bench","command":"boot","description":"启动样机"});
            value[legacy] = json!("legacy");
            assert!(serde_json::from_value::<CommandArgs>(value).is_err());
        }
    }
}
