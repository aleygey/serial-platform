use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use regex_syntax::ParserBuilder;
use serde::Deserialize;
use serde_json::{Value, json};
use serial_protocol::{
    Actor, ActorKind, CommandCaptureCompletionKind, CommandCaptureConfidence,
    CommandCaptureMatcher, CommandCaptureMatcherKind, CommandCaptureReport, ConfigurePortsRequest,
    CreateMonitorRequest, Cursor, Direction, EchoMode, EventKind, EventQuery, EventQueryResponse,
    MAX_BREAK_DURATION_MS, MAX_COMMAND_CAPTURE_DETAIL_BYTES, MAX_COMMAND_DESCRIPTION_BYTES,
    MAX_MONITOR_MATCHERS, MAX_MONITOR_PATTERN_BYTES, MAX_MONITOR_TOTAL_PATTERN_BYTES,
    MAX_PHYSICAL_WRITE_TIMEOUT_MS, MIN_BREAK_DURATION_MS, MonitorMatcher, PROTOCOL_VERSION,
    SequenceWritePrecondition, SessionState, SlotSnapshot, StatusResponse, WritePacing,
};
use tokio::sync::oneshot;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use uuid::Uuid;

mod macros;

use crate::{
    api::ApiClient,
    capture::{Capture, CaptureOptions, CommandBoundary, Completion, CompletionPattern},
    config::CaptureLimits,
    render::{MatchExcerptOptions, MatchExcerptPattern, RenderOptions, render_events},
    session::{SequenceBoundaryRejected, SessionHandle, UserCommandUsed, WriteOutcomeUncertain},
};

const DEFAULT_TEXT_CHARS: usize = 16_000;
const MAX_WRITE_BYTES: usize = 4096;
const MAX_COMMAND_SEQUENCE_STEPS: usize = 8;
const MAX_COMMAND_SEQUENCE_TOTAL_WRITE_BYTES: usize = MAX_COMMAND_SEQUENCE_STEPS * MAX_WRITE_BYTES;
const MAX_COMMAND_SEQUENCE_TIMEOUT_SECONDS: u64 = 300;
const WRITE_OUTCOME_UNCERTAIN_RETRY_HINT: &str = "Do not retry automatically. Inspect the exact \
    operation in the TX/control timeline and confirm the device's current state before deciding \
    whether another physical action is safe.";
const MAX_MONITOR_DESCRIPTION_BYTES: usize = 1024;

struct PreparedCommandStep {
    bytes: Vec<u8>,
    description: String,
    timeout: Duration,
    patterns: Vec<CompletionPattern>,
    until_regex: Option<regex::Regex>,
    capture_matchers: Vec<CommandCaptureMatcher>,
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
    echo_ambiguous: bool,
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

    fn is_user_command_used(&self) -> bool {
        self.error.downcast_ref::<UserCommandUsed>().is_some()
    }

    fn is_write_outcome_uncertain(&self) -> bool {
        self.error.downcast_ref::<WriteOutcomeUncertain>().is_some()
    }
}

struct SequenceStop {
    code: &'static str,
    message: String,
}

#[derive(Default)]
struct HumanReadAcknowledgement {
    pending_revision: Option<u64>,
    human_command_seq: Option<u64>,
    acknowledged: bool,
    warning: Option<String>,
}

#[derive(Debug)]
struct ContextChanged {
    recent_context: Value,
}

impl std::fmt::Display for ContextChanged {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "serial context changed since the previous Agent operation; no bytes were written; inspect recent_context with a live read(scope=tail or continue), then retry"
        )
    }
}

impl std::error::Error for ContextChanged {}

pub(crate) fn structured_tool_error(error: &anyhow::Error) -> Option<Value> {
    if let Some(changed) = error.downcast_ref::<ContextChanged>() {
        return Some(json!({
            "error": {
                "code": "context_changed",
                "message": changed.to_string(),
                "no_bytes_written": true,
                "recent_context": changed.recent_context,
                "retry_hint": "Call read(scope=tail) to confirm the new serial state, then retry the operation."
            }
        }));
    }
    if let Some(used) = error.downcast_ref::<UserCommandUsed>() {
        return Some(json!({
            "error": {
                "code": "user_command_used",
                "message": used.to_string(),
                "no_bytes_written": true,
                "retry_hint": "Call read(scope=tail) or read(scope=continue) until the Human TX is returned and acknowledged. wait and archive reads do not clear this gate."
            }
        }));
    }
    error
        .downcast_ref::<WriteOutcomeUncertain>()
        .map(|uncertain| {
            json!({
                "error": write_outcome_uncertain_details(uncertain.to_string()),
            })
        })
}

fn write_outcome_uncertain_details(message: String) -> Value {
    json!({
        "source": "seriald",
        "code": "write_outcome_uncertain",
        "message": message,
        "outcome": "uncertain",
        "no_bytes_written": false,
        "retryable": false,
        "automatic_retry_allowed": false,
        "retry_hint": WRITE_OUTCOME_UNCERTAIN_RETRY_HINT,
    })
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
        if name == "macro_run" {
            return self.macro_run_cancellable(arguments, None).await;
        }
        let mut output = match name {
            "devices" => self.devices(parse(arguments)?).await,
            "model_identity_set" => self.model_identity_set(parse(arguments)?).await,
            "read" => self.read(parse(arguments)?).await,
            "command" => self.command(parse(arguments)?).await,
            "command_sequence" => self.command_sequence(parse(arguments)?).await,
            "signal" => self.signal(parse(arguments)?).await,
            "macro_list" => self.macro_list(parse(arguments)?).await,
            "macro_save" => self.macro_save(parse(arguments)?).await,
            "wait" => self.wait(parse(arguments)?).await,
            "search" => self.search(parse(arguments)?).await,
            "monitor_start" => self.monitor_start(parse(arguments)?).await,
            "monitor_list" => self.monitor_list(parse(arguments)?).await,
            "monitor_status" => self.monitor_status(parse(arguments)?).await,
            "monitor_incidents" => self.monitor_incidents(parse(arguments)?).await,
            "monitor_stop" => self.monitor_stop(parse(arguments)?).await,
            "run_start" => self.run_start(parse(arguments)?).await,
            "run_end" => self.run_end(parse(arguments)?).await,
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
            "read" | "command" | "command_sequence" | "signal" | "macro_run" | "wait" | "run_start"
        ) {
            return;
        }
        // Archived evidence can describe another daemon epoch and therefore
        // cannot acknowledge a pending change on the live physical session.
        if tool_name == "read" && output.get("scope").and_then(Value::as_str) == Some("archive") {
            return;
        }
        let Some(port) = output.get("port").and_then(Value::as_str) else {
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
        // Only a live read is an acknowledgement boundary. wait deliberately
        // does not clear the gate, and a live read that failed to cover/ACK a
        // pending Human TX reports `user_command_acknowledged=false`.
        let is_observation = tool_observes_serial_context(tool_name, output);
        let previous = self
            .operation_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(port.to_owned(), current.clone());
        if tool_name == "run_start" {
            return;
        }
        let Some(previous) = previous else {
            if is_observation {
                self.clear_pending_context(port);
            }
            return;
        };
        if previous.epoch != current.epoch || previous.after_seq >= current.after_seq {
            if is_observation {
                self.clear_pending_context(port);
            }
            return;
        }
        let context = self.recent_context_between(port, &previous, &current).await;
        if is_observation {
            self.clear_pending_context(port);
        } else if let Some(context) = context.as_ref() {
            self.pending_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(port.to_owned(), context.clone());
        }
        if let Some(context) = context {
            output["recent_context"] = context;
        }
    }

    async fn recent_context_between(
        &self,
        port: &str,
        previous: &Cursor,
        current: &Cursor,
    ) -> Option<Value> {
        if previous.epoch != current.epoch || previous.after_seq >= current.after_seq {
            return None;
        }
        let own_actor_id = self.session.actor_id().await.ok().flatten();
        match self
            .api
            .recent_activity(port, current.epoch, previous.after_seq, current.after_seq)
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
    /// subsequent live read is the explicit acknowledgement boundary.
    async fn ensure_serial_context_unchanged(&self, slot: &SlotSnapshot) -> Result<()> {
        if let Some(recent_context) = self
            .pending_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&slot.config.port)
            .cloned()
        {
            return Err(ContextChanged { recent_context }.into());
        }
        let previous = self
            .operation_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&slot.config.port)
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
                .insert(slot.config.port.clone(), recent_context.clone());
            return Err(ContextChanged { recent_context }.into());
        }
        if let Some(recent_context) = self
            .recent_context_between(&slot.config.port, &previous, &current)
            .await
        {
            self.pending_context
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(slot.config.port.clone(), recent_context.clone());
            return Err(ContextChanged { recent_context }.into());
        }
        Ok(())
    }

    fn clear_pending_context(&self, port: &str) {
        self.pending_context
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(port);
    }

    async fn context_changed_after_boundary(
        &self,
        slot: &SlotSnapshot,
        boundary_error: &anyhow::Error,
    ) -> anyhow::Error {
        let current_slot = self
            .slot(&slot.config.port)
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
            .get(&slot.config.port)
            .cloned()
            .unwrap_or(Cursor {
                epoch: slot.daemon_epoch,
                after_seq: slot.head_seq,
            });
        let recent_context = if previous.epoch == current.epoch {
            self.recent_context_between(&slot.config.port, &previous, &current)
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
            .insert(slot.config.port.clone(), recent_context.clone());
        ContextChanged { recent_context }.into()
    }

    async fn devices(&self, args: DevicesArgs) -> Result<Value> {
        let status = self.status().await?;
        let ports: Vec<Value> = status
            .ports
            .iter()
            .filter(|slot| {
                args.port
                    .as_ref()
                    .is_none_or(|port| &slot.config.port == port)
            })
            .map(slot_summary)
            .collect();
        if let Some(port) = args.port
            && ports.is_empty()
        {
            bail!("unknown serial port {port:?}");
        }
        Ok(json!({
            "daemon_epoch": status.daemon_epoch,
            "ports": ports,
            "macro_catalog": self.macro_context().await,
            "selection_note": "Choose a port explicitly and confirm model_family and model_name match the physically connected device before writing. If they do not match, call model_identity_set with the exact existing family/name; if that identity is not in the human-managed catalog, ask the user to create it in the TUI/App first. command, command_sequence, and wait automatically use command_prompts; pass expect or regex to override prompt matching for a call. A Run scopes evidence; it does not reset the device."
        }))
    }

    async fn model_identity_set(&self, args: ModelIdentitySetArgs) -> Result<Value> {
        let model_family = args.model_family.into_option();
        let model_name = args.model_name.into_option();
        if model_family.is_some() != model_name.is_some() {
            bail!("model_family and model_name must both be strings or both be null");
        }
        let status = self.status().await?;
        let current = status
            .ports
            .iter()
            .find(|slot| slot.config.port == args.port)
            .ok_or_else(|| anyhow!("unknown serial port {:?}", args.port))?;
        let previous_model_family = current.config.model_family.clone();
        let previous_model_name = current.config.model_name.clone();
        if let (Some(family_name), Some(model_name)) =
            (model_family.as_deref(), model_name.as_deref())
        {
            let catalog = self.api.model_families().await?;
            let family = catalog
                .families
                .iter()
                .find(|family| family.name == family_name)
                .ok_or_else(|| {
                    anyhow!(
                        "unknown model family {family_name:?}; ask the user to create it in the TUI/App first"
                    )
                })?;
            if !family.model_names.iter().any(|name| name == model_name) {
                bail!(
                    "model name {model_name:?} is not listed in model family {family_name:?}; ask the user to create it in the TUI/App first"
                );
            }
        }
        let mut port_configs = status
            .ports
            .into_iter()
            .map(|port| port.config)
            .collect::<Vec<_>>();
        let configured = port_configs
            .iter_mut()
            .find(|port| port.port == args.port)
            .expect("port was validated against the same status snapshot");
        configured.model_family.clone_from(&model_family);
        configured.model_name.clone_from(&model_name);
        let config_revision = self
            .api
            .configure_ports(&ConfigurePortsRequest {
                ports: port_configs,
                source: "agent:serial-mcp".into(),
                expected_revision: Some(status.config_revision),
            })
            .await?
            .config_revision;
        Ok(json!({
            "port": args.port,
            "previous_model_family": previous_model_family,
            "previous_model_name": previous_model_name,
            "model_family": model_family,
            "model_name": model_name,
            "config_revision": config_revision,
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
        validate_read_exclusions(&args.exclude_patterns)?;
        let slot = self.slot(&args.port).await?;
        let scope = args.scope.as_deref().unwrap_or("tail");
        if args.through_seq.is_some() && scope != "archive" {
            bail!("through_seq is only valid with scope=archive");
        }
        // `wait` intentionally does not acknowledge Human intervention, but
        // it may advance the ordinary live cursor beyond that TX. While the
        // daemon gate is pending, force the next live read to start immediately
        // before the exact Human event so the proof remains recoverable. The
        // daemon still returns an explicit ring gap if that event was evicted;
        // in that case we never fabricate an acknowledgement.
        let human_recovery_cursor = pending_human_read_cursor(&slot);
        let (epoch, response) = match scope {
            "tail" => {
                let response = self
                    .api
                    .live_tail(&args.port, 200, human_recovery_cursor.as_ref())
                    .await?;
                let epoch = response
                    .next_cursor
                    .as_ref()
                    .map(|cursor| cursor.epoch)
                    .unwrap_or(slot.daemon_epoch);
                (epoch, response)
            }
            "continue" => {
                let cursor = human_recovery_cursor
                    .clone()
                    .or_else(|| self.live_cursor(&slot.config.port))
                    .unwrap_or(Cursor {
                        epoch: slot.daemon_epoch,
                        after_seq: slot.head_seq,
                    });
                let response = self.api.live_tail(&args.port, 1_000, Some(&cursor)).await?;
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
                        &args.port,
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
        let human_ack = if scope == "archive" {
            HumanReadAcknowledgement::default()
        } else {
            self.acknowledge_human_context_from_read(&slot, epoch, &response)
                .await
        };
        let filtering_bypassed =
            human_recovery_cursor.is_some() || human_ack.pending_revision.is_some();
        let exclusions = if filtering_bypassed {
            &[][..]
        } else {
            args.exclude_patterns.as_slice()
        };
        let mut output = render_response_with_exclusions(
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
            exclusions,
        );
        if !args.exclude_patterns.is_empty() {
            output["display_filter"] = json!({"exclude_patterns":args.exclude_patterns,"mode":"literal_substring_complete_rx_line","applied":!filtering_bypassed,"excluded_lines":output["excluded_lines"].as_u64().unwrap_or(0),"evidence_unchanged":true,"boundary_fragments_retained":true});
            output["display_filter"]["effective_patterns"] = json!(exclusions);
            if filtering_bypassed {
                output["display_filter"]["bypass_reason"] =
                    json!("Human intervention context must be shown unfiltered");
            } else {
                let warning = json!(
                    "Display filtering is active: complete RX lines containing exclude_patterns may be hidden. An empty display does not prove no output. Remove exclude_patterns to inspect unfiltered evidence."
                );
                if let Some(warnings) = output.get_mut("warnings").and_then(Value::as_array_mut) {
                    warnings.push(warning);
                } else {
                    output["warnings"] = json!([warning]);
                }
            }
        }
        if scope == "tail" {
            output["source"] = json!("live_ring");
            output["bounded_tail"] = json!(true);
            output["tail_events"] = json!(200);
        } else if scope == "continue" {
            output["source"] = json!("live_ring");
            output["bounded_continue"] = json!(true);
            output["limit_events"] = json!(1_000);
        }
        if let Some(cursor) = human_recovery_cursor {
            output["user_command_recovery_after_seq"] = json!(cursor.after_seq);
        }
        if output["cursor"]["epoch"] == json!(slot.daemon_epoch)
            && let Some(after_seq) = output["cursor"]["after_seq"].as_u64()
        {
            self.remember_live_cursor(
                &slot.config.port,
                Cursor {
                    epoch: slot.daemon_epoch,
                    after_seq,
                },
            );
        }
        if let Some(revision) = human_ack.pending_revision {
            output["user_command_context_revision"] = json!(revision);
            output["user_command_seq"] = json!(human_ack.human_command_seq);
            output["user_command_acknowledged"] = json!(human_ack.acknowledged);
            if !human_ack.acknowledged {
                output["user_command_retry_hint"] = json!(
                    "Read the live tail/continuation that contains the Human TX event. wait and \
                     scope=archive do not acknowledge it."
                );
            }
            if let Some(warning) = human_ack.warning {
                output["user_command_acknowledgement_warning"] = json!(warning);
            }
        }
        Ok(output)
    }

    async fn acknowledge_human_context_from_read(
        &self,
        initial_slot: &SlotSnapshot,
        response_epoch: Uuid,
        response: &EventQueryResponse,
    ) -> HumanReadAcknowledgement {
        let (slot, status_warning) = match self.slot(&initial_slot.config.port).await {
            Ok(slot) => (slot, None),
            Err(error) => (
                initial_slot.clone(),
                Some(format!(
                    "could not refresh Run context after the read, so no acknowledgement was sent: \
                     {error}"
                )),
            ),
        };
        let Some(context) = slot.run_context.as_ref() else {
            return HumanReadAcknowledgement::default();
        };
        if context.revision <= context.acknowledged_revision
            || context.last_human_command_seq.is_none()
        {
            return HumanReadAcknowledgement::default();
        }
        let human_command_seq = context
            .last_human_command_seq
            .expect("pending context has a Human command sequence");
        let mut acknowledgement = HumanReadAcknowledgement {
            pending_revision: Some(context.revision),
            human_command_seq: Some(human_command_seq),
            acknowledged: false,
            warning: status_warning,
        };
        if response_epoch != slot.daemon_epoch {
            acknowledgement.warning = Some(
                "the read belongs to a different daemon epoch and cannot acknowledge the live \
                 Human command"
                    .into(),
            );
            return acknowledgement;
        }
        let covers_human_tx =
            live_read_covers_human_tx(response, slot.daemon_epoch, human_command_seq);
        let through_seq = response
            .next_cursor
            .as_ref()
            .filter(|cursor| cursor.epoch == slot.daemon_epoch)
            .map(|cursor| cursor.after_seq)
            .or_else(|| response.events.last().map(|event| event.seq));
        if !covers_human_tx || through_seq.is_none_or(|through| through < human_command_seq) {
            acknowledgement.warning = Some(
                "this bounded live read did not include the pending Human TX event, so the \
                 physical-action gate remains closed"
                    .into(),
            );
            return acknowledgement;
        }
        if slot.active_run.as_ref().map(|run| run.id) != Some(context.run_id) {
            acknowledgement.warning = Some(
                "the pending Run context no longer matches the active Run; no acknowledgement \
                 was sent"
                    .into(),
            );
            return acknowledgement;
        }
        match self
            .session
            .acknowledge_run_context(
                slot.config.port.clone(),
                context.run_id,
                context.revision,
                through_seq.expect("coverage checked a live through sequence"),
            )
            .await
        {
            Ok(acknowledged)
                if acknowledged.run_id == context.run_id
                    && acknowledged.acknowledged_revision >= context.revision
                    && acknowledged
                        .acknowledged_through_seq
                        .is_some_and(|through| through >= human_command_seq) =>
            {
                acknowledgement.acknowledged = true;
                acknowledgement.warning = None;
            }
            Ok(acknowledged) => {
                acknowledgement.warning = Some(format!(
                    "seriald returned an incomplete Run-context acknowledgement (revision {}, \
                     through {:?}); the physical-action gate remains closed",
                    acknowledged.acknowledged_revision, acknowledged.acknowledged_through_seq
                ));
            }
            Err(error) => {
                acknowledgement.warning = Some(format!(
                    "the Human TX was read, but seriald did not acknowledge it; retry the live \
                     read before another physical action: {error}"
                ));
            }
        }
        acknowledgement
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        if args.query.trim().is_empty() {
            bail!("query must not be empty");
        }
        let compiled_regex = args
            .regex
            .then(|| compile_regex(&args.query, "query"))
            .transpose()?;
        let slot = self.slot(&args.port).await?;
        let scope = args.scope.as_deref().unwrap_or("current_run");
        let (epoch, after_seq, run_id) = match scope {
            "current_run" => {
                let after_seq = current_run_after_seq(args.epoch, args.after_seq, &slot)?;
                let run = current_run_id(args.run_id, after_seq, &slot)?;
                (slot.daemon_epoch, after_seq, Some(run))
            }
            "current_cursor" => {
                let cursor = requested_cursor(args.epoch, args.after_seq, &slot)?
                    .or_else(|| self.live_cursor(&slot.config.port))
                    .context("scope=current_cursor has no remembered cursor; call read/run_start first or pass epoch and after_seq")?;
                (cursor.epoch, Some(cursor.after_seq), args.run_id)
            }
            "archive" => {
                let epoch = match args.epoch {
                    Some(epoch) => epoch,
                    None => bail!("{}", self.archive_epoch_hint(&args.port, &slot).await),
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
        let response = self.api.events(&args.port, &query).await?;
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
            self.attach_archive_guidance(&mut output, &args.port, scope)
                .await;
        }
        Ok(output)
    }

    /// Error text for scope=archive without an epoch, carrying a concrete
    /// example value the caller can retry with.
    async fn archive_epoch_hint(&self, port: &str, slot: &SlotSnapshot) -> String {
        let example = self
            .api
            .archives(Some(port))
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
            .ports
            .iter()
            .any(|slot| slot.config.port == request.spec.port)
        {
            bail!("unknown port {:?}", request.spec.port);
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
        let response = serde_json::to_value(self.api.monitors(args.port.as_deref()).await?)
            .context("seriald returned an invalid Monitor list")?;
        let monitors = response
            .get("monitors")
            .and_then(Value::as_array)
            .context("seriald Monitor list omitted monitors")?
            .iter()
            .filter(|monitor| args.include_stopped || monitor["status"] == "running")
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

    async fn monitor_stop(&self, args: MonitorStopArgs) -> Result<Value> {
        let existing = monitor_from_response(self.api.monitor(args.monitor_id).await?)?;
        let revision = existing
            .get("revision")
            .and_then(Value::as_u64)
            .context("seriald Monitor response omitted revision")?;
        let response = self.api.stop_monitor(args.monitor_id, revision).await?;
        if args.delete_history {
            self.api
                .delete_monitor_history(args.monitor_id, response.monitor.revision)
                .await?;
            return Ok(
                json!({"monitor_id":args.monitor_id,"stopped":true,"deleted":true,"incidents_retained":false,"serial_journal_retained":true}),
            );
        }
        let mut output = compact_monitor(&monitor_from_response(response)?);
        output["stopped"] = json!(true);
        output["incidents_retained"] = json!(true);
        Ok(output)
    }

    /// Point an empty search at wider scopes and the retained archive epochs.
    async fn attach_archive_guidance(&self, output: &mut Value, port: &str, scope: &str) {
        match self.api.archives(Some(port)).await {
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
        let slot = self.slot_online(&run_use.port).await?;
        let active_run = matching_active_run(&slot, run_use.run_id, "wait")?;
        let watched_run = (active_run.id, active_run.start_seq);
        let (patterns, until_regex, completion_mode, _) =
            requested_completion(args.expect.as_deref(), args.regex.as_deref(), &slot, true)?;
        let complete_on_quiet = completion_mode == "quiet";
        let remembered_cursor = self.live_cursor(&slot.config.port);
        let (cursor, _) = select_wait_cursor(None, remembered_cursor, &slot);
        let started_epoch = cursor.epoch;
        let started_after_seq = cursor.after_seq;
        let capture = Capture::attach(
            self.api.endpoint(),
            &self.actor_label,
            run_use.port.clone(),
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
                &slot.config.port,
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
            &slot.config.port,
            Cursor {
                epoch: started_epoch,
                after_seq: last_seq,
            },
        );
        let gap = !result.gaps.is_empty();
        let truncated = result.truncated || rendered.text_truncated;
        let confidence = capture_confidence(&result.completion, truncated, gap);
        let mut output = json!({
            "port": slot.config.port,
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
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot_online_for_physical_action(&run_use.port).await?;
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
        let port = run_use.port.clone();
        let run_id = run_use.run_id;
        let run_token = run_use.run_token;
        let _write_guard = self.write_guard(&port).await;
        let status = self.status().await?;
        ensure_sequence_write_precondition_supported(&status)?;
        ensure_serial_context_precondition_supported(&status)?;
        let slot = status
            .ports
            .into_iter()
            .find(|slot| slot.config.port == port)
            .with_context(|| format!("unknown port {port:?}"))?;
        if slot.session_state != SessionState::Online {
            bail!(
                "port {port:?} is {:?}: {}",
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
                    let user_command_used = failure.is_user_command_used();
                    let write_outcome_uncertain = failure.is_write_outcome_uncertain();
                    if step_outputs.is_empty() {
                        if boundary_changed {
                            return Err(self
                                .context_changed_after_boundary(&slot, &failure.error)
                                .await);
                        }
                        return Err(failure.error);
                    }
                    let mut failure_details = if write_outcome_uncertain {
                        write_outcome_uncertain_details(failure.error.to_string())
                    } else {
                        json!({
                            "code": if boundary_changed {
                                "sequence_boundary_changed"
                            } else if user_command_used {
                                "user_command_used"
                            } else {
                                "step_error"
                            },
                            "message": failure.error.to_string(),
                        })
                    };
                    failure_details["step_index"] = json!(step_index);
                    failure_details["phase"] = json!(failure.phase);
                    failure_details["next_step_sent"] = json!(false);
                    let mut output = command_sequence_output(
                        &slot,
                        run_id,
                        sequence_id,
                        description,
                        requested_steps,
                        completed_steps,
                        step_outputs,
                        Some(failure_details),
                    );
                    let run_open = self
                        .session
                        .run_ownership_retained(port.clone(), run_id, run_token)
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
                        .run_ownership_retained(port.clone(), run_id, run_token)
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
            .run_ownership_retained(port, run_id, run_token)
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
            &self.actor_label,
            slot.config.port.clone(),
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
                slot.config.port.clone(),
                prepared.bytes,
                operation_id,
                expected_run_id,
                run_token,
                effective_write_pacing(slot),
                Some(prepared.description.clone()),
                prepared.capture_matchers,
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
        let boundary = result
            .command_boundary
            .as_ref()
            .ok_or_else(|| CommandStepFailure {
                phase: "capture",
                error: anyhow!("command capture lost its authoritative write boundary"),
            })?;
        let interfered = boundary.interfered;
        let echo_missing = boundary.echo_required && !boundary.echo_observed;
        let echo_ambiguous = boundary.ambiguous_echo_retained;
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: DEFAULT_TEXT_CHARS,
                include_raw: false,
                // Unambiguous echoes were consumed by collect_after_write.
                // Ambiguous plain-CRLF wraps remain visible and warning-tagged
                // so rendering cannot silently discard real RX evidence.
                echo: None,
                collapse_repeats: true,
                include_events: false,
                match_excerpt: None,
            },
        );
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
            echo_ambiguous,
            rx_event_count,
        );
        let (capture_completion, completion_detail) =
            command_capture_completion(&result.completion);
        let authoritative_capture = self
            .session
            .record_command_capture(
                slot.config.port.clone(),
                CommandCaptureReport {
                    daemon_epoch: slot.daemon_epoch,
                    generation: slot.generation,
                    run_id: expected_run_id,
                    operation_id,
                    tx_event_seq: write.event_seq,
                    evidence_from_seq: write.event_seq,
                    evidence_through_seq: last_seq.max(write.event_seq),
                    completion: capture_completion,
                    completion_detail,
                    confidence: command_capture_confidence(confidence),
                },
            )
            .await
            .map_err(|error| CommandStepFailure {
                phase: "record_capture",
                error: anyhow!(
                    "the command write was confirmed at event {}, but seriald did not persist \
                     its completed capture boundary; do not blindly resend the command. Inspect \
                     the TX/RX timeline and retry only the evidence-recording workflow: {error}",
                    write.event_seq
                ),
            })?;
        let cursor = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: last_seq,
        };
        self.remember_live_cursor(&slot.config.port, cursor.clone());
        let mut output = json!({
            "port": slot.config.port,
            "write": command_write_status(echo_missing, echo_ambiguous),
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
            "authoritative_capture": authoritative_capture,
            "description": prepared.description,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": last_seq}
        });
        if echo_ambiguous {
            output["echo_retained"] = json!(true);
        }
        let no_rx = command_has_no_rx(rx_event_count, boundary.echo_observed);
        attach_capture_warnings(
            &mut output,
            &result.completion,
            result.truncated,
            rendered.text_truncated,
            gap,
            interfered,
            echo_missing,
            echo_ambiguous,
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
            echo_ambiguous,
            no_rx,
        })
    }

    async fn signal(&self, args: SignalArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot_online_for_physical_action(&run_use.port).await?;
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
                slot.config.port.clone(),
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
            &slot.config.port,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: sent.event_seq,
            },
        );
        Ok(json!({
            "port": slot.config.port,
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
        let active_run = matching_active_run(slot, expected_run_id, "signal")?;
        let operation_id = Uuid::new_v4();
        let byte_count = bytes.len();
        let write = match self
            .session
            .write(
                slot.config.port.clone(),
                bytes,
                operation_id,
                expected_run_id,
                run_token,
                effective_write_pacing(slot),
                None,
                Vec::new(),
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
            &slot.config.port,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: write.event_seq,
            },
        );
        Ok(json!({
            "port": slot.config.port,
            "write": "confirmed",
            "kind": label,
            "bytes": byte_count,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": write.event_seq}
        }))
    }

    async fn run_start(&self, args: RunStartArgs) -> Result<Value> {
        let _write_guard = self.write_guard(&args.port).await;
        let slot = self.slot_online(&args.port).await?;
        if let Some(run) = slot.active_run {
            bail!("port already has active Run {} ({})", run.id, run.label);
        }
        let started = self
            .session
            .start_run_with_handle(args.port.clone(), args.label, BTreeMap::new())
            .await?;
        let run = started.run;
        self.remember_live_cursor(
            &slot.config.port,
            Cursor {
                epoch: slot.daemon_epoch,
                after_seq: run.start_seq,
            },
        );
        Ok(json!({
            "port": args.port,
            "approval_id": started.approval_id,
            "run_id": run.id,
            "run_handle": started.run_handle,
            "macro_catalog": self.macro_context().await,
            "cursor": {"epoch": slot.daemon_epoch, "after_seq": run.start_seq},
            "cleanup_required": "Call run_end before the final reply unless deliberately handing this live Run to a continuing agent workflow."
        }))
    }

    async fn run_end(&self, args: RunEndArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot(&run_use.port).await?;
        matching_active_run(&slot, run_use.run_id, "run_end")?;
        match args.outcome {
            RunEndOutcome::Completed => {
                let ended = self
                    .session
                    .end_run(run_use.port.clone(), run_use.run_id, run_use.run_token)
                    .await?;
                Ok(run_end_output(
                    run_use.port,
                    ended.id,
                    args.run_handle,
                    RunEndOutcome::Completed,
                    "best_effort",
                ))
            }
            RunEndOutcome::Aborted => {
                self.session
                    .abort_run(run_use.port.clone(), run_use.run_id, run_use.run_token)
                    .await?;
                Ok(run_end_output(
                    run_use.port,
                    run_use.run_id,
                    args.run_handle,
                    RunEndOutcome::Aborted,
                    "released",
                ))
            }
        }
    }

    async fn slot(&self, port: &str) -> Result<SlotSnapshot> {
        self.status()
            .await?
            .ports
            .into_iter()
            .find(|slot| slot.config.port == port)
            .with_context(|| format!("unknown port {port:?}"))
    }

    async fn status(&self) -> Result<StatusResponse> {
        let status = self.api.status().await?;
        ensure_protocol_compatible(&status)?;
        Ok(status)
    }

    async fn slot_online(&self, port: &str) -> Result<SlotSnapshot> {
        let slot = self.slot(port).await?;
        if slot.session_state != SessionState::Online {
            bail!(
                "port {port:?} is {:?}: {}",
                slot.session_state,
                slot.state_reason.as_deref().unwrap_or("no reason reported")
            );
        }
        Ok(slot)
    }

    async fn slot_online_for_physical_action(&self, port: &str) -> Result<SlotSnapshot> {
        let status = self.status().await?;
        ensure_serial_context_precondition_supported(&status)?;
        let slot = status
            .ports
            .into_iter()
            .find(|slot| slot.config.port == port)
            .with_context(|| format!("unknown port {port:?}"))?;
        if slot.session_state != SessionState::Online {
            bail!(
                "port {port:?} is {:?}: {}",
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
                &slot.config.port,
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
                    &slot.config.port,
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
            &slot.config.port,
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
                &slot.config.port,
                run_id,
                &diagnosis,
                no_bytes_written,
            ));
        }
        anyhow!(
            "human_takeover_or_control_revoked: port {:?} Run {} lost fenced serial control; \
             taken_over_by=unknown; run_id={}; no_bytes_written={}; start a new Run only after \
             the current owner releases control and the DUT model/state is reconfirmed: {}",
            slot.config.port,
            run_id,
            run_id,
            no_bytes_written,
            error
        )
    }

    fn live_cursor(&self, port: &str) -> Option<Cursor> {
        self.live_cursors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(port)
            .cloned()
    }

    fn remember_live_cursor(&self, port: &str, cursor: Cursor) {
        remember_live_cursor(
            &mut self
                .live_cursors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            port,
            cursor,
        );
    }

    async fn write_guard(&self, port: &str) -> OwnedMutexGuard<()> {
        let lock = self
            .write_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(port.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(())))
            .clone();
        lock.lock_owned().await
    }
}

fn tool_observes_serial_context(tool_name: &str, output: &Value) -> bool {
    tool_name == "read"
        && output.get("scope").and_then(Value::as_str) != Some("archive")
        && output
            .get("user_command_acknowledged")
            .and_then(Value::as_bool)
            != Some(false)
}

fn pending_human_read_cursor(slot: &SlotSnapshot) -> Option<Cursor> {
    let context = slot.run_context.as_ref()?;
    if context.revision <= context.acknowledged_revision {
        return None;
    }
    let human_command_seq = context.last_human_command_seq?;
    Some(Cursor {
        epoch: slot.daemon_epoch,
        after_seq: human_command_seq.saturating_sub(1),
    })
}

fn live_read_covers_human_tx(
    response: &EventQueryResponse,
    daemon_epoch: Uuid,
    human_command_seq: u64,
) -> bool {
    response.events.iter().any(|event| {
        event.seq == human_command_seq
            && event.daemon_epoch == daemon_epoch
            && event.direction == Direction::Tx
            && event.metadata.get("human_command").and_then(Value::as_bool) == Some(true)
    })
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
            if matches!(
                event.kind,
                EventKind::PortReconfigured | EventKind::PortRemoved
            ) {
                for field in [
                    "port",
                    "source",
                    "previous_model_family",
                    "new_model_family",
                    "previous_model_name",
                    "new_model_name",
                ] {
                    if let Some(value) = event.metadata.get(field) {
                        summary[field] = value.clone();
                    }
                }
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
    port: &str,
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
        "{code}: port {port:?} Run {run_id} was aborted; reason={:?}; \
         taken_over_by={taken_over_by:?}; run_id={run_id}; \
         no_bytes_written={no_bytes_written}; start a new Run only after the current owner \
         releases control and the DUT model/state is reconfirmed",
        diagnosis.reason,
    )
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

fn remember_live_cursor(cursors: &mut BTreeMap<String, Cursor>, port: &str, cursor: Cursor) {
    match cursors.get_mut(port) {
        Some(current) if current.epoch == cursor.epoch => {
            current.after_seq = current.after_seq.max(cursor.after_seq);
        }
        Some(current) => *current = cursor,
        None => {
            cursors.insert(port.to_string(), cursor);
        }
    }
}

fn parse<T: for<'de> Deserialize<'de>>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|error| anyhow!("invalid tool arguments: {error}"))
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
            "{operation} expected Run {expected_run_id}, but port has active Run {}; refusing to \
             adopt or modify another caller's Run",
            active.id
        );
    }
    Ok(active)
}

fn run_end_output(
    port: String,
    run_id: Uuid,
    run_handle: String,
    outcome: RunEndOutcome,
    control_release: &'static str,
) -> Value {
    json!({
        "port": port,
        "run_id": run_id,
        "run_handle": run_handle,
        "outcome": outcome,
        "run_open": false,
        "control_release": control_release,
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
                if expect.len() > MAX_COMMAND_CAPTURE_DETAIL_BYTES {
                    bail!(
                        "steps[{index}].expect must not exceed {MAX_COMMAND_CAPTURE_DETAIL_BYTES} UTF-8 bytes"
                    );
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
    let (patterns, until_regex, completion_mode, capture_matchers) =
        requested_completion(expect, regex, slot, true)?;
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
        capture_matchers,
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
    if executed.echo_missing || executed.echo_ambiguous {
        return stop(
            "echo_uncertain",
            if executed.echo_ambiguous {
                "the command echo used an ambiguous plain-CRLF wrap; raw RX was retained"
            } else {
                "the configured command echo was not observed completely"
            },
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
        "port": slot.config.port,
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

fn validate_monitor_matchers(matchers: &[MonitorMatcher]) -> Result<()> {
    if matchers.is_empty() || matchers.len() > MAX_MONITOR_MATCHERS {
        bail!("matchers must contain 1-{MAX_MONITOR_MATCHERS} conditions");
    }
    let mut total_bytes = 0usize;
    for (index, matcher) in matchers.iter().enumerate() {
        let value = match matcher {
            MonitorMatcher::Contains(value) | MonitorMatcher::Regex(value) => value,
        };
        if value.is_empty() {
            bail!("matchers[{index}].value must not be empty");
        }
        if value.len() > MAX_MONITOR_PATTERN_BYTES {
            bail!(
                "matchers[{index}].value must not exceed {MAX_MONITOR_PATTERN_BYTES} UTF-8 bytes"
            );
        }
        total_bytes = total_bytes.saturating_add(value.len());
        if let MonitorMatcher::Regex(regex) = matcher {
            let field = format!("matchers[{index}].value");
            compile_regex(regex, &field)?;
            let hir = ParserBuilder::new()
                .utf8(false)
                .build()
                .parse(regex)
                .with_context(|| format!("{field} is not a valid regex"))?;
            match hir.properties().minimum_len() {
                Some(0) => bail!("matchers[{index}] regex must consume at least one byte"),
                None => bail!("matchers[{index}] regex cannot match any byte sequence"),
                Some(_) => {}
            }
        }
    }
    if total_bytes > MAX_MONITOR_TOTAL_PATTERN_BYTES {
        bail!("matcher values must not exceed {MAX_MONITOR_TOTAL_PATTERN_BYTES} total UTF-8 bytes");
    }
    Ok(())
}

fn create_monitor_request(args: MonitorStartArgs) -> Result<CreateMonitorRequest> {
    validate_monitor_matchers(&args.matchers)?;
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
            "port": args.port,
            "matchers": args.matchers,
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
    json!({
        "monitor_id": monitor.get("id").cloned().unwrap_or(Value::Null),
        "port": spec.get("port").cloned().unwrap_or(Value::Null),
        "status": monitor.get("status").cloned().unwrap_or(Value::Null),
        "severity": spec.get("severity").cloned().unwrap_or(Value::Null),
        "description": spec.get("description").cloned().unwrap_or(Value::Null),
        "matchers": spec.get("matchers").cloned().unwrap_or_else(|| json!([])),
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
        "port": incident.get("port").cloned().unwrap_or(Value::Null),
        "severity": incident.get("severity").cloned().unwrap_or(Value::Null),
        "description": incident.get("description").cloned().unwrap_or(Value::Null),
        "matches": incident.get("matches").cloned().unwrap_or_else(|| json!([])),
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
                bail!("cursor is ahead of port head_seq {}", slot.head_seq);
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
    render_response_with_exclusions(slot, query_epoch, response, options, scope, &[])
}

fn render_response_with_exclusions(
    slot: &SlotSnapshot,
    query_epoch: Uuid,
    response: serial_protocol::EventQueryResponse,
    options: RenderOptions,
    scope: &str,
    exclusions: &[String],
) -> Value {
    let rendered =
        crate::render::render_events_with_exclusions(&response.events, options, exclusions);
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
        "port": slot.config.port,
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
    if rendered.excluded_lines > 0 {
        output["excluded_lines"] = json!(rendered.excluded_lines);
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

fn command_capture_completion(
    completion: &Completion,
) -> (CommandCaptureCompletionKind, Option<String>) {
    match completion {
        Completion::Pattern(value) => (CommandCaptureCompletionKind::Literal, Some(value.clone())),
        Completion::Prompt(value) => (CommandCaptureCompletionKind::Prompt, Some(value.clone())),
        Completion::Regex(value) => (CommandCaptureCompletionKind::Regex, Some(value.clone())),
        Completion::Quiet => (CommandCaptureCompletionKind::Quiet, None),
        Completion::Signal(value) => (CommandCaptureCompletionKind::Signal, Some(value.clone())),
        Completion::RunAborted { reason, .. } => (
            CommandCaptureCompletionKind::RunAborted,
            Some(bounded_internal_capture_detail(reason)),
        ),
        Completion::Timeout => (CommandCaptureCompletionKind::Timeout, None),
        Completion::Disconnected(reason) => (
            CommandCaptureCompletionKind::Disconnected,
            Some(bounded_internal_capture_detail(reason)),
        ),
    }
}

/// Internal transport/Run diagnostics do not come from the validated matcher
/// input path and can include an arbitrarily long nested error. Bound them
/// before the post-TX capture RPC, without ever truncating matcher variants.
fn bounded_internal_capture_detail(value: &str) -> String {
    if value.len() <= MAX_COMMAND_CAPTURE_DETAIL_BYTES {
        return value.to_string();
    }
    let mut end = MAX_COMMAND_CAPTURE_DETAIL_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn command_capture_confidence(confidence: &str) -> CommandCaptureConfidence {
    match confidence {
        "high" => CommandCaptureConfidence::High,
        "medium" => CommandCaptureConfidence::Medium,
        "low" => CommandCaptureConfidence::Low,
        "partial" => CommandCaptureConfidence::Partial,
        "interfered" => CommandCaptureConfidence::Interfered,
        "incomplete" => CommandCaptureConfidence::Incomplete,
        "unreliable" => CommandCaptureConfidence::Unreliable,
        other => unreachable!("command_confidence returned unknown label {other:?}"),
    }
}

fn command_write_status(echo_missing: bool, echo_ambiguous: bool) -> &'static str {
    if echo_missing || echo_ambiguous {
        "uncertain"
    } else {
        "confirmed"
    }
}

fn command_confidence(
    completion: &Completion,
    output_truncated: bool,
    has_gap: bool,
    interfered: bool,
    echo_missing: bool,
    echo_ambiguous: bool,
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
    } else if echo_missing || echo_ambiguous {
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
    echo_ambiguous: bool,
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
    if echo_ambiguous {
        warnings.push(
            "plain-CRLF echo wrap was ambiguous; original RX was retained in command text".into(),
        );
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
    slot.effective_write_eol.as_deref().unwrap_or("\r")
}

fn effective_echo_mode(slot: &SlotSnapshot) -> EchoMode {
    slot.effective_echo.unwrap_or(EchoMode::Auto)
}

fn effective_write_pacing(slot: &SlotSnapshot) -> WritePacing {
    slot.effective_write_pacing
        .unwrap_or_else(|| WritePacing::resolve(None, &serial_protocol::SerialSettings::default()))
}

fn effective_prompts(slot: &SlotSnapshot) -> (Option<String>, Option<String>) {
    (
        slot.effective_shell_prompt.clone(),
        slot.effective_uboot_prompt.clone(),
    )
}

type RequestedCompletion = (
    Vec<CompletionPattern>,
    Option<regex::Regex>,
    String,
    Vec<CommandCaptureMatcher>,
);

fn requested_completion(
    expect: Option<&str>,
    regex: Option<&str>,
    slot: &SlotSnapshot,
    use_profile_prompts: bool,
) -> Result<RequestedCompletion> {
    if expect.is_some() && regex.is_some() {
        bail!("expect and regex are alternative completion boundaries; choose one");
    }
    if let Some(regex) = regex {
        return Ok((
            Vec::new(),
            Some(compile_regex(regex, "regex")?),
            "regex".into(),
            vec![CommandCaptureMatcher {
                kind: CommandCaptureMatcherKind::Regex,
                value: regex.to_string(),
            }],
        ));
    }
    if let Some(expect) = expect {
        if expect.is_empty() {
            bail!("expect must not be empty");
        }
        if expect.len() > MAX_COMMAND_CAPTURE_DETAIL_BYTES {
            bail!("expect must not exceed {MAX_COMMAND_CAPTURE_DETAIL_BYTES} UTF-8 bytes");
        }
        return Ok((
            vec![CompletionPattern::Literal(expect.to_string())],
            None,
            "expect".into(),
            vec![CommandCaptureMatcher {
                kind: CommandCaptureMatcherKind::Contains,
                value: expect.to_string(),
            }],
        ));
    }

    let (shell_prompt, uboot_prompt) = effective_prompts(slot);
    let capture_matchers: Vec<_> = if use_profile_prompts {
        [
            shell_prompt.as_ref().map(|value| CommandCaptureMatcher {
                kind: CommandCaptureMatcherKind::ShellPrompt,
                value: value.clone(),
            }),
            uboot_prompt.as_ref().map(|value| CommandCaptureMatcher {
                kind: CommandCaptureMatcherKind::UbootPrompt,
                value: value.clone(),
            }),
        ]
        .into_iter()
        .flatten()
        .collect()
    } else {
        Vec::new()
    };
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
    Ok((patterns, None, mode.into(), capture_matchers))
}

#[cfg(test)]
pub(crate) mod completion_tests {
    use super::*;

    pub(crate) fn slot(shell: Option<&str>, uboot: Option<&str>) -> SlotSnapshot {
        SlotSnapshot {
            config: serial_protocol::SlotConfig {
                port: "/dev/cu.usbserial-210".into(),
                transport_profile: None,
                model_profile: Some("TL-AS7230 1.0".into()),
                model_family: Some("TL-AS7230".into()),
                model_name: Some("TL-AS7230-W 1.0".into()),
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
            target_activity: serial_protocol::TargetActivity::Active,
            last_rx_wall_time_ns: None,
            rx_offset: 0,
            tx_offset: 0,
            rx_overflow_bytes: 0,
            control: None,
            pending_run_start: None,
            active_run: None,
            run_context: None,
            active_trigger: None,
            logging: serial_protocol::LoggingState::Healthy,
            effective_shell_prompt: shell.map(str::to_owned),
            effective_uboot_prompt: uboot.map(str::to_owned),
            effective_write_eol: Some("\r".into()),
            effective_echo: Some(EchoMode::Auto),
            effective_transport: None,
            effective_write_pacing: None,
        }
    }

    #[test]
    fn profile_prompt_matchers_preserve_kind_and_value() {
        let (_, _, mode, matchers) =
            requested_completion(None, None, &slot(Some("root# "), Some("U-Boot> ")), true)
                .unwrap();
        assert_eq!(mode, "prompt");
        assert_eq!(
            matchers,
            vec![
                CommandCaptureMatcher {
                    kind: CommandCaptureMatcherKind::ShellPrompt,
                    value: "root# ".into(),
                },
                CommandCaptureMatcher {
                    kind: CommandCaptureMatcherKind::UbootPrompt,
                    value: "U-Boot> ".into(),
                },
            ]
        );
    }

    #[test]
    fn device_summary_exposes_identity_and_prompts_without_profile_or_transport_details() {
        let summary = slot_summary(&slot(Some("root# "), Some("U-Boot> ")));
        assert_eq!(summary["model_family"], "TL-AS7230");
        assert_eq!(summary["model_name"], "TL-AS7230-W 1.0");
        assert_eq!(summary["command_prompts"]["shell"], "root# ");
        assert_eq!(summary["command_prompts"]["uboot"], "U-Boot> ");
        for hidden in [
            "transport_profile",
            "model_profile",
            "effective_transport",
            "effective_device",
            "write_eol",
            "echo",
            "write_pacing",
        ] {
            assert!(summary.get(hidden).is_none(), "unexpected {hidden}");
        }
        let serialized = serde_json::to_string(&summary).unwrap();
        assert!(!serialized.contains("TL-AS7230 1.0"));
        for hidden in [
            "transport_profile",
            "model_profile",
            "effective_transport",
            "effective_device",
            "write_eol",
            "\"echo\"",
            "write_pacing",
            "write_chunk",
        ] {
            assert!(!serialized.contains(hidden), "leaked {hidden}");
        }
    }

    #[test]
    fn run_end_outcome_defaults_and_accepts_only_completed_or_aborted() {
        let defaulted: RunEndArgs =
            serde_json::from_value(json!({"run_handle": "abcdefghijklmnopqrstuv"})).unwrap();
        assert_eq!(defaulted.outcome, RunEndOutcome::Completed);

        let completed: RunEndArgs = serde_json::from_value(json!({
            "run_handle": "abcdefghijklmnopqrstuv",
            "outcome": "completed"
        }))
        .unwrap();
        assert_eq!(completed.outcome, RunEndOutcome::Completed);

        let aborted: RunEndArgs = serde_json::from_value(json!({
            "run_handle": "abcdefghijklmnopqrstuv",
            "outcome": "aborted"
        }))
        .unwrap();
        assert_eq!(aborted.outcome, RunEndOutcome::Aborted);

        assert!(
            serde_json::from_value::<RunEndArgs>(json!({
                "run_handle": "abcdefghijklmnopqrstuv",
                "outcome": "cancelled"
            }))
            .is_err()
        );
    }

    #[test]
    fn run_end_outputs_explicit_terminal_outcome_and_release_state() {
        let run_id = Uuid::new_v4();
        let completed = run_end_output(
            "COM4".into(),
            run_id,
            "abcdefghijklmnopqrstuv".into(),
            RunEndOutcome::Completed,
            "best_effort",
        );
        assert_eq!(completed["outcome"], "completed");
        assert_eq!(completed["run_open"], false);
        assert_eq!(completed["control_release"], "best_effort");

        let aborted = run_end_output(
            "COM4".into(),
            run_id,
            "abcdefghijklmnopqrstuv".into(),
            RunEndOutcome::Aborted,
            "released",
        );
        assert_eq!(aborted["outcome"], "aborted");
        assert_eq!(aborted["run_open"], false);
        assert_eq!(aborted["control_release"], "released");
    }

    #[test]
    fn aborted_run_end_refuses_a_foreign_run() {
        let expected_run_id = Uuid::new_v4();
        let foreign_run_id = Uuid::new_v4();
        let mut foreign = slot(None, None);
        foreign.active_run = Some(serial_protocol::RunInfo {
            id: foreign_run_id,
            owner: Actor {
                id: "agent:foreign".into(),
                label: "foreign".into(),
                kind: ActorKind::Agent,
            },
            label: "foreign run".into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 1,
            end_seq: None,
            metadata: BTreeMap::new(),
        });
        let error = matching_active_run(&foreign, expected_run_id, "run_end")
            .unwrap_err()
            .to_string();
        assert!(error.contains(&foreign_run_id.to_string()));
        assert!(error.contains("refusing to adopt or modify another caller's Run"));
    }

    #[test]
    fn explicit_and_quiet_boundaries_produce_exact_audit_matchers() {
        let (_, _, _, contains) =
            requested_completion(Some("Password:"), None, &slot(None, None), true).unwrap();
        assert_eq!(contains[0].kind, CommandCaptureMatcherKind::Contains);
        assert_eq!(contains[0].value, "Password:");

        let (_, _, _, regex) =
            requested_completion(None, Some("ready\\s+#"), &slot(None, None), true).unwrap();
        assert_eq!(regex[0].kind, CommandCaptureMatcherKind::Regex);
        assert_eq!(regex[0].value, "ready\\s+#");

        let (_, _, mode, quiet) =
            requested_completion(None, None, &slot(None, None), true).unwrap();
        assert_eq!(mode, "quiet");
        assert!(quiet.is_empty());
    }

    #[test]
    fn explicit_matchers_preserve_control_characters_through_the_shared_bound() {
        let matcher = format!("{}\n\u{0000}tail", "界".repeat(100));
        assert!(matcher.len() > MAX_COMMAND_DESCRIPTION_BYTES);

        let (_, _, _, contains) =
            requested_completion(Some(&matcher), None, &slot(None, None), true).unwrap();
        assert_eq!(contains[0].value, matcher);

        let (_, _, _, regex) =
            requested_completion(None, Some(&matcher), &slot(None, None), true).unwrap();
        assert_eq!(regex[0].value, matcher);
        let (kind, detail) = command_capture_completion(&Completion::Regex(matcher.clone()));
        assert_eq!(kind, CommandCaptureCompletionKind::Regex);
        assert_eq!(detail.as_deref(), Some(matcher.as_str()));

        let maximum = "x".repeat(MAX_COMMAND_CAPTURE_DETAIL_BYTES);
        assert!(requested_completion(Some(&maximum), None, &slot(None, None), true).is_ok());
        let oversized = "x".repeat(MAX_COMMAND_CAPTURE_DETAIL_BYTES + 1);
        assert!(requested_completion(Some(&oversized), None, &slot(None, None), true).is_err());
        assert!(requested_completion(None, Some(&oversized), &slot(None, None), true).is_err());
    }

    #[test]
    fn recent_context_reports_human_model_identity_switches_without_profile_names() {
        let epoch = Uuid::new_v4();
        let event = serial_protocol::TimelineEvent {
            port: "COM4".into(),
            daemon_epoch: epoch,
            seq: 9,
            generation: 1,
            wall_time_ns: 1,
            monotonic_time_ns: 1,
            kind: EventKind::PortReconfigured,
            direction: Direction::None,
            actor: Some(Actor {
                id: "system:seriald".into(),
                label: "seriald".into(),
                kind: ActorKind::System,
            }),
            run_id: None,
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: Vec::new(),
            metadata: BTreeMap::from([
                ("port".into(), json!("COM4")),
                ("source".into(), json!("human:desktop")),
                ("previous_model_profile".into(), json!("private-profile-a")),
                ("new_model_profile".into(), json!("private-profile-b")),
                ("previous_model_family".into(), json!("TL-AS7230")),
                ("new_model_family".into(), json!("TL-AS7250")),
                ("previous_model_name".into(), json!("TL-AS7230-W 1.0")),
                ("new_model_name".into(), json!("TL-AS7230-F4GE 1.0")),
            ]),
            durable: true,
        };
        let context = summarize_recent_context(
            EventQueryResponse {
                events: vec![event],
                next_cursor: None,
                truncated: false,
                first_available_seq: Some(1),
                gaps: Vec::new(),
            },
            Some("agent:self"),
            &Cursor {
                epoch,
                after_seq: 8,
            },
            &Cursor {
                epoch,
                after_seq: 9,
            },
        )
        .unwrap();
        assert_eq!(context["events"][0]["source"], "human:desktop");
        assert!(context["events"][0].get("previous_model_profile").is_none());
        assert!(context["events"][0].get("new_model_profile").is_none());
        assert_eq!(context["events"][0]["previous_model_family"], "TL-AS7230");
        assert_eq!(context["events"][0]["new_model_family"], "TL-AS7250");
        assert_eq!(context["events"][0]["new_model_name"], "TL-AS7230-F4GE 1.0");
    }

    #[test]
    fn ambiguous_plain_crlf_echo_is_uncertain_medium_and_warning_tagged() {
        let completion = Completion::Pattern("dut# ".into());
        assert_eq!(command_write_status(false, true), "uncertain");
        assert_eq!(
            command_confidence(&completion, false, false, false, false, true, 2),
            "medium"
        );

        let mut output = json!({"echo_retained": true});
        attach_capture_warnings(
            &mut output,
            &completion,
            false,
            false,
            false,
            false,
            false,
            true,
            false,
        );
        assert_eq!(output["echo_retained"], true);
        assert!(
            output["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|warning| warning
                    .as_str()
                    .is_some_and(|text| text.contains("ambiguous")))
        );

        let executed = ExecutedCommandStep {
            output,
            completion,
            cursor: Cursor {
                epoch: Uuid::new_v4(),
                after_seq: 10,
            },
            truncated: false,
            gap: false,
            interfered: false,
            echo_missing: false,
            echo_ambiguous: true,
            no_rx: false,
        };
        let stop = command_sequence_stop(&executed, true)
            .expect("an ambiguous intermediate echo must stop the sequence");
        assert_eq!(stop.code, "echo_uncertain");
    }

    #[test]
    fn command_capture_report_maps_every_completion_and_confidence_exactly() {
        let run_id = Uuid::new_v4();
        let cases = [
            (
                Completion::Pattern("ready".into()),
                CommandCaptureCompletionKind::Literal,
                Some("ready"),
            ),
            (
                Completion::Prompt("root# ".into()),
                CommandCaptureCompletionKind::Prompt,
                Some("root# "),
            ),
            (
                Completion::Regex("ready\\s+#".into()),
                CommandCaptureCompletionKind::Regex,
                Some("ready\\s+#"),
            ),
            (Completion::Quiet, CommandCaptureCompletionKind::Quiet, None),
            (
                Completion::Signal("ctrl_c".into()),
                CommandCaptureCompletionKind::Signal,
                Some("ctrl_c"),
            ),
            (
                Completion::RunAborted {
                    run_id,
                    reason: "takeover".into(),
                },
                CommandCaptureCompletionKind::RunAborted,
                Some("takeover"),
            ),
            (
                Completion::Timeout,
                CommandCaptureCompletionKind::Timeout,
                None,
            ),
            (
                Completion::Disconnected("closed".into()),
                CommandCaptureCompletionKind::Disconnected,
                Some("closed"),
            ),
        ];
        for (completion, expected_kind, expected_detail) in cases {
            let (kind, detail) = command_capture_completion(&completion);
            assert_eq!(kind, expected_kind);
            assert_eq!(detail.as_deref(), expected_detail);
        }
        for (label, expected) in [
            ("high", CommandCaptureConfidence::High),
            ("medium", CommandCaptureConfidence::Medium),
            ("low", CommandCaptureConfidence::Low),
            ("partial", CommandCaptureConfidence::Partial),
            ("interfered", CommandCaptureConfidence::Interfered),
            ("incomplete", CommandCaptureConfidence::Incomplete),
            ("unreliable", CommandCaptureConfidence::Unreliable),
        ] {
            assert_eq!(command_capture_confidence(label), expected);
        }
    }

    #[test]
    fn command_capture_bounds_internal_reasons_but_never_truncates_legal_matchers() {
        let legal_matcher = format!(
            "{}\n\u{0000}",
            "x".repeat(MAX_COMMAND_CAPTURE_DETAIL_BYTES - 2)
        );
        assert_eq!(legal_matcher.len(), MAX_COMMAND_CAPTURE_DETAIL_BYTES);
        for completion in [
            Completion::Pattern(legal_matcher.clone()),
            Completion::Prompt(legal_matcher.clone()),
            Completion::Regex(legal_matcher.clone()),
        ] {
            let (_, detail) = command_capture_completion(&completion);
            assert_eq!(detail.as_deref(), Some(legal_matcher.as_str()));
        }

        let oversized_reason =
            format!("{}界tail", "r".repeat(MAX_COMMAND_CAPTURE_DETAIL_BYTES - 1));
        for completion in [
            Completion::Disconnected(oversized_reason.clone()),
            Completion::RunAborted {
                run_id: Uuid::new_v4(),
                reason: oversized_reason.clone(),
            },
        ] {
            let (_, detail) = command_capture_completion(&completion);
            let detail = detail.expect("internal completion has a bounded detail");
            assert!(detail.len() <= MAX_COMMAND_CAPTURE_DETAIL_BYTES);
            assert!(oversized_reason.starts_with(&detail));
            assert!(detail.is_char_boundary(detail.len()));
        }
    }

    #[test]
    fn human_command_gate_requires_exact_live_tx_and_only_read_can_acknowledge() {
        let epoch = Uuid::new_v4();
        let human_seq = 41;
        let human_tx = serial_protocol::TimelineEvent {
            port: "COM4".into(),
            daemon_epoch: epoch,
            seq: human_seq,
            generation: 1,
            wall_time_ns: 1,
            monotonic_time_ns: 1,
            kind: EventKind::Tx,
            direction: Direction::Tx,
            actor: Some(Actor {
                id: "human:test".into(),
                label: "human".into(),
                kind: ActorKind::Human,
            }),
            run_id: Some(Uuid::new_v4()),
            operation_id: Some(Uuid::new_v4()),
            stream_offset_start: Some(0),
            stream_offset_end: Some(1),
            data: vec![b'\n'],
            metadata: BTreeMap::from([("human_command".into(), json!(true))]),
            durable: true,
        };
        let response = EventQueryResponse {
            events: vec![human_tx.clone()],
            next_cursor: Some(Cursor {
                epoch,
                after_seq: human_seq,
            }),
            truncated: false,
            first_available_seq: Some(1),
            gaps: Vec::new(),
        };
        assert!(live_read_covers_human_tx(&response, epoch, human_seq));
        assert!(!live_read_covers_human_tx(&response, epoch, human_seq + 1));
        let mut not_human = response.clone();
        not_human.events[0].metadata.clear();
        assert!(!live_read_covers_human_tx(&not_human, epoch, human_seq));

        assert!(tool_observes_serial_context(
            "read",
            &json!({"scope":"tail","user_command_acknowledged":true})
        ));
        assert!(!tool_observes_serial_context(
            "read",
            &json!({"scope":"tail","user_command_acknowledged":false})
        ));
        assert!(!tool_observes_serial_context(
            "read",
            &json!({"scope":"archive"})
        ));
        assert!(!tool_observes_serial_context(
            "wait",
            &json!({"scope":"tail"})
        ));

        let mut gated = slot(None, None);
        gated.daemon_epoch = epoch;
        gated.head_seq = human_seq + 500;
        gated.run_context = Some(serial_protocol::RunContextState {
            run_id: Uuid::new_v4(),
            revision: 4,
            last_human_command_seq: Some(human_seq),
            acknowledged_revision: 3,
            acknowledged_through_seq: Some(human_seq.saturating_sub(1)),
        });
        assert_eq!(
            pending_human_read_cursor(&gated),
            Some(Cursor {
                epoch,
                after_seq: human_seq - 1,
            }),
            "a wait cursor beyond the Human TX must not make the next live read start too late"
        );

        gated
            .run_context
            .as_mut()
            .expect("Run context")
            .acknowledged_revision = 4;
        assert_eq!(pending_human_read_cursor(&gated), None);

        let mut evicted_window = response;
        evicted_window.events = vec![human_tx];
        evicted_window.events[0].seq = human_seq + 400;
        evicted_window.next_cursor = Some(Cursor {
            epoch,
            after_seq: human_seq + 500,
        });
        assert!(
            !live_read_covers_human_tx(&evicted_window, epoch, human_seq),
            "a newer tail after ring eviction must never masquerade as the exact Human TX"
        );
    }

    #[test]
    fn user_read_required_has_stable_structured_tool_error() {
        let error: anyhow::Error = UserCommandUsed {
            message: "seriald UserReadRequired".into(),
        }
        .into();
        let structured = structured_tool_error(&error).unwrap();
        assert_eq!(structured["error"]["code"], "user_command_used");
        assert_eq!(structured["error"]["no_bytes_written"], true);
        assert!(
            structured["error"]["retry_hint"]
                .as_str()
                .unwrap()
                .contains("archive reads do not clear")
        );
    }

    #[test]
    fn uncertain_physical_write_has_non_retryable_structured_tool_error() {
        let error: anyhow::Error = WriteOutcomeUncertain {
            message: "write may have reached the DUT".into(),
        }
        .into();
        let structured = structured_tool_error(&error).unwrap();
        assert_eq!(structured["error"]["source"], "seriald");
        assert_eq!(structured["error"]["code"], "write_outcome_uncertain");
        assert_eq!(structured["error"]["outcome"], "uncertain");
        assert_eq!(structured["error"]["no_bytes_written"], false);
        assert_eq!(structured["error"]["retryable"], false);
        assert_eq!(structured["error"]["automatic_retry_allowed"], false);
        assert!(
            structured["error"]["retry_hint"]
                .as_str()
                .unwrap()
                .starts_with("Do not retry automatically")
        );
    }
}

#[cfg(test)]
mod model_configuration_argument_tests {
    use super::*;

    #[test]
    fn identity_fields_are_required_and_accept_strings_or_null() {
        let selected: ModelIdentitySetArgs = serde_json::from_value(json!({
            "port": "COM4",
            "model_family": "TL-AS7230",
            "model_name": "TL-AS7230-W 1.0"
        }))
        .unwrap();
        assert_eq!(
            selected.model_family.into_option().as_deref(),
            Some("TL-AS7230")
        );
        assert_eq!(
            selected.model_name.into_option().as_deref(),
            Some("TL-AS7230-W 1.0")
        );

        let detached: ModelIdentitySetArgs = serde_json::from_value(json!({
            "port": "COM4", "model_family": null, "model_name": null
        }))
        .unwrap();
        assert_eq!(detached.model_family.into_option(), None);
        assert_eq!(detached.model_name.into_option(), None);
        assert!(
            serde_json::from_value::<ModelIdentitySetArgs>(json!({
                "port": "COM4", "model_family": null
            }))
            .is_err()
        );
    }
}

#[cfg(test)]
mod monitor_argument_tests {
    use super::*;

    #[test]
    fn monitor_regex_must_consume_bytes_on_every_match() {
        for expression in [r".*", r"\b|foo.*bar"] {
            assert!(
                validate_monitor_matchers(&[MonitorMatcher::Regex(expression.into())]).is_err()
            );
        }
        validate_monitor_matchers(&[MonitorMatcher::Regex("foo.*bar".into())]).unwrap();
    }
}

fn compile_regex(value: &str, field: &str) -> Result<regex::Regex> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    if value.len() > MAX_COMMAND_CAPTURE_DETAIL_BYTES {
        bail!("{field} must not exceed {MAX_COMMAND_CAPTURE_DETAIL_BYTES} UTF-8 bytes");
    }
    regex::Regex::new(value).with_context(|| format!("{field} is not a valid regex"))
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
            "seriald does not advertise atomic command_sequence write boundaries; no bytes were written. Install seriald and serial-mcp from the same release"
        );
    }
    Ok(())
}

fn ensure_serial_context_precondition_supported(status: &StatusResponse) -> Result<()> {
    if !status.serial_context_precondition_supported {
        bail!(
            "seriald does not advertise atomic serial-context boundaries for Write, BREAK, and Macro; no bytes were written. Install seriald and serial-mcp from the same release"
        );
    }
    Ok(())
}

fn slot_summary(slot: &SlotSnapshot) -> Value {
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
    let pending_run_start = slot.pending_run_start.as_ref().map(|approval| {
        json!({
            "id": approval.id,
            "label": approval.label,
            "requester": actor_summary(&approval.requester),
            "required_approver": actor_summary(&approval.required_approver),
            "requested_wall_time_ns": approval.requested_wall_time_ns,
            "expires_wall_time_ns": approval.expires_wall_time_ns,
        })
    });
    let run_context = slot.run_context.as_ref().map(|context| {
        json!({
            "run_id": context.run_id,
            "revision": context.revision,
            "last_human_command_seq": context.last_human_command_seq,
            "acknowledged_revision": context.acknowledged_revision,
            "acknowledged_through_seq": context.acknowledged_through_seq,
            "user_read_required": context.revision > context.acknowledged_revision,
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
        "port": slot.config.port,
        "enabled": slot.config.enabled,
        "model_family": slot.config.model_family,
        "model_name": slot.config.model_name,
        "endpoint_present": slot.endpoint_present,
        "session_state": slot.session_state,
        "state_code": slot.state_code,
        "state_reason": slot.state_reason,
        "target_activity": slot.target_activity,
        "command_prompts": {
            "shell": shell_prompt,
            "uboot": uboot_prompt
        },
        "cursor": {"epoch": slot.daemon_epoch, "after_seq": slot.head_seq},
        "generation": slot.generation,
        "control": control,
        "pending_run_start": pending_run_start,
        "active_run": active_run,
        "run_context": run_context,
        "active_trigger": active_trigger,
        "logging": slot.logging,
        "rx_overflow_bytes": slot.rx_overflow_bytes,
    })
}

fn actor_summary(actor: &serial_protocol::Actor) -> Value {
    json!({"id": actor.id, "label": actor.label, "kind": actor.kind})
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DevicesArgs {
    port: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelIdentitySetArgs {
    port: String,
    model_family: RequiredNullableString,
    model_name: RequiredNullableString,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RequiredNullableString {
    Value(String),
    Null(()),
}

impl RequiredNullableString {
    fn into_option(self) -> Option<String> {
        match self {
            Self::Value(value) => Some(value),
            Self::Null(()) => None,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    port: String,
    scope: Option<String>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    through_seq: Option<u64>,
    #[serde(default)]
    exclude_patterns: Vec<String>,
}

fn validate_read_exclusions(lines: &[String]) -> Result<()> {
    if lines.len() > 64 || lines.iter().map(String::len).sum::<usize>() > 8192 {
        bail!("exclude_patterns permits at most 64 patterns and 8192 UTF-8 bytes total");
    }
    if lines
        .iter()
        .any(|line| line.is_empty() || line.contains(['\r', '\n']) || line.len() > 1024)
    {
        bail!(
            "each exclude_patterns item must be one nonempty literal pattern (no CR/LF), at most 1024 UTF-8 bytes"
        );
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    port: String,
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
    port: String,
    matchers: Vec<MonitorMatcher>,
    description: Option<String>,
    idempotency_key: Option<Uuid>,
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct MonitorListArgs {
    port: Option<String>,
    #[serde(default)]
    include_stopped: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MonitorStopArgs {
    monitor_id: Uuid,
    #[serde(default)]
    delete_history: bool,
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
struct SignalArgs {
    run_handle: String,
    signal: String,
    duration_ms: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunStartArgs {
    port: String,
    label: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RunEndArgs {
    run_handle: String,
    #[serde(default)]
    outcome: RunEndOutcome,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RunEndOutcome {
    #[default]
    Completed,
    Aborted,
}
