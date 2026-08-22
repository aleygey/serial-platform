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
    Actor, ActorKind, CommandCaptureMatcher, CommandCaptureMatcherKind,
    ConfigureModelProfilesRequest, ConfigurePortsRequest, CreateMonitorRequest, Cursor,
    DEFAULT_TRIGGER_INTERVAL_MS, DEFAULT_TRIGGER_MAX_FIRES, DEFAULT_TRIGGER_TIMEOUT_MS, Direction,
    EchoMode, EventKind, EventQuery, EventQueryResponse, MAX_BREAK_DURATION_MS,
    MAX_COMMAND_DESCRIPTION_BYTES, MAX_MODEL_NAMES_PER_PROFILE, MAX_MONITOR_MATCHERS,
    MAX_MONITOR_PATTERN_BYTES, MAX_MONITOR_TOTAL_PATTERN_BYTES, MAX_PHYSICAL_WRITE_TIMEOUT_MS,
    MAX_TRIGGER_ACTION_BYTES, MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES,
    MAX_TRIGGER_INTERVAL_MS, MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS,
    MAX_TRIGGER_TIMEOUT_MS, MAX_TRIGGER_TOTAL_BYTES, MIN_BREAK_DURATION_MS,
    MIN_TRIGGER_INTERVAL_MS, MIN_TRIGGER_TIMEOUT_MS, ModelProfile, MonitorMatcher,
    PROTOCOL_VERSION, SequenceWritePrecondition, SessionState, SlotSnapshot, StatusResponse,
    TriggerInfo, TriggerSpec, TriggerStatus, WritePacing,
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
            "model_profiles" => self.model_profiles(parse(arguments)?).await,
            "model_profile_set" => self.model_profile_set(parse(arguments)?).await,
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
        let is_observation = matches!(tool_name, "read" | "wait");
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
    /// subsequent read/wait is the explicit acknowledgement boundary.
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
            "config_revision": status.config_revision,
            "ports": ports,
            "selection_note": "Choose a port explicitly and confirm its model_profile and model_name match the connected device before writing. A Run scopes evidence; it does not reset the device."
        }))
    }

    async fn model_profiles(&self, args: ModelProfilesArgs) -> Result<Value> {
        let status = self.status().await?;
        if let Some(port) = args.port.as_deref() {
            self.slot(port).await?;
        }
        let catalog = self.api.model_profiles().await?;
        let bindings = status
            .ports
            .into_iter()
            .filter(|slot| {
                args.port
                    .as_deref()
                    .is_none_or(|port| slot.config.port == port)
            })
            .map(|slot| {
                json!({
                    "port": slot.config.port,
                    "model_profile": slot.config.model_profile,
                    "model_name": slot.config.model_name,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "config_revision": catalog.config_revision,
            "profiles": catalog.profiles,
            "bindings": bindings,
            "port_filter": args.port,
        }))
    }

    async fn model_profile_set(&self, args: ModelProfileSetArgs) -> Result<Value> {
        let status = self.status().await?;
        let current = status
            .ports
            .iter()
            .find(|slot| slot.config.port == args.port)
            .ok_or_else(|| anyhow!("unknown serial port {:?}", args.port))?;
        let previous = current.config.model_profile.clone();
        let previous_model_name = current.config.model_name.clone();
        let next_model_profile = args.profile.as_ref().map(|profile| profile.name.clone());
        let next_model_name = match args.profile.as_ref() {
            None => {
                if matches!(&args.model_name, ModelNameUpdate::Set(Some(_))) {
                    bail!("model_name requires a non-null model profile");
                }
                None
            }
            Some(profile) => match &args.model_name {
                ModelNameUpdate::Set(Some(model_name)) => {
                    if !profile.model_names.iter().any(|name| name == model_name) {
                        bail!(
                            "model_name {:?} is not listed in model profile {:?}",
                            model_name,
                            profile.name
                        );
                    }
                    Some(model_name.clone())
                }
                ModelNameUpdate::Set(None) => None,
                ModelNameUpdate::Unspecified
                    if previous.as_deref() == Some(profile.name.as_str())
                        && previous_model_name
                            .as_ref()
                            .is_some_and(|name| profile.model_names.contains(name)) =>
                {
                    previous_model_name.clone()
                }
                ModelNameUpdate::Unspecified => None,
            },
        };

        let catalog = self.api.model_profiles().await?;
        let mut config_revision = catalog.config_revision;
        let mut final_profiles = catalog.profiles;
        let mut requires_final_catalog_write = false;
        if let Some(profile) = args.profile.as_ref() {
            for binding in &status.ports {
                if binding.config.port != args.port
                    && binding.config.model_profile.as_deref() == Some(profile.name.as_str())
                    && binding
                        .config
                        .model_name
                        .as_ref()
                        .is_some_and(|name| !profile.model_names.contains(name))
                {
                    bail!(
                        "model profile {:?} cannot remove concrete model {:?} while port {:?} still uses it",
                        profile.name,
                        binding.config.model_name,
                        binding.config.port
                    );
                }
            }
            replace_model_profile(&mut final_profiles, profile.clone());
            let mut first_profiles = final_profiles.clone();
            if previous.as_deref() == Some(profile.name.as_str())
                && previous_model_name
                    .as_ref()
                    .is_some_and(|name| !profile.model_names.contains(name))
            {
                if profile.model_names.len() >= MAX_MODEL_NAMES_PER_PROFILE {
                    bail!(
                        "model profile {:?} is at the concrete-model limit and cannot replace the current port binding atomically",
                        profile.name
                    );
                }
                let mut transition = profile.clone();
                transition
                    .model_names
                    .push(previous_model_name.clone().expect("checked concrete model"));
                replace_model_profile(&mut first_profiles, transition);
                requires_final_catalog_write = true;
            }
            config_revision = self
                .api
                .configure_model_profiles(&ConfigureModelProfilesRequest {
                    profiles: first_profiles,
                    expected_revision: Some(catalog.config_revision),
                })
                .await?
                .config_revision;
        }

        let latest = self.status().await?;
        let mut port_configs = latest
            .ports
            .into_iter()
            .map(|port| port.config)
            .collect::<Vec<_>>();
        let configured = port_configs
            .iter_mut()
            .find(|port| port.port == args.port)
            .expect("port was validated against the same daemon");
        configured.model_profile = next_model_profile;
        configured.model_name = next_model_name.clone();
        config_revision = self
            .api
            .configure_ports(&ConfigurePortsRequest {
                ports: port_configs,
                source: "agent:serial-mcp".into(),
                expected_revision: Some(config_revision),
            })
            .await?
            .config_revision;
        if requires_final_catalog_write {
            config_revision = self
                .api
                .configure_model_profiles(&ConfigureModelProfilesRequest {
                    profiles: final_profiles,
                    expected_revision: Some(config_revision),
                })
                .await?
                .config_revision;
        }
        Ok(json!({
            "port": args.port,
            "previous_model_profile": previous,
            "previous_model_name": previous_model_name,
            "model_profile": args.profile,
            "model_name": next_model_name,
            "config_revision": config_revision,
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
        let slot = self.slot(&args.port).await?;
        let scope = args.scope.as_deref().unwrap_or("tail");
        if args.through_seq.is_some() && scope != "archive" {
            bail!("through_seq is only valid with scope=archive");
        }
        let (epoch, response) = match scope {
            "tail" => {
                let response = self.api.live_tail(&args.port, 200, None).await?;
                let epoch = response
                    .next_cursor
                    .as_ref()
                    .map(|cursor| cursor.epoch)
                    .unwrap_or(slot.daemon_epoch);
                (epoch, response)
            }
            "continue" => {
                let cursor = self.live_cursor(&slot.config.port).unwrap_or(Cursor {
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
                &slot.config.port,
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
        self.remember_live_cursor(&slot.config.port, cursor.clone());
        let mut output = json!({
            "port": slot.config.port,
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
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot_online_for_physical_action(&run_use.port).await?;
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
        let active_run = matching_active_run(slot, expected_run_id, "input/signal")?;
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

    async fn trigger(&self, args: TriggerArgs) -> Result<Value> {
        let run_use = self
            .session
            .authorize_run_use(args.run_handle.clone())
            .await?;
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot_online_for_physical_action(&run_use.port).await?;
        let active_run = matching_active_run(&slot, run_use.run_id, "trigger")?;
        self.ensure_serial_context_unchanged(&slot).await?;
        let expected_run_id = run_use.run_id;
        if let Some(active) = &slot.active_trigger {
            bail!(
                "port already has Trigger {} in status {:?}; wait for it to finish or cancel it \
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
            &self.actor_label,
            run_use.port.clone(),
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
                run_use.port.clone(),
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
                        run_use.port.clone(),
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
            .run_ownership_retained(slot.config.port.clone(), expected_run_id, run_use.run_token)
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
            &slot.config.port,
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
            "port": slot.config.port,
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
                    slot.config.port.clone(),
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
                slot.config.port.clone(),
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
                    slot.config.port.clone(),
                    slot.daemon_epoch,
                    slot.generation,
                    trigger_id,
                )
                .await?;
        }
    }

    async fn run_start(&self, args: RunStartArgs) -> Result<Value> {
        let _write_guard = self.write_guard(&args.port).await;
        let slot = self.slot_online(&args.port).await?;
        if let Some(run) = slot.active_run {
            bail!("port already has active Run {} ({})", run.id, run.label);
        }
        let started = self
            .session
            .start_run_with_handle(
                args.port.clone(),
                args.label,
                BTreeMap::new(),
                Duration::from_secs(15),
            )
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
        let _write_guard = self.write_guard(&run_use.port).await;
        let slot = self.slot(&run_use.port).await?;
        matching_active_run(&slot, run_use.run_id, "run_end")?;
        let ended = self
            .session
            .end_run(run_use.port.clone(), run_use.run_id, run_use.run_token)
            .await?;
        Ok(json!({
            "port": run_use.port,
            "run_id": ended.id,
            "run_handle": args.run_handle,
            "run_open": false,
            "control_release": "best_effort"
        }))
    }

    async fn release(&self, args: ReleaseArgs) -> Result<Value> {
        let (port, run_use) = match args.run_handle.as_ref() {
            Some(handle) => {
                if !args.abort_run {
                    bail!(
                        "run_handle is valid for release only with abort_run=true; use run_end for normal completion"
                    );
                }
                if args.port.is_some() {
                    bail!("aborting release needs only run_handle and abort_run=true; omit port");
                }
                let authorized = self.session.authorize_run_use(handle.clone()).await?;
                (authorized.port.clone(), Some(authorized))
            }
            None => {
                if args.abort_run {
                    bail!("abort_run=true requires run_handle from run_start");
                }
                let port = args
                    .port
                    .context("release requires port, or run_handle with abort_run=true")?;
                (port, None)
            }
        };
        let run_capability = run_use.as_ref().map(|run| (run.run_id, run.run_token));
        let _write_guard = self.write_guard(&port).await;
        let local = self.session.local_control_state(port.clone()).await?;
        if !local.has_lease {
            // Public status may show a foreign Run, but release controls only
            // this MCP connection. Avoid consulting or modifying that Run;
            // the local no-lease release also discards any stale owned_run.
            let had_lease = self
                .session
                .release(port.clone(), false, None, true)
                .await?;
            return Ok(release_output(port, had_lease));
        }

        let current = self.slot(&port).await?;
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
            .release(port.clone(), args.abort_run, authorize, allow_stale_cleanup)
            .await?;
        Ok(release_output(port, had_lease))
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
                    "previous_model_profile",
                    "new_model_profile",
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
            "{operation} expected Run {expected_run_id}, but port has active Run {}; refusing to \
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

fn release_output(port: String, had_lease: bool) -> Value {
    json!({
        "port": port,
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

fn replace_model_profile(profiles: &mut Vec<ModelProfile>, profile: ModelProfile) {
    match profiles
        .iter()
        .position(|candidate| candidate.name == profile.name)
    {
        Some(index) => profiles[index] = profile,
        None => profiles.push(profile),
    }
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
mod completion_tests {
    use super::*;

    fn slot(shell: Option<&str>, uboot: Option<&str>) -> SlotSnapshot {
        SlotSnapshot {
            config: serial_protocol::SlotConfig {
                port: "/dev/cu.usbserial-210".into(),
                transport_profile: None,
                model_profile: Some("TL-AS7230 1.0".into()),
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
            active_run: None,
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
    fn recent_context_reports_human_model_profile_switches() {
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
                ("previous_model_profile".into(), json!("TL-AS7230 1.0")),
                ("new_model_profile".into(), json!("TL-AS7230 2.0")),
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
        assert_eq!(
            context["events"][0]["previous_model_profile"],
            "TL-AS7230 1.0"
        );
        assert_eq!(context["events"][0]["new_model_profile"], "TL-AS7230 2.0");
        assert_eq!(context["events"][0]["new_model_name"], "TL-AS7230-F4GE 1.0");
    }
}

#[cfg(test)]
mod model_profile_argument_tests {
    use super::*;

    fn arguments(model_name: Option<Value>) -> Value {
        let mut value = json!({
            "port": "COM4",
            "profile": {
                "name": "TL-AS7230",
                "model_names": ["TL-AS7230-W 1.0"]
            }
        });
        if let Some(model_name) = model_name {
            value["model_name"] = model_name;
        }
        value
    }

    #[test]
    fn model_name_argument_distinguishes_omitted_null_and_string() {
        let omitted: ModelProfileSetArgs = serde_json::from_value(arguments(None)).unwrap();
        assert_eq!(omitted.model_name, ModelNameUpdate::Unspecified);

        let cleared: ModelProfileSetArgs =
            serde_json::from_value(arguments(Some(Value::Null))).unwrap();
        assert_eq!(cleared.model_name, ModelNameUpdate::Set(None));

        let selected: ModelProfileSetArgs =
            serde_json::from_value(arguments(Some(json!("TL-AS7230-W 1.0")))).unwrap();
        assert_eq!(
            selected.model_name,
            ModelNameUpdate::Set(Some("TL-AS7230-W 1.0".into()))
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
        // Trigger writes use the port's configured physical write pacing.
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
            "seriald does not advertise atomic command_sequence write boundaries; no bytes were written. Install seriald and serial-mcp from the same release"
        );
    }
    Ok(())
}

fn ensure_serial_context_precondition_supported(status: &StatusResponse) -> Result<()> {
    if !status.serial_context_precondition_supported {
        bail!(
            "seriald does not advertise atomic serial-context boundaries for Write, BREAK, and Trigger; no bytes were written. Install seriald and serial-mcp from the same release"
        );
    }
    Ok(())
}

fn slot_summary(slot: &SlotSnapshot) -> Value {
    let effective_transport = slot.effective_transport.unwrap_or_else(|| {
        serial_protocol::resolve_transport_settings(
            &serial_protocol::SerialSettings::default(),
            None,
        )
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
        "port": slot.config.port,
        "enabled": slot.config.enabled,
        "transport_profile": slot.config.transport_profile,
        "model_profile": slot.config.model_profile,
        "model_name": slot.config.model_name,
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

fn actor_summary(actor: &serial_protocol::Actor) -> Value {
    json!({"id": actor.id, "label": actor.label, "kind": actor.kind})
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DevicesArgs {
    port: Option<String>,
}
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ModelProfilesArgs {
    port: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelProfileSetArgs {
    port: String,
    profile: Option<ModelProfile>,
    #[serde(default, deserialize_with = "deserialize_model_name_update")]
    model_name: ModelNameUpdate,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum ModelNameUpdate {
    #[default]
    Unspecified,
    Set(Option<String>),
}

fn deserialize_model_name_update<'de, D>(deserializer: D) -> Result<ModelNameUpdate, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).map(ModelNameUpdate::Set)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    port: String,
    scope: Option<String>,
    epoch: Option<Uuid>,
    after_seq: Option<u64>,
    through_seq: Option<u64>,
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
    port: String,
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
    port: Option<String>,
    #[serde(default)]
    abort_run: bool,
    run_handle: Option<String>,
}
