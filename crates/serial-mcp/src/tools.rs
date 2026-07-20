use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use serial_protocol::{Cursor, Direction, EchoMode, EventQuery, SessionState, SlotSnapshot};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    capture::{Capture, CaptureOptions},
    render::render_events,
    session::SessionHandle,
};

const DEFAULT_TEXT_CHARS: usize = 16_000;
const MAX_TEXT_CHARS: usize = 64_000;
const MAX_WRITE_BYTES: usize = 4096;

#[derive(Clone)]
pub struct AgentTools {
    api: ApiClient,
    session: SessionHandle,
    actor_label: String,
}

impl AgentTools {
    pub fn new(api: ApiClient, session: SessionHandle, actor_label: String) -> Self {
        Self {
            api,
            session,
            actor_label,
        }
    }

    pub async fn call(&self, name: &str, arguments: Value) -> Result<Value> {
        match name {
            "devices" => self.devices(parse(arguments)?).await,
            "read" => self.read(parse(arguments)?).await,
            "command" => self.command(parse(arguments)?).await,
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
        let slots: Vec<Value> = status
            .slots
            .iter()
            .filter(|slot| args.slot_id.as_ref().is_none_or(|id| &slot.config.id == id))
            .map(slot_summary)
            .collect();
        if let Some(slot_id) = args.slot_id
            && slots.is_empty()
        {
            bail!("unknown Slot {slot_id:?}");
        }
        Ok(json!({
            "daemon_epoch": status.daemon_epoch,
            "slots": slots,
            "selection_note": "Choose a Slot explicitly before writing. A Run isolates one task from previous device state."
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
        let slot = self.slot(&args.slot_id).await?;
        let cursor = requested_cursor(args.epoch, args.after_seq, &slot)?;
        let after_seq = cursor
            .as_ref()
            .map(|value| value.after_seq)
            .unwrap_or_else(|| {
                slot.head_seq
                    .saturating_sub(args.tail_events.unwrap_or(200).clamp(1, 2000) as u64)
            });
        let response = self
            .api
            .events(
                &args.slot_id,
                &EventQuery {
                    epoch: Some(slot.daemon_epoch),
                    after_seq: Some(after_seq),
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: None,
                    kind: None,
                    actor_id: None,
                    run_id: None,
                    operation_id: None,
                    contains: None,
                    limit_events: Some(args.limit_events.unwrap_or(1000).clamp(1, 2000)),
                    limit_bytes: Some(args.limit_bytes.unwrap_or(512 * 1024).clamp(1, 1024 * 1024)),
                },
            )
            .await?;
        Ok(render_response(
            &slot,
            response,
            args.max_chars,
            args.include_raw,
            None,
            "tail_or_cursor",
        ))
    }

    async fn search(&self, args: SearchArgs) -> Result<Value> {
        if args.contains.trim().is_empty() {
            bail!("contains must not be empty");
        }
        let slot = self.slot(&args.slot_id).await?;
        let scope = args.scope.as_deref().unwrap_or("current_run");
        let (epoch, after_seq, run_id) = match scope {
            "current_run" => {
                let run = match args.run_id {
                    Some(id) => Some(id),
                    None => slot.active_run.as_ref().map(|run| run.id),
                }
                .context(
                    "no active Run; pass run_id or use scope=current_cursor/archive explicitly",
                )?;
                (slot.daemon_epoch, None, Some(run))
            }
            "current_cursor" => {
                let cursor = requested_cursor(args.epoch, args.after_seq, &slot)?
                    .context("scope=current_cursor requires epoch and after_seq")?;
                (cursor.epoch, Some(cursor.after_seq), None)
            }
            "archive" => (
                args.epoch
                    .context("scope=archive requires an explicit epoch")?,
                args.after_seq,
                args.run_id,
            ),
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
                    operation_id: None,
                    contains: Some(args.contains.clone()),
                    limit_events: Some(args.limit_events.unwrap_or(200).clamp(1, 1000)),
                    limit_bytes: Some(args.limit_bytes.unwrap_or(512 * 1024).clamp(1, 1024 * 1024)),
                },
            )
            .await?;
        Ok(render_response(
            &slot,
            response,
            args.max_chars,
            args.include_raw,
            None,
            scope,
        ))
    }

    async fn wait(&self, args: WaitArgs) -> Result<Value> {
        let slot = self.slot_online(&args.slot_id).await?;
        let cursor = requested_cursor(args.epoch, args.after_seq, &slot)?.unwrap_or(Cursor {
            epoch: slot.daemon_epoch,
            after_seq: slot.head_seq,
        });
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            args.slot_id,
            cursor,
        )
        .await?;
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: millis(args.quiet_ms, 300, 50, 5000),
                patterns: args.contains.into_iter().collect(),
                allow_empty_quiet: false,
            })
            .await;
        let rendered = render_events(
            &result.events,
            max_chars(args.max_chars),
            args.include_raw,
            None,
        );
        let last_seq = result
            .events
            .last()
            .map(|event| event.seq)
            .unwrap_or(slot.head_seq);
        Ok(json!({
            "slot_id": slot.config.id, "epoch": slot.daemon_epoch, "after_seq": last_seq,
            "completion": result.completion.label(), "complete": result.completion.is_complete(),
            "text": rendered.text, "events": rendered.events,
            "capture_truncated": result.truncated, "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed, "gaps": result.gaps
        }))
    }

    async fn command(&self, args: CommandArgs) -> Result<Value> {
        if args.command.is_empty() {
            bail!("command must not be empty");
        }
        let slot = self.slot_online(&args.slot_id).await?;
        let operation_id = Uuid::new_v4();
        let cursor = Cursor {
            epoch: slot.daemon_epoch,
            after_seq: slot.head_seq,
        };
        let capture = Capture::attach(
            self.api.endpoint(),
            self.api.token(),
            &self.actor_label,
            args.slot_id.clone(),
            cursor,
        )
        .await?;
        let mut bytes = args.command.as_bytes().to_vec();
        let eol = args
            .eol
            .clone()
            .unwrap_or_else(|| slot.config.settings.write_eol.clone());
        bytes.extend_from_slice(eol.as_bytes());
        if bytes.len() > MAX_WRITE_BYTES {
            bail!("command plus EOL exceeds {MAX_WRITE_BYTES} bytes");
        }

        let (patterns, completion_mode) = completion_patterns(&args, &slot)?;
        let write = self
            .session
            .write(
                args.slot_id.clone(),
                bytes,
                operation_id,
                seconds(args.control_wait_seconds, 15, 0, 60),
            )
            .await?;
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: millis(args.quiet_ms, 300, 50, 5000),
                patterns,
                allow_empty_quiet: true,
            })
            .await;
        let echo =
            matches!(slot.config.settings.echo, EchoMode::On).then_some(args.command.as_str());
        let rendered = render_events(
            &result.events,
            max_chars(args.max_chars),
            args.include_raw,
            echo,
        );
        let interfered = result.events.iter().any(|event| {
            event.direction == Direction::Tx && event.operation_id != Some(operation_id)
        });
        let last_seq = result
            .events
            .last()
            .map(|event| event.seq)
            .unwrap_or(write.event_seq);
        Ok(json!({
            "slot_id": slot.config.id, "epoch": slot.daemon_epoch, "after_seq": last_seq,
            "request_id": write.request_id, "operation_id": operation_id, "tx_event_seq": write.event_seq,
            "actor": write.actor, "completion_mode": completion_mode,
            "completion": result.completion.label(), "complete": result.completion.is_complete(),
            "interfered": interfered, "text": rendered.text, "events": rendered.events,
            "capture_truncated": result.truncated, "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed, "gaps": result.gaps,
            "guidance": if interfered { "Another actor wrote during this operation; do not treat output as isolated." } else { "Output belongs to this operation window; prompt/quiet completion is still a heuristic." }
        }))
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
                seconds(args.control_wait_seconds, 15, 0, 60),
            )
            .await?;
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
        Ok(json!({"run": self.session.end_run(args.slot_id, run_id).await?}))
    }

    async fn release(&self, args: ReleaseArgs) -> Result<Value> {
        let slot = self.slot(&args.slot_id).await?;
        if let Some(run) = slot.active_run
            && !args.abort_run
        {
            bail!(
                "active Run {} would be aborted; call run_end first or pass abort_run=true",
                run.id
            );
        }
        self.session.release(args.slot_id.clone()).await?;
        Ok(json!({"slot_id": args.slot_id, "released": true, "serial_port_closed": false}))
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

fn render_response(
    slot: &SlotSnapshot,
    response: serial_protocol::EventQueryResponse,
    requested_max: Option<usize>,
    include_raw: bool,
    echo: Option<&str>,
    scope: &str,
) -> Value {
    let rendered = render_events(
        &response.events,
        max_chars(requested_max),
        include_raw,
        echo,
    );
    let after_seq = response
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.after_seq)
        .or_else(|| response.events.last().map(|event| event.seq))
        .unwrap_or(slot.head_seq);
    json!({
        "slot_id": slot.config.id, "scope": scope, "epoch": response.next_cursor.as_ref().map(|c| c.epoch).unwrap_or(slot.daemon_epoch),
        "after_seq": after_seq, "head_seq": slot.head_seq, "text": rendered.text,
        "events": rendered.events, "truncated": response.truncated,
        "text_truncated": rendered.text_truncated, "repeated_lines_collapsed": rendered.repeated_lines_collapsed,
        "first_available_seq": response.first_available_seq, "gaps": response.gaps
    })
}

fn completion_patterns(args: &CommandArgs, slot: &SlotSnapshot) -> Result<(Vec<String>, String)> {
    let mode = args.completion.as_deref().unwrap_or("auto");
    let patterns = match mode {
        "auto" => [
            slot.config.settings.shell_prompt.clone(),
            slot.config.settings.uboot_prompt.clone(),
        ]
        .into_iter()
        .flatten()
        .collect(),
        "prompt" => {
            let patterns: Vec<String> = args
                .until
                .clone()
                .into_iter()
                .chain(
                    [
                        slot.config.settings.shell_prompt.clone(),
                        slot.config.settings.uboot_prompt.clone(),
                    ]
                    .into_iter()
                    .flatten(),
                )
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
        "profile": slot.config.profile, "enabled": slot.config.enabled, "session_state": slot.session_state,
        "state_reason": slot.state_reason, "target_activity": slot.target_activity, "baud_rate": slot.config.settings.baud_rate,
        "write_eol": slot.config.settings.write_eol, "echo": slot.config.settings.echo,
        "shell_prompt": slot.config.settings.shell_prompt, "uboot_prompt": slot.config.settings.uboot_prompt,
        "epoch": slot.daemon_epoch, "head_seq": slot.head_seq, "generation": slot.generation,
        "control": slot.control, "active_run": slot.active_run, "logging": slot.logging
    })
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
    limit_events: Option<usize>,
    limit_bytes: Option<usize>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
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
    limit_events: Option<usize>,
    limit_bytes: Option<usize>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
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
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommandArgs {
    slot_id: String,
    command: String,
    eol: Option<String>,
    completion: Option<String>,
    until: Option<String>,
    timeout_seconds: Option<u64>,
    quiet_ms: Option<u64>,
    control_wait_seconds: Option<u64>,
    max_chars: Option<usize>,
    #[serde(default)]
    include_raw: bool,
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
