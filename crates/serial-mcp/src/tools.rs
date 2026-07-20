use std::{collections::BTreeMap, time::Duration};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use serial_protocol::{
    Cursor, Direction, EchoMode, EventQuery, SessionState, SlotSnapshot, WritePacing,
};
use uuid::Uuid;

use crate::{
    api::ApiClient,
    capture::{Capture, CaptureOptions},
    config::CaptureLimits,
    render::{RenderOptions, render_events},
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
    capture_limits: CaptureLimits,
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
            "selection_note": "Choose a Slot explicitly before writing. A Run isolates one task from previous device state."
        }))
    }

    async fn read(&self, args: ReadArgs) -> Result<Value> {
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
            },
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
            },
            scope,
        );
        if no_matches {
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
            self.capture_limits,
        )
        .await?;
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: millis(args.quiet_ms, 300, 50, 5000),
                patterns: args.contains.into_iter().collect(),
                until_regex: None,
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
            },
        );
        let last_seq = result
            .events
            .last()
            .map(|event| event.seq)
            .unwrap_or(slot.head_seq);
        let mut output = json!({
            "slot_id": slot.config.id, "epoch": slot.daemon_epoch, "after_seq": last_seq,
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
            self.capture_limits,
        )
        .await?;
        let bytes = compose_write_bytes(
            &args.command,
            args.eol.as_deref(),
            &slot.config.settings.write_eol,
        )?;

        let (patterns, completion_mode) = completion_patterns(&args, &slot)?;
        let until_regex = args
            .until_regex
            .as_deref()
            .map(regex::Regex::new)
            .transpose()
            .context("until_regex is not a valid regex")?;
        let pacing = write_pacing(args.chunk_size, args.inter_char_delay_ms, &slot);
        let write = self
            .session
            .write(
                args.slot_id.clone(),
                bytes,
                operation_id,
                pacing,
                seconds(args.control_wait_seconds, 15, 0, 60),
            )
            .await?;
        let result = capture
            .collect(CaptureOptions {
                timeout: seconds(args.timeout_seconds, 10, 1, 120),
                quiet: millis(args.quiet_ms, 300, 50, 5000),
                patterns,
                until_regex,
                allow_empty_quiet: true,
            })
            .await;
        let echo = (matches!(slot.config.settings.echo, EchoMode::On) && !args.command.is_empty())
            .then_some(args.command.as_str());
        let with_events = args.include_events || args.include_raw;
        let rendered = render_events(
            &result.events,
            RenderOptions {
                max_chars: max_chars(args.max_chars),
                include_raw: args.include_raw,
                echo,
                collapse_repeats: args.collapse_repeats,
                include_events: args.include_events,
            },
        );
        let interfered = result.events.iter().any(|event| {
            event.direction == Direction::Tx && event.operation_id != Some(operation_id)
        });
        let last_seq = result
            .events
            .last()
            .map(|event| event.seq)
            .unwrap_or(write.event_seq);
        let mut output = json!({
            "slot_id": slot.config.id, "epoch": slot.daemon_epoch, "after_seq": last_seq,
            "request_id": write.request_id, "operation_id": operation_id, "tx_event_seq": write.event_seq,
            "actor": write.actor, "completion_mode": completion_mode,
            "completion": result.completion.label(), "complete": result.completion.is_complete(),
            "interfered": interfered, "text": rendered.text,
            "capture_truncated": result.truncated, "text_truncated": rendered.text_truncated,
            "repeated_lines_collapsed": rendered.repeated_lines_collapsed, "gaps": result.gaps,
            "guidance": if interfered { "Another actor wrote during this operation; do not treat output as isolated." } else { "Output belongs to this operation window; prompt/quiet completion is still a heuristic." }
        });
        if with_events {
            output["events"] = json!(rendered.events);
        }
        Ok(output)
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
    if with_events {
        output["events"] = json!(rendered.events);
    }
    output
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
        "profile": slot.config.profile, "device_profile": slot.config.device_profile,
        "enabled": slot.config.enabled, "session_state": slot.session_state,
        "state_reason": slot.state_reason, "target_activity": slot.target_activity, "baud_rate": slot.config.settings.baud_rate,
        "write_eol": slot.config.settings.write_eol, "echo": slot.config.settings.echo,
        "shell_prompt": slot.config.settings.shell_prompt, "uboot_prompt": slot.config.settings.uboot_prompt,
        "effective_shell_prompt": slot.effective_shell_prompt, "effective_uboot_prompt": slot.effective_uboot_prompt,
        "epoch": slot.daemon_epoch, "head_seq": slot.head_seq, "generation": slot.generation,
        "control": slot.control, "active_run": slot.active_run, "logging": slot.logging
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
    control_wait_seconds: Option<u64>,
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
            "collapse_repeats": false,
        }))
        .unwrap();
        assert_eq!(search.operation_id, Some(Uuid::nil()));
        assert!(!search.collapse_repeats);

        let wait: WaitArgs = serde_json::from_value(json!({"slot_id": "bench"})).unwrap();
        assert!(wait.collapse_repeats);
    }

    #[test]
    fn command_args_accept_an_empty_command() {
        let args: CommandArgs =
            serde_json::from_value(json!({"slot_id": "bench", "command": ""})).unwrap();
        assert!(args.command.is_empty());
        assert!(args.collapse_repeats);
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
}
