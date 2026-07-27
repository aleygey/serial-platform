use std::io::{BufRead, Write};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use serial_protocol::{
    DEFAULT_TRIGGER_INTERVAL_MS, DEFAULT_TRIGGER_MAX_FIRES, DEFAULT_TRIGGER_TIMEOUT_MS,
    MAX_TRIGGER_ACTION_BYTES, MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES,
    MAX_TRIGGER_INTERVAL_MS, MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS,
    MAX_TRIGGER_TIMEOUT_MS, MAX_TRIGGER_TOTAL_BYTES, MIN_TRIGGER_INTERVAL_MS,
    MIN_TRIGGER_TIMEOUT_MS,
};

use crate::tools::AgentTools;

const LATEST_PROTOCOL: &str = "2025-11-25";
const SUPPORTED_PROTOCOLS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

pub async fn serve(tools: AgentTools) -> Result<()> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.context("failed reading MCP stdin")?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(response) = dispatch(&tools, &line).await {
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    Ok(())
}

async fn dispatch(tools: &AgentTools, line: &str) -> Option<Value> {
    let request: RpcRequest = match serde_json::from_str(line) {
        Ok(request) => request,
        Err(error) => {
            return Some(rpc_error(
                Value::Null,
                -32700,
                format!("parse error: {error}"),
            ));
        }
    };
    let id = request.id.clone()?;
    if request.jsonrpc.as_deref() != Some("2.0") {
        return Some(rpc_error(id, -32600, "jsonrpc must be 2.0"));
    }
    match request.method.as_str() {
        "initialize" => {
            let requested = request
                .params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or(LATEST_PROTOCOL);
            let protocol = if SUPPORTED_PROTOCOLS.contains(&requested) {
                requested
            } else {
                LATEST_PROTOCOL
            };
            Some(rpc_result(
                id,
                json!({
                    "protocolVersion": protocol,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "serial-mcp", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": "Inspect devices first. Start a Run for each task before command or trigger, then initialize device state explicitly. A Run isolates only the log/event interval; it does not reset or clean device state. Trigger bytes and literals are always explicit call parameters; no Profile behavior is inferred. End the Run when finished; serial-mcp stops renewal and best-effort releases control without closing the serial port. Never infer current state from archive history."
                }),
            ))
        }
        "ping" => Some(rpc_result(id, json!({}))),
        "tools/list" => Some(rpc_result(id, json!({"tools": tool_definitions()}))),
        "tools/call" => {
            let params: ToolCall = match serde_json::from_value(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return Some(rpc_error(id, -32602, format!("invalid tool call: {error}")));
                }
            };
            if !tool_definitions()
                .iter()
                .any(|tool| tool["name"] == params.name)
            {
                return Some(rpc_error(
                    id,
                    -32602,
                    format!("unknown tool {:?}", params.name),
                ));
            }
            match tools
                .call(&params.name, Value::Object(params.arguments))
                .await
            {
                Ok(value) => Some(rpc_result(id, tool_result(value, false))),
                Err(error) => Some(rpc_result(
                    id,
                    tool_result(json!({"error": error.to_string()}), true),
                )),
            }
        }
        _ => Some(rpc_error(
            id,
            -32601,
            format!("method {:?} not found", request.method),
        )),
    }
}

fn tool_result(value: Value, is_error: bool) -> Value {
    let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
    json!({"content": [{"type": "text", "text": text}], "structuredContent": value, "isError": is_error})
}

fn rpc_result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}
fn rpc_error(id: Value, code: i64, message: impl Into<String>) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message.into()}})
}

#[derive(Deserialize)]
struct RpcRequest {
    jsonrpc: Option<String>,
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Deserialize)]
struct ToolCall {
    name: String,
    #[serde(default)]
    arguments: Map<String, Value>,
}

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": properties, "required": required, "additionalProperties": false})
}

fn tool(name: &str, description: &str, input_schema: Value, read_only: bool) -> Value {
    json!({
        "name": name, "description": description, "inputSchema": input_schema,
        "annotations": {"readOnlyHint": read_only, "destructiveHint": false, "idempotentHint": read_only, "openWorldHint": false}
    })
}

pub fn tool_definitions() -> Vec<Value> {
    let cursor = json!({
        "epoch": {"type":"string","format":"uuid","description":"Daemon epoch returned by a previous call; must accompany after_seq. A current_run continuation must use the current Slot epoch."},
        "after_seq": {"type":"integer","minimum":0,"description":"Return only events after this sequence; must accompany epoch. For a truncated current_run search, repeat the same filters with both returned cursor values."}
    });
    let read_cursor = json!({
        "epoch": {"type":"string","format":"uuid","description":"Epoch to read; defaults to the current daemon epoch. Supply an archived epoch (listed by search guidance or GET /api/v1/archives) to read history."},
        "after_seq": {"type":"integer","minimum":0,"description":"Return only events after this sequence; requires epoch. Omit both to tail the current epoch, or pass epoch alone to read an archive from its start."}
    });
    let wait_cursor = json!({
        "epoch": {"type":"string","format":"uuid","description":"Explicit daemon epoch override; must accompany after_seq. An explicit cursor always takes priority over serial-mcp's remembered live cursor."},
        "after_seq": {"type":"integer","minimum":0,"description":"Explicit sequence override; must accompany epoch. Omit both to resume from this serial-mcp process's last live cursor for the Slot (recorded by run_start, command, or wait), falling back to the current head only when no compatible cursor exists."}
    });
    let bounds = json!({
        "max_chars": {"type":"integer","minimum":256,"maximum":64000,"default":16000},
        "include_raw": {"type":"boolean","default":false,"description":"Include base64 raw bytes; use only when exact bytes matter."},
        "include_events": {"type":"boolean","default":false,"description":"Include the full per-event array (verbose). Default false returns text plus the cursor fields (epoch/after_seq/head_seq/first_available_seq/gaps/truncated) needed to continue reading."},
        "collapse_repeats": {"type":"boolean","default":true,"description":"Fold byte-identical adjacent lines into a repeat marker; false keeps the exact line stream."}
    });
    vec![
        tool(
            "devices",
            "List authoritative serial Slots, online state, profile, effective prompts, control owner, active Run/Trigger, and cursors. Call before selecting a device. Run boundaries isolate log/event intervals only and never reset device state.",
            object(
                json!({"slot_id":{"type":"string","description":"Optional exact Slot ID."}}),
                &[],
            ),
            true,
        ),
        tool(
            "read",
            "Read a bounded recent tail or continue from an exact epoch/after_seq cursor, including archived epochs. Reports gaps and folds only byte-identical adjacent lines. operation_id can identify the confirmed TX audit only; RX output must be scoped by a Run or cursor. truncated marks a server-side query that reached limit_events/limit_bytes; continue from the returned after_seq.",
            object(
                merge(&[
                    json!({"slot_id":{"type":"string"},"tail_events":{"type":"integer","minimum":1,"maximum":2000,"default":200},"limit_events":{"type":"integer","minimum":1,"maximum":2000,"default":1000},"limit_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":524288}}),
                    json!({"direction":{"type":"string","enum":["rx","tx","none"],"description":"Filter by event direction; none matches directionless control events."},"operation_id":{"type":"string","format":"uuid","description":"Confirmed TX audit filter only; requires direction=tx. Device RX always has operation_id=null because shared serial output cannot be assigned causally. Use command capture_window, run_id, or an epoch/after_seq cursor for RX."}}),
                    read_cursor,
                    bounds.clone(),
                ]),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "command",
            "Require this serial-mcp process to own the Slot's active Run, atomically attach, renew that Run's existing write-control lease (never reacquire/takeover), write command+EOL with expected_run_id, and capture a bounded RX window after the confirmed TX audit. seriald rejects the write with zero bytes if the Run ended, changed, or belongs to another actor. With echo=on, completion is disarmed until the complete command echo is observed, so an old prompt or prewrite RX backlog cannot complete this command. Returns run_id, prewrite_activity, boundary confidence, and an interference flag; foreign TX means the RX is not isolated. prompt/contains wait for their exact boundary or timeout; quiet applies only when explicitly selected or auto has no configured prompt. When until_regex is supplied it is the sole completion boundary: profile prompts, literals, and quiet cannot pre-empt it, and completion_mode is regex. complete means the capture boundary was observed, never that the command succeeded; execution_status remains unknown because this generic adapter does not rewrite commands to inspect a shell exit code. When the target shell is known, emit a unique status sentinel from the command and use that sentinel as the literal or regex boundary; serial-mcp never injects shell syntax itself. RX events are Run/cursor scoped and never carry operation_id. capture_truncated marks capture-window overflow (default 4096 events / 1 MiB; oldest events dropped), text_truncated marks rendered text cut to max_chars.",
            object(
                merge(&[
                    json!({
                        "slot_id":{"type":"string"},"command":{"type":"string","minLength":0,"maxLength":4096,"description":"Command text; may be empty to send a bare EOL (plain Enter)."},
                        "eol":{"type":"string","description":"Override profile EOL for this call only; default profile usually uses \\r."},
                        "completion":{"type":"string","enum":["auto","prompt","contains","quiet"],"default":"auto","description":"auto uses configured prompts, otherwise quiet. prompt/contains require exact evidence and do not finish on quiet_ms gaps. With until_regex, omit this field or leave it auto; explicit prompt/contains/quiet is rejected as ambiguous."},
                        "until":{"type":"string","description":"Literal completion text; required for contains, optional extra prompt for prompt. Cannot be combined with until_regex."},
                        "until_regex":{"type":"string","minLength":1,"description":"Sole authoritative regex completion boundary matched against the rolling RX window. Profile prompts, literals, and quiet are disabled until this regex matches or timeout occurs; the result reports completion_mode=regex."},
                        "inter_char_delay_ms":{"type":"integer","minimum":0,"description":"Write pacing override for this call: delay between written chunks in ms; seriald already defaults to 1 ms per character. Omit to keep the Slot setting."},
                        "chunk_size":{"type":"integer","minimum":1,"description":"Write pacing override for this call: bytes written per chunk; seriald defaults to 1 (one character). Omit to keep the Slot setting."},
                        "timeout_seconds":{"type":"integer","minimum":1,"maximum":120,"default":10},
                        "quiet_ms":{"type":"integer","minimum":50,"maximum":5000,"default":300,"description":"Quiet boundary duration; used only by completion=quiet or auto when no prompt is configured."}
                    }),
                    bounds.clone(),
                ]),
                &["slot_id", "command"],
            ),
            false,
        ),
        tool(
            "trigger",
            "Run one bounded, generic serial reaction inside seriald while this adapter owns the Slot's active Run and write control. seriald atomically arms RX observation, optionally performs initial_write once (the one-time kickoff), then sends action after each completed interval until a caller-supplied literal stop pattern matches or a terminal limit/failure occurs. This MCP v1 surface accepts explicit UTF-8 text plus EOL; the lower seriald protocol remains raw-byte capable. No device Profile, prompt, bootloader, or flashing behavior is inferred. timeout_ms stops new scheduling; one already accepted bounded driver write may settle before the authoritative terminal result. The tool keeps the Run lease renewable, returns evidence clipped to the authoritative Trigger start/end sequence, and reports whether this MCP connection still retains the Run for a follow-up write. Only outcome=matched means a stop pattern was observed.",
            object(
                merge(&[
                    json!({
                        "slot_id":{"type":"string"},
                        "initial_write":{
                            "type":"object",
                            "description":"Optional one-time kickoff performed by the same atomic daemon Trigger after RX observation is armed. It is not counted in fires_confirmed and is never sent as a separate ordinary write. MCP v1 accepts UTF-8 text only; text plus EOL is limited by encoded byte length at runtime.",
                            "properties":{
                                "text":{"type":"string","maxLength":MAX_TRIGGER_INITIAL_WRITE_BYTES,"description":"UTF-8 text for the one-time initial write; text plus EOL must encode to at most 4096 bytes."},
                                "eol":{"type":"string","maxLength":MAX_TRIGGER_INITIAL_WRITE_BYTES,"default":"","description":"Explicit UTF-8 EOL appended to text. Defaults to empty and never inherits the Slot/Profile EOL; the combined 4096-byte limit is enforced at runtime."}
                            },
                            "required":["text"],
                            "additionalProperties":false
                        },
                        "start_contains":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES,"description":"Optional case-sensitive UTF-8 literal RX text that must be observed after arming before action sends begin. Matching can span RX chunks; encoded length is limited to 256 bytes at runtime."},
                        "action":{
                            "type":"object",
                            "description":"Action written once per fire. The combined UTF-8 text and explicit EOL must not be empty and must encode to at most 256 bytes.",
                            "properties":{
                                "text":{"type":"string","maxLength":MAX_TRIGGER_ACTION_BYTES},
                                "eol":{"type":"string","maxLength":MAX_TRIGGER_ACTION_BYTES,"default":"","description":"Explicit UTF-8 EOL appended to each action. Defaults to empty and never inherits the Slot/Profile EOL; the combined 256-byte limit is enforced at runtime."}
                            },
                            "required":["text"],
                            "additionalProperties":false
                        },
                        "interval_ms":{"type":"integer","minimum":MIN_TRIGGER_INTERVAL_MS,"maximum":MAX_TRIGGER_INTERVAL_MS,"default":DEFAULT_TRIGGER_INTERVAL_MS,"description":"Delay after one action write is confirmed before the next is eligible. Pacing/write time is additional; missed ticks are never replayed in a burst."},
                        "stop_contains":{"type":"array","default":[],"maxItems":MAX_TRIGGER_PATTERNS,"items":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES},"description":"Optional caller-supplied case-sensitive UTF-8 literal RX stop patterns, each limited to 256 encoded bytes at runtime. No Profile prompt is added implicitly. Omit for a bounded one-shot/repeat that intentionally ends at max_fires or timeout; when a matched outcome is required, supply at least one literal."},
                        "timeout_ms":{"type":"integer","minimum":MIN_TRIGGER_TIMEOUT_MS,"maximum":MAX_TRIGGER_TIMEOUT_MS,"default":DEFAULT_TRIGGER_TIMEOUT_MS,"description":"Deadline for scheduling new writes. One already accepted bounded physical write may finish and be audited before the terminal result is returned."},
                        "max_fires":{"type":"integer","minimum":1,"maximum":MAX_TRIGGER_FIRES,"default":DEFAULT_TRIGGER_MAX_FIRES,"description":format!("Maximum confirmed action writes; the optional initial_write is not counted. Reaching it yields outcome=max_fires_reached and matched=false. The combined runtime plan len(initial_write) + len(action) * max_fires must not exceed {MAX_TRIGGER_TOTAL_BYTES} UTF-8 bytes.")},
                        "inter_char_delay_ms":{"type":"integer","minimum":0,"description":"Optional pacing override shared by initial_write and every action. Omit both pacing fields to retain the Slot transport setting. If only this field is supplied, chunk_size inherits the Slot setting."},
                        "chunk_size":{"type":"integer","minimum":1,"description":"Optional bytes-per-chunk pacing override shared by initial_write and every action. If only this field is supplied, inter_char_delay_ms inherits the Slot setting. For a target validated for bursts, action={text:'slp'}, chunk_size=3, inter_char_delay_ms=0 avoids conservative character pacing."}
                    }),
                    bounds.clone(),
                ]),
                &["slot_id", "action"],
            ),
            false,
        ),
        tool(
            "wait",
            "Wait for RX after an explicit cursor, otherwise after this serial-mcp process's last live cursor for the Slot; only a missing, stale-epoch, or ahead-of-head remembered cursor falls back to the current head. This closes the command-response-to-wait attach race for delayed output. The result reports cursor_source and started_after_seq. Literal contains waits for that exact match or timeout and is never ended by a short quiet gap; without contains, capture ends after RX followed by quiet. capture_truncated marks capture-window overflow (default 4096 events / 1 MiB; oldest events dropped), text_truncated marks rendered text cut to max_chars.",
            object(
                merge(&[
                    json!({"slot_id":{"type":"string"},"contains":{"type":"string","minLength":1},"timeout_seconds":{"type":"integer","minimum":1,"maximum":120,"default":10},"quiet_ms":{"type":"integer","minimum":50,"maximum":5000,"default":300}}),
                    wait_cursor,
                    bounds.clone(),
                ]),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "search",
            "Bounded literal search. Defaults to the current Run to prevent stale logs from an earlier test being mistaken for current evidence; archive search is explicit. A truncated current_run page returns continuation fields; repeat the same filters, run_id, scope, epoch, and after_seq before concluding that no match exists. Text defaults to matching lines plus five surrounding lines even when one matching RX event contains many lines; include_events/include_raw retain exact evidence. operation_id can filter confirmed TX only, never RX. A complete empty result includes wider archive guidance; an incomplete empty result prioritizes continuation guidance.",
            object(
                merge(&[
                    json!({
                        "slot_id":{"type":"string"},"contains":{"type":"string","minLength":1},
                        "scope":{"type":"string","enum":["current_run","current_cursor","archive"],"default":"current_run","description":"current_run keeps an explicit/active Run constraint and accepts the returned current-epoch cursor for pagination; current_cursor starts at an exact live cursor; archive requires an explicit epoch."},
                        "run_id":{"type":"string","format":"uuid","description":"Run constraint. Omit only when scope=current_run can resolve the active Run; keep it unchanged across truncated-page continuations."},"direction":{"type":"string","enum":["rx","tx","none"]},
                        "operation_id":{"type":"string","format":"uuid","description":"Confirmed TX audit filter only; requires direction=tx. Device RX has operation_id=null; scope output with run_id or current_cursor instead."},
                        "context_lines":{"type":"integer","minimum":0,"maximum":50,"default":5,"description":"Number of lines retained before and after each literal match in rendered text. Exact returned events/raw bytes are unaffected."},
                        "limit_events":{"type":"integer","minimum":1,"maximum":1000,"default":200},"limit_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":524288}
                    }),
                    cursor,
                    bounds,
                ]),
                &["slot_id", "contains"],
            ),
            true,
        ),
        tool(
            "run_start",
            "Acquire queued Agent write control and create an adapter-owned task boundary on one Slot. A Run scopes later commands/searches and is the only lease renewed in the background, but it does not reset or initialize the device; initialize device state explicitly.",
            object(
                json!({
                    "slot_id":{"type":"string"},"label":{"type":"string","minLength":1,"maxLength":128},
                    "metadata":{"type":"object","additionalProperties":true,"default":{}},"control_wait_seconds":{"type":"integer","minimum":0,"maximum":15,"default":15,"description":"Maximum time to queue for Agent write control. Capped at 15 seconds so one blocked Slot cannot starve lease renewal for another Slot."}
                }),
                &["slot_id", "label"],
            ),
            false,
        ),
        tool(
            "run_end",
            "End this adapter process's active Run (or its explicit run_id), stop periodic control renewal, and immediately attempt a best-effort control release. Release failure does not undo a successfully ended Run; any unreleased lease expires by TTL. The serial port remains open for shared observation.",
            object(
                json!({"slot_id":{"type":"string"},"run_id":{"type":"string","format":"uuid"}}),
                &["slot_id"],
            ),
            false,
        ),
        tool(
            "release",
            "Release this adapter process's write-control lease. With no local lease it is idempotent success. It protects only a Run started by this process and refuses to abort that Run unless abort_run=true. It never closes the serial port.",
            object(
                json!({"slot_id":{"type":"string"},"abort_run":{"type":"boolean","default":false}}),
                &["slot_id"],
            ),
            false,
        ),
    ]
}

fn merge(values: &[Value]) -> Value {
    let mut output = Map::new();
    for value in values {
        if let Some(map) = value.as_object() {
            output.extend(map.clone());
        }
    }
    Value::Object(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_names_form_the_stable_agent_surface() {
        let names: Vec<_> = tool_definitions()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            names,
            [
                "devices",
                "read",
                "command",
                "trigger",
                "wait",
                "search",
                "run_start",
                "run_end",
                "release"
            ]
        );
    }

    #[test]
    fn schemas_reject_unknown_arguments() {
        for tool in tool_definitions() {
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
        }
    }

    #[test]
    fn agent_control_wait_is_bounded_below_the_renewal_interval() {
        let tool = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "run_start")
            .unwrap();
        assert_eq!(
            tool["inputSchema"]["properties"]["control_wait_seconds"]["maximum"],
            15
        );
        let command = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "command")
            .unwrap();
        assert!(
            command["inputSchema"]["properties"]
                .get("control_wait_seconds")
                .is_none()
        );
    }

    #[test]
    fn schemas_describe_regex_authority_and_run_scoped_search_continuation() {
        let tools = tool_definitions();
        let command = tools.iter().find(|tool| tool["name"] == "command").unwrap();
        assert_eq!(
            command["inputSchema"]["properties"]["until_regex"]["minLength"],
            1
        );
        assert!(
            command["inputSchema"]["properties"]["until_regex"]["description"]
                .as_str()
                .unwrap()
                .contains("Sole authoritative")
        );

        let search = tools.iter().find(|tool| tool["name"] == "search").unwrap();
        assert!(
            search["description"]
                .as_str()
                .unwrap()
                .contains("truncated current_run")
        );
        assert!(
            search["inputSchema"]["properties"]["after_seq"]["description"]
                .as_str()
                .unwrap()
                .contains("same filters")
        );

        let wait = tools.iter().find(|tool| tool["name"] == "wait").unwrap();
        assert!(
            wait["description"]
                .as_str()
                .unwrap()
                .contains("cursor_source")
        );
        assert!(
            wait["inputSchema"]["properties"]["after_seq"]["description"]
                .as_str()
                .unwrap()
                .contains("last live cursor")
        );
        assert!(
            wait["inputSchema"]["properties"]["epoch"]["description"]
                .as_str()
                .unwrap()
                .contains("always takes priority")
        );
    }

    #[test]
    fn trigger_schema_is_generic_and_uses_only_explicit_call_bytes() {
        let trigger = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "trigger")
            .unwrap();
        let schema = &trigger["inputSchema"];
        assert_eq!(schema["required"], json!(["slot_id", "action"]));
        assert_eq!(
            schema["properties"]["action"]["properties"]["eol"]["default"],
            ""
        );
        assert_eq!(
            schema["properties"]["initial_write"]["properties"]["eol"]["default"],
            ""
        );
        assert_eq!(schema["properties"]["stop_contains"]["default"], json!([]));
        assert!(
            schema["properties"]["max_fires"]["description"]
                .as_str()
                .is_some_and(|description| {
                    description.contains(&MAX_TRIGGER_TOTAL_BYTES.to_string())
                })
        );
        let serialized = serde_json::to_string(&trigger).unwrap().to_lowercase();
        for forbidden in ["sigmastar", "uboot"] {
            assert!(
                !serialized.contains(forbidden),
                "trigger schema contains device-specific term {forbidden:?}"
            );
        }
    }
}
