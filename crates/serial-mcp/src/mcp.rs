use std::io::{BufRead, Write};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{Map, Value, json};

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
                    "instructions": "Inspect devices first. Start a Run for each task, initialize device state explicitly, use command for bounded operations, and end/release when finished. Never infer current state from archive history."
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
        "epoch": {"type":"string","format":"uuid","description":"Daemon epoch returned by a previous call; must accompany after_seq."},
        "after_seq": {"type":"integer","minimum":0,"description":"Return only events after this sequence; must accompany epoch."}
    });
    let bounds = json!({
        "max_chars": {"type":"integer","minimum":256,"maximum":64000,"default":16000},
        "include_raw": {"type":"boolean","default":false,"description":"Include base64 raw bytes; use only when exact bytes matter."}
    });
    vec![
        tool(
            "devices",
            "List authoritative serial Slots, online state, profile, prompts, control owner, active Run, and cursors. Call before selecting a device.",
            object(
                json!({"slot_id":{"type":"string","description":"Optional exact Slot ID."}}),
                &[],
            ),
            true,
        ),
        tool(
            "read",
            "Read a bounded recent tail or continue from an exact epoch/after_seq cursor. Reports gaps and folds only byte-identical adjacent lines.",
            object(
                merge(&[
                    json!({"slot_id":{"type":"string"},"tail_events":{"type":"integer","minimum":1,"maximum":2000,"default":200},"limit_events":{"type":"integer","minimum":1,"maximum":2000,"default":1000},"limit_bytes":{"type":"integer","minimum":1,"maximum":1048576,"default":524288}}),
                    cursor.clone(),
                    bounds.clone(),
                ]),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "command",
            "Atomically attach, queue for write control (never takeover), write command+EOL, and capture until prompt/literal/quiet/timeout. Returns operation ID and interference flag.",
            object(
                merge(&[
                    json!({
                        "slot_id":{"type":"string"},"command":{"type":"string","minLength":1,"maxLength":4096},
                        "eol":{"type":"string","description":"Override profile EOL for this call only; default profile usually uses \\r."},
                        "completion":{"type":"string","enum":["auto","prompt","contains","quiet"],"default":"auto"},
                        "until":{"type":"string","description":"Literal completion text; required for contains, optional extra prompt for prompt."},
                        "timeout_seconds":{"type":"integer","minimum":1,"maximum":120,"default":10},
                        "quiet_ms":{"type":"integer","minimum":50,"maximum":5000,"default":300},
                        "control_wait_seconds":{"type":"integer","minimum":0,"maximum":60,"default":15}
                    }),
                    bounds.clone(),
                ]),
                &["slot_id", "command"],
            ),
            false,
        ),
        tool(
            "wait",
            "Wait for new RX after the current head or an exact cursor. Literal contains completes on a match; without it, completes after RX followed by quiet.",
            object(
                merge(&[
                    json!({"slot_id":{"type":"string"},"contains":{"type":"string","minLength":1},"timeout_seconds":{"type":"integer","minimum":1,"maximum":120,"default":10},"quiet_ms":{"type":"integer","minimum":50,"maximum":5000,"default":300}}),
                    cursor.clone(),
                    bounds.clone(),
                ]),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "search",
            "Bounded literal search. Defaults to the current Run to prevent stale logs from an earlier test being mistaken for current evidence; archive search is explicit.",
            object(
                merge(&[
                    json!({
                        "slot_id":{"type":"string"},"contains":{"type":"string","minLength":1},
                        "scope":{"type":"string","enum":["current_run","current_cursor","archive"],"default":"current_run"},
                        "run_id":{"type":"string","format":"uuid"},"direction":{"type":"string","enum":["rx","tx","none"]},
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
            "Create a task boundary on one Slot. A Run scopes later searches but does not reset the device; initialize device state explicitly.",
            object(
                json!({
                    "slot_id":{"type":"string"},"label":{"type":"string","minLength":1,"maxLength":128},
                    "metadata":{"type":"object","additionalProperties":true,"default":{}},"control_wait_seconds":{"type":"integer","minimum":0,"maximum":60,"default":15}
                }),
                &["slot_id", "label"],
            ),
            false,
        ),
        tool(
            "run_end",
            "End the active Run (or an explicit run_id) without closing the serial port or releasing shared observation.",
            object(
                json!({"slot_id":{"type":"string"},"run_id":{"type":"string","format":"uuid"}}),
                &["slot_id"],
            ),
            false,
        ),
        tool(
            "release",
            "Release this adapter's write-control lease. It never closes the serial port. Refuses to abort an active Run unless abort_run=true.",
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
}
