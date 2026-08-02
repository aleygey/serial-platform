use std::{
    collections::HashMap,
    io::{BufRead, Write},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use serial_protocol::{
    MAX_TRIGGER_ACTION_BYTES, MAX_TRIGGER_FIRES, MAX_TRIGGER_INITIAL_WRITE_BYTES,
    MAX_TRIGGER_INTERVAL_MS, MAX_TRIGGER_PATTERN_BYTES, MAX_TRIGGER_PATTERNS,
    MAX_TRIGGER_TIMEOUT_MS, MIN_TRIGGER_INTERVAL_MS, MIN_TRIGGER_TIMEOUT_MS,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::tools::AgentTools;

const LATEST_PROTOCOL: &str = "2025-11-25";
const SUPPORTED_PROTOCOLS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

pub async fn serve(tools: AgentTools) -> Result<()> {
    let (input_tx, mut input_rx) = mpsc::unbounded_channel();
    let input_thread = std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) => {
                    if input_tx.send(Input::Line(line)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = input_tx.send(Input::Error(error.to_string()));
                    return;
                }
            }
        }
    });

    // Only this thread owns stdout. Tool calls may finish concurrently, but
    // their JSON-RPC frames can never interleave at the byte level.
    let (output_tx, output_rx) = std::sync::mpsc::channel::<Value>();
    let output_thread = std::thread::spawn(move || -> Result<()> {
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        for response in output_rx {
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
        Ok(())
    });

    let active_requests: ActiveRequests = Arc::new(Mutex::new(HashMap::new()));
    let mut tasks = JoinSet::new();
    let mut input_error = None;
    while let Some(input) = input_rx.recv().await {
        while tasks.try_join_next().is_some() {}
        let line = match input {
            Input::Line(line) => line,
            Input::Error(error) => {
                input_error = Some(error);
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }

        let request: RpcRequest = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                let _ = output_tx.send(rpc_error(
                    Value::Null,
                    -32700,
                    format!("parse error: {error}"),
                ));
                continue;
            }
        };
        if request.jsonrpc.as_deref() != Some("2.0") {
            if let Some(id) = request.id {
                let _ = output_tx.send(rpc_error(id, -32600, "jsonrpc must be 2.0"));
            }
            continue;
        }
        if is_cancel_notification(&request.method) {
            if let Some(cancelled_id) = cancellation_id(&request.params) {
                cancel_request(&active_requests, &cancelled_id);
            }
            continue;
        }

        // JSON-RPC notifications deliberately have no response. MCP's
        // initialized notification is the common case; unknown notifications
        // are ignored as required by JSON-RPC.
        let Some(id) = request.id.clone() else {
            continue;
        };
        let key = request_key(&id);
        let (cancel_tx, cancel_rx) = if request_is_cancellable(&request) {
            let (tx, rx) = oneshot::channel();
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        {
            let mut active = active_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if active.contains_key(&key) {
                let _ = output_tx.send(rpc_error(
                    id,
                    -32600,
                    "request id is already active; mutating calls are never cancelled or replaced",
                ));
                continue;
            }
            active.insert(key.clone(), cancel_tx);
        }

        let tools = tools.clone();
        let output = output_tx.clone();
        let active_requests = active_requests.clone();
        tasks.spawn(async move {
            let response = match cancel_rx {
                Some(cancel_rx) => {
                    tokio::select! {
                        _ = cancel_rx => rpc_error(id.clone(), -32800, "request cancelled"),
                        response = dispatch_request(&tools, request, id.clone()) => response,
                    }
                }
                None => dispatch_request(&tools, request, id.clone()).await,
            };
            active_requests
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&key);
            let _ = output.send(response);
        });
    }

    // Closing stdin cancels only read-only observations. A command, signal,
    // Run transition, release, or Trigger may already have crossed its
    // physical side-effect boundary, so dropping that future could hide the
    // authoritative outcome and invite an unsafe retry. Let those calls
    // converge before the adapter exits.
    cancel_all_read_only(&active_requests);
    while tasks.join_next().await.is_some() {}
    drop(output_tx);
    input_thread
        .join()
        .map_err(|_| anyhow::anyhow!("MCP stdin reader panicked"))?;
    output_thread
        .join()
        .map_err(|_| anyhow::anyhow!("MCP stdout writer panicked"))??;
    if let Some(error) = input_error {
        anyhow::bail!("failed reading MCP stdin: {error}");
    }
    Ok(())
}

enum Input {
    Line(String),
    Error(String),
}

type ActiveRequests = Arc<Mutex<HashMap<String, Option<oneshot::Sender<()>>>>>;

fn is_cancel_notification(method: &str) -> bool {
    matches!(method, "notifications/cancelled" | "$/cancelRequest")
}

fn cancellation_id(params: &Value) -> Option<Value> {
    params
        .get("requestId")
        .or_else(|| params.get("id"))
        .cloned()
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| id.to_string())
}

fn cancel_request(active_requests: &ActiveRequests, id: &Value) {
    if let Some(cancel) = active_requests
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_mut(&request_key(id))
        .and_then(Option::take)
    {
        let _ = cancel.send(());
    }
}

fn cancel_all_read_only(active_requests: &ActiveRequests) {
    let mut active = active_requests
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for cancel in active.values_mut().filter_map(Option::take) {
        let _ = cancel.send(());
    }
}

fn request_is_cancellable(request: &RpcRequest) -> bool {
    if request.method != "tools/call" {
        return false;
    }
    matches!(
        request
            .params
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        "devices"
            | "read"
            | "wait"
            | "search"
            | "monitor_list"
            | "monitor_status"
            | "monitor_incidents"
    )
}

async fn dispatch_request(tools: &AgentTools, request: RpcRequest, id: Value) -> Value {
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
            rpc_result(
                id,
                json!({
                    "protocolVersion": protocol,
                    "capabilities": {"tools": {"listChanged": false}},
                    "serverInfo": {"name": "serial-mcp", "version": env!("CARGO_PKG_VERSION")},
                    "instructions": "Inspect devices, start a Run before writes, and initialize device state explicitly. Runs scope evidence only. command inherits Slot settings; input/signal are raw; Trigger bytes are explicit. Monitor Jobs persist in seriald after this MCP process exits; monitor_start returns immediately and monitor_incidents reads bounded retained results. End Runs and stop Monitors when they are no longer needed."
                }),
            )
        }
        "ping" => rpc_result(id, json!({})),
        "tools/list" => rpc_result(id, json!({"tools": tool_definitions()})),
        "tools/call" => {
            let params: ToolCall = match serde_json::from_value(request.params) {
                Ok(params) => params,
                Err(error) => {
                    return rpc_error(id, -32602, format!("invalid tool call: {error}"));
                }
            };
            if !tool_definitions()
                .iter()
                .any(|tool| tool["name"] == params.name)
            {
                return rpc_error(id, -32602, format!("unknown tool {:?}", params.name));
            }
            match tools
                .call(&params.name, Value::Object(params.arguments))
                .await
            {
                Ok(value) => rpc_result(id, tool_result(value, false)),
                Err(error) => {
                    rpc_result(id, tool_result(json!({"error": error.to_string()}), true))
                }
            }
        }
        _ => rpc_error(id, -32601, format!("method {:?} not found", request.method)),
    }
}

fn tool_result(value: Value, is_error: bool) -> Value {
    // Keep the compatibility text for hosts that do not read
    // structuredContent, but avoid charging the Agent for pretty-print
    // whitespace on a duplicated representation.
    let text = serde_json::to_string(&value).unwrap_or_else(|_| value.to_string());
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
    vec![
        tool(
            "devices",
            "List authoritative Slots and ownership.",
            object(json!({"slot_id":{"type":"string"}}), &[]),
            true,
        ),
        tool(
            "read",
            "Read bounded serial output; archive requires epoch.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "scope":{"type":"string","enum":["tail","continue","archive"]},
                    "epoch":{"type":"string","format":"uuid"},
                    "after_seq":{"type":"integer","minimum":0},
                    "through_seq":{"type":"integer","minimum":1,"description":"Archive inclusive end."}
                }),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "command",
            "Write in the active Run and capture RX; uses Slot EOL, pacing, and prompts.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "command":{"type":"string","maxLength":4096,"description":"Empty sends Enter."},
                    "expect":{"type":"string","minLength":1},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "timeout_seconds":{"type":"integer","minimum":1,"maximum":120}
                }),
                &["slot_id", "command"],
            ),
            false,
        ),
        tool(
            "input",
            "Write exact UTF-8 bytes without EOL in the active Run.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "text":{"type":"string","minLength":1,"maxLength":4096}
                }),
                &["slot_id", "text"],
            ),
            false,
        ),
        tool(
            "signal",
            "Send Ctrl-C/D/Z or serial Break in the active Run.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "signal":{"type":"string","enum":["ctrl_c","ctrl_d","ctrl_z","break"]},
                    "duration_ms":{"type":"integer","minimum":1,"maximum":5000,"description":"Break only."}
                }),
                &["slot_id", "signal"],
            ),
            false,
        ),
        tool(
            "trigger",
            "Run bounded daemon-side repeated writes and matchers.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "kickoff": trigger_write_schema(MAX_TRIGGER_INITIAL_WRITE_BYTES),
                    "start_contains":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES},
                    "action": trigger_write_schema(MAX_TRIGGER_ACTION_BYTES),
                    "interval_ms":{"type":"integer","minimum":MIN_TRIGGER_INTERVAL_MS,"maximum":MAX_TRIGGER_INTERVAL_MS},
                    "stop_contains":{"type":"array","maxItems":MAX_TRIGGER_PATTERNS,"items":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES}},
                    "timeout_ms":{"type":"integer","minimum":MIN_TRIGGER_TIMEOUT_MS,"maximum":MAX_TRIGGER_TIMEOUT_MS},
                    "max_fires":{"type":"integer","minimum":1,"maximum":MAX_TRIGGER_FIRES}
                }),
                &["slot_id", "action"],
            ),
            false,
        ),
        tool(
            "wait",
            "Wait for RX using a literal, regex, prompt, or quiet boundary.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "expect":{"type":"string","minLength":1},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "timeout_seconds":{"type":"integer","minimum":1,"maximum":120}
                }),
                &["slot_id"],
            ),
            true,
        ),
        tool(
            "search",
            "Search current Run by default; archive requires explicit epoch.",
            object(
                json!({
                    "slot_id":{"type":"string"},
                    "query":{"type":"string","minLength":1,"maxLength":4096},
                    "regex":{"type":"boolean"},
                    "scope":{"type":"string","enum":["current_run","current_cursor","archive"]},
                    "run_id":{"type":"string","format":"uuid"},
                    "epoch":{"type":"string","format":"uuid"},
                    "after_seq":{"type":"integer","minimum":0}
                }),
                &["slot_id", "query"],
            ),
            true,
        ),
        tool(
            "monitor_start",
            "Start a persistent Monitor with one literal or regex matcher.",
            {
                let mut schema = object(
                    json!({
                    "slot_id":{"type":"string"},
                    "contains":{"type":"string","minLength":1,"maxLength":4096},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "description":{"type":"string","minLength":1,"maxLength":1024},
                    "idempotency_key":{"type":"string","format":"uuid","description":"Reuse on retry."}
                    }),
                    &["slot_id"],
                );
                schema["oneOf"] = json!([
                    {"required":["contains"]},
                    {"required":["regex"]}
                ]);
                schema
            },
            false,
        ),
        tool(
            "monitor_list",
            "List persistent Monitors; optionally filter by Slot.",
            object(json!({"slot_id":{"type":"string"}}), &[]),
            true,
        ),
        tool(
            "monitor_status",
            "Get one Monitor's authoritative state.",
            object(
                json!({"monitor_id":{"type":"string","format":"uuid"}}),
                &["monitor_id"],
            ),
            true,
        ),
        tool(
            "monitor_incidents",
            "Read retained incidents; continue with the returned after cursor.",
            object(
                json!({
                    "monitor_id":{"type":"string","format":"uuid"},
                    "after":{"type":"string","pattern":"^[0-9]+$","minLength":1,"maxLength":20}
                }),
                &["monitor_id"],
            ),
            true,
        ),
        tool(
            "monitor_stop",
            "Stop a Monitor; retained incidents remain readable.",
            object(
                json!({"monitor_id":{"type":"string","format":"uuid"}}),
                &["monitor_id"],
            ),
            false,
        ),
        tool(
            "run_start",
            "Acquire write control and start an evidence Run.",
            object(
                json!({
                    "slot_id":{"type":"string"},"label":{"type":"string","minLength":1,"maxLength":128}
                }),
                &["slot_id", "label"],
            ),
            false,
        ),
        tool(
            "run_end",
            "End this MCP's Run and release control.",
            object(json!({"slot_id":{"type":"string"}}), &["slot_id"]),
            false,
        ),
        tool(
            "release",
            "Release control without closing the serial port.",
            object(
                json!({"slot_id":{"type":"string"},"abort_run":{"type":"boolean"}}),
                &["slot_id"],
            ),
            false,
        ),
    ]
}

fn trigger_write_schema(max_length: usize) -> Value {
    json!({
        "type":"object",
        "properties":{
            "text":{"type":"string","maxLength":max_length},
            "eol":{"type":"string","maxLength":max_length}
        },
        "required":["text"],
        "additionalProperties":false
    })
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
                "input",
                "signal",
                "trigger",
                "wait",
                "search",
                "monitor_start",
                "monitor_list",
                "monitor_status",
                "monitor_incidents",
                "monitor_stop",
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
    fn common_tool_schemas_hide_adapter_owned_policy() {
        let tool = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "run_start")
            .unwrap();
        assert!(
            tool["inputSchema"]["properties"]
                .get("control_wait_seconds")
                .is_none()
        );
        let command = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "command")
            .unwrap();
        let mut fields: Vec<_> = command["inputSchema"]["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            ["command", "expect", "regex", "slot_id", "timeout_seconds"]
        );
        for hidden in ["eol", "quiet_ms", "chunk_size", "inter_char_delay_ms"] {
            assert!(command["inputSchema"]["properties"].get(hidden).is_none());
        }
    }

    #[test]
    fn schemas_keep_regex_and_archive_escape_hatches_without_policy_defaults() {
        let tools = tool_definitions();
        let command = tools.iter().find(|tool| tool["name"] == "command").unwrap();
        assert_eq!(
            command["inputSchema"]["properties"]["regex"]["minLength"],
            1
        );
        assert_eq!(
            command["inputSchema"]["properties"]["regex"]["maxLength"],
            4096
        );
        assert!(
            command["inputSchema"]["properties"]["timeout_seconds"]
                .get("default")
                .is_none()
        );

        let search = tools.iter().find(|tool| tool["name"] == "search").unwrap();
        assert!(
            search["description"]
                .as_str()
                .unwrap()
                .contains("archive requires explicit epoch")
        );
        assert!(
            search["inputSchema"]["properties"]["regex"]
                .get("default")
                .is_none()
        );
        assert_eq!(
            search["inputSchema"]["properties"]["query"]["maxLength"],
            4096
        );

        let read = tools.iter().find(|tool| tool["name"] == "read").unwrap();
        assert_eq!(
            read["inputSchema"]["properties"]["through_seq"]["minimum"],
            1
        );

        let wait = tools.iter().find(|tool| tool["name"] == "wait").unwrap();
        assert!(wait["inputSchema"]["properties"].get("after_seq").is_none());
        assert_eq!(
            wait["inputSchema"]["properties"]["regex"]["maxLength"],
            4096
        );
    }

    #[test]
    fn monitor_schemas_keep_daemon_policy_out_of_agent_arguments() {
        let tools = tool_definitions();
        let start = tools
            .iter()
            .find(|tool| tool["name"] == "monitor_start")
            .unwrap();
        let properties = start["inputSchema"]["properties"].as_object().unwrap();
        assert_eq!(start["inputSchema"]["required"], json!(["slot_id"]));
        for expected in [
            "slot_id",
            "contains",
            "regex",
            "description",
            "idempotency_key",
        ] {
            assert!(properties.contains_key(expected));
        }
        assert_eq!(start["inputSchema"]["oneOf"].as_array().unwrap().len(), 2);
        for hidden in [
            "message_center",
            "delivery_mode",
            "cooldown_seconds",
            "max_incidents",
            "poll_interval_ms",
        ] {
            assert!(!properties.contains_key(hidden));
        }

        let incidents = tools
            .iter()
            .find(|tool| tool["name"] == "monitor_incidents")
            .unwrap();
        assert_eq!(incidents["inputSchema"]["required"], json!(["monitor_id"]));
        assert!(
            incidents["inputSchema"]["properties"]
                .get("after")
                .is_some()
        );
        assert!(
            incidents["inputSchema"]["properties"]
                .get("limit")
                .is_none()
        );
    }

    #[test]
    fn tool_result_uses_compact_compatibility_text() {
        let value = json!({"slot_id":"bench","nested":{"ready":true}});
        let result = tool_result(value.clone(), false);
        assert_eq!(
            result["content"][0]["text"],
            r#"{"nested":{"ready":true},"slot_id":"bench"}"#
        );
        assert_eq!(result["structuredContent"], value);
        assert_eq!(result["isError"], false);
    }

    #[test]
    fn signal_schema_covers_raw_controls_and_physical_break() {
        let signal = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "signal")
            .unwrap();
        assert_eq!(
            signal["inputSchema"]["properties"]["signal"]["enum"],
            json!(["ctrl_c", "ctrl_d", "ctrl_z", "break"])
        );
        assert!(
            signal["inputSchema"]["properties"]["duration_ms"]
                .get("default")
                .is_none()
        );
    }

    #[test]
    fn cancellation_notifications_accept_mcp_and_lsp_id_shapes() {
        assert!(is_cancel_notification("notifications/cancelled"));
        assert!(is_cancel_notification("$/cancelRequest"));
        assert_eq!(cancellation_id(&json!({"requestId": 7})), Some(json!(7)));
        assert_eq!(cancellation_id(&json!({"id": "abc"})), Some(json!("abc")));
    }

    #[test]
    fn only_read_only_tool_calls_are_cancellable() {
        for name in [
            "devices",
            "read",
            "wait",
            "search",
            "monitor_list",
            "monitor_status",
            "monitor_incidents",
        ] {
            assert!(request_is_cancellable(&RpcRequest {
                jsonrpc: Some("2.0".into()),
                id: Some(json!(1)),
                method: "tools/call".into(),
                params: json!({"name": name, "arguments": {}}),
            }));
        }
        for name in [
            "command",
            "input",
            "signal",
            "trigger",
            "monitor_start",
            "monitor_stop",
            "run_start",
            "run_end",
            "release",
        ] {
            assert!(!request_is_cancellable(&RpcRequest {
                jsonrpc: Some("2.0".into()),
                id: Some(json!(1)),
                method: "tools/call".into(),
                params: json!({"name": name, "arguments": {}}),
            }));
        }
    }

    #[test]
    fn trigger_schema_is_generic_and_uses_only_explicit_call_bytes() {
        let trigger = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "trigger")
            .unwrap();
        let schema = &trigger["inputSchema"];
        assert_eq!(schema["required"], json!(["slot_id", "action"]));
        assert!(
            schema["properties"]["action"]["properties"]["eol"]
                .get("default")
                .is_none()
        );
        assert!(schema["properties"].get("kickoff").is_some());
        assert!(schema["properties"].get("initial_write").is_none());
        assert!(
            schema["properties"]["stop_contains"]
                .get("default")
                .is_none()
        );
        assert!(schema["properties"].get("chunk_size").is_none());
        assert!(schema["properties"].get("inter_char_delay_ms").is_none());
        let serialized = serde_json::to_string(&trigger).unwrap().to_lowercase();
        for forbidden in ["sigmastar", "uboot"] {
            assert!(
                !serialized.contains(forbidden),
                "trigger schema contains device-specific term {forbidden:?}"
            );
        }
    }

    #[test]
    fn report_tool_definition_json_size() {
        let bytes = serde_json::to_vec(&tool_definitions()).unwrap().len();
        eprintln!("tool_definition_json_bytes={bytes}");
        // v0.5 adds five persistent Monitor operations while keeping the
        // complete 16-tool MCP surface below a bounded prompt cost.
        assert!(bytes <= 7_800, "tool definitions grew to {bytes} bytes");
        for tool in tool_definitions() {
            assert!(
                tool["description"].as_str().unwrap().len() <= 180,
                "{} description is too large",
                tool["name"]
            );
        }
    }
}
