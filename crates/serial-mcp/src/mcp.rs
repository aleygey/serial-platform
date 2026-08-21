use std::{
    collections::HashMap,
    io::{BufRead, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header::ORIGIN},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use serial_protocol::{
    DEFAULT_TRIGGER_INTERVAL_MS, DEFAULT_TRIGGER_MAX_FIRES, DEFAULT_TRIGGER_TIMEOUT_MS,
    MAX_COMMAND_DESCRIPTION_BYTES, MAX_TRIGGER_ACTION_BYTES, MAX_TRIGGER_FIRES,
    MAX_TRIGGER_INITIAL_WRITE_BYTES, MAX_TRIGGER_INTERVAL_MS, MAX_TRIGGER_PATTERN_BYTES,
    MAX_TRIGGER_PATTERNS, MAX_TRIGGER_TIMEOUT_MS, MIN_TRIGGER_INTERVAL_MS, MIN_TRIGGER_TIMEOUT_MS,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::tools::AgentTools;

const LATEST_PROTOCOL: &str = "2025-11-25";
const SUPPORTED_PROTOCOLS: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];
const MAX_COMMAND_SEQUENCE_STEPS: usize = 8;
const SERVER_INSTRUCTIONS: &str = "Inspect devices and model_profiles before executing commands. Start a Run before writes and pass the opaque run_handle returned by run_start to every Run-scoped tool. Runs scope evidence only. Before the final reply, call run_end unless deliberately handing the live Run to a continuing agent workflow. Every command requires a concise purpose for durable history. Use command_sequence for dependent interactions such as username then password; every non-final step needs an explicit expect or regex boundary, and a failed step prevents later writes. command and command_sequence use the selected port's transport_profile and model_profile; input and signal are raw. Monitor Jobs persist after this MCP process exits; stop them when no longer needed.";

pub async fn serve_stdio(tools: AgentTools) -> Result<()> {
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

    // Closing stdin cancels only read-only observations. A command, command
    // sequence, signal, Run transition, release, or Trigger may have crossed its
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

#[derive(Clone)]
struct HttpState {
    tools: AgentTools,
    active_requests: ActiveRequests,
    listen: SocketAddr,
}

pub async fn serve_http(tools: AgentTools, listen: SocketAddr, managed: bool) -> Result<()> {
    if listen.ip() != IpAddr::V4(Ipv4Addr::LOCALHOST) {
        anyhow::bail!("Streamable HTTP MCP must listen on 127.0.0.1");
    }
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let state = HttpState {
        tools,
        active_requests: Arc::new(Mutex::new(HashMap::new())),
        listen,
    };
    axum::serve(
        listener,
        Router::new()
            .route("/mcp", post(http_post))
            .with_state(state),
    )
    .with_graceful_shutdown(http_shutdown_signal(managed))
    .await?;
    Ok(())
}

async fn http_shutdown_signal(managed: bool) {
    if managed {
        use tokio::io::AsyncReadExt as _;

        let mut stdin = tokio::io::stdin();
        let mut buffer = [0_u8; 256];
        loop {
            match stdin.read(&mut buffer).await {
                Ok(0) => return,
                Ok(_) => {}
                Err(error) => {
                    eprintln!("serial-mcp: failed to read the managed shutdown pipe: {error}");
                    return;
                }
            }
        }
    } else {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn http_post(State(state): State<HttpState>, headers: HeaderMap, body: Bytes) -> Response {
    if !origin_allowed(&headers, state.listen) {
        return (StatusCode::FORBIDDEN, "origin is not allowed").into_response();
    }
    if let Some(version) = headers
        .get("MCP-Protocol-Version")
        .and_then(|value| value.to_str().ok())
        && !SUPPORTED_PROTOCOLS.contains(&version)
    {
        return (StatusCode::BAD_REQUEST, "unsupported MCP-Protocol-Version").into_response();
    }
    let request = match serde_json::from_slice::<RpcRequest>(&body) {
        Ok(request) => request,
        Err(error) => {
            return Json(rpc_error(
                Value::Null,
                -32700,
                format!("parse error: {error}"),
            ))
            .into_response();
        }
    };
    if request.jsonrpc.as_deref() != Some("2.0") {
        return request.id.map_or_else(
            || StatusCode::ACCEPTED.into_response(),
            |id| Json(rpc_error(id, -32600, "jsonrpc must be 2.0")).into_response(),
        );
    }
    if is_cancel_notification(&request.method) {
        if let Some(cancelled_id) = cancellation_id(&request.params) {
            cancel_request(&state.active_requests, &cancelled_id);
        }
        return StatusCode::ACCEPTED.into_response();
    }
    let Some(id) = request.id.clone() else {
        return StatusCode::ACCEPTED.into_response();
    };
    let key = request_key(&id);
    let (cancel_tx, cancel_rx) = if request_is_cancellable(&request) {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    {
        let mut active = state
            .active_requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if active.contains_key(&key) {
            return Json(rpc_error(id, -32600, "request id is already active")).into_response();
        }
        active.insert(key.clone(), cancel_tx);
    }
    let response = match cancel_rx {
        Some(cancel_rx) => tokio::select! {
            _ = cancel_rx => rpc_error(id.clone(), -32800, "request cancelled"),
            response = dispatch_request(&state.tools, request, id.clone()) => response,
        },
        None => dispatch_request(&state.tools, request, id).await,
    };
    state
        .active_requests
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&key);
    Json(response).into_response()
}

fn origin_allowed(headers: &HeaderMap, listen: SocketAddr) -> bool {
    let Some(origin) = headers.get(ORIGIN) else {
        return true;
    };
    let Ok(origin) = origin.to_str() else {
        return false;
    };
    let Ok(url) = reqwest::Url::parse(origin) else {
        return false;
    };
    let local_host = matches!(url.host_str(), Some("localhost" | "127.0.0.1"));
    local_host && url.port_or_known_default() == Some(listen.port())
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
            | "model_profiles"
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
                    "instructions": SERVER_INSTRUCTIONS
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
                Err(error) => rpc_result(
                    id,
                    tool_result(
                        crate::api::structured_http_error(&error)
                            .or_else(|| crate::tools::structured_tool_error(&error))
                            .unwrap_or_else(|| json!({"error": {"code": "tool_error", "message": error.to_string()}})),
                        true,
                    ),
                ),
            }
        }
        _ => rpc_error(id, -32601, format!("method {:?} not found", request.method)),
    }
}

fn tool_result(value: Value, is_error: bool) -> Value {
    // Keep the compatibility text for hosts that do not read
    // structuredContent, but avoid charging the Agent for pretty-print
    // whitespace on a duplicated representation.
    let text = if is_error {
        value
            .pointer("/error/message")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| serde_json::to_string(&value).unwrap_or_else(|_| value.to_string()))
    } else {
        serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
    };
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
    // These annotations are safety hints consumed by MCP hosts. Serial writes
    // can change or reboot a physical DUT, so they must not be advertised as
    // harmless merely because the adapter itself keeps an audit trail.
    let destructive = matches!(
        name,
        "model_profile_set"
            | "command"
            | "command_sequence"
            | "input"
            | "signal"
            | "trigger"
            | "monitor_stop"
    );
    let open_world = matches!(
        name,
        "devices"
            | "read"
            | "command"
            | "command_sequence"
            | "input"
            | "signal"
            | "trigger"
            | "wait"
            | "search"
    );
    json!({
        "name": name, "description": description, "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": destructive,
            "idempotentHint": read_only,
            "openWorldHint": open_world
        }
    })
}

pub fn tool_definitions() -> Vec<Value> {
    vec![
        tool(
            "devices",
            "List serial ports, configured profiles, and ownership.",
            object(json!({"port":{"type":"string"}}), &[]),
            true,
        ),
        tool(
            "model_profiles",
            "Read model profiles and their current port bindings.",
            object(json!({"port":{"type":"string"}}), &[]),
            true,
        ),
        tool(
            "model_profile_set",
            "Create or update one model profile and bind it to a port; pass null to detach the port.",
            object(
                json!({
                    "port":{"type":"string","minLength":1},
                    "profile":{"description":"Complete model profile, or null to detach.","anyOf":[
                        {"type":"object","additionalProperties":false,"required":["name"],"properties":{
                            "name":{"type":"string","minLength":1,"maxLength":64},
                            "shell_prompt":{"type":["string","null"],"minLength":1,"maxLength":4096},
                            "uboot_prompt":{"type":["string","null"],"minLength":1,"maxLength":4096},
                            "write_eol":{"type":["string","null"]},
                            "echo":{"type":["string","null"],"enum":["on","off","auto",null]},
                            "write_chunk_size":{"type":["integer","null"],"minimum":1},
                            "write_chunk_delay_ms":{"type":["integer","null"],"minimum":0,"maximum":10000}
                        }},
                        {"type":"null"}
                    ]}
                }),
                &["port", "profile"],
            ),
            false,
        ),
        tool(
            "read",
            "Read bounded serial output; archive requires epoch.",
            object(
                json!({
                    "port":{"type":"string"},
                    "scope":{"type":"string","enum":["tail","continue","archive"]},
                    "epoch":{"type":"string","format":"uuid"},
                    "after_seq":{"type":"integer","minimum":0},
                    "through_seq":{"type":"integer","minimum":1,"description":"Archive inclusive end."}
                }),
                &["port"],
            ),
            true,
        ),
        tool(
            "command",
            "Write in the active Run and capture RX; include a concise purpose for the durable Run command history.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "command":{"type":"string","maxLength":4096,"description":"Empty sends Enter."},
                    "description":{"type":"string","minLength":1,"maxLength":MAX_COMMAND_DESCRIPTION_BYTES,"description":"Concise human-readable purpose, for example: 查看样机内存。"},
                    "expect":{"type":"string","minLength":1},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "timeout_seconds":{"type":"integer","minimum":1,"maximum":120}
                }),
                &["run_handle", "command", "description"],
            ),
            false,
        ),
        tool(
            "command_sequence",
            "Run 1-8 dependent commands in order; each non-final step needs a matcher, and failure stops before every later write.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "description":{"type":"string","minLength":1,"maxLength":MAX_COMMAND_DESCRIPTION_BYTES,"description":"Concise human-readable purpose for the complete dependent interaction."},
                    "steps":{
                        "type":"array",
                        "minItems":1,
                        "maxItems":MAX_COMMAND_SEQUENCE_STEPS,
                        "description":"Ordered dependent steps. Every non-final step requires expect or regex. Planned writes are limited to 32768 bytes and effective timeouts to 300 seconds.",
                        "items":{
                            "type":"object",
                            "properties":{
                                "command":{"type":"string","maxLength":4096,"description":"Empty sends Enter; command plus effective EOL is limited to 4096 UTF-8 bytes."},
                                "description":{"type":"string","minLength":1,"maxLength":MAX_COMMAND_DESCRIPTION_BYTES,"description":"Concise human-readable purpose retained with this step's TX audit."},
                                "expect":{"type":"string","minLength":1,"maxLength":4096},
                                "regex":{"type":"string","minLength":1,"maxLength":4096},
                                "timeout_seconds":{"type":"integer","minimum":1,"maximum":120,"description":"Per-step deadline; defaults to 10 seconds."}
                            },
                            "required":["command","description"],
                            "additionalProperties":false
                        }
                    }
                }),
                &["run_handle", "description", "steps"],
            ),
            false,
        ),
        tool(
            "input",
            "Write exact UTF-8 bytes without EOL in the active Run.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "text":{"type":"string","minLength":1,"maxLength":4096}
                }),
                &["run_handle", "text"],
            ),
            false,
        ),
        tool(
            "signal",
            "Send Ctrl-C/D/Z or serial Break in the active Run.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "signal":{"type":"string","enum":["ctrl_c","ctrl_d","ctrl_z","break"]},
                    "duration_ms":{"type":"integer","minimum":1,"maximum":5000,"description":"Break only."}
                }),
                &["run_handle", "signal"],
            ),
            false,
        ),
        tool(
            "trigger",
            "Optional kickoff, then bounded actions. Omit start_contains normally; set it only to gate the first action on live RX.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "kickoff": trigger_write_schema(MAX_TRIGGER_INITIAL_WRITE_BYTES),
                    "start_contains":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES,"description":"Advanced RX gate; omit for normal kickoff-then-action."},
                    "action": trigger_write_schema(MAX_TRIGGER_ACTION_BYTES),
                    "interval_ms":{"type":"integer","minimum":MIN_TRIGGER_INTERVAL_MS,"maximum":MAX_TRIGGER_INTERVAL_MS,"default":DEFAULT_TRIGGER_INTERVAL_MS},
                    "stop_contains":{"type":"array","maxItems":MAX_TRIGGER_PATTERNS,"description":"Observation continues after max_fires until match or timeout.","items":{"type":"string","minLength":1,"maxLength":MAX_TRIGGER_PATTERN_BYTES}},
                    "timeout_ms":{"type":"integer","minimum":MIN_TRIGGER_TIMEOUT_MS,"maximum":MAX_TRIGGER_TIMEOUT_MS,"default":DEFAULT_TRIGGER_TIMEOUT_MS},
                    "max_fires":{"type":"integer","minimum":1,"maximum":MAX_TRIGGER_FIRES,"default":DEFAULT_TRIGGER_MAX_FIRES,"description":"Confirmed-action send budget, not an observation cutoff."}
                }),
                &["run_handle", "action"],
            ),
            false,
        ),
        tool(
            "wait",
            "Wait for RX using a literal, regex, prompt, or quiet boundary.",
            object(
                json!({
                    "run_handle":run_handle_schema(),
                    "expect":{"type":"string","minLength":1},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "timeout_seconds":{"type":"integer","minimum":1,"maximum":120}
                }),
                &["run_handle"],
            ),
            true,
        ),
        tool(
            "search",
            "Search current Run by default; archive requires explicit epoch.",
            object(
                json!({
                    "port":{"type":"string"},
                    "query":{"type":"string","minLength":1,"maxLength":4096},
                    "regex":{"type":"boolean"},
                    "scope":{"type":"string","enum":["current_run","current_cursor","archive"]},
                    "run_id":{"type":"string","format":"uuid"},
                    "epoch":{"type":"string","format":"uuid"},
                    "after_seq":{"type":"integer","minimum":0}
                }),
                &["port", "query"],
            ),
            true,
        ),
        tool(
            "monitor_start",
            "Start a persistent Monitor with one literal or regex matcher.",
            {
                let mut schema = object(
                    json!({
                    "port":{"type":"string"},
                    "contains":{"type":"string","minLength":1,"maxLength":4096},
                    "regex":{"type":"string","minLength":1,"maxLength":4096},
                    "description":{"type":"string","minLength":1,"maxLength":1024},
                    "idempotency_key":{"type":"string","format":"uuid","description":"Reuse on retry."}
                    }),
                    &["port"],
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
            "List persistent Monitors; optionally filter by port.",
            object(json!({"port":{"type":"string"}}), &[]),
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
            "Confirm the physical DUT by serial, telnet, web UI, or a human, then acquire control and return one opaque run_handle.",
            object(
                json!({
                    "port":{"type":"string"},"label":{"type":"string","minLength":1,"maxLength":128}
                }),
                &["port", "label"],
            ),
            false,
        ),
        tool(
            "run_end",
            "End the Run authorized by run_handle and release control; call before the final reply unless deliberately handing off.",
            object(
                json!({
                    "run_handle":run_handle_schema()
                }),
                &["run_handle"],
            ),
            false,
        ),
        tool(
            "release",
            "Release only this MCP's control; no local lease is a no-op, while aborting its active Run requires run_handle.",
            {
                let mut schema = object(
                    json!({
                        "port":{"type":"string"},
                        "abort_run":{"type":"boolean"},
                        "run_handle":run_handle_schema()
                    }),
                    &[],
                );
                schema["oneOf"] = json!([
                    {
                        "required":["port"],
                        "properties":{"abort_run":{"const":false}},
                        "not":{"required":["run_handle"]}
                    },
                    {
                        "required":["run_handle","abort_run"],
                        "properties":{"abort_run":{"const":true}},
                        "not":{"required":["port"]}
                    }
                ]);
                schema
            },
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

fn run_handle_schema() -> Value {
    json!({
        "type":"string",
        "minLength":22,
        "maxLength":22,
        "pattern":"^[A-Za-z0-9_-]{22}$"
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;

    fn test_http_app() -> Router {
        let endpoint = "http://127.0.0.1:1".to_string();
        let tools = AgentTools::new(
            crate::api::ApiClient::new(endpoint.clone()).unwrap(),
            crate::session::SessionHandle::spawn(
                endpoint,
                "test-agent".into(),
                Some(std::time::Duration::from_secs(1_800)),
            ),
            "test-agent".into(),
            crate::config::CaptureLimits::default(),
        );
        let state = HttpState {
            tools,
            active_requests: Arc::new(Mutex::new(HashMap::new())),
            listen: "127.0.0.1:3211".parse().unwrap(),
        };
        Router::new()
            .route("/mcp", post(http_post))
            .with_state(state)
    }

    #[tokio::test]
    async fn streamable_http_request_notification_and_get_contract() {
        let initialize = test_http_app()
            .oneshot(
                Request::post("/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({
                            "jsonrpc":"2.0",
                            "id":1,
                            "method":"initialize",
                            "params":{"protocolVersion":LATEST_PROTOCOL}
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(initialize.status(), StatusCode::OK);
        let payload: Value =
            serde_json::from_slice(&to_bytes(initialize.into_body(), 256 * 1024).await.unwrap())
                .unwrap();
        assert_eq!(payload["result"]["protocolVersion"], LATEST_PROTOCOL);

        let notification = test_http_app()
            .oneshot(
                Request::post("/mcp")
                    .body(Body::from(
                        json!({"jsonrpc":"2.0","method":"notifications/initialized"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(notification.status(), StatusCode::ACCEPTED);
        assert!(
            to_bytes(notification.into_body(), 16)
                .await
                .unwrap()
                .is_empty()
        );

        let get = test_http_app()
            .oneshot(Request::get("/mcp").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn streamable_http_validates_origin_and_protocol_header() {
        let notification = || {
            Body::from(json!({"jsonrpc":"2.0","method":"notifications/initialized"}).to_string())
        };
        let local = test_http_app()
            .oneshot(
                Request::post("/mcp")
                    .header("origin", "http://localhost:3211")
                    .header("MCP-Protocol-Version", LATEST_PROTOCOL)
                    .body(notification())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(local.status(), StatusCode::ACCEPTED);

        for origin in ["http://evil.example:3211", "http://127.0.0.1:9999"] {
            let response = test_http_app()
                .oneshot(
                    Request::post("/mcp")
                        .header("origin", origin)
                        .body(notification())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{origin}");
        }

        let unsupported = test_http_app()
            .oneshot(
                Request::post("/mcp")
                    .header("MCP-Protocol-Version", "2000-01-01")
                    .body(notification())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);
    }

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
                "model_profiles",
                "model_profile_set",
                "read",
                "command",
                "command_sequence",
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
    fn agent_guidance_explains_runs_sequences_and_profiles() {
        assert!(SERVER_INSTRUCTIONS.contains("Before the final reply, call run_end"));
        assert!(SERVER_INSTRUCTIONS.contains("Every command requires"));
        assert!(SERVER_INSTRUCTIONS.contains("Use command_sequence"));
        assert!(SERVER_INSTRUCTIONS.contains("every non-final step"));
        assert!(SERVER_INSTRUCTIONS.contains("failed step prevents later writes"));
        assert!(SERVER_INSTRUCTIONS.contains("transport_profile and model_profile"));
    }

    #[test]
    fn schemas_reject_unknown_arguments() {
        for tool in tool_definitions() {
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
        }
    }

    #[test]
    fn annotations_do_not_hide_physical_or_persistent_side_effects() {
        let tools = tool_definitions();
        for name in [
            "model_profile_set",
            "command",
            "command_sequence",
            "input",
            "signal",
            "trigger",
            "monitor_stop",
        ] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(tool["annotations"]["destructiveHint"], true, "{name}");
        }
        for name in [
            "devices",
            "read",
            "command",
            "command_sequence",
            "input",
            "signal",
            "trigger",
            "wait",
            "search",
        ] {
            let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
            assert_eq!(tool["annotations"]["openWorldHint"], true, "{name}");
        }
        let catalog = tools
            .iter()
            .find(|tool| tool["name"] == "model_profiles")
            .unwrap();
        assert_eq!(catalog["annotations"]["readOnlyHint"], true);
        assert_eq!(catalog["annotations"]["destructiveHint"], false);
        assert_eq!(catalog["annotations"]["openWorldHint"], false);
    }

    #[test]
    fn model_profile_tools_expose_one_profile_and_port_binding_contract() {
        let tools = tool_definitions();
        let read = tools
            .iter()
            .find(|tool| tool["name"] == "model_profiles")
            .unwrap();
        assert_eq!(read["annotations"]["readOnlyHint"], true);
        assert!(read["inputSchema"]["properties"].get("port").is_some());

        let set = tools
            .iter()
            .find(|tool| tool["name"] == "model_profile_set")
            .unwrap();
        assert_eq!(set["annotations"]["readOnlyHint"], false);
        assert_eq!(set["inputSchema"]["required"], json!(["port", "profile"]));
        let properties = set["inputSchema"]["properties"].as_object().unwrap();
        assert!(properties.contains_key("port"));
        assert!(properties.contains_key("profile"));
        assert_eq!(properties.len(), 2);
        let profile = &properties["profile"]["anyOf"][0];
        assert_eq!(profile["required"], json!(["name"]));
        for field in ["name", "shell_prompt", "uboot_prompt", "write_eol", "echo"] {
            assert!(
                profile["properties"].get(field).is_some(),
                "missing {field}"
            );
        }
        let serialized = serde_json::to_string(set).unwrap();
        assert!(!serialized.contains("slot_id"));
        assert!(!serialized.contains("device_model"));
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
            [
                "command",
                "description",
                "expect",
                "regex",
                "run_handle",
                "timeout_seconds"
            ]
        );
        assert_eq!(
            command["inputSchema"]["required"],
            json!(["run_handle", "command", "description"])
        );
        assert_eq!(
            command["inputSchema"]["properties"]["description"]["maxLength"],
            MAX_COMMAND_DESCRIPTION_BYTES
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
    fn command_sequence_schema_is_bounded_strict_and_step_described() {
        let sequence = tool_definitions()
            .into_iter()
            .find(|tool| tool["name"] == "command_sequence")
            .unwrap();
        let schema = &sequence["inputSchema"];
        assert_eq!(
            schema["required"],
            json!(["run_handle", "description", "steps"])
        );
        assert_eq!(
            schema["properties"]["description"]["maxLength"],
            MAX_COMMAND_DESCRIPTION_BYTES
        );
        let steps = &schema["properties"]["steps"];
        assert_eq!(steps["minItems"], 1);
        assert_eq!(steps["maxItems"], MAX_COMMAND_SEQUENCE_STEPS);
        let steps_description = steps["description"].as_str().unwrap();
        for constraint in ["non-final", "32768", "300"] {
            assert!(steps_description.contains(constraint));
        }
        let item = &steps["items"];
        assert_eq!(item["additionalProperties"], false);
        assert_eq!(item["required"], json!(["command", "description"]));
        let mut fields: Vec<_> = item["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        fields.sort_unstable();
        assert_eq!(
            fields,
            [
                "command",
                "description",
                "expect",
                "regex",
                "timeout_seconds"
            ]
        );
        assert_eq!(item["properties"]["command"]["maxLength"], 4096);
        assert_eq!(
            item["properties"]["description"]["maxLength"],
            MAX_COMMAND_DESCRIPTION_BYTES
        );
        assert_eq!(item["properties"]["regex"]["maxLength"], 4096);
        assert_eq!(item["properties"]["expect"]["maxLength"], 4096);
        assert_eq!(item["properties"]["timeout_seconds"]["minimum"], 1);
        assert_eq!(item["properties"]["timeout_seconds"]["maximum"], 120);
        assert!(
            sequence["description"]
                .as_str()
                .unwrap()
                .contains("failure stops")
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
        assert_eq!(start["inputSchema"]["required"], json!(["port"]));
        for expected in [
            "port",
            "contains",
            "regex",
            "description",
            "idempotency_key",
        ] {
            assert!(properties.contains_key(expected));
        }
        assert_eq!(start["inputSchema"]["oneOf"].as_array().unwrap().len(), 2);
        for hidden in [
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
    fn tool_result_uses_compact_text_and_structured_content() {
        let value = json!({"port":"bench","nested":{"ready":true}});
        let result = tool_result(value.clone(), false);
        assert_eq!(
            result["content"][0]["text"],
            r#"{"nested":{"ready":true},"port":"bench"}"#
        );
        assert_eq!(result["structuredContent"], value);
        assert_eq!(result["isError"], false);
    }

    #[test]
    fn structured_error_text_is_plain_message_not_nested_json() {
        let value = json!({
            "error": {
                "source": "seriald",
                "http_status": 429,
                "code": "query_budget_exceeded",
                "message": "journal query budget was exceeded",
                "phase": "segment discovery"
            }
        });
        let result = tool_result(value.clone(), true);
        assert_eq!(
            result["content"][0]["text"],
            "journal query budget was exceeded"
        );
        assert_eq!(result["structuredContent"], value);
        assert_eq!(result["isError"], true);
        assert!(
            !result["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains(r#"\{"#)
        );
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
            "model_profiles",
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
            "model_profile_set",
            "command",
            "command_sequence",
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
        assert_eq!(schema["required"], json!(["run_handle", "action"]));
        assert!(
            schema["properties"]["action"]["properties"]["eol"]
                .get("default")
                .is_none()
        );
        assert!(schema["properties"].get("kickoff").is_some());
        assert!(schema["properties"].get("initial_write").is_none());
        let start_description = schema["properties"]["start_contains"]["description"]
            .as_str()
            .unwrap();
        assert!(start_description.contains("Advanced RX gate"));
        assert!(start_description.contains("omit for normal"));
        assert!(
            schema["properties"]["stop_contains"]
                .get("default")
                .is_none()
        );
        assert!(schema["properties"].get("chunk_size").is_none());
        assert!(schema["properties"].get("inter_char_delay_ms").is_none());
        assert_eq!(
            schema["properties"]["interval_ms"]["default"],
            DEFAULT_TRIGGER_INTERVAL_MS
        );
        assert_eq!(
            schema["properties"]["timeout_ms"]["default"],
            DEFAULT_TRIGGER_TIMEOUT_MS
        );
        assert_eq!(
            schema["properties"]["max_fires"]["default"],
            DEFAULT_TRIGGER_MAX_FIRES
        );
        assert!(
            schema["properties"]["max_fires"]["description"]
                .as_str()
                .unwrap()
                .contains("send budget")
        );
        assert!(
            schema["properties"]["stop_contains"]["description"]
                .as_str()
                .unwrap()
                .contains("after max_fires")
        );
        let serialized = serde_json::to_string(&trigger).unwrap().to_lowercase();
        for forbidden in ["sigmastar", "uboot"] {
            assert!(
                !serialized.contains(forbidden),
                "trigger schema contains device-specific term {forbidden:?}"
            );
        }
    }

    #[test]
    fn run_scoped_tools_require_only_one_opaque_run_handle() {
        let tools = tool_definitions();
        for name in [
            "command",
            "command_sequence",
            "input",
            "signal",
            "trigger",
            "wait",
            "run_end",
        ] {
            let schema = &tools.iter().find(|tool| tool["name"] == name).unwrap()["inputSchema"];
            let required = schema["required"].as_array().unwrap();
            assert_eq!(
                required.iter().filter(|item| *item == "run_handle").count(),
                1
            );
            assert_eq!(
                schema["properties"]["run_handle"]["type"], "string",
                "{name}"
            );
            assert_eq!(
                schema["properties"]["run_handle"]["minLength"], 22,
                "{name}"
            );
            assert_eq!(
                schema["properties"]["run_handle"]["maxLength"], 22,
                "{name}"
            );
            for removed_field in ["port", "run_id", "run_token"] {
                assert!(
                    schema["properties"].get(removed_field).is_none(),
                    "{name} exposes {removed_field}"
                );
            }
        }

        let release = tools.iter().find(|tool| tool["name"] == "release").unwrap();
        assert_eq!(release["inputSchema"]["required"], json!([]));
        let modes = release["inputSchema"]["oneOf"].as_array().unwrap();
        assert_eq!(modes.len(), 2);
        assert_eq!(modes[0]["required"], json!(["port"]));
        assert_eq!(modes[0]["properties"]["abort_run"]["const"], false);
        assert_eq!(modes[0]["not"]["required"], json!(["run_handle"]));
        assert_eq!(modes[1]["required"], json!(["run_handle", "abort_run"]));
        assert_eq!(modes[1]["properties"]["abort_run"]["const"], true);
        assert_eq!(modes[1]["not"]["required"], json!(["port"]));
        assert!(
            release["inputSchema"]["properties"]
                .get("run_handle")
                .is_some()
        );
        assert!(release["inputSchema"]["properties"].get("run_id").is_none());
        assert!(
            release["inputSchema"]["properties"]
                .get("run_token")
                .is_none()
        );
    }

    #[test]
    fn report_tool_definition_json_size() {
        let bytes = serde_json::to_vec(&tool_definitions()).unwrap().len();
        eprintln!("tool_definition_json_bytes={bytes}");
        // The eight Run-scoped tools intentionally repeat one compact opaque
        // capability schema so hosts cannot omit it while prompt cost stays bounded.
        assert!(bytes <= 13_000, "tool definitions grew to {bytes} bytes");
        for tool in tool_definitions() {
            assert!(
                tool["description"].as_str().unwrap().len() <= 180,
                "{} description is too large",
                tool["name"]
            );
        }
    }
}
