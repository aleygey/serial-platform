//! Exercise the complete tool dispatcher against a socket/HTTP test daemon.
//! No physical serial port is opened; the real daemon VM has separate PTY tests.
use super::*;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use serial_protocol::*;
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone)]
struct TestDaemon(Arc<Mutex<DaemonData>>);
struct DaemonData {
    slot: SlotSnapshot,
    actor: Actor,
    lease: ControlLease,
    run: RunInfo,
    execution: Option<MacroExecutionInfo>,
    requests: Vec<ClientMessage>,
    stop_polls: usize,
    uncertain_start: bool,
    evidence_after: u64,
    evidence_truncated: bool,
}

async fn fixture(uncertain_start: bool) -> (AgentTools, TestDaemon, tokio::task::JoinHandle<()>) {
    let mut slot = crate::tools::completion_tests::slot(Some("root# "), Some("U-Boot> "));
    slot.config.port = "COM6".into();
    slot.head_seq = 17;
    let actor = Actor {
        id: "agent:fixture".into(),
        label: "test".into(),
        kind: ActorKind::Agent,
    };
    let lease = ControlLease {
        id: Uuid::new_v4(),
        owner: actor.clone(),
        epoch: slot.daemon_epoch,
        generation: slot.generation,
        fence: 1,
        issued_wall_time_ns: 0,
        expires_wall_time_ns: i64::MAX,
    };
    let run = RunInfo {
        id: Uuid::new_v4(),
        owner: actor.clone(),
        label: "test".into(),
        status: RunStatus::Active,
        start_seq: 17,
        end_seq: None,
        metadata: Default::default(),
    };
    let data = TestDaemon(Arc::new(Mutex::new(DaemonData {
        slot,
        actor,
        lease,
        run,
        execution: None,
        requests: Vec::new(),
        stop_polls: 0,
        uncertain_start,
        evidence_after: 17,
        evidence_truncated: false,
    })));
    let router = Router::new()
        .route("/api/v1/ws",get(ws_upgrade))
        .route("/api/v1/status",get(status))
        .route("/api/v1/macros",get(|| async { Json(json!({"catalog_revision":1,"macros":[],"definition":null,"total":0,"next_offset":null})) }))
        .route("/api/v1/ports/COM6/events",get(evidence))
        .route("/api/v1/ports/COM6/recent-activity",get(evidence))
        .with_state(data.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let tools = AgentTools::new(
        crate::api::ApiClient::new(endpoint.clone()).unwrap(),
        crate::session::SessionHandle::spawn(
            endpoint,
            "test".into(),
            Some(Duration::from_secs(1800)),
            None,
        ),
        "test".into(),
        crate::config::CaptureLimits::default(),
    );
    (tools, data, server)
}

async fn status(State(data): State<TestDaemon>) -> Json<StatusResponse> {
    let data = data.0.lock().unwrap();
    Json(StatusResponse {
        server_id: Uuid::nil(),
        daemon_epoch: data.slot.daemon_epoch,
        protocol_version: PROTOCOL_VERSION,
        config_revision: 1,
        sequence_write_precondition_supported: true,
        serial_context_precondition_supported: true,
        ports: vec![data.slot.clone()],
    })
}
async fn evidence(State(data): State<TestDaemon>) -> Json<EventQueryResponse> {
    let data = data.0.lock().unwrap();
    Json(EventQueryResponse {
        events: Vec::new(),
        next_cursor: Some(Cursor {
            epoch: data.slot.daemon_epoch,
            after_seq: data.evidence_after,
        }),
        truncated: data.evidence_truncated,
        first_available_seq: Some(17),
        gaps: Vec::new(),
    })
}
async fn ws_upgrade(
    State(data): State<TestDaemon>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    upgrade.on_upgrade(|socket| serve_socket(socket, data))
}
async fn serve_socket(mut socket: WebSocket, data: TestDaemon) {
    while let Some(Ok(Message::Binary(bytes))) = socket.recv().await {
        let request = decode_client_control(&bytes).unwrap();
        let response = {
            let mut data = data.0.lock().unwrap();
            data.requests.push(request.clone());
            let result = match &request {
                ClientMessage::Hello { .. } => {
                    let response = ServerMessage::Welcome {
                        server_id: Uuid::nil(),
                        daemon_epoch: data.slot.daemon_epoch,
                        protocol_version: PROTOCOL_VERSION,
                        actor: data.actor.clone(),
                    };
                    Some(response)
                }
                ClientMessage::RequestRunStart { .. } => {
                    data.slot.active_run = Some(data.run.clone());
                    data.slot.control = Some(data.lease.clone());
                    Some(ServerMessage::Result {
                        request_id: request.request_id(),
                        result: CommandResult::RunStartGranted {
                            approval_id: request.request_id(),
                            lease: data.lease.clone(),
                            run: data.run.clone(),
                        },
                    })
                }
                ClientMessage::RenewControl { .. } => Some(ServerMessage::Result {
                    request_id: request.request_id(),
                    result: CommandResult::ControlRenewed {
                        lease: data.lease.clone(),
                    },
                }),
                ClientMessage::MacroStart {
                    operation_id,
                    expected_run_id,
                    spec,
                    ..
                } => {
                    assert_eq!(*expected_run_id, Some(data.run.id));
                    data.execution = Some(MacroExecutionInfo {
                        id: *operation_id,
                        port: "COM6".into(),
                        daemon_epoch: data.slot.daemon_epoch,
                        generation: data.slot.generation,
                        owner: data.actor.clone(),
                        run_id: Some(data.run.id),
                        macro_id: spec.macro_id.clone(),
                        revision: spec.revision,
                        description: spec.description.clone().unwrap_or("test".into()),
                        status: MacroStatus::Running,
                        started_at_ns: 0,
                        completed_at_ns: None,
                        line: 1,
                        column: 1,
                        writes: 0,
                        input_verified_writes: 0,
                        send_only_writes: 0,
                        bytes_written: 0,
                        first_seq: 17,
                        through_seq: 17,
                        message: None,
                        outcome_uncertain: false,
                    });
                    if data.uncertain_start {
                        Some(ServerMessage::Error {
                            request_id: Some(request.request_id()),
                            code: ErrorCode::WriteOutcomeUncertain,
                            message: "start reply uncertain".into(),
                            retryable: false,
                        })
                    } else {
                        Some(ServerMessage::Result {
                            request_id: request.request_id(),
                            result: CommandResult::MacroStarted {
                                execution: Box::new(data.execution.clone().unwrap()),
                            },
                        })
                    }
                }
                ClientMessage::MacroCancel { execution_id, .. } => {
                    let execution = data.execution.as_mut().unwrap();
                    assert_eq!(execution.id, *execution_id);
                    execution.status = MacroStatus::Stopping;
                    Some(ServerMessage::Result {
                        request_id: request.request_id(),
                        result: CommandResult::MacroCancelled {
                            execution: Box::new(execution.clone()),
                        },
                    })
                }
                ClientMessage::MacroStatus { execution_id, .. } => {
                    data.stop_polls += usize::from(
                        data.execution.as_ref().unwrap().status == MacroStatus::Stopping,
                    );
                    let finished = data.stop_polls >= 2;
                    let execution = data.execution.as_mut().unwrap();
                    assert_eq!(execution.id, *execution_id);
                    if finished {
                        execution.status = MacroStatus::Cancelled;
                        execution.completed_at_ns = Some(1);
                    }
                    Some(ServerMessage::Result {
                        request_id: request.request_id(),
                        result: CommandResult::MacroStatus {
                            execution: Box::new(execution.clone()),
                        },
                    })
                }
                _ => panic!("unexpected test RPC: {request:?}"),
            };
            result.unwrap()
        };
        if socket
            .send(Message::Binary(encode_control(&response).unwrap().into()))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn run_and_cancel(uncertain_start: bool, timeout: bool) {
    let (tools, data, server) = fixture(uncertain_start).await;
    let run = tools
        .call("run_start", json!({"port":"COM6","label":"macro test"}))
        .await
        .unwrap();
    let request = RpcRequest {
        jsonrpc: Some("2.0".into()),
        id: Some(json!(2)),
        method: "tools/call".into(),
        params: json!({"name":"macro_run","arguments":{"run_handle":run["run_handle"],"script":"delay(10000);","description":"Cancellation test","timeout_seconds":if timeout {1} else {30}}}),
    };
    let (cancel, receive) = oneshot::channel();
    let task = tokio::spawn(async move {
        dispatch_cancellable(&tools, request, json!(2), Some(receive)).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while data.0.lock().unwrap().execution.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if !timeout {
        cancel.send(()).unwrap();
    }
    let result = tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["result"]["structuredContent"]["status"], "cancelled");
    assert_eq!(
        result["result"]["structuredContent"]["stopped_confirmed"],
        true
    );
    assert_eq!(
        result["result"]["structuredContent"]["stop_reason"],
        if timeout {
            "deadline"
        } else {
            "caller_cancelled"
        }
    );
    assert_eq!(result["result"]["isError"], true);
    let data = data.0.lock().unwrap();
    assert!(
        data.stop_polls >= 2,
        "must not finish on the Stopping acknowledgement"
    );
    assert_eq!(
        data.requests
            .iter()
            .filter(|request| matches!(request, ClientMessage::MacroStart { .. }))
            .count(),
        1,
        "must never replay Start"
    );
    assert_eq!(
        data.requests
            .iter()
            .filter(|request| matches!(request, ClientMessage::MacroCancel { .. }))
            .count(),
        1
    );
    server.abort();
}

#[tokio::test]
async fn mcp_cancellation_waits_for_macro_stop_instead_of_dropping_physical_future() {
    run_and_cancel(false, false).await;
}
#[tokio::test]
async fn mcp_macro_deadline_requests_stop_and_waits_for_terminal_confirmation() {
    run_and_cancel(false, true).await;
}
#[tokio::test]
async fn uncertain_macro_start_is_recovered_by_id_without_any_replay() {
    run_and_cancel(true, false).await;
}

#[tokio::test]
async fn macro_evidence_never_advances_past_unread_or_human_intervention_context() {
    for terminal in [MacroStatus::Succeeded, MacroStatus::InterruptedByUser] {
        let (tools, data, server) = fixture(false).await;
        let run = tools
            .call("run_start", json!({"port":"COM6","label":"cursor test"}))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            tools.call("macro_run",json!({"run_handle":run["run_handle"],"script":"delay(10000);","description":"Cursor test"})).await.unwrap()
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while data.0.lock().unwrap().execution.is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        {
            let mut data = data.0.lock().unwrap();
            data.execution.as_mut().unwrap().status = terminal;
            data.execution.as_mut().unwrap().through_seq = 30;
            data.evidence_after = 20;
            data.evidence_truncated = true;
        }
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result["cursor"]["after_seq"],
            if terminal == MacroStatus::InterruptedByUser {
                17
            } else {
                20
            }
        );
        assert_eq!(result["evidence_complete"], false);
        assert_eq!(result["evidence"]["truncated"], true);
        assert_eq!(result["unread_evidence"]["after_seq"], 20);
        assert_eq!(result["unread_evidence"]["through_seq"], 30);
        server.abort();
    }
}
