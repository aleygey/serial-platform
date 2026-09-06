use super::*;
use serial_protocol::{MacroExecutionInfo, MacroRunSpec, MacroStatus};

async fn fixture() -> (
    SessionState,
    WebSocketStream<TcpStream>,
    ControlLease,
    Uuid,
    Uuid,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_async(stream).await.unwrap()
    });
    let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
    let server = accept.await.unwrap();
    let mut state = SessionState::with_run_idle_ttl(
        format!("http://{address}"),
        "agent".into(),
        Some(Duration::from_secs(1800)),
        None,
    );
    let actor = Actor {
        id: "agent:macro-test".into(),
        label: "macro test".into(),
        kind: ActorKind::Agent,
    };
    let lease = ControlLease {
        id: Uuid::new_v4(),
        owner: actor.clone(),
        epoch: Uuid::new_v4(),
        generation: 2,
        fence: 3,
        issued_wall_time_ns: 1,
        expires_wall_time_ns: i64::MAX,
    };
    let run = Uuid::new_v4();
    let token = Uuid::new_v4();
    state.socket = Some(socket);
    state.actor = Some(actor);
    state.leases.insert("COM6".into(), lease.clone());
    state.owned_runs.insert(
        "COM6".into(),
        OwnedRun::new_with_handle(run, token, "abcdefghijklmnopqrstuv".into(), Instant::now()),
    );
    (state, server, lease, run, token)
}

fn execution(
    lease: &ControlLease,
    run: Uuid,
    operation: Uuid,
    status: MacroStatus,
) -> MacroExecutionInfo {
    MacroExecutionInfo {
        id: operation,
        port: "COM6".into(),
        daemon_epoch: lease.epoch,
        generation: lease.generation,
        owner: lease.owner.clone(),
        run_id: Some(run),
        macro_id: Some("boot".into()),
        revision: Some(2),
        description: "Enter boot".into(),
        status,
        started_at_ns: 1,
        completed_at_ns: None,
        line: 1,
        column: 1,
        writes: 0,
        bytes_written: 0,
        first_seq: 10,
        through_seq: 10,
        message: None,
        outcome_uncertain: false,
    }
}

async fn receive(socket: &mut WebSocketStream<TcpStream>) -> ClientMessage {
    let frame = socket.next().await.unwrap().unwrap();
    let Message::Binary(bytes) = frame else {
        panic!("expected binary request")
    };
    serial_protocol::decode_client_control(&bytes).unwrap()
}

async fn reply(socket: &mut WebSocketStream<TcpStream>, id: Uuid, result: CommandResult) {
    let bytes = serial_protocol::encode_control(&ServerMessage::Result {
        request_id: id,
        result,
    })
    .unwrap();
    socket.send(Message::Binary(bytes.into())).await.unwrap();
}

fn start(lease: &ControlLease, run: Uuid, token: Uuid, operation: Uuid) -> MacroAction {
    MacroAction::Start {
        daemon_epoch: lease.epoch,
        generation: lease.generation,
        operation_id: operation,
        expected_run_id: run,
        run_token: token,
        sequence_precondition: SequenceWritePrecondition {
            cursor: serial_protocol::Cursor {
                epoch: lease.epoch,
                after_seq: 10,
            },
            expected_generation: lease.generation,
            expected_tx_offset: 0,
        },
        spec: MacroRunSpec {
            macro_id: Some("boot".into()),
            revision: Some(2),
            script: None,
            description: None,
            args: BTreeMap::new(),
            timeout_seconds: 30,
        },
    }
}

use std::collections::BTreeMap;

#[tokio::test]
async fn macro_start_uses_exact_owned_capability_and_stable_operation_without_raw_override() {
    let (mut state, mut socket, lease, run, token) = fixture().await;
    let operation = Uuid::new_v4();
    let expected = execution(&lease, run, operation, MacroStatus::Running);
    let actor_result = expected.clone();
    let server_lease = lease.clone();
    let server = tokio::spawn(async move {
        let renewal = receive(&mut socket).await;
        assert!(
            matches!(renewal,ClientMessage::RenewControl { control_id,fence,.. } if control_id == server_lease.id && fence == server_lease.fence)
        );
        reply(
            &mut socket,
            renewal.request_id(),
            CommandResult::ControlRenewed {
                lease: server_lease.clone(),
            },
        )
        .await;
        let request = receive(&mut socket).await;
        match &request {
            ClientMessage::MacroStart {
                operation_id,
                control_id,
                fence,
                daemon_epoch,
                generation,
                expected_run_id,
                sequence_precondition,
                spec,
                ..
            } => {
                assert_eq!(*operation_id, operation);
                assert_eq!(*control_id, server_lease.id);
                assert_eq!(*fence, server_lease.fence);
                assert_eq!(*daemon_epoch, server_lease.epoch);
                assert_eq!(*generation, 2);
                assert_eq!(*expected_run_id, Some(run));
                assert!(sequence_precondition.is_some());
                assert_eq!(spec.revision, Some(2));
                assert_eq!(spec.timeout_seconds, 30);
            }
            _ => panic!("wrong request"),
        }
        reply(
            &mut socket,
            request.request_id(),
            CommandResult::MacroStarted {
                execution: Box::new(actor_result),
            },
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), socket.next())
                .await
                .is_err(),
            "Start must not be replayed"
        );
    });
    let result = state
        .macro_request("COM6".into(), start(&lease, run, token, operation))
        .await
        .unwrap();
    assert_eq!(result, expected);
    server.await.unwrap();
}

#[tokio::test]
async fn macro_wrong_run_token_is_rejected_before_any_rpc() {
    let (mut state, mut socket, lease, run, _) = fixture().await;
    let error = state
        .macro_request(
            "COM6".into(),
            start(&lease, run, Uuid::new_v4(), Uuid::new_v4()),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("capability mismatch"));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), socket.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn macro_cancel_ack_is_stopping_and_status_is_the_terminal_authority() {
    let (mut state, mut socket, lease, run, token) = fixture().await;
    let operation = Uuid::new_v4();
    let stopping = execution(&lease, run, operation, MacroStatus::Stopping);
    let terminal = execution(&lease, run, operation, MacroStatus::Cancelled);
    let server_terminal = terminal.clone();
    let server = tokio::spawn(async move {
        let renewal = receive(&mut socket).await;
        reply(
            &mut socket,
            renewal.request_id(),
            CommandResult::ControlRenewed { lease },
        )
        .await;
        let cancel = receive(&mut socket).await;
        assert!(
            matches!(cancel,ClientMessage::MacroCancel { execution_id,.. } if execution_id == operation)
        );
        reply(
            &mut socket,
            cancel.request_id(),
            CommandResult::MacroCancelled {
                execution: Box::new(stopping),
            },
        )
        .await;
        let status = receive(&mut socket).await;
        assert!(
            matches!(status,ClientMessage::MacroStatus { execution_id,.. } if execution_id == operation)
        );
        reply(
            &mut socket,
            status.request_id(),
            CommandResult::MacroStatus {
                execution: Box::new(server_terminal),
            },
        )
        .await;
    });
    let result = state
        .macro_request(
            "COM6".into(),
            MacroAction::Cancel {
                execution_id: operation,
                expected_run_id: run,
                run_token: token,
            },
        )
        .await
        .unwrap();
    assert!(!result.status.is_terminal());
    let result = state
        .macro_request(
            "COM6".into(),
            MacroAction::Status {
                execution_id: operation,
            },
        )
        .await
        .unwrap();
    assert_eq!(result, terminal);
    server.await.unwrap();
}
