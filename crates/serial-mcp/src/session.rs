use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use rand::TryRngCore;
use serde_json::Value;
use serial_protocol::{
    Actor, ActorKind, ClientMessage, CommandCaptureCompleted, CommandCaptureMatcher,
    CommandCaptureReport, CommandResult, CommandSequenceAuditContext, ControlLease, ErrorCode,
    PROTOCOL_VERSION, RunContextState, RunInfo, SequenceWritePrecondition, ServerMessage,
    TriggerInfo, TriggerSpec, TriggerStatus, WireFrame, WritePacing, decode_wire_frame,
    encode_client_control,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type RenewalPlan = (Vec<String>, Vec<(String, ControlLease)>);
const LEASE_TTL_MS: u64 = 60_000;
const RENEW_INTERVAL: Duration = Duration::from_secs(20);
/// An owned Run is not renewed forever merely because the MCP adapter process
/// remains alive. Tool calls pin the Run while they are active; after the last
/// pin is dropped, this deadline bounds how long an abandoned LLM session can
/// continue occupying the physical port.
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_SERVICE_MARGIN: Duration = Duration::from_secs(5);
/// seriald caps one physical write at 15 seconds. The adapter must not call a
/// correctly progressing write uncertain before that legal server deadline.
const WRITE_RPC_TIMEOUT: Duration = Duration::from_secs(20);
/// Human approval normally expires at seriald's configured deadline (60s by
/// default). This process-local ceiling keeps both HTTP and stdio MCP calls
/// bounded even if a daemon reports a malformed or unexpectedly long expiry.
const RUN_START_MAX_WAIT: Duration = Duration::from_secs(120);
const RUN_START_POLL_INTERVAL: Duration = Duration::from_millis(250);
const RUN_START_EXPIRY_GRACE: Duration = Duration::from_secs(2);
const RUN_HANDLE_BYTES: usize = 16;
const RUN_HANDLE_CHARS: usize = 22;

fn validate_run_handle_shape(run_handle: &str) -> Result<()> {
    if run_handle.len() != RUN_HANDLE_CHARS
        || !run_handle
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        bail!(
            "invalid run_handle format: expected exactly {RUN_HANDLE_CHARS} base64url \
             characters returned by run_start"
        );
    }
    Ok(())
}

fn new_run_handle() -> Result<String> {
    let mut bytes = [0_u8; RUN_HANDLE_BYTES];
    rand::rngs::OsRng
        .try_fill_bytes(&mut bytes)
        .context("operating-system CSPRNG failed while creating run_handle")?;
    let handle = URL_SAFE_NO_PAD.encode(bytes);
    debug_assert_eq!(handle.len(), RUN_HANDLE_CHARS);
    Ok(handle)
}

pub(crate) fn ensure_welcome_protocol(protocol_version: u16) -> Result<()> {
    if protocol_version != PROTOCOL_VERSION {
        bail!(
            "seriald WebSocket protocol version {protocol_version} is incompatible with \
             serial-mcp protocol version {PROTOCOL_VERSION}; install seriald and serial-mcp \
             from the same release"
        );
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExpectedDaemonIdentity {
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
}

fn ensure_welcome_identity(
    expected: Option<ExpectedDaemonIdentity>,
    server_id: Uuid,
    daemon_epoch: Uuid,
) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if expected.server_id != server_id || expected.daemon_epoch != daemon_epoch {
        bail!(
            "seriald identity changed while serial-mcp was running: expected server {} epoch {}, \
             but the WebSocket welcomed server {} epoch {}; restart serial-mcp",
            expected.server_id,
            expected.daemon_epoch,
            server_id,
            daemon_epoch
        );
    }
    Ok(())
}

#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::Sender<SessionRequest>,
    lifecycle_tx: mpsc::UnboundedSender<RunLifecycle>,
}

/// Public Run identity plus the one opaque MCP capability issued to its caller.
#[derive(Debug)]
pub struct StartedRun {
    pub approval_id: Uuid,
    pub run: RunInfo,
    pub run_handle: String,
}

/// Authorized process-local Run state. The public tool boundary sees only
/// `run_handle`; this resolved tuple is carried to the serialized physical
/// action boundary, where the private token is validated again.
pub struct AuthorizedRunUse {
    pub port: String,
    pub run_id: Uuid,
    pub(crate) run_token: Uuid,
    _guard: RunUseGuard,
}

#[derive(Clone, Debug)]
struct RunCapability {
    port: String,
    run_id: Uuid,
    run_token: Uuid,
}

/// Keeps an authorized Run alive for the complete lifetime of one tool call.
/// Dropping a cancelled or failed tool future releases the pin as well.
pub struct RunUseGuard {
    lifecycle_tx: mpsc::UnboundedSender<RunLifecycle>,
    port: String,
    run_id: Uuid,
}

impl Drop for RunUseGuard {
    fn drop(&mut self) {
        let _ = self.lifecycle_tx.send(RunLifecycle::EndUse {
            port: self.port.clone(),
            run_id: self.run_id,
        });
    }
}

enum RunLifecycle {
    EndUse { port: String, run_id: Uuid },
}

enum SessionRequest {
    UpdateRunIdleTtl {
        run_idle_ttl: Option<Duration>,
        reply: oneshot::Sender<()>,
    },
    ActorIdentity {
        reply: Reply,
    },
    BeginRunUse {
        run_handle: String,
        lifecycle_tx: mpsc::UnboundedSender<RunLifecycle>,
        reply: Reply,
    },
    Write {
        port: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        effective_pacing: WritePacing,
        description: Option<String>,
        command_capture_matchers: Vec<CommandCaptureMatcher>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
        reply: Reply,
    },
    SendBreak {
        port: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
        reply: Reply,
    },
    TriggerStart {
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
        spec: TriggerSpec,
        reply: Reply,
    },
    TriggerStatus {
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        reply: Reply,
    },
    TriggerCancel {
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        reply: Reply,
    },
    RunOwnership {
        port: String,
        run_id: Uuid,
        run_token: Uuid,
        reply: Reply,
    },
    StartRun {
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        reply: Reply,
    },
    AcknowledgeRunContext {
        port: String,
        run_id: Uuid,
        revision: u64,
        through_seq: u64,
        reply: Reply,
    },
    RecordCommandCapture {
        port: String,
        report: CommandCaptureReport,
        reply: Reply,
    },
    EndRun {
        port: String,
        run_id: Uuid,
        run_token: Uuid,
        reply: Reply,
    },
    AbortRun {
        port: String,
        run_id: Uuid,
        run_token: Uuid,
        reply: Reply,
    },
}

type Reply = oneshot::Sender<Result<SessionResponse>>;

struct RunStartPolicy<'a> {
    max_wait: Duration,
    cleanup_margin: Duration,
    poll_interval: Duration,
    expiry_grace: Duration,
    caller: Option<&'a Reply>,
}

// Responses cross a single oneshot and are consumed immediately. Keeping the
// protocol values inline avoids an allocation on every session RPC.
#[allow(clippy::large_enum_variant)]
enum SessionResponse {
    ActorIdentity(Option<String>),
    Write { event_seq: u64 },
    Break { event_seq: u64 },
    Trigger(TriggerInfo),
    Run(RunInfo),
    RunStarted(StartedRun),
    RunAuthorized(AuthorizedRunUse),
    RunContext(RunContextState),
    CommandCapture(CommandCaptureCompleted),
    RunAborted,
    RunOwnership { retained: bool },
}

impl SessionHandle {
    pub fn spawn(
        endpoint: String,
        actor_label: String,
        run_idle_ttl: Option<Duration>,
        expected_daemon: Option<ExpectedDaemonIdentity>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(32);
        let (lifecycle_tx, lifecycle_rx) = mpsc::unbounded_channel();
        tokio::spawn(run_session(
            SessionState::with_run_idle_ttl(endpoint, actor_label, run_idle_ttl, expected_daemon),
            rx,
            lifecycle_rx,
        ));
        Self { tx, lifecycle_tx }
    }

    pub async fn update_run_idle_ttl(&self, run_idle_ttl: Option<Duration>) -> Result<()> {
        let (reply, applied) = oneshot::channel();
        self.tx
            .send(SessionRequest::UpdateRunIdleTtl {
                run_idle_ttl,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        applied
            .await
            .context("serial session task stopped before applying Run timeout")
    }

    /// Server-issued identity for this exact WebSocket connection. Labels are
    /// intentionally not capabilities and are often shared by many adapters.
    pub async fn actor_id(&self) -> Result<Option<String>> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::ActorIdentity { reply })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::ActorIdentity(actor_id) => Ok(actor_id),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    /// Resolves and validates the opaque Run capability before a caller waits on the
    /// per-Slot write lock, then pins the Run until the returned guard drops.
    /// Every physical action validates the same capability again inside the
    /// serialized Session actor, closing validation/action races.
    pub async fn authorize_run_use(&self, run_handle: String) -> Result<AuthorizedRunUse> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::BeginRunUse {
                run_handle,
                lifecycle_tx: self.lifecycle_tx.clone(),
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunAuthorized(authorized) => Ok(authorized),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    // Mirrors the complete serial write boundary; grouping these values would
    // obscure which capability and audit fields cross the Session actor.
    #[allow(clippy::too_many_arguments)]
    pub async fn write(
        &self,
        port: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        effective_pacing: WritePacing,
        description: Option<String>,
        command_capture_matchers: Vec<CommandCaptureMatcher>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
    ) -> Result<WriteResult> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Write {
                port,
                data,
                operation_id,
                expected_run_id,
                run_token,
                effective_pacing,
                description,
                command_capture_matchers,
                command_sequence,
                sequence_precondition,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Write { event_seq } => Ok(WriteResult { event_seq }),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn start_run_with_handle(
        &self,
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
    ) -> Result<StartedRun> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::StartRun {
                port,
                label,
                metadata,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunStarted(started) => Ok(started),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn acknowledge_run_context(
        &self,
        port: String,
        run_id: Uuid,
        revision: u64,
        through_seq: u64,
    ) -> Result<RunContextState> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::AcknowledgeRunContext {
                port,
                run_id,
                revision,
                through_seq,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunContext(context) => Ok(context),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn record_command_capture(
        &self,
        port: String,
        report: CommandCaptureReport,
    ) -> Result<CommandCaptureCompleted> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::RecordCommandCapture {
                port,
                report,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::CommandCapture(capture) => Ok(capture),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn send_break(
        &self,
        port: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
    ) -> Result<WriteResult> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::SendBreak {
                port,
                duration_ms,
                operation_id,
                expected_run_id,
                run_token,
                sequence_precondition,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Break { event_seq } => Ok(WriteResult { event_seq }),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn trigger_start(
        &self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
        spec: TriggerSpec,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerStart {
                port,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                run_token,
                sequence_precondition,
                spec,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Trigger(trigger) => Ok(trigger),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn trigger_status(
        &self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerStatus {
                port,
                daemon_epoch,
                generation,
                trigger_id,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Trigger(trigger) => Ok(trigger),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn trigger_cancel(
        &self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerCancel {
                port,
                daemon_epoch,
                generation,
                trigger_id,
                expected_run_id,
                run_token,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Trigger(trigger) => Ok(trigger),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn run_ownership_retained(
        &self,
        port: String,
        run_id: Uuid,
        run_token: Uuid,
    ) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::RunOwnership {
                port,
                run_id,
                run_token,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunOwnership { retained } => Ok(retained),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn end_run(&self, port: String, run_id: Uuid, run_token: Uuid) -> Result<RunInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::EndRun {
                port,
                run_id,
                run_token,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Run(run) => Ok(run),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn abort_run(&self, port: String, run_id: Uuid, run_token: Uuid) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::AbortRun {
                port,
                run_id,
                run_token,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunAborted => Ok(()),
            _ => bail!("serial session returned the wrong response type"),
        }
    }
}

pub struct WriteResult {
    pub event_seq: u64,
}

async fn receive(response: oneshot::Receiver<Result<SessionResponse>>) -> Result<SessionResponse> {
    response
        .await
        .context("serial session task dropped its response")?
}

async fn run_session(
    mut state: SessionState,
    mut rx: mpsc::Receiver<SessionRequest>,
    mut lifecycle_rx: mpsc::UnboundedReceiver<RunLifecycle>,
) {
    let mut renew = tokio::time::interval(RENEW_INTERVAL);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            // A completed long request can leave both branches ready. Always
            // renew first so queued work for other Slots cannot consume the
            // remaining lease lifetime.
            biased;
            _ = renew.tick() => state.renew_all().await,
            Some(RunLifecycle::EndUse { port, run_id }) = lifecycle_rx.recv() => {
                state.end_run_use(&port, run_id);
            }
            request = rx.recv() => {
                let Some(request) = request else { break; };
                state.handle(request).await;
            }
        }
    }
}

struct OwnedRun {
    id: Uuid,
    token: Uuid,
    handle: String,
    active_uses: u32,
    idle_since: Instant,
}

impl OwnedRun {
    fn new_with_handle(id: Uuid, token: Uuid, handle: String, now: Instant) -> Self {
        Self {
            id,
            token,
            handle,
            active_uses: 0,
            idle_since: now,
        }
    }

    fn idle_expired(&self, now: Instant, run_idle_ttl: Option<Duration>) -> bool {
        self.active_uses == 0
            && run_idle_ttl.is_some_and(|ttl| now.saturating_duration_since(self.idle_since) >= ttl)
    }
}

struct SessionState {
    endpoint: String,
    actor_label: String,
    expected_daemon: Option<ExpectedDaemonIdentity>,
    socket: Option<Socket>,
    actor: Option<Actor>,
    leases: HashMap<String, ControlLease>,
    owned_runs: HashMap<String, OwnedRun>,
    run_idle_ttl: Option<Duration>,
}

impl SessionState {
    fn with_run_idle_ttl(
        endpoint: String,
        actor_label: String,
        run_idle_ttl: Option<Duration>,
        expected_daemon: Option<ExpectedDaemonIdentity>,
    ) -> Self {
        Self {
            endpoint,
            actor_label,
            expected_daemon,
            socket: None,
            actor: None,
            leases: HashMap::new(),
            owned_runs: HashMap::new(),
            run_idle_ttl,
        }
    }

    async fn handle(&mut self, request: SessionRequest) {
        match request {
            SessionRequest::UpdateRunIdleTtl {
                run_idle_ttl,
                reply,
            } => {
                self.run_idle_ttl = run_idle_ttl;
                self.renew_all().await;
                let _ = reply.send(());
            }
            SessionRequest::ActorIdentity { reply } => {
                send_reply(
                    reply,
                    Ok(SessionResponse::ActorIdentity(
                        self.actor.as_ref().map(|actor| actor.id.clone()),
                    )),
                );
            }
            SessionRequest::BeginRunUse {
                run_handle,
                lifecycle_tx,
                reply,
            } => {
                let result = self.begin_run_use(&run_handle).await.map(|capability| {
                    SessionResponse::RunAuthorized(AuthorizedRunUse {
                        _guard: RunUseGuard {
                            lifecycle_tx,
                            port: capability.port.clone(),
                            run_id: capability.run_id,
                        },
                        port: capability.port,
                        run_id: capability.run_id,
                        run_token: capability.run_token,
                    })
                });
                send_reply(reply, result);
            }
            SessionRequest::Write {
                port,
                data,
                operation_id,
                expected_run_id,
                run_token,
                effective_pacing,
                description,
                command_capture_matchers,
                command_sequence,
                sequence_precondition,
                reply,
            } => {
                let result = self
                    .write(
                        port,
                        data,
                        operation_id,
                        expected_run_id,
                        run_token,
                        effective_pacing,
                        description,
                        command_capture_matchers,
                        command_sequence,
                        sequence_precondition,
                    )
                    .await
                    .map(|event_seq| SessionResponse::Write { event_seq });
                send_reply(reply, result);
            }
            SessionRequest::StartRun {
                port,
                label,
                metadata,
                reply,
            } => {
                let result = self
                    .start_run_observed(port, label, metadata, &reply)
                    .await
                    .map(SessionResponse::RunStarted);
                send_reply(reply, result);
            }
            SessionRequest::AcknowledgeRunContext {
                port,
                run_id,
                revision,
                through_seq,
                reply,
            } => {
                let result = self
                    .acknowledge_run_context(port, run_id, revision, through_seq)
                    .await
                    .map(SessionResponse::RunContext);
                send_reply(reply, result);
            }
            SessionRequest::RecordCommandCapture {
                port,
                report,
                reply,
            } => {
                let result = self
                    .record_command_capture(port, report)
                    .await
                    .map(SessionResponse::CommandCapture);
                send_reply(reply, result);
            }
            SessionRequest::SendBreak {
                port,
                duration_ms,
                operation_id,
                expected_run_id,
                run_token,
                sequence_precondition,
                reply,
            } => {
                let result = self
                    .send_break(
                        port,
                        duration_ms,
                        operation_id,
                        expected_run_id,
                        run_token,
                        sequence_precondition,
                    )
                    .await
                    .map(|event_seq| SessionResponse::Break { event_seq });
                send_reply(reply, result);
            }
            SessionRequest::TriggerStart {
                port,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                run_token,
                sequence_precondition,
                spec,
                reply,
            } => {
                let result = self
                    .trigger_start(
                        port,
                        daemon_epoch,
                        generation,
                        operation_id,
                        expected_run_id,
                        run_token,
                        sequence_precondition,
                        spec,
                    )
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::TriggerStatus {
                port,
                daemon_epoch,
                generation,
                trigger_id,
                reply,
            } => {
                let result = self
                    .trigger_status(port, daemon_epoch, generation, trigger_id)
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::TriggerCancel {
                port,
                daemon_epoch,
                generation,
                trigger_id,
                expected_run_id,
                run_token,
                reply,
            } => {
                let result = self
                    .trigger_cancel(
                        port,
                        daemon_epoch,
                        generation,
                        trigger_id,
                        expected_run_id,
                        run_token,
                    )
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::RunOwnership {
                port,
                run_id,
                run_token,
                reply,
            } => {
                let retained = self.socket.is_some()
                    && self.leases.contains_key(&port)
                    && self
                        .owned_runs
                        .get(&port)
                        .is_some_and(|owned| owned.id == run_id && owned.token == run_token);
                send_reply(reply, Ok(SessionResponse::RunOwnership { retained }));
            }
            SessionRequest::EndRun {
                port,
                run_id,
                run_token,
                reply,
            } => {
                let result = self
                    .end_run(port, run_id, run_token)
                    .await
                    .map(SessionResponse::Run);
                send_reply(reply, result);
            }
            SessionRequest::AbortRun {
                port,
                run_id,
                run_token,
                reply,
            } => {
                let result = self
                    .abort_run(port, run_id, run_token)
                    .await
                    .map(|()| SessionResponse::RunAborted);
                send_reply(reply, result);
            }
        }
    }

    async fn begin_run_use(&mut self, run_handle: &str) -> Result<RunCapability> {
        validate_run_handle_shape(run_handle)?;
        let now = Instant::now();
        let capability = self.resolve_run_handle(run_handle)?;
        let expired = self
            .owned_runs
            .get(&capability.port)
            .is_some_and(|owned| owned.idle_expired(now, self.run_idle_ttl));
        if expired {
            // Do not let a late caller resurrect an abandoned Run between the
            // exact idle deadline and the next periodic renewal tick.
            self.best_effort_release(&capability.port).await;
            bail!(
                "run_handle expired: Run {} on port {:?} exceeded the {}-second orphan timeout \
                 and was released; call run_start for a new handle",
                capability.run_id,
                capability.port,
                self.run_idle_ttl
                    .expect("an expired Run has a finite timeout")
                    .as_secs()
            );
        }
        let owned = self
            .owned_runs
            .get_mut(&capability.port)
            .expect("validated owned Run remains present");
        owned.active_uses = owned
            .active_uses
            .checked_add(1)
            .context("too many concurrent tool calls pin this Run")?;
        Ok(capability)
    }

    fn resolve_run_handle(&self, run_handle: &str) -> Result<RunCapability> {
        self.owned_runs
            .iter()
            .find(|(_, owned)| owned.handle == run_handle)
            .map(|(port, owned)| RunCapability {
                port: port.clone(),
                run_id: owned.id,
                run_token: owned.token,
            })
            .with_context(|| {
                "unknown run_handle: it expired, belongs to another serial-mcp process, or was \
                 never issued here; call run_start and use its exact run_handle"
            })
    }

    fn end_run_use(&mut self, port: &str, run_id: Uuid) {
        let Some(owned) = self.owned_runs.get_mut(port) else {
            return;
        };
        // A delayed guard from a terminal old Run must not mutate the idle
        // state of a newly-started Run on the same port.
        if owned.id != run_id || owned.active_uses == 0 {
            return;
        }
        owned.active_uses -= 1;
        if owned.active_uses == 0 {
            owned.idle_since = Instant::now();
        }
    }

    fn validate_run_capability(
        &self,
        port: &str,
        run_id: Uuid,
        run_token: Uuid,
    ) -> Result<&OwnedRun> {
        let Some(owned) = self.owned_runs.get(port) else {
            bail!(
                "serial-mcp does not own an active Run on port {port:?}; call run_start and \
                 use the returned run_handle; no bytes were written"
            );
        };
        if owned.id != run_id || owned.token != run_token {
            bail!(
                "internal Run capability mismatch for port {port:?}; the run_handle is no \
                 longer valid, so call run_start; no bytes were written"
            );
        }
        Ok(owned)
    }

    async fn connect(&mut self) -> Result<()> {
        if self.socket.is_some() {
            return Ok(());
        }
        self.leases.clear();
        self.owned_runs.clear();
        self.actor = None;
        let request = ws_url(&self.endpoint)?.into_client_request()?;
        let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
            .await
            .context("timed out connecting to seriald WebSocket")??;
        let hello = ClientMessage::Hello {
            request_id: Uuid::new_v4(),
            protocol_version: PROTOCOL_VERSION,
            client_name: self.actor_label.clone(),
            actor_kind: ActorKind::Agent,
        };
        send_control(&mut socket, &hello).await?;
        loop {
            match next_frame(&mut socket).await? {
                WireFrame::Control(ServerMessage::Welcome {
                    protocol_version,
                    server_id,
                    daemon_epoch,
                    actor,
                    ..
                }) => {
                    ensure_welcome_protocol(protocol_version)?;
                    ensure_welcome_identity(self.expected_daemon, server_id, daemon_epoch)?;
                    self.actor = Some(actor);
                    self.socket = Some(socket);
                    return Ok(());
                }
                WireFrame::Control(ServerMessage::Error { message, .. }) => {
                    bail!("seriald rejected hello: {message}")
                }
                _ => {}
            }
        }
    }

    async fn renew_owned_run_control(
        &mut self,
        port: &str,
        expected_run_id: Uuid,
        run_token: Uuid,
    ) -> Result<ControlLease> {
        self.validate_run_capability(port, expected_run_id, run_token)?;
        if self.socket.is_none() {
            self.disconnect();
            bail!(
                "the serial connection was lost and Run {expected_run_id} can no longer be \
                 trusted; start a new Run before writing"
            );
        }
        let Some(lease) = self.leases.get(port).cloned() else {
            self.disconnect();
            bail!(
                "serial-mcp lost the control lease for Run {expected_run_id}; start a new Run \
                 before writing"
            );
        };
        let request = ClientMessage::RenewControl {
            request_id: Uuid::new_v4(),
            port: port.to_string(),
            control_id: lease.id,
            fence: lease.fence,
            ttl_ms: LEASE_TTL_MS,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlRenewed { lease }) => {
                self.leases.insert(port.to_string(), lease.clone());
                Ok(lease)
            }
            Ok(other) => {
                self.disconnect();
                bail!(
                    "unexpected control renewal result for Run {expected_run_id}: {other:?}; \
                     start a new Run before writing"
                )
            }
            Err(error) if is_control_loss_rejection(&error) => {
                self.disconnect();
                bail!(
                    "human_takeover_or_control_revoked: serial control for Run \
                     {expected_run_id} was revoked before renewal completed; \
                     taken_over_by=unknown; run_id={expected_run_id}; no_bytes_written=true; \
                     start a new Run only after the current owner releases control and the DUT \
                     model/state is reconfirmed: {error}"
                )
            }
            Err(error) => {
                self.disconnect();
                bail!(
                    "control renewal failed for Run {expected_run_id}; seriald may have aborted \
                     this Run, so a new Run is required before writing: {error}"
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn trigger_start(
        &mut self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
        spec: TriggerSpec,
    ) -> Result<TriggerInfo> {
        let lease = self
            .renew_owned_run_control(&port, expected_run_id, run_token)
            .await?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::TriggerStart {
            request_id,
            port,
            control_id: lease.id,
            fence: lease.fence,
            daemon_epoch,
            generation,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            sequence_precondition: Some(sequence_precondition),
            spec,
        };
        match self.call(request).await {
            Ok(CommandResult::TriggerStarted { trigger }) => Ok(*trigger),
            Ok(other) => bail!("unexpected trigger-start result: {other:?}"),
            Err(error) if is_user_read_required(&error) => {
                Err(anyhow::Error::new(UserCommandUsed {
                    message: error.to_string(),
                }))
            }
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
            Err(error) if daemon_reports_write_outcome_uncertain(&error) => Err(
                physical_write_outcome_uncertain("Trigger start", request_id, operation_id, error),
            ),
            Err(error) if is_control_loss_rejection(&error) => {
                self.disconnect();
                bail!(
                    "human_takeover_or_control_revoked: serial control for Run \
                     {expected_run_id} was revoked before Trigger {request_id} was accepted; \
                     taken_over_by=unknown; run_id={expected_run_id}; no_bytes_written=true: \
                     {error}"
                )
            }
            Err(error) if error.downcast_ref::<DaemonRequestError>().is_some() => bail!(
                "seriald rejected Trigger start request {request_id} (operation {operation_id}) \
                 before accepting a Job: {error}"
            ),
            Err(error) => bail!(
                "Trigger start outcome is uncertain after request {request_id} (operation \
                 {operation_id}); inspect active_trigger/TX timeline before starting another \
                 Trigger: {error}"
            ),
        }
    }

    async fn send_break(
        &mut self,
        port: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
    ) -> Result<u64> {
        let lease = self
            .renew_owned_run_control(&port, expected_run_id, run_token)
            .await?;
        self.actor
            .as_ref()
            .context("serial session has no actor identity")?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::SendBreak {
            request_id,
            port,
            control_id: lease.id,
            fence: lease.fence,
            duration_ms,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            sequence_precondition: Some(sequence_precondition),
        };
        let timeout = Duration::from_millis(duration_ms).saturating_add(RPC_SERVICE_MARGIN);
        match self.call_with_timeout(request, timeout).await {
            Ok(CommandResult::BreakSent { event_seq }) => Ok(event_seq),
            Ok(other) => bail!("unexpected Break result: {other:?}"),
            Err(error) if is_user_read_required(&error) => {
                Err(anyhow::Error::new(UserCommandUsed {
                    message: error.to_string(),
                }))
            }
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
            Err(error) if daemon_reports_write_outcome_uncertain(&error) => Err(
                physical_write_outcome_uncertain("Break", request_id, operation_id, error),
            ),
            Err(error) if is_expected_run_rejection(&error) => {
                self.disconnect();
                bail!(
                    "seriald rejected Break request {request_id} (operation {operation_id}) \
                     because the expected Run boundary is no longer valid. Start a new Run \
                     before retrying: {error}"
                )
            }
            Err(error) if is_control_loss_rejection(&error) => {
                self.disconnect();
                bail!(
                    "human_takeover_or_control_revoked: serial control for Run \
                     {expected_run_id} was revoked before Break {request_id} reached the port; \
                     taken_over_by=unknown; run_id={expected_run_id}; no_bytes_written=true: \
                     {error}"
                )
            }
            Err(error) => bail!(
                "Break outcome is uncertain after request {request_id} (operation \
                 {operation_id}); inspect the TX/control timeline before retrying: {error}"
            ),
        }
    }

    async fn trigger_status(
        &mut self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    ) -> Result<TriggerInfo> {
        // Status is a read-only lookup against seriald's bounded terminal
        // Trigger cache. It deliberately does not require this connection's
        // old actor/Run/lease: takeover or disconnect can revoke those before
        // the adapter observes the authoritative control_lost/run_lost state.
        let result = match self
            .call(trigger_status_request(
                &port,
                daemon_epoch,
                generation,
                trigger_id,
            ))
            .await
        {
            Ok(result) => result,
            Err(error) if is_transport_error(&error) || is_timeout_error(&error) => self
                .call(trigger_status_request(
                    &port,
                    daemon_epoch,
                    generation,
                    trigger_id,
                ))
                .await
                .with_context(|| {
                    format!(
                        "Trigger {trigger_id} status remained unavailable after reconnect; its \
                         terminal outcome is uncertain"
                    )
                })?,
            Err(error) => return Err(error),
        };
        let trigger = match result {
            CommandResult::TriggerStatus { trigger } => *trigger,
            other => bail!("unexpected trigger-status result: {other:?}"),
        };
        self.observe_trigger_terminal(&port, trigger.status);
        Ok(trigger)
    }

    async fn trigger_cancel(
        &mut self,
        port: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
    ) -> Result<TriggerInfo> {
        let lease = self
            .renew_owned_run_control(&port, expected_run_id, run_token)
            .await?;
        let request = ClientMessage::TriggerCancel {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            control_id: lease.id,
            fence: lease.fence,
            daemon_epoch,
            generation,
            trigger_id,
        };
        let trigger = match self.call(request).await? {
            CommandResult::TriggerCancelled { trigger } => *trigger,
            other => bail!("unexpected trigger-cancel result: {other:?}"),
        };
        self.observe_trigger_terminal(&port, trigger.status);
        Ok(trigger)
    }

    fn observe_trigger_terminal(&mut self, port: &str, status: TriggerStatus) {
        if matches!(
            status,
            TriggerStatus::ControlLost
                | TriggerStatus::RunLost
                | TriggerStatus::GenerationChanged
                | TriggerStatus::PortClosed
        ) {
            self.leases.remove(port);
            self.owned_runs.remove(port);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        &mut self,
        port: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        effective_pacing: WritePacing,
        description: Option<String>,
        command_capture_matchers: Vec<CommandCaptureMatcher>,
        command_sequence: Option<CommandSequenceAuditContext>,
        sequence_precondition: Option<SequenceWritePrecondition>,
    ) -> Result<u64> {
        let lease = self
            .renew_owned_run_control(&port, expected_run_id, run_token)
            .await?;
        self.actor
            .as_ref()
            .context("serial session has no actor identity")?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::Write {
            request_id,
            port,
            control_id: lease.id,
            fence: lease.fence,
            data,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            // Agent tools never override Slot/Device pacing. The effective
            // value is used only for the local RPC deadline below.
            pacing: None,
            description,
            command_capture_matchers,
            command_sequence,
            sequence_precondition,
            // Cooperative injection is Human-only. Agent writes always use
            // the ordinary fenced owner path.
            cooperative: false,
        };
        let rpc_timeout = write_request_timeout(
            match &request {
                ClientMessage::Write { data, .. } => data.len(),
                _ => 0,
            },
            effective_pacing,
        );
        match self.call_with_timeout(request, rpc_timeout).await {
            Ok(CommandResult::WriteAccepted { event_seq }) => Ok(event_seq),
            Ok(other) => bail!("unexpected write result: {other:?}"),
            Err(error) if is_user_read_required(&error) => {
                Err(anyhow::Error::new(UserCommandUsed {
                    message: error.to_string(),
                }))
            }
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
            Err(error) if daemon_reports_write_outcome_uncertain(&error) => Err(
                physical_write_outcome_uncertain("write", request_id, operation_id, error),
            ),
            Err(error) if is_expected_run_rejection(&error) => {
                self.disconnect();
                bail!(
                    "seriald rejected write request {request_id} (operation {operation_id}) \
                     because the expected Run boundary is no longer valid; no bytes reached the \
                     serial port. Start a new Run before retrying: {error}"
                )
            }
            Err(error) if is_control_loss_rejection(&error) => {
                self.disconnect();
                bail!(
                    "human_takeover_or_control_revoked: serial control for Run \
                     {expected_run_id} was revoked before write {request_id} reached the port; \
                     taken_over_by=unknown; run_id={expected_run_id}; no_bytes_written=true; \
                     start a new Run only after the current owner releases control and the DUT \
                     model/state is reconfirmed: {error}"
                )
            }
            Err(error) if is_definite_prewrite_rejection(&error) => bail!(
                "seriald rejected write request {request_id} (operation {operation_id}) before \
                 any bytes reached the serial port; it is safe to retry after correcting the \
                 pacing or starting/restoring the expected Run and control lease: {error}"
            ),
            Err(error) => bail!(
                "write outcome is uncertain after request {request_id} (operation {operation_id}); inspect the TX timeline before retrying: {error}"
            ),
        }
    }

    #[cfg(test)]
    async fn start_run(
        &mut self,
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
    ) -> Result<StartedRun> {
        self.start_run_with_policy(
            port,
            label,
            metadata,
            RunStartPolicy {
                max_wait: RUN_START_MAX_WAIT,
                cleanup_margin: DEFAULT_RPC_TIMEOUT,
                poll_interval: RUN_START_POLL_INTERVAL,
                expiry_grace: RUN_START_EXPIRY_GRACE,
                caller: None,
            },
        )
        .await
    }

    async fn start_run_observed(
        &mut self,
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        reply: &Reply,
    ) -> Result<StartedRun> {
        self.start_run_with_policy(
            port,
            label,
            metadata,
            RunStartPolicy {
                max_wait: RUN_START_MAX_WAIT,
                cleanup_margin: DEFAULT_RPC_TIMEOUT,
                poll_interval: RUN_START_POLL_INTERVAL,
                expiry_grace: RUN_START_EXPIRY_GRACE,
                caller: Some(reply),
            },
        )
        .await
    }

    async fn start_run_with_policy(
        &mut self,
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        policy: RunStartPolicy<'_>,
    ) -> Result<StartedRun> {
        let RunStartPolicy {
            max_wait,
            cleanup_margin,
            poll_interval,
            expiry_grace,
            caller,
        } = policy;
        if let Some(run) = self.owned_runs.get(&port) {
            bail!(
                "serial-mcp already owns active Run {} on port {port:?}",
                run.id
            );
        }
        let run_handle = loop {
            let candidate = new_run_handle()?;
            if self
                .owned_runs
                .values()
                .all(|owned| owned.handle != candidate)
            {
                break candidate;
            }
        };
        let request_id = Uuid::new_v4();
        let request = ClientMessage::RequestRunStart {
            request_id,
            port: port.clone(),
            label,
            metadata,
            ttl_ms: LEASE_TTL_MS,
        };
        let hard_deadline = tokio::time::Instant::now() + max_wait;
        // Reserve one bounded RPC window for CancelRunStart. A responsive
        // pending approval is therefore cleaned up before the 120-second
        // process-local hard ceiling, not 5 seconds after it.
        let hard_approval_deadline = hard_deadline - cleanup_margin.min(max_wait);
        let mut pending_approval: Option<serial_protocol::PendingRunStartApproval> = None;
        let mut approval_deadline = hard_approval_deadline;
        let mut next_renewal = tokio::time::Instant::now() + RENEW_INTERVAL;

        loop {
            if caller.is_some_and(oneshot::Sender::is_closed) {
                if let Some(approval) = pending_approval.as_ref() {
                    self.cancel_run_start(&port, approval.id).await?;
                }
                bail!(
                    "run_start caller disconnected; the pending approval was cancelled, no Run \
                     remains owned by this MCP, and no bytes were written"
                );
            }
            if tokio::time::Instant::now() >= approval_deadline {
                let approval_id = pending_approval
                    .as_ref()
                    .map(|approval| approval.id)
                    .context(
                        "run_start exceeded its local approval deadline before seriald returned a \
                     pending approval identity; no Run was created and no bytes were written",
                    )?;
                self.cancel_run_start(&port, approval_id).await?;
                bail!(
                    "run_start approval {approval_id} timed out locally and was cancelled; no \
                     Run was created and no bytes were written"
                );
            }

            let poll_timeout = approval_deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .min(DEFAULT_RPC_TIMEOUT);
            if poll_timeout.is_zero() {
                continue;
            }
            let result = self.call_with_timeout(request.clone(), poll_timeout).await;
            match result {
                Ok(CommandResult::RunStartGranted {
                    approval_id,
                    lease,
                    run,
                }) => {
                    if approval_id != request_id {
                        self.disconnect();
                        bail!(
                            "seriald returned a mismatched run_start grant; the connection was \
                             closed so no Run remains owned by this MCP and no bytes were written"
                        );
                    }
                    let run_token = Uuid::new_v4();
                    self.leases.insert(port.clone(), lease);
                    self.owned_runs.insert(
                        port.clone(),
                        OwnedRun::new_with_handle(
                            run.id,
                            run_token,
                            run_handle.clone(),
                            Instant::now(),
                        ),
                    );
                    if caller.is_some_and(oneshot::Sender::is_closed) {
                        self.best_effort_release(&port).await;
                        bail!(
                            "run_start caller disconnected as approval was granted; the new Run \
                             was immediately released, no Run remains owned by this MCP, and no \
                             bytes were written"
                        );
                    }
                    return Ok(StartedRun {
                        approval_id,
                        run,
                        run_handle,
                    });
                }
                Ok(CommandResult::RunStartPending { approval }) => {
                    let request_content_matches = match &request {
                        ClientMessage::RequestRunStart {
                            label,
                            metadata,
                            ttl_ms,
                            ..
                        } => {
                            approval.label == *label
                                && approval.metadata == *metadata
                                && approval.control_ttl_ms == *ttl_ms
                        }
                        _ => unreachable!("run_start polls one RequestRunStart message"),
                    };
                    let requester_matches = self.actor.as_ref().is_some_and(|actor| {
                        approval.requester.id == actor.id
                            && approval.requester.kind == ActorKind::Agent
                    });
                    if approval.id != request_id
                        || approval.port != port
                        || !request_content_matches
                        || !requester_matches
                        || approval.required_approver.kind != ActorKind::Human
                    {
                        self.disconnect();
                        bail!(
                            "seriald returned a mismatched run_start approval identity or request \
                             body; the connection was closed so the pending request is cancelled; \
                             no Run was created and no bytes were written"
                        );
                    }
                    if pending_approval
                        .as_ref()
                        .is_some_and(|previous| previous != approval.as_ref())
                    {
                        self.disconnect();
                        bail!(
                            "seriald changed the run_start approval while polling; the \
                             connection was closed so no pending request remains; no Run was \
                             created and no bytes were written"
                        );
                    }
                    let expires_wall_time_ns = approval.expires_wall_time_ns;
                    pending_approval = Some(*approval);
                    approval_deadline = hard_approval_deadline.min(run_start_deadline(
                        expires_wall_time_ns,
                        max_wait,
                        expiry_grace,
                    ));
                }
                Ok(CommandResult::RunStartDenied { approval_id }) => bail!(
                    "Human denied run_start approval {approval_id}; no Run was created and no \
                     bytes were written"
                ),
                Ok(CommandResult::RunStartTimedOut { approval_id }) => bail!(
                    "run_start approval {approval_id} expired without a Human decision; no Run \
                     was created and no bytes were written"
                ),
                Ok(CommandResult::RunStartCancelled { approval_id }) => bail!(
                    "run_start approval {approval_id} was cancelled; no Run was created and no \
                     bytes were written"
                ),
                Ok(other) => {
                    self.disconnect();
                    bail!(
                        "unexpected run_start result {other:?}; the connection was closed to \
                         cancel any pending approval; no Run was accepted and no bytes were written"
                    )
                }
                Err(error) => {
                    // Disconnect is the authoritative cancellation boundary for
                    // this Agent's pending request if polling itself fails.
                    self.disconnect();
                    return Err(error).context(
                        "run_start approval polling failed; the Agent connection was closed to \
                         cancel the pending request; no Run remains owned by this MCP and no bytes \
                         were written",
                    );
                }
            }

            if tokio::time::Instant::now() >= next_renewal {
                // A Human may take up to a minute to decide. Keep unrelated
                // Runs held by this MCP session alive while this actor waits.
                self.renew_all().await;
                if self.socket.is_none() {
                    bail!(
                        "the Agent connection closed while run_start was waiting for Human \
                         approval; pending state was cleared, no Run remains owned by this MCP, \
                         and no bytes were written"
                    );
                }
                next_renewal = tokio::time::Instant::now() + RENEW_INTERVAL;
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn cancel_run_start(&mut self, port: &str, approval_id: Uuid) -> Result<()> {
        let cancel = ClientMessage::CancelRunStart {
            request_id: Uuid::new_v4(),
            port: port.to_string(),
            approval_id,
        };
        match self.call(cancel).await {
            Ok(CommandResult::RunStartCancelled {
                approval_id: cancelled,
            }) if cancelled == approval_id => Ok(()),
            Ok(other) => {
                self.disconnect();
                bail!(
                    "seriald did not confirm cancellation of run_start approval {approval_id} \
                     (returned {other:?}); the connection was closed to clear pending state"
                )
            }
            Err(error) => {
                self.disconnect();
                Err(error).with_context(|| {
                    format!(
                        "failed to cancel run_start approval {approval_id}; the connection was \
                         closed to clear pending state"
                    )
                })
            }
        }
    }

    async fn acknowledge_run_context(
        &mut self,
        port: String,
        run_id: Uuid,
        revision: u64,
        through_seq: u64,
    ) -> Result<RunContextState> {
        let request = ClientMessage::AcknowledgeRunContext {
            request_id: Uuid::new_v4(),
            port,
            run_id,
            revision,
            through_seq,
        };
        match self.call(request).await? {
            CommandResult::RunContextAcknowledged { context } => Ok(context),
            other => bail!("unexpected run-context acknowledgement result: {other:?}"),
        }
    }

    async fn record_command_capture(
        &mut self,
        port: String,
        report: CommandCaptureReport,
    ) -> Result<CommandCaptureCompleted> {
        let request = ClientMessage::RecordCommandCapture {
            request_id: Uuid::new_v4(),
            port,
            report: Box::new(report),
        };
        match self.call(request).await? {
            CommandResult::CommandCaptureRecorded { capture } => Ok(*capture),
            other => bail!("unexpected command-capture record result: {other:?}"),
        }
    }

    async fn end_run(&mut self, port: String, run_id: Uuid, run_token: Uuid) -> Result<RunInfo> {
        self.validate_run_capability(&port, run_id, run_token)?;
        let lease = self
            .renew_owned_run_control(&port, run_id, run_token)
            .await?;
        let request = ClientMessage::EndRun {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            control_id: lease.id,
            fence: lease.fence,
            run_id,
        };
        match self.call(request).await {
            Ok(CommandResult::RunEnded { run }) => {
                self.owned_runs.remove(&port);
                self.best_effort_release(&port).await;
                Ok(run)
            }
            Ok(other) => bail!("unexpected end-run result: {other:?}"),
            Err(error) => {
                self.disconnect();
                Err(error)
            }
        }
    }

    async fn abort_run(&mut self, port: String, run_id: Uuid, run_token: Uuid) -> Result<()> {
        let Some(lease) = self.leases.get(&port).cloned() else {
            self.owned_runs.remove(&port);
            bail!(
                "aborted run_end cannot send ReleaseControl because local control was already \
                 lost; local Run ownership was discarded. Inspect devices for remote \
                 convergence, then use a fresh run_start before any further write"
            );
        };
        self.validate_run_capability(&port, run_id, run_token)?;
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlReleased) => {
                // seriald emits RunAborted before ControlReleased while
                // handling this request on the Slot actor. Therefore this
                // acknowledgement is the authoritative terminal boundary for
                // both the Run and its control lease.
                self.leases.remove(&port);
                self.owned_runs.remove(&port);
                Ok(())
            }
            Ok(other) => {
                // An unexpected acknowledgement cannot justify retaining a
                // capability that may already have crossed its release
                // boundary. Stop renewal and force a fresh Run.
                self.leases.remove(&port);
                self.owned_runs.remove(&port);
                bail!("unexpected release result: {other:?}")
            }
            Err(error) => {
                // A lease can expire immediately before Release reaches
                // seriald. The daemon has already aborted that Run, so stale
                // local maps must never claim that ownership was retained.
                self.leases.remove(&port);
                self.owned_runs.remove(&port);
                Err(error).context(
                    "control release failed; local Run ownership was discarded and a fresh \
                     run_start is required",
                )
            }
        }
    }

    async fn best_effort_release(&mut self, port: &str) {
        self.owned_runs.remove(port);
        let Some(lease) = self.leases.remove(port) else {
            return;
        };
        if self.socket.is_none() {
            return;
        }
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            port: port.to_string(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlReleased) => {}
            Ok(other) => eprintln!(
                "serial-mcp: best-effort control release returned an unexpected result for port \
                 {port:?}: {other:?}; the lease will expire at its TTL"
            ),
            Err(error) => eprintln!(
                "serial-mcp: best-effort control release failed for port {port:?}: {error}; \
                 the lease will expire at its TTL"
            ),
        }
    }

    async fn renew_all(&mut self) {
        if self.owned_runs.is_empty() {
            return;
        }
        if self.socket.is_none() {
            self.disconnect();
            return;
        }
        let (idle_slots, leases) = match self.renewal_plan(Instant::now()) {
            Ok(plan) => plan,
            Err(error) => {
                eprintln!("serial-mcp: {error}; forgetting all active Runs");
                self.disconnect();
                return;
            }
        };
        for port in idle_slots {
            let idle_seconds = self
                .run_idle_ttl
                .expect("idle ports exist only with a finite timeout")
                .as_secs();
            eprintln!(
                "serial-mcp: Run on port {port:?} was idle for {idle_seconds} seconds; \
                 releasing control and aborting the abandoned Run"
            );
            self.best_effort_release(&port).await;
            if self.socket.is_none() {
                return;
            }
        }
        for (port, lease) in leases {
            let request = ClientMessage::RenewControl {
                request_id: Uuid::new_v4(),
                port: port.clone(),
                control_id: lease.id,
                fence: lease.fence,
                ttl_ms: LEASE_TTL_MS,
            };
            match self.call(request).await {
                Ok(CommandResult::ControlRenewed { lease }) => {
                    self.leases.insert(port, lease);
                }
                Ok(_) | Err(_) => {
                    eprintln!(
                        "serial-mcp: control renewal failed; the active Run may have been aborted"
                    );
                    self.disconnect();
                    return;
                }
            }
        }
    }

    fn renewal_plan(&self, now: Instant) -> Result<RenewalPlan> {
        let mut idle_slots = self
            .owned_runs
            .iter()
            .filter(|entry| entry.1.idle_expired(now, self.run_idle_ttl))
            .map(|entry| entry.0.clone())
            .collect::<Vec<_>>();
        idle_slots.sort();
        let mut targets = self
            .owned_runs
            .iter()
            .filter(|(_, run)| !run.idle_expired(now, self.run_idle_ttl))
            .map(|(port, _)| {
                self.leases
                    .get(port)
                    .cloned()
                    .map(|lease| (port.clone(), lease))
                    .with_context(|| {
                        format!(
                            "active Run on port {port:?} has no local control lease; its \
                             ownership can no longer be trusted"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        Ok((idle_slots, targets))
    }

    async fn call(&mut self, request: ClientMessage) -> Result<CommandResult> {
        let timeout = request_timeout(&request);
        self.call_with_timeout(request, timeout).await
    }

    async fn call_with_timeout(
        &mut self,
        request: ClientMessage,
        timeout: Duration,
    ) -> Result<CommandResult> {
        self.connect().await?;
        let request_id = request.request_id();
        let socket = self
            .socket
            .as_mut()
            .context("serial WebSocket is unavailable")?;
        if let Err(error) = send_control(socket, &request).await {
            self.disconnect();
            return Err(error);
        }
        let response = tokio::time::timeout(timeout, wait_result(socket, request_id)).await;
        match response {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(error)) => {
                if is_transport_error(&error) {
                    self.disconnect();
                }
                Err(error)
            }
            Err(_) => {
                self.disconnect();
                bail!(
                    "timed out after {} ms waiting for seriald request {request_id}",
                    timeout.as_millis()
                )
            }
        }
    }

    fn disconnect(&mut self) {
        self.socket = None;
        self.actor = None;
        self.leases.clear();
        self.owned_runs.clear();
    }
}

fn request_timeout(request: &ClientMessage) -> Duration {
    match request {
        ClientMessage::Write { .. } => WRITE_RPC_TIMEOUT,
        _ => DEFAULT_RPC_TIMEOUT,
    }
}

fn run_start_deadline(
    expires_wall_time_ns: i64,
    max_wait: Duration,
    grace: Duration,
) -> tokio::time::Instant {
    let now_wall_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let expires_wall_ns = u128::try_from(expires_wall_time_ns).unwrap_or_default();
    let remaining_ns = expires_wall_ns.saturating_sub(now_wall_ns);
    let remaining_ns = u64::try_from(remaining_ns).unwrap_or(u64::MAX);
    tokio::time::Instant::now() + Duration::from_nanos(remaining_ns).min(max_wait) + grace
}

fn write_request_timeout(data_len: usize, pacing: WritePacing) -> Duration {
    if data_len == 0 || pacing.chunk_delay_ms == 0 {
        return DEFAULT_RPC_TIMEOUT;
    }
    let chunk_size = usize::try_from(pacing.chunk_size.max(1)).unwrap_or(usize::MAX);
    let chunks = data_len.saturating_add(chunk_size - 1) / chunk_size;
    let delay_count = chunks.saturating_sub(1);
    let delay_ms = u64::try_from(delay_count)
        .unwrap_or(u64::MAX)
        .saturating_mul(pacing.chunk_delay_ms);
    DEFAULT_RPC_TIMEOUT
        .saturating_add(Duration::from_millis(delay_ms))
        .min(WRITE_RPC_TIMEOUT)
}

fn trigger_status_request(
    port: &str,
    daemon_epoch: Uuid,
    generation: u64,
    trigger_id: Uuid,
) -> ClientMessage {
    ClientMessage::TriggerStatus {
        request_id: Uuid::new_v4(),
        port: port.to_string(),
        daemon_epoch,
        generation,
        trigger_id,
    }
}

fn send_reply(reply: Reply, result: Result<SessionResponse>) {
    let _ = reply.send(result);
}

async fn wait_result(socket: &mut Socket, request_id: Uuid) -> Result<CommandResult> {
    loop {
        match next_frame(socket).await? {
            WireFrame::Control(ServerMessage::Result {
                request_id: response_id,
                result,
            }) if response_id == request_id => return Ok(result),
            WireFrame::Control(ServerMessage::Error {
                request_id: Some(response_id),
                code,
                message,
                retryable,
            }) if response_id == request_id => {
                return Err(daemon_error(code, retryable, message));
            }
            _ => {}
        }
    }
}

fn daemon_error(code: ErrorCode, retryable: bool, message: String) -> anyhow::Error {
    anyhow::Error::new(DaemonRequestError {
        code,
        // An accepted physical write whose terminal result could not be
        // confirmed is never safe for an automatic retry. Enforce that
        // invariant locally even if an older or faulty daemon marks it true.
        retryable: retryable && code != ErrorCode::WriteOutcomeUncertain,
        message,
    })
}

#[derive(Debug)]
struct DaemonRequestError {
    code: ErrorCode,
    retryable: bool,
    message: String,
}

impl std::fmt::Display for DaemonRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "seriald {:?} (retryable={}): {}",
            self.code, self.retryable, self.message
        )
    }
}

impl std::error::Error for DaemonRequestError {}

/// A daemon-enforced sequence boundary failed before the physical writer was
/// reached. Keeping a concrete marker lets the MCP return a stable structured
/// partial result instead of parsing daemon prose.
#[derive(Debug)]
pub(crate) struct SequenceBoundaryRejected {
    message: String,
}

impl std::fmt::Display for SequenceBoundaryRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "command sequence boundary changed before the next write; no bytes were written: {}",
            self.message
        )
    }
}

impl std::error::Error for SequenceBoundaryRejected {}

/// A Human command changed the active Agent Run's serial context. seriald
/// rejects every physical Agent action until the Human TX has been read and
/// acknowledged. The concrete marker becomes a stable MCP structured error.
#[derive(Debug)]
pub(crate) struct UserCommandUsed {
    pub(crate) message: String,
}

impl std::fmt::Display for UserCommandUsed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "a Human command changed this Run's serial context; read the live timeline through \
             that command before another physical action; no bytes were written: {}",
            self.message
        )
    }
}

impl std::error::Error for UserCommandUsed {}

/// seriald accepted a physical action far enough that bytes or another
/// physical effect may have reached the DUT, but could not confirm its
/// terminal outcome. This concrete marker survives the session/tool layers so
/// MCP clients receive a non-retryable structured result instead of a generic
/// string that an Agent might retry automatically.
#[derive(Debug)]
pub(crate) struct WriteOutcomeUncertain {
    pub(crate) message: String,
}

impl std::fmt::Display for WriteOutcomeUncertain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for WriteOutcomeUncertain {}

fn physical_write_outcome_uncertain(
    action: &str,
    request_id: Uuid,
    operation_id: Uuid,
    error: anyhow::Error,
) -> anyhow::Error {
    anyhow::Error::new(WriteOutcomeUncertain {
        message: format!(
            "seriald could not confirm the {action} outcome after request {request_id} \
             (operation {operation_id}); bytes or another physical effect may have reached the \
             device. Do not retry automatically; inspect the TX/control timeline and current \
             device state first: {error}"
        ),
    })
}

fn is_sequence_boundary_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| error.code == ErrorCode::SequenceBoundaryChanged)
}

fn is_user_read_required(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| error.code == ErrorCode::UserReadRequired)
}

fn daemon_reports_write_outcome_uncertain(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| error.code == ErrorCode::WriteOutcomeUncertain)
}

fn is_definite_prewrite_rejection(error: &anyhow::Error) -> bool {
    let Some(error) = error.downcast_ref::<DaemonRequestError>() else {
        return false;
    };
    // These authorization checks happen before seriald calls the physical
    // writer, so their retry safety does not depend on daemon prose.
    if matches!(
        error.code,
        ErrorCode::ControlRequired | ErrorCode::StaleFence | ErrorCode::SequenceBoundaryChanged
    ) {
        return true;
    }
    let explicitly_unwritten = error.message.contains("(no bytes were written)");
    explicitly_unwritten
        && ((error.code == ErrorCode::BadRequest
            && error
                .message
                .starts_with("serial write pacing requires an estimated "))
            || (error.code == ErrorCode::Conflict
                && (error.message.starts_with("control lease has only ")
                    || error
                        .message
                        .starts_with("serial write expected active Run "))))
}

fn is_control_loss_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| {
            matches!(
                error.code,
                ErrorCode::ControlRequired | ErrorCode::StaleFence
            )
        })
}

fn is_expected_run_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| {
            error.code == ErrorCode::Conflict
                && error
                    .message
                    .starts_with("serial write expected active Run ")
                && error.message.contains("(no bytes were written)")
        })
}

fn is_transport_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("WebSocket") || message.contains("connection") || message.contains("closed")
}

fn is_timeout_error(error: &anyhow::Error) -> bool {
    error.to_string().contains("timed out")
}

async fn send_control(socket: &mut Socket, message: &ClientMessage) -> Result<()> {
    let bytes = encode_client_control(message)?;
    socket.send(Message::Binary(bytes.into())).await?;
    Ok(())
}

async fn next_frame(socket: &mut Socket) -> Result<WireFrame> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Binary(bytes))) => return Ok(decode_wire_frame(&bytes)?),
            Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
            Some(Ok(Message::Close(frame))) => bail!("seriald WebSocket closed: {frame:?}"),
            Some(Ok(Message::Text(_)))
            | Some(Ok(Message::Pong(_)))
            | Some(Ok(Message::Frame(_))) => {}
            Some(Err(error)) => return Err(error.into()),
            None => bail!("seriald WebSocket connection ended"),
        }
    }
}

fn ws_url(endpoint: &str) -> Result<String> {
    let rest = endpoint
        .strip_prefix("http://")
        .context("seriald endpoint is not an http:// origin")?;
    Ok(format!("ws://{rest}/api/v1/ws"))
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestServerSocket = WebSocketStream<TcpStream>;

    fn test_actor(kind: ActorKind) -> Actor {
        Actor {
            id: format!("{kind:?}:test"),
            label: "test".into(),
            kind,
        }
    }

    fn test_lease(owner: Actor) -> ControlLease {
        ControlLease {
            id: Uuid::new_v4(),
            owner,
            epoch: Uuid::new_v4(),
            generation: 7,
            fence: 11,
            issued_wall_time_ns: 1,
            expires_wall_time_ns: i64::MAX,
        }
    }

    fn test_run(owner: Actor, label: &str) -> RunInfo {
        RunInfo {
            id: Uuid::new_v4(),
            owner,
            label: label.into(),
            status: serial_protocol::RunStatus::Active,
            start_seq: 17,
            end_seq: None,
            metadata: Default::default(),
        }
    }

    async fn run_start_test_state() -> (SessionState, TestServerSocket) {
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
            Some(Duration::from_secs(1_800)),
            None,
        );
        state.socket = Some(socket);
        state.actor = Some(test_actor(ActorKind::Agent));
        (state, server)
    }

    async fn receive_test_request(socket: &mut TestServerSocket) -> ClientMessage {
        let frame = socket.next().await.unwrap().unwrap();
        let Message::Binary(bytes) = frame else {
            panic!("expected binary client request");
        };
        serial_protocol::decode_client_control(&bytes).unwrap()
    }

    async fn send_test_result(
        socket: &mut TestServerSocket,
        request_id: Uuid,
        result: CommandResult,
    ) {
        let response =
            serial_protocol::encode_control(&ServerMessage::Result { request_id, result }).unwrap();
        socket.send(Message::Binary(response.into())).await.unwrap();
    }

    async fn send_test_error(
        socket: &mut TestServerSocket,
        request_id: Uuid,
        code: ErrorCode,
        message: &str,
        retryable: bool,
    ) {
        let response = serial_protocol::encode_control(&ServerMessage::Error {
            request_id: Some(request_id),
            code,
            message: message.into(),
            retryable,
        })
        .unwrap();
        socket.send(Message::Binary(response.into())).await.unwrap();
    }

    fn pending_approval(
        request_id: Uuid,
        port: &str,
        label: &str,
    ) -> serial_protocol::PendingRunStartApproval {
        serial_protocol::PendingRunStartApproval {
            id: request_id,
            port: port.into(),
            requester: test_actor(ActorKind::Agent),
            required_approver: test_actor(ActorKind::Human),
            label: label.into(),
            metadata: Default::default(),
            control_ttl_ms: LEASE_TTL_MS,
            daemon_epoch: Uuid::new_v4(),
            generation: 7,
            expected_control_id: Uuid::new_v4(),
            expected_fence: 9,
            requested_wall_time_ns: 1,
            expires_wall_time_ns: i64::MAX,
        }
    }

    #[tokio::test]
    async fn run_start_idle_uses_one_atomic_request_and_accepts_grant() {
        let (mut state, mut server) = run_start_test_state().await;
        let owner = test_actor(ActorKind::Agent);
        let lease = test_lease(owner.clone());
        let run = test_run(owner, "inspect boot");
        let port = "COM7".to_string();
        let (started, request) = tokio::join!(
            state.start_run(port.clone(), "inspect boot".into(), Default::default()),
            async {
                let request = receive_test_request(&mut server).await;
                let request_id = request.request_id();
                send_test_result(
                    &mut server,
                    request_id,
                    CommandResult::RunStartGranted {
                        approval_id: request_id,
                        lease: lease.clone(),
                        run: run.clone(),
                    },
                )
                .await;
                request
            }
        );
        let started = started.unwrap();
        assert_eq!(started.approval_id, request.request_id());
        assert_eq!(started.run.id, run.id);
        assert!(matches!(
            request,
            ClientMessage::RequestRunStart {
                request_id: _,
                port: request_port,
                label,
                ttl_ms: LEASE_TTL_MS,
                ..
            } if request_port == port && label == "inspect boot"
        ));
        assert_eq!(state.leases[&port], lease);
        assert_eq!(state.owned_runs[&port].id, run.id);
    }

    #[tokio::test]
    async fn daemon_uncertain_write_stays_typed_and_never_retryable() {
        let (mut state, mut server) = run_start_test_state().await;
        let port = "COM13".to_string();
        let run_id = Uuid::new_v4();
        let run_token = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let lease = test_lease(state.actor.clone().unwrap());
        state.leases.insert(port.clone(), lease.clone());
        state.owned_runs.insert(
            port.clone(),
            OwnedRun::new_with_handle(
                run_id,
                run_token,
                "abcdefghijklmnopqrstuv".into(),
                Instant::now(),
            ),
        );

        let (result, write_request) = tokio::join!(
            state.write(
                port.clone(),
                b"reboot\r".to_vec(),
                operation_id,
                run_id,
                run_token,
                WritePacing {
                    chunk_size: 4_096,
                    chunk_delay_ms: 0,
                },
                Some("reboot the DUT".into()),
                Vec::new(),
                None,
                None,
            ),
            async {
                let renew = receive_test_request(&mut server).await;
                assert!(matches!(renew, ClientMessage::RenewControl { .. }));
                send_test_result(
                    &mut server,
                    renew.request_id(),
                    CommandResult::ControlRenewed {
                        lease: lease.clone(),
                    },
                )
                .await;
                let write = receive_test_request(&mut server).await;
                send_test_error(
                    &mut server,
                    write.request_id(),
                    ErrorCode::WriteOutcomeUncertain,
                    "writer completion channel closed",
                    true,
                )
                .await;
                write
            }
        );

        assert!(matches!(
            write_request,
            ClientMessage::Write {
                operation_id: Some(request_operation_id),
                ..
            } if request_operation_id == operation_id
        ));
        let error = result.unwrap_err();
        assert!(error.downcast_ref::<WriteOutcomeUncertain>().is_some());
        let message = error.to_string();
        assert!(message.contains("Do not retry automatically"), "{message}");
        assert!(message.contains("retryable=false"), "{message}");
        assert!(!message.contains("retryable=true"), "{message}");
        assert!(message.contains(&operation_id.to_string()), "{message}");
    }

    #[tokio::test]
    async fn pending_run_start_polls_the_exact_same_request_then_accepts_grant() {
        let (mut state, mut server) = run_start_test_state().await;
        let owner = test_actor(ActorKind::Agent);
        let lease = test_lease(owner.clone());
        let run = test_run(owner, "approved boot");
        let (started, requests) = tokio::join!(
            state.start_run("COM8".into(), "approved boot".into(), Default::default()),
            async {
                let first = receive_test_request(&mut server).await;
                let request_id = first.request_id();
                send_test_result(
                    &mut server,
                    request_id,
                    CommandResult::RunStartPending {
                        approval: Box::new(pending_approval(request_id, "COM8", "approved boot")),
                    },
                )
                .await;
                let second = receive_test_request(&mut server).await;
                send_test_result(
                    &mut server,
                    second.request_id(),
                    CommandResult::RunStartGranted {
                        approval_id: second.request_id(),
                        lease: lease.clone(),
                        run: run.clone(),
                    },
                )
                .await;
                (first, second)
            }
        );
        let started = started.unwrap();
        assert_eq!(started.run.id, run.id);
        assert_eq!(started.approval_id, requests.0.request_id());
        assert_eq!(requests.0, requests.1);
        assert!(matches!(requests.0, ClientMessage::RequestRunStart { .. }));
    }

    #[tokio::test]
    async fn pending_run_start_surfaces_all_authoritative_terminal_outcomes() {
        for (terminal, expected) in [
            (0_u8, "Human denied"),
            (1_u8, "expired without a Human decision"),
            (2_u8, "was cancelled"),
        ] {
            let (mut state, mut server) = run_start_test_state().await;
            let (result, ()) = tokio::join!(
                state.start_run("COM9".into(), "terminal".into(), Default::default()),
                async {
                    let first = receive_test_request(&mut server).await;
                    let request_id = first.request_id();
                    send_test_result(
                        &mut server,
                        request_id,
                        CommandResult::RunStartPending {
                            approval: Box::new(pending_approval(request_id, "COM9", "terminal")),
                        },
                    )
                    .await;
                    let poll = receive_test_request(&mut server).await;
                    assert_eq!(first, poll);
                    let result = match terminal {
                        0 => CommandResult::RunStartDenied {
                            approval_id: request_id,
                        },
                        1 => CommandResult::RunStartTimedOut {
                            approval_id: request_id,
                        },
                        _ => CommandResult::RunStartCancelled {
                            approval_id: request_id,
                        },
                    };
                    send_test_result(&mut server, request_id, result).await;
                }
            );
            let error = result.unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
            assert!(error.contains("no Run was created"), "{error}");
            assert!(error.contains("no bytes were written"), "{error}");
            assert!(state.leases.is_empty());
            assert!(state.owned_runs.is_empty());
        }
    }

    #[tokio::test]
    async fn local_run_start_deadline_cancels_pending_without_legacy_control_rpc() {
        let (mut state, mut server) = run_start_test_state().await;
        let (result, requests) = tokio::join!(
            state.start_run_with_policy(
                "COM10".into(),
                "cancel me".into(),
                Default::default(),
                RunStartPolicy {
                    max_wait: Duration::from_millis(10),
                    cleanup_margin: Duration::ZERO,
                    poll_interval: Duration::from_millis(1),
                    expiry_grace: Duration::ZERO,
                    caller: None,
                },
            ),
            async {
                let mut polled = Vec::new();
                let mut approval = None;
                loop {
                    let request = receive_test_request(&mut server).await;
                    match &request {
                        ClientMessage::RequestRunStart {
                            request_id,
                            port,
                            label,
                            ..
                        } => {
                            polled.push(request.clone());
                            let pending = approval
                                .get_or_insert_with(|| pending_approval(*request_id, port, label));
                            send_test_result(
                                &mut server,
                                *request_id,
                                CommandResult::RunStartPending {
                                    approval: Box::new(pending.clone()),
                                },
                            )
                            .await;
                        }
                        ClientMessage::CancelRunStart {
                            request_id,
                            approval_id,
                            ..
                        } => {
                            send_test_result(
                                &mut server,
                                *request_id,
                                CommandResult::RunStartCancelled {
                                    approval_id: *approval_id,
                                },
                            )
                            .await;
                            break (polled, request);
                        }
                        other => panic!("legacy or unexpected run_start RPC: {other:?}"),
                    }
                }
            }
        );
        let error = result.unwrap_err().to_string();
        assert!(error.contains("timed out locally"), "{error}");
        assert!(error.contains("no bytes were written"), "{error}");
        assert!(!requests.0.is_empty());
        assert!(requests.0.iter().all(|request| request == &requests.0[0]));
        assert!(matches!(requests.1, ClientMessage::CancelRunStart { .. }));
        assert!(state.leases.is_empty());
        assert!(state.owned_runs.is_empty());
    }

    #[tokio::test]
    async fn dropped_mcp_caller_cancels_pending_approval() {
        let (mut state, mut server) = run_start_test_state().await;
        let (reply, response) = oneshot::channel::<Result<SessionResponse>>();
        let (result, cancel) = tokio::join!(
            state.start_run_with_policy(
                "COM12".into(),
                "disconnected caller".into(),
                Default::default(),
                RunStartPolicy {
                    max_wait: Duration::from_secs(1),
                    cleanup_margin: Duration::ZERO,
                    poll_interval: Duration::from_millis(1),
                    expiry_grace: Duration::ZERO,
                    caller: Some(&reply),
                },
            ),
            async {
                let request = receive_test_request(&mut server).await;
                let request_id = request.request_id();
                send_test_result(
                    &mut server,
                    request_id,
                    CommandResult::RunStartPending {
                        approval: Box::new(pending_approval(
                            request_id,
                            "COM12",
                            "disconnected caller",
                        )),
                    },
                )
                .await;
                drop(response);
                let cancel = receive_test_request(&mut server).await;
                let ClientMessage::CancelRunStart {
                    request_id,
                    approval_id,
                    ..
                } = cancel
                else {
                    panic!("dropped caller must cancel the pending run_start")
                };
                send_test_result(
                    &mut server,
                    request_id,
                    CommandResult::RunStartCancelled { approval_id },
                )
                .await;
                approval_id
            }
        );
        let error = result.unwrap_err().to_string();
        assert!(error.contains("caller disconnected"), "{error}");
        assert!(error.contains("no Run remains owned"), "{error}");
        assert_ne!(cancel, Uuid::nil());
        assert!(state.leases.is_empty());
        assert!(state.owned_runs.is_empty());
    }

    #[tokio::test]
    async fn run_context_ack_and_command_capture_use_v7_rpcs() {
        let (mut state, mut server) = run_start_test_state().await;
        let port = "COM11".to_string();
        let run_id = Uuid::new_v4();
        let context = RunContextState {
            run_id,
            revision: 3,
            last_human_command_seq: Some(44),
            acknowledged_revision: 3,
            acknowledged_through_seq: Some(47),
        };
        let (acknowledged, request) = tokio::join!(
            state.acknowledge_run_context(port.clone(), run_id, 3, 47),
            async {
                let request = receive_test_request(&mut server).await;
                send_test_result(
                    &mut server,
                    request.request_id(),
                    CommandResult::RunContextAcknowledged {
                        context: context.clone(),
                    },
                )
                .await;
                request
            }
        );
        assert_eq!(acknowledged.unwrap(), context);
        assert!(matches!(
            request,
            ClientMessage::AcknowledgeRunContext {
                port: request_port,
                run_id: request_run_id,
                revision: 3,
                through_seq: 47,
                ..
            } if request_port == port && request_run_id == run_id
        ));

        let report = CommandCaptureReport {
            daemon_epoch: Uuid::new_v4(),
            generation: 7,
            run_id,
            operation_id: Uuid::new_v4(),
            tx_event_seq: 50,
            evidence_from_seq: 50,
            evidence_through_seq: 55,
            completion: serial_protocol::CommandCaptureCompletionKind::Prompt,
            completion_detail: Some("root# ".into()),
            confidence: serial_protocol::CommandCaptureConfidence::High,
        };
        let completed = CommandCaptureCompleted {
            daemon_epoch: report.daemon_epoch,
            generation: report.generation,
            run_id,
            operation_id: report.operation_id,
            tx_event_seq: report.tx_event_seq,
            evidence_from_seq: report.evidence_from_seq,
            evidence_through_seq: report.evidence_through_seq,
            completion: report.completion,
            completion_detail: report.completion_detail.clone(),
            confidence: report.confidence,
            record_event_seq: 56,
            tx_stream_offset_start: Some(10),
            tx_stream_offset_end: Some(15),
            rx_stream_offset_start: Some(20),
            rx_stream_offset_end: Some(30),
        };
        let (recorded, request) = tokio::join!(
            state.record_command_capture(port.clone(), report.clone()),
            async {
                let request = receive_test_request(&mut server).await;
                send_test_result(
                    &mut server,
                    request.request_id(),
                    CommandResult::CommandCaptureRecorded {
                        capture: Box::new(completed.clone()),
                    },
                )
                .await;
                request
            }
        );
        assert_eq!(recorded.unwrap(), completed);
        assert!(matches!(
            request,
            ClientMessage::RecordCommandCapture {
                port: request_port,
                report: request_report,
                ..
            } if request_port == port && *request_report == report
        ));
    }

    async fn abort_test_state(
        response: CommandResult,
    ) -> (
        SessionState,
        tokio::task::JoinHandle<ClientMessage>,
        String,
        Uuid,
        Uuid,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let frame = socket.next().await.unwrap().unwrap();
            let Message::Binary(bytes) = frame else {
                panic!("expected binary ReleaseControl frame");
            };
            let request = serial_protocol::decode_client_control(&bytes).unwrap();
            let request_id = request.request_id();
            let response = serial_protocol::encode_control(&ServerMessage::Result {
                request_id,
                result: response,
            })
            .unwrap();
            socket.send(Message::Binary(response.into())).await.unwrap();
            request
        });
        let (socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
        let mut state = SessionState::with_run_idle_ttl(
            format!("http://{address}"),
            "agent".into(),
            Some(Duration::from_secs(1_800)),
            None,
        );
        state.socket = Some(socket);
        let port = "COM4".to_owned();
        let run_id = Uuid::new_v4();
        let run_token = Uuid::new_v4();
        let actor = Actor {
            id: "agent:test".into(),
            label: "test".into(),
            kind: ActorKind::Agent,
        };
        state.leases.insert(
            port.clone(),
            ControlLease {
                id: Uuid::new_v4(),
                owner: actor,
                epoch: Uuid::new_v4(),
                generation: 7,
                fence: 11,
                issued_wall_time_ns: 1,
                expires_wall_time_ns: i64::MAX,
            },
        );
        state.owned_runs.insert(
            port.clone(),
            OwnedRun::new_with_handle(
                run_id,
                run_token,
                "abcdefghijklmnopqrstuv".into(),
                Instant::now(),
            ),
        );
        (state, server, port, run_id, run_token)
    }

    #[tokio::test]
    async fn run_idle_timeout_update_is_applied_inside_the_session_actor() {
        let mut state = SessionState::with_run_idle_ttl(
            "http://127.0.0.1:3210".into(),
            "agent".into(),
            Some(Duration::from_secs(1_800)),
            None,
        );
        let (reply, applied) = oneshot::channel();
        state
            .handle(SessionRequest::UpdateRunIdleTtl {
                run_idle_ttl: None,
                reply,
            })
            .await;
        applied.await.unwrap();
        assert_eq!(state.run_idle_ttl, None);

        let (reply, applied) = oneshot::channel();
        state
            .handle(SessionRequest::UpdateRunIdleTtl {
                run_idle_ttl: Some(Duration::from_secs(3_600)),
                reply,
            })
            .await;
        applied.await.unwrap();
        assert_eq!(state.run_idle_ttl, Some(Duration::from_secs(3_600)));
    }

    #[tokio::test]
    async fn aborted_run_sends_capability_guarded_release_and_requires_control_released() {
        let (mut state, server, port, run_id, run_token) =
            abort_test_state(CommandResult::ControlReleased).await;
        let lease = state.leases[&port].clone();
        state
            .abort_run(port.clone(), run_id, run_token)
            .await
            .unwrap();
        let request = server.await.unwrap();
        assert!(matches!(
            request,
            ClientMessage::ReleaseControl {
                port: request_port,
                control_id,
                fence,
                ..
            } if request_port == port && control_id == lease.id && fence == lease.fence
        ));
        assert!(!state.leases.contains_key(&port));
        assert!(!state.owned_runs.contains_key(&port));

        let (mut state, server, port, run_id, run_token) =
            abort_test_state(CommandResult::AcquireCancelled { removed: false }).await;
        let error = state
            .abort_run(port.clone(), run_id, run_token)
            .await
            .unwrap_err()
            .to_string();
        server.await.unwrap();
        assert!(error.contains("unexpected release result"));
        assert!(!state.leases.contains_key(&port));
        assert!(!state.owned_runs.contains_key(&port));
    }

    #[tokio::test]
    async fn aborted_run_without_a_local_lease_fails_and_discards_ownership() {
        let mut state = SessionState::with_run_idle_ttl(
            "http://127.0.0.1:3210".into(),
            "agent".into(),
            Some(Duration::from_secs(1_800)),
            None,
        );
        let port = "COM4".to_owned();
        let run_id = Uuid::new_v4();
        let run_token = Uuid::new_v4();
        state.owned_runs.insert(
            port.clone(),
            OwnedRun::new_with_handle(
                run_id,
                run_token,
                "abcdefghijklmnopqrstuv".into(),
                Instant::now(),
            ),
        );
        let error = state
            .abort_run(port.clone(), run_id, run_token)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("local control was already lost"));
        assert!(error.contains("fresh run_start"));
        assert!(!state.owned_runs.contains_key(&port));
    }

    #[test]
    fn http_session_rejects_a_different_daemon_identity() {
        let expected = ExpectedDaemonIdentity {
            server_id: Uuid::new_v4(),
            daemon_epoch: Uuid::new_v4(),
        };
        assert!(
            ensure_welcome_identity(Some(expected), expected.server_id, expected.daemon_epoch)
                .is_ok()
        );
        assert!(
            ensure_welcome_identity(Some(expected), Uuid::new_v4(), expected.daemon_epoch)
                .unwrap_err()
                .to_string()
                .contains("restart serial-mcp")
        );
        assert!(
            ensure_welcome_identity(Some(expected), expected.server_id, Uuid::new_v4())
                .unwrap_err()
                .to_string()
                .contains("restart serial-mcp")
        );
    }

    #[test]
    fn stdio_session_accepts_any_daemon_identity() {
        assert!(ensure_welcome_identity(None, Uuid::new_v4(), Uuid::new_v4()).is_ok());
    }
}
