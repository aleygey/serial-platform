use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use rand::TryRngCore;
use serde_json::Value;
use serial_protocol::{
    Actor, ActorKind, ClientMessage, CommandCaptureMatcher, CommandResult,
    CommandSequenceAuditContext, ControlLease, ControlMode, ErrorCode, PROTOCOL_VERSION, RunInfo,
    SequenceWritePrecondition, ServerMessage, TriggerInfo, TriggerSpec, TriggerStatus, WireFrame,
    WritePacing, decode_wire_frame, encode_client_control,
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
// The session task serializes requests and lease renewal on one socket.
// Keeping queue waits at 15 seconds prevents one blocked Slot from starving
// the 20-second renewal cadence of leases held for other Slots.
const MAX_CONTROL_WAIT: Duration = Duration::from_secs(15);
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
pub struct StartedRun {
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

/// Process-local ownership known by the serialized MCP control session.
///
/// Public daemon snapshots cannot answer whether a lease belongs to this MCP
/// connection, so release planning must use this state instead of inferring
/// ownership from a visible Run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalControlState {
    pub has_lease: bool,
    pub owned_run_id: Option<Uuid>,
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
    LocalControlState {
        port: String,
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
        control_wait: Duration,
        reply: Reply,
    },
    EndRun {
        port: String,
        run_id: Uuid,
        run_token: Uuid,
        reply: Reply,
    },
    Release {
        port: String,
        abort_run: bool,
        run_capability: Option<(Uuid, Uuid)>,
        allow_stale_cleanup: bool,
        reply: Reply,
    },
}

type Reply = oneshot::Sender<Result<SessionResponse>>;

// Responses cross a single oneshot and are consumed immediately. Keeping the
// protocol values inline avoids an allocation on every session RPC.
#[allow(clippy::large_enum_variant)]
enum SessionResponse {
    ActorIdentity(Option<String>),
    LocalControlState(LocalControlState),
    Write { event_seq: u64 },
    Break { event_seq: u64 },
    Trigger(TriggerInfo),
    Run(RunInfo),
    RunStarted(StartedRun),
    RunAuthorized(AuthorizedRunUse),
    Released { had_lease: bool },
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

    pub async fn local_control_state(&self, port: String) -> Result<LocalControlState> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::LocalControlState { port, reply })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::LocalControlState(state) => Ok(state),
            _ => bail!("serial session returned the wrong response type"),
        }
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
        control_wait: Duration,
    ) -> Result<StartedRun> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::StartRun {
                port,
                label,
                metadata,
                control_wait,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunStarted(started) => Ok(started),
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

    pub async fn release(
        &self,
        port: String,
        abort_run: bool,
        run_capability: Option<(Uuid, Uuid)>,
        allow_stale_cleanup: bool,
    ) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Release {
                port,
                abort_run,
                run_capability,
                allow_stale_cleanup,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Released { had_lease } => Ok(had_lease),
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
            SessionRequest::LocalControlState { port, reply } => {
                let state = self.local_control_state(&port);
                send_reply(reply, Ok(SessionResponse::LocalControlState(state)));
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
                control_wait,
                reply,
            } => {
                let result = self
                    .start_run(port, label, metadata, control_wait)
                    .await
                    .map(SessionResponse::RunStarted);
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
            SessionRequest::Release {
                port,
                abort_run,
                run_capability,
                allow_stale_cleanup,
                reply,
            } => {
                let result = self
                    .release(port, abort_run, run_capability, allow_stale_cleanup)
                    .await
                    .map(|had_lease| SessionResponse::Released { had_lease });
                send_reply(reply, result);
            }
        }
    }

    fn local_control_state(&self, port: &str) -> LocalControlState {
        LocalControlState {
            has_lease: self.leases.contains_key(port),
            owned_run_id: self.owned_runs.get(port).map(|run| run.id),
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

    async fn acquire_control(&mut self, port: &str, wait: Duration) -> Result<ControlLease> {
        self.connect().await?;
        if let Some(lease) = self.leases.get(port).cloned() {
            let request_id = Uuid::new_v4();
            let renew = ClientMessage::RenewControl {
                request_id,
                port: port.to_string(),
                control_id: lease.id,
                fence: lease.fence,
                ttl_ms: LEASE_TTL_MS,
            };
            match self.call(renew).await {
                Ok(CommandResult::ControlRenewed { lease }) => {
                    self.leases.insert(port.to_string(), lease.clone());
                    return Ok(lease);
                }
                Ok(_) | Err(_) => {
                    self.leases.remove(port);
                }
            }
        }

        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let request = ClientMessage::AcquireControl {
                request_id: Uuid::new_v4(),
                port: port.to_string(),
                mode: ControlMode::Queue,
                ttl_ms: LEASE_TTL_MS,
            };
            let rpc_timeout = request_timeout(&request, Some(wait));
            match self.call_with_timeout(request, rpc_timeout).await? {
                CommandResult::ControlGranted { lease } => {
                    self.leases.insert(port.to_string(), lease.clone());
                    return Ok(lease);
                }
                CommandResult::ControlQueued { position } => {
                    if tokio::time::Instant::now() >= deadline {
                        self.cancel_queued_acquire(port).await.with_context(|| {
                            format!(
                                "timed out queued at position {position} and failed to cancel the \
                                 pending write-control request"
                            )
                        })?;
                        bail!(
                            "write control remained queued at position {position}; the queued \
                             acquire was cancelled and no takeover was attempted"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                other => bail!("unexpected acquire result: {other:?}"),
            }
        }
    }

    async fn cancel_queued_acquire(&mut self, port: &str) -> Result<()> {
        let cancel = ClientMessage::CancelAcquire {
            request_id: Uuid::new_v4(),
            port: port.to_string(),
            // Queued actors have no lease ID. seriald matches cancellation by
            // actor identity and intentionally ignores this wire field.
            control_id: Uuid::nil(),
        };
        let removed = match self.call(cancel).await? {
            CommandResult::AcquireCancelled { removed } => removed,
            other => bail!("unexpected cancel-acquire result: {other:?}"),
        };
        if removed {
            return Ok(());
        }

        // The waiter can be granted between the deadline check and cancel.
        // Resolve that race without disconnecting this actor (which may own
        // valid Runs on other Slots): reacquire returns AlreadyHeld when the
        // grant won, then release that exact lease; otherwise cancel the newly
        // observed queue entry.
        let probe = ClientMessage::AcquireControl {
            request_id: Uuid::new_v4(),
            port: port.to_string(),
            mode: ControlMode::Queue,
            ttl_ms: LEASE_TTL_MS,
        };
        match self.call(probe).await? {
            CommandResult::ControlGranted { lease } => {
                let release = ClientMessage::ReleaseControl {
                    request_id: Uuid::new_v4(),
                    port: port.to_string(),
                    control_id: lease.id,
                    fence: lease.fence,
                };
                match self.call(release).await? {
                    CommandResult::ControlReleased => Ok(()),
                    other => bail!("unexpected raced-acquire release result: {other:?}"),
                }
            }
            CommandResult::ControlQueued { .. } => {
                match self
                    .call(ClientMessage::CancelAcquire {
                        request_id: Uuid::new_v4(),
                        port: port.to_string(),
                        control_id: Uuid::nil(),
                    })
                    .await?
                {
                    CommandResult::AcquireCancelled { .. } => Ok(()),
                    other => bail!("unexpected second cancel-acquire result: {other:?}"),
                }
            }
            other => bail!("unexpected acquire race probe result: {other:?}"),
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
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
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
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
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
            Err(error) if is_sequence_boundary_rejection(&error) => {
                Err(anyhow::Error::new(SequenceBoundaryRejected {
                    message: error.to_string(),
                }))
            }
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

    async fn start_run(
        &mut self,
        port: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        control_wait: Duration,
    ) -> Result<StartedRun> {
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
        let lease = self.acquire_control(&port, control_wait).await?;
        let request = ClientMessage::StartRun {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            control_id: lease.id,
            fence: lease.fence,
            label,
            metadata,
        };
        match self.call(request).await {
            Ok(CommandResult::RunStarted { run }) => {
                let run_token = Uuid::new_v4();
                self.owned_runs.insert(
                    port,
                    OwnedRun::new_with_handle(
                        run.id,
                        run_token,
                        run_handle.clone(),
                        Instant::now(),
                    ),
                );
                Ok(StartedRun { run, run_handle })
            }
            Ok(other) => {
                self.best_effort_release(&port).await;
                bail!("unexpected start-run result: {other:?}")
            }
            Err(error) => {
                self.best_effort_release(&port).await;
                Err(error)
            }
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

    async fn release(
        &mut self,
        port: String,
        abort_run: bool,
        run_capability: Option<(Uuid, Uuid)>,
        allow_stale_cleanup: bool,
    ) -> Result<bool> {
        let Some(lease) = self.leases.get(&port).cloned() else {
            self.owned_runs.remove(&port);
            return Ok(false);
        };
        self.prepare_release(&port, abort_run, run_capability, allow_stale_cleanup)?;
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            port: port.clone(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlReleased) => {
                self.leases.remove(&port);
                self.owned_runs.remove(&port);
                Ok(true)
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

    fn prepare_release(
        &mut self,
        port: &str,
        abort_run: bool,
        run_capability: Option<(Uuid, Uuid)>,
        allow_stale_cleanup: bool,
    ) -> Result<()> {
        if allow_stale_cleanup {
            // A fresh daemon snapshot proved that no Run is active. Local Run
            // ownership is therefore stale bookkeeping, not authority to be
            // protected by abort_run. Discard it before attempting to release
            // the remaining local lease.
            self.owned_runs.remove(port);
        } else if let Some(run) = self.owned_runs.get(port) {
            if !abort_run {
                bail!(
                    "serial-mcp owns active Run {}; call run_end first or pass abort_run=true",
                    run.id
                );
            }
            let (run_id, run_token) = run_capability.context(
                "release would abort an active Run; pass the run_handle returned by this \
                 caller's run_start",
            )?;
            self.validate_run_capability(port, run_id, run_token)?;
        }
        Ok(())
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
        let timeout = request_timeout(&request, None);
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

fn request_timeout(request: &ClientMessage, control_wait: Option<Duration>) -> Duration {
    match request {
        ClientMessage::Write { .. } => WRITE_RPC_TIMEOUT,
        ClientMessage::AcquireControl { .. } => control_wait
            .unwrap_or(MAX_CONTROL_WAIT)
            .min(MAX_CONTROL_WAIT)
            .saturating_add(RPC_SERVICE_MARGIN),
        _ => DEFAULT_RPC_TIMEOUT,
    }
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
        retryable,
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

fn is_sequence_boundary_rejection(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<DaemonRequestError>()
        .is_some_and(|error| error.code == ErrorCode::SequenceBoundaryChanged)
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
