use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use serial_protocol::{
    Actor, ActorKind, ClientMessage, CommandResult, ControlLease, ControlMode, ErrorCode,
    PROTOCOL_VERSION, Role, RunInfo, ServerMessage, TriggerInfo, TriggerSpec, TriggerStatus,
    WireFrame, WritePacing, decode_wire_frame, encode_client_control,
};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const LEASE_TTL_MS: u64 = 60_000;
const RENEW_INTERVAL: Duration = Duration::from_secs(20);
const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const RPC_SERVICE_MARGIN: Duration = Duration::from_secs(5);
/// seriald caps one physical write at 15 seconds. The adapter must not call a
/// correctly progressing write uncertain before that legal server deadline.
const WRITE_RPC_TIMEOUT: Duration = Duration::from_secs(20);
// The session task serializes requests and lease renewal on one socket.
// Keeping queue waits at 15 seconds prevents one blocked Slot from starving
// the 20-second renewal cadence of leases held for other Slots.
const MAX_CONTROL_WAIT: Duration = Duration::from_secs(15);

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

#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::Sender<SessionRequest>,
}

enum SessionRequest {
    Write {
        slot_id: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        effective_pacing: WritePacing,
        reply: Reply,
    },
    SendBreak {
        slot_id: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        reply: Reply,
    },
    TriggerStart {
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        spec: TriggerSpec,
        reply: Reply,
    },
    TriggerStatus {
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        reply: Reply,
    },
    TriggerCancel {
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
        reply: Reply,
    },
    RunOwnership {
        slot_id: String,
        run_id: Uuid,
        reply: Reply,
    },
    StartRun {
        slot_id: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        control_wait: Duration,
        reply: Reply,
    },
    EndRun {
        slot_id: String,
        run_id: Uuid,
        reply: Reply,
    },
    Release {
        slot_id: String,
        abort_run: bool,
        reply: Reply,
    },
}

type Reply = oneshot::Sender<std::result::Result<SessionResponse, String>>;

// Responses cross a single oneshot and are consumed immediately. Keeping the
// protocol values inline avoids an allocation on every session RPC.
#[allow(clippy::large_enum_variant)]
enum SessionResponse {
    Write { event_seq: u64 },
    Break { event_seq: u64 },
    Trigger(TriggerInfo),
    Run(RunInfo),
    Released { had_lease: bool },
    RunOwnership { retained: bool },
}

impl SessionHandle {
    pub fn spawn(endpoint: String, token: String, actor_label: String) -> Self {
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(run_session(
            SessionState::new(endpoint, token, actor_label),
            rx,
        ));
        Self { tx }
    }

    pub async fn write(
        &self,
        slot_id: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        effective_pacing: WritePacing,
    ) -> Result<WriteResult> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Write {
                slot_id,
                data,
                operation_id,
                expected_run_id,
                effective_pacing,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Write { event_seq } => Ok(WriteResult { event_seq }),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn start_run(
        &self,
        slot_id: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        control_wait: Duration,
    ) -> Result<RunInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::StartRun {
                slot_id,
                label,
                metadata,
                control_wait,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Run(run) => Ok(run),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn send_break(
        &self,
        slot_id: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
    ) -> Result<WriteResult> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::SendBreak {
                slot_id,
                duration_ms,
                operation_id,
                expected_run_id,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Break { event_seq } => Ok(WriteResult { event_seq }),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn trigger_start(
        &self,
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        spec: TriggerSpec,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerStart {
                slot_id,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
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
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerStatus {
                slot_id,
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
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
    ) -> Result<TriggerInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::TriggerCancel {
                slot_id,
                daemon_epoch,
                generation,
                trigger_id,
                expected_run_id,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Trigger(trigger) => Ok(trigger),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn run_ownership_retained(&self, slot_id: String, run_id: Uuid) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::RunOwnership {
                slot_id,
                run_id,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::RunOwnership { retained } => Ok(retained),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn end_run(&self, slot_id: String, run_id: Uuid) -> Result<RunInfo> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::EndRun {
                slot_id,
                run_id,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Run(run) => Ok(run),
            _ => bail!("serial session returned the wrong response type"),
        }
    }

    pub async fn release(&self, slot_id: String, abort_run: bool) -> Result<bool> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Release {
                slot_id,
                abort_run,
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

async fn receive(
    response: oneshot::Receiver<std::result::Result<SessionResponse, String>>,
) -> Result<SessionResponse> {
    response
        .await
        .context("serial session task dropped its response")?
        .map_err(anyhow::Error::msg)
}

async fn run_session(mut state: SessionState, mut rx: mpsc::Receiver<SessionRequest>) {
    let mut renew = tokio::time::interval(RENEW_INTERVAL);
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            // A completed long request can leave both branches ready. Always
            // renew first so queued work for other Slots cannot consume the
            // remaining lease lifetime.
            biased;
            _ = renew.tick() => state.renew_all().await,
            request = rx.recv() => {
                let Some(request) = request else { break; };
                state.handle(request).await;
            }
        }
    }
}

struct SessionState {
    endpoint: String,
    token: String,
    actor_label: String,
    socket: Option<Socket>,
    actor: Option<Actor>,
    role: Option<Role>,
    leases: HashMap<String, ControlLease>,
    owned_runs: HashMap<String, Uuid>,
}

impl SessionState {
    fn new(endpoint: String, token: String, actor_label: String) -> Self {
        Self {
            endpoint,
            token,
            actor_label,
            socket: None,
            actor: None,
            role: None,
            leases: HashMap::new(),
            owned_runs: HashMap::new(),
        }
    }

    async fn handle(&mut self, request: SessionRequest) {
        match request {
            SessionRequest::Write {
                slot_id,
                data,
                operation_id,
                expected_run_id,
                effective_pacing,
                reply,
            } => {
                let result = self
                    .write(
                        slot_id,
                        data,
                        operation_id,
                        expected_run_id,
                        effective_pacing,
                    )
                    .await
                    .map(|event_seq| SessionResponse::Write { event_seq });
                send_reply(reply, result);
            }
            SessionRequest::StartRun {
                slot_id,
                label,
                metadata,
                control_wait,
                reply,
            } => {
                let result = self
                    .start_run(slot_id, label, metadata, control_wait)
                    .await
                    .map(SessionResponse::Run);
                send_reply(reply, result);
            }
            SessionRequest::SendBreak {
                slot_id,
                duration_ms,
                operation_id,
                expected_run_id,
                reply,
            } => {
                let result = self
                    .send_break(slot_id, duration_ms, operation_id, expected_run_id)
                    .await
                    .map(|event_seq| SessionResponse::Break { event_seq });
                send_reply(reply, result);
            }
            SessionRequest::TriggerStart {
                slot_id,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                spec,
                reply,
            } => {
                let result = self
                    .trigger_start(
                        slot_id,
                        daemon_epoch,
                        generation,
                        operation_id,
                        expected_run_id,
                        spec,
                    )
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::TriggerStatus {
                slot_id,
                daemon_epoch,
                generation,
                trigger_id,
                reply,
            } => {
                let result = self
                    .trigger_status(slot_id, daemon_epoch, generation, trigger_id)
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::TriggerCancel {
                slot_id,
                daemon_epoch,
                generation,
                trigger_id,
                expected_run_id,
                reply,
            } => {
                let result = self
                    .trigger_cancel(
                        slot_id,
                        daemon_epoch,
                        generation,
                        trigger_id,
                        expected_run_id,
                    )
                    .await
                    .map(SessionResponse::Trigger);
                send_reply(reply, result);
            }
            SessionRequest::RunOwnership {
                slot_id,
                run_id,
                reply,
            } => {
                let retained = retains_run_ownership(
                    self.socket.is_some(),
                    self.owned_runs.get(&slot_id).copied(),
                    self.leases.contains_key(&slot_id),
                    run_id,
                );
                send_reply(reply, Ok(SessionResponse::RunOwnership { retained }));
            }
            SessionRequest::EndRun {
                slot_id,
                run_id,
                reply,
            } => {
                let result = self
                    .end_run(slot_id, run_id)
                    .await
                    .map(SessionResponse::Run);
                send_reply(reply, result);
            }
            SessionRequest::Release {
                slot_id,
                abort_run,
                reply,
            } => {
                let result = self
                    .release(slot_id, abort_run)
                    .await
                    .map(|had_lease| SessionResponse::Released { had_lease });
                send_reply(reply, result);
            }
        }
    }

    async fn connect(&mut self) -> Result<()> {
        if self.socket.is_some() {
            return Ok(());
        }
        self.leases.clear();
        self.owned_runs.clear();
        self.actor = None;
        self.role = None;
        let mut request = ws_url(&self.endpoint)?.into_client_request()?;
        request.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", self.token)
                .parse()
                .context("operator token cannot be encoded as an HTTP header")?,
        );
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
                    actor,
                    role,
                    ..
                }) => {
                    ensure_welcome_protocol(protocol_version)?;
                    if role < Role::Operator {
                        bail!("serial-mcp requires an operator token; daemon granted {role:?}");
                    }
                    self.actor = Some(actor);
                    self.role = Some(role);
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

    async fn acquire_control(&mut self, slot_id: &str, wait: Duration) -> Result<ControlLease> {
        self.connect().await?;
        if let Some(lease) = self.leases.get(slot_id).cloned() {
            let request_id = Uuid::new_v4();
            let renew = ClientMessage::RenewControl {
                request_id,
                slot_id: slot_id.to_string(),
                control_id: lease.id,
                fence: lease.fence,
                ttl_ms: LEASE_TTL_MS,
            };
            match self.call(renew).await {
                Ok(CommandResult::ControlRenewed { lease }) => {
                    self.leases.insert(slot_id.to_string(), lease.clone());
                    return Ok(lease);
                }
                Ok(_) | Err(_) => {
                    self.leases.remove(slot_id);
                }
            }
        }

        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let request = ClientMessage::AcquireControl {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.to_string(),
                mode: ControlMode::Queue,
                ttl_ms: LEASE_TTL_MS,
            };
            let rpc_timeout = request_timeout(&request, Some(wait));
            match self.call_with_timeout(request, rpc_timeout).await? {
                CommandResult::ControlGranted { lease } => {
                    self.leases.insert(slot_id.to_string(), lease.clone());
                    return Ok(lease);
                }
                CommandResult::ControlQueued { position } => {
                    if tokio::time::Instant::now() >= deadline {
                        self.cancel_queued_acquire(slot_id).await.with_context(|| {
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

    async fn cancel_queued_acquire(&mut self, slot_id: &str) -> Result<()> {
        let cancel = ClientMessage::CancelAcquire {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.to_string(),
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
            slot_id: slot_id.to_string(),
            mode: ControlMode::Queue,
            ttl_ms: LEASE_TTL_MS,
        };
        match self.call(probe).await? {
            CommandResult::ControlGranted { lease } => {
                let release = ClientMessage::ReleaseControl {
                    request_id: Uuid::new_v4(),
                    slot_id: slot_id.to_string(),
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
                        slot_id: slot_id.to_string(),
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
        slot_id: &str,
        expected_run_id: Uuid,
    ) -> Result<ControlLease> {
        match self.owned_runs.get(slot_id) {
            Some(run_id) if *run_id == expected_run_id => {}
            Some(run_id) => bail!(
                "serial-mcp owns Run {run_id} on Slot {slot_id:?}, not expected Run \
                 {expected_run_id}; no bytes were written"
            ),
            None => bail!(
                "serial-mcp does not own an active Run on Slot {slot_id:?}; call run_start before \
                 command; no bytes were written"
            ),
        }
        if self.socket.is_none() {
            self.disconnect();
            bail!(
                "the serial connection was lost and Run {expected_run_id} can no longer be \
                 trusted; start a new Run before writing"
            );
        }
        let Some(lease) = self.leases.get(slot_id).cloned() else {
            self.disconnect();
            bail!(
                "serial-mcp lost the control lease for Run {expected_run_id}; start a new Run \
                 before writing"
            );
        };
        let request = ClientMessage::RenewControl {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.to_string(),
            control_id: lease.id,
            fence: lease.fence,
            ttl_ms: LEASE_TTL_MS,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlRenewed { lease }) => {
                self.leases.insert(slot_id.to_string(), lease.clone());
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

    async fn trigger_start(
        &mut self,
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        spec: TriggerSpec,
    ) -> Result<TriggerInfo> {
        let lease = self
            .renew_owned_run_control(&slot_id, expected_run_id)
            .await?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::TriggerStart {
            request_id,
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            daemon_epoch,
            generation,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            spec,
        };
        match self.call(request).await {
            Ok(CommandResult::TriggerStarted { trigger }) => Ok(*trigger),
            Ok(other) => bail!("unexpected trigger-start result: {other:?}"),
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
        slot_id: String,
        duration_ms: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
    ) -> Result<u64> {
        let lease = self
            .renew_owned_run_control(&slot_id, expected_run_id)
            .await?;
        self.actor
            .as_ref()
            .context("serial session has no actor identity")?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::SendBreak {
            request_id,
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            duration_ms,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
        };
        let timeout = Duration::from_millis(duration_ms).saturating_add(RPC_SERVICE_MARGIN);
        match self.call_with_timeout(request, timeout).await {
            Ok(CommandResult::BreakSent { event_seq }) => Ok(event_seq),
            Ok(other) => bail!("unexpected Break result: {other:?}"),
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
        slot_id: String,
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
                &slot_id,
                daemon_epoch,
                generation,
                trigger_id,
            ))
            .await
        {
            Ok(result) => result,
            Err(error) if is_transport_error(&error) || is_timeout_error(&error) => self
                .call(trigger_status_request(
                    &slot_id,
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
        self.observe_trigger_terminal(&slot_id, trigger.status);
        Ok(trigger)
    }

    async fn trigger_cancel(
        &mut self,
        slot_id: String,
        daemon_epoch: Uuid,
        generation: u64,
        trigger_id: Uuid,
        expected_run_id: Uuid,
    ) -> Result<TriggerInfo> {
        let lease = self
            .renew_owned_run_control(&slot_id, expected_run_id)
            .await?;
        let request = ClientMessage::TriggerCancel {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
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
        self.observe_trigger_terminal(&slot_id, trigger.status);
        Ok(trigger)
    }

    fn observe_trigger_terminal(&mut self, slot_id: &str, status: TriggerStatus) {
        if matches!(
            status,
            TriggerStatus::ControlLost
                | TriggerStatus::RunLost
                | TriggerStatus::GenerationChanged
                | TriggerStatus::PortClosed
        ) {
            self.leases.remove(slot_id);
            self.owned_runs.remove(slot_id);
        }
    }

    async fn write(
        &mut self,
        slot_id: String,
        data: Vec<u8>,
        operation_id: Uuid,
        expected_run_id: Uuid,
        effective_pacing: WritePacing,
    ) -> Result<u64> {
        let lease = self
            .renew_owned_run_control(&slot_id, expected_run_id)
            .await?;
        self.actor
            .as_ref()
            .context("serial session has no actor identity")?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::Write {
            request_id,
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            data,
            operation_id: Some(operation_id),
            expected_run_id: Some(expected_run_id),
            // Agent tools never override Slot/Device pacing. The effective
            // value is used only for the local RPC deadline below.
            pacing: None,
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
        slot_id: String,
        label: String,
        metadata: std::collections::BTreeMap<String, Value>,
        control_wait: Duration,
    ) -> Result<RunInfo> {
        if let Some(run_id) = self.owned_runs.get(&slot_id) {
            bail!("serial-mcp already owns active Run {run_id} on Slot {slot_id:?}");
        }
        let lease = self.acquire_control(&slot_id, control_wait).await?;
        let request = ClientMessage::StartRun {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
            control_id: lease.id,
            fence: lease.fence,
            label,
            metadata,
        };
        match self.call(request).await {
            Ok(CommandResult::RunStarted { run }) => {
                self.owned_runs.insert(slot_id, run.id);
                Ok(run)
            }
            Ok(other) => {
                self.best_effort_release(&slot_id).await;
                bail!("unexpected start-run result: {other:?}")
            }
            Err(error) => {
                self.best_effort_release(&slot_id).await;
                Err(error)
            }
        }
    }

    async fn end_run(&mut self, slot_id: String, run_id: Uuid) -> Result<RunInfo> {
        match self.owned_runs.get(&slot_id) {
            Some(owned_run_id) if *owned_run_id == run_id => {}
            Some(owned_run_id) => bail!(
                "serial-mcp owns Run {owned_run_id} on Slot {slot_id:?}, not requested Run {run_id}"
            ),
            None => bail!(
                "serial-mcp does not own an active Run on Slot {slot_id:?}; it cannot end another \
                 process's Run"
            ),
        }
        let lease = self.renew_owned_run_control(&slot_id, run_id).await?;
        let request = ClientMessage::EndRun {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
            control_id: lease.id,
            fence: lease.fence,
            run_id,
        };
        match self.call(request).await {
            Ok(CommandResult::RunEnded { run }) => {
                self.owned_runs.remove(&slot_id);
                self.best_effort_release(&slot_id).await;
                Ok(run)
            }
            Ok(other) => bail!("unexpected end-run result: {other:?}"),
            Err(error) => {
                self.disconnect();
                Err(error)
            }
        }
    }

    async fn release(&mut self, slot_id: String, abort_run: bool) -> Result<bool> {
        let Some(lease) = self.leases.get(&slot_id).cloned() else {
            self.owned_runs.remove(&slot_id);
            return Ok(false);
        };
        if let Some(run_id) = self.owned_runs.get(&slot_id)
            && !abort_run
        {
            bail!("serial-mcp owns active Run {run_id}; call run_end first or pass abort_run=true");
        }
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await? {
            CommandResult::ControlReleased => {
                self.leases.remove(&slot_id);
                self.owned_runs.remove(&slot_id);
                Ok(true)
            }
            other => bail!("unexpected release result: {other:?}"),
        }
    }

    async fn best_effort_release(&mut self, slot_id: &str) {
        self.owned_runs.remove(slot_id);
        let Some(lease) = self.leases.remove(slot_id) else {
            return;
        };
        if self.socket.is_none() {
            return;
        }
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.to_string(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await {
            Ok(CommandResult::ControlReleased) => {}
            Ok(other) => eprintln!(
                "serial-mcp: best-effort control release returned an unexpected result for Slot \
                 {slot_id:?}: {other:?}; the lease will expire at its TTL"
            ),
            Err(error) => eprintln!(
                "serial-mcp: best-effort control release failed for Slot {slot_id:?}: {error}; \
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
        let leases = match self.renewal_targets() {
            Ok(leases) => leases,
            Err(error) => {
                eprintln!("serial-mcp: {error}; forgetting all active Runs");
                self.disconnect();
                return;
            }
        };
        for (slot_id, lease) in leases {
            let request = ClientMessage::RenewControl {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.clone(),
                control_id: lease.id,
                fence: lease.fence,
                ttl_ms: LEASE_TTL_MS,
            };
            match self.call(request).await {
                Ok(CommandResult::ControlRenewed { lease }) => {
                    self.leases.insert(slot_id, lease);
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

    fn renewal_targets(&self) -> Result<Vec<(String, ControlLease)>> {
        let mut targets = self
            .owned_runs
            .keys()
            .map(|slot_id| {
                self.leases
                    .get(slot_id)
                    .cloned()
                    .map(|lease| (slot_id.clone(), lease))
                    .with_context(|| {
                        format!(
                            "active Run on Slot {slot_id:?} has no local control lease; its \
                             ownership can no longer be trusted"
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        targets.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(targets)
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
        self.role = None;
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

fn retains_run_ownership(
    socket_connected: bool,
    owned_run_id: Option<Uuid>,
    has_lease: bool,
    expected_run_id: Uuid,
) -> bool {
    socket_connected && owned_run_id == Some(expected_run_id) && has_lease
}

fn trigger_status_request(
    slot_id: &str,
    daemon_epoch: Uuid,
    generation: u64,
    trigger_id: Uuid,
) -> ClientMessage {
    ClientMessage::TriggerStatus {
        request_id: Uuid::new_v4(),
        slot_id: slot_id.to_string(),
        daemon_epoch,
        generation,
        trigger_id,
    }
}

fn send_reply(reply: Reply, result: Result<SessionResponse>) {
    let _ = reply.send(result.map_err(|error| error.to_string()));
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

fn is_definite_prewrite_rejection(error: &anyhow::Error) -> bool {
    let Some(error) = error.downcast_ref::<DaemonRequestError>() else {
        return false;
    };
    // These authorization checks happen before seriald calls the physical
    // writer, so their retry safety does not depend on daemon prose.
    if matches!(
        error.code,
        ErrorCode::ControlRequired | ErrorCode::StaleFence
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

    #[test]
    fn websocket_welcome_protocol_gate_is_fail_closed() {
        assert!(ensure_welcome_protocol(PROTOCOL_VERSION).is_ok());
        let error = ensure_welcome_protocol(PROTOCOL_VERSION.saturating_sub(1)).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("install seriald and serial-mcp from the same release")
        );
    }

    #[test]
    fn websocket_url_is_derived_without_exposing_credentials() {
        assert_eq!(
            ws_url("http://192.168.56.1:3210").unwrap(),
            "ws://192.168.56.1:3210/api/v1/ws"
        );
    }

    #[test]
    fn request_timeouts_follow_the_operation_budget() {
        let acquire = ClientMessage::AcquireControl {
            request_id: Uuid::nil(),
            slot_id: "bench".into(),
            mode: ControlMode::Queue,
            ttl_ms: LEASE_TTL_MS,
        };
        assert_eq!(
            request_timeout(&acquire, Some(Duration::from_secs(5))),
            Duration::from_secs(10)
        );
        assert_eq!(
            request_timeout(&acquire, Some(Duration::from_secs(15))),
            Duration::from_secs(20)
        );
        assert_eq!(
            request_timeout(&acquire, Some(Duration::from_secs(60))),
            Duration::from_secs(20)
        );

        let write = ClientMessage::Write {
            request_id: Uuid::nil(),
            slot_id: "bench".into(),
            control_id: Uuid::nil(),
            fence: 1,
            data: b"help\r".to_vec(),
            operation_id: Some(Uuid::nil()),
            expected_run_id: Some(Uuid::nil()),
            pacing: None,
            cooperative: false,
        };
        assert_eq!(request_timeout(&write, None), Duration::from_secs(20));
        assert_eq!(
            write_request_timeout(
                8,
                WritePacing {
                    chunk_size: 8,
                    chunk_delay_ms: 1,
                }
            ),
            Duration::from_secs(5)
        );
        assert_eq!(
            write_request_timeout(
                4_096,
                WritePacing {
                    chunk_size: 1,
                    chunk_delay_ms: 1,
                }
            ),
            Duration::from_millis(9_095)
        );
        assert_eq!(
            write_request_timeout(
                4_096,
                WritePacing {
                    chunk_size: 1,
                    chunk_delay_ms: 10,
                }
            ),
            WRITE_RPC_TIMEOUT
        );

        let release = ClientMessage::ReleaseControl {
            request_id: Uuid::nil(),
            slot_id: "bench".into(),
            control_id: Uuid::nil(),
            fence: 1,
        };
        assert_eq!(request_timeout(&release, None), Duration::from_secs(5));
    }

    #[test]
    fn retained_run_ownership_requires_socket_matching_run_and_lease() {
        let run_id = Uuid::new_v4();
        assert!(retains_run_ownership(true, Some(run_id), true, run_id));
        assert!(!retains_run_ownership(false, Some(run_id), true, run_id));
        assert!(!retains_run_ownership(true, None, true, run_id));
        assert!(!retains_run_ownership(
            true,
            Some(Uuid::new_v4()),
            true,
            run_id
        ));
        assert!(!retains_run_ownership(true, Some(run_id), false, run_id));
    }

    #[test]
    fn trigger_status_lookup_needs_no_local_run_or_control_fields() {
        let daemon_epoch = Uuid::new_v4();
        let trigger_id = Uuid::new_v4();
        match trigger_status_request("bench", daemon_epoch, 7, trigger_id) {
            ClientMessage::TriggerStatus {
                slot_id,
                daemon_epoch: actual_epoch,
                generation,
                trigger_id: actual_trigger,
                ..
            } => {
                assert_eq!(slot_id, "bench");
                assert_eq!(actual_epoch, daemon_epoch);
                assert_eq!(generation, 7);
                assert_eq!(actual_trigger, trigger_id);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[test]
    fn definite_prewrite_rejections_are_safe_to_retry() {
        let pacing = daemon_error(
            ErrorCode::BadRequest,
            false,
            "serial write pacing requires an estimated 16000 ms, exceeding the 15000 ms request \
             limit; increase chunk_size, reduce chunk_delay_ms, or split the write (no bytes \
             were written)"
                .into(),
        );
        assert!(is_definite_prewrite_rejection(&pacing));

        let lease = daemon_error(
            ErrorCode::Conflict,
            true,
            "control lease has only 1000 ms remaining, but this serial write requires 2000 ms \
             plus a 100 ms scheduling margin; renew control or shorten the write and retry (no \
             bytes were written)"
                .into(),
        );
        assert!(is_definite_prewrite_rejection(&lease));

        let run = daemon_error(
            ErrorCode::Conflict,
            false,
            format!(
                "serial write expected active Run {}, but no Run is active (no bytes were written)",
                Uuid::nil()
            ),
        );
        assert!(is_expected_run_rejection(&run));
        assert!(is_definite_prewrite_rejection(&run));

        for code in [ErrorCode::ControlRequired, ErrorCode::StaleFence] {
            let revoked = daemon_error(code, false, "lease is no longer current".into());
            assert!(is_control_loss_rejection(&revoked));
            assert!(is_definite_prewrite_rejection(&revoked));
        }

        let partial = daemon_error(
            ErrorCode::Conflict,
            false,
            "serial write failed before all bytes were accepted (3/10)".into(),
        );
        assert!(!is_definite_prewrite_rejection(&partial));
        assert!(!is_definite_prewrite_rejection(&anyhow::anyhow!(
            "transport timeout"
        )));
    }

    fn test_actor() -> Actor {
        Actor {
            id: "agent:test".into(),
            label: "test".into(),
            kind: ActorKind::Agent,
        }
    }

    fn test_lease() -> ControlLease {
        ControlLease {
            id: Uuid::new_v4(),
            owner: test_actor(),
            epoch: Uuid::new_v4(),
            generation: 1,
            fence: 1,
            issued_wall_time_ns: 0,
            expires_wall_time_ns: i64::MAX,
        }
    }

    fn test_trigger_spec() -> TriggerSpec {
        TriggerSpec {
            initial_write: Some(b"reset\r".to_vec()),
            start_contains: None,
            action: b"x".to_vec(),
            interval_ms: 20,
            stop_contains: vec![b"ready".to_vec()],
            timeout_ms: 5_000,
            max_fires: 250,
            pacing: None,
        }
    }

    #[test]
    fn renewal_targets_include_only_slots_with_owned_active_runs() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        state.leases.insert("run-slot".into(), test_lease());
        state.leases.insert("bare-lease".into(), test_lease());
        state.owned_runs.insert("run-slot".into(), Uuid::new_v4());

        let targets = state.renewal_targets().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, "run-slot");

        state
            .owned_runs
            .insert("missing-lease".into(), Uuid::new_v4());
        assert!(state.renewal_targets().is_err());
    }

    #[tokio::test]
    async fn write_without_an_owned_run_fails_before_connecting() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        let error = state
            .write(
                "bench".into(),
                b"help\r".to_vec(),
                Uuid::new_v4(),
                Uuid::new_v4(),
                WritePacing {
                    chunk_size: 1,
                    chunk_delay_ms: 1,
                },
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("call run_start"));
        assert!(state.socket.is_none());
        assert!(state.leases.is_empty());
    }

    #[tokio::test]
    async fn break_without_an_owned_run_fails_before_connecting() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        let error = state
            .send_break("bench".into(), 250, Uuid::new_v4(), Uuid::new_v4())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("call run_start"));
        assert!(state.socket.is_none());
        assert!(state.leases.is_empty());
    }

    #[tokio::test]
    async fn trigger_without_an_owned_run_fails_before_connecting() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        let error = state
            .trigger_start(
                "bench".into(),
                Uuid::new_v4(),
                1,
                Uuid::new_v4(),
                Uuid::new_v4(),
                test_trigger_spec(),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("call run_start"));
        assert!(state.socket.is_none());
        assert!(state.leases.is_empty());
    }

    #[test]
    fn trigger_ownership_loss_forgets_only_the_affected_slot() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        for slot in ["bench-a", "bench-b"] {
            state.leases.insert(slot.into(), test_lease());
            state.owned_runs.insert(slot.into(), Uuid::new_v4());
        }

        state.observe_trigger_terminal("bench-a", TriggerStatus::ControlLost);
        assert!(!state.leases.contains_key("bench-a"));
        assert!(!state.owned_runs.contains_key("bench-a"));
        assert!(state.leases.contains_key("bench-b"));
        assert!(state.owned_runs.contains_key("bench-b"));
    }

    #[tokio::test]
    async fn release_without_a_local_lease_is_idempotent_and_forgets_stale_run_state() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        state.owned_runs.insert("bench".into(), Uuid::new_v4());

        assert!(!state.release("bench".into(), false).await.unwrap());
        assert!(state.owned_runs.is_empty());
        assert!(state.socket.is_none());
    }

    #[tokio::test]
    async fn lost_connection_clears_owned_runs_instead_of_reacquiring_control() {
        let mut state =
            SessionState::new("http://127.0.0.1:1".into(), "token".into(), "test".into());
        let run_id = Uuid::new_v4();
        state.owned_runs.insert("bench".into(), run_id);
        state.leases.insert("bench".into(), test_lease());

        let error = state
            .renew_owned_run_control("bench", run_id)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("connection was lost"));
        assert!(state.owned_runs.is_empty());
        assert!(state.leases.is_empty());
    }
}
