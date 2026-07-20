use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use serial_protocol::{
    Actor, ActorKind, ClientMessage, CommandResult, ControlLease, ControlMode, ErrorCode,
    PROTOCOL_VERSION, Role, RunInfo, ServerMessage, WireFrame, decode_wire_frame,
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
const LEASE_TTL_MS: u64 = 60_000;
const RENEW_INTERVAL: Duration = Duration::from_secs(20);

#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::Sender<SessionRequest>,
}

enum SessionRequest {
    Write {
        slot_id: String,
        data: Vec<u8>,
        operation_id: Uuid,
        control_wait: Duration,
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
        reply: Reply,
    },
}

type Reply = oneshot::Sender<std::result::Result<SessionResponse, String>>;

enum SessionResponse {
    Write {
        event_seq: u64,
        actor: Actor,
        request_id: Uuid,
    },
    Run(RunInfo),
    Released,
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
        control_wait: Duration,
    ) -> Result<WriteResult> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Write {
                slot_id,
                data,
                operation_id,
                control_wait,
                reply,
            })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Write {
                event_seq,
                actor,
                request_id,
            } => Ok(WriteResult {
                event_seq,
                actor,
                request_id,
            }),
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

    pub async fn release(&self, slot_id: String) -> Result<()> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(SessionRequest::Release { slot_id, reply })
            .await
            .context("serial session task stopped")?;
        match receive(response).await? {
            SessionResponse::Released => Ok(()),
            _ => bail!("serial session returned the wrong response type"),
        }
    }
}

pub struct WriteResult {
    pub event_seq: u64,
    pub actor: Actor,
    pub request_id: Uuid,
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
            request = rx.recv() => {
                let Some(request) = request else { break; };
                state.handle(request).await;
            }
            _ = renew.tick() => state.renew_all().await,
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
        }
    }

    async fn handle(&mut self, request: SessionRequest) {
        match request {
            SessionRequest::Write {
                slot_id,
                data,
                operation_id,
                control_wait,
                reply,
            } => {
                let result = self
                    .write(slot_id, data, operation_id, control_wait)
                    .await
                    .map(|(event_seq, actor, request_id)| SessionResponse::Write {
                        event_seq,
                        actor,
                        request_id,
                    });
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
            SessionRequest::Release { slot_id, reply } => {
                let result = self
                    .release(slot_id)
                    .await
                    .map(|()| SessionResponse::Released);
                send_reply(reply, result);
            }
        }
    }

    async fn connect(&mut self) -> Result<()> {
        if self.socket.is_some() {
            return Ok(());
        }
        self.leases.clear();
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
                WireFrame::Control(ServerMessage::Welcome { actor, role, .. }) => {
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

    async fn ensure_control(&mut self, slot_id: &str, wait: Duration) -> Result<ControlLease> {
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
            match self.call(request).await? {
                CommandResult::ControlGranted { lease } => {
                    self.leases.insert(slot_id.to_string(), lease.clone());
                    return Ok(lease);
                }
                CommandResult::ControlQueued { position } => {
                    if tokio::time::Instant::now() >= deadline {
                        bail!(
                            "write control is still queued at position {position}; no takeover was attempted"
                        );
                    }
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                other => bail!("unexpected acquire result: {other:?}"),
            }
        }
    }

    async fn write(
        &mut self,
        slot_id: String,
        data: Vec<u8>,
        operation_id: Uuid,
        control_wait: Duration,
    ) -> Result<(u64, Actor, Uuid)> {
        let lease = self.ensure_control(&slot_id, control_wait).await?;
        let actor = self
            .actor
            .clone()
            .context("serial session has no actor identity")?;
        let request_id = Uuid::new_v4();
        let request = ClientMessage::Write {
            request_id,
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            data,
            operation_id: Some(operation_id),
            pacing: None,
        };
        match self.call(request).await {
            Ok(CommandResult::WriteAccepted { event_seq }) => Ok((event_seq, actor, request_id)),
            Ok(other) => bail!("unexpected write result: {other:?}"),
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
        let lease = self.ensure_control(&slot_id, control_wait).await?;
        let request = ClientMessage::StartRun {
            request_id: Uuid::new_v4(),
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            label,
            metadata,
        };
        match self.call(request).await? {
            CommandResult::RunStarted { run } => Ok(run),
            other => bail!("unexpected start-run result: {other:?}"),
        }
    }

    async fn end_run(&mut self, slot_id: String, run_id: Uuid) -> Result<RunInfo> {
        let lease = self
            .ensure_control(&slot_id, Duration::from_secs(5))
            .await?;
        let request = ClientMessage::EndRun {
            request_id: Uuid::new_v4(),
            slot_id,
            control_id: lease.id,
            fence: lease.fence,
            run_id,
        };
        match self.call(request).await? {
            CommandResult::RunEnded { run } => Ok(run),
            other => bail!("unexpected end-run result: {other:?}"),
        }
    }

    async fn release(&mut self, slot_id: String) -> Result<()> {
        let lease = self
            .leases
            .get(&slot_id)
            .cloned()
            .context("this serial-mcp process does not hold control for the Slot")?;
        let request = ClientMessage::ReleaseControl {
            request_id: Uuid::new_v4(),
            slot_id: slot_id.clone(),
            control_id: lease.id,
            fence: lease.fence,
        };
        match self.call(request).await? {
            CommandResult::ControlReleased => {
                self.leases.remove(&slot_id);
                Ok(())
            }
            other => bail!("unexpected release result: {other:?}"),
        }
    }

    async fn renew_all(&mut self) {
        if self.socket.is_none() || self.leases.is_empty() {
            return;
        }
        let leases: Vec<(String, ControlLease)> = self
            .leases
            .iter()
            .map(|(slot, lease)| (slot.clone(), lease.clone()))
            .collect();
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

    async fn call(&mut self, request: ClientMessage) -> Result<CommandResult> {
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
        let response =
            tokio::time::timeout(Duration::from_secs(5), wait_result(socket, request_id)).await;
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
                bail!("timed out waiting for seriald request {request_id}")
            }
        }
    }

    fn disconnect(&mut self) {
        self.socket = None;
        self.actor = None;
        self.role = None;
        self.leases.clear();
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
    anyhow!("seriald {code:?} (retryable={retryable}): {message}")
}

fn is_transport_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    message.contains("WebSocket") || message.contains("connection") || message.contains("closed")
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
    fn websocket_url_is_derived_without_exposing_credentials() {
        assert_eq!(
            ws_url("http://192.168.56.1:3210").unwrap(),
            "ws://192.168.56.1:3210/api/v1/ws"
        );
    }
}
