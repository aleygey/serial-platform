use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, ErrorCode, PROTOCOL_VERSION, Role, ServerMessage,
    Subscription, WireFrame, decode_wire_frame, encode_client_control,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};
use uuid::Uuid;

#[derive(Debug)]
pub enum NetworkCommand {
    Send {
        generation: u64,
        message: ClientMessage,
    },
    /// Close the current actor connection and immediately reconnect. This is
    /// the v1 escape hatch for cancelling a queued control request because the
    /// protocol does not yet expose a dedicated cancel-acquire message.
    Reconnect {
        reason: String,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum NetworkEvent {
    TransportConnected { generation: u64 },
    Disconnected { reason: String },
    Frame(Box<WireFrame>),
    SendRejected { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReconnectDirective {
    PreserveCursors(String),
    ResetCursors(String),
}

pub struct NetworkHandle {
    pub commands: mpsc::Sender<NetworkCommand>,
    pub events: mpsc::Receiver<NetworkEvent>,
}

pub fn spawn(endpoint: String, token: Option<String>, slots: Vec<String>) -> NetworkHandle {
    // Writes and control RPCs are bounded too: a stalled connection must not
    // accumulate arbitrary RAW keystrokes or a large paste in memory.
    let (command_tx, command_rx) = mpsc::channel(256);
    // A slow terminal must not create an unbounded client-side queue. Once
    // this fills, WebSocket backpressure lets seriald apply its per-consumer
    // lag policy without ever blocking the physical serial reader.
    let (event_tx, event_rx) = mpsc::channel(1_024);
    tokio::spawn(run_worker(endpoint, token, slots, command_rx, event_tx));
    NetworkHandle {
        commands: command_tx,
        events: event_rx,
    }
}

/// Authenticates a short-lived WebSocket without attaching any Slot and
/// returns the daemon-authoritative role. `serialctl init` uses this to avoid
/// accidentally persisting an observer or admin credential for daily use.
pub async fn probe_role(endpoint: &str, token: &str) -> Result<Role> {
    let mut socket = connect(endpoint, Some(token)).await?;
    let hello = ClientMessage::Hello {
        request_id: Uuid::new_v4(),
        protocol_version: PROTOCOL_VERSION,
        client_name: "serialctl-init-role-check".into(),
        actor_kind: ActorKind::Human,
    };
    send_control(&mut socket, &hello).await?;

    let role = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Binary(bytes))) => match decode_wire_frame(&bytes)? {
                    WireFrame::Control(ServerMessage::Welcome { role, .. }) => {
                        return Ok::<Role, anyhow::Error>(role);
                    }
                    WireFrame::Control(ServerMessage::Error { message, .. }) => {
                        bail!("seriald rejected the operator credential: {message}")
                    }
                    _ => {}
                },
                Some(Ok(Message::Ping(payload))) => socket.send(Message::Pong(payload)).await?,
                Some(Ok(Message::Close(_))) | None => {
                    bail!("seriald closed the role-check connection before authentication")
                }
                Some(Ok(Message::Text(_))) => {
                    bail!("seriald sent an unsupported text role-check response")
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.into()),
            }
        }
    })
    .await
    .context("timed out verifying the operator token")??;
    let _ = socket.close(None).await;
    Ok(role)
}

async fn run_worker(
    endpoint: String,
    token: Option<String>,
    slots: Vec<String>,
    mut commands: mpsc::Receiver<NetworkCommand>,
    events: mpsc::Sender<NetworkEvent>,
) {
    let mut cursors = HashMap::<String, Cursor>::new();
    let mut slot_epochs = HashMap::new();
    let mut generation = 0u64;
    let mut backoff = Duration::from_millis(250);

    'reconnect: loop {
        let connection = tokio::select! {
            result = connect(&endpoint, token.as_deref()) => result,
            command = commands.recv() => {
                match command {
                    Some(NetworkCommand::Shutdown) | None => break 'reconnect,
                    Some(NetworkCommand::Reconnect { .. }) => continue 'reconnect,
                    Some(NetworkCommand::Send { .. }) => {
                        let _ = events.send(NetworkEvent::SendRejected {
                            reason: "not connected; input was not queued".into(),
                        }).await;
                        continue 'reconnect;
                    }
                }
            }
        };

        let mut socket = match connection {
            Ok(socket) => socket,
            Err(error) => {
                let _ = events
                    .send(NetworkEvent::Disconnected {
                        reason: format!("{error:#}"),
                    })
                    .await;
                let sleep = tokio::time::sleep(backoff);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        _ = &mut sleep => break,
                        command = commands.recv() => match command {
                            Some(NetworkCommand::Shutdown) | None => break 'reconnect,
                            Some(NetworkCommand::Reconnect { .. }) => break,
                            Some(NetworkCommand::Send { .. }) => {
                                let _ = events.send(NetworkEvent::SendRejected {
                                    reason: "not connected; input was not queued".into(),
                                }).await;
                            }
                        }
                    }
                }
                backoff = (backoff * 2).min(Duration::from_secs(5));
                continue;
            }
        };

        generation = generation.wrapping_add(1).max(1);
        backoff = Duration::from_millis(250);
        if events
            .send(NetworkEvent::TransportConnected { generation })
            .await
            .is_err()
        {
            break;
        }

        let hello = ClientMessage::Hello {
            request_id: Uuid::new_v4(),
            protocol_version: PROTOCOL_VERSION,
            client_name: "serialctl".into(),
            actor_kind: ActorKind::Human,
        };
        if let Err(error) = send_control(&mut socket, &hello).await {
            let _ = events
                .send(NetworkEvent::Disconnected {
                    reason: format!("WebSocket hello failed: {error:#}"),
                })
                .await;
            continue;
        }
        let attach_request_id = Uuid::new_v4();
        let attach = ClientMessage::Attach {
            request_id: attach_request_id,
            subscriptions: slots
                .iter()
                .map(|slot_id| Subscription {
                    slot_id: slot_id.clone(),
                    cursor: cursors.get(slot_id).cloned(),
                    tail_events: 500,
                })
                .collect(),
        };
        if let Err(error) = send_control(&mut socket, &attach).await {
            let _ = events
                .send(NetworkEvent::Disconnected {
                    reason: format!("WebSocket attach failed: {error:#}"),
                })
                .await;
            continue;
        }

        let mut ping = tokio::time::interval(Duration::from_secs(10));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await;

        let disconnect_reason = loop {
            tokio::select! {
                incoming = socket.next() => {
                    match incoming {
                        Some(Ok(Message::Binary(bytes))) => match decode_wire_frame(&bytes) {
                            Ok(frame) => {
                                let reconnect = reconnect_directive(&frame, attach_request_id);
                                update_cursors(&frame, &mut cursors, &mut slot_epochs);
                                if events
                                    .send(NetworkEvent::Frame(Box::new(frame)))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                if let Some(directive) = reconnect {
                                    let reason = match directive {
                                        ReconnectDirective::PreserveCursors(reason) => reason,
                                        ReconnectDirective::ResetCursors(reason) => {
                                            cursors.clear();
                                            slot_epochs.clear();
                                            reason
                                        }
                                    };
                                    break reason;
                                }
                            }
                            Err(error) => break format!("invalid protocol frame: {error}"),
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(error) = socket.send(Message::Pong(payload)).await {
                                break format!("WebSocket pong failed: {error}");
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            break frame
                                .map(|frame| format!("server closed connection: {}", frame.reason))
                                .unwrap_or_else(|| "server closed connection".into());
                        }
                        Some(Ok(Message::Text(_))) => {
                            break "server sent an unsupported text WebSocket frame".into();
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => break format!("WebSocket receive failed: {error}"),
                        None => break "WebSocket stream ended".into(),
                    }
                }
                command = commands.recv() => {
                    match command {
                        Some(NetworkCommand::Shutdown) | None => {
                            let _ = socket.close(None).await;
                            return;
                        }
                        Some(NetworkCommand::Reconnect { reason }) => {
                            let _ = socket.close(None).await;
                            break reason;
                        }
                        Some(NetworkCommand::Send { generation: expected, message }) => {
                            if expected != generation {
                                let _ = events.send(NetworkEvent::SendRejected {
                                    reason: "connection changed; stale input was not sent".into(),
                                }).await;
                            } else if let Err(error) = send_control(&mut socket, &message).await {
                                break format!("WebSocket send failed: {error:#}");
                            }
                        }
                    }
                }
                _ = ping.tick() => {
                    let message = ClientMessage::Ping { request_id: Uuid::new_v4() };
                    if let Err(error) = send_control(&mut socket, &message).await {
                        break format!("WebSocket heartbeat failed: {error:#}");
                    }
                }
            }
        };

        while let Ok(command) = commands.try_recv() {
            match command {
                NetworkCommand::Shutdown => return,
                NetworkCommand::Reconnect { .. } => {}
                NetworkCommand::Send { .. } => {
                    let _ = events
                        .send(NetworkEvent::SendRejected {
                            reason: "connection dropped; pending input was not sent".into(),
                        })
                        .await;
                }
            }
        }
        let _ = events
            .send(NetworkEvent::Disconnected {
                reason: disconnect_reason,
            })
            .await;
    }
}

fn reconnect_directive(frame: &WireFrame, attach_request_id: Uuid) -> Option<ReconnectDirective> {
    match frame {
        WireFrame::Control(ServerMessage::Lagged {
            slot_id,
            from_seq,
            to_seq,
        }) => Some(ReconnectDirective::PreserveCursors(format!(
            "{slot_id} lagged at sequences {from_seq}..={to_seq}; reconnecting all Slots"
        ))),
        WireFrame::Control(ServerMessage::Error {
            request_id: Some(request_id),
            code: ErrorCode::CursorAhead,
            message,
            ..
        }) if *request_id == attach_request_id => Some(ReconnectDirective::ResetCursors(format!(
            "attach cursor was ahead of the daemon ({message}); retrying from an authoritative snapshot"
        ))),
        _ => None,
    }
}

async fn connect(
    endpoint: &str,
    token: Option<&str>,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let base = crate::api::normalize_endpoint(endpoint)?;
    let rest = base
        .strip_prefix("http://")
        .expect("normalized v1 endpoint always uses http");
    let ws_base = format!("ws://{rest}");
    let mut request = format!("{ws_base}/api/v1/ws")
        .into_client_request()
        .context("invalid seriald WebSocket URL")?;
    if let Some(token) = token {
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("token contains invalid HTTP header characters")?,
        );
    }
    let (socket, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
        .await
        .context("WebSocket connection timed out")??;
    Ok(socket)
}

async fn send_control<S>(socket: &mut S, message: &ClientMessage) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let bytes = encode_client_control(message)?;
    socket.send(Message::Binary(bytes.into())).await?;
    Ok(())
}

fn update_cursors(
    frame: &WireFrame,
    cursors: &mut HashMap<String, Cursor>,
    slot_epochs: &mut HashMap<String, uuid::Uuid>,
) {
    match frame {
        WireFrame::Rx(header, _) | WireFrame::Tx(header, _) => {
            advance_cursor(cursors, &header.slot_id, header.daemon_epoch, header.seq);
        }
        WireFrame::Control(ServerMessage::Timeline { event, .. }) => {
            advance_cursor(cursors, &event.slot_id, event.daemon_epoch, event.seq);
        }
        WireFrame::Control(ServerMessage::Snapshot { slot }) => {
            slot_epochs.insert(slot.config.id.clone(), slot.daemon_epoch);
        }
        WireFrame::Control(ServerMessage::Ready { slot_id, head_seq }) => {
            if let Some(epoch) = slot_epochs.get(slot_id).copied() {
                advance_cursor(cursors, slot_id, epoch, *head_seq);
            }
        }
        _ => {}
    }
}

fn advance_cursor(
    cursors: &mut HashMap<String, Cursor>,
    slot_id: &str,
    epoch: uuid::Uuid,
    seq: u64,
) {
    match cursors.get_mut(slot_id) {
        Some(cursor) if cursor.epoch == epoch => cursor.after_seq = cursor.after_seq.max(seq),
        Some(cursor) => {
            *cursor = Cursor {
                epoch,
                after_seq: seq,
            }
        }
        None => {
            cursors.insert(
                slot_id.to_string(),
                Cursor {
                    epoch,
                    after_seq: seq,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lagged_forces_reconnect_without_discarding_the_last_received_cursor() {
        let frame = WireFrame::Control(ServerMessage::Lagged {
            slot_id: "slot-1".into(),
            from_seq: 11,
            to_seq: 20,
        });
        assert!(matches!(
            reconnect_directive(&frame, Uuid::new_v4()),
            Some(ReconnectDirective::PreserveCursors(_))
        ));
    }

    #[test]
    fn cursor_ahead_on_the_current_attach_resets_cursors_before_retrying() {
        let attach_request_id = Uuid::new_v4();
        let frame = WireFrame::Control(ServerMessage::Error {
            request_id: Some(attach_request_id),
            code: ErrorCode::CursorAhead,
            message: "ahead".into(),
            retryable: false,
        });
        assert!(matches!(
            reconnect_directive(&frame, attach_request_id),
            Some(ReconnectDirective::ResetCursors(_))
        ));
    }
}
