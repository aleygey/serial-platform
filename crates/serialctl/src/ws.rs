use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serial_protocol::{
    ActorKind, ClientMessage, Cursor, ErrorCode, PROTOCOL_VERSION, ServerMessage, Subscription,
    WireFrame, decode_wire_frame, encode_client_control,
};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

#[derive(Debug)]
// `Send` is the hot-path variant and this channel is already bounded. Boxing
// every control message would add an allocation without reducing retained
// queue growth.
#[allow(clippy::large_enum_variant)]
pub enum NetworkCommand {
    Send {
        generation: u64,
        message: ClientMessage,
    },
    /// Close the current actor connection and immediately reconnect. Directed
    /// queued-control cancellation now uses `CancelAcquire`; retain this
    /// transport-level recovery hook for future connection-wide faults.
    #[allow(dead_code)]
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

pub fn spawn(
    endpoint: String,
    ports: Vec<String>,
    initial_cursors: HashMap<String, Cursor>,
) -> NetworkHandle {
    // Writes and control RPCs are bounded too: a stalled connection must not
    // accumulate arbitrary RAW keystrokes or a large paste in memory.
    let (command_tx, command_rx) = mpsc::channel(256);
    // A slow terminal must not create an unbounded client-side queue. Once
    // this fills, WebSocket backpressure lets seriald apply its per-consumer
    // lag policy without ever blocking the physical serial reader.
    let (event_tx, event_rx) = mpsc::channel(1_024);
    tokio::spawn(run_worker(
        endpoint,
        ports,
        initial_cursors,
        command_rx,
        event_tx,
    ));
    NetworkHandle {
        commands: command_tx,
        events: event_rx,
    }
}

async fn run_worker(
    endpoint: String,
    ports: Vec<String>,
    initial_cursors: HashMap<String, Cursor>,
    mut commands: mpsc::Receiver<NetworkCommand>,
    events: mpsc::Sender<NetworkEvent>,
) {
    // Startup journal recovery supplies only the last sequence it actually
    // scanned. Beginning replay there lets the in-memory ring close a pending
    // durability tail without duplicating the recovered prefix.
    let mut cursors = initial_cursors;
    let mut slot_epochs = HashMap::new();
    let mut generation = 0u64;
    let mut backoff = Duration::from_millis(250);

    'reconnect: loop {
        let connection = tokio::select! {
            result = connect(&endpoint) => result,
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
            subscriptions: build_subscriptions(&ports, &cursors),
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

fn build_subscriptions(ports: &[String], cursors: &HashMap<String, Cursor>) -> Vec<Subscription> {
    ports
        .iter()
        .map(|port| Subscription {
            port: port.clone(),
            cursor: cursors.get(port).cloned(),
            tail_events: 500,
        })
        .collect()
}

fn reconnect_directive(frame: &WireFrame, attach_request_id: Uuid) -> Option<ReconnectDirective> {
    match frame {
        WireFrame::Control(ServerMessage::Lagged {
            port,
            from_seq,
            to_seq,
        }) => Some(ReconnectDirective::PreserveCursors(format!(
            "{port} lagged at sequences {from_seq}..={to_seq}; reconnecting all Ports"
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
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let base = crate::api::normalize_endpoint(endpoint)?;
    let rest = base
        .strip_prefix("http://")
        .expect("normalized v1 endpoint always uses http");
    let ws_base = format!("ws://{rest}");
    let request = format!("{ws_base}/api/v1/ws")
        .into_client_request()
        .context("invalid seriald WebSocket URL")?;
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
            advance_cursor(cursors, &header.port, header.daemon_epoch, header.seq);
        }
        WireFrame::Control(ServerMessage::Timeline { event, .. }) => {
            advance_cursor(cursors, &event.port, event.daemon_epoch, event.seq);
        }
        WireFrame::Control(ServerMessage::Snapshot { port: slot }) => {
            slot_epochs.insert(slot.config.port.clone(), slot.daemon_epoch);
        }
        WireFrame::Control(ServerMessage::Ready { port, head_seq }) => {
            if let Some(epoch) = slot_epochs.get(port).copied() {
                advance_cursor(cursors, port, epoch, *head_seq);
            }
        }
        _ => {}
    }
}

fn advance_cursor(cursors: &mut HashMap<String, Cursor>, port: &str, epoch: uuid::Uuid, seq: u64) {
    match cursors.get_mut(port) {
        Some(cursor) if cursor.epoch == epoch => cursor.after_seq = cursor.after_seq.max(seq),
        Some(cursor) => {
            *cursor = Cursor {
                epoch,
                after_seq: seq,
            }
        }
        None => {
            cursors.insert(
                port.to_string(),
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
            port: "COM4".into(),
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

    #[test]
    fn startup_subscription_resumes_at_the_scanned_journal_cursor_not_live_head() {
        let epoch = Uuid::new_v4();
        let ports = vec!["COM4".to_string()];
        let cursors = HashMap::from([(
            "COM4".to_string(),
            Cursor {
                epoch,
                after_seq: 10,
            },
        )]);

        // The status snapshot may already report #12 while #11/#12 are still
        // awaiting journal acknowledgement. Only K=#10 is supplied here, so
        // seriald's ring remains responsible for replaying that live tail.
        let subscriptions = build_subscriptions(&ports, &cursors);

        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].cursor.as_ref().unwrap().after_seq, 10);
        assert_eq!(subscriptions[0].cursor.as_ref().unwrap().epoch, epoch);
    }

    #[test]
    fn startup_subscription_without_a_verified_cursor_uses_tail_attach() {
        let subscriptions = build_subscriptions(&["COM4".into()], &HashMap::new());

        assert!(subscriptions[0].cursor.is_none());
        assert_eq!(subscriptions[0].tail_events, 500);
    }
}
