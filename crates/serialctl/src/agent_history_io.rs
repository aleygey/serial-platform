use crate::api::ApiClient;
use serial_protocol::{
    AgentHistoryResponse, AgentHistoryVisibility, AgentRunRecord, EventKind, EventQuery,
    TimelineEvent,
};
use tokio::sync::mpsc;
use uuid::Uuid;

pub enum Command {
    Clear {
        port: String,
        ids: Option<Vec<Uuid>>,
    },
    Load(AgentRunRecord),
}

pub enum Event {
    Snapshot {
        port: String,
        history: AgentHistoryResponse,
    },
    Cleared {
        port: String,
        visibility: AgentHistoryVisibility,
    },
    Loaded {
        port: String,
        id: Uuid,
        events: Vec<TimelineEvent>,
        limited: bool,
    },
    Failed(String),
}

pub struct Io {
    pub commands: mpsc::Sender<Command>,
    pub events: mpsc::Receiver<Event>,
}

pub fn spawn(api: ApiClient) -> Io {
    let (commands, mut requests) = mpsc::channel(8);
    let (output, events) = mpsc::channel(8);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = requests.recv() => {
                    let Some(command) = command else { break; };
                    let result = match command {
                        Command::Clear { port, ids } => api.clear_agent_history(&port, ids).await.map(|visibility| Event::Cleared { port, visibility }),
                        Command::Load(record) => load(&api, record).await,
                    };
                    if output.send(result.unwrap_or_else(|error| Event::Failed(error.to_string()))).await.is_err() { break; }
                }
                _ = tick.tick() => {
                    let Ok(status) = api.status().await else { continue; };
                    for slot in status.ports {
                        let port = slot.config.port;
                        match api.agent_history(&port).await {
                            Ok(history) => if output.send(Event::Snapshot { port, history }).await.is_err() { return; },
                            Err(error) => tracing::warn!(%error, "Agent history refresh failed"),
                        }
                    }
                }
            }
        }
    });
    Io { commands, events }
}

async fn load(api: &ApiClient, record: AgentRunRecord) -> anyhow::Result<Event> {
    let mut events = Vec::new();
    let mut limited = false;
    for kind in [EventKind::Tx, EventKind::CommandCaptureCompleted] {
        let response = api
            .events(
                &record.port,
                &EventQuery {
                    epoch: Some(record.epoch),
                    after_seq: Some(record.run.start_seq.saturating_sub(1)),
                    through_seq: record.run.end_seq,
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: None,
                    kind: Some(kind),
                    actor_id: None,
                    run_id: Some(record.run.id),
                    operation_id: None,
                    contains: None,
                    regex: None,
                    limit_events: Some(1024),
                    limit_bytes: Some(2 * 1024 * 1024),
                },
            )
            .await?;
        limited |= response.truncated || !response.gaps.is_empty();
        events.extend(response.events);
    }
    events.sort_by_key(|event| event.seq);
    Ok(Event::Loaded {
        port: record.port,
        id: record.run.id,
        events,
        limited,
    })
}
