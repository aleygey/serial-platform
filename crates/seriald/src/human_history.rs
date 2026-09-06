//! Persistent suggestions derived only from confirmed Human LINE submissions.
use crate::config::atomic_write;
use serde::{Deserialize, Serialize};
use serial_protocol::*;
use std::{
    collections::VecDeque,
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

const MAX_ENTRIES: usize = 10_000;
const MAX_SEEN: usize = 32_768;

#[derive(Clone)]
pub(crate) struct HumanHistory {
    path: PathBuf,
    state: Arc<Mutex<State>>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
struct Persisted {
    revision: u64,
    entries: Vec<HumanCommandHistoryEntry>,
    seen: VecDeque<Uuid>,
    #[serde(default)]
    truncated: bool,
}
#[derive(Default)]
struct State {
    data: Persisted,
    warning: Option<String>,
}
impl HumanHistory {
    pub(crate) fn open(path: PathBuf) -> io::Result<Self> {
        let data = match fs::symlink_metadata(&path) {
            Ok(meta) => {
                if !meta.file_type().is_file() || meta.len() > 48 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid history file",
                    ));
                }
                let value: Persisted = serde_json::from_slice(&fs::read(&path)?)?;
                if value.entries.len() > MAX_ENTRIES
                    || value.seen.len() > MAX_SEEN
                    || value.entries.iter().any(|e| {
                        e.command.is_empty()
                            || e.command.len() > 4096
                            || e.command.chars().any(char::is_control)
                            || e.revision > value.revision
                    })
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid history entries",
                    ));
                }
                value
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Persisted::default(),
            Err(e) => return Err(e),
        };
        Ok(Self {
            path,
            state: Arc::new(Mutex::new(State {
                data,
                warning: None,
            })),
        })
    }

    /// Called on the journal worker, never on the physical serial writer.
    pub(crate) fn record(&self, event: &TimelineEvent) -> io::Result<()> {
        if event.kind != EventKind::Tx
            || event.direction != Direction::Tx
            || event
                .actor
                .as_ref()
                .is_none_or(|a| a.kind != ActorKind::Human)
            || event
                .metadata
                .get("partial")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        {
            return Ok(());
        }
        let Some(command) = event
            .metadata
            .get("human_line_input")
            .and_then(serde_json::Value::as_str)
        else {
            return Ok(());
        };
        if command.is_empty() || command.len() > 4096 || command.chars().any(char::is_control) {
            return Ok(());
        }
        let Some(operation_id) = event.operation_id else {
            return Ok(());
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.data.seen.contains(&operation_id) {
            return Ok(());
        }
        let mut next = state.data.clone();
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("history revision exhausted"))?;
        let previous = next
            .entries
            .iter()
            .position(|e| e.command == command)
            .map(|i| next.entries.remove(i));
        next.entries.push(HumanCommandHistoryEntry {
            id: previous.as_ref().map_or(operation_id, |e| e.id),
            command: command.to_owned(),
            port: event.port.clone(),
            wall_time_ns: event.wall_time_ns,
            revision: next.revision,
            uses: previous.map_or(1, |e| e.uses.saturating_add(1)),
        });
        if next.entries.len() > MAX_ENTRIES {
            next.entries.remove(0);
            next.truncated = true;
        }
        next.seen.push_back(operation_id);
        if next.seen.len() > MAX_SEEN {
            next.seen.pop_front();
        }
        match atomic_write(&self.path, &serde_json::to_vec(&next)?) {
            Ok(()) => {
                state.data = next;
                state.warning = None;
                Ok(())
            }
            Err(e) => {
                state.warning = Some(format!(
                    "Human history save failed; command may already have executed: {e}"
                ));
                Err(e)
            }
        }
    }

    pub(crate) fn query(
        &self,
        server_id: Uuid,
        query: &HumanCommandHistoryQuery,
    ) -> HumanCommandHistoryResponse {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut matching = state.data.entries.iter().rev().filter(|e| {
            query.before_revision.is_none_or(|r| e.revision < r)
                && query
                    .prefix
                    .as_ref()
                    .is_none_or(|p| e.command.starts_with(p))
                && query
                    .contains
                    .as_ref()
                    .is_none_or(|p| e.command.contains(p))
        });
        let entries: Vec<_> = matching
            .by_ref()
            .take(query.limit.unwrap_or(1000).clamp(1, 2000))
            .cloned()
            .collect();
        let next_before_revision = matching
            .next()
            .and_then(|_| entries.last().map(|e| e.revision));
        HumanCommandHistoryResponse {
            server_id, revision: state.data.revision, entries, next_before_revision,
            warning: state.warning.clone().or_else(|| state.data.truncated.then(|| {
                format!("Suggestions retain the latest {MAX_ENTRIES} distinct Human commands; older TX audit remains in retained journals")
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn tx(command: &str, kind: ActorKind) -> TimelineEvent {
        TimelineEvent {
            port: "A".into(),
            daemon_epoch: Uuid::new_v4(),
            seq: 1,
            generation: 1,
            wall_time_ns: 1,
            monotonic_time_ns: 1,
            kind: EventKind::Tx,
            direction: Direction::Tx,
            actor: Some(Actor {
                id: "h".into(),
                label: "human".into(),
                kind,
            }),
            run_id: None,
            operation_id: Some(Uuid::new_v4()),
            stream_offset_start: Some(0),
            stream_offset_end: Some(command.len() as u64 + 1),
            data: format!("{command}\r").into_bytes(),
            metadata: std::collections::BTreeMap::from([(
                "human_line_input".into(),
                command.into(),
            )]),
            durable: true,
        }
    }
    #[test]
    fn persistent_manual_only_shared_and_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.json");
        let history = HumanHistory::open(path.clone()).unwrap();
        let event = tx("help", ActorKind::Human);
        history.record(&event).unwrap();
        history.record(&event).unwrap();
        history.record(&tx("agent-only", ActorKind::Agent)).unwrap();
        let mut raw = tx("raw", ActorKind::Human);
        raw.metadata.clear();
        history.record(&raw).unwrap();
        let mut second = tx("help", ActorKind::Human);
        second.port = "B".into();
        history.record(&second).unwrap();
        let result = HumanHistory::open(path)
            .unwrap()
            .query(Uuid::nil(), &Default::default());
        assert_eq!(result.revision, 2);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].port, "B");
        assert_eq!(result.entries[0].uses, 2);
    }
    #[test]
    fn partial_and_control_bytes_are_not_suggestions() {
        let dir = tempfile::tempdir().unwrap();
        let history = HumanHistory::open(dir.path().join("history.json")).unwrap();
        history.record(&tx("\u{4}", ActorKind::Human)).unwrap();
        let mut partial = tx("help", ActorKind::Human);
        partial.metadata.insert("partial".into(), true.into());
        history.record(&partial).unwrap();
        assert!(
            history
                .query(Uuid::nil(), &Default::default())
                .entries
                .is_empty()
        );
    }
}
