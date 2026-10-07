//! Persistent Agent panel index. Hiding a Run never changes journal evidence.
use crate::config::atomic_write;
use serde::Serialize;
use serial_protocol::{ActorKind, EventKind, RunInfo, RunStatus, TimelineEvent};
use serial_protocol::{AgentHistoryVisibility as RunVisibility, AgentRunRecord as RunRecord};
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

#[derive(Clone)]
pub struct RunHistory {
    root: PathBuf,
    lock: Arc<Mutex<()>>,
    pub(crate) indexing: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) warning: Arc<Mutex<Option<String>>>,
}

impl RunHistory {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            lock: Arc::new(Mutex::new(())),
            indexing: Default::default(),
            warning: Default::default(),
        }
    }

    fn port_dir(&self, port: &str) -> PathBuf {
        // File paths never contain untrusted device names or path separators.
        self.root.join(
            port.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        )
    }

    pub fn record(&self, event: &TimelineEvent) -> io::Result<()> {
        if !matches!(
            event.kind,
            EventKind::RunStarted | EventKind::RunEnded | EventKind::RunAborted
        ) {
            return Ok(());
        }
        let Some(run) = event
            .metadata
            .get("run")
            .and_then(|value| serde_json::from_value::<RunInfo>(value.clone()).ok())
        else {
            return Ok(());
        };
        if run.owner.kind != ActorKind::Agent {
            return Ok(());
        }
        let _lock = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let directory = self.port_dir(&event.port);
        fs::create_dir_all(&directory)?;
        let path = directory.join(format!("{}.json", run.id));
        let old: Option<RunRecord> = read_optional(&path)?;
        if old
            .as_ref()
            .is_some_and(|old| old.epoch == event.daemon_epoch && old.through_seq >= event.seq)
        {
            return Ok(());
        }
        let record = RunRecord {
            port: event.port.clone(),
            epoch: event.daemon_epoch,
            started_wall_time_ns: old
                .as_ref()
                .map_or(event.wall_time_ns, |old| old.started_wall_time_ns),
            through_seq: event.seq,
            hidden: old.as_ref().is_some_and(|old| old.hidden),
            run,
        };
        write_json(&path, &record)
    }

    pub fn list(&self, port: &str, current_epoch: Uuid) -> io::Result<Vec<RunRecord>> {
        // Each record is replaced atomically. Reading a large catalog must not
        // hold the writer lock: otherwise opening the panel could delay a Run
        // lifecycle append on the serial journal thread. A polling snapshot
        // may miss a newly-created record, which the next refresh supplies.
        let entries = match fs::read_dir(self.port_dir(port)) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let mut records = Vec::new();
        for entry in entries {
            let entry = entry?;
            let Some(id) = entry
                .path()
                .file_stem()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse::<Uuid>().ok())
            else {
                continue;
            };
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(mut record) = read_optional::<RunRecord>(&entry.path())? else {
                continue;
            };
            if record.port != port || record.run.id != id {
                return Err(io::Error::other("Agent history identity mismatch"));
            }
            // A previous daemon cannot still own physical control after restart.
            if record.epoch != current_epoch && record.run.status == RunStatus::Active {
                record.run.status = RunStatus::Aborted;
            }
            records.push(record);
            if records.len() > 100_000 {
                return Err(io::Error::other(
                    "Agent history catalog exceeds safe scan bound",
                ));
            }
        }
        records.sort_by_key(|record| (record.started_wall_time_ns, record.run.id));
        Ok(records)
    }

    pub fn visibility(&self, port: &str) -> io::Result<RunVisibility> {
        let _lock = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        Ok(read_optional(&self.port_dir(port).join("visibility.json"))?.unwrap_or_default())
    }

    /// Validates the entire selection before one atomic visibility commit.
    /// The index and raw events themselves remain intact and independently readable.
    pub fn hide(
        &self,
        port: &str,
        ids: Option<&[Uuid]>,
        current_epoch: Uuid,
        active: Option<Uuid>,
    ) -> io::Result<RunVisibility> {
        if ids.is_none()
            && (self.indexing.load(std::sync::atomic::Ordering::Acquire)
                || self
                    .warning
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "历史索引尚未完整恢复，暂不能清空全部历史；可选择整轮 Run 清理",
            ));
        }
        let records = self.list(port, current_epoch)?;
        let known = records
            .iter()
            .map(|record| (record.run.id, record))
            .collect::<BTreeMap<_, _>>();
        let selected = ids.map_or_else(
            || {
                records
                    .iter()
                    .filter(|record| {
                        record.run.status != RunStatus::Active && Some(record.run.id) != active
                    })
                    .map(|record| record.run.id)
                    .collect()
            },
            |ids| ids.to_vec(),
        );
        for id in &selected {
            if Some(*id) == active
                || known
                    .get(id)
                    .is_some_and(|record| record.run.status == RunStatus::Active)
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "正在执行的 Run 不能清理",
                ));
            }
            if !known.contains_key(id) {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "Run 尚未归档，未清理任何记录",
                ));
            }
        }
        // Run IDs are never reused; a terminal Run cannot become active again.
        // Only the small visibility commit needs serialization. The caller's
        // authoritative active ID is protected in addition to the index state.
        let _lock = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        let directory = self.port_dir(port);
        fs::create_dir_all(&directory)?;
        let path = directory.join("visibility.json");
        let mut visibility: RunVisibility = read_optional(&path)?.unwrap_or_default();
        visibility.hidden.extend(selected);
        visibility.hidden.sort_unstable();
        visibility.hidden.dedup();
        visibility.revision = visibility
            .revision
            .checked_add(1)
            .ok_or_else(|| io::Error::other("history revision exhausted"))?;
        write_json(&path, &visibility)?;
        Ok(visibility)
    }
}

fn read_optional<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() > 8 * 1024 * 1024 {
        return Err(io::Error::other("invalid Agent history index file"));
    }
    serde_json::from_slice(&fs::read(path)?)
        .map(Some)
        .map_err(io::Error::other)
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    atomic_write(path, &serde_json::to_vec(value).map_err(io::Error::other)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{Actor, Direction};

    fn lifecycle(id: Uuid, epoch: Uuid, seq: u64, status: RunStatus) -> TimelineEvent {
        let run = RunInfo {
            id,
            owner: Actor {
                id: "agent".into(),
                label: "Agent".into(),
                kind: ActorKind::Agent,
            },
            label: "test".into(),
            status,
            start_seq: 1,
            end_seq: (status != RunStatus::Active).then_some(seq),
            metadata: Default::default(),
        };
        TimelineEvent {
            port: "COM3".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind: if status == RunStatus::Active {
                EventKind::RunStarted
            } else {
                EventKind::RunEnded
            },
            direction: Direction::None,
            actor: Some(run.owner.clone()),
            run_id: Some(id),
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: Vec::new(),
            durable: true,
            metadata: [("run".into(), serde_json::to_value(run).unwrap())].into(),
        }
    }

    #[test]
    fn cleanup_is_whole_run_persistent_and_never_removes_the_index_or_live_run() {
        let root = tempfile::tempdir().unwrap();
        let history = RunHistory::new(root.path().to_owned());
        let epoch = Uuid::new_v4();
        let done = Uuid::new_v4();
        let active = Uuid::new_v4();
        history
            .record(&lifecycle(done, epoch, 1, RunStatus::Active))
            .unwrap();
        history
            .record(&lifecycle(done, epoch, 3, RunStatus::Completed))
            .unwrap();
        history
            .record(&lifecycle(active, epoch, 4, RunStatus::Active))
            .unwrap();
        assert!(
            history
                .hide("COM3", Some(&[done, active]), epoch, Some(active))
                .is_err()
        );
        assert!(
            history.visibility("COM3").unwrap().hidden.is_empty(),
            "selection validation is atomic"
        );
        history.hide("COM3", None, epoch, Some(active)).unwrap();
        let reopened = RunHistory::new(root.path().to_owned());
        assert_eq!(reopened.visibility("COM3").unwrap().hidden, vec![done]);
        assert_eq!(
            reopened.list("COM3", epoch).unwrap().len(),
            2,
            "raw index records are retained"
        );
        assert!(reopened.visibility("COM4").unwrap().hidden.is_empty());
        history
            .record(&lifecycle(done, epoch, 1, RunStatus::Active))
            .unwrap();
        assert_eq!(
            history.list("COM3", epoch).unwrap()[0].run.status,
            RunStatus::Completed,
            "backfill cannot resurrect an ended run"
        );
    }

    #[test]
    fn old_daemon_active_run_becomes_aborted_but_current_live_run_remains_protected() {
        let root = tempfile::tempdir().unwrap();
        let history = RunHistory::new(root.path().to_owned());
        let old_epoch = Uuid::new_v4();
        let new_epoch = Uuid::new_v4();
        let id = Uuid::new_v4();
        history
            .record(&lifecycle(id, old_epoch, 1, RunStatus::Active))
            .unwrap();
        assert!(history.hide("COM3", Some(&[id]), old_epoch, None).is_err());
        assert_eq!(
            history.list("COM3", new_epoch).unwrap()[0].run.status,
            RunStatus::Aborted
        );
        history.hide("COM3", Some(&[id]), new_epoch, None).unwrap();
    }
}
