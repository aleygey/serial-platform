//! Disk-backed search over the exact text batches accepted by the TUI parser.
//! Completed rows are immutable. The one pending terminal row is replaced,
//! never appended on every RX chunk. Query workers scan a fixed head, then can
//! extend that head without rescanning old rows or retaining all hits in RAM.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak, mpsc};
use std::thread;

use ratatui::style::{Color, Modifier, Style};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use serial_protocol::EventKind;
use uuid::Uuid;

use crate::display::{DisplayLine, RunBoundary, StreamDisplayBatch, gap_line};

const QUEUE_MESSAGES: usize = 256;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_ROW_BYTES: usize = 256 * 1024;
const MAX_QUERY_BYTES: usize = 4096;
const MAX_PAGE: usize = 256;
const MAX_CONTEXT: usize = 512;
const MATCH_BYTES: u64 = 32;

#[derive(Debug, Clone)]
pub struct SearchQuery {
    pub query: String,
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SearchProgress {
    pub revision: u64,
    pub scanned_rows: u64,
    pub total_rows: u64,
    pub matches: u64,
    pub complete: bool,
    pub cancelled: bool,
    pub error: Option<String>,
    /// Nonempty means coverage is incomplete, even if the available scan ended.
    pub gaps: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SearchHit {
    pub ordinal: u64,
    pub row_id: u64,
    pub byte_start: usize,
    pub end_row_id: u64,
    pub byte_end: usize,
    pub preview: String,
    pub revision: u64,
    pub provisional: bool,
}

#[derive(Debug, Clone)]
pub struct ArchivedLine {
    pub row_id: u64,
    pub line: DisplayLine,
}

#[derive(Debug)]
struct Directory {
    path: PathBuf,
    preserve: AtomicBool,
}
impl Drop for Directory {
    fn drop(&mut self) {
        // This path was atomically created here and is owned only by this
        // archive. Never clean the shared temp root or a user-supplied path.
        if !self.preserve.load(Ordering::Acquire)
            && let Err(error) = fs::remove_dir_all(&self.path)
        {
            tracing::warn!(path = %self.path.display(), %error, "session search archive cleanup failed");
        }
    }
}

#[derive(Debug)]
struct ArchiveShared {
    directory: Arc<Directory>,
    revision: AtomicU64,
    queued_bytes: AtomicUsize,
    interrupted_stream: AtomicBool,
    gaps: Mutex<VecDeque<String>>,
    queries: AtomicUsize,
}
impl ArchiveShared {
    fn gap(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let mut gaps = self.gaps.lock().unwrap();
        if gaps.back() != Some(&reason) {
            if gaps.len() == 64 {
                gaps.pop_front();
            }
            gaps.push_back(reason);
        }
    }
    fn fault(&self, reason: impl Into<String>) {
        self.gap(reason);
        self.directory.preserve.store(true, Ordering::Release);
        self.interrupted_stream.store(true, Ordering::Release);
    }
}

#[derive(Debug)]
enum Message {
    Append {
        completed: Vec<DisplayLine>,
        pending: Option<DisplayLine>,
        pending_committed: bool,
        boundary: bool,
        bytes: usize,
    },
    Gap(String),
    Boundary,
    Snapshot(Weak<QueryState>, u64),
}

#[derive(Debug, Clone)]
pub struct SessionArchive {
    shared: Arc<ArchiveShared>,
    sender: mpsc::SyncSender<Message>,
}

impl SessionArchive {
    pub fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!("serial-platform-session-{}", Uuid::new_v4()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path)?;
        let directory = Arc::new(Directory {
            path,
            preserve: AtomicBool::new(false),
        });
        let data = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(directory.path.join("rows.data"))?;
        let index = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(directory.path.join("rows.index"))?;
        let shared = Arc::new(ArchiveShared {
            directory,
            revision: AtomicU64::new(0),
            queued_bytes: AtomicUsize::new(0),
            interrupted_stream: AtomicBool::new(false),
            gaps: Mutex::new(VecDeque::new()),
            queries: AtomicUsize::new(0),
        });
        let (sender, receiver) = mpsc::sync_channel(QUEUE_MESSAGES);
        let worker_shared = shared.clone();
        thread::Builder::new()
            .name("serial-session-spool".into())
            .spawn(move || archive_worker(receiver, worker_shared, data, index))?;
        Ok(Self { shared, sender })
    }

    pub fn path(&self) -> &Path {
        &self.shared.directory.path
    }
    /// Revision of queued changes. A refresh is queued after those changes.
    pub fn revision(&self) -> u64 {
        self.shared.revision.load(Ordering::Acquire)
    }
    pub fn gaps(&self) -> Vec<String> {
        self.shared.gaps.lock().unwrap().iter().cloned().collect()
    }

    /// Never waits for disk or queue space on the TUI thread. Returns false and
    /// records an explicit coverage gap when backpressure prevents persistence.
    pub fn append(&self, batch: &StreamDisplayBatch) -> bool {
        let bytes =
            batch
                .completed
                .iter()
                .chain(batch.pending.iter())
                .fold(0usize, |total, line| {
                    total
                        .saturating_add(line.text.len())
                        .saturating_add(line.source.len())
                        .saturating_add(256)
                });
        if bytes > QUEUE_BYTES
            || self
                .shared
                .queued_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|total| *total <= QUEUE_BYTES)
                })
                .is_err()
        {
            self.shared.fault("session archive queue byte budget exceeded; some displayed history was not recorded");
            self.shared.revision.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        let boundary = self.shared.interrupted_stream.swap(false, Ordering::AcqRel);
        let message = Message::Append {
            completed: batch.completed.clone(),
            pending: batch.pending.clone(),
            pending_committed: batch.pending_committed,
            boundary,
            bytes,
        };
        if self.sender.try_send(message).is_err() {
            self.shared.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.shared.fault("session archive queue full or disconnected; some displayed history was not recorded");
            self.shared.revision.fetch_add(1, Ordering::AcqRel);
            return false;
        }
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
        true
    }

    /// Call before parser reset or when capture reports a gap. The worker first
    /// commits the old pending row, then inserts a hard continuity boundary.
    pub fn record_gap(&self, reason: impl Into<String>) {
        let reason = reason.into();
        self.shared.gap(reason.clone());
        if self.sender.try_send(Message::Gap(reason)).is_err() {
            self.shared
                .fault("could not persist a session continuity boundary");
        }
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
    }

    /// Seal a fully captured stream before a deliberate parser reset. This
    /// prevents joining text across sessions without claiming history was lost.
    pub fn record_boundary(&self) {
        if self.sender.try_send(Message::Boundary).is_err() {
            self.shared
                .fault("could not persist a session continuity boundary");
        }
        self.shared.revision.fetch_add(1, Ordering::AcqRel);
    }

    pub fn search(&self, query: SearchQuery) -> io::Result<SearchHandle> {
        if query.query.is_empty() || query.query.len() > MAX_QUERY_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "search text must contain 1..=4096 UTF-8 bytes",
            ));
        }
        let regex = RegexBuilder::new(&regex::escape(&query.query))
            .case_insensitive(!query.case_sensitive)
            .size_limit(1024 * 1024)
            .build()
            .map_err(io::Error::other)?;
        self.shared
            .queries
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                (value < 4).then_some(value + 1)
            })
            .map_err(|_| {
                io::Error::other("too many active session searches; cancel the previous search")
            })?;
        let state = Arc::new(QueryState {
            archive: self.shared.clone(),
            matches_path: self
                .shared
                .directory
                .path
                .join(format!("matches-{}.bin", Uuid::new_v4())),
            cancelled: AtomicBool::new(false),
            input: Mutex::new(None),
            wake: Condvar::new(),
            data: Mutex::new(QueryData::default()),
        });
        let worker_state = state.clone();
        let thread = thread::Builder::new()
            .name("serial-session-search".into())
            .spawn(move || {
                search_worker(
                    worker_state,
                    regex,
                    query
                        .query
                        .chars()
                        .count()
                        .saturating_mul(4)
                        .saturating_add(4),
                )
            });
        if let Err(error) = thread {
            self.shared.queries.fetch_sub(1, Ordering::AcqRel);
            return Err(error);
        }
        let handle = SearchHandle {
            inner: Arc::new(QueryHandle {
                state,
                sender: self.sender.clone(),
            }),
        };
        handle.refresh()?;
        Ok(handle)
    }
}

#[derive(Debug, Clone)]
struct Snapshot {
    rows: u64,
    pending: Option<DisplayLine>,
    revision: u64,
    gaps: Vec<String>,
}

#[derive(Debug, Default)]
struct QueryData {
    progress: SearchProgress,
    requested_revision: u64,
    committed_matches: u64,
    provisional: Vec<HitRecord>,
    snapshot: Option<Snapshot>,
}

#[derive(Debug)]
struct QueryState {
    archive: Arc<ArchiveShared>,
    matches_path: PathBuf,
    cancelled: AtomicBool,
    input: Mutex<Option<Snapshot>>,
    wake: Condvar,
    data: Mutex<QueryData>,
}

#[derive(Debug)]
struct QueryHandle {
    state: Arc<QueryState>,
    sender: mpsc::SyncSender<Message>,
}
impl Drop for QueryHandle {
    fn drop(&mut self) {
        self.state.cancelled.store(true, Ordering::Release);
        self.state.wake.notify_all();
        self.state.archive.queries.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone)]
pub struct SearchHandle {
    inner: Arc<QueryHandle>,
}
impl SearchHandle {
    pub fn cancel(&self) {
        self.inner.state.cancelled.store(true, Ordering::Release);
        self.inner.state.wake.notify_all();
        let mut data = self.inner.state.data.lock().unwrap();
        data.progress.cancelled = true;
        data.progress.complete = false;
    }
    /// Extend to the archive head after currently queued append operations.
    /// Only new completed rows and the replacement pending row are searched.
    pub fn refresh(&self) -> io::Result<()> {
        if self.inner.state.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "search was cancelled",
            ));
        }
        // Hold the publication lock across the nonblocking enqueue so an
        // immediately completed snapshot cannot race our complete=false write.
        // A full queue leaves the previous completed head available for retry.
        let mut data = self.inner.state.data.lock().unwrap();
        let revision = self.inner.state.archive.revision.load(Ordering::Acquire);
        self.inner
            .sender
            .try_send(Message::Snapshot(
                Arc::downgrade(&self.inner.state),
                revision,
            ))
            .map_err(|error| {
                io::Error::new(
                    if matches!(error, mpsc::TrySendError::Full(_)) {
                        io::ErrorKind::WouldBlock
                    } else {
                        io::ErrorKind::BrokenPipe
                    },
                    "session archive is busy; retry refresh",
                )
            })?;
        data.requested_revision = data.requested_revision.max(revision);
        data.progress.complete = false;
        Ok(())
    }
    pub fn progress(&self) -> SearchProgress {
        let mut progress = self.inner.state.data.lock().unwrap().progress.clone();
        progress.gaps = self
            .inner
            .state
            .archive
            .gaps
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();
        progress
    }
    /// Disk I/O: call from a blocking worker, not the TUI event/render thread.
    pub fn page(&self, offset: u64, limit: usize) -> io::Result<Vec<SearchHit>> {
        if self.inner.state.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "search was cancelled",
            ));
        }
        if limit > MAX_PAGE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "search page exceeds 256 matches",
            ));
        }
        let (committed, provisional, snapshot) = {
            let data = self.inner.state.data.lock().unwrap();
            (
                data.committed_matches,
                data.provisional.clone(),
                data.snapshot.clone(),
            )
        };
        let Some(snapshot) = snapshot else {
            return Ok(Vec::new());
        };
        let total = committed.saturating_add(provisional.len() as u64);
        if offset >= total || limit == 0 {
            return Ok(Vec::new());
        }
        let mut file = File::open(&self.inner.state.matches_path)?;
        if offset < committed {
            file.seek(SeekFrom::Start(
                offset
                    .checked_mul(MATCH_BYTES)
                    .ok_or_else(|| io::Error::other("match offset overflow"))?,
            ))?;
        }
        let mut reader = RowReader::open(&self.inner.state.archive.directory.path)?;
        let mut result = Vec::new();
        let mut last_line: Option<(u64, DisplayLine)> = None;
        for ordinal in offset..total.min(offset.saturating_add(limit as u64)) {
            let record = if ordinal < committed {
                HitRecord::read(&mut file)?
            } else {
                provisional[(ordinal - committed) as usize]
            };
            if last_line.as_ref().map(|(row, _)| *row) != Some(record.row_id) {
                let line = read_snapshot_line(&mut reader, &snapshot, record.row_id)?;
                last_line = Some((record.row_id, line));
            }
            let line = &last_line.as_ref().unwrap().1;
            result.push(SearchHit {
                ordinal,
                row_id: record.row_id,
                byte_start: record.byte_start as usize,
                end_row_id: record.end_row_id,
                byte_end: record.byte_end as usize,
                preview: preview(&line.text, record.byte_start as usize),
                revision: snapshot.revision,
                provisional: ordinal >= committed,
            });
        }
        Ok(result)
    }
    /// Reload normalized rows (including styles and old daemon epochs) without
    /// consulting the current in-memory scrollback or a live daemon.
    pub fn context(
        &self,
        hit: &SearchHit,
        before: usize,
        after: usize,
    ) -> io::Result<Vec<ArchivedLine>> {
        let snapshot = self
            .inner
            .state
            .data
            .lock()
            .unwrap()
            .snapshot
            .clone()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::WouldBlock, "search snapshot is not ready")
            })?;
        if hit.provisional && hit.revision != snapshot.revision {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "pending terminal row changed; refresh the selected match",
            ));
        }
        let total = snapshot.rows + u64::from(snapshot.pending.is_some());
        let first = hit.row_id.saturating_sub(before as u64);
        let end = hit
            .end_row_id
            .saturating_add(after as u64)
            .saturating_add(1)
            .min(total);
        if end.saturating_sub(first) > MAX_CONTEXT as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "context exceeds 512 rows",
            ));
        }
        let mut reader = RowReader::open(&self.inner.state.archive.directory.path)?;
        (first..end)
            .map(|row_id| {
                read_snapshot_line(&mut reader, &snapshot, row_id)
                    .map(|line| ArchivedLine { row_id, line })
            })
            .collect()
    }
}

fn archive_worker(
    receiver: mpsc::Receiver<Message>,
    shared: Arc<ArchiveShared>,
    mut data: File,
    mut index: File,
) {
    let mut rows = 0u64;
    let mut pending: Option<DisplayLine> = None;
    let mut break_next_stream = false;
    let mut failed = false;
    while let Ok(message) = receiver.recv() {
        let result: io::Result<()> = (|| {
            match message {
                Message::Append {
                    completed,
                    pending: mut next_pending,
                    pending_committed,
                    boundary,
                    bytes,
                } => {
                    shared.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                    if failed {
                        return Ok(());
                    }
                    let epoch_changed = pending.as_ref().is_some_and(|old| {
                        next_pending
                            .as_ref()
                            .or_else(|| completed.first())
                            .is_some_and(|new| old.daemon_epoch != new.daemon_epoch)
                    });
                    if boundary || epoch_changed {
                        if let Some(line) = pending.take().filter(|_| !pending_committed) {
                            write_row(&mut data, &mut index, &mut rows, &line)?;
                        }
                        write_row(
                            &mut data,
                            &mut index,
                            &mut rows,
                            &gap_line(0, "session stream continuity changed"),
                        )?;
                    }
                    // pending_committed means the parser's completed rows already
                    // contain the old pending text, so it must not be appended twice.
                    if pending_committed {
                        pending = None;
                    }
                    for mut line in completed {
                        if break_next_stream
                            && matches!(line.event_kind, EventKind::Rx | EventKind::Tx)
                        {
                            line.continues_previous = false;
                            break_next_stream = false;
                        }
                        write_row(&mut data, &mut index, &mut rows, &line)?;
                    }
                    if break_next_stream && let Some(line) = next_pending.as_mut() {
                        line.continues_previous = false;
                    }
                    pending = next_pending;
                }
                Message::Boundary => {
                    if failed {
                        return Ok(());
                    }
                    if let Some(line) = pending.take() {
                        write_row(&mut data, &mut index, &mut rows, &line)?;
                    }
                    break_next_stream = true;
                }
                Message::Gap(reason) => {
                    if failed {
                        return Ok(());
                    }
                    if let Some(line) = pending.take() {
                        write_row(&mut data, &mut index, &mut rows, &line)?;
                    }
                    write_row(&mut data, &mut index, &mut rows, &gap_line(0, reason))?;
                }
                Message::Snapshot(query, revision) => {
                    if let Some(query) = query.upgrade() {
                        // Do not stamp this with the global counter: newer
                        // appends may already be queued behind this snapshot.
                        let snapshot = Snapshot {
                            rows,
                            pending: pending.clone(),
                            revision,
                            gaps: shared.gaps.lock().unwrap().iter().cloned().collect(),
                        };
                        *query.input.lock().unwrap() = Some(snapshot);
                        query.wake.notify_one();
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            failed = true;
            shared.fault(format!("session archive write failed: {error}; unavailable history must not be reported as zero matches (archive retained at {})", shared.directory.path.display()));
        }
    }
    // Close spool handles before the final directory owner can clean up on
    // Windows, where open handles otherwise prevent removing their files.
    drop(data);
    drop(index);
}

#[derive(Debug, Clone, Copy)]
struct HitRecord {
    row_id: u64,
    byte_start: u64,
    end_row_id: u64,
    byte_end: u64,
}
impl HitRecord {
    fn write(self, writer: &mut impl Write) -> io::Result<()> {
        for value in [self.row_id, self.byte_start, self.end_row_id, self.byte_end] {
            writer.write_all(&value.to_le_bytes())?;
        }
        Ok(())
    }
    fn read(reader: &mut impl Read) -> io::Result<Self> {
        let mut bytes = [0u8; MATCH_BYTES as usize];
        reader.read_exact(&mut bytes)?;
        let mut values = bytes
            .as_chunks::<8>()
            .0
            .iter()
            .map(|bytes| u64::from_le_bytes(*bytes));
        Ok(Self {
            row_id: values.next().unwrap(),
            byte_start: values.next().unwrap(),
            end_row_id: values.next().unwrap(),
            byte_end: values.next().unwrap(),
        })
    }
}

#[derive(Debug, Clone)]
struct Fragment {
    row_id: u64,
    byte_start: usize,
    text: String,
}
#[derive(Debug, Clone, Default)]
struct MatcherState {
    carry: Vec<Fragment>,
    logical_bytes: u64,
    matched_through: u64,
}
impl MatcherState {
    fn scan(
        &mut self,
        row_id: u64,
        line: &DisplayLine,
        regex: &Regex,
        carry_bytes: usize,
        mut hit: impl FnMut(HitRecord) -> io::Result<()>,
    ) -> io::Result<()> {
        let stream = matches!(line.event_kind, EventKind::Rx | EventKind::Tx);
        if !stream {
            for found in regex.find_iter(&line.text) {
                hit(HitRecord {
                    row_id,
                    byte_start: found.start() as u64,
                    end_row_id: row_id,
                    byte_end: found.end() as u64,
                })?;
            }
            if matches!(
                line.event_kind,
                EventKind::Gap
                    | EventKind::SerialOpening
                    | EventKind::SerialOpened
                    | EventKind::SerialClosed
                    | EventKind::SerialOpenFailed
                    | EventKind::PortRemoved
            ) {
                *self = Self::default();
            }
            return Ok(());
        }
        if !line.continues_previous {
            *self = Self::default();
        }
        let old_bytes = self.logical_bytes;
        let carry_len: usize = self.carry.iter().map(|f| f.text.len()).sum();
        let base = old_bytes.saturating_sub(carry_len as u64);
        let mut fragments = self.carry.clone();
        fragments.push(Fragment {
            row_id,
            byte_start: 0,
            text: line.text.clone(),
        });
        let mut joined = String::with_capacity(carry_len + line.text.len());
        let mut offsets = Vec::with_capacity(fragments.len());
        for fragment in &fragments {
            offsets.push(joined.len());
            joined.push_str(&fragment.text);
        }
        let mut start = usize::try_from(self.matched_through.saturating_sub(base))
            .unwrap_or(joined.len())
            .min(joined.len());
        while !joined.is_char_boundary(start) {
            start += 1;
        }
        for found in regex.find_iter(&joined[start..]) {
            let first = start + found.start();
            let end = start + found.end();
            if base.saturating_add(end as u64) <= old_bytes {
                continue;
            }
            let first_part = offsets
                .partition_point(|offset| *offset <= first)
                .saturating_sub(1);
            let last_part = offsets
                .partition_point(|offset| *offset < end)
                .saturating_sub(1);
            hit(HitRecord {
                row_id: fragments[first_part].row_id,
                byte_start: (fragments[first_part].byte_start + first - offsets[first_part]) as u64,
                end_row_id: fragments[last_part].row_id,
                byte_end: (fragments[last_part].byte_start + end - offsets[last_part]) as u64,
            })?;
            self.matched_through = base.saturating_add(end as u64);
        }
        self.logical_bytes = old_bytes
            .checked_add(line.text.len() as u64)
            .ok_or_else(|| io::Error::other("logical stream byte count overflow"))?;
        let mut tail = joined.len().saturating_sub(carry_bytes);
        while !joined.is_char_boundary(tail) {
            tail += 1;
        }
        self.carry.clear();
        for (fragment, offset) in fragments.into_iter().zip(offsets) {
            if offset + fragment.text.len() <= tail {
                continue;
            }
            let trim = tail.saturating_sub(offset);
            self.carry.push(Fragment {
                row_id: fragment.row_id,
                byte_start: fragment.byte_start + trim,
                text: fragment.text[trim..].to_owned(),
            });
        }
        Ok(())
    }
}

fn search_worker(state: Arc<QueryState>, regex: Regex, carry_bytes: usize) {
    let outcome: io::Result<()> = (|| {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&state.matches_path)?;
        let mut matches = BufWriter::with_capacity(64 * 1024, file);
        let mut reader = RowReader::open(&state.archive.directory.path)?;
        let mut scanned = 0u64;
        let mut count = 0u64;
        let mut matcher = MatcherState::default();
        loop {
            let snapshot = {
                let mut input = state.input.lock().unwrap();
                while input.is_none() && !state.cancelled.load(Ordering::Acquire) {
                    input = state.wake.wait(input).unwrap();
                }
                if state.cancelled.load(Ordering::Acquire) {
                    break;
                }
                input.take().unwrap()
            };
            {
                let mut data = state.data.lock().unwrap();
                data.provisional.clear();
                data.snapshot = Some(snapshot.clone());
                data.progress = SearchProgress {
                    revision: snapshot.revision,
                    scanned_rows: scanned,
                    total_rows: snapshot.rows + u64::from(snapshot.pending.is_some()),
                    matches: count,
                    complete: false,
                    cancelled: false,
                    error: None,
                    gaps: snapshot.gaps.clone(),
                };
            }
            while scanned < snapshot.rows {
                if state.cancelled.load(Ordering::Acquire) {
                    break;
                }
                let line = reader.read(scanned)?;
                matcher.scan(scanned, &line, &regex, carry_bytes, |record| {
                    if count.is_multiple_of(128) && state.cancelled.load(Ordering::Acquire) {
                        return Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "search cancelled",
                        ));
                    }
                    record.write(&mut matches)?;
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| io::Error::other("match count overflow"))?;
                    Ok(())
                })?;
                scanned += 1;
                if scanned.is_multiple_of(128) {
                    matches.flush()?;
                    let mut data = state.data.lock().unwrap();
                    data.committed_matches = count;
                    data.progress.matches = count;
                    data.progress.scanned_rows = scanned;
                }
            }
            matches.flush()?;
            if state.cancelled.load(Ordering::Acquire) {
                break;
            }
            let mut provisional = Vec::new();
            if let Some(pending) = &snapshot.pending {
                let mut tail_matcher = matcher.clone();
                tail_matcher.scan(snapshot.rows, pending, &regex, carry_bytes, |record| {
                    provisional.push(record);
                    Ok(())
                })?;
            }
            let mut data = state.data.lock().unwrap();
            data.committed_matches = count;
            data.progress.matches = count + provisional.len() as u64;
            data.progress.scanned_rows = data.progress.total_rows;
            data.progress.complete = snapshot.revision >= data.requested_revision;
            data.provisional = provisional;
        }
        Ok(())
    })();
    let mut data = state.data.lock().unwrap();
    if state.cancelled.load(Ordering::Acquire) {
        data.progress.cancelled = true;
        data.progress.complete = false;
        if let Err(error) = fs::remove_file(&state.matches_path)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "cancelled session search result cleanup failed");
        }
    } else if let Err(error) = outcome {
        data.progress.error = Some(format!("session search failed: {error}"));
        data.progress.complete = false;
        state
            .archive
            .directory
            .preserve
            .store(true, Ordering::Release);
    }
}

fn preview(text: &str, offset: usize) -> String {
    let mut start = offset.min(text.len()).saturating_sub(80);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    let snippet: String = text[start..].chars().take(180).collect();
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        snippet,
        if start + snippet.len() < text.len() {
            "…"
        } else {
            ""
        }
    )
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredLine {
    epoch: Option<Uuid>,
    seq: u64,
    kind: EventKind,
    source: String,
    text: String,
    source_style: StoredStyle,
    marker: Option<u32>,
    solid: Option<StoredStyle>,
    boundary: Option<u8>,
    echoed: bool,
    continues: bool,
}
#[derive(Debug, Serialize, Deserialize)]
struct StoredStyle {
    fg: Option<u32>,
    bg: Option<u32>,
    add: u16,
    sub: u16,
}
impl StoredStyle {
    fn from_style(style: Style) -> Self {
        Self {
            fg: style.fg.map(encode_color),
            bg: style.bg.map(encode_color),
            add: style.add_modifier.bits(),
            sub: style.sub_modifier.bits(),
        }
    }
    fn style(self) -> Style {
        Style {
            fg: self.fg.map(decode_color),
            bg: self.bg.map(decode_color),
            add_modifier: Modifier::from_bits_truncate(self.add),
            sub_modifier: Modifier::from_bits_truncate(self.sub),
            ..Style::default()
        }
    }
}
impl StoredLine {
    fn from_line(line: &DisplayLine) -> Self {
        Self {
            epoch: line.daemon_epoch,
            seq: line.seq,
            kind: line.event_kind,
            source: line.source.clone(),
            text: line.text.clone(),
            source_style: StoredStyle::from_style(line.source_style),
            marker: line.marker_color.map(encode_color),
            solid: line.solid_style.map(StoredStyle::from_style),
            boundary: line.run_boundary.map(|b| match b {
                RunBoundary::Started => 1,
                RunBoundary::Ended => 2,
                RunBoundary::Aborted => 3,
            }),
            echoed: line.echoed,
            continues: line.continues_previous,
        }
    }
    fn line(self) -> DisplayLine {
        DisplayLine {
            daemon_epoch: self.epoch,
            seq: self.seq,
            event_kind: self.kind,
            bytes: self.text.len() + self.source.len() + 16,
            source: self.source,
            text: self.text,
            source_style: self.source_style.style(),
            marker_color: self.marker.map(decode_color),
            solid_style: self.solid.map(StoredStyle::style),
            run_boundary: self.boundary.and_then(|b| match b {
                1 => Some(RunBoundary::Started),
                2 => Some(RunBoundary::Ended),
                3 => Some(RunBoundary::Aborted),
                _ => None,
            }),
            echoed: self.echoed,
            continues_previous: self.continues,
        }
    }
}

fn write_row(
    data: &mut File,
    index: &mut File,
    rows: &mut u64,
    line: &DisplayLine,
) -> io::Result<()> {
    if line.text.len() + line.source.len() > MAX_ROW_BYTES {
        return Err(io::Error::other(
            "normalized terminal row exceeds archive row bound",
        ));
    }
    let payload = serde_json::to_vec(&StoredLine::from_line(line)).map_err(io::Error::other)?;
    let offset = data.stream_position()?;
    data.write_all(&(payload.len() as u32).to_le_bytes())?;
    data.write_all(&payload)?;
    index.write_all(&offset.to_le_bytes())?;
    *rows = rows
        .checked_add(1)
        .ok_or_else(|| io::Error::other("archive row count overflow"))?;
    Ok(())
}
struct RowReader {
    data: File,
    index: File,
}
impl RowReader {
    fn open(path: &Path) -> io::Result<Self> {
        Ok(Self {
            data: File::open(path.join("rows.data"))?,
            index: File::open(path.join("rows.index"))?,
        })
    }
    fn read(&mut self, row: u64) -> io::Result<DisplayLine> {
        self.index.seek(SeekFrom::Start(
            row.checked_mul(8)
                .ok_or_else(|| io::Error::other("row offset overflow"))?,
        ))?;
        let mut offset = [0; 8];
        self.index.read_exact(&mut offset)?;
        self.data
            .seek(SeekFrom::Start(u64::from_le_bytes(offset)))?;
        let mut length = [0; 4];
        self.data.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length) as usize;
        if length > MAX_ROW_BYTES.saturating_mul(6).saturating_add(2048) {
            return Err(io::Error::other("corrupt archive row length"));
        }
        let mut payload = vec![0; length];
        self.data.read_exact(&mut payload)?;
        serde_json::from_slice::<StoredLine>(&payload)
            .map(StoredLine::line)
            .map_err(io::Error::other)
    }
}
fn read_snapshot_line(
    reader: &mut RowReader,
    snapshot: &Snapshot,
    row: u64,
) -> io::Result<DisplayLine> {
    if row < snapshot.rows {
        reader.read(row)
    } else if row == snapshot.rows {
        snapshot
            .pending
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "snapshot has no pending row"))
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "row is outside the search snapshot",
        ))
    }
}

fn encode_color(color: Color) -> u32 {
    match color {
        Color::Reset => 0,
        Color::Black => 1,
        Color::Red => 2,
        Color::Green => 3,
        Color::Yellow => 4,
        Color::Blue => 5,
        Color::Magenta => 6,
        Color::Cyan => 7,
        Color::Gray => 8,
        Color::DarkGray => 9,
        Color::LightRed => 10,
        Color::LightGreen => 11,
        Color::LightYellow => 12,
        Color::LightBlue => 13,
        Color::LightMagenta => 14,
        Color::LightCyan => 15,
        Color::White => 16,
        Color::Indexed(value) => 0x1000000 | u32::from(value),
        Color::Rgb(r, g, b) => {
            0x2000000 | (u32::from(r) << 16) | (u32::from(g) << 8) | u32::from(b)
        }
    }
}
fn decode_color(value: u32) -> Color {
    match value {
        0 => Color::Reset,
        1 => Color::Black,
        2 => Color::Red,
        3 => Color::Green,
        4 => Color::Yellow,
        5 => Color::Blue,
        6 => Color::Magenta,
        7 => Color::Cyan,
        8 => Color::Gray,
        9 => Color::DarkGray,
        10 => Color::LightRed,
        11 => Color::LightGreen,
        12 => Color::LightYellow,
        13 => Color::LightBlue,
        14 => Color::LightMagenta,
        15 => Color::LightCyan,
        16 => Color::White,
        value if value & 0x2000000 != 0 => {
            Color::Rgb((value >> 16) as u8, (value >> 8) as u8, value as u8)
        }
        value if value & 0x1000000 != 0 => Color::Indexed(value as u8),
        _ => Color::Reset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::display::TerminalStreamParser;
    use serial_protocol::{Direction, TimelineEvent};
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    fn row(seq: u64, text: impl Into<String>) -> DisplayLine {
        let mut line = gap_line(seq, text);
        line.daemon_epoch = Some(Uuid::nil());
        line.event_kind = EventKind::Rx;
        line.source = "设备".into();
        line.solid_style = None;
        line
    }
    fn event(seq: u64, bytes: &[u8]) -> TimelineEvent {
        TimelineEvent {
            port: "COM1".into(),
            daemon_epoch: Uuid::nil(),
            seq,
            generation: 1,
            wall_time_ns: 0,
            monotonic_time_ns: seq,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: None,
            stream_offset_end: None,
            data: bytes.to_vec(),
            metadata: BTreeMap::new(),
            durable: true,
        }
    }
    fn append_rows(archive: &SessionArchive, rows: Vec<DisplayLine>) {
        assert!(archive.append(&StreamDisplayBatch {
            completed: rows,
            pending: None,
            pending_committed: false
        }));
    }
    fn search(archive: &SessionArchive, query: &str) -> SearchHandle {
        archive
            .search(SearchQuery {
                query: query.into(),
                case_sensitive: true,
            })
            .unwrap()
    }
    fn complete(handle: &SearchHandle) -> SearchProgress {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let progress = handle.progress();
            assert!(progress.error.is_none(), "{progress:?}");
            if progress.complete {
                return progress;
            }
            assert!(
                Instant::now() < deadline,
                "search did not complete: {progress:?}"
            );
            thread::sleep(Duration::from_millis(2));
        }
    }
    fn refresh(archive: &SessionArchive, handle: &SearchHandle) -> SearchProgress {
        handle.refresh().unwrap();
        let progress = complete(handle);
        assert_eq!(progress.revision, archive.revision());
        progress
    }

    #[test]
    fn finds_first_history_after_old_scroll_limits_and_loads_old_context() {
        let archive = SessionArchive::new().unwrap();
        let mut rows = Vec::new();
        for i in 0..25_001 {
            rows.push(row(i + 1, format!("row {i:05} {}", "x".repeat(200))));
        }
        rows[0].text = "first unique boot record".into();
        let old_epoch = Uuid::new_v4();
        rows[0].daemon_epoch = Some(old_epoch);
        append_rows(&archive, rows);
        let handle = search(&archive, "first unique");
        let progress = complete(&handle);
        assert_eq!(progress.total_rows, 25_001);
        assert_eq!(progress.matches, 1);
        let hit = handle.page(0, 10).unwrap().remove(0);
        assert_eq!(hit.row_id, 0);
        let context = handle.context(&hit, 0, 2).unwrap();
        assert_eq!(context.len(), 3);
        assert_eq!(context[0].line.daemon_epoch, Some(old_epoch));
        assert!(context[1].line.text.starts_with("row 00001"));
    }

    #[test]
    fn million_matches_are_disk_paged_without_a_recent_result_cap() {
        let archive = SessionArchive::new().unwrap();
        append_rows(
            &archive,
            (0..1000).map(|i| row(i, "x".repeat(1000))).collect(),
        );
        let handle = search(&archive, "x");
        assert_eq!(complete(&handle).matches, 1_000_000);
        let page = handle.page(999_990, 20).unwrap();
        assert_eq!(page.len(), 10);
        assert_eq!(page[0].ordinal, 999_990);
        assert_eq!(page.last().unwrap().row_id, 999);
        assert_eq!(page.last().unwrap().byte_start, 999);
        assert_eq!(
            fs::metadata(&handle.inner.state.matches_path)
                .unwrap()
                .len(),
            1_000_000 * MATCH_BYTES
        );
        assert!(
            handle
                .inner
                .state
                .data
                .lock()
                .unwrap()
                .provisional
                .is_empty()
        );
        assert_eq!(handle.page(123_456, 1).unwrap()[0].byte_start, 456);
    }

    #[test]
    fn pending_progress_replaces_content_and_commits_exactly_once() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        archive.append(&parser.push_event(&event(1, b"progress 100%")));
        let handle = search(&archive, "100");
        assert_eq!(complete(&handle).matches, 1);
        let old = handle.page(0, 1).unwrap().remove(0);
        assert!(old.provisional);
        archive.append(&parser.push_event(&event(2, b"\r42%\x1b[K")));
        assert_eq!(refresh(&archive, &handle).matches, 0);
        assert!(handle.context(&old, 0, 0).is_err());
        archive.append(&parser.push_event(&event(3, b"\n")));
        assert_eq!(refresh(&archive, &handle).matches, 0);
        let correct = search(&archive, "42%");
        let progress = complete(&correct);
        assert_eq!(progress.total_rows, 1);
        assert_eq!(progress.matches, 1);
        assert!(!correct.page(0, 1).unwrap()[0].provisional);
    }

    #[test]
    fn incremental_head_only_scans_new_rows_and_old_pending_does_not_duplicate() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        archive.append(&parser.push_event(&event(1, b"hit\nhit")));
        let handle = search(&archive, "hit");
        assert_eq!(complete(&handle).matches, 2);
        assert_eq!(handle.inner.state.data.lock().unwrap().committed_matches, 1);
        archive.append(&parser.push_event(&event(2, b"\nhit\n")));
        assert_eq!(refresh(&archive, &handle).matches, 3);
        assert_eq!(handle.inner.state.data.lock().unwrap().committed_matches, 3);
        assert_eq!(
            fs::metadata(&handle.inner.state.matches_path)
                .unwrap()
                .len(),
            3 * MATCH_BYTES
        );
        assert_eq!(refresh(&archive, &handle).matches, 3);
    }

    #[test]
    fn fixed_head_revision_does_not_claim_later_queued_output_was_scanned() {
        let archive = SessionArchive::new().unwrap();
        append_rows(&archive, vec![row(1, "old")]);
        let handle = search(&archive, "later");
        append_rows(&archive, vec![row(2, "later")]);
        let first = complete(&handle);
        assert_eq!(first.matches, 0);
        assert!(first.revision < archive.revision());
        assert_eq!(refresh(&archive, &handle).matches, 1);
    }

    #[test]
    fn saturated_refresh_preserves_completed_head_and_can_be_retried() {
        let archive = SessionArchive::new().unwrap();
        append_rows(&archive, vec![row(1, "needle")]);
        let mut handle = search(&archive, "needle");
        assert_eq!(complete(&handle).matches, 1);
        // Substitute a deterministically full queue, without blocking disk
        // workers or depending on the machine's filesystem speed.
        let (sender, receiver) = mpsc::sync_channel(1);
        sender.try_send(Message::Boundary).unwrap();
        let original =
            std::mem::replace(&mut Arc::get_mut(&mut handle.inner).unwrap().sender, sender);
        assert_eq!(
            handle.refresh().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(handle.progress().complete);
        assert_eq!(handle.progress().matches, 1);
        receiver.recv().unwrap();
        handle.refresh().unwrap();
        assert!(!handle.progress().complete);
        assert!(matches!(receiver.recv().unwrap(), Message::Snapshot(_, _)));
        Arc::get_mut(&mut handle.inner).unwrap().sender = original;
        assert_eq!(refresh(&archive, &handle).matches, 1);
    }

    #[test]
    fn lossless_boundary_seals_pending_without_gap_or_cross_session_match() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        archive.append(&parser.push_event(&event(1, b"before")));
        archive.record_boundary();
        parser.reset();
        archive.append(&parser.push_event(&event(2, b"after")));
        let handle = search(&archive, "beforeafter");
        let progress = complete(&handle);
        assert_eq!(progress.matches, 0);
        assert!(progress.gaps.is_empty());
        assert_eq!(progress.total_rows, 2);
        archive.append(&parser.push_event(&event(3, b" suffix\n")));
        assert_eq!(refresh(&archive, &handle).matches, 0);
        let old = search(&archive, "before");
        assert_eq!(complete(&old).matches, 1);
        let context = old.context(&old.page(0, 1).unwrap()[0], 0, 1).unwrap();
        assert_eq!(context[1].line.text, "after suffix");
        assert!(!context[1].line.continues_previous);
    }

    #[test]
    fn parser_epoch_flush_does_not_append_the_old_pending_twice() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        archive.append(&parser.push_event(&event(1, b"old")));
        let mut next = event(2, b"new");
        next.daemon_epoch = Uuid::new_v4();
        let batch = parser.push_event(&next);
        assert!(batch.pending_committed);
        archive.append(&batch);
        let handle = search(&archive, "old");
        assert_eq!(complete(&handle).matches, 1);
    }

    #[test]
    fn long_line_split_utf8_and_ansi_are_searchable_across_parser_chunks() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        let mut first = vec![b'a'; 16 * 1024 - 1];
        first.extend_from_slice(b"NE");
        let batch = parser.push_event(&event(1, &first));
        assert!(!batch.completed[0].continues_previous);
        assert!(batch.pending.as_ref().unwrap().continues_previous);
        archive.append(&batch);
        let handle = search(&archive, "NEEDLE");
        assert_eq!(complete(&handle).matches, 0);
        archive.append(&parser.push_event(&event(2, b"\x1b[31mEDLE\x1b[0m\n")));
        assert_eq!(refresh(&archive, &handle).matches, 1);
        let hit = handle.page(0, 1).unwrap().remove(0);
        assert_eq!((hit.row_id, hit.end_row_id), (0, 1));
        assert_eq!(hit.byte_start, 16 * 1024 - 1);
        assert_eq!(hit.byte_end, 5);
        let context = handle.context(&hit, 0, 0).unwrap();
        assert_eq!(context.len(), 2);
        assert!(context[1].line.continues_previous);
        archive.append(&parser.push_event(&event(3, &[0xe4, 0xb8])));
        archive.append(&parser.push_event(&event(4, &[0xad, 0xe6, 0x96, 0x87, b'\n'])));
        let unicode = search(&archive, "中文");
        assert_eq!(complete(&unicode).matches, 1);
    }

    #[test]
    fn actual_newlines_epochs_generations_and_gaps_do_not_join() {
        let archive = SessionArchive::new().unwrap();
        let mut parser = TerminalStreamParser::new();
        archive.append(&parser.push_event(&event(1, b"old\nnew\n")));
        let none = search(&archive, "oldnew");
        assert_eq!(complete(&none).matches, 0);
        archive.append(&parser.push_event(&event(2, b"before")));
        archive.record_gap("connection capture gap");
        parser.reset();
        let mut after = event(3, b"after\n");
        after.daemon_epoch = Uuid::new_v4();
        archive.append(&parser.push_event(&after));
        let no_cross = search(&archive, "beforeafter");
        let result = complete(&no_cross);
        assert_eq!(result.matches, 0);
        assert!(!result.gaps.is_empty());
        let before = search(&archive, "before");
        assert_eq!(complete(&before).matches, 1);
        drop(none);
        drop(no_cross);
        drop(before);
        archive.append(&parser.push_event(&event(4, b"gen1")));
        let mut next_generation = event(5, b"gen2\n");
        next_generation.generation = 2;
        archive.append(&parser.push_event(&next_generation));
        let generation = search(&archive, "gen1gen2");
        assert_eq!(complete(&generation).matches, 0);
    }

    #[test]
    fn annotations_do_not_join_into_or_break_an_artificial_stream_continuation() {
        let archive = SessionArchive::new().unwrap();
        let first = row(1, "abc");
        let mut annotation = row(2, "not-stream");
        annotation.event_kind = EventKind::RunStarted;
        let mut second = row(3, "def");
        second.continues_previous = true;
        append_rows(&archive, vec![first, annotation, second]);
        let handle = search(&archive, "cde");
        assert_eq!(complete(&handle).matches, 1);
        let hit = handle.page(0, 1).unwrap().remove(0);
        assert_eq!((hit.row_id, hit.end_row_id), (0, 2));
        let none = search(&archive, "streamdef");
        assert_eq!(complete(&none).matches, 0);
    }

    #[test]
    fn unicode_case_insensitive_matches_preserve_original_byte_offsets() {
        let archive = SessionArchive::new().unwrap();
        append_rows(&archive, vec![row(1, "x K K k 中")]);
        let handle = archive
            .search(SearchQuery {
                query: "k".into(),
                case_sensitive: false,
            })
            .unwrap();
        assert_eq!(complete(&handle).matches, 3);
        let hits = handle.page(0, 10).unwrap();
        assert_eq!(
            hits.iter().map(|h| h.byte_start).collect::<Vec<_>>(),
            [2, 6, 8]
        );
        assert_eq!(hits[0].byte_end, 5);
    }

    #[test]
    fn matches_remain_nonoverlapping_across_many_storage_segments() {
        let archive = SessionArchive::new().unwrap();
        let rows = (0..6)
            .map(|i| {
                let mut line = row(i, "a");
                line.continues_previous = i > 0;
                line
            })
            .collect();
        append_rows(&archive, rows);
        let handle = search(&archive, "aa");
        assert_eq!(complete(&handle).matches, 3);
        assert_eq!(
            handle
                .page(0, 5)
                .unwrap()
                .iter()
                .map(|h| (h.row_id, h.end_row_id))
                .collect::<Vec<_>>(),
            [(0, 1), (2, 3), (4, 5)]
        );
    }

    #[test]
    fn cancelled_queries_stop_and_cannot_overwrite_new_query_state() {
        let archive = SessionArchive::new().unwrap();
        append_rows(&archive, vec![row(1, "old new")]);
        let old = search(&archive, "old");
        old.cancel();
        let new = search(&archive, "new");
        assert_eq!(complete(&new).matches, 1);
        assert!(old.progress().cancelled);
        assert!(old.page(0, 1).is_err());
        assert!(old.refresh().is_err());
    }

    #[test]
    fn queue_backpressure_is_explicit_not_a_false_zero_match() {
        let archive = SessionArchive::new().unwrap();
        let batch = StreamDisplayBatch {
            completed: vec![row(1, "x".repeat(QUEUE_BYTES + 1))],
            pending: None,
            pending_committed: false,
        };
        assert!(!archive.append(&batch));
        let handle = search(&archive, "x");
        let progress = complete(&handle);
        assert_eq!(progress.matches, 0);
        assert!(!progress.gaps.is_empty());
        let path = archive.path().to_owned();
        drop(handle);
        drop(archive);
        // The fault intentionally preserves this exact test-created directory.
        thread::sleep(Duration::from_millis(20));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn corrupt_disk_search_reports_error_instead_of_complete_zero() {
        let archive = SessionArchive::new().unwrap();
        append_rows(&archive, vec![row(1, "needle")]);
        let ready = search(&archive, "needle");
        assert_eq!(complete(&ready).matches, 1);
        drop(ready);
        File::create(archive.path().join("rows.data")).unwrap();
        let failed = search(&archive, "needle");
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let progress = failed.progress();
            if progress.error.is_some() {
                assert!(!progress.complete);
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(2));
        }
        let path = archive.path().to_owned();
        drop(failed);
        drop(archive);
        thread::sleep(Duration::from_millis(20));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn owned_temp_directory_is_removed_after_all_handles_drop() {
        let archive = SessionArchive::new().unwrap();
        let path = archive.path().to_owned();
        let handle = search(&archive, "none");
        complete(&handle);
        drop(archive);
        assert!(path.exists());
        drop(handle);
        let deadline = Instant::now() + Duration::from_secs(2);
        while path.exists() {
            assert!(Instant::now() < deadline, "archive not cleaned");
            thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn context_retains_projection_styles_and_hit_bounds_are_checked() {
        let archive = SessionArchive::new().unwrap();
        let mut line = row(1, "needle");
        line.source_style = Style::default()
            .fg(Color::Rgb(12, 34, 56))
            .bg(Color::Indexed(200))
            .add_modifier(Modifier::BOLD);
        line.marker_color = Some(Color::Cyan);
        line.echoed = true;
        append_rows(&archive, vec![line.clone()]);
        let handle = search(&archive, "needle");
        complete(&handle);
        let hit = handle.page(0, 1).unwrap().remove(0);
        let recovered = handle.context(&hit, 0, 0).unwrap().remove(0).line;
        assert_eq!(recovered.source_style, line.source_style);
        assert_eq!(recovered.marker_color, line.marker_color);
        assert!(recovered.echoed);
        assert!(handle.page(0, MAX_PAGE + 1).is_err());
        assert!(handle.page(10, 1).unwrap().is_empty());
    }
}
