//! Crash-recoverable, append-only journal for serial timeline events.
//!
//! The segment files are the source of truth. Each record stores a JSON
//! [`DataFrameHeader`] followed by the event's raw bytes, protected by a CRC.
//! Queries intentionally run on blocking workers instead of the journal writer
//! so a large historical scan cannot stall serial ingestion.

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use serial_protocol::{
    ArchiveListResponse, ArchiveSummary, Cursor, DataFrameHeader, EventQuery, EventQueryResponse,
    GapRange, GapReason, MAX_HEADER_BYTES, MAX_PAYLOAD_BYTES, TimelineEvent,
};
use std::collections::{HashMap, hash_map::Entry};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot};
use uuid::Uuid;

const SEGMENT_MAGIC: &[u8; 8] = b"SRLSEG01";
const RECORD_MAGIC: &[u8; 8] = b"SRLEVT01";
const FORMAT_VERSION: u32 = 1;
const SEGMENT_PREFIX_LEN: usize = 20;
const RECORD_PREFIX_LEN: usize = 20;
const MAX_SEGMENT_HEADER_BYTES: usize = 64 * 1024;
const MAX_SLOT_ID_BYTES: usize = 80;
const GAP_LEDGER_NAME: &str = "retention-gaps.jsonl";
const DEFAULT_QUERY_EVENTS: usize = 1_000;
const MAX_QUERY_EVENTS: usize = 10_000;
const DEFAULT_QUERY_BYTES: usize = 1024 * 1024;
const MAX_QUERY_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONCURRENT_QUERIES: usize = 2;
const MAX_QUERY_SCAN_BYTES: u64 = 256 * 1024 * 1024;
const MAX_QUERY_SCAN_TIME: Duration = Duration::from_secs(5);
const MAX_QUERY_SEGMENTS: usize = 16_384;
const MAX_ARCHIVE_SUMMARIES: usize = 4_096;
const MAX_QUERY_GAPS: usize = 1_024;
const MAX_GAP_LEDGER_LINE_BYTES: usize = 64 * 1024;

/// Runtime limits for the append-only journal.
#[derive(Debug, Clone)]
pub struct JournalConfig {
    pub root_dir: PathBuf,
    pub queue_capacity: usize,
    pub max_segment_bytes: u64,
    pub max_segment_age: Duration,
    pub max_total_bytes: u64,
    pub cleanup_low_watermark: f64,
}

impl JournalConfig {
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
            queue_capacity: 1_024,
            max_segment_bytes: 64 * 1024 * 1024,
            max_segment_age: Duration::from_secs(60 * 60),
            max_total_bytes: 10 * 1024 * 1024 * 1024,
            cleanup_low_watermark: 0.90,
        }
    }

    fn validate(&self) -> Result<(), JournalError> {
        if self.queue_capacity == 0 {
            return Err(JournalError::InvalidConfig(
                "queue_capacity must be greater than zero".into(),
            ));
        }
        if self.max_segment_bytes == 0 {
            return Err(JournalError::InvalidConfig(
                "max_segment_bytes must be greater than zero".into(),
            ));
        }
        if self.max_segment_age.is_zero() {
            return Err(JournalError::InvalidConfig(
                "max_segment_age must be greater than zero".into(),
            ));
        }
        if self.max_total_bytes == 0 {
            return Err(JournalError::InvalidConfig(
                "max_total_bytes must be greater than zero".into(),
            ));
        }
        if !self.cleanup_low_watermark.is_finite()
            || self.cleanup_low_watermark <= 0.0
            || self.cleanup_low_watermark > 1.0
        {
            return Err(JournalError::InvalidConfig(
                "cleanup_low_watermark must be in (0, 1]".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("journal configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("slot id must contain 1..={MAX_SLOT_ID_BYTES} UTF-8 bytes")]
    InvalidSlotId,
    #[error("journal event payload is too large: {actual} bytes (max {maximum})")]
    PayloadTooLarge { actual: usize, maximum: usize },
    #[error("journal event header is too large: {actual} bytes (max {maximum})")]
    HeaderTooLarge { actual: usize, maximum: usize },
    #[error(
        "event sequence is not monotonic for slot {slot_id} epoch {epoch}: previous {previous}, got {got}"
    )]
    NonMonotonicSequence {
        slot_id: String,
        epoch: Uuid,
        previous: u64,
        got: u64,
    },
    #[error("journal writer queue is full")]
    QueueFull,
    #[error("journal writer is closed")]
    WriterClosed,
    #[error("journal writer thread panicked")]
    WriterPanicked,
    #[error("journal data is corrupt at {path}: {message}")]
    Corrupt { path: PathBuf, message: String },
    #[error(
        "journal segment {path} was poisoned after an append failed ({append_error}) and rollback failed ({rollback_error})"
    )]
    SegmentPoisoned {
        path: PathBuf,
        append_error: String,
        rollback_error: String,
    },
    #[error(
        "journal retention could not reach {target_bytes} bytes (usage {usage_bytes} bytes): {message}"
    )]
    RetentionFailed {
        usage_bytes: u64,
        target_bytes: u64,
        message: String,
    },
    #[error(
        "journal retention is backing off for {retry_after_ms} ms (usage {usage_bytes} bytes, limit {limit_bytes} bytes)"
    )]
    RetentionBackoff {
        usage_bytes: u64,
        limit_bytes: u64,
        retry_after_ms: u64,
    },
    #[error(
        "journal query budget was exceeded during {phase} after {scanned_bytes} bytes and {elapsed_ms} ms"
    )]
    QueryBudgetExceeded {
        phase: &'static str,
        scanned_bytes: u64,
        elapsed_ms: u64,
    },
    #[error("journal query has more than {maximum} distinct gap ranges")]
    TooManyQueryGaps { maximum: usize },
    #[error("journal query has more than {maximum} segment files")]
    TooManyQuerySegments { maximum: usize },
    #[error("journal I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("journal JSON codec failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Owns the dedicated writer thread. Clone [`JournalHandle`] values for users.
pub struct JournalManager {
    handle: JournalHandle,
    worker: Option<JoinHandle<()>>,
}

impl JournalManager {
    /// Recovers unfinished `.open` files before accepting new events.
    pub fn open(config: JournalConfig) -> Result<Self, JournalError> {
        config.validate()?;
        validate_root(&config.root_dir)?;
        fs::create_dir_all(slots_root(&config.root_dir))?;
        recover_gap_ledger(&config.root_dir)?;
        recover_open_segments(&config.root_dir)?;

        let config = Arc::new(config);
        let state = WriterState::initialize(Arc::clone(&config))?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let handle = JournalHandle {
            sender,
            config,
            query_gate: Arc::new(Semaphore::new(MAX_CONCURRENT_QUERIES)),
        };
        let worker = thread::Builder::new()
            .name("seriald-journal".into())
            .spawn(move || writer_loop(state, receiver))?;

        Ok(Self {
            handle,
            worker: Some(worker),
        })
    }

    pub fn handle(&self) -> JournalHandle {
        self.handle.clone()
    }

    /// Flushes, seals all active segments, and joins the writer thread.
    pub async fn shutdown(mut self) -> Result<(), JournalError> {
        let result = self.handle.request_shutdown().await;
        if let Some(worker) = self.worker.take() {
            tokio::task::spawn_blocking(move || worker.join())
                .await
                .map_err(|_| JournalError::WriterPanicked)?
                .map_err(|_| JournalError::WriterPanicked)?;
        }
        result
    }
}

impl Drop for JournalManager {
    fn drop(&mut self) {
        if self.worker.is_some() {
            let _ = self
                .handle
                .sender
                .try_send(WriterCommand::Shutdown { reply: None });
        }
    }
}

/// Cloneable client for appending, querying, and flushing journal events.
#[derive(Clone)]
pub struct JournalHandle {
    sender: mpsc::Sender<WriterCommand>,
    config: Arc<JournalConfig>,
    query_gate: Arc<Semaphore>,
}

impl JournalHandle {
    /// Appends an event through the bounded writer queue.
    ///
    /// The returned event has `durable=true`; the input event is never reported
    /// durable before its complete record has been appended and flushed.
    pub async fn append(&self, event: TimelineEvent) -> Result<TimelineEvent, JournalError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(WriterCommand::Append {
                event: Box::new(event),
                reply,
            })
            .await
            .map_err(|_| JournalError::WriterClosed)?;
        result.await.map_err(|_| JournalError::WriterClosed)?
    }

    /// Non-waiting enqueue variant for hot paths that must detect saturation.
    pub fn try_append(&self, event: TimelineEvent) -> Result<PendingAppend, JournalError> {
        let (reply, result) = oneshot::channel();
        match self.sender.try_send(WriterCommand::Append {
            event: Box::new(event),
            reply,
        }) {
            Ok(()) => Ok(PendingAppend { result }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(JournalError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(JournalError::WriterClosed),
        }
    }

    /// Queries immutable segments and the current append-only file on a blocking
    /// worker. A slow historical search therefore never occupies the writer.
    pub async fn query(
        &self,
        slot_id: impl Into<String>,
        query: EventQuery,
    ) -> Result<EventQueryResponse, JournalError> {
        let slot_id = slot_id.into();
        validate_slot_id(&slot_id)?;
        let config = Arc::clone(&self.config);
        let permit = Arc::clone(&self.query_gate)
            .acquire_owned()
            .await
            .map_err(|_| JournalError::WriterClosed)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            query_files(&config, &slot_id, &query)
        })
        .await
        .map_err(|_| JournalError::WriterPanicked)?
    }

    /// Lists retained Slot/epoch archives without loading event payloads into
    /// memory. It shares the historical-query semaphore and scan budgets.
    pub async fn list_archives(
        &self,
        slot_id: Option<String>,
    ) -> Result<ArchiveListResponse, JournalError> {
        if let Some(slot_id) = slot_id.as_deref() {
            validate_slot_id(slot_id)?;
        }
        let config = Arc::clone(&self.config);
        let permit = Arc::clone(&self.query_gate)
            .acquire_owned()
            .await
            .map_err(|_| JournalError::WriterClosed)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            list_archive_files(&config, slot_id.as_deref())
        })
        .await
        .map_err(|_| JournalError::WriterPanicked)?
    }

    pub async fn flush(&self) -> Result<(), JournalError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(WriterCommand::Flush { reply })
            .await
            .map_err(|_| JournalError::WriterClosed)?;
        result.await.map_err(|_| JournalError::WriterClosed)?
    }

    async fn request_shutdown(&self) -> Result<(), JournalError> {
        let (reply, result) = oneshot::channel();
        self.sender
            .send(WriterCommand::Shutdown { reply: Some(reply) })
            .await
            .map_err(|_| JournalError::WriterClosed)?;
        result.await.map_err(|_| JournalError::WriterClosed)?
    }
}

/// Completion token returned by [`JournalHandle::try_append`].
pub struct PendingAppend {
    result: oneshot::Receiver<Result<TimelineEvent, JournalError>>,
}

impl PendingAppend {
    pub async fn wait(self) -> Result<TimelineEvent, JournalError> {
        self.result.await.map_err(|_| JournalError::WriterClosed)?
    }
}

enum WriterCommand {
    Append {
        event: Box<TimelineEvent>,
        reply: oneshot::Sender<Result<TimelineEvent, JournalError>>,
    },
    Flush {
        reply: oneshot::Sender<Result<(), JournalError>>,
    },
    Shutdown {
        reply: Option<oneshot::Sender<Result<(), JournalError>>>,
    },
}

fn writer_loop(mut state: WriterState, mut receiver: mpsc::Receiver<WriterCommand>) {
    while let Some(command) = receiver.blocking_recv() {
        match command {
            WriterCommand::Append { event, reply } => {
                let _ = reply.send(state.append(*event));
            }
            WriterCommand::Flush { reply } => {
                let _ = reply.send(state.flush_all());
            }
            WriterCommand::Shutdown { reply } => {
                let result = state.finish();
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
                return;
            }
        }
    }

    if let Err(error) = state.finish() {
        tracing::error!(%error, "failed to finish serial journal after channel closed");
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct StreamKey {
    slot_id: String,
    epoch: Uuid,
}

struct WriterState {
    config: Arc<JournalConfig>,
    open_segments: HashMap<StreamKey, OpenSegment>,
    heads: HashMap<StreamKey, u64>,
    total_bytes: u64,
    retention_retry_at: Option<Instant>,
    retention_failures: u32,
    #[cfg(test)]
    retention_delete_failures_remaining: usize,
    #[cfg(test)]
    retention_scan_count: usize,
}

impl WriterState {
    fn initialize(config: Arc<JournalConfig>) -> Result<Self, JournalError> {
        let mut heads: HashMap<StreamKey, u64> = HashMap::new();
        for segment in discover_segments(&config.root_dir, None)? {
            if let Some(last_seq) = segment.last_seq {
                let key = StreamKey {
                    slot_id: segment.header.slot_id.clone(),
                    epoch: segment.header.daemon_epoch,
                };
                heads
                    .entry(key)
                    .and_modify(|head| *head = (*head).max(last_seq))
                    .or_insert(last_seq);
            }
        }

        let total_bytes = directory_size(&config.root_dir)?;
        let mut state = Self {
            config,
            open_segments: HashMap::new(),
            heads,
            total_bytes,
            retention_retry_at: None,
            retention_failures: 0,
            #[cfg(test)]
            retention_delete_failures_remaining: 0,
            #[cfg(test)]
            retention_scan_count: 0,
        };
        state.ensure_capacity(0)?;
        Ok(state)
    }

    fn append(&mut self, mut event: TimelineEvent) -> Result<TimelineEvent, JournalError> {
        validate_slot_id(&event.slot_id)?;
        if event.seq == 0 {
            return Err(JournalError::NonMonotonicSequence {
                slot_id: event.slot_id,
                epoch: event.daemon_epoch,
                previous: 0,
                got: 0,
            });
        }

        let key = StreamKey {
            slot_id: event.slot_id.clone(),
            epoch: event.daemon_epoch,
        };
        let previous = self.heads.get(&key).copied().unwrap_or(0);
        if event.seq <= previous {
            return Err(JournalError::NonMonotonicSequence {
                slot_id: event.slot_id,
                epoch: event.daemon_epoch,
                previous,
                got: event.seq,
            });
        }

        if event.seq > previous.saturating_add(1) {
            self.record_gap(StoredGap {
                slot_id: event.slot_id.clone(),
                epoch: event.daemon_epoch,
                first_seq: previous.saturating_add(1),
                last_seq: event.seq - 1,
                reason: GapReason::LoggingFault,
                recorded_wall_time_ns: now_wall_time_ns(),
            })?;
        }

        event.durable = true;
        let record = encode_record(&event)?;
        let should_rotate = self
            .open_segments
            .get(&key)
            .is_some_and(|segment| segment.should_rotate(&self.config, record.len() as u64));
        if should_rotate {
            self.rotate(&key)?;
        }

        let pending_header =
            (!self.open_segments.contains_key(&key)).then(|| SegmentHeader::for_event(&event));
        let header_bytes = pending_header
            .as_ref()
            .map(segment_header_encoded_len)
            .transpose()?
            .unwrap_or(0);
        self.ensure_capacity((record.len() as u64).saturating_add(header_bytes))?;

        if let Some(header) = pending_header {
            let segment = OpenSegment::create_with_header(&self.config.root_dir, header)?;
            self.total_bytes = self.total_bytes.saturating_add(segment.bytes_written);
            self.open_segments.insert(key.clone(), segment);
        }

        // Temporarily remove the segment from the writable set. A failed append
        // is only reinserted when its original length and cursor were both
        // restored. A poisoned file remains readable up to its valid prefix,
        // but no later record may ever be appended behind that partial tail.
        let mut segment = self
            .open_segments
            .remove(&key)
            .expect("open segment inserted");
        match segment.append(&record, event.seq) {
            Ok(bytes_written) => {
                self.total_bytes = self.total_bytes.saturating_add(bytes_written);
                self.open_segments.insert(key.clone(), segment);
                self.heads.insert(key, event.seq);
                Ok(event)
            }
            Err(SegmentAppendError::Recovered(error)) => {
                self.open_segments.insert(key, segment);
                Err(error)
            }
            Err(SegmentAppendError::Poisoned(error)) => {
                if let Ok(metadata) = fs::metadata(&segment.path) {
                    self.total_bytes = self
                        .total_bytes
                        .saturating_sub(segment.bytes_written)
                        .saturating_add(metadata.len());
                }
                drop(segment);
                Err(error)
            }
        }
    }

    fn rotate(&mut self, key: &StreamKey) -> Result<(), JournalError> {
        if let Some(segment) = self.open_segments.remove(key) {
            segment.seal()?;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<(), JournalError> {
        for segment in self.open_segments.values_mut() {
            segment.file.flush()?;
            segment.file.sync_data()?;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<(), JournalError> {
        let segments = std::mem::take(&mut self.open_segments);
        let mut first_error = None;
        for (_, segment) in segments {
            if let Err(error) = segment.seal()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        if let Err(error) = self.enforce_retention(0)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn record_gap(&mut self, gap: StoredGap) -> Result<(), JournalError> {
        let update = append_gap_ledger(&self.config.root_dir, &gap)?;
        self.total_bytes = self
            .total_bytes
            .saturating_sub(update.truncated_bytes)
            .saturating_add(update.appended_bytes);
        Ok(())
    }

    fn ensure_capacity(&mut self, additional_bytes: u64) -> Result<(), JournalError> {
        let projected = self.total_bytes.saturating_add(additional_bytes);
        if projected <= self.config.max_total_bytes {
            self.retention_retry_at = None;
            self.retention_failures = 0;
            return Ok(());
        }

        let now = Instant::now();
        if let Some(retry_at) = self.retention_retry_at
            && retry_at > now
        {
            let retry_after_ms = retry_at
                .saturating_duration_since(now)
                .as_millis()
                .min(u64::MAX as u128) as u64;
            return Err(JournalError::RetentionBackoff {
                usage_bytes: projected,
                limit_bytes: self.config.max_total_bytes,
                retry_after_ms: retry_after_ms.max(1),
            });
        }

        match self.enforce_retention(additional_bytes) {
            Ok(()) => {
                self.retention_retry_at = None;
                self.retention_failures = 0;
                Ok(())
            }
            Err(error) => {
                self.retention_failures = self.retention_failures.saturating_add(1);
                let exponent = self.retention_failures.saturating_sub(1).min(6);
                let delay_seconds = (1_u64 << exponent).min(60);
                self.retention_retry_at = Some(now + Duration::from_secs(delay_seconds));
                Err(error)
            }
        }
    }

    fn enforce_retention(&mut self, additional_bytes: u64) -> Result<(), JournalError> {
        #[cfg(test)]
        {
            self.retention_scan_count = self.retention_scan_count.saturating_add(1);
        }

        self.total_bytes = directory_size(&self.config.root_dir)?;
        if self.total_bytes.saturating_add(additional_bytes) <= self.config.max_total_bytes {
            return Ok(());
        }

        let target = ((self.config.max_total_bytes as f64) * self.config.cleanup_low_watermark)
            .floor() as u64;
        let mut sealed: Vec<_> = discover_segments(&self.config.root_dir, None)?
            .into_iter()
            .filter(|segment| segment.sealed)
            .collect();
        sealed.sort_by_key(|segment| {
            (
                segment.header.created_wall_time_ns,
                segment.header.first_seq,
            )
        });

        let mut deletion_failures = Vec::new();
        for segment in sealed {
            if self.total_bytes <= target
                && self.total_bytes.saturating_add(additional_bytes) <= self.config.max_total_bytes
            {
                break;
            }
            let Some(last_seq) = segment.last_seq else {
                continue;
            };
            let size = match fs::metadata(&segment.path) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let gap = StoredGap {
                slot_id: segment.header.slot_id.clone(),
                epoch: segment.header.daemon_epoch,
                first_seq: segment.header.first_seq,
                last_seq,
                reason: GapReason::Retention,
                recorded_wall_time_ns: now_wall_time_ns(),
            };

            #[cfg(test)]
            let remove_result = if self.retention_delete_failures_remaining > 0 {
                self.retention_delete_failures_remaining -= 1;
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected retention deletion failure",
                ))
            } else {
                fs::remove_file(&segment.path)
            };
            #[cfg(not(test))]
            let remove_result = fs::remove_file(&segment.path);

            match remove_result {
                Ok(()) => {
                    self.total_bytes = self.total_bytes.saturating_sub(size);
                    // A crash between deletion and this small ledger append is
                    // still made visible by query's synthetic leading gap. Doing
                    // the deletion first avoids reporting a false gap when a
                    // Windows reader temporarily prevents removal.
                    self.record_gap(gap)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(
                        path = %segment.path.display(),
                        %error,
                        "could not remove retained journal segment"
                    );
                    deletion_failures.push(format!("{}: {error}", segment.path.display()));
                }
            }
        }
        self.total_bytes = directory_size(&self.config.root_dir)?;
        let projected = self.total_bytes.saturating_add(additional_bytes);
        if !deletion_failures.is_empty()
            || self.total_bytes > target
            || projected > self.config.max_total_bytes
        {
            let message = if deletion_failures.is_empty() {
                "insufficient sealed segments are available for cleanup".to_string()
            } else {
                deletion_failures.join("; ")
            };
            return Err(JournalError::RetentionFailed {
                usage_bytes: projected,
                target_bytes: target,
                message,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentHeader {
    slot_id: String,
    daemon_epoch: Uuid,
    segment_id: Uuid,
    created_wall_time_ns: i64,
    first_seq: u64,
}

impl SegmentHeader {
    fn for_event(event: &TimelineEvent) -> Self {
        Self {
            slot_id: event.slot_id.clone(),
            daemon_epoch: event.daemon_epoch,
            segment_id: Uuid::new_v4(),
            created_wall_time_ns: now_wall_time_ns(),
            first_seq: event.seq,
        }
    }
}

#[derive(Debug)]
enum SegmentAppendError {
    Recovered(JournalError),
    Poisoned(JournalError),
}

struct OpenSegment {
    header: SegmentHeader,
    path: PathBuf,
    file: File,
    bytes_written: u64,
    last_seq: u64,
    opened_at: Instant,
    #[cfg(test)]
    poison_next_append: bool,
}

impl OpenSegment {
    #[cfg(test)]
    fn create(root: &Path, event: &TimelineEvent) -> Result<Self, JournalError> {
        Self::create_with_header(root, SegmentHeader::for_event(event))
    }

    fn create_with_header(root: &Path, header: SegmentHeader) -> Result<Self, JournalError> {
        let directory = epoch_dir(root, &header.slot_id, header.daemon_epoch);
        fs::create_dir_all(&directory)?;
        let name = format!("{:020}-{}.open", header.first_seq, header.segment_id);
        let path = directory.join(name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let bytes_written = write_segment_header(&mut file, &header)?;
        file.flush()?;
        Ok(Self {
            header,
            path,
            file,
            bytes_written,
            last_seq: 0,
            opened_at: Instant::now(),
            #[cfg(test)]
            poison_next_append: false,
        })
    }

    fn should_rotate(&self, config: &JournalConfig, next_record_bytes: u64) -> bool {
        self.last_seq != 0
            && (self.bytes_written.saturating_add(next_record_bytes) > config.max_segment_bytes
                || self.opened_at.elapsed() >= config.max_segment_age)
    }

    fn append(&mut self, record: &[u8], seq: u64) -> Result<u64, SegmentAppendError> {
        let old_len = self.bytes_written;
        #[cfg(test)]
        let poison_injected = std::mem::take(&mut self.poison_next_append);
        #[cfg(not(test))]
        let poison_injected = false;

        let append_result = if poison_injected {
            let partial_len = (record.len() / 2).max(1).min(record.len());
            match self
                .file
                .write_all(&record[..partial_len])
                .and_then(|()| self.file.flush())
            {
                Ok(()) => Err(io::Error::other("injected append failure")),
                Err(error) => Err(error),
            }
        } else {
            self.file.write_all(record).and_then(|()| self.file.flush())
        };

        if let Err(append_error) = append_result {
            let mut rollback_errors = Vec::new();
            if poison_injected {
                rollback_errors.push("injected truncate/seek rollback failure".to_string());
            } else {
                if let Err(error) = self.file.set_len(old_len) {
                    rollback_errors.push(format!("truncate failed: {error}"));
                }
                if let Err(error) = self.file.seek(SeekFrom::End(0)) {
                    rollback_errors.push(format!("seek failed: {error}"));
                }
            }

            if rollback_errors.is_empty() {
                return Err(SegmentAppendError::Recovered(JournalError::Io(
                    append_error,
                )));
            }
            return Err(SegmentAppendError::Poisoned(
                JournalError::SegmentPoisoned {
                    path: self.path.clone(),
                    append_error: append_error.to_string(),
                    rollback_error: rollback_errors.join("; "),
                },
            ));
        }
        self.bytes_written = self.bytes_written.saturating_add(record.len() as u64);
        self.last_seq = seq;
        Ok(record.len() as u64)
    }

    fn seal(mut self) -> Result<SegmentDescriptor, JournalError> {
        if self.last_seq == 0 {
            drop(self.file);
            fs::remove_file(&self.path)?;
            return Err(JournalError::Corrupt {
                path: self.path,
                message: "cannot seal an empty segment".into(),
            });
        }
        self.file.flush()?;
        self.file.sync_data()?;
        drop(self.file);
        let sealed_path = sealed_path(&self.path, &self.header, self.last_seq);
        fs::rename(&self.path, &sealed_path)?;
        Ok(SegmentDescriptor {
            header: self.header,
            path: sealed_path,
            last_seq: Some(self.last_seq),
            sealed: true,
        })
    }
}

#[derive(Debug, Clone)]
struct SegmentDescriptor {
    header: SegmentHeader,
    path: PathBuf,
    last_seq: Option<u64>,
    sealed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StoredGap {
    slot_id: String,
    epoch: Uuid,
    first_seq: u64,
    last_seq: u64,
    reason: GapReason,
    recorded_wall_time_ns: i64,
}

fn encode_record(event: &TimelineEvent) -> Result<Vec<u8>, JournalError> {
    if event.data.len() > MAX_PAYLOAD_BYTES {
        return Err(JournalError::PayloadTooLarge {
            actual: event.data.len(),
            maximum: MAX_PAYLOAD_BYTES,
        });
    }
    let header = serde_json::to_vec(&DataFrameHeader::from(event))?;
    if header.len() > MAX_HEADER_BYTES {
        return Err(JournalError::HeaderTooLarge {
            actual: header.len(),
            maximum: MAX_HEADER_BYTES,
        });
    }

    let header_len = u32::try_from(header.len()).expect("bounded header length");
    let payload_len = u32::try_from(event.data.len()).expect("bounded payload length");
    let checksum = record_checksum(header_len, payload_len, &header, &event.data);
    let mut record = Vec::with_capacity(RECORD_PREFIX_LEN + header.len() + event.data.len());
    record.extend_from_slice(RECORD_MAGIC);
    record.extend_from_slice(&header_len.to_le_bytes());
    record.extend_from_slice(&payload_len.to_le_bytes());
    record.extend_from_slice(&checksum.to_le_bytes());
    record.extend_from_slice(&header);
    record.extend_from_slice(&event.data);
    Ok(record)
}

fn record_checksum(header_len: u32, payload_len: u32, header: &[u8], payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&header_len.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(header);
    hasher.update(payload);
    hasher.finalize()
}

fn segment_header_encoded_len(header: &SegmentHeader) -> Result<u64, JournalError> {
    let encoded_len = serde_json::to_vec(header)?.len();
    if encoded_len > MAX_SEGMENT_HEADER_BYTES {
        return Err(JournalError::HeaderTooLarge {
            actual: encoded_len,
            maximum: MAX_SEGMENT_HEADER_BYTES,
        });
    }
    Ok((SEGMENT_PREFIX_LEN + encoded_len) as u64)
}

fn write_segment_header(file: &mut File, header: &SegmentHeader) -> Result<u64, JournalError> {
    let encoded = serde_json::to_vec(header)?;
    if encoded.len() > MAX_SEGMENT_HEADER_BYTES {
        return Err(JournalError::HeaderTooLarge {
            actual: encoded.len(),
            maximum: MAX_SEGMENT_HEADER_BYTES,
        });
    }
    let encoded_len = u32::try_from(encoded.len()).expect("bounded segment header");
    let mut hasher = Hasher::new();
    hasher.update(&FORMAT_VERSION.to_le_bytes());
    hasher.update(&encoded_len.to_le_bytes());
    hasher.update(&encoded);
    let checksum = hasher.finalize();

    file.write_all(SEGMENT_MAGIC)?;
    file.write_all(&FORMAT_VERSION.to_le_bytes())?;
    file.write_all(&encoded_len.to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(&encoded)?;
    Ok((SEGMENT_PREFIX_LEN + encoded.len()) as u64)
}

fn read_segment_header(file: &mut File, path: &Path) -> Result<(SegmentHeader, u64), JournalError> {
    file.seek(SeekFrom::Start(0))?;
    let mut prefix = [0_u8; SEGMENT_PREFIX_LEN];
    file.read_exact(&mut prefix)
        .map_err(|error| corrupt_or_io(path, error, "incomplete segment header"))?;
    if &prefix[..8] != SEGMENT_MAGIC {
        return Err(corrupt(path, "invalid segment magic"));
    }
    let version = u32::from_le_bytes(prefix[8..12].try_into().expect("fixed slice"));
    if version != FORMAT_VERSION {
        return Err(corrupt(path, format!("unsupported version {version}")));
    }
    let header_len = u32::from_le_bytes(prefix[12..16].try_into().expect("fixed slice")) as usize;
    if header_len > MAX_SEGMENT_HEADER_BYTES {
        return Err(corrupt(path, "segment header length exceeds limit"));
    }
    let expected_checksum = u32::from_le_bytes(prefix[16..20].try_into().expect("fixed slice"));
    let mut encoded = vec![0; header_len];
    file.read_exact(&mut encoded)
        .map_err(|error| corrupt_or_io(path, error, "incomplete segment metadata"))?;
    let mut hasher = Hasher::new();
    hasher.update(&version.to_le_bytes());
    hasher.update(&(header_len as u32).to_le_bytes());
    hasher.update(&encoded);
    if hasher.finalize() != expected_checksum {
        return Err(corrupt(path, "segment header checksum mismatch"));
    }
    let header: SegmentHeader = serde_json::from_slice(&encoded)
        .map_err(|error| corrupt(path, format!("invalid segment metadata: {error}")))?;
    validate_slot_id(&header.slot_id)?;
    Ok((header, (SEGMENT_PREFIX_LEN + header_len) as u64))
}

enum RecordRead {
    Event(Box<TimelineEvent>),
    Eof,
    Invalid(String),
}

fn read_record(file: &mut File) -> Result<RecordRead, JournalError> {
    let mut prefix = [0_u8; RECORD_PREFIX_LEN];
    let read = read_up_to(file, &mut prefix)?;
    if read == 0 {
        return Ok(RecordRead::Eof);
    }
    if read != RECORD_PREFIX_LEN {
        return Ok(RecordRead::Invalid("incomplete record prefix".into()));
    }
    if &prefix[..8] != RECORD_MAGIC {
        return Ok(RecordRead::Invalid("invalid record magic".into()));
    }
    let header_len = u32::from_le_bytes(prefix[8..12].try_into().expect("fixed slice")) as usize;
    let payload_len = u32::from_le_bytes(prefix[12..16].try_into().expect("fixed slice")) as usize;
    let expected_checksum = u32::from_le_bytes(prefix[16..20].try_into().expect("fixed slice"));
    if header_len > MAX_HEADER_BYTES {
        return Ok(RecordRead::Invalid(
            "record header length exceeds limit".into(),
        ));
    }
    if payload_len > MAX_PAYLOAD_BYTES {
        return Ok(RecordRead::Invalid(
            "record payload length exceeds limit".into(),
        ));
    }
    let mut header = vec![0; header_len];
    if read_up_to(file, &mut header)? != header_len {
        return Ok(RecordRead::Invalid("incomplete record header".into()));
    }
    let mut payload = vec![0; payload_len];
    if read_up_to(file, &mut payload)? != payload_len {
        return Ok(RecordRead::Invalid("incomplete record payload".into()));
    }
    if record_checksum(header_len as u32, payload_len as u32, &header, &payload)
        != expected_checksum
    {
        return Ok(RecordRead::Invalid("record checksum mismatch".into()));
    }
    let decoded: DataFrameHeader = match serde_json::from_slice(&header) {
        Ok(decoded) => decoded,
        Err(error) => {
            return Ok(RecordRead::Invalid(format!(
                "invalid record metadata: {error}"
            )));
        }
    };
    let mut event = decoded.into_event(payload);
    event.durable = true;
    Ok(RecordRead::Event(Box::new(event)))
}

fn read_up_to(file: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut total = 0;
    while total < buffer.len() {
        match file.read(&mut buffer[total..]) {
            Ok(0) => break,
            Ok(read) => total += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(total)
}

fn recover_open_segments(root: &Path) -> Result<(), JournalError> {
    for path in collect_files(root, Some("open"))? {
        recover_open_segment(&path)?;
    }
    Ok(())
}

fn recover_open_segment(path: &Path) -> Result<(), JournalError> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let (header, data_offset) = read_segment_header(&mut file, path)?;
    file.seek(SeekFrom::Start(data_offset))?;
    let mut last_good_offset = data_offset;
    let mut last_seq = None;
    let mut previous = 0;
    loop {
        match read_record(&mut file)? {
            RecordRead::Event(event)
                if event.slot_id == header.slot_id
                    && event.daemon_epoch == header.daemon_epoch
                    && event.seq > previous =>
            {
                previous = event.seq;
                last_seq = Some(event.seq);
                last_good_offset = file.stream_position()?;
            }
            RecordRead::Event(_) => break,
            RecordRead::Eof => break,
            RecordRead::Invalid(reason) => {
                tracing::warn!(path = %path.display(), %reason, "truncating incomplete journal tail");
                break;
            }
        }
    }

    if file.metadata()?.len() != last_good_offset {
        file.set_len(last_good_offset)?;
    }
    file.flush()?;
    file.sync_data()?;
    drop(file);

    let Some(last_seq) = last_seq else {
        fs::remove_file(path)?;
        return Ok(());
    };
    let destination = sealed_path(path, &header, last_seq);
    fs::rename(path, destination)?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct QueryLimits {
    max_scan_bytes: u64,
    max_scan_time: Duration,
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_scan_bytes: MAX_QUERY_SCAN_BYTES,
            max_scan_time: MAX_QUERY_SCAN_TIME,
        }
    }
}

struct QueryBudget {
    limits: QueryLimits,
    started: Instant,
    scanned_bytes: u64,
}

impl QueryBudget {
    fn new(limits: QueryLimits) -> Self {
        Self {
            limits,
            started: Instant::now(),
            scanned_bytes: 0,
        }
    }

    fn add_bytes(&mut self, bytes: u64) {
        self.scanned_bytes = self.scanned_bytes.saturating_add(bytes);
    }

    fn exhausted(&self) -> bool {
        self.scanned_bytes >= self.limits.max_scan_bytes
            || self.started.elapsed() >= self.limits.max_scan_time
    }

    fn ensure_available(&self, phase: &'static str) -> Result<(), JournalError> {
        if self.exhausted() {
            Err(self.error(phase))
        } else {
            Ok(())
        }
    }

    fn charge(&mut self, bytes: u64, phase: &'static str) -> Result<(), JournalError> {
        self.add_bytes(bytes);
        self.ensure_available(phase)
    }

    fn error(&self, phase: &'static str) -> JournalError {
        JournalError::QueryBudgetExceeded {
            phase,
            scanned_bytes: self.scanned_bytes,
            elapsed_ms: self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        }
    }
}

#[derive(Default)]
struct StreamSearchCarry {
    bytes: Vec<u8>,
    generation: Option<u64>,
    next_offset: Option<u64>,
}

impl StreamSearchCarry {
    fn clear(&mut self) {
        self.bytes.clear();
        self.generation = None;
        self.next_offset = None;
    }

    fn matches(&mut self, event: &TimelineEvent, needle: &[u8]) -> bool {
        let continuous = self.generation == Some(event.generation)
            && self.next_offset.is_some()
            && event.stream_offset_start == self.next_offset;
        if !continuous {
            self.bytes.clear();
        }
        let matched = contains_stream_chunk(&event.data, needle, &mut self.bytes);
        self.generation = Some(event.generation);
        self.next_offset = event.stream_offset_end;
        matched
    }
}

#[derive(Debug)]
struct ArchiveAccumulator {
    summary: ArchiveSummary,
}

impl ArchiveAccumulator {
    fn new(header: &SegmentHeader, last_seq: u64, bytes: u64, sealed: bool) -> Self {
        Self {
            summary: ArchiveSummary {
                slot_id: header.slot_id.clone(),
                epoch: header.daemon_epoch,
                first_seq: header.first_seq,
                last_seq,
                first_segment_wall_time_ns: header.created_wall_time_ns,
                last_segment_wall_time_ns: header.created_wall_time_ns,
                segment_count: 1,
                total_bytes: bytes,
                has_open_segment: !sealed,
            },
        }
    }

    fn add(&mut self, header: &SegmentHeader, last_seq: u64, bytes: u64, sealed: bool) {
        self.summary.first_seq = self.summary.first_seq.min(header.first_seq);
        self.summary.last_seq = self.summary.last_seq.max(last_seq);
        self.summary.first_segment_wall_time_ns = self
            .summary
            .first_segment_wall_time_ns
            .min(header.created_wall_time_ns);
        self.summary.last_segment_wall_time_ns = self
            .summary
            .last_segment_wall_time_ns
            .max(header.created_wall_time_ns);
        self.summary.segment_count = self.summary.segment_count.saturating_add(1);
        self.summary.total_bytes = self.summary.total_bytes.saturating_add(bytes);
        self.summary.has_open_segment |= !sealed;
    }
}

fn list_archive_files(
    config: &JournalConfig,
    slot_id: Option<&str>,
) -> Result<ArchiveListResponse, JournalError> {
    list_archive_files_with_limits(config, slot_id, QueryLimits::default())
}

fn list_archive_files_with_limits(
    config: &JournalConfig,
    slot_id: Option<&str>,
    limits: QueryLimits,
) -> Result<ArchiveListResponse, JournalError> {
    let mut budget = QueryBudget::new(limits);
    let search_root = match slot_id {
        Some(slot_id) => slot_dir(&config.root_dir, slot_id),
        None => slots_root(&config.root_dir),
    };
    if !search_root.exists() {
        return Ok(ArchiveListResponse {
            archives: Vec::new(),
            truncated: false,
        });
    }

    let mut archives: HashMap<(String, Uuid), ArchiveAccumulator> = HashMap::new();
    let mut directories = vec![search_root];
    let mut directory_count = 0_usize;
    let mut segment_count = 0_usize;
    let mut incomplete = false;

    while let Some(directory) = directories.pop() {
        budget.ensure_available("archive discovery")?;
        directory_count = directory_count.saturating_add(1);
        if directory_count > MAX_QUERY_SEGMENTS.saturating_mul(2) {
            return Err(JournalError::TooManyQuerySegments {
                maximum: MAX_QUERY_SEGMENTS,
            });
        }
        for entry in fs::read_dir(directory)? {
            budget.ensure_available("archive discovery")?;
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let path = entry.path();
            let extension = path.extension().and_then(|value| value.to_str());
            let sealed = extension == Some("slog");
            if !sealed && extension != Some("open") {
                continue;
            }
            segment_count = segment_count.saturating_add(1);
            if segment_count > MAX_QUERY_SEGMENTS {
                return Err(JournalError::TooManyQuerySegments {
                    maximum: MAX_QUERY_SEGMENTS,
                });
            }

            let metadata = match fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let (header, data_offset) = match read_segment_header(&mut file, &path) {
                Ok(value) => value,
                Err(error) => {
                    incomplete = true;
                    tracing::warn!(path = %path.display(), %error, "skipping unreadable segment in archive catalog");
                    continue;
                }
            };
            budget.charge(data_offset, "archive discovery")?;
            if slot_id.is_some_and(|expected| header.slot_id != expected)
                || !path.starts_with(epoch_dir(
                    &config.root_dir,
                    &header.slot_id,
                    header.daemon_epoch,
                ))
            {
                incomplete = true;
                tracing::warn!(path = %path.display(), "skipping misplaced segment in archive catalog");
                continue;
            }

            let last_seq = if sealed {
                parse_sealed_seq_range(&path)
                    .filter(|(first_seq, _)| *first_seq == header.first_seq)
                    .map(|(_, last_seq)| last_seq)
            } else {
                None
            };
            let last_seq = match last_seq {
                Some(last_seq) if last_seq >= header.first_seq => Some(last_seq),
                _ => scan_last_seq_metadata(&mut file, &path, &header, data_offset, &mut budget)?,
            };
            let Some(last_seq) = last_seq else {
                // A newly created `.open` file can be observed before its first
                // complete record. It is not yet a usable archive.
                continue;
            };

            let key = (header.slot_id.clone(), header.daemon_epoch);
            match archives.entry(key) {
                Entry::Vacant(entry) => {
                    entry.insert(ArchiveAccumulator::new(
                        &header,
                        last_seq,
                        metadata.len(),
                        sealed,
                    ));
                }
                Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .add(&header, last_seq, metadata.len(), sealed);
                }
            }
        }
    }

    let mut archives = archives
        .into_values()
        .map(|archive| archive.summary)
        .collect::<Vec<_>>();
    archives.sort_by(|left, right| {
        right
            .last_segment_wall_time_ns
            .cmp(&left.last_segment_wall_time_ns)
            .then_with(|| left.slot_id.cmp(&right.slot_id))
            .then_with(|| right.epoch.cmp(&left.epoch))
    });
    if archives.len() > MAX_ARCHIVE_SUMMARIES {
        archives.truncate(MAX_ARCHIVE_SUMMARIES);
        incomplete = true;
    }
    Ok(ArchiveListResponse {
        archives,
        truncated: incomplete,
    })
}

/// Reads only record metadata and validates payload CRC in fixed-size chunks.
/// It never materializes a full TimelineEvent or raw serial payload.
fn scan_last_seq_metadata(
    file: &mut File,
    path: &Path,
    segment: &SegmentHeader,
    data_offset: u64,
    budget: &mut QueryBudget,
) -> Result<Option<u64>, JournalError> {
    file.seek(SeekFrom::Start(data_offset))?;
    let mut last_seq = None;
    loop {
        budget.ensure_available("archive open-segment scan")?;
        let record_start = file.stream_position()?;
        let mut prefix = [0_u8; RECORD_PREFIX_LEN];
        let read = read_up_to(file, &mut prefix)?;
        budget.charge(read as u64, "archive open-segment scan")?;
        if read == 0 {
            break;
        }
        if read != RECORD_PREFIX_LEN || &prefix[..8] != RECORD_MAGIC {
            break;
        }
        let header_len = u32::from_le_bytes(prefix[8..12].try_into().expect("fixed slice"));
        let payload_len = u32::from_le_bytes(prefix[12..16].try_into().expect("fixed slice"));
        let expected_checksum = u32::from_le_bytes(prefix[16..20].try_into().expect("fixed slice"));
        if header_len as usize > MAX_HEADER_BYTES || payload_len as usize > MAX_PAYLOAD_BYTES {
            break;
        }

        let mut encoded_header = vec![0_u8; header_len as usize];
        let read = read_up_to(file, &mut encoded_header)?;
        budget.charge(read as u64, "archive open-segment scan")?;
        if read != encoded_header.len() {
            break;
        }
        let mut hasher = Hasher::new();
        hasher.update(&header_len.to_le_bytes());
        hasher.update(&payload_len.to_le_bytes());
        hasher.update(&encoded_header);

        let mut remaining = payload_len as usize;
        let mut buffer = [0_u8; 32 * 1024];
        while remaining > 0 {
            let wanted = remaining.min(buffer.len());
            let read = read_up_to(file, &mut buffer[..wanted])?;
            budget.charge(read as u64, "archive open-segment scan")?;
            if read != wanted {
                return Ok(last_seq);
            }
            hasher.update(&buffer[..read]);
            remaining -= read;
        }
        if hasher.finalize() != expected_checksum {
            break;
        }
        let header: DataFrameHeader = match serde_json::from_slice(&encoded_header) {
            Ok(header) => header,
            Err(_) => break,
        };
        if header.slot_id != segment.slot_id
            || header.daemon_epoch != segment.daemon_epoch
            || last_seq.is_some_and(|previous| header.seq <= previous)
        {
            return Err(corrupt(path, "record identity or sequence is invalid"));
        }
        last_seq = Some(header.seq);

        // Protect against a future codec bug that fails to advance the cursor.
        if file.stream_position()? <= record_start {
            return Err(corrupt(path, "record scan made no progress"));
        }
    }
    Ok(last_seq)
}

fn query_files(
    config: &JournalConfig,
    slot_id: &str,
    query: &EventQuery,
) -> Result<EventQueryResponse, JournalError> {
    query_files_with_limits(config, slot_id, query, QueryLimits::default())
}

fn query_files_with_limits(
    config: &JournalConfig,
    slot_id: &str,
    query: &EventQuery,
    limits: QueryLimits,
) -> Result<EventQueryResponse, JournalError> {
    let mut budget = QueryBudget::new(limits);
    if let (Some(after), Some(before)) = (query.after_wall_time_ns, query.before_wall_time_ns)
        && after >= before
    {
        return Err(JournalError::InvalidConfig(
            "after_wall_time_ns must be less than before_wall_time_ns".into(),
        ));
    }
    if query
        .contains
        .as_ref()
        .is_some_and(|value| value.len() > 4_096)
    {
        return Err(JournalError::InvalidConfig(
            "contains must not exceed 4096 UTF-8 bytes".into(),
        ));
    }
    if query.actor_id.as_ref().is_some_and(|value| {
        value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
    }) {
        return Err(JournalError::InvalidConfig(
            "actor_id must contain 1..=256 non-control UTF-8 bytes".into(),
        ));
    }
    if query.epoch.is_none() && query.after_seq.is_some_and(|after| after > 0) {
        return Err(JournalError::InvalidConfig(
            "epoch is required when after_seq is greater than zero".into(),
        ));
    }

    let limit_events = query
        .limit_events
        .unwrap_or(DEFAULT_QUERY_EVENTS)
        .clamp(1, MAX_QUERY_EVENTS);
    let limit_bytes = query
        .limit_bytes
        .unwrap_or(DEFAULT_QUERY_BYTES)
        .clamp(1, MAX_QUERY_BYTES);
    let contains = query.contains.as_deref().map(str::as_bytes);
    let mut descriptors = discover_segments_for_query(&config.root_dir, slot_id, &mut budget)?;

    // Sequence numbers are only meaningful within one daemon epoch. An
    // omitted epoch is accepted only for the first page and means the latest
    // epoch for this slot; continuation requests must carry the returned epoch.
    let selected_epoch = query.epoch.or_else(|| {
        descriptors
            .iter()
            .max_by_key(|segment| {
                (
                    segment.header.created_wall_time_ns,
                    segment.header.first_seq,
                    segment.header.segment_id,
                )
            })
            .map(|segment| segment.header.daemon_epoch)
    });
    let selected_epoch = match selected_epoch {
        Some(epoch) => Some(epoch),
        None => find_latest_gap_epoch(&config.root_dir, slot_id, &mut budget)?,
    };
    descriptors
        .retain(|segment| selected_epoch.is_some_and(|epoch| segment.header.daemon_epoch == epoch));
    descriptors.sort_by_key(|segment| (segment.header.first_seq, segment.header.segment_id));

    let mut gaps = match selected_epoch {
        Some(epoch) => load_query_gap_ranges(
            &config.root_dir,
            slot_id,
            epoch,
            query.after_seq,
            &mut budget,
        )?,
        None => Vec::new(),
    };

    let mut events = Vec::new();
    let mut bytes = 0_usize;
    let mut truncated = false;
    let first_available_seq = descriptors
        .iter()
        .filter(|segment| segment.last_seq.is_some())
        .map(|segment| segment.header.first_seq)
        .min();
    let mut last_scanned_seq = query.after_seq;
    let seed_segment_id = contains.and_then(|_| {
        query.after_seq.and_then(|after| {
            descriptors
                .iter()
                .filter(|segment| segment.last_seq.is_some_and(|last| last <= after))
                .max_by_key(|segment| {
                    (
                        segment.last_seq.unwrap_or(0),
                        segment.header.first_seq,
                        segment.header.segment_id,
                    )
                })
                .map(|segment| segment.header.segment_id)
        })
    });
    let mut rx_search_carry = StreamSearchCarry::default();
    let mut tx_search_carry = StreamSearchCarry::default();
    let mut previous_scanned_seq = None;
    // Only records after the requested cursor form this page's authoritative
    // continuity chain. Records at or before the cursor may be scanned to seed
    // a cross-record text search, but must not move the chain backwards.
    let sequence_floor = query.after_seq.unwrap_or(0);
    let mut previous_contiguous_seq = sequence_floor;

    'segments: for segment in descriptors {
        let fully_consumed = query
            .after_seq
            .is_some_and(|after| segment.last_seq.is_some_and(|last| last <= after));
        if fully_consumed && seed_segment_id != Some(segment.header.segment_id) {
            continue;
        }
        if budget.exhausted() {
            if cursor_progressed(last_scanned_seq, query.after_seq) {
                truncated = true;
                break;
            }
            return Err(budget.error("event scan"));
        }
        let mut file = match File::open(&segment.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let (disk_header, data_offset) = read_segment_header(&mut file, &segment.path)?;
        budget.add_bytes(data_offset);
        if budget.exhausted() {
            if cursor_progressed(last_scanned_seq, query.after_seq) {
                truncated = true;
                break;
            }
            return Err(budget.error("event scan"));
        }
        if disk_header.slot_id != slot_id || disk_header.daemon_epoch != segment.header.daemon_epoch
        {
            return Err(corrupt(
                &segment.path,
                "segment identity changed while querying",
            ));
        }
        file.seek(SeekFrom::Start(data_offset))?;
        let mut last_good_seq = None;
        loop {
            if budget.exhausted() {
                if cursor_progressed(last_scanned_seq, query.after_seq) {
                    truncated = true;
                    break 'segments;
                }
                return Err(budget.error("event scan"));
            }
            let record_start = file.stream_position()?;
            let record = read_record(&mut file)?;
            budget.add_bytes(file.stream_position()?.saturating_sub(record_start));
            let scan_limit_hit = budget.exhausted();
            let event = match record {
                RecordRead::Event(event) => event,
                RecordRead::Eof => {
                    if scan_limit_hit {
                        if cursor_progressed(last_scanned_seq, query.after_seq) {
                            truncated = true;
                            break 'segments;
                        }
                        return Err(budget.error("event scan"));
                    }
                    break;
                }
                RecordRead::Invalid(reason) => {
                    rx_search_carry.clear();
                    tx_search_carry.clear();
                    if let Some(last_seq) = segment.last_seq {
                        let first_seq = last_good_seq
                            .map(|seq: u64| seq.saturating_add(1))
                            .unwrap_or(segment.header.first_seq);
                        if first_seq <= last_seq {
                            push_gap_bounded(
                                &mut gaps,
                                GapRange {
                                    epoch: segment.header.daemon_epoch,
                                    first_seq,
                                    last_seq,
                                    reason: GapReason::Corruption,
                                },
                            )?;
                        }
                    }
                    tracing::warn!(path = %segment.path.display(), %reason, "journal query stopped at corrupt record");
                    if !segment.sealed {
                        // A concurrent append can expose a temporary partial tail.
                        // Make the non-authoritative result explicit so callers
                        // retry from their last cursor.
                        truncated = true;
                    }
                    if scan_limit_hit {
                        if cursor_progressed(last_scanned_seq, query.after_seq) {
                            truncated = true;
                            break 'segments;
                        }
                        return Err(budget.error("event scan"));
                    }
                    break;
                }
            };
            if event.slot_id != slot_id || event.daemon_epoch != segment.header.daemon_epoch {
                return Err(corrupt(
                    &segment.path,
                    "record identity does not match segment",
                ));
            }
            last_good_seq = Some(event.seq);

            if event.seq > sequence_floor {
                let expected = previous_contiguous_seq.saturating_add(1);
                if event.seq > expected {
                    push_uncovered_gap(
                        &mut gaps,
                        GapRange {
                            epoch: event.daemon_epoch,
                            first_seq: expected,
                            last_seq: event.seq - 1,
                            reason: GapReason::SequenceDiscontinuity,
                        },
                    )?;
                    rx_search_carry.clear();
                    tx_search_carry.clear();
                }
                previous_contiguous_seq = previous_contiguous_seq.max(event.seq);
            }

            if previous_scanned_seq
                .is_some_and(|previous: u64| event.seq != previous.saturating_add(1))
            {
                rx_search_carry.clear();
                tx_search_carry.clear();
            }
            previous_scanned_seq = Some(event.seq);
            if event_breaks_stream(event.kind) {
                rx_search_carry.clear();
                tx_search_carry.clear();
            }

            let scope_match = event_matches_query_scope(event.as_ref(), query);
            let contains_match = if !scope_match {
                match event.direction {
                    serial_protocol::Direction::Rx => rx_search_carry.clear(),
                    serial_protocol::Direction::Tx => tx_search_carry.clear(),
                    serial_protocol::Direction::None => {}
                }
                false
            } else {
                contains.is_none_or(|needle| match event.direction {
                    serial_protocol::Direction::Rx => {
                        rx_search_carry.matches(event.as_ref(), needle)
                    }
                    serial_protocol::Direction::Tx => {
                        tx_search_carry.matches(event.as_ref(), needle)
                    }
                    serial_protocol::Direction::None => contains_bytes(&event.data, needle),
                })
            };
            let eligible = query.after_seq.is_none_or(|after| event.seq > after)
                && scope_match
                && contains_match;
            if !eligible {
                last_scanned_seq =
                    Some(last_scanned_seq.map_or(event.seq, |seq| seq.max(event.seq)));
                if scan_limit_hit {
                    if cursor_progressed(last_scanned_seq, query.after_seq) {
                        truncated = true;
                        break 'segments;
                    }
                    return Err(budget.error("event scan"));
                }
                continue;
            }

            let event_bytes = event
                .data
                .len()
                .saturating_add(serde_json::to_vec(&DataFrameHeader::from(event.as_ref()))?.len());
            if events.len() >= limit_events || bytes.saturating_add(event_bytes) > limit_bytes {
                if events.is_empty() {
                    // The byte limit is a response-size target, not a reason to
                    // make pagination permanently stall on one valid record.
                } else {
                    truncated = true;
                    break 'segments;
                }
            }
            bytes = bytes.saturating_add(event_bytes);
            last_scanned_seq = Some(last_scanned_seq.map_or(event.seq, |seq| seq.max(event.seq)));
            events.push(*event);
            if scan_limit_hit {
                truncated = true;
                break 'segments;
            }
        }
    }

    if let (Some(epoch), Some(after), Some(first_available)) =
        (selected_epoch, query.after_seq, first_available_seq)
    {
        let missing_first = after.saturating_add(1);
        if missing_first < first_available {
            push_uncovered_gap(
                &mut gaps,
                GapRange {
                    epoch,
                    first_seq: missing_first,
                    last_seq: first_available - 1,
                    reason: GapReason::Retention,
                },
            )?;
        }
    }
    gaps = merge_gap_ranges(gaps);
    if gaps.len() > MAX_QUERY_GAPS {
        return Err(JournalError::TooManyQueryGaps {
            maximum: MAX_QUERY_GAPS,
        });
    }

    let next_cursor = selected_epoch
        .zip(last_scanned_seq)
        .map(|(epoch, after_seq)| Cursor { epoch, after_seq });
    Ok(EventQueryResponse {
        events,
        next_cursor,
        truncated,
        first_available_seq,
        gaps,
    })
}

fn cursor_progressed(last_scanned: Option<u64>, after_seq: Option<u64>) -> bool {
    match (last_scanned, after_seq) {
        (Some(last), Some(after)) => last > after,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

fn event_matches_query_scope(event: &TimelineEvent, query: &EventQuery) -> bool {
    !query
        .after_wall_time_ns
        .is_some_and(|after| event.wall_time_ns <= after)
        && !query
            .before_wall_time_ns
            .is_some_and(|before| event.wall_time_ns >= before)
        && !query
            .direction
            .is_some_and(|direction| event.direction != direction)
        && !query.kind.is_some_and(|kind| event.kind != kind)
        && !query.actor_id.as_ref().is_some_and(|actor_id| {
            event.actor.as_ref().map(|actor| actor.id.as_str()) != Some(actor_id.as_str())
        })
        && !query
            .run_id
            .is_some_and(|run_id| event.run_id != Some(run_id))
        && !query
            .operation_id
            .is_some_and(|operation_id| event.operation_id != Some(operation_id))
}

fn event_breaks_stream(kind: serial_protocol::EventKind) -> bool {
    matches!(
        kind,
        serial_protocol::EventKind::SerialOpening
            | serial_protocol::EventKind::SerialOpened
            | serial_protocol::EventKind::SerialOpenFailed
            | serial_protocol::EventKind::SerialClosed
            | serial_protocol::EventKind::SlotReconfigured
            | serial_protocol::EventKind::SlotRemoved
            | serial_protocol::EventKind::LoggingDegraded
            | serial_protocol::EventKind::Gap
    )
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    needle.is_empty()
        || haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn contains_stream_chunk(data: &[u8], needle: &[u8], carry: &mut Vec<u8>) -> bool {
    if needle.is_empty() {
        carry.clear();
        return true;
    }
    let mut combined = Vec::with_capacity(carry.len().saturating_add(data.len()));
    combined.extend_from_slice(carry);
    combined.extend_from_slice(data);
    let matched = contains_bytes(&combined, needle);
    let keep = needle.len().saturating_sub(1).min(combined.len());
    carry.clear();
    carry.extend_from_slice(&combined[combined.len() - keep..]);
    matched
}

fn push_gap_bounded(gaps: &mut Vec<GapRange>, gap: GapRange) -> Result<(), JournalError> {
    gaps.push(gap);
    if gaps.len() >= MAX_QUERY_GAPS.saturating_mul(2) {
        let merged = merge_gap_ranges(std::mem::take(gaps));
        if merged.len() > MAX_QUERY_GAPS {
            return Err(JournalError::TooManyQueryGaps {
                maximum: MAX_QUERY_GAPS,
            });
        }
        *gaps = merged;
    }
    Ok(())
}

/// Adds only portions of `candidate` not already explained by a more specific
/// retained gap. This keeps query-derived conservative gaps from duplicating a
/// writer, retention, or corruption range for the same missing sequence.
fn push_uncovered_gap(gaps: &mut Vec<GapRange>, candidate: GapRange) -> Result<(), JournalError> {
    if candidate.first_seq > candidate.last_seq {
        return Ok(());
    }
    let mut covered = gaps
        .iter()
        .filter(|gap| {
            gap.epoch == candidate.epoch
                && gap.last_seq >= candidate.first_seq
                && gap.first_seq <= candidate.last_seq
        })
        .map(|gap| (gap.first_seq, gap.last_seq))
        .collect::<Vec<_>>();
    covered.sort_unstable();

    let mut next = candidate.first_seq;
    for (first, last) in covered {
        if first > next {
            push_gap_bounded(
                gaps,
                GapRange {
                    epoch: candidate.epoch,
                    first_seq: next,
                    last_seq: candidate.last_seq.min(first - 1),
                    reason: candidate.reason,
                },
            )?;
        }
        if last >= candidate.last_seq {
            return Ok(());
        }
        next = next.max(last.saturating_add(1));
        if next > candidate.last_seq {
            return Ok(());
        }
    }
    push_gap_bounded(
        gaps,
        GapRange {
            epoch: candidate.epoch,
            first_seq: next,
            last_seq: candidate.last_seq,
            reason: candidate.reason,
        },
    )
}

fn merge_gap_ranges(mut gaps: Vec<GapRange>) -> Vec<GapRange> {
    gaps.sort_by_key(|gap| {
        (
            gap.epoch,
            gap_reason_rank(gap.reason),
            gap.first_seq,
            gap.last_seq,
        )
    });
    let mut merged: Vec<GapRange> = Vec::with_capacity(gaps.len());
    for gap in gaps {
        let should_merge = merged.last().is_some_and(|previous| {
            previous.epoch == gap.epoch
                && previous.reason == gap.reason
                && gap.first_seq <= previous.last_seq.saturating_add(1)
        });
        if should_merge {
            let previous = merged.last_mut().expect("checked above");
            previous.last_seq = previous.last_seq.max(gap.last_seq);
        } else {
            merged.push(gap);
        }
    }
    merged
}

fn gap_reason_rank(reason: GapReason) -> u8 {
    match reason {
        GapReason::EpochChanged => 0,
        GapReason::RingEvicted => 1,
        GapReason::Retention => 2,
        GapReason::Corruption => 3,
        GapReason::LoggingFault => 4,
        GapReason::SequenceDiscontinuity => 5,
    }
}

fn discover_segments(
    root: &Path,
    slot_id: Option<&str>,
) -> Result<Vec<SegmentDescriptor>, JournalError> {
    let search_root = match slot_id {
        Some(slot_id) => slot_dir(root, slot_id),
        None => slots_root(root),
    };
    if !search_root.exists() {
        return Ok(Vec::new());
    }
    let mut result = Vec::new();
    for path in collect_files(&search_root, None)? {
        let extension = path.extension().and_then(|value| value.to_str());
        let sealed = extension == Some("slog");
        if !sealed && extension != Some("open") {
            continue;
        }
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let (header, _) = read_segment_header(&mut file, &path)?;
        if slot_id.is_some_and(|expected| header.slot_id != expected) {
            return Err(corrupt(
                &path,
                "segment is stored under the wrong slot directory",
            ));
        }
        let last_seq = if sealed {
            parse_sealed_last_seq(&path).or_else(|| scan_last_seq(&path).ok().flatten())
        } else {
            scan_last_seq(&path)?
        };
        result.push(SegmentDescriptor {
            header,
            path,
            last_seq,
            sealed,
        });
    }
    Ok(result)
}

fn discover_segments_for_query(
    root: &Path,
    slot_id: &str,
    budget: &mut QueryBudget,
) -> Result<Vec<SegmentDescriptor>, JournalError> {
    let search_root = slot_dir(root, slot_id);
    if !search_root.exists() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut directories = vec![search_root];
    let mut directory_count = 0_usize;
    while let Some(directory) = directories.pop() {
        budget.ensure_available("segment discovery")?;
        directory_count = directory_count.saturating_add(1);
        if directory_count > MAX_QUERY_SEGMENTS.saturating_mul(2) {
            return Err(JournalError::TooManyQuerySegments {
                maximum: MAX_QUERY_SEGMENTS,
            });
        }

        for entry in fs::read_dir(directory)? {
            budget.ensure_available("segment discovery")?;
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                continue;
            }

            let path = entry.path();
            let extension = path.extension().and_then(|value| value.to_str());
            let sealed = extension == Some("slog");
            if !sealed && extension != Some("open") {
                continue;
            }
            if result.len() >= MAX_QUERY_SEGMENTS {
                return Err(JournalError::TooManyQuerySegments {
                    maximum: MAX_QUERY_SEGMENTS,
                });
            }

            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            let (header, data_offset) = read_segment_header(&mut file, &path)?;
            budget.charge(data_offset, "segment discovery")?;
            if header.slot_id != slot_id {
                return Err(corrupt(
                    &path,
                    "segment is stored under the wrong slot directory",
                ));
            }
            let last_seq = if sealed {
                match parse_sealed_last_seq(&path) {
                    Some(last_seq) => Some(last_seq),
                    None => {
                        scan_last_seq_for_query(&mut file, &path, &header, data_offset, budget)?
                    }
                }
            } else {
                scan_last_seq_for_query(&mut file, &path, &header, data_offset, budget)?
            };
            result.push(SegmentDescriptor {
                header,
                path,
                last_seq,
                sealed,
            });
        }
    }
    Ok(result)
}

fn scan_last_seq_for_query(
    file: &mut File,
    path: &Path,
    header: &SegmentHeader,
    data_offset: u64,
    budget: &mut QueryBudget,
) -> Result<Option<u64>, JournalError> {
    file.seek(SeekFrom::Start(data_offset))?;
    let mut last = None;
    loop {
        budget.ensure_available("segment discovery")?;
        let record_start = file.stream_position()?;
        let record = read_record(file)?;
        budget.charge(
            file.stream_position()?.saturating_sub(record_start),
            "segment discovery",
        )?;
        match record {
            RecordRead::Event(event)
                if event.slot_id == header.slot_id && event.daemon_epoch == header.daemon_epoch =>
            {
                last = Some(event.seq);
            }
            RecordRead::Event(_) => {
                return Err(corrupt(path, "record identity does not match segment"));
            }
            RecordRead::Eof | RecordRead::Invalid(_) => break,
        }
    }
    Ok(last)
}

fn scan_last_seq(path: &Path) -> Result<Option<u64>, JournalError> {
    let mut file = File::open(path)?;
    let (header, data_offset) = read_segment_header(&mut file, path)?;
    file.seek(SeekFrom::Start(data_offset))?;
    let mut last = None;
    loop {
        match read_record(&mut file)? {
            RecordRead::Event(event)
                if event.slot_id == header.slot_id && event.daemon_epoch == header.daemon_epoch =>
            {
                last = Some(event.seq);
            }
            RecordRead::Event(_) | RecordRead::Eof | RecordRead::Invalid(_) => break,
        }
    }
    Ok(last)
}

fn parse_sealed_last_seq(path: &Path) -> Option<u64> {
    parse_sealed_seq_range(path).map(|(_, last_seq)| last_seq)
}

fn parse_sealed_seq_range(path: &Path) -> Option<(u64, u64)> {
    let stem = path.file_stem()?.to_str()?;
    let mut parts = stem.splitn(3, '-');
    let first = parts.next()?.parse::<u64>().ok()?;
    let last = parts.next()?.parse::<u64>().ok()?;
    Some((first, last))
}

fn sealed_path(open_path: &Path, header: &SegmentHeader, last_seq: u64) -> PathBuf {
    open_path.with_file_name(format!(
        "{:020}-{:020}-{}.slog",
        header.first_seq, last_seq, header.segment_id
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GapLedgerAppend {
    appended_bytes: u64,
    truncated_bytes: u64,
}

fn append_gap_ledger(root: &Path, gap: &StoredGap) -> Result<GapLedgerAppend, JournalError> {
    fs::create_dir_all(root)?;
    let path = root.join(GAP_LEDGER_NAME);
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)?;
    let truncated_bytes = recover_gap_ledger_file(&mut file, &path)?;
    let mut encoded = serde_json::to_vec(gap)?;
    encoded.push(b'\n');
    file.seek(SeekFrom::End(0))?;
    file.write_all(&encoded)?;
    file.flush()?;
    file.sync_data()?;
    Ok(GapLedgerAppend {
        appended_bytes: encoded.len() as u64,
        truncated_bytes,
    })
}

fn recover_gap_ledger(root: &Path) -> Result<u64, JournalError> {
    let path = root.join(GAP_LEDGER_NAME);
    let mut file = match OpenOptions::new().read(true).write(true).open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    recover_gap_ledger_file(&mut file, &path)
}

/// Restores the ledger to the end of its last complete, semantically valid
/// JSONL record. A torn final write is discarded before any later append can
/// concatenate with it and hide the new gap from readers.
fn recover_gap_ledger_file(file: &mut File, path: &Path) -> Result<u64, JournalError> {
    let original_len = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;
    let mut last_valid_end = 0_u64;
    let mut offset = 0_u64;
    let mut discarding_oversized_line = false;
    {
        let mut reader = BufReader::new(&mut *file);
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = (&mut reader)
                .take((MAX_GAP_LEDGER_LINE_BYTES + 1) as u64)
                .read_until(b'\n', &mut line)?;
            if read == 0 {
                break;
            }
            offset = offset.saturating_add(read as u64);
            let complete = line.last() == Some(&b'\n');
            if discarding_oversized_line || read > MAX_GAP_LEDGER_LINE_BYTES {
                discarding_oversized_line = !complete;
                continue;
            }
            if !complete {
                break;
            }
            line.pop();
            let valid = serde_json::from_slice::<StoredGap>(&line)
                .ok()
                .is_some_and(|gap| {
                    validate_slot_id(&gap.slot_id).is_ok() && gap.first_seq <= gap.last_seq
                });
            if valid {
                last_valid_end = offset;
            }
        }
    }

    let truncated_bytes = original_len.saturating_sub(last_valid_end);
    if truncated_bytes > 0 {
        file.set_len(last_valid_end)?;
        file.flush()?;
        file.sync_data()?;
        tracing::warn!(
            path = %path.display(),
            truncated_bytes,
            "recovered gap ledger to its last complete valid record"
        );
    }
    file.seek(SeekFrom::End(0))?;
    Ok(truncated_bytes)
}

fn visit_gap_ledger(
    root: &Path,
    budget: &mut QueryBudget,
    mut visit: impl FnMut(StoredGap) -> Result<(), JournalError>,
) -> Result<(), JournalError> {
    let path = root.join(GAP_LEDGER_NAME);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    loop {
        budget.ensure_available("gap ledger")?;
        line.clear();
        let read = (&mut reader)
            .take((MAX_GAP_LEDGER_LINE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        budget.charge(read as u64, "gap ledger")?;
        if read > MAX_GAP_LEDGER_LINE_BYTES {
            return Err(corrupt(&path, "gap ledger line exceeds size limit"));
        }
        if line.last() == Some(&b'\n') {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        match serde_json::from_slice::<StoredGap>(&line) {
            Ok(gap) => visit(gap)?,
            Err(error) => {
                // A process can be killed while appending the final ledger line.
                // Ignore only malformed lines and preserve every complete entry.
                tracing::warn!(path = %path.display(), %error, "ignoring incomplete gap ledger line");
            }
        }
    }
    Ok(())
}

fn find_latest_gap_epoch(
    root: &Path,
    slot_id: &str,
    budget: &mut QueryBudget,
) -> Result<Option<Uuid>, JournalError> {
    let mut latest = None;
    visit_gap_ledger(root, budget, |gap| {
        if gap.slot_id == slot_id {
            let candidate = (gap.recorded_wall_time_ns, gap.epoch);
            if latest.is_none_or(|current| candidate > current) {
                latest = Some(candidate);
            }
        }
        Ok(())
    })?;
    Ok(latest.map(|(_, epoch)| epoch))
}

fn load_query_gap_ranges(
    root: &Path,
    slot_id: &str,
    epoch: Uuid,
    after_seq: Option<u64>,
    budget: &mut QueryBudget,
) -> Result<Vec<GapRange>, JournalError> {
    let mut gaps = Vec::new();
    visit_gap_ledger(root, budget, |gap| {
        if gap.slot_id == slot_id
            && gap.epoch == epoch
            && after_seq.is_none_or(|after| gap.last_seq > after)
        {
            push_gap_bounded(
                &mut gaps,
                GapRange {
                    epoch: gap.epoch,
                    first_seq: gap.first_seq,
                    last_seq: gap.last_seq,
                    reason: gap.reason,
                },
            )?;
        }
        Ok(())
    })?;
    let gaps = merge_gap_ranges(gaps);
    if gaps.len() > MAX_QUERY_GAPS {
        return Err(JournalError::TooManyQueryGaps {
            maximum: MAX_QUERY_GAPS,
        });
    }
    Ok(gaps)
}

fn collect_files(root: &Path, extension: Option<&str>) -> Result<Vec<PathBuf>, JournalError> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    let mut directories = vec![root.to_path_buf()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file()
                && extension.is_none_or(|expected| {
                    entry.path().extension().and_then(|value| value.to_str()) == Some(expected)
                })
            {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
}

fn directory_size(root: &Path) -> Result<u64, JournalError> {
    let mut total = 0_u64;
    for path in collect_files(root, None)? {
        match fs::metadata(path) {
            Ok(metadata) => total = total.saturating_add(metadata.len()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(total)
}

fn slots_root(root: &Path) -> PathBuf {
    root.join("slots")
}

fn slot_dir(root: &Path, slot_id: &str) -> PathBuf {
    slots_root(root).join(format!("slot-{}", hex_encode(slot_id.as_bytes())))
}

fn epoch_dir(root: &Path, slot_id: &str, epoch: Uuid) -> PathBuf {
    slot_dir(root, slot_id).join(epoch.to_string())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn validate_slot_id(slot_id: &str) -> Result<(), JournalError> {
    if slot_id.is_empty() || slot_id.len() > MAX_SLOT_ID_BYTES {
        Err(JournalError::InvalidSlotId)
    } else {
        Ok(())
    }
}

fn validate_root(root: &Path) -> Result<(), JournalError> {
    if root.as_os_str().is_empty() {
        Err(JournalError::InvalidConfig(
            "journal root directory cannot be empty".into(),
        ))
    } else {
        Ok(())
    }
}

fn now_wall_time_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_nanos().min(i64::MAX as u128) as i64),
    }
}

fn corrupt(path: &Path, message: impl Into<String>) -> JournalError {
    JournalError::Corrupt {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

fn corrupt_or_io(path: &Path, error: io::Error, message: &str) -> JournalError {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        corrupt(path, message)
    } else {
        JournalError::Io(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{Actor, ActorKind, Direction, EventKind};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn test_config(temp: &TempDir) -> JournalConfig {
        let mut config = JournalConfig::new(temp.path().join("journal"));
        config.max_segment_bytes = 4 * 1024;
        config.max_segment_age = Duration::from_secs(60 * 60);
        config.max_total_bytes = 1024 * 1024;
        config.cleanup_low_watermark = 0.75;
        config
    }

    fn event(epoch: Uuid, seq: u64, direction: Direction, data: Vec<u8>) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: 1_000 + seq as i64,
            monotonic_time_ns: seq * 10,
            kind: match direction {
                Direction::Rx => EventKind::Rx,
                Direction::Tx => EventKind::Tx,
                Direction::None => EventKind::Checkpoint,
            },
            direction,
            actor: (direction == Direction::Tx).then(|| Actor {
                id: "human:test".into(),
                label: "test".into(),
                kind: ActorKind::Human,
            }),
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(0),
            stream_offset_end: Some(data.len() as u64),
            data,
            metadata: BTreeMap::new(),
            durable: false,
        }
    }

    fn query(epoch: Uuid) -> EventQuery {
        EventQuery {
            epoch: Some(epoch),
            after_seq: None,
            before_wall_time_ns: None,
            after_wall_time_ns: None,
            direction: None,
            kind: None,
            actor_id: None,
            run_id: None,
            operation_id: None,
            contains: None,
            limit_events: Some(1_000),
            limit_bytes: Some(4 * 1024 * 1024),
        }
    }

    fn write_sealed_test_segment(
        config: &JournalConfig,
        header: SegmentHeader,
        mut timeline_event: TimelineEvent,
    ) -> PathBuf {
        timeline_event.durable = true;
        let seq = timeline_event.seq;
        let record = encode_record(&timeline_event).unwrap();
        let mut segment = OpenSegment::create_with_header(&config.root_dir, header).unwrap();
        segment.append(&record, seq).unwrap();
        segment.seal().unwrap().path
    }

    fn write_sealed_test_events(
        config: &JournalConfig,
        header: SegmentHeader,
        timeline_events: Vec<TimelineEvent>,
    ) -> PathBuf {
        let mut segment = OpenSegment::create_with_header(&config.root_dir, header).unwrap();
        for mut timeline_event in timeline_events {
            timeline_event.durable = true;
            let seq = timeline_event.seq;
            let record = encode_record(&timeline_event).unwrap();
            segment.append(&record, seq).unwrap();
        }
        segment.seal().unwrap().path
    }

    #[tokio::test]
    async fn archive_catalog_lists_epochs_filters_slots_and_tracks_open_segments() {
        let temporary = tempfile::tempdir().unwrap();
        let config = test_config(&temporary);
        let manager = JournalManager::open(config.clone()).unwrap();
        let handle = manager.handle();
        let first_epoch = Uuid::new_v4();
        let second_epoch = Uuid::new_v4();
        let other_epoch = Uuid::new_v4();

        handle
            .append(event(first_epoch, 1, Direction::Rx, b"first".to_vec()))
            .await
            .unwrap();
        handle
            .append(event(first_epoch, 2, Direction::Tx, b"second".to_vec()))
            .await
            .unwrap();
        handle
            .append(event(second_epoch, 1, Direction::Rx, b"new run".to_vec()))
            .await
            .unwrap();
        let mut other = event(other_epoch, 1, Direction::Rx, b"other slot".to_vec());
        other.slot_id = "slot-2".into();
        handle.append(other).await.unwrap();

        let all = handle.list_archives(None).await.unwrap();
        assert!(!all.truncated);
        assert_eq!(all.archives.len(), 3);
        let first = all
            .archives
            .iter()
            .find(|archive| archive.slot_id == "slot-1" && archive.epoch == first_epoch)
            .unwrap();
        assert_eq!((first.first_seq, first.last_seq), (1, 2));
        assert_eq!(first.segment_count, 1);
        assert!(first.total_bytes > 0);
        assert!(first.has_open_segment);

        let filtered = handle
            .list_archives(Some("slot-1".to_string()))
            .await
            .unwrap();
        assert_eq!(filtered.archives.len(), 2);
        assert!(
            filtered
                .archives
                .iter()
                .all(|archive| archive.slot_id == "slot-1")
        );

        manager.shutdown().await.unwrap();
        let reopened = JournalManager::open(config).unwrap();
        let sealed = reopened.handle().list_archives(None).await.unwrap();
        assert!(
            sealed
                .archives
                .iter()
                .all(|archive| !archive.has_open_segment)
        );
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn archive_catalog_aggregates_rotated_segment_metadata() {
        let temporary = tempfile::tempdir().unwrap();
        let mut config = test_config(&temporary);
        config.max_segment_bytes = 512;
        let manager = JournalManager::open(config).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        handle
            .append(event(epoch, 1, Direction::Rx, vec![b'a'; 700]))
            .await
            .unwrap();
        handle
            .append(event(epoch, 2, Direction::Rx, vec![b'b'; 700]))
            .await
            .unwrap();

        let catalog = handle.list_archives(None).await.unwrap();
        assert_eq!(catalog.archives.len(), 1);
        let archive = &catalog.archives[0];
        assert_eq!((archive.first_seq, archive.last_seq), (1, 2));
        assert_eq!(archive.segment_count, 2);
        assert!(archive.has_open_segment);
        assert!(archive.total_bytes > 1_400);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn archive_catalog_obeys_the_shared_scan_byte_budget() {
        let temporary = tempfile::tempdir().unwrap();
        let config = test_config(&temporary);
        let manager = JournalManager::open(config.clone()).unwrap();
        let epoch = Uuid::new_v4();
        manager
            .handle()
            .append(event(epoch, 1, Direction::Rx, b"payload".to_vec()))
            .await
            .unwrap();

        let error = list_archive_files_with_limits(
            &config,
            None,
            QueryLimits {
                max_scan_bytes: 1,
                max_scan_time: Duration::from_secs(1),
            },
        )
        .unwrap_err();
        assert!(matches!(error, JournalError::QueryBudgetExceeded { .. }));
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn raw_bytes_round_trip_and_only_return_durable_after_append() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        let bytes: Vec<_> = (0..=255).collect();

        let appended = handle
            .append(event(epoch, 1, Direction::Rx, bytes.clone()))
            .await
            .unwrap();
        assert!(appended.durable);

        let response = handle.query("slot-1", query(epoch)).await.unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].data, bytes);
        assert!(response.events[0].durable);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn query_applies_direction_contains_time_and_limits() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        handle
            .append(event(epoch, 1, Direction::Rx, b"booting".to_vec()))
            .await
            .unwrap();
        handle
            .append(event(epoch, 2, Direction::Tx, b"version\r".to_vec()))
            .await
            .unwrap();
        handle
            .append(event(epoch, 3, Direction::Rx, b"version 1.0".to_vec()))
            .await
            .unwrap();

        let mut filtered = query(epoch);
        filtered.direction = Some(Direction::Rx);
        filtered.contains = Some("version".into());
        filtered.after_wall_time_ns = Some(1_001);
        let response = handle.query("slot-1", filtered).await.unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 3);

        let mut limited = query(epoch);
        limited.limit_events = Some(1);
        let response = handle.query("slot-1", limited).await.unwrap();
        assert_eq!(response.events.len(), 1);
        assert!(response.truncated);
        assert_eq!(response.next_cursor.unwrap().after_seq, 1);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn query_matches_text_across_rx_event_boundaries() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        let mut first = event(epoch, 1, Direction::Rx, b"Sigma".to_vec());
        first.stream_offset_start = Some(0);
        first.stream_offset_end = Some(5);
        handle.append(first).await.unwrap();
        let mut second = event(epoch, 2, Direction::Rx, b"Star #".to_vec());
        second.stream_offset_start = Some(5);
        second.stream_offset_end = Some(11);
        handle.append(second).await.unwrap();

        let mut filtered = query(epoch);
        filtered.after_seq = Some(1);
        filtered.direction = Some(Direction::Rx);
        filtered.contains = Some("SigmaStar #".into());
        let response = handle.query("slot-1", filtered).await.unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 2);
        assert_eq!(response.next_cursor.unwrap().after_seq, 2);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn query_contains_respects_scope_offsets_generation_and_reopen_boundaries() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let run_a = Uuid::new_v4();
        let run_b = Uuid::new_v4();

        let scoped_epoch = Uuid::new_v4();
        let mut first = event(scoped_epoch, 1, Direction::Rx, b"ERR".to_vec());
        first.run_id = Some(run_a);
        first.stream_offset_start = Some(0);
        first.stream_offset_end = Some(3);
        handle.append(first).await.unwrap();
        let mut second = event(scoped_epoch, 2, Direction::Rx, b"OR".to_vec());
        second.run_id = Some(run_b);
        second.stream_offset_start = Some(3);
        second.stream_offset_end = Some(5);
        handle.append(second).await.unwrap();
        let mut third = event(scoped_epoch, 3, Direction::Rx, b"ERR".to_vec());
        third.run_id = Some(run_b);
        third.stream_offset_start = Some(5);
        third.stream_offset_end = Some(8);
        handle.append(third).await.unwrap();
        let mut fourth = event(scoped_epoch, 4, Direction::Rx, b"OR".to_vec());
        fourth.run_id = Some(run_b);
        fourth.stream_offset_start = Some(8);
        fourth.stream_offset_end = Some(10);
        handle.append(fourth).await.unwrap();

        let mut scoped_query = query(scoped_epoch);
        scoped_query.run_id = Some(run_b);
        scoped_query.contains = Some("ERROR".into());
        let response = handle.query("slot-1", scoped_query).await.unwrap();
        assert_eq!(
            response
                .events
                .iter()
                .map(|item| item.seq)
                .collect::<Vec<_>>(),
            vec![4]
        );

        let offset_epoch = Uuid::new_v4();
        let mut first = event(offset_epoch, 1, Direction::Rx, b"ERR".to_vec());
        first.stream_offset_start = Some(0);
        first.stream_offset_end = Some(3);
        handle.append(first).await.unwrap();
        let mut second = event(offset_epoch, 2, Direction::Rx, b"OR".to_vec());
        second.stream_offset_start = Some(10);
        second.stream_offset_end = Some(12);
        handle.append(second).await.unwrap();
        let mut offset_query = query(offset_epoch);
        offset_query.contains = Some("ERROR".into());
        assert!(
            handle
                .query("slot-1", offset_query)
                .await
                .unwrap()
                .events
                .is_empty()
        );

        let generation_epoch = Uuid::new_v4();
        let mut first = event(generation_epoch, 1, Direction::Rx, b"ERR".to_vec());
        first.stream_offset_start = Some(0);
        first.stream_offset_end = Some(3);
        handle.append(first).await.unwrap();
        let mut second = event(generation_epoch, 2, Direction::Rx, b"OR".to_vec());
        second.generation = 2;
        second.stream_offset_start = Some(3);
        second.stream_offset_end = Some(5);
        handle.append(second).await.unwrap();
        let mut generation_query = query(generation_epoch);
        generation_query.contains = Some("ERROR".into());
        assert!(
            handle
                .query("slot-1", generation_query)
                .await
                .unwrap()
                .events
                .is_empty()
        );

        let reopen_epoch = Uuid::new_v4();
        let mut first = event(reopen_epoch, 1, Direction::Rx, b"ERR".to_vec());
        first.stream_offset_start = Some(0);
        first.stream_offset_end = Some(3);
        handle.append(first).await.unwrap();
        let mut closed = event(reopen_epoch, 2, Direction::None, Vec::new());
        closed.kind = EventKind::SerialClosed;
        closed.stream_offset_start = None;
        closed.stream_offset_end = None;
        handle.append(closed).await.unwrap();
        let mut second = event(reopen_epoch, 3, Direction::Rx, b"OR".to_vec());
        second.stream_offset_start = Some(3);
        second.stream_offset_end = Some(5);
        handle.append(second).await.unwrap();
        let mut reopen_query = query(reopen_epoch);
        reopen_query.contains = Some("ERROR".into());
        assert!(
            handle
                .query("slot-1", reopen_query)
                .await
                .unwrap()
                .events
                .is_empty()
        );

        manager.shutdown().await.unwrap();
    }

    #[test]
    fn query_skips_fully_consumed_segments_under_a_small_scan_budget() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        let old_events = (1..=10)
            .map(|seq| event(epoch, seq, Direction::Rx, vec![b'x'; 512]))
            .collect();
        write_sealed_test_events(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 1,
            },
            old_events,
        );
        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 200,
                first_seq: 11,
            },
            event(epoch, 11, Direction::Rx, b"new".to_vec()),
        );

        let mut request = query(epoch);
        request.after_seq = Some(10);
        let response = query_files_with_limits(
            &config,
            "slot-1",
            &request,
            QueryLimits {
                max_scan_bytes: 3 * 1024,
                max_scan_time: Duration::from_secs(60),
            },
        )
        .unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 11);
        assert_eq!(response.next_cursor.unwrap().after_seq, 11);
    }

    #[test]
    fn query_returns_first_event_when_response_or_scan_budget_is_smaller() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        let header = SegmentHeader {
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            segment_id: Uuid::new_v4(),
            created_wall_time_ns: 100,
            first_seq: 1,
        };
        let timeline_event = event(epoch, 1, Direction::Rx, vec![b'x'; 1_024]);
        let header_bytes = segment_header_encoded_len(&header).unwrap();
        let record_bytes = encode_record(&TimelineEvent {
            durable: true,
            ..timeline_event.clone()
        })
        .unwrap()
        .len() as u64;
        write_sealed_test_segment(&config, header, timeline_event);

        let mut request = query(epoch);
        request.limit_bytes = Some(1);
        let response = query_files_with_limits(
            &config,
            "slot-1",
            &request,
            QueryLimits {
                max_scan_bytes: header_bytes
                    .saturating_mul(2)
                    .saturating_add(record_bytes / 2),
                max_scan_time: Duration::from_secs(60),
            },
        )
        .unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 1);
        assert!(response.truncated);
        assert_eq!(response.next_cursor.unwrap().after_seq, 1);
    }

    #[test]
    fn query_rejects_nonzero_cursor_without_epoch() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let mut request = query(Uuid::new_v4());
        request.epoch = None;
        request.after_seq = Some(1);
        let error = query_files(&config, "slot-1", &request).unwrap_err();
        assert!(matches!(error, JournalError::InvalidConfig(_)));
    }

    #[test]
    fn query_budget_covers_segment_discovery_and_gap_ledger_reads() {
        let segment_temp = TempDir::new().unwrap();
        let segment_config = test_config(&segment_temp);
        let segment_epoch = Uuid::new_v4();
        write_sealed_test_segment(
            &segment_config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: segment_epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 1,
            },
            event(segment_epoch, 1, Direction::Rx, b"event".to_vec()),
        );
        let error = query_files_with_limits(
            &segment_config,
            "slot-1",
            &query(segment_epoch),
            QueryLimits {
                max_scan_bytes: 1,
                max_scan_time: Duration::from_secs(60),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            JournalError::QueryBudgetExceeded {
                phase: "segment discovery",
                ..
            }
        ));

        let ledger_temp = TempDir::new().unwrap();
        let ledger_config = test_config(&ledger_temp);
        let ledger_epoch = Uuid::new_v4();
        append_gap_ledger(
            &ledger_config.root_dir,
            &StoredGap {
                slot_id: "slot-1".into(),
                epoch: ledger_epoch,
                first_seq: 1,
                last_seq: 1,
                reason: GapReason::Retention,
                recorded_wall_time_ns: 100,
            },
        )
        .unwrap();
        let error = query_files_with_limits(
            &ledger_config,
            "slot-1",
            &query(ledger_epoch),
            QueryLimits {
                max_scan_bytes: 1,
                max_scan_time: Duration::from_secs(60),
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            JournalError::QueryBudgetExceeded {
                phase: "gap ledger",
                ..
            }
        ));
    }

    #[test]
    fn query_rejects_an_unbounded_number_of_gap_ranges() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        let mut ledger = Vec::new();
        for index in 0..=MAX_QUERY_GAPS {
            let first_seq = (index as u64).saturating_mul(2).saturating_add(1);
            ledger.extend_from_slice(
                &serde_json::to_vec(&StoredGap {
                    slot_id: "slot-1".into(),
                    epoch,
                    first_seq,
                    last_seq: first_seq,
                    reason: GapReason::Retention,
                    recorded_wall_time_ns: index as i64,
                })
                .unwrap(),
            );
            ledger.push(b'\n');
        }
        fs::create_dir_all(&config.root_dir).unwrap();
        fs::write(config.root_dir.join(GAP_LEDGER_NAME), ledger).unwrap();

        let error = query_files(&config, "slot-1", &query(epoch)).unwrap_err();
        assert!(matches!(
            error,
            JournalError::TooManyQueryGaps {
                maximum: MAX_QUERY_GAPS
            }
        ));
    }

    #[tokio::test]
    async fn query_scopes_events_to_run_operation_actor_and_kind() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        let run_id = Uuid::new_v4();
        let operation_id = Uuid::new_v4();
        let mut matching = event(epoch, 1, Direction::Tx, b"result".to_vec());
        matching.run_id = Some(run_id);
        matching.operation_id = Some(operation_id);
        handle.append(matching).await.unwrap();
        handle
            .append(event(epoch, 2, Direction::Tx, b"result".to_vec()))
            .await
            .unwrap();

        let mut filtered = query(epoch);
        filtered.kind = Some(EventKind::Tx);
        filtered.actor_id = Some("human:test".into());
        filtered.run_id = Some(run_id);
        filtered.operation_id = Some(operation_id);
        let response = handle.query("slot-1", filtered).await.unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 1);
        manager.shutdown().await.unwrap();
    }

    #[test]
    fn startup_recovers_complete_prefix_and_truncates_partial_record() {
        let temp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&temp));
        fs::create_dir_all(slots_root(&config.root_dir)).unwrap();
        let epoch = Uuid::new_v4();
        let first = event(epoch, 1, Direction::Rx, b"complete".to_vec());
        let second = event(epoch, 2, Direction::Rx, b"partial".to_vec());
        let mut segment = OpenSegment::create(&config.root_dir, &first).unwrap();
        let first_record = encode_record(&TimelineEvent {
            durable: true,
            ..first
        })
        .unwrap();
        segment.append(&first_record, 1).unwrap();
        let path = segment.path.clone();
        drop(segment);
        let second_record = encode_record(&TimelineEvent {
            durable: true,
            ..second
        })
        .unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&second_record[..second_record.len() / 2])
            .unwrap();
        drop(file);

        recover_open_segments(&config.root_dir).unwrap();
        assert!(
            collect_files(&config.root_dir, Some("open"))
                .unwrap()
                .is_empty()
        );
        let sealed = collect_files(&config.root_dir, Some("slog")).unwrap();
        assert_eq!(sealed.len(), 1);
        assert_eq!(scan_last_seq(&sealed[0]).unwrap(), Some(1));
    }

    #[tokio::test]
    async fn rotates_at_size_boundary() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(&temp);
        config.max_segment_bytes = 700;
        let manager = JournalManager::open(config.clone()).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        for seq in 1..=5 {
            handle
                .append(event(epoch, seq, Direction::Rx, vec![seq as u8; 300]))
                .await
                .unwrap();
        }
        manager.shutdown().await.unwrap();
        assert!(collect_files(&config.root_dir, Some("slog")).unwrap().len() >= 2);
    }

    #[tokio::test]
    async fn retention_returns_first_available_and_explicit_gap() {
        let temp = TempDir::new().unwrap();
        let mut config = test_config(&temp);
        config.max_segment_bytes = 750;
        config.max_total_bytes = 2_400;
        config.cleanup_low_watermark = 0.55;
        let manager = JournalManager::open(config).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        for seq in 1..=6 {
            handle
                .append(event(epoch, seq, Direction::Rx, vec![b'x'; 350]))
                .await
                .unwrap();
        }
        handle.flush().await.unwrap();

        let mut request = query(epoch);
        request.after_seq = Some(0);
        let response = handle.query("slot-1", request).await.unwrap();
        assert!(response.first_available_seq.unwrap_or(1) > 1);
        assert!(response.gaps.iter().any(|gap| {
            gap.epoch == epoch && gap.reason == GapReason::Retention && gap.first_seq == 1
        }));
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sequence_jump_is_recorded_but_duplicate_is_rejected() {
        let temp = TempDir::new().unwrap();
        let manager = JournalManager::open(test_config(&temp)).unwrap();
        let handle = manager.handle();
        let epoch = Uuid::new_v4();
        handle
            .append(event(epoch, 2, Direction::Rx, b"two".to_vec()))
            .await
            .unwrap();
        let error = handle
            .append(event(epoch, 2, Direction::Rx, b"duplicate".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(error, JournalError::NonMonotonicSequence { .. }));

        let response = handle.query("slot-1", query(epoch)).await.unwrap();
        assert!(response.gaps.iter().any(|gap| {
            gap.reason == GapReason::LoggingFault && gap.first_seq == 1 && gap.last_seq == 1
        }));
        manager.shutdown().await.unwrap();
    }

    #[test]
    fn query_synthesizes_gap_for_adjacent_retained_sequence_jump() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        write_sealed_test_events(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 1,
            },
            vec![
                event(epoch, 1, Direction::Rx, b"one".to_vec()),
                event(epoch, 3, Direction::Rx, b"three".to_vec()),
            ],
        );

        let response = query_files(&config, "slot-1", &query(epoch)).unwrap();
        assert_eq!(
            response
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(response.gaps.iter().any(|gap| {
            gap.epoch == epoch
                && gap.first_seq == 2
                && gap.last_seq == 2
                && gap.reason == GapReason::SequenceDiscontinuity
        }));
    }

    #[test]
    fn query_synthesizes_gap_from_cursor_to_first_scanned_record() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 5,
            },
            event(epoch, 5, Direction::Rx, b"five".to_vec()),
        );
        let mut request = query(epoch);
        request.after_seq = Some(2);

        let response = query_files(&config, "slot-1", &request).unwrap();
        assert_eq!(response.events[0].seq, 5);
        assert!(response.gaps.iter().any(|gap| {
            gap.epoch == epoch
                && gap.first_seq == 3
                && gap.last_seq == 4
                && gap.reason == GapReason::SequenceDiscontinuity
        }));
        assert!(!response.gaps.iter().any(|gap| {
            gap.first_seq == 3 && gap.last_seq == 4 && gap.reason == GapReason::Retention
        }));
    }

    #[test]
    fn query_filters_do_not_turn_present_records_into_sequence_gaps() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();
        write_sealed_test_events(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 1,
            },
            vec![
                event(epoch, 1, Direction::Rx, b"one".to_vec()),
                event(epoch, 2, Direction::Tx, b"two".to_vec()),
                event(epoch, 3, Direction::Rx, b"three".to_vec()),
            ],
        );
        let mut request = query(epoch);
        request.direction = Some(Direction::Tx);

        let response = query_files(&config, "slot-1", &request).unwrap();
        assert_eq!(response.events.len(), 1);
        assert_eq!(response.events[0].seq, 2);
        assert!(
            response
                .gaps
                .iter()
                .all(|gap| gap.reason != GapReason::SequenceDiscontinuity)
        );
    }

    #[test]
    fn append_recovers_torn_gap_ledger_tail_before_writing_new_gap() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        fs::create_dir_all(&config.root_dir).unwrap();
        let epoch = Uuid::new_v4();
        let first = StoredGap {
            slot_id: "slot-1".into(),
            epoch,
            first_seq: 1,
            last_seq: 1,
            reason: GapReason::Retention,
            recorded_wall_time_ns: 100,
        };
        let second = StoredGap {
            first_seq: 3,
            last_seq: 3,
            recorded_wall_time_ns: 200,
            ..first.clone()
        };
        let mut bytes = serde_json::to_vec(&first).unwrap();
        bytes.push(b'\n');
        let valid_prefix = bytes.clone();
        bytes.extend_from_slice(br#"{"slot_id":"torn"#);
        fs::write(config.root_dir.join(GAP_LEDGER_NAME), bytes).unwrap();

        let update = append_gap_ledger(&config.root_dir, &second).unwrap();
        assert!(update.truncated_bytes > 0);
        assert!(update.appended_bytes > 0);
        let recovered = fs::read(config.root_dir.join(GAP_LEDGER_NAME)).unwrap();
        assert!(recovered.starts_with(&valid_prefix));
        assert!(!recovered.windows(4).any(|window| window == b"torn"));

        let mut budget = QueryBudget::new(QueryLimits::default());
        let gaps =
            load_query_gap_ranges(&config.root_dir, "slot-1", epoch, None, &mut budget).unwrap();
        assert_eq!(gaps.len(), 2);
        assert_eq!((gaps[0].first_seq, gaps[0].last_seq), (1, 1));
        assert_eq!((gaps[1].first_seq, gaps[1].last_seq), (3, 3));
    }

    #[tokio::test]
    async fn journal_startup_truncates_torn_gap_ledger_tail() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        fs::create_dir_all(&config.root_dir).unwrap();
        let gap = StoredGap {
            slot_id: "slot-1".into(),
            epoch: Uuid::new_v4(),
            first_seq: 1,
            last_seq: 1,
            reason: GapReason::LoggingFault,
            recorded_wall_time_ns: 100,
        };
        let mut valid = serde_json::to_vec(&gap).unwrap();
        valid.push(b'\n');
        let mut torn = valid.clone();
        torn.extend_from_slice(br#"{"slot_id":"partial"#);
        fs::write(config.root_dir.join(GAP_LEDGER_NAME), torn).unwrap();

        let manager = JournalManager::open(config.clone()).unwrap();
        assert_eq!(
            fs::read(config.root_dir.join(GAP_LEDGER_NAME)).unwrap(),
            valid
        );
        manager.shutdown().await.unwrap();
    }

    #[test]
    fn poisoned_segment_is_closed_and_later_records_use_a_new_segment() {
        let temp = TempDir::new().unwrap();
        let config = Arc::new(test_config(&temp));
        fs::create_dir_all(slots_root(&config.root_dir)).unwrap();
        let mut state = WriterState::initialize(Arc::clone(&config)).unwrap();
        let epoch = Uuid::new_v4();

        state
            .append(event(epoch, 1, Direction::Rx, b"one".to_vec()))
            .unwrap();
        let key = StreamKey {
            slot_id: "slot-1".into(),
            epoch,
        };
        let (poisoned_path, valid_len) = {
            let segment = state.open_segments.get_mut(&key).unwrap();
            segment.poison_next_append = true;
            (segment.path.clone(), segment.bytes_written)
        };

        let error = state
            .append(event(epoch, 2, Direction::Rx, b"two".to_vec()))
            .unwrap_err();
        assert!(matches!(error, JournalError::SegmentPoisoned { .. }));
        assert!(!state.open_segments.contains_key(&key));
        assert!(fs::metadata(&poisoned_path).unwrap().len() > valid_len);

        let appended = state
            .append(event(epoch, 3, Direction::Rx, b"three".to_vec()))
            .unwrap();
        assert!(appended.durable);
        assert_ne!(state.open_segments[&key].path, poisoned_path);

        let response = query_files(&config, "slot-1", &query(epoch)).unwrap();
        let sequences: Vec<_> = response.events.iter().map(|item| item.seq).collect();
        assert_eq!(sequences, vec![1, 3]);
        assert!(response.gaps.iter().any(|gap| {
            gap.epoch == epoch
                && gap.first_seq == 2
                && gap.last_seq == 2
                && gap.reason == GapReason::LoggingFault
        }));
    }

    #[test]
    fn retention_failure_rejects_append_and_retries_with_backoff() {
        let temp = TempDir::new().unwrap();
        let mut initial_config = test_config(&temp);
        initial_config.max_segment_bytes = 700;
        initial_config.max_total_bytes = 1024 * 1024;
        let mut state = WriterState::initialize(Arc::new(initial_config)).unwrap();
        let epoch = Uuid::new_v4();

        for seq in 1..=4 {
            state
                .append(event(epoch, seq, Direction::Rx, vec![b'x'; 350]))
                .unwrap();
        }
        let head_before = state.heads[&StreamKey {
            slot_id: "slot-1".into(),
            epoch,
        }];
        let mut constrained_config = (*state.config).clone();
        constrained_config.max_total_bytes = state.total_bytes.saturating_sub(1).max(1);
        constrained_config.cleanup_low_watermark = 0.50;
        state.config = Arc::new(constrained_config);
        state.retention_delete_failures_remaining = usize::MAX;

        let error = state
            .append(event(epoch, 5, Direction::Rx, vec![b'y'; 350]))
            .unwrap_err();
        assert!(matches!(error, JournalError::RetentionFailed { .. }));
        assert_eq!(
            state.heads[&StreamKey {
                slot_id: "slot-1".into(),
                epoch,
            }],
            head_before
        );
        let scans_after_failure = state.retention_scan_count;

        let error = state
            .append(event(epoch, 5, Direction::Rx, vec![b'y'; 350]))
            .unwrap_err();
        assert!(matches!(error, JournalError::RetentionBackoff { .. }));
        assert_eq!(state.retention_scan_count, scans_after_failure);

        let response = query_files(&state.config, "slot-1", &query(epoch)).unwrap();
        assert!(response.events.iter().all(|item| item.seq <= head_before));
        assert!(!response.events.iter().any(|item| item.seq == 5));
    }

    #[test]
    fn query_without_epoch_paginates_only_the_latest_epoch() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let old_epoch = Uuid::new_v4();
        let latest_epoch = Uuid::new_v4();

        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: old_epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 1,
            },
            event(old_epoch, 1, Direction::Rx, b"old".to_vec()),
        );
        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: latest_epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 200,
                first_seq: 1,
            },
            event(latest_epoch, 1, Direction::Rx, b"new-1".to_vec()),
        );
        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: latest_epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 201,
                first_seq: 2,
            },
            event(latest_epoch, 2, Direction::Rx, b"new-2".to_vec()),
        );

        let mut request = query(old_epoch);
        request.epoch = None;
        request.after_seq = Some(0);
        let response = query_files(&config, "slot-1", &request).unwrap();
        assert_eq!(
            response
                .events
                .iter()
                .map(|item| (item.daemon_epoch, item.seq))
                .collect::<Vec<_>>(),
            vec![(latest_epoch, 1), (latest_epoch, 2)]
        );
        assert_eq!(
            response.next_cursor,
            Some(Cursor {
                epoch: latest_epoch,
                after_seq: 2,
            })
        );
    }

    #[test]
    fn query_orders_segments_in_one_epoch_by_first_sequence() {
        let temp = TempDir::new().unwrap();
        let config = test_config(&temp);
        let epoch = Uuid::new_v4();

        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 100,
                first_seq: 2,
            },
            event(epoch, 2, Direction::Rx, b"second".to_vec()),
        );
        write_sealed_test_segment(
            &config,
            SegmentHeader {
                slot_id: "slot-1".into(),
                daemon_epoch: epoch,
                segment_id: Uuid::new_v4(),
                created_wall_time_ns: 200,
                first_seq: 1,
            },
            event(epoch, 1, Direction::Rx, b"first".to_vec()),
        );

        let response = query_files(&config, "slot-1", &query(epoch)).unwrap();
        assert_eq!(
            response
                .events
                .iter()
                .map(|item| item.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
}
