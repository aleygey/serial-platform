use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream};
use serial_protocol::{Cursor, DataFrameHeader, EventQuery, GapRange, SlotSnapshot, TimelineEvent};
use tokio::time::{Instant as TokioInstant, timeout, timeout_at};
use uuid::Uuid;

use crate::api::ApiClient;

/// The console is an in-memory projection of the durable journal, so startup
/// recovery must remain bounded just like the live projection. Sequence
/// numbers are dense within one daemon epoch; this window therefore caps both
/// the amount of old work requested and the oldest row the console may show.
pub(crate) const STARTUP_HISTORY_SEQUENCE_WINDOW: u64 = 20_000;
const STARTUP_HISTORY_PAGE_EVENTS: usize = 2_000;
const STARTUP_HISTORY_PAGE_BYTES: usize = 2 * 1024 * 1024;
const STARTUP_HISTORY_TOTAL_EVENTS: usize = 20_000;
const STARTUP_HISTORY_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const STARTUP_HISTORY_QUERY_TIMEOUT: Duration = Duration::from_secs(6);
const STARTUP_HISTORY_SLOT_TIMEOUT: Duration = Duration::from_secs(12);
const STARTUP_HISTORY_ALL_SLOTS_TIMEOUT: Duration = Duration::from_secs(10);
const STARTUP_HISTORY_CONCURRENCY: usize = 2;

#[derive(Debug, Clone)]
pub(crate) struct StartupHistoryTarget {
    pub(crate) port: String,
    pub(crate) epoch: Uuid,
    pub(crate) head_seq: u64,
}

impl From<&SlotSnapshot> for StartupHistoryTarget {
    fn from(snapshot: &SlotSnapshot) -> Self {
        Self {
            port: snapshot.config.port.clone(),
            epoch: snapshot.daemon_epoch,
            head_seq: snapshot.head_seq,
        }
    }
}

#[derive(Debug)]
pub(crate) struct StartupHistory {
    pub(crate) port: String,
    pub(crate) epoch: Uuid,
    pub(crate) head_seq: u64,
    pub(crate) events: Vec<TimelineEvent>,
    pub(crate) gaps: Vec<GapRange>,
    /// Last sequence the journal actually scanned. The WebSocket must resume
    /// here, not at `head_seq`: the status snapshot can already include a live
    /// event whose durability acknowledgement is still pending.
    pub(crate) resume_cursor: Option<Cursor>,
    pub(crate) limited: bool,
    pub(crate) error: Option<String>,
}

impl StartupHistory {
    fn empty(target: StartupHistoryTarget) -> Self {
        Self {
            port: target.port,
            epoch: target.epoch,
            head_seq: target.head_seq,
            events: Vec::new(),
            gaps: Vec::new(),
            resume_cursor: None,
            limited: false,
            error: None,
        }
    }

    fn normalize(&mut self) {
        self.events
            .sort_by_key(|event| (event.daemon_epoch, event.seq));
        self.events.dedup_by(|left, right| {
            left.daemon_epoch == right.daemon_epoch && left.seq == right.seq
        });
        self.gaps
            .sort_by_key(|gap| (gap.epoch, gap.first_seq, gap.last_seq));
        self.gaps.dedup();
    }
}

pub(crate) async fn load_startup_histories<F>(
    api: ApiClient,
    targets: Vec<StartupHistoryTarget>,
    mut consume: F,
) where
    F: FnMut(StartupHistory),
{
    let mut unfinished = targets
        .iter()
        .cloned()
        .map(|target| (target.port.clone(), target))
        .collect::<HashMap<_, _>>();
    let mut pending = Box::pin(
        stream::iter(targets.into_iter().map(|target| {
            let api = api.clone();
            async move { load_startup_history(&api, target).await }
        }))
        // seriald itself permits two concurrent journal scans. Matching that
        // bound avoids queueing an arbitrary number of large startup reads.
        .buffer_unordered(STARTUP_HISTORY_CONCURRENCY),
    );
    let deadline = TokioInstant::now() + STARTUP_HISTORY_ALL_SLOTS_TIMEOUT;
    loop {
        match timeout_at(deadline, pending.next()).await {
            Ok(Some(history)) => {
                unfinished.remove(&history.port);
                consume(history);
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
    drop(pending);

    for target in unfinished.into_values() {
        let mut history = StartupHistory::empty(target);
        history.limited = true;
        history.error = Some("startup history recovery reached its global time limit".into());
        consume(history);
    }
}

async fn load_startup_history(api: &ApiClient, target: StartupHistoryTarget) -> StartupHistory {
    let mut history = StartupHistory::empty(target);
    if history.head_seq == 0 {
        return history;
    }

    let started = Instant::now();
    let initial_after = history
        .head_seq
        .saturating_sub(STARTUP_HISTORY_SEQUENCE_WINDOW);
    history.limited = initial_after > 0;
    let mut after_seq = initial_after;
    let mut loaded_bytes = 0usize;

    loop {
        let elapsed = started.elapsed();
        let Some(remaining) = STARTUP_HISTORY_SLOT_TIMEOUT.checked_sub(elapsed) else {
            history.limited = true;
            history.error = Some("startup history recovery reached its time limit".into());
            break;
        };
        let query_timeout = remaining.min(STARTUP_HISTORY_QUERY_TIMEOUT);
        let query = EventQuery {
            // Never merge an older daemon epoch into the active serial view.
            // Archived epochs remain available through history search/logs.
            epoch: Some(history.epoch),
            after_seq: Some(after_seq),
            through_seq: Some(history.head_seq),
            before_wall_time_ns: None,
            after_wall_time_ns: None,
            direction: None,
            kind: None,
            actor_id: None,
            run_id: None,
            operation_id: None,
            contains: None,
            regex: None,
            limit_events: Some(STARTUP_HISTORY_PAGE_EVENTS),
            limit_bytes: Some(STARTUP_HISTORY_PAGE_BYTES),
        };

        let response = match timeout(query_timeout, api.events(&history.port, &query)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                history.limited = true;
                history.error = Some(format!("{error:#}"));
                break;
            }
            Err(_) => {
                history.limited = true;
                history.error = Some("startup history query timed out".into());
                break;
            }
        };

        let previous_after = after_seq;
        let next = verified_page_cursor(
            history.epoch,
            history.head_seq,
            previous_after,
            response.next_cursor,
        );
        let invalid_event = response.events.iter().any(|event| {
            event.port != history.port
                || event.daemon_epoch != history.epoch
                || event.seq <= previous_after
                || event.seq > history.head_seq
        });
        let invalid_gap = response.gaps.iter().any(|gap| {
            gap.epoch != history.epoch
                || gap.first_seq > gap.last_seq
                || gap.first_seq <= previous_after
                || gap.last_seq > history.head_seq
        });
        if invalid_event || invalid_gap {
            history.limited = true;
            history.error = Some("startup history response crossed its Port/epoch boundary".into());
            break;
        }
        loaded_bytes =
            loaded_bytes.saturating_add(response.events.iter().fold(0usize, |total, event| {
                let header_bytes = serde_json::to_vec(&DataFrameHeader::from(event))
                    .map_or(usize::MAX, |header| header.len());
                total.saturating_add(event.data.len().saturating_add(header_bytes))
            }));
        history.events.extend(response.events);
        history.gaps.extend(response.gaps);
        if let Some(cursor) = next.clone() {
            history.resume_cursor = Some(cursor);
        }

        let over_local_budget = history.events.len() >= STARTUP_HISTORY_TOTAL_EVENTS
            || loaded_bytes >= STARTUP_HISTORY_TOTAL_BYTES;
        if over_local_budget {
            history.limited = true;
            break;
        }
        if !response.truncated {
            // `resume_cursor` may legitimately remain below `head_seq`: live
            // state becomes visible before a pending journal ACK completes.
            // The subsequent ring attach closes exactly that tail.
            break;
        }
        let Some(cursor) = next else {
            history.limited = true;
            history.error =
                Some("truncated history page did not return a continuation cursor".into());
            break;
        };
        if cursor.after_seq <= previous_after {
            history.limited = true;
            history.error = Some("startup history cursor did not advance".into());
            break;
        }
        after_seq = cursor.after_seq;
        if after_seq >= history.head_seq {
            break;
        }
    }

    // A nonzero live head without any verified journal cursor falls back to
    // an ordinary bounded tail attach. Never label that projection complete:
    // `cursor: None` intentionally cannot prove where retained history began.
    if history.head_seq > 0 && history.resume_cursor.is_none() {
        history.limited = true;
    }
    history.normalize();
    history
}

fn verified_page_cursor(
    epoch: Uuid,
    head_seq: u64,
    previous_after: u64,
    next_cursor: Option<Cursor>,
) -> Option<Cursor> {
    next_cursor.filter(|cursor| {
        cursor.epoch == epoch && cursor.after_seq >= previous_after && cursor.after_seq <= head_seq
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn history(epoch: Uuid, head_seq: u64) -> StartupHistory {
        StartupHistory::empty(StartupHistoryTarget {
            port: "COM4".into(),
            epoch,
            head_seq,
        })
    }

    #[test]
    fn pending_non_durable_tail_never_advances_resume_cursor_to_snapshot_head() {
        let epoch = Uuid::new_v4();
        // The journal has scanned through #10 while status already reports
        // live head #12. The WebSocket must replay #11 and #12 from its ring.
        let cursor = verified_page_cursor(
            epoch,
            12,
            0,
            Some(Cursor {
                epoch,
                after_seq: 10,
            }),
        )
        .expect("journal cursor is within the snapshot boundary");

        assert_eq!(cursor.after_seq, 10);
        assert_ne!(cursor.after_seq, 12);
    }

    #[test]
    fn absent_journal_cursor_is_not_replaced_with_an_unverified_boundary() {
        let epoch = Uuid::new_v4();
        let recovered = history(epoch, 12);

        assert!(recovered.resume_cursor.is_none());
    }

    #[test]
    fn normalization_orders_and_deduplicates_epoch_sequence_and_gaps() {
        let epoch = Uuid::new_v4();
        let mut recovered = history(epoch, 12);
        let make_event = |seq| TimelineEvent {
            port: "COM4".into(),
            daemon_epoch: epoch,
            seq,
            wall_time_ns: 0,
            monotonic_time_ns: 0,
            kind: serial_protocol::EventKind::Rx,
            direction: serial_protocol::Direction::Rx,
            generation: 1,
            stream_offset_start: None,
            stream_offset_end: None,
            actor: None,
            run_id: None,
            operation_id: None,
            data: vec![b'x'],
            metadata: Default::default(),
            durable: true,
        };
        recovered.events = vec![make_event(2), make_event(1), make_event(2)];
        let gap = GapRange {
            epoch,
            first_seq: 3,
            last_seq: 4,
            reason: serial_protocol::GapReason::Retention,
        };
        recovered.gaps = vec![gap.clone(), gap];

        recovered.normalize();

        assert_eq!(
            recovered
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(recovered.gaps.len(), 1);
    }
}
