use serial_protocol::{Cursor, GapReason, TimelineEvent};
use std::collections::VecDeque;
use uuid::Uuid;

const EVENT_OVERHEAD_ESTIMATE: usize = 256;
const BTREE_ENTRY_OVERHEAD_ESTIMATE: usize = 3 * std::mem::size_of::<usize>();
const UUID_BYTES: usize = std::mem::size_of::<Uuid>();

#[derive(Debug)]
pub struct EventRing {
    events: VecDeque<TimelineEvent>,
    bytes: usize,
    max_events: usize,
    max_bytes: usize,
}

#[derive(Debug, Clone)]
pub struct ReplayWindow {
    pub events: Vec<TimelineEvent>,
    pub gap: Option<ReplayGap>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayGap {
    pub reason: GapReason,
    pub requested_after_seq: Option<u64>,
    pub first_available_seq: Option<u64>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReplayError {
    #[error("cursor {cursor} is ahead of head {head}")]
    CursorAhead { cursor: u64, head: u64 },
}

impl EventRing {
    pub fn new(max_events: usize, max_bytes: usize) -> Self {
        Self {
            events: VecDeque::new(),
            bytes: 0,
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    pub fn push(&mut self, event: TimelineEvent) {
        self.bytes = self.bytes.saturating_add(event_size(&event));
        self.events.push_back(event);
        while self.events.len() > self.max_events || self.bytes > self.max_bytes {
            let Some(evicted) = self.events.pop_front() else {
                break;
            };
            self.bytes = self.bytes.saturating_sub(event_size(&evicted));
        }
    }

    pub fn oldest_seq(&self) -> Option<u64> {
        self.events.front().map(|event| event.seq)
    }

    pub fn head_seq(&self) -> Option<u64> {
        self.events.back().map(|event| event.seq)
    }

    pub fn replay(
        &self,
        daemon_epoch: Uuid,
        cursor: Option<&Cursor>,
        head_seq: u64,
        tail_events: usize,
    ) -> Result<ReplayWindow, ReplayError> {
        let bounded_tail = tail_events.clamp(1, self.max_events);
        let tail = || {
            let eligible = self
                .events
                .iter()
                .filter(|event| event.seq <= head_seq)
                .collect::<Vec<_>>();
            let skip = eligible.len().saturating_sub(bounded_tail);
            eligible.into_iter().skip(skip).cloned().collect::<Vec<_>>()
        };

        let Some(cursor) = cursor else {
            return Ok(ReplayWindow {
                events: tail(),
                gap: None,
            });
        };

        if cursor.epoch != daemon_epoch {
            return Ok(ReplayWindow {
                events: tail(),
                gap: Some(ReplayGap {
                    reason: GapReason::EpochChanged,
                    requested_after_seq: Some(cursor.after_seq),
                    first_available_seq: self.oldest_seq(),
                }),
            });
        }

        if cursor.after_seq > head_seq {
            return Err(ReplayError::CursorAhead {
                cursor: cursor.after_seq,
                head: head_seq,
            });
        }

        if cursor.after_seq == head_seq {
            return Ok(ReplayWindow {
                events: Vec::new(),
                gap: None,
            });
        }

        let oldest = self.oldest_seq();
        if oldest.is_none_or(|first| cursor.after_seq.saturating_add(1) < first) {
            return Ok(ReplayWindow {
                events: tail(),
                gap: Some(ReplayGap {
                    reason: GapReason::RingEvicted,
                    requested_after_seq: Some(cursor.after_seq),
                    first_available_seq: oldest,
                }),
            });
        }

        Ok(ReplayWindow {
            events: self
                .events
                .iter()
                .filter(|event| event.seq > cursor.after_seq && event.seq <= head_seq)
                .cloned()
                .collect(),
            gap: None,
        })
    }
}

fn event_size(event: &TimelineEvent) -> usize {
    // This is an intentionally conservative in-memory estimate, not the wire
    // size. JSON length captures recursively allocated metadata strings and
    // values; a per-entry allowance covers BTreeMap nodes. The fixed estimate
    // covers scalar header fields and enum/Option storage.
    let encoded_metadata_bytes = if event.metadata.is_empty() {
        2 // `{}` without allocating on the RX hot path.
    } else {
        serde_json::to_vec(&event.metadata).map_or(EVENT_OVERHEAD_ESTIMATE, |encoded| encoded.len())
    };
    let metadata_bytes = encoded_metadata_bytes.saturating_add(
        event
            .metadata
            .len()
            .saturating_mul(BTREE_ENTRY_OVERHEAD_ESTIMATE),
    );
    event
        .data
        .capacity()
        .saturating_add(event.slot_id.capacity())
        + event
            .actor
            .as_ref()
            .map_or(0, |actor| actor.id.capacity() + actor.label.capacity())
        + event.run_id.map_or(0, |_| UUID_BYTES)
        + event.operation_id.map_or(0, |_| UUID_BYTES)
        + metadata_bytes
        + EVENT_OVERHEAD_ESTIMATE
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{Direction, EventKind};
    use std::collections::BTreeMap;

    fn event(epoch: Uuid, seq: u64) -> TimelineEvent {
        TimelineEvent {
            slot_id: "slot-1".into(),
            daemon_epoch: epoch,
            seq,
            generation: 1,
            wall_time_ns: seq as i64,
            monotonic_time_ns: seq,
            kind: EventKind::Rx,
            direction: Direction::Rx,
            actor: None,
            run_id: None,
            operation_id: None,
            stream_offset_start: Some(seq - 1),
            stream_offset_end: Some(seq),
            data: vec![seq as u8],
            metadata: BTreeMap::new(),
            durable: true,
        }
    }

    #[test]
    fn replay_is_strictly_after_cursor() {
        let epoch = Uuid::new_v4();
        let mut ring = EventRing::new(10, 10_000);
        for seq in 1..=5 {
            ring.push(event(epoch, seq));
        }
        let replay = ring
            .replay(
                epoch,
                Some(&Cursor {
                    epoch,
                    after_seq: 3,
                }),
                5,
                10,
            )
            .unwrap();
        assert_eq!(
            replay
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![4, 5]
        );
        assert!(replay.gap.is_none());
    }

    #[test]
    fn eviction_is_an_explicit_gap() {
        let epoch = Uuid::new_v4();
        let mut ring = EventRing::new(2, 10_000);
        for seq in 1..=4 {
            ring.push(event(epoch, seq));
        }
        let replay = ring
            .replay(
                epoch,
                Some(&Cursor {
                    epoch,
                    after_seq: 1,
                }),
                4,
                10,
            )
            .unwrap();
        assert_eq!(replay.gap.unwrap().reason, GapReason::RingEvicted);
        assert_eq!(
            replay
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![3, 4]
        );
    }

    #[test]
    fn old_epoch_returns_tail_with_gap() {
        let epoch = Uuid::new_v4();
        let mut ring = EventRing::new(5, 10_000);
        ring.push(event(epoch, 1));
        let replay = ring
            .replay(
                epoch,
                Some(&Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 99,
                }),
                1,
                5,
            )
            .unwrap();
        assert_eq!(replay.gap.unwrap().reason, GapReason::EpochChanged);
        assert_eq!(replay.events.len(), 1);
    }

    #[test]
    fn tail_never_crosses_the_captured_snapshot_head() {
        let epoch = Uuid::new_v4();
        let mut ring = EventRing::new(10, 10_000);
        for seq in 1..=5 {
            ring.push(event(epoch, seq));
        }
        let replay = ring.replay(epoch, None, 3, 10).unwrap();
        assert_eq!(
            replay
                .events
                .iter()
                .map(|event| event.seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn metadata_and_optional_ids_count_toward_the_byte_budget() {
        let epoch = Uuid::new_v4();
        let baseline = event(epoch, 1);
        let baseline_size = event_size(&baseline);

        let mut enriched = event(epoch, 2);
        enriched.run_id = Some(Uuid::new_v4());
        enriched.operation_id = Some(Uuid::new_v4());
        enriched
            .metadata
            .insert("payload".into(), serde_json::json!("x".repeat(2_048)));
        assert!(event_size(&enriched) >= baseline_size + 2_048 + (2 * UUID_BYTES));

        let mut ring = EventRing::new(10, baseline_size + 512);
        ring.push(enriched);
        assert_eq!(ring.head_seq(), None, "oversized metadata must be evicted");
    }
}
