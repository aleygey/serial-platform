use serial_protocol::{Actor, ControlLease, ControlMode};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use uuid::Uuid;

const MIN_TTL_MS: u64 = 5_000;
const MAX_TTL_MS: u64 = 60_000;
const MAX_WAITERS: usize = 128;
const WAIT_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
struct Waiter {
    actor: Actor,
    ttl_ms: u64,
    deadline: Instant,
}

#[derive(Debug, Clone)]
struct ActiveLease {
    // The protocol timestamps are informational. Only this deadline authorizes expiry.
    lease: ControlLease,
    deadline: Instant,
}

#[derive(Debug)]
pub struct ControlState {
    daemon_epoch: Uuid,
    generation: u64,
    next_fence: u64,
    current: Option<ActiveLease>,
    queue: VecDeque<Waiter>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AcquireOutcome {
    Granted(ControlLease),
    AlreadyHeld(ControlLease),
    Queued {
        position: usize,
    },
    QueueFull,
    TakenOver {
        revoked: ControlLease,
        granted: ControlLease,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReleaseOutcome {
    pub released: ControlLease,
    pub promoted: Option<ControlLease>,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ControlError {
    #[error("this actor does not hold write control")]
    NotOwner,
    #[error("control id or fencing token is stale")]
    StaleFence,
    #[error("control lease has expired")]
    Expired,
}

impl ControlState {
    pub fn new(daemon_epoch: Uuid, generation: u64) -> Self {
        Self {
            daemon_epoch,
            generation,
            next_fence: 1,
            current: None,
            queue: VecDeque::new(),
        }
    }

    pub fn current(&self) -> Option<&ControlLease> {
        self.current.as_ref().map(|active| &active.lease)
    }

    pub fn acquire(
        &mut self,
        actor: Actor,
        mode: ControlMode,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> AcquireOutcome {
        if let Some(current) = &self.current {
            if current.lease.owner.id == actor.id {
                return AcquireOutcome::AlreadyHeld(current.lease.clone());
            }
            if mode == ControlMode::Queue {
                let position = if let Some(index) = self
                    .queue
                    .iter()
                    .position(|waiter| waiter.actor.id == actor.id)
                {
                    let waiter = &mut self.queue[index];
                    waiter.ttl_ms = clamp_ttl(ttl_ms);
                    waiter.deadline = monotonic_now + WAIT_TIMEOUT;
                    index + 1
                } else {
                    if self.queue.len() >= MAX_WAITERS {
                        return AcquireOutcome::QueueFull;
                    }
                    self.queue.push_back(Waiter {
                        actor,
                        ttl_ms: clamp_ttl(ttl_ms),
                        deadline: monotonic_now + WAIT_TIMEOUT,
                    });
                    self.queue.len()
                };
                return AcquireOutcome::Queued { position };
            }

            let revoked = self.current.take().expect("checked above").lease;
            self.queue.retain(|waiter| waiter.actor.id != actor.id);
            let granted = self.grant(actor, ttl_ms, wall_now_ns, monotonic_now);
            return AcquireOutcome::TakenOver { revoked, granted };
        }

        let granted = self.grant(actor, ttl_ms, wall_now_ns, monotonic_now);
        AcquireOutcome::Granted(granted)
    }

    pub fn renew(
        &mut self,
        actor_id: &str,
        control_id: Uuid,
        fence: u64,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Result<ControlLease, ControlError> {
        self.validate(actor_id, control_id, fence, monotonic_now)?;
        let ttl_ms = clamp_ttl(ttl_ms);
        let current = self.current.as_mut().expect("validated");
        current.lease.expires_wall_time_ns = wall_expiry(wall_now_ns, ttl_ms);
        current.deadline = monotonic_expiry(monotonic_now, ttl_ms);
        Ok(current.lease.clone())
    }

    pub fn release(
        &mut self,
        actor_id: &str,
        control_id: Uuid,
        fence: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Result<ReleaseOutcome, ControlError> {
        self.validate(actor_id, control_id, fence, monotonic_now)?;
        let released = self.current.take().expect("validated").lease;
        let promoted = self.promote(wall_now_ns, monotonic_now);
        Ok(ReleaseOutcome { released, promoted })
    }

    pub fn validate(
        &self,
        actor_id: &str,
        control_id: Uuid,
        fence: u64,
        monotonic_now: Instant,
    ) -> Result<&ControlLease, ControlError> {
        let current = self.current.as_ref().ok_or(ControlError::NotOwner)?;
        if current.lease.id != control_id
            || current.lease.fence != fence
            || current.lease.epoch != self.daemon_epoch
            || current.lease.generation != self.generation
        {
            return Err(ControlError::StaleFence);
        }
        if current.lease.owner.id != actor_id {
            return Err(ControlError::NotOwner);
        }
        if monotonic_now >= current.deadline {
            return Err(ControlError::Expired);
        }
        Ok(&current.lease)
    }

    pub fn expire(&mut self, wall_now_ns: i64, monotonic_now: Instant) -> Option<ReleaseOutcome> {
        self.queue.retain(|waiter| monotonic_now < waiter.deadline);
        if !self
            .current
            .as_ref()
            .is_some_and(|active| monotonic_now >= active.deadline)
        {
            return None;
        }
        let released = self.current.take().expect("checked above").lease;
        let promoted = self.promote(wall_now_ns, monotonic_now);
        Some(ReleaseOutcome { released, promoted })
    }

    pub fn disconnect(
        &mut self,
        actor_id: &str,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Option<ReleaseOutcome> {
        self.queue.retain(|waiter| waiter.actor.id != actor_id);
        if !self
            .current
            .as_ref()
            .is_some_and(|active| active.lease.owner.id == actor_id)
        {
            return None;
        }
        let released = self.current.take().expect("checked above").lease;
        let promoted = self.promote(wall_now_ns, monotonic_now);
        Some(ReleaseOutcome { released, promoted })
    }

    pub fn change_generation(
        &mut self,
        generation: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Option<ReleaseOutcome> {
        self.generation = generation;
        self.queue.clear();
        let released = self.current.take()?.lease;
        let promoted = self.promote(wall_now_ns, monotonic_now);
        Some(ReleaseOutcome { released, promoted })
    }

    fn grant(
        &mut self,
        actor: Actor,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> ControlLease {
        let ttl_ms = clamp_ttl(ttl_ms);
        let lease = ControlLease {
            id: Uuid::new_v4(),
            owner: actor,
            epoch: self.daemon_epoch,
            generation: self.generation,
            fence: self.next_fence,
            issued_wall_time_ns: wall_now_ns,
            expires_wall_time_ns: wall_expiry(wall_now_ns, ttl_ms),
        };
        self.next_fence = self.next_fence.saturating_add(1);
        self.current = Some(ActiveLease {
            lease: lease.clone(),
            deadline: monotonic_expiry(monotonic_now, ttl_ms),
        });
        lease
    }

    fn promote(&mut self, wall_now_ns: i64, monotonic_now: Instant) -> Option<ControlLease> {
        while let Some(waiter) = self.queue.pop_front() {
            if monotonic_now < waiter.deadline {
                return Some(self.grant(waiter.actor, waiter.ttl_ms, wall_now_ns, monotonic_now));
            }
        }
        None
    }
}

fn clamp_ttl(ttl_ms: u64) -> u64 {
    ttl_ms.clamp(MIN_TTL_MS, MAX_TTL_MS)
}

fn wall_expiry(now_ns: i64, ttl_ms: u64) -> i64 {
    now_ns.saturating_add((clamp_ttl(ttl_ms) as i64).saturating_mul(1_000_000))
}

fn monotonic_expiry(now: Instant, ttl_ms: u64) -> Instant {
    now.checked_add(Duration::from_millis(clamp_ttl(ttl_ms)))
        .expect("bounded control TTL must fit in Instant")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::ActorKind;

    fn actor(id: &str) -> Actor {
        Actor {
            id: id.into(),
            label: id.into(),
            kind: ActorKind::Human,
        }
    }

    #[test]
    fn queue_promotes_in_order() {
        let epoch = Uuid::new_v4();
        let mut state = ControlState::new(epoch, 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(first) =
            state.acquire(actor("a"), ControlMode::Queue, 30_000, 0, monotonic_now)
        else {
            panic!("expected grant");
        };
        assert_eq!(
            state.acquire(actor("b"), ControlMode::Queue, 30_000, 0, monotonic_now,),
            AcquireOutcome::Queued { position: 1 }
        );
        let released = state
            .release("a", first.id, first.fence, 1, monotonic_now)
            .unwrap();
        assert_eq!(released.promoted.unwrap().owner.id, "b");
    }

    #[test]
    fn control_wait_queue_is_bounded() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let now = Instant::now();
        let AcquireOutcome::Granted(_) =
            state.acquire(actor("owner"), ControlMode::Queue, 10_000, 0, now)
        else {
            panic!("first actor should hold control");
        };
        for index in 0..MAX_WAITERS {
            assert!(matches!(
                state.acquire(
                    actor(&format!("waiter-{index}")),
                    ControlMode::Queue,
                    10_000,
                    0,
                    now,
                ),
                AcquireOutcome::Queued { .. }
            ));
        }
        assert_eq!(
            state.acquire(actor("one-too-many"), ControlMode::Queue, 10_000, 0, now,),
            AcquireOutcome::QueueFull
        );
    }

    #[test]
    fn expired_waiter_is_never_promoted_late() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let now = Instant::now();
        let AcquireOutcome::Granted(owner) =
            state.acquire(actor("owner"), ControlMode::Queue, MAX_TTL_MS, 0, now)
        else {
            panic!("first actor should hold control");
        };
        assert!(matches!(
            state.acquire(
                actor("short-lived-waiter"),
                ControlMode::Queue,
                MIN_TTL_MS,
                0,
                now,
            ),
            AcquireOutcome::Queued { position: 1 }
        ));

        let after_waiter_deadline = now + WAIT_TIMEOUT + Duration::from_millis(1);
        let released = state
            .expire(1, after_waiter_deadline)
            .expect("owner and waiter should expire together");
        assert_eq!(released.released.id, owner.id);
        assert!(released.promoted.is_none());
        assert!(state.current().is_none());
    }

    #[test]
    fn takeover_invalidates_old_fence() {
        let epoch = Uuid::new_v4();
        let mut state = ControlState::new(epoch, 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(first) =
            state.acquire(actor("a"), ControlMode::Queue, 30_000, 0, monotonic_now)
        else {
            panic!("expected grant");
        };
        let AcquireOutcome::TakenOver { granted, .. } =
            state.acquire(actor("b"), ControlMode::Takeover, 30_000, 1, monotonic_now)
        else {
            panic!("expected takeover");
        };
        assert!(granted.fence > first.fence);
        assert_eq!(
            state.validate("a", first.id, first.fence, monotonic_now),
            Err(ControlError::StaleFence)
        );
    }

    #[test]
    fn generation_change_revokes_control() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(first) =
            state.acquire(actor("a"), ControlMode::Queue, 30_000, 0, monotonic_now)
        else {
            panic!("expected grant");
        };
        let revoked = state.change_generation(2, 1, monotonic_now).unwrap();
        assert_eq!(revoked.released.id, first.id);
        assert!(state.current().is_none());
    }

    #[test]
    fn forward_wall_clock_jump_does_not_expire_lease_early() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(lease) = state.acquire(
            actor("a"),
            ControlMode::Queue,
            30_000,
            1_000_000_000,
            monotonic_now,
        ) else {
            panic!("expected grant");
        };

        let wall_after_jump = i64::MAX;
        let monotonic_after_one_second = monotonic_now + Duration::from_secs(1);
        assert!(
            state
                .validate("a", lease.id, lease.fence, monotonic_after_one_second)
                .is_ok()
        );
        assert!(
            state
                .expire(wall_after_jump, monotonic_after_one_second)
                .is_none()
        );
    }

    #[test]
    fn backward_wall_clock_jump_does_not_extend_lease() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(lease) = state.acquire(
            actor("a"),
            ControlMode::Queue,
            MIN_TTL_MS,
            10_000_000_000,
            monotonic_now,
        ) else {
            panic!("expected grant");
        };

        let wall_after_jump = i64::MIN;
        let monotonic_at_deadline = monotonic_now + Duration::from_millis(MIN_TTL_MS);
        assert_eq!(
            state.validate("a", lease.id, lease.fence, monotonic_at_deadline),
            Err(ControlError::Expired)
        );
        assert_eq!(
            state
                .expire(wall_after_jump, monotonic_at_deadline)
                .expect("monotonic deadline should expire the lease")
                .released
                .id,
            lease.id
        );
    }

    #[test]
    fn acquire_does_not_implicitly_expire_or_promote() {
        let mut state = ControlState::new(Uuid::new_v4(), 1);
        let monotonic_now = Instant::now();
        let AcquireOutcome::Granted(first) =
            state.acquire(actor("a"), ControlMode::Queue, MIN_TTL_MS, 0, monotonic_now)
        else {
            panic!("expected grant");
        };
        assert_eq!(
            state.acquire(actor("b"), ControlMode::Queue, MAX_TTL_MS, 0, monotonic_now,),
            AcquireOutcome::Queued { position: 1 }
        );

        let after_deadline = monotonic_now + Duration::from_millis(MIN_TTL_MS + 1);
        assert_eq!(
            state.acquire(
                actor("c"),
                ControlMode::Queue,
                MIN_TTL_MS,
                i64::MAX,
                after_deadline,
            ),
            AcquireOutcome::Queued { position: 2 }
        );
        assert_eq!(
            state.current().expect("lease is still present").id,
            first.id
        );

        let expired = state
            .expire(i64::MIN, after_deadline)
            .expect("caller explicitly applies expiration");
        assert_eq!(expired.released.id, first.id);
        assert_eq!(
            expired.promoted.expect("first waiter is promoted").owner.id,
            "b"
        );
    }
}
