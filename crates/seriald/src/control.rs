use serial_protocol::{Actor, ControlLease, ControlMode};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Minimum TTL a client can request; also the floor for the configured TTL
/// ceiling.
pub const MIN_TTL_MS: u64 = 5_000;
/// Default lease TTL ceiling. Overridable through the daemon `[control]`
/// configuration.
pub const MAX_TTL_MS: u64 = 60_000;
/// Hard ceiling for a configured control lease. Longer ownership is not a
/// useful serial-console lease and makes monotonic deadline arithmetic harder
/// to bound defensively.
pub const MAX_CONTROL_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
/// Legacy v6 configuration default retained for config compatibility. Generic
/// control queueing is disabled in v7.
pub const MAX_WAITERS: usize = 128;
/// Default lifetime of a pending Run-start approval. This reuses the existing
/// `[control].wait_timeout` setting for backwards-compatible configuration.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(60);
/// Hard ceiling for a configured Run-start approval lifetime.
pub const MAX_CONTROL_WAIT_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Runtime control limits, usually derived from the daemon configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlLimits {
    pub max_ttl_ms: u64,
    pub wait_timeout: Duration,
    pub max_waiters: usize,
}

impl Default for ControlLimits {
    fn default() -> Self {
        Self {
            max_ttl_ms: MAX_TTL_MS,
            wait_timeout: WAIT_TIMEOUT,
            max_waiters: MAX_WAITERS,
        }
    }
}

impl ControlLimits {
    /// Applies the runtime safety bounds even when a caller constructs limits
    /// directly instead of loading a validated [`crate::config::ControlConfig`].
    #[must_use]
    pub fn bounded(self) -> Self {
        Self {
            max_ttl_ms: self.max_ttl_ms.clamp(MIN_TTL_MS, MAX_CONTROL_TTL_MS),
            wait_timeout: self.wait_timeout.min(MAX_CONTROL_WAIT_TIMEOUT),
            max_waiters: self.max_waiters,
        }
    }
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
    limits: ControlLimits,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AcquireOutcome {
    Granted(ControlLease),
    AlreadyHeld(ControlLease),
    Busy(ControlLease),
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
    #[error("write control is held by another actor")]
    Busy,
}

impl ControlState {
    pub fn new(daemon_epoch: Uuid, generation: u64, limits: ControlLimits) -> Self {
        Self {
            daemon_epoch,
            generation,
            next_fence: 1,
            current: None,
            limits: limits.bounded(),
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
                return AcquireOutcome::Busy(current.lease.clone());
            }

            let revoked = self.current.take().expect("checked above").lease;
            let granted = self.grant(actor, ttl_ms, wall_now_ns, monotonic_now);
            return AcquireOutcome::TakenOver { revoked, granted };
        }

        let granted = self.grant(actor, ttl_ms, wall_now_ns, monotonic_now);
        AcquireOutcome::Granted(granted)
    }

    /// Grants a lease only while the slot is idle. Unlike the legacy acquire
    /// RPC, this is the primitive used by atomic Run start and Human-command
    /// transitions, so it can never queue or revoke an unrelated owner.
    pub fn grant_if_idle(
        &mut self,
        actor: Actor,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Result<ControlLease, ControlError> {
        if self.current.is_some() {
            return Err(ControlError::Busy);
        }
        Ok(self.grant(actor, ttl_ms, wall_now_ns, monotonic_now))
    }

    /// Atomically replaces one exact, still-live lease with a lease for
    /// `actor`. This is the approval commit point: validation and transfer
    /// happen in one mutable Slot turn, with no idle interval in between.
    #[allow(clippy::too_many_arguments)]
    pub fn transfer_exact(
        &mut self,
        expected_owner_id: &str,
        expected_control_id: Uuid,
        expected_fence: u64,
        actor: Actor,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Result<(ControlLease, ControlLease), ControlError> {
        self.validate(
            expected_owner_id,
            expected_control_id,
            expected_fence,
            monotonic_now,
        )?;
        let revoked = self.current.take().expect("validated").lease;
        let granted = self.grant(actor, ttl_ms, wall_now_ns, monotonic_now);
        Ok((revoked, granted))
    }

    pub fn approval_timeout(&self) -> Duration {
        self.limits.wait_timeout
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
        let ttl_ms = self.clamp_ttl(ttl_ms);
        let expires_wall_time_ns = self.wall_expiry(wall_now_ns, ttl_ms);
        let deadline = self.monotonic_expiry(monotonic_now, ttl_ms);
        let current = self.current.as_mut().expect("validated");
        current.lease.expires_wall_time_ns = expires_wall_time_ns;
        current.deadline = deadline;
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
        let _ = (wall_now_ns, monotonic_now);
        Ok(ReleaseOutcome {
            released,
            promoted: None,
        })
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

    /// Returns the authoritative monotonic time remaining on a validated
    /// lease. Callers that start a bounded physical operation use this to
    /// reject work that cannot finish before the lease expires.
    pub fn remaining_ttl(
        &self,
        actor_id: &str,
        control_id: Uuid,
        fence: u64,
        monotonic_now: Instant,
    ) -> Result<Duration, ControlError> {
        self.validate(actor_id, control_id, fence, monotonic_now)?;
        Ok(self
            .current
            .as_ref()
            .expect("validated control has an active lease")
            .deadline
            .saturating_duration_since(monotonic_now))
    }

    /// Returns the monotonic time remaining on whichever lease is current.
    ///
    /// This intentionally does not authorize a caller. It is used only after
    /// another policy check has established an explicit relationship to the
    /// current owner, such as a Human cooperative write into an Agent Run.
    pub fn current_remaining_ttl(&self, monotonic_now: Instant) -> Result<Duration, ControlError> {
        let current = self.current.as_ref().ok_or(ControlError::NotOwner)?;
        if current.lease.epoch != self.daemon_epoch || current.lease.generation != self.generation {
            return Err(ControlError::StaleFence);
        }
        if monotonic_now >= current.deadline {
            return Err(ControlError::Expired);
        }
        Ok(current.deadline.saturating_duration_since(monotonic_now))
    }

    pub fn expire(&mut self, wall_now_ns: i64, monotonic_now: Instant) -> Option<ReleaseOutcome> {
        if self
            .current
            .as_ref()
            .is_none_or(|active| monotonic_now < active.deadline)
        {
            return None;
        }
        let released = self.current.take().expect("checked above").lease;
        let _ = wall_now_ns;
        Some(ReleaseOutcome {
            released,
            promoted: None,
        })
    }

    pub fn disconnect(
        &mut self,
        actor_id: &str,
        wall_now_ns: i64,
        _monotonic_now: Instant,
    ) -> Option<ReleaseOutcome> {
        if self
            .current
            .as_ref()
            .is_none_or(|active| active.lease.owner.id != actor_id)
        {
            return None;
        }
        let released = self.current.take().expect("checked above").lease;
        let _ = wall_now_ns;
        Some(ReleaseOutcome {
            released,
            promoted: None,
        })
    }

    pub fn change_generation(
        &mut self,
        generation: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> Option<ReleaseOutcome> {
        self.generation = generation;
        let released = self.current.take()?.lease;
        let _ = (wall_now_ns, monotonic_now);
        Some(ReleaseOutcome {
            released,
            promoted: None,
        })
    }

    fn grant(
        &mut self,
        actor: Actor,
        ttl_ms: u64,
        wall_now_ns: i64,
        monotonic_now: Instant,
    ) -> ControlLease {
        let ttl_ms = self.clamp_ttl(ttl_ms);
        let lease = ControlLease {
            id: Uuid::new_v4(),
            owner: actor,
            epoch: self.daemon_epoch,
            generation: self.generation,
            fence: self.next_fence,
            issued_wall_time_ns: wall_now_ns,
            expires_wall_time_ns: self.wall_expiry(wall_now_ns, ttl_ms),
        };
        self.next_fence = self.next_fence.saturating_add(1);
        self.current = Some(ActiveLease {
            lease: lease.clone(),
            deadline: self.monotonic_expiry(monotonic_now, ttl_ms),
        });
        lease
    }

    /// Generic acquire queueing is disabled in protocol v7. Retained for the
    /// legacy CancelAcquire RPC, which therefore always reports no removal.
    pub fn cancel(&mut self, actor_id: &str) -> bool {
        let _ = actor_id;
        false
    }

    fn clamp_ttl(&self, ttl_ms: u64) -> u64 {
        ttl_ms
            .max(MIN_TTL_MS)
            .min(self.limits.max_ttl_ms.max(MIN_TTL_MS))
    }

    fn wall_expiry(&self, now_ns: i64, ttl_ms: u64) -> i64 {
        let ttl_ns = self
            .clamp_ttl(ttl_ms)
            .saturating_mul(1_000_000)
            .min(i64::MAX as u64) as i64;
        now_ns.saturating_add(ttl_ns)
    }

    fn monotonic_expiry(&self, now: Instant, ttl_ms: u64) -> Instant {
        monotonic_deadline(now, Duration::from_millis(self.clamp_ttl(ttl_ms)))
    }
}

fn monotonic_deadline(now: Instant, duration: Duration) -> Instant {
    // Immediate expiry is the conservative fallback on platforms whose
    // Instant range cannot represent even a validated future duration.
    now.checked_add(duration).unwrap_or(now)
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
    fn protocol_v7_queue_mode_is_immediate_busy_and_never_promotes() {
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
        let now = Instant::now();
        let AcquireOutcome::Granted(owner) =
            state.acquire(actor("owner"), ControlMode::Queue, 30_000, 0, now)
        else {
            panic!("idle acquire must grant");
        };
        assert!(matches!(
            state.acquire(actor("other"), ControlMode::Queue, 30_000, 0, now),
            AcquireOutcome::Busy(lease) if lease.id == owner.id
        ));
        let released = state
            .release("owner", owner.id, owner.fence, 1, now)
            .unwrap();
        assert!(released.promoted.is_none());
        assert!(state.current().is_none());
    }

    #[test]
    fn exact_approval_transfer_is_atomic_and_fenced() {
        let mut state = ControlState::new(Uuid::new_v4(), 7, ControlLimits::default());
        let now = Instant::now();
        let human = state.grant_if_idle(actor("human"), 30_000, 0, now).unwrap();
        let agent = Actor {
            id: "agent".into(),
            label: "agent".into(),
            kind: ActorKind::Agent,
        };
        assert_eq!(
            state.transfer_exact(
                "human",
                human.id,
                human.fence.saturating_add(1),
                agent.clone(),
                30_000,
                1,
                now,
            ),
            Err(ControlError::StaleFence)
        );
        assert_eq!(state.current().unwrap().id, human.id);

        let (revoked, granted) = state
            .transfer_exact("human", human.id, human.fence, agent, 30_000, 2, now)
            .unwrap();
        assert_eq!(revoked.id, human.id);
        assert_eq!(granted.owner.kind, ActorKind::Agent);
        assert!(granted.fence > revoked.fence);
        assert_eq!(
            state.validate("human", human.id, human.fence, now),
            Err(ControlError::StaleFence)
        );
    }

    #[test]
    fn takeover_invalidates_old_fence() {
        let epoch = Uuid::new_v4();
        let mut state = ControlState::new(epoch, 1, ControlLimits::default());
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
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
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
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
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
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
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
    fn minimum_lease_reports_exact_monotonic_time_remaining() {
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
        let started = Instant::now();
        let AcquireOutcome::Granted(lease) =
            state.acquire(actor("a"), ControlMode::Queue, MIN_TTL_MS, 0, started)
        else {
            panic!("expected grant");
        };

        assert_eq!(
            state
                .remaining_ttl("a", lease.id, lease.fence, started)
                .unwrap(),
            Duration::from_millis(MIN_TTL_MS)
        );
        let near_expiry = started + Duration::from_millis(MIN_TTL_MS - 75);
        assert_eq!(
            state
                .remaining_ttl("a", lease.id, lease.fence, near_expiry)
                .unwrap(),
            Duration::from_millis(75)
        );
        assert_eq!(
            state.current_remaining_ttl(near_expiry).unwrap(),
            Duration::from_millis(75)
        );
        assert_eq!(
            state.remaining_ttl(
                "a",
                lease.id,
                lease.fence,
                started + Duration::from_millis(MIN_TTL_MS),
            ),
            Err(ControlError::Expired)
        );
        assert_eq!(
            state.current_remaining_ttl(started + Duration::from_millis(MIN_TTL_MS),),
            Err(ControlError::Expired)
        );
    }

    #[test]
    fn cancel_unknown_actor_returns_false() {
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
        let now = Instant::now();
        let AcquireOutcome::Granted(_) =
            state.acquire(actor("owner"), ControlMode::Queue, 30_000, 0, now)
        else {
            panic!("expected grant");
        };
        assert!(!state.cancel("ghost"));
    }

    #[test]
    fn cancel_does_not_remove_current_holder() {
        let mut state = ControlState::new(Uuid::new_v4(), 1, ControlLimits::default());
        let now = Instant::now();
        let AcquireOutcome::Granted(owner) =
            state.acquire(actor("owner"), ControlMode::Queue, 30_000, 0, now)
        else {
            panic!("expected grant");
        };
        assert!(!state.cancel("owner"));
        assert_eq!(state.current().expect("owner still holds").id, owner.id);
    }

    #[test]
    fn custom_limits_clamp_the_ttl_ceiling() {
        let limits = ControlLimits {
            max_ttl_ms: 10_000,
            ..ControlLimits::default()
        };
        let mut state = ControlState::new(Uuid::new_v4(), 1, limits);
        let now = Instant::now();
        let AcquireOutcome::Granted(lease) =
            state.acquire(actor("a"), ControlMode::Queue, 60_000, 0, now)
        else {
            panic!("expected grant");
        };
        assert_eq!(lease.expires_wall_time_ns, 10_000 * 1_000_000);
    }

    #[test]
    fn extreme_runtime_limits_bound_lease_and_approval_deadlines() {
        let limits = ControlLimits {
            max_ttl_ms: u64::MAX,
            wait_timeout: Duration::MAX,
            max_waiters: usize::MAX,
        };
        let mut state = ControlState::new(Uuid::new_v4(), 1, limits);
        assert_eq!(state.limits.max_ttl_ms, MAX_CONTROL_TTL_MS);
        assert_eq!(state.approval_timeout(), MAX_CONTROL_WAIT_TIMEOUT);
        let now = Instant::now();
        let lease = state
            .grant_if_idle(actor("owner"), u64::MAX, i64::MAX - 1, now)
            .unwrap();
        assert_eq!(lease.expires_wall_time_ns, i64::MAX);
    }

    #[test]
    fn unrepresentable_monotonic_deadline_fails_closed_without_panicking() {
        let now = Instant::now();
        assert!(now.checked_add(Duration::MAX).is_none());
        assert_eq!(monotonic_deadline(now, Duration::MAX), now);
    }
}
