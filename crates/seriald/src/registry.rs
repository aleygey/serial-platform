use crate::config::{ConfigValidationError, MAX_SLOT_IDENTITIES_PER_DAEMON, validate_slots};
use crate::journal::JournalHandle;
use crate::slot::{SlotError, SlotHandle};
use serial_protocol::{DeviceProfile, SlotConfig, SlotSnapshot};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use uuid::Uuid;

#[derive(Clone)]
pub struct SlotRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    daemon_epoch: Uuid,
    daemon_started: Instant,
    journal: JournalHandle,
    slots: RwLock<SlotMaps>,
    mutation: Arc<Mutex<RegistryMutation>>,
}

#[derive(Clone, Default)]
struct SlotMaps {
    active: HashMap<String, SlotHandle>,
    retired: HashMap<String, SlotHandle>,
}

struct RegistryMutation {
    lifecycle: RegistryLifecycle,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RegistryLifecycle {
    Running,
    Degraded,
    Shutdown,
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error(transparent)]
    InvalidConfig(#[from] ConfigValidationError),
    #[error(transparent)]
    Slot(#[from] SlotError),
    #[error("this daemon epoch would retain {requested} Slot identities; the maximum is {limit}")]
    IdentityLimit { requested: usize, limit: usize },
    #[error("the Slot registry has shut down")]
    Shutdown,
    #[error("the Slot registry is degraded after an abandoned or failed rollback")]
    Degraded,
    #[error(
        "Slot reconfiguration failed ({apply}); restoring the old runtime also failed ({rollback})"
    )]
    ApplyRollback {
        apply: SlotError,
        rollback: RegistryRollbackError,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("{details}")]
pub struct RegistryRollbackError {
    details: String,
}

/// A runtime replacement whose actor changes are staged while the Registry
/// mutation gate remains held. The caller must commit it after persistence or
/// explicitly roll it back.
pub struct AppliedSlotReplacement {
    registry: SlotRegistry,
    gate: Option<OwnedMutexGuard<RegistryMutation>>,
    candidate: Option<SlotMaps>,
    staged_handles: Vec<SlotHandle>,
    new_handles: Vec<SlotHandle>,
    completed: bool,
}

impl AppliedSlotReplacement {
    /// Activates staged actors only after persistence succeeded, then
    /// atomically publishes the candidate active/retired maps.
    pub async fn commit(mut self) -> Result<Vec<SlotSnapshot>, RegistryError> {
        for handle in self.staged_handles.iter().chain(self.new_handles.iter()) {
            if let Err(error) = handle.commit_staged_reconfiguration().await {
                if let Some(gate) = self.gate.as_mut() {
                    gate.lifecycle = RegistryLifecycle::Degraded;
                }
                self.completed = true;
                self.gate.take();
                return Err(RegistryError::Slot(error));
            }
        }
        let candidate = self
            .candidate
            .take()
            .expect("an applied replacement has candidate maps");
        let snapshots = sorted_snapshots(&candidate.active);
        *self.registry.inner.slots.write().await = candidate;
        self.completed = true;
        self.gate.take();
        Ok(snapshots)
    }

    /// Restores all previously active and retired actors and closes every
    /// actor created only for this staged replacement.
    pub async fn rollback(mut self) -> Result<(), RegistryRollbackError> {
        self.candidate.take();
        let result =
            rollback_actors(&self.staged_handles, std::mem::take(&mut self.new_handles)).await;
        if result.is_err()
            && let Some(gate) = self.gate.as_mut()
        {
            gate.lifecycle = RegistryLifecycle::Degraded;
        }
        self.completed = true;
        self.gate.take();
        result
    }

    async fn fail_apply(self, apply: SlotError) -> RegistryError {
        match self.rollback().await {
            Ok(()) => RegistryError::Slot(apply),
            Err(rollback) => RegistryError::ApplyRollback { apply, rollback },
        }
    }
}

impl Drop for AppliedSlotReplacement {
    fn drop(&mut self) {
        if !self.completed
            && let Some(gate) = self.gate.as_mut()
        {
            // Async rollback cannot run from Drop. Refuse later mutations
            // instead of pretending the partially changed runtime is safe.
            gate.lifecycle = RegistryLifecycle::Degraded;
        }
    }
}

impl SlotRegistry {
    pub fn new(
        daemon_epoch: Uuid,
        daemon_started: Instant,
        journal: JournalHandle,
        configs: Vec<SlotConfig>,
        device_profiles: Vec<DeviceProfile>,
    ) -> Self {
        validate_slots(&configs, &device_profiles)
            .expect("SlotRegistry requires validated Slot configuration");
        let active = configs
            .into_iter()
            .map(|config| {
                let id = config.id.clone();
                let device_profile = find_device_profile(&device_profiles, &config);
                let handle = SlotHandle::spawn(
                    config,
                    device_profile,
                    daemon_epoch,
                    daemon_started,
                    journal.clone(),
                );
                (id, handle)
            })
            .collect();
        Self {
            inner: Arc::new(RegistryInner {
                daemon_epoch,
                daemon_started,
                journal,
                slots: RwLock::new(SlotMaps {
                    active,
                    retired: HashMap::new(),
                }),
                mutation: Arc::new(Mutex::new(RegistryMutation {
                    lifecycle: RegistryLifecycle::Running,
                })),
            }),
        }
    }

    pub fn daemon_epoch(&self) -> Uuid {
        self.inner.daemon_epoch
    }

    pub async fn get(&self, slot_id: &str) -> Option<SlotHandle> {
        self.inner.slots.read().await.active.get(slot_id).cloned()
    }

    pub async fn handles(&self) -> Vec<SlotHandle> {
        let mut handles = self
            .inner
            .slots
            .read()
            .await
            .active
            .values()
            .cloned()
            .collect::<Vec<_>>();
        handles.sort_by(|left, right| left.id().cmp(right.id()));
        handles
    }

    pub async fn snapshots(&self) -> Vec<SlotSnapshot> {
        self.handles()
            .await
            .into_iter()
            .map(|handle| handle.snapshot())
            .collect()
    }

    /// Stages a full configuration while preserving actors for every Slot ID
    /// seen during this daemon epoch. The mutation gate remains held in the
    /// returned receipt so persistence can decide between commit and rollback.
    pub async fn apply_replacement(
        &self,
        configs: Vec<SlotConfig>,
        device_profiles: Vec<DeviceProfile>,
    ) -> Result<AppliedSlotReplacement, RegistryError> {
        validate_slots(&configs, &device_profiles)?;
        let gate = self.inner.mutation.clone().lock_owned().await;
        match gate.lifecycle {
            RegistryLifecycle::Running => {}
            RegistryLifecycle::Degraded => return Err(RegistryError::Degraded),
            RegistryLifecycle::Shutdown => return Err(RegistryError::Shutdown),
        }

        let previous = self.inner.slots.read().await.clone();
        let requested = configs
            .iter()
            .cloned()
            .map(|config| (config.id.clone(), config))
            .collect::<HashMap<_, _>>();
        let identity_count = previous
            .active
            .keys()
            .chain(previous.retired.keys())
            .chain(requested.keys())
            .collect::<HashSet<_>>()
            .len();
        if identity_count > MAX_SLOT_IDENTITIES_PER_DAEMON {
            return Err(RegistryError::IdentityLimit {
                requested: identity_count,
                limit: MAX_SLOT_IDENTITIES_PER_DAEMON,
            });
        }

        let mut transaction = AppliedSlotReplacement {
            registry: self.clone(),
            gate: Some(gate),
            candidate: None,
            staged_handles: Vec::new(),
            new_handles: Vec::new(),
            completed: false,
        };

        let mut active_to_stage = previous
            .active
            .keys()
            .filter(|id| {
                requested
                    .get(*id)
                    .is_none_or(|config| previous.active[*id].snapshot().config != *config)
            })
            .cloned()
            .collect::<Vec<_>>();
        active_to_stage.sort();
        for id in active_to_stage {
            let handle = previous
                .active
                .get(&id)
                .expect("id came from active map")
                .clone();
            let result = if let Some(config) = requested.get(&id) {
                handle
                    .stage_reconfiguration(
                        config.clone(),
                        find_device_profile(&device_profiles, config),
                        true,
                    )
                    .await
            } else {
                handle.stage_removal().await
            };
            if let Err(error) = result {
                return Err(transaction.fail_apply(error).await);
            }
            transaction.staged_handles.push(handle);
        }

        // Retired actors remain parked while their candidate config is held
        // privately by the actor. They are not returned to the active map yet.
        let mut retired_to_activate = requested
            .keys()
            .filter(|id| previous.retired.contains_key(*id))
            .cloned()
            .collect::<Vec<_>>();
        retired_to_activate.sort();
        for id in retired_to_activate {
            let handle = previous
                .retired
                .get(&id)
                .expect("id came from retired map")
                .clone();
            let config = requested.get(&id).expect("id came from requested map");
            if let Err(error) = handle
                .stage_reconfiguration(
                    config.clone(),
                    find_device_profile(&device_profiles, config),
                    false,
                )
                .await
            {
                return Err(transaction.fail_apply(error).await);
            }
            transaction.staged_handles.push(handle);
        }

        let mut active = HashMap::with_capacity(configs.len());
        for config in configs {
            let id = config.id.clone();
            let existing_active = previous.active.get(&id).cloned();
            let existing_retired = previous.retired.get(&id).cloned();
            let handle = if let Some(existing) = existing_active {
                existing
            } else if let Some(existing) = existing_retired {
                existing
            } else {
                let device_profile = find_device_profile(&device_profiles, &config);
                let handle = SlotHandle::spawn_staged(
                    config,
                    device_profile,
                    self.inner.daemon_epoch,
                    self.inner.daemon_started,
                    self.inner.journal.clone(),
                );
                transaction.new_handles.push(handle.clone());
                handle
            };
            active.insert(id, handle);
        }

        let mut retired = previous.retired.clone();
        for id in requested.keys() {
            retired.remove(id);
        }
        for (id, handle) in &previous.active {
            if !requested.contains_key(id) {
                retired.insert(id.clone(), handle.clone());
            }
        }
        transaction.candidate = Some(SlotMaps { active, retired });
        Ok(transaction)
    }

    /// Convenience operation for callers that do not need a persistence phase.
    pub async fn replace(
        &self,
        configs: Vec<SlotConfig>,
        device_profiles: Vec<DeviceProfile>,
    ) -> Result<Vec<SlotSnapshot>, RegistryError> {
        self.apply_replacement(configs, device_profiles)
            .await?
            .commit()
            .await
    }

    /// Pushes a persisted device profile catalog into the live actors so
    /// snapshots resolve prompts from the new profiles. Slots and their ports
    /// are untouched; validation happened before persistence.
    pub async fn apply_device_profiles(&self, device_profiles: Vec<DeviceProfile>) {
        for handle in self.handles().await {
            let config = handle.snapshot().config;
            handle
                .set_device_profile(find_device_profile(&device_profiles, &config))
                .await;
        }
    }

    pub async fn disconnect_actor(&self, actor_id: &str) {
        for handle in self.handles().await {
            handle.disconnect_actor(actor_id.to_owned()).await;
        }
    }

    pub async fn shutdown(&self) {
        let mut gate = self.inner.mutation.clone().lock_owned().await;
        if gate.lifecycle == RegistryLifecycle::Shutdown {
            return;
        }
        gate.lifecycle = RegistryLifecycle::Shutdown;
        let handles = {
            let mut slots = self.inner.slots.write().await;
            let previous = std::mem::take(&mut *slots);
            previous
                .active
                .into_values()
                .chain(previous.retired.into_values())
                .collect::<Vec<_>>()
        };
        for handle in handles {
            handle.shutdown().await;
        }
    }
}

async fn rollback_actors(
    staged_handles: &[SlotHandle],
    new_handles: Vec<SlotHandle>,
) -> Result<(), RegistryRollbackError> {
    for handle in new_handles {
        handle.shutdown().await;
    }

    let mut errors = Vec::new();
    for handle in staged_handles.iter().rev() {
        if let Err(error) = handle.rollback_staged_reconfiguration().await {
            errors.push(format!("restore {}: {error}", handle.id()));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(RegistryRollbackError {
            details: errors.join("; "),
        })
    }
}

fn sorted_snapshots(active: &HashMap<String, SlotHandle>) -> Vec<SlotSnapshot> {
    let mut snapshots = active
        .values()
        .map(|handle| handle.snapshot())
        .collect::<Vec<_>>();
    snapshots.sort_by(|left, right| left.config.id.cmp(&right.config.id));
    snapshots
}

/// Resolves the device profile attached to one Slot. A missing name resolves
/// to `None`; configuration validation rejects unknown references before the
/// registry ever sees them.
fn find_device_profile(
    device_profiles: &[DeviceProfile],
    config: &SlotConfig,
) -> Option<DeviceProfile> {
    let name = config.device_profile.as_deref()?;
    device_profiles
        .iter()
        .find(|profile| profile.name == name)
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{JournalConfig, JournalManager};
    use serial_protocol::{EventKind, SerialSettings, SessionState};
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn disabled_slot(id: &str, display_name: &str) -> SlotConfig {
        let settings = SerialSettings {
            auto_open: false,
            ..SerialSettings::default()
        };
        SlotConfig {
            id: id.into(),
            display_name: display_name.into(),
            port: format!("COM_{id}"),
            profile: "generic-115200".into(),
            device_profile: None,
            enabled: false,
            settings,
        }
    }

    fn auto_open_slot(id: &str, display_name: &str) -> SlotConfig {
        SlotConfig {
            id: id.into(),
            display_name: display_name.into(),
            port: "__seriald_missing_candidate_port__".into(),
            profile: "generic-115200".into(),
            device_profile: None,
            enabled: true,
            settings: SerialSettings {
                auto_open: true,
                ..SerialSettings::default()
            },
        }
    }

    fn registry(journal: &JournalManager, configs: Vec<SlotConfig>) -> SlotRegistry {
        SlotRegistry::new(
            Uuid::new_v4(),
            Instant::now(),
            journal.handle(),
            configs,
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn reconfigure_preserves_epoch_sequence_and_live_channel() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let epoch = Uuid::new_v4();
        let registry = SlotRegistry::new(
            epoch,
            Instant::now(),
            journal.handle(),
            vec![disabled_slot("slot-1", "before")],
            Vec::new(),
        );
        let original = registry.get("slot-1").await.unwrap();
        let mut live = original.attach(None, 10).await.unwrap().live;

        let snapshots = registry
            .replace(vec![disabled_slot("slot-1", "after")], Vec::new())
            .await
            .unwrap();
        assert_eq!(snapshots[0].daemon_epoch, epoch);
        assert_eq!(snapshots[0].head_seq, 1);
        assert_eq!(snapshots[0].config.display_name, "after");
        let event = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, EventKind::SlotReconfigured);
        assert_eq!(event.seq, 1);

        let snapshots = registry
            .replace(vec![disabled_slot("slot-1", "after-again")], Vec::new())
            .await
            .unwrap();
        assert_eq!(snapshots[0].head_seq, 2);
        assert_eq!(original.snapshot().head_seq, 2);

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn removed_slot_is_retired_and_readded_with_the_same_timeline() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, vec![disabled_slot("slot-1", "before")]);
        let original = registry.get("slot-1").await.unwrap();
        let mut live = original.attach(None, 10).await.unwrap().live;

        registry.replace(Vec::new(), Vec::new()).await.unwrap();
        assert!(registry.get("slot-1").await.is_none());
        assert_eq!(registry.inner.slots.read().await.retired.len(), 1);
        let removed = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(removed.seq, 1);
        assert_eq!(removed.kind, EventKind::SlotRemoved);

        let snapshots = registry
            .replace(vec![disabled_slot("slot-1", "after")], Vec::new())
            .await
            .unwrap();
        assert_eq!(snapshots[0].head_seq, 2);
        assert_eq!(original.snapshot().head_seq, 2);
        let event = tokio::time::timeout(Duration::from_secs(1), live.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.seq, 2);
        assert_eq!(event.kind, EventKind::SlotReconfigured);

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn staged_rollback_discards_new_identity_and_restores_old_map() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, vec![disabled_slot("slot-old", "Old")]);
        let original = registry.get("slot-old").await.unwrap();
        let mut live = original.attach(None, 10).await.unwrap().live;

        let applied = registry
            .apply_replacement(vec![disabled_slot("slot-new", "New")], Vec::new())
            .await
            .unwrap();
        assert!(registry.get("slot-old").await.is_some());
        assert!(registry.get("slot-new").await.is_none());
        applied.rollback().await.unwrap();

        assert!(matches!(
            live.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(original.snapshot().head_seq, 0);

        let maps = registry.inner.slots.read().await;
        assert!(maps.active.contains_key("slot-old"));
        assert!(!maps.active.contains_key("slot-new"));
        assert!(maps.retired.is_empty());
        drop(maps);
        let snapshots = registry
            .replace(vec![disabled_slot("slot-new", "New")], Vec::new())
            .await
            .unwrap();
        assert_eq!(snapshots[0].head_seq, 0);

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn staged_candidate_is_hidden_from_concurrent_readers_and_cannot_open() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, vec![disabled_slot("slot-old", "Old")]);
        let original = registry.get("slot-old").await.unwrap();
        let mut live = original.attach(None, 10).await.unwrap().live;
        let replacement = disabled_slot("slot-old", "Candidate");
        let added = auto_open_slot("slot-new", "New candidate");

        let applied = registry
            .apply_replacement(vec![replacement, added], Vec::new())
            .await
            .unwrap();
        let new_candidate = applied.new_handles[0].clone();

        let (snapshots, old_handle, new_handle) = tokio::join!(
            registry.snapshots(),
            registry.get("slot-old"),
            registry.get("slot-new")
        );
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].config.display_name, "Old");
        assert_eq!(old_handle.unwrap().snapshot().config.display_name, "Old");
        assert!(new_handle.is_none());
        assert_eq!(
            new_candidate.snapshot().session_state,
            SessionState::Disabled
        );
        assert_eq!(new_candidate.snapshot().head_seq, 0);

        // A normal auto-open actor would have attempted this nonexistent port
        // on its first maintenance tick and emitted opening/failure events.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(new_candidate.snapshot().head_seq, 0);
        assert!(matches!(
            live.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        applied.rollback().await.unwrap();
        assert_eq!(original.snapshot().config.display_name, "Old");
        assert!(matches!(
            live.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_replacements_share_one_actor_and_one_sequence() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, Vec::new());
        let first = registry.replace(vec![disabled_slot("slot-1", "first")], Vec::new());
        let second = registry.replace(vec![disabled_slot("slot-1", "second")], Vec::new());
        let (first, second) = tokio::join!(first, second);
        first.unwrap();
        second.unwrap();

        let snapshot = registry.get("slot-1").await.unwrap().snapshot();
        assert_eq!(snapshot.head_seq, 1);
        let maps = registry.inner.slots.read().await;
        assert_eq!(maps.active.len(), 1);
        assert!(maps.retired.is_empty());
        drop(maps);

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn failed_transition_restores_already_paused_actors() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(
            &journal,
            vec![
                disabled_slot("slot-a", "a-before"),
                disabled_slot("slot-z", "z-before"),
            ],
        );
        registry.get("slot-z").await.unwrap().shutdown().await;

        let error = registry
            .replace(
                vec![
                    disabled_slot("slot-a", "a-after"),
                    disabled_slot("slot-z", "z-after"),
                ],
                Vec::new(),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RegistryError::ApplyRollback { .. } | RegistryError::Slot(SlotError::Closed)
        ));
        assert_eq!(
            registry
                .get("slot-a")
                .await
                .unwrap()
                .snapshot()
                .config
                .display_name,
            "a-before"
        );

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cumulative_identity_limit_is_checked_before_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, Vec::new());
        for index in 0..MAX_SLOT_IDENTITIES_PER_DAEMON {
            registry
                .replace(
                    vec![disabled_slot(
                        &format!("slot-{index}"),
                        &format!("Slot {index}"),
                    )],
                    Vec::new(),
                )
                .await
                .unwrap();
        }
        let before = registry.snapshots().await;
        let error = registry
            .replace(vec![disabled_slot("slot-over-limit", "over")], Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            RegistryError::IdentityLimit {
                requested,
                limit: MAX_SLOT_IDENTITIES_PER_DAEMON,
            } if requested == MAX_SLOT_IDENTITIES_PER_DAEMON + 1
        ));
        assert_eq!(registry.snapshots().await, before);

        registry.shutdown().await;
        journal.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_prevents_later_replacement() {
        let temporary = tempfile::tempdir().unwrap();
        let journal = JournalManager::open(JournalConfig::new(temporary.path())).unwrap();
        let registry = registry(&journal, vec![disabled_slot("slot-1", "before")]);
        registry.shutdown().await;
        assert!(matches!(
            registry
                .replace(vec![disabled_slot("slot-1", "after")], Vec::new())
                .await,
            Err(RegistryError::Shutdown)
        ));
        journal.shutdown().await.unwrap();
    }
}
