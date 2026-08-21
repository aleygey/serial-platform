use crate::config::{ConfigValidationError, MAX_PORT_IDENTITIES_PER_DAEMON, validate_ports};
use crate::control::ControlLimits;
use crate::journal::JournalHandle;
use crate::slot::{SlotError, SlotHandle};
use serial_protocol::{
    ModelProfile, SlotConfig, SlotSnapshot, TransportProfile, resolve_model_settings,
    resolve_transport_settings,
};
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
    control_limits: ControlLimits,
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
    #[error("this daemon epoch would retain {requested} port identities; the maximum is {limit}")]
    IdentityLimit { requested: usize, limit: usize },
    #[error("the port registry has shut down")]
    Shutdown,
    #[error("the port registry is degraded after an abandoned or failed rollback")]
    Degraded,
    #[error(
        "port reconfiguration failed ({apply}); restoring the old runtime also failed ({rollback})"
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

/// A model-profile catalog refresh staged in every affected port actor while
/// the registry mutation gate remains held. Staging is inert: the caller must
/// persist the catalog and then commit, or explicitly roll it back.
pub struct AppliedModelProfileReplacement {
    gate: Option<OwnedMutexGuard<RegistryMutation>>,
    staged_handles: Vec<SlotHandle>,
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

impl AppliedModelProfileReplacement {
    /// Publishes the already-staged effective settings after persistence has
    /// succeeded. A staged actor stays alive under the registry lifecycle
    /// gate, so a commit failure is an internal runtime fault and degrades the
    /// registry just like a staged port replacement failure.
    pub async fn commit(mut self) -> Result<(), RegistryError> {
        for handle in &self.staged_handles {
            if let Err(error) = handle.commit_staged_reconfiguration().await {
                if let Some(gate) = self.gate.as_mut() {
                    gate.lifecycle = RegistryLifecycle::Degraded;
                }
                self.completed = true;
                self.gate.take();
                return Err(RegistryError::Slot(error));
            }
        }
        self.completed = true;
        self.gate.take();
        Ok(())
    }

    /// Discards every staged candidate without changing a live snapshot,
    /// sequence, control/Run state, or physical port.
    pub async fn rollback(mut self) -> Result<(), RegistryRollbackError> {
        let result = rollback_actors(&self.staged_handles, Vec::new()).await;
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

impl Drop for AppliedModelProfileReplacement {
    fn drop(&mut self) {
        if !self.completed
            && let Some(gate) = self.gate.as_mut()
        {
            // Async rollback cannot run from Drop. Block later mutations
            // rather than allowing an abandoned staged catalog to commit.
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
        transport_profiles: Vec<TransportProfile>,
        model_profiles: Vec<ModelProfile>,
        control_limits: ControlLimits,
    ) -> Self {
        validate_ports(&configs, &transport_profiles, &model_profiles)
            .expect("port registry requires validated port configuration");
        let active = configs
            .into_iter()
            .map(|config| {
                let id = config.port.clone();
                let transport_profile = find_transport_profile(&transport_profiles, &config);
                let model_profile = find_model_profile(&model_profiles, &config);
                let handle = SlotHandle::spawn(
                    config,
                    transport_profile,
                    model_profile,
                    control_limits,
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
                control_limits,
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

    pub async fn get(&self, port: &str) -> Option<SlotHandle> {
        self.inner.slots.read().await.active.get(port).cloned()
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

    /// Stages a full configuration while preserving actors for every port
    /// seen during this daemon epoch. The mutation gate remains held in the
    /// returned receipt so persistence can decide between commit and rollback.
    pub async fn apply_replacement(
        &self,
        configs: Vec<SlotConfig>,
        transport_profiles: Vec<TransportProfile>,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<AppliedSlotReplacement, RegistryError> {
        self.apply_replacement_with_source(
            configs,
            transport_profiles,
            model_profiles,
            "system:configuration".to_owned(),
        )
        .await
    }

    pub async fn apply_replacement_with_source(
        &self,
        configs: Vec<SlotConfig>,
        transport_profiles: Vec<TransportProfile>,
        model_profiles: Vec<ModelProfile>,
        source: String,
    ) -> Result<AppliedSlotReplacement, RegistryError> {
        validate_ports(&configs, &transport_profiles, &model_profiles)?;
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
            .map(|config| (config.port.clone(), config))
            .collect::<HashMap<_, _>>();
        let identity_count = previous
            .active
            .keys()
            .chain(previous.retired.keys())
            .chain(requested.keys())
            .collect::<HashSet<_>>()
            .len();
        if identity_count > MAX_PORT_IDENTITIES_PER_DAEMON {
            return Err(RegistryError::IdentityLimit {
                requested: identity_count,
                limit: MAX_PORT_IDENTITIES_PER_DAEMON,
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
                requested.get(*id).is_none_or(|config| {
                    let snapshot = previous.active[*id].snapshot();
                    let transport = find_transport_profile(&transport_profiles, config);
                    let model = find_model_profile(&model_profiles, config);
                    let baseline = serial_protocol::SerialSettings::default();
                    let expected_transport =
                        resolve_transport_settings(&baseline, transport.as_ref());
                    let expected_model = resolve_model_settings(&baseline, model.as_ref());
                    snapshot.config != *config
                        || snapshot.effective_transport != Some(expected_transport)
                        || snapshot.effective_shell_prompt != expected_model.shell_prompt
                        || snapshot.effective_uboot_prompt != expected_model.uboot_prompt
                        || snapshot.effective_write_eol.as_deref()
                            != Some(expected_model.write_eol.as_str())
                        || snapshot.effective_echo != Some(expected_model.echo)
                        || snapshot.effective_write_pacing != Some(expected_model.write_pacing)
                })
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
                        find_transport_profile(&transport_profiles, config),
                        find_model_profile(&model_profiles, config),
                        source.clone(),
                        true,
                    )
                    .await
            } else {
                handle.stage_removal(source.clone()).await
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
                    find_transport_profile(&transport_profiles, config),
                    find_model_profile(&model_profiles, config),
                    source.clone(),
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
            let id = config.port.clone();
            let existing_active = previous.active.get(&id).cloned();
            let existing_retired = previous.retired.get(&id).cloned();
            let handle = if let Some(existing) = existing_active {
                existing
            } else if let Some(existing) = existing_retired {
                existing
            } else {
                let transport_profile = find_transport_profile(&transport_profiles, &config);
                let model_profile = find_model_profile(&model_profiles, &config);
                let handle = SlotHandle::spawn_staged(
                    config,
                    transport_profile,
                    model_profile,
                    self.inner.control_limits,
                    self.inner.daemon_epoch,
                    self.inner.daemon_started,
                    self.inner.journal.clone(),
                    source.clone(),
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
        transport_profiles: Vec<TransportProfile>,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<Vec<SlotSnapshot>, RegistryError> {
        self.apply_replacement(configs, transport_profiles, model_profiles)
            .await?
            .commit()
            .await
    }

    /// Stages a validated model-profile catalog in every affected live actor.
    /// No snapshot/event/port state changes until the returned receipt commits,
    /// so actor failure here can be rolled back before persistence.
    pub async fn stage_model_profiles(
        &self,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<AppliedModelProfileReplacement, RegistryError> {
        let gate = self.inner.mutation.clone().lock_owned().await;
        match gate.lifecycle {
            RegistryLifecycle::Running => {}
            RegistryLifecycle::Degraded => return Err(RegistryError::Degraded),
            RegistryLifecycle::Shutdown => return Err(RegistryError::Shutdown),
        }
        let mut transaction = AppliedModelProfileReplacement {
            gate: Some(gate),
            staged_handles: Vec::new(),
            completed: false,
        };
        for handle in self.handles().await {
            let config = handle.snapshot().config;
            match handle
                .stage_model_profile(find_model_profile(&model_profiles, &config))
                .await
            {
                Ok(true) => transaction.staged_handles.push(handle),
                Ok(false) => {}
                Err(error) => return Err(transaction.fail_apply(error).await),
            }
        }
        Ok(transaction)
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
    snapshots.sort_by(|left, right| left.config.port.cmp(&right.config.port));
    snapshots
}

/// Resolves the model profile attached to one port. A missing name resolves
/// to `None`; configuration validation rejects unknown references before the
/// registry ever sees them.
fn find_model_profile(
    model_profiles: &[ModelProfile],
    config: &SlotConfig,
) -> Option<ModelProfile> {
    let name = config.model_profile.as_deref()?;
    model_profiles
        .iter()
        .find(|profile| profile.name == name)
        .cloned()
}

fn find_transport_profile(
    transport_profiles: &[TransportProfile],
    config: &SlotConfig,
) -> Option<TransportProfile> {
    let name = config.transport_profile.as_deref()?;
    transport_profiles
        .iter()
        .find(|profile| profile.name == name)
        .cloned()
}
