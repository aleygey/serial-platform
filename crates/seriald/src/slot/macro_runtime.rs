//! Daemon-owned VM driver. Every physical command still crosses the Slot's
//! single writer and lease/context checks; no client-side macro execution.
use super::*;
use crate::macros::PreparedMacro;
use serial_macro::{EffectKind, EffectResult, Step, Vm};
use serial_protocol::{EchoMode, MacroExecutionInfo, MacroStatus};
use std::sync::Mutex as StdMutex;

type SharedInfo = Arc<StdMutex<MacroExecutionInfo>>;

#[derive(Debug, Clone)]
struct Stop {
    status: MacroStatus,
    message: String,
}

pub(super) struct ActiveMacro {
    info: SharedInfo,
    control_id: Uuid,
    fence: u64,
    deadline: Instant,
    stop: watch::Sender<Option<Stop>>,
}

impl ActiveMacro {
    pub(super) fn id(&self) -> Uuid {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).id
    }
    pub(super) fn is_stopping(&self) -> bool {
        self.stop.borrow().is_some()
    }
    fn snapshot(&self) -> MacroExecutionInfo {
        self.info.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
    fn public_snapshot(&self) -> MacroExecutionInfo {
        let mut info = self.snapshot();
        if info.status.is_terminal() {
            info.status = if self.is_stopping() {
                MacroStatus::Stopping
            } else {
                MacroStatus::Running
            };
            info.completed_at_ns = None;
        }
        info
    }
    pub(super) fn add_tx_metadata(&self, metadata: &mut BTreeMap<String, Value>) {
        let info = self.snapshot();
        metadata.insert("macro_execution_id".into(), json!(info.id));
        metadata.insert("macro_id".into(), json!(info.macro_id));
        metadata.insert("macro_revision".into(), json!(info.revision));
        metadata.insert("macro_line".into(), json!(info.line));
        metadata.insert("macro_column".into(), json!(info.column));
    }
}

pub(super) struct MacroCommand {
    actor: Actor,
    kind: MacroCommandKind,
    reply: Reply,
}

enum MacroCommandKind {
    #[cfg(all(test, unix))]
    InstallPty {
        stream: SerialStream,
    },
    Start {
        handle: SlotHandle,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Option<Uuid>,
        precondition: Option<SequenceWritePrecondition>,
        prepared: Box<PreparedMacro>,
    },
    Status {
        id: Uuid,
    },
    Cancel {
        id: Uuid,
        control_id: Uuid,
        fence: u64,
    },
    Write {
        id: Uuid,
        data: Vec<u8>,
        operation_id: Uuid,
        precondition: SequenceWritePrecondition,
    },
    Finished {
        id: Uuid,
    },
}

impl SlotHandle {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_macro(
        &self,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Option<Uuid>,
        precondition: Option<SequenceWritePrecondition>,
        prepared: PreparedMacro,
    ) -> Result<CommandResult, SlotError> {
        self.physical_request(Some(operation_id), "macro start", |reply| {
            SlotCommand::Macro(Box::new(MacroCommand {
                actor,
                reply,
                kind: MacroCommandKind::Start {
                    handle: self.clone(),
                    control_id,
                    fence,
                    daemon_epoch,
                    generation,
                    operation_id,
                    expected_run_id,
                    precondition,
                    prepared: Box::new(prepared),
                },
            }))
        })
        .await
    }
    pub async fn macro_status(&self, actor: Actor, id: Uuid) -> Result<CommandResult, SlotError> {
        self.request(|reply| {
            SlotCommand::Macro(Box::new(MacroCommand {
                actor,
                reply,
                kind: MacroCommandKind::Status { id },
            }))
        })
        .await
    }
    pub async fn cancel_macro(
        &self,
        actor: Actor,
        control_id: Uuid,
        fence: u64,
        id: Uuid,
    ) -> Result<CommandResult, SlotError> {
        self.request(|reply| {
            SlotCommand::Macro(Box::new(MacroCommand {
                actor,
                reply,
                kind: MacroCommandKind::Cancel {
                    id,
                    control_id,
                    fence,
                },
            }))
        })
        .await
    }
    async fn macro_write(
        &self,
        actor: Actor,
        id: Uuid,
        data: Vec<u8>,
        operation_id: Uuid,
        precondition: SequenceWritePrecondition,
    ) -> Result<CommandResult, SlotError> {
        self.physical_request(Some(operation_id), "macro command", |reply| {
            SlotCommand::Macro(Box::new(MacroCommand {
                actor,
                reply,
                kind: MacroCommandKind::Write {
                    id,
                    data,
                    operation_id,
                    precondition,
                },
            }))
        })
        .await
    }
}

impl SlotActor {
    pub(super) fn macro_write_deadline(&self, ordinary: Instant) -> tokio::time::Instant {
        tokio::time::Instant::from_std(
            self.active_macro
                .as_ref()
                .filter(|active| self.macro_write_authorized == Some(active.id()))
                .map_or(ordinary, |active| ordinary.min(active.deadline)),
        )
    }
    pub(super) fn stop_macro(&mut self, status: MacroStatus, message: &str) {
        if let Some(active) = self.active_macro.as_ref()
            && !active.is_stopping()
        {
            active.stop.send_replace(Some(Stop {
                status,
                message: message.into(),
            }));
            let mut info = active.info.lock().unwrap_or_else(|e| e.into_inner());
            info.status = MacroStatus::Stopping;
            info.message = Some(message.into());
        }
    }

    fn macro_info(&self, id: Uuid) -> Result<MacroExecutionInfo, SlotError> {
        if let Some(active) = &self.active_macro
            && active.id() == id
        {
            // Only the actor's Finished transition commits a terminal result
            // and releases the macro guard. A driver's provisional outcome
            // must not escape to a status poll before that linearization point.
            return Ok(active.public_snapshot());
        }
        self.terminal_macros
            .get(&id)
            .cloned()
            .ok_or(SlotError::MacroNotFound(id))
    }

    pub(super) async fn reap_macro(&mut self) {
        let Some(active) = &self.active_macro else {
            return;
        };
        if active.stop.receiver_count() != 0 {
            return;
        }
        {
            let mut info = active.info.lock().unwrap_or_else(|e| e.into_inner());
            if !info.status.is_terminal() {
                info.status = MacroStatus::Failed;
                info.message =
                    Some("macro driver stopped unexpectedly; do not automatically replay".into());
                info.outcome_uncertain = true;
                info.completed_at_ns = Some(wall_time_ns());
            }
        }
        self.finish_macro(active.id()).await;
    }

    async fn finish_macro(&mut self, id: Uuid) {
        if self
            .active_macro
            .as_ref()
            .is_none_or(|active| active.id() != id)
        {
            return;
        }
        let active = self.active_macro.take().expect("matching active macro");
        let mut info = active.snapshot();
        // A Human command queued before the driver's finish must win over a
        // provisional successful outcome, even when the final script was delay(0).
        if let Some(stop) = active.stop.borrow().as_ref() {
            info.status = stop.status;
            info.message = Some(stop.message.clone());
        }
        info.through_seq = self.seq;
        info.completed_at_ns.get_or_insert_with(wall_time_ns);
        self.emit(
            EventKind::Checkpoint,
            Direction::None,
            Vec::new(),
            Some(info.owner.clone()),
            Some(id),
            metadata([
                ("macro_execution", json!(info)),
                ("macro_phase", json!("completed")),
            ]),
        )
        .await;
        self.terminal_macros.insert(id, info);
        self.terminal_macro_order.push_back(id);
        while self.terminal_macro_order.len() > 128 {
            if let Some(old) = self.terminal_macro_order.pop_front() {
                self.terminal_macros.remove(&old);
            }
        }
    }

    pub(super) async fn handle_macro_command(&mut self, command: MacroCommand) {
        let MacroCommand { actor, kind, reply } = command;
        self.expire_control().await;
        let result = self.execute_macro_command(actor, kind).await;
        let _ = reply.send(result);
    }

    async fn execute_macro_command(
        &mut self,
        actor: Actor,
        kind: MacroCommandKind,
    ) -> Result<CommandResult, SlotError> {
        match kind {
            #[cfg(all(test, unix))]
            MacroCommandKind::InstallPty { stream } => {
                // PTYs do not implement DTR/RTS ioctls. Only substitute opening;
                // exercise the real daemon reader, writer, actor and journal.
                let (worker, events) = spawn_port_worker(stream);
                self.port = Some(worker);
                self.port_events = Some(events);
                self.generation += 1;
                self.control
                    .change_generation(self.generation, wall_time_ns(), Instant::now());
                self.config.enabled = true;
                self.session_state = SessionState::Online;
                self.emit(
                    EventKind::SerialOpened,
                    Direction::None,
                    Vec::new(),
                    Some(system_actor()),
                    None,
                    BTreeMap::new(),
                )
                .await;
                Ok(CommandResult::Pong {
                    server_wall_time_ns: wall_time_ns(),
                })
            }
            MacroCommandKind::Status { id } => Ok(CommandResult::MacroStatus {
                execution: Box::new(self.macro_info(id)?),
            }),
            MacroCommandKind::Cancel {
                id,
                control_id,
                fence,
            } => {
                let info = self.macro_info(id)?;
                if info.owner.id != actor.id {
                    return Err(ControlError::NotOwner.into());
                }
                if !info.status.is_terminal() {
                    self.control
                        .validate(&actor.id, control_id, fence, Instant::now())?;
                    self.stop_macro(MacroStatus::Cancelled, "macro stopped by its owner");
                }
                Ok(CommandResult::MacroCancelled {
                    execution: Box::new(self.macro_info(id)?),
                })
            }
            MacroCommandKind::Finished { id } => {
                if self.macro_info(id)?.owner.id != actor.id {
                    return Err(ControlError::NotOwner.into());
                }
                self.finish_macro(id).await;
                Ok(CommandResult::MacroStatus {
                    execution: Box::new(self.macro_info(id)?),
                })
            }
            MacroCommandKind::Start {
                handle,
                control_id,
                fence,
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                precondition,
                prepared,
            } => {
                self.control
                    .validate(&actor.id, control_id, fence, Instant::now())?;
                validate_expected_write_run(expected_run_id, &actor, self.active_run.as_ref())?;
                validate_agent_run_context(
                    &actor,
                    self.active_run.as_ref(),
                    self.run_context.as_ref(),
                )?;
                if actor.kind == ActorKind::Agent && expected_run_id.is_none() {
                    return Err(SlotError::NoActiveRun);
                }
                if daemon_epoch != self.daemon_epoch || generation != self.generation {
                    return Err(SlotError::GenerationMismatch);
                }
                if operation_id.is_nil() {
                    return Err(SlotError::MacroInvalid(
                        "operation_id must not be nil".into(),
                    ));
                }
                let fingerprint = serde_json::to_vec(&(
                    "macro",
                    daemon_epoch,
                    generation,
                    expected_run_id,
                    &prepared.spec,
                ))
                .map_err(|e| SlotError::MacroInvalid(e.to_string()))?;
                if let Some(cached) = self.write_request_cache.get(&operation_id) {
                    return if cached.fingerprint == fingerprint {
                        cached.result.clone()
                    } else {
                        Err(SlotError::RequestIdReused)
                    };
                }
                if self
                    .executed_write_ids
                    .was_executed_or_reserveable(operation_id)?
                {
                    return Err(SlotError::WriteResultExpired);
                }
                if self.active_macro.is_some() {
                    return Err(SlotError::MacroActive);
                }
                if self.active_trigger.is_some() {
                    return Err(SlotError::TriggerActive);
                }
                if self.port.is_none() {
                    return Err(SlotError::PortOffline);
                }
                if self.logging != LoggingState::Healthy {
                    return Err(SlotError::MacroInvalid("serial journal is degraded; repair evidence capture before starting a macro".into()));
                }
                if let Some(precondition) = &precondition {
                    validate_sequence_write_precondition(
                        precondition,
                        self.daemon_epoch,
                        self.generation,
                        self.tx_offset,
                        self.seq,
                        &*self.ring.lock().await,
                    )?;
                }
                if let Some(applies) = prepared
                    .definition
                    .as_ref()
                    .and_then(|d| d.applies_to.as_ref())
                    && (self.config.model_family.as_deref() != Some(&applies.model_family)
                        || (!applies.model_names.is_empty()
                            && !self
                                .config
                                .model_name
                                .as_ref()
                                .is_some_and(|n| applies.model_names.contains(n))))
                {
                    return Err(SlotError::MacroInvalid(
                        "macro does not apply to the selected port's model".into(),
                    ));
                }
                let settings =
                    resolve_model_settings(&SerialSettings::default(), self.model_profile.as_ref());
                if settings.write_eol.is_empty() {
                    return Err(SlotError::MacroInvalid(
                        "Macro Script v1 requires a nonempty Profile EOL".into(),
                    ));
                }
                let mut prompts = BTreeMap::new();
                if let Some(prompt) = &settings.shell_prompt {
                    prompts.insert("shell".into(), prompt.clone());
                }
                if let Some(prompt) = &settings.uboot_prompt {
                    prompts.insert("uboot".into(), prompt.clone());
                }
                let prompt_values: Vec<String> = prompts.values().cloned().collect();
                let vm = prepared
                    .program
                    .start(prepared.arguments.clone(), prompts)
                    .map_err(|e| SlotError::MacroInvalid(e.to_string()))?;
                let info = MacroExecutionInfo {
                    id: operation_id,
                    port: self.config.port.clone(),
                    daemon_epoch,
                    generation,
                    owner: actor.clone(),
                    run_id: expected_run_id,
                    macro_id: prepared.definition.as_ref().map(|d| d.id.clone()),
                    revision: prepared.definition.as_ref().map(|d| d.revision),
                    description: prepared.description,
                    status: MacroStatus::Running,
                    started_at_ns: wall_time_ns(),
                    completed_at_ns: None,
                    line: 1,
                    column: 1,
                    writes: 0,
                    bytes_written: 0,
                    first_seq: self.seq + 1,
                    through_seq: self.seq,
                    message: None,
                    outcome_uncertain: false,
                };
                let start_metadata = metadata([
                    ("macro_execution", json!(info)),
                    ("macro_phase", json!("started")),
                    ("macro_definition", json!(prepared.definition)),
                    ("macro_request", json!(prepared.spec)),
                    ("macro_profile", json!(settings)),
                    ("macro_model_family", json!(self.config.model_family)),
                    ("macro_model_name", json!(self.config.model_name)),
                ]);
                if serde_json::to_vec(&start_metadata)
                    .map_err(|e| SlotError::MacroInvalid(e.to_string()))?
                    .len()
                    > serial_protocol::MAX_HEADER_BYTES - 8192
                {
                    return Err(SlotError::MacroInvalid("combined source, arguments and Profile exceed the bounded audit record; shorten the macro or its parameters (no bytes written)".into()));
                }
                let shared = Arc::new(StdMutex::new(info.clone()));
                let (stop, stopped) = watch::channel(None);
                let deadline = Instant::now() + Duration::from_secs(prepared.spec.timeout_seconds);
                self.active_macro = Some(ActiveMacro {
                    info: shared.clone(),
                    control_id,
                    fence,
                    deadline,
                    stop,
                });
                let events = handle.events.subscribe();
                let initial_seq = self.seq;
                self.emit(
                    EventKind::Checkpoint,
                    Direction::None,
                    Vec::new(),
                    Some(actor.clone()),
                    Some(operation_id),
                    start_metadata,
                )
                .await;
                let driver = Driver {
                    handle,
                    actor,
                    id: operation_id,
                    info: shared,
                    stop: stopped,
                    vm,
                    events,
                    epoch: daemon_epoch,
                    generation,
                    deadline,
                    daemon_started: self.daemon_started,
                    observed_seq: initial_seq,
                    tx_offset: self.tx_offset,
                    eol: settings.write_eol,
                    echo: settings.echo,
                    watchers: HashMap::new(),
                    prompt_values,
                    echo_filter: EchoFilter::default(),
                    projection: Projection::default(),
                    total_bytes: 0,
                };
                // Spawn before acknowledgement: losing the client reply never
                // creates a second execution or an unowned, unstarted guard.
                tokio::spawn(driver.run());
                let result = Ok(CommandResult::MacroStarted {
                    execution: Box::new(info),
                });
                self.cache_write_result(operation_id, fingerprint, result.clone());
                result
            }
            MacroCommandKind::Write {
                id,
                data,
                operation_id,
                precondition,
            } => {
                let active = self
                    .active_macro
                    .as_ref()
                    .ok_or(SlotError::MacroNotFound(id))?;
                if active.id() != id || active.snapshot().owner.id != actor.id {
                    return Err(ControlError::NotOwner.into());
                }
                if active.is_stopping() {
                    return Err(SlotError::MacroActive);
                }
                if Instant::now() >= active.deadline {
                    return Err(SlotError::MacroInvalid(
                        "macro deadline reached before write".into(),
                    ));
                }
                let control_id = active.control_id;
                let fence = active.fence;
                let info = active.snapshot();
                // The reader barrier drains bytes that were already buffered
                // before TX, so a fresh watch cannot match stale output.
                self.flush_pretrigger_rx().await?;
                if self
                    .active_macro
                    .as_ref()
                    .is_none_or(|active| active.is_stopping() || Instant::now() >= active.deadline)
                {
                    return Err(SlotError::MacroInvalid(
                        "macro stopped or deadline reached before physical write".into(),
                    ));
                }
                validate_agent_run_context(
                    &actor,
                    self.active_run.as_ref(),
                    self.run_context.as_ref(),
                )?;
                self.macro_write_authorized = Some(id);
                let result = self
                    .execute(SlotRequest::Write {
                        actor,
                        control_id,
                        fence,
                        data,
                        operation_id: Some(operation_id),
                        expected_run_id: info.run_id,
                        pacing: None,
                        description: Some(info.description),
                        command_capture_matchers: Vec::new(),
                        command_sequence: None,
                        sequence_precondition: Some(precondition),
                        cooperative: false,
                    })
                    .await;
                self.macro_write_authorized = None;
                result
            }
        }
    }
}

struct Watcher {
    matcher: LiteralMatcher,
    boundary: Option<u64>,
    matched: bool,
    matched_at: Option<Instant>,
    prompt: bool,
}

struct Driver {
    handle: SlotHandle,
    actor: Actor,
    id: Uuid,
    info: SharedInfo,
    stop: watch::Receiver<Option<Stop>>,
    vm: Vm,
    events: broadcast::Receiver<TimelineEvent>,
    epoch: Uuid,
    generation: u64,
    deadline: Instant,
    daemon_started: Instant,
    observed_seq: u64,
    tx_offset: u64,
    eol: String,
    echo: EchoMode,
    watchers: HashMap<u64, Watcher>,
    prompt_values: Vec<String>,
    echo_filter: EchoFilter,
    projection: Projection,
    total_bytes: usize,
}

type DriverResult<T> = Result<T, Stop>;
fn failed(message: impl Into<String>) -> Stop {
    Stop {
        status: MacroStatus::Failed,
        message: message.into(),
    }
}

impl Driver {
    fn check(&self) -> DriverResult<()> {
        if let Some(stop) = self.stop.borrow().clone() {
            return Err(stop);
        }
        if Instant::now() >= self.deadline {
            return Err(Stop {
                status: MacroStatus::TimedOut,
                message: "macro overall deadline reached".into(),
            });
        }
        Ok(())
    }
    async fn run(mut self) {
        let result = self.execute().await;
        {
            let span = self.vm.last_span();
            let mut info = self.info.lock().unwrap_or_else(|e| e.into_inner());
            info.line = span.line;
            info.column = span.column;
            match result {
                Ok(()) => {
                    info.status = MacroStatus::Succeeded;
                }
                Err(stop) => {
                    info.status = stop.status;
                    info.message = Some(stop.message);
                }
            }
            info.completed_at_ns = Some(wall_time_ns());
            info.through_seq = self.observed_seq;
        }
        let actor = self.actor.clone();
        let id = self.id;
        let _ = self
            .handle
            .request(|reply| {
                SlotCommand::Macro(Box::new(MacroCommand {
                    actor,
                    reply,
                    kind: MacroCommandKind::Finished { id },
                }))
            })
            .await;
    }
    fn event(&mut self, event: TimelineEvent) -> DriverResult<()> {
        if event.seq <= self.observed_seq && event.daemon_epoch == self.epoch {
            return Ok(());
        }
        if event.daemon_epoch != self.epoch || event.generation != self.generation {
            return Err(failed("serial epoch/generation changed; macro stopped"));
        }
        if event.seq != self.observed_seq + 1 {
            return Err(failed(
                "serial timeline is incomplete; matching is not trustworthy",
            ));
        }
        self.observed_seq = event.seq;
        match event.kind {
            EventKind::Gap | EventKind::LoggingDegraded => {
                return Err(failed(
                    "serial evidence gap or degraded journal; macro stopped",
                ));
            }
            EventKind::SerialClosed | EventKind::PortRemoved => {
                return Err(failed("serial port disconnected"));
            }
            EventKind::ControlExpired | EventKind::ControlReleased | EventKind::ControlRevoked => {
                return Err(failed("macro control lease was lost"));
            }
            EventKind::Tx
                if event
                    .metadata
                    .get("macro_execution_id")
                    .and_then(Value::as_str)
                    != Some(&self.id.to_string()) =>
            {
                return Err(Stop {
                    status: MacroStatus::InterruptedByUser,
                    message:
                        "another actor wrote to the serial port; read context before continuing"
                            .into(),
                });
            }
            EventKind::Rx => {
                if !self
                    .watchers
                    .values()
                    .any(|watch| !watch.matched && watch.boundary.is_some())
                {
                    self.projection.feed_unobserved(&event.data);
                    self.info
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .through_seq = self.observed_seq;
                    return Ok(());
                }
                let visible = self.projection.feed(&event.data)?;
                let visible = self.echo_filter.feed(&visible)?;
                for (id, watch) in &mut self.watchers {
                    if !watch.matched && watch.boundary.is_some_and(|seq| event.seq > seq) {
                        watch.matched = watch.matcher.feed(&visible, watch.prompt);
                        if watch.matched {
                            watch.matched_at = self
                                .daemon_started
                                .checked_add(Duration::from_nanos(event.monotonic_time_ns));
                            self.vm
                                .mark_matched(*id)
                                .map_err(|e| failed(e.to_string()))?;
                        }
                    }
                }
            }
            _ => {}
        }
        self.info
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .through_seq = self.observed_seq;
        Ok(())
    }
    async fn drain(&mut self) -> DriverResult<()> {
        let mut batch = 0;
        loop {
            self.check()?;
            if batch == 128 {
                tokio::task::yield_now().await;
                batch = 0;
            }
            match self.events.try_recv() {
                Ok(event) => {
                    self.event(event)?;
                    batch += 1;
                }
                Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    return Err(failed("macro RX subscriber overflow; evidence incomplete"));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return Err(failed("serial timeline closed"));
                }
            }
        }
    }
    async fn receive_until(&mut self, until: Instant) -> DriverResult<bool> {
        self.check()?;
        tokio::select! {
            biased;
            changed = self.stop.changed() => {
                changed.map_err(|_| failed("macro owner stopped"))?;
                self.check()?;
                Ok(true)
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(until.min(self.deadline))) => {
                self.check()?;
                Ok(false)
            }
            event = self.events.recv() => {
                self.event(event.map_err(|e| failed(format!("macro RX stream lost: {e}")))?)?;
                Ok(true)
            }
        }
    }
    async fn execute(&mut self) -> DriverResult<()> {
        loop {
            self.check()?;
            self.drain().await?;
            match self.vm.advance().map_err(|e| failed(e.to_string()))? {
                Step::Complete => return Ok(()),
                Step::Yielded => tokio::task::yield_now().await,
                Step::Effect(effect) => {
                    {
                        let mut info = self.info.lock().unwrap_or_else(|e| e.into_inner());
                        info.line = effect.span.line;
                        info.column = effect.span.column;
                    }
                    let result = match effect.kind {
                        EffectKind::Watch { watcher, pattern } => {
                            self.watchers.insert(
                                watcher,
                                Watcher {
                                    matcher: LiteralMatcher::new(pattern.as_bytes()),
                                    boundary: None,
                                    matched: false,
                                    matched_at: None,
                                    prompt: self.prompt_values.contains(&pattern),
                                },
                            );
                            EffectResult::Done
                        }
                        EffectKind::Command { text } => {
                            let mut data = text.as_bytes().to_vec();
                            data.extend_from_slice(self.eol.as_bytes());
                            self.total_bytes = self.total_bytes.saturating_add(data.len());
                            if data.len() > MAX_WRITE_BYTES || self.total_bytes > 1024 * 1024 {
                                return Err(failed(
                                    "macro TX byte budget exceeded before write (including EOL)",
                                ));
                            }
                            self.check()?;
                            let precondition = SequenceWritePrecondition {
                                cursor: Cursor {
                                    epoch: self.epoch,
                                    after_seq: self.observed_seq,
                                },
                                expected_generation: self.generation,
                                expected_tx_offset: self.tx_offset,
                            };
                            let length = data.len();
                            // Do not drop an accepted physical request on cancellation.
                            // Wait for its bounded write outcome, then stop before the next.
                            let written = self
                                .handle
                                .macro_write(
                                    self.actor.clone(),
                                    self.id,
                                    data,
                                    Uuid::new_v4(),
                                    precondition,
                                )
                                .await;
                            let event_seq = match written {
                                Ok(CommandResult::WriteAccepted { event_seq }) => event_seq,
                                Ok(_) => return Err(failed("unexpected macro writer result")),
                                Err(error) => {
                                    if let SlotError::PartialWrite { written, .. } = &error {
                                        let mut info =
                                            self.info.lock().unwrap_or_else(|e| e.into_inner());
                                        if *written > 0 {
                                            info.writes += 1;
                                            info.bytes_written += *written as u64;
                                        }
                                    }
                                    if matches!(
                                        &error,
                                        SlotError::PartialWrite { .. }
                                            | SlotError::WriteOutcomeUncertain { .. }
                                    ) {
                                        self.info
                                            .lock()
                                            .unwrap_or_else(|e| e.into_inner())
                                            .outcome_uncertain = true;
                                    }
                                    self.check()?;
                                    return Err(failed(error.to_string()));
                                }
                            };
                            self.tx_offset += length as u64;
                            {
                                let mut info = self.info.lock().unwrap_or_else(|e| e.into_inner());
                                info.writes += 1;
                                info.bytes_written += length as u64;
                            }
                            // Process barrier-drained pre-TX RX without the new
                            // echo filter/watch. Bind immediately at the TX event.
                            while self.observed_seq < event_seq {
                                self.check()?;
                                let event = tokio::time::timeout_at(
                                    tokio::time::Instant::from_std(self.deadline),
                                    self.events.recv(),
                                )
                                .await
                                .map_err(|_| {
                                    failed("macro deadline reached while confirming TX evidence")
                                })?
                                .map_err(|e| failed(format!("macro TX evidence lost: {e}")))?;
                                self.event(event)?;
                            }
                            for watcher in self.watchers.values_mut() {
                                if watcher.boundary.is_none() {
                                    watcher.boundary = Some(event_seq);
                                }
                            }
                            self.echo_filter =
                                EchoFilter::new(&text, self.echo, &self.prompt_values);
                            EffectResult::Done
                        }
                        EffectKind::Wait {
                            watcher,
                            timeout_ms,
                            strict,
                        } => {
                            if self
                                .watchers
                                .get(&watcher)
                                .is_none_or(|w| w.boundary.is_none())
                            {
                                return Err(failed(
                                    "watch must be followed by cmd before wait/expect",
                                ));
                            }
                            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
                            loop {
                                self.drain().await?;
                                if self
                                    .watchers
                                    .get(&watcher)
                                    .is_some_and(|w| w.matched_at.is_some_and(|at| at <= deadline))
                                {
                                    break;
                                }
                                if Instant::now() >= deadline {
                                    break;
                                }
                                if !self.receive_until(deadline).await? {
                                    break;
                                }
                            }
                            let matched = self
                                .watchers
                                .get(&watcher)
                                .is_some_and(|w| w.matched_at.is_some_and(|at| at <= deadline));
                            if strict && !matched {
                                return Err(failed(format!(
                                    "expect timed out at {}:{}",
                                    effect.span.line, effect.span.column
                                )));
                            }
                            EffectResult::Matched(matched)
                        }
                        EffectKind::Delay { duration_ms } => {
                            let deadline = Instant::now() + Duration::from_millis(duration_ms);
                            while Instant::now() < deadline {
                                if !self.receive_until(deadline).await? {
                                    break;
                                }
                            }
                            EffectResult::Done
                        }
                    };
                    self.check()?;
                    self.vm
                        .resume(effect.id, result)
                        .map_err(|e| failed(e.to_string()))?;
                }
            }
        }
    }
}

// A bounded projection of observed RX text, not a reconstruction of the final
// terminal screen. SGR/OSC decoration is ignored and CR/CRLF delimit observed
// output, including progress lines later overwritten by a bare CR. Unsupported
// CSI cursor editing and backspace fail explicitly instead of being discarded.
#[derive(Default)]
struct Projection {
    escape: u8,
    escape_len: usize,
    cr: bool,
    poisoned: bool,
}
impl Projection {
    fn feed(&mut self, bytes: &[u8]) -> DriverResult<Vec<u8>> {
        self.feed_inner(bytes, true)
    }
    // Keep framing before a watcher is bound. A title begun before TX must
    // not leak its remaining payload as evidence after TX. Allocate no visible
    // output while idle, and keep consuming after unsupported cursor controls.
    fn feed_unobserved(&mut self, bytes: &[u8]) {
        let result = self.feed_inner(bytes, false);
        debug_assert!(result.is_ok());
    }
    fn feed_inner(&mut self, bytes: &[u8], strict: bool) -> DriverResult<Vec<u8>> {
        if strict && self.poisoned {
            return Err(failed(
                "terminal framing became uncertain before macro observation",
            ));
        }
        let mut out = if strict {
            Vec::with_capacity(bytes.len())
        } else {
            Vec::new()
        };
        for &byte in bytes {
            if self.escape != 0 {
                self.escape_len = self.escape_len.saturating_add(1).min(8193);
                if self.escape_len > 8192 {
                    self.poisoned = true;
                    if strict {
                        return Err(failed("unterminated terminal escape in macro evidence"));
                    }
                }
                // CAN/SUB cancel sequences. ESC restarts CSI/short escapes;
                // within string controls only ST (or OSC's BEL) ends the text.
                if matches!(byte, 0x18 | 0x1a) {
                    self.escape = 0;
                    continue;
                }
                if byte == 0x1b && matches!(self.escape, 1 | 2 | 7) {
                    self.escape = 1;
                    self.escape_len = 0;
                    continue;
                }
                match self.escape {
                    1 => match byte {
                        b'[' => self.escape = 2,
                        b']' => self.escape = 3,
                        b'P' | b'X' | b'^' | b'_' => {
                            self.escape = 5;
                            if strict {
                                return Err(failed(
                                    "unsupported terminal string in macro evidence",
                                ));
                            }
                        }
                        0x20..=0x2f => {
                            self.escape = 7;
                            if strict {
                                return Err(failed(
                                    "unsupported terminal escape in macro evidence",
                                ));
                            }
                        }
                        0x30..=0x7e => {
                            self.escape = 0;
                            if strict {
                                return Err(failed(
                                    "unsupported terminal escape in macro evidence",
                                ));
                            }
                        }
                        _ => {
                            self.escape = 0;
                            self.poisoned = true;
                            if strict {
                                return Err(failed("invalid terminal escape in macro evidence"));
                            }
                        }
                    },
                    2 if (0x40..=0x7e).contains(&byte) => {
                        self.escape = 0;
                        if byte != b'm' && strict {
                            return Err(failed(
                                "terminal cursor editing is not supported as macro completion evidence",
                            ));
                        }
                    }
                    3 if byte == 7 => self.escape = 0,
                    3 if byte == 0x1b => self.escape = 4,
                    4 if byte == b'\\' => self.escape = 0,
                    4 if byte == 0x1b => {}
                    4 => self.escape = 3,
                    5 | 6 => {
                        if strict {
                            return Err(failed(
                                "unsupported terminal string overlaps macro observation",
                            ));
                        }
                        self.escape = match (self.escape, byte) {
                            (6, b'\\') => 0,
                            (_, 0x1b) => 6,
                            _ => 5,
                        };
                    }
                    7 => {
                        if strict {
                            return Err(failed(
                                "unsupported terminal escape overlaps macro observation",
                            ));
                        }
                        if (0x30..=0x7e).contains(&byte) {
                            self.escape = 0;
                        } else if !(0x20..=0x2f).contains(&byte) {
                            self.poisoned = true;
                        }
                    }
                    _ => {}
                }
                continue;
            }
            match byte {
                0x1b => {
                    self.escape = 1;
                    self.escape_len = 0;
                }
                b'\r' => {
                    if strict {
                        out.push(b'\n');
                    }
                    self.cr = true;
                }
                b'\n' if self.cr => self.cr = false,
                b'\n' => {
                    if strict {
                        out.push(b'\n');
                    }
                }
                8 | 127 => {
                    if strict {
                        return Err(failed(
                            "terminal backspace editing makes macro completion evidence uncertain",
                        ));
                    }
                }
                0..=8 | 11..=12 | 14..=31 => {}
                _ => {
                    self.cr = false;
                    if strict {
                        out.push(byte);
                    }
                }
            }
        }
        Ok(out)
    }
}

#[derive(Default)]
struct EchoFilter {
    expected: Vec<u8>,
    pending: Vec<u8>,
    active: bool,
}
impl EchoFilter {
    fn new(command: &str, mode: EchoMode, _prompts: &[String]) -> Self {
        Self {
            expected: command.as_bytes().to_vec(),
            pending: Vec::new(),
            active: mode != EchoMode::Off && !command.is_empty(),
        }
    }
    fn feed(&mut self, bytes: &[u8]) -> DriverResult<Vec<u8>> {
        if self.expected.len() > MAX_WRITE_BYTES {
            return Err(failed(
                "echo expectation exceeds the physical command limit",
            ));
        }
        let mut out = Vec::new();
        for &byte in bytes {
            if !self.active {
                out.push(byte);
                continue;
            }
            if byte == b'\n' {
                // Only the complete, exact command at the fresh TX boundary
                // is an echo. Arbitrary leading text or a suffix equality is
                // not evidence: `failed command slp` must remain visible.
                if self.pending != self.expected {
                    out.append(&mut self.pending);
                    out.push(byte);
                } else {
                    self.pending.clear();
                }
                self.active = false;
            } else if self.pending.len() < self.expected.len()
                && byte == self.expected[self.pending.len()]
            {
                // In particular, an echoed command containing a configured
                // prompt remains withheld until its real line terminator.
                self.pending.push(byte);
            } else {
                // The first mismatch proves this is device output. Release
                // immediately, including a normal prompt with no newline.
                // Do not speculate that a leading prompt precedes an echo:
                // that is indistinguishable from a standalone live prompt.
                out.append(&mut self.pending);
                out.push(byte);
                self.active = false;
            }
        }
        Ok(out)
    }
}

struct LiteralMatcher {
    pattern: Vec<u8>,
    prefix: Vec<usize>,
    matched_prefix: usize,
    seen: bool,
}
impl LiteralMatcher {
    fn new(pattern: &[u8]) -> Self {
        let mut prefix = vec![0; pattern.len()];
        for i in 1..pattern.len() {
            let mut n = prefix[i - 1];
            while n > 0 && pattern[i] != pattern[n] {
                n = prefix[n - 1];
            }
            if pattern[i] == pattern[n] {
                n += 1;
            }
            prefix[i] = n;
        }
        Self {
            pattern: pattern.to_vec(),
            prefix,
            matched_prefix: 0,
            seen: false,
        }
    }
    fn feed(&mut self, bytes: &[u8], prompt: bool) -> bool {
        if self.pattern.is_empty() {
            return false;
        }
        for &byte in bytes {
            while self.matched_prefix > 0 && byte != self.pattern[self.matched_prefix] {
                self.matched_prefix = self.prefix[self.matched_prefix - 1];
            }
            if byte == self.pattern[self.matched_prefix] {
                self.matched_prefix += 1;
            }
            if self.matched_prefix == self.pattern.len() {
                self.seen = true;
                self.matched_prefix = self.prefix[self.matched_prefix - 1];
                if !prompt {
                    return true;
                }
            } else if prompt && byte != b' ' && byte != b'\t' && byte != b'\n' {
                self.seen = false;
            }
        }
        self.seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn literal_matches_utf8_across_chunks_and_overlaps() {
        let mut matcher = LiteralMatcher::new("aab中文".as_bytes());
        assert!(!matcher.feed(b"aaa", false));
        assert!(!matcher.feed(&"b中".as_bytes()[..3], false));
        assert!(matcher.feed(&"b中文".as_bytes()[3..], false));
    }
    #[test]
    fn projection_handles_split_sgr_and_crlf_but_rejects_editing() {
        let mut p = Projection::default();
        assert_eq!(
            p.feed(b"\x1b[3").unwrap_or_else(|_| panic!("projection")),
            b""
        );
        assert_eq!(
            p.feed(b"1mReady\r")
                .unwrap_or_else(|_| panic!("projection")),
            b"Ready\n"
        );
        assert_eq!(
            p.feed(b"\nOK").unwrap_or_else(|_| panic!("projection")),
            b"OK"
        );
        assert!(p.feed(b"\x1b[2K").is_err());
    }
    #[test]
    fn projection_preserves_observed_cr_progress_but_ignores_split_osc_decoration() {
        let mut p = Projection::default();
        assert_eq!(p.feed(b"Ready\rFAIL\r\n").unwrap(), b"Ready\nFAIL\n");
        assert_eq!(p.feed(b"\x1b]0;not ").unwrap(), b"");
        assert_eq!(p.feed(b"Ready\x1b").unwrap(), b"");
        assert_eq!(p.feed(b"\\real output").unwrap(), b"real output");
        assert_eq!(p.feed(b"\x1b]2;title\x07next").unwrap(), b"next");
    }
    #[test]
    fn projection_escape_and_echo_buffers_are_bounded() {
        let mut p = Projection::default();
        assert!(p.feed(b"\x1b]").unwrap().is_empty());
        assert!(p.feed(&vec![b'x'; 8192]).is_err());
        let mut filter = EchoFilter::new("bounded", EchoMode::Auto, &[]);
        assert!(filter.feed(b"bounded").unwrap().is_empty());
        assert_eq!(filter.pending.len(), "bounded".len());
        let output = vec![b'x'; 64 * 1024];
        assert_eq!(
            filter.feed(&output).unwrap().len(),
            output.len() + "bounded".len()
        );
        assert!(filter.pending.is_empty());
    }
    #[test]
    fn unobserved_projection_tracks_split_titles_and_consumes_after_idle_editing() {
        let mut p = Projection::default();
        // Unsupported editing before a watcher is not evidence and must not
        // prevent the rest of this chunk from registering the opening OSC.
        p.feed_unobserved(b"idle\x08\x1b[2K\x1b]0;hidden ");
        assert!(p.feed(b"Ready").unwrap().is_empty());
        assert_eq!(p.feed(b"\x1b\\real Ready").unwrap(), b"real Ready");
        p.feed_unobserved(b"\x1b[38;2;");
        assert_eq!(p.feed(b"10;20;30mReady").unwrap(), b"Ready");
        p.feed_unobserved(b"\x1b[\x1b]0;");
        assert!(p.feed(b"Ready\x07").unwrap().is_empty());
    }
    #[test]
    fn idle_unsupported_strings_are_consumed_or_fail_if_observation_starts_inside() {
        for prefix in *b"PX^_" {
            let mut p = Projection::default();
            p.feed_unobserved(&[0x1b, prefix]);
            p.feed_unobserved(b"payload Ready\x1b");
            p.feed_unobserved(b"\\\x1b]0;title ");
            assert!(p.feed(b"Ready\x07").unwrap().is_empty());
            let mut overlap = Projection::default();
            overlap.feed_unobserved(&[0x1b, prefix]);
            assert!(overlap.feed(b"Ready").is_err());
        }
        let mut oversized = Projection::default();
        oversized.feed_unobserved(b"\x1b]0;");
        oversized.feed_unobserved(&vec![b'x'; 16 * 1024]);
        oversized.feed_unobserved(b"\x07ordinary output");
        assert!(oversized.feed(b"Ready").is_err());
    }
    #[test]
    fn echoed_command_is_not_completion_evidence() {
        let mut filter = EchoFilter::new("echo Ready", EchoMode::On, &[]);
        assert!(filter.feed(b"echo Re").unwrap().is_empty());
        assert_eq!(filter.feed(b"ady\nReady\n").unwrap(), b"Ready\n");
        let mut off = EchoFilter::new("help", EchoMode::Off, &[]);
        assert_eq!(off.feed(b"help").unwrap(), b"help");
    }

    #[test]
    fn command_containing_or_equaling_prompt_cannot_satisfy_a_watcher_by_echo() {
        for command in ["echo U-Boot> ", "U-Boot> "] {
            let mut filter = EchoFilter::new(command, EchoMode::On, &["U-Boot> ".into()]);
            let mut matcher = LiteralMatcher::new(b"U-Boot> ");
            for byte in command.bytes() {
                let visible = filter.feed(&[byte]).unwrap();
                assert!(visible.is_empty());
                assert!(!matcher.feed(&visible, true));
            }
            assert!(filter.feed(b"\n").unwrap().is_empty());
            let visible = filter.feed(b"U-Boot> ").unwrap();
            assert!(matcher.feed(&visible, true));
        }
    }

    #[test]
    fn missing_echo_releases_bare_prompt_but_does_not_guess_through_ambiguous_prefix() {
        let mut filter = EchoFilter::new("slp", EchoMode::On, &["U-Boot> ".into()]);
        assert_eq!(filter.feed(b"U-Boot> ").unwrap(), b"U-Boot> ");
        let mut ambiguous = EchoFilter::new("U-Boot> reboot", EchoMode::On, &["U-Boot> ".into()]);
        // Without a terminator or a mismatching byte, these bytes could still
        // be the start of the echoed command. They must not become evidence.
        assert!(ambiguous.feed(b"U-Boot> ").unwrap().is_empty());
        assert_eq!(ambiguous.feed(b"\n").unwrap(), b"U-Boot> \n");
    }

    #[test]
    fn device_output_with_command_suffix_is_never_swallowed() {
        let mut filter = EchoFilter::new("slp", EchoMode::On, &[]);
        assert_eq!(
            filter.feed(b"failed command slp\n").unwrap(),
            b"failed command slp\n"
        );
        let mut prefix = EchoFilter::new("status", EchoMode::On, &[]);
        assert!(prefix.feed(b"stat").unwrap().is_empty());
        assert_eq!(prefix.feed(b"e failed\n").unwrap(), b"state failed\n");
    }

    #[test]
    fn sgr_decorated_split_echo_is_filtered_after_terminal_projection() {
        let mut projection = Projection::default();
        let mut filter = EchoFilter::new("echo U-Boot>", EchoMode::On, &[]);
        let first = projection.feed(b"\x1b[3").unwrap();
        assert!(filter.feed(&first).unwrap().is_empty());
        let second = projection.feed(b"2mecho U-Boot>\x1b[0m\r").unwrap();
        assert!(filter.feed(&second).unwrap().is_empty());
        let third = projection.feed(b"\nU-Boot>").unwrap();
        assert_eq!(filter.feed(&third).unwrap(), b"U-Boot>");
    }

    #[test]
    fn prompt_matcher_requires_the_observed_text_tail_and_preserves_literal_matching() {
        let mut prompt = LiteralMatcher::new(b"U-Boot>");
        assert!(!prompt.feed(b"U-Boot> still booting", true));
        assert!(!prompt.feed(b"\nU-Bo", true));
        assert!(prompt.feed(b"ot> ", true));
        let mut literal = LiteralMatcher::new(b"Ready");
        assert!(literal.feed(b"Ready then further logs", false));
    }

    #[cfg(unix)]
    mod integration {
        use super::*;
        use crate::journal::{JournalConfig, JournalManager};
        use crate::macros::MacroCatalog;
        use serial_protocol::{ControlLease, MacroRunSpec};

        struct Fixture {
            slot: SlotHandle,
            master: SerialStream,
            manager: JournalManager,
            directory: tempfile::TempDir,
            actor: Actor,
            lease: ControlLease,
            run_id: Uuid,
        }
        impl Fixture {
            async fn new() -> Self {
                let (master, mut slave) = SerialStream::pair().expect("PTY pair");
                slave
                    .set_exclusive(false)
                    .expect("PTY shared open for daemon");
                let port = slave.name().expect("PTY path");
                let directory = tempfile::tempdir().unwrap();
                let manager =
                    JournalManager::open(JournalConfig::new(directory.path().join("journal")))
                        .unwrap();
                let slot = SlotHandle::spawn(
                    SlotConfig {
                        port,
                        enabled: false,
                        transport_profile: None,
                        model_profile: Some("test".into()),
                        model_family: None,
                        model_name: None,
                    },
                    None,
                    Some(ModelProfile {
                        name: "test".into(),
                        shell_prompt: Some("# ".into()),
                        uboot_prompt: Some("U-Boot> ".into()),
                        write_eol: Some("\r".into()),
                        echo: Some(EchoMode::On),
                        write_chunk_size: Some(4096),
                        write_chunk_delay_ms: Some(0),
                    }),
                    ControlLimits::default(),
                    Uuid::new_v4(),
                    Instant::now(),
                    manager.handle(),
                );
                slot.request(|reply| {
                    SlotCommand::Macro(Box::new(MacroCommand {
                        actor: system_actor(),
                        reply,
                        kind: MacroCommandKind::InstallPty { stream: slave },
                    }))
                })
                .await
                .unwrap();
                let timeout = Instant::now() + Duration::from_secs(5);
                while slot.snapshot().session_state != SessionState::Online {
                    assert!(
                        Instant::now() < timeout,
                        "port failed to open: {:?}",
                        slot.snapshot()
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let actor = Actor {
                    id: "macro-test-agent".into(),
                    label: "Macro test".into(),
                    kind: ActorKind::Agent,
                };
                let (lease, run_id) = match slot
                    .request_run_start(
                        Uuid::new_v4(),
                        actor.clone(),
                        "Test macro".into(),
                        BTreeMap::new(),
                        60_000,
                    )
                    .await
                    .unwrap()
                {
                    CommandResult::RunStartGranted { lease, run, .. } => (lease, run.id),
                    other => panic!("unexpected {other:?}"),
                };
                Self {
                    slot,
                    master,
                    manager,
                    directory,
                    actor,
                    lease,
                    run_id,
                }
            }
            fn prepare(&self, script: &str) -> PreparedMacro {
                MacroCatalog::new(self.directory.path().join("macros.json"))
                    .prepare(MacroRunSpec {
                        macro_id: None,
                        revision: None,
                        script: Some(script.into()),
                        args: BTreeMap::new(),
                        description: Some("Test macro".into()),
                        timeout_seconds: 3,
                    })
                    .unwrap()
            }
            async fn start(&self, id: Uuid, script: &str) -> Result<CommandResult, SlotError> {
                let snapshot = self.slot.snapshot();
                self.slot
                    .start_macro(
                        self.actor.clone(),
                        self.lease.id,
                        self.lease.fence,
                        snapshot.daemon_epoch,
                        snapshot.generation,
                        id,
                        Some(self.run_id),
                        None,
                        self.prepare(script),
                    )
                    .await
            }
            async fn command(&mut self) -> Vec<u8> {
                tokio::time::timeout(Duration::from_secs(3), async {
                    let mut data = Vec::new();
                    loop {
                        let byte = self.master.read_u8().await.unwrap();
                        data.push(byte);
                        if byte == b'\r' || byte == 4 {
                            return data;
                        }
                    }
                })
                .await
                .expect("macro did not send command")
            }
            async fn terminal(&self, id: Uuid) -> MacroExecutionInfo {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        match self
                            .slot
                            .macro_status(self.actor.clone(), id)
                            .await
                            .unwrap()
                        {
                            CommandResult::MacroStatus { execution }
                                if execution.status.is_terminal() =>
                            {
                                return *execution;
                            }
                            _ => tokio::time::sleep(Duration::from_millis(10)).await,
                        }
                    }
                })
                .await
                .unwrap()
            }
            async fn close(self) {
                self.slot.shutdown().await;
                self.manager.shutdown().await.unwrap();
            }
        }

        #[tokio::test]
        async fn reboot_macro_ignores_old_prompt_matches_fresh_split_rx_and_never_replays() {
            let mut f = Fixture::new().await;
            f.master.write_all(b"U-Boot> ").await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            let script = r#"let boot = watch(prompt("uboot")); cmd("reboot");
                while (!boot.matched) { cmd("slp"); wait(boot, 80); }
                expect(boot, 0);"#;
            let id = Uuid::new_v4();
            f.start(id, script).await.unwrap();
            assert_eq!(f.command().await, b"reboot\r");
            assert_eq!(f.command().await, b"slp\r");
            f.master.write_all(b"slp\r\nU-Bo").await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
            f.master.write_all(b"ot> ").await.unwrap();
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::Succeeded, "{info:?}");
            assert_eq!(info.writes, 2);
            f.start(id, script).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(150), f.master.read_u8())
                    .await
                    .is_err()
            );
            f.close().await;
        }

        #[tokio::test]
        async fn human_ctrl_d_interrupts_macro_preserves_read_gate_and_does_not_enter_history() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"cmd("first"); delay(1000); cmd("forbidden");"#)
                .await
                .unwrap();
            assert_eq!(f.command().await, b"first\r");
            let human = Actor {
                id: "human".into(),
                label: "Human".into(),
                kind: ActorKind::Human,
            };
            f.slot
                .send_human_input(
                    Uuid::new_v4(),
                    human,
                    f.slot.snapshot().generation,
                    vec![4],
                    Some(Uuid::new_v4()),
                    Some("Ctrl-D".into()),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(f.command().await, vec![4]);
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::InterruptedByUser, "{info:?}");
            assert_eq!(info.writes, 1);
            assert!(matches!(
                f.start(Uuid::new_v4(), r#"cmd("forbidden");"#).await,
                Err(SlotError::UserReadRequired { .. })
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), f.master.read_u8())
                    .await
                    .is_err()
            );
            let history = f
                .manager
                .handle()
                .human_command_history(Uuid::new_v4(), Default::default())
                .await
                .unwrap();
            assert!(history.entries.is_empty());
            f.close().await;
        }

        #[tokio::test]
        async fn expect_timeout_prevents_later_write_and_invalid_prompt_sends_nothing() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"let ready = watch("Ready"); cmd("echo Ready"); expect(ready, 120); cmd("forbidden");"#).await.unwrap();
            assert_eq!(f.command().await, b"echo Ready\r");
            f.master.write_all(b"echo Ready\r\n").await.unwrap();
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::Failed, "{info:?}");
            assert_eq!(info.writes, 1);
            let invalid = r#"cmd("forbidden"); let ready = watch(prompt("missing")); "#;
            // Unknown prompt name is a compile-time error, before the first cmd.
            assert!(serial_macro::compile(invalid, &BTreeMap::new(), Default::default()).is_err());
            assert!(
                tokio::time::timeout(Duration::from_millis(100), f.master.read_u8())
                    .await
                    .is_err()
            );
            f.close().await;
        }

        #[tokio::test]
        async fn cancel_stops_future_writes_and_releases_macro_guard() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"cmd("first"); delay(2000); cmd("forbidden");"#)
                .await
                .unwrap();
            assert_eq!(f.command().await, b"first\r");
            f.slot
                .cancel_macro(f.actor.clone(), f.lease.id, f.lease.fence, id)
                .await
                .unwrap();
            assert_eq!(f.terminal(id).await.status, MacroStatus::Cancelled);
            let next = Uuid::new_v4();
            f.start(next, r#"cmd("next");"#).await.unwrap();
            assert_eq!(f.command().await, b"next\r");
            assert_eq!(f.terminal(next).await.status, MacroStatus::Succeeded);
            f.close().await;
        }

        #[tokio::test]
        async fn hidden_osc_started_before_watch_cannot_satisfy_new_completion() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"cmd("first"); delay(80); let w = watch("Ready"); cmd("next"); expect(w, 120); cmd("forbidden");"#).await.unwrap();
            assert_eq!(f.command().await, b"first\r");
            f.master.write_all(b"\x1b]0;title-").await.unwrap();
            assert_eq!(f.command().await, b"next\r");
            f.master.write_all(b"Ready\x07").await.unwrap();
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::Failed, "{info:?}");
            assert_eq!(info.writes, 2);
            f.close().await;
        }

        #[tokio::test]
        async fn command_containing_prompt_is_not_mistaken_for_a_response() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"let boot = watch(prompt("uboot")); cmd("echo U-Boot> "); expect(boot, 120); cmd("forbidden");"#).await.unwrap();
            assert_eq!(f.command().await, b"echo U-Boot> \r");
            f.master.write_all(b"echo U-Boot> \r\n").await.unwrap();
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::Failed, "{info:?}");
            assert_eq!(info.writes, 1);
            f.close().await;
        }

        #[tokio::test]
        async fn persistent_watch_keeps_escape_state_across_commands() {
            let mut f = Fixture::new().await;
            let id = Uuid::new_v4();
            f.start(id, r#"let ready = watch("Ready"); cmd("first"); wait(ready, 80); cmd("second"); expect(ready, 120);"#).await.unwrap();
            assert_eq!(f.command().await, b"first\r");
            f.master.write_all(b"\x1b[3").await.unwrap();
            assert_eq!(f.command().await, b"second\r");
            f.master.write_all(b"1mReady\r\n").await.unwrap();
            let info = f.terminal(id).await;
            assert_eq!(info.status, MacroStatus::Succeeded, "{info:?}");
            assert_eq!(info.writes, 2);
            f.close().await;
        }

        #[tokio::test]
        async fn audit_payload_is_bounded_before_first_write() {
            let mut f = Fixture::new().await;
            let script = format!("//{}\ncmd(\"forbidden\");", "\0".repeat(60_000));
            let error = f.start(Uuid::new_v4(), &script).await.unwrap_err();
            assert!(
                matches!(error, SlotError::MacroInvalid(message) if message.contains("audit record"))
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(100), f.master.read_u8())
                    .await
                    .is_err()
            );
            f.close().await;
        }

        #[tokio::test]
        async fn provisional_terminal_is_not_exposed_until_actor_commits_finish() {
            let f = Fixture::new().await;
            let id = Uuid::new_v4();
            let result = f.start(id, "delay(1000);").await.unwrap();
            let CommandResult::MacroStarted { mut execution } = result else {
                panic!("start result")
            };
            execution.status = MacroStatus::Succeeded;
            execution.completed_at_ns = Some(wall_time_ns());
            let (stop, _receiver) = watch::channel(None);
            let active = ActiveMacro {
                info: Arc::new(StdMutex::new(*execution)),
                control_id: f.lease.id,
                fence: f.lease.fence,
                deadline: Instant::now() + Duration::from_secs(1),
                stop,
            };
            assert_eq!(active.public_snapshot().status, MacroStatus::Running);
            assert!(active.public_snapshot().completed_at_ns.is_none());
            active.stop.send_replace(Some(Stop {
                status: MacroStatus::InterruptedByUser,
                message: "Human".into(),
            }));
            assert_eq!(active.public_snapshot().status, MacroStatus::Stopping);
            f.slot
                .cancel_macro(f.actor.clone(), f.lease.id, f.lease.fence, id)
                .await
                .unwrap();
            f.terminal(id).await;
            f.close().await;
        }
    }
}
