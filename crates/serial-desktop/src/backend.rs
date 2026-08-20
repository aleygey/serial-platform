use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt as _, StreamExt as _};
use serial_protocol::{
    Actor, ActorKind, ClientMessage, CommandResult, ConfigureDeviceProfilesRequest,
    ConfigureSlotsRequest, ConfigureTransportProfilesRequest, ControlLease, ControlMode, Cursor,
    DeviceModel, DeviceModelListResponse, DeviceProfile, EventKind, EventQuery, HealthResponse,
    ModelConfirmationMethod, PROTOCOL_VERSION, PortDescriptor, SerialSettings, ServerMessage,
    SetSlotDeviceModelRequest, SlotConfig, SlotModelBinding, SlotSnapshot, StatusResponse,
    Subscription, TimelineEvent, TransportProfile, WireFrame, decode_wire_frame,
    encode_client_control,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest as _,
        http::{HeaderValue, header::AUTHORIZATION},
    },
};
use uuid::Uuid;

use crate::{
    client::{ApiClient, websocket_url},
    process::{LocalService, LocalServiceState, endpoint_bind},
};

const CONTROL_TTL_MS: u64 = 30_000;
const HISTORY_EVENTS: u64 = 2_000;
const HISTORY_BYTES: usize = 2 * 1024 * 1024;

/// Complete, resolved settings edited by the desktop configuration page.
///
/// The backend never writes a bound or shared Profile in place. It stages
/// content-addressed, currently unbound Slot-exclusive entries, then switches
/// both bindings in the final revision-guarded Slots transaction. A GUI edit
/// therefore cannot partially reconfigure the live Slot or another Slot that
/// happened to share its old Profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSettingsDraft {
    pub slot_id: String,
    pub expected_revision: u64,
    pub port: String,
    pub transport: TransportProfile,
    pub device: DeviceProfile,
}

#[derive(Debug)]
pub enum BackendCommand {
    Connect {
        endpoint: String,
        token: Option<String>,
    },
    StartLocal {
        endpoint: String,
        token: Option<String>,
        program: Option<PathBuf>,
    },
    StopLocal,
    Refresh,
    SetSlotEnabled {
        slot_id: String,
        enabled: bool,
    },
    CreateSlot {
        slot_id: String,
        display_name: String,
        port: String,
        profile: String,
    },
    SetSlotProfiles {
        slot_id: String,
        transport_profile: String,
        device_profile: Option<String>,
    },
    ApplySlotSettings(SlotSettingsDraft),
    SetSlotModel {
        slot_id: String,
        model_id: Option<String>,
    },
    SendLine {
        slot_id: String,
        text: String,
    },
    Shutdown,
}

#[derive(Debug)]
pub enum BackendEvent {
    LocalService(LocalServiceState),
    Connection {
        connected: bool,
        message: String,
    },
    Health(HealthResponse),
    Status(Box<StatusResponse>),
    Ports(Vec<PortDescriptor>),
    TransportProfiles(Vec<TransportProfile>),
    DeviceProfiles(Vec<DeviceProfile>),
    DeviceModels {
        models: Vec<DeviceModel>,
        bindings: Vec<SlotModelBinding>,
    },
    SlotSettingsApplied {
        slot_id: String,
        transport_profile: String,
        device_profile: String,
        cleanup_warning: Option<String>,
    },
    Snapshot(Box<SlotSnapshot>),
    Timeline {
        event: Box<TimelineEvent>,
        replay: bool,
    },
    Gap {
        slot_id: String,
        message: String,
    },
    Notice(String),
    Error(String),
}

#[derive(Debug)]
enum WsCommand {
    SendLine { slot_id: String, text: String },
    Shutdown,
}

pub struct BackendHandle {
    pub commands: mpsc::Sender<BackendCommand>,
    pub events: mpsc::Receiver<BackendEvent>,
}

impl BackendHandle {
    pub fn spawn() -> Self {
        let (command_tx, command_rx) = mpsc::channel(128);
        let (event_tx, event_rx) = mpsc::channel(4_096);
        thread::Builder::new()
            .name("serial-desktop-backend".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(run_backend(command_rx, event_tx)),
                    Err(error) => tracing::error!(%error, "cannot create desktop Tokio runtime"),
                }
            })
            .expect("spawn serial desktop backend thread");
        Self {
            commands: command_tx,
            events: event_rx,
        }
    }
}

async fn run_backend(
    mut commands: mpsc::Receiver<BackendCommand>,
    events: mpsc::Sender<BackendEvent>,
) {
    let mut api: Option<ApiClient> = None;
    let mut local = LocalService::default();
    let mut last_local_state = local.state().clone();
    let mut ws: Option<(mpsc::Sender<WsCommand>, tokio::task::JoinHandle<()>)> = None;
    let mut poll = tokio::time::interval(Duration::from_secs(2));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                match command {
                    BackendCommand::Connect { endpoint, token } => {
                        stop_ws(&mut ws).await;
                        match ApiClient::new(&endpoint, token) {
                            Ok(client) => {
                                api = Some(client.clone());
                                let _ = events.send(BackendEvent::Connection {
                                    connected: false,
                                    message: "正在连接 seriald…".into(),
                                }).await;
                                match start_session(&client, &events).await {
                                    Ok(session) => ws = Some(session),
                                    Err(error) => {
                                        let _ = events.send(BackendEvent::Error(format!("连接 seriald 失败：{error:#}"))).await;
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("无效服务地址：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::StartLocal { endpoint, token, program } => {
                        local.refresh();
                        if matches!(
                            local.state(),
                            LocalServiceState::Starting { .. } | LocalServiceState::Running { .. }
                        ) {
                            let _ = events.send(BackendEvent::LocalService(local.state().clone())).await;
                            continue;
                        }
                        let already_running = match ApiClient::new(&endpoint, token) {
                            Ok(probe) => probe.health_reachable().await,
                            Err(_) => false,
                        };
                        if already_running {
                            let _ = events.send(BackendEvent::Notice(
                                "检测到健康端点已响应；仅连接现有服务，不纳入 App 进程管理".into()
                            )).await;
                            let _ = events.send(BackendEvent::LocalService(LocalServiceState::Stopped)).await;
                            continue;
                        }
                        let result = endpoint_bind(&endpoint)
                            .and_then(|bind| local.start(program.as_deref(), &bind));
                        match result {
                            Ok(()) => {
                                last_local_state = local.state().clone();
                                let _ = events.send(BackendEvent::LocalService(last_local_state.clone())).await;
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("启动本地服务失败：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::StopLocal => {
                        if let Err(error) = local.stop() {
                            let _ = events.send(BackendEvent::Error(format!("停止本地服务失败：{error:#}"))).await;
                        }
                        last_local_state = local.state().clone();
                        let _ = events.send(BackendEvent::LocalService(last_local_state.clone())).await;
                    }
                    BackendCommand::Refresh => {
                        if let Some(client) = api.as_ref()
                            && let Err(error) = refresh(client, &events).await
                        {
                            let _ = events.send(BackendEvent::Error(format!("刷新失败：{error:#}"))).await;
                        }
                    }
                    BackendCommand::SetSlotEnabled { slot_id, enabled } => {
                        let result = match api.as_ref() {
                            Some(client) => set_slot_enabled(client, &slot_id, enabled).await,
                            None => Err(anyhow::anyhow!("尚未连接 seriald")),
                        };
                        match result {
                            Ok(status) => {
                                let _ = events.send(BackendEvent::Status(Box::new(status))).await;
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("更新串口状态失败：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::CreateSlot { slot_id, display_name, port, profile } => {
                        let result = match api.as_ref() {
                            Some(client) => create_slot(client, slot_id, display_name, port, profile).await,
                            None => Err(anyhow::anyhow!("尚未连接 seriald")),
                        };
                        match result {
                            Ok(status) => {
                                let _ = events.send(BackendEvent::Status(Box::new(status))).await;
                                // A newly created Slot was not part of the old
                                // Attach subscription set. Reconnect from the
                                // durable cursor so it becomes live immediately.
                                stop_ws(&mut ws).await;
                                if let Some(client) = api.as_ref()
                                    && let Ok(session) = start_session(client, &events).await
                                {
                                    ws = Some(session);
                                }
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("创建串口配置失败：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::SetSlotProfiles {
                        slot_id,
                        transport_profile,
                        device_profile,
                    } => {
                        let result = match api.as_ref() {
                            Some(client) => apply_slot_profiles(
                                client,
                                &slot_id,
                                &transport_profile,
                                device_profile.as_deref(),
                            ).await,
                            None => Err(anyhow::anyhow!("尚未连接 seriald")),
                        };
                        match result {
                            Ok(status) => {
                                let _ = events.send(BackendEvent::Status(Box::new(status))).await;
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("应用 Slot Profile 失败：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::ApplySlotSettings(draft) => {
                        let slot_id = draft.slot_id.clone();
                        let result = match api.as_ref() {
                            Some(client) => apply_slot_settings(client, draft).await,
                            None => Err(anyhow::anyhow!("尚未连接 seriald")),
                        };
                        match result {
                            Ok(applied) => {
                                let _ = events.send(BackendEvent::Status(Box::new(applied.status))).await;
                                let _ = events.send(BackendEvent::TransportProfiles(applied.transport_profiles)).await;
                                let _ = events.send(BackendEvent::DeviceProfiles(applied.device_profiles)).await;
                                let _ = events.send(BackendEvent::SlotSettingsApplied {
                                    slot_id,
                                    transport_profile: applied.transport_profile,
                                    device_profile: applied.device_profile,
                                    cleanup_warning: applied.cleanup_warning,
                                }).await;
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!(
                                    "保存当前 Slot 配置失败：{error:#}"
                                ))).await;
                            }
                        }
                    }
                    BackendCommand::SetSlotModel { slot_id, model_id } => {
                        let result = match api.as_ref() {
                            Some(client) => apply_slot_model(client, &slot_id, model_id.as_deref()).await,
                            None => Err(anyhow::anyhow!("尚未连接 seriald")),
                        };
                        match result {
                            Ok(catalog) => {
                                let _ = events.send(BackendEvent::DeviceModels {
                                    models: catalog.models,
                                    bindings: catalog.bindings,
                                }).await;
                            }
                            Err(error) => {
                                let _ = events.send(BackendEvent::Error(format!("应用样机型号失败：{error:#}"))).await;
                            }
                        }
                    }
                    BackendCommand::SendLine { slot_id, text } => {
                        match ws.as_ref() {
                            Some((sender, _)) => {
                                if sender.try_send(WsCommand::SendLine { slot_id, text }).is_err() {
                                    let _ = events.send(BackendEvent::Error("发送队列繁忙，命令未排队".into())).await;
                                }
                            }
                            None => {
                                let _ = events.send(BackendEvent::Error("实时连接尚未建立，命令未发送".into())).await;
                            }
                        }
                    }
                    BackendCommand::Shutdown => break,
                }
            }
            _ = poll.tick() => {
                local.refresh();
                if local.state() != &last_local_state {
                    last_local_state = local.state().clone();
                    let _ = events.send(BackendEvent::LocalService(last_local_state.clone())).await;
                }
                if let Some(client) = api.as_ref() {
                    match client.status().await {
                        Ok(status) => {
                            let _ = events.send(BackendEvent::Status(Box::new(status))).await;
                        }
                        Err(error) => {
                            let _ = events.send(BackendEvent::Connection {
                                connected: false,
                                message: format!("seriald 不可用：{error}"),
                            }).await;
                        }
                    }
                }
                if ws.as_ref().is_some_and(|(_, task)| task.is_finished()) {
                    stop_ws(&mut ws).await;
                }
                // `serial serve` can need a moment to bind after the GUI starts
                // it. A failed first Connect therefore remains retryable instead
                // of leaving the desktop permanently offline.
                if ws.is_none()
                    && let Some(client) = api.as_ref()
                    && let Ok(session) = start_session(client, &events).await
                {
                    ws = Some(session);
                }
            }
        }
    }

    stop_ws(&mut ws).await;
    // Dropping the desktop leaves a locally managed daemon running so journal
    // shutdown is never replaced by an implicit force-kill. StopLocal is the
    // only explicit, user-confirmed process shutdown path.
}

async fn stop_ws(ws: &mut Option<(mpsc::Sender<WsCommand>, tokio::task::JoinHandle<()>)>) {
    if let Some((sender, mut task)) = ws.take() {
        let _ = sender.try_send(WsCommand::Shutdown);
        if tokio::time::timeout(Duration::from_secs(1), &mut task)
            .await
            .is_err()
        {
            task.abort();
            let _ = task.await;
        }
    }
}

async fn refresh(api: &ApiClient, events: &mpsc::Sender<BackendEvent>) -> Result<()> {
    let (health, status, ports, profiles, device_profiles, device_models) = tokio::try_join!(
        api.health(),
        api.status(),
        api.ports(),
        api.transport_profiles(),
        api.device_profiles(),
        api.device_models(),
    )?;
    events.send(BackendEvent::Health(health)).await.ok();
    events
        .send(BackendEvent::Status(Box::new(status)))
        .await
        .ok();
    events.send(BackendEvent::Ports(ports)).await.ok();
    events
        .send(BackendEvent::TransportProfiles(profiles.profiles))
        .await
        .ok();
    events
        .send(BackendEvent::DeviceProfiles(device_profiles.profiles))
        .await
        .ok();
    events
        .send(BackendEvent::DeviceModels {
            models: device_models.models,
            bindings: device_models.bindings,
        })
        .await
        .ok();
    Ok(())
}

async fn start_session(
    api: &ApiClient,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<(mpsc::Sender<WsCommand>, tokio::task::JoinHandle<()>)> {
    let status = api.status().await?;
    let (ports, profiles, device_profiles, device_models) = tokio::join!(
        api.ports(),
        api.transport_profiles(),
        api.device_profiles(),
        api.device_models(),
    );
    let ports = ports.unwrap_or_default();
    let profiles = profiles
        .map(|response| response.profiles)
        .unwrap_or_default();
    let device_profiles = device_profiles
        .map(|response| response.profiles)
        .unwrap_or_default();
    let device_models = device_models.ok();
    events
        .send(BackendEvent::Status(Box::new(status.clone())))
        .await
        .ok();
    events.send(BackendEvent::Ports(ports)).await.ok();
    events
        .send(BackendEvent::TransportProfiles(profiles))
        .await
        .ok();
    events
        .send(BackendEvent::DeviceProfiles(device_profiles))
        .await
        .ok();
    if let Some(device_models) = device_models {
        events
            .send(BackendEvent::DeviceModels {
                models: device_models.models,
                bindings: device_models.bindings,
            })
            .await
            .ok();
    }

    let mut cursors = HashMap::new();
    let history_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    for slot in &status.slots {
        let query = EventQuery {
            epoch: Some(slot.daemon_epoch),
            after_seq: Some(slot.head_seq.saturating_sub(HISTORY_EVENTS)),
            through_seq: Some(slot.head_seq),
            before_wall_time_ns: None,
            after_wall_time_ns: None,
            direction: None,
            kind: None,
            actor_id: None,
            run_id: None,
            operation_id: None,
            contains: None,
            regex: None,
            limit_events: Some(HISTORY_EVENTS as usize),
            limit_bytes: Some(HISTORY_BYTES),
        };
        match tokio::time::timeout_at(history_deadline, api.events(&slot.config.id, &query)).await {
            Ok(Ok(response)) => {
                if let Some(cursor) = response
                    .next_cursor
                    .filter(|cursor| cursor.epoch == slot.daemon_epoch)
                {
                    cursors.insert(slot.config.id.clone(), cursor);
                }
                for gap in response.gaps {
                    events
                        .send(BackendEvent::Gap {
                            slot_id: slot.config.id.clone(),
                            message: format!(
                                "历史缺口 #{}..=#{} ({:?})",
                                gap.first_seq, gap.last_seq, gap.reason
                            ),
                        })
                        .await
                        .ok();
                }
                for event in response.events {
                    events
                        .send(BackendEvent::Timeline {
                            event: Box::new(event),
                            replay: true,
                        })
                        .await
                        .ok();
                }
            }
            Ok(Err(error)) => {
                events
                    .send(BackendEvent::Error(format!(
                        "{} 的历史回填失败：{error:#}",
                        slot.config.display_name
                    )))
                    .await
                    .ok();
            }
            Err(_) => {
                events
                    .send(BackendEvent::Gap {
                        slot_id: slot.config.id.clone(),
                        message: "启动历史回填已达到 10 秒总上限；已切换到有界实时尾部".into(),
                    })
                    .await
                    .ok();
            }
        }
    }

    let (command_tx, command_rx) = mpsc::channel(128);
    let endpoint = api.endpoint().to_string();
    let token = api.token().map(ToOwned::to_owned);
    let slots = status
        .slots
        .iter()
        .map(|slot| slot.config.id.clone())
        .collect();
    let event_tx = events.clone();
    let task = tokio::spawn(async move {
        if let Err(error) = run_ws(endpoint, token, slots, cursors, command_rx, &event_tx).await {
            let _ = event_tx
                .send(BackendEvent::Connection {
                    connected: false,
                    message: format!("实时连接断开：{error:#}"),
                })
                .await;
        }
    });
    Ok((command_tx, task))
}

async fn set_slot_enabled(api: &ApiClient, slot_id: &str, enabled: bool) -> Result<StatusResponse> {
    let status = api.status().await?;
    let target = status
        .slots
        .iter()
        .find(|slot| slot.config.id == slot_id)
        .with_context(|| format!("未知串口配置 {slot_id}"))?;
    if enabled
        && target
            .effective_transport
            .is_some_and(|transport| !transport.auto_open)
    {
        bail!("当前传输配置已关闭 auto_open；请先在配置页选择允许自动打开的传输配置");
    }
    let mut slots = status
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let target = slots
        .iter_mut()
        .find(|slot| slot.id == slot_id)
        .expect("target was found in the same status response");
    target.enabled = enabled;
    if enabled {
        target.settings.auto_open = true;
    }
    api.configure_slots(ConfigureSlotsRequest {
        slots,
        expected_revision: Some(status.config_revision),
    })
    .await?;
    api.status().await
}

async fn create_slot(
    api: &ApiClient,
    slot_id: String,
    display_name: String,
    port: String,
    profile: String,
) -> Result<StatusResponse> {
    let slot_id = slot_id.trim();
    let display_name = display_name.trim();
    let port = port.trim();
    let profile = profile.trim();
    if slot_id.is_empty() || display_name.is_empty() || port.is_empty() || profile.is_empty() {
        bail!("Slot ID、显示名称、串口和传输配置均不能为空");
    }
    let status = api.status().await?;
    if status.slots.iter().any(|slot| slot.config.id == slot_id) {
        bail!("Slot ID 已存在：{slot_id}");
    }
    if status.slots.iter().any(|slot| slot.config.port == port) {
        bail!("串口已被其他 Slot 使用：{port}");
    }
    let profiles = api.transport_profiles().await?;
    let chosen_profile = profiles
        .profiles
        .iter()
        .find(|candidate| candidate.name == profile)
        .with_context(|| format!("未知传输配置 {profile}"))?;
    if !chosen_profile.auto_open {
        bail!("传输配置 {profile} 已关闭 auto_open，不能在创建时自动打开串口");
    }
    let mut slots = status
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    slots.push(SlotConfig {
        id: slot_id.to_string(),
        display_name: display_name.to_string(),
        port: port.to_string(),
        profile: profile.to_string(),
        device_profile: None,
        enabled: true,
        settings: SerialSettings::default(),
    });
    api.configure_slots(ConfigureSlotsRequest {
        slots,
        expected_revision: Some(status.config_revision),
    })
    .await?;
    api.status().await
}

async fn apply_slot_profiles(
    api: &ApiClient,
    slot_id: &str,
    transport_profile: &str,
    device_profile: Option<&str>,
) -> Result<StatusResponse> {
    let transport_profile = transport_profile.trim();
    let device_profile = device_profile
        .map(str::trim)
        .filter(|name| !name.is_empty());
    let (transports, devices) = tokio::try_join!(api.transport_profiles(), api.device_profiles(),)?;
    let transport = transports
        .profiles
        .iter()
        .find(|candidate| candidate.name == transport_profile)
        .with_context(|| format!("未知传输配置 {transport_profile}"))?;
    if let Some(device_profile) = device_profile
        && !devices
            .profiles
            .iter()
            .any(|candidate| candidate.name == device_profile)
    {
        bail!("未知设备配置 {device_profile}");
    }

    let status = api.status().await?;
    let mut slots = status
        .slots
        .into_iter()
        .map(|slot| slot.config)
        .collect::<Vec<_>>();
    let target = slots
        .iter_mut()
        .find(|slot| slot.id == slot_id)
        .with_context(|| format!("未知串口配置 {slot_id}"))?;
    if target.enabled && !transport.auto_open {
        bail!("传输配置 {transport_profile} 已关闭 auto_open；请先关闭 Slot 再应用");
    }
    target.profile = transport_profile.to_string();
    target.device_profile = device_profile.map(ToOwned::to_owned);
    api.configure_slots(ConfigureSlotsRequest {
        slots,
        expected_revision: Some(status.config_revision),
    })
    .await?;
    api.status().await
}

#[derive(Debug)]
struct AppliedSlotSettings {
    status: StatusResponse,
    transport_profiles: Vec<TransportProfile>,
    device_profiles: Vec<DeviceProfile>,
    transport_profile: String,
    device_profile: String,
    cleanup_warning: Option<String>,
}

async fn apply_slot_settings(
    api: &ApiClient,
    mut draft: SlotSettingsDraft,
) -> Result<AppliedSlotSettings> {
    draft.slot_id = draft.slot_id.trim().to_string();
    draft.port = draft.port.trim().to_string();
    if draft.slot_id.is_empty() || draft.port.is_empty() {
        bail!("Slot ID 和串口不能为空");
    }
    if draft.transport.baud_rate == 0 {
        bail!("波特率必须大于 0");
    }
    if draft.device.write_chunk_size == Some(0) {
        bail!("写入分块必须大于 0 bytes");
    }
    draft.device.shell_prompt = draft
        .device
        .shell_prompt
        .take()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    draft.device.uboot_prompt = draft
        .device
        .uboot_prompt
        .take()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    // Do not silently rebase a form that was opened before somebody else
    // changed the station configuration. Every following mutation also uses
    // the revision returned by the immediately preceding transaction.
    let status = api.status().await?;
    if status.config_revision != draft.expected_revision {
        bail!(
            "配置已从修订号 {} 更新为 {}；请重新加载后再保存",
            draft.expected_revision,
            status.config_revision
        );
    }
    let _target = status
        .slots
        .iter()
        .find(|slot| slot.config.id == draft.slot_id)
        .with_context(|| format!("未知串口配置 {}", draft.slot_id))?;
    if status
        .slots
        .iter()
        .any(|slot| slot.config.id != draft.slot_id && slot.config.port.trim() == draft.port)
    {
        bail!("串口 {} 已被其他 Slot 使用", draft.port);
    }

    let transports = api.transport_profiles().await?;
    let devices = api.device_profiles().await?;
    if transports.config_revision != status.config_revision
        || devices.config_revision != status.config_revision
    {
        bail!("读取 Profile 时配置发生变化；请重新加载后再保存");
    }

    // Profile identity is content-addressed. A changed form therefore gets a
    // new, currently unbound Profile instead of mutating the Profile that the
    // live Slot is using. Equal content reuses the same name and catalog row.
    let transport_name = content_addressed_transport_name(&draft.slot_id, &draft.transport)?;
    let device_name = content_addressed_device_name(&draft.slot_id, &draft.device)?;
    draft.transport.name = transport_name.clone();
    draft.device.name = device_name.clone();

    // Prune only old *unbound* Profiles from this Slot's reserved namespace
    // while staging the new entry. Thus a failed final switch leaves the live
    // Slot untouched and at most one prepared candidate beside its bound
    // Profile; repeated failures cannot grow the catalog without bound.
    let (prepared_transports, transport_catalog_changed) = prepare_transport_catalog(
        transports.profiles,
        &status.slots,
        &draft.slot_id,
        draft.transport.clone(),
    )?;
    let (prepared_devices, device_catalog_changed) = prepare_device_catalog(
        devices.profiles,
        &status.slots,
        &draft.slot_id,
        draft.device.clone(),
    )?;

    let mut revision = status.config_revision;
    let mut transport_profiles = prepared_transports;
    if transport_catalog_changed {
        let response = api
            .configure_transport_profiles(ConfigureTransportProfilesRequest {
                profiles: transport_profiles,
                expected_revision: Some(revision),
            })
            .await
            .context("准备未绑定的 Slot 专属 Transport Profile 失败；当前 Slot 未改变")?;
        revision = response.config_revision;
        transport_profiles = response.profiles;
    }

    let mut device_profiles = prepared_devices;
    if device_catalog_changed {
        let response = api
            .configure_device_profiles(ConfigureDeviceProfilesRequest {
                profiles: device_profiles,
                expected_revision: Some(revision),
            })
            .await
            .context(
                "准备未绑定的 Slot 专属 Device Profile 失败；当前 Slot 未改变，已准备的 Transport Profile 将在下次保存时复用",
            )?;
        revision = response.config_revision;
        device_profiles = response.profiles;
    }

    let slots = slot_replacement(&status, &draft, &transport_name, &device_name)?;
    let current_slots = status
        .slots
        .iter()
        .map(|slot| slot.config.clone())
        .collect::<Vec<_>>();
    let mut applied_status = status.clone();
    applied_status.config_revision = revision;
    if slots != current_slots {
        let response = api
            .configure_slots(ConfigureSlotsRequest {
                slots,
                expected_revision: Some(revision),
            })
            .await
            .context(
                "最终 Slot 切换失败或未确认；seriald 拒绝事务时当前 Slot 不变，网络中断时请刷新确认。已准备的未绑定 Profiles 将在下次保存时复用",
            )?;
        revision = response.config_revision;
        applied_status.config_revision = revision;
        applied_status.slots = response.slots;
    }

    // The switch is complete. Removing the old now-unbound exclusive entries
    // is optional cleanup: failure never rolls back or obscures the successful
    // Slot update. The next preparation pass also prunes them, preserving the
    // same bound + one candidate upper bound.
    let mut cleanup_warnings = Vec::new();
    let (clean_transports, transport_cleanup_needed) = prune_transport_catalog(
        transport_profiles.clone(),
        &applied_status.slots,
        &draft.slot_id,
        &transport_name,
    );
    if transport_cleanup_needed {
        match api
            .configure_transport_profiles(ConfigureTransportProfilesRequest {
                profiles: clean_transports,
                expected_revision: Some(revision),
            })
            .await
        {
            Ok(response) => {
                revision = response.config_revision;
                transport_profiles = response.profiles;
            }
            Err(error) => cleanup_warnings.push(format!(
                "旧 Transport Profile 暂未回收（{error}），下次保存会重试"
            )),
        }
    }

    // If transport cleanup conflicted, its revision is uncertain from the
    // client's point of view, so skip the second cleanup rather than guessing.
    if cleanup_warnings.is_empty() {
        let (clean_devices, device_cleanup_needed) = prune_device_catalog(
            device_profiles.clone(),
            &applied_status.slots,
            &draft.slot_id,
            &device_name,
        );
        if device_cleanup_needed {
            match api
                .configure_device_profiles(ConfigureDeviceProfilesRequest {
                    profiles: clean_devices,
                    expected_revision: Some(revision),
                })
                .await
            {
                Ok(response) => {
                    revision = response.config_revision;
                    device_profiles = response.profiles;
                }
                Err(error) => cleanup_warnings.push(format!(
                    "旧 Device Profile 暂未回收（{error}），下次保存会重试"
                )),
            }
        }
    }
    applied_status.config_revision = revision;

    Ok(AppliedSlotSettings {
        status: applied_status,
        transport_profiles,
        device_profiles,
        transport_profile: transport_name,
        device_profile: device_name,
        cleanup_warning: (!cleanup_warnings.is_empty()).then(|| cleanup_warnings.join("；")),
    })
}

fn content_addressed_transport_name(slot_id: &str, profile: &TransportProfile) -> Result<String> {
    let mut material = profile.clone();
    material.name.clear();
    content_addressed_profile_name(slot_id, "transport", &serde_json::to_vec(&material)?)
}

fn content_addressed_device_name(slot_id: &str, profile: &DeviceProfile) -> Result<String> {
    let mut material = profile.clone();
    material.name.clear();
    content_addressed_profile_name(slot_id, "device", &serde_json::to_vec(&material)?)
}

fn content_addressed_profile_name(slot_id: &str, kind: &str, material: &[u8]) -> Result<String> {
    if !matches!(kind, "transport" | "device") {
        bail!("unsupported desktop Profile kind {kind}");
    }
    let slot_hash = Sha256::digest(slot_id.as_bytes());
    let content_hash = Sha256::digest(material);
    Ok(format!(
        "desktop-slot-{}-{}-{kind}",
        hex_prefix(&slot_hash, 8),
        hex_prefix(&content_hash, 10),
    ))
}

fn hex_prefix(bytes: &[u8], count: usize) -> String {
    bytes
        .iter()
        .take(count)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn desktop_profile_prefix(slot_id: &str) -> String {
    let slot_hash = Sha256::digest(slot_id.as_bytes());
    format!("desktop-slot-{}-", hex_prefix(&slot_hash, 8))
}

fn legacy_dedicated_profile_name(kind: &str, slot_id: &str) -> String {
    let slot = slot_id
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .take(20)
        .collect::<String>();
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in slot_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("desktop-slot-{slot}-{hash:08x}-{kind}", hash = hash as u32)
}

fn is_owned_desktop_profile(name: &str, slot_id: &str, kind: &str) -> bool {
    (name.starts_with(&desktop_profile_prefix(slot_id)) && name.ends_with(&format!("-{kind}")))
        || name == legacy_dedicated_profile_name(kind, slot_id)
}

fn prepare_transport_catalog(
    profiles: Vec<TransportProfile>,
    slots: &[SlotSnapshot],
    slot_id: &str,
    desired: TransportProfile,
) -> Result<(Vec<TransportProfile>, bool)> {
    if let Some(existing) = profiles.iter().find(|profile| profile.name == desired.name)
        && existing != &desired
    {
        bail!(
            "内容寻址 Transport Profile {} 的内容不一致；拒绝覆盖保留名称",
            desired.name
        );
    }
    if slots.iter().any(|slot| {
        slot.config.id != slot_id && slot.config.profile.as_str() == desired.name.as_str()
    }) {
        bail!("Slot 专属 Transport Profile 被其他 Slot 使用；拒绝覆盖");
    }
    let used = slots
        .iter()
        .map(|slot| slot.config.profile.as_str())
        .collect::<HashSet<_>>();
    let original = profiles.clone();
    let mut prepared = profiles
        .into_iter()
        .filter(|profile| {
            profile.name == desired.name
                || !is_owned_desktop_profile(&profile.name, slot_id, "transport")
                || used.contains(profile.name.as_str())
        })
        .collect::<Vec<_>>();
    if !prepared.iter().any(|profile| profile.name == desired.name) {
        prepared.push(desired);
    }
    let changed = prepared != original;
    Ok((prepared, changed))
}

fn prepare_device_catalog(
    profiles: Vec<DeviceProfile>,
    slots: &[SlotSnapshot],
    slot_id: &str,
    desired: DeviceProfile,
) -> Result<(Vec<DeviceProfile>, bool)> {
    if let Some(existing) = profiles.iter().find(|profile| profile.name == desired.name)
        && existing != &desired
    {
        bail!(
            "内容寻址 Device Profile {} 的内容不一致；拒绝覆盖保留名称",
            desired.name
        );
    }
    if slots.iter().any(|slot| {
        slot.config.id != slot_id
            && slot.config.device_profile.as_deref() == Some(desired.name.as_str())
    }) {
        bail!("Slot 专属 Device Profile 被其他 Slot 使用；拒绝覆盖");
    }
    let used = slots
        .iter()
        .filter_map(|slot| slot.config.device_profile.as_deref())
        .collect::<HashSet<_>>();
    let original = profiles.clone();
    let mut prepared = profiles
        .into_iter()
        .filter(|profile| {
            profile.name == desired.name
                || !is_owned_desktop_profile(&profile.name, slot_id, "device")
                || used.contains(profile.name.as_str())
        })
        .collect::<Vec<_>>();
    if !prepared.iter().any(|profile| profile.name == desired.name) {
        prepared.push(desired);
    }
    let changed = prepared != original;
    Ok((prepared, changed))
}

fn prune_transport_catalog(
    profiles: Vec<TransportProfile>,
    slots: &[SlotSnapshot],
    slot_id: &str,
    desired_name: &str,
) -> (Vec<TransportProfile>, bool) {
    let used = slots
        .iter()
        .map(|slot| slot.config.profile.as_str())
        .collect::<HashSet<_>>();
    let original_len = profiles.len();
    let profiles = profiles
        .into_iter()
        .filter(|profile| {
            profile.name == desired_name
                || !is_owned_desktop_profile(&profile.name, slot_id, "transport")
                || used.contains(profile.name.as_str())
        })
        .collect::<Vec<_>>();
    let changed = profiles.len() != original_len;
    (profiles, changed)
}

fn prune_device_catalog(
    profiles: Vec<DeviceProfile>,
    slots: &[SlotSnapshot],
    slot_id: &str,
    desired_name: &str,
) -> (Vec<DeviceProfile>, bool) {
    let used = slots
        .iter()
        .filter_map(|slot| slot.config.device_profile.as_deref())
        .collect::<HashSet<_>>();
    let original_len = profiles.len();
    let profiles = profiles
        .into_iter()
        .filter(|profile| {
            profile.name == desired_name
                || !is_owned_desktop_profile(&profile.name, slot_id, "device")
                || used.contains(profile.name.as_str())
        })
        .collect::<Vec<_>>();
    let changed = profiles.len() != original_len;
    (profiles, changed)
}

fn slot_replacement(
    status: &StatusResponse,
    draft: &SlotSettingsDraft,
    transport_name: &str,
    device_name: &str,
) -> Result<Vec<SlotConfig>> {
    let mut slots = status
        .slots
        .iter()
        .map(|slot| slot.config.clone())
        .collect::<Vec<_>>();
    let slot = slots
        .iter_mut()
        .find(|slot| slot.id == draft.slot_id)
        .with_context(|| format!("未知串口配置 {}", draft.slot_id))?;
    slot.port.clone_from(&draft.port);
    slot.profile = transport_name.to_string();
    slot.device_profile = Some(device_name.to_string());
    slot.settings.baud_rate = draft.transport.baud_rate;
    slot.settings.data_bits = draft.transport.data_bits;
    slot.settings.parity = draft.transport.parity;
    slot.settings.stop_bits = draft.transport.stop_bits;
    slot.settings.flow_control = draft.transport.flow_control;
    slot.settings.dtr = draft.transport.dtr;
    slot.settings.rts = draft.transport.rts;
    slot.settings.auto_open = draft.transport.auto_open;
    slot.settings.shell_prompt = draft.device.shell_prompt.clone();
    slot.settings.uboot_prompt = draft.device.uboot_prompt.clone();
    if let Some(write_eol) = draft.device.write_eol.as_ref() {
        slot.settings.write_eol.clone_from(write_eol);
    }
    if let Some(echo) = draft.device.echo {
        slot.settings.echo = echo;
    }
    if let Some(chunk_size) = draft.device.write_chunk_size {
        slot.settings.write_chunk_size = chunk_size;
    }
    if let Some(chunk_delay_ms) = draft.device.write_chunk_delay_ms {
        slot.settings.write_chunk_delay_ms = chunk_delay_ms;
    }
    if !draft.transport.auto_open {
        slot.enabled = false;
    }
    Ok(slots)
}

async fn apply_slot_model(
    api: &ApiClient,
    slot_id: &str,
    model_id: Option<&str>,
) -> Result<DeviceModelListResponse> {
    let model_id = model_id.map(str::trim).filter(|id| !id.is_empty());
    let catalog = api.device_models().await?;
    if let Some(model_id) = model_id
        && !catalog.models.iter().any(|model| model.id == model_id)
    {
        bail!("未知样机型号 {model_id}");
    }
    let current = catalog
        .bindings
        .iter()
        .find(|binding| binding.slot_id == slot_id)
        .map(|binding| binding.model_id.clone());
    let request = model_binding_request(model_id, current, catalog.config_revision);
    api.set_slot_device_model(slot_id, &request).await?;
    api.device_models().await
}

fn model_binding_request(
    model_id: Option<&str>,
    current: Option<String>,
    config_revision: u64,
) -> SetSlotDeviceModelRequest {
    SetSlotDeviceModelRequest {
        model_id: model_id.map(ToOwned::to_owned),
        create_if_missing: false,
        update_existing: false,
        name: None,
        parent_id: None,
        clear_parent: false,
        aliases: Vec::new(),
        clear_aliases: false,
        confirmation_method: model_id.map(|_| ModelConfirmationMethod::Human),
        note: None,
        source: "human:serial-desktop".into(),
        expected_revision: Some(config_revision),
        expected_current: Some(current),
    }
}

async fn run_ws(
    endpoint: String,
    token: Option<String>,
    slots: Vec<String>,
    cursors: HashMap<String, Cursor>,
    mut commands: mpsc::Receiver<WsCommand>,
    events: &mpsc::Sender<BackendEvent>,
) -> Result<()> {
    let mut request = websocket_url(&endpoint)?
        .into_client_request()
        .context("build desktop WebSocket request")?;
    if let Some(token) = token.as_deref() {
        request.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .context("token contains invalid header characters")?,
        );
    }
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
        .await
        .context("WebSocket connection timed out")??;
    send_control(
        &mut socket,
        &ClientMessage::Hello {
            request_id: Uuid::new_v4(),
            protocol_version: PROTOCOL_VERSION,
            client_name: "serial-desktop".into(),
            actor_kind: ActorKind::Human,
        },
    )
    .await?;
    send_control(
        &mut socket,
        &ClientMessage::Attach {
            request_id: Uuid::new_v4(),
            subscriptions: slots
                .iter()
                .map(|slot_id| Subscription {
                    slot_id: slot_id.clone(),
                    cursor: cursors.get(slot_id).cloned(),
                    tail_events: 500,
                })
                .collect(),
        },
    )
    .await?;
    let mut actor: Option<Actor> = None;
    let mut snapshots = HashMap::<String, SlotSnapshot>::new();
    let mut leases = HashMap::<String, ControlLease>::new();
    let mut pending = HashMap::<String, VecDeque<String>>::new();
    let mut acquire_requests = HashMap::<Uuid, String>::new();
    let mut acquiring = HashSet::<String>::new();
    let mut renew_requests = HashMap::<Uuid, String>::new();
    let mut renew = tokio::time::interval(Duration::from_secs(10));
    renew.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    renew.tick().await;

    loop {
        tokio::select! {
            incoming = socket.next() => {
                let Some(incoming) = incoming else { bail!("WebSocket stream ended") };
                match incoming? {
                    Message::Binary(bytes) => {
                        match decode_wire_frame(&bytes)? {
                            WireFrame::Rx(header, data) | WireFrame::Tx(header, data) => {
                                let replay = header.replay;
                                events.send(BackendEvent::Timeline {
                                    event: Box::new(header.into_event(data)),
                                    replay,
                                }).await.ok();
                            }
                            WireFrame::Control(message) => {
                                handle_server_message(
                                    message,
                                    &mut socket,
                                    events,
                                    &mut actor,
                                    &mut snapshots,
                                    &mut leases,
                                    &mut pending,
                                    &mut acquire_requests,
                                    &mut acquiring,
                                    &mut renew_requests,
                                ).await?;
                            }
                        }
                    }
                    Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
                    Message::Close(_) => bail!("server closed WebSocket"),
                    Message::Text(_) => bail!("server sent unsupported text WebSocket frame"),
                    _ => {}
                }
            }
            command = commands.recv() => match command {
                Some(WsCommand::SendLine { slot_id, text }) => {
                    pending.entry(slot_id.clone()).or_default().push_back(text);
                    if let Some(lease) = leases.get(&slot_id).cloned()
                        && actor.as_ref().is_some_and(|actor| actor.id == lease.owner.id)
                    {
                        flush_lines(&mut socket, &slot_id, &lease, &snapshots, &mut pending).await?;
                    } else if acquiring.insert(slot_id.clone()) {
                        let request_id = Uuid::new_v4();
                        acquire_requests.insert(request_id, slot_id.clone());
                        send_control(&mut socket, &ClientMessage::AcquireControl {
                            request_id,
                            slot_id,
                            mode: ControlMode::Queue,
                            ttl_ms: CONTROL_TTL_MS,
                        }).await?;
                    }
                }
                Some(WsCommand::Shutdown) | None => {
                    for slot_id in acquiring.drain() {
                        let _ = send_control(&mut socket, &ClientMessage::CancelAcquire {
                            request_id: Uuid::new_v4(),
                            slot_id,
                            // A queued actor has no lease. seriald matches the
                            // cancellation by authenticated actor identity.
                            control_id: Uuid::nil(),
                        }).await;
                    }
                    for (slot_id, lease) in leases.drain() {
                        let _ = send_control(&mut socket, &ClientMessage::ReleaseControl {
                            request_id: Uuid::new_v4(),
                            slot_id,
                            control_id: lease.id,
                            fence: lease.fence,
                        }).await;
                    }
                    let _ = socket.close(None).await;
                    return Ok(());
                }
            },
            _ = renew.tick() => {
                send_control(&mut socket, &ClientMessage::Ping {
                    request_id: Uuid::new_v4(),
                }).await?;
                for (slot_id, lease) in leases.clone() {
                    let request_id = Uuid::new_v4();
                    renew_requests.insert(request_id, slot_id.clone());
                    send_control(&mut socket, &ClientMessage::RenewControl {
                        request_id,
                        slot_id,
                        control_id: lease.id,
                        fence: lease.fence,
                        ttl_ms: CONTROL_TTL_MS,
                    }).await?;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_message<S>(
    message: ServerMessage,
    socket: &mut S,
    events: &mpsc::Sender<BackendEvent>,
    actor: &mut Option<Actor>,
    snapshots: &mut HashMap<String, SlotSnapshot>,
    leases: &mut HashMap<String, ControlLease>,
    pending: &mut HashMap<String, VecDeque<String>>,
    acquire_requests: &mut HashMap<Uuid, String>,
    acquiring: &mut HashSet<String>,
    renew_requests: &mut HashMap<Uuid, String>,
) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    match message {
        ServerMessage::Welcome { actor: issued, .. } => {
            *actor = Some(issued);
            events
                .send(BackendEvent::Connection {
                    connected: true,
                    message: "实时连接已建立".into(),
                })
                .await
                .ok();
        }
        ServerMessage::Snapshot { slot } => {
            snapshots.insert(slot.config.id.clone(), (*slot).clone());
            events.send(BackendEvent::Snapshot(slot)).await.ok();
        }
        ServerMessage::Timeline { event, replay } => {
            observe_timeline_control(
                &event,
                socket,
                actor,
                snapshots,
                leases,
                pending,
                acquire_requests,
                acquiring,
            )
            .await?;
            events
                .send(BackendEvent::Timeline {
                    event: Box::new(event),
                    replay,
                })
                .await
                .ok();
        }
        ServerMessage::Gap {
            slot_id, reason, ..
        } => {
            events
                .send(BackendEvent::Gap {
                    slot_id,
                    message: format!("实时回放存在缺口：{reason:?}"),
                })
                .await
                .ok();
        }
        ServerMessage::Result { request_id, result } => match result {
            CommandResult::ControlGranted { lease } => {
                if let Some(slot_id) = acquire_requests.remove(&request_id) {
                    acquiring.remove(&slot_id);
                    leases.insert(slot_id.clone(), lease.clone());
                    flush_lines(socket, &slot_id, &lease, snapshots, pending).await?;
                }
            }
            CommandResult::ControlQueued { position } => {
                events
                    .send(BackendEvent::Notice(format!(
                        "控制权已排队，当前位置 {position}"
                    )))
                    .await
                    .ok();
            }
            CommandResult::ControlRenewed { lease } => {
                if let Some(slot_id) = renew_requests.remove(&request_id) {
                    leases.insert(slot_id, lease);
                }
            }
            CommandResult::WriteAccepted { event_seq } => {
                events
                    .send(BackendEvent::Notice(format!(
                        "命令已确认发送，事件 #{event_seq}"
                    )))
                    .await
                    .ok();
            }
            CommandResult::HelloAccepted { actor: issued, .. } => *actor = Some(issued),
            _ => {}
        },
        ServerMessage::Error {
            request_id,
            message,
            ..
        } => {
            if let Some(request_id) = request_id {
                if let Some(slot_id) = acquire_requests.remove(&request_id) {
                    acquiring.remove(&slot_id);
                    pending.remove(&slot_id);
                }
                if let Some(slot_id) = renew_requests.remove(&request_id) {
                    leases.remove(&slot_id);
                }
            }
            events
                .send(BackendEvent::Error(format!("seriald 拒绝请求：{message}")))
                .await
                .ok();
        }
        ServerMessage::Lagged {
            slot_id,
            from_seq,
            to_seq,
        } => {
            events
                .send(BackendEvent::Gap {
                    slot_id,
                    message: format!("实时事件缺失 #{from_seq}..=#{to_seq}"),
                })
                .await
                .ok();
        }
        ServerMessage::ReplayBegin { .. } | ServerMessage::Ready { .. } => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn observe_timeline_control<S>(
    event: &TimelineEvent,
    socket: &mut S,
    actor: &Option<Actor>,
    snapshots: &mut HashMap<String, SlotSnapshot>,
    leases: &mut HashMap<String, ControlLease>,
    pending: &mut HashMap<String, VecDeque<String>>,
    acquire_requests: &mut HashMap<Uuid, String>,
    acquiring: &mut HashSet<String>,
) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    match event.kind {
        EventKind::ControlGranted => {
            let Some(lease) = event
                .metadata
                .get("lease")
                .and_then(|value| serde_json::from_value::<ControlLease>(value.clone()).ok())
            else {
                return Ok(());
            };
            if actor
                .as_ref()
                .is_none_or(|current| current.id != lease.owner.id)
            {
                return Ok(());
            }
            let slot_id = event.slot_id.clone();
            leases.insert(slot_id.clone(), lease.clone());
            acquiring.remove(&slot_id);
            acquire_requests.retain(|_, queued_slot| queued_slot != &slot_id);
            flush_lines(socket, &slot_id, &lease, snapshots, pending).await?;
        }
        EventKind::ControlReleased | EventKind::ControlRevoked | EventKind::ControlExpired => {
            let released_is_ours = actor.as_ref().is_some_and(|current| {
                event
                    .actor
                    .as_ref()
                    .is_some_and(|released| released.id == current.id)
            });
            if released_is_ours {
                leases.remove(&event.slot_id);
            }
        }
        _ => {}
    }
    Ok(())
}

async fn flush_lines<S>(
    socket: &mut S,
    slot_id: &str,
    lease: &ControlLease,
    snapshots: &HashMap<String, SlotSnapshot>,
    pending: &mut HashMap<String, VecDeque<String>>,
) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let eol = snapshots
        .get(slot_id)
        .and_then(|snapshot| snapshot.effective_write_eol.as_deref())
        .or_else(|| {
            snapshots
                .get(slot_id)
                .map(|snapshot| snapshot.config.settings.write_eol.as_str())
        })
        .unwrap_or("\r")
        .to_string();
    let queue = pending.entry(slot_id.to_string()).or_default();
    while let Some(line) = queue.pop_front() {
        let mut data = line.into_bytes();
        data.extend_from_slice(eol.as_bytes());
        send_control(
            socket,
            &ClientMessage::Write {
                request_id: Uuid::new_v4(),
                slot_id: slot_id.to_string(),
                control_id: lease.id,
                fence: lease.fence,
                data,
                operation_id: Some(Uuid::new_v4()),
                expected_run_id: None,
                pacing: None,
                description: None,
                command_sequence: None,
                sequence_precondition: None,
                cooperative: false,
            },
        )
        .await?;
    }
    Ok(())
}

async fn send_control<S>(socket: &mut S, message: &ClientMessage) -> Result<()>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let encoded = encode_client_control(message)?;
    socket.send(Message::Binary(encoded.into())).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serial_protocol::{EchoMode, SessionState, TargetActivity};

    use super::*;

    fn status(auto_open: bool) -> StatusResponse {
        let config = SlotConfig {
            id: "slot-1".into(),
            display_name: "DUT".into(),
            port: "COM3".into(),
            profile: "generic".into(),
            device_profile: None,
            enabled: false,
            settings: SerialSettings::default(),
        };
        StatusResponse {
            server_id: Uuid::nil(),
            daemon_epoch: Uuid::nil(),
            protocol_version: PROTOCOL_VERSION,
            config_revision: 7,
            sequence_write_precondition_supported: true,
            serial_context_precondition_supported: true,
            slots: vec![SlotSnapshot {
                config,
                daemon_epoch: Uuid::nil(),
                head_seq: 0,
                ring_oldest_seq: None,
                generation: 1,
                endpoint_present: true,
                session_state: SessionState::Disabled,
                state_reason: None,
                state_code: None,
                target_activity: TargetActivity::Unknown,
                last_rx_wall_time_ns: None,
                rx_offset: 0,
                tx_offset: 0,
                rx_overflow_bytes: 0,
                control: None,
                active_run: None,
                active_trigger: None,
                logging: serial_protocol::LoggingState::Healthy,
                effective_shell_prompt: None,
                effective_uboot_prompt: None,
                effective_write_eol: Some("\r".into()),
                effective_echo: Some(EchoMode::On),
                effective_transport: Some(serial_protocol::ResolvedTransportSettings {
                    baud_rate: 115_200,
                    data_bits: serial_protocol::DataBits::Eight,
                    parity: serial_protocol::Parity::None,
                    stop_bits: serial_protocol::StopBits::One,
                    flow_control: serial_protocol::FlowControl::None,
                    dtr: false,
                    rts: false,
                    auto_open,
                }),
                effective_write_pacing: None,
            }],
        }
    }

    fn transport(baud_rate: u32) -> TransportProfile {
        TransportProfile {
            name: String::new(),
            baud_rate,
            data_bits: serial_protocol::DataBits::Eight,
            parity: serial_protocol::Parity::None,
            stop_bits: serial_protocol::StopBits::One,
            flow_control: serial_protocol::FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        }
    }

    fn device(prompt: &str) -> DeviceProfile {
        DeviceProfile {
            name: String::new(),
            shell_prompt: Some(prompt.into()),
            uboot_prompt: None,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::On),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        }
    }

    fn settings_draft(transport: TransportProfile, device: DeviceProfile) -> SlotSettingsDraft {
        SlotSettingsDraft {
            slot_id: "slot-1".into(),
            expected_revision: 7,
            port: "COM4".into(),
            transport,
            device,
        }
    }

    #[test]
    fn slot_enable_is_an_authoritative_configuration_mutation_shape() {
        let mut current = status(true);
        let revision = current.config_revision;
        let mut slots = current
            .slots
            .drain(..)
            .map(|slot| slot.config)
            .collect::<Vec<_>>();
        slots[0].enabled = true;
        let request = ConfigureSlotsRequest {
            slots,
            expected_revision: Some(revision),
        };

        assert!(request.slots[0].enabled);
        assert_eq!(request.expected_revision, Some(7));
    }

    #[test]
    fn disabled_auto_open_is_detectable_before_enabling() {
        let current = status(false);
        assert!(
            current.slots[0]
                .effective_transport
                .is_some_and(|transport| !transport.auto_open)
        );
    }

    #[test]
    fn model_binding_is_guarded_by_revision_and_observed_current_binding() {
        let request = model_binding_request(Some("tl-as7230"), Some("old".into()), 11);

        assert_eq!(request.model_id.as_deref(), Some("tl-as7230"));
        assert_eq!(request.expected_revision, Some(11));
        assert_eq!(request.expected_current, Some(Some("old".into())));
        assert_eq!(
            request.confirmation_method,
            Some(ModelConfirmationMethod::Human)
        );
    }

    #[test]
    fn desktop_profile_names_are_content_addressed_slot_scoped_and_bounded() {
        let first = content_addressed_transport_name("slot / 一号", &transport(115_200)).unwrap();
        let second = content_addressed_transport_name("slot / 一号", &transport(115_200)).unwrap();
        let changed = content_addressed_transport_name("slot / 一号", &transport(921_600)).unwrap();
        let other = content_addressed_transport_name("slot / 二号", &transport(115_200)).unwrap();

        assert_eq!(first, second);
        assert_ne!(first, changed);
        assert_ne!(first, other);
        assert!(first.starts_with("desktop-slot-"));
        assert!(first.ends_with("-transport"));
        assert!(first.len() <= 64);
    }

    #[test]
    fn repeated_identical_preparation_reuses_profiles_without_growth() {
        let current = status(true);
        let mut desired_transport = transport(115_200);
        desired_transport.name =
            content_addressed_transport_name("slot-1", &desired_transport).unwrap();
        let mut desired_device = device("# ");
        desired_device.name = content_addressed_device_name("slot-1", &desired_device).unwrap();

        let (first_transports, first_changed) = prepare_transport_catalog(
            vec![transport(9_600)],
            &current.slots,
            "slot-1",
            desired_transport.clone(),
        )
        .unwrap();
        let (second_transports, second_changed) = prepare_transport_catalog(
            first_transports.clone(),
            &current.slots,
            "slot-1",
            desired_transport,
        )
        .unwrap();
        let (first_devices, _) =
            prepare_device_catalog(Vec::new(), &current.slots, "slot-1", desired_device.clone())
                .unwrap();
        let (second_devices, second_device_changed) = prepare_device_catalog(
            first_devices.clone(),
            &current.slots,
            "slot-1",
            desired_device,
        )
        .unwrap();

        assert!(first_changed);
        assert!(!second_changed);
        assert!(!second_device_changed);
        assert_eq!(first_transports, second_transports);
        assert_eq!(first_devices, second_devices);
    }

    #[test]
    fn preparation_and_later_stage_failure_leave_current_slot_binding_unchanged() {
        let current = status(true);
        let original_slot = current.slots[0].config.clone();
        let mut desired_transport = transport(921_600);
        desired_transport.name =
            content_addressed_transport_name("slot-1", &desired_transport).unwrap();
        let mut desired_device = device("login: ");
        desired_device.name = content_addressed_device_name("slot-1", &desired_device).unwrap();

        let (prepared_transports, _) = prepare_transport_catalog(
            vec![transport(9_600)],
            &current.slots,
            "slot-1",
            desired_transport.clone(),
        )
        .unwrap();
        let (prepared_devices, _) =
            prepare_device_catalog(Vec::new(), &current.slots, "slot-1", desired_device.clone())
                .unwrap();

        // These are the exact states left behind if Device preparation or the
        // final Slots transaction fails: only unbound catalog rows exist.
        assert_eq!(current.slots[0].config, original_slot);
        assert_ne!(current.slots[0].config.profile, desired_transport.name);
        assert_ne!(
            current.slots[0].config.device_profile.as_deref(),
            Some(desired_device.name.as_str())
        );
        assert!(
            prepared_transports
                .iter()
                .any(|profile| profile.name == desired_transport.name)
        );
        assert!(
            prepared_devices
                .iter()
                .any(|profile| profile.name == desired_device.name)
        );
    }

    #[test]
    fn changed_content_is_bounded_before_switch_and_old_profiles_prune_after_success() {
        let mut current = status(true);
        let mut old_transport = transport(115_200);
        old_transport.name = content_addressed_transport_name("slot-1", &old_transport).unwrap();
        let mut stale_transport = transport(230_400);
        stale_transport.name =
            content_addressed_transport_name("slot-1", &stale_transport).unwrap();
        let mut desired_transport = transport(921_600);
        desired_transport.name =
            content_addressed_transport_name("slot-1", &desired_transport).unwrap();
        let mut old_device = device("# ");
        old_device.name = content_addressed_device_name("slot-1", &old_device).unwrap();
        let mut desired_device = device("login: ");
        desired_device.name = content_addressed_device_name("slot-1", &desired_device).unwrap();
        current.slots[0].config.profile = old_transport.name.clone();
        current.slots[0].config.device_profile = Some(old_device.name.clone());

        let (prepared_transports, _) = prepare_transport_catalog(
            vec![old_transport.clone(), stale_transport, transport(9_600)],
            &current.slots,
            "slot-1",
            desired_transport.clone(),
        )
        .unwrap();
        let (prepared_devices, _) = prepare_device_catalog(
            vec![old_device.clone()],
            &current.slots,
            "slot-1",
            desired_device.clone(),
        )
        .unwrap();
        let owned_before_switch = prepared_transports
            .iter()
            .filter(|profile| is_owned_desktop_profile(&profile.name, "slot-1", "transport"))
            .count();
        assert_eq!(owned_before_switch, 2);
        assert_eq!(
            prepared_devices
                .iter()
                .filter(|profile| is_owned_desktop_profile(&profile.name, "slot-1", "device"))
                .count(),
            2
        );

        let draft = settings_draft(desired_transport.clone(), desired_device.clone());
        let switched = slot_replacement(
            &current,
            &draft,
            &desired_transport.name,
            &desired_device.name,
        )
        .unwrap();
        current.slots[0].config = switched[0].clone();
        let (clean_transports, transport_changed) = prune_transport_catalog(
            prepared_transports,
            &current.slots,
            "slot-1",
            &desired_transport.name,
        );
        let (clean_devices, device_changed) = prune_device_catalog(
            prepared_devices,
            &current.slots,
            "slot-1",
            &desired_device.name,
        );

        assert!(transport_changed);
        assert!(device_changed);
        assert!(
            clean_transports
                .iter()
                .any(|profile| profile.name == desired_transport.name)
        );
        assert!(
            !clean_transports
                .iter()
                .any(|profile| profile.name == old_transport.name)
        );
        assert_eq!(
            clean_devices
                .iter()
                .filter(|profile| is_owned_desktop_profile(&profile.name, "slot-1", "device"))
                .count(),
            1
        );
    }
}
