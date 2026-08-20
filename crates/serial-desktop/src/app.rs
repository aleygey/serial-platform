use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use eframe::egui::{
    self, Align, Color32, FontData, FontDefinitions, FontFamily, Key, Layout, RichText, ScrollArea,
    TextEdit,
};
use serial_protocol::{
    DataBits, DeviceModel, DeviceProfile, EchoMode, FlowControl, HealthResponse, Parity,
    PortDescriptor, ResolvedTransportSettings, SessionState, SlotModelBinding, StatusResponse,
    StopBits, TransportProfile,
};

use crate::{
    backend::{BackendCommand, BackendEvent, BackendHandle, SlotSettingsDraft},
    config::{ConfigStore, DesktopConfig, ThemePreference},
    model::{SlotViewModel, ensure_slot},
    process::LocalServiceState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Page {
    Console,
    Settings,
}

pub struct DesktopApp {
    backend: BackendHandle,
    store: Option<ConfigStore>,
    config: DesktopConfig,
    token: String,
    page: Page,
    slots: BTreeMap<String, SlotViewModel>,
    status: Option<StatusResponse>,
    health: Option<HealthResponse>,
    ports: Vec<PortDescriptor>,
    profiles: Vec<TransportProfile>,
    device_profiles: Vec<DeviceProfile>,
    device_models: Vec<DeviceModel>,
    model_bindings: Vec<SlotModelBinding>,
    connected: bool,
    connection_message: String,
    local_state: LocalServiceState,
    notice: Option<String>,
    error: Option<String>,
    new_slot_id: String,
    new_display_name: String,
    new_port: String,
    new_profile: String,
    profile_editor_slot: Option<String>,
    edit_transport_profile: String,
    edit_device_profile: String,
    edit_model_id: String,
    slot_settings_editor: Option<SlotSettingsDraft>,
    console_target: Option<(String, u64)>,
    agent_follow: bool,
    history_cursor: Option<usize>,
    focus_input: bool,
    confirm_stop_local: bool,
}

impl DesktopApp {
    pub fn new(creation: &eframe::CreationContext<'_>) -> Self {
        install_system_cjk_font(&creation.egui_ctx);

        let (store, config, startup_error) = match ConfigStore::discover() {
            Ok(store) => match store.load() {
                Ok(config) => (Some(store), config, None),
                Err(error) => (
                    Some(store),
                    DesktopConfig::default(),
                    Some(format!("桌面配置读取失败，已使用默认值：{error:#}")),
                ),
            },
            Err(error) => (
                None,
                DesktopConfig::default(),
                Some(format!("无法确定桌面配置目录：{error:#}")),
            ),
        };
        apply_theme(&creation.egui_ctx, config.theme);

        let backend = BackendHandle::spawn();
        let mut app = Self {
            backend,
            store,
            config,
            token: String::new(),
            page: Page::Console,
            slots: BTreeMap::new(),
            status: None,
            health: None,
            ports: Vec::new(),
            profiles: Vec::new(),
            device_profiles: Vec::new(),
            device_models: Vec::new(),
            model_bindings: Vec::new(),
            connected: false,
            connection_message: "尚未连接".into(),
            local_state: LocalServiceState::Stopped,
            notice: None,
            error: startup_error,
            new_slot_id: String::new(),
            new_display_name: String::new(),
            new_port: String::new(),
            new_profile: String::new(),
            profile_editor_slot: None,
            edit_transport_profile: String::new(),
            edit_device_profile: String::new(),
            edit_model_id: String::new(),
            slot_settings_editor: None,
            console_target: None,
            agent_follow: true,
            history_cursor: None,
            focus_input: false,
            confirm_stop_local: false,
        };
        if app.config.auto_start_local {
            app.start_local();
        }
        app.reconnect();
        app
    }

    fn queue(&mut self, command: BackendCommand) {
        if let Err(error) = self.backend.commands.try_send(command) {
            self.error = Some(format!("后台命令队列繁忙：{error}"));
        }
    }

    fn reconnect(&mut self) {
        self.queue(BackendCommand::Connect {
            endpoint: self.config.endpoint.clone(),
            token: (!self.token.trim().is_empty()).then(|| self.token.trim().to_string()),
        });
    }

    fn start_local(&mut self) {
        let program = (!self.config.local_program.trim().is_empty())
            .then(|| PathBuf::from(self.config.local_program.trim()));
        self.queue(BackendCommand::StartLocal {
            endpoint: self.config.endpoint.clone(),
            token: (!self.token.trim().is_empty()).then(|| self.token.trim().to_string()),
            program,
        });
    }

    fn persist_config(&mut self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        if let Err(error) = store.save(&self.config) {
            self.error = Some(format!("保存桌面配置失败：{error:#}"));
        }
    }

    fn drain_events(&mut self) {
        for _ in 0..4_096 {
            let Ok(event) = self.backend.events.try_recv() else {
                break;
            };
            match event {
                BackendEvent::LocalService(state) => self.local_state = state,
                BackendEvent::Connection { connected, message } => {
                    self.connected = connected;
                    self.connection_message = message;
                }
                BackendEvent::Health(health) => self.health = Some(health),
                BackendEvent::Status(status) => self.apply_status(*status),
                BackendEvent::Ports(ports) => {
                    self.ports = ports;
                    if self.new_port.is_empty() {
                        self.new_port = self
                            .ports
                            .first()
                            .map(|port| port.name.clone())
                            .unwrap_or_default();
                    }
                }
                BackendEvent::TransportProfiles(profiles) => {
                    self.profiles = profiles;
                    if self.new_profile.is_empty() {
                        self.new_profile = self
                            .profiles
                            .iter()
                            .find(|profile| profile.auto_open)
                            .or_else(|| self.profiles.first())
                            .map(|profile| profile.name.clone())
                            .unwrap_or_default();
                    }
                }
                BackendEvent::DeviceProfiles(profiles) => {
                    self.device_profiles = profiles;
                    self.profile_editor_slot = None;
                }
                BackendEvent::DeviceModels { models, bindings } => {
                    self.device_models = models;
                    self.model_bindings = bindings;
                    self.profile_editor_slot = None;
                }
                BackendEvent::SlotSettingsApplied {
                    slot_id,
                    transport_profile,
                    device_profile,
                    cleanup_warning,
                } => {
                    if self.config.selected_slot.as_deref() == Some(slot_id.as_str()) {
                        self.profile_editor_slot = None;
                        self.slot_settings_editor = None;
                    }
                    let mut notice = format!(
                        "已保存 {slot_id}；为避免影响共享配置，已绑定独占 Profiles：{transport_profile} / {device_profile}"
                    );
                    if let Some(warning) = cleanup_warning {
                        notice.push_str(&format!("；{warning}"));
                    }
                    self.notice = Some(notice);
                }
                BackendEvent::Snapshot(snapshot) => {
                    let slot_id = snapshot.config.id.clone();
                    ensure_slot(&mut self.slots, &slot_id).set_snapshot(*snapshot);
                }
                BackendEvent::Timeline { event, replay } => {
                    let slot_id = event.slot_id.clone();
                    ensure_slot(&mut self.slots, &slot_id).push_event(*event, replay);
                }
                BackendEvent::Gap { slot_id, message } => {
                    ensure_slot(&mut self.slots, &slot_id).push_notice(message);
                }
                BackendEvent::Notice(message) => self.notice = Some(message),
                BackendEvent::Error(message) => self.error = Some(message),
            }
        }
    }

    fn apply_status(&mut self, status: StatusResponse) {
        for snapshot in &status.slots {
            ensure_slot(&mut self.slots, &snapshot.config.id).set_snapshot(snapshot.clone());
        }
        let selected_exists = self
            .config
            .selected_slot
            .as_ref()
            .is_some_and(|id| status.slots.iter().any(|slot| &slot.config.id == id));
        if !selected_exists {
            self.config.selected_slot = status.slots.first().map(|slot| slot.config.id.clone());
            self.history_cursor = None;
            self.profile_editor_slot = None;
            self.slot_settings_editor = None;
            self.console_target = None;
            self.agent_follow = true;
        }
        self.status = Some(status);
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        let (console, settings, refresh, focus, theme, send) = ctx.input(|input| {
            let command = input.modifiers.command;
            (
                command && input.key_pressed(Key::Num1),
                command && input.key_pressed(Key::Comma),
                input.key_pressed(Key::F5) || (command && input.key_pressed(Key::R)),
                command && input.key_pressed(Key::L),
                command && input.key_pressed(Key::D),
                command && input.key_pressed(Key::Enter),
            )
        });
        if console {
            self.page = Page::Console;
        }
        if settings {
            self.page = Page::Settings;
        }
        if refresh {
            self.queue(BackendCommand::Refresh);
        }
        if focus {
            self.page = Page::Console;
            self.focus_input = true;
        }
        if theme {
            self.config.theme = match self.config.theme {
                ThemePreference::System | ThemePreference::Light => ThemePreference::Dark,
                ThemePreference::Dark => ThemePreference::Light,
            };
            apply_theme(ctx, self.config.theme);
            self.persist_config();
        }
        if send {
            self.send_selected_line();
        }
    }

    fn send_selected_line(&mut self) {
        let Some(slot_id) = self.config.selected_slot.clone() else {
            self.error = Some("请先选择一个 Slot".into());
            return;
        };
        let text = self
            .config
            .drafts
            .get(&slot_id)
            .cloned()
            .unwrap_or_default();
        if text.is_empty() {
            return;
        }
        self.queue(BackendCommand::SendLine {
            slot_id: slot_id.clone(),
            text: text.clone(),
        });
        self.config.remember_input(&slot_id, text);
        self.config.drafts.insert(slot_id, String::new());
        self.history_cursor = None;
        self.persist_config();
    }

    fn browse_history(&mut self, older: bool) {
        let Some(slot_id) = self.config.selected_slot.clone() else {
            return;
        };
        let Some(history) = self.config.input_history.get(&slot_id) else {
            return;
        };
        if history.is_empty() {
            return;
        }
        let cursor = match (older, self.history_cursor) {
            (true, Some(cursor)) => cursor.saturating_sub(1),
            (true, None) => history.len() - 1,
            (false, Some(cursor)) if cursor + 1 < history.len() => cursor + 1,
            (false, _) => {
                self.history_cursor = None;
                self.config.drafts.insert(slot_id, String::new());
                return;
            }
        };
        self.config.drafts.insert(slot_id, history[cursor].clone());
        self.history_cursor = Some(cursor);
    }

    fn top_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("navigation")
            .exact_height(48.0)
            .show(ctx, |ui| {
                ui.horizontal_centered(|ui| {
                    ui.add_space(8.0);
                    ui.label(RichText::new("Serial Platform").strong().size(18.0));
                    ui.separator();
                    if ui
                        .selectable_label(self.page == Page::Console, "串口工作台  ⌘1")
                        .clicked()
                    {
                        self.page = Page::Console;
                    }
                    if ui
                        .selectable_label(self.page == Page::Settings, "配置  ⌘,")
                        .clicked()
                    {
                        self.page = Page::Settings;
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        ui.add_space(8.0);
                        if ui.button("刷新  F5").clicked() {
                            self.queue(BackendCommand::Refresh);
                        }
                        let color = if self.connected {
                            Color32::from_rgb(70, 190, 115)
                        } else {
                            Color32::from_rgb(230, 150, 70)
                        };
                        ui.colored_label(
                            color,
                            if self.connected {
                                "● 已连接"
                            } else {
                                "● 离线"
                            },
                        )
                        .on_hover_text(&self.connection_message);
                    });
                });
            });
    }

    fn banners(&mut self, ctx: &egui::Context) {
        if self.error.is_some() || self.notice.is_some() {
            egui::TopBottomPanel::top("messages").show(ctx, |ui| {
                if let Some(error) = self.error.clone() {
                    ui.horizontal(|ui| {
                        ui.colored_label(Color32::from_rgb(235, 90, 90), format!("错误：{error}"));
                        if ui.small_button("关闭").clicked() {
                            self.error = None;
                        }
                    });
                }
                if let Some(notice) = self.notice.clone() {
                    ui.horizontal(|ui| {
                        ui.label(notice);
                        if ui.small_button("关闭").clicked() {
                            self.notice = None;
                        }
                    });
                }
            });
        }
    }

    fn slot_sidebar(&mut self, ctx: &egui::Context) {
        egui::SidePanel::left("slots")
            .resizable(true)
            .default_width(210.0)
            .width_range(170.0..=340.0)
            .show(ctx, |ui| {
                ui.heading("串口设备");
                ui.add_space(4.0);
                let snapshots = self
                    .status
                    .as_ref()
                    .map(|status| status.slots.clone())
                    .unwrap_or_default();
                if snapshots.is_empty() {
                    ui.weak("尚无 Slot，请到“配置”创建。");
                }
                ScrollArea::vertical().show(ui, |ui| {
                    for snapshot in snapshots {
                        let selected = self.config.selected_slot.as_deref()
                            == Some(snapshot.config.id.as_str());
                        let color = session_color(snapshot.session_state);
                        egui::Frame::group(ui.style()).show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.colored_label(color, "●");
                                if ui
                                    .selectable_label(selected, &snapshot.config.display_name)
                                    .clicked()
                                {
                                    self.config.selected_slot = Some(snapshot.config.id.clone());
                                    self.history_cursor = None;
                                    self.profile_editor_slot = None;
                                    self.slot_settings_editor = None;
                                    self.console_target = None;
                                    self.agent_follow = true;
                                    if let Some(slot) = self.slots.get_mut(&snapshot.config.id) {
                                        slot.follow_output = true;
                                        slot.unseen = 0;
                                    }
                                    self.persist_config();
                                }
                            });
                            ui.horizontal(|ui| {
                                ui.small(format!(
                                    "{} · {}",
                                    snapshot.config.port,
                                    session_label(snapshot.session_state)
                                ));
                                ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                    let label = if snapshot.config.enabled {
                                        "关闭"
                                    } else {
                                        "打开"
                                    };
                                    if ui.small_button(label).clicked() {
                                        self.queue(BackendCommand::SetSlotEnabled {
                                            slot_id: snapshot.config.id.clone(),
                                            enabled: !snapshot.config.enabled,
                                        });
                                    }
                                });
                            });
                        });
                        ui.add_space(4.0);
                    }
                });
            });
    }

    fn agent_panel(&mut self, ctx: &egui::Context) {
        egui::SidePanel::right("agent_history")
            .resizable(true)
            .default_width(330.0)
            .width_range(240.0..=520.0)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.heading("Agent 任务与命令");
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if self.agent_follow {
                            ui.weak("跟随最新");
                        } else if ui.small_button("返回最新").clicked() {
                            self.agent_follow = true;
                        }
                    });
                });
                ui.weak("最新记录置顶；展开任务或选择具体命令可定位串口输出");
                ui.separator();
                let slot_id = self.config.selected_slot.clone();
                let records = self
                    .config
                    .selected_slot
                    .as_ref()
                    .and_then(|slot_id| self.slots.get(slot_id))
                    .map(|slot| slot.agent_records.iter().rev().cloned().collect::<Vec<_>>())
                    .unwrap_or_default();
                if records.is_empty() {
                    ui.weak("当前 Slot 还没有 Agent 命令记录。");
                }
                let manual_scroll_intent = ui.ui_contains_pointer()
                    && ui.ctx().input(|input| input.raw_scroll_delta.y != 0.0);
                if manual_scroll_intent {
                    self.agent_follow = false;
                }
                let mut target_seq = None;
                let mut first_response = None;
                let output = ScrollArea::vertical()
                    .id_salt(("agent-history", slot_id.as_deref()))
                    .show(ui, |ui| {
                        for (record_index, record) in records.into_iter().enumerate() {
                            let title = record.status.map_or_else(
                                || record.description.clone(),
                                |status| format!("Run {status} · {}", record.description),
                            );
                            if !record.commands.is_empty() {
                                let collapsing = egui::CollapsingHeader::new(title)
                                    .id_salt((
                                        "agent-record",
                                        slot_id.as_deref(),
                                        record.seq,
                                        record.sequence_id,
                                    ))
                                    .show(ui, |ui| {
                                        for (position, command) in
                                            record.commands.iter().enumerate()
                                        {
                                            if record.commands.len() > 1 {
                                                let number = command
                                                    .step_index
                                                    .map_or(position + 1, |index| {
                                                        index.saturating_add(1)
                                                    });
                                                ui.horizontal_top(|ui| {
                                                    ui.weak(format!("{number}."));
                                                    let response = ui.selectable_label(
                                                        false,
                                                        RichText::new(command.text()).monospace(),
                                                    );
                                                    if response.clicked() {
                                                        target_seq = Some(command.first_seq);
                                                    }
                                                });
                                            } else {
                                                let response = ui.selectable_label(
                                                    false,
                                                    RichText::new(command.text()).monospace(),
                                                );
                                                if response.clicked() {
                                                    target_seq = Some(command.first_seq);
                                                }
                                            }
                                        }
                                    });
                                if collapsing.header_response.clicked() {
                                    target_seq = Some(record.seq);
                                }
                                if record_index == 0 {
                                    first_response = Some(collapsing.header_response);
                                }
                            } else {
                                let response = ui.selectable_label(false, title);
                                if response.clicked() {
                                    target_seq = Some(record.seq);
                                }
                                if record_index == 0 {
                                    first_response = Some(response);
                                }
                            }
                            ui.add_space(3.0);
                        }
                    });
                if self.agent_follow
                    && let Some(response) = first_response
                {
                    response.scroll_to_me(Some(Align::TOP));
                }
                let pointer_over = ui
                    .ctx()
                    .pointer_hover_pos()
                    .is_some_and(|position| output.inner_rect.contains(position));
                let dragging = ui.ctx().input(|input| input.pointer.primary_down());
                if self.agent_follow && output.state.offset.y > 1.0 && pointer_over && dragging {
                    self.agent_follow = false;
                }
                if let (Some(slot_id), Some(seq)) = (slot_id, target_seq) {
                    self.agent_follow = false;
                    self.console_target = Some((slot_id.clone(), seq));
                    if let Some(slot) = self.slots.get_mut(&slot_id) {
                        slot.follow_output = false;
                    }
                }
            });
    }

    fn input_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("human_input")
            .exact_height(64.0)
            .show(ctx, |ui| {
                let Some(slot_id) = self.config.selected_slot.clone() else {
                    ui.centered_and_justified(|ui| ui.weak("选择 Slot 后即可发送命令"));
                    return;
                };
                ui.horizontal(|ui| {
                    ui.label("人类输入");
                    let draft = self.config.drafts.entry(slot_id).or_default();
                    let response = ui.add_sized(
                        [ui.available_width() - 100.0, 36.0],
                        TextEdit::singleline(draft)
                            .hint_text("输入命令，Enter 发送；↑/↓ 查看历史")
                            .font(egui::TextStyle::Monospace),
                    );
                    if self.focus_input {
                        response.request_focus();
                        self.focus_input = false;
                    }
                    let (enter, older, newer) = ui.input(|input| {
                        (
                            response.has_focus()
                                && !input.modifiers.command
                                && input.key_pressed(Key::Enter),
                            response.has_focus() && input.key_pressed(Key::ArrowUp),
                            response.has_focus() && input.key_pressed(Key::ArrowDown),
                        )
                    });
                    if ui.button("发送").clicked() || enter {
                        self.send_selected_line();
                    } else if older {
                        self.browse_history(true);
                    } else if newer {
                        self.browse_history(false);
                    }
                });
            });
    }

    fn console(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            let selected = self.config.selected_slot.clone();
            let Some(slot_id) = selected else {
                ui.centered_and_justified(|ui| ui.heading("选择或配置一个串口设备"));
                return;
            };
            let Some(slot) = self.slots.get(&slot_id) else {
                ui.centered_and_justified(|ui| ui.label("正在加载 Slot 状态…"));
                return;
            };
            let rows = slot.rows.iter().cloned().collect::<Vec<_>>();
            let mut follow_output = slot.follow_output;
            let unseen = slot.unseen;
            let snapshot = slot.snapshot.clone();
            let manual_scroll_intent =
                ui.ui_contains_pointer() && ui.ctx().input(|input| input.raw_scroll_delta.y != 0.0);
            if manual_scroll_intent {
                follow_output = false;
                if let Some(slot) = self.slots.get_mut(&slot_id) {
                    slot.follow_output = false;
                }
            }
            if let Some(snapshot) = snapshot.as_ref() {
                ui.horizontal(|ui| {
                    ui.heading(&snapshot.config.display_name);
                    ui.weak(&snapshot.config.port);
                    ui.colored_label(
                        session_color(snapshot.session_state),
                        session_label(snapshot.session_state),
                    );
                    if let Some(reason) = snapshot.state_reason.as_deref() {
                        ui.weak(reason);
                    }
                    ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                        if follow_output {
                            ui.weak("跟随最新");
                        } else if ui
                            .button(format!("返回最新（{unseen} 条新记录）"))
                            .clicked()
                        {
                            self.console_target = None;
                            if let Some(slot) = self.slots.get_mut(&slot_id) {
                                slot.follow_output = true;
                                slot.unseen = 0;
                            }
                        }
                    });
                });
            }
            ui.separator();
            let target_seq = self
                .console_target
                .as_ref()
                .filter(|(target_slot, _)| target_slot == &slot_id)
                .map(|(_, seq)| *seq);
            let target_index =
                target_seq.and_then(|target| nearest_console_row_index(&rows, target));
            let mut target_requested = false;
            let output = ScrollArea::vertical()
                .id_salt(("serial-output", &slot_id))
                .stick_to_bottom(follow_output && target_seq.is_none())
                .auto_shrink([false; 2])
                .show(ui, |ui| {
                    for (index, row) in rows.iter().enumerate() {
                        let marker = match row.direction {
                            serial_protocol::Direction::Rx => "←",
                            serial_protocol::Direction::Tx => "→",
                            serial_protocol::Direction::None => "•",
                        };
                        let line =
                            format!("{} {} {:<12} {}", row.time, marker, row.source, row.text);
                        let response = ui.add(
                            egui::Label::new(RichText::new(line).monospace())
                                .selectable(true)
                                .wrap(),
                        );
                        if target_index == Some(index) {
                            response.scroll_to_me(Some(Align::Center));
                            target_requested = true;
                            let stroke = ui.visuals().selection.stroke;
                            ui.painter().rect_stroke(
                                response.rect.expand(1.0),
                                2.0,
                                stroke,
                                egui::StrokeKind::Inside,
                            );
                        }
                    }
                });
            if target_requested {
                self.console_target = None;
            }
            let max_offset = (output.content_size.y - output.inner_rect.height()).max(0.0);
            let pointer_over = ui
                .ctx()
                .pointer_hover_pos()
                .is_some_and(|position| output.inner_rect.contains(position));
            let near_bottom = output.state.offset.y >= max_offset - 2.0;
            if let Some(slot) = self.slots.get_mut(&slot_id) {
                let dragging = ui.ctx().input(|input| input.pointer.primary_down());
                if pointer_over && dragging && !near_bottom {
                    slot.follow_output = false;
                } else if near_bottom && target_seq.is_none() {
                    slot.follow_output = true;
                    slot.unseen = 0;
                }
            }
        });
    }

    fn settings(&mut self, ctx: &egui::Context) {
        self.sync_profile_editor();
        egui::CentralPanel::default().show(ctx, |ui| {
            ScrollArea::vertical().show(ui, |ui| {
                ui.heading("桌面与本地服务");
                ui.add_space(8.0);
                egui::Grid::new("desktop-settings-grid")
                    .num_columns(2)
                    .spacing([16.0, 10.0])
                    .show(ui, |ui| {
                        ui.label("seriald 地址");
                        ui.text_edit_singleline(&mut self.config.endpoint);
                        ui.end_row();
                        ui.label("访问令牌");
                        ui.add(
                            TextEdit::singleline(&mut self.token)
                                .password(true)
                                .hint_text("仅保存在内存中"),
                        );
                        ui.end_row();
                        ui.label("本地 serial 路径");
                        ui.add(
                            TextEdit::singleline(&mut self.config.local_program)
                                .hint_text("留空：使用 App 旁边的 serial"),
                        );
                        ui.end_row();
                        ui.label("启动行为");
                        ui.checkbox(
                            &mut self.config.auto_start_local,
                            "启动 App 时管理本地 serial serve",
                        );
                        ui.end_row();
                        ui.label("主题");
                        egui::ComboBox::from_id_salt("theme")
                            .selected_text(theme_label(self.config.theme))
                            .show_ui(ui, |ui| {
                                ui.selectable_value(
                                    &mut self.config.theme,
                                    ThemePreference::System,
                                    "跟随系统",
                                );
                                ui.selectable_value(
                                    &mut self.config.theme,
                                    ThemePreference::Dark,
                                    "深色",
                                );
                                ui.selectable_value(
                                    &mut self.config.theme,
                                    ThemePreference::Light,
                                    "浅色",
                                );
                            });
                        ui.end_row();
                    });
                ui.horizontal(|ui| {
                    if ui.button("保存并重新连接").clicked() {
                        apply_theme(ctx, self.config.theme);
                        self.persist_config();
                        self.reconnect();
                    }
                    if ui.button("启动本地服务").clicked() {
                        self.start_local();
                    }
                    if ui.button("停止本地服务").clicked() {
                        self.confirm_stop_local = true;
                    }
                    ui.weak(local_state_label(&self.local_state));
                });
                ui.weak("关闭 App 不会强制结束 seriald；需要停止时请使用上方按钮并确认。");
                if let Some(store) = self.store.as_ref() {
                    ui.weak(format!("配置文件：{}", store.path().display()));
                }
                if let Some(health) = self.health.as_ref() {
                    ui.weak(format!(
                        "服务 {} · 运行 {} 秒 · 认证{}",
                        health.status,
                        health.uptime_ms / 1_000,
                        if health.auth_required {
                            "已启用"
                        } else {
                            "未启用"
                        }
                    ));
                }

                ui.add_space(24.0);
                self.selected_slot_profile_editor(ui);

                ui.add_space(24.0);
                ui.heading("新建串口 Slot");
                ui.weak("创建使用 seriald 的配置事务；App 不直接访问物理串口。");
                egui::Grid::new("new-slot-grid")
                    .num_columns(2)
                    .spacing([16.0, 10.0])
                    .show(ui, |ui| {
                        ui.label("Slot ID");
                        ui.text_edit_singleline(&mut self.new_slot_id);
                        ui.end_row();
                        ui.label("机型 / 显示名称");
                        ui.text_edit_singleline(&mut self.new_display_name);
                        ui.end_row();
                        ui.label("串口");
                        egui::ComboBox::from_id_salt("new-slot-port")
                            .selected_text(if self.new_port.is_empty() {
                                "请选择"
                            } else {
                                &self.new_port
                            })
                            .show_ui(ui, |ui| {
                                for port in &self.ports {
                                    ui.selectable_value(
                                        &mut self.new_port,
                                        port.name.clone(),
                                        &port.name,
                                    );
                                }
                            });
                        ui.end_row();
                        ui.label("传输配置");
                        egui::ComboBox::from_id_salt("new-slot-profile")
                            .selected_text(if self.new_profile.is_empty() {
                                "请选择"
                            } else {
                                &self.new_profile
                            })
                            .show_ui(ui, |ui| {
                                for profile in &self.profiles {
                                    let suffix = if profile.auto_open {
                                        ""
                                    } else {
                                        "（禁止自动打开）"
                                    };
                                    ui.selectable_value(
                                        &mut self.new_profile,
                                        profile.name.clone(),
                                        format!(
                                            "{} · {} bps{suffix}",
                                            profile.name, profile.baud_rate
                                        ),
                                    );
                                }
                            });
                        ui.end_row();
                    });
                if ui.button("创建并打开串口").clicked() {
                    self.queue(BackendCommand::CreateSlot {
                        slot_id: self.new_slot_id.clone(),
                        display_name: self.new_display_name.clone(),
                        port: self.new_port.clone(),
                        profile: self.new_profile.clone(),
                    });
                }

                ui.add_space(24.0);
                ui.heading("已配置串口");
                if let Some(status) = self.status.as_ref() {
                    ui.weak(format!("配置修订号 {}", status.config_revision));
                    let slots = status.slots.clone();
                    for slot in slots {
                        ui.horizontal(|ui| {
                            ui.label(RichText::new(&slot.config.display_name).strong());
                            ui.monospace(&slot.config.port);
                            ui.weak(format!("profile: {}", slot.config.profile));
                            let label = if slot.config.enabled {
                                "关闭串口"
                            } else {
                                "打开串口"
                            };
                            if ui.button(label).clicked() {
                                self.queue(BackendCommand::SetSlotEnabled {
                                    slot_id: slot.config.id.clone(),
                                    enabled: !slot.config.enabled,
                                });
                            }
                        });
                    }
                }
                ui.add_space(24.0);
                ui.heading("快捷键");
                ui.monospace("⌘/Ctrl+1  工作台    ⌘/Ctrl+,  配置    F5 / ⌘/Ctrl+R  刷新");
                ui.monospace("⌘/Ctrl+L  聚焦输入    ⌘/Ctrl+Enter  发送    ⌘/Ctrl+D  切换主题");
            });
        });
    }

    fn sync_profile_editor(&mut self) {
        let selected = self.config.selected_slot.clone();
        if self.profile_editor_slot == selected && self.slot_settings_editor.is_some() {
            return;
        }
        self.profile_editor_slot = selected.clone();
        let Some(slot_id) = selected else {
            self.edit_transport_profile.clear();
            self.edit_device_profile.clear();
            self.edit_model_id.clear();
            self.slot_settings_editor = None;
            return;
        };
        if let Some((revision, snapshot)) = self.status.as_ref().and_then(|status| {
            status
                .slots
                .iter()
                .find(|slot| slot.config.id == slot_id)
                .map(|snapshot| (status.config_revision, snapshot))
        }) {
            self.edit_transport_profile = snapshot.config.profile.clone();
            self.edit_device_profile = snapshot.config.device_profile.clone().unwrap_or_default();
            self.slot_settings_editor = Some(slot_settings_draft(revision, snapshot));
        }
        self.edit_model_id = self
            .model_bindings
            .iter()
            .find(|binding| binding.slot_id == slot_id)
            .map(|binding| binding.model_id.clone())
            .unwrap_or_default();
    }

    fn selected_slot_profile_editor(&mut self, ui: &mut egui::Ui) {
        ui.heading("当前 Slot 配置");
        let Some(slot_id) = self.config.selected_slot.clone() else {
            ui.weak("先在工作台选择一个 Slot。");
            return;
        };
        let Some((current_revision, snapshot)) = self.status.as_ref().and_then(|status| {
            status
                .slots
                .iter()
                .find(|slot| slot.config.id == slot_id)
                .cloned()
                .map(|snapshot| (status.config_revision, snapshot))
        }) else {
            ui.weak("正在加载 Slot 配置…");
            return;
        };

        ui.label(RichText::new(&snapshot.config.display_name).strong());
        ui.weak(format!(
            "{} · Slot {}",
            snapshot.config.port, snapshot.config.id
        ));
        ui.add_space(8.0);
        ui.label(RichText::new("直接编辑当前串口").strong());
        ui.weak(
            "保存会先准备内容寻址、未绑定的 Slot 专属 Profiles，再一次性切换绑定；不会修改当前或其他 Slot 正在使用的 Profile。",
        );
        let mut apply_settings = false;
        let mut reload_settings = false;
        if let Some(editor) = self.slot_settings_editor.as_mut() {
            slot_settings_form(ui, editor, &self.ports);
            let stale = editor.expected_revision != current_revision;
            ui.horizontal(|ui| {
                if ui
                    .add_enabled(!stale, egui::Button::new("保存当前串口配置"))
                    .on_disabled_hover_text("配置已更新，请先重新加载")
                    .clicked()
                {
                    apply_settings = true;
                }
                if ui.button("放弃修改并重新加载").clicked() {
                    reload_settings = true;
                }
                ui.weak(format!("基于配置修订号 {}", editor.expected_revision));
            });
            if stale {
                ui.colored_label(
                    Color32::from_rgb(230, 150, 70),
                    format!(
                        "服务配置现为修订号 {current_revision}；为避免覆盖并发修改，请放弃修改并重新加载。"
                    ),
                );
            }
            if !editor.transport.auto_open && snapshot.config.enabled {
                ui.colored_label(
                    Color32::from_rgb(230, 150, 70),
                    "关闭 auto-open 并保存时会同时关闭当前 Slot。",
                );
            }
        } else {
            ui.weak("正在准备可编辑配置…");
        }
        if apply_settings && let Some(editor) = self.slot_settings_editor.clone() {
            self.queue(BackendCommand::ApplySlotSettings(editor));
        }
        if reload_settings {
            self.profile_editor_slot = None;
            self.slot_settings_editor = None;
        }

        ui.add_space(14.0);
        ui.label(RichText::new("有效 Transport 参数").strong());
        if let Some(transport) = snapshot.effective_transport {
            egui::Grid::new("effective-transport-grid")
                .num_columns(4)
                .striped(true)
                .show(ui, |ui| {
                    ui.label("波特率");
                    ui.monospace(transport.baud_rate.to_string());
                    ui.label("数据位");
                    ui.monospace(format!("{:?}", transport.data_bits));
                    ui.end_row();
                    ui.label("校验");
                    ui.monospace(format!("{:?}", transport.parity));
                    ui.label("停止位");
                    ui.monospace(format!("{:?}", transport.stop_bits));
                    ui.end_row();
                    ui.label("流控");
                    ui.monospace(format!("{:?}", transport.flow_control));
                    ui.label("DTR / RTS");
                    ui.monospace(format!("{} / {}", transport.dtr, transport.rts));
                    ui.end_row();
                    ui.label("auto_open");
                    ui.monospace(transport.auto_open.to_string());
                    ui.label("当前 Profile");
                    ui.monospace(&snapshot.config.profile);
                    ui.end_row();
                });
        } else {
            ui.weak("daemon 未提供 resolved transport；显示兼容快照。");
            ui.monospace(format!(
                "{} bps · {:?} · {:?} · {:?} · {:?} · DTR={} · RTS={} · auto_open={}",
                snapshot.config.settings.baud_rate,
                snapshot.config.settings.data_bits,
                snapshot.config.settings.parity,
                snapshot.config.settings.stop_bits,
                snapshot.config.settings.flow_control,
                snapshot.config.settings.dtr,
                snapshot.config.settings.rts,
                snapshot.config.settings.auto_open,
            ));
        }

        ui.add_space(10.0);
        ui.label(RichText::new("有效 Device 参数").strong());
        egui::Grid::new("effective-device-grid")
            .num_columns(4)
            .striped(true)
            .show(ui, |ui| {
                ui.label("Shell 提示符");
                ui.monospace(optional_text(snapshot.effective_shell_prompt.as_deref()));
                ui.label("U-Boot 提示符");
                ui.monospace(optional_text(snapshot.effective_uboot_prompt.as_deref()));
                ui.end_row();
                ui.label("换行符");
                ui.monospace(visible_eol(
                    snapshot
                        .effective_write_eol
                        .as_deref()
                        .unwrap_or(&snapshot.config.settings.write_eol),
                ));
                ui.label("回显");
                ui.monospace(format!(
                    "{:?}",
                    snapshot
                        .effective_echo
                        .unwrap_or(snapshot.config.settings.echo)
                ));
                ui.end_row();
                let pacing =
                    snapshot
                        .effective_write_pacing
                        .unwrap_or(serial_protocol::WritePacing {
                            chunk_size: snapshot.config.settings.write_chunk_size,
                            chunk_delay_ms: snapshot.config.settings.write_chunk_delay_ms,
                        });
                ui.label("写入分块");
                ui.monospace(format!("{} bytes", pacing.chunk_size));
                ui.label("分块延迟");
                ui.monospace(format!("{} ms", pacing.chunk_delay_ms));
                ui.end_row();
                ui.label("当前 Device Profile");
                ui.monospace(
                    snapshot
                        .config
                        .device_profile
                        .as_deref()
                        .unwrap_or("无（兼容设置）"),
                );
                ui.label("探测");
                ui.monospace(if snapshot.config.settings.probe.is_some() {
                    "已配置"
                } else {
                    "关闭"
                });
                ui.end_row();
            });

        ui.add_space(10.0);
        ui.horizontal(|ui| {
            ui.label("Transport Profile");
            egui::ComboBox::from_id_salt("edit-transport-profile")
                .selected_text(if self.edit_transport_profile.is_empty() {
                    "请选择"
                } else {
                    &self.edit_transport_profile
                })
                .show_ui(ui, |ui| {
                    for profile in &self.profiles {
                        ui.selectable_value(
                            &mut self.edit_transport_profile,
                            profile.name.clone(),
                            format!("{} · {} bps", profile.name, profile.baud_rate),
                        );
                    }
                });
            ui.label("Device Profile");
            egui::ComboBox::from_id_salt("edit-device-profile")
                .selected_text(if self.edit_device_profile.is_empty() {
                    "无（兼容设置）"
                } else {
                    &self.edit_device_profile
                })
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut self.edit_device_profile,
                        String::new(),
                        "无（兼容设置）",
                    );
                    for profile in &self.device_profiles {
                        ui.selectable_value(
                            &mut self.edit_device_profile,
                            profile.name.clone(),
                            &profile.name,
                        );
                    }
                });
            if ui.button("应用 Profile").clicked() {
                self.queue(BackendCommand::SetSlotProfiles {
                    slot_id: slot_id.clone(),
                    transport_profile: self.edit_transport_profile.clone(),
                    device_profile: (!self.edit_device_profile.is_empty())
                        .then(|| self.edit_device_profile.clone()),
                });
            }
        });

        let current_binding = self
            .model_bindings
            .iter()
            .find(|binding| binding.slot_id == slot_id)
            .cloned();
        let current_model = current_binding.as_ref().and_then(|binding| {
            self.device_models
                .iter()
                .find(|model| model.id == binding.model_id)
        });
        ui.add_space(10.0);
        ui.label(RichText::new("样机型号绑定").strong());
        if let Some(model) = current_model {
            ui.label(format!(
                "{} · ID={} · 父级={} · 别名={}",
                model.name,
                model.id,
                model.parent_id.as_deref().unwrap_or("无"),
                if model.aliases.is_empty() {
                    "无".into()
                } else {
                    model.aliases.join(", ")
                }
            ));
        } else {
            ui.weak("当前未绑定样机型号。");
        }
        if let Some(binding) = current_binding {
            ui.weak(format!(
                "确认方式 {:?} · 来源 {}{}",
                binding.confirmation_method,
                binding.source,
                binding
                    .note
                    .as_deref()
                    .map(|note| format!(" · {note}"))
                    .unwrap_or_default()
            ));
        }
        ui.horizontal(|ui| {
            ui.label("型号");
            egui::ComboBox::from_id_salt("edit-device-model")
                .selected_text(model_choice_label(&self.device_models, &self.edit_model_id))
                .show_ui(ui, |ui| {
                    ui.selectable_value(&mut self.edit_model_id, String::new(), "未绑定");
                    for model in &self.device_models {
                        ui.selectable_value(
                            &mut self.edit_model_id,
                            model.id.clone(),
                            format!("{} · {}", model.name, model.id),
                        );
                    }
                });
            if ui.button("应用型号绑定").clicked() {
                self.queue(BackendCommand::SetSlotModel {
                    slot_id,
                    model_id: (!self.edit_model_id.is_empty()).then(|| self.edit_model_id.clone()),
                });
            }
        });
        ui.weak("型号选择由当前操作员确认为 Human；所有修改均带配置修订号，冲突时失败而不会覆盖别人的更新。");
    }

    fn stop_confirmation(&mut self, ctx: &egui::Context) {
        if !self.confirm_stop_local {
            return;
        }
        egui::Window::new("停止本地服务？")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
            .show(ctx, |ui| {
                ui.label("只会停止由本 App 启动的 serial serve。Unix 会先请求优雅退出并等待 journal 刷盘，超时才强制结束。");
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("取消").clicked() {
                        self.confirm_stop_local = false;
                    }
                    if ui.button(RichText::new("确认停止").color(Color32::from_rgb(235, 90, 90))).clicked() {
                        self.confirm_stop_local = false;
                        self.queue(BackendCommand::StopLocal);
                    }
                });
            });
    }
}

impl eframe::App for DesktopApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();
        self.handle_shortcuts(ctx);
        self.top_bar(ctx);
        self.banners(ctx);
        match self.page {
            Page::Console => {
                self.input_bar(ctx);
                self.slot_sidebar(ctx);
                self.agent_panel(ctx);
                self.console(ctx);
            }
            Page::Settings => self.settings(ctx),
        }
        self.stop_confirmation(ctx);
        ctx.request_repaint_after(Duration::from_millis(100));
    }

    fn save(&mut self, _storage: &mut dyn eframe::Storage) {
        self.persist_config();
    }

    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        let _ = self.backend.commands.try_send(BackendCommand::Shutdown);
        self.persist_config();
    }
}

fn apply_theme(ctx: &egui::Context, theme: ThemePreference) {
    ctx.set_theme(match theme {
        ThemePreference::System => egui::ThemePreference::System,
        ThemePreference::Dark => egui::ThemePreference::Dark,
        ThemePreference::Light => egui::ThemePreference::Light,
    });
}

fn install_system_cjk_font(ctx: &egui::Context) {
    let candidates: &[&str] = if cfg!(target_os = "macos") {
        &[
            "/System/Library/Fonts/PingFang.ttc",
            "/System/Library/Fonts/STHeiti Light.ttc",
        ]
    } else if cfg!(target_os = "windows") {
        &[
            "C:\\Windows\\Fonts\\msyh.ttc",
            "C:\\Windows\\Fonts\\simhei.ttf",
        ]
    } else {
        &[
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
    };
    let Some(bytes) = candidates.iter().find_map(|path| std::fs::read(path).ok()) else {
        return;
    };
    let mut fonts = FontDefinitions::default();
    fonts.font_data.insert(
        "serial-system-cjk".into(),
        Arc::new(FontData::from_owned(bytes)),
    );
    for family in [FontFamily::Proportional, FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .push("serial-system-cjk".into());
    }
    ctx.set_fonts(fonts);
}

fn session_label(state: SessionState) -> &'static str {
    match state {
        SessionState::Disabled => "已关闭",
        SessionState::WaitingForPort => "等待串口",
        SessionState::Opening => "正在打开",
        SessionState::Online => "在线",
        SessionState::Stopping => "正在关闭",
        SessionState::Backoff => "重试等待",
    }
}

fn session_color(state: SessionState) -> Color32 {
    match state {
        SessionState::Online => Color32::from_rgb(70, 190, 115),
        SessionState::Opening | SessionState::Stopping | SessionState::WaitingForPort => {
            Color32::from_rgb(230, 170, 70)
        }
        SessionState::Backoff => Color32::from_rgb(235, 90, 90),
        SessionState::Disabled => Color32::GRAY,
    }
}

fn theme_label(theme: ThemePreference) -> &'static str {
    match theme {
        ThemePreference::System => "跟随系统",
        ThemePreference::Dark => "深色",
        ThemePreference::Light => "浅色",
    }
}

fn local_state_label(state: &LocalServiceState) -> String {
    match state {
        LocalServiceState::Stopped => "本地服务未由 App 管理".into(),
        LocalServiceState::Starting { program } => format!("正在启动 {}", program.display()),
        LocalServiceState::Running { pid, program } => {
            format!("本地服务 PID {pid} · {}", program.display())
        }
        LocalServiceState::Exited { code } => format!("本地服务已退出（{code:?}）"),
    }
}

fn nearest_console_row_index(rows: &[crate::model::ConsoleRow], target_seq: u64) -> Option<usize> {
    rows.iter()
        .position(|row| row.seq >= target_seq)
        .or_else(|| (!rows.is_empty()).then(|| rows.len() - 1))
}

fn slot_settings_draft(
    expected_revision: u64,
    snapshot: &serial_protocol::SlotSnapshot,
) -> SlotSettingsDraft {
    let transport = snapshot
        .effective_transport
        .unwrap_or(ResolvedTransportSettings {
            baud_rate: snapshot.config.settings.baud_rate,
            data_bits: snapshot.config.settings.data_bits,
            parity: snapshot.config.settings.parity,
            stop_bits: snapshot.config.settings.stop_bits,
            flow_control: snapshot.config.settings.flow_control,
            dtr: snapshot.config.settings.dtr,
            rts: snapshot.config.settings.rts,
            auto_open: snapshot.config.settings.auto_open,
        });
    let pacing = snapshot
        .effective_write_pacing
        .unwrap_or(serial_protocol::WritePacing {
            chunk_size: snapshot.config.settings.write_chunk_size,
            chunk_delay_ms: snapshot.config.settings.write_chunk_delay_ms,
        });
    SlotSettingsDraft {
        slot_id: snapshot.config.id.clone(),
        expected_revision,
        port: snapshot.config.port.clone(),
        transport: TransportProfile {
            name: snapshot.config.profile.clone(),
            baud_rate: transport.baud_rate,
            data_bits: transport.data_bits,
            parity: transport.parity,
            stop_bits: transport.stop_bits,
            flow_control: transport.flow_control,
            dtr: transport.dtr,
            rts: transport.rts,
            auto_open: transport.auto_open,
        },
        device: DeviceProfile {
            name: snapshot
                .config
                .device_profile
                .clone()
                .unwrap_or_else(|| "未绑定".into()),
            shell_prompt: snapshot.effective_shell_prompt.clone(),
            uboot_prompt: snapshot.effective_uboot_prompt.clone(),
            write_eol: Some(
                snapshot
                    .effective_write_eol
                    .clone()
                    .unwrap_or_else(|| snapshot.config.settings.write_eol.clone()),
            ),
            echo: Some(
                snapshot
                    .effective_echo
                    .unwrap_or(snapshot.config.settings.echo),
            ),
            write_chunk_size: Some(pacing.chunk_size),
            write_chunk_delay_ms: Some(pacing.chunk_delay_ms),
        },
    }
}

fn slot_settings_form(ui: &mut egui::Ui, editor: &mut SlotSettingsDraft, ports: &[PortDescriptor]) {
    egui::Grid::new(("slot-settings-form", &editor.slot_id))
        .num_columns(2)
        .spacing([18.0, 8.0])
        .show(ui, |ui| {
            ui.label("串口");
            ui.horizontal(|ui| {
                ui.text_edit_singleline(&mut editor.port);
                egui::ComboBox::from_id_salt(("slot-settings-port", &editor.slot_id))
                    .selected_text("已发现串口")
                    .show_ui(ui, |ui| {
                        for port in ports {
                            ui.selectable_value(&mut editor.port, port.name.clone(), &port.name);
                        }
                    });
            });
            ui.end_row();

            ui.label("波特率");
            ui.add(
                egui::DragValue::new(&mut editor.transport.baud_rate)
                    .range(1..=10_000_000)
                    .speed(100.0),
            );
            ui.end_row();

            ui.label("数据位");
            enum_combo(
                ui,
                ("data-bits", &editor.slot_id),
                &mut editor.transport.data_bits,
                &[
                    (DataBits::Five, "5"),
                    (DataBits::Six, "6"),
                    (DataBits::Seven, "7"),
                    (DataBits::Eight, "8"),
                ],
            );
            ui.end_row();

            ui.label("校验");
            enum_combo(
                ui,
                ("parity", &editor.slot_id),
                &mut editor.transport.parity,
                &[
                    (Parity::None, "无"),
                    (Parity::Odd, "奇校验"),
                    (Parity::Even, "偶校验"),
                ],
            );
            ui.end_row();

            ui.label("停止位");
            enum_combo(
                ui,
                ("stop-bits", &editor.slot_id),
                &mut editor.transport.stop_bits,
                &[(StopBits::One, "1"), (StopBits::Two, "2")],
            );
            ui.end_row();

            ui.label("流控");
            enum_combo(
                ui,
                ("flow-control", &editor.slot_id),
                &mut editor.transport.flow_control,
                &[
                    (FlowControl::None, "无"),
                    (FlowControl::Software, "软件"),
                    (FlowControl::Hardware, "硬件"),
                ],
            );
            ui.end_row();

            ui.label("控制线 / 自动打开");
            ui.horizontal(|ui| {
                ui.checkbox(&mut editor.transport.dtr, "DTR");
                ui.checkbox(&mut editor.transport.rts, "RTS");
                ui.checkbox(&mut editor.transport.auto_open, "auto-open");
            });
            ui.end_row();

            ui.label("Shell 提示符");
            ui.text_edit_singleline(editor.device.shell_prompt.get_or_insert_with(String::new));
            ui.end_row();

            ui.label("U-Boot 提示符");
            ui.text_edit_singleline(editor.device.uboot_prompt.get_or_insert_with(String::new));
            ui.end_row();

            ui.label("发送换行符");
            let write_eol = editor.device.write_eol.get_or_insert_with(|| "\r".into());
            egui::ComboBox::from_id_salt(("write-eol", &editor.slot_id))
                .selected_text(visible_eol(write_eol))
                .show_ui(ui, |ui| {
                    for (value, label) in
                        [("", "无"), ("\r", "\\r"), ("\n", "\\n"), ("\r\n", "\\r\\n")]
                    {
                        ui.selectable_value(write_eol, value.to_string(), label);
                    }
                });
            ui.end_row();

            ui.label("回显");
            enum_combo(
                ui,
                ("echo", &editor.slot_id),
                editor.device.echo.get_or_insert(EchoMode::Auto),
                &[
                    (EchoMode::Auto, "自动"),
                    (EchoMode::On, "开启"),
                    (EchoMode::Off, "关闭"),
                ],
            );
            ui.end_row();

            ui.label("写入节奏");
            ui.horizontal(|ui| {
                ui.add(
                    egui::DragValue::new(editor.device.write_chunk_size.get_or_insert(1))
                        .range(1..=1_048_576),
                );
                ui.weak("bytes / chunk，间隔");
                ui.add(
                    egui::DragValue::new(editor.device.write_chunk_delay_ms.get_or_insert(1))
                        .range(0..=60_000),
                );
                ui.weak("ms");
            });
            ui.end_row();
        });
}

fn enum_combo<T: Copy + PartialEq>(
    ui: &mut egui::Ui,
    id: impl std::hash::Hash,
    selected: &mut T,
    choices: &[(T, &'static str)],
) {
    let label = choices
        .iter()
        .find(|(value, _)| value == selected)
        .map_or("未知", |(_, label)| *label);
    egui::ComboBox::from_id_salt(id)
        .selected_text(label)
        .show_ui(ui, |ui| {
            for (value, label) in choices {
                ui.selectable_value(selected, *value, *label);
            }
        });
}

fn optional_text(value: Option<&str>) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or("无")
        .to_string()
}

fn visible_eol(value: &str) -> String {
    if value.is_empty() {
        return "无".into();
    }
    value
        .chars()
        .map(|character| match character {
            '\r' => "\\r".into(),
            '\n' => "\\n".into(),
            '\t' => "\\t".into(),
            character if character.is_control() => format!("\\u{{{:04X}}}", u32::from(character)),
            character => character.to_string(),
        })
        .collect()
}

fn model_choice_label(models: &[DeviceModel], selected: &str) -> String {
    if selected.is_empty() {
        return "未绑定".into();
    }
    models
        .iter()
        .find(|model| model.id == selected)
        .map_or_else(
            || selected.to_string(),
            |model| format!("{} · {}", model.name, model.id),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serial_session_labels_are_localized() {
        assert_eq!(session_label(SessionState::Online), "在线");
        assert_eq!(session_label(SessionState::Disabled), "已关闭");
    }

    #[test]
    fn local_state_text_contains_the_managed_pid() {
        let state = LocalServiceState::Running {
            pid: 42,
            program: PathBuf::from("serial"),
        };
        assert!(local_state_label(&state).contains("42"));
    }

    #[test]
    fn line_endings_are_visible_in_profile_details() {
        assert_eq!(visible_eol("\r\n"), "\\r\\n");
        assert_eq!(optional_text(None), "无");
    }

    #[test]
    fn command_navigation_uses_exact_or_nearest_retained_sequence() {
        let row = |seq| crate::model::ConsoleRow {
            seq,
            direction: serial_protocol::Direction::Rx,
            time: String::new(),
            source: String::new(),
            text: String::new(),
            replay: false,
        };
        let rows = vec![row(10), row(20), row(30)];

        assert_eq!(nearest_console_row_index(&rows, 20), Some(1));
        assert_eq!(nearest_console_row_index(&rows, 21), Some(2));
        assert_eq!(nearest_console_row_index(&rows, 99), Some(2));
        assert_eq!(nearest_console_row_index(&[], 1), None);
    }
}
