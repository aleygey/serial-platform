//! The same private configuration draft is used by online and offline setup.
//! No serial I/O or persistence takes place in this wizard.
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute, terminal,
};
use serial_protocol::PortDescriptor;
use serial_protocol::{
    DataBits, EchoMode, FlowControl, ModelFamily, ModelProfile, Parity, SlotConfig, StopBits,
    TransportProfile,
};
use std::collections::BTreeSet;
use std::io::{self, IsTerminal, Write};
use unicode_width::UnicodeWidthChar;

#[derive(Clone, Debug, Default)]
pub struct Draft {
    pub ports: Vec<SlotConfig>,
    pub transport_profiles: Vec<TransportProfile>,
    pub model_profiles: Vec<ModelProfile>,
    pub model_families: Vec<ModelFamily>,
}

pub enum PortChoice {
    Selected(Vec<String>),
    Refresh,
    Manual,
    Later,
    Cancel,
}
pub fn port_picker(
    ports: &[PortDescriptor],
    configured: &[SlotConfig],
    selected: &mut BTreeSet<String>,
) -> Result<PortChoice, String> {
    terminal::enable_raw_mode().map_err(|e| e.to_string())?;
    let _guard = Screen;
    execute!(
        std::io::stdout(),
        terminal::EnterAlternateScreen,
        cursor::Hide
    )
    .map_err(|e| e.to_string())?;
    let mut index = 0usize;
    loop {
        let (width, height) = terminal::size().map_err(|e| e.to_string())?;
        execute!(
            std::io::stdout(),
            cursor::MoveTo(0, 0),
            terminal::Clear(terminal::ClearType::All)
        )
        .map_err(|e| e.to_string())?;
        print!("步骤 1/3 · 选择串口（扫描不会打开串口或发送命令）\r\n\r\n");
        let rows = usize::from(height.saturating_sub(7)).max(1);
        let start = index.saturating_sub(rows - 1);
        for (i, port) in ports.iter().enumerate().skip(start).take(rows) {
            let detail = format!(
                "{} [{}] {} · {}{}{}",
                if i == index { "›" } else { " " },
                if selected.contains(&port.name) {
                    "x"
                } else {
                    " "
                },
                port.name,
                port.product
                    .as_deref()
                    .or(port.manufacturer.as_deref())
                    .unwrap_or(&port.port_type),
                port.serial_number
                    .as_ref()
                    .map(|s| format!(" · {s}"))
                    .unwrap_or_default(),
                if configured.iter().any(|p| p.port == port.name) {
                    " · 已配置"
                } else {
                    ""
                }
            );
            // Limit by cells, not bytes, without exposing device-provided controls.
            let mut col = 0;
            for c in safe(&detail).chars() {
                let cells = if c.is_ascii() { 1 } else { 2 };
                if col + cells > width.saturating_sub(1) {
                    break;
                }
                print!("{c}");
                col += cells;
            }
            print!("\r\n");
        }
        if ports.is_empty() {
            print!("未发现串口：请检查 USB 连接及驱动，然后刷新。\r\n");
        }
        print!(
            "\r\n↑↓ 选择 · 空格 多选 · Enter 确认 · R/F5 刷新\r\nM 手动输入 · L 稍后配置 · Esc/Q 取消\r\n"
        );
        std::io::stdout().flush().map_err(|e| e.to_string())?;
        let Event::Key(key) = event::read().map_err(|e| e.to_string())? else {
            continue;
        };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        match key.code {
            KeyCode::Esc | KeyCode::Char('q' | 'Q') => return Ok(PortChoice::Cancel),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Ok(PortChoice::Cancel);
            }
            KeyCode::Up => index = index.saturating_sub(1),
            KeyCode::Down => index = (index + 1).min(ports.len().saturating_sub(1)),
            KeyCode::Char(' ') => {
                if let Some(port) = ports.get(index)
                    && !selected.remove(&port.name)
                {
                    selected.insert(port.name.clone());
                }
            }
            KeyCode::Enter if !selected.is_empty() => {
                return Ok(PortChoice::Selected(selected.iter().cloned().collect()));
            }
            KeyCode::Char('r' | 'R') | KeyCode::F(5) => return Ok(PortChoice::Refresh),
            KeyCode::Char('m' | 'M') => return Ok(PortChoice::Manual),
            KeyCode::Char('l' | 'L') => return Ok(PortChoice::Later),
            _ => {}
        }
    }
}

fn safe(text: &str) -> String {
    text.chars().filter(|c| !c.is_control()).collect()
}
pub fn ask(label: &str, default: &str) -> Result<String, String> {
    print!("{} [{}]：", safe(label), safe(default));
    io::stdout().flush().map_err(|e| e.to_string())?;
    let mut input = String::new();
    if io::stdin()
        .read_line(&mut input)
        .map_err(|e| e.to_string())?
        == 0
    {
        return Err("已取消，未保存配置".into());
    }
    let input = input.trim_end_matches(['\r', '\n']);
    if input == "q" {
        return Err("已取消，未保存配置".into());
    }
    Ok(if input.is_empty() {
        default.into()
    } else {
        input.into()
    })
}

struct Screen;
impl Drop for Screen {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), cursor::Show, terminal::LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

/// Arrows + Enter in a terminal; a numbered fallback also supports redirected
/// input. Merely moving the highlight never changes the draft.
pub fn choose(title: &str, choices: &[String], default: usize) -> Result<usize, String> {
    if choices.is_empty() {
        return Err("没有可选项".into());
    }
    let mut selected = default.min(choices.len() - 1);
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        println!("{}", safe(title));
        for (index, choice) in choices.iter().enumerate() {
            println!("  {}. {}", index + 1, safe(choice));
        }
        loop {
            if let Ok(value) =
                ask("选择编号（q 取消）", &(selected + 1).to_string())?.parse::<usize>()
                && (1..=choices.len()).contains(&value)
            {
                return Ok(value - 1);
            }
            println!("请选择 1–{}", choices.len());
        }
    }
    terminal::enable_raw_mode().map_err(|e| e.to_string())?;
    let _screen = Screen;
    execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Hide)
        .map_err(|e| e.to_string())?;
    loop {
        let (width, height) = terminal::size().map_err(|e| e.to_string())?;
        execute!(
            io::stdout(),
            cursor::MoveTo(0, 0),
            terminal::Clear(terminal::ClearType::All)
        )
        .map_err(|e| e.to_string())?;
        let clip = |value: &str| {
            let mut columns = 0usize;
            safe(value)
                .chars()
                .take_while(|character| {
                    columns += character.width().unwrap_or(0);
                    columns <= usize::from(width.saturating_sub(2))
                })
                .collect::<String>()
        };
        print!("{}\r\n\r\n", clip(title));
        let rows = usize::from(height.saturating_sub(6)).max(1);
        let start = selected.saturating_sub(rows - 1);
        for (index, choice) in choices.iter().enumerate().skip(start).take(rows) {
            print!(
                "{} {}\r\n",
                if index == selected { "›" } else { " " },
                clip(choice)
            );
        }
        print!("\r\n↑↓ 选择 · Enter 确认 · Esc/Q 取消（尚未保存）\r\n");
        io::stdout().flush().map_err(|e| e.to_string())?;
        let Event::Key(key) = event::read().map_err(|e| e.to_string())? else {
            continue;
        };
        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
            continue;
        }
        match key.code {
            KeyCode::Up => selected = selected.saturating_sub(1),
            KeyCode::Down => selected = (selected + 1).min(choices.len() - 1),
            KeyCode::Home => selected = 0,
            KeyCode::End => selected = choices.len() - 1,
            KeyCode::Enter => return Ok(selected),
            KeyCode::Esc | KeyCode::Char('q' | 'Q') => return Err("已取消，未保存配置".into()),
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                return Err("已取消，未保存配置".into());
            }
            _ => {}
        }
    }
}

fn number(label: &str, current: u32, min: u32, max: u32) -> Result<u32, String> {
    loop {
        if let Ok(value) = ask(label, &current.to_string())?.parse::<u32>()
            && (min..=max).contains(&value)
        {
            return Ok(value);
        }
        println!("请输入 {min}–{max}，q 取消。");
    }
}

fn prompt(
    label: &str,
    current: Option<&str>,
    observed: &[String],
    example: &str,
) -> Result<Option<String>, String> {
    let mut candidates = Vec::new();
    for value in observed {
        if !value.is_empty()
            && value.len() <= 256
            && !value.chars().any(char::is_control)
            && !candidates.contains(value)
        {
            candidates.push(value.clone());
        }
        if candidates.len() == 12 {
            break;
        }
    }
    let mut choices = vec![format!("保留当前值：{}", current.unwrap_or("尚未配置"))];
    choices.extend(
        candidates
            .iter()
            .map(|value| format!("已接收输出候选：{value}（请确认）")),
    );
    choices.push(format!("手动填写（例如 {example}）"));
    choices.push("稍后配置 / 清除此提示符".into());
    let selected = choose(label, &choices, 0)?;
    if selected == 0 {
        return Ok(current.map(str::to_owned));
    }
    if selected <= candidates.len() {
        return Ok(Some(candidates[selected - 1].clone()));
    }
    if selected == candidates.len() + 1 {
        loop {
            let value = ask(label, current.unwrap_or(""))?;
            if !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control) {
                return Ok(Some(value));
            }
            println!("请输入实际命令提示符，包含末尾空格；不确定时可取消并选择稍后配置。");
        }
    }
    Ok(None)
}

fn unique(prefix: &str, names: impl Iterator<Item = String>) -> String {
    let names = names.collect::<Vec<_>>();
    if !names.iter().any(|name| name == prefix) {
        return prefix.into();
    }
    (2..)
        .map(|index| format!("{prefix}-{index}"))
        .find(|name| !names.contains(name))
        .unwrap()
}

impl Draft {
    pub fn configure_port(
        &mut self,
        port: &str,
        observed_prompts: &[String],
    ) -> Result<(), String> {
        let existing = self.ports.iter().find(|slot| slot.port == port).cloned();
        let mut transport = existing
            .as_ref()
            .and_then(|slot| slot.transport_profile.as_ref())
            .and_then(|name| {
                self.transport_profiles
                    .iter()
                    .find(|profile| &profile.name == name)
            })
            .cloned()
            .unwrap_or(TransportProfile {
                name: String::new(),
                baud_rate: 115200,
                data_bits: DataBits::Eight,
                parity: Parity::None,
                stop_bits: StopBits::One,
                flow_control: FlowControl::None,
                dtr: false,
                rts: false,
                auto_open: true,
            });
        let choices = vec![
            format!("保留当前串口参数（{}）", transport.baud_rate),
            "115200 · 8N1 · 无流控".into(),
            "9600 · 8N1 · 无流控".into(),
            "921600 · 8N1 · 无流控".into(),
            "自定义串口参数".into(),
        ];
        let uart = choose(&format!("{port} · 1/3 串口参数"), &choices, 0)?;
        match uart {
            1..=3 => {
                transport.baud_rate = [115200, 9600, 921600][uart - 1];
                transport.data_bits = DataBits::Eight;
                transport.parity = Parity::None;
                transport.stop_bits = StopBits::One;
                transport.flow_control = FlowControl::None;
                // DTR/RTS remain unchanged: a baud preset must not reset a DUT.
            }
            4 => {
                transport.baud_rate = number("波特率", transport.baud_rate, 1, 4_000_000)?;
                transport.data_bits = [
                    DataBits::Five,
                    DataBits::Six,
                    DataBits::Seven,
                    DataBits::Eight,
                ][choose(
                    "数据位",
                    &["5".into(), "6".into(), "7".into(), "8".into()],
                    match transport.data_bits {
                        DataBits::Five => 0,
                        DataBits::Six => 1,
                        DataBits::Seven => 2,
                        DataBits::Eight => 3,
                    },
                )?];
                transport.parity = [Parity::None, Parity::Odd, Parity::Even][choose(
                    "校验位",
                    &["无".into(), "奇校验".into(), "偶校验".into()],
                    match transport.parity {
                        Parity::None => 0,
                        Parity::Odd => 1,
                        Parity::Even => 2,
                    },
                )?];
                transport.stop_bits = [StopBits::One, StopBits::Two][choose(
                    "停止位",
                    &["1".into(), "2".into()],
                    usize::from(transport.stop_bits == StopBits::Two),
                )?];
                transport.flow_control = [
                    FlowControl::None,
                    FlowControl::Software,
                    FlowControl::Hardware,
                ][choose(
                    "流控",
                    &["无".into(), "软件".into(), "硬件".into()],
                    match transport.flow_control {
                        FlowControl::None => 0,
                        FlowControl::Software => 1,
                        FlowControl::Hardware => 2,
                    },
                )?];
                for (name, value) in [("DTR", &mut transport.dtr), ("RTS", &mut transport.rts)] {
                    *value = choose(
                        &format!("{name}（修改电平可能影响设备）"),
                        &["低".into(), "高".into()],
                        usize::from(*value),
                    )? == 1;
                }
            }
            _ => {}
        }

        let current_identity = existing
            .as_ref()
            .and_then(|slot| slot.model_family.as_ref().zip(slot.model_name.as_ref()))
            .map(|(family, model)| (family.clone(), model.clone()));
        let identities = self
            .model_families
            .iter()
            .flat_map(|family| {
                family
                    .model_names
                    .iter()
                    .map(|model| (family.name.clone(), model.clone()))
            })
            .collect::<Vec<_>>();
        let mut choices = vec![format!(
            "保留当前机型：{}",
            current_identity
                .as_ref()
                .map_or("未设置".into(), |(family, model)| format!(
                    "{family} / {model}"
                ))
        )];
        choices.extend(
            identities
                .iter()
                .map(|(family, model)| format!("{family} / {model}")),
        );
        choices.extend([
            "新增机型（一级系列 / 二级型号）".into(),
            "不设置机型身份".into(),
        ]);
        let selected = choose(
            &format!("{port} · 2/3 机型名（不改变串口或交互参数）"),
            &choices,
            0,
        )?;
        let identity = if selected == 0 {
            current_identity
        } else if selected <= identities.len() {
            Some(identities[selected - 1].clone())
        } else if selected == identities.len() + 1 {
            let family = required("一级机型系列")?;
            let model = required("二级具体型号")?;
            Some((family, model))
        } else {
            None
        };

        let mut model = existing
            .as_ref()
            .and_then(|slot| slot.model_profile.as_ref())
            .and_then(|name| {
                self.model_profiles
                    .iter()
                    .find(|profile| &profile.name == name)
            })
            .cloned()
            .unwrap_or(ModelProfile {
                name: String::new(),
                shell_prompt: None,
                uboot_prompt: None,
                write_eol: Some("\r".into()),
                echo: Some(EchoMode::Auto),
                write_chunk_size: Some(1),
                write_chunk_delay_ms: Some(1),
            });
        let mut choices = vec![format!(
            "保留当前交互配置：{}",
            if model.name.is_empty() {
                "通用（提示符稍后配置）"
            } else {
                &model.name
            }
        )];
        choices.extend(self.model_profiles.iter().map(|profile| {
            format!(
                "使用 {} · Shell {} · U-Boot {}",
                profile.name,
                profile.shell_prompt.as_deref().unwrap_or("未设置"),
                profile.uboot_prompt.as_deref().unwrap_or("未设置")
            )
        }));
        choices.push("逐项配置命令提示符、换行与发送节奏".into());
        let selected = choose(
            &format!("{port} · 3/3 设备交互（不要求理解 Profile）"),
            &choices,
            0,
        )?;
        if selected > 0 && selected <= self.model_profiles.len() {
            model = self.model_profiles[selected - 1].clone();
        } else if selected > self.model_profiles.len() {
            model.shell_prompt = prompt(
                "Shell 命令提示符",
                model.shell_prompt.as_deref(),
                observed_prompts,
                "root@device:~# ",
            )?;
            model.uboot_prompt = prompt(
                "U-Boot 命令提示符",
                model.uboot_prompt.as_deref(),
                observed_prompts,
                "=> ",
            )?;
            let index = match model.write_eol.as_deref() {
                Some("\n") => 1,
                Some("\r\n") => 2,
                _ => 0,
            };
            model.write_eol = Some(
                ["\r", "\n", "\r\n"][choose(
                    "命令行结束符 EOL",
                    &["CR（常用）".into(), "LF".into(), "CRLF".into()],
                    index,
                )?]
                .into(),
            );
            if choose(
                "发送节奏",
                &[
                    format!(
                        "保留：每次 {} 字节，间隔 {} ms",
                        model.write_chunk_size.unwrap_or(1),
                        model.write_chunk_delay_ms.unwrap_or(1)
                    ),
                    "自定义".into(),
                ],
                0,
            )? == 1
            {
                model.write_chunk_size = Some(number(
                    "每次发送字节数",
                    model.write_chunk_size.unwrap_or(1),
                    1,
                    4096,
                )?);
                model.write_chunk_delay_ms = Some(u64::from(number(
                    "块间延时（ms）",
                    model.write_chunk_delay_ms.unwrap_or(1).min(1000) as u32,
                    0,
                    1000,
                )?));
            }
        }
        self.save_port_draft(port, existing, transport, model, identity);
        Ok(())
    }

    fn save_port_draft(
        &mut self,
        port: &str,
        existing: Option<SlotConfig>,
        mut transport: TransportProfile,
        mut model: ModelProfile,
        identity: Option<(String, String)>,
    ) {
        // Editing one port creates/reuses a value-equivalent profile. It never
        // overwrites a shared profile and never changes an unrelated port.
        let transport_name = self
            .transport_profiles
            .iter()
            .find(|profile| {
                let mut value = (*profile).clone();
                value.name = transport.name.clone();
                value == transport
            })
            .map(|profile| profile.name.clone())
            .unwrap_or_else(|| {
                transport.name = unique(
                    &format!("uart-{}", transport.baud_rate),
                    self.transport_profiles
                        .iter()
                        .map(|profile| profile.name.clone()),
                );
                self.transport_profiles.push(transport.clone());
                transport.name.clone()
            });
        let model_name = self
            .model_profiles
            .iter()
            .find(|profile| {
                let mut value = (*profile).clone();
                value.name = model.name.clone();
                value == model
            })
            .map(|profile| profile.name.clone())
            .unwrap_or_else(|| {
                model.name = unique(
                    "serial-input",
                    self.model_profiles
                        .iter()
                        .map(|profile| profile.name.clone()),
                );
                self.model_profiles.push(model.clone());
                model.name.clone()
            });
        if let Some((family, model)) = &identity {
            if let Some(entry) = self
                .model_families
                .iter_mut()
                .find(|entry| &entry.name == family)
            {
                if !entry.model_names.contains(model) {
                    entry.model_names.push(model.clone());
                }
            } else {
                self.model_families.push(ModelFamily {
                    name: family.clone(),
                    model_names: vec![model.clone()],
                });
            }
        }
        let slot = SlotConfig {
            port: port.into(),
            transport_profile: Some(transport_name),
            model_profile: Some(model_name),
            model_family: identity.as_ref().map(|value| value.0.clone()),
            model_name: identity.map(|value| value.1),
            enabled: existing.is_none_or(|slot| slot.enabled),
        };
        if let Some(value) = self.ports.iter_mut().find(|slot| slot.port == port) {
            *value = slot;
        } else {
            self.ports.push(slot);
        }
    }
}

fn required(label: &str) -> Result<String, String> {
    loop {
        let value = ask(label, "")?;
        if !value.trim().is_empty() && value == value.trim() && !value.chars().any(char::is_control)
        {
            return Ok(value);
        }
        println!("请输入非空名称，不要包含首尾空格；q 取消。");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_identity_change_reuses_behavior_and_does_not_change_other_ports() {
        let transport = TransportProfile {
            name: "shared-uart".into(),
            baud_rate: 921600,
            data_bits: DataBits::Eight,
            parity: Parity::Even,
            stop_bits: StopBits::Two,
            flow_control: FlowControl::Hardware,
            dtr: true,
            rts: false,
            auto_open: false,
        };
        let model = ModelProfile {
            name: "shared-behavior".into(),
            shell_prompt: Some("dut# ".into()),
            uboot_prompt: Some("=> ".into()),
            write_eol: Some("\r\n".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(2),
            write_chunk_delay_ms: Some(3),
        };
        let slot = SlotConfig {
            port: "COM3".into(),
            transport_profile: Some(transport.name.clone()),
            model_profile: Some(model.name.clone()),
            model_family: Some("family".into()),
            model_name: Some("first".into()),
            enabled: false,
        };
        let other = SlotConfig {
            port: "COM4".into(),
            ..slot.clone()
        };
        let mut draft = Draft {
            ports: vec![slot.clone(), other.clone()],
            transport_profiles: vec![transport.clone()],
            model_profiles: vec![model.clone()],
            model_families: vec![],
        };
        draft.save_port_draft(
            "COM3",
            Some(slot),
            transport.clone(),
            model.clone(),
            Some(("family".into(), "second".into())),
        );
        assert_eq!(draft.ports[1], other);
        assert!(!draft.ports[0].enabled);
        assert_eq!(draft.transport_profiles, vec![transport]);
        assert_eq!(draft.model_profiles, vec![model]);
        assert_eq!(draft.ports[0].model_name.as_deref(), Some("second"));
    }
    #[test]
    fn changed_shared_profile_is_cloned_instead_of_overwritten() {
        let transport = TransportProfile {
            name: "uart".into(),
            baud_rate: 115200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        };
        let model = ModelProfile {
            name: "serial-input".into(),
            shell_prompt: Some("old# ".into()),
            uboot_prompt: None,
            write_eol: None,
            echo: None,
            write_chunk_size: None,
            write_chunk_delay_ms: None,
        };
        let mut draft = Draft {
            transport_profiles: vec![transport.clone()],
            model_profiles: vec![model.clone()],
            ..Default::default()
        };
        let mut changed = model.clone();
        changed.shell_prompt = Some("new# ".into());
        draft.save_port_draft("COM3", None, transport, changed, None);
        assert_eq!(draft.model_profiles[0], model);
        assert_eq!(draft.model_profiles.len(), 2);
        assert_ne!(
            draft.ports[0].model_profile.as_deref(),
            Some("serial-input")
        );
    }
}
