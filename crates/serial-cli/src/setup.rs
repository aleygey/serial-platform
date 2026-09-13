//! First-use setup: enumerate only, edit a private draft, then commit under the
//! daemon instance lock. Existing unrelated ports/profiles are never replaced.
use super::*;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{self, ClearType},
};
use serial_protocol::PortDescriptor;
use std::collections::BTreeSet;

fn safe(value: &str) -> String {
    value.chars().filter(|c| !c.is_control()).collect()
}

fn ask(label: &str, default: &str) -> Result<String, String> {
    print!("{label} [{}]：", safe(default));
    std::io::stdout().flush().map_err(|e| e.to_string())?;
    let mut input = String::new();
    if std::io::stdin()
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
fn number(label: &str, default: u32, min: u32, max: u32) -> Result<u32, String> {
    loop {
        if let Ok(value) = ask(label, &default.to_string())?.parse::<u32>()
            && (min..=max).contains(&value)
        {
            return Ok(value);
        }
        println!("请输入 {min}–{max} 范围内的整数；q 取消。");
    }
}
fn optional(label: &str, current: Option<&str>) -> Result<Option<String>, String> {
    let value = ask(label, current.unwrap_or(""))?;
    Ok((!value.is_empty() && value != "-").then_some(value))
}

fn parse_ports(text: &str, ports: &[PortDescriptor]) -> Result<Vec<String>, String> {
    let mut selected = BTreeSet::new();
    for item in text.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        let name = match item.parse::<usize>() {
            Ok(index) => ports
                .get(index.wrapping_sub(1))
                .ok_or("串口编号超出范围")?
                .name
                .clone(),
            Err(_) => item.into(),
        };
        if name.chars().any(char::is_control) {
            return Err("串口名含控制字符".into());
        }
        selected.insert(name);
    }
    Ok(selected.into_iter().collect())
}

enum PortChoice {
    Selected(Vec<String>),
    Refresh,
    Manual,
    Later,
    Cancel,
}
struct TerminalGuard;
impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(
            std::io::stdout(),
            cursor::Show,
            terminal::LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}
fn port_picker(
    ports: &[PortDescriptor],
    configured: &[SlotConfig],
    selected: &mut BTreeSet<String>,
) -> Result<PortChoice, String> {
    terminal::enable_raw_mode().map_err(|e| e.to_string())?;
    let _guard = TerminalGuard;
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
            terminal::Clear(ClearType::All)
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
fn choose_ports(config: &DaemonConfig) -> Result<Vec<String>, String> {
    let mut selected = BTreeSet::new();
    let mut first = true;
    loop {
        let ports = match seriald::enumerate_ports() {
            Ok(ports) => ports,
            Err(e) => {
                println!("扫描失败：{e}；可刷新或手动输入。");
                Vec::new()
            }
        };
        if first {
            selected.extend(
                ports
                    .iter()
                    .filter(|p| config.ports.iter().any(|s| s.port == p.name))
                    .map(|p| p.name.clone()),
            );
            if ports.len() == 1 && selected.is_empty() {
                selected.insert(ports[0].name.clone());
            }
            first = false;
        }
        let choice = if std::io::stdin().is_terminal() && std::io::stdout().is_terminal() {
            port_picker(&ports, &config.ports, &mut selected)?
        } else {
            for (i, port) in ports.iter().enumerate() {
                println!(
                    "{}. {} {}",
                    i + 1,
                    safe(&port.name),
                    safe(port.product.as_deref().unwrap_or(""))
                );
            }
            let text = ask(
                "串口编号/名称（逗号多选；r 刷新；l 稍后；q 取消）",
                &selected.iter().cloned().collect::<Vec<_>>().join(","),
            )?;
            match text.as_str() {
                "r" => PortChoice::Refresh,
                "l" => PortChoice::Later,
                _ => match parse_ports(&text, &ports) {
                    Ok(v) => PortChoice::Selected(v),
                    Err(e) => {
                        println!("{e}");
                        continue;
                    }
                },
            }
        };
        match choice {
            PortChoice::Selected(ports) => return Ok(ports),
            PortChoice::Later => return Ok(Vec::new()),
            PortChoice::Cancel => return Err("已取消，未保存配置".into()),
            PortChoice::Refresh => {}
            PortChoice::Manual => {
                let text = ask("串口名称（逗号多选，例如 COM4 或 /dev/ttyUSB0）", "")?;
                match parse_ports(&text, &[]) {
                    Ok(v) if !v.is_empty() => return Ok(v),
                    Ok(_) => {}
                    Err(e) => println!("{e}"),
                }
            }
        }
    }
}

fn unique_name(prefix: &str, names: impl Iterator<Item = String>) -> String {
    let names = names.collect::<BTreeSet<_>>();
    if !names.contains(prefix) {
        return prefix.into();
    }
    (2..)
        .map(|n| format!("{prefix}-{n}"))
        .find(|n| !names.contains(n))
        .unwrap()
}
fn configure_port(config: &mut DaemonConfig, port: &str) -> Result<(), String> {
    println!("\n步骤 2/3 · {}（Enter 保留默认；q 取消）", safe(port));
    let existing = config.ports.iter().find(|p| p.port == port).cloned();
    let transport = existing
        .as_ref()
        .and_then(|p| p.transport_profile.as_ref())
        .and_then(|name| config.transport_profiles.iter().find(|p| &p.name == name))
        .cloned();
    let model = existing
        .as_ref()
        .and_then(|p| p.model_profile.as_ref())
        .and_then(|name| config.model_profiles.iter().find(|p| &p.name == name))
        .cloned();
    let mut transport = transport.unwrap_or(TransportProfile {
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
    transport.baud_rate = number(
        "波特率（常用 9600 / 115200 / 921600）",
        transport.baud_rate,
        1,
        4_000_000,
    )?;
    let mut model = model.unwrap_or(ModelProfile {
        name: String::new(),
        shell_prompt: None,
        uboot_prompt: None,
        write_eol: Some("\r".into()),
        echo: Some(EchoMode::Auto),
        write_chunk_size: Some(1),
        write_chunk_delay_ms: Some(1),
    });
    let eol_default = match model.write_eol.as_deref() {
        Some("\n") => "LF",
        Some("\r\n") => "CRLF",
        _ => "CR",
    };
    loop {
        let eol = ask("命令换行 CR / LF / CRLF", eol_default)?;
        model.write_eol = match eol.to_ascii_uppercase().as_str() {
            "CR" => Some("\r".into()),
            "LF" => Some("\n".into()),
            "CRLF" => Some("\r\n".into()),
            _ => {
                println!("请选择 CR、LF 或 CRLF");
                continue;
            }
        };
        break;
    }
    let advanced = ask(
        "调整高级串口参数？y/n（数据位、校验、流控、DTR/RTS、发送节奏）",
        "n",
    )? == "y";
    if advanced {
        transport.data_bits = match number(
            "数据位",
            match transport.data_bits {
                DataBits::Five => 5,
                DataBits::Six => 6,
                DataBits::Seven => 7,
                DataBits::Eight => 8,
            },
            5,
            8,
        )? {
            5 => DataBits::Five,
            6 => DataBits::Six,
            7 => DataBits::Seven,
            _ => DataBits::Eight,
        };
        transport.stop_bits = if number(
            "停止位",
            if transport.stop_bits == StopBits::Two {
                2
            } else {
                1
            },
            1,
            2,
        )? == 2
        {
            StopBits::Two
        } else {
            StopBits::One
        };
        transport.parity = match number(
            "校验 0无/1奇/2偶",
            match transport.parity {
                Parity::None => 0,
                Parity::Odd => 1,
                Parity::Even => 2,
            },
            0,
            2,
        )? {
            1 => Parity::Odd,
            2 => Parity::Even,
            _ => Parity::None,
        };
        transport.flow_control = match number(
            "流控 0无/1软件/2硬件",
            match transport.flow_control {
                FlowControl::None => 0,
                FlowControl::Software => 1,
                FlowControl::Hardware => 2,
            },
            0,
            2,
        )? {
            1 => FlowControl::Software,
            2 => FlowControl::Hardware,
            _ => FlowControl::None,
        };
        transport.dtr = number("DTR 电平 0低/1高", u32::from(transport.dtr), 0, 1)? == 1;
        transport.rts = number("RTS 电平 0低/1高", u32::from(transport.rts), 0, 1)? == 1;
        model.write_chunk_size = Some(number(
            "每次发送字节数",
            model.write_chunk_size.unwrap_or(1),
            1,
            4096,
        )?);
        model.write_chunk_delay_ms = Some(u64::from(number(
            "发送块间隔（毫秒）",
            model.write_chunk_delay_ms.unwrap_or(1).min(1000) as u32,
            0,
            1000,
        )?));
    }
    let mut family = existing.as_ref().and_then(|p| p.model_family.clone());
    let mut concrete = existing.as_ref().and_then(|p| p.model_name.clone());
    if ask("配置设备机型及提示符？y/n（可稍后在 TUI 配置）", "n")? == "y" {
        println!("现有机型（一级与二级在同一页）：");
        for entry in &config.model_families {
            println!(
                "  {} → {}",
                safe(&entry.name),
                safe(&entry.model_names.join(" / "))
            );
        }
        family = optional("一级机型系列（- 清除身份）", family.as_deref())?;
        concrete = if family.is_some() {
            loop {
                let name = optional("二级具体型号", concrete.as_deref())?;
                if name.is_some() {
                    break name;
                }
                println!("选择一级系列后，请填写二级型号。");
            }
        } else {
            None
        };
        if let (Some(family), Some(name)) = (&family, &concrete) {
            if let Some(entry) = config.model_families.iter_mut().find(|f| &f.name == family) {
                if !entry.model_names.contains(name) {
                    entry.model_names.push(name.clone());
                }
            } else {
                config.model_families.push(ModelFamily {
                    name: family.clone(),
                    model_names: vec![name.clone()],
                });
            }
        }
        model.shell_prompt = optional(
            "Shell 提示符（可留空；- 清除）",
            model.shell_prompt.as_deref(),
        )?;
        model.uboot_prompt = optional(
            "U-Boot 提示符（可留空；- 清除）",
            model.uboot_prompt.as_deref(),
        )?;
    }
    // Compare content without the generated name. Reuse equal profiles, clone
    // changed ones so editing one port cannot reconfigure unrelated devices.
    let transport_name = config
        .transport_profiles
        .iter()
        .find(|p| {
            let mut copy = (*p).clone();
            copy.name = transport.name.clone();
            copy == transport
        })
        .map(|p| p.name.clone())
        .unwrap_or_else(|| {
            transport.name = unique_name(
                &format!("uart-{}", transport.baud_rate),
                config.transport_profiles.iter().map(|p| p.name.clone()),
            );
            config.transport_profiles.push(transport.clone());
            transport.name.clone()
        });
    let model_name = config
        .model_profiles
        .iter()
        .find(|p| {
            let mut copy = (*p).clone();
            copy.name = model.name.clone();
            copy == model
        })
        .map(|p| p.name.clone())
        .unwrap_or_else(|| {
            model.name = unique_name(
                "serial-input",
                config.model_profiles.iter().map(|p| p.name.clone()),
            );
            config.model_profiles.push(model.clone());
            model.name.clone()
        });
    let slot = SlotConfig {
        port: port.into(),
        transport_profile: Some(transport_name),
        model_profile: Some(model_name),
        model_family: family,
        model_name: concrete,
        enabled: existing.as_ref().is_none_or(|p| p.enabled),
    };
    if let Some(current) = config.ports.iter_mut().find(|p| p.port == port) {
        *current = slot;
    } else {
        config.ports.push(slot);
    }
    Ok(())
}

pub(super) fn save_checked(
    store: &ConfigStore,
    base: Option<&DaemonConfig>,
    mut draft: DaemonConfig,
) -> Result<(), String> {
    if store.paths().config_file.exists() {
        let current = store.load().map_err(|e| e.to_string())?;
        if base.is_none_or(|base| {
            serde_json::to_value(base).ok() != serde_json::to_value(&current).ok()
        }) {
            return Err("配置已被其他操作修改；未覆盖，请重新打开 setup".into());
        }
    } else if base.is_some() {
        return Err("配置文件已变化；未保存".into());
    }
    draft.config_revision = draft
        .config_revision
        .checked_add(1)
        .ok_or("配置版本号已耗尽")?;
    store.save(&draft).map_err(|e| e.to_string())
}

pub(super) fn configure(store: &ConfigStore, offer_start: bool) -> Result<bool, String> {
    let _lock = seriald::runtime::ActiveInstance::acquire(store.paths())
        .map_err(|e| format!("无法进行离线配置：{e}。运行中的服务请使用 serialctl setup。"))?;
    let base = if store.paths().config_file.exists() {
        Some(store.load().map_err(|e| e.to_string())?)
    } else {
        None
    };
    let original = base.clone().unwrap_or_else(DaemonConfig::generate);
    let selected = choose_ports(&original)?;
    loop {
        let mut draft = original.clone();
        for port in &selected {
            configure_port(&mut draft, port)?;
        }
        if ask("配置服务监听地址？y/n（默认保留本机设置）", "n")? == "y" {
            println!("非回环地址会允许对应网络访问 seriald/MCP，请只在可信网络开启。");
            loop {
                match ask("监听 IP:端口", &draft.bind.to_string())?.parse::<SocketAddr>() {
                    Ok(bind) => {
                        draft.bind = bind;
                        break;
                    }
                    Err(_) => println!("地址格式应为 IP:端口，例如 127.0.0.1:3210"),
                }
            }
        }
        println!("\n步骤 3/3 · 确认配置（未选中的已有端口及配置保留）");
        println!(
            "seriald {} · MCP http://{}/mcp",
            draft.bind,
            SocketAddr::new(connect_address(draft.bind).ip(), 3211)
        );
        for port in &selected {
            let slot = draft.ports.iter().find(|p| &p.port == port).unwrap();
            let t = draft
                .transport_profiles
                .iter()
                .find(|p| Some(&p.name) == slot.transport_profile.as_ref())
                .unwrap();
            println!(
                "  {} · {} {:?}/{:?}/{:?} · {:?} · {}",
                safe(port),
                t.baud_rate,
                t.data_bits,
                t.parity,
                t.stop_bits,
                t.flow_control,
                safe(slot.model_name.as_deref().unwrap_or("机型稍后配置"))
            );
        }
        if let Err(e) = draft.validate() {
            println!("配置无效：{e}；请重新修改。");
            continue;
        }
        let action = ask(
            if offer_start {
                "1 保存并打开 / 2 仅保存 / r 返回修改 / q 取消"
            } else {
                "1 保存并继续启动 / r 返回修改 / q 取消"
            },
            "1",
        )?;
        if action == "r" {
            continue;
        }
        if action != "1" && !(offer_start && action == "2") {
            println!("请选择有效操作");
            continue;
        }
        save_checked(store, base.as_ref(), draft)?;
        println!("配置已保存。打开串口不会自动发送探测命令；串口在线但没有输出不代表连接失败。");
        if action == "2" {
            println!("稍后运行 serial 即可进入串口界面。");
        }
        return Ok(action == "1");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn numbered_manual_selection_is_deduplicated_and_invalid_indices_rejected() {
        let ports = vec![PortDescriptor {
            name: "COM4".into(),
            port_type: "usb".into(),
            manufacturer: None,
            product: None,
            serial_number: None,
        }];
        assert_eq!(
            parse_ports("1,COM4,COM9", &ports).unwrap(),
            vec!["COM4", "COM9"]
        );
        assert!(parse_ports("0", &ports).is_err());
        assert!(parse_ports("2", &ports).is_err());
    }
    #[test]
    fn save_preserves_unrelated_config_and_rejects_stale_drafts() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(dir.path()));
        let base = store.load_or_create().unwrap().config;
        let mut draft = base.clone();
        draft.ports.push(SlotConfig {
            port: "COM4".into(),
            transport_profile: None,
            model_profile: None,
            model_family: None,
            model_name: None,
            enabled: true,
        });
        save_checked(&store, Some(&base), draft).unwrap();
        assert!(save_checked(&store, Some(&base), base.clone()).is_err());
        let saved = store.load().unwrap();
        assert_eq!(saved.server_id, base.server_id);
        assert_eq!(saved.transport_profiles, base.transport_profiles);
        assert_eq!(saved.ports.len(), 1);
    }
    #[test]
    fn cancel_has_no_configuration_side_effects_and_active_backend_excludes_setup() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(dir.path()));
        let _lock = seriald::runtime::ActiveInstance::acquire(store.paths()).unwrap();
        assert!(seriald::runtime::ActiveInstance::acquire(store.paths()).is_err());
        assert!(!store.paths().config_file.exists());
    }
}
