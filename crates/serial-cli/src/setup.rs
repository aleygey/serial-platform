//! First-use setup: enumerate only, edit a private draft, then commit under the
//! daemon instance lock. Existing unrelated ports/profiles are never replaced.
use super::*;
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

use serial_setup::{PortChoice, port_picker};
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

fn configure_port(config: &mut DaemonConfig, port: &str) -> Result<(), String> {
    let mut draft = serial_setup::Draft {
        ports: config.ports.clone(),
        transport_profiles: config.transport_profiles.clone(),
        model_profiles: config.model_profiles.clone(),
        model_families: config.model_families.clone(),
    };
    draft.configure_port(port, &[])?;
    config.ports = draft.ports;
    config.transport_profiles = draft.transport_profiles;
    config.model_profiles = draft.model_profiles;
    config.model_families = draft.model_families;
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
    use serial_protocol::SlotConfig;
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
