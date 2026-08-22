use std::{
    env,
    ffi::{OsStr, OsString},
    io::{Error, ErrorKind, IsTerminal as _, Read as _, Write as _},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    path::PathBuf,
    process::{self, Child, Command, ExitStatus, Stdio},
    str::FromStr as _,
    thread,
    time::{Duration, Instant},
};

use serial_protocol::{
    DataBits, EchoMode, FlowControl, McpHealthResponse, ModelProfile, PROTOCOL_VERSION, Parity,
    SlotConfig, StopBits, TransportProfile,
};
use seriald::config::{ConfigPaths, ConfigStore, DaemonConfig};
use seriald::runtime::{ActiveEndpoint, connect_address, discover_active};

const HELP: &str = "\
Unified serial-platform command

Usage:
  serial                         Start backend, MCP HTTP, and TUI
  serial console [serialctl options]
  serial setup [--root DIR]      Configure offline; backend need not be running
  serial [--root DIR] serve [seriald options]
  serial profile <transport|model|attach|detach> ...
  serial status|doctor|archives|logs ...
  serial mcp [serial-mcp options]  Start stdio MCP for an MCP host
  serial mcp --dump-tools
  serial paths [seriald options]

The unified command manages sibling components from the same release package.
The default MCP endpoint is http://127.0.0.1:3211/mcp.
";

const SETUP_HELP: &str = "\
Offline setup; seriald does not need to be running.

Usage: serial setup [--root DIR]

后端地址：seriald 监听 IP 和端口
串口 Profile：波特率、数据位、校验位
机型 Profile：同系列设备共用的 Shell/U-Boot 提示符和输入方式
具体机型名：当前串口所连接设备的具体型号
";

const MCP_STARTUP_TIMEOUT: Duration = Duration::from_secs(8);
const MCP_IDENTITY_WAIT: Duration = Duration::from_secs(2);
const MCP_IDENTITY_POLL: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Component {
    Daemon,
    Console,
    Mcp,
}

#[derive(Debug, PartialEq, Eq)]
struct Dispatch {
    component: Component,
    args: Vec<OsString>,
}

fn main() {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|arg| arg == OsStr::new("--version") || arg == OsStr::new("-V"))
    {
        println!("serial {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if args
        .first()
        .is_some_and(|arg| arg == OsStr::new("--help") || arg == OsStr::new("-h"))
    {
        print!("{HELP}");
        return;
    }

    let (first, _) = dispatch_token(&args);
    if first == Some("setup") {
        if args
            .iter()
            .any(|arg| arg == OsStr::new("--help") || arg == OsStr::new("-h"))
        {
            print!("{SETUP_HELP}");
            return;
        }
        if let Err(error) = run_setup(&args) {
            eprintln!("serial setup: {error}");
            process::exit(1);
        }
        return;
    }
    if first.is_none() {
        match run_unified(&args) {
            Ok(status) => process::exit(status.code().unwrap_or(1)),
            Err(error) => {
                eprintln!("serial: {error}");
                process::exit(1);
            }
        }
    }

    let dispatch = dispatch(args);
    let program = component_program(dispatch.component);
    match run_component(dispatch.component, program, &dispatch.args) {
        Ok(status) => process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            eprintln!(
                "serial: cannot start {name}: {error}\n\
                 Keep all required executables from the same release package together.",
                name = program.to_string_lossy(),
            );
            process::exit(1);
        }
    }
}

fn dispatch(mut args: Vec<OsString>) -> Dispatch {
    let (first, daemon_root) = dispatch_token(&args);
    let component = match first {
        None if daemon_root => Component::Daemon,
        None => Component::Console,
        Some("serve" | "paths" | "discover") => Component::Daemon,
        Some("mcp") => Component::Mcp,
        Some("console" | "profile" | "status" | "doctor" | "archives" | "logs") => {
            Component::Console
        }
        Some(first) if daemon_root && first.starts_with('-') => Component::Daemon,
        Some(_) => Component::Console,
    };

    match first {
        // serialctl's default command is the interactive console.
        Some("console") => {
            args.remove(0);
        }
        // serial-mcp has no subcommand; `mcp` selects the component only.
        Some("mcp") => {
            args.remove(0);
        }
        _ => {}
    }

    Dispatch { component, args }
}

fn dispatch_token(args: &[OsString]) -> (Option<&str>, bool) {
    let mut index = 0usize;
    let mut daemon_root = false;
    while let Some(value) = args.get(index).and_then(|value| value.to_str()) {
        if value == "--root" {
            daemon_root = true;
            index = index.saturating_add(2);
            continue;
        }
        if value.starts_with("--root=") {
            daemon_root = true;
            index = index.saturating_add(1);
            continue;
        }
        return (Some(value), daemon_root);
    }
    (None, daemon_root)
}

fn component_program(component: Component) -> &'static OsStr {
    match component {
        Component::Daemon => OsStr::new(if cfg!(windows) {
            "seriald.exe"
        } else {
            "seriald"
        }),
        Component::Console => OsStr::new(if cfg!(windows) {
            "serialctl.exe"
        } else {
            "serialctl"
        }),
        Component::Mcp => OsStr::new(if cfg!(windows) {
            "serial-mcp.exe"
        } else {
            "serial-mcp"
        }),
    }
}

fn run_setup(args: &[OsString]) -> Result<(), String> {
    let store = config_store(args)?;
    configure_offline(&store, true)
}

fn run_unified(args: &[OsString]) -> std::io::Result<ExitStatus> {
    let root = resolved_root(args).map_err(Error::other)?;
    let store = config_store_for_root(root.as_deref()).map_err(Error::other)?;
    if !store.paths().config_file.exists() {
        configure_offline(&store, std::io::stdin().is_terminal()).map_err(Error::other)?;
    }
    let config = store
        .load()
        .map_err(|error| Error::other(error.to_string()))?;
    let discovered = discover_active(store.paths(), config.server_id)
        .map_err(|error| Error::other(error.to_string()))?;
    let (active, mut daemon) = if let Some(active) = discovered {
        (active, None)
    } else {
        let daemon_address = connect_address(config.bind);
        let mut command = Command::new(required_sibling(component_program(Component::Daemon))?);
        append_root_arg(&mut command, root.as_deref());
        configure_managed_child(&mut command);
        command
            .args(["serve", "--managed"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let child = command.spawn()?;
        let (daemon, active) = wait_for_child_endpoint(
            child,
            store.paths(),
            config.server_id,
            daemon_address,
            "seriald",
        )?;
        (active, daemon)
    };
    let endpoint = active.endpoint.clone();

    let mut mcp = None;
    let result = (|| {
        let mcp_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3211);
        if tcp_ready(mcp_address) {
            if wait_for_matching_mcp(mcp_address, &active, MCP_IDENTITY_WAIT).is_none() {
                return Err(Error::new(
                    ErrorKind::AddrInUse,
                    "127.0.0.1:3211 is occupied, but it is not protocol v5 serial-mcp connected to the selected seriald",
                ));
            }
        } else {
            let mut command = Command::new(required_sibling(component_program(Component::Mcp))?);
            configure_managed_child(&mut command);
            command
                .args([
                    "--endpoint",
                    &endpoint,
                    "--listen",
                    "127.0.0.1:3211",
                    "--managed",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            mcp = wait_for_mcp_child(command.spawn()?, mcp_address, &active)?;
        }

        println!("seriald {endpoint}  |  MCP http://127.0.0.1:3211/mcp");
        let console = required_sibling(component_program(Component::Console))?;
        Command::new(console)
            .env("SERIALD_ENDPOINT", &endpoint)
            .status()
    })();
    stop_child(&mut mcp);
    stop_child(&mut daemon);
    result
}

fn config_store(args: &[OsString]) -> Result<ConfigStore, String> {
    let root = resolved_root(args)?;
    config_store_for_root(root.as_deref())
}

fn config_store_for_root(root: Option<&std::path::Path>) -> Result<ConfigStore, String> {
    match root {
        Some(root) => Ok(ConfigStore::new(ConfigPaths::from_root(root))),
        None => ConfigStore::platform_default().map_err(|error| error.to_string()),
    }
}

fn resolved_root(args: &[OsString]) -> Result<Option<PathBuf>, String> {
    resolve_root(args, env::var_os("SERIALD_ROOT"))
}

fn resolve_root(
    args: &[OsString],
    environment_root: Option<OsString>,
) -> Result<Option<PathBuf>, String> {
    Ok(extract_root(args)?.or_else(|| environment_root.map(PathBuf::from)))
}

fn extract_root(args: &[OsString]) -> Result<Option<PathBuf>, String> {
    let mut index = 0;
    while let Some(arg) = args.get(index) {
        let Some(arg) = arg.to_str() else {
            return Err("--root path must be valid UTF-8".into());
        };
        if arg == "--root" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| "--root requires a directory".to_owned())?;
            return Ok(Some(PathBuf::from(value)));
        }
        if let Some(value) = arg.strip_prefix("--root=") {
            return Ok(Some(PathBuf::from(value)));
        }
        index += 1;
    }
    Ok(None)
}

fn append_root_arg(command: &mut Command, root: Option<&std::path::Path>) {
    if let Some(root) = root {
        command.arg("--root").arg(root);
    }
}

struct OfflineConfiguration {
    bind: SocketAddr,
    transport_profiles: Vec<TransportProfile>,
    model_profiles: Vec<ModelProfile>,
    ports: Vec<SlotConfig>,
}

fn configure_offline(store: &ConfigStore, interactive: bool) -> Result<(), String> {
    if !interactive {
        store
            .load_or_create()
            .map(|_| ())
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    println!("后端地址 / Endpoint：seriald 监听 IP 和端口");
    println!("串口 Profile / Transport Profile：波特率、数据位、校验位");
    println!("机型 Profile / Model Profile：同系列设备共用的 Shell/U-Boot 提示符和输入方式");
    println!("具体机型名 / Model name：当前串口所连接设备的具体型号");

    let defaults = if store.paths().config_file.exists() {
        store.load().map_err(|error| error.to_string())?
    } else {
        DaemonConfig::generate()
    };
    let bind = prompt("后端 IP:端口", &defaults.bind.to_string())?;
    let bind = SocketAddr::from_str(&bind).map_err(|_| "后端地址格式应为 IP:端口".to_owned())?;

    let existing_ports = defaults
        .ports
        .iter()
        .map(|slot| slot.port.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let ports = prompt("串口名（多个用逗号分隔，可留空）", &existing_ports)?
        .split(',')
        .map(str::trim)
        .filter(|port| !port.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    let transport_name = defaults
        .transport_profiles
        .first()
        .map(|profile| profile.name.as_str())
        .unwrap_or("default-115200");
    let transport_name = prompt("串口 Profile 名", transport_name)?;
    let baud_default = defaults
        .transport_profiles
        .first()
        .map(|profile| profile.baud_rate)
        .unwrap_or(115_200);
    let baud_rate = prompt("波特率", &baud_default.to_string())?
        .parse::<u32>()
        .map_err(|_| "波特率必须是整数".to_owned())?;
    let transport_profiles = vec![TransportProfile {
        name: transport_name.clone(),
        baud_rate,
        data_bits: DataBits::Eight,
        parity: Parity::None,
        stop_bits: StopBits::One,
        flow_control: FlowControl::None,
        dtr: false,
        rts: false,
        auto_open: true,
    }];

    let current_model = defaults.model_profiles.first();
    let model_profile_name = prompt(
        "机型 Profile 名（留空表示不绑定）",
        current_model
            .map(|profile| profile.name.as_str())
            .unwrap_or(""),
    )?;
    let current_concrete_model = defaults
        .ports
        .first()
        .and_then(|port| port.model_name.as_deref())
        .or_else(|| {
            current_model.and_then(|profile| profile.model_names.first().map(String::as_str))
        });
    let concrete_model_name = if model_profile_name.is_empty() {
        String::new()
    } else {
        prompt(
            "具体机型名（留空表示暂不标记）",
            current_concrete_model.unwrap_or(""),
        )?
    };
    let shell_prompt = prompt_optional(
        "Shell 提示符（- 清空）",
        current_model.and_then(|profile| profile.shell_prompt.as_deref()),
    )?;
    let uboot_prompt = prompt_optional(
        "U-Boot 提示符（- 清空）",
        current_model.and_then(|profile| profile.uboot_prompt.as_deref()),
    )?;
    let model_profiles = if model_profile_name.is_empty() {
        Vec::new()
    } else {
        vec![ModelProfile {
            name: model_profile_name.clone(),
            model_names: (!concrete_model_name.is_empty())
                .then(|| concrete_model_name.clone())
                .into_iter()
                .collect(),
            shell_prompt,
            uboot_prompt,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        }]
    };
    let ports = ports
        .into_iter()
        .map(|port| SlotConfig {
            port,
            transport_profile: Some(transport_name.clone()),
            model_profile: (!model_profile_name.is_empty()).then(|| model_profile_name.clone()),
            model_name: (!concrete_model_name.is_empty()).then(|| concrete_model_name.clone()),
            enabled: true,
        })
        .collect();
    persist_offline_configuration(
        store,
        OfflineConfiguration {
            bind,
            transport_profiles,
            model_profiles,
            ports,
        },
    )?;
    println!("配置已保存：{}", store.paths().config_file.display());
    Ok(())
}

fn persist_offline_configuration(
    store: &ConfigStore,
    draft: OfflineConfiguration,
) -> Result<(), String> {
    let mut current = store
        .load_or_create()
        .map_err(|error| error.to_string())?
        .config;
    current.bind = draft.bind;
    current.transport_profiles = draft.transport_profiles;
    current.model_profiles = draft.model_profiles;
    current.ports = draft.ports;
    current.config_revision = current.config_revision.saturating_add(1);
    store.save(&current).map_err(|error| error.to_string())
}

fn prompt(label: &str, default: &str) -> Result<String, String> {
    if default.is_empty() {
        print!("{label}: ");
    } else {
        print!("{label} [{default}]: ");
    }
    std::io::stdout()
        .flush()
        .map_err(|error| error.to_string())?;
    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|error| error.to_string())?;
    let input = input.trim_end_matches(['\r', '\n']);
    Ok(if input.is_empty() {
        default.to_owned()
    } else {
        input.to_owned()
    })
}

fn prompt_optional(label: &str, default: Option<&str>) -> Result<Option<String>, String> {
    let value = prompt(label, default.unwrap_or(""))?;
    Ok((!value.is_empty() && value != "-").then_some(value))
}

fn required_sibling(program: &OsStr) -> std::io::Result<PathBuf> {
    sibling_program(program)
        .filter(|candidate| candidate.is_file())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("missing {}", program.to_string_lossy()),
            )
        })
}

fn tcp_ready(address: SocketAddr) -> bool {
    TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok()
}

fn matching_mcp_health(address: SocketAddr, seriald: &ActiveEndpoint) -> Option<McpHealthResponse> {
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(500)) else {
        return None;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    let request = format!("GET /health HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return None;
    }
    let mut response = Vec::new();
    if stream.take(64 * 1024).read_to_end(&mut response).is_err() {
        return None;
    }
    matching_mcp_health_response(&response, seriald)
}

fn wait_for_matching_mcp(
    address: SocketAddr,
    seriald: &ActiveEndpoint,
    timeout: Duration,
) -> Option<McpHealthResponse> {
    wait_for_matching_mcp_with(timeout, || matching_mcp_health(address, seriald))
}

fn wait_for_matching_mcp_with(
    timeout: Duration,
    mut probe: impl FnMut() -> Option<McpHealthResponse>,
) -> Option<McpHealthResponse> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(health) = probe() {
            return Some(health);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(MCP_IDENTITY_POLL);
    }
}

#[cfg(test)]
fn mcp_health_response_matches(response: &[u8], seriald: &ActiveEndpoint) -> bool {
    matching_mcp_health_response(response, seriald).is_some()
}

fn matching_mcp_health_response(
    response: &[u8],
    seriald: &ActiveEndpoint,
) -> Option<McpHealthResponse> {
    let body_offset = response.windows(4).position(|part| part == b"\r\n\r\n")?;
    let header = &response[..body_offset];
    if !header.starts_with(b"HTTP/1.1 200 ") && !header.starts_with(b"HTTP/1.0 200 ") {
        return None;
    }
    let Ok(health) = serde_json::from_slice::<McpHealthResponse>(&response[body_offset + 4..])
    else {
        return None;
    };
    (health.status == "ok"
        && health.service == "serial-mcp"
        && health.protocol_version == PROTOCOL_VERSION
        && health.protocol_version == seriald.protocol_version
        && health.pid != 0
        && health.seriald_endpoint == seriald.endpoint
        && health.seriald_server_id == seriald.server_id
        && health.seriald_daemon_epoch == seriald.daemon_epoch)
        .then_some(health)
}

fn wait_for_mcp_child(
    mut child: Child,
    address: SocketAddr,
    seriald: &ActiveEndpoint,
) -> std::io::Result<Option<Child>> {
    let deadline = Instant::now() + MCP_STARTUP_TIMEOUT;
    loop {
        if let Some(health) = matching_mcp_health(address, seriald) {
            if health.pid == child.id() {
                drain_child_stderr(&mut child);
                return Ok(Some(child));
            }
            let mut loser = Some(child);
            stop_child(&mut loser);
            return Ok(None);
        }
        if let Some(status) = child.try_wait()? {
            let detail = read_child_stderr(&mut child);
            if wait_for_competing_mcp_after_exit(
                address,
                seriald,
                &detail,
                child.id(),
                MCP_IDENTITY_WAIT.min(deadline.saturating_duration_since(Instant::now())),
            )
            .is_some()
            {
                return Ok(None);
            }
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            };
            return Err(Error::other(format!(
                "serial-mcp exited during startup ({status}){suffix}"
            )));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let detail = read_child_stderr(&mut child);
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            };
            return Err(Error::new(
                ErrorKind::TimedOut,
                format!(
                    "serial-mcp did not publish a matching protocol v5 identity on {address}{suffix}"
                ),
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_competing_mcp_after_exit(
    address: SocketAddr,
    seriald: &ActiveEndpoint,
    detail: &str,
    child_pid: u32,
    timeout: Duration,
) -> Option<McpHealthResponse> {
    wait_for_competing_mcp_after_exit_with(tcp_ready(address), detail, child_pid, timeout, || {
        matching_mcp_health(address, seriald)
    })
}

fn wait_for_competing_mcp_after_exit_with(
    address_ready: bool,
    detail: &str,
    child_pid: u32,
    timeout: Duration,
    probe: impl FnMut() -> Option<McpHealthResponse>,
) -> Option<McpHealthResponse> {
    if !address_ready && !address_in_use_detail(detail) {
        return None;
    }
    wait_for_matching_mcp_with(timeout, probe).filter(|health| health.pid != child_pid)
}

fn address_in_use_detail(detail: &str) -> bool {
    detail.contains("Address already in use")
        || detail.contains("os error 48")
        || detail.contains("os error 98")
        || detail.contains("os error 10048")
}

fn wait_for_child_endpoint(
    mut child: Child,
    paths: &ConfigPaths,
    server_id: uuid::Uuid,
    expected_address: SocketAddr,
    name: &str,
) -> std::io::Result<(Option<Child>, ActiveEndpoint)> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = child.try_wait()? {
            if let Some(active) = discover_active(paths, server_id)
                .map_err(|error| Error::other(error.to_string()))?
            {
                return Ok((None, active));
            }
            let detail = read_child_stderr(&mut child);
            if detail.contains("already owned by another process") {
                while Instant::now() < deadline {
                    if let Some(active) = discover_active(paths, server_id)
                        .map_err(|error| Error::other(error.to_string()))?
                    {
                        return Ok((None, active));
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            };
            return Err(Error::other(format!(
                "{name} exited during startup ({status}){suffix}"
            )));
        }
        let active =
            discover_active(paths, server_id).map_err(|error| Error::other(error.to_string()))?;
        if let Some(active) = active.as_ref()
            && active.address == expected_address
        {
            drain_child_stderr(&mut child);
            return Ok((Some(child), active.clone()));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            if let Some(active) = active {
                return Ok((None, active));
            }
            let detail = read_child_stderr(&mut child);
            let suffix = if detail.is_empty() {
                String::new()
            } else {
                format!(": {detail}")
            };
            return Err(Error::new(
                ErrorKind::TimedOut,
                format!(
                    "{name} did not publish a verified endpoint at http://{expected_address}{suffix}"
                ),
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn read_child_stderr(child: &mut Child) -> String {
    let Some(mut stderr) = child.stderr.take() else {
        return String::new();
    };
    let mut bytes = Vec::new();
    let _ = stderr.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).trim().to_owned()
}

fn drain_child_stderr(child: &mut Child) {
    let Some(mut stderr) = child.stderr.take() else {
        return;
    };
    thread::spawn(move || {
        let _ = std::io::copy(&mut stderr, &mut std::io::sink());
    });
}

fn stop_child(child: &mut Option<Child>) {
    if let Some(child) = child.as_mut() {
        drop(child.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(3);
        while child.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(25));
        }
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
        }
        let _ = child.wait();
    }
}

#[cfg(unix)]
fn configure_managed_child(command: &mut Command) {
    use std::os::unix::process::CommandExt as _;

    command.process_group(0);
}

#[cfg(windows)]
fn configure_managed_child(command: &mut Command) {
    use std::os::windows::process::CommandExt as _;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

fn run_component(
    component: Component,
    program: &OsStr,
    args: &[OsString],
) -> std::io::Result<ExitStatus> {
    let executable = sibling_program(program)
        .filter(|candidate| candidate.is_file())
        .ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!(
                    "matching sibling executable {} is missing",
                    program.to_string_lossy()
                ),
            )
        })?;
    let mut command = Command::new(executable);
    command.args(args);
    run_command(command, component == Component::Daemon)
}

#[cfg(unix)]
fn run_command(mut command: Command, _preserve_child_ctrl_c: bool) -> std::io::Result<ExitStatus> {
    use std::os::unix::process::CommandExt as _;

    // Replace the launcher process so service managers and MCP hosts observe
    // and terminate the real component PID.
    Err(command.exec())
}

#[cfg(windows)]
fn run_command(command: Command, preserve_child_ctrl_c: bool) -> std::io::Result<ExitStatus> {
    windows_job::run(command, preserve_child_ctrl_c)
}

#[cfg(not(any(unix, windows)))]
fn run_command(mut command: Command, _preserve_child_ctrl_c: bool) -> std::io::Result<ExitStatus> {
    command.status()
}

fn sibling_program(program: &OsStr) -> Option<PathBuf> {
    env::current_exe()
        .ok()
        .and_then(|current| current.parent().map(|parent| parent.join(program)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_writes_a_clean_offline_port_configuration() {
        let root = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(root.path()));
        configure_offline(&store, false).unwrap();
        let contents = std::fs::read_to_string(&store.paths().config_file).unwrap();
        assert!(contents.contains("ports = []"));
        assert!(!contents.contains("token"));
        assert!(!contents.contains("auth"));
        assert!(!contents.contains("slots"));
    }

    #[test]
    fn interactive_setup_rebases_on_the_atomic_first_create_winner() {
        let root = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(root.path()));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let winner_store = store.clone();
        let winner_barrier = barrier.clone();
        let winner = std::thread::spawn(move || {
            let loaded = winner_store.load_or_create().unwrap();
            winner_barrier.wait();
            (loaded.config.server_id, loaded.config.config_revision)
        });

        barrier.wait();
        let stale = DaemonConfig::generate();
        let requested_bind = "127.0.0.1:4321".parse().unwrap();
        persist_offline_configuration(
            &store,
            OfflineConfiguration {
                bind: requested_bind,
                transport_profiles: stale.transport_profiles,
                model_profiles: stale.model_profiles,
                ports: stale.ports,
            },
        )
        .unwrap();

        let (winner_server_id, winner_revision) = winner.join().unwrap();
        let saved = store.load().unwrap();
        assert_eq!(saved.server_id, winner_server_id);
        assert_eq!(saved.config_revision, winner_revision + 1);
        assert_eq!(saved.bind, requested_bind);
    }

    #[test]
    fn root_only_invocation_selects_unified_mode_and_setup_is_detected() {
        let root_only = vec![OsString::from("--root"), OsString::from("/tmp/serial")];
        assert_eq!(dispatch_token(&root_only), (None, true));
        let setup = vec![
            OsString::from("--root"),
            OsString::from("/tmp/serial"),
            OsString::from("setup"),
        ];
        assert_eq!(dispatch_token(&setup), (Some("setup"), true));
    }

    #[test]
    fn unified_root_matches_seriald_environment_resolution() {
        let environment = OsString::from("environment-root");
        assert_eq!(
            resolve_root(&[], Some(environment.clone())).unwrap(),
            Some(PathBuf::from("environment-root"))
        );
        assert_eq!(
            resolve_root(
                &[OsString::from("--root"), OsString::from("explicit-root")],
                Some(environment.clone()),
            )
            .unwrap(),
            Some(PathBuf::from("explicit-root"))
        );
        assert_eq!(
            resolve_root(&[OsString::from("--root=inline-root")], Some(environment)).unwrap(),
            Some(PathBuf::from("inline-root"))
        );
        assert_eq!(resolve_root(&[], None).unwrap(), None);
    }

    #[test]
    fn setup_help_is_short_and_describes_each_configuration_item() {
        assert!(SETUP_HELP.contains("serial setup [--root DIR]"));
        assert!(SETUP_HELP.contains("后端地址"));
        assert!(SETUP_HELP.contains("串口 Profile"));
        assert!(SETUP_HELP.contains("机型 Profile"));
        assert!(SETUP_HELP.contains("具体机型名"));
    }

    #[test]
    fn mcp_health_must_match_service_protocol_and_selected_seriald_identity() {
        let seriald = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            42,
        );
        let health = McpHealthResponse {
            status: "ok".into(),
            service: "serial-mcp".into(),
            protocol_version: PROTOCOL_VERSION,
            pid: 43,
            seriald_endpoint: seriald.endpoint.clone(),
            seriald_server_id: seriald.server_id,
            seriald_daemon_epoch: seriald.daemon_epoch,
        };
        let body = serde_json::to_string(&health).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(mcp_health_response_matches(response.as_bytes(), &seriald));

        let wrong_epoch = McpHealthResponse {
            seriald_daemon_epoch: uuid::Uuid::new_v4(),
            ..health
        };
        let body = serde_json::to_string(&wrong_epoch).unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(!mcp_health_response_matches(response.as_bytes(), &seriald));
        assert!(!mcp_health_response_matches(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
            &seriald
        ));
    }

    #[test]
    fn occupied_port_waits_for_a_delayed_matching_mcp_identity() {
        let seriald = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            42,
        );
        let health = McpHealthResponse {
            status: "ok".into(),
            service: "serial-mcp".into(),
            protocol_version: PROTOCOL_VERSION,
            pid: 44,
            seriald_endpoint: seriald.endpoint,
            seriald_server_id: seriald.server_id,
            seriald_daemon_epoch: seriald.daemon_epoch,
        };
        let mut attempts = 0;
        let matched = wait_for_matching_mcp_with(Duration::from_millis(500), || {
            attempts += 1;
            (attempts == 3).then(|| health.clone())
        });
        assert_eq!(matched, Some(health));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn addr_in_use_loser_waits_for_the_delayed_winner_identity() {
        let seriald = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            uuid::Uuid::new_v4(),
            uuid::Uuid::new_v4(),
            42,
        );
        let winner = McpHealthResponse {
            status: "ok".into(),
            service: "serial-mcp".into(),
            protocol_version: PROTOCOL_VERSION,
            pid: 44,
            seriald_endpoint: seriald.endpoint,
            seriald_server_id: seriald.server_id,
            seriald_daemon_epoch: seriald.daemon_epoch,
        };
        let mut attempts = 0;
        let matched = wait_for_competing_mcp_after_exit_with(
            false,
            "bind serial-mcp: Address already in use (os error 48)",
            43,
            Duration::from_millis(500),
            || {
                attempts += 1;
                (attempts == 3).then(|| winner.clone())
            },
        );
        assert_eq!(matched, Some(winner));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn unrelated_listener_is_rejected_after_the_bounded_identity_wait() {
        let started = Instant::now();
        let mut attempts = 0;
        let matched = wait_for_matching_mcp_with(Duration::from_millis(120), || {
            attempts += 1;
            None
        });
        assert!(matched.is_none());
        assert!(attempts >= 2);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}

#[cfg(windows)]
mod windows_job {
    use std::{
        ffi::c_void,
        io,
        mem::size_of,
        os::windows::io::AsRawHandle as _,
        process::{Child, Command, ExitStatus},
        ptr,
    };

    type Handle = *mut c_void;
    type Bool = i32;

    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS: i32 = 9;
    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x0000_2000;
    const CTRL_C_EVENT: u32 = 0;

    #[repr(C)]
    #[derive(Default)]
    struct BasicLimitInformation {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: u32,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: u32,
        affinity: usize,
        priority_class: u32,
        scheduling_class: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct IoCounters {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    #[repr(C)]
    #[derive(Default)]
    struct ExtendedLimitInformation {
        basic_limit_information: BasicLimitInformation,
        io_info: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateJobObjectW(attributes: *const c_void, name: *const u16) -> Handle;
        fn SetInformationJobObject(
            job: Handle,
            information_class: i32,
            information: *const c_void,
            information_length: u32,
        ) -> Bool;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> Bool;
        fn SetConsoleCtrlHandler(
            handler: Option<unsafe extern "system" fn(u32) -> Bool>,
            add: Bool,
        ) -> Bool;
        fn CloseHandle(handle: Handle) -> Bool;
    }

    struct Job(Handle);

    impl Job {
        fn kill_on_close() -> io::Result<Self> {
            // SAFETY: null attributes/name request an unnamed job using
            // process defaults; the returned handle is checked and owned.
            let handle = unsafe { CreateJobObjectW(ptr::null(), ptr::null()) };
            if handle.is_null() {
                return Err(io::Error::last_os_error());
            }
            let mut limits = ExtendedLimitInformation::default();
            limits.basic_limit_information.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `limits` is the documented C layout and remains valid
            // for the duration of this synchronous call.
            let configured = unsafe {
                SetInformationJobObject(
                    handle,
                    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
                    (&raw const limits).cast(),
                    size_of::<ExtendedLimitInformation>() as u32,
                )
            };
            if configured == 0 {
                let error = io::Error::last_os_error();
                // SAFETY: `handle` is a valid owned kernel handle.
                unsafe {
                    CloseHandle(handle);
                }
                return Err(error);
            }
            Ok(Self(handle))
        }

        fn assign(&self, child: &Child) -> io::Result<()> {
            // SAFETY: both handles are valid during this synchronous call.
            let assigned =
                unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle().cast()) };
            if assigned == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            // SAFETY: this type uniquely owns the handle. Closing it also
            // terminates a still-running child because KILL_ON_JOB_CLOSE was
            // configured above.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    unsafe extern "system" fn keep_parent_alive_for_child_ctrl_c(control_type: u32) -> Bool {
        if control_type == CTRL_C_EVENT { 1 } else { 0 }
    }

    struct CtrlCGuard;

    impl CtrlCGuard {
        fn install() -> io::Result<Self> {
            // Install only after the child is spawned, otherwise Windows
            // would let it inherit the parent's Ctrl-C ignore behavior.
            // SAFETY: the handler has static lifetime and only returns a
            // constant; Windows owns no Rust data through this registration.
            let installed =
                unsafe { SetConsoleCtrlHandler(Some(keep_parent_alive_for_child_ctrl_c), 1) };
            if installed == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self)
            }
        }
    }

    impl Drop for CtrlCGuard {
        fn drop(&mut self) {
            // SAFETY: this removes the exact static handler installed above.
            unsafe {
                SetConsoleCtrlHandler(Some(keep_parent_alive_for_child_ctrl_c), 0);
            }
        }
    }

    pub(super) fn run(mut command: Command, preserve_child_ctrl_c: bool) -> io::Result<ExitStatus> {
        let job = Job::kill_on_close()?;
        let mut child = command.spawn()?;
        if let Err(error) = job.assign(&child) {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        let ctrl_c_guard = if preserve_child_ctrl_c {
            match CtrlCGuard::install() {
                Ok(guard) => Some(guard),
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(error);
                }
            }
        } else {
            None
        };
        let status = child.wait()?;
        drop(ctrl_c_guard);
        drop(job);
        Ok(status)
    }
}
