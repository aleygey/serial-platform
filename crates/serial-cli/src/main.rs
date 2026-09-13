use std::{
    env,
    ffi::{OsStr, OsString},
    io::{Error, ErrorKind, IsTerminal as _, Read as _, Write as _},
    net::SocketAddr,
    net::TcpStream,
    path::PathBuf,
    process::{self, Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use serial_protocol::{
    DataBits, EchoMode, FlowControl, McpHealthResponse, ModelFamily, ModelProfile,
    PROTOCOL_VERSION, Parity, SlotConfig, StopBits, TransportProfile,
};
use seriald::config::{ConfigPaths, ConfigStore, DaemonConfig};
use seriald::runtime::{ActiveEndpoint, connect_address, discover_active};

const HELP: &str = "\
Unified serial-platform command

Usage:
  serial                         Start backend, MCP HTTP, and TUI
  serial console [serialctl options]
  serial setup [--root DIR]      Scan ports and configure; reuse a running backend
  serial [--root DIR] serve [seriald options]
  serial profile <transport|model|attach|detach> ...
  serial status|doctor|archives|logs ...
  serial mcp [serial-mcp options]  Start stdio MCP for an MCP host
  serial mcp --dump-tools
  serial paths [seriald options]

The unified command manages sibling components from the same release package.
The unified MCP endpoint uses the selected seriald interface IP on port 3211;
wildcard seriald binds fall back to loopback.
";

const SETUP_HELP: &str = "\
Setup wizard; seriald does not need to be running.

Usage: serial setup [--root DIR]
       serial setup --endpoint http://HOST:3210

自动扫描串口（不打开设备、不发送探测命令），支持刷新、多选和手动输入。
本地离线向导：↑↓ 选择、空格多选、Enter 确认、R 刷新、M 手动输入、L 稍后配置。
按步骤配置并在保存前确认；q 取消且不保存。已有配置、机型和端口保留。
已有后端时改用在线配置；--endpoint 指定后端时扫描的是后端机器的串口。

后端地址：seriald 监听 IP 和端口
串口 Profile：波特率、数据位、校验位
机型 Profile：Shell/U-Boot 提示符和发送行为
一级机型名：设备系列
二级机型名：当前串口连接的具体型号
";

const MCP_STARTUP_TIMEOUT: Duration = Duration::from_secs(8);
const MCP_IDENTITY_WAIT: Duration = Duration::from_secs(2);
const MCP_IDENTITY_POLL: Duration = Duration::from_millis(50);
const MCP_HTTP_PORT: u16 = 3211;

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
    let remote = args
        .iter()
        .position(|a| a == "--endpoint")
        .map(|index| {
            args.get(index + 1)
                .and_then(|v| v.to_str())
                .ok_or("--endpoint requires a URL")
        })
        .transpose()?;
    let active = if store.paths().config_file.exists() {
        let config = store.load().map_err(|e| e.to_string())?;
        discover_active(store.paths(), config.server_id).map_err(|e| e.to_string())?
    } else {
        None
    };
    if let Some(endpoint) = remote.or_else(|| active.as_ref().map(|a| a.endpoint.as_str())) {
        println!("配置已运行的后端 {endpoint}；扫描该后端所在电脑的串口。");
        let status = Command::new(
            required_sibling(component_program(Component::Console)).map_err(|e| e.to_string())?,
        )
        .args(["--endpoint", endpoint, "setup"])
        .status()
        .map_err(|e| e.to_string())?;
        return if status.success() {
            Ok(())
        } else {
            Err("在线配置未完成".into())
        };
    }
    if setup::configure(&store, true)? {
        let status = run_unified(args).map_err(|e| e.to_string())?;
        if !status.success() {
            return Err("配置已保存，但启动未成功；请检查上方连接错误后重试 serial".into());
        }
    }
    Ok(())
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
        let mcp_address = mcp_listen_address(&active);
        if tcp_ready(mcp_address) {
            if wait_for_matching_mcp(mcp_address, &active, MCP_IDENTITY_WAIT).is_none() {
                return Err(Error::new(
                    ErrorKind::AddrInUse,
                    format!(
                        "{mcp_address} is occupied, but it is not protocol {PROTOCOL_VERSION} \
                         serial-mcp connected to the selected seriald"
                    ),
                ));
            }
        } else {
            let mut command = Command::new(required_sibling(component_program(Component::Mcp))?);
            configure_managed_child(&mut command);
            let mcp_listen = mcp_address.to_string();
            command
                .args([
                    "--endpoint",
                    &endpoint,
                    "--listen",
                    &mcp_listen,
                    "--managed",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped());
            mcp = wait_for_mcp_child(command.spawn()?, mcp_address, &active)?;
        }

        println!("seriald {endpoint}  |  MCP http://{mcp_address}/mcp");
        let console = required_sibling(component_program(Component::Console))?;
        Command::new(console)
            .env("SERIALD_ENDPOINT", &endpoint)
            .status()
    })();
    stop_child(&mut mcp);
    stop_child(&mut daemon);
    result
}

fn mcp_listen_address(seriald: &ActiveEndpoint) -> SocketAddr {
    SocketAddr::new(seriald.address.ip(), MCP_HTTP_PORT)
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

mod setup;

fn configure_offline(store: &ConfigStore, interactive: bool) -> Result<(), String> {
    if !interactive {
        store.load_or_create().map_err(|e| e.to_string())?;
        return Ok(());
    }
    setup::configure(store, false).map(|_| ())
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
                    "serial-mcp did not publish a matching protocol v{PROTOCOL_VERSION} identity on {address}{suffix}"
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
        assert!(SETUP_HELP.contains("一级机型名"));
        assert!(SETUP_HELP.contains("二级机型名"));
    }

    #[test]
    fn unified_mcp_inherits_the_selected_seriald_interface() {
        let server_id = uuid::Uuid::new_v4();
        let daemon_epoch = uuid::Uuid::new_v4();
        let host_only = ActiveEndpoint::new(
            "192.168.56.109:4321".parse().unwrap(),
            server_id,
            daemon_epoch,
            42,
        );
        assert_eq!(
            mcp_listen_address(&host_only),
            "192.168.56.109:3211".parse().unwrap()
        );

        let ipv6 = ActiveEndpoint::new(
            "[fd00::109]:4321".parse().unwrap(),
            server_id,
            daemon_epoch,
            42,
        );
        assert_eq!(
            mcp_listen_address(&ipv6),
            "[fd00::109]:3211".parse().unwrap()
        );
    }

    #[test]
    fn wildcard_seriald_bind_keeps_unified_mcp_on_loopback() {
        let server_id = uuid::Uuid::new_v4();
        let daemon_epoch = uuid::Uuid::new_v4();
        let ipv4 =
            ActiveEndpoint::new("0.0.0.0:3210".parse().unwrap(), server_id, daemon_epoch, 42);
        assert_eq!(mcp_listen_address(&ipv4), "127.0.0.1:3211".parse().unwrap());

        let ipv6 = ActiveEndpoint::new("[::]:3210".parse().unwrap(), server_id, daemon_epoch, 42);
        assert_eq!(mcp_listen_address(&ipv6), "[::1]:3211".parse().unwrap());
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
