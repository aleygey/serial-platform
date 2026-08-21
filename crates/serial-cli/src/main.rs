use std::{
    env,
    ffi::{OsStr, OsString},
    io::{Error, ErrorKind, IsTerminal as _, Write as _},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
    path::PathBuf,
    process::{self, Child, Command, ExitStatus, Stdio},
    str::FromStr as _,
    thread,
    time::{Duration, Instant},
};

use serial_protocol::{
    DataBits, EchoMode, FlowControl, ModelProfile, Parity, SlotConfig, StopBits, TransportProfile,
};
use seriald::config::{ConfigPaths, ConfigStore, DaemonConfig};

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
机型 Profile：机型名、Shell/U-Boot 提示符和输入方式
";

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
        Some("serve" | "paths") => Component::Daemon,
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
    let store = config_store(args).map_err(Error::other)?;
    if !store.paths().config_file.exists() {
        configure_offline(&store, std::io::stdin().is_terminal()).map_err(Error::other)?;
    }
    let config = store
        .load()
        .map_err(|error| Error::other(error.to_string()))?;
    let daemon_address = local_daemon_address(config.bind);
    let endpoint = format!("http://{daemon_address}");

    let mut daemon = if tcp_ready(daemon_address) {
        None
    } else {
        let mut command = Command::new(required_sibling(component_program(Component::Daemon))?);
        append_root_args(&mut command, args);
        configure_managed_child(&mut command);
        command
            .args(["serve", "--managed"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = command.spawn()?;
        Some(wait_for_child_port(child, daemon_address, "seriald")?)
    };

    let mut mcp = None;
    let result = (|| {
        let mcp_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3211);
        if !tcp_ready(mcp_address) {
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
                .stderr(Stdio::null());
            mcp = Some(wait_for_child_port(
                command.spawn()?,
                mcp_address,
                "serial-mcp",
            )?);
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
    match extract_root(args)? {
        Some(root) => Ok(ConfigStore::new(ConfigPaths::from_root(&root))),
        None => ConfigStore::platform_default().map_err(|error| error.to_string()),
    }
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

fn append_root_args(command: &mut Command, args: &[OsString]) {
    if let Ok(Some(root)) = extract_root(args) {
        command.arg("--root").arg(root);
    }
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
    println!("机型 Profile / Model Profile：机型名、Shell/U-Boot 提示符和输入方式");

    let mut config = if store.paths().config_file.exists() {
        store.load().map_err(|error| error.to_string())?
    } else {
        DaemonConfig::generate()
    };
    let bind = prompt("后端 IP:端口", &config.bind.to_string())?;
    config.bind = SocketAddr::from_str(&bind).map_err(|_| "后端地址格式应为 IP:端口".to_owned())?;

    let existing_ports = config
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

    let transport_name = config
        .transport_profiles
        .first()
        .map(|profile| profile.name.as_str())
        .unwrap_or("default-115200");
    let transport_name = prompt("串口 Profile 名", transport_name)?;
    let baud_default = config
        .transport_profiles
        .first()
        .map(|profile| profile.baud_rate)
        .unwrap_or(115_200);
    let baud_rate = prompt("波特率", &baud_default.to_string())?
        .parse::<u32>()
        .map_err(|_| "波特率必须是整数".to_owned())?;
    config.transport_profiles = vec![TransportProfile {
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

    let current_model = config.model_profiles.first();
    let model_name = prompt(
        "机型名（留空表示不绑定）",
        current_model
            .map(|profile| profile.name.as_str())
            .unwrap_or(""),
    )?;
    let shell_prompt = prompt_optional(
        "Shell 提示符（- 清空）",
        current_model.and_then(|profile| profile.shell_prompt.as_deref()),
    )?;
    let uboot_prompt = prompt_optional(
        "U-Boot 提示符（- 清空）",
        current_model.and_then(|profile| profile.uboot_prompt.as_deref()),
    )?;
    config.model_profiles = if model_name.is_empty() {
        Vec::new()
    } else {
        vec![ModelProfile {
            name: model_name.clone(),
            shell_prompt,
            uboot_prompt,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        }]
    };
    config.ports = ports
        .into_iter()
        .map(|port| SlotConfig {
            port,
            transport_profile: Some(transport_name.clone()),
            model_profile: (!model_name.is_empty()).then(|| model_name.clone()),
            enabled: true,
        })
        .collect();
    config.config_revision = config.config_revision.saturating_add(1);
    store.save(&config).map_err(|error| error.to_string())?;
    println!("配置已保存：{}", store.paths().config_file.display());
    Ok(())
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

fn local_daemon_address(bind: SocketAddr) -> SocketAddr {
    if bind.ip().is_unspecified() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), bind.port())
    } else {
        bind
    }
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

fn wait_for_child_port(
    mut child: Child,
    address: SocketAddr,
    name: &str,
) -> std::io::Result<Child> {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if tcp_ready(address) {
            return Ok(child);
        }
        if let Some(status) = child.try_wait()? {
            return Err(Error::other(format!(
                "{name} exited during startup ({status})"
            )));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(Error::new(
                ErrorKind::TimedOut,
                format!("{name} did not listen on {address}"),
            ));
        }
        thread::sleep(Duration::from_millis(50));
    }
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
    fn setup_help_is_short_and_describes_the_three_configuration_groups() {
        assert!(SETUP_HELP.contains("serial setup [--root DIR]"));
        assert!(SETUP_HELP.contains("后端地址"));
        assert!(SETUP_HELP.contains("串口 Profile"));
        assert!(SETUP_HELP.contains("机型 Profile"));
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
