use std::{
    env,
    ffi::{OsStr, OsString},
    io::{Error, ErrorKind},
    path::PathBuf,
    process::{self, Command, ExitStatus},
};

const HELP: &str = "\
Unified serial-platform command

Usage:
  serial [console] [serialctl options]
  serial [--root DIR] serve [seriald options]
  serial setup [serialctl options]
  serial profile <transport|device|attach|detach> ...
  serial model <list|tree|add|update|attach|detach> ...
  serial status|doctor|archives|logs ...
  serial mcp [serial-mcp options]
  serial mcp --dump-tools
  serial credentials|auth|paths [seriald options]

The legacy seriald, serialctl, and serial-mcp executables remain available
during the 0.4 transition. `serial` delegates to the sibling component from
the same release package. Components from another installation are never
selected from PATH because mixing protocol versions is unsafe.
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
        Some("serve" | "credentials" | "auth" | "paths") => Component::Daemon,
        Some("mcp") => Component::Mcp,
        Some(
            "console" | "setup" | "init" | "profile" | "model" | "status" | "doctor" | "archives"
            | "logs",
        ) => Component::Console,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn values(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn no_command_and_console_both_open_the_tui() {
        assert_eq!(
            dispatch(Vec::new()),
            Dispatch {
                component: Component::Console,
                args: Vec::new()
            }
        );
        assert_eq!(
            dispatch(values(&["console", "--initial-slot", "dut-1"])),
            Dispatch {
                component: Component::Console,
                args: values(&["--initial-slot", "dut-1"])
            }
        );
    }

    #[test]
    fn management_commands_keep_their_subcommand() {
        assert_eq!(
            dispatch(values(&["profile", "device", "list", "--json"])),
            Dispatch {
                component: Component::Console,
                args: values(&["profile", "device", "list", "--json"])
            }
        );
        assert_eq!(
            dispatch(values(&["doctor", "stream", "--slot", "dut-1"])),
            Dispatch {
                component: Component::Console,
                args: values(&["doctor", "stream", "--slot", "dut-1"])
            }
        );
        assert_eq!(
            dispatch(values(&["model", "tree", "--json"])),
            Dispatch {
                component: Component::Console,
                args: values(&["model", "tree", "--json"])
            }
        );
    }

    #[test]
    fn daemon_and_mcp_commands_select_the_right_component() {
        assert_eq!(
            dispatch(values(&["serve", "--bind", "0.0.0.0:3210"])),
            Dispatch {
                component: Component::Daemon,
                args: values(&["serve", "--bind", "0.0.0.0:3210"])
            }
        );
        assert_eq!(
            dispatch(values(&["mcp", "--actor-label", "codex"])),
            Dispatch {
                component: Component::Mcp,
                args: values(&["--actor-label", "codex"])
            }
        );
        assert_eq!(
            dispatch(values(&["auth", "disable"])),
            Dispatch {
                component: Component::Daemon,
                args: values(&["auth", "disable"])
            }
        );
    }

    #[test]
    fn daemon_root_can_precede_or_follow_the_daemon_subcommand() {
        assert_eq!(
            dispatch(values(&[
                "--root",
                "portable",
                "serve",
                "--bind",
                "127.0.0.1:0"
            ])),
            Dispatch {
                component: Component::Daemon,
                args: values(&["--root", "portable", "serve", "--bind", "127.0.0.1:0"])
            }
        );
        assert_eq!(
            dispatch(values(&["serve", "--root=portable"])),
            Dispatch {
                component: Component::Daemon,
                args: values(&["serve", "--root=portable"])
            }
        );
        assert_eq!(
            dispatch(values(&["--root", "portable"])),
            Dispatch {
                component: Component::Daemon,
                args: values(&["--root", "portable"])
            }
        );
    }
}
