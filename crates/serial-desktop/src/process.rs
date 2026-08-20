use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

#[cfg(unix)]
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalServiceState {
    Stopped,
    Starting { program: PathBuf },
    Running { pid: u32, program: PathBuf },
    Exited { code: Option<i32> },
}

pub struct LocalService {
    child: Option<Child>,
    program: Option<PathBuf>,
    state: LocalServiceState,
}

impl Default for LocalService {
    fn default() -> Self {
        Self {
            child: None,
            program: None,
            state: LocalServiceState::Stopped,
        }
    }
}

impl LocalService {
    pub fn state(&self) -> &LocalServiceState {
        &self.state
    }

    pub fn start(&mut self, configured_program: Option<&Path>, bind: &str) -> Result<()> {
        self.refresh();
        if self.child.is_some() {
            return Ok(());
        }
        let program = resolve_program(configured_program)?;
        self.state = LocalServiceState::Starting {
            program: program.clone(),
        };
        let mut command = Command::new(&program);
        command
            .arg("serve")
            .arg("--bind")
            .arg(bind)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            command.creation_flags(CREATE_NO_WINDOW);
        }
        let child = command
            .spawn()
            .with_context(|| format!("start local service with {}", program.display()))?;
        let pid = child.id();
        self.child = Some(child);
        self.program = Some(program.clone());
        self.state = LocalServiceState::Running { pid, program };
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let Some(mut child) = self.child.take() else {
            self.state = LocalServiceState::Stopped;
            return Ok(());
        };
        if let Err(error) = stop_managed_child(&mut child) {
            self.child = Some(child);
            return Err(error);
        }
        self.program = None;
        self.state = LocalServiceState::Stopped;
        Ok(())
    }

    pub fn refresh(&mut self) {
        let Some(child) = self.child.as_mut() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = None;
                self.program = None;
                self.state = LocalServiceState::Exited {
                    code: status.code(),
                };
            }
            Ok(None) => {}
            Err(_) => {
                self.child = None;
                self.program = None;
                self.state = LocalServiceState::Exited { code: None };
            }
        }
    }
}

/// Stops only the child process owned by this [`LocalService`]. On Unix the
/// daemon first receives SIGINT so its ordinary shutdown path can flush the
/// journal; SIGKILL is a bounded last resort. Windows currently has no safe
/// child-console signal when the GUI is detached, so the explicitly confirmed
/// stop falls back to `TerminateProcess` there.
fn stop_managed_child(child: &mut Child) -> Result<()> {
    if child
        .try_wait()
        .context("check managed local serial service before stop")?
        .is_some()
    {
        return Ok(());
    }
    #[cfg(unix)]
    {
        let pid = i32::try_from(child.id()).context("managed serial service PID exceeds i32")?;
        // SAFETY: `pid` is taken from the live Child owned by this object and
        // SIGINT neither dereferences memory nor crosses a Rust ownership edge.
        let result = unsafe { libc::kill(pid, libc::SIGINT) };
        if result != 0 {
            if child
                .try_wait()
                .context("recheck managed serial service after signal race")?
                .is_some()
            {
                return Ok(());
            }
            return Err(std::io::Error::last_os_error()).context("signal managed serial service");
        }
        if wait_for_exit(child, Duration::from_secs(3))? {
            return Ok(());
        }
    }

    child
        .kill()
        .context("force-stop unresponsive managed local serial service")?;
    child
        .wait()
        .context("reap managed local serial service after stop")?;
    Ok(())
}

#[cfg(unix)]
fn wait_for_exit(child: &mut Child, timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    loop {
        if child
            .try_wait()
            .context("poll managed local serial service shutdown")?
            .is_some()
        {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn endpoint_bind(endpoint: &str) -> Result<String> {
    let endpoint = endpoint.trim().trim_end_matches('/');
    let authority = endpoint
        .strip_prefix("http://")
        .context("local service endpoint must start with http://")?;
    if authority.is_empty()
        || authority.contains('/')
        || authority.contains('?')
        || authority.contains('#')
    {
        bail!("local service endpoint must contain only one host:port authority");
    }
    if !(authority.starts_with("127.0.0.1:") || authority.starts_with("localhost:")) {
        bail!("desktop-managed seriald must bind to loopback");
    }
    Ok(authority.to_string())
}

fn resolve_program(configured_program: Option<&Path>) -> Result<PathBuf> {
    if let Some(program) = configured_program.filter(|path| !path.as_os_str().is_empty()) {
        if program.is_file() {
            return Ok(program.to_path_buf());
        }
        bail!(
            "configured local serial executable does not exist: {}",
            program.display()
        );
    }

    let current = std::env::current_exe().context("resolve desktop executable path")?;
    for candidate in default_program_candidates(&current) {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    bail!(
        "neither `serial` nor `seriald` was found next to serial-desktop or in target/debug; build one or configure its path in Settings"
    )
}

fn default_program_candidates(current_executable: &Path) -> Vec<PathBuf> {
    let (launcher, daemon) = if cfg!(windows) {
        ("serial.exe", "seriald.exe")
    } else {
        ("serial", "seriald")
    };
    let mut candidates = Vec::with_capacity(4);
    if let Some(parent) = current_executable.parent() {
        candidates.push(parent.join(launcher));
        candidates.push(parent.join(daemon));
    }
    candidates.push(PathBuf::from("target").join("debug").join(launcher));
    candidates.push(PathBuf::from("target").join("debug").join(daemon));
    candidates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_endpoint_accepts_only_loopback_http_authorities() {
        assert_eq!(
            endpoint_bind("http://127.0.0.1:3210/").unwrap(),
            "127.0.0.1:3210"
        );
        assert_eq!(
            endpoint_bind("http://localhost:3210").unwrap(),
            "localhost:3210"
        );
        assert!(endpoint_bind("https://127.0.0.1:3210").is_err());
        assert!(endpoint_bind("http://0.0.0.0:3210").is_err());
        assert!(endpoint_bind("http://127.0.0.1:3210/api").is_err());
    }

    #[test]
    fn explicit_program_must_be_a_real_file() {
        let temporary = tempfile::tempdir().unwrap();
        let missing = temporary.path().join("serial");
        assert!(resolve_program(Some(&missing)).is_err());
        std::fs::write(&missing, b"fixture").unwrap();
        assert_eq!(resolve_program(Some(&missing)).unwrap(), missing);
    }

    #[test]
    fn default_resolution_accepts_unified_launcher_or_daemon_siblings() {
        let candidates = default_program_candidates(Path::new("/release/serial-desktop"));
        let launcher = if cfg!(windows) {
            "serial.exe"
        } else {
            "serial"
        };
        let daemon = if cfg!(windows) {
            "seriald.exe"
        } else {
            "seriald"
        };

        assert_eq!(candidates[0], Path::new("/release").join(launcher));
        assert_eq!(candidates[1], Path::new("/release").join(daemon));
    }
}
