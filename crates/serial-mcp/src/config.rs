use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";
const DEFAULT_CAPTURE_MAX_EVENTS: usize = 4096;
const DEFAULT_CAPTURE_MAX_BYTES: usize = 1024 * 1024;
const HARD_CAPTURE_MAX_EVENTS: usize = 16_384;
const HARD_CAPTURE_MAX_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS: u64 = 30 * 60;
pub(crate) const MIN_ORPHAN_RUN_TIMEOUT_SECONDS: u64 = 5 * 60;

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct ClientConfig {
    endpoint: Option<String>,
    #[allow(dead_code)]
    last_port: Option<String>,
    // These are serialctl-owned console preferences. serial-mcp reads the
    // same file, so it must accept them without giving them Agent semantics.
    #[allow(dead_code)]
    human_idle_release_seconds: Option<u64>,
    #[allow(dead_code)]
    language: Option<String>,
    #[allow(dead_code)]
    mouse_capture: Option<bool>,
    #[allow(dead_code)]
    agent_history_rows: Option<u16>,
    orphan_run_timeout_seconds: Option<u64>,
    capture_max_events: Option<usize>,
    capture_max_bytes: Option<usize>,
}

/// Bounds for one bounded capture window. When either limit is exceeded the
/// oldest events are dropped and the response reports `capture_truncated`.
/// Values come from the shared serialctl.toml; both are optional and keep
/// these defaults so capture memory stays bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}

impl Default for CaptureLimits {
    fn default() -> Self {
        Self {
            max_events: DEFAULT_CAPTURE_MAX_EVENTS,
            max_bytes: DEFAULT_CAPTURE_MAX_BYTES,
        }
    }
}

pub struct ResolvedConfig {
    pub endpoint: String,
    pub capture: CaptureLimits,
    /// `None` is the explicit unlimited mode selected with zero seconds.
    pub orphan_run_timeout: Option<Duration>,
    pub config_path: PathBuf,
    /// Exact bytes used to resolve the initial timeout. The watcher starts
    /// from this snapshot so a save between resolve and task startup cannot be
    /// mistaken for the already-applied value.
    pub config_snapshot: Option<Vec<u8>>,
    pub orphan_run_timeout_overridden: bool,
}

pub fn resolve(
    config_override: Option<PathBuf>,
    endpoint_override: Option<String>,
    orphan_run_timeout_override: Option<u64>,
) -> Result<ResolvedConfig> {
    let orphan_run_timeout_overridden = orphan_run_timeout_override.is_some();
    let config_path = match config_override {
        Some(path) => path,
        None => default_config_path()?,
    };
    let (config, config_snapshot) = match fs::read(&config_path) {
        Ok(contents) => {
            let text = std::str::from_utf8(&contents).with_context(|| {
                format!("serialctl config {} is not UTF-8", config_path.display())
            })?;
            let config = toml::from_str::<ClientConfig>(text)
                .with_context(|| format!("invalid serialctl config {}", config_path.display()))?;
            (config, Some(contents))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            (ClientConfig::default(), None)
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("cannot read serialctl config {}", config_path.display())
            });
        }
    };

    let endpoint = endpoint_override
        .or(config.endpoint)
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    // A zero limit would make every capture empty, so treat it as unset.
    // The upper bounds are deliberately not configurable: this process may
    // run inside an Agent host and one noisy UART must not retain unbounded
    // memory merely because the shared config contains an accidental value.
    let capture = CaptureLimits {
        max_events: config
            .capture_max_events
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CAPTURE_MAX_EVENTS)
            .min(HARD_CAPTURE_MAX_EVENTS),
        max_bytes: config
            .capture_max_bytes
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CAPTURE_MAX_BYTES)
            .min(HARD_CAPTURE_MAX_BYTES),
    };

    let orphan_run_timeout_seconds = orphan_run_timeout_override
        .or(config.orphan_run_timeout_seconds)
        .unwrap_or(DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS);
    let orphan_run_timeout = checked_orphan_run_timeout(orphan_run_timeout_seconds)?;

    Ok(ResolvedConfig {
        endpoint,
        capture,
        orphan_run_timeout,
        config_path,
        config_snapshot,
        orphan_run_timeout_overridden,
    })
}

fn parse_orphan_run_timeout(contents: &str) -> Result<Option<Duration>> {
    let config = toml::from_str::<ClientConfig>(contents).context("invalid serialctl config")?;
    checked_orphan_run_timeout(
        config
            .orphan_run_timeout_seconds
            .unwrap_or(DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS),
    )
}

fn checked_orphan_run_timeout(seconds: u64) -> Result<Option<Duration>> {
    anyhow::ensure!(
        seconds == 0 || seconds >= MIN_ORPHAN_RUN_TIMEOUT_SECONDS,
        "orphan Run timeout must be 0 (unlimited) or at least {MIN_ORPHAN_RUN_TIMEOUT_SECONDS} seconds"
    );
    Ok((seconds != 0).then(|| Duration::from_secs(seconds)))
}

#[cfg(test)]
fn reloaded_or_previous(contents: &str, previous: Option<Duration>) -> Option<Duration> {
    parse_orphan_run_timeout(contents).unwrap_or(previous)
}

pub async fn watch_orphan_run_timeout(
    path: PathBuf,
    session: crate::session::SessionHandle,
    initial: Option<Duration>,
    initial_snapshot: Option<Vec<u8>>,
) {
    let mut observed = initial_snapshot;
    let mut applied = initial;
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        poll.tick().await;
        let contents = match fs::read(&path) {
            Ok(contents) => Some(contents),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                eprintln!(
                    "serial-mcp: cannot reload Run timeout config {}: {error}; keeping the active value",
                    path.display()
                );
                continue;
            }
        };
        let Some(contents) = take_changed_contents(&mut observed, contents) else {
            continue;
        };
        let parsed = match contents {
            Some(contents) => String::from_utf8(contents)
                .context("serialctl config is not UTF-8")
                .and_then(|contents| parse_orphan_run_timeout(&contents)),
            None => checked_orphan_run_timeout(DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS),
        };
        let next = match parsed {
            Ok(next) => next,
            Err(error) => {
                eprintln!(
                    "serial-mcp: ignored invalid orphan Run timeout update in {}: {error:#}; keeping the active value",
                    path.display()
                );
                continue;
            }
        };
        if next == applied {
            continue;
        }
        if session.update_run_idle_ttl(next).await.is_err() {
            break;
        }
        applied = next;
        match applied {
            Some(timeout) => eprintln!(
                "serial-mcp: orphan Run timeout updated to {} seconds",
                timeout.as_secs()
            ),
            None => eprintln!("serial-mcp: orphan Run timeout updated to unlimited"),
        }
    }
}

fn take_changed_contents(
    observed: &mut Option<Vec<u8>>,
    current: Option<Vec<u8>>,
) -> Option<Option<Vec<u8>>> {
    if *observed == current {
        return None;
    }
    *observed = current.clone();
    Some(current)
}

fn default_config_path() -> Result<PathBuf> {
    let project = ProjectDirs::from("dev", "serial-platform", "serial-platform")
        .context("cannot determine the per-user serial-platform config directory")?;
    Ok(project.config_dir().join("serialctl.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_without_capture_keys_keeps_defaults() {
        let config: ClientConfig = toml::from_str(
            "endpoint = \"http://127.0.0.1:3210\"\nlast_port = \"COM4\"\nui_hint = true\n",
        )
        .unwrap();
        assert!(config.capture_max_events.is_none());
        assert!(config.capture_max_bytes.is_none());
    }

    #[test]
    fn endpoint_config_needs_no_credentials() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        fs::write(&config_path, "endpoint = \"http://127.0.0.1:3210\"\n").unwrap();
        let resolved = resolve(Some(config_path), None, None).unwrap();
        assert_eq!(resolved.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(
            resolved.orphan_run_timeout,
            Some(Duration::from_secs(DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS))
        );
    }

    #[test]
    fn capture_keys_are_optional_and_parsed_when_present() {
        let config: ClientConfig =
            toml::from_str("capture_max_events = 8192\ncapture_max_bytes = 2097152\n").unwrap();
        assert_eq!(config.capture_max_events, Some(8192));
        assert_eq!(config.capture_max_bytes, Some(2 * 1024 * 1024));
    }

    #[test]
    fn shared_serialctl_console_fields_are_accepted() {
        let config: ClientConfig = toml::from_str(
            r#"
human_idle_release_seconds = 60
language = "zh"
mouse_capture = false
agent_history_rows = 12
orphan_run_timeout_seconds = 1800
"#,
        )
        .unwrap();
        assert_eq!(config.human_idle_release_seconds, Some(60));
        assert_eq!(config.language.as_deref(), Some("zh"));
        assert_eq!(config.mouse_capture, Some(false));
        assert_eq!(config.agent_history_rows, Some(12));
        assert_eq!(config.orphan_run_timeout_seconds, Some(1800));
    }

    #[test]
    fn orphan_run_timeout_uses_default_config_and_cli_precedence() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        fs::write(&config_path, "orphan_run_timeout_seconds = 3600\n").unwrap();

        let from_config = resolve(Some(config_path.clone()), None, None).unwrap();
        assert_eq!(
            from_config.orphan_run_timeout,
            Some(Duration::from_secs(3600))
        );
        assert!(!from_config.orphan_run_timeout_overridden);

        let overridden = resolve(Some(config_path), None, Some(7200)).unwrap();
        assert_eq!(
            overridden.orphan_run_timeout,
            Some(Duration::from_secs(7200))
        );
        assert!(overridden.orphan_run_timeout_overridden);
    }

    #[test]
    fn watcher_observes_a_save_between_resolve_and_its_first_poll() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        let initial = b"orphan_run_timeout_seconds = 1800\n".to_vec();
        fs::write(&config_path, &initial).unwrap();
        let resolved = resolve(Some(config_path.clone()), None, None).unwrap();
        assert_eq!(resolved.config_snapshot, Some(initial));

        let updated = b"orphan_run_timeout_seconds = 3600\n".to_vec();
        fs::write(&config_path, &updated).unwrap();
        let mut observed = resolved.config_snapshot;
        assert_eq!(
            take_changed_contents(&mut observed, Some(updated.clone())),
            Some(Some(updated.clone()))
        );
        assert_eq!(take_changed_contents(&mut observed, Some(updated)), None);
    }

    #[test]
    fn missing_config_snapshot_detects_the_first_created_file() {
        let mut observed = None;
        let created = b"orphan_run_timeout_seconds = 3600\n".to_vec();
        assert_eq!(
            take_changed_contents(&mut observed, Some(created.clone())),
            Some(Some(created))
        );
    }

    #[test]
    fn orphan_run_timeout_accepts_unlimited_and_has_no_finite_upper_bound() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        fs::write(&config_path, "orphan_run_timeout_seconds = 299\n").unwrap();
        assert!(resolve(Some(config_path.clone()), None, None).is_err());

        fs::write(&config_path, "orphan_run_timeout_seconds = 0\n").unwrap();
        assert_eq!(
            resolve(Some(config_path.clone()), None, None)
                .unwrap()
                .orphan_run_timeout,
            None
        );

        fs::write(&config_path, "orphan_run_timeout_seconds = 86401\n").unwrap();
        assert_eq!(
            resolve(Some(config_path), None, None)
                .unwrap()
                .orphan_run_timeout,
            Some(Duration::from_secs(86401))
        );
    }

    #[test]
    fn hot_reload_accepts_finite_and_unlimited_and_retains_on_invalid_input() {
        let previous = Some(Duration::from_secs(1800));
        assert_eq!(
            reloaded_or_previous("orphan_run_timeout_seconds = 3600\n", previous),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            reloaded_or_previous("orphan_run_timeout_seconds = 0\n", previous),
            None
        );
        assert_eq!(
            reloaded_or_previous("orphan_run_timeout_seconds = 30\n", previous),
            previous
        );
        assert_eq!(reloaded_or_previous("not = [valid", previous), previous);
    }

    #[test]
    fn capture_limits_have_non_configurable_hard_caps() {
        let config: ClientConfig =
            toml::from_str("capture_max_events = 999999999\ncapture_max_bytes = 999999999\n")
                .unwrap();
        let capture = CaptureLimits {
            max_events: config
                .capture_max_events
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_CAPTURE_MAX_EVENTS)
                .min(HARD_CAPTURE_MAX_EVENTS),
            max_bytes: config
                .capture_max_bytes
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_CAPTURE_MAX_BYTES)
                .min(HARD_CAPTURE_MAX_BYTES),
        };
        assert_eq!(capture.max_events, HARD_CAPTURE_MAX_EVENTS);
        assert_eq!(capture.max_bytes, HARD_CAPTURE_MAX_BYTES);
    }
}
