use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
};

use crate::DEFAULT_ENDPOINT;
use anyhow::{Context, Result, ensure};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

pub const DEFAULT_ORPHAN_RUN_TIMEOUT_SECONDS: u64 = 30 * 60;
pub const MIN_ORPHAN_RUN_TIMEOUT_SECONDS: u64 = 5 * 60;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ClientConfig {
    pub endpoint: Option<String>,
    pub last_port: Option<String>,
    /// Seconds of human inactivity before held write control is released.
    /// Defaults to 60 when unset.
    pub human_idle_release_seconds: Option<u64>,
    /// UI language override ("en" or "zh"). Defaults to Chinese when unset.
    pub language: Option<crate::i18n::Lang>,
    /// Capture mouse events for in-app output scrolling and selection.
    /// Defaults to true. Set false to return all mouse handling to the
    /// terminal emulator (which also disables serialctl wheel scrolling).
    pub mouse_capture: Option<bool>,
    /// Number of content rows reserved for the Agent task/command-history
    /// pane on terminals tall enough to show it inline. Values are clamped to
    /// 3..=20; the default of 5 preserves the existing seven-row footprint
    /// once the two visual separators are included.
    pub agent_history_rows: Option<u16>,
    /// Seconds an unpinned Agent Run may remain idle before a newly started
    /// serial-mcp process treats it as orphaned and aborts it. Zero disables
    /// idle cleanup; in that mode only explicit `run_end`, adapter exit, or a
    /// takeover ends ownership. Existing MCP processes read configuration only
    /// at startup.
    pub orphan_run_timeout_seconds: Option<u64>,
    /// serial-mcp capture preferences share this file. serialctl preserves
    /// them when saving its own console preferences.
    pub capture_max_events: Option<usize>,
    pub capture_max_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub path: PathBuf,
    pub config: ClientConfig,
}

#[derive(Clone)]
pub struct ResolvedConfig {
    pub endpoint: String,
    pub last_port: Option<String>,
}

impl fmt::Debug for ResolvedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedConfig")
            .field("endpoint", &self.endpoint)
            .field("last_port", &self.last_port)
            .finish()
    }
}

impl LoadedConfig {
    pub fn load(path_override: Option<PathBuf>) -> Result<Self> {
        let path = match path_override {
            Some(path) => path,
            None => default_config_path()?,
        };
        let config = match fs::read_to_string(&path) {
            Ok(contents) => toml::from_str(&contents)
                .with_context(|| format!("invalid client config {}", path.display()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => ClientConfig::default(),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("cannot read client config {}", path.display()));
            }
        };
        validate_client_config(&config)
            .with_context(|| format!("invalid client config {}", path.display()))?;
        Ok(Self { path, config })
    }

    pub fn resolve(&self, endpoint_override: Option<String>) -> Result<ResolvedConfig> {
        let endpoint = endpoint_override
            .or_else(|| self.config.endpoint.clone())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        Ok(ResolvedConfig {
            endpoint,
            last_port: self.config.last_port.clone(),
        })
    }

    pub fn save(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("cannot create config directory {}", parent.display()))?;
            protect_config_directory(parent)
                .with_context(|| format!("cannot protect config directory {}", parent.display()))?;
        }
        let encoded = toml::to_string_pretty(&self.config)?;
        fs::write(&self.path, encoded)
            .with_context(|| format!("cannot write client config {}", self.path.display()))
    }
}

fn validate_client_config(config: &ClientConfig) -> Result<()> {
    if let Some(timeout) = config.orphan_run_timeout_seconds {
        ensure!(
            timeout == 0 || timeout >= MIN_ORPHAN_RUN_TIMEOUT_SECONDS,
            "orphan Run timeout must be 0 (unlimited) or at least \
             {MIN_ORPHAN_RUN_TIMEOUT_SECONDS} seconds"
        );
    }
    Ok(())
}

#[cfg(unix)]
fn protect_config_directory(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn protect_config_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn default_config_path() -> Result<PathBuf> {
    let project = ProjectDirs::from("dev", "serial-platform", "serial-platform")
        .context("cannot determine the user configuration directory")?;
    Ok(project.config_dir().join("serialctl.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_ignores_unrelated_client_preferences() {
        let config = toml::from_str::<ClientConfig>("mystery = true").unwrap();
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn agent_history_rows_round_trips() {
        let config = toml::from_str::<ClientConfig>("agent_history_rows = 12").unwrap();
        assert_eq!(config.agent_history_rows, Some(12));
        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("agent_history_rows = 12")
        );
    }

    #[test]
    fn shared_mcp_preferences_survive_a_console_save() {
        let mut config = toml::from_str::<ClientConfig>(
            "orphan_run_timeout_seconds = 3600\ncapture_max_events = 8192\n\
             capture_max_bytes = 2097152\n",
        )
        .unwrap();
        config.agent_history_rows = Some(12);

        let encoded = toml::to_string(&config).unwrap();
        assert!(encoded.contains("orphan_run_timeout_seconds = 3600"));
        assert!(encoded.contains("capture_max_events = 8192"));
        assert!(encoded.contains("capture_max_bytes = 2097152"));
        assert!(encoded.contains("agent_history_rows = 12"));
    }

    #[test]
    fn orphan_run_timeout_uses_the_same_strict_bounds_as_serial_mcp() {
        for invalid in [1, 299] {
            let config = ClientConfig {
                orphan_run_timeout_seconds: Some(invalid),
                ..ClientConfig::default()
            };
            assert!(validate_client_config(&config).is_err());
        }

        for valid in [0, 300, 1_800, 86_401, u64::MAX] {
            let config = ClientConfig {
                orphan_run_timeout_seconds: Some(valid),
                ..ClientConfig::default()
            };
            validate_client_config(&config).unwrap();
        }
    }

    #[test]
    fn resolved_debug_includes_only_endpoint_and_last_port() {
        let resolved = ResolvedConfig {
            endpoint: "ws://127.0.0.1:3210".into(),
            last_port: Some("COM4".into()),
        };
        let debug = format!("{resolved:?}");
        assert!(debug.contains("COM4"));
        assert!(!debug.contains("token"));
    }

    #[cfg(unix)]
    #[test]
    fn configuration_directory_is_private() {
        use std::os::unix::fs::PermissionsExt as _;
        use uuid::Uuid;

        let temporary =
            std::env::temp_dir().join(format!("serialctl-config-test-{}", Uuid::new_v4().simple()));
        let config_dir = temporary.join("nested-config");
        LoadedConfig {
            path: config_dir.join("serialctl.toml"),
            config: ClientConfig::default(),
        }
        .save()
        .unwrap();
        assert_eq!(
            fs::metadata(&config_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(fs::read_dir(&config_dir).unwrap().count(), 1);
        fs::remove_dir_all(&temporary).unwrap();
    }
}
