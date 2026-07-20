use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use directories::ProjectDirs;
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";
const DEFAULT_CAPTURE_MAX_EVENTS: usize = 4096;
const DEFAULT_CAPTURE_MAX_BYTES: usize = 1024 * 1024;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClientConfig {
    endpoint: Option<String>,
    token_file: Option<PathBuf>,
    #[allow(dead_code)]
    last_slot: Option<String>,
    capture_max_events: Option<usize>,
    capture_max_bytes: Option<usize>,
}

/// Bounds for one bounded capture window. When either limit is exceeded the
/// oldest events are dropped and the response reports `capture_truncated`.
/// Values come from the shared serialctl.toml; both are optional and keep
/// these defaults so older configs are unaffected.
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
    pub token: String,
    pub capture: CaptureLimits,
}

pub fn resolve(
    config_override: Option<PathBuf>,
    endpoint_override: Option<String>,
    token_file_override: Option<PathBuf>,
) -> Result<ResolvedConfig> {
    let config_path = match config_override {
        Some(path) => path,
        None => default_config_path()?,
    };
    let config = match fs::read_to_string(&config_path) {
        Ok(contents) => toml::from_str::<ClientConfig>(&contents)
            .with_context(|| format!("invalid serialctl config {}", config_path.display()))?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => ClientConfig::default(),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("cannot read serialctl config {}", config_path.display())
            });
        }
    };

    let endpoint = endpoint_override
        .or(config.endpoint)
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
    let token_file = token_file_override
        .or(config.token_file)
        .unwrap_or_else(|| {
            config_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("token")
        });
    let token = fs::read_to_string(&token_file)
        .with_context(|| format!("cannot read operator token {}", token_file.display()))?
        .trim()
        .to_string();
    if token.is_empty() {
        bail!("operator token file {} is empty", token_file.display());
    }

    // A zero limit would make every capture empty, so treat it as unset.
    let capture = CaptureLimits {
        max_events: config
            .capture_max_events
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CAPTURE_MAX_EVENTS),
        max_bytes: config
            .capture_max_bytes
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_CAPTURE_MAX_BYTES),
    };

    Ok(ResolvedConfig {
        endpoint,
        token,
        capture,
    })
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
    fn resolved_config_debug_never_exposes_a_token() {
        let config = ResolvedConfig {
            endpoint: DEFAULT_ENDPOINT.into(),
            token: "do-not-log-this-token".into(),
            capture: CaptureLimits::default(),
        };
        let summary = format!("endpoint={}", config.endpoint);
        assert!(!summary.contains(&config.token));
    }

    #[test]
    fn legacy_config_without_capture_keys_keeps_defaults() {
        let config: ClientConfig =
            toml::from_str("endpoint = \"http://127.0.0.1:3210\"\nlast_slot = \"bench\"\n")
                .unwrap();
        assert!(config.capture_max_events.is_none());
        assert!(config.capture_max_bytes.is_none());
    }

    #[test]
    fn capture_keys_are_optional_and_parsed_when_present() {
        let config: ClientConfig =
            toml::from_str("capture_max_events = 8192\ncapture_max_bytes = 2097152\n").unwrap();
        assert_eq!(config.capture_max_events, Some(8192));
        assert_eq!(config.capture_max_bytes, Some(2 * 1024 * 1024));
    }
}
