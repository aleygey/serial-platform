use std::{fs, path::PathBuf};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";
const DEFAULT_CAPTURE_MAX_EVENTS: usize = 4096;
const DEFAULT_CAPTURE_MAX_BYTES: usize = 1024 * 1024;
const HARD_CAPTURE_MAX_EVENTS: usize = 16_384;
const HARD_CAPTURE_MAX_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClientConfig {
    endpoint: Option<String>,
    token_file: Option<PathBuf>,
    #[allow(dead_code)]
    last_slot: Option<String>,
    // These are serialctl-owned console preferences. serial-mcp reads the
    // same file, so it must accept them without giving them Agent semantics.
    #[allow(dead_code)]
    human_idle_release_seconds: Option<u64>,
    #[allow(dead_code)]
    language: Option<String>,
    #[allow(dead_code)]
    merge_echo: Option<bool>,
    #[allow(dead_code)]
    mouse_capture: Option<bool>,
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
    pub token: Option<String>,
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
    let legacy_default_token = config_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("token");
    let token_file = token_file_override.or(config.token_file).or_else(|| {
        // Before token-free personal mode, serial-mcp implicitly read this
        // path even when serialctl.toml omitted token_file. Preserve that
        // established installation shape, but no longer fail when the file
        // does not exist.
        legacy_default_token
            .exists()
            .then_some(legacy_default_token)
    });
    let token = token_file
        .as_deref()
        .map(|path| {
            let token = fs::read_to_string(path)
                .with_context(|| format!("cannot read operator token {}", path.display()))?
                .trim()
                .to_string();
            anyhow::ensure!(
                !token.is_empty(),
                "operator token file {} is empty",
                path.display()
            );
            Ok::<_, anyhow::Error>(token)
        })
        .transpose()?;

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
            token: Some("do-not-log-this-token".into()),
            capture: CaptureLimits::default(),
        };
        let summary = format!("endpoint={}", config.endpoint);
        assert!(!summary.contains(config.token.as_deref().unwrap()));
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
    fn token_file_is_optional_for_loopback_personal_mode() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        fs::write(&config_path, "endpoint = \"http://127.0.0.1:3210\"\n").unwrap();
        let resolved = resolve(Some(config_path), None, None).unwrap();
        assert_eq!(resolved.endpoint, DEFAULT_ENDPOINT);
        assert!(resolved.token.is_none());
    }

    #[test]
    fn existing_implicit_legacy_token_file_is_still_loaded() {
        let temporary = tempfile::tempdir().unwrap();
        let config_path = temporary.path().join("serialctl.toml");
        fs::write(&config_path, "endpoint = \"http://127.0.0.1:3210\"\n").unwrap();
        fs::write(temporary.path().join("token"), "legacy-token\n").unwrap();
        let resolved = resolve(Some(config_path), None, None).unwrap();
        assert_eq!(resolved.token.as_deref(), Some("legacy-token"));
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
merge_echo = true
mouse_capture = false
"#,
        )
        .unwrap();
        assert_eq!(config.human_idle_release_seconds, Some(60));
        assert_eq!(config.language.as_deref(), Some("zh"));
        assert_eq!(config.merge_echo, Some(true));
        assert_eq!(config.mouse_capture, Some(false));
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
