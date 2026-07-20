use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use directories::ProjectDirs;
use serde::Deserialize;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:3210";

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClientConfig {
    endpoint: Option<String>,
    token_file: Option<PathBuf>,
    #[allow(dead_code)]
    last_slot: Option<String>,
}

pub struct ResolvedConfig {
    pub endpoint: String,
    pub token: String,
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

    Ok(ResolvedConfig { endpoint, token })
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
        };
        let summary = format!("endpoint={}", config.endpoint);
        assert!(!summary.contains(&config.token));
    }
}
