use std::{
    fmt, fs, io,
    io::Write as _,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use uuid::Uuid;

use crate::DEFAULT_ENDPOINT;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ClientConfig {
    pub endpoint: Option<String>,
    pub token_file: Option<PathBuf>,
    pub last_slot: Option<String>,
    /// Seconds of human inactivity before held write control is released.
    /// Defaults to 60 when unset.
    pub human_idle_release_seconds: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LoadedConfig {
    pub path: PathBuf,
    pub config: ClientConfig,
}

#[derive(Clone)]
pub struct ResolvedConfig {
    pub endpoint: String,
    pub token: Option<String>,
    pub last_slot: Option<String>,
}

impl fmt::Debug for ResolvedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedConfig")
            .field("endpoint", &self.endpoint)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("last_slot", &self.last_slot)
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
        Ok(Self { path, config })
    }

    pub fn resolve(
        &self,
        endpoint_override: Option<String>,
        token_file_override: Option<PathBuf>,
    ) -> Result<ResolvedConfig> {
        let endpoint = endpoint_override
            .or_else(|| self.config.endpoint.clone())
            .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string());
        let token_path = token_file_override.or_else(|| self.config.token_file.clone());
        let token = token_path.as_deref().map(read_required_token).transpose()?;
        Ok(ResolvedConfig {
            endpoint,
            token,
            last_slot: self.config.last_slot.clone(),
        })
    }

    pub fn default_token_path(&self) -> PathBuf {
        self.path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("token")
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

pub fn read_token_if_present(path: &std::path::Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(token) => {
            let token = token.trim().to_string();
            Ok((!token.is_empty()).then_some(token))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("cannot read token {}", path.display())),
    }
}

fn read_required_token(path: &std::path::Path) -> Result<String> {
    read_token_if_present(path)?.with_context(|| format!("token file {} is empty", path.display()))
}

pub fn write_token(path: &std::path::Path, token: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("cannot create token directory {}", parent.display()))?;
    }
    write_token_contents(path, format!("{}\n", token.trim()).as_bytes())
        .with_context(|| format!("cannot write token {}", path.display()))?;
    Ok(())
}

#[cfg(unix)]
fn write_token_contents(path: &Path, contents: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "token has no parent"))?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("token");
    let (temporary_path, mut temporary) = loop {
        let candidate = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4().simple()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&candidate)
        {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };

    let result = (|| {
        temporary.write_all(contents)?;
        temporary.sync_all()?;
        drop(temporary);
        fs::rename(&temporary_path, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

#[cfg(not(unix))]
fn write_token_contents(path: &Path, contents: &[u8]) -> io::Result<()> {
    // On Windows, files created in the per-user configuration directory
    // inherit its ACL. Explicit ACL management belongs to the installer or
    // service account setup; the roadmap documents hardening for shared hosts.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    file.write_all(contents)?;
    file.sync_all()
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
    fn config_rejects_unknown_fields() {
        assert!(toml::from_str::<ClientConfig>("mystery = true").is_err());
    }

    #[test]
    fn resolved_debug_redacts_the_bearer_token() {
        let resolved = ResolvedConfig {
            endpoint: "ws://127.0.0.1:3210".into(),
            token: Some("do-not-print-this-token".into()),
            last_slot: Some("slot-1".into()),
        };
        let debug = format!("{resolved:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("do-not-print-this-token"));
    }

    #[cfg(unix)]
    #[test]
    fn token_and_configuration_directory_are_private() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary =
            std::env::temp_dir().join(format!("serialctl-config-test-{}", Uuid::new_v4().simple()));
        let config_dir = temporary.join("nested-config");
        let token_path = config_dir.join("token");
        LoadedConfig {
            path: config_dir.join("serialctl.toml"),
            config: ClientConfig::default(),
        }
        .save()
        .unwrap();
        write_token(&token_path, "first-secret").unwrap();
        write_token(&token_path, "replacement-secret").unwrap();

        assert_eq!(
            fs::read_to_string(&token_path).unwrap(),
            "replacement-secret\n"
        );
        assert_eq!(
            fs::metadata(&token_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(&config_dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(fs::read_dir(&config_dir).unwrap().count(), 2);
        fs::remove_dir_all(&temporary).unwrap();
    }
}
