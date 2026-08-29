//! Persistent daemon configuration and cross-platform storage paths.
//!
//! The persisted `server_id` identifies one installation. A fresh
//! `daemon_epoch` is intentionally generated on every load so cursors, control
//! leases, and writes from an earlier daemon process cannot be mistaken for
//! current state.

use std::{
    collections::{HashMap, HashSet},
    fmt, fs, io,
    io::Write as _,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serial_protocol::{
    EchoMode, FlowControl, MAX_COMMAND_CAPTURE_DETAIL_BYTES, MAX_MODEL_FAMILIES,
    MAX_MODEL_NAMES_PER_FAMILY, ModelFamily, ModelProfile, SlotConfig, TransportProfile,
};
use thiserror::Error;
use uuid::Uuid;

use crate::control::{
    ControlLimits, MAX_CONTROL_TTL_MS, MAX_CONTROL_WAIT_TIMEOUT, MAX_TTL_MS, MAX_WAITERS,
    WAIT_TIMEOUT,
};

pub const CONFIG_SCHEMA_VERSION: u32 = 3;
pub const DEFAULT_PORT: u16 = 3210;
pub const GIB: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_LOG_BYTES: u64 = 10 * GIB;
pub const DEFAULT_RETENTION_TARGET_PERCENT: u8 = 90;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Hard bound for both one active configuration and the number of distinct
/// Port identities retained during one daemon epoch.
pub const MAX_PORT_IDENTITIES_PER_DAEMON: usize = 128;
/// Hard bound for the model profile catalog.
pub const MAX_MODEL_PROFILES: usize = 128;
/// Hard bound for the physical UART profile catalog.
pub const MAX_TRANSPORT_PROFILES: usize = 128;
const MAX_CONFIG_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_PORT_NAME_BYTES: usize = 512;
const MAX_PROFILE_NAME_BYTES: usize = 64;
const MAX_MODEL_NAME_BYTES: usize = 128;
const MAX_CONFIG_MIGRATION_RETRIES: usize = 8;
const MAX_CONFIG_MIGRATION_BACKUPS: usize = 128;

/// Files owned by one serial-platform installation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigPaths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub config_file: PathBuf,
    pub journal_dir: PathBuf,
    pub journal_index: PathBuf,
    pub monitor_state_file: PathBuf,
}

impl ConfigPaths {
    /// Resolves OS-native user configuration and local-data locations.
    pub fn platform_default() -> Result<Self, ConfigError> {
        let project = ProjectDirs::from("io", "OpenChamber", "serial-platform")
            .ok_or(ConfigError::ProjectDirectoriesUnavailable)?;
        Ok(Self::new(
            project.config_dir().to_path_buf(),
            project.data_local_dir().to_path_buf(),
        ))
    }

    #[must_use]
    pub fn new(config_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            config_file: config_dir.join("seriald.toml"),
            journal_dir: data_dir.join("journal"),
            journal_index: data_dir.join("journal.sqlite3"),
            monitor_state_file: data_dir.join("monitors.json"),
            config_dir,
            data_dir,
        }
    }

    /// Creates isolated paths below `root`; intended for tests and explicitly
    /// portable installations, never as an implicit fallback for user paths.
    #[must_use]
    pub fn from_root(root: &Path) -> Self {
        Self::new(root.join("config"), root.join("data"))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LoggingConfig {
    /// Hard retention ceiling across all closed and active journal segments.
    pub max_total_bytes: u64,
    /// When pruning is necessary, continue until usage is at or below this
    /// percentage of `max_total_bytes`.
    pub retention_target_percent: u8,
    /// Rotate an active journal segment after this many uncompressed bytes.
    pub segment_max_bytes: u64,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            max_total_bytes: DEFAULT_MAX_LOG_BYTES,
            retention_target_percent: DEFAULT_RETENTION_TARGET_PERCENT,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
        }
    }
}

/// Startup-time bounds for the write-control lease machinery.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControlConfig {
    /// Ceiling applied to client-requested lease TTLs.
    pub max_ttl_ms: u64,
    /// Lifetime of a pending Human Run-start approval before it is dropped.
    pub wait_timeout_ms: u64,
    /// Legacy sizing field retained in schema 3; v7 has no Control wait queue.
    pub max_waiters: usize,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            max_ttl_ms: MAX_TTL_MS,
            wait_timeout_ms: WAIT_TIMEOUT.as_millis() as u64,
            max_waiters: MAX_WAITERS,
        }
    }
}

impl ControlConfig {
    /// Converts persisted values into defensively bounded runtime limits.
    /// [`DaemonConfig::validate`] rejects values above these bounds; applying
    /// them here as well protects direct in-process construction.
    #[must_use]
    pub fn limits(&self) -> ControlLimits {
        ControlLimits {
            max_ttl_ms: self.max_ttl_ms,
            wait_timeout: Duration::from_millis(self.wait_timeout_ms),
            max_waiters: self.max_waiters,
        }
        .bounded()
    }
}

/// Values persisted in `seriald.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub schema_version: u32,
    /// Monotonic persisted configuration generation used for optimistic
    /// concurrency across multiple serialctl/admin clients.
    #[serde(default = "default_config_revision")]
    pub config_revision: u64,
    pub server_id: Uuid,
    pub bind: SocketAddr,
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub control: ControlConfig,
    #[serde(default)]
    pub ports: Vec<SlotConfig>,
    #[serde(default)]
    pub transport_profiles: Vec<TransportProfile>,
    #[serde(default)]
    pub model_profiles: Vec<ModelProfile>,
    #[serde(default)]
    pub model_families: Vec<ModelFamily>,
}

/// The schema-2 shape is retained solely for a lossless, one-way startup
/// migration. In schema 2, model identity names lived inside the interaction
/// profile and a port's selected family was implied by `model_profile`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonConfigV2 {
    schema_version: u32,
    #[serde(default = "default_config_revision")]
    config_revision: u64,
    server_id: Uuid,
    bind: SocketAddr,
    #[serde(default)]
    logging: LoggingConfig,
    #[serde(default)]
    control: ControlConfig,
    #[serde(default)]
    ports: Vec<SlotConfigV2>,
    #[serde(default)]
    transport_profiles: Vec<TransportProfile>,
    #[serde(default)]
    model_profiles: Vec<ModelProfileV2>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlotConfigV2 {
    port: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transport_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_name: Option<String>,
    enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelProfileV2 {
    name: String,
    /// `None` also distinguishes the released v0.8.0 schema-2 shape from the
    /// later schema-2 development shape when another profile or port carries
    /// one of the added identity fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_names: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shell_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    uboot_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_eol: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    echo: Option<EchoMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_chunk_size: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    write_chunk_delay_ms: Option<u64>,
}

impl DaemonConfigV2 {
    fn migrate(self) -> DaemonConfig {
        // The released v0.8.0 schema used ModelProfile.name as both behavior
        // profile and device identity. A later development build added
        // model_names/model_name without changing schema_version. Presence of
        // either added field selects that extended shape config-wide; if no
        // marker exists, preserving the released behavior is the only
        // lossless interpretation of the ambiguous TOML.
        let extended_identity_shape = self
            .model_profiles
            .iter()
            .any(|profile| profile.model_names.is_some())
            || self.ports.iter().any(|slot| slot.model_name.is_some());
        let model_families = self
            .model_profiles
            .iter()
            .map(|profile| ModelFamily {
                name: profile.name.clone(),
                model_names: if extended_identity_shape {
                    profile.model_names.clone().unwrap_or_default()
                } else {
                    vec![profile.name.clone()]
                },
            })
            .collect();
        let model_profiles = self
            .model_profiles
            .into_iter()
            .map(|profile| ModelProfile {
                name: profile.name,
                shell_prompt: profile.shell_prompt,
                uboot_prompt: profile.uboot_prompt,
                write_eol: profile.write_eol,
                echo: profile.echo,
                write_chunk_size: profile.write_chunk_size,
                write_chunk_delay_ms: profile.write_chunk_delay_ms,
            })
            .collect();
        let ports = self
            .ports
            .into_iter()
            .map(|slot| {
                let (model_family, model_name) = if extended_identity_shape {
                    (
                        slot.model_name
                            .as_ref()
                            .and_then(|_| slot.model_profile.clone()),
                        slot.model_name,
                    )
                } else {
                    (slot.model_profile.clone(), slot.model_profile.clone())
                };
                SlotConfig {
                    port: slot.port,
                    transport_profile: slot.transport_profile,
                    model_profile: slot.model_profile,
                    model_family,
                    model_name,
                    enabled: slot.enabled,
                }
            })
            .collect();

        DaemonConfig {
            schema_version: CONFIG_SCHEMA_VERSION,
            config_revision: self.config_revision,
            server_id: self.server_id,
            bind: self.bind,
            logging: self.logging,
            control: self.control,
            ports,
            transport_profiles: self.transport_profiles,
            model_profiles,
            model_families,
        }
    }
}

#[derive(Deserialize)]
struct ConfigSchemaHeader {
    schema_version: u32,
}

struct ParsedConfig {
    config: DaemonConfig,
    schema2_source: Option<String>,
}

const fn default_config_revision() -> u64 {
    1
}

impl DaemonConfig {
    pub fn generate() -> Self {
        Self {
            schema_version: CONFIG_SCHEMA_VERSION,
            config_revision: default_config_revision(),
            server_id: Uuid::new_v4(),
            bind: default_bind_address(),
            logging: LoggingConfig::default(),
            control: ControlConfig::default(),
            ports: Vec::new(),
            transport_profiles: Vec::new(),
            model_profiles: Vec::new(),
            model_families: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.schema_version != CONFIG_SCHEMA_VERSION {
            return Err(ConfigValidationError::UnsupportedSchemaVersion(
                self.schema_version,
            ));
        }
        if self.server_id.is_nil() {
            return Err(ConfigValidationError::NilServerId);
        }
        if self.bind.port() == 0 {
            return Err(ConfigValidationError::InvalidBindPort);
        }
        validate_logging(&self.logging)?;
        validate_control(&self.control)?;
        validate_transport_profiles(&self.transport_profiles)?;
        validate_model_profiles(&self.model_profiles)?;
        validate_model_families(&self.model_families)?;
        validate_ports(
            &self.ports,
            &self.transport_profiles,
            &self.model_profiles,
            &self.model_families,
        )
    }

    /// Replaces every configured port after validating the complete result.
    pub fn replace_ports(&mut self, ports: Vec<SlotConfig>) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.ports, ports);
        if let Err(error) = self.validate() {
            self.ports = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Builds a fully validated candidate without changing the live in-memory
    /// configuration. Runtime and persistence layers can then commit it in
    /// their own transaction order.
    pub fn staged_with_ports(&self, ports: Vec<SlotConfig>) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_ports(ports)?;
        staged.bump_revision()?;
        Ok(staged)
    }

    pub fn replace_transport_profiles(
        &mut self,
        transport_profiles: Vec<TransportProfile>,
    ) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.transport_profiles, transport_profiles);
        if let Err(error) = self.validate() {
            self.transport_profiles = previous;
            return Err(error);
        }
        Ok(())
    }

    pub fn staged_with_transport_profiles(
        &self,
        transport_profiles: Vec<TransportProfile>,
    ) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_transport_profiles(transport_profiles)?;
        staged.bump_revision()?;
        Ok(staged)
    }

    /// Replaces the model profile catalog in memory after validating the
    /// complete resulting daemon configuration, including every port's
    /// profile reference.
    pub fn replace_model_profiles(
        &mut self,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.model_profiles, model_profiles);
        if let Err(error) = self.validate() {
            self.model_profiles = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Builds a fully validated candidate with a replaced model profile
    /// catalog without changing the live in-memory configuration.
    pub fn staged_with_model_profiles(
        &self,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_model_profiles(model_profiles)?;
        staged.bump_revision()?;
        Ok(staged)
    }

    pub fn replace_model_families(
        &mut self,
        model_families: Vec<ModelFamily>,
    ) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.model_families, model_families);
        if let Err(error) = self.validate() {
            self.model_families = previous;
            return Err(error);
        }
        Ok(())
    }

    pub fn staged_with_model_families(
        &self,
        model_families: Vec<ModelFamily>,
    ) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_model_families(model_families)?;
        staged.bump_revision()?;
        Ok(staged)
    }

    fn bump_revision(&mut self) -> Result<(), ConfigValidationError> {
        self.config_revision = self
            .config_revision
            .checked_add(1)
            .ok_or(ConfigValidationError::RevisionExhausted)?;
        Ok(())
    }
}

#[must_use]
pub const fn default_bind_address() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), DEFAULT_PORT)
}

/// Runtime configuration returned by one daemon startup.
pub struct LoadedConfig {
    pub config: DaemonConfig,
    pub daemon_epoch: Uuid,
    pub paths: ConfigPaths,
}

impl fmt::Debug for LoadedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedConfig")
            .field("config", &self.config)
            .field("daemon_epoch", &self.daemon_epoch)
            .field("paths", &self.paths)
            .finish()
    }
}

/// Owns configuration I/O. Constructing a store does not touch the filesystem.
#[derive(Clone, Debug)]
pub struct ConfigStore {
    paths: ConfigPaths,
}

impl ConfigStore {
    pub fn platform_default() -> Result<Self, ConfigError> {
        Ok(Self::new(ConfigPaths::platform_default()?))
    }

    #[must_use]
    pub fn new(paths: ConfigPaths) -> Self {
        Self { paths }
    }

    #[must_use]
    pub fn paths(&self) -> &ConfigPaths {
        &self.paths
    }

    /// Loads an existing valid configuration or atomically creates the first
    /// one. A valid schema-2 configuration is backed up and atomically
    /// rewritten in schema 3. Existing unreadable or invalid files are never
    /// overwritten.
    pub fn load_or_create(&self) -> Result<LoadedConfig, ConfigError> {
        self.ensure_directories()?;

        let config = if self.paths.config_file.exists() {
            self.load_and_persist_migration()?
        } else {
            let config = DaemonConfig::generate();
            config.validate()?;
            let serialized =
                toml::to_string_pretty(&config).map_err(|_| ConfigError::Serialization)?;
            if atomic_create(&self.paths.config_file, serialized.as_bytes())
                .map_err(|source| io_error(&self.paths.config_file, source))?
            {
                config
            } else {
                self.load_and_persist_migration()?
            }
        };

        Ok(LoadedConfig {
            config,
            daemon_epoch: Uuid::new_v4(),
            paths: self.paths.clone(),
        })
    }

    /// Loads and validates an existing configuration without creating or
    /// rewriting one. Schema 2 is migrated in memory so discovery and unified
    /// launcher paths can read the installation identity before seriald owns
    /// the runtime lock.
    pub fn load(&self) -> Result<DaemonConfig, ConfigError> {
        Ok(self.read_existing()?.config)
    }

    fn load_and_persist_migration(&self) -> Result<DaemonConfig, ConfigError> {
        for _ in 0..MAX_CONFIG_MIGRATION_RETRIES {
            let parsed = self.read_existing()?;
            let Some(schema2_source) = parsed.schema2_source.as_deref() else {
                return Ok(parsed.config);
            };
            if self.persist_schema2_migration(schema2_source, &parsed.config)? {
                return Ok(parsed.config);
            }
        }
        Err(ConfigError::MigrationConflict {
            path: self.paths.config_file.clone(),
        })
    }

    fn read_existing(&self) -> Result<ParsedConfig, ConfigError> {
        let serialized = self.read_serialized()?;
        let header: ConfigSchemaHeader =
            toml::from_str(&serialized).map_err(|_| ConfigError::InvalidToml {
                path: self.paths.config_file.clone(),
            })?;
        let (config, schema2_source) = match header.schema_version {
            2 => {
                let legacy: DaemonConfigV2 =
                    toml::from_str(&serialized).map_err(|_| ConfigError::InvalidToml {
                        path: self.paths.config_file.clone(),
                    })?;
                (legacy.migrate(), Some(serialized))
            }
            CONFIG_SCHEMA_VERSION => {
                let config: DaemonConfig =
                    toml::from_str(&serialized).map_err(|_| ConfigError::InvalidToml {
                        path: self.paths.config_file.clone(),
                    })?;
                (config, None)
            }
            version => {
                return Err(ConfigValidationError::UnsupportedSchemaVersion(version).into());
            }
        };
        config.validate()?;
        Ok(ParsedConfig {
            config,
            schema2_source,
        })
    }

    fn read_serialized(&self) -> Result<String, ConfigError> {
        let metadata = fs::metadata(&self.paths.config_file)
            .map_err(|source| io_error(&self.paths.config_file, source))?;
        if metadata.len() > MAX_CONFIG_FILE_BYTES {
            return Err(ConfigError::ConfigFileTooLarge {
                path: self.paths.config_file.clone(),
                bytes: metadata.len(),
            });
        }
        restrict_config_file_permissions(&self.paths.config_file)
            .map_err(|source| io_error(&self.paths.config_file, source))?;
        fs::read_to_string(&self.paths.config_file)
            .map_err(|source| io_error(&self.paths.config_file, source))
    }

    /// Returns `false` when the source changed since it was parsed. The caller
    /// must then reload and migrate the newer contents instead of overwriting
    /// a concurrent configuration update.
    fn persist_schema2_migration(
        &self,
        expected_source: &str,
        migrated: &DaemonConfig,
    ) -> Result<bool, ConfigError> {
        if self.read_serialized()? != expected_source {
            return Ok(false);
        }

        self.ensure_schema2_backup(expected_source)?;

        // Creating the backup takes time and may race a non-daemon offline
        // writer. Recheck immediately before the atomic replacement.
        if self.read_serialized()? != expected_source {
            return Ok(false);
        }
        let serialized =
            toml::to_string_pretty(migrated).map_err(|_| ConfigError::Serialization)?;
        #[cfg(windows)]
        {
            let mut last_error = match atomic_write(&self.paths.config_file, serialized.as_bytes())
            {
                Ok(()) => return Ok(true),
                Err(source) if is_windows_replace_contention(&source) => source,
                Err(source) => return Err(io_error(&self.paths.config_file, source)),
            };
            for delay_ms in [1, 2, 4, 8, 16, 32, 64] {
                std::thread::sleep(Duration::from_millis(delay_ms));
                match self.read_serialized() {
                    Ok(current) if current != expected_source => return Ok(false),
                    Ok(_) => {}
                    Err(ConfigError::Io { source, .. })
                        if is_windows_replace_contention(&source) =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
                match atomic_write(&self.paths.config_file, serialized.as_bytes()) {
                    Ok(()) => return Ok(true),
                    Err(source) if is_windows_replace_contention(&source) => {
                        last_error = source;
                    }
                    Err(source) => return Err(io_error(&self.paths.config_file, source)),
                }
            }
            Err(io_error(&self.paths.config_file, last_error))
        }
        #[cfg(not(windows))]
        {
            atomic_write(&self.paths.config_file, serialized.as_bytes())
                .map_err(|source| io_error(&self.paths.config_file, source))?;
            Ok(true)
        }
    }

    #[cfg(test)]
    fn schema2_backup_path(&self) -> PathBuf {
        self.schema2_backup_path_at(0)
    }

    fn schema2_backup_path_at(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.paths.config_file.with_extension("toml.schema2.bak")
        } else {
            self.paths
                .config_file
                .with_extension(format!("toml.schema2.{index}.bak"))
        }
    }

    fn ensure_schema2_backup(&self, expected_source: &str) -> Result<PathBuf, ConfigError> {
        for index in 0..MAX_CONFIG_MIGRATION_BACKUPS {
            let backup_path = self.schema2_backup_path_at(index);
            if atomic_create(&backup_path, expected_source.as_bytes())
                .map_err(|source| io_error(&backup_path, source))?
                || regular_file_has_contents(&backup_path, expected_source.as_bytes())
            {
                return Ok(backup_path);
            }
        }
        Err(ConfigError::MigrationBackupExhausted {
            path: self.paths.config_file.clone(),
            limit: MAX_CONFIG_MIGRATION_BACKUPS,
        })
    }

    /// Validates and atomically replaces the persisted configuration.
    pub fn save(&self, config: &DaemonConfig) -> Result<(), ConfigError> {
        config.validate()?;
        self.ensure_directories()?;
        let serialized = toml::to_string_pretty(config).map_err(|_| ConfigError::Serialization)?;
        atomic_write(&self.paths.config_file, serialized.as_bytes())
            .map_err(|source| io_error(&self.paths.config_file, source))
    }

    /// Persists a validated port replacement and only then commits it to the
    /// caller's in-memory configuration. A failed write leaves both unchanged.
    pub fn update_ports(
        &self,
        current: &mut DaemonConfig,
        ports: Vec<SlotConfig>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_ports(ports)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    /// Persists a validated model profile catalog replacement and only then
    /// commits it to the caller's in-memory configuration.
    pub fn update_model_profiles(
        &self,
        current: &mut DaemonConfig,
        model_profiles: Vec<ModelProfile>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_model_profiles(model_profiles)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    pub fn update_model_families(
        &self,
        current: &mut DaemonConfig,
        model_families: Vec<ModelFamily>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_model_families(model_families)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    pub fn update_transport_profiles(
        &self,
        current: &mut DaemonConfig,
        transport_profiles: Vec<TransportProfile>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_transport_profiles(transport_profiles)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    fn ensure_directories(&self) -> Result<(), ConfigError> {
        for directory in [
            &self.paths.config_dir,
            &self.paths.data_dir,
            &self.paths.journal_dir,
        ] {
            fs::create_dir_all(directory).map_err(|source| io_error(directory, source))?;
            restrict_directory_permissions(directory)
                .map_err(|source| io_error(directory, source))?;
        }
        Ok(())
    }
}

fn regular_file_has_contents(path: &Path, expected: &[u8]) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    metadata.file_type().is_file()
        && metadata.len() == expected.len() as u64
        && fs::read(path).is_ok_and(|contents| contents == expected)
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("the operating system did not provide a user configuration directory")]
    ProjectDirectoriesUnavailable,
    #[error("configuration I/O failed at {path}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "configuration file at {path} exceeds the {MAX_CONFIG_FILE_BYTES}-byte limit ({bytes} bytes)"
    )]
    ConfigFileTooLarge { path: PathBuf, bytes: u64 },
    #[error("configuration at {path} kept changing while schema 2 migration was in progress")]
    MigrationConflict { path: PathBuf },
    #[error(
        "configuration at {path} has no free schema-2 backup slot among {limit} protected paths"
    )]
    MigrationBackupExhausted { path: PathBuf, limit: usize },
    #[error("configuration file at {path} is not valid TOML")]
    InvalidToml { path: PathBuf },
    #[error("configuration could not be serialized")]
    Serialization,
    #[error(transparent)]
    Validation(#[from] ConfigValidationError),
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ConfigValidationError {
    #[error("unsupported configuration schema version {0}")]
    UnsupportedSchemaVersion(u32),
    #[error("server_id must not be nil")]
    NilServerId,
    #[error("bind port must be non-zero")]
    InvalidBindPort,
    #[error("configuration revision is exhausted")]
    RevisionExhausted,
    #[error("max_total_bytes must be non-zero")]
    InvalidLogCapacity,
    #[error("retention_target_percent must be between 1 and 99")]
    InvalidRetentionTarget,
    #[error("segment_max_bytes must be non-zero and no greater than max_total_bytes")]
    InvalidSegmentSize,
    #[error("control.max_ttl_ms is {actual}, exceeding the configured lease ceiling of {limit} ms")]
    ControlMaxTtlTooLarge { actual: u64, limit: u64 },
    #[error(
        "control.wait_timeout_ms is {actual}, exceeding the Run-start approval ceiling of {limit} ms"
    )]
    ControlWaitTimeoutTooLarge { actual: u64, limit: u64 },
    #[error("port at index {index} has invalid field {field}: {reason}")]
    InvalidPort {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("ports at indexes {first} and {second} refer to the same serial port")]
    DuplicatePort { first: usize, second: usize },
    #[error("configuration contains {actual} ports; the maximum is {limit}")]
    TooManyPorts { actual: usize, limit: usize },
    #[error("model profile at index {index} has invalid field {field}: {reason}")]
    InvalidModelProfile {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("model profiles at indexes {first} and {second} use the same name")]
    DuplicateModelProfileName { first: usize, second: usize },
    #[error("configuration contains {actual} model profiles; the maximum is {limit}")]
    TooManyModelProfiles { actual: usize, limit: usize },
    #[error("model family at index {index} has invalid field {field}: {reason}")]
    InvalidModelFamily {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("model families at indexes {first} and {second} use the same name")]
    DuplicateModelFamilyName { first: usize, second: usize },
    #[error("configuration contains {actual} model families; the maximum is {limit}")]
    TooManyModelFamilies { actual: usize, limit: usize },
    #[error("transport profile at index {index} has invalid field {field}: {reason}")]
    InvalidTransportProfile {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("transport profiles at indexes {first} and {second} use the same name")]
    DuplicateTransportProfileName { first: usize, second: usize },
    #[error("configuration contains {actual} transport profiles; the maximum is {limit}")]
    TooManyTransportProfiles { actual: usize, limit: usize },
    #[error(
        "port {port} references unknown transport profile {name:?}; available profiles: {available}"
    )]
    UnknownTransportProfile {
        port: String,
        name: String,
        available: String,
    },
    #[error(
        "port {port} references unknown model profile {name:?}; available profiles: {available}"
    )]
    UnknownModelProfile {
        port: String,
        name: String,
        available: String,
    },
    #[error("port {port} must bind model_family and model_name together")]
    IncompleteModelIdentity { port: String },
    #[error(
        "port {port} references unknown model family {name:?}; available families: {available}"
    )]
    UnknownModelFamily {
        port: String,
        name: String,
        available: String,
    },
    #[error(
        "port {port} references model {name:?}, which is not in family {family:?}; available models: {available}"
    )]
    UnknownModelName {
        port: String,
        family: String,
        name: String,
        available: String,
    },
}

fn validate_logging(logging: &LoggingConfig) -> Result<(), ConfigValidationError> {
    if logging.max_total_bytes == 0 {
        return Err(ConfigValidationError::InvalidLogCapacity);
    }
    if !(1..=99).contains(&logging.retention_target_percent) {
        return Err(ConfigValidationError::InvalidRetentionTarget);
    }
    if logging.segment_max_bytes == 0 || logging.segment_max_bytes > logging.max_total_bytes {
        return Err(ConfigValidationError::InvalidSegmentSize);
    }
    Ok(())
}

fn validate_control(control: &ControlConfig) -> Result<(), ConfigValidationError> {
    if control.max_ttl_ms > MAX_CONTROL_TTL_MS {
        return Err(ConfigValidationError::ControlMaxTtlTooLarge {
            actual: control.max_ttl_ms,
            limit: MAX_CONTROL_TTL_MS,
        });
    }
    let wait_limit_ms = MAX_CONTROL_WAIT_TIMEOUT.as_millis() as u64;
    if control.wait_timeout_ms > wait_limit_ms {
        return Err(ConfigValidationError::ControlWaitTimeoutTooLarge {
            actual: control.wait_timeout_ms,
            limit: wait_limit_ms,
        });
    }
    Ok(())
}

pub(crate) fn validate_ports(
    ports: &[SlotConfig],
    transport_profiles: &[TransportProfile],
    model_profiles: &[ModelProfile],
    model_families: &[ModelFamily],
) -> Result<(), ConfigValidationError> {
    if ports.len() > MAX_PORT_IDENTITIES_PER_DAEMON {
        return Err(ConfigValidationError::TooManyPorts {
            actual: ports.len(),
            limit: MAX_PORT_IDENTITIES_PER_DAEMON,
        });
    }
    let mut seen_ports: HashMap<String, usize> = HashMap::new();

    for (index, slot) in ports.iter().enumerate() {
        validate_text_field(index, "port", &slot.port, MAX_PORT_NAME_BYTES)?;
        if let Some(transport_profile) = slot.transport_profile.as_deref() {
            validate_profile(index, transport_profile)?;
            if !transport_profiles
                .iter()
                .any(|profile| profile.name == transport_profile)
            {
                let available = transport_profiles
                    .iter()
                    .map(|profile| profile.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(ConfigValidationError::UnknownTransportProfile {
                    port: slot.port.clone(),
                    name: transport_profile.to_owned(),
                    available: catalog_summary(available),
                });
            }
        }

        if let Some(model_profile) = slot.model_profile.as_deref()
            && !model_profiles
                .iter()
                .any(|profile| profile.name == model_profile)
        {
            let available = model_profiles
                .iter()
                .map(|profile| profile.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(ConfigValidationError::UnknownModelProfile {
                port: slot.port.clone(),
                name: model_profile.to_owned(),
                available: catalog_summary(available),
            });
        }

        match (slot.model_family.as_deref(), slot.model_name.as_deref()) {
            (None, None) => {}
            (Some(_), None) | (None, Some(_)) => {
                return Err(ConfigValidationError::IncompleteModelIdentity {
                    port: slot.port.clone(),
                });
            }
            (Some(model_family), Some(model_name)) => {
                validate_text_field(index, "model_family", model_family, MAX_MODEL_NAME_BYTES)?;
                validate_text_field(index, "model_name", model_name, MAX_MODEL_NAME_BYTES)?;
                let Some(family) = model_families
                    .iter()
                    .find(|family| family.name == model_family)
                else {
                    return Err(ConfigValidationError::UnknownModelFamily {
                        port: slot.port.clone(),
                        name: model_family.to_owned(),
                        available: catalog_summary(
                            model_families
                                .iter()
                                .map(|family| family.name.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                        ),
                    });
                };
                if !family.model_names.iter().any(|name| name == model_name) {
                    return Err(ConfigValidationError::UnknownModelName {
                        port: slot.port.clone(),
                        family: family.name.clone(),
                        name: model_name.to_owned(),
                        available: catalog_summary(family.model_names.join(", ")),
                    });
                }
            }
        }

        let port_key = port_identity_key(&slot.port);
        if let Some(first) = seen_ports.insert(port_key, index) {
            return Err(ConfigValidationError::DuplicatePort {
                first,
                second: index,
            });
        }
    }
    Ok(())
}

fn catalog_summary(names: String) -> String {
    if names.is_empty() {
        "(none configured)".to_owned()
    } else {
        names
    }
}

fn port_identity_key(port: &str) -> String {
    port_identity_key_for_platform(port, cfg!(windows))
}

fn port_identity_key_for_platform(port: &str, windows: bool) -> String {
    if windows {
        port.to_ascii_lowercase()
    } else {
        port.to_owned()
    }
}

pub(crate) fn validate_transport_profiles(
    profiles: &[TransportProfile],
) -> Result<(), ConfigValidationError> {
    if profiles.len() > MAX_TRANSPORT_PROFILES {
        return Err(ConfigValidationError::TooManyTransportProfiles {
            actual: profiles.len(),
            limit: MAX_TRANSPORT_PROFILES,
        });
    }
    let mut names: HashMap<&str, usize> = HashMap::new();
    for (index, profile) in profiles.iter().enumerate() {
        if profile.name.is_empty()
            || profile.name.len() > MAX_PROFILE_NAME_BYTES
            || profile.name != profile.name.trim()
            || profile.name.chars().any(char::is_control)
        {
            return Err(ConfigValidationError::InvalidTransportProfile {
                index,
                field: "name",
                reason: "must be a non-empty, trimmed name of at most 64 bytes",
            });
        }
        if let Some(first) = names.insert(&profile.name, index) {
            return Err(ConfigValidationError::DuplicateTransportProfileName {
                first,
                second: index,
            });
        }
        if !(50..=12_000_000).contains(&profile.baud_rate) {
            return Err(ConfigValidationError::InvalidTransportProfile {
                index,
                field: "baud_rate",
                reason: "must be between 50 and 12000000",
            });
        }
        if profile.flow_control == FlowControl::Hardware && profile.rts {
            return Err(ConfigValidationError::InvalidTransportProfile {
                index,
                field: "rts",
                reason: "must be false when hardware flow control owns RTS",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_model_profiles(
    profiles: &[ModelProfile],
) -> Result<(), ConfigValidationError> {
    if profiles.len() > MAX_MODEL_PROFILES {
        return Err(ConfigValidationError::TooManyModelProfiles {
            actual: profiles.len(),
            limit: MAX_MODEL_PROFILES,
        });
    }
    let mut names: HashMap<&str, usize> = HashMap::new();
    for (index, profile) in profiles.iter().enumerate() {
        if profile.name.is_empty()
            || profile.name.len() > MAX_PROFILE_NAME_BYTES
            || profile.name != profile.name.trim()
            || profile.name.chars().any(char::is_control)
        {
            return Err(ConfigValidationError::InvalidModelProfile {
                index,
                field: "name",
                reason: "must be a non-empty, trimmed name of at most 64 bytes",
            });
        }
        if let Some(first) = names.insert(&profile.name, index) {
            return Err(ConfigValidationError::DuplicateModelProfileName {
                first,
                second: index,
            });
        }
        for (field, pattern) in [
            ("shell_prompt", profile.shell_prompt.as_deref()),
            ("uboot_prompt", profile.uboot_prompt.as_deref()),
        ] {
            if pattern.is_some_and(|pattern| {
                pattern.is_empty()
                    || pattern.len() > MAX_COMMAND_CAPTURE_DETAIL_BYTES
                    || pattern.contains('\0')
            }) {
                return Err(ConfigValidationError::InvalidModelProfile {
                    index,
                    field,
                    reason: "must be non-empty, at most 4096 bytes, and contain no NUL",
                });
            }
        }
        if profile
            .write_eol
            .as_deref()
            .is_some_and(|eol| !matches!(eol, "" | "\r" | "\n" | "\r\n"))
        {
            return Err(ConfigValidationError::InvalidModelProfile {
                index,
                field: "write_eol",
                reason: "must be empty, CR, LF, or CRLF",
            });
        }
        if profile.write_chunk_size == Some(0) {
            return Err(ConfigValidationError::InvalidModelProfile {
                index,
                field: "write_chunk_size",
                reason: "must be greater than zero when configured",
            });
        }
        if profile
            .write_chunk_delay_ms
            .is_some_and(|delay| delay > 10_000)
        {
            return Err(ConfigValidationError::InvalidModelProfile {
                index,
                field: "write_chunk_delay_ms",
                reason: "must not exceed 10000 ms",
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_model_families(
    families: &[ModelFamily],
) -> Result<(), ConfigValidationError> {
    if families.len() > MAX_MODEL_FAMILIES {
        return Err(ConfigValidationError::TooManyModelFamilies {
            actual: families.len(),
            limit: MAX_MODEL_FAMILIES,
        });
    }
    let mut family_names: HashMap<&str, usize> = HashMap::new();
    for (index, family) in families.iter().enumerate() {
        if family.name.is_empty()
            || family.name.len() > MAX_MODEL_NAME_BYTES
            || family.name != family.name.trim()
            || family.name.chars().any(char::is_control)
        {
            return Err(ConfigValidationError::InvalidModelFamily {
                index,
                field: "name",
                reason: "must be a non-empty, trimmed name of at most 128 bytes",
            });
        }
        if let Some(first) = family_names.insert(&family.name, index) {
            return Err(ConfigValidationError::DuplicateModelFamilyName {
                first,
                second: index,
            });
        }
        if family.model_names.len() > MAX_MODEL_NAMES_PER_FAMILY {
            return Err(ConfigValidationError::InvalidModelFamily {
                index,
                field: "model_names",
                reason: "must contain at most 128 concrete model names",
            });
        }
        let mut model_names = HashSet::new();
        for model_name in &family.model_names {
            if model_name.is_empty()
                || model_name.len() > MAX_MODEL_NAME_BYTES
                || model_name != model_name.trim()
                || model_name.chars().any(char::is_control)
                || !model_names.insert(model_name)
            {
                return Err(ConfigValidationError::InvalidModelFamily {
                    index,
                    field: "model_names",
                    reason: "each model name must be non-empty, trimmed, unique, and at most 128 bytes",
                });
            }
        }
    }
    Ok(())
}

fn validate_profile(index: usize, profile: &str) -> Result<(), ConfigValidationError> {
    if profile.is_empty()
        || profile.len() > MAX_PROFILE_NAME_BYTES
        || profile != profile.trim()
        || profile.chars().any(char::is_control)
    {
        Err(invalid_port(
            index,
            "profile",
            "must be a non-empty, trimmed name of at most 64 bytes",
        ))
    } else {
        Ok(())
    }
}

fn validate_text_field(
    index: usize,
    field: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), ConfigValidationError> {
    if value.is_empty()
        || value.len() > max_bytes
        || value != value.trim()
        || value.chars().any(char::is_control)
    {
        Err(invalid_port(
            index,
            field,
            "must be non-empty, trimmed, bounded text without control characters",
        ))
    } else {
        Ok(())
    }
}

fn invalid_port(index: usize, field: &'static str, reason: &'static str) -> ConfigValidationError {
    ConfigValidationError::InvalidPort {
        index,
        field,
        reason,
    }
}

fn io_error(path: &Path, source: io::Error) -> ConfigError {
    ConfigError::Io {
        path: path.to_path_buf(),
        source,
    }
}

pub(crate) fn atomic_write(target: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = target.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "configuration has no parent")
    })?;
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("seriald.toml");

    let mut attempted_paths = HashSet::new();
    let (temporary_path, mut temporary) = loop {
        let candidate = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4().simple()));
        if !attempted_paths.insert(candidate.clone()) {
            continue;
        }
        match open_private_temporary(&candidate) {
            Ok(file) => break (candidate, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };

    let result = (|| {
        temporary.write_all(contents)?;
        temporary.sync_all()?;
        drop(temporary);
        replace_file(&temporary_path, target)?;
        sync_parent_directory(parent)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

/// Publishes fully-written contents only when the target is still absent.
/// Hard-link creation is atomic within one directory, so concurrent first
/// launchers all load the one winning installation identity.
fn atomic_create(target: &Path, contents: &[u8]) -> io::Result<bool> {
    let parent = target.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "configuration has no parent")
    })?;
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("seriald.toml");
    let temporary_path = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4().simple()));
    let mut temporary = open_private_temporary(&temporary_path)?;
    let result = (|| {
        temporary.write_all(contents)?;
        temporary.sync_all()?;
        drop(temporary);
        match fs::hard_link(&temporary_path, target) {
            Ok(()) => {
                sync_parent_directory(parent)?;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(error) => Err(error),
        }
    })();
    let _ = fs::remove_file(&temporary_path);
    result
}

fn open_private_temporary(path: &Path) -> io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(windows)]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    use std::{iter, os::windows::ffi::OsStrExt as _};

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x8;

    #[link(name = "Kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(
            existing_file_name: *const u16,
            new_file_name: *const u16,
            flags: u32,
        ) -> i32;
    }

    let source: Vec<u16> = source
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    let target: Vec<u16> = target
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();
    // SAFETY: both pointers refer to NUL-terminated UTF-16 buffers that remain
    // alive for the duration of the call. Flags request an atomic replacement
    // on the same volume and ask Windows to flush it before returning.
    let replaced = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if replaced == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(windows)]
fn is_windows_replace_contention(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(5 | 32 | 33))
}

#[cfg(not(windows))]
fn replace_file(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> io::Result<()> {
    fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_parent: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn restrict_directory_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn restrict_directory_permissions(_path: &Path) -> io::Result<()> {
    // Windows user-profile directories inherit the user's ACL. ACL management
    // remains an installer/service responsibility rather than shelling out.
    Ok(())
}

#[cfg(unix)]
fn restrict_config_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict_config_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{DataBits, EchoMode, Parity, StopBits};

    fn transport_profile(name: &str) -> TransportProfile {
        TransportProfile {
            name: name.into(),
            baud_rate: 115_200,
            data_bits: DataBits::Eight,
            parity: Parity::None,
            stop_bits: StopBits::One,
            flow_control: FlowControl::None,
            dtr: false,
            rts: false,
            auto_open: true,
        }
    }

    fn model_profile(name: &str) -> ModelProfile {
        ModelProfile {
            name: name.into(),
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: Some("U-Boot> ".into()),
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        }
    }

    fn model_family(name: &str) -> ModelFamily {
        ModelFamily {
            name: name.into(),
            model_names: vec!["TL-AS7230-W 1.0".into(), "TL-AS7230-F4GE 1.0".into()],
        }
    }

    fn slot(port: &str) -> SlotConfig {
        SlotConfig {
            port: port.into(),
            transport_profile: None,
            model_profile: None,
            model_family: None,
            model_name: None,
            enabled: false,
        }
    }

    fn schema2_config() -> DaemonConfigV2 {
        DaemonConfigV2 {
            schema_version: 2,
            config_revision: 37,
            server_id: Uuid::parse_str("842d93cb-5dee-47fe-8453-0583e878497d").unwrap(),
            bind: "127.0.0.1:4321".parse().unwrap(),
            logging: LoggingConfig {
                max_total_bytes: 12 * GIB,
                retention_target_percent: 83,
                segment_max_bytes: 2 * 1024 * 1024,
            },
            control: ControlConfig {
                max_ttl_ms: 90_000,
                wait_timeout_ms: 75_000,
                max_waiters: 17,
            },
            ports: vec![
                SlotConfigV2 {
                    port: "COM4".into(),
                    transport_profile: Some("uart-fast".into()),
                    model_profile: Some("TL-AS7230".into()),
                    model_name: Some("TL-AS7230-W 1.0".into()),
                    enabled: true,
                },
                SlotConfigV2 {
                    port: "COM5".into(),
                    transport_profile: None,
                    model_profile: Some("generic-shell".into()),
                    model_name: None,
                    enabled: false,
                },
                SlotConfigV2 {
                    port: "COM6".into(),
                    transport_profile: None,
                    model_profile: None,
                    model_name: None,
                    enabled: true,
                },
            ],
            transport_profiles: vec![transport_profile("uart-fast")],
            model_profiles: vec![
                ModelProfileV2 {
                    name: "TL-AS7230".into(),
                    model_names: Some(vec!["TL-AS7230-W 1.0".into(), "TL-AS7230-F4GE 1.0".into()]),
                    shell_prompt: Some("/ # ".into()),
                    uboot_prompt: Some("U-Boot> ".into()),
                    write_eol: Some("\r".into()),
                    echo: Some(EchoMode::Auto),
                    write_chunk_size: Some(7),
                    write_chunk_delay_ms: Some(13),
                },
                ModelProfileV2 {
                    name: "generic-shell".into(),
                    model_names: None,
                    shell_prompt: Some("# ".into()),
                    uboot_prompt: None,
                    write_eol: Some("\n".into()),
                    echo: Some(EchoMode::On),
                    write_chunk_size: None,
                    write_chunk_delay_ms: None,
                },
            ],
        }
    }

    fn write_schema2(store: &ConfigStore, config: &DaemonConfigV2) -> String {
        store.ensure_directories().unwrap();
        let source = toml::to_string_pretty(config).unwrap();
        fs::write(&store.paths.config_file, &source).unwrap();
        source
    }

    #[test]
    fn fresh_configuration_is_token_free_and_works_on_lan_bindings() {
        let mut config = DaemonConfig::generate();
        config.bind = "0.0.0.0:3210".parse().unwrap();
        config.validate().unwrap();
        let serialized = toml::to_string_pretty(&config).unwrap();
        assert!(!serialized.contains("auth"));
        assert!(!serialized.contains("token"));
    }

    #[test]
    fn port_is_the_only_identity_and_unix_device_paths_are_valid() {
        let mut config = DaemonConfig::generate();
        config.ports = vec![slot("/dev/cu.usbserial-210")];
        config.validate().unwrap();
        assert_eq!(config.ports[0].port, "/dev/cu.usbserial-210");
    }

    #[test]
    fn port_identity_is_case_insensitive_on_windows_and_exact_on_unix() {
        assert_eq!(
            port_identity_key_for_platform("COM4", true),
            port_identity_key_for_platform("com4", true)
        );
        assert_ne!(
            port_identity_key_for_platform("COM4", false),
            port_identity_key_for_platform("com4", false)
        );

        let mut config = DaemonConfig::generate();
        config.ports = vec![slot("COM4"), slot("com4")];
        if cfg!(windows) {
            assert!(matches!(
                config.validate(),
                Err(ConfigValidationError::DuplicatePort { .. })
            ));
        } else {
            config.validate().unwrap();
        }
    }

    #[test]
    fn port_profile_references_must_resolve() {
        let mut config = DaemonConfig::generate();
        config.transport_profiles = vec![transport_profile("uart")];
        config.model_profiles = vec![model_profile("TL-AS7230 1.0")];
        let mut configured = slot("COM4");
        configured.transport_profile = Some("uart".into());
        configured.model_profile = Some("TL-AS7230 1.0".into());
        config.ports = vec![configured];
        config.validate().unwrap();

        config.ports[0].model_profile = Some("missing".into());
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::UnknownModelProfile { .. })
        ));
    }

    #[test]
    fn concrete_model_name_must_belong_to_the_selected_family() {
        let mut config = DaemonConfig::generate();
        config.model_profiles = vec![model_profile("TL-AS7230")];
        config.model_families = vec![model_family("TL-AS7230")];
        let mut configured = slot("COM4");
        configured.model_profile = Some("TL-AS7230".into());
        configured.model_family = Some("TL-AS7230".into());
        configured.model_name = Some("TL-AS7230-W 1.0".into());
        config.ports = vec![configured];
        config.validate().unwrap();

        config.ports[0].model_name = Some("TL-AS9999".into());
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::UnknownModelName { .. })
        ));

        config.ports[0].model_family = None;
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::IncompleteModelIdentity { .. })
        ));
    }

    #[test]
    fn model_identity_is_independent_from_interaction_profile() {
        let mut config = DaemonConfig::generate();
        config.model_profiles = vec![model_profile("shared-shell")];
        config.model_families = vec![model_family("TL-AS7230")];
        let mut configured = slot("COM4");
        configured.model_profile = Some("shared-shell".into());
        configured.model_family = Some("TL-AS7230".into());
        configured.model_name = Some("TL-AS7230-W 1.0".into());
        config.ports = vec![configured];
        config.validate().unwrap();
    }

    #[test]
    fn model_family_replacement_is_revisioned_and_preserves_bound_names() {
        let mut config = DaemonConfig::generate();
        config.model_families = vec![model_family("TL-AS7230")];
        let mut configured = slot("COM4");
        configured.model_family = Some("TL-AS7230".into());
        configured.model_name = Some("TL-AS7230-W 1.0".into());
        config.ports = vec![configured];
        let previous_revision = config.config_revision;

        let staged = config
            .staged_with_model_families(config.model_families.clone())
            .unwrap();
        assert_eq!(staged.config_revision, previous_revision + 1);

        let missing_bound_name = vec![ModelFamily {
            name: "TL-AS7230".into(),
            model_names: vec!["TL-AS7230-F4GE 1.0".into()],
        }];
        assert!(matches!(
            config.staged_with_model_families(missing_bound_name),
            Err(ConfigValidationError::UnknownModelName { .. })
        ));
    }

    #[test]
    fn store_creates_and_reloads_the_clean_schema() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let created = store.load_or_create().unwrap();
        assert_eq!(created.config.schema_version, CONFIG_SCHEMA_VERSION);
        let loaded = store.load().unwrap();
        assert_eq!(loaded.server_id, created.config.server_id);
    }

    #[test]
    fn schema2_migration_preserves_configuration_and_splits_model_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let legacy = schema2_config();
        let original = write_schema2(&store, &legacy);

        let loaded = store.load_or_create().unwrap().config;

        assert_eq!(loaded.schema_version, CONFIG_SCHEMA_VERSION);
        assert_eq!(loaded.config_revision, legacy.config_revision);
        assert_eq!(loaded.server_id, legacy.server_id);
        assert_eq!(loaded.bind, legacy.bind);
        assert_eq!(loaded.logging, legacy.logging);
        assert_eq!(loaded.control, legacy.control);
        assert_eq!(loaded.transport_profiles, legacy.transport_profiles);
        assert_eq!(loaded.ports.len(), 3);
        assert_eq!(loaded.ports[0].model_profile.as_deref(), Some("TL-AS7230"));
        assert_eq!(loaded.ports[0].model_family.as_deref(), Some("TL-AS7230"));
        assert_eq!(
            loaded.ports[0].model_name.as_deref(),
            Some("TL-AS7230-W 1.0")
        );
        assert_eq!(
            loaded.ports[1].model_profile.as_deref(),
            Some("generic-shell")
        );
        assert!(loaded.ports[1].model_family.is_none());
        assert!(loaded.ports[1].model_name.is_none());
        assert!(loaded.ports[2].model_profile.is_none());
        assert!(loaded.ports[2].model_family.is_none());
        assert!(loaded.ports[2].model_name.is_none());

        assert_eq!(loaded.model_profiles.len(), 2);
        assert_eq!(loaded.model_profiles[0].name, "TL-AS7230");
        assert_eq!(
            loaded.model_profiles[0].shell_prompt.as_deref(),
            Some("/ # ")
        );
        assert_eq!(loaded.model_profiles[0].write_chunk_size, Some(7));
        assert_eq!(loaded.model_profiles[0].write_chunk_delay_ms, Some(13));
        assert_eq!(loaded.model_families.len(), 2);
        assert_eq!(loaded.model_families[0].name, "TL-AS7230");
        assert_eq!(
            loaded.model_families[0].model_names,
            ["TL-AS7230-W 1.0", "TL-AS7230-F4GE 1.0"]
        );
        assert_eq!(loaded.model_families[1].name, "generic-shell");
        assert!(loaded.model_families[1].model_names.is_empty());
        assert_eq!(
            fs::read_to_string(store.schema2_backup_path()).unwrap(),
            original
        );

        let persisted = fs::read_to_string(&store.paths.config_file).unwrap();
        assert!(persisted.contains("schema_version = 3"));
        assert_eq!(store.load().unwrap().server_id, legacy.server_id);
    }

    #[test]
    fn released_schema2_profile_identity_is_preserved_without_extended_fields() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut legacy = schema2_config();
        legacy.model_profiles = vec![ModelProfileV2 {
            name: "TL-AS7230-W 1.0".into(),
            model_names: None,
            shell_prompt: Some("/ # ".into()),
            uboot_prompt: None,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: Some(1),
            write_chunk_delay_ms: Some(1),
        }];
        legacy.ports = vec![
            SlotConfigV2 {
                port: "COM4".into(),
                transport_profile: Some("uart-fast".into()),
                model_profile: Some("TL-AS7230-W 1.0".into()),
                model_name: None,
                enabled: true,
            },
            SlotConfigV2 {
                port: "COM5".into(),
                transport_profile: None,
                model_profile: None,
                model_name: None,
                enabled: false,
            },
        ];
        write_schema2(&store, &legacy);

        let migrated = store.load_or_create().unwrap().config;

        assert_eq!(
            migrated.model_families,
            vec![ModelFamily {
                name: "TL-AS7230-W 1.0".into(),
                model_names: vec!["TL-AS7230-W 1.0".into()],
            }]
        );
        assert_eq!(
            migrated.ports[0].model_profile.as_deref(),
            Some("TL-AS7230-W 1.0")
        );
        assert_eq!(
            migrated.ports[0].model_family.as_deref(),
            Some("TL-AS7230-W 1.0")
        );
        assert_eq!(
            migrated.ports[0].model_name.as_deref(),
            Some("TL-AS7230-W 1.0")
        );
        assert!(migrated.ports[1].model_family.is_none());
        assert!(migrated.ports[1].model_name.is_none());
    }

    #[test]
    fn schema2_migration_preserves_maximum_revision() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut legacy = schema2_config();
        legacy.config_revision = u64::MAX;
        write_schema2(&store, &legacy);

        let migrated = store.load_or_create().unwrap().config;

        assert_eq!(migrated.config_revision, u64::MAX);
        assert!(matches!(
            migrated.staged_with_ports(migrated.ports.clone()),
            Err(ConfigValidationError::RevisionExhausted)
        ));
    }

    #[test]
    fn schema2_migration_preserves_the_default_revision_when_it_was_omitted() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let source = toml::to_string_pretty(&schema2_config()).unwrap().replacen(
            "config_revision = 37\n",
            "",
            1,
        );
        store.ensure_directories().unwrap();
        fs::write(&store.paths.config_file, source).unwrap();

        let migrated = store.load_or_create().unwrap().config;

        assert_eq!(migrated.config_revision, default_config_revision());
    }

    #[test]
    fn explicit_empty_model_names_selects_the_extended_schema2_shape() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut legacy = schema2_config();
        legacy.model_profiles = vec![ModelProfileV2 {
            name: "generic-shell".into(),
            model_names: Some(Vec::new()),
            shell_prompt: Some("# ".into()),
            uboot_prompt: None,
            write_eol: Some("\r".into()),
            echo: Some(EchoMode::Auto),
            write_chunk_size: None,
            write_chunk_delay_ms: None,
        }];
        legacy.ports = vec![SlotConfigV2 {
            port: "COM4".into(),
            transport_profile: None,
            model_profile: Some("generic-shell".into()),
            model_name: None,
            enabled: true,
        }];
        let source = write_schema2(&store, &legacy);
        assert!(source.contains("model_names = []"));

        let migrated = store.load_or_create().unwrap().config;

        assert!(migrated.model_families[0].model_names.is_empty());
        assert_eq!(
            migrated.ports[0].model_profile.as_deref(),
            Some("generic-shell")
        );
        assert!(migrated.ports[0].model_family.is_none());
        assert!(migrated.ports[0].model_name.is_none());
    }

    #[test]
    fn schema2_load_is_read_only_until_startup_persists_the_migration() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());

        let loaded = store.load().unwrap();

        assert_eq!(loaded.schema_version, CONFIG_SCHEMA_VERSION);
        assert_eq!(
            fs::read_to_string(&store.paths.config_file).unwrap(),
            original
        );
        assert!(!store.schema2_backup_path().exists());
    }

    #[test]
    fn persisted_schema2_migration_is_idempotent() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        write_schema2(&store, &schema2_config());
        store.load_or_create().unwrap();
        let first_config = fs::read(&store.paths.config_file).unwrap();
        let first_backup = fs::read(store.schema2_backup_path()).unwrap();

        store.load_or_create().unwrap();

        assert_eq!(fs::read(&store.paths.config_file).unwrap(), first_config);
        assert_eq!(fs::read(store.schema2_backup_path()).unwrap(), first_backup);
    }

    #[test]
    fn schema2_migration_uses_a_numbered_path_when_the_base_backup_conflicts() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        let sentinel = b"existing schema-2 backup";
        fs::write(store.schema2_backup_path(), sentinel).unwrap();

        store.load_or_create().unwrap();

        assert_eq!(fs::read(store.schema2_backup_path()).unwrap(), sentinel);
        assert_eq!(
            fs::read_to_string(store.schema2_backup_path_at(1)).unwrap(),
            original
        );
        assert_eq!(store.load().unwrap().schema_version, CONFIG_SCHEMA_VERSION);
    }

    #[test]
    fn schema2_migration_reuses_an_identical_existing_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        fs::write(store.schema2_backup_path(), &original).unwrap();

        store.load_or_create().unwrap();

        assert_eq!(
            fs::read_to_string(store.schema2_backup_path()).unwrap(),
            original
        );
        assert!(!store.schema2_backup_path_at(1).exists());
    }

    #[test]
    fn schema2_migration_does_not_reuse_a_differently_sized_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        let conflicting = vec![b'x'; original.len() + 1];
        fs::write(store.schema2_backup_path(), &conflicting).unwrap();

        store.load_or_create().unwrap();

        assert_eq!(fs::read(store.schema2_backup_path()).unwrap(), conflicting);
        assert_eq!(
            fs::read_to_string(store.schema2_backup_path_at(1)).unwrap(),
            original
        );
    }

    #[cfg(unix)]
    #[test]
    fn schema2_migration_never_treats_a_symlink_as_the_backup() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        symlink(&store.paths.config_file, store.schema2_backup_path()).unwrap();

        store.load_or_create().unwrap();

        assert!(
            fs::symlink_metadata(store.schema2_backup_path())
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(
            fs::read_to_string(store.schema2_backup_path_at(1)).unwrap(),
            original
        );
        assert_eq!(store.load().unwrap().schema_version, CONFIG_SCHEMA_VERSION);
    }

    #[test]
    fn concurrent_schema2_migrators_converge_without_overwriting_the_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        let workers = 8;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.load_or_create().map(|loaded| loaded.config)
                })
            })
            .collect::<Vec<_>>();

        let migrated = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();

        assert!(
            migrated
                .iter()
                .all(|config| config.schema_version == CONFIG_SCHEMA_VERSION)
        );
        assert!(migrated.iter().all(|config| config.config_revision == 37));
        assert_eq!(
            fs::read_to_string(store.schema2_backup_path()).unwrap(),
            original
        );
        assert_eq!(store.load().unwrap().schema_version, CONFIG_SCHEMA_VERSION);
    }

    #[test]
    fn unsupported_schema_is_rejected_without_modifying_the_file() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut config = schema2_config();
        config.schema_version = CONFIG_SCHEMA_VERSION + 1;
        let original = write_schema2(&store, &config);

        assert!(matches!(
            store.load_or_create(),
            Err(ConfigError::Validation(
                ConfigValidationError::UnsupportedSchemaVersion(version)
            )) if version == CONFIG_SCHEMA_VERSION + 1
        ));
        assert_eq!(
            fs::read_to_string(&store.paths.config_file).unwrap(),
            original
        );
        assert!(!store.schema2_backup_path().exists());
    }

    #[test]
    fn invalid_schema2_is_rejected_without_modifying_or_backing_up_the_file() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut config = schema2_config();
        config.ports[0].model_name = Some("not-in-the-profile".into());
        let original = write_schema2(&store, &config);

        assert!(matches!(
            store.load_or_create(),
            Err(ConfigError::Validation(
                ConfigValidationError::UnknownModelName { .. }
            ))
        ));
        assert_eq!(
            fs::read_to_string(&store.paths.config_file).unwrap(),
            original
        );
        assert!(!store.schema2_backup_path().exists());
    }

    #[cfg(unix)]
    #[test]
    fn failed_schema2_replacement_leaves_the_original_file_intact() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let original = write_schema2(&store, &schema2_config());
        fs::write(store.schema2_backup_path(), &original).unwrap();
        let parsed = store.read_existing().unwrap();
        fs::set_permissions(&store.paths.config_dir, fs::Permissions::from_mode(0o500)).unwrap();

        let result = store
            .persist_schema2_migration(parsed.schema2_source.as_deref().unwrap(), &parsed.config);

        fs::set_permissions(&store.paths.config_dir, fs::Permissions::from_mode(0o700)).unwrap();
        assert!(matches!(result, Err(ConfigError::Io { .. })));
        assert_eq!(
            fs::read_to_string(&store.paths.config_file).unwrap(),
            original
        );
    }

    #[test]
    fn concurrent_first_loaders_share_one_generated_server_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let workers = 12;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(workers));
        let handles = (0..workers)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.load_or_create().unwrap().config.server_id
                })
            })
            .collect::<Vec<_>>();
        let identities = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        assert!(
            identities
                .iter()
                .all(|server_id| *server_id == identities[0])
        );
        assert_eq!(store.load().unwrap().server_id, identities[0]);
    }
}
