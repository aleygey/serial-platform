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
use serial_protocol::{FlowControl, ModelProfile, SlotConfig, TransportProfile};
use thiserror::Error;
use uuid::Uuid;

use crate::control::{
    ControlLimits, MAX_CONTROL_TTL_MS, MAX_CONTROL_WAIT_TIMEOUT, MAX_TTL_MS, MAX_WAITERS,
    WAIT_TIMEOUT,
};

pub const CONFIG_SCHEMA_VERSION: u32 = 2;
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
const MAX_PROMPT_PATTERN_BYTES: usize = 4096;

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
    /// Lifetime of a queued acquire request before it is dropped.
    pub wait_timeout_ms: u64,
    /// Bound for the per-slot control wait queue.
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
        validate_ports(&self.ports, &self.transport_profiles, &self.model_profiles)
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
    /// one. Existing unreadable or invalid files are never overwritten.
    pub fn load_or_create(&self) -> Result<LoadedConfig, ConfigError> {
        self.ensure_directories()?;

        let config = if self.paths.config_file.exists() {
            self.load()?
        } else {
            let config = DaemonConfig::generate();
            config.validate()?;
            self.save(&config)?;
            config
        };

        Ok(LoadedConfig {
            config,
            daemon_epoch: Uuid::new_v4(),
            paths: self.paths.clone(),
        })
    }

    /// Loads and validates an existing configuration without creating one.
    pub fn load(&self) -> Result<DaemonConfig, ConfigError> {
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
        let serialized = fs::read_to_string(&self.paths.config_file)
            .map_err(|source| io_error(&self.paths.config_file, source))?;
        let config: DaemonConfig =
            toml::from_str(&serialized).map_err(|_| ConfigError::InvalidToml {
                path: self.paths.config_file.clone(),
            })?;
        config.validate()?;
        Ok(config)
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
        "control.wait_timeout_ms is {actual}, exceeding the queued-acquire ceiling of {limit} ms"
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
                    || pattern.len() > MAX_PROMPT_PATTERN_BYTES
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

    fn slot(port: &str) -> SlotConfig {
        SlotConfig {
            port: port.into(),
            transport_profile: None,
            model_profile: None,
            enabled: false,
        }
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
    fn store_creates_and_reloads_the_clean_schema() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let created = store.load_or_create().unwrap();
        assert_eq!(created.config.schema_version, CONFIG_SCHEMA_VERSION);
        let loaded = store.load().unwrap();
        assert_eq!(loaded.server_id, created.config.server_id);
    }
}
