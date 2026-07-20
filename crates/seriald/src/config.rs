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
};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use serial_protocol::{DeviceProfile, FlowControl, SlotConfig};
use thiserror::Error;
use uuid::Uuid;

use crate::auth::{AuthConfig, AuthError, CredentialDisplay};

pub const CONFIG_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_PORT: u16 = 3210;
pub const GIB: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_MAX_LOG_BYTES: u64 = 10 * GIB;
pub const DEFAULT_RETENTION_TARGET_PERCENT: u8 = 90;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Hard bound for both one active configuration and the number of distinct
/// Slot identities retained during one daemon epoch.
pub const MAX_SLOT_IDENTITIES_PER_DAEMON: usize = 128;
/// Hard bound for the device-model profile catalog.
pub const MAX_DEVICE_PROFILES: usize = 128;

const MAX_CONFIG_FILE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_SLOT_ID_BYTES: usize = 64;
const MAX_DISPLAY_NAME_BYTES: usize = 128;
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

/// Values persisted in `seriald.toml`.
///
/// `Debug` is safe because [`AuthConfig`] recursively redacts every token.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonConfig {
    pub schema_version: u32,
    pub server_id: Uuid,
    pub bind: SocketAddr,
    #[serde(default)]
    pub logging: LoggingConfig,
    pub auth: AuthConfig,
    #[serde(default)]
    pub slots: Vec<SlotConfig>,
    #[serde(default)]
    pub device_profiles: Vec<DeviceProfile>,
}

impl DaemonConfig {
    fn generate() -> (Self, CredentialDisplay) {
        let (auth, credentials) = AuthConfig::generate();
        (
            Self {
                schema_version: CONFIG_SCHEMA_VERSION,
                server_id: Uuid::new_v4(),
                bind: default_bind_address(),
                logging: LoggingConfig::default(),
                auth,
                slots: Vec::new(),
                device_profiles: Vec::new(),
            },
            credentials,
        )
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
        self.auth.validate()?;
        validate_device_profiles(&self.device_profiles)?;
        validate_slots(&self.slots, &self.device_profiles)
    }

    /// Replaces all Slot configuration in memory after validating the complete
    /// resulting daemon configuration.
    pub fn replace_slots(&mut self, slots: Vec<SlotConfig>) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.slots, slots);
        if let Err(error) = self.validate() {
            self.slots = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Builds a fully validated candidate without changing the live in-memory
    /// configuration. Runtime and persistence layers can then commit it in
    /// their own transaction order.
    pub fn staged_with_slots(&self, slots: Vec<SlotConfig>) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_slots(slots)?;
        Ok(staged)
    }

    /// Replaces the device profile catalog in memory after validating the
    /// complete resulting daemon configuration, including every Slot's
    /// profile reference.
    pub fn replace_device_profiles(
        &mut self,
        device_profiles: Vec<DeviceProfile>,
    ) -> Result<(), ConfigValidationError> {
        let previous = std::mem::replace(&mut self.device_profiles, device_profiles);
        if let Err(error) = self.validate() {
            self.device_profiles = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Builds a fully validated candidate with a replaced device profile
    /// catalog without changing the live in-memory configuration.
    pub fn staged_with_device_profiles(
        &self,
        device_profiles: Vec<DeviceProfile>,
    ) -> Result<Self, ConfigValidationError> {
        let mut staged = self.clone();
        staged.replace_device_profiles(device_profiles)?;
        Ok(staged)
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
    /// Present only when this call created the first configuration file.
    pub initial_credentials: Option<CredentialDisplay>,
}

impl fmt::Debug for LoadedConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoadedConfig")
            .field("config", &self.config)
            .field("daemon_epoch", &self.daemon_epoch)
            .field("paths", &self.paths)
            .field(
                "initial_credentials",
                &self.initial_credentials.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Owns configuration I/O. Constructing a store does not touch the filesystem.
#[derive(Clone, Debug)]
pub struct ConfigStore {
    paths: ConfigPaths,
    #[cfg(test)]
    fail_saves: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ConfigStore {
    pub fn platform_default() -> Result<Self, ConfigError> {
        Ok(Self::new(ConfigPaths::platform_default()?))
    }

    #[must_use]
    pub fn new(paths: ConfigPaths) -> Self {
        Self {
            paths,
            #[cfg(test)]
            fail_saves: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    #[must_use]
    pub fn paths(&self) -> &ConfigPaths {
        &self.paths
    }

    /// Loads an existing valid configuration or atomically creates the first
    /// one. Existing unreadable or invalid files are never overwritten.
    pub fn load_or_create(&self) -> Result<LoadedConfig, ConfigError> {
        self.ensure_directories()?;

        let (config, initial_credentials) = if self.paths.config_file.exists() {
            (self.load()?, None)
        } else {
            let (config, credentials) = DaemonConfig::generate();
            config.validate()?;
            self.save(&config)?;
            (config, Some(credentials))
        };

        Ok(LoadedConfig {
            config,
            daemon_epoch: Uuid::new_v4(),
            paths: self.paths.clone(),
            initial_credentials,
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
        // Do not retain the parser's source error: TOML diagnostics can include
        // the offending line, which could be a plaintext bearer token.
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
        #[cfg(test)]
        if self.fail_saves.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(io_error(
                &self.paths.config_file,
                io::Error::other("forced configuration save failure"),
            ));
        }
        self.ensure_directories()?;
        // Serialization errors are deliberately sanitized for the same reason
        // as parse errors: configuration contains bearer credentials.
        let serialized = toml::to_string_pretty(config).map_err(|_| ConfigError::Serialization)?;
        atomic_write(&self.paths.config_file, serialized.as_bytes())
            .map_err(|source| io_error(&self.paths.config_file, source))
    }

    /// Persists a validated Slot replacement and only then commits it to the
    /// caller's in-memory configuration. A failed write leaves both unchanged.
    pub fn update_slots(
        &self,
        current: &mut DaemonConfig,
        slots: Vec<SlotConfig>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_slots(slots)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    /// Persists a validated device profile catalog replacement and only then
    /// commits it to the caller's in-memory configuration.
    pub fn update_device_profiles(
        &self,
        current: &mut DaemonConfig,
        device_profiles: Vec<DeviceProfile>,
    ) -> Result<(), ConfigError> {
        let updated = current.staged_with_device_profiles(device_profiles)?;
        self.save(&updated)?;
        *current = updated;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_save_failure(&self, fail: bool) {
        self.fail_saves
            .store(fail, std::sync::atomic::Ordering::SeqCst);
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
    #[error("max_total_bytes must be non-zero")]
    InvalidLogCapacity,
    #[error("retention_target_percent must be between 1 and 99")]
    InvalidRetentionTarget,
    #[error("segment_max_bytes must be non-zero and no greater than max_total_bytes")]
    InvalidSegmentSize,
    #[error("authentication configuration is invalid: {0}")]
    Authentication(#[from] AuthError),
    #[error("Slot at index {index} has invalid field {field}: {reason}")]
    InvalidSlot {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("Slots at indexes {first} and {second} use the same id")]
    DuplicateSlotId { first: usize, second: usize },
    #[error("Slots at indexes {first} and {second} use the same physical port")]
    DuplicatePort { first: usize, second: usize },
    #[error("configuration contains {actual} Slots; the maximum is {limit}")]
    TooManySlots { actual: usize, limit: usize },
    #[error("device profile at index {index} has invalid field {field}: {reason}")]
    InvalidDeviceProfile {
        index: usize,
        field: &'static str,
        reason: &'static str,
    },
    #[error("device profiles at indexes {first} and {second} use the same name")]
    DuplicateDeviceProfileName { first: usize, second: usize },
    #[error("configuration contains {actual} device profiles; the maximum is {limit}")]
    TooManyDeviceProfiles { actual: usize, limit: usize },
    #[error(
        "Slot {slot_id} references unknown device profile {name:?}; available profiles: {available}"
    )]
    UnknownDeviceProfile {
        slot_id: String,
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

pub(crate) fn validate_slots(
    slots: &[SlotConfig],
    device_profiles: &[DeviceProfile],
) -> Result<(), ConfigValidationError> {
    if slots.len() > MAX_SLOT_IDENTITIES_PER_DAEMON {
        return Err(ConfigValidationError::TooManySlots {
            actual: slots.len(),
            limit: MAX_SLOT_IDENTITIES_PER_DAEMON,
        });
    }
    let mut ids: HashMap<&str, usize> = HashMap::new();
    let mut ports: HashMap<String, usize> = HashMap::new();

    for (index, slot) in slots.iter().enumerate() {
        validate_slot_id(index, &slot.id)?;
        validate_text_field(
            index,
            "display_name",
            &slot.display_name,
            MAX_DISPLAY_NAME_BYTES,
        )?;
        validate_text_field(index, "port", &slot.port, MAX_PORT_NAME_BYTES)?;
        validate_profile(index, &slot.profile)?;
        validate_serial_settings(index, slot)?;

        if let Some(device_profile) = slot.device_profile.as_deref() {
            if !device_profiles
                .iter()
                .any(|profile| profile.name == device_profile)
            {
                let available = device_profiles
                    .iter()
                    .map(|profile| profile.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(ConfigValidationError::UnknownDeviceProfile {
                    slot_id: slot.id.clone(),
                    name: device_profile.to_owned(),
                    available: if available.is_empty() {
                        "(none configured)".to_owned()
                    } else {
                        available
                    },
                });
            }
        }

        if let Some(first) = ids.insert(&slot.id, index) {
            return Err(ConfigValidationError::DuplicateSlotId {
                first,
                second: index,
            });
        }
        // Windows COM names are case-insensitive. Applying the same rule on
        // every platform keeps portable configuration deterministic.
        let port_key = slot.port.to_ascii_lowercase();
        if let Some(first) = ports.insert(port_key, index) {
            return Err(ConfigValidationError::DuplicatePort {
                first,
                second: index,
            });
        }
    }
    Ok(())
}

pub(crate) fn validate_device_profiles(
    profiles: &[DeviceProfile],
) -> Result<(), ConfigValidationError> {
    if profiles.len() > MAX_DEVICE_PROFILES {
        return Err(ConfigValidationError::TooManyDeviceProfiles {
            actual: profiles.len(),
            limit: MAX_DEVICE_PROFILES,
        });
    }
    let mut names: HashMap<&str, usize> = HashMap::new();
    for (index, profile) in profiles.iter().enumerate() {
        if profile.name.is_empty()
            || profile.name.len() > MAX_PROFILE_NAME_BYTES
            || profile.name != profile.name.trim()
            || profile.name.chars().any(char::is_control)
        {
            return Err(ConfigValidationError::InvalidDeviceProfile {
                index,
                field: "name",
                reason: "must be a non-empty, trimmed name of at most 64 bytes",
            });
        }
        if let Some(first) = names.insert(&profile.name, index) {
            return Err(ConfigValidationError::DuplicateDeviceProfileName {
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
                return Err(ConfigValidationError::InvalidDeviceProfile {
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
            return Err(ConfigValidationError::InvalidDeviceProfile {
                index,
                field: "write_eol",
                reason: "must be empty, CR, LF, or CRLF",
            });
        }
    }
    Ok(())
}

fn validate_slot_id(index: usize, id: &str) -> Result<(), ConfigValidationError> {
    let valid = !id.is_empty()
        && id.len() <= MAX_SLOT_ID_BYTES
        && id.bytes().enumerate().all(|(position, byte)| match byte {
            b'a'..=b'z' | b'0'..=b'9' => true,
            b'-' | b'_' => position > 0,
            _ => false,
        });
    if valid {
        Ok(())
    } else {
        Err(invalid_slot(
            index,
            "id",
            "use 1-64 lowercase ASCII letters, digits, '-' or '_'",
        ))
    }
}

fn validate_profile(index: usize, profile: &str) -> Result<(), ConfigValidationError> {
    if profile.is_empty()
        || profile.len() > MAX_PROFILE_NAME_BYTES
        || profile != profile.trim()
        || profile.chars().any(char::is_control)
    {
        Err(invalid_slot(
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
        Err(invalid_slot(
            index,
            field,
            "must be non-empty, trimmed, bounded text without control characters",
        ))
    } else {
        Ok(())
    }
}

fn validate_serial_settings(index: usize, slot: &SlotConfig) -> Result<(), ConfigValidationError> {
    let settings = &slot.settings;
    if !(50..=12_000_000).contains(&settings.baud_rate) {
        return Err(invalid_slot(
            index,
            "settings.baud_rate",
            "must be between 50 and 12000000",
        ));
    }
    if !matches!(settings.write_eol.as_str(), "" | "\r" | "\n" | "\r\n") {
        return Err(invalid_slot(
            index,
            "settings.write_eol",
            "must be empty, CR, LF, or CRLF",
        ));
    }
    if settings.flow_control == FlowControl::Hardware && settings.rts {
        return Err(invalid_slot(
            index,
            "settings.rts",
            "must be false when hardware flow control owns RTS",
        ));
    }
    for (field, pattern) in [
        ("settings.shell_prompt", settings.shell_prompt.as_deref()),
        ("settings.uboot_prompt", settings.uboot_prompt.as_deref()),
    ] {
        if pattern.is_some_and(|pattern| {
            pattern.is_empty() || pattern.len() > MAX_PROMPT_PATTERN_BYTES || pattern.contains('\0')
        }) {
            return Err(invalid_slot(
                index,
                field,
                "must be non-empty, at most 4096 bytes, and contain no NUL",
            ));
        }
    }
    if settings.probe.is_some() {
        return Err(invalid_slot(
            index,
            "settings.probe",
            "automatic probes are not supported in v1; use null",
        ));
    }
    Ok(())
}

fn invalid_slot(index: usize, field: &'static str, reason: &'static str) -> ConfigValidationError {
    ConfigValidationError::InvalidSlot {
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

fn atomic_write(target: &Path, contents: &[u8]) -> io::Result<()> {
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
    use serial_protocol::SerialSettings;

    fn slot(id: &str, port: &str) -> SlotConfig {
        SlotConfig {
            id: id.to_owned(),
            display_name: id.to_owned(),
            port: port.to_owned(),
            profile: "generic-115200".to_owned(),
            device_profile: None,
            enabled: true,
            settings: SerialSettings::default(),
        }
    }

    fn device_profile(name: &str) -> DeviceProfile {
        DeviceProfile {
            name: name.to_owned(),
            shell_prompt: Some("root@dut:/# ".to_owned()),
            uboot_prompt: Some("SigmaStar =>".to_owned()),
            write_eol: None,
            echo: None,
        }
    }

    #[test]
    fn first_load_creates_defaults_and_reload_preserves_server_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));

        let first = store.load_or_create().unwrap();
        let credentials = first.initial_credentials.as_ref().unwrap();
        assert_eq!(first.config.bind, "127.0.0.1:3210".parse().unwrap());
        assert_eq!(first.config.logging.max_total_bytes, 10 * GIB);
        assert_eq!(first.config.logging.retention_target_percent, 90);
        assert_eq!(
            first.config.logging.segment_max_bytes,
            DEFAULT_SEGMENT_MAX_BYTES
        );
        assert!(first.config.slots.is_empty());
        assert_eq!(
            first
                .config
                .auth
                .authenticate_bearer(credentials.admin_token())
                .unwrap()
                .role(),
            serial_protocol::Role::Admin
        );

        let first_server = first.config.server_id;
        let first_epoch = first.daemon_epoch;
        let second = store.load_or_create().unwrap();
        assert!(second.initial_credentials.is_none());
        assert_eq!(second.config.server_id, first_server);
        assert_ne!(second.daemon_epoch, first_epoch);

        let persisted = fs::read_to_string(&store.paths().config_file).unwrap();
        assert!(!persisted.contains("daemon_epoch"));
    }

    #[cfg(unix)]
    #[test]
    fn persisted_configuration_is_private_to_the_user() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        store.load_or_create().unwrap();
        let mode = fs::metadata(&store.paths().config_file)
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn updating_slots_commits_only_after_validation_and_persistence() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let mut loaded = store.load_or_create().unwrap();

        store
            .update_slots(
                &mut loaded.config,
                vec![slot("slot-1", "COM3"), slot("slot-2", "COM4")],
            )
            .unwrap();
        assert_eq!(loaded.config.slots.len(), 2);
        assert_eq!(store.load().unwrap().slots, loaded.config.slots);

        let before = loaded.config.slots.clone();
        let error = store
            .update_slots(
                &mut loaded.config,
                vec![slot("replacement", "COM8"), slot("duplicate", "com8")],
            )
            .unwrap_err();
        assert!(matches!(
            error,
            ConfigError::Validation(ConfigValidationError::DuplicatePort { .. })
        ));
        assert_eq!(loaded.config.slots, before);
        assert_eq!(store.load().unwrap().slots, before);
    }

    #[test]
    fn invalid_existing_toml_is_not_overwritten_or_regenerated() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        fs::create_dir_all(&store.paths().config_dir).unwrap();
        let invalid = "this is not valid = [ toml";
        fs::write(&store.paths().config_file, invalid).unwrap();

        assert!(matches!(
            store.load_or_create(),
            Err(ConfigError::InvalidToml { .. })
        ));
        assert_eq!(
            fs::read_to_string(&store.paths().config_file).unwrap(),
            invalid
        );
    }

    #[test]
    fn validation_rejects_duplicate_ids_and_invalid_serial_settings() {
        let (mut config, _) = DaemonConfig::generate();
        config.slots = vec![slot("slot-1", "COM3"), slot("slot-1", "COM4")];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::DuplicateSlotId { .. })
        ));

        let mut invalid = slot("slot-2", "COM5");
        invalid.settings.baud_rate = 0;
        config.slots = vec![invalid];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidSlot {
                field: "settings.baud_rate",
                ..
            })
        ));

        let mut with_probe = slot("slot-3", "COM6");
        with_probe.settings.probe = Some(serial_protocol::ProbeConfig {
            request: b"status\r".to_vec(),
            response_pattern: "ready".to_owned(),
            timeout_ms: 1_000,
        });
        config.slots = vec![with_probe];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidSlot {
                field: "settings.probe",
                ..
            })
        ));

        let mut hardware_flow = slot("slot-4", "COM7");
        hardware_flow.settings.flow_control = FlowControl::Hardware;
        hardware_flow.settings.rts = true;
        config.slots = vec![hardware_flow];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidSlot {
                field: "settings.rts",
                ..
            })
        ));
    }

    #[test]
    fn failed_in_memory_replacement_rolls_back() {
        let (mut config, _) = DaemonConfig::generate();
        config.replace_slots(vec![slot("slot-1", "COM3")]).unwrap();
        let before = config.slots.clone();
        let error = config
            .replace_slots(vec![slot("Bad ID", "COM4")])
            .unwrap_err();
        assert!(matches!(
            error,
            ConfigValidationError::InvalidSlot { field: "id", .. }
        ));
        assert_eq!(config.slots, before);
    }

    #[test]
    fn staged_replacement_keeps_source_unchanged_and_limits_active_slots() {
        let (config, _) = DaemonConfig::generate();
        let staged = config
            .staged_with_slots(vec![slot("slot-1", "COM3")])
            .unwrap();
        assert!(config.slots.is_empty());
        assert_eq!(staged.slots.len(), 1);

        let too_many = (0..=MAX_SLOT_IDENTITIES_PER_DAEMON)
            .map(|index| slot(&format!("slot-{index}"), &format!("COM{index}")))
            .collect();
        assert_eq!(
            config.staged_with_slots(too_many).unwrap_err(),
            ConfigValidationError::TooManySlots {
                actual: MAX_SLOT_IDENTITIES_PER_DAEMON + 1,
                limit: MAX_SLOT_IDENTITIES_PER_DAEMON,
            }
        );
    }

    #[test]
    fn loaded_debug_output_does_not_contain_credentials() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();
        let admin = loaded
            .initial_credentials
            .as_ref()
            .unwrap()
            .admin_token()
            .to_owned();
        let debug = format!("{loaded:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(&admin));
    }

    #[test]
    fn device_profile_catalog_is_validated_as_a_whole() {
        let (mut config, _) = DaemonConfig::generate();

        // Duplicate names are rejected.
        config.device_profiles = vec![device_profile("evb"), device_profile("evb")];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::DuplicateDeviceProfileName { .. })
        ));

        // Invalid names follow the same rules as Slot profile names.
        config.device_profiles = vec![device_profile(" padded ")];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidDeviceProfile { field: "name", .. })
        ));

        // Invalid prompt patterns and line endings are rejected.
        let mut invalid = device_profile("evb");
        invalid.uboot_prompt = Some(String::new());
        config.device_profiles = vec![invalid];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidDeviceProfile {
                field: "uboot_prompt",
                ..
            })
        ));
        let mut invalid = device_profile("evb");
        invalid.write_eol = Some("\r\r".to_owned());
        config.device_profiles = vec![invalid];
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidDeviceProfile {
                field: "write_eol",
                ..
            })
        ));

        // The catalog is bounded.
        config.device_profiles = (0..=MAX_DEVICE_PROFILES)
            .map(|index| device_profile(&format!("profile-{index}")))
            .collect();
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::TooManyDeviceProfiles { .. })
        ));
    }

    #[test]
    fn slot_device_profile_references_must_exist() {
        let (mut config, _) = DaemonConfig::generate();
        config.device_profiles = vec![device_profile("sigmastar-evb")];
        let mut referencing = slot("slot-1", "COM3");
        referencing.device_profile = Some("missing-model".to_owned());
        config.slots = vec![referencing];
        let error = config.validate().unwrap_err();
        let message = error.to_string();
        assert!(matches!(
            error,
            ConfigValidationError::UnknownDeviceProfile { .. }
        ));
        assert!(message.contains("missing-model"));
        assert!(message.contains("sigmastar-evb"));

        // A valid reference passes.
        let mut valid = slot("slot-2", "COM4");
        valid.device_profile = Some("sigmastar-evb".to_owned());
        config.slots = vec![valid];
        config.validate().unwrap();
    }

    #[test]
    fn device_profile_replacement_rolls_back_on_validation_failure() {
        let (mut config, _) = DaemonConfig::generate();
        let mut referencing = slot("slot-1", "COM3");
        referencing.device_profile = Some("sigmastar-evb".to_owned());
        config.slots = vec![referencing];
        config
            .replace_device_profiles(vec![device_profile("sigmastar-evb")])
            .unwrap();

        // Removing a profile that a Slot still references is rejected and
        // leaves the previous catalog in place.
        let error = config.replace_device_profiles(Vec::new()).unwrap_err();
        assert!(matches!(
            error,
            ConfigValidationError::UnknownDeviceProfile { .. }
        ));
        assert_eq!(config.device_profiles.len(), 1);

        let staged = config
            .staged_with_device_profiles(vec![device_profile("sigmastar-evb"), device_profile("rk")])
            .unwrap();
        assert_eq!(config.device_profiles.len(), 1);
        assert_eq!(staged.device_profiles.len(), 2);
    }

    #[test]
    fn legacy_toml_without_device_profiles_loads_with_an_empty_catalog() {
        let temporary = tempfile::tempdir().unwrap();
        let store = ConfigStore::new(ConfigPaths::from_root(temporary.path()));
        let loaded = store.load_or_create().unwrap();

        // Rewrite the persisted configuration without the device_profiles
        // key, as a pre-profile daemon would have written it.
        let persisted = fs::read_to_string(&store.paths().config_file).unwrap();
        let without_profiles = persisted
            .lines()
            .filter(|line| !line.starts_with("device_profiles"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&store.paths().config_file, without_profiles).unwrap();

        let reloaded = store.load().unwrap();
        assert!(reloaded.device_profiles.is_empty());
        assert_eq!(reloaded.server_id, loaded.config.server_id);
    }
}
