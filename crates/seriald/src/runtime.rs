//! One active daemon and one discoverable endpoint per serial-platform data root.

use crate::config::{ConfigPaths, atomic_write};
use serde::{Deserialize, Serialize};
use serial_protocol::{HealthResponse, PROTOCOL_VERSION};
use std::{
    fs,
    io::{self, Read as _, Write as _},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    time::Duration,
};
use thiserror::Error;
use uuid::Uuid;

const ACTIVE_ENDPOINT_SCHEMA: u16 = 1;
const ACTIVE_ENDPOINT_FILE: &str = "active-endpoint.json";
const INSTANCE_LOCK_FILE: &str = "seriald.lock";
const MAX_MARKER_BYTES: u64 = 16 * 1024;

/// The verified local endpoint published by the process that owns this data root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveEndpoint {
    pub schema_version: u16,
    pub endpoint: String,
    pub address: SocketAddr,
    pub server_id: Uuid,
    pub daemon_epoch: Uuid,
    pub protocol_version: u16,
    pub pid: u32,
}

impl ActiveEndpoint {
    #[must_use]
    pub fn new(bind: SocketAddr, server_id: Uuid, daemon_epoch: Uuid, pid: u32) -> Self {
        let address = connect_address(bind);
        Self {
            schema_version: ACTIVE_ENDPOINT_SCHEMA,
            endpoint: endpoint_url(address),
            address,
            server_id,
            daemon_epoch,
            protocol_version: PROTOCOL_VERSION,
            pid,
        }
    }

    fn structurally_valid(&self) -> bool {
        self.schema_version == ACTIVE_ENDPOINT_SCHEMA
            && self.protocol_version == PROTOCOL_VERSION
            && self.endpoint == endpoint_url(self.address)
            && !self.address.ip().is_unspecified()
            && self.address.port() != 0
    }
}

/// Converts a listener bind into the local address clients can actually dial.
#[must_use]
pub fn connect_address(bind: SocketAddr) -> SocketAddr {
    match bind.ip() {
        IpAddr::V4(address) if address.is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), bind.port())
        }
        IpAddr::V6(address) if address.is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), bind.port())
        }
        _ => bind,
    }
}

#[must_use]
pub fn endpoint_url(address: SocketAddr) -> String {
    format!("http://{address}")
}

/// Owns the OS-level single-instance lock for one data root.
pub struct ActiveInstance {
    _lock_file: fs::File,
    marker_path: PathBuf,
    identity: Option<(Uuid, Uuid)>,
}

impl ActiveInstance {
    pub fn acquire(paths: &ConfigPaths) -> Result<Self, RuntimeError> {
        fs::create_dir_all(&paths.data_dir).map_err(|source| RuntimeError::Io {
            path: paths.data_dir.clone(),
            source,
        })?;
        let lock_path = paths.data_dir.join(INSTANCE_LOCK_FILE);
        let lock_file = open_instance_lock(&lock_path).map_err(|source| {
            if is_lock_contention(&source) {
                RuntimeError::AlreadyRunning {
                    data_dir: paths.data_dir.clone(),
                    endpoint: marker_hint(paths),
                }
            } else {
                RuntimeError::Io {
                    path: lock_path,
                    source,
                }
            }
        })?;
        Ok(Self {
            _lock_file: lock_file,
            marker_path: marker_path(paths),
            identity: None,
        })
    }

    pub fn publish(&mut self, endpoint: &ActiveEndpoint) -> Result<(), RuntimeError> {
        let encoded = serde_json::to_vec(endpoint).map_err(RuntimeError::Serialize)?;
        atomic_write(&self.marker_path, &encoded).map_err(|source| RuntimeError::Io {
            path: self.marker_path.clone(),
            source,
        })?;
        self.identity = Some((endpoint.server_id, endpoint.daemon_epoch));
        Ok(())
    }

    pub fn clear_marker(&mut self) {
        let Some(identity) = self.identity.take() else {
            return;
        };
        let Ok(Some(marker)) = read_marker(&self.marker_path) else {
            return;
        };
        if (marker.server_id, marker.daemon_epoch) == identity {
            let _ = fs::remove_file(&self.marker_path);
        }
    }
}

impl Drop for ActiveInstance {
    fn drop(&mut self) {
        self.clear_marker();
    }
}

/// Returns an endpoint only after its health response proves that the marker is live.
pub fn discover_active(
    paths: &ConfigPaths,
    expected_server_id: Uuid,
) -> Result<Option<ActiveEndpoint>, RuntimeError> {
    discover_active_with(paths, expected_server_id, health_matches)
}

fn discover_active_with(
    paths: &ConfigPaths,
    expected_server_id: Uuid,
    verify: impl FnOnce(&ActiveEndpoint) -> bool,
) -> Result<Option<ActiveEndpoint>, RuntimeError> {
    let marker_path = marker_path(paths);
    let Some(marker) = read_marker(&marker_path).map_err(|source| RuntimeError::Io {
        path: marker_path,
        source,
    })?
    else {
        return Ok(None);
    };
    if !marker.structurally_valid() || marker.server_id != expected_server_id {
        return Ok(None);
    }
    Ok(verify(&marker).then_some(marker))
}

fn health_matches(marker: &ActiveEndpoint) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&marker.address, Duration::from_millis(500))
    else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    let request = format!(
        "GET /api/v1/health HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        marker.address
    );
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }
    let mut response = Vec::new();
    if stream.take(64 * 1024).read_to_end(&mut response).is_err() {
        return false;
    }
    response_matches(marker, &response)
}

fn response_matches(marker: &ActiveEndpoint, response: &[u8]) -> bool {
    let Some(body_offset) = response.windows(4).position(|part| part == b"\r\n\r\n") else {
        return false;
    };
    let header = &response[..body_offset];
    if !header.starts_with(b"HTTP/1.1 200 ") && !header.starts_with(b"HTTP/1.0 200 ") {
        return false;
    }
    let Ok(health) = serde_json::from_slice::<HealthResponse>(&response[body_offset + 4..]) else {
        return false;
    };
    health.status == "ok"
        && health.server_id == marker.server_id
        && health.daemon_epoch == marker.daemon_epoch
        && health.protocol_version == marker.protocol_version
}

fn marker_path(paths: &ConfigPaths) -> PathBuf {
    paths.data_dir.join(ACTIVE_ENDPOINT_FILE)
}

fn marker_hint(paths: &ConfigPaths) -> Option<String> {
    read_marker(&marker_path(paths))
        .ok()
        .flatten()
        .filter(ActiveEndpoint::structurally_valid)
        .map(|marker| marker.endpoint)
}

fn read_marker(path: &Path) -> io::Result<Option<ActiveEndpoint>> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if metadata.len() > MAX_MARKER_BYTES {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes).ok())
}

#[cfg(unix)]
fn open_instance_lock(path: &Path) -> io::Result<fs::File> {
    use std::os::{fd::AsRawFd as _, unix::fs::OpenOptionsExt as _};

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;
    unsafe extern "C" {
        fn flock(file_descriptor: i32, operation: i32) -> i32;
    }

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(path)?;
    // SAFETY: `file` owns a valid descriptor for this synchronous system call.
    if unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(file)
}

#[cfg(windows)]
fn open_instance_lock(path: &Path) -> io::Result<fs::File> {
    use std::os::windows::fs::OpenOptionsExt as _;

    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0)
        .open(path)
}

#[cfg(not(any(unix, windows)))]
fn open_instance_lock(path: &Path) -> io::Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

#[cfg(unix)]
fn is_lock_contention(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(11 | 35))
}

#[cfg(windows)]
fn is_lock_contention(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(32 | 33))
}

#[cfg(not(any(unix, windows)))]
fn is_lock_contention(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::AlreadyExists
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(
        "seriald data root {data_dir} is already owned by another process{detail}",
        detail = endpoint.as_ref().map(|value| format!(" at {value}")).unwrap_or_default(),
        data_dir = data_dir.display()
    )]
    AlreadyRunning {
        data_dir: PathBuf,
        endpoint: Option<String>,
    },
    #[error("seriald runtime I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("serialize active seriald endpoint: {0}")]
    Serialize(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigPaths;

    fn paths(root: &Path) -> ConfigPaths {
        ConfigPaths::from_root(root)
    }

    #[test]
    fn wildcard_binds_publish_dialable_loopback_urls() {
        let v4 = ActiveEndpoint::new(
            "0.0.0.0:4321".parse().unwrap(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
        );
        assert_eq!(v4.endpoint, "http://127.0.0.1:4321");
        let v6 = ActiveEndpoint::new(
            "[::]:4322".parse().unwrap(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            1,
        );
        assert_eq!(v6.endpoint, "http://[::1]:4322");
    }

    #[test]
    fn one_data_root_has_only_one_owner() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(root.path());
        let first = ActiveInstance::acquire(&paths).unwrap();
        assert!(matches!(
            ActiveInstance::acquire(&paths),
            Err(RuntimeError::AlreadyRunning { .. })
        ));
        drop(first);
        ActiveInstance::acquire(&paths).unwrap();
    }

    #[test]
    fn discovery_accepts_verified_custom_endpoint_and_rejects_stale_marker() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(root.path());
        let server_id = Uuid::new_v4();
        let daemon_epoch = Uuid::new_v4();
        let stale = ActiveEndpoint::new(
            "127.0.0.1:9".parse().unwrap(),
            server_id,
            Uuid::new_v4(),
            40,
        );
        fs::create_dir_all(&paths.data_dir).unwrap();
        atomic_write(&marker_path(&paths), &serde_json::to_vec(&stale).unwrap()).unwrap();
        assert_eq!(
            discover_active_with(&paths, server_id, |_| false).unwrap(),
            None
        );

        let mut owner = ActiveInstance::acquire(&paths).unwrap();
        let marker = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            server_id,
            daemon_epoch,
            42,
        );
        owner.publish(&marker).unwrap();
        assert_eq!(
            discover_active_with(&paths, server_id, |candidate| {
                candidate.endpoint == "http://127.0.0.1:4321"
            })
            .unwrap(),
            Some(marker)
        );
    }

    #[test]
    fn cleanup_never_deletes_an_endpoint_published_by_another_identity() {
        let root = tempfile::tempdir().unwrap();
        let paths = paths(root.path());
        let mut owner = ActiveInstance::acquire(&paths).unwrap();
        let ours = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            41,
        );
        owner.publish(&ours).unwrap();

        let replacement = ActiveEndpoint::new(
            "127.0.0.1:4322".parse().unwrap(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            42,
        );
        atomic_write(
            &marker_path(&paths),
            &serde_json::to_vec(&replacement).unwrap(),
        )
        .unwrap();
        owner.clear_marker();

        assert_eq!(
            read_marker(&marker_path(&paths)).unwrap(),
            Some(replacement)
        );
    }

    #[test]
    fn health_response_must_match_marker_identity_and_protocol() {
        let server_id = Uuid::new_v4();
        let daemon_epoch = Uuid::new_v4();
        let marker = ActiveEndpoint::new(
            "127.0.0.1:4321".parse().unwrap(),
            server_id,
            daemon_epoch,
            42,
        );
        let body = serde_json::to_string(&HealthResponse {
            status: "ok".into(),
            server_id,
            daemon_epoch,
            uptime_ms: 1,
            protocol_version: PROTOCOL_VERSION,
        })
        .unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        );
        assert!(response_matches(&marker, response.as_bytes()));

        let wrong_epoch = ActiveEndpoint {
            daemon_epoch: Uuid::new_v4(),
            ..marker
        };
        assert!(!response_matches(&wrong_epoch, response.as_bytes()));
    }

    #[test]
    fn roots_have_independent_endpoint_owners() {
        let first_root = tempfile::tempdir().unwrap();
        let second_root = tempfile::tempdir().unwrap();
        let first = ActiveInstance::acquire(&paths(first_root.path())).unwrap();
        let second = ActiveInstance::acquire(&paths(second_root.path())).unwrap();
        drop((first, second));
    }
}
