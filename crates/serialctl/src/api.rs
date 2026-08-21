use std::fmt;

use anyhow::{Context, Result, bail};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serial_protocol::{
    ArchiveListResponse, ConfigureModelProfilesRequest, ConfigureModelProfilesResponse,
    ConfigurePortsRequest, ConfigurePortsResponse, ConfigureTransportProfilesRequest,
    ConfigureTransportProfilesResponse, EventQuery, EventQueryResponse, HealthResponse,
    JournalDiagnostics, ModelProfile, ModelProfileListResponse, PortDescriptor, SlotConfig,
    SlotDiagnostics, SlotSnapshot, StatusResponse, StorageDiagnosticsResponse, TransportProfile,
    TransportProfileListResponse,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationStatus {
    pub server_id: uuid::Uuid,
    pub daemon_epoch: uuid::Uuid,
    pub ports: Vec<SlotSnapshot>,
    #[serde(default)]
    pub protocol_version: Option<u16>,
    #[serde(default)]
    pub config_revision: Option<u64>,
}

pub type ConfigurePortsDocumentResponse = ConfigurePortsResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileCatalog<T> {
    pub profiles: Vec<T>,
    #[serde(default)]
    pub config_revision: Option<u64>,
}

#[derive(Debug)]
pub struct ApiHttpError {
    pub status: reqwest::StatusCode,
    pub body: String,
}

impl fmt::Display for ApiHttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "seriald returned {}: {}",
            self.status,
            self.body.trim()
        )
    }
}

impl std::error::Error for ApiHttpError {}

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
    endpoint: String,
}

impl ApiClient {
    pub fn new(endpoint: String) -> Result<Self> {
        let endpoint = normalize_endpoint(&endpoint)?;
        Ok(Self {
            client: Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(15))
                .build()?,
            endpoint,
        })
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get_json("/api/v1/health").await
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get_json("/api/v1/status").await
    }

    pub async fn configuration_status(&self) -> Result<ConfigurationStatus> {
        self.get_json("/api/v1/status").await
    }

    pub async fn ports(&self) -> Result<Vec<PortDescriptor>> {
        self.get_json("/api/v1/ports").await
    }

    pub async fn storage_diagnostics(&self) -> Result<JournalDiagnostics> {
        Ok(self
            .get_json::<StorageDiagnosticsResponse>("/api/v1/diagnostics/storage")
            .await?
            .journal)
    }

    pub async fn port_diagnostics(&self, port: &str) -> Result<SlotDiagnostics> {
        self.get_json(&format!(
            "/api/v1/ports/{}/diagnostics",
            encode_path_segment(port)
        ))
        .await
    }

    pub async fn configure_ports(
        &self,
        ports: Vec<SlotConfig>,
        expected_revision: Option<u64>,
    ) -> Result<ConfigurePortsDocumentResponse> {
        let response = self
            .client
            .put(self.url("/api/v1/config/ports"))
            .json(&ConfigurePortsRequest {
                ports,
                source: "human:serialctl".into(),
                expected_revision,
            })
            .send()
            .await
            .context("seriald configuration request failed")?;
        decode_response(response).await
    }

    pub async fn transport_profiles(&self) -> Result<ProfileCatalog<TransportProfile>> {
        let response = self
            .get_json::<TransportProfileListResponse>("/api/v1/config/transport-profiles")
            .await?;
        Ok(ProfileCatalog {
            profiles: response.profiles,
            config_revision: Some(response.config_revision),
        })
    }

    pub async fn configure_transport_profiles(
        &self,
        profiles: Vec<TransportProfile>,
        expected_revision: Option<u64>,
    ) -> Result<ProfileCatalog<TransportProfile>> {
        let response = self
            .client
            .put(self.url("/api/v1/config/transport-profiles"))
            .json(&ConfigureTransportProfilesRequest {
                profiles,
                expected_revision,
            })
            .send()
            .await
            .context("seriald transport profile configuration request failed")?;
        let response = decode_response::<ConfigureTransportProfilesResponse>(response).await?;
        Ok(ProfileCatalog {
            profiles: response.profiles,
            config_revision: Some(response.config_revision),
        })
    }

    pub async fn model_profiles(&self) -> Result<ProfileCatalog<ModelProfile>> {
        let response = self
            .get_json::<ModelProfileListResponse>("/api/v1/config/model-profiles")
            .await?;
        Ok(ProfileCatalog {
            profiles: response.profiles,
            config_revision: Some(response.config_revision),
        })
    }

    pub async fn configure_model_profiles(
        &self,
        profiles: Vec<ModelProfile>,
        expected_revision: Option<u64>,
    ) -> Result<ProfileCatalog<ModelProfile>> {
        let response = self
            .client
            .put(self.url("/api/v1/config/model-profiles"))
            .json(&ConfigureModelProfilesRequest {
                profiles,
                expected_revision,
            })
            .send()
            .await
            .context("seriald model profile configuration request failed")?;
        let response = decode_response::<ConfigureModelProfilesResponse>(response).await?;
        Ok(ProfileCatalog {
            profiles: response.profiles,
            config_revision: Some(response.config_revision),
        })
    }

    pub async fn archives(&self, port: Option<&str>) -> Result<ArchiveListResponse> {
        let mut request = self.client.get(self.url("/api/v1/archives"));
        if let Some(port) = port {
            request = request.query(&[("port", port)]);
        }
        let response = self
            .client
            .execute(request.build()?)
            .await
            .context("seriald archive catalog request failed")?;
        decode_response(response).await
    }

    pub async fn events(&self, port: &str, query: &EventQuery) -> Result<EventQueryResponse> {
        let encoded_port = encode_path_segment(port);
        let response = self
            .client
            .get(self.url(&format!("/api/v1/ports/{encoded_port}/events")))
            .query(query)
            .send()
            .await
            .context("seriald event query failed")?;
        decode_response(response).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .client
            .get(self.url(path))
            .send()
            .await
            .with_context(|| format!("request to {path} failed"))?;
        decode_response(response).await
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint, path)
    }
}

pub fn is_not_found(error: &anyhow::Error) -> bool {
    has_http_status(error, reqwest::StatusCode::NOT_FOUND)
}

pub fn is_conflict(error: &anyhow::Error) -> bool {
    has_http_status(error, reqwest::StatusCode::CONFLICT)
}

fn has_http_status(error: &anyhow::Error, status: reqwest::StatusCode) -> bool {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ApiHttpError>())
        .is_some_and(|http| http.status == status)
}

pub fn normalize_endpoint(endpoint: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(endpoint.trim()).context("invalid seriald endpoint URL")?;
    if url.scheme() != "http" {
        bail!(
            "seriald v1 endpoints must use http://; bind it only to loopback or the host-only VM network"
        );
    }
    if url.host().is_none() {
        bail!("seriald endpoint must include a host");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("seriald endpoint must not contain user information");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("seriald endpoint must not contain a query or fragment");
    }
    if url.path() != "/" && !url.path().is_empty() {
        bail!("seriald endpoint must be an origin without a path");
    }
    url.set_path("");
    Ok(url.as_str().trim_end_matches('/').to_string())
}

async fn decode_response<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "response body unavailable".into());
        return Err(ApiHttpError { status, body }.into());
    }
    response
        .json::<T>()
        .await
        .context("seriald returned an invalid JSON response")
}

fn encode_path_segment(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_segments_are_percent_encoded() {
        assert_eq!(
            encode_path_segment("/dev/cu.port 二"),
            "%2Fdev%2Fcu.port%20%E4%BA%8C"
        );
    }

    #[test]
    fn endpoints_are_normalized_and_restricted_to_an_http_origin() {
        assert_eq!(
            normalize_endpoint(" http://127.0.0.1:3210/ ").unwrap(),
            "http://127.0.0.1:3210"
        );
        assert_eq!(
            normalize_endpoint("http://[::1]:3210").unwrap(),
            "http://[::1]:3210"
        );
        for endpoint in [
            "https://127.0.0.1:3210",
            "http://user@127.0.0.1:3210",
            "http://127.0.0.1:3210/base",
            "http://127.0.0.1:3210?unexpected=bad",
            "http://127.0.0.1:3210#fragment",
        ] {
            assert!(normalize_endpoint(endpoint).is_err(), "accepted {endpoint}");
        }
    }
}
