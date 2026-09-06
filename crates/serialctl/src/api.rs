use std::fmt;

use anyhow::{Context, Result, bail, ensure};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serial_protocol::{
    ArchiveListResponse, ConfigureModelFamiliesRequest, ConfigureModelFamiliesResponse,
    ConfigureModelProfilesRequest, ConfigureModelProfilesResponse, ConfigurePortsRequest,
    ConfigurePortsResponse, ConfigureTransportProfilesRequest, ConfigureTransportProfilesResponse,
    EventQuery, EventQueryResponse, HealthResponse, JournalDiagnostics, ModelFamily,
    ModelFamilyListResponse, ModelProfile, ModelProfileListResponse, MonitorIncidentListResponse,
    MonitorListResponse, PortDescriptor, SlotConfig, SlotDiagnostics, SlotSnapshot, StatusResponse,
    StorageDiagnosticsResponse, TransportProfile, TransportProfileListResponse,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelFamilyCatalog {
    pub families: Vec<ModelFamily>,
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

pub(crate) const HUMAN_HISTORY_MAX_ENTRIES: usize = 10_000;

/// Publish only a complete, single-revision snapshot. A concurrent update
/// invalidates this bounded traversal; the caller retries on its next refresh.
async fn collect_human_history_pages<F, Fut>(
    mut fetch: F,
) -> Result<serial_protocol::HumanCommandHistoryResponse>
where
    F: FnMut(Option<u64>) -> Fut,
    Fut: std::future::Future<Output = Result<serial_protocol::HumanCommandHistoryResponse>>,
{
    let mut snapshot = None::<serial_protocol::HumanCommandHistoryResponse>;
    let mut before = None;
    let mut seen = std::collections::HashSet::new();
    for _ in 0..5 {
        let page = fetch(before).await?;
        ensure!(
            page.entries.len() <= 2000,
            "Human history page exceeds the negotiated limit"
        );
        let result = snapshot.get_or_insert_with(|| serial_protocol::HumanCommandHistoryResponse {
            server_id: page.server_id,
            revision: page.revision,
            entries: Vec::new(),
            next_before_revision: None,
            warning: page.warning.clone(),
        });
        ensure!(
            result.server_id == page.server_id && result.revision == page.revision,
            "Human history changed during pagination; preserving the previous complete snapshot"
        );
        let next = page.next_before_revision;
        for entry in page.entries {
            if seen.insert(entry.command.clone()) {
                result.entries.push(entry);
            }
        }
        if let Some(warning) = page.warning {
            result.warning = Some(warning);
        }
        if next.is_none() {
            return Ok(snapshot.unwrap());
        }
        ensure!(next.is_some_and(|cursor| cursor > 0 && before.is_none_or(|previous| cursor < previous)),
            "Human history pagination did not advance");
        before = next;
    }
    bail!(
        "Human history exceeds the 10000-entry snapshot bound; preserving the previous complete snapshot"
    )
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

    pub async fn human_command_history(
        &self,
    ) -> Result<serial_protocol::HumanCommandHistoryResponse> {
        collect_human_history_pages(|before_revision| async move {
            let response = self
                .client
                .get(self.url("/api/v1/history/commands"))
                .query(&serial_protocol::HumanCommandHistoryQuery {
                    before_revision,
                    limit: Some(2000),
                    ..Default::default()
                })
                .send()
                .await
                .context("Human history request failed")?;
            decode_response(response).await
        })
        .await
    }

    pub async fn macros(
        &self,
        query: &serial_protocol::MacroListQuery,
    ) -> Result<serial_protocol::MacroListResponse> {
        let response = self
            .client
            .get(self.url("/api/v1/macros"))
            .query(query)
            .send()
            .await
            .context("macro catalog request failed")?;
        decode_response(response).await
    }

    pub async fn save_macro(
        &self,
        definition: &serial_protocol::MacroSaveRequest,
    ) -> Result<serial_protocol::MacroSaveResponse> {
        let response = self
            .client
            .post(self.url("/api/v1/macros"))
            .json(definition)
            .send()
            .await
            .context("macro save failed")?;
        decode_response(response).await
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

    pub async fn model_families(&self) -> Result<ModelFamilyCatalog> {
        let response = self
            .get_json::<ModelFamilyListResponse>("/api/v1/config/model-families")
            .await?;
        Ok(ModelFamilyCatalog {
            families: response.families,
            config_revision: Some(response.config_revision),
        })
    }

    pub async fn monitors(&self, port: Option<&str>) -> Result<MonitorListResponse> {
        let mut request = self.client.get(self.url("/api/v1/monitors"));
        if let Some(port) = port {
            request = request.query(&[("port", port)]);
        }
        decode_response(
            request
                .send()
                .await
                .context("seriald Monitor list request failed")?,
        )
        .await
    }

    pub async fn monitor_incidents(
        &self,
        monitor_id: uuid::Uuid,
        after_incident_seq: Option<u64>,
        limit: usize,
    ) -> Result<MonitorIncidentListResponse> {
        let mut request = self
            .client
            .get(self.url(&format!("/api/v1/monitors/{monitor_id}/incidents")));
        request = request.query(&[
            ("limit", limit.to_string()),
            ("include_acked", "true".into()),
        ]);
        if let Some(after) = after_incident_seq {
            request = request.query(&[("after_incident_seq", after)]);
        }
        decode_response(
            request
                .send()
                .await
                .context("seriald Monitor Incident list request failed")?,
        )
        .await
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

    pub async fn configure_model_families(
        &self,
        families: Vec<ModelFamily>,
        expected_revision: Option<u64>,
    ) -> Result<ModelFamilyCatalog> {
        let response = self
            .client
            .put(self.url("/api/v1/config/model-families"))
            .json(&ConfigureModelFamiliesRequest {
                families,
                expected_revision,
            })
            .send()
            .await
            .context("seriald model-family configuration request failed")?;
        let response = decode_response::<ConfigureModelFamiliesResponse>(response).await?;
        Ok(ModelFamilyCatalog {
            families: response.families,
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

    fn history_page(
        server_id: uuid::Uuid,
        revision: u64,
        newest: u64,
        count: u64,
        next: Option<u64>,
    ) -> serial_protocol::HumanCommandHistoryResponse {
        serial_protocol::HumanCommandHistoryResponse {
            server_id,
            revision,
            entries: (newest + 1 - count..=newest)
                .rev()
                .map(|value| serial_protocol::HumanCommandHistoryEntry {
                    id: uuid::Uuid::new_v4(),
                    command: format!("command-{value}"),
                    port: "COM3".into(),
                    wall_time_ns: 0,
                    revision: value,
                    uses: 1,
                })
                .collect(),
            next_before_revision: next,
            warning: None,
        }
    }

    #[tokio::test]
    async fn human_history_fetches_all_five_pages_in_recent_first_order() {
        let server = uuid::Uuid::new_v4();
        let mut pages = (0..5)
            .map(|page| {
                let newest = 10_000 - page * 2_000;
                history_page(
                    server,
                    10_000,
                    newest,
                    2_000,
                    (page < 4).then_some(newest - 1_999),
                )
            })
            .collect::<std::collections::VecDeque<_>>();
        let mut cursors = Vec::new();
        let snapshot = collect_human_history_pages(|before| {
            cursors.push(before);
            std::future::ready(Ok(pages.pop_front().unwrap()))
        })
        .await
        .unwrap();
        assert_eq!(
            cursors,
            vec![None, Some(8001), Some(6001), Some(4001), Some(2001)]
        );
        assert_eq!(snapshot.entries.len(), HUMAN_HISTORY_MAX_ENTRIES);
        assert_eq!(snapshot.entries.first().unwrap().command, "command-10000");
        assert_eq!(snapshot.entries.last().unwrap().command, "command-1");
        assert!(snapshot.next_before_revision.is_none());
    }

    #[tokio::test]
    async fn human_history_rejects_mixed_revision_or_server_during_pagination() {
        for change_server in [false, true] {
            let server = uuid::Uuid::new_v4();
            let mut pages = std::collections::VecDeque::from([
                history_page(server, 5000, 5000, 1, Some(5000)),
                history_page(
                    if change_server {
                        uuid::Uuid::new_v4()
                    } else {
                        server
                    },
                    if change_server { 5000 } else { 5001 },
                    4999,
                    1,
                    None,
                ),
            ]);
            let error =
                collect_human_history_pages(|_| std::future::ready(Ok(pages.pop_front().unwrap())))
                    .await
                    .unwrap_err();
            assert!(error.to_string().contains("changed during pagination"));
        }
    }

    #[tokio::test]
    async fn human_history_deduplicates_pages_and_rejects_a_stalled_cursor() {
        let server = uuid::Uuid::new_v4();
        let mut pages = std::collections::VecDeque::from([
            history_page(server, 9, 9, 2, Some(8)),
            history_page(server, 9, 8, 2, None),
        ]);
        let result =
            collect_human_history_pages(|_| std::future::ready(Ok(pages.pop_front().unwrap())))
                .await
                .unwrap();
        assert_eq!(
            result
                .entries
                .iter()
                .map(|entry| entry.command.as_str())
                .collect::<Vec<_>>(),
            vec!["command-9", "command-8", "command-7"]
        );
        let mut pages = std::collections::VecDeque::from([
            history_page(server, 9, 9, 1, Some(9)),
            history_page(server, 9, 8, 1, Some(9)),
        ]);
        assert!(
            collect_human_history_pages(|_| std::future::ready(Ok(pages.pop_front().unwrap())))
                .await
                .unwrap_err()
                .to_string()
                .contains("did not advance")
        );
    }

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
