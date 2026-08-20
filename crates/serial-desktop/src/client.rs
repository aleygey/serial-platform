use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serial_protocol::{
    ConfigureDeviceProfilesRequest, ConfigureDeviceProfilesResponse, ConfigureSlotsRequest,
    ConfigureSlotsResponse, ConfigureTransportProfilesRequest, ConfigureTransportProfilesResponse,
    DeviceModelListResponse, DeviceProfileListResponse, EventQuery, EventQueryResponse,
    HealthResponse, PortDescriptor, SetSlotDeviceModelRequest, SetSlotDeviceModelResponse,
    StatusResponse, TransportProfileListResponse,
};

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
    endpoint: String,
    token: Option<String>,
}

impl ApiClient {
    pub fn new(endpoint: &str, token: Option<String>) -> Result<Self> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(2))
                .timeout(Duration::from_secs(8))
                .build()
                .context("build desktop HTTP client")?,
            endpoint: normalize_endpoint(endpoint)?,
            token,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get("/api/v1/health").await
    }

    /// Probes the health route before the desktop starts a local daemon. Any
    /// HTTP response means the endpoint is already owned (including 401 from
    /// an authenticated seriald), so the App must not launch or later stop a
    /// second process on that address.
    pub async fn health_reachable(&self) -> bool {
        self.authorize(self.client.get(self.url("/api/v1/health")))
            .send()
            .await
            .is_ok()
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get("/api/v1/status").await
    }

    pub async fn ports(&self) -> Result<Vec<PortDescriptor>> {
        self.get("/api/v1/ports").await
    }

    pub async fn transport_profiles(&self) -> Result<TransportProfileListResponse> {
        self.get("/api/v1/config/transport-profiles").await
    }

    pub async fn device_profiles(&self) -> Result<DeviceProfileListResponse> {
        self.get("/api/v1/config/device-profiles").await
    }

    pub async fn device_models(&self) -> Result<DeviceModelListResponse> {
        self.get("/api/v1/config/device-models").await
    }

    pub async fn events(&self, slot_id: &str, query: &EventQuery) -> Result<EventQueryResponse> {
        let encoded_slot = encode_path_segment(slot_id);
        let response = self
            .authorize(
                self.client
                    .get(self.url(&format!("/api/v1/slots/{encoded_slot}/events")))
                    .query(query),
            )
            .send()
            .await
            .context("query persistent serial history")?;
        decode(response).await
    }

    pub async fn configure_slots(
        &self,
        request: ConfigureSlotsRequest,
    ) -> Result<ConfigureSlotsResponse> {
        let response = self
            .authorize(
                self.client
                    .put(self.url("/api/v1/config/slots"))
                    .json(&request),
            )
            .send()
            .await
            .context("configure serial Slots")?;
        decode(response).await
    }

    pub async fn configure_transport_profiles(
        &self,
        request: ConfigureTransportProfilesRequest,
    ) -> Result<ConfigureTransportProfilesResponse> {
        let response = self
            .authorize(
                self.client
                    .put(self.url("/api/v1/config/transport-profiles"))
                    .json(&request),
            )
            .send()
            .await
            .context("configure Transport Profiles")?;
        decode(response).await
    }

    pub async fn configure_device_profiles(
        &self,
        request: ConfigureDeviceProfilesRequest,
    ) -> Result<ConfigureDeviceProfilesResponse> {
        let response = self
            .authorize(
                self.client
                    .put(self.url("/api/v1/config/device-profiles"))
                    .json(&request),
            )
            .send()
            .await
            .context("configure Device Profiles")?;
        decode(response).await
    }

    pub async fn set_slot_device_model(
        &self,
        slot_id: &str,
        request: &SetSlotDeviceModelRequest,
    ) -> Result<SetSlotDeviceModelResponse> {
        let encoded_slot = encode_path_segment(slot_id);
        let response = self
            .authorize(
                self.client
                    .put(self.url(&format!("/api/v1/slots/{encoded_slot}/device-model")))
                    .json(request),
            )
            .send()
            .await
            .context("bind Slot device model")?;
        decode(response).await
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .authorize(self.client.get(self.url(path)))
            .send()
            .await
            .with_context(|| format!("request {path}"))?;
        decode(response).await
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match self.token.as_ref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint)
    }
}

fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

pub fn normalize_endpoint(endpoint: &str) -> Result<String> {
    let endpoint = endpoint.trim().trim_end_matches('/');
    if !endpoint.starts_with("http://") {
        bail!("desktop MVP accepts only http:// seriald endpoints");
    }
    let authority = endpoint.trim_start_matches("http://");
    if authority.is_empty()
        || authority.contains('/')
        || authority.contains('?')
        || authority.contains('#')
    {
        bail!("seriald endpoint must be one HTTP origin without a path");
    }
    Ok(endpoint.to_string())
}

pub fn websocket_url(endpoint: &str) -> Result<String> {
    Ok(format!(
        "ws://{}/api/v1/ws",
        normalize_endpoint(endpoint)?
            .strip_prefix("http://")
            .expect("normalized endpoint starts with http")
    ))
}

async fn decode<T: DeserializeOwned>(response: Response) -> Result<T> {
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        bail!("seriald returned {status}: {}", body.trim());
    }
    response.json().await.context("decode seriald response")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_are_normalized_and_ws_keeps_the_same_origin() {
        assert_eq!(
            normalize_endpoint(" http://127.0.0.1:3210/ ").unwrap(),
            "http://127.0.0.1:3210"
        );
        assert_eq!(
            websocket_url("http://127.0.0.1:3210").unwrap(),
            "ws://127.0.0.1:3210/api/v1/ws"
        );
        assert!(normalize_endpoint("https://example.com").is_err());
        assert!(normalize_endpoint("http://localhost:3210/path").is_err());
        assert_eq!(encode_path_segment("dut / 1"), "dut%20%2F%201");
    }
}
