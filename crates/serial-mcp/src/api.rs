use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder};
use serde_json::{Map, Value, json};
use serial_protocol::{
    ArchiveListResponse, ConfigureModelProfilesRequest, ConfigureModelProfilesResponse,
    ConfigurePortsRequest, ConfigurePortsResponse, CreateMonitorRequest, Cursor, EventQuery,
    EventQueryResponse, HealthResponse, ModelProfileListResponse, MonitorIncidentListResponse,
    MonitorListResponse, MonitorResponse, StatusResponse,
};

const MONITOR_INCIDENT_PAGE_LIMIT: usize = 20;

#[derive(Debug)]
struct ApiHttpError {
    status: reqwest::StatusCode,
    code: Option<String>,
    message: String,
    fields: Map<String, Value>,
}

impl std::fmt::Display for ApiHttpError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code.as_deref() {
            Some(code) => write!(
                formatter,
                "seriald returned {} ({code}): {}",
                self.status, self.message
            ),
            None => write!(
                formatter,
                "seriald returned {}: {}",
                self.status, self.message
            ),
        }
    }
}

impl std::error::Error for ApiHttpError {}

fn api_http_error(status: reqwest::StatusCode, body: String) -> ApiHttpError {
    let fields = match serde_json::from_str::<Value>(&body) {
        Ok(Value::Object(fields)) => fields,
        _ => Map::new(),
    };
    let code = fields
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let message = fields
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let trimmed = body.trim();
            if trimmed.is_empty() {
                "response body unavailable".to_owned()
            } else {
                trimmed.to_owned()
            }
        });
    ApiHttpError {
        status,
        code,
        message,
        fields,
    }
}

/// Converts a seriald HTTP failure anywhere in an anyhow context chain into a
/// bounded, stable MCP error object. The raw JSON body is deliberately not
/// nested as a string, which avoids the escaped `{"code":...}` failure that
/// previously obscured query-budget diagnostics from Agents.
pub(crate) fn structured_http_error(error: &anyhow::Error) -> Option<Value> {
    let error = error.downcast_ref::<ApiHttpError>()?;
    let retryable = error
        .fields
        .get("retryable")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| error.status.is_server_error());
    let mut structured = json!({
        "source": "seriald",
        "http_status": error.status.as_u16(),
        "code": error.code.as_deref().unwrap_or("http_error"),
        "message": error.message,
        "retryable": retryable,
    });
    for field in ["phase", "scanned_bytes", "elapsed_ms", "retry_hint"] {
        if let Some(value) = error.fields.get(field) {
            structured[field] = value.clone();
        }
    }
    Some(json!({"error": structured}))
}

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

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        self.get_json("/api/v1/health").await
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get_json("/api/v1/status").await
    }

    pub async fn model_profiles(&self) -> Result<ModelProfileListResponse> {
        self.get_json("/api/v1/config/model-profiles").await
    }

    pub async fn configure_model_profiles(
        &self,
        request: &ConfigureModelProfilesRequest,
    ) -> Result<ConfigureModelProfilesResponse> {
        let response = self
            .request(
                self.client
                    .put(self.url("/api/v1/config/model-profiles"))
                    .json(request),
            )
            .send()
            .await
            .context("seriald model profile update failed")?;
        decode_response(response).await
    }

    pub async fn configure_ports(
        &self,
        request: &ConfigurePortsRequest,
    ) -> Result<ConfigurePortsResponse> {
        let response = self
            .request(
                self.client
                    .put(self.url("/api/v1/config/ports"))
                    .json(request),
            )
            .send()
            .await
            .context("seriald port configuration update failed")?;
        decode_response(response).await
    }

    pub async fn events(&self, port: &str, query: &EventQuery) -> Result<EventQueryResponse> {
        let encoded_port = encode_path_segment(port);
        let response = self
            .request(
                self.client
                    .get(self.url(&format!("/api/v1/ports/{encoded_port}/events")))
                    .query(query),
            )
            .send()
            .await
            .context("seriald event query failed")?;
        decode_response(response).await
    }

    pub async fn live_tail(
        &self,
        port: &str,
        tail_events: usize,
        cursor: Option<&Cursor>,
    ) -> Result<EventQueryResponse> {
        let encoded_port = encode_path_segment(port);
        let mut query = vec![("tail_events", tail_events.clamp(1, 2_000).to_string())];
        if let Some(cursor) = cursor {
            query.push(("epoch", cursor.epoch.to_string()));
            query.push(("after_seq", cursor.after_seq.to_string()));
        }
        let response = self
            .request(
                self.client
                    .get(self.url(&format!("/api/v1/ports/{encoded_port}/tail")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald live-tail request failed")?;
        decode_response(response).await
    }

    pub async fn recent_activity(
        &self,
        port: &str,
        epoch: uuid::Uuid,
        after_seq: u64,
        through_seq: u64,
    ) -> Result<EventQueryResponse> {
        let encoded_port = encode_path_segment(port);
        let query = [
            ("epoch", epoch.to_string()),
            ("after_seq", after_seq.to_string()),
            ("through_seq", through_seq.to_string()),
        ];
        let response = self
            .request(
                self.client
                    .get(self.url(&format!("/api/v1/ports/{encoded_port}/recent-activity")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald recent-activity request failed")?;
        decode_response(response).await
    }

    pub async fn archives(&self, port: Option<&str>) -> Result<ArchiveListResponse> {
        let query: Vec<(&str, &str)> = port.map(|id| vec![("port", id)]).unwrap_or_default();
        let response = self
            .request(self.client.get(self.url("/api/v1/archives")).query(&query))
            .send()
            .await
            .context("seriald archive list failed")?;
        decode_response(response).await
    }

    pub async fn create_monitor(&self, request: &CreateMonitorRequest) -> Result<MonitorResponse> {
        let mut first_error = None;
        for attempt in 0..2 {
            match self
                .request(self.client.post(self.url("/api/v1/monitors")).json(request))
                .send()
                .await
            {
                Ok(response) => return decode_monitor_collection_response(response).await,
                Err(error) if attempt == 0 => first_error = Some(error.to_string()),
                Err(error) => {
                    bail!(
                        "seriald Monitor creation failed after one idempotent transport retry; \
                         request_id {} was reused (first error: {}; retry error: {error})",
                        request.request_id,
                        first_error.as_deref().unwrap_or("unavailable")
                    );
                }
            }
        }
        unreachable!("the bounded Monitor creation retry loop always returns")
    }

    pub async fn monitors(&self, port: Option<&str>) -> Result<MonitorListResponse> {
        let query: Vec<(&str, &str)> = port.map(|id| vec![("port", id)]).unwrap_or_default();
        let response = self
            .request(self.client.get(self.url("/api/v1/monitors")).query(&query))
            .send()
            .await
            .context("seriald Monitor list failed")?;
        decode_monitor_collection_response(response).await
    }

    pub async fn monitor(&self, monitor_id: uuid::Uuid) -> Result<MonitorResponse> {
        let encoded_id = encode_path_segment(&monitor_id.to_string());
        let response = self
            .request(
                self.client
                    .get(self.url(&format!("/api/v1/monitors/{encoded_id}"))),
            )
            .send()
            .await
            .context("seriald Monitor status failed")?;
        decode_response(response).await
    }

    pub async fn monitor_incidents(
        &self,
        monitor_id: uuid::Uuid,
        after_incident_seq: Option<u64>,
    ) -> Result<MonitorIncidentListResponse> {
        let encoded_id = encode_path_segment(&monitor_id.to_string());
        let mut query = vec![
            ("limit", MONITOR_INCIDENT_PAGE_LIMIT.to_string()),
            ("include_acked", "true".to_string()),
        ];
        if let Some(after) = after_incident_seq {
            query.push(("after_incident_seq", after.to_string()));
        }
        let response = self
            .request(
                self.client
                    .get(self.url(&format!("/api/v1/monitors/{encoded_id}/incidents")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald Monitor incident query failed")?;
        decode_response(response).await
    }

    pub async fn stop_monitor(
        &self,
        monitor_id: uuid::Uuid,
        expected_revision: u64,
    ) -> Result<MonitorResponse> {
        let encoded_id = encode_path_segment(&monitor_id.to_string());
        let query = [("expected_revision", expected_revision.to_string())];
        let response = self
            .request(
                self.client
                    .delete(self.url(&format!("/api/v1/monitors/{encoded_id}")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald Monitor stop failed")?;
        decode_response(response).await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .request(self.client.get(self.url(path)))
            .send()
            .await
            .with_context(|| format!("request to {path} failed"))?;
        decode_response(response).await
    }

    fn request(&self, request: RequestBuilder) -> RequestBuilder {
        request
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.endpoint, path)
    }
}

pub fn normalize_endpoint(endpoint: &str) -> Result<String> {
    let mut url = reqwest::Url::parse(endpoint.trim()).context("invalid seriald endpoint URL")?;
    if url.scheme() != "http" {
        bail!("seriald v1 endpoints must use http:// on loopback or a trusted host-only network");
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
        return Err(api_http_error(status, body).into());
    }
    response
        .json::<T>()
        .await
        .context("seriald returned an invalid JSON response")
}

async fn decode_monitor_collection_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T> {
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| "response body unavailable".into());
        let is_seriald_error = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .is_some_and(|value| {
                value
                    .get("code")
                    .and_then(serde_json::Value::as_str)
                    .is_some()
                    && value
                        .get("message")
                        .and_then(serde_json::Value::as_str)
                        .is_some()
            });
        if is_seriald_error {
            return Err(api_http_error(status, body).into());
        }
        bail!(
            "seriald does not expose the Monitor API; Monitor tools require seriald 0.5 or newer: {}",
            body.trim()
        );
    }
    decode_response(response).await
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
    fn macos_and_linux_device_paths_are_encoded_as_one_route_segment() {
        assert_eq!(
            encode_path_segment("/dev/cu.usbserial-210"),
            "%2Fdev%2Fcu.usbserial-210"
        );
        assert_eq!(
            encode_path_segment("/dev/serial/by-id/usb ACME"),
            "%2Fdev%2Fserial%2Fby-id%2Fusb%20ACME"
        );
    }

    #[test]
    fn endpoint_is_an_http_origin_without_credentials_or_path() {
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:3210/").unwrap(),
            "http://127.0.0.1:3210"
        );
        assert!(normalize_endpoint("https://127.0.0.1:3210").is_err());
        assert!(normalize_endpoint("http://user@127.0.0.1:3210").is_err());
        assert!(normalize_endpoint("http://127.0.0.1:3210/api").is_err());
    }
}
