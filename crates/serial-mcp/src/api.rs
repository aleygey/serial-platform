use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder};
use serde_json::{Map, Value, json};
use serial_protocol::{
    ArchiveListResponse, CreateMonitorRequest, Cursor, DeviceModelListResponse, EventQuery,
    EventQueryResponse, MonitorIncidentListResponse, MonitorListResponse, MonitorResponse,
    SetSlotDeviceModelRequest, SetSlotDeviceModelResponse, StatusResponse,
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
    token: Option<String>,
}

impl ApiClient {
    pub fn new(endpoint: String, token: Option<String>) -> Result<Self> {
        let endpoint = normalize_endpoint(&endpoint)?;
        Ok(Self {
            client: Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(15))
                .build()?,
            endpoint,
            token,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get_json("/api/v1/status").await
    }

    pub async fn device_models(&self) -> Result<DeviceModelListResponse> {
        self.get_json("/api/v1/config/device-models").await
    }

    /// Compatibility probe used by the long-standing `devices` tool. A 404
    /// means this daemon predates the additive model catalog; other transport,
    /// authentication, and decoding failures remain visible to the caller.
    pub async fn device_models_if_supported(&self) -> Result<Option<DeviceModelListResponse>> {
        let response = self
            .authorize(self.client.get(self.url("/api/v1/config/device-models")))
            .send()
            .await
            .context("seriald device model capability probe failed")?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode_response(response).await.map(Some)
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
            .context("seriald Slot model binding request failed")?;
        decode_response(response).await
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
            .context("seriald event query failed")?;
        decode_response(response).await
    }

    pub async fn live_tail(
        &self,
        slot_id: &str,
        tail_events: usize,
        cursor: Option<&Cursor>,
    ) -> Result<EventQueryResponse> {
        let encoded_slot = encode_path_segment(slot_id);
        let mut query = vec![("tail_events", tail_events.clamp(1, 2_000).to_string())];
        if let Some(cursor) = cursor {
            query.push(("epoch", cursor.epoch.to_string()));
            query.push(("after_seq", cursor.after_seq.to_string()));
        }
        let response = self
            .authorize(
                self.client
                    .get(self.url(&format!("/api/v1/slots/{encoded_slot}/tail")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald live-tail request failed")?;
        decode_response(response).await
    }

    pub async fn recent_activity(
        &self,
        slot_id: &str,
        epoch: uuid::Uuid,
        after_seq: u64,
        through_seq: u64,
    ) -> Result<EventQueryResponse> {
        let encoded_slot = encode_path_segment(slot_id);
        let query = [
            ("epoch", epoch.to_string()),
            ("after_seq", after_seq.to_string()),
            ("through_seq", through_seq.to_string()),
        ];
        let response = self
            .authorize(
                self.client
                    .get(self.url(&format!("/api/v1/slots/{encoded_slot}/recent-activity")))
                    .query(&query),
            )
            .send()
            .await
            .context("seriald recent-activity request failed")?;
        decode_response(response).await
    }

    pub async fn archives(&self, slot_id: Option<&str>) -> Result<ArchiveListResponse> {
        let query: Vec<(&str, &str)> = slot_id.map(|id| vec![("slot_id", id)]).unwrap_or_default();
        let response = self
            .authorize(self.client.get(self.url("/api/v1/archives")).query(&query))
            .send()
            .await
            .context("seriald archive list failed")?;
        decode_response(response).await
    }

    pub async fn create_monitor(&self, request: &CreateMonitorRequest) -> Result<MonitorResponse> {
        let mut first_error = None;
        for attempt in 0..2 {
            match self
                .authorize(self.client.post(self.url("/api/v1/monitors")).json(request))
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

    pub async fn monitors(&self, slot_id: Option<&str>) -> Result<MonitorListResponse> {
        let query: Vec<(&str, &str)> = slot_id.map(|id| vec![("slot_id", id)]).unwrap_or_default();
        let response = self
            .authorize(self.client.get(self.url("/api/v1/monitors")).query(&query))
            .send()
            .await
            .context("seriald Monitor list failed")?;
        decode_monitor_collection_response(response).await
    }

    pub async fn monitor(&self, monitor_id: uuid::Uuid) -> Result<MonitorResponse> {
        let encoded_id = encode_path_segment(&monitor_id.to_string());
        let response = self
            .authorize(
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
            .authorize(
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
            .authorize(
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
            .authorize(self.client.get(self.url(path)))
            .send()
            .await
            .with_context(|| format!("request to {path} failed"))?;
        decode_response(response).await
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn api_client_with_response(
        status: &str,
        body: &str,
    ) -> (ApiClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let status = status.to_owned();
        let body = body.to_owned();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept test request");
            let mut request = [0_u8; 2048];
            let _ = stream.read(&mut request).await.expect("read test request");
            let response = format!(
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .await
                .expect("write test response");
        });
        (
            ApiClient::new(format!("http://{address}"), Some("test-token".to_owned()))
                .expect("construct API client"),
            server,
        )
    }

    #[test]
    fn endpoints_are_restricted_to_plain_http_origins() {
        assert_eq!(
            normalize_endpoint(" http://127.0.0.1:3210/ ").unwrap(),
            "http://127.0.0.1:3210"
        );
        for endpoint in [
            "https://127.0.0.1:3210",
            "http://user@127.0.0.1:3210",
            "http://127.0.0.1:3210/base",
            "http://127.0.0.1:3210?token=bad",
        ] {
            assert!(normalize_endpoint(endpoint).is_err(), "accepted {endpoint}");
        }
    }

    #[tokio::test]
    async fn monitor_collection_404_reports_missing_capability() {
        let (client, server) = api_client_with_response("404 Not Found", "route not found").await;

        let error = client.monitors(None).await.unwrap_err().to_string();
        server.await.expect("test server completed");

        assert!(error.contains("does not expose the Monitor API"));
        assert!(error.contains("require seriald 0.5 or newer"));
    }

    #[tokio::test]
    async fn query_budget_error_is_structured_without_nested_escaped_json() {
        let body = r#"{"code":"query_budget_exceeded","message":"journal query budget was exceeded during segment discovery after 104134 bytes and 5130 ms","retryable":true,"phase":"segment discovery","scanned_bytes":104134,"elapsed_ms":5130,"retry_hint":"retry from the last cursor"}"#;
        let (client, server) = api_client_with_response("429 Too Many Requests", body).await;

        let error = client.status().await.unwrap_err();
        server.await.expect("test server completed");
        let display = error.to_string();
        assert!(display.contains("query_budget_exceeded"));
        assert!(display.contains("segment discovery"));
        assert!(!display.contains(r#"\{"code\""#));

        let structured = structured_http_error(&error).expect("typed seriald HTTP error");
        assert_eq!(structured["error"]["http_status"], 429);
        assert_eq!(structured["error"]["code"], "query_budget_exceeded");
        assert_eq!(structured["error"]["phase"], "segment discovery");
        assert_eq!(structured["error"]["scanned_bytes"], 104134);
        assert_eq!(structured["error"]["elapsed_ms"], 5130);
        assert_eq!(
            structured["error"]["retry_hint"],
            "retry from the last cursor"
        );
        assert_eq!(structured["error"]["retryable"], true);
    }

    #[tokio::test]
    async fn monitor_collection_structured_404_preserves_seriald_error() {
        let body = r#"{"code":"not_found","message":"unknown Slot missing-slot"}"#;
        let (client, server) = api_client_with_response("404 Not Found", body).await;

        let request = CreateMonitorRequest {
            request_id: uuid::Uuid::new_v4(),
            spec: serial_protocol::MonitorSpec {
                slot_id: "missing-slot".to_owned(),
                contains: Some("panic".to_owned()),
                regex: None,
                start_cursor: None,
                severity: serial_protocol::MonitorSeverity::Warning,
                description: None,
                debounce_ms: 250,
                cooldown_ms: 30_000,
                duration_ms: None,
            },
        };
        let error = client
            .create_monitor(&request)
            .await
            .unwrap_err()
            .to_string();
        server.await.expect("test server completed");

        assert!(error.contains("seriald returned 404 Not Found"));
        assert!(error.contains("unknown Slot missing-slot"));
        assert!(!error.contains("does not expose the Monitor API"));
    }

    #[tokio::test]
    async fn monitor_entity_404_preserves_seriald_not_found() {
        let monitor_id = uuid::Uuid::new_v4();
        let body =
            format!(r#"{{"code":"not_found","message":"Monitor Job {monitor_id} was not found"}}"#);
        let (client, server) = api_client_with_response("404 Not Found", &body).await;

        let error = client.monitor(monitor_id).await.unwrap_err().to_string();
        server.await.expect("test server completed");

        assert!(error.contains("seriald returned 404 Not Found"));
        assert!(error.contains(&monitor_id.to_string()));
        assert!(error.contains("Monitor Job"));
        assert!(!error.contains("does not expose the Monitor API"));
    }

    #[tokio::test]
    async fn monitor_incidents_404_preserves_seriald_not_found() {
        let monitor_id = uuid::Uuid::new_v4();
        let body =
            format!(r#"{{"code":"not_found","message":"Monitor Job {monitor_id} was not found"}}"#);
        let (client, server) = api_client_with_response("404 Not Found", &body).await;

        let error = client
            .monitor_incidents(monitor_id, None)
            .await
            .unwrap_err()
            .to_string();
        server.await.expect("test server completed");

        assert!(error.contains("seriald returned 404 Not Found"));
        assert!(error.contains(&monitor_id.to_string()));
        assert!(!error.contains("does not expose the Monitor API"));
    }

    #[tokio::test]
    async fn monitor_stop_404_preserves_seriald_not_found() {
        let monitor_id = uuid::Uuid::new_v4();
        let body =
            format!(r#"{{"code":"not_found","message":"Monitor Job {monitor_id} was not found"}}"#);
        let (client, server) = api_client_with_response("404 Not Found", &body).await;

        let error = client
            .stop_monitor(monitor_id, 1)
            .await
            .unwrap_err()
            .to_string();
        server.await.expect("test server completed");

        assert!(error.contains("seriald returned 404 Not Found"));
        assert!(error.contains(&monitor_id.to_string()));
        assert!(!error.contains("does not expose the Monitor API"));
    }

    #[tokio::test]
    async fn device_model_catalog_uses_the_observer_read_endpoint() {
        let body =
            r#"{"models":[{"id":"family","name":"Family"}],"bindings":[],"config_revision":7}"#;
        let (client, server) = api_client_with_response("200 OK", body).await;

        let catalog = client.device_models().await.unwrap();
        server.await.expect("test server completed");

        assert_eq!(catalog.config_revision, 7);
        assert_eq!(catalog.models[0].id, "family");
        assert!(catalog.bindings.is_empty());
    }

    #[tokio::test]
    async fn optional_device_model_probe_treats_only_not_found_as_unsupported() {
        let (client, server) = api_client_with_response("404 Not Found", "route not found").await;

        let catalog = client.device_models_if_supported().await.unwrap();
        server.await.expect("test server completed");

        assert!(catalog.is_none());
    }

    #[tokio::test]
    async fn device_model_binding_decodes_the_operator_response() {
        let body = r#"{"binding":{"slot_id":"bench","model_id":"variant","confirmation_method":"serial","updated_wall_time_ns":1,"source":"agent:serial-mcp"},"model":{"id":"variant","name":"Variant"},"created":true,"config_revision":8}"#;
        let (client, server) = api_client_with_response("200 OK", body).await;
        let request = SetSlotDeviceModelRequest {
            model_id: Some("variant".into()),
            create_if_missing: true,
            update_existing: false,
            name: Some("Variant".into()),
            parent_id: None,
            clear_parent: false,
            aliases: Vec::new(),
            clear_aliases: false,
            confirmation_method: Some(serial_protocol::ModelConfirmationMethod::Serial),
            note: None,
            source: "agent:serial-mcp".into(),
            expected_revision: Some(7),
            expected_current: Some(None),
        };

        let response = client
            .set_slot_device_model("bench", &request)
            .await
            .unwrap();
        server.await.expect("test server completed");

        assert!(response.created);
        assert_eq!(response.config_revision, 8);
        assert_eq!(response.binding.unwrap().model_id, "variant");
    }

    #[test]
    fn device_model_binding_path_encodes_slot_ids() {
        assert_eq!(encode_path_segment("bench / A"), "bench%20%2F%20A");
    }
}
