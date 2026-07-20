use anyhow::{Context, Result, bail};
use reqwest::{Client, RequestBuilder};
use serial_protocol::{EventQuery, EventQueryResponse, StatusResponse};

#[derive(Clone)]
pub struct ApiClient {
    client: Client,
    endpoint: String,
    token: String,
}

impl ApiClient {
    pub fn new(endpoint: String, token: String) -> Result<Self> {
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

    pub fn token(&self) -> &str {
        &self.token
    }

    pub async fn status(&self) -> Result<StatusResponse> {
        self.get_json("/api/v1/status").await
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

    async fn get_json<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let response = self
            .authorize(self.client.get(self.url(path)))
            .send()
            .await
            .with_context(|| format!("request to {path} failed"))?;
        decode_response(response).await
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        request.bearer_auth(&self.token)
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
        bail!("seriald returned {status}: {}", body.trim());
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
}
