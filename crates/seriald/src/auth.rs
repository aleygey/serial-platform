//! Authentication and server-owned actor identities.
//!
//! Bearer tokens are persisted by the configuration module, but their wrapper
//! deliberately never implements `Display` and always redacts `Debug` output.
//! Plaintext credentials leave this module only through [`CredentialDisplay`],
//! which is intended for first-run output or an explicit credentials command.

use std::fmt;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::RngCore as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serial_protocol::{Actor, ActorKind, Role};
use thiserror::Error;
use uuid::Uuid;

const TOKEN_BYTES: usize = 32;
const TOKEN_ENCODED_BYTES: usize = 43;
const MAX_ACTOR_LABEL_BYTES: usize = 96;

/// A bearer token that is safe to include in otherwise-debuggable structures.
///
/// Serialization is implemented because the daemon must persist the token.
/// Callers cannot obtain the inner value except through the deliberately named
/// credential-display API on [`AuthConfig`].
#[derive(Clone, PartialEq, Eq)]
struct BearerToken(String);

impl BearerToken {
    fn generate() -> Self {
        let mut random = [0_u8; TOKEN_BYTES];
        rand::rng().fill_bytes(&mut random);
        Self(URL_SAFE_NO_PAD.encode(random))
    }

    fn parse(value: String) -> Result<Self, &'static str> {
        if !is_valid_token_text(&value) {
            return Err("bearer token is not a 256-bit base64url value");
        }
        Ok(Self(value))
    }

    fn matches(&self, candidate: &str) -> bool {
        constant_time_eq(self.0.as_bytes(), candidate.as_bytes())
    }

    fn reveal_for_explicit_display(&self) -> String {
        self.0.clone()
    }
}

impl fmt::Debug for BearerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerToken([REDACTED])")
    }
}

impl Serialize for BearerToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for BearerToken {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(D::Error::custom)
    }
}

/// Persisted credentials for the three v1 permission levels.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    observer_token: BearerToken,
    operator_token: BearerToken,
    admin_token: BearerToken,
}

impl AuthConfig {
    /// Generates independent 256-bit credentials and a one-time display DTO.
    #[must_use]
    pub fn generate() -> (Self, CredentialDisplay) {
        let config = Self {
            observer_token: BearerToken::generate(),
            operator_token: BearerToken::generate(),
            admin_token: BearerToken::generate(),
        };
        let display = config.credentials_for_explicit_display();
        (config, display)
    }

    /// Validates invariants that cannot be expressed by TOML deserialization.
    pub fn validate(&self) -> Result<(), AuthError> {
        let observer = self.observer_token.0.as_bytes();
        let operator = self.operator_token.0.as_bytes();
        let admin = self.admin_token.0.as_bytes();
        if constant_time_eq(observer, operator)
            || constant_time_eq(observer, admin)
            || constant_time_eq(operator, admin)
        {
            return Err(AuthError::DuplicateCredentials);
        }
        Ok(())
    }

    /// Authenticates a complete HTTP `Authorization` header.
    ///
    /// Parsing errors and credential failures never include the supplied value.
    pub fn authenticate_authorization(
        &self,
        authorization: Option<&str>,
    ) -> Result<Principal, AuthError> {
        let bearer = parse_bearer_authorization(authorization)?;
        self.authenticate_bearer(bearer)
    }

    /// Authenticates a token already extracted from a trusted transport layer.
    pub fn authenticate_bearer(&self, bearer: &str) -> Result<Principal, AuthError> {
        // Always compare all three values so a failed request does not reveal
        // which role prefix was closest through early-return timing.
        let observer = self.observer_token.matches(bearer);
        let operator = self.operator_token.matches(bearer);
        let admin = self.admin_token.matches(bearer);

        let credential = if admin {
            CredentialId::Admin
        } else if operator {
            CredentialId::Operator
        } else if observer {
            CredentialId::Observer
        } else {
            return Err(AuthError::InvalidCredentials);
        };

        Ok(Principal { credential })
    }

    /// Explicitly materializes plaintext credentials for first-run output or a
    /// user-invoked credentials command. The DTO intentionally has no `Debug`
    /// or `Display` implementation.
    #[must_use]
    pub fn credentials_for_explicit_display(&self) -> CredentialDisplay {
        CredentialDisplay {
            observer_token: self.observer_token.reveal_for_explicit_display(),
            operator_token: self.operator_token.reveal_for_explicit_display(),
            admin_token: self.admin_token.reveal_for_explicit_display(),
        }
    }
}

/// Plaintext credentials intended only for an explicit display path.
///
/// This type deliberately does not implement `Debug` or `Display`, preventing
/// accidental inclusion through common structured-logging fields.
#[derive(Clone, Serialize)]
pub struct CredentialDisplay {
    observer_token: String,
    operator_token: String,
    admin_token: String,
}

impl CredentialDisplay {
    #[must_use]
    pub fn observer_token(&self) -> &str {
        &self.observer_token
    }

    #[must_use]
    pub fn operator_token(&self) -> &str {
        &self.operator_token
    }

    #[must_use]
    pub fn admin_token(&self) -> &str {
        &self.admin_token
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CredentialId {
    Observer,
    Operator,
    Admin,
}

/// An authenticated server-side principal. It contains no bearer token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Principal {
    credential: CredentialId,
}

impl Principal {
    #[must_use]
    pub fn role(self) -> Role {
        match self.credential {
            CredentialId::Observer => Role::Observer,
            CredentialId::Operator => Role::Operator,
            CredentialId::Admin => Role::Admin,
        }
    }

    pub fn require_role(self, required: Role) -> Result<(), AuthError> {
        if role_allows(self.role(), required) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Issues a fresh actor identity for one authenticated connection.
    ///
    /// `kind` is an authenticated client's audit declaration. The server
    /// validates it, rejects the reserved `System` kind, and always creates the
    /// opaque actor ID. A shared bearer credential does not make Human/Agent/
    /// Script declarations independently verifiable identities.
    pub fn issue_actor(
        self,
        kind: ActorKind,
        requested_label: &str,
    ) -> Result<AuthenticatedActor, AuthError> {
        if kind == ActorKind::System {
            return Err(AuthError::ReservedActorKind);
        }
        let label = normalize_actor_label(requested_label)?;
        let prefix = match kind {
            ActorKind::Human => "human",
            ActorKind::Agent => "agent",
            ActorKind::Script => "script",
            ActorKind::System => unreachable!("reserved actor kind rejected above"),
        };
        Ok(AuthenticatedActor {
            actor: Actor {
                id: format!("{prefix}:{}", Uuid::new_v4().simple()),
                label,
                kind,
            },
            role: self.role(),
        })
    }
}

/// Connection-bound identity used by request handlers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthenticatedActor {
    actor: Actor,
    role: Role,
}

impl AuthenticatedActor {
    #[must_use]
    pub fn actor(&self) -> &Actor {
        &self.actor
    }

    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    pub fn require_role(&self, required: Role) -> Result<(), AuthError> {
        if role_allows(self.role, required) {
            Ok(())
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Checks an actor carried by an internal command against the actor bound
    /// to this authenticated connection.
    pub fn validate_actor(&self, claimed: &Actor) -> Result<(), AuthError> {
        if &self.actor == claimed {
            Ok(())
        } else {
            Err(AuthError::ActorMismatch)
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("authorization header is required")]
    MissingAuthorization,
    #[error("authorization header is malformed")]
    MalformedAuthorization,
    #[error("authorization scheme must be Bearer")]
    UnsupportedAuthorizationScheme,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("configured bearer credentials must be distinct")]
    DuplicateCredentials,
    #[error("the authenticated role does not permit this operation")]
    Forbidden,
    #[error("actor label must be non-empty, short, and contain no control characters")]
    InvalidActorLabel,
    #[error("the system actor kind is reserved for seriald")]
    ReservedActorKind,
    #[error("actor identity does not belong to this authenticated connection")]
    ActorMismatch,
}

#[must_use]
pub fn role_allows(actual: Role, required: Role) -> bool {
    role_rank(actual) >= role_rank(required)
}

/// Extracts a bearer credential without ever including it in an error value.
pub fn parse_bearer_authorization(authorization: Option<&str>) -> Result<&str, AuthError> {
    let authorization = authorization.ok_or(AuthError::MissingAuthorization)?;
    if authorization
        .bytes()
        .any(|byte| matches!(byte, b'\r' | b'\n'))
    {
        return Err(AuthError::MalformedAuthorization);
    }

    let mut parts = authorization.trim().split_ascii_whitespace();
    let scheme = parts.next().ok_or(AuthError::MalformedAuthorization)?;
    let bearer = parts.next().ok_or(AuthError::MalformedAuthorization)?;
    if parts.next().is_some() || bearer.is_empty() {
        return Err(AuthError::MalformedAuthorization);
    }
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(AuthError::UnsupportedAuthorizationScheme);
    }
    Ok(bearer)
}

fn role_rank(role: Role) -> u8 {
    match role {
        Role::Observer => 0,
        Role::Operator => 1,
        Role::Admin => 2,
    }
}

fn normalize_actor_label(label: &str) -> Result<String, AuthError> {
    let label = label.trim();
    if label.is_empty()
        || label.len() > MAX_ACTOR_LABEL_BYTES
        || label.chars().any(char::is_control)
    {
        return Err(AuthError::InvalidActorLabel);
    }
    Ok(label.to_owned())
}

fn is_valid_token_text(value: &str) -> bool {
    if value.len() != TOKEN_ENCODED_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return false;
    }
    let Ok(decoded) = URL_SAFE_NO_PAD.decode(value) else {
        return false;
    };
    decoded.len() == TOKEN_BYTES
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut difference = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        difference |= usize::from(left_byte ^ right_byte);
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_credentials_are_unique_and_authenticate_at_the_expected_role() {
        let (auth, display) = AuthConfig::generate();
        assert_eq!(display.observer_token().len(), TOKEN_ENCODED_BYTES);
        assert_ne!(display.observer_token(), display.operator_token());
        assert_ne!(display.observer_token(), display.admin_token());
        assert_ne!(display.operator_token(), display.admin_token());

        assert_eq!(
            auth.authenticate_bearer(display.observer_token())
                .unwrap()
                .role(),
            Role::Observer
        );
        assert_eq!(
            auth.authenticate_bearer(display.operator_token())
                .unwrap()
                .role(),
            Role::Operator
        );
        assert_eq!(
            auth.authenticate_bearer(display.admin_token())
                .unwrap()
                .role(),
            Role::Admin
        );
        assert_eq!(
            auth.authenticate_bearer("not-a-token"),
            Err(AuthError::InvalidCredentials)
        );
    }

    #[test]
    fn debug_output_never_contains_plaintext_credentials() {
        let (auth, display) = AuthConfig::generate();
        let debug = format!("{auth:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(display.observer_token()));
        assert!(!debug.contains(display.operator_token()));
        assert!(!debug.contains(display.admin_token()));
    }

    #[test]
    fn bearer_header_parsing_is_strict_without_echoing_secrets() {
        assert_eq!(
            parse_bearer_authorization(Some("bearer abc_DEF-123")),
            Ok("abc_DEF-123")
        );
        assert_eq!(parse_bearer_authorization(Some("Bearer   abc")), Ok("abc"));
        assert_eq!(
            parse_bearer_authorization(None),
            Err(AuthError::MissingAuthorization)
        );
        assert_eq!(
            parse_bearer_authorization(Some("Basic abc")),
            Err(AuthError::UnsupportedAuthorizationScheme)
        );
        assert_eq!(
            parse_bearer_authorization(Some("Bearer abc extra")),
            Err(AuthError::MalformedAuthorization)
        );
        assert_eq!(
            parse_bearer_authorization(Some("Bearer abc\r\nInjected: yes")),
            Err(AuthError::MalformedAuthorization)
        );
    }

    #[test]
    fn roles_are_hierarchical() {
        assert!(role_allows(Role::Admin, Role::Observer));
        assert!(role_allows(Role::Admin, Role::Operator));
        assert!(role_allows(Role::Operator, Role::Observer));
        assert!(!role_allows(Role::Observer, Role::Operator));
        assert!(!role_allows(Role::Operator, Role::Admin));
    }

    #[test]
    fn actors_are_server_generated_and_connection_bound() {
        let (auth, display) = AuthConfig::generate();
        let principal = auth.authenticate_bearer(display.operator_token()).unwrap();
        let first = principal
            .issue_actor(ActorKind::Human, "  bench user  ")
            .unwrap();
        let second = principal
            .issue_actor(ActorKind::Human, "bench user")
            .unwrap();

        assert_eq!(first.actor().label, "bench user");
        assert_eq!(first.actor().kind, ActorKind::Human);
        assert_eq!(first.role(), Role::Operator);
        assert_ne!(first.actor().id, second.actor().id);
        assert_eq!(
            first.validate_actor(second.actor()),
            Err(AuthError::ActorMismatch)
        );
        assert_eq!(
            principal.issue_actor(ActorKind::System, "seriald"),
            Err(AuthError::ReservedActorKind)
        );
    }

    #[test]
    fn invalid_actor_labels_are_rejected() {
        let (auth, display) = AuthConfig::generate();
        let principal = auth.authenticate_bearer(display.admin_token()).unwrap();
        assert_eq!(
            principal.issue_actor(ActorKind::Human, "\n"),
            Err(AuthError::InvalidActorLabel)
        );
        assert_eq!(
            principal.issue_actor(ActorKind::Human, &"x".repeat(MAX_ACTOR_LABEL_BYTES + 1)),
            Err(AuthError::InvalidActorLabel)
        );
    }

    #[test]
    fn serialized_auth_can_be_loaded_without_exposing_tokens_through_debug() {
        let (auth, display) = AuthConfig::generate();
        let encoded = toml::to_string(&auth).unwrap();
        assert!(encoded.contains(display.admin_token()));
        let decoded: AuthConfig = toml::from_str(&encoded).unwrap();
        assert_eq!(
            decoded
                .authenticate_bearer(display.admin_token())
                .unwrap()
                .role(),
            Role::Admin
        );
        assert!(!format!("{decoded:?}").contains(display.admin_token()));
    }
}
