#![forbid(unsafe_code)]

//! Authentication and authorization for the coordinator control plane.
//!
//! Authentication is **layered**: a request is authenticated by the first
//! credential it presents, in decreasing order of strength —
//!
//! 1. **OIDC / JWT bearer token** (`Authorization: Bearer …`), validated by
//!    [`crate::security::jwt`] against a rotatable key set.
//! 2. **SPIFFE X.509-SVID** from the verified mTLS peer certificate, mapped to
//!    roles by [`crate::security::spiffe`].
//! 3. **Dev metadata headers** (`x-dtruntime-subject/role/worker-id`) — a
//!    convenience for local development, disabled in hardened deployments
//!    because headers are caller-asserted, not cryptographically proven.
//!
//! Authorization is then role/permission based ([`crate::security::rbac`]): the
//! resulting [`AuthContext`] carries verified roles, and each RPC requires a
//! [`Permission`] that at least one of those roles must grant.

use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use crate::security::jwt::JwtValidator;
use crate::security::rbac::{self, Role};
use crate::security::spiffe::SpiffePolicy;

pub use crate::security::rbac::Permission;

/// Whether authorization is enforced. `Metadata` is retained as the historical
/// name for "enforced" so existing flags/config keep working.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication is performed; every request is anonymous (dev/local).
    Disabled,
    /// Authentication is enforced via the layered credential sources.
    Metadata,
}

impl std::str::FromStr for AuthMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disabled" | "off" | "none" => Ok(Self::Disabled),
            "metadata" | "enforced" | "on" => Ok(Self::Metadata),
            _ => Err(format!("unsupported auth mode: {value}")),
        }
    }
}

/// Which credential proved the caller's identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialSource {
    Jwt,
    Spiffe,
    Metadata,
}

impl CredentialSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Jwt => "jwt",
            Self::Spiffe => "spiffe",
            Self::Metadata => "metadata",
        }
    }
}

/// A verified caller identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub subject: String,
    pub roles: Vec<Role>,
    pub worker_id: Option<String>,
    pub source: CredentialSource,
}

impl AuthContext {
    /// True if any of the caller's roles grants `permission`.
    pub fn has_permission(&self, permission: Permission) -> bool {
        rbac::any_grants(&self.roles, permission)
    }

    /// True if the caller holds the `worker` role.
    pub fn is_worker(&self) -> bool {
        self.roles.contains(&Role::Worker)
    }

    /// A stable label for audit logging (highest-privilege role first).
    pub fn role_label(&self) -> &'static str {
        // roles are sorted with Admin first (see rbac::Role ordering).
        self.roles.first().map(|r| r.as_str()).unwrap_or("none")
    }
}

/// An authentication/authorization failure. `Copy` — every variant carries only
/// `'static` context, so no token/key material can leak through it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationError {
    MissingIdentity,
    InvalidRole,
    PermissionDenied,
    WorkerIdentityMismatch,
    /// A bearer token was rejected; the `&str` is the safe reason.
    TokenRejected(&'static str),
    /// A SPIFFE SVID was rejected; the `&str` is the safe reason.
    SpiffeRejected(&'static str),
    /// A bearer token was presented but no JWT validator is configured.
    BearerUnsupported,
}

impl AuthorizationError {
    pub fn to_status(self) -> Status {
        match self {
            Self::PermissionDenied | Self::WorkerIdentityMismatch => {
                Status::new(Code::PermissionDenied, self.as_str())
            }
            _ => Status::new(Code::Unauthenticated, self.as_str()),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MissingIdentity => "missing caller identity",
            Self::InvalidRole => "invalid or unrecognized caller role",
            Self::PermissionDenied => "caller is not authorized for this operation",
            Self::WorkerIdentityMismatch => "worker identity does not match request worker_id",
            Self::TokenRejected(reason) => reason,
            Self::SpiffeRejected(reason) => reason,
            Self::BearerUnsupported => "bearer token presented but JWT auth is not configured",
        }
    }
}

/// Composes the credential sources into a single authentication decision.
#[derive(Clone)]
pub struct Authenticator {
    mode: AuthMode,
    jwt: Option<Arc<JwtValidator>>,
    spiffe: Option<Arc<SpiffePolicy>>,
    /// Accept dev metadata headers as a fallback identity.
    allow_metadata_fallback: bool,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("mode", &self.mode)
            .field("jwt", &self.jwt.is_some())
            .field("spiffe", &self.spiffe.is_some())
            .field("allow_metadata_fallback", &self.allow_metadata_fallback)
            .finish()
    }
}

impl Authenticator {
    pub fn new(mode: AuthMode) -> Self {
        Self {
            mode,
            jwt: None,
            spiffe: None,
            allow_metadata_fallback: true,
        }
    }

    pub fn with_jwt(mut self, validator: Arc<JwtValidator>) -> Self {
        self.jwt = Some(validator);
        self
    }

    pub fn with_spiffe(mut self, policy: Arc<SpiffePolicy>) -> Self {
        self.spiffe = Some(policy);
        self
    }

    /// Enable/disable the dev metadata-header fallback. Production deployments
    /// disable it so only cryptographically-proven credentials are honored.
    pub fn with_metadata_fallback(mut self, allow: bool) -> Self {
        self.allow_metadata_fallback = allow;
        self
    }

    /// Authenticate a request. `peer_cert_der` is the DER of the verified mTLS
    /// leaf certificate, when present. Returns `Ok(None)` only when auth is
    /// disabled (anonymous access).
    pub fn authenticate(
        &self,
        metadata: &MetadataMap,
        peer_cert_der: Option<&[u8]>,
    ) -> Result<Option<AuthContext>, AuthorizationError> {
        if self.mode == AuthMode::Disabled {
            return Ok(None);
        }

        // 1) Bearer token wins if present.
        if let Some(token) = extract_bearer(metadata) {
            let validator = self
                .jwt
                .as_ref()
                .ok_or(AuthorizationError::BearerUnsupported)?;
            let verified = validator
                .validate(token)
                .map_err(|e| AuthorizationError::TokenRejected(e.as_str()))?;
            return Ok(Some(AuthContext {
                subject: verified.subject,
                roles: verified.roles,
                worker_id: verified.worker_id,
                source: CredentialSource::Jwt,
            }));
        }

        // 2) SPIFFE SVID from the mTLS peer certificate.
        if let Some(policy) = &self.spiffe {
            if let Some(der) = peer_cert_der {
                let identity = policy
                    .authenticate(der)
                    .map_err(|e| AuthorizationError::SpiffeRejected(e.as_str()))?;
                return Ok(Some(AuthContext {
                    subject: identity.spiffe_id,
                    roles: identity.roles,
                    worker_id: identity.worker_id,
                    source: CredentialSource::Spiffe,
                }));
            }
            // SPIFFE configured but no client certificate — fall through to the
            // dev fallback if allowed, otherwise reject.
        }

        // 3) Dev metadata headers.
        if self.allow_metadata_fallback {
            return self.metadata_identity(metadata).map(Some);
        }

        Err(AuthorizationError::MissingIdentity)
    }

    fn metadata_identity(&self, metadata: &MetadataMap) -> Result<AuthContext, AuthorizationError> {
        let subject = metadata
            .get("x-dtruntime-subject")
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or(AuthorizationError::MissingIdentity)?;

        let role_value = metadata
            .get("x-dtruntime-role")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let roles = rbac::parse_roles([role_value]);
        if roles.is_empty() {
            return Err(AuthorizationError::InvalidRole);
        }

        let worker_id = metadata
            .get("x-dtruntime-worker-id")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        Ok(AuthContext {
            subject: subject.to_string(),
            roles,
            worker_id,
            source: CredentialSource::Metadata,
        })
    }
}

/// Backwards-compatible alias: the type was historically named `Authorizer`.
pub type Authorizer = Authenticator;

/// Extract a bearer token from the `authorization` metadata (case-insensitive
/// scheme). Returns `None` when absent or malformed.
fn extract_bearer(metadata: &MetadataMap) -> Option<&str> {
    let value = metadata.get("authorization")?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() {
        Some(token.trim())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::jwt::JwtConfig;
    use crate::security::keyset::{KeySource, ReloadableKeySet};
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

    const SECRET: &[u8] = b"integration-secret";

    fn metadata_with(pairs: &[(&str, &str)]) -> MetadataMap {
        use tonic::metadata::{MetadataKey, MetadataValue};
        let mut m = MetadataMap::new();
        for (k, v) in pairs {
            let key = MetadataKey::from_bytes(k.as_bytes()).unwrap();
            let value: MetadataValue<_> = v.parse().unwrap();
            m.insert(key, value);
        }
        m
    }

    fn jwt_validator() -> (Arc<JwtValidator>, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, SECRET).unwrap();
        let keys = Arc::new(
            ReloadableKeySet::load(KeySource::HmacSecret {
                path,
                algorithm: Algorithm::HS256,
            })
            .unwrap(),
        );
        (Arc::new(JwtValidator::new(JwtConfig::default(), keys)), dir)
    }

    fn bearer(roles: serde_json::Value) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({"sub":"svc","exp":now+300,"roles":roles}),
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap();
        format!("Bearer {token}")
    }

    #[test]
    fn disabled_mode_is_anonymous() {
        let auth = Authenticator::new(AuthMode::Disabled);
        assert_eq!(auth.authenticate(&MetadataMap::new(), None), Ok(None));
    }

    #[test]
    fn jwt_bearer_is_authenticated_over_metadata() {
        let (validator, _d) = jwt_validator();
        let auth = Authenticator::new(AuthMode::Metadata).with_jwt(validator);
        // Both a bearer token and (spoofed) metadata role are present; the
        // cryptographic bearer token must win.
        let md = metadata_with(&[
            ("authorization", &bearer(serde_json::json!("operator"))),
            ("x-dtruntime-role", "admin"),
            ("x-dtruntime-subject", "attacker"),
        ]);
        let ctx = auth.authenticate(&md, None).unwrap().unwrap();
        assert_eq!(ctx.source, CredentialSource::Jwt);
        assert_eq!(ctx.subject, "svc");
        assert_eq!(ctx.roles, vec![Role::Operator]);
    }

    #[test]
    fn bearer_without_validator_is_rejected() {
        let auth = Authenticator::new(AuthMode::Metadata);
        let md = metadata_with(&[("authorization", &bearer(serde_json::json!("admin")))]);
        assert_eq!(
            auth.authenticate(&md, None),
            Err(AuthorizationError::BearerUnsupported)
        );
    }

    #[test]
    fn invalid_bearer_token_is_rejected() {
        let (validator, _d) = jwt_validator();
        let auth = Authenticator::new(AuthMode::Metadata).with_jwt(validator);
        let md = metadata_with(&[("authorization", "Bearer not-a-jwt")]);
        assert!(matches!(
            auth.authenticate(&md, None),
            Err(AuthorizationError::TokenRejected(_))
        ));
    }

    #[test]
    fn metadata_fallback_when_no_bearer() {
        let auth = Authenticator::new(AuthMode::Metadata);
        let md = metadata_with(&[
            ("x-dtruntime-subject", "dev"),
            ("x-dtruntime-role", "viewer"),
        ]);
        let ctx = auth.authenticate(&md, None).unwrap().unwrap();
        assert_eq!(ctx.source, CredentialSource::Metadata);
        assert_eq!(ctx.roles, vec![Role::Viewer]);
    }

    #[test]
    fn metadata_fallback_can_be_disabled() {
        let auth = Authenticator::new(AuthMode::Metadata).with_metadata_fallback(false);
        let md = metadata_with(&[
            ("x-dtruntime-subject", "dev"),
            ("x-dtruntime-role", "admin"),
        ]);
        assert_eq!(
            auth.authenticate(&md, None),
            Err(AuthorizationError::MissingIdentity)
        );
    }

    #[test]
    fn unknown_metadata_role_is_invalid() {
        let auth = Authenticator::new(AuthMode::Metadata);
        let md = metadata_with(&[
            ("x-dtruntime-subject", "dev"),
            ("x-dtruntime-role", "superuser"),
        ]);
        assert_eq!(
            auth.authenticate(&md, None),
            Err(AuthorizationError::InvalidRole)
        );
    }

    #[test]
    fn permission_checks_follow_roles() {
        let ctx = AuthContext {
            subject: "x".into(),
            roles: vec![Role::Viewer],
            worker_id: None,
            source: CredentialSource::Metadata,
        };
        assert!(ctx.has_permission(Permission::Read));
        assert!(!ctx.has_permission(Permission::Manage));
        assert_eq!(ctx.role_label(), "viewer");
    }
}
