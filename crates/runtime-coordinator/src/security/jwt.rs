#![forbid(unsafe_code)]

//! OIDC / JWT bearer-token validation.
//!
//! The coordinator acts as an OAuth2 **resource server**: it does not mint
//! tokens, it validates them. A caller presents `Authorization: Bearer <jwt>`;
//! the coordinator verifies the signature against a rotatable
//! [`ReloadableKeySet`], enforces `exp`/`nbf`, and — when configured — the
//! `iss` and `aud` claims, then derives the caller's subject, roles, and
//! optional worker identity from claims. This is the standard integration
//! shape for any OIDC provider (Okta, Auth0, Keycloak, Entra ID, Google) and
//! for cloud IAM that federates through OIDC (e.g. GCP/EKS workload identity):
//! point `--oidc-jwks-file` at the provider's published JWKS.

use std::sync::Arc;

use jsonwebtoken::{Algorithm, DecodingKey, Validation};

use super::keyset::ReloadableKeySet;
use super::rbac::{self, Role};

/// Validation policy for bearer tokens.
#[derive(Clone, Debug)]
pub struct JwtConfig {
    /// Required `iss`; `None` skips issuer checking (not recommended for prod).
    pub issuer: Option<String>,
    /// Accepted `aud` values; empty skips audience checking.
    pub audiences: Vec<String>,
    /// Claim carrying roles/groups. May be a string (space/comma separated) or
    /// an array of strings. Default: `roles`.
    pub role_claim: String,
    /// Claim carrying the subject. Default: `sub`.
    pub subject_claim: String,
    /// Optional claim binding the token to a worker identity. Default:
    /// `worker_id`.
    pub worker_id_claim: String,
    /// Clock-skew leeway in seconds applied to `exp`/`nbf`.
    pub leeway_secs: u64,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            issuer: None,
            audiences: Vec::new(),
            role_claim: "roles".to_string(),
            subject_claim: "sub".to_string(),
            worker_id_claim: "worker_id".to_string(),
            leeway_secs: 60,
        }
    }
}

/// A successfully validated token's authorization-relevant claims.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedToken {
    pub subject: String,
    pub roles: Vec<Role>,
    pub worker_id: Option<String>,
}

/// Why a bearer token was rejected. Every variant is safe to surface to the
/// caller (no key material or token contents leak).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JwtError {
    Malformed,
    UnknownKey,
    InvalidSignature,
    Expired,
    NotYetValid,
    WrongIssuer,
    WrongAudience,
    MissingSubject,
    NoAuthorizedRoles,
}

impl JwtError {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Malformed => "malformed bearer token",
            Self::UnknownKey => "token signed by an unknown key",
            Self::InvalidSignature => "token signature verification failed",
            Self::Expired => "token has expired",
            Self::NotYetValid => "token is not yet valid",
            Self::WrongIssuer => "token issuer is not accepted",
            Self::WrongAudience => "token audience is not accepted",
            Self::MissingSubject => "token is missing a subject claim",
            Self::NoAuthorizedRoles => "token carries no recognized role",
        }
    }
}

/// Validates bearer tokens against a rotatable key set.
#[derive(Debug)]
pub struct JwtValidator {
    config: JwtConfig,
    keys: Arc<ReloadableKeySet>,
}

impl JwtValidator {
    pub fn new(config: JwtConfig, keys: Arc<ReloadableKeySet>) -> Self {
        Self { config, keys }
    }

    /// Validate a raw JWT string, returning its authorization claims.
    pub fn validate(&self, token: &str) -> Result<VerifiedToken, JwtError> {
        let header = jsonwebtoken::decode_header(token).map_err(|_| JwtError::Malformed)?;
        let keyset = self.keys.current();
        let candidates = keyset.candidates(header.kid.as_deref());
        if candidates.is_empty() {
            return Err(JwtError::UnknownKey);
        }

        // Try each candidate key. A signature failure against one key is not
        // fatal while other candidate keys remain (rotation overlap). A
        // key-independent failure (exp/nbf/iss/aud) is reported immediately.
        for candidate in candidates {
            match self.validate_with(token, candidate.algorithm, &candidate.key) {
                Ok(claims) => return self.extract(&claims),
                Err(JwtError::InvalidSignature) | Err(JwtError::UnknownKey) => continue,
                Err(other) => return Err(other),
            }
        }
        // Every candidate key failed signature verification.
        Err(JwtError::InvalidSignature)
    }

    fn validate_with(
        &self,
        token: &str,
        algorithm: Algorithm,
        key: &DecodingKey,
    ) -> Result<serde_json::Value, JwtError> {
        let mut validation = Validation::new(algorithm);
        validation.leeway = self.config.leeway_secs;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        // Pinning to the configured algorithm blocks algorithm-confusion
        // (e.g. an HS256 token verified against an RSA public key).
        validation.algorithms = vec![algorithm];
        match &self.config.issuer {
            Some(iss) => validation.set_issuer(&[iss]),
            None => validation.validate_aud = !self.config.audiences.is_empty(),
        }
        if self.config.audiences.is_empty() {
            validation.validate_aud = false;
        } else {
            validation.set_audience(&self.config.audiences);
        }

        use jsonwebtoken::errors::ErrorKind;
        jsonwebtoken::decode::<serde_json::Value>(token, key, &validation)
            .map(|data| data.claims)
            .map_err(|e| match e.kind() {
                ErrorKind::ExpiredSignature => JwtError::Expired,
                ErrorKind::ImmatureSignature => JwtError::NotYetValid,
                ErrorKind::InvalidIssuer => JwtError::WrongIssuer,
                ErrorKind::InvalidAudience => JwtError::WrongAudience,
                ErrorKind::InvalidSignature => JwtError::InvalidSignature,
                ErrorKind::InvalidAlgorithm | ErrorKind::InvalidAlgorithmName => {
                    JwtError::UnknownKey
                }
                _ => JwtError::Malformed,
            })
    }

    fn extract(&self, claims: &serde_json::Value) -> Result<VerifiedToken, JwtError> {
        let subject = claims
            .get(&self.config.subject_claim)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty())
            .ok_or(JwtError::MissingSubject)?;

        let roles = extract_roles(claims, &self.config.role_claim);
        if roles.is_empty() {
            return Err(JwtError::NoAuthorizedRoles);
        }

        let worker_id = claims
            .get(&self.config.worker_id_claim)
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty());

        Ok(VerifiedToken {
            subject,
            roles,
            worker_id,
        })
    }
}

/// Extract roles from a claim that may be a string (space/comma separated) or
/// an array of strings; unknown tokens are ignored.
fn extract_roles(claims: &serde_json::Value, claim: &str) -> Vec<Role> {
    match claims.get(claim) {
        Some(serde_json::Value::String(s)) => rbac::parse_roles([s.as_str()]),
        Some(serde_json::Value::Array(items)) => {
            let tokens: Vec<String> = items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            rbac::parse_roles(tokens)
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::keyset::KeySource;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    const SECRET: &[u8] = b"test-signing-secret-please-rotate";

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn validator(config: JwtConfig) -> (JwtValidator, tempfile::TempDir) {
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
        (JwtValidator::new(config, keys), dir)
    }

    fn mint(claims: serde_json::Value) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    }

    #[test]
    fn valid_token_yields_subject_and_roles() {
        let (v, _d) = validator(JwtConfig {
            issuer: Some("https://issuer.example".into()),
            audiences: vec!["dtruntime".into()],
            ..Default::default()
        });
        let token = mint(serde_json::json!({
            "sub": "alice@example.com",
            "iss": "https://issuer.example",
            "aud": "dtruntime",
            "exp": now() + 300,
            "roles": ["operator", "viewer"],
        }));
        let verified = v.validate(&token).unwrap();
        assert_eq!(verified.subject, "alice@example.com");
        assert_eq!(verified.roles, vec![Role::Operator, Role::Viewer]);
    }

    #[test]
    fn expired_token_is_rejected() {
        let (v, _d) = validator(JwtConfig::default());
        // Beyond the 60s default leeway so it is unambiguously expired.
        let token = mint(serde_json::json!({
            "sub": "a", "exp": now() - 120, "roles": "admin"
        }));
        assert_eq!(v.validate(&token), Err(JwtError::Expired));
    }

    #[test]
    fn wrong_audience_is_rejected() {
        let (v, _d) = validator(JwtConfig {
            audiences: vec!["dtruntime".into()],
            ..Default::default()
        });
        let token = mint(serde_json::json!({
            "sub": "a", "aud": "other", "exp": now() + 300, "roles": "admin"
        }));
        assert_eq!(v.validate(&token), Err(JwtError::WrongAudience));
    }

    #[test]
    fn wrong_issuer_is_rejected() {
        let (v, _d) = validator(JwtConfig {
            issuer: Some("https://trusted".into()),
            ..Default::default()
        });
        let token = mint(serde_json::json!({
            "sub": "a", "iss": "https://evil", "exp": now() + 300, "roles": "admin"
        }));
        assert_eq!(v.validate(&token), Err(JwtError::WrongIssuer));
    }

    #[test]
    fn tampered_signature_is_rejected() {
        let (v, _d) = validator(JwtConfig::default());
        let token = mint(serde_json::json!({"sub":"a","exp":now()+300,"roles":"admin"}));
        // Flip the last character of the signature segment.
        let mut bad = token.clone();
        let last = bad.pop().unwrap();
        bad.push(if last == 'a' { 'b' } else { 'a' });
        assert!(matches!(
            v.validate(&bad),
            Err(JwtError::InvalidSignature) | Err(JwtError::Malformed)
        ));
    }

    #[test]
    fn token_without_recognized_role_is_rejected() {
        let (v, _d) = validator(JwtConfig::default());
        let token = mint(serde_json::json!({
            "sub": "a", "exp": now() + 300, "roles": ["some-other-group"]
        }));
        assert_eq!(v.validate(&token), Err(JwtError::NoAuthorizedRoles));
    }

    #[test]
    fn worker_id_claim_is_extracted() {
        let (v, _d) = validator(JwtConfig::default());
        let token = mint(serde_json::json!({
            "sub": "worker-7", "exp": now() + 300, "roles": "worker", "worker_id": "worker-7"
        }));
        let verified = v.validate(&token).unwrap();
        assert_eq!(verified.worker_id.as_deref(), Some("worker-7"));
    }

    #[test]
    fn key_rotation_validates_tokens_signed_by_the_new_key() {
        // Start with SECRET, then rotate the file to a new secret and confirm a
        // token signed by the new secret validates after reload.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, SECRET).unwrap();
        let keys = Arc::new(
            ReloadableKeySet::load(KeySource::HmacSecret {
                path: path.clone(),
                algorithm: Algorithm::HS256,
            })
            .unwrap(),
        );
        let v = JwtValidator::new(JwtConfig::default(), keys.clone());

        let new_secret = b"rotated-secret-value";
        std::fs::write(&path, new_secret).unwrap();
        keys.force_reload().unwrap();

        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({"sub":"a","exp":now()+300,"roles":"admin"}),
            &EncodingKey::from_secret(new_secret),
        )
        .unwrap();
        assert!(v.validate(&token).is_ok());
    }

    #[test]
    fn leeway_allows_small_clock_skew_on_nbf() {
        let (v, _d) = validator(JwtConfig::default());
        // nbf slightly in the future, within the 60s default leeway.
        let token = mint(serde_json::json!({
            "sub": "a", "exp": now() + 300, "nbf": now() + 30, "roles": "admin"
        }));
        assert!(v.validate(&token).is_ok());
        let _ = Duration::from_secs(0);
    }
}
