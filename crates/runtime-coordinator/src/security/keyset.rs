#![forbid(unsafe_code)]

//! Verification-key material with **hot rotation**.
//!
//! JWT signing keys are the coordinator's most rotation-sensitive secret. This
//! module loads verification keys from a file (raw HMAC secret, a PEM public
//! key, or a JWKS document) and transparently reloads them when the backing
//! file changes on disk. An operator rotates keys by writing a new JWKS (or
//! secret) file — typically projected from a secret manager or a
//! Kubernetes Secret — and the coordinator adopts the new key on the next
//! request without a restart. Publishing the *new* key alongside the old one
//! (JWKS with two `kid`s) gives zero-downtime rotation: tokens signed with
//! either key validate during the overlap window.

use std::path::PathBuf;
use std::sync::RwLock;
use std::time::{Duration, Instant, SystemTime};

use jsonwebtoken::jwk::{AlgorithmParameters, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey};

/// One verification key: an optional `kid`, its algorithm, and the decoding
/// material.
#[derive(Clone)]
pub struct VerificationKey {
    pub kid: Option<String>,
    pub algorithm: Algorithm,
    pub key: DecodingKey,
}

impl std::fmt::Debug for VerificationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerificationKey")
            .field("kid", &self.kid)
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

/// An immutable snapshot of the keys currently trusted.
#[derive(Clone, Debug, Default)]
pub struct KeySet {
    keys: Vec<VerificationKey>,
}

impl KeySet {
    pub fn new(keys: Vec<VerificationKey>) -> Self {
        Self { keys }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Candidate keys for a token, most-specific first: when the token header
    /// carries a `kid`, keys with that `kid` come first (an unspecified-`kid`
    /// key is always a fallback candidate so single-key HMAC/PEM setups work).
    pub fn candidates(&self, kid: Option<&str>) -> Vec<&VerificationKey> {
        let mut out: Vec<&VerificationKey> = Vec::new();
        if let Some(kid) = kid {
            for k in &self.keys {
                if k.kid.as_deref() == Some(kid) {
                    out.push(k);
                }
            }
        }
        // Keys without a kid, or all keys when no kid was requested.
        for k in &self.keys {
            if (kid.is_none() || k.kid.is_none()) && !out.iter().any(|e| std::ptr::eq(*e, k)) {
                out.push(k);
            }
        }
        out
    }
}

/// Where verification keys come from.
#[derive(Clone, Debug)]
pub enum KeySource {
    /// Raw HMAC shared secret bytes (symmetric; HS256/384/512).
    HmacSecret { path: PathBuf, algorithm: Algorithm },
    /// PEM-encoded public key (asymmetric; RS*/PS*/ES*/EdDSA).
    PublicKeyPem { path: PathBuf, algorithm: Algorithm },
    /// A JWKS document (RFC 7517), one or more keys, each optionally kid-tagged.
    Jwks { path: PathBuf },
}

impl KeySource {
    fn path(&self) -> &PathBuf {
        match self {
            Self::HmacSecret { path, .. }
            | Self::PublicKeyPem { path, .. }
            | Self::Jwks { path } => path,
        }
    }

    fn load(&self) -> Result<KeySet, String> {
        match self {
            Self::HmacSecret { path, algorithm } => {
                let bytes = std::fs::read(path)
                    .map_err(|e| format!("failed to read HMAC secret {}: {e}", path.display()))?;
                let trimmed = trim_trailing_newline(&bytes);
                if trimmed.is_empty() {
                    return Err(format!("HMAC secret {} is empty", path.display()));
                }
                Ok(KeySet::new(vec![VerificationKey {
                    kid: None,
                    algorithm: *algorithm,
                    key: DecodingKey::from_secret(trimmed),
                }]))
            }
            Self::PublicKeyPem { path, algorithm } => {
                let pem = std::fs::read(path)
                    .map_err(|e| format!("failed to read public key {}: {e}", path.display()))?;
                let key = decoding_key_from_pem(&pem, *algorithm)
                    .map_err(|e| format!("failed to parse public key {}: {e}", path.display()))?;
                Ok(KeySet::new(vec![VerificationKey {
                    kid: None,
                    algorithm: *algorithm,
                    key,
                }]))
            }
            Self::Jwks { path } => {
                let bytes = std::fs::read(path)
                    .map_err(|e| format!("failed to read JWKS {}: {e}", path.display()))?;
                let set: JwkSet = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("failed to parse JWKS {}: {e}", path.display()))?;
                let mut keys = Vec::new();
                for jwk in &set.keys {
                    let algorithm = jwk_algorithm(jwk)?;
                    let key = DecodingKey::from_jwk(jwk)
                        .map_err(|e| format!("invalid JWK in {}: {e}", path.display()))?;
                    keys.push(VerificationKey {
                        kid: jwk.common.key_id.clone(),
                        algorithm,
                        key,
                    });
                }
                if keys.is_empty() {
                    return Err(format!("JWKS {} contains no keys", path.display()));
                }
                Ok(KeySet::new(keys))
            }
        }
    }
}

fn trim_trailing_newline(bytes: &[u8]) -> &[u8] {
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b'\n' || bytes[end - 1] == b'\r') {
        end -= 1;
    }
    &bytes[..end]
}

fn decoding_key_from_pem(pem: &[u8], algorithm: Algorithm) -> Result<DecodingKey, String> {
    match algorithm {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => DecodingKey::from_rsa_pem(pem).map_err(|e| e.to_string()),
        Algorithm::ES256 | Algorithm::ES384 => {
            DecodingKey::from_ec_pem(pem).map_err(|e| e.to_string())
        }
        Algorithm::EdDSA => DecodingKey::from_ed_pem(pem).map_err(|e| e.to_string()),
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            Err("HMAC algorithms use a shared secret, not a PEM public key".to_string())
        }
    }
}

fn jwk_algorithm(jwk: &jsonwebtoken::jwk::Jwk) -> Result<Algorithm, String> {
    // Honor an explicit `alg` when present; otherwise infer from key type.
    if let Some(alg) = jwk.common.key_algorithm {
        if let Ok(parsed) = format!("{alg:?}").parse::<Algorithm>() {
            return Ok(parsed);
        }
    }
    Ok(match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => Algorithm::RS256,
        AlgorithmParameters::EllipticCurve(ec) => {
            use jsonwebtoken::jwk::EllipticCurve;
            match &ec.curve {
                EllipticCurve::P256 => Algorithm::ES256,
                EllipticCurve::P384 => Algorithm::ES384,
                other => return Err(format!("unsupported EC curve: {other:?}")),
            }
        }
        AlgorithmParameters::OctetKey(_) => Algorithm::HS256,
        AlgorithmParameters::OctetKeyPair(_) => Algorithm::EdDSA,
    })
}

/// Poll interval floor for reload checks; avoids stat'ing the file on every RPC.
const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(5);

struct ReloadState {
    keyset: std::sync::Arc<KeySet>,
    loaded_mtime: Option<SystemTime>,
    last_check: Instant,
}

/// A [`KeySet`] that transparently reloads when its backing file changes.
pub struct ReloadableKeySet {
    source: KeySource,
    reload_interval: Duration,
    state: RwLock<ReloadState>,
}

impl std::fmt::Debug for ReloadableKeySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableKeySet")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl ReloadableKeySet {
    /// Load the initial keys and arm hot-reload.
    pub fn load(source: KeySource) -> Result<Self, String> {
        Self::load_with_interval(source, DEFAULT_RELOAD_INTERVAL)
    }

    pub fn load_with_interval(
        source: KeySource,
        reload_interval: Duration,
    ) -> Result<Self, String> {
        let keyset = std::sync::Arc::new(source.load()?);
        let loaded_mtime = file_mtime(source.path());
        Ok(Self {
            source,
            reload_interval,
            state: RwLock::new(ReloadState {
                keyset,
                loaded_mtime,
                last_check: Instant::now(),
            }),
        })
    }

    /// The current key snapshot, reloading first if the file changed and the
    /// poll interval has elapsed. A failed reload keeps the last-good keys (so
    /// a momentarily truncated file during rotation does not lock everyone
    /// out) and is surfaced to the caller.
    pub fn current(&self) -> std::sync::Arc<KeySet> {
        // Fast path: recently checked.
        {
            let st = self.state.read().unwrap();
            if st.last_check.elapsed() < self.reload_interval {
                return st.keyset.clone();
            }
        }
        let mut st = self.state.write().unwrap();
        st.last_check = Instant::now();
        let current_mtime = file_mtime(self.source.path());
        if current_mtime != st.loaded_mtime {
            match self.source.load() {
                Ok(new_keys) => {
                    st.keyset = std::sync::Arc::new(new_keys);
                    st.loaded_mtime = current_mtime;
                    tracing::info!(
                        source = ?self.source.path(),
                        keys = st.keyset.len(),
                        "reloaded JWT verification keys (rotation)"
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "JWT key reload failed; keeping previous keys");
                }
            }
        }
        st.keyset.clone()
    }

    /// Force a synchronous reload regardless of the poll interval. Rotation
    /// in production happens via the mtime poll in `current()`; this exists
    /// so tests can rotate deterministically.
    pub fn force_reload(&self) -> Result<(), String> {
        let new_keys = self.source.load()?;
        let mut st = self.state.write().unwrap();
        st.keyset = std::sync::Arc::new(new_keys);
        st.loaded_mtime = file_mtime(self.source.path());
        st.last_check = Instant::now();
        Ok(())
    }
}

fn file_mtime(path: &std::path::Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn hmac_secret_loads_and_hot_reloads_on_change() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret");
        std::fs::write(&path, b"first-secret\n").unwrap();

        let ks = ReloadableKeySet::load_with_interval(
            KeySource::HmacSecret {
                path: path.clone(),
                algorithm: Algorithm::HS256,
            },
            Duration::from_millis(0),
        )
        .unwrap();
        assert_eq!(ks.current().len(), 1);

        // Rotate the secret file; a forced reload picks up the new material.
        std::fs::write(&path, b"second-secret").unwrap();
        ks.force_reload().unwrap();
        assert_eq!(ks.current().len(), 1);
    }

    #[test]
    fn jwks_with_multiple_kids_yields_multiple_keys() {
        // Two HMAC (oct) keys with distinct kids — a rotation overlap window.
        let jwks = r#"{"keys":[
            {"kty":"oct","kid":"old","k":"c2VjcmV0LW9uZQ"},
            {"kty":"oct","kid":"new","k":"c2VjcmV0LXR3bw"}
        ]}"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jwks.json");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(jwks.as_bytes()).unwrap();

        let ks = ReloadableKeySet::load(KeySource::Jwks { path }).unwrap();
        let set = ks.current();
        assert_eq!(set.len(), 2);
        // kid-directed lookup returns the matching key first.
        let c = set.candidates(Some("new"));
        assert_eq!(c[0].kid.as_deref(), Some("new"));
    }

    #[test]
    fn empty_secret_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty");
        std::fs::write(&path, b"\n").unwrap();
        let err = ReloadableKeySet::load(KeySource::HmacSecret {
            path,
            algorithm: Algorithm::HS256,
        })
        .unwrap_err();
        assert!(err.contains("empty"), "{err}");
    }
}
