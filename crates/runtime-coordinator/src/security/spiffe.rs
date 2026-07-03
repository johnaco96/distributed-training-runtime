#![forbid(unsafe_code)]

//! SPIFFE workload identity from mTLS client certificates.
//!
//! When mTLS is enabled, a SPIRE-issued X.509-SVID presents the workload's
//! identity as a URI SAN of the form `spiffe://<trust-domain>/<path>`. This
//! module extracts and validates that SPIFFE ID from the verified peer
//! certificate and maps the path to coordinator roles via an ordered rule
//! list — turning a cryptographically-proven workload identity into an
//! authorization decision without any shared secret. The trust chain itself is
//! still enforced by the TLS layer (`--tls-client-ca`); this module only reads
//! identity from an already-verified certificate.

use x509_parser::prelude::*;

use super::rbac::{parse_roles, Role};

/// A rule mapping a SPIFFE path to a set of roles. Matched in order; first
/// match wins. A pattern ending in `*` is a prefix match; otherwise it is exact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpiffeRoleRule {
    pub pattern: String,
    pub roles: Vec<Role>,
}

impl SpiffeRoleRule {
    fn matches(&self, path: &str) -> bool {
        match self.pattern.strip_suffix('*') {
            Some(prefix) => path.starts_with(prefix),
            None => self.pattern == path,
        }
    }
}

/// Policy for authenticating SPIFFE SVIDs.
#[derive(Clone, Debug)]
pub struct SpiffePolicy {
    /// Required trust domain (the authority component of the SPIFFE ID).
    trust_domain: String,
    role_rules: Vec<SpiffeRoleRule>,
}

/// An authenticated SPIFFE identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpiffeIdentity {
    pub spiffe_id: String,
    pub path: String,
    pub roles: Vec<Role>,
    pub worker_id: Option<String>,
}

/// Why a SPIFFE SVID was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpiffeError {
    CertParse,
    NoUriSan,
    MalformedId,
    WrongTrustDomain,
    NoRolesMapped,
}

impl SpiffeError {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CertParse => "could not parse peer certificate",
            Self::NoUriSan => "peer certificate has no SPIFFE URI SAN",
            Self::MalformedId => "malformed SPIFFE ID",
            Self::WrongTrustDomain => "SPIFFE ID is from an untrusted domain",
            Self::NoRolesMapped => "SPIFFE ID maps to no authorized role",
        }
    }
}

impl SpiffePolicy {
    pub fn new(trust_domain: impl Into<String>, role_rules: Vec<SpiffeRoleRule>) -> Self {
        Self {
            trust_domain: trust_domain.into(),
            role_rules,
        }
    }

    /// Parse a role-map file. Each non-empty, non-comment line is
    /// `spiffe-path-pattern = role[,role...]`, e.g.
    /// `/ns/dtruntime/sa/coordinator-admin = admin` or `/workload/worker/* = worker`.
    pub fn parse_role_rules(text: &str) -> Result<Vec<SpiffeRoleRule>, String> {
        let mut rules = Vec::new();
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let (pattern, roles_str) = line
                .split_once('=')
                .ok_or_else(|| format!("line {}: expected 'pattern = roles'", lineno + 1))?;
            let pattern = pattern.trim();
            if pattern.is_empty() {
                return Err(format!("line {}: empty pattern", lineno + 1));
            }
            let roles = parse_roles([roles_str]);
            if roles.is_empty() {
                return Err(format!(
                    "line {}: no recognized roles in '{}'",
                    lineno + 1,
                    roles_str.trim()
                ));
            }
            rules.push(SpiffeRoleRule {
                pattern: pattern.to_string(),
                roles,
            });
        }
        Ok(rules)
    }

    /// Authenticate a DER-encoded peer certificate.
    pub fn authenticate(&self, cert_der: &[u8]) -> Result<SpiffeIdentity, SpiffeError> {
        let spiffe_id = extract_spiffe_id(cert_der)?;
        self.authenticate_id(&spiffe_id)
    }

    /// Authenticate an already-extracted SPIFFE ID string. Split out so the
    /// path→role policy is testable without minting certificates.
    pub fn authenticate_id(&self, spiffe_id: &str) -> Result<SpiffeIdentity, SpiffeError> {
        let rest = spiffe_id
            .strip_prefix("spiffe://")
            .ok_or(SpiffeError::MalformedId)?;
        let (authority, path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, ""),
        };
        if authority.is_empty() {
            return Err(SpiffeError::MalformedId);
        }
        if authority != self.trust_domain {
            return Err(SpiffeError::WrongTrustDomain);
        }

        let roles = self
            .role_rules
            .iter()
            .find(|rule| rule.matches(path))
            .map(|rule| rule.roles.clone())
            .unwrap_or_default();
        if roles.is_empty() {
            return Err(SpiffeError::NoRolesMapped);
        }

        // A worker identity binds to the trailing path segment (the common
        // SPIRE `/.../worker/<id>` layout), so worker-self checks can compare it.
        let worker_id = if roles.contains(&Role::Worker) {
            path.rsplit('/').find(|s| !s.is_empty()).map(str::to_string)
        } else {
            None
        };

        Ok(SpiffeIdentity {
            spiffe_id: spiffe_id.to_string(),
            path: path.to_string(),
            roles,
            worker_id,
        })
    }
}

/// Extract the first `spiffe://` URI SAN from a DER certificate.
pub fn extract_spiffe_id(cert_der: &[u8]) -> Result<String, SpiffeError> {
    let (_, cert) = X509Certificate::from_der(cert_der).map_err(|_| SpiffeError::CertParse)?;
    for ext in cert.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                if let GeneralName::URI(uri) = name {
                    if uri.starts_with("spiffe://") {
                        return Ok((*uri).to_string());
                    }
                }
            }
        }
    }
    Err(SpiffeError::NoUriSan)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> SpiffePolicy {
        let rules = SpiffePolicy::parse_role_rules(
            "# coordinator control plane\n\
             /ns/dtruntime/sa/admin = admin\n\
             /ns/dtruntime/sa/operator = operator\n\
             /workload/worker/* = worker\n",
        )
        .unwrap();
        SpiffePolicy::new("dtruntime.example.org", rules)
    }

    #[test]
    fn maps_admin_path_to_admin_role() {
        let id = policy()
            .authenticate_id("spiffe://dtruntime.example.org/ns/dtruntime/sa/admin")
            .unwrap();
        assert_eq!(id.roles, vec![Role::Admin]);
        assert!(id.worker_id.is_none());
    }

    #[test]
    fn maps_worker_prefix_and_extracts_worker_id() {
        let id = policy()
            .authenticate_id("spiffe://dtruntime.example.org/workload/worker/worker-7")
            .unwrap();
        assert_eq!(id.roles, vec![Role::Worker]);
        assert_eq!(id.worker_id.as_deref(), Some("worker-7"));
    }

    #[test]
    fn rejects_wrong_trust_domain() {
        assert_eq!(
            policy().authenticate_id("spiffe://evil.example.org/ns/dtruntime/sa/admin"),
            Err(SpiffeError::WrongTrustDomain)
        );
    }

    #[test]
    fn rejects_unmapped_path() {
        assert_eq!(
            policy().authenticate_id("spiffe://dtruntime.example.org/ns/other/sa/x"),
            Err(SpiffeError::NoRolesMapped)
        );
    }

    #[test]
    fn rejects_non_spiffe_uri() {
        assert_eq!(
            policy().authenticate_id("https://dtruntime.example.org/x"),
            Err(SpiffeError::MalformedId)
        );
    }

    #[test]
    fn role_map_parser_rejects_garbage() {
        assert!(SpiffePolicy::parse_role_rules("no-equals-sign").is_err());
        assert!(SpiffePolicy::parse_role_rules("/path = notarole").is_err());
    }
}
