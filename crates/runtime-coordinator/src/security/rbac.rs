#![forbid(unsafe_code)]

//! Role-based access control model.
//!
//! Expands the original two-role (`admin`/`worker`) scheme into four roles
//! mapped onto a small set of coarse permissions. Every coordinator RPC checks
//! for a required [`Permission`]; a caller is authorized when **any** of its
//! roles grants that permission. Roles come from a verified credential (a JWT
//! claim, a SPIFFE SVID mapping, or — in dev — a metadata header), never from
//! the caller's raw assertion of "I am an admin".

use std::str::FromStr;

/// A coordinator role. A verified credential may carry more than one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Role {
    /// Full control, including worker-plane operations and configuration.
    Admin,
    /// Manage runs (lifecycle, registration, datasets, commits) but not
    /// worker-plane self operations or admin-only configuration.
    Operator,
    /// Read-only inspection of runs, workers, shards, and checkpoints.
    Viewer,
    /// A training worker acting on its own behalf (heartbeats, leases,
    /// checkpoint reports). Scoped to its own `worker_id`.
    Worker,
}

impl Role {
    /// Parse a role token (case-insensitive). Accepts the canonical names and
    /// the historical `admin`/`worker` values for backward compatibility.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "admin" | "administrator" => Some(Self::Admin),
            "operator" | "op" | "ops" => Some(Self::Operator),
            "viewer" | "reader" | "read-only" | "readonly" => Some(Self::Viewer),
            "worker" => Some(Self::Worker),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Operator => "operator",
            Self::Viewer => "viewer",
            Self::Worker => "worker",
        }
    }

    /// Does this role grant `permission`?
    pub fn grants(self, permission: Permission) -> bool {
        use Permission::*;
        use Role::*;
        match self {
            Admin => true,
            Operator => matches!(permission, Read | Manage | RegisterRun),
            Viewer => matches!(permission, Read),
            Worker => matches!(permission, WorkerSelf),
        }
    }
}

impl FromStr for Role {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        Self::parse(s).ok_or(())
    }
}

/// A coarse capability a caller must hold to invoke a group of RPCs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Read-only inspection (get status, list runs/workers/shards/checkpoints,
    /// runtime info).
    Read,
    /// Manage a run's lifecycle and metadata (pause/resume/retry/drain/cancel,
    /// register dataset, commit checkpoint).
    Manage,
    /// Register a new run.
    RegisterRun,
    /// Worker-plane self operations (register worker, heartbeat, leases, shard
    /// progress, checkpoint begin/report/corrupt, rendezvous). Additionally
    /// gated by a worker-identity match at the call site.
    WorkerSelf,
}

impl Permission {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Manage => "manage",
            Self::RegisterRun => "register_run",
            Self::WorkerSelf => "worker_self",
        }
    }
}

/// Parse a comma/space/semicolon-separated list of role tokens, ignoring
/// unknown entries (an OIDC `roles`/`groups` claim often carries unrelated
/// groups). Returns the de-duplicated set in a stable order.
pub fn parse_roles<I, S>(tokens: I) -> Vec<Role>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut roles: Vec<Role> = Vec::new();
    for token in tokens {
        for part in token.as_ref().split([',', ' ', ';']) {
            if let Some(role) = Role::parse(part) {
                if !roles.contains(&role) {
                    roles.push(role);
                }
            }
        }
    }
    roles.sort();
    roles
}

/// True if any role in `roles` grants `permission`.
pub fn any_grants(roles: &[Role], permission: Permission) -> bool {
    roles.iter().any(|r| r.grants(permission))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_parsing_is_case_insensitive_and_accepts_aliases() {
        assert_eq!(Role::parse("Admin"), Some(Role::Admin));
        assert_eq!(Role::parse("OPERATOR"), Some(Role::Operator));
        assert_eq!(Role::parse("read-only"), Some(Role::Viewer));
        assert_eq!(Role::parse("worker"), Some(Role::Worker));
        assert_eq!(Role::parse("root"), None);
    }

    #[test]
    fn permission_lattice() {
        // Admin can do everything.
        for p in [
            Permission::Read,
            Permission::Manage,
            Permission::RegisterRun,
            Permission::WorkerSelf,
        ] {
            assert!(Role::Admin.grants(p));
        }
        // Operator manages but is not a worker.
        assert!(Role::Operator.grants(Permission::Manage));
        assert!(Role::Operator.grants(Permission::RegisterRun));
        assert!(Role::Operator.grants(Permission::Read));
        assert!(!Role::Operator.grants(Permission::WorkerSelf));
        // Viewer is read-only.
        assert!(Role::Viewer.grants(Permission::Read));
        assert!(!Role::Viewer.grants(Permission::Manage));
        // Worker is self-scoped only.
        assert!(Role::Worker.grants(Permission::WorkerSelf));
        assert!(!Role::Worker.grants(Permission::Read));
        assert!(!Role::Worker.grants(Permission::Manage));
    }

    #[test]
    fn parse_roles_dedupes_and_ignores_unknown() {
        let roles = parse_roles(["admin, ops", "unknown viewer", "admin"]);
        assert_eq!(roles, vec![Role::Admin, Role::Operator, Role::Viewer]);
    }

    #[test]
    fn any_grants_scans_all_roles() {
        let roles = vec![Role::Viewer, Role::Worker];
        assert!(any_grants(&roles, Permission::Read));
        assert!(any_grants(&roles, Permission::WorkerSelf));
        assert!(!any_grants(&roles, Permission::Manage));
    }
}
