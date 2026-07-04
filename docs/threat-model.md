# Threat Model

## 1) Assets to protect

- Coordinator control plane.
- Run metadata.
- Worker identities.
- Dataset shard assignments.
- Checkpoint manifests.
- Checkpoint artifacts.
- Object storage credentials.
- Benchmark and metrics outputs.
- Dashboard/admin views (when added).
- Kubernetes deployment configuration.

## 2) Actors

- Legitimate local developer.
- Coordinator service.
- Worker/training process.
- Python SDK client.
- Future dashboard/admin user.
- Kubernetes workload.
- Object storage backend.
- External attacker.
- Compromised worker.
- Compromised credentials.

## 3) Threats

- Unauthenticated coordinator access.
- Worker impersonation.
- Unauthorized run/dataset registration.
- Unauthorized shard assignment queries.
- Forged checkpoint artifact reports.
- Malicious checkpoint commit attempts.
- Checkpoint artifact tampering.
- Replayed RPCs.
- Stolen object storage credentials.
- Leaked Kubernetes Secret values.
- Dashboard exposure without authentication.
- Log leakage of credentials/secrets.
- Namespace escape / privileged pod risks.
- Accidental public exposure of local service.
- Stale worker identity reuse.
- Coordinator failover inconsistency in future HA design.

## 4) Mitigation map

### Implemented

- **Unauthenticated access** → auth enforcement (`--auth-mode metadata`) with
  layered OIDC/JWT + SPIFFE credentials; header identity disable-able.
- **Worker impersonation** → worker-self checks bind the request `worker_id` to
  the verified credential (JWT `worker_id` claim or SPIFFE path segment).
- **Privilege escalation / unauthorized run or dataset registration** →
  role/permission RBAC (viewer read-only, operator manage, admin all).
- **Token forgery / tampering** → signature verification with algorithm pinning
  (blocks alg-confusion); `exp`/`nbf`/`iss`/`aud` enforced.
- **Credential theft blast radius** → short-lived tokens + SVIDs; JWT keys
  hot-rotate without downtime.
- **Replayed RPCs** → idempotency keys in mutation paths.
- **Forged/tampered checkpoint artifacts** → checksum validation on
  commit/stream/download with corrupt-fallback.
- **Stolen object-store credentials** → least-privilege, scoped, short-lived
  credentials guidance ([secret-rotation.md](secret-rotation.md)).
- **Log leakage of secrets** → audit sinks never emit tokens/keys/credentials.
- **Weak audit/forensics** → structured audit to DB + NDJSON/SIEM sink + auth
  allow/deny counters.
- **Vulnerable dependencies** → `cargo-deny` + `pip-audit` in CI; Dependabot.
- **Accidental exposure** → local Service is ClusterIP-only; non-privileged pods.

### Planned

- Dashboard authentication/authorization (no dashboard yet).
- Validation against a live SPIRE server and a production OIDC provider/SIEM.
- Per-run / per-namespace attribute-based scoping beyond roles.

## 5) Security validation

The following are covered by automated tests (Rust unit + service, Python), and
several were additionally validated live against a running coordinator:

- Missing/invalid credential is rejected as `UNAUTHENTICATED`; unauthorized role
  as `PERMISSION_DENIED` (`auth.rs`, `service.rs` tests).
- Forged JWT signature, expired token, wrong `aud`/`iss` are rejected
  (`security::jwt` tests; `threat_*` service tests; live).
- Spoofed metadata role does not authorize when header identity is disabled
  (`threat_header_identity_ignored_when_fallback_disabled`; live).
- Viewer can read but not manage; operator can manage (`viewer_can_read_but_not_manage`,
  `operator_can_manage_runs`; live).
- Wrong worker identity cannot act as another worker (`metadata_auth_worker_scoped_*`).
- JWT key rotation: a token signed by the rotated key validates, the retired key
  is rejected (`security::jwt::key_rotation_*`; live secret-file rotation).
- SPIFFE ID from the wrong trust domain / unmapped path is rejected
  (`security::spiffe` tests).
- Object-store credentials and tokens are not emitted in audit output
  ([audit-logging.md](audit-logging.md)).

## 6) Security scope

This document defines security risks and controls. Not all future controls listed above are yet fully implemented. Dashboard authentication/authorization, HA failover hardening, and production S3 hardening remain planned work.

## 7) Authorization model

Enforcement is enabled with `--auth-mode metadata` (or
`DTRUNTIME_AUTH_MODE=metadata`; default `disabled` for local dev). Identity is
established by a layered authenticator — OIDC/JWT bearer tokens, SPIFFE
X.509-SVID, or dev metadata headers — and mapped to roles
(admin/operator/viewer/worker), each granting a set of permissions checked
per-RPC. Header identity can be disabled (`--auth-disable-header-identity`) so
only cryptographically-proven credentials authenticate. See
[security-auth.md](security-auth.md).

## 8) Audit logging

Structured audit/security events are emitted for security-relevant coordinator operations. Audit logs may include logical identity and run/checkpoint identifiers and should be protected. Dashboard/SIEM integration and durable audit storage are planned future work.
