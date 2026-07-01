# Security Architecture

## 1) Current security status

The project provides a local/dev Kubernetes deployment path, but that path is intentionally non-production.

Current state:
- Local/dev Kubernetes manifests and runbook are available.
- mTLS is implemented via optional `--tls-cert`/`--tls-key`/`--tls-client-ca` flags on the coordinator.
- Role-based authorization is implemented via metadata-based identity (`--auth-mode metadata`).
- Structured audit logging is implemented via the `DbAuditSink` and `audit_events` table.
- Dashboard/admin UI is not implemented.
- HA coordinator replication/failover is not implemented.
- Production S3 hardening is not implemented.
- Checkpoint integrity checks and idempotency protections are useful controls, but they are not a complete security boundary.

## 2) Target security architecture

Target long-term model (not yet fully implemented):
- mTLS for service-to-service transport.
- Workload identity (ideally SPIFFE/SPIRE-compatible identities).
- Short-lived credentials where possible.
- Explicit authorization policy for coordinator RPCs.
- Audit logging for security-relevant events and policy decisions.
- Kubernetes Secrets may be used as local/dev placeholders only.
- External secret manager integration for production secrets.
- Least-privilege object-store credentials and scoped access.
- Dashboard access protected by human identity provider/OIDC in a future phase.
- HA coordinator architecture backed by durable metadata and leader election.

## 3) Trust boundaries

Primary trust boundaries in this project:
- Python SDK clients ↔ coordinator control plane.
- Worker/training processes ↔ coordinator control plane.
- Coordinator ↔ storage backend (local filesystem or S3-compatible object storage).
- Coordinator ↔ Kubernetes control plane/runtime APIs.
- Future dashboard/admin users ↔ runtime control plane surfaces.
- Local demo cluster ↔ production cluster environments.
- Object storage artifact bytes ↔ coordinator checkpoint metadata/manifest state.

## 4) Security principles

Security posture principles for this repository:
- Secure-by-default production path.
- Local/dev convenience paths must be explicitly labeled as non-production.
- No hardcoded secrets.
- No secrets in logs.
- Least privilege.
- Explicit identity.
- Explicit authorization.
- Defense in depth.
- Auditable decisions.
- Reproducible deployment configuration.

## 5) What is implemented

The following security controls are implemented:

- **mTLS**: Optional transport security for coordinator gRPC and Python RuntimeClient via `--tls-cert`, `--tls-key`, and `--tls-client-ca` flags. Coordinator TLS is provisioned via cert-manager PKI in secure local Kubernetes paths. No certificates are committed to the repository.
- **Role-based authorization**: Metadata-based caller identity for local/dev testing via `--auth-mode metadata`. Python client attaches `x-dtruntime-subject`, `x-dtruntime-role`, and `x-dtruntime-worker-id` headers. Coordinator enforces admin-only and worker-scoped endpoint policies.
- **Structured audit logging**: The coordinator emits structured audit events via `DbAuditSink` to the `audit_events` table, covering startup, authn/authz decisions, and key state-changing RPCs.

## 6) Non-claims

This repository does **not** yet claim:
- OIDC/SPIFFE/SPIRE identity integration.
- Dashboard implementation.
- HA replication/failover implementation.
- Production S3 hardening complete.
- Full SIEM/durable audit pipeline.
