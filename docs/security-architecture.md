# Security Architecture (Phase 20F-1)

## 1) Current security status

The project now has a local/dev Kubernetes deployment path (Phase 20E), but that path is intentionally non-production.

Current state:
- Local/dev Kubernetes manifests and runbook are available.
- Coordinator RPC access does not yet enforce production authentication/authorization.
- gRPC transport is plaintext in local/dev mode.
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

## 5) Phase breakdown (20F track)

- **20F-1:** Security architecture, threat model, and production-readiness documentation foundation (this phase).
- **20F-2:** Optional TLS/mTLS foundation for coordinator gRPC and Python client.
- **20F-3:** Workload identity and authorization model.
- **20F-4:** Audit logging and security event model.
- **20F-5:** Secure dashboard, HA, and production S3 hardening tracks.

## 6) Non-claims for Phase 20F-1

This phase does **not** claim:
- mTLS already implemented.
- OIDC already implemented.
- Dashboard implementation.
- HA replication/failover implementation.
- Production S3 hardening complete.
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n
