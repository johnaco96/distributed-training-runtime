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

- Idempotency keys in coordinator mutation paths.
- Checkpoint checksum/integrity-oriented validation on commit paths.
- Optional TLS/mTLS transport for coordinator gRPC and Python client.
- Certificate handling model for local/dev and future production integration tests.
- Workload identity model with optional metadata-based caller identity.
- Explicit authorization policy scaffolding for coordinator RPCs.
- Identity-to-action mapping (e.g., worker scoped to its own reports/checkpoints).
- Structured audit/security event logging.
- Local Kubernetes manifests avoid privileged pod settings.
- Local coordinator Service is ClusterIP-only.
- Non-production limitations are explicitly documented.
- Storage abstraction supports local filesystem and S3-compatible paths.

### Planned

- Dashboard authentication/authorization.
- HA consistency and failover hardening.
- Production-grade secret management integration.
- Deeper production S3 operational hardening/tuning.

## 5) Security test plan (future)

Planned security-focused test coverage includes:
- Missing/invalid client cert is rejected (when TLS/mTLS is configured).
- Unauthorized worker cannot report another worker's checkpoint artifacts.
- Replayed idempotency-key requests remain safe/idempotent.
- Wrong workload identity is rejected for protected RPCs.
- Checkpoint checksum mismatch is rejected.
- Object-store credentials are not emitted in logs.
- Dashboard requires authentication once dashboard is added.
- HA failover path does not double-commit checkpoint state.

## 6) Security scope

This document defines security risks and controls. Not all future controls listed above are yet fully implemented. Dashboard authentication/authorization, HA failover hardening, and production S3 hardening remain planned work.

## 7) Authorization model

An optional metadata-based caller identity model is available for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only — not full SPIFFE/SPIRE or production RBAC.

## 8) Audit logging

Structured audit/security events are emitted for security-relevant coordinator operations. Audit logs may include logical identity and run/checkpoint identifiers and should be protected. Dashboard/SIEM integration and durable audit storage are planned future work.
