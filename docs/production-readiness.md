# Production Readiness (Phase 20F-1 Foundation)

## 1) Current status

Distributed Training Runtime currently provides a validated local/dev prototype path. It does not yet claim full production readiness.

## 2) Deployment readiness

- Local container image and local Kubernetes manifests exist.
- Single-replica coordinator deployment path is documented.
- Deployment flow is local/dev-first and intentionally conservative.

## 3) Security readiness

Implemented today:
- Checkpoint integrity/idempotency-related protections.
- Local-only packaging defaults (ClusterIP service, no privileged pods in local manifests).

Not yet complete for production:
- mTLS.
- Workload identity.
- Explicit authorization policy.
- Security audit logging.
- Externalized production secret management.

## 4) Observability/dashboard readiness

- Structured logs and in-process metrics foundation exist.
- Dashboard UI/admin surface is not implemented.
- Security event views are not implemented.

## 5) HA/coordinator replication readiness

- Coordinator currently runs as a single replica in local Kubernetes examples.
- Durable HA metadata + leader election + fencing + failover validation are pending.

## 6) Storage/S3 readiness

- Local filesystem and S3-compatible interfaces exist.
- Production S3 credential lifecycle, IAM scoping, encryption/lifecycle/tuning, and rotation guidance are not fully implemented.

## 7) Benchmark readiness

- Local benchmark tooling exists and emits reproducible artifacts.
- Results are environment-specific and must be labeled accordingly.
- Current benchmarks are not production throughput claims.

## 8) Kubernetes hardening readiness

- Local manifests are intentionally minimal and local/dev oriented.
- Production hardening (policy, identity, secret management, stricter admission/pod security controls) is pending.

## 9) Production release gate checklist

Before production claims, all of the following should be complete and validated:
- mTLS enabled and tested.
- Workload identity implemented.
- Authorization policy enforced.
- Audit logs implemented.
- No secrets in logs.
- Kubernetes manifests hardened.
- S3 credentials sourced from a secret manager.
- Least-privilege storage policy validated.
- HA failover tests completed.
- Backup/restore tests completed.
- Dashboard authentication implemented (if dashboard exists).
- Benchmark results labeled, reproducible, and environment-scoped.

## 10) Explicit non-claims

This repository currently does **not** claim:
- Full production readiness.
- Full production security posture.
- Dashboard readiness.
- HA coordinator readiness.
- Completed production S3 hardening.
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n


## Phase 20F-3 authorization scaffolding
Phase 20F-3 adds an internal authorization model with optional metadata-based caller identity for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only (not full SPIFFE/SPIRE or production RBAC). Audit/security events remain planned for Phase 20F-4; dashboard/HA/production S3 hardening remain Phase 20F-5.


Phase 20F-4 adds structured audit/security events; dashboard/SIEM/durable audit storage remain future work (20F-5+). Audit logs may include logical identity and run/checkpoint identifiers and should be protected.

## Phase 20F-5 gating update

Production readiness claims must now satisfy the explicit gates in [production-gates.md](production-gates.md). Phase 20 completion is documented as a security/product-direction milestone, not full production readiness.



## Phase 20 hardening update (in progress)

- Added stricter env-gated MinIO/S3 integration-test behavior with explicit skip messaging.
- Expanded benchmark/runbook documentation labels and caveats for local/dev versus secure/minio optional surfaces.
- Secure Kubernetes runbook depth expanded for local certificate/secret creation flow.
- Production readiness is still not claimed; remaining gaps are tracked in status/production docs.
\n- Phase 20 update: benchmark expansion and MinIO/S3 validation are environment-gated; no production-readiness claim.


Observability update: Step 1 Prometheus/Grafana is implemented. See `docs/observability.md`, `observability/prometheus/prometheus.yml`, and `observability/grafana/dtruntime-dashboard.json`.

## Step 2 status: readiness/liveness probes

Step 2 of the production-readiness roadmap is implemented for local Kubernetes manifests and coordinator HTTP health endpoints:
- `/healthz` (liveness)
- `/readyz` (readiness)
- startup probe wired to `/healthz`

Health endpoints are intentionally low-cost and do not expose secrets.

## Step 3 status: Secure TLS/mTLS deployment validation
Implemented and validated in `k8s/local-secure/` with secret-backed TLS/mTLS mounts and secure Python job connectivity. Runbook includes deterministic namespace cleanup, cert generation, secret creation, ordered applies, readiness/probe checks, secure job completion, and cleanup.
