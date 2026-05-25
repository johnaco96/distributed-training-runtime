# Production Readiness

## 1) Current status

Distributed Training Runtime provides a validated local/dev prototype path with meaningful production-oriented controls implemented.

## 2) Deployment readiness

- Local container image and local Kubernetes manifests exist.
- Single-replica coordinator deployment path is documented.
- Deployment flow is local/dev-first and intentionally conservative.

## 3) Security readiness

Implemented:
- Checkpoint integrity/idempotency-related protections.
- Local-only packaging defaults (ClusterIP service, no privileged pods in local manifests).
- Optional mTLS via `--tls-cert`/`--tls-key`/`--tls-client-ca` with cert-manager PKI in local-secure Kubernetes path.
- Role-based authorization via `--auth-mode metadata`.
- Structured audit logging via `DbAuditSink` and `audit_events` table.

Not yet complete for production:
- Full workload identity (SPIFFE/SPIRE).
- Externalized production secret management.

## 4) Observability/dashboard readiness

- Structured logs and in-process metrics foundation exist.
- Prometheus/Grafana stack implemented (`observability/` directory).
- Coordinator exposes `/healthz` and `/readyz` HTTP health endpoints.
- Dashboard UI/admin surface is not implemented.

## 5) HA/coordinator replication readiness

- PostgreSQL-backed HA is implemented via advisory lock with 2-replica coordinator support.
- Coordinator uses advisory lock for leader election; replicas wait for the lock before serving.
- SQLite is also supported for single-node persistence.

## 6) Storage/S3 readiness

- Local filesystem and S3-compatible interfaces exist.
- S3 write-through backend implemented for checkpoint artifacts.
- Production S3 credential lifecycle, IAM scoping, encryption/lifecycle/tuning, and rotation guidance are not fully implemented.

## 7) Benchmark readiness

- Local benchmark tooling exists and emits reproducible artifacts.
- Results are environment-specific and must be labeled accordingly.
- Current benchmarks are not production throughput claims.

## 8) Kubernetes hardening readiness

- Local manifests are intentionally minimal and local/dev oriented.
- Secure local Kubernetes path exists under `k8s/local-secure/`.
- Production hardening (policy, identity, secret management, stricter admission/pod security controls) is pending.

## 9) Production release gate checklist

Before production claims, all of the following should be complete and validated:
- mTLS enabled and tested. ✓ (implemented for local-secure path)
- Workload identity implemented.
- Authorization policy enforced. ✓ (metadata-based implemented)
- Audit logs implemented. ✓ (DbAuditSink implemented)
- No secrets in logs. ✓
- Kubernetes manifests hardened.
- S3 credentials sourced from a secret manager.
- Least-privilege storage policy validated.
- HA failover tests completed. ✓ (advisory lock HA implemented)
- Backup/restore tests completed.
- Dashboard authentication implemented (if dashboard exists).
- Benchmark results labeled, reproducible, and environment-scoped.

## 10) Explicit non-claims

This repository currently does **not** claim:
- Full production readiness.
- Dashboard readiness.
- Completed production S3 hardening.

## Step 2 status: readiness/liveness probes

Coordinator HTTP health endpoints and Kubernetes startup/readiness/liveness probes are implemented:
- `/healthz` (liveness)
- `/readyz` (readiness)
- startup probe wired to `/healthz`

## Step 3 status: Secure TLS/mTLS deployment validation

Implemented and validated in `k8s/local-secure/` with secret-backed TLS/mTLS mounts and secure Python job connectivity to `runtime-coordinator.dtruntime.svc.cluster.local:50051`. Hostname verification remains enabled; no generated cert/key material should be committed.

## Step 4 — Real Kubernetes benchmark/validation run

Implemented by `scripts/run_k8s_secure_validation.ps1` with timestamped output, unique image-tag support, secure rollout checks, and metrics capture.

### Linux profiling benchmark status

A Linux-only benchmark suite is available through `scripts/run_linux_scaling_benchmark.sh` and `scripts/collect_linux_profile.sh`. Results are scoped to host/workload tested and must be interpreted with caveats.

### High-scale logical-worker benchmarking

The runtime includes a `runtime-bench logical-workers` mode that emulates hundreds to thousands of logical workers from one or more benchmark processes while issuing real coordinator gRPC control-plane RPCs.

Use:
`cargo run -p runtime-bench -- logical-workers --workers 1000 --duration-secs 60 --concurrency 128 --heartbeat-interval-ms 1000 --shards 10000 --checkpoint-every-secs 30 --checkpoint-artifacts-per-checkpoint 4 --checkpoint-bytes 67108864 --out-json logical-workers.json --out-csv logical-workers.csv`

Interpretation guidance: use p50/p95/p99 latencies together with coordinator CPU/RSS and disk/network metrics from Linux profiling outputs. Claims about cluster-scale behavior must be based on measured runs and reported caveats; unavailable metrics are recorded as unavailable/null rather than zero.

### Observability

Step 1 Prometheus/Grafana is implemented. See `docs/observability.md`, `observability/prometheus/prometheus.yml`, and `observability/grafana/dtruntime-dashboard.json`.
