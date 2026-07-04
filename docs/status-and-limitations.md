# Status and Limitations

## Completion status

The following capabilities are implemented and locally validated:

- Rust coordinator gRPC control plane.
- Python SDK for coordinator interactions.
- Dataset registration and deterministic shard assignment flows.
- Checkpoint lifecycle support including integrity-oriented commit validation and cleanup helper paths.
- Two-phase commit (2PC) for checkpoint commits.
- Storage abstraction with local filesystem support and optional S3/MinIO-compatible backend paths.
- S3 write-through backend for checkpoint artifacts.
- Shard progress persistence.
- Benchmark harness for local RPC and scaling-style smoke runs.
- Observability foundation (structured logs + in-process metrics + Prometheus/Grafana).
- PyTorch single-worker example.
- PyTorch simulated distributed-style example.
- Coordinator-driven PyTorch orchestration example.
- Optional mTLS via `--tls-cert`/`--tls-key`/`--tls-client-ca` with cert-manager PKI.
- Layered authentication: OIDC/JWT bearer tokens (rotatable JWKS/HMAC/PEM keys),
  SPIFFE X.509-SVID from the mTLS peer cert, and a dev metadata-header identity
  (disable-able via `--auth-disable-header-identity`) — see `docs/security-auth.md`.
- Role/permission RBAC (admin/operator/viewer/worker) enforced per-RPC.
- JWT verification-key hot rotation from a file without restart — see
  `docs/secret-rotation.md`.
- Structured audit logging via `DbAuditSink` and `audit_events` table, plus an
  optional NDJSON/SIEM sink (`--audit-log-file`) and auth allow/deny counters.
- Supply-chain scanning in CI: `cargo-deny` (advisories/bans/sources) and
  `pip-audit`, with Dependabot update PRs; `SECURITY.md` policy.
- Prometheus alerting + SLI recording rules (promtool-validated), a
  `PrometheusRule` for the Operator, defined SLOs with multiwindow burn-rate
  alerts, and per-alert on-call runbooks — see `docs/slos.md`,
  `docs/runbooks-oncall.md`, `observability/prometheus/`.
- Gated operational resilience tests against the real release binaries:
  coordinator failover (hard-kill + rehydrate), chaos/preemption (worker-failure
  injection + recovery), and soak (sustained load); a nightly CI workflow runs
  them. Measured control-plane/checkpoint/data-plane benchmark numbers captured
  in `docs/benchmarks.md` (+ raw JSON under `observability/benchmarks/`). See
  `docs/observability-ops-tests.md`.
- Production install paths: a Helm chart (`deploy/helm/dtruntime`, helm-lint +
  template validated) and a hardened raw-manifest bundle (`k8s/prod`, kustomize
  validated) — HA coordinator, PDB, NetworkPolicy, non-root read-only-rootfs
  securityContext, ServiceMonitor, PrometheusRule, cert-manager. See
  `docs/production-deployment.md`.
- Release process: tag-triggered workflow building a `twine check`-clean Python
  sdist/wheel (locally built), multi-arch container images (cosign keyless
  signing + SLSA provenance + SBOM), and a packaged Helm chart; `docs/RELEASING.md`,
  `CHANGELOG.md`. Cross-platform CI matrix (Linux/macOS/Windows + container build).
- PostgreSQL HA via advisory lock (2-replica coordinator support).
- SQLite and PostgreSQL metadata persistence backends.
- Local Kubernetes manifests (`k8s/local/`) and secure local Kubernetes path (`k8s/local-secure/`).
- Health endpoints: `/healthz` (liveness) and `/readyz` (readiness).
- Real `torch.distributed` execution: coordinator-backed rendezvous
  (`JoinRendezvous`/`GetRendezvousState`), coordinator-assigned ranks, and
  process-group formation over c10d TCPStore (see `docs/torch-distributed.md`).
- Worker launcher/orchestration: `python -m dtruntime.launch` (torchrun-style
  supervision with restart budget and multi-node idempotent run registration).
- Rank replacement after worker failure and elastic world-size changes
  (min/max worker bounds with a settle window).
- DDP, FSDP, and DeepSpeed strategy wrappers (`dtruntime.strategies`).
- Kubernetes operator (`runtime-operator`, kube-rs): TrainingRun CRD,
  managed-coordinator rendering, Indexed worker Jobs, status projection
  (see `docs/kubernetes-operator.md`).
- Scheduler backends: plain Kubernetes Jobs, Volcano gang scheduling, Kueue
  queue admission; documented submitter contract for Slurm/Ray/SageMaker/AWS
  Batch (see `docs/schedulers.md`).
- GPU/accelerator requests, node selectors, tolerations, affinity, topology
  spread constraints, and priority classes on TrainingRun specs.
- Durable run lifecycle states (pending/running/recovering/completed/failed/
  cancelled) persisted and rehydrated across coordinator restarts (see
  `docs/run-lifecycle.md`).
- Graceful worker deregistration (`DeregisterWorker`: completed/preempted/
  error) and admin run cancellation (`CancelRun`).
- Stuck-run detection, terminal-run retention cleanup, and orphaned-state
  sweeping via the coordinator's periodic reconcile task.
- Coordinator failover transparency for in-flight training (validated:
  hard-kill + restart on the same metadata DB mid-training completes the run).
- Distributed model + optimizer checkpointing on torch.distributed.checkpoint
  (`dtruntime.checkpointing.DistributedCheckpointer`): multi-process restore
  through the coordinator data plane (`FetchCheckpointArtifact`), world-size
  resharding (validated save@2 → restore@1), strategy-agnostic state dicts,
  async save, partial-upload retry (see `docs/checkpointing-distributed.md`).
- Corrupt-checkpoint detection (checksum verification on write, stream-out,
  and download) with automatic fallback to the previous committed checkpoint
  (`ReportCheckpointCorrupt`), plus `ListCheckpoints` for inspection.
- Checkpoint retention/GC (`--checkpoint-keep-last`) removing old records and
  local artifacts via the reconcile task.
- Streaming data pipeline (`dtruntime.data`): `DistributedShardDataset`
  (`IterableDataset` for PyTorch `DataLoader`), deterministic per-epoch
  shuffling, disk cache with prefetch/backpressure, content-addressed dataset
  versioning, DataLoader-worker sharding, and npz/dtrs/jsonl/parquet/
  webdataset formats (see `docs/data-pipeline.md`).
- Production S3 hardening: retryable-vs-permanent error classification,
  multipart upload for large objects, full-jitter retry backoff, and
  server-side encryption (`--s3-sse`); validated with load + integrity tests
  against MinIO (see `docs/storage-hardening.md`).
- Versioned, forward-only DB schema migrations with a `schema_migrations`
  ledger, plus SQLite WAL durability; validated with concurrent
  durability/load tests (SQLite always; PostgreSQL gated) — see
  `docs/schema-migrations.md`.
- Enforced protobuf wire compatibility: a golden-snapshot test fails any
  breaking field change (see `docs/protocol-compatibility.md`).
- Admin lifecycle operations: pause/resume/retry/drain runs (plus existing
  cancel) via `PauseRun`/`ResumeRun`/`RetryRun`/`DrainRun`, with a persisted
  per-run retry counter (added as schema migration v2). See
  `docs/run-lifecycle.md`.
- Admin inspection RPCs: `ListRuns` (all/active-jobs/failures by state),
  `ListWorkers`, and `ListShardProgress` (shard leases + offsets).
- Operator CLI `dtruntime-admin`: inspect runs/workers/shards/checkpoints, drive
  lifecycle actions, `doctor` (coordinator probe + stuck-worker/bad-checkpoint
  diagnosis), and `checkpoint verify` (download + checksum artifacts), with
  `--json` output and structured, remediation-oriented error reporting (see
  `docs/admin-cli.md`).

## Current limitations and non-goals

This project currently does **not** claim:

- Multi-node training validated on a real GPU cluster (the torch.distributed
  path is validated with multi-process gloo on a single machine and in kind;
  NCCL multi-node uses the same rendezvous protocol but is not yet
  cluster-proven).
- Volcano/Kueue backends exercised against real Volcano/Kueue installations
  (adaptation logic is unit-tested; the rendered objects follow those
  projects' documented contracts).
- Production-ready cloud deployment posture.
- Proven production fault tolerance at scale.
- Large-scale benchmark claims without measured evidence.
- Dashboard/admin UI.
- A live SIEM deployment (the NDJSON `--audit-log-file` sink is SIEM-ingestable
  and validated locally, but not exercised against a running SIEM/log pipeline).
- S3 hardening validated against AWS S3 at scale (the hardening — multipart,
  error classification, jitter, SSE — is implemented and load-tested against
  MinIO, but not yet exercised against production AWS S3 under real load).
- OIDC/SPIFFE/SPIRE integration validated against a *live* provider/SPIRE
  server (the token/SVID validation is implemented and tested locally with
  minted tokens and generated certs, following the standard JWKS/SVID contracts).

Important boundary: local simulated worker scaling is useful for smoke/perf exploration, but it is **not** proof of real cluster scale.

## Validation status

Locally validated baseline commands generally include:

- `cargo fmt`
- `cargo check --workspace`
- `cargo test --workspace`
- `PYTHONPATH=python python -m pytest python/tests`
- `PYTHONPATH=python python python/examples/local_demo.py`
- `DTRUNTIME_RUN_LOCAL_INTEGRATION=1 PYTHONPATH=python python -m pytest python/tests/test_local_integration.py -v`

Optional or environment-gated validation surfaces:

- MinIO tests (require env gate and local MinIO service setup).
- PyTorch example runs (require torch installed).
- Any network-dependent dependency fetches (may fail in restricted environments).

Before external demo/release checkpoints, re-run the full local validation set in a clean environment and include any optional paths relevant to your deployment goals.

## Security references

- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
- [Security auth plan](security-auth.md)
- [High availability](high-availability.md)
- [Production S3](production-s3.md)
