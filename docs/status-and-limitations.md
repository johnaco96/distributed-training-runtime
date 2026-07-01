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
- Role-based authorization via `--auth-mode metadata`.
- Structured audit logging via `DbAuditSink` and `audit_events` table.
- PostgreSQL HA via advisory lock (2-replica coordinator support).
- SQLite and PostgreSQL metadata persistence backends.
- Local Kubernetes manifests (`k8s/local/`) and secure local Kubernetes path (`k8s/local-secure/`).
- Health endpoints: `/healthz` (liveness) and `/readyz` (readiness).

## Current limitations and non-goals

This project currently does **not** claim:

- Production scheduler integration.
- Production Kubernetes deployment/orchestration support (beyond local/dev manifests).
- Real `torch.distributed` execution.
- Real multi-node ML orchestration.
- Production-ready cloud deployment posture.
- Proven production fault tolerance at scale.
- Large-scale benchmark claims without measured evidence.
- Dashboard/admin UI.
- Full SIEM/durable audit pipeline.
- Complete production S3 hardening.
- OIDC/SPIFFE/SPIRE identity integration.

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
