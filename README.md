# Distributed Training Runtime

Distributed Training Runtime is a **local-first ML runtime control-plane prototype**. It is a **Rust/Python runtime for worker coordination, dataset shard assignment, checkpoint metadata, and training recovery experiments**.

This repository is positioned as a **serious ML infrastructure prototype/research project**: it demonstrates practical control-plane concepts and reproducible local validation flows without claiming production-scale deployment readiness.

## What is this project?

A local runtime stack with:
- Rust coordinator services (gRPC control-plane surface)
- Python SDK and runnable examples
- Coordinator-backed worker/dataset/shard/checkpoint flows
- Local benchmark surfaces for orchestration overhead exploration

## What problem does it explore?

How to make ML training control-plane behaviors explicit and testable in a local-first environment:
- deterministic shard assignment
- worker registration/heartbeat lifecycle
- checkpoint metadata + artifact lifecycle
- recovery/resume experiments
- overhead trade-offs between standalone training and coordinator-driven orchestration

## What works today?

- Workspace Rust crates build/test locally (when Cargo dependency access is available).
- Python SDK tests run locally.
- Local demo (`local_demo.py`) exercises coordinator + SDK plumbing.
- Coordinator-driven PyTorch example (`pytorch_coordinator_train.py`) runs local-first orchestration flow.
- Benchmark harness (`benchmark_pytorch_orchestration.py`) compares standalone vs coordinator-driven local training and emits JSON/CSV outputs.

## Core demo path

1. Run Rust + Python validation checks.
2. Run `python/examples/local_demo.py` to confirm end-to-end local coordinator interaction.
3. Run `python/examples/pytorch_coordinator_train.py` with a small workload.
4. Inspect produced metrics/checkpoint outputs.

## Benchmark path

Run `python/examples/benchmark_pytorch_orchestration.py` in smoke mode, then inspect:
- `benchmark-summary.json`
- `benchmark-results.csv`

This benchmark compares local standalone training vs local coordinator-driven training. Current timings include coordinator startup overhead unless/ until a warmed external-coordinator mode is used.

## What is **not** claimed yet?

This project does **not** currently claim:
- production replacement for Ray, Kubeflow, or TorchElastic
- Kubernetes-native training operator behavior
- production cloud scheduler functionality
- proven multi-node distributed training performance
- `torch.distributed` integration
- production HA coordinator replication
- production auth/dashboard stack

## Reviewer quick path

Run these first from repository root:

```bash
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
PYTHONPATH=python python python/examples/local_demo.py
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator
PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke
```

## Documentation map

- [Quickstart](docs/quickstart.md)
- [Architecture](docs/architecture.md)
- [Runbooks](docs/runbooks.md)
- [Examples](docs/examples.md)
- [Benchmarks](docs/benchmarks.md)
- [ML integration](docs/ml-integration.md)
- [ML runbook](docs/ml-runbook.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Status and limitations](docs/status-and-limitations.md)
- [Observability](docs/observability.md)
- [Product direction](docs/product-direction.md)
- [Demo checklist](docs/demo-checklist.md)
- [Project packaging](docs/project-packaging.md)

## Validation notes

Environment dependencies can affect validation:
- Cargo checks/tests require crates.io access.
- PyTorch examples require `torch`.
- `local_demo.py` requires `google.protobuf`.
- Optional MinIO/S3 checks require environment setup.

When unavailable, those paths should fail clearly or skip; treat that as environment limitation rather than a product-claim change.


## Kubernetes local demo (Phase 20E)

A local/dev-first Kubernetes demo path is now available.

- Container image: `Dockerfile`
- Manifests: `k8s/local/`
- Runbook: [`docs/kubernetes-local.md`](docs/kubernetes-local.md)

This path demonstrates packaging/deployment for local clusters (kind/minikube) only. It does not claim production Kubernetes readiness, HA, auth, dashboard, or `torch.distributed` execution.

## Phase 20F-1 security foundation

Phase **20F-1** (security architecture/threat-model foundation) is now documented.

New security docs:
- [Security architecture](docs/security-architecture.md)
- [Threat model](docs/threat-model.md)
- [Production readiness](docs/production-readiness.md)
- [Security auth plan](docs/security-auth.md)
- [High availability plan](docs/high-availability.md)
- [Dashboard plan](docs/dashboard-plan.md)
- [Production S3 plan](docs/production-s3.md)

Important: auth/mTLS, dashboard, HA replication, and production S3 hardening are **not** fully implemented in 20F-1. The next implementation step is **20F-2** (optional TLS/mTLS foundation).
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n


## Phase 20F-3 authorization scaffolding
Phase 20F-3 adds an internal authorization model with optional metadata-based caller identity for local/dev testing. Default mode remains disabled for compatibility. Enable with `runtime-coordinator --addr 0.0.0.0:50051 --auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and optional `x-dtruntime-worker-id` via constructor args or env (`DTRUNTIME_AUTH_SUBJECT`, `DTRUNTIME_AUTH_ROLE`, `DTRUNTIME_AUTH_WORKER_ID`). This is policy scaffolding only (not full SPIFFE/SPIRE or production RBAC). Audit/security events remain planned for Phase 20F-4; dashboard/HA/production S3 hardening remain Phase 20F-5.


Phase 20F-4 adds structured audit/security events; dashboard/SIEM/durable audit storage remain future work (20F-5+). Audit logs may include logical identity and run/checkpoint identifiers and should be protected.

## Phase 20F-5 completion status

Phase **20F-5** is complete for **secure design tracks** (dashboard, HA, and production S3 planning), and Phase 20 is complete as a **product-direction/security-foundation milestone**.

This is **not** a claim that dashboard implementation, coordinator HA replication, or production S3 hardening are implemented. Local Kubernetes remains local/dev only.

See:
- [Phase 20F summary](docs/phase-20f-summary.md)
- [Production gates checklist](docs/production-gates.md)



## Phase 20 hardening update (in progress)

- Added stricter env-gated MinIO/S3 integration-test behavior with explicit skip messaging.
- Expanded benchmark/runbook documentation labels and caveats for local/dev versus secure/minio optional surfaces.
- Secure Kubernetes runbook depth expanded for local certificate/secret creation flow.
- Production readiness is still not claimed; remaining gaps are tracked in status/production docs.
\n- Phase 20 update: benchmark expansion and MinIO/S3 validation are environment-gated; no production-readiness claim.


## Phase 20 benchmark surface expansion
Added runtime-bench subcommands: `dataset-prefetch`, `cache-aware-placement`, `auth-audit-overhead`, and `status-snapshot`. Outputs now include `environment`, `surface`, `mode`, `caveats`, `timestamp_utc`, duration/latency fields, plus JSON/CSV outputs. MinIO-gated paths remain optional and must be enabled via `DTRUNTIME_RUN_MINIO_TESTS=1`; otherwise they should skip cleanly with caveats.


## Step 1 production observability
Prometheus /metrics is available via `--metrics-addr` and Grafana dashboard assets are under `observability/`.

## Step 2 production probes

Coordinator health endpoints are now available on the metrics HTTP listener (`--metrics-addr`):
- `GET /healthz` for liveness
- `GET /readyz` for readiness

Local Kubernetes manifests wire startup/readiness/liveness probes to these endpoints to prevent early traffic routing and to restart unhealthy pods.

## Step 3: Secure TLS/mTLS Kubernetes validation
Use `k8s/local-secure/` as the runnable secure deployment path. Generate local/dev certs with `scripts/generate_dev_certs.ps1`, create `dtruntime-tls`, `dtruntime-client-ca`, and `dtruntime-client-tls` Secrets, then deploy coordinator + job in order as documented in `k8s/local-secure/README.md`. No certificate or key material is committed to git.

### Step 4 production-style Kubernetes validation benchmark

Use `scripts/run_k8s_secure_validation.ps1` to run secure coordinator rollout + secure PyTorch job + metrics artifact capture. The benchmark is evidence of secure runtime correctness in Kubernetes, not a production-scale throughput benchmark.
