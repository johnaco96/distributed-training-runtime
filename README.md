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

This benchmark compares local standalone training vs local coordinator-driven training. Current timings include coordinator startup overhead unless/until a warmed external-coordinator mode is used.

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
- [Project packaging](docs/project-packaging.md)

## Validation notes

Environment dependencies can affect validation:
- Cargo checks/tests require crates.io access.
- PyTorch examples require `torch`.
- `local_demo.py` requires `google.protobuf`.
- Optional MinIO/S3 checks require environment setup.

When unavailable, those paths should fail clearly or skip; treat that as environment limitation rather than a product-claim change.

## Kubernetes local demo

A local/dev-first Kubernetes demo path is available.

- Container image: `Dockerfile`
- Manifests: `k8s/local/`
- Runbook: [`docs/kubernetes-local.md`](docs/kubernetes-local.md)

This path demonstrates packaging/deployment for local clusters (kind/minikube) only. It does not claim production Kubernetes readiness, HA, auth, dashboard, or `torch.distributed` execution.

## Security

Transport security and an authorization model are available for coordinator gRPC and the Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.

**TLS/mTLS:** Coordinator accepts `--tls-cert`, `--tls-key`, and optional `--tls-client-ca` flags. The Python client supports `tls_ca_cert`, `tls_client_cert`, `tls_client_key` constructor args and corresponding `DTRUNTIME_TLS_*` env vars. No certificates or keys are committed in-repo; see `k8s/local/tls-secret.example.yaml` for placeholder examples.

**Authorization:** An optional metadata-based caller identity model is available for local/dev testing. Enable with `--auth-mode metadata` or `DTRUNTIME_AUTH_MODE=metadata`. The Python client can attach `x-dtruntime-subject`, `x-dtruntime-role`, and `x-dtruntime-worker-id` via constructor args or `DTRUNTIME_AUTH_*` env vars. This is policy scaffolding — not full SPIFFE/SPIRE or production RBAC.

**Audit logging:** Structured audit/security events are emitted for security-relevant coordinator operations. Audit logs may include logical identity and run/checkpoint identifiers.

Security docs:
- [Security architecture](docs/security-architecture.md)
- [Threat model](docs/threat-model.md)
- [Production readiness](docs/production-readiness.md)
- [Security auth plan](docs/security-auth.md)
- [High availability plan](docs/high-availability.md)
- [Production S3 plan](docs/production-s3.md)

## Observability

Prometheus `/metrics` is available via `--metrics-addr`. Grafana dashboard assets are under `observability/`.

Coordinator health endpoints are available on the metrics HTTP listener (`--metrics-addr`):
- `GET /healthz` for liveness
- `GET /readyz` for readiness

Local Kubernetes manifests wire startup/readiness/liveness probes to these endpoints to prevent early traffic routing and to restart unhealthy pods.

Secure Kubernetes validation: use `k8s/local-secure/` as the runnable secure deployment path. Generate local/dev certs with `scripts/generate_dev_certs.ps1`, create `dtruntime-tls`, `dtruntime-client-ca`, and `dtruntime-client-tls` Secrets, then deploy coordinator + job in order as documented in `k8s/local-secure/README.md`. No certificate or key material is committed to git.

Use `scripts/run_k8s_secure_validation.ps1` to run secure coordinator rollout + secure PyTorch job + metrics artifact capture. This validates secure runtime correctness in Kubernetes and is not a production-scale throughput benchmark.

## Benchmarking

### Standalone vs coordinator-driven comparison

Run `python/examples/benchmark_pytorch_orchestration.py` to compare local standalone training against coordinator-driven training. Results include wall-clock timings, overhead, final loss/accuracy, checkpoint/resume status, and coordinator RPC summary.

### Linux scaling and checkpoint profiling

Use `scripts/run_linux_scaling_benchmark.sh` for Linux-only scaling plus checkpoint-stall summary artifacts under `.dtruntime-results/`. This focuses on control-plane throughput, latency percentiles, and checkpoint stall percent.

### High-scale logical-worker benchmarking

The `runtime-bench logical-workers` mode emulates hundreds to thousands of logical workers from one or more benchmark processes while issuing real coordinator gRPC control-plane RPCs (register worker, shard assignment, heartbeat, checkpoint metadata lifecycle, and restore-latest checks). This is the recommended way to benchmark control-plane scalability without requiring thousands of physical GPUs or pods.

```
cargo run -p runtime-bench -- logical-workers --workers 1000 --duration-secs 60 --concurrency 128 --heartbeat-interval-ms 1000 --shards 10000 --checkpoint-every-secs 30 --checkpoint-artifacts-per-checkpoint 4 --checkpoint-bytes 67108864 --out-json logical-workers.json --out-csv logical-workers.csv
```

Interpretation guidance: use p50/p95/p99 latencies together with coordinator CPU/RSS and disk/network metrics from Linux profiling outputs. Claims about cluster-scale behavior must be based on measured runs and reported caveats; unavailable metrics are recorded as unavailable/null rather than zero.

The benchmark suite also includes `runtime-bench` subcommands: `dataset-prefetch`, `cache-aware-placement`, `auth-audit-overhead`, and `status-snapshot`. Outputs include `environment`, `surface`, `mode`, `caveats`, `timestamp_utc`, duration/latency fields, plus JSON/CSV outputs. MinIO-gated paths remain optional and must be enabled via `DTRUNTIME_RUN_MINIO_TESTS=1`; otherwise they skip cleanly with caveats.
