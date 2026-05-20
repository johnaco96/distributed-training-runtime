# Runbooks

This hub points to copy-ready validation and demo paths without duplicating full manuals.

## Recommended order (new developers)

1. [Quickstart](quickstart.md)
2. Full local validation
3. Local demo
4. Benchmarks
5. PyTorch examples
6. Optional S3/MinIO validation

## 1) Full local validation runbook

```bash
cargo fmt
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
```

## 2) Local demo runbook

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

Cleanup:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
```

## 3) Benchmark runbook

Smoke/local scaling commands:

```bash
cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --simulate-large --workers-list 32,64 --heartbeats 1
```

More benchmark guidance: [Benchmarks](benchmarks.md), [Scaling tests](scaling-tests.md).

## 4) ML/PyTorch runbook (optional)

Single-worker fresh/resume/no-checkpoint:

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 2 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke --resume
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-no-checkpoint --no-checkpoint
```

Simulated distributed-style fresh/resume/no-checkpoint:

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke --resume
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-no-checkpoint --no-checkpoint
```

Detailed notes: [ML Integration Runbook](ml-runbook.md).

## 5) Checkpoint/storage runbook (optional S3/MinIO)

```bash
cargo check -p runtime-storage --features s3
cargo test -p runtime-storage --features s3
cargo check -p runtime-checkpoint --features s3
cargo test -p runtime-checkpoint --features s3
```

Run MinIO-backed tests only when MinIO is configured and env-gated.

Details: [Object storage](object-storage.md), [Checkpoint cleanup](checkpoint-cleanup.md).

## 6) Cleanup runbook

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-no-checkpoint -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-no-checkpoint -ErrorAction SilentlyContinue
```

## Related docs

- [Quickstart](quickstart.md)
- [Example commands](examples.md)
- [Troubleshooting](troubleshooting.md)
- [Status and limitations](status-and-limitations.md)


## Phase 20D packaging/demo references

- Demo/release checklist: [`docs/demo-checklist.md`](demo-checklist.md)
- Project packaging guide: [`docs/project-packaging.md`](project-packaging.md)
- Coordinator-driven PyTorch example: `python/examples/pytorch_coordinator_train.py`
- Orchestration benchmark: `python/examples/benchmark_pytorch_orchestration.py`


## 7) Local Kubernetes runbook (Phase 20E)

For local cluster demo deployment steps, see [`docs/kubernetes-local.md`](kubernetes-local.md).

Scope is intentionally local/dev only (single coordinator replica, no HA/auth/dashboard).

## Security runbook references (Phase 20F-1)

- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
- [Security auth plan](security-auth.md)
- [High availability plan](high-availability.md)
- [Dashboard plan](dashboard-plan.md)
- [Production S3 plan](production-s3.md)

Next implementation step is 20F-2 (optional TLS/mTLS foundation).
\n\n## Phase 20F-2 TLS/mTLS foundation\nPhase 20F-2 adds optional transport security for coordinator gRPC and Python RuntimeClient. Plaintext remains the default local/dev behavior when TLS options are omitted.\n\nCoordinator TLS options:\n- --tls-cert /path/to/server.crt\n- --tls-key /path/to/server.key\n- --tls-client-ca /path/to/ca.crt (optional; enables client cert verification/mTLS)\n\nExample server commands:\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key\n- runtime-coordinator --addr 0.0.0.0:50051 --tls-cert /path/to/server.crt --tls-key /path/to/server.key --tls-client-ca /path/to/ca.crt\n\nPython RuntimeClient supports TLS args and env vars: tls_ca_cert, tls_client_cert, tls_client_key and DTRUNTIME_TLS_CA_CERT, DTRUNTIME_TLS_CLIENT_CERT, DTRUNTIME_TLS_CLIENT_KEY.\n\nNo certificates/keys are committed in-repo; use placeholders such as k8s/local/tls-secret.example.yaml.\nNo workload authorization policy is implemented in 20F-2; that is planned for Phase 20F-3.\nNo production PKI issuance/rotation is implemented in 20F-2.\n
