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

## 7) Local Kubernetes runbook

For local cluster demo deployment steps, see [`docs/kubernetes-local.md`](kubernetes-local.md).

Scope is intentionally local/dev only (single coordinator replica, no HA/auth/dashboard).

## 8) Demo/release references

- Project packaging guide: [`docs/project-packaging.md`](project-packaging.md)
- Coordinator-driven PyTorch example: `python/examples/pytorch_coordinator_train.py`
- Orchestration benchmark: `python/examples/benchmark_pytorch_orchestration.py`

## 9) TLS/mTLS local smoke

Generate local development certificates (PowerShell):

```powershell
pwsh -File scripts/generate_dev_certs.ps1
```

TLS coordinator:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --tls-cert .dtruntime-certs/server.crt --tls-key .dtruntime-certs/server.key
```

mTLS coordinator:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --tls-cert .dtruntime-certs/server.crt --tls-key .dtruntime-certs/server.key --tls-client-ca .dtruntime-certs/ca.crt
```

## Related docs

- [Quickstart](quickstart.md)
- [Example commands](examples.md)
- [Troubleshooting](troubleshooting.md)
- [Status and limitations](status-and-limitations.md)
- [Security architecture](security-architecture.md)
- [Threat model](threat-model.md)
- [Production readiness](production-readiness.md)
- [Security auth plan](security-auth.md)
- [High availability](high-availability.md)
- [Production S3](production-s3.md)

## Observability

Step 1 Prometheus/Grafana is implemented. See `docs/observability.md`, `observability/prometheus/prometheus.yml`, and `observability/grafana/dtruntime-dashboard.json`.
