# Demo and Release Checklist (Phase 20D)

## Prerequisites

- Rust toolchain installed.
- Python 3.10+ virtual environment active.
- Run all Python commands with `PYTHONPATH=python`.
- Optional dependencies for expanded demo scope:
  - `torch` for PyTorch examples/benchmark
  - `google.protobuf` for `local_demo.py`
  - MinIO/S3-compatible endpoint for optional object-storage checks

## Clean repo check

```bash
git status --short
```

Expect clean working tree before demo/release validation.

## Core Rust validation

```bash
cargo fmt
cargo check --workspace
cargo test --workspace
```

## Python validation

```bash
PYTHONPATH=python python -m pytest python/tests
```

## Local demo

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

## Coordinator-driven PyTorch demo

```bash
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator
```

## Benchmark smoke

```bash
PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke
```

## Optional PyTorch standalone/simulated checks

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
```

## Optional S3/MinIO checks

- Enable env-gated checks only when MinIO/S3-compatible endpoint is configured.
- Prefer the documented integration/test commands in runbooks instead of ad hoc commands.

## Cleanup commands

```bash
rm -rf .dtruntime-demo
rm -rf .dtruntime-pytorch-coordinator
rm -rf .dtruntime-pytorch-bench-smoke
rm -rf .dtruntime-pytorch-smoke
rm -rf .dtruntime-pytorch-dist-smoke
```

## Outputs to show in a demo

- `local_demo.py` console output
- `.dtruntime-pytorch-coordinator/metrics.json`
- `.dtruntime-pytorch-bench-smoke/benchmark-summary.json`
- `.dtruntime-pytorch-bench-smoke/benchmark-results.csv`

## Honest talking points

- This is a **local-first** ML runtime control-plane prototype.
- Current benchmark path includes coordinator startup unless warmed/external coordinator mode is used.
- PyTorch orchestration here is **not** real `torch.distributed`.
- Kubernetes deployment is intentionally deferred to Phase 20E.
- Production HA/auth/dashboard posture is intentionally deferred to later phases.
