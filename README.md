# Distributed Training Runtime

Distributed Training Runtime is a **local-first prototype** for coordinating ML-oriented runtime tasks with a Rust coordinator and a Python SDK. It focuses on dataset sharding, worker coordination, checkpoint metadata/artifact flows, and local validation workflows that are useful for development and experimentation.

## What this project currently does

- Provides a Rust gRPC coordinator and Python client SDK for local runtime experiments.
- Supports dataset registration, shard assignment, worker heartbeats, and lease-aware coordination.
- Supports checkpoint metadata flows plus local/object-storage artifact paths (including MinIO/S3-compatible backend support).
- Includes local benchmark harnesses for coordinator/worker RPC and scaling-oriented simulations.
- Includes PyTorch examples for single-worker training and a **simulated** distributed-style workflow with checkpoint resume.

## What this project does **not** claim yet

- Not production distributed training orchestration.
- Not real multi-node scheduler-backed ML execution.
- Not `torch.distributed` integration.
- Not Kubernetes deployment support.
- Not Prometheus/Grafana exporter integration (current observability is structured logs + in-process metrics).
- Not a fully validated cloud-production S3 path by default (must be explicitly validated in your environment).

## Current status (Phases 10–18 complete)

This repository is currently at **Phase 19D (documentation polish complete)**, with Phases 10 through 18 completed.

Highlights from completed phases:
- Benchmark harness foundations and scaling simulation paths.
- Observability foundations (structured logs + in-process metrics).
- Hardening work for retries/idempotency, lease behavior, and checkpoint cleanup helpers.
- PyTorch integration examples (single-worker and simulated distributed-style) with checkpoint save/restore.

## Architecture overview (concise)

```text
Python training / demo scripts
            |
            v
       Python SDK
            |
            v
            gRPC
            |
            v
     Rust coordinator (MVP)
            |
            +--> Worker lifecycle + leasing
            +--> Dataset/shard coordination
            +--> Checkpoint metadata + artifact storage backends
```

For deeper architecture details, see [`docs/architecture.md`](docs/architecture.md).

## Quickstart

Start here: **[`docs/quickstart.md`](docs/quickstart.md)**

The quickstart covers prerequisites, setup (PowerShell-first), fast validation commands, example runs, and cleanup.

## Core commands

```bash
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
```

## Examples

### Local demo

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

### Benchmark harness smoke

```bash
cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2 --heartbeats 2
```

### PyTorch single-worker example

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 2 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke --resume
```

### PyTorch simulated distributed-style example

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke --resume
```

## Documentation index

- [Quickstart](docs/quickstart.md)
- [Architecture](docs/architecture.md)
- [Benchmarks](docs/benchmarks.md)
- [Scaling tests](docs/scaling-tests.md)
- [Observability](docs/observability.md)
- [Protocol compatibility](docs/protocol-compatibility.md)
- [Retry and idempotency](docs/retry-idempotency.md)
- [Leases](docs/leases.md)
- [Checkpoint cleanup](docs/checkpoint-cleanup.md)
- [Object storage](docs/object-storage.md)
- [Checkpoint protocol](docs/checkpoint-protocol.md)
- [ML integration](docs/ml-integration.md)
- [ML runbook](docs/ml-runbook.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Status and limitations](docs/status-and-limitations.md)

## Limitations and non-goals (current)

For a consolidated status/limitations view, see [Status and limitations](docs/status-and-limitations.md).


- Coordinator behavior is still MVP-oriented and local-development focused.
- Persistence and object-storage paths exist, but production hardening/operations posture is not claimed here.
- Distributed PyTorch workflow is intentionally simulated and does not use real `torch.distributed` primitives.
- No production scheduler integration.
- No Kubernetes deployment path.
- No Prometheus/Grafana exporter integration yet.
- Performance expectations should come from your own local benchmark runs in this repo; no external scaling guarantees are claimed.

## Validation status

Recent local validation scope (environment-dependent):
- `cargo fmt`
- `cargo check --workspace`
- `cargo test --workspace`
- `PYTHONPATH=python python -m pytest python/tests`
- `PYTHONPATH=python python python/examples/local_demo.py`
- local integration tests

If your environment is missing optional dependencies/services (for example, crates index access, `google.protobuf`, PyTorch, or Docker/MinIO), see the quickstart and runbook notes for expected behavior and workarounds.
