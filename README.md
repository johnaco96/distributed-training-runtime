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
