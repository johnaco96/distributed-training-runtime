# Example Commands

PowerShell-first command index for common local workflows. If commands fail due to setup/dependencies, see [Troubleshooting](troubleshooting.md).

## A) Environment setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
$env:PYTHONPATH="python"
```

Optional gates/deps:

```powershell
$env:DTRUNTIME_RUN_LOCAL_INTEGRATION="1"
$env:PROTOC="C:\path\to\protoc.exe"   # only if protoc is not auto-detected
python -m pip install torch         # optional, only for PyTorch examples
python -m pip install protobuf      # optional, needed for local_demo/protobuf imports
```

Unix/macOS note:

```bash
source .venv/bin/activate
export PYTHONPATH=python
```

## B) Rust validation

```bash
cargo fmt
cargo check --workspace
cargo test --workspace
```

## C) Python validation

```bash
PYTHONPATH=python python -m pytest python/tests
DTRUNTIME_RUN_LOCAL_INTEGRATION=1 PYTHONPATH=python python -m pytest python/tests/test_local_integration.py -v
```

## D) Local demo

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

Cleanup:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
```

## E) Benchmark commands

```bash
cargo run -p runtime-bench -- --help
cargo run -p runtime-bench -- dataset-read --items 128 --payload-bytes 65536 --concurrency 16
cargo run -p runtime-bench -- checkpoint-write --artifacts 16 --payload-bytes 1048576 --concurrency 16
cargo run -p runtime-bench -- checkpoint-restore --iterations 100 --artifacts 16 --payload-bytes 1048576 --concurrency 16
cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --simulate-large --workers-list 32,64 --heartbeats 1
```

## F) PyTorch examples (optional)

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

Cleanup:

```powershell
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-no-checkpoint -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-no-checkpoint -ErrorAction SilentlyContinue
```

## G) S3/MinIO optional commands

The S3 feature checks below do not require MinIO. The MinIO roundtrip test requires MinIO to be running/configured and must be explicitly enabled with DTRUNTIME_RUN_MINIO_TESTS=1.

```bash
cargo check -p runtime-storage --features s3
cargo test -p runtime-storage --features s3
cargo check -p runtime-checkpoint --features s3
cargo test -p runtime-checkpoint --features s3
```

PowerShell MinIO environment example:

```powershell
$env:DTRUNTIME_RUN_MINIO_TESTS="1"
$env:DTRUNTIME_S3_ENDPOINT_URL="http://127.0.0.1:9000"
$env:DTRUNTIME_S3_REGION="us-east-1"
$env:DTRUNTIME_S3_BUCKET="dtruntime-checkpoints"
$env:DTRUNTIME_S3_ACCESS_KEY_ID="minioadmin"
$env:DTRUNTIME_S3_SECRET_ACCESS_KEY="minioadmin"
$env:DTRUNTIME_S3_FORCE_PATH_STYLE="true"
$env:DTRUNTIME_S3_KEY_PREFIX="dev"
cargo test -p runtime-checkpoint --features s3 minio -- --nocapture
```

## H) Git/status and cleanup

```bash
git status --short
git diff
```

Generated output cleanup:

```bash
rm -rf .dtruntime-demo .dtruntime-pytorch-smoke .dtruntime-pytorch-no-checkpoint .dtruntime-pytorch-dist-smoke .dtruntime-pytorch-dist-no-checkpoint
```


Coordinator-driven PyTorch orchestration (Phase 20B):

```bash
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator --resume
PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator-no-checkpoint --no-checkpoint
```


## Phase 20C: Local PyTorch orchestration benchmark

Use `python/examples/benchmark_pytorch_orchestration.py` to compare local standalone training (`pytorch_train.py`) versus local coordinator-driven training (`pytorch_coordinator_train.py`). It records wall-clock timings, overhead (seconds and percent), final loss/accuracy, checkpoint/resume status, and coordinator RPC summary when available. Results are emitted as JSON and CSV for reproducibility and automation.

Example:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 3 --epochs 2 --samples 128 --batch-size 16 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench`

Smoke:

`PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke`

Caveats: this is local-only and environment-specific. Coordinator timings include coordinator startup in the current implementation (no warmed/external coordinator benchmark mode yet). These numbers do **not** prove distributed multi-node scaling, Kubernetes performance, or production throughput.


## Phase 20D reviewer-oriented examples

- Coordinator-driven PyTorch demo:
  - `PYTHONPATH=python python python/examples/pytorch_coordinator_train.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-coordinator`
- Orchestration benchmark smoke:
  - `PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py --runs 1 --epochs 1 --samples 32 --batch-size 8 --workers 2 --shards 4 --out-dir .dtruntime-pytorch-bench-smoke`
- Demo checklist: [`docs/demo-checklist.md`](demo-checklist.md)
- Packaging guide: [`docs/project-packaging.md`](project-packaging.md)


## Phase 20E: local Kubernetes demo

Use the local manifests in `k8s/local/` with the runbook at [`docs/kubernetes-local.md`](kubernetes-local.md) to run:
- one coordinator Deployment
- one coordinator-driven PyTorch demo Job

This proves local packaging/deployment flow only; it does not prove production Kubernetes behavior or multi-node distributed training.
