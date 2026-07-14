# Quickstart

This quickstart gives a concise local setup and validation path for the Distributed Training Runtime prototype.

For a system-level map of components and flows, see [Architecture Overview](architecture.md).

For command copy/paste coverage, see [Example commands](../README.md).

For workflow sequencing, see [Runbooks](PATH-TO-PRODUCTION.md).

For common setup/runtime issues, see [Troubleshooting](troubleshooting.md). For current project boundaries, see [Status and limitations](../README.md).

## Prerequisites

Required:
- Rust toolchain (`rustup`, `cargo`)
- Python 3.10+ (3.12 validated recently)
- `protoc` (Protocol Buffers compiler)
- `PYTHONPATH=python` when running Python commands from repo root

Optional:
- PyTorch (only needed for PyTorch examples)
- Docker/MinIO (only needed for object-storage validation paths)

## Setup (Windows PowerShell first)

From repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
$env:PYTHONPATH = "python"
```

Verify core tools:

```powershell
cargo --version
python --version
protoc --version
```

## Optional Unix/macOS equivalent

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
export PYTHONPATH=python
```

## Fast validation path

Run these from repo root:

```bash
cargo check --workspace
cargo test --workspace
PYTHONPATH=python python -m pytest python/tests
```

## Run coordinator/local demo

```bash
PYTHONPATH=python python python/examples/local_demo.py
```

## Run benchmark smoke

```bash
cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2 --heartbeats 2
```

## Run PyTorch examples (optional)

If PyTorch is installed:

### Single-worker fresh + resume

```bash
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 1 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke
PYTHONPATH=python python python/examples/pytorch_train.py --epochs 2 --samples 32 --batch-size 8 --out-dir .dtruntime-pytorch-smoke --resume
```

### Real 2-worker DDP with crash recovery

The flagship path — real multi-process gloo `torch.distributed`, a worker
killed mid-training, re-rendezvous, and resume from the last committed
checkpoint:

```bash
python demo/run_demo.py
```

Or drive the underlying example directly through the launcher:

```bash
python -m dtruntime.launch --coordinator 127.0.0.1:50051 --run-name smoke \
    --nproc-per-node 2 python/examples/pytorch_ddp_train.py -- --steps 20
```

## Cleanup commands

PowerShell:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
```

Unix/macOS:

```bash
rm -rf .dtruntime-demo .dtruntime-pytorch-smoke
```

## Notes on scope and expectations

- This is a local-first runtime: everything above runs on one machine with no
  external services.
- The DDP examples run real multi-process `torch.distributed` (gloo backend);
  the demo injects a real worker crash and recovers from it.
- S3/MinIO support is implemented and MinIO-tested, but production cloud
  readiness is not claimed by default.
- Use the benchmark commands in [benchmarks/phase1-baseline.md](benchmarks/phase1-baseline.md)
  and [benchmarks/phase3-dataplane.md](benchmarks/phase3-dataplane.md) as the
  source of performance evidence for your environment.
