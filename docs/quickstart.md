# Quickstart

This quickstart gives a concise local setup and validation path for the Distributed Training Runtime prototype.

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

### Simulated distributed-style fresh + resume

```bash
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 1 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke
PYTHONPATH=python python python/examples/pytorch_simulated_distributed.py --workers 2 --epochs 2 --samples 32 --batch-size 8 --shards 4 --out-dir .dtruntime-pytorch-dist-smoke --resume
```

## Cleanup commands

PowerShell:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-smoke -ErrorAction SilentlyContinue
```

Unix/macOS:

```bash
rm -rf .dtruntime-demo .dtruntime-pytorch-smoke .dtruntime-pytorch-dist-smoke
```

## Notes on scope and expectations

- This is currently a local/distributed-runtime prototype, not a production control plane.
- The PyTorch "distributed" example is simulated and does not use `torch.distributed`.
- S3/MinIO support exists, but production cloud readiness is not claimed by default.
- Use local benchmark commands/results as your source of performance evidence for your environment.
