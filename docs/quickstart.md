# Quickstart

This quickstart gives a concise local setup and validation path for the Distributed Training Runtime prototype.

- [Architecture Overview](architecture.md) — components, flows, and where each
  responsibility lives.
- [README](../README.md) — the headline demo, measured performance, and the
  project's honest limits.
- [PyTorch integration](pytorch-integration.md) — adapting an existing training
  script in a few lines.
- [Troubleshooting](troubleshooting.md) — common setup and runtime issues.
- [Path to production](PATH-TO-PRODUCTION.md) — what a real cluster deployment
  additionally needs.

## Prerequisites

Required:
- Rust toolchain (`rustup`, `cargo`)
- Python 3.9+ (the CI compatibility matrix covers 3.9–3.12 on Linux, macOS,
  and Windows; 3.12 is the primary target)
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
cargo check --workspace --exclude dtruntime-native
cargo test --workspace --exclude dtruntime-native
PYTHONPATH=python python -m pytest python/tests
```

`dtruntime-native` is the optional PyO3 accelerator; building it needs a
CPython >= 3.11 interpreter that `cargo` cannot locate on its own, so it is
excluded here. Build it separately with `python scripts/build_native.py` —
the Python suite exercises it (and its pure-Python fallback) either way.

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
- Use the benchmark commands in [benchmarks/control-plane-scale.md](benchmarks/control-plane-scale.md)
  and [benchmarks/data-plane-throughput.md](benchmarks/data-plane-throughput.md) as the
  source of performance evidence for your environment.
