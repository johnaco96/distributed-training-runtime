# Phase 10 Local End-to-End Demo

## Purpose

This demo exercises Phase 10 end-to-end behavior with the **real Rust coordinator** and the **Python SDK/gRPC client path** in one local run.

It demonstrates:

- run registration,
- two-worker registration/heartbeat,
- dataset registration,
- shard assignment,
- checkpoint begin/report/commit,
- simulated worker restart/rejoin,
- restore latest committed checkpoint metadata.

## Prerequisites

- Rust/Cargo installed.
- Python virtual environment created and activated.
- Python dependencies installed.
- `protoc` available if regeneration is needed.
- `PYTHONPATH=python` for SDK imports in this repo layout.

## Windows PowerShell Setup

```powershell
.\.venv\Scripts\Activate.ps1
```

If script execution is blocked:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Set `PROTOC` if needed:

```powershell
$env:PROTOC="C:\Users\johna\AppData\Local\Microsoft\WinGet\Packages\Google.Protobuf_Microsoft.Winget.Source_8wekyb3d8bbwe\bin\protoc.exe"
```

Set `PYTHONPATH`:

```powershell
$env:PYTHONPATH="python"
```

## Baseline checks

```powershell
cargo fmt
cargo check --workspace
cargo test --workspace
python -m pytest python/tests
```

## Local integration test

```powershell
$env:DTRUNTIME_RUN_LOCAL_INTEGRATION="1"
$env:PYTHONPATH="python"
python -m pytest python/tests/test_local_integration.py -v
```

## Run the Phase 10 demo

```powershell
$env:PYTHONPATH="python"
python python/examples/local_demo.py
```

## Optional manual coordinator mode

The demo starts/stops coordinator automatically by default. If needed, you can run coordinator manually and point the demo to it.

Start coordinator:

```powershell
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051
```

Run demo against manual coordinator:

```powershell
$env:PYTHONPATH="python"
python python/examples/local_demo.py --addr 127.0.0.1:50051
```

## Expected output shape (example)

```text
Starting runtime-coordinator at 127.0.0.1:54xxx
Coordinator reachable: 127.0.0.1:54xxx
Run registered: run_id=run-...
Workers registered and heartbeated: ['worker-0', 'worker-1']
Dataset registered: dataset_id=dataset-demo-...
Shard assignment for worker-0: [...]
Shard assignment for worker-1: [...]
Checkpoint started: checkpoint_id=checkpoint-demo-1
Artifact URIs: {'worker-0': 'file:///...worker-0-model.bin', 'worker-1': 'file:///...worker-1-model.bin'}
Checkpoint commit state: 4
Simulating worker-1 stop (heartbeats paused briefly).
Not waiting for production stale/failure timeout windows in this demo.
worker-1 has rejoined/reactivated.
Restored latest checkpoint id: checkpoint-demo-1
Phase 10 local demo completed successfully
```

IDs and ports vary by run.

## Real vs simulated behavior

### Real

- Rust coordinator process.
- Python SDK + generated gRPC calls.
- RegisterRun call.
- worker registration and heartbeats.
- dataset registration and shard assignment.
- checkpoint begin/report/commit.
- restore latest checkpoint metadata.

### Simulated / demo-local

- checkpoint artifact bytes are written locally by the demo script to `.dtruntime-demo/checkpoints/...` and reported by URI.
- worker death/restart is simulated by pausing and then re-registering/heartbeating `worker-1`.

## Current MVP limitations

- checkpoint artifact bytes are written locally by the Python demo, not uploaded through a coordinator data plane.
- coordinator checkpoint metadata is currently in-memory.
- local storage-backed checkpoint library exists, but coordinator persistence is not wired into coordinator runtime yet.
- worker death is simulated at demo level.
- deterministic stale/failed transitions are covered by Rust tests.
- coordinator process restart recovery belongs to Phase 11, not Phase 10.
