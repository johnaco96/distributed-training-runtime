# Troubleshooting Guide

This guide captures the most common local-development issues encountered during development.

## A) Rust/build issues

### crates.io/proxy/network 403 during `cargo` commands
If `cargo check`/`cargo test` fails while downloading dependencies (for example `CONNECT tunnel failed, response 403`), this is usually an environment/proxy/network issue, not a runtime code regression.

- Retry later or from a network with crates.io access.
- If you use a corporate proxy, confirm Cargo proxy configuration.
- Treat this as a setup blocker, not a code failure.

### Dependency fetch failures in `cargo check` / `cargo test`
When dependencies cannot be fetched, formatting/docs edits may still proceed, but full Rust validation is incomplete.

### Rust toolchain too old (optional S3/AWS features)
Some optional S3/AWS crates may require newer toolchains.

- Check: `rustc --version`
- Update with `rustup update`
- Re-run `cargo check --workspace`

### `cargo fmt` behavior
`cargo fmt` can rewrite files for style only. Run it before checks to avoid style-only CI noise.

### Warnings vs failures
- Warnings do not automatically fail default Cargo commands.
- Non-zero exit from `cargo check`/`cargo test` should be treated as a failure.

## B) Protobuf issues

### Missing `protoc`
If build or Python flows mention `protoc` not found:

- Install Protocol Buffers compiler.
- Verify: `protoc --version`

### Missing `google/protobuf/timestamp.proto`
This usually indicates protobuf include paths are incomplete. Ensure standard protobuf includes are available in your `protoc` installation.

### Set `PROTOC` on Windows PowerShell
If auto-detection fails:

```powershell
$env:PROTOC = "C:\\path\\to\\protoc.exe"
```

### `google.protobuf` missing for `local_demo.py`
Install Python protobuf package in your active venv:

```powershell
python -m pip install protobuf
```

## C) Python / `PYTHONPATH` issues

### `ModuleNotFoundError: dtruntime`
Run Python commands from repo root with:

```powershell
$env:PYTHONPATH = "python"
```

Unix/macOS equivalent:

```bash
export PYTHONPATH=python
```

### Virtual environment reminders
Use a venv and activate it before installs/tests to avoid mixing global packages.

## D) PyTorch issues

### `torch` not installed
PyTorch examples are optional and should be skipped when torch is unavailable.

### Expected optional behavior
- PyTorch-specific tests/examples may skip or fail gracefully when torch is missing.
- Core Rust + Python SDK validation does not require torch.

### PyTorch 2.6+ `weights_only` considerations
Checkpoint config/state should remain safe primitive types to avoid unsafe pickle-like restore patterns and compatibility surprises.

### GPU/CUDA not required
All project examples are intended to run on CPU.

### No torchvision/external dataset required
Examples use synthetic/local data; external datasets are not required for basic validation.

## E) Local demo / integration test issues

### `local_demo.py` dependency note
`python/examples/local_demo.py` needs `google.protobuf` available.

### Integration test env gate
`python/tests/test_local_integration.py` is gated behind:

```bash
DTRUNTIME_RUN_LOCAL_INTEGRATION=1
```

### Coordinator port conflicts
If startup fails on an in-use port, stop the conflicting process and retry.

### Cleanup local demo output
```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
```

## F) Benchmark issues

### CLI typo pitfalls
Use `--heartbeats` (not misspellings like `--hearbeats`).

### External coordinator transport errors
If benchmark client mode targets an unavailable address, transport errors are expected. Verify host/port and coordinator lifecycle.

### `scaling-workers` lifecycle
`scaling-workers` local mode manages a coordinator lifecycle for local smoke runs; external mode requires a running coordinator.

### Interpreting output
Treat results as local smoke/perf signals, not production-scale claims.

## G) S3 / MinIO issues

### Optional validation surface
- S3 feature checks are optional.
- MinIO integration tests are env-gated and not required for normal local validation.

### MinIO env gate
```bash
DTRUNTIME_RUN_MINIO_TESTS=1
```

### Typical required env vars
Set endpoint/credentials/bucket/region/path-style values expected by your local MinIO setup.

### Endpoint/bucket/path-style reminders
- Confirm bucket exists.
- Use correct endpoint URL.
- Ensure path-style setting matches your MinIO configuration.

## H) Generated output cleanup

Common local output directories:
- `.dtruntime-demo`
- `.dtruntime-pytorch-smoke`
- `.dtruntime-pytorch-no-checkpoint`
- `.dtruntime-pytorch-dist-smoke`
- `.dtruntime-pytorch-dist-no-checkpoint`

PowerShell cleanup:

```powershell
Remove-Item -Recurse -Force .dtruntime-demo -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-no-checkpoint -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-smoke -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .dtruntime-pytorch-dist-no-checkpoint -ErrorAction SilentlyContinue
```

## I) Security configuration

When using optional TLS/mTLS or auth mode:
- No real secrets/tokens should be committed.
- Local placeholder values should be clearly marked example-only.
- Use `--auth-mode metadata` only for local/dev testing; this is not production identity assurance.
- Audit logs are implemented via `DbAuditSink` and include logical identity and run/checkpoint identifiers; protect logs accordingly.
