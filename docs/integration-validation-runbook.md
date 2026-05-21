# Integration Validation Runbook (WSL/Linux Native)

Use native Linux/WSL filesystem paths for checkpoint I/O because `/mnt/c` introduces significant syscall and fsync overhead that can mask runtime behavior.

## 1) Start coordinator

```bash
source "$HOME/.cargo/env"
cargo run --release -p runtime-coordinator -- \
  --addr 127.0.0.1:50051 \
  --metrics-addr 127.0.0.1:9090
```

## 2) Activate Python env

```bash
source .venv-wsl/bin/activate
export DTRUNTIME_COORDINATOR_ADDR="127.0.0.1:50051"
export DTRUNTIME_METRICS_ADDR="http://127.0.0.1:9090"
```

Health checks:

```bash
curl http://127.0.0.1:9090/healthz
curl http://127.0.0.1:9090/readyz
```

## 3) Full integration validation (localfs + restore + failure-recovery + multi-worker)

```bash
scripts/run_integration_validation.sh
```

## 4) Fresh-process restore-only validation

```bash
PYTHONPATH=python python python/examples/restore_latest_checkpoint.py \
  --run-id <RUN_ID> \
  --expect-backend localfs \
  --local-root .dtruntime-results/integration-<timestamp>/localfs-managed
```

## 5) MinIO/S3-compatible validation

Set MinIO/S3 env first:

```bash
export DTRUNTIME_S3_ENDPOINT=http://127.0.0.1:9000
export DTRUNTIME_S3_REGION=us-east-1
export DTRUNTIME_S3_ACCESS_KEY_ID=minioadmin
export DTRUNTIME_S3_SECRET_ACCESS_KEY=minioadmin
export DTRUNTIME_S3_BUCKET=dtruntime-checkpoints
```

Then rerun:

```bash
scripts/run_integration_validation.sh
```

## 6) Artifacts produced

The script writes results under:

- `.dtruntime-results/integration-<timestamp>/localfs-managed/`
- `.dtruntime-results/integration-<timestamp>/minio-managed/`
- `.dtruntime-results/integration-<timestamp>/failure-recovery/`
- `.dtruntime-results/integration-<timestamp>/multi-worker/`

## 7) Pass/fail interpretation

Pass requires:
- Managed localfs run commits checkpoint and restore succeeds.
- Fresh-process restore confirms latest checkpoint artifact exists.
- Multi-worker path shows >=4 workers and shard assignments in metrics.
- MinIO path only passes when object-list and restore metadata are both present.

## 8) Before optimization

This runbook validates wiring and recoverability, not latency optimization. Defer async checkpointing and throughput tuning until after these integration checks are stable.
