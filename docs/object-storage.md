# Phase 12A Object Storage Foundation

Phase 12A adds an **S3-compatible storage backend foundation** in `runtime-storage`.

This phase does **not** claim full coordinator checkpoint artifact upload/restore wiring; that can be completed in Phase 12B.

## Purpose

- Keep `LocalStorage` behavior intact for existing workflows/tests.
- Add an S3-compatible backend (`S3Storage`) that can target MinIO.
- Keep MinIO tests optional and environment-gated.

## Checkpoint object key layout

The S3-compatible backend uses the same logical key layout as local storage:

```text
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.pending.json
runs/{run_id}/checkpoints/{checkpoint_id}/workers/{worker_id}/{artifact_name}
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.committed.json
runs/{run_id}/checkpoints/{checkpoint_id}/COMMITTED
runs/{run_id}/LATEST
```

## S3-compatible configuration

Environment variables (recommended):

- `DTRUNTIME_S3_ENDPOINT_URL` (example: `http://127.0.0.1:9000` for MinIO)
- `DTRUNTIME_S3_REGION` (example: `us-east-1`)
- `DTRUNTIME_S3_BUCKET` (example: `dtruntime-checkpoints`)
- `DTRUNTIME_S3_ACCESS_KEY_ID`
- `DTRUNTIME_S3_SECRET_ACCESS_KEY`
- `DTRUNTIME_S3_FORCE_PATH_STYLE` (`true` for MinIO)
- `DTRUNTIME_S3_KEY_PREFIX` (optional)

## Local MinIO setup

Bash:

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

PowerShell:

```powershell
docker run -p 9000:9000 -p 9001:9001 `
  -e MINIO_ROOT_USER=minioadmin `
  -e MINIO_ROOT_PASSWORD=minioadmin `
  quay.io/minio/minio server /data --console-address ":9001"
```

Create bucket: `dtruntime-checkpoints`.

## Running env-gated MinIO tests

MinIO tests are local integration tests and should run only when explicitly enabled, for example with:

```powershell
$env:DTRUNTIME_RUN_MINIO_TESTS="1"
$env:DTRUNTIME_S3_ENDPOINT_URL="http://127.0.0.1:9000"
$env:DTRUNTIME_S3_REGION="us-east-1"
$env:DTRUNTIME_S3_BUCKET="dtruntime-checkpoints"
$env:DTRUNTIME_S3_ACCESS_KEY_ID="minioadmin"
$env:DTRUNTIME_S3_SECRET_ACCESS_KEY="minioadmin"
$env:DTRUNTIME_S3_FORCE_PATH_STYLE="true"
```

Real cloud S3 validation is a later step after local MinIO validation.

## Limitations

- Phase 12A provides storage backend primitives and key mapping.
- Full coordinator artifact upload/restore data-plane wiring is expected in Phase 12B.
- Production readiness, HA, and cloud-hardening are out of scope.
