# Object Storage

An **S3-compatible storage backend** is provided in `runtime-storage` and wired into **checkpoint store write/restore tests** with an **env-gated MinIO validation path**.

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
docker run -p 9000:9000 -p 9001:9001   -e MINIO_ROOT_USER=minioadmin   -e MINIO_ROOT_PASSWORD=minioadmin   quay.io/minio/minio server /data --console-address ":9001"
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
$env:DTRUNTIME_S3_KEY_PREFIX="dev"
```

Then run:

```powershell
cargo test -p runtime-storage --features s3
cargo test -p runtime-checkpoint --features s3 minio -- --nocapture
```

Manual bucket creation flow (if bucket is not already present):

- Open `http://127.0.0.1:9001`
- Log in with `minioadmin / minioadmin`
- Create bucket `dtruntime-checkpoints`

Real AWS S3 validation is **not required** for local development.

## Reliability guardrails

A reusable `StorageOperationPolicy` in `runtime-storage` provides async-capable storage operation safety for `S3Storage`. Timeout/retry/backpressure cover `put`, `get`, `exists` (head), `delete`, and `list_prefix`.

Default policy values:

- `operation_timeout`: 30 seconds
- `max_retries`: 3
- `retry_initial_backoff`: 100 milliseconds
- `retry_max_backoff`: 2 seconds
- `max_concurrent_operations`: 16

Primitives provided:

- timeout guardrail using `tokio::time::timeout` for async storage calls,
- retry helper with capped exponential backoff for retryable storage failures,
- concurrency limiter based on a semaphore to enforce max in-flight operations.

Scope note:

- MinIO/AWS paths can use the same policies.
- Normal unit/integration tests remain MinIO-optional and do not require AWS credentials.

## Extended S3 operation coverage

Policy coverage applies to additional S3 operations:

- Timeout/retry/backpressure apply to `S3Storage::put`, `get`, `exists`, `delete`, and `list_prefix`.
- `runtime-storage` includes small backend-neutral batch helpers: `put_many`, `get_many`, and `exists_many`, each with bounded concurrency via `StorageOperationPolicy.max_concurrent_operations` and per-item result reporting.
- Multipart upload, coordinator gRPC artifact-byte streaming, and production S3 tuning remain future work.

## Concurrent checkpoint artifact helper

`runtime-checkpoint` provides a bounded concurrent upload helper that uses `StorageBackend`, so it works with both `LocalStorage` and `S3Storage`. Concurrency is bounded via `StorageOperationPolicy.max_concurrent_operations`. This helper uploads artifact bytes and returns artifact metadata including checksum/size, but it is not multipart upload and does not stream artifact bytes through coordinator gRPC.

## Limitations

- Storage backend provides primitives and key mapping.
- Coordinator checkpoint RPCs may still report artifact metadata rather than uploading artifact bytes through coordinator RPCs.
- Real AWS cloud validation, production readiness, HA, and hardening are out of scope for local development.

## Artifact integrity validation

- Artifact integrity validation is backend-neutral and uses the shared `StorageBackend` interface.
- `LocalStorage` and `S3Storage` both expose stored artifact bytes via `StorageBackend::get`.
- Validation should not trust reported metadata blindly; it verifies stored object bytes against:
  - expected existence/readability,
  - expected byte length,
  - supported checksum algorithm,
  - expected checksum digest.
- Commit-time integrity validation is enforced before manifest/marker/latest writes.

## How the backends are used

- `S3Storage` and `LocalStorage` are backend implementations of the same storage abstraction.
- Real worker artifact-byte uploads go through these backends, not only metadata reporting.
- The object key layout is:

```text
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.pending.json
runs/{run_id}/checkpoints/{checkpoint_id}/workers/{worker_id}/{artifact_name}
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.committed.json
runs/{run_id}/checkpoints/{checkpoint_id}/COMMITTED
runs/{run_id}/LATEST
```

## Dataset prefetching

`runtime-dataset` includes a bounded prefetch helper for dataset shard objects that reads through the existing `StorageBackend` abstraction. It is backend-neutral, so it works with local storage, memory-backed test storage, and S3-backed storage implementations.

Highlights:
- Accepts `DatasetPrefetchItem` inputs containing shard id, storage key, and optional expected size and checksum metadata.
- Uses `StorageOperationPolicy.max_concurrent_operations` to bound in-flight prefetches.
- Returns per-item success/failure results (partial failures do not panic or abort the whole batch).
- Validates expected size and checksum when provided.

Scope note: this is a prototype for worker-side loading workflows. It is **not** a production dataset cache, does **not** implement cache-aware placement, and does **not** include distributed prefetch scheduling.
