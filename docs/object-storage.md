# Phase 12 Object Storage (12A + 12B)

Phase 12A added an **S3-compatible storage backend foundation** in `runtime-storage`.
Phase 12B wires that backend into **checkpoint store write/restore tests** and adds an **env-gated MinIO validation path**.

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

## Running env-gated MinIO tests (Phase 12B)

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

Real AWS S3 validation is **not required** for Phase 12B.

## Phase 13A reliability guardrails foundation

Phase 13A/13D provide a reusable `StorageOperationPolicy` in `runtime-storage` for async-capable storage operations. In `S3Storage`, timeout/retry/backpressure now cover `put`, `get`, `exists` (head), `delete`, and `list_prefix`.

Default policy values:

- `operation_timeout`: 30 seconds
- `max_retries`: 3
- `retry_initial_backoff`: 100 milliseconds
- `retry_max_backoff`: 2 seconds
- `max_concurrent_operations`: 16

This adds timeout/retry/backpressure primitives:

- timeout guardrail using `tokio::time::timeout` for async storage calls,
- retry helper with capped exponential backoff for retryable storage failures,
- concurrency limiter based on a semaphore to enforce max in-flight operations.

Scope note:

- This is a **foundation** for Phase 13. It does **not** claim full concurrent checkpoint upload completion, dataset prefetching completion, benchmark harness completion, or production S3 tuning completion.
- MinIO/AWS paths can use these same policies in later Phase 13 work.
- Normal unit/integration tests remain MinIO-optional and do not require AWS credentials.



## Phase 13D scope update

Phase 13D extends policy coverage to additional S3 operations without a full async rewrite:

- Timeout/retry/backpressure currently apply to `S3Storage::put`, `get`, `exists`, `delete`, and `list_prefix`.
- `runtime-storage` now includes small backend-neutral batch helpers: `put_many`, `get_many`, and `exists_many`, each with bounded concurrency via `StorageOperationPolicy.max_concurrent_operations` and per-item result reporting.
- This remains foundation-level work: multipart upload, coordinator gRPC artifact-byte streaming, production S3 tuning, and benchmark harness work are still future phases.

## Phase 13B concurrent checkpoint artifact helper

`runtime-checkpoint` now provides a bounded concurrent upload helper that uses `StorageBackend`, so it works with both `LocalStorage` and `S3Storage`. Concurrency is bounded via `StorageOperationPolicy.max_concurrent_operations`. This helper uploads artifact bytes and returns artifact metadata including checksum/size, but it is not multipart upload and does not stream artifact bytes through coordinator gRPC.

## Limitations

- Phase 12A provides storage backend primitives and key mapping.
- Phase 12B wires S3-compatible storage into checkpoint storage tests and adds local env-gated MinIO validation.
- Coordinator checkpoint RPCs may still report artifact metadata rather than uploading artifact bytes through coordinator RPCs.
- Real AWS cloud validation, production readiness, HA, and hardening are out of scope.

## Artifact integrity validation (Phase 12.5A)

- Artifact integrity validation is backend-neutral and uses the shared `StorageBackend` interface.
- `LocalStorage` and `S3Storage` both expose stored artifact bytes via `StorageBackend::get`.
- Validation should not trust reported metadata blindly; it verifies stored object bytes against:
  - expected existence/readability,
  - expected byte length,
  - supported checksum algorithm,
  - expected checksum digest.
- Phase 12.5A adds reusable helpers for this validation layer.
- Phase 12.5B now enforces commit-time integrity validation before manifest/marker/latest writes.

## How Phase 12.5 uses object storage

- `S3Storage` and `LocalStorage` are backend implementations of the same storage abstraction.
- Phase 12.5 will use these backends for **real worker artifact-byte uploads**, not only metadata reporting.
- The object key layout remains:

```text
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.pending.json
runs/{run_id}/checkpoints/{checkpoint_id}/workers/{worker_id}/{artifact_name}
runs/{run_id}/checkpoints/{checkpoint_id}/manifest.committed.json
runs/{run_id}/checkpoints/{checkpoint_id}/COMMITTED
runs/{run_id}/LATEST
```

## Phase 13C: Dataset prefetching prototype

`runtime-dataset` now includes a bounded prefetch helper for dataset shard objects that reads through the existing `StorageBackend` abstraction. It is intentionally small and backend-neutral, so it works with local storage, memory-backed test storage, and S3-backed storage implementations without introducing a separate cache layer.

Highlights:
- Accepts `DatasetPrefetchItem` inputs containing shard id, storage key, and optional expected size and checksum metadata.
- Uses `StorageOperationPolicy.max_concurrent_operations` to bound in-flight prefetches.
- Returns per-item success/failure results (partial failures do not panic or abort the whole batch).
- Validates expected size and checksum when provided.

Scope note: this is a prototype for worker-side loading workflows. It is **not** a production dataset cache, does **not** implement cache-aware placement, and does **not** include distributed prefetch scheduling.

