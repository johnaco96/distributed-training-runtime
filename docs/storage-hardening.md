# Object-store hardening

The S3 backend (`crates/runtime-storage/src/s3.rs`) is hardened for production
object stores (AWS S3, MinIO, any S3-compatible endpoint). This complements
[production-s3.md](production-s3.md) (deployment) and
[object-storage.md](object-storage.md) (the storage model).

## Error classification (retryable vs permanent)

Every SDK error is classified by `classify_s3_error` rather than blindly
retried:

- **Transport failures** (timeout, dispatch, connection reset) → retryable.
- **Service errors** are classified by S3 error code:
  - retryable: `SlowDown`, `ServiceUnavailable`, `InternalError`,
    `ThrottlingException`, `TooManyRequests`, `RequestTimeout`,
    `RequestTimeTooSkewed`;
  - permanent (never retried): auth/validation errors like `AccessDenied`,
    `InvalidAccessKeyId`, `SignatureDoesNotMatch`, `NoSuchBucket`,
    `InvalidArgument`, and any **unknown** service code (fail fast rather than
    burn the retry budget on an error that will never succeed).
- `NoSuchKey` maps to `NotFound` so callers can distinguish a missing object
  from a real failure.

Before this, all backend errors were treated as retryable IO errors, so a
403/404/validation error paid the full retry budget (4× latency) on every
occurrence.

## Retry backoff with full jitter

The retry loop uses **full jitter** — a uniformly random delay in
`[0, exponential_backoff]` (`retry_backoff_with_jitter`). Full jitter
de-correlates retries across workers that failed at the same instant (a
throttling event, a coordinator blip), avoiding a synchronised retry storm.
The pure helper is unit-tested; the loop seeds it from the wall clock.

## Multipart upload

Objects at or above the multipart threshold (default 16 MiB) upload via
multipart (`multipart_put_async`):

- parts (default 8 MiB, clamped to the S3 5 MiB minimum) upload with **bounded
  concurrency** via the shared operation semaphore;
- each part is retried independently;
- on any failure the multipart upload is **aborted** so partial parts do not
  linger and accrue storage cost.

Multipart lifts the single-`PutObject` 5 GiB ceiling and avoids buffering a
multi-GB checkpoint into one HTTP request. Small objects still use a single
`PutObject` (one request is cheaper).

Tunable via `S3StorageConfig::multipart_threshold_bytes` /
`multipart_part_size_bytes`.

## Server-side encryption

`--s3-sse AES256` (SSE-S3) or `--s3-sse aws:kms` with `--s3-sse-kms-key-id`
(SSE-KMS) applies encryption to every upload, including each multipart create.
Also settable via `DTRUNTIME_S3_SSE` / `DTRUNTIME_S3_SSE_KMS_KEY_ID`.

## Concurrency

A semaphore bounds concurrent S3 operations (default 64,
`--max-s3-concurrent-ops`), so a large checkpoint fan-out does not exhaust the
connection pool or trigger throttling.

## Validation

- Unit tests: retryable-code classification, jitter bounds, backend-error
  retryability (`cargo test -p runtime-storage --features s3`).
- Load + integrity tests against real MinIO
  (`crates/runtime-storage/tests/s3_load.rs`, gated
  `DTRUNTIME_RUN_MINIO_TESTS=1`):
  - 200 objects put+get concurrently across 16 lanes with per-object byte
    verification;
  - a 17 MiB object uploaded via multipart (5 MiB parts) and verified to
    reassemble byte-for-byte.
- The checkpoint MinIO tests (`crates/runtime-checkpoint/tests/s3_minio.rs`)
  exercise the full commit/restore/cleanup path over S3.
