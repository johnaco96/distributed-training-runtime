# Retry and idempotency notes (Phase 17B)

## Scope

This document describes retry/idempotency behavior currently implemented without changing `runtime.v1` protobuf wire definitions.

## Coordinator idempotent operations

The coordinator currently supports idempotent replay (same key, same logical operation) for:

- `RegisterRun`
- `RegisterWorker` (scoped by `run_id`)
- `RegisterDataset` (scoped by `run_id`)
- `BeginCheckpoint` (scoped by `run_id`)
- `ReportCheckpointArtifact` (scoped by `run_id` + `checkpoint_id`)

`CommitCheckpoint` remains safe for repeated calls on an already-committed checkpoint, but does not currently persist a distinct commit idempotency map.

## Idempotency key validation

Coordinator service-level validation applies to mutating operations listed above plus `CommitCheckpoint`:

- Empty key is still allowed to preserve existing semantics.
- Non-empty key must be <= 256 bytes.
- Overlong key is rejected with `INVALID_ARGUMENT`.

## Retry guidance

### Safe to retry

Clients may retry transient gRPC failures (`UNAVAILABLE`, `DEADLINE_EXCEEDED`) for supported idempotent operations when using stable idempotency keys.

### Not safe to blindly retry

Mutating operations without idempotency keys should not be retried blindly because duplicates may be created.

## Current retry implementation status

- Storage retries: implemented in `runtime-storage` policy/retry helpers (Phase 13).
- Coordinator RPC automatic client retries: not globally enabled by default.
- Benchmarks: send deterministic idempotency keys per logical benchmark run to avoid accidental cross-run key collisions.

## Limitations and future work

- No protocol version negotiation yet.
- No exactly-once/distributed transaction guarantees.
- Lease hardening and checkpoint cleanup remain pending follow-up work.
