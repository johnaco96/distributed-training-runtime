# Distributed Training Runtime

A Rust and Python runtime for distributed ML dataset loading, checkpointing, worker coordination, and recovery.

## Goal

Provide a simple Python API for ML training jobs while a Rust backend coordinates:

- distributed dataset registration
- sharded dataset access
- async I/O
- fault-tolerant checkpoint writes
- worker heartbeats
- recovery from node failure
- local and object-storage-backed persistence

## Architecture

Python SDK talks to a Rust coordinator over gRPC.

```text
Python Training Script
        |
        v
Python SDK
        |
        v
gRPC
        |
        v
Rust Coordinator
        |
        v
Storage / Workers / Checkpoints

## Project status and roadmap

Completed through **Phase 12.5 — Checkpoint Artifact Data Plane foundation**:

- [x] Add object storage abstraction implementation.
- [x] Support MinIO locally.
- [x] Add S3-compatible checkpoint writes.
- [x] Add S3-compatible checkpoint restore.
- [x] Test local MinIO before real cloud storage.
- [x] Document object key layout.

### Phase 12.5 — Checkpoint Artifact Data Plane

Phase 12.5A foundation status:

- [x] Add reusable checksum algorithm abstraction (`sha256` currently supported).
- [x] Add reusable backend-neutral artifact integrity validation helpers.
- [x] Enforce artifact validation in commit path (Phase 12.5B).

- Wire real checkpoint artifact bytes into the checkpoint flow.
- Support worker-side artifact upload to the configured storage backend.
- Support LocalStorage and S3Storage/MinIO backends.
- Ensure ReportCheckpointArtifact metadata matches the actual stored object.
- Validate:
  - artifact exists
  - size matches
  - checksum matches
  - checksum algorithm matches
  - URI/key is backend-neutral
- Reject CommitCheckpoint if required artifacts are missing or invalid.
- Add LocalStorage artifact-byte roundtrip tests.
- Add MinIO artifact-byte roundtrip tests.
- Document artifact metadata vs artifact bytes clearly.

### Phase 13 — Performance and Async I/O (foundation complete)

- Improve async read/write paths.
- Add batching where useful.
- Add concurrent checkpoint uploads.
- Add dataset prefetching prototype.
- Add backpressure limits.
- Add timeout and retry behavior.

#### Phase 13A — Timeout/Retry/Backpressure foundation (complete)

- [x] Add reusable storage operation policy defaults.
- [x] Add retry helper with capped exponential backoff and retryability guard.
- [x] Add async timeout helper for async-capable storage operations.
- [x] Add concurrency/backpressure primitive for storage operations.
- [x] Wire bounded concurrent checkpoint artifact upload helper (Phase 13B).
- [x] Add dataset prefetching prototype (Phase 13C complete).
- [ ] Add full benchmark harness (later Phase 13 work).

Phase 13 should optimize the **real artifact byte path introduced in Phase 12.5**, not only metadata reporting.

Coordinator artifact-byte upload via control-plane RPC remains intentionally out of scope until Phase 12.5 is implemented.

- Phase 13C dataset prefetching prototype: complete.
- Benchmark harness: pending.
- Full async read/write rewrite: pending.
- Production dataset prefetch/cache-aware placement: pending.


#### Phase 13D — Storage async/read-write cleanup and status cleanup (complete)

- [x] Extend `StorageOperationPolicy` coverage in `S3Storage` beyond put (get/exists/delete/list_prefix).
- [x] Add minimal backend-neutral batch helpers in `runtime-storage` (`put_many`, `get_many`, `exists_many`).
- [x] Keep `StorageBackend` API compatibility and LocalStorage behavior.
- [x] Clean roadmap/documentation inconsistencies for Phase 13 status.

Next planned step: **Phase 14 benchmark harness**.


### Phase 14 — Benchmark Harness (started)

- [x] Phase 13 complete.
- [x] Phase 14A local benchmark CLI and local benchmarks (`dataset-read`, `checkpoint-write`, `checkpoint-restore`) complete.
- [ ] Phase 14B worker registration/heartbeat overhead benchmark pending.
- Local benchmark scope only; no cloud/distributed performance claims yet.
