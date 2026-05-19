# Phase 11 Metadata Persistence (MVP)

The coordinator now supports optional SQLite-backed metadata persistence via `--metadata-db <path>`.

## Why SQLite
- Structured relational metadata (runs, workers, datasets, shards, checkpoints, artifacts).
- Easy local inspection and testing.
- Good fit for non-HA single-node coordinator MVP.

## Persisted metadata
- runs
- workers
- datasets and shards
- checkpoint records
- checkpoint artifact metadata
- latest committed checkpoint pointer

## Not persisted in this phase
- checkpoint artifact bytes (only metadata is stored)
- distributed/replicated coordinator state
- production HA behavior

## Running with persistence
```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --metadata-db .dtruntime-demo/coordinator.db
```

If `--metadata-db` is omitted, coordinator runs fully in-memory as before.

## Restart recovery behavior
On restart with the same DB path, `GetLatestCheckpoint` can return the latest committed checkpoint metadata (including artifact metadata). Uncommitted checkpoints are not promoted to latest.

## Limitations
- Worker liveness is still runtime state and recalculated after restart.
- SQLite metadata is local-node only and not replicated.
- S3/MinIO integration is deferred to later phases.
