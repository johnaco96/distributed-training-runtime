# Metadata Persistence

The coordinator supports optional SQLite-backed metadata persistence via `--metadata-db <path>`.

## Selected backend
- SQLite is the default metadata backend for single-node persistence.
- PostgreSQL is supported for multi-replica HA deployments via advisory lock leader election.

## Persisted metadata
- runs
- workers
- datasets
- shards
- checkpoint records
- checkpoint artifact metadata
- committed checkpoint state
- latest committed checkpoint pointer

## Restart/recovery behavior
- On restart with the same DB path, `GetLatestCheckpoint` can reload the latest committed checkpoint metadata.
- Partial/uncommitted checkpoints do not replace the latest committed checkpoint.

## Running with persistence
```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --metadata-db .dtruntime-demo/coordinator.db
```

If `--metadata-db` is omitted, coordinator stays in-memory only.

## Current limitations
- Checkpoint artifact bytes are not stored in SQLite (metadata only).
- SQLite stores coordinator metadata only.
- Worker liveness remains runtime state and is recalculated/refreshed after restart.

## Coordinator CLI help

```bash
cargo run -p runtime-coordinator -- --help
```

The coordinator supports:
- `--addr <ADDR>` to override bind address
- `--metadata-db <PATH>` to enable SQLite metadata persistence
- `--help` / `-h` to print usage

When `--metadata-db` is omitted, coordinator metadata remains in-memory only.

## Manual restart validation (optional)

Terminal 1:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --metadata-db .dtruntime-demo/coordinator.db
```

Then run a client/demo flow that commits a checkpoint, stop the coordinator, and restart with the same command:

```bash
cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --metadata-db .dtruntime-demo/coordinator.db
```

After restart, verify `GetLatestCheckpoint` can restore the latest committed checkpoint metadata.
