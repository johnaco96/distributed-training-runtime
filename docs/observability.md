# Observability (Phase 15A/15B in-process)

Phase 15A adds a **local, dependency-light observability foundation** for the runtime coordinator.

## Scope in Phase 15A

Implemented in this phase:

- structured logging initialization helper (`init_tracing_from_env`)
- coordinator startup/shutdown logging
- reusable in-process metrics primitives (`RuntimeMetrics` + snapshot)

Phase 15B wiring now updates metrics from coordinator execution paths. Still not implemented:

- Prometheus endpoint
- Grafana dashboards
- external metrics server/exporter

## Logging

The coordinator initializes tracing via `init_tracing_from_env()`.

- Uses `RUST_LOG` when set.
- Defaults to `info` level if `RUST_LOG` is not set.
- Uses compact structured-ish log output for local development.
- Uses `try_init()` so repeated initialization in tests does not panic.

Example:

```bash
RUST_LOG=debug cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051
```

## In-process metrics currently tracked

`RuntimeMetrics` tracks these counters/gauges:

- `active_workers` (gauge)
- `stale_workers` (gauge)
- `checkpoint_writes` (counter)
- `failed_writes` (counter)
- `shard_assignment_count` (counter)
- `storage_bytes_read` (counter)
- `storage_bytes_written` (counter)

Metrics are currently available as **in-process snapshots** only; they are not exported over HTTP and not integrated with Prometheus yet.

## Phase 15B metric semantics

- `active_workers` is refreshed from run worker state during worker registration, heartbeat, and run status reconciliation.
- `stale_workers` is refreshed alongside `active_workers` after reconciliation/status reads.
- `shard_assignment_count` increments by the number of shard assignments returned by successful `GetShardAssignment` calls.
- `checkpoint_writes` increments on successful `CommitCheckpoint` RPC outcomes.
- `failed_writes` increments on failed `CommitCheckpoint` RPC outcomes.
- `storage_bytes_written` increments by `artifact_report.size_bytes` on successful `ReportCheckpointArtifact` outcomes.
- `storage_bytes_read` remains available but is not yet wired to backend byte reads.

Current limitation: storage throughput counters reflect coordinator-observed reported artifact bytes, not physical backend transfer accounting.

## Later optional work

Future Phase 15 work may add Prometheus/Grafana integration once metric wiring and export strategy are finalized.
