# Observability (Phase 15A foundation)

Phase 15A adds a **local, dependency-light observability foundation** for the runtime coordinator.

## Scope in Phase 15A

Implemented in this phase:

- structured logging initialization helper (`init_tracing_from_env`)
- coordinator startup/shutdown logging
- reusable in-process metrics primitives (`RuntimeMetrics` + snapshot)

Not implemented yet in this phase:

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

## Later optional work

Future Phase 15 work may add Prometheus/Grafana integration once metric wiring and export strategy are finalized.
