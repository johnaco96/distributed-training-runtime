# runtime-bench

gRPC load generator for the coordinator. Every subcommand does real work —
real RPCs against a real coordinator, or real file I/O — and reports
latency percentiles (p50/p90/p99), throughput, and failure counts as a
one-line summary plus optional `--out-csv` / `--out-json`.

| Subcommand | What it measures | Driven by |
|---|---|---|
| `logical-workers` | Thousands of concurrent simulated workers running the full protocol (register, heartbeat cadence, shard assignment, optional checkpoint 2PC, failure injection) over pooled HTTP/2 channels | `docs/benchmarks/phase1-baseline.md`, `scripts/run_flamegraph.py`, soak/chaos tests (`python/tests/test_soak.py`, `test_chaos.py`) |
| `scaling-workers` | The same worker RPC flow swept across a list of worker counts | `docs/quickstart.md`, `docs/troubleshooting.md` |
| `worker-rpc` | Single-flow worker RPC latency (register + N heartbeats) | `docs/quickstart.md` |
| `sharded-data-load` | End-to-end data plane: shard assignment → real file reads → progress reports | manual |
| `dataset-read` | Concurrent shard reads through the storage backend | manual |
| `checkpoint-write` / `checkpoint-restore` | Artifact write/restore throughput through `runtime-checkpoint` + `runtime-storage` | manual |

Start with:

```bash
cargo run --release -p runtime-bench -- logical-workers \
    --workers 5000 --duration-secs 30 --concurrency 1024 \
    --heartbeat-interval-ms 100 --start-coordinator
```
