# Benchmarks (Phase 14)

Phase 14A adds a **local-only** benchmark harness via `runtime-bench`.

## Scope
- Local dataset read benchmark.
- Local checkpoint write benchmark.
- Local checkpoint restore benchmark (manifest restore latency).
- Local coordinator RPC overhead benchmark for run/worker registration and worker heartbeats.

## Commands
- `cargo run -p runtime-bench -- worker-rpc --workers 8 --heartbeats 10`
- `cargo run -p runtime-bench -- worker-rpc --workers 2 --heartbeats 2 --addr 127.0.0.1:50051 --start-coordinator false`
- `cargo run -p runtime-bench -- dataset-read --items 128 --payload-bytes 65536 --concurrency 16`
- `cargo run -p runtime-bench -- checkpoint-write --artifacts 16 --payload-bytes 1048576 --concurrency 16`
- `cargo run -p runtime-bench -- checkpoint-restore --iterations 100 --artifacts 16 --payload-bytes 1048576 --concurrency 16`

## Metrics
- **MB/s**: total transferred bytes divided by elapsed benchmark wall time.
- **p50/p90/p99 latency**: percentile latency over recorded operation samples.
- **failure rate**: failure_count / operation_count.
- **retry count**: currently reported as zero in Phase 14A unless future integrations expose retries.

## Output files
- CSV: `--out-csv <path>`
- JSON: `--out-json <path>`

CSV columns:
`benchmark,operation_count,success_count,failure_count,failure_rate,total_bytes,elapsed_ms,mb_per_sec,p50_latency_ms,p90_latency_ms,p99_latency_ms,retry_count`

## Worker RPC benchmark
`worker-rpc` performs one `RegisterRun`, then `RegisterWorker` for N workers, and `Heartbeat` for each worker repeated M times against a coordinator gRPC endpoint.

Defaults:
- `--workers 8`
- `--heartbeats 10`
- `--start-coordinator true` (starts `runtime-coordinator` locally unless `--addr` and `--start-coordinator false` are used)

It reports operation count, success/failure, failure rate, elapsed time, p50/p90/p99 latency, and retry count (currently zero). Throughput fields remain zero for this RPC-control-plane benchmark.

## Limitations (current)
- Local-only benchmark paths.
- No cloud S3 benchmark included yet.
- Local-first benchmark; remote coordinators are only measured when explicitly provided via `--addr`.
- Not a distributed-scale benchmark; do not interpret as hundreds/thousands of workers performance.
- No production networking/performance claims.
- No production/distributed scaling claims.

Do not commit generated benchmark output files.
