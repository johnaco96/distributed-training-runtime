# Benchmarks (Phase 14A)

Phase 14A adds a **local-only** benchmark harness via `runtime-bench`.

## Scope
- Local dataset read benchmark.
- Local checkpoint write benchmark.
- Local checkpoint restore benchmark (manifest restore latency).

## Commands
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

## Limitations (current)
- Local-only benchmark paths.
- No cloud S3 benchmark included yet.
- No coordinator registration/heartbeat RPC overhead benchmark yet (planned for Phase 14B).
- No production/distributed scaling claims.

Do not commit generated benchmark output files.
