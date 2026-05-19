# Scaling Tests Runbook (Phase 16C)

This document is the Phase 16 runbook for `runtime-bench` scaling tests.

## Scope

Phase 16 measures **local coordinator control-plane RPC behavior** as worker counts increase.

- Real local worker-count benchmarks: `1,2,4,8,16`
- Simulated larger-count benchmarks: `32,64,128` (logical workers)
- Focus metrics: failure rate and latency percentiles (`p50`, `p90`, `p99`)

This is not a cloud benchmark, not Kubernetes benchmarking, and not multi-machine distributed training validation.

## Prerequisite

Verify CLI availability and options:

```bash
cargo run -p runtime-bench -- --help
```

## Local coordinator scaling benchmark (real local counts)

Run the Phase 16 baseline worker counts:

```bash
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2,4,8,16 --heartbeats 2
```

## Simulated larger-count benchmark

Run the simulated default larger counts:

```bash
cargo run -p runtime-bench -- scaling-workers --simulate-large --heartbeats 1
```

Run explicit simulated counts:

```bash
cargo run -p runtime-bench -- scaling-workers --simulate-large --workers-list 32,64,128 --heartbeats 1
```

## Output files

Write CSV output:

```bash
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2,4,8,16 --heartbeats 2 --out-csv scaling-results.csv
```

Write JSON output:

```bash
cargo run -p runtime-bench -- scaling-workers --workers-list 1,2,4,8,16 --heartbeats 2 --out-json scaling-results.json
```

Cleanup guidance:

- Do **not** commit generated benchmark output files (`*.csv`, `*.json`) to git.

## Expected output shape

Console summary (one line per worker count):

- `benchmark=<name>`
- `ops=<operation_count>`
- `success=<success_count>`
- `fail=<failure_count>`
- `failure_rate=<ratio>`
- `bytes=<total_bytes>`
- `elapsed_ms=<total wall time>`
- `mb_per_sec=<throughput>`
- `p50=<p50_latency_ms>`
- `p90=<p90_latency_ms>`
- `p99=<p99_latency_ms>`
- `retries=<retry_count>`

CSV columns:

`benchmark,operation_count,success_count,failure_count,failure_rate,total_bytes,elapsed_ms,mb_per_sec,p50_latency_ms,p90_latency_ms,p99_latency_ms,retry_count`

JSON output contains the same fields per summary entry.

## Metric interpretation

- `benchmark`: benchmark name (`worker-rpc-<n>-workers` or `simulated-worker-rpc-<n>-workers`).
- `operation_count` / `ops`: total attempted control-plane operations in the run.
- `success_count` / `success`: operations that completed successfully.
- `failure_count` / `fail`: operations that failed.
- `failure_rate`: `failure_count / operation_count`.
- `elapsed_ms`: wall-clock elapsed runtime in milliseconds.
- `p50 latency`: median operation latency in milliseconds.
- `p90 latency`: 90th percentile operation latency in milliseconds.
- `p99 latency`: 99th percentile operation latency in milliseconds.
- `retry_count`: retries reported by the benchmark path (currently expected to be `0` for this control-plane flow unless retry behavior changes).
- `total_bytes` / MB/s behavior: for `worker-rpc` and `scaling-workers`, these are control-plane RPCs and data-plane transfer is not measured, so `total_bytes=0` and `mb_per_sec=0.0` are expected.

For scaling interpretation, prioritize `failure_rate`, `p50`, `p90`, and `p99`.

## Honest scaling language requirements

When reporting results:

- `1,2,4,8,16` are **local benchmark counts**.
- `32,64,128` simulated mode uses **logical worker IDs** against a local coordinator.
- Simulated mode is **not** separate physical workers.
- Simulated mode is **not** multi-machine distributed training.
- Do **not** claim 100s/1000s of worker support until those exact counts are benchmarked and results are published.
- Every result report should include:
  - machine specs (CPU/RAM)
  - OS
  - commit SHA
  - exact command
  - mode (`local` or `simulated`)

## Result reporting template

Use this template for benchmark reports (leave as placeholders until you have real run output):

| Commit | Machine | Mode | Workers | Heartbeats | Ops | Failure rate | p50 ms | p90 ms | p99 ms | Notes |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| `<sha>` | `<cpu/ram>` | `<local\|simulated>` | `<n>` | `<n>` | `<n>` | `<ratio>` | `<ms>` | `<ms>` | `<ms>` | `<notes>` |
