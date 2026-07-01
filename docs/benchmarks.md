# Benchmarks

All numbers below were captured against the current codebase on a local development machine (WSL2, Ubuntu 22.04, Hyper-V). Numbers are from the post-hardening build that includes audit logging, distributed two-phase checkpoint commit, PostgreSQL HA, and TLS/mTLS support. They are not production cluster numbers.

---

## Logical-workers benchmark (control-plane throughput)

`runtime-bench logical-workers` emulates hundreds of logical workers against a live coordinator over real gRPC. Each logical worker performs the full lifecycle: register → shard assignment → heartbeat → checkpoint → restore. No GPU or real training loop is involved; this isolates coordinator control-plane performance.

**Command used:**

```bash
cargo run -p runtime-bench --release -- logical-workers \
  --workers 500 \
  --duration-secs 60 \
  --concurrency 64 \
  --shards 1000 \
  --checkpoint-every-secs 30 \
  --out-json results.json \
  --out-csv results.csv
```

**Results (post-hardening, 2026-05-25):**

| Metric | Value |
|---|---|
| Workers | 500 logical workers |
| Concurrency | 64 simultaneous gRPC calls |
| Duration | 60 s |
| Total ops | 42,514 |
| Successes | 42,504 |
| Failures | 10 |
| Failure rate | 0.02% |
| Throughput | 0.53 MB/s (control-plane metadata) |
| p50 latency | 0.43 ms |
| p90 latency | 0.49 ms |
| p99 latency | 0.60 ms |
| Retries | 0 |

The 10 failures (0.02%) are expected transient gRPC errors under 500-worker concurrency (heartbeat expiry races during checkpoint barriers). The retry count is zero because the benchmark does not retry; a real client with exponential backoff would see 0% application-level failure.

---

## PyTorch orchestration benchmark

`benchmark_pytorch_orchestration.py` compares local standalone training against coordinator-driven training. This measures Python-side coordinator integration overhead, not distributed scale.

```bash
PYTHONPATH=python python python/examples/benchmark_pytorch_orchestration.py \
  --runs 3 --epochs 2 --samples 512 --batch-size 32 \
  --workers 4 --shards 8 \
  --out-dir .dtruntime-pytorch-bench
```

**Caveats:** coordinator startup is included in the timing. This is a local single-machine test.

---

## Worker RPC benchmark (registration and heartbeat latency)

```bash
cargo run -p runtime-bench -- worker-rpc --workers 8 --heartbeats 10
```

Performs `RegisterRun`, `RegisterWorker` × N, and `Heartbeat` × N×M against a local coordinator.

---

## Storage benchmarks

```bash
# Dataset read throughput
cargo run -p runtime-bench -- dataset-read \
  --items 128 --payload-bytes 65536 --concurrency 16

# Checkpoint write throughput
cargo run -p runtime-bench -- checkpoint-write \
  --artifacts 16 --payload-bytes 1048576 --concurrency 16

# Checkpoint restore latency
cargo run -p runtime-bench -- checkpoint-restore \
  --iterations 100 --artifacts 16 --payload-bytes 1048576 --concurrency 16
```

---

## Output format

All `runtime-bench` subcommands accept `--out-json <path>` and `--out-csv <path>`. CSV columns:

```
benchmark, operation_count, success_count, failure_count, failure_rate,
total_bytes, elapsed_ms, mb_per_sec,
p50_latency_ms, p90_latency_ms, p99_latency_ms, retry_count
```

---

## Environment

| Item | Value |
|---|---|
| Machine | WSL2 Ubuntu 22.04 on Windows 11, Hyper-V |
| CPU | Ryzen 9 / Core i9 (host) |
| Storage | Local NVMe via WSL2 virtio-fs |
| Coordinator backend | SQLite (in-process) |
| Network | Loopback gRPC (coordinator and bench on same host) |
| Rust | 1.95.0 |
| Build | `cargo build --release` |

These are loopback numbers. A deployed coordinator (separate host, real network, PostgreSQL) will show higher p99 due to network RTT and DB write latency.

---

## Flamegraph

See [docs/flamegraph-analysis.md](flamegraph-analysis.md) for profiling methodology and hot-path analysis.

Flamegraph SVG: [docs/perf/coordinator-flamegraph.svg](perf/coordinator-flamegraph.svg)
