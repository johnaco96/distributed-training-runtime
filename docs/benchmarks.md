# Benchmarks

All numbers below were captured against the current codebase on a local development machine (WSL2, Ubuntu 22.04, Hyper-V). Numbers are from the post-hardening build that includes audit logging, distributed two-phase checkpoint commit, PostgreSQL HA, and TLS/mTLS support. They are not production cluster numbers.

---

## Measured results (2026-07-03, local loopback build)

Captured with `target/release/runtime-bench` on the security-hardened build
(OIDC/JWT + RBAC + SIEM audit) on the local dev machine. Raw JSON is committed
under [`observability/benchmarks/`](../observability/benchmarks/). These are
single-node, simulated-worker numbers — not real GPU-cluster results.

**Control plane — `logical-workers`, 500 workers, 30 s, checkpoints every 10 s**
(→ `observability/benchmarks/logical-workers-500w-30s.json`):

| Metric | Value |
|---|---|
| Control-plane RPCs | 24,515 ops (register + heartbeat + shard-assign + checkpoint begin/report/commit + restore) |
| Success rate | 99.98% (6 failures / 24,515) |
| Sustained rate | ~804 ops/s (paced by the 1 s heartbeat interval, not saturation) |
| RPC latency | p50 **0.28 ms**, p90 0.37 ms, p99 **0.48 ms** (per-request round-trip, one request in flight at a time) |
| Heartbeat latency | p50 0.19 ms / p99 0.32 ms |
| Checkpoint commit latency | p50 0.28 ms / p99 0.33 ms |

**Checkpoint write — 32 artifacts × 1 MiB**
(→ `observability/benchmarks/checkpoint-write-32x1MiB.json`): 32 MiB written at
**629 MB/s** aggregate, 100% success.

**Data plane — `sharded-data-load`, 128 shards × 1 MiB, 16 workers**
(→ `observability/benchmarks/sharded-data-load-128x1MiB.json`): 128 MiB read via
coordinator-assigned shard URIs at **~5.75 GB/s** aggregate local-filesystem
read throughput. Note on the raw JSON: its `failure_count` (128) counts the
per-worker end-of-stream assignment sentinels, not read errors — every shard
byte was read and verified; the accounting simply treats "no more shards"
responses as non-success ops.

**Resilience (gated ops tests, this run):**

| Test | Scenario | Result |
|---|---|---|
| `test_failover.py` | Hard-kill coordinator + restart on same SQLite store | Run state + idempotency rehydrated; same `run_id` returned |
| `test_chaos.py` | 40 workers, 30% preemption **with recovery**, 20 s, checkpoints | Coordinator survived; fleet recovered; healthy success ratio |
| `test_soak.py` | 100 workers, 60 s sustained, checkpoints | >99% success, p99 < 2 s, no degradation |

See [observability-ops-tests.md](observability-ops-tests.md) to reproduce.

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

For recorded results see the committed JSON under
[`observability/benchmarks/`](../observability/benchmarks/) and the measured
table above. The generator runs each logical worker as its own concurrent
task over pooled HTTP/2 channels; `--concurrency` bounds total in-flight
RPCs.

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
| Machine | Local dev machine, loopback (see raw JSON metadata) |
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
