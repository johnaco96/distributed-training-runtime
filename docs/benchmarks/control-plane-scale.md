# Coordinator scale baseline (measured)

**Environment:** WSL2 Ubuntu 26.04 on Windows 11, 24 logical CPUs, 7 GB RAM for the VM,
repo + benchmarks on the WSL2 ext4 filesystem, loopback networking. All numbers are
**single-machine, loopback, simulated (protocol-level) workers** — no training compute,
no real network. Treat them as an upper bound on control-plane behavior, not a cluster
result. Binaries: `cargo build --release`; profiles: `cargo build --profile perf` +
`perf record -F 499 -g --call-graph fp`.

Reproduce:

```bash
cargo run --release -p runtime-bench -- logical-workers \
    --workers 5000 --duration-secs 30 --concurrency 1024 \
    --heartbeat-interval-ms 100 --start-coordinator
```

## Load-generator methodology

`runtime-bench logical-workers` runs **every simulated worker as its own
tokio task** on its configured heartbeat cadence, multiplexed over a shared
pool of HTTP/2 channels (`--channels`), with total in-flight RPCs bounded by
`--concurrency`. Offered load is therefore exactly what the flags specify:
`workers × ticks × 2` RPCs per run (Heartbeat + GetShardAssignment per
tick), and a run's op count matching that product proves no ticks were
dropped. Latencies are recorded per RPC from each worker task, so the
percentiles reflect the coordinator under genuine concurrent load rather
than a single serialized request stream.

## Scale sweep (before the heartbeat fix)

30 s runs, 1 s heartbeat cadence, 2 RPCs per tick per worker
(Heartbeat + GetShardAssignment). "ops == offered" means no ticks were missed.

| Workers | Concurrency | Ops (all successful) | p50 | p90 | p99 | Peak RSS |
|---|---|---|---|---|---|---|
| 500 | 256 | 30,500 (== offered) | 2.9 ms | 4.7 ms | 6.6 ms | 27 MB |
| 1000 | 256 | 61,000 (== offered) | 2.8 ms | 4.9 ms | 7.1 ms | 27 MB |
| 2000 | 512 | 122,000 (== offered) | 6.6 ms | 12.4 ms | 17.7 ms | 31 MB |
| 5000 | 512 | 305,000 (== offered) | 11.7 ms | 21.8 ms | 31.0 ms | 34 MB |
| 5000 @ 100 ms cadence | 1024 | 1,220,668 (**~40k RPC/s, saturated**) | 24.2 ms | 43.6 ms | 62.3 ms | 50 MB |

Zero failed RPCs in every run. The 100 ms-cadence run is the interesting one:
offered load is ~100k RPC/s and the coordinator could only clear ~40k.

## Hotspot found and fixed

`perf` under the saturating config showed the top two self-time functions were
both O(workers) scans executed on **every heartbeat**:

- `CoordinatorState::worker_counts` (5.85%) — full roster scan to refresh the
  active/stale Prometheus gauges, called from the heartbeat handler.
- `refresh_active_worker_count` (4.28%) — second full scan inside
  `record_heartbeat`.

That is O(workers²) work per heartbeat wave. Since a steady-state heartbeat
cannot change any count, both scans now run only when the heartbeat actually
flips the worker's state; the periodic reconcile task refreshes the gauges
from a global recount so silently-dead workers still surface.

## After the fix (same configs)

| Config | Before | After |
|---|---|---|
| 5000 workers, 1 s cadence — p50 / p99 | 11.7 / 31.0 ms | **4.7 / 12.7 ms** |
| 5000 workers, 100 ms cadence — throughput | ~40.3k RPC/s | **~98.7k RPC/s (full offered load)** |
| 5000 workers, 100 ms cadence — p50 / p99 | 24.2 / 62.3 ms | **9.6 / 24.9 ms** |
| Failed RPCs (all runs) | 0 | 0 |
| Peak RSS at 100 ms cadence | 50 MB | 56 MB |

The coordinator now sustains ~99k RPC/s on loopback — 5000 workers
heartbeating every 100 ms with zero failures — with room to spare at the
1 s cadence real workers use (SDK default is 300 s process-group timeout and
seconds-scale heartbeats, so 5000 real workers generate ~10k RPC/s).

## Known next targets (profiled, not yet fixed)

Post-fix profile is dominated by allocation churn, not algorithms:
`String::clone` 4.8%, `ShardRecord::clone` 2.6% plus matching malloc/free —
`get_shard_assignment` clones every assigned shard record (URI strings and
all) per call. Candidates: return `Arc<ShardRecord>` slices or serialize
straight from the map. Also unmeasured here: SQLite `persist_worker` runs on
every heartbeat (I/O, invisible to CPU profiles); batching heartbeat
durability (votes stay synchronous) is the likely next latency win.

## Caveats, honestly

- Loopback: no network latency/jitter/packet loss; a real cluster adds RTTs.
- Simulated workers: no gRPC-per-worker connection churn (channels are
  pooled), no TLS, no auth tokens in these runs.
- 7 GB WSL2 VM: RSS numbers are real but small; SQLite lives on ext4.
- Failure-injection runs (`--failure-rate 0.1 --recover-failed-workers`)
  completed with 0 failed RPCs from the survivors' perspective; deeper chaos
  coverage lives in `python/tests/test_chaos.py`.
