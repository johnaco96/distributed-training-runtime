# Operational tests: failover, chaos, and soak

Three gated tests exercise the coordinator's operational resilience against the
**real** release binaries (no mocks). They are skipped by default; each is
enabled by its own environment variable so they never slow the normal test run.

Build the release binaries first:

```bash
cargo build --release -p runtime-coordinator -p runtime-bench
```

Shared helpers live in `python/tests/ops_common.py` (binary discovery, coordinator
spawn/kill, and a `runtime-bench logical-workers` wrapper).

## Failover — `test_failover.py`

Verifies that a **hard crash** (SIGKILL-equivalent, no graceful shutdown) of the
coordinator followed by a restart on the **same metadata store** transparently
rehydrates durable run state:

- The run survives the crash (same `run_id`, same lifecycle state).
- Idempotency is restored from the store: re-registering with the same key
  returns the original `run_id` instead of creating a duplicate run.

```bash
DTRUNTIME_RUN_FAILOVER_TESTS=1 python -m pytest python/tests/test_failover.py -q
```

## Chaos / preemption — `test_chaos.py`

Drives `runtime-bench logical-workers` with worker-failure injection
(`--failure-rate 0.3 --recover-failed-workers`) while checkpoints flow, then
asserts the coordinator stayed healthy and the fleet recovered (full worker
lifecycle exercised, healthy success ratio through the churn).

```bash
DTRUNTIME_RUN_CHAOS_TESTS=1 python -m pytest python/tests/test_chaos.py -q
# Tunables: DTRUNTIME_CHAOS_WORKERS (40), DTRUNTIME_CHAOS_DURATION_SECS (20)
```

## Soak — `test_soak.py`

Sustains a worker fleet against the coordinator for an extended window and
asserts non-degrading behavior (>99% success, bounded p99, ran the full
duration). The default is a short, CI-friendly run; set a long duration for a
real soak:

```bash
DTRUNTIME_RUN_SOAK_TESTS=1 python -m pytest python/tests/test_soak.py -q
# Multi-hour soak:
DTRUNTIME_RUN_SOAK_TESTS=1 DTRUNTIME_SOAK_DURATION_SECS=3600 \
  DTRUNTIME_SOAK_WORKERS=500 python -m pytest python/tests/test_soak.py -q -s
```

## CI

`.github/workflows/ops-tests.yml` runs failover + short chaos + short soak on a
schedule (and on demand via `workflow_dispatch`). They are intentionally kept
out of the per-PR CI because they build release binaries and run for minutes.

## Reproducing the benchmark numbers

The [benchmarks](benchmarks.md) "Measured results" section was produced with:

```bash
target/release/runtime-bench logical-workers --start-coordinator \
  --workers 500 --duration-secs 30 --concurrency 128 \
  --heartbeat-interval-ms 1000 --checkpoint-every-secs 10 \
  --out-json observability/benchmarks/logical-workers-500w-30s.json

target/release/runtime-bench checkpoint-write --artifacts 32 \
  --payload-bytes 1048576 --concurrency 16 \
  --out-json observability/benchmarks/checkpoint-write-32x1MiB.json

target/release/runtime-bench sharded-data-load --num-shards 128 \
  --shard-size-bytes 1048576 --worker-count 16 --concurrency 16 \
  --out-json observability/benchmarks/sharded-data-load-128x1MiB.json
```
