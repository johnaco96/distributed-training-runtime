#!/usr/bin/env bash
# Async checkpoint profiling script.
#
# Runs the multi-worker async checkpoint benchmark under continuous Linux
# profiling tools, then generates a flamegraph of the coordinator process.
#
# Requirements (install on WSL/Linux):
#   sudo apt-get install linux-perf linux-tools-$(uname -r) sysstat
#   sudo apt-get install git  # for flamegraph scripts
#   git clone https://github.com/brendangregg/FlameGraph /tmp/FlameGraph
#
# Usage:
#   # Terminal 1 — start coordinator with storage root:
#   cargo run -p runtime-coordinator -- \
#     --addr 127.0.0.1:50051 \
#     --storage-root /tmp/dtruntime-storage \
#     --status-addr 127.0.0.1:8080 \
#     --metrics-addr 127.0.0.1:9090
#
#   # Terminal 2 — run profiling sweep:
#   PYTHONPATH=python bash scripts/run_async_checkpoint_profile.sh
#
# Outputs:
#   .dtruntime-results/async-profile-<timestamp>/
#     system/          — lscpu, meminfo, uname, disk layout
#     perf/            — perf.data, flamegraph.svg, perf-stat.txt
#     iostat/          — continuous iostat during each benchmark run
#     scaling/         — per-worker-count JSON results
#     scaling_curve.json — aggregate scaling table

set -euo pipefail

TS="$(date -u +%Y%m%dT%H%M%SZ)"
ROOT=".dtruntime-results/async-profile-${TS}"
COORD_ADDR="${DTRUNTIME_COORDINATOR_ADDR:-127.0.0.1:50051}"
METRICS_ADDR="${DTRUNTIME_METRICS_ADDR:-http://127.0.0.1:9090}"
FLAMEGRAPH_DIR="${FLAMEGRAPH_DIR:-/tmp/FlameGraph}"
WORKER_COUNTS="${WORKER_COUNTS:-1 4 8 16}"
STEPS="${STEPS:-80}"
CHECKPOINT_EVERY="${CHECKPOINT_EVERY:-20}"
CHECKPOINT_BYTES="${CHECKPOINT_BYTES:-$((16*1024*1024))}"

mkdir -p "$ROOT/system" "$ROOT/perf" "$ROOT/iostat" "$ROOT/scaling"

record_unavailable() { echo "unavailable: $2" > "$1"; }

# ── System snapshot ──────────────────────────────────────────────────────────
echo "[profile] collecting system snapshot..."
uname -a > "$ROOT/system/uname.txt" 2>/dev/null || record_unavailable "$ROOT/system/uname.txt" "uname"
lscpu > "$ROOT/system/lscpu.txt" 2>/dev/null || record_unavailable "$ROOT/system/lscpu.txt" "lscpu"
cat /proc/meminfo > "$ROOT/system/meminfo.txt" 2>/dev/null || record_unavailable "$ROOT/system/meminfo.txt" "/proc/meminfo"
df -h > "$ROOT/system/df.txt" 2>/dev/null || record_unavailable "$ROOT/system/df.txt" "df"
cat /proc/cpuinfo | grep -E "model name|cpu MHz" | sort -u > "$ROOT/system/cpuinfo_summary.txt" 2>/dev/null || true

# ── Find coordinator PID ─────────────────────────────────────────────────────
COORD_PID=""
COORD_PID="$(pgrep -f 'runtime-coordinator' | head -1 || true)"
if [[ -z "$COORD_PID" ]]; then
    echo "[profile] WARNING: runtime-coordinator process not found — skipping perf record"
    echo "[profile] Start the coordinator first: cargo run -p runtime-coordinator -- --addr 127.0.0.1:50051 --storage-root /tmp/dtruntime-storage"
    PERF_AVAILABLE=false
else
    echo "[profile] coordinator PID: $COORD_PID"
    PERF_AVAILABLE=true
fi

# ── Verify coordinator is reachable ─────────────────────────────────────────
echo "[profile] checking coordinator health at $COORD_ADDR ..."
python - <<'PY' "$COORD_ADDR"
import sys
from dtruntime.client import RuntimeClient
try:
    c = RuntimeClient(sys.argv[1])
    c._stub.GetRuntimeInfo(c._runtime_pb2.GetRuntimeInfoRequest(), metadata=c.rpc_metadata())
    c.close()
    print("[profile] coordinator reachable — stream-checkpoint-artifact capability confirmed")
except Exception as e:
    print(f"[profile] ERROR: coordinator not reachable at {sys.argv[1]}: {e}", file=sys.stderr)
    sys.exit(1)
PY

# ── Scaling sweep ────────────────────────────────────────────────────────────
declare -A SCALING_RESULTS
RUN_INDEX=0

for WORKERS in $WORKER_COUNTS; do
    RUN_INDEX=$((RUN_INDEX + 1))
    RUN_ID="async-profile-w${WORKERS}-${TS}"
    OUT="$ROOT/scaling/workers-${WORKERS}"
    mkdir -p "$OUT"

    echo ""
    echo "[profile] ── workers=${WORKERS} ──────────────────────────────────────"

    # Start continuous iostat in background for this run.
    if command -v iostat >/dev/null 2>&1; then
        iostat -x 1 > "$ROOT/iostat/workers-${WORKERS}.txt" 2>&1 &
        IOSTAT_PID=$!
    fi

    # Start perf record on coordinator PID (if available and perf is installed).
    PERF_DATA="$ROOT/perf/workers-${WORKERS}.perf.data"
    PERF_RECORD_PID=""
    if [[ "$PERF_AVAILABLE" == "true" ]] && command -v perf >/dev/null 2>&1; then
        # -F 99: 99 Hz sampling (avoids lockstep with 100 Hz clock)
        # -g: capture call graphs (frame pointers or DWARF)
        # --call-graph dwarf: use DWARF unwinding for accurate Rust stacks
        perf record -F 99 -p "$COORD_PID" --call-graph dwarf \
            -o "$PERF_DATA" 2>"$ROOT/perf/workers-${WORKERS}-record.log" &
        PERF_RECORD_PID=$!
        echo "[profile] perf record started (PID $PERF_RECORD_PID)"
    fi

    # Run the benchmark.
    BENCH_START="$(date +%s%3N)"
    PYTHONPATH=python python python/examples/multi_worker_async_benchmark.py \
        --workers "$WORKERS" \
        --steps "$STEPS" \
        --checkpoint-every-steps "$CHECKPOINT_EVERY" \
        --checkpoint-bytes "$CHECKPOINT_BYTES" \
        --run-id "$RUN_ID" \
        --coordinator-addr "$COORD_ADDR" \
        --output-dir "$OUT" \
        --json-out "$OUT/result.json" \
        2>&1 | tee "$OUT/benchmark.log"
    BENCH_END="$(date +%s%3N)"
    BENCH_MS=$((BENCH_END - BENCH_START))

    # Stop perf record.
    if [[ -n "$PERF_RECORD_PID" ]]; then
        kill -INT "$PERF_RECORD_PID" 2>/dev/null || true
        wait "$PERF_RECORD_PID" 2>/dev/null || true
        echo "[profile] perf record stopped, data at $PERF_DATA"

        # Generate flamegraph if FlameGraph scripts are available.
        if [[ -d "$FLAMEGRAPH_DIR" ]] && [[ -f "$FLAMEGRAPH_DIR/flamegraph.pl" ]]; then
            echo "[profile] generating flamegraph for workers=${WORKERS}..."
            perf script -i "$PERF_DATA" 2>/dev/null \
                | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" 2>/dev/null \
                | "$FLAMEGRAPH_DIR/flamegraph.pl" \
                    --title "coordinator — ${WORKERS} workers streaming checkpoints" \
                    --width 1800 \
                > "$ROOT/perf/flamegraph-workers-${WORKERS}.svg" 2>/dev/null \
                && echo "[profile] flamegraph: $ROOT/perf/flamegraph-workers-${WORKERS}.svg" \
                || echo "[profile] flamegraph generation failed (non-fatal)"
        else
            echo "[profile] FlameGraph not found at $FLAMEGRAPH_DIR — skipping SVG (set FLAMEGRAPH_DIR)"
        fi
    fi

    # Stop iostat.
    if [[ -n "${IOSTAT_PID:-}" ]]; then
        kill "$IOSTAT_PID" 2>/dev/null || true
        wait "$IOSTAT_PID" 2>/dev/null || true
    fi

    # Snapshot coordinator metrics after this run.
    curl -fsS "$METRICS_ADDR/metrics" > "$OUT/coordinator-metrics.prom" 2>/dev/null \
        || echo "[profile] metrics endpoint not reachable" > "$OUT/coordinator-metrics.prom"

    echo "[profile] workers=${WORKERS} done in ${BENCH_MS}ms"
    SCALING_RESULTS[$WORKERS]="$OUT/result.json"
done

# ── perf stat summary (system-wide, 5-second window) ────────────────────────
if command -v perf >/dev/null 2>&1; then
    echo ""
    echo "[profile] collecting perf stat (5s system-wide)..."
    perf stat -a sleep 5 > "$ROOT/perf/perf-stat.txt" 2>&1 || true
fi

# ── Build scaling curve JSON ─────────────────────────────────────────────────
echo ""
echo "[profile] building scaling curve..."
python - "$ROOT" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
scaling_dir = root / "scaling"
curve = []
for result_file in sorted(scaling_dir.glob("*/result.json")):
    try:
        data = json.loads(result_file.read_text(encoding="utf-8"))
        curve.append({
            "workers": data.get("workers"),
            "aggregate_throughput_mb_s": data.get("aggregate_throughput_mb_s"),
            "aggregate_checkpoint_bytes": data.get("aggregate_checkpoint_bytes"),
            "stall_percent_p50": data.get("stall_percent_p50"),
            "stall_percent_p99": data.get("stall_percent_p99"),
            "step_time_p50_ms": data.get("step_time_p50_ms"),
            "total_wall_seconds": data.get("total_wall_seconds"),
            "checkpoints_committed": data.get("checkpoints_committed"),
        })
    except Exception as e:
        print(f"warning: could not parse {result_file}: {e}", file=sys.stderr)

curve.sort(key=lambda x: x.get("workers") or 0)
out = root / "scaling_curve.json"
out.write_text(json.dumps(curve, indent=2) + "\n", encoding="utf-8")
print(json.dumps(curve, indent=2))
PY

echo ""
echo "[profile] ── complete ───────────────────────────────────────────────────"
echo "[profile] results: $ROOT"
echo "[profile] scaling: $ROOT/scaling_curve.json"
if [[ -n "$(ls "$ROOT/perf/"*.svg 2>/dev/null)" ]]; then
    echo "[profile] flamegraphs: $ROOT/perf/*.svg"
fi
