#!/usr/bin/env python3
"""
Flamegraph capture script for runtime-coordinator.
Starts coordinator, perf-records it under 500-worker gRPC load for 30s,
then generates flamegraph.svg in .flamegraph-tmp/.
"""
import os
import pathlib
import shutil
import signal
import subprocess
import time

HOME = pathlib.Path.home()
# Repo root: this script lives in <repo>/scripts/.
DTR = pathlib.Path(__file__).resolve().parent.parent
OUTDIR  = DTR / ".flamegraph-tmp"
COORD   = DTR / "target" / "perf" / "runtime-coordinator"
BENCH   = DTR / "target" / "perf" / "runtime-bench"
CARGO_BIN = HOME / ".cargo" / "bin"
PORT    = "50199"
STORAGE = pathlib.Path("/tmp/flamegraph-dtr-storage")

env = os.environ.copy()
env["PATH"] = str(CARGO_BIN) + ":" + env.get("PATH", "")

OUTDIR.mkdir(parents=True, exist_ok=True)
STORAGE.mkdir(parents=True, exist_ok=True)

print("=== Starting coordinator ===")
coord = subprocess.Popen(
    [str(COORD), "--addr", f"127.0.0.1:{PORT}", "--storage-root", str(STORAGE)],
    env=env,
    stdout=subprocess.DEVNULL,
    stderr=open("/tmp/coord-flame.log", "w"),
)
print(f"coordinator PID={coord.pid}")
time.sleep(2)

perf_data = str(OUTDIR / "perf.data")
print("=== Starting perf record (--call-graph fp) ===")
perf = subprocess.Popen(
    # --call-graph fp is more reliable than dwarf on WSL2/Hyper-V because
    # the coordinator binary is now built with -C force-frame-pointers=yes.
    ["perf", "record", "-g", "-F", "997", "-p", str(coord.pid),
     "--call-graph", "fp", "-o", perf_data],
    env=env,
)
print(f"perf PID={perf.pid}")
time.sleep(1)

print("=== Running bench (30s) ===")
bench = subprocess.run(
    [str(BENCH), "logical-workers",
     "--target", f"http://127.0.0.1:{PORT}",
     "--workers", "500",
     "--duration-secs", "30",
     "--concurrency", "64",
     "--shards", "1000",
     "--heartbeat-interval-ms", "2000"],
    env=env,
    check=False,
)
print(f"bench exit={bench.returncode}")

print("=== Stopping perf and coordinator ===")
perf.send_signal(signal.SIGINT)
try:
    perf.wait(timeout=10)
except subprocess.TimeoutExpired:
    perf.kill()
coord.terminate()
try:
    coord.wait(timeout=5)
except subprocess.TimeoutExpired:
    coord.kill()

pdata = OUTDIR / "perf.data"
print(f"perf.data size: {pdata.stat().st_size // 1024} KB")

svg_path = OUTDIR / "flamegraph.svg"
print("=== Generating flamegraph ===")
perf_script = subprocess.Popen(
    ["perf", "script", "-i", str(pdata)],
    stdout=subprocess.PIPE,
    stderr=subprocess.DEVNULL,
    env=env,
)
collapse = subprocess.Popen(
    [str(CARGO_BIN / "inferno-collapse-perf"), "--kernel"],
    stdin=perf_script.stdout,
    stdout=subprocess.PIPE,
    env=env,
)
perf_script.stdout.close()
with open(str(svg_path), "wb") as svg_f:
    flamegraph = subprocess.Popen(
        [str(CARGO_BIN / "inferno-flamegraph"),
         "--title", "runtime-coordinator: 500-worker gRPC load",
         "--subtitle", "30s perf record -F997 --call-graph fp (force-frame-pointers)"],
        stdin=collapse.stdout,
        stdout=svg_f,
        env=env,
    )
    collapse.stdout.close()
    flamegraph.wait()
    collapse.wait()
    perf_script.wait()

size_kb = svg_path.stat().st_size // 1024
print(f"flamegraph.svg: {size_kb} KB -> {svg_path}")
print("DONE")

shutil.rmtree(str(STORAGE), ignore_errors=True)
