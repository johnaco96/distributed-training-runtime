#!/usr/bin/env python3
"""Real-numbers benchmark: ResNet-18 on CIFAR-10, with a mid-training crash.

    python demo/run_benchmark.py

Trains a real torchvision ResNet-18 on the real CIFAR-10 dataset across 2 gloo
workers through the coordinator, crashes one worker mid-training, and resumes
from the last checkpoint. Prints legitimate SYSTEMS metrics — throughput,
checkpoint save time/size, and the loss/accuracy curve showing training really
learns and stays continuous across the crash — and writes them to
target/benchmark/results.json.

Prereqs: `pip install -e ".[dev]" && pip install torch torchvision`. CPU-only;
CIFAR-10 (~170MB) downloads once on first run.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request
from pathlib import Path

from _common import _c, banner, coordinator_binary, free_port, wait_grpc

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "python" / "examples" / "resnet_cifar_train.py"
DATA_DIR = REPO / "target" / "cifar-data"
STEPS = 100
BATCH = 32
CKPT_EVERY = 25
CRASH_STEP = 60
CRASH_RANK = "1"


def ensure_cifar10() -> None:
    """Fetch real CIFAR-10 (PNG images, class-per-folder) from a fast mirror and
    extract it once. Skips if already present."""
    train_dir = DATA_DIR / "cifar10" / "train"
    if train_dir.is_dir() and any(train_dir.iterdir()):
        print("      dataset already present (cached)")
        return
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tgz = DATA_DIR / "cifar10.tgz"
    url = "https://s3.amazonaws.com/fast-ai-sample/cifar10.tgz"
    t0 = time.perf_counter()
    urllib.request.urlretrieve(url, tgz)
    with tarfile.open(tgz) as tf:
        tf.extractall(DATA_DIR)
    print(f"      dataset ready in {time.perf_counter() - t0:.0f}s "
          f"({sum(1 for _ in train_dir.rglob('*.png'))} training images)")


def main() -> int:
    banner("Real-Numbers Benchmark - ResNet-18 on CIFAR-10")
    print("  A real ResNet-18 trains on real CIFAR-10 across 2 gloo workers via\n"
          "  the coordinator, with distributed checkpointing. One worker crashes\n"
          f"  at step {CRASH_STEP}; the run resumes from checkpoint and finishes.\n"
          "  We measure throughput, checkpoint cost, and loss continuity.\n")

    print(_c("[1/4] Preparing real CIFAR-10 (downloads ~168MB once, then cached)...", "1"))
    ensure_cifar10()
    print()

    workdir = REPO / "target" / "benchmark"
    shutil.rmtree(workdir, ignore_errors=True)
    reports = workdir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    address = f"127.0.0.1:{free_port()}"

    print(_c("[2/4] Starting the coordinator...", "1"))
    coord = subprocess.Popen(
        [coordinator_binary(), "--addr", address,
         "--metadata-db", str(workdir / "meta.db"),
         "--storage-root", str(workdir / "artifacts")],
        stdout=(workdir / "coordinator.log").open("w"), stderr=subprocess.STDOUT)
    try:
        wait_grpc(address, coord)
        print(f"      coordinator up at {_c(address, '32')}\n")

        print(_c(f"[3/4] Training ResNet-18 (2 workers, worker {CRASH_RANK} crashes "
                 f"at step {CRASH_STEP})...", "1"))
        print(_c("      --- live training output (rank 0) ---", "2"))
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(REPO / "python"), "DTRUNTIME_TEST_DIR": str(reports),
            "DTRUNTIME_HEARTBEAT_INTERVAL": "1", "DTRUNTIME_TEST_GRACE": "120",
            # Bound the blocked all-reduce on the surviving rank so failure is
            # detected in ~30s instead of the 300s default -> snappy recovery.
            "DTRUNTIME_PG_TIMEOUT": "30",
            "DTRUNTIME_WORKER_HOST": "127.0.0.1", "DTRUNTIME_DATA_DIR": str(DATA_DIR),
            "DTRUNTIME_TEST_FAIL_RANK": CRASH_RANK, "DTRUNTIME_TEST_FAIL_STEP": str(CRASH_STEP),
        })
        wall0 = time.perf_counter()
        run = subprocess.run(
            [sys.executable, "-m", "dtruntime.launch", "--coordinator", address,
             "--run-name", "resnet-cifar-benchmark", "--nproc-per-node", "2",
             "--world-size", "2", "--backend", "gloo", "--max-restarts", "3",
             "--worker-host", "127.0.0.1", str(EXAMPLE), "--steps", str(STEPS),
             "--batch-size", str(BATCH), "--checkpoint",
             "--checkpoint-every", str(CKPT_EVERY), "--restore"],
            cwd=REPO, env=env, timeout=1800)
        wall = time.perf_counter() - wall0
        print(_c("      --- end training output ---", "2"))
        if run.returncode != 0:
            print(_c("  training exited non-zero - see output above.", "31"))
            return 1

        print(_c("\n[4/4] Results (real, measured)...", "1"))
        data = [json.loads(p.read_text()) for p in sorted(reports.glob("report-*.json"))]
        if len(data) < 2:
            print(_c(f"  expected 2 worker reports, got {len(data)}", "31"))
            return 1
        r0 = next((r for r in data if r.get("rank") == 0), data[0])
        agg_throughput = sum(r.get("samples_per_sec_per_rank", 0) for r in data)
        ckpt_ms = [m for r in data for m in r.get("checkpoint_save_ms", [])]
        ckpt_dirs = sorted((workdir / "artifacts").rglob("dcp-step-*"))
        per_ckpt_bytes = _dir_bytes(ckpt_dirs[-1]) if ckpt_dirs else 0
        total_ckpt_bytes = _dir_bytes(workdir / "artifacts")
        curve = r0.get("loss_curve", [])
        # Smooth over windows: single-batch loss/acc are noisy, so average the
        # opening 5 and closing 10 steps to report a fair learning signal.
        losses = [c.get("loss") for c in curve if c.get("loss") is not None]
        accs = [c.get("acc") for c in curve if c.get("acc") is not None]
        loss_start = _mean(losses[:5])
        loss_end = _mean(losses[-10:])
        acc_start = _mean(accs[:5])
        acc_end = _mean(accs[-10:])
        restored = [r for r in data if r.get("restored_step", -1) >= 0]
        crashed = any(reports.glob("crashed-*"))

        def row(k: str, v: str) -> None:
            print(f"      {k:<26} {_c(v, '1;32')}")

        row("Model / dataset", "ResNet-18 (real) / CIFAR-10 (real)")
        row("Workers x batch", f"2 x {BATCH} (gloo, CPU)")
        row("Throughput (aggregate)", f"{agg_throughput:.0f} images/sec")
        row("Avg step time / rank", f"{r0.get('avg_step_ms', 0):.0f} ms")
        row("Checkpoint save", f"~{_mean(ckpt_ms):.0f} ms, {per_ckpt_bytes/1e6:.0f} MB each "
                               f"({len(ckpt_dirs)} retained, {total_ckpt_bytes/1e6:.0f} MB total)"
                               if ckpt_ms else "n/a")
        row("Loss  start -> end", f"{loss_start:.3f} -> {loss_end:.3f}  (avg of first 5 / last 10)")
        row("Acc   start -> end", f"{acc_start:.3f} -> {acc_end:.3f}")
        row("Fault injected", f"worker {CRASH_RANK} crashed at step {CRASH_STEP}"
                              if crashed else "none")
        row("Recovered from", f"checkpoint step {restored[0]['restored_step']}"
                              if restored else "n/a")
        row("Total wall time", f"{wall:.1f} s")

        json.dump({
            "model": "resnet18", "dataset": "cifar10", "workers": 2, "batch_size": BATCH,
            "aggregate_images_per_sec": round(agg_throughput, 1),
            "avg_step_ms_per_rank": r0.get("avg_step_ms"),
            "checkpoint_save_ms_avg": round(_mean(ckpt_ms), 1) if ckpt_ms else None,
            "checkpoint_bytes_each": per_ckpt_bytes,
            "checkpoint_bytes_total": total_ckpt_bytes,
            "checkpoints_retained": len(ckpt_dirs),
            "loss_start": round(loss_start, 4), "loss_end": round(loss_end, 4),
            "acc_start": round(acc_start, 4), "acc_end": round(acc_end, 4),
            "crashed": crashed,
            "restored_from_step": restored[0]["restored_step"] if restored else None,
            "wall_seconds": round(wall, 1), "loss_curve": curve,
        }, (workdir / "results.json").open("w"), indent=2)

        ok = crashed and restored and loss_end < loss_start
        banner("SUCCESS - real model, real data, real recovery, real numbers."
               if ok else "BENCHMARK INCOMPLETE - see above.")
        print(f"  Full metrics + loss curve: {workdir / 'results.json'}\n")
        return 0 if ok else 1
    finally:
        if coord.poll() is None:
            coord.terminate()
            try:
                coord.wait(timeout=10)
            except subprocess.TimeoutExpired:
                coord.kill()


def _dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def _mean(xs: list) -> float:
    return sum(xs) / len(xs) if xs else float("nan")


if __name__ == "__main__":
    raise SystemExit(main())
