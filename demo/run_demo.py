#!/usr/bin/env python3
"""One-command failure-recovery demo for the Distributed Training Runtime.

    python demo/run_demo.py

Starts the Rust coordinator, launches a REAL 2-worker gloo DDP training job with
distributed checkpointing, crashes one worker mid-training, and shows the job
re-form and resume from the last checkpoint - no progress lost. Everything runs
locally on CPU; no GPU or cluster required.

Prereqs: `pip install -e ".[dev]" && pip install torch` and a coordinator binary
(the script builds it with `cargo build --release -p runtime-coordinator` if the
release binary is missing).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from _common import _c, banner, coordinator_binary, free_port, wait_grpc

# Windows consoles default to cp1252; force UTF-8 and never crash on output.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

REPO = Path(__file__).resolve().parents[1]
EXAMPLE = REPO / "python" / "examples" / "pytorch_ddp_train.py"
STEPS = 20
CRASH_RANK = "1"
CRASH_STEP = "8"
CKPT_EVERY = 5


def main() -> int:
    banner("Distributed Training Runtime - Failure Recovery Demo")
    print(
        "  A real 2-worker gloo DDP job trains a small MLP through the Rust\n"
        "  coordinator's rendezvous, checkpointing every few steps. One worker\n"
        f"  is crashed at step {CRASH_STEP}; the survivors re-rendezvous and the\n"
        "  run resumes from the last checkpoint. All local, all CPU.\n"
    )

    # Fresh state every run: a stale metadata DB would rehydrate the previous
    # (completed) run and reject the new workers.
    workdir = REPO / "target" / "demo"
    shutil.rmtree(workdir, ignore_errors=True)
    reports = workdir / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    address = f"127.0.0.1:{free_port()}"

    print(_c("[1/3] Starting the coordinator...", "1"))
    coord_log = (workdir / "coordinator.log").open("w", encoding="utf-8")
    coord = subprocess.Popen(
        [coordinator_binary()] + [
            "--addr", address,
            "--metadata-db", str(workdir / "meta.db"),
            "--storage-root", str(workdir / "artifacts"),
        ],
        stdout=coord_log, stderr=subprocess.STDOUT,
    )
    try:
        wait_grpc(address, coord)
        print(f"      coordinator up at {_c(address, '32')}\n")

        print(_c(f"[2/3] Launching 2-worker DDP training "
                 f"(worker {CRASH_RANK} crashes at step {CRASH_STEP})...", "1"))
        print(_c("      --- live launcher + training output ---", "2"))
        env = dict(os.environ)
        env.update({
            "PYTHONPATH": str(REPO / "python"),
            "DTRUNTIME_TEST_DIR": str(reports),
            "DTRUNTIME_HEARTBEAT_INTERVAL": "1",
            "DTRUNTIME_TEST_GRACE": "120",
            "DTRUNTIME_WORKER_HOST": "127.0.0.1",
            "DTRUNTIME_TEST_FAIL_RANK": CRASH_RANK,
            "DTRUNTIME_TEST_FAIL_STEP": CRASH_STEP,
        })
        launch = subprocess.run(
            [sys.executable, "-m", "dtruntime.launch",
             "--coordinator", address, "--run-name", "failure-recovery-demo",
             "--nproc-per-node", "2", "--world-size", "2", "--backend", "gloo",
             "--max-restarts", "3", "--worker-host", "127.0.0.1",
             str(EXAMPLE), "--steps", str(STEPS),
             "--checkpoint", "--checkpoint-every", str(CKPT_EVERY), "--restore"],
            cwd=REPO, env=env, timeout=600,
        )
        print(_c("      --- end launcher output ---", "2"))
        if launch.returncode != 0:
            print(_c("\n  launcher exited non-zero - see output above.", "31"))
            return 1

        print(_c("\n[3/3] Verifying recovery from the workers' reports...", "1"))
        # Each rank writes a report ONLY on successful completion; the crashed
        # process exits without one and leaves a `crashed-<worker>` marker.
        data = [json.loads(p.read_text()) for p in sorted(reports.glob("report-*.json"))]
        crashed = any(reports.glob("crashed-*"))
        restored = [r for r in data if r.get("restored_step", -1) >= 0]
        max_round = max((r.get("round", 0) for r in data), default=0)
        completed = len(data)  # one report per rank that finished all steps

        def check(ok: bool, msg: str) -> None:
            print(f"      {_c('[OK]', '32') if ok else _c('[!!]', '31')} {msg}")

        check(crashed, f"worker {CRASH_RANK} crashed at step {CRASH_STEP} (injected)")
        check(max_round >= 1, "survivors re-rendezvoused and re-formed the process group")
        check(bool(restored),
              "resumed from checkpoint step "
              f"{restored[0]['restored_step'] if restored else '-'}")
        check(completed >= 2, f"both ranks finished all {STEPS} steps ({completed}/2 reports)")

        ok = crashed and bool(restored) and completed >= 2
        banner("SUCCESS - the run survived a worker crash and finished."
               if ok else "DEMO INCOMPLETE - see checks above.")
        if ok:
            print("  A worker died mid-training, the coordinator re-formed the\n"
                  "  process group, and training resumed from the last checkpoint\n"
                  "  without losing progress. Try `dtruntime-admin -a "
                  f"{address} runs list` while it runs.\n")
        return 0 if ok else 1
    finally:
        if coord.poll() is None:
            coord.terminate()
            try:
                coord.wait(timeout=10)
            except subprocess.TimeoutExpired:
                coord.kill()


if __name__ == "__main__":
    raise SystemExit(main())
