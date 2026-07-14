#!/usr/bin/env python3
"""Distributed Training Runtime checkpoint benchmark (one process per rank).

Measures, per checkpoint round:
  stall_s      time the "training" thread was blocked (stage-0 snapshot)
  local_s      time until the artifact was fsynced to local staging
  committed_s  time until this rank's 2PC commit vote was accepted

Recovery mode (--measure-recovery) measures time from process start until the
latest committed checkpoint is fully restored into memory.

Run one process per rank (same --run-key on every rank):

  python bench_dtr.py --addr <coordinator:50051> --rank 0 --world-size 2 \
      --state-mb 2048 --checkpoints 3 \
      --dest s3://checkpoints/bench --staging-dir /nvme/dtr-staging \
      --run-key bench-1 --out dtr-rank0.json

S3 credentials via DTRUNTIME_S3_ENDPOINT / DTRUNTIME_S3_ACCESS_KEY /
DTRUNTIME_S3_SECRET_KEY (see README).
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "python"))

import torch  # noqa: E402

from dtruntime import AsyncCheckpointer, connect  # noqa: E402
from dtruntime.generated import runtime_pb2  # noqa: E402

PROCESS_START = time.perf_counter()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--addr", required=True, help="coordinator host:port")
    p.add_argument("--rank", type=int, required=True)
    p.add_argument("--world-size", type=int, required=True)
    p.add_argument("--state-mb", type=int, default=2048,
                   help="per-rank model+optimizer state size in MB")
    p.add_argument("--checkpoints", type=int, default=3)
    p.add_argument("--train-seconds", type=float, default=3.0,
                   help="simulated training time between checkpoints")
    p.add_argument("--dest", default=None,
                   help="s3://bucket/prefix or directory; default: local staging only")
    p.add_argument("--staging-dir", default=".dtr-bench-staging")
    p.add_argument("--run-key", required=True,
                   help="shared idempotency key — same value on every rank")
    p.add_argument("--out", required=True, help="metrics JSON output path")
    p.add_argument("--measure-recovery", action="store_true",
                   help="restore the latest checkpoint and report recovery time")
    return p.parse_args()


def make_state(state_mb: int) -> dict:
    # ~3/4 "weights" + ~1/4 "optimizer state", float32.
    weights = torch.randn((state_mb * 3 // 4) * 1024 * 1024 // 4)
    momentum = torch.randn((state_mb - state_mb * 3 // 4) * 1024 * 1024 // 4)
    return {"weights": weights, "momentum": momentum, "step": 0}


def simulate_training(state: dict, seconds: float) -> None:
    """Busy tensor work that mutates the live state (like an optimizer step)."""
    deadline = time.perf_counter() + seconds
    chunk = state["weights"][: 4 * 1024 * 1024]
    while time.perf_counter() < deadline:
        chunk.mul_(1.000001).add_(0.000001)
    state["step"] += 1


def main() -> int:
    args = parse_args()
    metrics: dict = {
        "system": "dtruntime",
        "rank": args.rank,
        "world_size": args.world_size,
        "state_mb": args.state_mb,
        "dest": args.dest or "local-staging",
        "host": socket.gethostname(),
    }

    with connect(args.addr) as client:
        run = client.register_run(
            run_name="multinode-bench",
            world_size=args.world_size,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_S3_COMPATIBLE
            if (args.dest or "").startswith("s3://")
            else runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            idempotency_key=args.run_key,
        )
        run_id = run.run_id.value
        worker = client.register_worker(
            run_id=run_id,
            worker_id=f"bench-worker-{args.rank}",
            rank=args.rank,
            host=socket.gethostname(),
            idempotency_key=f"{args.run_key}-w{args.rank}",
        )
        worker_id = worker.worker_id.value
        print(f"[rank {args.rank}] run={run_id} worker={worker_id}", flush=True)

        ckptr = AsyncCheckpointer(
            client,
            run_id,
            worker_id,
            staging_dir=Path(args.staging_dir) / worker_id,
            dest=args.dest,
        )

        if args.measure_recovery:
            state, info = ckptr.restore_latest()
            recovery_s = time.perf_counter() - PROCESS_START
            metrics["recovery_s"] = recovery_s
            metrics["recovered_checkpoint"] = info["checkpoint_id"]
            metrics["recovered_from"] = info["restored_from"]
            print(
                f"[rank {args.rank}] RECOVERED {info['checkpoint_id']} "
                f"({info['restored_from']}) in {recovery_s:.2f}s",
                flush=True,
            )
        else:
            state = make_state(args.state_mb)

        handles = []
        for round_no in range(1, args.checkpoints + 1):
            simulate_training(state, args.train_seconds)
            handle = ckptr.save(state, step=round_no)
            handles.append(handle)
            print(
                f"[rank {args.rank}] ckpt {round_no}: stall {handle.t_stall_s * 1000:.0f} ms",
                flush=True,
            )

        for handle in handles:
            handle.wait(timeout=600)
        ckptr.close()

        metrics["stall_s"] = [h.t_stall_s for h in handles]
        metrics["local_s"] = [h.t_local_s for h in handles]
        metrics["committed_s"] = [h.t_committed_s for h in handles]
        metrics["artifact_bytes"] = handles[-1].size_bytes if handles else 0
        metrics["globally_committed"] = sum(1 for h in handles if h.globally_committed)

    Path(args.out).write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"[rank {args.rank}] wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
