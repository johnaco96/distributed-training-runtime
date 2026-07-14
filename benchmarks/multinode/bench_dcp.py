#!/usr/bin/env python3
"""torchrun + PyTorch Distributed Checkpoint (DCP) baseline.

Same shape as bench_dtr.py so the numbers are directly comparable:
  stall_s      time the training thread was blocked
               (async mode: dcp.async_save staging; sync mode: full dcp.save)
  e2e_s        time until the checkpoint was fully durable

Launch with torchrun (single node dress rehearsal):
  torchrun --nproc-per-node=2 bench_dcp.py --state-mb 2048 --checkpoints 3 \
      --ckpt-dir /nvme/dcp-ckpts --mode async --out-prefix dcp

Multi-node:
  torchrun --nnodes=2 --node-rank=<n> --master-addr=<coordinator-host> \
      --nproc-per-node=1 bench_dcp.py ...
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path

import torch
import torch.distributed as dist
import torch.distributed.checkpoint as dcp

PROCESS_START = time.perf_counter()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state-mb", type=int, default=2048)
    p.add_argument("--checkpoints", type=int, default=3)
    p.add_argument("--train-seconds", type=float, default=3.0)
    p.add_argument("--ckpt-dir", required=True)
    p.add_argument("--mode", choices=["async", "sync"], default="async")
    p.add_argument("--out-prefix", default="dcp")
    p.add_argument("--measure-recovery", action="store_true")
    p.add_argument("--init-method", default=None,
                   help="process-group init method override, e.g. "
                        "file:///tmp/dcp-rdzv (useful on Windows where "
                        "torchrun's libuv TCPStore is unavailable)")
    return p.parse_args()


def make_state(state_mb: int) -> dict:
    weights = torch.randn((state_mb * 3 // 4) * 1024 * 1024 // 4)
    momentum = torch.randn((state_mb - state_mb * 3 // 4) * 1024 * 1024 // 4)
    return {"weights": weights, "momentum": momentum, "step": torch.tensor(0)}


def simulate_training(state: dict, seconds: float) -> None:
    deadline = time.perf_counter() + seconds
    chunk = state["weights"][: 4 * 1024 * 1024]
    while time.perf_counter() < deadline:
        chunk.mul_(1.000001).add_(0.000001)
    state["step"] += 1


def main() -> int:
    args = parse_args()
    rank = int(os.environ.get("RANK", "0"))
    world_size = int(os.environ.get("WORLD_SIZE", "1"))
    if args.init_method:
        dist.init_process_group(
            backend="gloo",
            init_method=args.init_method,
            rank=rank,
            world_size=world_size,
        )
    elif world_size > 1 or "MASTER_ADDR" in os.environ:
        dist.init_process_group(backend="gloo")

    ckpt_root = Path(args.ckpt_dir)
    ckpt_root.mkdir(parents=True, exist_ok=True)
    metrics: dict = {
        "system": f"torchrun+dcp-{args.mode}",
        "rank": rank,
        "world_size": world_size,
        "state_mb": args.state_mb,
        "dest": str(ckpt_root),
        "host": socket.gethostname(),
    }

    if args.measure_recovery:
        dirs = sorted(d for d in ckpt_root.iterdir() if d.is_dir())
        if not dirs:
            raise RuntimeError(f"no checkpoints to recover under {ckpt_root}")
        state = make_state(args.state_mb)
        dcp.load(state, checkpoint_id=str(dirs[-1]))
        recovery_s = time.perf_counter() - PROCESS_START
        metrics["recovery_s"] = recovery_s
        metrics["recovered_checkpoint"] = dirs[-1].name
        print(f"[rank {rank}] RECOVERED {dirs[-1].name} in {recovery_s:.2f}s", flush=True)
    else:
        state = make_state(args.state_mb)

    stalls: list[float] = []
    e2es: list[float] = []
    pending = None
    for round_no in range(1, args.checkpoints + 1):
        simulate_training(state, args.train_seconds)
        ckpt_id = str(ckpt_root / f"ckpt-{round_no:04d}")
        t0 = time.perf_counter()
        if args.mode == "async":
            # async_save blocks while staging the state dict, then writes in
            # the background — the staging time is the training stall.
            if pending is not None:
                pending.result()  # DCP allows one in-flight async save
            pending = dcp.async_save(state, checkpoint_id=ckpt_id)
            stall = time.perf_counter() - t0
            stalls.append(stall)
        else:
            dcp.save(state, checkpoint_id=ckpt_id)
            stall = time.perf_counter() - t0
            stalls.append(stall)
            e2es.append(stall)
        print(f"[rank {rank}] ckpt {round_no}: stall {stall * 1000:.0f} ms", flush=True)

    if pending is not None:
        t_wait = time.perf_counter()
        pending.result()
        e2es.append(stalls[-1] + (time.perf_counter() - t_wait))

    metrics["stall_s"] = stalls
    metrics["e2e_s"] = e2es
    out = Path(f"{args.out_prefix}-rank{rank}.json")
    out.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"[rank {rank}] wrote {out}", flush=True)

    if dist.is_initialized():
        dist.barrier()
        dist.destroy_process_group()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
