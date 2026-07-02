"""Real DDP training driven by the coordinator rendezvous.

Launch with the coordinator-backed launcher (single or multi node)::

    python -m dtruntime.launch \
        --coordinator localhost:50051 --run-name ddp-demo \
        --nproc-per-node 2 \
        python/examples/pytorch_ddp_train.py --steps 50

Each process calls :func:`dtruntime.torch_dist.init`, receives its rank and
the c10d store endpoint from the coordinator, forms the process group, and
trains a small MLP under DistributedDataParallel.  Worker death is handled
elastically: when a peer dies mid-training the survivors re-rendezvous and
training restarts from the model construction inside ``train`` (in real jobs,
restore from the latest committed checkpoint there).

Test hooks (used by python/tests/test_torch_distributed_integration.py):

- ``DTRUNTIME_TEST_FAIL_RANK`` / ``DTRUNTIME_TEST_FAIL_STEP``: the process
  holding that rank exits abruptly at the given step — once, recorded via a
  marker file in ``DTRUNTIME_TEST_DIR`` — to simulate a worker crash.
- ``DTRUNTIME_TEST_DIR``: when set, each process writes a JSON report with its
  final rank, world size, round, steps, and loss for the test to assert on.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import torch
import torch.distributed as dist
import torch.nn as nn

from dtruntime import torch_dist


def build_model() -> nn.Module:
    torch.manual_seed(0)  # same initial weights on every rank
    return nn.Sequential(nn.Linear(32, 64), nn.ReLU(), nn.Linear(64, 4))


def train(ctx: torch_dist.TrainingContext, steps: int, report: dict) -> None:
    model = ctx.wrap_model(build_model(), strategy="ddp")
    optimizer = torch.optim.SGD(model.parameters(), lr=0.05)
    loss_fn = nn.CrossEntropyLoss()

    fail_rank = int(os.environ.get("DTRUNTIME_TEST_FAIL_RANK", "-1"))
    fail_step = int(os.environ.get("DTRUNTIME_TEST_FAIL_STEP", "-1"))
    test_dir = os.environ.get("DTRUNTIME_TEST_DIR")

    generator = torch.Generator().manual_seed(1234 + ctx.rank)
    loss_value = float("nan")
    for step in range(steps):
        ctx.raise_if_membership_changed()

        if ctx.rank == fail_rank and step == fail_step and test_dir:
            crash_marker = Path(test_dir) / f"crashed-{os.environ['DTRUNTIME_WORKER_ID']}"
            if not crash_marker.exists():
                crash_marker.touch()
                print(f"[rank {ctx.rank}] simulating crash at step {step}", flush=True)
                os._exit(17)

        features = torch.randn(16, 32, generator=generator)
        labels = torch.randint(0, 4, (16,), generator=generator)
        optimizer.zero_grad()
        loss = loss_fn(model(features), labels)
        loss.backward()  # DDP all-reduces gradients here — real collective traffic
        optimizer.step()
        loss_value = float(loss.detach())

    # Prove the replicas stayed synchronised: after identical updates the
    # parameter sum must agree across ranks (gather to compare on rank 0).
    param_sum = torch.tensor([sum(p.sum().item() for p in model.parameters())])
    gathered = [torch.zeros_like(param_sum) for _ in range(ctx.world_size)]
    dist.all_gather(gathered, param_sum)
    max_spread = float(torch.stack(gathered).std())

    report.update(
        {
            "rank": ctx.rank,
            "world_size": ctx.world_size,
            "round": ctx.rendezvous_round,
            "steps": steps,
            "final_loss": loss_value,
            "param_sum_spread": max_spread,
        }
    )
    print(
        f"[rank {ctx.rank}/{ctx.world_size}] done: loss={loss_value:.4f} "
        f"param_spread={max_spread:.2e} round={ctx.rendezvous_round}",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--steps", type=int, default=50)
    parser.add_argument("--max-rejoin", type=int, default=10)
    args = parser.parse_args()

    report: dict = {}
    ctx = torch_dist.init()
    try:
        ctx.run_elastic(
            lambda c: train(c, args.steps, report),
            max_rejoin=args.max_rejoin,
            collective_error_grace=float(os.environ.get("DTRUNTIME_TEST_GRACE", "150")),
        )
    finally:
        ctx.shutdown()

    test_dir = os.environ.get("DTRUNTIME_TEST_DIR")
    if test_dir and report:
        out = Path(test_dir) / f"report-{os.environ.get('DTRUNTIME_WORKER_ID', 'w')}.json"
        out.write_text(json.dumps(report), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
