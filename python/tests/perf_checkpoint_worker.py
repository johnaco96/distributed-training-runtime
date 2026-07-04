"""Worker process for the large-checkpoint perf test: single-rank run that
saves and restores a ~200 MB model through the coordinator."""

from __future__ import annotations

import json
import os
import time

import torch
import torch.nn as nn

from dtruntime import RuntimeClient
from dtruntime.checkpointing import DistributedCheckpointer
from dtruntime.generated import runtime_pb2
from dtruntime import torch_dist


def build_model() -> nn.Module:
    torch.manual_seed(7)
    # ~52.4M params * 4 bytes ≈ 200 MB.
    return nn.Sequential(nn.Linear(4096, 6400), nn.Linear(6400, 4096))


def main() -> int:
    address = os.environ["DTRUNTIME_COORDINATOR_ADDR"]
    admin = RuntimeClient(address)
    run_id = admin.register_run(
        run_name="ckpt-perf",
        world_size=1,
        assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
        storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
        idempotency_key="ckpt-perf",
    ).run_id.value
    admin.close()

    ctx = torch_dist.init(
        coordinator_addr=address, run_id=run_id, worker_id="perf-0", backend="gloo"
    )
    try:
        model = ctx.wrap_model(build_model(), strategy="none")
        size_mb = sum(p.numel() * p.element_size() for p in model.parameters()) / 1e6
        checkpointer = DistributedCheckpointer(ctx)

        started = time.monotonic()
        checkpointer.save(model, step=1)
        save_seconds = time.monotonic() - started

        saved_sum = float(sum(p.sum().item() for p in model.parameters()))
        for p in model.parameters():
            with torch.no_grad():
                p.zero_()

        started = time.monotonic()
        restored = checkpointer.restore(model)
        restore_seconds = time.monotonic() - started
        assert restored is not None

        restored_sum = float(sum(p.sum().item() for p in model.parameters()))
        metrics = {
            "size_mb": size_mb,
            "save_seconds": save_seconds,
            "save_mb_per_s": size_mb / max(save_seconds, 1e-6),
            "restore_seconds": restore_seconds,
            "restore_mb_per_s": size_mb / max(restore_seconds, 1e-6),
            "restored_matches": abs(saved_sum - restored_sum) < 1e-3,
        }
        with open(os.environ["DTRUNTIME_PERF_OUT"], "w", encoding="utf-8") as f:
            json.dump(metrics, f)
        return 0
    finally:
        ctx.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
