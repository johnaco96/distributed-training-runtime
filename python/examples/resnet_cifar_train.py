"""Real ResNet-18 training on CIFAR-10, driven by the coordinator.

A genuine, recognizable model (torchvision ResNet-18, adapted for 32x32 CIFAR)
trains on the real CIFAR-10 dataset through the coordinator's rendezvous, under
DistributedDataParallel, with distributed checkpointing. It records legitimate
*systems* metrics — throughput (samples/sec), per-step time, checkpoint
save/restore time, and the loss/accuracy curve — so a mid-training worker crash
and checkpoint-resume can be measured, not just asserted.

Launch via the coordinator-backed launcher (see demo/run_benchmark.py):

    python -m dtruntime.launch --coordinator HOST:PORT --run-name resnet-cifar \
        --nproc-per-node 2 --backend gloo \
        python/examples/resnet_cifar_train.py --steps 60 --batch-size 32 \
        --checkpoint --checkpoint-every 20 --restore

Test/demo hooks (same as pytorch_ddp_train.py): DTRUNTIME_TEST_FAIL_RANK /
DTRUNTIME_TEST_FAIL_STEP crash a rank once at a step; DTRUNTIME_TEST_DIR /
DTRUNTIME_DATA_DIR set the report + dataset directories.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import torch
import torch.nn as nn
import torchvision
import torchvision.transforms as T
from torch.utils.data import DataLoader, DistributedSampler

from dtruntime import torch_dist


def build_model() -> nn.Module:
    """torchvision ResNet-18, adapted for CIFAR-10 (3x3 stem, no maxpool, 10 classes)."""
    torch.manual_seed(0)  # identical init on every rank
    from torchvision.models import resnet18

    model = resnet18(num_classes=10)
    model.conv1 = nn.Conv2d(3, 64, kernel_size=3, stride=1, padding=1, bias=False)
    model.maxpool = nn.Identity()
    return model


def build_loader(rank: int, world_size: int, batch_size: int, data_dir: str):
    tf = T.Compose([
        T.ToTensor(),
        T.Normalize((0.4914, 0.4822, 0.4465), (0.2470, 0.2435, 0.2616)),
    ])
    # Real CIFAR-10 images (PNG, class-per-folder), pre-fetched by the orchestrator.
    train_dir = os.path.join(data_dir, "cifar10", "train")
    ds = torchvision.datasets.ImageFolder(train_dir, transform=tf)
    sampler = DistributedSampler(ds, num_replicas=world_size, rank=rank, shuffle=True, seed=0)
    loader = DataLoader(ds, batch_size=batch_size, sampler=sampler, num_workers=0, drop_last=True)
    return loader, sampler


def _dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def train(ctx: torch_dist.TrainingContext, args, report: dict) -> None:
    model = ctx.wrap_model(build_model(), strategy="ddp")
    # Adam converges reliably on a short (<1 epoch) CPU smoke-run without the
    # warmup + LR schedule that SGD(0.1) needs to keep BatchNorm stable at small
    # batch size; the point here is to show real, monotone loss descent through
    # the DDP all-reduce, not to reproduce a full training recipe.
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=5e-4)
    loss_fn = nn.CrossEntropyLoss()

    from dtruntime.checkpointing import DistributedCheckpointer
    checkpointer = DistributedCheckpointer(ctx)

    restored_step = -1
    if args.restore:
        t0 = time.perf_counter()
        restored = checkpointer.restore(model, optimizer)
        if restored is not None:
            restored_step = restored.step
            report["restored_step"] = restored.step
            report["restore_ms"] = round((time.perf_counter() - t0) * 1000, 1)
            print(f"[rank {ctx.rank}] restored checkpoint step {restored.step} "
                  f"in {report['restore_ms']} ms", flush=True)

    loader, sampler = build_loader(ctx.rank, ctx.world_size, args.batch_size, args.data_dir)
    sampler.set_epoch(0)
    batches = iter(loader)

    fail_rank = int(os.environ.get("DTRUNTIME_TEST_FAIL_RANK", "-1"))
    fail_step = int(os.environ.get("DTRUNTIME_TEST_FAIL_STEP", "-1"))
    test_dir = os.environ.get("DTRUNTIME_TEST_DIR")

    curve = report.setdefault("loss_curve", [])
    step_times, ckpt_ms, images = [], [], 0
    last_saved = -1
    loss_v = acc_v = float("nan")

    for step in range(restored_step + 1, args.steps):
        ctx.raise_if_membership_changed()

        if ctx.rank == fail_rank and step == fail_step and test_dir:
            marker = Path(test_dir) / f"crashed-{os.environ['DTRUNTIME_WORKER_ID']}"
            if not marker.exists():
                marker.touch()
                print(f"[rank {ctx.rank}] simulating crash at step {step}", flush=True)
                os._exit(17)

        try:
            x, y = next(batches)
        except StopIteration:
            batches = iter(loader)
            x, y = next(batches)

        t0 = time.perf_counter()
        optimizer.zero_grad()
        out = model(x)
        loss = loss_fn(out, y)
        loss.backward()          # DDP all-reduces gradients here (real collective)
        optimizer.step()
        step_times.append(time.perf_counter() - t0)
        images += x.size(0)

        loss_v = float(loss.detach())
        acc_v = float((out.argmax(1) == y).float().mean())
        curve.append({"step": step, "loss": round(loss_v, 4), "acc": round(acc_v, 4)})
        if ctx.rank == 0:
            print(f"  step {step:3d}  loss {loss_v:.4f}  acc {acc_v:.3f}  "
                  f"{x.size(0) / step_times[-1]:.0f} img/s", flush=True)

        if args.checkpoint and args.checkpoint_every > 0 and step > restored_step \
                and (step + 1) % args.checkpoint_every == 0:
            t0 = time.perf_counter()
            checkpointer.save(model, optimizer, step=step)
            ckpt_ms.append(round((time.perf_counter() - t0) * 1000, 1))
            last_saved = step

    if args.checkpoint and last_saved != args.steps - 1:
        checkpointer.save(model, optimizer, step=args.steps - 1)

    train_s = sum(step_times)
    report.update({
        "rank": ctx.rank, "world_size": ctx.world_size, "round": ctx.rendezvous_round,
        "steps": args.steps, "batch_size": args.batch_size,
        "final_loss": round(loss_v, 4), "final_acc": round(acc_v, 4),
        "images": images,
        "samples_per_sec_per_rank": round(images / train_s, 1) if train_s else 0,
        "avg_step_ms": round(1000 * train_s / max(len(step_times), 1), 1),
        "checkpoint_save_ms": ckpt_ms,
    })


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--steps", type=int, default=60)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--data-dir", default=os.environ.get("DTRUNTIME_DATA_DIR", "target/cifar-data"))
    p.add_argument("--checkpoint", action="store_true")
    p.add_argument("--checkpoint-every", type=int, default=0)
    p.add_argument("--restore", action="store_true")
    p.add_argument("--max-rejoin", type=int, default=10)
    args = p.parse_args()

    report: dict = {}
    ctx = torch_dist.init()
    try:
        ctx.run_elastic(
            lambda c: train(c, args, report),
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
    raise SystemExit(main())
