from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, Subset, TensorDataset
except ImportError:  # pragma: no cover - exercised in no-torch environments
    torch = None
    nn = None
    DataLoader = None
    Subset = None
    TensorDataset = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulated distributed-style PyTorch training (single-process)")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--samples", type=int, default=128)
    parser.add_argument("--input-dim", type=int, default=16)
    parser.add_argument("--num-classes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--shards", type=int, default=None)
    parser.add_argument("--out-dir", type=Path, default=Path(".dtruntime-pytorch-distributed"))
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-checkpoint", action="store_true")
    return parser.parse_args()


def require_torch() -> None:
    if torch is None:
        raise RuntimeError(
            "PyTorch is required for python/examples/pytorch_simulated_distributed.py. "
            "Install torch to run this example."
        )


def build_dataset(samples: int, input_dim: int, num_classes: int, seed: int) -> TensorDataset:
    generator = torch.Generator().manual_seed(seed)
    features = torch.randn(samples, input_dim, generator=generator)
    weights = torch.randn(input_dim, num_classes, generator=generator)
    logits = features @ weights
    targets = torch.argmax(logits, dim=1)
    return TensorDataset(features, targets)


def build_model(input_dim: int, num_classes: int) -> nn.Module:
    return nn.Sequential(nn.Linear(input_dim, 32), nn.ReLU(), nn.Linear(32, num_classes))


def build_shards(samples: int, shards: int) -> list[list[int]]:
    shard_lists = [[] for _ in range(shards)]
    for sample_idx in range(samples):
        shard_lists[sample_idx % shards].append(sample_idx)
    return shard_lists


def assign_shards(workers: int, shard_lists: list[list[int]]) -> dict[int, list[int]]:
    assignments: dict[int, list[int]] = {}
    for worker_rank in range(workers):
        assignments[worker_rank] = [shard_id for shard_id in range(len(shard_lists)) if shard_id % workers == worker_rank]
    return assignments


def worker_indices(assignments: dict[int, list[int]], shard_lists: list[list[int]], worker_rank: int) -> list[int]:
    indices: list[int] = []
    for shard_id in assignments[worker_rank]:
        indices.extend(shard_lists[shard_id])
    return indices


def train_worker_epoch(
    *,
    model: nn.Module,
    dataset: TensorDataset,
    indices: list[int],
    batch_size: int,
    lr: float,
) -> tuple[dict[str, torch.Tensor], float, float, int]:
    loader = DataLoader(Subset(dataset, indices), batch_size=batch_size, shuffle=False)
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)
    criterion = nn.CrossEntropyLoss()
    model.train()
    total_loss = 0.0
    total = 0
    correct = 0
    steps = 0
    for batch_x, batch_y in loader:
        optimizer.zero_grad()
        logits = model(batch_x)
        loss = criterion(logits, batch_y)
        loss.backward()
        optimizer.step()

        total_loss += float(loss.item()) * batch_x.size(0)
        total += batch_x.size(0)
        correct += int((torch.argmax(logits, dim=1) == batch_y).sum().item())
        steps += 1

    avg_loss = total_loss / total if total else 0.0
    avg_acc = correct / total if total else 0.0
    return ({k: v.detach().clone() for k, v in model.state_dict().items()}, avg_loss, avg_acc, steps)


def average_states(worker_states: list[dict[str, torch.Tensor]]) -> dict[str, torch.Tensor]:
    avg_state: dict[str, torch.Tensor] = {}
    for key in worker_states[0]:
        stacked = torch.stack([state[key] for state in worker_states], dim=0)
        avg_state[key] = stacked.mean(dim=0)
    return avg_state


def evaluate(model: nn.Module, dataset: TensorDataset, batch_size: int) -> tuple[float, float]:
    criterion = nn.CrossEntropyLoss()
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
    model.eval()
    total_loss = 0.0
    total = 0
    correct = 0
    with torch.no_grad():
        for batch_x, batch_y in loader:
            logits = model(batch_x)
            loss = criterion(logits, batch_y)
            total_loss += float(loss.item()) * batch_x.size(0)
            total += batch_x.size(0)
            correct += int((torch.argmax(logits, dim=1) == batch_y).sum().item())
    return total_loss / total, correct / total


def serializable_config(args: argparse.Namespace) -> dict[str, int | float | bool | str | None]:
    return {
        "workers": args.workers,
        "epochs": args.epochs,
        "samples": args.samples,
        "input_dim": args.input_dim,
        "num_classes": args.num_classes,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "seed": args.seed,
        "shards": args.shards,
        "out_dir": str(args.out_dir),
        "checkpoint_every": args.checkpoint_every,
        "resume": args.resume,
        "no_checkpoint": args.no_checkpoint,
    }


def write_checkpoint(
    *,
    checkpoints_dir: Path,
    epoch: int,
    global_step: int,
    args: argparse.Namespace,
    model: nn.Module,
    metrics: dict[str, Any],
    shard_assignments: dict[int, list[int]],
    worker_metrics: list[dict[str, Any]],
) -> Path:
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    ckpt_path = checkpoints_dir / f"epoch-{epoch:04d}.pt"
    state = {
        "global_model_state_dict": model.state_dict(),
        "epoch": epoch,
        "global_step": global_step,
        "seed": args.seed,
        "config": serializable_config(args),
        "shard_assignments": shard_assignments,
        "worker_metrics": worker_metrics,
        "metrics": metrics,
    }
    torch.save(state, ckpt_path)

    latest = {
        "checkpoint_path": ckpt_path.name,
        "epoch": epoch,
        "global_step": global_step,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (checkpoints_dir / "latest.json").write_text(json.dumps(latest, indent=2), encoding="utf-8")
    return ckpt_path


def restore_latest(checkpoints_dir: Path, model: nn.Module) -> tuple[int, int]:
    latest_path = checkpoints_dir / "latest.json"
    if not latest_path.exists():
        raise RuntimeError(f"--resume requested but latest checkpoint metadata not found at {latest_path}")
    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    ckpt_path = checkpoints_dir / latest["checkpoint_path"]
    if not ckpt_path.exists():
        raise RuntimeError(f"--resume requested but checkpoint file is missing: {ckpt_path}")
    try:
        checkpoint = torch.load(ckpt_path, map_location="cpu", weights_only=True)
    except TypeError:
        checkpoint = torch.load(ckpt_path, map_location="cpu")
    model.load_state_dict(checkpoint["global_model_state_dict"])
    return int(checkpoint["epoch"]), int(checkpoint.get("global_step", 0))


def main() -> None:
    args = parse_args()
    require_torch()

    shards = args.shards if args.shards is not None else args.workers
    if args.workers < 1 or shards < 1:
        raise RuntimeError("--workers and --shards must be >= 1")
    if args.checkpoint_every < 1:
        raise RuntimeError("--checkpoint-every must be >= 1")

    torch.manual_seed(args.seed)
    dataset = build_dataset(args.samples, args.input_dim, args.num_classes, args.seed)
    shard_lists = build_shards(args.samples, shards)
    assignments = assign_shards(args.workers, shard_lists)

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    shard_assignments_path = out_dir / "shard_assignments.json"
    shard_assignments_path.write_text(
        json.dumps({str(k): v for k, v in assignments.items()}, indent=2), encoding="utf-8"
    )

    global_model = build_model(args.input_dim, args.num_classes)
    checkpoints_dir = out_dir / "checkpoints"
    resumed_from_checkpoint = False
    start_epoch = 1
    global_step = 0
    if args.resume:
        restored_epoch, global_step = restore_latest(checkpoints_dir, global_model)
        resumed_from_checkpoint = True
        start_epoch = restored_epoch + 1

    final_loss = 0.0
    final_accuracy = 0.0
    final_worker_metrics: list[dict[str, Any]] = []
    latest_ckpt_path: str | None = None

    for epoch in range(start_epoch, args.epochs + 1):
        worker_states: list[dict[str, torch.Tensor]] = []
        epoch_worker_metrics: list[dict[str, Any]] = []
        epoch_steps = 0

        for worker_rank in range(args.workers):
            local_model = build_model(args.input_dim, args.num_classes)
            local_model.load_state_dict(global_model.state_dict())
            indices = worker_indices(assignments, shard_lists, worker_rank)
            worker_state, worker_loss, worker_acc, worker_steps = train_worker_epoch(
                model=local_model,
                dataset=dataset,
                indices=indices,
                batch_size=args.batch_size,
                lr=args.lr,
            )
            worker_states.append(worker_state)
            epoch_steps += worker_steps
            epoch_worker_metrics.append(
                {
                    "worker_rank": worker_rank,
                    "assigned_shards": assignments[worker_rank],
                    "samples_seen": len(indices),
                    "last_train_loss": worker_loss,
                    "last_train_accuracy": worker_acc,
                }
            )

        global_model.load_state_dict(average_states(worker_states))
        global_step += epoch_steps
        final_loss, final_accuracy = evaluate(global_model, dataset, args.batch_size)
        final_worker_metrics = epoch_worker_metrics
        print(f"epoch={epoch} loss={final_loss:.4f} accuracy={final_accuracy:.4f} global_step={global_step}")

        if not args.no_checkpoint and epoch % args.checkpoint_every == 0:
            ckpt_path = write_checkpoint(
                checkpoints_dir=checkpoints_dir,
                epoch=epoch,
                global_step=global_step,
                args=args,
                model=global_model,
                metrics={"loss": final_loss, "accuracy": final_accuracy},
                shard_assignments=assignments,
                worker_metrics=epoch_worker_metrics,
            )
            latest_ckpt_path = str(ckpt_path)

    metrics = {
        "epochs_requested": args.epochs,
        "final_epoch": args.epochs,
        "workers": args.workers,
        "shards": shards,
        "samples": args.samples,
        "batch_size": args.batch_size,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
        "resumed_from_checkpoint": resumed_from_checkpoint,
        "checkpoint_path": latest_ckpt_path,
        "global_step": global_step,
        "worker_metrics": final_worker_metrics,
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
