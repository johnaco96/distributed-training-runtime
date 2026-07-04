from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset
except ImportError:  # pragma: no cover - exercised in no-torch environments
    torch = None
    nn = None
    DataLoader = None
    TensorDataset = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal deterministic local PyTorch training example")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--lr", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--samples", type=int, default=256)
    parser.add_argument("--input-dim", type=int, default=16)
    parser.add_argument("--num-classes", type=int, default=2)
    parser.add_argument("--out-dir", type=Path, default=Path(".dtruntime-pytorch"))
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-checkpoint", action="store_true")
    return parser.parse_args()


def require_torch() -> None:
    if torch is None:
        raise RuntimeError(
            "PyTorch is required for python/examples/pytorch_train.py. "
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
    return nn.Sequential(
        nn.Linear(input_dim, 32),
        nn.ReLU(),
        nn.Linear(32, num_classes),
    )


def train_one_epoch(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    criterion: nn.Module,
) -> tuple[float, float, int]:
    model.train()
    total_loss = 0.0
    correct = 0
    total = 0
    steps = 0
    for batch_x, batch_y in loader:
        optimizer.zero_grad()
        logits = model(batch_x)
        loss = criterion(logits, batch_y)
        loss.backward()
        optimizer.step()

        total_loss += float(loss.item()) * batch_x.size(0)
        preds = torch.argmax(logits, dim=1)
        correct += int((preds == batch_y).sum().item())
        total += batch_x.size(0)
        steps += 1

    return total_loss / total, correct / total, steps


def evaluate(model: nn.Module, loader: DataLoader, criterion: nn.Module) -> tuple[float, float]:
    model.eval()
    total_loss = 0.0
    correct = 0
    total = 0
    with torch.no_grad():
        for batch_x, batch_y in loader:
            logits = model(batch_x)
            loss = criterion(logits, batch_y)
            total_loss += float(loss.item()) * batch_x.size(0)
            preds = torch.argmax(logits, dim=1)
            correct += int((preds == batch_y).sum().item())
            total += batch_x.size(0)

    return total_loss / total, correct / total


def save_metrics(out_dir: Path, metrics: dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_path = out_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return metrics_path


def checkpoint_path_for_epoch(checkpoints_dir: Path, epoch: int) -> Path:
    return checkpoints_dir / f"epoch-{epoch:04d}.pt"


def serializable_config(args: argparse.Namespace) -> dict[str, int | float | bool | str | None]:
    return {
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "seed": args.seed,
        "samples": args.samples,
        "input_dim": args.input_dim,
        "num_classes": args.num_classes,
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
    optimizer: torch.optim.Optimizer,
    scheduler: torch.optim.lr_scheduler.StepLR,
    train_loss: float,
    eval_loss: float,
    eval_accuracy: float,
) -> Path:
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    ckpt_path = checkpoint_path_for_epoch(checkpoints_dir, epoch)
    state = {
        "model_state_dict": model.state_dict(),
        "optimizer_state_dict": optimizer.state_dict(),
        "scheduler_state_dict": scheduler.state_dict(),
        "epoch": epoch,
        "global_step": global_step,
        "seed": args.seed,
        "config": serializable_config(args),
        "train_loss": train_loss,
        "eval_loss": eval_loss,
        "eval_accuracy": eval_accuracy,
    }
    torch.save(state, ckpt_path)

    latest = {
        "checkpoint_path": ckpt_path.name,
        "epoch": epoch,
        "global_step": global_step,
        "final_train_loss": train_loss,
        "final_loss": eval_loss,
        "final_accuracy": eval_accuracy,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    (checkpoints_dir / "latest.json").write_text(json.dumps(latest, indent=2), encoding="utf-8")
    return ckpt_path


def restore_latest(
    checkpoints_dir: Path,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    scheduler: torch.optim.lr_scheduler.StepLR,
) -> tuple[int, int]:
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
    model.load_state_dict(checkpoint["model_state_dict"])
    optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
    scheduler.load_state_dict(checkpoint["scheduler_state_dict"])
    return int(checkpoint["epoch"]), int(checkpoint.get("global_step", 0))


def main() -> None:
    args = parse_args()
    require_torch()

    if args.checkpoint_every < 1:
        raise RuntimeError("--checkpoint-every must be >= 1")

    torch.manual_seed(args.seed)
    dataset = build_dataset(args.samples, args.input_dim, args.num_classes, args.seed)
    train_loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=False)

    model = build_model(args.input_dim, args.num_classes)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.95)
    criterion = nn.CrossEntropyLoss()

    resumed_from_checkpoint = False
    start_epoch = 1
    global_step = 0
    checkpoints_dir = args.out_dir / "checkpoints"
    if args.resume:
        restored_epoch, global_step = restore_latest(checkpoints_dir, model, optimizer, scheduler)
        resumed_from_checkpoint = True
        start_epoch = restored_epoch + 1

    final_train_loss = 0.0
    final_loss = 0.0
    final_accuracy = 0.0
    latest_ckpt_path: str | None = None

    for epoch in range(start_epoch, args.epochs + 1):
        train_loss, train_accuracy, steps = train_one_epoch(model, train_loader, optimizer, criterion)
        global_step += steps
        final_train_loss = train_loss
        final_loss, final_accuracy = evaluate(model, train_loader, criterion)
        scheduler.step()
        print(
            f"epoch={epoch} train_loss={train_loss:.4f} train_accuracy={train_accuracy:.4f} "
            f"loss={final_loss:.4f} accuracy={final_accuracy:.4f}"
        )
        if not args.no_checkpoint and (epoch % args.checkpoint_every == 0):
            ckpt_path = write_checkpoint(
                checkpoints_dir=checkpoints_dir,
                epoch=epoch,
                global_step=global_step,
                args=args,
                model=model,
                optimizer=optimizer,
                scheduler=scheduler,
                train_loss=train_loss,
                eval_loss=final_loss,
                eval_accuracy=final_accuracy,
            )
            latest_ckpt_path = str(ckpt_path)

    metrics = {
        "epochs_requested": args.epochs,
        "samples": args.samples,
        "batch_size": args.batch_size,
        "input_dim": args.input_dim,
        "num_classes": args.num_classes,
        "final_epoch": args.epochs,
        "final_train_loss": final_train_loss,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
        "resumed_from_checkpoint": resumed_from_checkpoint,
        "checkpoint_path": latest_ckpt_path,
        "global_step": global_step,
    }
    metrics_path = save_metrics(args.out_dir, metrics)
    print(f"wrote metrics: {metrics_path}")


if __name__ == "__main__":
    main()
