from __future__ import annotations

import argparse
import json
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
) -> tuple[float, float]:
    model.train()
    total_loss = 0.0
    correct = 0
    total = 0
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

    return total_loss / total, correct / total


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


def main() -> None:
    args = parse_args()
    require_torch()

    torch.manual_seed(args.seed)
    dataset = build_dataset(args.samples, args.input_dim, args.num_classes, args.seed)
    train_loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=False)

    model = build_model(args.input_dim, args.num_classes)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr)
    criterion = nn.CrossEntropyLoss()

    final_loss = 0.0
    final_accuracy = 0.0
    for epoch in range(1, args.epochs + 1):
        train_loss, train_accuracy = train_one_epoch(model, train_loader, optimizer, criterion)
        final_loss, final_accuracy = evaluate(model, train_loader, criterion)
        print(
            f"epoch={epoch} train_loss={train_loss:.4f} train_accuracy={train_accuracy:.4f} "
            f"loss={final_loss:.4f} accuracy={final_accuracy:.4f}"
        )

    checkpoints_dir = args.out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    # Phase 18B will save model/optimizer/scheduler state through runtime checkpoint flow.

    metrics = {
        "epochs": args.epochs,
        "samples": args.samples,
        "input_dim": args.input_dim,
        "num_classes": args.num_classes,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
    }
    metrics_path = save_metrics(args.out_dir, metrics)
    print(f"wrote metrics: {metrics_path}")


if __name__ == "__main__":
    main()
