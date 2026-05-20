from __future__ import annotations

import argparse
import hashlib
import json
import socket
import subprocess
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from dtruntime.client import connect

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, Subset, TensorDataset
except ImportError:  # pragma: no cover
    torch = None
    nn = None
    DataLoader = None
    Subset = None
    TensorDataset = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Coordinator-driven local PyTorch orchestration example")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--samples", type=int, default=128)
    parser.add_argument("--input-dim", type=int, default=16)
    parser.add_argument("--num-classes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--out-dir", type=Path, default=Path(".dtruntime-pytorch-coordinator"))
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-checkpoint", action="store_true")
    parser.add_argument("--addr", type=str, default=None)
    parser.add_argument("--start-coordinator", action="store_true")
    parser.add_argument("--shards", type=int, default=None)
    return parser.parse_args()


def require_torch() -> None:
    if torch is None:
        raise RuntimeError("PyTorch is required. Install torch to run pytorch_coordinator_train.py")


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
        "out_dir": str(args.out_dir),
        "checkpoint_every": args.checkpoint_every,
        "resume": args.resume,
        "no_checkpoint": args.no_checkpoint,
        "addr": args.addr,
        "start_coordinator": args.start_coordinator,
        "shards": args.shards,
    }


def build_dataset(samples: int, input_dim: int, num_classes: int, seed: int) -> TensorDataset:
    generator = torch.Generator().manual_seed(seed)
    features = torch.randn(samples, input_dim, generator=generator)
    weights = torch.randn(input_dim, num_classes, generator=generator)
    logits = features @ weights
    targets = torch.argmax(logits, dim=1)
    return TensorDataset(features, targets)


def build_model(input_dim: int, num_classes: int) -> nn.Module:
    return nn.Sequential(nn.Linear(input_dim, 32), nn.ReLU(), nn.Linear(32, num_classes))


def build_shards(samples: int, shard_count: int) -> dict[str, list[int]]:
    out = {f"shard-{idx:04d}": [] for idx in range(shard_count)}
    keys = list(out.keys())
    for sample_idx in range(samples):
        out[keys[sample_idx % shard_count]].append(sample_idx)
    return out


def shard_payload(shard_map: dict[str, list[int]]) -> list[dict[str, Any]]:
    payload = []
    for shard_id, indices in shard_map.items():
        payload.append({"shard_id": shard_id, "shard_uri": f"synthetic://{shard_id}", "size_bytes": len(indices)})
    return payload


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def maybe_start_coordinator(args: argparse.Namespace) -> Iterator[tuple[str, subprocess.Popen[Any] | None]]:
    should_start = args.addr is None or args.start_coordinator
    if not should_start:
        yield args.addr, None
        return
    port = find_free_port()
    addr = f"127.0.0.1:{port}"
    proc = subprocess.Popen(["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", addr])
    try:
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                with connect(addr) as client:
                    client.restore_latest("warmup")
                break
            except Exception:
                time.sleep(0.2)
        yield addr, proc
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def train_worker_epoch(model: nn.Module, dataset: TensorDataset, indices: list[int], batch_size: int, optimizer: torch.optim.Optimizer, criterion: nn.Module) -> tuple[float, float, int]:
    loader = DataLoader(Subset(dataset, indices), batch_size=batch_size, shuffle=False)
    total_loss = 0.0
    total = 0
    correct = 0
    steps = 0
    model.train()
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
    return (total_loss / total if total else 0.0, correct / total if total else 0.0, steps)


def evaluate(model: nn.Module, dataset: TensorDataset, batch_size: int) -> tuple[float, float]:
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=False)
    criterion = nn.CrossEntropyLoss()
    total_loss = 0.0
    total = 0
    correct = 0
    model.eval()
    with torch.no_grad():
        for batch_x, batch_y in loader:
            logits = model(batch_x)
            loss = criterion(logits, batch_y)
            total_loss += float(loss.item()) * batch_x.size(0)
            total += batch_x.size(0)
            correct += int((torch.argmax(logits, dim=1) == batch_y).sum().item())
    return total_loss / total, correct / total


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    require_torch()
    if args.checkpoint_every < 1:
        raise RuntimeError("--checkpoint-every must be >= 1")

    shard_count = args.shards if args.shards is not None else args.workers * 2
    torch.manual_seed(args.seed)
    dataset = build_dataset(args.samples, args.input_dim, args.num_classes, args.seed)
    shard_map = build_shards(args.samples, shard_count)
    checkpoints_dir = args.out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)
    latest_local = checkpoints_dir / "latest.json"

    model = build_model(args.input_dim, args.num_classes)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.95)
    criterion = nn.CrossEntropyLoss()

    start_epoch = 1
    global_step = 0
    resumed_from_checkpoint = False
    restored_checkpoint_id: str | None = None

    if args.resume and latest_local.exists():
        latest = json.loads(latest_local.read_text(encoding="utf-8"))
        ckpt_path = checkpoints_dir / latest["checkpoint_path"]
        checkpoint = torch.load(ckpt_path, map_location="cpu", weights_only=True)
        model.load_state_dict(checkpoint["model_state_dict"])
        optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
        scheduler.load_state_dict(checkpoint["scheduler_state_dict"])
        start_epoch = int(checkpoint["epoch"]) + 1
        global_step = int(checkpoint.get("global_step", 0))
        resumed_from_checkpoint = True
        restored_checkpoint_id = latest.get("checkpoint_id")

    run_id = f"pt-coordinator-{args.seed}-{int(time.time())}"
    dataset_id = f"dataset-{args.seed}"
    worker_ids = [f"worker-{i}" for i in range(args.workers)]

    checkpoint_id: str | None = None
    checkpoint_path: str | None = None
    final_loss = 0.0
    final_accuracy = 0.0
    assignment_map: dict[str, list[str]] = {}
    worker_metrics: dict[str, dict[str, float | int]] = {}
    rpc_summary = {"registered_workers": 0, "heartbeat_count": 0, "shard_assignment_requests": 0, "checkpoints_committed": 0}

    with maybe_start_coordinator(args) as (coordinator_addr, _proc):
        with connect(coordinator_addr) as client:
            client.register_dataset(run_id=run_id, name="synthetic", uri="synthetic://dataset", format="tensor", shards=shard_payload(shard_map), dataset_id=dataset_id)
            for rank, worker_id in enumerate(worker_ids):
                client.register_worker(run_id=run_id, worker_id=worker_id, rank=rank, host="localhost")
                rpc_summary["registered_workers"] += 1

            for epoch in range(start_epoch, args.epochs + 1):
                for worker_id in worker_ids:
                    assignment = client.request_shard(run_id=run_id, worker_id=worker_id, dataset_id=dataset_id)
                    assigned = [item.value for item in assignment.shard_ids]
                    assignment_map[worker_id] = assigned
                    rpc_summary["shard_assignment_requests"] += 1
                    all_indices: list[int] = []
                    for shard_id in assigned:
                        all_indices.extend(shard_map.get(shard_id, []))
                    if not all_indices:
                        continue
                    loss, acc, steps = train_worker_epoch(model, dataset, all_indices, args.batch_size, optimizer, criterion)
                    global_step += steps
                    worker_metrics[worker_id] = {"loss": loss, "accuracy": acc, "steps": steps, "epoch": epoch}
                    client.heartbeat(run_id=run_id, worker_id=worker_id, active_shard_ids=assigned)
                    rpc_summary["heartbeat_count"] += 1

                final_loss, final_accuracy = evaluate(model, dataset, args.batch_size)
                scheduler.step()

                if not args.no_checkpoint and (epoch % args.checkpoint_every == 0):
                    ckpt_file = checkpoints_dir / f"epoch-{epoch:04d}.pt"
                    state = {
                        "model_state_dict": model.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                        "scheduler_state_dict": scheduler.state_dict(),
                        "epoch": epoch,
                        "global_step": global_step,
                        "config": serializable_config(args),
                        "worker_metrics": worker_metrics,
                        "assignment_map": assignment_map,
                        "final_loss": final_loss,
                        "final_accuracy": final_accuracy,
                    }
                    torch.save(state, ckpt_file)
                    begin = client.begin_checkpoint(run_id=run_id, worker_id=worker_ids[0], step=global_step)
                    checkpoint_id = begin.checkpoint_id.value
                    client.report_checkpoint_artifact(
                        run_id=run_id,
                        worker_id=worker_ids[0],
                        checkpoint_id=checkpoint_id,
                        artifact_name=ckpt_file.name,
                        artifact_uri=str(ckpt_file.resolve()),
                        size_bytes=ckpt_file.stat().st_size,
                        checksum=sha256_file(ckpt_file),
                        checksum_algorithm="sha256",
                    )
                    client.commit_checkpoint(run_id=run_id, checkpoint_id=checkpoint_id)
                    rpc_summary["checkpoints_committed"] += 1
                    latest = client.restore_latest(run_id=run_id)
                    latest_local.write_text(
                        json.dumps(
                            {
                                "checkpoint_path": ckpt_file.name,
                                "epoch": epoch,
                                "global_step": global_step,
                                "checkpoint_id": latest.checkpoint_id.value or checkpoint_id,
                                "created_at": datetime.now(UTC).isoformat(),
                            },
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    checkpoint_path = str(ckpt_file)
                    restored_checkpoint_id = latest.checkpoint_id.value or checkpoint_id

    args.out_dir.mkdir(parents=True, exist_ok=True)
    metrics = {
        "epochs_requested": args.epochs,
        "final_epoch": args.epochs,
        "workers": args.workers,
        "shards": shard_count,
        "samples": args.samples,
        "batch_size": args.batch_size,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
        "resumed_from_checkpoint": resumed_from_checkpoint,
        "checkpoint_id": checkpoint_id,
        "latest_checkpoint_id": restored_checkpoint_id,
        "checkpoint_path": checkpoint_path,
        "global_step": global_step,
        "coordinator_addr": args.addr or "local-subprocess",
        "worker_metrics": worker_metrics,
        "shard_assignments": assignment_map,
        "coordinator_rpc_summary": rpc_summary,
    }
    (args.out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
