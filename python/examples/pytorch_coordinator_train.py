"""Coordinator-driven distributed PyTorch training.

This script demonstrates the full distributed training pipeline:

  Dataset on disk → coordinator assigns shards → workers read real data
  → train → checkpoint → recover on restart

Two operating modes
-------------------
Multi-worker (default, ``--rank`` omitted)
    One process simulates *N* workers sequentially. Shards are written to
    ``--shard-dir`` (or ``<out-dir>/shards``), the dataset is registered,
    and all workers are driven in a loop.  Good for local correctness
    testing without k8s.

Single-worker (``--rank R``)
    This process represents exactly one worker with the given rank.
    Shard files are expected to already exist in ``--shard-dir`` (written
    by rank 0 or a prep job) before ranks 1+ start.  All *world_size*
    workers register the dataset with the same idempotency key — the
    coordinator deduplicates and returns the same result to everyone.
    This is the mode used by the k8s Indexed Job.

Usage examples
--------------
Local multi-worker (starts its own coordinator)::

    python python/examples/pytorch_coordinator_train.py \\
        --workers 4 --epochs 3 --samples 256 --shards 8

Single-worker (k8s pod — coordinator already running)::

    # Rank 0: writes shards + trains
    python python/examples/pytorch_coordinator_train.py \\
        --addr runtime-coordinator:50051 \\
        --workers 4 --rank 0 --epochs 3 --samples 256 --shards 8 \\
        --shard-dir /mnt/shards

    # Ranks 1-3: read existing shards + train
    python python/examples/pytorch_coordinator_train.py \\
        --addr runtime-coordinator:50051 \\
        --workers 4 --rank 1 --epochs 3 --samples 256 --shards 8 \\
        --shard-dir /mnt/shards
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import numpy as np

from dtruntime.client import connect
from dtruntime.shard_loader import ShardLoader, build_and_write_shards
from dtruntime.generated import runtime_pb2

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset
except ImportError:  # pragma: no cover
    torch = None
    nn = None
    DataLoader = None
    TensorDataset = None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Dataset / model geometry
    parser.add_argument("--workers", type=int, default=2,
                        help="Total number of workers (world size).")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--samples", type=int, default=128,
                        help="Total training samples across all workers.")
    parser.add_argument("--input-dim", type=int, default=16)
    parser.add_argument("--num-classes", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--shards", type=int, default=None,
                        help="Number of shards (default: workers * 2).")
    parser.add_argument("--shard-dir", type=Path, default=None,
                        help="Directory for shard .npz files. "
                             "Defaults to <out-dir>/shards. "
                             "In single-worker mode, rank 0 writes here "
                             "and other ranks read from here.")
    # Coordinator connection
    parser.add_argument("--addr", type=str, default=None,
                        help="Coordinator address (host:port). "
                             "Omit to start a local subprocess.")
    parser.add_argument("--start-coordinator", action="store_true",
                        help="Force-start a local coordinator subprocess "
                             "even when --addr is given.")
    # Single-worker mode
    parser.add_argument("--rank", type=int, default=None,
                        help="This worker's rank (0-indexed). "
                             "When set, runs as a single distributed worker "
                             "rather than simulating all workers in one process.")
    # Checkpoint / resume
    parser.add_argument("--checkpoint-every", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--no-checkpoint", action="store_true")
    parser.add_argument("--out-dir", type=Path,
                        default=Path(".dtruntime-pytorch-coordinator"))
    # Data-prep only mode (used by shard-prep-job.yaml)
    parser.add_argument("--prep-only", action="store_true",
                        help="Write shard files to --shard-dir and register the "
                             "dataset with the coordinator, then exit. "
                             "Does not require PyTorch. "
                             "Run this before the Indexed-Job worker pods start.")
    return parser.parse_args()


def require_torch() -> None:
    if torch is None:
        raise RuntimeError(
            "PyTorch is required. Install it with: pip install torch"
        )


# ---------------------------------------------------------------------------
# Dataset helpers
# ---------------------------------------------------------------------------

def build_dataset(
    samples: int, input_dim: int, num_classes: int, seed: int
) -> tuple["np.ndarray", "np.ndarray"]:
    """Build a synthetic classification dataset and return numpy arrays."""
    rng = np.random.default_rng(seed)
    features = rng.standard_normal((samples, input_dim)).astype(np.float32)
    weights = rng.standard_normal((input_dim, num_classes)).astype(np.float32)
    logits = features @ weights
    labels = logits.argmax(axis=1).astype(np.int64)
    return features, labels


def build_model(input_dim: int, num_classes: int) -> "nn.Module":
    return nn.Sequential(
        nn.Linear(input_dim, 32), nn.ReLU(), nn.Linear(32, num_classes)
    )


# ---------------------------------------------------------------------------
# Training helpers
# ---------------------------------------------------------------------------

def train_on_tensors(
    model: "nn.Module",
    features: "torch.Tensor",
    labels: "torch.Tensor",
    batch_size: int,
    optimizer: "torch.optim.Optimizer",
    criterion: "nn.Module",
) -> tuple[float, float, int]:
    """Run one pass over the given tensors. Returns (loss, accuracy, steps)."""
    if features.shape[0] == 0:
        return 0.0, 0.0, 0

    loader = DataLoader(
        TensorDataset(features, labels),
        batch_size=batch_size,
        shuffle=False,
    )
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
    return (
        total_loss / total if total else 0.0,
        correct / total if total else 0.0,
        steps,
    )


def evaluate(
    model: "nn.Module",
    features: "torch.Tensor",
    labels: "torch.Tensor",
    batch_size: int,
) -> tuple[float, float]:
    loader = DataLoader(
        TensorDataset(features, labels), batch_size=batch_size, shuffle=False
    )
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
    return (
        total_loss / total if total else 0.0,
        correct / total if total else 0.0,
    )


def shard_metadata_from_existing(shard_dir: Path) -> list[dict]:
    """Reconstruct shard metadata from ``.npz`` files that already exist on disk.

    Used by rank-0 in single-worker mode when the shard files were written by a
    separate shard-prep-job rather than by this process.  The metadata is
    identical to what ``build_and_write_shards`` would have returned.

    Args:
        shard_dir: Directory containing ``*.npz`` shard files.

    Returns:
        List of dicts suitable for ``RuntimeClient.register_dataset(shards=...)``.
    """
    shards = []
    for path in sorted(shard_dir.glob("*.npz")):
        data = path.read_bytes()
        checksum = hashlib.sha256(data).hexdigest()
        size_bytes = len(data)
        shards.append({
            "shard_id": path.stem,
            "shard_uri": f"file://{path.resolve()}",
            "size_bytes": size_bytes,
            "byte_range_start": 0,
            "byte_range_end": size_bytes,
            "checksum": checksum,
            "checksum_algorithm": "sha256",
        })
    return shards


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Coordinator lifecycle
# ---------------------------------------------------------------------------

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@contextmanager
def maybe_start_coordinator(
    args: argparse.Namespace,
) -> Iterator[tuple[str, "subprocess.Popen[Any] | None"]]:
    should_start = args.addr is None or args.start_coordinator
    if not should_start:
        yield args.addr, None
        return
    port = find_free_port()
    addr = f"127.0.0.1:{port}"
    # Prefer the pre-built release binary so we never pay a cargo compilation
    # round-trip (which can take 30+ s and blow the readiness timeout).
    # Use the platform-specific executable suffix: on Windows the extension-
    # less file (if present) is a WSL/Linux build and is not runnable here.
    _exe_suffix = ".exe" if os.name == "nt" else ""
    _project_root = Path(__file__).resolve().parent.parent.parent
    _release_bin = (
        _project_root / "target" / "release" / f"runtime-coordinator{_exe_suffix}"
    )
    if _release_bin.exists():
        coordinator_cmd = [str(_release_bin), "--addr", addr]
    else:
        # Fall back to cargo run --release when no binary is present (CI/fresh clone).
        coordinator_cmd = [
            "cargo", "run", "--release", "-p", "runtime-coordinator",
            "--", "--addr", addr,
        ]
    proc = subprocess.Popen(
        coordinator_cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        import grpc

        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with connect(addr) as client:
                    client.restore_latest("warmup")
                break
            except grpc.RpcError as exc:
                # Any structured gRPC status other than UNAVAILABLE (e.g.
                # NOT_FOUND for the "warmup" run) proves the server is up
                # and answering RPCs — that's all this probe needs.
                if exc.code() is not grpc.StatusCode.UNAVAILABLE:
                    break
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"Coordinator subprocess exited before becoming ready "
                        f"(returncode={proc.returncode})"
                    )
                time.sleep(0.2)
            except Exception:
                if proc.poll() is not None:
                    raise RuntimeError(
                        f"Coordinator subprocess exited before becoming ready "
                        f"(returncode={proc.returncode})"
                    )
                time.sleep(0.2)
        else:
            raise RuntimeError(
                f"Timed out waiting for coordinator to be ready at {addr}"
            )
        yield addr, proc
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def wait_for_dataset(
    client: Any,
    run_id: str,
    dataset_id: str,
    timeout_seconds: float = 120.0,
    worker_id: str = "probe",
) -> None:
    """Block until the coordinator has the dataset registered.

    Used by ranks > 0 in single-worker mode: rank 0 writes and registers
    the dataset; other ranks poll until it appears.

    ``worker_id`` must be a worker that is already registered with the run
    so the coordinator accepts the shard-assignment probe call.  Pass the
    caller's own worker_id rather than the default ``"probe"`` placeholder.
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            # A successful shard assignment proves the dataset exists.
            # Empty assignments (pool exhausted) still count — any non-error
            # response means the coordinator knows about this dataset.
            client.request_shard(
                run_id=run_id,
                worker_id=worker_id,
                dataset_id=dataset_id,
                idempotency_key="",
            )
            return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError(
        f"Dataset {dataset_id!r} not registered within {timeout_seconds}s. "
        "Check that rank-0 or the data-prep job completed successfully."
    )


# ---------------------------------------------------------------------------
# Prep-only mode (shard-prep-job.yaml)
# ---------------------------------------------------------------------------

def run_prep_only(args: argparse.Namespace, coordinator_addr: str) -> None:
    """Write shard files and register the dataset, then exit.

    This function does not require PyTorch.  It is the entry-point for the
    ``shard-prep-job.yaml`` Kubernetes Job, which runs once before any
    training worker pods start.

    Args:
        args: Parsed CLI arguments.
        coordinator_addr: Host:port of the running coordinator.
    """
    shard_count = args.shards if args.shards is not None else args.workers * 2
    shard_dir = args.shard_dir or (args.out_dir / "shards")
    idempotency_suffix = f"{args.seed}-{args.workers}-{args.samples}-{args.epochs}"
    run_name = f"pt-coordinator-{args.seed}"
    dataset_id = f"dataset-{args.seed}"

    shard_dir.mkdir(parents=True, exist_ok=True)
    print(f"[prep] building dataset ({args.samples} samples, {shard_count} shards) …")
    features_all, labels_all = build_dataset(
        args.samples, args.input_dim, args.num_classes, args.seed
    )
    shard_metadata = build_and_write_shards(
        shard_dir, features_all, labels_all, shard_count
    )
    print(f"[prep] wrote {len(shard_metadata)} shard files to {shard_dir}")

    with connect(coordinator_addr) as client:
        run_resp = client.register_run(
            idempotency_key=f"pt-register-run-{idempotency_suffix}",
            run_name=run_name,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            world_size=args.workers,
            run_version="v1",
        )
        run_id = run_resp.run_id.value
        print(f"[prep] run_id={run_id}")

        client.register_dataset(
            run_id=run_id,
            name="training-shards",
            uri=str(shard_dir.resolve()),
            format="npz",
            shards=shard_metadata,
            dataset_id=dataset_id,
            dataset_version="v1",
            idempotency_key=f"pt-dataset-{idempotency_suffix}",
        )
        print(
            f"[prep] dataset registered: {len(shard_metadata)} shards "
            f"(run_id={run_id}, dataset_id={dataset_id})"
        )
        print(
            f"[prep] workers can start: "
            f"--rank R --workers {args.workers} --shards {shard_count} "
            f"--shard-dir {shard_dir}"
        )


# ---------------------------------------------------------------------------
# Multi-worker mode (all workers in one process)
# ---------------------------------------------------------------------------

def run_multi_worker(args: argparse.Namespace, coordinator_addr: str) -> dict:
    """Simulate all workers from one process — used for local testing."""
    shard_count = args.shards if args.shards is not None else args.workers * 2
    shard_dir = args.shard_dir or (args.out_dir / "shards")
    checkpoints_dir = args.out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    # Build dataset and write real shard files
    print(f"[train] building dataset ({args.samples} samples, {shard_count} shards) ...")
    features_all, labels_all = build_dataset(
        args.samples, args.input_dim, args.num_classes, args.seed
    )
    shard_metadata = build_and_write_shards(
        shard_dir, features_all, labels_all, shard_count
    )
    print(f"[train] wrote {len(shard_metadata)} shard files to {shard_dir}")

    idempotency_suffix = (
        f"{args.seed}-{args.workers}-{args.samples}-{args.epochs}"
    )
    run_name = f"pt-coordinator-{args.seed}"
    dataset_id = f"dataset-{args.seed}"
    logical_worker_ids = [f"worker-{i}" for i in range(args.workers)]

    model = build_model(args.input_dim, args.num_classes)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.95)
    criterion = nn.CrossEntropyLoss()
    features_tensor = torch.from_numpy(features_all)
    labels_tensor = torch.from_numpy(labels_all).long()

    start_epoch = 1
    global_step = 0
    checkpoint_id: str | None = None
    final_loss = 0.0
    final_accuracy = 0.0
    assignment_map: dict[str, list[str]] = {}
    worker_metrics: dict[str, dict] = {}
    rpc_summary = {
        "registered_workers": 0,
        "heartbeat_count": 0,
        "shard_assignment_requests": 0,
        "checkpoints_committed": 0,
    }
    latest_checkpoint_metadata: dict | None = None
    latest_local = checkpoints_dir / "latest.json"

    # Resume from checkpoint if requested
    restored_checkpoint_id: str | None = None
    if args.resume and latest_local.exists():
        latest = json.loads(latest_local.read_text(encoding="utf-8"))
        ckpt_paths = latest.get("checkpoint_paths") or (
            [latest["checkpoint_path"]] if "checkpoint_path" in latest else []
        )
        if not ckpt_paths:
            raise RuntimeError("latest.json missing checkpoint_paths")
        ckpt = torch.load(
            checkpoints_dir / ckpt_paths[0], map_location="cpu", weights_only=True
        )
        model.load_state_dict(ckpt["model_state_dict"])
        optimizer.load_state_dict(ckpt["optimizer_state_dict"])
        scheduler.load_state_dict(ckpt["scheduler_state_dict"])
        start_epoch = int(ckpt["epoch"]) + 1
        global_step = int(ckpt.get("global_step", 0))
        restored_checkpoint_id = latest.get("checkpoint_id")
        print(f"[train] resumed from checkpoint at epoch {start_epoch - 1}")

    with connect(coordinator_addr) as admin_client:
        # Security: control-plane calls (register_run/register_dataset/commit)
        # use the admin identity; data-plane calls (register_worker/shards/
        # heartbeats) use per-worker scoped identities.  When auth metadata is
        # configured, each simulated worker gets its own worker-scoped client;
        # otherwise all workers share the admin channel (local dev).
        auth_active = bool(
            os.getenv("DTRUNTIME_AUTH_SUBJECT") and os.getenv("DTRUNTIME_AUTH_ROLE")
        )
        worker_role = os.getenv("DTRUNTIME_WORKER_AUTH_ROLE", "worker")
        worker_clients = {
            worker_id: (
                admin_client.for_worker(worker_id=worker_id, worker_role=worker_role)
                if auth_active
                else admin_client
            )
            for worker_id in logical_worker_ids
        }

        # Register run
        run_resp = admin_client.register_run(
            idempotency_key=f"pt-register-run-{idempotency_suffix}",
            run_name=run_name,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            world_size=args.workers,
            run_version="v1",
        )
        run_id = run_resp.run_id.value
        print(f"[train] run_id={run_id}")

        # Register dataset with real file:// URIs
        admin_client.register_dataset(
            run_id=run_id,
            name="training-shards",
            uri=str(shard_dir.resolve()),
            format="npz",
            shards=shard_metadata,
            dataset_id=dataset_id,
            dataset_version="v1",
            idempotency_key=f"pt-dataset-{idempotency_suffix}",
        )
        print(f"[train] dataset registered: {len(shard_metadata)} shards")

        # Register all workers under their own identities
        worker_ids: list[str] = []
        for rank, worker_id in enumerate(logical_worker_ids):
            registered = worker_clients[worker_id].register_worker(
                run_id=run_id,
                worker_id=worker_id,
                rank=rank,
                host="localhost",
                idempotency_key=f"pt-register-worker-{rank}-{idempotency_suffix}",
            )
            actual_id = registered.worker_id.value or worker_id
            worker_ids.append(actual_id)
            if actual_id != worker_id:
                worker_clients[actual_id] = worker_clients[worker_id]
            rpc_summary["registered_workers"] += 1

        # Training loop
        for epoch in range(start_epoch, args.epochs + 1):
            print(f"[train] epoch {epoch}/{args.epochs}")
            for worker_id in worker_ids:
                loader = ShardLoader(
                    worker_clients[worker_id], run_id, worker_id, dataset_id
                )
                idem = f"pt-shard-{worker_id}-{epoch}-{idempotency_suffix}"
                shard_ids, feat, lab = loader.load_next_shard_as_tensors(
                    idempotency_key=idem
                )
                assignment_map[worker_id] = shard_ids
                rpc_summary["shard_assignment_requests"] += 1

                if feat.shape[0] == 0:
                    print(f"[train]   {worker_id}: no shards assigned, skipping")
                    continue

                loss, acc, steps = train_on_tensors(
                    model, feat, lab, args.batch_size, optimizer, criterion
                )
                global_step += steps
                worker_metrics[worker_id] = {
                    "loss": loss, "accuracy": acc, "steps": steps, "epoch": epoch,
                }
                print(
                    f"[train]   {worker_id}: shards={shard_ids} "
                    f"loss={loss:.4f} acc={acc:.4f} steps={steps}"
                )
                worker_clients[worker_id].heartbeat(
                    run_id=run_id,
                    worker_id=worker_id,
                    active_shard_ids=shard_ids,
                    idempotency_key=f"pt-hb-{worker_id}-{epoch}-{idempotency_suffix}",
                )
                rpc_summary["heartbeat_count"] += 1

            final_loss, final_accuracy = evaluate(
                model, features_tensor, labels_tensor, args.batch_size
            )
            scheduler.step()
            print(
                f"[train] epoch {epoch} done — "
                f"eval loss={final_loss:.4f} acc={final_accuracy:.4f}"
            )

            # Checkpoint
            if not args.no_checkpoint and (epoch % args.checkpoint_every == 0):
                state = {
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "scheduler_state_dict": scheduler.state_dict(),
                    "epoch": epoch,
                    "global_step": global_step,
                    "final_loss": final_loss,
                    "final_accuracy": final_accuracy,
                    "worker_metrics": worker_metrics,
                    "assignment_map": assignment_map,
                    # Stringify config values (argparse holds Path objects,
                    # which torch.load(weights_only=True) refuses to unpickle).
                    "config": {k: str(v) for k, v in vars(args).items()},
                }
                begin = worker_clients[worker_ids[0]].begin_checkpoint(
                    run_id=run_id,
                    worker_id=worker_ids[0],
                    step=global_step,
                    idempotency_key=f"pt-begin-ckpt-{epoch}-{idempotency_suffix}",
                )
                checkpoint_id = begin.checkpoint_id.value
                worker_ckpt_files: list[str] = []
                for wid in worker_ids:
                    wf = checkpoints_dir / f"epoch-{epoch:04d}-{wid}.pt"
                    torch.save({**state, "worker_id": wid}, wf)
                    worker_clients[wid].report_checkpoint_artifact(
                        run_id=run_id,
                        worker_id=wid,
                        checkpoint_id=checkpoint_id,
                        artifact_name=wf.name,
                        artifact_uri=str(wf.resolve()),
                        size_bytes=wf.stat().st_size,
                        checksum=sha256_file(wf),
                        checksum_algorithm="sha256",
                        idempotency_key=f"pt-report-ckpt-{epoch}-{wid}-{idempotency_suffix}",
                    )
                    worker_ckpt_files.append(wf.name)
                admin_client.commit_checkpoint(
                    run_id=run_id,
                    checkpoint_id=checkpoint_id,
                    idempotency_key=f"pt-commit-ckpt-{epoch}-{idempotency_suffix}",
                )
                rpc_summary["checkpoints_committed"] += 1
                latest_resp = admin_client.restore_latest(run_id=run_id)
                latest_checkpoint_metadata = {
                    "checkpoint_id": latest_resp.manifest.checkpoint_id.value,
                    "step": latest_resp.manifest.step,
                    "artifact_count": len(latest_resp.manifest.artifacts),
                }
                latest_local.write_text(
                    json.dumps({
                        "checkpoint_paths": worker_ckpt_files,
                        "epoch": epoch,
                        "global_step": global_step,
                        "checkpoint_id": checkpoint_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }, indent=2),
                    encoding="utf-8",
                )
                restored_checkpoint_id = checkpoint_id
                print(f"[train] checkpoint {checkpoint_id} committed")

    return {
        "mode": "multi-worker",
        "run_id": run_id,
        "epochs_completed": args.epochs,
        "final_epoch": args.epochs,
        "resumed_from_checkpoint": start_epoch > 1,
        "workers": args.workers,
        "shards": shard_count,
        "samples": args.samples,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
        "global_step": global_step,
        "checkpoint_id": checkpoint_id,
        "latest_checkpoint_id": restored_checkpoint_id,
        "coordinator_addr": coordinator_addr,
        "worker_metrics": worker_metrics,
        "shard_assignments": assignment_map,
        "coordinator_rpc_summary": rpc_summary,
        "latest_checkpoint_metadata": latest_checkpoint_metadata,
    }


# ---------------------------------------------------------------------------
# Single-worker mode (one pod = one rank)
# ---------------------------------------------------------------------------

def run_single_worker(args: argparse.Namespace, coordinator_addr: str) -> dict:
    """Run as one worker with rank ``args.rank``.

    Rank 0 writes all shard files and registers the dataset.
    All ranks register the run and their own worker, then train independently
    on their assigned subset of shards.
    """
    rank = args.rank  # guaranteed non-None here
    world_size = args.workers
    shard_count = args.shards if args.shards is not None else world_size * 2
    shard_dir = args.shard_dir or (args.out_dir / "shards")
    checkpoints_dir = args.out_dir / "checkpoints"
    checkpoints_dir.mkdir(parents=True, exist_ok=True)

    idempotency_suffix = (
        f"{args.seed}-{world_size}-{args.samples}-{args.epochs}"
    )
    run_name = f"pt-coordinator-{args.seed}"
    dataset_id = f"dataset-{args.seed}"
    worker_id = f"worker-{rank}"

    print(f"[rank {rank}/{world_size}] connecting to {coordinator_addr}")

    model = build_model(args.input_dim, args.num_classes)
    optimizer = torch.optim.SGD(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=1, gamma=0.95)
    criterion = nn.CrossEntropyLoss()

    start_epoch = 1
    global_step = 0
    checkpoint_id: str | None = None
    final_loss = 0.0
    final_accuracy = 0.0
    rpc_summary = {
        "registered_workers": 0,
        "heartbeat_count": 0,
        "shard_assignment_requests": 0,
        "checkpoints_committed": 0,
    }
    latest_checkpoint_metadata: dict | None = None
    latest_local = checkpoints_dir / "latest.json"
    restored_checkpoint_id: str | None = None

    with connect(coordinator_addr) as client:
        # All ranks register the run with the same idempotency key —
        # coordinator deduplicates and every caller gets the same run_id.
        run_resp = client.register_run(
            idempotency_key=f"pt-register-run-{idempotency_suffix}",
            run_name=run_name,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            world_size=world_size,
            run_version="v1",
        )
        run_id = run_resp.run_id.value
        print(f"[rank {rank}] run_id={run_id}")

        # Register this worker before dataset registration so shard assignment
        # is available as soon as the dataset exists.
        registered = client.register_worker(
            run_id=run_id,
            worker_id=worker_id,
            rank=rank,
            host=socket.gethostname(),
            idempotency_key=f"pt-register-worker-{rank}-{idempotency_suffix}",
        )
        actual_worker_id = registered.worker_id.value or worker_id
        rpc_summary["registered_workers"] += 1
        print(f"[rank {rank}] registered as worker_id={actual_worker_id}")

        # Rank 0: write shard files (if not already present) and register the dataset.
        # Idempotent: if a shard-prep-job already wrote the files, reuse them.
        # Other ranks: wait for the dataset to appear in the coordinator.
        if rank == 0:
            shard_dir.mkdir(parents=True, exist_ok=True)
            existing = sorted(shard_dir.glob("*.npz"))
            if existing:
                # Shards were written by a separate shard-prep-job; don't re-generate.
                print(
                    f"[rank 0] found {len(existing)} existing shard files in {shard_dir} "
                    "— skipping data generation"
                )
                shard_metadata = shard_metadata_from_existing(shard_dir)
            else:
                print(f"[rank 0] building dataset ({args.samples} samples) …")
                features_all, labels_all = build_dataset(
                    args.samples, args.input_dim, args.num_classes, args.seed
                )
                shard_metadata = build_and_write_shards(
                    shard_dir, features_all, labels_all, shard_count
                )
                print(f"[rank 0] wrote {len(shard_metadata)} shards to {shard_dir}")
            client.register_dataset(
                run_id=run_id,
                name="training-shards",
                uri=str(shard_dir.resolve()),
                format="npz",
                shards=shard_metadata,
                dataset_id=dataset_id,
                dataset_version="v1",
                idempotency_key=f"pt-dataset-{idempotency_suffix}",
            )
            print(f"[rank 0] dataset registered ({len(shard_metadata)} shards)")
        else:
            # Shards are pre-created on the shared volume (by shard-prep-job or
            # rank 0).  Re-registering with the same idempotency key is a
            # coordinator no-op if rank 0 already registered the dataset — no
            # polling probe needed (fixes the k8s worker-job dataset race).
            shard_metadata = shard_metadata_from_existing(shard_dir)
            client.register_dataset(
                run_id=run_id,
                name="training-shards",
                uri=str(shard_dir.resolve()),
                format="npz",
                shards=shard_metadata,
                dataset_id=dataset_id,
                dataset_version="v1",
                idempotency_key=f"pt-dataset-{idempotency_suffix}",
            )
            print(f"[rank {rank}] dataset available")

        # Resume from checkpoint if requested (rank 0 saves, all ranks load)
        if args.resume and latest_local.exists():
            latest = json.loads(latest_local.read_text(encoding="utf-8"))
            ckpt_key = f"epoch-????-worker-0.pt"
            ckpt_files = list(checkpoints_dir.glob("epoch-*-worker-0.pt"))
            if ckpt_files:
                ckpt_file = sorted(ckpt_files)[-1]
                ckpt = torch.load(ckpt_file, map_location="cpu", weights_only=True)
                model.load_state_dict(ckpt["model_state_dict"])
                optimizer.load_state_dict(ckpt["optimizer_state_dict"])
                scheduler.load_state_dict(ckpt["scheduler_state_dict"])
                start_epoch = int(ckpt["epoch"]) + 1
                global_step = int(ckpt.get("global_step", 0))
                restored_checkpoint_id = latest.get("checkpoint_id")
                print(f"[rank {rank}] resumed from {ckpt_file.name}")

        loader = ShardLoader(client, run_id, actual_worker_id, dataset_id)

        for epoch in range(start_epoch, args.epochs + 1):
            idem = f"pt-shard-{actual_worker_id}-{epoch}-{idempotency_suffix}"
            shard_ids, feat, lab = loader.load_next_shard_as_tensors(
                idempotency_key=idem
            )
            rpc_summary["shard_assignment_requests"] += 1

            if feat.shape[0] > 0:
                loss, acc, steps = train_on_tensors(
                    model, feat, lab, args.batch_size, optimizer, criterion
                )
                global_step += steps
                final_loss, final_accuracy = loss, acc
            else:
                loss, acc, steps = 0.0, 0.0, 0
                print(f"[rank {rank}] epoch {epoch}: no shards assigned")

            print(
                f"[rank {rank}] epoch {epoch}: "
                f"shards={shard_ids} loss={loss:.4f} acc={acc:.4f} steps={steps}"
            )
            client.heartbeat(
                run_id=run_id,
                worker_id=actual_worker_id,
                active_shard_ids=shard_ids,
                idempotency_key=f"pt-hb-{actual_worker_id}-{epoch}-{idempotency_suffix}",
            )
            rpc_summary["heartbeat_count"] += 1
            scheduler.step()

            # Checkpoint: rank 0 coordinates, all ranks save their state
            if (
                rank == 0
                and not args.no_checkpoint
                and (epoch % args.checkpoint_every == 0)
            ):
                begin = client.begin_checkpoint(
                    run_id=run_id,
                    worker_id=actual_worker_id,
                    step=global_step,
                    idempotency_key=f"pt-begin-ckpt-{epoch}-{idempotency_suffix}",
                )
                checkpoint_id = begin.checkpoint_id.value
                wf = checkpoints_dir / f"epoch-{epoch:04d}-{actual_worker_id}.pt"
                state = {
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "scheduler_state_dict": scheduler.state_dict(),
                    "epoch": epoch,
                    "global_step": global_step,
                    "rank": rank,
                    "worker_id": actual_worker_id,
                    "final_loss": final_loss,
                    "final_accuracy": final_accuracy,
                }
                torch.save(state, wf)
                client.report_checkpoint_artifact(
                    run_id=run_id,
                    worker_id=actual_worker_id,
                    checkpoint_id=checkpoint_id,
                    artifact_name=wf.name,
                    artifact_uri=str(wf.resolve()),
                    size_bytes=wf.stat().st_size,
                    checksum=sha256_file(wf),
                    checksum_algorithm="sha256",
                    idempotency_key=f"pt-report-ckpt-{epoch}-{actual_worker_id}-{idempotency_suffix}",
                )
                client.commit_checkpoint(
                    run_id=run_id,
                    checkpoint_id=checkpoint_id,
                    idempotency_key=f"pt-commit-ckpt-{epoch}-{idempotency_suffix}",
                )
                rpc_summary["checkpoints_committed"] += 1
                latest_resp = client.restore_latest(run_id=run_id)
                latest_checkpoint_metadata = {
                    "checkpoint_id": latest_resp.manifest.checkpoint_id.value,
                    "step": latest_resp.manifest.step,
                    "artifact_count": len(latest_resp.manifest.artifacts),
                }
                latest_local.write_text(
                    json.dumps({
                        "checkpoint_paths": [wf.name],
                        "epoch": epoch,
                        "global_step": global_step,
                        "checkpoint_id": checkpoint_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }, indent=2),
                    encoding="utf-8",
                )
                restored_checkpoint_id = checkpoint_id
                print(f"[rank {rank}] checkpoint {checkpoint_id} committed")

    return {
        "mode": "single-worker",
        "rank": rank,
        "world_size": world_size,
        "run_id": run_id,
        "worker_id": actual_worker_id,
        "epochs_completed": args.epochs,
        "final_epoch": args.epochs,
        "resumed_from_checkpoint": start_epoch > 1,
        "shards_assigned": shard_ids,
        "final_loss": final_loss,
        "final_accuracy": final_accuracy,
        "global_step": global_step,
        "checkpoint_id": checkpoint_id,
        "latest_checkpoint_id": restored_checkpoint_id,
        "coordinator_addr": coordinator_addr,
        "coordinator_rpc_summary": rpc_summary,
        "latest_checkpoint_metadata": latest_checkpoint_metadata,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    # --prep-only: write shards + register dataset, then exit (no torch needed).
    if args.prep_only:
        with maybe_start_coordinator(args) as (coordinator_addr, _proc):
            run_prep_only(args, coordinator_addr)
        return

    require_torch()
    if args.checkpoint_every < 1:
        raise RuntimeError("--checkpoint-every must be >= 1")

    with maybe_start_coordinator(args) as (coordinator_addr, _proc):
        if args.rank is not None:
            metrics = run_single_worker(args, coordinator_addr)
        else:
            metrics = run_multi_worker(args, coordinator_addr)

    out_file = args.out_dir / "metrics.json"
    out_file.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(f"\n[train] done — metrics written to {out_file}")
    print(
        f"[train] final loss={metrics['final_loss']:.4f}  "
        f"accuracy={metrics['final_accuracy']:.4f}"
    )


if __name__ == "__main__":
    main()
