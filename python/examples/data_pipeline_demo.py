"""End-to-end data pipeline demo (single process, no torch.distributed).

Registers a content-addressed dataset with the coordinator, then streams it
through a ``DistributedShardDataset`` + PyTorch ``DataLoader`` across two
epochs, showing deterministic epoch shuffling, the disk cache warming, and
per-shard progress reporting.

Run (start a coordinator first, or let this script spawn one)::

    PYTHONPATH=python python python/examples/data_pipeline_demo.py \
        --coordinator localhost:50051 --epochs 2 --batch-size 16

If ``--coordinator`` is omitted the script starts its own coordinator on a
free port and tears it down at the end.
"""

from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _spawn_coordinator() -> tuple[subprocess.Popen, str]:
    import grpc

    address = f"127.0.0.1:{_free_port()}"
    proc = subprocess.Popen(
        ["cargo", "run", "-p", "runtime-coordinator", "--", "--addr", address],
        cwd=REPO_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        ch = grpc.insecure_channel(address)
        try:
            grpc.channel_ready_future(ch).result(timeout=1)
            return proc, address
        except Exception:
            pass
        finally:
            ch.close()
    raise RuntimeError("coordinator did not start")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coordinator", default=None)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--samples", type=int, default=256)
    parser.add_argument("--shards", type=int, default=8)
    parser.add_argument("--num-workers", type=int, default=0)
    args = parser.parse_args()

    from dtruntime import connect
    from dtruntime.data import DistributedShardDataset
    from dtruntime.data.formats import features_label_transform
    from dtruntime.data.versioning import register_versioned_dataset
    from dtruntime.generated import runtime_pb2
    from dtruntime.shard_loader import build_and_write_shards
    from torch.utils.data import DataLoader

    proc = None
    address = args.coordinator
    if address is None:
        print("no --coordinator given; starting a local one ...")
        proc, address = _spawn_coordinator()

    tmp = Path.cwd() / ".dtruntime-data-demo"
    tmp.mkdir(exist_ok=True)
    client = connect(address)
    try:
        suffix = uuid.uuid4().hex[:8]
        run_id = client.register_run(
            run_name=f"data-demo-{suffix}",
            world_size=1,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            idempotency_key=f"run-{suffix}",
        ).run_id.value
        client.register_worker(run_id=run_id, worker_id="w0", rank=0)

        features = np.random.rand(args.samples, 32).astype(np.float32)
        labels = np.random.randint(0, 10, args.samples).astype(np.int64)
        shards = build_and_write_shards(tmp / "shards", features, labels, args.shards)
        _, version = register_versioned_dataset(
            client, run_id, "demo", f"file://{tmp / 'shards'}", "npz", shards,
            dataset_id="dataset", idempotency_key=f"ds-{suffix}",
        )
        print(f"registered dataset version {version}")

        ds = DistributedShardDataset(
            client=client, run_id=run_id, worker_id="w0", dataset_id="dataset",
            dataset_format="npz", shuffle=True, seed=1234, shuffle_buffer=64,
            cache_dir=str(tmp / "cache"), prefetch_shards=2,
            transform=features_label_transform,
        )
        loader = DataLoader(ds, batch_size=args.batch_size, num_workers=args.num_workers)

        first_epoch_order = None
        for epoch in range(args.epochs):
            ds.set_epoch(epoch)
            started = time.monotonic()
            n_records, order = 0, []
            for features_b, labels_b in loader:
                n_records += len(labels_b)
                order.extend(labels_b.tolist()[:4])  # sample of order
            elapsed = time.monotonic() - started
            cache = ds.cache
            print(
                f"epoch {epoch}: {n_records} records in {elapsed:.2f}s | "
                f"cache hits={cache.hits} misses={cache.misses} "
                f"bytes={cache.total_bytes} | order-head={order[:4]}"
            )
            if first_epoch_order is None:
                first_epoch_order = order
            elif order == first_epoch_order:
                print("  note: order identical to epoch 0 (unexpected for shuffle)")
        print("done.")
        return 0
    finally:
        client.close()
        if proc is not None and proc.poll() is None:
            proc.terminate()


if __name__ == "__main__":
    sys.exit(main())
