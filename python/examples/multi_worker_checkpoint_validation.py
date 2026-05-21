from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from dtruntime.client import RuntimeClient


def _write_artifact(path: Path, worker_id: str, checkpoint_id: str) -> tuple[int, str]:
    payload = f"{checkpoint_id}:{worker_id}".encode("utf-8") + os.urandom(1024 * 1024)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    return len(payload), digest


def run_validation(args: argparse.Namespace) -> dict[str, Any]:
    client = RuntimeClient(args.coordinator_addr)
    workers = [f"worker-{rank}" for rank in range(args.world_size)]
    artifacts: list[dict[str, Any]] = []
    assignments: list[dict[str, Any]] = []

    run_response = client.register_run(
        run_name=args.run_id,
        world_size=args.world_size,
        assignment_policy=client._runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
        storage_backend_kind=client._runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
        idempotency_key=f"multi-register-run-{args.run_id}",
    )
    runtime_run_id = run_response.run_id.value

    shards = [
        {"shard_id": f"shard-{idx}", "shard_uri": f"synthetic://{runtime_run_id}/shard-{idx}"}
        for idx in range(args.shard_count)
    ]
    dataset_response = client.register_dataset(
        run_id=runtime_run_id,
        name="multi-worker-validation-dataset",
        uri=f"synthetic://{runtime_run_id}/dataset",
        format="tensor",
        shards=shards,
        dataset_id=f"dataset-{runtime_run_id}",
        idempotency_key=f"multi-register-dataset-{runtime_run_id}",
    )
    dataset_id = dataset_response.dataset_id.value

    for rank, worker_id in enumerate(workers):
        client.register_worker(
            run_id=runtime_run_id,
            worker_id=worker_id,
            rank=rank,
            host="localhost",
            idempotency_key=f"multi-register-worker-{runtime_run_id}-{worker_id}",
        )
        shard_response = client.request_shard(
            run_id=runtime_run_id,
            worker_id=worker_id,
            dataset_id=dataset_id,
            idempotency_key=f"multi-shard-{runtime_run_id}-{worker_id}",
        )
        assignments.append(
            {
                "worker_id": worker_id,
                "rank": rank,
                "assignments": [
                    {
                        "shard_id": s.shard_id.value,
                        "lease_epoch": s.lease_epoch,
                        "lease_state": int(s.lease_state),
                    }
                    for s in shard_response.assignments
                ],
            }
        )

    begin = client.begin_checkpoint(
        run_id=runtime_run_id,
        worker_id=workers[0],
        step=args.step,
        checkpoint_id=args.checkpoint_id,
        idempotency_key=f"multi-begin-{runtime_run_id}-{args.step}",
    )
    checkpoint_id = begin.checkpoint_id.value

    for rank, worker_id in enumerate(workers):
        artifact_name = f"rank-{rank}-step-{args.step}.bin"
        artifact_uri = f"{runtime_run_id}/{checkpoint_id}/{artifact_name}"
        local_path = Path(args.output_dir) / "checkpoints" / artifact_uri
        size_bytes, checksum = _write_artifact(local_path, worker_id, checkpoint_id)
        client.report_checkpoint_artifact(
            run_id=runtime_run_id,
            worker_id=worker_id,
            checkpoint_id=checkpoint_id,
            artifact_name=artifact_name,
            artifact_uri=artifact_uri,
            size_bytes=size_bytes,
            checksum=checksum,
            checksum_algorithm="sha256",
            idempotency_key=f"multi-artifact-{runtime_run_id}-{worker_id}-{args.step}",
        )
        artifacts.append(
            {
                "worker_id": worker_id,
                "rank": rank,
                "artifact_name": artifact_name,
                "artifact_uri": artifact_uri,
                "size_bytes": size_bytes,
                "checksum": checksum,
                "local_path": str(local_path),
                "exists": local_path.exists(),
            }
        )

    commit = client.commit_checkpoint(
        run_id=runtime_run_id,
        checkpoint_id=checkpoint_id,
        idempotency_key=f"multi-commit-{runtime_run_id}-{args.step}",
    )
    restore = client.restore_latest(runtime_run_id)
    restore_manifest = restore.manifest

    client.close()

    return {
        "requested_run_id": args.run_id,
        "runtime_run_id": runtime_run_id,
        "dataset_id": dataset_id,
        "checkpoint_id": checkpoint_id,
        "workers": workers,
        "ranks_shards": assignments,
        "artifacts": artifacts,
        "commit_status": int(commit.state),
        "restore_status": int(restore_manifest.state),
        "restore_latest": {
            "checkpoint_id": restore_manifest.checkpoint_id.value,
            "step": restore_manifest.step,
            "artifact_count": len(restore_manifest.artifacts),
            "artifacts": [
                {
                    "artifact_name": a.artifact_name,
                    "artifact_uri": a.artifact_uri,
                    "size_bytes": a.size_bytes,
                    "checksum": a.checksum,
                }
                for a in restore_manifest.artifacts
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--coordinator-addr", default=os.getenv("DTRUNTIME_COORDINATOR_ADDR", "127.0.0.1:50051"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--checkpoint-id", default="global-ckpt-step-10")
    parser.add_argument("--world-size", type=int, default=4)
    parser.add_argument("--shard-count", type=int, default=4)
    parser.add_argument("--step", type=int, default=10)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--json-out", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run_validation(args)
    out_path = Path(args.json_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
