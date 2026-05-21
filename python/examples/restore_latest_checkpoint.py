from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from dtruntime.client import RuntimeClient


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Restore latest checkpoint metadata and validate artifact existence.")
    p.add_argument("--run-id", required=True)
    p.add_argument("--expect-backend", choices=["localfs", "minio"], default="localfs")
    p.add_argument("--local-root", default=".dtruntime-results")
    p.add_argument("--json-out", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    addr = os.getenv("DTRUNTIME_COORDINATOR_ADDR", "127.0.0.1:50051")
    with RuntimeClient(addr) as client:
        response = client.restore_latest(run_id=args.run_id)

    manifest = response.manifest
    if not manifest or not manifest.checkpoint_id.value:
        raise SystemExit(f"restore_latest returned no checkpoint for run_id={args.run_id}")
    if not manifest.artifacts:
        raise SystemExit(f"restore_latest returned checkpoint without artifacts for run_id={args.run_id}")

    artifact_uri = manifest.artifacts[0].artifact_uri
    artifacts = [
        {
            "artifact_name": artifact.artifact_name,
            "artifact_uri": artifact.artifact_uri,
            "size_bytes": artifact.size_bytes,
            "checksum": artifact.checksum,
            "checksum_algorithm": artifact.checksum_algorithm,
        }
        for artifact in manifest.artifacts
    ]
    verification: dict[str, object] = {
        "run_id": manifest.run_id.value,
        "checkpoint_id": manifest.checkpoint_id.value,
        "step": manifest.step,
        "artifact_uri": artifact_uri,
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "storage_backend_kind": manifest.storage_backend_kind,
    }

    if args.expect_backend == "localfs":
        artifact_path = Path(args.local_root) / "checkpoints" / artifact_uri
        exists = artifact_path.exists()
        verification["artifact_path"] = str(artifact_path)
        verification["artifact_exists"] = exists
        if not exists:
            raise SystemExit(f"localfs artifact missing: {artifact_path}")
    else:
        if not artifact_uri:
            raise SystemExit("minio restore metadata missing artifact_uri key")

    payload = json.dumps(verification, indent=2)
    print(payload)
    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
