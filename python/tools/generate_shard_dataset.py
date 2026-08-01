#!/usr/bin/env python3
"""
Generate a synthetic DTRS binary shard dataset and upload it to S3/MinIO.

Each shard is a flat binary file (DTRS v1 format) containing ``--records``
float32 input vectors of length ``--input-dim`` paired with int32 class labels.

Usage (MinIO example)::

    python python/tools/generate_shard_dataset.py \\
        --shards 4 --records 2000 --input-dim 128 --num-classes 10 \\
        --s3-endpoint http://localhost:9100 \\
        --s3-bucket dtruntime-checkpoints \\
        --s3-access-key minioadmin --s3-secret-key minioadmin \\
        --prefix datasets/v1 \\
        --seed 42

Outputs one JSON line per shard (shard_id, shard_uri, size_bytes) so the
benchmark can pipe it straight into ``register_dataset``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parents[2] / "python"))

from dtruntime.data.shard_reader import (
    ensure_s3_bucket,
    generate_shard_bytes,
    upload_shard_to_s3,
)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate DTRS binary shards and upload to S3/MinIO.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--shards", type=int, default=4, help="Number of shard files to generate")
    p.add_argument("--records", type=int, default=2000, help="Records per shard")
    p.add_argument("--input-dim", type=int, default=128, help="Input feature dimension")
    p.add_argument("--num-classes", type=int, default=10, help="Number of label classes")
    p.add_argument("--seed", type=int, default=0, help="RNG seed (0 = unseeded)")
    p.add_argument("--s3-endpoint", default="http://localhost:9100")
    p.add_argument("--s3-bucket", default="dtruntime-checkpoints")
    p.add_argument("--s3-access-key", default="minioadmin")
    p.add_argument("--s3-secret-key", default="minioadmin")
    p.add_argument("--s3-region", default="us-east-1")
    p.add_argument(
        "--prefix",
        default="datasets/v1",
        help="S3 key prefix under which shards are stored",
    )
    p.add_argument(
        "--quiet", action="store_true", help="Suppress progress output; only emit JSON"
    )
    return p.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)

    if not args.quiet:
        print(
            f"[generate] {args.shards} shards × {args.records} records "
            f"× {args.input_dim}-dim → s3://{args.s3_bucket}/{args.prefix}/",
            file=sys.stderr,
        )

    # Ensure bucket exists (MinIO does not auto-create).
    ensure_s3_bucket(
        bucket=args.s3_bucket,
        s3_endpoint=args.s3_endpoint,
        s3_access_key=args.s3_access_key,
        s3_secret_key=args.s3_secret_key,
        s3_region=args.s3_region,
    )

    results = []
    t0 = time.perf_counter()
    for i in range(args.shards):
        seed = (args.seed * 31 + i) if args.seed else None
        data = generate_shard_bytes(
            record_count=args.records,
            input_dim=args.input_dim,
            num_classes=args.num_classes,
            seed=seed,
        )
        key = f"{args.prefix}/shard-{i:04d}.bin"
        uri = upload_shard_to_s3(
            data=data,
            bucket=args.s3_bucket,
            key=key,
            s3_endpoint=args.s3_endpoint,
            s3_access_key=args.s3_access_key,
            s3_secret_key=args.s3_secret_key,
            s3_region=args.s3_region,
        )
        record = {
            "shard_id": f"shard-{i:04d}",
            "shard_uri": uri,
            "size_bytes": len(data),
        }
        results.append(record)
        if not args.quiet:
            mb = len(data) / (1024 * 1024)
            print(f"[generate]   shard-{i:04d}  {mb:.2f} MiB  → {uri}", file=sys.stderr)

    elapsed = time.perf_counter() - t0
    total_mib = sum(r["size_bytes"] for r in results) / (1024 * 1024)
    if not args.quiet:
        print(
            f"[generate] done: {total_mib:.2f} MiB in {elapsed:.2f}s "
            f"({total_mib/elapsed:.1f} MiB/s)",
            file=sys.stderr,
        )

    # Emit JSON to stdout for downstream piping.
    print(json.dumps({"shards": results, "input_dim": args.input_dim, "num_classes": args.num_classes}))


if __name__ == "__main__":
    main()
