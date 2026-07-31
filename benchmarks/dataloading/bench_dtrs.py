#!/usr/bin/env python3
"""A/B benchmark: DTRS shard decode/load — pure Python vs dtruntime._native.

Measures the paths a training job actually uses:

  A. ShardReader.read_all()  pure Python  (DTRUNTIME_NATIVE=0 semantics)
  B. ShardReader.read_all()  Rust decode, zero-copy tensor handoff
  C. data pipeline records   pure Python  (DtrsFormat.iter_records)
  D. data pipeline records   Rust batch decode + row copies
  E. _native.ShardStream     Rust fetch + verify + decode with prefetch
     (file:// by default; s3:// when DTRUNTIME_S3_* + --s3-bucket given)

Honest-numbers notes: single process, local machine; run it yourself with
    python benchmarks/dataloading/bench_dtrs.py [--records 4096] [--dim 512]
        [--shards 8] [--repeats 3] [--s3-bucket BUCKET] [--out results.json]
Environment (endpoint/credentials for the s3 leg): DTRUNTIME_S3_ENDPOINT,
DTRUNTIME_S3_ACCESS_KEY, DTRUNTIME_S3_SECRET_KEY, DTRUNTIME_S3_REGION.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "python"))

from dtruntime.data.formats import get_format  # noqa: E402
from dtruntime.data.shard_reader import (  # noqa: E402
    ShardReader,
    generate_shard_bytes,
    native_module,
)


def _timed(fn, repeats: int) -> list[float]:
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return times


def _report(name: str, times: list[float], records: int, nbytes: int) -> dict:
    best = min(times)
    med = statistics.median(times)
    row = {
        "name": name,
        "best_s": round(best, 4),
        "median_s": round(med, 4),
        "records_per_s": round(records / best),
        "mb_per_s": round(nbytes / best / 1e6, 1),
    }
    print(
        f"{name:<38} best {best:8.3f}s  median {med:8.3f}s  "
        f"{row['records_per_s']:>10,} rec/s  {row['mb_per_s']:>8.1f} MB/s"
    )
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=4096, help="records per shard")
    ap.add_argument("--dim", type=int, default=512, help="feature dimension")
    ap.add_argument("--shards", type=int, default=8)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--prefetch", type=int, default=3)
    ap.add_argument("--s3-bucket", default=None, help="also run the s3:// ShardStream leg")
    ap.add_argument("--out", default=None, help="write results JSON here")
    args = ap.parse_args()

    if native_module() is None:
        print(
            "dtruntime._native is not built - run: python scripts/build_native.py",
            file=sys.stderr,
        )
        return 1
    from dtruntime import _native

    tmp = Path(tempfile.mkdtemp(prefix="dtrs-bench-"))
    paths: list[Path] = []
    checksums: list[str] = []
    total_bytes = 0
    for i in range(args.shards):
        data = generate_shard_bytes(
            record_count=args.records, input_dim=args.dim, num_classes=100, seed=i
        )
        p = tmp / f"shard-{i:04d}.bin"
        p.write_bytes(data)
        paths.append(p)
        checksums.append(hashlib.sha256(data).hexdigest())
        total_bytes += len(data)
    total_records = args.records * args.shards
    print(
        f"{args.shards} shards x {args.records} records x dim {args.dim} "
        f"({total_bytes / 1e6:.1f} MB total), best of {args.repeats}\n"
    )

    results = []

    def read_all_python():
        os.environ["DTRUNTIME_NATIVE"] = "0"
        try:
            for p in paths:
                ShardReader(str(p)).read_all()
        finally:
            os.environ.pop("DTRUNTIME_NATIVE", None)

    def read_all_native():
        for p in paths:
            ShardReader(str(p)).read_all()

    fmt = get_format("dtrs")

    def pipeline_python():
        os.environ["DTRUNTIME_NATIVE"] = "0"
        try:
            for p in paths:
                for _ in fmt.iter_records(p):
                    pass
        finally:
            os.environ.pop("DTRUNTIME_NATIVE", None)

    def pipeline_native():
        for p in paths:
            for _ in fmt.iter_records(p):
                pass

    def stream_native_files():
        specs = [(str(p), c) for p, c in zip(paths, checksums)]
        for _ in _native.ShardStream(specs, prefetch=args.prefetch):
            pass

    results.append(
        _report("read_all / pure python", _timed(read_all_python, args.repeats), total_records, total_bytes)
    )
    results.append(
        _report("read_all / native zero-copy", _timed(read_all_native, args.repeats), total_records, total_bytes)
    )
    results.append(
        _report("pipeline records / pure python", _timed(pipeline_python, args.repeats), total_records, total_bytes)
    )
    results.append(
        _report("pipeline records / native", _timed(pipeline_native, args.repeats), total_records, total_bytes)
    )
    results.append(
        _report(
            f"ShardStream file:// (prefetch={args.prefetch}, sha256 verify)",
            _timed(stream_native_files, args.repeats),
            total_records,
            total_bytes,
        )
    )

    if args.s3_bucket:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("DTRUNTIME_S3_ENDPOINT"),
            region_name=os.environ.get("DTRUNTIME_S3_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("DTRUNTIME_S3_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("DTRUNTIME_S3_SECRET_KEY"),
        )
        keys = []
        for p in paths:
            key = f"bench/{p.name}"
            client.upload_file(str(p), args.s3_bucket, key)
            keys.append(key)
        specs = [
            (f"s3://{args.s3_bucket}/{k}", c) for k, c in zip(keys, checksums)
        ]

        def stream_native_s3():
            for _ in _native.ShardStream(specs, prefetch=args.prefetch):
                pass

        results.append(
            _report(
                f"ShardStream s3:// (prefetch={args.prefetch}, sha256 verify)",
                _timed(stream_native_s3, args.repeats),
                total_records,
                total_bytes,
            )
        )
        for k in keys:
            client.delete_object(Bucket=args.s3_bucket, Key=k)

    if args.out:
        payload = {
            "config": vars(args),
            "total_records": total_records,
            "total_bytes": total_bytes,
            "results": results,
        }
        Path(args.out).write_text(json.dumps(payload, indent=2))
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
