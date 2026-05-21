from __future__ import annotations

import argparse
import csv
import json
import os
import socket
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import torch
from torch import nn

from dtruntime.client import RuntimeClient


@dataclass
class BenchmarkResult:
    total_duration_seconds: float
    total_steps: int
    samples_per_second: float
    step_time_p50_ms: float
    step_time_p95_ms: float
    step_time_p99_ms: float
    checkpoint_count: int
    checkpoint_bytes_total: int
    checkpoint_write_seconds_total: float
    checkpoint_write_p50_ms: float | None
    checkpoint_write_p95_ms: float | None
    checkpoint_write_p99_ms: float | None
    training_stall_seconds_total: float
    stall_percent: float
    checkpoint_throughput_mb_s: float
    restore_latest_seconds: float | None
    worker_id: str
    shard_count: int
    backend: str
    mode: str


def percentile_ms(values: list[float], q: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    idx = int(round((len(s) - 1) * q))
    return s[idx] * 1000.0


def calculate_stall_percent(stall_seconds: float, total_seconds: float) -> float:
    if total_seconds <= 0:
        return 0.0
    return (stall_seconds / total_seconds) * 100.0


def write_checkpoint_bytes(path: Path, bytes_count: int) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = os.urandom(bytes_count)
    path.write_bytes(payload)
    return path.stat().st_size


def _write_s3_object(key: str, bytes_count: int) -> int:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("DTRUNTIME_S3_ENDPOINT"),
        region_name=os.getenv("DTRUNTIME_S3_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("DTRUNTIME_S3_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("DTRUNTIME_S3_SECRET_ACCESS_KEY"),
    )
    payload = os.urandom(bytes_count)
    client.put_object(Bucket=os.environ["DTRUNTIME_S3_BUCKET"], Key=key, Body=payload)
    return len(payload)


def run_training(args: argparse.Namespace) -> BenchmarkResult:
    torch.manual_seed(args.seed)
    model = nn.Sequential(nn.Linear(args.input_dim, 64), nn.ReLU(), nn.Linear(64, args.num_classes))
    opt = torch.optim.SGD(model.parameters(), lr=0.01)
    loss_fn = nn.CrossEntropyLoss()

    features = torch.randn(args.samples, args.input_dim)
    labels = torch.randint(0, args.num_classes, (args.samples,))

    step_times: list[float] = []
    checkpoint_times: list[float] = []
    checkpoint_bytes_total = 0
    stall_total = 0.0
    checkpoint_count = 0
    restore_latest_seconds: float | None = None

    runtime_client: RuntimeClient | None = None
    runtime_run_id = args.run_id
    dataset_id = ""
    worker_host = socket.gethostname()
    if args.mode == "dtruntime-managed":
        coordinator_addr = os.getenv("DTRUNTIME_COORDINATOR_ADDR", "127.0.0.1:50051")
        runtime_client = RuntimeClient(coordinator_addr)
        run_response = runtime_client.register_run(
            run_name=args.run_id,
            world_size=1,
            assignment_policy=runtime_client._runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=(
                runtime_client._runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM
                if args.backend == "localfs"
                else runtime_client._runtime_pb2.STORAGE_BACKEND_KIND_S3_COMPATIBLE
            ),
            idempotency_key=f"bench-register-run-{args.run_id}",
        )
        runtime_run_id = run_response.run_id.value
        runtime_client.register_worker(
            run_id=runtime_run_id,
            worker_id=args.worker_id,
            rank=0,
            host=worker_host,
            idempotency_key=f"bench-register-worker-{args.run_id}-{args.worker_id}",
        )
        shards = [{"shard_id": f"shard-{idx}", "shard_uri": f"synthetic://{args.run_id}/shard-{idx}"} for idx in range(args.shard_count)]
        dataset_response = runtime_client.register_dataset(
            run_id=runtime_run_id,
            name="synthetic-training-dataset",
            uri=f"synthetic://{args.run_id}/dataset",
            format="tensor",
            shards=shards,
            dataset_id=f"dataset-{args.run_id}",
            idempotency_key=f"bench-register-dataset-{args.run_id}",
        )
        dataset_id = dataset_response.dataset_id.value
        runtime_client.request_shard(
            run_id=runtime_run_id,
            worker_id=args.worker_id,
            dataset_id=dataset_id,
            idempotency_key=f"bench-shard-assignment-{args.run_id}-{args.worker_id}",
        )

    total_steps = args.steps
    start = time.perf_counter()
    for step in range(1, total_steps + 1):
        s0 = time.perf_counter()
        idx = ((step - 1) * args.batch_size) % args.samples
        batch_x = features[idx : idx + args.batch_size]
        batch_y = labels[idx : idx + args.batch_size]
        out = model(batch_x)
        loss = loss_fn(out, batch_y)
        opt.zero_grad()
        loss.backward()
        opt.step()
        step_times.append(time.perf_counter() - s0)

        if step % args.checkpoint_every_steps == 0:
            c0 = time.perf_counter()
            key = f"{args.run_id}/worker-{args.worker_id}/step-{step}.bin"
            checkpoint_id = ""
            if runtime_client is not None:
                begin = runtime_client.begin_checkpoint(
                    run_id=runtime_run_id,
                    worker_id=args.worker_id,
                    step=step,
                    checkpoint_id=f"ckpt-{args.run_id}-{step}",
                    idempotency_key=f"bench-begin-checkpoint-{args.run_id}-{args.worker_id}-{step}",
                )
                checkpoint_id = begin.checkpoint_id.value
            if args.backend == "minio":
                written = _write_s3_object(key, args.checkpoint_bytes)
            else:
                written = write_checkpoint_bytes(Path(args.output_dir) / "checkpoints" / key, args.checkpoint_bytes)
            if runtime_client is not None:
                runtime_client.report_checkpoint_artifact(
                    run_id=runtime_run_id,
                    worker_id=args.worker_id,
                    checkpoint_id=checkpoint_id,
                    artifact_name=f"step-{step}",
                    artifact_uri=key,
                    size_bytes=written,
                    idempotency_key=f"bench-report-artifact-{args.run_id}-{args.worker_id}-{step}",
                )
                runtime_client.commit_checkpoint(
                    run_id=runtime_run_id,
                    checkpoint_id=checkpoint_id,
                    idempotency_key=f"bench-commit-checkpoint-{args.run_id}-{step}",
                )
            c_elapsed = time.perf_counter() - c0
            checkpoint_times.append(c_elapsed)
            checkpoint_bytes_total += written
            checkpoint_count += 1
            stall_total += c_elapsed
    total_duration = time.perf_counter() - start

    if runtime_client is not None:
        restore_start = time.perf_counter()
        runtime_client.restore_latest(run_id=runtime_run_id)
        restore_latest_seconds = time.perf_counter() - restore_start
        runtime_client.close()

    ckpt_seconds = sum(checkpoint_times)
    throughput = (checkpoint_bytes_total / (1024 * 1024) / ckpt_seconds) if ckpt_seconds > 0 else 0.0
    return BenchmarkResult(
        total_duration_seconds=total_duration,
        total_steps=total_steps,
        samples_per_second=(args.samples / total_duration) if total_duration > 0 else 0.0,
        step_time_p50_ms=percentile_ms(step_times, 0.50) or 0.0,
        step_time_p95_ms=percentile_ms(step_times, 0.95) or 0.0,
        step_time_p99_ms=percentile_ms(step_times, 0.99) or 0.0,
        checkpoint_count=checkpoint_count,
        checkpoint_bytes_total=checkpoint_bytes_total,
        checkpoint_write_seconds_total=ckpt_seconds,
        checkpoint_write_p50_ms=percentile_ms(checkpoint_times, 0.50),
        checkpoint_write_p95_ms=percentile_ms(checkpoint_times, 0.95),
        checkpoint_write_p99_ms=percentile_ms(checkpoint_times, 0.99),
        training_stall_seconds_total=stall_total,
        stall_percent=calculate_stall_percent(stall_total, total_duration),
        checkpoint_throughput_mb_s=throughput,
        restore_latest_seconds=restore_latest_seconds,
        worker_id=args.worker_id,
        shard_count=args.shard_count,
        backend=args.backend,
        mode=args.mode,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["baseline-blocking", "dtruntime-managed"], required=True)
    p.add_argument("--backend", choices=["localfs", "minio"], default="localfs")
    p.add_argument("--samples", type=int, default=1000)
    p.add_argument("--steps", type=int, default=100)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--checkpoint-every-steps", type=int, default=10)
    p.add_argument("--checkpoint-bytes", type=int, default=1024 * 1024)
    p.add_argument("--input-dim", type=int, default=32)
    p.add_argument("--num-classes", type=int, default=10)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--run-id", default="bench-run")
    p.add_argument("--worker-id", default="0")
    p.add_argument("--shard-count", type=int, default=1)
    p.add_argument("--output-dir", default=".dtruntime-results/e2e-local")
    p.add_argument("--json-out", default=None)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    result = run_training(args)
    payload = asdict(result)
    out = json.dumps(payload, indent=2)
    print(out)
    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(out + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
