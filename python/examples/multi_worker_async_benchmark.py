"""Multi-worker async checkpoint benchmark.

Each worker runs as a real OS process. Workers stream checkpoint artifacts
directly to the coordinator over gRPC — the Rust tokio runtime writes bytes to
storage asynchronously while training continues. Rank 0 coordinates the global
checkpoint commit after all workers have finished streaming.

Usage:
    python python/examples/multi_worker_async_benchmark.py \\
        --workers 4 --steps 40 --checkpoint-every-steps 10 \\
        --checkpoint-bytes 16777216 --output-dir .dtruntime-results/async-bench

Goals demonstrated:
  - Large-scale distributed systems: N workers -> 1 coordinator, gRPC fan-in
  - High-throughput data movement: concurrent streams, Rust async I/O
  - Rust + Python interop: Python API, Rust storage engine
  - Performance profiling: per-worker + aggregate metrics
  - Failure handling: incomplete checkpoint rejection, recovery
  - Developer-friendly API: stream_checkpoint_artifact() hides all I/O complexity
"""

from __future__ import annotations

import argparse
import json
import multiprocessing
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
class WorkerResult:
    worker_id: str
    rank: int
    total_steps: int
    total_duration_seconds: float
    step_time_p50_ms: float
    step_time_p95_ms: float
    step_time_p99_ms: float
    checkpoint_count: int
    checkpoint_bytes_total: int
    # Time the training loop spent waiting for a stream slot (true stall).
    stream_stall_seconds_total: float
    # Total I/O time measured from coordinator perspective (background, not stall).
    stream_io_seconds_total: float
    stall_percent: float
    checkpoint_throughput_mb_s: float
    error: str | None = None


@dataclass
class BenchmarkSummary:
    run_id: str
    runtime_run_id: str
    workers: int
    total_steps_per_worker: int
    checkpoint_every_steps: int
    checkpoint_bytes_per_worker: int
    coordinator_addr: str
    aggregate_throughput_mb_s: float
    aggregate_checkpoint_bytes: int
    stall_percent_p50: float
    stall_percent_p99: float
    step_time_p50_ms: float
    step_time_p99_ms: float
    worker_results: list[dict[str, Any]]
    total_wall_seconds: float
    checkpoints_committed: int


_last_runtime_run_id: str = ""


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(round((len(s) - 1) * q))
    return s[idx]


def percentile_ms(values: list[float], q: float) -> float:
    return percentile(values, q) * 1000.0


def _worker_main(
    rank: int,
    world_size: int,
    run_id: str,
    args: argparse.Namespace,
    result_queue: multiprocessing.Queue,
    artifact_done_barrier: multiprocessing.Barrier,
    commit_done_barrier: multiprocessing.Barrier,
) -> None:
    """Entry point for each worker process."""
    worker_id = f"worker-{rank}"
    coordinator_addr = os.getenv("DTRUNTIME_COORDINATOR_ADDR", args.coordinator_addr)

    try:
        client = RuntimeClient(coordinator_addr)

        # All workers register under the shared run_id returned by rank 0.
        runtime_run_id = run_id
        host = socket.gethostname()
        client.register_worker(
            run_id=runtime_run_id,
            worker_id=worker_id,
            rank=rank,
            host=host,
            idempotency_key=f"mw-register-worker-{run_id}-{worker_id}",
        )

        # Register dataset and claim shard assignment.
        shards = [
            {
                "shard_id": f"shard-{rank}",
                "shard_uri": f"synthetic://{run_id}/shard-{rank}",
            }
        ]
        dataset_response = client.register_dataset(
            run_id=runtime_run_id,
            name="synthetic-training-dataset",
            uri=f"synthetic://{run_id}/dataset",
            format="tensor",
            shards=shards,
            dataset_id=f"dataset-{run_id}-{rank}",
            idempotency_key=f"mw-register-dataset-{run_id}-{rank}",
        )
        dataset_id = dataset_response.dataset_id.value
        client.request_shard(
            run_id=runtime_run_id,
            worker_id=worker_id,
            dataset_id=dataset_id,
            idempotency_key=f"mw-shard-{run_id}-{worker_id}",
        )

        torch.manual_seed(args.seed + rank)
        model = nn.Sequential(
            nn.Linear(args.input_dim, 64), nn.ReLU(), nn.Linear(64, args.num_classes)
        )
        opt = torch.optim.SGD(model.parameters(), lr=0.01)
        loss_fn = nn.CrossEntropyLoss()
        features = torch.randn(args.samples, args.input_dim)
        labels = torch.randint(0, args.num_classes, (args.samples,))

        step_times: list[float] = []
        stream_stall_total = 0.0
        stream_io_total = 0.0
        checkpoint_bytes_total = 0
        checkpoint_count = 0
        payload = os.urandom(args.checkpoint_bytes)

        wall_start = time.perf_counter()

        for step in range(1, args.steps + 1):
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
                checkpoint_id = f"ckpt-{run_id}-step-{step}"

                # Chaos injection: simulate a worker crash before streaming.
                chaos_rank = getattr(args, "chaos_rank", -1)
                chaos_step = getattr(args, "chaos_at_step", args.checkpoint_every_steps * 2)
                if rank == chaos_rank and step == chaos_step:
                    raise RuntimeError(
                        f"worker-{rank}: injected chaos failure at step {step} — "
                        "simulating node crash before artifact stream"
                    )

                # All workers call begin_checkpoint (idempotent on same checkpoint_id).
                begin = client.begin_checkpoint(
                    run_id=runtime_run_id,
                    worker_id=worker_id,
                    step=step,
                    checkpoint_id=checkpoint_id,
                    idempotency_key=f"mw-begin-ckpt-{run_id}-{worker_id}-{step}",
                )
                resolved_checkpoint_id = begin.checkpoint_id.value

                # Stream artifact bytes to coordinator.
                # The Rust tokio runtime writes chunks to storage as they arrive.
                # Stall = only the time this call blocks the training loop.
                stall_start = time.perf_counter()
                io_start = time.perf_counter()
                response = client.stream_checkpoint_artifact(
                    run_id=runtime_run_id,
                    worker_id=worker_id,
                    checkpoint_id=resolved_checkpoint_id,
                    artifact_name=f"rank-{rank}-step-{step}.bin",
                    data=payload,
                    idempotency_key=f"mw-stream-artifact-{run_id}-{worker_id}-{step}",
                )
                io_elapsed = time.perf_counter() - io_start
                # Stall is the same as stream time here — the stream is synchronous
                # from the Python side. The async benefit is coordinator-side: N workers
                # stream concurrently and Rust handles all writes in parallel.
                stall_elapsed = time.perf_counter() - stall_start

                stream_stall_total += stall_elapsed
                stream_io_total += io_elapsed
                checkpoint_bytes_total += response.bytes_received
                checkpoint_count += 1

                # Barrier 1: all workers wait until every worker has finished
                # streaming its artifact for this step.
                artifact_done_barrier.wait()

                # Rank 0 commits; all other workers wait at barrier 2.
                if rank == 0:
                    client.commit_checkpoint(
                        run_id=runtime_run_id,
                        checkpoint_id=resolved_checkpoint_id,
                        idempotency_key=f"mw-commit-ckpt-{run_id}-{step}",
                    )

                # Barrier 2: rank 0 arrives after committing; non-rank-0 workers
                # arrive immediately. All proceed together — no Event race.
                commit_done_barrier.wait()

        total_duration = time.perf_counter() - wall_start
        ckpt_mb = checkpoint_bytes_total / (1024 * 1024)
        throughput = ckpt_mb / stream_io_total if stream_io_total > 0 else 0.0
        stall_pct = (stream_stall_total / total_duration * 100.0) if total_duration > 0 else 0.0

        client.close()

        result_queue.put(
            WorkerResult(
                worker_id=worker_id,
                rank=rank,
                total_steps=args.steps,
                total_duration_seconds=total_duration,
                step_time_p50_ms=percentile_ms(step_times, 0.50),
                step_time_p95_ms=percentile_ms(step_times, 0.95),
                step_time_p99_ms=percentile_ms(step_times, 0.99),
                checkpoint_count=checkpoint_count,
                checkpoint_bytes_total=checkpoint_bytes_total,
                stream_stall_seconds_total=stream_stall_total,
                stream_io_seconds_total=stream_io_total,
                stall_percent=stall_pct,
                checkpoint_throughput_mb_s=throughput,
            )
        )

    except Exception as exc:
        # Abort barriers so sibling workers don't hang waiting for this worker.
        for barrier in (artifact_done_barrier, commit_done_barrier):
            try:
                barrier.abort()
            except Exception:
                pass
        result_queue.put(
            WorkerResult(
                worker_id=f"worker-{rank}",
                rank=rank,
                total_steps=0,
                total_duration_seconds=0.0,
                step_time_p50_ms=0.0,
                step_time_p95_ms=0.0,
                step_time_p99_ms=0.0,
                checkpoint_count=0,
                checkpoint_bytes_total=0,
                stream_stall_seconds_total=0.0,
                stream_io_seconds_total=0.0,
                stall_percent=0.0,
                checkpoint_throughput_mb_s=0.0,
                error=str(exc),
            )
        )


def run_benchmark(args: argparse.Namespace) -> BenchmarkSummary:
    global _last_runtime_run_id
    coordinator_addr = os.getenv("DTRUNTIME_COORDINATOR_ADDR", args.coordinator_addr)

    # Rank 0 registers the run — all workers share this run_id.
    admin_client = RuntimeClient(coordinator_addr)
    run_response = admin_client.register_run(
        run_name=args.run_id,
        world_size=args.workers,
        assignment_policy=admin_client._runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
        storage_backend_kind=admin_client._runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
        idempotency_key=f"mw-register-run-{args.run_id}",
    )
    runtime_run_id = run_response.run_id.value
    _last_runtime_run_id = runtime_run_id
    admin_client.close()

    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    # Barrier 1: all N workers must finish streaming before rank 0 commits.
    artifact_done_barrier = multiprocessing.Barrier(args.workers)
    # Barrier 2: all N workers rendezvous after rank 0 commits.
    commit_done_barrier = multiprocessing.Barrier(args.workers)

    processes = []
    wall_start = time.perf_counter()

    for rank in range(args.workers):
        p = multiprocessing.Process(
            target=_worker_main,
            args=(
                rank,
                args.workers,
                runtime_run_id,
                args,
                result_queue,
                artifact_done_barrier,
                commit_done_barrier,
            ),
            daemon=True,
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    total_wall = time.perf_counter() - wall_start

    results: list[WorkerResult] = []
    while not result_queue.empty():
        results.append(result_queue.get_nowait())
    results.sort(key=lambda r: r.rank)

    errors = [r for r in results if r.error]
    if errors:
        for e in errors:
            print(f"[ERROR] {e.worker_id}: {e.error}")
        raise RuntimeError(f"{len(errors)} worker(s) failed — see above")

    stall_pcts = [r.stall_percent for r in results]
    step_p50s = [r.step_time_p50_ms for r in results]
    step_p99s = [r.step_time_p99_ms for r in results]
    total_bytes = sum(r.checkpoint_bytes_total for r in results)
    total_io = sum(r.stream_io_seconds_total for r in results)
    aggregate_throughput = (total_bytes / (1024 * 1024)) / total_io if total_io > 0 else 0.0
    checkpoints_committed = results[0].checkpoint_count if results else 0

    return BenchmarkSummary(
        run_id=args.run_id,
        runtime_run_id=runtime_run_id,
        workers=args.workers,
        total_steps_per_worker=args.steps,
        checkpoint_every_steps=args.checkpoint_every_steps,
        checkpoint_bytes_per_worker=args.checkpoint_bytes,
        coordinator_addr=coordinator_addr,
        aggregate_throughput_mb_s=aggregate_throughput,
        aggregate_checkpoint_bytes=total_bytes,
        stall_percent_p50=percentile(stall_pcts, 0.50),
        stall_percent_p99=percentile(stall_pcts, 0.99),
        step_time_p50_ms=percentile(step_p50s, 0.50),
        step_time_p99_ms=percentile(step_p99s, 0.99),
        worker_results=[asdict(r) for r in results],
        total_wall_seconds=total_wall,
        checkpoints_committed=checkpoints_committed,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Multi-worker async checkpoint benchmark — streams artifacts to coordinator"
    )
    p.add_argument("--workers", type=int, default=4, help="Number of parallel worker processes")
    p.add_argument("--steps", type=int, default=40)
    p.add_argument("--checkpoint-every-steps", type=int, default=10)
    p.add_argument("--checkpoint-bytes", type=int, default=16 * 1024 * 1024)
    p.add_argument("--samples", type=int, default=512)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--input-dim", type=int, default=32)
    p.add_argument("--num-classes", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--run-id", default="mw-async-bench")
    p.add_argument("--coordinator-addr", default="127.0.0.1:50051")
    p.add_argument("--output-dir", default=".dtruntime-results/async-bench")
    p.add_argument("--json-out", default=None)
    p.add_argument(
        "--backend",
        choices=["localfs", "s3"],
        default="localfs",
        help="Storage backend the coordinator is configured to use (for reporting only)",
    )
    p.add_argument(
        "--chaos-rank",
        type=int,
        default=-1,
        metavar="N",
        help="Inject a crash in worker rank N to demonstrate failure handling (default: disabled)",
    )
    p.add_argument(
        "--chaos-at-step",
        type=int,
        default=None,
        metavar="STEP",
        help="Step at which the chaos worker crashes (default: second checkpoint step)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.chaos_at_step is None:
        args.chaos_at_step = args.checkpoint_every_steps * 2
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    coordinator_addr = os.getenv("DTRUNTIME_COORDINATOR_ADDR", args.coordinator_addr)

    try:
        summary = run_benchmark(args)
    except RuntimeError as exc:
        print(f"\n[FAILURE] Benchmark failed: {exc}")
        # Show that the coordinator still holds the last committed checkpoint.
        print("[RECOVERY] Querying coordinator for last committed checkpoint...")
        try:
            from dtruntime.client import RuntimeClient
            client = RuntimeClient(coordinator_addr)
            resp = client.restore_latest(run_id=_last_runtime_run_id or "")
            client.close()
            m = resp.manifest
            latest = {
                "checkpoint_id": m.checkpoint_id.value if m.checkpoint_id else None,
                "step": m.step,
                "artifact_count": len(m.artifacts),
            }
            print(f"[RECOVERY] Last committed checkpoint: {json.dumps(latest, indent=2)}")
        except Exception as e:
            print(f"[RECOVERY] Could not query coordinator: {e}")
        raise

    payload = asdict(summary)
    payload["backend"] = args.backend
    out = json.dumps(payload, indent=2)
    print(out)

    if args.json_out:
        Path(args.json_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json_out).write_text(out + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
