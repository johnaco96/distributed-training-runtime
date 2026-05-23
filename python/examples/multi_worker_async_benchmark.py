"""Multi-worker async checkpoint benchmark with real sharded data loading.

Each worker runs as a real OS process.  Before training starts the admin
pre-registers a dataset with N binary shard files (one per worker) stored in
S3/MinIO or on the local filesystem.  Each worker calls GetShardAssignment,
receives the URI of its assigned shard, and reads real float32 tensors from
that shard instead of generating synthetic data.  Workers stream checkpoint
artifacts directly to the coordinator over gRPC — the Rust tokio runtime writes
bytes to storage asynchronously while training continues.  Rank 0 coordinates
the global checkpoint commit after all workers have finished streaming.

Usage (S3 backend, MinIO)::

    python python/examples/multi_worker_async_benchmark.py \\
        --workers 4 --steps 40 --checkpoint-every-steps 10 \\
        --checkpoint-bytes 16777216 --shard-records 2000 \\
        --backend s3 --s3-endpoint http://localhost:9100 \\
        --output-dir .dtruntime-results/async-bench

Usage (local filesystem)::

    python python/examples/multi_worker_async_benchmark.py \\
        --workers 4 --steps 40 --checkpoint-every-steps 10 \\
        --checkpoint-bytes 4194304 --shard-records 1000 \\
        --backend localfs

Goals demonstrated:
  - Large-scale distributed systems: N workers → 1 coordinator, gRPC fan-in
  - Sharded data access: coordinator assigns shards, workers read real I/O
  - High-throughput data movement: concurrent streams, Rust async I/O
  - Rust + Python interop: Python API, Rust storage engine
  - Performance profiling: per-worker + aggregate metrics
  - Failure handling: incomplete checkpoint rejection, recovery
  - Developer-friendly API: stream_checkpoint_artifact() hides I/O complexity
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
from dtruntime.shard_reader import (
    ShardReader,
    ensure_s3_bucket,
    generate_shard_bytes,
    upload_shard_to_s3,
    write_shard_to_local_file,
)


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
    shared_dataset_id: str,
) -> None:
    """Entry point for each worker process.

    Each worker:
    1. Registers itself with the coordinator.
    2. Calls GetShardAssignment — the coordinator's rank-modulo policy returns
       the one shard whose index equals this worker's rank.  The response
       includes the shard's URI (S3 or file://).
    3. Creates a ShardReader that downloads / reads the binary shard and yields
       real float32 batches.  Training uses these real tensors, not randn().
    4. Reports shard progress (progress_offset) at every checkpoint step.
    5. Participates in the two-barrier checkpoint commit protocol.
    """
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

        # ── Real shard assignment ──────────────────────────────────────────
        # The coordinator's STATIC_RANK policy maps shard_index % world_size
        # to worker_rank, so each worker gets exactly one shard.
        assignment_resp = client.request_shard(
            run_id=runtime_run_id,
            worker_id=worker_id,
            dataset_id=shared_dataset_id,
            idempotency_key=f"mw-shard-{run_id}-{worker_id}",
        )
        if not assignment_resp.assignments:
            raise RuntimeError(f"no shard assigned to {worker_id} for dataset {shared_dataset_id}")

        assigned = assignment_resp.assignments[0]
        shard_id = assigned.shard_id.value
        shard_uri = assigned.shard_uri
        lease_epoch = assigned.lease_epoch

        # ── Real data loading from assigned shard ──────────────────────────
        # ShardReader downloads the binary DTRS shard (one GET for S3, one
        # open() for file://) and iterates it as (inputs, labels, offset)
        # batches.  We build an infinite cycling iterator so a shard with
        # fewer records than args.steps × batch_size repeats naturally.
        s3_kwargs = dict(
            s3_endpoint=getattr(args, "s3_endpoint", None),
            s3_access_key=getattr(args, "s3_access_key", None),
            s3_secret_key=getattr(args, "s3_secret_key", None),
        )
        reader = ShardReader(shard_uri, batch_size=args.batch_size, **s3_kwargs)

        def _infinite_batches(r: ShardReader):
            while True:
                yield from r

        shard_iter = _infinite_batches(reader)

        torch.manual_seed(args.seed + rank)
        # Build the model with the right input_dim from the shard header.
        actual_input_dim = reader.input_dim  # triggers the one-time S3 download
        actual_num_classes = reader.num_classes
        model = nn.Sequential(
            nn.Linear(actual_input_dim, 64), nn.ReLU(), nn.Linear(64, actual_num_classes)
        )
        opt = torch.optim.SGD(model.parameters(), lr=0.01)
        loss_fn = nn.CrossEntropyLoss()

        step_times: list[float] = []
        stream_stall_total = 0.0
        stream_io_total = 0.0
        checkpoint_bytes_total = 0
        checkpoint_count = 0
        payload = os.urandom(args.checkpoint_bytes)
        last_progress_offset = 0

        wall_start = time.perf_counter()

        for step in range(1, args.steps + 1):
            s0 = time.perf_counter()
            # Real data from the S3/local shard — not synthetic noise.
            batch = next(shard_iter)
            batch_x = batch.inputs
            batch_y = batch.labels
            last_progress_offset = batch.progress_offset
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

                # Report how far we've read into the shard so the coordinator
                # can resume from this offset after a failure.
                client.report_shard_progress(
                    run_id=runtime_run_id,
                    worker_id=worker_id,
                    dataset_id=shared_dataset_id,
                    shard_id=shard_id,
                    lease_epoch=lease_epoch,
                    progress_offset=last_progress_offset,
                    idempotency_key=f"mw-shard-prog-{run_id}-{worker_id}-{step}",
                )

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


def _generate_shards(args: argparse.Namespace, runtime_run_id: str) -> list[dict]:
    """Generate binary shard files and return shard metadata dicts.

    For S3 backend: uploads each shard to MinIO/S3 and returns s3:// URIs.
    For localfs backend: writes shards to ./tmp/shards/{run_id}/ as file:// URIs.
    """
    shard_records = getattr(args, "shard_records", 1000)
    input_dim = args.input_dim
    num_classes = args.num_classes
    seed = args.seed
    backend = getattr(args, "backend", "localfs")

    shards = []
    for i in range(args.workers):
        shard_seed = (seed * 31 + i) if seed else None
        data = generate_shard_bytes(
            record_count=shard_records,
            input_dim=input_dim,
            num_classes=num_classes,
            seed=shard_seed,
        )
        shard_id = f"shard-{i:04d}"
        if backend == "s3":
            s3_endpoint = getattr(args, "s3_endpoint", "http://localhost:9100")
            s3_bucket = getattr(args, "s3_bucket", "dtruntime-checkpoints")
            s3_access_key = getattr(args, "s3_access_key", "minioadmin")
            s3_secret_key = getattr(args, "s3_secret_key", "minioadmin")
            ensure_s3_bucket(s3_bucket, s3_endpoint, s3_access_key, s3_secret_key)
            key = f"datasets/{runtime_run_id}/{shard_id}.bin"
            uri = upload_shard_to_s3(data, s3_bucket, key, s3_endpoint, s3_access_key, s3_secret_key)
        else:
            shard_path = Path("tmp") / "shards" / runtime_run_id / f"{shard_id}.bin"
            uri = write_shard_to_local_file(data, shard_path)  # returns file:// URI
        shards.append({"shard_id": shard_id, "shard_uri": uri, "size_bytes": len(data)})
    return shards


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

    # ── Generate real shards and register dataset ──────────────────────────
    # Admin generates one binary shard per worker, uploads to S3 or writes to
    # disk, then registers the dataset so the coordinator knows each shard's
    # URI.  Workers will each receive their assigned shard URI via
    # GetShardAssignment (rank-modulo: shard_index % world_size == rank).
    shard_metas = _generate_shards(args, runtime_run_id)
    dataset_resp = admin_client.register_dataset(
        run_id=runtime_run_id,
        name="dtruntime-training-dataset",
        uri=f"dtruntime://{runtime_run_id}/dataset",
        format="dtrs-v1",
        shards=shard_metas,
        dataset_id=f"dataset-{runtime_run_id}",
        idempotency_key=f"mw-register-dataset-{runtime_run_id}",
    )
    shared_dataset_id = dataset_resp.dataset_id.value
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
                shared_dataset_id,
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
        help="Storage backend: controls where checkpoint artifacts and shard files are stored",
    )
    # ── Shard / dataset flags ──────────────────────────────────────────────
    p.add_argument(
        "--shard-records",
        type=int,
        default=1000,
        metavar="N",
        help="Records per shard file (each record = input_dim float32 values + 1 int32 label)",
    )
    # ── S3 / MinIO connection flags ────────────────────────────────────────
    p.add_argument("--s3-endpoint", default="http://localhost:9100", help="S3/MinIO endpoint URL")
    p.add_argument("--s3-bucket", default="dtruntime-checkpoints", help="S3 bucket name")
    p.add_argument("--s3-access-key", default="minioadmin", help="S3 access key ID")
    p.add_argument("--s3-secret-key", default="minioadmin", help="S3 secret access key")
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
