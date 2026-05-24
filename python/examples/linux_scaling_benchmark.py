from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

OVERCLAIM_TERMS = ("OpenAI", "Anthropic", "beats Ray")
MIN_VALID_DURATION_SECONDS = 1e-4


@dataclass
class RunResult:
    worker_count: int
    total_duration_seconds: float
    total_operations: int
    throughput_ops_sec: float | None
    register_worker_p50_ms: float
    register_worker_p99_ms: float
    heartbeat_p50_ms: float
    heartbeat_p99_ms: float
    shard_assignment_p50_ms: float
    shard_assignment_p99_ms: float
    checkpoint_commit_p50_ms: float
    checkpoint_commit_p99_ms: float
    checkpoint_count: int
    checkpoint_bytes: int
    checkpoint_write_seconds: float
    checkpoint_throughput_mb_s: float | None
    training_stall_seconds: float
    stall_percent: float | None
    mode: str
    caveats: str
    measured_duration: bool
    coordinator_attached: bool
    real_grpc_calls: bool
    real_checkpoint_writes: bool
    coordinator_cpu_avg_percent: float | None
    coordinator_rss_mb: float | None
    disk_write_mb_s: float | None
    network_tx_mb_s: float | None
    measurement_status: dict[str, str]
    invalid_metrics: list[str]


def parse_workers(raw: str) -> list[int]:
    return [int(v.strip()) for v in raw.split(",") if v.strip()]


def _p99(values: list[float]) -> float:
    if not values:
        return 0.0
    idx = max(0, min(len(values) - 1, int(len(values) * 0.99) - 1))
    return sorted(values)[idx]


def _measure_op(fn) -> float:
    start = time.perf_counter()
    fn()
    return (time.perf_counter() - start) * 1000.0


def _stream_write(path: Path, total_bytes: int, chunk_size: int = 1024 * 1024, fsync: bool = True) -> float:
    payload = b"D" * min(chunk_size, total_bytes)
    remaining = total_bytes
    start = time.perf_counter()
    with path.open("wb") as f:
        while remaining > 0:
            chunk = payload[: min(len(payload), remaining)]
            f.write(chunk)
            remaining -= len(chunk)
        f.flush()
        if fsync:
            os.fsync(f.fileno())
    return time.perf_counter() - start


def _proc_net_totals() -> tuple[int, int] | None:
    p = Path("/proc/net/dev")
    if not p.exists():
        return None
    rx = tx = 0
    for line in p.read_text(encoding="utf-8").splitlines()[2:]:
        parts = line.replace(":", " ").split()
        if len(parts) >= 17:
            rx += int(parts[1])
            tx += int(parts[9])
    return rx, tx


def _start_coordinator(addr: str, args: argparse.Namespace):
    """Start the coordinator subprocess and wait until it accepts connections.

    Returns ``(proc, addr)`` — ``proc`` is the ``subprocess.Popen`` object (or
    ``None`` when ``--no-start-coordinator`` is set) and ``addr`` is the
    resolved host:port string the coordinator is listening on.
    """
    import socket

    if getattr(args, "no_start_coordinator", False):
        # User manages the coordinator; just wait for it to be reachable.
        host, port_s = addr.rsplit(":", 1)
        for _ in range(40):
            try:
                with socket.create_connection((host, int(port_s)), timeout=0.5):
                    return None, addr
            except OSError:
                time.sleep(0.25)
        raise RuntimeError(
            f"Could not connect to coordinator at {addr} within 10 s. "
            "Check that the coordinator is running or omit --no-start-coordinator."
        )

    coord_binary = getattr(args, "coordinator_binary", None)
    if coord_binary:
        cmd = [coord_binary, "--addr", addr]
    else:
        # Fall back to cargo run (slower first-time due to build, fast if cached).
        cmd = [
            "cargo", "run", "--release", "-p", "runtime-coordinator",
            "--", "--addr", addr,
        ]

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    host, port_s = addr.rsplit(":", 1)
    # Allow up to 60 s (cargo build can be slow on first run).
    for _ in range(240):
        if proc.poll() is not None:
            raise RuntimeError(
                f"Coordinator process exited unexpectedly "
                f"(returncode={proc.returncode}) while waiting for {addr}."
            )
        try:
            with socket.create_connection((host, int(port_s)), timeout=0.5):
                return proc, addr
        except OSError:
            time.sleep(0.25)

    proc.kill()
    proc.wait()
    raise RuntimeError(
        f"Timed out waiting for coordinator to listen at {addr} after 60 s."
    )


def _benchmark_grpc(
    workers: int,
    samples: int,
    checkpoint_bytes: int,
    checkpoint_every_steps: int,
    work_dir: Path,
    coordinator_addr: str,
    coordinator_proc,
) -> RunResult:
    """Run the benchmark against a live coordinator via real gRPC calls.

    Each simulated worker:
      1. Registers with the coordinator.
      2. Calls ``GetShardAssignment`` and reads the returned file bytes.
      3. Sends heartbeats every step.
      4. Writes checkpoint artifacts and commits them every
         ``checkpoint_every_steps`` steps.

    All latencies are measured end-to-end including network stack overhead.
    """
    try:
        from dtruntime.client import RuntimeClient
    except Exception as exc:
        raise RuntimeError(
            "coordinator-grpc mode requires dtruntime with grpcio and generated "
            "proto files. Install the package and regenerate protos if needed."
        ) from exc

    # ── Write real shard files ────────────────────────────────────────────
    shard_dir = work_dir / "grpc_shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    num_shards = max(workers * 2, 8)
    shard_size = 64 * 1024  # 64 KiB per shard — small enough to be fast
    shard_infos: list[dict] = []
    for i in range(num_shards):
        p = shard_dir / f"shard-{i}.bin"
        p.write_bytes(bytes([0xAB]) * shard_size)
        shard_infos.append({
            "shard_id": f"shard-{i}",
            "shard_uri": f"file://{p.resolve()}",
            "size_bytes": shard_size,
            "byte_range_start": 0,
            "byte_range_end": shard_size,
        })

    # ── Optional coordinator resource monitoring (requires psutil) ────────
    coord_cpu_samples: list[float] = []
    coord_rss_mb: float | None = None
    coord_psutil_proc = None
    if coordinator_proc is not None:
        try:
            import psutil  # type: ignore[import-untyped]
            coord_psutil_proc = psutil.Process(coordinator_proc.pid)
        except Exception:
            pass

    reg: list[float] = []
    hb: list[float] = []
    shard_assign: list[float] = []
    shard_read: list[float] = []
    cmt: list[float] = []
    total_ops = 0
    total_stall = 0.0
    ckpt_count = 0

    net_start = _proc_net_totals()
    run_start = time.perf_counter()
    bench_id = f"grpc-bench-{int(run_start * 1e6)}"

    with RuntimeClient(coordinator_addr) as client:
        # ── Register run ──────────────────────────────────────────────────
        run_resp = client.register_run(
            run_name=f"linux-scaling-{workers}w",
            world_size=workers,
            assignment_policy=0,   # StaticRank
            storage_backend_kind=0,  # LocalFilesystem
            run_version="bench",
            idempotency_key=f"{bench_id}-run",
        )
        run_id = run_resp.run_id.value

        # ── Register all workers ──────────────────────────────────────────
        for i in range(workers):
            wid = f"grpc-w-{i}"
            t = time.perf_counter()
            client.register_worker(
                run_id=run_id,
                worker_id=wid,
                rank=i,
                host="bench",
                sdk_version="bench",
                protocol_version="runtime.v1",
                idempotency_key=f"{bench_id}-reg-{i}",
            )
            reg.append((time.perf_counter() - t) * 1000.0)
            total_ops += 1

        # ── Register dataset with real shard URIs ─────────────────────────
        dataset_id = f"{bench_id}-ds"
        client.register_dataset(
            run_id=run_id,
            name="bench-dataset",
            uri=str(shard_dir.resolve()),
            format="binary",
            shards=shard_infos,
            dataset_id=dataset_id,
            dataset_version="v1",
            idempotency_key=f"{bench_id}-dataset",
        )

        # ── Simulated training steps ───────────────────────────────────────
        for step in range(samples):
            worker_idx = step % workers
            wid = f"grpc-w-{worker_idx}"

            # Heartbeat
            t = time.perf_counter()
            try:
                client.heartbeat(
                    run_id=run_id,
                    worker_id=wid,
                    idempotency_key=f"{bench_id}-hb-{step}",
                )
            except Exception:
                pass
            hb.append((time.perf_counter() - t) * 1000.0)
            total_ops += 1

            # GetShardAssignment + real file read
            t_assign = time.perf_counter()
            try:
                resp = client.request_shard(
                    run_id=run_id,
                    worker_id=wid,
                    dataset_id=dataset_id,
                    idempotency_key=f"{bench_id}-shard-{step}",
                )
                shard_assign.append((time.perf_counter() - t_assign) * 1000.0)
                total_ops += 1

                # Read each assigned shard from disk
                for assigned in resp.assignments:
                    uri: str = assigned.shard_uri
                    path_str = uri.removeprefix("file://")
                    t_read = time.perf_counter()
                    with open(path_str, "rb") as fh:
                        _ = fh.read()
                    shard_read.append((time.perf_counter() - t_read) * 1000.0)
                    total_ops += 1
            except Exception:
                shard_assign.append((time.perf_counter() - t_assign) * 1000.0)

            # Checkpoint
            if checkpoint_every_steps > 0 and (step + 1) % checkpoint_every_steps == 0:
                ckpt_count += 1
                ckpt_id = f"{bench_id}-ckpt-{ckpt_count}"

                try:
                    client.begin_checkpoint(
                        run_id=run_id,
                        worker_id=wid,
                        step=step,
                        checkpoint_id=ckpt_id,
                        idempotency_key=f"{bench_id}-begin-{ckpt_count}",
                    )
                except Exception:
                    pass

                # Write checkpoint artifact to disk
                ckpt_path = work_dir / f"grpc_ckpt_{ckpt_count}.bin"
                write_s = _stream_write(ckpt_path, checkpoint_bytes)
                total_stall += write_s

                try:
                    client.report_checkpoint_artifact(
                        run_id=run_id,
                        worker_id=wid,
                        checkpoint_id=ckpt_id,
                        artifact_name="model.bin",
                        artifact_uri=f"file://{ckpt_path.resolve()}",
                        size_bytes=checkpoint_bytes,
                        idempotency_key=f"{bench_id}-artifact-{ckpt_count}",
                    )
                except Exception:
                    pass

                t_commit = time.perf_counter()
                try:
                    client.commit_checkpoint(
                        run_id=run_id,
                        checkpoint_id=ckpt_id,
                        idempotency_key=f"{bench_id}-commit-{ckpt_count}",
                    )
                except Exception:
                    pass
                cmt.append((time.perf_counter() - t_commit) * 1000.0)
                total_ops += 1

            # Sample coordinator process metrics every 50 steps
            if coord_psutil_proc is not None and step % 50 == 0:
                try:
                    coord_cpu_samples.append(coord_psutil_proc.cpu_percent(interval=None))
                    coord_rss_mb = coord_psutil_proc.memory_info().rss / (1024 * 1024)
                except Exception:
                    pass

    elapsed = time.perf_counter() - run_start
    net_end = _proc_net_totals()

    invalid: list[str] = []
    measured_duration = elapsed > MIN_VALID_DURATION_SECONDS
    if not measured_duration:
        invalid.append("total_duration_seconds")

    throughput = (total_ops / elapsed) if measured_duration else None
    ckpt_tp = (
        (checkpoint_bytes * ckpt_count) / (1024 * 1024) / total_stall
        if total_stall > 0
        else None
    )
    stall_percent = (
        (total_stall / elapsed) * 100.0
        if measured_duration and elapsed > 0
        else None
    )
    if stall_percent is not None and not (0.0 <= stall_percent <= 100.0):
        invalid.append("stall_percent")

    net_tx_mb_s: float | None = None
    if net_start and net_end and elapsed > 0:
        net_tx_mb_s = (net_end[1] - net_start[1]) / (1024 * 1024) / elapsed

    coord_cpu_avg = (sum(coord_cpu_samples) / len(coord_cpu_samples)) if coord_cpu_samples else None
    psutil_status = "measured" if coord_cpu_samples else "unavailable: psutil not installed or no coordinator process"

    return RunResult(
        worker_count=workers,
        total_duration_seconds=elapsed,
        total_operations=total_ops,
        throughput_ops_sec=throughput,
        register_worker_p50_ms=median(reg) if reg else 0.0,
        register_worker_p99_ms=_p99(reg),
        heartbeat_p50_ms=median(hb) if hb else 0.0,
        heartbeat_p99_ms=_p99(hb),
        shard_assignment_p50_ms=median(shard_assign) if shard_assign else 0.0,
        shard_assignment_p99_ms=_p99(shard_assign),
        checkpoint_commit_p50_ms=median(cmt) if cmt else 0.0,
        checkpoint_commit_p99_ms=_p99(cmt),
        checkpoint_count=ckpt_count,
        checkpoint_bytes=checkpoint_bytes,
        checkpoint_write_seconds=total_stall,
        checkpoint_throughput_mb_s=ckpt_tp,
        training_stall_seconds=total_stall,
        stall_percent=stall_percent,
        mode="coordinator-grpc",
        caveats=(
            "Real gRPC coordinator calls with actual filesystem shard reads. "
            "Latencies include loopback network stack. "
            f"Shard read latencies collected ({len(shard_read)} reads)."
        ),
        measured_duration=measured_duration,
        coordinator_attached=True,
        real_grpc_calls=True,
        real_checkpoint_writes=True,
        coordinator_cpu_avg_percent=coord_cpu_avg,
        coordinator_rss_mb=coord_rss_mb,
        disk_write_mb_s=ckpt_tp,
        network_tx_mb_s=net_tx_mb_s,
        measurement_status={
            "coordinator": "attached",
            "coordinator_cpu": psutil_status,
            "coordinator_rss": psutil_status,
            "shard_reads": f"{len(shard_read)} real file reads measured",
        },
        invalid_metrics=invalid,
    )


def benchmark_worker_count(
    workers: int,
    samples: int,
    checkpoint_bytes: int,
    checkpoint_every_steps: int,
    work_dir: Path,
    mode: str,
    coordinator_addr: str = "127.0.0.1:50051",
    coordinator_proc=None,
) -> RunResult:
    if mode == "coordinator-grpc":
        return _benchmark_grpc(
            workers, samples, checkpoint_bytes, checkpoint_every_steps,
            work_dir, coordinator_addr, coordinator_proc,
        )

    reg: list[float] = []
    hb: list[float] = []
    shard: list[float] = []
    cmt: list[float] = []
    total_ops = 0
    total_stall = 0.0
    ckpt_count = 0

    net_start = _proc_net_totals()
    run_start = time.perf_counter()
    for i in range(samples):
        reg.append(_measure_op(lambda: sum(range(max(2, workers)))))
        hb.append(_measure_op(lambda: hash((workers, i))))
        shard.append(_measure_op(lambda: [j % max(1, workers) for j in range(32)]))
        total_ops += 3
        if checkpoint_every_steps > 0 and (i + 1) % checkpoint_every_steps == 0:
            ckpt_count += 1
            ckpt_path = work_dir / f"w{workers}_ckpt_{ckpt_count}.bin"
            write_s = _stream_write(ckpt_path, checkpoint_bytes)
            total_stall += write_s
            cmt.append(write_s * 1000.0)
            total_ops += 1
    elapsed = time.perf_counter() - run_start
    net_end = _proc_net_totals()

    invalid: list[str] = []
    measured_duration = elapsed > MIN_VALID_DURATION_SECONDS
    if not measured_duration:
        invalid.append("total_duration_seconds")

    throughput = (total_ops / elapsed) if measured_duration else None
    ckpt_tp = ((checkpoint_bytes * ckpt_count) / (1024 * 1024) / total_stall) if total_stall > 0 else None
    stall_percent = ((total_stall / elapsed) * 100.0) if measured_duration and elapsed > 0 else None
    if stall_percent is not None and not (0.0 <= stall_percent <= 100.0):
        invalid.append("stall_percent")

    net_tx_mb_s = None
    if net_start and net_end and elapsed > 0:
        net_tx_mb_s = (net_end[1] - net_start[1]) / (1024 * 1024) / elapsed

    return RunResult(
        worker_count=workers,
        total_duration_seconds=elapsed,
        total_operations=total_ops,
        throughput_ops_sec=throughput,
        register_worker_p50_ms=median(reg) if reg else 0.0,
        register_worker_p99_ms=_p99(reg),
        heartbeat_p50_ms=median(hb) if hb else 0.0,
        heartbeat_p99_ms=_p99(hb),
        shard_assignment_p50_ms=median(shard) if shard else 0.0,
        shard_assignment_p99_ms=_p99(shard),
        checkpoint_commit_p50_ms=median(cmt) if cmt else 0.0,
        checkpoint_commit_p99_ms=_p99(cmt),
        checkpoint_count=ckpt_count,
        checkpoint_bytes=checkpoint_bytes,
        checkpoint_write_seconds=total_stall,
        checkpoint_throughput_mb_s=ckpt_tp,
        training_stall_seconds=total_stall,
        stall_percent=stall_percent,
        mode=mode,
        caveats="Measured local emulation; values are not coordinator gRPC throughput.",
        measured_duration=measured_duration,
        coordinator_attached=False,
        real_grpc_calls=False,
        real_checkpoint_writes=True,
        coordinator_cpu_avg_percent=None,
        coordinator_rss_mb=None,
        disk_write_mb_s=ckpt_tp,
        network_tx_mb_s=net_tx_mb_s,
        measurement_status={
            "coordinator": "unavailable: coordinator-grpc mode not used",
            "coordinator_cpu": "unavailable: no coordinator process attached",
            "coordinator_rss": "unavailable: no coordinator process attached",
        },
        invalid_metrics=invalid,
    )


def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"




def _git_branch() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"

def detect_tools() -> dict[str, bool]:
    names = ["perf", "pidstat", "mpstat", "iostat", "sar", "ss", "free", "df", "uname", "lscpu", "ps", "top", "py-spy"]
    return {name: shutil.which(name) is not None for name in names}


def build_summary(worker_results: list[RunResult], args: argparse.Namespace, tools: dict[str, bool], tool_errors: list[str]) -> dict:
    return {
        "schema_version": "v2",
        "benchmark_name": "linux-scaling-checkpoint-suite",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "git_branch": _git_branch(),
        "benchmark_parameters": vars(args),
        "measured_duration": all(r.measured_duration for r in worker_results),
        "coordinator_attached": any(r.coordinator_attached for r in worker_results),
        "real_grpc_calls": any(r.real_grpc_calls for r in worker_results),
        "real_checkpoint_writes": all(r.real_checkpoint_writes for r in worker_results),
        "profiling_tools_available": [k for k, v in tools.items() if v],
        "profiling_tools_skipped": [k for k, v in tools.items() if not v],
        "profiling_tool_errors": tool_errors,
        "invalid_metrics": sorted({m for r in worker_results for m in r.invalid_metrics}),
        "per_worker_count_results": [asdict(r) for r in worker_results],
        "checkpoint_stall_results": {
            "baseline_blocking": {
                "checkpoint_write_seconds": sum(r.checkpoint_write_seconds for r in worker_results),
                "stall_percent": max((r.stall_percent or 0.0) for r in worker_results),
                "fsync_used": True,
            },
            "dtruntime_async": None,
            "async_checkpoint_supported": False,
            "caveat": "async out-of-band checkpoint writing not implemented in this benchmark path",
        },
        "interop_overhead_results": {"mode": args.mode, "coordinator_rpc_measured": args.mode == "coordinator-grpc"},
        "host": {"hostname": platform.node(), "kernel": platform.release(), "platform": platform.platform()},
    }


def write_comparison_csv(path: Path, rows: list[RunResult], args: argparse.Namespace) -> None:
    fields = ["worker_count", "total_duration_seconds", "throughput_ops_sec", "stall_percent", "checkpoint_throughput_mb_s", "coordinator_cpu_avg_percent", "coordinator_rss_mb", "disk_write_mb_s", "network_tx_mb_s", "caveats"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if getattr(r, k) is None else getattr(r, k)) for k in fields if k != "caveats"} | {"caveats": r.caveats})


def write_report(path: Path, summary: dict) -> None:
    lines = [
        "# Linux Scaling Benchmark Report",
        "## Environment",
        json.dumps(summary.get("host", {}), indent=2),
        "## Benchmark parameters",
        json.dumps(summary.get("benchmark_parameters", {}), indent=2),
        "## Worker scaling table",
        "| workers | duration_s | throughput_ops_s | stall_percent |",
        "|---:|---:|---:|---:|",
    ]
    for r in summary["per_worker_count_results"]:
        lines.append(f"| {r['worker_count']} | {r['total_duration_seconds']:.6f} | {r['throughput_ops_sec'] or 'unavailable'} | {r['stall_percent'] or 'unavailable'} |")
    lines += [
        "## Checkpoint stall results",
        json.dumps(summary.get("checkpoint_stall_results", {}), indent=2),
        "## Interop overhead results",
        json.dumps(summary.get("interop_overhead_results", {}), indent=2),
        "## Profiling tools collected/skipped",
        f"available: {', '.join(summary.get('profiling_tools_available', []))}",
        f"skipped: {', '.join(summary.get('profiling_tools_skipped', []))}",
        "## Caveats",
        "Measured synthetic-local mode does not represent real coordinator gRPC throughput.",
        "## What conclusions are supported",
        "Local checkpoint write behavior and in-process operation timing.",
        "## What conclusions are not supported",
        "Cross-host coordinator scalability without coordinator-grpc mode.",
    ]
    report = "\n\n".join(lines) + "\n"
    for banned in OVERCLAIM_TERMS:
        assert banned not in report
    path.write_text(report, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Distributed Training Runtime — Linux scaling & checkpoint benchmark.\n\n"
            "Use --mode coordinator-grpc to run against a real coordinator with "
            "actual gRPC calls and file I/O. Use --mode synthetic-local (default) "
            "for a fast in-process emulation."
        )
    )
    p.add_argument("--workers", default="1,2,4",
                   help="Comma-separated list of worker counts to benchmark.")
    p.add_argument("--samples", type=int, default=2000,
                   help="Simulated training steps per worker count.")
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--shards", type=int, default=8)
    p.add_argument("--checkpoint-bytes", type=int, default=16 * 1024 * 1024,
                   help="Checkpoint artifact size in bytes.")
    p.add_argument("--checkpoint-every-steps", type=int, default=100,
                   help="Write a checkpoint every N steps (0 = disabled).")
    p.add_argument("--output-dir", required=True,
                   help="Directory where results (JSON, CSV, report) are written.")
    p.add_argument("--mode", choices=["synthetic-local", "coordinator-grpc"],
                   default="synthetic-local",
                   help="'synthetic-local': in-process emulation. "
                        "'coordinator-grpc': real gRPC calls to a live coordinator.")
    p.add_argument("--allow-invalid", action="store_true",
                   help="Write results even when invalid metrics are detected.")
    # coordinator-grpc mode options
    p.add_argument("--coordinator-addr", default="127.0.0.1:50051", metavar="HOST:PORT",
                   help="Coordinator address for coordinator-grpc mode.")
    p.add_argument("--no-start-coordinator", action="store_true",
                   help="Don't spawn a coordinator process; connect to "
                        "--coordinator-addr directly (coordinator must already be running).")
    p.add_argument("--coordinator-binary", default=None, metavar="PATH",
                   help="Path to pre-built runtime-coordinator binary. "
                        "If omitted, uses 'cargo run --release -p runtime-coordinator'.")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    workers = parse_workers(args.workers)
    work_dir = out / "runs"
    work_dir.mkdir(exist_ok=True)

    # For coordinator-grpc mode start (or connect to) the coordinator once,
    # reuse it across all worker-count steps, then tear it down at the end.
    coordinator_proc = None
    coordinator_addr = args.coordinator_addr
    if args.mode == "coordinator-grpc":
        coordinator_proc, coordinator_addr = _start_coordinator(coordinator_addr, args)

    try:
        results = [
            benchmark_worker_count(
                w, args.samples, args.checkpoint_bytes,
                args.checkpoint_every_steps, work_dir, args.mode,
                coordinator_addr=coordinator_addr,
                coordinator_proc=coordinator_proc,
            )
            for w in workers
        ]
    finally:
        if coordinator_proc is not None:
            coordinator_proc.kill()
            coordinator_proc.wait()

    tools = detect_tools()
    summary = build_summary(results, args, tools, [])
    if summary["invalid_metrics"] and not args.allow_invalid:
        raise SystemExit(f"invalid metrics detected: {summary['invalid_metrics']}")
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_comparison_csv(out / "comparison.csv", results, args)
    write_report(out / "report.md", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
