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


def benchmark_worker_count(workers: int, samples: int, checkpoint_bytes: int, checkpoint_every_steps: int, work_dir: Path, mode: str) -> RunResult:
    if mode == "coordinator-grpc":
        raise NotImplementedError("real coordinator mode not implemented")
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
    p = argparse.ArgumentParser()
    p.add_argument("--workers", default="1,2,4")
    p.add_argument("--samples", type=int, default=2000)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--shards", type=int, default=8)
    p.add_argument("--checkpoint-bytes", type=int, default=16 * 1024 * 1024)
    p.add_argument("--checkpoint-every-steps", type=int, default=100)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--mode", choices=["synthetic-local", "coordinator-grpc"], default="synthetic-local")
    p.add_argument("--allow-invalid", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    workers = parse_workers(args.workers)
    work_dir = out / "runs"
    work_dir.mkdir(exist_ok=True)
    results = [benchmark_worker_count(w, args.samples, args.checkpoint_bytes, args.checkpoint_every_steps, work_dir, args.mode) for w in workers]
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
