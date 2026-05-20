from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark standalone vs coordinator-driven local PyTorch training")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--samples", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--input-dim", type=int, default=16)
    parser.add_argument("--num-classes", type=int, default=2)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--seed", type=int, default=1234)
    parser.add_argument("--out-dir", type=Path, default=Path(".dtruntime-pytorch-bench"))
    parser.add_argument("--keep-outputs", action="store_true")
    parser.add_argument("--json-out", type=Path, default=None)
    parser.add_argument("--csv-out", type=Path, default=None)
    parser.add_argument("--skip-resume", action="store_true")
    parser.add_argument("--python", dest="python_executable", default=sys.executable)
    parser.add_argument("--environment", default="local")
    parser.add_argument("--secure-mode", action="store_true")
    parser.add_argument("--minio-enabled", action="store_true")
    return parser.parse_args()


def _deps_check(skip_resume: bool) -> None:
    if importlib.util.find_spec("torch") is None:
        raise RuntimeError("PyTorch is not installed. Install torch to run benchmark_pytorch_orchestration.py.")
    if importlib.util.find_spec("google.protobuf") is None:
        raise RuntimeError(
            "google.protobuf is not installed. Coordinator benchmark path requires protobuf-generated runtime stubs."
        )


def json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe(v) for v in value]
    return value


def _stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"mean": None, "median": None, "min": None, "max": None}
    return {
        "mean": float(statistics.mean(values)),
        "median": float(statistics.median(values)),
        "min": float(min(values)),
        "max": float(max(values)),
    }


def _run_command(cmd: list[str], out_dir: Path) -> dict[str, Any]:
    env = os.environ.copy()
    env["PYTHONPATH"] = "python"
    started = time.perf_counter()
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
    elapsed = time.perf_counter() - started
    metrics = {}
    metrics_path = out_dir / "metrics.json"
    if metrics_path.exists():
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    return {
        "command": cmd,
        "returncode": proc.returncode,
        "elapsed_seconds": elapsed,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "metrics": metrics,
        "metrics_path": str(metrics_path),
    }


def _extract_checkpoint(metrics: dict[str, Any]) -> dict[str, Any]:
    path = metrics.get("checkpoint_path")
    return {
        "checkpoint_path": path,
        "checkpoint_present": bool(path),
        "resumed_from_checkpoint": metrics.get("resumed_from_checkpoint"),
        "final_loss": metrics.get("final_loss"),
        "final_accuracy": metrics.get("final_accuracy"),
        "rpc_summary": metrics.get("rpc_summary"),
    }


def main() -> None:
    args = parse_args()
    _deps_check(args.skip_resume)

    root = Path(__file__).resolve().parents[2]
    standalone_script = root / "python/examples/pytorch_train.py"
    coordinator_script = root / "python/examples/pytorch_coordinator_train.py"

    out_dir = args.out_dir
    runs_dir = out_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    run_results: list[dict[str, Any]] = []
    for run_idx in range(args.runs):
        run_seed = args.seed + run_idx
        run_dir = runs_dir / f"run-{run_idx:03d}"
        standalone_out = run_dir / "standalone"
        standalone_resume_out = run_dir / "standalone-resume"
        coordinator_out = run_dir / "coordinator"
        coordinator_resume_out = run_dir / "coordinator-resume"

        standalone_cmd = [
            args.python_executable,
            str(standalone_script),
            "--epochs",
            str(args.epochs),
            "--samples",
            str(args.samples),
            "--batch-size",
            str(args.batch_size),
            "--input-dim",
            str(args.input_dim),
            "--num-classes",
            str(args.num_classes),
            "--seed",
            str(run_seed),
            "--out-dir",
            str(standalone_out),
        ]
        coordinator_cmd = [
            args.python_executable,
            str(coordinator_script),
            "--workers",
            str(args.workers),
            "--epochs",
            str(args.epochs),
            "--samples",
            str(args.samples),
            "--batch-size",
            str(args.batch_size),
            "--input-dim",
            str(args.input_dim),
            "--num-classes",
            str(args.num_classes),
            "--seed",
            str(run_seed),
            "--shards",
            str(args.shards),
            "--out-dir",
            str(coordinator_out),
        ]

        standalone_fresh = _run_command(standalone_cmd, standalone_out)
        coordinator_fresh = _run_command(coordinator_cmd, coordinator_out)

        standalone_resume = None
        coordinator_resume = None
        if not args.skip_resume:
            standalone_resume = _run_command([*standalone_cmd, "--resume"], standalone_out)
            coordinator_resume = _run_command([*coordinator_cmd, "--resume"], coordinator_out)

        result = {
            "run_index": run_idx,
            "seed": run_seed,
            "standalone_fresh_seconds": standalone_fresh["elapsed_seconds"],
            "coordinator_fresh_seconds": coordinator_fresh["elapsed_seconds"],
            "standalone_resume_seconds": standalone_resume["elapsed_seconds"] if standalone_resume else None,
            "coordinator_resume_seconds": coordinator_resume["elapsed_seconds"] if coordinator_resume else None,
            "fresh_overhead_seconds": coordinator_fresh["elapsed_seconds"] - standalone_fresh["elapsed_seconds"],
            "fresh_overhead_percent": (
                (coordinator_fresh["elapsed_seconds"] - standalone_fresh["elapsed_seconds"]) / standalone_fresh["elapsed_seconds"] * 100.0
                if standalone_fresh["elapsed_seconds"] > 0
                else None
            ),
            "resume_overhead_seconds": (
                coordinator_resume["elapsed_seconds"] - standalone_resume["elapsed_seconds"]
                if standalone_resume and coordinator_resume
                else None
            ),
            "resume_overhead_percent": (
                (coordinator_resume["elapsed_seconds"] - standalone_resume["elapsed_seconds"]) / standalone_resume["elapsed_seconds"] * 100.0
                if standalone_resume and coordinator_resume and standalone_resume["elapsed_seconds"] > 0
                else None
            ),
            "standalone_fresh": {**_extract_checkpoint(standalone_fresh["metrics"]), "returncode": standalone_fresh["returncode"]},
            "coordinator_fresh": {**_extract_checkpoint(coordinator_fresh["metrics"]), "returncode": coordinator_fresh["returncode"]},
            "standalone_resume": ({**_extract_checkpoint(standalone_resume["metrics"]), "returncode": standalone_resume["returncode"]} if standalone_resume else None),
            "coordinator_resume": ({**_extract_checkpoint(coordinator_resume["metrics"]), "returncode": coordinator_resume["returncode"]} if coordinator_resume else None),
            "commands": {
                "standalone_fresh": standalone_fresh["command"],
                "coordinator_fresh": coordinator_fresh["command"],
                "standalone_resume": standalone_resume["command"] if standalone_resume else None,
                "coordinator_resume": coordinator_resume["command"] if coordinator_resume else None,
            },
            "diagnostics": {
                "standalone_fresh": {"stdout": standalone_fresh["stdout"], "stderr": standalone_fresh["stderr"]},
                "coordinator_fresh": {"stdout": coordinator_fresh["stdout"], "stderr": coordinator_fresh["stderr"]},
                "standalone_resume": ({"stdout": standalone_resume["stdout"], "stderr": standalone_resume["stderr"]} if standalone_resume else None),
                "coordinator_resume": ({"stdout": coordinator_resume["stdout"], "stderr": coordinator_resume["stderr"]} if coordinator_resume else None),
            },
        }
        run_results.append(result)

        if not args.keep_outputs:
            shutil.rmtree(run_dir, ignore_errors=True)

    successful_runs = sum(
        1
        for item in run_results
        if item["standalone_fresh"]["returncode"] == 0 and item["coordinator_fresh"]["returncode"] == 0
        and (args.skip_resume or (item["standalone_resume"]["returncode"] == 0 and item["coordinator_resume"]["returncode"] == 0))
    )

    summary = {
        "environment_label": args.environment,
        "surface_labels": ["local", "docker", "kubernetes", "secure-mode", "minio-gated"],
        "description": "Local benchmark of standalone PyTorch vs coordinator-driven orchestration.",
        "caveats": [
            "Environment-specific local measurements; results vary across machines.",
            "Coordinator timings include coordinator startup because benchmark uses local started coordinator mode.",
            "Not evidence of distributed multi-node scaling, Kubernetes performance, or production throughput.",
            "Secure-mode and MinIO labels are declarative unless corresponding services/certs are provided.",
        ],
        "settings": json_safe(vars(args)),
        "successful_runs": successful_runs,
        "failed_runs": args.runs - successful_runs,
        "standalone_fresh_seconds": _stats([r["standalone_fresh_seconds"] for r in run_results]),
        "coordinator_fresh_seconds": _stats([r["coordinator_fresh_seconds"] for r in run_results]),
        "fresh_overhead_seconds": _stats([r["fresh_overhead_seconds"] for r in run_results]),
        "fresh_overhead_percent": _stats([r["fresh_overhead_percent"] for r in run_results if r["fresh_overhead_percent"] is not None]),
        "standalone_resume_seconds": _stats([r["standalone_resume_seconds"] for r in run_results if r["standalone_resume_seconds"] is not None]),
        "coordinator_resume_seconds": _stats([r["coordinator_resume_seconds"] for r in run_results if r["coordinator_resume_seconds"] is not None]),
        "resume_overhead_seconds": _stats([r["resume_overhead_seconds"] for r in run_results if r["resume_overhead_seconds"] is not None]),
        "resume_overhead_percent": _stats([r["resume_overhead_percent"] for r in run_results if r["resume_overhead_percent"] is not None]),
        "runs": run_results,
        "optional_surfaces": {
            "secure_mode_enabled": bool(args.secure_mode),
            "minio_enabled": bool(args.minio_enabled),
            "dataset_prefetch_summary_benchmark": "skipped: requires runtime-bench dataset prefetch benchmark command",
            "auth_audit_microbenchmark": "skipped: requires runtime-bench auth/audit microbenchmark command",
        },
    }

    json_path = args.json_out or (out_dir / "benchmark-summary.json")
    csv_path = args.csv_out or (out_dir / "benchmark-results.csv")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(json_safe(summary), indent=2), encoding="utf-8")

    fieldnames = [
        "run_index",
        "seed",
        "standalone_fresh_seconds",
        "coordinator_fresh_seconds",
        "fresh_overhead_seconds",
        "fresh_overhead_percent",
        "standalone_resume_seconds",
        "coordinator_resume_seconds",
        "resume_overhead_seconds",
        "resume_overhead_percent",
        "standalone_fresh_returncode",
        "coordinator_fresh_returncode",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in run_results:
            writer.writerow(
                {
                    "run_index": row["run_index"],
                    "seed": row["seed"],
                    "standalone_fresh_seconds": row["standalone_fresh_seconds"],
                    "coordinator_fresh_seconds": row["coordinator_fresh_seconds"],
                    "fresh_overhead_seconds": row["fresh_overhead_seconds"],
                    "fresh_overhead_percent": row["fresh_overhead_percent"],
                    "standalone_resume_seconds": row["standalone_resume_seconds"],
                    "coordinator_resume_seconds": row["coordinator_resume_seconds"],
                    "resume_overhead_seconds": row["resume_overhead_seconds"],
                    "resume_overhead_percent": row["resume_overhead_percent"],
                    "standalone_fresh_returncode": row["standalone_fresh"]["returncode"],
                    "coordinator_fresh_returncode": row["coordinator_fresh"]["returncode"],
                }
            )

    print(f"Wrote benchmark summary JSON: {json_path}")
    print(f"Wrote benchmark results CSV: {csv_path}")


if __name__ == "__main__":
    main()
