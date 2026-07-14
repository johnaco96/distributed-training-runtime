#!/usr/bin/env python3
"""Merge bench_dtr / bench_dcp JSON outputs into a comparison table.

Usage:
    python report.py results-dir/ [--out report.md]

Aggregates across ranks per system: stall p50/max (over every checkpoint on
every rank), end-to-end durability time, and recovery time when measured.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def load(results_dir: Path) -> dict[str, list[dict]]:
    by_system: dict[str, list[dict]] = defaultdict(list)
    for path in sorted(results_dir.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if "system" in data:
            by_system[data["system"]].append(data)
    return by_system


def fmt_s(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 1000:.0f} ms" if value < 1 else f"{value:.2f} s"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args()

    by_system = load(args.results_dir)
    if not by_system:
        raise SystemExit(f"no result JSONs in {args.results_dir}")

    lines = [
        "| System | World | State/rank | Stall p50 | Stall max | Durable e2e | Recovery |",
        "|---|---|---|---|---|---|---|",
    ]
    for system, runs in sorted(by_system.items()):
        world = max(r.get("world_size", 1) for r in runs)
        state_mb = max(r.get("state_mb", 0) for r in runs)
        stalls = [s for r in runs for s in r.get("stall_s", [])]
        e2es = [
            e
            for r in runs
            for e in (r.get("committed_s") or r.get("e2e_s") or [])
            if e is not None
        ]
        recoveries = [r["recovery_s"] for r in runs if "recovery_s" in r]
        lines.append(
            "| {} | {} | {:.1f} GB | {} | {} | {} | {} |".format(
                system,
                world,
                state_mb / 1024,
                fmt_s(statistics.median(stalls)) if stalls else "—",
                fmt_s(max(stalls)) if stalls else "—",
                fmt_s(max(e2es)) if e2es else "—",
                fmt_s(max(recoveries)) if recoveries else "—",
            )
        )

    report = "\n".join(lines) + "\n"
    print(report)
    if args.out:
        args.out.write_text(report, encoding="utf-8")
        print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
