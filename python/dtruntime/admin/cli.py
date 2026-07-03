"""``dtruntime-admin`` — operator CLI for inspecting and managing runs.

Command groups::

    runs list [--state STATE]
    runs describe RUN_ID
    run cancel|pause|resume|retry|drain RUN_ID [--reason TEXT]
    workers list RUN_ID [--state STATE]
    shards list RUN_ID [--dataset DATASET_ID]
    checkpoints list RUN_ID [--committed-only] [--limit N]
    checkpoint verify RUN_ID CHECKPOINT_ID [--worker-id ID] [--report-corrupt]
    doctor [RUN_ID]

Global options ``--address/-a``, ``--timeout``, and ``--json`` apply to every
command.  TLS and caller-identity (auth) settings are read from the standard
``DTRUNTIME_TLS_*`` / ``DTRUNTIME_AUTH_*`` environment variables understood by
:class:`dtruntime.client.RuntimeClient`; admin operations require the ``admin``
role.  Every failure is reported through :mod:`dtruntime.admin.errors` with a
remediation hint and a stable exit code.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from . import errors, format

DEFAULT_ADDRESS = os.getenv("DTRUNTIME_COORDINATOR_ADDRESS", "127.0.0.1:50051")

_RUN_STATE_PREFIX = "RUN_STATE_"
_WORKER_STATE_PREFIX = "WORKER_STATE_"
_CHECKPOINT_STATE_PREFIX = "CHECKPOINT_STATE_"


@dataclass
class CommandResult:
    """A command's output: a JSON-serializable payload and a human rendering."""

    data: Any
    text: str
    # Non-zero to signal a diagnostic failure (e.g. doctor found problems)
    # even though the RPCs themselves succeeded.
    exit_code: int = errors.EXIT_OK


# ── enum <-> label helpers (need the generated module for the enum tables) ──


def _run_state_label(pb2: Any, value: int) -> str:
    return format.strip_enum_prefix(pb2.RunState.Name(value), _RUN_STATE_PREFIX)


def _worker_state_label(pb2: Any, value: int) -> str:
    return format.strip_enum_prefix(pb2.WorkerState.Name(value), _WORKER_STATE_PREFIX)


def _checkpoint_state_label(pb2: Any, value: int) -> str:
    return format.strip_enum_prefix(pb2.CheckpointState.Name(value), _CHECKPOINT_STATE_PREFIX)


def _parse_run_state(pb2: Any, value: str) -> int:
    token = format.add_enum_prefix(value, _RUN_STATE_PREFIX)
    try:
        return pb2.RunState.Value(token)
    except ValueError:
        valid = ", ".join(
            format.strip_enum_prefix(n, _RUN_STATE_PREFIX)
            for n in pb2.RunState.keys()
            if n != "RUN_STATE_UNSPECIFIED"
        )
        raise errors.AdminError(
            f"unknown run state {value!r}",
            remediation=f"Valid states: {valid}.",
            exit_code=errors.EXIT_USAGE,
        )


def _parse_worker_state(pb2: Any, value: str) -> int:
    token = format.add_enum_prefix(value, _WORKER_STATE_PREFIX)
    try:
        return pb2.WorkerState.Value(token)
    except ValueError:
        valid = ", ".join(
            format.strip_enum_prefix(n, _WORKER_STATE_PREFIX)
            for n in pb2.WorkerState.keys()
            if n != "WORKER_STATE_UNSPECIFIED"
        )
        raise errors.AdminError(
            f"unknown worker state {value!r}",
            remediation=f"Valid states: {valid}.",
            exit_code=errors.EXIT_USAGE,
        )


def _artifact_owner(artifact_uri: str) -> str:
    """Uploading worker id from ``.../workers/<worker_id>/<artifact...>``.

    Inlined from :mod:`dtruntime.checkpointing` so the CLI does not import the
    torch-dependent checkpointing module for basic admin work.
    """
    parts = artifact_uri.replace("\\", "/").split("/")
    try:
        index = len(parts) - 1 - parts[::-1].index("workers")
        return parts[index + 1]
    except (ValueError, IndexError):
        return ""


# ── command handlers ────────────────────────────────────────────────────────


def cmd_runs_list(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    state_filter = _parse_run_state(pb2, args.state) if args.state else 0
    resp = client.list_runs(state_filter=state_filter, timeout=args.timeout)
    rows = []
    data = []
    for r in resp.runs:
        state = _run_state_label(pb2, r.state)
        ckpt = r.latest_checkpoint_id.value if r.latest_checkpoint_id.value else "-"
        data.append(
            {
                "run_id": r.run_id.value,
                "run_name": r.run_name,
                "state": state,
                "world_size": r.world_size,
                "active_workers": r.active_worker_count,
                "retry_count": r.retry_count,
                "latest_checkpoint_id": ckpt if ckpt != "-" else None,
                "registered_at": format.format_timestamp(r.registered_at),
            }
        )
        rows.append(
            [
                r.run_id.value,
                r.run_name,
                state,
                f"{r.active_worker_count}/{r.world_size}",
                r.retry_count,
                ckpt,
                format.format_timestamp(r.registered_at),
            ]
        )
    text = format.render_table(
        ["RUN ID", "NAME", "STATE", "WORKERS", "RETRIES", "CHECKPOINT", "REGISTERED"],
        rows,
    )
    return CommandResult(data=data, text=text)


def _find_run_summary(client: Any, pb2: Any, run_id: str, timeout: Optional[float]) -> Any:
    resp = client.list_runs(state_filter=0, timeout=timeout)
    for r in resp.runs:
        if r.run_id.value == run_id:
            return r
    raise errors.AdminError(
        f"run {run_id!r} was not found",
        remediation="List known runs with 'dtruntime-admin runs list'.",
        exit_code=errors.EXIT_NOT_FOUND,
        status="NOT_FOUND",
    )


def cmd_runs_describe(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    summary = _find_run_summary(client, pb2, args.run_id, args.timeout)
    workers_resp = client.list_workers(run_id=args.run_id, state_filter=0, timeout=args.timeout)
    state = _run_state_label(pb2, summary.state)
    ckpt = summary.latest_checkpoint_id.value or None
    data = {
        "run_id": summary.run_id.value,
        "run_name": summary.run_name,
        "state": state,
        "state_reason": summary.state_reason,
        "world_size": summary.world_size,
        "active_workers": summary.active_worker_count,
        "retry_count": summary.retry_count,
        "latest_checkpoint_id": ckpt,
        "latest_checkpoint_state": _checkpoint_state_label(pb2, summary.latest_checkpoint_state),
        "registered_at": format.format_timestamp(summary.registered_at),
        "workers": [
            {
                "worker_id": w.worker_id.value,
                "rank": w.rank,
                "state": _worker_state_label(pb2, w.state),
                "host": w.host,
            }
            for w in workers_resp.workers
        ],
    }
    kv = format.render_kv(
        [
            ("run id", summary.run_id.value),
            ("name", summary.run_name),
            ("state", state),
            ("reason", summary.state_reason or "-"),
            ("world size", summary.world_size),
            ("active workers", summary.active_worker_count),
            ("retries", summary.retry_count),
            ("latest checkpoint", ckpt or "-"),
            ("checkpoint state", data["latest_checkpoint_state"]),
            ("registered", format.format_timestamp(summary.registered_at)),
        ]
    )
    worker_rows = [
        [w.worker_id.value, w.rank, _worker_state_label(pb2, w.state), w.host]
        for w in sorted(workers_resp.workers, key=lambda w: w.rank)
    ]
    worker_table = format.render_table(["WORKER ID", "RANK", "STATE", "HOST"], worker_rows)
    return CommandResult(data=data, text=f"{kv}\n\nWorkers:\n{worker_table}")


def _run_action_result(pb2: Any, run_id: str, action: str, resp: Any) -> CommandResult:
    state = _run_state_label(pb2, resp.state)
    reason = getattr(resp, "state_reason", "")
    data = {"run_id": run_id, "action": action, "state": state, "state_reason": reason}
    text = f"Run {run_id} {action}: now {state}"
    if reason:
        text += f" ({reason})"
    return CommandResult(data=data, text=text)


def cmd_run_cancel(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.cancel_run(run_id=args.run_id, reason=args.reason)
    return _run_action_result(pb2, args.run_id, "cancelled", resp)


def cmd_run_pause(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.pause_run(run_id=args.run_id, reason=args.reason, timeout=args.timeout)
    return _run_action_result(pb2, args.run_id, "paused", resp)


def cmd_run_resume(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.resume_run(run_id=args.run_id, reason=args.reason, timeout=args.timeout)
    return _run_action_result(pb2, args.run_id, "resumed", resp)


def cmd_run_retry(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.retry_run(run_id=args.run_id, reason=args.reason, timeout=args.timeout)
    return _run_action_result(pb2, args.run_id, "retried", resp)


def cmd_run_drain(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.drain_run(run_id=args.run_id, reason=args.reason, timeout=args.timeout)
    return _run_action_result(pb2, args.run_id, "draining", resp)


def cmd_workers_list(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    state_filter = _parse_worker_state(pb2, args.state) if args.state else 0
    resp = client.list_workers(run_id=args.run_id, state_filter=state_filter, timeout=args.timeout)
    now = resp.server_time
    rows = []
    data = []
    for w in resp.workers:
        state = _worker_state_label(pb2, w.state)
        data.append(
            {
                "worker_id": w.worker_id.value,
                "rank": w.rank,
                "state": state,
                "host": w.host,
                "sdk_version": w.sdk_version,
                "last_heartbeat_at": format.format_timestamp(w.last_heartbeat_at),
                "heartbeat_deadline": format.relative_deadline(w.next_heartbeat_deadline, now),
                "failure_deadline": format.relative_deadline(w.failure_deadline, now),
            }
        )
        rows.append(
            [
                w.rank,
                w.worker_id.value,
                state,
                w.host,
                w.sdk_version or "-",
                format.format_timestamp(w.last_heartbeat_at),
                format.relative_deadline(w.next_heartbeat_deadline, now),
                format.relative_deadline(w.failure_deadline, now),
            ]
        )
    rows.sort(key=lambda r: r[0])
    text = format.render_table(
        ["RANK", "WORKER ID", "STATE", "HOST", "SDK", "LAST HB", "HB DUE", "FAIL DUE"],
        rows,
    )
    return CommandResult(data=data, text=text)


def cmd_shards_list(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.list_shard_progress(
        run_id=args.run_id, dataset_id=args.dataset or "", timeout=args.timeout
    )
    rows = []
    data = []
    for s in resp.shards:
        data.append(
            {
                "shard_id": s.shard_id.value,
                "dataset_id": s.dataset_id.value,
                "worker_id": s.worker_id.value or None,
                "lease_epoch": s.lease_epoch,
                "progress_offset": s.progress_offset,
                "is_complete": s.is_complete,
                "updated_at": format.format_timestamp(s.updated_at),
            }
        )
        rows.append(
            [
                s.shard_id.value,
                s.dataset_id.value,
                s.worker_id.value or "-",
                s.lease_epoch,
                s.progress_offset,
                "yes" if s.is_complete else "no",
                format.format_timestamp(s.updated_at),
            ]
        )
    text = format.render_table(
        ["SHARD ID", "DATASET", "WORKER", "EPOCH", "OFFSET", "DONE", "UPDATED"],
        rows,
    )
    return CommandResult(data=data, text=text)


def cmd_checkpoints_list(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.list_checkpoints(
        run_id=args.run_id, committed_only=args.committed_only, limit=args.limit
    )
    rows = []
    data = []
    for c in resp.checkpoints:
        state = _checkpoint_state_label(pb2, c.state)
        total_bytes = sum(a.size_bytes for a in c.artifacts)
        data.append(
            {
                "checkpoint_id": c.checkpoint_id.value,
                "step": c.step,
                "state": state,
                "num_artifacts": len(c.artifacts),
                "total_bytes": total_bytes,
                "created_at": format.format_timestamp(c.created_at),
                "committed_at": format.format_timestamp(c.committed_at),
            }
        )
        rows.append(
            [
                c.checkpoint_id.value,
                c.step,
                state,
                len(c.artifacts),
                total_bytes,
                format.format_timestamp(c.committed_at),
            ]
        )
    text = format.render_table(
        ["CHECKPOINT ID", "STEP", "STATE", "ARTIFACTS", "BYTES", "COMMITTED"],
        rows,
    )
    return CommandResult(data=data, text=text)


def cmd_checkpoint_verify(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    resp = client.list_checkpoints(run_id=args.run_id, committed_only=False, limit=0)
    manifest = next(
        (c for c in resp.checkpoints if c.checkpoint_id.value == args.checkpoint_id), None
    )
    if manifest is None:
        raise errors.AdminError(
            f"checkpoint {args.checkpoint_id!r} was not found for run {args.run_id!r}",
            remediation="List checkpoints with 'dtruntime-admin checkpoints list <run-id>'.",
            exit_code=errors.EXIT_NOT_FOUND,
            status="NOT_FOUND",
        )

    artifact_results = []
    problems = []
    with tempfile.TemporaryDirectory(prefix="dtruntime-verify-") as tmp:
        tmp_dir = Path(tmp)
        for artifact in manifest.artifacts:
            owner = _artifact_owner(artifact.artifact_uri)
            entry: dict[str, Any] = {"artifact_name": artifact.artifact_name, "ok": False}
            if not owner:
                entry["error"] = "could not determine owning worker from artifact uri"
                problems.append(f"{artifact.artifact_name}: {entry['error']}")
                artifact_results.append(entry)
                continue
            destination = tmp_dir / artifact.artifact_name
            try:
                size, checksum = client.fetch_checkpoint_artifact(
                    run_id=args.run_id,
                    worker_id=args.worker_id,
                    checkpoint_id=args.checkpoint_id,
                    artifact_name=artifact.artifact_name,
                    owner_worker_id=owner,
                    destination=destination,
                )
            except Exception as exc:  # noqa: BLE001 - reported as corruption
                entry["error"] = f"unreadable: {errors.from_rpc_error(exc).message}"
                problems.append(f"{artifact.artifact_name}: {entry['error']}")
                artifact_results.append(entry)
                continue
            if artifact.checksum and checksum and checksum != artifact.checksum:
                entry["error"] = f"checksum mismatch: {checksum} != {artifact.checksum}"
                problems.append(f"{artifact.artifact_name}: {entry['error']}")
            elif artifact.size_bytes and size != artifact.size_bytes:
                entry["error"] = f"size mismatch: {size} != {artifact.size_bytes}"
                problems.append(f"{artifact.artifact_name}: {entry['error']}")
            else:
                entry["ok"] = True
                entry["size_bytes"] = size
            artifact_results.append(entry)

    ok = not problems
    data = {
        "checkpoint_id": args.checkpoint_id,
        "run_id": args.run_id,
        "state": _checkpoint_state_label(pb2, manifest.state),
        "verified": ok,
        "artifacts": artifact_results,
    }
    reported = False
    if problems and args.report_corrupt:
        reason = "; ".join(problems)[:500]
        client.report_checkpoint_corrupt(
            run_id=args.run_id,
            worker_id=args.worker_id,
            checkpoint_id=args.checkpoint_id,
            reason=f"admin verify: {reason}",
        )
        reported = True
        data["reported_corrupt"] = True

    if ok:
        text = (
            f"checkpoint {args.checkpoint_id} OK: "
            f"{len(artifact_results)} artifact(s) verified"
        )
        return CommandResult(data=data, text=text)
    lines = [f"checkpoint {args.checkpoint_id} FAILED verification:"]
    lines.extend(f"  - {p}" for p in problems)
    if reported:
        lines.append("  (reported corrupt; latest pointer will fall back)")
    return CommandResult(data=data, text="\n".join(lines), exit_code=errors.EXIT_INVALID_STATE)


def cmd_doctor(client: Any, pb2: Any, args: argparse.Namespace) -> CommandResult:
    # Coordinator handshake first — confirms reachability + protocol.
    info = client.get_runtime_info(timeout=args.timeout)
    pv = info.protocol_version
    protocol = f"{pv.major}.{pv.minor}.{pv.patch}"
    features = list(info.capabilities.features)
    data: dict[str, Any] = {
        "coordinator": {"reachable": True, "protocol_version": protocol, "features": features}
    }
    lines = [f"coordinator reachable - protocol {protocol}"]

    if not args.run_id:
        lines.append(f"capabilities: {', '.join(features) if features else '(none)'}")
        return CommandResult(data=data, text="\n".join(lines))

    # Per-run diagnosis: stuck workers + bad checkpoints.
    summary = _find_run_summary(client, pb2, args.run_id, args.timeout)
    workers_resp = client.list_workers(run_id=args.run_id, state_filter=0, timeout=args.timeout)
    now = workers_resp.server_time
    findings = []

    run_state = _run_state_label(pb2, summary.state)
    lines.append("")
    lines.append(f"run {args.run_id}: state={run_state}, reason={summary.state_reason or '-'}")

    stuck_states = {"stale", "failed", "recovering"}
    stuck_workers = []
    for w in workers_resp.workers:
        wstate = _worker_state_label(pb2, w.state)
        overdue = (
            w.failure_deadline.seconds != 0
            and now.seconds != 0
            and w.failure_deadline.seconds < now.seconds
        )
        if wstate in stuck_states or overdue:
            note = wstate + (" (heartbeat overdue)" if overdue else "")
            stuck_workers.append({"worker_id": w.worker_id.value, "rank": w.rank, "state": note})
            findings.append(f"worker {w.worker_id.value} (rank {w.rank}) is {note}")

    if summary.active_worker_count == 0 and run_state in {"running", "recovering", "pending"}:
        findings.append("run has no active workers but is not terminal (possible stuck launch)")

    # Checkpoint health.
    ckpt_resp = client.list_checkpoints(run_id=args.run_id, committed_only=False, limit=0)
    committed = [
        c
        for c in ckpt_resp.checkpoints
        if _checkpoint_state_label(pb2, c.state) == "committed"
    ]
    failed_ckpts = [
        c.checkpoint_id.value
        for c in ckpt_resp.checkpoints
        if _checkpoint_state_label(pb2, c.state) in {"failed", "aborted"}
    ]
    if not committed:
        findings.append("no committed checkpoint exists - a restart would start from scratch")
    for cid in failed_ckpts:
        findings.append(f"checkpoint {cid} is in a failed/aborted state")

    data["run"] = {
        "run_id": args.run_id,
        "state": run_state,
        "active_workers": summary.active_worker_count,
        "stuck_workers": stuck_workers,
        "committed_checkpoints": len(committed),
        "failed_checkpoints": failed_ckpts,
        "findings": findings,
    }

    if findings:
        lines.append("issues:")
        lines.extend(f"  - {f}" for f in findings)
        return CommandResult(data=data, text="\n".join(lines), exit_code=errors.EXIT_INVALID_STATE)
    lines.append("no issues detected")
    return CommandResult(data=data, text="\n".join(lines))


# ── argument parsing ────────────────────────────────────────────────────────


def _add_reason(p: argparse.ArgumentParser) -> None:
    p.add_argument("--reason", default="", help="human-readable reason recorded in the audit log")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dtruntime-admin",
        description="Inspect and manage Distributed Training Runtime runs.",
    )
    parser.add_argument(
        "-a",
        "--address",
        default=DEFAULT_ADDRESS,
        help=f"coordinator gRPC address (default: {DEFAULT_ADDRESS}; "
        "env DTRUNTIME_COORDINATOR_ADDRESS)",
    )
    parser.add_argument(
        "--timeout", type=float, default=None, help="per-RPC timeout in seconds"
    )
    parser.add_argument(
        "--token",
        default=None,
        help="OIDC/JWT bearer token for authentication (env DTRUNTIME_AUTH_TOKEN)",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    sub = parser.add_subparsers(dest="group", required=True)

    # runs
    runs = sub.add_parser("runs", help="list or describe runs").add_subparsers(
        dest="command", required=True
    )
    p = runs.add_parser("list", help="list runs")
    p.add_argument("--state", help="filter by run state (e.g. running, failed, paused)")
    p.set_defaults(func=cmd_runs_list)
    p = runs.add_parser("describe", help="describe a run and its workers")
    p.add_argument("run_id")
    p.set_defaults(func=cmd_runs_describe)

    # run (singular) actions
    run = sub.add_parser("run", help="administrative actions on a run").add_subparsers(
        dest="command", required=True
    )
    for name, func in [
        ("cancel", cmd_run_cancel),
        ("pause", cmd_run_pause),
        ("resume", cmd_run_resume),
        ("retry", cmd_run_retry),
        ("drain", cmd_run_drain),
    ]:
        p = run.add_parser(name, help=f"{name} a run")
        p.add_argument("run_id")
        _add_reason(p)
        p.set_defaults(func=func)

    # workers
    workers = sub.add_parser("workers", help="list workers").add_subparsers(
        dest="command", required=True
    )
    p = workers.add_parser("list", help="list a run's workers")
    p.add_argument("run_id")
    p.add_argument("--state", help="filter by worker state (e.g. active, failed, stale)")
    p.set_defaults(func=cmd_workers_list)

    # shards
    shards = sub.add_parser("shards", help="list shard progress/leases").add_subparsers(
        dest="command", required=True
    )
    p = shards.add_parser("list", help="list a run's shard progress")
    p.add_argument("run_id")
    p.add_argument("--dataset", help="restrict to one dataset id")
    p.set_defaults(func=cmd_shards_list)

    # checkpoints
    checkpoints = sub.add_parser("checkpoints", help="list checkpoints").add_subparsers(
        dest="command", required=True
    )
    p = checkpoints.add_parser("list", help="list a run's checkpoints")
    p.add_argument("run_id")
    p.add_argument("--committed-only", action="store_true", help="only committed checkpoints")
    p.add_argument("--limit", type=int, default=0, help="max checkpoints (0 = all)")
    p.set_defaults(func=cmd_checkpoints_list)

    # checkpoint (singular) verify
    checkpoint = sub.add_parser(
        "checkpoint", help="inspect/verify a single checkpoint"
    ).add_subparsers(dest="command", required=True)
    p = checkpoint.add_parser(
        "verify", help="download and checksum-verify a checkpoint's artifacts"
    )
    p.add_argument("run_id")
    p.add_argument("checkpoint_id")
    p.add_argument(
        "--worker-id",
        default="dtruntime-admin",
        help="caller worker id used for the fetch RPC (default: dtruntime-admin)",
    )
    p.add_argument(
        "--report-corrupt",
        action="store_true",
        help="mark the checkpoint corrupt if verification fails",
    )
    p.set_defaults(func=cmd_checkpoint_verify)

    # doctor
    p = sub.add_parser(
        "doctor",
        help="diagnose coordinator reachability and (optionally) a run's health",
    )
    p.add_argument("run_id", nargs="?", help="run to diagnose (omit for a coordinator probe)")
    p.set_defaults(func=cmd_doctor, group="doctor", command="doctor")

    return parser


def _build_client(address: str, token: Optional[str] = None) -> Any:
    from dtruntime.client import RuntimeClient

    return RuntimeClient(address, auth_token=token)


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        from dtruntime.generated import runtime_pb2 as pb2
    except ImportError:  # pragma: no cover - packaging safety net
        err = errors.AdminError(
            "generated protobuf modules are unavailable",
            remediation="Reinstall dtruntime and regenerate stubs "
            "(python scripts/generate_python_proto.py).",
        )
        return _emit_error(err, args)

    try:
        client = _build_client(args.address, token=args.token)
    except Exception as exc:  # noqa: BLE001
        return _emit_error(errors.from_rpc_error(exc), args)

    try:
        result = args.func(client, pb2, args)
    except errors.AdminError as exc:
        return _emit_error(exc, args)
    except Exception as exc:  # noqa: BLE001 - normalize any RPC/other failure
        return _emit_error(errors.from_rpc_error(exc), args)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            try:
                close()
            except Exception:  # pragma: no cover - defensive
                pass

    if args.json:
        print(json.dumps(result.data, indent=2, default=str))
    else:
        print(result.text)
    return result.exit_code


def _emit_error(err: errors.AdminError, args: argparse.Namespace) -> int:
    if getattr(args, "json", False):
        print(json.dumps(err.to_dict(), indent=2), file=sys.stderr)
    else:
        print(f"error: {err.message}", file=sys.stderr)
        if err.remediation:
            print(f"  hint: {err.remediation}", file=sys.stderr)
    return err.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
