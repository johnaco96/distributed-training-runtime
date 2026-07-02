"""Worker launcher: coordinator-backed equivalent of ``torchrun``.

Runs one node-level agent that registers the run with the coordinator (the
registration is idempotent, so every node of a multi-node job can run the same
command), spawns ``--nproc-per-node`` training processes, and supervises them:

- a process that exits non-zero is restarted while the restart budget lasts
  (the replacement re-joins the coordinator rendezvous, which reassigns its
  rank and signals the surviving workers to re-rendezvous);
- a process that exits zero is treated as completed;
- when the restart budget is exhausted the whole node fails.

Fixed-size example (2 nodes x 2 processes)::

    # node A and node B both run:
    python -m dtruntime.launch \
        --coordinator coordinator:50051 --run-name my-run \
        --nproc-per-node 2 --world-size 4 \
        train.py --epochs 10

Elastic example (any 2..8 workers)::

    python -m dtruntime.launch \
        --coordinator coordinator:50051 --run-name my-run \
        --nproc-per-node 4 --min-workers 2 --max-workers 8 \
        train.py

The training script calls :func:`dtruntime.torch_dist.init` which reads the
``DTRUNTIME_*`` environment variables this launcher injects.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Optional, Sequence

logger = logging.getLogger("dtruntime.launch")

_POLL_INTERVAL_SECONDS = 0.5


@dataclass
class LaunchConfig:
    coordinator: str
    run_name: str
    script: str
    script_args: list[str] = field(default_factory=list)
    nproc_per_node: int = 1
    world_size: int = 0
    min_workers: int = 0
    max_workers: int = 0
    settle_timeout_ms: int = 0
    max_restarts: int = 3
    backend: Optional[str] = None
    run_id: Optional[str] = None
    node_id: Optional[str] = None
    worker_host: Optional[str] = None

    @property
    def is_elastic(self) -> bool:
        return self.min_workers > 0

    def resolved_world_size(self) -> int:
        if self.is_elastic:
            return self.max_workers or self.min_workers
        return self.world_size or self.nproc_per_node

    def resolved_node_id(self) -> str:
        return self.node_id or socket.gethostname()


def parse_args(argv: Sequence[str] | None = None) -> LaunchConfig:
    parser = argparse.ArgumentParser(
        prog="python -m dtruntime.launch",
        description="Launch and supervise coordinator-backed distributed training workers.",
    )
    parser.add_argument("--coordinator", required=True, help="coordinator gRPC address host:port")
    parser.add_argument("--run-name", required=True, help="run name; also the idempotency scope")
    parser.add_argument("--run-id", default=None, help="join an already-registered run instead of registering one")
    parser.add_argument("--nproc-per-node", type=int, default=1, help="training processes on this node")
    parser.add_argument(
        "--world-size",
        type=int,
        default=0,
        help="total workers across all nodes for fixed-size runs (default: nproc-per-node)",
    )
    parser.add_argument("--min-workers", type=int, default=0, help="elastic lower bound (enables elastic mode)")
    parser.add_argument("--max-workers", type=int, default=0, help="elastic upper bound")
    parser.add_argument(
        "--settle-timeout-ms",
        type=int,
        default=0,
        help="elastic rendezvous settle window after min-workers is reached (0 = server default)",
    )
    parser.add_argument("--max-restarts", type=int, default=3, help="total worker restarts before the node fails")
    parser.add_argument("--backend", default=None, help="torch.distributed backend (default: nccl if CUDA else gloo)")
    parser.add_argument("--node-id", default=None, help="stable node identifier (default: hostname)")
    parser.add_argument(
        "--worker-host",
        default=None,
        help="address other nodes can reach this node's workers on (default: autodetect)",
    )
    parser.add_argument("script", help="training script to run")
    parser.add_argument("script_args", nargs=argparse.REMAINDER, help="arguments passed to the script")
    args = parser.parse_args(argv)

    if args.min_workers > 0 and args.max_workers < args.min_workers:
        parser.error("--max-workers must be >= --min-workers")
    if args.min_workers == 0 and args.max_workers > 0:
        parser.error("--min-workers is required when --max-workers is set")
    if args.nproc_per_node < 1:
        parser.error("--nproc-per-node must be >= 1")

    return LaunchConfig(
        coordinator=args.coordinator,
        run_name=args.run_name,
        run_id=args.run_id,
        script=args.script,
        script_args=list(args.script_args),
        nproc_per_node=args.nproc_per_node,
        world_size=args.world_size,
        min_workers=args.min_workers,
        max_workers=args.max_workers,
        settle_timeout_ms=args.settle_timeout_ms,
        max_restarts=args.max_restarts,
        backend=args.backend,
        node_id=args.node_id,
        worker_host=args.worker_host,
    )


def register_run(config: LaunchConfig) -> str:
    """Register (or idempotently re-register) the run and return its run id.

    Every node of a multi-node job runs this with the same run name; the
    idempotency key makes them all converge on one run.
    """
    if config.run_id:
        return config.run_id

    from dtruntime.client import RuntimeClient
    from dtruntime.generated import runtime_pb2

    client = RuntimeClient(config.coordinator)
    try:
        request = runtime_pb2.RegisterRunRequest(
            idempotency_key=f"launch-{config.run_name}",
            run_name=config.run_name,
            assignment_policy=runtime_pb2.ASSIGNMENT_POLICY_STATIC_RANK,
            storage_backend_kind=runtime_pb2.STORAGE_BACKEND_KIND_LOCAL_FILESYSTEM,
            world_size=config.resolved_world_size(),
            min_world_size=config.min_workers,
            max_world_size=config.max_workers,
            rendezvous_settle_timeout_ms=config.settle_timeout_ms,
        )
        response = client.stub.RegisterRun(request, metadata=client.rpc_metadata())
        return str(response.run_id.value)
    finally:
        client.close()


def worker_env(config: LaunchConfig, run_id: str, local_rank: int) -> dict[str, str]:
    """Environment injected into one worker process."""
    node_id = config.resolved_node_id()
    env = dict(os.environ)
    env.update(
        {
            "DTRUNTIME_COORDINATOR_ADDR": config.coordinator,
            "DTRUNTIME_RUN_ID": run_id,
            "DTRUNTIME_WORKER_ID": f"{node_id}-{local_rank}",
            "DTRUNTIME_LOCAL_RANK": str(local_rank),
            # Also set the conventional torch var so user code and libraries
            # that read LOCAL_RANK keep working.
            "LOCAL_RANK": str(local_rank),
        }
    )
    if config.worker_host:
        env["DTRUNTIME_WORKER_HOST"] = config.worker_host
    if config.backend:
        env["DTRUNTIME_TORCH_BACKEND"] = config.backend
    return env


@dataclass
class _Child:
    local_rank: int
    process: subprocess.Popen
    restarts: int = 0


class WorkerSupervisor:
    """Spawns and supervises the node's worker processes."""

    def __init__(self, config: LaunchConfig, run_id: str) -> None:
        self._config = config
        self._run_id = run_id
        self._children: list[_Child] = []
        self._restarts_used = 0

    def _spawn(self, local_rank: int) -> subprocess.Popen:
        command = [sys.executable, self._config.script, *self._config.script_args]
        env = worker_env(self._config, self._run_id, local_rank)
        logger.info("starting worker local_rank=%d: %s", local_rank, " ".join(command))
        return subprocess.Popen(command, env=env)

    def start(self) -> None:
        for local_rank in range(self._config.nproc_per_node):
            self._children.append(_Child(local_rank, self._spawn(local_rank)))

    def supervise(self) -> int:
        """Block until all workers complete; returns the process exit code."""
        try:
            while True:
                running = 0
                for child in self._children:
                    code = child.process.poll()
                    if code is None:
                        running += 1
                        continue
                    if code == 0:
                        continue
                    if self._restarts_used >= self._config.max_restarts:
                        logger.error(
                            "worker local_rank=%d exited with %d and the restart "
                            "budget (%d) is exhausted; failing the node",
                            child.local_rank,
                            code,
                            self._config.max_restarts,
                        )
                        self.terminate_all()
                        return 1
                    self._restarts_used += 1
                    child.restarts += 1
                    logger.warning(
                        "worker local_rank=%d exited with %d; restarting "
                        "(restart %d/%d)",
                        child.local_rank,
                        code,
                        self._restarts_used,
                        self._config.max_restarts,
                    )
                    child.process = self._spawn(child.local_rank)
                    running += 1
                if running == 0:
                    logger.info("all workers completed")
                    return 0
                time.sleep(_POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("interrupted; terminating workers")
            self.terminate_all()
            return 130

    def terminate_all(self) -> None:
        for child in self._children:
            if child.process.poll() is None:
                child.process.terminate()
        deadline = time.monotonic() + 10
        for child in self._children:
            remaining = max(0.0, deadline - time.monotonic())
            try:
                child.process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                child.process.kill()


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    config = parse_args(argv)

    run_id = register_run(config)
    logger.info("run %s registered (name=%s)", run_id, config.run_name)

    supervisor = WorkerSupervisor(config, run_id)

    def _handle_term(signum: int, frame: object) -> None:  # pragma: no cover
        supervisor.terminate_all()
        raise SystemExit(143)

    signal.signal(signal.SIGTERM, _handle_term)

    supervisor.start()
    return supervisor.supervise()


if __name__ == "__main__":
    raise SystemExit(main())
