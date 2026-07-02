"""Client-side rendezvous for torch.distributed process-group formation.

Workers call :func:`rendezvous` after registering with the coordinator.  The
call joins the coordinator's rendezvous barrier and polls until the round
completes, returning the rank assignment and the c10d TCPStore endpoint that
every worker passes to ``torch.distributed.init_process_group``.
"""

from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dtruntime.client import RuntimeClient


class RendezvousError(RuntimeError):
    """Raised when a rendezvous cannot be completed."""


class RendezvousTimeout(RendezvousError):
    """Raised when the rendezvous did not complete within the timeout."""


@dataclass(frozen=True)
class RendezvousResult:
    """Completed rendezvous round as seen by one worker."""

    round: int
    rank: int
    world_size: int
    store_addr: str
    store_port: int

    @property
    def init_method(self) -> str:
        """torch.distributed init_method URL for this round."""
        return f"tcp://{self.store_addr}:{self.store_port}"


def free_port(host: str = "") -> int:
    """Bind an ephemeral port and return it.

    Used to propose a fresh TCPStore port on every join so a re-elected rank 0
    never collides with a lingering store from a previous round.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host or "0.0.0.0", 0))
        return int(sock.getsockname()[1])


def default_host() -> str:
    """Best-effort address other workers on the network can reach."""
    hostname = socket.gethostname()
    try:
        return socket.gethostbyname(hostname)
    except OSError:
        return hostname


def rendezvous(
    client: "RuntimeClient",
    run_id: str,
    worker_id: str,
    host: str | None = None,
    store_port: int | None = None,
    timeout: float = 600.0,
    poll_interval: float = 0.5,
) -> RendezvousResult:
    """Join the coordinator rendezvous and block until the round completes.

    The returned :class:`RendezvousResult` carries this worker's rank, the
    agreed world size, and the TCPStore endpoint for
    ``torch.distributed.init_process_group``.

    Raises :class:`RendezvousTimeout` if the round does not complete within
    ``timeout`` seconds (for example when too few workers join a fixed-size
    run).
    """
    resolved_host = host or default_host()
    proposed_port = store_port if store_port is not None else free_port()

    response = client.join_rendezvous(
        run_id=run_id,
        worker_id=worker_id,
        host=resolved_host,
        store_port=proposed_port,
    )
    info = response.info
    joined_round = info.round

    deadline = time.monotonic() + timeout
    while True:
        if _is_complete(info):
            if info.has_rank:
                return RendezvousResult(
                    round=info.round,
                    rank=info.rank,
                    world_size=info.world_size,
                    store_addr=info.store_addr,
                    store_port=info.store_port,
                )
            # The round completed without ranking this worker (it arrived
            # after an elastic round filled up).  Re-join to trigger the next
            # round rather than spinning on a round we can never be part of.
            response = client.join_rendezvous(
                run_id=run_id,
                worker_id=worker_id,
                host=resolved_host,
                store_port=proposed_port,
                previous_round=joined_round,
            )
            info = response.info
            joined_round = info.round

        if time.monotonic() >= deadline:
            raise RendezvousTimeout(
                f"rendezvous for run {run_id} did not complete within {timeout:.0f}s "
                f"(round {info.round}, {info.world_size} worker(s) joined)"
            )
        time.sleep(poll_interval)
        info = client.get_rendezvous_state(run_id=run_id, worker_id=worker_id).info


def _is_complete(info: object) -> bool:
    # RENDEZVOUS_ROUND_STATE_COMPLETE == 2; compared numerically so this module
    # does not import generated enums at module load time.
    return int(getattr(info, "state", 0)) == 2
