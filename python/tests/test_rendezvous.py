from __future__ import annotations

import types

import pytest

from dtruntime.rendezvous import (
    RendezvousResult,
    RendezvousTimeout,
    default_host,
    free_port,
    rendezvous,
)

_WAITING = 1
_COMPLETE = 2


def _info(
    round: int = 1,
    state: int = _WAITING,
    world_size: int = 0,
    rank: int = 0,
    has_rank: bool = False,
    store_addr: str = "",
    store_port: int = 0,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        round=round,
        state=state,
        world_size=world_size,
        rank=rank,
        has_rank=has_rank,
        store_addr=store_addr,
        store_port=store_port,
    )


class FakeClient:
    """Scripted coordinator: returns queued infos for join/poll calls."""

    def __init__(self, join_infos: list, state_infos: list) -> None:
        self._join_infos = list(join_infos)
        self._state_infos = list(state_infos)
        self.join_calls: list[dict] = []
        self.state_calls = 0

    def join_rendezvous(self, **kwargs) -> types.SimpleNamespace:
        self.join_calls.append(kwargs)
        info = self._join_infos.pop(0)
        return types.SimpleNamespace(info=info)

    def get_rendezvous_state(self, **kwargs) -> types.SimpleNamespace:
        self.state_calls += 1
        info = self._state_infos.pop(0) if self._state_infos else self._last_state
        self._last_state = info
        return types.SimpleNamespace(info=info)


def test_rendezvous_returns_result_when_join_completes_immediately() -> None:
    complete = _info(
        round=7, state=_COMPLETE, world_size=2, rank=1, has_rank=True,
        store_addr="10.0.0.1", store_port=29501,
    )
    client = FakeClient(join_infos=[complete], state_infos=[])

    result = rendezvous(client, "run-1", "w1", host="10.0.0.2", store_port=29502)

    assert result == RendezvousResult(
        round=7, rank=1, world_size=2, store_addr="10.0.0.1", store_port=29501
    )
    assert result.init_method == "tcp://10.0.0.1:29501"
    assert client.join_calls[0]["host"] == "10.0.0.2"
    assert client.join_calls[0]["store_port"] == 29502


def test_rendezvous_polls_until_round_completes() -> None:
    waiting = _info(round=3, state=_WAITING, world_size=1)
    complete = _info(
        round=3, state=_COMPLETE, world_size=2, rank=0, has_rank=True,
        store_addr="hostA", store_port=29500,
    )
    client = FakeClient(join_infos=[waiting], state_infos=[waiting, complete])

    result = rendezvous(client, "run-1", "w0", host="hostA", poll_interval=0.01)

    assert result.rank == 0
    assert client.state_calls == 2


def test_rendezvous_rejoins_when_round_completed_without_a_rank() -> None:
    # First join lands in a round that completed without this worker.
    complete_without_us = _info(
        round=5, state=_COMPLETE, world_size=2, has_rank=False,
        store_addr="other", store_port=29500,
    )
    waiting_next = _info(round=6, state=_WAITING, world_size=1)
    complete_with_us = _info(
        round=6, state=_COMPLETE, world_size=3, rank=2, has_rank=True,
        store_addr="other", store_port=29500,
    )
    client = FakeClient(
        join_infos=[complete_without_us, waiting_next],
        state_infos=[complete_with_us],
    )

    result = rendezvous(client, "run-1", "late", host="me", poll_interval=0.01)

    assert result.rank == 2
    assert len(client.join_calls) == 2
    assert client.join_calls[1]["previous_round"] == 5


def test_rendezvous_times_out_when_round_never_completes() -> None:
    waiting = _info(round=1, state=_WAITING, world_size=1)
    client = FakeClient(join_infos=[waiting], state_infos=[waiting] * 100)

    with pytest.raises(RendezvousTimeout):
        rendezvous(client, "run-1", "w0", host="me", timeout=0.05, poll_interval=0.01)


def test_free_port_returns_bindable_port() -> None:
    port = free_port()
    assert 0 < port < 65536


def test_default_host_returns_non_empty() -> None:
    assert default_host()
