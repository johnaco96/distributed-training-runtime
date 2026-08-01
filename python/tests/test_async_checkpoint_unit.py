"""Unit tests for async checkpoint safety that do not require PyTorch."""

from __future__ import annotations

import dtruntime.async_checkpoint as async_checkpoint
import pytest
from dtruntime import AsyncCheckpointer, CheckpointError


def test_pickle_fallback_requires_explicit_trust(monkeypatch, tmp_path):
    """Safe defaults must never silently deserialize arbitrary pickle data."""
    monkeypatch.setattr(async_checkpoint, "_torch", lambda: None)
    path = tmp_path / "state.pt"
    state = {"step": 7, "weights": [1.0, 2.0]}

    with pytest.raises(CheckpointError, match="weights_only=False"):
        async_checkpoint._serialize_to(path, state)

    async_checkpoint._serialize_to(path, state, allow_pickle=True)
    checkpointer = object.__new__(AsyncCheckpointer)
    checkpointer.weights_only = True
    with pytest.raises(CheckpointError, match="weights_only=False"):
        checkpointer._load(path)

    checkpointer.weights_only = False
    assert checkpointer._load(path) == state


def test_failed_serialization_removes_temporary_file(monkeypatch, tmp_path):
    class Unserializable:
        def __reduce__(self):
            raise RuntimeError("serialization failed")

    monkeypatch.setattr(async_checkpoint, "_torch", lambda: None)
    path = tmp_path / "state.pt"

    with pytest.raises(RuntimeError, match="serialization failed"):
        async_checkpoint._serialize_to(
            path, {"not_picklable": Unserializable()}, allow_pickle=True
        )

    assert not path.with_suffix(".pt.tmp").exists()
