from __future__ import annotations

import pytest
from dtruntime.strategies import SUPPORTED_STRATEGIES, StrategyError, wrap_model


def test_unknown_strategy_raises_with_supported_list() -> None:
    with pytest.raises(StrategyError, match="ddp"):
        wrap_model(object(), strategy="tensor-parallel")


def test_none_strategy_returns_model_unchanged() -> None:
    model = object()
    assert wrap_model(model, strategy="none") is model


def test_ddp_requires_initialised_process_group() -> None:
    torch = pytest.importorskip("torch")
    assert not torch.distributed.is_initialized()
    with pytest.raises(StrategyError, match="init"):
        wrap_model(torch.nn.Linear(2, 2), strategy="ddp")


def test_fsdp_requires_initialised_process_group() -> None:
    torch = pytest.importorskip("torch")
    assert not torch.distributed.is_initialized()
    with pytest.raises(StrategyError, match="init"):
        wrap_model(torch.nn.Linear(2, 2), strategy="fsdp")


def test_deepspeed_missing_dependency_message() -> None:
    pytest.importorskip("torch")
    try:
        import deepspeed  # noqa: F401

        pytest.skip("deepspeed installed; missing-dependency path not testable")
    except ImportError:
        pass
    with pytest.raises(StrategyError, match="pip install deepspeed"):
        wrap_model(object(), strategy="deepspeed")


def test_supported_strategies_cover_common_wrappers() -> None:
    assert set(SUPPORTED_STRATEGIES) == {"none", "ddp", "fsdp", "deepspeed"}
