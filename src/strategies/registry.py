from __future__ import annotations

from typing import Type

from .base import StrategyBase
from .quick_flip_scalper import QuickFlipScalper

_REGISTRY: dict[str, Type[StrategyBase]] = {
    "quick_flip_scalper": QuickFlipScalper,
}


def get_strategy_class(name: str) -> Type[StrategyBase]:
    if name not in _REGISTRY:
        raise KeyError(f"Unknown strategy '{name}'. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]
