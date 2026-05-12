from datetime import datetime, timedelta, timezone

import pytest

from src.broker.models import Candle
from src.indicators.atr import atr


def _mk(n: int, base: float = 100.0) -> list[Candle]:
    out = []
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(n):
        out.append(Candle(
            ts=t0 + timedelta(days=i),
            open=base + i, high=base + i + 2,
            low=base + i - 2, close=base + i + 1,
        ))
    return out


def test_atr_basic():
    candles = _mk(20)
    val = atr(candles, period=14)
    assert val > 0


def test_atr_too_few():
    with pytest.raises(ValueError):
        atr(_mk(5), period=14)
