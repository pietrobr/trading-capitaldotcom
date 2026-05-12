from __future__ import annotations

from collections.abc import Sequence

from src.broker.models import Candle


def atr(candles: Sequence[Candle], period: int = 14) -> float:
    """Wilder's ATR over the given candles. Returns the latest ATR value."""
    if len(candles) < period + 1:
        raise ValueError(
            f"Need at least {period + 1} candles for ATR({period}), got {len(candles)}"
        )
    trs: list[float] = []
    for i in range(1, len(candles)):
        c = candles[i]
        prev_close = candles[i - 1].close
        tr = max(
            c.high - c.low,
            abs(c.high - prev_close),
            abs(c.low - prev_close),
        )
        trs.append(tr)

    # Wilder's smoothing: first value = simple average of first `period` TRs,
    # then ATR_i = (ATR_{i-1} * (period - 1) + TR_i) / period
    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return atr_val
