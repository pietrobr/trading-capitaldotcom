"""
Single- and two-candle reversal patterns used by Quick Flip Scalper.

All functions return True/False on the *last* candle of the input.
Thresholds follow common conventions and are tunable via parameters.
"""
from __future__ import annotations

from collections.abc import Sequence

from src.broker.models import Candle


def _body(c: Candle) -> float:
    return abs(c.close - c.open)


def _range(c: Candle) -> float:
    return max(c.high - c.low, 1e-12)


def _upper_shadow(c: Candle) -> float:
    return c.high - max(c.open, c.close)


def _lower_shadow(c: Candle) -> float:
    return min(c.open, c.close) - c.low


def is_hammer(c: Candle, body_max_pct: float = 0.35, lower_min_ratio: float = 2.0) -> bool:
    """Bullish hammer: small body in upper part, long lower shadow."""
    rng = _range(c)
    body = _body(c)
    if body / rng > body_max_pct:
        return False
    lower = _lower_shadow(c)
    upper = _upper_shadow(c)
    if body == 0:
        return lower > upper * lower_min_ratio
    return lower >= body * lower_min_ratio and lower > upper


def is_inverted_hammer(
    c: Candle, body_max_pct: float = 0.35, upper_min_ratio: float = 2.0
) -> bool:
    """Bearish inverted hammer (shooting star) when found at top."""
    rng = _range(c)
    body = _body(c)
    if body / rng > body_max_pct:
        return False
    upper = _upper_shadow(c)
    lower = _lower_shadow(c)
    if body == 0:
        return upper > lower * upper_min_ratio
    return upper >= body * upper_min_ratio and upper > lower


def is_bullish_engulfing(candles: Sequence[Candle]) -> bool:
    if len(candles) < 2:
        return False
    prev, cur = candles[-2], candles[-1]
    prev_bear = prev.close < prev.open
    cur_bull = cur.close > cur.open
    engulfs = cur.close >= prev.open and cur.open <= prev.close
    return prev_bear and cur_bull and engulfs


def is_bearish_engulfing(candles: Sequence[Candle]) -> bool:
    if len(candles) < 2:
        return False
    prev, cur = candles[-2], candles[-1]
    prev_bull = prev.close > prev.open
    cur_bear = cur.close < cur.open
    engulfs = cur.close <= prev.open and cur.open >= prev.close
    return prev_bull and cur_bear and engulfs
