from datetime import datetime, timezone

from src.broker.models import Candle
from src.indicators.patterns import (
    is_bearish_engulfing,
    is_bullish_engulfing,
    is_hammer,
    is_inverted_hammer,
)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def c(o, h, l, cl):
    return Candle(ts=T0, open=o, high=h, low=l, close=cl)


def test_hammer():
    # small body near top, long lower shadow
    assert is_hammer(c(10.0, 10.2, 9.0, 10.1))
    assert not is_hammer(c(10.0, 11.0, 9.9, 10.9))  # big body


def test_inverted_hammer():
    assert is_inverted_hammer(c(10.0, 11.0, 9.9, 10.1))
    assert not is_inverted_hammer(c(10.0, 10.2, 9.0, 10.1))


def test_bullish_engulfing():
    prev = c(10.0, 10.1, 9.5, 9.6)   # bearish
    cur = c(9.5, 10.3, 9.4, 10.2)    # bullish that engulfs
    assert is_bullish_engulfing([prev, cur])


def test_bearish_engulfing():
    prev = c(9.6, 10.1, 9.5, 10.0)   # bullish
    cur = c(10.2, 10.3, 9.4, 9.5)    # bearish that engulfs
    assert is_bearish_engulfing([prev, cur])
