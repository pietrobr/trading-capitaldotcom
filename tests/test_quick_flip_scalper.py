from datetime import datetime, timezone

from src.broker.models import Candle
from src.strategies.base import StrategyContext
from src.strategies.quick_flip_scalper import QuickFlipScalper

T0 = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)


def _ctx() -> StrategyContext:
    return StrategyContext(
        strategy_name="quick_flip_scalper",
        symbol="EURUSD",
        session_id="london_open",
        session_start_utc=T0,
    )


def _candle(o, h, l, cl) -> Candle:
    return Candle(ts=T0, open=o, high=h, low=l, close=cl)


def test_box_invalid_when_range_below_atr_threshold():
    s = QuickFlipScalper(_ctx(), {"atr_filter_pct": 0.25}, daily_atr=1.0)
    s.on_session_start()
    # range = 0.10, threshold = 0.25 -> invalid
    s.on_candle_15m(_candle(1.10, 1.15, 1.05, 1.12))
    assert s.box_valid is False
    # No orders even with strong reversal
    out = s.on_candle_5m(_candle(1.04, 1.06, 1.00, 1.055))  # below box
    assert out == []


def test_long_entry_on_hammer_below_box():
    s = QuickFlipScalper(_ctx(), {"atr_filter_pct": 0.25}, daily_atr=0.20)
    s.on_session_start()
    # range 0.10 >= 0.25*0.20=0.05 -> valid
    s.on_candle_15m(_candle(1.10, 1.15, 1.05, 1.12))
    # 5m hammer below L15=1.05
    hammer = _candle(o=1.04, h=1.045, l=1.00, cl=1.043)  # small body up, long lower shadow
    orders = s.on_candle_5m(hammer)
    assert len(orders) == 1
    o = orders[0]
    assert o.side == "BUY"
    assert o.type == "STOP"
    assert o.level == 1.045
    assert o.stop_loss == 1.00
    assert o.take_profit == 1.15  # H15


def test_short_entry_on_inverted_hammer_above_box():
    s = QuickFlipScalper(_ctx(), {"atr_filter_pct": 0.25}, daily_atr=0.20)
    s.on_session_start()
    s.on_candle_15m(_candle(1.10, 1.15, 1.05, 1.12))
    inv = _candle(o=1.16, h=1.20, l=1.155, cl=1.158)
    orders = s.on_candle_5m(inv)
    assert len(orders) == 1
    o = orders[0]
    assert o.side == "SELL"
    assert o.type == "STOP"
    assert o.level == 1.155
    assert o.stop_loss == 1.20
    assert o.take_profit == 1.05  # L15


def test_max_trades_per_session():
    s = QuickFlipScalper(_ctx(), {"max_trades_per_session": 1}, daily_atr=0.20)
    s.on_session_start()
    s.on_candle_15m(_candle(1.10, 1.15, 1.05, 1.12))
    hammer = _candle(o=1.04, h=1.045, l=1.00, cl=1.043)
    s.on_candle_5m(hammer)
    # second valid signal is ignored
    out = s.on_candle_5m(hammer)
    assert out == []


def test_window_end_returns_pending_refs():
    s = QuickFlipScalper(_ctx(), {}, daily_atr=0.20)
    s.on_session_start()
    s.on_candle_15m(_candle(1.10, 1.15, 1.05, 1.12))
    s.on_candle_5m(_candle(o=1.04, h=1.045, l=1.00, cl=1.043))
    refs = s.on_window_end()
    assert len(refs) == 1
    assert "EURUSD" in refs[0] and "london_open" in refs[0]
