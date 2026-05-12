from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from src.broker.models import Candle, Order


@dataclass
class StrategyContext:
    """Per-instance context: identifies (strategy, symbol, session, day)."""
    strategy_name: str
    symbol: str
    session_id: str
    session_start_utc: datetime

    @property
    def instance_id(self) -> str:
        d = self.session_start_utc.strftime("%Y%m%d")
        return f"{self.strategy_name}:{self.symbol}:{self.session_id}:{d}"


class StrategyBase(ABC):
    """
    Lifecycle (called by the engine):
      - on_session_start()    : at session open T0
      - on_candle_15m(c)      : when a 15m candle closes
      - on_candle_5m(c)       : when a 5m candle closes
      - on_window_end()       : at T0 + entry_window_minutes (cancel pendings)

    Strategy returns Order objects for the engine to dispatch.
    """

    def __init__(self, ctx: StrategyContext, params: dict, daily_atr: float) -> None:
        self.ctx = ctx
        self.params = params
        self.daily_atr = daily_atr

    @abstractmethod
    def on_session_start(self) -> None: ...

    @abstractmethod
    def on_candle_15m(self, candle: Candle) -> list[Order]: ...

    @abstractmethod
    def on_candle_5m(self, candle: Candle) -> list[Order]: ...

    @abstractmethod
    def on_window_end(self) -> list[str]:
        """Return list of working-order client_refs to cancel."""
