from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

Side = Literal["BUY", "SELL"]
OrderType = Literal["MARKET", "STOP", "LIMIT"]
Resolution = Literal[
    "MINUTE", "MINUTE_5", "MINUTE_15", "MINUTE_30", "HOUR", "HOUR_4", "DAY", "WEEK"
]


@dataclass(frozen=True)
class Candle:
    ts: datetime  # UTC
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0


@dataclass
class Order:
    epic: str
    side: Side
    size: float
    type: OrderType
    level: float | None = None       # for STOP/LIMIT
    stop_loss: float | None = None
    take_profit: float | None = None
    client_ref: str | None = None


@dataclass
class Position:
    deal_id: str
    epic: str
    side: Side
    size: float
    open_level: float
