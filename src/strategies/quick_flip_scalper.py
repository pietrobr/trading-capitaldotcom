"""
Quick Flip Scalper.

Spec:
  - Opening range = first 15m candle of the session: store H15, L15.
  - ATR(14) on Daily; require (H15 - L15) >= atr_filter_pct * ATR_daily.
  - During first `entry_window_minutes` (default 90), watch 5m candles:
      * If close < L15 -> look for Hammer or Bullish Engulfing -> BUY STOP at pattern.high.
        SL = pattern.low; TP = H15.
      * If close > H15 -> look for Inverted Hammer or Bearish Engulfing -> SELL STOP at pattern.low.
        SL = pattern.high; TP = L15.
  - Cancel any pending order at window end.
  - max_trades_per_session limits how many entries are armed (default 1).
"""
from __future__ import annotations

from src.broker.models import Candle, Order
from src.utils.events import EVENTS
from src.indicators.patterns import (
    is_bearish_engulfing,
    is_bullish_engulfing,
    is_hammer,
    is_inverted_hammer,
)

from .base import StrategyBase, StrategyContext


class QuickFlipScalper(StrategyBase):
    def __init__(self, ctx: StrategyContext, params: dict, daily_atr: float) -> None:
        super().__init__(ctx, params, daily_atr)
        self.atr_filter_pct: float = float(params.get("atr_filter_pct", 0.25))
        self.max_trades: int = int(params.get("max_trades_per_session", 1))
        self.position_size: float = float(params.get("position_size", 1.0))

        self.h15: float | None = None
        self.l15: float | None = None
        self.box_valid: bool = False
        self.box_evaluated: bool = False
        self.trades_armed: int = 0
        self.recent_5m: list[Candle] = []
        self.pending_refs: list[str] = []

    # ---------- lifecycle ----------
    def on_session_start(self) -> None:
        EVENTS.info(
            self.ctx.instance_id,
            "Session start",
            daily_atr=round(self.daily_atr, 6),
            atr_filter_pct=self.atr_filter_pct,
            max_trades=self.max_trades,
        )

    def on_candle_15m(self, candle: Candle) -> list[Order]:
        # Only the FIRST 15m candle of the session defines the opening range.
        # Once evaluated, the box is locked: no further 15m candle can change it,
        # even if the ATR filter failed.
        if self.box_evaluated:
            return []
        self.box_evaluated = True
        self.h15 = candle.high
        self.l15 = candle.low
        rng = self.h15 - self.l15
        threshold = self.atr_filter_pct * self.daily_atr
        self.box_valid = rng >= threshold
        ratio = (rng / self.daily_atr) if self.daily_atr > 0 else 0.0
        EVENTS.info(
            self.ctx.instance_id,
            "Opening range captured",
            ts=candle.ts.isoformat(),
            H15=round(self.h15, 6), L15=round(self.l15, 6),
            range=round(rng, 6),
            daily_ATR=round(self.daily_atr, 6),
            atr_filter_pct=self.atr_filter_pct,
            threshold=round(threshold, 6),
            range_vs_ATR=f"{round(ratio * 100, 2)}%",
            comparison=f"range {round(rng, 6)} {'>=' if self.box_valid else '<'} threshold {round(threshold, 6)}",
            box_valid=self.box_valid,
        )
        if not self.box_valid:
            EVENTS.warn(
                self.ctx.instance_id,
                "ATR filter NOT met: session disabled (no retry on later 15m candles)",
                range=round(rng, 6),
                threshold=round(threshold, 6),
                shortfall=round(threshold - rng, 6),
            )
        else:
            EVENTS.success(
                self.ctx.instance_id,
                "ATR filter PASSED: box locked, ready for entries",
                range=round(rng, 6),
                threshold=round(threshold, 6),
                range_vs_ATR=f"{round(ratio * 100, 2)}%",
            )
        return []

    def on_candle_5m(self, candle: Candle) -> list[Order]:
        if not self.box_valid or self.h15 is None or self.l15 is None:
            return []
        if self.trades_armed >= self.max_trades:
            return []

        self.recent_5m.append(candle)
        if len(self.recent_5m) > 10:
            self.recent_5m = self.recent_5m[-10:]

        orders: list[Order] = []

        # LONG setup: price below box, look for bullish reversal
        if candle.close < self.l15:
            if is_hammer(candle) or is_bullish_engulfing(self.recent_5m):
                ref = f"{self.ctx.instance_id}:LONG:{self.trades_armed}"
                orders.append(
                    Order(
                        epic=self.ctx.symbol,
                        side="BUY",
                        size=self.position_size,
                        type="STOP",
                        level=candle.high,
                        stop_loss=candle.low,
                        take_profit=self.h15,
                        client_ref=ref,
                    )
                )
                self.pending_refs.append(ref)
                self.trades_armed += 1
                EVENTS.info(
                    self.ctx.instance_id,
                    "LONG entry armed (Hammer/Bullish Engulfing)",
                    entry=round(candle.high, 6),
                    stop_loss=round(candle.low, 6),
                    take_profit=round(self.h15, 6),
                    ref=ref,
                )

        # SHORT setup: price above box, look for bearish reversal
        elif candle.close > self.h15:
            if is_inverted_hammer(candle) or is_bearish_engulfing(self.recent_5m):
                ref = f"{self.ctx.instance_id}:SHORT:{self.trades_armed}"
                orders.append(
                    Order(
                        epic=self.ctx.symbol,
                        side="SELL",
                        size=self.position_size,
                        type="STOP",
                        level=candle.low,
                        stop_loss=candle.high,
                        take_profit=self.l15,
                        client_ref=ref,
                    )
                )
                self.pending_refs.append(ref)
                self.trades_armed += 1
                EVENTS.info(
                    self.ctx.instance_id,
                    "SHORT entry armed (Inverted Hammer/Bearish Engulfing)",
                    entry=round(candle.low, 6),
                    stop_loss=round(candle.high, 6),
                    take_profit=round(self.l15, 6),
                    ref=ref,
                )

        return orders

    def on_window_end(self) -> list[str]:
        refs = self.pending_refs[:]
        self.pending_refs.clear()
        if refs:
            EVENTS.info(
                self.ctx.instance_id,
                "Entry window ended; cancelling pending orders",
                count=len(refs),
            )
        else:
            EVENTS.info(self.ctx.instance_id, "Entry window ended; no pending orders")
        return refs
