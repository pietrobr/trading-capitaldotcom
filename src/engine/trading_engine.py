"""
TradingEngine: orchestrates strategy instances across instruments and sessions.

This is a skeleton focused on correctness of the per-instance lifecycle.
Live data feeding (websocket) and full async loop are wired in main.py.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from loguru import logger

from src.broker.capital_client import CapitalClient
from src.broker.models import Candle, Order, Resolution
from src.config.models import AppConfig, SessionConfig
from src.engine.session_clock import (
    is_active_day,
    market_close_utc,
    next_session_start_utc,
    session_start_utc,
)
from src.indicators.atr import atr
from src.strategies.base import StrategyBase, StrategyContext
from src.strategies.registry import get_strategy_class
from src.utils.events import EVENTS


@dataclass
class _ScheduledInstance:
    strategy_name: str
    symbol: str
    session_id: str
    session: SessionConfig
    params: dict
    start_utc: datetime
    end_utc: datetime
    instance: StrategyBase | None = None
    deal_ids_by_ref: dict[str, str] = field(default_factory=dict)
    last_15m_ts: datetime | None = None
    last_5m_ts: datetime | None = None
    task: asyncio.Task | None = None
    closed: bool = False

    @property
    def instance_id_for_log(self) -> str:
        d = self.start_utc.strftime("%Y%m%d")
        return f"{self.strategy_name}:{self.symbol}:{self.session_id}:{d}"


class TradingEngine:
    def __init__(
        self,
        config: AppConfig,
        client: CapitalClient,
        dry_run: bool = True,
    ) -> None:
        self.config = config
        self.client = client
        self.dry_run = dry_run
        self.manual_instances: list[_ScheduledInstance] = []
        # Sessions auto-started by the scheduler when their start time fires.
        self.auto_instances: list[_ScheduledInstance] = []
        # Keys of (strategy, symbol, session, local_date) already started today,
        # to prevent duplicate launches inside the same start window.
        self._started_keys: set[str] = set()
        self._scheduler_task: asyncio.Task | None = None
        # End-of-session summaries, newest last.
        self.session_summaries: list[dict] = []
        # Debug mode: shrink timeframes to speed up testing.
        # Box = 2 minutes (aggregated from 2 MINUTE candles).
        # Entries = 1-minute candles. Polling cadence is also accelerated.
        self.debug_mode: bool = False

    # ---------- timeframe helpers ----------
    def _box_resolution(self) -> tuple[Resolution, int, int]:
        """Return (api_resolution, box_minutes, base_resolution_minutes).
        box_minutes = duration of the synthetic box candle.
        base_resolution_minutes = minutes per fetched candle (1 in debug, 15 normally)."""
        if self.debug_mode:
            return "MINUTE", 2, 1
        return "MINUTE_15", 15, 15

    def _entry_resolution(self) -> tuple[Resolution, int]:
        if self.debug_mode:
            return "MINUTE", 1
        return "MINUTE_5", 5

    def _aggregate(
        self, candles: list[Candle], group_size: int, group_minutes: int,
        now: datetime,
    ) -> list[Candle]:
        """Combine `group_size` consecutive 1-min candles into synthetic candles
        of `group_minutes` minutes. Only fully-closed groups are returned.
        Candles must be sorted ascending by ts."""
        if group_size == 1:
            return [c for c in candles if c.ts + timedelta(minutes=group_minutes) <= now]
        out: list[Candle] = []
        # Align to multiples of group_minutes from epoch UTC for deterministic boxes.
        for c in candles:
            mins = c.ts.minute
            if mins % group_minutes != 0:
                continue
            ts0 = c.ts
            group: list[Candle] = []
            for j in range(group_size):
                expected = ts0 + timedelta(minutes=j)
                cand = next((x for x in candles if x.ts == expected), None)
                if cand is None:
                    break
                group.append(cand)
            if len(group) != group_size:
                continue
            if ts0 + timedelta(minutes=group_minutes) > now:
                continue
            out.append(Candle(
                ts=ts0,
                open=group[0].open,
                high=max(x.high for x in group),
                low=min(x.low for x in group),
                close=group[-1].close,
                volume=sum(x.volume for x in group),
            ))
        return out

    # ---------- manual trigger ----------
    def trigger_manual(
        self, strategy_name: str, symbol: str, session_id: str | None = None
    ) -> _ScheduledInstance:
        strat_cfg = next(
            (s for s in self.config.strategies if s.name == strategy_name), None
        )
        if strat_cfg is None:
            raise ValueError(f"Unknown strategy '{strategy_name}'")
        deployment = next(
            (d for d in strat_cfg.deployments if d.symbol == symbol), None
        )
        if deployment is None:
            raise ValueError(
                f"Symbol '{symbol}' is not deployed for strategy '{strategy_name}'"
            )
        if session_id is not None and session_id not in deployment.sessions:
            raise ValueError(
                f"Session '{session_id}' is not configured for "
                f"{strategy_name} on {symbol}"
            )
        window_min = int(strat_cfg.params.get("entry_window_minutes", 90))
        if self.debug_mode:
            # Shrink window to keep debug sessions short.
            window_min = min(window_min, 15)
        now = datetime.now(timezone.utc)
        # End of session = market close (NYSE 16:00 ET for stocks/indices,
        # Friday 17:00 NY for forex). In debug mode use the short window so we
        # don't keep a debug instance running until the bell.
        end_utc = now + timedelta(minutes=window_min)
        if not self.debug_mode:
            inst_cfg = next(
                (m for m in self.config.instruments if m.symbol == symbol), None
            )
            if inst_cfg is not None:
                close = market_close_utc(inst_cfg.type, now)
                if close is not None and close > now:
                    end_utc = close
        label = f"manual_{session_id}_{now.strftime('%H%M%S')}" if session_id \
            else f"manual_{now.strftime('%H%M%S')}"
        sched = _ScheduledInstance(
            strategy_name=strat_cfg.name,
            symbol=symbol,
            session_id=label,
            session=None,  # type: ignore[arg-type]  # manual: no SessionConfig
            params=strat_cfg.params,
            start_utc=now,
            end_utc=end_utc,
        )
        self.manual_instances.append(sched)
        EVENTS.info(
            sched.instance_id_for_log,
            "Session window OPENED (manual)",
            window_min=int((sched.end_utc - sched.start_utc).total_seconds() // 60),
            start_utc=sched.start_utc.isoformat(),
            end_utc=sched.end_utc.isoformat(),
            session=session_id,
        )
        logger.info(
            "Manual trigger {}:{}:{} window {} min",
            sched.strategy_name, sched.symbol, sched.session_id, window_min,
        )
        return sched

    def _prune_expired_manual(self, now_utc: datetime) -> None:
        kept: list[_ScheduledInstance] = []
        for m in self.manual_instances:
            if m.end_utc > now_utc and not m.closed:
                kept.append(m)
            else:
                if m.task and not m.task.done():
                    m.task.cancel()
                EVENTS.info(
                    m.instance_id_for_log,
                    "Session window CLOSED",
                    end_utc=m.end_utc.isoformat(),
                )
        self.manual_instances = kept

    def _prune_expired_auto(self, now_utc: datetime) -> None:
        kept: list[_ScheduledInstance] = []
        for m in self.auto_instances:
            if m.end_utc > now_utc and not m.closed:
                kept.append(m)
            else:
                if m.task and not m.task.done():
                    m.task.cancel()
                EVENTS.info(
                    m.instance_id_for_log,
                    "Session window CLOSED (auto)",
                    end_utc=m.end_utc.isoformat(),
                )
        self.auto_instances = kept

    def manual_status_snapshot(self, now_utc: datetime | None = None) -> list[dict]:
        """Return all currently-running instances (auto-scheduled + manual) with
        open/closed status and remaining time."""
        now_utc = now_utc or datetime.now(timezone.utc)
        out: list[dict] = []
        for m in self.auto_instances + self.manual_instances:
            remaining = (m.end_utc - now_utc).total_seconds()
            elapsed = (now_utc - m.start_utc).total_seconds()
            out.append({
                "instance_id": m.instance_id_for_log,
                "strategy": m.strategy_name,
                "symbol": m.symbol,
                "session": m.session_id,
                "start_utc": m.start_utc.isoformat(),
                "end_utc": m.end_utc.isoformat(),
                "open": remaining > 0,
                "remaining_seconds": max(0, int(remaining)),
                "elapsed_seconds": max(0, int(elapsed)),
            })
        return out

    # ---------- planning ----------
    def plan_today(self, now_utc: datetime | None = None) -> list[_ScheduledInstance]:
        now_utc = now_utc or datetime.now(timezone.utc)
        plan: list[_ScheduledInstance] = []
        for strat_cfg in self.config.strategies:
            if not strat_cfg.enabled:
                continue
            window_min = int(strat_cfg.params.get("entry_window_minutes", 90))
            for dep in strat_cfg.deployments:
                for sid in dep.sessions:
                    sess = self.config.sessions[sid]
                    start = next_session_start_utc(sess, now_utc)
                    plan.append(
                        _ScheduledInstance(
                            strategy_name=strat_cfg.name,
                            symbol=dep.symbol,
                            session_id=sid,
                            session=sess,
                            params=strat_cfg.params,
                            start_utc=start,
                            end_utc=start + timedelta(minutes=window_min),
                        )
                    )
        plan.sort(key=lambda x: x.start_utc)
        # Prune expired manual instances (emit a closing event for each).
        self._prune_expired_manual(now_utc)
        self._prune_expired_auto(now_utc)
        plan = self.auto_instances + self.manual_instances + plan
        for s in plan:
            logger.debug(
                "Scheduled {}:{}:{} start={} end={}",
                s.strategy_name, s.symbol, s.session_id,
                s.start_utc.isoformat(), s.end_utc.isoformat(),
            )
        return plan

    # ---------- per-instance run ----------
    async def _bootstrap_instance(self, sched: _ScheduledInstance) -> None:
        atr_period = int(sched.params.get("atr_period", 14))
        instance_id = sched.instance_id_for_log
        EVENTS.info(instance_id, "Bootstrapping: fetching daily candles for ATR",
                    atr_period=atr_period)
        try:
            daily_to = sched.start_utc
            daily_from = daily_to - timedelta(days=atr_period * 3 + 5)
            daily = await self.client.get_prices(
                sched.symbol, "DAY", daily_from, daily_to, max_points=atr_period * 3 + 5
            )
            atr_value = atr(daily, period=atr_period)
        except Exception as e:
            EVENTS.error(instance_id, "Bootstrap failed", error=str(e))
            raise
        ctx = StrategyContext(
            strategy_name=sched.strategy_name,
            symbol=sched.symbol,
            session_id=sched.session_id,
            session_start_utc=sched.start_utc,
        )
        cls = get_strategy_class(sched.strategy_name)
        sched.instance = cls(ctx, sched.params, atr_value)
        sched.instance.on_session_start()

    async def _dispatch_orders(self, sched: _ScheduledInstance, orders: list[Order]) -> None:
        instance_id = sched.instance_id_for_log
        for o in orders:
            if self.dry_run:
                EVENTS.info(
                    instance_id, "DRY-RUN: would place order",
                    side=o.side, type=o.type, size=o.size, level=o.level,
                    sl=o.stop_loss, tp=o.take_profit, ref=o.client_ref,
                )
                continue
            try:
                resp = await self.client.place_order(o)
                deal_id = resp.get("dealReference") or resp.get("dealId")
                if o.client_ref and deal_id:
                    sched.deal_ids_by_ref[o.client_ref] = deal_id
                EVENTS.info(
                    instance_id, "Order placed",
                    side=o.side, type=o.type, level=o.level, deal_id=deal_id,
                )
            except Exception as e:
                EVENTS.error(instance_id, "place_order failed",
                             ref=o.client_ref, error=str(e))

    async def _cancel_refs(self, sched: _ScheduledInstance, refs: list[str]) -> None:
        instance_id = sched.instance_id_for_log
        for ref in refs:
            deal_id = sched.deal_ids_by_ref.pop(ref, None)
            if not deal_id:
                continue
            if self.dry_run:
                EVENTS.info(instance_id, "DRY-RUN: would cancel order",
                            deal_id=deal_id, ref=ref)
                continue
            try:
                await self.client.cancel_working_order(deal_id)
                EVENTS.info(instance_id, "Order cancelled", deal_id=deal_id, ref=ref)
            except Exception as e:
                EVENTS.error(instance_id, "cancel failed", ref=ref, error=str(e))

    # ---------- auto-scheduler ----------
    async def run_scheduler(self, poll_seconds: int = 15) -> None:
        """Background loop: launches each configured (strategy, symbol, session)
        when its start time arrives on an active day, exactly once per day.
        Self-contained — does not raise on per-instance failures."""
        EVENTS.info("system", "Auto-scheduler started", poll_seconds=poll_seconds)
        try:
            while True:
                try:
                    await self._scheduler_tick(datetime.now(timezone.utc))
                except Exception as e:
                    EVENTS.error("system", "Scheduler tick failed", error=str(e))
                await asyncio.sleep(poll_seconds)
        except asyncio.CancelledError:
            EVENTS.warn("system", "Auto-scheduler stopped")
            raise

    async def _scheduler_tick(self, now_utc: datetime) -> None:
        from zoneinfo import ZoneInfo
        for strat_cfg in self.config.strategies:
            if not strat_cfg.enabled:
                continue
            window_min = int(strat_cfg.params.get("entry_window_minutes", 90))
            for dep in strat_cfg.deployments:
                for sid in dep.sessions:
                    sess = self.config.sessions.get(sid)
                    if sess is None:
                        continue
                    local_now = now_utc.astimezone(ZoneInfo(sess.timezone))
                    today_local = local_now.date()
                    if not is_active_day(sess, today_local):
                        continue
                    start = session_start_utc(sess, today_local)
                    end = start + timedelta(minutes=window_min)
                    if not (start <= now_utc < end):
                        continue
                    key = f"{strat_cfg.name}:{dep.symbol}:{sid}:{today_local.isoformat()}"
                    if key in self._started_keys:
                        continue
                    self._started_keys.add(key)
                    sched = _ScheduledInstance(
                        strategy_name=strat_cfg.name,
                        symbol=dep.symbol,
                        session_id=sid,
                        session=sess,
                        params=strat_cfg.params,
                        start_utc=start,
                        end_utc=end,
                    )
                    self.auto_instances.append(sched)
                    EVENTS.info(
                        sched.instance_id_for_log,
                        "Session window OPENED (auto)",
                        window_min=window_min,
                        start_utc=start.isoformat(),
                        end_utc=end.isoformat(),
                    )
                    try:
                        await self.start_polled_instance(sched)
                    except Exception as e:
                        EVENTS.error(
                            sched.instance_id_for_log,
                            "Auto bootstrap failed",
                            error=str(e),
                        )

    # ---------- polled instance loop ----------
    async def start_polled_instance(self, sched: _ScheduledInstance) -> None:
        """Bootstrap the instance and launch a background poller for its window."""
        instance_id = sched.instance_id_for_log
        await self._bootstrap_instance(sched)
        # Seed the opening range from the most recent already-closed 15m candle
        # so the user does not have to wait up to 15 minutes for the box.
        await self._seed_initial_15m(sched)
        if sched.task is None or sched.task.done():
            sched.task = asyncio.create_task(self._poll_loop(sched))
            interval = int(sched.params.get("poll_interval_s", 30))
            if self.debug_mode:
                interval = min(interval, 10)
            EVENTS.info(instance_id, "Polling loop started",
                        poll_interval_s=interval,
                        debug=self.debug_mode)

    def _maybe_close_for_invalid_box(self, sched: _ScheduledInstance) -> None:
        """Deprecated: kept as no-op. The strategy now retries the box on each
        subsequent 15m candle until session end instead of closing early."""
        return

    async def close_manual(self, instance_id: str) -> bool:
        """Close a running session (auto or manual) immediately: cancel pending
        orders, stop the polling task, mark closed and emit lifecycle events.
        Returns True if the instance was found, False otherwise."""
        sched = next(
            (m for m in self.manual_instances + self.auto_instances
             if m.instance_id_for_log == instance_id),
            None,
        )
        if sched is None:
            return False
        if sched.closed:
            return True
        log_id = sched.instance_id_for_log
        EVENTS.warn(log_id, "Manual close requested")
        try:
            refs = sched.instance.on_window_end() if sched.instance else []
            await self._cancel_refs(sched, refs)
        except Exception as e:
            EVENTS.error(log_id, "Error during manual close cleanup", error=str(e))
        sched.end_utc = datetime.now(timezone.utc)
        sched.closed = True
        if sched.task and not sched.task.done():
            sched.task.cancel()
        EVENTS.info(log_id, "Session window CLOSED (manual stop)",
                    end_utc=sched.end_utc.isoformat())
        self._build_session_summary(sched, reason="manual_stop")
        return True

    async def _seed_initial_15m(self, sched: _ScheduledInstance) -> None:
        """Seed strategy with the OPENING box candle (ts == session start), once it has fully closed."""
        instance_id = sched.instance_id_for_log
        now = datetime.now(timezone.utc)
        api_res, box_min, base_min = self._box_resolution()
        opening_ts = sched.start_utc
        opening_close = opening_ts + timedelta(minutes=box_min)
        if now < opening_close:
            EVENTS.info(instance_id,
                        f"Opening {box_min}m candle not yet closed; will wait for poller",
                        opening_ts=opening_ts.isoformat(),
                        closes_at=opening_close.isoformat())
            # Make poller pick up the opening candle as soon as it closes.
            sched.last_15m_ts = opening_ts - timedelta(minutes=box_min)
            return
        group = box_min // base_min
        # Fetch from a bit before the opening up to a bit after its close.
        from_ts = opening_ts - timedelta(minutes=base_min)
        to_ts = opening_close + timedelta(minutes=base_min)
        try:
            raw = await self.client.get_prices(
                sched.symbol, api_res, from_ts, to_ts,
                max_points=max(8, group * 4),
            )
        except Exception as e:
            EVENTS.error(instance_id, "Seed box fetch failed", error=str(e))
            return
        boxes = self._aggregate(raw, group, box_min, now)
        seed = next((b for b in boxes if b.ts == opening_ts), None)
        if seed is None:
            EVENTS.warn(instance_id,
                        f"Opening {box_min}m candle not available from broker yet",
                        opening_ts=opening_ts.isoformat())
            sched.last_15m_ts = opening_ts - timedelta(minutes=box_min)
            return
        sched.last_15m_ts = seed.ts
        EVENTS.info(instance_id,
                    f"Seeding box from opening {box_min}m candle"
                    + (" [DEBUG]" if self.debug_mode else ""),
                    ts=seed.ts.isoformat(),
                    o=seed.open, h=seed.high, l=seed.low, c=seed.close)
        if sched.instance:
            orders = sched.instance.on_candle_15m(seed)
            await self._dispatch_orders(sched, orders)

    def _build_session_summary(self, sched: _ScheduledInstance, reason: str) -> dict:
        inst = sched.instance
        cancelled = list(getattr(inst, "pending_refs", []) or [])
        orders_placed = len(sched.deal_ids_by_ref)
        summary = {
            "instance_id": sched.instance_id_for_log,
            "strategy": sched.strategy_name,
            "symbol": sched.symbol,
            "session": sched.session_id,
            "start_utc": sched.start_utc.isoformat(),
            "end_utc": sched.end_utc.isoformat(),
            "reason": reason,  # "window_end" | "manual_stop"
            "box_valid": bool(getattr(inst, "box_valid", False)),
            "h15": getattr(inst, "h15", None),
            "l15": getattr(inst, "l15", None),
            "daily_atr": getattr(inst, "daily_atr", None),
            "trades_armed": int(getattr(inst, "trades_armed", 0)),
            "orders_placed": orders_placed,
            "pending_cancelled": len(cancelled),
        }
        EVENTS.info(
            sched.instance_id_for_log, "Session summary",
            box_valid=summary["box_valid"],
            trades_armed=summary["trades_armed"],
            orders_placed=summary["orders_placed"],
            pending_cancelled=summary["pending_cancelled"],
            reason=reason,
        )
        self.session_summaries.append(summary)
        # Cap to last 200 to avoid unbounded growth.
        if len(self.session_summaries) > 200:
            self.session_summaries = self.session_summaries[-200:]
        return summary

    async def _poll_loop(self, sched: _ScheduledInstance) -> None:
        instance_id = sched.instance_id_for_log
        interval = int(sched.params.get("poll_interval_s", 30))
        if self.debug_mode:
            interval = min(interval, 10)
        # First poll fires after `interval` so the broker has time to publish
        # the closed candles after session start.
        try:
            while True:
                now = datetime.now(timezone.utc)
                if now >= sched.end_utc:
                    EVENTS.info(instance_id, "Window end reached; cleaning up")
                    refs = sched.instance.on_window_end() if sched.instance else []
                    await self._cancel_refs(sched, refs)
                    sched.closed = True
                    self._build_session_summary(sched, reason="window_end")
                    return
                await asyncio.sleep(min(interval, max(1, (sched.end_utc - now).total_seconds())))
                try:
                    await self._poll_once(sched)
                except Exception as e:
                    EVENTS.error(instance_id, "Poll iteration failed", error=str(e))
        except asyncio.CancelledError:
            EVENTS.warn(instance_id, "Polling loop cancelled")
            raise

    async def _poll_once(self, sched: _ScheduledInstance) -> None:
        """Fetch any new closed box and entry candles since last cursor and dispatch them."""
        instance_id = sched.instance_id_for_log
        now = datetime.now(timezone.utc)
        entry_res, entry_min = self._entry_resolution()
        box_api_res, box_min, base_min = self._box_resolution()
        # ---- Entry candles ----
        from_dt = sched.last_5m_ts + timedelta(seconds=1) \
            if sched.last_5m_ts is not None \
            else min(sched.start_utc, now - timedelta(minutes=max(30, entry_min * 6)))
        to_dt = now
        try:
            entries = await self.client.get_prices(
                sched.symbol, entry_res, from_dt, to_dt, max_points=200
            )
        except Exception as e:
            EVENTS.error(instance_id, f"Fetch {entry_min}m candles failed", error=str(e))
            entries = []
        for candle in entries:
            if candle.ts < sched.start_utc:
                continue
            if candle.ts + timedelta(minutes=entry_min) > now:
                continue
            if sched.last_5m_ts is not None and candle.ts <= sched.last_5m_ts:
                continue
            sched.last_5m_ts = candle.ts
            EVENTS.info(instance_id, f"{entry_min}m candle closed",
                        ts=candle.ts.isoformat(),
                        o=candle.open, h=candle.high, l=candle.low, c=candle.close)
            if sched.instance:
                orders = sched.instance.on_candle_5m(candle)
                await self._dispatch_orders(sched, orders)

        # ---- Box candles ----
        need_box = True
        if sched.instance is not None and getattr(sched.instance, "box_valid", False):
            need_box = False
        if need_box:
            from_box = sched.last_15m_ts + timedelta(seconds=1) \
                if sched.last_15m_ts is not None \
                else sched.start_utc - timedelta(minutes=box_min)
            try:
                raw_box = await self.client.get_prices(
                    sched.symbol, box_api_res, from_box, now, max_points=200,
                )
            except Exception as e:
                EVENTS.error(instance_id, f"Fetch {box_min}m candles failed", error=str(e))
                raw_box = []
            group = box_min // base_min
            boxes = self._aggregate(raw_box, group, box_min, now)
            for candle in boxes:
                if sched.last_15m_ts is not None and candle.ts <= sched.last_15m_ts:
                    continue
                # Strategy uses ONLY the opening candle of the session.
                if candle.ts != sched.start_utc:
                    sched.last_15m_ts = candle.ts
                    continue
                sched.last_15m_ts = candle.ts
                EVENTS.info(instance_id, f"Opening {box_min}m candle closed",
                            ts=candle.ts.isoformat(),
                            o=candle.open, h=candle.high, l=candle.low, c=candle.close)
                if sched.instance:
                    orders = sched.instance.on_candle_15m(candle)
                    await self._dispatch_orders(sched, orders)
                break  # opening candle delivered; ignore all subsequent 15m

    # ---------- public entrypoint (skeleton) ----------
    async def run_instance(
        self,
        sched: _ScheduledInstance,
        candle_15m_feed,
        candle_5m_feed,
    ) -> None:
        """
        Run a single instance to completion. Feeds are async iterators yielding Candle.
        Concrete feed implementations (live websocket / replay) live elsewhere.
        """
        await self._bootstrap_instance(sched)
        assert sched.instance is not None

        async def consume_15m():
            async for c in candle_15m_feed:
                if c.ts >= sched.end_utc:
                    break
                orders = sched.instance.on_candle_15m(c)
                await self._dispatch_orders(sched, orders)

        async def consume_5m():
            async for c in candle_5m_feed:
                if c.ts >= sched.end_utc:
                    break
                orders = sched.instance.on_candle_5m(c)
                await self._dispatch_orders(sched, orders)

        await asyncio.gather(consume_15m(), consume_5m())
        refs = sched.instance.on_window_end()
        await self._cancel_refs(sched, refs)
