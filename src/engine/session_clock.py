from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.config.models import InstrumentType, SessionConfig

_DAY_INDEX = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


# Per market-type close time. tz is local; weekday is Mon=0..Sun=6.
# For weekly-cycle markets (forex), we use the weekly close (Fri 17:00 NY).
_MARKET_CLOSE: dict[str, dict] = {
    "stock":     {"tz": "America/New_York", "time": time(16, 0), "weekly": False},
    "index":     {"tz": "America/New_York", "time": time(16, 0), "weekly": False},
    "commodity": {"tz": "America/New_York", "time": time(17, 0), "weekly": False},
    "forex":     {"tz": "America/New_York", "time": time(17, 0), "weekly": True,
                  "weekday": 4},  # Friday close of FX week
    # crypto: 24/7 — no scheduled close (caller will fall back to a default).
}


def market_close_utc(market_type: InstrumentType, now_utc: datetime) -> datetime | None:
    """Return the next scheduled market close in UTC for the given market type.
    Returns None when the market has no scheduled close (e.g. crypto)."""
    spec = _MARKET_CLOSE.get(market_type)
    if spec is None:
        return None
    tz = ZoneInfo(spec["tz"])
    local_now = now_utc.astimezone(tz)
    if spec.get("weekly"):
        target_wd = spec["weekday"]
        days_ahead = (target_wd - local_now.weekday()) % 7
        candidate_day = (local_now + timedelta(days=days_ahead)).date()
        local_close = datetime.combine(candidate_day, spec["time"], tzinfo=tz)
        if local_close <= local_now:
            local_close = local_close + timedelta(days=7)
    else:
        candidate_day = local_now.date()
        local_close = datetime.combine(candidate_day, spec["time"], tzinfo=tz)
        if local_close <= local_now:
            local_close = local_close + timedelta(days=1)
    return local_close.astimezone(ZoneInfo("UTC"))


def session_start_utc(session: SessionConfig, on_day: date) -> datetime:
    """UTC datetime of the session start for the given local calendar day."""
    tz = ZoneInfo(session.timezone)
    local_dt = datetime.combine(on_day, session.start, tzinfo=tz)
    return local_dt.astimezone(ZoneInfo("UTC"))


def is_active_day(session: SessionConfig, on_day: date) -> bool:
    allowed = {_DAY_INDEX[d] for d in session.days}
    return on_day.weekday() in allowed


def next_session_start_utc(session: SessionConfig, now_utc: datetime) -> datetime:
    """Next UTC datetime when this session opens, strictly in the future."""
    tz = ZoneInfo(session.timezone)
    local_now = now_utc.astimezone(tz)
    for offset in range(0, 14):
        candidate_day = (local_now + timedelta(days=offset)).date()
        if not is_active_day(session, candidate_day):
            continue
        local_dt = datetime.combine(candidate_day, session.start, tzinfo=tz)
        utc_dt = local_dt.astimezone(ZoneInfo("UTC"))
        if utc_dt > now_utc:
            return utc_dt
    raise RuntimeError("No active session day found in the next 2 weeks")
