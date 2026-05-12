from datetime import datetime, timezone

from src.config.models import SessionConfig
from src.engine.session_clock import next_session_start_utc


def test_london_open_dst():
    # Mon 5 May 2026 00:00 UTC -> next London 09:00 = 08:00 UTC (BST)
    sess = SessionConfig.model_validate({
        "start": "09:00", "timezone": "Europe/London",
        "days": ["mon", "tue", "wed", "thu", "fri"],
    })
    now = datetime(2026, 5, 4, 0, 0, tzinfo=timezone.utc)
    nxt = next_session_start_utc(sess, now)
    assert nxt == datetime(2026, 5, 4, 8, 0, tzinfo=timezone.utc)


def test_skip_weekend():
    sess = SessionConfig.model_validate({
        "start": "09:30", "timezone": "America/New_York",
        "days": ["mon", "tue", "wed", "thu", "fri"],
    })
    # Saturday -> next Monday 09:30 NY = 13:30 UTC (EDT)
    now = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
    nxt = next_session_start_utc(sess, now)
    assert nxt.weekday() == 0
    assert nxt.hour == 13 and nxt.minute == 30
