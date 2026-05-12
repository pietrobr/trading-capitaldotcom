from __future__ import annotations

from typing import Literal

DEMO_BASE_URL = "https://demo-api-capital.backend-capital.com"
LIVE_BASE_URL = "https://api-capital.backend-capital.com"


def base_url(mode: Literal["demo", "live"]) -> str:
    return DEMO_BASE_URL if mode == "demo" else LIVE_BASE_URL
