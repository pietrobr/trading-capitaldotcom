"""
Application state shared across HTTP handlers.
The web server starts even when config or credentials are missing — state
fields hold the load errors so the dashboard can show what to fix.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from src.broker.capital_client import CapitalClient
from src.config.loader import load_config
from src.config.models import AppConfig
from src.engine.trading_engine import TradingEngine


@dataclass
class AppState:
    config_path: str = "config.yaml"
    config: AppConfig | None = None
    config_error: str | None = None
    credentials_ok: bool = False
    credentials_error: str | None = None
    client: CapitalClient | None = None
    engine: TradingEngine | None = None
    logged_in: bool = False
    last_login_error: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def ready(self) -> bool:
        return self.config is not None and self.credentials_ok


def _check_credentials() -> tuple[bool, str | None]:
    missing = [
        k for k in ("CAPITAL_API_KEY", "CAPITAL_IDENTIFIER", "CAPITAL_PASSWORD")
        if not os.environ.get(k)
    ]
    if missing:
        return False, f"Missing env vars: {', '.join(missing)}"
    return True, None


def load_state(config_path: str = "config.yaml") -> AppState:
    state = AppState(config_path=config_path)

    if not Path(config_path).exists():
        state.config_error = f"Config file not found: {config_path}"
        logger.warning(state.config_error)
    else:
        try:
            state.config = load_config(config_path)
            logger.info("Config loaded ({} strategies)", len(state.config.strategies))
        except Exception as e:
            state.config_error = f"Config invalid: {e}"
            logger.exception("Config load failed")

    state.credentials_ok, state.credentials_error = _check_credentials()
    if not state.credentials_ok:
        logger.warning(state.credentials_error)

    return state


async def try_login(state: AppState) -> None:
    """Best-effort login. Failures are stored in state, never raised."""
    if not state.ready or state.config is None:
        return
    if state.logged_in and state.client is not None:
        return
    try:
        state.client = CapitalClient(
            api_key=os.environ["CAPITAL_API_KEY"],
            identifier=os.environ["CAPITAL_IDENTIFIER"],
            password=os.environ["CAPITAL_PASSWORD"],
            mode=state.config.account.mode,
        )
        await state.client.login()
        state.logged_in = True
        state.last_login_error = None
        state.engine = TradingEngine(state.config, state.client, dry_run=True)
    except Exception as e:
        state.last_login_error = str(e)
        state.logged_in = False
        logger.exception("Login failed")


async def try_logout(state: AppState) -> None:
    """Best-effort logout. Always clears local session state."""
    if state.client is not None:
        try:
            await state.client.logout()
            await state.client.aclose()
        except Exception as e:
            logger.warning("Logout error: {}", e)
    state.client = None
    state.engine = None
    state.logged_in = False
    state.last_login_error = None
