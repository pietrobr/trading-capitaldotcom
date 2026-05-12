"""
Minimal Capital.com REST client.
Docs: https://open-api.capital.com/

Implements only what the bot needs:
  - session login (POST /api/v1/session)
  - historical prices (GET /api/v1/prices/{epic})
  - place / cancel orders, list positions

Designed to be easy to mock in tests.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

import httpx
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .endpoints import base_url
from .models import Candle, Order, Position, Resolution


class CapitalApiError(RuntimeError):
    """Wraps an httpx HTTPStatusError with a readable message."""

    def __init__(self, status_code: int, body: str, url: str) -> None:
        self.status_code = status_code
        self.body = body
        self.url = url
        super().__init__(f"HTTP {status_code} on {url}: {body[:300]}")


def _is_transient(exc: BaseException) -> bool:
    if isinstance(exc, CapitalApiError):
        # Retry only on 5xx and 429.
        return exc.status_code >= 500 or exc.status_code == 429
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException)):
        return True
    return False


class CapitalClient:
    def __init__(
        self,
        api_key: str,
        identifier: str,
        password: str,
        mode: Literal["demo", "live"] = "demo",
        timeout: float = 15.0,
    ) -> None:
        self._mode = mode
        self._api_key = api_key
        self._identifier = identifier
        self._password = password
        self._base = base_url(mode)
        self._client = httpx.AsyncClient(base_url=self._base, timeout=timeout)
        self._cst: str | None = None
        self._x_security_token: str | None = None

    @property
    def mode(self) -> str:
        return self._mode

    async def aclose(self) -> None:
        await self._client.aclose()

    # ---------- session ----------
    async def login(self) -> None:
        headers = {
            "X-CAP-API-KEY": self._api_key,
            "Content-Type": "application/json",
        }
        body = {"identifier": self._identifier, "password": self._password}
        r = await self._client.post("/api/v1/session", json=body, headers=headers)
        r.raise_for_status()
        self._cst = r.headers.get("CST")
        self._x_security_token = r.headers.get("X-SECURITY-TOKEN")
        if not self._cst or not self._x_security_token:
            raise RuntimeError("Capital.com login: missing session tokens in response")
        logger.info("Capital.com session established (mode={})", self._mode)

    async def logout(self) -> None:
        if not self._cst or not self._x_security_token:
            return
        try:
            await self._client.delete("/api/v1/session", headers=self._auth_headers())
        finally:
            self._cst = None
            self._x_security_token = None
            logger.info("Capital.com session closed (mode={})", self._mode)

    def _auth_headers(self) -> dict[str, str]:
        if not self._cst or not self._x_security_token:
            raise RuntimeError("Not logged in. Call login() first.")
        return {
            "X-CAP-API-KEY": self._api_key,
            "CST": self._cst,
            "X-SECURITY-TOKEN": self._x_security_token,
            "Content-Type": "application/json",
        }

    @staticmethod
    def _is_session_expired(r: httpx.Response) -> bool:
        # Capital returns 401 with errorCode like "error.invalid.session.token" /
        # "error.null.cst" when the session has expired.
        if r.status_code != 401:
            return False
        try:
            code = (r.json() or {}).get("errorCode", "") or ""
        except Exception:
            code = ""
        code = code.lower()
        return (
            "session" in code
            or "cst" in code
            or "token" in code
            or "unauthor" in code
            or code == ""
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send an authenticated request; on expired session re-login once and retry."""
        kwargs["headers"] = self._auth_headers()
        r = await self._client.request(method, path, **kwargs)
        if self._is_session_expired(r):
            logger.warning("Capital.com session expired, re-logging in and retrying {} {}", method, path)
            await self.login()
            kwargs["headers"] = self._auth_headers()
            r = await self._client.request(method, path, **kwargs)
        return r

    # ---------- market data ----------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(min=1, max=8),
        retry=retry_if_exception(_is_transient),
        reraise=True,
    )
    async def get_prices(
        self,
        epic: str,
        resolution: Resolution,
        from_dt: datetime,
        to_dt: datetime,
        max_points: int = 1000,
    ) -> list[Candle]:
        params = {
            "resolution": resolution,
            "from": from_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "to": to_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "max": max_points,
        }
        r = await self._request(
            "GET", f"/api/v1/prices/{epic}", params=params
        )
        if r.status_code == 404:
            # Capital returns 404 with errorCode=error.prices.not-found when
            # no candles are available in the requested range yet.
            try:
                code = r.json().get("errorCode", "")
            except Exception:
                code = ""
            if "prices.not-found" in code or r.status_code == 404:
                return []
        if r.status_code >= 400:
            raise CapitalApiError(r.status_code, r.text, str(r.request.url))
        data = r.json().get("prices", [])
        return [_parse_candle(p) for p in data]

    # ---------- trading ----------
    async def place_order(self, order: Order) -> dict[str, Any]:
        body: dict[str, Any] = {
            "epic": order.epic,
            "direction": order.side,
            "size": order.size,
        }
        if order.type == "MARKET":
            path = "/api/v1/positions"
        else:
            path = "/api/v1/workingorders"
            body["type"] = order.type
            body["level"] = order.level
        if order.stop_loss is not None:
            body["stopLevel"] = order.stop_loss
        if order.take_profit is not None:
            body["profitLevel"] = order.take_profit
        if order.client_ref:
            body["reference"] = order.client_ref

        r = await self._request("POST", path, json=body)
        r.raise_for_status()
        return r.json()

    async def cancel_working_order(self, deal_id: str) -> dict[str, Any]:
        r = await self._request(
            "DELETE", f"/api/v1/workingorders/{deal_id}"
        )
        r.raise_for_status()
        return r.json()

    async def list_positions(self) -> list[Position]:
        r = await self._request("GET", "/api/v1/positions")
        r.raise_for_status()
        out: list[Position] = []
        for item in r.json().get("positions", []):
            pos = item["position"]
            mkt = item["market"]
            out.append(
                Position(
                    deal_id=pos["dealId"],
                    epic=mkt["epic"],
                    side=pos["direction"],
                    size=float(pos["size"]),
                    open_level=float(pos["level"]),
                )
            )
        return out


def _parse_candle(p: dict[str, Any]) -> Candle:
    # Capital returns OHLC as {bid, ask} pairs; use mid.
    def mid(field: str) -> float:
        v = p[field]
        if isinstance(v, dict):
            return (float(v["bid"]) + float(v["ask"])) / 2.0
        return float(v)

    ts = datetime.fromisoformat(p["snapshotTimeUTC"].replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return Candle(
        ts=ts,
        open=mid("openPrice"),
        high=mid("highPrice"),
        low=mid("lowPrice"),
        close=mid("closePrice"),
        volume=float(p.get("lastTradedVolume", 0) or 0),
    )
