from __future__ import annotations

from datetime import time
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator

AccountMode = Literal["demo", "live"]
InstrumentType = Literal["forex", "index", "stock", "crypto", "commodity"]
DayName = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class AccountConfig(BaseModel):
    mode: AccountMode = "demo"


class SessionConfig(BaseModel):
    start: time
    timezone: str
    days: list[DayName] = Field(default_factory=lambda: ["mon", "tue", "wed", "thu", "fri"])

    @field_validator("timezone")
    @classmethod
    def _validate_tz(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError as e:
            raise ValueError(f"Unknown timezone: {v}") from e
        return v

    @field_validator("start", mode="before")
    @classmethod
    def _parse_time(cls, v):
        if isinstance(v, str):
            hh, mm = v.split(":")
            return time(int(hh), int(mm))
        return v


class InstrumentConfig(BaseModel):
    symbol: str
    type: InstrumentType


class StrategyDeployment(BaseModel):
    symbol: str
    sessions: list[str]


class StrategyConfig(BaseModel):
    name: str
    enabled: bool = True
    deployments: list[StrategyDeployment]
    params: dict = Field(default_factory=dict)


class AppConfig(BaseModel):
    account: AccountConfig = Field(default_factory=AccountConfig)
    sessions: dict[str, SessionConfig]
    instruments: list[InstrumentConfig]
    strategies: list[StrategyConfig]

    @model_validator(mode="after")
    def _validate_refs(self) -> "AppConfig":
        known_symbols = {m.symbol for m in self.instruments}
        session_ids = set(self.sessions.keys())
        for strat in self.strategies:
            for dep in strat.deployments:
                if dep.symbol not in known_symbols:
                    raise ValueError(
                        f"Strategy '{strat.name}' references unknown symbol '{dep.symbol}'"
                    )
                for sid in dep.sessions:
                    if sid not in session_ids:
                        raise ValueError(
                            f"Strategy '{strat.name}' references unknown session '{sid}'"
                        )
        return self
