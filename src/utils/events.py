"""In-memory ring buffer of structured events shown in the web UI."""
from __future__ import annotations

import json
import threading
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from loguru import logger

EventLevel = Literal["info", "warn", "error", "success"]


@dataclass(frozen=True)
class Event:
    ts: str
    level: EventLevel
    instance: str          # strategy:symbol:session:date or "system"
    message: str
    data: dict = field(default_factory=dict)


class EventLog:
    def __init__(self, capacity: int = 500, persist_path: Path | None = None) -> None:
        self._buf: deque[Event] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._persist_path = persist_path
        if persist_path is not None:
            try:
                persist_path.parent.mkdir(parents=True, exist_ok=True)
                if persist_path.exists():
                    self._load_from_disk()
            except Exception as exc:
                logger.warning("EventLog: could not load persisted events: {}", exc)

    def _load_from_disk(self) -> None:
        assert self._persist_path is not None
        loaded = 0
        with self._persist_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    self._buf.append(Event(
                        ts=obj["ts"],
                        level=obj["level"],
                        instance=obj["instance"],
                        message=obj["message"],
                        data=obj.get("data", {}) or {},
                    ))
                    loaded += 1
                except Exception:
                    continue
        if loaded:
            logger.info("EventLog: restored {} events from {}", loaded, self._persist_path)

    def _append_to_disk(self, ev: Event) -> None:
        if self._persist_path is None:
            return
        try:
            with self._persist_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(asdict(ev), default=str) + "\n")
        except Exception as exc:
            logger.warning("EventLog: failed to persist event: {}", exc)

    def add(self, level: EventLevel, instance: str, message: str, **data) -> None:
        ev = Event(
            ts=datetime.now(timezone.utc).isoformat(),
            level=level,
            instance=instance,
            message=message,
            data=data,
        )
        with self._lock:
            self._buf.append(ev)
        self._append_to_disk(ev)
        log_fn = {
            "info": logger.info,
            "warn": logger.warning,
            "error": logger.error,
            "success": logger.info,
        }[level]
        log_fn("[{}] {} {}", instance, message, data if data else "")

    def info(self, instance: str, message: str, **data) -> None:
        self.add("info", instance, message, **data)

    def warn(self, instance: str, message: str, **data) -> None:
        self.add("warn", instance, message, **data)

    def error(self, instance: str, message: str, **data) -> None:
        self.add("error", instance, message, **data)

    def success(self, instance: str, message: str, **data) -> None:
        self.add("success", instance, message, **data)

    def snapshot(self, since_ts: str | None = None, limit: int = 200) -> list[dict]:
        with self._lock:
            items = list(self._buf)
        if since_ts:
            items = [e for e in items if e.ts > since_ts]
        items = items[-limit:]
        return [asdict(e) for e in items]

    def clear(self) -> int:
        with self._lock:
            n = len(self._buf)
            self._buf.clear()
        if self._persist_path is not None:
            try:
                self._persist_path.write_text("", encoding="utf-8")
            except Exception as exc:
                logger.warning("EventLog: failed to truncate persist file: {}", exc)
        return n


# Single shared instance, persisted across server restarts.
EVENTS = EventLog(persist_path=Path("logs/events.jsonl"))
