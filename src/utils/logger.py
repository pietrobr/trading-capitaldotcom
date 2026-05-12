from __future__ import annotations

import sys
from pathlib import Path

from loguru import logger


def setup_logging(log_dir: str = "logs", level: str = "INFO") -> None:
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level=level, enqueue=True)
    logger.add(
        f"{log_dir}/bot_{{time:YYYY-MM-DD}}.log",
        rotation="00:00",
        retention="30 days",
        level="DEBUG",
        enqueue=True,
    )
