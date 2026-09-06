"""统一日志配置。"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | pid=%(process)d | %(filename)s:%(lineno)d | "
    "%(name)s | %(message)s"
)


def configure_logging(level: str = "INFO", *, log_path: str | Path | None = None) -> None:
    """配置包含文件与行号的日志格式，并对平台日志轮转。"""
    root = logging.getLogger()
    numeric = getattr(logging, level.upper(), logging.INFO)
    if root.handlers:
        root.setLevel(numeric)
        return
    handlers: list[logging.Handler] = []
    if log_path is not None:
        path = Path(log_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                path, maxBytes=50 * 1024 * 1024, backupCount=3, encoding="utf-8",
            )
        )
    else:
        handlers.append(logging.StreamHandler())
    logging.basicConfig(level=numeric, format=LOG_FORMAT, handlers=handlers)
