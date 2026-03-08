"""Structured logging helpers for the simplified GX framework."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from .utils import utc_now


class JsonLogFormatter(logging.Formatter):
    """Format log records as JSON strings."""

    _reserved = {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as JSON."""
        payload = {
            "timestamp_utc": utc_now().isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in self._reserved and not key.startswith("_")
        }
        if extras:
            payload.update(extras)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(logs_root: str | Path, log_level: str = "INFO") -> logging.Logger:
    """Return a configured framework logger with console and file handlers.

    Args:
        logs_root: Root directory for log files.
        log_level: Logging level for the logger and handlers.

    Returns:
        A configured logger instance.
    """
    logs_path = Path(logs_root)
    logs_path.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("gx_framework")
    logger.setLevel(_coerce_log_level(log_level))
    logger.propagate = False

    log_file = logs_path / f"gx_validation_{utc_now().strftime('%Y%m%d')}.log"
    formatter = JsonLogFormatter()

    existing_targets = {
        getattr(handler, "baseFilename", None): handler
        for handler in logger.handlers
    }

    if None not in existing_targets:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(_coerce_log_level(log_level))
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if str(log_file) not in existing_targets:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(_coerce_log_level(log_level))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def _coerce_log_level(log_level: str) -> int:
    """Resolve a logging level string to its numeric value."""
    return getattr(logging, str(log_level).upper(), logging.INFO)