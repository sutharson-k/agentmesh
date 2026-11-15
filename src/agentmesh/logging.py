"""Structured JSON logging with optional human-friendly console mode."""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from .util import now


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(now(), 3),
            "level": record.levelname.lower(),
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = value
        return json.dumps(payload, separators=(",", ":"), default=str)


class ConsoleFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\x1b[90m",
        "INFO": "\x1b[36m",
        "WARNING": "\x1b[33m",
        "ERROR": "\x1b[31m",
        "CRITICAL": "\x1b[41m",
    }

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        reset = "\x1b[0m" if color else ""
        stamp = self.formatTime(record, "%H:%M:%S")
        extras = getattr(record, "extra_fields", {})
        suffix = ""
        if extras:
            suffix = " " + " ".join(f"{k}={v}" for k, v in extras.items())
        return f"{color}{stamp} {record.levelname:<7}{reset} [{self.service}] {record.getMessage()}{suffix}"


def setup_logging(level: str = "INFO", service: str = "agentmesh", json_logs: bool = True) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter(service) if json_logs else ConsoleFormatter(service))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    # uvicorn access logs duplicate our own structured output
    logging.getLogger("uvicorn.access").disabled = True


def log_with(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    logger.log(level, msg, extra={"extra_fields": fields})
