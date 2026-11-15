"""Small shared helpers: time, ids, redaction, bounded text."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import time
import uuid
from typing import Any

_SECRET_KEY = re.compile(r"(?i)(token|secret|password|api[_-]?key|authorization)")


def now() -> float:
    """Wall-clock seconds. Used for persisted timestamps."""
    return time.time()


def mono() -> float:
    """Monotonic seconds. Used for durations and timeouts."""
    return time.monotonic()


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


def new_lease_id() -> str:
    return f"lease_{uuid.uuid4().hex}"


def secure_token(nbytes: int = 24) -> str:
    return secrets.token_urlsafe(nbytes)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def tail_text(text: str, limit: int = 16_000) -> str:
    """Keep the last `limit` characters, noting how much was dropped."""
    if len(text) <= limit:
        return text
    dropped = len(text) - limit
    return f"...[{dropped} bytes truncated]...\n" + text[-limit:]


def redact_env(env: dict[str, str]) -> dict[str, str]:
    """Never persist raw secrets in task specs or events."""
    out: dict[str, str] = {}
    for key, value in env.items():
        if _SECRET_KEY.search(key):
            out[key] = "***"
        else:
            out[key] = value
    return out


def safe_label(value: str, limit: int = 120) -> str:
    value = value.strip()
    return value[:limit]


def coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        code = ctypes.c_ulong()
        ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        ctypes.windll.kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
