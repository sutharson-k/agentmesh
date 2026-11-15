"""Sandbox backend contract shared by Docker, Firecracker and the fake backend."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


class BackendUnavailable(RuntimeError):
    """Raised during startup when the host cannot provide this backend."""


@dataclass
class SandboxRequest:
    task_id: str
    attempt: int
    run_id: str
    image: str
    argv: list[str]
    env: dict[str, str]
    cpu: float
    memory_mb: int
    pids: int
    timeout_s: int
    network_mode: str
    allow: list[str]
    workspace: str
    labels: dict[str, str] = field(default_factory=dict)
    log_tail_limit: int = 16_384

    @property
    def sandbox_key(self) -> str:
        """Stable identity for *this* one agent run, never reused."""
        return f"{self.task_id}:{self.attempt}:{self.run_id}"


@dataclass
class RunResult:
    outcome: str  # succeeded | failed | timeout | canceled
    exit_code: int | None = None
    logs: str = ""
    result: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    peak_memory_mb: int | None = None
    cpu_seconds: float | None = None
    duration_s: float | None = None
    backend: str = "unknown"
    sandbox_id: str = ""

    def as_complete_payload(self) -> dict[str, Any]:
        return {
            "result": {
                **self.result,
                "backend": self.backend,
                "sandbox_id": self.sandbox_id,
            },
            "peak_memory_mb": self.peak_memory_mb,
            "cpu_seconds": self.cpu_seconds,
            "duration_s": self.duration_s,
        }


class SandboxBackend:
    name = "base"

    async def prepare(self) -> None:
        """Validate host prerequisites. Raise BackendUnavailable if unusable."""

    async def run(self, req: SandboxRequest, cancel_event: asyncio.Event) -> RunResult:
        raise NotImplementedError

    async def close(self) -> None:
        """Best-effort cleanup of long-lived resources."""

    def info(self) -> dict[str, Any]:
        return {"backend": self.name}


class CommandError(RuntimeError):
    def __init__(self, argv: list[str], code: int, stderr: str) -> None:
        self.argv = argv
        self.code = code
        self.stderr = stderr
        super().__init__(f"{' '.join(argv[:3])}... exited {code}: {stderr.strip()[:500]}")
