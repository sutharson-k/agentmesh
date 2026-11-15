"""In-process fake backend.

Used for tests and the local demo. It creates a genuinely separate workspace
per run (the worker does that), but does not isolate the process with the OS.
It must never be selected in production; ``auto`` only falls back to it when
Docker is unavailable and logs a loud warning.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from ...util import mono
from .base import RunResult, SandboxBackend, SandboxRequest


def _parse_marker(prompt: str, key: str, default: float) -> float:
    marker = f"[{key}="
    if marker in prompt:
        try:
            return float(prompt.split(marker, 1)[1].split("]", 1)[0])
        except (ValueError, IndexError):
            return default
    return default


class FakeBackend(SandboxBackend):
    name = "fake"

    def __init__(self, *, latency_s: float = 0.3) -> None:
        self.latency_s = latency_s

    async def run(self, req: SandboxRequest, cancel_event: asyncio.Event) -> RunResult:
        start = mono()
        ws = Path(req.workspace)
        ws.mkdir(parents=True, exist_ok=True)
        prompt = ""
        prompt_path = ws / ".agentmesh" / "prompt.txt"
        if prompt_path.exists():
            prompt = prompt_path.read_text(encoding="utf-8", errors="replace")
        delay = _parse_marker(prompt, "sleep", self.latency_s)
        fail = "[fail]" in prompt
        timeout_marker = _parse_marker(prompt, "timeout", 0.0)
        lines = [
            f"agent={req.task_id} sandbox={req.sandbox_key} backend=fake",
            f"resources cpu={req.cpu} mem={req.memory_mb}MB pids={req.pids}",
            f"network={req.network_mode} allow={','.join(req.allow)}",
            "step 1/3 inspect workspace",
        ]
        deadline = start + (timeout_marker or req.timeout_s)
        outcome = "succeeded"
        error = None
        exit_code = 0
        while mono() < deadline:
            if cancel_event.is_set():
                outcome, exit_code, error = "canceled", 143, "canceled"
                lines.append("canceled: sandbox terminated")
                break
            if mono() - start >= delay:
                break
            await asyncio.sleep(min(0.05, max(0.001, delay - (mono() - start))))
        else:
            outcome, exit_code, error = "timeout", 124, "sandbox exceeded timeout"
            lines.append("timeout: sandbox terminated")

        if outcome == "succeeded":
            if fail:
                outcome, exit_code, error = "failed", 3, "simulated agent failure"
                lines.append("step 2/3 failed: simulated agent failure")
            else:
                lines += ["step 2/3 edit files", "step 3/3 summarize", "mock-done"]
                digest = hashlib.sha256(prompt.encode()).hexdigest()[:12]
                (ws / "agent_report.md").write_text(
                    f"# Agent report\n\n- task: `{req.task_id}`\n- sandbox: `{req.sandbox_key}`\n"
                    f"- prompt sha256: `{digest}`\n- backend: fake\n",
                    encoding="utf-8",
                )
                (ws / "solution.json").write_text(
                    json.dumps(
                        {
                            "task_id": req.task_id,
                            "status": "ok",
                            "sandbox_key": req.sandbox_key,
                            "prompt_chars": len(prompt),
                        },
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                (ws / "agentmesh.log").write_text("\n".join(lines) + "\n", encoding="utf-8")

        duration = mono() - start
        result: dict[str, Any] = {
            "sandbox_key": req.sandbox_key,
            "network_mode": req.network_mode,
            "isolated_workspace": str(ws),
        }
        if outcome == "succeeded":
            solution = ws / "solution.json"
            if solution.exists():
                try:
                    result.update(json.loads(solution.read_text(encoding="utf-8")))
                except json.JSONDecodeError:
                    pass
        return RunResult(
            outcome=outcome,
            exit_code=exit_code,
            logs="\n".join(lines) + "\n",
            result=result,
            error=error,
            peak_memory_mb=int(min(req.memory_mb, 64 + len(prompt) / 100)),
            cpu_seconds=round(min(duration * req.cpu, req.cpu * req.timeout_s), 3),
            duration_s=round(duration, 3),
            backend="fake",
            sandbox_id=req.sandbox_key,
        )

    async def close(self) -> None:
        return None
