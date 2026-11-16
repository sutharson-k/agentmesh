"""Distributed worker daemon.

Workers pull work from the control plane over authenticated HTTP, run exactly
one sandboxed agent per leased task, heartbeat their leases, and report
results. Any worker can die at any point without losing work: leases expire and
the control plane requeues the task.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import shutil
import signal
import socket
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from ..agents.runtime import AgentConfigError, resolve_agent_command, write_prompt
from ..config import Config
from ..util import mono, tail_text
from .backends import SandboxBackend, SandboxRequest, create_backend
from .backends.base import BackendUnavailable, RunResult

log = logging.getLogger("agentmesh.worker")

EGRESS_MEMORY_OVERHEAD_MB = 96
EGRESS_CPU_OVERHEAD = 0.25


@dataclass
class RunningState:
    task_id: str
    lease_id: str
    attempt: int
    spec: dict[str, Any]
    cpu: float
    memory_mb: int
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_reason: str = ""
    process: asyncio.Task[Any] | None = None
    workspace: str = ""
    started_at: float = field(default_factory=time.time)

    @property
    def reserved_cpu(self) -> float:
        overhead = EGRESS_CPU_OVERHEAD if self.spec.get("network", {}).get("mode") == "egress_allowlist" else 0.0
        return self.cpu + overhead

    @property
    def reserved_memory_mb(self) -> int:
        overhead = (
            EGRESS_MEMORY_OVERHEAD_MB
            if self.spec.get("network", {}).get("mode") == "egress_allowlist"
            else 0
        )
        return self.memory_mb + overhead


class WorkerDaemon:
    def __init__(
        self,
        cfg: Config,
        backend: SandboxBackend | None = None,
        *,
        client: httpx.AsyncClient | None = None,
        allow_fake_fallback: bool = False,
    ) -> None:
        self.cfg = cfg
        self.wcfg = cfg.worker
        self.worker_id = self.wcfg.worker_id or self._default_id()
        self.workdir = Path(self.wcfg.workdir).resolve()
        self.backend = backend or create_backend(
            cfg, self.worker_id, allow_fake_fallback=allow_fake_fallback
        )
        headers = {"User-Agent": f"agentmesh-worker/{self.worker_id}"}
        if self.wcfg.token:
            headers["Authorization"] = f"Bearer {self.wcfg.token}"
        self._owns_client = client is None
        self.http = client or httpx.AsyncClient(
            base_url=self.wcfg.control_url.rstrip("/"),
            headers=headers,
            timeout=httpx.Timeout(45.0, connect=10.0),
        )
        self.running: dict[str, RunningState] = {}
        self.stop_event = asyncio.Event()
        self._shutdown_done = asyncio.Event()
        self._hb_failures = 0
        self._leases_total = 0
        self._completed_total = 0
        self._registered = False

    @staticmethod
    def _default_id() -> str:
        return f"{socket.gethostname().split('.')[0]}-{secrets.token_hex(4)}"

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)
        await self.backend.prepare()
        log.info(
            "worker starting",
            extra={
                "extra_fields": {
                    "worker_id": self.worker_id,
                    "backend": self.backend.name,
                    "slots": self.wcfg.slots,
                    "control_url": self.wcfg.control_url,
                }
            },
        )
        await self._register()
        hb = asyncio.create_task(self._heartbeat_loop(), name=f"{self.worker_id}-heartbeat")
        try:
            await self._lease_loop()
        finally:
            self.stop_event.set()
            await self._shutdown_tasks()
            hb.cancel()
            await asyncio.gather(hb, return_exceptions=True)
            if self._owns_client:
                await self.http.aclose()
            await self.backend.close()

    def stop(self) -> None:
        self.stop_event.set()

    async def _register(self) -> None:
        payload = {
            "worker_id": self.worker_id,
            "hostname": socket.gethostname(),
            "pool": self.wcfg.pool,
            "slots": self.wcfg.slots,
            "cpu_total": self.wcfg.cpu_total,
            "memory_mb_total": self.wcfg.memory_mb_total,
            "capabilities": self.wcfg.capabilities,
            "labels": self.wcfg.labels,
            "backend": self.backend.name,
        }
        last_error: Exception | None = None
        for attempt in range(10):
            try:
                resp = await self.http.post("/v1/workers/register", json=payload)
                resp.raise_for_status()
                data = resp.json()
                self.worker_id = data.get("worker_id", self.worker_id)
                self._registered = True
                return
            except Exception as exc:
                last_error = exc
                log.warning("worker registration failed (attempt %s): %s", attempt + 1, exc)
                await asyncio.sleep(min(10, 0.5 * 2**attempt))
        raise RuntimeError(f"could not register with control plane: {last_error}")

    # -------------------------------------------------------------- capacity

    @property
    def free_slots(self) -> int:
        return max(0, self.wcfg.slots - len(self.running))

    @property
    def free_cpu(self) -> float:
        return max(0.0, self.wcfg.cpu_total - sum(s.reserved_cpu for s in self.running.values()))

    @property
    def free_memory_mb(self) -> int:
        return max(
            0, self.wcfg.memory_mb_total - sum(s.reserved_memory_mb for s in self.running.values())
        )

    # ------------------------------------------------------------------ pull

    async def _lease_loop(self) -> None:
        backoff = 0.5
        while not self.stop_event.is_set():
            if self.free_slots <= 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
                continue
            body = {
                "wait_s": self.wcfg.lease_wait_s,
                "free_cpu": round(self.free_cpu, 3),
                "free_mem_mb": self.free_memory_mb,
                "free_slots": self.free_slots,
            }
            try:
                resp = await self.http.post(f"/v1/workers/{self.worker_id}/lease", json=body)
                if resp.status_code == 401:
                    raise RuntimeError("control plane rejected worker credentials")
                resp.raise_for_status()
                leases = resp.json().get("tasks", [])
                backoff = 0.5
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("lease request failed: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(10.0, backoff * 1.7)
                continue
            now = mono()
            for lease in leases:
                task = lease["task"]
                task_id = task["id"]
                if task_id in self.running:
                    log.warning("control plane returned already-running task %s", task_id)
                    continue
                spec = task["spec"]
                state = RunningState(
                    task_id=task_id,
                    lease_id=lease["lease_id"],
                    attempt=int(task.get("attempt", 1)),
                    spec=spec,
                    cpu=float(spec.get("cpu", 1.0)),
                    memory_mb=int(spec.get("memory_mb", 512)),
                )
                self.running[task_id] = state
                self._leases_total += 1
                state.process = asyncio.create_task(
                    self._run_task(state), name=f"task-{task_id}"
                )
                log.info(
                    "leased task",
                    extra={
                        "extra_fields": {
                            "task_id": task_id,
                            "attempt": state.attempt,
                            "tenant": task.get("tenant"),
                        }
                    },
                )
            _ = now
            if not leases:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=0.25)
                except asyncio.TimeoutError:
                    pass

    async def _heartbeat_loop(self) -> None:
        interval = self.wcfg.heartbeat_interval_s
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=interval)
                return
            except asyncio.TimeoutError:
                pass
            body = {
                "running": [
                    {"task_id": state.task_id, "lease_id": state.lease_id}
                    for state in list(self.running.values())
                    if state.process is not None
                ],
                "free_cpu": round(self.free_cpu, 3),
                "free_mem_mb": self.free_memory_mb,
                "free_slots": self.free_slots,
            }
            try:
                resp = await self.http.post(f"/v1/workers/{self.worker_id}/heartbeat", json=body)
                if resp.status_code == 404:
                    log.warning("worker was forgotten by control plane; re-registering")
                    await self._register()
                    continue
                resp.raise_for_status()
                self._hb_failures = 0
                for task_id in resp.json().get("cancel", []):
                    state = self.running.get(task_id)
                    if state and not state.cancel_event.is_set():
                        log.info("cancel requested for task %s", task_id)
                        state.cancel_reason = "canceled by control plane"
                        state.cancel_event.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._hb_failures += 1
                log.warning("heartbeat failed (%s): %s", self._hb_failures, exc)
                if self._hb_failures >= 5:
                    log.error("control plane unreachable; terminating local sandboxes")
                    for state in self.running.values():
                        state.cancel_reason = "lost control plane"
                        state.cancel_event.set()
                    self.stop_event.set()
                    return

    # ------------------------------------------------------------- execution

    async def _run_task(self, state: RunningState) -> None:
        run_id = secrets.token_hex(6)
        workspace = (
            self.workdir
            / "runs"
            / state.task_id.replace("task_", "t")
            / f"attempt{state.attempt}-{run_id}"
        )
        workspace.mkdir(parents=True, exist_ok=False)
        try:
            os.chmod(workspace, 0o777)
        except OSError:
            pass
        state.workspace = str(workspace)
        spec = state.spec
        logs: list[str] = []
        try:
            target = spec.get("network") or {}
            allow = list(target.get("allow") or []) if target.get("mode") == "egress_allowlist" else []
            argv, agent_env, prompt = resolve_agent_command(
                spec, templates=self.cfg.agents.commands
            )
            prompt_file = write_prompt(
                str(workspace),
                prompt,
                {"task_id": state.task_id, "attempt": state.attempt, "run_id": run_id},
            )
            env = dict(spec.get("env") or {})
            env.update(agent_env)
            env.update(self.cfg.agents.env)
            env.update(
                {
                    "AGENTMESH_TASK_ID": state.task_id,
                    "AGENTMESH_TENANT": spec.get("tenant", "default"),
                    "AGENTMESH_ATTEMPT": str(state.attempt),
                    "AGENTMESH_PROMPT_FILE": "/workspace/.agentmesh/prompt.txt",
                    "HOME": "/workspace",
                }
            )
            req = SandboxRequest(
                task_id=state.task_id,
                attempt=state.attempt,
                run_id=run_id,
                image=spec.get("image", "python:3.12-slim"),
                argv=argv,
                env=env,
                cpu=float(spec.get("cpu", 1.0)),
                memory_mb=int(spec.get("memory_mb", 512)),
                pids=int(spec.get("pids", 256)),
                timeout_s=int(spec.get("timeout_s", 900)),
                network_mode=(spec.get("network") or {}).get("mode", "none"),
                allow=allow,
                workspace=str(workspace),
                labels=spec.get("labels") or {},
                log_tail_limit=self.wcfg.log_tail_bytes,
            )
            result = await self.backend.run(req, state.cancel_event)
            _ = prompt_file
            artifacts = self._artifacts(workspace)
            result.result.setdefault("artifacts", artifacts)
            result.result.setdefault("workspace", str(workspace))
            log_text = result.logs
            if not log_text:
                log_text = f"agent finished with exit code {result.exit_code}\n"
            self._write_local_log(workspace, log_text)
            await self._report(state, result)
        except AgentConfigError as exc:
            await self._report_error(state, f"agent configuration error: {exc}", workspace)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.exception("task execution failed", extra={"extra_fields": {"task_id": state.task_id}})
            await self._report_error(state, f"worker error: {exc}", workspace)
        finally:
            self.running.pop(state.task_id, None)
            self._completed_total += 1

    def _write_local_log(self, workspace: Path, text: str) -> None:
        try:
            (workspace / "agentmesh.log").write_text(text, encoding="utf-8", errors="replace")
        except OSError:
            pass

    def _artifacts(self, workspace: Path) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        try:
            for path in sorted(workspace.rglob("*")):
                if not path.is_file() or ".agentmesh" in path.parts:
                    continue
                try:
                    size = path.stat().st_size
                except OSError:
                    continue
                out.append({"path": str(path.relative_to(workspace)), "size": size})
                if len(out) >= 200:
                    break
        except OSError:
            pass
        return out

    async def _report(self, state: RunningState, result: RunResult) -> None:
        payload = {
            "task_id": state.task_id,
            "lease_id": state.lease_id,
            "outcome": result.outcome,
            "exit_code": result.exit_code,
            "error": result.error,
            "logs_tail": tail_text(result.logs, self.wcfg.log_tail_bytes),
            **result.as_complete_payload(),
        }
        if result.outcome == "succeeded":
            crypto = workspace_solution(state.workspace)
            if crypto:
                payload["result"].setdefault("solution", crypto)
        await self._post_complete(payload, state.task_id)

    async def _report_error(self, state: RunningState, error: str, workspace: Path) -> None:
        self._write_local_log(workspace, error + "\n")
        payload = {
            "task_id": state.task_id,
            "lease_id": state.lease_id,
            "outcome": "failed",
            "exit_code": None,
            "error": error,
            "logs_tail": error,
            "result": {"workspace": str(workspace)},
        }
        await self._post_complete(payload, state.task_id)

    async def _post_complete(self, payload: dict[str, Any], task_id: str) -> None:
        for attempt in range(4):
            try:
                resp = await self.http.post(
                    f"/v1/workers/{self.worker_id}/complete", json=payload
                )
                if resp.status_code in (200, 204, 409):
                    return
                if resp.status_code == 404:
                    log.warning("task %s no longer known to control plane", task_id)
                    return
                resp.raise_for_status()
                return
            except Exception as exc:
                log.warning("result report failed for %s (attempt %s): %s", task_id, attempt + 1, exc)
                await asyncio.sleep(min(8, 2**attempt))
        log.error("could not report result for %s; lease will expire", task_id)

    async def _shutdown_tasks(self) -> None:
        if not self.running:
            return
        log.info("draining %s running sandboxes", len(self.running))
        for state in list(self.running.values()):
            state.cancel_reason = "worker shutting down"
            state.cancel_event.set()
        deadline = mono() + 25
        while self.running and mono() < deadline:
            await asyncio.sleep(0.1)
        for state in list(self.running.values()):
            if state.process:
                state.process.cancel()
        if self.running:
            await asyncio.gather(
                *[s.process for s in self.running.values() if s.process], return_exceptions=True
            )

    def snapshot(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "backend": self.backend.name,
            "slots": self.wcfg.slots,
            "running": len(self.running),
            "free_slots": self.free_slots,
            "free_cpu": round(self.free_cpu, 2),
            "free_memory_mb": self.free_memory_mb,
            "leased_total": self._leases_total,
            "completed_total": self._completed_total,
            "tasks": [
                {
                    "task_id": s.task_id,
                    "attempt": s.attempt,
                    "workspace": s.workspace,
                    "started_at": s.started_at,
                }
                for s in self.running.values()
            ],
        }


def workspace_solution(workspace: str) -> dict[str, Any] | None:
    path = Path(workspace) / "solution.json"
    try:
        if path.exists() and path.stat().st_size < 1_000_000:
            return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return None


def install_signal_handlers(daemon: WorkerDaemon, loop: asyncio.AbstractEventLoop) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, daemon.stop)
        except (NotImplementedError, RuntimeError):
            # Windows / Proactor loop: rely on KeyboardInterrupt in the CLI.
            pass
