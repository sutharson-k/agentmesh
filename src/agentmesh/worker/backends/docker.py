"""Docker sandbox backend: one hardened container per agent run.

Isolation per run:
  * fresh container + PID/IPC/UTS/mount namespaces (Docker defaults)
  * read-only root filesystem, only a private workspace bind mount
  * cgroup v2 CPU, memory (swap disabled), pids and shm limits
  * all capabilities dropped, no-new-privileges, non-root uid, 2k fd limit
  * ``--network none`` by default, or a dedicated internal network whose only
    peer is a per-run egress proxy container enforcing the task allowlist
  * unique container/network/workspace identifiers derived from a random run
    nonce, so retries never inherit another agent's sandbox
"""

from __future__ import annotations

import asyncio
import json
import re
import secrets
import time
from pathlib import Path
from typing import Any

from ...config import DockerConfig
from ...sandbox.docker_spec import (
    DockerSandboxSpec,
    EgressSidecar,
    build_create_argv,
    build_network_create_argv,
    build_sidecar_argv,
    container_name,
    egress_network_name,
)
from ...util import mono
from .base import BackendUnavailable, CommandError, RunResult, SandboxBackend, SandboxRequest

MEM_UNITS = {"b": 1 / (1024**2), "kib": 1 / 1024, "mib": 1, "gib": 1024, "kb": 1 / 1000, "mb": 1, "gb": 1000}
LOG_LIMIT = 2 * 1024 * 1024


def parse_mem(value: str) -> float:
    match = re.match(r"\s*([0-9.]+)\s*([a-zA-Z]+)", value)
    if not match:
        return 0.0
    num, unit = float(match.group(1)), match.group(2).lower()
    return num * MEM_UNITS.get(unit, 1)


class DockerBackend(SandboxBackend):
    name = "docker"

    def __init__(self, cfg: DockerConfig, *, worker_id: str, workdir: str) -> None:
        self.cfg = cfg
        self.worker_id = worker_id
        self.workdir = Path(workdir)
        self.docker = "docker"
        self.proxy_script = str(
            Path(__file__).resolve().parents[2] / "sandbox" / "egress_proxy.py"
        )
        self._prepared = False

    # -------------------------------------------------------------- plumbing

    async def _run(
        self, args: list[str], *, timeout: float = 120, check: bool = True
    ) -> tuple[int, str, str]:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise CommandError(args, -1, f"timed out after {timeout}s")
        code = proc.returncode or 0
        stdout = out.decode("utf-8", "replace")
        stderr = err.decode("utf-8", "replace")
        if check and code != 0:
            raise CommandError(args, code, stderr or stdout)
        return code, stdout, stderr

    async def _exists(self, kind: str, name: str) -> bool:
        code, _, _ = await self._run([self.docker, kind, "inspect", name], check=False)
        return code == 0

    async def prepare(self) -> None:
        try:
            _, out, _ = await self._run([self.docker, "info", "--format", "{{.ServerVersion}}"], timeout=20)
        except (CommandError, FileNotFoundError) as exc:
            raise BackendUnavailable(f"Docker daemon unavailable: {exc}") from exc
        self.docker_version = out.strip()
        if not await self._exists("network", self.cfg.uplink_network):
            await self._run(build_network_create_argv(self.cfg.uplink_network, internal=False))
        if self.cfg.pull_images:
            if not await self._exists("image", self.cfg.proxy_image):
                await self._run([self.docker, "pull", self.cfg.proxy_image], timeout=600)
        elif not await self._exists("image", self.cfg.proxy_image):
            # Egress tasks would fail lazily; surface it at startup instead.
            pass
        self._prepared = True

    async def close(self) -> None:
        return None

    def info(self) -> dict[str, Any]:
        return {"backend": "docker", "docker_version": getattr(self, "docker_version", None)}

    # ------------------------------------------------------------------- run

    async def run(self, req: SandboxRequest, cancel_event: asyncio.Event) -> RunResult:
        started = mono()
        deadline = started + req.timeout_s
        attempt_nonce = req.run_id[:10]
        task_container = container_name(req.task_id, req.attempt, attempt_nonce)
        network = egress_network_name(req.task_id, attempt_nonce)
        proxy_name = f"mesh-egress-{attempt_nonce}-{secrets.token_hex(3)}"
        sidecar: EgressSidecar | None = None
        logs = bytearray()
        peak_memory = 0.0
        cpu_seconds = 0.0
        cancel_flag = False
        timed_out = False
        exit_code: int | None = None
        oom = False
        error: str | None = None
        egress_audit: dict[str, Any] | None = None

        try:
            if req.network_mode == "egress_allowlist":
                sidecar = EgressSidecar(
                    name=proxy_name,
                    network=network,
                    uplink_network=self.cfg.uplink_network,
                    allow=req.allow,
                    token=secrets.token_urlsafe(24),
                    image=self.cfg.proxy_image,
                    port=self.cfg.proxy_port,
                    host_script=self.proxy_script,
                )
                await self._run(build_network_create_argv(network, internal=True))
                await self._run(build_sidecar_argv(sidecar), timeout=120)
                await self._run(
                    [
                        self.docker,
                        "network",
                        "connect",
                        "--alias",
                        "egress",
                        network,
                        proxy_name,
                    ]
                )
                await self._wait_proxy(proxy_name, deadline)

            # A retry can reuse a task id but never a run id; guard anyway.
            if await self._exists("container", task_container):
                await self._run([self.docker, "rm", "-f", task_container], check=False)

            spec = DockerSandboxSpec(
                task_id=req.task_id,
                image=req.image,
                argv=req.argv,
                env=req.env,
                cpu=req.cpu,
                memory_mb=req.memory_mb,
                pids=req.pids,
                network_mode=req.network_mode,
                allow=req.allow,
                workspace_dir=req.workspace,
                allowed_images=self.cfg.allowed_images,
                run_as_user=self.cfg.run_as_user,
                shm_size_mb=self.cfg.shm_size_mb,
                seccomp_profile=self.cfg.seccomp_profile,
                labels={**req.labels, "worker_id": self.worker_id, "run_id": req.run_id},
                egress=sidecar,
                pull=self.cfg.pull_images,
                attempt=req.attempt,
                nonce=attempt_nonce,
            )
            create_argv = build_create_argv(spec, self.docker)
            await self._run(create_argv, timeout=120)
            proc = await asyncio.create_subprocess_exec(
                self.docker,
                "start",
                "-a",
                task_container,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def drain(stream: asyncio.StreamReader) -> None:
                while True:
                    chunk = await stream.read(65_536)
                    if not chunk:
                        break
                    if len(logs) < LOG_LIMIT:
                        logs.extend(chunk[: LOG_LIMIT - len(logs)])

            readers = [asyncio.create_task(drain(proc.stdout)), asyncio.create_task(drain(proc.stderr))]
            wait_task = asyncio.create_task(proc.wait())

            async def sample_stats() -> None:
                nonlocal peak_memory, cpu_seconds
                last = mono()
                while proc.returncode is None:
                    await asyncio.sleep(2.0)
                    if proc.returncode is not None:
                        break
                    try:
                        code, out, _ = await self._run(
                            [
                                self.docker,
                                "stats",
                                "--no-stream",
                                "--format",
                                "{{json .}}",
                                task_container,
                            ],
                            timeout=10,
                            check=False,
                        )
                    except Exception:
                        continue
                    if code != 0 or not out.strip():
                        continue
                    try:
                        row = json.loads(out.strip().splitlines()[-1])
                    except json.JSONDecodeError:
                        continue
                    mem = parse_mem(str(row.get("MemUsage", "0").split("/")[0]))
                    peak_memory = max(peak_memory, mem)
                    try:
                        pct = float(str(row.get("CPUPerc", "0%")).strip("%"))
                    except ValueError:
                        pct = 0.0
                    now_mono = mono()
                    cpu_seconds += pct / 100.0 * (now_mono - last)
                    last = now_mono

            stats_task = asyncio.create_task(sample_stats())
            while True:
                try:
                    await asyncio.wait_for(asyncio.shield(wait_task), timeout=0.25)
                    break
                except asyncio.TimeoutError:
                    pass
                if cancel_event.is_set() and not cancel_flag:
                    cancel_flag = True
                    await self._terminate(task_container)
                if mono() > deadline and not timed_out and not cancel_flag:
                    timed_out = True
                    await self._terminate(task_container)
            await asyncio.gather(*readers, return_exceptions=True)
            stats_task.cancel()
            await asyncio.gather(stats_task, return_exceptions=True)

            code, out, _ = await self._run(
                [self.docker, "inspect", "--format", "{{json .State}}", task_container],
                check=False,
            )
            if code == 0 and out.strip():
                try:
                    state = json.loads(out.strip())
                    exit_code = int(state.get("ExitCode", 0))
                    oom = bool(state.get("OOMKilled", False))
                except (json.JSONDecodeError, TypeError, ValueError):
                    exit_code = proc.returncode
            else:
                exit_code = proc.returncode

            if sidecar is not None:
                egress_audit = await self._egress_audit(proxy_name)
                if egress_audit:
                    logs.extend(
                        (
                            "\n[egress-audit] "
                            + json.dumps(
                                {
                                    "allowed": egress_audit.get("allowed"),
                                    "denied": egress_audit.get("denied"),
                                    "denials": egress_audit.get("denials", [])[:5],
                                }
                            )
                            + "\n"
                        ).encode()
                    )

            if cancel_flag:
                outcome, error = "canceled", "canceled by control plane"
            elif timed_out:
                outcome, error = "timeout", f"sandbox exceeded {req.timeout_s}s timeout"
            elif oom:
                outcome, error = "failed", "container was OOM-killed"
            elif exit_code == 0:
                outcome = "succeeded"
            else:
                outcome, error = "failed", f"agent exited with code {exit_code}"
        except Exception as exc:
            outcome = "failed"
            error = f"docker sandbox error: {exc}"
            logs.extend(f"\n[docker-backend] {exc}\n".encode())
        finally:
            await self._run([self.docker, "rm", "-f", task_container], check=False)
            if sidecar is not None:
                await self._run([self.docker, "rm", "-f", proxy_name], check=False)
                await self._run([self.docker, "network", "rm", network], check=False)

        duration = round(mono() - started, 3)
        text = logs.decode("utf-8", "replace")
        result: dict[str, Any] = {
            "sandbox_key": req.sandbox_key,
            "network_mode": req.network_mode,
            "egress": egress_audit,
            "isolated_workspace": req.workspace,
        }
        return RunResult(
            outcome=outcome,
            exit_code=exit_code,
            logs=text,
            result=result,
            error=error,
            peak_memory_mb=int(peak_memory) if peak_memory else None,
            cpu_seconds=round(cpu_seconds, 2),
            duration_s=duration,
            backend="docker",
            sandbox_id=req.sandbox_key,
        )

    async def _wait_proxy(self, name: str, deadline: float) -> None:
        while mono() < deadline and mono() < deadline:
            code, out, _ = await self._run([self.docker, "logs", "--tail", "50", name], check=False)
            if code == 0 and "listening" in out:
                return
            code, state, _ = await self._run(
                [self.docker, "inspect", "--format", "{{.State.Running}}", name], check=False
            )
            if code == 0 and state.strip() == "false":
                raise RuntimeError("egress proxy exited during startup")
            await asyncio.sleep(0.2)
        raise RuntimeError("egress proxy did not become ready")

    async def _egress_audit(self, name: str) -> dict[str, Any] | None:
        code, out, _ = await self._run([self.docker, "logs", name], check=False)
        if code != 0:
            return None
        allowed = denied = 0
        denials: list[dict[str, Any]] = []
        for line in out.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            if event.get("kind") == "allowed":
                allowed += 1
            elif event.get("kind") == "denied":
                denied += 1
                if len(denials) < 20:
                    denials.append(
                        {
                            "host": event.get("host"),
                            "reason": event.get("reason"),
                            "method": event.get("method"),
                        }
                    )
        return {"allowed": allowed, "denied": denied, "denials": denials}

    async def _terminate(self, name: str) -> None:
        await self._run([self.docker, "kill", "--signal", "SIGTERM", name], check=False)
        for _ in range(10):
            await asyncio.sleep(0.5)
            code, out, _ = await self._run(
                [self.docker, "inspect", "--format", "{{.State.Running}}", name], check=False
            )
            if code != 0 or out.strip() != "true":
                return
        await self._run([self.docker, "kill", "--signal", "SIGKILL", name], check=False)
