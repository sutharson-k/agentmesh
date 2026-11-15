from __future__ import annotations

import logging

from ...config import Config
from .base import BackendUnavailable, CommandError, RunResult, SandboxBackend, SandboxRequest
from .docker import DockerBackend
from .fake import FakeBackend
from .firecracker import FirecrackerBackend

log = logging.getLogger("agentmesh.worker.backends")


def create_backend(cfg: Config, worker_id: str, *, allow_fake_fallback: bool = False) -> SandboxBackend:
    choice = cfg.worker.backend
    if choice == "fake":
        return FakeBackend()
    if choice == "docker":
        return DockerBackend(cfg.worker.docker, worker_id=worker_id, workdir=cfg.worker.workdir)
    if choice == "firecracker":
        return FirecrackerBackend(
            cfg.worker.firecracker, worker_id=worker_id, workdir=cfg.worker.workdir
        )
    if choice == "auto":
        docker = DockerBackend(cfg.worker.docker, worker_id=worker_id, workdir=cfg.worker.workdir)
        try:
            import asyncio

            asyncio.get_event_loop()
        except RuntimeError:
            pass
        # Actual availability is determined in prepare(); the daemon retries
        # the fake backend only when explicitly permitted.
        return _AutoBackend(docker, cfg, worker_id, allow_fake_fallback)
    raise BackendUnavailable(f"unknown backend {choice!r}")


class _AutoBackend(SandboxBackend):
    name = "auto"

    def __init__(self, docker: DockerBackend, cfg: Config, worker_id: str, allow_fake: bool) -> None:
        self.docker = docker
        self.cfg = cfg
        self.worker_id = worker_id
        self.allow_fake = allow_fake
        self.inner: SandboxBackend | None = None

    async def prepare(self) -> None:
        try:
            await self.docker.prepare()
        except BackendUnavailable as exc:
            if not self.allow_fake:
                raise BackendUnavailable(
                    f"{exc}. Set worker.backend=fake explicitly to run without isolation "
                    "(development only)."
                ) from exc
            log.warning("docker unavailable, falling back to fake backend: %s", exc)
            self.inner = FakeBackend()
            await self.inner.prepare()
            self.name = "fake"
        else:
            self.inner = self.docker
            self.name = "docker"

    async def run(self, req: SandboxRequest, cancel_event) -> RunResult:
        assert self.inner is not None, "prepare() must be called first"
        return await self.inner.run(req, cancel_event)

    async def close(self) -> None:
        if self.inner:
            await self.inner.close()

    def info(self) -> dict:
        return {"backend": self.name, **(self.inner.info() if self.inner else {})}


__all__ = [
    "BackendUnavailable",
    "CommandError",
    "DockerBackend",
    "FakeBackend",
    "FirecrackerBackend",
    "RunResult",
    "SandboxBackend",
    "SandboxRequest",
    "create_backend",
]
