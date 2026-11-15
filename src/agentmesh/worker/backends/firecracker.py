"""Firecracker microVM backend (Linux/KVM).

Each agent run gets a fresh microVM: its own kernel boot, writable rootfs
copy, vCPU/memory allotment, vsock channel and (by default) no network device.
The guest must contain ``agentmesh.guest.agent`` listening on vsock port 52000;
see docs/FIRECRACKER.md. The VM is always destroyed after the run.

The Docker backend is the default and the fully exercised path in CI. This
adapter refuses to start (rather than silently degrading) when KVM, the
firecracker binary, or the guest images are unavailable.
"""

from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import shutil
import struct
import sys
import tarfile
from pathlib import Path
from typing import Any

from ...config import FirecrackerConfig
from ...util import mono
from .base import BackendUnavailable, RunResult, SandboxBackend, SandboxRequest

VSOCK_GUEST_CID = 3
ARCHIVE_LIMIT = 32 * 1024 * 1024
LOG_LIMIT = 2 * 1024 * 1024


def build_vm_config(
    *,
    kernel_image: str,
    rootfs_image: str,
    vcpu: int,
    memory_mb: int,
    uds_path: str,
    guest_cid: int = VSOCK_GUEST_CID,
    boot_args: str = "console=ttyS0 noapic reboot=k panic=1 pci=off nomodules quiet",
    log_path: str | None = None,
) -> dict[str, Any]:
    """Pure VM configuration builder, unit-tested independently of KVM."""
    if vcpu < 1 or vcpu > 64:
        raise ValueError("vcpu must be 1..64")
    if memory_mb < 128 or memory_mb > 131_072:
        raise ValueError("memory_mb must be 128..131072")
    if guest_cid < 3:
        raise ValueError("guest_cid must be >= 3")
    config: dict[str, Any] = {
        "boot-source": {
            "kernel_image_path": kernel_image,
            "boot_args": boot_args,
        },
        "drives": [
            {
                "drive_id": "rootfs",
                "path_on_host": rootfs_image,
                "is_root_device": True,
                "is_read_only": False,
            }
        ],
        "machine-config": {"vcpu_count": vcpu, "mem_size_mib": memory_mb},
        "vsock": {"guest_cid": guest_cid, "uds_path": uds_path},
    }
    if log_path:
        config["logger"] = {"log_path": log_path, "level": "Warn", "show_level": True}
    return config


def _frame(obj: dict[str, Any]) -> bytes:
    payload = json.dumps(obj).encode()
    return struct.pack(">I", len(payload)) + payload


async def _read_frame(reader: asyncio.StreamReader, timeout: float) -> dict[str, Any]:
    header = await asyncio.wait_for(reader.readexactly(4), timeout=timeout)
    (length,) = struct.unpack(">I", header)
    if length > ARCHIVE_LIMIT * 2:
        raise ValueError("guest frame too large")
    payload = await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
    return json.loads(payload)


def _unpack_workspace(raw: bytes, workspace: str) -> int:
    workspace_path = Path(workspace).resolve()
    workspace_path.mkdir(parents=True, exist_ok=True)
    files = 0
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        for member in tar.getmembers():
            target = (workspace_path / member.name).resolve()
            if not str(target).startswith(str(workspace_path) + os.sep) and target != workspace_path:
                continue
            if member.issym() or member.islnk():
                continue
            tar.extract(member, path=workspace_path, filter="data")
            files += 1
    return files


class FirecrackerBackend(SandboxBackend):
    name = "firecracker"

    def __init__(self, cfg: FirecrackerConfig, *, worker_id: str, workdir: str) -> None:
        self.cfg = cfg
        self.worker_id = worker_id
        self.workdir = Path(workdir)
        self.run_root = Path(cfg.run_root)
        self.version: str | None = None

    async def prepare(self) -> None:
        if sys.platform != "linux":
            raise BackendUnavailable("Firecracker requires Linux with KVM")
        if not os.path.exists("/dev/kvm"):
            raise BackendUnavailable("Firecracker requires /dev/kvm; enable nested virtualization")
        for path, label in [
            (self.cfg.firecracker_bin, "firecracker binary"),
            (self.cfg.kernel_image, "kernel image"),
            (self.cfg.rootfs_image, "rootfs image"),
        ]:
            if not Path(path).exists():
                raise BackendUnavailable(f"Firecracker {label} not found: {path}")
        proc = await asyncio.create_subprocess_exec(
            self.cfg.firecracker_bin,
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await proc.communicate()
        if proc.returncode != 0:
            raise BackendUnavailable("firecracker --version failed")
        self.version = out.decode("utf-8", "replace").strip().splitlines()[0]
        self.run_root.mkdir(parents=True, exist_ok=True)

    def info(self) -> dict[str, Any]:
        return {"backend": "firecracker", "version": self.version}

    async def run(self, req: SandboxRequest, cancel_event: asyncio.Event) -> RunResult:
        if req.network_mode != "none":
            return RunResult(
                outcome="failed",
                exit_code=None,
                logs="",
                error=(
                    "firecracker backend currently supports network_mode=none only; "
                    "use a Docker worker for allowlisted egress"
                ),
                backend="firecracker",
                sandbox_id=req.sandbox_key,
            )
        started = mono()
        deadline = started + req.timeout_s
        run_dir = self.run_root / f"{self.worker_id[:8]}-{req.run_id}"
        rootfs = run_dir / "rootfs.ext4"
        uds = run_dir / "vsock.sock"
        config_path = run_dir / "vm-config.json"
        logs = bytearray()
        outcome = "failed"
        exit_code: int | None = None
        error: str | None = None
        files = 0
        proc: asyncio.subprocess.Process | None = None
        writer: asyncio.StreamWriter | None = None
        try:
            run_dir.mkdir(parents=True, exist_ok=False)
            # Copy-on-write where the filesystem supports it. Never boot the
            # shared base image directly: each agent needs its own disk.
            code, _, err = await self._cp(self.cfg.rootfs_image, str(rootfs))
            if code != 0:
                raise RuntimeError(f"rootfs copy failed: {err}")
            config = build_vm_config(
                kernel_image=self.cfg.kernel_image,
                rootfs_image=str(rootfs),
                vcpu=self.cfg.vcpu,
                memory_mb=self.cfg.memory_mb,
                uds_path=str(uds),
                boot_args=self.cfg.boot_args,
                log_path=str(run_dir / "firecracker.log"),
            )
            config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            proc = await asyncio.create_subprocess_exec(
                self.cfg.firecracker_bin,
                "--api-sock",
                str(run_dir / "api.sock"),
                "--config-file",
                str(config_path),
                cwd=str(run_dir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                start_new_session=True,
            )
            reader, writer = await self._connect_guest(uds, deadline, proc)
            writer.write(
                _frame(
                    {
                        "type": "run",
                        "argv": req.argv,
                        "env": {
                            **req.env,
                            "AGENTMESH_TASK_ID": req.task_id,
                            "HOME": "/root",
                        },
                        "cwd": "/workspace",
                        "timeout_s": req.timeout_s,
                        "archive": True,
                    }
                )
            )
            await writer.drain()
            while True:
                if cancel_event.is_set():
                    outcome, error = "canceled", "canceled by control plane"
                    break
                remaining = deadline - mono()
                if remaining <= 0:
                    outcome, error = "timeout", f"microVM exceeded {req.timeout_s}s timeout"
                    break
                try:
                    frame = await _read_frame(reader, timeout=min(1.0, max(remaining, 0.05)))
                except asyncio.TimeoutError:
                    continue
                kind = frame.get("type")
                if kind == "stdout":
                    logs.extend(base64.b64decode(frame.get("data", "")))
                    if len(logs) > LOG_LIMIT:
                        del logs[: len(logs) - LOG_LIMIT]
                elif kind == "stderr":
                    logs.extend(base64.b64decode(frame.get("data", "")))
                    if len(logs) > LOG_LIMIT:
                        del logs[: len(logs) - LOG_LIMIT]
                elif kind == "archive":
                    raw = base64.b64decode(frame.get("data", ""))
                    if len(raw) > ARCHIVE_LIMIT:
                        raise RuntimeError("guest workspace archive exceeds limit")
                    files = _unpack_workspace(raw, req.workspace)
                elif kind == "exit":
                    exit_code = int(frame.get("code", 1))
                    outcome = "succeeded" if exit_code == 0 else "failed"
                    if exit_code != 0:
                        error = f"agent exited with code {exit_code}"
                elif kind == "error":
                    outcome, error = "failed", f"guest agent error: {frame.get('message')}"
                    break
                if kind == "exit":
                    break
        except Exception as exc:
            outcome = "failed"
            error = f"firecracker error: {exc}"
            logs.extend(f"\n[firecracker-backend] {exc}\n".encode())
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            if proc is not None and proc.returncode is None:
                proc.kill()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass
            # Destroy the per-run disk and runtime directory. No VM state is
            # ever reused by another agent.
            shutil.rmtree(run_dir, ignore_errors=True)

        return RunResult(
            outcome=outcome,
            exit_code=exit_code,
            logs=logs.decode("utf-8", "replace"),
            result={
                "sandbox_key": req.sandbox_key,
                "backend": "firecracker",
                "vm_rootfs": "ephemeral",
                "workspace_files": files,
                "network_mode": "none",
            },
            error=error,
            peak_memory_mb=None,
            cpu_seconds=None,
            duration_s=round(mono() - started, 3),
            backend="firecracker",
            sandbox_id=req.sandbox_key,
        )

    async def _cp(self, src: str, dst: str) -> tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_exec(
            "cp", "--reflink=auto", "--sparse=always", src, dst,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        return proc.returncode or 0, out, err

    async def _connect_guest(
        self,
        uds: str,
        deadline: float,
        proc: asyncio.subprocess.Process,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        last: Exception | None = None
        while mono() < deadline and mono() < deadline:
            if proc.returncode is not None:
                raise RuntimeError(f"firecracker exited with {proc.returncode}")
            try:
                reader, writer = await asyncio.open_unix_connection(uds, limit=2**20)
                writer.write(f"CONNECT {self.cfg.guest_agent_port}\n".encode())
                await writer.drain()
                handshake = await asyncio.wait_for(reader.readline(), timeout=5)
                if handshake:
                    return reader, writer
                writer.close()
            except (OSError, asyncio.TimeoutError, ConnectionError) as exc:
                last = exc
            await asyncio.sleep(0.15)
        raise RuntimeError(f"guest agent did not become ready: {last}")

    async def close(self) -> None:
        return None
