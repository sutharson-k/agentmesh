"""Configuration loading and validation for the control plane and workers."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any

import yaml


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    if value is None:
        return default
    return bool(value)


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    db_path: str = "var/agentmesh.db"
    api_keys: list[str] = field(default_factory=list)
    lease_ttl_s: int = 30
    heartbeat_timeout_s: int = 45
    maintenance_interval_s: float = 2.0
    max_global_running: int = 512
    tenant_max_concurrent: int = 64
    log_level: str = "INFO"
    json_logs: bool = True
    max_task_timeout_s: int = 86_400
    max_submissions_per_minute: int = 600

    @property
    def auth_enabled(self) -> bool:
        return bool(self.api_keys)


@dataclass
class DockerConfig:
    # This image is used only for the egress proxy sidecar, never for task containers.
    proxy_image: str = "python:3.12-alpine"
    proxy_port: int = 3128
    egress_network: str = "agentmesh-egress"
    uplink_network: str = "agentmesh-uplink"
    egress_token: str = ""
    allowed_images: list[str] = field(
        default_factory=lambda: [
            "python:3.12-slim",
            "python:3.11-slim",
            "python:3.12",
            "node:22-slim",
            "node:20-slim",
            "ubuntu:24.04",
            "debian:12-slim",
            "alpine:3.20",
            "golang:1.23",
            "rust:1.82",
        ]
    )
    run_as_user: str = "10001:10001"
    shm_size_mb: int = 64
    pull_images: bool = False
    seccomp_profile: str | None = None


@dataclass
class FirecrackerConfig:
    enabled: bool = False
    firecracker_bin: str = "/usr/local/bin/firecracker"
    jailer_bin: str = "/usr/local/bin/jailer"
    kernel_image: str = "/var/lib/agentmesh/vmlinux"
    rootfs_image: str = "/var/lib/agentmesh/rootfs.ext4"
    run_root: str = "/srv/agentmesh/firecracker"
    vcpu: int = 2
    memory_mb: int = 1024
    tap_prefix: str = "fc"
    boot_args: str = "console=ttyS0 noapic reboot=k panic=1 pci=off nomodules quiet"
    guest_agent_port: int = 52_000


@dataclass
class WorkerConfig:
    control_url: str = "http://127.0.0.1:8080"
    token: str = ""
    worker_id: str = ""
    pool: str = "default"
    slots: int = 8
    cpu_total: float = 8.0
    memory_mb_total: int = 8192
    capabilities: list[str] = field(default_factory=list)
    labels: dict[str, str] = field(default_factory=dict)
    backend: str = "auto"
    workdir: str = "var/workers"
    heartbeat_interval_s: float = 5.0
    lease_wait_s: float = 5.0
    log_tail_bytes: int = 16_384
    docker: DockerConfig = field(default_factory=DockerConfig)
    firecracker: FirecrackerConfig = field(default_factory=FirecrackerConfig)


@dataclass
class AgentConfig:
    """argv templates for external agent CLIs. Placeholders: {prompt}, {prompt_file}, {model}."""

    commands: dict[str, list[str]] = field(
        default_factory=lambda: {
            "claude": ["claude", "-p", "{prompt}", "--output-format", "text"],
            "codex": ["codex", "exec", "--full-auto", "{prompt}"],
            "opencode": ["opencode", "run", "{prompt}"],
        }
    )


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    worker: WorkerConfig = field(default_factory=WorkerConfig)
    agents: AgentConfig = field(default_factory=AgentConfig)
    source: str | None = None

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        cfg = cls()
        candidates: list[Path] = []
        if path:
            candidates.append(Path(path))
        elif os.environ.get("AGENTMESH_CONFIG"):
            candidates.append(Path(os.environ["AGENTMESH_CONFIG"]))
        else:
            candidates.extend([Path("agentmesh.yaml"), Path("config/agentmesh.yaml")])
        for candidate in candidates:
            if candidate.exists():
                data = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
                if not isinstance(data, dict):
                    raise ValueError(f"{candidate}: top level must be a mapping")
                cfg._apply(data)
                cfg.source = str(candidate)
                break
        cfg._apply_env()
        cfg.validate()
        return cfg

    def _apply(self, data: dict[str, Any]) -> None:
        self._apply_section(self.server, data.get("server"), "server")
        self._apply_section(self.worker, data.get("worker"), "worker")
        agents = data.get("agents")
        if isinstance(agents, dict):
            commands = agents.get("commands")
            if isinstance(commands, dict):
                parsed: dict[str, list[str]] = {}
                for kind, argv in commands.items():
                    if isinstance(argv, list) and all(isinstance(x, str) for x in argv):
                        parsed[str(kind)] = list(argv)
                self.agents.commands.update(parsed)

    @staticmethod
    def _apply_section(target: Any, data: Any, name: str) -> None:
        if data is None:
            return
        if not isinstance(data, dict):
            raise ValueError(f"{name}: expected a mapping")
        known = {f.name: f for f in fields(target)}
        for key, value in data.items():
            if key not in known:
                continue
            current = getattr(target, key)
            if is_dataclass(current) and not isinstance(current, type):
                # Nested sections (worker.docker, worker.firecracker) must be
                # merged recursively; assigning the raw dict would replace the
                # dataclass instance and break later validation.
                Config._apply_section(current, value, f"{name}.{key}")
            elif isinstance(current, bool):
                setattr(target, key, _as_bool(value, current))
            elif isinstance(current, int) and not isinstance(current, bool):
                setattr(target, key, int(value))
            elif isinstance(current, float):
                setattr(target, key, float(value))
            elif isinstance(current, list) and value is not None and not isinstance(value, list):
                setattr(target, key, [str(value)])
            elif isinstance(current, dict) and isinstance(value, dict):
                setattr(target, key, {str(k): str(v) for k, v in value.items()})
            else:
                setattr(target, key, value)

    def _apply_env(self) -> None:
        env = os.environ
        s, w = self.server, self.worker
        if v := env.get("AGENTMESH_HOST"):
            s.host = v
        if v := env.get("AGENTMESH_PORT"):
            s.port = int(v)
        if v := env.get("AGENTMESH_DB_PATH"):
            s.db_path = v
        if v := env.get("AGENTMESH_API_KEYS"):
            s.api_keys = [x.strip() for x in v.split(",") if x.strip()]
        if v := env.get("AGENTMESH_LOG_LEVEL"):
            s.log_level = v
        if v := env.get("AGENTMESH_CONTROL_URL"):
            w.control_url = v
        if v := env.get("AGENTMESH_TOKEN"):
            w.token = v
        if v := env.get("AGENTMESH_WORKER_ID"):
            w.worker_id = v
        if v := env.get("AGENTMESH_BACKEND"):
            w.backend = v
        if v := env.get("AGENTMESH_WORKDIR"):
            w.workdir = v
        if v := env.get("AGENTMESH_SLOTS"):
            w.slots = int(v)
        if v := env.get("AGENTMESH_EGRESS_TOKEN"):
            w.docker.egress_token = v

    def validate(self) -> None:
        s, w = self.server, self.worker
        if not 0 < s.port < 65_536:
            raise ValueError("server.port must be 1..65535")
        if s.lease_ttl_s < 5:
            raise ValueError("server.lease_ttl_s must be >= 5")
        if s.heartbeat_timeout_s <= s.lease_ttl_s:
            raise ValueError("server.heartbeat_timeout_s must exceed lease_ttl_s")
        if w.slots < 1 or w.slots > 4096:
            raise ValueError("worker.slots must be 1..4096")
        if w.backend not in {"auto", "docker", "firecracker", "fake"}:
            raise ValueError("worker.backend must be auto|docker|firecracker|fake")
        if w.cpu_total <= 0 or w.memory_mb_total <= 0:
            raise ValueError("worker capacity must be positive")
        if w.heartbeat_interval_s <= 0 or w.lease_wait_s < 0:
            raise ValueError("invalid worker timing configuration")
        if not s.api_keys:
            # Open mode is allowed for local development, but is never silent.
            pass


DEFAULT_CONFIG_YAML = """\
server:
  host: 127.0.0.1
  port: 8080
  db_path: var/agentmesh.db
  api_keys: []          # empty = open mode (development only); use bearer tokens in production
  lease_ttl_s: 30
  heartbeat_timeout_s: 45
  max_global_running: 512
  tenant_max_concurrent: 64

worker:
  control_url: http://127.0.0.1:8080
  pool: default
  slots: 8
  cpu_total: 8
  memory_mb_total: 8192
  backend: auto          # auto | docker | firecracker | fake
  workdir: var/workers
  docker:
    allowed_images:
      - python:3.12-slim
      - node:22-slim
      - ubuntu:24.04
"""
