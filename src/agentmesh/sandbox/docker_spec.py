"""Hardened Docker container and network specifications.

The argument builders are pure functions so they can be unit-tested without a
Docker daemon. Every task container gets its own network namespace; in
allowlist mode it is attached to a per-task, internal-only network containing
exactly one other container: a per-task egress proxy. The proxy is the only
path off the host.
"""

from __future__ import annotations

import fnmatch
import ipaddress
import re
import secrets
from dataclasses import dataclass, field
from pathlib import Path

DOCKER_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")
DOMAIN = re.compile(r"^(?:\*\.)?[a-z0-9](?:[a-z0-9.-]{0,251})$")
NETWORK_MODE = {"none", "egress_allowlist"}


class SandboxSpecError(ValueError):
    pass


def validate_docker_name(name: str) -> str:
    if not DOCKER_NAME_RE.match(name):
        raise SandboxSpecError(f"invalid docker name: {name!r}")
    return name


def image_allowed(image: str, allowlist: list[str]) -> bool:
    """Allowlist entries are glob patterns matched against the full reference."""
    return any(fnmatch.fnmatchcase(image, pattern) for pattern in allowlist)


def docker_path(path: str | Path) -> str:
    return str(path).replace("\\", "/")


def short_id(task_id: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9]", "", task_id)
    return clean[-18:] or secrets.token_hex(6)


@dataclass
class EgressSidecar:
    name: str
    network: str
    uplink_network: str
    allow: list[str]
    token: str
    image: str = "python:3.12-alpine"
    port: int = 3128
    script_path: str = "/opt/agentmesh/egress_proxy.py"
    host_script: str = ""
    memory_mb: int = 96
    pids: int = 64


@dataclass
class DockerSandboxSpec:
    task_id: str
    image: str
    argv: list[str]
    env: dict[str, str] = field(default_factory=dict)
    cpu: float = 1.0
    memory_mb: int = 512
    pids: int = 256
    network_mode: str = "none"
    allow: list[str] = field(default_factory=list)
    workspace_dir: str = ""
    allowed_images: list[str] = field(default_factory=list)
    run_as_user: str = "10001:10001"
    shm_size_mb: int = 64
    seccomp_profile: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    egress: EgressSidecar | None = None
    pull: bool = False
    # Per-run identity. The worker generates a fresh nonce for every task
    # attempt, guaranteeing a fresh container, network and workspace.
    attempt: int = 1
    nonce: str = ""


def container_name(task_id: str, attempt: int = 1, nonce: str = "") -> str:
    """Unique name for one sandbox. Includes a per-run nonce so a retry or a
    crashed worker can never collide with an older sandbox container."""
    suffix = f"-{nonce}" if nonce else ""
    return validate_docker_name(f"mesh-task-{short_id(task_id)}-{attempt}{suffix}")


def egress_network_name(task_id: str, nonce: str = "") -> str:
    suffix = f"-{nonce}" if nonce else ""
    return validate_docker_name(f"mesh-egress-{short_id(task_id)}{suffix}")


def build_create_argv(spec: DockerSandboxSpec, docker: str = "docker") -> list[str]:
    """Build ``docker create`` argv with defense-in-depth isolation flags."""
    if not spec.workspace_dir:
        raise SandboxSpecError("workspace_dir is required")
    if spec.network_mode not in NETWORK_MODE:
        raise SandboxSpecError(f"unsupported network mode {spec.network_mode!r}")
    if spec.network_mode == "egress_allowlist" and spec.egress is None:
        raise SandboxSpecError("egress_allowlist mode requires a proxy sidecar")
    if not spec.argv:
        raise SandboxSpecError("argv must not be empty")
    if spec.allowed_images and not image_allowed(spec.image, spec.allowed_images):
        raise SandboxSpecError(f"image {spec.image!r} is not in the worker allowlist")

    name = container_name(spec.task_id, spec.attempt, spec.nonce)
    args = [
        docker,
        "create",
        "--name",
        name,
        "--label",
        "agentmesh.managed=true",
        "--label",
        f"agentmesh.task_id={spec.task_id}",
    ]
    for key, value in spec.labels.items():
        if not re.match(r"^[a-zA-Z0-9_.-]{1,60}$", key):
            raise SandboxSpecError(f"invalid label key {key!r}")
        args += ["--label", f"agentmesh.user.{key}={value[:200]}"]

    # --- identity & filesystem ------------------------------------------------
    args += ["--user", spec.run_as_user]
    args += ["--read-only"]
    args += [
        "--mount",
        f"type=bind,source={docker_path(spec.workspace_dir)},target=/workspace",
    ]
    args += ["--workdir", "/workspace"]
    args += ["--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m"]
    args += ["--tmpfs", "/run:rw,noexec,nosuid,nodev,size=16m"]
    args += ["--shm-size", f"{max(16, spec.shm_size_mb)}m"]
    args += ["--init"]

    # --- kernel / privilege hardening ----------------------------------------
    args += ["--cap-drop", "ALL"]
    args += ["--security-opt", "no-new-privileges:true"]
    if spec.seccomp_profile:
        profile = Path(spec.seccomp_profile)
        if not profile.is_file():
            raise SandboxSpecError(f"seccomp profile not found: {profile}")
        args += ["--security-opt", f"seccomp={docker_path(profile)}"]
    args += ["--pids-limit", str(max(16, spec.pids))]
    args += ["--ulimit", "nofile=2048:2048"]
    args += ["--ulimit", "core=0"]

    # --- resource limits (cgroups) -------------------------------------------
    args += ["--cpus", f"{spec.cpu:.3f}".rstrip("0").rstrip(".")]
    args += ["--memory", f"{spec.memory_mb}m"]
    args += ["--memory-swap", f"{spec.memory_mb}m"]  # swap disabled
    args += ["--oom-kill-disable=false"]

    # --- network --------------------------------------------------------------
    if spec.network_mode == "none":
        args += ["--network", "none"]
    else:
        assert spec.egress is not None
        args += ["--network", spec.egress.network]
        proxy_url = f"http://agentmesh:{spec.egress.token}@egress:{spec.egress.port}"
        for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            args += ["--env", f"{key}={proxy_url}"]
        args += ["--env", "NO_PROXY=localhost,127.0.0.1"]
        args += ["--env", "no_proxy=localhost,127.0.0.1"]

    args += ["--hostname", f"task-{short_id(spec.task_id)[:12]}"]
    args += ["--stop-signal", "SIGTERM", "--stop-timeout", "5"]
    args += ["--log-driver", "none"]
    args += ["--pull", "missing" if spec.pull else "never"]

    for key, value in spec.env.items():
        if "\x00" in key or "\x00" in value:
            raise SandboxSpecError("environment contains NUL bytes")
        args += ["--env", f"{key}={value}"]

    args.append(spec.image)
    args.extend(spec.argv)
    return args


def build_network_create_argv(network: str, *, internal: bool, docker: str = "docker") -> list[str]:
    args = [docker, "network", "create", "--label", "agentmesh.managed=true"]
    if internal:
        args.append("--internal")
    args.append(validate_docker_name(network))
    return args


def build_sidecar_argv(spec: EgressSidecar, docker: str = "docker") -> list[str]:
    name = validate_docker_name(spec.name)
    args = [
        docker,
        "run",
        "-d",
        "--name",
        name,
        "--label",
        "agentmesh.managed=true",
        "--label",
        "agentmesh.role=egress-proxy",
        "--network",
        spec.uplink_network,
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--user",
        "10001:10001",
        "--pids-limit",
        str(spec.pids),
        "--memory",
        f"{spec.memory_mb}m",
        "--memory-swap",
        f"{spec.memory_mb}m",
        "--cpus",
        "0.25",
        "--tmpfs",
        "/tmp:rw,noexec,nosuid,nodev,size=8m",
        "--mount",
        f"type=bind,source={docker_path(spec.host_script)},target={spec.script_path},readonly",
        "--env",
        f"AGENTMESH_EGRESS_ALLOW={','.join(spec.allow)}",
        "--env",
        f"AGENTMESH_EGRESS_TOKEN={spec.token}",
        "--env",
        f"AGENTMESH_EGRESS_PORT={spec.port}",
        "--log-driver",
        "json-file",
        "--log-opt",
        "max-size=1m",
        "--log-opt",
        "max-file=1",
        spec.image,
        "python",
        "-u",
        spec.script_path,
    ]
    return args


def validate_workspace(path: str | Path) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_dir():
        raise SandboxSpecError(f"workspace does not exist: {resolved}")
    return resolved


def check_allowlist(allow: list[str]) -> list[str]:
    out = []
    for raw in allow:
        domain = raw.strip().lower().rstrip(".")
        if not domain or not DOMAIN.match(domain):
            raise SandboxSpecError(f"invalid egress domain {raw!r}")
        out.append(domain)
    return sorted(set(out))


def is_public_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )
