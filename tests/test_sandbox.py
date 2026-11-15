from __future__ import annotations

import asyncio
import base64
import sys
from pathlib import Path

import pytest

from agentmesh.config import DockerConfig
from agentmesh.sandbox.docker_spec import (
    DockerSandboxSpec,
    EgressSidecar,
    SandboxSpecError,
    build_create_argv,
    build_network_create_argv,
    build_sidecar_argv,
    check_allowlist,
    container_name,
    egress_network_name,
    image_allowed,
    is_public_ip,
)
from agentmesh.sandbox.egress_proxy import Policy, resolve_public
from agentmesh.worker.backends.docker import parse_mem
from agentmesh.worker.backends.firecracker import build_vm_config

import agentmesh.sandbox.egress_proxy as proxy_module


def spec(tmp_path: Path, **overrides) -> DockerSandboxSpec:
    (tmp_path / "ws").mkdir(exist_ok=True)
    data = dict(
        task_id="task_abc123",
        image="python:3.12-slim",
        argv=["python", "-c", "print(1)"],
        env={"FOO": "bar"},
        workspace_dir=str(tmp_path / "ws"),
        allowed_images=["python:3.12-slim"],
    )
    data.update(overrides)
    return DockerSandboxSpec(**data)


def test_container_is_hardened(tmp_path):
    args = build_create_argv(spec(tmp_path))
    joined = " ".join(args)
    assert "--cap-drop ALL" in joined
    assert "no-new-privileges:true" in joined
    assert "--read-only" in joined
    assert "--pids-limit 256" in joined
    assert "--memory 512m" in joined
    assert "--memory-swap 512m" in joined
    assert "--network none" in joined
    assert "--user 10001:10001" in joined
    assert "--init" in joined
    assert "--privileged" not in args
    assert "/var/run/docker.sock" not in joined
    assert "/workspace" in joined
    assert "--tmpfs" in args
    assert "--ulimit" in joined
    # No host network / pid / ipc sharing, no device passthrough.
    assert "--network host" not in joined
    assert "--pid host" not in joined
    assert "--ipc host" not in joined
    assert "--device" not in args
    assert args[-2:] == ["-c", "print(1)"]


def test_read_only_root_and_private_workspace(tmp_path):
    args = build_create_argv(spec(tmp_path))
    mounts = [args[i + 1] for i, a in enumerate(args) if a == "--mount"]
    assert len(mounts) == 1
    assert "target=/workspace" in mounts[0]
    assert str(tmp_path / "ws").replace("\\", "/") in mounts[0]


def test_image_allowlist_enforced(tmp_path):
    with pytest.raises(SandboxSpecError):
        build_create_argv(spec(tmp_path, image="evil/image:latest"))
    assert image_allowed("python:3.12-slim", ["python:*"])
    assert not image_allowed("python:3.12-slim", ["node:*"])


def test_egress_mode_creates_isolated_network_and_proxy(tmp_path):
    sidecar = EgressSidecar(
        name="mesh-egress-test",
        network="mesh-egress-net",
        uplink_network="agentmesh-uplink",
        allow=["api.github.com"],
        token="tok",
        host_script=str(tmp_path / "proxy.py"),
    )
    args = build_create_argv(spec(tmp_path, network_mode="egress_allowlist", egress=sidecar))
    joined = " ".join(args)
    assert "--network mesh-egress-net" in joined
    assert "HTTP_PROXY=http://agentmesh:tok@egress:3128" in joined
    assert "HTTPS_PROXY=" in joined
    sidecar_args = build_sidecar_argv(sidecar)
    joined_side = " ".join(sidecar_args)
    assert "--cap-drop ALL" in joined_side
    assert "--read-only" in joined_side
    assert "--network agentmesh-uplink" in joined_side
    net_args = build_network_create_argv("mesh-egress-net", internal=True)
    assert "--internal" in net_args


def test_missing_sidecar_rejected(tmp_path):
    with pytest.raises(SandboxSpecError):
        build_create_argv(spec(tmp_path, network_mode="egress_allowlist"))


def test_each_run_has_unique_container_and_network():
    names = {
        container_name("task_same", 1, f"run{i}") for i in range(25)
    }
    assert len(names) == 25
    assert container_name("task_same", 1, "a") != container_name("task_same", 2, "a")
    nets = {egress_network_name("task_same", f"n{i}") for i in range(25)}
    assert len(nets) == 25


def test_parse_mem_units():
    assert parse_mem("12.5MiB") == pytest.approx(12.5)
    assert parse_mem("1GiB") == pytest.approx(1024)
    assert round(parse_mem("512KiB"), 2) == 0.5


def test_allowlist_validation():
    assert check_allowlist(["API.gitHub.com", "*.npmjs.org"]) == ["*.npmjs.org", "api.github.com"]
    with pytest.raises(SandboxSpecError):
        check_allowlist(["http://evil.com"])


def test_proxy_policy_semantics():
    policy = Policy(["api.github.com", "*.pypi.org"], "secret")
    assert policy.allows_domain("api.github.com")
    assert policy.allows_domain("pypi.org")
    assert policy.allows_domain("files.pypi.org")
    assert not policy.allows_domain("evilippypi.org")
    assert not policy.allows_domain("127.0.0.1")
    assert not policy.allows_domain("169.254.169.254")
    assert not policy.allows_port(22)
    assert policy.allows_port(443)
    auth = "Basic " + base64.b64encode(b"agentmesh:secret").decode()
    assert policy.authorized(auth)
    assert not policy.authorized("Basic " + base64.b64encode(b"agentmesh:wrong").decode())
    assert not policy.authorized(None)


def test_private_ip_detection():
    assert is_public_ip("8.8.8.8")
    for addr in ("127.0.0.1", "10.0.0.5", "192.168.1.10", "169.254.169.254", "::1"):
        assert not is_public_ip(addr)


def test_proxy_blocks_localhost_resolution():
    async def go():
        with pytest.raises(PermissionError):
            await resolve_public("127.0.0.1", 80)

    asyncio.run(go())


def test_no_allowed_egress_means_deny_all():
    policy = Policy([], "tok")
    assert not policy.allows_domain("example.com")


def test_firecracker_vm_config_is_isolated_and_limited(tmp_path):
    cfg = build_vm_config(
        kernel_image="/k/vmlinux",
        rootfs_image=str(tmp_path / "rootfs.ext4"),
        vcpu=2,
        memory_mb=1024,
        uds_path=str(tmp_path / "vsock.sock"),
        log_path=str(tmp_path / "fc.log"),
    )
    assert cfg["machine-config"]["vcpu_count"] == 2
    assert cfg["machine-config"]["mem_size_mib"] == 1024
    assert cfg["drives"][0]["is_root_device"] is True
    assert "network-interfaces" not in cfg
    assert cfg["vsock"]["guest_cid"] == 3
    assert cfg["logger"]["level"] == "Warn"
    with pytest.raises(ValueError):
        build_vm_config(
            kernel_image="/k", rootfs_image="/r", vcpu=0, memory_mb=64, uds_path="/s"
        )


def test_default_docker_config_has_no_wide_allowlist():
    cfg = DockerConfig()
    assert "python:3.12-slim" in cfg.allowed_images
    assert "*" not in cfg.allowed_images
