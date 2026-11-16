from __future__ import annotations

from pathlib import Path

from agentmesh.config import Config


def test_example_yaml_loads_nested_sections(tmp_path, monkeypatch):
    """worker.docker / worker.firecracker must merge into their dataclasses."""
    root = Path(__file__).resolve().parents[1]
    cfg = Config.load(str(root / "agentmesh.example.yaml"))
    assert cfg.worker.backend == "auto"
    assert cfg.worker.docker.proxy_port == 3128
    assert cfg.worker.docker.allowed_images
    assert cfg.worker.firecracker.enabled is False
    assert cfg.worker.firecracker.guest_agent_port == 52000
    assert cfg.server.port == 8080


def test_nested_sections_override_without_replacing(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "worker:\n"
        "  slots: 3\n"
        "  docker:\n"
        "    proxy_port: 4444\n"
        "    allowed_images: [ubuntu:24.04]\n",
        encoding="utf-8",
    )
    cfg = Config.load(str(path))
    assert cfg.worker.slots == 3
    assert cfg.worker.docker.proxy_port == 4444
    assert cfg.worker.docker.proxy_image == "python:3.12-alpine"
    assert cfg.worker.docker.allowed_images == ["ubuntu:24.04"]
    assert cfg.worker.firecracker.guest_agent_port == 52000
