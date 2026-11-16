from __future__ import annotations

from pathlib import Path

import pytest

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


def test_secrets_resolve_from_env_and_file(tmp_path, monkeypatch):
    secret_file = tmp_path / "worker.token"
    secret_file.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("AM_TEST_KEY", "from-env")
    path = tmp_path / "config.yaml"
    path.write_text(
        "server:\n"
        "  api_keys: [\"env:AM_TEST_KEY\"]\n"
        "worker:\n"
        f"  token: \"file:{secret_file.as_posix()}\"\n",
        encoding="utf-8",
    )
    cfg = Config.load(str(path))
    assert cfg.server.api_keys == ["from-env"]
    assert cfg.worker.token == "from-file"


def test_missing_secret_reference_fails_closed(tmp_path, monkeypatch):
    monkeypatch.delenv("AM_DOES_NOT_EXIST", raising=False)
    path = tmp_path / "config.yaml"
    path.write_text(
        "server:\n  api_keys: [\"env:AM_DOES_NOT_EXIST\"]\n", encoding="utf-8"
    )
    # A missing key must not silently disable authentication.
    with pytest.raises(ValueError, match="AM_DOES_NOT_EXIST"):
        Config.load(str(path))
