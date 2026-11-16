from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from agentmesh.api import create_app
from agentmesh.catalog import CatalogError, fetch_ollama_models
from agentmesh.config import Config


def client(tmp_path) -> TestClient:
    cfg = Config()
    cfg.server.db_path = str(tmp_path / "models.db")
    cfg.server.maintenance_interval_s = 0.2
    return TestClient(create_app(cfg, start_maintenance=False))


def test_import_list_filter_and_delete(tmp_path):
    api = client(tmp_path)
    response = api.post(
        "/v1/models/import",
        json={
            "models": [
                {
                    "provider": "openrouter",
                    "name": "free/model",
                    "free": True,
                    "context_length": 32_000,
                },
                {"provider": "openrouter", "name": "paid/model", "free": False},
            ]
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["inserted"] == 2

    listed = api.get("/v1/models").json()
    assert listed["count"] == 2
    assert {m["id"] for m in listed["models"]} == {
        "openrouter:free/model",
        "openrouter:paid/model",
    }

    free = api.get("/v1/models?free_only=true").json()
    assert [m["name"] for m in free["models"]] == ["free/model"]

    deleted = api.delete("/v1/models/openrouter:free/model")
    assert deleted.status_code == 200, deleted.text
    assert api.get("/v1/models").json()["count"] == 1
    assert api.delete("/v1/models/openrouter:free/model").status_code == 404


def test_ollama_import_uses_catalog(tmp_path, monkeypatch):
    api = client(tmp_path)

    def fake_fetch(base_url, **kwargs):
        assert base_url == "http://127.0.0.1:11434"
        return [
            {
                "provider": "ollama",
                "name": "qwen2.5-coder:7b",
                "free": True,
                "size_bytes": 4_700_000_000,
                "context_length": None,
                "metadata": {"family": "qwen2"},
            }
        ]

    monkeypatch.setattr("agentmesh.api.fetch_ollama_models", fake_fetch)
    response = api.post("/v1/models/import/ollama", json={})
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["inserted"] == 1
    assert body["models"][0]["provider"] == "ollama"
    assert body["models"][0]["free"] is True


def test_ollama_rejects_non_loopback_by_default():
    with pytest.raises(CatalogError, match="loopback"):
        fetch_ollama_models("http://example.com:11434")


def test_manual_import_cannot_smuggle_credentials(tmp_path):
    api = client(tmp_path)
    # Model entries are metadata only; unknown credential fields are ignored
    # rather than persisted.
    response = api.post(
        "/v1/models/import",
        json={
            "models": [
                {
                    "provider": "custom",
                    "name": "local-model",
                    "free": True,
                    "api_key": "sk-should-not-be-stored",
                }
            ]
        },
    )
    assert response.status_code == 200, response.text
    model = response.json()["models"][0]
    assert "api_key" not in model
    assert "sk-should-not-be-stored" not in str(model)
