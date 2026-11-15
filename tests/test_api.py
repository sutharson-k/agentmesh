from __future__ import annotations

import time

from fastapi.testclient import TestClient

from agentmesh.api import create_app
from agentmesh.config import Config


def make_app(tmp_path, *, api_keys: list[str] | None = None, lease_ttl_s: int = 30):
    cfg = Config()
    cfg.server.db_path = str(tmp_path / "api.db")
    cfg.server.api_keys = api_keys or []
    cfg.server.lease_ttl_s = lease_ttl_s
    cfg.server.heartbeat_timeout_s = max(45, lease_ttl_s + 5)
    cfg.server.maintenance_interval_s = 0.2
    return create_app(cfg)


def task_payload(name="hello", **overrides):
    payload = {
        "name": name,
        "agent": {"kind": "mock", "prompt": "write a function"},
        "cpu": 1,
        "memory_mb": 256,
        "timeout_s": 60,
    }
    payload.update(overrides)
    return payload


def test_submit_get_list_and_idempotency(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        r = client.post("/v1/tasks", json=task_payload())
        assert r.status_code == 201, r.text
        task = r.json()["task"]
        assert task["status"] == "queued"
        assert task["spec"]["agent"]["prompt_preview"].startswith("write a function")

        r = client.post(
            "/v1/tasks",
            json=task_payload(name="other"),
            headers={"Idempotency-Key": "same-key"},
        )
        assert r.status_code == 201
        first_id = r.json()["task"]["id"]
        r = client.post(
            "/v1/tasks",
            json=task_payload(name="other"),
            headers={"Idempotency-Key": "same-key"},
        )
        assert r.status_code == 200
        assert r.json()["task"]["id"] == first_id

        r = client.get(f"/v1/tasks/{task['id']}")
        assert r.status_code == 200
        assert r.json()["id"] == task["id"]
        r = client.get("/v1/tasks", params={"limit": 10})
        assert r.json()["count"] == 2
        r = client.get("/v1/tasks", params={"status": "nonsense"})
        assert r.status_code == 400
        r = client.get("/v1/tasks/task_missing")
        assert r.status_code == 404


def test_validation_rejects_unsafe_or_ambiguous_tasks(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        for payload in [
            {"name": "x"},  # neither agent nor command
            {"name": "x", "agent": {"kind": "mock", "prompt": "p"}, "command": ["echo"]},
            {"name": "x", "agent": {"kind": "mock", "prompt": "p"}, "image": "bad image"},
            {
                "name": "x",
                "agent": {"kind": "mock", "prompt": "p"},
                "network": {"mode": "egress_allowlist", "allow": ["http://evil"]},
            },
            {"name": "x", "agent": {"kind": "MOCK", "prompt": "p"}},
        ]:
            r = client.post("/v1/tasks", json=payload)
            assert r.status_code == 422, (payload, r.text)


def test_full_worker_protocol(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        reg = {
            "worker_id": "w-test",
            "hostname": "host",
            "pool": "default",
            "slots": 4,
            "cpu_total": 4,
            "memory_mb_total": 4096,
            "capabilities": ["docker"],
            "labels": {"zone": "a"},
            "backend": "docker",
        }
        r = client.post("/v1/workers/register", json=reg)
        assert r.status_code == 200
        assert r.json()["worker_id"] == "w-test"
        assert r.json()["lease_ttl_s"] == 30

        created = client.post("/v1/tasks", json=task_payload()).json()["task"]
        r = client.post(
            "/v1/workers/w-test/lease",
            json={"wait_s": 0, "free_cpu": 4, "free_mem_mb": 4096, "free_slots": 4},
        )
        assert r.status_code == 200
        leases = r.json()["tasks"]
        assert len(leases) == 1
        lease = leases[0]
        assert lease["task"]["id"] == created["id"]
        assert lease["task"]["spec"]["agent"]["prompt"] == "write a function"

        r = client.post(
            "/v1/workers/w-test/heartbeat",
            json={
                "running": [{"task_id": created["id"], "lease_id": lease["lease_id"]}],
                "free_cpu": 3,
                "free_mem_mb": 3840,
                "free_slots": 3,
            },
        )
        assert r.status_code == 200
        assert r.json()["renewed"] == 1
        assert client.get(f"/v1/tasks/{created['id']}").json()["status"] == "running"

        r = client.post(
            "/v1/workers/w-test/complete",
            json={
                "task_id": created["id"],
                "lease_id": lease["lease_id"],
                "outcome": "succeeded",
                "exit_code": 0,
                "logs_tail": "agent finished",
                "result": {"files": ["main.py"]},
                "duration_s": 1.5,
            },
        )
        assert r.status_code == 200, r.text
        done = r.json()["task"]
        assert done["status"] == "succeeded"
        assert done["duration_s"] is not None and done["duration_s"] >= 0
        assert client.get(f"/v1/tasks/{created['id']}/logs").json()["logs"] == "agent finished"

        # Completing again with the same lease must conflict, not duplicate work.
        r = client.post(
            "/v1/workers/w-test/complete",
            json={
                "task_id": created["id"],
                "lease_id": lease["lease_id"],
                "outcome": "succeeded",
                "exit_code": 0,
            },
        )
        assert r.status_code == 409

        stats = client.get("/v1/stats").json()
        assert stats["workers"]["online"] == 1
        assert stats["tasks"]["by_status"]["succeeded"] == 1
        assert client.get("/v1/workers").json()["count"] == 1
        assert client.get("/v1/workers/w-test").json()["status"] == "online"


def test_cancel_and_retry_flow(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        task = client.post("/v1/tasks", json=task_payload()).json()["task"]
        r = client.post(f"/v1/tasks/{task['id']}/cancel")
        assert r.status_code == 200
        assert r.json()["status"] == "canceled"
        events = client.get(f"/v1/tasks/{task['id']}/events").json()["events"]
        assert any(e["kind"] == "canceled" for e in events)
        r = client.post(f"/v1/tasks/{task['id']}/retry")
        assert r.status_code == 200
        assert r.json()["status"] == "queued"
        assert r.json()["attempts"] == 0
        # A queued task cannot be manually retried.
        assert client.post(f"/v1/tasks/{task['id']}/retry").status_code == 409


def test_auth_metrics_and_dashboard(tmp_path):
    app = make_app(tmp_path, api_keys=["s3cret"])
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/v1/stats").status_code == 401
        assert client.get("/v1/stats", headers={"X-API-Key": "s3cret"}).status_code == 200
        assert (
            client.get(
                "/v1/stats", headers={"Authorization": "Bearer wrong"}
            ).status_code
            == 401
        )
        r = client.post(
            "/v1/tasks",
            json=task_payload(),
            headers={"Authorization": "Bearer s3cret"},
        )
        assert r.status_code == 201
        metrics = client.get("/metrics", headers={"Authorization": "Bearer s3cret"})
        assert metrics.status_code == 200
        body = metrics.text
        assert "agentmesh_tasks{status=\"queued\"} 1" in body
        assert "agentmesh_workers{status=\"online\"}" in body
        html = client.get("/dashboard")
        assert html.status_code == 200
        assert "AgentMesh" in html.text


def test_tenant_limits_and_batch(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        r = client.put("/v1/tenants/team-a", json={"weight": 3, "max_concurrent": 2})
        assert r.status_code == 200
        assert r.json()["weight"] == 3
        tasks = [task_payload(name=f"batch-{i}", tenant="team-a") for i in range(5)]
        r = client.post("/v1/tasks/batch", json=tasks)
        assert r.status_code == 201
        assert r.json()["created"] == 5
        tenants = client.get("/v1/tenants").json()["tenants"]
        team = next(t for t in tenants if t["name"] == "team-a")
        assert team["max_concurrent"] == 2
        assert client.post("/v1/tasks/batch", json=[]).status_code == 422


def test_lease_does_not_oversubscribe_worker_capacity(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as client:
        client.post(
            "/v1/workers/register",
            json={
                "worker_id": "w-small",
                "hostname": "host",
                "slots": 2,
                "cpu_total": 2,
                "memory_mb_total": 1024,
            },
        )
        for i in range(5):
            client.post(
                "/v1/tasks",
                json=task_payload(name=f"cap-{i}", cpu=1, memory_mb=512),
            )
        r = client.post(
            "/v1/workers/w-small/lease",
            json={"wait_s": 0, "free_cpu": 2, "free_mem_mb": 1024, "free_slots": 2},
        )
        leases = r.json()["tasks"]
        assert len(leases) == 2
        assert sum(x["task"]["spec"]["cpu"] for x in leases) <= 2

        # The worker advertises zero free capacity: nothing new must be leased.
        r = client.post(
            "/v1/workers/w-small/lease",
            json={"wait_s": 0, "free_cpu": 0, "free_mem_mb": 0, "free_slots": 0},
        )
        assert r.json()["tasks"] == []
