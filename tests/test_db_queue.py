from __future__ import annotations

import threading
import time

import pytest

from agentmesh.db import Conflict, Store
from agentmesh.models import AgentSpec, CompleteRequest, TaskSubmit


def submit(store: Store, name: str = "t", **overrides) -> dict:
    data = {
        "name": name,
        "agent": AgentSpec(kind="mock", prompt="do work"),
        "tenant": "default",
    }
    data.update(overrides)
    task, _ = store.submit_task(TaskSubmit(**data))
    return task


def worker(store: Store, worker_id: str = "w1", **overrides) -> dict:
    data = {
        "worker_id": worker_id,
        "hostname": "host",
        "pool": "default",
        "cpu_total": 64,
        "memory_mb_total": 65536,
        "slots": 64,
        "capabilities": [],
        "labels": {},
        "backend": "fake",
    }
    data.update(overrides)
    return store.register_worker(data)


def claim(store: Store, worker_id: str = "w1", **kwargs):
    params = {"max_tasks": 64, "free_cpu": 64, "free_mem_mb": 65536}
    params.update(kwargs)
    return store.claim_tasks(worker_id, **params)


def test_submit_and_idempotency(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        first = submit(store, idempotency_key="abc")
        second, created = store.submit_task(
            TaskSubmit(
                name="ignored",
                agent=AgentSpec(kind="mock", prompt="other"),
                idempotency_key="abc",
            )
        )
        assert created is False
        assert second["id"] == first["id"]
        assert second["name"] == "t"
        assert first["status"] == "queued"
    finally:
        store.close()


def test_priority_and_capability_matching(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store, capabilities=["gpu"])
        low = submit(store, name="low", priority=1)
        high = submit(store, name="high", priority=90)
        needs_tpm = submit(store, name="tpm", priority=99, require={"capabilities": ["tpm"]})
        claimed = claim(store)
        ids = [c["task"]["id"] for c in claimed]
        assert ids[0] == high["id"]
        assert low["id"] in ids
        assert needs_tpm["id"] not in ids
    finally:
        store.close()


def test_capacity_is_respected_and_small_tasks_backfill(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        big = submit(store, name="big", cpu=6, memory_mb=6144)
        small1 = submit(store, name="s1", cpu=1, memory_mb=512)
        small2 = submit(store, name="s2", cpu=1, memory_mb=512)
        claimed = claim(store, max_tasks=3, free_cpu=8, free_mem_mb=8192)
        ids = [c["task"]["id"] for c in claimed]
        assert big["id"] in ids
        assert small1["id"] in ids and small2["id"] in ids
        assert len(ids) == 3
    finally:
        store.close()


def test_atomic_claims_do_not_duplicate(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store, "w1")
        worker(store, "w2")
        for i in range(30):
            submit(store, name=f"t{i}")
        results: list[str] = []
        lock = threading.Lock()

        def run(wid: str) -> None:
            while True:
                got = claim(store, wid, max_tasks=5, free_cpu=100, free_mem_mb=100000)
                if not got:
                    return
                with lock:
                    results.extend(c["task"]["id"] for c in got)

        threads = [threading.Thread(target=run, args=(f"w{i}",)) for i in (1, 2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) == 30
        assert len(set(results)) == 30
    finally:
        store.close()


def test_tenant_fairness_weighted(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        store.set_tenant("heavy", weight=5, max_concurrent=64)
        store.set_tenant("light", weight=1, max_concurrent=64)
        for i in range(6):
            submit(store, f"h{i}", tenant="heavy")
            submit(store, f"l{i}", tenant="light")
        claimed = claim(store, max_tasks=8)
        tenants = [c["task"]["tenant"] for c in claimed]
        assert tenants.count("heavy") > tenants.count("light")
    finally:
        store.close()


def test_tenant_max_concurrent(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        store.set_tenant("solo", weight=1, max_concurrent=1)
        submit(store, "a", tenant="solo")
        submit(store, "b", tenant="solo")
        claimed = claim(store, max_tasks=10)
        assert len(claimed) == 1
    finally:
        store.close()


def test_complete_success_and_failure_retry(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        t = submit(store, max_retries=2)
        lease = claim(store)[0]
        done = store.complete_task(
            CompleteRequest(
                task_id=t["id"],
                lease_id=lease["lease_id"],
                outcome="succeeded",
                exit_code=0,
                result={"files": 1},
                logs_tail="hello",
            )
        )
        assert done["status"] == "succeeded"
        assert done["result"] == {"files": 1}
        assert store.task_logs(t["id"])["logs"] == "hello"

        t2 = submit(store, name="fail")
        lease2 = claim(store)[0]
        retried = store.complete_task(
            CompleteRequest(
                task_id=t2["id"],
                lease_id=lease2["lease_id"],
                outcome="failed",
                exit_code=2,
                error="boom",
            )
        )
        assert retried["status"] == "queued"
        assert retried["attempts"] == 1
        assert retried["worker_id"] is None
        raw = store.get_task_raw(t2["id"])
        assert raw["not_before"] > time.time()
        events = [e["kind"] for e in store.task_events(t2["id"])]
        assert "retry_scheduled" in events
    finally:
        store.close()


def test_lease_expiry_requeues_and_exhausts(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"), lease_ttl_s=1)
    try:
        worker(store)
        t = submit(store, max_retries=1)
        claim(store)
        time.sleep(1.05)
        assert store.reap_expired_leases() == 1
        assert store.get_task(t["id"])["status"] == "queued"
        assert any(e["kind"] == "lease_expired" for e in store.task_events(t["id"]))

        # Backoff has elapsed in the next second; claim and expire again, now
        # without retry budget -> terminal failure.
        time.sleep(1.1)
        lease = claim(store)
        assert lease
        time.sleep(1.05)
        store.reap_expired_leases()
        task = store.get_task(t["id"])
        assert task["status"] == "failed"
        assert "retries exhausted" in (task["error"] or "")
    finally:
        store.close()


def test_cancel_running_task_is_signalled_on_heartbeat(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        t = submit(store)
        lease = claim(store)[0]
        hb = store.heartbeat(
            "w1",
            {
                "running": [{"task_id": t["id"], "lease_id": lease["lease_id"]}],
                "free_cpu": 1,
                "free_mem_mb": 1,
                "free_slots": 1,
            },
        )
        assert hb["cancel"] == []
        assert store.get_task(t["id"])["status"] == "running"
        store.cancel_task(t["id"], "test")
        hb = store.heartbeat("w1", {"running": []})
        assert t["id"] in hb["cancel"]
        ignored = store.complete_task(
            CompleteRequest(
                task_id=t["id"],
                lease_id=lease["lease_id"],
                outcome="failed",
                exit_code=137,
                error="killed",
                logs_tail="late logs",
            )
        )
        assert ignored["status"] == "canceled"
        assert store.task_logs(t["id"])["logs"] == "late logs"
    finally:
        store.close()


def test_worker_loss_requeues_and_marks_offline(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        t = submit(store, max_retries=1)
        claim(store)
        recovered = store.reap_offline_workers(heartbeat_timeout_s=-1)
        assert t["id"] in recovered
        assert store.get_task(t["id"])["status"] == "queued"
        assert store.list_workers()[0]["status"] == "offline"
    finally:
        store.close()


def test_stats_and_counters(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store, "w-stats")
        t = submit(store)
        lease = claim(store, "w-stats")[0]
        store.complete_task(
            CompleteRequest(
                task_id=t["id"],
                lease_id=lease["lease_id"],
                outcome="succeeded",
                exit_code=0,
                duration_s=1.2,
            )
        )
        stats = store.stats()
        assert stats["tasks"]["by_status"]["succeeded"] == 1
        assert stats["workers"]["online"] == 1
        assert stats["counters"]["submitted"] == 1
        assert stats["counters"]["succeeded"] == 1
        with pytest.raises(Conflict):
            store.retry_task(t["id"])
        failed = submit(store, name="will-fail", max_retries=0)
        failed_lease = claim(store, "w-stats")[0]
        done = store.complete_task(
            CompleteRequest(
                task_id=failed["id"],
                lease_id=failed_lease["lease_id"],
                outcome="failed",
                exit_code=1,
                error="nope",
            )
        )
        assert done["status"] == "failed"
        assert store.retry_task(failed["id"])["status"] == "queued"
    finally:
        store.close()


def test_rejects_lease_from_wrong_worker(tmp_path):
    store = Store(str(tmp_path / "db.sqlite"))
    try:
        worker(store)
        t = submit(store)
        lease = claim(store)[0]
        with pytest.raises(Conflict):
            store.complete_task(
                CompleteRequest(task_id=t["id"], lease_id="lease_wrong", outcome="succeeded")
            )
        assert lease["lease_id"]
    finally:
        store.close()
