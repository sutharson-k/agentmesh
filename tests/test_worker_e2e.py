"""End-to-end distributed test: real HTTP control plane + N worker daemons.

Proves the two headline properties:
  * 100+ agents run concurrently across workers
  * every agent gets its own unique workspace/sandbox identity
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import httpx

from agentmesh.api import create_app
from agentmesh.config import Config
from agentmesh.worker.backends.fake import FakeBackend
from agentmesh.worker.daemon import WorkerDaemon

TASKS = 120
WORKERS = 4
SLOTS_PER_WORKER = 35


async def wait_until(predicate, timeout: float, interval: float = 0.1):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(interval)
    raise AssertionError("condition not met before timeout")


def test_120_agents_run_in_parallel_with_isolated_sandboxes(tmp_path):
    asyncio.run(_scenario(tmp_path))


async def _scenario(tmp_path: Path) -> None:
    cfg = Config()
    cfg.server.db_path = str(tmp_path / "e2e.db")
    cfg.server.maintenance_interval_s = 0.2
    cfg.server.heartbeat_timeout_s = 120
    cfg.server.lease_ttl_s = 60
    cfg.server.max_global_running = 512
    app = create_app(cfg)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://control", timeout=30
        ) as control:
            # Submit while the queue is empty, then bring workers online. This
            # guarantees all 120 tasks exist simultaneously, which is what the
            # concurrency assertion measures.
            submissions = [
                {
                    "name": f"agent-{i:03d}",
                    "tenant": f"team-{i % 3}",
                    "priority": 50 + (i % 5),
                    "agent": {
                        "kind": "mock",
                        "prompt": f"[sleep=2.5] implement feature {i}",
                    },
                    "cpu": 1,
                    "memory_mb": 128,
                    "timeout_s": 60,
                }
                for i in range(TASKS)
            ]
            resp = await control.post("/v1/tasks/batch", json=submissions)
            assert resp.status_code == 201, resp.text
            assert resp.json()["created"] == TASKS

            workers: list[WorkerDaemon] = []
            worker_tasks: list[asyncio.Task] = []
            clients: list[httpx.AsyncClient] = []
            for i in range(WORKERS):
                wcfg = Config()
                wcfg.worker.slots = SLOTS_PER_WORKER
                wcfg.worker.cpu_total = SLOTS_PER_WORKER
                wcfg.worker.memory_mb_total = SLOTS_PER_WORKER * 1024
                wcfg.worker.workdir = str(tmp_path / f"worker-{i}")
                wcfg.worker.lease_wait_s = 0.2
                wcfg.worker.heartbeat_interval_s = 0.2
                wcfg.worker.pool = "default"
                client = httpx.AsyncClient(
                    transport=transport, base_url="http://control", timeout=30
                )
                clients.append(client)
                daemon = WorkerDaemon(
                    wcfg,
                    backend=FakeBackend(latency_s=2.5),
                    client=client,
                )
                workers.append(daemon)
                worker_tasks.append(asyncio.create_task(daemon.start()))

            async def four_workers():
                data = (await control.get("/v1/workers")).json()
                return len(data["workers"]) == WORKERS and all(
                    w["status"] == "online" for w in data["workers"]
                )

            await wait_until(four_workers, timeout=15)

            peak_active = 0

            async def all_done():
                nonlocal peak_active
                stats = (await control.get("/v1/stats")).json()
                peak_active = max(peak_active, stats["tasks"]["active"])
                terminal = sum(
                    stats["tasks"]["by_status"].get(s, 0)
                    for s in ("succeeded", "failed", "canceled", "timed_out")
                )
                return terminal if terminal >= TASKS else None

            try:
                await wait_until(all_done, timeout=60, interval=0.1)
            except AssertionError:
                debug = (await control.get("/v1/stats")).json()
                print("\nE2E DEBUG", debug["tasks"], debug["workers"])
                print("daemons", [d.snapshot() for d in workers])
                listing = (await control.get("/v1/tasks", params={"limit": 5})).json()["tasks"]
                print("sample", [(t["status"], t["attempts"], t["error"]) for t in listing])
                raise
            stats = (await control.get("/v1/stats")).json()
            assert stats["tasks"]["by_status"].get("succeeded") == TASKS, stats
            assert peak_active >= 100, f"only {peak_active} agents ran concurrently"
            assert stats["workers"]["online"] == WORKERS

            listing = (
                await control.get("/v1/tasks", params={"limit": 1000})
            ).json()["tasks"]
            assert len(listing) == TASKS
            workspaces = set()
            sandbox_keys = set()
            for task in listing:
                assert task["status"] == "succeeded"
                result = task["result"]
                workspaces.add(result["workspace"])
                sandbox_keys.add(result["sandbox_key"])
                assert task["duration_s"] is not None
            assert len(workspaces) == TASKS, "sandbox workspaces were shared"
            assert len(sandbox_keys) == TASKS, "sandbox identities were shared"
            # Workspaces physically exist on the workers and are not nested.
            for ws in workspaces:
                assert Path(ws).is_dir()
            assert len({Path(w).parent for w in workspaces}) >= 2

            events = (
                await control.get(f"/v1/tasks/{listing[0]['id']}/events")
            ).json()["events"]
            kinds = {e["kind"] for e in events}
            assert {"submitted", "leased", "started", "succeeded"} <= kinds

            # --- cancellation reaches a running sandbox ---------------------
            resp = await control.post(
                "/v1/tasks",
                json={
                    "name": "cancel-me",
                    "agent": {"kind": "mock", "prompt": "[sleep=20] long job"},
                    "timeout_s": 120,
                },
            )
            cancel_id = resp.json()["task"]["id"]

            async def running():
                task = (await control.get(f"/v1/tasks/{cancel_id}")).json()
                return task if task["status"] == "running" else None

            await wait_until(running, timeout=20)
            resp = await control.post(f"/v1/tasks/{cancel_id}/cancel")
            assert resp.json()["status"] == "canceled"

            async def worker_cleaned_up():
                remaining = sum(len(w.running) for w in workers)
                return remaining == 0

            await wait_until(worker_cleaned_up, timeout=15)
            assert (
                (await control.get(f"/v1/tasks/{cancel_id}")).json()["status"]
                == "canceled"
            )

            for worker in workers:
                worker.stop()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            for client in clients:
                await client.aclose()
