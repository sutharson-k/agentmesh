#!/usr/bin/env python
"""Live 120-agent demo: one control plane + N worker processes over HTTP.

Uses the deterministic fake sandbox backend so it runs anywhere, but the
control plane, queue, leases, workers and dashboard are the real system. The
script proves every agent gets a distinct workspace and sandbox identity.

    python scripts/demo_swarm.py --tasks 120 --workers 4 --slots 32
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import httpx
import uvicorn

from agentmesh.api import create_app
from agentmesh.config import Config
from agentmesh.logging import setup_logging

TERMINAL = {"succeeded", "failed", "timed_out", "canceled"}


def wait_http(url: str, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.15)
    raise RuntimeError(f"control plane at {url} did not become ready")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tasks", type=int, default=120)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--slots", type=int, default=32, help="slots per worker")
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--hold", action="store_true", help="keep the dashboard running after completion")
    parser.add_argument("--keep-db", action="store_true")
    args = parser.parse_args()

    var = ROOT / "var"
    var.mkdir(exist_ok=True)
    db_path = var / "demo.db"
    if not args.keep_db:
        for suffix in ("", "-wal", "-shm"):
            Path(str(db_path) + suffix).unlink(missing_ok=True)

    setup_logging("WARNING", "demo", json_logs=False)
    cfg = Config()
    cfg.server.host = "127.0.0.1"
    cfg.server.port = args.port
    cfg.server.db_path = str(db_path)
    cfg.server.maintenance_interval_s = 0.5
    cfg.server.max_global_running = max(512, args.tasks * 2)
    cfg.worker.heartbeat_interval_s = 0.5

    app = create_app(cfg)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning", access_log=False)
    )
    thread = threading.Thread(target=server.run, name="control-plane", daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{args.port}"
    workers: list[subprocess.Popen] = []
    log_files: list[Path] = []
    exit_code = 0

    try:
        wait_http(base + "/healthz")
        env = {
            **os.environ,
            "PYTHONPATH": str(ROOT / "src"),
            "AGENTMESH_LOG_LEVEL": "WARNING",
            "AGENTMESH_CONTROL_URL": base,
        }
        for i in range(args.workers):
            workdir = var / "demo-workers" / f"w{i}"
            shutil.rmtree(workdir, ignore_errors=True)
            log_path = var / f"demo-worker-{i}.log"
            log_files.append(log_path)
            handle = log_path.open("w", encoding="utf-8")
            cmd = [
                sys.executable,
                "-m",
                "agentmesh",
                "worker",
                "--url",
                base,
                "--backend",
                "fake",
                "--slots",
                str(args.slots),
                "--cpu",
                str(args.slots),
                "--memory",
                str(args.slots * 768),
                "--workdir",
                str(workdir),
                "--worker-id",
                f"demo-w{i}",
                "--no-json-logs",
            ]
            workers.append(
                subprocess.Popen(
                    cmd,
                    cwd=str(ROOT),
                    env=env,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                )
            )

        with httpx.Client(base_url=base, timeout=30) as client:
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                data = client.get("/v1/workers").json()
                online = [w for w in data["workers"] if w["status"] == "online"]
                if len(online) == args.workers:
                    break
                if any(p.poll() is not None for p in workers):
                    raise RuntimeError("a worker exited during startup; see var/demo-worker-*.log")
                time.sleep(0.2)
            else:
                raise RuntimeError("workers did not register in time")

            print(
                f"\n  AgentMesh demo · {args.tasks} agents · {args.workers} workers × "
                f"{args.slots} slots · {args.workers * args.slots} sandbox capacity\n"
            )
            submissions = [
                {
                    "name": f"agent-{i:03d}",
                    "tenant": f"team-{i % 3}",
                    "priority": 50 + (i % 5),
                    "agent": {
                        "kind": "mock",
                        "prompt": f"[sleep=1.2] implement feature {i}",
                    },
                    "cpu": 1,
                    "memory_mb": 128,
                    "timeout_s": 120,
                    "labels": {"demo": "true"},
                }
                for i in range(args.tasks)
            ]
            started = time.monotonic()
            resp = client.post("/v1/tasks/batch", json=submissions)
            resp.raise_for_status()
            assert resp.json()["created"] == args.tasks

            peak = 0
            last_line = ""
            while True:
                stats = client.get("/v1/stats").json()
                tasks = stats["tasks"]
                by = tasks["by_status"]
                peak = max(peak, tasks["active"])
                done = sum(by.get(s, 0) for s in TERMINAL)
                line = (
                    f"  active {tasks['active']:>3}  ·  queued {tasks['queued']:>3}  ·  "
                    f"done {done:>3}/{args.tasks}  ·  workers {stats['workers']['online']}  ·  "
                    f"peak {peak:>3}"
                )
                if line != last_line:
                    print("\r" + line + "   ", end="", flush=True)
                    last_line = line
                if done >= args.tasks:
                    break
                if time.monotonic() - started > 180:
                    raise RuntimeError("demo timed out")
                time.sleep(0.25)
            elapsed = time.monotonic() - started
            print("\n")

            listing = client.get("/v1/tasks", params={"limit": 1000}).json()["tasks"]
            workspaces = {t["result"]["workspace"] for t in listing}
            keys = {t["result"]["sandbox_key"] for t in listing}
            succeeded = sum(1 for t in listing if t["status"] == "succeeded")
            stats = client.get("/v1/stats").json()
            avg = stats["tasks"]["avg_duration_s"]
            wait = stats["tasks"]["avg_queue_wait_s"]

            print("  summary")
            print(f"    completed         {succeeded}/{args.tasks} in {elapsed:.2f}s")
            print(f"    peak concurrency  {peak} agents")
            print(f"    throughput        {args.tasks / max(elapsed, 0.001):.1f} agents/s")
            print(f"    avg agent time    {avg}s   avg queue wait {wait}s")
            print(f"    unique sandboxes  {len(keys)}")
            print(f"    unique workspaces {len(workspaces)}")
            print(f"    dashboard         {base}/dashboard\n")
            exit_code = 0 if succeeded == args.tasks and len(keys) == args.tasks else 1

            if args.hold:
                print("  dashboard is live; press Ctrl+C to stop\n")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    pass
    except Exception as exc:
        print(f"\ndemo failed: {exc}", file=sys.stderr)
        for path in log_files:
            if path.exists():
                tail = path.read_text(encoding="utf-8", errors="replace")[-2000:]
                if tail.strip():
                    print(f"--- {path.name} ---\n{tail}", file=sys.stderr)
        exit_code = 1
    finally:
        for proc in workers:
            if proc.poll() is None:
                proc.terminate()
        for proc in workers:
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
        server.should_exit = True
        thread.join(timeout=8)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
