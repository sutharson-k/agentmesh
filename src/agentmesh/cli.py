"""Command line interface: server, worker, submit, status, task, cancel."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

from . import __version__
from .config import Config, DEFAULT_CONFIG_YAML
from .logging import setup_logging
from .models import AgentSpec, NetworkPolicy, Requirements, TaskSubmit
from .util import iso

TERMINAL = {"succeeded", "failed", "timed_out", "canceled"}


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, default=str))


def _apply_common(cfg: Config, args: argparse.Namespace) -> Config:
    if getattr(args, "config", None):
        cfg.source = args.config
    if getattr(args, "json_logs", None) is not None:
        cfg.server.json_logs = args.json_logs
    return cfg


def _client(args: argparse.Namespace) -> httpx.Client:
    headers = {}
    token = getattr(args, "token", None) or os.environ.get("AGENTMESH_TOKEN", "")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return httpx.Client(
        base_url=args.url.rstrip("/"),
        headers=headers,
        timeout=httpx.Timeout(60.0, connect=10.0),
    )


def _die(message: str, code: int = 1) -> None:
    print(f"error: {message}", file=sys.stderr)
    raise SystemExit(code)


# ----------------------------------------------------------------- commands


def cmd_server(args: argparse.Namespace) -> int:
    cfg = Config.load(args.config)
    if args.host:
        cfg.server.host = args.host
    if args.port:
        cfg.server.port = args.port
    if args.db:
        cfg.server.db_path = args.db
    if args.api_key:
        cfg.server.api_keys = args.api_key
    if args.log_level:
        cfg.server.log_level = args.log_level
    cfg.validate()
    from .server import run_server

    run_server(cfg)
    return 0


async def _run_worker(cfg: Config) -> None:
    from .worker.daemon import WorkerDaemon, install_signal_handlers

    daemon = WorkerDaemon(cfg)
    loop = asyncio.get_running_loop()
    install_signal_handlers(daemon, loop)
    try:
        await daemon.start()
    except asyncio.CancelledError:
        pass


def cmd_worker(args: argparse.Namespace) -> int:
    cfg = Config.load(args.config)
    w = cfg.worker
    if args.url:
        w.control_url = args.url
    if args.token:
        w.token = args.token
    if args.backend:
        w.backend = args.backend
    if args.slots:
        w.slots = args.slots
    if args.cpu:
        w.cpu_total = args.cpu
    if args.memory:
        w.memory_mb_total = args.memory
    if args.pool:
        w.pool = args.pool
    if args.workdir:
        w.workdir = args.workdir
    if args.worker_id:
        w.worker_id = args.worker_id
    if args.capability:
        w.capabilities = sorted(set(w.capabilities + args.capability))
    if args.label:
        for item in args.label:
            key, _, value = item.partition("=")
            if not key:
                _die(f"invalid label {item!r}; expected key=value")
            w.labels[key] = value
    cfg.validate()
    setup_logging(cfg.server.log_level, "worker", json_logs=cfg.server.json_logs)
    try:
        asyncio.run(_run_worker(cfg))
    except KeyboardInterrupt:
        pass
    return 0


def _task_payload(args: argparse.Namespace) -> TaskSubmit:
    if args.file:
        return TaskSubmit.model_validate_json(Path(args.file).read_text(encoding="utf-8"))
    if not args.name:
        _die("--name is required (or use --file)")
    agent = None
    command = None
    if args.prompt is not None:
        prompt = args.prompt
        if prompt == "-":
            prompt = sys.stdin.read()
        elif prompt.startswith("@"):
            prompt = Path(prompt[1:]).read_text(encoding="utf-8")
        agent = AgentSpec(kind=args.agent_kind, prompt=prompt, model=args.model)
    elif args.command:
        command = args.command
    else:
        _die("provide --prompt or --command")
    network = NetworkPolicy(mode="none")
    if args.allow:
        network = NetworkPolicy(mode="egress_allowlist", allow=args.allow)
    return TaskSubmit(
        name=args.name,
        tenant=args.tenant,
        priority=args.priority,
        image=args.image,
        command=command,
        agent=agent,
        cpu=args.cpu or 1.0,
        memory_mb=args.memory or 512,
        timeout_s=args.timeout,
        max_retries=args.retries,
        network=network,
        require=Requirements(
            pool=args.pool,
            capabilities=args.capability or [],
            labels=dict(item.split("=", 1) for item in (args.require_label or [])),
        ),
        labels=dict(item.split("=", 1) for item in (args.label or [])),
        idempotency_key=args.idempotency_key,
    )


def cmd_submit(args: argparse.Namespace) -> int:
    with _client(args) as client:
        if args.file and args.batch:
            payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
            tasks = payload["tasks"] if isinstance(payload, dict) else payload
            resp = client.post("/v1/tasks/batch", json=tasks)
            if resp.status_code >= 400:
                _die(resp.text)
            data = resp.json()
            if args.json:
                _print_json(data)
            else:
                print(f"submitted {data['count']} tasks ({data['created']} new)")
            ids = [item["task"]["id"] for item in data["tasks"]]
        else:
            task = _task_payload(args)
            resp = client.post("/v1/tasks", json=task.model_dump(mode="json"))
            if resp.status_code >= 400:
                _die(resp.text)
            item = resp.json()
            if args.json:
                _print_json(item)
            else:
                print(f"task {item['task']['id']} ({'created' if item['created'] else 'existing'})")
            ids = [item["task"]["id"]]

        if args.wait:
            for task_id in ids:
                _wait_for(client, task_id, show_logs=args.logs)
    return 0


def _wait_for(client: httpx.Client, task_id: str, *, show_logs: bool = True) -> dict[str, Any]:
    last = None
    deadline = time.monotonic() + 24 * 3600
    while True:
        resp = client.get(f"/v1/tasks/{task_id}")
        if resp.status_code >= 400:
            _die(resp.text)
        task = resp.json()
        if task["status"] != last:
            print(f"[{task_id}] {task['status']} (attempt {task['attempts']})")
            last = task["status"]
        if task["status"] in TERMINAL:
            if show_logs:
                logs = client.get(f"/v1/tasks/{task_id}/logs").json().get("logs", "")
                if logs:
                    print(logs[-4000:])
            if task["status"] != "succeeded":
                _die(f"task {task_id} ended {task['status']}: {task.get('error')}")
            return task
        if time.monotonic() > deadline:
            _die("timed out waiting for task")
        time.sleep(1.0)


def cmd_status(args: argparse.Namespace) -> int:
    with _client(args) as client:
        stats = client.get("/v1/stats").json()
        workers = client.get("/v1/workers").json()["workers"]
        tasks = client.get("/v1/tasks", params={"limit": args.limit}).json()["tasks"]
    if args.json:
        _print_json({"stats": stats, "workers": workers, "tasks": tasks})
        return 0
    t = stats["tasks"]
    w = stats["workers"]
    print(f"AgentMesh {stats.get('version')}  db={stats.get('db_size_bytes', 0)} bytes")
    print(
        f"tasks: {t['total']} total | queued {t['queued']} | active {t['active']} | "
        f"succeeded {t['by_status'].get('succeeded', 0)} | failed {t['by_status'].get('failed', 0)}"
    )
    print(
        f"workers: {w['online']} online / {w['total']} | slots {w['free_slots']}/{w['capacity_slots']} free "
        f"| cpu {w['free_cpu']}/{w['capacity_cpu']} | mem {w['free_memory_mb']}/{w['capacity_memory_mb']} MB"
    )
    if t.get("oldest_queued_age_s"):
        print(f"oldest queued: {t['oldest_queued_age_s']}s")
    print()
    print(f"{'WORKER':<28} {'STATUS':<9} {'BACKEND':<12} {'SLOTS':>10} {'CPU':>12} {'MEM MB':>14}")
    for worker in workers:
        free, cap = worker["free"], worker["capacity"]
        print(
            f"{worker['id']:<28} {worker['status']:<9} {worker['backend']:<12} "
            f"{free['slots']:>4}/{cap['slots']:<5} {free['cpu']:>5}/{cap['cpu']:<6} "
            f"{free['memory_mb']:>6}/{cap['memory_mb']:<7}"
        )
    print()
    print(f"{'TASK':<30} {'STATUS':<10} {'TENANT':<12} {'ATT':>3} {'WORKER':<20} {'NAME'}")
    for task in tasks:
        print(
            f"{task['id']:<30} {task['status']:<10} {task['tenant']:<12} "
            f"{task['attempts']:>3} {str(task['worker_id'] or '-'):<20} {task['name'][:40]}"
        )
    return 0


def cmd_task(args: argparse.Namespace) -> int:
    with _client(args) as client:
        if args.logs:
            data = client.get(f"/v1/tasks/{args.id}/logs").json()
            print(data.get("logs", ""))
            return 0
        if args.events:
            data = client.get(f"/v1/tasks/{args.id}/events").json()
            if args.json:
                _print_json(data)
            else:
                for event in data["events"]:
                    print(f"{event['ts_iso']}  {event['kind']:<20} {json.dumps(event['detail'])}")
            return 0
        resp = client.get(f"/v1/tasks/{args.id}")
        if resp.status_code >= 400:
            _die(resp.text)
        task = resp.json()
    _print_json(task)
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    with _client(args) as client:
        resp = client.post(f"/v1/tasks/{args.id}/cancel", params={"reason": args.reason})
        if resp.status_code >= 400:
            _die(resp.text)
        print(f"task {args.id}: {resp.json()['status']}")
    return 0


def cmd_retry(args: argparse.Namespace) -> int:
    with _client(args) as client:
        resp = client.post(f"/v1/tasks/{args.id}/retry")
        if resp.status_code >= 400:
            _die(resp.text)
        print(f"task {args.id}: {resp.json()['status']}")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if target.exists() and not args.force:
        _die(f"{target} exists; use --force")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(DEFAULT_CONFIG_YAML, encoding="utf-8")
    print(f"wrote {target}")
    return 0


# ------------------------------------------------------------------- parser


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentmesh", description=__doc__)
    parser.add_argument("--version", action="version", version=f"agentmesh {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    def add_http(p: argparse.ArgumentParser) -> None:
        p.add_argument("--url", default=os.environ.get("AGENTMESH_CONTROL_URL", "http://127.0.0.1:8080"))
        p.add_argument("--token", default=os.environ.get("AGENTMESH_TOKEN", ""))
        p.add_argument("--json", action="store_true", help="emit machine-readable JSON")

    p = sub.add_parser("server", help="run the control plane")
    p.add_argument("--config")
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument("--db")
    p.add_argument("--api-key", action="append", help="bearer token allowed to call the API (repeatable)")
    p.add_argument("--log-level", default=None)
    p.add_argument("--json-logs", dest="json_logs", action="store_true", default=None)
    p.add_argument("--no-json-logs", dest="json_logs", action="store_false")
    p.set_defaults(func=cmd_server)

    p = sub.add_parser("worker", help="run a sandbox worker")
    p.add_argument("--config")
    p.add_argument("--url")
    p.add_argument("--token")
    p.add_argument("--backend", choices=["auto", "docker", "firecracker", "fake"])
    p.add_argument("--slots", type=int)
    p.add_argument("--cpu", type=float)
    p.add_argument("--memory", type=int)
    p.add_argument("--pool")
    p.add_argument("--workdir")
    p.add_argument("--worker-id")
    p.add_argument("--capability", action="append")
    p.add_argument("--label", action="append")
    p.add_argument("--allow-fake-fallback", action="store_true")
    p.add_argument("--json-logs", dest="json_logs", action="store_true", default=None)
    p.add_argument("--no-json-logs", dest="json_logs", action="store_false")
    p.set_defaults(func=cmd_worker)

    p = sub.add_parser("submit", help="submit a task or a JSON batch")
    add_http(p)
    p.add_argument("--file")
    p.add_argument("--batch", action="store_true", help="treat --file as a batch")
    p.add_argument("--name")
    p.add_argument("--tenant", default="default")
    p.add_argument("--priority", type=int, default=50)
    p.add_argument("--image", default="python:3.12-slim")
    p.add_argument("--prompt", help="agent prompt; '-' reads stdin, '@file' reads a file")
    p.add_argument("--agent-kind", default="mock")
    p.add_argument("--model")
    p.add_argument("--command", nargs=argparse.REMAINDER)
    p.add_argument("--cpu", type=float)
    p.add_argument("--memory", type=int)
    p.add_argument("--timeout", type=int, default=900)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--allow", action="append", help="egress allowlist domain (repeatable)")
    p.add_argument("--pool", help="required worker pool")
    p.add_argument("--capability", action="append", help="required worker capability")
    p.add_argument("--require-label", action="append")
    p.add_argument("--label", action="append")
    p.add_argument("--idempotency-key")
    p.add_argument("--wait", action="store_true", help="wait for completion")
    p.add_argument("--logs", action="store_true", help="show logs when waiting")
    p.set_defaults(func=cmd_submit)

    p = sub.add_parser("status", help="show cluster, worker and task status")
    add_http(p)
    p.add_argument("--limit", type=int, default=25)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("task", help="inspect a task")
    add_http(p)
    p.add_argument("id")
    p.add_argument("--events", action="store_true")
    p.add_argument("--logs", action="store_true")
    p.set_defaults(func=cmd_task)

    p = sub.add_parser("cancel", help="cancel a task")
    add_http(p)
    p.add_argument("id")
    p.add_argument("--reason", default="canceled by user")
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("retry", help="manually requeue a failed task")
    add_http(p)
    p.add_argument("id")
    p.set_defaults(func=cmd_retry)

    p = sub.add_parser("init", help="write a starter configuration file")
    p.add_argument("--path", default="agentmesh.yaml")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_init)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if getattr(args, "json_logs", None) is None:
        args.json_logs = True
    try:
        return int(args.func(args) or 0)
    except httpx.ConnectError as exc:
        _die(f"cannot reach control plane at {getattr(args, 'url', '?')}: {exc}")
    except httpx.HTTPError as exc:
        _die(f"HTTP error: {exc}")
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
