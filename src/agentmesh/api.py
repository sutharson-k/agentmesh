"""Control plane HTTP API.

Coordinates tasks, workers, leases and tenant policy. The service is stateless
apart from SQLite; any number of API replicas can share the database as long as
the filesystem provides proper locking (single node / shared volume).
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from . import __version__
from .catalog import CatalogError, fetch_ollama_models, fetch_openrouter_models
from .config import Config
from .db import Conflict, NotFound, Store, WorkerOffline
from .metrics import render_metrics
from .models import (
    CompleteRequest,
    Heartbeat,
    LeaseRequest,
    ModelImportRequest,
    OllamaImportRequest,
    OpenRouterImportRequest,
    TaskQuery,
    TaskSubmit,
    TenantLimits,
    WorkerRegister,
)
from .util import iso, now

log = logging.getLogger("agentmesh.api")
DASHBOARD = Path(__file__).parent / "web" / "dashboard.html"
MAX_BODY_BYTES = 4 * 1024 * 1024


class WorkerStatusUpdate(BaseModel):
    status: str = Field(pattern="^(online|draining|offline)$")


class RateLimiter:
    def __init__(self, limit_per_minute: int) -> None:
        self.limit = limit_per_minute
        self._hits: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        if self.limit <= 0:
            return True
        async with self._lock:
            window = time.monotonic() - 60
            hits = self._hits[key]
            while hits and hits[0] < window:
                hits.popleft()
            if len(hits) >= self.limit:
                return False
            hits.append(time.monotonic())
            return True


def create_app(
    config: Config | None = None,
    *,
    store: Store | None = None,
    start_maintenance: bool = True,
) -> FastAPI:
    cfg = config or Config.load()
    owns_store = store is None
    db = store or Store(
        cfg.server.db_path,
        lease_ttl_s=cfg.server.lease_ttl_s,
        max_global_running=cfg.server.max_global_running,
        default_tenant_max=cfg.server.tenant_max_concurrent,
    )
    limiter = RateLimiter(cfg.server.max_submissions_per_minute)

    async def maintenance_loop() -> None:
        while True:
            try:
                result = await run_in_threadpool(db.housekeeping, cfg.server.heartbeat_timeout_s)
                if result["leases_requeued"] or result["worker_tasks_recovered"]:
                    log.warning("housekeeping recovered work: %s", result)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("housekeeping iteration failed")
            await asyncio.sleep(cfg.server.maintenance_interval_s)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if not cfg.server.auth_enabled:
            log.warning("API authentication is DISABLED (no server.api_keys configured)")
        log.info(
            "control plane starting",
            extra={
                "extra_fields": {
                    "db": cfg.server.db_path,
                    "auth": cfg.server.auth_enabled,
                    "version": __version__,
                }
            },
        )
        await run_in_threadpool(db.housekeeping, cfg.server.heartbeat_timeout_s)
        task = asyncio.create_task(maintenance_loop(), name="agentmesh-maintenance") if start_maintenance else None
        try:
            yield
        finally:
            if task:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if owns_store:
                db.close()

    app = FastAPI(
        title="AgentMesh Control Plane",
        version=__version__,
        description="Distributed orchestrator for sandboxed AI coding agents",
        lifespan=lifespan,
    )
    app.state.store = db
    app.state.config = cfg

    # ------------------------------------------------------------- middleware

    @app.middleware("http")
    async def guard(request: Request, call_next):
        length = request.headers.get("content-length")
        if length and length.isdigit() and int(length) > MAX_BODY_BYTES:
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        path = request.url.path
        if request.method != "OPTIONS" and cfg.server.auth_enabled:
            open_paths = {"/healthz", "/readyz", "/dashboard", "/"}
            if path not in open_paths:
                supplied = ""
                auth = request.headers.get("authorization", "")
                if auth.lower().startswith("bearer "):
                    supplied = auth[7:].strip()
                elif request.headers.get("x-api-key"):
                    supplied = request.headers["x-api-key"].strip()
                if not any(hmac.compare_digest(supplied, key) for key in cfg.server.api_keys):
                    return JSONResponse(
                        {"detail": "unauthorized"},
                        status_code=401,
                        headers={"WWW-Authenticate": "Bearer"},
                    )
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-AgentMesh-Version"] = __version__
        response.headers["X-Response-Time-Ms"] = f"{(time.perf_counter() - started) * 1000:.1f}"
        return response

    # ---------------------------------------------------------- error mapping

    @app.exception_handler(NotFound)
    async def not_found(_: Request, exc: NotFound):
        return JSONResponse({"detail": str(exc)}, status_code=404)

    @app.exception_handler(Conflict)
    async def conflict(_: Request, exc: Conflict):
        return JSONResponse({"detail": str(exc)}, status_code=409)

    @app.exception_handler(WorkerOffline)
    async def worker_offline(_: Request, exc: WorkerOffline):
        return JSONResponse({"detail": str(exc)}, status_code=503)

    @app.exception_handler(ValueError)
    async def bad_request(_: Request, exc: ValueError):
        return JSONResponse({"detail": str(exc)}, status_code=400)

    # ---------------------------------------------------------------- health

    @app.get("/healthz", include_in_schema=False)
    async def healthz():
        return {"status": "ok", "version": __version__, "time": iso(now())}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        ok = await run_in_threadpool(db.ping)
        if not ok:
            return JSONResponse({"status": "not-ready"}, status_code=503)
        return {"status": "ready", "version": __version__}

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse("/dashboard")

    @app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
    async def dashboard():
        try:
            return HTMLResponse(DASHBOARD.read_text(encoding="utf-8"))
        except OSError:
            return HTMLResponse("<h1>AgentMesh</h1><p>dashboard asset missing</p>", status_code=500)

    # ----------------------------------------------------------------- tasks

    async def _limiter_key(request: Request) -> str:
        auth = request.headers.get("authorization", "")
        if auth:
            return f"key:{auth[-12:]}"
        return f"ip:{request.client.host if request.client else 'unknown'}"

    def _submit(task: TaskSubmit) -> dict[str, Any]:
        if task.timeout_s > cfg.server.max_task_timeout_s:
            raise HTTPException(422, detail="timeout exceeds server maximum")
        public, created = db.submit_task(task)
        return {"task": public, "created": created}

    @app.post("/v1/tasks", status_code=201)
    async def submit_task(task: TaskSubmit, request: Request, response: Response):
        if not await limiter.allow(await _limiter_key(request)):
            raise HTTPException(429, detail="submission rate limit exceeded")
        header_key = request.headers.get("idempotency-key")
        if header_key and not task.idempotency_key:
            task = task.model_copy(update={"idempotency_key": header_key[:200]})
        result = await run_in_threadpool(_submit, task)
        response.status_code = 201 if result["created"] else 200
        return result

    @app.post("/v1/tasks/batch", status_code=201)
    async def submit_batch(tasks: list[TaskSubmit], request: Request):
        if not tasks:
            raise HTTPException(422, detail="empty batch")
        if len(tasks) > 1000:
            raise HTTPException(422, detail="batch limited to 1000 tasks")
        if not await limiter.allow(await _limiter_key(request)):
            raise HTTPException(429, detail="submission rate limit exceeded")
        results = [await run_in_threadpool(_submit, task) for task in tasks]
        return {
            "tasks": results,
            "created": sum(1 for r in results if r["created"]),
            "count": len(results),
        }

    @app.get("/v1/tasks")
    async def list_tasks(
        status: str | None = None,
        tenant: str | None = None,
        worker_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ):
        query = TaskQuery(
            status=status, tenant=tenant, worker_id=worker_id, limit=limit, offset=offset
        )
        tasks = await run_in_threadpool(
            lambda: db.list_tasks(
                statuses=query.statuses(),
                tenant=query.tenant,
                worker_id=query.worker_id,
                limit=query.limit,
                offset=query.offset,
            )
        )
        return {"tasks": tasks, "count": len(tasks), "limit": limit, "offset": offset}

    @app.get("/v1/tasks/{task_id}")
    async def get_task(task_id: str):
        return await run_in_threadpool(db.get_task, task_id)

    @app.post("/v1/tasks/{task_id}/cancel")
    async def cancel_task(task_id: str, reason: str = "canceled by user"):
        return await run_in_threadpool(db.cancel_task, task_id, reason)

    @app.post("/v1/tasks/{task_id}/retry")
    async def retry_task(task_id: str):
        return await run_in_threadpool(db.retry_task, task_id)

    @app.get("/v1/tasks/{task_id}/events")
    async def task_events(task_id: str, limit: int = 200):
        events = await run_in_threadpool(db.task_events, task_id, min(limit, 1000))
        return {"task_id": task_id, "events": events}

    @app.get("/v1/tasks/{task_id}/logs")
    async def task_logs(task_id: str):
        return await run_in_threadpool(db.task_logs, task_id)

    # --------------------------------------------------------------- workers

    @app.post("/v1/workers/register")
    async def register_worker(worker: WorkerRegister):
        data = worker.model_dump()
        public = await run_in_threadpool(db.register_worker, data)
        log.info("worker registered", extra={"extra_fields": {"worker_id": public["id"]}})
        return {
            "worker_id": public["id"],
            "lease_ttl_s": cfg.server.lease_ttl_s,
            "heartbeat_interval_s": cfg.worker.heartbeat_interval_s,
            "server_time": now(),
        }

    @app.get("/v1/workers")
    async def list_workers():
        workers = await run_in_threadpool(db.list_workers)
        return {"workers": workers, "count": len(workers)}

    @app.get("/v1/workers/{worker_id}")
    async def get_worker(worker_id: str):
        return Store.public_worker(await run_in_threadpool(db.get_worker, worker_id))

    @app.post("/v1/workers/{worker_id}/status")
    async def set_worker_status(worker_id: str, update: WorkerStatusUpdate):
        return await run_in_threadpool(db.set_worker_status, worker_id, update.status)

    @app.post("/v1/workers/{worker_id}/heartbeat")
    async def heartbeat(worker_id: str, hb: Heartbeat):
        return await run_in_threadpool(db.heartbeat, worker_id, hb.model_dump())

    @app.post("/v1/workers/{worker_id}/lease")
    async def lease(worker_id: str, body: LeaseRequest, request: Request):
        deadline = time.monotonic() + body.wait_s
        while True:
            claimed = await run_in_threadpool(
                lambda: db.claim_tasks(
                    worker_id,
                    max_tasks=min(body.free_slots, 4096),
                    free_cpu=body.free_cpu,
                    free_mem_mb=body.free_mem_mb,
                )
            )
            if claimed:
                return {"tasks": claimed, "server_time": now()}
            remaining = deadline - time.monotonic()
            if remaining <= 0 or await request.is_disconnected():
                return {"tasks": [], "server_time": now()}
            await asyncio.sleep(min(0.25, remaining))

    @app.post("/v1/workers/{worker_id}/complete")
    async def complete(worker_id: str, body: CompleteRequest):
        current = await run_in_threadpool(db.get_task_raw, body.task_id)
        if current["worker_id"] != worker_id:
            raise Conflict("task is not leased to this worker")
        task = await run_in_threadpool(db.complete_task, body)
        status = task["status"]
        log.info(
            "task reported",
            extra={
                "extra_fields": {
                    "task_id": body.task_id,
                    "outcome": body.outcome,
                    "status": status,
                    "worker_id": worker_id,
                }
            },
        )
        return {"task": task}

    # --------------------------------------------------------------- tenants

    @app.get("/v1/tenants")
    async def list_tenants():
        tenants = await run_in_threadpool(db.list_tenants)
        return {"tenants": tenants}

    @app.put("/v1/tenants/{name}")
    async def set_tenant(name: str, limits: TenantLimits):
        if not name or len(name) > 64:
            raise HTTPException(422, detail="invalid tenant name")
        return await run_in_threadpool(db.set_tenant, name, limits.weight, limits.max_concurrent)

    # ---------------------------------------------------------------- models

    @app.get("/v1/models")
    async def list_models(free_only: bool = False, provider: str | None = None):
        models = await run_in_threadpool(
            db.list_models, free_only=free_only, provider=provider
        )
        return {"models": models, "count": len(models), "free_only": free_only}

    @app.post("/v1/models/import")
    async def import_models(body: ModelImportRequest):
        entries = [m.model_dump() for m in body.models]
        result = await run_in_threadpool(
            db.import_models, entries, source="manual", replace=body.replace
        )
        result["models"] = await run_in_threadpool(db.list_models)
        return result

    @app.post("/v1/models/import/openrouter")
    async def import_openrouter(body: OpenRouterImportRequest):
        try:
            entries = await run_in_threadpool(
                fetch_openrouter_models,
                url=cfg.catalog.openrouter_url,
                free_only=body.free_only,
                timeout_s=cfg.catalog.request_timeout_s,
            )
        except CatalogError as exc:
            raise HTTPException(502, detail=str(exc)) from exc
        result = await run_in_threadpool(
            db.import_models, entries, source="openrouter", replace=True
        )
        result["free_only"] = body.free_only
        result["models"] = await run_in_threadpool(db.list_models)
        return result

    @app.post("/v1/models/import/ollama")
    async def import_ollama(body: OllamaImportRequest):
        base_url = body.base_url or cfg.catalog.ollama_url
        try:
            entries = await run_in_threadpool(
                fetch_ollama_models,
                base_url,
                allow_remote=cfg.catalog.allow_remote_ollama,
                timeout_s=cfg.catalog.request_timeout_s,
            )
        except CatalogError as exc:
            raise HTTPException(502, detail=str(exc)) from exc
        result = await run_in_threadpool(
            db.import_models, entries, source="ollama", replace=True
        )
        result["base_url"] = base_url
        result["models"] = await run_in_threadpool(db.list_models)
        return result

    @app.delete("/v1/models/{model_id:path}")
    async def delete_model(model_id: str):
        removed = await run_in_threadpool(db.delete_model, model_id)
        if not removed:
            raise HTTPException(404, detail="model not found")
        return {"deleted": model_id}

    # ------------------------------------------------------------------ ops

    @app.get("/v1/stats")
    async def stats():
        data = await run_in_threadpool(db.stats)
        data["version"] = __version__
        data["db_size_bytes"] = await run_in_threadpool(db.size_bytes)
        data["auth_enabled"] = cfg.server.auth_enabled
        return data

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        data = await run_in_threadpool(db.stats)
        counters = await run_in_threadpool(db.counters)
        return Response(render_metrics(data, counters, version=__version__), media_type="text/plain; version=0.0.4")

    return app
