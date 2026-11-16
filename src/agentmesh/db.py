"""Durable state, queue and scheduling primitives.

SQLite is used in WAL mode. Every state transition is transactional and the
claim path uses ``BEGIN IMMEDIATE`` so that two workers can never lease the
same task. The design intentionally keeps all coordination in the database:
workers are stateless apart from their sandbox workspaces, so the control
plane can run as a single process (or behind a leader-elected replica) and
workers can disappear at any time.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .models import CompleteRequest, TaskSubmit
from .util import iso, new_id, new_lease_id, now, redact_env, tail_text

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id                    TEXT PRIMARY KEY,
    idempotency_key       TEXT,
    tenant                TEXT NOT NULL,
    name                  TEXT NOT NULL,
    spec                  TEXT NOT NULL,
    status                TEXT NOT NULL,
    priority              INTEGER NOT NULL,
    attempts              INTEGER NOT NULL DEFAULT 0,
    max_retries           INTEGER NOT NULL,
    not_before            REAL NOT NULL DEFAULT 0,
    worker_id             TEXT,
    lease_id              TEXT,
    lease_expires_at      REAL,
    created_at            REAL NOT NULL,
    updated_at            REAL NOT NULL,
    started_at            REAL,
    finished_at           REAL,
    exit_code             INTEGER,
    error                 TEXT,
    result                TEXT,
    logs                  TEXT NOT NULL DEFAULT '',
    required_capabilities TEXT NOT NULL DEFAULT '[]'
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idem ON tasks(idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(status, not_before, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_tenant ON tasks(tenant, status);
CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks(worker_id, status);

CREATE TABLE IF NOT EXISTS task_events (
    seq      INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id  TEXT NOT NULL,
    ts       REAL NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, seq);

CREATE TABLE IF NOT EXISTS workers (
    id               TEXT PRIMARY KEY,
    hostname         TEXT NOT NULL,
    pool             TEXT NOT NULL,
    capacity_cpu     REAL NOT NULL,
    capacity_mem_mb  INTEGER NOT NULL,
    capacity_slots   INTEGER NOT NULL,
    free_cpu         REAL NOT NULL,
    free_mem_mb      INTEGER NOT NULL,
    free_slots       INTEGER NOT NULL,
    capabilities     TEXT NOT NULL DEFAULT '[]',
    labels           TEXT NOT NULL DEFAULT '{}',
    status           TEXT NOT NULL,
    backend          TEXT NOT NULL DEFAULT 'unknown',
    version          TEXT NOT NULL DEFAULT '',
    registered_at    REAL NOT NULL,
    last_heartbeat   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    id          TEXT PRIMARY KEY,
    task_id     TEXT NOT NULL,
    worker_id   TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    created_at  REAL NOT NULL,
    expires_at  REAL NOT NULL,
    released_at REAL,
    outcome     TEXT
);
CREATE INDEX IF NOT EXISTS idx_leases_task ON leases(task_id);

CREATE TABLE IF NOT EXISTS tenants (
    name            TEXT PRIMARY KEY,
    weight          REAL NOT NULL DEFAULT 1.0,
    max_concurrent  INTEGER NOT NULL DEFAULT 64,
    virtual_time    REAL NOT NULL DEFAULT 0,
    created_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

-- Model catalog. Rows are metadata only; credentials are never stored here.
CREATE TABLE IF NOT EXISTS models (
    id             TEXT PRIMARY KEY,
    provider       TEXT NOT NULL,
    name           TEXT NOT NULL,
    free           INTEGER NOT NULL DEFAULT 0,
    context_length INTEGER,
    size_bytes     INTEGER,
    metadata       TEXT NOT NULL DEFAULT '{}',
    source         TEXT NOT NULL DEFAULT 'manual',
    imported_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_models_provider ON models(provider, name);
CREATE INDEX IF NOT EXISTS idx_models_free ON models(free, provider);
"""

TERMINAL = {"succeeded", "failed", "canceled", "timed_out"}
ACTIVE = {"leased", "running"}


class StoreError(RuntimeError):
    pass


class NotFound(StoreError):
    pass


class Conflict(StoreError):
    pass


class WorkerOffline(StoreError):
    pass


class Store:
    def __init__(
        self,
        path: str,
        *,
        lease_ttl_s: int = 30,
        max_global_running: int = 512,
        default_tenant_max: int = 64,
    ) -> None:
        self.path = path
        self.lease_ttl_s = lease_ttl_s
        self.max_global_running = max_global_running
        self.default_tenant_max = default_tenant_max
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None, timeout=30
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.execute("PRAGMA user_version=1")

    # ------------------------------------------------------------------ infra

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
            except BaseException:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()
            finally:
                cur.close()

    def _query(self, sql: str, params: tuple[Any, ...] = ()) -> list[sqlite3.Row]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            try:
                return cur.fetchall()
            finally:
                cur.close()

    def _one(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Row | None:
        rows = self._query(sql, params)
        return rows[0] if rows else None

    @staticmethod
    def _event(cur: sqlite3.Cursor, task_id: str, kind: str, detail: dict[str, Any] | None = None) -> None:
        cur.execute(
            "INSERT INTO task_events(task_id, ts, kind, detail) VALUES (?,?,?,?)",
            (task_id, now(), kind, json.dumps(detail or {}, separators=(",", ":"))),
        )

    @staticmethod
    def _bump(cur: sqlite3.Cursor, name: str, amount: int = 1) -> None:
        cur.execute(
            "INSERT INTO counters(name, value) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
            (name, amount),
        )

    @staticmethod
    def _row(row: sqlite3.Row) -> dict[str, Any]:
        data = dict(row)
        for key in (
            "capabilities",
            "labels",
            "required_capabilities",
            "result",
            "detail",
            "metadata",
        ):
            if key in data and isinstance(data[key], str):
                try:
                    data[key] = json.loads(data[key])
                except json.JSONDecodeError:
                    data[key] = [] if key != "labels" else {}
        if "spec" in data and isinstance(data["spec"], str):
            data["spec"] = json.loads(data["spec"])
        return data

    def _task_internal(self, task_id: str) -> dict[str, Any]:
        row = self._one("SELECT * FROM tasks WHERE id = ?", (task_id,))
        if not row:
            raise NotFound(f"task {task_id} not found")
        return self._row(row)

    def _ensure_tenant(self, cur: sqlite3.Cursor, tenant: str) -> None:
        cur.execute(
            "INSERT OR IGNORE INTO tenants(name, weight, max_concurrent, virtual_time, created_at) "
            "VALUES (?, 1.0, ?, 0, ?)",
            (tenant, self.default_tenant_max, now()),
        )

    # ------------------------------------------------------------- projections

    @staticmethod
    def public_task(task: dict[str, Any], *, include_spec: bool = True) -> dict[str, Any]:
        spec = task.get("spec", {})
        prompt = ((spec.get("agent") or {}).get("prompt") or "") if spec else ""
        public: dict[str, Any] = {
            "id": task["id"],
            "name": task["name"],
            "tenant": task["tenant"],
            "status": task["status"],
            "priority": task["priority"],
            "attempts": task["attempts"],
            "max_retries": task["max_retries"],
            "created_at": task["created_at"],
            "created_at_iso": iso(task["created_at"]),
            "updated_at": task["updated_at"],
            "started_at": task["started_at"],
            "finished_at": task["finished_at"],
            "worker_id": task["worker_id"],
            "exit_code": task["exit_code"],
            "error": task["error"],
            "result": task["result"],
            "queue_wait_s": round(task["started_at"] - task["created_at"], 3)
            if task.get("started_at")
            else None,
            "duration_s": round(task["finished_at"] - task["started_at"], 3)
            if task.get("started_at") and task.get("finished_at")
            else None,
        }
        if include_spec:
            agent = spec.get("agent") or {}
            public["spec"] = {
                "image": spec.get("image"),
                "command": spec.get("command"),
                "agent": {
                    "kind": agent.get("kind"),
                    "model": agent.get("model"),
                    "prompt_preview": prompt[:280] + ("..." if len(prompt) > 280 else ""),
                }
                if spec.get("agent")
                else None,
                "env": redact_env(spec.get("env") or {}),
                "resources": {
                    "cpu": spec.get("cpu"),
                    "memory_mb": spec.get("memory_mb"),
                    "pids": spec.get("pids"),
                    "timeout_s": spec.get("timeout_s"),
                },
                "network": spec.get("network"),
                "require": spec.get("require"),
                "labels": spec.get("labels"),
            }
        return public

    @staticmethod
    def public_worker(worker: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": worker["id"],
            "hostname": worker["hostname"],
            "pool": worker["pool"],
            "status": worker["status"],
            "backend": worker["backend"],
            "version": worker["version"],
            "capacity": {
                "slots": worker["capacity_slots"],
                "cpu": worker["capacity_cpu"],
                "memory_mb": worker["capacity_mem_mb"],
            },
            "free": {
                "slots": worker["free_slots"],
                "cpu": round(worker["free_cpu"], 3),
                "memory_mb": worker["free_mem_mb"],
            },
            "capabilities": worker["capabilities"],
            "labels": worker["labels"],
            "registered_at": worker["registered_at"],
            "last_heartbeat": worker["last_heartbeat"],
            "last_heartbeat_iso": iso(worker["last_heartbeat"]),
        }

    # ----------------------------------------------------------------- tasks

    def submit_task(self, submission: TaskSubmit) -> tuple[dict[str, Any], bool]:
        """Insert a task. Returns (public task, created). Idempotent by key."""
        spec = submission.model_dump(mode="json")
        with self._tx() as cur:
            if submission.idempotency_key:
                existing = cur.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?",
                    (submission.idempotency_key,),
                ).fetchone()
                if existing:
                    return self.public_task(self._row(existing)), False
            self._ensure_tenant(cur, submission.tenant)
            task_id = new_id("task")
            ts = now()
            cur.execute(
                """INSERT INTO tasks(
                    id, idempotency_key, tenant, name, spec, status, priority, max_retries,
                    created_at, updated_at, required_capabilities
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    task_id,
                    submission.idempotency_key,
                    submission.tenant,
                    submission.name,
                    json.dumps(spec, separators=(",", ":")),
                    "queued",
                    submission.priority,
                    submission.max_retries,
                    ts,
                    ts,
                    json.dumps(submission.require.capabilities),
                ),
            )
            self._event(cur, task_id, "submitted", {"tenant": submission.tenant})
            self._bump(cur, "submitted")
        return self.public_task(self._task_internal(task_id)), True

    def get_task(self, task_id: str) -> dict[str, Any]:
        return self.public_task(self._task_internal(task_id))

    def get_task_raw(self, task_id: str) -> dict[str, Any]:
        return self._task_internal(task_id)

    def list_tasks(
        self,
        *,
        statuses: list[str] | None = None,
        tenant: str | None = None,
        worker_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if statuses:
            clauses.append(f"status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)
        if tenant:
            clauses.append("tenant = ?")
            params.append(tenant)
        if worker_id:
            clauses.append("worker_id = ?")
            params.append(worker_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        rows = self._query(
            f"SELECT * FROM tasks{where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        )
        return [self.public_task(self._row(r)) for r in rows]

    def task_events(self, task_id: str, limit: int = 200) -> list[dict[str, Any]]:
        if not self._one("SELECT id FROM tasks WHERE id = ?", (task_id,)):
            raise NotFound(f"task {task_id} not found")
        rows = self._query(
            "SELECT seq, ts, kind, detail FROM task_events WHERE task_id = ? ORDER BY seq DESC LIMIT ?",
            (task_id, limit),
        )
        out = []
        for row in reversed(rows):
            data = dict(row)
            try:
                data["detail"] = json.loads(data["detail"])
            except json.JSONDecodeError:
                data["detail"] = {}
            data["ts_iso"] = iso(data["ts"])
            out.append(data)
        return out

    def retry_task(self, task_id: str) -> dict[str, Any]:
        """Manually requeue a terminal task, resetting its attempt budget."""
        with self._tx() as cur:
            row = cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                raise NotFound(f"task {task_id} not found")
            task = self._row(row)
            if task["status"] not in {"failed", "timed_out", "canceled"}:
                raise Conflict(f"cannot retry a {task['status']} task")
            cur.execute(
                """UPDATE tasks SET status='queued', attempts=0, worker_id=NULL, lease_id=NULL,
                   lease_expires_at=NULL, started_at=NULL, finished_at=NULL, exit_code=NULL,
                   error=NULL, result=NULL, logs='', not_before=0, updated_at=? WHERE id=?""",
                (now(), task_id),
            )
            self._event(cur, task_id, "manual_retry", {"previous_status": task["status"]})
            self._bump(cur, "manual_retries")
        return self.public_task(self._task_internal(task_id))

    def task_logs(self, task_id: str) -> dict[str, Any]:
        task = self._task_internal(task_id)
        return {
            "task_id": task_id,
            "status": task["status"],
            "logs": task["logs"],
            "truncated": False,
        }

    def cancel_task(self, task_id: str, reason: str = "canceled by user") -> dict[str, Any]:
        with self._tx() as cur:
            row = cur.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            if not row:
                raise NotFound(f"task {task_id} not found")
            task = self._row(row)
            if task["status"] in TERMINAL:
                return self.public_task(task)
            if task["status"] == "queued":
                cur.execute(
                    "UPDATE tasks SET status='canceled', finished_at=?, updated_at=?, error=? WHERE id=?",
                    (now(), now(), reason, task_id),
                )
            else:
                # Leased/running: leave the lease in place so the owning worker can
                # observe the cancellation and terminate the sandbox. The heartbeat
                # response carries the cancel signal.
                cur.execute(
                    "UPDATE tasks SET status='canceled', updated_at=?, error=? WHERE id=?",
                    (now(), reason, task_id),
                )
            cur.execute(
                "UPDATE leases SET released_at=COALESCE(released_at, ?), outcome=COALESCE(outcome,'canceled') "
                "WHERE task_id=? AND released_at IS NULL",
                (now(), task_id),
            )
            self._event(cur, task_id, "canceled", {"reason": reason, "worker_id": task["worker_id"]})
            self._bump(cur, "canceled")
        return self.public_task(self._task_internal(task_id))

    # ---------------------------------------------------------------- workers

    def register_worker(self, data: dict[str, Any]) -> dict[str, Any]:
        worker_id = data.get("worker_id") or new_id("worker")
        ts = now()
        with self._tx() as cur:
            existing = cur.execute("SELECT id FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if existing:
                cur.execute(
                    """UPDATE workers SET hostname=?, pool=?, capacity_cpu=?, capacity_mem_mb=?,
                       capacity_slots=?, free_cpu=?, free_mem_mb=?, free_slots=?, capabilities=?,
                       labels=?, status='online', backend=?, version=?, last_heartbeat=?
                       WHERE id=?""",
                    (
                        data["hostname"],
                        data["pool"],
                        data["cpu_total"],
                        data["memory_mb_total"],
                        data["slots"],
                        data["cpu_total"],
                        data["memory_mb_total"],
                        data["slots"],
                        json.dumps(data.get("capabilities") or []),
                        json.dumps(data.get("labels") or {}),
                        data.get("backend", "unknown"),
                        data.get("version", ""),
                        ts,
                        worker_id,
                    ),
                )
            else:
                cur.execute(
                    """INSERT INTO workers(
                        id, hostname, pool, capacity_cpu, capacity_mem_mb, capacity_slots,
                        free_cpu, free_mem_mb, free_slots, capabilities, labels, status,
                        backend, version, registered_at, last_heartbeat
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        worker_id,
                        data["hostname"],
                        data["pool"],
                        data["cpu_total"],
                        data["memory_mb_total"],
                        data["slots"],
                        data["cpu_total"],
                        data["memory_mb_total"],
                        data["slots"],
                        json.dumps(data.get("capabilities") or []),
                        json.dumps(data.get("labels") or {}),
                        "online",
                        data.get("backend", "unknown"),
                        data.get("version", ""),
                        ts,
                        ts,
                    ),
                )
            cur.execute(
                "INSERT INTO task_events(task_id, ts, kind, detail) VALUES ('__system__',?,?,?)",
                (ts, "worker_registered", json.dumps({"worker_id": worker_id})),
            )
        worker = self._one("SELECT * FROM workers WHERE id = ?", (worker_id,))
        assert worker is not None
        return self.public_worker(self._row(worker))

    def get_worker(self, worker_id: str) -> dict[str, Any]:
        row = self._one("SELECT * FROM workers WHERE id = ?", (worker_id,))
        if not row:
            raise NotFound(f"worker {worker_id} not found")
        return self._row(row)

    def list_workers(self) -> list[dict[str, Any]]:
        rows = self._query("SELECT * FROM workers ORDER BY registered_at DESC")
        return [self.public_worker(self._row(r)) for r in rows]

    def set_worker_status(self, worker_id: str, status: str) -> dict[str, Any]:
        if status not in {"online", "draining", "offline"}:
            raise ValueError("invalid worker status")
        with self._tx() as cur:
            row = cur.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if not row:
                raise NotFound(f"worker {worker_id} not found")
            cur.execute(
                "UPDATE workers SET status=?, last_heartbeat=? WHERE id=?",
                (status, now(), worker_id),
            )
        return self.public_worker(self.get_worker(worker_id))

    def heartbeat(self, worker_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        running = payload.get("running") or []
        ts = now()
        with self._tx() as cur:
            row = cur.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if not row:
                raise NotFound(f"worker {worker_id} not found")
            worker = self._row(row)
            new_status = "draining" if worker["status"] == "draining" else "online"
            cur.execute(
                """UPDATE workers SET free_cpu=?, free_mem_mb=?, free_slots=?, last_heartbeat=?,
                   status=? WHERE id=?""",
                (
                    payload.get("free_cpu", 0),
                    payload.get("free_mem_mb", 0),
                    payload.get("free_slots", 0),
                    ts,
                    new_status,
                    worker_id,
                ),
            )
            renewed = 0
            for item in running:
                task_row = cur.execute(
                    "SELECT * FROM tasks WHERE id = ? AND lease_id = ? AND worker_id = ?",
                    (item["task_id"], item["lease_id"], worker_id),
                ).fetchone()
                if not task_row:
                    continue
                task = self._row(task_row)
                if task["status"] in {"leased", "running"}:
                    expires = ts + self.lease_ttl_s
                    cur.execute(
                        "UPDATE tasks SET lease_expires_at=?, updated_at=?, status='running', "
                        "started_at=COALESCE(started_at, ?) WHERE id=?",
                        (expires, ts, ts, task["id"]),
                    )
                    cur.execute(
                        "UPDATE leases SET expires_at=? WHERE id=?",
                        (expires, item["lease_id"]),
                    )
                    if task["status"] == "leased":
                        self._event(cur, task["id"], "started", {"worker_id": worker_id})
                    renewed += 1
            cancels = [
                r["id"]
                for r in cur.execute(
                    "SELECT id FROM tasks WHERE worker_id=? AND status='canceled'", (worker_id,)
                ).fetchall()
            ]
        return {"ok": True, "cancel": cancels, "renewed": renewed, "lease_ttl_s": self.lease_ttl_s}

    # --------------------------------------------------------------- claiming

    def claim_tasks(
        self,
        worker_id: str,
        *,
        max_tasks: int,
        free_cpu: float,
        free_mem_mb: int,
    ) -> list[dict[str, Any]]:
        """Atomically lease up to max_tasks queued tasks for a worker.

        Ordering is weighted-fair across tenants within priority classes:
        priority (with anti-starvation aging) first, then each tenant's virtual
        finish time. Tasks that do not fit remaining CPU/memory are skipped so a
        smaller task can still use the leftover capacity.
        """
        ts = now()
        claimed: list[dict[str, Any]] = []
        with self._tx() as cur:
            wrow = cur.execute("SELECT * FROM workers WHERE id = ?", (worker_id,)).fetchone()
            if not wrow:
                raise NotFound(f"worker {worker_id} not found")
            worker = self._row(wrow)
            if worker["status"] == "draining":
                return []
            if worker["status"] == "offline":
                raise WorkerOffline(f"worker {worker_id} is offline")

            active_total = cur.execute(
                "SELECT COUNT(*) AS n FROM tasks WHERE status IN ('leased','running')"
            ).fetchone()["n"]
            if active_total >= self.max_global_running:
                return []

            tenants = {
                r["name"]: dict(r) for r in cur.execute("SELECT * FROM tenants").fetchall()
            }
            active_by_tenant = {
                r["tenant"]: r["n"]
                for r in cur.execute(
                    "SELECT tenant, COUNT(*) AS n FROM tasks "
                    "WHERE status IN ('leased','running') GROUP BY tenant"
                ).fetchall()
            }
            worker_caps = set(worker["capabilities"])
            worker_labels = worker["labels"]

            candidates = [
                self._row(r)
                for r in cur.execute(
                    "SELECT * FROM tasks WHERE status='queued' AND not_before <= ? "
                    "ORDER BY priority DESC, created_at ASC LIMIT 500",
                    (ts,),
                ).fetchall()
            ]
            skipped: set[str] = set()
            cpu_left = float(free_cpu)
            mem_left = int(free_mem_mb)

            while len(claimed) < max_tasks and candidates:
                eligible: list[tuple[float, float, float, dict[str, Any]]] = []
                for task in candidates:
                    if task["id"] in skipped:
                        continue
                    spec = task["spec"]
                    require = spec.get("require") or {}
                    if require.get("pool") and require["pool"] != worker["pool"]:
                        skipped.add(task["id"])
                        continue
                    req_caps = set(require.get("capabilities") or [])
                    if not req_caps.issubset(worker_caps):
                        skipped.add(task["id"])
                        continue
                    req_labels = require.get("labels") or {}
                    if any(worker_labels.get(k) != v for k, v in req_labels.items()):
                        skipped.add(task["id"])
                        continue
                    tenant = task["tenant"]
                    limit = tenants.get(tenant, {}).get("max_concurrent", self.default_tenant_max)
                    if active_by_tenant.get(tenant, 0) >= limit:
                        skipped.add(task["id"])
                        continue
                    if spec.get("cpu", 0) > cpu_left + 1e-9 or spec.get("memory_mb", 0) > mem_left:
                        # Skip so a smaller task may still fit, but it may become
                        # eligible if the worker reports more capacity next time.
                        if spec.get("cpu", 0) > free_cpu or spec.get("memory_mb", 0) > free_mem_mb:
                            skipped.add(task["id"])
                        continue
                    wait_minutes = max(0.0, (ts - task["created_at"]) / 60.0)
                    # Aging is bucketed per 5 minutes. Sub-minute jitter must
                    # not break ties between tenants, otherwise WFQ degrades
                    # into FIFO because submission timestamps differ slightly.
                    aged_priority = task["priority"] + min(15, int(wait_minutes // 5))
                    tenant_state = tenants.get(tenant, {})
                    vt = tenant_state.get("virtual_time", 0.0)
                    # Ties on virtual time go to the heavier tenant; without
                    # this, equal timestamps can turn WFQ into plain round-robin.
                    increment = 1.0 / max(tenant_state.get("weight", 1.0), 0.01)
                    eligible.append(
                        (-aged_priority, vt, increment, task["created_at"], task)
                    )
                if not eligible:
                    break
                eligible.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
                *_, task = eligible[0]
                spec = task["spec"]
                tenant = task["tenant"]
                lease_id = new_lease_id()
                expires = ts + self.lease_ttl_s
                updated = cur.execute(
                    """UPDATE tasks SET status='leased', worker_id=?, lease_id=?, lease_expires_at=?,
                       attempts=attempts+1, not_before=0, updated_at=?,
                       started_at=COALESCE(started_at, ?)
                       WHERE id=? AND status='queued'""",
                    (worker_id, lease_id, expires, ts, ts, task["id"]),
                )
                if updated.rowcount != 1:
                    candidates = [c for c in candidates if c["id"] != task["id"]]
                    continue
                attempt = task["attempts"] + 1
                cur.execute(
                    """INSERT INTO leases(id, task_id, worker_id, attempt, created_at, expires_at)
                       VALUES (?,?,?,?,?,?)""",
                    (lease_id, task["id"], worker_id, attempt, ts, expires),
                )
                self._event(
                    cur,
                    task["id"],
                    "leased",
                    {"worker_id": worker_id, "attempt": attempt, "lease_id": lease_id},
                )
                cur.execute(
                    "UPDATE tenants SET virtual_time = virtual_time + ? WHERE name = ?",
                    (1.0 / max(tenants.get(tenant, {}).get("weight", 1.0), 0.01), tenant),
                )
                if tenant in tenants:
                    tenants[tenant]["virtual_time"] += 1.0 / max(tenants[tenant]["weight"], 0.01)
                active_by_tenant[tenant] = active_by_tenant.get(tenant, 0) + 1
                active_total += 1
                cpu_left -= spec.get("cpu", 0)
                mem_left -= spec.get("memory_mb", 0)
                candidates = [c for c in candidates if c["id"] != task["id"]]
                claimed.append(
                    {
                        "task": {
                            "id": task["id"],
                            "name": task["name"],
                            "tenant": tenant,
                            "attempt": attempt,
                            "spec": spec,
                        },
                        "lease_id": lease_id,
                        "lease_expires_at": expires,
                    }
                )
                if active_total >= self.max_global_running:
                    break
        return claimed

    # ------------------------------------------------------------- completion

    def complete_task(self, req: CompleteRequest) -> dict[str, Any]:
        ts = now()
        with self._tx() as cur:
            row = cur.execute("SELECT * FROM tasks WHERE id = ?", (req.task_id,)).fetchone()
            if not row:
                raise NotFound(f"task {req.task_id} not found")
            task = self._row(row)
            if task["lease_id"] != req.lease_id:
                raise Conflict("lease mismatch or lease already released")
            if task["status"] == "canceled":
                cur.execute(
                    "UPDATE tasks SET logs=?, updated_at=? WHERE id=?",
                    (tail_text(req.logs_tail), ts, req.task_id),
                )
                self._event(cur, req.task_id, "late_result_ignored", {"outcome": req.outcome})
                return self.public_task(self._task_internal(req.task_id))
            if task["status"] not in ACTIVE:
                raise Conflict(f"task is already {task['status']}")
            if task["status"] == "leased":
                # A fast task can finish before the worker's first heartbeat
                # marks it running. Emit the lifecycle event so the audit trail
                # is always submitted -> leased -> started -> terminal.
                self._event(cur, req.task_id, "started", {"worker_id": task["worker_id"]})

            cur.execute(
                "UPDATE leases SET released_at=?, outcome=? WHERE id=?",
                (ts, req.outcome, req.lease_id),
            )
            progress = {
                "exit_code": req.exit_code,
                "peak_memory_mb": req.peak_memory_mb,
                "cpu_seconds": req.cpu_seconds,
                "duration_s": req.duration_s,
            }
            logs = tail_text(req.logs_tail)
            if req.outcome == "succeeded":
                cur.execute(
                    """UPDATE tasks SET status='succeeded', finished_at=?, updated_at=?, exit_code=?,
                       error=NULL, result=?, logs=? WHERE id=?""",
                    (
                        ts,
                        ts,
                        req.exit_code if req.exit_code is not None else 0,
                        json.dumps(req.result),
                        logs,
                        req.task_id,
                    ),
                )
                self._event(cur, req.task_id, "succeeded", progress)
                self._bump(cur, "succeeded")
                self._bump(cur, "completed")
            else:
                final_status = "timed_out" if req.outcome == "timeout" else "failed"
                retry = task["attempts"] <= task["max_retries"]
                if retry:
                    backoff = min(60.0, float(2 ** max(0, task["attempts"] - 1)))
                    cur.execute(
                        """UPDATE tasks SET status='queued', not_before=?, updated_at=?, worker_id=NULL,
                           lease_id=NULL, lease_expires_at=NULL, started_at=NULL, exit_code=?,
                           error=?, result=?, logs=?
                           WHERE id=?""",
                        (
                            ts + backoff,
                            ts,
                            req.exit_code,
                            req.error,
                            json.dumps(req.result),
                            logs,
                            req.task_id,
                        ),
                    )
                    self._event(
                        cur,
                        req.task_id,
                        "retry_scheduled",
                        {"attempt": task["attempts"], "backoff_s": backoff, "reason": req.outcome},
                    )
                    self._bump(cur, "retried")
                else:
                    cur.execute(
                        """UPDATE tasks SET status=?, finished_at=?, updated_at=?, exit_code=?,
                           error=?, result=?, logs=? WHERE id=?""",
                        (
                            final_status,
                            ts,
                            ts,
                            req.exit_code,
                            req.error or req.outcome,
                            json.dumps(req.result),
                            logs,
                            req.task_id,
                        ),
                    )
                    self._event(
                        cur,
                        req.task_id,
                        final_status,
                        {**progress, "reason": req.outcome},
                    )
                    self._bump(cur, "timed_out" if req.outcome == "timeout" else "failed")
                    self._bump(cur, "completed")
        return self.public_task(self._task_internal(req.task_id))

    # --------------------------------------------------------------- recovery

    def reap_expired_leases(self, *, limit: int = 1000) -> int:
        ts = now()
        requeued = 0
        with self._tx() as cur:
            rows = cur.execute(
                "SELECT * FROM tasks WHERE status IN ('leased','running') AND lease_expires_at < ? LIMIT ?",
                (ts, limit),
            ).fetchall()
            for row in rows:
                task = self._row(row)
                retry = task["attempts"] <= task["max_retries"]
                cur.execute(
                    "UPDATE leases SET released_at=?, outcome='lease_expired' "
                    "WHERE id=? AND released_at IS NULL",
                    (ts, task["lease_id"]),
                )
                if retry:
                    cur.execute(
                        """UPDATE tasks SET status='queued', worker_id=NULL, lease_id=NULL,
                           lease_expires_at=NULL, started_at=NULL, not_before=?, updated_at=?, error=?
                           WHERE id=?""",
                        (ts + min(30.0, float(2 ** max(0, task["attempts"] - 1))), ts,
                         "lease expired before completion", task["id"]),
                    )
                    self._event(
                        cur,
                        task["id"],
                        "lease_expired",
                        {"attempt": task["attempts"], "worker_id": task["worker_id"]},
                    )
                    self._bump(cur, "lease_expired")
                    requeued += 1
                else:
                    cur.execute(
                        "UPDATE tasks SET status='failed', finished_at=?, updated_at=?, error=? "
                        "WHERE id=?",
                        (ts, ts, "lease expired; retries exhausted", task["id"]),
                    )
                    self._event(
                        cur,
                        task["id"],
                        "failed",
                        {"reason": "lease_expired", "attempts": task["attempts"]},
                    )
                    self._bump(cur, "failed")
                    self._bump(cur, "completed")
        return requeued

    def reap_offline_workers(self, heartbeat_timeout_s: float) -> list[str]:
        """Mark silent workers offline and immediately requeue their work."""
        ts = now()
        recovered: list[str] = []
        with self._tx() as cur:
            rows = cur.execute(
                "SELECT * FROM workers WHERE status IN ('online','draining') AND last_heartbeat < ?",
                (ts - heartbeat_timeout_s,),
            ).fetchall()
            for row in rows:
                worker_id = row["id"]
                cur.execute("UPDATE workers SET status='offline' WHERE id=?", (worker_id,))
                tasks = cur.execute(
                    "SELECT * FROM tasks WHERE worker_id=? AND status IN ('leased','running')",
                    (worker_id,),
                ).fetchall()
                for trow in tasks:
                    task = self._row(trow)
                    retry = task["attempts"] <= task["max_retries"]
                    cur.execute(
                        "UPDATE leases SET released_at=?, outcome='worker_lost' "
                        "WHERE id=? AND released_at IS NULL",
                        (ts, task["lease_id"]),
                    )
                    if retry:
                        cur.execute(
                            """UPDATE tasks SET status='queued', worker_id=NULL, lease_id=NULL,
                               lease_expires_at=NULL, started_at=NULL, not_before=?, updated_at=?,
                               error=? WHERE id=?""",
                            (ts + 1.0, ts, f"worker {worker_id} lost", task["id"]),
                        )
                        self._event(
                            cur,
                            task["id"],
                            "worker_lost",
                            {"worker_id": worker_id, "attempt": task["attempts"], "requeued": True},
                        )
                        self._bump(cur, "worker_lost")
                    else:
                        cur.execute(
                            "UPDATE tasks SET status='failed', finished_at=?, updated_at=?, error=? "
                            "WHERE id=?",
                            (ts, ts, f"worker {worker_id} lost; retries exhausted", task["id"]),
                        )
                        self._event(
                            cur,
                            task["id"],
                            "failed",
                            {"reason": "worker_lost", "worker_id": worker_id},
                        )
                        self._bump(cur, "failed")
                        self._bump(cur, "completed")
                    recovered.append(task["id"])
        return recovered

    # -------------------------------------------------------------- tenants

    def set_tenant(self, name: str, weight: float, max_concurrent: int) -> dict[str, Any]:
        with self._tx() as cur:
            cur.execute(
                """INSERT INTO tenants(name, weight, max_concurrent, virtual_time, created_at)
                   VALUES (?,?,?,0,?)
                   ON CONFLICT(name) DO UPDATE SET weight=excluded.weight,
                   max_concurrent=excluded.max_concurrent""",
                (name, weight, max_concurrent, now()),
            )
        return self.get_tenant(name)

    def get_tenant(self, name: str) -> dict[str, Any]:
        row = self._one("SELECT * FROM tenants WHERE name=?", (name,))
        if not row:
            raise NotFound(f"tenant {name} not found")
        data = dict(row)
        active = self._one(
            "SELECT COUNT(*) AS n FROM tasks WHERE tenant=? AND status IN ('leased','running')",
            (name,),
        )
        data["active"] = active["n"] if active else 0
        return data

    def list_tenants(self) -> list[dict[str, Any]]:
        rows = self._query("SELECT * FROM tenants ORDER BY name")
        out = []
        for row in rows:
            data = dict(row)
            active = self._one(
                "SELECT COUNT(*) AS n FROM tasks WHERE tenant=? AND status IN ('leased','running')",
                (data["name"],),
            )
            data["active"] = active["n"] if active else 0
            out.append(data)
        return out

    # ---------------------------------------------------------------- models

    @staticmethod
    def model_id(provider: str, name: str) -> str:
        return f"{provider}:{name}"

    def import_models(
        self,
        entries: list[dict[str, Any]],
        *,
        source: str = "manual",
        replace: bool = False,
    ) -> dict[str, Any]:
        """Upsert catalog rows. Metadata only: never store provider secrets."""
        ts = now()
        inserted = 0
        updated = 0
        with self._tx() as cur:
            if replace and entries:
                providers = {str(e["provider"]).strip().lower() for e in entries}
                for provider in providers:
                    cur.execute("DELETE FROM models WHERE provider=?", (provider,))
            for entry in entries:
                provider = str(entry["provider"]).strip().lower()
                name = str(entry["name"]).strip()
                model_id = self.model_id(provider, name)
                exists = cur.execute("SELECT 1 FROM models WHERE id=?", (model_id,)).fetchone()
                cur.execute(
                    """INSERT INTO models(id, provider, name, free, context_length, size_bytes,
                                          metadata, source, imported_at)
                       VALUES (?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(id) DO UPDATE SET free=excluded.free,
                         context_length=excluded.context_length,
                         size_bytes=excluded.size_bytes, metadata=excluded.metadata,
                         source=excluded.source, imported_at=excluded.imported_at""",
                    (
                        model_id,
                        provider,
                        name,
                        1 if entry.get("free") else 0,
                        entry.get("context_length"),
                        entry.get("size_bytes"),
                        json.dumps(entry.get("metadata") or {}, separators=(",", ":")),
                        source,
                        ts,
                    ),
                )
                if exists:
                    updated += 1
                else:
                    inserted += 1
        return {
            "inserted": inserted,
            "updated": updated,
            "total": inserted + updated,
            "source": source,
        }

    def list_models(
        self, *, free_only: bool = False, provider: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM models"
        clauses: list[str] = []
        params: list[Any] = []
        if free_only:
            clauses.append("free = 1")
        if provider:
            clauses.append("provider = ?")
            params.append(provider.strip().lower())
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY provider, name"
        out = []
        for row in self._query(sql, tuple(params)):
            data = self._row(row)
            data["free"] = bool(data["free"])
            out.append(data)
        return out

    def delete_model(self, model_id: str) -> bool:
        with self._tx() as cur:
            cur.execute("DELETE FROM models WHERE id=?", (model_id,))
            return cur.rowcount > 0

    def clear_models(self, provider: str | None = None) -> int:
        with self._tx() as cur:
            if provider:
                cur.execute("DELETE FROM models WHERE provider=?", (provider.strip().lower(),))
            else:
                cur.execute("DELETE FROM models")
            return cur.rowcount

    # ----------------------------------------------------------------- stats

    def stats(self) -> dict[str, Any]:
        ts = now()
        status_counts = {
            r["status"]: r["n"]
            for r in self._query("SELECT status, COUNT(*) AS n FROM tasks GROUP BY status")
        }
        workers = self._query("SELECT * FROM workers")
        workers_public = [self._row(w) for w in workers]
        online = [w for w in workers_public if w["status"] == "online"]
        counters = {
            r["name"]: r["value"]
            for r in self._query("SELECT name, value FROM counters WHERE name NOT LIKE '\\_\\_%' ESCAPE '\\'")
        }
        oldest = self._one(
            "SELECT MIN(created_at) AS ts FROM tasks WHERE status='queued'"
        )
        queued = status_counts.get("queued", 0)
        active = status_counts.get("leased", 0) + status_counts.get("running", 0)
        avg = self._one(
            """SELECT AVG(started_at - created_at) AS wait, AVG(finished_at - started_at) AS dur
               FROM (SELECT * FROM tasks WHERE status IN ('succeeded','failed','timed_out')
               ORDER BY finished_at DESC LIMIT 500)"""
        )
        tenant_rows = self.list_tenants()
        total_tasks = sum(status_counts.values())
        return {
            "now": ts,
            "tasks": {
                "total": total_tasks,
                "by_status": status_counts,
                "queued": queued,
                "active": active,
                "oldest_queued_age_s": round(ts - oldest["ts"], 1) if oldest and oldest["ts"] else 0,
                "avg_queue_wait_s": round(avg["wait"], 2) if avg and avg["wait"] is not None else None,
                "avg_duration_s": round(avg["dur"], 2) if avg and avg["dur"] is not None else None,
            },
            "workers": {
                "total": len(workers_public),
                "online": len(online),
                "draining": len([w for w in workers_public if w["status"] == "draining"]),
                "offline": len([w for w in workers_public if w["status"] == "offline"]),
                "capacity_slots": sum(w["capacity_slots"] for w in online),
                "free_slots": sum(w["free_slots"] for w in online),
                "capacity_cpu": round(sum(w["capacity_cpu"] for w in online), 2),
                "free_cpu": round(sum(w["free_cpu"] for w in online), 2),
                "capacity_memory_mb": sum(w["capacity_mem_mb"] for w in online),
                "free_memory_mb": sum(w["free_mem_mb"] for w in online),
            },
            "tenants": tenant_rows,
            "counters": counters,
            "config": {
                "lease_ttl_s": self.lease_ttl_s,
                "max_global_running": self.max_global_running,
                "default_tenant_max": self.default_tenant_max,
            },
        }

    def counters(self) -> dict[str, int]:
        return {
            r["name"]: r["value"]
            for r in self._query("SELECT name, value FROM counters WHERE name NOT LIKE '\\_\\_%' ESCAPE '\\'")
        }

    def ping(self) -> bool:
        try:
            self._one("SELECT 1 AS ok")
            return True
        except sqlite3.Error:
            return False

    def size_bytes(self) -> int:
        if self.path == ":memory:" or not os.path.exists(self.path):
            return 0
        return os.path.getsize(self.path)

    def housekeeping(self, heartbeat_timeout_s: float) -> dict[str, int]:
        requeued = self.reap_expired_leases()
        recovered = self.reap_offline_workers(heartbeat_timeout_s)
        return {"leases_requeued": requeued, "worker_tasks_recovered": len(recovered)}
