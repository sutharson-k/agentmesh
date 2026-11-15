# Architecture

## Goals

1. Run **100+ concurrent AI coding agents** across many hosts.
2. Give every agent a **fresh, isolated sandbox** with hard CPU/memory/PID/network limits.
3. Never lose accepted work when a worker dies.
4. Keep the coordination path small enough to reason about and test.

## Components

### Control plane

A stateless FastAPI service over a SQLite database in WAL mode. It owns tasks, workers, leases, tenants and the audit trail. API replicas can share the database if the filesystem supports correct locking; the default deployment is one control-plane process per database.

### Worker

A daemon on each sandbox host. It advertises slots/CPU/memory/capabilities/labels, long-polls the control plane for leases, runs one sandbox per leased task, renews leases via heartbeat, streams a bounded log tail and result back, and exposes cancellation. Workers never talk to each other.

### Sandbox backend

Pluggable interface (`worker/backends/base.py`):

- `docker` — hardened container per run (default).
- `firecracker` — microVM per run on Linux/KVM.
- `fake` — deterministic in-process backend for tests/demos; **not** an isolation boundary and never selected implicitly in production.

## Task lifecycle

```
queued ──lease──▶ leased ──first heartbeat──▶ running ──result──▶ succeeded
  ▲                  │                          │
  │                  └──── lease expired ───────┘
  │                              │
  ├──── retry (attempts left) ◀──┤
  │                              └──── attempts exhausted ──▶ failed / timed_out
  └──────────────── cancel ──▶ canceled
```

- Claim increments `attempts` and is atomic (`BEGIN IMMEDIATE` + conditional `UPDATE ... WHERE status='queued'`). Two workers can never hold the same task.
- `started_at` is set when the task is leased; wall-clock `duration_s` and queue wait are derived from persisted timestamps.
- Failures requeue with exponential backoff (1s, 2s, 4s… capped at 60s) while `attempts <= max_retries`; otherwise the task becomes terminal. Timeouts preserve a `timed_out` terminal state.
- Cancellation of a running task sets `status='canceled'` and keeps the lease. The next heartbeat returns the task id in `cancel`; the worker kills the sandbox. A late result is ignored but its logs are retained.
- Manual retry resets attempts and clears execution state; it is only allowed on terminal non-success tasks.

## Scheduling

For each lease request the control plane:

1. Rejects draining/offline workers and checks the global active cap.
2. Loads queued candidates (`not_before <= now`), ordered by priority.
3. Filters by worker pool, required capabilities and required labels.
4. Filters by per-tenant concurrency.
5. Sorts eligible tasks by:
   - aged priority (base priority + 1 per 5 minutes waiting, capped at +15),
   - tenant virtual time (weighted fair queuing),
   - virtual-time increment (heavier tenant wins exact ties),
   - FIFO age.
6. Repeatedly claims the best task that fits the worker's remaining CPU/memory, skipping oversized tasks so smaller work can backfill.

Tenant virtual time advances by `1 / weight` per claim, implementing weighted fair queuing across tenants. The bucket-based aging avoids the subtle bug where sub-second submission jitter turns WFQ into FIFO.

## Leases and failure recovery

A lease is `(task_id, lease_id, worker_id, expires_at, attempt)`. Worker heartbeats extend leases for every task it reports running, and return cancellation notices.

- **Lease expiry** (worker hung) is reaped every maintenance tick: requeue with backoff or fail after retries.
- **Worker silence** past `heartbeat_timeout_s`: worker marked offline, all its tasks immediately requeued (without waiting for lease timeout).
- **Result idempotency**: completion is guarded by task status + lease id + worker id. A second completion returns 409; a late result for a canceled task never resurrects it.
- **Worker shutdown**: the daemon cancels its sandboxes and reports failure so work resumes immediately instead of waiting for lease expiry.

## Concurrency model

Workers report actual free slots/CPU/memory with each lease request and heartbeat. The control plane never assigns more than the worker advertises; the worker itself refuses to exceed `slots`. This is a pull-based, credit-style flow-control loop:

```
free capacity ──lease request──▶ tasks (each ≤ free resources)
      ▲                                   │
      └──────── completion/heartbeat ◀────┘
```

Backpressure is implicit: a full worker stops leasing; a slow control plane reduces throughput without overcommitting hosts. The global cap (`max_global_running`) protects the database and downstream quotas.

## Failure domains

| Failure | Behaviour |
|---|---|
| Worker process crash | Heartbeats stop; leases expire or worker-loss recovery requeues tasks. |
| Worker host dies | Same, plus all sandboxes die with the host. |
| Sandbox OOM | Cgroup kills the container/VM; worker reports failure; retries follow policy. |
| Agent hangs | Per-task timeout; worker sends SIGTERM then SIGKILL, reports `timeout`. |
| Control plane restart | SQLite state persists; workers re-register after 404; expired leases recover. |
| Worker can't reach control plane | After 5 failed heartbeats it terminates local sandboxes and stops (fail-safe, no unaccountable compute). |
| Duplicate submission | `Idempotency-Key` / `idempotency_key` returns the original task. |

## Why SQLite

The coordination workload is small (state transitions, leases) relative to agent I/O. SQLite WAL gives ACID transitions and simple operations with zero infrastructure. `Store` is an isolation seam: a Postgres implementation with `SELECT ... FOR UPDATE SKIP LOCKED` can replace it for multi-node control planes without changing worker or API semantics.

## Scaling notes

- One worker can advertise hundreds of slots if CPU/memory allow; per-task limits remain enforced by cgroups.
- Long-polling workers wake within ~250ms of new work and avoid tight polling.
- Batch submission is sequential per task today; for very large bursts, insert batches in one transaction or add a bulk endpoint.
- SQLite is the single write bottleneck; expect thousands of state transitions per second on local NVMe, far above what 100+ agents need.

## Observability design

Every state change writes an append-only event (`submitted`, `leased`, `started`, `retry_scheduled`, `lease_expired`, `worker_lost`, `succeeded`, `failed`, `canceled`, `late_result_ignored`). Events are the audit source of truth; task rows are the current projection. Metrics are computed from the same tables, so the dashboard and alerts cannot disagree with the API.
