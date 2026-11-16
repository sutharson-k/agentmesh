# AgentMesh

**A distributed orchestration backend for fleets of sandboxed AI coding agents.**

AgentMesh schedules coding-agent tasks across many worker machines, gives **every agent its own isolated sandbox** (Docker container or Firecracker microVM), enforces CPU / memory / PID / network limits, and survives worker crashes without losing work.

```
                 ┌──────────────────────────────────────────────┐
   clients ─────▶│ control plane  (FastAPI + SQLite WAL)        │
   agentmesh     │  • durable priority queue + leases           │
   submit/status │  • weighted-fair tenant scheduling           │
                 │  • capability / pool placement               │
                 │  • lease recovery + retries                  │
                 │  • Prometheus metrics + live dashboard       │
                 └───────────────┬──────────────────────────────┘
                                 │ authenticated HTTP (pull model)
          ┌──────────────────────┼──────────────────────┐
          ▼                      ▼                      ▼
   ┌─────────────┐        ┌─────────────┐        ┌─────────────┐
   │ worker-1    │        │ worker-2    │        │ worker-N    │
   │ slots 1..k  │        │ slots 1..k  │        │ slots 1..k  │
   ├─────────────┤        ├─────────────┤        ├─────────────┤
   │ sandbox     │        │ sandbox     │        │ sandbox     │
   │ sandbox     │        │ sandbox     │        │ sandbox     │
   │ ...         │        │ ...         │        │ ...         │
   └─────────────┘        └─────────────┘        └─────────────┘
        each sandbox = one agent, one private workspace, one network namespace
```

## What is implemented

| Area | Details |
|---|---|
| **One agent, one sandbox** | Every task attempt gets a fresh container/microVM, a unique random run nonce, a private workspace, its own network namespace and cgroup limits. Retries never reuse a sandbox. |
| **Durable queue** | SQLite WAL task queue with priority, tenant weights, tenant concurrency caps, capability/pool placement and `BEGIN IMMEDIATE` atomic claims. |
| **Distributed workers** | Workers pull leases over authenticated HTTP, heartbeat, stream results back, drain gracefully, and can disappear at any time. Expired leases are requeued with exponential backoff. |
| **Docker isolation** | Read-only rootfs, all capabilities dropped, `no-new-privileges`, non-root UID, `--pids-limit`, CPU quota, memory + swap hard caps, nofile limit, init process, private workspace mount. |
| **Strict egress** | Default `--network none`. Optional allowlist mode: a **per-task** internal Docker network plus a **per-task egress proxy** sidecar. The proxy validates a per-task token, allowlisted hostnames, ports 80/443, and rejects private/link-local/metadata IPs (DNS-rebinding safe). |
| **Firecracker** | Real microVM adapter: per-run writable rootfs, vCPU/memory config, vsock guest-agent protocol, no network device by default, VM destroyed after the run. See [docs/FIRECRACKER.md](docs/FIRECRACKER.md). |
| **Fairness** | Weighted fair queuing per tenant (virtual finish time), priority classes, and bucketed anti-starvation aging. |
| **Model catalog** | Import model metadata from OpenRouter (optionally **free models only**) or a local **Ollama** daemon. Metadata only — API keys are never accepted or stored. Dashboard buttons plus `POST /v1/models/import/*`. |
| **Secrets** | Config values can be `env:VAR` or `file:PATH` references; a missing reference fails startup instead of silently disabling auth. Bearer tokens are never written to config or the database. |
| **Observability** | JSON logs, lifecycle audit events, `/v1/stats`, Prometheus `/metrics`, and a zero-dependency live dashboard at `/dashboard`. |
| **Ops** | Bearer-token auth, submission rate limiting, body-size limits, graceful worker drain, manual retry, idempotency keys. |

**Validated:** 42 tests, including an end-to-end run of **120 concurrent agents across 4 worker processes** with unique sandboxes, plus cancellation, lease expiry, worker loss, tenant fairness, auth and sandbox-spec hardening tests. See [ARCHITECTURE.md](ARCHITECTURE.md) and [SECURITY.md](SECURITY.md).

## Quickstart

Requires Python 3.11+.

```bash
git clone https://github.com/sutharson-k/agentmesh.git
cd agentmesh
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -e ".[dev]"

agentmesh init                 # writes agentmesh.yaml
agentmesh server               # terminal 1: control plane on :8080
agentmesh worker --backend fake --slots 8   # terminal 2 (dev only, no isolation)
agentmesh submit --name hello --prompt "write a hello-world CLI" --wait --logs
```

Open <http://127.0.0.1:8080/dashboard> for the live fleet view.

### Run the 120-agent demo

No Docker or model credentials required; uses real control-plane + worker **processes** over HTTP with the fake sandbox backend:

```bash
python scripts/demo_swarm.py --tasks 120 --workers 4 --slots 32
```

It prints live queue/worker stats, verifies every agent got a distinct workspace and sandbox identity, prints a summary, and can keep the dashboard alive with `--hold`.

### Use real Docker sandboxes

Start Docker, then run workers on the Docker host:

```bash
agentmesh server --api-key "$AGENTMESH_TOKEN"
agentmesh worker --backend docker --slots 8 --cpu 8 --memory 16384 \
  --url http://127.0.0.1:8080 --token "$AGENTMESH_TOKEN"
```

Submit a real command (image must be in the worker allowlist):

```bash
agentmesh submit --name repo-task --image python:3.12-slim \
  --command python -c "print('hello from an isolated container')" --wait --logs
```

Submit an AI-agent task (configure the CLI template under `agents.commands` in YAML first):

```bash
agentmesh submit --name fix-tests --agent-kind claude \
  --prompt "Run the test suite and fix the failing tests. Do not touch CI config." \
  --allow api.anthropic.com --allow github.com --wait --logs
```

### Model catalog (Ollama + OpenRouter)

The dashboard has a **Models** panel with:

- **free models only** toggle — filters both the imported table and the OpenRouter import,
- **import openrouter** — pulls public pricing metadata; with the toggle on, only zero-priced models are stored,
- **import ollama** — point at a local daemon (default `http://127.0.0.1:11434`) and import installed models.

Imports never require or persist an API key. The Ollama URL must be loopback by default (`catalog.allow_remote_ollama: true` to opt out), so the control plane cannot be used as an SSRF pivot.

Using Ollama as the agent runtime needs a reachable endpoint **from inside the sandbox**:

```yaml
agents:
  commands:
    ollama: ["ollama", "run", "{model}", "{prompt}"]
  env:
    OLLAMA_HOST: http://host.docker.internal:11434
```

Then submit with `agent.kind: ollama` and `agent.model: qwen2.5-coder:7b`. Sandboxes are network-isolated by default, so add an egress policy (or run the endpoint where the worker can reach it) that permits the model server. The shipped egress proxy is hostname/443-oriented; for a private model server on the host network you will usually run a dedicated worker with a reachable proxy.

### Load testing (Rust)

`tools/loadgen` is a small Rust binary that submits thousands of tasks, samples queue depth, and verifies per-agent sandbox uniqueness:

```bash
cargo build --release --manifest-path tools/loadgen/Cargo.toml
./tools/loadgen/target/release/agentmesh-loadgen --url http://127.0.0.1:8080 --tasks 5000 --tenants 8
# machine-readable:
AGENTMESH_TOKEN=... ./tools/loadgen/target/release/agentmesh-loadgen --tasks 20000 --json
```

It exits non-zero on failures or duplicated workspaces, so it works as a CI performance gate. Pass the token via `AGENTMESH_TOKEN` or `--token-file`; avoid `--token`, which lands in shell history.

### Firecracker

Firecracker runs on Linux with KVM. Configure kernels/rootfs and build the guest agent into the image; see [docs/FIRECRACKER.md](docs/FIRECRACKER.md). Set `worker.backend: firecracker`. The adapter refuses to start rather than silently degrading isolation if `/dev/kvm` or images are missing.

## CLI

```
agentmesh server      # control plane
agentmesh worker      # sandbox worker
agentmesh submit      # single task or --file batch (--batch)
agentmesh status      # cluster + queue + workers + recent tasks
agentmesh task ID     # full task JSON; --events / --logs
agentmesh cancel ID   # cancel queued/running work
agentmesh retry ID    # requeue a failed task
agentmesh init        # starter config
```

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| POST | `/v1/tasks` | Submit task (supports `Idempotency-Key`) |
| POST | `/v1/tasks/batch` | Submit up to 1000 tasks |
| GET | `/v1/tasks?status=&tenant=&worker_id=&limit=&offset=` | List tasks |
| GET | `/v1/tasks/{id}` | Task detail |
| GET | `/v1/tasks/{id}/events` | Immutable lifecycle audit trail |
| GET | `/v1/tasks/{id}/logs` | Last log tail (bounded) |
| POST | `/v1/tasks/{id}/cancel` \| `/retry` | Cancel / requeue |
| POST | `/v1/workers/register` | Register capacity, pool, capabilities, labels |
| POST | `/v1/workers/{id}/heartbeat` | Renew leases, receive cancel signals |
| POST | `/v1/workers/{id}/lease` | Long-poll for work, respecting free capacity |
| POST | `/v1/workers/{id}/complete` | Report result (lease-guarded) |
| GET | `/v1/workers`, `/v1/workers/{id}` | Fleet inventory |
| POST | `/v1/workers/{id}/status` | `online` / `draining` / `offline` |
| GET/PUT | `/v1/tenants`, `/v1/tenants/{name}` | Fairness weights and concurrency caps |
| GET | `/v1/models?free_only=&provider=` | Catalog listing (metadata only) |
| POST | `/v1/models/import` \| `/import/openrouter` \| `/import/ollama` | Import metadata from a list, OpenRouter, or Ollama |
| DELETE | `/v1/models/{id}` | Remove a catalog entry |
| GET | `/v1/stats`, `/metrics` | JSON stats / Prometheus exposition |
| GET | `/dashboard` | Live operations dashboard |

Auth is a bearer token (or `X-API-Key`) when `server.api_keys` is non-empty. Workers use the same token. Health endpoints and the dashboard shell stay public.

## Configuration

All settings live in `agentmesh.yaml` (see `agentmesh.example.yaml`) and can be overridden with `AGENTMESH_*` environment variables or CLI flags. Tuning notes:

- `server.lease_ttl_s` (default 30) must exceed two worker heartbeat intervals.
- `server.heartbeat_timeout_s` (default 45) triggers fast worker-loss recovery.
- `worker.slots`, `cpu_total`, `memory_mb_total` define the advertised sandbox capacity; the lease endpoint will never oversubscribe it.
- `worker.docker.allowed_images` is a glob allowlist enforced **before** container creation. Do not add `*`.
- `server.tenant_max_concurrent` is the default per-tenant active cap; adjust per tenant via API.

## Metrics

`agentmesh_tasks{status}`, `agentmesh_active_leases`, `agentmesh_queue_depth`, `agentmesh_oldest_queued_age_seconds`, `agentmesh_workers{status}`, slot/CPU/memory capacity gauges, `agentmesh_events_total{event}` and build info. Suggested alerts: oldest-queued age rising while free slots > 0, workers offline, failure ratio, lease-expiry rate.

## Project layout

```
src/agentmesh/
  api.py                 control-plane HTTP + auth/rate limiting
  db.py                  schema, durable queue, leases, fairness, recovery, model catalog
  models.py              validated wire models
  catalog.py             OpenRouter + loopback-only Ollama metadata import
  cli.py                 server / worker / submit / status / task
  metrics.py             Prometheus rendering
  sandbox/docker_spec.py hardened container + network argv builders
  sandbox/egress_proxy.py per-task allowlist CONNECT proxy
  worker/daemon.py       pull, heartbeat, sandbox lifecycle, result reporting
  worker/backends/       docker.py, firecracker.py, fake.py
  agents/runtime.py      CLI templates + built-in mock agent
  guest/agent.py         Firecracker guest agent (vsock, archive return)
  web/dashboard.html     live control dashboard
tests/                   42 unit + integration + concurrency tests
scripts/demo_swarm.py    real multi-process 120-agent demo
tools/loadgen/           Rust load generator / benchmark
deploy/                  systemd units + control-plane Dockerfile
```

## Testing

```bash
pytest -q
```

The suite covers queue atomicity, capacity + capability placement, WFQ fairness, idempotency, retry/backoff, lease expiry, worker-loss recovery, cancellation, API auth/metrics, Docker argument hardening, proxy policy, Firecracker config, and a full 120-agent distributed run.

## Production hardening checklist

- Set `server.api_keys` and use TLS termination in front of the control plane.
- Run workers directly on isolated Linux hosts; never mount the Docker socket into task containers (AgentMesh does not).
- Use a dedicated Docker host per trust domain; enable user-namespace remapping and a custom seccomp profile for stronger multi-tenant isolation.
- Keep egress allowlists narrow; verify proxy audit logs (`docker logs` on the sidecar) and collect them centrally.
- Run one control-plane replica per SQLite database, or replace `Store` with Postgres for multi-node control planes.
- Pin task images by digest and mirror them on workers.

## License

MIT. See [LICENSE](LICENSE).
