# Security model

AgentMesh assumes that **the code and prompts executed by an agent are untrusted**. A coding agent can read its prompt, fetch dependencies, run arbitrary commands and try to attack its neighbours. The orchestrator's job is to constrain blast radius and make escapes require a kernel or daemon vulnerability.

## Trust boundaries

| Boundary | Control |
|---|---|
| Control-plane API | Bearer token (constant-time compare). Unset keys = open mode, logged loudly, development only. |
| Worker → control plane | Same bearer token; lease ids are unguessable and bound to worker + task. |
| Task container ↔ host | Read-only rootfs, one private workspace bind mount, all capabilities dropped, `no-new-privileges`, non-root UID, default seccomp, no devices, no Docker socket. |
| Task ↔ task | Separate PID/IPC/UTS/mount/net namespaces and cgroups. In allowlist mode, a dedicated internal Docker network per task with exactly two peers (agent + egress proxy). Workspaces are never shared. |
| Task ↔ internet | `--network none` by default. Allowlist mode has no route off-host except a per-task proxy sidecar that enforces domain/port/token policy and rejects private/metadata IPs. |
| Worker ↔ task | One sandbox per task; the worker never executes agent code in its own process. `fake` backend is explicitly non-isolating. |
| Firecracker guest ↔ host | Hardware virtualization boundary, no network device by default, per-VM writable rootfs copy, vsock-only control channel, VM and disk destroyed after the run. |

## What is enforced per agent

**Namespaces / kernel:** fresh container (PID, mount, UTS, IPC, network) or microVM. Docker default seccomp profile. `/proc`, `/sys` and host paths are not mounted. No privileged mode, no host namespaces, no device passthrough.

**Resources:** `--cpus`, `--memory` with `--memory-swap` equal (swap disabled), `--pids-limit`, `--ulimit nofile`, `--shm-size`, read-only root plus bounded tmpfs. Firecracker uses `vcpu_count` and `mem_size_mib`.

**Filesystem:** read-only root; a single bind mount at `/workspace` sourced from a fresh random per-run directory; `/tmp` is `noexec,nosuid,nodev`, 64 MB. Artifacts are collected from that workspace only.

**Network:** default no interfaces at all. In allowlist mode the container is placed on an `--internal` bridge network created uniquely for that run. A sidecar proxy is the only other member and the only route to the uplink network. The proxy:

- requires a per-task Basic proxy token,
- allows only listed hostnames (`*.example.com` includes the apex), ports 80/443,
- rejects IP literals and hostnames resolving to loopback/private/link-local/CGNAT/metadata space,
- resolves and validates before every connection (mitigates DNS rebinding),
- emits a JSON audit line for every allow/deny/close.

Applications that ignore `HTTP(S)_PROXY` simply have no route; the policy fails closed.

## Secrets

Task env values whose keys look like secrets (`TOKEN`, `SECRET`, `PASSWORD`, `API_KEY`, `AUTHORIZATION`) are redacted in API projections and events. The full env is stored in the database and passed to the sandbox because it is required for execution: protect the control-plane database accordingly. Prompt previews are truncated in listings. Logs may contain secrets emitted by the agent; treat `/v1/tasks/{id}/logs` and workspace artifacts as sensitive.

## Known limitations

- **Docker shares the host kernel.** A kernel or container-runtime escape compromises the host. For hostile multi-tenant workloads, use Firecracker or one Docker host per trust domain, and enable user-namespace remapping (`userns-remap`) on the daemon.
- The default worker does **not** enable a custom seccomp profile. Set `worker.docker.seccomp_profile` to a tightened profile matched to your agent images.
- The workspace bind mount is writable by the sandbox UID. On Linux, the worker relaxes directory permissions so the non-root container user can write; other local host users may be able to read the path depending on umask. Isolate worker hosts and use dedicated users/filesystems.
- The control plane has a single shared bearer-token trust tier; there is no per-tenant authentication/authorization. Put tenant-facing submission behind your own authz layer if needed.
- The proxy only prevents egress through the provided route. Kernel-level side channels (timing, CPU contention) are not mitigated beyond cgroup limits.
- Audit logs from the proxy live in Docker's json-file log for the sidecar (size-capped) and are summarized into the task result; ship them to a central log store for retention.
- Firecracker support requires a prepared guest image containing `agentmesh.guest.agent`; see [FIRECRACKER.md](FIRECRACKER.md).

## Hardening checklist

1. Set `server.api_keys` and terminate TLS in front of the API.
2. Run workers on dedicated Linux hosts; keep the Docker daemon root-only.
3. Keep `worker.docker.allowed_images` as a short digest-pinned allowlist.
4. Prefer `network.mode=none`; keep egress allowlists minimal and reviewed.
5. Enable userns-remap + custom seccomp if co-tenancy is hostile.
6. Restrict control-plane DB and worker workspace permissions to the service user.
7. Monitor `lease_expired` / `worker_lost` rates and egress denials.
8. Never select the `fake` backend in production; `auto` refuses to fall back to it.
