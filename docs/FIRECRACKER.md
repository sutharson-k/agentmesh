# Firecracker backend

The Firecracker adapter gives every agent run a real microVM: separate guest kernel, writable rootfs copy, fixed vCPU/memory, vsock control channel and **no network device by default**. It is aimed at hostile multi-tenant workloads where sharing the host kernel with Docker is not acceptable.

The adapter is available on Linux only and refuses to start if KVM or the artifacts are missing. Docker remains the default, fully exercised backend in CI.

## Host prerequisites

```bash
# Ubuntu 24.04 example
sudo apt install -y qemu-utils curl
curl -fsSL https://github.com/firecracker-microvm/firecracker/releases/latest/download/firecracker-v1.10.1-x86_64.tgz \
  | tar -xz -C /tmp
sudo install /tmp/release-v1.10.1-x86_64/firecracker-v1.10.1-x86_64 /usr/local/bin/firecracker
ls -l /dev/kvm          # must exist and be accessible to the worker user
```

The worker process needs permission to read `/dev/kvm`, create TAP devices (not used in network-none mode), and write to `firecracker.run_root`.

## Build the guest rootfs

The guest must boot, bring up networking only if you add it later, and run the bundled guest agent on vsock port 52000.

1. Create an ext4 rootfs on a Debian/Ubuntu cloud image using `debootstrap` or `mmdebstrap`.
2. Install Python 3 (the guest agent is stdlib-only) and any toolchain your agents need.
3. Copy the guest agent into the image and install a systemd unit:

```ini
# /etc/systemd/system/agentmesh-guest.service
[Unit]
Description=AgentMesh guest agent
After=network.target

[Service]
ExecStart=/usr/bin/python3 /opt/agentmesh/guest_agent.py
Restart=always
[Install]
WantedBy=multi-user.target
```

```bash
sudo mkdir -p /mnt/rootfs
sudo mount -o loop rootfs.ext4 /mnt/rootfs
sudo install -D src/agentmesh/guest/agent.py /mnt/rootfs/opt/agentmesh/guest_agent.py
# enable the unit offline with systemctl --root, then unmount
sudo umount /mnt/rootfs
```

4. Extract a kernel image (`vmlinux`) matching the distribution.
5. Place artifacts on the worker and point the config at them:

```yaml
worker:
  backend: firecracker
  firecracker:
    enabled: true
    firecracker_bin: /usr/local/bin/firecracker
    kernel_image: /var/lib/agentmesh/vmlinux
    rootfs_image: /var/lib/agentmesh/rootfs.ext4
    run_root: /srv/agentmesh/firecracker
    vcpu: 2
    memory_mb: 1024
    guest_agent_port: 52000
```

## Run lifecycle

1. Create a unique run directory (`run_root/<worker>-<random>`).
2. Copy the base rootfs with `cp --reflink=auto` so the VM gets a private writable disk.
3. Write the Firecracker config (`boot-source`, `drives`, `machine-config`, `vsock`, `logger`), boot with `--config-file`.
4. Connect to the vsock UDS and send `CONNECT 52000`, then a length-prefixed JSON run request with argv, env, cwd, timeout and archive flag.
5. The guest agent streams stdout/stderr frames and returns a gzipped tar of `/workspace`.
6. Kill the VM and delete the run directory. Nothing is reused.

Guest protocol:

```
4-byte big-endian length + JSON
host → guest: {"type":"run","argv":[...],"env":{...},"cwd":"/workspace","timeout_s":900}
guest → host: {"type":"stdout","data":"<base64>"}
              {"type":"stderr","data":"<base64>"}
              {"type":"archive","name":"workspace.tar.gz","data":"<base64>"}
              {"type":"exit","code":0}
              {"type":"error","message":"..."}
```

## Security properties

- Hardware virtualization boundary instead of a shared kernel.
- No network interface is configured; the current adapter rejects `network.mode=egress_allowlist` rather than pretending to enforce it.
- Per-run writable disk; the base image is never booted directly.
- Only the workspace archive and logs cross back to the host; archive paths are validated against traversal and symlinks/links are skipped on extraction.
- vCPU and memory ceilings are enforced by the VMM.

## Current limitations

- Network-none only. Adding egress requires TAP + NAT + a broker; not wired into the worker yet.
- No jailer/chroot integration; run workers on dedicated hosts and consider invoking `jailer` in your own wrapper for production.
- `cp --reflink` is filesystem-dependent; without reflink support, each boot copies the whole image (correct but slow).
- Boot latency (seconds) makes Firecracker better for longer agent sessions than sub-second jobs.
- Guest image maintenance is your responsibility; pin it like a container image.
