# Deployment notes

The control plane is safe to containerize. **Workers should normally run directly on the sandbox host** because they need access to the Docker daemon (or `/dev/kvm` for Firecracker). Mounting the Docker socket into a worker container would give that container root-equivalent power; avoid it unless the worker container itself is treated as a host-level trust boundary.

## Control plane

```bash
docker build -f deploy/Dockerfile.control -t agentmesh-control .
docker run -d --name agentmesh -p 8080:8080 \
  -e AGENTMESH_API_KEYS="$AGENTMESH_TOKEN" \
  -v agentmesh-data:/var/lib/agentmesh \
  agentmesh-control
```

Or install the systemd unit:

```bash
sudo useradd --system --home /opt/agentmesh agentmesh
sudo python -m venv /opt/agentmesh/.venv
sudo /opt/agentmesh/.venv/bin/pip install /path/to/agentmesh
sudo install -m 600 agentmesh.example.yaml /etc/agentmesh/agentmesh.yaml
sudo install -m 600 /dev/null /etc/agentmesh/control.env
echo 'AGENTMESH_API_KEYS=replace-with-a-long-token' | sudo tee /etc/agentmesh/control.env
sudo cp deploy/agentmesh-control.service /etc/systemd/system/
sudo systemctl enable --now agentmesh-control
```

## Worker hosts

```bash
sudo groupadd -f agentmesh
sudo useradd --system --gid agentmesh --groups docker --home /opt/agentmesh agentmesh
sudo install -d -o agentmesh -g agentmesh /var/lib/agentmesh /srv/agentmesh
# install the package into /opt/agentmesh/.venv as above, configure /etc/agentmesh/agentmesh.yaml
echo 'AGENTMESH_TOKEN=replace-with-the-same-token' | sudo tee /etc/agentmesh/worker.env
sudo cp deploy/agentmesh-worker.service /etc/systemd/system/
sudo systemctl enable --now agentmesh-worker
```

Tune `worker.slots`, `cpu_total` and `memory_mb_total` to the host. Keep headroom for the OS and the egress proxy sidecars (AgentMesh accounts for ~0.25 CPU and 96 MB per allowlist task when reporting free capacity).

For Firecracker workers, also grant access to `/dev/kvm` and follow [../docs/FIRECRACKER.md](../docs/FIRECRACKER.md).
