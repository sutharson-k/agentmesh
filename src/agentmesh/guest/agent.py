"""AgentMesh guest agent for Firecracker microVMs.

Bake this into the VM root filesystem and run it at boot as root:

    python3 /opt/agentmesh/guest_agent.py

It listens on AF_VSOCK port 52000, accepts one JSON run request, streams
stdout/stderr back as length-prefixed frames, tars the workspace at the end,
and exits. The host side is ``agentmesh.worker.backends.firecracker``.

Frame format: 4-byte big-endian length + UTF-8 JSON object.
This file is stdlib-only and has no dependency on the control plane.
"""

from __future__ import annotations

import base64
import io
import json
import os
import socket
import struct
import subprocess
import sys
import tarfile
import threading
import time

PORT = int(os.environ.get("AGENTMESH_GUEST_PORT", "52000"))
ARCHIVE_LIMIT = 32 * 1024 * 1024


def send(sock: socket.socket, obj: dict) -> None:
    payload = json.dumps(obj).encode()
    sock.sendall(struct.pack(">I", len(payload)) + payload)


def recv(sock: socket.socket) -> dict:
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            raise ConnectionError("closed")
        header += chunk
    (length,) = struct.unpack(">I", header)
    if length > 64 * 1024 * 1024:
        raise ValueError("frame too large")
    data = b""
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data += chunk
    return json.loads(data)


def archive_workspace(workspace: str) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tar:
        if not os.path.isdir(workspace):
            return b""
        for root, dirs, files in os.walk(workspace):
            dirs[:] = [d for d in dirs if d not in {".git", "node_modules", "__pycache__"}]
            for name in files:
                path = os.path.join(root, name)
                try:
                    if os.path.getsize(path) > 4 * 1024 * 1024 or os.path.islink(path):
                        continue
                    tar.add(path, arcname=os.path.relpath(path, workspace))
                except OSError:
                    continue
    data = buf.getvalue()
    return data if len(data) <= ARCHIVE_LIMIT else b""


def handle(sock: socket.socket) -> None:
    req = recv(sock)
    if req.get("type") != "run":
        send(sock, {"type": "error", "message": "expected run request"})
        return
    argv = req.get("argv") or []
    env = {str(k): str(v) for k, v in (req.get("env") or {}).items()}
    cwd = req.get("cwd") or "/workspace"
    timeout_s = int(req.get("timeout_s") or 900)
    os.makedirs(cwd, exist_ok=True)
    try:
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            env={**os.environ, **env},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except Exception as exc:
        send(sock, {"type": "error", "message": f"spawn failed: {exc}"})
        return

    lock = threading.Lock()

    def pump(stream, kind: str) -> None:
        while True:
            chunk = stream.read(65_536)
            if not chunk:
                break
            with lock:
                try:
                    send(sock, {"type": kind, "data": base64.b64encode(chunk).decode()})
                except OSError:
                    return

    threads = [
        threading.Thread(target=pump, args=(proc.stdout, "stdout"), daemon=True),
        threading.Thread(target=pump, args=(proc.stderr, "stderr"), daemon=True),
    ]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + timeout_s
    while proc.poll() is None:
        if time.monotonic() > deadline:
            import signal

            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except OSError:
                proc.kill()
            break
        time.sleep(0.05)
    code = proc.wait()
    for thread in threads:
        thread.join(timeout=2)
    if req.get("archive"):
        raw = archive_workspace(cwd)
        if raw:
            with lock:
                send(
                    sock,
                    {
                        "type": "archive",
                        "name": "workspace.tar.gz",
                        "data": base64.b64encode(raw).decode(),
                    },
                )
    send(sock, {"type": "exit", "code": code})


def main() -> None:
    server = socket.socket(socket.AF_VSOCK, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((socket.VMADDR_CID_ANY, PORT))
    server.listen(4)
    print(f"agentmesh guest agent listening on vsock {PORT}", flush=True)
    while True:
        conn, _ = server.accept()
        try:
            handle(conn)
        except Exception as exc:
            try:
                send(conn, {"type": "error", "message": repr(exc)})
            except OSError:
                pass
        finally:
            conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
