"""Per-task egress allowlist proxy.

Runs inside a minimal sidecar container on a per-task internal Docker network.
The task container has no route to the internet; this proxy is its only exit.
It enforces a hostname allowlist, blocks private/metadata IPs, limits ports to
80/443, requires a per-task bearer token, and emits a JSON audit line for every
decision.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import json
import os
import socket
import sys
import time
from typing import Iterable

MAX_HEADER_BYTES = 64 * 1024
CONNECT_TIMEOUT = 10.0
IDLE_TIMEOUT = 120.0
MAX_CLIENTS = 256
ALLOWED_PORTS = {80, 443}
BLOCKED_NETS = [
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
]


def log(kind: str, **fields: object) -> None:
    payload = {"ts": round(time.time(), 3), "kind": kind, **fields}
    print(json.dumps(payload, separators=(",", ":")), flush=True)


class Policy:
    def __init__(self, domains: Iterable[str], token: str) -> None:
        self.domains = {d.strip().lower().rstrip(".") for d in domains if d.strip()}
        self.token = token

    def allows_domain(self, host: str) -> bool:
        host = host.strip().lower().rstrip(".")
        if not host or len(host) > 253:
            return False
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            # IP literals are always denied; DNS names only.
            _ = ip
            return False
        for pattern in self.domains:
            if pattern.startswith("*."):
                base = pattern[2:]
                if host == base or host.endswith("." + base):
                    return True
            elif host == pattern:
                return True
        return False

    def allows_port(self, port: int) -> bool:
        return port in ALLOWED_PORTS

    def authorized(self, header_value: str | None) -> bool:
        if not self.token:
            return True
        if not header_value:
            return False
        scheme, _, encoded = header_value.partition(" ")
        if scheme.lower() != "basic" or not encoded:
            return False
        try:
            decoded = base64.b64decode(encoded).decode("utf-8", "replace")
        except Exception:
            return False
        username, _, password = decoded.partition(":")
        supplied = password or username
        import hmac

        return hmac.compare_digest(supplied, self.token)


async def resolve_public(host: str, port: int) -> list[str]:
    loop = asyncio.get_running_loop()
    infos = await asyncio.wait_for(
        loop.getaddrinfo(host, port, type=socket.SOCK_STREAM), timeout=CONNECT_TIMEOUT
    )
    addresses: list[str] = []
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if any(ip in net for net in BLOCKED_NETS) or not ip.is_global:
            raise PermissionError(f"resolved to blocked address {ip}")
        addresses.append(addr)
    if not addresses:
        raise OSError("no public address")
    return addresses


async def read_head(reader: asyncio.StreamReader) -> tuple[str, dict[str, str]]:
    raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=15)
    if len(raw) > MAX_HEADER_BYTES:
        raise ValueError("headers too large")
    text = raw.decode("iso-8859-1")
    lines = text.split("\r\n")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        key, _, value = line.partition(":")
        headers[key.strip().lower()] = value.strip()
    return lines[0], headers


async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, meter: list[int]) -> None:
    try:
        while True:
            chunk = await asyncio.wait_for(reader.read(65_536), timeout=IDLE_TIMEOUT)
            if not chunk:
                break
            meter[0] += len(chunk)
            writer.write(chunk)
            await writer.drain()
    except (asyncio.TimeoutError, ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def connect_any(host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    addresses = await resolve_public(host, port)
    last: Exception | None = None
    for addr in addresses:
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(addr, port), timeout=CONNECT_TIMEOUT
            )
        except (OSError, asyncio.TimeoutError) as exc:
            last = exc
    raise OSError(f"connect failed: {last}")


async def handle(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    policy: Policy,
    semaphore: asyncio.Semaphore,
) -> None:
    peer = writer.get_extra_info("peername")
    started = time.time()
    client_tag = f"{peer[0]}:{peer[1]}" if peer else "?"
    try:
        async with semaphore:
            try:
                request_line, headers = await read_head(reader)
            except Exception:
                return
            parts = request_line.split(" ")
            if len(parts) != 3:
                writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            method, target, version = parts

            if not policy.authorized(headers.get("proxy-authorization")):
                log("denied", reason="proxy_auth", client=client_tag, target=target)
                writer.write(
                    b"HTTP/1.1 407 Proxy Authentication Required\r\n"
                    b"Proxy-Authenticate: Basic realm=\"agentmesh\"\r\n"
                    b"Content-Length: 0\r\n\r\n"
                )
                await writer.drain()
                return

            if method.upper() == "CONNECT":
                host, _, port_raw = target.partition(":")
                port = int(port_raw or "443") if port_raw.isdigit() else 443
                if not policy.allows_domain(host) or not policy.allows_port(port):
                    log("denied", reason="allowlist", client=client_tag, host=host, port=port)
                    writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                    await writer.drain()
                    return
                try:
                    upstream_r, upstream_w = await connect_any(host, port)
                except Exception as exc:
                    log("denied", reason="connect_failed", client=client_tag, host=host, port=port, error=str(exc))
                    writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                    await writer.drain()
                    return
                log("allowed", method="CONNECT", client=client_tag, host=host, port=port)
                writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                await writer.drain()
                sent, received = [0], [0]
                await asyncio.gather(
                    pipe(reader, upstream_w, sent),
                    pipe(upstream_r, writer, received),
                )
                log(
                    "closed",
                    method="CONNECT",
                    client=client_tag,
                    host=host,
                    sent_bytes=sent[0],
                    received_bytes=received[0],
                    duration_s=round(time.time() - started, 3),
                )
                return

            # Plain HTTP proxy request: absolute URI required.
            if not target.startswith("http://"):
                writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            rest = target[len("http://") :]
            authority, _, path = rest.partition("/")
            host, _, port_raw = authority.partition(":")
            port = int(port_raw) if port_raw.isdigit() else 80
            if not policy.allows_domain(host) or not policy.allows_port(port):
                log("denied", reason="allowlist", client=client_tag, host=host, port=port, method=method)
                writer.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            try:
                upstream_r, upstream_w = await connect_any(host, port)
            except Exception as exc:
                log("denied", reason="connect_failed", client=client_tag, host=host, error=str(exc))
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                return
            log("allowed", method=method, client=client_tag, host=host, port=port, path="/" + path)
            safe_headers = {
                k: v
                for k, v in headers.items()
                if k not in {"proxy-authorization", "proxy-connection", "connection"}
            }
            safe_headers["connection"] = "close"
            request = f"{method} /{path} {version}\r\n" + "".join(
                f"{k}: {v}\r\n" for k, v in safe_headers.items()
            ) + "\r\n"
            upstream_w.write(request.encode("iso-8859-1"))
            await upstream_w.drain()
            sent, received = [0], [0]
            await asyncio.gather(
                pipe(reader, upstream_w, sent),
                pipe(upstream_r, writer, received),
            )
            log(
                "closed",
                method=method,
                client=client_tag,
                host=host,
                sent_bytes=sent[0],
                received_bytes=received[0],
                duration_s=round(time.time() - started, 3),
            )
    except Exception as exc:  # never crash the sidecar on a bad request
        log("error", client=client_tag, error=repr(exc))
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main() -> None:
    allow = [d for d in os.environ.get("AGENTMESH_EGRESS_ALLOW", "").split(",") if d]
    token = os.environ.get("AGENTMESH_EGRESS_TOKEN", "")
    port = int(os.environ.get("AGENTMESH_EGRESS_PORT", "3128"))
    policy = Policy(allow, token)
    semaphore = asyncio.Semaphore(MAX_CLIENTS)
    server = await asyncio.start_server(
        lambda r, w: handle(r, w, policy, semaphore), host="0.0.0.0", port=port, backlog=256
    )
    log("listening", port=port, domains=sorted(policy.domains), auth=bool(token))
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
