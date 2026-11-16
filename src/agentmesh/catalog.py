"""Model catalog sources.

Only *metadata* is imported. Provider credentials are never accepted or
persisted here: OpenRouter's catalog is public and Ollama is a local endpoint,
so importing models never requires storing an API key.

Requests to Ollama are loopback-only by default to avoid turning the control
plane into an SSRF pivot. Set ``catalog.allow_remote_ollama`` (or
``AGENTMESH_ALLOW_REMOTE_OLLAMA=1``) only when the endpoint is trusted.
"""

from __future__ import annotations

import ipaddress
from typing import Any
from urllib.parse import urlparse

import httpx

DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434"


class CatalogError(RuntimeError):
    pass


def _validate_ollama_url(base_url: str, *, allow_remote: bool) -> str:
    raw = (base_url or DEFAULT_OLLAMA_URL).strip().rstrip("/")
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise CatalogError("ollama URL must be an http(s) URL with a host")
    if parsed.username or parsed.password:
        raise CatalogError("ollama URL must not embed credentials")
    if not allow_remote:
        host = parsed.hostname
        loopback = host == "localhost"
        if not loopback:
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = False
        if not loopback:
            raise CatalogError(
                "ollama URL must be loopback (localhost/127.0.0.1); "
                "set catalog.allow_remote_ollama=true to allow remote endpoints"
            )
    if parsed.port is None:
        host = parsed.hostname
        if ":" in host:  # IPv6 literal
            host = f"[{host}]"
        raw = f"{parsed.scheme}://{host}:11434"
    return raw


def fetch_ollama_models(
    base_url: str = DEFAULT_OLLAMA_URL,
    *,
    allow_remote: bool = False,
    timeout_s: float = 15.0,
) -> list[dict[str, Any]]:
    """Read installed models from an Ollama daemon (``/api/tags``)."""
    base = _validate_ollama_url(base_url, allow_remote=allow_remote)
    url = f"{base}/api/tags"
    try:
        with httpx.Client(timeout=timeout_s) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError as exc:
        raise CatalogError(f"cannot reach Ollama at {base}: {exc}") from exc
    except ValueError as exc:
        raise CatalogError(f"Ollama at {base} returned invalid JSON") from exc

    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list):
        raise CatalogError(f"Ollama at {base} returned no models list")
    entries: list[dict[str, Any]] = []
    for model in models:
        if not isinstance(model, dict):
            continue
        name = model.get("name") or model.get("model")
        if not name:
            continue
        details = model.get("details") if isinstance(model.get("details"), dict) else {}
        entries.append(
            {
                "provider": "ollama",
                "name": str(name),
                "free": True,  # runs locally; no per-token billing
                "size_bytes": model.get("size") if isinstance(model.get("size"), int) else None,
                "context_length": None,
                "metadata": {
                    "family": details.get("family"),
                    "parameter_size": details.get("parameter_size"),
                    "quantization_level": details.get("quantization_level"),
                    "modified_at": model.get("modified_at"),
                },
            }
        )
    return entries


def fetch_openrouter_models(
    *,
    url: str = "https://openrouter.ai/api/v1/models",
    free_only: bool = True,
    timeout_s: float = 15.0,
) -> list[dict[str, Any]]:
    """Read OpenRouter's public catalog, optionally only zero-priced models."""
    try:
        with httpx.Client(timeout=timeout_s, headers={"User-Agent": "agentmesh-catalog"}) as client:
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
    except httpx.HTTPError as exc:
        raise CatalogError(f"cannot reach OpenRouter: {exc}") from exc
    except ValueError as exc:
        raise CatalogError("OpenRouter returned invalid JSON") from exc

    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise CatalogError("OpenRouter returned no model list")
    entries: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("id"):
            continue
        pricing = row.get("pricing") if isinstance(row.get("pricing"), dict) else {}
        prompt_price = _price(pricing.get("prompt"))
        completion_price = _price(pricing.get("completion"))
        is_free = prompt_price == 0.0 and completion_price == 0.0
        if free_only and not is_free:
            continue
        entries.append(
            {
                "provider": "openrouter",
                "name": str(row["id"]),
                "free": is_free,
                "context_length": row.get("context_length")
                if isinstance(row.get("context_length"), int)
                else None,
                "size_bytes": None,
                "metadata": {
                    "label": row.get("name"),
                    "description": (row.get("description") or "")[:500],
                    "pricing": {
                        "prompt": prompt_price,
                        "completion": completion_price,
                    },
                },
            }
        )
    return entries


def _price(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
