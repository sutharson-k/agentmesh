"""Wire models and validation for task submissions and worker messages."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

IMAGE_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._/:@-]{0,254}$")
ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
DOMAIN_RE = re.compile(r"^(\*\.)?[a-zA-Z0-9]([a-zA-Z0-9.-]{0,251})$")
KIND_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
TENANT_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")


class NetworkPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["none", "egress_allowlist"] = "none"
    allow: list[str] = Field(default_factory=list, max_length=64)

    @field_validator("allow")
    @classmethod
    def _valid_domains(cls, domains: list[str]) -> list[str]:
        out = []
        for raw in domains:
            domain = raw.strip().lower().rstrip(".")
            if domain.startswith("*."):
                if not DOMAIN_RE.match(domain):
                    raise ValueError(f"invalid domain pattern: {raw!r}")
                out.append(domain)
            elif DOMAIN_RE.match(domain) and "." in domain:
                out.append(domain)
            else:
                raise ValueError(f"invalid domain: {raw!r}")
        return sorted(set(out))


class AgentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = "mock"
    prompt: str = Field(default="", max_length=200_000)
    model: str | None = Field(default=None, max_length=200)

    @field_validator("kind")
    @classmethod
    def _kind(cls, value: str) -> str:
        if not KIND_RE.match(value):
            raise ValueError("agent.kind must match [a-z0-9][a-z0-9_-]{0,31}")
        return value


class Requirements(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pool: str | None = Field(default=None, max_length=64)
    capabilities: list[str] = Field(default_factory=list, max_length=32)
    labels: dict[str, str] = Field(default_factory=dict)


class TaskSubmit(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    tenant: str = "default"
    priority: int = Field(default=50, ge=0, le=100)
    image: str = "python:3.12-slim"
    command: list[str] | None = Field(default=None, max_length=128)
    agent: AgentSpec | None = None
    env: dict[str, str] = Field(default_factory=dict)
    cpu: float = Field(default=1.0, gt=0, le=32)
    memory_mb: int = Field(default=512, ge=32, le=131_072)
    pids: int = Field(default=256, ge=16, le=8192)
    timeout_s: int = Field(default=900, ge=10, le=86_400)
    max_retries: int = Field(default=2, ge=0, le=10)
    network: NetworkPolicy = Field(default_factory=NetworkPolicy)
    require: Requirements = Field(default_factory=Requirements)
    labels: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, max_length=200)

    @field_validator("tenant")
    @classmethod
    def _tenant(cls, value: str) -> str:
        if not TENANT_RE.match(value):
            raise ValueError("tenant must match [a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}")
        return value

    @field_validator("image")
    @classmethod
    def _image(cls, value: str) -> str:
        if not IMAGE_RE.match(value) or ".." in value:
            raise ValueError("invalid image reference")
        return value

    @field_validator("command")
    @classmethod
    def _command(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("command must not be empty when supplied")
        for arg in value:
            if not isinstance(arg, str) or "\x00" in arg or len(arg) > 16_384:
                raise ValueError("command arguments must be strings without NUL bytes")
        if sum(len(a) for a in value) > 131_072:
            raise ValueError("command is too large")
        return value

    @field_validator("env")
    @classmethod
    def _env(cls, value: dict[str, str]) -> dict[str, str]:
        if len(value) > 64:
            raise ValueError("at most 64 environment variables")
        for key, val in value.items():
            if not ENV_KEY_RE.match(key):
                raise ValueError(f"invalid environment variable name: {key!r}")
            if "\x00" in val or len(val) > 32_768:
                raise ValueError(f"invalid value for {key}")
        return value

    @field_validator("labels", "require")
    @classmethod
    def _labels_size(cls, value: Any) -> Any:
        return value

    @model_validator(mode="after")
    def _needs_work(self) -> "TaskSubmit":
        if self.agent is None and self.command is None:
            raise ValueError("provide either `command` or `agent`")
        if self.agent is not None and self.command is not None:
            raise ValueError("provide only one of `command` or `agent`")
        if self.agent is not None and self.agent.kind == "mock" and not self.agent.prompt:
            raise ValueError("mock agent requires a prompt")
        if len(self.labels) > 32:
            raise ValueError("at most 32 labels")
        return self

    def stored_env(self) -> dict[str, str]:
        from .util import redact_env

        return redact_env(self.env)


class WorkerRegister(BaseModel):
    model_config = ConfigDict(extra="ignore")

    worker_id: str | None = Field(default=None, max_length=80)
    hostname: str = Field(default="unknown", max_length=200)
    pool: str = Field(default="default", max_length=64)
    slots: int = Field(default=8, ge=1, le=4096)
    cpu_total: float = Field(default=8, gt=0, le=4096)
    memory_mb_total: int = Field(default=8192, ge=128, le=4_194_304)
    capabilities: list[str] = Field(default_factory=list, max_length=64)
    labels: dict[str, str] = Field(default_factory=dict)
    backend: str = Field(default="unknown", max_length=32)
    version: str = Field(default="0.1.0", max_length=32)


class RunningTask(BaseModel):
    task_id: str
    lease_id: str


class Heartbeat(BaseModel):
    model_config = ConfigDict(extra="ignore")

    running: list[RunningTask] = Field(default_factory=list, max_length=4096)
    free_cpu: float = Field(default=0, ge=0)
    free_mem_mb: int = Field(default=0, ge=0)
    free_slots: int = Field(default=0, ge=0)


class LeaseRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wait_s: float = Field(default=0, ge=0, le=30)
    free_cpu: float = Field(default=0, ge=0)
    free_mem_mb: int = Field(default=0, ge=0)
    free_slots: int = Field(default=0, ge=0)


class CompleteRequest(BaseModel):
    model_config = ConfigDict(extra="ignore")

    task_id: str
    lease_id: str
    outcome: Literal["succeeded", "failed", "timeout", "canceled"]
    exit_code: int | None = None
    error: str | None = Field(default=None, max_length=4000)
    logs_tail: str = Field(default="", max_length=200_000)
    result: dict[str, Any] = Field(default_factory=dict)
    peak_memory_mb: int | None = None
    cpu_seconds: float | None = None
    duration_s: float | None = None


class TenantLimits(BaseModel):
    weight: float = Field(default=1.0, gt=0, le=100)
    max_concurrent: int = Field(default=64, ge=1, le=4096)


class ModelEntry(BaseModel):
    """A model imported into the catalog (never contains credentials)."""

    model_config = ConfigDict(extra="ignore")

    provider: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=300)
    free: bool = False
    context_length: int | None = Field(default=None, ge=0, le=100_000_000)
    size_bytes: int | None = Field(default=None, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider")
    @classmethod
    def _provider(cls, value: str) -> str:
        value = value.strip().lower()
        if not re.match(r"^[a-z0-9][a-z0-9_-]{0,63}$", value):
            raise ValueError("provider must match [a-z0-9][a-z0-9_-]{0,63}")
        return value


class ModelImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    models: list[ModelEntry] = Field(min_length=1, max_length=5000)
    replace: bool = False


class OpenRouterImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    free_only: bool = True


class OllamaImportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str | None = Field(default=None, max_length=500)


class TaskQuery(BaseModel):
    status: str | None = None
    tenant: str | None = None
    worker_id: str | None = None
    limit: int = Field(default=50, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

    def statuses(self) -> list[str] | None:
        if not self.status:
            return None
        allowed = {
            "queued",
            "leased",
            "running",
            "succeeded",
            "failed",
            "canceled",
            "timed_out",
        }
        values = [s.strip() for s in self.status.split(",") if s.strip()]
        bad = [s for s in values if s not in allowed]
        if bad:
            raise ValueError(f"unknown status filter: {', '.join(bad)}")
        return values
