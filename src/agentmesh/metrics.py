"""Prometheus text exposition without external dependencies."""

from __future__ import annotations

from typing import Any


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_metrics(stats: dict[str, Any], counters: dict[str, int], *, version: str) -> str:
    lines: list[str] = []
    tasks = stats["tasks"]
    workers = stats["workers"]

    lines += [
        "# HELP agentmesh_tasks Current number of tasks by state.",
        "# TYPE agentmesh_tasks gauge",
    ]
    for status in (
        "queued",
        "leased",
        "running",
        "succeeded",
        "failed",
        "timed_out",
        "canceled",
    ):
        lines.append(f'agentmesh_tasks{{status="{status}"}} {tasks["by_status"].get(status, 0)}')

    lines += [
        "# HELP agentmesh_tasks_total Total tasks known to the control plane.",
        "# TYPE agentmesh_tasks_total gauge",
        f"agentmesh_tasks_total {tasks['total']}",
        "# HELP agentmesh_queue_depth Queued tasks waiting for a worker.",
        "# TYPE agentmesh_queue_depth gauge",
        f"agentmesh_queue_depth {tasks['queued']}",
        "# HELP agentmesh_active_leases Tasks currently leased to workers.",
        "# TYPE agentmesh_active_leases gauge",
        f"agentmesh_active_leases {tasks['active']}",
        "# HELP agentmesh_oldest_queued_age_seconds Age of the oldest queued task.",
        "# TYPE agentmesh_oldest_queued_age_seconds gauge",
        f"agentmesh_oldest_queued_age_seconds {tasks['oldest_queued_age_s']}",
    ]

    lines += [
        "# HELP agentmesh_workers Number of registered workers by status.",
        "# TYPE agentmesh_workers gauge",
    ]
    for status in ("online", "draining", "offline"):
        lines.append(f'agentmesh_workers{{status="{status}"}} {workers.get(status, 0)}')
    lines += [
        "# HELP agentmesh_capacity_slots Total sandbox slots on online workers.",
        "# TYPE agentmesh_capacity_slots gauge",
        f"agentmesh_capacity_slots {workers['capacity_slots']}",
        "# HELP agentmesh_free_slots Free sandbox slots on online workers.",
        "# TYPE agentmesh_free_slots gauge",
        f"agentmesh_free_slots {workers['free_slots']}",
        "# HELP agentmesh_capacity_cpu Total CPU cores on online workers.",
        "# TYPE agentmesh_capacity_cpu gauge",
        f"agentmesh_capacity_cpu {workers['capacity_cpu']}",
        "# HELP agentmesh_free_cpu Unallocated CPU cores on online workers.",
        "# TYPE agentmesh_free_cpu gauge",
        f"agentmesh_free_cpu {workers['free_cpu']}",
        "# HELP agentmesh_capacity_memory_mb Total memory on online workers.",
        "# TYPE agentmesh_capacity_memory_mb gauge",
        f"agentmesh_capacity_memory_mb {workers['capacity_memory_mb']}",
        "# HELP agentmesh_free_memory_mb Unallocated memory on online workers.",
        "# TYPE agentmesh_free_memory_mb gauge",
        f"agentmesh_free_memory_mb {workers['free_memory_mb']}",
    ]

    if tasks["avg_queue_wait_s"] is not None:
        lines += [
            "# HELP agentmesh_avg_queue_wait_seconds Average queue wait of recent completions.",
            "# TYPE agentmesh_avg_queue_wait_seconds gauge",
            f"agentmesh_avg_queue_wait_seconds {tasks['avg_queue_wait_s']}",
        ]
    if tasks["avg_duration_s"] is not None:
        lines += [
            "# HELP agentmesh_avg_duration_seconds Average runtime of recent completions.",
            "# TYPE agentmesh_avg_duration_seconds gauge",
            f"agentmesh_avg_duration_seconds {tasks['avg_duration_s']}",
        ]

    lines += [
        "# HELP agentmesh_events_total Lifecycle counters.",
        "# TYPE agentmesh_events_total counter",
    ]
    for name, value in sorted(counters.items()):
        lines.append(f'agentmesh_events_total{{event="{_escape(name)}"}} {value}')

    lines += [
        "# HELP agentmesh_info Build information.",
        "# TYPE agentmesh_info gauge",
        f'agentmesh_info{{version="{_escape(version)}"}} 1',
    ]
    return "\n".join(lines) + "\n"
