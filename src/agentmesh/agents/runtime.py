"""Agent command resolution and the built-in mock coding agent.

Every task runs exactly one agent process inside exactly one sandbox. The mock
agent is deliberately deterministic so the full distributed pipeline can be
tested and demoed without Docker or model credentials.
"""

from __future__ import annotations

import json
from typing import Any

MOCK_AGENT_SCRIPT = r'''
import json, os, sys, time, hashlib, pathlib

task_id = os.environ.get("AGENTMESH_TASK_ID", "unknown")
prompt_path = pathlib.Path("/workspace/.agentmesh/prompt.txt")
prompt = prompt_path.read_text(encoding="utf-8", errors="replace") if prompt_path.exists() else ""
delay = float(os.environ.get("AGENTMESH_MOCK_DELAY", "0.35"))
fail = "[fail]" in prompt

def emit(msg):
    print(msg, flush=True)

emit(f"agent={task_id} mock-start")
emit(f"prompt_chars={len(prompt)}")
steps = ["inspect workspace", "plan patch", "apply change", "run checks", "summarize"]
for i, step in enumerate(steps, 1):
    if delay:
        time.sleep(delay / len(steps))
    emit(f"step {i}/5 {step}")

if fail:
    emit("simulated failure requested by prompt marker")
    sys.exit(3)

digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]
report = pathlib.Path("/workspace/agent_report.md")
report.write_text(
    "# Agent report\n\n"
    f"- task: `{task_id}`\n"
    f"- prompt sha256: `{digest}`\n"
    f"- steps: {len(steps)}\n"
    "- outcome: completed\n",
    encoding="utf-8",
)
solution = {
    "task_id": task_id,
    "status": "ok",
    "prompt_chars": len(prompt),
    "prompt_sha256_12": digest,
    "files": ["agent_report.md"],
}
pathlib.Path("/workspace/solution.json").write_text(json.dumps(solution, indent=2), encoding="utf-8")
emit("mock-done")
'''


class AgentConfigError(ValueError):
    pass


def substitute(template: list[str], *, prompt: str, prompt_file: str, model: str | None) -> list[str]:
    out = []
    for arg in template:
        out.append(
            arg.replace("{prompt}", prompt)
            .replace("{prompt_file}", prompt_file)
            .replace("{model}", model or "")
        )
    return out


def resolve_agent_command(
    spec: dict[str, Any],
    *,
    templates: dict[str, list[str]],
    prompt_file: str = "/workspace/.agentmesh/prompt.txt",
) -> tuple[list[str], dict[str, str], str]:
    """Return (argv, extra_env, prompt) for the single agent of this task."""
    agent = spec.get("agent")
    command = spec.get("command")
    if command:
        return list(command), {}, ""
    if not agent:
        raise AgentConfigError("task has neither command nor agent")
    kind = agent.get("kind", "mock")
    prompt = agent.get("prompt", "")
    model = agent.get("model")
    if kind == "mock":
        argv = ["python", "-u", "-c", MOCK_AGENT_SCRIPT]
        return argv, {"AGENTMESH_MOCK_DELAY": "0.35"}, prompt
    template = templates.get(kind)
    if not template:
        raise AgentConfigError(
            f"agent kind {kind!r} has no configured CLI template; "
            f"configure agents.commands.{kind} on the worker"
        )
    return (
        substitute(template, prompt=prompt, prompt_file=prompt_file, model=model),
        {"AGENTMESH_PROMPT_FILE": prompt_file},
        prompt,
    )


def write_prompt(workspace: str, prompt: str, extra: dict[str, Any] | None = None) -> str:
    """Write the prompt into the task's private workspace (never host-shared)."""
    import os
    from pathlib import Path

    agent_dir = Path(workspace) / ".agentmesh"
    agent_dir.mkdir(parents=True, exist_ok=True)
    target = agent_dir / "prompt.txt"
    target.write_text(prompt, encoding="utf-8")
    if extra is not None:
        (agent_dir / "task.json").write_text(json.dumps(extra, indent=2), encoding="utf-8")
    try:
        os.chmod(agent_dir, 0o700)
        os.chmod(target, 0o600)
    except OSError:
        pass
    return str(target)
