"""How the coordinator reaches a computer's agent (section 4.3).

``LocalExecutor`` calls the agent's operations in this process;
``SshExecutor`` runs ``fila internal agent <operation>`` on the node over SSH,
JSON in and out. Both offer the same methods, so the server and the command
line treat every computer alike.
"""

from __future__ import annotations

import json
import subprocess
from typing import List, Optional

from . import agent
from .bundle import agent_archive
from .errors import AgentError, NodeUnreachable

AGENT_PATH = ".fila/fila.pyz"  # relative to the remote user's home


class Executor:
    node: str
    jobs_dir: str
    shell: List[str]
    notify: Optional[str] = None

    def call(self, operation: str, args: dict):
        raise NotImplementedError

    def _args(self, **extra) -> dict:
        return dict(jobs_dir=self.jobs_dir, **extra)

    def version(self) -> dict:
        return self.call("version", {})

    def probe(self) -> dict:
        return self.call("probe", self._args())

    def cycle(self, attempts: List[dict], now: float) -> dict:
        """The probe plus the diary of each ``{job, attempt, launched_at}``."""
        return self.call("cycle", self._args(attempts=attempts, now=now))

    def launch(self, job: int, attempt: int, command: str, cpus: List[int], request: dict,
               time_limit: Optional[float], directory: Optional[str] = None, script: Optional[dict] = None) -> dict:
        return self.call("launch", self._args(
            job=job, attempt=attempt, command=command, shell=self.shell, cpus=list(cpus), request=request,
            time_limit=time_limit, dir=directory, script=script, notify=self.notify, node=self.node,
        ))

    def request(self, job: int, attempt: int, what: str) -> None:
        self.call("request", self._args(job=job, attempt=attempt, what=what))

    def read_log(self, job: int, attempt: int, stream: str = "stdout", offset: int = 0,
                 tail: Optional[int] = None) -> dict:
        return self.call("read_log", self._args(job=job, attempt=attempt, stream=stream, offset=offset, tail=tail))

    def clean(self, job: int, logs_only: bool) -> Optional[int]:
        return self.call("clean", self._args(job=job, logs_only=logs_only))["freed"]

    def check_shell(self) -> dict:
        return self.call("check_shell", {"shell": self.shell})

    def benchmark(self) -> dict:
        return self.call("benchmark", {})

    def time_command(self, command: str) -> dict:
        return self.call("time_command", {"shell": self.shell, "command": command})

    def selftest(self) -> List[dict]:
        return self.call("selftest", self._args(shell=self.shell))


class LocalExecutor(Executor):
    def __init__(self, node: str, jobs_dir: str, shell: List[str], notify: Optional[str] = None, probe=None) -> None:
        self.node = node
        self.jobs_dir = jobs_dir
        self.shell = list(shell)
        self.notify = notify  # file the supervisor touches when the attempt ends
        self._probe = probe  # tests inject a fake probe; None is this machine

    def call(self, operation: str, args: dict):
        if operation in ("probe", "cycle"):
            return getattr(agent, operation)(args, probe=self._probe)
        return agent.OPERATIONS[operation](args)

    def attempt_dir(self, job: int, attempt: int) -> str:
        return agent.attempt_dir(self.jobs_dir, job, attempt)


class SshExecutor(Executor):
    """The node's ``python`` is a shell word on the remote side (``python3``,
    ``$HOME/venvs/x/bin/python``); the remote login shell must be POSIX."""

    def __init__(self, node: str, host: str, jobs_dir: str, shell: List[str], python: str = "python3",
                 ssh_command: Optional[List[str]] = None, timeout: float = 60.0) -> None:
        self.node = node
        self.host = host
        self.jobs_dir = jobs_dir
        self.shell = list(shell)
        self.python = python
        self.ssh_command = list(ssh_command or ["ssh"])
        self.timeout = timeout

    def _ssh(self, remote: str, data: bytes = b"", timeout: Optional[float] = None) -> subprocess.CompletedProcess:
        try:
            result = subprocess.run(
                self.ssh_command + [self.host, remote], input=data, capture_output=True, timeout=timeout or self.timeout
            )
        except subprocess.TimeoutExpired:
            raise NodeUnreachable(f"{self.node}: no answer in {timeout or self.timeout:.0f} s") from None
        except OSError as error:
            raise NodeUnreachable(f"{self.node}: cannot start ssh: {error}") from None
        if result.returncode == 255:
            raise NodeUnreachable(f"{self.node}: {result.stderr.decode('utf-8', 'replace').strip()}")
        return result

    def call(self, operation: str, args: dict):
        timeout = max(self.timeout, 180.0) if operation in ("selftest", "benchmark", "time_command") else None
        result = self._ssh(
            f"cd && {self.python} {AGENT_PATH} internal agent {operation}", json.dumps(args).encode(), timeout=timeout
        )
        try:
            answer = json.loads(result.stdout.decode("utf-8"))
        except ValueError:
            detail = (result.stderr or result.stdout).decode("utf-8", "replace").strip()[-500:]
            raise AgentError(f"{self.node}: the agent did not answer ({detail or 'no output'})") from None
        if not answer.get("ok"):
            if answer.get("kind") == "unreachable":  # the node answered; what lies behind it did not
                raise NodeUnreachable(answer.get("error"))
            raise AgentError(f"{self.node}: {answer.get('error')}")
        return answer["result"]

    def python_version(self) -> Optional[tuple]:
        result = self._ssh(f"cd && {self.python} -c 'import sys; print(*sys.version_info[:2])'")
        if result.returncode != 0:
            return None
        return tuple(int(part) for part in result.stdout.decode().split())

    def deploy(self) -> None:
        """Copy this coordinator's agent to the node, replacing any older one."""
        temporary = AGENT_PATH + ".tmp"
        result = self._ssh(f"cd && mkdir -p .fila && cat > {temporary} && mv -f {temporary} {AGENT_PATH}", agent_archive())
        if result.returncode != 0:
            raise AgentError(f"{self.node}: cannot copy the agent: {result.stderr.decode('utf-8', 'replace').strip()}")


def executor_for(entry, general: dict, notify: Optional[str] = None, probe=None, entries: Optional[dict] = None,
                 via: Optional[Executor] = None, store=None):
    """The executor of a registered node; a cluster's goes through its via
    computer, a pull node's through the channel recorded in ``store``."""
    if entry.type == "slurm":
        from .slurm import SlurmExecutor

        if via is None:
            via_entry = (entries or {}).get(entry.via)
            if via_entry is None or via_entry.type != "computer":
                raise ValueError(f"{entry.name}: via {entry.via} is not a registered computer")
            via = executor_for(via_entry, general, store=store)
        return SlurmExecutor(entry, via, general.get("ssh_command") or ["ssh"])
    if entry.access == "pull":
        from .channel import PullExecutor

        if store is None:
            raise ValueError(f"{entry.name}: a pull node is reached only through the coordinator's database")
        return PullExecutor(entry.name, entry.jobs_dir, entry.shell, store, timeout=float(general.get("ssh_timeout_s", 60)))
    if entry.access == "local":
        return LocalExecutor(entry.name, entry.jobs_dir, entry.shell, notify=notify, probe=probe)
    if entry.access == "ssh":
        return SshExecutor(
            entry.name, entry.host, entry.jobs_dir, entry.shell, python=entry.python or "python3",
            ssh_command=general.get("ssh_command"), timeout=float(general.get("ssh_timeout_s", 60)),
        )
    raise ValueError(f"{entry.name}: {entry.access} access is not supported yet")
