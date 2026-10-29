"""Section 12.2: fila as an MCP server for Claude Code (stdio transport).

The Model Context Protocol over stdio is JSON-RPC 2.0, one message per line.
This implements the part a tool server needs (``initialize``, ``tools/list``,
``tools/call``, ``ping``) with the standard library, so it ships inside the
same single-file agent. Every tool runs the matching command line with
``--json``: the tools do exactly what the terminal does, client mode included.
"""

from __future__ import annotations

import io
import json
import sys
import threading
from typing import Callable, List, Optional

from . import __version__

PROTOCOL_VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
MAX_WAIT = "10m"  # a tool call must answer in minutes; long waits belong to `fila wait` in the background

INSTRUCTIONS = (
    "fila queues jobs on the user's registered nodes (computers and Slurm clusters) and runs commands on their "
    "terminals. Use `run` for short commands now (git pull, builds, checks) and `submit` for computations, always "
    "declaring cores and mem. Use `explain` when a job does not start. Results stay on the node; fila never moves "
    "files. For long jobs, prefer the command line `fila wait <ids>` in the background over this server's `wait`."
)

_STRING = {"type": "string"}
_INT = {"type": "integer"}
_BOOL = {"type": "boolean"}


def _tool(name: str, description: str, properties: dict, required: List[str] = ()) -> dict:
    return {"name": name, "description": description,
            "inputSchema": {"type": "object", "properties": properties, "required": list(required)}}


TOOLS = [
    _tool("run", "Run a shell command now on nodes' terminals and wait for each node's result (not for heavy work).",
          {"command": dict(_STRING, description="the command, as typed in the node's shell"),
           "on": dict(_STRING, description="comma-separated nodes, all or online"),
           "dir": dict(_STRING, description="directory on the node (default: the user's home)"),
           "time": dict(_STRING, description="time limit, e.g. 10m"),
           "cores": dict(_INT, description="reserve these CPUs while it runs"),
           "mem": dict(_STRING, description="reserve and limit this memory, e.g. 2G"),
           "strict": dict(_BOOL, description="refuse if the declared resources are not free")},
          ["command", "on"]),
    _tool("submit", "Queue a job. fila picks the fastest node that fits unless `on` restricts it.",
          {"command": _STRING, "cores": dict(_INT, description="logical CPUs"), "mem": dict(_STRING, description="e.g. 4G"),
           "time": dict(_STRING, description="time limit, e.g. 2h or 3d"), "on": dict(_STRING, description="comma-separated nodes"),
           "dir": dict(_STRING, description="directory on the node where the command starts"),
           "after": {"type": "array", "items": _INT, "description": "job ids that must complete first"},
           "even_if_failed": _BOOL, "retry_after": _STRING, "on_use": {"type": "string", "enum": ["keep", "suspend"]},
           "hint": {"type": "string", "enum": ["multithread", "nomultithread"]}, "name": _STRING,
           "slurm_nodelist": _STRING, "slurm_exclude": _STRING, "slurm_extra": _STRING,
           "dry_run": dict(_BOOL, description="only show where it would go")},
          ["command", "cores", "mem"]),
    _tool("status", "The queue: waiting and running jobs (all includes finished ones).",
          {"all": _BOOL, "state": dict(_STRING, description="comma-separated states"), "node": _STRING}),
    _tool("resources", "What each node has free right now.", {}),
    _tool("result", "A job's state, reason, exit code and attempts.", {"job": _INT}, ["job"]),
    _tool("log", "The end of a job's output.", {"job": _INT, "attempt": _INT, "stderr": _BOOL}, ["job"]),
    _tool("explain", "Why a job is in its state, node by node.", {"job": _INT}, ["job"]),
    _tool("wait", f"Wait until jobs end, at most {MAX_WAIT}.",
          {"jobs": {"type": "array", "items": _INT}, "timeout": dict(_STRING, description=f"default and maximum {MAX_WAIT}")},
          ["jobs"]),
    _tool("cancel", "Cancel a job (and with cascade its unfinished dependents).", {"job": _INT, "cascade": _BOOL}, ["job"]),
    _tool("retry", "Queue a failed, cancelled or unreachable job again.", {"job": _INT, "on": _STRING}, ["job"]),
]


def command_line(name: str, arguments: dict) -> List[str]:
    """The ``fila`` arguments a tool call stands for."""
    a = arguments

    def options(*pairs) -> List[str]:
        result = []
        for key, flag in pairs:
            value = a.get(key)
            if value is True:
                result.append(flag)
            elif value not in (None, False, ""):
                result += [flag, ",".join(map(str, value)) if isinstance(value, list) else str(value)]
        return result

    if name == "run":
        return ["run", "--json", "--wait", "--on", a["on"]] + options(
            ("dir", "--dir"), ("time", "--time"), ("cores", "--cores"), ("mem", "--mem"), ("strict", "--strict")
        ) + ["--", a["command"]]
    if name == "submit":
        return ["submit", "--json", "--cores", str(a["cores"]), "--mem", str(a["mem"])] + options(
            ("time", "--time"), ("on", "--on"), ("dir", "--dir"), ("after", "--after"),
            ("even_if_failed", "--even-if-failed"), ("retry_after", "--retry-after"), ("on_use", "--on-use"),
            ("hint", "--hint"), ("name", "--name"), ("slurm_nodelist", "--slurm-nodelist"),
            ("slurm_exclude", "--slurm-exclude"), ("slurm_extra", "--slurm-extra"), ("dry_run", "--dry-run"),
        ) + ["--", a["command"]]
    if name == "status":
        return ["status", "--json"] + options(("all", "--all"), ("state", "--state"), ("node", "--node"))
    if name == "resources":
        return ["resources", "--json"]
    if name in ("result", "explain"):
        return [name, "--json", str(a["job"])]
    if name == "log":
        return ["log", "--json", str(a["job"])] + options(("attempt", "--attempt"), ("stderr", "--stderr"))
    if name == "wait":
        return ["wait", "--json"] + [str(job) for job in a["jobs"]] + ["--timeout", _capped(a.get("timeout"))]
    if name == "cancel":
        return ["cancel", "--json", str(a["job"])] + options(("cascade", "--cascade"))
    if name == "retry":
        return ["retry", "--json", str(a["job"])] + options(("on", "--on"))
    raise KeyError(name)


def _capped(timeout: Optional[str]) -> str:
    from .units import parse_duration

    if not timeout:
        return MAX_WAIT
    try:
        return timeout if parse_duration(timeout) <= parse_duration(MAX_WAIT) else MAX_WAIT
    except ValueError:
        return MAX_WAIT


# Exit codes that mean "fila could not do it" rather than "the job did not succeed".
_TOOL_ERRORS = {4: "refused", 5: "usage error", 6: "coordinator unavailable"}


class Server:
    def __init__(self, run_fila: Callable[[List[str]], tuple], stdin=None, stdout=None) -> None:
        self.run_fila = run_fila  # argv -> (exit code, stdout, stderr)
        self.stdin = stdin or sys.stdin
        self.stdout = stdout or sys.stdout
        self.lock = threading.Lock()

    def send(self, message: dict) -> None:
        with self.lock:
            self.stdout.write(json.dumps(message) + "\n")
            self.stdout.flush()

    def serve(self) -> None:
        threads = []
        for line in self.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError:
                self.send({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}})
                continue
            if message.get("method") == "tools/call" and "id" in message:  # may be slow: answer in a thread
                thread = threading.Thread(target=self.handle, args=(message,), daemon=True)
                thread.start()
                threads.append(thread)
            else:
                self.handle(message)
        for thread in threads:
            thread.join()

    def handle(self, message: dict) -> None:
        if "id" not in message:  # a notification (initialized, cancelled): nothing to answer
            return
        method, params = message.get("method"), message.get("params") or {}
        try:
            result = self.dispatch(method, params)
        except KeyError:
            self.send({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32601, "message": f"Method not found: {method}"}})
            return
        except Exception as error:  # never leave the client without an answer
            self.send({"jsonrpc": "2.0", "id": message["id"], "error": {"code": -32603, "message": str(error)}})
            return
        self.send({"jsonrpc": "2.0", "id": message["id"], "result": result})

    def dispatch(self, method: str, params: dict) -> dict:
        if method == "initialize":
            wanted = params.get("protocolVersion")
            return {
                "protocolVersion": wanted if wanted in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0],
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "fila", "version": __version__},
                "instructions": INSTRUCTIONS,
            }
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": TOOLS}
        if method == "tools/call":
            return self.call(params.get("name"), params.get("arguments") or {})
        raise KeyError(method)

    def call(self, name: str, arguments: dict) -> dict:
        try:
            argv = command_line(name, arguments)
        except KeyError as missing:
            return _text({"error": f"unknown tool or missing argument: {missing}"}, error=True)
        code, out, err = self.run_fila(argv)
        try:
            output = json.loads(out) if out.strip() else None
        except ValueError:
            output = out
        return _text({"exit_code": code, "result": output, "stderr": err.strip() or None}, error=code in _TOOL_ERRORS)


def _text(data: dict, error: bool) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(data, indent=2)}], "isError": error}


def run_in_process(home: Optional[str]) -> Callable[[List[str]], tuple]:
    """Run ``fila <argv>`` here, capturing what it writes."""
    from .cli import main

    def run(argv: List[str]) -> tuple:
        out, err = io.StringIO(), io.StringIO()
        code = main(argv, home=home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    return run
