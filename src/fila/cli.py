"""The ``fila`` command line (section 11 of the specification).

``main`` returns the exit code instead of exiting, and takes its home
directory, streams, environment, clock and sleep as arguments, so tests drive
the real command line.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from typing import Callable, List, Mapping, Optional, TextIO

from . import agent, service, supervisor
from .bundle import agent_archive
from .executor import AgentError, NodeUnreachable, SshExecutor, executor_for
from .registry import NodeEntry, Registry, RegistryError, load_toml
from .scheduler import Resources, Start, check_request, describe, free_now
from .server import Server, node_view, server_alive, slurm_view
from .slurm import check_extra, sbatch_script
from .store import FINAL_STATES, SETTLED_STATES, JobRecord, NotAllowed, Store, UnknownJob
from .units import format_mem, parse_duration, parse_mem

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_NOT_RUN = 2
EXIT_TIMEOUT = 3
EXIT_REFUSED = 4
EXIT_USAGE = 5
EXIT_UNAVAILABLE = 6

WAIT_POLL_S = 1.0


class UsageError(Exception):
    """Invalid option, unknown node or job (exit code 5)."""


class Refused(Exception):
    """A valid request fila will not accept (exit code 4)."""


class Unavailable(Exception):
    """The coordinator is not running (exit code 6)."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # argparse would exit with 2, which means "not run" here
        raise UsageError(message)


class Context:
    def __init__(
        self,
        home: str,
        store: Store,
        out: TextIO,
        json_output: bool,
        clock: Callable[[], float],
        sleep: Callable[[float], None],
        environ: Mapping[str, str],
    ) -> None:
        self.home = home
        self.registry = Registry(os.path.join(home, "nodes.toml"))
        self.store = store
        self.out = out
        self.json = json_output
        self.clock = clock
        self.sleep = sleep
        self.environ = environ

    def emit(self, data, text: str) -> None:
        self.out.write(json.dumps(data, indent=2) + "\n" if self.json else text + "\n")

    def wake_server(self) -> None:
        """Ask the server for a cycle now instead of at its next interval."""
        with open(os.path.join(self.home, "wake"), "w") as handle:
            handle.write(str(self.clock()))

    def entries(self) -> dict:
        try:
            return self.registry.load()[1]
        except RegistryError as error:
            raise UsageError(str(error)) from None


def main(
    argv: Optional[List[str]] = None,
    *,
    home: Optional[str] = None,
    stdout: Optional[TextIO] = None,
    stderr: Optional[TextIO] = None,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], None] = time.sleep,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    out = stdout or sys.stdout
    err = stderr or sys.stderr
    environ = os.environ if environ is None else environ
    argv = _attach_values(sys.argv[1:] if argv is None else argv)
    home = home or default_home(environ)
    try:
        client = _client_config(home, environ)
    except RegistryError as error:
        err.write(f"fila: {error}\n")
        return EXIT_USAGE
    if client is not None and argv[:1] not in (["internal"], ["worker"], ["mcp"], ["guide"]):
        return _forward(client, argv, out, err, environ)  # the MCP server stays here; each tool call is forwarded
    try:
        args = _parser().parse_args(argv)
        if args.command_name in ("internal", "worker", "guide", "mcp"):  # no coordinator database here
            args.home, args.out, args.err = home, out, err
            return args.handler(None, args)
        store = Store(os.path.join(home, "fila.db"))
        return args.handler(Context(home, store, out, args.json, clock, sleep, environ), args)
    except UnknownJob as error:
        err.write(f"fila: unknown job: {error}\n")
        return EXIT_USAGE
    except UsageError as error:
        err.write(f"fila: {error}\n")
        return EXIT_USAGE
    except (Refused, NotAllowed) as error:
        err.write(f"fila: refused: {error}\n")
        return EXIT_REFUSED
    except (NodeUnreachable, AgentError) as error:
        err.write(f"fila: {error}\n")
        return EXIT_REFUSED
    except Unavailable as error:
        err.write(f"fila: {error}\n")
        return EXIT_UNAVAILABLE
    except SystemExit as exit_:  # --help
        return int(exit_.code or 0)


def entry() -> None:
    sys.exit(main())


# Options whose value is itself options: "--slurm-extra --time=5" must not read --time=5 as a fila option.
_VALUE_IS_OPTIONS = ("--slurm-extra",)


def _attach_values(argv: List[str]) -> List[str]:
    result, index = [], 0
    while index < len(argv):
        arg = argv[index]
        if arg == "--":
            return result + argv[index:]
        if arg in _VALUE_IS_OPTIONS and index + 1 < len(argv):
            result.append(f"{arg}={argv[index + 1]}")
            index += 2
            continue
        result.append(arg)
        index += 1
    return result


# --- Client mode (section 4.5) ---


def _client_config(home: str, environ: Mapping[str, str]) -> Optional[dict]:
    """``client.toml`` (coordinator, remote_command, ssh) or ``FILA_COORDINATOR``; None on the coordinator."""
    path = os.path.join(home, "client.toml")
    config = load_toml(path) if os.path.exists(path) else {}
    if environ.get("FILA_COORDINATOR"):
        config["coordinator"] = environ["FILA_COORDINATOR"]
    return config if config.get("coordinator") else None


def _forward(config: dict, argv: List[str], out: TextIO, err: TextIO, environ: Mapping[str, str]) -> int:
    """Run the same command on the coordinator over SSH. The remote shell is
    assumed POSIX (bash, sh), as on a Linux or macOS coordinator."""
    argv, stdin_data = _script_through_stdin(argv)
    remote = config.get("remote_command", "fila") + " " + " ".join(shlex.quote(arg) for arg in argv)
    command = list(config.get("ssh", ["ssh"])) + [config["coordinator"], remote]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL if stdin_data is None else subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=dict(environ),
        )
    except OSError as error:
        err.write(f"fila: cannot start ssh: {error}\n")
        return EXIT_UNAVAILABLE
    errors: List[bytes] = []
    drain = threading.Thread(target=lambda: errors.append(process.stderr.read()))
    drain.start()
    if stdin_data is not None:
        process.stdin.write(stdin_data)
        process.stdin.close()
    for chunk in iter(lambda: process.stdout.read1(65536) if hasattr(process.stdout, "read1") else process.stdout.read(), b""):
        out.write(chunk.decode("utf-8", errors="replace"))
        out.flush()
    process.wait()
    drain.join()
    error_text = b"".join(errors).decode("utf-8", errors="replace")
    if process.returncode == 255:
        err.write(f"fila: cannot reach the coordinator {config['coordinator']}: {error_text.strip()}\n")
        return EXIT_UNAVAILABLE
    err.write(error_text)
    return process.returncode


def _script_through_stdin(argv: List[str]) -> tuple:
    """A local ``--script`` file is not on the coordinator: send its content on stdin."""
    for index, arg in enumerate(argv):
        if arg == "--":
            break
        if arg == "--script" and index + 1 < len(argv) or arg.startswith("--script="):
            path = argv[index + 1] if arg == "--script" else arg.split("=", 1)[1]
            if path == "-":
                return argv, None
            with open(path, "rb") as handle:
                content = handle.read()
            end = index + 2 if arg == "--script" else index + 1
            rest = ["--script", "-", "--script-name", os.path.basename(path)]
            return argv[:index] + rest + argv[end:], content
    return argv, None


def default_home(environ: Mapping[str, str]) -> str:
    if environ.get("FILA_HOME"):
        return environ["FILA_HOME"]
    if os.name == "nt":
        return os.path.join(environ.get("APPDATA", os.path.expanduser("~")), "fila")
    return os.path.join(environ.get("XDG_CONFIG_HOME", os.path.expanduser("~/.config")), "fila")


def _parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="machine-readable output")

    parser = _Parser(prog="fila", description="Queue and resource manager for non-local nodes.")
    commands = parser.add_subparsers(dest="command_name", required=True, parser_class=_Parser)

    submit = commands.add_parser("submit", parents=[common], help="queue a job")
    submit.add_argument("--name")
    submit.add_argument("--cores", type=int, help="logical CPUs (required)")
    submit.add_argument("--mem", help="memory, e.g. 4G (required)")
    submit.add_argument("--time", help="time limit, e.g. 2h, 3d, 7-00:00:00")
    submit.add_argument("--on", help="comma-separated candidate nodes")
    submit.add_argument("--dir", help="directory on the node where the command starts")
    submit.add_argument("--script", help="local script sent with the job and run on the node (- reads stdin)")
    submit.add_argument("--script-name", help=argparse.SUPPRESS)
    submit.add_argument("--after", help="comma-separated job ids that must complete first")
    submit.add_argument("--even-if-failed", action="store_true", help="run after --after jobs end in any state")
    submit.add_argument("--retry-after", help="retry elsewhere if the node stays unreachable this long")
    submit.add_argument("--on-use", choices=["keep", "suspend"])
    submit.add_argument("--hint", choices=["multithread", "nomultithread"])
    submit.add_argument("--slurm-nodelist", help="comma-separated Slurm machines")
    submit.add_argument("--slurm-exclude", help="comma-separated Slurm machines to avoid")
    submit.add_argument("--slurm-extra", help="extra #SBATCH options, passed through")
    submit.add_argument("--dry-run", action="store_true", help="show what would happen without queueing")
    submit.add_argument("--wait", action="store_true", help="wait until the job ends")
    submit.add_argument("--timeout", help="with --wait, give up after this long (exit code 3)")
    submit.add_argument("command", nargs=argparse.REMAINDER, help="-- followed by the command")
    submit.set_defaults(handler=_submit)

    status = commands.add_parser("status", parents=[common], help="show the queue")
    status.add_argument("--all", action="store_true", help="include finished jobs")
    status.add_argument("--state", help="comma-separated states to show")
    status.add_argument("--node", help="only jobs on this node")
    status.set_defaults(handler=_status)

    resources = commands.add_parser("resources", parents=[common], help="what each node has free right now")
    resources.set_defaults(handler=_resources)

    result = commands.add_parser("result", parents=[common], help="a job's outcome and attempts")
    result.add_argument("job", type=int)
    result.set_defaults(handler=_result)

    explain = commands.add_parser("explain", parents=[common], help="why a job is in its state")
    explain.add_argument("job", type=int)
    explain.set_defaults(handler=_explain)

    wait = commands.add_parser("wait", parents=[common], help="wait until jobs end")
    wait.add_argument("jobs", type=int, nargs="+")
    wait.add_argument("--timeout", help="give up after this long (exit code 3)")
    wait.set_defaults(handler=_wait)

    cancel = commands.add_parser("cancel", parents=[common], help="cancel a job")
    cancel.add_argument("job", type=int)
    cancel.add_argument("--cascade", action="store_true", help="also cancel every unfinished dependent")
    cancel.set_defaults(handler=_cancel)

    retry = commands.add_parser("retry", parents=[common], help="queue a failed, cancelled or unreachable job again")
    retry.add_argument("job", type=int)
    retry.add_argument("--on", help="comma-separated candidate nodes for the new attempt")
    retry.set_defaults(handler=_retry)

    run = commands.add_parser("run", parents=[common], help="run a command now on nodes' terminals")
    run.add_argument("--on", required=True, help="comma-separated nodes, all or online")
    run.add_argument("--dir", help="directory on the node (default: the user's home)")
    run.add_argument("--time", help="time limit (default: run_timeout)")
    run.add_argument("--cores", type=int, help="reserve these CPUs while it runs")
    run.add_argument("--mem", help="reserve and limit this memory while it runs")
    run.add_argument("--strict", action="store_true", help="refuse if the declared resources are not free")
    run.add_argument("--wait", action="store_true", help="wait and show each node's result")
    run.add_argument("command", nargs=argparse.REMAINDER, help="-- followed by the command")
    run.set_defaults(handler=_run)

    for name, handler, text in (
        ("suspend", _suspend, "freeze running jobs (CPU only; memory stays used)"),
        ("resume", _resume, "continue suspended jobs"),
    ):
        sub = commands.add_parser(name, parents=[common], help=text)
        sub.add_argument("job", type=int, nargs="?")
        sub.add_argument("--node", help="every running job on this node")
        sub.set_defaults(handler=handler)

    log = commands.add_parser("log", parents=[common], help="a job's output")
    log.add_argument("job", type=int)
    log.add_argument("--attempt", type=int, help="default: the latest")
    log.add_argument("--stderr", action="store_true", help="the error output instead")
    log.add_argument("--follow", action="store_true", help="keep printing until the attempt ends")
    log.set_defaults(handler=_log)

    clean = commands.add_parser("clean", parents=[common], help="delete old directories of finished jobs")
    clean.add_argument("--older-than", required=True, help="finished at least this long ago, e.g. 30d")
    clean.add_argument("--node", help="only this node")
    clean.add_argument("--logs-only", action="store_true", help="delete only fila's logs, keep what the job wrote")
    clean.set_defaults(handler=_clean)

    nodes = commands.add_parser("nodes", help="registered nodes")
    node_commands = nodes.add_subparsers(dest="nodes_command", required=True, parser_class=_Parser)
    register = node_commands.add_parser("register", parents=[common], help="register and validate a node")
    register.add_argument("name")
    register.add_argument("--type", default="computer", choices=["computer", "slurm"])
    register.add_argument("--access", choices=["local", "ssh", "pull"])
    register.add_argument("--os", choices=["linux", "windows", "macos"])
    register.add_argument("--shell", required=True, help='e.g. "/bin/bash -lc" or a JSON list')
    register.add_argument("--jobs-dir", required=True)
    register.add_argument("--host", help="ssh: the alias in ~/.ssh/config; slurm: the login node as the via computer sees it")
    register.add_argument("--via", help="slurm: the registered computer that reaches the cluster")
    register.add_argument("--partition", help="slurm: the partition (one cluster node per partition)")
    register.add_argument("--account", help="slurm: --account for every job")
    register.add_argument("--qos", help="slurm: --qos for every job")
    register.add_argument("--exclude", help="slurm: comma-separated Slurm machines to avoid by default")
    register.add_argument("--max-pending", type=int, default=2, help="slurm: K of the pending allowance (section 7.5)")
    register.add_argument("--max-cores-per-job", type=int, help="slurm: override what the partition and QoS say")
    register.add_argument("--key", help="pull: the public key that `fila worker install` printed on the node")
    register.add_argument("--wait", type=float, default=30.0, help="pull: seconds to wait for the node to connect")
    register.add_argument("--authorized-keys", help=argparse.SUPPRESS)  # default ~/.ssh/authorized_keys
    register.add_argument("--python", help="ssh: the node's interpreter, a shell word (default python3)")
    register.add_argument("--cores", type=int, help="most logical CPUs fila may use")
    register.add_argument("--mem", help="most memory fila may use")
    register.add_argument("--reserve", help='margin fila never takes, e.g. "cores=1,mem=4G"')
    register.add_argument("--reserve-in-use", help="margin while someone uses the machine")
    register.add_argument("--on-use", choices=["keep", "suspend"], default="keep")
    register.add_argument("--idle-minutes", type=float, default=5.0)
    register.add_argument("--default-hint", choices=["multithread", "nomultithread"], default="multithread")
    register.add_argument("--speed", type=float, help="speed per core; default: measured by the built-in test")
    register.add_argument("--min-free-disk")
    register.add_argument("--max-time")
    register.set_defaults(handler=_nodes_register)
    for name, handler, text in (
        ("list", _nodes_list, "registered nodes and what is free"),
        ("drain", _nodes_drain, "stop sending new jobs to a node"),
        ("resume", _nodes_resume, "send jobs to a drained node again"),
        ("remove", _nodes_remove, "forget a node"),
    ):
        sub = node_commands.add_parser(name, parents=[common], help=text)
        if name != "list":
            sub.add_argument("name")
        sub.set_defaults(handler=handler)
    for name, handler, text in (
        ("validate", _nodes_validate, "redo the registration checks"),
        ("update", _nodes_update, "copy this coordinator's agent to ssh nodes"),
        ("benchmark", _nodes_benchmark, "measure the speed per core"),
    ):
        sub = node_commands.add_parser(name, parents=[common], help=text)
        sub.add_argument("name", nargs="?", help="default: every node")
        if name == "benchmark":
            sub.add_argument("--command", help="your own benchmark, run through the node's shell")
        sub.set_defaults(handler=handler)

    guide = commands.add_parser("guide", help="how an agent should use fila (for a project's CLAUDE.md)")
    guide.set_defaults(handler=_guide, json=False)

    mcp = commands.add_parser("mcp", help="fila as an MCP server for Claude Code (stdio)")
    mcp.add_argument("--register", action="store_true", help="add it to Claude Code (claude mcp add --scope user)")
    mcp.set_defaults(handler=_mcp, json=False)

    selftest = commands.add_parser("selftest", parents=[common], help="check that limits are enforced on nodes")
    selftest.add_argument("--on", required=True, help="comma-separated computers or all")
    selftest.set_defaults(handler=_selftest)

    server = commands.add_parser("server", parents=[common], help="run the coordinator")
    server.add_argument("--once", action="store_true", help="one cycle, then exit")
    server.add_argument("--install", action="store_true", help="run it as a permanent service of this user")
    server.add_argument("--uninstall", action="store_true", help="stop and remove the service")
    server.add_argument("--dry-run", action="store_true", help="with --install/--uninstall, only show the steps")
    server.add_argument("--service-name", default="fila-server", help=argparse.SUPPRESS)
    server.set_defaults(handler=_server)

    internal = commands.add_parser("internal", help=argparse.SUPPRESS)
    internal_commands = internal.add_subparsers(dest="internal_command", required=True, parser_class=_Parser)
    supervise = internal_commands.add_parser("supervise", parents=[common])
    supervise.add_argument("attempt_dir")
    supervise.set_defaults(handler=_internal_supervise)
    agent_command = internal_commands.add_parser("agent", parents=[common])
    agent_command.add_argument("operation", choices=sorted(agent.OPERATIONS))
    agent_command.set_defaults(handler=_internal_agent)
    channel_command = internal_commands.add_parser("channel", parents=[common])
    channel_command.add_argument("--node", required=True)
    channel_command.set_defaults(handler=_internal_channel)

    worker = commands.add_parser("worker", help="the agent of a pull node, which connects to the coordinator")
    worker_commands = worker.add_subparsers(dest="worker_command", required=True, parser_class=_Parser)
    worker_install = worker_commands.add_parser("install", parents=[common], help="set this computer up as a pull node")
    worker_install.add_argument("--coordinator", required=True, help="user@host of the coordinator, as ssh reaches it")
    worker_install.add_argument("--node", required=True, help="this node's name on the coordinator")
    worker_install.add_argument("--remote-command", default="fila", help="how to call fila on the coordinator")
    worker_install.add_argument("--no-service", action="store_true", help="do not install the permanent service")
    worker_install.add_argument("--dir", help=argparse.SUPPRESS)  # default ~/.fila
    worker_install.add_argument("--service-name", default="fila-worker", help=argparse.SUPPRESS)
    worker_install.set_defaults(handler=_worker_install)
    worker_run = worker_commands.add_parser("run", parents=[common], help="keep the channel open (the service runs this)")
    worker_run.add_argument("--config", help="default ~/.fila/worker.toml")
    worker_run.set_defaults(handler=_worker_run)
    return parser


# --- Handlers ---


def _submit(ctx: Context, args: argparse.Namespace) -> int:
    if args.cores is None or args.mem is None:
        raise Refused("--cores and --mem are required for submit")
    request = Resources(cores=args.cores, mem=_parse(parse_mem, args.mem))
    spec = _submit_spec(ctx, args)
    command = _command_text(args.command, required=spec.get("script") is None) or spec["script"]["name"]
    refusal = check_request(ctx.store.nodes(), request, on=_tuple(spec.get("on")), hint=spec.get("hint"))
    refusal = refusal or check_extra(spec.get("slurm_extra"))
    if refusal is not None:
        raise Refused(refusal)
    if args.dry_run:
        return _dry_run(ctx, command, request, spec)
    job_id = ctx.store.add_job(command, request, spec, _created_by(ctx.environ), ctx.clock(), name=args.name)
    ctx.wake_server()
    if args.wait:
        return _wait_for(ctx, [job_id], args.timeout)
    ctx.emit({"id": job_id}, str(job_id))
    return EXIT_OK


def _dry_run(ctx: Context, command: str, request: Resources, spec: dict) -> int:
    action, reason = ctx.store.preview(command, request, spec, ctx.clock())
    if isinstance(action, Start):
        data = {"would": "start", "node": action.node, "cpus": list(action.cpus)}
        text = f"would start on {action.node}, CPUs {','.join(map(str, action.cpus))}"
    elif action is not None:
        script = sbatch_script(action.job, 1, command, dict(request), spec, ctx.entries()[action.node], action.node)
        data = {"would": "submit_to_slurm", "node": action.node, "script": script}
        text = f"would submit to Slurm on {action.node}:\n{script}"
    else:
        data = {"would": "wait", "summary": reason.summary, "details": list(reason.details)}
        text = "would wait: " + "\n  ".join((reason.summary,) + reason.details)
    ctx.emit(data, text)
    return EXIT_OK


def _status(ctx: Context, args: argparse.Namespace) -> int:
    jobs = ctx.store.jobs(include_final=args.all or args.state is not None)
    if args.state is not None:
        states = set(_names(args.state))
        jobs = [job for job in jobs if job.state in states]
    if args.node is not None:
        jobs = [job for job in jobs if _current_node(job) == args.node]
    lines = [
        f"{job.id:>6}  {job.state:<18}  {_current_node(job) or '-':<12}"
        f"  {job.request['cores']:>3}c {format_mem(job.request['mem']):>5}  {job.command}"
        for job in jobs
    ]
    ctx.emit([_job_json(job) for job in jobs], "\n".join(lines) or "no jobs")
    return EXIT_OK


def _resources(ctx: Context, args: argparse.Namespace) -> int:
    """Section 11: the scheduler's view of each node from the last cycle."""
    jobs = ctx.store.snapshot(ctx.clock()).jobs
    rows, lines = [], []
    for node in ctx.store.nodes():
        free = free_now(node, jobs)
        row = {
            "name": node.name, "kind": node.kind, "state": node.state, "speed": node.speed,
            "capacity": dict(node.capacity), "free": {name: max(0, amount) for name, amount in free.items()},
        }
        if node.kind == "slurm":
            row["slurm_machines"] = [vars(machine) for machine in node.slurm]
            row["max_cores_per_job"] = node.max_cores_per_job
        else:
            row["reserve"] = dict(node.reserve)
            row["physical_cores"] = len(node.physical_cores())
        rows.append(row)
        lines.append(
            f"{node.name:<14} {node.state:<11} free {row['free'].get('cores', 0):>4}c"
            f" {format_mem(row['free'].get('mem', 0)):>7} of {node.capacity.get('cores', 0)}c"
            f" {format_mem(node.capacity.get('mem', 0))}  speed {node.speed}"
        )
    ctx.emit(rows, "\n".join(lines) or "no nodes registered")
    return EXIT_OK


def _result(ctx: Context, args: argparse.Namespace) -> int:
    job = ctx.store.job(args.job)
    data = dict(_job_json(job), spec=job.spec, finished_at=job.finished_at, attempts=job.attempts)
    lines = [f"job {job.id}: {job.state}" + (f" ({job.reason})" if job.reason else ""), f"command: {job.command}"]
    for attempt in job.attempts:
        lines.append(
            f"attempt {attempt['number']}: {attempt['state']} on {attempt['node'] or '-'}"
            + (f", exit code {attempt['exit_code']}" if attempt["exit_code"] is not None else "")
            + (f", started {_when(attempt['started_at'])}" if attempt["started_at"] else "")
            + (f", ended {_when(attempt['finished_at'])}" if attempt["finished_at"] else "")
        )
    ctx.emit(data, "\n".join(lines))
    return EXIT_FAILED if job.state in SETTLED_STATES and job.state != "completed" else EXIT_OK


def _explain(ctx: Context, args: argparse.Namespace) -> int:
    job = ctx.store.job(args.job)
    reason = ctx.store.reason(job.id)
    node = _current_node(job)
    details: tuple = ()
    if job.state == "running":
        attempt = job.attempts[-1]
        summary = f"running on {node} since {_when(attempt['started_at'])} (attempt {attempt['number']})"
    elif job.state == "slurm_queued":
        summary = f"slurm_queued: submitted to Slurm on {node}, waiting in its queue"
    elif job.state == "unreachable":
        summary = f"unreachable: node {node} lost contact; the job may still be running there"
    elif job.state in FINAL_STATES:
        summary = job.state + (f" ({job.reason})" if job.reason else "")
    elif reason is not None:
        summary, details = reason.summary, reason.details
    else:
        summary = f"{job.state}: not yet considered by the server"
    ctx.emit(
        {"id": job.id, "state": job.state, "summary": summary, "details": list(details)},
        "\n  ".join((f"{job.id}: {summary}",) + details),
    )
    return EXIT_OK


def _wait(ctx: Context, args: argparse.Namespace) -> int:
    for job_id in args.jobs:
        ctx.store.job(job_id)  # unknown ids fail before waiting
    return _wait_for(ctx, args.jobs, args.timeout)


def _wait_for(ctx: Context, job_ids: List[int], timeout: Optional[str]) -> int:
    """Section 11: block until every job is settled; 0 if all completed,
    1 if any ended otherwise, 3 on timeout. Reads only the local database."""
    deadline = None if timeout is None else ctx.clock() + _parse(parse_duration, timeout)
    while True:
        jobs = [ctx.store.job(job_id) for job_id in job_ids]
        settled = all(job.state in SETTLED_STATES for job in jobs)
        if settled or (deadline is not None and ctx.clock() >= deadline):
            break
        ctx.sleep(WAIT_POLL_S)
    lines = [f"{job.id:>6}  {job.state}" + (f" ({job.reason})" if job.reason else "") for job in jobs]
    ctx.emit([_job_json(job) for job in jobs], "\n".join(lines))
    if not settled:
        return EXIT_TIMEOUT
    return EXIT_OK if all(job.state == "completed" for job in jobs) else EXIT_FAILED


def _cancel(ctx: Context, args: argparse.Namespace) -> int:
    result = ctx.store.cancel(args.job, ctx.clock(), cascade=args.cascade)
    ctx.wake_server()
    lines = []
    if result["cancelled"]:
        lines.append("cancelled: " + ", ".join(map(str, result["cancelled"])))
    if result["requested"]:
        lines.append("cancellation requested (running): " + ", ".join(map(str, result["requested"])))
    ctx.emit(result, "\n".join(lines))
    return EXIT_OK


def _retry(ctx: Context, args: argparse.Namespace) -> int:
    on = None
    if args.on is not None:
        on = _names(args.on)
        _check_nodes(ctx, on)
    ctx.store.retry(args.job, ctx.clock(), on=on)
    ctx.wake_server()
    ctx.emit({"id": args.job}, f"{args.job} queued again")
    return EXIT_OK


RUN_FINAL = ("completed", "failed", "cancelled", "not_run")


def _run(ctx: Context, args: argparse.Namespace) -> int:
    """Section 5.2: one record per target; unreachable targets are not run, never waited for."""
    command = _command_text(args.command)
    general, entries = ctx.registry.load()
    if not server_alive(ctx.home, ctx.clock(), float(general["probe_interval_s"])):
        raise Unavailable("the fila server is not running (start it with: fila server)")
    targets = _run_targets(ctx, entries, args.on)
    request = Resources(
        cores=args.cores or 0, mem=_parse(parse_mem, args.mem) if args.mem else 0
    )
    spec: dict = {}
    if args.time is not None:
        spec["time_limit"] = _parse(parse_duration, args.time)
    if args.dir is not None:
        spec["dir"] = args.dir
    views = {node.name: node for node in ctx.store.nodes()}
    snapshot_jobs = ctx.store.snapshot(ctx.clock()).jobs
    ids = []
    for name in targets:
        view = views.get(name)
        state = view.state if view else "unreachable"
        if state == "online" and args.strict and not request.fits_in(free_now(view, snapshot_jobs)):
            raise Refused(f"{name} does not have {describe(request)} free now")
        not_run = None if state == "online" else state
        ids.append(ctx.store.add_run(command, name, request, spec, _created_by(ctx.environ), ctx.clock(), not_run))
    ctx.wake_server()
    if not args.wait:
        records = [ctx.store.job(job_id) for job_id in ids]
        ctx.emit(
            [{"id": r.id, "node": r.spec["node"], "status": r.state, "reason": r.reason} for r in records],
            "\n".join(f"{r.id:>6}  {r.spec['node']:<12} {r.state}" + (f" ({r.reason})" if r.reason else "") for r in records),
        )
        return EXIT_NOT_RUN if any(r.state == "not_run" for r in records) else EXIT_OK
    while not all(ctx.store.job(job_id).state in RUN_FINAL for job_id in ids):
        ctx.sleep(0.2)
    return _report_runs(ctx, [ctx.store.job(job_id) for job_id in ids], int(general["json_tail_kb"]) * 1024)


def _run_targets(ctx: Context, entries: dict, on: str) -> List[str]:
    computers = sorted(name for name, entry in entries.items() if entry.type == "computer")
    if on == "all":
        return computers
    if on == "online":
        views = {node.name: node for node in ctx.store.nodes()}
        return [name for name in computers if views.get(name) and views[name].state == "online"]
    names = _names(on)
    unknown = [name for name in names if name not in entries]
    if unknown:
        raise UsageError(f"unknown node: {', '.join(unknown)}")
    clusters = [name for name in names if entries[name].type != "computer"]
    if clusters:
        raise Refused(
            f"{', '.join(clusters)} only accepts submit; use fila run --on <via> \"ssh <alias> '...'\" instead"
        )
    return names


def _report_runs(ctx: Context, records: List[JobRecord], tail_bytes: int) -> int:
    results, blocks = [], []
    for record in records:
        attempt = record.attempts[-1] if record.attempts else None
        tail = stderr_tail = ""
        log = None
        if attempt is not None:
            try:
                executor = _node_executor(ctx, record.spec["node"])
                out = executor.read_log(record.id, 1, "stdout", tail=tail_bytes)
                tail, log = out["text"], out["path"]
                stderr_tail = executor.read_log(record.id, 1, "stderr", tail=tail_bytes)["text"]
            except (NodeUnreachable, AgentError, Refused) as error:
                stderr_tail = f"fila: cannot read the output: {error}"
        duration = None
        if attempt is not None and attempt["started_at"] and attempt["finished_at"]:
            duration = round(attempt["finished_at"] - attempt["started_at"], 3)
        status = "completed" if record.state == "completed" else ("not_run" if record.state == "not_run" else "failed")
        results.append({
            "id": record.id, "node": record.spec["node"], "status": status, "reason": record.reason,
            "exit_code": attempt["exit_code"] if attempt else None, "duration_s": duration,
            "tail": tail, "stderr_tail": stderr_tail, "log": log,
        })
        header = f"== {record.spec['node']}: {status}" + (f" ({record.reason})" if record.reason else "")
        blocks.append("\n".join(filter(None, [header, tail.rstrip(), stderr_tail.rstrip()])))
    ctx.emit(results, "\n".join(blocks))
    if any(r["status"] == "not_run" for r in results):
        return EXIT_NOT_RUN
    return EXIT_OK if all(r["status"] == "completed" for r in results) else EXIT_FAILED


def _suspend(ctx: Context, args: argparse.Namespace) -> int:
    return _hold(ctx, args, True)


def _resume(ctx: Context, args: argparse.Namespace) -> int:
    return _hold(ctx, args, False)


def _hold(ctx: Context, args: argparse.Namespace, suspend: bool) -> int:
    if (args.job is None) == (args.node is None):
        raise UsageError("give a job id or --node")
    if args.job is not None:
        job = ctx.store.job(args.job)
        if job.state != "running":
            raise Refused(f"job {job.id} is {job.state}, not running")
        entry = ctx.entries().get(job.attempts[-1]["node"])
        if entry is not None and entry.type == "slurm":
            raise Refused("jobs on a Slurm cluster cannot be suspended by fila (scontrol suspend needs an administrator)")
        ids = [job.id]
    else:
        _registered(ctx, args.node)
        ids = [job.id for job, attempt in ctx.store.active_attempts() if attempt["node"] == args.node and job.state == "running"]
    for job_id in ids:
        ctx.store.set_hold(job_id, suspend)
    ctx.wake_server()
    verb = "suspended" if suspend else "resumed"
    ctx.emit({verb: ids}, f"{verb}: {', '.join(map(str, ids)) or 'nothing running'}")
    return EXIT_OK


def _log(ctx: Context, args: argparse.Namespace) -> int:
    job = ctx.store.job(args.job)
    if not job.attempts:
        raise Refused(f"job {job.id} has not started yet")
    number = args.attempt or len(job.attempts)
    if not 1 <= number <= len(job.attempts):
        raise UsageError(f"job {job.id} has attempts 1 to {len(job.attempts)}")
    attempt = job.attempts[number - 1]
    executor = _node_executor(ctx, attempt["node"])
    stream = "stderr" if args.stderr else "stdout"
    if ctx.json:
        limit = int(ctx.registry.load()[0]["json_tail_kb"]) * 1024
        answer = executor.read_log(job.id, number, stream, tail=limit)
        ctx.emit({"path": answer["path"], "attempt": number, "tail": answer["text"]}, "")
        return EXIT_OK
    offset = 0
    while True:
        answer = executor.read_log(job.id, number, stream, offset=offset)
        offset = answer["offset"]
        ctx.out.write(answer["text"])
        ctx.out.flush()
        if not args.follow or answer["finished"]:
            if args.follow:  # what was written between the last read and the end
                ctx.out.write(executor.read_log(job.id, number, stream, offset=offset)["text"])
            return EXIT_OK
        ctx.sleep(0.5)


def _node_executor(ctx: Context, node: str):
    entries = ctx.entries()
    entry = entries.get(node)
    if entry is None:
        raise Refused(f"node {node} is no longer registered")
    try:
        return executor_for(entry, ctx.registry.load()[0], entries=entries, store=ctx.store)
    except ValueError as error:
        raise Refused(str(error)) from None


def _clean(ctx: Context, args: argparse.Namespace) -> int:
    """Section 10: only ``<jobs_dir>/<id>/`` of finished jobs; never a ``--dir`` path."""
    cutoff = ctx.clock() - _parse(parse_duration, args.older_than)
    entries = ctx.entries()
    if args.node is not None:
        _registered(ctx, args.node)
    cleaned, skipped, freed = [], [], 0
    for job in ctx.store.jobs(include_final=True, include_runs=True):
        if job.state not in FINAL_STATES or job.finished_at is None or job.finished_at > cutoff:
            continue
        nodes = sorted({attempt["node"] for attempt in job.attempts if attempt["node"]})
        if args.node is not None:
            nodes = [node for node in nodes if node == args.node]
        removed = False
        for node in nodes:
            entry = entries.get(node)
            try:
                if entry is None:
                    raise Refused(f"{node} is no longer registered")
                amount = executor_for(entry, ctx.registry.load()[0], entries=entries, store=ctx.store).clean(job.id, args.logs_only)
            except (NodeUnreachable, AgentError, Refused, ValueError) as error:
                skipped.append({"id": job.id, "node": node, "reason": str(error)})
                continue
            if amount is not None:
                freed += amount
                removed = True
        if removed:
            cleaned.append(job.id)
    text = f"cleaned {len(cleaned)} jobs, freed {format_mem(freed) if freed else '0'}"
    if skipped:
        text += f"; skipped {len(skipped)} whose node could not be reached"
    ctx.emit({"cleaned": cleaned, "freed_bytes": freed, "skipped": skipped}, text)
    return EXIT_OK


MIN_PYTHON = (3, 9)


def _nodes_register(ctx: Context, args: argparse.Namespace) -> int:
    """Section 3.5: validate everything before the node is accepted."""
    general, entries = ctx.registry.load()
    if args.name in entries:
        raise UsageError(f"node {args.name} is already registered")
    if args.type == "slurm":
        return _register_cluster(ctx, args, general, entries)
    if args.access is None:
        raise UsageError("computers need --access local, ssh or pull")
    if args.access == "pull":
        return _register_pull(ctx, args, general, entries)
    if args.access == "ssh" and not args.host:
        raise UsageError("ssh nodes need --host (an alias from ~/.ssh/config)")
    here = service.system()
    if args.access == "local" and args.os not in (None, here):
        raise Refused(f"a local node runs this machine's system ({here}), not {args.os}")
    entry = NodeEntry(
        name=args.name, type="computer", access=args.access, os=args.os, shell=_parse_shell(args.shell),
        jobs_dir=args.jobs_dir, host=args.host, python=args.python or ("python3" if args.access == "ssh" else None),
        limits=_amounts(f"cores={args.cores}" if args.cores else "", args.mem),
        reserve_always=_amounts(args.reserve), reserve_in_use=_amounts(args.reserve_in_use), on_use=args.on_use,
        idle_minutes=args.idle_minutes, default_hint=args.default_hint,
        min_free_disk=None if args.min_free_disk is None else _parse(parse_mem, args.min_free_disk),
        max_time=None if args.max_time is None else _parse(parse_duration, args.max_time),
    )
    try:
        executor = executor_for(entry, general, store=ctx.store)
        if isinstance(executor, SshExecutor):
            version = executor.python_version()
            if version is None:
                raise Refused(f"{entry.python} does not run on {entry.host}; pass --python")
            if version < MIN_PYTHON:
                raise Refused(f"{entry.host} has Python {version[0]}.{version[1]}; fila needs 3.9 or newer (--python)")
            executor.deploy()
        info = _checked_agent(executor)
        if args.os is not None and args.os != info["system"]:
            raise Refused(f"{args.name} runs {info['system']}, not {args.os}")
        entry.os = info["system"]
        probe = executor.probe()
        if args.speed is not None:
            entry.speed, entry.speed_source = args.speed, "manual"
        else:
            entry.speed, entry.speed_source = executor.benchmark()["speed"], "builtin"
    except (NodeUnreachable, AgentError) as error:
        raise Refused(str(error)) from None
    entries[args.name] = entry
    ctx.registry.save(general, entries)
    ctx.store.put_node(node_view(entry, probe, drained=False)[0])  # submit can check it before the first cycle
    ctx.wake_server()
    hardware = probe["hardware"]
    ctx.emit(
        {"name": args.name, "system": entry.os, "logical_cpus": hardware["logical_cpus"],
         "physical_cores": len(hardware["topology"]), "mem": hardware["total_mem"], "speed": entry.speed},
        f"registered {args.name} ({entry.os}): {hardware['logical_cpus']} logical CPUs on"
        f" {len(hardware['topology'])} physical cores, {format_mem(hardware['total_mem'])} memory,"
        f" speed {entry.speed} per core",
    )
    return EXIT_OK


def _register_cluster(ctx: Context, args: argparse.Namespace, general: dict, entries: dict) -> int:
    """Section 3.5: reach the login node from the via computer, read the
    partition's machines and limits, and make the jobs directory absolute
    (Slurm does not expand ``~`` in ``#SBATCH`` lines)."""
    for option in ("via", "host", "partition"):
        if not getattr(args, option):
            raise UsageError(f"Slurm clusters need --{option}")
    via = entries.get(args.via)
    if via is None or via.type != "computer":
        raise UsageError(f"--via {args.via} is not a registered computer")
    entry = NodeEntry(
        name=args.name, type="slurm", via=args.via, host=args.host, partition=args.partition, account=args.account,
        qos=args.qos, exclude=_names(args.exclude) if args.exclude else [], max_pending=args.max_pending,
        max_cores_per_job=args.max_cores_per_job, shell=_parse_shell(args.shell), jobs_dir=args.jobs_dir,
        limits=_amounts(f"cores={args.cores}" if args.cores else "", args.mem), default_hint=args.default_hint,
        speed=args.speed if args.speed is not None else 1.0, speed_source="manual" if args.speed is not None else "default",
        min_free_disk=None if args.min_free_disk is None else _parse(parse_mem, args.min_free_disk),
        max_time=None if args.max_time is None else _parse(parse_duration, args.max_time),
    )
    try:
        probe = executor_for(entry, general, entries=entries, store=ctx.store).probe()
    except (NodeUnreachable, AgentError) as error:
        raise Refused(str(error)) from None
    if not probe["partition_found"]:
        raise Refused(f"the cluster has no partition {args.partition}")
    if not probe["machines"]:
        raise Refused(f"partition {args.partition} lists no usable Slurm machines")
    if not probe["shell_ok"]:
        raise Refused(f"{entry.shell[0]} does not exist on the login node")
    entry.jobs_dir = probe["jobs_dir"]
    entry.max_time = entry.max_time or probe["max_time"]
    entries[args.name] = entry
    ctx.registry.save(general, entries)
    view = slurm_view(entry, probe, drained=False)
    ctx.store.put_node(view)
    ctx.wake_server()
    largest = max(probe["machines"], key=lambda m: m["total_cores"])
    limit = view.max_cores_per_job
    ctx.emit(
        {"name": args.name, "machines": len(probe["machines"]), "largest_machine": largest,
         "max_cores_per_job": limit, "max_time": entry.max_time, "jobs_dir": entry.jobs_dir},
        f"registered {args.name}: partition {args.partition}, {len(probe['machines'])} Slurm machines"
        f" (largest {largest['name']}: {largest['total_cores']} CPUs, {format_mem(largest['total_mem'])}),"
        f" {'no per-job core limit' if limit is None else f'at most {limit} cores per job'}, jobs in {entry.jobs_dir}",
    )
    return EXIT_OK


def _register_pull(ctx: Context, args: argparse.Namespace, general: dict, entries: dict) -> int:
    """Section 3.5: pairing. The node's key may only open this node's channel;
    the checks run once the channel is up."""
    if not args.key or not re.match(r"^(ssh-|ecdsa-|sk-)\S+ \S+", args.key.strip()):
        raise UsageError("pull nodes need --key with the public key `fila worker install` printed on the node")
    entry = NodeEntry(
        name=args.name, type="computer", access="pull", os=args.os, shell=_parse_shell(args.shell),
        jobs_dir=args.jobs_dir, limits=_amounts(f"cores={args.cores}" if args.cores else "", args.mem),
        reserve_always=_amounts(args.reserve), reserve_in_use=_amounts(args.reserve_in_use), on_use=args.on_use,
        idle_minutes=args.idle_minutes, default_hint=args.default_hint,
        min_free_disk=None if args.min_free_disk is None else _parse(parse_mem, args.min_free_disk),
        max_time=None if args.max_time is None else _parse(parse_duration, args.max_time),
    )
    entries[args.name] = entry
    ctx.registry.save(general, entries)  # the channel accepts only registered pull nodes
    path = _authorize(ctx.home, args.name, args.key.strip(), args.authorized_keys)
    executor = executor_for(entry, general, store=ctx.store)
    deadline = time.monotonic() + args.wait  # real time: the node connects in the real world
    while not executor.connected() and time.monotonic() < deadline:
        time.sleep(0.5)
    if not executor.connected():
        ctx.emit(
            {"name": args.name, "connected": False, "authorized_keys": path},
            f"registered {args.name}; its key may now open the channel ({path}).\n"
            f"The node has not connected yet: start its worker, then run `fila nodes validate {args.name}`.",
        )
        return EXIT_OK
    try:
        info = _checked_agent(executor)
        entry.os = info["system"]
        probe = executor.probe()
        if args.speed is not None:
            entry.speed, entry.speed_source = args.speed, "manual"
        else:
            entry.speed, entry.speed_source = executor.benchmark()["speed"], "builtin"
    except (NodeUnreachable, AgentError) as error:
        raise Refused(str(error)) from None
    ctx.registry.save(general, entries)
    ctx.store.put_node(node_view(entry, probe, drained=False)[0])
    ctx.wake_server()
    hardware = probe["hardware"]
    ctx.emit(
        {"name": args.name, "connected": True, "system": entry.os, "logical_cpus": hardware["logical_cpus"],
         "mem": hardware["total_mem"], "speed": entry.speed},
        f"registered {args.name} ({entry.os}) over its channel: {hardware['logical_cpus']} logical CPUs,"
        f" {format_mem(hardware['total_mem'])} memory, speed {entry.speed} per core",
    )
    return EXIT_OK


def _authorize(home: str, node: str, key: str, path: Optional[str]) -> str:
    """Add the node's key to authorized_keys, restricted to opening its own channel (section 13.2)."""
    path = path or os.path.expanduser("~/.ssh/authorized_keys")
    launcher = service.write_cli_launcher(home).replace("\\", "/")
    python = sys.executable.replace("\\", "/")
    command = f"{python} {launcher} internal channel --node {node}".replace('"', '\\"')
    line = f'restrict,command="{command}" {key}'
    os.makedirs(os.path.dirname(path), exist_ok=True)
    existing = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            existing = [old for old in handle.read().splitlines() if key.split()[1] not in old]  # replace, never duplicate
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write("\n".join(existing + [line]) + "\n")
    if os.name != "nt":
        os.chmod(os.path.dirname(path), 0o700)
        os.chmod(path, 0o600)
    return path


def _internal_channel(ctx: Optional[Context], args: argparse.Namespace) -> int:
    """The coordinator's end of a pull node's channel: started by its SSH login."""
    from .channel import serve_channel

    entry = Registry(os.path.join(args.home, "nodes.toml")).load()[1].get(args.node)
    if entry is None or entry.access != "pull":
        sys.stdout.write(json.dumps({"type": "error", "error": f"{args.node} is not a registered pull node"}) + "\n")
        return EXIT_REFUSED
    serve_channel(Store(os.path.join(args.home, "fila.db")), args.node, sys.stdin.buffer, sys.stdout.buffer,
                  os.path.join(args.home, "wake"))
    return EXIT_OK


WORKER_SSH_OPTIONS = ["-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=accept-new",
                      "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3"]


def _worker_install(ctx: Optional[Context], args: argparse.Namespace) -> int:
    """Node side of the pairing: a key of its own, the agent, the config and the service."""
    directory = os.path.abspath(args.dir or os.path.expanduser("~/.fila"))
    os.makedirs(directory, exist_ok=True)
    key = os.path.join(directory, "id_ed25519")
    if not os.path.exists(key):
        try:
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"fila-{args.node}", "-f", key],
                           check=True, capture_output=True)
        except (OSError, subprocess.CalledProcessError) as error:
            args.err.write(f"fila: cannot create the key with ssh-keygen: {error}\n")
            return EXIT_FAILED
    with open(key + ".pub", encoding="utf-8") as handle:
        public = handle.read().strip()
    archive = os.path.join(directory, "fila.pyz")
    with open(archive, "wb") as handle:
        handle.write(agent_archive())
    # The coordinator's host key is pinned on the first connection; a different one later is refused.
    ssh = ["ssh", "-i", key, "-o", f"UserKnownHostsFile={os.path.join(directory, 'known_hosts')}"] + WORKER_SSH_OPTIONS
    config = os.path.join(directory, "worker.toml")
    with open(config, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(f"coordinator = {json.dumps(args.coordinator)}\nnode = {json.dumps(args.node)}\n"
                     f"remote_command = {json.dumps(args.remote_command)}\nssh = {json.dumps(ssh)}\n")
    steps = []
    if not args.no_service:
        plan = service.install_plan(
            directory, name=args.service_name, args=["worker", "run", "--config", config], log="worker.log",
            launcher_name="worker_launcher.py", package_root=archive, description="fila pull worker",
        )
        problems = service.execute(plan)
        steps = [" ".join(command) for command in plan.commands] + [f"problem: {p}" for p in problems]
    register = (f'fila nodes register {args.node} --access pull --key "{public}" '
                '--shell "<the shell on this computer>" --jobs-dir "<jobs directory on this computer>"')
    args.out.write(json.dumps({"public_key": public, "config": config, "service": steps, "register": register}, indent=2)
                   if args.json else
                   f"worker set up in {directory}\n" + "".join(f"  {step}\n" for step in steps)
                   + f"\nOn the coordinator, run:\n  {register}\n")
    args.out.write("\n")
    return EXIT_OK


def _guide(ctx: Optional[Context], args: argparse.Namespace) -> int:
    from .guide import GUIDE

    args.out.write(GUIDE)
    return EXIT_OK


# Section 12.2: reading tools run freely; run and cancel ask (submit and retry follow Claude Code's default).
MCP_PERMISSIONS = {
    "allow": [f"mcp__fila__{tool}" for tool in ("status", "resources", "result", "log", "explain", "wait")],
    "ask": ["mcp__fila__run", "mcp__fila__cancel"],
}


def _mcp(ctx: Optional[Context], args: argparse.Namespace) -> int:
    from . import mcp

    if args.register:
        launcher = service.write_cli_launcher(args.home)
        command = ["claude", "mcp", "add", "--scope", "user", "fila", "--", sys.executable, launcher, "mcp"]
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        except (OSError, subprocess.TimeoutExpired) as error:
            args.err.write(f"fila: cannot run claude: {error}\n")
            return EXIT_FAILED
        args.out.write((result.stdout or result.stderr).strip() + "\n")
        args.out.write("\nSuggested permissions for .claude/settings.json:\n"
                       + json.dumps({"permissions": MCP_PERMISSIONS}, indent=2) + "\n")
        return EXIT_OK if result.returncode == 0 else EXIT_FAILED
    # The protocol is UTF-8 lines; never let Windows turn \n into \r\n.
    stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="\n", line_buffering=True)
    mcp.Server(mcp.run_in_process(args.home), stdin, stdout).serve()
    return EXIT_OK


def _worker_run(ctx: Optional[Context], args: argparse.Namespace) -> int:
    from .channel import run_worker

    path = args.config or os.path.expanduser("~/.fila/worker.toml")
    config = load_toml(path)

    def restart_after_update() -> None:
        sys.exit(75)  # the service starts the worker again, from the new agent

    run_worker(config["coordinator"], config["node"], config["ssh"], config.get("remote_command", "fila"),
               home_dir=os.path.dirname(os.path.abspath(path)), on_install=restart_after_update)
    return EXIT_OK


def _checked_agent(executor) -> dict:
    """The agent answers with this coordinator's protocol and the node's shell works."""
    info = executor.version()
    if info["protocol"] != agent.PROTOCOL:
        raise Refused(f"{executor.node} has agent protocol {info['protocol']}, this coordinator {agent.PROTOCOL}")
    shell = executor.check_shell()
    if not shell["ok"]:
        raise Refused(f"{executor.node}: {shell['message']}")
    return info


def _nodes_validate(ctx: Context, args: argparse.Namespace) -> int:
    """Section 3.5: redo the registration checks, e.g. after editing nodes.toml."""
    entries = ctx.entries()
    names = [args.name] if args.name else sorted(entries)
    if args.name:
        _registered(ctx, args.name)
    results = []
    for name in names:
        entry = entries[name]
        try:
            executor = executor_for(entry, ctx.registry.load()[0], entries=entries, store=ctx.store)
            if entry.type == "slurm":
                probe = executor.probe()
                if not probe["partition_found"] or not probe["shell_ok"]:
                    raise Refused(f"partition {entry.partition} or shell {entry.shell[0]} missing on the cluster")
            else:
                _checked_agent(executor)
            results.append({"name": name, "ok": True, "problem": None})
        except (NodeUnreachable, AgentError, Refused, ValueError) as error:
            results.append({"name": name, "ok": False, "problem": str(error)})
    ctx.emit(results, "\n".join(f"{r['name']:<14} {'ok' if r['ok'] else r['problem']}" for r in results) or "no nodes")
    return EXIT_OK if all(r["ok"] for r in results) else EXIT_FAILED


def _nodes_update(ctx: Context, args: argparse.Namespace) -> int:
    """Copy this coordinator's agent to ssh nodes (local nodes already run it)."""
    entries = ctx.entries()
    names = [args.name] if args.name else sorted(entries)
    if args.name:
        _registered(ctx, args.name)
    results = []
    for name in names:
        entry = entries[name]
        if entry.access not in ("ssh", "pull"):
            continue
        try:
            executor = executor_for(entry, ctx.registry.load()[0], store=ctx.store)
            if entry.access == "pull":  # the worker saves it and restarts from it
                executor.install_agent(agent_archive())
                results.append({"name": name, "ok": True, "problem": None})
                continue
            executor.deploy()
            _checked_agent(executor)
            results.append({"name": name, "ok": True, "problem": None})
        except (NodeUnreachable, AgentError, Refused) as error:
            results.append({"name": name, "ok": False, "problem": str(error)})
    ctx.wake_server()
    ctx.emit(results, "\n".join(f"{r['name']:<14} {'updated' if r['ok'] else r['problem']}" for r in results) or "no ssh nodes")
    return EXIT_OK if all(r["ok"] for r in results) else EXIT_FAILED


def _nodes_benchmark(ctx: Context, args: argparse.Namespace) -> int:
    """Section 7.4: the speed factor per core, from the built-in test or the user's command."""
    general, entries = ctx.registry.load()
    names = [args.name] if args.name else sorted(entries)
    if args.name:
        _registered(ctx, args.name)
    results = []
    for name in names:
        entry = entries[name]
        try:
            executor = executor_for(entry, general, entries=entries, store=ctx.store)
            if entry.type == "slurm" and not args.command:
                raise Refused("a cluster runs no fila code: measure it with --command (submitted with sbatch)")
            if args.command:
                timed = executor.time_command(args.command)
                if timed["exit_code"] != 0 or not timed["seconds"]:
                    raise Refused(f"the command failed on {name}: {timed['stderr'].strip()[-300:]}")
                entry.speed, entry.speed_source = round(COMMAND_REFERENCE_S / timed["seconds"], 3), "command"
            else:
                entry.speed, entry.speed_source = executor.benchmark()["speed"], "builtin"
            results.append({"name": name, "speed": entry.speed, "source": entry.speed_source, "problem": None})
        except (NodeUnreachable, AgentError, Refused, ValueError) as error:
            results.append({"name": name, "speed": None, "source": None, "problem": str(error)})
    ctx.registry.save(general, entries)
    ctx.wake_server()
    lines = [f"{r['name']:<14} " + (f"{r['speed']} ({r['source']})" if r["speed"] else r["problem"]) for r in results]
    if len({r["source"] for r in results if r["source"]}) > 1:
        lines.append("note: speeds from different tests are not comparable")
    ctx.emit(results, "\n".join(lines) or "no nodes")
    return EXIT_OK if all(r["speed"] for r in results) else EXIT_FAILED


COMMAND_REFERENCE_S = 100.0  # a user benchmark taking 100 s counts as speed 1.0


def _selftest(ctx: Context, args: argparse.Namespace) -> int:
    """Section 14: run the enforcement checks inside each node."""
    general, entries = ctx.registry.load()
    names = _run_targets(ctx, entries, args.on)
    report, lines = [], []
    for name in names:
        try:
            results = executor_for(entries[name], general, store=ctx.store).selftest()
        except (NodeUnreachable, AgentError, ValueError) as error:
            results = [{"check": "reach", "ok": False, "message": str(error)}]
        report.append({"node": name, "checks": results})
        lines.append(f"== {name}")
        for check in results:
            mark = {True: "ok", False: "FAIL", None: "n/a"}[check["ok"]]
            lines.append(f"  {mark:<4} {check['check']}" + (f"  ({check['message']})" if check["ok"] is not True and check["message"] else ""))
    ctx.emit(report, "\n".join(lines) or "no nodes")
    failed = any(check["ok"] is False for node in report for check in node["checks"])
    return EXIT_FAILED if failed else EXIT_OK


def _parse_shell(text: str) -> List[str]:
    """A JSON list, or a command line; Windows paths keep their backslashes."""
    if text.strip().startswith("["):
        try:
            shell = json.loads(text)
        except ValueError as error:
            raise UsageError(f"--shell: {error}") from None
    else:
        shell = [part.strip('"') for part in shlex.split(text, posix=os.name != "nt")]
    if not shell or not all(isinstance(part, str) for part in shell):
        raise UsageError("--shell needs a program and its arguments")
    return shell


def _amounts(*texts: Optional[str]) -> dict:
    """``"cores=1,mem=4G"`` (and a bare memory size) to {"cores": 1, "mem": bytes}."""
    amounts: dict = {}
    for text in texts:
        if not text:
            continue
        if "=" not in text:
            amounts["mem"] = _parse(parse_mem, text)
            continue
        for item in _names(text):
            name, _, value = item.partition("=")
            if name == "cores":
                amounts["cores"] = int(value)
            elif name == "mem":
                amounts["mem"] = _parse(parse_mem, value)
            else:
                raise UsageError(f"unknown resource {name!r} (use cores and mem)")
    return amounts


def _nodes_list(ctx: Context, args: argparse.Namespace) -> int:
    views = {node.name: node for node in ctx.store.nodes()}
    rows = []
    for name, entry in sorted(ctx.entries().items()):
        view = views.get(name)
        rows.append({
            "name": name,
            "type": entry.type,
            "access": entry.access,
            "state": view.state if view else "unknown",
            "capacity": dict(view.capacity) if view else None,
            "free": dict(view.probe_free - view.reserve) if view else None,
            "speed": entry.speed,
        })
    lines = [
        f"{row['name']:<14} {row['access'] or row['type']:<6} {row['state']:<10}"
        + (f" {row['capacity']['cores']:>3}c {format_mem(row['capacity']['mem']):>6}" if row["capacity"] else "")
        for row in rows
    ]
    ctx.emit(rows, "\n".join(lines) or "no nodes registered")
    return EXIT_OK


def _registered(ctx: Context, name: str):
    entry = ctx.entries().get(name)
    if entry is None:
        raise UsageError(f"unknown node: {name}")
    return entry


def _nodes_drain(ctx: Context, args: argparse.Namespace) -> int:
    _registered(ctx, args.name)
    ctx.store.set_drained(args.name, True)
    ctx.wake_server()
    ctx.emit({"name": args.name, "drained": True}, f"{args.name} drained: running jobs continue, no new ones start")
    return EXIT_OK


def _nodes_resume(ctx: Context, args: argparse.Namespace) -> int:
    _registered(ctx, args.name)
    ctx.store.set_drained(args.name, False)
    ctx.wake_server()
    ctx.emit({"name": args.name, "drained": False}, f"{args.name} receives jobs again")
    return EXIT_OK


def _nodes_remove(ctx: Context, args: argparse.Namespace) -> int:
    _registered(ctx, args.name)
    busy = [job.id for job, attempt in ctx.store.active_attempts() if attempt["node"] == args.name]
    if busy:
        raise Refused(f"{args.name} still has jobs: {', '.join(map(str, busy))}")
    general, entries = ctx.registry.load()
    del entries[args.name]
    ctx.registry.save(general, entries)
    ctx.store.keep_nodes(entries)
    ctx.emit({"name": args.name, "removed": True}, f"{args.name} removed")
    return EXIT_OK


def _server(ctx: Context, args: argparse.Namespace) -> int:
    if args.install or args.uninstall:
        if args.install and args.uninstall:
            raise UsageError("choose --install or --uninstall")
        if args.install:
            plan = service.install_plan(ctx.home, name=args.service_name)
        else:
            plan = service.uninstall_plan(ctx.home, name=args.service_name)
        steps = [f"write {path}" for path in plan.files] + [" ".join(command) for command in plan.commands]
        problems = [] if args.dry_run else service.execute(plan)
        verb = "would" if args.dry_run else ("installed" if args.install else "removed")
        ctx.emit(
            {"system": service.system(), "steps": steps, "notes": plan.notes, "problems": problems, "dry_run": args.dry_run},
            "\n".join([f"{verb} ({service.system()}):"] + [f"  {step}" for step in steps] + plan.notes
                      + [f"problem: {problem}" for problem in problems]),
        )
        return EXIT_FAILED if problems else EXIT_OK
    server = Server(ctx.home, clock=ctx.clock)
    if args.once:
        applied = server.cycle()
        ctx.emit({"applied": len(applied)}, f"one cycle: {len(applied)} decisions applied")
        return EXIT_OK
    try:
        server.serve()
    except KeyboardInterrupt:
        pass
    return EXIT_OK


def _internal_supervise(ctx: Optional[Context], args: argparse.Namespace) -> int:
    supervisor.supervise(args.attempt_dir)
    return EXIT_OK


def _internal_agent(ctx: Optional[Context], args: argparse.Namespace) -> int:
    """One agent operation: JSON arguments on stdin, ``{"ok", "result"|"error"}`` on stdout."""
    try:
        request = json.loads(sys.stdin.read() or "{}")
        answer = {"ok": True, "result": agent.OPERATIONS[args.operation](request)}
    except NodeUnreachable as error:  # e.g. the cluster behind this node
        answer = {"ok": False, "kind": "unreachable", "error": str(error)}
    except Exception as error:  # reported to the coordinator, which decides what it means
        answer = {"ok": False, "error": f"{type(error).__name__}: {error}"}
    sys.stdout.write(json.dumps(answer))
    sys.stdout.flush()
    return EXIT_OK if answer["ok"] else EXIT_FAILED


# --- Helpers ---


def _submit_spec(ctx: Context, args: argparse.Namespace) -> dict:
    """The submit options as stored with the job; validates names and ids."""
    spec: dict = {}
    if args.time is not None:
        spec["time_limit"] = _parse(parse_duration, args.time)
    if args.retry_after is not None:
        spec["retry_after"] = _parse(parse_duration, args.retry_after)
    if args.on is not None:
        spec["on"] = _names(args.on)
        _check_nodes(ctx, spec["on"])
    if args.after is not None:
        try:
            spec["after"] = [int(item) for item in _names(args.after)]
        except ValueError:
            raise UsageError(f"--after takes job ids, got {args.after!r}") from None
        missing = set(spec["after"]) - set(ctx.store.job_ids())
        if missing:
            raise UsageError(f"unknown job: {', '.join(str(i) for i in sorted(missing))}")
    # Line endings become \n: a script saved on Windows must still run in bash.
    if args.script == "-":
        data = sys.stdin.buffer.read() if hasattr(sys.stdin, "buffer") else sys.stdin.read().encode("utf-8")
        content = data.decode("utf-8").replace("\r\n", "\n")
        spec["script"] = {"name": args.script_name or "script", "content": content}
    elif args.script is not None:
        try:
            with open(args.script, encoding="utf-8", newline="") as handle:
                content = handle.read().replace("\r\n", "\n")
        except OSError as error:
            raise UsageError(f"cannot read --script: {error}") from None
        spec["script"] = {"name": args.script_name or os.path.basename(args.script), "content": content}
    for option in ("dir", "even_if_failed", "on_use", "hint", "slurm_extra"):
        value = getattr(args, option)
        if value not in (None, False):
            spec[option] = value
    for option in ("slurm_nodelist", "slurm_exclude"):
        if getattr(args, option) is not None:
            spec[option] = _names(getattr(args, option))
    return spec


def _check_nodes(ctx: Context, names: List[str]) -> None:
    unknown = set(names) - {node.name for node in ctx.store.nodes()}
    if unknown:
        raise UsageError(f"unknown node: {', '.join(sorted(unknown))}")


def _command_text(parts: List[str], required: bool = True) -> str:
    """One argument is a shell command as written; several are joined with POSIX quoting."""
    if parts and parts[0] == "--":
        parts = parts[1:]
    if not parts:
        if required:
            raise UsageError("missing command after --")
        return ""
    return parts[0] if len(parts) == 1 else shlex.join(parts)


def _parse(parser: Callable, text: str):
    try:
        return parser(text)
    except ValueError as error:
        raise UsageError(str(error)) from None


def _names(text: str) -> List[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def _tuple(items: Optional[List]) -> Optional[tuple]:
    return None if items is None else tuple(items)


def _created_by(environ: Mapping[str, str]) -> str:
    return "claude-code" if environ.get("CLAUDECODE") else "user"


def _current_node(job: JobRecord) -> Optional[str]:
    return job.attempts[-1]["node"] if job.attempts else None


def _when(timestamp: Optional[float]) -> str:
    if timestamp is None:
        return "-"
    return datetime.datetime.fromtimestamp(timestamp).isoformat(sep=" ", timespec="seconds")


def _job_json(job: JobRecord) -> dict:
    return {
        "id": job.id,
        "name": job.name,
        "state": job.state,
        "reason": job.reason,
        "command": job.command,
        "request": dict(job.request),
        "node": _current_node(job),
        "attempt": len(job.attempts),
        "created_by": job.created_by,
        "submitted_at": job.submitted_at,
    }


if __name__ == "__main__":
    entry()
