"""Slurm clusters (sections 3.2 and 8).

A cluster runs no fila code. Its ``via`` computer's agent reaches the login
node over SSH and runs only Slurm's own commands (``sinfo``, ``scontrol``,
``sacctmgr``, ``sbatch``, ``squeue``, ``sacct``, ``scancel``) and ``tail``,
``df``, ``rm`` on fila's own files. The coordinator writes the batch script.

Three parts: pure functions (the script, the parsers), the agent operation
(``agent_operation``, run on the via computer) and ``SlurmExecutor`` on the
coordinator.
"""

from __future__ import annotations

import re
import shlex
import subprocess
import time
from typing import Dict, List, Optional, Tuple

from .errors import AgentError, NodeUnreachable
from .units import format_mem, parse_duration

MiB = 1024**2

# Options fila owns: --slurm-extra may not change them, or accounting would lie (section 8).
OWNED_OPTIONS = ("--cpus-per-task", "--mem", "--mem-per-cpu", "--time", "--nodes", "--ntasks", "--output",
                 "--error", "--chdir", "-c", "-t", "-N", "-n", "-o", "-e", "-D")

# sacct/squeue state -> (fila state, reason)
_FINAL = {
    "COMPLETED": ("completed", None),
    "FAILED": ("failed", "exit_code"),
    "OUT_OF_MEMORY": ("failed", "out_of_memory"),
    "TIMEOUT": ("failed", "timeout"),
    "DEADLINE": ("failed", "timeout"),
    "NODE_FAIL": ("failed", "node_failure"),
    "BOOT_FAIL": ("failed", "node_failure"),
    "PREEMPTED": ("failed", "lost"),
    "CANCELLED": ("cancelled", "requested"),
}
_RUNNING = ("RUNNING", "COMPLETING", "CONFIGURING", "STAGE_OUT", "SUSPENDED", "RESIZING")
_UNUSABLE = ("DOWN", "DRAIN", "FAIL", "MAINT", "NOT_RESPONDING", "FUTURE", "POWER", "RESERVED", "UNKNOWN")


# --- The batch script (section 8) ---


def sbatch_script(job: int, attempt: int, command: str, request: dict, spec: dict, entry, node: str) -> str:
    attempt_dir = f"{entry.jobs_dir}/{job}/attempt-{attempt}"
    lines = [shebang(entry.shell), f"#SBATCH --job-name=fila-{job}", f"#SBATCH --partition={entry.partition}"]
    if entry.account:
        lines.append(f"#SBATCH --account={entry.account}")
    if entry.qos:
        lines.append(f"#SBATCH --qos={entry.qos}")
    lines += [
        "#SBATCH --nodes=1",
        "#SBATCH --ntasks=1",
        f"#SBATCH --cpus-per-task={request['cores']}",
        f"#SBATCH --mem={-(-request['mem'] // MiB)}M",
    ]
    time_limit = spec.get("time_limit", entry.max_time)
    if time_limit:
        lines.append(f"#SBATCH --time={slurm_time(time_limit)}")
    lines += [
        f"#SBATCH --chdir={spec.get('dir') or f'{entry.jobs_dir}/{job}'}",
        f"#SBATCH --output={attempt_dir}/stdout.log",
        f"#SBATCH --error={attempt_dir}/stderr.log",
    ]
    if spec.get("hint") or entry.default_hint != "multithread":
        lines.append(f"#SBATCH --hint={spec.get('hint') or entry.default_hint}")
    if spec.get("slurm_nodelist"):
        lines.append(f"#SBATCH --nodelist={','.join(spec['slurm_nodelist'])}")
    exclude = list(entry.exclude or []) + list(spec.get("slurm_exclude", []))
    if exclude:
        lines.append(f"#SBATCH --exclude={','.join(dict.fromkeys(exclude))}")
    lines += [f"#SBATCH {option}" for option in shlex.split(spec.get("slurm_extra") or "")]
    variables = {
        "FILA_JOB_ID": job, "FILA_ATTEMPT": attempt, "FILA_CORES": request["cores"],
        "FILA_MEM": format_mem(request["mem"]), "FILA_NODE": node,
    }
    lines.append("export " + " ".join(f"{name}={shlex.quote(str(value))}" for name, value in variables.items()))
    script = spec.get("script")
    if script is not None:
        lines += [f"cat > {shlex.quote(script['name'])} <<'FILA_SCRIPT_EOF'", script["content"].rstrip("\n"),
                  "FILA_SCRIPT_EOF", f"chmod +x {shlex.quote(script['name'])}"]
        if command == script["name"]:
            command = f"./{shlex.quote(script['name'])}"
    lines.append(command)
    return "\n".join(lines) + "\n"


def shebang(shell: List[str]) -> str:
    """``#!/bin/bash -l`` from a shell like ``["/bin/bash", "-lc"]``: a script needs no ``-c``."""
    flags = [flag.replace("c", "") if flag.startswith("-") else flag for flag in shell[1:]]
    flags = [flag for flag in flags if flag not in ("-", "")]
    return "#!" + " ".join([shell[0]] + flags[:1])


def slurm_time(seconds: float) -> str:
    seconds = int(-(-seconds // 1))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"


def check_extra(text: Optional[str]) -> Optional[str]:
    """Why ``--slurm-extra`` is refused, or None."""
    for option in shlex.split(text or ""):
        name = option.split("=", 1)[0]
        if name in OWNED_OPTIONS:
            return f"--slurm-extra may not set {name}: fila sets it from --cores, --mem, --time and --dir"
    return None


# --- Parsers of Slurm's output ---


def _fields(line: str) -> Dict[str, str]:
    return dict(re.findall(r"(\w+)=(\S*)", line))


def parse_machines(names_text: str, nodes_text: str) -> List[dict]:
    """``sinfo -N -o %N`` (the partition's nodes) and ``scontrol show node -o``."""
    names = {line.strip() for line in names_text.splitlines() if line.strip()}
    machines = []
    for line in nodes_text.splitlines():
        fields = _fields(line)
        if fields.get("NodeName") not in names:
            continue
        total_cores, total_mem = int(fields.get("CPUTot", 0)), int(fields.get("RealMemory", 0)) * MiB
        usable = not any(word in fields.get("State", "UNKNOWN") for word in _UNUSABLE)
        machines.append({
            "name": fields["NodeName"],
            "idle_cores": total_cores - int(fields.get("CPUAlloc", 0)) if usable else 0,
            "free_mem": total_mem - int(fields.get("AllocMem", 0)) * MiB if usable else 0,
            "total_cores": total_cores,
            "total_mem": total_mem,
        })
    return sorted(machines, key=lambda machine: machine["name"])


def parse_limits(partition_text: str, qos_text: str, qos: Optional[str]) -> Tuple[Optional[int], Optional[float]]:
    """(most cores per job, most time) from the partition and its QoS (or the registered one)."""
    fields = _fields(partition_text)
    cores = None
    if fields.get("MaxCPUsPerNode", "UNLIMITED").isdigit():
        cores = int(fields["MaxCPUsPerNode"])
    wanted = qos or fields.get("QoS")
    for line in qos_text.splitlines():
        parts = line.split("|")
        if len(parts) >= 2 and parts[0] == wanted:
            match = re.search(r"cpu=(\d+)", parts[1])
            if match:
                cores = min(cores, int(match.group(1))) if cores else int(match.group(1))
    max_time = None
    if fields.get("MaxTime") not in (None, "UNLIMITED", "INFINITE"):
        try:
            max_time = parse_duration(fields["MaxTime"])
        except ValueError:
            pass
    return cores, max_time


def parse_states(queue_text: str, sacct_text: str, ids: List[str]) -> Dict[str, dict]:
    """``squeue -o '%i|%T'`` and ``sacct -P -X -o JobID,State,ExitCode,Start,End``.
    sacct wins when it knows the job; squeue covers a slow or missing accounting."""
    result: Dict[str, dict] = {}
    for line in queue_text.splitlines():
        parts = line.strip().split("|")
        if len(parts) == 2 and parts[0] in ids:
            result[parts[0]] = {"state": parts[1].split()[0], "exit_code": None, "start": None, "end": None}
    for line in sacct_text.splitlines():
        parts = line.strip().split("|")
        if len(parts) < 5 or parts[0] not in ids:
            continue
        code, _, signal = parts[2].partition(":")
        exit_code = int(code) if code.isdigit() else None
        if exit_code == 0 and signal.isdigit() and int(signal):
            exit_code = 128 + int(signal)
        result[parts[0]] = {
            "state": parts[1].split()[0], "exit_code": exit_code, "start": _epoch(parts[3]), "end": _epoch(parts[4]),
        }
    return result


def outcome(state: str) -> Optional[tuple]:
    """(fila state, reason) for a final Slurm state; "running"/"pending" otherwise."""
    if state in _FINAL:
        return _FINAL[state]
    if state in _RUNNING:
        return ("running", None)
    return ("pending", None)


def _epoch(text: str) -> Optional[float]:
    try:
        return time.mktime(time.strptime(text, "%Y-%m-%dT%H:%M:%S"))
    except ValueError:
        return None


# --- Agent side: run on the via computer ---


def _remote(args: dict, command: str, data: bytes = b"", timeout: float = 120.0) -> tuple:
    try:
        result = subprocess.run(
            list(args["ssh_command"]) + [args["host"], command], input=data, capture_output=True, timeout=timeout
        )
    except subprocess.TimeoutExpired:
        raise NodeUnreachable(f"{args['host']}: no answer in {timeout:.0f} s") from None
    except OSError as error:
        raise NodeUnreachable(f"{args['host']}: cannot start ssh: {error}") from None
    if result.returncode == 255:
        raise NodeUnreachable(f"{args['host']}: {result.stderr.decode('utf-8', 'replace').strip()}")
    return result.returncode, result.stdout.decode("utf-8", "replace"), result.stderr.decode("utf-8", "replace")


def _path(path: str) -> str:
    """A remote path for the login shell: ``~/x`` keeps ``$HOME`` unquoted."""
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        return '"$HOME"/' + shlex.quote(path[2:])
    return shlex.quote(path)


def agent_operation(args: dict):
    action = args["action"]
    if action == "probe":
        return _probe(args)
    if action == "submit":
        directory = _path(f"{args['jobs_dir']}/{args['job']}/attempt-{args['attempt']}")
        code, out, err = _remote(
            args, f"mkdir -p {directory} && cat > {directory}/job.sbatch && sbatch --parsable {directory}/job.sbatch",
            args["script"].encode("utf-8"),
        )
        if code != 0:
            raise AgentError(f"sbatch refused the job: {err.strip() or out.strip()}")
        return {"slurm_id": out.strip().split(";")[0]}
    if action == "states":
        ids = [str(item) for item in args["ids"]]
        code, out, _ = _remote(
            args, "squeue -h -u \"$USER\" -o '%i|%T'; echo @@FILA@@; "
            f"sacct -n -P -X -j {','.join(ids) or '0'} -o JobID,State,ExitCode,Start,End 2>/dev/null; true",
        )
        queue, _, sacct = out.partition("@@FILA@@")
        return parse_states(queue, sacct, ids)
    if action == "cancel":
        _remote(args, f"scancel {shlex.quote(str(args['slurm_id']))}")
        return {}
    if action == "read_log":
        return _read_log(args)
    if action == "clean":
        return _clean(args)
    if action == "time_command":
        return _time_command(args)
    raise AgentError(f"unknown slurm action: {action}")


def _probe(args: dict) -> dict:
    partition = shlex.quote(args["partition"])
    jobs_dir = _path(args["jobs_dir"])
    sections = [
        'echo "$HOME"',
        f"mkdir -p {jobs_dir} && cd {jobs_dir} && pwd && df -Pk . | tail -1",
        f"sinfo -h -N -p {partition} -o %N",
        "scontrol show node -o",
        f"scontrol show partition {partition} -o",
        "sacctmgr -n -P show qos format=Name,MaxTRES,MaxWall 2>/dev/null",
        f"command -v {shlex.quote(args['shell'][0])} >/dev/null && echo shell-ok",
    ]
    code, out, err = _remote(args, "; echo @@FILA@@; ".join(sections))
    parts = [part.strip("\n") for part in out.split("@@FILA@@")]
    if len(parts) != len(sections):
        raise AgentError(f"unexpected answer from the login node: {err.strip() or out.strip()[:300]}")
    home, disk, names, nodes, partition_text, qos_text, shell = parts
    disk_lines = disk.strip().splitlines()
    if len(disk_lines) < 2:
        raise AgentError(f"cannot use {args['jobs_dir']} on the cluster: {err.strip()}")
    max_cores, max_time = parse_limits(partition_text, qos_text, args.get("qos"))
    return {
        "home": home.strip(),
        "jobs_dir": disk_lines[0].strip(),
        "disk_free": int(disk_lines[-1].split()[3]) * 1024,
        "partition_found": "PartitionName=" in partition_text,
        "machines": parse_machines(names, nodes),
        "max_cores_per_job": max_cores,
        "max_time": max_time,
        "shell_ok": shell.strip() == "shell-ok",
    }


def _read_log(args: dict) -> dict:
    path = f"{args['jobs_dir']}/{args['job']}/attempt-{args['attempt']}/{args.get('stream', 'stdout')}.log"
    quoted = _path(path)
    if args.get("tail") is not None:
        read = f"tail -c {int(args['tail'])} {quoted}"
    else:
        read = f"tail -c +{int(args.get('offset', 0)) + 1} {quoted}"
    _, out, _ = _remote(
        args,
        f"if [ -f {quoted} ]; then wc -c < {quoted}; fi; echo @@FILA@@; if [ -f {quoted} ]; then {read}; fi;"
        f" echo @@FILA@@; squeue -h -n fila-{int(args['job'])} -o %T 2>/dev/null; true",
    )
    size, text, queue = (out.split("@@FILA@@") + ["", ""])[:3]
    text = text[1:] if text.startswith("\n") else text
    size = int(size.strip() or 0)
    offset = size if args.get("tail") is not None else int(args.get("offset", 0)) + len(text.encode("utf-8"))
    return {"text": text, "offset": min(offset, size), "path": path, "finished": not queue.strip()}


def _clean(args: dict) -> dict:
    job_dir = _path(f"{args['jobs_dir']}/{int(args['job'])}")
    if args.get("logs_only"):
        command = (f"if [ -d {job_dir} ]; then cd {job_dir} && du -ck attempt-*/stdout.log attempt-*/stderr.log"
                   " 2>/dev/null | tail -1 | cut -f1; rm -f attempt-*/stdout.log attempt-*/stderr.log; fi")
    else:
        command = f"if [ -d {job_dir} ]; then du -sk {job_dir} | cut -f1; rm -rf -- {job_dir}; fi"
    _, out, _ = _remote(args, command)
    return {"freed": int(out.split()[0]) * 1024 if out.split() else None}


def _time_command(args: dict) -> dict:
    """A user benchmark as a Slurm job (``sbatch --wait``): its elapsed time on a compute node."""
    directory = _path(f"{args['jobs_dir']}/benchmark")
    code, out, err = _remote(
        args,
        f"mkdir -p {directory} && cat > {directory}/bench.sbatch && "
        f"ID=$(sbatch --parsable --wait {directory}/bench.sbatch); ID=${{ID%%;*}}; "
        "sacct -n -P -X -j \"$ID\" -o ElapsedRaw,State",
        args["script"].encode("utf-8"),
        timeout=float(args.get("timeout", 3600)),
    )
    line = out.strip().splitlines()[-1] if out.strip() else ""
    parts = line.split("|")
    if len(parts) < 2 or not parts[0].isdigit():
        return {"seconds": None, "exit_code": 1, "stderr": err or out}
    return {"seconds": float(parts[0]), "exit_code": 0 if parts[1].startswith("COMPLETED") else 1, "stderr": err}


# --- Coordinator side ---


class SlurmExecutor:
    """A registered cluster, reached through its via computer's executor."""

    def __init__(self, entry, via, ssh_command: List[str]) -> None:
        self.entry = entry
        self.node = entry.name
        self.via = via
        self.ssh_command = list(ssh_command)

    def _call(self, action: str, **args):
        return self.via.call("slurm", dict(
            action=action, host=self.entry.host, ssh_command=self.ssh_command, jobs_dir=self.entry.jobs_dir, **args
        ))

    def probe(self) -> dict:
        return self._call("probe", partition=self.entry.partition, shell=self.entry.shell, qos=self.entry.qos)

    def submit(self, job: int, attempt: int, command: str, request: dict, spec: dict) -> str:
        script = sbatch_script(job, attempt, command, request, spec, self.entry, self.node)
        return self._call("submit", job=job, attempt=attempt, script=script)["slurm_id"]

    def states(self, slurm_ids: List[str]) -> Dict[str, dict]:
        return self._call("states", ids=list(slurm_ids)) if slurm_ids else {}

    def cancel(self, slurm_id: str) -> None:
        self._call("cancel", slurm_id=slurm_id)

    def read_log(self, job: int, attempt: int, stream: str = "stdout", offset: int = 0, tail: Optional[int] = None) -> dict:
        return self._call("read_log", job=job, attempt=attempt, stream=stream, offset=offset, tail=tail)

    def clean(self, job: int, logs_only: bool) -> Optional[int]:
        return self._call("clean", job=job, logs_only=logs_only)["freed"]

    def time_command(self, command: str) -> dict:
        request = {"cores": 1, "mem": 1024 * MiB}
        spec = {"dir": f"{self.entry.jobs_dir}/benchmark", "time_limit": 3600}
        script = sbatch_script(0, 0, command, request, spec, self.entry, self.node).replace(
            f"{self.entry.jobs_dir}/0/attempt-0", f"{self.entry.jobs_dir}/benchmark"
        )
        return self._call("time_command", script=script)
