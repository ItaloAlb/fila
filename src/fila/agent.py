"""Everything that happens inside a computer node (section 4.2).

The coordinator calls these operations directly for a ``local`` node and as
``fila internal agent <operation>`` (JSON on stdin and stdout) over SSH for an
``ssh`` node. Layout on the node (section 10)::

    <jobs_dir>/<job id>/                 default working directory, shared by attempts
    <jobs_dir>/<job id>/attempt-<n>/     job.json, status.json, stdout.log, stderr.log
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
import time
import zlib
from typing import Callable, Dict, List, Optional

from . import __version__, slurm, supervisor
from .probe import LocalProbe
from .units import format_mem

# Raised when coordinator and agent must change together (section 4.2).
PROTOCOL = 1
# Seconds a just-launched supervisor may take to write its first status.
START_GRACE_S = 60.0
FILA_LOGS = ("stdout.log", "stderr.log", "supervisor.log")

_default_probe: Optional[LocalProbe] = None


def _probe(probe=None):
    global _default_probe
    if probe is not None:
        return probe
    if _default_probe is None:
        _default_probe = LocalProbe()
    return _default_probe


def _jobs_dir(args: dict) -> str:
    return os.path.expanduser(args["jobs_dir"])


def job_dir(jobs_dir: str, job: int) -> str:
    return os.path.join(os.path.expanduser(jobs_dir), str(job))


def attempt_dir(jobs_dir: str, job: int, attempt: int) -> str:
    return os.path.join(job_dir(jobs_dir, job), f"attempt-{attempt}")


# --- Operations ---


def version(args: dict = None) -> dict:
    return {
        "protocol": PROTOCOL,
        "fila": __version__,
        "python": platform.python_version(),
        "system": {"win32": "windows", "darwin": "macos"}.get(sys.platform, "linux"),
    }


def probe(args: dict, probe=None) -> dict:
    """Hardware, current reading and free disk in the jobs directory."""
    source = _probe(probe)
    hardware = source.hardware()
    reading = source.sample()
    jobs_dir = _jobs_dir(args)
    os.makedirs(jobs_dir, exist_ok=True)
    return dict(
        version(),
        hardware={
            "logical_cpus": hardware.logical_cpus,
            "topology": [list(core) for core in hardware.topology],
            "total_mem": hardware.total_mem,
        },
        reading={
            "free_cores": reading.free_cores,
            "available_mem": reading.available_mem,
            "idle_seconds": None if reading.idle_seconds == float("inf") else reading.idle_seconds,
        },
        disk_free=source.disk_free(jobs_dir),
    )


def launch(args: dict) -> dict:
    """Write the attempt's ``job.json`` and start its detached supervisor."""
    jobs_dir, job, attempt = _jobs_dir(args), args["job"], args["attempt"]
    directory = attempt_dir(jobs_dir, job, attempt)
    os.makedirs(directory, exist_ok=True)
    command = args["command"]
    script = args.get("script")
    if script is not None:
        path = os.path.join(job_dir(jobs_dir, job), script["name"])
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(script["content"])
        if os.name != "nt":
            os.chmod(path, 0o755)
        if command == script["name"]:
            command = path
    request = args.get("request", {})
    cwd = os.path.expanduser(args["dir"]) if args.get("dir") else job_dir(jobs_dir, job)
    supervisor.write_spec(
        directory,
        {
            "command": command,
            "shell": args["shell"],
            "cwd": cwd,
            "cpus": list(args.get("cpus", [])),
            "mem": request.get("mem") or None,  # a run without --mem has no limit
            "time_limit": args.get("time_limit"),
            "grace_s": 30,
            "notify": args.get("notify"),
            "env": {
                "FILA_JOB_ID": str(job),
                "FILA_ATTEMPT": str(attempt),
                "FILA_CORES": str(request.get("cores", 0)),
                "FILA_MEM": format_mem(request["mem"]) if request.get("mem") else "",
                "FILA_NODE": args["node"],
            },
        },
    )
    _spawn_detached([sys.executable, "-m", "fila", "internal", "supervise", directory], directory)
    return {"attempt_dir": directory}


def statuses(args: dict) -> List[Optional[dict]]:
    """The diary of each ``{job, attempt, launched_at}``: None while a fresh
    supervisor has not written yet, ``{"state": "lost"}`` when it died
    without finishing. ``now`` is the coordinator's clock."""
    jobs_dir, now = _jobs_dir(args), args["now"]
    result = []
    for item in args["attempts"]:
        directory = attempt_dir(jobs_dir, item["job"], item["attempt"])
        status = supervisor.read_status(directory)
        if status is None:
            launched_at = item.get("launched_at")
            result.append({"state": "lost"} if launched_at is not None and now - launched_at > START_GRACE_S else None)
            continue
        if status["state"] != "finished" and not supervisor.pid_alive(status["supervisor_pid"]):
            status = supervisor.read_status(directory)  # it may have finished just now
            if status["state"] != "finished":
                status = dict(status, state="lost")
        result.append(status)
    return result


def cycle(args: dict, probe=None) -> dict:
    """One round trip per node and server cycle: the probe and every diary."""
    return {"probe": globals()["probe"](args, probe), "statuses": statuses(args)}


def request(args: dict) -> dict:
    supervisor.request(attempt_dir(_jobs_dir(args), args["job"], args["attempt"]), args["what"])
    return {}


def read_log(args: dict) -> dict:
    """``limit`` bytes from ``offset`` (or the last ``tail`` bytes) of a log; the
    attempt's state tells a follower when to stop."""
    directory = attempt_dir(_jobs_dir(args), args["job"], args["attempt"])
    path = os.path.join(directory, f"{args.get('stream', 'stdout')}.log")
    try:
        size = os.path.getsize(path)
        offset = max(0, size - args["tail"]) if args.get("tail") is not None else args.get("offset", 0)
        with open(path, "rb") as handle:
            handle.seek(offset)
            data = handle.read(args.get("limit", 16 * 1024 * 1024))
    except FileNotFoundError:
        data, offset = b"", args.get("offset", 0)
    status = supervisor.read_status(directory) or {}
    return {
        "text": data.decode("utf-8", errors="replace"),
        "offset": offset + len(data),
        "path": path,
        "finished": status.get("state") == "finished",
    }


def clean(args: dict) -> dict:
    """Delete ``<jobs_dir>/<job>/`` (or only fila's logs in it); never anything else."""
    root = os.path.realpath(_jobs_dir(args))
    directory = os.path.realpath(job_dir(root, args["job"]))
    if os.path.dirname(directory) != root or not os.path.isdir(directory):
        return {"freed": None}
    if args.get("logs_only"):
        targets = [
            os.path.join(directory, attempt, name)
            for attempt in os.listdir(directory)
            if attempt.startswith("attempt-")
            for name in FILA_LOGS
            if os.path.exists(os.path.join(directory, attempt, name))
        ]
        freed = sum(os.path.getsize(path) for path in targets)
        for path in targets:
            os.remove(path)
        return {"freed": freed if targets else None}
    freed = sum(os.path.getsize(os.path.join(folder, name)) for folder, _, names in os.walk(directory) for name in names)
    shutil.rmtree(directory)
    return {"freed": freed}


def check_shell(args: dict) -> dict:
    try:
        result = subprocess.run(list(args["shell"]) + ["echo ok"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"ok": False, "message": f"the shell does not run: {error}"}
    if result.returncode != 0 or "ok" not in result.stdout:
        return {"ok": False, "message": f"the shell did not answer 'echo ok': {(result.stderr or result.stdout).strip()[-300:]}"}
    return {"ok": True, "message": ""}


# Fixed work for the built-in speed test (section 7.4): zlib in C, one core.
_BENCH_WORK = 24
_BENCH_REFERENCE_S = 2.0  # this work in 2 s counts as speed 1.0


def benchmark(args: dict = None) -> dict:
    words = [f"{i * 2654435761 % 4294967296:08x}".encode() for i in range(4096)]
    block = b" ".join(words[(i * 7919) % 4096] for i in range(262144))  # ~2.3 MB, partly compressible
    started = time.perf_counter()
    for _ in range(_BENCH_WORK):
        zlib.compress(block, 6)
    seconds = time.perf_counter() - started
    return {"seconds": seconds, "speed": round(_BENCH_REFERENCE_S / seconds, 3)}


def time_command(args: dict) -> dict:
    """Wall time of a user benchmark run through the node's shell, in ``cwd``."""
    cwd = os.path.expanduser(args.get("cwd") or "~")
    started = time.perf_counter()
    result = subprocess.run(list(args["shell"]) + [args["command"]], cwd=cwd, capture_output=True, text=True)
    return {"seconds": time.perf_counter() - started, "exit_code": result.returncode, "stderr": result.stderr[-2000:]}


_AFFINITY_PROGRAM = """
import os
if hasattr(os, "sched_getaffinity"):
    print(sorted(os.sched_getaffinity(0)))
else:
    import ctypes
    from ctypes import wintypes
    k = ctypes.WinDLL("kernel32")
    k.GetCurrentProcess.restype = wintypes.HANDLE
    k.GetProcessAffinityMask.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p]
    mask, system = ctypes.c_size_t(), ctypes.c_size_t()
    k.GetProcessAffinityMask(k.GetCurrentProcess(), ctypes.byref(mask), ctypes.byref(system))
    print([cpu for cpu in range(64) if mask.value >> cpu & 1])
"""
_COUNTER_PROGRAM = """
import time
for i in range(600):
    with open("counter.txt", "a") as f:
        f.write("x")
    time.sleep(0.05)
"""
_LEFTOVER_PROGRAM = """
import subprocess, sys
child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])
open("leftover.pid", "w").write(str(child.pid))
"""


def selftest(args: dict) -> List[dict]:
    """Section 14: check on this node that fila's enforcement works on this
    system, with this node's Python; the node's own shell is checked too."""
    root = os.path.join(_jobs_dir(args), ".selftest")
    shutil.rmtree(root, ignore_errors=True)
    results = [dict(check="shell", **check_shell(args))]

    def attempt(name: str, program: str, **limits) -> tuple:
        directory = os.path.join(root, name)
        cwd = os.path.join(directory, "work")
        spec = dict(command=program, shell=[sys.executable, "-c"], cwd=cwd, cpus=[], mem=None, time_limit=None,
                    env={}, grace_s=1)
        spec.update(limits)
        supervisor.write_spec(directory, spec)
        return directory, cwd

    def run(name: str, program: str, **limits) -> tuple:
        directory, cwd = attempt(name, program, **limits)
        return supervisor.supervise(directory, poll_s=0.05), directory, cwd

    def record(check: str, ok: Optional[bool], message: str = "") -> None:
        results.append({"check": check, "ok": ok, "message": message})

    status, _, _ = run("start", "print('ok')")
    record("start", status["exit_code"] == 0 and status["reason"] is None, f"{status['reason']} {status['exit_code']}")
    status, _, _ = run("exit_code", "import sys; sys.exit(3)")
    record("exit_code", status["exit_code"] == 3 and status["reason"] == "exit_code")
    started = time.time()
    status, _, _ = run("time_limit", "import time; time.sleep(60)", time_limit=1.0)
    record("time_limit", status["reason"] == "timeout" and time.time() - started < 15, str(status["reason"]))
    status, _, _ = run("memory_limit", "x = bytearray(512 * 1024 * 1024); print(len(x))", mem=128 * 1024 * 1024)
    if sys.platform == "win32":
        how = "hard limit (Job Object)"
    else:
        how = "hard limit (cgroup)" if supervisor.cgroup_memory_limit() else "measured and killed"
    record("memory_limit", status["reason"] == "out_of_memory", f"{status['reason']}: {how}")
    cpus = [0, 1] if (os.cpu_count() or 1) > 1 else [0]
    status, directory, _ = run("cpu_affinity", _AFFINITY_PROGRAM, cpus=cpus)
    with open(os.path.join(directory, "stdout.log")) as handle:
        seen = handle.read().strip()
    if sys.platform == "darwin":
        record("cpu_affinity", None, "macOS has no CPU affinity: cores are not enforced")
    else:
        record("cpu_affinity", seen == str(cpus), f"expected {cpus}, saw {seen}")
    record("suspend_resume", *_check_suspend(attempt))
    status, _, cwd = run("leftovers", _LEFTOVER_PROGRAM)
    with open(os.path.join(cwd, "leftover.pid")) as handle:
        leftover = int(handle.read())
    deadline = time.time() + 10
    while supervisor.pid_alive(leftover) and time.time() < deadline:
        time.sleep(0.1)
    record("leftovers", not supervisor.pid_alive(leftover), "processes left behind are killed at the end")
    shutil.rmtree(root, ignore_errors=True)
    return results


def _check_suspend(attempt) -> tuple:
    import threading

    directory, cwd = attempt("suspend_resume", _COUNTER_PROGRAM)
    thread = threading.Thread(target=supervisor.supervise, args=(directory, 0.05))
    thread.start()

    def count() -> int:
        try:
            with open(os.path.join(cwd, "counter.txt")) as handle:
                return len(handle.read())
        except OSError:
            return 0

    def wait(condition, timeout=10.0) -> bool:
        deadline = time.time() + timeout
        while not condition():
            if time.time() > deadline:
                return False
            time.sleep(0.05)
        return True

    try:
        if not wait(lambda: count() > 3):
            return False, "the program did not start"
        supervisor.request(directory, "suspend")
        if not wait(lambda: (supervisor.read_status(directory) or {}).get("suspended")):
            return False, "suspend was not applied"
        time.sleep(0.3)
        frozen = count()
        time.sleep(0.6)
        if count() != frozen:
            return False, "the program kept running while suspended"
        supervisor.request(directory, "resume")
        if not wait(lambda: count() > frozen + 2):
            return False, "the program did not continue after resume"
        return True, ""
    finally:
        supervisor.request(directory, "cancel")
        thread.join(15)


OPERATIONS: Dict[str, Callable[[dict], object]] = {
    "slurm": slurm.agent_operation,
    "selftest": selftest,
    "version": version,
    "probe": probe,
    "launch": launch,
    "statuses": statuses,
    "cycle": cycle,
    "request": request,
    "read_log": read_log,
    "clean": clean,
    "check_shell": check_shell,
    "benchmark": benchmark,
    "time_command": time_command,
}


def _spawn_detached(args: List[str], directory: str) -> None:
    """Start a process that outlives its parent, the channel and the SSH session."""
    package_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # also a .pyz path
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [package_root, env.get("PYTHONPATH")]))
    log = open(os.path.join(directory, "supervisor.log"), "ab")
    options = dict(stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env, close_fds=True)
    try:
        if sys.platform == "win32":
            detached, new_group, breakaway = 0x00000008, 0x00000200, 0x01000000
            try:
                subprocess.Popen(args, creationflags=detached | new_group | breakaway, **options)
            except OSError:  # the parent's job forbids breakaway
                subprocess.Popen(args, creationflags=detached | new_group, **options)
        else:
            subprocess.Popen(args, start_new_session=True, **options)
    finally:
        log.close()
