"""Section 4.2/6.3: one supervisor process per attempt.

The supervisor starts the command through the node's shell, pins it to its
CPUs, enforces the memory and time limits, obeys cancel/suspend/resume
requests and keeps the diary (``status.json``). It runs detached from the
channel and the agent, so losing either never kills the job.

Files in the attempt directory:

- ``job.json``: what to run (written before the supervisor starts);
- ``status.json``: the diary, replaced atomically on every change;
- ``stdout.log`` / ``stderr.log``: the command's output;
- ``cancel`` / ``suspend``: requests; ``suspend`` present means "keep frozen".
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from typing import List, Optional

from .files import read_json as _read_json, write_json as _write_json

STATUS = "status.json"
SPEC = "job.json"


def write_spec(attempt_dir: str, spec: dict) -> None:
    """``spec``: command, shell (list), cwd, cpus, mem (bytes or None),
    time_limit (seconds or None), env (added to the environment), grace_s."""
    os.makedirs(attempt_dir, exist_ok=True)
    _write_json(os.path.join(attempt_dir, SPEC), spec)


def read_status(attempt_dir: str) -> Optional[dict]:
    return _read_json(os.path.join(attempt_dir, STATUS))


def request(attempt_dir: str, what: str) -> None:
    """``cancel``, ``suspend`` or ``resume`` the attempt; the supervisor acts within one poll."""
    if what == "resume":
        try:
            os.remove(os.path.join(attempt_dir, "suspend"))
        except FileNotFoundError:
            pass
        return
    if what not in ("cancel", "suspend"):
        raise ValueError(what)
    open(os.path.join(attempt_dir, what), "w").close()


def supervise(attempt_dir: str, poll_s: float = 0.5) -> dict:
    """Run the attempt to its end and return the final status."""
    spec = _read_json(os.path.join(attempt_dir, SPEC))
    status = {
        "state": "starting", "supervisor_pid": os.getpid(), "pid": None, "started_at": None,
        "finished_at": None, "exit_code": None, "reason": None, "suspended": False, "suspended_seconds": 0.0,
    }
    _write_status(attempt_dir, status)
    try:
        os.makedirs(spec["cwd"], exist_ok=True)
        group = _ProcessGroup(spec.get("cpus") or [], spec.get("mem") or None)
    except OSError as error:  # bad directory or limits: the attempt ends here, with the reason
        with open(os.path.join(attempt_dir, "stderr.log"), "a") as err:
            err.write(f"fila: cannot prepare the job: {error}\n")
        status.update(state="finished", finished_at=time.time(), reason="start_failed")
        _write_status(attempt_dir, status)
        _notify(spec)
        return status
    try:
        with open(os.path.join(attempt_dir, "stdout.log"), "wb") as out, open(
            os.path.join(attempt_dir, "stderr.log"), "wb"
        ) as err:
            process = group.start(
                list(spec["shell"]) + [spec["command"]],
                cwd=spec["cwd"],
                env=dict(os.environ, **spec.get("env", {})),
                stdout=out,
                stderr=err,
            )
        status.update(state="running", pid=process.pid, started_at=time.time())
        _write_status(attempt_dir, status)
        reason = _watch(attempt_dir, spec, status, group, process, poll_s)
        group.kill()  # whatever the command left behind
        if reason is None and group.memory_exceeded():
            reason = "out_of_memory"
        exit_code = process.returncode
        if reason is None and exit_code != 0:
            reason = "exit_code"
    except OSError as error:  # the shell could not start
        exit_code, reason = None, "exit_code"
        with open(os.path.join(attempt_dir, "stderr.log"), "a") as err:
            err.write(f"fila: cannot start {spec['shell']}: {error}\n")
    finally:
        group.close()
    status.update(state="finished", finished_at=time.time(), exit_code=exit_code, reason=reason, suspended=False)
    _write_status(attempt_dir, status)
    _notify(spec)
    return status


def _notify(spec: dict) -> None:
    """Touch the file the server watches, so it reconciles now instead of at its next cycle."""
    if spec.get("notify"):
        try:
            with open(spec["notify"], "w") as handle:
                handle.write(str(time.time()))
        except OSError:
            pass


def _watch(attempt_dir: str, spec: dict, status: dict, group, process, poll_s: float) -> Optional[str]:
    """Poll until the command ends; return why fila ended it, or None."""
    time_limit = spec.get("time_limit")
    grace = spec.get("grace_s", 30)
    suspended_since = None
    while process.poll() is None:
        if os.path.exists(os.path.join(attempt_dir, "cancel")):
            group.terminate(grace, process)
            return "cancelled"
        if group.memory_exceeded():
            group.kill()
            process.wait()
            return "out_of_memory"
        want_suspended = os.path.exists(os.path.join(attempt_dir, "suspend"))
        if want_suspended and suspended_since is None:
            group.suspend()
            suspended_since = time.time()
            status["suspended"] = True
            _write_status(attempt_dir, status)
        elif not want_suspended and suspended_since is not None:
            group.resume()
            status["suspended_seconds"] += time.time() - suspended_since
            suspended_since = None
            status["suspended"] = False
            _write_status(attempt_dir, status)
        if time_limit is not None and suspended_since is None:
            if time.time() - status["started_at"] - status["suspended_seconds"] > time_limit:
                group.terminate(grace, process)
                return "timeout"
        time.sleep(poll_s)
    return None


def pid_alive(pid: int) -> bool:
    if sys.platform == "win32":
        return _windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# --- Files ---


def _write_status(attempt_dir: str, status: dict) -> None:
    _write_json(os.path.join(attempt_dir, STATUS), status)


# --- POSIX: a session (process group) per attempt ---


class _PosixGroup:
    """Affinity via ``sched_setaffinity`` (inherited by children). Memory has
    a hard limit in a cgroup v2 scope where systemd delegates one to the user
    (Linux); elsewhere it is measured over the process group and enforced by
    killing it."""

    def __init__(self, cpus: List[int], mem: Optional[int]) -> None:
        self.cpus = cpus
        self.mem = mem
        self.pgid: Optional[int] = None
        self._exceeded = False
        self.scope: Optional[str] = None
        self._events: Optional[str] = None
        if mem is not None and cgroup_memory_limit():
            self.scope = f"fila-{os.getpid()}-{int(time.time() * 1000)}.scope"

    def start(self, args: List[str], **kwargs) -> subprocess.Popen:
        cpus = self.cpus
        if self.scope is not None:  # systemd-run execs the command in place: same pid, session and affinity
            args = [
                "systemd-run", "--user", "--scope", "--quiet", f"--unit={self.scope}",
                "-p", f"MemoryMax={self.mem}", "-p", "MemorySwapMax=0", "-p", "OOMPolicy=stop", "--",
            ] + list(args)

        def pin() -> None:
            if cpus and hasattr(os, "sched_setaffinity"):
                os.sched_setaffinity(0, cpus)

        process = subprocess.Popen(args, stdin=subprocess.DEVNULL, start_new_session=True, preexec_fn=pin, **kwargs)
        self.pgid = process.pid
        return process

    def memory_exceeded(self) -> bool:
        if self.mem is None or self._exceeded or self.pgid is None:
            return self._exceeded
        if self.scope is not None:  # the kernel already killed it; only notice
            self._exceeded = self._oom_killed()
        else:
            self._exceeded = _group_rss(self.pgid) > self.mem
        return self._exceeded

    def _signal(self, sig: int) -> None:
        if self.pgid is None:
            return
        try:
            os.killpg(self.pgid, sig)
        except (ProcessLookupError, PermissionError):
            pass

    def terminate(self, grace: float, process: subprocess.Popen) -> None:
        self._signal(signal.SIGTERM)
        self._signal(signal.SIGCONT)  # a stopped process only sees SIGTERM once continued
        try:
            process.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            self.kill()
            process.wait()

    def kill(self) -> None:
        self._signal(signal.SIGKILL)

    def suspend(self) -> None:
        self._signal(signal.SIGSTOP)

    def resume(self) -> None:
        self._signal(signal.SIGCONT)

    def _oom_killed(self) -> bool:
        """The scope's own events while it lives; systemd's verdict once it is gone."""
        if self._events is None:
            try:
                with open(f"/proc/{self.pgid}/cgroup") as handle:
                    path = next(line[3:].strip() for line in handle if line.startswith("0::"))
                if path.endswith("/" + self.scope):  # not before systemd-run has moved itself in
                    self._events = f"/sys/fs/cgroup{path}/memory.events"
            except (OSError, StopIteration):
                pass
        if self._events is not None:
            try:
                with open(self._events) as handle:
                    events = dict(line.split() for line in handle if line.strip())
                return int(events.get("oom_kill", 0)) > 0
            except OSError:
                pass
        answer = subprocess.run(
            ["systemctl", "--user", "show", "-p", "Result", "--value", self.scope], capture_output=True, text=True
        )
        return answer.stdout.strip() == "oom-kill"

    def close(self) -> None:
        if self.scope is not None:  # a scope ended by the OOM killer stays loaded as failed
            subprocess.run(["systemctl", "--user", "reset-failed", self.scope], capture_output=True)


_CGROUP_PROBE = """
path = [line for line in open('/proc/self/cgroup').read().splitlines() if line.startswith('0::')][0][3:]
print(open('/sys/fs/cgroup' + path + '/memory.max').read().strip())
"""

_cgroup_memory: Optional[bool] = None


def cgroup_memory_limit() -> bool:
    """Whether ``systemd-run --user --scope -p MemoryMax=...`` really limits
    memory here: Linux, cgroup v2, a user manager and a delegated memory controller."""
    global _cgroup_memory
    if _cgroup_memory is None:
        _cgroup_memory = False
        if sys.platform.startswith("linux") and shutil.which("systemd-run") and shutil.which("systemctl"):
            limit = 64 * 1024 * 1024
            try:
                answer = subprocess.run(
                    ["systemd-run", "--user", "--scope", "--quiet", "-p", f"MemoryMax={limit}", "--",
                     sys.executable, "-c", _CGROUP_PROBE],
                    capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
                )
                _cgroup_memory = answer.returncode == 0 and answer.stdout.strip() == str(limit)
            except (OSError, subprocess.SubprocessError):
                pass
    return _cgroup_memory


def _group_rss(pgid: int) -> int:
    """Resident memory of every process in the group."""
    if sys.platform.startswith("linux"):
        page = os.sysconf("SC_PAGE_SIZE")
        total = 0
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                with open(f"/proc/{entry}/stat") as handle:
                    fields = handle.read().rsplit(")", 1)[1].split()
            except OSError:
                continue
            if int(fields[2]) == pgid:  # fields after the command: state, ppid, pgrp, ...
                total += int(fields[21]) * page
        return total
    output = subprocess.run(["ps", "-o", "rss=", "-g", str(pgid)], capture_output=True, text=True).stdout
    return sum(int(line) for line in output.split()) * 1024


# --- Windows: a Job Object per attempt ---

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _ntdll = ctypes.WinDLL("ntdll")

    _CREATE_SUSPENDED = 0x00000004
    _CREATE_NEW_PROCESS_GROUP = 0x00000200
    _CREATE_NO_WINDOW = 0x08000000
    _LIMIT_AFFINITY = 0x00000010
    _LIMIT_JOB_MEMORY = 0x00000200
    _LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
    _EXTENDED_LIMIT_INFORMATION = 9
    _ASSOCIATE_COMPLETION_PORT = 7
    _BASIC_PROCESS_ID_LIST = 3
    _MSG_JOB_MEMORY_LIMIT = 10
    _PROCESS_SUSPEND_RESUME = 0x0800
    _PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
    _STILL_ACTIVE = 259
    _INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_uint64) for name in ("r", "w", "o", "rb", "wb", "ob")]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    class _CompletionPort(ctypes.Structure):
        _fields_ = [("CompletionKey", ctypes.c_void_p), ("CompletionPort", wintypes.HANDLE)]

    _kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _kernel32.SetInformationJobObject.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    _kernel32.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p
    ]
    _kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _kernel32.CreateIoCompletionPort.restype = wintypes.HANDLE
    _kernel32.CreateIoCompletionPort.argtypes = [wintypes.HANDLE, wintypes.HANDLE, ctypes.c_size_t, wintypes.DWORD]
    _kernel32.GetQueuedCompletionStatus.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD), ctypes.POINTER(ctypes.c_size_t),
        ctypes.POINTER(ctypes.c_void_p), wintypes.DWORD,
    ]
    _kernel32.OpenProcess.restype = wintypes.HANDLE
    _kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    _ntdll.NtSuspendProcess.argtypes = [wintypes.HANDLE]
    _ntdll.NtResumeProcess.argtypes = [wintypes.HANDLE]

    def _check(ok):
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())
        return ok

    class _WindowsJob:
        """Affinity and a hard memory limit on the whole job; the completion
        port reports when a process tried to pass the memory limit."""

        def __init__(self, cpus: List[int], mem: Optional[int]) -> None:
            self.job = _check(_kernel32.CreateJobObjectW(None, None))
            limits = _ExtendedLimits()
            flags = _LIMIT_KILL_ON_JOB_CLOSE
            if cpus:
                flags |= _LIMIT_AFFINITY
                limits.BasicLimitInformation.Affinity = sum(1 << cpu for cpu in cpus)
            if mem is not None:
                flags |= _LIMIT_JOB_MEMORY
                limits.JobMemoryLimit = mem
            limits.BasicLimitInformation.LimitFlags = flags
            _check(_kernel32.SetInformationJobObject(
                self.job, _EXTENDED_LIMIT_INFORMATION, ctypes.byref(limits), ctypes.sizeof(limits)
            ))
            self.port = _check(_kernel32.CreateIoCompletionPort(_INVALID_HANDLE_VALUE, None, 0, 1))
            association = _CompletionPort(CompletionKey=None, CompletionPort=self.port)
            _check(_kernel32.SetInformationJobObject(
                self.job, _ASSOCIATE_COMPLETION_PORT, ctypes.byref(association), ctypes.sizeof(association)
            ))
            self._exceeded = False

        def start(self, args: List[str], **kwargs) -> subprocess.Popen:
            # Created suspended so it joins the job before running a single instruction.
            process = subprocess.Popen(
                args,
                stdin=subprocess.DEVNULL,
                creationflags=_CREATE_SUSPENDED | _CREATE_NEW_PROCESS_GROUP | _CREATE_NO_WINDOW,
                **kwargs,
            )
            handle = int(process._handle)
            try:
                _check(_kernel32.AssignProcessToJobObject(self.job, handle))
            except OSError:
                process.kill()
                raise
            _ntdll.NtResumeProcess(handle)
            return process

        def memory_exceeded(self) -> bool:
            message, key, overlapped = wintypes.DWORD(), ctypes.c_size_t(), ctypes.c_void_p()
            while _kernel32.GetQueuedCompletionStatus(
                self.port, ctypes.byref(message), ctypes.byref(key), ctypes.byref(overlapped), 0
            ):
                if message.value == _MSG_JOB_MEMORY_LIMIT:
                    self._exceeded = True
            return self._exceeded

        def terminate(self, grace: float, process: subprocess.Popen) -> None:
            # Windows has no SIGTERM for ordinary programs: the job ends at once.
            self.kill()
            process.wait()

        def kill(self) -> None:
            _kernel32.TerminateJobObject(self.job, 1)

        def _pids(self) -> List[int]:
            capacity = 1024

            class _ProcessIds(ctypes.Structure):
                _fields_ = [
                    ("assigned", wintypes.DWORD),
                    ("listed", wintypes.DWORD),
                    ("ids", ctypes.c_size_t * capacity),
                ]

            ids = _ProcessIds()
            _check(_kernel32.QueryInformationJobObject(
                self.job, _BASIC_PROCESS_ID_LIST, ctypes.byref(ids), ctypes.sizeof(ids), None
            ))
            return [ids.ids[index] for index in range(ids.listed)]

        def _each_process(self, operation) -> None:
            for pid in self._pids():
                handle = _kernel32.OpenProcess(_PROCESS_SUSPEND_RESUME, False, pid)
                if handle:
                    operation(handle)
                    _kernel32.CloseHandle(handle)

        def suspend(self) -> None:
            self._each_process(_ntdll.NtSuspendProcess)

        def resume(self) -> None:
            self._each_process(_ntdll.NtResumeProcess)

        def close(self) -> None:
            _kernel32.CloseHandle(self.port)
            _kernel32.CloseHandle(self.job)  # kill-on-close: nothing of the job survives

    def _windows_pid_alive(pid: int) -> bool:
        handle = _kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            code = wintypes.DWORD()
            return bool(_kernel32.GetExitCodeProcess(handle, ctypes.byref(code))) and code.value == _STILL_ACTIVE
        finally:
            _kernel32.CloseHandle(handle)

    _ProcessGroup = _WindowsJob
else:
    _ProcessGroup = _PosixGroup
