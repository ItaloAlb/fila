"""Section 5/6: what a computer has and what is free on it right now.

Only the standard library: ``/proc`` on Linux, ``sysctl``/``vm_stat`` on macOS
and ``ctypes`` on Windows.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class Hardware:
    logical_cpus: int
    topology: Tuple[Tuple[int, ...], ...]  # logical CPU ids grouped by physical core
    total_mem: int


@dataclass(frozen=True)
class Reading:
    free_cores: int  # logical CPUs minus the current load
    available_mem: int
    idle_seconds: float  # since the last keyboard or mouse input; inf if unknown


class LocalProbe:
    """Readings of the machine this process runs on. Keep one instance: the
    CPU load on Windows comes from the difference between two samples."""

    def __init__(self) -> None:
        self._previous_times: Optional[Tuple[int, int]] = None
        self._hardware: Optional[Hardware] = None

    def hardware(self) -> Hardware:
        if self._hardware is None:
            topology = _topology()
            self._hardware = Hardware(
                logical_cpus=sum(len(core) for core in topology), topology=topology, total_mem=_total_mem()
            )
        return self._hardware

    def sample(self) -> Reading:
        cpus = self.hardware().logical_cpus
        load = self._load(cpus)
        return Reading(
            free_cores=max(0, min(cpus, int(cpus - load))),
            available_mem=_available_mem(),
            idle_seconds=_idle_seconds(),
        )

    def disk_free(self, path: str) -> int:
        return shutil.disk_usage(path).free

    def _load(self, cpus: int) -> float:
        """Busy logical CPUs: the 1-minute load average on POSIX; the busy
        fraction since the previous sample times the CPU count on Windows."""
        if sys.platform != "win32":
            return os.getloadavg()[0]
        if self._previous_times is None:  # a fresh process (an agent call): take a first sample now
            self._previous_times = _windows_cpu_times()
            time.sleep(0.25)
        idle, total = _windows_cpu_times()
        previous, self._previous_times = self._previous_times, (idle, total)
        if total == previous[1]:
            return 0.0
        busy = 1.0 - (idle - previous[0]) / (total - previous[1])
        return max(0.0, busy) * cpus


# --- Topology ---


def _usable_cpus() -> List[int]:
    if hasattr(os, "sched_getaffinity"):
        return sorted(os.sched_getaffinity(0))
    if sys.platform == "win32":
        return _windows_process_cpus()
    return list(range(os.cpu_count() or 1))


def _topology() -> Tuple[Tuple[int, ...], ...]:
    usable = set(_usable_cpus())
    if sys.platform == "win32":
        cores = _windows_cores()
    elif sys.platform.startswith("linux"):
        cores = _linux_cores(usable)
    else:
        cores = [[cpu] for cpu in sorted(usable)]  # macOS exposes no per-CPU core map
    grouped = [tuple(cpu for cpu in core if cpu in usable) for core in cores]
    return tuple(core for core in grouped if core)


def _linux_cores(usable: set) -> List[List[int]]:
    groups: dict = {}
    for cpu in sorted(usable):
        base = f"/sys/devices/system/cpu/cpu{cpu}/topology"
        try:
            with open(f"{base}/physical_package_id") as package, open(f"{base}/core_id") as core:
                key = (package.read().strip(), core.read().strip())
        except OSError:
            key = ("cpu", str(cpu))
        groups.setdefault(key, []).append(cpu)
    return sorted(groups.values())


# --- Memory and idle time ---


def _total_mem() -> int:
    if sys.platform == "win32":
        return _windows_memory()[0]
    if sys.platform.startswith("linux"):
        return _meminfo("MemTotal")
    return int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))


def _available_mem() -> int:
    if sys.platform == "win32":
        return _windows_memory()[1]
    if sys.platform.startswith("linux"):
        return _meminfo("MemAvailable")
    return _macos_available_mem()


def _meminfo(key: str) -> int:
    with open("/proc/meminfo") as handle:
        for line in handle:
            if line.startswith(key + ":"):
                return int(line.split()[1]) * 1024
    raise OSError(f"{key} missing from /proc/meminfo")


def _macos_available_mem() -> int:
    output = subprocess.check_output(["vm_stat"], text=True)
    page = int(re.search(r"page size of (\d+)", output).group(1))
    pages = sum(
        int(re.search(rf"{name}:\s+(\d+)", output).group(1))
        for name in ("Pages free", "Pages inactive", "Pages speculative")
    )
    return pages * page


def _idle_seconds() -> float:
    try:
        if sys.platform == "win32":
            return _windows_idle_seconds()
        if sys.platform.startswith("linux"):
            return _linux_idle_seconds()
        return _macos_idle_seconds()
    except (OSError, ValueError, subprocess.SubprocessError):
        return float("inf")  # unknown: never counts as "in use"


def _linux_idle_seconds() -> float:
    """From the session manager: ``IdleHint`` and when it was set."""
    output = subprocess.check_output(
        ["loginctl", "show-user", str(os.getuid()), "--property=IdleHint", "--property=IdleSinceHint"],
        text=True,
        stderr=subprocess.DEVNULL,
        timeout=5,
    )
    values = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
    if values.get("IdleHint") != "yes":
        return 0.0
    since = int(values.get("IdleSinceHint", "0")) / 1e6
    return max(0.0, time.time() - since) if since else float("inf")


def _macos_idle_seconds() -> float:
    output = subprocess.check_output(["ioreg", "-c", "IOHIDSystem"], text=True, timeout=5)
    return int(re.search(r'"HIDIdleTime" = (\d+)', output).group(1)) / 1e9


# --- Windows ---

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _user32 = ctypes.WinDLL("user32", use_last_error=True)

    class _ProcessorInfo(ctypes.Structure):
        _fields_ = [("mask", ctypes.c_size_t), ("relationship", wintypes.DWORD), ("reserved", ctypes.c_ulonglong * 2)]

    class _MemoryStatus(ctypes.Structure):
        _fields_ = [
            ("length", wintypes.DWORD),
            ("load", wintypes.DWORD),
            ("total_phys", ctypes.c_ulonglong),
            ("avail_phys", ctypes.c_ulonglong),
            ("total_page", ctypes.c_ulonglong),
            ("avail_page", ctypes.c_ulonglong),
            ("total_virtual", ctypes.c_ulonglong),
            ("avail_virtual", ctypes.c_ulonglong),
            ("avail_extended", ctypes.c_ulonglong),
        ]

    class _LastInput(ctypes.Structure):
        _fields_ = [("size", wintypes.UINT), ("time", wintypes.DWORD)]

    _kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    _kernel32.GetProcessAffinityMask.argtypes = [
        wintypes.HANDLE, ctypes.POINTER(ctypes.c_size_t), ctypes.POINTER(ctypes.c_size_t)
    ]

    def _check(ok) -> None:
        if not ok:
            raise ctypes.WinError(ctypes.get_last_error())

    def _windows_process_cpus() -> List[int]:
        process_mask, system_mask = ctypes.c_size_t(), ctypes.c_size_t()
        _check(_kernel32.GetProcessAffinityMask(_kernel32.GetCurrentProcess(), process_mask, system_mask))
        return [cpu for cpu in range(64) if process_mask.value >> cpu & 1]

    def _windows_cores() -> List[List[int]]:
        size = wintypes.DWORD()
        _kernel32.GetLogicalProcessorInformation(None, ctypes.byref(size))
        entries = (_ProcessorInfo * (size.value // ctypes.sizeof(_ProcessorInfo)))()
        _check(_kernel32.GetLogicalProcessorInformation(entries, ctypes.byref(size)))
        relation_processor_core = 0
        return [
            [cpu for cpu in range(64) if entry.mask >> cpu & 1]
            for entry in entries
            if entry.relationship == relation_processor_core
        ]

    def _windows_memory() -> Tuple[int, int]:
        status = _MemoryStatus(length=ctypes.sizeof(_MemoryStatus))
        _check(_kernel32.GlobalMemoryStatusEx(ctypes.byref(status)))
        return status.total_phys, status.avail_phys

    def _windows_cpu_times() -> Tuple[int, int]:
        """(idle, total) in 100 ns units; the kernel time includes the idle time."""
        idle, kernel, user = wintypes.FILETIME(), wintypes.FILETIME(), wintypes.FILETIME()
        _check(_kernel32.GetSystemTimes(ctypes.byref(idle), ctypes.byref(kernel), ctypes.byref(user)))

        def value(filetime) -> int:
            return filetime.dwHighDateTime << 32 | filetime.dwLowDateTime

        return value(idle), value(kernel) + value(user)

    def _windows_idle_seconds() -> float:
        last = _LastInput(size=ctypes.sizeof(_LastInput))
        _check(_user32.GetLastInputInfo(ctypes.byref(last)))
        return ((_kernel32.GetTickCount() - last.time) & 0xFFFFFFFF) / 1000.0
