# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""``fila top`` (section 11): one view of the nodes and the jobs, like ``htop``.

``collect`` reads a snapshot from the coordinator's database (what
``resources``, ``status`` and ``explain`` show) into plain data;
``collect_local`` reads the smaller snapshot a node has on its own (its probe,
its attempts' journals and the worker's channel state) for ``--local``.
``render`` turns either snapshot into lines of text. It is pure, so
``--once``, the interactive view and the tests share it. Everything on screen
is English.
"""

from __future__ import annotations

import datetime
import os
import socket
from typing import Dict, List, Optional, Sequence, TextIO

from . import supervisor
from .channel import WORKER_STATE
from .files import read_json
from .scheduler import OCCUPYING, free_now, total
from .server import server_alive
from .store import Store

GiB = 1024**3

UNICODE = {"rule": "─", "fila": "█", "held": "▒", "free": "·", "none": "—",
           "more": "…", "sep": "·"}
ASCII = {"rule": "-", "fila": "#", "held": "+", "free": ".", "none": "-", "more": "~", "sep": "|"}

ORDERS = ("id", "state", "node", "elapsed")
WAITING_STATES = ("pending", "waiting_dependency", "blocked")


def charset_for(stream: TextIO) -> dict:
    """The drawing characters ``stream`` can encode (a Windows pipe may only take cp1252)."""
    encoding = getattr(stream, "encoding", None)
    if encoding is None:
        return UNICODE
    try:
        "".join(UNICODE.values()).encode(encoding)
    except (UnicodeEncodeError, LookupError):
        return ASCII
    return UNICODE


# --- Snapshot ---


def collect(store: Store, home: str, now: float, interval: float, show_finished: bool = False,
            node: Optional[str] = None, coordinator: Optional[str] = None) -> dict:
    """Nodes, jobs and why each waiting job waits, as plain data (``fila top --json``)."""
    jobs = store.snapshot(now).jobs
    nodes = []
    for view in store.nodes():
        if node is not None and view.name != node:
            continue
        here = [job for job in jobs if job.node == view.name and job.state in OCCUPYING]
        row = {
            "name": view.name, "kind": view.kind, "state": view.state, "speed": view.speed,
            "in_use": view.in_use, "capacity": dict(view.capacity),
            "free": {name: max(0, amount) for name, amount in free_now(view, jobs).items()},
            "fila": dict(total(job.request for job in here)),
        }
        if view.kind == "slurm":
            row["slurm_pending"] = sum(1 for job in here if job.state == "slurm_queued")
            row["max_pending"] = view.max_pending
        nodes.append(row)
    rows = []
    for job in store.jobs(include_final=show_finished):
        attempt = job.attempts[-1] if job.attempts else None
        if node is not None and (attempt is None or attempt["node"] != node):
            continue
        reason = store.reason(job.id) if job.state in WAITING_STATES else None
        rows.append({
            "id": job.id, "name": job.name, "command": job.command, "state": job.state,
            "suspended": bool(attempt and attempt["suspended"]) and job.state == "running",
            "node": attempt["node"] if attempt else None,
            "cores": job.request.get("cores", 0), "mem": job.request.get("mem", 0),
            "elapsed": _elapsed(job.state, attempt, now),
            "waiting": None if reason is None else _waiting(reason.summary, reason.details),
        })
    return {
        "coordinator": coordinator or socket.gethostname(),
        "now": now,
        "server_running": server_alive(home, now, interval),
        "nodes": nodes,
        "jobs": rows,
    }


def _elapsed(state: str, attempt: Optional[dict], now: float) -> Optional[float]:
    """Running time of the current attempt, without the time it spent suspended."""
    if attempt is None or attempt["started_at"] is None or state == "slurm_queued":
        return None
    end = attempt["finished_at"] if attempt["finished_at"] is not None else now
    return max(0.0, end - attempt["started_at"] - float(attempt["suspended_seconds"] or 0.0))


def _waiting(summary: str, details: Sequence[str]) -> str:
    """The ``explain`` text of a waiting job, in a few words."""
    state, _, rest = summary.partition(": ")
    if state == "waiting_dependency":
        return "after " + rest.replace("waiting for ", "", 1)
    if state == "blocked":
        return rest
    relevant = [line for line in details if not line.endswith(": excluded by --on")]
    if not relevant:
        return rest
    text = "waiting: " + relevant[0] + (f" (+{len(relevant) - 1} more)" if len(relevant) > 1 else "")
    reserved = rest.partition("; ")[2]  # the starvation reservation, if any
    return text + (f"; {reserved}" if reserved else "")


# --- The node's own snapshot (--local) ---

RECENT_S = 3600.0  # finished attempts shown without --all
LIVE_STATES = ("starting", "running", "suspended")


def collect_local(worker_dir: str, jobs_dirs: Sequence[str], now: float, probe=None, show_finished: bool = False,
                  idle_minutes: float = 5.0) -> dict:
    """What this node runs, from its own files only: no coordinator database
    here. ``jobs_dirs`` adds to the ones the worker recorded in worker.json."""
    from .probe import LocalProbe

    worker = read_json(os.path.join(worker_dir, WORKER_STATE))
    dirs = list(dict.fromkeys(list(jobs_dirs) + list((worker or {}).get("jobs_dirs", []))))
    attempts = sorted((attempt for jobs_dir in dirs for attempt in _local_attempts(jobs_dir, now, show_finished)),
                      key=lambda attempt: (attempt["job"], attempt["attempt"]))
    source = probe or LocalProbe()
    hardware, reading = source.hardware(), source.sample()
    live = [attempt for attempt in attempts if attempt["state"] in LIVE_STATES]
    idle = reading.idle_seconds
    return {
        "view": "local",
        "node": (worker or {}).get("node") or socket.gethostname(),
        "now": now,
        "channel": _channel(worker),
        "resources": {
            "cores": hardware.logical_cpus, "cores_used": hardware.logical_cpus - reading.free_cores,
            "cores_fila": sum(len(attempt["cpus"]) for attempt in live),
            "mem": hardware.total_mem, "mem_used": hardware.total_mem - reading.available_mem,
            "mem_fila": sum(attempt["mem_limit"] or 0 for attempt in live),
            "in_use": None if idle is None or idle == float("inf") else idle < idle_minutes * 60,
        },
        "jobs_dirs": dirs,
        "attempts": attempts,
    }


def _channel(worker: Optional[dict]) -> Optional[dict]:
    """The worker's channel as worker.json says, or ``not running`` if its process is gone."""
    if worker is None:
        return None
    state = worker.get("state")
    if state != "stopped" and not supervisor.pid_alive(int(worker.get("pid") or 0)):
        state = "not running"
    return dict({key: worker.get(key) for key in ("coordinator", "heard_at", "retry_at", "reason")}, state=state)


def _local_attempts(jobs_dir: str, now: float, show_finished: bool) -> List[dict]:
    """Every ``<jobs_dir>/<job>/attempt-<n>/`` journal: live ones always,
    finished or lost ones of the last hour (all of them with ``show_finished``)."""
    root = os.path.expanduser(jobs_dir)
    try:
        jobs = [name for name in os.listdir(root) if name.isdigit()]
    except OSError:
        return []
    found = []
    for job in jobs:
        try:
            attempts = [name for name in os.listdir(os.path.join(root, job))
                        if name.startswith("attempt-") and name[len("attempt-"):].isdigit()]
        except OSError:
            continue
        for name in attempts:
            entry = _local_attempt(os.path.join(root, job, name), int(job), int(name[len("attempt-"):]), now)
            recent = entry["ended_at"] is not None and now - entry["ended_at"] <= RECENT_S
            if entry["state"] in LIVE_STATES or show_finished or recent:
                found.append(dict(entry, jobs_dir=jobs_dir))
    return found


def _local_attempt(directory: str, job: int, attempt: int, now: float) -> dict:
    spec = read_json(os.path.join(directory, supervisor.SPEC)) or {}
    status = supervisor.read_status(directory)
    if status is None:  # job.json written, the supervisor not started yet
        state, status = "starting", {}
    else:
        state = status["state"]
        if state != "finished" and not supervisor.pid_alive(int(status.get("supervisor_pid") or 0)):
            status = supervisor.read_status(directory) or status  # it may have finished just now
            state = status["state"] if status["state"] == "finished" else "lost"
        elif state == "running" and status.get("suspended"):
            state = "suspended"
    ended_at = status.get("finished_at")
    if state == "lost":  # no end time: the last write of its journal stands for it
        try:
            ended_at = os.path.getmtime(os.path.join(directory, supervisor.STATUS))
        except OSError:
            ended_at = None
    return {
        "job": job, "attempt": attempt, "state": state, "command": spec.get("command", ""),
        "cpus": list(spec.get("cpus") or []), "mem_limit": spec.get("mem"),
        "elapsed": _local_elapsed(status, state, now), "exit_code": status.get("exit_code"),
        "reason": status.get("reason"), "ended_at": ended_at,
    }


def _local_elapsed(status: dict, state: str, now: float) -> Optional[float]:
    """Running time on this machine's clock, without the time spent suspended."""
    started = status.get("started_at")
    if started is None:
        return None
    end = status.get("finished_at") or now
    frozen = float(status.get("suspended_seconds") or 0.0)
    if state == "suspended" and status.get("suspended_since"):
        frozen += max(0.0, now - status["suspended_since"])
    return max(0.0, end - started - frozen)


# --- Drawing ---


MAIN_KEYS = ("[Enter] details  [l] log  [e] explain  [Tab] nodes/jobs",
             "[f] filter  [a] show finished  [o] order")
LOCAL_KEYS = ("[Enter] details  [l] log   actions (cancel, suspend, drain) are done on the coordinator",)


def render(snapshot: dict, width: int, *, order: str = "id", text_filter: Optional[str] = None,
           selected=None, interval: Optional[float] = None, interactive: bool = False,
           charset: Optional[dict] = None) -> List[str]:
    """The whole screen as lines of at most ``width`` characters (40 or more).
    ``selected`` is a job id, or ``(job, attempt)`` in the local view."""
    width = max(width, 40)
    chars = charset or UNICODE
    top, rows, bottom, _ = _parts(snapshot, width, order, text_filter, selected, None, interval, interactive, chars)
    return [_fit(line, width, chars) for line in top + rows + bottom]


def screen(snapshot: dict, width: int, height: int, *, order: str = "id", text_filter: Optional[str] = None,
           selected=None, selected_node: Optional[str] = None, first_row: int = 0,
           interval: Optional[float] = None, status: Optional[str] = None,
           charset: Optional[dict] = None) -> tuple:
    """The interactive screen in ``height`` lines: the job (or attempt) rows
    start at ``first_row`` and fill what the header and the keys leave.
    Returns (lines, index of the line to highlight or None, rows that fit).
    ``status`` (a prompt, a message) replaces the last line of keys."""
    width, height = max(width, 40), max(height, 5)
    chars = charset or UNICODE
    top, rows, bottom, marked = _parts(snapshot, width, order, text_filter, selected, selected_node, interval,
                                       True, chars)
    if status is not None:
        bottom = bottom[:-1] + [status]
    while bottom and len(top) + len(bottom) >= height:  # the keys go first when the terminal is short
        bottom = bottom[1:]
    fit_rows = max(1, height - len(top) - len(bottom))
    shown = rows[first_row:first_row + fit_rows]
    lines = (top + shown + [""] * (fit_rows - len(shown)) + bottom)[:height]
    highlight = None
    if marked is not None:
        kind, index = marked
        if kind == "top":
            highlight = index
        elif first_row <= index < first_row + fit_rows:
            highlight = len(top) + index - first_row
    return [_fit(line, width, chars) for line in lines], highlight, fit_rows


def _parts(snapshot: dict, width: int, order: str, text_filter: Optional[str], selected, selected_node,
           interval: Optional[float], interactive: bool, chars: dict) -> tuple:
    """(fixed top lines, scrolling rows, key lines, ("top"|"row", index) of the selection)."""
    rule = chars["rule"] * width
    if snapshot.get("view") == "local":
        top = _local_top(snapshot, width, interval, interactive, chars)
        attempts = _filtered_attempts(snapshot["attempts"], text_filter)
        table = _attempt_lines(attempts, width, chars, selected)
        if not table:
            table = ["", "no attempts here" if snapshot["jobs_dirs"] else "no jobs directory known: pass --jobs-dir"]
        keys = [rule, *LOCAL_KEYS] if interactive else []
        rows = table[1:]
        marked = next((("row", i) for i, a in enumerate(attempts) if (a["job"], a["attempt"]) == selected), None)
        return top + [line for line in table[:1] if line], rows, keys, marked
    top = [_header(snapshot, width, interval, interactive, chars), rule]
    node_lines = _node_lines(snapshot["nodes"], width, chars)
    marked = None
    if selected_node is not None:
        names = [node["name"] for node in snapshot["nodes"]]
        if selected_node in names:
            marked = ("top", len(top) + 1 + names.index(selected_node))
    top += (node_lines or ["no nodes registered"]) + [rule]
    jobs = _ordered(_filtered(snapshot["jobs"], text_filter), order)
    table = _job_lines(jobs, width, selected, chars)
    if marked is None and selected is not None:
        marked = next((("row", i) for i, job in enumerate(jobs) if job["id"] == selected), None)
    keys = [rule, *MAIN_KEYS] if interactive else []
    if not table:
        return top, ["no jobs"], keys, marked
    return top + table[:1], table[1:], keys, marked


def ordered_jobs(snapshot: dict, order: str = "id", text_filter: Optional[str] = None) -> List[dict]:
    """The rows of the list in screen order: jobs, or attempts in the local view."""
    if snapshot.get("view") == "local":
        return _filtered_attempts(snapshot["attempts"], text_filter)
    return _ordered(_filtered(snapshot["jobs"], text_filter), order)


def _filtered_attempts(attempts: List[dict], text: Optional[str]) -> List[dict]:
    if not text:
        return list(attempts)
    needle = text.lower()
    return [a for a in attempts if needle in a["state"].lower() or needle in (a["command"] or "").lower()]


def _header(snapshot: dict, width: int, interval: Optional[float], interactive: bool, chars: dict) -> str:
    parts = ["fila top", f"coordinator {snapshot['coordinator']}",
             datetime.datetime.fromtimestamp(snapshot["now"]).strftime("%H:%M:%S")]
    if interval is not None:
        parts.append(f"refresh {interval:g} s")
    if not snapshot["server_running"]:
        parts.append("server not running")
    left = f" {chars['sep']} ".join(parts)
    if not interactive:
        return left
    right = "q quit  ? help"
    return left + " " * max(width - len(left) - len(right), 1) + right


# (key, title, width) of the node columns, in screen order.
_NODE_COLUMNS = (("name", "NODE", 14), ("state", "STATE", 13), ("cores", "CORES", 22),
                 ("mem", "MEMORY", 25), ("speed", "SPEED", 7), ("in_use", "IN USE", 7))


def _node_layout(width: int) -> tuple:
    """The node columns that fit, their widths and the size of the bars: SPEED
    goes first, then IN USE, then the bars shrink."""
    keys = [key for key, _, _ in _NODE_COLUMNS]
    widths = {key: size for key, _, size in _NODE_COLUMNS}
    if width >= 88:
        return keys, widths, 12, 10
    if width >= 81:
        return [k for k in keys if k != "speed"], widths, 12, 10
    if width >= 74:
        return [k for k in keys if k not in ("speed", "in_use")], widths, 12, 10
    return keys[:4], dict(widths, cores=16, mem=20), 6, 6


def _node_lines(nodes: List[dict], width: int, chars: dict) -> List[str]:
    if not nodes:
        return []
    keys, widths, core_bar, mem_bar = _node_layout(width)
    titles = {key: title for key, title, _ in _NODE_COLUMNS}
    lines = ["".join(_cell(titles[key], widths[key]) for key in keys).rstrip()]
    for node in nodes:
        cells = _node_cells(node, core_bar, mem_bar, chars)
        lines.append("".join(_cell(cells[key], widths[key]) for key in keys).rstrip())
    return lines


def _node_cells(node: dict, core_bar: int, mem_bar: int, chars: dict) -> Dict[str, str]:
    none = chars["none"]
    cells = {"name": node["name"], "state": node["state"], "cores": none, "mem": none, "speed": none, "in_use": none}
    if node["state"] in ("unreachable", "outdated"):  # the last numbers are stale
        return cells
    cells["speed"] = str(round(node["speed"], 2))
    if node["kind"] == "slurm":  # fila's jobs pending in Slurm against max_pending_slurm (section 7.5)
        pending = f"{node['slurm_pending']}/{node['max_pending']} in Slurm"
        cells["cores"] = pending if core_bar < 12 else "pending " + pending
        return cells
    capacity, free, fila = node["capacity"], node["free"], node["fila"]
    cores = capacity.get("cores", 0)
    used = cores - free.get("cores", 0)
    cells["cores"] = f"{_bar(cores, fila.get('cores', 0), used, core_bar, chars)}  {used}/{cores}"
    mem = capacity.get("mem", 0)
    used_mem = mem - free.get("mem", 0)
    cells["mem"] = f"{_bar(mem, fila.get('mem', 0), used_mem, mem_bar, chars)}  {_mem_pair(used_mem, mem)}"
    if node["in_use"] is not None:
        cells["in_use"] = "yes" if node["in_use"] else "no"
    return cells


def _bar(capacity: int, fila: int, used: int, size: int, chars: dict) -> str:
    """``[`` fila's jobs, then other use and the reserve, then free ``]``, in ``size`` marks."""
    if capacity <= 0:
        return "[" + chars["free"] * size + "]"
    used = min(max(used, 0), capacity)
    fila = min(max(fila, 0), used)
    marks_used = round(used / capacity * size)
    marks_fila = min(round(fila / capacity * size), marks_used)
    return ("[" + chars["fila"] * marks_fila + chars["held"] * (marks_used - marks_fila)
            + chars["free"] * (size - marks_used) + "]")


def _mem_pair(used: int, total_mem: int) -> str:
    if total_mem >= GiB:
        return f"{used / GiB:.1f}/{total_mem / GiB:.1f}G"
    return f"{used // 1024**2}/{total_mem // 1024**2}M"


# (key, title, width) of the job columns after ID and STATE, which size to their content.
_JOB_COLUMNS = (("node", "NODE", 13), ("cores", "CORES", 7), ("mem", "MEM", 7), ("elapsed", "ELAPSED", 10))
_MIN_COMMAND = 12  # below this, MEM and ELAPSED make room for the command


def _job_lines(jobs: List[dict], width: int, selected: Optional[int], chars: dict) -> List[str]:
    if not jobs:
        return []
    none = chars["none"]
    rows = []
    for job in jobs:
        what = job["name"] or job["command"]
        if job["waiting"]:
            what += f"   ({job['waiting']})"
        rows.append((job["id"], {
            "id": str(job["id"]), "state": "suspended" if job["suspended"] else job["state"],
            "node": job["node"] or none, "cores": str(job["cores"]), "mem": _short_mem(job["mem"]),
            "elapsed": none if job["elapsed"] is None else _clock(job["elapsed"]), "what": what,
        }))
    id_width = max(2, max(len(cells["id"]) for _, cells in rows))
    state_width = max(11, max(len(cells["state"]) for _, cells in rows) + 2)
    columns = list(_JOB_COLUMNS)
    if 2 + id_width + 2 + state_width + sum(size for _, _, size in columns) + _MIN_COMMAND > width:
        columns = [column for column in columns if column[0] not in ("mem", "elapsed")]

    def line(marker: str, cells: Dict[str, str]) -> str:
        return (marker + cells["id"].rjust(id_width) + "  " + _cell(cells["state"], state_width)
                + "".join(_cell(cells[key], size) for key, _, size in columns) + cells["what"])

    titles = dict({key: title for key, title, _ in _JOB_COLUMNS}, id="ID", state="STATE", what="NAME / COMMAND")
    return [line("  ", titles)] + [line("> " if job_id == selected else "  ", cells) for job_id, cells in rows]


def _short_mem(amount: int) -> str:
    for unit, size in (("T", 1024**4), ("G", GiB), ("M", 1024**2), ("K", 1024)):
        if amount >= size:
            value = amount / size
            return f"{value:.0f}{unit}" if amount % size == 0 or value >= 10 else f"{value:.1f}{unit}"
    return str(amount)


def _clock(seconds: float) -> str:
    hours, rest = divmod(int(seconds), 3600)
    return f"{hours:02d}:{rest // 60:02d}:{rest % 60:02d}"


def _filtered(jobs: List[dict], text: Optional[str]) -> List[dict]:
    """Jobs whose node, state, name or command contains ``text``, in any case."""
    if not text:
        return list(jobs)
    needle = text.lower()
    return [job for job in jobs
            if any(needle in str(job[key] or "").lower() for key in ("node", "state", "name", "command"))]


_STATE_ORDER = ("running", "unreachable", "slurm_queued", "pending", "waiting_dependency", "blocked",
                "failed", "cancelled", "completed")


def _ordered(jobs: List[dict], order: str) -> List[dict]:
    if order not in ORDERS:
        raise ValueError(f"unknown order {order!r}; one of {', '.join(ORDERS)}")
    if order == "state":
        rank = {state: index for index, state in enumerate(_STATE_ORDER)}
        return sorted(jobs, key=lambda job: (rank.get(job["state"], len(rank)), job["id"]))
    if order == "node":
        return sorted(jobs, key=lambda job: (job["node"] is None, job["node"] or "", job["id"]))
    if order == "elapsed":
        return sorted(jobs, key=lambda job: (job["elapsed"] is None, -(job["elapsed"] or 0.0), job["id"]))
    return sorted(jobs, key=lambda job: job["id"])


# --- Drawing the node's own view ---


def _local_top(snapshot: dict, width: int, interval: Optional[float], interactive: bool, chars: dict) -> List[str]:
    """Header, channel and resources: the fixed part above the attempts."""
    rule = chars["rule"] * width
    parts = ["fila top --local", f"node {snapshot['node']}",
             datetime.datetime.fromtimestamp(snapshot["now"]).strftime("%H:%M:%S")]
    if interval is not None:
        parts.append(f"refresh {interval:g} s")
    header = f" {chars['sep']} ".join(parts)
    if interactive:
        right = "q quit  ? help"
        header += " " * max(width - len(header) - len(right), 1) + right
    lines = [header]
    if snapshot["channel"] is not None:
        lines.append(_channel_line(snapshot["channel"], snapshot["now"], chars))
    return lines + [rule, _resources_line(snapshot["resources"], width, chars), rule]


def _channel_line(channel: dict, now: float, chars: dict) -> str:
    sep, state, coordinator = f" {chars['sep']} ", channel["state"], channel["coordinator"]
    if state == "connected":
        heard = channel["heard_at"]
        return f"channel: connected to {coordinator}" + ("" if heard is None else f"{sep}heard {_ago(now - heard)} ago")
    if state == "connecting":
        return f"channel: connecting to {coordinator}"
    if state == "waiting":
        text = f"channel: cannot reach {coordinator}"
        if channel["retry_at"] is not None:
            text += f"{sep}retry in {_ago(channel['retry_at'] - now)}"
        return text + (f"{sep}{channel['reason']}" if channel["reason"] else "")
    if state == "stopped":
        return f"channel: worker stopped (coordinator {coordinator})"
    return f"channel: worker not running (coordinator {coordinator})"


def _ago(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 120:
        return f"{seconds} s"
    if seconds < 7200:
        return f"{seconds // 60} min"
    return f"{seconds // 3600} h"


def _resources_line(resources: dict, width: int, chars: dict) -> str:
    """CORES and MEMORY bars (fila's attempts, other use, free) and IN USE, as wide as fits."""
    in_use = chars["none"] if resources["in_use"] is None else ("yes" if resources["in_use"] else "no")
    cores, mem = resources["cores"], resources["mem"]
    line = ""
    for core_bar, mem_bar in ((20, 14), (12, 10), (8, 6), (4, 4)):
        cores_bar = _bar(cores, resources["cores_fila"], resources["cores_used"], core_bar, chars)
        mem_text = _bar(mem, resources["mem_fila"], resources["mem_used"], mem_bar, chars)
        line = (f"CORES  {cores_bar}  {resources['cores_used']}/{cores} used    "
                f"MEMORY  {mem_text}  {_mem_pair(resources['mem_used'], mem)}   IN USE  {in_use}")
        if len(line) <= width:
            break
    return line


_ATTEMPT_COLUMNS = (("attempt", "ATTEMPT", 9), ("cpus", "CPUS", 12), ("mem", "MEM LIMIT", 11),
                    ("elapsed", "ELAPSED", 10))


def _attempt_lines(attempts: List[dict], width: int, chars: dict, selected=None) -> List[str]:
    """The attempts table; ``selected`` is a ``(job, attempt)`` pair."""
    if not attempts:
        return []
    none = chars["none"]
    rows = []
    for attempt in attempts:
        live = attempt["state"] in LIVE_STATES
        what = attempt["command"]
        if not live:
            what += f"   ({_outcome(attempt)})"
        rows.append({
            "marker": "> " if (attempt["job"], attempt["attempt"]) == selected else "  ",
            "job": str(attempt["job"]), "attempt": str(attempt["attempt"]), "state": attempt["state"],
            "cpus": _cpu_list(attempt["cpus"]) if live and attempt["cpus"] else none,
            "mem": _short_mem(attempt["mem_limit"]) if live and attempt["mem_limit"] else none,
            "elapsed": none if attempt["elapsed"] is None else _clock(attempt["elapsed"]), "what": what,
        })
    job_width = max(3, max(len(row["job"]) for row in rows))
    state_width = max(11, max(len(row["state"]) for row in rows) + 2)
    columns = list(_ATTEMPT_COLUMNS)
    if 2 + job_width + 2 + state_width + sum(size for _, _, size in columns) + _MIN_COMMAND > width:
        columns = [column for column in columns if column[0] not in ("mem", "elapsed")]

    def line(cells: Dict[str, str]) -> str:
        text = cells["marker"] + cells["job"].rjust(job_width) + "  " + _cell(cells["attempt"], columns[0][2])
        text += _cell(cells["state"], state_width)
        return text + "".join(_cell(cells[key], size) for key, _, size in columns[1:]) + cells["what"]

    titles = dict({key: title for key, title, _ in _ATTEMPT_COLUMNS}, job="JOB", state="STATE", what="COMMAND",
                  marker="  ")
    return [line(titles)] + [line(row) for row in rows]


def _outcome(attempt: dict) -> str:
    if attempt["state"] == "lost":
        return "lost"
    reason, code = attempt["reason"], attempt["exit_code"]
    if reason in (None, "exit_code") and code is not None:
        return f"exit {code}"
    return reason or "ended"


def _cpu_list(cpus: Sequence[int]) -> str:
    """``0-3,8,9``: runs of three or more CPUs as ranges."""
    parts, ordered, start = [], sorted(cpus), 0
    while start < len(ordered):
        end = start
        while end + 1 < len(ordered) and ordered[end + 1] == ordered[end] + 1:
            end += 1
        if end - start >= 2:
            parts.append(f"{ordered[start]}-{ordered[end]}")
        else:
            parts.extend(str(cpu) for cpu in ordered[start:end + 1])
        start = end + 1
    return ",".join(parts)


def _cell(text: str, size: int) -> str:
    """``text`` in ``size`` columns, always leaving one blank before the next."""
    return (text if len(text) < size else text[: size - 1]).ljust(size)


def _fit(line: str, width: int, chars: dict) -> str:
    if len(line) <= width:
        return line
    return line[: width - len(chars["more"])] + chars["more"]
