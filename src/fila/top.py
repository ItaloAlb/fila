# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""``fila top`` (section 11): one view of the nodes and the jobs, like ``htop``.

``collect`` reads a snapshot from the coordinator's database (what
``resources``, ``status`` and ``explain`` show) into plain data; ``render``
turns a snapshot into lines of text. ``render`` is pure, so ``--once``, the
interactive view and the tests share it. Everything on screen is English.
"""

from __future__ import annotations

import datetime
import socket
from typing import Dict, List, Optional, Sequence, TextIO

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


# --- Drawing ---


def render(snapshot: dict, width: int, *, order: str = "id", text_filter: Optional[str] = None,
           selected: Optional[int] = None, interval: Optional[float] = None, interactive: bool = False,
           charset: Optional[dict] = None) -> List[str]:
    """The screen as lines of at most ``width`` characters (40 or more)."""
    chars = charset or UNICODE
    width = max(width, 40)
    rule = chars["rule"] * width
    lines = [_header(snapshot, width, interval, interactive, chars), rule]
    lines += _node_lines(snapshot["nodes"], width, chars) or ["no nodes registered"]
    lines.append(rule)
    jobs = _ordered(_filtered(snapshot["jobs"], text_filter), order)
    lines += _job_lines(jobs, width, selected, chars) or ["no jobs"]
    if interactive:
        lines += [rule, "[Enter] details  [l] log  [e] explain  [c] cancel  [s/r] suspend/resume",
                  "[d] drain node  [f] filter  [a] show finished  [o] order"]
    return [_fit(line, width, chars) for line in lines]


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


def _cell(text: str, size: int) -> str:
    """``text`` in ``size`` columns, always leaving one blank before the next."""
    return (text if len(text) < size else text[: size - 1]).ljust(size)


def _fit(line: str, width: int, chars: dict) -> str:
    if len(line) <= width:
        return line
    return line[: width - len(chars["more"])] + chars["more"]
