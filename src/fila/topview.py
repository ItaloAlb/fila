# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The interactive ``fila top`` (section 11) without a terminal: what each key
does and what the screen shows.

``View`` keeps the selection, the focus (jobs or nodes), the order, the
filter and the page on top of the list (details, log, explain, help). It gets
its data from two callables: ``fetch(show_finished)`` returns a snapshot
(``top.collect`` or ``top.collect_local``) and ``ask(argv)`` runs the command
line in this process and returns ``(exit code, stdout, stderr)``, so the pages
show exactly what ``fila result``, ``fila log --json`` and ``fila explain``
print. ``terminal.run`` feeds it keys and draws its screens; tests feed it
keys directly.
"""

from __future__ import annotations

import json
import os
from typing import Callable, List, Optional, Tuple

from . import top

ORDERS = top.ORDERS
LOG_TAIL_BYTES = 64 * 1024

HELP = [
    "fila top: the nodes and the jobs of this coordinator, refreshed every few seconds.",
    "",
    "Moving",
    "  Up/Down, j/k       select the previous or next row",
    "  PgUp/PgDn          one screen up or down",
    "  Home/End, g/G      the first or the last row",
    "  Tab                switch between the jobs and the nodes",
    "",
    "Looking",
    "  Enter              details of the job (fila result) or of the node",
    "  l                  the end of the job's output, followed (Tab: stdout/stderr)",
    "  e                  why the job is in its state (fila explain)",
    "",
    "Choosing what to see",
    "  f                  filter by node, state, name or command",
    "  a                  show or hide finished jobs",
    "  o                  order by id, state, node or elapsed time",
    "",
    "  ?                  this help",
    "  q, Esc             quit (on a page: back to the list)",
]

LOCAL_HELP = [
    "fila top --local: what fila runs on this node, from the node's own files.",
    "",
    "  Up/Down, j/k       select the previous or next attempt",
    "  PgUp/PgDn, Home/End",
    "  Enter              the attempt's job.json and status.json",
    "  l                  the end of the attempt's output, followed (Tab: stdout/stderr)",
    "  f                  filter by state or command",
    "  a                  show every finished attempt, or only the last hour's",
    "  ?                  this help",
    "  q, Esc             quit (on a page: back to the list)",
    "",
    "Cancelling, suspending and draining are done on the coordinator: a change",
    "made here would be undone by the server at its next cycle.",
]


class Page:
    """Text over the list: a title, lines from ``load()`` (called again on every
    refresh) and a scroll position. A followed page stays at its end."""

    def __init__(self, title: str, load: Callable[[], List[str]], follow: bool = False) -> None:
        self.title, self.load, self.follow = title, load, follow
        self.lines: List[str] = []
        self.first = 0
        self.at_end = follow
        self.reload()

    def reload(self) -> None:
        try:
            self.lines = self.load() or ["(empty)"]
        except Exception as error:  # a page must never take the view down
            self.lines = [f"cannot read it: {error}"]


class View:
    def __init__(self, fetch: Callable[[bool], dict], ask: Optional[Callable[[List[str]], tuple]] = None,
                 local: bool = False, interval: Optional[float] = 2.0, charset: Optional[dict] = None) -> None:
        self.fetch, self.ask, self.local = fetch, ask, local
        self.interval, self.charset = interval, charset or top.UNICODE
        self.snapshot: Optional[dict] = None
        self.show_finished = False
        self.order = "id"
        self.text_filter: Optional[str] = None
        self.focus = "jobs"
        self.selected = None  # a job id, or (job, attempt) in the local view
        self.selected_node: Optional[str] = None
        self.first_row = 0
        self.mode = "main"  # main, page, filter
        self.page: Optional[Page] = None
        self.typed = ""
        self.message: Optional[str] = None
        self.done = False
        self.wants_refresh = False
        self.stream = "stdout"  # the log page's stream
        self.rows_fit = 10  # rows the last screen could show, for PgUp/PgDn

    # --- Data ---

    def refresh(self) -> None:
        """A new snapshot, keeping the selection on the same job when it is still there."""
        self.wants_refresh = False
        try:
            self.snapshot = self.fetch(self.show_finished)
        except Exception as error:  # keep showing the last snapshot
            self.message = f"refresh failed: {error}"
            if self.snapshot is None:
                raise
        self._keep_selection()
        if self.page is not None:
            self._reload_page()

    def rows(self) -> List[dict]:
        return top.ordered_jobs(self.snapshot, self.order, self.text_filter) if self.snapshot else []

    def _key(self, row: dict):
        return (row["job"], row["attempt"]) if self.local else row["id"]

    def _keep_selection(self, index: Optional[int] = None) -> None:
        keys = [self._key(row) for row in self.rows()]
        if not keys:
            self.selected = None
        elif index is not None:
            self.selected = keys[max(0, min(index, len(keys) - 1))]
        elif self.selected not in keys:
            self.selected = keys[0]
        nodes = [] if self.local or not self.snapshot else [node["name"] for node in self.snapshot["nodes"]]
        if self.selected_node not in nodes:
            self.selected_node = nodes[0] if nodes else None
        if self.focus == "nodes" and not nodes:
            self.focus = "jobs"

    # --- Screen ---

    def screen(self, width: int, height: int) -> Tuple[List[str], Optional[int]]:
        """The lines to draw and the one to highlight."""
        if self.mode == "page" and self.page is not None:
            return self._page_screen(width, height), None
        status = self.message
        if self.mode == "filter":
            status = f"filter: {self.typed}_   [Enter] apply  [Esc] cancel"
        lines, highlight, fit = self._list_screen(width, height, status)
        index = self._selected_index()
        if index is not None and self.focus == "jobs":
            first = self.first_row
            if index < first:
                first = index
            elif index >= first + fit:
                first = index - fit + 1
            if first != self.first_row:
                self.first_row = first
                lines, highlight, fit = self._list_screen(width, height, status)
        self.rows_fit = fit
        return lines, highlight

    def _list_screen(self, width: int, height: int, status: Optional[str]) -> tuple:
        return top.screen(
            self.snapshot, width, height, order=self.order, text_filter=self.text_filter,
            selected=self.selected, selected_node=self.selected_node if self.focus == "nodes" else None,
            first_row=self.first_row, interval=self.interval, status=status, charset=self.charset,
        )

    def _page_screen(self, width: int, height: int) -> List[str]:
        page, rule = self.page, self.charset["rule"] * max(width, 40)
        keys = "[Up/Down PgUp/PgDn Home/End] scroll  [Esc] back"
        if page.follow:
            keys += "  [Tab] stdout/stderr"
        fit = max(1, height - 4)
        last = max(0, len(page.lines) - fit)
        if page.at_end:
            page.first = last
        page.first = max(0, min(page.first, last))
        shown = page.lines[page.first:page.first + fit]
        lines = [page.title, rule] + shown + [""] * (fit - len(shown)) + [rule, self.message or keys]
        self.rows_fit = fit
        return [_fit(line, max(width, 40), self.charset) for line in lines[:max(height, 5)]]

    def _selected_index(self) -> Optional[int]:
        keys = [self._key(row) for row in self.rows()]
        return keys.index(self.selected) if self.selected in keys else None

    # --- Keys ---

    def handle(self, key: str) -> None:
        self.message = None
        if self.mode == "filter":
            self._filter_key(key)
        elif self.mode == "page":
            self._page_key(key)
        else:
            self._main_key(key)

    def _main_key(self, key: str) -> None:
        if key in ("q", "esc", "quit"):
            self.done = True
        elif key in ("up", "k", "down", "j", "pgup", "pgdn", "home", "g", "end", "G"):
            self._move(key)
        elif key == "tab":
            if self.local or not (self.snapshot and self.snapshot["nodes"]):
                self.message = "no nodes panel here" if self.local else "no nodes registered"
            else:
                self.focus = "nodes" if self.focus == "jobs" else "jobs"
        elif key == "enter":
            self._open_details()
        elif key == "l":
            self._open_log()
        elif key == "e":
            self._open_explain()
        elif key == "a":
            self.show_finished = not self.show_finished
            self.message = "showing finished jobs" if self.show_finished else "hiding finished jobs"
            self.wants_refresh = True
        elif key == "o":
            if self.local:
                self.message = "attempts are ordered by job and attempt"
            else:
                self.order = ORDERS[(ORDERS.index(self.order) + 1) % len(ORDERS)]
                self.message = f"order: {self.order}"
        elif key == "f":
            self.mode, self.typed = "filter", self.text_filter or ""
        elif key == "?":
            self._show(Page("fila top: keys", lambda: list(LOCAL_HELP if self.local else HELP)))
        elif key == "resize":
            pass
        elif len(key) == 1:
            self.message = f"no action for '{key}' (? shows the keys)"

    def _move(self, key: str) -> None:
        step = {"up": -1, "k": -1, "down": 1, "j": 1}.get(key, 0)
        page = max(1, self.rows_fit)
        if self.focus == "nodes":
            names = [node["name"] for node in self.snapshot["nodes"]]
            index = names.index(self.selected_node) if self.selected_node in names else 0
            index = _moved(index, len(names), key, step, page)
            self.selected_node = names[index] if names else None
            return
        rows = self.rows()
        if not rows:
            return
        index = self._selected_index() or 0
        self._keep_selection(_moved(index, len(rows), key, step, page))

    def _filter_key(self, key: str) -> None:
        if key == "enter":
            self.text_filter = self.typed or None
            self.mode = "main"
            self.first_row = 0
            self._keep_selection()
            self.message = f"filter: {self.text_filter}" if self.text_filter else "no filter"
        elif key in ("esc", "quit"):
            self.mode = "main"
        elif key == "backspace":
            self.typed = self.typed[:-1]
        elif len(key) == 1 and key.isprintable():
            self.typed += key

    def _page_key(self, key: str) -> None:
        page = self.page
        if key in ("q", "esc", "quit", "enter"):
            self.mode, self.page = "main", None
            return
        if key == "tab" and page.follow:
            self.stream = "stderr" if self.stream == "stdout" else "stdout"
            self._open_log()
            return
        fit = max(1, self.rows_fit)
        last = max(0, len(page.lines) - fit)
        moves = {"up": -1, "k": -1, "down": 1, "j": 1, "pgup": -fit, "pgdn": fit}
        if key in moves:
            page.first = max(0, min(page.first + moves[key], last))
        elif key in ("home", "g"):
            page.first = 0
        elif key in ("end", "G"):
            page.first = last
        page.at_end = page.follow and page.first >= last

    # --- Pages ---

    def _show(self, page: Page) -> None:
        self.page, self.mode = page, "page"

    def _reload_page(self) -> None:
        page = self.page
        page.reload()

    def _selected_row(self) -> Optional[dict]:
        rows = self.rows()
        index = self._selected_index()
        return None if index is None else rows[index]

    def _open_details(self) -> None:
        if self.focus == "nodes" and not self.local:
            node = next((n for n in self.snapshot["nodes"] if n["name"] == self.selected_node), None)
            if node is not None:
                self._show(Page(f"node {node['name']}", lambda name=node["name"]: self._node_details(name)))
            return
        row = self._selected_row()
        if row is None:
            self.message = "no job selected"
        elif self.local:
            self._show(Page(f"job {row['job']}, attempt {row['attempt']}", lambda: _attempt_files(row)))
        else:
            self._show(Page(f"job {row['id']}: fila result", lambda: self._ask_text(["result", str(row["id"])])))

    def _open_log(self) -> None:
        row = self._selected_row() if self.focus == "jobs" else None
        if row is None:
            self.message = "no job selected"
            return
        stream = self.stream
        if self.local:
            title = f"job {row['job']}, attempt {row['attempt']}: {stream}.log (following)"
            self._show(Page(title, lambda: _local_log(row, stream), follow=True))
        else:
            title = f"job {row['id']}: {stream} (following)"
            self._show(Page(title, lambda: self._remote_log(row["id"], stream), follow=True))

    def _open_explain(self) -> None:
        if self.local:
            self.message = "explain needs the coordinator: run fila explain there"
            return
        row = self._selected_row() if self.focus == "jobs" else None
        if row is None:
            self.message = "no job selected"
            return
        self._show(Page(f"job {row['id']}: fila explain", lambda: self._ask_text(["explain", str(row["id"])])))

    def _ask_text(self, argv: List[str]) -> List[str]:
        code, out, err = self.ask(argv)
        text = out if out.strip() else err
        return text.rstrip("\n").splitlines()

    def _remote_log(self, job_id: int, stream: str) -> List[str]:
        argv = ["log", "--json", str(job_id)] + (["--stderr"] if stream == "stderr" else [])
        code, out, err = self.ask(argv)
        if code != 0 or not out.strip():
            return (err or out).rstrip("\n").splitlines()
        return json.loads(out)["tail"].rstrip("\n").splitlines() or ["(no output yet)"]

    def _node_details(self, name: str) -> List[str]:
        node = next((n for n in self.snapshot["nodes"] if n["name"] == name), None)
        if node is None:
            return ["the node is no longer registered"]
        lines = [f"{key}: {value}" for key, value in node.items() if key not in ("capacity", "free", "fila")]
        for key in ("capacity", "free", "fila"):
            amounts = node.get(key) or {}
            lines.append(f"{key}: " + ", ".join(
                f"{name}={top._short_mem(amount) if name == 'mem' else amount}" for name, amount in amounts.items()))
        jobs = [job for job in self.snapshot["jobs"] if job["node"] == name]
        lines += ["", f"jobs here: {', '.join(str(job['id']) for job in jobs) or 'none'}"]
        return lines


def _moved(index: int, count: int, key: str, step: int, page: int) -> int:
    if key == "pgup":
        index -= page
    elif key == "pgdn":
        index += page
    elif key in ("home", "g"):
        index = 0
    elif key in ("end", "G"):
        index = count - 1
    else:
        index += step
    return max(0, min(index, count - 1))


def _attempt_files(row: dict) -> List[str]:
    from . import agent

    directory = agent.attempt_dir(row["jobs_dir"], row["job"], row["attempt"])
    lines = [f"directory: {directory}", ""]
    for name in ("job.json", "status.json"):
        path = os.path.join(directory, name)
        try:
            with open(path, encoding="utf-8") as handle:
                text = json.dumps(json.load(handle), indent=2)
        except (OSError, ValueError) as error:
            text = f"cannot read it: {error}"
        lines += [f"{name}:"] + text.splitlines() + [""]
    return lines


def _local_log(row: dict, stream: str) -> List[str]:
    from . import agent

    answer = agent.read_log({"jobs_dir": row["jobs_dir"], "job": row["job"], "attempt": row["attempt"],
                             "stream": stream, "tail": LOG_TAIL_BYTES})
    return answer["text"].rstrip("\n").splitlines() or ["(no output yet)"]


def _fit(line: str, width: int, chars: dict) -> str:
    return top._fit(line, width, chars)
