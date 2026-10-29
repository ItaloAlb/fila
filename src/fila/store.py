"""The coordinator's SQLite database: nodes, jobs, attempts and the last reasons.

Every command line call and the server open the same file. Each operation
opens its own connection and commits before returning, so concurrent callers
only ever see whole transitions.
"""

from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from typing import Iterable, Iterator, List, Optional, Tuple

from .scheduler import (
    OCCUPYING,
    Action,
    Block,
    Job,
    Node,
    Plan,
    Reason,
    Release,
    Resources,
    SlurmMachine,
    Snapshot,
    Start,
    SubmitToSlurm,
    Unblock,
    plan,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    command TEXT NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    request TEXT NOT NULL,
    spec TEXT NOT NULL,
    created_by TEXT NOT NULL,
    submitted_at REAL NOT NULL,
    pending_since REAL,
    finished_at REAL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    kind TEXT NOT NULL DEFAULT 'submit',
    hold INTEGER,
    unreachable_since REAL
);
CREATE TABLE IF NOT EXISTS attempts (
    job_id INTEGER NOT NULL REFERENCES jobs(id),
    number INTEGER NOT NULL,
    state TEXT NOT NULL,
    reason TEXT,
    node TEXT,
    cpus TEXT NOT NULL DEFAULT '[]',
    slurm_id TEXT,
    exit_code INTEGER,
    started_at REAL,
    finished_at REAL,
    PRIMARY KEY (job_id, number)
);
CREATE TABLE IF NOT EXISTS node_flags (
    name TEXT PRIMARY KEY,
    drained INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS channels (
    node TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    connected_at REAL NOT NULL,
    last_seen REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node TEXT NOT NULL,
    operation TEXT NOT NULL,
    args TEXT NOT NULL,
    created_at REAL NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0,
    answer TEXT
);
CREATE TABLE IF NOT EXISTS reasons (
    job_id INTEGER PRIMARY KEY REFERENCES jobs(id),
    summary TEXT NOT NULL,
    details TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

FINAL_STATES = ("completed", "failed", "cancelled", "not_run")
# Columns added after the first schema, added to older databases on open.
_LATER_COLUMNS = {"kind": "TEXT NOT NULL DEFAULT 'submit'", "hold": "INTEGER", "unreachable_since": "REAL"}
# What ``fila wait`` waits for: a blocked job will not move until someone retries its cause.
SETTLED_STATES = FINAL_STATES + ("blocked",)
RETRYABLE = ("failed", "cancelled", "unreachable")


class UnknownJob(LookupError):
    pass


class NotAllowed(Exception):
    """The job's state does not allow the requested change."""


@dataclass(frozen=True)
class JobRecord:
    """A job as stored. ``spec`` holds the submit options (``after``, ``on``,
    ``hint``, ``time_limit``, ``dir``, ...)."""

    id: int
    name: Optional[str]
    command: str
    state: str
    reason: Optional[str]
    request: Resources
    spec: dict
    created_by: str
    submitted_at: float
    pending_since: Optional[float]
    finished_at: Optional[float]
    cancel_requested: bool
    attempts: List[dict] = field(default_factory=list)
    kind: str = "submit"  # "submit" (queued) or "run" (immediate, one target node)
    hold: Optional[bool] = None  # manual suspend (True) or resume (False); None follows on_use


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            columns = {row["name"] for row in db.execute("PRAGMA table_info(jobs)")}
            for name, definition in _LATER_COLUMNS.items():
                if name not in columns:
                    db.execute(f"ALTER TABLE jobs ADD COLUMN {name} {definition}")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    # --- Nodes ---

    def put_node(self, node: Node) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT OR REPLACE INTO nodes (name, data) VALUES (?, ?)",
                (node.name, json.dumps(_node_to_dict(node))),
            )

    def nodes(self) -> List[Node]:
        with self._connect() as db:
            rows = db.execute("SELECT data FROM nodes ORDER BY name").fetchall()
        return [_node_from_dict(json.loads(row["data"])) for row in rows]

    def keep_nodes(self, names: Iterable[str]) -> None:
        """Forget the scheduler view of nodes no longer registered."""
        names = list(names)
        with self._connect() as db:
            placeholders = ", ".join("?" * len(names)) or "NULL"
            db.execute(f"DELETE FROM nodes WHERE name NOT IN ({placeholders})", names)

    # --- Pull channels (section 4.3): the channel process and the server meet here ---

    def channel_open(self, node: str, pid: int, now: float) -> None:
        """A node connected; a newer connection replaces an older one."""
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO channels (node, pid, connected_at, last_seen) VALUES (?, ?, ?, ?)",
                       (node, pid, now, now))

    def channel_seen(self, node: str, pid: int, now: float) -> bool:
        """The node sent something. False if another connection took over."""
        with self._connect() as db:
            return db.execute("UPDATE channels SET last_seen = ? WHERE node = ? AND pid = ?", (now, node, pid)).rowcount == 1

    def channel_close(self, node: str, pid: int, now: float) -> None:
        """Drop the connection and answer its unanswered requests as unreachable."""
        with self._connect() as db:
            if db.execute("DELETE FROM channels WHERE node = ? AND pid = ?", (node, pid)).rowcount:
                db.execute(
                    "UPDATE channel_requests SET answer = ? WHERE node = ? AND answer IS NULL",
                    (json.dumps({"ok": False, "kind": "unreachable", "error": f"{node}: the channel closed"}), node),
                )

    def channel(self, node: str) -> Optional[dict]:
        with self._connect() as db:
            row = db.execute("SELECT * FROM channels WHERE node = ?", (node,)).fetchone()
        return None if row is None else dict(row)

    def channel_request(self, node: str, operation: str, args: dict, now: float) -> int:
        with self._connect() as db:
            return int(db.execute(
                "INSERT INTO channel_requests (node, operation, args, created_at) VALUES (?, ?, ?, ?)",
                (node, operation, json.dumps(args), now),
            ).lastrowid)

    def channel_take(self, node: str) -> List[dict]:
        """Requests not yet sent to the node, marked as sent."""
        with self._connect() as db:
            rows = db.execute(
                "SELECT id, operation, args FROM channel_requests WHERE node = ? AND sent = 0 ORDER BY id", (node,)
            ).fetchall()
            if rows:
                db.execute(
                    f"UPDATE channel_requests SET sent = 1 WHERE id IN ({', '.join('?' * len(rows))})",
                    [row["id"] for row in rows],
                )
        return [{"id": row["id"], "operation": row["operation"], "args": json.loads(row["args"])} for row in rows]

    def channel_answer(self, node: str, request_id: int, answer: dict) -> bool:
        """Only an answer to one of this node's own requests is accepted (section 13.5)."""
        with self._connect() as db:
            return db.execute(
                "UPDATE channel_requests SET answer = ? WHERE id = ? AND node = ? AND answer IS NULL",
                (json.dumps(answer), request_id, node),
            ).rowcount == 1

    def channel_result(self, request_id: int) -> Optional[dict]:
        """The answer, removing the request once read; None while there is none."""
        with self._connect() as db:
            row = db.execute("SELECT answer FROM channel_requests WHERE id = ?", (request_id,)).fetchone()
            if row is None or row["answer"] is None:
                return None
            db.execute("DELETE FROM channel_requests WHERE id = ?", (request_id,))
            return json.loads(row["answer"])

    def channel_forget(self, request_id: int) -> None:
        with self._connect() as db:
            db.execute("DELETE FROM channel_requests WHERE id = ?", (request_id,))

    def set_drained(self, name: str, drained: bool) -> None:
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO node_flags (name, drained) VALUES (?, ?)", (name, int(drained)))

    def drained(self) -> set:
        with self._connect() as db:
            return {row[0] for row in db.execute("SELECT name FROM node_flags WHERE drained = 1")}

    def active_attempts(self) -> List[Tuple[JobRecord, dict]]:
        """Jobs holding a node right now, with their current attempt."""
        placeholders = ", ".join("?" * len(OCCUPYING))
        with self._connect() as db:
            rows = db.execute(f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY id", OCCUPYING).fetchall()
            records = [_job_from_row(db, row) for row in rows]
        return [(record, record.attempts[-1]) for record in records if record.attempts]

    # --- Jobs ---

    def add_job(
        self,
        command: str,
        request: Resources,
        spec: dict,
        created_by: str,
        now: float,
        name: Optional[str] = None,
    ) -> int:
        state = "waiting_dependency" if spec.get("after") else "pending"
        with self._connect() as db:
            cursor = db.execute(
                "INSERT INTO jobs (name, command, state, request, spec, created_by, submitted_at, pending_since)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    command,
                    state,
                    json.dumps(dict(request)),
                    json.dumps(spec),
                    created_by,
                    now,
                    now if state == "pending" else None,
                ),
            )
            return int(cursor.lastrowid)

    # --- Scheduler contract ---

    def snapshot(self, now: float, starvation_after: float = 3600.0) -> Snapshot:
        """The scheduler's view: active jobs plus every dependency of the
        waiting ones, finished or not."""
        with self._connect() as db:
            rows = {row["id"]: row for row in db.execute("SELECT * FROM jobs")}
            active = {job_id for job_id, row in rows.items() if row["state"] not in FINAL_STATES}
            needed = set(active)
            for job_id in active:
                needed.update(json.loads(rows[job_id]["spec"]).get("after", []))
            jobs = tuple(_scheduler_job(_job_from_row(db, rows[job_id])) for job_id in sorted(needed) if job_id in rows)
        return Snapshot(now=now, nodes=tuple(self.nodes()), jobs=jobs, starvation_after=starvation_after)

    def apply(self, result: Plan, now: float) -> List[Action]:
        """Record the plan's decisions and reasons. A decision about a job that
        changed state since the snapshot (cancelled meanwhile, for example) is
        skipped; the actions actually recorded are returned, so the server only
        launches those."""
        applied: List[Action] = []
        with self._connect() as db:
            for action in result.actions:
                if _apply_action(db, action, now):
                    applied.append(action)
            db.execute("DELETE FROM reasons")
            db.executemany(
                "INSERT INTO reasons (job_id, summary, details, updated_at) VALUES (?, ?, ?, ?)",
                [(job_id, r.summary, json.dumps(list(r.details)), now) for job_id, r in result.reasons.items()],
            )
        return applied

    def reason(self, job_id: int) -> Optional[Reason]:
        with self._connect() as db:
            row = db.execute("SELECT summary, details FROM reasons WHERE job_id = ?", (job_id,)).fetchone()
        return None if row is None else Reason(row["summary"], tuple(json.loads(row["details"])))

    def finish(
        self, job_id: int, state: str, now: float, exit_code: Optional[int] = None, reason: Optional[str] = None
    ) -> None:
        """A job reached a final state; its current attempt, if any, ends with it."""
        with self._connect() as db:
            db.execute(
                "UPDATE jobs SET state = ?, reason = ?, finished_at = ? WHERE id = ?", (state, reason, now, job_id)
            )
            db.execute(
                "UPDATE attempts SET state = ?, reason = ?, exit_code = ?, finished_at = ?"
                " WHERE job_id = ? AND number = (SELECT MAX(number) FROM attempts WHERE job_id = ?)",
                (state, reason, exit_code, now, job_id, job_id),
            )

    # --- User requests ---

    def cancel(self, job_id: int, now: float, cascade: bool = False) -> dict:
        """Cancel a job, and with ``cascade`` every unfinished dependent.

        Waiting jobs end at once; running ones get a request the server acts on.
        """
        result: dict = {"cancelled": [], "requested": []}
        with self._connect() as db:
            rows = {row["id"]: row for row in db.execute("SELECT id, state, spec FROM jobs")}
            if job_id not in rows:
                raise UnknownJob(job_id)
            if rows[job_id]["state"] in FINAL_STATES:
                raise NotAllowed(f"job {job_id} is already {rows[job_id]['state']}")
            targets = [job_id] + (_dependents(rows, job_id) if cascade else [])
            for target in targets:
                state = rows[target]["state"]
                if state in FINAL_STATES:
                    continue
                if state in OCCUPYING:
                    db.execute("UPDATE jobs SET cancel_requested = 1 WHERE id = ?", (target,))
                    result["requested"].append(target)
                else:
                    db.execute(
                        "UPDATE jobs SET state = 'cancelled', reason = 'requested', finished_at = ? WHERE id = ?",
                        (now, target),
                    )
                    result["cancelled"].append(target)
        return result

    def retry(self, job_id: int, now: float, on: Optional[List[str]] = None) -> None:
        """Queue a failed, cancelled or unreachable job again. An unreachable
        attempt is abandoned; the next start creates a new attempt."""
        with self._connect() as db:
            rows = {row["id"]: row for row in db.execute("SELECT id, state, spec FROM jobs")}
            if job_id not in rows:
                raise UnknownJob(job_id)
            state = rows[job_id]["state"]
            if state == "blocked":
                cause = _root_cause(rows, job_id)
                raise NotAllowed(f"job {job_id} is blocked by job {cause}; retry {cause} instead")
            if state not in RETRYABLE:
                raise NotAllowed(f"job {job_id} is {state}; only failed, cancelled or unreachable jobs can be retried")
            if state == "unreachable":
                db.execute(
                    "UPDATE attempts SET state = 'abandoned', finished_at = ?"
                    " WHERE job_id = ? AND number = (SELECT MAX(number) FROM attempts WHERE job_id = ?)",
                    (now, job_id, job_id),
                )
            spec = json.loads(rows[job_id]["spec"])
            if on is not None:
                spec["on"] = list(on)
            new_state = "waiting_dependency" if spec.get("after") else "pending"
            db.execute(
                "UPDATE jobs SET state = ?, reason = NULL, finished_at = NULL, cancel_requested = 0,"
                " unreachable_since = NULL, pending_since = ?, spec = ? WHERE id = ?",
                (new_state, now if new_state == "pending" else None, json.dumps(spec), job_id),
            )

    def mark_unreachable(self, node_name: str, now: float) -> None:
        """The node lost contact: it and its running jobs become ``unreachable`` (section 9)."""
        with self._connect() as db:
            row = db.execute("SELECT data FROM nodes WHERE name = ?", (node_name,)).fetchone()
            if row is not None:
                data = dict(json.loads(row["data"]), state="unreachable")
                db.execute("UPDATE nodes SET data = ? WHERE name = ?", (json.dumps(data), node_name))
            current = (
                "SELECT job_id FROM attempts a WHERE node = ? AND state = 'running'"
                " AND number = (SELECT MAX(number) FROM attempts WHERE job_id = a.job_id)"
            )
            db.execute(
                f"UPDATE jobs SET state = 'unreachable', unreachable_since = ? WHERE id IN ({current})", (now, node_name)
            )
            db.execute("UPDATE attempts SET state = 'unreachable' WHERE node = ? AND state = 'running'", (node_name,))

    def set_slurm_id(self, job_id: int, slurm_id: str) -> None:
        with self._connect() as db:
            db.execute(
                "UPDATE attempts SET slurm_id = ? WHERE job_id = ? AND number = (SELECT MAX(number) FROM attempts WHERE job_id = ?)",
                (slurm_id, job_id, job_id),
            )

    def slurm_started(self, job_id: int, started_at: float) -> None:
        """Slurm moved the job from its queue to a compute node."""
        with self._connect() as db:
            db.execute(
                "UPDATE jobs SET state = 'running', unreachable_since = NULL WHERE id = ?"
                " AND state IN ('slurm_queued', 'unreachable')",
                (job_id,),
            )
            db.execute(
                "UPDATE attempts SET state = 'running', started_at = ? WHERE job_id = ?"
                " AND number = (SELECT MAX(number) FROM attempts WHERE job_id = ?)",
                (started_at, job_id, job_id),
            )

    def reattach(self, job_id: int) -> None:
        """The node answered again and the attempt is still running (section 9)."""
        with self._connect() as db:
            db.execute(
                "UPDATE jobs SET state = 'running', unreachable_since = NULL WHERE id = ? AND state = 'unreachable'",
                (job_id,),
            )
            db.execute(
                "UPDATE attempts SET state = 'running' WHERE job_id = ? AND state = 'unreachable'"
                " AND number = (SELECT MAX(number) FROM attempts WHERE job_id = ?)",
                (job_id, job_id),
            )

    def retry_overdue(self, now: float) -> List[int]:
        """Section 5.6: retry every unreachable job whose ``--retry-after`` has passed."""
        with self._connect() as db:
            rows = db.execute(
                "SELECT id, spec, unreachable_since FROM jobs WHERE state = 'unreachable' AND unreachable_since IS NOT NULL"
            ).fetchall()
        overdue = [
            row["id"]
            for row in rows
            if json.loads(row["spec"]).get("retry_after") is not None
            and now - row["unreachable_since"] >= json.loads(row["spec"])["retry_after"]
        ]
        for job_id in overdue:
            self.retry(job_id, now)
        return overdue

    def preview(self, command: str, request: Resources, spec: dict, now: float) -> Tuple[Optional[Action], Optional[Reason]]:
        """What the scheduler would do with this job right now, without storing it."""
        snapshot = self.snapshot(now)
        job_id = max((job.id for job in snapshot.jobs), default=0) + 1
        record = JobRecord(
            id=job_id, name=None, command=command, state="waiting_dependency" if spec.get("after") else "pending",
            reason=None, request=request, spec=spec, created_by="", submitted_at=now, pending_since=now,
            finished_at=None, cancel_requested=False,
        )
        result = plan(replace(snapshot, jobs=snapshot.jobs + (_scheduler_job(record),)))
        action = next((a for a in result.actions if a.job == job_id and isinstance(a, (Start, SubmitToSlurm))), None)
        return action, result.reasons.get(job_id)

    def job(self, job_id: int) -> JobRecord:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                raise UnknownJob(job_id)
            return _job_from_row(db, row)

    def job_ids(self) -> List[int]:
        with self._connect() as db:
            return [row[0] for row in db.execute("SELECT id FROM jobs")]

    def jobs(self, include_final: bool = False, include_runs: bool = False) -> List[JobRecord]:
        conditions, values = [], []
        if not include_final:
            conditions.append("state NOT IN ({})".format(", ".join("?" * len(FINAL_STATES))))
            values += FINAL_STATES
        if not include_runs:
            conditions.append("kind = 'submit'")
        query = "SELECT * FROM jobs" + (" WHERE " + " AND ".join(conditions) if conditions else "")
        with self._connect() as db:
            rows = db.execute(query + " ORDER BY id", values).fetchall()
            return [_job_from_row(db, row) for row in rows]

    # --- Immediate commands (fila run) ---

    def add_run(
        self, command: str, node: str, request: Resources, spec: dict, created_by: str, now: float,
        not_run: Optional[str] = None,
    ) -> int:
        """Record a ``run`` on one node: ``starting`` until the server launches
        it, or already ``not_run`` with the reason."""
        spec = dict(spec, node=node)
        state = "starting" if not_run is None else "not_run"
        with self._connect() as db:
            cursor = db.execute(
                "INSERT INTO jobs (kind, name, command, state, reason, request, spec, created_by, submitted_at,"
                " finished_at) VALUES ('run', NULL, ?, ?, ?, ?, ?, ?, ?, ?)",
                (command, state, not_run, json.dumps(dict(request)), json.dumps(spec), created_by, now,
                 None if not_run is None else now),
            )
            return int(cursor.lastrowid)

    def runs_to_start(self) -> List[JobRecord]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM jobs WHERE kind = 'run' AND state = 'starting' ORDER BY id").fetchall()
            return [_job_from_row(db, row) for row in rows]

    def start_run(self, job_id: int, now: float) -> bool:
        """The server launches a run: it gets its single attempt. False if it
        was cancelled before."""
        with self._connect() as db:
            row = db.execute("SELECT spec, cancel_requested FROM jobs WHERE id = ? AND state = 'starting'", (job_id,)).fetchone()
            if row is None:
                return False
            if row["cancel_requested"]:
                db.execute("UPDATE jobs SET state = 'cancelled', reason = 'requested', finished_at = ? WHERE id = ?", (now, job_id))
                return False
            db.execute("UPDATE jobs SET state = 'running' WHERE id = ?", (job_id,))
            db.execute(
                "INSERT INTO attempts (job_id, number, state, node, started_at) VALUES (?, 1, 'running', ?, ?)",
                (job_id, json.loads(row["spec"])["node"], now),
            )
            return True

    def set_hold(self, job_id: int, hold: Optional[bool]) -> None:
        with self._connect() as db:
            db.execute("UPDATE jobs SET hold = ? WHERE id = ?", (None if hold is None else int(hold), job_id))


def _dependents(rows: dict, job_id: int) -> List[int]:
    """Every job that depends on ``job_id``, directly or through others."""
    found: List[int] = []
    frontier = [job_id]
    while frontier:
        current = frontier.pop()
        for other_id, row in rows.items():
            if current in json.loads(row["spec"]).get("after", []) and other_id not in found:
                found.append(other_id)
                frontier.append(other_id)
    return sorted(found)


def _root_cause(rows: dict, job_id: int) -> int:
    """Follow blocked dependencies down to the job that actually ended badly."""
    current = job_id
    while rows[current]["state"] == "blocked":
        after = json.loads(rows[current]["spec"]).get("after", [])
        current = next(dep for dep in after if rows[dep]["state"] in ("failed", "cancelled", "blocked"))
    return current


_LAUNCHABLE = ("pending", "waiting_dependency")


def _apply_action(db: sqlite3.Connection, action: Action, now: float) -> bool:
    if isinstance(action, (Start, SubmitToSlurm)):
        state = "running" if isinstance(action, Start) else "slurm_queued"
        if not _transition(db, action.job, _LAUNCHABLE, state):
            return False
        number = db.execute("SELECT COALESCE(MAX(number), 0) + 1 FROM attempts WHERE job_id = ?", (action.job,)).fetchone()[0]
        cpus = action.cpus if isinstance(action, Start) else ()
        db.execute(
            "INSERT INTO attempts (job_id, number, state, node, cpus, started_at) VALUES (?, ?, ?, ?, ?, ?)",
            (action.job, number, state, action.node, json.dumps(list(cpus)), now if isinstance(action, Start) else None),
        )
        return True
    if isinstance(action, Block):
        return _transition(db, action.job, ("waiting_dependency",), "blocked")
    if isinstance(action, Unblock):
        return _transition(db, action.job, ("blocked",), "waiting_dependency")
    if isinstance(action, Release):
        return _transition(db, action.job, ("waiting_dependency",), "pending", pending_since=now)
    raise TypeError(f"unknown action: {action!r}")


def _transition(db: sqlite3.Connection, job_id: int, from_states: tuple, to_state: str, pending_since=None) -> bool:
    placeholders = ", ".join("?" * len(from_states))
    if pending_since is None:
        cursor = db.execute(
            f"UPDATE jobs SET state = ? WHERE id = ? AND state IN ({placeholders}) AND cancel_requested = 0",
            (to_state, job_id, *from_states),
        )
    else:
        cursor = db.execute(
            f"UPDATE jobs SET state = ?, pending_since = ? WHERE id = ? AND state IN ({placeholders})"
            " AND cancel_requested = 0",
            (to_state, pending_since, job_id, *from_states),
        )
    return cursor.rowcount == 1


def _scheduler_job(record: JobRecord) -> Job:
    """The scheduler's job. A ``run`` only occupies its node: it is never placed."""
    spec = record.spec
    if record.kind == "run":
        return Job(id=record.id, request=record.request, state="running", node=spec["node"])
    current = record.attempts[-1] if record.attempts and record.state in OCCUPYING else None
    return Job(
        id=record.id,
        request=record.request,
        state=record.state,
        node=current["node"] if current else None,
        cpus=tuple(current["cpus"]) if current else (),
        started_at=current["started_at"] if current else None,
        after=tuple(spec.get("after", ())),
        even_if_failed=spec.get("even_if_failed", False),
        submitted_at=record.submitted_at,
        pending_since=record.pending_since,
        on=None if spec.get("on") is None else tuple(spec["on"]),
        hint=spec.get("hint"),
        time_limit=spec.get("time_limit"),
        slurm_nodelist=None if spec.get("slurm_nodelist") is None else tuple(spec["slurm_nodelist"]),
        slurm_exclude=tuple(spec.get("slurm_exclude", ())),
    )


def _job_from_row(db: sqlite3.Connection, row: sqlite3.Row) -> JobRecord:
    attempts = [
        dict(attempt, cpus=json.loads(attempt["cpus"]))
        for attempt in db.execute("SELECT * FROM attempts WHERE job_id = ? ORDER BY number", (row["id"],))
    ]
    return JobRecord(
        id=row["id"],
        name=row["name"],
        command=row["command"],
        state=row["state"],
        reason=row["reason"],
        request=Resources(**json.loads(row["request"])),
        spec=json.loads(row["spec"]),
        created_by=row["created_by"],
        submitted_at=row["submitted_at"],
        pending_since=row["pending_since"],
        finished_at=row["finished_at"],
        cancel_requested=bool(row["cancel_requested"]),
        attempts=attempts,
        kind=row["kind"],
        hold=None if row["hold"] is None else bool(row["hold"]),
    )


def _node_to_dict(node: Node) -> dict:
    return {
        "name": node.name,
        "kind": node.kind,
        "state": node.state,
        "capacity": dict(node.capacity),
        "probe_free": dict(node.probe_free),
        "reserve": dict(node.reserve),
        "speed": node.speed,
        "topology": [list(core) for core in node.topology],
        "default_hint": node.default_hint,
        "slurm": [vars(machine) for machine in node.slurm],
        "max_cores_per_job": node.max_cores_per_job,
        "max_pending": node.max_pending,
        "exclude": list(node.exclude),
    }


def _node_from_dict(data: dict) -> Node:
    return Node(
        name=data["name"],
        kind=data["kind"],
        state=data["state"],
        capacity=Resources(**data["capacity"]),
        probe_free=Resources(**data["probe_free"]),
        reserve=Resources(**data["reserve"]),
        speed=data["speed"],
        topology=tuple(tuple(core) for core in data["topology"]),
        default_hint=data["default_hint"],
        slurm=tuple(SlurmMachine(**machine) for machine in data["slurm"]),
        max_cores_per_job=data["max_cores_per_job"],
        max_pending=data["max_pending"],
        exclude=tuple(data["exclude"]),
    )
