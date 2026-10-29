"""Section 4.1: the server, the only process that decides and launches.

Each cycle reads the registry, makes one call per computer (probe plus every
diary), reconciles the attempts, asks the scheduler what to do and launches
what the database accepted.
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from typing import Callable, Dict, List, Optional

from dataclasses import replace

from . import agent
from .executor import AgentError, Executor, NodeUnreachable, executor_for
from .files import read_json, write_json
from .registry import NodeEntry, Registry
from .scheduler import Action, Node, Resources, SlurmMachine, Start, SubmitToSlurm, plan
from .slurm import SlurmExecutor, outcome
from .store import Store
from .supervisor import pid_alive


class Server:
    def __init__(
        self,
        home: str,
        clock: Callable[[], float] = time.time,
        probe_factory: Optional[Callable[[NodeEntry], object]] = None,
    ) -> None:
        self.home = home
        self.store = Store(os.path.join(home, "fila.db"))
        self.registry = Registry(os.path.join(home, "nodes.toml"))
        self.clock = clock
        self._probe_factory = probe_factory  # tests: a fake probe for local nodes
        self._probes: Dict[str, object] = {}
        self.general: dict = {}

    def cycle(self) -> List[Action]:
        """One pass; returns the launch decisions that were applied."""
        now = self.clock()
        self.general, entries = self.registry.load()
        executors = {name: self.executor(entry) for name, entry in entries.items() if _is_computer(entry)}
        clusters = {
            name: SlurmExecutor(entry, executors[entry.via], self.general.get("ssh_command") or ["ssh"])
            for name, entry in entries.items()
            if entry.type == "slurm" and entry.via in executors
        }
        statuses, in_use = self._visit_nodes(entries, executors, now)
        slurm_states = self._visit_clusters(entries, clusters, now)
        self.store.keep_nodes(entries)
        self._reconcile(entries, executors, in_use, statuses, now)
        self._reconcile_slurm(clusters, slurm_states, now)
        self.store.retry_overdue(now)
        self._start_runs(executors, now)
        snapshot = self.store.snapshot(now, starvation_after=float(self.general["starvation_after"]))
        applied = self.store.apply(plan(snapshot), now)
        for action in applied:
            if isinstance(action, Start) and action.node in executors:
                self._launch(action, entries[action.node], executors[action.node], now)
            elif isinstance(action, SubmitToSlurm) and action.node in clusters:
                self._submit(action, clusters[action.node], now)
        _write_heartbeat(self.home, now)
        return applied

    def serve(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        """Cycle until ``should_stop``; a touch of ``<home>/wake`` starts the next cycle at once."""
        wake = os.path.join(self.home, "wake")
        while not should_stop():
            last_wake = _mtime(wake)  # read before the cycle: a touch during it must start the next one
            try:
                self.cycle()
            except Exception:  # one bad cycle must not stop the coordinator; the next one retries
                _log(f"cycle failed:\n{traceback.format_exc()}")
            deadline = time.time() + float(self.general.get("probe_interval_s", 30))
            while time.time() < deadline and not should_stop() and _mtime(wake) == last_wake:
                time.sleep(0.2)

    def executor(self, entry: NodeEntry) -> Executor:
        probe = None
        if entry.access == "local" and self._probe_factory is not None:
            probe = self._probes.setdefault(entry.name, self._probe_factory(entry))
        return executor_for(
            entry, self.general, notify=os.path.join(self.home, "wake"), probe=probe, store=self.store
        )

    # --- Nodes ---

    def _visit_nodes(self, entries: Dict[str, NodeEntry], executors: Dict[str, Executor], now: float) -> tuple:
        """Probe every computer and read its diaries; record its view for the scheduler."""
        drained = self.store.drained()
        previous = {node.name: node for node in self.store.nodes()}
        by_node: Dict[str, list] = {}
        for job, attempt in self.store.active_attempts():
            by_node.setdefault(attempt["node"], []).append((job, attempt))
        statuses: Dict[int, Optional[dict]] = {}
        in_use: Dict[str, bool] = {}
        for name, executor in executors.items():
            attempts = by_node.get(name, [])
            try:
                answer = executor.cycle(
                    [{"job": job.id, "attempt": a["number"], "launched_at": a["started_at"]} for job, a in attempts], now
                )
            except NodeUnreachable as error:
                self.store.put_node(_unusable_view(entries[name], previous.get(name), "unreachable"))
                self.store.mark_unreachable(name, now)
                _log(str(error), quiet=previous.get(name) is not None and previous[name].state == "unreachable")
                continue
            except AgentError as error:
                self.store.put_node(_unusable_view(entries[name], previous.get(name), "outdated"))
                _log(str(error))
                continue
            probe = answer["probe"]
            if probe.get("protocol") != agent.PROTOCOL:
                self.store.put_node(_unusable_view(entries[name], previous.get(name), "outdated"))
                continue
            view, in_use[name] = node_view(entries[name], probe, name in drained)
            self.store.put_node(view)
            for (job, _), status in zip(attempts, answer["statuses"]):
                statuses[job.id] = status
        return statuses, in_use

    def _visit_clusters(self, entries: Dict[str, NodeEntry], clusters: Dict[str, SlurmExecutor], now: float) -> dict:
        """Probe each cluster through its via computer and read Slurm's states of fila's jobs there."""
        drained = self.store.drained()
        previous = {node.name: node for node in self.store.nodes()}
        ids: Dict[str, List[str]] = {}
        for job, attempt in self.store.active_attempts():
            if attempt["node"] in clusters and attempt["slurm_id"]:
                ids.setdefault(attempt["node"], []).append(attempt["slurm_id"])
        states: Dict[str, Dict[str, dict]] = {}
        for name, entry in entries.items():
            if entry.type != "slurm":
                continue
            cluster = clusters.get(name)
            try:
                if cluster is None:
                    raise NodeUnreachable(f"{name}: via {entry.via} is not a computer fila can reach")
                probe = cluster.probe()
                states[name] = cluster.states(ids.get(name, []))
            except (NodeUnreachable, AgentError) as error:
                self.store.put_node(_unusable_view(entry, previous.get(name), "unreachable"))
                self.store.mark_unreachable(name, now)
                _log(str(error), quiet=previous.get(name) is not None and previous[name].state == "unreachable")
                continue
            self.store.put_node(slurm_view(entry, probe, name in drained))
        return states

    # --- Attempts ---

    def _submit(self, action: SubmitToSlurm, cluster: SlurmExecutor, now: float) -> None:
        job = self.store.job(action.job)
        try:
            slurm_id = cluster.submit(job.id, len(job.attempts), job.command, dict(job.request), job.spec)
        except NodeUnreachable as error:  # lost the cluster between the probe and now: queue it again
            self.store.finish(job.id, "failed", now, reason="start_failed")
            self.store.retry(job.id, now)
            _log(str(error))
            return
        except AgentError as error:
            self.store.finish(job.id, "failed", now, reason="start_failed")
            _log(f"job {job.id}: {error}")
            return
        self.store.set_slurm_id(job.id, slurm_id)

    def _reconcile_slurm(self, clusters: Dict[str, SlurmExecutor], states: Dict[str, Dict[str, dict]], now: float) -> None:
        """Section 8: sacct/squeue decide; Slurm itself enforces cores, memory and time."""
        for job, attempt in self.store.active_attempts():
            node, slurm_id = attempt["node"], attempt["slurm_id"]
            if node not in states or not slurm_id or slurm_id not in states[node]:
                continue
            state = states[node][slurm_id]
            fila_state, reason = outcome(state["state"])
            if fila_state == "running":
                if job.state != "running":
                    self.store.slurm_started(job.id, state["start"] or now)
            elif fila_state != "pending":
                self.store.finish(job.id, fila_state, state["end"] or now, exit_code=state["exit_code"], reason=reason)
                continue
            if job.cancel_requested:
                try:
                    clusters[node].cancel(slurm_id)
                except (NodeUnreachable, AgentError) as error:
                    _log(str(error))

    def _launch(self, action: Start, entry: NodeEntry, executor: Executor, now: float) -> None:
        job = self.store.job(action.job)
        try:
            executor.launch(
                job.id, len(job.attempts), job.command, list(action.cpus), dict(job.request),
                job.spec.get("time_limit", entry.max_time), directory=job.spec.get("dir"), script=job.spec.get("script"),
            )
        except NodeUnreachable as error:  # lost the node between the probe and now: queue it again
            self.store.finish(job.id, "failed", now, reason="start_failed")
            self.store.retry(job.id, now)
            _log(str(error))
        except (AgentError, OSError) as error:
            self.store.finish(job.id, "failed", now, reason="start_failed")
            _log(f"job {job.id}: cannot start: {error}")

    def _start_runs(self, executors: Dict[str, Executor], now: float) -> None:
        """Section 5.2: a ``run`` starts at once, outside the queue, in the
        node's home directory unless ``--dir``."""
        for job in self.store.runs_to_start():
            executor = executors.get(job.spec["node"])
            if executor is None or not self.store.start_run(job.id, now):
                continue
            try:
                executor.launch(
                    job.id, 1, job.command, [], dict(job.request),
                    job.spec.get("time_limit", float(self.general["run_timeout"])), directory=job.spec.get("dir") or "~",
                )
            except NodeUnreachable:
                self.store.finish(job.id, "not_run", now, reason="unreachable")
            except (AgentError, OSError) as error:
                self.store.finish(job.id, "failed", now, reason="start_failed")
                _log(f"run {job.id}: cannot start: {error}")

    def _reconcile(self, entries: Dict[str, NodeEntry], executors: Dict[str, Executor], in_use: dict,
                   statuses: Dict[int, Optional[dict]], now: float) -> None:
        """Section 9: bring each attempt's diary into the database."""
        for job, attempt in self.store.active_attempts():
            if job.id not in statuses or attempt["node"] not in executors:  # unreachable, or a cluster
                continue
            status = statuses[job.id]
            if status is None:
                continue
            if status["state"] == "lost":
                self.store.finish(job.id, "failed", now, reason="lost")
                continue
            if status["state"] == "finished":
                state, reason = _outcome(status)
                ended = status.get("finished_at") or now  # when it really ended, not when we noticed
                self.store.finish(job.id, state, ended, exit_code=status["exit_code"], reason=reason)
                continue
            if job.state == "unreachable":  # the node is back and the attempt survived
                self.store.reattach(job.id)
            executor = executors[attempt["node"]]
            try:
                if job.cancel_requested:
                    executor.request(job.id, attempt["number"], "cancel")
                    continue
                if job.hold is not None:  # fila suspend / fila resume win over on_use
                    suspend = job.hold
                else:
                    policy = job.spec.get("on_use") or entries[attempt["node"]].on_use
                    suspend = policy == "suspend" and bool(in_use.get(attempt["node"]))
                if suspend != bool(status.get("suspended")):
                    executor.request(job.id, attempt["number"], "suspend" if suspend else "resume")
            except (NodeUnreachable, AgentError) as error:  # the next cycle tries again
                _log(str(error))


def node_view(entry: NodeEntry, probe: dict, drained: bool) -> tuple:
    """Section 6: the scheduler's node for this cycle from an agent probe, and
    whether someone is using the machine."""
    hardware, reading = probe["hardware"], probe["reading"]
    capacity = Resources(
        cores=min(entry.limits.get("cores", hardware["logical_cpus"]), hardware["logical_cpus"]),
        mem=min(entry.limits.get("mem", hardware["total_mem"]), hardware["total_mem"]),
    )
    idle = reading["idle_seconds"]
    in_use = idle is not None and idle < entry.idle_minutes * 60
    reserve = entry.reserve_in_use if in_use and entry.reserve_in_use else entry.reserve_always
    state = "online"
    if drained:
        state = "drained"
    elif entry.min_free_disk is not None and probe["disk_free"] < entry.min_free_disk:
        state = "low_disk"
    view = Node(
        name=entry.name,
        capacity=capacity,
        probe_free=Resources(cores=reading["free_cores"], mem=reading["available_mem"]),
        reserve=Resources(**reserve),
        state=state,
        speed=entry.speed,
        topology=tuple(tuple(core) for core in hardware["topology"]),
        default_hint=entry.default_hint,
    )
    return view, in_use


def slurm_view(entry: NodeEntry, probe: dict, drained: bool) -> Node:
    """Section 7.1/7.5: a cluster as the scheduler sees it, from ``sinfo``/``scontrol``."""
    machines = tuple(SlurmMachine(**machine) for machine in probe["machines"])
    capacity = Resources(
        cores=entry.limits.get("cores", sum(m.total_cores for m in machines)),
        mem=entry.limits.get("mem", sum(m.total_mem for m in machines)),
    )
    state = "online"
    if drained:
        state = "drained"
    elif entry.min_free_disk is not None and probe["disk_free"] < entry.min_free_disk:
        state = "low_disk"
    return Node(
        name=entry.name, kind="slurm", capacity=capacity, probe_free=capacity, state=state, speed=entry.speed,
        default_hint=entry.default_hint, slurm=machines,
        max_cores_per_job=entry.max_cores_per_job or probe["max_cores_per_job"], max_pending=entry.max_pending,
        exclude=tuple(entry.exclude),
    )


def _unusable_view(entry: NodeEntry, previous: Optional[Node], state: str) -> Node:
    """Keep the last known capacity (a submit is checked against it) but take the node out."""
    if previous is not None:
        return replace(previous, state=state, speed=entry.speed)
    capacity = Resources(cores=entry.limits.get("cores", 0), mem=entry.limits.get("mem", 0))
    return Node(name=entry.name, capacity=capacity, probe_free=capacity, state=state, speed=entry.speed)


def _outcome(status: dict) -> tuple:
    if status["reason"] is None and status["exit_code"] == 0:
        return "completed", None
    if status["reason"] == "cancelled":
        return "cancelled", "requested"
    return "failed", status["reason"] or "exit_code"


HEARTBEAT = "server.json"


def _write_heartbeat(home: str, now: float) -> None:
    write_json(os.path.join(home, HEARTBEAT), {"pid": os.getpid(), "at": now})


def server_alive(home: str, now: float, interval: float) -> bool:
    """Whether a server process wrote its heartbeat recently."""
    beat = read_json(os.path.join(home, HEARTBEAT))
    return beat is not None and now - beat["at"] < max(3 * interval, 60.0) and pid_alive(beat["pid"])


def _is_computer(entry: NodeEntry) -> bool:
    return entry.type == "computer" and entry.access in ("local", "ssh", "pull")


def _mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except OSError:
        return None


def _log(text: str, quiet: bool = False) -> None:
    if not quiet:
        sys.stderr.write(f"fila server {time.strftime('%Y-%m-%d %H:%M:%S')}: {text}\n")
        sys.stderr.flush()
