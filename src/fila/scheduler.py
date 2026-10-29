"""Scheduling decisions as a pure function.

``plan`` receives a ``Snapshot`` of the nodes and jobs at one instant and
returns what the server should do now, and why each waiting job did not move.
It reads no clock, disk or network, so every rule of the specification
(sections 5 to 7) is testable in isolation. ``check_request`` is the
submit-time check that a request can ever run.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Iterator, List, Mapping, Optional, Tuple, Union


class Resources(Mapping[str, int]):
    """Countable amounts by name (``cores``, ``mem`` in bytes, ...).

    A missing name counts as zero, so arithmetic works between resources that
    do not list the same names.
    """

    def __init__(self, **amounts: int) -> None:
        self._amounts = dict(amounts)

    def __getitem__(self, name: str) -> int:
        return self._amounts[name]

    def __iter__(self) -> Iterator[str]:
        return iter(self._amounts)

    def __len__(self) -> int:
        return len(self._amounts)

    def __repr__(self) -> str:
        inner = ", ".join(f"{name}={amount}" for name, amount in self._amounts.items())
        return f"Resources({inner})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Mapping) and dict(self.items()) == dict(other.items())

    def __hash__(self) -> int:
        return hash(frozenset(self._amounts.items()))

    def _names(self, other: Resources) -> List[str]:
        return list(dict.fromkeys([*self, *other]))

    def __add__(self, other: Resources) -> Resources:
        return Resources(**{n: self.get(n, 0) + other.get(n, 0) for n in self._names(other)})

    def __sub__(self, other: Resources) -> Resources:
        return Resources(**{n: self.get(n, 0) - other.get(n, 0) for n in self._names(other)})

    def minimum(self, other: Resources) -> Resources:
        return Resources(**{n: min(self.get(n, 0), other.get(n, 0)) for n in self._names(other)})

    def fits_in(self, other: Resources) -> bool:
        return all(amount <= other.get(name, 0) for name, amount in self.items())


NOTHING = Resources()


def total(resources: Iterable[Resources]) -> Resources:
    result = NOTHING
    for item in resources:
        result = result + item
    return result


@dataclass(frozen=True)
class SlurmMachine:
    """One compute node inside a cluster, as ``sinfo -N`` reports it."""

    name: str
    idle_cores: int
    free_mem: int
    total_cores: int
    total_mem: int

    def free(self) -> Resources:
        return Resources(cores=self.idle_cores, mem=self.free_mem)

    def total(self) -> Resources:
        return Resources(cores=self.total_cores, mem=self.total_mem)


@dataclass(frozen=True)
class Node:
    """A registered node as the server sees it in this cycle.

    ``capacity`` is the registered limit or the hardware; ``probe_free`` is what
    the node reported as free; ``reserve`` is the margin already chosen for
    this cycle (``always`` or ``in_use``). A ``slurm`` node uses ``slurm``,
    ``max_cores_per_job``, ``max_pending`` and ``exclude`` instead of the probe
    and the topology.
    """

    name: str
    capacity: Resources
    probe_free: Resources
    kind: str = "computer"  # "computer" | "slurm"
    reserve: Resources = NOTHING
    state: str = "online"
    speed: float = 1.0
    topology: Tuple[Tuple[int, ...], ...] = ()
    default_hint: str = "multithread"
    slurm: Tuple[SlurmMachine, ...] = ()
    max_cores_per_job: Optional[int] = None
    max_pending: int = 2
    exclude: Tuple[str, ...] = ()

    def physical_cores(self) -> Tuple[Tuple[int, ...], ...]:
        """Logical CPU ids grouped by physical core; without a topology, one CPU per core."""
        return self.topology or tuple((cpu,) for cpu in range(self.capacity.get("cores", 0)))


@dataclass(frozen=True)
class Job:
    id: int
    request: Resources
    state: str = "pending"
    node: Optional[str] = None
    cpus: Tuple[int, ...] = ()
    after: Tuple[int, ...] = ()
    even_if_failed: bool = False
    submitted_at: float = 0.0
    on: Optional[Tuple[str, ...]] = None
    hint: Optional[str] = None  # "multithread" | "nomultithread"; None takes the node default
    pending_since: Optional[float] = None  # when it became ready; None means submitted_at
    started_at: Optional[float] = None
    time_limit: Optional[float] = None  # seconds; None means no declared limit
    slurm_nodelist: Optional[Tuple[str, ...]] = None
    slurm_exclude: Tuple[str, ...] = ()

    def waiting_since(self) -> float:
        return self.submitted_at if self.pending_since is None else self.pending_since

    def expected_end(self) -> float:
        if self.started_at is None or self.time_limit is None:
            return float("inf")
        return self.started_at + self.time_limit


# States in which a job holds resources on its node (pending in Slurm counts too).
OCCUPYING = ("running", "unreachable", "slurm_queued")
UNSUCCESSFUL = ("failed", "cancelled", "blocked")
FINAL = ("completed",) + UNSUCCESSFUL


@dataclass(frozen=True)
class Snapshot:
    """The nodes and jobs at ``now``. ``jobs`` must include every dependency of
    the waiting jobs it lists, finished ones too."""

    now: float
    nodes: Tuple[Node, ...]
    jobs: Tuple[Job, ...]
    starvation_after: float = 3600.0


@dataclass(frozen=True)
class Start:
    job: int
    node: str
    cpus: Tuple[int, ...]


@dataclass(frozen=True)
class SubmitToSlurm:
    job: int
    node: str


@dataclass(frozen=True)
class Block:
    """The job's dependency ``cause`` ended without success (section 5.4)."""

    job: int
    cause: int


@dataclass(frozen=True)
class Unblock:
    """A blocked job waits again: its unsuccessful dependency was retried."""

    job: int


@dataclass(frozen=True)
class Release:
    """Every dependency is done but the job did not start: it becomes ``pending``."""

    job: int


Action = Union[Start, SubmitToSlurm, Block, Unblock, Release]


@dataclass(frozen=True)
class Reason:
    """Why a waiting job did not move (``fila explain``): a summary and one line per node."""

    summary: str
    details: Tuple[str, ...] = ()


@dataclass(frozen=True)
class Plan:
    actions: List[Action]
    reasons: Mapping[int, Reason]  # by job id


def check_request(
    nodes: Iterable[Node],
    request: Resources,
    on: Optional[Tuple[str, ...]] = None,
    hint: Optional[str] = None,
) -> Optional[str]:
    """Return why ``request`` can never run, or ``None`` if some node can hold it.

    Only capacity matters, not what is free now or whether the node is
    reachable: a job that fits a node that is busy or down waits as ``pending``.
    """
    nodes = [node for node in nodes if on is None or node.name in on]
    if any(_holds(node, request, hint) for node in nodes):
        return None
    if not nodes:
        return f"no node can hold {describe(request)}: no candidate nodes"
    largest = max(nodes, key=lambda node: tuple(node.capacity.get(name, 0) for name in request))
    return (
        f"no node can hold {describe(request)}: "
        f"largest candidate is {largest.name} with {describe(largest.capacity)}"
    )


def free_now(node: Node, jobs: Iterable[Job]) -> Resources:
    """What ``node`` has free for one more job right now (section 6.1)."""
    return _free(node, [job for job in jobs if job.node == node.name and job.state in OCCUPYING])


def describe(resources: Resources) -> str:
    return ", ".join(f"{name}={_amount(name, amount)}" for name, amount in resources.items())


def _amount(name: str, amount: int) -> str:
    if name != "mem":
        return str(amount)
    for unit, size in (("T", 1024**4), ("G", 1024**3), ("M", 1024**2), ("K", 1024)):
        if amount >= size:
            return f"{amount // size}{unit}" if amount % size == 0 else f"{amount / size:.1f}{unit}"
    return str(amount)


def plan(snapshot: Snapshot) -> Plan:
    actions, state = _dependency_transitions(snapshot.jobs)
    by_id = {job.id: job for job in snapshot.jobs}
    reasons = {}
    for job in snapshot.jobs:
        if state[job.id] == "blocked":
            cause = _blocking_cause(job, state)
            reasons[job.id] = Reason(f"blocked: dependency {cause} is {state[cause]}")
        elif job.state == "waiting_dependency" and not _ready(job, by_id):
            waiting = ", ".join(f"{dep} ({by_id[dep].state})" for dep in job.after if not _done(job, by_id[dep]))
            reasons[job.id] = Reason(f"waiting_dependency: waiting for {waiting}")

    ready = [
        job
        for job in sorted(snapshot.jobs, key=lambda job: (job.submitted_at, job.id))
        if state[job.id] != "blocked" and _ready(job, by_id)
    ]
    starving = _starving(ready, snapshot)
    if starving is not None:
        ready.remove(starving)
        ready.insert(0, starving)

    cycle = _Cycle(snapshot.jobs)
    for job in ready:
        placed, details = _place(job, snapshot.nodes, cycle)
        if placed is None:
            placed = _pending_allowance(job, snapshot.nodes, cycle)
        if placed is not None:
            action, machine = placed
            actions.append(action)
            cycle.record(job, action, machine)
            continue
        if job.state == "waiting_dependency":
            actions.append(Release(job=job.id))
        summary = "pending: no node can start it now"
        if job is starving:
            reserved = _reservation(job, snapshot.nodes, cycle)
            if reserved is not None:
                cycle.reserved[reserved] = job.id
                summary += f"; {reserved} is reserved for it (starvation protection)"
        reasons[job.id] = Reason(summary, details)
    return Plan(actions=actions, reasons=reasons)


class _Cycle:
    """What one planning cycle has already committed, on top of the snapshot."""

    def __init__(self, jobs: Tuple[Job, ...]) -> None:
        self.occupying = [job for job in jobs if job.state in OCCUPYING]
        self.taken: dict = {}  # (node, Slurm machine) -> resources submitted this cycle
        self.pending_slurm: dict = {}
        self.reserved: dict = {}  # node -> starving job id
        for job in jobs:
            if job.state == "slurm_queued":
                self.pending_slurm[job.node] = self.pending_slurm.get(job.node, 0) + 1

    def on_node(self, name: str) -> List[Job]:
        return [job for job in self.occupying if job.node == name]

    def machine_free(self, node: Node, machine: SlurmMachine) -> Resources:
        return machine.free() - self.taken.get((node.name, machine.name), NOTHING)

    def record(self, job: Job, action: Action, machine: Optional[str]) -> None:
        if isinstance(action, Start):
            self.occupying.append(replace(job, state="running", node=action.node, cpus=action.cpus))
            return
        self.occupying.append(replace(job, state="slurm_queued", node=action.node, cpus=()))
        if machine is None:  # submitted on the pending allowance
            self.pending_slurm[action.node] = self.pending_slurm.get(action.node, 0) + 1
        else:
            key = (action.node, machine)
            self.taken[key] = self.taken.get(key, NOTHING) + job.request


# --- Dependencies (section 5.4) ---


def _dependency_transitions(jobs: Tuple[Job, ...]) -> Tuple[List[Action], dict]:
    """Block and Unblock actions for this cycle, and every job's state after them.

    A waiting job is blocked when a dependency is unsuccessful; a blocked job
    waits again when none is (its cause was retried). Both propagate down the
    chain in the same cycle.
    """
    state = {job.id: job.state for job in jobs}
    changed = True
    while changed:
        changed = False
        for job in jobs:
            if job.even_if_failed or state[job.id] not in ("waiting_dependency", "blocked"):
                continue
            new_state = "waiting_dependency" if _blocking_cause(job, state) is None else "blocked"
            if new_state != state[job.id]:
                state[job.id] = new_state
                changed = True
    actions: List[Action] = []
    for job in jobs:
        if state[job.id] == job.state:
            continue
        if state[job.id] == "blocked":
            actions.append(Block(job=job.id, cause=_blocking_cause(job, state)))
        else:
            actions.append(Unblock(job=job.id))
    return actions, state


def _blocking_cause(job: Job, state: Mapping[int, str]) -> Optional[int]:
    return next((dep for dep in job.after if state.get(dep) in UNSUCCESSFUL), None)


def _done(job: Job, dependency: Job) -> bool:
    """Whether ``dependency`` no longer holds ``job`` back; with ``even_if_failed``
    any final state is enough."""
    return dependency.state in (FINAL if job.even_if_failed else ("completed",))


def _ready(job: Job, by_id: Mapping[int, Job]) -> bool:
    if job.state == "pending":
        return True
    return job.state == "waiting_dependency" and all(_done(job, by_id[dep]) for dep in job.after)


# --- Queue order and starvation (section 7.2) ---


def _starving(ready: List[Job], snapshot: Snapshot) -> Optional[Job]:
    """The pending job that has waited longest, if past ``starvation_after``.

    A job released from its dependencies in this cycle has not waited yet.
    """
    pending = [job for job in ready if job.state == "pending"]
    if not pending:
        return None
    oldest = min(pending, key=lambda job: job.waiting_since())
    if snapshot.now - oldest.waiting_since() < snapshot.starvation_after:
        return None
    return oldest


def _reservation(job: Job, nodes: Tuple[Node, ...], cycle: _Cycle) -> Optional[str]:
    """Among the computers that can hold ``job``, the one whose fila jobs are
    expected (by their declared time limits) to free enough room first.

    Slurm keeps its own queue; a starving job reaches it through the pending
    allowance instead.
    """
    best = None
    for node in _candidates(job, nodes, cycle):
        if node.kind != "computer" or not _holds(node, job.request, job.hint):
            continue
        remaining = sorted(cycle.on_node(node.name), key=Job.expected_end)
        estimate = float("-inf")
        while not _fits_capacity(node, job, remaining):
            estimate = remaining.pop(0).expected_end()
        key = (estimate, -node.speed, node.name)
        if best is None or key < best:
            best = key
    return None if best is None else best[2]


def _fits_capacity(node: Node, job: Job, on_node: List[Job]) -> bool:
    """Whether ``job`` fits next to ``on_node`` by capacity alone, ignoring the probe."""
    assignment = _assign_cpus(node, job.request["cores"], _blocked_cpus(node, on_node), job.hint)
    if assignment is None:
        return False
    reserved = total(_footprint(node, other) for other in on_node)
    return _with_cores(job.request, assignment[1]).fits_in(node.capacity - reserved)


# --- Node choice (sections 7.1, 7.3 and 7.5) ---


def _candidates(job: Job, nodes: Iterable[Node], cycle: _Cycle) -> List[Node]:
    return [node for node in nodes if _excluded(job, node, cycle) is None]


def _excluded(job: Job, node: Node, cycle: _Cycle) -> Optional[str]:
    """Why ``node`` is not a candidate for ``job`` at all, or ``None``."""
    if job.on is not None and node.name not in job.on:
        return "excluded by --on"
    if node.state != "online":
        return node.state
    if node.name in cycle.reserved:
        return f"reserved for job {cycle.reserved[node.name]} (starvation protection)"
    return None


def _place(job: Job, nodes: Tuple[Node, ...], cycle: _Cycle) -> Tuple[Optional[tuple], Tuple[str, ...]]:
    """The fastest node that can start the job now; ties go to the node left
    with less, which keeps large holes for large jobs.

    Returns (action, Slurm machine) or ``None``, and one line per node that
    could not start it.
    """
    best = None
    details = []
    for node in nodes:
        option = _start_now(job, node, cycle)
        if isinstance(option, str):
            details.append(f"{node.name}: {option}")
        elif best is None or option[0] < best[0]:
            best = option
    return (None if best is None else best[1:]), tuple(details)


def _start_now(job: Job, node: Node, cycle: _Cycle) -> Union[tuple, str]:
    """(choice key, action, Slurm machine) if ``job`` can start on ``node`` now,
    otherwise the reason it cannot."""
    excluded = _excluded(job, node, cycle)
    if excluded is not None:
        return excluded
    on_node = cycle.on_node(node.name)
    free = _free(node, on_node)
    if node.kind == "slurm":
        if not _within_job_limit(node, job.request):
            return f"cores={job.request['cores']} is above the per-job limit of {node.max_cores_per_job}"
        if not job.request.fits_in(free):
            return _shortfall(node, on_node, job.request)
        machine = next((m for m in _machines(node, job) if job.request.fits_in(cycle.machine_free(node, m))), None)
        if machine is None:
            pending = cycle.pending_slurm.get(node.name, 0)
            return (
                f"no Slurm machine has {describe(job.request)} idle now; "
                f"{pending} of {node.max_pending} allowed jobs already pending in Slurm"
            )
        need, action, machine_name = job.request, SubmitToSlurm(job=job.id, node=node.name), machine.name
    else:
        assignment = _assign_cpus(node, job.request["cores"], _blocked_cpus(node, on_node), job.hint)
        if assignment is None:
            return f"not enough free whole physical cores for cores={job.request['cores']}"
        cpus, footprint = assignment
        need = _with_cores(job.request, footprint)
        if not need.fits_in(free):
            return _shortfall(node, on_node, need)
        action, machine_name = Start(job=job.id, node=node.name, cpus=cpus), None
    left = free - need
    key = (-node.speed, tuple(left.get(name, 0) for name in job.request))
    return key, action, machine_name


def _pending_allowance(job: Job, nodes: Tuple[Node, ...], cycle: _Cycle) -> Optional[tuple]:
    """Section 7.5: when no node can start the job now, submit it to a cluster
    anyway while fila has fewer than ``max_pending`` jobs pending there."""
    best = None
    for node in _candidates(job, nodes, cycle):
        if node.kind != "slurm" or cycle.pending_slurm.get(node.name, 0) >= node.max_pending:
            continue
        if not _holds(node, job.request, job.hint, job):
            continue
        if not job.request.fits_in(_free(node, cycle.on_node(node.name))):
            continue
        key = (-node.speed, node.name)
        if best is None or key < best[0]:
            best = (key, SubmitToSlurm(job=job.id, node=node.name))
    return None if best is None else (best[1], None)


def _machines(node: Node, job: Optional[Job] = None) -> List[SlurmMachine]:
    """The cluster's Slurm machines this job may land on (``--slurm-nodelist``/``--slurm-exclude``)."""
    excluded = set(node.exclude) | set(job.slurm_exclude if job else ())
    allowed = job.slurm_nodelist if job else None
    return [m for m in node.slurm if m.name not in excluded and (allowed is None or m.name in allowed)]


def _within_job_limit(node: Node, request: Resources) -> bool:
    return node.max_cores_per_job is None or request.get("cores", 0) <= node.max_cores_per_job


def _holds(node: Node, request: Resources, hint: Optional[str], job: Optional[Job] = None) -> bool:
    """Whether the empty node could ever hold ``request``.

    A cluster holds it when one Slurm machine does, within the per-job limit
    and the registered capacity.
    """
    if node.kind == "slurm":
        return (
            _within_job_limit(node, request)
            and request.fits_in(node.capacity)
            and any(request.fits_in(m.total()) for m in _machines(node, job))
        )
    assignment = _assign_cpus(node, request.get("cores", 0), set(), hint)
    return assignment is not None and _with_cores(request, assignment[1]).fits_in(node.capacity)


# --- Resources and CPUs (sections 6.1 and 6.2) ---


def _free(node: Node, on_node: List[Job]) -> Resources:
    """Section 6.1: the smaller of what fila has not reserved and what the probe sees."""
    return _accounted_free(node, on_node).minimum(node.probe_free - node.reserve)


def _accounted_free(node: Node, on_node: List[Job]) -> Resources:
    return node.capacity - total(_footprint(node, job) for job in on_node)


def _shortfall(node: Node, on_node: List[Job], need: Resources) -> str:
    """Which resources ``need`` lacks on ``node``, and how much of the gap is
    load outside fila or the configured reserve."""
    accounted = _accounted_free(node, on_node)
    parts = []
    for name, amount in need.items():
        free = min(accounted.get(name, 0), node.probe_free.get(name, 0) - node.reserve.get(name, 0))
        if amount <= free:
            continue
        text = f"needs {name}={_amount(name, amount)}, free {_amount(name, max(free, 0))}"
        outside = accounted.get(name, 0) - node.probe_free.get(name, 0)
        notes = []
        if outside > 0:
            notes.append(f"{_amount(name, outside)} in use outside fila")
        if node.reserve.get(name, 0) > 0:
            notes.append(f"{_amount(name, node.reserve[name])} kept as reserve")
        if notes:
            text += f" ({', '.join(notes)})"
        parts.append(text)
    return "; ".join(parts)


def _blocked_cpus(node: Node, on_node: List[Job]) -> set:
    """Every logical CPU of a physical core that some job on the node touches."""
    touched = {cpu for job in on_node for cpu in job.cpus}
    return {cpu for core in node.physical_cores() if touched & set(core) for cpu in core}


def _assign_cpus(
    node: Node, cores: int, blocked: set, hint: Optional[str]
) -> Optional[Tuple[Tuple[int, ...], int]]:
    """Section 6.2: CPUs from whole free physical cores, lowest first.

    Returns the CPUs the job may use and its footprint, the number of logical
    CPUs it takes from other jobs: an odd request on SMT leaves a sibling idle,
    and ``nomultithread`` (from the job, else the node default) holds every
    sibling.
    """
    one_per_core = (hint or node.default_hint) == "nomultithread"
    cpus: List[int] = []
    footprint = 0
    for core in node.physical_cores():
        if len(cpus) >= cores:
            break
        if blocked & set(core):
            continue
        cpus.extend(core[:1] if one_per_core else core[: cores - len(cpus)])
        footprint += len(core)
    if len(cpus) < cores:
        return None
    return tuple(cpus), footprint


def _footprint(node: Node, job: Job) -> Resources:
    if not job.cpus:
        return job.request
    return _with_cores(job.request, len(_blocked_cpus(node, [job])))


def _with_cores(resources: Resources, cores: int) -> Resources:
    return Resources(**{**resources, "cores": cores})
