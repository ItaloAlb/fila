import unittest

from fila.scheduler import (
    Block,
    Job,
    Node,
    Release,
    Resources,
    SlurmMachine,
    Snapshot,
    Start,
    SubmitToSlurm,
    Unblock,
    check_request,
    plan,
)

GiB = 1024**3


def computer(name, cores=4, mem=8 * GiB, probe_free=None, **fields):
    resources = Resources(cores=cores, mem=mem)
    return Node(name=name, capacity=resources, probe_free=probe_free or resources, **fields)


def cluster(name, slurm_nodes, capacity=None, **fields):
    """A Slurm node of fila; ``slurm_nodes`` are (name, idle cores, free mem, total cores, total mem)."""
    machines = tuple(SlurmMachine(*row) for row in slurm_nodes)
    if capacity is None:
        capacity = Resources(cores=sum(m.total_cores for m in machines), mem=sum(m.total_mem for m in machines))
    return Node(name=name, kind="slurm", capacity=capacity, probe_free=capacity, slurm=machines, **fields)


def job(id, cores=1, mem=1 * GiB, state="pending", **fields):
    return Job(id=id, request=Resources(cores=cores, mem=mem), state=state, **fields)


def snapshot(nodes, jobs, now=0.0):
    return Snapshot(now=now, nodes=tuple(nodes), jobs=tuple(jobs))


class PlacementTest(unittest.TestCase):
    def test_job_that_fits_starts_on_the_online_node(self):
        result = plan(snapshot([computer("lab", cores=4)], [job(1, cores=2)]))

        self.assertEqual(result.actions, [Start(job=1, node="lab", cpus=(0, 1))])


class FreeResourcesTest(unittest.TestCase):
    """Free = min(capacity - reserved by fila, probe - reserve), per resource."""

    def test_probe_showing_outside_load_keeps_job_waiting(self):
        busy = computer("lab", cores=4, probe_free=Resources(cores=1, mem=8 * GiB))

        result = plan(snapshot([busy], [job(1, cores=2)]))

        self.assertEqual(result.actions, [])

    def test_fila_reservations_count_even_before_the_probe_sees_them(self):
        running = job(10, cores=2, state="running", node="lab", cpus=(0, 1))

        result = plan(snapshot([computer("lab", cores=4)], [running, job(1, cores=3)]))

        self.assertEqual(result.actions, [])

    def test_reserve_is_subtracted_from_the_probe(self):
        node = computer("lab", cores=4, reserve=Resources(cores=1, mem=0))

        result = plan(snapshot([node], [job(1, cores=4)]))

        self.assertEqual(result.actions, [])


class RequestCheckTest(unittest.TestCase):
    """A job must fit inside one node; the sum of nodes never counts."""

    def test_request_larger_than_every_node_is_refused_even_if_the_sum_fits(self):
        nodes = [computer("lab", cores=4), computer("casa", cores=6), computer("extra", cores=20)]

        refusal = check_request(nodes, Resources(cores=25, mem=1 * GiB))

        self.assertIsNotNone(refusal)
        self.assertIn("extra", refusal)

    def test_request_that_fits_one_node_is_accepted(self):
        nodes = [computer("lab", cores=4), computer("casa", cores=6)]

        self.assertIsNone(check_request(nodes, Resources(cores=6, mem=1 * GiB)))

    def test_job_whose_only_fitting_node_is_down_waits_instead_of_being_refused(self):
        nodes = [computer("lab", cores=2), computer("casa", cores=8, state="unreachable")]

        self.assertIsNone(check_request(nodes, Resources(cores=6, mem=1 * GiB)))
        self.assertEqual(plan(snapshot(nodes, [job(1, cores=6)])).actions, [])


class NodeChoiceTest(unittest.TestCase):
    def test_fastest_node_per_core_wins(self):
        nodes = [computer("lab", speed=1.0), computer("casa", speed=1.5)]

        result = plan(snapshot(nodes, [job(1)]))

        self.assertEqual([action.node for action in result.actions], ["casa"])

    def test_speed_tie_goes_to_the_node_left_with_less(self):
        nodes = [computer("big", cores=8), computer("small", cores=4)]

        result = plan(snapshot(nodes, [job(1, cores=3)]))

        self.assertEqual([action.node for action in result.actions], ["small"])

    def test_on_restricts_the_candidates(self):
        nodes = [computer("lab", speed=1.0), computer("casa", speed=1.5)]

        result = plan(snapshot(nodes, [job(1, on=("lab",))]))

        self.assertEqual([action.node for action in result.actions], ["lab"])

    def test_drained_and_outdated_nodes_receive_nothing(self):
        nodes = [computer("lab", state="drained"), computer("casa", state="outdated")]

        self.assertEqual(plan(snapshot(nodes, [job(1)])).actions, [])

    def test_refusal_only_considers_the_on_candidates(self):
        nodes = [computer("lab", cores=4), computer("casa", cores=8)]

        refusal = check_request(nodes, Resources(cores=6, mem=1 * GiB), on=("lab",))

        self.assertIn("lab", refusal)


SMT_4x2 = ((0, 4), (1, 5), (2, 6), (3, 7))  # 4 physical cores, 2 logical CPUs each


class CpuAssignmentTest(unittest.TestCase):
    def test_job_gets_whole_physical_cores(self):
        node = computer("casa", cores=8, topology=SMT_4x2)

        result = plan(snapshot([node], [job(1, cores=2)]))

        self.assertEqual(result.actions, [Start(job=1, node="casa", cpus=(0, 4))])

    def test_two_jobs_never_share_a_physical_core(self):
        node = computer("casa", cores=8, topology=SMT_4x2)

        result = plan(snapshot([node], [job(1, cores=1), job(2, cores=1)]))

        self.assertEqual(
            result.actions,
            [Start(job=1, node="casa", cpus=(0,)), Start(job=2, node="casa", cpus=(1,))],
        )

    def test_nomultithread_uses_one_cpu_per_core_and_holds_the_siblings(self):
        node = computer("casa", cores=8, topology=SMT_4x2)
        jobs = [
            job(1, cores=2, hint="nomultithread"),
            job(2, cores=4),
            job(3, cores=1),
        ]

        result = plan(snapshot([node], jobs))

        self.assertEqual(
            result.actions,
            [Start(job=1, node="casa", cpus=(0, 1)), Start(job=2, node="casa", cpus=(2, 6, 3, 7))],
        )

    def test_node_default_hint_applies_when_the_job_gives_none(self):
        node = computer("casa", cores=8, topology=SMT_4x2, default_hint="nomultithread")

        result = plan(snapshot([node], [job(1, cores=2)]))

        self.assertEqual(result.actions, [Start(job=1, node="casa", cpus=(0, 1))])

    def test_nomultithread_request_beyond_the_physical_cores_is_refused(self):
        node = computer("casa", cores=8, topology=SMT_4x2)
        request = Resources(cores=5, mem=1 * GiB)

        self.assertIsNone(check_request([node], request))
        self.assertIsNotNone(check_request([node], request, hint="nomultithread"))


class QueueOrderTest(unittest.TestCase):
    def test_small_job_overtakes_a_large_one_that_does_not_fit_now(self):
        node = computer("lab", cores=8, probe_free=Resources(cores=4, mem=8 * GiB))
        jobs = [job(1, cores=6), job(2, cores=2)]

        result = plan(snapshot([node], jobs))

        self.assertEqual([action.job for action in result.actions], [2])

    def test_jobs_are_considered_in_submission_order(self):
        jobs = [job(2, cores=3, submitted_at=20.0), job(1, cores=3, submitted_at=10.0)]

        result = plan(snapshot([computer("lab", cores=4)], jobs))

        self.assertEqual([action.job for action in result.actions], [1])


HOUR = 3600.0


class StarvationTest(unittest.TestCase):
    """Section 7.2: after T, the oldest waiting job reserves a node for itself."""

    def setUp(self):
        self.running = job(10, cores=3, state="running", node="lab", cpus=(0, 1, 2), started_at=0.0, time_limit=10 * HOUR)
        self.big = job(1, cores=4, submitted_at=0.0)
        self.small = job(2, cores=1, submitted_at=1.0)

    def test_before_t_small_jobs_keep_filling(self):
        result = plan(snapshot([computer("lab", cores=4)], [self.running, self.big, self.small], now=0.5 * HOUR))

        self.assertEqual([action.job for action in result.actions], [2])

    def test_after_t_the_starving_job_reserves_its_node(self):
        result = plan(snapshot([computer("lab", cores=4)], [self.running, self.big, self.small], now=2 * HOUR))

        self.assertEqual(result.actions, [])

    def test_reserved_node_is_the_one_expected_to_free_up_first(self):
        early = job(11, cores=3, state="running", node="casa", cpus=(0, 1, 2), started_at=0.0, time_limit=3 * HOUR)
        nodes = [computer("lab", cores=4), computer("casa", cores=4)]

        result = plan(snapshot(nodes, [self.running, early, self.big, self.small], now=2 * HOUR))

        self.assertEqual(result.actions, [Start(job=2, node="lab", cpus=(3,))])


class SlurmTest(unittest.TestCase):
    def test_job_is_submitted_when_a_slurm_machine_fits_it_now(self):
        node = cluster("cluster", [("apollo01", 0, 0, 256, 512 * GiB), ("apollo05", 64, 100 * GiB, 256, 512 * GiB)])

        result = plan(snapshot([node], [job(1, cores=32, mem=32 * GiB)]))

        self.assertEqual(result.actions, [SubmitToSlurm(job=1, node="cluster")])

    BUSY = [("apollo05", 0, 0, 256, 512 * GiB)]

    def test_busy_cluster_still_takes_up_to_k_pending_jobs_when_nothing_else_can_start(self):
        node = cluster("cluster", self.BUSY, max_pending=2)
        jobs = [job(1, cores=32), job(2, cores=32), job(3, cores=32)]

        result = plan(snapshot([node], jobs))

        self.assertEqual(result.actions, [SubmitToSlurm(job=1, node="cluster"), SubmitToSlurm(job=2, node="cluster")])

    def test_jobs_already_pending_in_slurm_count_toward_k(self):
        node = cluster("cluster", self.BUSY, max_pending=2)
        queued = [job(10 + i, cores=32, state="slurm_queued", node="cluster") for i in range(2)]

        result = plan(snapshot([node], queued + [job(1, cores=32)]))

        self.assertEqual(result.actions, [])

    def test_pending_allowance_is_not_used_when_a_computer_can_start_now(self):
        nodes = [cluster("cluster", self.BUSY, speed=2.0), computer("lab", cores=4)]

        result = plan(snapshot(nodes, [job(1, cores=2)]))

        self.assertEqual([action.node for action in result.actions], ["lab"])

    def test_registered_capacity_counts_jobs_pending_in_slurm(self):
        machines = [("apollo05", 256, 512 * GiB, 256, 512 * GiB)]
        node = cluster("cluster", machines, capacity=Resources(cores=64, mem=512 * GiB))
        queued = job(10, cores=64, state="slurm_queued", node="cluster")

        result = plan(snapshot([node], [queued, job(1, cores=1)]))

        self.assertEqual(result.actions, [])

    def test_per_job_core_limit_of_the_partition_refuses_larger_requests(self):
        node = cluster("cluster", [("apollo05", 256, 512 * GiB, 256, 512 * GiB)], max_cores_per_job=20)

        self.assertIsNotNone(check_request([node], Resources(cores=25, mem=1 * GiB)))
        self.assertIsNone(check_request([node], Resources(cores=20, mem=1 * GiB)))

    def test_excluded_slurm_machines_are_not_counted_as_free(self):
        machines = [("apollo01", 256, 512 * GiB, 256, 512 * GiB), ("apollo05", 0, 0, 256, 512 * GiB)]
        node = cluster("cluster", machines, max_pending=0)

        excluded = plan(snapshot([node], [job(1, cores=32, slurm_exclude=("apollo01",))]))
        listed = plan(snapshot([node], [job(1, cores=32, slurm_nodelist=("apollo01",))]))

        self.assertEqual(excluded.actions, [])
        self.assertEqual(listed.actions, [SubmitToSlurm(job=1, node="cluster")])

    def test_jobs_submitted_in_the_same_cycle_use_up_the_idle_cores(self):
        node = cluster("cluster", [("apollo05", 64, 512 * GiB, 256, 512 * GiB)], max_pending=0)

        result = plan(snapshot([node], [job(1, cores=48), job(2, cores=48)]))

        self.assertEqual(result.actions, [SubmitToSlurm(job=1, node="cluster")])


class ReasonTest(unittest.TestCase):
    """Every waiting job gets a reason, one line per node, from the same rules that decided."""

    def test_pending_job_explains_each_node(self):
        nodes = [
            computer("lab", cores=8, probe_free=Resources(cores=4, mem=8 * GiB)),
            computer("casa", cores=8, state="unreachable"),
            computer("other", cores=8),
        ]

        reason = plan(snapshot(nodes, [job(1, cores=6, on=("lab", "casa"))])).reasons[1]

        self.assertIn("pending", reason.summary)
        lab, casa, other = (next(d for d in reason.details if d.startswith(name)) for name in ("lab", "casa", "other"))
        self.assertIn("cores", lab)
        self.assertIn("outside fila", lab)
        self.assertIn("unreachable", casa)
        self.assertIn("--on", other)

    def test_waiting_and_blocked_jobs_name_the_dependency(self):
        jobs = [
            job(1, state="running", node="lab", cpus=(0,)),
            job(2, state="failed"),
            job(3, state="waiting_dependency", after=(1,)),
            job(4, state="waiting_dependency", after=(2,)),
        ]

        reasons = plan(snapshot([computer("lab")], jobs)).reasons

        self.assertIn("1 (running)", reasons[3].summary)
        self.assertIn("blocked", reasons[4].summary)
        self.assertIn("2", reasons[4].summary)

    def test_other_jobs_see_the_node_reserved_against_starvation(self):
        running = job(10, cores=3, state="running", node="lab", cpus=(0, 1, 2), started_at=0.0, time_limit=10 * HOUR)
        jobs = [running, job(1, cores=4, submitted_at=0.0), job(2, cores=1, submitted_at=1.0)]

        reasons = plan(snapshot([computer("lab", cores=4)], jobs, now=2 * HOUR)).reasons

        self.assertIn("reserved", reasons[1].summary)
        self.assertIn("reserved for job 1", reasons[2].details[0])

    def test_busy_cluster_reports_the_pending_allowance(self):
        node = cluster("cluster", [("apollo05", 0, 0, 256, 512 * GiB)], max_pending=1)
        queued = job(10, cores=32, state="slurm_queued", node="cluster")

        reason = plan(snapshot([node], [queued, job(1, cores=32)])).reasons[1]

        self.assertIn("1 of 1", reason.details[0])

    def test_running_and_started_jobs_have_no_reason(self):
        jobs = [job(1, state="running", node="lab", cpus=(0,)), job(2)]

        self.assertEqual(plan(snapshot([computer("lab")], jobs)).reasons, {})


class DependencyTest(unittest.TestCase):
    def test_dependent_waits_even_with_free_resources(self):
        jobs = [job(1, cores=1), job(2, cores=1, state="waiting_dependency", after=(1,))]

        result = plan(snapshot([computer("lab", cores=4)], jobs))

        self.assertEqual([action.job for action in result.actions], [1])

    def test_dependent_starts_once_every_dependency_completed(self):
        jobs = [
            job(1, state="completed"),
            job(2, state="completed"),
            job(3, state="waiting_dependency", after=(1, 2)),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [Start(job=3, node="lab", cpus=(0,))])

    def test_dependent_with_dependencies_done_but_no_room_becomes_pending(self):
        node = computer("lab", cores=1, probe_free=Resources(cores=0, mem=8 * GiB))
        jobs = [job(1, state="completed"), job(2, state="waiting_dependency", after=(1,))]

        result = plan(snapshot([node], jobs))

        self.assertEqual(result.actions, [Release(job=2)])

    def test_waiting_for_starvation_starts_counting_at_release(self):
        running = job(10, cores=3, state="running", node="lab", cpus=(0, 1, 2), started_at=0.0, time_limit=10 * HOUR)
        jobs = [
            running,
            job(1, state="completed"),
            job(2, cores=4, state="waiting_dependency", after=(1,), submitted_at=0.0),
            job(3, cores=1, submitted_at=1.0),
        ]

        result = plan(snapshot([computer("lab", cores=4)], jobs, now=5 * HOUR))

        self.assertCountEqual(result.actions, [Release(job=2), Start(job=3, node="lab", cpus=(3,))])

    def test_dependent_keeps_waiting_while_any_dependency_is_unfinished(self):
        jobs = [
            job(1, state="completed"),
            job(2, state="running", node="lab", cpus=(0,)),
            job(3, state="waiting_dependency", after=(1, 2)),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [])

    def test_failed_or_cancelled_dependency_blocks_the_dependent(self):
        for final in ("failed", "cancelled"):
            with self.subTest(final=final):
                jobs = [job(1, state=final), job(2, state="waiting_dependency", after=(1,))]

                result = plan(snapshot([computer("lab")], jobs))

                self.assertEqual(result.actions, [Block(job=2, cause=1)])

    def test_block_propagates_down_the_chain_in_one_cycle(self):
        jobs = [
            job(1, state="failed"),
            job(2, state="waiting_dependency", after=(1,)),
            job(3, state="waiting_dependency", after=(2,)),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [Block(job=2, cause=1), Block(job=3, cause=2)])

    def test_even_if_failed_dependent_runs_after_any_final_state(self):
        jobs = [
            job(1, state="completed"),
            job(2, state="failed"),
            job(3, state="cancelled"),
            job(4, state="waiting_dependency", after=(1, 2, 3), even_if_failed=True),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [Start(job=4, node="lab", cpus=(0,))])

    def test_retried_cause_unblocks_the_chain(self):
        jobs = [
            job(1, state="running", node="lab", cpus=(0,)),
            job(2, state="blocked", after=(1,)),
            job(3, state="blocked", after=(2,)),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [Unblock(job=2), Unblock(job=3)])

    def test_job_stays_blocked_while_another_dependency_is_unsuccessful(self):
        jobs = [
            job(1, state="running", node="lab", cpus=(0,)),
            job(2, state="failed"),
            job(3, state="blocked", after=(1, 2)),
        ]

        result = plan(snapshot([computer("lab")], jobs))

        self.assertEqual(result.actions, [])


if __name__ == "__main__":
    unittest.main()
