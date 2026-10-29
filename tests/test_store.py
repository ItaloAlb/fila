import os
import shutil
import tempfile
import unittest

from fila.scheduler import Block, Node, Plan, Reason, Release, Resources, Start, SubmitToSlurm, Unblock, plan
from fila.store import Store

GiB = 1024**3


def computer(name, cores=4, mem=8 * GiB):
    resources = Resources(cores=cores, mem=mem)
    return Node(name=name, capacity=resources, probe_free=resources)


class StoreCase(unittest.TestCase):
    def setUp(self):
        home = tempfile.mkdtemp(prefix="fila-test-")
        self.addCleanup(shutil.rmtree, home, ignore_errors=True)
        self.store = Store(os.path.join(home, "fila.db"))
        self.store.put_node(computer("lab"))

    def add(self, cores=1, now=0.0, **spec):
        return self.store.add_job("true", Resources(cores=cores, mem=GiB), spec, "user", now)

    def state(self, job_id):
        return next(job for job in self.store.jobs(include_final=True) if job.id == job_id).state


class SnapshotTest(StoreCase):
    def test_snapshot_carries_jobs_and_their_scheduling_options(self):
        job_id = self.add(cores=2, now=5.0, on=["lab"], hint="nomultithread", time_limit=60.0)

        snapshot = self.store.snapshot(now=10.0)

        (job,) = snapshot.jobs
        self.assertEqual(
            (job.id, job.state, job.request, job.on, job.hint, job.time_limit, job.submitted_at, job.pending_since),
            (job_id, "pending", Resources(cores=2, mem=GiB), ("lab",), "nomultithread", 60.0, 5.0, 5.0),
        )
        self.assertEqual([node.name for node in snapshot.nodes], ["lab"])
        self.assertEqual(snapshot.now, 10.0)

    def test_snapshot_includes_finished_dependencies_of_waiting_jobs_only(self):
        done = self.add()
        unrelated = self.add()
        self.store.finish(done, "completed", exit_code=0, now=1.0)
        self.store.finish(unrelated, "completed", exit_code=0, now=1.0)
        waiting = self.add(after=[done])

        ids = {job.id for job in self.store.snapshot(now=2.0).jobs}

        self.assertEqual(ids, {done, waiting})


class ApplyTest(StoreCase):
    def test_start_creates_a_running_attempt(self):
        job_id = self.add(cores=2)

        self.store.apply(Plan([Start(job=job_id, node="lab", cpus=(0, 1))], {}), now=3.0)

        (job,) = self.store.snapshot(now=4.0).jobs
        self.assertEqual((job.state, job.node, job.cpus, job.started_at), ("running", "lab", (0, 1), 3.0))

    def test_submit_to_slurm_creates_a_queued_attempt(self):
        job_id = self.add()

        self.store.apply(Plan([SubmitToSlurm(job=job_id, node="cluster")], {}), now=3.0)

        (job,) = self.store.snapshot(now=4.0).jobs
        self.assertEqual((job.state, job.node), ("slurm_queued", "cluster"))

    def test_dependency_transitions_change_the_state(self):
        failed = self.add()
        self.store.finish(failed, "failed", exit_code=1, reason="exit_code", now=1.0)
        blocked = self.add(after=[failed])
        unblocked = self.add(after=[failed])
        released = self.add(after=[failed])
        self.store.apply(Plan([Block(job=blocked, cause=failed), Block(job=unblocked, cause=failed)], {}), now=2.0)

        self.store.apply(Plan([Unblock(job=unblocked), Release(job=released)], {}), now=3.0)

        self.assertEqual(
            (self.state(blocked), self.state(unblocked), self.state(released)),
            ("blocked", "waiting_dependency", "pending"),
        )
        released_job = next(job for job in self.store.snapshot(now=4.0).jobs if job.id == released)
        self.assertEqual(released_job.pending_since, 3.0)

    def test_reasons_are_replaced_on_every_apply(self):
        first, second = self.add(), self.add()
        self.store.apply(Plan([], {first: Reason("pending: x", ("lab: y",))}), now=1.0)

        self.store.apply(Plan([], {second: Reason("pending: z")}), now=2.0)

        self.assertIsNone(self.store.reason(first))
        self.assertEqual(self.store.reason(second), Reason("pending: z"))

    def test_finish_closes_the_current_attempt(self):
        job_id = self.add()
        self.store.apply(Plan([Start(job=job_id, node="lab", cpus=(0,))], {}), now=1.0)

        self.store.finish(job_id, "failed", exit_code=137, reason="out_of_memory", now=9.0)

        job = self.store.jobs(include_final=True)[0]
        self.assertEqual((job.state, job.reason, job.finished_at), ("failed", "out_of_memory", 9.0))
        self.assertEqual(
            [(a["number"], a["state"], a["exit_code"], a["finished_at"]) for a in job.attempts],
            [(1, "failed", 137, 9.0)],
        )

    def test_plan_round_trip_through_the_store(self):
        first = self.add(cores=4)
        second = self.add(cores=4)

        snapshot = self.store.snapshot(now=1.0)
        self.store.apply(plan(snapshot), now=1.0)

        self.assertEqual((self.state(first), self.state(second)), ("running", "pending"))
        self.assertIn("pending", self.store.reason(second).summary)


class RetryAfterTest(StoreCase):
    def running_job(self, **spec):
        job_id = self.add(**spec)
        self.store.apply(Plan([Start(job=job_id, node="lab", cpus=(0,))], {}), now=0.0)
        self.store.mark_unreachable("lab", now=100.0)
        return job_id

    def test_job_is_retried_once_its_node_stayed_unreachable_long_enough(self):
        job_id = self.running_job(retry_after=3600.0)

        self.assertEqual(self.store.retry_overdue(now=100.0 + 3599.0), [])
        self.assertEqual(self.store.retry_overdue(now=100.0 + 3600.0), [job_id])

        job = self.store.job(job_id)
        self.assertEqual(job.state, "pending")
        self.assertEqual(job.attempts[0]["state"], "abandoned")

    def test_without_retry_after_nothing_is_retried(self):
        self.running_job()

        self.assertEqual(self.store.retry_overdue(now=10**9), [])


if __name__ == "__main__":
    unittest.main()
