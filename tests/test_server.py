"""The server end to end on this machine: registry, probe, plan, detached
supervisors, reconciliation. Commands run with Python as the shell."""

import io
import json
import os
import shutil
import signal
import sys
import tempfile
import time
import unittest

from fila.cli import main
from fila.probe import Hardware, Reading
from fila.registry import NodeEntry, Registry
from fila.server import Server
from fila.supervisor import read_status

GiB = 1024**3


class FakeProbe:
    def __init__(self, cores=4, mem=8 * GiB, idle_seconds=3600.0):
        self.cores, self.mem, self.idle = cores, mem, idle_seconds

    def hardware(self):
        return Hardware(logical_cpus=self.cores, topology=tuple((cpu,) for cpu in range(self.cores)), total_mem=self.mem)

    def sample(self):
        return Reading(free_cores=self.cores, available_mem=self.mem, idle_seconds=self.idle)

    def disk_free(self, path):
        return 100 * GiB


class ServerCase(unittest.TestCase):
    probe = None  # None: the real machine

    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-srv-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self.entry = NodeEntry(
            name="here", type="computer", access="local", os=sys.platform, shell=[sys.executable, "-c"],
            jobs_dir=os.path.join(self.home, "jobs"), limits={"cores": 2, "mem": 1 * GiB},
        )
        self.register()
        probe = self.probe
        self.server = Server(self.home, probe_factory=None if probe is None else (lambda entry: probe))
        self.server.cycle()  # the first cycle probes the node

    def register(self):
        Registry(os.path.join(self.home, "nodes.toml")).save({}, {"here": self.entry})

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    def submit(self, code, *options):
        status, out, err = self.fila("submit", "--json", "--cores", "1", "--mem", "64M", *options, "--", code)
        self.assertEqual(status, 0, err)
        return json.loads(out)["id"]

    def result(self, job_id):
        return json.loads(self.fila("result", "--json", str(job_id))[1])

    def cycle_until(self, condition, timeout=30.0):
        deadline = time.time() + timeout
        while True:
            self.server.cycle()
            if condition():
                return
            if time.time() > deadline:
                self.fail("condition not reached in time")
            time.sleep(0.1)

    def state(self, job_id):
        return self.result(job_id)["state"]


class RunToEndTest(ServerCase):
    def test_submitted_job_runs_to_completion_and_keeps_its_output(self):
        job_id = self.submit("import os; print('hello from', os.environ['FILA_NODE'])")

        self.cycle_until(lambda: self.state(job_id) == "completed")

        result = self.result(job_id)
        self.assertEqual([(a["node"], a["exit_code"]) for a in result["attempts"]], [("here", 0)])
        code, out, _ = self.fila("log", str(job_id))
        self.assertEqual((code, out.strip()), (0, "hello from here"))

    def test_end_time_comes_from_the_diary_not_from_when_the_server_noticed(self):
        job_id = self.submit("print(1)")
        self.server.cycle()
        diary = os.path.join(self.entry.jobs_dir, str(job_id), "attempt-1")
        deadline = time.time() + 20
        while (read_status(diary) or {}).get("state") != "finished" and time.time() < deadline:
            time.sleep(0.05)
        time.sleep(1.0)

        self.cycle_until(lambda: self.state(job_id) == "completed")

        self.assertAlmostEqual(self.result(job_id)["finished_at"], read_status(diary)["finished_at"], places=3)

    def test_serving_server_notices_the_end_without_waiting_for_its_interval(self):
        import threading

        stop = threading.Event()
        thread = threading.Thread(target=self.server.serve, args=(stop.is_set,))
        thread.start()
        self.addCleanup(thread.join, 10)
        self.addCleanup(stop.set)
        started = time.time()

        job_id = self.submit("print(1)")
        while self.state(job_id) != "completed" and time.time() - started < 20:
            time.sleep(0.1)

        self.assertEqual(self.state(job_id), "completed")
        self.assertLess(time.time() - started, 15)  # the interval is 30 s

    def test_failure_is_recorded_and_blocks_dependents(self):
        failing = self.submit("import sys; sys.exit(2)")
        dependent = self.submit("print('never')", "--after", str(failing))

        self.cycle_until(lambda: self.state(dependent) == "blocked")

        self.assertEqual((self.state(failing), self.result(failing)["reason"]), ("failed", "exit_code"))
        self.assertEqual(self.result(failing)["attempts"][0]["exit_code"], 2)

    def test_cancel_reaches_a_running_job(self):
        job_id = self.submit("import time; time.sleep(60)")
        self.cycle_until(lambda: self.attempt_state(job_id) == "running")

        self.fila("cancel", str(job_id))
        self.cycle_until(lambda: self.state(job_id) == "cancelled")

    def test_supervisor_that_died_makes_the_job_lost(self):
        job_id = self.submit("import time; time.sleep(60)")
        self.cycle_until(lambda: self.attempt_state(job_id) == "running")

        os.kill(self.attempt_status(job_id)["supervisor_pid"], signal.SIGTERM)
        self.cycle_until(lambda: self.state(job_id) == "failed")

        self.assertEqual(self.result(job_id)["reason"], "lost")

    def attempt_status(self, job_id):
        return read_status(os.path.join(self.entry.jobs_dir, str(job_id), "attempt-1")) or {}

    def attempt_state(self, job_id):
        return self.attempt_status(job_id).get("state")


class RunTest(ServerCase):
    """``fila run``: the terminal of each node, through the server."""

    def run_and_cycle(self, *args):
        """``fila run --wait`` in a thread while this thread plays the server."""
        import threading

        result = {}

        def target():
            result["out"] = self.fila("run", "--json", "--wait", *args)

        thread = threading.Thread(target=target)
        thread.start()
        deadline = time.time() + 30
        while thread.is_alive() and time.time() < deadline:
            self.server.cycle()
            time.sleep(0.1)
        thread.join(5)
        code, out, err = result["out"]
        return code, json.loads(out) if out else None, err

    def test_run_executes_now_and_returns_the_output(self):
        code, results, err = self.run_and_cycle("--on", "here", "--", "print('from the terminal')")

        self.assertEqual(code, 0, err)
        (result,) = results
        self.assertEqual((result["node"], result["status"], result["exit_code"]), ("here", "completed", 0))
        self.assertIn("from the terminal", result["tail"])

    def test_failing_command_exits_1(self):
        code, results, _ = self.run_and_cycle("--on", "here", "--", "import sys; sys.exit(4)")

        self.assertEqual(code, 1)
        self.assertEqual((results[0]["status"], results[0]["exit_code"]), ("failed", 4))

    def test_unreachable_target_is_not_run_without_waiting(self):
        self.fila("nodes", "drain", "here")

        code, results, _ = self.run_and_cycle("--on", "all", "--", "print(1)")

        self.assertEqual(code, 2)
        self.assertEqual((results[0]["status"], results[0]["reason"]), ("not_run", "drained"))

    def test_run_time_limit_defaults_to_the_run_timeout(self):
        general, entries = Registry(os.path.join(self.home, "nodes.toml")).load()
        Registry(os.path.join(self.home, "nodes.toml")).save(dict(general, run_timeout=0.5), entries)

        code, results, _ = self.run_and_cycle("--on", "here", "--", "import time; time.sleep(30)")

        self.assertEqual((code, results[0]["reason"]), (1, "timeout"))

    def test_run_needs_a_live_server(self):
        os.remove(os.path.join(self.home, "server.json"))

        code, _, err = self.fila("run", "--on", "here", "--", "print(1)")

        self.assertEqual(code, 6)
        self.assertIn("server", err)


class CleanTest(ServerCase):
    def finished_job(self, code="open('result.txt', 'w').write('42')"):
        job_id = self.submit(code)
        self.cycle_until(lambda: self.state(job_id) == "completed")
        return job_id

    def job_dir(self, job_id):
        return os.path.join(self.entry.jobs_dir, str(job_id))

    def test_clean_removes_old_finished_job_directories_but_keeps_the_history(self):
        job_id = self.finished_job()

        code, out, err = self.fila("clean", "--older-than", "0s", "--json")

        self.assertEqual(code, 0, err)
        self.assertEqual(json.loads(out)["cleaned"], [job_id])
        self.assertFalse(os.path.exists(self.job_dir(job_id)))
        self.assertEqual(self.state(job_id), "completed")

    def test_recent_and_running_jobs_are_kept(self):
        done = self.finished_job()
        running = self.submit("import time; time.sleep(60)")
        self.cycle_until(lambda: self.state(running) == "running")

        self.fila("clean", "--older-than", "1d")
        self.fila("clean", "--older-than", "0s")

        self.assertFalse(os.path.exists(self.job_dir(done)))
        self.assertTrue(os.path.exists(self.job_dir(running)))
        self.fila("cancel", str(running))
        self.cycle_until(lambda: self.state(running) == "cancelled")

    def test_logs_only_keeps_what_the_job_wrote(self):
        job_id = self.finished_job()

        self.fila("clean", "--older-than", "0s", "--logs-only")

        self.assertTrue(os.path.exists(os.path.join(self.job_dir(job_id), "result.txt")))
        self.assertFalse(os.path.exists(os.path.join(self.job_dir(job_id), "attempt-1", "stdout.log")))

    def test_directories_given_with_dir_are_never_touched(self):
        outside = tempfile.mkdtemp(prefix="fila-dir-")
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        job_id = self.submit("open('mine.txt', 'w').write('x')", "--dir", outside)
        self.cycle_until(lambda: self.state(job_id) == "completed")

        self.fila("clean", "--older-than", "0s")

        self.assertTrue(os.path.exists(os.path.join(outside, "mine.txt")))


class SuspendTest(ServerCase):
    def test_manual_suspend_and_resume(self):
        job_id = self.submit("import time\nfor i in range(600): time.sleep(0.05)")
        self.cycle_until(lambda: self.attempt_state(job_id) == "running")

        self.assertEqual(self.fila("suspend", str(job_id))[0], 0)
        self.cycle_until(lambda: self.attempt_status(job_id).get("suspended"))

        self.assertEqual(self.fila("resume", str(job_id))[0], 0)
        self.cycle_until(lambda: not self.attempt_status(job_id).get("suspended"))
        self.fila("cancel", str(job_id))
        self.cycle_until(lambda: self.state(job_id) == "cancelled")

    def attempt_status(self, job_id):
        return read_status(os.path.join(self.entry.jobs_dir, str(job_id), "attempt-1")) or {}

    def attempt_state(self, job_id):
        return self.attempt_status(job_id).get("state")


class DrainTest(ServerCase):
    probe = FakeProbe()

    def test_drained_node_receives_no_new_job(self):
        self.fila("nodes", "drain", "here")
        job_id = self.submit("print(1)")

        self.server.cycle()

        self.assertEqual(self.state(job_id), "pending")
        self.assertIn("drained", self.fila("explain", str(job_id))[1])


class InUseTest(ServerCase):
    probe = FakeProbe(cores=4, idle_seconds=10.0)

    def test_in_use_reserve_applies_while_someone_uses_the_machine(self):
        self.entry.limits = {}
        self.entry.reserve_in_use = {"cores": 4}
        self.register()
        job_id = self.submit("print(1)")

        self.server.cycle()

        self.assertEqual(self.state(job_id), "pending")
        self.assertIn("kept as reserve", self.fila("explain", str(job_id))[1])


if __name__ == "__main__":
    unittest.main()
