import io
import json
import os
import shutil
import sys
import tempfile
import unittest

from fila.cli import main
from fila.scheduler import Node, Plan, Resources, Start, plan
from fila.store import Store

GiB = 1024**3


def computer(name, cores=4, mem=8 * GiB, probe_free=None, **fields):
    resources = Resources(cores=cores, mem=mem)
    return Node(name=name, capacity=resources, probe_free=probe_free or resources, **fields)


class CliCase(unittest.TestCase):
    """Runs the real command line against a fresh home directory."""

    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-test-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self.store = Store(os.path.join(self.home, "fila.db"))
        self.store.put_node(computer("lab", cores=4))
        self.now = 1000.0

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err, clock=lambda: self.now)
        return code, out.getvalue(), err.getvalue()

    def fila_json(self, subcommand, *args):
        code, out, err = self.fila(subcommand, "--json", *args)
        self.assertEqual(err, "", err)
        return code, json.loads(out)


class SubmitTest(CliCase):
    def test_submitted_job_is_listed_as_pending(self):
        code, submitted = self.fila_json("submit", "--cores", "2", "--mem", "1G", "--", "echo", "hi")

        self.assertEqual(code, 0)
        code, jobs = self.fila_json("status")
        self.assertEqual(code, 0)
        self.assertEqual([(j["id"], j["state"], j["command"]) for j in jobs], [(submitted["id"], "pending", "echo hi")])

    def test_missing_cores_or_memory_is_refused(self):
        for args in (["--cores", "2"], ["--mem", "1G"]):
            with self.subTest(args=args):
                code, _, err = self.fila("submit", *args, "--", "true")

                self.assertEqual(code, 4)
                self.assertIn("--cores and --mem", err)

    def test_request_larger_than_any_node_is_refused_with_the_largest_candidate(self):
        code, _, err = self.fila("submit", "--cores", "25", "--mem", "1G", "--", "true")

        self.assertEqual(code, 4)
        self.assertIn("lab", err)
        self.assertEqual(self.store.jobs(), [])

    def test_usage_errors_exit_with_5(self):
        cases = [
            ["submit", "--cores", "2", "--mem", "lots", "--", "true"],
            ["submit", "--cores", "2", "--mem", "1G", "--on", "nowhere", "--", "true"],
            ["submit", "--cores", "2", "--mem", "1G", "--after", "99", "--", "true"],
            ["submit", "--cores", "2", "--mem", "1G", "--time", "soon", "--", "true"],
            ["submit", "--cores", "2", "--mem", "1G"],
            ["status", "--bogus"],
            ["nonsense"],
        ]
        for args in cases:
            with self.subTest(args=args):
                self.assertEqual(self.fila(*args)[0], 5)

    def test_job_with_dependencies_waits_for_them(self):
        _, first = self.fila_json("submit", "--cores", "1", "--mem", "1G", "--", "true")
        _, second = self.fila_json("submit", "--cores", "1", "--mem", "1G", "--after", str(first["id"]), "--", "true")

        _, jobs = self.fila_json("status")

        self.assertEqual({j["id"]: j["state"] for j in jobs}, {first["id"]: "pending", second["id"]: "waiting_dependency"})

    def test_submit_options_are_kept_for_the_scheduler(self):
        self.store.put_node(computer("casa", cores=8))
        self.fila(
            "submit", "--cores", "2", "--mem", "512M", "--time", "2h", "--on", "lab,casa",
            "--hint", "nomultithread", "--", "true",
        )

        job = self.store.jobs()[0]

        self.assertEqual(job.request, Resources(cores=2, mem=512 * 1024**2))
        self.assertEqual(job.spec["time_limit"], 7200.0)
        self.assertEqual(job.spec["on"], ["lab", "casa"])
        self.assertEqual(job.spec["hint"], "nomultithread")


class JobCase(CliCase):
    def submit(self, *args):
        code, out = self.fila_json("submit", "--cores", "1", "--mem", "1G", *args, "--", "true")
        self.assertEqual(code, 0)
        return out["id"]

    def start(self, job_id, node="lab", cpus=(0,)):
        self.store.apply(Plan([Start(job=job_id, node=node, cpus=cpus)], {}), now=self.now)

    def states(self):
        return {job.id: job.state for job in self.store.jobs(include_final=True)}


class CancelTest(JobCase):
    def test_waiting_job_is_cancelled_at_once(self):
        job_id = self.submit()

        code, _, _ = self.fila("cancel", str(job_id))

        self.assertEqual(code, 0)
        self.assertEqual(self.states()[job_id], "cancelled")

    def test_running_job_gets_a_cancellation_request_for_the_server(self):
        job_id = self.submit()
        self.start(job_id)

        code, result = self.fila_json("cancel", str(job_id))

        self.assertEqual(code, 0)
        self.assertEqual(result["requested"], [job_id])
        self.assertEqual(self.states()[job_id], "running")
        self.assertTrue(self.store.jobs()[0].cancel_requested)

    def test_cascade_cancels_every_unfinished_dependent(self):
        first = self.submit()
        second = self.submit("--after", str(first))
        third = self.submit("--after", str(second))
        other = self.submit()

        self.fila("cancel", str(first), "--cascade")

        self.assertEqual(
            self.states(), {first: "cancelled", second: "cancelled", third: "cancelled", other: "pending"}
        )

    def test_finished_job_cannot_be_cancelled(self):
        job_id = self.submit()
        self.store.finish(job_id, "completed", now=self.now, exit_code=0)

        self.assertEqual(self.fila("cancel", str(job_id))[0], 4)

    def test_unknown_job_is_a_usage_error(self):
        self.assertEqual(self.fila("cancel", "99")[0], 5)

    def test_scheduler_decision_made_before_the_cancel_is_dropped(self):
        job_id = self.submit()
        decision = plan(self.store.snapshot(now=self.now))
        self.fila("cancel", str(job_id))

        applied = self.store.apply(decision, now=self.now)

        self.assertEqual(applied, [])
        self.assertEqual(self.states()[job_id], "cancelled")


class RetryTest(JobCase):
    def test_failed_job_gets_a_new_attempt(self):
        job_id = self.submit()
        self.start(job_id)
        self.store.finish(job_id, "failed", now=self.now, exit_code=1, reason="exit_code")

        self.assertEqual(self.fila("retry", str(job_id))[0], 0)
        self.start(job_id, cpus=(1,))

        job = self.store.jobs()[0]
        self.assertEqual(job.state, "running")
        self.assertEqual([(a["number"], a["state"]) for a in job.attempts], [(1, "failed"), (2, "running")])

    def test_retry_on_another_node_abandons_the_unreachable_attempt(self):
        self.store.put_node(computer("casa"))
        job_id = self.submit()
        self.start(job_id)
        self.store.mark_unreachable("lab", now=self.now)

        self.assertEqual(self.fila("retry", str(job_id), "--on", "casa")[0], 0)

        job = self.store.jobs()[0]
        self.assertEqual((job.state, job.spec["on"]), ("pending", ["casa"]))
        self.assertEqual(job.attempts[0]["state"], "abandoned")

    def test_retry_is_refused_for_jobs_that_did_not_end_badly(self):
        pending = self.submit()
        running = self.submit()
        self.start(running)

        self.assertEqual(self.fila("retry", str(pending))[0], 4)
        self.assertEqual(self.fila("retry", str(running))[0], 4)

    def test_retry_of_a_blocked_job_points_to_its_cause(self):
        cause = self.submit()
        self.store.finish(cause, "failed", now=self.now, exit_code=1, reason="exit_code")
        blocked = self.submit("--after", str(cause))
        self.store.apply(plan(self.store.snapshot(now=self.now)), now=self.now)

        code, _, err = self.fila("retry", str(blocked))

        self.assertEqual(code, 4)
        self.assertIn(f"retry {cause}", err)


class ResultTest(JobCase):
    def test_result_shows_the_attempts_and_exits_by_outcome(self):
        job_id = self.submit()
        self.start(job_id)
        self.store.finish(job_id, "failed", now=self.now, exit_code=3, reason="exit_code")

        code, result = self.fila_json("result", str(job_id))

        self.assertEqual(code, 1)
        self.assertEqual((result["state"], result["reason"]), ("failed", "exit_code"))
        self.assertEqual([(a["number"], a["node"], a["exit_code"]) for a in result["attempts"]], [(1, "lab", 3)])

    def test_completed_job_result_exits_0(self):
        job_id = self.submit()
        self.store.finish(job_id, "completed", now=self.now, exit_code=0)

        self.assertEqual(self.fila("result", str(job_id))[0], 0)


class ExplainTest(JobCase):
    def test_explain_shows_the_scheduler_reason(self):
        job_id = self.submit()
        self.store.put_node(computer("lab", cores=4, probe_free=Resources(cores=0, mem=8 * GiB)))
        self.store.apply(plan(self.store.snapshot(now=self.now)), now=self.now)

        code, explained = self.fila_json("explain", str(job_id))

        self.assertEqual(code, 0)
        self.assertIn("pending", explained["summary"])
        self.assertTrue(any("outside fila" in line for line in explained["details"]))

    def test_explain_before_the_server_saw_the_job(self):
        job_id = self.submit()

        _, explained = self.fila_json("explain", str(job_id))

        self.assertIn("not yet", explained["summary"])

    def test_explain_a_running_job_names_its_node(self):
        job_id = self.submit()
        self.start(job_id)

        _, explained = self.fila_json("explain", str(job_id))

        self.assertIn("running on lab", explained["summary"])


class WaitTest(JobCase):
    def setUp(self):
        super().setUp()
        self.during_wait = []

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()

        def sleep(seconds):
            self.now += seconds
            if self.during_wait:
                self.during_wait.pop(0)()

        code = main(list(args), home=self.home, stdout=out, stderr=err, clock=lambda: self.now, sleep=sleep)
        return code, out.getvalue(), err.getvalue()

    def test_wait_returns_0_when_every_job_completes(self):
        first, second = self.submit(), self.submit()
        self.during_wait = [
            lambda: self.store.finish(first, "completed", now=self.now, exit_code=0),
            lambda: self.store.finish(second, "completed", now=self.now, exit_code=0),
        ]

        code, result = self.fila_json("wait", str(first), str(second))

        self.assertEqual(code, 0)
        self.assertEqual({j["id"]: j["state"] for j in result}, {first: "completed", second: "completed"})

    def test_wait_returns_1_when_a_job_ends_without_success(self):
        first, second = self.submit(), self.submit()
        self.during_wait = [
            lambda: self.store.finish(first, "completed", now=self.now, exit_code=0),
            lambda: self.store.finish(second, "failed", now=self.now, exit_code=1, reason="exit_code"),
        ]

        self.assertEqual(self.fila("wait", str(first), str(second))[0], 1)

    def test_blocked_job_counts_as_finished_for_wait(self):
        cause = self.submit()
        blocked = self.submit("--after", str(cause))
        self.during_wait = [
            lambda: self.store.finish(cause, "failed", now=self.now, exit_code=1, reason="exit_code"),
            lambda: self.store.apply(plan(self.store.snapshot(now=self.now)), now=self.now),
        ]

        self.assertEqual(self.fila("wait", str(blocked))[0], 1)

    def test_wait_times_out_with_3(self):
        job_id = self.submit()

        code, result = self.fila_json("wait", str(job_id), "--timeout", "10s")

        self.assertEqual(code, 3)
        self.assertEqual(result[0]["state"], "pending")
        self.assertGreaterEqual(self.now, 1010.0)


class DryRunTest(JobCase):
    def test_dry_run_shows_where_the_job_would_start_without_storing_it(self):
        code, result = self.fila_json("submit", "--cores", "2", "--mem", "1G", "--dry-run", "--", "true")

        self.assertEqual(code, 0)
        self.assertEqual((result["would"], result["node"], result["cpus"]), ("start", "lab", [0, 1]))
        self.assertEqual(self.store.jobs(), [])

    def test_dry_run_explains_why_the_job_would_wait(self):
        self.start(self.submit("--cores", "1"), cpus=(0, 1, 2, 3))

        _, result = self.fila_json("submit", "--cores", "2", "--mem", "1G", "--dry-run", "--", "true")

        self.assertEqual(result["would"], "wait")
        self.assertIn("pending", result["summary"])


class ClientModeTest(CliCase):
    """``client.toml`` sends every command to the coordinator over SSH (a fake one here)."""

    def setUp(self):
        super().setUp()  # self.home is the coordinator, with node "lab"
        self.client_home = tempfile.mkdtemp(prefix="fila-client-")
        self.addCleanup(shutil.rmtree, self.client_home, ignore_errors=True)
        python = sys.executable.replace("\\", "/")
        fake_ssh = os.path.join(os.path.dirname(__file__), "fake_ssh.py")
        with open(os.path.join(self.client_home, "client.toml"), "w") as handle:
            handle.write(
                f'coordinator = "italo@lab"\n'
                f'remote_command = "{python} -m fila"\n'
                f"ssh = {json.dumps([sys.executable, fake_ssh])}\n"
            )
        src = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
        self.environ = dict(os.environ, FAKE_SSH_HOME=self.home, PYTHONPATH=src)

    def client(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.client_home, stdout=out, stderr=err, environ=self.environ)
        return code, out.getvalue(), err.getvalue()

    def test_commands_run_on_the_coordinator(self):
        code, out, err = self.client("submit", "--json", "--cores", "1", "--mem", "1G", "--", "echo 'it''s quoted' && true")

        self.assertEqual(code, 0, err)
        job = self.store.job(json.loads(out)["id"])
        self.assertEqual(job.command, "echo 'it''s quoted' && true")
        self.assertEqual(self.client("status")[0], 0)
        self.assertIn(str(job.id), self.client("status")[1])

    def test_exit_codes_come_back(self):
        self.assertEqual(self.client("cancel", "999")[0], 5)

    def test_local_script_travels_with_the_job(self):
        script = os.path.join(self.client_home, "run.sh")
        with open(script, "w") as handle:
            handle.write("#!/bin/sh\necho from the client\n")

        code, out, err = self.client("submit", "--json", "--cores", "1", "--mem", "1G", "--script", script)

        self.assertEqual(code, 0, err)
        spec = self.store.job(json.loads(out)["id"]).spec
        self.assertEqual(spec["script"], {"name": "run.sh", "content": "#!/bin/sh\necho from the client\n"})

    def test_unreachable_coordinator_exits_with_6(self):
        self.environ["FAKE_SSH_FAIL"] = "1"

        code, _, err = self.client("status")

        self.assertEqual(code, 6)
        self.assertIn("coordinator", err)


class NodesTest(CliCase):
    SHELL = "cmd /c" if os.name == "nt" else "/bin/sh -c"

    def register(self, name="here", *extra):
        jobs_dir = os.path.join(self.home, "jobs-" + name)
        return self.fila("nodes", "register", name, "--access", "local", "--shell", self.SHELL, "--jobs-dir", jobs_dir, *extra)

    def test_local_node_is_validated_probed_and_listed(self):
        code, out, err = self.register("here", "--cores", "2", "--reserve", "cores=1,mem=1G")

        self.assertEqual(code, 0, err)
        self.assertIn("logical CPUs", out)
        code, out, _ = self.fila("nodes", "list", "--json")
        nodes = json.loads(out)
        here =next(node for node in nodes if node["name"] == "here")
        self.assertEqual((here["access"], here["state"], here["capacity"]["cores"]), ("local", "online", 2))

    def test_registration_problems(self):
        self.register("here")
        jobs_dir = os.path.join(self.home, "jobs")
        cases = [
            (5, self.register("here")),
            (5, self.fila("nodes", "register", "far", "--access", "ssh", "--shell", self.SHELL, "--jobs-dir", jobs_dir)),
            (5, self.fila("nodes", "register", "far", "--access", "pull", "--shell", self.SHELL, "--jobs-dir", jobs_dir)),
            (4, self.fila("nodes", "register", "bad", "--access", "local", "--shell", "no-such-shell -c", "--jobs-dir", jobs_dir)),
            (5, self.fila("nodes", "drain", "nowhere")),
        ]
        for expected, (code, _, err) in cases:
            with self.subTest(err=err):
                self.assertEqual(code, expected)

    def test_node_with_running_jobs_cannot_be_removed(self):
        self.register("here")
        _, submitted = self.fila_json("submit", "--cores", "1", "--mem", "1M", "--on", "here", "--", "true")
        self.store.apply(Plan([Start(job=submitted["id"], node="here", cpus=(0,))], {}), now=self.now)

        self.assertEqual(self.fila("nodes", "remove", "here")[0], 4)


if __name__ == "__main__":
    unittest.main()
