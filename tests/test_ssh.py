"""ssh computers end to end. The "remote" node is this machine reached through
a fake ssh (bash with another HOME), so the agent really travels as a .pyz,
runs in isolation and answers in JSON."""

import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

from fila import agent
from fila.cli import main
from fila.registry import Registry
from fila.server import Server

FAKE_SSH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_remote_ssh.py")
PYTHON = sys.executable.replace("\\", "/")  # a shell word for bash on the node
sys.path.insert(0, os.path.dirname(FAKE_SSH))
from fake_remote_ssh import bash  # noqa: E402

SHELL = json.dumps([bash(), "-c"])


class SshCase(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-coord-")
        self.remote_home = tempfile.mkdtemp(prefix="fila-remote-")
        for path in (self.home, self.remote_home):
            self.addCleanup(shutil.rmtree, path, ignore_errors=True)
        environ = mock.patch.dict(os.environ, {"FAKE_REMOTE_HOME": self.remote_home})
        environ.start()
        self.addCleanup(environ.stop)
        Registry(os.path.join(self.home, "nodes.toml")).save({"ssh_command": [sys.executable, FAKE_SSH]}, {})
        self.server = Server(self.home)

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    def register(self, *extra):
        return self.fila(
            "nodes", "register", "far", "--access", "ssh", "--host", "far", "--python", PYTHON,
            "--shell", SHELL, "--jobs-dir", "~/jobs", "--cores", "2", *extra,
        )

    def submit(self, code, *options):
        status, out, err = self.fila("submit", "--json", "--cores", "1", "--mem", "64M", *options, "--", code)
        self.assertEqual(status, 0, err)
        return json.loads(out)["id"]

    def result(self, job_id):
        return json.loads(self.fila("result", "--json", str(job_id))[1])

    def cycle_until(self, condition, timeout=60.0):
        deadline = time.time() + timeout
        while True:
            self.server.cycle()
            if condition():
                return
            if time.time() > deadline:
                self.fail("condition not reached in time")
            time.sleep(0.2)


class RegisterTest(SshCase):
    def test_registration_copies_the_agent_and_probes_the_node(self):
        code, out, err = self.register()

        self.assertEqual(code, 0, err)
        self.assertIn("logical CPUs", out)
        self.assertTrue(os.path.exists(os.path.join(self.remote_home, ".fila", "fila.pyz")))
        entry = Registry(os.path.join(self.home, "nodes.toml")).load()[1]["far"]
        self.assertEqual((entry.access, entry.host, entry.speed_source), ("ssh", "far", "builtin"))

    def test_unreachable_host_is_refused(self):
        with mock.patch.dict(os.environ, {"FAKE_SSH_FAIL": "1"}):
            code, _, err = self.register()

        self.assertEqual(code, 4)
        self.assertIn("timed out", err)

    def test_missing_python_on_the_node_is_refused(self):
        code, _, err = self.fila(
            "nodes", "register", "far", "--access", "ssh", "--host", "far", "--python", "no-such-python3",
            "--shell", SHELL, "--jobs-dir", "~/jobs",
        )

        self.assertEqual(code, 4)
        self.assertIn("--python", err)


class RunOnSshNodeTest(SshCase):
    def setUp(self):
        super().setUp()
        code, _, err = self.register("--speed", "1.0")
        self.assertEqual(code, 0, err)

    def test_job_runs_on_the_node_and_its_log_comes_back(self):
        job_id = self.submit('echo "on $FILA_NODE $(pwd)"')

        self.cycle_until(lambda: self.result(job_id)["state"] == "completed")

        code, out, _ = self.fila("log", str(job_id))
        self.assertEqual(code, 0)
        self.assertTrue(out.startswith("on far"))
        self.assertIn(f"jobs/{job_id}", out)
        self.assertTrue(os.path.isdir(os.path.join(self.remote_home, "jobs", str(job_id), "attempt-1")))

    def test_lost_contact_keeps_the_job_and_reattaches_it(self):
        job_id = self.submit("sleep 4; echo done")
        self.cycle_until(lambda: self.result(job_id)["state"] == "running")

        with mock.patch.dict(os.environ, {"FAKE_SSH_FAIL": "1"}):
            self.server.cycle()
            self.assertEqual(self.result(job_id)["state"], "unreachable")
            self.assertIn("unreachable", self.fila("nodes", "list")[1])

        self.cycle_until(lambda: self.result(job_id)["state"] in ("running", "completed"))
        self.cycle_until(lambda: self.result(job_id)["state"] == "completed")

    def test_node_with_another_protocol_is_outdated_until_updated(self):
        with mock.patch.object(agent, "PROTOCOL", agent.PROTOCOL + 1):
            self.server.cycle()
            self.assertIn("outdated", self.fila("nodes", "list")[1])
            self.assertEqual(self.fila("nodes", "validate", "far")[0], 1)

        self.server.cycle()
        self.assertIn("online", self.fila("nodes", "list")[1])
        self.assertEqual(self.fila("nodes", "update", "far")[0], 0)
        self.assertEqual(self.fila("nodes", "validate")[0], 0)

    def test_benchmark_measures_the_speed(self):
        code, out, err = self.fila("nodes", "benchmark", "far", "--json")

        self.assertEqual(code, 0, err)
        (result,) = json.loads(out)
        self.assertGreater(result["speed"], 0)
        self.assertEqual(result["source"], "builtin")

    def test_run_on_all_reaches_the_ssh_node(self):
        import threading

        self.server.cycle()  # run needs a live server (its heartbeat)
        result = {}
        thread = threading.Thread(target=lambda: result.update(out=self.fila("run", "--json", "--wait", "--on", "all", "--", "echo terminal; uname -s")))
        thread.start()
        deadline = time.time() + 60
        while thread.is_alive() and time.time() < deadline:
            self.server.cycle()
            time.sleep(0.2)
        thread.join(5)

        code, out, err = result["out"]
        self.assertEqual(code, 0, err)
        (far,) = json.loads(out)
        self.assertEqual((far["node"], far["status"]), ("far", "completed"))
        self.assertIn("terminal", far["tail"])

    def test_selftest_checks_enforcement_on_the_node(self):
        code, out, err = self.fila("selftest", "--on", "far", "--json")

        (node,) = json.loads(out)
        failed = [check for check in node["checks"] if check["ok"] is False]
        self.assertEqual((code, failed), (0, []), err)
        self.assertEqual(
            [check["check"] for check in node["checks"]],
            ["shell", "start", "exit_code", "time_limit", "memory_limit", "cpu_affinity", "suspend_resume", "leftovers"],
        )

    def test_clean_reaches_the_node(self):
        job_id = self.submit("echo 1")
        self.cycle_until(lambda: self.result(job_id)["state"] == "completed")

        self.fila("clean", "--older-than", "0s")

        self.assertFalse(os.path.exists(os.path.join(self.remote_home, "jobs", str(job_id))))


if __name__ == "__main__":
    unittest.main()
