# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Pull nodes end to end: a worker opens the channel through a fake SSH that
behaves like sshd with the node's restricted key; the server reaches the node
only through that channel."""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from fila.channel import run_worker
from fila.cli import main
from fila.files import read_json
from fila.registry import Registry
from fila.server import Server
from fila.store import Store

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fake_remote_ssh import bash  # noqa: E402

FAKE_SSH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_coordinator_ssh.py")
KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHk9xBQm0cBfEwd7lP0Ye9G3n1k8p8Hc0r2mMx1vQkqY fila-desktop"


class PullCase(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-coord-")
        self.node_dir = tempfile.mkdtemp(prefix="fila-node-")
        for path in (self.home, self.node_dir):
            self.addCleanup(shutil.rmtree, path, ignore_errors=True)
        self.jobs_dir = os.path.join(self.node_dir, "jobs")
        self.keys = os.path.join(self.home, "authorized_keys")  # never the real ~/.ssh/authorized_keys
        environ = mock.patch.dict(os.environ, {"FAKE_COORDINATOR_HOME": self.home})
        environ.start()
        self.addCleanup(environ.stop)
        Registry(os.path.join(self.home, "nodes.toml")).save({}, {})
        self.server = Server(self.home)
        self.installs = []

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    def start_worker(self):
        stop = threading.Event()
        thread = threading.Thread(
            target=run_worker,
            args=("user@lab", "desktop", [sys.executable, FAKE_SSH], "fila"),
            kwargs=dict(stop=stop, home_dir=self.node_dir, log=io.StringIO(), on_install=lambda: self.installs.append(1)),
            daemon=True,
        )
        thread.start()
        self.addCleanup(thread.join, 10)
        self.addCleanup(stop.set)
        return stop, thread

    def register(self, wait="20"):
        return self.fila(
            "nodes", "register", "desktop", "--access", "pull", "--key", KEY, "--shell", json.dumps([bash(), "-c"]),
            "--jobs-dir", self.jobs_dir, "--cores", "2", "--authorized-keys", self.keys, "--wait", wait, "--json",
        )

    def submit(self, command):
        code, out, err = self.fila("submit", "--json", "--cores", "1", "--mem", "64M", "--on", "desktop", "--", command)
        self.assertEqual(code, 0, err)
        return json.loads(out)["id"]

    def state(self, job_id):
        return json.loads(self.fila("result", "--json", str(job_id))[1])["state"]

    def cycle_until(self, condition, timeout=60.0):
        deadline = time.time() + timeout
        while True:
            self.server.cycle()
            if condition():
                return
            if time.time() > deadline:
                self.fail("condition not reached in time")
            time.sleep(0.2)


class PairingTest(PullCase):
    def test_worker_install_prepares_a_key_the_agent_and_its_config(self):
        out, err = io.StringIO(), io.StringIO()
        code = main(["worker", "install", "--coordinator", "user@lab", "--node", "desktop", "--no-service",
                     "--dir", self.node_dir, "--json"], stdout=out, stderr=err)

        self.assertEqual(code, 0, err.getvalue())
        result = json.loads(out.getvalue())
        self.assertTrue(result["public_key"].startswith("ssh-ed25519 "))
        self.assertIn("--access pull --key", result["register"])
        for name in ("id_ed25519", "fila.pyz", "worker.toml"):
            self.assertTrue(os.path.exists(os.path.join(self.node_dir, name)), name)
        with open(os.path.join(self.node_dir, "worker.toml")) as handle:
            self.assertIn("StrictHostKeyChecking=accept-new", handle.read())

    def test_register_writes_a_restricted_key_and_validates_over_the_channel(self):
        self.start_worker()

        code, out, err = self.register()

        self.assertEqual(code, 0, err)
        self.assertTrue(json.loads(out)["connected"])
        with open(self.keys) as handle:
            line = handle.read().strip()
        self.assertTrue(line.startswith('restrict,command="'))
        self.assertIn("internal channel --node desktop", line)
        self.assertTrue(line.endswith(KEY))
        entry = Registry(os.path.join(self.home, "nodes.toml")).load()[1]["desktop"]
        self.assertEqual((entry.access, entry.speed_source), ("pull", "builtin"))

    def test_registering_again_replaces_the_key_instead_of_duplicating_it(self):
        self.register(wait="0")
        self.fila("nodes", "remove", "desktop")

        self.register(wait="0")

        with open(self.keys) as handle:
            self.assertEqual(len(handle.read().strip().splitlines()), 1)

    def test_node_that_connects_later_is_validated_then(self):
        code, out, _ = self.register(wait="0")
        self.assertEqual((code, json.loads(out)["connected"]), (0, False))

        self.start_worker()
        deadline = time.time() + 20
        while self.fila("nodes", "validate", "desktop")[0] != 0 and time.time() < deadline:
            time.sleep(0.5)

        self.assertEqual(self.fila("nodes", "validate", "desktop")[0], 0)

    def test_channel_refuses_a_node_that_is_not_a_registered_pull_node(self):
        result = subprocess.run([sys.executable, FAKE_SSH, "user@lab", "fila internal channel --node intruder"],
                                input=b"", capture_output=True, timeout=30)

        self.assertEqual(result.returncode, 4)
        self.assertIn(b"not a registered pull node", result.stdout)

    def test_a_node_may_answer_only_its_own_requests(self):
        store = Store(os.path.join(self.home, "fila.db"))
        request = store.channel_request("desktop", "version", {}, time.time())

        self.assertFalse(store.channel_answer("other", request, {"ok": True, "result": {}}))
        self.assertTrue(store.channel_answer("desktop", request, {"ok": True, "result": {}}))


class PullJobTest(PullCase):
    def setUp(self):
        super().setUp()
        self.stop, self.thread = self.start_worker()
        code, out, err = self.register()
        self.assertEqual(code, 0, err)

    def test_job_runs_on_the_pull_node_and_its_log_comes_back(self):
        job_id = self.submit('echo "on $FILA_NODE"')

        self.cycle_until(lambda: self.state(job_id) == "completed")

        self.assertEqual(self.fila("log", str(job_id))[1].strip(), "on desktop")
        self.assertTrue(os.path.isdir(os.path.join(self.jobs_dir, str(job_id), "attempt-1")))

    def test_lost_channel_keeps_the_job_and_reattaches_it(self):
        job_id = self.submit("sleep 5; echo done")
        self.cycle_until(lambda: self.state(job_id) == "running")

        self.stop.set()
        self.thread.join(15)
        self.cycle_until(lambda: self.state(job_id) == "unreachable", timeout=20)
        self.assertIn("unreachable", self.fila("nodes", "list")[1])

        self.start_worker()
        self.cycle_until(lambda: self.state(job_id) == "completed")

    def test_the_worker_says_where_its_channel_stands_and_top_local_reads_it(self):
        job_id = self.submit("echo hi")
        self.cycle_until(lambda: self.state(job_id) == "completed")

        state = read_json(os.path.join(self.node_dir, "worker.json"))
        self.assertEqual((state["coordinator"], state["node"], state["state"]), ("user@lab", "desktop", "connected"))
        self.assertIn(self.jobs_dir, state["jobs_dirs"])
        self.assertLess(time.time() - state["heard_at"], 60)
        out = io.StringIO()
        code = main(["top", "--local", "--once", "--worker-dir", self.node_dir, "--width", "100"],
                    stdout=out, stderr=io.StringIO())
        lines = out.getvalue().splitlines()
        self.assertEqual(code, 0)
        self.assertIn("node desktop", lines[0])
        self.assertTrue(lines[1].startswith("channel: connected to user@lab"), lines[1])
        self.assertTrue(any(line.split()[:3] == [str(job_id), "1", "finished"] for line in lines), lines)

    def test_nodes_update_sends_the_agent_through_the_channel(self):
        code, _, err = self.fila("nodes", "update", "desktop")

        self.assertEqual(code, 0, err)
        deadline = time.time() + 20
        while not self.installs and time.time() < deadline:
            time.sleep(0.2)
        self.assertEqual(self.installs, [1])
        self.assertTrue(os.path.exists(os.path.join(self.node_dir, "fila.pyz")))


class WorkerStateTest(unittest.TestCase):
    """``~/.fila/worker.json`` while the worker cannot reach the coordinator, and after it stops."""

    def test_a_failing_session_leaves_the_worker_waiting_then_stopped(self):
        home = tempfile.mkdtemp(prefix="fila-node-")
        self.addCleanup(shutil.rmtree, home, ignore_errors=True)
        path = os.path.join(home, "worker.json")
        stop = threading.Event()
        failing_ssh = [sys.executable, "-c", "import sys; sys.exit(255)"]
        thread = threading.Thread(target=run_worker, args=("user@lab", "desktop", failing_ssh),
                                  kwargs=dict(stop=stop, home_dir=home, log=io.StringIO()), daemon=True)
        thread.start()
        deadline = time.time() + 20
        while (read_json(path) or {}).get("state") != "waiting":
            self.assertLess(time.time(), deadline, read_json(path))
            time.sleep(0.05)

        state = read_json(path)
        self.assertEqual((state["coordinator"], state["node"], state["pid"]), ("user@lab", "desktop", os.getpid()))
        self.assertIn("255", state["reason"])
        self.assertLessEqual(state["retry_at"] - time.time(), 5.0)
        self.assertIsNone(state["heard_at"])

        stop.set()
        thread.join(10)
        self.assertEqual(read_json(path)["state"], "stopped")


if __name__ == "__main__":
    unittest.main()
