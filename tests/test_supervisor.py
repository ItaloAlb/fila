"""The supervisor against real processes. The "shell" is Python itself
(``python -c``), so the commands below run on every operating system."""

import os
import shutil
import sys
import tempfile
import textwrap
import threading
import time
import unittest

from fila import supervisor
from fila.supervisor import pid_alive, read_status, request, supervise, write_spec

AFFINITY = textwrap.dedent(
    """
    import os, sys
    if hasattr(os, "sched_getaffinity"):
        print(sorted(os.sched_getaffinity(0)))
    else:
        import ctypes
        from ctypes import wintypes
        k = ctypes.WinDLL("kernel32")
        k.GetCurrentProcess.restype = wintypes.HANDLE
        k.GetProcessAffinityMask.argtypes = [wintypes.HANDLE, ctypes.c_void_p, ctypes.c_void_p]
        mask, system = ctypes.c_size_t(), ctypes.c_size_t()
        k.GetProcessAffinityMask(k.GetCurrentProcess(), ctypes.byref(mask), ctypes.byref(system))
        print([cpu for cpu in range(64) if mask.value >> cpu & 1])
    """
)

COUNTER = textwrap.dedent(
    """
    import time
    for i in range(400):
        with open("counter.txt", "a") as f:
            f.write("x")
        time.sleep(0.05)
    """
)

GRANDCHILD = textwrap.dedent(
    """
    import subprocess, sys
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    open("grandchild.pid", "w").write(str(child.pid))
    """
)


class SupervisorCase(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="fila-sup-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.attempt = os.path.join(self.root, "attempt-1")
        self.cwd = os.path.join(self.root, "work")

    def spec(self, command, **fields):
        spec = dict(
            command=command, shell=[sys.executable, "-c"], cwd=self.cwd, cpus=[0], mem=None,
            time_limit=None, env={"FILA_JOB_ID": "7"}, grace_s=1,
        )
        spec.update(fields)
        write_spec(self.attempt, spec)

    def run_supervisor(self):
        return supervise(self.attempt, poll_s=0.05)

    def in_background(self):
        thread = threading.Thread(target=self.run_supervisor)
        thread.start()
        self.addCleanup(thread.join, 30)
        return thread

    def output(self, name="stdout.log"):
        with open(os.path.join(self.attempt, name)) as handle:
            return handle.read()

    def wait_for(self, condition, timeout=10.0):
        deadline = time.time() + timeout
        while not condition():
            if time.time() > deadline:
                self.fail("condition not reached in time")
            time.sleep(0.05)


class OutcomeTest(SupervisorCase):
    def test_successful_command_is_recorded_with_its_output(self):
        self.spec("import os; print('hi', os.environ['FILA_JOB_ID'], os.getcwd())")

        status = self.run_supervisor()

        self.assertEqual((status["state"], status["exit_code"], status["reason"]), ("finished", 0, None))
        self.assertEqual(self.output().split()[:2], ["hi", "7"])
        self.assertEqual(os.path.normcase(self.output().split()[2]), os.path.normcase(self.cwd))
        self.assertEqual(read_status(self.attempt), status)

    def test_nonzero_exit_is_a_failure_with_its_code(self):
        self.spec("import sys; sys.exit(3)")

        status = self.run_supervisor()

        self.assertEqual((status["exit_code"], status["reason"]), (3, "exit_code"))

    def test_time_limit_ends_the_job(self):
        self.spec("import time; time.sleep(30)", time_limit=0.5)

        started = time.time()
        status = self.run_supervisor()

        self.assertEqual(status["reason"], "timeout")
        self.assertLess(time.time() - started, 10)

    def test_memory_beyond_the_declared_amount_ends_the_job(self):
        self.spec("x = bytearray(400 * 1024 * 1024); print(len(x))", mem=100 * 1024 * 1024)

        status = self.run_supervisor()

        self.assertEqual(status["reason"], "out_of_memory")

    def test_command_is_pinned_to_the_assigned_cpus(self):
        cpus = [0, 1] if (os.cpu_count() or 1) > 1 else [0]
        self.spec(AFFINITY, cpus=cpus)

        status = self.run_supervisor()

        self.assertEqual(status["exit_code"], 0, self.output("stderr.log"))
        self.assertEqual(self.output().strip(), str(cpus))

    def test_processes_left_behind_are_killed_when_the_command_ends(self):
        self.spec(GRANDCHILD)

        self.run_supervisor()

        with open(os.path.join(self.cwd, "grandchild.pid")) as handle:
            grandchild = int(handle.read())
        self.wait_for(lambda: not pid_alive(grandchild))


CGROUP = textwrap.dedent(
    """
    path = [line for line in open("/proc/self/cgroup") if line.startswith("0::")][0][3:].strip()
    print(path.rsplit("/", 1)[-1], open("/sys/fs/cgroup" + path + "/memory.max").read().strip())
    """
)


@unittest.skipUnless(supervisor.cgroup_memory_limit(), "no cgroup v2 memory controller delegated to the user")
class CgroupMemoryTest(SupervisorCase):
    """Linux with systemd: a hard limit in a scope of its own (section 6.3)."""

    def test_job_runs_in_a_scope_with_the_declared_limit(self):
        self.spec(CGROUP, mem=100 * 1024 * 1024)

        status = self.run_supervisor()

        self.assertEqual(status["exit_code"], 0, self.output("stderr.log"))
        scope, limit = self.output().split()
        self.assertTrue(scope.startswith("fila-") and scope.endswith(".scope"), scope)
        self.assertEqual(limit, str(100 * 1024 * 1024))

    def test_job_beyond_the_limit_is_killed_by_the_kernel(self):
        self.spec("x = bytearray(400 * 1024 * 1024); print(len(x))", mem=100 * 1024 * 1024)

        status = self.run_supervisor()

        self.assertEqual(status["reason"], "out_of_memory")
        self.assertEqual(self.output(), "")


class MeasuredMemoryTest(SupervisorCase):
    """Where no cgroup is delegated, memory is measured and the group killed."""

    def setUp(self):
        super().setUp()
        saved = supervisor._cgroup_memory
        supervisor._cgroup_memory = False
        self.addCleanup(setattr, supervisor, "_cgroup_memory", saved)

    def test_memory_beyond_the_declared_amount_ends_the_job(self):
        self.spec("import time; x = bytearray(400 * 1024 * 1024); time.sleep(30)", mem=100 * 1024 * 1024)

        status = self.run_supervisor()

        self.assertEqual(status["reason"], "out_of_memory")


class ControlTest(SupervisorCase):
    def counter(self):
        try:
            return len(open(os.path.join(self.cwd, "counter.txt")).read())
        except OSError:
            return 0

    def test_cancel_request_ends_the_job(self):
        self.spec("import time; time.sleep(30)")
        thread = self.in_background()
        self.wait_for(lambda: (read_status(self.attempt) or {}).get("state") == "running")

        request(self.attempt, "cancel")
        thread.join(10)

        self.assertEqual(read_status(self.attempt)["reason"], "cancelled")

    def test_suspend_freezes_and_resume_continues(self):
        self.spec(COUNTER)
        self.in_background()
        self.wait_for(lambda: self.counter() > 3)

        request(self.attempt, "suspend")
        self.wait_for(lambda: read_status(self.attempt).get("suspended"))
        time.sleep(0.3)
        frozen = self.counter()
        time.sleep(0.6)
        self.assertEqual(self.counter(), frozen)

        request(self.attempt, "resume")
        self.wait_for(lambda: self.counter() > frozen + 2)
        request(self.attempt, "cancel")

    def test_suspended_time_does_not_count_toward_the_limit(self):
        self.spec(COUNTER, time_limit=1.0)
        thread = self.in_background()
        self.wait_for(lambda: self.counter() > 0)

        request(self.attempt, "suspend")
        time.sleep(1.5)
        request(self.attempt, "resume")
        self.wait_for(lambda: not read_status(self.attempt).get("suspended"))
        self.assertTrue(thread.is_alive())

        thread.join(10)
        status = read_status(self.attempt)
        self.assertEqual(status["reason"], "timeout")
        self.assertGreaterEqual(status["suspended_seconds"], 1.0)


if __name__ == "__main__":
    unittest.main()
