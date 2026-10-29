"""Slurm clusters: the batch script and parsers, then the whole path through a
via computer to a fake Slurm behind a fake SSH."""

import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

from fila.cli import main
from fila.registry import NodeEntry, Registry
from fila.server import Server
from fila.slurm import check_extra, outcome, parse_limits, parse_machines, parse_states, sbatch_script, shebang

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fake_slurm  # noqa: E402
from fake_remote_ssh import bash  # noqa: E402

GiB = 1024**3
FAKE_SSH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fake_remote_ssh.py")


def cluster_entry(**fields):
    values = dict(name="cluster-cpu", type="slurm", via="lab", host="cluster", partition="cpuq",
                  shell=["/bin/bash", "-lc"], jobs_dir="/home/u/fila/jobs", exclude=["apollo01"])
    values.update(fields)
    return NodeEntry(**values)


class ScriptTest(unittest.TestCase):
    def test_script_carries_the_declared_resources_and_paths(self):
        spec = {"time_limit": 3 * 86400, "dir": "/home/u/alea", "slurm_exclude": ["apollo02"],
                "hint": "nomultithread", "slurm_extra": "--constraint=avx512 --mail-type=END"}

        script = sbatch_script(42, 1, "./build/alea x.yaml", {"cores": 64, "mem": 32 * GiB}, spec, cluster_entry(), "cluster-cpu")

        lines = script.splitlines()
        self.assertEqual(lines[0], "#!/bin/bash -l")
        for expected in (
            "#SBATCH --partition=cpuq", "#SBATCH --nodes=1", "#SBATCH --cpus-per-task=64", "#SBATCH --mem=32768M",
            "#SBATCH --time=3-00:00:00", "#SBATCH --chdir=/home/u/alea",
            "#SBATCH --output=/home/u/fila/jobs/42/attempt-1/stdout.log", "#SBATCH --exclude=apollo01,apollo02",
            "#SBATCH --hint=nomultithread", "#SBATCH --constraint=avx512", "#SBATCH --mail-type=END",
        ):
            self.assertIn(expected, lines)
        self.assertIn("FILA_CORES=64", script)
        self.assertEqual(lines[-1], "./build/alea x.yaml")

    def test_default_directory_is_the_jobs_own(self):
        script = sbatch_script(7, 2, "true", {"cores": 1, "mem": 1}, {}, cluster_entry(exclude=[]), "c")

        self.assertIn("#SBATCH --chdir=/home/u/fila/jobs/7", script)
        self.assertIn("#SBATCH --mem=1M", script)  # rounded up to whole MiB
        self.assertNotIn("--exclude", script)

    def test_shebang_drops_the_command_flag(self):
        self.assertEqual(shebang(["/bin/bash", "-lc"]), "#!/bin/bash -l")
        self.assertEqual(shebang(["/bin/sh", "-c"]), "#!/bin/sh")

    def test_extra_options_that_change_resources_are_refused(self):
        for extra in ("--time=10", "--mem 4G", "-c 8", "--nodes=2"):
            with self.subTest(extra=extra):
                self.assertIsNotNone(check_extra(extra))
        self.assertIsNone(check_extra("--constraint=avx512 --qos=long"))


class ParserTest(unittest.TestCase):
    NODES = (
        "NodeName=apollo01 Arch=x86_64 CPUAlloc=200 CPUTot=256 RealMemory=515000 AllocMem=100000 State=MIXED OS=Linux 5\n"
        "NodeName=apollo02 CPUAlloc=0 CPUTot=256 RealMemory=515000 AllocMem=0 State=IDLE+DRAIN Reason=bad disk\n"
        "NodeName=gpu01 CPUAlloc=0 CPUTot=64 RealMemory=200000 AllocMem=0 State=IDLE\n"
    )

    def test_machines_of_the_partition_with_what_is_free(self):
        machines = parse_machines("apollo01\napollo02\n", self.NODES)

        self.assertEqual([m["name"] for m in machines], ["apollo01", "apollo02"])
        self.assertEqual((machines[0]["idle_cores"], machines[0]["free_mem"]), (56, 415000 * 1024**2))
        self.assertEqual((machines[1]["idle_cores"], machines[1]["total_cores"]), (0, 256))  # drained: nothing free

    def test_limits_from_partition_and_qos(self):
        partition = "PartitionName=cpuq MaxNodes=UNLIMITED MaxTime=7-00:00:00 MaxCPUsPerNode=128 QoS=normal"

        self.assertEqual(parse_limits(partition, "normal|cpu=20,mem=64G|\nlong||\n", None), (20, 7 * 86400.0))
        self.assertEqual(parse_limits(partition.replace("MaxTime=7-00:00:00", "MaxTime=UNLIMITED"), "", None), (128, None))

    def test_states_prefer_sacct_and_read_exit_codes(self):
        queue = "1001|PENDING\n1002|RUNNING\n"
        sacct = (
            "1002|RUNNING|0:0|2026-10-29T10:00:00|Unknown\n"
            "1003|FAILED|3:0|2026-10-29T10:00:00|2026-10-29T11:00:00\n"
            "1004|CANCELLED by 1234|0:15|2026-10-29T10:00:00|2026-10-29T10:05:00\n"
            "1005|OUT_OF_MEMORY|0:125|2026-10-29T10:00:00|2026-10-29T10:05:00\n"
        )

        states = parse_states(queue, sacct, ["1001", "1002", "1003", "1004", "1005"])

        self.assertEqual(states["1001"]["state"], "PENDING")
        self.assertEqual((states["1003"]["state"], states["1003"]["exit_code"]), ("FAILED", 3))
        self.assertEqual(states["1004"]["state"], "CANCELLED")
        self.assertEqual(outcome("OUT_OF_MEMORY"), ("failed", "out_of_memory"))
        self.assertEqual(outcome("TIMEOUT"), ("failed", "timeout"))
        self.assertEqual(outcome("NODE_FAIL"), ("failed", "node_failure"))
        self.assertEqual(outcome("RUNNING"), ("running", None))
        self.assertEqual(outcome("PENDING"), ("pending", None))


class ClusterCase(unittest.TestCase):
    """Coordinator + local via computer ``lab`` + fake cluster ``cluster`` behind a fake SSH."""

    NODES = {"apollo01": (16, 64000, 0), "apollo05": (8, 32000, 0)}
    QOS_CPU = "12"

    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-coord-")
        self.cluster_home = tempfile.mkdtemp(prefix="fila-cluster-")
        tools = tempfile.mkdtemp(prefix="fila-slurm-")
        for path in (self.home, self.cluster_home, tools):
            self.addCleanup(shutil.rmtree, path, ignore_errors=True)
        self.state = os.path.join(tools, "state.json")
        fake_slurm.new_state(self.state, self.NODES, qos_cpu=self.QOS_CPU)
        fake_slurm.install(os.path.join(tools, "bin"))
        environ = mock.patch.dict(os.environ, {
            "FAKE_REMOTE_HOME": self.cluster_home, "FAKE_REMOTE_PATH": os.path.join(tools, "bin"),
            "FAKE_SLURM_STATE": self.state, "FAKE_SLURM_BASH": bash(),
        })
        environ.start()
        self.addCleanup(environ.stop)
        lab = NodeEntry(name="lab", type="computer", access="local", os=sys.platform, shell=[bash(), "-c"],
                        jobs_dir=os.path.join(self.home, "lab-jobs"), limits={"cores": 1})
        Registry(os.path.join(self.home, "nodes.toml")).save({"ssh_command": [sys.executable, FAKE_SSH]}, {"lab": lab})
        code, out, err = self.fila(
            "nodes", "register", "cluster-cpu", "--type", "slurm", "--via", "lab", "--host", "cluster",
            "--partition", "cpuq", "--shell", "/bin/bash -l", "--jobs-dir", "~/fila/jobs", "--max-pending", "2",
        )
        self.assertEqual(code, 0, err)
        self.registered = out
        self.server = Server(self.home)

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    def submit(self, command, cores="2", *options):
        code, out, err = self.fila("submit", "--json", "--cores", cores, "--mem", "1G", "--on", "cluster-cpu",
                                   *options, "--", command)
        self.assertEqual(code, 0, err)
        return json.loads(out)["id"]

    def result(self, job_id):
        return json.loads(self.fila("result", "--json", str(job_id))[1])

    def slurm_jobs(self):
        with open(self.state) as handle:
            return json.load(handle)["jobs"]


class RegisterClusterTest(ClusterCase):
    def test_registration_reads_machines_limits_and_makes_the_jobs_dir_absolute(self):
        entry = Registry(os.path.join(self.home, "nodes.toml")).load()[1]["cluster-cpu"]

        self.assertIn("2 Slurm machines", self.registered)
        self.assertIn("at most 12 cores per job", self.registered)
        self.assertFalse(entry.jobs_dir.startswith("~"))
        self.assertTrue(entry.jobs_dir.endswith("/fila/jobs"))
        self.assertEqual(entry.max_time, 7 * 86400.0)

    def test_unknown_partition_is_refused(self):
        code, _, err = self.fila(
            "nodes", "register", "other", "--type", "slurm", "--via", "lab", "--host", "cluster",
            "--partition", "gpuq", "--shell", "/bin/bash -l", "--jobs-dir", "~/fila/jobs",
        )

        self.assertEqual(code, 4)
        self.assertIn("gpuq", err)

    def test_requests_beyond_the_qos_are_refused_at_submit(self):
        code, _, err = self.fila("submit", "--cores", "14", "--mem", "1G", "--on", "cluster-cpu", "--", "true")

        self.assertEqual(code, 4)
        self.assertIn("cluster-cpu", err)

    def test_extra_options_owned_by_fila_are_refused(self):
        code, _, err = self.fila("submit", "--cores", "1", "--mem", "1G", "--slurm-extra", "--time=5", "--", "true")

        self.assertEqual(code, 4)
        self.assertIn("--time", err)

    def test_dry_run_shows_the_script(self):
        code, out, _ = self.fila("submit", "--json", "--cores", "2", "--mem", "1G", "--on", "cluster-cpu",
                                 "--dry-run", "--", "echo hi")

        result = json.loads(out)
        self.assertEqual((code, result["would"]), (0, "submit_to_slurm"))
        self.assertIn("#SBATCH --cpus-per-task=2", result["script"])


class ClusterJobTest(ClusterCase):
    def test_job_is_submitted_runs_and_its_log_comes_back(self):
        job_id = self.submit('echo "on $FILA_NODE with $FILA_CORES cores"')

        self.server.cycle()
        self.assertEqual(self.result(job_id)["state"], "slurm_queued")
        (slurm_job,) = self.slurm_jobs().values()
        self.assertEqual(slurm_job["options"]["cpus-per-task"], "2")
        self.assertEqual(slurm_job["options"]["exclude"] if "exclude" in slurm_job["options"] else None, None)

        fake_slurm.run_pending(self.state, bash())
        self.server.cycle()

        result = self.result(job_id)
        self.assertEqual((result["state"], result["attempts"][0]["exit_code"]), ("completed", 0))
        self.assertEqual(result["attempts"][0]["slurm_id"], "1000")
        self.assertEqual(self.fila("log", str(job_id))[1].strip(), "on cluster-cpu with 2 cores")

    def test_running_state_failures_and_memory(self):
        failing = self.submit("exit 3")
        hungry = self.submit("true")
        self.server.cycle()
        fake_slurm.start(self.state, "1001")
        self.server.cycle()
        self.assertEqual(self.result(hungry)["state"], "running")

        fake_slurm.run_pending(self.state, bash())
        fake_slurm.finish(self.state, "1001", "OUT_OF_MEMORY", "0:125")
        self.server.cycle()

        self.assertEqual((self.result(failing)["reason"], self.result(failing)["attempts"][0]["exit_code"]), ("exit_code", 3))
        self.assertEqual((self.result(hungry)["state"], self.result(hungry)["reason"]), ("failed", "out_of_memory"))

    def test_cancel_calls_scancel(self):
        job_id = self.submit("sleep 60")
        self.server.cycle()

        self.fila("cancel", str(job_id))
        self.server.cycle()
        self.server.cycle()

        self.assertEqual(self.slurm_jobs()["1000"]["state"], "CANCELLED")
        self.assertEqual(self.result(job_id)["state"], "cancelled")

    def test_unreachable_cluster_keeps_its_jobs(self):
        queued = self.submit("true")
        self.server.cycle()

        with mock.patch.dict(os.environ, {"FAKE_SSH_FAIL": "1"}):
            self.server.cycle()
            self.assertIn("unreachable", self.fila("nodes", "list")[1])
        self.assertEqual(self.result(queued)["state"], "slurm_queued")

        fake_slurm.run_pending(self.state, bash())
        self.server.cycle()
        self.assertEqual(self.result(queued)["state"], "completed")

    def test_clean_removes_the_jobs_files_on_the_cluster(self):
        job_id = self.submit("echo x")
        self.server.cycle()
        fake_slurm.run_pending(self.state, bash())
        self.server.cycle()
        jobs_dir = fake_slurm.windows_path(Registry(os.path.join(self.home, "nodes.toml")).load()[1]["cluster-cpu"].jobs_dir)
        self.assertTrue(os.path.isdir(os.path.join(jobs_dir, str(job_id))))

        self.fila("clean", "--older-than", "0s")

        self.assertFalse(os.path.isdir(os.path.join(jobs_dir, str(job_id))))


class BusyClusterTest(ClusterCase):
    NODES = {"apollo01": (16, 64000, 16), "apollo05": (8, 32000, 8)}  # every CPU allocated

    def test_busy_cluster_takes_k_pending_jobs(self):
        jobs = [self.submit("true") for _ in range(3)]

        self.server.cycle()

        self.assertEqual([self.result(job)["state"] for job in jobs], ["slurm_queued", "slurm_queued", "pending"])
        explained = self.fila("explain", str(jobs[2]))[1]
        self.assertIn("2 of 2 allowed jobs already pending", explained)


if __name__ == "__main__":
    unittest.main()
