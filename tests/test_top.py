# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""``fila top``: the pure drawing function against snapshots built by hand,
the snapshot read from a real database, and ``fila top --once`` against a
server cycling a local node."""

import datetime
import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest

from fila import top
from fila.cli import main
from fila.scheduler import Node, Resources, SlurmMachine
from fila.store import Store

GiB = 1024**3
NOW = datetime.datetime(2026, 11, 20, 18, 42, 7).timestamp()
DRAWING = set("".join(top.UNICODE.values()))


def node(name="lab", state="online", kind="computer", cores=12, mem=16 * GiB, free_cores=4, free_mem=8 * GiB,
         fila_cores=4, fila_mem=4 * GiB, speed=1.28, in_use=True, **extra):
    row = {"name": name, "kind": kind, "state": state, "speed": speed, "in_use": in_use,
           "capacity": {"cores": cores, "mem": mem}, "free": {"cores": free_cores, "mem": free_mem},
           "fila": {"cores": fila_cores, "mem": fila_mem}}
    row.update(extra)
    return row


def job(id, state="running", node="lab", cores=4, mem=8 * GiB, elapsed=4353.0, name=None, command="./run",
        waiting=None, suspended=False):
    return {"id": id, "name": name, "command": command, "state": state, "suspended": suspended, "node": node,
            "cores": cores, "mem": mem, "elapsed": elapsed, "waiting": waiting}


def snapshot(nodes=None, jobs=None, server_running=True):
    return {"coordinator": "lab", "now": NOW, "server_running": server_running,
            "nodes": [node()] if nodes is None else nodes, "jobs": [job(42)] if jobs is None else jobs}


class HeaderTest(unittest.TestCase):
    def test_header_names_the_coordinator_and_the_time(self):
        first = top.render(snapshot(), 100)[0]

        self.assertTrue(first.startswith("fila top · coordinator lab · 18:42:07"), first)
        self.assertNotIn("server not running", first)

    def test_a_stale_heartbeat_is_said_in_the_header(self):
        self.assertIn("server not running", top.render(snapshot(server_running=False), 100)[0])

    def test_the_interactive_header_shows_the_refresh_and_the_keys(self):
        first = top.render(snapshot(), 100, interval=2, interactive=True)[0]

        self.assertIn("refresh 2 s", first)
        self.assertTrue(first.endswith("q quit  ? help"))
        self.assertEqual(len(first), 100)


class NodePanelTest(unittest.TestCase):
    def line(self, **fields):
        lines = top.render(snapshot(nodes=[node(**fields)], jobs=[]), 100)
        return lines[3]  # header, rule, column titles, the node

    def test_bars_split_fila_jobs_other_use_and_free(self):
        line = self.line()

        # 12 cores, 4 free, 4 held by fila jobs: 4 marks fila, 4 other use or reserve, 4 free.
        self.assertIn("[████▒▒▒▒····]  8/12", line)
        self.assertIn("8.0/16.0G", line)
        self.assertIn("1.28", line)
        self.assertTrue(line.rstrip().endswith("yes"))

    def test_a_cluster_shows_its_pending_allowance(self):
        line = self.line(name="cluster-cpu", kind="slurm", slurm_pending=1, max_pending=2)

        self.assertIn("pending 1/2 in Slurm", line)

    def test_an_unreachable_node_shows_no_numbers(self):
        line = self.line(name="desktop", state="unreachable")

        self.assertEqual(line.split(), ["desktop", "unreachable", "—", "—", "—", "—"])

    def test_unknown_use_is_a_dash(self):
        self.assertTrue(self.line(in_use=None).rstrip().endswith("—"))

    def test_narrow_terminals_drop_speed_then_in_use_then_shrink_the_bars(self):
        titles = {width: top.render(snapshot(jobs=[]), width)[2].split() for width in (100, 84, 76, 60)}

        self.assertEqual(titles[100], ["NODE", "STATE", "CORES", "MEMORY", "SPEED", "IN", "USE"])
        self.assertEqual(titles[84], ["NODE", "STATE", "CORES", "MEMORY", "IN", "USE"])
        self.assertEqual(titles[76], ["NODE", "STATE", "CORES", "MEMORY"])
        self.assertIn("[██▒▒··]", top.render(snapshot(jobs=[]), 60)[3])


class JobPanelTest(unittest.TestCase):
    def rows(self, jobs, width=100, **options):
        lines = top.render(snapshot(nodes=[], jobs=jobs), width, **options)
        start = lines.index("  ID  STATE      NODE         CORES  MEM    ELAPSED   NAME / COMMAND") \
            if width >= 80 else 4
        return lines[start + 1:]

    def test_a_running_job_shows_its_request_and_elapsed_time(self):
        (row,) = self.rows([job(42, name="moire_dmc nu=1/3")])

        self.assertEqual(row.split(), ["42", "running", "lab", "4", "8G", "01:12:33", "moire_dmc", "nu=1/3"])

    def test_waiting_jobs_say_why(self):
        rows = self.rows([
            job(43, state="pending", node=None, elapsed=None, waiting="waiting: cluster-cpu: no Slurm machine idle"),
            job(44, state="blocked", node=None, elapsed=None, command="analyse", waiting="dependency 42 is failed"),
        ], width=120)

        self.assertTrue(rows[0].endswith("./run   (waiting: cluster-cpu: no Slurm machine idle)"), rows[0])
        self.assertIn("—", rows[0].split()[2])
        self.assertTrue(rows[1].endswith("analyse   (dependency 42 is failed)"))

    def test_a_suspended_job_says_so(self):
        (row,) = self.rows([job(45, suspended=True)])

        self.assertEqual(row.split()[1], "suspended")

    def test_the_command_is_truncated_first(self):
        (row,) = self.rows([job(42, command="x" * 200)], width=90)

        self.assertEqual(len(row), 90)
        self.assertTrue(row.endswith("…"))
        self.assertEqual(row.split()[:6], ["42", "running", "lab", "4", "8G", "01:12:33"])

    def test_below_the_minimum_memory_and_elapsed_disappear(self):
        lines = top.render(snapshot(nodes=[], jobs=[job(42)]), 60)
        titles = next(line for line in lines if line.lstrip().startswith("ID"))

        self.assertNotIn("MEM", titles)
        self.assertNotIn("ELAPSED", titles)
        self.assertTrue(all(len(line) <= 60 for line in lines))

    def test_order_and_filter(self):
        jobs = [job(1, elapsed=10.0, node="lab"), job(2, elapsed=None, state="pending", node=None),
                job(3, elapsed=500.0, node="desktop", command="./bench")]

        def ids(**options):
            return [int(row.split()[0]) for row in self.rows(jobs, **options)]

        self.assertEqual(ids(), [1, 2, 3])
        self.assertEqual(ids(order="elapsed"), [3, 1, 2])
        self.assertEqual(ids(order="node"), [3, 1, 2])
        self.assertEqual(ids(order="state"), [1, 3, 2])
        self.assertEqual(ids(text_filter="BENCH"), [3])
        self.assertEqual(ids(text_filter="pending"), [2])
        with self.assertRaises(ValueError):
            ids(order="size")

    def test_the_selected_job_is_marked(self):
        rows = self.rows([job(41), job(42)], selected=42)

        self.assertEqual([row[:2] for row in rows], ["  ", "> "])


class ScreenTest(unittest.TestCase):
    def full_snapshot(self):
        return snapshot(
            nodes=[node(), node(name="cluster-cpu", kind="slurm", slurm_pending=1, max_pending=2),
                   node(name="desktop", state="unreachable")],
            jobs=[job(42), job(43, state="pending", node=None, elapsed=None, waiting="waiting: lab: needs cores=6"),
                  job(44, state="blocked", node=None, elapsed=None, waiting="dependency 42 is failed")],
            server_running=False,
        )

    def test_every_line_fits_the_width(self):
        for width in (40, 60, 79, 80, 87, 120):
            with self.subTest(width=width):
                lines = top.render(self.full_snapshot(), width, interactive=True, interval=2)

                self.assertTrue(all(len(line) <= width for line in lines), [l for l in lines if len(l) > width])

    def test_nothing_on_screen_is_in_portuguese(self):
        # The screen is English, like the rest of the program: only ASCII text and drawing characters.
        lines = top.render(self.full_snapshot(), 100, interactive=True, interval=2)

        for line in lines:
            self.assertTrue(all(char.isascii() or char in DRAWING for char in line), line)
        text = " ".join(lines).lower()
        for word in ("trabalho", "fila de", "livre", "esperando", "memoria", "nucleo", "sair", "ajuda", "cancelar"):
            self.assertNotIn(word, text)

    def test_the_ascii_charset_encodes_everywhere(self):
        lines = top.render(self.full_snapshot(), 100, charset=top.ASCII, interactive=True)

        "\n".join(lines).encode("ascii")
        self.assertIn("[####++++....]", "\n".join(lines))

    def test_empty_panels_say_so(self):
        lines = top.render(snapshot(nodes=[], jobs=[]), 80)

        self.assertIn("no nodes registered", lines)
        self.assertIn("no jobs", lines)

    def test_charset_follows_the_stream_encoding(self):
        class Stream(io.StringIO):
            def __init__(self, encoding):
                super().__init__()
                self._encoding = encoding

            @property
            def encoding(self):
                return self._encoding

        self.assertIs(top.charset_for(Stream("utf-8")), top.UNICODE)
        self.assertIs(top.charset_for(Stream("cp1252")), top.ASCII)
        self.assertIs(top.charset_for(io.StringIO()), top.UNICODE)


class WaitingTextTest(unittest.TestCase):
    def test_explain_texts_are_summarised(self):
        cases = [
            (("waiting_dependency: waiting for 42 (running)", ()), "after 42 (running)"),
            (("blocked: dependency 42 is failed", ()), "dependency 42 is failed"),
            (("pending: no node can start it now", ("lab: needs cores=6, free 4", "desktop: unreachable")),
             "waiting: lab: needs cores=6, free 4 (+1 more)"),
            (("pending: no node can start it now", ("lab: excluded by --on", "desktop: unreachable")),
             "waiting: desktop: unreachable"),
            (("pending: no node can start it now; lab is reserved for it (starvation protection)",
              ("lab: needs cores=6, free 4",)),
             "waiting: lab: needs cores=6, free 4; lab is reserved for it (starvation protection)"),
            (("pending: no node can start it now", ()), "no node can start it now"),
        ]
        for (summary, details), expected in cases:
            with self.subTest(summary=summary):
                self.assertEqual(top._waiting(summary, details), expected)


class CollectTest(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-top-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self.store = Store(os.path.join(self.home, "fila.db"))
        self.store.put_node(Node(name="lab", capacity=Resources(cores=8, mem=16 * GiB),
                                 probe_free=Resources(cores=8, mem=16 * GiB), in_use=False))
        self.store.put_node(Node(name="cluster-cpu", kind="slurm", capacity=Resources(cores=64, mem=256 * GiB),
                                 probe_free=Resources(cores=64, mem=256 * GiB), max_pending=2,
                                 slurm=(SlurmMachine("node01", 64, 256 * GiB, 64, 256 * GiB),)))

    def add(self, cores=2, **spec):
        return self.store.add_job("./run", Resources(cores=cores, mem=GiB), spec, "user", NOW - 100)

    def test_snapshot_carries_nodes_jobs_and_reasons(self):
        from fila.scheduler import plan

        running = self.add()
        waiting = self.add(cores=999)
        self.store.apply(plan(self.store.snapshot(NOW - 60)), NOW - 60)
        self.store.attempt_progress(running, 1, suspended=True, suspended_seconds=20.0)

        data = top.collect(self.store, self.home, NOW, interval=30.0, coordinator="lab")

        lab = next(n for n in data["nodes"] if n["name"] == "lab")
        self.assertEqual((lab["fila"]["cores"], lab["in_use"], lab["free"]["cores"]), (2, False, 6))
        cluster = next(n for n in data["nodes"] if n["name"] == "cluster-cpu")
        self.assertEqual((cluster["slurm_pending"], cluster["max_pending"]), (0, 2))
        jobs = {j["id"]: j for j in data["jobs"]}
        self.assertEqual((jobs[running]["state"], jobs[running]["node"], jobs[running]["suspended"]),
                         ("running", "lab", True))
        self.assertEqual(jobs[running]["elapsed"], 40.0)  # 60 s since the start, 20 of them suspended
        self.assertEqual(jobs[waiting]["state"], "pending")
        self.assertTrue(jobs[waiting]["waiting"].startswith("waiting: "), jobs[waiting]["waiting"])
        self.assertFalse(data["server_running"])
        json.dumps(data)  # plain data, for --json

    def test_finished_jobs_only_with_show_finished_and_node_filter(self):
        done = self.add()
        self.store.finish(done, "completed", NOW, exit_code=0)
        other = self.add()

        self.assertEqual([j["id"] for j in top.collect(self.store, self.home, NOW, 30.0)["jobs"]], [other])
        everything = top.collect(self.store, self.home, NOW, 30.0, show_finished=True)
        self.assertEqual([j["id"] for j in everything["jobs"]], [done, other])
        only_cluster = top.collect(self.store, self.home, NOW, 30.0, node="cluster-cpu")
        self.assertEqual(([n["name"] for n in only_cluster["nodes"]], only_cluster["jobs"]), (["cluster-cpu"], []))


class OnceTest(unittest.TestCase):
    """``fila top --once`` through the real command line, after a server cycle on this machine."""

    def setUp(self):
        from fila.registry import NodeEntry, Registry
        from fila.server import Server

        self.home = tempfile.mkdtemp(prefix="fila-top-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        entry = NodeEntry(name="here", type="computer", access="local", os=sys.platform,
                          shell=[sys.executable, "-c"], jobs_dir=os.path.join(self.home, "jobs"),
                          limits={"cores": 2, "mem": 1 * GiB})
        Registry(os.path.join(self.home, "nodes.toml")).save({}, {"here": entry})
        self.server = Server(self.home)
        self.server.cycle()

    def fila(self, *args):
        out, err = io.StringIO(), io.StringIO()
        code = main(list(args), home=self.home, stdout=out, stderr=err)
        return code, out.getvalue(), err.getvalue()

    def test_once_prints_the_nodes_and_the_jobs(self):
        self.fila("submit", "--cores", "1", "--mem", "64M", "--name", "nap", "--", "import time; time.sleep(20)")
        self.fila("submit", "--cores", "2", "--mem", "64M", "--", "pass")  # waits: the first holds a core
        self.server.cycle()
        self.addCleanup(self.stop, 1)

        code, out, err = self.fila("top", "--once", "--width", "100")

        self.assertEqual((code, err), (0, ""))
        lines = out.splitlines()
        self.assertTrue(lines[0].startswith("fila top"), lines[0])
        self.assertTrue(any(line.startswith("here") and "online" in line for line in lines), out)
        self.assertTrue(any(line.split()[:3] == ["1", "running", "here"] and "nap" in line for line in lines), out)
        self.assertTrue(any(line.split()[:2] == ["2", "pending"] and "(waiting: " in line for line in lines), out)
        self.assertTrue(all(len(line) <= 100 for line in lines))

    def stop(self, job_id):
        """Cancel the job and let the server deliver it, so no process outlives the test."""
        self.fila("cancel", str(job_id))
        deadline = time.time() + 30
        while json.loads(self.fila("result", "--json", str(job_id))[1])["state"] != "cancelled":
            if time.time() > deadline:
                self.fail(f"job {job_id} was not cancelled")
            self.server.cycle()
            time.sleep(0.1)

    def test_once_json_is_the_snapshot(self):
        code, out, _ = self.fila("top", "--once", "--json")

        data = json.loads(out)
        self.assertEqual((code, [n["name"] for n in data["nodes"]], data["jobs"]), (0, ["here"], []))

    def test_the_interactive_view_needs_a_terminal(self):  # here stdout is a StringIO
        code, _, err = self.fila("top")

        self.assertEqual(code, 5)
        self.assertIn("--once", err)


# --- The node's own view (fila top --local) ---


def local_snapshot(channel="connected", attempts=None, jobs_dirs=("/jobs",)):
    channels = {
        "connected": {"coordinator": "user@lab", "state": "connected", "heard_at": NOW - 3, "retry_at": None,
                      "reason": None},
        "waiting": {"coordinator": "user@lab", "state": "waiting", "heard_at": None, "retry_at": NOW + 25,
                    "reason": "ssh exited with 255"},
        "not running": {"coordinator": "user@lab", "state": "not running", "heard_at": NOW - 900, "retry_at": None,
                        "reason": None},
        None: None,
    }
    if attempts is None:
        attempts = [
            local_attempt(42, cpus=[0, 1, 2, 3], mem=8 * GiB, elapsed=4353.0, command="./build/alea configs/x.yaml"),
            local_attempt(45, state="suspended", cpus=[4, 5], mem=2 * GiB, elapsed=602.0, command="./bench"),
            local_attempt(40, attempt=2, state="finished", elapsed=190.0, command="echo ok", exit_code=0),
        ]
    return {"view": "local", "node": "desktop", "now": NOW, "channel": channels[channel],
            "resources": {"cores": 28, "cores_used": 6, "cores_fila": 6, "mem": 16 * GiB, "mem_used": 9 * GiB,
                          "mem_fila": 10 * GiB, "in_use": False},
            "jobs_dirs": list(jobs_dirs), "attempts": attempts}


def local_attempt(job, attempt=1, state="running", cpus=(), mem=None, elapsed=None, command="./run", exit_code=None,
                  reason=None):
    return {"job": job, "attempt": attempt, "state": state, "command": command, "cpus": list(cpus), "mem_limit": mem,
            "elapsed": elapsed, "exit_code": exit_code, "reason": reason, "ended_at": None, "jobs_dir": "/jobs"}


class LocalRenderTest(unittest.TestCase):
    def test_the_screen_shows_the_node_its_channel_resources_and_attempts(self):
        lines = top.render(local_snapshot(), 100)

        self.assertTrue(lines[0].startswith("fila top --local · node desktop · 18:42:07"), lines[0])
        self.assertEqual(lines[1], "channel: connected to user@lab · heard 3 s ago")
        self.assertIn("6/28 used", lines[3])
        self.assertIn("9.0/16.0G", lines[3])
        self.assertTrue(lines[3].endswith("IN USE  no"), lines[3])
        self.assertEqual(lines[5], "  JOB  ATTEMPT  STATE      CPUS        MEM LIMIT  ELAPSED   COMMAND")
        self.assertEqual(lines[6].split(), ["42", "1", "running", "0-3", "8G", "01:12:33", "./build/alea", "configs/x.yaml"])
        self.assertEqual(lines[7].split()[:4], ["45", "1", "suspended", "4,5"])
        self.assertEqual(lines[8].split()[:5], ["40", "2", "finished", "—", "—"])
        self.assertTrue(lines[8].endswith("echo ok   (exit 0)"), lines[8])

    def test_channel_states(self):
        self.assertEqual(top.render(local_snapshot("waiting"), 100)[1],
                         "channel: cannot reach user@lab · retry in 25 s · ssh exited with 255")
        self.assertEqual(top.render(local_snapshot("not running"), 100)[1],
                         "channel: worker not running (coordinator user@lab)")
        self.assertTrue(top.render(local_snapshot(None), 100)[1].startswith("─"))  # no channel line

    def test_outcomes_of_ended_attempts(self):
        attempts = [local_attempt(1, state="finished", exit_code=3, reason="exit_code"),
                    local_attempt(2, state="finished", reason="timeout"),
                    local_attempt(3, state="lost")]

        rows = top.render(local_snapshot(attempts=attempts), 100)[-3:]

        self.assertEqual([row.rsplit("(", 1)[1] for row in rows], ["exit 3)", "timeout)", "lost)"])

    def test_without_attempts_it_says_why(self):
        self.assertEqual(top.render(local_snapshot(attempts=[]), 80)[-1], "no attempts here")
        self.assertEqual(top.render(local_snapshot(attempts=[], jobs_dirs=()), 80)[-1],
                         "no jobs directory known: pass --jobs-dir")

    def test_interactive_footer_says_actions_belong_to_the_coordinator(self):
        lines = top.render(local_snapshot(), 100, interactive=True, interval=2)

        self.assertTrue(lines[0].endswith("q quit  ? help"))
        self.assertIn("refresh 2 s", lines[0])
        self.assertIn("are done on the coordinator", lines[-1])

    def test_it_fits_any_width_and_stays_english(self):
        for width in (40, 60, 80, 100, 140):
            with self.subTest(width=width):
                lines = top.render(local_snapshot(), width, interactive=True, interval=2)

                self.assertTrue(all(len(line) <= width for line in lines))
                for line in lines:
                    self.assertTrue(all(char.isascii() or char in DRAWING for char in line), line)

    def test_cpu_lists(self):
        self.assertEqual(top._cpu_list([0, 1, 2, 3, 8, 9]), "0-3,8,9")
        self.assertEqual(top._cpu_list([5, 4]), "4,5")
        self.assertEqual(top._cpu_list([0, 2, 3, 4, 7]), "0,2-4,7")


class LocalProbe:
    def hardware(self):
        from fila.probe import Hardware

        return Hardware(logical_cpus=28, topology=tuple((cpu,) for cpu in range(28)), total_mem=16 * GiB)

    def sample(self):
        from fila.probe import Reading

        return Reading(free_cores=22, available_mem=7 * GiB, idle_seconds=3600.0)


class LocalCollectTest(unittest.TestCase):
    """Journals and a worker.json written by hand, as a supervisor and a worker would."""

    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="fila-local-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        self.worker_dir = os.path.join(self.root, "dot-fila")
        self.jobs_dir = os.path.join(self.root, "jobs")
        os.makedirs(self.worker_dir)
        self.now = time.time()
        self.alive = os.getpid()
        self.dead = self.exited_pid()

    def exited_pid(self):
        import subprocess

        process = subprocess.Popen([sys.executable, "-c", "pass"])
        process.wait()
        return process.pid

    def journal(self, job, attempt, spec, status=None):
        directory = os.path.join(self.jobs_dir, str(job), f"attempt-{attempt}")
        os.makedirs(directory)
        with open(os.path.join(directory, "job.json"), "w") as handle:
            json.dump(dict({"command": "./run", "cpus": [], "mem": None}, **spec), handle)
        if status is not None:
            with open(os.path.join(directory, "status.json"), "w") as handle:
                json.dump(dict({"supervisor_pid": self.alive, "suspended": False, "suspended_seconds": 0.0,
                                "suspended_since": None, "finished_at": None, "exit_code": None, "reason": None},
                               **status), handle)

    def worker(self, **fields):
        data = dict(coordinator="user@lab", node="desktop", pid=self.alive, state="connected",
                    heard_at=self.now - 3, retry_at=None, reason=None, jobs_dirs=[self.jobs_dir])
        data.update(fields)
        with open(os.path.join(self.worker_dir, "worker.json"), "w") as handle:
            json.dump(data, handle)

    def collect(self, jobs_dirs=(), **options):
        return top.collect_local(self.worker_dir, list(jobs_dirs), self.now, probe=LocalProbe(), **options)

    def test_running_suspended_starting_lost_and_recently_finished_attempts(self):
        n = self.now
        self.journal(42, 1, {"command": "./sim", "cpus": [0, 1, 2, 3], "mem": 8 * GiB},
                     {"state": "running", "started_at": n - 4353})
        self.journal(45, 1, {"cpus": [4, 5], "mem": 2 * GiB},
                     {"state": "running", "started_at": n - 612, "suspended": True, "suspended_since": n - 8,
                      "suspended_seconds": 2.0})
        self.journal(46, 1, {})  # the supervisor has not written yet
        self.journal(40, 2, {"command": "echo ok"},
                     {"state": "finished", "started_at": n - 250, "finished_at": n - 60, "exit_code": 0})
        self.journal(39, 1, {}, {"state": "finished", "started_at": n - 9000, "finished_at": n - 8000, "exit_code": 0})
        self.journal(38, 1, {}, {"state": "running", "started_at": n - 100, "supervisor_pid": self.dead})
        os.makedirs(os.path.join(self.jobs_dir, "notes"))
        os.makedirs(os.path.join(self.jobs_dir, "41", "attempt-x"))
        self.worker()

        data = self.collect()

        found = {(a["job"], a["attempt"]): a for a in data["attempts"]}
        self.assertEqual(sorted(found), [(38, 1), (40, 2), (42, 1), (45, 1), (46, 1)])  # 39 ended two hours ago
        self.assertEqual({key: a["state"] for key, a in found.items()},
                         {(38, 1): "lost", (40, 2): "finished", (42, 1): "running", (45, 1): "suspended",
                          (46, 1): "starting"})
        self.assertAlmostEqual(found[(42, 1)]["elapsed"], 4353.0, delta=1)
        self.assertAlmostEqual(found[(45, 1)]["elapsed"], 602.0, delta=1)  # 612 s minus 2 + 8 s frozen
        self.assertAlmostEqual(found[(40, 2)]["elapsed"], 190.0, delta=1)
        self.assertEqual((data["node"], data["channel"]["state"], data["channel"]["coordinator"]),
                         ("desktop", "connected", "user@lab"))
        self.assertEqual(data["resources"], {"cores": 28, "cores_used": 6, "cores_fila": 6, "mem": 16 * GiB,
                                             "mem_used": 9 * GiB, "mem_fila": 10 * GiB, "in_use": False})
        self.assertIn((39, 1), {(a["job"], a["attempt"]) for a in self.collect(show_finished=True)["attempts"]})
        json.dumps(data)

    def test_a_dead_worker_is_not_running_and_a_stopped_one_says_so(self):
        self.worker(pid=self.dead)
        self.assertEqual(self.collect()["channel"]["state"], "not running")
        self.worker(pid=self.dead, state="stopped")
        self.assertEqual(self.collect()["channel"]["state"], "stopped")

    def test_without_a_worker_the_jobs_dir_comes_from_the_command_line(self):
        self.journal(7, 1, {}, {"state": "running", "started_at": self.now - 5})

        self.assertEqual(self.collect()["attempts"], [])
        data = self.collect(jobs_dirs=[self.jobs_dir])
        self.assertEqual(([a["job"] for a in data["attempts"]], data["channel"]), ([7], None))

    def test_the_command_line_needs_no_coordinator_and_opens_no_database(self):
        self.journal(7, 1, {"command": "./bench"}, {"state": "running", "started_at": self.now - 5})
        home = os.path.join(self.root, "home")
        out, err = io.StringIO(), io.StringIO()
        environ = dict(os.environ, FILA_COORDINATOR="nobody@nowhere.invalid")  # client mode would forward

        code = main(["top", "--local", "--once", "--jobs-dir", self.jobs_dir, "--worker-dir", self.worker_dir,
                     "--width", "100"], home=home, stdout=out, stderr=err, environ=environ)

        self.assertEqual((code, err.getvalue()), (0, ""))
        self.assertTrue(any(line.split()[:3] == ["7", "1", "running"] for line in out.getvalue().splitlines()))
        self.assertFalse(os.path.exists(os.path.join(home, "fila.db")))

    def test_local_only_options(self):
        out, err = io.StringIO(), io.StringIO()
        home = os.path.join(self.root, "home")

        self.assertEqual(main(["top", "--once", "--jobs-dir", self.jobs_dir], home=home, stdout=out, stderr=err), 5)
        self.assertEqual(main(["top", "--local", "--once", "--node", "x", "--worker-dir", self.worker_dir],
                              home=home, stdout=out, stderr=err), 5)


if __name__ == "__main__":
    unittest.main()
