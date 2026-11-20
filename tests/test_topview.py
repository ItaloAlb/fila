# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The interactive ``fila top`` without a terminal: keys in, screens out."""

import json
import os
import shutil
import tempfile
import unittest

from fila.topview import View

from tests.test_top import job, local_attempt, local_snapshot, node, snapshot


class Recorder:
    """A fake command line: remembers what was asked and answers canned text."""

    def __init__(self):
        self.calls = []

    def __call__(self, argv):
        self.calls.append(argv)
        if argv[0] == "log":
            return 0, json.dumps({"path": "/x", "attempt": 1, "tail": "\n".join(f"line {i}" for i in range(50))}), ""
        return 0, f"{argv[0]} of job {argv[-1]}\nsecond line\n", ""


class ViewCase(unittest.TestCase):
    def view(self, jobs=None, nodes=None, local=False, **snapshot_fields):
        self.fetched = []
        self.ask = Recorder()

        def fetch(show_finished):
            self.fetched.append(show_finished)
            if local:
                return local_snapshot(**snapshot_fields)
            return snapshot(nodes=nodes, jobs=jobs)

        view = View(fetch, self.ask, local=local, interval=2)
        view.refresh()
        return view

    def keys(self, view, *keys):
        for key in keys:
            view.handle(key)

    def selected_line(self, view, width=100, height=30):
        lines, highlight = view.screen(width, height)
        return lines[highlight] if highlight is not None else None


class MovingTest(ViewCase):
    def test_the_first_job_is_selected_and_the_arrows_move(self):
        view = self.view(jobs=[job(1), job(2), job(3)])

        self.assertEqual(view.selected, 1)
        self.assertTrue(self.selected_line(view).startswith(">  1"))
        self.keys(view, "down", "j")
        self.assertEqual(view.selected, 3)
        self.keys(view, "down")  # stays at the end
        self.assertEqual(view.selected, 3)
        self.keys(view, "home")
        self.assertEqual(view.selected, 1)
        self.keys(view, "end", "k")
        self.assertEqual(view.selected, 2)

    def test_the_selection_follows_its_job_across_refreshes(self):
        jobs = [job(1), job(2), job(3)]
        view = self.view(jobs=jobs)
        self.keys(view, "down")
        jobs.pop(0)  # job 1 ended and left the list

        view.refresh()

        self.assertEqual(view.selected, 2)

    def test_a_long_list_scrolls_to_keep_the_selection_on_screen(self):
        view = self.view(jobs=[job(i) for i in range(1, 41)])

        for _ in range(30):
            view.handle("down")
        lines, highlight = view.screen(100, 20)

        self.assertEqual(len(lines), 20)
        self.assertIsNotNone(highlight)
        self.assertTrue(lines[highlight].startswith("> 31"), lines[highlight])
        view.handle("pgup")
        view.screen(100, 20)
        self.assertLess(view.selected, 31)

    def test_tab_moves_to_the_nodes_and_enter_shows_one(self):
        view = self.view(nodes=[node("lab"), node("desktop")], jobs=[job(1, node="desktop")])

        self.keys(view, "tab", "down")
        self.assertEqual(view.selected_node, "desktop")
        self.assertTrue(self.selected_line(view).startswith("desktop"))
        self.keys(view, "enter")

        lines, _ = view.screen(100, 30)
        self.assertEqual(lines[0], "node desktop")
        self.assertIn("jobs here: 1", lines)


class PagesTest(ViewCase):
    def test_enter_shows_fila_result_and_esc_comes_back(self):
        view = self.view(jobs=[job(7)])

        self.keys(view, "enter")

        lines, highlight = view.screen(100, 20)
        self.assertEqual(self.ask.calls, [["result", "7"]])
        self.assertEqual((lines[0], lines[2], highlight), ("job 7: fila result", "result of job 7", None))
        self.keys(view, "esc")
        self.assertEqual(view.mode, "main")
        self.assertFalse(view.done)

    def test_the_log_follows_its_end_and_tab_switches_to_stderr(self):
        view = self.view(jobs=[job(7)])

        self.keys(view, "l")
        lines, _ = view.screen(100, 20)

        self.assertEqual(self.ask.calls[-1], ["log", "--json", "7"])
        self.assertEqual(lines[-3], "line 49")  # the end is shown: following
        self.keys(view, "up")
        self.assertNotEqual(view.screen(100, 20)[0][-3], "line 49")  # scrolled back: no longer following
        self.keys(view, "end")
        self.assertEqual(view.screen(100, 20)[0][-3], "line 49")
        self.keys(view, "tab")
        self.assertEqual(self.ask.calls[-1], ["log", "--json", "7", "--stderr"])

    def test_pages_are_reloaded_on_refresh(self):
        view = self.view(jobs=[job(7)])
        self.keys(view, "e")
        self.assertEqual(self.ask.calls, [["explain", "7"]])

        view.refresh()

        self.assertEqual(self.ask.calls, [["explain", "7"], ["explain", "7"]])

    def test_help_lists_the_keys(self):
        view = self.view()

        self.keys(view, "?")

        self.assertIn("Tab                switch between the jobs and the nodes", "\n".join(view.screen(100, 40)[0]))

    def test_without_jobs_the_pages_say_so(self):
        view = self.view(jobs=[])

        for key in ("enter", "l", "e"):
            self.keys(view, key)
            self.assertEqual(view.mode, "main")
            self.assertEqual(view.message, "no job selected")


class ChoosingTest(ViewCase):
    def test_a_toggles_finished_jobs_and_asks_for_a_refresh(self):
        view = self.view()

        self.keys(view, "a")
        self.assertTrue(view.wants_refresh)
        view.refresh()

        self.assertEqual(self.fetched, [False, True])

    def test_o_cycles_the_order(self):
        view = self.view(jobs=[job(1, elapsed=10.0), job(2, elapsed=500.0)])

        self.keys(view, "o", "o", "o")

        self.assertEqual(view.order, "elapsed")
        self.assertEqual(view.screen(100, 20)[0][-1], "order: elapsed")
        self.assertEqual([row["id"] for row in view.rows()], [2, 1])

    def test_f_filters_by_what_is_typed(self):
        view = self.view(jobs=[job(1, command="./sim"), job(2, command="./bench")])

        self.keys(view, "f", "b", "e", "x", "backspace", "n")
        self.assertIn("filter: ben_", view.screen(100, 20)[0][-1])
        self.keys(view, "enter")

        self.assertEqual((view.text_filter, view.selected), ("ben", 2))
        self.keys(view, "f", "esc")
        self.assertEqual(view.text_filter, "ben")

    def test_q_and_esc_quit_from_the_list(self):
        for key in ("q", "esc", "quit"):
            with self.subTest(key=key):
                view = self.view()
                self.keys(view, key)
                self.assertTrue(view.done)

    def test_an_unknown_key_says_how_to_get_help(self):
        view = self.view()

        self.keys(view, "z")

        self.assertIn("? shows the keys", view.screen(100, 20)[0][-1])

    def test_a_failed_refresh_keeps_the_last_screen(self):
        view = self.view(jobs=[job(1)])
        view.fetch = lambda show_finished: (_ for _ in ()).throw(RuntimeError("database is locked"))

        view.refresh()

        self.assertEqual(view.selected, 1)
        self.assertEqual(view.screen(100, 20)[0][-1], "refresh failed: database is locked")


class LocalViewTest(ViewCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="fila-view-")
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        directory = os.path.join(self.root, "42", "attempt-1")
        os.makedirs(directory)
        with open(os.path.join(directory, "stdout.log"), "w") as handle:
            handle.write("hello\nworld\n")
        with open(os.path.join(directory, "job.json"), "w") as handle:
            json.dump({"command": "./sim", "cpus": [0, 1]}, handle)

    def attempts(self):
        return [dict(local_attempt(42, cpus=[0, 1], command="./sim"), jobs_dir=self.root),
                dict(local_attempt(45, state="suspended", command="./bench"), jobs_dir=self.root)]

    def test_the_attempts_are_selected_and_their_log_read_from_the_node(self):
        view = self.view(local=True, attempts=self.attempts())

        self.assertEqual(view.selected, (42, 1))
        self.keys(view, "l")
        lines, _ = view.screen(100, 12)

        self.assertEqual(lines[0], "job 42, attempt 1: stdout.log (following)")
        self.assertIn("world", lines)
        self.assertEqual(self.ask.calls, [])  # nothing goes to the coordinator

    def test_enter_shows_the_attempt_files_and_e_points_to_the_coordinator(self):
        view = self.view(local=True, attempts=self.attempts())

        self.keys(view, "enter")
        text = "\n".join(view.screen(100, 40)[0])
        self.assertIn('"command": "./sim"', text)
        self.assertIn("status.json:", text)

        self.keys(view, "esc", "e")
        self.assertIn("needs the coordinator", view.message)
        self.keys(view, "tab")
        self.assertEqual(view.focus, "jobs")

    def test_the_local_footer_and_help(self):
        view = self.view(local=True, attempts=self.attempts())

        lines, highlight = view.screen(100, 20)

        self.assertIn("are done on the coordinator", lines[-1])
        self.assertTrue(lines[highlight].startswith(">  42"), lines[highlight])
        self.keys(view, "?")
        self.assertIn("undone by the server", "\n".join(view.screen(100, 40)[0]))


if __name__ == "__main__":
    unittest.main()
