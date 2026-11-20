# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Console programs started by a process without a console (the Windows
services run under pythonw.exe) must not open windows on the desktop."""

import os
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from fila import channel, cli, console, executor, slurm

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
HIDDEN = console.CREATE_NO_WINDOW


class NoWindowTest(unittest.TestCase):
    def test_only_windows_without_a_console_hides_its_children(self):
        cases = [("linux", False, 0), ("darwin", False, 0), ("win32", True, 0), ("win32", False, HIDDEN)]
        for platform, has_console, expected in cases:
            with self.subTest(platform=platform, has_console=has_console):
                with mock.patch.object(sys, "platform", platform), \
                        mock.patch.object(console, "_has_console", return_value=has_console):
                    self.assertEqual(console.no_window(), expected)

    @unittest.skipUnless(sys.platform == "win32", "Windows consoles")
    def test_a_process_without_a_console_asks_for_hidden_children(self):
        detached = 0x00000008  # DETACHED_PROCESS: no console, like pythonw.exe
        program = "from fila.console import no_window; print(no_window())"

        result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=60,
                                creationflags=detached, env=dict(os.environ, PYTHONPATH=SRC))

        self.assertEqual(result.stdout.strip(), str(HIDDEN), result.stderr)


class CallSitesTest(unittest.TestCase):
    """Every place that starts ssh passes the flag."""

    def setUp(self):
        for module in (executor, slurm, channel, cli):
            patcher = mock.patch.object(module, "no_window", return_value=HIDDEN)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_the_ssh_executor(self):
        done = subprocess.CompletedProcess([], 0, b'{"ok": true, "result": {}}', b"")
        with mock.patch.object(executor.subprocess, "run", return_value=done) as run:
            executor.SshExecutor("lab", "lab", "~/jobs", ["/bin/sh", "-c"])._ssh("true")

        self.assertEqual(run.call_args.kwargs["creationflags"], HIDDEN)

    def test_the_cluster_commands(self):
        done = subprocess.CompletedProcess([], 0, b"", b"")
        with mock.patch.object(slurm.subprocess, "run", return_value=done) as run:
            slurm._remote({"ssh_command": ["ssh"], "host": "cluster"}, "true")

        self.assertEqual(run.call_args.kwargs["creationflags"], HIDDEN)

    def test_the_pull_workers_channel(self):
        home = tempfile.mkdtemp(prefix="fila-console-")
        self.addCleanup(lambda: __import__("shutil").rmtree(home, ignore_errors=True))
        with mock.patch.object(channel.subprocess, "Popen", side_effect=OSError("no ssh")) as popen:
            reason = channel._session("user@lab", "desktop", ["ssh"], "fila", threading.Event(), home, None)

        self.assertEqual(reason, "cannot start ssh: no ssh")
        self.assertEqual(popen.call_args.kwargs["creationflags"], HIDDEN)

    def test_client_mode(self):
        with mock.patch.object(cli.subprocess, "Popen", side_effect=OSError("no ssh")) as popen:
            code = cli._forward({"coordinator": "user@lab"}, ["status"], mock.Mock(), mock.Mock(), {})

        self.assertEqual(code, cli.EXIT_UNAVAILABLE)
        self.assertEqual(popen.call_args.kwargs["creationflags"], HIDDEN)


if __name__ == "__main__":
    unittest.main()
