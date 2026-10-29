import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from fila.service import execute, install_plan, uninstall_plan


class ServicePlanTest(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-svc-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)

    def test_linux_runs_a_user_unit_that_survives_logout(self):
        plan = install_plan(self.home, system_name="linux", python="/usr/bin/python3")

        unit = next(content for path, content in plan.files.items() if path.endswith(".service"))
        self.assertIn(f"ExecStart=/usr/bin/python3 {os.path.join(self.home, 'server_launcher.py')}", unit)
        self.assertIn("Restart=on-failure", unit)
        self.assertIn("KillMode=process", unit)  # restarting the server leaves the jobs running
        self.assertIn(["systemctl", "--user", "enable", "--now", "fila-server.service"], plan.commands)
        self.assertEqual(plan.commands[-1][:2], ["loginctl", "enable-linger"])

    def test_macos_uses_a_launch_agent_kept_alive(self):
        plan = install_plan(self.home, system_name="macos", python="/usr/bin/python3")

        agent = next(content for path, content in plan.files.items() if path.endswith(".plist"))
        self.assertIn("<string>/usr/bin/python3</string>", agent)
        self.assertIn("<key>KeepAlive</key>", agent)

    def test_windows_task_has_no_time_limit_and_restarts(self):
        plan = install_plan(self.home, system_name="windows", python=r"C:\Python\pythonw.exe")

        task = next(content for path, content in plan.files.items() if path.endswith(".xml"))
        self.assertIn("<ExecutionTimeLimit>PT0S</ExecutionTimeLimit>", task)  # the default would kill it after 72 h
        self.assertIn("<RestartOnFailure>", task)
        self.assertIn("<LogonType>InteractiveToken</LogonType>", task)
        self.assertEqual(plan.commands[0][:2], ["schtasks", "/Create"])

    def test_uninstall_stops_the_service(self):
        self.assertIn(["schtasks", "/Delete", "/TN", "fila-server", "/F"], uninstall_plan(self.home, system_name="windows").commands)
        self.assertIn(["systemctl", "--user", "disable", "--now", "fila-server.service"], uninstall_plan(self.home, system_name="linux").commands)

    def test_launcher_runs_the_server_in_its_home(self):
        plan = install_plan(self.home)
        execute(type(plan)(files={path: content for path, content in plan.files.items() if path.endswith(".py")}))

        launcher = os.path.join(self.home, "server_launcher.py")
        result = subprocess.run([sys.executable, launcher, "--once"], capture_output=True, text=True, timeout=60)

        self.assertEqual(result.returncode, 0, result.stderr)
        with open(os.path.join(self.home, "server.json")) as handle:
            self.assertIn("pid", json.load(handle))
        with open(os.path.join(self.home, "server.log"), encoding="utf-8") as handle:
            self.assertIn("one cycle", handle.read())


if __name__ == "__main__":
    unittest.main()
