"""The MCP server: protocol answers, tool-to-command mapping, and a real
``fila mcp`` process spoken to over stdio."""

import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

from fila.cli import main
from fila.mcp import TOOLS, Server, command_line

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")


def rpc(method, id=1, **params):
    return {"jsonrpc": "2.0", "id": id, "method": method, "params": params}


class ProtocolTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.answer = (0, '{"ok": true}', "")

    def server(self, *messages):
        stdin = io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        stdout = io.StringIO()

        def run_fila(argv):
            self.calls.append(argv)
            return self.answer

        Server(run_fila, stdin, stdout).serve()
        return [json.loads(line) for line in stdout.getvalue().splitlines()]

    def test_initialize_negotiates_the_protocol_version(self):
        known, unknown = self.server(
            rpc("initialize", 1, protocolVersion="2025-03-26", capabilities={}),
            rpc("initialize", 2, protocolVersion="1999-01-01", capabilities={}),
        )

        self.assertEqual(known["result"]["protocolVersion"], "2025-03-26")
        self.assertEqual(unknown["result"]["protocolVersion"], "2025-06-18")
        self.assertIn("tools", known["result"]["capabilities"])
        self.assertEqual(known["result"]["serverInfo"]["name"], "fila")

    def test_notifications_get_no_answer_and_unknown_methods_an_error(self):
        answers = self.server({"jsonrpc": "2.0", "method": "notifications/initialized"}, rpc("resources/list", 3), rpc("ping", 4))

        self.assertEqual([a["id"] for a in answers], [3, 4])
        self.assertEqual(answers[0]["error"]["code"], -32601)
        self.assertEqual(answers[1]["result"], {})

    def test_tools_are_listed_with_schemas(self):
        (answer,) = self.server(rpc("tools/list"))

        names = [tool["name"] for tool in answer["result"]["tools"]]
        self.assertEqual(names, ["run", "submit", "status", "resources", "result", "log", "explain", "wait", "cancel", "retry"])
        submit = next(tool for tool in answer["result"]["tools"] if tool["name"] == "submit")
        self.assertEqual(submit["inputSchema"]["required"], ["command", "cores", "mem"])

    def test_a_tool_call_runs_the_command_line_and_returns_its_json(self):
        (answer,) = self.server(rpc("tools/call", name="status", arguments={"all": True}))

        self.assertEqual(self.calls, [["status", "--json", "--all"]])
        text = json.loads(answer["result"]["content"][0]["text"])
        self.assertEqual((text["exit_code"], text["result"], answer["result"]["isError"]), (0, {"ok": True}, False))

    def test_refusals_are_tool_errors_but_failed_jobs_are_results(self):
        self.answer = (4, "", "fila: refused: no node can hold cores=999")
        (refused,) = self.server(rpc("tools/call", name="submit", arguments={"command": "x", "cores": 999, "mem": "1G"}))
        self.answer = (1, '[{"id": 3, "state": "failed"}]', "")
        (failed,) = self.server(rpc("tools/call", name="wait", arguments={"jobs": [3]}))

        self.assertTrue(refused["result"]["isError"])
        self.assertIn("no node can hold", json.loads(refused["result"]["content"][0]["text"])["stderr"])
        self.assertFalse(failed["result"]["isError"])


class MappingTest(unittest.TestCase):
    def test_submit_maps_every_option_and_keeps_the_command_after_the_separator(self):
        argv = command_line("submit", {"command": "make && ./run --fast", "cores": 4, "mem": "2G", "time": "2h",
                                       "after": [1, 2], "on": "lab", "slurm_extra": "--constraint=avx512", "dry_run": True})

        self.assertEqual(argv[:6], ["submit", "--json", "--cores", "4", "--mem", "2G"])
        self.assertIn("--after", argv)
        self.assertEqual(argv[argv.index("--after") + 1], "1,2")
        self.assertIn("--dry-run", argv)
        self.assertEqual(argv[-2:], ["--", "make && ./run --fast"])

    def test_wait_is_capped_to_minutes(self):
        self.assertEqual(command_line("wait", {"jobs": [5], "timeout": "3d"})[-2:], ["--timeout", "10m"])
        self.assertEqual(command_line("wait", {"jobs": [5], "timeout": "30s"})[-2:], ["--timeout", "30s"])
        self.assertEqual(command_line("wait", {"jobs": [5]})[-2:], ["--timeout", "10m"])

    def test_every_listed_tool_has_a_mapping(self):
        minimal = {"command": "x", "on": "all", "cores": 1, "mem": "1G", "job": 1, "jobs": [1]}
        for tool in TOOLS:
            with self.subTest(tool=tool["name"]):
                self.assertEqual(command_line(tool["name"], minimal)[0], tool["name"])


class ProcessTest(unittest.TestCase):
    def test_a_real_fila_mcp_process_answers_over_stdio(self):
        home = tempfile.mkdtemp(prefix="fila-mcp-")
        self.addCleanup(shutil.rmtree, home, ignore_errors=True)
        messages = [
            rpc("initialize", 1, protocolVersion="2025-06-18", capabilities={}, clientInfo={"name": "test"}),
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            rpc("tools/call", 2, name="resources", arguments={}),
            rpc("tools/call", 3, name="result", arguments={"job": 99}),
        ]
        env = dict(os.environ, FILA_HOME=home, PYTHONPATH=SRC)

        result = subprocess.run([sys.executable, "-m", "fila", "mcp"], input="".join(json.dumps(m) + "\n" for m in messages).encode(),
                                capture_output=True, env=env, timeout=60)

        self.assertNotIn(b"\r", result.stdout)  # one JSON message per \n line, even on Windows
        answers = {a["id"]: a for a in map(json.loads, result.stdout.decode().splitlines())}
        self.assertEqual(answers[1]["result"]["serverInfo"]["name"], "fila")
        self.assertEqual(json.loads(answers[2]["result"]["content"][0]["text"])["result"], [])
        self.assertTrue(answers[3]["result"]["isError"])  # unknown job: usage error, exit code 5


class GuideTest(unittest.TestCase):
    def test_guide_explains_run_and_submit(self):
        out = io.StringIO()

        self.assertEqual(main(["guide"], stdout=out), 0)
        self.assertIn("fila submit --cores", out.getvalue())
        self.assertIn("fila wait", out.getvalue())


if __name__ == "__main__":
    unittest.main()
