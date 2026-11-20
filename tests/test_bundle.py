# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The single-file agent and the TOML reader it carries for Python 3.9 and 3.10."""

import io
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile

from fila._vendor import tomli
from fila.bundle import agent_archive

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

REGISTRY = """\
[general]
probe_interval_s = 10

[nodes.lab]
type = "computer"
access = "local"
os = "linux"
shell = ["/bin/bash", "-lc"]
jobs_dir = "~/fila/jobs"
reserve = { always = { mem = "1G" }, in_use = { cores = 2, mem = "3G" } }
"""


class VendoredTomlTest(unittest.TestCase):
    def test_the_vendored_reader_reads_a_registry(self):
        data = tomli.loads(REGISTRY)

        self.assertEqual(data["general"], {"probe_interval_s": 10})
        self.assertEqual(data["nodes"]["lab"]["reserve"]["in_use"], {"cores": 2, "mem": "3G"})

    @unittest.skipIf(sys.version_info < (3, 11), "tomllib is Python 3.11+")
    def test_it_agrees_with_the_standard_library(self):
        import tomllib

        self.assertEqual(tomli.loads(REGISTRY), tomllib.loads(REGISTRY))

    def test_the_registry_loads_without_tomllib(self):
        # Python 3.9 and 3.10 have no tomllib: the vendored reader must take over.
        home = tempfile.mkdtemp(prefix="fila-toml-")
        self.addCleanup(shutil.rmtree, home, ignore_errors=True)
        with open(os.path.join(home, "nodes.toml"), "w", encoding="utf-8") as handle:
            handle.write(REGISTRY)
        script = (
            "import sys\n"
            "class Block:\n"
            "    def find_spec(self, name, path=None, target=None):\n"
            "        if name == 'tomllib': raise ImportError(name)\n"
            "sys.meta_path.insert(0, Block())\n"
            "from fila.registry import Registry\n"
            f"general, nodes = Registry({os.path.join(home, 'nodes.toml')!r}).load()\n"
            "print(general['probe_interval_s'], sorted(nodes), nodes['lab'].reserve_in_use)\n"
        )

        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60,
                                env=dict(os.environ, PYTHONPATH=SRC))

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.split(" ", 2)[:2], ["10", "['lab']"])


class ArchiveTest(unittest.TestCase):
    def test_the_agent_carries_the_package_and_the_vendored_reader_with_its_license(self):
        names = set(zipfile.ZipFile(io.BytesIO(agent_archive())).namelist())

        self.assertIn("__main__.py", names)
        self.assertIn("fila/cli.py", names)
        self.assertIn("fila/_vendor/tomli/_parser.py", names)
        self.assertIn("fila/_vendor/tomli/LICENSE", names)
        self.assertFalse([name for name in names if "__pycache__" in name or name.endswith(".pyc")])


if __name__ == "__main__":
    unittest.main()
