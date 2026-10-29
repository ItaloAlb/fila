"""Stands in for ``ssh HOST COMMAND`` in tests: parses COMMAND the way a POSIX
remote shell would and runs it here, against the coordinator home in
``FAKE_SSH_HOME``. ``FAKE_SSH_FAIL=1`` makes it fail like an unreachable host."""

import os
import shlex
import subprocess
import sys

if os.environ.get("FAKE_SSH_FAIL"):
    sys.stderr.write("ssh: connect to host lab: Connection timed out\n")
    sys.exit(255)

host, command = sys.argv[1], sys.argv[2]
env = dict(os.environ, FILA_HOME=os.environ["FAKE_SSH_HOME"])
env.pop("FILA_COORDINATOR", None)
sys.exit(subprocess.run(shlex.split(command), env=env).returncode)
