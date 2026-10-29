"""Stands in for ``ssh HOST COMMAND`` against a "remote" computer that is this
machine: COMMAND runs in bash with HOME set to ``FAKE_REMOTE_HOME``, stdin
passed through, like a login on another node. ``FAKE_SSH_FAIL=1`` makes it
fail like an unreachable host (exit code 255)."""

import os
import shutil
import subprocess
import sys


def bash() -> str:
    for candidate in (r"C:\Program Files\Git\usr\bin\bash.exe", r"C:\msys64\usr\bin\bash.exe", "/bin/bash"):
        if os.path.exists(candidate):
            return candidate
    return shutil.which("bash")


def main() -> int:
    if os.environ.get("FAKE_SSH_FAIL"):
        sys.stderr.write("ssh: connect to host far: Connection timed out\n")
        return 255
    command = sys.argv[2]
    remote_home = os.environ["FAKE_REMOTE_HOME"]
    # bash's cd uses HOME; Python's ~ on Windows uses USERPROFILE: both point at the remote home.
    env = dict(os.environ, HOME=remote_home.replace("\\", "/"), USERPROFILE=remote_home)
    for name in ("FILA_HOME", "FILA_COORDINATOR", "PYTHONPATH"):  # nothing of the coordinator leaks to the node
        env.pop(name, None)
    if os.environ.get("FAKE_REMOTE_PATH"):  # e.g. the fake Slurm commands on a "login node"
        command = f'export PATH="{posix_path(os.environ["FAKE_REMOTE_PATH"])}:$PATH"; {command}'
    return subprocess.run([bash(), "-c", command], env=env).returncode


def posix_path(path: str) -> str:
    """``C:\\x\\y`` to ``/c/x/y``, as bash on Windows expects in PATH."""
    path = path.replace("\\", "/")
    if len(path) > 1 and path[1] == ":":
        return f"/{path[0].lower()}{path[2:]}"
    return path


if __name__ == "__main__":
    sys.exit(main())
