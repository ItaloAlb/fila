"""Stands in for a pull node's ``ssh user@coordinator COMMAND``: like sshd with
the node's restricted key, it ignores COMMAND's program and starts the
channel for the coordinator home in ``FAKE_COORDINATOR_HOME``.

Like a real SSH connection, it relays the bytes instead of handing its own
stdin/stdout to the channel: when this process dies, the channel sees the
end of its input at once, as sshd would make it. ``FAKE_SSH_FAIL=1`` fails
like an unreachable host."""

import os
import shlex
import subprocess
import sys
import threading

SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")


def relay(source, target, close_target: bool) -> None:
    try:
        while True:
            chunk = source.read1(65536) if hasattr(source, "read1") else source.read(65536)
            if not chunk:
                break
            target.write(chunk)
            target.flush()
    except (OSError, ValueError):
        pass
    finally:
        if close_target:
            try:
                target.close()
            except OSError:
                pass


def main() -> int:
    if os.environ.get("FAKE_SSH_FAIL"):
        sys.stderr.write("ssh: connect to host lab: Connection timed out\n")
        return 255
    words = shlex.split(sys.argv[-1])
    node = words[words.index("--node") + 1]
    env = dict(os.environ, FILA_HOME=os.environ["FAKE_COORDINATOR_HOME"], PYTHONPATH=SRC)
    channel = subprocess.Popen([sys.executable, "-m", "fila", "internal", "channel", "--node", node], env=env,
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    threading.Thread(target=relay, args=(sys.stdin.buffer, channel.stdin, True), daemon=True).start()
    relay(channel.stdout, sys.stdout.buffer, False)
    return channel.wait()


if __name__ == "__main__":
    sys.exit(main())
