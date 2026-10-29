"""Pull nodes (section 4.3): the node opens and keeps an SSH session to the
coordinator, and requests travel down it.

Three parts:

- ``serve_channel``: the coordinator's end, the process the node's SSH login
  starts (``fila internal channel --node <name>``, forced by the restricted
  key). It meets the server in the database: the server writes requests,
  this process sends them and stores the answers.
- ``run_worker``: the node's end (``fila worker run``), which reconnects with
  growing waits and answers with the agent's operations.
- ``PullExecutor``: what the server and the command line use, like any executor.

Messages are one JSON object per line. Coordinator to node: ``request``
(``id``, ``operation``, ``args``) and ``ping``. Node to coordinator:
``hello``, ``response`` (``id``, ``answer``), ``ping`` and ``event``
(``wake``: an attempt ended).
"""

from __future__ import annotations

import base64
import json
import os
import shlex
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import BinaryIO, Callable, List, Optional

from . import __version__, agent
from .errors import AgentError, NodeUnreachable
from .executor import Executor

HEARTBEAT_S = 15.0
DEAD_AFTER_S = 45.0
POLL_S = 0.05
RECONNECT_DELAYS_S = (5, 10, 30, 60, 120, 300)
SLOW_OPERATIONS = ("selftest", "benchmark", "time_command", "install_agent")


def _line(message: dict) -> bytes:
    return json.dumps(message).encode("utf-8") + b"\n"


# --- The coordinator's end ---


def serve_channel(store, node: str, stdin: BinaryIO, stdout: BinaryIO, wake_path: str,
                  pid: Optional[int] = None) -> None:
    """Relay requests for ``node`` until the session ends, goes silent for
    ``DEAD_AFTER_S`` or a newer connection of the same node takes over."""
    pid = pid or os.getpid()
    heard = [time.time()]
    store.channel_open(node, pid, heard[0])
    lock = threading.Lock()
    closed = threading.Event()

    def send(message: dict) -> None:
        with lock:
            stdout.write(_line(message))
            stdout.flush()

    def read() -> None:
        try:
            for raw in stdin:
                try:
                    message = json.loads(raw.decode("utf-8"))
                except ValueError:
                    continue
                heard[0] = time.time()
                kind = message.get("type")
                if kind == "response":
                    store.channel_answer(node, int(message["id"]), message.get("answer") or {})
                elif kind == "event" and message.get("name") == "wake":
                    with open(wake_path, "w") as handle:  # the server reconciles now
                        handle.write(str(heard[0]))
        finally:
            closed.set()

    threading.Thread(target=read, daemon=True).start()
    next_ping = time.time() + HEARTBEAT_S
    next_seen = 0.0
    try:
        while not closed.is_set():
            now = time.time()
            if now - heard[0] > DEAD_AFTER_S:
                break
            if now >= next_seen:
                if not store.channel_seen(node, pid, heard[0]):
                    break  # another connection of this node took over
                next_seen = now + 1.0
            for request in store.channel_take(node):
                send(dict(type="request", **request))
            if now >= next_ping:
                send({"type": "ping"})
                next_ping = now + HEARTBEAT_S
            closed.wait(POLL_S)
    except OSError:  # the session broke while writing
        pass
    finally:
        store.channel_close(node, pid, time.time())


# --- The node's end ---


def run_worker(coordinator: str, node: str, ssh_command: List[str], remote_command: str = "fila",
               stop: Optional[threading.Event] = None, home_dir: Optional[str] = None,
               on_install: Optional[Callable[[], None]] = None, log=None) -> None:
    """Keep a channel open to the coordinator, reconnecting after 5 s, 10 s,
    30 s... up to 5 min; a connection that lasted a minute resets the waits."""
    stop = stop or threading.Event()
    home_dir = home_dir or os.path.expanduser("~/.fila")
    log = log or sys.stderr
    failures = 0
    while not stop.is_set():
        started = time.time()
        reason = _session(coordinator, node, ssh_command, remote_command, stop, home_dir, on_install)
        if stop.is_set():
            return
        failures = 0 if time.time() - started > 60 else failures + 1
        delay = RECONNECT_DELAYS_S[min(max(failures, 1), len(RECONNECT_DELAYS_S)) - 1]
        log.write(f"fila worker {time.strftime('%Y-%m-%d %H:%M:%S')}: channel closed ({reason}); retrying in {delay} s\n")
        log.flush()
        stop.wait(delay)


def _session(coordinator: str, node: str, ssh_command: List[str], remote_command: str,
             stop: threading.Event, home_dir: str, on_install) -> str:
    os.makedirs(home_dir, exist_ok=True)
    wake_file = os.path.join(home_dir, "wake")
    command = f"{remote_command} internal channel --node {shlex.quote(node)}"
    try:
        process = subprocess.Popen(
            list(ssh_command) + [coordinator, command],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=open(os.path.join(home_dir, "ssh.log"), "ab"),
        )
    except OSError as error:
        return f"cannot start ssh: {error}"
    lock = threading.Lock()
    heard = [time.time()]
    pool = ThreadPoolExecutor(max_workers=8)
    reinstall = threading.Event()

    def send(message: dict) -> None:
        with lock:
            try:
                process.stdin.write(_line(message))
                process.stdin.flush()
            except OSError:
                pass

    def answer(message: dict) -> None:
        operation, args = message.get("operation"), dict(message.get("args") or {})
        try:
            if operation == "install_agent":
                path = os.path.join(home_dir, "fila.pyz")
                with open(path + ".tmp", "wb") as handle:
                    handle.write(base64.b64decode(args["archive"]))
                os.replace(path + ".tmp", path)
                result = {"ok": True, "result": {"path": path}}
                reinstall.set()
            else:
                if operation == "launch":
                    args["notify"] = wake_file  # the supervisor wakes this worker, which wakes the server
                result = {"ok": True, "result": agent.OPERATIONS[operation](args)}
        except NodeUnreachable as error:
            result = {"ok": False, "kind": "unreachable", "error": str(error)}
        except Exception as error:  # reported to the coordinator, which decides what it means
            result = {"ok": False, "error": f"{type(error).__name__}: {error}"}
        send({"type": "response", "id": message["id"], "answer": result})

    def keepalive() -> None:
        last_wake = _mtime(wake_file)
        next_ping = time.time() + HEARTBEAT_S
        while process.poll() is None:
            if stop.is_set() or time.time() - heard[0] > DEAD_AFTER_S or reinstall.is_set():
                process.kill()
                return
            if time.time() >= next_ping:
                send({"type": "ping"})
                next_ping = time.time() + HEARTBEAT_S
            if _mtime(wake_file) != last_wake:
                last_wake = _mtime(wake_file)
                send({"type": "event", "name": "wake"})
            time.sleep(0.25)

    send({"type": "hello", "node": node, "protocol": agent.PROTOCOL, "fila": __version__})
    threading.Thread(target=keepalive, daemon=True).start()
    try:
        for raw in process.stdout:
            heard[0] = time.time()
            try:
                message = json.loads(raw.decode("utf-8"))
            except ValueError:
                continue
            if message.get("type") == "request":
                pool.submit(answer, message)
    finally:
        try:
            process.stdin.close()  # the coordinator's end sees the session end
        except OSError:
            pass
        process.kill()
        process.wait()
        pool.shutdown(wait=False)
    if reinstall.is_set() and on_install is not None:
        on_install()  # e.g. start again from the new agent
    return "agent updated" if reinstall.is_set() else f"ssh exited with {process.returncode}"


def _mtime(path: str) -> Optional[float]:
    try:
        return os.path.getmtime(path)
    except OSError:
        return None


# --- The executor ---


class PullExecutor(Executor):
    """Requests through the channel the node keeps open; the same methods as
    the other executors."""

    def __init__(self, node: str, jobs_dir: str, shell: List[str], store, timeout: float = 60.0) -> None:
        self.node = node
        self.jobs_dir = jobs_dir
        self.shell = list(shell)
        self.store = store
        self.timeout = timeout
        self.notify = None  # set by the worker, on the node

    def connected(self) -> bool:
        from .supervisor import pid_alive

        channel = self.store.channel(self.node)
        return (
            channel is not None
            and time.time() - channel["last_seen"] < DEAD_AFTER_S
            and pid_alive(channel["pid"])
        )

    def call(self, operation: str, args: dict):
        if not self.connected():
            raise NodeUnreachable(f"{self.node}: no channel (the node's worker is not connected)")
        request = self.store.channel_request(self.node, operation, args, time.time())
        timeout = max(self.timeout, 600.0) if operation in SLOW_OPERATIONS else self.timeout
        deadline = time.time() + timeout
        while time.time() < deadline:
            answer = self.store.channel_result(request)
            if answer is not None:
                if answer.get("ok"):
                    return answer["result"]
                if answer.get("kind") == "unreachable":
                    raise NodeUnreachable(answer.get("error"))
                raise AgentError(f"{self.node}: {answer.get('error')}")
            time.sleep(0.02)
        self.store.channel_forget(request)
        raise NodeUnreachable(f"{self.node}: no answer in {timeout:.0f} s")

    def install_agent(self, archive: bytes) -> None:
        self.call("install_agent", {"archive": base64.b64encode(archive).decode("ascii")})
