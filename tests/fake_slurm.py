"""A small Slurm for tests: the commands fila uses, answering like the real
ones, with the cluster's state in the JSON file ``FAKE_SLURM_STATE``.

Run as ``python fake_slurm.py <command> <args>`` (wrapper scripts named
``sbatch``, ``squeue``... call it). Tests move the cluster with
``run_pending`` and ``finish``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time

COMMANDS = ("sinfo", "scontrol", "sacctmgr", "sbatch", "squeue", "sacct", "scancel")


def new_state(path: str, nodes: dict, max_cpus_per_node: str = "UNLIMITED", qos_cpu: str = "") -> None:
    """``nodes``: name -> (total CPUs, memory MB, allocated CPUs)."""
    state = {
        "next_id": 1000,
        "partitions": {"cpuq": {"nodes": sorted(nodes), "MaxCPUsPerNode": max_cpus_per_node,
                                "MaxTime": "7-00:00:00", "QoS": "normal"}},
        "qos": {"normal": {"MaxTRES": f"cpu={qos_cpu}" if qos_cpu else "", "MaxWall": ""}},
        "nodes": {name: {"CPUTot": cpus, "RealMemory": mem, "CPUAlloc": alloc, "AllocMem": 0, "State": "IDLE"}
                  for name, (cpus, mem, alloc) in nodes.items()},
        "jobs": {},
    }
    _save(path, state)


def _load(path: str) -> dict:
    with open(path) as handle:
        return json.load(handle)


def _save(path: str, state: dict) -> None:
    with open(path + ".tmp", "w") as handle:
        json.dump(state, handle, indent=1)
    os.replace(path + ".tmp", path)


def windows_path(path: str) -> str:
    """A path as bash on Windows prints it (``/c/x``, ``/tmp/x``) for Python; POSIX systems unchanged."""
    if os.name != "nt" or not path.startswith("/"):
        return path
    bash = os.environ.get("FAKE_SLURM_BASH", "")
    cygpath = os.path.join(os.path.dirname(bash), "cygpath.exe")
    return subprocess.run([cygpath, "-m", path], capture_output=True, text=True, check=True).stdout.strip()


def _stamp(seconds) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(seconds)) if seconds else "Unknown"


def _options(script_path: str) -> dict:
    options = {}
    with open(windows_path(script_path)) as handle:
        for line in handle:
            match = re.match(r"#SBATCH\s+--([\w-]+)(?:=(.*))?", line.strip())
            if match:
                options[match.group(1)] = match.group(2) or ""
    return options


def run_pending(path: str, bash: str) -> None:
    """Start every pending job and run it to its end, like a free cluster would."""
    state = _load(path)
    for job_id, job in state["jobs"].items():
        if job["state"] != "PENDING":
            continue
        job.update(state="RUNNING", start=time.time())
        _save(path, state)
        options = job["options"]
        with open(windows_path(options["output"]), "w") as out, open(windows_path(options["error"]), "w") as err:
            code = subprocess.run([bash, job["script"]], cwd=windows_path(options["chdir"]), stdout=out, stderr=err).returncode
        job.update(state="COMPLETED" if code == 0 else "FAILED", exit=f"{code}:0", end=time.time())
        _save(path, state)


def finish(path: str, job_id: str, slurm_state: str, exit_code: str = "0:0") -> None:
    state = _load(path)
    job = state["jobs"][job_id]
    job.update(state=slurm_state, exit=exit_code, start=job["start"] or time.time(), end=time.time())
    _save(path, state)


def start(path: str, job_id: str) -> None:
    state = _load(path)
    state["jobs"][job_id].update(state="RUNNING", start=time.time())
    _save(path, state)


def main(argv) -> int:
    command, args = argv[0], argv[1:]
    path = os.environ["FAKE_SLURM_STATE"]
    state = _load(path)
    if command == "sinfo":
        partition = args[args.index("-p") + 1]
        print("\n".join(state["partitions"].get(partition, {}).get("nodes", [])))
    elif command == "scontrol" and args[:2] == ["show", "node"]:
        for name, node in state["nodes"].items():
            print(f"NodeName={name} " + " ".join(f"{key}={value}" for key, value in node.items()) + " OS=Linux 5.4 #1 SMP")
    elif command == "scontrol" and args[:2] == ["show", "partition"]:
        partition = state["partitions"].get(args[2])
        if partition is None:
            print(f"Partition {args[2]} not found", file=sys.stderr)
            return 1
        print(f"PartitionName={args[2]} Nodes={','.join(partition['nodes'])} MaxNodes=UNLIMITED "
              f"MaxTime={partition['MaxTime']} MaxCPUsPerNode={partition['MaxCPUsPerNode']} QoS={partition['QoS']}")
    elif command == "sacctmgr":
        for name, qos in state["qos"].items():
            print(f"{name}|{qos['MaxTRES']}|{qos['MaxWall']}")
    elif command == "sbatch":
        script = args[-1]
        options = _options(script)
        if options.get("partition") not in state["partitions"]:
            print("sbatch: error: invalid partition specified", file=sys.stderr)
            return 1
        job_id = str(state["next_id"])
        state["next_id"] += 1
        state["jobs"][job_id] = {"state": "PENDING", "script": script, "options": options, "exit": "0:0",
                                 "start": None, "end": None, "name": options.get("job-name", "")}
        _save(path, state)
        print(job_id)
        if "--wait" in args:
            run_pending(path, os.environ["FAKE_SLURM_BASH"])
            return int(_load(path)["jobs"][job_id]["exit"].split(":")[0])
    elif command == "squeue":
        name = args[args.index("-n") + 1] if "-n" in args else None
        for job_id, job in state["jobs"].items():
            if job["state"] in ("PENDING", "RUNNING") and (name is None or job["name"] == name):
                print(job["state"] if name else f"{job_id}|{job['state']}")
    elif command == "sacct":
        ids = args[args.index("-j") + 1].split(",")
        fields = args[args.index("-o") + 1]
        for job_id in ids:
            job = state["jobs"].get(job_id)
            if job is None:
                continue
            if fields.startswith("ElapsedRaw"):
                print(f"{int((job['end'] or time.time()) - (job['start'] or time.time()))}|{job['state']}")
            else:
                print(f"{job_id}|{job['state']}|{job['exit']}|{_stamp(job['start'])}|{_stamp(job['end'])}")
    elif command == "scancel":
        job = state["jobs"].get(args[0])
        if job is not None and job["state"] in ("PENDING", "RUNNING"):
            job.update(state="CANCELLED", exit="0:15", end=time.time())
            _save(path, state)
    return 0


def install(bin_dir: str) -> None:
    """Wrapper scripts named like Slurm's commands, for the remote PATH."""
    os.makedirs(bin_dir, exist_ok=True)
    python = sys.executable.replace("\\", "/")
    here = os.path.abspath(__file__).replace("\\", "/")
    for command in COMMANDS:
        with open(os.path.join(bin_dir, command), "w", newline="\n") as handle:
            handle.write(f'#!/bin/sh\nexec "{python}" "{here}" {command} "$@"\n')
        os.chmod(os.path.join(bin_dir, command), 0o755)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
