<!--
SPDX-FileCopyrightText: 2026 The fila Authors
SPDX-License-Identifier: GPL-3.0-or-later
-->

# Specification: `fila`, a queue and resource manager for non-local nodes

This page describes what `fila` does and how. The source code cites it by
section ("section 7.5"), so the numbering is stable.

## 1. Purpose and scope

`fila` joins machines that **share no network, computer or cluster** into a
single pool of resources. It manages a queue of jobs and allocates cores and
memory on those nodes. It plays the role of a home-made cluster built from
machines in different places.

Like Slurm, `fila` is **agnostic about the commands it runs**. It does not
know what git, a build, a Monte Carlo run or any particular program is. It
takes an opaque command and the declared resources, decides where and when to
run it, enforces the limits, follows the job to the end and records the
outcome.

`fila` does **not**:
- manage repositories, synchronise versions or build software;
- move input or output files (results stay on the node, as with Slurm);
- interpret the content of commands.

Those tasks belong to whoever uses `fila` (you, or an agent working for you),
through ordinary commands that `fila` runs.

Other characteristics:
- **A single user.** There are no accounts, quotas or fair sharing between
  people. The `created_by` field only tells the user apart from an agent
  (section 2).
- **Language.** The program (code, comments, subcommands, options,
  configuration keys, states, JSON fields and messages) and its documentation
  are in English.
- **Use by agents.** First through the command line; then as a registered
  tool over the Model Context Protocol (section 12).

HTCondor was ruled out because it needs heavy configuration and help from the
cluster administrator.

## 2. Concepts

| Term | Meaning |
|---|---|
| **Coordinator** | A role, not a machine. It is the `fila server` process, which keeps the queue, decides and launches. It runs on any computer (Linux, Windows or macOS) the user chooses. |
| **Node** | A resource registered with the coordinator: a **computer** or a **Slurm cluster**. |
| **Slurm node** | A compute machine inside a cluster (for example `node05`). Not to be confused with a `fila` node. |
| **Job** | Something `fila` runs. Either immediate (`run`) or queued (`submit`). |
| **Attempt** | One execution of a job. A job can have several attempts, all with the same `id`. |
| **Agent** | `fila` installed on a computer, where it probes resources and launches and supervises jobs. |

The coordinator's machine **only counts towards the pool if it is registered**
as a node. Otherwise it only coordinates.

The user decides where everything lives: which machine coordinates, which ones
are nodes and how they are connected. The program imposes no arrangement.

Every job records who created it in `created_by`: the value of the
`FILA_CREATED_BY` environment variable of the command that queued it, or
`user` when it is unset. An agent's environment sets `FILA_CREATED_BY=agent`;
jobs that arrive through the MCP server (section 12.2) get `agent` unless the
variable says otherwise.

## 3. Nodes

### 3.1 Computers

A computer has the `fila` agent installed and accepts both execution modes
(`run` and `submit`). The `access` field says how **this** coordinator reaches
it:

| `access` | How it works |
|---|---|
| `local` | It is the coordinator's own machine. |
| `ssh` | The coordinator reaches the computer over SSH. |
| `pull` | The coordinator **cannot** reach the computer. The computer opens and keeps a channel to the coordinator (section 4.3). For machines behind NAT or a firewall. |

### 3.2 Slurm clusters

A cluster **has no agent**: no `fila` process runs on it, not even Python. This
avoids needing the administrator's permission for permanent processes on the
login node.

- **Bound to a computer.** A cluster is always bound to a computer (`via`),
  which can be any registered computer or the coordinator's own machine. That
  computer's agent does everything on the cluster over SSH, with ordinary
  commands:
  - `sinfo`, `scontrol` and `sacctmgr` to probe and read limits;
  - sending the script and `sbatch --parsable` to submit;
  - `squeue` and `sacct` to follow;
  - `scancel` to cancel;
  - `tail` to read logs.
- **Only `submit`.** A `run --on <cluster>` is refused. To work on the login
  node (for example `git pull`), use the shell of the bound computer:
  `fila run --on <via> "ssh <alias> '...'"`.
- **Partition.** Each cluster node is a (cluster, partition) pair. To use two
  partitions of the same cluster, register two nodes with the same alias.
- **A job occupies a single Slurm node** (`--nodes=1`). MPI across several
  Slurm nodes is out of scope.

### 3.3 Reachability and network

To register a computer, the coordinator must reach it (`ssh`) or it must reach
the coordinator (`pull`). If the coordinator runs on a machine with no open
ports, no computer can be `pull` with respect to it.

**All networking is left to OpenSSH.** Registration only records an alias.
VPNs, `ProxyJump`, ports and keys live in the `~/.ssh/config` of the machine
that makes the connection: the coordinator's for `ssh` computers, and the bound
computer's for clusters. Example of going through another machine:

```
Host cluster
    ProxyJump lab
```

### 3.4 Registry

The registry is a TOML file (`nodes.toml`) on the coordinator's machine. It
can be edited by hand and then checked with `fila nodes validate`.

| Field | Computer | Cluster | Description |
|---|---|---|---|
| `type` | `computer` | `slurm` | Node type |
| `access` | required | n/a | `local`, `ssh` or `pull` |
| `host` | if `ssh` | required | SSH alias (for a cluster, as the `via` computer sees it) |
| `via` | n/a | required | Computer that controls the cluster |
| `os` | required | n/a | `linux`, `windows` or `macos` |
| `shell` | **required** | **required** | Shell that interprets commands, no default (section 5.1) |
| `python` | optional | n/a | Interpreter path, if the system `python3` is too old |
| `jobs_dir` | required | required | Jobs directory on the node (section 10) |
| `min_free_disk` | optional | optional | Below this, the node takes no new jobs |
| `limits` | optional | optional | Ceiling of what `fila` uses on the node (`cores`, `mem`). Without it, the hardware or what Slurm reports |
| `reserve.always` | optional | n/a | Margin `fila` never takes |
| `reserve.in_use` | optional | n/a | Margin while someone uses the machine (section 6.5) |
| `on_use` | optional | n/a | `keep` (default) or `suspend` |
| `idle_minutes` | optional | n/a | Minutes without keyboard or mouse input before the machine counts as free (default 5) |
| `default_hint` | optional | optional | `multithread` (default) or `nomultithread` |
| `speed` | measured or manual | measured or manual | Speed factor per core (section 7.4) |
| `partition`, `account`, `qos` | n/a | `partition` required | Default Slurm options |
| `exclude` | n/a | optional | Slurm nodes to avoid by default |
| `max_pending_slurm` | n/a | optional | K of the Slurm policy (section 7.5), default 2 |
| `max_time` | optional | optional | Longest time a job may run on the node |

Settings for the whole coordinator:

```toml
[general]
probe_interval_s  = 30
starvation_after  = "1h"     # T of the starvation protection (section 7.2)
run_timeout       = "10m"    # default time limit of a run
json_tail_kb      = 200      # end of the output returned with --json
```

An example with three nodes, the coordinator running on `lab`:

```toml
[nodes.lab]
type      = "computer"
access    = "local"
os        = "linux"
shell     = ["/bin/bash", "-lc"]
jobs_dir  = "~/fila/jobs"
reserve   = { always = { mem = "1G" }, in_use = { cores = 2, mem = "3G" } }

[nodes.cluster-cpu]
type      = "slurm"
via       = "lab"
host      = "cluster"
shell     = ["/bin/bash", "-l"]
partition = "batch"
jobs_dir  = "/scratch/alice/fila/jobs"   # absolute: Slurm does not expand ~
max_time  = "7-00:00:00"
max_pending_slurm = 2

[nodes.desktop]
type      = "computer"
access    = "pull"
os        = "windows"
shell     = ['C:\msys64\usr\bin\bash.exe', '-lc']
jobs_dir  = "~/fila/jobs"
min_free_disk = "5G"
reserve   = { always = { cores = 1, mem = "4G" } }
on_use    = "keep"
```

### 3.5 Registration

```
fila nodes register <name> --type computer --access local|ssh|pull --os ... --shell "..." [options]
fila nodes register <name> --type slurm --via <computer> --host <alias> --partition <p> --shell "..." [options]
fila nodes list | remove <name> | drain <name> | resume <name> | validate [<name>]
fila nodes benchmark [<name>] [--command "..."]
fila nodes update [<name>]
```

`register` **validates before accepting**. If any check fails, the node is not
added.

- **`ssh` or `local` computer:**
  1. tests the connection;
  2. checks the Python version;
  3. copies the agent;
  4. runs `<shell> 'echo ok'`;
  5. probes the hardware (physical and logical cores, memory, disk);
  6. runs the built-in speed test.
- **`pull` computer:** registration is a **pairing**, done in this order:
  1. **On the node:** `fila worker install --coordinator <user@host> --node <name>`.
     It creates its own key (`~/.fila/id_ed25519`), copies the agent
     (`~/.fila/fila.pyz`), writes `~/.fila/worker.toml` and installs the
     `fila-worker` service. It also prints the line to run on the coordinator.
     - The worker's SSH uses `StrictHostKeyChecking=accept-new` with its own
       `known_hosts`: the coordinator's key is recorded on the first
       connection, and a different key later is refused.
  2. **On the coordinator:** `fila nodes register <name> --access pull --key "<public key>" --shell ... --jobs-dir ...`.
     It appends to `~/.ssh/authorized_keys` the line
     `restrict,command="<python> <home>/fila_launcher.py internal channel --node <name>" <key>`,
     so the key can only open that node's channel, and waits up to `--wait`
     seconds (30 by default) for the channel to open, to run the checks above
     through it.
     - If the node has not connected yet, the registration is kept and the
       checks run later with `fila nodes validate <name>`.
     - Registering again replaces the line of the same key, without
       duplicating it.
     - `restrict` needs OpenSSH 7.2 or newer.
- **Cluster:**
  1. tests `ssh <host>` from the `via` computer;
  2. reads `scontrol show partition` (`MaxCPUsPerNode`, `MaxNodes`, `MaxTime`)
     and `sacctmgr show qos` (`MaxTRES`, `MaxTRESPerUser`);
  3. reads `sinfo -N` (CPUs and memory of each Slurm node in the partition);
  4. checks the `shell`.

  The limits read can be overridden by `limits` and `max_time` in the registry.

`drain` works as in Slurm: the node takes no new jobs, and running ones carry
on. `resume` returns it to normal.

### 3.6 Node states

| State | Meaning |
|---|---|
| `online` | Reachable and accepting jobs. |
| `unreachable` | No contact. For `pull`, the heartbeat failed for more than 45 s. For a cluster, the `via` computer is out of contact or SSH fails. |
| `drained` | Paused by `nodes drain`. |
| `outdated` | The agent's protocol version does not match the coordinator's. Takes no jobs until `nodes update`. |
| `low_disk` | Free space in `jobs_dir` is below `min_free_disk`. Running jobs carry on; new ones do not start. |

## 4. Architecture

```
                       [ coordinator: fila server + SQLite + registry ]
                         │ local        │ SSH            ↑ SSH channel opened by the node
                         ▼              ▼                │
              [ local computer ]   [ ssh computer ]   [ pull computer ]
                                        │ SSH (no agent)
                                        ▼
                                  [ Slurm cluster ]
```

### 4.1 Server

- `fila server` is the **only process that decides and launches**. CLI commands
  (`submit`, `cancel`, `run`...) write the request into SQLite and notify the
  server through a local mechanism, without networking. This prevents two
  processes from launching the same job.
- **Cycle.** In each cycle the server:
  1. probes the nodes;
  2. checks what finished (journals, `squeue`/`sacct`);
  3. releases dependents;
  4. applies the scheduling (section 7).
- **Permanent system service:**
  - `systemd --user`, with `loginctl enable-linger`, on Linux;
  - `launchd` on macOS;
  - Task Scheduler on Windows.

  `fila server --install` (and `--uninstall`, with `--dry-run` to only show the
  steps) sets up the service. All of them run `<home>/server_launcher.py`,
  which knows where the code and `FILA_HOME` are and sends the output to
  `<home>/server.log`. Per system:
  - Linux: a unit in `~/.config/systemd/user/fila-server.service` with
    `Restart=on-failure` and `KillMode=process` (the supervisors live in the
    service's cgroup, and restarting it must not kill the jobs),
    `systemctl --user enable --now` and `loginctl enable-linger`.
  - macOS: a LaunchAgent in `~/Library/LaunchAgents/org.fila.fila-server.plist`,
    with `KeepAlive`.
  - Windows: a scheduled task defined in XML that starts at logon in the
    user's session (`InteractiveToken`), with `pythonw.exe` (no window), **no
    execution time limit** (`PT0S`; the Windows default would stop the server
    after 72 h) and restart on failure. A process without a console starts
    every console program (`ssh`, a node's shell, `schtasks`) with
    `CREATE_NO_WINDOW`, so no window opens on the desktop at each cycle; from a
    terminal, children share it as usual.
- **A failing cycle does not stop the server.** The error goes to `server.log`,
  and the next cycle tries again.
- **Reconciliation at start.** For each job marked `running`, the server checks
  the computer's journal or the cluster's `sacct`. Nothing is relaunched by
  mistake.
- **Clock.** Recorded times come from the coordinator's clock.
- **Database.** SQLite in WAL mode, readable only by the user. The history is
  kept forever.
- **Waking up.** The CLI (`submit`, `run`, `cancel`, `retry`, `drain`...) and
  the supervisor, when an attempt ends, touch `<home>/wake`. The server runs a
  cycle at once instead of waiting for the interval, so a short job finishes
  and releases its dependents within seconds.
- **Heartbeat.** In each cycle the server writes `<home>/server.json` (PID and
  time). `run` only accepts a request if that heartbeat is recent; otherwise it
  exits with code 6.
- **Server stopped.** `submit` still records the job, which stays `pending`
  until the server is back. `run` fails with a clear message.

### 4.2 Agent

- The same program (`fila.pyz`, a `zipapp`) runs on every computer. It needs
  neither `pip` nor a virtual environment: Python 3.9 or newer is enough.
- It uses **only the standard library**. A pure-Python TOML reader (tomli,
  MIT license, unchanged in `src/fila/_vendor/tomli/`) is included and used
  when `tomllib` is missing, because `tomllib` only exists from Python 3.11 on.
- **Probing per system:**
  - `/proc` on Linux;
  - `sysctl`/`vm_stat` on macOS;
  - `ctypes` on Windows (`GlobalMemoryStatusEx`, `GetSystemTimes`,
    `GetLogicalProcessorInformation`).
- **Protocol version.** On connecting, the coordinator and the agent compare
  versions. If they differ, the node becomes `outdated`. `fila nodes update`
  copies the new agent to `ssh` computers, and `pull` computers download it
  through their channel.
- **Operations.** Everything that happens inside a computer is an agent
  operation: `version`, `probe`, `cycle` (probe plus the requested journals),
  `launch`, `request`, `read_log`, `clean`, `check_shell`, `benchmark`,
  `time_command` and `selftest`. On a `local` node the coordinator calls them
  directly. On an `ssh` node it calls `fila internal agent <operation>`, with
  the arguments as JSON on standard input and the answer
  `{"ok", "result" | "error"}` on standard output.
- **On an `ssh` node** the agent lives in `~/.fila/fila.pyz`. It is copied
  through SSH itself (`cat > ...`), without `scp`. Each operation runs as
  `cd && <python> .fila/fila.pyz internal agent <op>`. The registry's `python`
  is a word of the remote shell (`python3`, `$HOME/venvs/x/bin/python`), and
  the node's login shell must be POSIX.
- **One supervisor per job.** Each job on a computer is launched by its own
  supervisor process (`fila internal supervise`), **detached from the SSH
  session and from the channel**. The supervisor:
  - applies affinity and limits (section 6.3);
  - watches memory and time;
  - carries out suspension and resumption;
  - writes the journal.

  So losing the channel, the SSH connection or the agent itself does not kill
  the job.

### 4.3 Transport

- **`local`:** direct calls.
- **`ssh`:** the coordinator runs `fila internal agent <operation>` over SSH,
  with **one call per node and per cycle** (`cycle`: probe and journals
  together), plus launch and request calls when needed. The SSH command is
  `ssh_command` in `[general]`. The default is
  `ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=3`,
  which never asks for a password and gives up on dead connections. Each call
  may take at most `ssh_timeout_s`, 60 s by default. `ControlMaster`/
  `ControlPersist` in `~/.ssh/config` is recommended, so that each command
  does not open a new connection.
  - **SSH exit code 255**, or no answer in time, means the node is
    `unreachable`: the node and its running jobs become `unreachable`. When the
    node answers again, a job whose journal still says "running" is reattached
    (`running`), and one that ended meanwhile is closed with its real exit
    code.
  - **An answer without the expected protocol** leaves the node `outdated`
    until `nodes update`.
- **`pull`:** a **persistent channel**.
  - The computer runs `ssh <coordinator> fila internal channel --node <name>`
    and keeps the connection open.
  - Requests, answers, live output and resource reports travel over it as
    **one JSON message per line**, each carrying the request `id`, so several
    commands run at the same time.
  - **Heartbeat** every 15 s in both directions, plus `ServerAliveInterval` in
    SSH. Without a heartbeat for 45 s, the node becomes `unreachable`.
  - **Automatic reconnection** with growing delays: 5 s, 10 s, 30 s… up to
    5 min.
  - No port is opened on the `pull` computer, and it needs no SSH server.
  - **The server and the channel meet in the database.** The
    `internal channel` process is not the server: `sshd` starts it on each
    connection. The two talk through SQLite (tables `channels` and
    `channel_requests`): the server writes a request, the channel sends it to
    the node within 50 ms and writes the answer.
  - **A node only speaks about itself.** An answer is only accepted for a
    request to that same node; the coordinator rejects messages about other
    nodes' jobs and never runs anything a node asks for.
  - **The node is connected** while the channel process is alive and has heard
    from the node in the last 45 s. A new connection from the same node
    replaces the old one.
  - **Messages:**
    - coordinator to node: `request` (with `id`, `operation` and `args`) and
      `ping`;
    - node to coordinator: `hello`, `response` (with `id` and `answer`),
      `ping` and `event`.
  - **A job that ends on the node** touches `~/.fila/wake`. The worker sends
    the `wake` event, and the channel touches the coordinator's `wake`, so the
    end arrives within seconds.
  - **`nodes update`** sends the new agent through the channel. The worker
    writes it and exits with code 75, and the service starts it again from the
    new agent. On Windows this restart takes about 1 min, the task's restart
    interval.
  - **`~/.fila/worker.json`** says where the channel stands, for
    `fila top --local`: the coordinator, the node's name, the worker's PID,
    the state (`connecting`, `connected`, `waiting` with `retry_at` and the
    `reason` the last session ended, `stopped`), when the coordinator was last
    heard (`heard_at`, at most a few seconds old while connected) and every
    `jobs_dir` the requests named, kept across restarts.

### 4.4 Local journal

Each computer keeps, in `<jobs_dir>`, a journal of its jobs: PID, start,
assigned CPUs, exit code, end and reason. The journal is the source of truth
for reconciliation after crashes and restarts (section 9).

### 4.5 Client mode

The CLI can talk to a coordinator on another machine. With
`coordinator = "<user@host>"` in the client's local configuration, a command
such as `fila status` transparently runs as `ssh <host> fila status`. An agent
can thus work on any machine that reaches the coordinator.

The configuration lives in `<home>/client.toml` (or in the `FILA_COORDINATOR`
variable):

```toml
coordinator    = "alice@lab"      # SSH destination
remote_command = "fila"           # how to call fila on the coordinator, e.g. "python3 -m fila"
ssh            = ["ssh"]          # SSH program and options
```

- Each argument is quoted the POSIX way, so the coordinator's remote shell must
  be POSIX (bash or sh), as on Linux or macOS.
- The output is streamed (`log --follow` works), and the remote exit code comes
  back unchanged. A failure of SSH itself (code 255) becomes code 6.
- A local `--script` travels on standard input: the coordinator receives it
  with `--script -`.
- Line endings of any script sent become `\n`, so a script saved on Windows
  runs in bash.

## 5. Jobs

### 5.1 Shell

Every command is interpreted by the node's registered `shell`, called as
`<shell...> "<command>"`. Examples:

```toml
shell = ["/bin/bash", "-lc"]                          # Linux, macOS, cluster
shell = ['C:\msys64\usr\bin\bash.exe', '-lc']         # Windows with MSYS2
```

The `-l` (login shell) loads the user's profile, so `PATH` and `module` are as
in an SSH session. Paths and command syntax are the responsibility of whoever
writes the commands.

### 5.2 `run`: the terminal, now

```
fila run "<command>" --on <nodes|all|online> [--dir D] [--time T] [--cores N --mem M [--strict]] [--wait] [--dry-run] [--json]
```

- Runs **now**, on the given computers, **in parallel**. It never waits in the
  queue.
- **Independent commands.** Each execution opens a fresh shell. It starts in
  the node's home directory, or in `--dir`. No `cd` or variable carries over
  from one command to the next.
- **No standard input.** It is closed, so a command that asks for an answer
  fails at once instead of hanging. Interactive programs are out of scope: use
  SSH directly for them.
- **Time limit** `run_timeout` (10 min by default), adjustable with `--time`.
  When it expires, the process is killed and the result is `failed` with reason
  `timeout`.
- **Optional resources.** Without `--cores`/`--mem`, nothing is reserved, but
  probing sees the load and keeps new jobs off the node (section 6.1). With
  them, the reservation counts while the command runs. If the request does not
  fit, `fila` warns and runs it anyway, or refuses with `--strict`.
- **Targets.**
  - `all` is every registered computer. Clusters are left out, because they do
    not accept `run`.
  - `online` is only the reachable ones.
  - An `unreachable`, `drained` or `outdated` target gets `not_run`, with the
    reason, without waiting.
- **One record per node**, with exit code, duration, the full output in a log
  and the end of the output in `--json`.

### 5.3 `submit`: the queue

```
fila submit [--name X] --cores N --mem M [--time T] [--on <nodes>] [--dir D] [--script F]
            [--after IDS] [--even-if-failed] [--retry-after T] [--on-use keep|suspend]
            [--hint multithread|nomultithread]
            [--slurm-nodelist L] [--slurm-exclude L] [--slurm-extra "..."]
            [--dry-run] [--wait] [--json] -- <command>
```

- **`--cores` and `--mem` are required.** Without them the job is refused.
- **`--time` is optional.** Without it, the node's `max_time` applies.
- **It fits in a single node.** If the request exceeds the **capacity** of every
  candidate node, it is refused at once with the reason (for example
  "25 cores: largest candidate is desktop with 6"). Several nodes are never
  added up. If the request fits some node's capacity but not what is free now,
  or that node is down, the job stays `pending`.
- **`--on`** restricts the candidates. Without it, `fila` chooses among all
  nodes (section 7.3).
- **`--script F`** sends the local file `F` to the job's directory on the node,
  and the command becomes running that script. It is the only file copy `fila`
  makes, because the script is the job's definition, not data. On a cluster,
  `#SBATCH` lines inside the script produce a **warning** and are ignored: the
  resources are always those declared to `fila`.
- **`--dry-run`** shows the chosen node and why, the CPUs and, for Slurm, the
  generated script, without running anything.
- **`--wait`** submits and waits, like `fila wait`.

### 5.4 Dependencies

- `--after 12,13,14`: the job only starts after **all** its dependencies have
  ended, **even if resources are free**.
- **Only `completed` counts as success.** It is the equivalent of Slurm's
  `afterok`.
- **If a dependency fails or is cancelled**, the dependent becomes `blocked`. It
  takes no resources and is highlighted in `status`. The block propagates to
  its own dependents. There are two ways out:
  - `fila retry <dependency id>`: a new attempt. If it succeeds, the dependents
    move on by themselves.
  - `fila cancel <dependency id> --cascade`: cancels the whole chain.
- **`--even-if-failed`**: the dependent runs once its dependencies reach any
  final state. Useful for clean-up or reports.
- Dependencies **only look at the state**. If B needs files from A on another
  node, whoever submits the jobs chains an explicit copy, for example a job
  with `scp` between the two.
- Jobs without dependencies are independent and are allocated while resources
  last.

### 5.5 States

States of a `submit` job:

| State | When |
|---|---|
| `waiting_dependency` | Has dependencies that have not ended. |
| `pending` | Ready, waiting for resources. |
| `slurm_queued` | Submitted to Slurm, which keeps it `PENDING`. |
| `running` | Running. The `suspended` mark says it is frozen (section 6.5). |
| `unreachable` | The node lost contact while it ran (section 9). |
| `completed` | Ended with exit code 0. |
| `failed` | Ended without success. The `reason` field says why. |
| `blocked` | A dependency failed or was cancelled. |
| `cancelled` | Cancelled by the user or in a cascade. |

Reasons for `failed`:

| Reason | When |
|---|---|
| `exit_code` | Non-zero exit code. |
| `out_of_memory` | Went over the declared memory. |
| `timeout` | Went over the time limit. |
| `lost` | The process disappeared, for example because the computer rebooted. |
| `start_failed` | The command never started: invalid directory, a limit refused by the system or a shell that does not open. |
| `node_failure` | Slurm's `NODE_FAIL`. |

Results of a `run`, per node: `completed`, `failed` (reasons `exit_code` and
`timeout`, plus `out_of_memory` if memory was declared) and `not_run` (reasons
`unreachable`, `drained` and `outdated`).

### 5.6 Attempts

- Each job has one or more attempts. Each attempt keeps its own node, CPUs,
  start, end, state, reason, exit code and logs and, on computers, whether it
  is suspended now and the time it has spent suspended, as its journal reported
  them at the last cycle (`suspended`, `suspended_seconds` in `result --json`).
- `fila retry <id> [--on <node>]` creates a new attempt. It applies to `failed`,
  `cancelled` or `unreachable` jobs.
- **Abandoned attempt.** When a job is resubmitted while its node is out of
  contact, the old attempt becomes `abandoned`. If it shows up later, its
  result is kept but does not count for the dependents.
- `--retry-after T`: if the node stays out of contact for longer than T,
  `fila` creates the new attempt on another node by itself. This is optional,
  per job. By default nothing is relaunched automatically.

## 6. Resources

### 6.1 Accounting

For each node and each resource (cores and memory):

```
free = min( capacity − reserved_by_fila ,  probe − reserve )
```

- `capacity`: the registry's `limits`. Without it, the computer's hardware, or
  what Slurm reports for the cluster.
- `reserved_by_fila`: the sum of what `fila` jobs on the node **requested**. On
  a computer, running jobs and `run` commands with declared resources count.
  On a cluster, running jobs **and** jobs pending in Slurm count.
- `probe`: what the system reports as free now. On a computer, cores minus the
  load, and the available memory. On a cluster, the idle CPUs and free memory
  of `sinfo` per Slurm node.
- `reserve`: `reserve.always`, or `reserve.in_use` while the machine is in use
  (section 6.5).

Taking the minimum guards against both possible mistakes:
- trusting only the probe, which is slow to reflect a job just launched;
- trusting only the accounting, which does not see use outside `fila`.

### 6.2 Core unit and hyperthreading

- **The unit is the logical CPU**, as in the operating system and in Slurm. The
  registry records the physical and logical cores of each computer.
- **Whole physical cores.** On computers, whenever possible, a job gets whole
  physical cores, and two jobs never share a physical core.
- **`--hint nomultithread`** (or the node's `default_hint`), with the same
  meaning as in Slurm:
  - **On a computer:** `--cores N` becomes N physical cores, using one logical
    CPU of each. The sibling CPUs stay reserved and idle, so the accounting
    counts 2N logical CPUs on a node with hyperthreading.
  - **On a cluster:** `fila` passes `--hint=nomultithread` to `sbatch`. It only
    takes effect if the cluster's Slurm has affinity configured
    (`TaskPlugin`).

### 6.3 Enforcing the limits

Declared resources are not a promise. `fila` enforces them the way Slurm does
with cgroups.

**Cores**
- The supervisor pins the process, and all its children, to the assigned CPUs:
  - `sched_setaffinity` on Linux;
  - an affinity mask inside a *Job Object* on Windows.
- Extra threads only share those CPUs; the job is not killed.
- macOS has no process affinity. There, the core limit is not enforced (a known
  limitation).

**Memory**
- A hard limit where the system allows it:
  - cgroup v2 through `systemd-run --user --scope -p MemoryMax=...` on Linux;
  - the *Job Object* memory limit on Windows.
- Where it does not (macOS, or Linux without a delegated cgroup), the
  supervisor measures the memory of the process group every few seconds and
  kills the job if it goes over the declared amount.
- Either way, the result is `failed` with reason `out_of_memory`.
- **Per system:**
  - Windows uses the hard limit and is told by the *Job Object* when a process
    tries to exceed it;
  - Linux uses the hard limit when `systemd-run --user --scope` works and the
    `memory` controller is delegated to the user. Each attempt gets a scope
    `fila-<pid>-<ms>.scope` with `MemoryMax` equal to the declared memory,
    `MemorySwapMax=0` and `OOMPolicy=stop`; the supervisor reads `oom_kill`
    from the scope's `memory.events` to report `out_of_memory`. Without
    delegation it measures and kills, and `selftest` says which of the two
    applies on the node. Verified on Ubuntu with systemd 255;
  - macOS only measures and kills.

**Time**
- When `--time` is exceeded, the supervisor sends `SIGTERM`, waits 30 s and
  sends `SIGKILL`. The result is `failed` with reason `timeout`.
- Windows has no `SIGTERM` for ordinary programs, so there the job is stopped
  at once, through the *Job Object* (a known limitation).
- On a cluster, Slurm itself enforces `--time`, `--mem` and `--cpus-per-task`.

### 6.4 Variables exported to the job

`FILA_JOB_ID`, `FILA_ATTEMPT`, `FILA_CORES`, `FILA_MEM` and `FILA_NODE`.

`fila` does **not** set `OMP_NUM_THREADS` or other programs' variables, as Slurm
does not either. GCC's OpenMP (libgomp) counts the CPUs in the affinity mask to
decide how many threads to open, so programs that use every available core
already get N threads. Verified on Windows with MSYS2 UCRT64 GCC 15.2: with 2, 3
and 6 CPUs assigned, `omp_get_max_threads()` returned 2, 3 and 6. Not yet
verified on Linux.

### 6.5 Use of the machine by its user

**Detection.** The machine is "in use" if there was keyboard or mouse input in
the last `idle_minutes`:
- `GetLastInputInfo` on Windows;
- `IdleHint` of the user's **local** sessions (`loginctl`, `Remote=no`) on
  Linux. Remote sessions (SSH, including a `pull` node's channel) do not count:
  logind never reports them as idle, and they would keep the machine "in use"
  forever. The desktop only marks the session idle after its own inactivity
  delay (in GNOME, `org.gnome.desktop.session idle-delay`); with it at `0`, the
  session never becomes idle;
- `HIDIdleTime` (`ioreg`) on macOS.

On Windows this requires the worker to run in the user's session, as a
scheduled task that starts at logon.

**Effects.**
- While the machine is in use, `reserve.in_use` applies to **new** jobs.
- For jobs **already running**, the user chooses, at three levels, and the most
  specific wins:
  1. the node's `on_use` in the registry: `keep` (default) or `suspend`;
  2. the job's `--on-use`;
  3. by hand, with `fila suspend <id>|--node <node>` and
     `fila resume <id>|--node <node>`.
- **`suspend`** freezes the processes: `SIGSTOP`/`SIGCONT` on Linux and macOS,
  suspension of the *Job Object*'s processes on Windows. Resumption is
  automatic when the machine is free again.
  - It frees the CPU, but **not the memory**.
  - Frozen time does not count towards `--time`.

### 6.6 Disk

Probing measures the free space in `jobs_dir`. Below `min_free_disk`, the node
takes no new jobs, and `explain` gives the reason.

### 6.7 Generic resources (future)

For now, only cores and memory. Node and job resources are stored as a
**dictionary**, not as fixed fields, so countable resources such as GPUs can be
added later, like Slurm's GRES:
- `gres = { gpu = 1 }` in the registry;
- `--gres gpu:1` on the job;
- `--gres` passed on to `sbatch`;
- `FILA_GPUS` on computers.

None of this requires changing the database or the scheduler.

## 7. Scheduling

### 7.1 Candidates

A node is a candidate for a ready job if:
- it is `online` and not set aside for another job (section 7.2);
- it is within `--on`, if given;
- the job fits its capacity **and** what is free now (section 6.1), resource by
  resource.

For a cluster, "fits now" means that `sinfo` shows a Slurm node of the
partition, honouring `nodelist` and `exclude`, with enough idle CPUs and free
memory.

### 7.2 Order and starvation protection

- **Filling.** In each cycle, ready jobs are visited in submission order, and
  each one that has a candidate is launched. A job that does not fit does not
  hold back the ones after it.
- **Starvation protection.** If the oldest job in the queue has waited longer
  than `starvation_after` (1 h by default), `fila` **sets aside for it** the
  node, among those that can hold the request, that should free resources
  soonest. No new job enters that node until it starts.
- Slurm's backfill (letting in short jobs that provably end before) and manual
  priorities are left for later refinements.

### 7.3 Choice of node

Among the candidates, the one with the **highest speed factor per core** wins.
On a tie, the one with **the least left over** after the allocation wins, to keep
large gaps free for large jobs.

`fila` does not wait for a faster node to become free: it picks the fastest
among those that can start now.

### 7.4 Speed factor

The factor measures the speed **of one core**, because a job gets the cores it
declared on any node. The nominal frequency and the node's number of cores are
no use for that.

- **Built-in test.** Runs on one core, with C code from the standard library:
  24 `zlib` compressions (level 6) of a fixed block of about 2.3 MB. That work in
  2 s is worth 1.0. It runs when computers are registered (unless `--speed` is
  given) and with `fila nodes benchmark`.
- **User's test (scale).** A command that takes 100 s is worth 1.0. The
  registry records where the factor came from (`speed_source`: `builtin`,
  `command`, `manual` or `default`), and `nodes benchmark` warns when factors
  from different tests are mixed.
- **User's test.** `fila nodes benchmark --command "..."` runs a representative
  command on each node and only measures the time. On clusters it is submitted
  with `sbatch` and runs on a compute node, never on the login node.
- **Manual setting.** `speed` in the registry.
- **Comparison.** Factors are only comparable between nodes measured with the
  same test. A cluster can only be measured with the user's test or set by
  hand. A node without a measurement is worth 1.0.

### 7.5 Slurm policy

`fila` keeps the job in its own queue and only calls `sbatch` when the cluster
"fits now" (section 7.1). There is one exception: if **no other candidate** can
start the job now, it may be submitted anyway, as long as `fila` has fewer than
`max_pending_slurm` (K, 2 by default) of its own jobs pending in that cluster.
This secures a place in Slurm's queue without flooding it.

A future refinement may cancel (`scancel`) and redirect a job stuck in Slurm
for more than X hours, if it has not started and another node can run it.

## 8. Slurm clusters: details

**Generated script.** `fila` generates the script from the declared resources
and the registry, sends it to `<jobs_dir>/<id>/job.sbatch` and submits it.
Example:

```bash
#!/bin/bash -l
#SBATCH --job-name=fila-42
#SBATCH --partition=batch
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=32768M
#SBATCH --time=3-00:00:00
#SBATCH --chdir=/scratch/alice/project
#SBATCH --output=/scratch/alice/fila/jobs/42/attempt-1/stdout.log
#SBATCH --error=/scratch/alice/fila/jobs/42/attempt-1/stderr.log
#SBATCH --exclude=node01
export FILA_JOB_ID=42 FILA_ATTEMPT=1 FILA_CORES=64 FILA_MEM=32G FILA_NODE=cluster-cpu
./build/program input.yaml
```

**Options.**
- `fila` understands `--slurm-nodelist` and `--slurm-exclude`: it passes them
  on to `sbatch` and filters `sinfo` by them.
- `--slurm-extra "..."` is passed through as `#SBATCH` lines, uninterpreted.
- Options that override what belongs to `fila` are **refused**:
  `--cpus-per-task`, `--mem`, `--mem-per-cpu`, `--time`, `--nodes`, `--ntasks`,
  `--output`, `--error` and `--chdir`.

**Following.**
- The state and the reason come from `squeue` and `sacct`:

  | Slurm | `fila` |
  |---|---|
  | `PENDING` | `slurm_queued` |
  | `RUNNING` | `running` |
  | `COMPLETED` | `completed` |
  | `FAILED` | `failed` / `exit_code` |
  | `OUT_OF_MEMORY` | `failed` / `out_of_memory` |
  | `TIMEOUT` | `failed` / `timeout` |
  | `NODE_FAIL` | `failed` / `node_failure` |
  | `CANCELLED` | `cancelled` |

- The exit code comes from `sacct`'s `ExitCode`.
- `fila log --follow` uses `tail` over SSH from the `via` computer.
- `fila cancel` calls `scancel`.

**Migration (refinement).** `fila submit --from-sbatch file.slurm` would read
the common directives (`--cpus-per-task`, `--mem`, `--time`, `--partition`,
`--nodelist`, `--exclude`, `--job-name`) and refuse those it cannot account for.

**Implementation**
- **Commands.** The `via` computer's agent connects to the login node over SSH
  (with the general `ssh_command`) and runs only:
  - `sinfo -h -N -p <partition> -o %N`: the partition's machines;
  - `scontrol show node -o`: `CPUTot`, `CPUAlloc`, `RealMemory`, `AllocMem` and
    `State`. Machines in `DOWN`, `DRAIN`, `FAIL`, `MAINT` and similar states
    count as having nothing free;
  - `scontrol show partition <p> -o` and `sacctmgr -n -P show qos`:
    `MaxCPUsPerNode`, the `cpu=` of the QoS `MaxTRES` and `MaxTime`;
  - `sbatch --parsable`, `squeue -h -u "$USER"`, `sacct -n -P -X`, `scancel`,
    `tail`, `df`, `du` and `rm` on `fila`'s own files.
- **Registration.** `nodes register --type slurm --via <computer> --host <alias> --partition <p>`
  also accepts `--account`, `--qos`, `--exclude`, `--max-pending`,
  `--max-cores-per-job`, `--cores`/`--mem` (a courtesy quota) and `--max-time`.
  It checks the partition, the machines and the shell, and records `jobs_dir`
  **as an absolute path**, because Slurm does not expand `~` in `#SBATCH`
  lines. Without `--max-time`, the partition's `MaxTime` applies.
- **`--script`** is embedded in the `sbatch` script itself (a heredoc), with no
  separate copy.
- **`--slurm-extra "--constraint=avx512"`**: the value right after the option is
  always the value, even when it starts with `--`.
- **State.** `sacct` decides when it knows the job, and `squeue` covers a slow
  or missing `sacct`. A running job records its real start (`sacct`'s `Start`).
- **`fila log`** reads with `tail` on the cluster, and `--follow` ends when
  `squeue -n fila-<id>` no longer lists the job.
- **`fila suspend`** is refused for cluster jobs, because `scontrol suspend`
  needs the administrator.
- **`nodes benchmark <cluster> --command ...`** submits the command with
  `sbatch --wait` and uses `sacct`'s `ElapsedRaw`.
- **Unreachable cluster** (the `via` computer is down, or SSH to the login node
  failed): the node becomes `unreachable`, and so do the jobs running there.
  `slurm_queued` jobs stay as they are, because Slurm looks after them.
  Everything is reconciled when the cluster is back.

## 9. Failures and reconciliation

- **Losing the channel or SSH does not kill the job** (detached supervisor,
  section 4.2).
- **Out of contact.** When the node becomes `unreachable`, its jobs become
  `unreachable`, not `failed`. **Nothing is relaunched automatically**, unless
  the job has `--retry-after` (section 5.6).
- **When the node is back**, `fila` reconciles each job with the journal (or
  with `sacct`, on a cluster):
  - still running: back to `running`;
  - ended during the absence: `completed` or `failed`, with the true exit code;
  - gone: `failed` with reason `lost`, and the dependents become `blocked`
    (section 5.4).
- **Not to wait:** `fila retry <id> --on <other node>` creates a new attempt,
  and the old one becomes `abandoned`.
- **Cluster.** If the `via` computer goes down, the cluster becomes
  `unreachable`. The jobs carry on in Slurm, and reconciliation through `sacct`
  happens when the computer is back.

## 10. Files

- Each job has its own directory, `<jobs_dir>/<id>/`, on the node where it runs,
  shared by all its attempts. By default a `submit` command runs there. A `run`
  starts in the user's home directory. With `--dir`, both run in another
  directory of the node, interpreted on the node itself.
- `fila`'s files are **always** in `<jobs_dir>/<id>/attempt-<n>/`, one directory
  per attempt, even with `--dir`:
  - `stdout.log` and `stderr.log`;
  - `job.json`: command, shell, assigned CPUs, limits and variables;
  - `status.json`: the attempt's journal (state, PIDs, times, exit code,
    reason, `suspended`, `suspended_seconds` of the suspensions already over
    and `suspended_since` of the current one, on the node's clock);
  - `supervisor.log`: errors of the supervisor itself;
  - `cancel` and `suspend`: requests to the supervisor; `suspend` present means
    "keep frozen";
  - `job.sbatch`, on clusters.
- The `--script` file is written to `<jobs_dir>/<id>/`.
- The program writes its results where it would if it ran in a terminal. `fila`
  only sets the starting directory.
- **No file is moved automatically.** Whoever wants the results fetches them
  with an ordinary command, in the direction that works:
  - from a `pull` computer: `fila run "scp output.h5 <coordinator>:~/results/" --on <node>`;
  - from an `ssh` computer or a cluster: `scp` from the coordinator's machine
    or from the `via` computer.
- **Nothing is deleted automatically.** `fila clean --older-than 30d [--node X] [--logs-only]`
  only deletes `<jobs_dir>/<id>/` directories of finished jobs, and never paths
  given with `--dir`.

## 11. Command line

```
fila run "<command>" --on <nodes|all|online> [...]       # the terminal, now (section 5.2)
fila submit [...] -- <command>                            # the queue (section 5.3)
fila status [--state S] [--node N] [--all]                # queue, running jobs and node summary
fila resources                                            # what each node has free now
fila top [--interval S] [--node N] [--all]               # nodes, jobs and why each waits, refreshed (like htop)
fila top --once [--width W] [--json]                      # one screen, and exit
fila top --local [--jobs-dir D] [--once]                  # on a node: what fila runs here, from its own files
fila guide                                                # the usage guide for an agent's instructions
fila mcp [--register]                                     # the MCP server (stdio), or how to register it
fila result <id>                                          # state, reason, code, node, attempts, directory, end of the log
fila log <id> [--attempt K] [--stderr] [--follow]
fila explain <id>                                         # why the job is in its state
fila wait <ids...> [--timeout T]
fila cancel <id> [--cascade]
fila retry <id> [--on <node>]
fila suspend <id>|--node <node>
fila resume  <id>|--node <node>
fila clean --older-than T [--node N] [--logs-only]
fila nodes register|list|remove|drain|resume|validate|benchmark|update ...
fila selftest --on <nodes>
fila server [--once] [--install|--uninstall [--dry-run]]  # the coordinator, or the permanent service
fila worker install|run ...                               # the agent of a pull computer
fila internal ...                                         # internal use (channel, supervisor, probing)
```

**`explain`.** Says in plain language why a job is stuck. Example:
"pending: needs 6 cores; desktop has 4 free (2 used outside fila); cluster-cpu
excluded by --on; waiting since 14:02; node reservation in 38 min (starvation
protection)".

**`top`.** One screen with the nodes and the jobs, like `htop`. The node panel
shows each node's state, a bar per resource (cores and memory: what `fila`'s
jobs hold, what other use and the reserve take, and what is free, with the
used amount over the capacity), the speed factor, and whether someone is using
the machine (section 6.5); for a cluster, how many of `fila`'s jobs are pending
in Slurm against `max_pending_slurm` (section 7.5). The job panel shows the
active jobs (`--all` adds the finished ones) with their state (`suspended` for
a frozen running job), node, requested cores and memory, the attempt's running
time without the time it spent suspended, and, for a waiting job, the reason
`explain` gives, in a few words. The header warns `server not running` when
the coordinator's heartbeat is old; the view still shows the database. Columns
shrink or disappear below about 80 columns, the command first. `--once` prints
one screen and exits; with `--json` it prints the same snapshot as data.

Without `--once`, the screen takes the terminal and is redrawn every
`--interval` seconds (2 by default) and when the terminal is resized. It
uses `curses` on Linux and macOS and, on Windows (whose Python has no
`curses`), escape sequences with `msvcrt` for the keys; both come with the
standard library. The keys (`?` lists them):

| Key | Does |
|---|---|
| Up/Down, `j`/`k`, PgUp/PgDn, Home/End | move the selection (highlighted) |
| Tab | switch between the job list and the node panel |
| Enter | the job's `fila result`, or the node's details |
| `l` | the end of the job's output, followed; Tab switches stdout/stderr |
| `e` | the job's `fila explain` |
| `f` | filter by node, state, name or command |
| `a` | show or hide finished jobs |
| `o` | order by id, state, node or elapsed time |
| `q`, Esc | quit; on a page, back to the list |

The pages show exactly what the command line prints, run in the same
process. In client mode (section 4.5) the interactive screen runs on the
coordinator through `ssh -t`, so it holds one connection instead of one per
refresh; `--once` and `--json` are forwarded like any other command.

**`top --local`.** The minimal view for whoever sits at a node, on any
computer (`local`, `ssh` or `pull`), without the coordinator: nodes have no
database, so it reads only what exists on the node. It shows the probe (cores
and memory used, with the part held by `fila`'s attempts, and whether the
machine is in use), the attempts in its jobs directories (running, suspended,
starting, lost, and those that ended in the last hour; `--all` shows every
ended one) with their CPUs, memory limit and running time, and, on a `pull`
node, the channel from `~/.fila/worker.json` (section 4.3). The jobs
directories are those the worker recorded plus any `--jobs-dir`; an `ssh` node
needs `--jobs-dir`. It is read only: cancelling, suspending and draining stay
on the coordinator, because a suspension made on the node would be undone by
the server at its next cycle and a cancellation must be recorded in the
database. It is never forwarded in client mode. Without `--once` it refreshes
like the full view; its keys are the moving ones, Enter (the attempt's
`job.json` and `status.json`), `l` (its output, followed), `f`, `a`, `?` and
`q`.

**`wait`.** Blocks until every given job reaches a final state: `completed`,
`failed`, `cancelled` or `blocked`. It only reads the local database, about once
a second, and does not talk to the nodes.

**Output.**
- Human-readable text by default, and `--json` on every subcommand.
- With `--json`, a command's output carries its end (`json_tail_kb`) and the
  path of the full log.

**Exit codes**, the same for every subcommand:

| Code | Meaning |
|---|---|
| 0 | Success, on every target or job. |
| 1 | Some target or job ended without success. |
| 2 | Some target did not run (`not_run`). |
| 3 | The wait timed out (`wait --timeout`, `--wait`). |
| 4 | Request refused: missing resources, larger than any node, `--strict` without free resources, a forbidden Slurm option, `run` on a cluster. |
| 5 | Usage error: invalid option, unknown node or job. |
| 6 | Coordinator unavailable. |

## 12. Integration with agents

### 12.1 Command line

`fila` ships a **generic usage guide** (`fila guide`), to be included in the
agent instructions of any project (`fila guide >> <instructions file>`). It is
written in English, like the program, and explains:

- **Commands.** The subcommands and when to use each: `run` for short commands
  on the nodes' terminals and `submit` for computations.
- **`--json`.** Always, when reading results.
- **Waiting without polling.** After a `submit`, run `fila wait <ids>` in the
  background and keep working; an agent that runs background commands is told
  when the jobs end.
- **`explain`** when a job does not start.
- **Heavy computations always go through `submit`**, never `run`.

Advice that depends on the project belongs in each project's own instructions,
not in `fila`:

- **Relative paths.** A job starts in `<jobs_dir>/<id>/`. To run as in the
  terminal, from the repository root, use `--dir`.
- **Unique outputs.** Simultaneous jobs in the same `--dir` must not write to
  the same file. Put `$FILA_JOB_ID` in output names.
- **Binaries in use.** Do not rebuild an executable that running jobs use.
  Windows locks the file, and on Linux jobs that start later pick up the new
  binary. Copy the binary per campaign.
- **Synchronising and building** are ordinary commands, for example
  `fila run "git pull && cmake --build build" --on all --wait`. On a cluster
  they go through the bound computer (`fila run --on <via> "ssh <alias> '...'"`).
  A heavy build on a cluster should be a `submit`.

To label the jobs an agent queues from the command line, set
`FILA_CREATED_BY=agent` in the agent's environment (section 2).

### 12.2 Registered tool (Model Context Protocol)

- A thin Python layer exposes tools with typed parameters: `run`, `submit`,
  `status`, `resources`, `result`, `log`, `explain`, `wait`, `cancel` and
  `retry`.
- **No official SDK, on purpose.** The `mcp` package needs Python 3.10 or newer
  and installation with `pip`, which would break two rules of the project: only
  the standard library, and a single `.pyz`. The protocol over stdio (JSON-RPC
  2.0, one message per line) is implemented with the standard library, only in
  the part a tool server needs: `initialize` (negotiating the version:
  2025-06-18, 2025-03-26 or 2024-11-05), `tools/list`, `tools/call` and `ping`.
- **Each tool runs the matching CLI command with `--json`.** So MCP does exactly
  what the terminal does, client mode included: on a client, the MCP server
  runs locally and each call goes on over SSH. The answer carries `exit_code`,
  `result` and `stderr`. Only codes 4, 5 and 6 (refused, usage error,
  coordinator down) become tool errors; a job that failed is a normal result.
- Jobs queued through the server are labelled `agent` (section 2).
- **Registration:** `fila mcp --register` writes `<home>/fila_launcher.py` and
  prints what an MCP client needs: the command (the current Python), the
  arguments (`<home>/fila_launcher.py mcp`) and the same as JSON in the
  `mcpServers` format most clients read. The server is registered under the
  name `fila` with the client's own command or settings.
- **Suggested permissions** (printed by `--register`):
  - free: `status`, `resources`, `result`, `log`, `explain` and `wait`;
  - ask for confirmation: `run` and `cancel`;
  - `submit` and `retry` follow the client's default.
- Tool calls have a short time limit. So the tool's `wait` accepts at most
  10 min, and long waits stay with `fila wait` in the background.

## 13. Security

1. **Encryption comes from SSH.** All traffic between the coordinator,
   computers and clusters goes over SSH, which encrypts and authenticates both
   ends. `fila` uses no cryptography of its own.
2. **Only `ed25519` keys**, never passwords. Each `pull` computer's key is
   restricted in the coordinator's `authorized_keys`: it can only open that
   node's channel. It gives no shell and cannot impersonate another node.
3. **No new port** on any machine. `fila server` does not listen on the network.
4. **The coordinator identifies itself too.** During pairing, the `pull`
   computer records the coordinator's SSH fingerprint and refuses connections
   that do not match.
5. **A node only speaks about itself** (section 4.3).
6. **Permissions.** The registry, the database and the logs are readable only
   by the user: `0600` on Linux and macOS, and an equivalent ACL on Windows. The
   agent runs as an ordinary user, never as an administrator.
7. **Everything is recorded.** Every `run` and every `submit` is kept in the
   database, with its origin (`created_by`), time, node and result.
8. **The critical point is the coordinator.** Whoever controls the coordinator
   can run anything on every node, because that is its role. The account on the
   coordinator's machine must be protected.

## 14. Tests

1. **The scheduler as a pure function.** It takes the state of the nodes, the
   jobs and the time, and returns the decisions. It uses no network or disk,
   and the clock is simulated. Each rule of sections 5 to 7 has its own tests,
   including:
   - starvation (a 6-core job behind several 2-core ones);
   - cascading blocks and `retry`;
   - refusal by capacity;
   - the Slurm policy with K;
   - choice by speed and tie-breaking.
2. **Fake nodes and a fake Slurm**, to test the whole flow (server, channel,
   supervisor, journal, reconciliation) in CI:
   - nodes run on the test machine itself, with artificial directories and
     limits;
   - a fake Slurm, with `sbatch`, `squeue`, `sacct`, `sinfo` and `scancel`
     scripts that answer like the real ones;
   - a channel that can be dropped on purpose.
3. **`fila selftest --on <nodes>`** on the real nodes. It runs **inside the
   node**, through the agent and with the node's Python, without depending on
   the server. It checks:
   - `shell`: the registered shell answers;
   - `start` and `exit_code`;
   - `time_limit`: a job that exceeds its time;
   - `memory_limit`: one that exceeds its memory on purpose;
   - `cpu_affinity`: "n/a" on macOS;
   - `suspend_resume`: suspension and resumption;
   - `leftovers`: processes left behind are killed.

   Dropping and restoring the channel is covered with `pull` nodes. Checking
   what OpenMP sees remains manual (section 6.4).
4. **`--dry-run`** on `run` and `submit`.

Run the suite from the repository root with
`python -m unittest discover -s tests -t .`.

## 15. Known limitations

- **macOS:** experimental, until `selftest` runs on a Mac. No CPU affinity (the
  core limit is not enforced), and memory is watched instead of limited.
- **Windows:**
  - no grace period when the time runs out: the process is killed at once,
    without `SIGTERM`;
  - detecting use requires the worker to run in the user's session. If the
    computer reboots and nobody logs in, the node stays down until the logon.
- **Slurm:** `--hint` and hard limits depend on the cluster's configuration
  (cgroups and `TaskPlugin`).
- **Lost jobs are not relaunched by themselves** without `--retry-after`, by
  design.
- **A job never occupies more than one node**, nor more than one Slurm node.
