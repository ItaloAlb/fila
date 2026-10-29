"""Section 12.1: how an agent should use fila, for any project's CLAUDE.md (``fila guide``)."""

GUIDE = """\
## Running work with `fila`

`fila` is a queue and resource manager over this user's registered nodes:
computers (local, reached by SSH, or connected from behind NAT) and Slurm
clusters. It does not know what commands do; it decides where and when they
run, enforces the declared cores, memory and time, and records the outcome.
Like Slurm, it never moves files: results stay on the node that ran the job.

Always pass `--json` when reading results. Exit codes are the same for every
command: 0 success, 1 a job or target did not succeed, 2 a target did not run,
3 a wait timed out, 4 refused, 5 usage error, 6 coordinator unavailable.

### Two ways to run

- `fila run "<command>" --on <nodes|all|online> --wait --json`: now, on each
  node's terminal, in a fresh shell starting in the user's home (or `--dir`),
  without stdin, with a 10-minute limit by default. For short commands only:
  `git pull`, builds, `ls`, checks. Clusters do not accept `run`.
- `fila submit --cores N --mem 4G [--time 2h] [--on <nodes>] [--dir D] -- <command>`:
  the queue. `--cores` and `--mem` are required and enforced (affinity, memory
  limit). Without `--on`, fila picks the fastest node per core that can start
  it now. A job runs inside one node; a request no node can hold is refused.

### The usual flow

1. Commit and push, then update and build everywhere:
   `fila run "cd ~/<repo> && git pull && cmake --build build -j" --on all --wait --json`
   (on a cluster, maintenance goes through its computer:
   `fila run --on <via> "ssh <alias> 'cd <repo> && git pull'"`; heavy builds
   there should be a `submit`).
2. Submit the computations. Each job starts in its own directory
   `<jobs_dir>/<id>/`; use `--dir` to start in the repository, and put
   `$FILA_JOB_ID` in output file names so parallel jobs never overwrite each
   other. Do not rebuild a binary that running jobs use: copy it per campaign.
   Chain jobs with `--after <ids>` (they run only if those completed).
3. Wait without polling: run `fila wait <ids> --json` in the background and
   continue; you are told when it ends.
4. Read `fila result <id> --json` and `fila log <id> --json`. If a job does not
   start, `fila explain <id> --json` says why, node by node.

### Other commands

`fila status --json` (the queue), `fila resources --json` (what is free),
`fila cancel <id> [--cascade]`, `fila retry <id> [--on <node>]`,
`fila submit ... --dry-run` (where it would go, the Slurm script for clusters),
`fila nodes list`. To fetch results, copy them with a normal command in the
direction that works: from a pull node,
`fila run "scp <file> <coordinator>:<dir>" --on <node>`; from an ssh node or a
cluster, `scp` from the coordinator's machine.

### Never

- Heavy computation with `fila run` or directly on a cluster's login node.
- Interactive commands (`vim`, `top`, a password prompt): there is no stdin.
"""
