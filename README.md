<!--
SPDX-FileCopyrightText: 2026 The fila Authors
SPDX-License-Identifier: GPL-3.0-or-later
-->

# fila

<!-- Badges (CI, license, DOI) go here once the repository is public. -->

`fila` joins computers that share no network or cluster (a workstation in the
lab, a desktop at home behind NAT, an account on a Slurm cluster) into a
single queue. Like Slurm, it treats commands as opaque: it takes a shell
command with the cores, memory and time it needs, runs it on the fastest node
that can start it now, enforces those limits, and records the outcome. It is
meant for a researcher with a handful of heterogeneous machines and no
administrator: it needs only Python 3.9 and OpenSSH, uses only the standard
library, opens no ports, installs nothing on clusters, and lets agents drive it
through the command line or the Model Context Protocol.

## Features

- **Nodes of four kinds:** the coordinator's own machine, computers reached by
  SSH, computers behind NAT or a firewall that keep a channel open to the
  coordinator, and Slurm partitions driven over SSH from a registered computer.
- **A real queue:** `fila submit` with required `--cores` and `--mem`, time
  limits, dependencies, retries elsewhere when a node disappears, and
  `--dry-run`; `fila run` for short commands on the nodes' terminals now.
- **Enforced limits:** CPU affinity on whole physical cores, hard memory limits
  (cgroup v2 on Linux, Job Objects on Windows), time limits, and suspension
  while someone uses the machine.
- **Scheduling:** the fastest node per core that can start the job now,
  starvation protection, and a bounded number of pending jobs per cluster.
- **Robust to failures:** a detached supervisor per job keeps it running when
  the connection drops, and a journal on each node reconciles the state.
- **Every command speaks JSON** (`--json`) and shares one set of exit codes.
- **Linux, Windows and macOS** (macOS is experimental), with permanent
  services for the coordinator and the workers.

## Installation

Requirements: Python 3.9 or newer on every machine, and the OpenSSH client
where `fila` connects to other machines. Nothing else: `fila` uses only the
standard library, and carries a copy of tomli (MIT license) to read TOML on
Python 3.9 and 3.10.

```sh
git clone https://github.com/ItaloAlb/fila.git
cd fila
python -m pip install .
```

Without `pip`, `PYTHONPATH=src python -m fila` behaves like `fila`. Remote
nodes need no installation: registering an `ssh` node copies the agent to it
(`~/.fila/fila.pyz`).

## Quick start

Turn this Linux machine into a two-core queue, start the coordinator, and run
a job:

```sh
fila nodes register here --access local --shell "/bin/bash -lc" --jobs-dir ~/fila/jobs --cores 2 --mem 2G --speed 1
fila server &
fila submit --cores 1 --mem 100M --wait --timeout 2m -- echo hello
fila log 1
```

`register` checks the shell and probes the machine before accepting it; the
coordinator (`fila server`) launches the job in its own directory
`~/fila/jobs/1/` and `submit --wait` returns once it has ended: `1  completed`.
`fila log 1` prints the job's output, `hello`. `fila status --all` and
`fila resources` show the queue and what is free. To keep the coordinator
running as a service of your user, use `fila server --install`.

## Documentation

See [docs/index.md](docs/index.md): the [specification](docs/specification.md)
describes every concept, command and file. `fila --help` and
`fila <command> --help` list the options, and `fila guide` prints the usage
guide for agents.

## Testing

```sh
python -m unittest discover -s tests -t .
```

The suite runs the real command line against temporary homes, fake SSH
hosts and a fake Slurm, so it needs no network and no cluster.

## Citing

If you use fila in published work, please cite it as described in
[CITATION.cff](CITATION.cff) (GitHub's "Cite this repository" button reads it).

## License

fila is free software: you can redistribute it and/or modify it under the
terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version. See [LICENSE](LICENSE).

## Acknowledgements

This study was financed in part by the Coordenação de Aperfeiçoamento de
Pessoal de Nível Superior – Brasil (CAPES) – Finance Code 001.
