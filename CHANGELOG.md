<!--
SPDX-FileCopyrightText: 2026 The fila Authors
SPDX-License-Identifier: GPL-3.0-or-later
-->

# Changelog

All notable changes to fila are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html);
[docs/releasing.md](docs/releasing.md) defines what counts as its public
interface.

## [0.1.0] - 2026-11-20

### Added

- A coordinator (`fila server`) that keeps a queue in SQLite and schedules
  jobs on registered nodes: the coordinator's own machine (`local`), computers
  reached by SSH (`ssh`), computers behind NAT that keep a channel open to the
  coordinator (`pull`), and Slurm clusters driven over SSH without an agent.
- `fila submit` for queued jobs with required `--cores` and `--mem`, optional
  `--time`, dependencies (`--after`, `--even-if-failed`), automatic retries
  elsewhere (`--retry-after`), Slurm options and `--dry-run`; `fila run` for
  short commands on the nodes' terminals now.
- Enforced limits on computers: CPU affinity on whole physical cores, hard
  memory limits (cgroup v2 on Linux, Job Objects on Windows, measured
  elsewhere) and time limits; suspension while the machine's user is at the
  keyboard.
- Scheduling by speed per core with starvation protection and a bounded number
  of pending jobs per Slurm cluster.
- `status`, `resources`, `result`, `log`, `explain`, `wait`, `cancel`,
  `retry`, `suspend`, `resume`, `clean`, `selftest` and `nodes` subcommands,
  each with `--json` and documented exit codes.
- Permanent services for the coordinator and for `pull` workers on Linux
  (systemd), macOS (launchd) and Windows (Task Scheduler).
- Client mode: any machine that reaches the coordinator over SSH runs the same
  commands.
- An MCP server (`fila mcp`) and a usage guide for agents (`fila guide`).

[0.1.0]: https://github.com/ItaloAlb/fila/releases/tag/v0.1.0
