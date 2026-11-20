<!--
SPDX-FileCopyrightText: 2026 The fila Authors
SPDX-License-Identifier: GPL-3.0-or-later
-->

# Releasing fila

How fila is versioned and how a version is released. This page is the
reference for maintainers and for anyone preparing a release.

## Versioning

fila follows [Semantic Versioning 2.0.0](https://semver.org). Its public
interface is:

- the command line: subcommands, options, their defaults, the text of
  `--json` output (field names and meaning) and the exit codes;
- the configuration files: `nodes.toml` (the registry) and `client.toml`, their
  keys and defaults;
- the files written for each attempt in `<jobs_dir>/<id>/attempt-<n>/`
  (`job.json`, `status.json`, the logs) and the variables exported to jobs;
- the MCP tools and their parameters;
- compatibility between a coordinator and its nodes, given by the agent
  protocol version: a coordinator and an agent with different protocol
  versions do not work together until `fila nodes update`.

The files that define it, and that a release compares against the previous
tag:

- `src/fila/cli.py` (the parser, the exit codes, the JSON writers)
- `src/fila/registry.py` (the registry and its keys)
- `src/fila/agent.py` (`PROTOCOL`, the attempt files, the job variables)
- `src/fila/supervisor.py` (the attempt journal)
- `src/fila/mcp.py` (`TOOLS`)
- `src/fila/store.py` (`SCHEMA`: a database from an earlier version must keep
  working, through `_LATER_COLUMNS`)

| Change | From 1.0.0 | While 0.x |
|---|---|---|
| An input that worked fails or changes meaning; an output changes format; a command-line option disappears; the agent protocol changes | MAJOR | MINOR |
| A compatible feature | MINOR | MINOR |
| A fix with no interface change | PATCH | PATCH |

A changed default that alters where or how jobs run is at least a MINOR
change.

A commit that breaks the interface says so in its body:
`BREAKING: <what breaks and how to migrate>`.

## Where the version lives

The single source is `__version__` in `src/fila/__init__.py`; `pyproject.toml`
reads it. A release also updates `version` and `date-released` in
`CITATION.cff` and adds the entry to `CHANGELOG.md`.

## Release procedure

1. `main` is clean, in sync with `origin`, and the full test suite passes.
2. Compare the previous tag with `HEAD`: `BREAKING:` lines in the commit
   bodies and the interface files listed above. Choose the new version from
   the table.
3. Write the changelog entry for users (Added, Changed, Deprecated, Removed,
   Fixed, Security).
4. Update the version source, `CITATION.cff` and `CHANGELOG.md` in one commit,
   `Release vX.Y.Z`.
5. Create the annotated tag `vX.Y.Z` with the changelog entry as its message,
   and push the commit and the tag.
6. Publish a GitHub release from the tag; Zenodo archives it and assigns a DOI.

## Readiness checks

Run before every release, in addition to the test suite:

- The README quick start runs verbatim on Linux (the CI "Quick start" step).
- `reuse lint` passes.
- Every subcommand in `fila --help` appears in section 11 of the
  specification.
