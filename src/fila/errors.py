# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Errors shared by the coordinator and the agent."""


class NodeUnreachable(Exception):
    """SSH could not reach the node or the cluster behind it (or it did not answer in time)."""


class AgentError(Exception):
    """The agent ran but the operation failed."""
