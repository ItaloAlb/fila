# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The agent as a single file (section 4.2): a zipapp of this package.

``python fila.pyz <args>`` behaves like ``fila <args>``; only the standard
library is needed on the node.
"""

from __future__ import annotations

import io
import os
import zipapp
from pathlib import Path
from typing import Optional

_PACKAGE_DIR = os.path.dirname(os.path.abspath(__file__))
_archive: Optional[bytes] = None


def agent_archive() -> bytes:
    """The zipapp's bytes, built once per process from the running code."""
    global _archive
    if _archive is None:
        if not os.path.isdir(_PACKAGE_DIR):  # running from a .pyz already: send that file
            with open(os.path.dirname(_PACKAGE_DIR), "rb") as handle:
                _archive = handle.read()
            return _archive
        buffer = io.BytesIO()
        zipapp.create_archive(
            os.path.dirname(_PACKAGE_DIR),
            buffer,
            interpreter="/usr/bin/env python3",
            main="fila.cli:entry",
            filter=_only_fila_sources,
        )
        _archive = buffer.getvalue()
    return _archive


def _only_fila_sources(path: Path) -> bool:
    """The package's modules, plus the vendored packages with their license files."""
    parts = path.parts
    if parts[0] != "fila" or "__pycache__" in parts:
        return False
    if len(parts) == 1 or (len(parts) == 2 and path.suffix == ".py"):
        return True
    return parts[1] == "_vendor" and path.suffix in (".py", "")  # "": the directories and LICENSE
