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
    return path.parts[0] == "fila" and (len(path.parts) == 1 or (len(path.parts) == 2 and path.suffix == ".py"))
