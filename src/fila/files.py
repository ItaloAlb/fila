"""Small JSON files shared between processes (diaries, heartbeat).

Writes are atomic (write aside, then replace), so a reader never sees half a
file. On Windows a replace fails while another process has the file open for
a moment, so both sides retry briefly.
"""

from __future__ import annotations

import json
import os
import time
from typing import Optional

_RETRIES = 50
_PAUSE_S = 0.02


def write_json(path: str, data: dict) -> None:
    temporary = f"{path}.{os.getpid()}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
    for _ in range(_RETRIES):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            time.sleep(_PAUSE_S)
    os.replace(temporary, path)


def read_json(path: str) -> Optional[dict]:
    """The file's content, or None if it does not exist (or stays unreadable)."""
    for _ in range(_RETRIES):
        try:
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return None
        except (PermissionError, ValueError):  # being replaced right now
            time.sleep(_PAUSE_S)
    return None
