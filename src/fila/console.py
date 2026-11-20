# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Console programs started without a window on Windows (section 4.1).

The coordinator and the pull worker run as services under ``pythonw.exe``,
which has no console. Windows gives every console program such a process
starts (``ssh.exe``, a node's shell, ``schtasks``) a console window of its
own, which would pop up on the user's desktop at every server cycle.
``no_window()`` hides them. When this process has a console, its children
share it: nothing is hidden, and ``ssh`` can still ask for a passphrase there.
"""

from __future__ import annotations

import sys

CREATE_NO_WINDOW = 0x08000000


def no_window() -> int:
    """``creationflags`` for a console child of this process: 0 except on
    Windows without a console of our own."""
    if sys.platform != "win32" or _has_console():
        return 0
    return CREATE_NO_WINDOW


def _has_console() -> bool:
    try:
        import ctypes

        return bool(ctypes.windll.kernel32.GetConsoleWindow())
    except (AttributeError, OSError):  # not Windows after all: nothing to hide
        return True
