# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The terminal under the interactive ``fila top`` (section 11).

Two implementations with the same three methods: ``size()``, ``draw(lines,
highlight)`` and ``read_key(timeout)``, which returns a key name (``up``,
``down``, ``left``, ``right``, ``pgup``, ``pgdn``, ``home``, ``end``,
``enter``, ``tab``, ``esc``, ``backspace``, ``resize``, ``quit``) or the
character typed, or None when the timeout passed.

- ``CursesTerminal`` on Linux and macOS, with the standard ``curses``.
- ``AnsiTerminal`` on Windows, whose Python has no ``curses``: escape
  sequences (the Windows 10 console understands them once virtual terminal
  processing is on) and ``msvcrt`` for the keys. Only the standard library.

``run`` is the loop: refresh every ``interval`` seconds, draw, wait for a key.
"""

from __future__ import annotations

import os
import shutil
import sys
import time
from typing import Callable, List, Optional

# msvcrt sends two characters for the arrows and the editing keys: a prefix, then one of these.
WINDOWS_KEYS = {"H": "up", "P": "down", "K": "left", "M": "right", "I": "pgup", "Q": "pgdn", "G": "home",
                "O": "end", "S": "delete", "\x0f": "tab"}
CONTROL_KEYS = {"\r": "enter", "\n": "enter", "\t": "tab", "\x1b": "esc", "\x08": "backspace", "\x7f": "backspace",
                "\x03": "quit"}


def run(view, terminal, interval: float, clock: Callable[[], float] = time.monotonic) -> None:
    """Show ``view`` until it is done; Ctrl+C quits too."""
    view.refresh()
    next_refresh = clock() + interval
    try:
        while not view.done:
            width, height = terminal.size()
            lines, highlight = view.screen(width - 1, height)  # never the last column: no wrap, no scroll
            terminal.draw(lines, highlight)
            key = terminal.read_key(max(0.0, next_refresh - clock()))
            if key is not None:
                view.handle(key)
            if view.wants_refresh or clock() >= next_refresh:
                view.refresh()
                next_refresh = clock() + interval
    except KeyboardInterrupt:
        pass


def open_terminal():
    """The terminal for this system, to use as a context manager."""
    if sys.platform == "win32":
        return AnsiTerminal()
    return CursesTerminal()


# --- Windows ---


def decode_windows_key(first: str, read_next: Callable[[], str]) -> Optional[str]:
    """A key name from ``msvcrt.getwch()``; ``read_next`` reads the second half of a two-part key."""
    if first in ("\x00", "\xe0"):
        return WINDOWS_KEYS.get(read_next())
    return CONTROL_KEYS.get(first, first)


def ansi_frame(lines: List[str], width: int, height: int, highlight: Optional[int]) -> str:
    """One screen as escape sequences: home, each line cleared to its end, the
    highlighted one in reverse video, then the rest of the screen cleared."""
    out = ["\x1b[H"]
    for index, line in enumerate(lines[:height]):
        text = line[:max(width - 1, 1)]
        if index == highlight:
            out.append("\x1b[7m" + text.ljust(max(width - 1, 1)) + "\x1b[0m")
        else:
            out.append(text + "\x1b[K")
        if index < min(len(lines), height) - 1:
            out.append("\r\n")
    out.append("\x1b[J")
    return "".join(out)


class AnsiTerminal:
    """The Windows console through escape sequences and ``msvcrt``."""

    POLL_S = 0.02

    def __init__(self, stream=None) -> None:
        self.stream = stream or sys.stdout
        self._console = None
        self._size = None

    def __enter__(self) -> "AnsiTerminal":
        self._console = _enable_virtual_terminal()
        self.stream.write("\x1b[?1049h\x1b[?25l")  # the alternate screen, no cursor
        self.stream.flush()
        return self

    def __exit__(self, *exc) -> None:
        self.stream.write("\x1b[0m\x1b[?25h\x1b[?1049l")
        self.stream.flush()
        _restore_console(self._console)

    def size(self) -> tuple:
        self._size = tuple(shutil.get_terminal_size((100, 30)))
        return self._size

    def draw(self, lines: List[str], highlight: Optional[int]) -> None:
        width, height = self._size or self.size()
        self.stream.write(ansi_frame(lines, width, height, highlight))
        self.stream.flush()

    def read_key(self, timeout: float) -> Optional[str]:
        import msvcrt

        deadline = time.monotonic() + timeout
        while True:
            if msvcrt.kbhit():
                key = decode_windows_key(msvcrt.getwch(), msvcrt.getwch)
                if key is not None:
                    return key
            if tuple(shutil.get_terminal_size((100, 30))) != self._size:
                return "resize"
            if time.monotonic() >= deadline:
                return None
            time.sleep(self.POLL_S)


def _enable_virtual_terminal():
    """Turn on escape sequences in the Windows console; returns what to restore."""
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.GetStdHandle(-11)  # STD_OUTPUT_HANDLE
        mode = wintypes.DWORD()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return None
        kernel32.SetConsoleMode(handle, mode.value | 0x0004)  # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        return kernel32, handle, mode.value
    except (AttributeError, OSError):
        return None


def _restore_console(console) -> None:
    if console is not None:
        kernel32, handle, mode = console
        kernel32.SetConsoleMode(handle, mode)


# --- Linux and macOS ---


def curses_key(value, curses) -> Optional[str]:
    """A key name from ``window.get_wch()``: an int for special keys, a str otherwise."""
    if isinstance(value, int):
        names = {
            curses.KEY_UP: "up", curses.KEY_DOWN: "down", curses.KEY_LEFT: "left", curses.KEY_RIGHT: "right",
            curses.KEY_PPAGE: "pgup", curses.KEY_NPAGE: "pgdn", curses.KEY_HOME: "home", curses.KEY_END: "end",
            curses.KEY_ENTER: "enter", curses.KEY_BACKSPACE: "backspace", curses.KEY_RESIZE: "resize",
            curses.KEY_BTAB: "tab",
        }
        return names.get(value)
    return CONTROL_KEYS.get(value, value)


class CursesTerminal:
    def __enter__(self) -> "CursesTerminal":
        import curses
        import locale

        locale.setlocale(locale.LC_ALL, "")  # drawing characters in a UTF-8 terminal
        os.environ.setdefault("ESCDELAY", "25")  # Esc answers at once instead of after a second
        self.curses = curses
        self.window = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.window.keypad(True)
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        return self

    def __exit__(self, *exc) -> None:
        self.window.keypad(False)
        self.curses.nocbreak()
        self.curses.echo()
        self.curses.endwin()

    def size(self) -> tuple:
        height, width = self.window.getmaxyx()
        return width, height

    def draw(self, lines: List[str], highlight: Optional[int]) -> None:
        curses = self.curses
        height, width = self.window.getmaxyx()
        self.window.erase()
        for index, line in enumerate(lines[:height]):
            try:
                if index == highlight:
                    self.window.addnstr(index, 0, line.ljust(width - 1), width - 1, curses.A_REVERSE)
                else:
                    self.window.addnstr(index, 0, line, width - 1)
            except curses.error:  # a resize between size() and here
                pass
        self.window.refresh()

    def read_key(self, timeout: float) -> Optional[str]:
        self.window.timeout(max(0, int(timeout * 1000)))
        try:
            value = self.window.get_wch()
        except self.curses.error:
            return None
        return curses_key(value, self.curses)
