# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""The terminal layer of the interactive ``fila top``: key decoding, the ANSI
frame, the refresh loop, and ``ssh -t`` in client mode."""

import io
import json
import os
import shutil
import sys
import tempfile
import unittest

from fila import terminal
from fila.cli import main
from fila.topview import View

from tests.test_top import job, snapshot


class WindowsKeysTest(unittest.TestCase):
    def test_two_part_keys_and_control_characters(self):
        def decode(first, second=None):
            return terminal.decode_windows_key(first, lambda: second)

        self.assertEqual([decode("\xe0", code) for code in "HPKMIQGO"],
                         ["up", "down", "left", "right", "pgup", "pgdn", "home", "end"])
        self.assertEqual(decode("\x00", "H"), "up")  # the numeric keypad sends \x00
        self.assertEqual([decode(c) for c in "\r\t\x1b\x08\x03"], ["enter", "tab", "esc", "backspace", "quit"])
        self.assertEqual((decode("q"), decode("G"), decode("?")), ("q", "G", "?"))
        self.assertIsNone(decode("\xe0", "X"))  # an unknown key is ignored


class CursesKeysTest(unittest.TestCase):
    def test_special_keys_and_characters(self):
        class FakeCurses:  # the constants curses defines; the real module is missing on Windows
            KEY_UP, KEY_DOWN, KEY_LEFT, KEY_RIGHT, KEY_PPAGE, KEY_NPAGE = 259, 258, 260, 261, 339, 338
            KEY_HOME, KEY_END, KEY_ENTER, KEY_BACKSPACE, KEY_RESIZE, KEY_BTAB = 262, 360, 343, 263, 410, 353

        decode = lambda value: terminal.curses_key(value, FakeCurses)  # noqa: E731

        self.assertEqual([decode(v) for v in (259, 258, 339, 338, 262, 360, 410)],
                         ["up", "down", "pgup", "pgdn", "home", "end", "resize"])
        self.assertEqual([decode(v) for v in ("\n", "\t", "\x1b", "\x7f", "l")], ["enter", "tab", "esc", "backspace", "l"])
        self.assertIsNone(decode(999))

    @unittest.skipIf(sys.platform == "win32", "Python on Windows has no curses")
    def test_the_real_constants_are_mapped(self):
        import curses

        self.assertEqual(terminal.curses_key(curses.KEY_UP, curses), "up")
        self.assertEqual(terminal.curses_key(curses.KEY_RESIZE, curses), "resize")


class AnsiFrameTest(unittest.TestCase):
    def test_lines_are_cleared_to_their_end_and_the_highlight_is_reversed(self):
        frame = terminal.ansi_frame(["one", "two", "three"], width=10, height=3, highlight=1)

        self.assertEqual(frame, "\x1b[Hone\x1b[K\r\n\x1b[7mtwo      \x1b[0m\r\nthree\x1b[K\x1b[J")

    def test_never_more_lines_than_the_height_nor_the_last_column(self):
        frame = terminal.ansi_frame(["x" * 20] * 5, width=10, height=2, highlight=None)

        self.assertEqual(frame.count("\r\n"), 1)  # no newline after the last line: the screen never scrolls
        self.assertNotIn("x" * 10, frame)


class FakeTerminal:
    def __init__(self, keys, size=(100, 20)):
        self.keys, self.size_ = list(keys), size
        self.frames, self.timeouts = [], []

    def size(self):
        return self.size_

    def draw(self, lines, highlight):
        self.frames.append((lines, highlight))

    def read_key(self, timeout):
        self.timeouts.append(timeout)
        return self.keys.pop(0) if self.keys else "q"


class RunTest(unittest.TestCase):
    def test_the_loop_draws_handles_keys_and_refreshes_on_time(self):
        now = [0.0]
        fetched = []

        def fetch(show_finished):
            fetched.append(now[0])
            return snapshot(jobs=[job(1), job(2)])

        class Clock:  # every key takes 1.5 s
            def __call__(self):
                return now[0]

        screen = FakeTerminal(["down", None, "a", None, "q"])
        original = screen.read_key

        def read_key(timeout):
            now[0] += 1.5
            return original(timeout)

        screen.read_key = read_key
        view = View(fetch, lambda argv: (0, "", ""), interval=2)

        terminal.run(view, screen, interval=2.0, clock=Clock())

        self.assertTrue(view.done)
        self.assertEqual(view.selected, 2)
        self.assertEqual(fetched, [0.0, 3.0, 4.5, 7.5])  # start, every 2 s at the next key, and right after "a"
        self.assertEqual(len(screen.frames[0][0]), 20)  # the whole height
        self.assertTrue(all(len(line) <= 99 for line in screen.frames[0][0]))  # never the last column

    def test_ctrl_c_quits_quietly(self):
        class Interrupting(FakeTerminal):
            def read_key(self, timeout):
                raise KeyboardInterrupt

        view = View(lambda show_finished: snapshot(), None, interval=2)

        terminal.run(view, Interrupting([]), interval=2.0)  # no exception


class ClientModeTest(unittest.TestCase):
    """In client mode the interactive screen runs on the coordinator through ``ssh -t``."""

    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="fila-client-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self.record = os.path.join(self.home, "ssh-argv.json")
        fake_ssh = os.path.join(self.home, "fake_ssh.py")
        with open(fake_ssh, "w") as handle:
            handle.write(f"import json, sys\njson.dump(sys.argv[1:], open({self.record!r}, 'w'))\n")
        with open(os.path.join(self.home, "client.toml"), "w") as handle:
            handle.write(f'coordinator = "user@lab"\nssh = {json.dumps([sys.executable, fake_ssh])}\n')

    def test_interactive_top_goes_through_ssh_t(self):
        code = main(["top", "--interval", "5"], home=self.home, stdout=io.StringIO(), stderr=io.StringIO())

        self.assertEqual(code, 0)
        with open(self.record) as handle:
            self.assertEqual(json.load(handle), ["-t", "user@lab", "fila top --interval 5"])

    def test_once_and_local_are_not_sent_through_a_terminal(self):
        self.assertFalse(any(map(terminal_forwarded, (["top", "--once"], ["top", "--json"], ["top", "--local"]))))
        self.assertTrue(terminal_forwarded(["top", "--all"]))


def terminal_forwarded(argv):
    from fila.cli import _interactive_top

    return _interactive_top(argv)


if __name__ == "__main__":
    unittest.main()
