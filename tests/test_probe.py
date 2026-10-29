import os
import unittest

from fila.probe import LocalProbe, linux_idle_seconds


class LocalProbeTest(unittest.TestCase):
    """Runs against the real machine: checks the readings are consistent, not exact values."""

    def setUp(self):
        self.probe = LocalProbe()
        self.hardware = self.probe.hardware()

    def test_topology_groups_every_usable_logical_cpu_once(self):
        cpus = [cpu for core in self.hardware.topology for cpu in core]

        self.assertEqual(len(cpus), len(set(cpus)))
        self.assertEqual(len(cpus), self.hardware.logical_cpus)
        self.assertLessEqual(len(self.hardware.topology), self.hardware.logical_cpus)
        self.assertEqual(self.hardware.logical_cpus, len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count())

    def test_reading_is_within_the_hardware(self):
        self.probe.sample()
        reading = self.probe.sample()

        self.assertGreater(self.hardware.total_mem, 0)
        self.assertGreater(reading.available_mem, 0)
        self.assertLessEqual(reading.available_mem, self.hardware.total_mem)
        self.assertGreaterEqual(reading.free_cores, 0)
        self.assertLessEqual(reading.free_cores, self.hardware.logical_cpus)
        self.assertGreaterEqual(reading.idle_seconds, 0)

    def test_disk_free_is_reported_for_a_directory(self):
        self.assertGreater(self.probe.disk_free(os.getcwd()), 0)


class LinuxIdleTest(unittest.TestCase):
    """``loginctl show-session`` output of several sessions, as systemd 255 prints it."""

    def test_remote_session_that_never_idles_does_not_count(self):
        output = "Remote=no\nIdleHint=yes\nIdleSinceHint=1000000000\n\nRemote=yes\nIdleHint=no\nIdleSinceHint=0\n"

        self.assertEqual(linux_idle_seconds(output, now=1600.0), 600.0)

    def test_active_local_session_means_in_use(self):
        output = "Remote=no\nIdleHint=no\nIdleSinceHint=0\n\nRemote=no\nIdleHint=yes\nIdleSinceHint=1000000000\n"

        self.assertEqual(linux_idle_seconds(output, now=1600.0), 0.0)

    def test_only_remote_sessions_is_never_in_use(self):
        self.assertEqual(linux_idle_seconds("Remote=yes\nIdleHint=no\nIdleSinceHint=0\n", now=1600.0), float("inf"))


if __name__ == "__main__":
    unittest.main()
