"""Offline runner cleanup retries are bounded and never suppress failures."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from tools.verify_hourly import cleanup_temporary_directory


class TestCleanupChecks(unittest.TestCase):
    def directory(self):
        return SimpleNamespace(name=str(Path(tempfile.gettempdir())/'offline-cleanup-mock-only'))

    def windows_error(self, code):
        error = OSError('offline simulated file lock')
        error.winerror = code
        return error

    def test_transient_windows_file_release_is_retried(self):
        original = Mock(side_effect=[self.windows_error(5), self.windows_error(145), None])
        sleep = Mock()
        cleanup_temporary_directory(self.directory(), original, sleep=sleep, platform='win32')
        self.assertEqual(original.call_count, 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [.1, .2])

    def test_persistent_lock_remains_a_failure_after_four_attempts(self):
        original = Mock(side_effect=self.windows_error(32))
        sleep = Mock()
        with self.assertRaises(OSError):
            cleanup_temporary_directory(self.directory(), original, sleep=sleep, platform='win32')
        self.assertEqual(original.call_count, 4)
        self.assertEqual(sleep.call_count, 3)

    def test_other_errors_are_not_retried(self):
        original, sleep = Mock(side_effect=OSError('different error')), Mock()
        with self.assertRaises(OSError):
            cleanup_temporary_directory(self.directory(), original, sleep=sleep)
        self.assertEqual(original.call_count, 1)
        sleep.assert_not_called()

    def test_outside_directory_and_temp_root_are_never_cleaned(self):
        original = Mock()
        temporary_root = Path(tempfile.gettempdir()).resolve()
        for target in (temporary_root, temporary_root.parent/'outside-cleanup-mock'):
            with self.assertRaises(RuntimeError):
                cleanup_temporary_directory(SimpleNamespace(name=str(target)), original)
        original.assert_not_called()
