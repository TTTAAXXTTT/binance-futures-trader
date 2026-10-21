"""File-only upgrade preserves committed WAL records; no services or API."""
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from unittest.mock import patch
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT / 'server'), str(ROOT)]
from upgrade import import_state
from execution.store import Store


class ServerUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        # WinRM can supply an ADMINI~1 temporary path; match the canonical
        # installation roots used by import_state before mocking resolution.
        self.base = Path(self.temporary.name).resolve()
        self.old, self.new = self.base / 'old', self.base / 'new'
        (self.old / 'user_data').mkdir(parents=True)
        (self.new / 'user_data').mkdir(parents=True)
        (self.old / '.env').write_text('BINANCE_API_KEY=example-key\nBINANCE_API_SECRET=example-secret\n')

    def test_latest_committed_wal_and_selections_are_copied_without_pid_or_source_reset(self):
        source = self.old / 'user_data/trades-live.sqlite'
        book = Store(source)
        with closing(sqlite3.connect(source)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('INSERT INTO hb_meta VALUES(?,?)', ('example-wallet', '"97.25"'))
            writer.commit()
            controls = {'symbols': {'BTCUSDT': False, 'ETHUSDT': True}}
            (self.old / 'user_data/hourly-controls.json').write_text(json.dumps(controls))
            (self.old / 'user_data/hourly-process.json').write_text('{"pid":999999}')
            auth = '{"username":"example-user","password":"example-rpc-password"}'
            (self.old / 'user_data/local-auth.json').write_text(auth)
            response = import_state(self.old, self.new)
            self.assertEqual(writer.execute('SELECT value FROM hb_meta WHERE key=?', ('example-wallet',)).fetchone()[0], '"97.25"')
        copied = Store(self.new / 'user_data/trades-live.sqlite')
        self.assertEqual(copied.meta('example-wallet'), '97.25')
        self.assertEqual(book.meta('example-wallet'), '97.25')
        self.assertEqual((self.new / '.env').read_bytes(), (self.old / '.env').read_bytes())
        self.assertEqual((self.new / 'user_data/local-auth.json').read_text(), auth)
        self.assertEqual(json.loads((self.new / 'user_data/hourly-controls.json').read_text()), controls)
        self.assertFalse((self.new / 'user_data/hourly-process.json').exists())
        self.assertTrue((self.old / 'user_data/hourly-process.json').exists())
        self.assertFalse(response['original_files_modified'])
        self.assertEqual(response['exchange_requests'], 0)
        settings = json.loads((self.new / 'user_data/hourly-settings.json').read_text())
        self.assertEqual(settings['strategy'], 'HourlyTrendRange')
        self.assertEqual(settings['startup_policy'], 'flat')

    def test_overlapping_roots_are_rejected_before_overwrite(self):
        Store(self.old / 'user_data/trades-live.sqlite')
        for target in (self.old, self.old / 'nested', self.base):
            with self.assertRaises(ValueError):
                import_state(self.old, target)
        self.assertEqual((self.old / '.env').read_text(), 'BINANCE_API_KEY=example-key\nBINANCE_API_SECRET=example-secret\n')

    def test_external_directory_link_is_rejected_before_copy(self):
        Store(self.old / 'user_data/trades-live.sqlite')
        original = Path.resolve
        external = self.base / 'outside-installation'
        def resolve(path, *args, **kwargs):
            if path == self.new / 'user_data':
                return external
            return original(path, *args, **kwargs)
        with patch.object(Path, 'resolve', resolve):
            with self.assertRaisesRegex(ValueError, 'user_data'):
                import_state(self.old, self.new)
        self.assertFalse((self.new / '.env').exists())


if __name__ == '__main__':
    unittest.main()
