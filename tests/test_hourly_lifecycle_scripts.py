"""Real SQLite and isolated PowerShell lifecycle checks; no real processes/API."""
import json
import gc
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from contextlib import closing, contextmanager

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from server import persist_stop_intent as helper
from execution.store import Store

INTENT = 'trading_intent_v1'
START = 'trading_start_request_v1'


@contextmanager
def database(path):
    with closing(sqlite3.connect(path)) as db:
        with db:
            yield db


class StopFixture(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.addCleanup(gc.collect)
        self.installation = Path(self.folder.name) / 'isolated trader'
        self.installation.mkdir()
        self.ledger = self.installation / 'user_data/trades-live.sqlite'

    def populate(self):
        self.ledger.parent.mkdir()
        store = Store(self.ledger)
        store.set_meta(INTENT, {'version': 1, 'desired_running': True,
                               'updated_at_ms': 100, 'source': 'human_start'})
        store.set_meta(START, {'version': 1, 'pending': True, 'requested_at_ms': 100})
        store.set_meta('symbol_enabled', {'BTCUSDT': False, 'ETHUSDT': True})
        store.set_meta('symbol_enabled_after', {'BTCUSDT': 3600000, 'ETHUSDT': 7200000})
        store.set_meta('fault_reason', 'offline retained fault')
        store.set_meta('execution_recovery_v1', {'pending': True, 'kind': 'order_uncertain'})
        store.set_meta('strategy_ledgers_v1', {'BTCUSDT': {'wallet': '100', 'epoch': 'offline-btc'},
                                             'ETHUSDT': {'wallet': '72.34', 'epoch': 'offline-eth'}})
        store.set_meta('funding_cursor', 12345)
        store.set_meta('funding_read_pending', True)
        store.save_trade({'id': 'offline-trade', 'status': 'open', 'quantity': '.001'})
        store.save_order({'id': 'offline-order', 'state': 'unknown', 'trade_id': 'offline-trade'})
        store.claim_fill('ETHUSDT', {'id': 7, 'orderId': 8, 'qty': '.001', 'commission': '.01'})
        store.claim_income({'tranId': 9, 'symbol': 'ETHUSDT', 'asset': 'USDT', 'income': '-.002'})
        store.save_candles('ETHUSDT', [[3600000, '100', '102', '99', '101']])
        store.claim_signal('ETHUSDT', 7200000, {'direction': -1, 'skip_reason': 'offline'})
        store.event(123, 'offline audit', {'retained': True})
        with database(self.ledger) as db:
            db.execute('CREATE TABLE legacy_positions(id INTEGER PRIMARY KEY,value TEXT)')
            db.execute('INSERT INTO legacy_positions VALUES(?,?)', (1, 'legacy retained'))
        return store

    def snapshot(self):
        with database(self.ledger) as db:
            tables = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
            return {table: list(db.execute('SELECT * FROM "' + table + '" ORDER BY rowid')) for table in tables}

    def assert_preserved(self, before):
        after = self.snapshot()
        for table in before:
            if table == 'hb_meta':
                self.assertEqual([(k, v) for k, v in before[table] if k not in {INTENT, START}],
                                 [(k, v) for k, v in after[table] if k not in {INTENT, START}])
            else:
                self.assertEqual(before[table], after[table], table)


class StopIntentTests(StopFixture):
    def test_missing_database_does_not_create_directory_or_book(self):
        self.assertFalse(helper.save_manual_stop(self.installation))
        self.assertFalse(self.ledger.exists())
        self.assertFalse(self.ledger.parent.exists())

    def test_existing_database_without_meta_is_unchanged(self):
        self.ledger.parent.mkdir()
        with database(self.ledger) as db:
            db.execute('CREATE TABLE legacy_only(id INTEGER PRIMARY KEY,value TEXT)')
            db.execute('INSERT INTO legacy_only VALUES(1,?)', ('retained',))
        before = self.snapshot()
        self.assertFalse(helper.save_manual_stop(self.installation))
        self.assertEqual(self.snapshot(), before)

    def test_manual_stop_changes_only_intent_and_cancels_pending_start(self):
        store = self.populate()
        before = self.snapshot()
        with patch.object(helper.time, 'time', return_value=1234.567):
            self.assertTrue(helper.save_manual_stop(self.installation))
        self.assertEqual(store.meta(INTENT), {'version': 1, 'desired_running': False,
            'updated_at_ms': 1234567, 'source': 'human_pause'})
        self.assertIsNone(store.meta(START))
        self.assert_preserved(before)

    def test_repeated_manual_stop_is_idempotent_for_history_and_selection(self):
        store = self.populate()
        before = self.snapshot()
        for clock in (1234, 1235):
            with patch.object(helper.time, 'time', return_value=clock):
                self.assertTrue(helper.save_manual_stop(self.installation))
        self.assertFalse(store.meta(INTENT)['desired_running'])
        self.assertEqual(store.meta(INTENT)['updated_at_ms'], 1235000)
        self.assert_preserved(before)

    def test_failed_pending_removal_rolls_back_intent_atomically(self):
        self.populate()
        with database(self.ledger) as db:
            db.execute("CREATE TRIGGER no_pending_delete BEFORE DELETE ON hb_meta "
                       "WHEN OLD.key='trading_start_request_v1' BEGIN "
                       "SELECT RAISE(ABORT,'offline forced failure'); END")
        before = self.snapshot()
        with self.assertRaises(sqlite3.Error):
            helper.save_manual_stop(self.installation)
        self.assertEqual(self.snapshot(), before)

    def test_database_disappearing_before_connect_is_never_recreated(self):
        self.populate()
        original_connect = sqlite3.connect
        def vanished(*args, **kwargs):
            self.ledger.unlink()
            return original_connect(*args, **kwargs)
        with patch.object(helper.sqlite3, 'connect', side_effect=vanished):
            try:
                helper.save_manual_stop(self.installation)
            except sqlite3.Error:
                pass
        self.assertFalse(self.ledger.exists(), 'is_file check must not allow sqlite to create a new book')

    def test_command_line_skips_missing_book_and_reports_invalid_existing_schema(self):
        scripts = self.installation / 'server'
        scripts.mkdir()
        target = scripts / 'persist_stop_intent.py'
        shutil.copy2(ROOT / 'server/persist_stop_intent.py', target)
        for populated in (False, True):
            with self.subTest(existing_invalid_book=populated):
                if populated:
                    self.ledger.parent.mkdir()
                    with database(self.ledger) as db:
                        db.execute('CREATE TABLE unrelated(value TEXT)')
                result = subprocess.run([sys.executable, '-E', '-X', 'utf8', str(target)],
                    cwd=self.installation, capture_output=True, text=True,
                    encoding='utf-8', errors='replace', timeout=20)
                if populated:
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                else:
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                if not populated:
                    self.assertFalse(self.ledger.exists())


@unittest.skipUnless(os.name == 'nt' and shutil.which('powershell.exe'), 'Isolated Windows script harness')
class StopScriptTests(StopFixture):
    """Run the actual stop script against a temp book and mocked process cmdlets."""
    def run_stop(self, *, preserve=False, simulate_last_worker_commit=False):
        script_folder = self.installation / 'server'
        script_folder.mkdir(exist_ok=True)
        source = (ROOT / 'server/Stop-Server.ps1').read_text(encoding='utf-8-sig')
        # Only the interpreter is replaced. Process/task cmdlets below are
        # shadowed; all filesystem targets remain in this temp installation.
        python_literal = "'" + sys.executable.replace("'", "''") + "'"
        source, replaced = re.subn(
            r"\$traderPython\s*=\s*Join-Path\s+\$traderRoot\s+'runtime\\python\\python\.exe'",
            lambda _: '$traderPython = ' + python_literal, source)
        self.assertGreaterEqual(replaced, 1)
        stop_file = script_folder / 'Stop-Server.ps1'
        stop_file.write_text(source, encoding='utf-8-sig')
        shutil.copy2(ROOT / 'server/persist_stop_intent.py', script_folder / 'persist_stop_intent.py')
        marker = self.installation / 'mock-process-stopped'
        racer = script_folder / 'offline-final-worker-write.py'
        racer.write_text("import json,sqlite3\nfrom pathlib import Path\n"
            "p=Path(__file__).resolve().parents[1]/'user_data/trades-live.sqlite'\n"
            "with sqlite3.connect(p) as db:\n"
            " db.execute('UPDATE hb_meta SET value=? WHERE key=?',(json.dumps({'version':1,"
            "'desired_running':True,'updated_at_ms':999,'source':'human_start'}),'trading_intent_v1'))\n"
            "(p.parents[1]/'mock-process-stopped').write_text('mocked, never killed real process')\n",
            encoding='utf-8')
        quote = lambda p: "'" + str(p).replace("'", "''") + "'"
        (self.installation / 'hourly_worker.py').write_text('# inert mock process target', encoding='utf-8')
        command_line = '"' + sys.executable + '" -E -X utf8 "' + str(self.installation / 'hourly_worker.py') + '"'
        fake = ("[pscustomobject]@{Name='python.exe';CommandLine=" + quote(command_line) +
                ";ProcessId=987654}" if simulate_last_worker_commit else '@()')
        harness = script_folder / 'isolated-harness.ps1'
        harness.write_text(
            "$ErrorActionPreference='Stop'\n"
            "function Get-ScheduledTask { param($TaskName,$ErrorAction) $null }\n"
            "function Stop-ScheduledTask { throw 'Real task stop forbidden in offline test' }\n"
            "function Wait-Process { param($Id,$Timeout,$ErrorAction)\n"
            " if ($Id -ne 987654) { throw 'Unexpected wait identity' }\n}\n"
            "function Get-CimInstance { param($ClassName) " + fake + " }\n"
            "function Stop-Process { param($Id,$ErrorAction)\n"
            " if ($Id -ne 987654) { throw 'Unexpected process identity' }\n"
            " & " + python_literal + " -E -X utf8 " + quote(racer) + "\n"
            " if ($LASTEXITCODE -ne 0) { throw 'Fake final worker commit failed' }\n}\n"
            "& " + quote(stop_file) + (' -PreserveTradingIntent' if preserve else '') + "\n",
            encoding='utf-8-sig')
        result = subprocess.run([shutil.which('powershell.exe'), '-NoProfile', '-NonInteractive',
            '-ExecutionPolicy', 'Bypass', '-File', str(harness)], cwd=self.installation,
            capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=25)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return marker

    def test_stop_script_default_persists_stop_and_preserves_the_entire_book(self):
        store = self.populate()
        before = self.snapshot()
        self.run_stop()
        self.assertFalse(store.meta(INTENT)['desired_running'])
        self.assertIsNone(store.meta(START))
        self.assert_preserved(before)
        self.assertTrue((self.installation / 'user_data/supervision-disabled').is_file())

    def test_preserve_switch_keeps_running_intent_pending_request_and_every_record(self):
        self.populate()
        before = self.snapshot()
        self.run_stop(preserve=True)
        self.assertEqual(self.snapshot(), before)
        self.assertTrue((self.installation / 'user_data/supervision-disabled').is_file())

    def test_explicit_stop_wins_over_last_worker_start_commit(self):
        store = self.populate()
        before = self.snapshot()
        marker = self.run_stop(simulate_last_worker_commit=True)
        self.assertTrue(marker.exists(), 'Mock worker stop must have run')
        self.assertFalse(store.meta(INTENT)['desired_running'], 'Post-stop intent must win over in-flight Start')
        self.assertIsNone(store.meta(START))
        self.assert_preserved(before)


if __name__ == '__main__':
    unittest.main()
