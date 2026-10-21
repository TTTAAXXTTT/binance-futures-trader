"""Process supervision checks: temporary ledgers and fake launchers only."""
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import nullcontext, closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/packages')]
from server import process_supervisor as supervision


class SupervisorChecks(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.data = self.root / 'user_data'
        self.data.mkdir()
        (self.root / '.env').write_text('BINANCE_API_KEY=offline-key\nBINANCE_API_SECRET=offline-secret\n')
        (self.data / 'local-auth.json').write_text(json.dumps({'username': 'original-rpc', 'password': 'offline-rpc-secret'}))
        self.ledger = self.data / 'trades-live.sqlite'
        with closing(sqlite3.connect(self.ledger)) as db:
            db.execute('CREATE TABLE hb_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('symbol_enabled', json.dumps({'BTCUSDT': False, 'ETHUSDT': True})))
            db.commit()
        self.now = 100.0
        self.alive = {'server/console_service.py': [{'pid': 100, 'created': 1}]}
        self.calls = []
        self.gate = Mock(side_effect=lambda _root: nullcontext())
        self.port_probe = Mock(return_value=False)
        self.helper = supervision.ProcessSupervisor(self.root,
            inspector=lambda _root, script: list(self.alive.get(script, [])),
            port_probe=self.port_probe, launcher=self.spawn, gate=self.gate,
            monotonic=lambda: self.now, wall=lambda: 1800000000 + self.now,
            parent_environment={'PYTHONHOME': 'ignored', 'PYTHONPATH': 'ignored', 'UNRELATED': 'preserved'})
        self.offline = patch('subprocess.Popen', side_effect=AssertionError('No real process in tests'))
        self.offline.start()
        self.addCleanup(self.offline.stop)

    def intent(self, desired):
        self.write_meta(supervision.INTENT_META, {'version': 1, 'desired_running': desired,
            'updated_at_ms': 12345, 'source': 'human_start' if desired else 'human_pause'})

    def write_meta(self, key, value):
        with closing(sqlite3.connect(self.ledger)) as db:
            db.execute('INSERT INTO hb_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                       (key, json.dumps(value)))
            db.commit()

    def records(self):
        with closing(sqlite3.connect(self.ledger)) as db:
            return db.execute('SELECT key,value FROM hb_meta ORDER BY key').fetchall()

    def spawn(self, root, script, log_name, env, *, port):
        self.calls.append((root, script, log_name, env, port))
        record = {'pid': 200 + len(self.calls), 'created': 2}
        self.alive[script] = [record]
        return record

    def test_saved_running_worker_is_rebuilt_with_existing_credentials_and_no_intent_write(self):
        self.intent(True)
        before = self.records()
        result = self.helper.check_once()
        self.assertEqual(len(self.calls), 1)
        _, script, _, env, port = self.calls[0]
        self.assertEqual(script, 'hourly_worker.py')
        self.assertEqual(port, 8791)
        self.assertEqual(env['HOURLY_RPC_USER'], 'original-rpc')
        self.assertEqual(env['HOURLY_RPC_PASSWORD'], 'offline-rpc-secret')
        self.assertEqual(env['BINANCE_API_KEY'], 'offline-key')
        self.assertNotIn('PYTHONHOME', env)
        self.assertNotIn('PYTHONPATH', env)
        self.assertEqual(env['UNRELATED'], 'preserved')
        self.assertEqual(self.records(), before)
        self.assertNotIn('offline-secret', json.dumps(result))
        self.assertNotIn('offline-rpc-secret', (self.data / 'supervisor-status.json').read_text())

    def test_manual_pause_and_single_symbol_selection_survive_worker_rebuild(self):
        self.intent(False)
        before = self.records()
        result = self.helper.check_once()
        self.assertTrue(result['processes']['hourly_worker.py']['launched'])
        self.assertEqual(self.records(), before)
        self.assertIs(supervision.saved_intent(self.root)['desired_running'], False)

    def test_missing_saved_intent_never_guesses_from_process_or_old_event(self):
        (self.data / 'hourly-process.json').write_text(json.dumps({'pid': 999999, 'created': 1}))
        result = self.helper.check_once()
        self.assertEqual(self.calls, [])
        self.assertIn('没有有效', result['processes']['hourly_worker.py']['reason'])

    def test_invalid_saved_intent_is_not_accepted_as_running(self):
        for value in ({'version': 1, 'desired_running': 'true'}, {'version': 1, 'desired_running': 1},
                      {'version': 2, 'desired_running': True}, {}, []):
            self.write_meta(supervision.INTENT_META, value)
            self.assertIsNone(supervision.saved_intent(self.root))
            self.helper.check_once()
        self.assertEqual(self.calls, [])

    def test_alive_processes_are_not_restarted_and_duplicate_is_reported_only(self):
        self.intent(True)
        self.alive['hourly_worker.py'] = [{'pid': 201, 'created': 2}, {'pid': 202, 'created': 3}]
        result = self.helper.check_once()
        self.assertEqual(self.calls, [])
        self.assertTrue(result['processes']['hourly_worker.py']['duplicate'])

    def test_repeated_checks_do_not_start_duplicate_worker(self):
        self.intent(True)
        self.helper.check_once()
        self.helper.check_once()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.gate.call_count, 4)

    def test_stop_flag_appearing_while_waiting_for_launch_lock_prevents_spawn(self):
        self.intent(True)
        @contextmanager
        def delayed_gate(_root):
            (self.data / supervision.DISABLED_FILE).write_text('human-stop')
            yield
        self.helper.gate = delayed_gate
        result = self.helper.check_once()
        self.assertFalse(result['enabled'])
        self.assertEqual(self.calls, [])

    def test_console_cannot_rebuild_supervisor_after_stop_wins_launch_lock(self):
        @contextmanager
        def delayed_gate(_root):
            (self.data / supervision.DISABLED_FILE).write_text('human-stop')
            yield
        with patch.object(supervision, 'launch_lock', delayed_gate), patch.object(supervision, 'spawn_hidden') as spawn:
            self.assertFalse(supervision.ensure_supervisor(self.root))
            spawn.assert_not_called()

    def test_occupied_unverified_port_is_never_replaced(self):
        self.intent(True)
        self.port_probe.return_value = True
        result = self.helper.check_once()
        self.assertEqual(self.calls, [])
        self.assertIn('端口已有', result['processes']['hourly_worker.py']['reason'])

    def test_failed_launch_has_bounded_retry_and_does_not_overwrite_intent(self):
        self.intent(True)
        before = self.records()
        self.helper.launcher = Mock(side_effect=RuntimeError('private failure sentinel'))
        first = self.helper.check_once()
        self.now += 29
        self.helper.check_once()
        self.assertEqual(self.helper.launcher.call_count, 1)
        self.now += 1
        self.helper.check_once()
        self.assertEqual(self.helper.launcher.call_count, 2)
        self.assertEqual(self.records(), before)
        self.assertNotIn('private failure sentinel', json.dumps(first))

    def test_missing_original_rpc_credentials_prevents_worker_launch(self):
        self.intent(True)
        (self.data / 'local-auth.json').write_text('{}')
        result = self.helper.check_once()
        self.assertEqual(self.calls, [])
        self.assertTrue(result['processes']['hourly_worker.py']['waiting'])
        self.assertEqual(json.loads((self.data / 'local-auth.json').read_text()), {})

    def test_console_exit_is_rebuilt_without_rpc_or_api_credentials(self):
        self.alive.clear()
        result = self.helper.check_once()
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0][1], 'server/console_service.py')
        self.assertIsNone(self.calls[0][3])
        self.assertTrue(result['processes']['server/console_service.py']['launched'])

    def test_disabled_supervision_does_not_launch_or_touch_ledger(self):
        self.intent(True)
        self.alive.clear()
        (self.data / supervision.DISABLED_FILE).write_text('manual local shutdown')
        before = self.records()
        result = self.helper.check_once()
        self.assertFalse(result['enabled'])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.gate.call_count, 0)
        self.assertEqual(self.records(), before)

    def test_second_lifetime_lease_is_rejected_and_release_allows_next_owner(self):
        path = self.data / 'lease.lock'
        with supervision.FileLease(path, timeout=0):
            with self.assertRaises(RuntimeError):
                with supervision.FileLease(path, timeout=0):
                    self.fail('Second supervisor must not own lease')
        with supervision.FileLease(path, timeout=0):
            pass

    def test_ensure_supervisor_reuses_existing_process(self):
        with patch.object(supervision, 'matching_processes', return_value=[{'pid': 300}]), \
             patch.object(supervision, 'spawn_hidden') as launcher:
            self.assertFalse(supervision.ensure_supervisor(self.root))
            launcher.assert_not_called()

    def test_display_keeper_checks_supervisor_without_direct_worker_launch(self):
        stop = SimpleNamespace(wait=Mock(side_effect=[False, True]))
        with patch.object(supervision, 'ensure_supervisor') as ensure:
            supervision.keep_supervisor_alive(self.root, stop)
        ensure.assert_called_once_with(self.root)
        self.assertEqual([call.args[0] for call in stop.wait.call_args_list], [15, 15])

    def test_registered_process_access_denied_is_not_misclassified_as_missing(self):
        class Denied(Exception):
            pass
        class Missing(Exception):
            pass
        (self.data / 'hourly-process.json').write_text(json.dumps({'pid': 777, 'created': 123}))
        module = SimpleNamespace(process_iter=lambda *_: [], Process=Mock(side_effect=Denied()),
                                 AccessDenied=Denied, NoSuchProcess=Missing)
        with patch.dict(sys.modules, {'psutil': module}):
            found = supervision.matching_processes(self.root, 'hourly_worker.py')
        self.assertEqual(found, [{'pid': 777, 'created': 123, 'verified': False}])

    def test_unverified_registered_worker_prevents_duplicate_launch(self):
        self.intent(True)
        self.alive['hourly_worker.py'] = [{'pid': 777, 'created': 123, 'verified': False}]
        result = self.helper.check_once()
        self.assertEqual(self.calls, [])
        self.assertIn('身份不可读取', result['processes']['hourly_worker.py']['reason'])

    def test_spawn_hidden_uses_exact_installation_and_hidden_process_flag(self):
        target = self.root / 'hourly_worker.py'
        target.write_text('# offline fixture')
        with patch.object(supervision.subprocess, 'Popen', return_value=SimpleNamespace(pid=333)) as launcher, \
             patch.dict(sys.modules, {'psutil': SimpleNamespace(
                 Process=lambda _pid: SimpleNamespace(create_time=lambda: 123))}):
            result = supervision.spawn_hidden(self.root, 'hourly_worker.py', 'fixture.log', {'FIXTURE': 'only'}, port=8791)
        self.assertEqual(result, {'pid': 333, 'created': 123, 'port': 8791})
        self.assertEqual(launcher.call_args.args[0][-1], str(target.resolve()))
        self.assertEqual(launcher.call_args.kwargs['cwd'], self.root.resolve())
        self.assertEqual(launcher.call_args.kwargs['creationflags'],
                         getattr(supervision.subprocess, 'CREATE_NO_WINDOW', 0))
        self.assertEqual(launcher.call_args.kwargs['stdin'], supervision.subprocess.DEVNULL)
        self.assertEqual(json.loads((self.data / 'hourly-process.json').read_text())['pid'], 333)


if __name__ == '__main__':
    unittest.main()
