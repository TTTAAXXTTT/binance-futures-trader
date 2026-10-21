"""Hourly console checks: temporary books, fake account and local RPC only."""
import base64
import copy
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack, closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade')]
from fastapi.testclient import TestClient
from control import hourly_app as console
REAL_REFRESH_PRIVATE = console.refresh_private
REAL_LAUNCH_WORKER = console.launch_stopped_worker
REAL_WORKER_SNAPSHOT = console.worker_snapshot


class HourlyConsoleChecks(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.folder = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.data = self.folder / 'user_data'
        self.data.mkdir()
        for name, value in {'ROOT': self.folder, 'DATA': self.data,
                'SETTINGS': self.data / 'hourly-settings.json', 'PROCESS': self.data / 'hourly-process.json',
                'CONTROLS': self.data / 'hourly-controls.json',
                'LEGACY_PROCESS': self.data / 'process.json', 'LEDGER': self.data / 'trades-live.sqlite'}.items():
            self.stack.enter_context(patch.object(console, name, value, create=True))
        self.account = {'connected': True, 'can_trade': True, 'hedge_mode': False, 'multi_asset': False,
            'positions': [], 'open_orders': 0, 'open_algo_orders': 0, 'equity_usdt': 1000,
            'leverage_limits': {'BTCUSDT': 125, 'ETHUSDT': 125}, 'account_key_fingerprint': 'private-fixture'}
        self.worker = {'connected': False, 'state': 'stopped', 'process_alive': False, 'trades': [],
            'history': [], 'unknown_orders': [],
            'markets': {'BTCUSDT': {'ready': True}, 'ETHUSDT': {'ready': True}}}
        self.legacy = {'connected': False, 'state': 'stopped', 'process_alive': False,
            'process_verified': True, 'trades': [], 'history': []}
        self.quant = {'present': True, 'unknown': False, 'raw_running': False, 'halted': True,
            'accepted_for_new_strategy': True, 'process_verified': True, 'position': False,
            'pending': 0, 'unfinished': False, 'pending_flow': False}
        self.book = {'verified': True, 'open_trades': 0}
        self.stack.enter_context(patch.dict(console.STATE, {'account': {}, 'worker': {}, 'legacy_worker': {},
            'old_service': {}, 'markets': {}, 'events': [], 'blockers': []}, clear=True))
        self.fresh_mock = self.stack.enter_context(patch.object(console, 'fresh', side_effect=self.fresh))
        self.refresh = self.stack.enter_context(patch.object(console, 'refresh_private', side_effect=lambda _client: self.fresh()))
        self.account_client_factory = self.stack.enter_context(patch.object(console.compat, 'AccountClient', return_value=object()))
        self.credentials = self.stack.enter_context(patch.object(console.compat, 'load_credentials',
            side_effect=AssertionError('Offline tests must never read real credentials')))
        self.stack.enter_context(patch.object(console.urllib.request, 'urlopen',
            side_effect=AssertionError('Offline tests must never use real network')))
        self.stack.enter_context(patch.object(console.subprocess, 'Popen',
            side_effect=AssertionError('Offline tests must never start a process')))
        self.stack.enter_context(patch('server.process_supervisor.matching_processes', return_value=[]))
        self.stack.enter_context(patch('server.process_supervisor.port_busy', return_value=False))
        self.rpc = self.stack.enter_context(patch.object(console, 'hourly_rpc', return_value={'message': 'fixture request'}))
        self.old_rpc = self.stack.enter_context(patch.object(console.compat, 'worker_rpc', return_value={'status': 'fixture'}))
        self.terminate = self.stack.enter_context(patch.object(console, 'terminate_legacy'))
        self.launch = self.stack.enter_context(patch.object(console, 'launch_stopped_worker', side_effect=self.launch_fixture))
        self.old_snapshot = self.stack.enter_context(patch.object(console, 'legacy_worker_snapshot', side_effect=lambda: copy.deepcopy(self.legacy)))
        self.new_snapshot = self.stack.enter_context(patch.object(console, 'worker_snapshot', side_effect=lambda: copy.deepcopy(self.worker)))
        self.stack.enter_context(patch.object(console.time, 'sleep', return_value=None))
        self.client = TestClient(console.app)  # No lifespan starts here.
        self.fresh()

    def fresh(self, client=None):
        console.STATE.update(account=copy.deepcopy(self.account), worker=copy.deepcopy(self.worker),
            legacy_worker=copy.deepcopy(self.legacy), old_service=copy.deepcopy(self.quant), legacy_book=copy.deepcopy(self.book),
            blockers=console.start_reasons(self.account, self.worker, self.legacy, self.quant), updated_at='fixture')
        return copy.deepcopy(console.STATE)

    def launch_fixture(self):
        # A real first worker has no durable selection even when candles are ready.
        self.worker.update(connected=True, state='stopped', warmed_up=False, strategy=console.STRATEGY,
            symbol_enabled={'BTCUSDT': False, 'ETHUSDT': False})

    def apply_marker(self):
        console.compat.atomic_json(console.SETTINGS, dict(console.FIXED, migrated=True, applied_at='fixture'))

    def post(self, endpoint, confirmed=True, symbols=None):
        body = {'confirmed': confirmed}
        if symbols is not None:
            body['symbols'] = symbols
        elif endpoint == 'start':
            body['symbols'] = {'BTCUSDT': True, 'ETHUSDT': False}
        return self.client.post('/api/' + endpoint, json=body)

    def assert_no_trading_mutation(self):
        self.rpc.assert_not_called()
        self.old_rpc.assert_not_called()
        self.launch.assert_not_called()
        self.terminate.assert_not_called()
        self.credentials.assert_not_called()

    def test_status_separates_saved_user_intent_from_waiting_execution(self):
        recovery = {'version': 'automatic-recovery-v1', 'pending': True,
                    'kind': 'network', 'reason': 'offline fixture',
                    'retry_in_seconds': 599, 'next_retry_at_ms': 123456}
        self.worker.update(connected=True, state='waiting', desired_running=True,
                           execution_recovery=recovery,
                           symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.fresh()
        snapshot = self.client.get('/api/status').json()
        self.assertTrue(snapshot['desired_running'])
        self.assertEqual(snapshot['execution_recovery'], recovery)
        self.assertEqual(snapshot['worker']['state'], 'waiting')
        self.assertEqual(snapshot['trade_controls']['symbols'], {'BTCUSDT': False, 'ETHUSDT': True})
        self.assert_no_trading_mutation()

    def test_worker_snapshot_accepts_automatic_recovery_states_without_process_fallback(self):
        for state in ('waiting', 'recovering'):
            self.rpc.return_value = {'strategy': console.STRATEGY, 'state': state, 'trades': [],
                                    'unknown_orders': [], 'desired_running': True}
            with patch.object(console, 'inspect_process', side_effect=AssertionError('Must accept valid state')):
                result = REAL_WORKER_SNAPSHOT()
            self.assertTrue(result['connected'])
            self.assertEqual(result['state'], state)

    def original_book(self):
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('CREATE TABLE IF NOT EXISTS hb_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
            db.commit()

    def test_initial_start_temporary_account_failures_forward_human_intent_to_engine(self):
        self.apply_marker()
        self.original_book()
        self.worker.update(connected=True, state='stopped', desired_running=False,
                           symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        selected = {'BTCUSDT': False, 'ETHUSDT': True}
        for kind in console.DEFERRED_START_KINDS:
            with self.subTest(kind=kind):
                self.account.update(connected=False, recovery_kind=kind, error='offline fixture')
                self.rpc.return_value = {'state': 'waiting', 'desired_running': True,
                    'execution_recovery': {'pending': True, 'kind': kind, 'retry_in_seconds': 600},
                    'message': '启动意图已保存，连接恢复后自动核对'}
                self.rpc.reset_mock()
                response = self.post('start', symbols=selected)
                self.assertEqual(response.status_code, 200, response.text)
                self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True, 'symbols': selected}, timeout=60)
                snapshot = self.client.get('/api/status').json()
                self.assertTrue(snapshot['desired_running'])
                self.assertEqual(snapshot['execution_recovery']['kind'], kind)
                self.assertEqual(snapshot['worker']['state'], 'waiting')
        self.launch.assert_not_called()
        self.refresh.assert_not_called()

    def test_initial_offline_start_can_launch_rpc_without_account_connection(self):
        self.apply_marker()
        self.original_book()
        self.account.update(connected=False, recovery_kind='network_read_failure')
        self.rpc.return_value = {'state': 'waiting', 'desired_running': True,
            'execution_recovery': {'pending': True, 'kind': 'network_read_failure'},
            'message': '启动意图已保存'}
        selected = {'BTCUSDT': False, 'ETHUSDT': True}
        response = self.post('start', symbols=selected)
        self.assertEqual(response.status_code, 200, response.text)
        self.launch.assert_called_once_with()
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True, 'symbols': selected}, timeout=60)
        self.assertEqual(response.json()['worker']['symbol_enabled'], selected)
        self.assertTrue(response.json()['worker']['desired_running'])

    def test_status_deferred_start_available_uses_structured_kind_and_static_requirements(self):
        self.account.update(connected=False, recovery_kind='network_read_wait')
        self.fresh()
        self.assertFalse(self.client.get('/api/status').json()['deferred_start_available'])
        self.apply_marker()
        self.assertFalse(self.client.get('/api/status').json()['deferred_start_available'])
        self.original_book()
        self.assertTrue(self.client.get('/api/status').json()['deferred_start_available'])
        self.worker.update(state='unknown', process_alive=True)
        self.fresh()
        self.assertFalse(self.client.get('/api/status').json()['deferred_start_available'])
        self.worker.update(state='stopped', process_alive=False)
        self.account.update(recovery_kind='credentials_error', error='network_read_wait text only')
        self.fresh()
        self.assertFalse(self.client.get('/api/status').json()['deferred_start_available'])
        self.assert_no_trading_mutation()

    def test_offline_symbol_change_with_saved_running_intent_is_rejected_without_overwriting_controls(self):
        self.original_book()
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('trading_intent_v1',
                json.dumps({'version': 1, 'desired_running': True, 'source': 'human_start'})))
            db.commit()
        initial = {'symbols': {'BTCUSDT': False, 'ETHUSDT': True}, 'version': 1, 'sentinel': 'preserved'}
        console.compat.atomic_json(console.CONTROLS, initial)
        before = console.CONTROLS.read_bytes()
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        rejected = self.post('symbols', symbols=chosen)
        self.assertEqual(rejected.status_code, 409)
        self.assertIn('后台交易进程正在恢复', rejected.json()['detail'])
        self.assertEqual(console.CONTROLS.read_bytes(), before)
        self.assert_no_trading_mutation()
        # An explicit human pause cancels that saved intent, so saving controls
        # while stopped remains possible without launching or arming a worker.
        stopped = self.post('pause')
        self.assertEqual(stopped.status_code, 200, stopped.text)
        allowed = self.post('symbols', symbols=chosen)
        self.assertEqual(allowed.status_code, 200, allowed.text)
        self.assertEqual(console.trade_controls()['symbols'], chosen)
        self.assert_no_trading_mutation()

    def test_disconnected_worker_running_intent_also_rejects_offline_symbol_changes(self):
        self.worker.update(connected=False, state='stopped', desired_running=True)
        self.assertEqual(self.post('symbols', symbols={'BTCUSDT': True, 'ETHUSDT': False}).status_code, 409)
        self.assertFalse(console.CONTROLS.exists())
        self.assert_no_trading_mutation()

    def test_permanent_or_unclassified_account_failure_is_not_deferred_by_message_text(self):
        self.apply_marker()
        self.original_book()
        for kind in ('credentials_error', 'exchange_rejected', 'permanent', None):
            with self.subTest(kind=kind):
                self.account.update(connected=False, recovery_kind=kind,
                                    error='network_read_failure 每10分钟自动重试')
                self.assertEqual(self.post('start').status_code, 409)
        self.assert_no_trading_mutation()

    def test_deferred_start_requires_existing_migration_and_original_ledger(self):
        self.account.update(connected=False, recovery_kind='network_read_wait')
        self.assertEqual(self.post('start').status_code, 409)
        self.apply_marker()
        self.assertEqual(self.post('start').status_code, 409)
        self.assert_no_trading_mutation()

    def test_deferred_start_keeps_legacy_and_unknown_order_blockers(self):
        self.apply_marker()
        self.original_book()
        self.account.update(connected=False, recovery_kind='network_read_wait')
        cases = [('legacy', {'process_alive': True}),
                 ('legacy', {'state': 'unknown'}),
                 ('worker', {'state': 'unknown', 'process_alive': True}),
                 ('worker', {'unknown_orders': [{'id': 'unverified-order'}]}),
                 ('worker', {'trades': [{'exit_intent': 'pending'}]}),
                 ('quant', {'raw_running': True}),
                 ('book', {'verified': False})]
        for target, changes in cases:
            with self.subTest(target=target, changes=changes):
                record = getattr(self, target)
                original = copy.deepcopy(record)
                record.update(changes)
                try:
                    self.assertEqual(self.post('start').status_code, 409)
                finally:
                    record.clear()
                    record.update(original)
        self.assert_no_trading_mutation()

    def test_human_pause_cancels_pending_start_when_worker_is_absent(self):
        self.original_book()
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('trading_intent_v1', json.dumps({'version': 1, 'desired_running': True})))
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('trading_start_request_v1', json.dumps({'pending': True, 'symbols': {'BTCUSDT': False, 'ETHUSDT': True}})))
            db.commit()
        response = self.post('pause')
        self.assertEqual(response.status_code, 200, response.text)
        with closing(sqlite3.connect(console.LEDGER)) as db:
            records = dict(db.execute('SELECT key,value FROM hb_meta'))
        self.assertNotIn('trading_start_request_v1', records)
        self.assertFalse(json.loads(records['trading_intent_v1'])['desired_running'])
        self.assert_no_trading_mutation()

    def test_private_refresh_preserves_structured_kind_and_clears_it_on_unclassified_error(self):
        from execution.binance import ApiError
        transient = ApiError(None, 'offline fixture', method='GET', path='/fapi/v1/time',
                             recovery_kind='network_read_failure', retry_at_ms=123456, retry_in_seconds=600)
        with patch.object(console.compat, 'read_account', side_effect=transient):
            REAL_REFRESH_PRIVATE(object())
        self.assertEqual(console.STATE['account']['recovery_kind'], 'network_read_failure')
        self.assertEqual(console.STATE['account']['retry_at_ms'], 123456)
        self.assertEqual(console.STATE['account']['retry_in_seconds'], 600)
        with patch.object(console.compat, 'read_account', side_effect=RuntimeError('network_read_failure text only')):
            REAL_REFRESH_PRIVATE(object())
        self.assertIsNone(console.STATE['account']['recovery_kind'])

    def test_technical_symbol_recovery_does_not_overwrite_running_intent(self):
        self.worker.update(connected=True, state='running', desired_running=True,
                           symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.rpc.return_value = {'state': 'recovering', 'desired_running': True,
            'symbol_enabled': {'BTCUSDT': True, 'ETHUSDT': True},
            'execution_recovery': {'pending': True, 'kind': 'configuration'}}
        result = self.post('symbols', symbols={'BTCUSDT': True, 'ETHUSDT': True})
        self.assertEqual(result.status_code, 200, result.text)
        snapshot = self.client.get('/api/status').json()
        self.assertTrue(snapshot['desired_running'])
        self.assertEqual(snapshot['worker']['state'], 'recovering')
        self.assertEqual(snapshot['execution_recovery']['kind'], 'configuration')
        self.assertEqual([call.args[0] for call in self.rpc.call_args_list], ['symbols'])

    def test_human_pause_during_technical_recovery_publishes_false_immediately(self):
        self.worker.update(connected=True, state='waiting', desired_running=True,
                           symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.rpc.return_value = {'state': 'paused', 'desired_running': False}
        result = self.post('pause')
        self.assertEqual(result.status_code, 200, result.text)
        snapshot = self.client.get('/api/status').json()
        self.assertFalse(snapshot['desired_running'])
        self.assertEqual(snapshot['trade_controls']['symbols'], {'BTCUSDT': False, 'ETHUSDT': True})
        self.assertEqual([call.args[0] for call in self.rpc.call_args_list], ['pause'])

    def test_human_pause_without_worker_updates_only_durable_intent(self):
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('CREATE TABLE hb_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
            for key, value in {'trading_intent_v1': {'version': 1, 'desired_running': True},
                               'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True},
                               'ledger-sentinel': {'wallet': '100'}}.items():
                db.execute('INSERT INTO hb_meta VALUES(?,?)', (key, json.dumps(value)))
            db.commit()
        result = self.post('pause')
        self.assertEqual(result.status_code, 200, result.text)
        with closing(sqlite3.connect(console.LEDGER)) as db:
            records = {key: json.loads(value) for key, value in db.execute('SELECT key,value FROM hb_meta')}
        self.assertIs(records['trading_intent_v1']['desired_running'], False)
        self.assertEqual(records['trading_intent_v1']['source'], 'human_pause')
        self.assertEqual(records['symbol_enabled'], {'BTCUSDT': False, 'ETHUSDT': True})
        self.assertEqual(records['ledger-sentinel'], {'wallet': '100'})
        self.assert_no_trading_mutation()

    def test_existing_recovering_worker_is_reused_without_stopped_state_requirement(self):
        from server import process_supervisor
        self.worker.update(connected=True, state='recovering', desired_running=True)
        with patch.object(console, 'inspect_process', return_value={'alive': True, 'verified': True}), \
             patch.object(process_supervisor, 'matching_processes', return_value=[{'pid': 123}]):
            # Call the implementation, not this fixture's route-launch mock.
            with patch.object(console, 'launch_stopped_worker', REAL_LAUNCH_WORKER):
                result = console.launch_stopped_worker()
        self.assertEqual(result['state'], 'recovering')
        self.credentials.assert_not_called()
        self.rpc.assert_not_called()

    def test_connected_start_reads_dynamic_state_with_one_fresh_account_client(self):
        self.apply_marker()
        self.worker.update(connected=True, state='fault', reconciled=True,
            fault_reason='fixture expired local rate limit', warmed_up=True,
            symbol_enabled={'BTCUSDT': True, 'ETHUSDT': False})
        self.rpc.return_value = {'state': 'running', 'message': 'fixture confirmed'}
        self.account_client_factory.reset_mock()
        self.fresh_mock.reset_mock()
        response = self.post('start')
        self.assertEqual(response.status_code, 200, response.text)
        self.account_client_factory.assert_called_once_with()
        self.fresh_mock.assert_called_once_with(self.account_client_factory.return_value)
        self.refresh.assert_called_once_with(self.account_client_factory.return_value)
        self.launch.assert_not_called()

    def test_new_worker_start_reuses_client_for_required_post_launch_read(self):
        self.apply_marker()
        self.rpc.return_value = {'state': 'running', 'message': 'fixture confirmed'}
        self.account_client_factory.reset_mock()
        self.fresh_mock.reset_mock()
        response = self.post('start')
        self.assertEqual(response.status_code, 200, response.text)
        self.account_client_factory.assert_called_once_with()
        self.assertEqual(self.fresh_mock.call_count, 2)
        self.assertTrue(all(call.args == (self.account_client_factory.return_value,)
            for call in self.fresh_mock.call_args_list))
        self.launch.assert_called_once_with()

    def test_status_only_fixed_strategy_old_position_is_not_new_position(self):
        self.legacy.update(connected=True, state='running', max_open_trades=2, trades=[{'trade_id': 9}])
        self.account['positions'] = [{'symbol': 'BTCUSDT', 'quantity': .003}]
        self.account['open_algo_orders'] = 1
        self.book['open_trades'] = 1
        self.fresh()
        result = self.client.get('/api/status').json()
        self.assertEqual(result['parameters']['leverage'], 50)
        self.assertEqual(result['parameters']['risk_fraction'], .50)
        self.assertEqual(result['parameters']['symbols'], ['BTCUSDT', 'ETHUSDT'])
        self.assertEqual(Path(result['installation_root']), self.folder.resolve())
        self.assertNotIn('strategy_profiles', result)
        self.assertNotIn('validation', result)
        self.assertNotIn('account_key_fingerprint', result['account'])
        self.assertFalse(result['migration']['required'])
        self.assertEqual(result['migration']['blockers'], [])
        self.assertFalse(result['worker']['connected'])
        self.assertEqual(len(result['legacy_worker']['trades']), 1)
        self.assert_no_trading_mutation()

    def test_execution_markets_override_rolling_preview(self):
        console.STATE['markets'] = {'BTCUSDT': {'preview': True, 'ema': 100}}
        console.STATE['worker'] = {'connected': True, 'state': 'running', 'markets': {'BTCUSDT': {'ema': 101, 'ready': True}}}
        result = self.client.get('/api/status').json()
        self.assertEqual(result['markets']['BTCUSDT']['ema'], 101)
        self.assertNotIn('preview', result['markets']['BTCUSDT'])

    def test_symbols_default_off_missing_or_corrupt_controls(self):
        expected = {'BTCUSDT': False, 'ETHUSDT': False}
        self.assertEqual(self.client.get('/api/status').json()['trade_controls']['symbols'], expected)
        console.CONTROLS.write_text('{broken fixture', encoding='utf-8')
        self.assertEqual(self.client.get('/api/status').json()['trade_controls']['symbols'], expected)
        console.compat.atomic_json(console.CONTROLS, {'symbols': {'BTCUSDT': 'false', 'ETHUSDT': True}})
        self.assertEqual(self.client.get('/api/status').json()['trade_controls']['symbols'], expected)
        self.assert_no_trading_mutation()

    def test_connected_worker_symbol_state_is_authoritative(self):
        console.compat.atomic_json(console.CONTROLS, {'symbols': {'BTCUSDT': False, 'ETHUSDT': False}})
        self.worker.update(connected=True, state='paused', symbol_enabled={'BTCUSDT': True, 'ETHUSDT': False})
        self.fresh()
        self.assertEqual(self.client.get('/api/status').json()['trade_controls']['symbols'],
            {'BTCUSDT': True, 'ETHUSDT': False})
        self.assert_no_trading_mutation()

    def test_offline_human_symbols_save_does_not_launch_or_start(self):
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        result = self.post('symbols', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['symbols'], chosen)
        self.assertEqual(console.trade_controls()['symbols'], chosen)
        self.assert_no_trading_mutation()

    def test_offline_paused_selection_survives_supervised_worker_restore(self):
        self.original_book()
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('trading_intent_v1',
                json.dumps({'version': 1, 'desired_running': False, 'updated_at_ms': 12345, 'source': 'human_pause'})))
            db.execute('INSERT INTO hb_meta VALUES(?,?)', ('symbol_enabled',
                json.dumps({'BTCUSDT': False, 'ETHUSDT': True})))
            db.commit()
        result = self.post('symbols', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        with closing(sqlite3.connect(console.LEDGER)) as db:
            meta = {key: json.loads(value) for key,value in db.execute('SELECT key,value FROM hb_meta')}
        self.assertEqual(meta['symbol_enabled'], chosen)
        self.assertFalse(meta['trading_intent_v1']['desired_running'])
        self.rpc.assert_not_called()
        self.launch.assert_not_called()

    def test_offline_selection_is_not_saved_if_worker_appears_under_launch_lock(self):
        self.new_snapshot.side_effect = [copy.deepcopy(self.worker),
            dict(copy.deepcopy(self.worker), connected=True, state='paused')]
        result = self.post('symbols', symbols={'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(result.status_code, 409)
        self.assertFalse(console.CONTROLS.exists())
        self.rpc.assert_not_called()

    def test_symbols_mapping_requires_exact_bool_and_supported_keys(self):
        invalid = [{}, {'BTCUSDT': True}, {'BTCUSDT': 'true', 'ETHUSDT': False},
            {'BTCUSDT': True, 'ETHUSDT': False, 'SOLUSDT': True}]
        for mapping in invalid:
            with self.subTest(mapping=mapping):
                self.assertEqual(self.post('symbols', symbols=mapping).status_code, 422)
        self.assert_no_trading_mutation()

    def test_unknown_alive_worker_cannot_save_an_unconfirmed_switch(self):
        self.worker.update(connected=False, state='unknown', process_alive=True)
        result = self.post('symbols', symbols={'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(result.status_code, 409, result.text)
        self.assertFalse(console.CONTROLS.exists())
        self.assert_no_trading_mutation()

    def test_human_symbol_disable_only_changes_entry_permission(self):
        existing = [{'id': 'fixture-trade', 'symbol': 'BTCUSDT', 'quantity': '0.004'}]
        self.worker.update(connected=True, state='running', trades=copy.deepcopy(existing),
            symbol_enabled={'BTCUSDT': True, 'ETHUSDT': False})
        chosen = {'BTCUSDT': False, 'ETHUSDT': False}
        self.rpc.return_value = {'state': 'running', 'symbol_enabled': chosen}
        result = self.post('symbols', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        self.rpc.assert_called_once_with('symbols', 'POST', {'confirmed': True, 'symbols': chosen})
        self.assertEqual(self.worker['trades'], existing)
        self.old_rpc.assert_not_called()
        self.launch.assert_not_called()
        self.terminate.assert_not_called()

    def test_pause_ack_is_visible_immediately_without_account_poll(self):
        chosen = {'BTCUSDT': False, 'ETHUSDT': True}
        existing = [{'id': 'fixture-existing-position', 'symbol': 'ETHUSDT'}]
        self.worker.update(connected=True, state='running', symbol_enabled=chosen, trades=existing)
        self.fresh()
        result = self.post('pause')
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['state'], 'paused')
        snapshot = self.client.get('/api/status').json()
        self.assertEqual(snapshot['worker']['state'], 'paused')
        self.assertEqual(snapshot['trade_controls']['symbols'], chosen)
        self.assertEqual(snapshot['worker']['trades'], existing)
        self.assertEqual(snapshot['control_revision'], result.json()['control_revision'])
        self.assertIn('worker_updated_at', snapshot)
        self.refresh.assert_not_called()
        self.old_rpc.assert_not_called()
        self.launch.assert_not_called()

    def test_symbol_disable_ack_replaces_cached_engine_selection_immediately(self):
        self.worker.update(connected=True, state='running',
            symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.fresh()
        chosen = {'BTCUSDT': False, 'ETHUSDT': False}
        self.rpc.return_value = {'state': 'running', 'symbol_enabled': chosen}
        result = self.post('symbols', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        snapshot = self.client.get('/api/status').json()
        self.assertEqual(snapshot['worker']['symbol_enabled'], chosen)
        self.assertEqual(snapshot['trade_controls']['symbols'], chosen)
        self.assertEqual(snapshot['worker']['state'], 'running')
        self.refresh.assert_not_called()

    def test_failed_pause_cannot_claim_a_confirmed_stop(self):
        self.worker.update(connected=True, state='running')
        self.fresh()
        self.rpc.side_effect = console.HTTPException(503, 'fixture RPC failure')
        result = self.post('pause')
        self.assertEqual(result.status_code, 503)
        self.assertEqual(console.STATE['worker']['state'], 'running')
        self.assertNotIn('control_revision', console.STATE)

    def test_conflicting_pause_ack_cannot_claim_a_confirmed_stop(self):
        self.worker.update(connected=True, state='running')
        self.fresh()
        self.rpc.return_value = {'state': 'running'}
        self.assertEqual(self.post('pause').status_code, 503)
        self.assertEqual(console.STATE['worker']['state'], 'running')
        self.assertNotIn('control_revision', console.STATE)

    def test_unconfirmed_symbol_response_does_not_publish_requested_flags(self):
        self.worker.update(connected=True, state='running',
            symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.fresh()
        result = self.post('symbols', symbols={'BTCUSDT': False, 'ETHUSDT': False})
        self.assertEqual(result.status_code, 503)
        self.assertTrue(console.STATE['worker']['symbol_enabled']['ETHUSDT'])
        self.assertFalse(console.CONTROLS.exists())

    def test_inflight_monitor_cannot_overwrite_newer_stop_confirmation(self):
        self.worker.update(connected=True, state='running',
            symbol_enabled={'BTCUSDT': False, 'ETHUSDT': True})
        self.fresh()
        def finish_worker_read_after_human_pause():
            older_running = copy.deepcopy(self.worker)
            console.publish_worker_control(self.worker, 'paused')
            return older_running
        with patch.object(console.compat, 'read_account', return_value=self.account), \
                patch.object(console.compat, 'old_status', return_value=self.quant), \
                patch.object(console, 'worker_snapshot', side_effect=finish_worker_read_after_human_pause):
            REAL_REFRESH_PRIVATE(object())
        self.assertEqual(console.STATE['worker']['state'], 'paused')
        self.assertTrue(console.STATE['account']['connected'])
        self.assertEqual(console.STATE['control_revision'], 1)
        self.rpc.assert_not_called()

    def test_next_monitor_can_publish_fresh_engine_state_after_confirmation(self):
        self.worker.update(connected=True, state='paused', symbol_enabled={'BTCUSDT': True, 'ETHUSDT': False})
        console.publish_worker_control(self.worker, 'paused')
        self.worker['reconciliation_reason'] = 'fixture updated account reconciliation'
        with patch.object(console.compat, 'read_account', return_value=self.account), \
                patch.object(console.compat, 'old_status', return_value=self.quant), \
                patch.object(console, 'legacy_ledger_snapshot', return_value=self.book):
            REAL_REFRESH_PRIVATE(object())
        self.assertEqual(console.STATE['worker']['reconciliation_reason'], self.worker['reconciliation_reason'])
        self.assertEqual(console.STATE['control_revision'], 1)

    def test_start_rejects_all_symbols_off_without_launch(self):
        self.apply_marker()
        result = self.post('start', symbols={'BTCUSDT': False, 'ETHUSDT': False})
        self.assertEqual(result.status_code, 422, result.text)
        self.assert_no_trading_mutation()

    def test_disabled_eth_leverage_limit_does_not_block_btc_start(self):
        self.apply_marker()
        self.account['leverage_limits']['ETHUSDT'] = 25
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        result = self.post('start', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True, 'symbols': chosen}, timeout=60)

    def test_every_human_action_requires_confirmation(self):
        for route in ['start', 'restore', 'pause', 'exit-all', 'migrate', 'legacy-pause', 'legacy-exit-all', 'symbols']:
            with self.subTest(route=route):
                self.assertEqual(self.post(route, False).status_code, 422)
        self.assert_no_trading_mutation()

    def test_retired_money_and_profile_routes_are_gone(self):
        for route in ['settings', 'strategy-profile', 'legacy-review']:
            with self.subTest(route=route):
                self.assertEqual(self.client.post('/api/' + route, json={}).status_code, 410)
        self.assert_no_trading_mutation()

    def test_cross_origin_mutations_are_rejected(self):
        result = self.client.post('/api/start', json={'confirmed': True}, headers={'Origin': 'https://fixture.invalid'})
        self.assertEqual(result.status_code, 403)
        self.assert_no_trading_mutation()

    def test_migration_rejects_account_positions_orders_and_algo_orders(self):
        for key, value in [('positions', [{'symbol': 'ETHUSDT'}]), ('open_orders', 1), ('open_algo_orders', 1)]:
            with self.subTest(key=key):
                original = self.account[key]
                self.account[key] = value
                self.assertEqual(self.post('migrate').status_code, 409)
                self.account[key] = original
        self.assert_no_trading_mutation()
        self.assertFalse(console.SETTINGS.exists())

    def test_migration_rejects_running_old_engine_and_open_book(self):
        self.legacy.update(connected=True, state='running', max_open_trades=2)
        self.assertEqual(self.post('migrate').status_code, 409)
        self.legacy.update(state='stopped', connected=False)
        self.book['open_trades'] = 1
        self.assertEqual(self.post('migrate').status_code, 409)
        self.book.update(open_trades=0, verified=False)
        self.assertEqual(self.post('migrate').status_code, 409)
        self.assert_no_trading_mutation()

    def test_migration_keeps_unreviewed_quant_lock(self):
        self.quant['accepted_for_new_strategy'] = False
        self.assertEqual(self.post('migrate').status_code, 409)
        self.assert_no_trading_mutation()
        self.assertTrue(self.quant['halted'])

    def test_human_migration_stops_only_flat_paused_legacy_and_never_launches(self):
        self.legacy.update(connected=True, state='running', max_open_trades=0)
        def old_rpc(path, method='GET', payload=None):
            self.assertEqual((path, method), ('stop', 'POST'))
            self.legacy['state'] = 'stopped'
            return {'status': 'stopped'}
        self.old_rpc.side_effect = old_rpc
        def terminate():
            self.legacy.update(connected=False, process_alive=False)
        self.terminate.side_effect = terminate
        result = self.post('migrate')
        self.assertEqual(result.status_code, 200, result.text)
        saved = console.compat.read_json(console.SETTINGS)
        self.assertTrue(saved['migrated'])
        self.assertEqual(saved['strategy'], console.STRATEGY)
        self.assertEqual(saved['risk_fraction'], .50)
        self.old_rpc.assert_called_once_with('stop', 'POST')
        self.terminate.assert_called_once()
        self.launch.assert_not_called()
        self.rpc.assert_not_called()
        self.credentials.assert_not_called()

    def test_late_position_after_stop_blocks_termination(self):
        self.legacy.update(connected=True, state='stopped')
        def old_rpc(*args, **kwargs):
            self.account['positions'] = [{'symbol': 'BTCUSDT'}]
        self.old_rpc.side_effect = old_rpc
        result = self.post('migrate')
        self.assertEqual(result.status_code, 409)
        self.terminate.assert_not_called()
        self.assertFalse(console.SETTINGS.exists())

    def test_account_failure_after_legacy_exit_cannot_apply_marker(self):
        def terminate():
            self.account['connected'] = False
        self.terminate.side_effect = terminate
        self.assertEqual(self.post('migrate').status_code, 409)
        self.assertFalse(console.SETTINGS.exists())

    def test_start_ignores_previous_strategy_reviews_and_never_sends_legacy_rpc(self):
        self.apply_marker()
        self.legacy.update(connected=True, state='running', max_open_trades=0)
        self.assertEqual(self.post('start').status_code, 200)
        self.old_rpc.assert_not_called()
        self.terminate.assert_not_called()

    def test_start_requires_one_way_single_asset_and_50x_eligibility(self):
        self.apply_marker()
        for key, value in [('hedge_mode', True), ('multi_asset', True), ('can_trade', False)]:
            with self.subTest(key=key):
                old = self.account[key]
                self.account[key] = value
                self.assertEqual(self.post('start').status_code, 409)
                self.account[key] = old
        self.account['leverage_limits']['BTCUSDT'] = 25
        self.assertEqual(self.post('start').status_code, 409)
        self.assert_no_trading_mutation()

    def test_unknown_and_fault_worker_cannot_start(self):
        self.apply_marker()
        for state in ['unknown', 'fault']:
            self.worker.update(connected=True, state=state)
            self.assertEqual(self.post('start').status_code, 409)
        self.worker.update(state='stopped', unknown_orders=[{'id': 'fixture'}])
        self.assertEqual(self.post('start').status_code, 409)
        self.assert_no_trading_mutation()

    def test_human_start_launches_stopped_then_only_new_rpc_start(self):
        self.apply_marker()
        result = self.post('start')
        self.assertEqual(result.status_code, 200, result.text)
        self.launch.assert_called_once()
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True,
            'symbols': {'BTCUSDT': True, 'ETHUSDT': False}}, timeout=60)
        self.old_rpc.assert_not_called()
        self.terminate.assert_not_called()

    def test_first_launch_checks_requested_btc_candles_not_default_off_meta(self):
        self.apply_marker()
        self.worker['markets']['ETHUSDT']['ready'] = False
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        result = self.post('start', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertFalse(self.worker['warmed_up'])
        self.assertEqual(self.worker['symbol_enabled'], {'BTCUSDT': False, 'ETHUSDT': False})
        self.launch.assert_called_once()
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True, 'symbols': chosen}, timeout=60)

    def test_requested_unwarmed_eth_rejected_after_stopped_launch(self):
        self.apply_marker()
        self.worker['markets']['ETHUSDT']['ready'] = False
        result = self.post('start', symbols={'BTCUSDT': False, 'ETHUSDT': True})
        self.assertEqual(result.status_code, 409, result.text)
        self.assertIn('ETHUSDT', result.json()['detail'])
        self.launch.assert_called_once()
        self.rpc.assert_not_called()

    def test_request_btc_only_not_blocked_by_old_unwarmed_eth_selection(self):
        self.apply_marker()
        self.worker.update(connected=True, state='paused', warmed_up=False,
            symbol_enabled={'BTCUSDT': True, 'ETHUSDT': True})
        self.worker['markets']['ETHUSDT']['ready'] = False
        chosen = {'BTCUSDT': True, 'ETHUSDT': False}
        result = self.post('start', symbols=chosen)
        self.assertEqual(result.status_code, 200, result.text)
        self.launch.assert_not_called()
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True, 'symbols': chosen}, timeout=60)

    def test_background_blockers_use_authoritative_engine_selection(self):
        self.apply_marker()
        console.compat.atomic_json(console.CONTROLS, {'symbols': {'BTCUSDT': True, 'ETHUSDT': True}})
        self.worker.update(connected=True, state='paused', warmed_up=False,
            symbol_enabled={'BTCUSDT': True, 'ETHUSDT': False})
        self.worker['markets']['ETHUSDT']['ready'] = False
        self.account['leverage_limits']['ETHUSDT'] = 25
        self.fresh()
        result = self.client.get('/api/status').json()
        self.assertEqual(result['blockers'], [])
        self.assertEqual(result['trade_controls']['symbols'], {'BTCUSDT': True, 'ETHUSDT': False})
        self.assert_no_trading_mutation()

    def test_symbol_enable_preserves_engine_paused_reason(self):
        self.worker.update(connected=True, state='running')
        self.rpc.return_value = {'state': 'paused', 'message': '新增品种已保存，请再次点击启动核对50倍全仓'}
        result = self.post('symbols', symbols={'BTCUSDT': True, 'ETHUSDT': True})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['state'], 'paused')
        self.assertIn('再次点击启动', result.json()['message'])
        self.launch.assert_not_called()
        self.old_rpc.assert_not_called()

    def test_pause_without_worker_reports_already_stopped_no_rpc(self):
        result = self.post('pause')
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['state'], 'stopped')
        self.assertIn('已经停止', result.json()['message'])
        self.assert_no_trading_mutation()

    def test_reconciled_fault_can_be_rechecked_by_human_start(self):
        self.apply_marker()
        self.worker.update(connected=True, state='fault', reconciled=True, warmed_up=True,
                           fault_reason='historical resolved fixture')
        result = self.post('start')
        self.assertEqual(result.status_code, 200, result.text)
        self.launch.assert_not_called()
        self.rpc.assert_called_once_with('start', 'POST', {'confirmed': True,
            'symbols': {'BTCUSDT': True, 'ETHUSDT': False}}, timeout=60)

    def test_fault_with_exit_intent_or_unpriced_fee_cannot_resume(self):
        self.apply_marker()
        self.worker.update(connected=True, state='fault', reconciled=True, warmed_up=True)
        for key in ['exit_intent', 'unpriced_fee']:
            with self.subTest(key=key):
                self.worker['trades'] = [{key: True}]
                self.assertEqual(self.post('start').status_code, 409)
        self.assert_no_trading_mutation()

    def test_restore_does_not_enable_entry(self):
        self.apply_marker()
        result = self.post('restore')
        self.assertEqual(result.status_code, 200, result.text)
        self.launch.assert_called_once()
        self.rpc.assert_not_called()
        self.old_rpc.assert_not_called()

    def test_legacy_human_exit_scopes_to_old_engine(self):
        self.legacy.update(connected=True, state='running', trades=[{'trade_id': 1}])
        self.assertEqual(self.post('legacy-pause').status_code, 200)
        self.assertEqual(self.post('legacy-exit-all').status_code, 200)
        self.assertEqual([(c.args[0], c.args[1]) for c in self.old_rpc.call_args_list],
            [('stopentry', 'POST'), ('stopentry', 'POST'), ('forceexit', 'POST')])
        self.assertEqual(self.old_rpc.call_args_list[-1].args[2], {'tradeid': 'all'})
        self.rpc.assert_not_called()
        self.launch.assert_not_called()

    def test_new_pause_and_exit_do_not_touch_legacy(self):
        self.worker.update(connected=True, state='running')
        self.assertEqual(self.post('pause').status_code, 200)
        self.assertEqual(self.post('exit-all').status_code, 200)
        self.assertEqual([c.args[0] for c in self.rpc.call_args_list], ['pause', 'exit-all'])
        self.old_rpc.assert_not_called()

    def test_legacy_book_is_read_only_and_retains_trade(self):
        with closing(sqlite3.connect(console.LEDGER)) as db:
            db.execute('CREATE TABLE trades(id INTEGER PRIMARY KEY,is_open INTEGER)')
            db.execute('INSERT INTO trades VALUES(9,1)')
            db.commit()
        before = console.LEDGER.read_bytes()
        result = console.legacy_ledger_snapshot()
        self.assertTrue(result['verified'])
        self.assertEqual(result['open_trades'], 1)
        self.assertEqual(result['history'][0]['trade_id'], 9)
        self.assertEqual(console.LEDGER.read_bytes(), before)


class HourlyTransportIdentityChecks(unittest.TestCase):
    def test_basic_8791_rpc_is_from_mock_auth_not_credentials(self):
        # Restore the real helper locally and replace its only IO boundary.
        response = io.BytesIO(b'{"state":"stopped"}')
        with patch.object(console, 'rpc_auth', return_value={'username': 'fixture-user', 'password': 'fixture-password'}), \
                patch.object(console.urllib.request, 'urlopen', return_value=response) as urlopen:
            result = console.hourly_rpc('status')
        self.assertEqual(result['state'], 'stopped')
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, 'http://127.0.0.1:8791/api/status')
        self.assertEqual(request.get_method(), 'GET')
        expected = 'Basic ' + base64.b64encode(b'fixture-user:fixture-password').decode()
        self.assertEqual(request.get_header('Authorization'), expected)

    def test_wrong_pid_create_time_is_not_terminated(self):
        import psutil
        process = Mock()
        process.is_running.return_value = True
        process.create_time.return_value = 200
        with patch.object(console.compat, 'read_json', return_value={'pid': 123, 'created': 100}), \
                patch.object(psutil, 'Process', return_value=process):
            identity = console.inspect_process(Path('fixture'), 'worker.py')
        self.assertFalse(identity['alive'])
        process.terminate.assert_not_called()

    def test_exact_old_script_and_config_identity_required(self):
        import psutil
        process = Mock()
        process.is_running.return_value = True
        process.create_time.return_value = 100
        process.cwd.return_value = str(console.ROOT)
        process.cmdline.return_value = ['python', str(console.ROOT / 'worker.py'), 'trade', '--config', str(console.DATA / 'wrong-config.json')]
        with patch.object(console.compat, 'read_json', return_value={'pid': 123, 'created': 100}), \
                patch.object(psutil, 'Process', return_value=process):
            identity = console.inspect_process(Path('fixture'), 'worker.py')
        self.assertTrue(identity['alive'])
        self.assertFalse(identity['verified'])
        process.terminate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
