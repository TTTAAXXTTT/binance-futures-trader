"""Independent stage 2 boundary review; fake GET client and temporary task DB only."""
from contextlib import closing, contextmanager
import copy
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.connection_check import ConnectionCheck
from execution.binance import ApiError
from execution.engine import Engine
from execution.store import Store
from test_hourly_engine import FakeExchange


class FakeClock:
    def __init__(self): self.mono, self.wall = 100.0, 1_800_000_000.0
    def monotonic(self): return self.mono
    def time(self): return self.wall


class ReadEvidence:
    def __init__(self): self.completed = False
    def snapshot(self): return {'completed': self.completed, 'generation': 0, 'owner': 'read-owner', 'wire_attempts': 7}


class GetOnlyClient:
    """Any attempt to use a trading write fails before a fake wire operation."""
    def __init__(self):
        self.calls, self.closed = [], False
        self.response_symbol = 'ETHUSDT'
        self.account_error = None
        self.network_hook = lambda: None
        self.network = {'generation': 0, 'recovering': False, 'waiting': False}
    @contextmanager
    def manual_connection_check(self, **kwargs):
        evidence = ReadEvidence()
        yield evidence
        evidence.completed = True
    def sync_time(self): self.calls.append(('GET', 'time')); return 1000
    def account(self):
        self.calls.append(('GET', 'account'))
        if self.account_error: raise self.account_error
        return {'canTrade': True}
    def positions(self): self.calls.append(('GET', 'positions')); return []
    def open_orders(self, symbol): self.calls.append(('GET', 'open_orders', symbol)); return []
    def open_algos(self, symbol): self.calls.append(('GET', 'open_algos', symbol)); return []
    def query_order(self, symbol, **kwargs):
        self.calls.append(('GET', 'query_order', symbol, copy.deepcopy(kwargs)))
        return {'symbol': self.response_symbol, 'status': 'NEW', 'orderId': kwargs.get('order_id'),
                'clientOrderId': kwargs.get('client_id')}
    def query_algo(self, **kwargs):
        self.calls.append(('GET', 'query_algo', copy.deepcopy(kwargs)))
        return {'symbol': self.response_symbol, 'algoStatus': 'NEW', 'algoId': kwargs.get('algo_id'),
                'clientAlgoId': kwargs.get('client_id')}
    def network_status(self): self.network_hook(); return copy.deepcopy(self.network)
    def close(self): self.closed = True
    def _write(self, *args, **kwargs): raise AssertionError('A connection check attempted a trading write')
    market_order = new_stop = new_stop_quantity = cancel_order = cancel_algo = set_cross50 = _write
    start = reconcile = _write


class StreamFixture:
    def __init__(self):
        self.connections = {'public': True, 'market': True}
        self.symbols = {s: dict(book_live=True, trade_live=True, mark_live=True)
                        for s in ('BTCUSDT', 'ETHUSDT')}
        self.requests = []
    def snapshot(self): return copy.deepcopy({'connections': self.connections, 'symbols': self.symbols})
    def request_reconnect(self, channels, **kwargs): self.requests.append(list(channels)); return {}


class InertThread:
    """A registration-only fixture, used to make cross-process DB races deterministic."""
    def __init__(self, *args, **kwargs): self.daemon = True
    def start(self): pass
    def is_alive(self): return True


class InertTimer(InertThread):
    def cancel(self): pass


class IndependentCoordinatorBoundaries(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.clock, self.client, self.streams = FakeClock(), GetOnlyClient(), StreamFixture()
        self.checker = ConnectionCheck(Path(self.temp.name) / 'tasks.sqlite', lambda: self.client,
            self.streams, clock=self.clock.time, monotonic=self.clock.monotonic, deadline_seconds=1)
        self.addCleanup(self.checker.close)

    def seed(self, context=None):
        context = context or {'symbols': {'BTCUSDT': False, 'ETHUSDT': True}, 'unknown_orders': []}
        self.checker._deadline = self.clock.mono + 1
        self.checker._task = {'version': 'manual-connection-check-v1', 'id': 'independent-check',
            'state': 'accepted', 'stage': 'accepted', 'active': True,
            'owner': self.checker.owner, 'owner_pid': self.checker.pid, 'owner_created': self.checker.created,
            'requested_at_ms': self.checker._now(), 'deadline_at_ms': self.checker._now() + 1000,
            'revision': 0, 'context': copy.deepcopy(context), 'result': {}}
        with closing(self.checker._connect()) as db: self.checker._save(db)
        return context

    def run_inline(self, context=None):
        context = self.seed(context)
        self.checker._run('independent-check', context)
        return self.checker.snapshot()

    def test_completion_after_final_network_read_exceeds_deadline_is_discarded(self):
        self.client.network_hook = lambda: setattr(self.clock, 'mono', self.clock.mono + 2)
        outcome = self.run_inline()
        self.assertNotEqual(outcome['state'], 'complete')
        self.assertIsNone(self.checker.take_notification())
        self.assertTrue(self.client.closed)

    def test_timer_callback_after_completed_task_cannot_reclassify_it(self):
        outcome = self.run_inline()
        self.assertEqual(outcome['state'], 'complete')
        self.checker._expire(outcome['id'])
        self.assertEqual(self.checker.snapshot()['state'], 'complete')

    def test_slow_final_persistence_cannot_publish_late_success(self):
        context = self.seed()
        real_save = self.checker._save
        def save_then_elapsed(db, **kwargs):
            real_save(db, **kwargs)
            if self.checker._task.get('state') == 'complete':
                self.clock.mono += 2
        with patch.object(self.checker, '_save', side_effect=save_then_elapsed):
            self.checker._run('independent-check', context)
        self.assertNotEqual(self.checker.snapshot()['state'], 'complete')
        self.assertIsNone(self.checker.take_notification())
        with closing(sqlite3.connect(self.checker.path)) as db:
            saved = json.loads(db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()[0])
        self.assertNotEqual(saved['state'], 'complete')

    def test_maintenance_close_discards_unconsumed_completion_notice(self):
        self.assertEqual(self.run_inline()['state'], 'complete')
        self.checker.close()
        self.assertIsNone(self.checker.take_notification())

    def test_unknown_order_id_with_wrong_symbol_is_not_reported_confirmed(self):
        self.client.response_symbol = 'BTCUSDT'
        context = {'unknown_orders': [{'kind': 'market', 'symbol': 'ETHUSDT',
                    'order_id': 12, 'client_id': 'original-12'}]}
        outcome = self.run_inline(context)
        observed = outcome.get('result', {}).get('unknown_orders', {})
        self.assertFalse(observed.get('ok', False))

    def test_unknown_algo_id_with_wrong_symbol_is_not_reported_confirmed(self):
        self.client.response_symbol = 'BTCUSDT'
        context = {'unknown_orders': [{'kind': 'stop', 'symbol': 'ETHUSDT',
                    'order_id': 12, 'client_id': 'original-stop'}]}
        outcome = self.run_inline(context)
        self.assertFalse(outcome.get('result', {}).get('unknown_orders', {}).get('ok', False))

    def test_identity_unavailable_is_observed_without_invalid_order_get(self):
        outcome = self.run_inline({'unknown_orders': [{'identity_unavailable': True}]})
        self.assertEqual(outcome['result']['unknown_orders']['unresolved'], 1)
        self.assertFalse(any(call[1].startswith('query_') for call in self.client.calls))

    def test_single_enabled_symbol_does_not_require_closed_symbol_quote(self):
        self.streams.symbols['BTCUSDT'] = dict(book_live=False, trade_live=False, mark_live=False)
        outcome = self.run_inline({'symbols': {'BTCUSDT': False, 'ETHUSDT': True}})
        self.assertEqual(outcome['state'], 'complete')
        self.assertEqual(self.streams.requests, [])

    def test_disabled_both_still_checks_connections_without_enabling_trading(self):
        self.assertEqual(self.run_inline({'symbols': {'BTCUSDT': False, 'ETHUSDT': False}})['state'], 'complete')
        self.assertEqual([c for c in self.client.calls if c[0] != 'GET'], [])

    def test_rest_success_market_unavailable_is_not_overall_success(self):
        self.streams.connections['market'] = False
        self.streams.symbols['ETHUSDT']['trade_live'] = False
        context = self.seed()
        # Advance virtual elapsed time at the first stream wait; no real network.
        with patch.object(self.checker._stop, 'wait', side_effect=lambda _: setattr(self.clock, 'mono', self.clock.mono + 2)):
            self.checker._run('independent-check', context)
        outcome = self.checker.snapshot()
        self.assertTrue(outcome['result']['rest']['ok'])
        self.assertFalse(outcome['result']['market']['ok'])
        self.assertNotEqual(outcome['state'], 'complete')
        self.assertEqual(self.streams.requests, [['market']])
        self.assertIsNone(self.checker.take_notification())

    def test_budget_and_exchange_cooldown_report_wait_without_notification(self):
        for kind in ('local_budget_wait', 'exchange_cooldown'):
            with self.subTest(kind=kind):
                error = ApiError(-1003, 'offline wait', recovery_kind=kind,
                    retry_at_ms=self.checker._now() + 480000, retry_in_seconds=480)
                self.client.account_error = error
                outcome = self.run_inline()
                self.assertEqual(outcome['state'], 'waiting')
                self.assertEqual(outcome['retry_in_seconds'], 480)
                self.assertIsNone(self.checker.take_notification())
                self.assertEqual(self.streams.requests, [])

    def test_new_network_generation_during_stream_phase_discards_success(self):
        self.client.network = {'generation': 1, 'recovering': False, 'waiting': False}
        outcome = self.run_inline()
        self.assertNotEqual(outcome['state'], 'complete')
        self.assertIsNone(self.checker.take_notification())

    def test_wall_clock_jump_does_not_expire_monotonic_task(self):
        context = self.seed()
        self.clock.wall += 100000
        self.checker._run('independent-check', context)
        self.assertEqual(self.checker.snapshot()['state'], 'complete')

    def test_foreign_owner_snapshot_reads_latest_durable_result(self):
        context = self.seed()
        follower = ConnectionCheck(self.checker.path, lambda: GetOnlyClient(), self.streams,
                                  owner_check=lambda *args: True)
        self.addCleanup(follower.close)
        follower.restore()
        self.assertEqual(follower.snapshot()['state'], 'accepted')
        self.checker._run('independent-check', context)
        # A bounded background refresh keeps state reads nonblocking.
        limit = time.monotonic() + .75
        while follower.snapshot()['state'] != 'complete' and time.monotonic() < limit:
            threading.Event().wait(.01)
        self.assertEqual(follower.snapshot()['state'], 'complete')

    def test_restore_cannot_overwrite_another_owner_newly_registered_task(self):
        self.seed()
        with closing(self.checker._connect()) as db:
            old = copy.deepcopy(self.checker._task)
            old.update(owner='dead-owner', owner_pid=-123, owner_created=-1)
            db.execute('UPDATE connection_check SET value=? WHERE id=1', (json.dumps(old),)); db.commit()
        newcomer = ConnectionCheck(self.checker.path, lambda: GetOnlyClient(), self.streams,
                                  owner_check=lambda *args: False)
        reply = []
        def concurrent_register(*_):
            with patch('execution.connection_check.threading.Thread', InertThread), patch('execution.connection_check.threading.Timer', InertTimer):
                try: reply.append(newcomer.request())
                except sqlite3.OperationalError: pass  # A transaction may correctly serialize admission.
            return False
        recovery = ConnectionCheck(self.checker.path, lambda: GetOnlyClient(), self.streams,
                                   owner_check=concurrent_register)
        recovery.restore()
        if not reply:
            with patch('execution.connection_check.threading.Thread', InertThread), patch('execution.connection_check.threading.Timer', InertTimer):
                reply.append(newcomer.request())
        with closing(sqlite3.connect(self.checker.path)) as db:
            saved = json.loads(db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()[0])
        self.assertEqual(saved['id'], reply[0]['connection_check']['id'])
        self.assertEqual(saved['owner'], newcomer.owner)

    def test_follower_late_read_cannot_replace_new_owned_task(self):
        context = self.seed()
        follower = ConnectionCheck(self.checker.path, lambda: GetOnlyClient(), self.streams,
                                  owner_check=lambda *args: True)
        follower._task = copy.deepcopy(self.checker._task)
        self.checker._run('independent-check', context)
        captured, release = threading.Event(), threading.Event()
        real_connect = follower._connect
        class Cursor:
            def __init__(self, underlying): self.underlying = underlying
            def fetchone(self):
                row = self.underlying.fetchone()
                if threading.current_thread().name == 'connection-inspection-observer':
                    captured.set(); release.wait(1)
                return row
        class Connection:
            def __init__(self, db): self.db = db
            def execute(self, sql, *args):
                cursor = self.db.execute(sql, *args)
                return Cursor(cursor) if sql.startswith('SELECT value FROM connection_check') else cursor
            def commit(self): return self.db.commit()
            def close(self): return self.db.close()
        with patch.object(follower, '_connect', side_effect=lambda: Connection(real_connect())):
            follower._start_follower()
            observer = follower._thread
            self.assertTrue(captured.wait(1), 'The deterministic observer never captured the old record')
            with patch('execution.connection_check.threading.Thread', InertThread), patch('execution.connection_check.threading.Timer', InertTimer):
                fresh = follower.request()
            release.set(); observer.join(1)
            try:
                self.assertEqual(follower.snapshot()['id'], fresh['connection_check']['id'])
                self.assertEqual(follower.snapshot()['state'], 'accepted')
            finally:
                follower.close()


class IndependentEngineNotificationBoundaries(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.client = FakeExchange()
        self.client.network_status = lambda: {'generation': 3, 'waiting': False, 'recovering': False, 'probing': False}
        self.engine = Engine(self.client, Store(Path(self.temp.name) / 'ledger.sqlite'))
        self.engine.desired_running = True
        self.engine.state = 'waiting'
        self.engine._recovery.update(pending=True, kind='network_read_failure')
        self.engine._recovery_next_probe = self.engine._monotonic() + 480

    def test_notification_cannot_advance_recovery_after_symbol_selection_revision(self):
        notice = {'context': {'token': self.engine.connection_check_token()},
                  'transport': {'completed': True, 'generation': 3}}
        self.engine.set_symbols({'BTCUSDT': False, 'ETHUSDT': False})
        deadline = self.engine._recovery_next_probe
        self.assertFalse(self.engine.accept_connection_check(notice))
        self.assertEqual(deadline, self.engine._recovery_next_probe)

    def test_synthetic_protection_blocker_has_no_fake_query_identity(self):
        self.engine._blocked_orders = lambda: [{'id': 't-protection-replacement'}]
        rows = self.engine.connection_check_orders()
        self.assertTrue(rows[0].get('identity_unavailable'))

    def test_unknown_cancel_uses_original_target_identity(self):
        self.engine.store.save_order({'id': 'original-stop', 'kind': 'stop', 'symbol': 'ETHUSDT',
                                     'client_id': 'original-protection', 'order_id': 77})
        self.engine._blocked_orders = lambda: [{'id': 'cancel-original', 'kind': 'cancel',
                                               'target_id': 'original-stop'}]
        rows = self.engine.connection_check_orders()
        self.assertEqual(rows, [{'kind': 'stop', 'symbol': 'ETHUSDT',
                                 'client_id': 'original-protection', 'order_id': 77}])

    def test_unknown_market_child_preserves_original_order_id_without_client_id(self):
        self.engine._blocked_orders = lambda: [{'id': 'child-ETHUSDT-77', 'kind': 'market',
            'symbol': 'ETHUSDT', 'order_id': 88, 'client_id': None}]
        rows = self.engine.connection_check_orders()
        self.assertEqual(rows[0]['order_id'], 88)
        self.assertIsNone(rows[0]['client_id'])


if __name__ == '__main__': unittest.main()
