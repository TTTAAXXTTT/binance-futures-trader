"""Same-window candidate acceptance, with local fake reads and no network access.

These scenarios exercise Engine's durable boundary rather than a second entry
strategy. The fake exchange is the existing lifecycle fixture; real SQLite,
reconciliation, sizing, order identity and manual-control metadata remain in use.
"""
import copy
import json
import sys
import tempfile
import unittest
from dataclasses import asdict
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tests'))

from execution.binance import ApiError, OrderUncertain, RequestExpired
from execution.engine import Engine, INTENT_META, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from execution.strategy import Analysis, HOUR_MS, Signal
from test_hourly_engine import FakeExchange, trade_row


class Stage3EntryCandidateScenarios(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / 'candidate-only.sqlite3')
        self.boundary = 1001 * HOUR_MS
        self.client = FakeExchange(self.boundary + 60_000)
        self.store.set_meta(LEDGER_META, {symbol: {
            'strategy': STRATEGY, 'version': STRATEGY_VERSION,
            'epoch': 'fixture-ledger-' + symbol, 'initialized_at_ms': 0,
            'activation_boundary_ms': 0, 'initial_wallet': '100', 'wallet': '100',
        } for symbol in ('BTCUSDT', 'ETHUSDT')})
        self.store.set_meta_many({
            INTENT_META: {'version': 1, 'desired_running': True,
                          'updated_at_ms': self.boundary - HOUR_MS, 'source': 'human_start'},
            'symbol_enabled': {'BTCUSDT': True, 'ETHUSDT': True},
            'symbol_enabled_after': {'BTCUSDT': self.boundary - HOUR_MS,
                                     'ETHUSDT': self.boundary - HOUR_MS},
        })
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        self.ready(self.engine)
        self.engine.refresh_rules()
        self.engine.reconcile()
        self.client.calls.clear()

    def ready(self, engine=None):
        """Represent the existing verified recovery gate, without a new human start."""
        engine = engine or self.engine
        engine.state, engine.fault_reason, engine.reconciled = 'running', None, True
        engine.desired_running = True
        engine._recovery.update(pending=False, kind=None, reason=None)
        self.store.set_meta('fault_reason', None)
        return engine

    def signal(self, direction=1, branch='trend', boundary=None):
        boundary = self.boundary if boundary is None else boundary
        return Signal(boundary - HOUR_MS, boundary, direction, D('1'), D('100'),
                      D('99') if direction == 1 else D('101'), D('99'), D('101'), branch)

    def candidate(self, symbol='BTCUSDT'):
        return self.store.entry_candidate(symbol, self.boundary)

    def entry_writes(self, symbol=None):
        return [r for r in self.client.calls if r[0] == 'market_order'
                and not r[2].get('reduce_only') and (symbol is None or r[1][0] == symbol)]

    def defer_before_book(self, symbol='BTCUSDT', signal=None):
        with patch.object(self.client, 'book_ticker', side_effect=ApiError(None, 'local read failure')):
            with self.assertRaises(ApiError):
                self.engine._entry(symbol, signal or self.signal(), True)
        value = self.candidate(symbol)
        self.assertIsNotNone(value, 'Qualified, definitely unsubmitted signal must remain durable')
        self.assertEqual(value['status'], 'precheck')
        self.assertEqual(self.entry_writes(), [])
        return value

    def test_first_minute_reconcile_failure_then_second_minute_one_original_entry(self):
        with patch.object(self.engine, 'reconcile', side_effect=ApiError(None, 'account GET failed')):
            with self.assertRaises(ApiError):
                self.engine._entry('BTCUSDT', self.signal(), True)
        before = self.candidate()
        self.assertIsNotNone(before)
        self.assertEqual(before['status'], 'precheck')
        self.assertIsNone(before.get('frozen'))
        self.assertEqual(self.entry_writes(), [])
        self.client.now = self.boundary + 120_000
        self.ready()
        self.engine._resume_entry_candidates()
        after = self.candidate()
        self.assertEqual(after['signal'], before['signal'])
        self.assertEqual(after['control'], before['control'])
        self.assertEqual(after['deadline_ms'], self.boundary + 300_000)
        self.assertNotEqual(after['status'], 'precheck')
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(len(self.store.trades()), 1)
        self.assertEqual(len([o for o in self.store.orders() if o['purpose'] == 'entry']), 1)

    def test_book_failure_keeps_original_signal_and_first_frozen_risk(self):
        before = self.defer_before_book()
        self.assertEqual(before['signal'], json.loads(json.dumps(asdict(self.signal()), default=str)))
        frozen = before['frozen']
        self.assertEqual(D(frozen['equity0']), D('100'))
        self.assertEqual(D(frozen['budget0']), D('50'))
        self.assertEqual(D(frozen['notional0']), D('1500'))
        self.assertEqual(D(frozen['entry_atr']), D('1'))
        self.assertEqual(frozen['ledger_epoch'], 'fixture-ledger-BTCUSDT')
        self.client.now += 60_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.candidate()['frozen'], frozen)
        self.assertEqual(len(self.entry_writes()), 1)

    def test_459_seconds_still_accepts_original_candidate(self):
        self.defer_before_book()
        self.client.now = self.boundary + 299_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(len(self.entry_writes()), 1)

    def test_500_seconds_expires_without_order_intent_or_write(self):
        self.defer_before_book()
        self.client.now = self.boundary + 300_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertNotEqual(self.candidate()['status'], 'precheck')
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.store.orders(), [])

    def test_clock_uncertainty_upper_boundary_closes_window(self):
        self.defer_before_book()
        self.client.now = self.boundary + 299_000
        self.client.now_upper_ms = lambda: self.client.now + 1000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertNotEqual(self.candidate()['status'], 'precheck')

    def test_deadline_crossed_during_fresh_read_never_submits(self):
        self.defer_before_book()
        self.client.now = self.boundary + 299_000
        original = self.client.book_ticker

        def slow_book(symbol):
            result = original(symbol)
            self.client.now = self.boundary + 300_000
            return result

        self.ready()
        with patch.object(self.client, 'book_ticker', side_effect=slow_book):
            self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])

    def test_existing_legacy_consumed_signal_is_never_migrated(self):
        old = {'direction': 1, 'branch': 'trend', 'time_ms': self.boundary + 1,
               'skip_reason': 'historical transport failure, already consumed'}
        self.assertTrue(self.store.claim_signal('BTCUSDT', self.boundary, old))
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.engine._resume_entry_candidates()
        self.assertIsNone(self.candidate())
        self.assertEqual(self.store.signal('BTCUSDT', self.boundary), old)
        self.assertEqual(self.entry_writes(), [])

    def test_pause_then_resume_same_millisecond_does_not_revive_candidate(self):
        self.defer_before_book()
        self.engine.pause()
        self.engine._persist_intent(True, 'human_start')
        self.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertNotEqual(self.candidate()['status'], 'precheck')

    def test_off_then_on_same_millisecond_does_not_revive_candidate(self):
        self.defer_before_book()
        self.engine.set_symbols({'BTCUSDT': False, 'ETHUSDT': True})
        self.engine.set_symbols({'BTCUSDT': True, 'ETHUSDT': True})
        self.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertNotEqual(self.candidate()['status'], 'precheck')

    def test_new_engine_recovers_unsubmitted_candidate_with_same_frozen_budget(self):
        before = self.defer_before_book()
        self.client.now += 60_000
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        restored.refresh_rules()
        restored.reconcile()
        self.ready(restored)
        restored._resume_entry_candidates()
        self.assertEqual(self.candidate()['frozen'], before['frozen'])
        self.assertEqual(self.candidate()['control'], before['control'])
        self.assertEqual(len(self.entry_writes()), 1)

    def test_expired_candidate_is_not_revived_after_process_restart(self):
        self.defer_before_book()
        self.client.now = self.boundary + 300_000
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        restored.refresh_rules()
        restored.reconcile()
        self.ready(restored)
        restored._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertNotEqual(self.candidate()['status'], 'precheck')

    def test_fresh_larger_equity_does_not_increase_already_frozen_risk(self):
        before = self.defer_before_book()
        frozen = copy.deepcopy(before['frozen'])
        original = self.engine.reconcile

        def higher_fresh_ledger():
            original()
            # Ledger reconstruction is tested separately; supply its newly
            # confirmed larger result to test the candidate's immutable cap.
            self.engine.strategy_ledgers['BTCUSDT']['equity'] = '200'

        self.client.now += 60_000
        self.ready()
        with patch.object(self.engine, 'reconcile', side_effect=higher_fresh_ledger):
            self.engine._resume_entry_candidates()
        trade = self.store.trades()[0]
        for field in ('equity0', 'budget0', 'notional0', 'entry_atr'):
            self.assertEqual(D(trade[field]), D(frozen[field]))
        self.assertEqual(trade['ledger_epoch'], frozen['ledger_epoch'])
        self.assertEqual(self.candidate()['frozen'], frozen)
        self.assertLessEqual(D(trade['plan']['notional']), D('1500'))
        self.assertEqual(len(self.entry_writes()), 1)

    def test_ledger_epoch_change_rejects_original_candidate_without_new_budget(self):
        before = self.defer_before_book()
        records = self.store.meta(LEDGER_META)
        records['BTCUSDT']['epoch'] = 'different-ledger-epoch'
        self.store.set_meta(LEDGER_META, records)
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.candidate()['frozen'], before['frozen'])
        self.assertNotEqual(self.candidate()['status'], 'precheck')

    def test_signal_held_at_boundary_cannot_be_used_after_later_flat(self):
        self.engine._entry('BTCUSDT', self.signal(), False)
        self.client.now += 60_000
        self.engine.positions = {}
        self.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertIsNone(self.candidate())
        self.assertEqual(self.entry_writes(), [])
        self.assertTrue(self.store.signal('BTCUSDT', self.boundary)['skip_reason'])

    def test_single_disabled_coin_does_not_revive_or_cancel_other_coin_candidate(self):
        btc = self.defer_before_book('BTCUSDT')
        eth = self.defer_before_book('ETHUSDT', self.signal(-1, 'range'))
        self.engine.set_symbols({'BTCUSDT': False, 'ETHUSDT': True})
        self.engine.set_symbols({'BTCUSDT': True, 'ETHUSDT': True})
        self.client.now += 60_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes('BTCUSDT'), [])
        self.assertEqual(len(self.entry_writes('ETHUSDT')), 1)
        self.assertNotEqual(self.candidate('BTCUSDT')['status'], 'precheck')
        self.assertEqual(self.candidate('ETHUSDT')['control'], eth['control'])
        trade = self.store.trades()[0]
        self.assertEqual(trade['symbol'], 'ETHUSDT')
        self.assertEqual(trade['branch'], 'range')
        self.assertEqual(trade['direction'], -1)
        self.assertEqual(trade['ledger_epoch'], eth['frozen']['ledger_epoch'])
        self.assertNotEqual(trade['ledger_epoch'], btc['frozen']['ledger_epoch'])

    def test_repeated_resume_and_original_entry_use_single_order_identity(self):
        self.defer_before_book()
        self.ready()
        self.engine._resume_entry_candidates()
        first = copy.deepcopy(self.candidate())
        for _ in range(5):
            self.ready()
            self.engine._resume_entry_candidates()
            self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(len(self.store.trades()), 1)
        self.assertEqual(len([o for o in self.store.orders() if o['purpose'] == 'entry']), 1)
        self.assertEqual(self.candidate()['signal'], first['signal'])

    def test_unknown_submission_is_original_query_only_after_restart(self):
        self.client.market_error = OrderUncertain(None, 'reply lost after one original write')
        self.engine._entry('BTCUSDT', self.signal(), True)
        candidate = self.candidate()
        self.assertIsNotNone(candidate)
        self.assertNotEqual(candidate['status'], 'precheck')
        self.assertEqual(len(self.entry_writes()), 1)
        order = [o for o in self.store.orders() if o['purpose'] == 'entry'][0]
        self.assertEqual(order['state'], 'unknown')
        self.client.market_error = None
        self.client.now += 60_000
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        restored.refresh_rules()
        restored.reconcile()
        self.ready(restored)
        restored._resume_entry_candidates()
        restored._entry('BTCUSDT', self.signal(), True)
        queries = [r for r in self.client.calls if r[0] == 'query_order']
        self.assertTrue(queries)
        self.assertTrue(all(r[2]['client_id'] == order['client_id'] for r in queries))
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(len([o for o in self.store.orders() if o['purpose'] == 'entry']), 1)

    def test_restored_stale_response_never_reopens_expired_candidate(self):
        self.defer_before_book()
        self.client.now = self.boundary + 300_000
        self.ready()
        self.engine._resume_entry_candidates()
        terminal = copy.deepcopy(self.candidate())
        self.client.now = self.boundary + 120_000  # A corrected clock cannot revive terminal state.
        self.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.candidate()['status'], terminal['status'])

    def test_future_hour_signal_never_enters_before_its_boundary(self):
        future = self.signal(boundary=self.boundary + HOUR_MS)
        self.engine._entry('BTCUSDT', future, True)
        self.engine._resume_entry_candidates()
        self.assertIsNone(self.store.entry_candidate('BTCUSDT', future.boundary_ms))
        self.assertEqual(self.entry_writes(), [])

    def test_price_deviation_rejection_consumes_candidate_permanently(self):
        self.defer_before_book()
        self.ready()
        with patch.object(self.client, 'book_ticker', return_value={'bidPrice': '120', 'askPrice': '120'}):
            self.engine._resume_entry_candidates()
        self.assertNotEqual(self.candidate()['status'], 'precheck')
        self.client.now += 10_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])

    def test_actual_verified_automatic_resume_preserves_only_qualified_same_window_coin(self):
        before = self.defer_before_book()
        prior_activation = self.store.meta('symbol_enabled_after')['BTCUSDT']
        self.engine.fault('local fake network interruption', kind='network_read_failure')
        self.client.now += 60_000
        self.engine.reconcile()
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('99'), D('1'), self.signal())
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        self.engine._maybe_resume(fresh_reconcile=True, generation=self.engine._fault_generation)
        self.assertEqual(self.engine.state, 'running')
        self.assertIsNone(self.engine.fault_reason)
        self.assertEqual(self.candidate()['status'], 'precheck')
        self.assertEqual(self.candidate()['control'], before['control'])
        self.assertEqual(self.store.candidate_control('BTCUSDT'), before['control'])
        self.assertEqual(self.store.meta('symbol_enabled_after')['BTCUSDT'], prior_activation)
        # ETH had no originally qualified candidate: automatic recovery cannot
        # convert its unobserved/old signal into an opportunity to trade.
        self.assertEqual(self.store.meta('symbol_enabled_after')['ETHUSDT'], self.boundary)
        self.assertTrue(self.store.signal('ETHUSDT', self.boundary)['skip_reason'])
        self.engine._resume_entry_candidates()
        self.assertEqual(len(self.entry_writes('BTCUSDT')), 1)
        self.assertEqual(self.entry_writes('ETHUSDT'), [])

    def test_unverified_automatic_resume_cannot_dispatch_pending_candidate(self):
        self.defer_before_book()
        self.engine.fault('local fake network interruption', kind='network_read_failure')
        self.client.now += 60_000
        self.engine._maybe_resume(fresh_reconcile=False, generation=self.engine._fault_generation)
        self.engine._resume_entry_candidates()
        self.assertNotEqual(self.engine.state, 'running')
        self.assertEqual(self.candidate()['status'], 'precheck')
        self.assertEqual(self.entry_writes(), [])

    def test_first_calibrated_v8_start_does_not_adopt_current_hour_legacy_signal(self):
        self.assertIsNone(self.store.meta('entry_candidate_activation_v1'))
        self.client.now = self.boundary - 2000
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        # Only a successful exchange-clock calibration supplies the cutover
        # hour. The constructor's initially wrong local boundary is not used.
        def calibrate():
            self.client._call('sync_time')
            self.client.now = self.boundary + 2000
            return self.client.now

        restored.analyses = {symbol: Analysis(True, '', 1001, D('99'), D('1'), self.signal())
                             for symbol in ('BTCUSDT', 'ETHUSDT')}
        with patch.object(self.client, 'sync_time', side_effect=calibrate):
            restored.tick()
        marker = self.store.meta('entry_candidate_activation_v1')
        self.assertIsNotNone(marker)
        self.assertEqual(marker['version'], 1)
        self.assertEqual(marker['activation_boundary_ms'], self.boundary)
        self.assertEqual(marker['installed_release'], '2026-10-15-stage3-entry-candidates-v8')
        self.ready(restored)
        restored._entry('BTCUSDT', self.signal(), True)
        self.assertIsNone(self.candidate())
        self.assertEqual(self.entry_writes(), [])
        self.client.now = self.boundary + HOUR_MS + 2000
        with patch.object(self.client, 'book_ticker', side_effect=ApiError(None, 'new hour read failure')):
            with self.assertRaises(ApiError):
                restored._entry('BTCUSDT', self.signal(boundary=self.boundary + HOUR_MS), True)
        next_candidate = self.store.entry_candidate('BTCUSDT', self.boundary + HOUR_MS)
        self.assertIsNotNone(next_candidate)
        self.assertEqual(next_candidate['status'], 'precheck')
        self.assertEqual(self.store.meta('entry_candidate_activation_v1'), marker)

    def install_admission_adapter(self, *, before_admission=None, after_admission=None):
        """Model production's explicit callback boundary without a real socket."""
        original = self.client.market_order
        self.client.supports_entry_before_send = True

        def admitted_market(symbol, side, quantity, client_id, reduce_only=False, *,
                            deadline_ms=None, before_send=None, wire_guard=None):
            self.assertFalse(reduce_only)
            self.assertTrue(callable(before_send))
            self.assertTrue(callable(wire_guard))
            if before_admission:
                before_admission()
            before_send()
            if after_admission:
                after_admission()
            wire_guard()
            return original(symbol, side, quantity, client_id, reduce_only)

        self.client.market_order = admitted_market

    def test_definite_local_budget_wait_before_atomic_admission_is_same_candidate_retry(self):
        remaining = [True]

        def local_budget():
            if remaining.pop() if remaining else False:
                raise ApiError(None, 'local budget wait', definite_not_submitted=True,
                               recovery_kind='local_budget_wait')

        self.install_admission_adapter(before_admission=local_budget)
        with self.assertRaises(ApiError):
            self.engine._entry('BTCUSDT', self.signal(), True)
        original = self.candidate()
        self.assertEqual(original['status'], 'precheck')
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.entry_writes(), [])
        self.client.now += 60_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.candidate()['signal'], original['signal'])
        self.assertEqual(self.candidate()['frozen'], original['frozen'])
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(len(self.store.orders()), 1)

    def test_human_pause_while_waiting_for_admission_prevents_any_intent_or_write(self):
        self.install_admission_adapter(before_admission=self.engine.pause)
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.candidate()['status'], 'superseded')
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.entry_writes(), [])
        self.assertFalse(self.store.meta(INTENT_META)['desired_running'])
        self.assertIsNone(self.engine.fault_reason)
        self.assertEqual(self.engine.state, 'paused')

    def test_human_pause_after_atomic_admission_is_not_a_network_fault_or_a_replay(self):
        self.install_admission_adapter(after_admission=self.engine.pause)
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.candidate()['status'], 'write_intent')
        self.assertEqual(len(self.store.orders()), 1)
        self.assertEqual(self.store.orders()[0]['state'], 'terminal')
        self.assertTrue(self.store.orders()[0]['locally_not_submitted'])
        self.assertIsNone(self.engine.fault_reason)
        self.assertEqual(self.engine.state, 'paused')
        self.engine._persist_intent(True, 'human_start')
        self.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(len(self.store.orders()), 1)

    def test_expiry_after_atomic_admission_never_reuses_or_replays_original_intent(self):
        def expired_before_wire():
            self.client.now = self.boundary + 300_000

        self.install_admission_adapter(after_admission=expired_before_wire)
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.candidate()['status'], 'write_intent')
        original = self.store.orders()[0]
        self.assertEqual(original['state'], 'terminal')
        self.assertTrue(original['locally_not_submitted'])
        self.client.now = self.boundary + 120_000
        self.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(len(self.store.orders()), 1)
        self.assertEqual(self.store.orders()[0]['client_id'], original['client_id'])

    def test_process_failure_after_atomic_intent_only_queries_original_identity(self):
        def crash_after_commit():
            raise RuntimeError('local simulated process interruption after admission')

        self.install_admission_adapter(after_admission=crash_after_commit)
        self.engine._entry('BTCUSDT', self.signal(), True)
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.candidate()['status'], 'write_intent')
        original = self.store.orders()[0]
        self.assertEqual(original['state'], 'unknown')
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        restored.refresh_rules()
        restored.reconcile()
        self.ready(restored)
        restored._resume_entry_candidates()
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(len(self.store.orders()), 1)
        queries = [row for row in self.client.calls if row[0] == 'query_order']
        self.assertTrue(queries)
        self.assertTrue(all(row[2]['client_id'] == original['client_id'] for row in queries))

    def test_manual_connection_success_routes_through_tick_gates_to_same_candidate(self):
        before = self.defer_before_book()
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('99'), D('1'), self.signal())
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        self.engine.fault('local fake network interruption', kind='network_read_failure')
        self.assertGreater(self.engine._recovery_next_probe, self.engine._monotonic())
        self.client.now = self.boundary + 120_000
        notification = {
            'context': {'token': self.engine.connection_check_token()},
            'transport': {'completed': True, 'generation': None},
        }
        self.assertTrue(self.engine.accept_connection_check(notification))
        self.assertEqual(self.entry_writes(), [], 'Accepted connection result is not entry permission')
        self.engine.tick()
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(self.candidate()['signal'], before['signal'])
        self.assertEqual(self.candidate()['frozen'], before['frozen'])
        names = [call[0] for call in self.client.calls]
        self.assertIn('account_config', names)
        self.assertIn('positions', names)
        self.assertIn('open_algos', names)

    def test_fast_constructor_clock_does_not_expire_candidate_before_first_calibration(self):
        self.store.set_meta('entry_candidate_activation_v1', {
            'version': 1, 'activation_boundary_ms': self.boundary - HOUR_MS,
            'installed_release': '2026-10-15-stage3-entry-candidates-v8',
        })
        before = self.defer_before_book()
        self.client.now = self.boundary + 400_000  # An initially wrong Windows clock.
        restored = Engine(self.client, self.store, activation_check=lambda: True, restore_saved_intent=True)
        restored.analyses = {symbol: Analysis(True, '', 1001, D('99'), D('1'), self.signal())
                             for symbol in ('BTCUSDT', 'ETHUSDT')}

        def calibrate():
            self.client._call('sync_time')
            self.client.now = self.boundary + 120_000
            return self.client.now

        with patch.object(self.client, 'sync_time', side_effect=calibrate):
            restored.tick()
        self.assertEqual(len(self.entry_writes()), 1)
        self.assertEqual(self.candidate()['signal'], before['signal'])
        self.assertEqual(self.candidate()['frozen'], before['frozen'])
        self.assertNotEqual(self.candidate()['status'], 'expired')

    def test_late_precheck_response_after_pause_and_new_start_cannot_revive_candidate(self):
        self.defer_before_book()
        original = self.client.book_ticker

        def human_actions_before_read_returns(symbol):
            result = original(symbol)
            self.engine.pause()
            self.engine._persist_intent(True, 'human_start')
            self.ready()
            return result

        with patch.object(self.client, 'book_ticker', side_effect=human_actions_before_read_returns):
            self.engine._resume_entry_candidates()
        self.assertEqual(self.candidate()['status'], 'superseded')
        self.assertEqual(self.entry_writes(), [])
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.store.trades(), [])



if __name__ == '__main__':
    unittest.main()
