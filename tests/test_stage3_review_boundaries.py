"""Independent review regressions: calibrated expiry, exit priority, quote-age admission."""
import copy
import sys
import unittest
from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tests'))

# Import the module, not its TestCase class: discovery must not duplicate that suite.
import test_stage3_entry_candidates as fixtures
from execution.binance import ApiError
from execution.engine import Engine
from execution.strategy import Analysis, HOUR_MS


class Stage3ReviewBoundaries(unittest.TestCase):
    def setUp(self):
        self.fx = fixtures.Stage3EntryCandidateScenarios()
        self.fx.setUp()
        self.addCleanup(self.fx.doCleanups)
        self.engine, self.client, self.store = self.fx.engine, self.fx.client, self.fx.store
        self.boundary = self.fx.boundary
        self.clock = [100.0]
        self.engine._monotonic = lambda: self.clock[0]

    def restore(self):
        self.store.set_meta('entry_candidate_activation_v1', {'version': 1,
            'activation_boundary_ms': self.boundary - HOUR_MS,
            'installed_release': '2026-10-15-stage3-entry-candidates-v8'})
        restored = Engine(self.client, self.store, activation_check=lambda: True,
                          restore_saved_intent=True, monotonic=lambda: self.clock[0])
        restored.analyses = {symbol: Analysis(True, '', 1001, Decimal('99'), Decimal('1'), self.fx.signal())
                             for symbol in ('BTCUSDT', 'ETHUSDT')}
        return restored

    def test_fast_unverified_clock_is_not_used_to_expire_before_first_sync(self):
        original = self.fx.defer_before_book()
        self.client.now = self.boundary + 900000
        restored = self.restore()
        observations = []

        def calibrate():
            observations.append(self.fx.candidate()['status'])
            self.client.now = self.boundary + 120000
            return self.client.now

        with patch.object(self.client, 'sync_time', side_effect=calibrate):
            restored.tick()
        self.assertEqual(observations, ['precheck'])
        self.assertNotEqual(self.fx.candidate()['status'], 'expired')
        self.assertEqual(self.fx.candidate()['frozen'], original['frozen'])
        self.assertEqual(len(self.fx.entry_writes()), 1)

    def test_failed_first_sync_preserves_candidate_until_verified_retry(self):
        original = self.fx.defer_before_book()
        self.client.now = self.boundary + 900000
        restored = self.restore()
        with patch.object(self.client, 'sync_time', side_effect=ApiError(None, 'offline first calibration failed')):
            restored.tick()
        self.assertTrue(restored._startup_clock_pending)
        self.assertEqual(self.fx.candidate()['status'], 'precheck')
        self.assertEqual(self.store.orders(), [])
        restored._recovery_next_probe = 0.0

        def calibrate():
            self.client.now = self.boundary + 120000
            return self.client.now

        with patch.object(self.client, 'sync_time', side_effect=calibrate):
            restored.tick()
        self.assertFalse(restored._startup_clock_pending)
        self.assertNotEqual(self.fx.candidate()['status'], 'expired')
        self.assertEqual(self.fx.candidate()['frozen'], original['frozen'])

    def test_verified_clock_expiry_still_prevents_original_window_extension(self):
        self.fx.defer_before_book()
        self.client.now = self.boundary + 900000
        restored = self.restore()

        def calibrate():
            self.client.now = self.boundary + 300000
            return self.client.now

        with patch.object(self.client, 'sync_time', side_effect=calibrate):
            restored.tick()
        self.assertEqual(self.fx.candidate()['status'], 'expired')
        self.assertEqual(self.fx.entry_writes(), [])
        self.assertEqual(self.store.orders(), [])

    def test_pending_hourly_stop_and_indicator_work_precede_other_coin_candidate_write(self):
        self.fx.defer_before_book('BTCUSDT')
        self.fx.ready()
        self.engine.last_boundary = self.boundary - HOUR_MS
        self.engine._pending_hour_boundary = self.boundary - HOUR_MS
        self.engine._hour_completed = {}
        self.engine.analyses = {symbol: Analysis(True, '', 1001, Decimal('99'), Decimal('1'),
            self.fx.signal() if symbol == 'BTCUSDT' else self.fx.signal(direction=0))
            for symbol in ('BTCUSDT', 'ETHUSDT')}
        events = []
        hourly_pending = {'stop': True, 'indicator': True}
        original_market = self.client.market_order

        def hourly_work(boundary, symbols):
            self.assertEqual(boundary, self.boundary)
            self.assertIn('ETHUSDT', symbols)
            events.append('hourly_stop')
            hourly_pending['stop'] = False
            events.append('hourly_indicator')
            hourly_pending['indicator'] = False

        def market(*args, **kwargs):
            if not kwargs.get('reduce_only'):
                self.assertFalse(any(hourly_pending.values()),
                    'A resumed BTC candidate cannot precede outstanding ETH hourly protection/exit work')
                events.append('entry_write')
            return original_market(*args, **kwargs)

        with ExitStack() as stack:
            stack.enter_context(patch.object(self.engine, 'refresh_candles'))
            stack.enter_context(patch.object(self.engine, '_hour_data_ready', return_value=True))
            stack.enter_context(patch.object(self.engine, '_hourly_exits', side_effect=hourly_work))
            stack.enter_context(patch.object(self.client, 'market_order', side_effect=market))
            self.engine.tick()
        self.assertEqual(len(self.fx.entry_writes('BTCUSDT')), 1)
        self.assertLess(events.index('hourly_stop'), events.index('entry_write'))
        self.assertLess(events.index('hourly_indicator'), events.index('entry_write'))

    def test_slow_mark_read_expires_quote_before_registration_then_fresh_read_can_execute(self):
        original_mark = self.client.mark_price

        def slow_mark(symbol):
            result = original_mark(symbol)
            self.clock[0] += 3.0
            return result

        with patch.object(self.client, 'mark_price', side_effect=slow_mark):
            with self.assertRaises(ApiError) as result:
                self.engine._entry('BTCUSDT', self.fx.signal(), True)
        self.assertTrue(result.exception.definite_not_submitted)
        before = self.fx.candidate()
        self.assertEqual(before['status'], 'precheck')
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.fx.entry_writes(), [])
        frozen = copy.deepcopy(before['frozen'])
        self.fx.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(len(self.fx.entry_writes()), 1)
        self.assertEqual(self.fx.candidate()['frozen'], frozen)

    def test_pool_queue_over_three_seconds_does_not_register_and_can_retry_fresh(self):
        original_market = self.client.market_order
        self.fx.install_admission_adapter(before_admission=lambda: self.clock.__setitem__(0, self.clock[0] + 3))
        with self.assertRaises(ApiError) as result:
            self.engine._entry('BTCUSDT', self.fx.signal(), True)
        self.assertTrue(result.exception.definite_not_submitted)
        before = self.fx.candidate()
        self.assertEqual(before['status'], 'precheck')
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.fx.entry_writes(), [])
        # Use a fresh, non-delayed explicit admission adapter for the same candidate.
        self.client.market_order = original_market
        self.fx.install_admission_adapter()
        self.fx.ready()
        self.engine._resume_entry_candidates()
        self.assertEqual(len(self.fx.entry_writes()), 1)
        self.assertEqual(len(self.store.orders()), 1)
        self.assertEqual(self.fx.candidate()['frozen'], before['frozen'])

    def test_quote_age_just_below_three_seconds_remains_eligible(self):
        self.fx.install_admission_adapter(before_admission=lambda: self.clock.__setitem__(0, self.clock[0] + 2.999))
        self.engine._entry('BTCUSDT', self.fx.signal(), True)
        self.assertEqual(len(self.fx.entry_writes()), 1)
        self.assertEqual(self.fx.candidate()['status'], 'write_intent')

    def test_quote_stales_after_atomic_registration_is_never_registered_or_sent_again(self):
        original_market = self.client.market_order
        self.fx.install_admission_adapter(after_admission=lambda: self.clock.__setitem__(0, self.clock[0] + 3))
        self.engine._entry('BTCUSDT', self.fx.signal(), True)
        candidate = copy.deepcopy(self.fx.candidate())
        self.assertEqual(candidate['status'], 'write_intent')
        original = self.store.orders()[0]
        self.assertEqual(original['state'], 'terminal')
        self.assertTrue(original['locally_not_submitted'])
        self.assertEqual(self.fx.entry_writes(), [])
        self.client.market_order = original_market
        self.client.supports_entry_before_send = False
        self.fx.ready()
        self.engine._resume_entry_candidates()
        self.engine._entry('BTCUSDT', self.fx.signal(), True)
        self.assertEqual(self.fx.entry_writes(), [])
        self.assertEqual(self.store.orders(), [original])
        self.assertEqual(self.fx.candidate(), candidate)

    def test_frozen_extra_keys_cannot_override_trade_identity_or_execution_fields(self):
        with patch.object(self.engine, 'reconcile', side_effect=ApiError(None, 'offline first snapshot unavailable')):
            with self.assertRaises(ApiError):
                self.engine._entry('BTCUSDT', self.fx.signal(), True)
        candidate = self.fx.candidate()
        self.assertIsNone(candidate['frozen'])
        frozen = {'equity0': '100', 'budget0': '50', 'notional0': '1500', 'entry_atr': '1',
                  'ledger_epoch': 'fixture-ledger-BTCUSDT'}
        for key, value in [('id', 'other-trade'), ('direction', -1), ('quantity', '999'),
                           ('branch', 'range'), ('signal_snapshot', {})]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.store.update_entry_candidate('BTCUSDT', self.boundary,
                    {'frozen': dict(frozen, **{key: value})}, candidate['revision'])
        self.assertEqual(self.fx.candidate(), candidate)
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.store.trades(), [])


if __name__ == '__main__':
    unittest.main()
