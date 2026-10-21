"""Offline candidate review: real bridge state machine and local exchange only."""
import copy
import hashlib
import importlib.util
import json
import sys
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
import test_hourly_stop_replacement_live as fixture
from execution.binance import ApiError, OrderUncertain
from test_hourly_engine import FakeExchange
from execution import engine as module

class StopCapabilityChecks(fixture.StopReplacementLiveTests):
    def setUp(self):
        super().setUp()
        self.client._key = 'offline-account-a'
        original = self.client.new_stop
        def confirmed_rejection(*args, **kwargs):
            try:
                return original(*args, **kwargs)
            except ApiError as exc:
                if exc.code == -4130:
                    exc.definite_rejected = True
                raise
        self.client.new_stop = confirmed_rejection

    def learn(self):
        self.start_replacement()
        self.assertTrue(self.engine._close_all_overlap_rejected('BTCUSDT'))
        state = self.drive()
        self.assertEqual(state['phase'], 'complete')
        return self.store.trade('trade-1')

    def test_capability_first_actual_rejection_is_persisted_without_key(self):
        self.start_replacement()
        marker = self.store.meta(module.STOP_CAPABILITY_META)
        self.assertTrue(self.engine._close_all_overlap_rejected('BTCUSDT'))
        self.assertNotIn(self.client._key, json.dumps(marker))
        self.assertEqual(len(self.writes('new_stop')), 1)
        self.assertEqual(len(self.writes('new_stop_quantity')), 1)
        self.assertEqual(self.client.algo_rows[400]['algoStatus'], 'NEW')

    def test_capability_second_replacement_skips_rejected_close_all_post(self):
        trade = self.learn()
        before_close = len(self.writes('new_stop'))
        before_bridge = len(self.writes('new_stop_quantity'))
        self.engine._ensure_stop(trade, D('99.50'))
        self.assertEqual(len(self.writes('new_stop')), before_close)
        self.assertEqual(len(self.writes('new_stop_quantity')), before_bridge + 1)
        state = self.drive()
        self.assertEqual(state['phase'], 'complete')
        self.assertEqual(len(self.writes('new_stop')), before_close + 1)
        self.assertEqual(state['target'], '99.50')

    def test_capability_restart_uses_same_persisted_account_symbol(self):
        self.learn()
        self.engine = self.make_engine()
        self.engine.reconcile()
        self.assertTrue(self.engine._close_all_overlap_rejected('BTCUSDT'))
        before = len(self.writes('new_stop'))
        self.engine._ensure_stop(self.store.trade('trade-1'), D('99.50'))
        self.assertEqual(len(self.writes('new_stop')), before)

    def test_capability_symbol_and_account_are_isolated(self):
        self.learn()
        self.assertFalse(self.engine._close_all_overlap_rejected('ETHUSDT'))
        self.client._key = 'offline-account-b'
        self.assertFalse(self.engine._close_all_overlap_rejected('BTCUSDT'))

    def test_capability_initial_without_existing_protection_still_normal_post(self):
        trade = self.learn()
        for raw in self.client.algo_rows.values():
            raw['algoStatus'] = 'CANCELED'
        self.engine.reconcile()
        trade = self.store.trade('trade-1')
        trade['tightest_stop'] = None
        trade['confirmed_stop'] = None
        self.store.save_trade(trade)
        self.client.new_stop = lambda *args: FakeExchange.new_stop(self.client, *args)
        before = len(self.writes('new_stop'))
        bridges = len(self.writes('new_stop_quantity'))
        self.assertTrue(self.engine._ensure_stop(trade, D('98')))
        self.assertEqual(len(self.writes('new_stop')), before + 1)
        self.assertEqual(len(self.writes('new_stop_quantity')), bridges)

    def test_capability_unknown_orders_do_not_bypass_query_maintenance(self):
        trade = self.learn()
        before = len(self.writes('new_stop_quantity'))
        self.engine._unknown = Mock(return_value=[{'state': 'unknown'}])
        self.assertFalse(self.engine._ensure_stop(trade, D('99.50')))
        self.assertEqual(len(self.writes('new_stop_quantity')), before)

    def test_capability_current_price_crossing_records_exit_before_bridge(self):
        trade = self.learn()
        self.client.latest = '99.40'
        before = len(self.writes('new_stop_quantity'))
        self.assertFalse(self.engine._ensure_stop(trade, D('99.50')))
        self.assertIsNotNone(self.store.trade('trade-1')['exit_intent'])
        self.assertEqual(len(self.writes('new_stop_quantity')), before)

    def test_capability_existing_exit_or_protective_child_blocks_direct_bridge(self):
        trade = self.learn()
        before = len(self.writes('new_stop_quantity'))
        trade['exit_intent'] = {'reason': 'offline exit', 'time_ms': self.client.now}
        self.assertFalse(self.engine._ensure_stop(trade, D('99.50')))
        trade['exit_intent'] = None
        self.engine._inflight_protective_exit = Mock(return_value=True)
        self.assertFalse(self.engine._ensure_stop(trade, D('99.50')))
        self.assertEqual(len(self.writes('new_stop_quantity')), before)

    def test_capability_unknown_generic_or_local_failure_never_learns(self):
        errors = [
            OrderUncertain(-4130, 'offline ambiguous'),
            ApiError(-4130, 'offline unconfirmed'),
            ApiError(None, 'offline EOF', recovery_kind='network_read_failure'),
            ApiError(-2022, 'offline generic rejection', definite_rejected=True),
            ApiError(-4130, 'offline preflight', definite_rejected=True, definite_not_submitted=True),
        ]
        for error in errors:
            self.engine._remember_close_all_overlap_rejection('BTCUSDT', error)
            self.assertFalse(self.engine._close_all_overlap_rejected('BTCUSDT'))
        self.assertIsNone(self.store.meta(module.STOP_CAPABILITY_META))

    def test_capability_corrupt_marker_is_not_permission(self):
        account = self.engine._stop_capability_account()
        self.store.set_meta(module.STOP_CAPABILITY_META, {'version': 1, 'accounts': {
            account: {'BTCUSDT': {'source': 'old_log', 'code': -4130, 'confirmed_at_ms': 1}}}})
        self.assertFalse(self.engine._close_all_overlap_rejected('BTCUSDT'))

if __name__ == '__main__':
    unittest.main()
