"""Candidate reservation and order commitment remain one durable identity, offline only."""
import concurrent.futures
import copy
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from execution.store import Store


class CandidateStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'ledger.sqlite'
        self.store = Store(self.path)
        self.store.set_meta_many({'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True},
            'symbol_enabled_after': {'BTCUSDT': 0, 'ETHUSDT': 0},
            'hourly_trend_range_ledgers_v1': {'ETHUSDT': {'epoch': 'original'}}})
        self.boundary = 3600000

    def value(self, symbol='ETHUSDT'):
        return {'symbol': symbol, 'boundary_ms': self.boundary,
            'signal': {'boundary_ms': self.boundary, 'direction': 1, 'branch': 'trend', 'atr': '20'},
            'control': self.store.candidate_control(symbol), 'created_ms': self.boundary + 1,
            'deadline_ms': self.boundary + 300000}

    def reserve(self):
        return self.store.register_entry_candidate(self.value())

    def freeze(self, value=None):
        value = self.reserve() if value is None else value
        return self.store.update_entry_candidate('ETHUSDT', self.boundary, {'frozen': {
            'equity0': '100', 'budget0': '50', 'notional0': '1500', 'entry_atr': '20',
            'ledger_epoch': 'original'}}, value['revision'])

    def pair(self, candidate):
        trade = dict(candidate['frozen'], id='trade-once', symbol='ETHUSDT', status='pending',
            direction=candidate['signal']['direction'], branch=candidate['signal']['branch'],
            signal_boundary_ms=self.boundary, signal_snapshot=copy.deepcopy(candidate['signal']))
        order = {'id': 'order-once', 'client_id': 'order-once', 'trade_id': trade['id'],
            'symbol': 'ETHUSDT', 'state': 'prepared', 'kind': 'market', 'purpose': 'entry'}
        return trade, order

    def commit(self, candidate):
        return self.store.commit_entry_candidate('ETHUSDT', self.boundary, *self.pair(candidate),
                                                 candidate['revision'])

    def test_old_consumed_signal_cannot_become_candidate_after_get_failure(self):
        self.store.claim_signal('ETHUSDT', self.boundary, {'direction': 1})
        self.assertIsNone(self.reserve())
        self.assertEqual(self.store.entry_candidates(), [])
        self.assertEqual(self.store.trades(), [])

    def test_reservation_atomically_retains_claim_without_replacing_payload(self):
        first = self.reserve()
        self.assertTrue(self.store.signal('ETHUSDT', self.boundary)['candidate_reserved'])
        other = self.value()
        other['signal']['atr'] = '99'
        self.assertEqual(self.store.register_entry_candidate(other), first)
        self.assertFalse(self.store.claim_signal('ETHUSDT', self.boundary, {}))

    def test_registration_cannot_bypass_disabled_or_boundary_activation(self):
        self.assertIsNone(self.store.register_entry_candidate(self.value('BTCUSDT')))
        self.store.set_meta('symbol_enabled_after', {'ETHUSDT': self.boundary})
        self.assertIsNone(self.reserve())
        self.assertIsNone(self.store.signal('ETHUSDT', self.boundary))

    def test_stale_control_at_registration_writes_nothing(self):
        value = self.value()
        self.store.set_meta('symbol_enabled', {'ETHUSDT': False, 'BTCUSDT': False})
        self.assertIsNone(self.store.register_entry_candidate(value))
        self.assertIsNone(self.store.signal('ETHUSDT', self.boundary))

    def test_candidate_and_claim_rollback_together_on_insert_failure(self):
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_candidate BEFORE INSERT ON hb_entry_candidates BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.reserve()
        self.assertIsNone(self.store.signal('ETHUSDT', self.boundary))

    def test_concurrent_registration_has_one_signal_and_one_identity(self):
        value = self.value()
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            results = list(pool.map(lambda _: Store(self.path).register_entry_candidate(value), range(8)))
        self.assertTrue(all(v == results[0] for v in results))
        self.assertEqual(len(self.store.entry_candidates()), 1)

    def test_waits_can_be_cas_updated_but_stale_result_cannot_overwrite(self):
        first = self.reserve()
        updated = self.store.update_entry_candidate('ETHUSDT', self.boundary,
            {'wait_reason': 'local_budget_wait', 'retry_at_ms': self.boundary + 2000}, first['revision'])
        self.assertIsNotNone(updated)
        self.assertIsNone(self.store.update_entry_candidate('ETHUSDT', self.boundary,
            {'wait_reason': 'obsolete'}, first['revision']))
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary), updated)

    def test_freeze_once_persists_across_new_store(self):
        frozen = self.freeze()
        self.assertEqual(Store(self.path).entry_candidates(True), [frozen])
        for key in ('equity0', 'budget0', 'notional0', 'entry_atr', 'ledger_epoch'):
            changed = copy.deepcopy(frozen['frozen'])
            changed[key] = 'other' if key == 'ledger_epoch' else '200'
            with self.assertRaises(ValueError):
                self.store.update_entry_candidate('ETHUSDT', self.boundary, {'frozen': changed}, frozen['revision'])

    def test_freeze_rejects_expanded_risk_wrong_atr_and_nonfinite(self):
        first = self.reserve()
        base = {'equity0': '100', 'budget0': '50', 'notional0': '1500', 'entry_atr': '20', 'ledger_epoch': 'original'}
        for key, val in [('budget0', '51'), ('notional0', '1501'), ('entry_atr', '21'), ('equity0', 'NaN')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.store.update_entry_candidate('ETHUSDT', self.boundary,
                    {'frozen': dict(base, **{key: val})}, first['revision'])

    def test_update_cannot_change_signal_control_or_write_intent(self):
        first = self.reserve()
        for update in ({'signal': {}}, {'control': {}}, {'boundary_ms': 1}, {'status': 'write_intent'}):
            with self.assertRaises(ValueError):
                self.store.update_entry_candidate('ETHUSDT', self.boundary, update, first['revision'])

    def test_global_pause_then_restart_same_millisecond_does_not_revive(self):
        frozen = self.freeze()
        for source, running in [('human_pause', False), ('human_start', True)]:
            self.store.set_meta('trading_intent_v1', {'version': 1, 'desired_running': running,
                'updated_at_ms': self.boundary + 2, 'source': source})
        current = self.store.entry_candidate('ETHUSDT', self.boundary)
        self.assertEqual(current['status'], 'superseded')
        self.assertEqual(self.store.candidate_control('ETHUSDT')['global_epoch'], 2)
        self.assertIsNone(self.commit(frozen))
        self.assertEqual(self.store.trades(), [])

    def test_global_duplicate_human_start_still_invalidates(self):
        intent = {'version': 1, 'desired_running': True, 'updated_at_ms': 1, 'source': 'human_start'}
        self.store.set_meta('trading_intent_v1', intent)
        first = self.reserve()
        self.store.set_meta('trading_intent_v1', intent)
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary)['status'], 'superseded')
        self.assertEqual(self.store.candidate_control('ETHUSDT')['global_epoch'], 2)

    def test_other_symbol_change_preserves_eth_candidate(self):
        frozen = self.freeze()
        eth_control = self.store.candidate_control('ETHUSDT')
        self.store.set_meta('symbol_enabled', {'ETHUSDT': True, 'BTCUSDT': True})
        self.assertEqual(self.store.candidate_control('ETHUSDT'), eth_control)
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary), frozen)
        self.assertIsNotNone(self.commit(frozen))

    def test_same_symbol_toggle_invalidates_even_if_final_bool_same(self):
        frozen = self.freeze()
        for enabled in (False, True):
            self.store.set_meta('symbol_enabled', {'ETHUSDT': enabled, 'BTCUSDT': False})
        self.assertIsNone(self.commit(frozen))
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary)['status'], 'superseded')

    def test_activation_change_invalidates_only_uncommitted(self):
        committed = self.commit(self.freeze())
        self.store.set_meta('symbol_enabled_after', {'ETHUSDT': self.boundary + 3600000})
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary), committed)
        self.assertEqual(len(self.store.orders()), 1)

    def test_meta_and_candidate_retirement_rollback_together(self):
        frozen = self.freeze()
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_update BEFORE UPDATE ON hb_entry_candidates BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.set_meta_many({'symbol_enabled': {'ETHUSDT': False, 'BTCUSDT': False}, 'other': True})
        self.assertEqual(self.store.candidate_control('ETHUSDT'), frozen['control'])
        self.assertIsNone(self.store.meta('other'))

    def test_commit_writes_order_trade_and_candidate_atomically(self):
        frozen = self.freeze()
        committed = self.commit(frozen)
        self.assertEqual(committed['status'], 'write_intent')
        self.assertEqual(len(self.store.trades()), 1)
        self.assertEqual(len(self.store.orders()), 1)
        self.assertEqual(self.store.entry_candidates(True), [])
        self.assertIsNone(self.commit(frozen))
        self.assertIsNone(self.store.update_entry_candidate('ETHUSDT', self.boundary,
            {'status': 'precheck'}, committed['revision']))

    def test_order_insert_crash_rolls_back_trade_and_candidate(self):
        frozen = self.freeze()
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_order BEFORE INSERT ON hb_orders BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.commit(frozen)
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.store.orders(), [])
        self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary), frozen)

    def test_candidate_update_crash_rolls_back_both_records(self):
        frozen = self.freeze()
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_commit BEFORE UPDATE ON hb_entry_candidates BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.commit(frozen)
        self.assertEqual(self.store.trades(), [])
        self.assertEqual(self.store.orders(), [])

    def test_concurrent_commit_cannot_create_second_order(self):
        frozen = self.freeze()
        trade, order = self.pair(frozen)
        def commit(i):
            return Store(self.path).commit_entry_candidate('ETHUSDT', self.boundary, dict(trade, id=f't{i}'),
                dict(order, id=f'o{i}', client_id=f'o{i}', trade_id=f't{i}'), frozen['revision'])
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            rows = list(pool.map(commit, range(4)))
        self.assertEqual(sum(row is not None for row in rows), 1)
        self.assertEqual(len(self.store.trades()), 1)
        self.assertEqual(len(self.store.orders()), 1)

    def test_same_symbol_active_trade_blocks_atomic_commit(self):
        for status in ('open', 'pending', 'closing'):
            with self.subTest(status=status):
                frozen = self.freeze()
                self.store.save_trade({'id': 'older', 'symbol': 'ETHUSDT', 'status': status})
                self.assertIsNone(self.commit(frozen))
                self.assertEqual(self.store.orders(), [])
                self.assertEqual(self.store.entry_candidate('ETHUSDT', self.boundary), frozen)

    def test_other_symbol_trade_does_not_block_commit(self):
        frozen = self.freeze()
        self.store.save_trade({'id': 'other', 'symbol': 'BTCUSDT', 'status': 'open'})
        self.assertIsNotNone(self.commit(frozen))
        self.assertEqual(len(self.store.trades()), 2)

    def test_same_symbol_closed_trade_does_not_block_commit(self):
        frozen = self.freeze()
        self.store.save_trade({'id': 'closed', 'symbol': 'ETHUSDT', 'status': 'closed'})
        self.assertIsNotNone(self.commit(frozen))
        self.assertEqual(self.store.trade('closed')['status'], 'closed')

    def test_commit_requires_original_frozen_budget_signal_and_client_id(self):
        frozen = self.freeze()
        trade, order = self.pair(frozen)
        for changed in [dict(trade, equity0='101'), dict(trade, signal_snapshot={}), dict(trade, entry_atr='21'),
                        dict(trade, direction=-1), dict(trade, branch='range')]:
            with self.assertRaises(ValueError):
                self.store.commit_entry_candidate('ETHUSDT', self.boundary, changed, order, frozen['revision'])
        with self.assertRaises(ValueError):
            self.store.commit_entry_candidate('ETHUSDT', self.boundary, trade,
                dict(order, client_id='new-identity'), frozen['revision'])
        self.assertEqual(self.store.orders(), [])

    def test_decimal_payload_normalizes_to_durable_signal_snapshot(self):
        frozen = self.freeze()
        trade, order = self.pair(frozen)
        trade['signal_snapshot']['atr'] = Decimal('20')
        trade['equity0'] = Decimal('100')
        self.assertIsNotNone(self.store.commit_entry_candidate('ETHUSDT', self.boundary,
            trade, order, frozen['revision']))

    def test_ledger_epoch_change_prevents_commit_without_reset(self):
        frozen = self.freeze()
        self.store.set_meta('hourly_trend_range_ledgers_v1', {'ETHUSDT': {'epoch': 'new'}})
        with self.assertRaises(ValueError):
            self.commit(frozen)
        self.assertEqual(self.store.trades(), [])

    def test_real_intent_requires_real_ledger_epoch(self):
        self.store.set_meta('trading_intent_v1', {'version': 1, 'desired_running': True,
            'updated_at_ms': 1, 'source': 'human_start'})
        frozen = self.freeze()
        self.store.set_meta('hourly_trend_range_ledgers_v1', {})
        with self.assertRaises(ValueError):
            self.commit(frozen)

    def test_legacy_control_none_remains_supported_without_real_intent(self):
        self.store.set_meta('hourly_trend_range_ledgers_v1', {})
        frozen = self.freeze()
        self.assertIsNone(frozen['control']['intent'])
        self.assertIsNotNone(self.commit(frozen))

    def test_invalid_intent_or_paused_intent_never_reserves(self):
        for intent in ({}, {'version': 1, 'desired_running': False, 'updated_at_ms': 1, 'source': 'human_pause'}):
            self.store.set_meta('trading_intent_v1', intent)
            self.assertIsNone(self.reserve())

    def test_candidate_expiry_is_terminal_and_retains_claim(self):
        first = self.reserve()
        expired = self.store.update_entry_candidate('ETHUSDT', self.boundary, {'status': 'expired'}, first['revision'])
        self.assertEqual(self.reserve(), expired)
        self.assertEqual(self.store.entry_candidates(True), [])
        self.assertIsNone(self.store.update_entry_candidate('ETHUSDT', self.boundary,
            {'status': 'precheck'}, expired['revision']))

    def test_no_window_extension_or_future_creation_allowed(self):
        for update in ({'deadline_ms': self.boundary + 300001},
                       {'created_ms': self.boundary + 300000}, {'created_ms': self.boundary - 1}):
            with self.assertRaises(ValueError):
                self.store.register_entry_candidate(dict(self.value(), **update))


if __name__ == '__main__':
    unittest.main()
