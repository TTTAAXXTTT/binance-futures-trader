"""Candidate crossings use durable reduce-only exits with local strict fixtures."""
import sys
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

import test_hourly_stop_replacement_live as fixtures
from execution.binance import ApiError
from execution.engine import ExecutionFault
from test_hourly_engine import fill_row


class StopCandidateExitTests(unittest.TestCase):
    def fixture(self):
        case = fixtures.StopReplacementLiveTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def assert_exit(self, case, direction, quantity='1'):
        trade = case.store.trade('trade-1')
        self.assertIsNotNone(trade.get('exit_intent'))
        self.assertEqual(trade['stop_replacement_v1']['phase'], 'exit')
        case.engine._risk_and_exit()
        orders = case.writes('market_order')
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0][1][1], 'SELL' if direction == 1 else 'BUY')
        self.assertEqual(D(orders[0][1][2]), D(quantity))
        self.assertTrue(orders[0][2]['reduce_only'])
        case.engine._risk_and_exit()
        self.assertEqual(len(case.writes('market_order')), 1)

    def test_blocked_bridge_crossing_and_equality_exit_both_directions(self):
        for direction in (1, -1):
            for equal in (False, True):
                with self.subTest(direction=direction, equal=equal):
                    case = self.fixture()
                    case.client.allow_bridge = False
                    case.start_replacement(direction)
                    target = D('99' if direction == 1 else '101')
                    case.client.latest = str(target if equal else target - direction * D('.5'))
                    case.engine._manage_protection()
                    self.assert_exit(case, direction)
                    self.assertEqual(len(case.writes('new_stop_quantity')), 1)
                    self.assertEqual(case.client.algo_rows[400]['algoStatus'], 'NEW')

    def test_uncrossed_structural_block_uses_price_only_without_more_gets_or_posts(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                case = self.fixture()
                case.client.allow_bridge = False
                case.start_replacement(direction)
                case.client.latest = '99.25' if direction == 1 else '100.75'
                before = len(case.client.calls)
                for _ in range(3):
                    case.client.now += 2000
                    case.engine._manage_protection()
                self.assertEqual([call[0] for call in case.client.calls[before:]],
                                 ['contract_price'] * 3)
                self.assertIsNone(case.store.trade('trade-1').get('exit_intent'))
                self.assertEqual(len(case.writes('new_stop_quantity')), 1)

    def test_not_yet_due_retry_still_exits_crossed_candidate(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                case = self.fixture()
                def reject(*args):
                    case.client._call('new_stop_quantity', *args)
                    raise ApiError(-4164, 'definite bridge rejection with retry delay')
                with patch.object(case.client, 'new_stop_quantity', side_effect=reject):
                    case.start_replacement(direction)
                state = case.store.trade('trade-1')['stop_replacement_v1']
                self.assertGreater(state['next_retry_at_ms'], case.client.now)
                case.client.latest = '98.5' if direction == 1 else '101.5'
                case.engine._manage_protection()
                self.assert_exit(case, direction)
                self.assertEqual(len(case.writes('new_stop_quantity')), 1)

    def test_blocked_final_checks_crossing_without_replaying_rejected_final(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                case = self.fixture()
                case.start_replacement(direction)
                case.drive(3)
                def reject(*args):
                    case.client._call('new_stop', *args)
                    raise ApiError(-4130, 'definite final rejection')
                with patch.object(case.client, 'new_stop', side_effect=reject):
                    case.drive(1)
                self.assertEqual(case.store.trade('trade-1')['stop_replacement_v1']['phase'],
                                 'blocked_final')
                count = len(case.writes('new_stop'))
                case.client.latest = '98.5' if direction == 1 else '101.5'
                case.engine._manage_protection()
                self.assert_exit(case, direction)
                self.assertEqual(len(case.writes('new_stop')), count)
                self.assertEqual(case.bridge()['raw']['algoStatus'], 'NEW')

    def test_ensure_stop_already_crossed_queued_target_exits_including_equality(self):
        for direction in (1, -1):
            for equal in (False, True):
                with self.subTest(direction=direction, equal=equal):
                    case = self.fixture()
                    case.client.defer_cancel.add('old')
                    case.start_replacement(direction)
                    case.drive(3)
                    target = D('99.5' if direction == 1 else '100.5')
                    case.client.latest = str(target if equal else target - direction * D('.25'))
                    case.engine._ensure_stop(case.store.trade('trade-1'), target)
                    self.assert_exit(case, direction)
                    self.assertEqual(len(case.writes('new_stop_quantity')), 1)

    def test_next_tick_crosses_previously_uncrossed_queued_target(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                case = self.fixture()
                case.client.defer_cancel.add('old')
                case.start_replacement(direction)
                case.drive(3)
                case.engine._ensure_stop(case.store.trade('trade-1'),
                                         D('99.5' if direction == 1 else '100.5'))
                self.assertIsNone(case.store.trade('trade-1').get('exit_intent'))
                case.client.latest = '99.25' if direction == 1 else '100.75'
                case.client.now += 2000
                case.engine.tick()
                self.assert_exit(case, direction)

    def test_uncrossed_queued_target_and_weaker_candidate_do_not_exit(self):
        for direction in (1, -1):
            with self.subTest(direction=direction):
                case = self.fixture()
                case.client.defer_cancel.add('old')
                case.start_replacement(direction)
                case.drive(3)
                case.client.latest = '99.75' if direction == 1 else '100.25'
                for target in ('99.5', '98.5') if direction == 1 else ('100.5', '101.5'):
                    case.engine._ensure_stop(case.store.trade('trade-1'), D(target))
                trade = case.store.trade('trade-1')
                self.assertIsNone(trade.get('exit_intent'))
                self.assertEqual(trade['stop_replacement_v1']['queued_target'],
                                 '99.5' if direction == 1 else '100.5')
                self.assertFalse(case.writes('market_order'))

    def test_unknown_created_bridge_trigger_has_priority_over_candidate_market_exit(self):
        case = self.fixture()
        case.client.uncertain_create.add('bridge')
        case.start_replacement()
        bridge = case.bridge()
        self.assertEqual(bridge['state'], 'unknown')
        raw = next(row for row in case.client.algo_rows.values()
                   if row['clientAlgoId'] == bridge['client_id'])
        raw.update(algoStatus='TRIGGERED', actualOrderId='0')
        case.client.latest = '98.5'
        case.engine._manage_protection()
        case.engine._risk_and_exit()
        trade = case.store.trade('trade-1')
        self.assertIsNotNone(trade.get('exit_intent'))
        self.assertIn('保护', trade['exit_intent']['reason'])
        self.assertFalse(case.writes('market_order'))
        self.assertEqual(len(case.writes('new_stop_quantity')), 1)

    def test_unknown_bridge_failed_query_cannot_authorize_duplicate_exit_or_stop(self):
        case = self.fixture()
        case.client.uncertain_create.add('bridge')
        case.start_replacement()
        bridge = case.bridge()
        case.client.read_failure.add(bridge['client_id'])
        case.client.latest = '98.5'
        with self.assertRaises(ExecutionFault):
            case.engine._manage_protection()
        self.assertFalse(case.writes('market_order'))
        self.assertEqual(len(case.writes('new_stop_quantity')), 1)
        self.assertEqual(case.client.algo_rows[400]['algoStatus'], 'NEW')

    def test_blocked_bridge_partial_protective_child_exits_only_confirmed_remainder(self):
        case = self.fixture()
        case.client.allow_bridge = False
        case.start_replacement()
        case.client.position_rows[0]['positionAmt'] = '.6'
        case.client.order_rows[700] = dict(symbol='BTCUSDT', orderId=700,
            clientOrderId='stop-child', status='FILLED', executedQty='.4', avgPrice='99')
        case.client.fill_rows[700] = [fill_row(2, 700, '.4', '99', case.client.now + 1,
                                               purpose='stop_exit', pnl='-.4')]
        case.client.algo_rows[400].update(algoStatus='FINISHED', actualOrderId='700')
        case.client.latest = '98.5'
        case.engine.reconcile()
        case.engine._manage_protection()
        self.assert_exit(case, 1, '.6')
        self.assertEqual(len(case.writes('new_stop_quantity')), 1)

    def test_observed_blocked_crossing_survives_query_failure_and_price_rebound(self):
        case = self.fixture()
        case.client.allow_bridge = False
        case.start_replacement()
        case.client.latest = '98.5'
        case.client.read_failure.add('old-stop')
        with self.assertRaises(ExecutionFault):
            case.engine._manage_protection()
        self.assertFalse(case.writes('market_order'))
        case.client.read_failure.clear()
        case.client.latest = '100'
        case.engine._manage_protection()
        self.assert_exit(case, 1)


if __name__ == '__main__':
    unittest.main()
