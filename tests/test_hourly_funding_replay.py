"""Late funding replay and historical ownership; local fakes/temp books only."""
import copy
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'tests'))

from execution import funding
from execution.binance import ApiError
from execution.engine import Engine, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from test_hourly_engine import FakeExchange, fill_row, trade_row

DAY = 86400000
HOUR = 3600000
NOW = 40 * DAY + HOUR


def settlement(identifier, timestamp, amount='-.02', symbol='BTCUSDT', **changes):
    row = dict(tranId=identifier, symbol=symbol, asset='USDT',
               incomeType='FUNDING_FEE', income=amount, time=timestamp)
    row.update(changes)
    return row


class FundingExchange(FakeExchange):
    """Visibility time is independent of the exchange's settlement timestamp."""
    def __init__(self, now=NOW):
        super().__init__(now)
        self.history = []
        self.handler = None

    def publish(self, row, visible_at=None):
        self.history.append((self.now if visible_at is None else visible_at, copy.deepcopy(row)))

    def history_page(self, **kwargs):
        rows = [copy.deepcopy(row) for visible, row in self.history
                if visible <= self.now and kwargs['startTime'] <= int(row['time']) <= kwargs['endTime']]
        rows.sort(key=lambda row: (int(row['time']), int(row['tranId']), row['symbol'], row['asset']))
        offset = (kwargs['page'] - 1) * kwargs['limit']
        return rows[offset:offset + kwargs['limit']]

    def income(self, **kwargs):
        self._call('income', **kwargs)
        return self.handler(**kwargs) if self.handler is not None else self.history_page(**kwargs)

    def queries(self):
        return [kwargs for name, _, kwargs in self.calls if name == 'income']


class FundingReplayChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'funding.sqlite'
        self.store = Store(self.path)
        self.client = FundingExchange()
        self.started = self.client.now
        self.store.set_meta('funding_cursor', self.client.now)
        self.store.set_meta(funding.RECHECK_META, self.client.now)

    def read(self, store=None):
        return funding.read_funding(self.client, store or self.store, self.client.now, self.started)

    def funding_events(self, store=None):
        return [event for event in (store or self.store).events(10000)
                if event['message'] == '资金费实际结算记录（已计入交易所权益）']

    def test_successful_empty_read_does_not_hide_a_later_visible_settlement(self):
        first_cursor = self.client.now
        self.read()
        row = settlement(1, first_cursor - 30000)
        self.client.publish(row, visible_at=first_cursor + 60000)
        self.client.now += 60000
        _, result = self.read()
        self.assertLess(row['time'], first_cursor)
        self.assertEqual(self.store.incomes(), [row])
        self.assertEqual(result['records_added'], 1)
        self.assertLessEqual(self.client.queries()[-1]['startTime'], row['time'])
        self.client.now += 60000
        _, repeated = self.read()
        self.assertEqual(repeated['records_added'], 0)
        self.assertEqual(len(self.funding_events()), 1)
        self.assertEqual(self.store.meta('funding_cursor'), self.client.now)

    def test_long_delay_is_discovered_only_when_the_six_hour_review_is_due(self):
        row = settlement(2, self.client.now - 3 * DAY)
        self.client.publish(row)
        self.client.now += 6 * HOUR - 1
        _, before = self.read()
        self.assertIsNone(before['audit'])
        self.assertEqual(self.store.incomes(), [])
        self.assertEqual(len(self.client.queries()), 1)
        self.client.now += 1
        self.client.calls.clear()
        _, due = self.read()
        self.assertEqual(self.store.incomes(), [row])
        self.assertEqual(due['records_added'], 1)
        self.assertEqual(len(self.client.queries()), 2)
        self.assertEqual(self.store.meta(funding.RECHECK_META), self.client.now)
        self.read()
        self.assertEqual(len(self.funding_events()), 1)

    def test_upgrade_without_a_review_marker_replays_history_and_preserves_old_state(self):
        old = settlement(3, self.client.now - 4 * DAY)
        late = settlement(4, self.client.now - 3 * DAY)
        self.store.claim_income(old)
        original_trade = trade_row('existing-closed', status='closed')
        self.store.save_trade(original_trade)
        self.store.set_meta('risk_latch_fixture', {'latched': True, 'losses': '12.5'})
        self.store.set_meta(funding.RECHECK_META, None)
        self.client.publish(old)
        self.client.publish(late)
        _, result = self.read()
        self.assertIsNotNone(result['audit'])
        self.assertEqual(result['records_added'], 1)
        self.assertEqual(self.store.incomes(), [old, late])
        self.assertEqual(self.store.trade(original_trade['id']), original_trade)
        self.assertEqual(self.store.meta('risk_latch_fixture'), {'latched': True, 'losses': '12.5'})
        self.assertIsNone(self.store.meta(funding.PENDING_META))

    def test_both_ordinary_endpoints_are_inclusive_and_do_not_duplicate(self):
        cursor = self.client.now
        rows = [settlement(5, cursor - DAY), settlement(6, cursor)]
        for row in rows:
            self.client.publish(row)
        self.read()
        self.read()
        self.assertEqual(self.store.incomes(), rows)
        self.assertEqual(len(self.funding_events()), 2)
        self.assertTrue(all(query['startTime'] <= cursor - DAY <= query['endTime']
                            for query in self.client.queries()))

    def test_review_seven_day_boundary_is_inclusive_but_older_records_remain_outside(self):
        self.store.set_meta(funding.RECHECK_META, None)
        outside = settlement(7, self.client.now - 7 * DAY)
        boundary = settlement(8, self.client.now - 7 * DAY + 1)
        self.client.publish(outside)
        self.client.publish(boundary)
        self.read()
        self.assertEqual(self.store.incomes(), [boundary])
        for query in self.client.queries():
            self.assertLess(query['endTime'] - query['startTime'], 7 * DAY)

    def test_first_read_near_epoch_clamps_start_and_merges_identical_windows(self):
        self.client.now = 5000
        self.store.set_meta('funding_cursor', 0)
        self.store.set_meta(funding.RECHECK_META, None)
        self.client.publish(settlement(9, 0))
        self.read()
        self.assertEqual(len(self.client.queries()), 1)
        self.assertEqual(self.client.queries()[0]['startTime'], 0)
        self.assertEqual(self.client.queries()[0]['endTime'], 5000)
        self.assertEqual(self.store.meta('funding_cursor'), 5000)

    def test_long_outage_advances_in_bounded_windows_without_skipping_the_next_range(self):
        cursor = self.client.now - 30 * DAY
        first = settlement(10, cursor + 5 * DAY)
        later = settlement(11, cursor + 7 * DAY)
        self.store.set_meta('funding_cursor', cursor)
        self.client.publish(first)
        self.client.publish(later)
        self.read()
        middle = self.store.meta('funding_cursor')
        self.assertGreater(middle, cursor)
        self.assertLess(middle, later['time'])
        self.assertEqual(self.store.incomes(), [first])
        self.read()
        self.assertEqual(self.store.incomes(), [first, later])
        self.assertGreater(self.store.meta('funding_cursor'), middle)
        self.assertTrue(all(query['endTime'] - query['startTime'] < 7 * DAY
                            for query in self.client.queries()))

    def test_future_cursor_and_clock_correction_never_commit_future_success_markers(self):
        before = self.client.now
        self.store.set_meta('funding_cursor', before + DAY)
        self.store.set_meta(funding.RECHECK_META, None)
        def correct_clock(**kwargs):
            self.client.now = before - 2000
            return []
        self.client.handler = correct_clock
        completed, _ = self.read()
        self.assertEqual(completed, before - 2000)
        self.assertEqual(self.store.meta('funding_cursor'), completed)
        self.assertEqual(self.store.meta(funding.RECHECK_META), completed)
        self.assertTrue(all(query['startTime'] <= query['endTime'] <= before
                            for query in self.client.queries()))

    def test_future_pending_range_is_safely_shrunk_after_restart_clock_backstep(self):
        now = self.client.now
        self.store.set_meta('funding_cursor', now + 1000)
        self.store.set_meta(funding.PENDING_META, {'version': 1,
            'ordinary': {'start': now + 500, 'end': now + 1000}, 'audit': None})
        restarted = Store(self.path)
        self.read(restarted)
        query = self.client.queries()[0]
        self.assertEqual((query['startTime'], query['endTime']), (now, now))
        self.assertEqual(restarted.meta('funding_cursor'), now)
        self.assertIsNone(restarted.meta(funding.PENDING_META))

    def test_exactly_one_thousand_records_requires_the_next_empty_page(self):
        self.assertEqual(funding.PAGE_SIZE, 1000)
        for identifier in range(1000):
            self.client.publish(settlement(identifier, self.client.now - 1000))
        self.read()
        self.assertEqual([query['page'] for query in self.client.queries()], [1, 2])
        self.assertTrue(all(query['limit'] == 1000 for query in self.client.queries()))
        self.assertEqual(len(self.store.incomes()), 1000)
        self.assertEqual(len(self.funding_events()), 1000)

    def test_five_distinct_full_pages_are_incomplete_and_do_not_advance_markers(self):
        self.assertEqual(funding.MAX_PAGES, 5)
        cursor = self.store.meta('funding_cursor')
        reviewed = self.store.meta(funding.RECHECK_META)
        def always_full(**kwargs):
            return [settlement((kwargs['page'] - 1) * 1000 + identifier,
                               self.client.now - 1000) for identifier in range(1000)]
        self.client.handler = always_full
        # This test measures the request bound; durable claims are tested separately.
        with patch.object(self.store, 'claim_income', return_value=False):
            with self.assertRaisesRegex(ApiError, '分页上限'):
                self.read()
        self.assertEqual([query['page'] for query in self.client.queries()], [1, 2, 3, 4, 5])
        self.assertEqual(self.store.meta('funding_cursor'), cursor)
        self.assertEqual(self.store.meta(funding.RECHECK_META), reviewed)
        self.assertIsNotNone(self.store.meta(funding.PENDING_META))

    def test_repeated_full_page_stops_early_without_claiming_completeness(self):
        rows = [settlement(identifier, self.client.now - 1000) for identifier in range(1000)]
        self.client.handler = lambda **kwargs: copy.deepcopy(rows)
        with patch.object(self.store, 'claim_income', return_value=False):
            with self.assertRaisesRegex(ApiError, '分页未前进'):
                self.read()
        self.assertEqual([query['page'] for query in self.client.queries()], [1, 2])
        self.assertIsNotNone(self.store.meta(funding.PENDING_META))

    def test_partial_page_failure_and_restart_replay_the_frozen_range_from_page_one(self):
        cursor = self.client.now - 60000
        self.store.set_meta('funding_cursor', cursor)
        rows = [settlement(20, cursor - 1000), settlement(21, cursor)]
        for row in rows:
            self.client.publish(row)
        def fail_page_two(**kwargs):
            if kwargs['page'] == 2:
                raise ApiError(None, 'fixture connection lost')
            return self.client.history_page(**kwargs)
        self.client.handler = fail_page_two
        with patch.object(funding, 'PAGE_SIZE', 2):
            with self.assertRaises(ApiError):
                self.read()
            pending = self.store.meta(funding.PENDING_META)
            self.assertEqual(self.store.meta('funding_cursor'), cursor)
            self.assertEqual(self.store.incomes(), rows)
            self.client.now += 2 * DAY
            self.client.handler = None
            self.client.calls.clear()
            restarted = Store(self.path)
            _, result = self.read(restarted)
        self.assertEqual(result['records_added'], 0)
        self.assertEqual([query['page'] for query in self.client.queries()], [1, 2])
        self.assertTrue(all((query['startTime'], query['endTime']) ==
                            (pending['ordinary']['start'], pending['ordinary']['end'])
                            for query in self.client.queries()))
        self.assertEqual(restarted.meta('funding_cursor'), pending['ordinary']['end'])
        self.assertEqual(len(self.funding_events(restarted)), 2)
        self.assertIsNone(restarted.meta(funding.PENDING_META))

    def test_failed_long_review_keeps_both_success_markers_and_reuses_the_old_review_range(self):
        cursor = self.client.now - 60000
        reviewed = self.client.now - 6 * HOUR
        self.store.set_meta('funding_cursor', cursor)
        self.store.set_meta(funding.RECHECK_META, reviewed)
        rows = [settlement(22, self.client.now - 3 * DAY), settlement(23, self.client.now - 2 * DAY)]
        for row in rows:
            self.client.publish(row)
        def fail_review_page_two(**kwargs):
            if kwargs['startTime'] < cursor - DAY and kwargs['page'] == 2:
                raise ApiError(None, 'fixture review failed')
            return self.client.history_page(**kwargs)
        self.client.handler = fail_review_page_two
        with patch.object(funding, 'PAGE_SIZE', 2):
            with self.assertRaises(ApiError):
                self.read()
            pending = self.store.meta(funding.PENDING_META)
            self.assertEqual(self.store.meta('funding_cursor'), cursor)
            self.assertEqual(self.store.meta(funding.RECHECK_META), reviewed)
            self.client.now += 6 * HOUR
            self.client.handler = None
            self.client.calls.clear()
            restarted = Store(self.path)
            self.read(restarted)
        query_ranges = {(query['startTime'], query['endTime']) for query in self.client.queries()}
        self.assertIn((pending['audit']['start'], pending['audit']['end']), query_ranges)
        self.assertEqual(restarted.meta(funding.RECHECK_META), pending['audit']['end'])
        self.assertEqual(restarted.incomes(), rows)
        self.assertEqual(len(self.funding_events(restarted)), 2)

    def test_success_marker_transaction_failure_is_atomic_and_retry_deduplicates(self):
        cursor = self.client.now - 60000
        self.store.set_meta('funding_cursor', cursor)
        self.store.set_meta(funding.RECHECK_META, None)
        self.client.publish(settlement(24, self.client.now - 1000))
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER reject_completion BEFORE UPDATE ON hb_meta "
                       "WHEN NEW.key='funding_replay_v1' AND NEW.value='null' "
                       "BEGIN SELECT RAISE(ABORT,'fixture commit refused'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.read()
        self.assertEqual(self.store.meta('funding_cursor'), cursor)
        self.assertIsNone(self.store.meta(funding.RECHECK_META))
        self.assertIsNotNone(self.store.meta(funding.PENDING_META))
        self.assertEqual(len(self.store.incomes()), 1)
        with self.store.connection() as db:
            db.execute('DROP TRIGGER reject_completion')
        _, result = self.read(Store(self.path))
        self.assertEqual(result['records_added'], 0)
        self.assertEqual(len(self.funding_events()), 1)
        self.assertIsNone(self.store.meta(funding.PENDING_META))

    def test_invalid_settlement_fields_cannot_be_saved_or_advance_the_cursor(self):
        changes = [dict(time=True), dict(time=self.client.now - 1000 + .5),
                   dict(time=str(self.client.now) + '.5'), dict(time=self.client.now + 1),
                   dict(time=self.client.now - DAY - 1), dict(tranId=True), dict(tranId=None),
                   dict(tranId='not-an-id'), dict(income='NaN'), dict(income='Infinity'),
                   dict(incomeType='COMMISSION'), dict(symbol=''), dict(asset='')]
        for index, change in enumerate(changes):
            with self.subTest(change=change):
                store = Store(Path(self.temp.name) / ('invalid-' + str(index) + '.sqlite'))
                store.set_meta('funding_cursor', self.client.now)
                store.set_meta(funding.RECHECK_META, self.client.now)
                row = settlement(25, self.client.now - 1000, **change)
                self.client.handler = lambda row=row, **kwargs: [copy.deepcopy(row)]
                with self.assertRaises(ApiError):
                    self.read(store)
                self.assertEqual(store.incomes(), [])
                self.assertEqual(store.meta('funding_cursor'), self.client.now)
                self.assertIsNotNone(store.meta(funding.PENDING_META))

    def test_invalid_page_shapes_and_pending_metadata_make_no_success_claim(self):
        for response in ({'rows': []}, None, [None], [settlement(26, self.client.now)] * 1001):
            with self.subTest(response_type=type(response).__name__):
                self.client.handler = lambda response=response, **kwargs: copy.deepcopy(response)
                with self.assertRaises(ApiError):
                    self.read()
                self.assertEqual(self.store.incomes(), [])
                self.assertEqual(self.store.meta('funding_cursor'), self.client.now)
        invalid = [False, {'version': 2}, {'version': 1, 'ordinary': None},
                   {'version': 1, 'ordinary': {'start': -1, 'end': 100}},
                   {'version': 1, 'ordinary': {'start': 100, 'end': 99}},
                   {'version': 1, 'ordinary': {'start': 0, 'end': 7 * DAY}}]
        for pending in invalid:
            with self.subTest(pending=pending):
                self.store.set_meta(funding.PENDING_META, pending)
                self.client.calls.clear()
                with self.assertRaises(ApiError):
                    self.read()
                self.assertEqual(self.client.queries(), [])
                self.assertEqual(self.store.meta(funding.PENDING_META), pending)

    def test_core_conflicts_preserve_the_original_record_and_recovery_cursor(self):
        row = settlement(27, self.client.now - 1000)
        self.store.claim_income(row)
        for change in (dict(income='-.03'), dict(time=row['time'] + 1),
                       dict(incomeType='COMMISSION')):
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, '核心金额或归属变化'):
                    self.store.claim_income(dict(row, **change))
                self.assertEqual(self.store.incomes(), [row])
        self.client.handler = lambda **kwargs: [dict(row, income='-.03')]
        with self.assertRaises(ValueError):
            self.read()
        self.assertEqual(self.store.meta('funding_cursor'), self.client.now)
        self.assertIsNotNone(self.store.meta(funding.PENDING_META))
        self.assertEqual(self.store.incomes(), [row])

    def test_equivalent_decimal_time_and_extra_info_do_not_replace_original_payload(self):
        row = settlement(28, self.client.now - 1000)
        self.assertTrue(self.store.claim_income(row))
        equivalent = dict(row, tranId='28', time=str(row['time']), income='-0.0200', info='extra metadata')
        self.assertFalse(self.store.claim_income(equivalent))
        other_symbol = dict(row, symbol='ETHUSDT', income='.01')
        self.assertTrue(self.store.claim_income(other_symbol))
        self.assertEqual(self.store.incomes(), [row, other_symbol])


class FundingOwnershipIntegrationChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'owned-funding.sqlite'
        self.store = Store(self.path)
        self.client = FundingExchange()
        self.initialized = self.client.now - 4 * DAY
        self.store.set_meta(LEDGER_META, {symbol: dict(strategy=STRATEGY, version=STRATEGY_VERSION,
            epoch='funding-fixture-' + symbol, initialized_at_ms=self.initialized,
            activation_boundary_ms=self.initialized, initial_wallet='100', wallet='100')
            for symbol in ('BTCUSDT', 'ETHUSDT')})
        self.store.set_meta('funding_cursor', self.client.now)
        self.store.set_meta(funding.RECHECK_META, self.client.now)
        self.engine = Engine(self.client, self.store, monotonic=lambda: 100.0)
        self.engine.state = 'running'
        self.client.calls.clear()

    def owned_trade(self, identifier='owned-closed', status='closed', entry_time=None, exit_time=None):
        trade = trade_row(identifier, status=status, ledger_epoch='funding-fixture-BTCUSDT')
        self.store.save_trade(trade)
        entry = fill_row(1, 101, '1', '100', self.initialized + 1000 if entry_time is None else entry_time,
                         trade=identifier, purpose='entry', fee='.05')
        entry['symbol'] = 'BTCUSDT'
        self.store.claim_fill('BTCUSDT', entry)
        if status == 'closed':
            exit_fill = fill_row(2, 102, '1', '102', self.initialized + 3 * DAY if exit_time is None else exit_time,
                                 trade=identifier, purpose='exit', fee='.051', pnl='2')
            exit_fill['symbol'] = 'BTCUSDT'
            self.store.claim_fill('BTCUSDT', exit_fill)
        self.engine._rebuild_strategy_ledgers()
        return trade

    def assert_wallet(self, value, engine=None, count=None):
        ledger = (engine or self.engine).strategy_ledgers['BTCUSDT']
        self.assertEqual(D(ledger['wallet']), D(value))
        if count is not None:
            self.assertEqual(ledger['funding_count'], count)
        self.assertEqual(self.client.account_value['totalWalletBalance'], '1000')
        self.assertEqual(self.client.writes(), [])

    def test_late_settlement_is_owned_at_settlement_time_even_after_position_closed_and_rebooted(self):
        self.owned_trade()
        original = copy.deepcopy(self.store.meta(LEDGER_META))
        self.engine._funding(force=True)
        self.assert_wallet('101.899', count=0)
        row = settlement(100, self.initialized + DAY)
        self.client.publish(row, visible_at=self.client.now + 6 * HOUR)
        self.client.now += 6 * HOUR
        self.engine._funding(force=True)
        self.assert_wallet('101.879', count=1)
        self.assertTrue(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['wallet']), D('100'))
        self.client.now += 6 * HOUR
        restarted = Engine(self.client, Store(self.path))
        restarted._funding(force=True)
        self.assert_wallet('101.879', engine=restarted, count=1)
        self.assertEqual(restarted.state, 'stopped')
        self.assertEqual(D(restarted.strategy_ledgers['BTCUSDT']['funding']), D('-.02'))
        for symbol in original:
            restored = self.store.meta(LEDGER_META)[symbol]
            self.assertEqual(restored['epoch'], original[symbol]['epoch'])
            self.assertEqual(restored['initialized_at_ms'], original[symbol]['initialized_at_ms'])
        events = [event for event in self.store.events(100)
                  if event['message'] == '资金费实际结算记录（已计入交易所权益）']
        self.assertEqual(len(events), 1)

    def test_partially_claimed_review_rebuilds_once_after_engine_restart_and_preserves_pause(self):
        self.owned_trade()
        self.engine.state = 'paused'
        self.store.set_meta(funding.RECHECK_META, None)
        for row in (settlement(101, self.initialized + DAY),
                    settlement(102, self.initialized + 2 * DAY, amount='.01')):
            self.client.publish(row)
        def fail_second_page(**kwargs):
            if kwargs['page'] == 2:
                raise ApiError(None, 'fixture TLS review failure')
            return self.client.history_page(**kwargs)
        self.client.handler = fail_second_page
        with patch.object(funding, 'PAGE_SIZE', 2):
            with self.assertRaises(ApiError):
                self.engine._funding(force=True)
            self.assertTrue(self.store.meta('funding_read_pending'))
            self.assertEqual(len(self.store.incomes()), 2)
            self.assert_wallet('101.899', count=0)
            self.client.handler = None
            restarted = Engine(self.client, Store(self.path))
            restarted.state = 'paused'
            self.assertTrue(restarted.funding_pending_recovery)
            restarted._funding(force=True)
        self.assert_wallet('101.889', engine=restarted, count=2)
        self.assertFalse(restarted.funding_pending_recovery)
        self.assertFalse(self.store.meta('funding_read_pending'))
        self.assertEqual(restarted.state, 'paused')
        restarted._funding(force=True)
        self.assert_wallet('101.889', engine=restarted, count=2)

    def test_pre_activation_funding_is_preserved_but_does_not_change_new_allocation(self):
        self.store.set_meta(funding.RECHECK_META, None)
        row = settlement(103, self.initialized - 1, amount='1000')
        self.client.publish(row)
        self.engine._funding(force=True)
        self.assertEqual(self.store.incomes(), [row])
        self.assert_wallet('100', count=0)
        self.assertTrue(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(self.engine.ledger_problems, [])

    def test_unowned_funding_faults_instead_of_guessing_a_wallet_debit(self):
        self.client.publish(settlement(104, self.client.now - 1000))
        self.engine._funding(force=True)
        self.assert_wallet('100', count=0)
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertTrue(self.engine.ledger_problems)
        self.assertEqual(self.engine.state, 'fault')

    def test_fill_at_exact_settlement_millisecond_remains_ambiguous(self):
        timestamp = self.client.now - 1000
        self.owned_trade(status='open', entry_time=timestamp)
        self.client.publish(settlement(105, timestamp))
        self.engine._funding(force=True)
        self.assert_wallet('99.95', count=0)
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(self.engine.state, 'fault')

    def test_non_usdt_settlement_is_not_converted_or_added_to_the_usdt_wallet(self):
        self.owned_trade(status='open')
        self.client.publish(settlement(106, self.client.now - 1000, asset='BNB', amount='1'))
        self.engine._funding(force=True)
        self.assert_wallet('99.95', count=0)
        self.assertFalse(self.engine.strategy_ledgers['BTCUSDT']['verified'])
        self.assertEqual(self.engine.state, 'fault')


if __name__ == '__main__':
    unittest.main()
