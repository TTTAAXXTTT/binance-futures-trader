"""Request-budget aggregates: temporary SQLite, virtual clocks, no transport."""
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from execution.request_budget import RequestBudget, RateLimited


class RequestUsageTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'budget.sqlite'
        self.now = 6000.0
        self.budget = self.client()

    def client(self, **kwargs):
        return RequestBudget(self.path, clock=lambda: self.now, **kwargs)

    def rows(self):
        with closing(sqlite3.connect(self.path)) as db:
            return db.execute('SELECT minute,method,endpoint,calls,weight '
                              'FROM request_usage ORDER BY minute,method,endpoint').fetchall()

    def test_shared_clients_aggregate_methods_and_endpoints_atomically(self):
        other = self.client()
        self.budget.reserve('get', '/fapi/v1/openOrders')
        other.reserve('GET', '/fapi/v1/openOrders', {'symbol': 'ETHUSDT'})
        other.reserve('POST', '/fapi/v1/order', {'quantity': '0.002'})
        self.budget.reserve('DELETE', '/fapi/v1/order', {'orderId': 'fixture'})
        self.assertEqual(self.rows(), [
            (100, 'DELETE', '/fapi/v1/order', 1, 1),
            (100, 'GET', '/fapi/v1/openOrders', 2, 41),
            (100, 'POST', '/fapi/v1/order', 1, 1),
        ])
        snapshot = other.snapshot()
        self.assertEqual(snapshot['used_weight'], 43)
        self.assertEqual(snapshot['reserved_peak_weight_last_hour'], 43)
        self.assertEqual(snapshot['current_endpoint_usage'][0], {
            'method': 'GET', 'endpoint': '/fapi/v1/openOrders',
            'calls': 2, 'reserved_weight': 41})

    def test_local_wait_has_no_aggregate_or_budget_side_effect(self):
        limited = self.client(read_cap=1, critical_cap=2)
        limited.reserve('GET', '/fapi/v1/time')
        before = self.rows()
        with self.assertRaises(RateLimited) as caught:
            limited.reserve('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.recovery_kind, 'local_budget_wait')
        self.assertEqual(self.rows(), before)
        self.assertEqual(limited.snapshot()['used_weight'], 1)
        # A critical reservation is counted separately and still shares the cap.
        limited.reserve('POST', '/fapi/v1/order', critical=True)
        with self.assertRaises(RateLimited):
            limited.reserve('DELETE', '/fapi/v1/order', critical=True)
        self.assertEqual(sum(row[3] for row in self.rows()), 2)
        self.assertEqual(limited.snapshot()['used_weight'], 2)

    def test_parameters_and_query_values_are_not_persisted(self):
        self.budget.reserve('GET', '/fapi/v1/order?signature=private-signature&clientId=private-id',
                            {'apiKey': 'private-key', 'symbol': 'private-symbol',
                             'orderId': 'private-order', 'timestamp': 'private-time'})
        self.assertEqual(self.rows()[0][2], '/fapi/v1/order')
        with closing(sqlite3.connect(self.path)) as db:
            contents = '\n'.join(db.iterdump())
        for sensitive in ('private-signature', 'private-id', 'private-key',
                          'private-symbol', 'private-order', 'private-time'):
            self.assertNotIn(sensitive, contents)

    def test_old_database_keeps_budget_and_gains_empty_usage_table(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('CREATE TABLE budget(id INTEGER PRIMARY KEY, minute INTEGER, '
                       'used INTEGER, cooldown REAL)')
            db.execute('INSERT INTO budget VALUES(1,100,1199,6050)')
        snapshot = self.budget.snapshot()
        self.assertEqual(snapshot['used_weight'], 1199)
        self.assertEqual(snapshot['cooldown_until_ms'], 6050000)
        self.assertEqual(snapshot['current_endpoint_usage'], [])
        self.assertEqual(snapshot['reserved_peak_weight_last_hour'], 0)
        with self.assertRaises(RateLimited) as caught:
            self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertEqual(self.rows(), [])

    def test_minute_rollover_keeps_history_but_resets_current_totals(self):
        self.budget.reserve('GET', '/fapi/v1/openAlgoOrders')
        self.now = 6059.999
        self.budget.reserve('GET', '/fapi/v1/time')
        self.now = 6060.0
        snapshot = self.budget.snapshot()
        self.assertEqual(snapshot['used_weight'], 0)
        self.assertEqual(snapshot['current_endpoint_usage'], [])
        self.assertEqual(snapshot['reserved_peak_weight_last_hour'], 41)
        self.budget.reserve('GET', '/fapi/v1/account')
        self.assertEqual(self.budget.snapshot()['used_weight'], 5)
        self.assertEqual(self.budget.snapshot()['current_endpoint_usage'], [{
            'method': 'GET', 'endpoint': '/fapi/v1/account', 'calls': 1,
            'reserved_weight': 5}])
        self.assertEqual(len(self.rows()), 3)

    def test_sixty_one_minute_retention_and_peak_expiration(self):
        self.budget.reserve('GET', '/fapi/v1/openOrders')  # minute 100
        self.now = 6060.0
        self.budget.reserve('GET', '/fapi/v1/account')  # minute 101
        self.now = 9600.0  # minute 160: oldest eligible minute is 100.
        self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual([row[0] for row in self.rows()], [100, 101, 160])
        self.assertEqual(self.budget.snapshot()['reserved_peak_weight_last_hour'], 40)
        self.now = 9660.0  # minute 161: remove 100 but keep 101.
        self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual([row[0] for row in self.rows()], [101, 160, 161])
        self.assertEqual(self.budget.snapshot()['reserved_peak_weight_last_hour'], 5)
        self.now = 9720.0
        self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual([row[0] for row in self.rows()], [160, 161, 162])
        self.assertEqual(self.budget.snapshot()['reserved_peak_weight_last_hour'], 1)

    def test_actual_429_cooldown_and_ip_header_shared_without_relaxing_caps(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'Retry-After': '120', 'X-MBX-USED-WEIGHT-1M': '1900'},
                            status=429, code=-1003, request_minute=int(self.now // 60))
        other = self.client()
        snapshot = other.snapshot()
        self.assertEqual(snapshot['used_weight'], 1900)
        self.assertEqual(snapshot['read_budget_per_minute'], 1200)
        self.assertEqual(snapshot['critical_budget_per_minute'], 1800)
        self.assertEqual(snapshot['reserved_peak_weight_last_hour'], 1)
        before = self.rows()
        for critical in (False, True):
            with self.subTest(critical=critical), self.assertRaises(RateLimited) as caught:
                other.reserve('GET', '/fapi/v1/time', critical=critical)
            self.assertEqual(caught.exception.source, 'exchange_cooldown')
        self.assertEqual(self.rows(), before)
        # A success header may raise totals but cannot erase exchange cooldown.
        other.observe({'X-MBX-USED-WEIGHT-1M': '2'}, status=200, request_minute=int(self.now // 60))
        self.assertEqual(other.snapshot()['cooldown_until_ms'], 6120000)
        self.assertEqual(other.snapshot()['used_weight'], 1900)
        self.now = 6120.0
        other.reserve('GET', '/fapi/v1/time')
        self.assertEqual(other.snapshot()['used_weight'], 1)
        self.assertEqual(other.snapshot()['cooldown_until_ms'], 0)

    def test_top_eight_usage_is_bounded_while_peak_counts_all_endpoints(self):
        for index in range(10):
            self.budget.reserve('GET', f'/offline/{index}')
        self.budget.reserve('GET', '/fapi/v1/openOrders')
        snapshot = self.budget.snapshot()
        self.assertEqual(len(snapshot['current_endpoint_usage']), 8)
        self.assertEqual(snapshot['current_endpoint_usage'][0]['endpoint'], '/fapi/v1/openOrders')
        self.assertEqual(snapshot['reserved_peak_weight_last_hour'], 240)
        self.assertEqual(snapshot['used_weight'], 240)


if __name__ == '__main__':
    unittest.main()
