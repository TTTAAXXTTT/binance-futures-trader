"""Structured recovery boundaries; virtual clocks and fake transport only."""
import io
import json
import sys
import tempfile
import unittest
from email.utils import formatdate
from pathlib import Path
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.binance import (ApiError, BinanceClient, NetworkRetryPending,
                               OrderUncertain, ReadBudgetDeferred, RequestExpired)
from execution.request_budget import RequestBudget, RateLimited
from test_hourly_binance import Response, Transport


def http_failure(status, code=-1003, headers=None, body=None):
    payload = json.dumps({'code': code, 'msg': 'offline exchange rejection'}).encode() if body is None else body
    return HTTPError('https://offline.invalid', status, 'offline', headers or {}, io.BytesIO(payload))


class RecoveryTransportTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.wall = self.mono = 6000.0
        self.budget = RequestBudget(Path(self.folder.name) / 'shared.sqlite',
            clock=lambda: self.wall, read_cap=5, critical_cap=8)
        self.budget.network.monotonic = lambda: self.mono

    def client(self, transport, *, budget=True, auto_sync_time=False, **kwargs):
        return BinanceClient('offline-key', 'offline-secret', transport,
            budget=self.budget if budget else None, clock=lambda: self.wall,
            monotonic=lambda: self.mono, auto_sync_time=auto_sync_time,
            sleep=lambda _: self.fail('Recovery must not block or sleep'), **kwargs)

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds

    def saturate(self, used=5):
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': str(used)}, request_minute=int(self.wall // 60))

    def test_rate_limited_source_and_exact_minute_boundary(self):
        self.advance(58)
        self.saturate()
        with self.assertRaises(RateLimited) as caught:
            self.budget.reserve('GET', '/fapi/v1/time')
        error = caught.exception
        self.assertEqual(error.source, 'local_budget')
        self.assertEqual(error.recovery_kind, 'local_budget_wait')
        self.assertEqual(error.retry_at_ms, 6060500)
        self.assertEqual(error.retry_in_seconds, 2.5)
        self.assertTrue(error.definite_not_submitted)
        self.assertEqual(self.budget.snapshot()['cooldown_until_ms'], 0)

    def test_local_get_wait_is_not_http_429_and_has_no_wire(self):
        self.saturate()
        transport = Transport()
        with self.assertRaises(ReadBudgetDeferred) as caught:
            self.client(transport).account()
        error = caught.exception
        self.assertEqual(error.code, -1003)  # Existing code consumers remain compatible.
        self.assertIsNone(error.status)
        self.assertEqual(error.recovery_kind, 'local_budget_wait')
        self.assertTrue(error.definite_not_submitted)
        self.assertFalse(error.definite_rejected)
        self.assertEqual(transport.requests, [])
        self.assertEqual(self.budget.network.snapshot()['failures'], 0)

    def test_local_wait_shared_with_second_client_then_recovers_next_minute(self):
        self.saturate()
        first, second = Transport(), Transport({'serverTime': 6060500})
        with self.assertRaises(ReadBudgetDeferred):
            self.client(first).public('GET', '/fapi/v1/time')
        c = self.client(second)
        with self.assertRaises(ReadBudgetDeferred):
            c.public('GET', '/fapi/v1/time')
        self.advance(60.5)
        self.assertEqual(c.public('GET', '/fapi/v1/time')['serverTime'], 6060500)
        self.assertEqual(len(second.requests), 1)
        self.assertEqual(first.requests, [])

    def test_post_and_delete_local_wait_are_definitely_not_submitted(self):
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                self.saturate(8)
                transport = Transport()
                with self.assertRaises(ApiError) as caught:
                    self.client(transport).signed(method, '/fapi/v1/order', {'newClientOrderId': 'offline'})
                self.assertNotIsInstance(caught.exception, OrderUncertain)
                self.assertEqual(caught.exception.recovery_kind, 'local_budget_wait')
                self.assertTrue(caught.exception.definite_not_submitted)
                self.assertIsNone(caught.exception.status)
                self.assertEqual(transport.requests, [])

    def test_actual_429_has_exchange_kind_and_retry_after(self):
        transport = Transport(http_failure(429, headers={'Retry-After': '180'}))
        c = self.client(transport)
        with self.assertRaises(ApiError) as caught:
            c.public('GET', '/fapi/v1/time')
        error = caught.exception
        self.assertNotIsInstance(error, ReadBudgetDeferred)
        self.assertEqual(error.status, 429)
        self.assertEqual(error.recovery_kind, 'exchange_cooldown')
        self.assertEqual(error.retry_at_ms, 6180000)
        self.assertEqual(error.retry_in_seconds, 180)
        self.assertFalse(error.definite_not_submitted)
        with self.assertRaises(ReadBudgetDeferred) as waiting:
            c.public('GET', '/fapi/v1/time')
        self.assertEqual(waiting.exception.recovery_kind, 'exchange_cooldown')
        self.assertTrue(waiting.exception.definite_not_submitted)
        self.assertEqual(len(transport.requests), 1)

    def test_real_429_without_database_still_exposes_retry_after(self):
        with self.assertRaises(ApiError) as caught:
            self.client(Transport(http_failure(429, headers={'Retry-After': '240'})), budget=False).account()
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertEqual(caught.exception.retry_in_seconds, 240)

    def test_418_ban_uses_longer_exchange_deadline(self):
        with self.assertRaises(ApiError) as caught:
            self.client(Transport(http_failure(418, headers={'Retry-After': '900'}))).public('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertEqual(caught.exception.retry_in_seconds, 900)
        self.assertEqual(self.budget.network.snapshot()['failures'], 0)

    def test_http_date_retry_after_and_shared_database_preserve_exchange_origin(self):
        deadline = self.wall + 240
        self.budget.observe({'Retry-After': formatdate(deadline, usegmt=True)}, status=429)
        second = RequestBudget(self.budget.path, clock=lambda: self.wall)
        with self.assertRaises(RateLimited) as caught:
            second.reserve('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.source, 'exchange_cooldown')
        self.assertEqual(caught.exception.retry_at_ms, int(deadline * 1000))
        self.assertEqual(caught.exception.retry_in_seconds, 240)

    def test_banned_until_overrides_shorter_retry_after(self):
        self.wall = 1700000000.0
        deadline = self.wall + 800
        self.budget.observe({'Retry-After': '120'}, status=418, code=-1003,
                            message=f'IP banned until {int(deadline * 1000)}')
        with self.assertRaises(RateLimited) as caught:
            self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertEqual(caught.exception.retry_in_seconds, 800)

    def test_invalid_nonfinite_retry_after_uses_definite_default_cooldown(self):
        for retry in ('inf', 'nan', '-1', 'not-a-date'):
            with self.subTest(retry=retry):
                self.budget.observe({'Retry-After': retry}, status=429)
                self.assertEqual(self.budget.snapshot()['cooldown_until_ms'], 6060000)

    def test_rate_cooldown_success_at_boundary_never_creates_network_failure(self):
        transport = Transport(http_failure(429, headers={'Retry-After': '120'}), {'serverTime': 6120000})
        c = self.client(transport)
        with self.assertRaises(ApiError):
            c.public('GET', '/fapi/v1/time')
        self.advance(119.9)
        with self.assertRaises(ReadBudgetDeferred) as waiting:
            c.public('GET', '/fapi/v1/time')
        self.assertAlmostEqual(waiting.exception.retry_in_seconds, .1)
        self.advance(.1)
        self.assertEqual(c.public('GET', '/fapi/v1/time')['serverTime'], 6120000)
        self.assertEqual(c.network_status()['failures'], 0)
        self.assertEqual(len(transport.requests), 2)

    def test_html_429_write_is_explicit_rejection_not_unknown(self):
        transport = Transport(http_failure(429, headers={'Retry-After': '120'}, body=b'<html>limit</html>'))
        with self.assertRaises(ApiError) as caught:
            self.client(transport).market_order('BTCUSDT', 'BUY', '.001', 'offline')
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(caught.exception.recovery_kind, 'exchange_cooldown')
        self.assertTrue(caught.exception.definite_rejected)
        self.assertFalse(caught.exception.definite_not_submitted)
        self.assertEqual(len(transport.requests), 1)

    def test_nonjson_other_write_responses_keep_original_uncertain_semantics(self):
        for status in (200, 400, 403, 408, 500):
            with self.subTest(status=status):
                transport = Transport(http_failure(status, body=b'<html>unconfirmed</html>'))
                with self.assertRaises(OrderUncertain) as caught:
                    self.client(transport, budget=False).market_order('BTCUSDT', 'BUY', '.001', 'offline')
                self.assertEqual(caught.exception.recovery_kind, 'order_uncertain')
                self.assertFalse(caught.exception.definite_not_submitted)
                self.assertFalse(caught.exception.definite_rejected)
                self.assertEqual(len(transport.requests), 1)

    def test_sync_local_wait_never_becomes_cached_600_second_failure(self):
        self.saturate()
        transport = Transport({'serverTime': 6060500})
        c = self.client(transport, auto_sync_time=True)
        for call in (c.sync_time, c.ensure_time_sync):
            with self.assertRaises(ReadBudgetDeferred) as caught:
                call()
            self.assertEqual(caught.exception.recovery_kind, 'local_budget_wait')
            self.assertEqual(caught.exception.retry_in_seconds, 60.5)
            self.assertEqual(c.clock_status()['sync_failures'], 0)
            self.assertEqual(c.clock_status()['retry_in_seconds'], 0)
            self.assertIsNone(c._time_sync_error)
        self.advance(60.5)
        self.assertEqual(c.ensure_time_sync(), 6060500)
        self.assertEqual(len(transport.requests), 1)

    def test_sync_actual_cooldown_preserves_kind_and_does_not_add_600(self):
        transport = Transport(http_failure(429, headers={'Retry-After': '120'}), {'serverTime': 6120000})
        c = self.client(transport, auto_sync_time=True)
        with self.assertRaises(ApiError) as caught:
            c.ensure_time_sync()
        self.assertEqual(caught.exception.retry_in_seconds, 120)
        self.assertEqual(c.clock_status()['sync_failures'], 0)
        self.advance(119)
        with self.assertRaises(ReadBudgetDeferred) as waiting:
            c.ensure_time_sync()
        self.assertEqual(waiting.exception.retry_in_seconds, 1)
        self.assertEqual(waiting.exception.recovery_kind, 'exchange_cooldown')
        self.advance(1)
        self.assertEqual(c.ensure_time_sync(), 6120000)
        self.assertEqual(len(transport.requests), 2)

    def test_signed_write_time_preflight_local_wait_marks_original_not_submitted(self):
        self.saturate()
        transport = Transport()
        with self.assertRaises(ReadBudgetDeferred) as caught:
            self.client(transport, auto_sync_time=True).market_order('BTCUSDT', 'BUY', '.001', 'offline')
        error = caught.exception
        self.assertTrue(error.definite_not_submitted)
        self.assertEqual(error.submission_method, 'POST')
        self.assertEqual(error.submission_path, '/fapi/v1/order')
        self.assertEqual(error.path, '/fapi/v1/time')
        self.assertEqual(transport.requests, [])

    def test_signed_delete_preflight_network_failure_has_no_cancel_wire(self):
        transport = Transport(TimeoutError('do not expose secret detail'))
        with self.assertRaises(ApiError) as caught:
            self.client(transport, auto_sync_time=True).cancel_order('BTCUSDT', client_id='offline')
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertEqual(caught.exception.recovery_kind, 'network_read_failure')
        self.assertEqual(caught.exception.submission_method, 'DELETE')
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/time')])

    def test_get_failure_and_shared_wait_are_distinct_and_wait_600(self):
        transport = Transport(URLError('private proxy:password@host'), {'ok': True})
        c = self.client(transport)
        with self.assertRaises(ApiError) as caught:
            c.public('GET', '/fapi/v1/time')
        self.assertEqual(caught.exception.recovery_kind, 'network_read_failure')
        self.assertEqual(caught.exception.retry_in_seconds, 600)
        self.assertFalse(caught.exception.definite_not_submitted)
        self.assertNotIn('password', str(caught.exception))
        self.advance(599)
        with self.assertRaises(NetworkRetryPending) as waiting:
            c.public('GET', '/fapi/v1/time')
        self.assertEqual(waiting.exception.recovery_kind, 'network_read_wait')
        self.assertEqual(waiting.exception.retry_in_seconds, 1)
        self.assertTrue(waiting.exception.definite_not_submitted)
        self.assertEqual(len(transport.requests), 1)
        self.advance(1)
        self.assertEqual(c.public('GET', '/fapi/v1/time'), {'ok': True})
        self.assertEqual(len(transport.requests), 2)

    def test_cached_time_network_error_keeps_recovery_metadata(self):
        transport = Transport(TimeoutError(), {'serverTime': 6600000})
        c = self.client(transport, auto_sync_time=True)
        with self.assertRaises(ApiError) as first:
            c.ensure_time_sync()
        self.assertEqual(first.exception.recovery_kind, 'network_read_failure')
        self.advance(10)
        with self.assertRaises(ApiError) as waiting:
            c.ensure_time_sync()
        self.assertEqual(waiting.exception.recovery_kind, 'network_read_wait')
        self.assertEqual(waiting.exception.retry_in_seconds, 590)
        self.assertEqual(waiting.exception.retry_at_ms, 6600000)
        self.assertTrue(waiting.exception.definite_not_submitted)
        self.assertEqual(c.clock_status()['sync_failures'], 1)
        self.advance(590)
        self.assertEqual(c.ensure_time_sync(), 6600000)

    def test_invalid_time_response_exposes_its_600_second_time_retry(self):
        transport = Transport({'serverTime': 'bad'})
        c = self.client(transport, auto_sync_time=True)
        with self.assertRaises(ApiError) as caught:
            c.ensure_time_sync()
        self.assertEqual(caught.exception.recovery_kind, 'network_read_failure')
        self.assertEqual(caught.exception.retry_in_seconds, 600)
        self.assertEqual(caught.exception.retry_at_ms, 6600000)
        self.assertEqual(c.clock_status()['sync_failures'], 1)
        self.assertEqual(len(transport.requests), 1)

    def test_http_5xx_get_and_post_keep_different_submission_certainty(self):
        for method in ('GET', 'POST'):
            with self.subTest(method=method):
                transport = Transport(http_failure(503, code=-1007))
                with self.assertRaises(ApiError) as caught:
                    self.client(transport, budget=False).signed(method, '/fapi/v1/order', {'newClientOrderId': 'offline'})
                error = caught.exception
                self.assertEqual(error.recovery_kind, 'network_read_failure' if method == 'GET' else 'order_uncertain')
                self.assertFalse(error.definite_not_submitted)
                self.assertFalse(error.definite_rejected)
                self.assertEqual(error.retry_in_seconds, 600)
                self.assertEqual(len(transport.requests), 1)

    def test_timeout_post_and_delete_unknown_never_auto_replay(self):
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                transport = Transport(TimeoutError())
                with self.assertRaises(OrderUncertain) as caught:
                    self.client(transport, budget=False).signed(method, '/fapi/v1/order', {'origClientOrderId': 'offline'})
                self.assertEqual(caught.exception.recovery_kind, 'order_uncertain')
                self.assertFalse(caught.exception.definite_not_submitted)
                self.assertFalse(caught.exception.definite_rejected)
                self.assertEqual(transport.paths(), [(method, '/fapi/v1/order')])

    def test_invalid_credentials_are_permanent_known_rejection(self):
        transport = Transport(Response({'code': -2015, 'msg': 'offline invalid key'}, status=401))
        with self.assertRaises(ApiError) as caught:
            self.client(transport, budget=False).market_order('BTCUSDT', 'BUY', '.001', 'offline')
        self.assertEqual(caught.exception.recovery_kind, 'credentials_error')
        self.assertTrue(caught.exception.definite_rejected)
        self.assertFalse(caught.exception.definite_not_submitted)
        self.assertNotIsInstance(caught.exception, OrderUncertain)

    def test_missing_credentials_are_definitely_not_submitted(self):
        transport = Transport()
        c = BinanceClient('', '', transport, auto_sync_time=False)
        with self.assertRaises(ApiError) as caught:
            c.market_order('BTCUSDT', 'BUY', '.001', 'offline')
        self.assertEqual(caught.exception.recovery_kind, 'credentials_error')
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertEqual(transport.requests, [])

    def test_get_timestamp_rejection_calibrates_and_reads_once_more(self):
        transport = Transport({'code': -1021, 'msg': 'offline timestamp'},
                              {'serverTime': 6000000}, {'ok': True})
        c = self.client(transport, budget=False)
        self.assertEqual(c.account(), {'ok': True})
        self.assertEqual(transport.paths(), [('GET', '/fapi/v3/account'), ('GET', '/fapi/v1/time'),
                                             ('GET', '/fapi/v3/account')])

    def test_write_timestamp_rejection_never_replays_after_calibration(self):
        transport = Transport({'code': -1021, 'msg': 'offline timestamp'}, {'serverTime': 6000000})
        with self.assertRaises(ApiError) as caught:
            self.client(transport, budget=False).market_order('BTCUSDT', 'BUY', '.001', 'offline')
        error = caught.exception
        self.assertEqual(error.recovery_kind, 'timestamp_rejected')
        self.assertTrue(error.definite_rejected)
        self.assertFalse(error.definite_not_submitted)
        self.assertNotIsInstance(error, OrderUncertain)
        self.assertEqual(transport.paths(), [('POST', '/fapi/v1/order'), ('GET', '/fapi/v1/time')])

    def test_write_timestamp_rejection_stays_definite_when_calibration_fails(self):
        transport = Transport({'code': -1021, 'msg': 'offline timestamp'}, TimeoutError())
        with self.assertRaises(ApiError) as caught:
            self.client(transport, budget=False).market_order('BTCUSDT', 'BUY', '.001', 'offline')
        self.assertEqual(caught.exception.recovery_kind, 'timestamp_rejected')
        self.assertEqual(caught.exception.recovery_cause_kind, 'network_read_failure')
        self.assertEqual(caught.exception.retry_in_seconds, 600)
        self.assertTrue(caught.exception.definite_rejected)
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(len(transport.requests), 2)

    def test_expired_signal_after_calibration_is_definitely_not_submitted(self):
        transport = Transport({'serverTime': 6000000})
        with self.assertRaises(RequestExpired) as caught:
            self.client(transport, budget=False, auto_sync_time=True).market_order(
                'BTCUSDT', 'BUY', '.001', 'offline', deadline_ms=6000000)
        self.assertEqual(caught.exception.recovery_kind, 'signal_expired')
        self.assertTrue(caught.exception.definite_not_submitted)
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/time')])


if __name__ == '__main__':
    unittest.main()
