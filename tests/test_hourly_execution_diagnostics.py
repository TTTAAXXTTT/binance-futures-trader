"""Execution evidence contracts with fake exchange I/O and temporary SQLite only."""
import io
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from decimal import Decimal
from unittest.mock import patch
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests'), str(ROOT / 'runtime/packages')]

from execution.binance import (ApiError, BinanceClient, LocalTransportDeferred,
                               OrderUncertain, PooledHTTPTransport, RequestExpired)
from execution.diagnostics import ExecutionDiagnostics, error_fields, trace_stage


class Clock:
    def __init__(self):
        self.wall = 6000.0
        self.mono = 1000.0

    def advance(self, seconds):
        self.wall += seconds
        self.mono += seconds


class Reply(io.BytesIO):
    def __init__(self, value, *, status=200, raw=False):
        super().__init__(value if raw else json.dumps(value).encode('utf-8'))
        self.status, self.headers = status, {}


class FakeTransport:
    def __init__(self, *responses, before=None):
        self.responses, self.requests = list(responses), []
        self.before = before

    def __call__(self, request, *, timeout):
        self.requests.append(request)
        if self.before is not None:
            self.before(request)
        if not self.responses:
            raise AssertionError('Unexpected fake transport request')
        value = self.responses.pop(0)
        if isinstance(value, BaseException):
            raise value
        return value if isinstance(value, Reply) else Reply(value)

    def paths(self):
        return [(r.get_method(), urlsplit(r.full_url).path) for r in self.requests]


class NativeTickTransport:
    """Serve real client/Engine REST contracts; any background fee call fails."""
    def __init__(self, clock, *, account_failure=None):
        from test_hourly_engine import FakeExchange
        self.clock, self.account_failure = clock, account_failure
        self.fixture, self.requests = FakeExchange(), []

    def __call__(self, request, *, timeout):
        self.requests.append(request)
        path = urlsplit(request.full_url).path
        if request.get_method() != 'GET':
            raise AssertionError('Unexpected trading write in native tick fixture')
        if path == '/fapi/v1/commissionRate':
            raise URLError('Forbidden background commission query would poison shared recovery gate')
        if path == '/fapi/v1/time':
            return Reply({'serverTime': int(self.clock.wall * 1000)})
        if path == '/fapi/v3/account':
            if self.account_failure is not None:
                raise self.account_failure
            return Reply({'totalWalletBalance': '1000', 'totalMarginBalance': '1000',
                'totalUnrealizedProfit': '0', 'totalMaintMargin': '0', 'availableBalance': '1000',
                'feeTier': 3, 'feeBurn': True, 'private': 'private-native-account-marker'})
        if path in {'/fapi/v1/openOrders', '/fapi/v1/openAlgoOrders',
                    '/fapi/v3/positionRisk', '/fapi/v1/income'}:
            return Reply([])
        if path == '/fapi/v1/exchangeInfo':
            return Reply(self.fixture.exchange_info())
        if path == '/fapi/v1/leverageBracket':
            return Reply(self.fixture.leverage_brackets())
        raise AssertionError('Unexpected native tick endpoint: ' + path)

    def paths(self):
        return [(r.get_method(), urlsplit(r.full_url).path) for r in self.requests]


class WireReply:
    def __init__(self, value):
        self.status, self.headers = 200, {}
        self.body = json.dumps(value).encode('utf-8')

    def read(self):
        return self.body

    def release_conn(self):
        pass

    def close(self):
        pass


class FakePools:
    def __init__(self):
        self.calls = []

    def factory(self, *args, **kwargs):
        owner = self
        class Pool:
            def request(self, method, url, **kwargs):
                owner.calls.append((method, url))
                return WireReply({'ok': True})

            def clear(self):
                pass
        return Pool()


class RefusingLock:
    def acquire(self, **kwargs):
        return False


class AdvancingLock:
    def __init__(self, clock, seconds):
        self.lock, self.clock, self.seconds = threading.RLock(), clock, seconds

    def acquire(self, **kwargs):
        result = self.lock.acquire(**kwargs)
        if result:
            self.clock.advance(self.seconds)
        return result

    def release(self):
        self.lock.release()


class DiagnosticsChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.path = self.directory / 'evidence.sqlite'
        self.clock = Clock()
        self.diag = self.recorder()

    def recorder(self, *, path=None, max_records=1000):
        return ExecutionDiagnostics(path or self.path, release='offline-stage1-test',
            monotonic=lambda: self.clock.mono, wall=lambda: self.clock.wall,
            max_records=max_records)

    def client(self, transport, *, diagnostics=None, **kwargs):
        return BinanceClient('offline-public-key', 'offline-private-secret',
            transport=transport, auto_sync_time=kwargs.pop('auto_sync_time', False),
            clock=lambda: self.clock.wall, monotonic=lambda: self.clock.mono,
            diagnostics=self.diag if diagnostics is None else diagnostics, **kwargs)

    def rows(self, table='operations'):
        with closing(sqlite3.connect(self.path)) as db:
            values = db.execute('SELECT value FROM ' + table + ' ORDER BY rowid').fetchall()
        return [json.loads(value[0]) for value in values]

    def request(self, method, path):
        values = self.diag.snapshot()['last']['requests']
        return next(v for v in values if v['method'] == method and v['path'] == path)

    def assert_absent(self, *secrets):
        persisted = self.rows() + self.rows('processes')
        text = json.dumps([self.diag.snapshot(), persisted], ensure_ascii=False)
        for secret in secrets:
            self.assertNotIn(secret, text)

    def test_constructor_is_inert_and_fee_tier_unobserved_remains_unknown(self):
        transport = FakeTransport()
        client = self.client(transport)
        self.assertEqual(transport.requests, [])
        self.assertFalse(self.path.exists())
        snapshot = self.diag.snapshot()
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            fee = snapshot['observations'].get('account_fee_' + symbol, {})
            self.assertIsNone(fee.get('makerCommissionRate'))
            self.assertIsNone(fee.get('takerCommissionRate'))
        self.assertIs(client.diagnostics, self.diag)

    def test_secrets_in_success_params_headers_and_raw_response_are_not_recorded(self):
        marker = 'response-private-marker-913'
        param = 'private-param-912'
        transport = FakeTransport({'ok': marker, 'signature': 'private-response-signature'})
        result = self.client(transport).signed('GET', '/fapi/v1/order',
            {'symbol': 'BTCUSDT', 'origClientOrderId': param, 'private': param})
        self.assertEqual(result['ok'], marker)
        query = parse_qs(urlsplit(transport.requests[0].full_url).query)
        signature = query['signature'][0]
        self.assert_absent(marker, param, signature, 'offline-public-key',
                           'offline-private-secret', 'private-response-signature', 'signature=')
        self.assertEqual(self.request('GET', '/fapi/v1/order')['wire_boundary_count'], 1)

    def test_secrets_in_network_exception_and_proxy_url_are_not_recorded(self):
        private = 'url-private-marker-808'
        proxy = 'proxy-private-password-809'
        raw = 'https://fapi.binance.com/fapi/v1/order?signature=' + private
        transport = FakeTransport(URLError(raw + ' http://user:' + proxy + '@proxy.example'))
        with self.assertRaises(OrderUncertain):
            self.client(transport).signed('POST', '/fapi/v1/order',
                {'newClientOrderId': private, 'symbol': 'BTCUSDT'})
        self.assert_absent(private, proxy, raw, 'offline-private-secret', 'offline-public-key')
        self.assertEqual(len(transport.requests), 1)
        trace = self.request('POST', '/fapi/v1/order')
        self.assertEqual(trace['kind'], 'order_uncertain')
        self.assertTrue(trace['submission_may_have_occurred'])
        self.assertFalse(trace['definite_not_submitted'])

    def test_secrets_in_exchange_error_message_are_not_recorded(self):
        secret = 'exchange-message-private-marker-908'
        transport = FakeTransport({'code': -2015, 'msg': secret})
        with self.assertRaises(ApiError) as caught:
            self.client(transport).signed('GET', '/fapi/v3/account')
        self.assertEqual(caught.exception.code, -2015)
        self.assert_absent(secret)
        self.assertEqual(self.request('GET', '/fapi/v3/account')['kind'], 'credentials_error')

    def test_rejected_unknown_path_and_exception_name_do_not_leak(self):
        secret = 'private-path-marker-528'
        transport = FakeTransport()
        with self.assertRaises(ValueError):
            self.client(transport).signed('GET', '/fapi/v1/order?signature=' + secret)
        self.assertEqual(transport.requests, [])
        self.assert_absent(secret)
        unknown = type('private-exception-marker-529', (RuntimeError,), {})
        fields = error_fields(unknown(secret), kind=secret)
        self.assertEqual(fields['exception_type'], 'OtherException')
        self.assertEqual(fields['kind'], 'execution_review')
        self.assertNotIn(secret, json.dumps(fields))

    def test_expired_preflight_is_not_a_wire_submission(self):
        transport = FakeTransport()
        with self.assertRaises(RequestExpired):
            self.client(transport).request('POST', '/fapi/v1/order',
                {'newClientOrderId': 'offline-expired'}, signed=True,
                _deadline_ms=int(self.clock.wall * 1000))
        trace = self.request('POST', '/fapi/v1/order')
        self.assertEqual(transport.requests, [])
        self.assertEqual(trace['wire_boundary_count'], 0)
        self.assertFalse(trace['submission_may_have_occurred'])
        self.assertTrue(trace['definite_not_submitted'])
        self.assertEqual(trace['kind'], 'request_expired')
        self.assertEqual(self.diag.snapshot()['last']['write_boundary_count'], 0)

    def test_failed_initial_time_sync_does_not_mark_outer_write_submitted(self):
        transport = FakeTransport(URLError('offline time read failed'))
        with self.assertRaises(ApiError) as caught:
            self.client(transport, auto_sync_time=True).signed('POST', '/fapi/v1/order',
                {'newClientOrderId': 'offline-never-submitted'})
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/time')])
        trace = self.request('POST', '/fapi/v1/order')
        self.assertTrue(trace['definite_not_submitted'])
        self.assertEqual(trace['wire_boundary_count'], 0)
        self.assertFalse(trace['submission_may_have_occurred'])
        self.assertEqual(self.diag.snapshot()['last']['write_boundary_count'], 0)

    def test_pooled_local_slot_deferral_does_not_cross_write_boundary(self):
        pools = FakePools()
        transport = PooledHTTPTransport(pool_factory=pools.factory,
            proxy_factory=pools.factory, proxy_getter=lambda: {}, bypass=lambda _: True,
            clock=lambda: self.clock.mono)
        transport._lock = RefusingLock()
        with self.assertRaises(LocalTransportDeferred):
            self.client(transport).signed('POST', '/fapi/v1/order', {})
        trace = self.request('POST', '/fapi/v1/order')
        self.assertEqual(pools.calls, [])
        self.assertEqual(trace['wire_boundary_count'], 0)
        self.assertTrue(trace['definite_not_submitted'])
        self.assertFalse(trace['submission_may_have_occurred'])
        self.assertEqual(trace['kind'], 'local_budget_wait')

    def test_pooled_expiry_after_slot_wait_does_not_cross_write_boundary(self):
        pools = FakePools()
        transport = PooledHTTPTransport(pool_factory=pools.factory,
            proxy_factory=pools.factory, proxy_getter=lambda: {}, bypass=lambda _: True,
            clock=lambda: self.clock.mono)
        transport._lock = AdvancingLock(self.clock, 2)
        deadline = int((self.clock.wall + 1) * 1000)
        with self.assertRaises(RequestExpired):
            self.client(transport).request('POST', '/fapi/v1/order', {},
                signed=True, _deadline_ms=deadline)
        trace = self.request('POST', '/fapi/v1/order')
        self.assertEqual(pools.calls, [])
        self.assertEqual(trace['wire_boundary_count'], 0)
        self.assertTrue(trace['definite_not_submitted'])
        self.assertFalse(trace['submission_may_have_occurred'])

    def test_post_and_delete_uncertainty_never_replay_writes(self):
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                transport = FakeTransport(TimeoutError('offline uncertain result'))
                client = self.client(transport)
                with self.assertRaises(OrderUncertain):
                    client.signed(method, '/fapi/v1/order', {'newClientOrderId': 'offline-once'})
                self.assertEqual(transport.paths(), [(method, '/fapi/v1/order')])
                trace = self.request(method, '/fapi/v1/order')
                self.assertEqual(trace['attempt_count'], 1)
                self.assertEqual(trace['wire_boundary_count'], 1)
                self.assertTrue(trace['submission_may_have_occurred'])
                self.assertEqual(trace['kind'], 'order_uncertain')

    def test_invalid_write_response_after_wire_remains_uncertain(self):
        secret = 'private-body-marker-504'
        transport = FakeTransport(Reply(secret.encode(), raw=True, status=502))
        with self.assertRaises(OrderUncertain):
            self.client(transport).signed('POST', '/fapi/v1/order', {})
        trace = self.request('POST', '/fapi/v1/order')
        self.assertTrue(trace['submission_may_have_occurred'])
        self.assertEqual(trace['wire_boundary_count'], 1)
        self.assertEqual(trace['http_status'], 502)
        self.assertEqual(len(transport.requests), 1)
        self.assert_absent(secret)

    def test_get_timestamp_rejection_retries_once_and_records_nested_time_read(self):
        transport = FakeTransport({'code': -1021, 'msg': 'offline timestamp rejection'},
            {'serverTime': 6001000}, {'availableBalance': '100'})
        result = self.client(transport).account()
        self.assertEqual(result, {'availableBalance': '100'})
        self.assertEqual(transport.paths(), [('GET', '/fapi/v3/account'),
            ('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])
        outer = self.request('GET', '/fapi/v3/account')
        self.assertEqual(outer['attempt_count'], 2)
        self.assertEqual(outer['wire_boundary_count'], 2)
        self.assertFalse(outer['submission_may_have_occurred'])
        self.assertEqual(outer['responses'][0]['code'], -1021)
        self.assertEqual(self.diag.snapshot()['last']['request_count'], 2)
        self.assertEqual(self.diag.snapshot()['last']['outcome'], 'complete')

    def test_repeated_get_timestamp_rejection_is_bounded_and_classified(self):
        transport = FakeTransport({'code': -1021, 'msg': 'offline first'},
            {'serverTime': 6001000}, {'code': -1021, 'msg': 'offline second'})
        with self.assertRaises(ApiError) as caught:
            self.client(transport).account()
        self.assertEqual(caught.exception.code, -1021)
        self.assertEqual(len(transport.requests), 3)
        outer = self.request('GET', '/fapi/v3/account')
        self.assertEqual(outer['attempt_count'], 2)
        self.assertEqual(outer['kind'], 'timestamp_rejected')
        self.assertEqual(outer['code'], -1021)

    def test_timestamp_rejected_write_is_definite_rejection_without_write_replay(self):
        transport = FakeTransport({'code': -1021, 'msg': 'offline timestamp'},
                                  {'serverTime': 6001000})
        with self.assertRaises(ApiError) as caught:
            self.client(transport).signed('POST', '/fapi/v1/order', {})
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(transport.paths(), [('POST', '/fapi/v1/order'), ('GET', '/fapi/v1/time')])
        trace = self.request('POST', '/fapi/v1/order')
        self.assertTrue(trace['submission_may_have_occurred'])
        self.assertTrue(trace['definite_rejected'])
        self.assertFalse(trace['definite_not_submitted'])
        self.assertEqual(trace['kind'], 'timestamp_rejected')
        self.assertEqual(trace['wire_boundary_count'], 1)

    def test_checkpoint_is_durable_before_wire_and_final_has_same_identity(self):
        observed = []
        def on_call(request):
            pending = self.rows()[-1]
            observed.append(pending)
            self.assertEqual(pending['outcome'], 'in_progress')
            self.assertEqual(pending['active_request']['method'], 'POST')
            self.assertEqual(pending['active_request']['wire_boundary_count'], 1)
            self.assertTrue(pending['active_request']['submission_may_have_occurred'])
            self.assertEqual(pending['write_boundary_count'], 1)
        transport = FakeTransport({'ok': True}, before=on_call)
        self.assertEqual(self.client(transport).signed('POST', '/fapi/v1/order', {}), {'ok': True})
        final = self.rows()[-1]
        self.assertEqual(final['outcome'], 'complete')
        self.assertEqual(final['id'], observed[0]['id'])
        self.assertEqual(final['process_id'], observed[0]['process_id'])

    def test_process_generation_and_prior_evidence_survive_recorder_replacement(self):
        with self.diag.operation('start'):
            self.clock.advance(.1)
        first_id = self.diag.process_id
        self.clock.advance(1)
        second = self.recorder()
        with second.operation('pause'):
            self.clock.advance(.2)
        self.assertNotEqual(first_id, second.process_id)
        operations, processes = self.rows(), self.rows('processes')
        self.assertEqual([row['process_id'] for row in operations], [first_id, second.process_id])
        self.assertEqual({p['id'] for p in processes}, {first_id, second.process_id})
        self.assertEqual(processes[0]['pid'], processes[1]['pid'])
        self.assertEqual(processes[0]['release'], 'offline-stage1-test')
        self.assertNotEqual(processes[0]['started_utc'], processes[1]['started_utc'])

    def test_record_and_detail_retention_are_bounded(self):
        recorder = self.recorder(max_records=3)
        for _ in range(7):
            with recorder.operation('pause'):
                pass
        self.assertEqual(len(self.rows()), 3)
        with recorder.operation('start'):
            for _ in range(85):
                with recorder.stage('local_check'):
                    pass
                with recorder.request('GET', '/fapi/v1/time', False):
                    recorder.request_phase('request_budget', new_attempt=True)
            for _ in range(25):
                recorder.record_fault(RuntimeError('private discarded message'))
        last = recorder.snapshot()['last']
        self.assertEqual(len(last['stages']), 80)
        self.assertEqual(len(last['requests']), 80)
        self.assertEqual(len(last['faults']), 20)
        self.assertEqual(last['request_count'], 85)
        self.assertGreaterEqual(recorder.snapshot()['dropped_details'], 10)
        self.assertEqual(len(self.rows()), 3)

    def test_process_retention_is_bounded_across_generations(self):
        identities = []
        for _ in range(5):
            recorder = self.recorder(max_records=2)
            identities.append(recorder.process_id)
            with recorder.operation('pause'):
                pass
        self.assertEqual([p['id'] for p in self.rows('processes')], identities[-2:])
        self.assertEqual(len(self.rows()), 2)

    def test_locked_diagnostic_database_does_not_fail_or_replay_exchange_read(self):
        with self.diag.operation('pause'):
            pass
        db = sqlite3.connect(self.path)
        db.execute('BEGIN EXCLUSIVE')
        transport = FakeTransport({'ok': True})
        started = time.monotonic()
        try:
            result = self.client(transport).public('GET', '/fapi/v1/time')
            elapsed = time.monotonic() - started
        finally:
            db.rollback()
            db.close()
        self.assertEqual(result, {'ok': True})
        self.assertEqual(len(transport.requests), 1)
        self.assertLess(elapsed, 1.0)
        self.assertGreaterEqual(self.diag.snapshot()['persist_errors'], 1)
        self.assertEqual(self.diag.snapshot()['last']['outcome'], 'complete')
        self.assertEqual(len(self.rows()), 1)

    def test_missing_diagnostic_parent_does_not_create_ledger_or_fail_request(self):
        path = self.directory / 'absent' / 'diagnostic.sqlite'
        recorder = self.recorder(path=path)
        transport = FakeTransport({'ok': True})
        result = self.client(transport, diagnostics=recorder).public('GET', '/fapi/v1/time')
        self.assertEqual(result, {'ok': True})
        self.assertEqual(len(transport.requests), 1)
        self.assertFalse(path.parent.exists())
        self.assertGreaterEqual(recorder.snapshot()['persist_errors'], 1)
        self.assertEqual(recorder.snapshot()['last']['outcome'], 'complete')

    def test_persistence_explicitly_closes_each_diagnostic_connection(self):
        connected, closed = [], []
        original_connect = sqlite3.connect
        class TrackedConnection(sqlite3.Connection):
            def close(self):
                closed.append(self)
                super().close()
        def connect(*args, **kwargs):
            connection = original_connect(*args, factory=TrackedConnection, **kwargs)
            connected.append(connection)
            return connection
        with patch('execution.diagnostics.sqlite3.connect', side_effect=connect):
            with self.diag.operation('pause'):
                pass
        self.assertEqual(len(connected), 1)
        self.assertEqual(closed, connected)

    def test_stage_and_request_durations_use_monotonic_time_when_wall_clock_jumps(self):
        with self.diag.operation('start'):
            with self.diag.stage('account_check'):
                self.clock.mono += .15
                self.clock.wall -= 2000
            with self.diag.request('GET', '/fapi/v1/time', False):
                self.clock.mono += .05
                self.clock.wall += 50000
        last = self.diag.snapshot()['last']
        self.assertEqual(last['duration_ms'], 200)
        self.assertEqual(last['stages'][0]['duration_ms'], 150)
        self.assertEqual(last['requests'][0]['duration_ms'], 50)
        self.assertEqual(self.rows()[-1]['duration_ms'], 200)

    def test_trace_decorator_preserves_return_and_records_internally_handled_fault(self):
        recorder = self.diag
        class Owner:
            diagnostics = recorder

            @trace_stage('tick')
            def tick(self):
                self.diagnostics.record_fault(ApiError(-2015, 'private handled credential error'))
                return 'handled'
        self.assertEqual(Owner().tick(), 'handled')
        last = recorder.snapshot()['last']
        self.assertEqual(last['outcome'], 'fault')
        self.assertEqual(last['faults'][0]['phase'], 'tick')
        self.assertEqual(last['faults'][0]['kind'], 'credentials_error')
        self.assert_absent('private handled credential error')

    def test_account_and_symbol_mode_observations_reuse_only_requested_responses(self):
        account = {'canTrade': True, 'dualSidePosition': False, 'multiAssetsMargin': False,
                   'private': 'private-account-marker'}
        symbols = [{'symbol': 'BTCUSDT', 'marginType': 'CROSSED', 'leverage': '50'},
                   {'symbol': 'ETHUSDT', 'marginType': 'ISOLATED', 'leverage': 20}]
        transport = FakeTransport(account, symbols)
        client = self.client(transport)
        self.assertEqual(client.account_config(), account)
        self.assertEqual(client.symbol_config(), symbols)
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/accountConfig'),
                                             ('GET', '/fapi/v1/symbolConfig')])
        observations = self.diag.snapshot()['observations']
        self.assertEqual(observations['account_modes']['compatibility'], 'compatible')
        self.assertEqual(observations['mode_BTCUSDT']['compatibility'], 'compatible')
        self.assertEqual(observations['mode_ETHUSDT']['compatibility'], 'incompatible')
        self.assert_absent('private-account-marker')
        self.diag.observe_account({'canTrade': True})
        self.assertEqual(self.diag.snapshot()['observations']['account_modes']['compatibility'], 'unknown')
        self.diag.observe_account({'canTrade': True, 'dualSidePosition': True, 'multiAssetsMargin': False})
        self.assertEqual(self.diag.snapshot()['observations']['account_modes']['compatibility'], 'incompatible')

    def test_malformed_unicode_leverage_is_unknown_without_failing_success_response(self):
        rows = [{'symbol': 'BTCUSDT', 'marginType': 'CROSSED', 'leverage': '\u00b2'}]
        transport = FakeTransport(rows)
        self.assertEqual(self.client(transport).symbol_config(), rows)
        mode = self.diag.snapshot()['observations']['mode_BTCUSDT']
        self.assertIsNone(mode['leverage'])
        self.assertEqual(mode['compatibility'], 'unknown')
        self.assertEqual(len(transport.requests), 1)

    def test_overlong_numeric_leverage_is_unknown_without_failing_success_response(self):
        rows = [{'symbol': 'BTCUSDT', 'marginType': 'CROSSED', 'leverage': '9' * 5000}]
        transport = FakeTransport(rows)
        self.assertEqual(self.client(transport).symbol_config(), rows)
        mode = self.diag.snapshot()['observations']['mode_BTCUSDT']
        self.assertIsNone(mode['leverage'])
        self.assertEqual(mode['compatibility'], 'unknown')
        self.assertEqual(len(transport.requests), 1)

    def test_fill_fee_observations_preserve_asset_and_never_guess_account_tier(self):
        self.diag.observe_fee({'symbol': 'BTCUSDT', 'commission': '0.025',
            'commissionAsset': 'USDT', 'quoteQty': '50', 'qty': '0.001', 'price': '50000'})
        fee = self.diag.snapshot()['observations']['last_fee_BTCUSDT']
        self.assertEqual(Decimal(fee['observed_rate']), Decimal('.0005'))
        self.assertEqual(fee['asset'], 'USDT')
        self.assertTrue(fee['comparable'])
        self.assertFalse(fee['rate_is_account_tier'])
        self.assertEqual(fee['denominator_source'], 'quoteQty')
        self.assertNotIn('account_fee_BTCUSDT', self.diag.snapshot()['observations'])
        self.diag.observe_fee({'symbol': 'ETHUSDT', 'commission': '0.00003',
            'commissionAsset': 'BNB', 'qty': '.1', 'price': '3000'})
        other = self.diag.snapshot()['observations']['last_fee_ETHUSDT']
        self.assertEqual(other['asset'], 'BNB')
        self.assertEqual(Decimal(other['actual_commission']), Decimal('.00003'))
        self.assertIsNone(other['observed_rate'])
        self.assertFalse(other['comparable'])
        with self.diag.operation('reconcile'):
            pass
        self.assertEqual(self.rows()[-1]['observations']['last_fee_ETHUSDT']['asset'], 'BNB')

    def test_fill_fee_fallback_zero_fee_and_invalid_denominator_are_explicit(self):
        row = {'symbol': 'BTCUSDT', 'commission': '0.025', 'commissionAsset': 'USDT',
               'qty': '.001', 'price': '50000'}
        self.diag.observe_fee(row)
        fee = self.diag.snapshot()['observations']['last_fee_BTCUSDT']
        self.assertEqual(fee['denominator_source'], 'qty_times_price')
        self.assertEqual(Decimal(fee['observed_rate']), Decimal('.0005'))
        self.diag.observe_fee(dict(row, commission='0'))
        self.assertEqual(Decimal(self.diag.snapshot()['observations']['last_fee_BTCUSDT']['observed_rate']), 0)
        self.diag.observe_fee(dict(row, qty='0'))
        invalid = self.diag.snapshot()['observations']['last_fee_BTCUSDT']
        self.assertIsNone(invalid['observed_rate'])
        self.assertFalse(invalid['comparable'])
        self.diag.observe_fee(dict(row, commission='NaN'))
        self.assertFalse(self.diag.snapshot()['observations']['last_fee_BTCUSDT']['comparable'])

    def test_explicit_account_fee_observation_has_no_extra_requests_or_planning_change(self):
        from execution.engine import FEE
        fixed_fee = FEE
        account_fee = {'symbol': 'BTCUSDT', 'makerCommissionRate': '0.0002',
                       'takerCommissionRate': '0.0004', 'private': 'private-fee-marker'}
        transport = FakeTransport(account_fee)
        client = self.client(transport)
        self.assertEqual(transport.requests, [])
        self.assertEqual(client.commission_rate('BTCUSDT'), account_fee)
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/commissionRate')])
        observed = self.diag.snapshot()['observations']['account_fee_BTCUSDT']
        self.assertEqual(Decimal(observed['makerCommissionRate']), Decimal('.0002'))
        self.assertEqual(Decimal(observed['takerCommissionRate']), Decimal('.0004'))
        self.assertEqual(observed['source'], 'commission_rate_get')
        self.assertEqual(FEE, fixed_fee)
        self.assertEqual(FEE, Decimal('.0005'))
        self.assert_absent('private-fee-marker')
        self.diag.observe_account_fee({'symbol': 'BTCUSDT', 'makerCommissionRate': 'NaN',
                                      'takerCommissionRate': '2'})
        observed = self.diag.snapshot()['observations']['account_fee_BTCUSDT']
        self.assertIsNone(observed['makerCommissionRate'])
        self.assertIsNone(observed['takerCommissionRate'])

    def native_engine(self, *, state='running', selection=None, diagnostics=True,
                      cold_clock=False, account_failure=None):
        from execution.engine import Engine, LEDGER_META, STRATEGY, STRATEGY_VERSION
        from execution.request_budget import RequestBudget
        from execution.store import Store
        from execution.strategy import Analysis, HOUR_MS
        count = getattr(self, '_native_count', 0) + 1
        self._native_count = count
        suffix = str(count)
        recorder = self.recorder(path=self.directory / ('native-evidence-' + suffix + '.sqlite')) if diagnostics else None
        transport = NativeTickTransport(self.clock, account_failure=account_failure)
        budget = RequestBudget(self.directory / ('native-budget-' + suffix + '.sqlite'),
                               clock=lambda: self.clock.wall)
        client = BinanceClient('offline-public-key', 'offline-private-secret', transport=transport,
            clock=lambda: self.clock.wall, monotonic=lambda: self.clock.mono,
            diagnostics=recorder, auto_sync_time=cold_clock, budget=budget)
        store = Store(self.directory / ('native-live-' + suffix + '.sqlite'))
        store.set_meta('symbol_enabled', selection or {'BTCUSDT': True, 'ETHUSDT': True})
        store.set_meta(LEDGER_META, {symbol: {'strategy': STRATEGY, 'version': STRATEGY_VERSION,
            'epoch': 'fixture-' + symbol, 'initialized_at_ms': 0,
            'activation_boundary_ms': 0, 'initial_wallet': '100', 'wallet': '100'}
            for symbol in ('BTCUSDT', 'ETHUSDT')})
        engine = Engine(client, store, activation_check=lambda: True,
                        monotonic=lambda: self.clock.mono, diagnostics=recorder)
        engine.refresh_rules()
        engine.analyses = {symbol: Analysis(True, '', 1001, Decimal('100'), Decimal('1'))
                           for symbol in ('BTCUSDT', 'ETHUSDT')}
        engine.state = state
        engine._persist_intent(state == 'running', 'human_start' if state == 'running' else 'human_pause')
        # The rule warmup may calibrate a cold client. Reset its anchors to
        # exercise the normal initial-time request inside the actual tick.
        if cold_clock:
            client._time_anchor = None
            client._calibrated_time_snapshot = None
        transport.requests.clear()
        return engine, client, transport, store

    def assert_no_background_fee_requests(self, transport):
        self.assertNotIn(('GET', '/fapi/v1/commissionRate'), transport.paths())
        self.assertTrue(all(method == 'GET' for method, _ in transport.paths()))

    def test_native_tick_never_queries_commission_for_running_paused_and_symbol_switches(self):
        from execution.engine import INTENT_META
        from execution.strategy import HOUR_MS
        self.clock.wall = (1001 * HOUR_MS + 6 * 60000) / 1000
        for state in ('running', 'paused'):
            for selection in ({'BTCUSDT': True, 'ETHUSDT': True},
                              {'BTCUSDT': True, 'ETHUSDT': False},
                              {'BTCUSDT': False, 'ETHUSDT': True}):
                with self.subTest(state=state, selection=selection):
                    engine, client, transport, store = self.native_engine(state=state, selection=selection)
                    saved = store.meta(INTENT_META)
                    engine.tick()
                    self.assertTrue(engine.reconciled)
                    self.assertEqual(engine.state, state)
                    self.assertEqual(engine.desired_running, state == 'running')
                    self.assertEqual(store.meta(INTENT_META), saved)
                    self.assertEqual(engine.symbol_enabled, selection)
                    self.assert_no_background_fee_requests(transport)
                    self.assertFalse(client.network_status()['waiting'])
                    self.assertEqual(client.network_status()['failures'], 0)
                    profile = engine.snapshot()['execution_diagnostics']['observations']['account_fee_profile']
                    self.assertEqual(profile['fee_tier'], 3)
                    self.assertTrue(profile['fee_burn'])

    def test_native_tick_inside_and_outside_entry_window_has_no_background_fee_queries(self):
        from execution.strategy import HOUR_MS
        for elapsed in (2000, 5 * 60000 - 1, 5 * 60000, 6 * 60000):
            with self.subTest(elapsed=elapsed):
                self.clock.wall = (1001 * HOUR_MS + elapsed) / 1000
                engine, client, transport, _ = self.native_engine()
                engine.tick()
                self.assertTrue(engine.reconciled)
                self.assertEqual(engine.state, 'running')
                self.assert_no_background_fee_requests(transport)
                self.assertFalse(client.network_status()['waiting'])
                self.assertEqual(client.network_status()['failures'], 0)

    def test_native_tick_cold_clock_performs_only_existing_time_calibration(self):
        from execution.strategy import HOUR_MS
        self.clock.wall = (1001 * HOUR_MS + 6 * 60000) / 1000
        engine, client, transport, _ = self.native_engine(cold_clock=True)
        engine.tick()
        self.assertEqual(transport.paths()[0], ('GET', '/fapi/v1/time'))
        self.assertEqual(transport.paths().count(('GET', '/fapi/v1/time')), 1)
        self.assertTrue(client.clock_status()['synced'])
        self.assertEqual(engine.state, 'running')
        self.assertTrue(engine.reconciled)
        self.assert_no_background_fee_requests(transport)
        self.assertFalse(client.network_status()['waiting'])

    def test_native_tick_observations_do_not_change_request_quota_or_shared_network_state(self):
        from execution.strategy import HOUR_MS
        self.clock.wall = (1001 * HOUR_MS + 6 * 60000) / 1000
        observed, with_diag, recorded_transport, observed_store = self.native_engine()
        control, without_diag, control_transport, control_store = self.native_engine(diagnostics=False)
        observed.tick()
        control.tick()
        self.assertEqual(recorded_transport.paths(), control_transport.paths())
        self.assertEqual(with_diag.network_status(), without_diag.network_status())
        self.assertEqual(with_diag.budget.snapshot(), without_diag.budget.snapshot())
        self.assertEqual(observed.state, control.state)
        self.assertEqual(observed.desired_running, control.desired_running)
        self.assertEqual(observed._recovery, control._recovery)
        self.assertEqual(observed_store.meta('trading_intent_v1'), control_store.meta('trading_intent_v1'))
        self.assert_no_background_fee_requests(recorded_transport)
        self.assertFalse(with_diag.network_status()['waiting'])
        self.assertEqual(with_diag.network_status()['retry_in_seconds'], 0)

    def test_native_tick_network_deadline_is_existing_read_failure_without_new_fee_calls(self):
        from execution.strategy import HOUR_MS
        self.clock.wall = (1001 * HOUR_MS + 6 * 60000) / 1000
        error = TimeoutError('offline account response deadline')
        observed, with_diag, recorded_transport, observed_store = self.native_engine(account_failure=error)
        control, without_diag, control_transport, control_store = self.native_engine(
            diagnostics=False, account_failure=TimeoutError('offline account response deadline'))
        observed.tick()
        control.tick()
        self.assertEqual(recorded_transport.paths(), [('GET', '/fapi/v3/account')])
        self.assertEqual(recorded_transport.paths(), control_transport.paths())
        self.assertEqual(with_diag.network_status(), without_diag.network_status())
        self.assertEqual(with_diag.budget.snapshot(), without_diag.budget.snapshot())
        self.assertEqual(observed.state, control.state)
        self.assertEqual(observed._recovery, control._recovery)
        self.assertTrue(observed.desired_running)
        self.assertEqual(observed_store.meta('trading_intent_v1'), control_store.meta('trading_intent_v1'))
        self.assertEqual(with_diag.network_status()['failures'], 1)
        self.assertEqual(with_diag.network_status()['retry_in_seconds'], 600)
        self.assert_no_background_fee_requests(recorded_transport)

    def test_native_tick_existing_retry_gate_is_not_extended_by_fee_observation(self):
        from execution.strategy import HOUR_MS
        self.clock.wall = (1001 * HOUR_MS + 6 * 60000) / 1000
        engine, client, transport, store = self.native_engine()
        client._network_retry.failed('offline existing network failure')
        before = client.network_status()
        intent = store.meta('trading_intent_v1')
        engine.tick()
        self.assertEqual(transport.paths(), [])
        self.assertEqual(client.network_status(), before)
        self.assertTrue(engine.desired_running)
        self.assertEqual(store.meta('trading_intent_v1'), intent)
        self.assert_no_background_fee_requests(transport)

    def test_existing_account_get_fee_profile_is_sanitized_and_never_becomes_a_rate(self):
        secret = 'private-wallet-fee-marker'
        for raw, tier, burn in (({'feeTier': 4, 'feeBurn': True}, 4, True),
                                ({'feeTier': False, 'feeBurn': secret}, None, None),
                                ({}, None, None)):
            with self.subTest(raw=raw):
                payload = dict(raw, private=secret, signature=secret)
                transport = FakeTransport(payload)
                self.assertEqual(self.client(transport).account(), payload)
                self.assertEqual(transport.paths(), [('GET', '/fapi/v3/account')])
                observations = self.diag.snapshot()['observations']
                profile = observations['account_fee_profile']
                self.assertEqual(profile['fee_tier'], tier)
                self.assertEqual(profile['fee_burn'], burn)
                self.assertEqual(profile['source'], 'existing_account_get')
                self.assertNotIn('makerCommissionRate', profile)
                self.assertNotIn('takerCommissionRate', profile)
                self.assertNotIn('account_fee_BTCUSDT', observations)
                self.assert_absent(secret)

    def test_merged_account_mode_fallback_is_observed_without_extra_requests(self):
        from execution.engine import Engine
        from execution.store import Store
        transport = FakeTransport({'canTrade': True}, {'dualSidePosition': False},
                                  {'multiAssetsMargin': False})
        client = self.client(transport)
        engine = Engine(client, Store(self.directory / 'fake-live.sqlite'),
                        monotonic=lambda: self.clock.mono, diagnostics=self.diag)
        config = engine._fresh_account_configuration()
        self.assertEqual(config, {'canTrade': True, 'dualSidePosition': False, 'multiAssetsMargin': False})
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/accountConfig'),
            ('GET', '/fapi/v1/positionSide/dual'), ('GET', '/fapi/v1/multiAssetsMargin')])
        self.assertEqual(self.diag.snapshot()['observations']['account_modes']['compatibility'], 'compatible')


if __name__ == '__main__':
    unittest.main()
