"""Keep-alive transport contract, using fake pools/sockets and clocks only."""
import json
import ssl
import sys
import tempfile
import traceback
import unittest
from http.client import IncompleteRead, RemoteDisconnected
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/packages')]
import urllib3
from execution.binance import (ApiError, BinanceClient, OrderUncertain, PooledHTTPTransport,
                               ReadBudgetDeferred, transport_failure_reason)
from execution.request_budget import RequestBudget


class WireResponse:
    def __init__(self, value=None, *, status=200, headers=None, failure=None):
        self.body = json.dumps(value).encode()
        self.status, self.headers = status, headers or {}
        self.failure = failure
        self.releases = self.closes = 0

    def read(self):
        if self.failure:
            raise self.failure
        return self.body

    def release_conn(self):
        self.releases += 1

    def close(self):
        self.closes += 1


class Pools:
    def __init__(self, *results):
        self.results, self.created, self.calls = list(results), [], []

    def factory(self, *args, **options):
        owner = self
        class Pool:
            clears = 0
            def request(self, method, url, **kwargs):
                owner.calls.append((method, url, kwargs, self))
                value = owner.results.pop(0)
                if isinstance(value, BaseException):
                    raise value
                return value
            def clear(self):
                self.clears += 1
        pool = Pool()
        self.created.append((args, options, pool))
        return pool


class PoolTransportTests(unittest.TestCase):
    def setUp(self):
        self.now = 6000.0

    def transport(self, pools, *, proxies=None, bypass=False):
        return PooledHTTPTransport(pool_factory=pools.factory, proxy_factory=pools.factory,
            proxy_getter=Mock(return_value=proxies or {}), bypass=Mock(return_value=bypass),
            clock=lambda: self.now)

    def client(self, transport, **kwargs):
        return BinanceClient('offline-key', 'offline-secret', transport=transport,
            auto_sync_time=False, clock=lambda: self.now, monotonic=lambda: self.now, **kwargs)

    def test_constructor_inert_and_custom_transport_preserved(self):
        pools = Pools()
        transport = self.transport(pools)
        client = self.client(transport)
        self.assertIs(client.transport, transport)
        self.assertEqual(pools.created, [])
        transport.proxy_getter.assert_not_called()
        self.assertEqual(transport.snapshot()['wire_attempts'], 0)

    def test_fully_consumed_responses_reuse_pool_and_preserve_system_tls(self):
        first, second = WireResponse({'one': 1}), WireResponse({'two': 2})
        pools = Pools(first, second)
        transport = self.transport(pools)
        client = self.client(transport)
        self.assertEqual(client.public('GET', '/fapi/v1/time'), {'one': 1})
        self.assertEqual(client.signed('GET', '/fapi/v3/account'), {'two': 2})
        self.assertEqual(len(pools.created), 1)
        self.assertIs(pools.calls[0][3], pools.calls[1][3])
        self.assertEqual((first.releases, second.releases), (1, 1))
        self.assertEqual((first.closes, second.closes), (0, 0))
        options = pools.created[0][1]
        self.assertEqual(options['maxsize'], 2)
        self.assertTrue(options['block'])
        self.assertFalse(options['retries'])
        self.assertEqual(options['ssl_context'].verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(options['ssl_context'].check_hostname)
        self.assertEqual(transport.snapshot()['pool_reuse_requests'], 1)
        self.assertNotIn('offline-key', json.dumps(client.connection_status()))

    def test_actual_socket_identity_counter_is_separate_from_manager_reuse(self):
        same_socket = object()
        first, second, replacement = WireResponse({}), WireResponse({}), WireResponse({}, headers={'Connection': 'close'})
        first.connection = second.connection = SimpleNamespace(sock=same_socket)
        replacement.connection = SimpleNamespace(sock=object())
        pools = Pools(first, second, replacement)
        transport = self.transport(pools)
        client = self.client(transport)
        for _ in range(3):
            client.public('GET', '/fapi/v1/time')
        metrics = client.connection_status()
        self.assertEqual(metrics['pool_reuse_requests'], 2)
        self.assertEqual(metrics['socket_observations'], 3)
        self.assertEqual(metrics['socket_reuse_responses'], 1)
        self.assertEqual(metrics['peer_close_responses'], 1)
        self.assertNotIn(str(id(same_socket)), json.dumps(metrics))

    def test_explicit_proxy_and_no_proxy_bypass_are_preserved(self):
        proxy = 'http://offline-user:offline-password@127.0.0.1:7897'
        proxied = Pools(WireResponse({}))
        transport = self.transport(proxied, proxies={'https': proxy})
        self.client(transport).public('GET', '/fapi/v1/time')
        self.assertEqual(proxied.created[0][0], (proxy,))
        self.assertIs(proxied.created[0][1]['proxy_ssl_context'], proxied.created[0][1]['ssl_context'])
        direct = Pools(WireResponse({}))
        bypassed = self.transport(direct, proxies={'https': proxy}, bypass=True)
        self.client(bypassed).public('GET', '/fapi/v1/time')
        self.assertEqual(direct.created[0][0], ())
        self.assertNotIn('offline-password', json.dumps(transport.snapshot()))
        self.assertNotIn('127.0.0.1', json.dumps(transport.snapshot()))

    def test_new_client_captures_updated_proxy_without_mutating_existing_pool(self):
        configured = {'https': 'http://127.0.0.1:7897'}
        first = Pools(WireResponse({}), WireResponse({}))
        transport = self.transport(first)
        transport.proxy_getter = lambda: dict(configured)
        client = self.client(transport)
        client.public('GET', '/fapi/v1/time')
        configured['https'] = 'http://127.0.0.1:7898'
        client.public('GET', '/fapi/v1/time')
        self.assertEqual(first.created[0][0], ('http://127.0.0.1:7897',))
        self.assertEqual(len(first.created), 1)
        second = Pools(WireResponse({}))
        changed = self.transport(second)
        changed.proxy_getter = lambda: dict(configured)
        self.client(changed).public('GET', '/fapi/v1/time')
        self.assertEqual(second.created[0][0], ('http://127.0.0.1:7898',))

    def test_incomplete_body_discarded_and_get_waits_600_seconds_without_replay(self):
        failure = urllib3.exceptions.ProtocolError('private signed URL signature=sentinel',
                                                    IncompleteRead(b'private-partial', 512))
        bad = WireResponse({}, failure=failure)
        pools = Pools(bad, WireResponse({'ok': True}))
        transport = self.transport(pools)
        client = self.client(transport)
        with self.assertRaises(ApiError) as caught:
            client.public('GET', '/fapi/v1/time')
        self.assertEqual(len(pools.calls), 1)
        self.assertEqual(bad.closes, 1)
        self.assertEqual(pools.created[0][2].clears, 1)
        self.assertNotIn('sentinel', str(caught.exception))
        self.assertIn('不完整', str(caught.exception))
        self.now += 599
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        self.assertEqual(len(pools.calls), 1)
        self.now += 1
        self.assertEqual(client.public('GET', '/fapi/v1/time'), {'ok': True})
        self.assertEqual(len(pools.created), 2)

    def test_close_before_full_read_discards_connection(self):
        response = WireResponse({'unread': True})
        pools = Pools(response)
        transport = self.transport(pools)
        held = transport(Request('https://fapi.binance.com/fapi/v1/time'), timeout=12)
        held.close()
        held.close()
        self.assertEqual(response.closes, 1)
        self.assertEqual(response.releases, 0)
        self.assertEqual(pools.created[0][2].clears, 1)

    def test_server_closed_or_tls_failure_never_replays_writes(self):
        failures = [RemoteDisconnected('signed URL signature=private'),
                    urllib3.exceptions.SSLError(ssl.SSLEOFError('proxy:user:password@private'))]
        for failure in failures:
            for method in ('POST', 'DELETE'):
                with self.subTest(kind=type(failure).__name__, method=method):
                    pools = Pools(failure, WireResponse({'must-not-send': True}))
                    transport = self.transport(pools)
                    client = self.client(transport)
                    with self.assertRaises(OrderUncertain) as caught:
                        client.signed(method, '/fapi/v1/algoOrder', {'clientAlgoId': 'original-offline-id'})
                    self.assertEqual(len(pools.calls), 1)
                    self.assertEqual(caught.exception.client_id, 'original-offline-id')
                    self.assertNotIn('password', str(caught.exception))
                    self.assertNotIn('signature=', str(caught.exception))
                    self.assertFalse(pools.calls[0][2]['retries'])
                    self.assertFalse(pools.calls[0][2]['redirect'])

    def test_healthy_http_rejection_body_is_consumed_and_not_replayed(self):
        denied = WireResponse({'code': -2015, 'msg': 'offline invalid API'}, status=401)
        pools = Pools(denied, WireResponse({'next': True}))
        transport = self.transport(pools)
        client = self.client(transport)
        with self.assertRaises(ApiError) as caught:
            client.signed('GET', '/fapi/v3/account')
        self.assertEqual(caught.exception.code, -2015)
        self.assertEqual(caught.exception.recovery_kind, 'credentials_error')
        self.assertEqual(denied.releases, 1)
        self.assertEqual(client.public('GET', '/fapi/v1/time'), {'next': True})
        self.assertEqual(len(pools.created), 1)

    def test_idle_pool_is_discarded_before_a_new_submission_and_close_is_final(self):
        pools = Pools(WireResponse({}), WireResponse({}))
        transport = self.transport(pools)
        client = self.client(transport)
        client.public('GET', '/fapi/v1/time')
        self.now += 61
        client.public('GET', '/fapi/v1/time')
        self.assertEqual(len(pools.created), 2)
        self.assertEqual(pools.created[0][2].clears, 1)
        client.close()
        self.assertTrue(client.connection_status()['closed'])
        with self.assertRaises(RuntimeError):
            transport(Request('https://fapi.binance.com/fapi/v1/time'), timeout=12)
        self.assertEqual(len(pools.calls), 2)

    def test_programming_error_is_not_reclassified_as_a_network_wait(self):
        pools = Pools(TypeError('offline programming defect'))
        client = self.client(self.transport(pools))
        with self.assertRaises(TypeError):
            client.public('GET', '/fapi/v1/time')
        self.assertFalse(client.network_status()['waiting'])

    def test_local_budget_message_is_distinct_from_real_exchange_1003(self):
        with tempfile.TemporaryDirectory() as folder:
            budget = RequestBudget(Path(folder) / 'budget.sqlite', clock=lambda: self.now, read_cap=1)
            budget.reserve('GET', '/fapi/v1/time')
            local_pool = Pools()
            with self.assertRaises(ReadBudgetDeferred) as local:
                self.client(self.transport(local_pool), budget=budget).public('GET', '/fapi/v1/time')
            self.assertEqual(local.exception.code, -1003)
            self.assertEqual(local.exception.recovery_kind, 'local_budget_wait')
            self.assertTrue(local.exception.definite_not_submitted)
            self.assertNotIn('Binance -1003', str(local.exception))
            self.assertIn('本地', str(local.exception))
            self.assertEqual(local_pool.calls, [])
            self.now += 60
            actual = Pools(WireResponse({'code': -1003, 'msg': 'real rate rejection'}, status=429,
                                       headers={'Retry-After': '120'}))
            with self.assertRaises(ApiError) as real:
                self.client(self.transport(actual), budget=budget).public('GET', '/fapi/v1/time')
            self.assertEqual(real.exception.recovery_kind, 'exchange_cooldown')
            self.assertIn('Binance -1003', str(real.exception))
            self.assertEqual(len(actual.calls), 1)
            self.assertGreater(budget.snapshot()['cooldown_seconds'], 0)

    def test_quantity_stop_parameters_use_reduce_only_and_never_close_position(self):
        pools = Pools(WireResponse({'algoId': 12}))
        client = self.client(self.transport(pools))
        client.new_stop_quantity('ETHUSDT', 'BUY', '2599.46', 'offline-stop', '.579')
        method, url, kwargs, _ = pools.calls[0]
        fields = {key: value[0] for key, value in parse_qs(kwargs['body'].decode()).items()}
        self.assertEqual((method, urlsplit(url).path), ('POST', '/fapi/v1/algoOrder'))
        for key, value in {'quantity': '0.579', 'reduceOnly': 'true', 'positionSide': 'BOTH',
                           'type': 'STOP_MARKET', 'triggerPrice': '2599.46',
                           'workingType': 'CONTRACT_PRICE', 'priceProtect': 'false'}.items():
            self.assertEqual(fields[key], value)
        self.assertNotIn('closePosition', fields)
        self.assertEqual(len(pools.calls), 1)

    def test_native_pool_timestamp_rejection_calibrates_but_does_not_replay_write(self):
        pools = Pools(WireResponse({'code': -1021, 'msg': 'timestamp rejected'}, status=400),
                      WireResponse({'serverTime': 6000000}), WireResponse({'must-not-send': True}))
        client = self.client(self.transport(pools))
        with self.assertRaises(ApiError) as caught:
            client.new_stop_quantity('ETHUSDT', 'BUY', '2600', 'original-id', '.5')
        self.assertEqual(caught.exception.code, -1021)
        self.assertTrue(caught.exception.definite_rejected)
        self.assertEqual([(method, urlsplit(url).path) for method, url, _, _ in pools.calls],
                         [('POST', '/fapi/v1/algoOrder'), ('GET', '/fapi/v1/time')])
        self.assertEqual(len(pools.created), 1)

    def test_native_pool_failure_is_shared_across_clients_until_600_second_boundary(self):
        with tempfile.TemporaryDirectory() as folder:
            budget = RequestBudget(Path(folder) / 'shared.sqlite', clock=lambda: self.now)
            budget.network.monotonic = lambda: self.now
            first = Pools(urllib3.exceptions.SSLError(ssl.SSLEOFError('private proxy secret')))
            other = Pools(WireResponse({'serverTime': 6600000}))
            one, two = self.client(self.transport(first), budget=budget), self.client(self.transport(other), budget=budget)
            with self.assertRaises(ApiError):
                one.public('GET', '/fapi/v1/time')
            self.now += 599
            with self.assertRaises(ApiError):
                two.public('GET', '/fapi/v1/time')
            self.assertEqual(len(first.calls), 1)
            self.assertEqual(len(other.calls), 0)
            self.now += 1
            self.assertEqual(two.public('GET', '/fapi/v1/time'), {'serverTime': 6600000})
            self.assertEqual(len(other.calls), 1)

    def test_proxy_auth_failure_retains_safe_reason_and_traceback_has_no_private_url(self):
        failure = urllib3.exceptions.ProxyError('http://user:private-password@proxy',
            OSError('Tunnel connection failed: 407 http://user:private-password@proxy?signature=private-token'))
        pools = Pools(failure)
        client = self.client(self.transport(pools))
        with self.assertRaises(ApiError) as caught:
            client.signed('GET', '/fapi/v3/account')
        text = ''.join(traceback.format_exception(caught.exception))
        self.assertIn('407', str(caught.exception))
        self.assertNotIn('private-password', text)
        self.assertNotIn('private-token', text)
        self.assertEqual(len(pools.calls), 1)


if __name__ == '__main__':
    unittest.main()
