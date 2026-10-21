"""Complete partial-depth quote contract and safe causes, with offline sockets."""
import copy
import json
import ssl
import sys
import unittest
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.binance import BinanceClient
from execution.streams import MarketStreams, connection_failure, STREAM_OPTIMIZATION_VERSION
from test_hourly_binance import Transport
from test_hourly_stream_retry import VirtualTime, VirtualStop, Connector, OfflineSocket


def depth(clock, symbol='BTCUSDT', **changes):
    data = {'e': 'depthUpdate', 's': symbol, 'st': 1, 'E': int(clock.wall() * 1000),
            'b': [[str(100 - i), '1'] for i in range(5)],
            'a': [[str(101 + i), '2'] for i in range(5)], **changes}
    return {'stream': symbol.lower() + '@depth5@500ms', 'data': data}


class PartialDepthCacheTests(unittest.TestCase):
    def setUp(self):
        self.clock = VirtualTime()
        self.streams = MarketStreams(clock=self.clock.wall, monotonic=self.clock.monotonic)

    def test_two_symbols_complete_snapshots_provide_best_prices_without_rest(self):
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            self.assertTrue(self.streams.ingest(depth(self.clock, symbol)))
        transport = Transport()
        client = BinanceClient('', '', transport=transport, streams=self.streams)
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            self.assertEqual(client.book_ticker(symbol),
                {'symbol': symbol, 'bidPrice': '100', 'askPrice': '101'})
        self.assertEqual(transport.requests, [])

    def test_expired_snapshot_falls_back_to_one_rest_read(self):
        self.assertTrue(self.streams.ingest(depth(self.clock)))
        self.clock.now = 3.001
        transport = Transport({'symbol': 'BTCUSDT', 'bidPrice': '99', 'askPrice': '100'})
        client = BinanceClient('', '', transport=transport, streams=self.streams)
        self.assertEqual(client.book_ticker('BTCUSDT')['bidPrice'], '99')
        self.assertEqual(transport.paths(), [('GET', '/fapi/v1/ticker/bookTicker')])

    def test_full_snapshot_replaces_best_prices_without_applying_diff_levels(self):
        self.assertTrue(self.streams.ingest(depth(self.clock)))
        self.clock.now = .5
        replacement = depth(self.clock, b=[[str(90 - i), '1'] for i in range(5)],
                            a=[[str(91 + i), '1'] for i in range(5)])
        self.assertTrue(self.streams.ingest(replacement))
        self.assertEqual(self.streams.latest('book', 'BTCUSDT')['bidPrice'], '90')
        self.assertEqual(len(self.streams.cache), 1)

    def test_outer_subscription_must_match_exact_symbol_levels_and_cadence(self):
        for name in (None, 'BTCUSDT@depth5@500ms', 'ethusdt@depth5@500ms',
                     'btcusdt@depth', 'btcusdt@depth5@100ms', 'btcusdt@depth10@500ms'):
            with self.subTest(stream=name):
                payload = depth(self.clock)
                if name is None:
                    payload.pop('stream')
                else:
                    payload['stream'] = name
                self.assertFalse(self.streams.ingest(payload))

    def test_raw_diff_depth_and_wrong_event_or_asset_type_are_rejected(self):
        self.assertFalse(self.streams.ingest(depth(self.clock)['data']))
        for changes in ({'st': 2}, {'st': True}, {'st': '1'}, {'s': 'SOLUSDT'},
                        {'e': 'depthSnapshot'}, {'E': True}, {'E': '1800000000000'}):
            with self.subTest(changes=changes):
                self.assertFalse(self.streams.ingest(depth(self.clock, **changes)))
        payload = depth(self.clock)
        payload['data'].pop('st')
        self.assertFalse(self.streams.ingest(payload))

    def test_each_side_must_contain_exactly_five_complete_levels(self):
        for side in ('b', 'a'):
            for length in (0, 1, 4, 6):
                with self.subTest(side=side, length=length):
                    payload = depth(self.clock)
                    payload['data'][side] = [['100', '1']] * length
                    self.assertFalse(self.streams.ingest(payload))
            for value in (None, {}, 'invalid', [['100']] * 5, [['100', '1', 'extra']] * 5):
                with self.subTest(side=side, malformed=True):
                    self.assertFalse(self.streams.ingest(depth(self.clock, **{side: value})))

    def test_every_price_and_quantity_must_be_positive_and_finite(self):
        for side in ('b', 'a'):
            for index in (0, 4):
                for field in (0, 1):
                    for value in ('0', '-1', 'NaN', 'Infinity', '-Infinity', 'invalid', True, None):
                        with self.subTest(side=side, index=index, field=field, value=value):
                            payload = depth(self.clock)
                            payload['data'][side][index][field] = value
                            self.assertFalse(self.streams.ingest(payload))

    def test_bids_descend_asks_ascend_and_equivalent_duplicate_prices_are_rejected(self):
        for side in ('b', 'a'):
            payload = depth(self.clock)
            payload['data'][side].reverse()
            self.assertFalse(self.streams.ingest(payload))
            payload = depth(self.clock)
            payload['data'][side][4][0] = payload['data'][side][3][0] + '.0'
            self.assertFalse(self.streams.ingest(payload))

    def test_crossed_book_rejected_but_equal_best_prices_allowed(self):
        payload = depth(self.clock, a=[[str(99 + i), '1'] for i in range(5)])
        self.assertFalse(self.streams.ingest(payload))
        payload['data']['a'] = [[str(100 + i), '1'] for i in range(5)]
        self.assertTrue(self.streams.ingest(payload))

    def test_late_or_future_snapshot_cannot_refresh_quote_at_ingest(self):
        event = int(self.clock.wall() * 1000)
        for offset in (-3001, 3001, -6000, 6000):
            with self.subTest(offset=offset):
                self.assertFalse(self.streams.ingest(depth(self.clock, E=event + offset)))
        self.assertTrue(self.streams.ingest(depth(self.clock, E=event - 3000)))
        self.assertIsNotNone(self.streams.latest('book', 'BTCUSDT'))
        self.clock.now = .001
        self.assertIsNone(self.streams.latest('book', 'BTCUSDT'))

    def test_older_or_invalid_snapshot_preserves_fresh_cache_and_seen_time(self):
        self.assertTrue(self.streams.ingest(depth(self.clock)))
        self.clock.now = .5
        self.assertTrue(self.streams.ingest(depth(self.clock)))
        cached = copy.deepcopy(self.streams.cache)
        self.assertFalse(self.streams.ingest(depth(self.clock, E=int((self.clock.wall() - .1) * 1000))))
        self.assertFalse(self.streams.ingest(depth(self.clock, b=[])))
        self.assertEqual(self.streams.cache, cached)

    def test_receipt_age_and_event_age_are_both_enforced(self):
        self.assertTrue(self.streams.ingest(depth(self.clock, E=int((self.clock.wall() + 1) * 1000))))
        self.clock.now = 3
        self.assertIsNotNone(self.streams.latest('book', 'BTCUSDT'))
        self.clock.now = 3.001
        self.assertIsNone(self.streams.latest('book', 'BTCUSDT'))
        self.clock.now = 5
        self.assertTrue(self.streams.ingest(depth(self.clock, E=int((self.clock.wall() - 2) * 1000))))
        self.clock.now = 6.001
        self.assertIsNone(self.streams.latest('book', 'BTCUSDT'))

    def test_legacy_book_ticker_ingestion_remains_compatible(self):
        self.assertTrue(self.streams.ingest({'e': 'bookTicker', 's': 'BTCUSDT',
            'E': int(self.clock.wall() * 1000), 'b': '100', 'a': '101'}))
        self.assertEqual(self.streams.latest('book', 'BTCUSDT')['askPrice'], '101')
        self.assertFalse(self.streams.ingest({'e': 'bookTicker', 's': 'BTCUSDT',
            'E': int(self.clock.wall() * 1000), 'b': '102', 'a': '101'}))

    def test_non_mapping_messages_are_invalid_instead_of_raising(self):
        for payload in (None, [], 'invalid', 1, {'data': None}, {'data': []}):
            with self.subTest(payload=payload):
                self.assertFalse(self.streams.ingest(payload))


class PartialDepthConnectionTests(unittest.TestCase):
    def make(self, *results, stop_on_wait=None):
        clock = VirtualTime()
        stop = VirtualStop(clock, stop_on_wait=stop_on_wait)
        connector = Connector(clock, *[result(stop, clock) if callable(result) else result for result in results])
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic,
                                stop_event=stop, connect=connector)
        return clock, stop, connector, streams

    def test_public_subscription_cadence_and_connection_limits(self):
        clock, stop, connector, streams = self.make(lambda event, _: OfflineSocket(event))
        streams._run('public')
        self.assertEqual(connector.calls[0][1], 'wss://fstream.binance.com/public/stream?streams='
                         'btcusdt@depth5@500ms/ethusdt@depth5@500ms')
        self.assertEqual(connector.calls[0][2],
            {'open_timeout': 12, 'ping_interval': 20, 'ping_timeout': 20, 'max_queue': 64})
        self.assertEqual(streams.snapshot()['stream_optimization_version'], STREAM_OPTIMIZATION_VERSION)
        self.assertEqual(streams.snapshot()['public_quote_source'], 'depth5@500ms')

    def test_market_subscription_is_unchanged(self):
        clock, stop, connector, streams = self.make(lambda event, _: OfflineSocket(event))
        streams._run('market')
        self.assertEqual(connector.calls[0][1], 'wss://fstream.binance.com/market/stream?streams='
            'btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@kline_1h/'
            'ethusdt@aggTrade/ethusdt@markPrice@1s/ethusdt@kline_1h')

    def test_expected_unclosed_hour_is_counted_separately_without_failure(self):
        def socket(event, clock):
            class UnclosedSocket(OfflineSocket):
                def recv(self, *, timeout):
                    event.set()
                    return json.dumps({'data': {'e': 'kline', 's': 'BTCUSDT', 'st': 1,
                        'E': int(clock.wall() * 1000), 'k': {'i': '1h', 'x': False}}})
            return UnclosedSocket(event)
        clock, stop, connector, streams = self.make(socket)
        streams._run('market')
        diagnostic = streams.snapshot()['diagnostics']['market']
        self.assertEqual(diagnostic['invalid_messages'], 1)
        self.assertEqual(diagnostic['ignored_unclosed_klines'], 1)
        self.assertIsNone(diagnostic['last_failure'])

    def test_depth_quote_is_counted_and_normal_stop_does_not_schedule_retry(self):
        def socket(event, clock):
            class DepthSocket(OfflineSocket):
                def recv(self, *, timeout):
                    event.set()
                    return json.dumps(depth(clock))
            return DepthSocket(event)
        clock, stop, connector, streams = self.make(socket)
        streams._run('public')
        snapshot = streams.snapshot()
        self.assertEqual(snapshot['diagnostics']['public']['messages_received'], 1)
        self.assertEqual(snapshot['diagnostics']['public']['invalid_messages'], 0)
        self.assertEqual(snapshot['diagnostics']['public']['phase'], 'stopped')
        self.assertIsNone(snapshot['diagnostics']['public']['last_failure'])
        self.assertIsNone(streams.retry['public']['deadline'])
        self.assertEqual(stop.waits, [])

    def test_normal_stop_during_receive_exception_is_not_a_failure(self):
        def socket(event, clock):
            class StoppedSocket(OfflineSocket):
                def recv(self, *, timeout):
                    event.set()
                    raise OSError('do not store this text')
            return StoppedSocket(event)
        clock, stop, connector, streams = self.make(socket)
        streams._run('public')
        self.assertIsNone(streams.snapshot()['diagnostics']['public']['last_failure'])
        self.assertEqual(stop.waits, [])

    def test_attempt_pressure_lag_compression_duration_and_failure_phase(self):
        def socket(event, clock):
            class PressureSocket(OfflineSocket):
                def __init__(self):
                    super().__init__(event)
                    self.calls = 0
                    self.recv_exc = ConnectionResetError(10054, 'secret request URL')
                    self.protocol = SimpleNamespace(parser_exc=EOFError('secret parser text'),
                        extensions=[SimpleNamespace(name='permessage-deflate')])
                    self.recv_messages = SimpleNamespace(frames=SimpleNamespace(qsize=lambda: 75), paused=True)
                def recv(self, *, timeout):
                    self.calls += 1
                    clock.now += .5
                    if self.calls == 1:
                        return json.dumps(depth(clock, E=int((clock.wall() - .2) * 1000)))
                    raise Closed('secret close text') from self.recv_exc
            class Closed(Exception):
                pass
            return PressureSocket()
        clock, stop, connector, streams = self.make(socket, stop_on_wait=1)
        streams._run('public')
        diagnostic = streams.snapshot()['diagnostics']['public']
        failure = diagnostic['last_failure']
        self.assertEqual(failure['kind'], 'connection_reset')
        self.assertEqual(failure['phase'], 'receive')
        self.assertEqual(failure['connection_duration_seconds'], 1)
        self.assertEqual(failure['queue_peak_frames'], 75)
        self.assertTrue(failure['receive_paused_observed'])
        self.assertEqual(failure['event_lag_peak_ms'], 200)
        self.assertTrue(failure['compression_negotiated'])
        self.assertEqual({item['source'] for item in failure['exception_chain']},
                         {'exception', 'cause', 'recv_exc', 'parser_exc'})
        self.assertEqual(stop.waits, [600])
        self.assertNotIn('secret', json.dumps(diagnostic))

    def test_next_attempt_resets_pressure_but_retains_previous_failure(self):
        def pressured(event, clock):
            class PressureSocket(OfflineSocket):
                calls = 0
                recv_messages = SimpleNamespace(frames=SimpleNamespace(qsize=lambda: 80), paused=True)
                protocol = SimpleNamespace(extensions=[])
                def recv(self, *, timeout):
                    self.calls += 1
                    if self.calls == 1:
                        return json.dumps(depth(clock))
                    raise EOFError('private text')
            return PressureSocket(event)
        clock, stop, connector, streams = self.make(pressured, lambda event, _: OfflineSocket(event))
        streams._run('public')
        diagnostic = streams.snapshot()['diagnostics']['public']
        self.assertEqual([call[0] for call in connector.calls], [0, 600])
        self.assertEqual(diagnostic['queue_peak_frames'], 80)
        self.assertEqual(diagnostic['attempt_queue_peak_frames'], 0)
        self.assertFalse(diagnostic['attempt_receive_paused_observed'])
        self.assertIsNone(diagnostic['attempt_event_lag_peak_ms'])
        self.assertEqual(diagnostic['last_failure']['queue_peak_frames'], 80)
        self.assertFalse(diagnostic['last_failure']['compression_negotiated'])

    def test_decode_failure_stage_has_no_payload_text_and_retries_after_600_seconds(self):
        def socket(event, clock):
            class InvalidSocket(OfflineSocket):
                def recv(self, *, timeout):
                    return 'private-secret invalid JSON'
            return InvalidSocket(event)
        clock, stop, connector, streams = self.make(socket, stop_on_wait=1)
        streams._run('public')
        failure = streams.snapshot()['diagnostics']['public']['last_failure']
        self.assertEqual(failure['phase'], 'decode')
        self.assertEqual(failure['kind'], 'payload_decode_error')
        self.assertNotIn('private-secret', json.dumps(failure))
        self.assertEqual(stop.waits, [600])


class SafeConnectionCauseTests(unittest.TestCase):
    def test_int_enum_close_codes_become_json_safe_integers(self):
        class Codes(IntEnum):
            NORMAL = 1000
            INTERNAL = 1011
        class Closed(Exception):
            rcvd = SimpleNamespace(code=Codes.NORMAL, reason='private peer text')
            sent = SimpleNamespace(code=Codes.INTERNAL, reason='private local text')
        result = connection_failure(Closed(), clock=lambda: 123)
        self.assertIs(type(result['received_close_code']), int)
        self.assertEqual(result['sent_close_code'], 1011)
        self.assertNotIn('private', json.dumps(result))

    def test_transport_and_parser_causes_are_captured_even_when_outer_cause_is_none(self):
        class Closed(Exception):
            def __str__(self):
                raise AssertionError('exception formatting must not be used')
        error = ssl.SSLEOFError(8, 'private://proxy password')
        error.winerror = 10054
        socket = SimpleNamespace(recv_exc=error,
            protocol=SimpleNamespace(parser_exc=EOFError('private parser detail')))
        result = connection_failure(Closed(), socket=socket, clock=lambda: 123)
        self.assertEqual(result['kind'], 'tls_eof')
        transport = next(item for item in result['exception_chain'] if item['source'] == 'recv_exc')
        self.assertEqual(transport['errno'], 8)
        self.assertEqual(transport['winerror'], 10054)
        self.assertNotIn('private', json.dumps(result))

    def test_eof_without_close_frame_is_distinguished_from_ping_timeout(self):
        socket = SimpleNamespace(recv_exc=None, protocol=SimpleNamespace(parser_exc=EOFError()))
        result = connection_failure(Exception(), socket=socket, clock=lambda: 123)
        self.assertEqual(result['kind'], 'transport_eof')
        self.assertIsNone(result['close_code'])
        self.assertFalse(result['received_close_frame'])
        self.assertFalse(result['sent_close_frame'])

    def test_exception_chain_is_bounded_and_cycles_do_not_format_text(self):
        errors = [OSError('private text') for _ in range(8)]
        for first, second in zip(errors, errors[1:]):
            first.__cause__ = second
        result = connection_failure(errors[0], clock=lambda: 123)
        self.assertEqual(len(result['exception_chain']), 4)
        errors[1].__cause__ = errors[0]
        result = connection_failure(errors[0], clock=lambda: 123)
        self.assertEqual(len(result['exception_chain']), 2)
        self.assertNotIn('private', json.dumps(result))

    def test_arbitrary_close_text_does_not_impersonate_library_ping_timeout(self):
        class Closed(Exception):
            sent = SimpleNamespace(code=1011, reason='keepalive ping timeout private-secret')
        result = connection_failure(Closed('private-secret'), clock=lambda: 123)
        self.assertEqual(result['kind'], 'connection_closed')
        self.assertNotIn('private-secret', json.dumps(result))


if __name__ == '__main__':
    unittest.main()
