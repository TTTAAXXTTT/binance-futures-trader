"""Calibrated stream freshness with virtual time and public-only local wires.

No external transport, real ledger, server or stream thread is started. The
worker wiring check replaces every service boundary and uses a temporary book.
"""
import io
import json
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime' / 'packages'), str(ROOT)]

from execution.binance import ApiError, BinanceClient
from execution.streams import HOUR, MarketStreams

BASE = 1800000000000


class VirtualClock:
    def __init__(self, bounds=(BASE, BASE)):
        self.mono_ms = 0
        self.wall_ms = BASE + HOUR
        self.bounds = bounds

    def monotonic(self):
        return self.mono_ms / 1000

    def wall(self):
        return self.wall_ms / 1000

    def exchange(self):
        return self.bounds

    def advance(self, milliseconds, *, move_bounds=True):
        self.mono_ms += milliseconds
        if move_bounds and self.bounds is not None:
            self.bounds = tuple(value + milliseconds for value in self.bounds)


class Response(io.BytesIO):
    status = 200

    def __init__(self, value):
        super().__init__(json.dumps(value).encode())


class PublicTimeWire:
    """Only the public time GET is allowed; there is no real wire fallback."""
    def __init__(self, clock, *samples):
        self.clock, self.samples, self.requests = clock, list(samples), []

    def __call__(self, request, *, timeout):
        if request.get_method() != 'GET' or urlsplit(request.full_url).path != '/fapi/v1/time':
            raise AssertionError('Only an offline public time GET is allowed')
        if request.get_header('X-mbx-apikey') is not None or 'signature=' in request.full_url:
            raise AssertionError('The local time fixture must remain public')
        self.requests.append(request)
        if not self.samples:
            raise AssertionError('Unexpected additional local time request')
        value, rtt_ms = self.samples.pop(0)
        self.clock.advance(rtt_ms, move_bounds=False)
        return value if isinstance(value, Response) else Response(value)


def depth(event_ms, symbol='BTCUSDT', *, bid=100, ask=101):
    return {'stream': symbol.lower() + '@depth5@500ms', 'data': {
        'e': 'depthUpdate', 's': symbol, 'st': 1, 'E': event_ms,
        'b': [[str(bid - index), '1'] for index in range(5)],
        'a': [[str(ask + index), '2'] for index in range(5)]}}


def closed_hour(event_ms, opened=HOUR, *, complete=True):
    return {'data': {'e': 'kline', 's': 'BTCUSDT', 'st': 1, 'E': event_ms,
        'k': {'t': opened, 'T': opened + HOUR - 1, 'i': '1h', 's': 'BTCUSDT',
              'o': '100', 'h': '102', 'l': '99', 'c': '101', 'x': complete}}}


class CalibratedStreamClockTests(unittest.TestCase):
    def make_streams(self, clock):
        connector = Mock(side_effect=AssertionError('External sockets are forbidden'))
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic,
            exchange_clock_ms=clock.exchange, connect=connector)
        self.addCleanup(connector.assert_not_called)
        return streams

    def test_first_calibration_does_not_promote_unverified_messages(self):
        clock = VirtualClock(None)
        streams = self.make_streams(clock)
        self.assertFalse(streams.ingest(depth(BASE)))
        self.assertFalse(streams.ingest(closed_hour(BASE)))
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertEqual(streams.closed_since('BTCUSDT', HOUR, 2 * HOUR), [])
        self.assertFalse(streams.snapshot()['event_clock_calibrated'])

        # The first trustworthy clock is an hour behind the host's wall clock.
        clock.bounds = (BASE + 6000, BASE + 6047)
        self.assertFalse(streams.ingest(depth(BASE)))
        self.assertTrue(streams.ingest(depth(BASE + 6000)))
        self.assertEqual(streams.latest('book', 'BTCUSDT')['bidPrice'], '100')
        self.assertTrue(streams.snapshot()['event_clock_calibrated'])

    def test_wall_clock_jumps_do_not_change_freshness(self):
        clock = VirtualClock((BASE, BASE + 47))
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE)))
        for jump in (-HOUR, 2 * HOUR, -3 * HOUR):
            clock.wall_ms += jump
            self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))
        clock.advance(3001)
        clock.wall_ms -= 24 * HOUR
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertFalse(streams.ingest(depth(BASE)))

    def test_monotonic_ttl_and_signed_future_age_keep_exact_three_second_boundary(self):
        clock = VirtualClock()
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE + 1000)))
        # Hold the raw exchange clock still to distinguish receipt TTL from age.
        clock.advance(3000, move_bounds=False)
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))
        clock.advance(1, move_bounds=False)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))

    def test_depth_event_age_preserves_three_second_ingest_boundary(self):
        for offset, accepted in ((-3000, True), (-3001, False), (3000, True), (3001, False)):
            with self.subTest(offset=offset):
                clock = VirtualClock()
                streams = self.make_streams(clock)
                self.assertEqual(streams.ingest(depth(BASE + offset)), accepted)
                self.assertEqual(streams.latest('book', 'BTCUSDT') is not None, accepted)

    def test_general_market_events_keep_five_second_ingest_and_three_second_use(self):
        for kind, target, values in (
                ('bookTicker', 'book', {'b': '100', 'a': '101'}),
                ('aggTrade', 'trade', {'p': '100'}),
                ('markPriceUpdate', 'mark', {'p': '100'})):
            for offset, accepted in ((-5000, True), (-5001, False), (5000, True), (5001, False)):
                with self.subTest(kind=kind, offset=offset):
                    clock = VirtualClock()
                    streams = self.make_streams(clock)
                    message = {'e': kind, 's': 'BTCUSDT', 'E': BASE + offset, **values}
                    self.assertEqual(streams.ingest(message), accepted)
                    self.assertIsNone(streams.latest(target, 'BTCUSDT'))

    def test_large_rtt_does_not_pin_new_quotes_after_a_better_calibration(self):
        clock = VirtualClock((BASE, BASE + 12000))
        streams = self.make_streams(clock)
        self.assertFalse(streams.ingest(depth(BASE)))
        # Most of the first RTT may precede the generation of serverTime.
        clock.advance(1000, move_bounds=False)
        clock.bounds = (BASE + 1000, BASE + 1047)
        self.assertTrue(streams.ingest(depth(BASE + 1000)))
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))

    def test_upper_bound_cannot_hide_a_future_event(self):
        clock = VirtualClock((BASE, BASE + 12000))
        streams = self.make_streams(clock)
        self.assertFalse(streams.ingest(depth(BASE + 12000)))
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))

    def test_received_age_proof_survives_rtt_improvement_but_new_event_recovers(self):
        clock = VirtualClock((BASE, BASE + 2500))
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE)))
        clock.advance(600, move_bounds=False)
        clock.bounds = (BASE + 600, BASE + 647)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertFalse(streams.ingest(depth(BASE)))
        self.assertTrue(streams.ingest(depth(BASE + 600)))
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))

    def test_backward_calibration_cannot_make_old_queued_event_fresh(self):
        clock = VirtualClock((BASE, BASE + 47))
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE)))
        clock.advance(4000, move_bounds=False)
        clock.bounds = (BASE + 1000, BASE + 1047)
        self.assertFalse(streams.ingest(depth(BASE)))
        self.assertFalse(streams.ingest(depth(BASE + 999)))
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        # Its projected age is exactly three seconds, so the original limit holds.
        self.assertTrue(streams.ingest(depth(BASE + 1000)))
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))

    def test_equal_event_can_replace_price_without_renewing_receipt_ttl(self):
        clock = VirtualClock()
        streams = self.make_streams(clock)
        event = BASE + 1000
        self.assertTrue(streams.ingest(depth(event)))
        clock.advance(2500)
        self.assertTrue(streams.ingest(depth(event, bid=99, ask=100)))
        self.assertEqual(streams.latest('book', 'BTCUSDT')['bidPrice'], '99')
        clock.advance(501)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))

    def test_equal_event_cannot_renew_received_age_when_rtt_improves(self):
        clock = VirtualClock((BASE, BASE + 2500))
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE)))
        clock.advance(500, move_bounds=False)
        clock.bounds = (BASE + 500, BASE + 547)
        self.assertTrue(streams.ingest(depth(BASE, bid=99, ask=100)))
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))
        clock.advance(1)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))

    def test_duplicate_detects_expiry_without_a_prior_latest_call(self):
        clock = VirtualClock()
        streams = self.make_streams(clock)
        event = BASE + 1000
        self.assertTrue(streams.ingest(depth(event)))
        clock.advance(3001)
        self.assertFalse(streams.ingest(depth(event)))
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertFalse(streams.ingest(depth(event)))

    def test_eviction_watermark_rejects_same_or_older_event_and_is_per_symbol(self):
        clock = VirtualClock()
        streams = self.make_streams(clock)
        event = BASE + 1000
        self.assertTrue(streams.ingest(depth(event)))
        clock.advance(3001)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        # All three events are otherwise within the current three-second window.
        self.assertFalse(streams.ingest(depth(event)))
        self.assertFalse(streams.ingest(depth(event - 1)))
        self.assertTrue(streams.ingest(depth(event, 'ETHUSDT')))
        self.assertTrue(streams.ingest(depth(BASE + 3001)))
        self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))

    def test_temporary_clock_unavailability_does_not_rejuvenate_cached_item(self):
        clock = VirtualClock()
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(BASE)))
        clock.bounds = None
        clock.advance(4000, move_bounds=False)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertFalse(streams.ingest(depth(BASE + 4000)))
        clock.bounds = (BASE + 1000, BASE + 1047)
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertFalse(streams.ingest(depth(BASE)))

    def test_invalid_clock_samples_are_not_replaced_with_wall_clock(self):
        for bounds in (None, (BASE,), (BASE, BASE - 1), [BASE, BASE], (True, BASE), ('bad', BASE)):
            with self.subTest(bounds=bounds):
                clock = VirtualClock(bounds)
                streams = self.make_streams(clock)
                self.assertFalse(streams.ingest(depth(clock.wall_ms)))
                self.assertIsNone(streams.latest('book', 'BTCUSDT'))
                clock.bounds = (BASE, BASE)
                self.assertTrue(streams.ingest(depth(BASE)))

    def test_hour_close_uses_raw_lower_bound_and_remains_strict(self):
        closed = 2 * HOUR - 1
        clock = VirtualClock((closed + 1000, closed + 1000))
        streams = self.make_streams(clock)
        self.assertTrue(streams.ingest(depth(closed + 1000)))
        # The projected floor and upper bound are already beyond the hour.
        clock.bounds = (closed, closed + 2000)
        self.assertFalse(streams.ingest(closed_hour(closed + 2000)))
        clock.bounds = (closed, closed)
        self.assertFalse(streams.ingest(closed_hour(closed)))
        clock.bounds = (closed + 1, closed + 1)
        self.assertFalse(streams.ingest(closed_hour(closed + 1, complete=False)))
        self.assertTrue(streams.ingest(closed_hour(closed + 1)))
        self.assertEqual(len(streams.closed_since('BTCUSDT', HOUR, 2 * HOUR)), 1)

    def test_legacy_clock_only_injection_stays_inert_and_usable(self):
        clock = VirtualClock()
        callback = Mock(side_effect=AssertionError('Construction must not read exchange time'))
        explicit = MarketStreams(exchange_clock_ms=callback)
        self.assertEqual(explicit.threads, [])
        callback.assert_not_called()
        legacy = MarketStreams(clock=clock.wall, monotonic=clock.monotonic)
        self.assertTrue(legacy.ingest(depth(clock.wall_ms)))
        self.assertIsNotNone(legacy.latest('book', 'BTCUSDT'))
        self.assertIsNone(legacy.snapshot()['event_clock_calibrated'])


class BinanceCalibratedBoundsTests(unittest.TestCase):
    def make_client(self, clock, wire):
        return BinanceClient('offline-public-key', 'offline-secret', transport=wire,
            clock=clock.wall, monotonic=clock.monotonic,
            sleep=lambda _: self.fail('An offline time sample must not sleep'))

    def test_first_sync_is_explicit_and_bounds_ignore_host_clock_jumps(self):
        clock = VirtualClock(None)
        wire = PublicTimeWire(clock, ({'serverTime': BASE}, 47))
        client = self.make_client(clock, wire)
        self.assertIsNone(client.calibrated_time_bounds_ms())
        streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic,
            exchange_clock_ms=client.calibrated_time_bounds_ms)
        self.assertFalse(streams.ingest(depth(BASE)))
        self.assertIsNone(streams.latest('book', 'BTCUSDT'))
        self.assertEqual(wire.requests, [])

        self.assertEqual(client.sync_time(), BASE)
        self.assertEqual(client.calibrated_time_bounds_ms(), (BASE, BASE + 47))
        clock.wall_ms -= 2 * HOUR
        clock.advance(2000, move_bounds=False)
        with patch.object(client, 'sync_time', side_effect=AssertionError('Read must not sync')):
            self.assertEqual(client.calibrated_time_bounds_ms(), (BASE + 2000, BASE + 2047))
            self.assertTrue(streams.ingest(depth(BASE + 2000)))
            self.assertIsNotNone(streams.latest('book', 'BTCUSDT'))
        self.assertEqual(len(wire.requests), 1)

    def test_failed_resync_preserves_last_valid_bounds(self):
        clock = VirtualClock(None)
        wire = PublicTimeWire(clock, ({'serverTime': BASE}, 47), ({'serverTime': 'bad'}, 12))
        client = self.make_client(clock, wire)
        client.sync_time()
        with self.assertRaises(ApiError):
            client.sync_time()
        self.assertEqual(client.calibrated_time_bounds_ms(), (BASE + 12, BASE + 59))

    def test_monotonic_epoch_backstep_returns_unknown_without_network(self):
        clock = VirtualClock(None)
        wire = PublicTimeWire(clock, ({'serverTime': BASE}, 47))
        client = self.make_client(clock, wire)
        client.sync_time()
        clock.mono_ms = 46
        self.assertIsNone(client.calibrated_time_bounds_ms())
        self.assertEqual(len(wire.requests), 1)

    def test_bounds_reader_does_not_wait_for_a_concurrent_sync(self):
        clock = VirtualClock(None)
        entered, release, finished = threading.Event(), threading.Event(), threading.Event()
        failures, observed = [], []

        class HeldResponse(Response):
            def read(self):
                entered.set()
                if not release.wait(2):
                    raise AssertionError('Offline calibration fixture was not released')
                return super().read()

        wire = PublicTimeWire(clock, ({'serverTime': BASE}, 20),
            (HeldResponse({'serverTime': BASE + 12000}), 12000))
        client = self.make_client(clock, wire)
        client.sync_time()

        def sync():
            try:
                client.sync_time()
            except BaseException as error:
                failures.append(error)

        def read():
            try:
                observed.append(client.calibrated_time_bounds_ms())
            except BaseException as error:
                failures.append(error)
            finally:
                finished.set()

        writer = threading.Thread(target=sync, daemon=True)
        reader = threading.Thread(target=read, daemon=True)
        writer.start()
        try:
            self.assertTrue(entered.wait(1), 'Local sync fixture did not enter its held response')
            reader.start()
            self.assertTrue(finished.wait(0.5), 'Bounds reader waited for the network sync lock')
            self.assertEqual(observed, [(BASE + 12000, BASE + 12020)])
        finally:
            release.set()
            writer.join(1)
            if reader.ident is not None:
                reader.join(1)
        self.assertFalse(writer.is_alive())
        self.assertFalse(reader.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(client.calibrated_time_bounds_ms(), (BASE + 12000, BASE + 24000))


class WorkerCalibratedClockWiringTests(unittest.TestCase):
    def test_production_wiring_binds_memory_clock_without_time_request_or_engine_tick(self):
        import hourly_worker
        from execution.engine import SOFTWARE_RELEASE

        clock = VirtualClock(None)
        wire = PublicTimeWire(clock)
        client = BinanceClient('offline-key', 'offline-secret', transport=wire,
            clock=clock.wall, monotonic=clock.monotonic)
        bounds = Mock(wraps=client.calibrated_time_bounds_ms)
        client.calibrated_time_bounds_ms = bounds
        engine = SimpleNamespace(client=client, tick=Mock(), start=Mock())
        created = []

        def make_streams(**kwargs):
            streams = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, **kwargs)
            created.append(streams)
            return streams

        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            ledger = root / 'user_data' / 'trades-live.sqlite'
            diagnostics_path = root / 'user_data' / 'execution-diagnostics.sqlite'
            ledger.parent.mkdir()
            sentinel = b'offline existing book sentinel'
            ledger.write_bytes(sentinel)
            with patch.object(hourly_worker, 'ROOT', root), \
                    patch.object(hourly_worker, '_load_api_credentials', return_value=('offline-key', 'offline-secret')), \
                    patch('server.network_environment.apply_network_environment'), \
                    patch.dict(os.environ, {'HOURLY_RPC_USER': 'offline-user', 'HOURLY_RPC_PASSWORD': 'offline-password'}), \
                    patch('execution.binance.BinanceClient', return_value=client) as client_factory, \
                    patch('execution.streams.MarketStreams', side_effect=make_streams) as stream_factory, \
                    patch.object(MarketStreams, 'start', autospec=True) as start_streams, \
                    patch.object(MarketStreams, 'stop', autospec=True) as stop_streams, \
                    patch('execution.diagnostics.ExecutionDiagnostics') as diagnostics_factory, \
                    patch('execution.engine.Engine', return_value=engine) as engine_factory, \
                    patch('execution.store.Store') as store_factory, \
                    patch.object(hourly_worker, 'build_app', return_value=object()), \
                    patch('uvicorn.run') as serve, \
                    patch.object(client, 'sync_time', side_effect=AssertionError('Wiring must not calibrate')), \
                    patch.object(client, 'close') as close:
                hourly_worker.main()
                client_factory.assert_called_once_with('offline-key', 'offline-secret')
                stream_factory.assert_called_once_with(exchange_clock_ms=bounds)
                self.assertIs(client.streams, created[0])
                self.assertEqual(created[0].threads, [])
                start_streams.assert_called_once_with(created[0])
                stop_streams.assert_called_once_with(created[0])
                store_factory.assert_called_once_with(ledger)
                diagnostics_factory.assert_called_once_with(diagnostics_path, release=SOFTWARE_RELEASE)
                self.assertIs(client.diagnostics, diagnostics_factory.return_value)
                engine_factory.assert_called_once_with(client, store_factory.return_value,
                    hourly_worker.activation_check, restore_saved_intent=True,
                    diagnostics=diagnostics_factory.return_value)
                self.assertEqual(diagnostics_factory.return_value.method_calls, [])
                self.assertFalse(diagnostics_path.exists())
                bounds.assert_not_called()
                engine.tick.assert_not_called()
                engine.start.assert_not_called()
                close.assert_called_once_with()
                serve.assert_called_once()
                self.assertEqual(wire.requests, [])
            self.assertEqual(ledger.read_bytes(), sentinel)
            self.assertFalse(diagnostics_path.exists())


if __name__ == '__main__':
    unittest.main()
