"""An old reference must not acquire a new three-second life before entry."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.binance import ApiError, BinanceClient
from execution.streams import MarketStreams
from test_hourly_binance import Transport
from test_hourly_calibrated_stream_clock import BASE, VirtualClock, depth


class EntryQuoteProof(unittest.TestCase):
    def setUp(self):
        self.clock = VirtualClock()
        self.streams = MarketStreams(clock=self.clock.wall, monotonic=self.clock.monotonic,
            exchange_clock_ms=self.clock.exchange)

    def proof(self, age=0):
        self.streams.ingest(depth(BASE - age))
        return self.streams.entry_book_observation('BTCUSDT')

    def client(self, *responses, streams=None):
        wire = Transport(*responses)
        client = BinanceClient('offline', 'offline', transport=wire, streams=streams,
            clock=self.clock.wall, monotonic=self.clock.monotonic, auto_sync_time=False)
        client._calibrated_time_snapshot = (BASE, self.clock.monotonic(), 0)
        return client, wire

    def test_old_cached_reference_uses_remaining_lifetime(self):
        book, proof = self.proof(2900)
        self.assertEqual(book['bidPrice'], '100')
        self.assertTrue(proof())
        self.clock.advance(101)
        self.assertFalse(proof())

    def test_cached_reference_valid_just_before_its_original_deadline(self):
        _, proof = self.proof(2900)
        self.clock.advance(99)
        self.assertTrue(proof())

    def test_receive_age_also_limits_reference(self):
        _, proof = self.proof()
        self.clock.advance(3000, move_bounds=False)
        self.assertFalse(proof())

    def test_later_depth_event_never_refreshes_original_reference(self):
        _, proof = self.proof(2900)
        self.clock.advance(200)
        self.streams.ingest(depth(BASE + 200, bid=102, ask=103))
        self.assertFalse(proof())
        self.assertTrue(self.streams.entry_book_observation('BTCUSDT')[1]())

    def test_backward_calibration_does_not_make_old_quote_new(self):
        _, proof = self.proof(2900)
        self.clock.advance(200, move_bounds=False)
        self.clock.bounds = (BASE - 1000, BASE - 1000)
        self.assertFalse(proof())

    def test_forward_calibration_can_expire_reference(self):
        _, proof = self.proof()
        self.clock.bounds = (BASE + 4000, BASE + 4000)
        self.assertFalse(proof())

    def test_monotonic_epoch_reversal_rejects_reference(self):
        self.clock.advance(1000)
        _, proof = self.proof(0)
        self.clock.mono_ms -= 500
        self.assertFalse(proof())

    def test_client_stream_reference_performs_no_rest_request(self):
        self.proof(2900)
        client, wire = self.client(streams=self.streams)
        _, proof = client.entry_book_ticker('BTCUSDT')
        self.assertEqual(wire.requests, [])
        self.clock.advance(200)
        self.assertFalse(proof())

    def test_rest_time_is_the_original_reference_age(self):
        client, wire = self.client({'symbol': 'BTCUSDT', 'bidPrice': '100', 'askPrice': '101', 'time': BASE - 2900})
        _, proof = client.entry_book_ticker('BTCUSDT')
        self.assertTrue(proof())
        self.clock.advance(200)
        self.assertFalse(proof())
        self.assertEqual(wire.paths(), [('GET', '/fapi/v1/ticker/bookTicker')])

    def test_rest_missing_invalid_or_old_time_cannot_authorize_entry(self):
        for stamp in (None, True, 'bad', BASE - 3000, BASE + 4000):
            with self.subTest(stamp=stamp):
                client, _ = self.client({'symbol': 'BTCUSDT', 'time': stamp})
                self.assertFalse(client.entry_book_ticker('BTCUSDT')[1]())

    def test_uncalibrated_rest_reference_is_not_an_entry_permission(self):
        client, _ = self.client({'symbol': 'BTCUSDT', 'time': BASE})
        client._calibrated_time_snapshot = None
        self.assertFalse(client.entry_book_ticker('BTCUSDT')[1]())

    def test_engine_checks_original_proof_before_atomic_intent(self):
        from test_stage3_entry_candidates import Stage3EntryCandidateScenarios
        fixture = Stage3EntryCandidateScenarios()
        fixture.setUp()
        try:
            with patch.object(fixture.client, 'entry_book_ticker', create=True,
                    return_value=({'bidPrice': '100', 'askPrice': '100'}, lambda: False)):
                with self.assertRaises(ApiError):
                    fixture.engine._entry('BTCUSDT', fixture.signal(), True)
            self.assertEqual(fixture.candidate()['status'], 'precheck')
            self.assertEqual(fixture.store.orders(), [])
            self.assertEqual(fixture.store.trades(), [])
            self.assertEqual(fixture.entry_writes(), [])
        finally:
            fixture.doCleanups()
