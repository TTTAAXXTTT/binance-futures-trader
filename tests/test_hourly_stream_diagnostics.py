"""Safe WebSocket causes/receive-pressure evidence without network connections."""
import json
import ssl
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.streams import MarketStreams, connection_failure
from test_hourly_stream_retry import VirtualTime, VirtualStop, Connector, OfflineSocket


class StreamDiagnosticTests(unittest.TestCase):
    def test_ping_timeout_cause_and_local_close_code_are_retained_without_private_text(self):
        class Closed(Exception):
            rcvd = None
            sent = SimpleNamespace(code=1011, reason='keepalive ping timeout')
        result = connection_failure(Closed('private credentials and request URL'), clock=lambda: 123)
        self.assertEqual(result['kind'], 'ping_timeout')
        self.assertEqual(result['close_code'], 1011)
        self.assertEqual(result['sent_close_code'], 1011)
        self.assertIsNone(result['received_close_code'])
        text = json.dumps(result)
        self.assertNotIn('private', text)
        self.assertNotIn('secret', text)
        self.assertNotIn('signature=', text)

    def test_peer_close_is_distinguished_from_local_ping_timeout(self):
        class Closed(Exception):
            rcvd = SimpleNamespace(code=1001, reason='private peer detail')
            sent = SimpleNamespace(code=1000, reason='private local detail')
        result = connection_failure(Closed(), clock=lambda: 123)
        self.assertEqual(result['received_close_code'], 1001)
        self.assertEqual(result['sent_close_code'], 1000)
        self.assertEqual(result['kind'], 'connection_closed')

    def test_failure_is_channel_specific_and_success_preserves_historical_cause(self):
        clock = VirtualTime()
        stop = VirtualStop(clock)
        connector = Connector(clock, ssl.SSLEOFError('private-key@proxy'), OfflineSocket(stop))
        stream = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, stop_event=stop, connect=connector)
        stream._run('public')
        snapshot = stream.snapshot()
        self.assertEqual([call[0] for call in connector.calls], [0, 600])
        self.assertEqual(snapshot['retry']['public']['failures'], 0)
        self.assertEqual(snapshot['diagnostics']['public']['last_failure']['kind'], 'tls_eof')
        self.assertIsNone(snapshot['diagnostics']['market']['last_failure'])
        self.assertEqual(snapshot['diagnostics']['public']['connection_attempts'], 2)
        self.assertEqual(snapshot['diagnostics']['public']['messages_received'], 1)
        self.assertNotIn('private-key', json.dumps(snapshot))

    def test_receive_burst_reports_pressure_and_never_reuses_expired_book_quotes(self):
        clock = VirtualTime()
        stop = VirtualStop(clock)
        class BurstSocket(OfflineSocket):
            def __init__(self):
                super().__init__(stop)
                self.received = 0
                self.recv_messages = SimpleNamespace(frames=SimpleNamespace(qsize=lambda: 256 - self.received), paused=True)
            def recv(self, *, timeout):
                self.received += 1
                clock.now += .05
                self.recv_messages.paused = self.received < 192
                if self.received == 256:
                    stop.set()
                return json.dumps({'data': {'e': 'bookTicker', 's': 'BTCUSDT',
                    'E': 1800000000000, 'b': '100', 'a': '101'}})
        connector = Connector(clock, BurstSocket())
        stream = MarketStreams(clock=clock.wall, monotonic=clock.monotonic, stop_event=stop, connect=connector)
        stream._run('public')
        diagnostics = stream.snapshot()['diagnostics']['public']
        self.assertEqual(diagnostics['messages_received'], 256)
        self.assertGreater(diagnostics['invalid_messages'], 100)
        self.assertEqual(diagnostics['queue_peak_frames'], 255)
        self.assertTrue(diagnostics['receive_paused_observed'])
        self.assertIsNone(stream.latest('book', 'BTCUSDT', max_age=3))
        self.assertEqual(connector.calls[0][2]['max_queue'], 64)
        self.assertEqual(connector.calls[0][2]['ping_interval'], 20)
        self.assertEqual(connector.calls[0][2]['ping_timeout'], 20)

    def test_valid_quote_still_expires_after_three_seconds(self):
        clock = VirtualTime()
        stream = MarketStreams(clock=clock.wall, monotonic=clock.monotonic)
        self.assertTrue(stream.ingest({'e': 'bookTicker', 's': 'ETHUSDT', 'E': 1800000000000, 'b': '99', 'a': '100'}))
        clock.now = 3
        self.assertIsNotNone(stream.latest('book', 'ETHUSDT'))
        clock.now = 3.001
        self.assertIsNone(stream.latest('book', 'ETHUSDT'))


if __name__ == '__main__':
    unittest.main()
