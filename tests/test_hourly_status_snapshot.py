"""Nonblocking state reads, coherent publications and honest cached freshness."""
import copy
import threading
import time
import tempfile
from pathlib import Path
import unittest

from fastapi.testclient import TestClient
from hourly_worker import build_app
from execution.status_view import age_status, signal_windows


class SlowEngine:
    def __init__(self):
        self.state = 'running'
        self.desired_running = True
        self.entered, self.release = threading.Event(), threading.Event()
        self.snapshot_calls = 0
        self.snapshot_failure = False
        self.tick_failure = False

    def snapshot(self):
        self.snapshot_calls += 1
        if self.snapshot_failure:
            raise RuntimeError('private-data-must-not-leak')
        return {'state': self.state, 'desired_running': self.desired_running,
                'trades': [{'generation': self.state}], 'server_time_ms': 3610000,
                'server_time_upper_ms': 3611000, 'markets': {'ETHUSDT': {
                    'direction': 1, 'boundary_ms': 3600000, 'signal_valid_now': True}}}

    def tick(self):
        self.state = 'partial-write'
        self.entered.set()
        if not self.release.wait(5):
            raise RuntimeError('test release deadline')
        if self.tick_failure:
            raise RuntimeError('private-loop-data')
        if self.desired_running:
            self.state = 'running'

    def pause(self):
        self.desired_running = False
        self.state = 'paused'
        return {'state': self.state, 'desired_running': False}

    def handle_loop_failure(self, exc):
        self.state = 'recovering'

    def start(self, symbols=None):
        return {'state': self.state}

    def set_symbols(self, symbols):
        return {'state': self.state}

    def exit_all(self):
        return self.pause()


class SnapshotPublicationTests(unittest.TestCase):
    def app(self, engine):
        return build_app(engine, 'offline-user', 'offline-password', tick_interval=1000)

    def test_slow_tick_does_not_block_authenticated_reads_or_expose_partial_state(self):
        engine = SlowEngine()
        with TestClient(self.app(engine)) as client:
            client.auth = ('offline-user', 'offline-password')
            try:
                self.assertTrue(engine.entered.wait(2))
                before = engine.snapshot_calls
                for _ in range(8):
                    started = time.monotonic()
                    response = client.get('/api/status')
                    self.assertLess(time.monotonic() - started, .5)
                    data = response.json()
                    self.assertEqual(data['state'], 'running')
                    self.assertEqual(data['trades'][0]['generation'], 'running')
                    self.assertTrue(data['snapshot_metadata']['execution_busy'])
                self.assertEqual(engine.snapshot_calls, before)
            finally:
                engine.release.set()

    def test_pause_is_only_confirmed_after_slow_tick_completes(self):
        engine = SlowEngine()
        replies = []
        with TestClient(self.app(engine)) as client:
            client.auth = ('offline-user', 'offline-password')
            thread = None
            try:
                self.assertTrue(engine.entered.wait(2))
                thread = threading.Thread(target=lambda: replies.append(client.post('/api/pause', json={'confirmed': True})))
                thread.start()
                time.sleep(.03)
                self.assertEqual(replies, [])
                self.assertTrue(client.get('/api/status').json()['desired_running'])
                engine.release.set()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertFalse(replies[0].json()['desired_running'])
                result = client.get('/api/status').json()
                self.assertEqual(result['state'], 'paused')
                self.assertFalse(result['desired_running'])
                self.assertEqual(result['trades'][0]['generation'], 'paused')
            finally:
                engine.release.set()
                if thread is not None:
                    thread.join(2)

    def test_response_mutation_does_not_change_published_state(self):
        engine = SlowEngine()
        client = TestClient(self.app(engine)); client.auth = ('offline-user', 'offline-password')
        response = client.get('/api/status').json()
        response['trades'][0]['generation'] = 'tampered'
        self.assertEqual(client.get('/api/status').json()['trades'][0]['generation'], 'running')
        self.assertEqual(engine.snapshot_calls, 1)

    def test_initial_publication_failure_is_503_not_invented_empty_account(self):
        engine = SlowEngine(); engine.snapshot_failure = True
        client = TestClient(self.app(engine)); client.auth = ('offline-user', 'offline-password')
        response = client.get('/api/status')
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('trades', response.json())
        self.assertNotIn('private-data', response.text)

    def test_failed_publication_retains_old_generation_with_age_and_error(self):
        engine = SlowEngine()
        client = TestClient(self.app(engine)); client.auth = ('offline-user', 'offline-password')
        engine.snapshot_failure = True
        self.assertEqual(client.post('/api/pause', json={'confirmed': True}).status_code, 200)
        result = client.get('/api/status').json()
        self.assertEqual(result['state'], 'running')
        self.assertTrue(result['snapshot_metadata']['publication_error'])
        self.assertFalse(engine.desired_running)
        self.assertNotIn('private-data', str(result))

    def test_unauthenticated_get_does_not_receive_cached_account(self):
        engine = SlowEngine()
        client = TestClient(self.app(engine))
        self.assertEqual(client.get('/api/status').status_code, 401)

    def test_aging_expires_signal_and_prices_without_clearing_unknown_or_retry(self):
        initial = {'server_time_ms': 3610000, 'server_time_upper_ms': 3611000,
            'snapshot_metadata': {'age_seconds': 2},
            'markets': {'ETHUSDT': {'boundary_ms': 3600000, 'direction': 1, 'signal_valid_now': True}},
            'unknown_orders': [{'id': 'unknown'}], 'account_snapshot': {'age_seconds': 4},
            'exchange_clock': {'sync_age_ms': 5000},
            'network_retry': {'waiting': True, 'retry_in_seconds': 2},
            'market_stream': {'connections': {'market': True}, 'symbols': {'ETHUSDT': {
                'book_live': True, 'trade_live': True, 'mark_live': True}}}}
        original = copy.deepcopy(initial)
        result = age_status(initial, 300)
        self.assertFalse(result['markets']['ETHUSDT']['signal_valid_now'])
        self.assertEqual(result['account_snapshot']['age_seconds'], 304)
        self.assertEqual(result['exchange_clock']['sync_age_ms'], 305000)
        self.assertEqual(result['snapshot_metadata']['age_seconds'], 302)
        self.assertTrue(result['network_retry']['waiting'])
        self.assertEqual(result['network_retry']['retry_in_seconds'], 0)
        self.assertEqual(result['unknown_orders'], [{'id': 'unknown'}])
        self.assertFalse(result['market_stream']['symbols']['ETHUSDT']['trade_live'])
        self.assertEqual(initial, original)

    def test_conservative_exact_window_end_is_closed_and_does_not_mutate(self):
        markets = {'BTCUSDT': {'boundary_ms': 3600000, 'direction': -1, 'signal_valid_now': True}}
        self.assertFalse(signal_windows(markets, 3899999, 3900000)['BTCUSDT']['signal_valid_now'])
        self.assertTrue(markets['BTCUSDT']['signal_valid_now'])
        self.assertFalse(signal_windows(markets, None)['BTCUSDT']['signal_valid_now'])

    def test_real_decimal_indicator_snapshot_and_pause_are_published(self):
        from execution.engine import Engine
        from execution.store import Store
        from test_hourly_engine import FakeExchange, raw_candle
        with tempfile.TemporaryDirectory() as temporary:
            exchange = FakeExchange()
            exchange.candle_rows = {symbol: [raw_candle(i) for i in range(1001)]
                                    for symbol in ('BTCUSDT', 'ETHUSDT')}
            engine = Engine(exchange, Store(Path(temporary) / 'offline.sqlite'))
            engine.refresh_rules()
            engine.refresh_candles()
            engine.state = 'running'
            engine.desired_running = True
            client = TestClient(self.app(engine)); client.auth = ('offline-user', 'offline-password')
            before = client.get('/api/status')
            self.assertEqual(before.status_code, 200)
            self.assertTrue(before.json()['desired_running'])
            self.assertIsInstance(before.json()['markets']['ETHUSDT']['signal']['atr'], (float, int))
            sequence = before.json()['snapshot_metadata']['sequence']
            self.assertEqual(client.post('/api/pause', json={'confirmed': True}).status_code, 200)
            after = client.get('/api/status').json()
            self.assertGreater(after['snapshot_metadata']['sequence'], sequence)
            self.assertFalse(after['desired_running'])
            self.assertEqual(after['state'], 'paused')
            self.assertIsNone(after['snapshot_metadata']['publication_error'])
            self.assertEqual(exchange.writes(), [])
