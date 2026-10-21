"""Rate limits, streamed prices and request cadence: offline-only fixtures."""
import asyncio
import copy
import io
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade'), str(ROOT), str(ROOT / 'tests')]
from execution.request_budget import RequestBudget, RateLimited, request_weight
from execution.streams import MarketStreams
from execution.binance import BinanceClient, ApiError
from execution.engine import Engine, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from execution.strategy import HOUR_MS
from test_hourly_engine import FakeExchange, raw_candle, trade_row
from test_hourly_binance import Transport, Response
from control import compat, hourly_app as console


class RequestBudgetTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.path = Path(self.folder.name) / 'budget.sqlite'
        self.now = 6000.0
        self.budget = RequestBudget(self.path, clock=lambda: self.now, read_cap=5, critical_cap=8)

    def test_constructor_inert_and_weights_distinguish_all_account(self):
        self.assertFalse(self.path.exists())
        self.assertEqual(request_weight('GET', '/fapi/v1/openOrders'), 40)
        self.assertEqual(request_weight('GET', '/fapi/v1/openOrders', {'symbol': 'BTCUSDT'}), 1)
        self.assertEqual(request_weight('GET', '/fapi/v1/klines', {'limit': 1500}), 10)
        self.assertEqual(request_weight('GET', '/fapi/v1/klines', {'limit': 3}), 1)

    def test_configuration_reads_reserve_full_exchange_weight(self):
        self.assertEqual(request_weight('GET', '/fapi/v1/commissionRate', {'symbol': 'BTCUSDT'}), 20)
        self.assertEqual(request_weight('GET', '/fapi/v1/symbolConfig', {'symbol': 'BTCUSDT'}), 5)
        self.budget.read_cap = 24
        self.budget.reserve('GET', '/fapi/v1/commissionRate', {'symbol': 'BTCUSDT'})
        with self.assertRaises(RateLimited):
            self.budget.reserve('GET', '/fapi/v1/symbolConfig', {'symbol': 'BTCUSDT'})
        self.assertEqual(self.budget.snapshot()['used_weight'], 20)

    def test_two_instances_share_limit_and_next_minute_resets(self):
        second = RequestBudget(self.path, clock=lambda: self.now, read_cap=5, critical_cap=8)
        for _ in range(5):
            second.reserve('GET', '/fapi/v1/time')
        with self.assertRaises(RateLimited):
            self.budget.reserve('GET', '/fapi/v1/time')
        self.now += 60
        self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual(second.snapshot()['used_weight'], 1)

    def test_critical_capacity_is_reserved_but_does_not_bypass_ban(self):
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '5'}, request_minute=int(self.now // 60))
        self.budget.reserve('POST', '/fapi/v1/order', critical=True)
        self.budget.observe({'Retry-After': '180'}, status=429, code=-1003)
        with self.assertRaises(RateLimited):
            self.budget.reserve('POST', '/fapi/v1/order', critical=True)
        self.assertEqual(self.budget.snapshot()['cooldown_seconds'], 181)

    def test_418_banned_until_and_restart_preserves_cooldown(self):
        until = self.now + 400
        self.budget.observe({}, status=418, code=-1003, message=f'IP banned until {int(until * 1000):013d}')
        reboot = RequestBudget(self.path, clock=lambda: self.now)
        self.assertGreaterEqual(reboot.snapshot()['cooldown_seconds'], 400)
        self.now = until + 1
        reboot.reserve('GET', '/fapi/v1/time')

    def test_ip_header_used_weight_can_exceed_own_local_requests(self):
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '7'}, request_minute=int(self.now // 60))
        with self.assertRaises(RateLimited):
            self.budget.reserve('GET', '/fapi/v1/time')
        self.assertEqual(self.budget.snapshot()['used_weight'], 7)

    def test_late_old_minute_header_does_not_replace_current_usage(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'Date': 'Thu, 01 Jan 1970 01:39:59 GMT', 'X-MBX-USED-WEIGHT-1M': '2000'})
        self.assertEqual(self.budget.snapshot()['used_weight'], 1)

    def test_rate_response_never_retries_and_next_client_no_wire(self):
        error = HTTPError('https://fake.invalid', 429, 'limited', {'Retry-After': '120'},
            io.BytesIO(json.dumps({'code': -1003, 'msg': 'Too many requests'}).encode()))
        transport = Transport(error, {'unexpected': True})
        client = BinanceClient('fake', 'fake', transport, budget=self.budget, clock=lambda: self.now,
            sleep=lambda _: self.fail('Rate response must not short-sleep/retry'))
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        self.assertEqual(len(transport.requests), 1)

    def test_html_429_also_persists_cooldown(self):
        error = HTTPError('https://fake.invalid', 429, 'limited', {}, io.BytesIO(b'<html>rate limit</html>'))
        client = BinanceClient('fake', 'fake', Transport(error), budget=self.budget, clock=lambda: self.now,
            sleep=lambda _: self.fail('must not retry'))
        with self.assertRaises(ApiError):
            client.public('GET', '/fapi/v1/time')
        self.assertGreater(self.budget.snapshot()['cooldown_seconds'], 0)

    def test_preflight_write_rejected_before_any_transport(self):
        self.budget.observe({}, status=429)
        transport = Transport()
        client = BinanceClient('fake', 'fake', transport, budget=self.budget, clock=lambda: self.now)
        with self.assertRaises(ApiError) as caught:
            client.market_order('BTCUSDT', 'BUY', '.001', 'offline-only')
        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(transport.requests, [])


class StreamCacheTests(unittest.TestCase):
    def setUp(self):
        self.now = 7200.001
        self.streams = MarketStreams(clock=lambda: self.now, connect=Mock(side_effect=AssertionError('Offline test')))

    def push(self, kind, **values):
        return self.streams.ingest({'stream': 'fixture', 'data': {'e': kind, 'E': int(self.now * 1000),
            's': 'BTCUSDT', 'st': 1, **values}})

    def test_public_prices_use_push_and_zero_rest_when_fresh(self):
        self.assertTrue(self.push('bookTicker', b='100', a='101'))
        self.assertTrue(self.push('aggTrade', p='100.5'))
        self.assertTrue(self.push('markPriceUpdate', p='100.6'))
        transport = Transport()
        client = BinanceClient('', '', transport=transport, streams=self.streams)
        self.assertEqual(client.book_ticker('BTCUSDT')['bidPrice'], '100')
        self.assertEqual(client.contract_price('BTCUSDT')['price'], '100.5')
        self.assertEqual(client.mark_price('BTCUSDT')['markPrice'], '100.6')
        self.assertEqual(transport.requests, [])
        self.assertEqual(self.streams.threads, [])

    def test_expired_quote_falls_back_to_one_rest(self):
        self.push('aggTrade', p='100')
        self.now += 4
        transport = Transport({'price': '99'})
        client = BinanceClient('', '', transport=transport, streams=self.streams)
        self.assertEqual(client.contract_price('BTCUSDT')['price'], '99')
        self.assertEqual(len(transport.requests), 1)

    def test_unrelated_stale_cm_or_invalid_quote_is_rejected(self):
        self.assertFalse(self.push('bookTicker', b='101', a='100'))
        self.assertFalse(self.push('aggTrade', p='NaN'))
        for change in ({'s': 'SOLUSDT'}, {'st': 2}, {'E': int((self.now - 6) * 1000)}):
            self.assertFalse(self.streams.ingest({'e': 'aggTrade', 'E': int(self.now * 1000),
                's': 'BTCUSDT', 'p': '100', **change}))

    def test_only_complete_hour_accepts_and_missing_gap_returns_no_sequence(self):
        k = {'t': HOUR_MS, 'T': 2 * HOUR_MS - 1, 'i': '1h', 's': 'BTCUSDT',
            'o': '100', 'h': '102', 'l': '99', 'c': '101', 'x': False}
        self.assertFalse(self.push('kline', k=k))
        k['x'] = True
        self.assertTrue(self.push('kline', k=k))
        self.assertEqual(len(self.streams.closed_since('BTCUSDT', HOUR_MS, 2 * HOUR_MS)), 1)
        self.assertEqual(self.streams.closed_since('BTCUSDT', 0, 2 * HOUR_MS), [])
        k['c'] = '101.5'
        self.assertFalse(self.push('kline', k=k))


class EngineCadenceTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = FakeExchange()
        self.store = Store(Path(self.folder.name) / 'offline.sqlite')
        self.store.set_meta(LEDGER_META, {symbol: {
            'strategy': STRATEGY, 'version': STRATEGY_VERSION,
            'epoch': 'fixture-ledger-' + symbol, 'initialized_at_ms': 0,
            'activation_boundary_ms': 0, 'initial_wallet': '100', 'wallet': '100'}
            for symbol in ('BTCUSDT', 'ETHUSDT')})
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            self.client.candle_rows[symbol] = [raw_candle(i) for i in range(1001)]
        self.engine = Engine(self.client, self.store)

    def count(self, name):
        return sum(c[0] == name for c in self.client.calls)

    def test_fifteen_two_second_ticks_empty_stopped_full_read_only_once(self):
        self.engine.tick()
        for _ in range(14):
            self.client.now += 2000
            self.engine.tick()
        self.assertEqual(self.count('account'), 1)
        self.assertEqual(self.count('open_orders'), 1)
        self.assertEqual(self.count('positions'), 1)
        self.assertEqual(self.client.writes(), [])
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(self.count('account'), 2)

    def test_hour_boundary_forces_fresh_once_even_stopped(self):
        self.engine.tick()
        self.client.now = 1002 * HOUR_MS + 2000
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            self.client.candle_rows[symbol].append(raw_candle(1001))
        self.engine.tick()
        count = self.count('account')
        rules = self.count('exchange_info')
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(self.count('account'), count)
        self.assertEqual(self.count('exchange_info'), rules)
        self.assertEqual(self.client.writes(), [])

    def test_stop_state_transition_forces_immediate_full_reconciliation(self):
        trade = trade_row(status='closed', quantity='0')
        self.store.save_trade(trade)
        self.assertTrue(self.engine._current_trade(trade))
        raw = self.client.stop_row(500, 'offline-stop', 98)
        self.client.algo_rows[500] = raw
        self.store.save_order({'id': 'offline-stop', 'client_id': 'offline-stop', 'kind': 'stop',
            'purpose': 'protection', 'trade_id': trade['id'], 'symbol': 'BTCUSDT',
            'order_id': 500, 'state': 'confirmed', 'raw': copy.deepcopy(raw)})
        self.engine.tick()
        before = self.count('account')
        queried_before = self.count('query_algo')
        self.assertGreater(queried_before, 0)
        self.client.algo_rows[500]['algoStatus'] = 'CANCELED'
        self.client.now += 2000
        self.engine.tick()
        self.assertEqual(self.count('account'), before + 1)
        self.assertEqual(self.count('query_algo'), queried_before + 1)
        self.assertEqual(self.store.order('offline-stop')['state'], 'terminal')
        self.assertEqual(self.client.writes(), [])


class AccountMetadataCacheTests(unittest.TestCase):
    def test_metadata_cached_dynamic_and_new_human_client_always_fresh(self):
        fake = Mock()
        fake.signed.return_value = {'fixture': True}
        with patch.object(compat, 'load_credentials', return_value=('fake-key', 'fake-secret')), \
                patch('execution.binance.BinanceClient', return_value=fake):
            client = compat.AccountClient()
            for _ in range(2):
                client.get('/fapi/v1/positionSide/dual')
                client.get('/fapi/v3/account')
            self.assertEqual(fake.signed.call_count, 3)
            compat.AccountClient().get('/fapi/v1/positionSide/dual')
            self.assertEqual(fake.signed.call_count, 4)

    def test_metadata_error_is_not_cached_or_returned_as_success(self):
        fake = Mock()
        fake.signed.side_effect = ApiError(-1003, 'fixture rate rejection')
        with patch.object(compat, 'load_credentials', return_value=('fake-key', 'fake-secret')), \
                patch('execution.binance.BinanceClient', return_value=fake):
            client = compat.AccountClient()
            for _ in range(2):
                with self.assertRaises(ApiError):
                    client.get('/fapi/v1/accountConfig')
            self.assertEqual(fake.signed.call_count, 2)


class ControllerPollingTests(unittest.TestCase):
    def run_monitor(self, connected, *, market_error=None):
        count = 0
        async def stop_after_three(_interval):
            nonlocal count
            self.assertEqual(_interval, 5)
            count += 1
            if count >= 3:
                raise asyncio.CancelledError()
        refresh = Mock(side_effect=lambda _: console.STATE.update(worker={'connected': connected}))
        preview = Mock(side_effect=market_error, return_value={'ready': True, 'price': 100})
        with patch.object(console.compat, 'AccountClient', return_value=object()), \
                patch.object(console, 'refresh_monitor_private', refresh), patch.object(console, 'market_snapshot', preview), \
                patch.object(console.asyncio, 'sleep', side_effect=stop_after_three), \
                patch.object(console.time, 'time', return_value=6000):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(console.monitor())
        return refresh, preview

    def test_connected_worker_eliminates_preview_history_requests(self):
        with patch.dict(console.STATE, {'worker': {}, 'markets': {}}, clear=True):
            refresh, preview = self.run_monitor(True)
        self.assertEqual(refresh.call_count, 3)
        preview.assert_not_called()

    def test_disconnected_worker_preview_requested_once_each_hour(self):
        with patch.dict(console.STATE, {'worker': {}, 'markets': {}}, clear=True):
            refresh, preview = self.run_monitor(False)
        self.assertEqual(preview.call_count, 2)

    def test_preview_rate_error_retains_old_data_and_backs_off(self):
        old = {'BTCUSDT': {'price': 99, 'ready': True}, 'ETHUSDT': {'price': 100, 'ready': True}}
        with patch.dict(console.STATE, {'worker': {}, 'markets': copy.deepcopy(old)}, clear=True):
            _, preview = self.run_monitor(False, market_error=ApiError(-1003, 'fixture rate limit'))
            self.assertEqual(console.STATE['markets']['BTCUSDT']['price'], 99)
            self.assertTrue(console.STATE['markets']['BTCUSDT']['stale'])
        self.assertEqual(preview.call_count, 2)

    def test_failed_account_read_retains_display_data_but_not_connected(self):
        with patch.dict(console.STATE, {'account': {'connected': True, 'equity_usdt': 100},
                'worker': {}, 'markets': {}, 'legacy_worker': {}, 'events': []}, clear=True), \
                tempfile.TemporaryDirectory() as folder, patch.object(console, 'DATA', Path(folder)), \
                patch.object(console, 'legacy_worker_snapshot', return_value={'connected': False}), \
                patch.object(console, 'worker_snapshot', return_value={'connected': False, 'state': 'stopped'}), \
                patch.object(console, 'legacy_ledger_snapshot', return_value={'verified': True, 'open_trades': 0}), \
                patch.object(console.compat, 'read_account', side_effect=ApiError(-1003, 'fixture rate limit')):
            console.refresh_private(object())
            self.assertFalse(console.STATE['account']['connected'])
            self.assertTrue(console.STATE['account']['stale'])
            self.assertEqual(console.STATE['account']['equity_usdt'], 100)
            self.assertIn('实盘账户尚未连接', console.STATE['blockers'])

    def test_ip_cooldown_blocks_start_even_if_old_snapshot_connected(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(console, 'DATA', Path(folder)):
            RequestBudget(Path(folder) / 'binance-request-budget.sqlite').observe({}, status=429)
            reasons = console.start_reasons({'connected': True, 'can_trade': True,
                'hedge_mode': False, 'multi_asset': False, 'equity_usdt': 100}, {}, {}, {})
            self.assertTrue(any('限流冷却' in reason for reason in reasons))


if __name__ == '__main__':
    unittest.main()
