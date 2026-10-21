"""Durable lifecycle checks using only an in-memory fake exchange and temp SQLite."""
import copy
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from execution.binance import ApiError, OrderUncertain
from execution.engine import Engine, ExecutionFault, candles_from, STRATEGY, STRATEGY_VERSION, LEDGER_META
from execution.store import Store
from execution.strategy import Analysis, HOUR_MS, Signal, analyze, trailing_stop


class FakeExchange:
    """Every method is local; all reads and writes appear in calls for assertions."""
    def __init__(self, now=1001 * HOUR_MS + 2000):
        self.now = now
        self.calls = []
        self.position_rows = []
        self.order_rows = {}
        self.algo_rows = {}
        self.fill_rows = {}
        self.candle_rows = {"BTCUSDT": [], "ETHUSDT": []}
        self.next_order = 100
        self.next_algo = 500
        self.market_error = None
        self.stop_error = None
        self.market_hook = None
        self.account_value = dict(totalWalletBalance="1000", totalUnrealizedProfit="0",
                                  availableBalance="1000")

    def _call(self, name, *args, **kwargs):
        self.calls.append((name, args, kwargs))

    def writes(self):
        return [row for row in self.calls if row[0] in {
            "market_order", "new_stop", "cancel_algo", "cancel_order", "set_cross50"}]

    def now_ms(self):
        return self.now

    def sync_time(self):
        self._call("sync_time")
        return self.now

    def account(self):
        self._call("account")
        return copy.deepcopy(self.account_value)

    def positions(self):
        self._call("positions")
        return [dict(copy.deepcopy(row), unRealizedProfit=row.get('unRealizedProfit', '0'))
                for row in self.position_rows]

    def open_orders(self):
        self._call("open_orders")
        return [copy.deepcopy(row) for row in self.order_rows.values()
                if row.get("status") in {"NEW", "PARTIALLY_FILLED"}]

    def open_algos(self):
        self._call("open_algos")
        return [copy.deepcopy(row) for row in self.algo_rows.values()
                if row.get("algoStatus") in {"NEW", "TRIGGERED"}]

    def query_order(self, symbol, client_id=None, order_id=None):
        self._call("query_order", symbol, client_id=client_id, order_id=order_id)
        for row in self.order_rows.values():
            if (client_id and row.get("clientOrderId") == client_id) or (
                    order_id is not None and row["orderId"] == order_id):
                return copy.deepcopy(row)
        raise ApiError(-2013, "Order does not exist")

    def fills_all(self, symbol, order_id=None):
        self._call("fills_all", symbol, order_id=order_id)
        return copy.deepcopy(self.fill_rows.get(order_id, []))

    def query_algo(self, algo_id=None, client_id=None):
        self._call("query_algo", algo_id=algo_id, client_id=client_id)
        for row in self.algo_rows.values():
            if (client_id and row.get("clientAlgoId") == client_id) or (
                    algo_id is not None and row["algoId"] == algo_id):
                return copy.deepcopy(row)
        raise ApiError(-2013, "Algo order does not exist")

    def market_order(self, symbol, side, quantity, client_id, reduce_only=False):
        self._call("market_order", symbol, side, quantity, client_id, reduce_only=reduce_only)
        if self.market_error:
            raise self.market_error
        identifier = self.next_order
        self.next_order += 1
        row = dict(symbol=symbol, orderId=identifier, clientOrderId=client_id,
                   status="NEW", executedQty="0", origQty=str(quantity))
        self.order_rows[identifier] = row
        if self.market_hook:
            self.market_hook(row, symbol, side, quantity, reduce_only)
        return copy.deepcopy(row)

    def new_stop(self, symbol, side, trigger, client_id):
        self._call("new_stop", symbol, side, trigger, client_id)
        if self.stop_error:
            raise self.stop_error
        identifier = self.next_algo
        self.next_algo += 1
        row = self.stop_row(identifier, client_id, trigger, symbol, side)
        self.algo_rows[identifier] = row
        return copy.deepcopy(row)

    @staticmethod
    def stop_row(identifier, client_id, trigger, symbol="BTCUSDT", side="SELL", status="NEW"):
        return dict(algoId=identifier, clientAlgoId=client_id, symbol=symbol,
                    side=side, algoStatus=status, orderType="STOP_MARKET",
                    triggerPrice=str(trigger), workingType="CONTRACT_PRICE",
                    closePosition=True, priceProtect=False, positionSide="BOTH", actualOrderId="0")

    def cancel_algo(self, algo_id):
        self._call("cancel_algo", algo_id=algo_id)
        self.algo_rows[algo_id]["algoStatus"] = "CANCELED"
        return copy.deepcopy(self.algo_rows[algo_id])

    def cancel_order(self, symbol, order_id):
        self._call("cancel_order", symbol, order_id=order_id)
        self.order_rows[order_id]["status"] = "CANCELED"
        return copy.deepcopy(self.order_rows[order_id])

    def account_config(self):
        self._call("account_config")
        return {"canTrade": True}

    def position_mode(self):
        self._call("position_mode")
        return {"dualSidePosition": False}

    def multi_assets_mode(self):
        self._call("multi_assets_mode")
        return {"multiAssetsMargin": False}

    def symbol_config(self, symbol):
        self._call("symbol_config", symbol)
        return [{"symbol": symbol, "marginType": "CROSSED", "leverage": 50}]

    def set_cross50(self, symbol):
        self._call("set_cross50", symbol)

    def exchange_info(self):
        self._call("exchange_info")
        filters = [dict(filterType="PRICE_FILTER", tickSize=".01", minPrice=".01", maxPrice="1000000"),
                   dict(filterType="LOT_SIZE", minQty=".1", maxQty="1000", stepSize=".1"),
                   dict(filterType="MARKET_LOT_SIZE", minQty=".1", maxQty="1000", stepSize=".1"),
                   dict(filterType="MIN_NOTIONAL", notional="5")]
        return {"symbols": [dict(symbol=symbol, status="TRADING", contractType="PERPETUAL",
                                 quoteAsset="USDT", marginAsset="USDT", filters=copy.deepcopy(filters))
                            for symbol in ("BTCUSDT", "ETHUSDT")]}

    def leverage_brackets(self):
        self._call("leverage_brackets")
        return [dict(symbol=symbol, brackets=[dict(notionalFloor="0", notionalCap="1000000",
                    initialLeverage=50, maintMarginRatio=".004", cum="0")])
                for symbol in ("BTCUSDT", "ETHUSDT")]

    def book_ticker(self, symbol):
        self._call("book_ticker", symbol)
        return {"bidPrice": "100", "askPrice": "100"}

    def contract_price(self, symbol):
        self._call("contract_price", symbol)
        return {"price": "100"}

    def mark_price(self, symbol):
        self._call("mark_price", symbol)
        return {"markPrice": "100"}

    def klines(self, symbol, **kwargs):
        self._call("klines", symbol, **kwargs)
        start = kwargs.get("startTime")
        rows = self.candle_rows[symbol]
        return [row for row in rows if start is None or int(row[0]) >= start][:kwargs.get("limit", 1500)]

    def income(self, **kwargs):
        self._call("income", **kwargs)
        return []


def trade_row(identifier="trade-1", **updates):
    row = dict(id=identifier, symbol="BTCUSDT", direction=1, status="pending", quantity="0",
               average="0", entry_atr="1", equity0="1000", budget0="10", notional0="3000",
               signal_boundary_ms=1001 * HOUR_MS, signal_close="100", actual_cost="0",
               first_fill_ms=None, confirmed_stop=None, exit_intent=None)
    row.update(updates)
    row.setdefault('strategy_id', STRATEGY)
    row.setdefault('strategy_version', STRATEGY_VERSION)
    row.setdefault('ledger_epoch', 'fixture-ledger-' + row['symbol'])
    row.setdefault('branch', 'trend')
    return row


def fill_row(identifier, order_id, quantity, price, timestamp, trade="trade-1", purpose="entry",
             fee=".05", pnl="0"):
    return dict(id=identifier, orderId=order_id, qty=str(quantity), price=str(price),
                time=timestamp, commission=str(fee), commissionAsset="USDT", realizedPnl=str(pnl),
                _trade_id=trade, _purpose=purpose)


def raw_candle(index, high="101", low="99", close="100"):
    return [index * HOUR_MS, close, high, low, close, "1", (index + 1) * HOUR_MS - 1,
            "100", 1, "0", "0", "0"]


class HourlyEngineLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / "offline-only.sqlite3")
        self.client = FakeExchange()
        self.store.set_meta(LEDGER_META, {symbol: {'strategy': STRATEGY, 'version': STRATEGY_VERSION,
            'epoch': 'fixture-ledger-' + symbol, 'initialized_at_ms': 0,
            'activation_boundary_ms': 0, 'initial_wallet': '100', 'wallet': '100'}
            for symbol in ('BTCUSDT', 'ETHUSDT')})
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        selected_at = self.client.now
        self.client.now -= HOUR_MS
        self.engine.set_symbols({"BTCUSDT": True, "ETHUSDT": True})
        self.engine.state = "running"  # Explicit mock human activation in the prior hour.
        self.client.now = selected_at
        self.engine.refresh_rules()
        self.client.calls.clear()

    def register_stop(self, trade, trigger="98", identifier=400, status="NEW"):
        row = self.client.stop_row(identifier, "old-stop-" + str(identifier), trigger, status=status)
        self.client.algo_rows[identifier] = row
        self.store.save_order(dict(id=row["clientAlgoId"], client_id=row["clientAlgoId"],
                                   kind="stop", purpose="protection", trade_id=trade["id"],
                                   symbol=trade["symbol"], order_id=identifier, state="confirmed", raw=row))
        self.engine.algo_map = {(trade["symbol"], str(identifier)): copy.deepcopy(row)}
        return row

    def test_human_start_syncs_before_activation_times_and_clears_resolved_1021_fault(self):
        boundary = 1001 * HOUR_MS
        self.client.now = boundary + HOUR_MS + 2000
        self.engine.state = 'fault'
        self.engine.fault_reason = 'Binance -1021: Timestamp outside recvWindow'
        self.store.set_meta('fault_reason', self.engine.fault_reason)
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        def sync():
            self.client._call('sync_time')
            self.client.now = boundary + 2000
            return self.client.now
        with patch.object(self.client, 'sync_time', side_effect=sync):
            response = self.engine.start({'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(self.client.calls[0][0], 'sync_time')
        self.assertEqual(response['state'], 'running')
        self.assertIsNone(self.engine.fault_reason)
        self.assertIsNone(self.store.meta('fault_reason'))
        self.assertEqual(self.engine.last_boundary, boundary)
        self.assertEqual(self.engine.symbol_enabled_after['BTCUSDT'], boundary)
        self.assertIsNotNone(self.store.signal('BTCUSDT', boundary))
        self.assertFalse(any(row[0] == 'market_order' for row in self.client.calls))

    def test_failed_start_clock_sync_keeps_activation_selection_and_permission_unchanged(self):
        self.engine.state = 'fault'
        selected = self.store.meta('symbol_enabled')
        after = self.store.meta('symbol_enabled_after')
        with patch.object(self.client, 'sync_time', side_effect=ApiError(None, 'offline public time failed')):
            with self.assertRaises(ApiError):
                self.engine.start({'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(self.store.meta('symbol_enabled'), selected)
        self.assertEqual(self.store.meta('symbol_enabled_after'), after)
        self.assertEqual(self.engine.state, 'fault')
        self.assertEqual(self.client.writes(), [])

    def test_tick_ensures_periodic_clock_sync_even_with_rules_already_loaded(self):
        self.engine.state = 'stopped'
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        with patch.object(self.client, 'ensure_time_sync', create=True) as ensure:
            self.engine.tick()
            self.client.now += 2000
            self.engine.tick()
            self.assertEqual(ensure.call_count, 2)
        self.assertFalse(any(row[0] == 'sync_time' for row in self.client.calls))
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.engine.state, 'stopped')

    def confirm_current_hour_for_clock_test(self):
        # A mocked refresh must provide an actually confirmed boundary; warmup
        # alone is no longer evidence that the latest completed hour arrived.
        boundary = self.client.now // HOUR_MS * HOUR_MS
        signal = Signal(boundary - HOUR_MS, boundary, 0, D('1'), D('100'),
                        D('100'), D('101'), D('99'))
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'), signal)
                                for symbol in ('BTCUSDT', 'ETHUSDT')}

    def test_start_near_hour_boundary_never_uses_pre_start_signal_after_slow_return(self):
        boundary = 1001 * HOUR_MS
        self.client.now = boundary - 2000
        self.client.now_upper_ms = lambda: self.client.now + 6000
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        self.store.set_meta(LEDGER_META, {})
        self.engine.start({'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(self.engine.last_boundary, boundary - HOUR_MS)
        self.assertEqual(self.engine.symbol_enabled_after['BTCUSDT'], boundary)
        self.assertEqual(self.store.meta(LEDGER_META)['BTCUSDT']['activation_boundary_ms'], boundary)
        self.assertFalse(any(row[0] == 'market_order' for row in self.client.calls))
        self.client.now = boundary + 2000
        with patch.object(self.engine, 'refresh_candles', side_effect=self.confirm_current_hour_for_clock_test), patch.object(self.engine, '_hourly_exits') as exits:
            self.engine.tick()
        exits.assert_called_once_with(boundary, ['BTCUSDT', 'ETHUSDT'])

    def test_entry_deadline_uses_upper_time_bound_after_slow_return(self):
        boundary = 1001 * HOUR_MS
        self.client.now = boundary + 299000
        self.client.now_upper_ms = lambda: self.client.now + 6000
        self.engine.reconciled = True
        signal = Signal(boundary - HOUR_MS, boundary, 1, D('1'), D('100'), D('99'), D('99'), D('98'))
        self.engine._entry('BTCUSDT', signal, True)
        self.assertIn('窗口', self.store.signal('BTCUSDT', boundary)['skip_reason'])
        self.assertFalse(any(row[0] == 'market_order' for row in self.client.calls))

    def test_persisted_entry_deadline_uses_upper_bound_without_unknown_order(self):
        trade = trade_row()
        self.store.save_trade(trade)
        self.client.now = trade['signal_boundary_ms'] + 299000
        self.client.now_upper_ms = lambda: self.client.now + 6000
        intent = self.engine._submit_market(trade, 'entry', D('1'))
        self.assertEqual(intent['state'], 'terminal')
        self.assertTrue(intent['locally_not_submitted'])
        self.assertTrue(intent['fills_complete'])
        self.assertFalse(any(row[0] in ('market_order', 'query_order') for row in self.client.calls))

    def test_clock_correction_during_account_or_funding_read_never_skips_actual_hour(self):
        boundary = 1001 * HOUR_MS
        for stage in ('account', 'income'):
            for forward in (True, False):
                with self.subTest(stage=stage, forward=forward):
                    self.client.now = boundary + (-2000 if forward else 2000)
                    corrected = boundary + (2000 if forward else -2000)
                    self.engine.state = 'paused'
                    self.engine.last_boundary = boundary - HOUR_MS
                    self.engine._hour_completed = {}
                    self.engine.last_full_reconcile_ms = None
                    self.engine.last_income = 0
                    self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                            for symbol in ('BTCUSDT', 'ETHUSDT')}
                    original = getattr(self.client, stage)
                    def corrected_read(*args, **kwargs):
                        result = original(*args, **kwargs)
                        self.client.now = corrected
                        return result
                    with patch.object(self.client, stage, side_effect=corrected_read), \
                         patch.object(self.engine, 'refresh_candles', side_effect=self.confirm_current_hour_for_clock_test) as refresh, \
                         patch.object(self.engine, '_hourly_exits') as hourly:
                        self.engine.tick()
                        if forward:
                            hourly.assert_called_once_with(boundary, ['BTCUSDT', 'ETHUSDT'])
                            refresh.assert_called_once()
                        else:
                            hourly.assert_not_called()
                            refresh.assert_not_called()
                    self.assertEqual(self.engine.last_boundary, boundary if forward else boundary - HOUR_MS)
                    if not forward:
                        self.client.now = boundary + 2000
                        with patch.object(self.engine, 'refresh_candles', side_effect=self.confirm_current_hour_for_clock_test), \
                             patch.object(self.engine, '_hourly_exits') as hourly:
                            self.engine.tick()
                            hourly.assert_called_once_with(boundary, ['BTCUSDT', 'ETHUSDT'])
                        self.assertEqual(self.engine.last_boundary, boundary)
                    self.assertEqual(self.client.writes(), [])

    def test_rule_refresh_clock_backstep_does_not_consume_future_hour(self):
        boundary = 1001 * HOUR_MS
        self.client.now = boundary + 2000
        self.engine.last_boundary = boundary - HOUR_MS
        self.engine.last_full_reconcile_ms = None
        self.engine.state = 'paused'
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        original = self.client.leverage_brackets
        def corrected_brackets():
            result = original()
            self.client.now = boundary - 2000
            return result
        with patch.object(self.client, 'leverage_brackets', side_effect=corrected_brackets), \
             patch.object(self.engine, 'refresh_candles') as refresh, \
             patch.object(self.engine, '_hourly_exits') as hourly:
            self.engine.tick()
            hourly.assert_not_called()
            refresh.assert_not_called()
        self.assertEqual(self.engine.last_boundary, boundary - HOUR_MS)
        self.assertEqual(self.client.writes(), [])

    def test_reconcile_completion_uses_corrected_clock_on_success_and_failure_and_throttles_failure(self):
        corrected = 1001 * HOUR_MS + 2000
        self.client.now = corrected + HOUR_MS
        original = self.client.account
        def corrected_account():
            result = original()
            self.client.now = corrected
            return result
        with patch.object(self.client, 'account', side_effect=corrected_account):
            self.engine.reconcile()
        self.assertEqual(self.engine.last_full_reconcile_ms, corrected)
        self.client.now = corrected + HOUR_MS
        def rejected_account():
            self.client.now = corrected
            raise ApiError(-1021, 'offline request rejected after correcting clock')
        with patch.object(self.client, 'account', side_effect=rejected_account):
            with self.assertRaises(ApiError):
                self.engine.reconcile()
        self.assertEqual(self.engine.last_full_reconcile_ms, corrected)
        self.assertFalse(self.engine.reconciled)
        self.engine.state = 'fault'
        self.engine.last_boundary = corrected // HOUR_MS * HOUR_MS
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        with patch.object(self.client, 'account', side_effect=ApiError(-1021, 'still rejected')) as account, \
             patch.object(self.engine, '_funding'):
            self.client.now = corrected + 2000
            self.engine.tick()
            account.assert_not_called()
            self.client.now = corrected + 30000
            self.engine.tick()
            account.assert_called_once()
        self.assertEqual(self.engine.last_full_reconcile_ms, corrected + 30000)
        self.assertEqual(self.engine.state, 'fault')
        self.assertEqual(self.client.writes(), [])

    def test_funding_window_never_starts_in_future_or_consumes_a_clock_backstep(self):
        boundary = 1001 * HOUR_MS
        self.client.now = boundary + 2000
        self.engine.started_ms = self.client.now + HOUR_MS
        self.engine.last_income = 0
        self.engine._funding()
        first = [row for row in self.client.calls if row[0] == 'income'][0]
        self.assertEqual(first[2]['startTime'], boundary + 2000 - 24 * HOUR_MS)
        self.assertEqual(first[2]['endTime'], boundary + 2000)
        self.engine.last_income = 0
        self.store.set_meta('funding_cursor', boundary - 60000)
        original = self.client.income
        def corrected_income(**kwargs):
            result = original(**kwargs)
            self.client.now = boundary - 2000
            return result
        with patch.object(self.client, 'income', side_effect=corrected_income):
            self.engine._funding()
        self.assertEqual(self.store.meta('funding_cursor'), boundary - 2000)
        self.assertEqual(self.engine.last_income, boundary - 2000)
        self.assertEqual(self.client.writes(), [])

    def test_failed_funding_has_monotonic_delay_and_keeps_cursor_and_records(self):
        clock = {'value': 100.0}
        self.engine._monotonic = lambda: clock['value']
        self.store.set_meta('funding_cursor', self.client.now - 60000)
        cursor = self.store.meta('funding_cursor')
        with patch.object(self.client, 'income', side_effect=ApiError(None, 'fixture TLS read failed')) as income:
            with self.assertRaises(ApiError):
                self.engine._funding()
            for elapsed in (2, 30, 59):
                clock['value'] = 100 + elapsed
                self.client.now += 10000
                self.engine._funding()
            income.assert_called_once()
            self.assertEqual(self.engine.last_income, 0)
            self.assertEqual(self.store.meta('funding_cursor'), cursor)
            self.assertEqual(self.store.incomes(), [])
            self.assertTrue(self.store.meta('funding_read_pending'))
            clock['value'] = 700
            with self.assertRaises(ApiError):
                self.engine._funding()
            self.assertEqual(income.call_count, 2)
        self.assertEqual(self.client.writes(), [])

    def test_funding_failure_delay_caps_and_success_resets_without_granting_entries(self):
        clock = {'value': 100.0}
        self.engine._monotonic = lambda: clock['value']
        self.engine.state = 'fault'
        self.engine.fault_reason = 'fixture network fault'
        with patch.object(self.client, 'income', side_effect=ApiError(None, 'fixture TLS read failed')) as income:
            for expected in (600, 600, 600, 600, 600):
                with self.assertRaises(ApiError):
                    self.engine._funding()
                before = income.call_count
                self.assertEqual(self.engine.snapshot()['funding_read']['retry_in_seconds'], expected)
                self.engine._funding()
                self.assertEqual(income.call_count, before)
                clock['value'] += expected
        self.engine._funding()
        self.assertEqual(self.engine.income_failures, 0)
        self.assertFalse(self.engine.funding_pending_recovery)
        self.assertFalse(self.store.meta('funding_read_pending'))
        self.assertEqual(self.engine.state, 'fault')
        self.assertEqual(self.engine.fault_reason, 'fixture network fault')
        self.assertEqual(self.client.writes(), [])

    def test_funding_retry_wait_does_not_starve_known_protection_or_exits(self):
        clock = {'value': 100.0}
        self.engine._monotonic = lambda: clock['value']
        self.engine.fault('fixture funding network fault')
        self.engine.last_full_reconcile_ms = self.client.now
        self.engine.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                                for symbol in ('BTCUSDT', 'ETHUSDT')}
        with patch.object(self.client, 'income', side_effect=ApiError(None, 'fixture TLS read failed')) as income:
            with self.assertRaises(ApiError):
                self.engine._funding()
            clock['value'] += 2
            self.client.now += 2000
            with patch.object(self.engine, '_manage_protection') as protection, \
                    patch.object(self.engine, '_risk_and_exit') as exits:
                self.engine.tick()
            income.assert_called_once()
            protection.assert_called_once()
            exits.assert_called_once()
        self.assertEqual(self.engine.state, 'fault')
        self.assertEqual(self.client.writes(), [])

    def test_human_recovery_and_restart_require_failed_funding_read_to_finish(self):
        self.store.set_meta('funding_read_pending', True)
        restarted = Engine(self.client, self.store, activation_check=lambda: True)
        restarted.analyses = {symbol: Analysis(True, '', 1001, D('100'), D('1'))
                              for symbol in ('BTCUSDT', 'ETHUSDT')}
        restarted.state = 'fault'
        restarted.fault_reason = 'fixture funding read failed'
        self.client.calls.clear()
        with patch.object(self.client, 'income', side_effect=ApiError(None, 'fixture still unavailable')) as income:
            with self.assertRaises(ApiError):
                restarted.start({'BTCUSDT': True, 'ETHUSDT': False})
            income.assert_called_once()
        self.assertEqual(restarted.state, 'fault')
        self.assertEqual(self.client.writes(), [])
        self.assertTrue(self.store.meta('funding_read_pending'))
        self.client.calls.clear()
        response = restarted.start({'BTCUSDT': True, 'ETHUSDT': False})
        self.assertEqual(response['state'], 'running')
        self.assertTrue(any(row[0] == 'income' for row in self.client.calls))
        self.assertFalse(self.store.meta('funding_read_pending'))
        self.assertFalse(any(row[0] == 'market_order' for row in self.client.calls))

    def test_restart_unknown_market_queries_persisted_id_without_any_write(self):
        trade = trade_row()
        self.store.save_trade(trade)
        self.store.save_order(dict(id="durable-entry", client_id="durable-entry", trade_id=trade["id"],
                                   symbol="BTCUSDT", kind="market", purpose="entry", state="unknown",
                                   quantity="1", time_ms=self.client.now, fills_complete=False))
        self.client.order_rows[101] = dict(symbol="BTCUSDT", orderId=101, clientOrderId="durable-entry",
                                           status="FILLED", executedQty="1", origQty="1")
        timestamp = 1000 * HOUR_MS + 2000
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", timestamp)]
        self.client.position_rows = [dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="100")]
        reboot = Engine(self.client, self.store, activation_check=lambda: True)
        self.client.calls.clear()
        reboot.tick()
        self.assertEqual(reboot.state, "stopped")
        self.assertEqual(self.client.writes(), [])
        queries = [row for row in self.client.calls if row[0] == "query_order"]
        self.assertTrue(queries)
        self.assertTrue(all(row[2]["client_id"] == "durable-entry" for row in queries))
        self.assertEqual(self.store.order("durable-entry")["state"], "terminal")
        self.assertEqual(self.store.trade(trade["id"])["first_fill_ms"], timestamp)

    def test_unknown_order_absent_query_does_not_retry_submission_or_cancel(self):
        trade = trade_row()
        self.store.save_trade(trade)
        self.store.save_order(dict(id="unknown-entry", client_id="unknown-entry", trade_id=trade["id"],
                                   symbol="BTCUSDT", kind="market", purpose="entry", state="prepared",
                                   quantity="1", time_ms=self.client.now, fills_complete=False))
        self.engine.tick()
        self.engine.tick()
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.store.order("unknown-entry")["state"], "unknown")
        self.assertFalse(self.engine.reconciled)

    def test_acknowledged_new_zero_fill_order_still_blocks_new_entries(self):
        trade = trade_row()
        self.store.save_trade(trade)
        intent = self.engine._submit_market(trade, "entry", D("1"))
        self.engine._query_intent(intent)
        actual = self.store.order(intent["id"])
        self.assertEqual(actual["state"], "confirmed")
        self.assertTrue(actual["fills_complete"])
        self.assertTrue(any(row["id"] == intent["id"] for row in self.engine._blocked_orders()))

    def test_db_intent_delay_to_exact_five_minute_boundary_never_sends_market_request(self):
        trade = trade_row()
        self.store.save_trade(trade)
        save = self.store.save_order
        def delayed_intent(value):
            save(value)
            if value["kind"] == "market" and value["purpose"] == "entry" and value["state"] == "prepared":
                self.client.now = trade["signal_boundary_ms"] + 300000
        with patch.object(self.store, "save_order", side_effect=delayed_intent):
            intent = self.engine._submit_market(trade, "entry", D("1"))
        self.assertEqual(intent["state"], "terminal")
        self.assertTrue(intent["locally_not_submitted"])
        self.assertTrue(intent["fills_complete"])
        self.assertFalse(any(row[0] == "market_order" for row in self.client.calls))
        self.engine._query_intent(intent)
        self.assertFalse(any(row[0] == "query_order" for row in self.client.calls))

    def test_submit_timeout_attempted_once_and_restart_only_queries(self):
        trade = trade_row()
        self.store.save_trade(trade)
        self.client.market_error = OrderUncertain(None, "offline simulated timeout")
        intent = self.engine._submit_market(trade, "entry", D("1"))
        self.assertEqual(intent["state"], "unknown")
        self.assertEqual(len([row for row in self.client.calls if row[0] == "market_order"]), 1)
        reboot = Engine(self.client, self.store)
        self.client.calls.clear()
        reboot.tick()
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.store.order(intent["id"])["state"], "unknown")

    def test_raw_fills_rebuild_after_crash_deduplicates_and_preserves_first_exchange_time(self):
        trade = trade_row(quantity="999", first_fill_ms=self.client.now, actual_cost="999")
        self.store.save_trade(trade)
        first = 1000 * HOUR_MS + 2000
        fills = [fill_row(1, 101, "2", "100", first, fee=".1"),
                 fill_row(2, 101, "2", "102", first + 1000, fee=".102"),
                 fill_row(3, 102, "1", "90", first + HOUR_MS, purpose="reduce", fee=".045", pnl="-11")]
        for row in fills:
            self.assertTrue(self.store.claim_fill("BTCUSDT", row))
            self.assertFalse(self.store.claim_fill("BTCUSDT", row))
        self.client.position_rows = [dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="3", entryPrice="101")]
        reboot = Engine(self.client, self.store)
        reboot.reconcile()
        actual = self.store.trade(trade["id"])
        self.assertEqual(actual["fill_quantity"], "3")
        self.assertEqual(D(actual["fill_average"]), D("101"))
        self.assertEqual(D(actual["actual_fees"]), D(".247"))
        self.assertEqual(D(actual["actual_cost"]), D("11.247"))
        self.assertEqual(actual["first_fill_ms"], first)
        reboot.reconcile()
        self.assertEqual(self.store.trade(trade["id"])["actual_cost"], actual["actual_cost"])
        self.assertEqual(self.client.account_value["totalWalletBalance"], "1000")
        self.assertEqual(self.client.writes(), [])

    def test_first_partial_fill_protected_in_entry_call_without_waiting_next_hour(self):
        timestamp = self.client.now - 1000
        def partial(row, symbol, side, requested, reduce_only):
            self.assertFalse(reduce_only)
            row.update(status="PARTIALLY_FILLED", executedQty="1", avgPrice="100")
            self.client.fill_rows[row["orderId"]] = [fill_row(1, row["orderId"], "1", "100", timestamp,
                                                          trade=self.store.trades(True)[0]["id"])]
            self.client.position_rows = [dict(symbol=symbol, positionSide="BOTH", positionAmt="1", entryPrice="100")]
        self.client.market_hook = partial
        self.engine.state = "running"
        self.engine.reconciled = True
        signal = Signal(1000 * HOUR_MS, 1001 * HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("BTCUSDT", signal, True)
        self.assertEqual(len([row for row in self.client.calls if row[0] == "market_order"]), 1)
        self.assertEqual(len([row for row in self.client.calls if row[0] == "new_stop"]), 1)
        actual = self.store.trades(True)[0]
        self.assertEqual(actual["quantity"], "1")
        self.assertEqual(actual["first_fill_ms"], timestamp)
        self.assertEqual(D(actual["confirmed_stop"]), D("95"))
        names = [row[0] for row in self.client.calls]
        self.assertLess(names.index("new_stop"), names.index("fills_all"))

    def test_new_stop_is_confirmed_before_old_is_canceled(self):
        trade = trade_row(status="open", quantity="1", average="100", tightest_stop="90")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        self.assertTrue(self.engine._ensure_stop(trade, D("95")))
        names = [row[0] for row in self.client.calls]
        cancel = names.index("cancel_algo")
        new_queries = [index for index, row in enumerate(self.client.calls)
                       if row[0] == "query_algo" and row[2]["client_id"] != "old-stop-400"]
        self.assertTrue(new_queries)
        self.assertLess(max(new_queries), cancel)
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D("95"))
        self.assertEqual(self.client.calls[cancel][2]["algo_id"], 400)

    def test_definitively_rejected_new_stop_retains_old_and_is_not_unknown(self):
        trade = trade_row(status="open", quantity="1", average="100", tightest_stop="90", confirmed_stop="90")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        self.client.stop_error = ApiError(-2021, "offline stop rejected")
        self.assertFalse(self.engine._ensure_stop(trade, D("95")))
        self.assertFalse(any(row[0] == "cancel_algo" for row in self.client.calls))
        self.assertEqual(self.client.algo_rows[400]["algoStatus"], "NEW")
        new = [row for row in self.store.orders() if row["id"] != "old-stop-400"][0]
        self.assertEqual(new["state"], "terminal")
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D("90"))

    def test_uncertain_new_stop_retains_old_and_queries_without_second_new_stop(self):
        trade = trade_row(status="open", quantity="1", average="100", tightest_stop="90", confirmed_stop="90")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        self.client.stop_error = OrderUncertain(None, "offline uncertain stop")
        self.assertFalse(self.engine._ensure_stop(trade, D("95")))
        self.assertFalse(self.engine._ensure_stop(trade, D("95")))
        self.assertEqual(len([row for row in self.client.calls if row[0] == "new_stop"]), 1)
        self.assertFalse(any(row[0] == "cancel_algo" for row in self.client.calls))
        self.assertEqual(self.engine._effective_stop(trade)[0], D("90"))

    def test_malformed_new_stop_response_is_unknown_and_never_cancels_old_or_resubmits(self):
        trade = trade_row(status="open", quantity="1", average="100", tightest_stop="90", confirmed_stop="90")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        def malformed(*args):
            self.client._call("new_stop", *args)
            return {"symbol": "BTCUSDT"}
        with patch.object(self.client, "new_stop", side_effect=malformed):
            self.assertFalse(self.engine._ensure_stop(trade, D("95")))
            self.assertFalse(self.engine._ensure_stop(trade, D("95")))
        new = [row for row in self.store.orders() if row["id"] != "old-stop-400"][0]
        self.assertEqual(new["state"], "unknown")
        self.assertEqual(len([row for row in self.client.calls if row[0] == "new_stop"]), 1)
        self.assertFalse(any(row[0] == "cancel_algo" for row in self.client.calls))
        self.assertEqual(self.engine._effective_stop(trade)[0], D("90"))

    def test_triggered_finished_or_wrong_fields_never_count_as_active_protection(self):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        for status in ("TRIGGERED", "FINISHED", "CANCELED", "EXPIRED"):
            with self.subTest(status=status):
                row = self.register_stop(trade, "90", status=status)
                self.assertFalse(self.engine._valid_stop(trade, row))
                self.assertIsNone(self.engine._effective_stop(trade)[0])
        row = self.register_stop(trade, "90")
        for field, wrong in (("workingType", "MARK_PRICE"), ("priceProtect", True),
                             ("closePosition", False), ("side", "BUY")):
            invalid = dict(row, **{field: wrong})
            self.assertFalse(self.engine._valid_stop(trade, invalid))

    def test_real_flatness_required_before_closed_stop_cleanup_and_cancel_once(self):
        trade = trade_row(status="closed", quantity="0")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        self.engine.positions = {"BTCUSDT": dict(positionAmt="1", entryPrice="100")}
        self.engine._closed_cleanup()
        self.assertFalse(any(row[0] == "cancel_algo" for row in self.client.calls))
        self.engine.positions = {}
        self.engine._closed_cleanup()
        self.engine._closed_cleanup()
        self.assertEqual(len([row for row in self.client.calls if row[0] == "cancel_algo"]), 1)

    def cancellation_target(self, kind, identifier):
        trade = trade_row(identifier='cancel-trade-' + str(identifier))
        self.store.save_trade(trade)
        if kind == 'stop':
            self.register_stop(trade, '90', identifier=identifier)
            return self.store.order('old-stop-' + str(identifier)), 'cancel_algo', 'query_algo'
        client_id = 'cancel-market-' + str(identifier)
        raw = dict(symbol='BTCUSDT', orderId=identifier, clientOrderId=client_id,
                   status='NEW', executedQty='0', origQty='1')
        self.client.order_rows[identifier] = raw
        intent = dict(id=client_id, client_id=client_id, trade_id=trade['id'], symbol='BTCUSDT',
                      kind='market', purpose='entry', state='confirmed', order_id=identifier,
                      quantity='1', fills_complete=False, time_ms=self.client.now, raw=raw)
        self.store.save_order(intent)
        return intent, 'cancel_order', 'query_order'

    def test_definite_cancel_rejection_requires_fresh_target_query_and_new_durable_attempt(self):
        for index, kind in enumerate(('stop', 'market')):
            with self.subTest(kind=kind):
                target, cancel_method, _ = self.cancellation_target(kind, 700 + index)
                before = len(self.client.writes())
                def reject(*args, **kwargs):
                    self.client._call(cancel_method, *args, **kwargs)
                    raise ApiError(-1003, 'Local request budget is cooling down', status=429)
                with patch.object(self.client, cancel_method, side_effect=reject):
                    self.engine._cancel(target)
                    self.engine._cancel(target)
                rejected = self.store.order('cancel-' + target['id'])
                self.assertEqual(rejected['state'], 'terminal')
                self.assertTrue(rejected['rejected'])
                self.assertFalse(any(o['id'] == rejected['id'] for o in self.engine._unknown()))
                self.client.now += 1000
                self.engine._cancel(target)
                self.assertEqual(len(self.client.writes()) - before, 1)
                self.engine._query_intent(self.store.order(target['id']))
                self.engine._cancel(target)
                self.engine._cancel(target)
                attempts = [o for o in self.store.orders() if o.get('target_id') == target['id']]
                self.assertEqual(len(attempts), 2)
                self.assertEqual(self.store.order(rejected['id']), rejected)
                latest = self.store.order(rejected['id'] + '-2')
                self.assertEqual(latest['state'], 'confirmed')
                self.assertEqual(latest['attempt'], 2)
                self.assertEqual(len(self.client.writes()) - before, 2)
                self.engine._query_intent(self.store.order(target['id']))
                self.engine._cancel(target)
                self.assertEqual(len(self.client.writes()) - before, 2)

    def test_uncertain_cancel_is_not_repeated_after_fresh_query_or_restart(self):
        for index, kind in enumerate(('stop', 'market')):
            with self.subTest(kind=kind):
                target, cancel_method, _ = self.cancellation_target(kind, 800 + index)
                before = len(self.client.writes())
                def uncertain(*args, **kwargs):
                    self.client._call(cancel_method, *args, **kwargs)
                    raise OrderUncertain(-1007, 'Timeout after request may have reached exchange')
                with patch.object(self.client, cancel_method, side_effect=uncertain):
                    self.engine._cancel(target)
                self.client.now += 1000
                self.engine._query_intent(self.store.order(target['id']))
                reboot = Engine(self.client, self.store)
                reboot._cancel(target)
                self.assertEqual(self.store.order('cancel-' + target['id'])['state'], 'unknown')
                self.assertEqual(len(self.client.writes()) - before, 1)

    def test_rejected_cancel_failed_target_query_does_not_authorize_new_attempt(self):
        target, cancel_method, query_method = self.cancellation_target('stop', 900)
        with patch.object(self.client, cancel_method, side_effect=ApiError(-1003, 'Rate limited', status=429)):
            self.engine._cancel(target)
        self.client.now += 1000
        # Every permitted GET must remain unconfirmed; a matching fresh open
        # conditional order now legitimately confirms the original target.
        with patch.object(self.client, query_method, side_effect=ApiError(-2013, 'Not found')), \
                patch.object(self.client, 'open_algos', return_value=[]):
            with self.assertRaises(ExecutionFault):
                self.engine._query_intent(self.store.order(target['id']))
        with patch.object(self.client, cancel_method) as cancel:
            self.engine._cancel(target)
            cancel.assert_not_called()
        self.assertEqual(len([o for o in self.store.orders() if o.get('target_id') == target['id']]), 1)

    def test_finishing_algo_alone_does_not_close_trade_with_actual_position(self):
        trade = trade_row()
        self.store.save_trade(trade)
        first = 1000 * HOUR_MS + 2000
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", first))
        self.register_stop(trade, "90", status="FINISHED")
        self.client.position_rows = [dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="100")]
        self.engine.reconcile()
        actual = self.store.trade(trade["id"])
        self.assertEqual(actual["status"], "open")
        self.assertEqual(actual["quantity"], "1")
        self.assertIsNone(actual["confirmed_stop"])

    def test_finished_stop_immediate_response_discovers_child_and_accounts_exit_after_reboot(self):
        first = 1000 * HOUR_MS + 2000
        exit_time = 1001 * HOUR_MS + 1500
        trade = trade_row(status="open", quantity="1", average="100", first_fill_ms=first)
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", first, fee=".05"))
        original = self.client.new_stop
        def immediate_stop(symbol, side, trigger, client_id):
            response = original(symbol, side, trigger, client_id)
            response.update(algoStatus="FINISHED", actualOrderId="900")
            self.client.algo_rows[response["algoId"]] = response
            self.client.order_rows[900] = dict(symbol=symbol, orderId=900, clientOrderId="stop-child",
                                               status="FILLED", executedQty="1", origQty="1")
            self.client.fill_rows[900] = [fill_row(2, 900, "1", "90", exit_time, purpose="stop_exit",
                                                   fee=".045", pnl="-10")]
            self.client.position_rows = []
            return copy.deepcopy(response)
        with patch.object(self.client, "new_stop", side_effect=immediate_stop):
            self.assertFalse(self.engine._ensure_stop(trade, D("98")))
        children = [row for row in self.store.orders() if row["purpose"] == "stop_exit"]
        self.assertEqual(len(children), 1)
        self.assertEqual(children[0]["symbol"], "BTCUSDT")
        self.assertEqual(children[0]["order_id"], 900)
        reboot = Engine(self.client, self.store)
        self.client.calls.clear()
        reboot.reconcile()
        actual = self.store.trade(trade["id"])
        self.assertEqual(actual["status"], "closed")
        self.assertEqual(actual["fill_quantity"], "0")
        self.assertEqual(actual["first_fill_ms"], first)
        self.assertEqual(actual["closed_ms"], exit_time)
        self.assertEqual(D(actual["actual_cost"]), D("10.095"))
        self.assertEqual(D(actual["actual_fees"]), D(".095"))
        self.assertEqual(D(actual["realized_pnl"]), D("-10"))
        self.assertEqual(self.client.writes(), [])

    def test_real_flat_with_late_stop_child_fills_does_not_close_ledger_prematurely(self):
        first = 1000 * HOUR_MS + 2000
        exit_time = 1001 * HOUR_MS + 1500
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", first))
        row = self.register_stop(trade, "90", status="FINISHED")
        row["actualOrderId"] = "900"
        intent = self.store.order("old-stop-400")
        intent.update(state="terminal", raw=row)
        self.store.save_order(intent)
        self.client.algo_rows[400] = row
        self.client.order_rows[900] = dict(symbol="BTCUSDT", orderId=900, clientOrderId="child-900",
                                           status="FILLED", executedQty="1", origQty="1")
        self.engine.reconcile()
        self.assertNotEqual(self.store.trade(trade["id"])["status"], "closed")
        self.assertFalse(self.engine.reconciled)
        self.client.fill_rows[900] = [fill_row(2, 900, "1", "90", exit_time,
                                               purpose="stop_exit", fee=".045", pnl="-10")]
        self.engine.reconcile()
        actual = self.store.trade(trade["id"])
        self.assertEqual(actual["status"], "closed")
        self.assertEqual(actual["closed_ms"], exit_time)
        self.assertEqual(D(actual["actual_fees"]), D(".095"))

    def test_same_algo_id_in_other_symbol_cannot_confirm_wrong_protection(self):
        btc = trade_row("btc", status="open", quantity="1", average="100")
        eth = trade_row("eth", symbol="ETHUSDT", status="open", quantity="1", average="100")
        self.store.save_trade(btc)
        self.store.save_trade(eth)
        self.register_stop(btc, "90", identifier=400)
        eth_raw = self.client.stop_row(400, "eth-stop", "95", symbol="ETHUSDT")
        self.store.save_order(dict(id="eth-stop", client_id="eth-stop", kind="stop", purpose="protection",
                                   trade_id=eth["id"], symbol="ETHUSDT", order_id=400,
                                   state="confirmed", raw=eth_raw))
        self.engine.algo_map[("ETHUSDT", "400")] = eth_raw
        self.assertEqual(self.engine._effective_stop(btc)[0], D("90"))
        self.assertEqual(self.engine._effective_stop(eth)[0], D("95"))
        del self.engine.algo_map[("BTCUSDT", "400")]
        self.assertIsNone(self.engine._effective_stop(btc)[0])
        self.assertEqual(self.engine._effective_stop(eth)[0], D("95"))

    def test_missing_false_field_or_one_way_position_side_invalidates_stop(self):
        trade = trade_row(status="open", quantity="1", average="100")
        row = self.client.stop_row(400, "check-fields", "90")
        self.assertTrue(self.engine._valid_stop(trade, row))
        for field in ("priceProtect", "positionSide"):
            missing = dict(row)
            missing.pop(field)
            self.assertFalse(self.engine._valid_stop(trade, missing))
        self.assertFalse(self.engine._valid_stop(trade, dict(row, positionSide="LONG")))

    def test_closed_trade_residual_close_position_stop_blocks_new_entry(self):
        trade = trade_row(status="closed", quantity="0", fill_quantity="0", closed_ms=self.client.now - HOUR_MS)
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        self.engine.state = "running"
        self.engine.reconciled = True
        self.assertTrue(self.engine._blocked_orders())
        signal = Signal(1000 * HOUR_MS, 1001 * HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("BTCUSDT", signal, True)
        self.assertFalse(any(row[0] == "market_order" for row in self.client.calls))
        self.assertEqual(len(self.store.trades()), 1)

    def test_triggering_stop_without_child_blocks_new_entry(self):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", self.client.now - HOUR_MS))
        row = self.register_stop(trade, "90", status="TRIGGERING")
        self.client.position_rows = [dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="100")]
        self.engine.state = "running"
        self.engine.reconciled = True
        self.assertTrue(self.engine._blocked_orders())
        self.assertFalse(self.engine._valid_stop(trade, row))
        signal = Signal(1000 * HOUR_MS, 1001 * HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("ETHUSDT", signal, True)
        self.assertFalse(any(row[0] == "market_order" for row in self.client.calls))

    def test_failed_first_protection_exits_confirmed_position_even_if_fee_records_late(self):
        trade = trade_row(status="open", quantity="1", average="100", fill_quantity="0")
        self.store.save_trade(trade)
        raw = dict(symbol="BTCUSDT", orderId=101, clientOrderId="late-fee-entry",
                   status="FILLED", executedQty="1", origQty="1")
        self.store.save_order(dict(id="late-fee-entry", client_id="late-fee-entry", trade_id=trade["id"],
                                   symbol="BTCUSDT", kind="market", purpose="entry", state="terminal",
                                   quantity="1", order_id=101, fills_complete=False, raw=raw))
        position = dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="100")
        self.client.position_rows = [position]
        self.engine.positions = {"BTCUSDT": position}
        self.engine.algo_map = {}
        self.client.stop_error = ApiError(-2021, "offline protective stop rejected")
        self.engine._manage_protection()
        self.assertIsNotNone(self.store.trade(trade["id"])["exit_intent"])
        self.engine._risk_and_exit()
        market = [row for row in self.client.calls if row[0] == "market_order"]
        self.assertEqual(len(market), 1)
        self.assertEqual(market[0][1][1], "SELL")
        self.assertEqual(market[0][1][2], D("1"))
        self.assertTrue(market[0][2]["reduce_only"])
        self.engine._risk_and_exit()
        self.assertEqual(len([row for row in self.client.calls if row[0] == "market_order"]), 1)

    def test_unexplained_real_flat_with_unclosed_fills_blocks_reconciliation_and_other_symbol_entry(self):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", self.client.now - HOUR_MS))
        self.client.position_rows = []
        self.engine.state = "running"
        self.engine.reconcile()
        self.assertFalse(self.engine.reconciled)
        self.assertNotEqual(self.store.trade(trade["id"])["status"], "closed")
        self.assertEqual(self.store.trade(trade["id"])["fill_quantity"], "1")
        self.assertFalse(self.engine._pending(self.store.trade(trade["id"])))
        signal = Signal(1000 * HOUR_MS, 1001 * HOUR_MS, 1, D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("ETHUSDT", signal, True)
        self.assertFalse(any(row[0] == "market_order" for row in self.client.calls))

    def test_impossible_initial_trigger_without_old_protection_faults_and_exits_real_quantity(self):
        trade = trade_row(status="open", quantity="1", average="1", entry_atr="1", fill_quantity="1")
        self.store.save_trade(trade)
        position = dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="1")
        self.client.position_rows = [position]
        self.engine.positions = {"BTCUSDT": position}
        self.engine.algo_map = {}
        self.engine.state = "running"
        self.engine._manage_protection()
        actual = self.store.trade(trade["id"])
        self.assertIsNotNone(actual["exit_intent"])
        self.assertIsNotNone(self.engine.fault_reason)
        self.assertNotEqual(self.engine.state, "running")
        self.assertFalse(any(row[0] == "new_stop" for row in self.client.calls))
        self.engine._risk_and_exit()
        market = [row for row in self.client.calls if row[0] == "market_order"]
        self.assertEqual(len(market), 1)
        self.assertEqual(market[0][1][1], "SELL")
        self.assertEqual(market[0][1][2], D("1"))
        self.assertTrue(market[0][2]["reduce_only"])

    def test_impossible_initial_candidate_retains_existing_valid_stop_without_widening(self):
        trade = trade_row(status="open", quantity="1", average="1", entry_atr="1", fill_quantity="1",
                          confirmed_stop=".9", tightest_stop=".9")
        self.store.save_trade(trade)
        self.register_stop(trade, ".9")
        self.engine._manage_protection()
        self.assertEqual(self.engine._effective_stop(trade)[0], D(".9"))
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D(".9"))
        self.assertIsNone(self.store.trade(trade["id"])["exit_intent"])
        self.assertEqual(self.client.writes(), [])
        self.assertTrue(any("保留" in row["message"] for row in self.store.events()))

    def test_hourly_trailing_receives_exact_first_fill_time_and_excludes_that_hour(self):
        self.client.now = 1003 * HOUR_MS + 2000
        first = 1000 * HOUR_MS + 2000
        trade = trade_row(status="open", quantity="1", average="100", first_fill_ms=first,
                          confirmed_stop="90", tightest_stop="90")
        self.store.save_trade(trade)
        self.register_stop(trade, "90")
        rows = [raw_candle(index) for index in range(1003)]
        rows[1000] = raw_candle(1000, high="1000", low="1")
        self.store.save_candles("BTCUSDT", rows)
        self.engine.analyses["BTCUSDT"] = analyze(candles_from(rows), self.client.now)
        results = []
        def inspect(*args, **kwargs):
            self.assertEqual(args[2], first)
            result = trailing_stop(*args, **kwargs)
            results.append(result)
            return result
        with patch("execution.engine.trailing_stop", side_effect=inspect):
            self.engine._hourly_exits(1003 * HOUR_MS)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].eligible_count, 2)
        self.assertEqual(results[0].highest, D("101"))
        self.assertEqual(results[0].lowest, D("99"))
        self.assertEqual(self.store.trade(trade["id"])["last_trail_hour"], 1002 * HOUR_MS)

    def test_occupied_signal_consumed_before_exit_cannot_reverse_same_hour(self):
        trade = trade_row(status="open", quantity="1", average="100", confirmed_stop="98", tightest_stop="98")
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", self.client.now - HOUR_MS))
        self.register_stop(trade, "98")
        self.client.position_rows = [dict(symbol="BTCUSDT", positionSide="BOTH", positionAmt="1", entryPrice="100")]
        for symbol in ("BTCUSDT", "ETHUSDT"):
            rows = [raw_candle(index) for index in range(1001)]
            rows[-1] = raw_candle(1000, high="102", low="99", close="101.1")
            self.store.save_candles(symbol, rows)
        self.engine.state = "running"
        self.engine.last_boundary = 1000 * HOUR_MS
        def confirmed_close(boundary, symbols=None):
            actual = self.store.trade(trade["id"])
            actual.update(status="closed", quantity="0", fill_quantity="0", closed_ms=self.client.now)
            self.store.save_trade(actual)
            self.client.position_rows = []
            self.engine.positions = {}
        with patch.object(self.engine, "_hourly_exits", side_effect=confirmed_close):
            self.engine.tick()
        consumed = self.store.signal("BTCUSDT", 1001 * HOUR_MS)
        self.assertIsNotNone(consumed)
        self.assertTrue(consumed["skip_reason"])
        signal = self.engine.analyses["BTCUSDT"].signal
        self.engine._entry("BTCUSDT", signal, True)
        self.assertFalse(any(row[0] == "market_order" and row[1][0] == "BTCUSDT" for row in self.client.calls))

    def test_store_rejects_changed_closed_candle_and_keeps_anchor(self):
        self.store.save_candles("BTCUSDT", [raw_candle(1), raw_candle(2)])
        with self.assertRaises(ValueError):
            self.store.save_candles("BTCUSDT", [raw_candle(1, close="101")])
        self.assertEqual(self.store.candles("BTCUSDT")[0], raw_candle(1))


if __name__ == "__main__":
    unittest.main()
