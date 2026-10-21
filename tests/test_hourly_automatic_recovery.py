"""Automatic recovery scenarios with virtual clocks, fake exchange and temp books.

No credentials, real network, services or production SQLite are used. Existing
strategy tests cover indicator mathematics; this suite controls only indicator
readiness while keeping reconciliation and accounting real.
"""
import copy
import io
import sys
import tempfile
import threading
import unittest
from collections import defaultdict, deque
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]

from execution.binance import ApiError, BinanceClient
from execution.engine import Engine, ExecutionFault, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.request_budget import RequestBudget
from execution.store import Store
from execution.strategy import Analysis, HOUR_MS, Signal
from test_hourly_binance import Transport
from test_hourly_engine import FakeExchange, fill_row, raw_candle, trade_row

INTENT_META = "trading_intent_v1"
START_REQUEST_META = "trading_start_request_v1"
SELECTION = {"BTCUSDT": False, "ETHUSDT": True}


class RecoveryExchange(FakeExchange):
    """Faults consume fake reads; shared waits never consume another wire read."""
    def __init__(self, monotonic):
        super().__init__()
        self.monotonic = monotonic
        self.failures = defaultdict(deque)
        self.gate = None
        self.can_trade = True
        self.hedge = False
        self.multi_asset = False
        self.margin_type = "CROSSED"
        self.leverage = 50
        self.budget = SimpleNamespace(snapshot=self.budget_status)

    def error(self, kind, *, seconds=0, code=None, status=None):
        error = ApiError(code, "offline " + kind, status=status,
                         method="GET", path="/fapi/v3/account")
        error.recovery_kind = kind
        error.retry_in_seconds = seconds
        error.retry_at_ms = self.now + round(seconds * 1000)
        error.definite_not_submitted = kind == "local_budget_wait"
        if kind == "network_read_wait":
            error.network_backoff = True
        return error

    def queue(self, method, error):
        self.failures[method].append(error)

    def _guard(self, method):
        if self.gate and self.monotonic() < self.gate["until"]:
            remaining = self.gate["until"] - self.monotonic()
            kind = self.gate["kind"]
            wait_kind = "network_read_wait" if kind == "network_read_failure" else kind
            raise self.error(wait_kind, seconds=remaining,
                             code=-1003 if kind == "exchange_cooldown" else None)
        self.gate = None
        if self.failures[method]:
            self._call(method)
            error = self.failures[method].popleft()
            seconds = max(0, float(getattr(error, "retry_in_seconds", 0)))
            if seconds:
                self.gate = {"until": self.monotonic() + seconds,
                             "kind": error.recovery_kind}
            raise error

    def ensure_time_sync(self):
        self._guard("ensure_time_sync")
        self._call("ensure_time_sync")
        return self.now

    def sync_time(self):
        self._guard("sync_time")
        return super().sync_time()

    def set_cross50(self, symbol):
        super().set_cross50(symbol)
        self.margin_type, self.leverage = "CROSSED", 50

    def clock_status(self):
        return {"version": "time-sync-v2", "synced": True,
                "sync_age_ms": 0, "sync_rtt_ms": 1, "sync_failures": 0,
                "retry_in_seconds": 0, "recv_window_ms": 30000}

    def network_status(self):
        remaining = max(0, self.gate["until"] - self.monotonic()) if self.gate else 0
        network = bool(self.gate and self.gate["kind"] in
                       {"network_read_failure", "network_read_wait"})
        return {"version": "ten-minute-retry-v1", "interval_seconds": 600,
                "waiting": network and remaining > 0,
                "retry_in_seconds": remaining if network else 0,
                "retry_at_ms": self.now + round(remaining * 1000) if network else 0,
                "failures": int(network and remaining > 0), "probing": False}

    def budget_status(self):
        remaining = max(0, self.gate["until"] - self.monotonic()) if self.gate else 0
        cooldown = bool(self.gate and self.gate["kind"] == "exchange_cooldown")
        return {"used_weight": 0, "read_budget_per_minute": 1200,
                "critical_budget_per_minute": 1800,
                "cooldown_seconds": remaining if cooldown else 0,
                "cooldown_until_ms": self.now + round(remaining * 1000) if cooldown else 0,
                "network_retry": self.network_status()}

    def account(self):
        self._guard("account")
        return super().account()

    def income(self, **kwargs):
        self._guard("income")
        return super().income(**kwargs)

    def account_config(self):
        self._guard("account_config")
        self._call("account_config")
        return {"canTrade": self.can_trade}

    def position_mode(self):
        self._guard("position_mode")
        self._call("position_mode")
        return {"dualSidePosition": self.hedge}

    def multi_assets_mode(self):
        self._guard("multi_assets_mode")
        self._call("multi_assets_mode")
        return {"multiAssetsMargin": self.multi_asset}

    def symbol_config(self, symbol):
        self._guard("symbol_config")
        self._call("symbol_config", symbol)
        return [{"symbol": symbol, "marginType": self.margin_type,
                 "leverage": self.leverage}]


class HourlyAutomaticRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = Store(Path(self.folder.name) / "offline-automatic-recovery.sqlite")
        self.monotonic = 100.0
        self.client = RecoveryExchange(lambda: self.monotonic)
        self.warm_ready = True
        self.warm_signal = None
        self.warm_ema12 = None
        self.warm_boll_mid = None
        self.store.set_meta(LEDGER_META, {
            symbol: {"strategy": STRATEGY, "version": STRATEGY_VERSION,
                     "epoch": "fixture-ledger-" + symbol,
                     "initialized_at_ms": 0, "activation_boundary_ms": 0,
                     "initial_wallet": "100", "wallet": "100"}
            for symbol in ("BTCUSDT", "ETHUSDT")
        })
        self.store.set_meta("symbol_enabled", SELECTION)
        def refresh(engine):
            self.client._call("indicator_warmup")
            engine.analyses = {
                symbol: Analysis(self.warm_ready, "" if self.warm_ready else "offline warming",
                                 1001, D("100"), D("1"),
                                 self.warm_signal if symbol == "ETHUSDT" else None,
                                 ema12=self.warm_ema12, boll_mid=self.warm_boll_mid)
                for symbol in ("BTCUSDT", "ETHUSDT")
            }
        self.warm_patch = patch.object(Engine, "refresh_candles", autospec=True,
                                       side_effect=refresh)
        self.warm_patch.start()
        self.addCleanup(self.warm_patch.stop)
        self.engine = self.new_engine()
        self.engine.refresh_rules()
        self.engine.refresh_candles()
        self.client.calls.clear()

    def new_engine(self, *, restore=False):
        return Engine(self.client, self.store, activation_check=lambda: True,
                      monotonic=lambda: self.monotonic, restore_saved_intent=restore)

    def advance(self, seconds):
        self.monotonic += seconds
        self.client.now += round(seconds * 1000)

    def calls(self, method):
        return [row for row in self.client.calls if row[0] == method]

    def start_human(self):
        self.engine.start(dict(SELECTION))
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.engine.state, "running")
        self.client.calls.clear()

    def ledger_identity(self):
        fields = ("strategy", "version", "epoch", "initialized_at_ms",
                  "activation_boundary_ms", "initial_wallet")
        return {symbol: {field: value[field] for field in fields}
                for symbol, value in self.store.meta(LEDGER_META).items()}

    def trip(self, kind="network_read_failure", *, seconds=0, code=None, status=None,
             method="account"):
        error = self.client.error(kind, seconds=seconds, code=code, status=status)
        self.client.queue(method, error)
        self.engine.last_full_reconcile_ms = None
        self.engine.tick()
        self.assertTrue(self.engine.desired_running)
        return error

    def assert_recovered(self):
        self.assertEqual(self.engine.state, "running")
        self.assertIsNone(self.engine.fault_reason)
        self.assertTrue(self.engine.reconciled)
        state = self.engine.snapshot()
        self.assertTrue(state["desired_running"])
        self.assertEqual(state["execution_recovery"]["version"], "automatic-recovery-v1")
        self.assertFalse(state["execution_recovery"]["pending"])
        self.assertEqual(state["symbol_enabled"], SELECTION)
        self.assertEqual(self.calls("set_cross50"), [])

    def owned_position(self, *, pending_exit=False):
        """One exchange-confirmed entry, valid hosted stop, optional original exit."""
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="open", quantity="1", average="100",
                          first_fill_ms=first, confirmed_stop="95", tightest_stop="95")
        if pending_exit:
            trade["exit_intent"] = {"reason": "original protective exit", "time_ms": first}
        self.store.save_trade(trade)
        entry = fill_row(1, 101, "1", "100", first, fee=".05")
        entry["symbol"] = "ETHUSDT"
        self.store.claim_fill("ETHUSDT", entry)
        raw_entry = {"symbol": "ETHUSDT", "orderId": 101,
            "clientOrderId": "owned-entry", "status": "FILLED", "origQty": "1",
            "executedQty": "1", "avgPrice": "100"}
        self.client.order_rows[101] = copy.deepcopy(raw_entry)
        self.client.fill_rows[101] = [copy.deepcopy(entry)]
        self.store.save_order({"id": "owned-entry", "client_id": "owned-entry",
            "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "market",
            "purpose": "entry", "state": "terminal", "order_id": 101,
            "raw": raw_entry, "quantity": "1", "time_ms": first, "fills_complete": True})
        raw_stop = self.client.stop_row(400, "owned-stop", "95", symbol="ETHUSDT")
        self.client.algo_rows[400] = copy.deepcopy(raw_stop)
        self.store.save_order({"id": "owned-stop", "client_id": "owned-stop",
            "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "stop",
            "purpose": "protection", "state": "confirmed", "order_id": 400,
            "raw": raw_stop, "trigger": "95", "time_ms": first})
        self.client.position_rows = [{"symbol": "ETHUSDT", "positionSide": "BOTH",
            "positionAmt": "1", "entryPrice": "100", "unRealizedProfit": "0"}]
        self.engine.positions = {"ETHUSDT": copy.deepcopy(self.client.position_rows[0])}
        self.engine.algo_map[("ETHUSDT", "400")] = copy.deepcopy(raw_stop)
        self.client.next_order = 700
        if pending_exit:
            raw_exit = {"symbol": "ETHUSDT", "orderId": 600,
                "clientOrderId": "original-exit", "status": "NEW", "origQty": "1",
                "executedQty": "0", "avgPrice": "0", "reduceOnly": True}
            self.client.order_rows[600] = copy.deepcopy(raw_exit)
            self.store.save_order({"id": "original-exit", "client_id": "original-exit",
                "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "market",
                "purpose": "exit", "state": "confirmed", "order_id": 600,
                "raw": raw_exit, "quantity": "1", "time_ms": first,
                "fills_complete": False})
        return trade

    def test_human_start_records_intent_and_fault_does_not_overwrite_it(self):
        self.start_human()
        saved = copy.deepcopy(self.store.meta(INTENT_META))
        self.assertEqual(saved["version"], 1)
        self.assertIs(saved["desired_running"], True)
        self.assertEqual(saved["source"], "human_start")
        self.assertIsInstance(saved["updated_at_ms"], int)
        self.engine.fault("offline read failure")
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.store.meta(INTENT_META), saved)
        self.assertEqual(self.client.writes(), [])

    def test_network_failure_success_requires_fresh_reads_without_start_or_ledger_reset(self):
        self.start_human()
        identity = self.ledger_identity()
        self.trip(seconds=600)
        self.assertTrue(self.engine.snapshot()["execution_recovery"]["pending"])
        self.assertNotEqual(self.engine.state, "running")
        self.advance(600)
        before = len(self.calls("account"))
        with patch.object(self.engine, "start", side_effect=AssertionError("No human start during recovery")):
            self.engine.tick()
        self.assertGreater(len(self.calls("account")), before)
        for method in ("income", "account_config", "position_mode", "multi_assets_mode", "symbol_config"):
            self.assertTrue(self.calls(method), method)
        self.assert_recovered()
        self.assertEqual(self.ledger_identity(), identity)
        self.assertEqual(self.client.writes(), [])

    def test_repeated_network_failures_keep_original_user_intent_and_wait_boundaries(self):
        self.start_human()
        self.trip(seconds=600)
        first_reads = len(self.calls("account"))
        for seconds in (100, 200, 299):
            self.advance(seconds)
            self.engine.tick()
        self.assertEqual(len(self.calls("account")), first_reads)
        self.assertTrue(self.engine.desired_running)
        self.client.queue("account", self.client.error("network_read_failure", seconds=600))
        self.advance(1)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.advance(600)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_failed_finally_reconcile_timestamp_is_never_treated_as_fresh_success(self):
        self.start_human()
        self.trip(seconds=600)
        self.advance(600)
        # A previous failed reconcile can publish its completion timestamp in
        # finally. Even a timestamp in this poll window proves no successful GET.
        self.engine.last_full_reconcile_ms = self.client.now
        before = len(self.calls("account"))
        self.engine.tick()
        self.assertGreater(len(self.calls("account")), before)
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_local_budget_wait_is_not_an_order_write_or_a_lost_user_intent(self):
        self.start_human()
        self.trip("local_budget_wait", seconds=60)
        self.assertTrue(self.store.meta(INTENT_META)["desired_running"])
        self.assertEqual(self.client.writes(), [])
        before = len(self.calls("account"))
        self.advance(59)
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.advance(1)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_real_http_429_preserves_longer_exchange_cooldown_before_resume(self):
        self.start_human()
        wire_error = HTTPError("https://offline.invalid/fapi/v3/account", 429, "limited",
            {"Retry-After": "900"}, io.BytesIO(b'{"code":-1003,"msg":"offline limited"}'))
        wire = Transport(wire_error)
        budget = RequestBudget(Path(self.folder.name) / "offline-ip-budget.sqlite",
                               clock=lambda: self.client.now / 1000)
        wire_client = BinanceClient("offline-key", "offline-secret", transport=wire,
            budget=budget, auto_sync_time=False, get_retries=0,
            clock=lambda: self.client.now / 1000, monotonic=lambda: self.monotonic,
            sleep=lambda _: self.fail("Real cooldown must not sleep/retry immediately"))
        with self.assertRaises(ApiError) as caught:
            wire_client.account()
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(caught.exception.recovery_kind, "exchange_cooldown")
        self.assertGreaterEqual(caught.exception.retry_in_seconds, 900)
        self.assertEqual(len(wire.requests), 1)
        self.client.queue("account", caught.exception)
        self.engine.last_full_reconcile_ms = None
        self.engine.tick()
        before = len(self.calls("account"))
        self.advance(600)
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.advance(299)
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.advance(2)  # RateLimited's conservative integer rounding may add one second.
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_timestamp_rejection_recovers_after_calibration_and_fresh_account_check(self):
        self.start_human()
        self.trip("timestamp_rejected", code=-1021)
        before = len(self.calls("account"))
        self.advance(1)
        self.engine.tick()
        self.assertGreater(len(self.calls("account")), before)
        self.assertTrue(self.calls("ensure_time_sync"))
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_failed_funding_read_must_finish_before_permission_is_restored(self):
        self.start_human()
        self.engine.last_income = 0
        self.trip(method="income", seconds=600)
        self.assertTrue(self.engine.funding_pending_recovery)
        self.advance(599)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.advance(1)
        self.engine.tick()
        self.assertFalse(self.engine.funding_pending_recovery)
        self.assertFalse(self.store.meta("funding_read_pending"))
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_unknown_original_order_blocks_recovery_until_original_is_resolved(self):
        self.start_human()
        trade = trade_row(symbol="ETHUSDT")
        self.store.save_trade(trade)
        intent = {"id": "original-entry", "client_id": "original-entry",
                  "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "market",
                  "purpose": "entry", "state": "unknown", "quantity": "1",
                  "time_ms": self.client.now, "fills_complete": False}
        self.store.save_order(intent)
        self.engine.handle_loop_failure(self.client.error("order_uncertain"))
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.engine.snapshot()["unknown_orders"])
        self.assertEqual(self.client.writes(), [])
        self.client.order_rows[101] = {"symbol": "ETHUSDT", "orderId": 101,
            "clientOrderId": "original-entry", "status": "CANCELED",
            "origQty": "1", "executedQty": "0", "avgPrice": "0"}
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.engine.snapshot()["unknown_orders"], [])
        self.assertEqual(self.client.writes(), [])

    def test_flat_position_with_late_exit_fees_is_not_yet_complete_recovery(self):
        self.start_human()
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="open", quantity="0", average="100",
                          first_fill_ms=first, exit_intent={"reason": "protection failed", "time_ms": first})
        self.store.save_trade(trade)
        for identifier, purpose, price in ((101, "entry", "100"), (102, "exit", "99")):
            raw = {"symbol": "ETHUSDT", "orderId": identifier,
                   "clientOrderId": "owned-" + purpose, "status": "FILLED",
                   "origQty": "1", "executedQty": "1", "avgPrice": price}
            self.client.order_rows[identifier] = copy.deepcopy(raw)
            self.store.save_order({"id": "owned-" + purpose, "client_id": "owned-" + purpose,
                "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "market",
                "purpose": purpose, "state": "terminal", "order_id": identifier,
                "raw": raw, "quantity": "1", "time_ms": first,
                "fills_complete": purpose == "entry"})
        entry = fill_row(1, 101, "1", "100", first, fee=".05")
        entry["symbol"] = "ETHUSDT"
        self.store.claim_fill("ETHUSDT", entry)
        self.client.fill_rows[101] = [copy.deepcopy(entry)]
        self.engine.handle_loop_failure(self.client.error("order_uncertain"))
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertIsNotNone(self.store.trade(trade["id"])["exit_intent"])
        self.assertEqual(self.client.writes(), [])
        exit_fill = fill_row(2, 102, "1", "99", first + 1000,
                             purpose="exit", fee=".05", pnl="-1")
        exit_fill["symbol"] = "ETHUSDT"
        self.client.fill_rows[102] = [exit_fill]
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.store.trade(trade["id"])["status"], "closed")
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        self.assertEqual(self.client.writes(), [])

    def test_ledger_corruption_blocks_resume_and_never_reinitializes_money(self):
        self.start_human()
        corrupted = self.store.meta(LEDGER_META)
        corrupted["ETHUSDT"]["initial_wallet"] = "200"
        self.store.set_meta(LEDGER_META, corrupted)
        self.engine.handle_loop_failure(self.client.error("network_read_failure"))
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.store.meta(LEDGER_META), corrupted)
        self.assertEqual(self.client.writes(), [])

    def test_non_usdt_actual_fee_blocks_automatic_resume_without_guessing_conversion(self):
        self.start_human()
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="closed", quantity="0",
                          average="100", first_fill_ms=first, closed_ms=first + 1000)
        self.store.save_trade(trade)
        entry = fill_row(1, 101, "1", "100", first)
        entry.update(symbol="ETHUSDT", commissionAsset="BNB")
        exit_fill = fill_row(2, 102, "1", "100", first + 1000, purpose="exit")
        exit_fill["symbol"] = "ETHUSDT"
        self.store.claim_fill("ETHUSDT", entry)
        self.store.claim_fill("ETHUSDT", exit_fill)
        self.engine.handle_loop_failure(self.client.error("network_read_failure"))
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertFalse(self.engine.strategy_ledgers["ETHUSDT"]["verified"])
        self.assertEqual(self.client.writes(), [])

    def test_indicator_warmup_is_required_and_later_ready_data_can_restore_intent(self):
        self.start_human()
        self.trip()
        self.warm_ready = False
        self.engine.analyses = {}
        self.advance(31)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.warm_ready = True
        self.engine.analyses = {}
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_incompatible_symbol_configuration_blocks_resume_without_reconfiguration(self):
        self.start_human()
        self.trip()
        self.client.margin_type, self.client.leverage = "ISOLATED", 20
        self.advance(31)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertEqual(self.calls("set_cross50"), [])
        self.client.margin_type, self.client.leverage = "CROSSED", 50
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_account_or_position_mode_rejections_cannot_be_cleared_by_healthy_network(self):
        for field, value in (("can_trade", False), ("hedge", True), ("multi_asset", True)):
            with self.subTest(field=field):
                self.start_human()
                self.trip()
                setattr(self.client, field, value)
                self.advance(31)
                self.engine.tick()
                self.assertNotEqual(self.engine.state, "running")
                self.assertEqual(self.client.writes(), [])
                setattr(self.client, field, not value)
                self.advance(31)
                self.engine.tick()
                self.assert_recovered()

    def test_non_finite_or_non_positive_market_prices_do_not_authorize_recovery(self):
        self.start_human()
        for price in ("0", "NaN", "Infinity"):
            with self.subTest(price=price):
                self.trip()
                self.advance(31)
                with patch.object(self.client, "mark_price", return_value={"markPrice": price}):
                    self.engine.tick()
                self.assertNotEqual(self.engine.state, "running")
                self.assertEqual(self.client.writes(), [])
                self.advance(601)
                self.engine.tick()
                self.assert_recovered()

    def test_unregistered_position_blocks_resume_without_automatically_taking_it_over(self):
        self.start_human()
        self.trip()
        self.client.position_rows = [{"symbol": "ETHUSDT", "positionSide": "BOTH",
            "positionAmt": "1", "entryPrice": "100", "unRealizedProfit": "0"}]
        self.advance(31)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertFalse(self.engine.reconciled)
        self.assertEqual(self.client.writes(), [])
        self.client.position_rows = []
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_exhausted_budget_keeps_exit_pending_and_never_repeats_market_submission(self):
        self.start_human()
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="open", quantity="1", average="100",
                          first_fill_ms=first, budget0=".01", confirmed_stop="95", tightest_stop="95")
        self.store.save_trade(trade)
        entry = fill_row(1, 101, "1", "100", first, fee=".05")
        entry["symbol"] = "ETHUSDT"
        self.store.claim_fill("ETHUSDT", entry)
        raw_entry = {"symbol": "ETHUSDT", "orderId": 101,
            "clientOrderId": "owned-entry", "status": "FILLED", "origQty": "1",
            "executedQty": "1", "avgPrice": "100"}
        self.client.order_rows[101] = raw_entry
        self.client.fill_rows[101] = [copy.deepcopy(entry)]
        self.store.save_order({"id": "owned-entry", "client_id": "owned-entry",
            "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "market",
            "purpose": "entry", "state": "terminal", "order_id": 101,
            "raw": raw_entry, "quantity": "1", "time_ms": first, "fills_complete": True})
        raw_stop = self.client.stop_row(400, "owned-stop", "95", symbol="ETHUSDT")
        self.client.algo_rows[400] = copy.deepcopy(raw_stop)
        self.store.save_order({"id": "owned-stop", "client_id": "owned-stop",
            "trade_id": trade["id"], "symbol": "ETHUSDT", "kind": "stop",
            "purpose": "protection", "state": "confirmed", "order_id": 400,
            "raw": raw_stop, "trigger": "95", "time_ms": first})
        self.client.position_rows = [{"symbol": "ETHUSDT", "positionSide": "BOTH",
            "positionAmt": "1", "entryPrice": "100", "unRealizedProfit": "0"}]
        self.client.next_order = 500
        self.engine.handle_loop_failure(self.client.error("network_read_failure"))
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertIsNotNone(self.store.trade(trade["id"])["exit_intent"])
        self.assertLessEqual(len(self.calls("market_order")), 1)
        self.advance(31)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertEqual(len(self.calls("market_order")), 1)
        self.assertTrue(self.calls("market_order")[0][1][-1])  # reduce_only
        self.advance(31)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertEqual(len(self.calls("market_order")), 1)
        self.assertEqual(self.calls("set_cross50"), [])

    def test_user_pause_during_network_recovery_persists_and_never_resumes(self):
        self.start_human()
        self.trip(seconds=600)
        self.engine.pause()
        self.assertFalse(self.engine.desired_running)
        saved = self.store.meta(INTENT_META)
        self.assertIs(saved["desired_running"], False)
        self.assertEqual(saved["source"], "human_pause")
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.state, "paused")
        self.assertFalse(self.engine.snapshot()["desired_running"])
        reboot = self.new_engine(restore=True)
        reboot.tick()
        self.assertIn(reboot.state, {"stopped", "paused"})
        self.assertFalse(reboot.desired_running)
        self.assertEqual(self.client.writes(), [])

    def test_user_exit_all_revokes_persistent_permission_before_restart(self):
        self.start_human()
        self.engine.exit_all()
        saved = self.store.meta(INTENT_META)
        self.assertIs(saved["desired_running"], False)
        self.assertEqual(saved["source"], "human_exit_all")
        reboot = self.new_engine(restore=True)
        reboot.tick()
        self.assertIn(reboot.state, {"stopped", "paused"})
        self.assertFalse(reboot.desired_running)
        self.assertEqual(self.client.writes(), [])

    def test_saved_running_intent_restarts_recovering_then_checks_before_running(self):
        self.start_human()
        identity = self.ledger_identity()
        reboot = self.new_engine(restore=True)
        self.engine = reboot
        self.assertEqual(reboot.state, "recovering")
        self.assertTrue(reboot.desired_running)
        self.assertEqual(self.client.writes(), [])
        reboot.tick()
        self.assert_recovered()
        self.assertTrue(self.calls("account"))
        self.assertTrue(self.calls("income"))
        self.assertEqual(self.ledger_identity(), identity)
        self.assertEqual(self.client.writes(), [])

    def test_constructor_opt_out_and_missing_intent_do_not_infer_permission_from_events(self):
        self.start_human()
        opted_out = self.new_engine(restore=False)
        opted_out.tick()
        self.assertEqual(opted_out.state, "stopped")
        self.assertFalse(opted_out.desired_running)
        with self.store.connection() as db:
            db.execute("DELETE FROM hb_meta WHERE key=?", (INTENT_META,))
        self.store.event(self.client.now, "用户启用新策略，等待下一个新小时收盘",
                         {"symbol_enabled": SELECTION})
        old_version = self.new_engine(restore=True)
        old_version.tick()
        self.assertEqual(old_version.state, "stopped")
        self.assertFalse(old_version.desired_running)
        self.assertEqual(self.client.writes(), [])

    def test_invalid_saved_intent_never_authorizes_trading(self):
        for value in (True, {"version": 2, "desired_running": True},
                      {"version": 1, "desired_running": "true", "updated_at_ms": self.client.now,
                       "source": "human_start"},
                      {"version": 1, "desired_running": True, "updated_at_ms": self.client.now,
                       "source": "unknown"}):
            with self.subTest(value=value):
                self.store.set_meta(INTENT_META, value)
                reboot = self.new_engine(restore=True)
                reboot.tick()
                self.assertEqual(reboot.state, "stopped")
                self.assertFalse(reboot.desired_running)
                self.assertEqual(self.client.writes(), [])

    def test_symbol_switch_during_recovery_remains_authoritative(self):
        self.start_human()
        self.trip(seconds=600)
        changed = {"BTCUSDT": True, "ETHUSDT": False}
        self.engine.set_symbols(changed)
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.snapshot()["symbol_enabled"], changed)
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.calls("set_cross50"), [])
        self.assertEqual(self.client.writes(), [])

    def test_stopped_engine_read_recovery_never_grants_an_intent(self):
        self.engine.handle_loop_failure(self.client.error("network_read_failure"))
        self.engine.tick()
        self.assertEqual(self.engine.state, "stopped")
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.client.writes(), [])

    def test_recovery_does_not_replay_the_hour_that_elapsed_while_faulted(self):
        self.start_human()
        self.trip(seconds=600)
        self.advance(HOUR_MS / 1000)
        boundary = self.client.now // HOUR_MS * HOUR_MS
        self.warm_signal = Signal(boundary - HOUR_MS, boundary, 1,
                                  D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine.analyses = {}
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.calls("market_order"), [])
        self.engine.tick()
        self.assertEqual(self.calls("market_order"), [])
        old = Signal(boundary - 2 * HOUR_MS, boundary - HOUR_MS, 1,
                     D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("ETHUSDT", old, True)
        self.assertEqual(self.calls("market_order"), [])

    def test_new_valid_signal_after_recovery_is_consumed_before_one_market_write(self):
        self.start_human()
        self.trip()
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.advance(HOUR_MS / 1000)
        boundary = self.client.now // HOUR_MS * HOUR_MS
        signal = Signal(boundary - HOUR_MS, boundary, 1,
                        D("1"), D("100"), D("99"), D("99"), D("98"))
        self.engine._entry("ETHUSDT", signal, True)
        self.engine._entry("ETHUSDT", signal, True)
        self.assertEqual(len(self.calls("market_order")), 1)
        self.assertIsNotNone(self.store.signal("ETHUSDT", boundary))
        self.assertEqual(self.calls("set_cross50"), [])

    def test_network_wait_preserves_hosted_stop_and_queries_original_pending_exit_without_replay(self):
        self.start_human()
        trade = self.owned_position(pending_exit=True)
        self.trip(seconds=600)
        before = len(self.calls("query_order"))
        for step in (100, 200, 299):
            self.advance(step)
            self.engine.tick()
        self.assertEqual(len(self.calls("query_order")), before)
        self.assertEqual(self.client.algo_rows[400]["algoStatus"], "NEW")
        self.assertEqual(self.calls("cancel_algo"), [])
        self.assertEqual(self.calls("new_stop"), [])
        self.assertEqual(self.calls("market_order"), [])
        self.advance(1)
        self.engine.tick()
        queried_ids = [row[2].get("order_id") for row in self.calls("query_order")]
        self.assertIn(600, queried_ids)
        self.assertNotEqual(self.engine.state, "running")
        self.assertEqual(self.client.algo_rows[400]["algoStatus"], "NEW")
        self.assertEqual(self.calls("market_order"), [])
        self.assertEqual(self.calls("cancel_algo"), [])
        self.advance(31)
        self.engine.tick()
        self.assertEqual(self.calls("market_order"), [])
        self.client.order_rows[600].update(status="FILLED", executedQty="1", avgPrice="99")
        exit_fill = fill_row(2, 600, "1", "99", self.client.now,
                             purpose="exit", fee=".05", pnl="-1")
        exit_fill["symbol"] = "ETHUSDT"
        self.client.fill_rows[600] = [exit_fill]
        self.client.position_rows = []
        self.advance(31)
        self.engine.tick()
        self.assertEqual(self.store.trade(trade["id"])["status"], "closed")
        self.assertEqual(self.calls("market_order"), [])
        # Cleanup may cancel the original closePosition stop only after real
        # flat/fills are confirmed, and cannot be mistaken for a new exit write.
        self.advance(31)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))

    def test_execution_review_wait_keeps_targeted_reads_and_hourly_ema_exit_active(self):
        self.start_human()
        trade = self.owned_position()
        boundary = (self.client.now // HOUR_MS + 1) * HOUR_MS
        self.client.now = boundary - 1000
        self.engine.last_full_reconcile_ms = self.client.now
        self.warm_ema12 = D("99")
        self.store.save_candles("ETHUSDT", [raw_candle(
            boundary // HOUR_MS - 1, high="99", low="97", close="98")])
        error = ExecutionFault("offline execution review pending")
        self.engine.handle_loop_failure(error)
        self.assertEqual(self.engine.snapshot()["execution_recovery"]["kind"], "execution_review")
        self.advance(2)
        self.assertGreater(self.engine.snapshot()["execution_recovery"]["retry_in_seconds"], 0)
        self.engine.tick()
        self.assertTrue(self.calls("query_algo"))
        self.assertTrue(self.calls("account"))
        self.assertIsNotNone(self.store.trade(trade["id"])["exit_intent"])
        self.assertIn("EMA12", self.store.trade(trade["id"])["exit_intent"]["reason"])
        self.assertEqual(len(self.calls("market_order")), 1)
        self.assertTrue(self.calls("market_order")[0][1][-1])
        self.engine.tick()
        self.assertEqual(len(self.calls("market_order")), 1)
        self.assertEqual(self.calls("set_cross50"), [])

    def _concurrent_control_after_recovery(self, method, expected_source):
        self.start_human()
        self.trip()
        self.advance(31)
        reading = threading.Event()
        release_read = threading.Event()
        control_requested = threading.Event()
        control_done = threading.Event()
        errors = []
        original_account = self.client.account
        def blocked_account():
            reading.set()
            if not release_read.wait(2):
                raise AssertionError("Offline recovery read was not released")
            return original_account()
        def tick():
            try:
                self.engine.tick()
            except BaseException as exc:
                errors.append(exc)
        def control():
            control_requested.set()
            try:
                getattr(self.engine, method)()
            except BaseException as exc:
                errors.append(exc)
            finally:
                control_done.set()
        with patch.object(self.client, "account", side_effect=blocked_account):
            recovery_thread = threading.Thread(target=tick, daemon=True)
            control_thread = threading.Thread(target=control, daemon=True)
            recovery_thread.start()
            try:
                self.assertTrue(reading.wait(2))
                control_thread.start()
                self.assertTrue(control_requested.wait(2))
                self.assertFalse(control_done.wait(.05))
            finally:
                release_read.set()
                recovery_thread.join(2)
                if control_thread.ident is not None:
                    control_thread.join(2)
        self.assertFalse(recovery_thread.is_alive())
        self.assertFalse(control_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(self.engine.state, "paused")
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.store.meta(INTENT_META)["source"], expected_source)
        self.engine.tick()
        self.assertEqual(self.engine.state, "paused")
        self.assertEqual(self.client.writes(), [])

    def test_concurrent_pause_serializes_after_tick_and_wins_persistent_intent(self):
        self._concurrent_control_after_recovery("pause", "human_pause")

    def test_concurrent_exit_all_serializes_after_tick_and_wins_persistent_intent(self):
        self._concurrent_control_after_recovery("exit_all", "human_exit_all")

    def test_control_before_a_waiting_tick_is_not_overwritten_by_recovery(self):
        self.start_human()
        self.trip()
        self.advance(31)
        finished = threading.Event()
        with self.engine.lock:
            self.engine.pause()
            thread = threading.Thread(target=lambda: (self.engine.tick(), finished.set()), daemon=True)
            thread.start()
            self.assertFalse(finished.wait(.05))
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.engine.state, "paused")
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.store.meta(INTENT_META)["source"], "human_pause")
        self.assertEqual(self.client.writes(), [])

    def test_wall_clock_forward_and_backward_jumps_do_not_expire_monotonic_600_second_wait(self):
        self.start_human()
        self.trip(seconds=600)
        before = len(self.calls("account"))
        self.client.now += 3600 * 1000
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.assertEqual(self.engine.snapshot()["execution_recovery"]["retry_in_seconds"], 600)
        self.client.now -= 7200 * 1000
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.monotonic += 599
        self.engine.tick()
        self.assertEqual(len(self.calls("account")), before)
        self.assertNotEqual(self.engine.state, "running")
        self.monotonic += 1
        self.engine.tick()
        self.assertGreater(len(self.calls("account")), before)
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_repeated_restore_restarts_keep_actual_wallet_and_ledger_epoch(self):
        self.start_human()
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="closed", quantity="0", average="100",
                          first_fill_ms=first, closed_ms=first + 1000)
        self.store.save_trade(trade)
        entry = fill_row(1, 101, "1", "100", first, fee=".05")
        exit_fill = fill_row(2, 102, "1", "99", first + 1000,
                             purpose="exit", fee=".05", pnl="-1")
        for fill in (entry, exit_fill):
            fill["symbol"] = "ETHUSDT"
            self.store.claim_fill("ETHUSDT", fill)
        self.engine.reconcile()
        identity = self.ledger_identity()
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        for restart in range(3):
            with self.subTest(restart=restart):
                self.advance(31)
                self.engine = self.new_engine(restore=True)
                self.assertEqual(self.engine.state, "recovering")
                self.engine.tick()
                self.assert_recovered()
                self.assertEqual(self.ledger_identity(), identity)
                self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
                self.assertEqual(len(self.store.fills("ETHUSDT")), 2)
                self.assertEqual(self.client.writes(), [])

    def request_first_start_during_network_failure(self):
        self.client.queue("sync_time", self.client.error("network_read_failure", seconds=600))
        response = self.engine.start(dict(SELECTION))
        self.assertTrue(response["desired_running"])
        self.assertTrue(self.engine.desired_running)
        self.assertNotEqual(self.engine.state, "running")
        request = self.store.meta(START_REQUEST_META)
        self.assertEqual(request["version"], 1)
        self.assertIs(request["pending"], True)
        self.assertIsInstance(request["requested_at_ms"], int)
        self.assertTrue(self.store.meta(INTENT_META)["desired_running"])
        self.assertEqual(self.client.writes(), [])

    def test_first_start_network_failure_persists_request_and_finishes_after_600_seconds(self):
        identity = self.ledger_identity()
        self.request_first_start_during_network_failure()
        before = len(self.calls("sync_time"))
        self.advance(599)
        self.engine.tick()
        self.assertEqual(len(self.calls("sync_time")), before)
        self.assertNotEqual(self.engine.state, "running")
        self.assertEqual(self.client.writes(), [])
        self.advance(1)
        self.engine.tick()
        self.assert_recovered()
        self.assertGreater(len(self.calls("sync_time")), before)
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.ledger_identity(), identity)
        self.assertEqual(self.client.writes(), [])

    def test_first_start_repeated_network_failure_retains_one_pending_request(self):
        self.request_first_start_during_network_failure()
        requested = self.store.meta(START_REQUEST_META)["requested_at_ms"]
        self.client.queue("sync_time", self.client.error("network_read_failure", seconds=600))
        self.advance(600)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.store.meta(START_REQUEST_META)["pending"])
        self.assertEqual(self.store.meta(START_REQUEST_META)["requested_at_ms"], requested)
        before = len(self.calls("sync_time"))
        self.advance(599)
        self.engine.tick()
        self.assertEqual(len(self.calls("sync_time")), before)
        self.advance(1)
        self.engine.tick()
        self.assert_recovered()
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.client.writes(), [])

    def test_restart_continues_saved_pending_first_start_without_resetting_wallet_or_epoch(self):
        first = self.client.now - 10000
        trade = trade_row(symbol="ETHUSDT", status="closed", quantity="0", average="100",
                          first_fill_ms=first, closed_ms=first + 1000)
        self.store.save_trade(trade)
        for fill in (fill_row(1, 101, "1", "100", first, fee=".05"),
                     fill_row(2, 102, "1", "99", first + 1000,
                              purpose="exit", fee=".05", pnl="-1")):
            fill["symbol"] = "ETHUSDT"
            self.store.claim_fill("ETHUSDT", fill)
        self.engine.reconcile()
        identity = self.ledger_identity()
        self.request_first_start_during_network_failure()
        self.advance(200)
        self.engine = self.new_engine(restore=True)
        self.assertEqual(self.engine.state, "recovering")
        self.assertTrue(self.store.meta(START_REQUEST_META)["pending"])
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.advance(400)
        self.engine.tick()
        self.assert_recovered()
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.ledger_identity(), identity)
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        self.assertEqual(len(self.store.fills("ETHUSDT")), 2)
        self.assertEqual(self.client.writes(), [])

    def test_pause_cancels_pending_first_start_and_survives_restart(self):
        self.request_first_start_during_network_failure()
        self.engine.pause()
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertFalse(self.engine.desired_running)
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.state, "paused")
        self.engine = self.new_engine(restore=True)
        self.engine.tick()
        self.assertFalse(self.engine.desired_running)
        self.assertNotEqual(self.engine.state, "running")
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.client.writes(), [])

    def test_exit_all_cancels_pending_first_start_without_automatic_reactivation(self):
        self.request_first_start_during_network_failure()
        self.engine.exit_all()
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertFalse(self.store.meta(INTENT_META)["desired_running"])
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.state, "paused")
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.client.writes(), [])

    def test_permanent_first_start_errors_never_create_a_running_intent(self):
        for kind, code in (("credentials_error", -2014), ("exchange_rejected", -2015),
                           ("permanent", 0)):
            with self.subTest(kind=kind):
                error = self.client.error(kind, code=code, status=403)
                self.client.queue("sync_time", error)
                with self.assertRaises(ApiError):
                    self.engine.start(dict(SELECTION))
                self.assertFalse(self.engine.desired_running)
                self.assertEqual(self.engine.state, "stopped")
                self.assertIsNone(self.store.meta(INTENT_META))
                self.assertIsNone(self.store.meta(START_REQUEST_META))
                self.assertEqual(self.client.writes(), [])

    def test_pending_first_start_follows_current_user_symbol_selection(self):
        self.request_first_start_during_network_failure()
        selected = {"BTCUSDT": True, "ETHUSDT": False}
        self.engine.set_symbols(selected)
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.state, "running")
        self.assertEqual(self.engine.snapshot()["symbol_enabled"], selected)
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.client.writes(), [])

    def test_saved_human_start_changes_incorrect_config_once_but_not_on_later_recovery(self):
        self.request_first_start_during_network_failure()
        self.client.margin_type, self.client.leverage = "ISOLATED", 20
        self.advance(600)
        self.engine.tick()
        self.assertEqual(self.engine.state, "running")
        self.assertEqual(len(self.calls("set_cross50")), 1)
        self.assertEqual(self.calls("set_cross50")[0][1], ("ETHUSDT",))
        self.assertEqual(self.client.margin_type, "CROSSED")
        self.assertEqual(self.client.leverage, 50)
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.client.calls.clear()
        self.trip(seconds=600)
        self.advance(600)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(self.client.writes(), [])

    def test_pending_first_start_cannot_skip_live_price_validation_before_running(self):
        self.request_first_start_during_network_failure()
        self.advance(600)
        with patch.object(self.client, "mark_price", return_value={"markPrice": "0"}):
            self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.store.meta(START_REQUEST_META)["pending"])
        self.assertEqual(self.client.writes(), [])
        self.advance(601)
        self.engine.tick()
        self.assert_recovered()
        self.assertIsNone(self.store.meta(START_REQUEST_META))

    def saved_running_eth_book(self):
        """Restore a saved permission over real immutable fills and a changed wallet."""
        self.start_human()
        first = self.client.now - 10000
        self.store.save_trade(trade_row(symbol="ETHUSDT", status="closed", quantity="0",
            average="100", first_fill_ms=first, closed_ms=first + 1000))
        for fill in (fill_row(1, 101, "1", "100", first, fee=".05"),
                     fill_row(2, 102, "1", "99", first + 1000,
                              purpose="exit", fee=".05", pnl="-1")):
            fill["symbol"] = "ETHUSDT"
            self.store.claim_fill("ETHUSDT", fill)
        self.engine.reconcile()
        self.advance(31)
        self.engine = self.new_engine(restore=True)
        self.engine.tick()
        self.assert_recovered()
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        self.client.calls.clear()

    def assert_both_symbols_running_without_order_writes(self):
        self.assertEqual(self.engine.state, "running")
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.engine.snapshot()["symbol_enabled"],
                         {"BTCUSDT": True, "ETHUSDT": True})
        self.assertFalse(self.engine.snapshot()["execution_recovery"]["pending"])
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertEqual(self.client.writes(), [])

    def test_enabling_symbol_with_saved_intent_checks_both_existing_books_without_reset(self):
        self.saved_running_eth_book()
        identity = self.ledger_identity()
        boundary = self.client.now // HOUR_MS * HOUR_MS
        selected = {"BTCUSDT": True, "ETHUSDT": True}
        self.engine.set_symbols(selected)
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.store.meta(START_REQUEST_META)["source"], "human_symbol_selection")
        self.assertEqual(self.store.signal("BTCUSDT", boundary)["direction"], 0)
        self.advance(3)
        self.engine.tick()
        self.assert_both_symbols_running_without_order_writes()
        for method in ("symbol_config", "book_ticker", "contract_price", "mark_price"):
            with self.subTest(read=method):
                self.assertTrue(any(row[1] == ("BTCUSDT",) for row in self.calls(method)))
                self.assertTrue(any(row[1] == ("ETHUSDT",) for row in self.calls(method)))
        self.assertEqual(self.ledger_identity(), identity)
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        self.assertEqual(len(self.store.fills("ETHUSDT")), 2)
        self.advance(31)
        self.engine.tick()
        self.assert_both_symbols_running_without_order_writes()
        self.assertEqual(self.ledger_identity(), identity)

    def test_new_symbol_missing_book_is_initialized_once_after_account_permission_recovers(self):
        self.saved_running_eth_book()
        records = self.store.meta(LEDGER_META)
        records.pop("BTCUSDT")
        self.store.set_meta(LEDGER_META, records)
        eth_identity = self.ledger_identity()["ETHUSDT"]
        self.client.can_trade = False
        self.engine.set_symbols({"BTCUSDT": True, "ETHUSDT": True})
        self.advance(3)
        self.engine.tick()
        self.assertNotEqual(self.engine.state, "running")
        self.assertTrue(self.store.meta(START_REQUEST_META)["pending"])
        self.assertNotIn("BTCUSDT", self.store.meta(LEDGER_META))
        self.assertEqual(self.ledger_identity()["ETHUSDT"], eth_identity)
        self.assertEqual(self.client.writes(), [])
        self.client.can_trade = True
        self.advance(31)
        self.engine.tick()
        self.assert_both_symbols_running_without_order_writes()
        identity = self.ledger_identity()
        self.assertEqual(identity["ETHUSDT"], eth_identity)
        self.assertEqual(D(self.engine.strategy_ledgers["BTCUSDT"]["wallet"]), D("100"))
        self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
        for restart in range(2):
            with self.subTest(restart=restart):
                self.advance(31)
                self.engine = self.new_engine(restore=True)
                self.engine.tick()
                self.assert_both_symbols_running_without_order_writes()
                self.assertEqual(self.ledger_identity(), identity)
                self.assertEqual(D(self.engine.strategy_ledgers["BTCUSDT"]["wallet"]), D("100"))
                self.assertEqual(D(self.engine.strategy_ledgers["ETHUSDT"]["wallet"]), D("98.90"))
                self.assertEqual(len(self.store.fills("ETHUSDT")), 2)

    def test_pause_cancels_new_symbol_activation_before_any_missing_book_is_initialized(self):
        self.saved_running_eth_book()
        records = self.store.meta(LEDGER_META)
        records.pop("BTCUSDT")
        self.store.set_meta(LEDGER_META, records)
        eth_identity = self.ledger_identity()["ETHUSDT"]
        self.engine.set_symbols({"BTCUSDT": True, "ETHUSDT": True})
        self.assertTrue(self.store.meta(START_REQUEST_META)["pending"])
        self.engine.pause()
        self.advance(31)
        self.engine.tick()
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.engine.state, "paused")
        self.assertIsNone(self.store.meta(START_REQUEST_META))
        self.assertNotIn("BTCUSDT", self.store.meta(LEDGER_META))
        self.assertEqual(self.ledger_identity()["ETHUSDT"], eth_identity)
        self.engine = self.new_engine(restore=True)
        self.engine.tick()
        self.assertFalse(self.engine.desired_running)
        self.assertNotIn("BTCUSDT", self.store.meta(LEDGER_META))
        self.assertEqual(self.client.writes(), [])

    @staticmethod
    def account_snapshot_contents(snapshot):
        value = copy.deepcopy(snapshot)
        value.pop("age_seconds", None)
        return value

    def test_account_snapshot_is_absent_until_a_complete_successful_reconciliation(self):
        self.assertIsNone(self.engine.snapshot()["account_snapshot"])
        self.assertFalse(self.engine.reconciled)
        with patch.object(self.client, "positions",
                          side_effect=self.client.error("network_read_failure", seconds=600)):
            with self.assertRaises(ApiError):
                self.engine.reconcile()
        self.assertIsNone(self.engine.snapshot()["account_snapshot"])
        self.assertFalse(self.engine.reconciled)
        self.assertIsNone(self.engine.last_successful_reconcile_ms)
        self.engine.reconcile()
        value = self.engine.snapshot()["account_snapshot"]
        self.assertIsInstance(value, dict)
        self.assertEqual(value["account"]["totalWalletBalance"], "1000")
        self.assertEqual(value["positions"], [])
        self.assertEqual(value["open_orders"], [])
        self.assertEqual(value["open_algo_orders"], [])
        self.assertEqual(value["age_seconds"], 0)
        self.assertTrue(self.engine.reconciled)
        self.assertEqual(self.client.writes(), [])

    def test_successful_account_snapshot_deeply_freezes_raw_zero_positions_and_hosted_stop(self):
        self.engine.reconcile()
        self.owned_position()
        source_account = dict(self.client.account_value,
            assets=[{"asset": "USDT", "walletBalance": "1000"}])
        source_positions = copy.deepcopy(self.client.position_rows) + [
            {"symbol": "BTCUSDT", "positionSide": "BOTH", "positionAmt": "0",
             "entryPrice": "0", "unRealizedProfit": "0"}]
        source_algos = [copy.deepcopy(self.client.algo_rows[400])]
        with patch.object(self.client, "account", return_value=source_account), \
             patch.object(self.client, "positions", return_value=source_positions), \
             patch.object(self.client, "open_algos", return_value=source_algos):
            self.engine.reconcile()
        self.assertTrue(self.engine.reconciled)
        expected = self.account_snapshot_contents(self.engine.snapshot()["account_snapshot"])
        self.assertEqual(len(expected["positions"]), 2)
        self.assertEqual(expected["positions"][1]["positionAmt"], "0")
        self.assertEqual(expected["open_algo_orders"][0]["algoId"], 400)
        source_account["assets"][0]["walletBalance"] = "9000"
        source_positions[0]["positionAmt"] = "99"
        source_positions[1]["positionAmt"] = "88"
        source_algos[0]["triggerPrice"] = "1"
        returned = self.engine.snapshot()["account_snapshot"]
        self.assertEqual(self.account_snapshot_contents(returned), expected)
        returned["account"]["assets"][0]["walletBalance"] = "8000"
        returned["positions"][0]["positionAmt"] = "77"
        returned["open_algo_orders"][0]["triggerPrice"] = "2"
        self.assertEqual(self.account_snapshot_contents(self.engine.snapshot()["account_snapshot"]),
                         expected)
        self.assertEqual(self.client.writes(), [])

    def test_failure_at_any_account_read_retains_only_the_previous_complete_snapshot(self):
        self.engine.reconcile()
        expected = self.account_snapshot_contents(self.engine.snapshot()["account_snapshot"])
        successful_at = self.engine.last_successful_reconcile_ms
        for index, method in enumerate(("account", "open_orders", "open_algos", "positions"), 1):
            with self.subTest(failed_read=method):
                self.client.account_value["totalWalletBalance"] = str(2000 + index)
                self.advance(5)
                error = self.client.error("network_read_failure", seconds=600)
                with patch.object(self.client, method, side_effect=error):
                    with self.assertRaises(ApiError):
                        self.engine.reconcile()
                state = self.engine.snapshot()
                self.assertFalse(state["reconciled"])
                self.assertEqual(self.account_snapshot_contents(state["account_snapshot"]), expected)
                self.assertEqual(state["account_snapshot"]["age_seconds"], index * 5)
                self.assertEqual(state["last_successful_reconcile_ms"], successful_at)
        self.assertEqual(self.client.writes(), [])

    def test_unregistered_open_order_prevents_publishing_a_new_account_snapshot(self):
        self.engine.reconcile()
        expected = self.account_snapshot_contents(self.engine.snapshot()["account_snapshot"])
        successful_at = self.engine.last_successful_reconcile_ms
        self.advance(10)
        self.client.account_value["totalWalletBalance"] = "2345"
        self.client.order_rows[999] = {"symbol": "ETHUSDT", "orderId": 999,
            "clientOrderId": "unregistered", "status": "NEW", "executedQty": "0"}
        self.engine.reconcile()
        state = self.engine.snapshot()
        self.assertFalse(state["reconciled"])
        self.assertIsNotNone(state["fault_reason"])
        self.assertEqual(self.account_snapshot_contents(state["account_snapshot"]), expected)
        self.assertEqual(state["account_snapshot"]["open_orders"], [])
        self.assertEqual(state["last_successful_reconcile_ms"], successful_at)
        self.assertEqual(state["account_snapshot"]["age_seconds"], 10)
        self.assertEqual(self.client.writes(), [])

    def test_account_snapshot_age_uses_monotonic_time_and_exposes_expiry(self):
        self.engine.reconcile()
        recorded = self.engine.snapshot()["account_snapshot"]["reconciled_at_ms"]
        self.advance(44)
        self.assertLess(self.engine.snapshot()["account_snapshot"]["age_seconds"], 45)
        self.client.now += 7200 * 1000
        self.assertEqual(self.engine.snapshot()["account_snapshot"]["age_seconds"], 44)
        self.client.now -= 14400 * 1000
        self.assertEqual(self.engine.snapshot()["account_snapshot"]["age_seconds"], 44)
        self.monotonic += 2
        state = self.engine.snapshot()
        self.assertGreater(state["account_snapshot"]["age_seconds"], 45)
        self.assertEqual(state["account_snapshot"]["age_seconds"], 46)
        self.assertEqual(state["account_snapshot"]["reconciled_at_ms"], recorded)
        self.client.account_value["totalWalletBalance"] = "999"
        self.engine.reconcile()
        fresh = self.engine.snapshot()["account_snapshot"]
        self.assertEqual(fresh["age_seconds"], 0)
        self.assertEqual(fresh["account"]["totalWalletBalance"], "999")
        self.assertEqual(fresh["reconciled_at_ms"], self.client.now)
        self.assertEqual(self.client.writes(), [])

    def test_restart_does_not_relabel_an_old_account_snapshot_as_a_new_read(self):
        self.engine.reconcile()
        self.assertIsNotNone(self.engine.snapshot()["account_snapshot"])
        self.advance(44)
        self.engine = self.new_engine()
        self.assertIsNone(self.engine.snapshot()["account_snapshot"])
        self.assertFalse(self.engine.reconciled)
        self.engine.reconcile()
        self.assertEqual(self.engine.snapshot()["account_snapshot"]["age_seconds"], 0)
        self.assertEqual(self.client.writes(), [])


if __name__ == "__main__":
    unittest.main()
