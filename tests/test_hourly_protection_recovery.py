"""Offline regressions for late conditional children and confirmed-fill protection.

Every exchange operation below uses FakeExchange. This suite has its own fixture,
so importing helpers does not inherit or repeat the existing lifecycle tests.
"""
import copy
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from execution.binance import ApiError, OrderUncertain
from execution.engine import Engine, ExecutionFault, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from execution.strategy import Analysis, HOUR_MS, Signal
from test_hourly_engine import FakeExchange, fill_row, trade_row


class HourlyProtectionRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Store(Path(self.temporary.name) / "offline-protection.sqlite")
        self.client = FakeExchange()
        self.store.set_meta(LEDGER_META, {
            symbol: {
                "strategy": STRATEGY,
                "version": STRATEGY_VERSION,
                "epoch": "fixture-ledger-" + symbol,
                "initialized_at_ms": 0,
                "activation_boundary_ms": 0,
                "initial_wallet": "100",
                "wallet": "100",
            }
            for symbol in ("BTCUSDT", "ETHUSDT")
        })
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        now = self.client.now
        self.client.now -= HOUR_MS
        self.engine.set_symbols({"BTCUSDT": True, "ETHUSDT": True})
        self.client.now = now
        self.engine.state = "running"
        self.engine.refresh_rules()
        self.engine.algo_map = {}
        self.client.calls.clear()

    def calls(self, method):
        return [call for call in self.client.calls if call[0] == method]

    def pending_entry(self, *, status="PARTIALLY_FILLED", executed="1", average="100"):
        trade = trade_row(quantity="0", average="0")
        self.store.save_trade(trade)
        raw = {
            "symbol": "BTCUSDT", "orderId": 101, "clientOrderId": "offline-entry",
            "status": status, "executedQty": executed, "origQty": "3", "avgPrice": average,
        }
        self.client.order_rows[101] = copy.deepcopy(raw)
        intent = {
            "id": "offline-entry", "client_id": "offline-entry", "trade_id": trade["id"],
            "symbol": "BTCUSDT", "kind": "market", "purpose": "entry", "state": "confirmed",
            "order_id": 101, "quantity": "3", "fills_complete": False,
            "time_ms": self.client.now, "raw": raw,
        }
        self.store.save_order(intent)
        return trade, intent

    def finished_stop(self, *, status="FINISHED"):
        first = 1000 * HOUR_MS + 2000
        trade = trade_row(status="open", quantity="1", average="100", first_fill_ms=first)
        self.store.save_trade(trade)
        self.store.claim_fill("BTCUSDT", fill_row(1, 101, "1", "100", first))
        raw = self.client.stop_row(400, "offline-stop", "90", status=status)
        self.client.algo_rows[400] = copy.deepcopy(raw)
        intent = {
            "id": "offline-stop", "client_id": "offline-stop", "trade_id": trade["id"],
            "symbol": "BTCUSDT", "kind": "stop", "purpose": "protection", "state": "terminal",
            "order_id": 400, "time_ms": self.client.now, "raw": copy.deepcopy(raw),
        }
        self.store.save_order(intent)
        return trade, intent, first

    def ack_then_fill(self, row, symbol, side, quantity, reduce_only):
        self.assertFalse(reduce_only)
        # The original ACK remains NEW/zero. A later query sees the fill.
        self.client.order_rows[row["orderId"]] = dict(
            row, status="FILLED", executedQty=str(quantity), avgPrice="100",
        )
        self.client.fill_rows[row["orderId"]] = [fill_row(
            10, row["orderId"], quantity, "100", self.client.now - 1000,
            fee=str(D(quantity) * D("100") * D(".0005")),
        )]
        self.client.position_rows = [{
            "symbol": symbol, "positionSide": "BOTH", "positionAmt": str(quantity),
            "entryPrice": "100",
        }]

    def positions_unavailable_after_submit(self):
        self.client._call("positions")
        if self.client.order_rows:
            raise ApiError(None, "offline positions GET unavailable")
        return []

    def test_finished_late_child_id_recovers_exit_fills_and_closure_after_reboot(self):
        trade, _, first = self.finished_stop()
        self.engine.state = "stopped"
        self.engine.reconcile()
        self.assertTrue(self.calls("query_algo"))
        self.assertIsNone(self.store.order("child-BTCUSDT-400"))
        self.assertFalse(self.engine.reconciled)

        closed = 1001 * HOUR_MS + 1500
        self.client.algo_rows[400]["actualOrderId"] = "900"
        self.client.order_rows[900] = {
            "symbol": "BTCUSDT", "orderId": 900, "clientOrderId": "offline-child",
            "status": "FILLED", "executedQty": "1", "origQty": "1", "avgPrice": "90",
        }
        self.client.fill_rows[900] = [fill_row(
            2, 900, "1", "90", closed, purpose="stop_exit", fee=".045", pnl="-10",
        )]
        reboot = Engine(self.client, self.store, activation_check=lambda: True)
        reboot.refresh_rules()
        self.client.calls.clear()
        reboot.reconcile()
        reboot.reconcile()  # Immutable fill IDs must keep repeated recovery idempotent.

        child = self.store.order("child-BTCUSDT-400")
        self.assertIsNotNone(child)
        self.assertEqual(child["order_id"], 900)
        self.assertEqual(child["purpose"], "stop_exit")
        self.assertTrue(child["fills_complete"])
        actual = self.store.trade(trade["id"])
        self.assertEqual(actual["status"], "closed")
        self.assertEqual(D(actual["fill_quantity"]), D("0"))
        self.assertEqual(actual["first_fill_ms"], first)
        self.assertEqual(actual["closed_ms"], closed)
        self.assertEqual(D(actual["actual_fees"]), D(".095"))
        self.assertEqual(D(actual["actual_cost"]), D("10.095"))
        self.assertEqual(len(self.store.fills("BTCUSDT")), 2)
        self.assertTrue(reboot.reconciled)
        self.assertEqual(reboot.state, "stopped")
        self.assertEqual(self.client.writes(), [])

    def test_missing_finished_child_blocks_other_symbol_entry_and_stays_queryable(self):
        _, intent, _ = self.finished_stop()
        self.engine.reconciled = True
        self.assertIn(intent["id"], [order["id"] for order in self.engine._blocked_orders()])
        for _ in range(2):
            self.engine._poll_owned_orders()
        self.assertEqual(len(self.calls("query_algo")), 2)
        self.assertIsNone(self.store.order("child-BTCUSDT-400"))

        signal = Signal(
            1000 * HOUR_MS, 1001 * HOUR_MS, 1,
            D("1"), D("100"), D("99"), D("99"), D("98"),
        )
        self.engine._entry("ETHUSDT", signal, True)
        self.assertEqual(self.calls("market_order"), [])

    def test_canceled_terminal_stop_does_not_resume_historical_polling(self):
        _, intent, _ = self.finished_stop(status="CANCELED")
        self.client.calls.clear()
        self.engine._query_intent(intent)
        self.engine._poll_owned_orders()
        self.assertEqual(self.calls("query_algo"), [])
        self.assertNotIn(intent["id"], [order["id"] for order in self.engine._blocked_orders()])
        self.assertEqual(self.client.writes(), [])

    def test_ack_confirmed_fills_protect_before_persistent_position_read_failure(self):
        self.client.market_hook = self.ack_then_fill
        self.engine.reconciled = True
        signal = Signal(
            1000 * HOUR_MS, 1001 * HOUR_MS, 1,
            D("1"), D("100"), D("99"), D("99"), D("98"),
        )
        with patch.object(self.client, "positions", side_effect=self.positions_unavailable_after_submit):
            with self.assertRaises(ApiError):
                self.engine._entry("BTCUSDT", signal, True)

        actual = self.store.trades(True)[0]
        self.assertTrue(self.store.fills("BTCUSDT"))
        self.assertEqual(D(actual["quantity"]), D("0"))
        self.assertEqual(D(actual["average"]), D("0"))
        self.assertEqual(self.engine.positions, {})
        self.assertFalse(self.engine.reconciled)
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(D(actual["confirmed_stop"]), D("95"))
        self.assertEqual(actual["first_fill_ms"], self.client.now - 1000)
        names = [call[0] for call in self.client.calls]
        positions_after_entry = next(
            index for index, name in enumerate(names)
            if name == "positions" and index > names.index("market_order")
        )
        self.assertLess(names.index("new_stop"), positions_after_entry)
        self.assertEqual(len(self.calls("market_order")), 1)

    def test_confirmed_order_average_protects_while_fill_records_are_late(self):
        trade, intent = self.pending_entry(status="FILLED")
        self.engine.algo_map = {}
        with patch.object(self.client, "fills_all", side_effect=ApiError(None, "offline fees delayed")):
            with self.assertRaises(ExecutionFault):
                self.engine._query_intent(intent)
        actual = self.store.trade(trade["id"])
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(D(actual["confirmed_stop"]), D("95"))
        self.assertEqual(D(actual["quantity"]), D("0"))
        self.assertEqual(self.engine.positions, {})
        self.assertIsNone(actual["first_fill_ms"])
        self.assertFalse(self.store.order(intent["id"])["fills_complete"])
        self.assertEqual(self.calls("market_order"), [])

    def test_late_partial_fills_preserve_first_time_and_never_widen_protection(self):
        trade, intent = self.pending_entry()
        first = self.client.now - 1000
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", first)]
        self.engine._query_intent(intent)
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D("95"))

        self.client.order_rows[101].update(executedQty="2", avgPrice="99.5")
        self.client.fill_rows[101].append(fill_row(2, 101, "1", "99", first + 500))
        self.engine._query_intent(self.store.order(intent["id"]))
        actual = self.store.trade(trade["id"])
        self.assertEqual(D(actual["fill_quantity"]), D("2"))
        self.assertEqual(D(actual["fill_average"]), D("99.5"))
        self.assertEqual(D(actual["confirmed_stop"]), D("95"))
        self.assertEqual(actual["first_fill_ms"], first)
        self.assertEqual(len(self.calls("new_stop")), 1)

        self.client.order_rows[101].update(status="FILLED", executedQty="3", avgPrice="103")
        self.client.fill_rows[101].append(fill_row(3, 101, "1", "110", first + 1000))
        self.engine._query_intent(self.store.order(intent["id"]))
        actual = self.store.trade(trade["id"])
        self.assertEqual(D(actual["fill_quantity"]), D("3"))
        self.assertEqual(D(actual["fill_average"]), D("103"))
        self.assertEqual(D(actual["confirmed_stop"]), D("98"))
        self.assertEqual(actual["first_fill_ms"], first)
        self.assertEqual(len(self.store.fills("BTCUSDT")), 3)
        self.assertEqual(D(actual["quantity"]), D("0"))
        self.assertEqual(self.engine.positions, {})
        self.assertEqual(len(self.calls("new_stop")), 2)

    def test_stopped_engine_confirmed_fills_remain_get_only_even_without_positions(self):
        _, _ = self.pending_entry(status="FILLED")
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", self.client.now - 1000)]
        self.engine.state = "stopped"
        self.client.calls.clear()
        with patch.object(self.client, "positions", side_effect=ApiError(None, "offline positions GET unavailable")):
            self.engine.tick()
        self.assertTrue(self.calls("query_order"))
        self.assertTrue(self.store.fills("BTCUSDT"))
        self.assertEqual(self.engine.state, "stopped")
        self.assertEqual(self.client.writes(), [])

    def prepare_fresh_ack_entry(self, *, uncertain_exit=False):
        """Use the real tick/entry path; only the exchange transport is fake."""
        boundary = self.client.now // HOUR_MS * HOUR_MS
        self.engine.last_boundary = boundary - HOUR_MS
        signal = Signal(
            boundary - HOUR_MS, boundary, 1,
            D("1"), D("100"), D("99"), D("99"), D("98"),
        )
        self.engine.analyses = {
            "BTCUSDT": Analysis(True, "", 1001, D("99"), D("1"), signal),
            "ETHUSDT": Analysis(True, "", 1001, D("99"), D("1")),
        }
        self.client.stop_error = ApiError(-2010, "offline first stop rejected")

        def entry_hook(row, symbol, side, quantity, reduce_only):
            self.ack_then_fill(row, symbol, side, quantity, reduce_only)
            self.client.market_hook = None
            if uncertain_exit:
                self.client.market_error = OrderUncertain(None, "offline exit response lost")

        self.client.market_hook = entry_hook
        return boundary

    def test_fresh_tick_ack_positions_failure_and_rejected_stop_emergency_closes_once(self):
        boundary = self.prepare_fresh_ack_entry()
        with patch.object(self.client, "positions", side_effect=self.positions_unavailable_after_submit), \
                patch.object(self.engine, "refresh_candles"):
            self.engine.tick()
            # A failing position read leaves this boundary unconsumed. The next
            # fresh tick must query the existing emergency order, not recreate it.
            self.assertEqual(self.engine.last_boundary, boundary - HOUR_MS)
            self.client.now += 2000
            self.engine.tick()

        markets = self.calls("market_order")
        self.assertEqual(len(markets), 2)
        self.assertFalse(markets[0][2]["reduce_only"])
        self.assertTrue(markets[1][2]["reduce_only"])
        self.assertEqual(markets[1][1][1], "SELL")
        self.assertEqual(D(markets[1][1][2]), D(markets[0][1][2]))
        exits = [order for order in self.store.orders() if order["purpose"] == "exit"]
        self.assertEqual(len(exits), 1)
        self.assertTrue(any(call[2]["client_id"] == exits[0]["client_id"]
                            for call in self.calls("query_order")))
        actual = self.store.trades(True)[0]
        self.assertTrue(actual["exit_intent"])
        self.assertTrue(actual["protection_pending_position"])
        self.assertEqual(D(actual["quantity"]), D("0"))
        self.assertEqual(D(actual["average"]), D("0"))
        self.assertEqual(self.engine.positions, {})
        self.assertFalse(self.engine.reconciled)
        self.assertEqual(self.engine.state, "fault")

    def test_fresh_tick_uncertain_emergency_exit_queries_original_without_resubmission(self):
        self.prepare_fresh_ack_entry(uncertain_exit=True)
        with patch.object(self.client, "positions", side_effect=self.positions_unavailable_after_submit), \
                patch.object(self.engine, "refresh_candles"):
            for _ in range(3):
                self.engine.tick()
                self.client.now += 2000

        markets = self.calls("market_order")
        self.assertEqual(len(markets), 2)  # One entry and one uncertain exit wire attempt.
        self.assertTrue(markets[-1][2]["reduce_only"])
        exits = [order for order in self.store.orders() if order["purpose"] == "exit"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["state"], "unknown")
        self.assertIsNone(exits[0].get("order_id"))
        original_queries = [call for call in self.calls("query_order")
                            if call[2]["client_id"] == exits[0]["client_id"]]
        self.assertGreaterEqual(len(original_queries), 2)
        self.assertTrue(all(call[2]["order_id"] is None for call in original_queries))
        self.assertFalse(self.engine.reconciled)

    def test_confirmed_exit_execution_deducted_before_late_fees_no_second_exit(self):
        trade, intent = self.pending_entry(status="FILLED")
        first = self.client.now - 1000
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", first)]
        self.engine._query_intent(intent)
        trade = self.store.trade(trade["id"])
        self.engine._exit_intent(trade, "offline protective exit already executed")
        raw_exit = {
            "symbol": "BTCUSDT", "orderId": 102, "clientOrderId": "offline-exit",
            "status": "FILLED", "executedQty": "1", "origQty": "1", "avgPrice": "90",
        }
        self.client.order_rows[102] = copy.deepcopy(raw_exit)
        self.store.save_order({
            "id": "offline-exit", "client_id": "offline-exit", "trade_id": trade["id"],
            "symbol": "BTCUSDT", "kind": "market", "purpose": "exit", "state": "terminal",
            "order_id": 102, "quantity": "1", "fills_complete": False,
            "time_ms": self.client.now, "raw": raw_exit,
        })
        self.engine.last_boundary -= HOUR_MS
        with patch.object(self.client, "positions", side_effect=self.positions_unavailable_after_submit):
            for _ in range(2):
                self.engine.tick()
                self.client.now += 2000
            before_fees = self.store.trade(trade["id"])
            self.assertEqual(D(before_fees["fill_quantity"]), D("1"))
            self.assertEqual(self.engine._confirmed_execution_quantity(before_fees), D("0"))
            self.assertFalse(self.store.order("offline-exit")["fills_complete"])
            self.assertEqual(self.calls("market_order"), [])
            self.client.fill_rows[102] = [fill_row(
                2, 102, "1", "90", first + 500, purpose="exit", fee=".045", pnl="-10",
            )]
            self.engine.tick()

        self.assertTrue(self.store.order("offline-exit")["fills_complete"])
        self.engine._rebuild_fills(self.store.trade(trade["id"]))
        actual = self.store.trade(trade["id"])
        self.assertEqual(D(actual["fill_quantity"]), D("0"))
        self.assertEqual(D(actual["actual_fees"]), D(".095"))
        self.assertEqual(D(actual["actual_cost"]), D("10.095"))
        self.assertEqual(self.calls("market_order"), [])
        self.assertEqual(self.engine.positions, {})

    def test_confirmed_fill_fallback_never_authorizes_ordinary_risk_reduction(self):
        trade, intent = self.pending_entry(status="FILLED")
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", self.client.now - 1000)]
        self.engine._query_intent(intent)
        actual = self.store.trade(trade["id"])
        actual.update(budget0=".001", notional0=".001")
        self.store.save_trade(actual)
        self.engine._risk_and_exit()
        self.engine._recover_confirmed_execution()
        self.assertEqual(self.calls("market_order"), [])

        # Even a cached nonzero trade quantity cannot authorize a normal risk
        # resize without a matching, freshly confirmed real position.
        actual = self.store.trade(trade["id"])
        actual.update(quantity="1", average="100")
        self.store.save_trade(actual)
        self.engine._risk_and_exit()
        self.engine._recover_confirmed_execution()
        actual = self.store.trade(trade["id"])
        self.assertIsNone(actual["exit_intent"])
        self.assertEqual(self.calls("market_order"), [])
        self.assertEqual(self.engine.positions, {})
        self.assertEqual(self.engine._confirmed_execution_quantity(actual), D("1"))

    def test_exit_fills_arrive_first_entry_query_still_fetches_and_repairs_accounting(self):
        trade, intent = self.pending_entry(status="FILLED")
        self.engine.state = "stopped"
        first = self.client.now - 1000
        self.store.claim_fill("BTCUSDT", fill_row(
            2, 102, "1", "90", first + 500, purpose="exit", fee=".045", pnl="-10",
        ))
        self.client.fill_rows[101] = [fill_row(1, 101, "1", "100", first)]
        self.engine._query_intent(intent)
        actual = self.store.trade(trade["id"])
        self.assertTrue(any(call[2]["order_id"] == 101 for call in self.calls("fills_all")))
        self.assertTrue(self.store.order(intent["id"])["fills_complete"])
        self.assertEqual(actual["first_fill_ms"], first)
        self.assertEqual(D(actual["fill_quantity"]), D("0"))
        self.assertEqual(D(actual["actual_fees"]), D(".095"))
        self.assertEqual(D(actual["actual_cost"]), D("10.095"))
        self.assertEqual(len(self.store.fills("BTCUSDT")), 2)
        self.assertEqual(self.client.writes(), [])


if __name__ == "__main__":
    unittest.main()
