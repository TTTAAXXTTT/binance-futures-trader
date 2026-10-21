"""Offline regressions for reading a newly accepted conditional protection order.

Run from the source tree under review, for example:
    python -B -m unittest discover -s ../server-updates/20261011/algo-review \
        -p test_hourly_algo_confirmation.py -v

All exchange operations use FakeExchange; SQLite files live in TemporaryDirectory.
Importing this file never reads credentials or starts a service.
"""
import copy
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from unittest.mock import patch

ROOT = Path.cwd()
if not (ROOT / "execution" / "engine.py").is_file():
    ROOT = Path(__file__).resolve().parents[1]
if not (ROOT / "execution" / "engine.py").is_file():
    raise RuntimeError("Run this offline test from the btc-eth-trader source directory")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from execution.binance import ApiError
from execution.engine import Engine, ExecutionFault, LEDGER_META, STRATEGY, STRATEGY_VERSION
from execution.store import Store
from test_hourly_engine import FakeExchange, trade_row


class HourlyAlgoConfirmationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = Store(Path(self.temporary.name) / "offline-algo-confirmation.sqlite")
        self.client = FakeExchange()
        self.store.set_meta(LEDGER_META, {
            symbol: {
                "strategy": STRATEGY, "version": STRATEGY_VERSION,
                "epoch": "fixture-ledger-" + symbol,
                "initialized_at_ms": 0, "activation_boundary_ms": 0,
                "initial_wallet": "100", "wallet": "100",
            }
            for symbol in ("BTCUSDT", "ETHUSDT")
        })
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        self.engine.refresh_rules()
        self.engine.algo_map = {}
        self.client.calls.clear()

    def calls(self, name):
        return [row for row in self.client.calls if row[0] == name]

    def intent(self, *, known_id=True):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        raw = self.client.stop_row(400, "original-stop", "95")
        intent = {
            "id": "original-stop", "client_id": "original-stop",
            "trade_id": trade["id"], "symbol": trade["symbol"],
            "kind": "stop", "purpose": "protection", "state": "confirmed",
            "trigger": "95", "time_ms": self.client.now, "raw": copy.deepcopy(raw),
        }
        if known_id:
            intent["order_id"] = 400
        else:
            intent["raw"] = {}
        self.store.save_order(intent)
        return trade, intent, raw

    def not_found_query(self, algo_id=None, client_id=None):
        self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
        raise ApiError(-2013, "Original conditional order is not visible yet")

    def open_rows(self, rows):
        def read(symbol=None):
            self.client._call("open_algos", symbol=symbol)
            return copy.deepcopy(rows)
        return read

    def assert_read_only(self):
        self.assertEqual(self.client.writes(), [])
        self.assertEqual(self.engine.state, "stopped")

    def test_known_exchange_id_is_first_and_success_does_not_query_client_id(self):
        _, intent, raw = self.intent()
        def read(algo_id=None, client_id=None):
            self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
            if algo_id == 400:
                return copy.deepcopy(raw)
            raise ApiError(-2013, "The alternate lookup is intentionally unavailable")
        with patch.object(self.client, "query_algo", side_effect=read):
            actual = self.engine._query_stop_original(intent)
        self.assertEqual(actual, raw)
        self.assertEqual(len(self.calls("query_algo")), 1)
        self.assertEqual(self.calls("query_algo")[0][2]["algo_id"], 400)
        self.assertEqual(self.calls("open_algos"), [])
        self.assert_read_only()

    def test_only_not_found_on_exchange_id_uses_original_client_id(self):
        _, intent, raw = self.intent()
        def read(algo_id=None, client_id=None):
            self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
            if algo_id is not None:
                raise ApiError(-2013, "Exchange-id read not visible yet")
            return copy.deepcopy(raw)
        with patch.object(self.client, "query_algo", side_effect=read):
            actual = self.engine._query_stop_original(intent)
        self.assertEqual(actual, raw)
        queries = self.calls("query_algo")
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0][2]["algo_id"], 400)
        self.assertEqual(queries[1][2]["client_id"], "original-stop")
        self.assertEqual(self.calls("open_algos"), [])
        self.assert_read_only()

    def test_without_exchange_id_reads_client_id_once(self):
        _, intent, raw = self.intent(known_id=False)
        self.client.algo_rows[400] = copy.deepcopy(raw)
        actual = self.engine._query_stop_original(intent)
        self.assertEqual(actual, raw)
        self.assertEqual(len(self.calls("query_algo")), 1)
        self.assertEqual(self.calls("query_algo")[0][2]["client_id"], "original-stop")
        self.assertEqual(self.calls("open_algos"), [])
        self.assert_read_only()

    def test_two_not_found_reads_can_use_one_unique_open_order(self):
        _, intent, raw = self.intent()
        unrelated = self.client.stop_row(401, "someone-elses-stop", "90")
        with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                patch.object(self.client, "open_algos", side_effect=self.open_rows([unrelated, raw])):
            actual = self.engine._query_stop_original(intent)
        self.assertEqual(actual, raw)
        self.assertEqual(len(self.calls("query_algo")), 2)
        self.assertEqual(len(self.calls("open_algos")), 1)
        self.assert_read_only()

    def test_missing_exchange_id_not_found_can_use_unique_client_identity_in_open_orders(self):
        _, intent, raw = self.intent(known_id=False)
        with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                patch.object(self.client, "open_algos", side_effect=self.open_rows([raw])):
            actual = self.engine._query_stop_original(intent)
        self.assertEqual(actual, raw)
        self.assertEqual(len(self.calls("query_algo")), 1)
        self.assertEqual(len(self.calls("open_algos")), 1)
        self.assert_read_only()

    def test_missing_or_duplicate_open_order_never_proves_original_protection(self):
        for duplicate in (False, True):
            with self.subTest(duplicate=duplicate):
                _, intent, raw = self.intent()
                self.client.calls.clear()
                rows = [raw, copy.deepcopy(raw)] if duplicate else []
                with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                        patch.object(self.client, "open_algos", side_effect=self.open_rows(rows)):
                    with self.assertRaises((ApiError, ExecutionFault)):
                        self.engine._query_stop_original(intent)
                self.assertEqual(len(self.calls("query_algo")), 2)
                self.assertEqual(len(self.calls("open_algos")), 1)
                self.assert_read_only()

    def test_same_algo_number_on_unrelated_symbol_does_not_shadow_original(self):
        _, intent, raw = self.intent()
        unrelated = self.client.stop_row(400, "other-symbol-stop", "90", symbol="ETHUSDT")
        with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                patch.object(self.client, "open_algos", side_effect=self.open_rows([unrelated, raw])):
            self.assertEqual(self.engine._query_stop_original(intent), raw)
        self.assertEqual(len(self.calls("open_algos")), 1)
        self.assert_read_only()

    def test_open_order_fallback_requires_all_original_identity_fields(self):
        for field, wrong in (("algoId", 999), ("clientAlgoId", "other-stop"),
                             ("symbol", "ETHUSDT")):
            for missing in (False, True):
                with self.subTest(field=field, missing=missing):
                    _, intent, raw = self.intent()
                    if missing:
                        raw.pop(field)
                    else:
                        raw[field] = wrong
                    self.client.calls.clear()
                    with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                            patch.object(self.client, "open_algos", side_effect=self.open_rows([raw])):
                        with self.assertRaises((ApiError, ExecutionFault)):
                            self.engine._query_stop_original(intent)
                    self.assertEqual(len(self.calls("open_algos")), 1)
                    self.assert_read_only()

    def test_successful_direct_read_with_wrong_identity_is_rejected_without_fallback(self):
        for field, wrong in (("algoId", 999), ("clientAlgoId", "other-stop"),
                             ("symbol", "ETHUSDT")):
            with self.subTest(field=field):
                _, intent, raw = self.intent()
                raw[field] = wrong
                self.client.calls.clear()
                def read(algo_id=None, client_id=None):
                    self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
                    return copy.deepcopy(raw)
                with patch.object(self.client, "query_algo", side_effect=read):
                    with self.assertRaises((ApiError, ExecutionFault)):
                        self.engine._query_stop_original(intent)
                self.assertEqual(len(self.calls("query_algo")), 1)
                self.assertEqual(self.calls("open_algos"), [])
                self.assert_read_only()

    def test_non_not_found_first_read_errors_never_trigger_alternate_reads(self):
        errors = [ApiError(None, "offline TLS failure"),
                  ApiError(-1003, "offline throttled", status=429),
                  ApiError(-2014, "offline permission failure", status=401),
                  ApiError(-1007, "offline timeout", status=503),
                  TimeoutError("offline read timeout")]
        for error in errors:
            with self.subTest(error=str(error)):
                _, intent, _ = self.intent()
                self.client.calls.clear()
                def read(algo_id=None, client_id=None):
                    self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
                    raise error
                with patch.object(self.client, "query_algo", side_effect=read):
                    with self.assertRaises(type(error)):
                        self.engine._query_stop_original(intent)
                self.assertEqual(len(self.calls("query_algo")), 1)
                self.assertEqual(self.calls("open_algos"), [])
                self.assert_read_only()

    def test_non_not_found_second_read_error_does_not_reach_open_orders(self):
        _, intent, _ = self.intent()
        def read(algo_id=None, client_id=None):
            self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
            if algo_id is not None:
                raise ApiError(-2013, "Original lookup unavailable")
            raise ApiError(-1003, "offline cooldown", status=429)
        with patch.object(self.client, "query_algo", side_effect=read):
            with self.assertRaises(ApiError) as caught:
                self.engine._query_stop_original(intent)
        self.assertEqual(caught.exception.code, -1003)
        self.assertEqual(len(self.calls("query_algo")), 2)
        self.assertEqual(self.calls("open_algos"), [])
        self.assert_read_only()

    def test_query_intent_returns_successful_get_result_and_records_query(self):
        _, intent, raw = self.intent()
        self.client.algo_rows[400] = copy.deepcopy(raw)
        actual = self.engine._query_intent(intent)
        self.assertEqual(actual, raw)
        saved = self.store.order(intent["id"])
        self.assertEqual(saved["raw"], raw)
        self.assertEqual(saved["last_query_ms"], self.client.now)
        self.assertEqual(len(self.calls("query_algo")), 1)
        self.assert_read_only()

    def test_new_protection_uses_one_successful_get_and_never_replays_creation(self):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        self.assertTrue(self.engine._ensure_stop(trade, D("95")))
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(len(self.calls("query_algo")), 1)
        created_id = next(iter(self.client.algo_rows))
        self.assertEqual(self.calls("query_algo")[0][2]["algo_id"], created_id)
        self.assertEqual(self.calls("open_algos"), [])
        self.assertEqual(self.calls("market_order"), [])
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D("95"))
        self.assertEqual(self.engine.state, "stopped")

    def test_failed_confirmation_is_bounded_and_keeps_durable_exit_intent(self):
        self.engine.state = "running"
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        with patch.object(self.client, "query_algo", side_effect=self.not_found_query), \
                patch.object(self.client, "open_algos", side_effect=self.open_rows([])):
            self.assertFalse(self.engine._ensure_stop(trade, D("95")))
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(len(self.calls("query_algo")), 2)
        self.assertEqual(len(self.calls("open_algos")), 1)
        actual = self.store.trade(trade["id"])
        self.assertIsNone(actual["confirmed_stop"])
        self.assertIsNotNone(actual["exit_intent"])
        self.assertEqual(self.engine.state, "fault")
        self.assertEqual(self.calls("market_order"), [])

    def test_wrong_protection_fields_and_non_finite_triggers_are_never_confirmed(self):
        defects = {
            "algoStatus": "EXPIRED", "orderType": "LIMIT", "side": "BUY",
            "positionSide": "LONG", "workingType": "MARK_PRICE",
            "closePosition": False, "priceProtect": True,
            "triggerPrice": "94", "algoType": "OTHER",
        }
        cases = list(defects.items()) + [("triggerPrice", value)
            for value in ("0", "NaN", "Infinity", "not-a-price")]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.setUp()
                trade = trade_row(status="open", quantity="1", average="100")
                self.store.save_trade(trade)
                original = self.client.query_algo
                def corrupt(algo_id=None, client_id=None):
                    row = original(algo_id=algo_id, client_id=client_id)
                    row[field] = value
                    return row
                with patch.object(self.client, "query_algo", side_effect=corrupt):
                    self.assertFalse(self.engine._ensure_stop(trade, D("95")))
                actual = self.store.trade(trade["id"])
                self.assertIsNone(actual["confirmed_stop"])
                self.assertIsNotNone(actual["exit_intent"])
                self.assertEqual(len(self.calls("new_stop")), 1)
                self.assertEqual(self.calls("open_algos"), [])
                self.assertEqual(self.calls("market_order"), [])

    def test_equivalent_numeric_and_boolean_response_representations_remain_valid(self):
        trade = trade_row(status="open", quantity="1", average="100")
        self.store.save_trade(trade)
        original = self.client.query_algo
        def equivalent(algo_id=None, client_id=None):
            row = original(algo_id=algo_id, client_id=client_id)
            row.update(triggerPrice="95.000", closePosition="true", priceProtect="false")
            return row
        with patch.object(self.client, "query_algo", side_effect=equivalent):
            self.assertTrue(self.engine._ensure_stop(trade, D("95")))
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(len(self.calls("query_algo")), 1)
        self.assertEqual(self.engine.state, "stopped")

    def test_failed_replacement_retains_old_confirmed_protection(self):
        trade, _, raw = self.intent()
        self.client.algo_rows[400] = copy.deepcopy(raw)
        self.engine.algo_map[(trade["symbol"], "400")] = copy.deepcopy(raw)
        trade.update(confirmed_stop="95", tightest_stop="95")
        self.store.save_trade(trade)
        def query(algo_id=None, client_id=None):
            self.client._call("query_algo", algo_id=algo_id, client_id=client_id)
            raise ApiError(-2013, "Replacement lookup unavailable")
        with patch.object(self.client, "query_algo", side_effect=query), \
                patch.object(self.client, "open_algos", side_effect=self.open_rows([raw])):
            self.assertFalse(self.engine._ensure_stop(trade, D("98")))
        self.assertEqual(self.calls("cancel_algo"), [])
        self.assertEqual(self.client.algo_rows[400]["algoStatus"], "NEW")
        self.assertEqual(D(self.store.trade(trade["id"])["confirmed_stop"]), D("95"))
        self.assertIsNone(self.store.trade(trade["id"])["exit_intent"])
        self.assertEqual(len(self.calls("new_stop")), 1)
        self.assertEqual(self.calls("market_order"), [])

    def test_finished_original_order_still_discovers_late_market_child(self):
        _, intent, raw = self.intent()
        raw.update(algoStatus="FINISHED", actualOrderId="0")
        intent.update(state="terminal", raw=copy.deepcopy(raw))
        self.store.save_order(intent)
        self.client.algo_rows[400] = copy.deepcopy(raw)
        self.engine._query_intent(intent)
        self.assertIsNone(self.store.order("child-BTCUSDT-400"))
        self.client.algo_rows[400]["actualOrderId"] = "900"
        self.engine._query_intent(self.store.order(intent["id"]))
        child = self.store.order("child-BTCUSDT-400")
        self.assertIsNotNone(child)
        self.assertEqual(child["order_id"], 900)
        self.assertEqual(child["purpose"], "stop_exit")
        self.assertEqual(len(self.calls("query_algo")), 2)
        self.assert_read_only()


if __name__ == "__main__":
    unittest.main()
