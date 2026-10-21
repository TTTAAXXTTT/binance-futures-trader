"""Worker RPC tests with temporary records and a fake engine only."""
import asyncio
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "runtime" / "packages"), str(ROOT)]

from fastapi.testclient import TestClient
import hourly_worker
from hourly_worker import STRATEGY, activation_check, build_app


class FakeEngine:
    def __init__(self):
        self.state = "stopped"
        self.tick_calls = 0
        self.calls = []
        self.error = None
        self.desired_running = False
        self.enabled_symbols = {"BTCUSDT": True, "ETHUSDT": False}
        self.start_requests = []

    def tick(self):
        self.tick_calls += 1
        if self.error:
            raise self.error

    def snapshot(self):
        return {"strategy": STRATEGY, "state": self.state, "trades": [],
                "desired_running": self.desired_running,
                "history": [], "unknown_orders": [], "warmed_up": True,
                "enabled_symbols": dict(self.enabled_symbols)}

    def start(self, symbols=None):
        self.start_requests.append(dict(symbols) if symbols is not None else None)
        if symbols is not None:
            self.enabled_symbols = dict(symbols)
        self.calls.append("start")
        self.desired_running = True
        self.state = "running"
        return {"message": "已启动", "state": self.state}

    def set_symbols(self, symbols):
        self.calls.append("symbols")
        self.enabled_symbols = dict(symbols)
        return {"message": "交易品种选择已保存", "enabled_symbols": dict(self.enabled_symbols)}

    def pause(self):
        self.calls.append("pause")
        self.desired_running = False
        self.state = "paused"
        return {"state": self.state}

    def exit_all(self):
        self.calls.append("exit-all")
        self.desired_running = False
        self.state = "paused"
        return {"message": "退出意图已保存"}

    def handle_loop_failure(self, exc):
        self.calls.append('loop-failure')
        if self.desired_running:
            self.state = 'recovering'


class WorkerRPCChecks(unittest.TestCase):
    def setUp(self):
        self.engine = FakeEngine()
        self.app = build_app(self.engine, "local-user", "local-password")
        self.client = TestClient(self.app)
        self.client.auth = ("local-user", "local-password")

    def test_building_app_is_inert(self):
        self.assertEqual(self.engine.tick_calls, 0)
        self.assertEqual(self.engine.calls, [])
        self.assertEqual(self.engine.state, "stopped")

    def test_get_status_does_not_start_or_tick(self):
        response = self.client.get("/api/status")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["state"], "stopped")
        self.assertEqual(self.engine.calls, [])
        self.assertEqual(self.engine.tick_calls, 0)

    def test_basic_required_for_status_and_all_mutations(self):
        for route, method in (("status", "get"), ("start", "post"), ("symbols", "post"), ("pause", "post"), ("exit-all", "post")):
            with self.subTest(route=route):
                req = getattr(self.client, method)
                response = req("/api/" + route, auth=("wrong", "wrong"),
                               **({"json": {"confirmed": True}} if method == "post" else {}))
                self.assertEqual(response.status_code, 401)
        self.assertEqual(self.engine.calls, [])

    def test_missing_credentials_has_basic_challenge(self):
        response = self.client.get("/api/status", auth=None)
        self.assertEqual(response.status_code, 401)
        self.assertIn("Basic", response.headers["www-authenticate"])

    def test_confirmed_must_be_exact_true_for_all_mutations(self):
        for route in ("start", "symbols", "pause", "exit-all"):
            for flag in (None, False, "true", 1):
                with self.subTest(route=route, flag=flag):
                    response = self.client.post("/api/" + route, json={"confirmed": flag,
                        "symbols": {"BTCUSDT": True, "ETHUSDT": False}})
                    self.assertEqual(response.status_code, 422)
        self.assertEqual(self.engine.calls, [])

    def test_confirmed_routes_call_only_requested_method_once(self):
        for route in ("start", "pause", "exit-all"):
            self.assertEqual(self.client.post("/api/" + route, json={"confirmed": True}).status_code, 200)
        self.assertEqual(self.engine.calls, ["start", "pause", "exit-all"])

    def test_runtime_rejection_is_reviewable_without_second_attempt(self):
        self.engine.start = Mock(side_effect=RuntimeError("订单或持仓尚未核对"))
        response = self.client.post("/api/start", json={"confirmed": True})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], "订单或持仓尚未核对")
        self.engine.start.assert_called_once()

    def test_start_without_selection_keeps_the_current_meta_selection(self):
        self.engine.enabled_symbols = {"BTCUSDT": False, "ETHUSDT": True}
        response = self.client.post("/api/start", json={"confirmed": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.engine.start_requests, [None])
        self.assertEqual(self.engine.enabled_symbols, {"BTCUSDT": False, "ETHUSDT": True})

    def test_start_passes_explicit_full_selection_without_enabling_others(self):
        selection = {"BTCUSDT": False, "ETHUSDT": True}
        self.engine.start = Mock(return_value={"state": "running"})
        response = self.client.post("/api/start", json={"confirmed": True, "symbols": selection})
        self.assertEqual(response.status_code, 200)
        self.engine.start.assert_called_once_with(symbols=selection)

    def test_save_symbols_is_human_confirmed_and_does_not_start_or_tick(self):
        selection = {"BTCUSDT": False, "ETHUSDT": True}
        response = self.client.post("/api/symbols", json={"confirmed": True, "symbols": selection})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["enabled_symbols"], selection)
        self.assertEqual(self.engine.state, "stopped")
        self.assertEqual(self.engine.calls, ["symbols"])
        self.assertEqual(self.engine.tick_calls, 0)

    def test_all_disabled_selection_can_be_saved_without_arming(self):
        selection = {"BTCUSDT": False, "ETHUSDT": False}
        self.assertEqual(self.client.post("/api/symbols", json={"confirmed": True,
            "symbols": selection}).status_code, 200)
        self.assertEqual(self.engine.enabled_symbols, selection)
        self.assertEqual(self.engine.state, "stopped")

    def test_missing_required_selection_is_reviewable_validation_error(self):
        response = self.client.post("/api/symbols", json={"confirmed": True})
        self.assertEqual(response.status_code, 422)
        self.assertIn("完整启用选择", response.json()["detail"])
        self.assertEqual(self.engine.calls, [])

    def test_invalid_selection_rejected_before_any_engine_call_for_both_routes(self):
        invalid = [None, [], {}, {"BTCUSDT": True}, {"ETHUSDT": False},
            {"BTCUSDT": True, "ETHUSDT": False, "SOLUSDT": True},
            {"BTCUSDT": "true", "ETHUSDT": False}, {"BTCUSDT": 1, "ETHUSDT": False},
            {"BTCUSDT": True, "ETHUSDT": None}]
        for route in ("symbols", "start"):
            for selection in invalid:
                with self.subTest(route=route, selection=selection):
                    response = self.client.post("/api/" + route, json={"confirmed": True, "symbols": selection})
                    self.assertEqual(response.status_code, 422)
                    self.assertIsInstance(response.json()["detail"], str)
        self.assertEqual(self.engine.calls, [])
        self.assertEqual(self.engine.start_requests, [])

    def test_engine_selection_validation_is_visible_and_never_retried(self):
        self.engine.set_symbols = Mock(side_effect=ValueError("至少选择一个可交易品种"))
        selection = {"BTCUSDT": False, "ETHUSDT": False}
        response = self.client.post("/api/symbols", json={"confirmed": True, "symbols": selection})
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["detail"], "至少选择一个可交易品种")
        self.engine.set_symbols.assert_called_once_with(symbols=selection)

    def test_engine_start_selection_rejection_is_409_without_retry(self):
        self.engine.start = Mock(side_effect=RuntimeError("请先暂停再改变交易品种"))
        selection = {"BTCUSDT": True, "ETHUSDT": False}
        response = self.client.post("/api/start", json={"confirmed": True, "symbols": selection})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], "请先暂停再改变交易品种")
        self.engine.start.assert_called_once_with(symbols=selection)

    def test_unexpected_failure_has_no_raw_exception_or_retry(self):
        self.engine.exit_all = Mock(side_effect=Exception("sensitive sentinel"))
        response = self.client.post("/api/exit-all", json={"confirmed": True})
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("sensitive sentinel", response.text)
        self.engine.exit_all.assert_called_once()

    def test_lifecycle_ticks_stopped_engine_never_auto_starts(self):
        async def exercise():
            async with self.app.router.lifespan_context(self.app):
                for _ in range(100):
                    if self.engine.tick_calls:
                        break
                    await asyncio.sleep(0.002)
        asyncio.run(exercise())
        self.assertEqual(self.engine.tick_calls, 1)
        self.assertEqual(self.engine.calls, [])
        self.assertEqual(self.engine.state, "stopped")

    def test_tick_exception_preserves_user_intent_and_surfaces_generic_error(self):
        self.engine.state = "running"
        self.engine.desired_running = True
        self.engine.error = ValueError("secret details must not leak")
        async def exercise():
            async with self.app.router.lifespan_context(self.app):
                for _ in range(100):
                    if self.engine.calls:
                        break
                    await asyncio.sleep(0.002)
        asyncio.run(exercise())
        self.assertEqual(self.engine.calls, ["loop-failure"])
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.engine.state, 'recovering')
        snapshot = self.client.get("/api/status").json()
        self.assertIn("loop_error", snapshot)
        self.assertNotIn("secret details", str(snapshot))

    def test_readonly_startup_failure_does_not_promote_stopped_to_paused(self):
        self.engine.error = ValueError("temporary read failure")
        async def exercise():
            async with self.app.router.lifespan_context(self.app):
                for _ in range(100):
                    if self.engine.tick_calls:
                        break
                    await asyncio.sleep(0.002)
        asyncio.run(exercise())
        self.assertEqual(self.engine.state, "stopped")
        self.assertEqual(self.engine.calls, ['loop-failure'])
        self.assertFalse(self.engine.desired_running)
        self.assertIn("loop_error", self.client.get("/api/status").json())

    def test_start_and_tick_are_serialized(self):
        entered = threading.Event()
        release = threading.Event()
        order = []
        def tick():
            order.append("tick-start")
            entered.set()
            release.wait(timeout=2)
            order.append("tick-end")
        self.engine.tick = tick
        original = self.engine.start
        def start(symbols=None):
            order.append("start")
            return original(symbols=symbols)
        self.engine.start = start
        async def exercise():
            async with self.app.router.lifespan_context(self.app):
                await asyncio.to_thread(entered.wait, 1)
                result = []
                caller = threading.Thread(target=lambda: result.append(self.client.post("/api/start", json={"confirmed": True})))
                caller.start()
                await asyncio.sleep(0.02)
                self.assertNotIn("start", order)
                release.set()
                await asyncio.to_thread(caller.join, 1)
                self.assertEqual(result[0].status_code, 200)
        asyncio.run(exercise())
        self.assertEqual(order, ["tick-start", "tick-end", "start"])

    def test_invalid_rpc_settings_rejected_before_lifecycle(self):
        with self.assertRaises(ValueError):
            build_app(self.engine, "", "password")
        with self.assertRaises(ValueError):
            build_app(self.engine, "user", "password", tick_interval=0)
        self.assertEqual(self.engine.calls, [])


class ActivationChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.data = self.root / "user_data"
        self.data.mkdir()
        self.probe = Mock(return_value=False)

    def settings(self, value=None):
        (self.data / "hourly-settings.json").write_text(json.dumps(value if value is not None else
            {"strategy": STRATEGY, "migrated": True}), encoding="utf-8")

    def process(self, value):
        (self.data / "process.json").write_text(json.dumps(value), encoding="utf-8")

    def test_missing_new_settings_never_probes_or_activates(self):
        self.assertFalse(activation_check(self.root, pid_exists=self.probe))
        self.probe.assert_not_called()


    def test_wrong_strategy_or_unconfirmed_new_settings_rejected(self):
        for value in ({"strategy": "AdaptiveFutures", "migrated": True},
                      {"strategy": STRATEGY, "migrated": False},
                      {"strategy": STRATEGY, "migrated": "true"}):
            self.settings(value)
            self.assertFalse(activation_check(self.root, pid_exists=self.probe))
        self.probe.assert_not_called()

    def test_old_pid_alive_is_not_part_of_new_strategy_activation(self):
        self.settings()
        self.process({"pid": 999999})
        probe = Mock(return_value=True)
        self.assertTrue(activation_check(self.root, pid_exists=probe))
        probe.assert_not_called()

    def test_old_pid_record_does_not_trigger_any_process_probe(self):
        self.settings()
        self.process({"pid": 999999})
        self.assertTrue(activation_check(self.root, pid_exists=self.probe))
        self.probe.assert_not_called()

    def test_valid_new_settings_with_no_legacy_record_has_no_process_actions(self):
        self.settings()
        self.assertTrue(activation_check(self.root, pid_exists=self.probe))
        self.probe.assert_not_called()

    def test_corrupt_or_invalid_old_record_is_not_part_of_activation(self):
        self.settings()
        for value in ({}, {"pid": 0}, {"pid": True}, {"pid": "invalid"}, []):
            self.process(value)
            self.assertTrue(activation_check(self.root, pid_exists=self.probe))
        (self.data / "process.json").write_text("broken", encoding="utf-8")
        self.assertTrue(activation_check(self.root, pid_exists=self.probe))
        self.probe.assert_not_called()


class MainStartupChecks(unittest.TestCase):
    def test_main_requires_existing_live_ledger_before_credentials_or_engine(self):
        with tempfile.TemporaryDirectory() as folder:
            with patch.object(hourly_worker, "ROOT", Path(folder)), patch.object(hourly_worker, "_load_api_credentials") as loader:
                with self.assertRaises(RuntimeError):
                    hourly_worker.main()
                loader.assert_not_called()
                self.assertFalse((Path(folder) / "user_data" / "trades-live.sqlite").exists())

    def test_main_uses_existing_book_and_requests_saved_intent_without_start_rpc(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            data = root / "user_data"
            data.mkdir()
            ledger = data / "trades-live.sqlite"
            ledger.write_bytes(b"existing test ledger sentinel")
            fake = FakeEngine()
            with patch.object(hourly_worker, "ROOT", root), patch.object(hourly_worker, "_load_api_credentials", return_value=("fake-key", "fake-secret")) as loader, \
                 patch.dict("os.environ", {"BINANCE_API_KEY": "fake-key", "BINANCE_API_SECRET": "fake-secret",
                     "HOURLY_RPC_USER": "fake-user", "HOURLY_RPC_PASSWORD": "fake-password"}), \
                 patch("execution.binance.BinanceClient") as transport, \
                 patch("execution.store.Store") as store, \
                 patch("execution.diagnostics.ExecutionDiagnostics") as diagnostics, \
                 patch("execution.engine.Engine", return_value=fake) as factory, \
                 patch("execution.streams.MarketStreams") as stream_factory, \
                 patch("uvicorn.run") as serve:
                hourly_worker.main()
                loader.assert_called_once_with(root)
                transport.assert_called_once_with("fake-key", "fake-secret")
                stream_factory.assert_called_once_with(exchange_clock_ms=transport.return_value.calibrated_time_bounds_ms)
                self.assertIs(transport.return_value.streams, stream_factory.return_value)
                self.assertIs(transport.return_value.diagnostics, diagnostics.return_value)
                diagnostics.assert_called_once_with(root / "user_data/execution-diagnostics.sqlite", release="2026-10-15-editable-strategy-equity-v9")
                stream_factory.return_value.start.assert_called_once_with()
                stream_factory.return_value.stop.assert_called_once_with()
                store.assert_called_once_with(ledger)
                factory.assert_called_once_with(transport.return_value, store.return_value, activation_check,
                                               restore_saved_intent=True, diagnostics=diagnostics.return_value)
                self.assertEqual(serve.call_args.kwargs["host"], "127.0.0.1")
                self.assertEqual(serve.call_args.kwargs["port"], 8791)
                self.assertEqual(fake.state, "stopped")
                self.assertEqual(fake.calls, [])
                self.assertEqual(fake.tick_calls, 0)
            self.assertEqual(ledger.read_bytes(), b"existing test ledger sentinel")

    def test_credentials_reader_only_reads_fake_fields_with_environment_precedence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / ".env").write_text("# fake fixture\nBINANCE_API_KEY='file-key'\nBINANCE_API_SECRET=\"file-secret\"\nUNRELATED_SETTING=ignored\n", encoding="utf-8")
            with patch.dict("os.environ", {"BINANCE_API_KEY": "", "BINANCE_API_SECRET": ""}):
                self.assertEqual(hourly_worker._load_api_credentials(root), ("file-key", "file-secret"))
            with patch.dict("os.environ", {"BINANCE_API_KEY": "controller-key", "BINANCE_API_SECRET": "controller-secret"}):
                self.assertEqual(hourly_worker._load_api_credentials(root), ("controller-key", "controller-secret"))


if __name__ == "__main__":
    unittest.main()
