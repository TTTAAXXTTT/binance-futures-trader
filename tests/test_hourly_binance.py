"""USD-M transport contract checks: injected responses, no account or network."""
import hashlib
import hmac
import io
import json
import socket
import ssl
import sys
import unittest
from decimal import Decimal
from http.client import BadStatusLine, IncompleteRead, RemoteDisconnected
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from execution.binance import AccountModeError, ApiError, BinanceClient, OrderUncertain, RequestExpired


class Response(io.BytesIO):
    def __init__(self, value, status=200):
        super().__init__(json.dumps(value).encode())
        self.status = status


class Transport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        if not self.responses:
            raise AssertionError("Unexpected transport call")
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, Response) else Response(value)

    def fields(self, index=-1):
        req = self.requests[index][0]
        text = req.data.decode() if req.data is not None else urlsplit(req.full_url).query
        return {k: v[0] for k, v in parse_qs(text).items()}

    def paths(self):
        return [(r.get_method(), urlsplit(r.full_url).path) for r, _ in self.requests]


def client(transport, **kwargs):
    return BinanceClient("test-public-key", "test-secret", transport=transport,
                         clock=kwargs.pop("clock", lambda: 1234.567),
                         auto_sync_time=kwargs.pop("auto_sync_time", False),
                         sleep=lambda _: None, **kwargs)


def fill(trade_id, timestamp=10000, order_id=7, commission="0.017", price="50000"):
    return {"id": trade_id, "time": timestamp, "orderId": order_id,
            "symbol": "BTCUSDT", "qty": "0.001", "price": price,
            "commission": commission, "commissionAsset": "USDT",
            "realizedPnl": "0", "side": "BUY"}


class BinanceTransportChecks(unittest.TestCase):
    def test_constructor_has_no_io(self):
        t = Transport()
        c = client(t)
        self.assertEqual(c.now_ms(), 1234567)
        self.assertEqual(t.requests, [])

    def test_signed_get_signature_matches_exact_encoded_query(self):
        t = Transport({"ok": True})
        c = client(t)
        self.assertEqual(c.signed("GET", "/fapi/v1/order", {"symbol": "BTCUSDT", "origClientOrderId": "x:/1"}), {"ok": True})
        req = t.requests[0][0]
        encoded, signature = urlsplit(req.full_url).query.rsplit("&signature=", 1)
        expected = hmac.new(b"test-secret", encoded.encode(), hashlib.sha256).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(req.get_header("X-mbx-apikey"), "test-public-key")
        self.assertEqual(t.fields()["timestamp"], "1234567")
        self.assertEqual(t.fields()["recvWindow"], "30000")

    def test_public_market_requests_have_no_credentials(self):
        t = Transport([], {"askPrice": "101", "bidPrice": "100"}, {"price": "100.5"}, {"markPrice": "100.4"})
        c = client(t)
        c.klines("BTCUSDT", 1001, startTime=0, endTime=999999)
        c.book_ticker("BTCUSDT")
        c.contract_price("BTCUSDT")
        c.mark_price("BTCUSDT")
        self.assertEqual(t.paths(), [("GET", "/fapi/v1/klines"), ("GET", "/fapi/v1/ticker/bookTicker"),
                                     ("GET", "/fapi/v2/ticker/price"), ("GET", "/fapi/v1/premiumIndex")])
        self.assertEqual(t.fields(0), {"symbol": "BTCUSDT", "interval": "1h", "limit": "1001", "startTime": "0", "endTime": "999999"})
        for req, _ in t.requests:
            self.assertIsNone(req.get_header("X-mbx-apikey"))
            self.assertNotIn("signature", req.full_url)

    def test_server_time_sync_is_conservative_and_does_not_auto_submit(self):
        now = {'wall': 1.0, 'monotonic': 10.0}
        class TimedResponse(Response):
            def read(self):
                now.update(wall=1.02, monotonic=10.02)
                return super().read()
        t = Transport(TimedResponse({'serverTime': 2010}))
        c = client(t, clock=lambda: now['wall'], monotonic=lambda: now['monotonic'])
        self.assertEqual(c.sync_time(), 2010)
        now['monotonic'] = 10.03
        self.assertEqual(c.now_ms(), 2020)
        self.assertEqual(c.clock_status()['sync_rtt_ms'], 20)
        self.assertEqual(t.paths(), [("GET", "/fapi/v1/time")])

    def test_market_order_is_ack_and_uses_exact_decimal_quantity(self):
        t = Transport({"orderId": 7, "clientOrderId": "entry-1"})
        client(t).market_order("BTCUSDT", "BUY", Decimal("0.00100000"), "entry-1")
        fields = t.fields()
        self.assertEqual(fields["quantity"], "0.00100000")
        self.assertEqual(fields["type"], "MARKET")
        self.assertEqual(fields["positionSide"], "BOTH")
        self.assertEqual(fields["reduceOnly"], "false")
        self.assertEqual(fields["newOrderRespType"], "ACK")
        self.assertEqual(fields["newClientOrderId"], "entry-1")
        self.assertNotIn("price", fields)

    def test_regular_exit_is_reduce_only_and_not_close_position(self):
        t = Transport({"orderId": 8})
        client(t).market_order("ETHUSDT", "SELL", "0.010", "exit-1", reduce_only=True)
        fields = t.fields()
        self.assertEqual(fields["reduceOnly"], "true")
        self.assertNotIn("closePosition", fields)

    def test_hosted_stop_has_legal_close_all_contract_price_fields(self):
        t = Transport({"algoId": 123})
        client(t).new_stop("BTCUSDT", "SELL", Decimal("98000.1"), "stop-1")
        self.assertEqual(t.paths(), [("POST", "/fapi/v1/algoOrder")])
        fields = t.fields()
        self.assertEqual(fields["algoType"], "CONDITIONAL")
        self.assertEqual(fields["type"], "STOP_MARKET")
        self.assertEqual(fields["triggerPrice"], "98000.1")
        self.assertEqual(fields["workingType"], "CONTRACT_PRICE")
        self.assertEqual(fields["priceProtect"], "false")
        self.assertEqual(fields["closePosition"], "true")
        self.assertEqual(fields["clientAlgoId"], "stop-1")
        for field in ("quantity", "reduceOnly", "stopPrice", "callbackRate"):
            self.assertNotIn(field, fields)

    def test_order_timeout_is_unknown_and_is_never_repeated(self):
        t = Transport(TimeoutError("network"), {"orderId": 7, "status": "NEW"})
        now = [0.0]
        c = client(t, clock=lambda: now[0], monotonic=lambda: now[0])
        with self.assertRaises(OrderUncertain) as caught:
            c.market_order("BTCUSDT", "BUY", "0.001", "persisted-1")
        self.assertEqual(caught.exception.client_id, "persisted-1")
        self.assertEqual(len(t.requests), 1)
        with self.assertRaises(ApiError):
            c.query_order('BTCUSDT', client_id='persisted-1')
        self.assertEqual(len(t.requests), 1)
        now[0] += 600
        result = c.query_order("BTCUSDT", client_id="persisted-1")
        self.assertEqual(result["orderId"], 7)
        self.assertEqual(t.paths(), [("POST", "/fapi/v1/order"), ("GET", "/fapi/v1/order")])
        self.assertEqual(t.fields()["origClientOrderId"], "persisted-1")

    def test_negative_unknown_order_codes_never_retry_write(self):
        for code in (-1006, -1007):
            with self.subTest(code=code):
                t = Transport({"code": code, "msg": "unknown"})
                with self.assertRaises(OrderUncertain) as caught:
                    client(t).new_stop("ETHUSDT", "BUY", "2000", "persisted-stop")
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(caught.exception.client_id, "persisted-stop")
                self.assertEqual(len(t.requests), 1)

    def test_http_5xx_write_unknown_even_if_body_is_not_json(self):
        for body in (b'{"code":-1000,"msg":"unknown"}', b"Service Unavailable"):
            with self.subTest(body=body):
                failure = HTTPError("https://unused", 503, "server", {}, io.BytesIO(body))
                t = Transport(failure)
                with self.assertRaises(OrderUncertain) as caught:
                    client(t).market_order("BTCUSDT", "BUY", "0.001", "unique-order")
                self.assertEqual(caught.exception.status, 503)
                self.assertEqual(len(t.requests), 1)

    def test_delete_timeout_unknown_never_repeated(self):
        t = Transport(URLError("lost"))
        with self.assertRaises(OrderUncertain) as caught:
            client(t).cancel_algo(client_id="old-stop")
        self.assertEqual(caught.exception.client_id, "old-stop")
        self.assertEqual(t.paths(), [("DELETE", "/fapi/v1/algoOrder")])

    def test_explicit_rejection_is_api_error_and_preserves_code(self):
        t = Transport({"code": -2010, "msg": "Order rejected"})
        with self.assertRaises(ApiError) as caught:
            client(t).market_order("BTCUSDT", "BUY", "0.001", "entry-2")
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(caught.exception.code, -2010)
        self.assertEqual(caught.exception.message, "Order rejected")
        self.assertEqual(len(t.requests), 1)

    def test_order_not_found_never_causes_submission_or_retries(self):
        t = Transport(HTTPError("https://unused", 404, "not found", {},
                                io.BytesIO(b'{"code":-2013,"msg":"Order does not exist"}')))
        with self.assertRaises(ApiError) as caught:
            client(t).query_order("BTCUSDT", "persisted-order")
        self.assertEqual(caught.exception.code, -2013)
        self.assertEqual(t.paths(), [("GET", "/fapi/v1/order")])

    def test_get_transient_is_bounded_and_can_succeed(self):
        t = Transport(TimeoutError(), Response({"code": -1007, "msg": "timeout"}, status=500), {"price": "10"})
        now = [0.0]
        c = client(t, clock=lambda: now[0], monotonic=lambda: now[0])
        for _ in range(2):
            with self.assertRaises(ApiError):
                c.contract_price('ETHUSDT')
            now[0] += 600
        self.assertEqual(c.contract_price("ETHUSDT"), {"price": "10"})
        self.assertEqual(len(t.requests), 3)

    def test_get_exhaustion_never_turns_into_write(self):
        t = Transport(TimeoutError(), TimeoutError(), TimeoutError())
        with self.assertRaises(ApiError) as caught:
            client(t).account()
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(t.paths(), [("GET", "/fapi/v3/account")])

    def test_get_network_error_names_cause_without_unknown_order_message(self):
        cases = [
            (URLError(socket.gaierror(11001, "private DNS detail")), "DNS"),
            (URLError(ssl.SSLCertVerificationError(1, "private TLS detail")), "TLS证书"),
            (URLError(TimeoutError("private timeout detail")), "超时"),
            (ConnectionResetError(10054, "private reset detail"), "重置"),
            (ConnectionRefusedError(10061, "private refused detail"), "拒绝"),
            (OSError(10051, "private unreachable detail"), "不可达"),
            (URLError("Tunnel connection failed: 407 user:password@private-proxy"), "代理认证"),
            (URLError(OSError("Tunnel connection failed: 407 user:password@private-proxy")), "代理认证"),
        ]
        for failure, detail in cases:
            with self.subTest(detail=detail):
                t = Transport(failure)
                with self.assertRaises(ApiError) as caught:
                    client(t, get_retries=0).account()
                self.assertNotIsInstance(caught.exception, OrderUncertain)
                self.assertIn("GET /fapi/v3/account", str(caught.exception))
                self.assertIn(detail, str(caught.exception))
                self.assertNotIn("查询原请求", str(caught.exception))
                self.assertNotIn("signature", str(caught.exception))
                self.assertNotIn("private", str(caught.exception))
                self.assertEqual(len(t.requests), 1)

    def test_get_unknown_error_does_not_expose_signed_url_or_credentials(self):
        raw = "https://user:password@private-proxy/?signature=private-signature&key=test-public-key&secret=test-secret"
        t = Transport(URLError(raw))
        with self.assertRaises(ApiError) as caught:
            client(t, get_retries=0).account()
        message = str(caught.exception)
        for value in ("password", "private-proxy", "private-signature", "test-public-key", "test-secret"):
            self.assertNotIn(value, message)
        self.assertIn("网络诊断", message)

    def test_get_interrupted_response_is_closed_and_retried_with_fresh_request(self):
        class InterruptedResponse(Response):
            def read(self):
                raise IncompleteRead(b"secret-partial-body", 100)
        response = InterruptedResponse({})
        t = Transport(response, {"serverTime": 1234})
        now = [0.0]
        c = client(t, clock=lambda: now[0], monotonic=lambda: now[0])
        with self.assertRaises(ApiError):
            c.public('GET', '/fapi/v1/time')
        now[0] += 600
        self.assertEqual(c.public("GET", "/fapi/v1/time"), {"serverTime": 1234})
        self.assertTrue(response.closed)
        self.assertEqual(t.paths(), [("GET", "/fapi/v1/time")] * 2)

    def test_http_connection_failures_obey_bounded_get_retries(self):
        for kind in (IncompleteRead, BadStatusLine, RemoteDisconnected):
            with self.subTest(kind=kind.__name__):
                failures = [kind(b"private-response") for _ in range(3)]
                t = Transport(*failures)
                with self.assertRaises(ApiError) as caught:
                    client(t).account()
                self.assertNotIsInstance(caught.exception, OrderUncertain)
                self.assertNotIn("private-response", str(caught.exception))
                self.assertEqual(t.paths(), [("GET", "/fapi/v3/account")])

    def test_write_interrupted_response_keeps_unknown_identity_and_never_resubmits(self):
        class InterruptedResponse(Response):
            def read(self):
                raise IncompleteRead(b"private-body-with-signature", 100)
        for method in ("POST", "DELETE"):
            with self.subTest(method=method):
                response = InterruptedResponse({})
                t = Transport(response)
                c = client(t)
                with self.assertRaises(OrderUncertain) as caught:
                    if method == "POST":
                        c.market_order("BTCUSDT", "BUY", "0.001", "persisted-entry")
                    else:
                        c.cancel_order("BTCUSDT", "persisted-entry")
                self.assertTrue(response.closed)
                self.assertEqual(caught.exception.client_id, "persisted-entry")
                self.assertIn("查询原请求", str(caught.exception))
                self.assertNotIn("private-body", str(caught.exception))
                self.assertEqual(t.paths(), [(method, "/fapi/v1/order")])

    def test_get_invalid_json_is_read_failure_and_keeps_retries_bounded(self):
        class InvalidResponse(Response):
            def read(self):
                return b"<html>private-proxy-error</html>"
        responses = [InvalidResponse({}) for _ in range(3)]
        t = Transport(*responses)
        with self.assertRaises(ApiError) as caught:
            client(t).account()
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertIn("响应格式无效", str(caught.exception))
        self.assertNotIn("查询", str(caught.exception))
        self.assertNotIn("private-proxy", str(caught.exception))
        self.assertTrue(responses[0].closed)
        self.assertFalse(any(response.closed for response in responses[1:]))
        self.assertEqual(t.paths(), [("GET", "/fapi/v3/account")])

    def test_identity_queries_use_correct_fields(self):
        t = Transport({}, {}, {}, {})
        c = client(t)
        c.query_order("BTCUSDT", order_id=7)
        c.cancel_order("BTCUSDT", client_id="cancel-1")
        c.query_algo(client_id="stop-1")
        c.cancel_algo(algo_id=44)
        self.assertEqual(t.fields(0)["orderId"], "7")
        self.assertEqual(t.fields(1)["origClientOrderId"], "cancel-1")
        self.assertEqual(t.fields(2)["clientAlgoId"], "stop-1")
        self.assertEqual(t.fields(3)["algoId"], "44")
        self.assertNotIn("symbol", t.fields(2))
        self.assertNotIn("symbol", t.fields(3))

    def test_account_and_fill_values_not_double_deducted_or_retimed(self):
        account = {"totalWalletBalance": "1000", "totalUnrealizedProfit": "-3.5", "availableBalance": "900"}
        trade = fill(10, timestamp=3600002, commission="0.025")
        t = Transport(account, [trade])
        c = client(t)
        self.assertEqual(c.account(), account)
        self.assertEqual(c.fills("BTCUSDT", order_id=7), [trade])
        self.assertEqual(t.fields()["orderId"], "7")
        self.assertNotIn("fromId", t.fields())

    def test_raw_exchange_filters_are_not_replaced_by_precision(self):
        data = {"symbols": [{"symbol": "BTCUSDT", "pricePrecision": 2,
                             "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10"},
                                         {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.001"}]}]}
        self.assertEqual(client(Transport(data)).exchange_info(), data)

    def test_fills_page_uses_from_id_and_keeps_same_millisecond_fills(self):
        t = Transport([fill(10), fill(11)], [fill(12)], [])
        c = client(t)
        actual = c.fills_all("BTCUSDT", order_id=7, fromId=10, limit=2)
        self.assertEqual([x["id"] for x in actual], [10, 11, 12])
        self.assertEqual([x["time"] for x in actual], [10000] * 3)
        self.assertEqual(t.fields(0)["fromId"], "10")
        self.assertEqual(t.fields(1)["fromId"], "12")

    def test_time_page_switches_to_id_without_skipping_same_timestamp(self):
        t = Transport([fill(10), fill(11)], [fill(11), fill(12)], [])
        result = client(t).fills_all("BTCUSDT", startTime=9999, endTime=10001, limit=2)
        self.assertEqual([r["id"] for r in result], [10, 11, 12])
        self.assertIn("startTime", t.fields(0))
        self.assertEqual(t.fields(1)["fromId"], "12")
        self.assertNotIn("startTime", t.fields(1))
        self.assertNotIn("endTime", t.fields(1))

    def test_long_fill_range_splits_seven_days_and_deduplicates(self):
        week = 7 * 86400000
        t = Transport([fill(1, 0)], [fill(2, week)])
        result = client(t).fills_all("BTCUSDT", startTime=0, endTime=week, limit=2)
        self.assertEqual([r["id"] for r in result], [1, 2])
        self.assertEqual(t.fields(0)["endTime"], str(week - 1))
        self.assertEqual(t.fields(1)["startTime"], str(week))

    def test_fills_truncation_and_missing_real_time_fail_closed(self):
        with self.assertRaises(ApiError):
            client(Transport([fill(1), fill(2)])).fills_all("BTCUSDT", limit=2, max_pages=1)
        broken = fill(1)
        del broken["time"]
        with self.assertRaises(ApiError):
            client(Transport([broken])).fills_all("BTCUSDT")

    def test_fill_invalid_mixed_cursor_arguments_do_not_call_transport(self):
        t = Transport()
        with self.assertRaises(ValueError):
            client(t).fills("BTCUSDT", fromId=3, startTime=1)
        self.assertEqual(t.requests, [])

    def test_funding_is_explicit_raw_settlement_and_page(self):
        raw = [{"incomeType": "FUNDING_FEE", "income": "-0.003", "asset": "USDT", "time": 1000, "tranId": 23}]
        t = Transport(raw)
        self.assertEqual(client(t).income("ETHUSDT", startTime=0, endTime=2000, page=2), raw)
        self.assertEqual(t.fields()["incomeType"], "FUNDING_FEE")
        self.assertEqual(t.fields()["page"], "2")

    def test_mode_mismatch_never_changes_account_modes_or_symbol(self):
        for hedge, multi in ((True, False), (False, True)):
            with self.subTest(hedge=hedge, multi=multi):
                t = Transport({"dualSidePosition": hedge}, {"multiAssetsMargin": multi})
                with self.assertRaises(AccountModeError):
                    client(t).set_cross50("BTCUSDT")
                self.assertTrue(all(method == "GET" for method, _ in t.paths()))

    def test_missing_mode_response_does_not_proceed(self):
        t = Transport({})
        with self.assertRaises(AccountModeError):
            client(t).set_cross50("BTCUSDT")
        self.assertEqual(len(t.requests), 1)

    def test_set_cross50_only_changes_symbol_and_verifies(self):
        initial = [{"symbol": "BTCUSDT", "marginType": "ISOLATED", "leverage": 3}]
        crossed = [{"symbol": "BTCUSDT", "marginType": "CROSSED", "leverage": 3}]
        desired = [{"symbol": "BTCUSDT", "marginType": "CROSSED", "leverage": 50}]
        t = Transport({"dualSidePosition": False}, {"multiAssetsMargin": False},
                      initial, {"code": 200}, crossed, {"leverage": 50}, desired)
        result = client(t).set_cross50("BTCUSDT")
        self.assertEqual(result, desired[0])
        writes = [(req, index) for index, (req, _) in enumerate(t.requests) if req.get_method() == "POST"]
        self.assertEqual([urlsplit(req.full_url).path for req, _ in writes], ["/fapi/v1/marginType", "/fapi/v1/leverage"])
        self.assertEqual(t.fields(writes[0][1])["marginType"], "CROSSED")
        self.assertEqual(t.fields(writes[1][1])["leverage"], "50")

    def test_already_cross50_causes_no_writes(self):
        t = Transport({"dualSidePosition": "false"}, {"multiAssetsMargin": "false"},
                      [{"symbol": "ETHUSDT", "marginType": "CROSSED", "leverage": 50}])
        client(t).set_cross50("ETHUSDT")
        self.assertTrue(all(method == "GET" for method, _ in t.paths()))

    def test_margin_write_unknown_stops_before_leverage_write(self):
        t = Transport({"dualSidePosition": False}, {"multiAssetsMargin": False},
                      [{"symbol": "BTCUSDT", "marginType": "ISOLATED", "leverage": 3}], TimeoutError())
        with self.assertRaises(OrderUncertain):
            client(t).set_cross50("BTCUSDT")
        self.assertEqual(t.paths()[-1], ("POST", "/fapi/v1/marginType"))
        self.assertEqual(len(t.requests), 4)

    def test_client_id_and_quantity_validation_happens_before_io(self):
        t = Transport()
        c = client(t)
        for quantity, order_id in (("-1", "valid"), ("NaN", "valid"), ("1", ""), ("1", "bad key")):
            with self.subTest(quantity=quantity, order_id=order_id):
                with self.assertRaises(ValueError):
                    c.market_order("BTCUSDT", "BUY", quantity, order_id)
        self.assertEqual(t.requests, [])


class BinanceTimeSynchronizationTests(unittest.TestCase):
    def setUp(self):
        self.time = {'wall': 1234.0, 'monotonic': 10.0}

    def calibrated_client(self, transport, **kwargs):
        return BinanceClient('test-public-key', 'test-secret', transport=transport,
            clock=lambda: self.time['wall'], monotonic=lambda: self.time['monotonic'],
            sleep=lambda _: None, **kwargs)

    def test_initial_signed_read_calibrates_before_timestamp_and_signature(self):
        t = Transport({'serverTime': 2000000}, {'availableBalance': '100'})
        c = self.calibrated_client(t)
        self.assertEqual(t.requests, [])
        self.assertEqual(c.account(), {'availableBalance': '100'})
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])
        self.assertEqual(t.fields(1)['timestamp'], '1999750')
        self.assertEqual(t.fields(1)['recvWindow'], '30000')
        self.assertIsNone(t.requests[0][0].get_header('X-mbx-apikey'))

    def test_monotonic_time_ignores_windows_wall_clock_changes(self):
        t = Transport({'serverTime': 2000000}, {}, {})
        c = self.calibrated_client(t)
        c.account()
        self.time['wall'] -= 3600
        self.time['monotonic'] += 2
        self.assertEqual(c.now_ms(), 2002000)
        self.time['wall'] += 7200
        c.account()
        self.assertEqual(t.fields()['timestamp'], '2001750')
        self.assertEqual(t.paths().count(('GET', '/fapi/v1/time')), 1)

    def test_periodic_sync_uses_monotonic_interval_and_refreshes_server_anchor(self):
        t = Transport({'serverTime': 2000000}, {}, {}, {'serverTime': 2300000}, {})
        c = self.calibrated_client(t)
        c.account()
        self.time['monotonic'] += 299
        c.account()
        self.assertEqual(t.paths().count(('GET', '/fapi/v1/time')), 1)
        self.time['monotonic'] += 1
        c.account()
        self.assertEqual(t.paths().count(('GET', '/fapi/v1/time')), 2)
        self.assertEqual(t.fields()['timestamp'], '2299750')

    def test_get_timestamp_rejection_syncs_and_retries_once_with_new_signature(self):
        t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'timestamp rejected'},
                      {'serverTime': 2001000}, {'availableBalance': '100'})
        c = self.calibrated_client(t, get_retries=0)
        self.assertEqual(c.account(), {'availableBalance': '100'})
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account'),
                                    ('GET', '/fapi/v1/time'), ('GET', '/fapi/v3/account')])
        self.assertEqual(t.fields(1)['timestamp'], '1999750')
        self.assertEqual(t.fields(3)['timestamp'], '2000750')
        self.assertNotEqual(t.fields(1)['signature'], t.fields(3)['signature'])

    def test_repeated_timestamp_rejection_is_bounded(self):
        t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'first'},
                      {'serverTime': 2001000}, {'code': -1021, 'msg': 'second'})
        with self.assertRaises(ApiError) as caught:
            self.calibrated_client(t).account()
        self.assertEqual(caught.exception.code, -1021)
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(len(t.requests), 4)

    def test_explicit_timestamp_rejected_write_is_not_repeated(self):
        for method in ('POST', 'DELETE'):
            with self.subTest(method=method):
                t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'timestamp rejected'},
                              {'serverTime': 2001000})
                c = self.calibrated_client(t)
                with self.assertRaises(ApiError) as caught:
                    if method == 'POST':
                        c.market_order('BTCUSDT', 'BUY', '.001', 'persisted-id')
                    else:
                        c.cancel_order('BTCUSDT', 'persisted-id')
                self.assertEqual(caught.exception.code, -1021)
                self.assertNotIsInstance(caught.exception, OrderUncertain)
                self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), (method, '/fapi/v1/order'),
                                            ('GET', '/fapi/v1/time')])
                self.assertEqual(c.now_ms(), 2001000)

    def test_initial_calibration_failure_never_submits_or_marks_unsent_write_unknown(self):
        t = Transport(TimeoutError('private URL'))
        c = self.calibrated_client(t, get_retries=0)
        with self.assertRaises(ApiError) as caught:
            c.market_order('BTCUSDT', 'BUY', '.001', 'persisted-id')
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertEqual(caught.exception.path, '/fapi/v1/time')
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time')])

    def test_rejected_write_keeps_definite_rejection_when_forced_sync_fails(self):
        t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'timestamp rejected'}, TimeoutError())
        c = self.calibrated_client(t, get_retries=0)
        with self.assertRaises(ApiError) as caught:
            c.market_order('BTCUSDT', 'BUY', '.001', 'persisted-id')
        self.assertEqual(caught.exception.code, -1021)
        self.assertNotIsInstance(caught.exception, OrderUncertain)
        self.assertIn('校时未完成', str(caught.exception))
        self.assertEqual(t.paths().count(('POST', '/fapi/v1/order')), 1)

    def test_timeout_or_unknown_5xx_write_remains_unknown_without_recovery_resubmit(self):
        failures = [TimeoutError(), Response({'code': -1007, 'msg': 'unknown'}, status=503),
                    Response({'code': -1021, 'msg': 'ambiguous 5xx'}, status=503)]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                t = Transport({'serverTime': 2000000}, failure)
                with self.assertRaises(OrderUncertain):
                    self.calibrated_client(t).market_order('BTCUSDT', 'BUY', '.001', 'persisted-id')
                self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('POST', '/fapi/v1/order')])

    def test_malformed_server_time_does_not_replace_valid_anchor(self):
        for bad_time in (None, True, 0, -1, 2000.5, 'private invalid response'):
            with self.subTest(bad_time=bad_time):
                t = Transport({'serverTime': 2000000}, {'serverTime': bad_time})
                c = self.calibrated_client(t)
                c.sync_time()
                with self.assertRaises(ApiError):
                    c.sync_time()
                self.assertEqual(c.now_ms(), 2000000)

    def test_failed_periodic_sync_waits_without_wire_and_preserves_anchor(self):
        t = Transport({'serverTime': 2000000}, URLError(ssl.SSLEOFError(8, 'private TLS detail')),
                      {'serverTime': 2330000})
        c = self.calibrated_client(t, get_retries=0)
        c.ensure_time_sync()
        self.time['monotonic'] += 300
        with self.assertRaises(ApiError) as first:
            c.ensure_time_sync()
        self.assertEqual(c.now_ms(), 2300000)
        for elapsed in (2, 10, 599):
            self.time['monotonic'] = 310 + elapsed
            with self.assertRaises(ApiError) as repeated:
                c.ensure_time_sync()
            self.assertEqual(str(repeated.exception), str(first.exception))
            self.assertNotIsInstance(repeated.exception, OrderUncertain)
        self.assertEqual(len(t.requests), 2)
        self.time['monotonic'] = 910
        c.ensure_time_sync()
        self.assertEqual(len(t.requests), 3)
        self.assertEqual(c.now_ms(), 2330000)
        self.assertEqual(c.clock_status()['sync_failures'], 0)
        self.assertEqual(c.clock_status()['retry_in_seconds'], 0)
        self.assertNotIn('private', str(first.exception))
        self.assertIn('提前断开', str(first.exception))

    def test_initial_sync_retry_delay_blocks_signed_read_before_wire(self):
        t = Transport(TimeoutError(), {'serverTime': 2000000}, {})
        c = self.calibrated_client(t, get_retries=0)
        with self.assertRaises(ApiError):
            c.account()
        self.time['wall'] += 7200  # A Windows clock jump must not end retry waiting.
        self.time['monotonic'] += 2
        with self.assertRaises(ApiError) as caught:
            c.account()
        self.assertEqual(caught.exception.path, '/fapi/v3/account')
        self.assertTrue(caught.exception.network_backoff)
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time')])
        self.time['monotonic'] += 598
        c.account()
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v1/time'),
                                    ('GET', '/fapi/v3/account')])

    def test_explicit_sync_respects_automatic_delay_without_writes(self):
        t = Transport(TimeoutError(), {'serverTime': 2000000})
        c = self.calibrated_client(t, get_retries=0)
        with self.assertRaises(ApiError):
            c.ensure_time_sync()
        with self.assertRaises(ApiError):
            c.sync_time()
        self.assertEqual(len(t.requests), 1)
        self.time['monotonic'] += 600
        c.sync_time()
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v1/time')])
        self.assertEqual(c.clock_status()['sync_failures'], 0)

    def test_sync_failure_delay_is_bounded_and_each_wait_has_no_wire(self):
        t = Transport(*[TimeoutError() for _ in range(5)])
        c = self.calibrated_client(t, get_retries=0)
        for expected in (600, 600, 600, 600, 600):
            with self.assertRaises(ApiError):
                c.ensure_time_sync()
            before = len(t.requests)
            self.assertEqual(c.clock_status()['retry_in_seconds'], expected)
            with self.assertRaises(ApiError):
                c.ensure_time_sync()
            self.assertEqual(len(t.requests), before)
            self.time['monotonic'] += expected

    def test_time_sync_retry_does_not_count_previous_timeout_in_successful_roundtrip(self):
        t = Transport(TimeoutError(), {'serverTime': 2000000}, {})
        original_wire = t.__call__
        def wire(request, *, timeout):
            if len(t.requests) == 0:
                self.time['monotonic'] += 12
            elif len(t.requests) == 1:
                self.time['monotonic'] += .020
            return original_wire(request, timeout=timeout)
        c = self.calibrated_client(wire)
        with self.assertRaises(ApiError):
            c.account()
        self.time['monotonic'] += 600
        c.account()
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time'), ('GET', '/fapi/v1/time'),
                                    ('GET', '/fapi/v3/account')])
        self.assertEqual(t.fields(2)['timestamp'], '1999750')

    def test_asymmetric_six_second_handshake_never_signs_in_future(self):
        t = Transport({'serverTime': 2000000}, {})
        original_wire = t.__call__
        def wire(request, *, timeout):
            if not t.requests:
                self.time['monotonic'] += 6.010
            return original_wire(request, timeout=timeout)
        c = self.calibrated_client(wire)
        c.account()
        server_at_signed_receipt = 2000010  # Only 10ms passed after serverTime was produced.
        stamp = int(t.fields()['timestamp'])
        self.assertLess(stamp, server_at_signed_receipt + 1000)
        self.assertLessEqual(server_at_signed_receipt - stamp, int(t.fields()['recvWindow']))
        self.assertGreaterEqual(c.now_upper_ms(), server_at_signed_receipt)
        self.assertEqual(c.clock_status()['sync_rtt_ms'], 6010)

    def test_slow_return_then_slow_request_fits_receive_window(self):
        t = Transport({'serverTime': 2000000}, {})
        original_wire = t.__call__
        def wire(request, *, timeout):
            self.time['monotonic'] += 6 if not t.requests else 12
            return original_wire(request, timeout=timeout)
        c = self.calibrated_client(wire)
        c.account()
        server_at_signed_receipt = 2018000
        stamp = int(t.fields()['timestamp'])
        self.assertLess(stamp, server_at_signed_receipt + 1000)
        self.assertLessEqual(server_at_signed_receipt - stamp, int(t.fields()['recvWindow']))

    def test_get_1021_recovery_with_slow_successful_sync_is_not_future_dated(self):
        t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'old stamp'},
                      {'serverTime': 2012000}, {})
        original_wire = t.__call__
        def wire(request, *, timeout):
            if len(t.requests) in (0, 2):
                self.time['monotonic'] += 6.010
            return original_wire(request, timeout=timeout)
        c = self.calibrated_client(wire)
        c.account()
        self.assertLess(int(t.fields(3)['timestamp']), 2012010 + 1000)
        self.assertEqual(t.paths().count(('GET', '/fapi/v3/account')), 2)

    def test_expired_entry_after_initial_sync_is_local_and_never_submitted(self):
        t = Transport({'serverTime': 2000000})
        original_wire = t.__call__
        def wire(request, *, timeout):
            self.time['monotonic'] += 6
            return original_wire(request, timeout=timeout)
        with self.assertRaises(RequestExpired):
            self.calibrated_client(wire).market_order('BTCUSDT', 'BUY', '.001', 'expired', deadline_ms=2005000)
        self.assertEqual(t.paths(), [('GET', '/fapi/v1/time')])

    def test_explicit_receive_window_setting_remains_effective(self):
        t = Transport({'serverTime': 2000000}, {})
        self.calibrated_client(t, recv_window=5000).account()
        self.assertEqual(t.fields()['recvWindow'], '5000')

    def test_repeated_1021_includes_running_fix_and_non_secret_request_details(self):
        t = Transport({'serverTime': 2000000}, {'code': -1021, 'msg': 'first'},
                      {'serverTime': 2001000}, {'code': -1021, 'msg': 'second'})
        with self.assertRaises(ApiError) as caught:
            self.calibrated_client(t).account()
        text = str(caught.exception)
        for detail in ('time-sync-v2', 'GET /fapi/v3/account', '校时RTT=', 'recvWindow=30000ms'):
            self.assertIn(detail, text)
        for secret in ('test-public-key', 'test-secret', 'signature=', 'https://'):
            self.assertNotIn(secret, text)


if __name__ == "__main__":
    unittest.main()
