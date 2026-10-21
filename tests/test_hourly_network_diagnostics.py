"""Public network diagnostics: fake wire, private sentinel errors, temporary files."""
import contextlib
import errno
import http.client
import io
import json
import socket
import ssl
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT / 'server'), str(ROOT)]
import network_diagnostics as diagnostics
from execution.binance import ApiError


class Response(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body if isinstance(body, bytes) else json.dumps(body).encode())
        self.status = status


class Transport:
    def __init__(self, response):
        self.response, self.requests = response, []

    def __call__(self, request, *, timeout):
        self.requests.append((request, timeout))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


class NetworkDiagnosticTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)

    def diagnose(self, value, **kwargs):
        wire = Transport(value)
        result = diagnostics.run_diagnostics(self.root, transport=wire,
            proxy_getter=lambda: {}, environment_proxy_getter=lambda: {},
            clock=lambda: 1700000000, **kwargs)
        return result, wire

    def test_success_is_one_fixed_unsigned_get_without_credentials_or_database(self):
        with patch('execution.request_budget.sqlite3.connect', side_effect=AssertionError('No ledger')), \
                patch('pathlib.Path.read_text', side_effect=AssertionError('No credentials')):
            report, wire = self.diagnose(Response({'serverTime': 1700000000123, 'private': 'not retained'}))
        self.assertTrue(report['result']['ok'])
        self.assertEqual(report['result']['clock_offset_ms'], 123)
        self.assertEqual(len(wire.requests), 1)
        request, timeout = wire.requests[0]
        self.assertEqual(request.full_url, diagnostics.ENDPOINT)
        self.assertEqual(request.get_method(), 'GET')
        self.assertIsNone(request.data)
        self.assertIsNone(request.get_header('X-mbx-apikey'))
        self.assertNotIn('signature', request.full_url)
        self.assertNotIn('not retained', json.dumps(report))
        self.assertFalse(report['direct_dns']['checked'])
        self.assertEqual(timeout, 12)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_proxy_summary_reveals_only_selection_booleans_and_source(self):
        private = 'https://secret-user:secret-password@private-proxy.invalid:8999'
        report = diagnostics.proxy_summary(getter=lambda: {'https': private, 'no': 'private-host'},
            environment_getter=lambda: {'https': private})
        self.assertTrue(report['https_proxy_configured'])
        self.assertTrue(report['bypass_rules_configured'])
        self.assertEqual(report['source'], 'environment')
        self.assertNotIn('secret', json.dumps(report))
        self.assertNotIn('private-', json.dumps(report))
        other = diagnostics.proxy_summary(getter=lambda: {'http': private}, environment_getter=lambda: {'http': private})
        self.assertFalse(other['https_proxy_configured'])
        self.assertTrue(other['other_proxy_configured'])

    def test_failures_are_classified_without_exception_urls_or_order_query_advice(self):
        cases = [
            (URLError(ssl.SSLCertVerificationError(1, 'private-password')), 'certificate_verification'),
            (URLError(ssl.SSLError(1, 'private-password')), 'tls_handshake'),
            (URLError(socket.gaierror(-2, 'private-proxy.invalid')), 'dns_resolution'),
            (URLError(TimeoutError('private-password')), 'timeout'),
            (URLError(ConnectionRefusedError('private-password')), 'connection_refused'),
            (URLError(ConnectionResetError('private-password')), 'connection_reset'),
            (http.client.IncompleteRead(b'private-password', 99), 'connection_reset'),
            (http.client.RemoteDisconnected('private-password'), 'connection_reset'),
            (http.client.BadStatusLine('private-password'), 'invalid_response'),
            (URLError(OSError('Tunnel connection failed: 407 private-password')), 'proxy_authentication'),
            (URLError(OSError('Tunnel connection failed: 502 private-password')), 'proxy_tunnel'),
            (URLError(OSError(errno.ENETUNREACH, 'private-password')), 'network_unreachable'),
            (URLError(OSError(errno.EHOSTUNREACH, 'private-password')), 'network_unreachable'),
        ]
        for winerror, category in [(10050, 'network_unreachable'), (10051, 'network_unreachable'),
                (10065, 'network_unreachable'), (10060, 'timeout'), (10061, 'connection_refused'),
                (10054, 'connection_reset')]:
            error = OSError('private-password')
            error.winerror = winerror
            cases.append((URLError(error), category))
        for error, expected in cases:
            with self.subTest(expected=expected):
                report, wire = self.diagnose(error)
                self.assertEqual(report['result']['category'], expected)
                self.assertFalse(report['result']['ok'])
                self.assertEqual(len(wire.requests), 1)
                text = json.dumps(report, ensure_ascii=False)
                self.assertNotIn('private-password', text)
                self.assertNotIn('private-proxy.invalid', text)
                self.assertNotIn('查询原', text)

    def test_http_failures_discard_response_messages(self):
        for status, category in [(403, 'access_denied'), (407, 'proxy_authentication'),
                (418, 'rate_limited'), (429, 'rate_limited'), (451, 'restricted_access'),
                (302, 'redirect_response'), (503, 'upstream_unavailable')]:
            for body in (b'{"code":-1234,"msg":"private-password"}', b'<html>private-password</html>'):
                with self.subTest(status=status, json_body=body.startswith(b'{')):
                    error = HTTPError(diagnostics.ENDPOINT, status, 'private-password', {}, io.BytesIO(body))
                    report, wire = self.diagnose(error)
                    self.assertEqual(report['result']['category'], category)
                    self.assertEqual(report['result']['http_status'], status)
                    self.assertEqual(len(wire.requests), 1)
                    self.assertNotIn('private-password', json.dumps(report))

    def test_malformed_public_response_is_not_retained(self):
        for response in (Response(b'<html>private-password</html>'),
                         Response({'serverTime': True}), Response({'serverTime': 'private-password'})):
            report, wire = self.diagnose(response)
            self.assertFalse(report['result']['ok'])
            self.assertEqual(report['result']['category'], 'invalid_response')
            self.assertNotIn('private-password', json.dumps(report))
            self.assertEqual(len(wire.requests), 1)

    def test_direct_dns_failure_does_not_overrule_successful_proxy_request(self):
        resolver = lambda *args, **kwargs: (_ for _ in ()).throw(socket.gaierror(-2, 'private-host'))
        report, wire = self.diagnose(Response({'serverTime': 1700000000000}), check_dns=True, resolver=resolver)
        self.assertEqual(report['direct_dns']['status'], 'failed')
        self.assertTrue(report['result']['ok'])
        self.assertEqual(len(wire.requests), 1)
        self.assertNotIn('private-host', json.dumps(report))

    def test_optional_dns_timeout_does_not_wait_for_the_resolver(self):
        release = threading.Event()
        def resolver(*args, **kwargs):
            release.wait()
            return []
        try:
            report = diagnostics.direct_dns_summary(enabled=True, resolver=resolver, timeout=.01)
            self.assertEqual(report['status'], 'timeout')
        finally:
            release.set()

    def test_redirect_handler_never_creates_a_followup_request(self):
        handler = diagnostics._NoRedirect()
        self.assertIsNone(handler.redirect_request(None, None, 302, 'redirect', {}, 'https://other.invalid'))

    def test_report_is_written_only_to_this_installations_log_directory(self):
        report, _ = self.diagnose(Response({'serverTime': 1700000000000}))
        path = diagnostics.write_report(self.root, report)
        self.assertEqual(path, self.root.resolve() / 'user_data/logs/network-diagnostics.json')
        self.assertEqual(json.loads(path.read_text())['result']['category'], 'success')
        self.assertEqual(list(path.parent.glob('*.tmp')), [])

    def test_cli_reports_failure_and_saved_report_without_raw_exception(self):
        error = URLError(TimeoutError('https://proxy-user:private-password@private-proxy.invalid'))
        report, _ = self.diagnose(error)
        output = io.StringIO()
        with patch('bootstrap.configure', return_value=self.root), \
                patch.object(diagnostics, 'run_diagnostics', return_value=report), \
                contextlib.redirect_stdout(output):
            result = diagnostics.main([])
        self.assertEqual(result, 1)
        self.assertTrue((self.root / diagnostics.REPORT_RELATIVE).is_file())
        self.assertIn('network-diagnostics.json', output.getvalue())
        self.assertIn('下一步', output.getvalue())
        self.assertNotIn('private-password', output.getvalue())
        self.assertNotIn('查询原', output.getvalue())


if __name__ == '__main__':
    unittest.main()
