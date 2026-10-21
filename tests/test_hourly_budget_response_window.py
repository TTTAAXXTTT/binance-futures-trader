"""Delayed no-Date headers remain attached to their original request window."""
import sys
import tempfile
import unittest
from pathlib import Path
from email.utils import formatdate

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.request_budget import RequestBudget, RateLimited
from execution.binance import BinanceClient
from test_hourly_binance import Response, Transport


class ResponseWindowTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.now = 6059.0
        self.budget = RequestBudget(Path(self.folder.name) / 'budget.sqlite', clock=lambda: self.now)

    def test_old_response_without_date_does_not_contaminate_new_reserved_window(self):
        old = self.budget.reserve('GET', '/fapi/v3/account')
        self.assertEqual(old, 100)
        self.now = 6061.0
        new = self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '2000'}, request_minute=old)
        self.assertEqual(self.budget.snapshot()['used_weight'], 1)
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '8'}, request_minute=new)
        self.assertEqual(self.budget.snapshot()['used_weight'], 8)

    def test_unattributed_missing_or_invalid_date_weight_is_ignored(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        for headers in ({'X-MBX-USED-WEIGHT-1M': '2000'},
                        {'Date': 'not-a-date', 'X-MBX-USED-WEIGHT-1M': '2000'}):
            self.budget.observe(headers)
            self.assertEqual(self.budget.snapshot()['used_weight'], 1)

    def test_late_unattributed_header_preserves_real_shared_cooldown(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'Retry-After': '180', 'X-MBX-USED-WEIGHT-1M': '2000'},
                            status=429, code=-1003)
        self.assertEqual(self.budget.snapshot()['used_weight'], 1)
        expected = self.budget.snapshot()['cooldown_until_ms']
        self.now += 60
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '0'}, status=200)
        self.assertEqual(self.budget.snapshot()['cooldown_until_ms'], expected)
        with self.assertRaises(RateLimited):
            RequestBudget(self.budget.path, clock=lambda: self.now).reserve('POST', '/fapi/v1/order', critical=True)

    def test_valid_date_is_authoritative_and_old_date_never_rolls_back(self):
        old = self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'Date': formatdate(6061, usegmt=True), 'X-MBX-USED-WEIGHT-1M': '8'}, request_minute=old)
        self.assertEqual(self.budget.snapshot()['used_weight'], 8)
        self.budget.observe({'Date': formatdate(6059, usegmt=True), 'X-MBX-USED-WEIGHT-1M': '2000'}, request_minute=101)
        self.assertEqual(self.budget.snapshot()['used_weight'], 8)

    def test_transport_captures_send_window_and_response_header_does_not_use_receive_time(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        budget, owner = self.budget, self
        class LateResponse(Response):
            def read(self):
                owner.now = 6061
                budget.reserve('GET', '/fapi/v1/time')
                return super().read()
        response = LateResponse({'availableBalance': '100'})
        response.headers = {'X-MBX-USED-WEIGHT-1M': '2000'}
        client = BinanceClient('fake', 'fake', transport=Transport(response), budget=self.budget,
                               clock=lambda: self.now, auto_sync_time=False)
        client.account()
        self.assertEqual(self.budget.snapshot()['used_weight'], 1)

    def test_invalid_negative_weight_cannot_create_a_negative_budget(self):
        self.budget.reserve('GET', '/fapi/v1/time')
        self.budget.observe({'X-MBX-USED-WEIGHT-1M': '-100'}, request_minute=100)
        self.assertEqual(self.budget.snapshot()['used_weight'], 1)


if __name__ == '__main__':
    unittest.main()
