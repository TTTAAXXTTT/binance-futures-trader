"""Cross-layer freshness checks with a clock that advances on every read."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests'), str(ROOT / 'runtime/packages')]

from control import hourly_app as console
from execution.binance import ApiError
from execution.engine import Engine
from execution.store import Store
from test_hourly_engine import FakeExchange


class AdvancingExchange(FakeExchange):
    def now_ms(self):
        self.now += 7
        return self.now


class SnapshotTimestampChecks(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = Store(Path(directory.name) / 'offline.sqlite')
        self.client = AdvancingExchange()
        self.client.account_value['totalMarginBalance'] = '1000'
        self.monotonic = 100.0
        self.engine = Engine(self.client, self.store,
                             monotonic=lambda: self.monotonic)
        self.configuration = {'can_trade': True, 'hedge_mode': False,
                              'multi_asset': False}
        self.network = patch.object(console.urllib.request, 'urlopen',
            side_effect=AssertionError('Freshness verification is offline'))
        self.network.start()
        self.addCleanup(self.network.stop)

    def worker(self):
        return dict(self.engine.snapshot(), connected=True)

    def test_success_uses_one_completion_timestamp_for_reconciliation_and_snapshot(self):
        before = self.client.now
        self.engine.reconcile()
        self.assertTrue(self.engine.reconciled)
        worker = self.worker()
        self.assertGreater(self.client.now, before)
        self.assertEqual(worker['last_successful_reconcile_ms'],
                         worker['account_snapshot']['reconciled_at_ms'])
        # The finally timestamp legitimately advances; it is not the proof of
        # success consumed by the monitor's display-only reuse path.
        self.assertGreater(self.engine.last_full_reconcile_ms,
                           worker['last_successful_reconcile_ms'])
        account = console.worker_account_view(worker, self.configuration, 0,
                                              rpc_elapsed=.2)
        self.assertIsNotNone(account)
        self.assertEqual(account['source'], 'worker_reconcile')
        self.assertEqual(account['equity_usdt'], 1000)
        self.assertEqual(account['observed_at_ms'],
                         worker['last_successful_reconcile_ms'])
        self.assertAlmostEqual(account['age_seconds'], .2)
        self.assertEqual(self.client.writes(), [])

    def test_failed_sweep_cannot_refresh_or_reuse_the_previous_success(self):
        self.engine.reconcile()
        old = self.worker()['account_snapshot']['reconciled_at_ms']
        self.monotonic += 10
        with patch.object(self.client, 'positions',
                          side_effect=ApiError(None, 'offline read failure')):
            with self.assertRaises(ApiError):
                self.engine.reconcile()
        worker = self.worker()
        self.assertFalse(worker['reconciled'])
        self.assertEqual(worker['last_successful_reconcile_ms'], old)
        self.assertEqual(worker['account_snapshot']['reconciled_at_ms'], old)
        self.assertEqual(worker['account_snapshot']['age_seconds'], 10)
        self.assertIsNone(console.worker_account_view(worker, self.configuration, 0))
        self.assertEqual(self.client.writes(), [])


if __name__ == '__main__':
    unittest.main()
