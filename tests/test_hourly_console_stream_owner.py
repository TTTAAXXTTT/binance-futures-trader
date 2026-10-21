"""Display ownership and stale RPC behavior; no real sockets or services."""
import asyncio
import copy
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade')]
from control import hourly_app as console
from execution.streams import MarketStreams


class ConsoleStreamOwnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_console_lifespan_never_opens_or_stops_worker_sockets(self):
        entered, finished = asyncio.Event(), asyncio.Event()
        async def harmless_monitor():
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                finished.set()
        with patch('control.installation.initialize_files'), patch.object(console, 'event'), \
                patch.object(console, 'monitor', harmless_monitor), \
                patch.object(MarketStreams, 'start', side_effect=AssertionError('duplicate live stream')), \
                patch.object(MarketStreams, 'stop', side_effect=AssertionError('display stops worker stream')):
            async with console.lifespan(console.app):
                await asyncio.wait_for(entered.wait(), 1)
            self.assertTrue(finished.is_set())


class ConsoleStreamStatusTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.worker = {'connected': True, 'state': 'running', 'desired_running': True,
            'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True},
            'market_stream': {'connections': {'public': True, 'market': True},
                'symbols': {'ETHUSDT': {'book_live': True}},
                'diagnostics': {'public': {'connection_attempts': 1}}}}
        self.stack.enter_context(patch.dict(console.STATE,
            {'account': {}, 'worker': self.worker, 'markets': {}, 'legacy_worker': {}}, clear=True))
        self.stack.enter_context(patch.object(console, 'trade_controls', return_value={
            'symbols': {'BTCUSDT': False, 'ETHUSDT': True}, 'desired_running': True}))
        self.stack.enter_context(patch.object(console, 'start_reasons', return_value=[]))
        self.stack.enter_context(patch.object(console, 'saved_user_intent', return_value={'desired_running': True}))
        self.stack.enter_context(patch.object(console.compat, 'read_json', return_value={}))
        self.stack.enter_context(patch('control.installation.configured', return_value=True))
        self.stack.enter_context(patch.object(console, 'RequestBudget', return_value=Mock(
            snapshot=Mock(return_value={'network_retry': {}}))))
        self.stack.enter_context(patch.object(console.urllib.request, 'urlopen',
            side_effect=AssertionError('display status must not contact exchange')))

    def test_connected_display_reuses_worker_evidence_and_returns_copy(self):
        result = console.status()
        self.assertEqual(result['market_stream'], self.worker['market_stream'])
        result['market_stream']['diagnostics']['public']['connection_attempts'] = 99
        self.assertEqual(self.worker['market_stream']['diagnostics']['public']['connection_attempts'], 1)

    def test_unreachable_worker_cannot_present_old_live_stream_snapshot(self):
        self.worker['connected'] = False
        stream = console.status()['market_stream']
        self.assertFalse(stream['available'])
        self.assertEqual(stream['connections'], {'public': False, 'market': False})
        self.assertFalse(stream['symbols']['ETHUSDT']['book_live'])
        self.assertEqual(self.worker['market_stream']['connections']['public'], True)

    def test_missing_worker_snapshot_is_unavailable_even_when_rpc_connected(self):
        self.worker.pop('market_stream')
        stream = console.status()['market_stream']
        self.assertFalse(stream['available'])
        self.assertFalse(stream['connections']['market'])


if __name__ == '__main__':
    unittest.main()
