"""Manual registration/display boundaries; fake RPC and no real services."""
import copy
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from control import hourly_app as console
from execution.status_view import age_status


ROOT = Path(__file__).resolve().parents[1]


def task(state='accepted', *, updated=1000, id='check-1'):
    return {'version': 1, 'id': id, 'state': state, 'stage': 'rest',
            'requested_at_ms': 1000, 'started_at_ms': 1000 if state != 'accepted' else None,
            'completed_at_ms': updated if state == 'complete' else None,
            'deadline_at_ms': 61000, 'updated_at_ms': updated, 'reason': None,
            'result': {'rest': None, 'streams': {}, 'unknown_orders': None}}


class DisplayFreshnessTests(unittest.TestCase):
    def snapshot(self, *, age=0):
        return {'server_time_ms': 1000, 'server_time_upper_ms': 1100,
                'market_stream': {'connections': {'public': True, 'market': True},
                    'retry': {'market': {'waiting': True, 'retry_in_seconds': 480}},
                    'symbols': {symbol: {
                        'book_live': True, 'trade_live': True, 'mark_live': True,
                        'latest_trade': {'price': 2500},
                        'freshness': {kind: {'available': True, 'reason': None,
                            'receive_age_ms': age, 'event_age_upper_ms': age}
                            for kind in ('book', 'trade', 'mark')}}
                        for symbol in ('BTCUSDT', 'ETHUSDT')}}}

    def test_age_moves_all_channels_and_symbols_without_mutation(self):
        source = self.snapshot(age=500)
        original = copy.deepcopy(source)
        result = age_status(source, 1)
        for symbol in result['market_stream']['symbols'].values():
            for kind in ('book', 'trade', 'mark'):
                self.assertEqual(symbol['freshness'][kind]['receive_age_ms'], 1500)
                self.assertEqual(symbol['freshness'][kind]['event_age_upper_ms'], 1500)
                self.assertTrue(symbol[kind + '_live'])
        self.assertEqual(result['market_stream']['retry']['market']['retry_in_seconds'], 479)
        self.assertEqual(source, original)

    def test_exact_three_second_limit_preserved_then_copy_expires(self):
        source = self.snapshot()
        exact = age_status(source, 3)
        self.assertTrue(exact['market_stream']['symbols']['ETHUSDT']['trade_live'])
        expired = age_status(exact, .001)
        stream = expired['market_stream']
        self.assertTrue(stream['display_copy_stale'])
        self.assertEqual(stream['connections'], {'public': True, 'market': True})
        for symbol in stream['symbols'].values():
            for kind in ('book', 'trade', 'mark'):
                self.assertFalse(symbol[kind + '_live'])
                self.assertFalse(symbol['freshness'][kind]['available'])
                self.assertTrue(symbol['freshness'][kind]['source_available'])
                self.assertEqual(symbol['freshness'][kind]['display_reason'], 'display_copy_expired')

    def test_old_quote_expires_before_display_copy_limit(self):
        aged = age_status(self.snapshot(age=2900), .101)
        self.assertFalse(aged['market_stream']['display_copy_stale'])
        self.assertFalse(aged['market_stream']['symbols']['ETHUSDT']['trade_live'])
        self.assertFalse(aged['market_stream']['symbols']['ETHUSDT']['freshness']['trade']['available'])

    def test_previous_unavailable_quote_never_becomes_fresh(self):
        source = self.snapshot()
        detail = source['market_stream']['symbols']['BTCUSDT']['freshness']['book']
        detail.update(available=False, reason='clock_uncalibrated')
        result = age_status(source, .1)
        self.assertFalse(result['market_stream']['symbols']['BTCUSDT']['book_live'])
        self.assertEqual(result['market_stream']['symbols']['BTCUSDT']['freshness']['book']['reason'],
                         'clock_uncalibrated')

    def test_legacy_snapshot_keeps_compatible_boundaries(self):
        source = {'market_stream': {'symbols': {'ETHUSDT': {'book_live': True,
                  'trade_live': True, 'mark_live': True}}}}
        self.assertTrue(age_status(source, 3)['market_stream']['symbols']['ETHUSDT']['book_live'])
        self.assertFalse(age_status(source, 3.1)['market_stream']['symbols']['ETHUSDT']['book_live'])

    def test_deadline_countdown_does_not_finalize_task_or_clear_unknown(self):
        source = self.snapshot()
        source.update(connection_check=dict(task('checking'), retry_at_ms=5000),
                      unknown_orders=[{'id': 'uncertain'}])
        result = age_status(source, 60)
        check = result['connection_check']
        self.assertTrue(check['deadline_elapsed'])
        self.assertEqual(check['state'], 'checking')
        self.assertEqual(check['retry_in_seconds'], 0)
        self.assertEqual(result['unknown_orders'], source['unknown_orders'])


class ConsoleConnectionCheckTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.dict(console.STATE, {'worker': {}, 'account': {},
            'events': [], 'connection_check': None}, clear=True))
        self.rpc = self.stack.enter_context(patch.object(console, 'hourly_rpc', return_value={
            'ok': True, 'merged': False, 'connection_check': task()}))
        for name in ('fresh', 'launch_stopped_worker', 'saved_user_intent'):
            self.stack.enter_context(patch.object(console, name,
                side_effect=AssertionError('Connection check must not start/recover or sweep account')))
        self.client = TestClient(console.app)

    def test_registers_once_with_short_deadline_without_changing_pause(self):
        console.STATE['worker'] = {'state': 'paused', 'desired_running': False,
                                   'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True}}
        before = copy.deepcopy(console.STATE['worker'])
        response = self.client.post('/api/check-connection', json={'confirmed': True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['connection_check']['id'], 'check-1')
        self.assertIn('受理不代表', response.json()['message'])
        self.rpc.assert_called_once_with('check-connection', 'POST', {'confirmed': True}, timeout=4)
        self.assertEqual(console.STATE['worker'], before)

    def test_requires_confirmation_and_rejects_foreign_origin(self):
        self.assertEqual(self.client.post('/api/check-connection', json={}).status_code, 422)
        self.assertEqual(self.client.post('/api/check-connection', json={'confirmed': True},
            headers={'origin': 'https://other.invalid'}).status_code, 403)
        self.rpc.assert_not_called()

    def test_merged_reply_is_not_reported_as_successful_connection(self):
        self.rpc.return_value['merged'] = True
        result = console.check_connection({'confirmed': True})
        self.assertTrue(result['merged'])
        self.assertIn('合并', result['message'])
        self.assertEqual(result['connection_check']['state'], 'accepted')

    def test_transport_timeout_is_unknown_registration_and_never_replayed(self):
        self.rpc.side_effect = TimeoutError('secret-network-details')
        result = self.client.post('/api/check-connection', json={'confirmed': True})
        self.assertEqual(result.status_code, 503)
        self.assertNotIn('secret', result.text)
        self.assertIn('未自动重发', result.json()['detail'])
        self.rpc.assert_called_once()
        self.assertIsNone(console.STATE['connection_check'])

    def test_invalid_worker_acknowledgement_never_invents_check_task(self):
        for reply in ({'ok': True}, {'ok': True, 'connection_check': task('stopped')}, [], None):
            self.rpc.return_value = reply
            result = self.client.post('/api/check-connection', json={'confirmed': True})
            self.assertEqual(result.status_code, 503)
        self.assertIsNone(console.STATE['connection_check'])

    def test_late_accepted_response_cannot_replace_newer_completed_check(self):
        console.STATE['connection_check'] = task('complete', updated=5000)
        reply = console.check_connection({'confirmed': True})
        self.assertEqual(reply['connection_check']['state'], 'complete')

    def test_next_task_can_replace_old_completed_task(self):
        console.STATE['connection_check'] = task('complete', updated=5000)
        self.rpc.return_value['connection_check'] = dict(task(id='check-2'), requested_at_ms=6000)
        self.assertEqual(console.check_connection({'confirmed': True})['connection_check']['id'], 'check-2')

    def test_status_publishes_latest_check_and_ages_without_network_or_pause_change(self):
        stream = DisplayFreshnessTests().snapshot()['market_stream']
        console.STATE.update(worker={'connected': True, 'state': 'paused', 'desired_running': False,
            'server_time_ms': 2000, 'server_time_upper_ms': 2100, 'market_stream': stream,
            'symbol_enabled': {'BTCUSDT': False, 'ETHUSDT': True},
            'connection_check': task('accepted')}, worker_observed_monotonic=10,
            connection_check=task('complete', updated=1500))
        with patch.object(console, 'saved_user_intent', return_value={'desired_running': False}), \
                patch.object(console, 'trade_controls', return_value={'desired_running': False,
                    'symbols': {'BTCUSDT': False, 'ETHUSDT': True}}), \
                patch.object(console, 'start_reasons', return_value=[]), \
                patch.object(console.compat, 'read_json', return_value={}), \
                patch('control.installation.configured', return_value=True), \
                patch.object(console.time, 'monotonic', return_value=14), \
                patch.object(console, 'RequestBudget', return_value=Mock(
                    snapshot=Mock(return_value={'network_retry': {}}))):
            result = console.status()
        self.assertEqual(result['connection_check']['state'], 'complete')
        self.assertFalse(result['desired_running'])
        self.assertFalse(result['market_stream']['symbols']['ETHUSDT']['trade_live'])
        self.assertFalse(result['market_stream']['symbols']['ETHUSDT']['freshness']['trade']['available'])
        self.assertTrue(result['market_stream']['connections']['market'])
        self.assertTrue(console.STATE['worker']['market_stream']['symbols']['ETHUSDT']['trade_live'])
        self.rpc.assert_not_called()

    def test_slow_registration_does_not_hold_control_action_lock(self):
        entered, release = threading.Event(), threading.Event()
        def delayed(*_args, **_kwargs):
            entered.set()
            if not release.wait(2):
                raise RuntimeError('test synchronization timeout')
            return {'ok': True, 'connection_check': task()}
        self.rpc.side_effect = delayed
        results = []
        thread = threading.Thread(target=lambda: results.append(console.check_connection({'confirmed': True})))
        try:
            thread.start()
            self.assertTrue(entered.wait(1))
            self.assertTrue(console.ACTION_LOCK.acquire(blocking=False))
            console.ACTION_LOCK.release()
        finally:
            release.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(results[0]['ok'])


class FrontendConnectionCheckTests(unittest.TestCase):
    def test_served_bundle_contains_new_button_and_uses_current_hashed_asset(self):
        html = (ROOT / 'frequi/dist/index.html').read_text(encoding='utf-8')
        import re
        scripts = re.findall(r'src="(/assets/[^"]+\.js)"', html)
        self.assertEqual(len(scripts), 1)
        source = (ROOT / 'frequi/dist' / scripts[0].lstrip('/')).read_text(encoding='utf-8')
        self.assertIn('立即检查连接', source)
        self.assertIn('/api/check-connection', source)
        self.assertIn('显示副本已过期', source)
        self.assertIn('受理不代表', source)


class ConnectionCheckOrderingTests(unittest.TestCase):
    def check(self, ident, revision=None, *, requested=10000, updated=11000, state='accepted'):
        value = dict(task(state, id=ident, updated=updated), requested_at_ms=requested)
        if revision is not None:
            value['revision'] = revision
        return value

    def test_new_task_survives_wall_clock_rollback(self):
        old = self.check('old', 10, state='complete')
        new = self.check('new', 11, requested=1000, updated=1000)
        self.assertEqual(console.latest_connection_check(old, new), new)
        self.assertEqual(console.latest_connection_check(new, old), new)

    def test_same_task_phase_update_survives_wall_clock_rollback(self):
        old = self.check('same', 11, state='checking')
        new = self.check('same', 12, requested=10000, updated=1000, state='complete')
        self.assertEqual(console.latest_connection_check(old, new), new)

    def test_late_old_task_cannot_replace_revision_even_with_later_wall_time(self):
        current = self.check('new', 12, requested=1000, updated=1000)
        late = self.check('old', 10, requested=10000, updated=20000, state='complete')
        self.assertEqual(console.latest_connection_check(current, late), current)

    def test_late_old_phase_cannot_replace_completed_revision(self):
        current = self.check('same', 12, updated=1000, state='complete')
        late = self.check('same', 11, updated=20000, state='checking')
        self.assertEqual(console.latest_connection_check(current, late), current)

    def test_legacy_snapshots_keep_timestamp_ordering(self):
        old = self.check('old', requested=1000, updated=9000, state='complete')
        new = self.check('new', requested=2000, updated=2000)
        self.assertEqual(console.latest_connection_check(old, new), new)
        completed = self.check('new', requested=2000, updated=3000, state='complete')
        self.assertEqual(console.latest_connection_check(completed, new), completed)

    def test_revisioned_snapshot_replaces_legacy_after_wall_clock_rollback(self):
        legacy = self.check('legacy', requested=10000, updated=20000, state='complete')
        new = self.check('new', 1, requested=1000, updated=1000)
        self.assertEqual(console.latest_connection_check(legacy, new), new)
        self.assertEqual(console.latest_connection_check(new, legacy), new)

    def test_ordering_returns_independent_copy(self):
        source = self.check('new', 2)
        chosen = console.latest_connection_check(self.check('old', 1), source)
        chosen['result']['rest'] = True
        self.assertIsNone(source['result']['rest'])


if __name__ == '__main__':
    unittest.main()
