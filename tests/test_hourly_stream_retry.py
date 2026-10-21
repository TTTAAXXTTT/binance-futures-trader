"""Ten-minute WebSocket reconnects, using virtual time and offline sockets only."""
import json
import sys
import threading
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT)]
from execution.streams import MarketStreams, NETWORK_RETRY_SECONDS, NETWORK_RETRY_VERSION


class VirtualTime:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def wall(self):
        return 1800000000.0 + self.now


class VirtualStop:
    def __init__(self, clock, *, advances=(), on_wait=None, stop_on_wait=None):
        self.clock = clock
        self.advances = list(advances)
        self.on_wait = on_wait
        self.stop_on_wait = stop_on_wait
        self.stopped = False
        self.waits = []

    def is_set(self):
        return self.stopped

    def set(self):
        self.stopped = True

    def clear(self):
        self.stopped = False

    def wait(self, timeout):
        self.waits.append(timeout)
        if self.on_wait:
            self.on_wait()
        if self.stop_on_wait == len(self.waits):
            self.set()
        else:
            self.clock.now += self.advances.pop(0) if self.advances else timeout
        return self.stopped


class OfflineSocket:
    def __init__(self, event, *, on_recv=None, disconnect=False):
        self.event = event
        self.on_recv = on_recv
        self.disconnect = disconnect

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def recv(self, *, timeout):
        if self.on_recv:
            self.on_recv()
        if self.disconnect:
            raise OSError('offline disconnect')
        self.event.set()
        return json.dumps({'data': {'s': 'IGNORED'}})

    def close(self):
        self.event.set()


class Connector:
    def __init__(self, clock, *results):
        self.clock, self.results, self.calls = clock, list(results), []

    def __call__(self, uri, **kwargs):
        self.calls.append((self.clock.now, uri, kwargs))
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class StreamRetryTests(unittest.TestCase):
    def make_streams(self, clock, event, connector):
        return MarketStreams(clock=clock.wall, monotonic=clock.monotonic,
                             stop_event=event, connect=connector)

    def test_constructor_inert_and_snapshot_retains_price_fields(self):
        clock = VirtualTime()
        event = VirtualStop(clock)
        connector = Connector(clock)
        streams = self.make_streams(clock, event, connector)
        snapshot = streams.snapshot()
        self.assertEqual(connector.calls, [])
        self.assertEqual(streams.threads, [])
        self.assertEqual(snapshot['network_retry_version'], NETWORK_RETRY_VERSION)
        self.assertEqual(snapshot['retry_interval_seconds'], 600)
        self.assertEqual(snapshot['connections'], {'public': False, 'market': False})
        self.assertEqual(set(snapshot['symbols']), {'BTCUSDT', 'ETHUSDT'})
        self.assertEqual(snapshot['retry']['market'], {
            'failures': 0, 'waiting': False, 'retry_in_seconds': 0.0,
            'next_retry_at_ms': None})

    def test_no_retry_before_600_second_boundary_even_if_wait_wakes_early(self):
        clock = VirtualTime()
        snapshots = []
        event = VirtualStop(clock, advances=(599, 1),
                            on_wait=lambda: snapshots.append(streams.snapshot()))
        socket = OfflineSocket(event)
        connector = Connector(clock, OSError('offline failed'), socket)
        streams = self.make_streams(clock, event, connector)
        streams._run('public')
        self.assertEqual([call[0] for call in connector.calls], [0, 600])
        self.assertEqual(event.waits, [600, 1])
        self.assertEqual([state['retry']['public']['retry_in_seconds']
                          for state in snapshots], [600, 1])
        self.assertEqual(snapshots[1]['retry']['public']['next_retry_at_ms'],
                         int((clock.wall() - 1 + 1) * 1000))

    def test_repeated_failures_retry_every_600_seconds_and_independent_channel(self):
        clock = VirtualTime()
        failures = []
        event = VirtualStop(clock, on_wait=lambda: failures.append(
            streams.snapshot()['retry']['market']['failures']))
        connector = Connector(clock, OSError('offline first'),
                              OSError('offline second'), OfflineSocket(event))
        streams = self.make_streams(clock, event, connector)
        streams._run('market')
        self.assertEqual([call[0] for call in connector.calls], [0, 600, 1200])
        self.assertEqual(event.waits, [600, 600])
        self.assertEqual(failures, [1, 2])
        self.assertEqual(streams.snapshot()['retry']['public']['failures'], 0)

    def test_connection_success_clears_failure_count_and_deadline(self):
        clock = VirtualTime()
        connected_snapshots = []
        event = VirtualStop(clock)
        socket = OfflineSocket(event, on_recv=lambda: connected_snapshots.append(streams.snapshot()))
        connector = Connector(clock, OSError('offline first'), socket)
        streams = self.make_streams(clock, event, connector)
        streams._run('public')
        connected = connected_snapshots[0]
        self.assertTrue(connected['connections']['public'])
        self.assertEqual(connected['retry']['public'], {
            'failures': 0, 'waiting': False, 'retry_in_seconds': 0.0,
            'next_retry_at_ms': None})
        self.assertEqual(streams.retry['public']['deadline'], None)

    def test_established_connection_disconnect_also_waits_600_seconds(self):
        clock = VirtualTime()
        event = VirtualStop(clock)
        connector = Connector(clock, OfflineSocket(event, disconnect=True), OfflineSocket(event))
        streams = self.make_streams(clock, event, connector)
        streams._run('market')
        self.assertEqual([call[0] for call in connector.calls], [0, 600])
        self.assertEqual(event.waits, [600])

    def test_virtual_stop_interrupts_retry_without_another_connection(self):
        clock = VirtualTime()
        event = VirtualStop(clock, stop_on_wait=1)
        connector = Connector(clock, OSError('offline failed'))
        streams = self.make_streams(clock, event, connector)
        streams._run('public')
        self.assertEqual(len(connector.calls), 1)
        self.assertEqual(clock.now, 0)
        self.assertFalse(streams.snapshot()['retry']['public']['waiting'])

    def test_real_event_stop_wakes_wait_without_waiting_ten_minutes(self):
        class ObservedStop(threading.Event):
            def __init__(self):
                super().__init__()
                self.wait_entered = threading.Event()

            def wait(self, timeout=None):
                self.wait_entered.set()
                return super().wait(timeout)

        event = ObservedStop()
        calls = []

        def connector(*args, **kwargs):
            calls.append((args, kwargs))
            raise OSError('offline failed')

        streams = MarketStreams(connect=connector, stop_event=event)
        worker = threading.Thread(target=streams._run, args=('public',), daemon=True)
        streams.threads = [worker]
        worker.start()
        self.assertTrue(event.wait_entered.wait(timeout=1))
        streams.stop()
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(calls), 1)
        self.assertEqual(streams.threads, [])
        self.assertIsNone(streams.retry['public']['deadline'])

    def test_wall_clock_jump_does_not_advance_retry_deadline(self):
        clock = VirtualTime()
        event = VirtualStop(clock, stop_on_wait=1)
        streams = self.make_streams(clock, event, Connector(clock))
        streams.retry['public'] = {'failures': 1, 'deadline': 600}
        streams.clock = lambda: clock.wall() + 86400
        clock.now = 599
        self.assertEqual(streams.snapshot()['retry']['public']['retry_in_seconds'], 1)
        self.assertFalse(streams._wait_for_retry('public'))
        self.assertEqual(event.waits, [1])


if __name__ == '__main__':
    unittest.main()
