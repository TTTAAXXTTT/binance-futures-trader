"""Persistent GET connection recovery; never authorizes or sends trading writes."""
import threading
import time
import uuid
from contextlib import closing, contextmanager
import sqlite3
from pathlib import Path

NETWORK_RETRY_SECONDS = 600
NETWORK_RETRY_VERSION = 'ten-minute-retry-v1'


class NetworkBackoff(RuntimeError):
    def __init__(self, state, now, wall):
        self.wait_seconds = max(0, max(state['retry_at'], state['probe_until']) - now)
        self.retry_at = wall + self.wait_seconds
        # Stable text is essential: engine events deduplicate by reason.
        self.message = state['reason'] or '连接读取等待中；每10分钟自动重试'
        super().__init__(self.message)


class NetworkRetryGate:
    def __init__(self, path=None, *, clock=time.time, monotonic=None):
        self.path = Path(path) if path is not None else None
        self.clock = clock
        self.monotonic = monotonic or (time.monotonic if clock is time.time else clock)
        self.lock = threading.RLock()
        self.local = self._initial()

    @staticmethod
    def _initial():
        return dict(generation=0, failures=0, retry_at=0.0, probe_until=0.0,
                    probe_id='', reason='', last_success=0.0, last_seen_mono=0.0)

    def _after_reboot(self, state):
        now = self.monotonic()
        if now < state['last_seen_mono']:
            # QPC/monotonic is shared by processes on Windows. On an OS reboot
            # its epoch can reset; carry at most the remaining 600-second wait.
            remaining = min(NETWORK_RETRY_SECONDS,
                            max(0, state['retry_at'] - state['last_seen_mono']))
            state.update(generation=state['generation'] + 1,
                         retry_at=now + remaining if state['retry_at'] else 0.0,
                         probe_until=0.0, probe_id='', last_seen_mono=now)
            return True
        return False

    @contextmanager
    def _state(self, *, write=False):
        with self.lock:
            if self.path is None:
                self._after_reboot(self.local)
                yield self.local
                return
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with closing(sqlite3.connect(self.path, timeout=3)) as db:
                with db:
                    db.execute('CREATE TABLE IF NOT EXISTS network_retry '
                        '(id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL, '
                        'failures INTEGER NOT NULL, retry_at REAL NOT NULL, probe_until REAL NOT NULL, '
                        'probe_id TEXT NOT NULL, reason TEXT NOT NULL, last_success REAL NOT NULL, '
                        'last_seen_mono REAL NOT NULL DEFAULT 0)')
                    columns = {row[1] for row in db.execute('PRAGMA table_info(network_retry)')}
                    if 'last_seen_mono' not in columns:
                        db.execute('ALTER TABLE network_retry ADD COLUMN last_seen_mono REAL NOT NULL DEFAULT 0')
                        # Compatibility with the initial wall-clock draft.
                        old = db.execute('SELECT retry_at FROM network_retry WHERE id=1').fetchone()
                        if old and old[0]:
                            remaining = min(NETWORK_RETRY_SECONDS, max(0, old[0] - self.clock()))
                            db.execute("UPDATE network_retry SET retry_at=?,probe_until=0,probe_id='',last_seen_mono=? WHERE id=1",
                                       (self.monotonic() + remaining, self.monotonic()))
                    db.execute("INSERT OR IGNORE INTO network_retry VALUES (1,0,0,0,0,'','',0,0)")
                    row = db.execute('SELECT generation,failures,retry_at,probe_until,probe_id,reason,last_success,last_seen_mono '
                                     'FROM network_retry WHERE id=1').fetchone()
                    reboot_state = dict(zip(self._initial(), row))
                    if self._after_reboot(reboot_state):
                        self._save(db, reboot_state)
                with db:
                    if write:
                        db.execute('BEGIN IMMEDIATE')
                    row = db.execute('SELECT generation,failures,retry_at,probe_until,probe_id,reason,last_success,last_seen_mono '
                                     'FROM network_retry WHERE id=1').fetchone()
                    state = dict(zip(self._initial(), row))
                    yield state
                    if write:
                        self._save(db, state)

    @staticmethod
    def _save(db, state):
        db.execute('UPDATE network_retry SET generation=?,failures=?,retry_at=?,probe_until=?, '
                   'probe_id=?,reason=?,last_success=?,last_seen_mono=? WHERE id=1', tuple(state.values()))

    @staticmethod
    def _blocked(state, now):
        # A healthy manual inspection owns recovery authority, not all healthy
        # trading reads. Those reads carry generation-only tickets and cannot
        # complete or release the inspection's lease.
        return state['retry_at'] > now or (bool(state['retry_at']) and state['probe_until'] > now)

    def check(self, ticket=None):
        with self._state() as state:
            now = self.monotonic()
            if ticket is not None:
                if (ticket == (state['generation'], state['probe_id'])
                        and state['probe_until'] > now):
                    return
                raise NetworkBackoff(state, now, self.clock())
            if self._blocked(state, now):
                raise NetworkBackoff(state, now, self.clock())

    def manual_claim(self, *, probe_seconds=30):
        """One shared, bounded recovery lease; only the network wait is bypassed.

        Healthy reads also acquire this lease, so an automatic read cannot race
        a multi-request manual probe and release it after its first response.
        Exchange cooldowns remain exclusively owned by RequestBudget.
        """
        with self._state(write=True) as state:
            now = self.monotonic()
            if state['probe_until'] > now:
                raise NetworkBackoff(state, now, self.clock())
            token = uuid.uuid4().hex
            state.update(probe_id=token, probe_until=now + probe_seconds,
                         last_seen_mono=now)
            return state['generation'], token

    def manual_complete(self, ticket):
        """Commit the entire probe only while its original lease is current."""
        with self._state(write=True) as state:
            now = self.monotonic()
            if (ticket != (state['generation'], state['probe_id'])
                    or state['probe_until'] <= now):
                return False
            state.update(failures=0, retry_at=0.0, probe_until=0.0, probe_id='',
                         reason='', last_success=self.clock(), last_seen_mono=now)
            return True

    def before_read(self, *, probe_seconds=30):
        with self._state(write=True) as state:
            now = self.monotonic()
            if self._blocked(state, now):
                raise NetworkBackoff(state, now, self.clock())
            token = ''
            if state['retry_at']:
                # Only one process may make the first recovery probe. Claim at
                # wire time, after any nested signed-request time calibration.
                token = uuid.uuid4().hex
                state.update(probe_id=token, probe_until=now + max(30, probe_seconds), last_seen_mono=now)
            return state['generation'], token

    def failed(self, reason, *, ticket=None, preserve_deadline=False):
        with self._state(write=True) as state:
            now = self.monotonic()
            if ticket is not None and (ticket != (state['generation'], state['probe_id'])
                                      or state['probe_until'] <= now):
                return False  # A late manual result cannot overwrite a new fault.
            retry_at = (state['retry_at'] if preserve_deadline and state['retry_at']
                        else now + NETWORK_RETRY_SECONDS)
            state.update(generation=state['generation'] + 1, failures=state['failures'] + 1,
                         retry_at=retry_at, probe_until=0.0, probe_id='',
                         reason=str(reason), last_seen_mono=now)
            return True

    def succeeded(self, ticket):
        if ticket is None:
            return
        generation, token = ticket
        with self._state(write=True) as state:
            if generation != state['generation']:
                return  # An older in-flight success cannot erase a newer failure.
            if state['probe_id'] and token != state['probe_id']:
                return  # A healthy concurrent GET does not own a manual lease.
            if state['retry_at'] and (not token or token != state['probe_id']):
                return
            state.update(failures=0, retry_at=0.0, probe_until=0.0, probe_id='',
                         reason='', last_success=self.clock(), last_seen_mono=self.monotonic())

    def defer_probe(self, ticket):
        """A definite exchange rejection is not a successful recovery probe."""
        if ticket is None or not ticket[1]:
            return
        with self._state(write=True) as state:
            if ticket == (state['generation'], state['probe_id']):
                state.update(retry_at=self.monotonic() + NETWORK_RETRY_SECONDS,
                             probe_until=0.0, probe_id='', last_seen_mono=self.monotonic())

    def release_probe(self, ticket):
        """No wire attempt occurred: retain the original recovery deadline."""
        if ticket is None or not ticket[1]:
            return
        with self._state(write=True) as state:
            if ticket == (state['generation'], state['probe_id']):
                state.update(probe_until=0.0, probe_id='', last_seen_mono=self.monotonic())

    def snapshot(self):
        with self._state() as state:
            now = self.monotonic()
            effective = max(state['retry_at'], state['probe_until']) if state['retry_at'] else 0
            return dict(version=NETWORK_RETRY_VERSION, interval_seconds=NETWORK_RETRY_SECONDS,
                        generation=state['generation'],
                        waiting=effective > now, retry_in_seconds=max(0, round(effective - now, 1)),
                        retry_at_ms=int((self.clock() + max(0, effective - now)) * 1000) if effective else 0,
                        failures=state['failures'],
                        last_failure_reason=state['reason'] or None,
                        probing=state['probe_until'] > now,
                        recovering=bool(state['retry_at']), last_success_ms=int(state['last_success'] * 1000))
