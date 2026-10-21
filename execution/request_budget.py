"""Cross-process Binance IP weight budget and persistent 429/418 cooldown.

This is transport metadata, independent from the existing trading ledger.
Construction is inert. No credentials, URLs, orders, or balances are stored.
"""
import re
import math
import sqlite3
import time
from contextlib import closing
from datetime import timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from .network_retry import NETWORK_RETRY_SECONDS, NetworkRetryGate


class RateLimited(RuntimeError):
    def __init__(self, retry_at, *, clock=time.time, source='local_budget'):
        self.retry_at = float(retry_at)
        self.retry_at_ms = int(self.retry_at * 1000)
        self.retry_in_seconds = max(0, self.retry_at - clock())
        self.wait_seconds = max(1, int(self.retry_in_seconds) + 1)
        self.source = source
        self.recovery_kind = 'exchange_cooldown' if source == 'exchange_cooldown' else 'local_budget_wait'
        self.definite_not_submitted = True
        description = '交易所请求冷却等待中' if source == 'exchange_cooldown' else '本地每分钟请求额度等待中'
        super().__init__(f'{description}，约{self.wait_seconds}秒后重新核对')


def cooldown_deadline(headers=None, *, status=None, code=None, message='', now=None):
    """Compute a real exchange cooldown without credentials or database I/O."""
    if status not in {418, 429} and code != -1003:
        return 0.0
    now = time.time() if now is None else now
    headers = {str(k).lower(): str(v) for k, v in (headers.items() if headers else [])}
    delay = 120 if status == 418 else 60
    retry = headers.get('retry-after', '')
    try:
        seconds = float(retry)
        if math.isfinite(seconds):
            delay = max(delay, seconds)
    except ValueError:
        try:
            delay = max(delay, parsedate_to_datetime(retry).timestamp() - now)
        except (ValueError, TypeError, OverflowError):
            pass
    match = re.search(r'(?:banned until|retry after)\s*(\d{13})', str(message), re.I)
    return max(now + delay, int(match.group(1)) / 1000 if match else 0)


def request_weight(method, path, params=None):
    params = params or {}
    if method.upper() != 'GET':
        return 1
    if path.endswith('/openOrders') or path.endswith('/openAlgoOrders'):
        return 1 if params.get('symbol') else 40
    if path.endswith('/klines'):
        limit = int(params.get('limit', 500))
        return 1 if limit < 100 else 2 if limit < 500 else 5 if limit <= 1000 else 10
    if path.endswith('/positionSide/dual') or path.endswith('/multiAssetsMargin'):
        return 30
    if path.endswith('/account') or path.endswith('/positionRisk') or path.endswith('/accountConfig'):
        return 5
    if path.endswith('/userTrades') or path.endswith('/income'):
        return 5 if path.endswith('/userTrades') else 30
    if path.endswith('/commissionRate'):
        return 20
    if path.endswith('/symbolConfig'):
        return 5
    if path in ('/fapi/v1/ticker/price', '/fapi/v2/ticker/price'):
        return 1 if params.get('symbol') else 2
    if path == '/fapi/v1/ticker/bookTicker':
        return 2 if params.get('symbol') else 5
    if path == '/fapi/v1/ticker/24hr':
        return 1 if params.get('symbol') else 40
    if path == '/fapi/v1/premiumIndex':
        return 1 if params.get('symbol') else 10
    if '/ticker/' in path:
        return 2 if params.get('symbol') else 10
    if path.endswith(('/time', '/exchangeInfo', '/leverageBracket', '/order', '/algoOrder')):
        return 1
    return 20


class RequestBudget:
    def __init__(self, path=None, *, clock=time.time, read_cap=1200, critical_cap=1800):
        self.path = Path(path) if path else Path(__file__).resolve().parents[1] / 'user_data' / 'binance-request-budget.sqlite'
        self.clock, self.read_cap, self.critical_cap = clock, read_cap, critical_cap
        self.network = NetworkRetryGate(self.path, clock=clock)

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=3)
        db.execute('CREATE TABLE IF NOT EXISTS budget (id INTEGER PRIMARY KEY CHECK(id=1), '
            'minute INTEGER NOT NULL, used INTEGER NOT NULL, cooldown REAL NOT NULL)')
        db.execute('INSERT OR IGNORE INTO budget VALUES (1,0,0,0)')
        db.execute('CREATE TABLE IF NOT EXISTS request_usage (minute INTEGER NOT NULL, '
            'method TEXT NOT NULL, endpoint TEXT NOT NULL, calls INTEGER NOT NULL, weight INTEGER NOT NULL, '
            'PRIMARY KEY(minute,method,endpoint))')
        db.commit()
        return db

    def reserve(self, method, path, params=None, *, critical=False):
        now = self.clock()
        minute = int(now // 60)
        cost = request_weight(method, path, params)
        with closing(self._connect()) as db:
            with db:
                db.execute('BEGIN IMMEDIATE')
                previous, used, cooldown = db.execute('SELECT minute,used,cooldown FROM budget WHERE id=1').fetchone()
                if cooldown > now:
                    raise RateLimited(cooldown, clock=self.clock, source='exchange_cooldown')
                if minute > previous:
                    used = 0
                # Never move an observed exchange window backwards because
                # the Windows clock trails it around the minute boundary.
                minute = max(minute, previous)
                cap = self.critical_cap if critical else self.read_cap
                if used + cost > cap:
                    raise RateLimited((minute + 1) * 60 + .5, clock=self.clock, source='local_budget')
                db.execute('UPDATE budget SET minute=?,used=?,cooldown=? WHERE id=1', (minute, used + cost, cooldown))
                # Keep aggregate endpoints only, never URL parameters, account IDs or keys.
                db.execute('INSERT INTO request_usage VALUES(?,?,?,?,?) ON CONFLICT(minute,method,endpoint) '
                    'DO UPDATE SET calls=calls+1,weight=weight+excluded.weight',
                    (minute, method.upper(), path.split('?', 1)[0], 1, cost))
                db.execute('DELETE FROM request_usage WHERE minute<?', (minute - 60,))
        return minute

    def observe(self, headers=None, *, status=None, code=None, message='', path=None,
                request_minute=None):
        headers = {str(k).lower(): str(v) for k, v in (headers.items() if headers else [])}
        now, used = self.clock(), None
        inaccurate_weight = path in ('/fapi/v1/ticker/bookTicker', '/fapi/v2/ticker/price')
        try:
            used = int(headers.get('x-mbx-used-weight-1m', ''))
            if used < 0:
                used = None
        except ValueError:
            pass
        cooldown = cooldown_deadline(headers, status=status, code=code, message=message, now=now)
        # Headers received across a minute boundary must not contaminate a later window.
        # A header without a Date has no receive-time window identity. Only
        # the caller's original request association can make it usable.
        minute = request_minute if type(request_minute) is int and request_minute >= 0 else None
        date = headers.get('date')
        if date:
            try:
                parsed = parsedate_to_datetime(date)
                parsed = parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
                minute = int(parsed.timestamp() // 60)
            except (ValueError, TypeError, OverflowError):
                pass
        with closing(self._connect()) as db:
            with db:
                db.execute('BEGIN IMMEDIATE')
                previous, old_used, old_cooldown = db.execute('SELECT minute,used,cooldown FROM budget WHERE id=1').fetchone()
                if inaccurate_weight or minute is None:
                    # Binance documents this header as unreliable. Keep both
                    # existing weight and its window; still persist real cooldown.
                    minute, updated = previous, old_used
                elif previous == minute:
                    updated = max(old_used, used or 0)
                elif minute > previous:
                    updated = used or 0
                else:
                    minute, updated = previous, old_used
                db.execute('UPDATE budget SET minute=?,used=?,cooldown=? WHERE id=1',
                    (minute, updated, max(cooldown, old_cooldown)))

    def snapshot(self):
        with closing(self._connect()) as db:
            minute, used, cooldown = db.execute('SELECT minute,used,cooldown FROM budget WHERE id=1').fetchone()
            current = int(self.clock() // 60)
            usage = [{'method': row[0], 'endpoint': row[1], 'calls': row[2], 'reserved_weight': row[3]}
                for row in db.execute('SELECT method,endpoint,calls,weight FROM request_usage '
                    'WHERE minute=? ORDER BY weight DESC LIMIT 8', (current,))]
            peak = db.execute('SELECT COALESCE(MAX(total),0) FROM '
                '(SELECT SUM(weight) AS total FROM request_usage WHERE minute>=? GROUP BY minute)',
                (current - 60,)).fetchone()[0]
        now = self.clock()
        return {'used_weight': used if minute >= int(now // 60) else 0,
            'read_budget_per_minute': self.read_cap, 'critical_budget_per_minute': self.critical_cap,
            'cooldown_until_ms': int(cooldown * 1000) if cooldown > now else 0,
            'cooldown_seconds': max(0, int(cooldown - now) + 1) if cooldown > now else 0,
            'current_endpoint_usage': usage, 'reserved_peak_weight_last_hour': peak,
            'network_retry': self.network.snapshot()}
