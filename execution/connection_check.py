"""Durable, bounded connection inspection; never changes trading state.

One worker owns the task. REST uses a dedicated GET-only client and the existing
cross-process recovery gate. Slow I/O never holds the engine or status locks.
"""
import copy
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid

VERSION = 'manual-connection-check-v1'
ACTIVE = {'accepted', 'checking'}


def owner_alive(pid, created):
    try:
        import psutil
        process = psutil.Process(pid)
        return process.is_running() and abs(process.create_time() - created) < 1
    except Exception:
        return False


class ConnectionCheck:
    def __init__(self, path, client_factory, streams=None, *, context_supplier=lambda: {},
                 clock=time.time, monotonic=time.monotonic, deadline_seconds=30,
                 owner_check=owner_alive, process_id=None):
        if not 0 < float(deadline_seconds) <= 120:
            raise ValueError('检查期限必须大于0且不超过120秒')
        self.path = Path(path)
        self.client_factory, self.streams = client_factory, streams
        self.context_supplier = context_supplier
        self.clock, self.monotonic = clock, monotonic
        self.deadline_seconds, self.owner_check = float(deadline_seconds), owner_check
        self.owner = process_id or uuid.uuid4().hex
        self.pid = os.getpid()
        try:
            import psutil
            self.created = psutil.Process(self.pid).create_time()
        except Exception:
            self.created = 0
        self.lock = threading.RLock()
        self._task, self._thread, self._timer = None, None, None
        self._deadline = None
        self._stop = threading.Event()
        self._notification = None

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=.1)
        db.execute('CREATE TABLE IF NOT EXISTS connection_check '
                   '(id INTEGER PRIMARY KEY CHECK(id=1), value TEXT NOT NULL)')
        db.commit()
        return db

    def restore(self):
        """Read only task metadata, preserving a live owner's in-flight lease."""
        if not self.path.is_file():
            return
        with self.lock, closing(self._connect()) as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()
            self._task = json.loads(row[0]) if row else None
            if self._task and self._task.get('active') and not self.owner_check(
                    self._task.get('owner_pid'), self._task.get('owner_created', -1)):
                self._task.update(active=False, state='interrupted', stage='interrupted',
                    reason='上次检查随进程结束而中断；可重新发起，不沿用旧成功结果',
                    completed_at_ms=self._now())
                self._save(db)
            elif self._task and self._task.get('active') and self._task.get('owner') != self.owner:
                self._start_follower()

    def _now(self):
        return round(self.clock() * 1000)

    def _save(self, db, *, enforce_deadline=False):
        self._task['revision'] = int(self._task.get('revision', 0)) + 1
        self._task['updated_at_ms'] = self._now()
        encoded = json.dumps(self._task, ensure_ascii=False, allow_nan=False)
        if enforce_deadline:
            self._require_live()
        db.execute('INSERT INTO connection_check VALUES(1,?) ON CONFLICT(id) '
                   'DO UPDATE SET value=excluded.value', (encoded,))
        if enforce_deadline:
            self._require_live()
        db.commit()

    def _update(self, identifier, *, final=False, **changes):
        with self.lock:
            if not self._task or self._task['id'] != identifier:
                return False
            if self._task.get('owner') != self.owner:
                return False
            if self._task.get('state') in ('complete', 'failed', 'interrupted') and not final:
                return False
            with closing(self._connect()) as db:
                row = db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()
                if row:
                    saved = json.loads(row[0])
                    if saved.get('id') != identifier or saved.get('owner') != self.owner:
                        return False
                previous = copy.deepcopy(self._task)
                try:
                    self._task.update(copy.deepcopy(changes))
                    self._save(db, enforce_deadline=changes.get('state') == 'complete')
                    if changes.get('state') == 'complete':
                        self._require_live()
                except Exception:
                    self._task = previous
                    raise
            return True

    def snapshot(self):
        with self.lock:
            value = copy.deepcopy(self._task)
            if not value:
                return {'version': VERSION, 'state': 'idle', 'active': False}
            for key in ('owner', 'owner_pid', 'owner_created', 'context'):
                value.pop(key, None)
            elapsed = bool(self._deadline is not None and self.monotonic() >= self._deadline)
            value['deadline_elapsed'] = elapsed or bool(value.get('deadline_elapsed'))
            if elapsed and value.get('active') and value['state'] in ACTIVE:
                value.update(state='failed', stage='deadline',
                    reason='检查超过期限；等待在途请求结束，不重复发送')
            value['retry_in_seconds'] = max(0, (value.get('retry_at_ms', 0) - self._now()) / 1000)
            return value

    def request(self):
        with self.lock:
            if self._stop.is_set():
                raise RuntimeError('检查服务正在结束，未登记新的检查')
            with closing(self._connect()) as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()
                previous = json.loads(row[0]) if row else None
                if previous and previous.get('active'):
                    same_owner = previous.get('owner') == self.owner
                    live_thread = same_owner and self._thread is not None and self._thread.is_alive()
                    if live_thread or (not same_owner and self.owner_check(
                            previous.get('owner_pid'), previous.get('owner_created', -1))):
                        self._task = previous
                        if not same_owner:
                            self._start_follower()
                        return {'ok': True, 'merged': True, 'connection_check': self.snapshot()}
                if previous and previous.get('state') == 'waiting' and previous.get('retry_at_ms', 0) > self._now():
                    self._task = previous
                    return {'ok': True, 'merged': True, 'connection_check': self.snapshot()}
                context = copy.deepcopy(self.context_supplier())
                self._deadline = self.monotonic() + self.deadline_seconds
                self._task = {'version': VERSION, 'id': uuid.uuid4().hex, 'state': 'accepted',
                    'stage': 'accepted', 'active': True, 'requested_at_ms': self._now(),
                    'started_at_ms': None, 'completed_at_ms': None,
                    'deadline_at_ms': self._now() + round(self.deadline_seconds * 1000),
                    'reason': '已登记，后台即将进行新鲜连接检查', 'retry_at_ms': 0,
                    'revision': int((previous or {}).get('revision', 0)), 'result': {},
                    'context': context, 'owner': self.owner, 'owner_pid': self.pid,
                    'owner_created': self.created}
                self._save(db)
            identifier = self._task['id']
            self._timer = threading.Timer(self.deadline_seconds, self._expire, args=(identifier,))
            self._timer.daemon = True
            self._thread = threading.Thread(target=self._run, args=(identifier, context), daemon=True,
                                            name='connection-inspection')
            self._timer.start()
            self._thread.start()
            return {'ok': True, 'merged': False, 'connection_check': self.snapshot()}

    def _expire(self, identifier):
        try:
            with self.lock:
                if (not self._task or self._task['id'] != identifier or
                        self._task.get('state') not in ACTIVE or not self._task.get('active')):
                    return
                self._update(identifier, state='failed', stage='deadline', deadline_elapsed=True,
                    completed_at_ms=self._now(), reason='检查超过期限；等待在途请求结束，不重复发送')
        except Exception:
            pass  # snapshot still derives expiration; no trading state is touched.

    def _require_live(self):
        if self._stop.is_set() or self.monotonic() >= self._deadline:
            raise TimeoutError('connection_check_deadline')

    def _start_follower(self):
        """A duplicate observer follows metadata; it never starts exchange I/O."""
        if self._thread is not None and self._thread.is_alive():
            return
        def follow():
            while not self._stop.wait(.05):
                try:
                    with closing(self._connect()) as db:
                        row = db.execute('SELECT value FROM connection_check WHERE id=1').fetchone()
                    with self.lock:
                        if not row:
                            return
                        if self._task and self._task.get('owner') == self.owner:
                            return  # A new local task supersedes this observer.
                        self._task = json.loads(row[0])
                        if not self._task.get('active'):
                            return
                except sqlite3.Error:
                    continue
        self._thread = threading.Thread(target=follow, daemon=True, name='connection-inspection-observer')
        self._thread.start()

    def _run(self, identifier, context):
        client = None
        result = {}
        try:
            self._update(identifier, state='checking', stage='rest', started_at_ms=self._now(),
                         reason='正在新鲜读取交易所时间与账户')
            client = self.client_factory()
            with client.manual_connection_check(deadline_seconds=max(.001, self._deadline - self.monotonic())) as evidence:
                self._require_live()
                client.sync_time()
                account = client.account()
                positions = client.positions()
                if not isinstance(account, dict) or not isinstance(positions, list):
                    raise ValueError('invalid_exchange_shape')
                ordinary_count, conditional_count = 0, 0
                for symbol in ('BTCUSDT', 'ETHUSDT'):
                    self._require_live()
                    ordinary = client.open_orders(symbol)
                    conditional = client.open_algos(symbol)
                    if not isinstance(ordinary, list) or not isinstance(conditional, list):
                        raise ValueError('invalid_order_shape')
                    ordinary_count += len(ordinary)
                    conditional_count += len(conditional)
                # Observe only. The engine's standard reconcile owns all financial writes.
                result['rest'] = {'ok': True, 'state': 'complete', 'reason': None,
                    'checked_at_ms': self._now(), 'position_count': len(positions),
                    'ordinary_order_count': ordinary_count, 'conditional_order_count': conditional_count}
                unknown = context.get('unknown_orders') or []
                queried, unresolved = 0, 0
                self._update(identifier, stage='unknown_orders', result=result,
                    reason='核对未确定订单的原身份；不重新提交订单')
                for order in unknown:
                    self._require_live()
                    if order.get('identity_unavailable'):
                        unresolved += 1
                        continue
                    if order.get('kind') == 'stop':
                        raw = client.query_algo(client_id=order.get('client_id'), algo_id=order.get('order_id'))
                    else:
                        raw = client.query_order(order['symbol'], client_id=order.get('client_id'),
                                                 order_id=order.get('order_id'))
                    queried += 1
                    identity_field = 'algoId' if order.get('kind') == 'stop' else 'orderId'
                    client_field = 'clientAlgoId' if order.get('kind') == 'stop' else 'clientOrderId'
                    identified = isinstance(raw, dict) and raw.get('status', raw.get('algoStatus'))
                    if identified and order.get('order_id') is not None:
                        identified = str(raw.get(identity_field)) == str(order['order_id'])
                    elif identified:
                        identified = raw.get(client_field) == order.get('client_id') and bool(order.get('client_id'))
                    if identified:
                        identified = raw.get('symbol') == order.get('symbol')
                    unresolved += not identified
                result['unknown_orders'] = {'ok': unresolved == 0, 'count': len(unknown),
                    'queried': queried, 'unresolved': unresolved,
                    'state': 'observed_only' if unknown else 'none'}
                self._require_live()
            result['transport'] = evidence.snapshot()
            self._require_live()
            if self.streams is not None:
                stream = self.streams.snapshot()
                disconnected = [c for c in ('public', 'market') if not stream.get('connections', {}).get(c)]
                if disconnected:
                    self.streams.request_reconnect(disconnected,
                        deadline_seconds=max(.001, self._deadline - self.monotonic()))
                self._update(identifier, stage='streams', result=result,
                    reason='检查行情通道与实际数据新鲜度')
                while True:
                    self._require_live()
                    stream = self.streams.snapshot()
                    selected = [s for s, enabled in (context.get('symbols') or {}).items() if enabled]
                    selected = selected or ['BTCUSDT', 'ETHUSDT']
                    for channel, kinds in (('public', ('book',)), ('market', ('trade', 'mark'))):
                        fresh = all(all(stream.get('symbols', {}).get(s, {}).get(k + '_live', False)
                                        for k in kinds) for s in selected)
                        okay = bool(stream.get('connections', {}).get(channel)) and fresh
                        result[channel] = {'ok': okay, 'state': 'complete' if okay else 'waiting',
                            'reason': None if okay else '通道或新鲜行情尚未恢复', 'checked_at_ms': self._now()}
                    result['streams'] = {c: result[c] for c in ('public', 'market')}
                    if all(result[c]['ok'] for c in ('public', 'market')):
                        break
                    self._update(identifier, result=result)
                    self._stop.wait(.1)
            else:
                for channel in ('public', 'market'):
                    result[channel] = {'ok': False, 'state': 'unavailable', 'reason': '行情服务尚未初始化'}
                raise ValueError('streams_not_initialized')
            self._require_live()
            current_network = getattr(client, 'network_status', lambda: {})()
            if (current_network.get('generation') != result['transport'].get('generation') or
                    current_network.get('recovering') or current_network.get('waiting')):
                from .binance import ManualConnectionCheckStale
                raise ManualConnectionCheckStale()
            with self.lock:
                self._require_live()
                if self._update(identifier, state='complete', stage='complete', result=result,
                        completed_at_ms=self._now(), reason='连接检查完成；交易状态由正常执行流程继续核对'):
                    self._notification = {'id': identifier, 'context': context,
                                          'transport': result.get('transport'), 'completed_at_ms': self._now()}
        except Exception as exc:
            kind = getattr(exc, 'recovery_kind', None)
            retry = getattr(exc, 'retry_at_ms', 0)
            if not retry and getattr(exc, 'retry_at', None):
                retry = round(exc.retry_at * 1000)
            waiting = kind in ('local_budget_wait', 'exchange_cooldown', 'network_read_wait', 'manual_check_busy')
            reasons = {'local_budget_wait': '本地请求额度等待；不会绕过额度限制',
                'exchange_cooldown': '交易所冷却等待；人工检查不会撤销限制',
                'network_read_wait': '已有恢复检查进行中，等待原检查',
                'manual_check_busy': '已有连接检查进行中',
                'manual_check_superseded': '期间发生了更新故障，旧检查结果已作废',
                'manual_check_timeout': '检查超过期限，旧结果不会用于恢复'}
            reason = reasons.get(kind, '连接检查未完成；保留原自动重试期限与交易状态')
            if isinstance(exc, TimeoutError):
                reason = '检查超过期限，旧结果不会用于恢复'
            try:
                self._update(identifier, state='waiting' if waiting else 'failed', stage='waiting' if waiting else 'failed',
                    result=result, completed_at_ms=self._now(), reason=reason, retry_at_ms=int(retry or 0))
            except Exception:
                pass
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            if self._timer:
                self._timer.cancel()
            try:
                self._update(identifier, final=True, active=False)
            except Exception:
                with self.lock:
                    if self._task and self._task.get('id') == identifier:
                        self._task.update(active=False, persistence_error=True)

    def take_notification(self):
        with self.lock:
            value, self._notification = self._notification, None
            return value

    def close(self):
        self._stop.set()
        with self.lock:
            self._notification = None
            if self._task and self._task.get('active'):
                try:
                    self._update(self._task['id'], state='interrupted', stage='interrupted',
                        reason='服务维护结束本次检查，未恢复任何交易意图', completed_at_ms=self._now())
                except Exception:
                    pass
            if self._timer:
                self._timer.cancel()
