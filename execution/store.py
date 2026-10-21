"""Append-only audit and durable intents in the existing live ledger.

hb_ tables coexist with Freqtrade's historical tables. No balance/risk reset,
old-table delete, or trading activation occurs when this module is imported.
"""
import json
import sqlite3
from decimal import Decimal
from contextlib import contextmanager


def dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str, allow_nan=False)


def funding_time_equivalent(previous, row, ledgers, fills):
    """Prove a legacy whole-second timestamp refinement changes no attribution."""
    try:
        for field in ('incomeType', 'symbol', 'asset', 'income'):
            a, b = previous[field], row[field]
            if field == 'income':
                a, b = Decimal(str(a)), Decimal(str(b))
                if not a.is_finite() or not b.is_finite():
                    return False
            if a != b:
                return False
        if previous['incomeType'] != 'FUNDING_FEE':
            return False
        old, new = previous['time'], row['time']
        if any(type(v) not in {int, str} or not str(v).isascii() or not str(v).isdigit() for v in (old, new)):
            return False
        old, new = int(old), int(new)
        if old % 1000 or not 0 < new - old < 1000:
            return False
        ledger = ledgers.get(row['symbol'])
        if (not isinstance(ledger, dict) or ledger.get('strategy') != 'HourlyTrendRange' or
                type(ledger.get('version')) is not int or ledger['version'] != 1 or
                not isinstance(ledger.get('epoch'), str) or not ledger['epoch'] or
                type(ledger.get('initialized_at_ms')) is not int or ledger['initialized_at_ms'] < 0 or
                type(ledger.get('activation_boundary_ms')) is not int or ledger['activation_boundary_ms'] < 0 or
                Decimal(str(ledger.get('initial_wallet'))) != 100):
            return False
        if old <= ledger['initialized_at_ms'] <= new:
            return False
        for fill in fills:
            if not isinstance(fill, dict) or not isinstance(fill.get('symbol'), str):
                return False
            if fill['symbol'] == row['symbol']:
                stamp = fill.get('time')
                if type(stamp) not in {int, str} or not str(stamp).isascii() or not str(stamp).isdigit():
                    return False
                if old <= int(stamp) <= new:
                    return False
        return True
    except (KeyError, ValueError, TypeError, ArithmeticError, AttributeError):
        return False


from .equity_adjustments import EquityAdjustmentStoreMixin


class Store(EquityAdjustmentStoreMixin):
    def __init__(self, path):
        self.path = str(path)
        with self.connection() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS hb_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS hb_candles(symbol TEXT,open_ms INTEGER,raw TEXT NOT NULL,
                    PRIMARY KEY(symbol,open_ms));
                CREATE TABLE IF NOT EXISTS hb_signals(symbol TEXT,boundary_ms INTEGER,
                    value TEXT NOT NULL,PRIMARY KEY(symbol,boundary_ms));
                CREATE TABLE IF NOT EXISTS hb_entry_candidates(symbol TEXT,boundary_ms INTEGER,
                    value TEXT NOT NULL,PRIMARY KEY(symbol,boundary_ms));
                CREATE TABLE IF NOT EXISTS hb_equity_adjustments(id TEXT PRIMARY KEY,symbol TEXT NOT NULL,
                    revision INTEGER NOT NULL,value TEXT NOT NULL,UNIQUE(symbol,revision));
                CREATE TABLE IF NOT EXISTS hb_trades(id TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS hb_orders(id TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS hb_fills(symbol TEXT,id INTEGER,value TEXT NOT NULL,
                    PRIMARY KEY(symbol,id));
                CREATE TABLE IF NOT EXISTS hb_income(id TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS hb_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    time_ms INTEGER NOT NULL,topic TEXT NOT NULL,value TEXT NOT NULL);
            ''')

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def meta(self, key, default=None):
        with self.connection() as db:
            row = db.execute('SELECT value FROM hb_meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.set_meta_many({key: value})

    def set_meta_many(self, values):
        """Commit related recovery markers together; no partial cursor advance."""
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            self._invalidate_candidate_controls(db, values)
            db.executemany('INSERT INTO hb_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                           [(key, dumps(value)) for key, value in values.items()])

    @staticmethod
    def _meta_in(db, key, default=None):
        row = db.execute('SELECT value FROM hb_meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def _write_meta_in(db, key, value):
        db.execute('INSERT INTO hb_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                   (key, dumps(value)))

    def _candidate_control_in(self, db, symbol):
        if symbol not in {'BTCUSDT', 'ETHUSDT'}:
            raise ValueError('入场候选品种无效')
        epochs = self._meta_in(db, 'entry_candidate_control_v1', {})
        enabled = self._meta_in(db, 'symbol_enabled', {})
        after = self._meta_in(db, 'symbol_enabled_after', {})
        if not isinstance(epochs, dict) or not isinstance(enabled, dict) or not isinstance(after, dict):
            raise ValueError('入场候选控制元数据无效')
        global_epoch = epochs.get('global_epoch', 0)
        symbol_epochs = epochs.get('symbol_epochs', {})
        if (type(global_epoch) is not int or global_epoch < 0 or not isinstance(symbol_epochs, dict) or
                type(symbol_epochs.get(symbol, 0)) is not int or symbol_epochs.get(symbol, 0) < 0):
            raise ValueError('入场候选控制代际无效')
        activation = after.get(symbol, 0)
        if type(activation) is not int or activation < 0:
            raise ValueError('品种启用边界无效')
        return {'intent': self._meta_in(db, 'trading_intent_v1'), 'global_epoch': global_epoch,
                'symbol_epoch': symbol_epochs.get(symbol, 0), 'enabled': enabled.get(symbol) is True,
                'activation_boundary_ms': activation}

    def candidate_control(self, symbol):
        """One database snapshot fences human actions, including same-millisecond toggles."""
        with self.connection() as db:
            db.execute('BEGIN')
            return self._candidate_control_in(db, symbol)

    @staticmethod
    def _candidate_control_allows(control, boundary):
        intent = control.get('intent')
        allowed_intent = intent is None or (isinstance(intent, dict) and intent.get('version') == 1 and
            intent.get('desired_running') is True and type(intent.get('updated_at_ms')) is int and
            intent.get('source') in {'human_start', 'human_pause', 'human_exit_all'})
        return (allowed_intent and control.get('enabled') is True and
                boundary > control['activation_boundary_ms'])

    def _invalidate_candidate_controls(self, db, values):
        """Persist choice changes and retire uncommitted candidates in this transaction."""
        all_symbols = {'BTCUSDT', 'ETHUSDT'}
        changed = set()
        global_change = 'trading_intent_v1' in values
        if global_change:
            changed.update(all_symbols)
        for key in ('symbol_enabled', 'symbol_enabled_after'):
            if key not in values:
                continue
            old, new = self._meta_in(db, key, {}), values[key]
            if not isinstance(old, dict) or not isinstance(new, dict):
                changed.update(all_symbols)
            else:
                changed.update(s for s in all_symbols if old.get(s) != new.get(s))
        if not changed:
            return
        epochs = self._meta_in(db, 'entry_candidate_control_v1', {})
        if not isinstance(epochs, dict):
            raise ValueError('入场候选控制代际无效')
        epochs = dict(epochs)
        global_epoch = epochs.get('global_epoch', 0)
        symbol_epochs = dict(epochs.get('symbol_epochs', {}))
        if type(global_epoch) is not int or global_epoch < 0 or any(
                type(v) is not int or v < 0 for v in symbol_epochs.values()):
            raise ValueError('入场候选控制代际无效')
        if global_change:
            global_epoch += 1
        for symbol in changed:
            if any(key in values and (not isinstance(values[key], dict) or
                    not isinstance(self._meta_in(db, key, {}), dict) or
                    self._meta_in(db, key, {}).get(symbol) != values[key].get(symbol))
                    for key in ('symbol_enabled', 'symbol_enabled_after')):
                symbol_epochs[symbol] = symbol_epochs.get(symbol, 0) + 1
        epochs.update(global_epoch=global_epoch, symbol_epochs=symbol_epochs)
        self._write_meta_in(db, 'entry_candidate_control_v1', epochs)
        for symbol, boundary, encoded in db.execute('SELECT symbol,boundary_ms,value FROM hb_entry_candidates'):
            value = json.loads(encoded)
            if symbol in changed and value.get('status') == 'precheck':
                value.update(status='superseded', reason='人工启停或品种选择已变化',
                             revision=value['revision'] + 1)
                db.execute('UPDATE hb_entry_candidates SET value=? WHERE symbol=? AND boundary_ms=?',
                           (dumps(value), symbol, boundary))

    @staticmethod
    def _candidate_in(db, symbol, boundary):
        row = db.execute('SELECT value FROM hb_entry_candidates WHERE symbol=? AND boundary_ms=?',
                         (symbol, boundary)).fetchone()
        return json.loads(row[0]) if row else None

    def entry_candidate(self, symbol, boundary):
        with self.connection() as db:
            return self._candidate_in(db, symbol, boundary)

    def entry_candidates(self, active_only=False):
        with self.connection() as db:
            values = [json.loads(r[0]) for r in db.execute(
                'SELECT value FROM hb_entry_candidates ORDER BY boundary_ms,symbol')]
        return [v for v in values if v.get('status') == 'precheck'] if active_only else values

    def register_entry_candidate(self, value):
        """Reserve a new hour once; consumed legacy signals never become retry candidates."""
        value = json.loads(dumps(value))
        symbol, boundary = value['symbol'], value['boundary_ms']
        if (type(boundary) is not int or boundary < 0 or boundary % 3600000 or
                not isinstance(value.get('signal'), dict)):
            raise ValueError('入场候选信号无效')
        if (type(value.get('created_ms')) is not int or type(value.get('deadline_ms')) is not int or
                value['created_ms'] < boundary or value['created_ms'] >= value['deadline_ms'] or
                value['deadline_ms'] != boundary + 300000 or value.get('status', 'precheck') != 'precheck'):
            raise ValueError('入场候选五分钟期限无效')
        if value['signal'].get('boundary_ms') != boundary:
            raise ValueError('入场候选小时身份不一致')
        if value['signal'].get('direction') not in {-1, 1} or value['signal'].get('branch') not in {'trend', 'range'}:
            raise ValueError('入场候选必须是有效入场信号')
        value.update(version=1, revision=1, status='precheck')
        value.setdefault('updated_ms', value['created_ms'])
        value.setdefault('frozen', None)
        if value['frozen'] is not None:
            self._validate_candidate_frozen(value, value['frozen'])
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            existing = self._candidate_in(db, symbol, boundary)
            if existing is not None:
                return existing
            if db.execute('SELECT 1 FROM hb_signals WHERE symbol=? AND boundary_ms=?',
                          (symbol, boundary)).fetchone():
                return None
            current = self._candidate_control_in(db, symbol)
            if value.get('control') != current or not self._candidate_control_allows(current, boundary):
                return None
            db.execute('INSERT INTO hb_signals VALUES(?,?,?)', (symbol, boundary,
                dumps({'candidate_version': 1, 'candidate_reserved': True,
                       'direction': value['signal'].get('direction'), 'time_ms': value['created_ms']})))
            db.execute('INSERT INTO hb_entry_candidates VALUES(?,?,?)', (symbol, boundary, dumps(value)))
            return value

    @staticmethod
    def _validate_candidate_frozen(candidate, frozen):
        required = {'equity0', 'budget0', 'notional0', 'entry_atr', 'ledger_epoch'}
        if not isinstance(frozen, dict) or set(frozen) != required:
            raise ValueError('入场候选冻结额度不完整')
        try:
            e, b, n, atr = (Decimal(str(frozen[k])) for k in ('equity0', 'budget0', 'notional0', 'entry_atr'))
            if (not all(v.is_finite() and v > 0 for v in (e, b, n, atr)) or b != e * Decimal('.50') or
                    n != e * 15 or atr != Decimal(str(candidate['signal']['atr'])) or
                    not isinstance(frozen['ledger_epoch'], str) or not frozen['ledger_epoch']):
                raise ValueError('入场候选冻结权益、风险额度或ATR无效')
        except (KeyError, ArithmeticError, TypeError) as exc:
            raise ValueError('入场候选冻结额度无效') from exc

    def update_entry_candidate(self, symbol, boundary, updates, expected_revision):
        """CAS only precheck state; the first risk snapshot can never be enlarged or replaced."""
        allowed = {'status', 'updated_ms', 'reason', 'wait_reason', 'retry_at_ms', 'attempts', 'frozen',
                   'stage', 'last_error', 'last_checked_ms'}
        if (not isinstance(updates, dict) or set(updates) - allowed or
                type(expected_revision) is not int or expected_revision < 1):
            raise ValueError('不可修改候选信号、身份或人工控制快照')
        updates = json.loads(dumps(updates))
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = self._candidate_in(db, symbol, boundary)
            if old is None or old.get('status') != 'precheck' or old['revision'] != expected_revision:
                return None
            if updates.get('status', 'precheck') not in {'precheck', 'expired', 'rejected', 'superseded'}:
                raise ValueError('入场候选状态转移无效')
            if 'frozen' in updates:
                if old.get('frozen') is not None and updates['frozen'] != old['frozen']:
                    raise ValueError('入场候选冻结额度不得重新计算')
                self._validate_candidate_frozen(old, updates['frozen'])
            value = dict(old, **updates, revision=old['revision'] + 1)
            db.execute('UPDATE hb_entry_candidates SET value=? WHERE symbol=? AND boundary_ms=?',
                       (dumps(value), symbol, boundary))
            return value

    def commit_entry_candidate(self, symbol, boundary, trade, order, expected_revision):
        """Persist one original order identity and trade before any wire operation, all or nothing."""
        if type(expected_revision) is not int or expected_revision < 1:
            raise ValueError('入场候选版本无效')
        trade, order = json.loads(dumps(trade)), json.loads(dumps(order))
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = self._candidate_in(db, symbol, boundary)
            if old is None or old.get('status') != 'precheck' or old['revision'] != expected_revision:
                return None
            current = self._candidate_control_in(db, symbol)
            if old['control'] != current or not self._candidate_control_allows(current, boundary):
                return None
            frozen = old.get('frozen')
            self._validate_candidate_frozen(old, frozen)
            for key in ('equity0', 'budget0', 'notional0', 'entry_atr', 'ledger_epoch'):
                a, b = frozen.get(key), trade.get(key)
                if key != 'ledger_epoch':
                    try:
                        a, b = Decimal(str(a)), Decimal(str(b))
                    except ArithmeticError as exc:
                        raise ValueError('交易额度与冻结候选不一致') from exc
                if a != b:
                    raise ValueError('交易额度与冻结候选不一致')
            if (trade.get('symbol') != symbol or trade.get('signal_boundary_ms') != boundary or
                    trade.get('direction') != old['signal']['direction'] or
                    trade.get('branch') != old['signal']['branch'] or
                    trade.get('signal_snapshot') != old['signal'] or not trade.get('id') or
                    order.get('trade_id') != trade['id'] or order.get('symbol') != symbol or not order.get('id') or
                    order.get('client_id') != order.get('id') or
                    order.get('kind') != 'market' or order.get('purpose') != 'entry' or order.get('state') != 'prepared'):
                raise ValueError('入场候选交易与原订单身份不一致')
            ledger = self._meta_in(db, 'hourly_trend_range_ledgers_v1', {})
            if (not isinstance(ledger, dict) or (ledger or current['intent'] is not None) and
                    (not isinstance(ledger.get(symbol), dict) or ledger[symbol].get('epoch') != frozen['ledger_epoch'])):
                raise ValueError('独立账本代际已变化')
            if (db.execute('SELECT 1 FROM hb_trades WHERE id=?', (trade['id'],)).fetchone() or
                    db.execute('SELECT 1 FROM hb_orders WHERE id=?', (order['id'],)).fetchone()):
                return None
            for encoded, in db.execute('SELECT value FROM hb_trades'):
                active = json.loads(encoded)
                if active.get('symbol') == symbol and active.get('status') != 'closed':
                    return None
            db.execute('INSERT INTO hb_trades VALUES(?,?)', (trade['id'], dumps(trade)))
            db.execute('INSERT INTO hb_orders VALUES(?,?)', (order['id'], dumps(order)))
            value = dict(old, status='write_intent', revision=old['revision'] + 1,
                         trade_id=trade['id'], order_id=order['id'])
            db.execute('UPDATE hb_entry_candidates SET value=? WHERE symbol=? AND boundary_ms=?',
                       (dumps(value), symbol, boundary))
            return value

    def event(self, time_ms, topic, value):
        with self.connection() as db:
            db.execute('INSERT INTO hb_events(time_ms,topic,value) VALUES(?,?,?)',
                       (time_ms, topic, dumps(value)))

    def events(self, limit=30):
        with self.connection() as db:
            rows = db.execute('SELECT time_ms,topic,value FROM hb_events ORDER BY seq DESC LIMIT ?',
                              (limit,)).fetchall()
        return [{'time_ms': r[0], 'message': r[1], 'details': json.loads(r[2])} for r in rows]

    def candles(self, symbol):
        with self.connection() as db:
            rows = db.execute('SELECT raw FROM hb_candles WHERE symbol=? ORDER BY open_ms',
                              (symbol,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def save_candles(self, symbol, rows):
        with self.connection() as db:
            for row in rows:
                old = db.execute('SELECT raw FROM hb_candles WHERE symbol=? AND open_ms=?',
                                 (symbol, int(row[0]))).fetchone()
                encoded = dumps(row)
                if old and old[0] != encoded:
                    raise ValueError('已收盘历史K线发生变化，不能移动指标初始化锚点')
                db.execute('INSERT OR IGNORE INTO hb_candles VALUES(?,?,?)',
                           (symbol, int(row[0]), encoded))

    def signal(self, symbol, boundary):
        with self.connection() as db:
            row = db.execute('SELECT value FROM hb_signals WHERE symbol=? AND boundary_ms=?',
                             (symbol, boundary)).fetchone()
        return json.loads(row[0]) if row else None

    def claim_signal(self, symbol, boundary, value):
        """Consumed before any wire operation, including skipped/ineligible signals."""
        with self.connection() as db:
            cursor = db.execute('INSERT OR IGNORE INTO hb_signals VALUES(?,?,?)',
                                (symbol, boundary, dumps(value)))
            return cursor.rowcount == 1

    def _get(self, table, key):
        with self.connection() as db:
            row = db.execute(f'SELECT value FROM {table} WHERE id=?', (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def _put(self, table, key, value):
        with self.connection() as db:
            db.execute(f'INSERT INTO {table} VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value',
                       (key, dumps(value)))

    def _all(self, table):
        with self.connection() as db:
            rows = db.execute(f'SELECT value FROM {table} ORDER BY rowid').fetchall()
        return [json.loads(r[0]) for r in rows]

    def trade(self, key):
        return self._get('hb_trades', key)

    def save_trade(self, value):
        self._put('hb_trades', value['id'], value)

    def trades(self, active_only=False):
        values = self._all('hb_trades')
        return [v for v in values if v['status'] != 'closed'] if active_only else values

    def order(self, key):
        return self._get('hb_orders', key)

    def save_order(self, value):
        self._put('hb_orders', value['id'], value)

    def orders(self):
        return self._all('hb_orders')

    def fills(self, symbol=None):
        with self.connection() as db:
            rows = db.execute('SELECT value FROM hb_fills' + (' WHERE symbol=?' if symbol else '') +
                              ' ORDER BY id', (symbol,) if symbol else ()).fetchall()
        return [json.loads(r[0]) for r in rows]

    def claim_fill(self, symbol, row):
        with self.connection() as db:
            cursor = db.execute('INSERT OR IGNORE INTO hb_fills VALUES(?,?,?)',
                                (symbol, int(row['id']), dumps(row)))
            return cursor.rowcount == 1

    def claim_income(self, row):
        key = str(row['tranId']) + ':' + row['symbol'] + ':' + row['asset']
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT value FROM hb_income WHERE id=?', (key,)).fetchone()
            if old:
                previous = json.loads(old[0])
                for field in ('incomeType', 'symbol', 'asset', 'time', 'income'):
                    a, b = previous.get(field), row.get(field)
                    if field == 'income' and a is not None and b is not None:
                        a, b = Decimal(str(a)), Decimal(str(b))
                    if field == 'time' and a is not None and b is not None:
                        a, b = int(a), int(b)
                    if a == b:
                        continue
                    if field == 'time':
                        meta = db.execute('SELECT value FROM hb_meta WHERE key=?',
                            ('hourly_trend_range_ledgers_v1',)).fetchone()
                        ledgers = json.loads(meta[0]) if meta else {}
                        fills = [json.loads(r[0]) for r in db.execute('SELECT value FROM hb_fills')]
                        if funding_time_equivalent(previous, row, ledgers, fills):
                            observation = {'stored_time_ms': int(a), 'observed_time_ms': int(b)}
                            saved = db.execute('SELECT value FROM hb_meta WHERE key=?',
                                ('funding_timestamp_observations_v1',)).fetchone()
                            seen = json.loads(saved[0]) if saved else {}
                            if not isinstance(seen, dict):
                                raise ValueError('资金费时间精度审计元数据无效')
                            if key in seen:
                                history = seen[key]
                                if (not isinstance(history, dict) or history.get('stored_time_ms') != int(a) or
                                        type(history.get('observed_time_ms')) is not int):
                                    raise ValueError('资金费时间精度历史证明无效')
                                observation['observed_time_ms'] = max(int(b), history['observed_time_ms'])
                            if seen.get(key) != observation:
                                seen[key] = observation
                                db.execute('INSERT INTO hb_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                    ('funding_timestamp_observations_v1', dumps(seen)))
                                db.execute('INSERT INTO hb_events(time_ms,topic,value) VALUES(?,?,?)',
                                    (int(b), '资金费重复结算时间精度已核对，保留原记录且不重复入账',
                                     dumps(dict(observation, identity=key, symbol=row['symbol']))))
                            continue
                    raise ValueError('同一资金费结算ID的核心金额或归属变化，保留原记录待核对')
                return False
            cursor = db.execute('INSERT OR IGNORE INTO hb_income VALUES(?,?)', (key, dumps(row)))
            return cursor.rowcount == 1

    def incomes(self):
        """Original settlement records, including their exchange transaction IDs."""
        return self._all('hb_income')
