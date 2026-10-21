"""Repair a proven external full exit in the original book, without trading.

This deliberately handles only the unambiguous case: one open legacy trade,
one entry order, one fully filled external exit, USDT fees, and no intervening
fills/funding. All exchange access belongs to the GET-only diagnostic caller.
The original book is backed up, records retained, and evidence appended.
"""
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


def dec(value):
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError('成交字段必须是有限数值')
    return result


def utc_text(ms):
    return datetime.fromtimestamp(int(ms) / 1000, timezone.utc).replace(tzinfo=None).isoformat(' ', timespec='microseconds')


def read_book(path, trade_id):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.execute('PRAGMA query_only=ON')
        db.row_factory = sqlite3.Row
        trade = db.execute('SELECT * FROM trades WHERE id=?', (trade_id,)).fetchone()
        if not trade:
            raise ValueError('原交易记录不存在')
        orders = [dict(r) for r in db.execute('SELECT * FROM orders WHERE ft_trade_id=? ORDER BY id', (trade_id,))]
        return {'trade': dict(trade), 'orders': orders,
                'open_trades': db.execute('SELECT COUNT(*) FROM trades WHERE is_open=1').fetchone()[0]}


def require_flat(guard):
    if (guard.get('connected') is not True or guard.get('positions') != []
            or guard.get('open_orders') != 0 or guard.get('open_algo_orders') != 0
            or guard.get('workers_stopped') is not True):
        raise ValueError('只有已核对全账户空仓、无订单且交易进程均退出时才能修正历史记录')


def make_plan(book, evidence):
    require_flat(evidence['guard'])
    t, orders = book['trade'], book['orders']
    if not t['is_open'] or book['open_trades'] != 1:
        raise ValueError('只处理原账本唯一未平交易，已关闭记录不可再次修改')
    if t.get('exchange') != 'binance' or t['pair'] not in ('BTC/USDT:USDT', 'ETH/USDT:USDT'):
        raise ValueError('交易品种或交易所不匹配')
    symbol = t['pair'].replace('/', '').split(':')[0]
    direction = -1 if t['is_short'] else 1
    entry_side, exit_side = ('SELL', 'BUY') if direction == -1 else ('BUY', 'SELL')
    entries = [o for o in orders if o['ft_order_side'] == entry_side.lower() and dec(o.get('filled') or 0) > 0]
    if len(entries) != 1 or any(dec(o.get('filled') or 0) > 0 for o in orders if o not in entries):
        raise ValueError('仅支持无加仓、无部分退出的一笔原入场')
    fills = evidence['fills']
    if not fills or len(fills) >= 1000 or len({str(r['id']) for r in fills}) != len(fills):
        raise ValueError('成交记录缺失、分页不完整或成交ID重复')
    if evidence.get('funding') or dec(t.get('funding_fees') or 0) != 0:
        raise ValueError('含资金费用的交易需要独立核对，不使用本次简单修复')
    for r in fills:
        if (r['symbol'] != symbol or r.get('positionSide') != 'BOTH'
                or r.get('commissionAsset') != 'USDT' or dec(r['qty']) <= 0
                or dec(r['price']) <= 0 or dec(r['commission']) < 0):
            raise ValueError('实际成交品种、模式、费用或数量不匹配')
    entry_id = str(entries[0]['order_id'])
    opening = [r for r in fills if str(r['orderId']) == entry_id and r['side'] == entry_side]
    exiting = [r for r in fills if str(r['orderId']) != entry_id and r['side'] == exit_side]
    if len(opening) + len(exiting) != len(fills) or not opening or not exiting:
        raise ValueError('存在其他成交或缺少实际开平仓成交')
    exit_ids = {str(r['orderId']) for r in exiting}
    if len(exit_ids) != 1 or any(str(o['order_id']) in exit_ids for o in orders):
        raise ValueError('退出订单不唯一或已经记入账本')
    if min(int(r['time']) for r in exiting) <= max(int(r['time']) for r in opening):
        raise ValueError('实际开平仓时间顺序不匹配')
    quantity = dec(t['amount'])
    entry_qty = sum((dec(r['qty']) for r in opening), Decimal(0))
    exit_qty = sum((dec(r['qty']) for r in exiting), Decimal(0))
    if entry_qty != quantity or exit_qty != quantity or dec(entries[0]['filled']) != quantity:
        raise ValueError('开平仓成交数量不能完整匹配原仓位')
    entry_value = sum((dec(r['price']) * dec(r['qty']) for r in opening), Decimal(0))
    exit_value = sum((dec(r['price']) * dec(r['qty']) for r in exiting), Decimal(0))
    entry_price, exit_price = entry_value / quantity, exit_value / quantity
    if entry_price != dec(t['open_rate']):
        raise ValueError('实际入场均价与原记录不一致')
    wire = evidence['exit_order']
    exit_id = next(iter(exit_ids))
    if (str(wire['orderId']) != exit_id or wire['symbol'] != symbol or wire['status'] != 'FILLED'
            or wire['side'] != exit_side or wire.get('positionSide') != 'BOTH'
            or dec(wire['executedQty']) != quantity or dec(wire['avgPrice']) != exit_price):
        raise ValueError('交易所退出订单状态或均价不匹配')
    gross = direction * (exit_value - entry_value)
    if (sum((dec(r['realizedPnl']) for r in exiting), Decimal(0)) != gross
            or any(dec(r['realizedPnl']) != 0 for r in opening)):
        raise ValueError('交易所已实现价格损益与完整仓位不匹配')
    fee_open = sum((dec(r['commission']) for r in opening), Decimal(0))
    fee_close = sum((dec(r['commission']) for r in exiting), Decimal(0))
    net = gross - fee_open - fee_close
    open_value = entry_value + direction * fee_open
    lev = dec(t['leverage'])
    if lev <= 0 or open_value <= 0:
        raise ValueError('原入场金额或杠杆无效')
    algo_rows = evidence.get('algos', [])
    if len({str(r['algoId']) for r in algo_rows}) != len(algo_rows):
        raise ValueError('条件单ID重复，不能采用有冲突的状态证据')
    algos = {str(r['algoId']): r for r in algo_rows}
    canceled = []
    for o in orders:
        if not o['ft_is_open']:
            continue
        actual = algos.get(str(o['order_id']))
        if (o['ft_order_side'] != 'stoploss' or not actual or actual['symbol'] != symbol
                or actual['algoStatus'] != 'CANCELED' or actual.get('actualOrderId') not in ('', '0', 0, None)):
            raise ValueError('旧账本仍有未确认终止的条件单，不能修改其状态')
        canceled.append({'id': o['id'], 'updated_at': utc_text(actual['updateTime'])})
    closed_at = utc_text(max(int(r['time']) for r in exiting))
    changes = {'is_open': 0, 'close_rate': float(exit_price), 'close_date': closed_at,
        'exit_reason': 'external_exit_reconciled', 'exit_order_status': 'closed',
        'fee_open': float(fee_open / entry_value), 'fee_open_cost': float(fee_open), 'fee_open_currency': 'USDT',
        'fee_close': float(fee_close / exit_value), 'fee_close_cost': float(fee_close), 'fee_close_currency': 'USDT',
        'open_trade_value': float(open_value), 'realized_profit': float(net),
        'close_profit_abs': float(net), 'close_profit': float(net / open_value * lev)}
    exit_row = {'ft_trade_id': t['id'], 'ft_order_side': exit_side.lower(), 'ft_pair': t['pair'],
        'ft_is_open': 0, 'ft_amount': float(quantity), 'ft_price': float(exit_price), 'order_id': exit_id,
        'status': 'closed', 'symbol': t['pair'], 'order_type': wire['type'].lower(), 'side': exit_side.lower(),
        'price': float(exit_price), 'average': float(exit_price), 'amount': float(dec(wire['origQty'])),
        'filled': float(quantity), 'remaining': 0.0, 'cost': float(exit_value),
        'order_date': utc_text(wire['time']), 'order_filled_date': closed_at,
        'order_update_date': utc_text(wire['updateTime']), 'funding_fee': 0.0,
        'ft_order_tag': 'external_exit_reconciled'}
    return {'trade_id': t['id'], 'exit_order_id': exit_id, 'before': book, 'changes': changes,
        'exit_row': exit_row, 'canceled': canceled, 'evidence': evidence, 'profit_usdt': str(net)}


def apply_plan(path, plan, guard):
    """Apply a previously proven historical plan; guard rechecks live GET state."""
    require_flat(guard())
    path = Path(path)
    if read_book(path, plan['trade_id']) != plan['before']:
        raise ValueError('账本已变化，必须重新查询核对，未执行修改')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    backup = path.parent / 'backups' / (path.stem + '-before-reconcile-' + stamp + '.sqlite')
    backup.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as source:
        with closing(sqlite3.connect(backup)) as target:
            source.backup(target)
    require_flat(guard())
    with closing(sqlite3.connect(path, timeout=5)) as db:
        db.execute('BEGIN IMMEDIATE')
        try:
            db.row_factory = sqlite3.Row
            current = {'trade': dict(db.execute('SELECT * FROM trades WHERE id=?', (plan['trade_id'],)).fetchone()),
                'orders': [dict(r) for r in db.execute('SELECT * FROM orders WHERE ft_trade_id=? ORDER BY id', (plan['trade_id'],))],
                'open_trades': db.execute('SELECT COUNT(*) FROM trades WHERE is_open=1').fetchone()[0]}
            if current != plan['before']:
                raise ValueError('写入前原账本发生变化，未执行修改')
            db.execute('CREATE TABLE IF NOT EXISTS legacy_reconciliation_audit '
                '(id INTEGER PRIMARY KEY, trade_id INTEGER NOT NULL, exit_order_id TEXT NOT NULL UNIQUE, '
                'reconciled_at TEXT NOT NULL, backup_path TEXT NOT NULL, evidence_json TEXT NOT NULL)')
            db.execute('INSERT INTO legacy_reconciliation_audit '
                '(trade_id,exit_order_id,reconciled_at,backup_path,evidence_json) VALUES (?,?,?,?,?)',
                (plan['trade_id'], plan['exit_order_id'], stamp, str(backup), json.dumps(plan, ensure_ascii=False)))
            columns = list(plan['exit_row'])
            db.execute('INSERT INTO orders (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in columns) + ')',
                       [plan['exit_row'][k] for k in columns])
            for canceled in plan['canceled']:
                db.execute("UPDATE orders SET ft_is_open=0,status='canceled',order_update_date=? WHERE id=?",
                           (canceled['updated_at'], canceled['id']))
            columns = list(plan['changes'])
            db.execute('UPDATE trades SET ' + ','.join(k + '=?' for k in columns) + ' WHERE id=? AND is_open=1',
                       [plan['changes'][k] for k in columns] + [plan['trade_id']])
            db.commit()
        except Exception:
            db.rollback()
            raise
    return {'trade_id': plan['trade_id'], 'exit_order_id': plan['exit_order_id'],
            'profit_usdt': plan['profit_usdt'], 'closed_at_utc': plan['changes']['close_date'],
            'backup_path': str(backup), 'exchange_requests': 'GET only; no orders submitted',
            'audit_table': 'legacy_reconciliation_audit'}
