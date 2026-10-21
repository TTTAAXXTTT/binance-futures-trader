"""Audited target-equity adjustments; never a deposit, trade or ledger reset."""
from decimal import Decimal
import re

LEDGER_KEY = 'hourly_trend_range_ledgers_v1'
SYMBOLS = {'BTCUSDT', 'ETHUSDT'}


def validate_request(symbol, target_equity, expected_revision, operation_id):
    if not isinstance(symbol, str) or symbol not in SYMBOLS:
        raise ValueError('只能编辑BTCUSDT或ETHUSDT的策略权益')
    if (not isinstance(target_equity, str) or not re.fullmatch(r'[0-9]{1,12}(?:\.[0-9]{1,8})?', target_equity)
            or Decimal(target_equity) <= 0):
        raise ValueError('策略权益必须是正数，最多12位整数和8位小数，不接受科学计数法')
    if type(expected_revision) is not int or expected_revision < 0:
        raise ValueError('策略权益版本无效，请重新读取页面')
    if not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', operation_id):
        raise ValueError('缺少有效的权益保存编号')
    return format(Decimal(target_equity), 'f')


def adjustment_summary(rows, symbol, epoch):
    """Fail closed on missing, reordered or cross-epoch allocation history."""
    total, revision, latest = Decimal(0), 0, None
    for row in rows:
        if row.get('symbol') != symbol:
            continue
        if (row.get('version') != 1 or row.get('ledger_epoch') != epoch or
                type(row.get('revision')) is not int or row['revision'] != revision + 1 or
                type(row.get('time_ms')) is not int or row['time_ms'] < 0 or
                row.get('source') != 'human_equity_edit' or
                type(row.get('activation_boundary_ms')) is not int or row['activation_boundary_ms'] < 0 or
                row['activation_boundary_ms'] % 3600000 or
                not isinstance(row.get('operation_id'), str)):
            raise ValueError('策略权益调整流水的版本、顺序或账本归属无法核对')
        numbers = {k: Decimal(str(row[k])) for k in
            ('delta', 'target_equity', 'observed_equity', 'observed_wallet', 'observed_unrealized')}
        if (not all(v.is_finite() for v in numbers.values()) or numbers['target_equity'] <= 0 or
                numbers['observed_wallet'] + numbers['observed_unrealized'] != numbers['observed_equity'] or
                numbers['target_equity'] - numbers['observed_equity'] != numbers['delta']):
            raise ValueError('策略权益调整流水金额无法核对')
        total += numbers['delta']
        revision, latest = row['revision'], row
    return {'allocation_adjustment': str(total), 'allocation_revision': revision,
            'last_equity_adjustment': latest}


class EquityAdjustmentStoreMixin:
    def equity_adjustments(self, symbol=None):
        import json
        with self.connection() as db:
            if symbol is None:
                rows = db.execute('SELECT value FROM hb_equity_adjustments ORDER BY symbol,revision')
            else:
                rows = db.execute('SELECT value FROM hb_equity_adjustments WHERE symbol=? ORDER BY revision', (symbol,))
            return [json.loads(r[0]) for r in rows]

    def equity_adjustment(self, operation_id):
        import json
        with self.connection() as db:
            row = db.execute('SELECT value FROM hb_equity_adjustments WHERE id=?', (operation_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def commit_equity_adjustment(self, row, *, expected_ledger, new_ledger=None):
        """Journal, optional first initialization and candidate fence commit together."""
        import json
        from .store import dumps
        target = validate_request(row['symbol'], row['target_equity'], row['revision'] - 1, row['operation_id'])
        symbol = row['symbol']
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            previous = db.execute('SELECT value FROM hb_equity_adjustments WHERE id=?', (row['operation_id'],)).fetchone()
            if previous:
                previous = json.loads(previous[0])
                if previous['symbol'] != symbol or Decimal(previous['target_equity']) != Decimal(target) or previous['revision'] != row['revision']:
                    raise RuntimeError('同一保存编号不能用于不同的权益修改')
                return previous
            records = self._meta_in(db, LEDGER_KEY, {})
            if not isinstance(records, dict) or records.get(symbol) != expected_ledger:
                raise RuntimeError('账本已发生变化，请重新读取后保存')
            record = expected_ledger if expected_ledger is not None else new_ledger
            if not isinstance(record, dict) or record.get('epoch') != row['ledger_epoch']:
                raise ValueError('权益调整的账本归属无效')
            if Decimal(str(record.get('wallet'))) != Decimal(row['observed_wallet']):
                raise RuntimeError('权益观察值与已核对账本不同，请重新读取后保存')
            rows = [json.loads(r[0]) for r in db.execute(
                'SELECT value FROM hb_equity_adjustments WHERE symbol=? ORDER BY revision', (symbol,))]
            summary = adjustment_summary(rows, symbol, record['epoch'])
            if row['revision'] != summary['allocation_revision'] + 1:
                raise RuntimeError('该品种权益已被其他页面修改，请重新读取后保存')
            adjustment_summary(rows + [row], symbol, record['epoch'])
            if expected_ledger is None:
                records[symbol] = record
                self._write_meta_in(db, LEDGER_KEY, records)
            epochs = self._meta_in(db, 'entry_candidate_control_v1', {})
            # Validate before incrementing; the existing fence protects the final entry commit.
            self._candidate_control_in(db, symbol)
            epochs = dict(epochs)
            per_symbol = dict(epochs.get('symbol_epochs', {}))
            per_symbol[symbol] = per_symbol.get(symbol, 0) + 1
            epochs.update(symbol_epochs=per_symbol)
            self._write_meta_in(db, 'entry_candidate_control_v1', epochs)
            for boundary, encoded in db.execute('SELECT boundary_ms,value FROM hb_entry_candidates WHERE symbol=?', (symbol,)):
                candidate = json.loads(encoded)
                if candidate.get('status') == 'precheck':
                    candidate.update(status='superseded', reason='用户调整本品种策略权益，等待新的小时信号',
                                     revision=candidate['revision'] + 1)
                    db.execute('UPDATE hb_entry_candidates SET value=? WHERE symbol=? AND boundary_ms=?',
                               (dumps(candidate), symbol, boundary))
            # Also prevent a not-yet-registered signal of the current hour using the new allocation.
            after = dict(self._meta_in(db, 'symbol_enabled_after', {}))
            after[symbol] = max(after.get(symbol, 0), row['activation_boundary_ms'])
            self._write_meta_in(db, 'symbol_enabled_after', after)
            db.execute('INSERT INTO hb_equity_adjustments VALUES(?,?,?,?)',
                       (row['operation_id'], symbol, row['revision'], dumps(row)))
            db.execute('INSERT INTO hb_events(time_ms,topic,value) VALUES(?,?,?)',
                       (row['time_ms'], '用户调整独立策略权益，保留历史和已有持仓预算', dumps(row)))
            return row
