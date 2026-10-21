"""Bounded late-settlement replay; immutable IDs prevent double accounting."""
from decimal import Decimal, InvalidOperation
from .binance import ApiError

DAY_MS = 86400000
OVERLAP_MS = DAY_MS
RECHECK_LOOKBACK_MS = 7 * DAY_MS
RECHECK_INTERVAL_MS = 6 * 3600000
MAX_PAGES = 5
PAGE_SIZE = 1000
PENDING_META = 'funding_replay_v1'
RECHECK_META = 'funding_recheck_completed_ms'


def _error(message):
    return ApiError(None, message, method='GET', path='/fapi/v1/income',
                    recovery_kind='execution_review')


def _window(value, now):
    if (not isinstance(value, dict) or type(value.get('start')) is not int or
            type(value.get('end')) is not int or value['start'] < 0 or
            value['end'] < value['start'] or value['end'] - value['start'] >= RECHECK_LOOKBACK_MS):
        raise _error('资金费待恢复读取范围无效，不能猜测成功游标')
    end = min(value['end'], now)
    return {'start': min(value['start'], end), 'end': end}


def _read_window(client, store, window):
    new_records = 0
    previous_full_ids = None
    for page in range(1, MAX_PAGES + 1):
        rows = client.income(income_type='FUNDING_FEE', startTime=window['start'],
                             endTime=window['end'], page=page, limit=PAGE_SIZE)
        if not isinstance(rows, list) or len(rows) > PAGE_SIZE:
            raise _error('资金费分页响应无效，读取完整性未确认')
        ids = []
        for row in rows:
            try:
                if not isinstance(row, dict) or row.get('incomeType') != 'FUNDING_FEE':
                    raise ValueError
                if isinstance(row.get('tranId'), bool) or not str(row['tranId']).isdigit():
                    raise ValueError
                if type(row['time']) not in {int, str} or not str(row['time']).isdigit():
                    raise ValueError
                millis = int(row['time'])
                amount = Decimal(str(row['income']))
                if (isinstance(row['time'], bool) or not amount.is_finite() or
                        not window['start'] <= millis <= window['end'] or
                        not isinstance(row.get('symbol'), str) or not row['symbol'] or
                        not isinstance(row.get('asset'), str) or not row['asset']):
                    raise ValueError
            except (KeyError, ValueError, TypeError, InvalidOperation):
                raise _error('资金费结算身份、金额或时间范围无效，账务待核对') from None
            ids.append((str(row['tranId']), row['symbol'], row['asset']))
            if store.claim_income(row):
                new_records += 1
                store.event(millis, '资金费实际结算记录（已计入交易所权益）', row)
        if len(rows) < PAGE_SIZE:
            return new_records
        if previous_full_ids == ids:
            raise _error('资金费分页未前进，读取完整性未确认')
        previous_full_ids = ids
    raise _error('资金费读取达到分页上限，不能将截断结果认定完整')


def read_funding(client, store, now, started_ms):
    """At most two <=7-day windows; retry a frozen range from page one."""
    cursor = min(int(store.meta('funding_cursor', started_ms)), now)
    pending = store.meta(PENDING_META)
    if pending is not None:
        if not isinstance(pending, dict) or pending.get('version') != 1:
            raise _error('资金费恢复元数据无效，未推进读取游标')
        ordinary = _window(pending.get('ordinary'), now)
        audit = _window(pending['audit'], now) if pending.get('audit') is not None else None
    else:
        start = max(0, cursor - OVERLAP_MS)
        ordinary = {'start': start, 'end': min(now, start + RECHECK_LOOKBACK_MS - 1)}
        last = store.meta(RECHECK_META)
        due = type(last) is not int or last > now or now - last >= RECHECK_INTERVAL_MS
        audit = {'start': max(0, now - RECHECK_LOOKBACK_MS + 1), 'end': now} if due else None
    pending = {'version': 1, 'ordinary': ordinary, 'audit': audit}
    store.set_meta(PENDING_META, pending)
    added = _read_window(client, store, ordinary)
    if audit is not None and audit != ordinary:
        added += _read_window(client, store, audit)
    completed = client.now_ms()
    updates = {'funding_cursor': min(ordinary['end'], completed), PENDING_META: None}
    if audit is not None:
        updates[RECHECK_META] = min(audit['end'], completed)
    store.set_meta_many(updates)
    return completed, {'version': 'late-settlement-replay-v1',
        'overlap_seconds': OVERLAP_MS // 1000,
        'recheck_lookback_seconds': RECHECK_LOOKBACK_MS // 1000,
        'recheck_interval_seconds': RECHECK_INTERVAL_MS // 1000,
        'max_pages_per_window': MAX_PAGES, 'records_added': added,
        'ordinary': ordinary, 'audit': audit, 'completed_at_ms': completed}
