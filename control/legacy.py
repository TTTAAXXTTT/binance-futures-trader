"""Read-only legacy execution review; never modifies its ledger or risk state."""
import hashlib
import json
import math
import re
import sqlite3
from contextlib import closing
from pathlib import Path


def legacy_process_state(root):
    import psutil
    target = Path(root).resolve()
    verified = True
    for process in psutil.process_iter():
        try:
            name = process.name().lower()
            if name not in ['python.exe', 'pythonw.exe', 'python', 'python3']:
                continue
            arguments = process.cmdline()
            if any(str(target).lower() in str(a).lower().replace('/', '\\') for a in arguments):
                return {'active': True, 'verified': True}
            cwd = Path(process.cwd()).resolve()
            if cwd == target or target in cwd.parents:
                return {'active': True, 'verified': True}
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            verified = False
    return {'active': False, 'verified': verified}


def inspect_stopped_legacy(root, current_fingerprint=None, process_probe=None):
    root = Path(root)
    process = (process_probe or legacy_process_state)(root)
    path = root / 'data/live-binance-BTCUSDT.sqlite3'
    result = {'present': True, 'source': 'offline-ledger', 'unknown': True,
              'raw_running': process['active'], 'process_verified': process['verified'],
              'review_available': False, 'accepted_for_new_strategy': False}
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            db.execute('PRAGMA query_only=ON')
            db.execute('BEGIN')
            if db.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('legacy ledger integrity')
            def state(key):
                row = db.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
                return json.loads(row[0]) if row else None
            risk, session = state('risk'), state('trading_session')
            position, budget, cash = state('exchange_position'), state('entry_budget'), state('cash_accounting')
            pending = db.execute("SELECT COUNT(*) FROM intents WHERE status IN ('PENDING','UNKNOWN')").fetchone()[0]
        if not isinstance(risk, dict) or not isinstance(risk.get('locked'), bool):
            raise ValueError('legacy risk missing')
        if not isinstance(session, dict) or not isinstance(budget, dict) or not isinstance(cash, dict):
            raise ValueError('legacy state missing')
        binding = budget.get('binding') or {}
        if not isinstance(binding, dict):
            raise ValueError('legacy account binding malformed')
        fingerprint = binding.get('api_key_sha256')
        if (binding.get('mode') != 'live' or binding.get('symbol') != 'BTCUSDT' or
                not isinstance(fingerprint, str) or not re.fullmatch('[0-9a-f]{64}', fingerprint)):
            raise ValueError('legacy account binding missing')
        flow = float(cash.get('pending_flow', float('nan')))
        if not math.isfinite(flow):
            raise ValueError('legacy accounting state missing')
        unfinished = bool(session.get('entry_id') and session.get('phase') not in ['completed','failed'])
        halted = bool(risk['locked'] or risk.get('strategy_review') or risk.get('accounting_review'))
        digest_input = {'risk': risk, 'session': session, 'position': position, 'pending': pending,
                        'pending_flow': flow, 'binding': binding}
        digest = hashlib.sha256(json.dumps(digest_input, sort_keys=True, ensure_ascii=True,
                                         allow_nan=False).encode()).hexdigest()
        result.update(unknown=False, state=session.get('phase', 'unknown'), position=position is not None,
                      pending=pending, unfinished=unfinished, halted=halted,
                      reason=risk.get('reason') or '', pending_flow=flow != 0,
                      symbol='BTCUSDT', ledger_digest=digest,
                      api_key_matches=(current_fingerprint == fingerprint) if current_fingerprint else None)
        after = (process_probe or legacy_process_state)(root)
        result.update(raw_running=process['active'] or after['active'],
                      process_verified=process['verified'] and after['verified'],
                      unknown=not (process['verified'] and after['verified']))
        result['review_available'] = bool(result['process_verified'] and not result['raw_running'] and
                                           position is None and pending == 0 and not unfinished and flow == 0)
        if not result['review_available']:
            result['error'] = '旧项目仍有执行进程或未完成登记，暂不能切换'
        return result
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return dict(result, error='旧项目停机账本核对未完成，暂不能切换')


def accepted_review(old, current_fingerprint, record):
    return bool(old.get('review_available') and current_fingerprint and isinstance(record, dict)
                and record.get('confirmed') is True
                and record.get('account_key_fingerprint') == current_fingerprint
                and record.get('ledger_digest') == old.get('ledger_digest'))
