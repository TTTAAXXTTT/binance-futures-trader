"""Persist an explicit process-stop choice; no network calls or order writes."""
import json
import sqlite3
from contextlib import closing
import time
from pathlib import Path


def save_manual_stop(root):
    root = Path(root).resolve()
    ledger = root / 'user_data/trades-live.sqlite'
    if not ledger.is_file():
        return False
    with closing(sqlite3.connect(ledger.as_uri() + '?mode=rw', uri=True, timeout=30)) as db, db:
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hb_meta'").fetchone():
            return False
        value = json.dumps({'version': 1, 'desired_running': False,
            'updated_at_ms': int(time.time() * 1000), 'source': 'human_pause'})
        db.execute('INSERT INTO hb_meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            ('trading_intent_v1', value))
        db.execute("DELETE FROM hb_meta WHERE key='trading_start_request_v1'")
    return True


if __name__ == '__main__':
    trader_root = Path(__file__).resolve().parents[1]
    saved = save_manual_stop(trader_root)
    if not saved and (trader_root / 'user_data/trades-live.sqlite').is_file():
        raise SystemExit('原账本存在但启停记录无法写入，停止操作未确认。')
