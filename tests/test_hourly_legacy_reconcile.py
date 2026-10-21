"""External legacy exit proof/application using synthetic evidence and temp SQLite only."""
import copy
import json
import sqlite3
import sys
import tempfile
import unittest
from decimal import Decimal as D
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from control.legacy_reconcile import apply_plan, make_plan, read_book, utc_text


FLAT = dict(connected=True, positions=[], open_orders=0, open_algo_orders=0, workers_stopped=True)
START = 1710000000000


class LegacyExternalExitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'original-offline-book.sqlite3'
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executescript('''
                CREATE TABLE trades(
                    id INTEGER PRIMARY KEY, pair TEXT, exchange TEXT, is_short INTEGER, is_open INTEGER,
                    amount REAL, open_rate REAL, leverage REAL, funding_fees REAL, stake_amount REAL,
                    stop_loss REAL, initial_stop_loss REAL, marker TEXT,
                    close_rate REAL, close_date TEXT, exit_reason TEXT, exit_order_status TEXT,
                    fee_open REAL, fee_open_cost REAL, fee_open_currency TEXT,
                    fee_close REAL, fee_close_cost REAL, fee_close_currency TEXT,
                    open_trade_value REAL, realized_profit REAL, close_profit_abs REAL, close_profit REAL
                );
                CREATE TABLE orders(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ft_trade_id INTEGER, ft_order_side TEXT,
                    ft_pair TEXT, ft_is_open INTEGER, ft_amount REAL, ft_price REAL, order_id TEXT,
                    status TEXT, symbol TEXT, order_type TEXT, side TEXT, price REAL, average REAL,
                    amount REAL, filled REAL, remaining REAL, cost REAL, order_date TEXT,
                    order_filled_date TEXT, order_update_date TEXT, funding_fee REAL, ft_order_tag TEXT,
                    marker TEXT
                );
                CREATE TABLE pairlocks(id INTEGER PRIMARY KEY, pair TEXT, lock_end_time TEXT, reason TEXT);
                CREATE TABLE original_wallet(id INTEGER PRIMARY KEY, wallet_usdt TEXT, marker TEXT);
                CREATE TABLE hb_meta(key TEXT PRIMARY KEY, value TEXT);
                INSERT INTO trades(id,pair,exchange,is_short,is_open,amount,open_rate,leverage,funding_fees,
                    stake_amount,stop_loss,initial_stop_loss,marker,fee_open,fee_open_cost,fee_open_currency)
                    VALUES(1,'BTC/USDT:USDT','binance',0,1,2,100,50,0,4,98,98,'retain-current',.0005,.1,'USDT');
                INSERT INTO trades(id,pair,exchange,is_short,is_open,amount,open_rate,leverage,funding_fees,
                    stake_amount,marker,close_rate,close_profit_abs,realized_profit,exit_reason)
                    VALUES(2,'ETH/USDT:USDT','binance',0,0,1.1,1500,50,0,33,'retain-history',1600,108,108,'original-exit');
                INSERT INTO orders(id,ft_trade_id,ft_order_side,ft_pair,ft_is_open,ft_amount,ft_price,order_id,
                    status,symbol,order_type,side,price,average,amount,filled,remaining,cost,marker)
                    VALUES(11,1,'buy','BTC/USDT:USDT',0,2,100,'101','closed','BTC/USDT:USDT','market',
                    'buy',100,100,2,2,0,200,'retain-entry');
                INSERT INTO orders(id,ft_trade_id,ft_order_side,ft_pair,ft_is_open,ft_amount,ft_price,order_id,
                    status,symbol,order_type,side,amount,filled,remaining,marker)
                    VALUES(12,1,'stoploss','BTC/USDT:USDT',1,2,98,'777','open','BTC/USDT:USDT','stop_market',
                    'sell',2,0,2,'retain-stop');
                INSERT INTO orders(id,ft_trade_id,ft_order_side,ft_pair,ft_is_open,ft_amount,ft_price,order_id,
                    status,symbol,order_type,side,price,average,amount,filled,remaining,cost,marker)
                    VALUES(13,2,'buy','ETH/USDT:USDT',0,1.1,1500,'303','closed','ETH/USDT:USDT','market',
                    'buy',1500,1500,1.1,1.1,0,1650,'retain-history-order');
                INSERT INTO pairlocks VALUES(1,'BTC/USDT:USDT','2030-01-01 00:00:00','original-loss-lock');
                INSERT INTO pairlocks VALUES(2,'ETH/USDT:USDT','2030-02-01 00:00:00','original-stats-lock');
                INSERT INTO original_wallet VALUES(1,'1234.56789','fees already accounted by exchange');
                INSERT INTO hb_meta VALUES('risk-budget','{"budget0":"10","actual_cost":"2.5"}');
            ''')
        self.evidence = self.proof()

    @staticmethod
    def proof(short=False):
        opening_side, closing_side = ('SELL', 'BUY') if short else ('BUY', 'SELL')
        exit_prices, exit_fees = (('90', '88'), ('.0225', '.066')) if short else (('110', '112'), ('.0275', '.084'))
        fills = [
            dict(id=1001, orderId=101, symbol='BTCUSDT', positionSide='BOTH', commissionAsset='USDT',
                 side=opening_side, qty='1', price='99', commission='.0495', realizedPnl='0', time=START),
            dict(id=1002, orderId=101, symbol='BTCUSDT', positionSide='BOTH', commissionAsset='USDT',
                 side=opening_side, qty='1', price='101', commission='.0505', realizedPnl='0', time=START + 1000),
            dict(id=1003, orderId=202, symbol='BTCUSDT', positionSide='BOTH', commissionAsset='USDT',
                 side=closing_side, qty='.5', price=exit_prices[0], commission=exit_fees[0], realizedPnl='5', time=START + 10000),
            dict(id=1004, orderId=202, symbol='BTCUSDT', positionSide='BOTH', commissionAsset='USDT',
                 side=closing_side, qty='1.5', price=exit_prices[1], commission=exit_fees[1], realizedPnl='18', time=START + 11000),
        ]
        wire = dict(orderId=202, symbol='BTCUSDT', status='FILLED', side=closing_side,
                    positionSide='BOTH', executedQty='2', origQty='2', avgPrice='88.5' if short else '111.5',
                    type='MARKET', time=START + 9999, updateTime=START + 11000)
        return dict(guard=copy.deepcopy(FLAT), fills=fills, funding=[], exit_order=wire,
                    algos=[dict(algoId=777, symbol='BTCUSDT', algoStatus='CANCELED', actualOrderId='0',
                                updateTime=START + 12000)])

    def rows(self, table):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]

    def book(self):
        return read_book(self.path, 1)

    def plan(self):
        return make_plan(self.book(), copy.deepcopy(self.evidence))

    def mutate(self, sql, values=()):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(sql, values)

    def audit_exists(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            return db.execute("SELECT 1 FROM sqlite_master WHERE name='legacy_reconciliation_audit'").fetchone() is not None

    def assert_refused(self, evidence):
        before = self.book()
        with self.assertRaises((ValueError, KeyError)):
            make_plan(before, evidence)
        self.assertEqual(self.book(), before)
        self.assertFalse(self.audit_exists())

    def test_long_full_exit_exact_net_and_fee_costs_without_wallet_debit(self):
        plan = self.plan()
        self.assertEqual(plan['profit_usdt'], '22.7885')
        self.assertEqual(D(str(plan['changes']['close_rate'])), D('111.5'))
        self.assertEqual(D(str(plan['changes']['fee_open_cost'])), D('.1'))
        self.assertEqual(D(str(plan['changes']['fee_close_cost'])), D('.1115'))
        self.assertEqual(plan['changes']['open_trade_value'], 200.1)
        self.assertAlmostEqual(plan['changes']['close_profit'], float(D('22.7885') / D('200.1') * 50))
        wallet = self.rows('original_wallet')
        result = apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        self.assertEqual(result['profit_usdt'], '22.7885')
        self.assertEqual(self.rows('original_wallet'), wallet)
        actual = self.rows('trades')[0]
        self.assertEqual(actual['is_open'], 0)
        self.assertEqual(actual['close_date'], utc_text(START + 11000))
        self.assertAlmostEqual(actual['realized_profit'], 22.7885)
        self.assertAlmostEqual(actual['close_profit_abs'], 22.7885)

    def test_short_full_exit_fee_direction_and_realized_profit(self):
        self.mutate('UPDATE trades SET is_short=1 WHERE id=1')
        self.mutate("UPDATE orders SET ft_order_side='sell',side='sell' WHERE id=11")
        self.mutate("UPDATE orders SET side='buy' WHERE id=12")
        self.evidence = self.proof(short=True)
        plan = self.plan()
        self.assertEqual(plan['profit_usdt'], '22.8115')
        self.assertEqual(plan['changes']['open_trade_value'], 199.9)
        self.assertEqual(plan['exit_row']['side'], 'buy')
        apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        actual = self.rows('trades')[0]
        self.assertEqual(actual['is_short'], 1)
        self.assertAlmostEqual(actual['close_profit'], float(D('22.8115') / D('199.9') * 50))

    def test_backup_audit_and_original_history_orders_locks_risk_retained(self):
        plan = self.plan()
        before_orders = self.rows('orders')
        before_history = self.rows('trades')[1]
        untouched = {table: self.rows(table) for table in ('pairlocks', 'original_wallet', 'hb_meta')}
        result = apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        backup = Path(result['backup_path'])
        self.assertTrue(backup.is_file())
        self.assertEqual(read_book(backup, 1), plan['before'])
        self.assertEqual(self.rows('trades')[1], before_history)
        for table, rows in untouched.items():
            self.assertEqual(self.rows(table), rows)
        after_orders = self.rows('orders')
        self.assertEqual(after_orders[0], before_orders[0])
        self.assertEqual(after_orders[2], before_orders[2])
        self.assertEqual(after_orders[1]['id'], before_orders[1]['id'])
        self.assertEqual(after_orders[1]['marker'], 'retain-stop')
        self.assertEqual(after_orders[1]['ft_is_open'], 0)
        self.assertEqual(after_orders[1]['status'], 'canceled')
        self.assertEqual(after_orders[1]['filled'], 0)
        self.assertEqual(len(after_orders), len(before_orders) + 1)
        self.assertEqual(after_orders[-1]['order_id'], '202')
        self.assertEqual(after_orders[-1]['ft_order_tag'], 'external_exit_reconciled')
        audit = self.rows('legacy_reconciliation_audit')
        self.assertEqual(len(audit), 1)
        self.assertEqual(audit[0]['trade_id'], 1)
        self.assertEqual(audit[0]['exit_order_id'], '202')
        self.assertEqual(json.loads(audit[0]['evidence_json'])['profit_usdt'], '22.7885')
        self.assertEqual(Path(audit[0]['backup_path']), backup)

    def test_repeated_apply_or_plan_cannot_insert_second_order_or_modify_closed_history(self):
        plan = self.plan()
        apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        before = {table: self.rows(table) for table in ('trades', 'orders', 'legacy_reconciliation_audit')}
        backups = list((self.path.parent / 'backups').glob('*.sqlite'))
        with self.assertRaises(ValueError):
            apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        with self.assertRaises(ValueError):
            make_plan(self.book(), self.evidence)
        for table, rows in before.items():
            self.assertEqual(self.rows(table), rows)
        self.assertEqual(list((self.path.parent / 'backups').glob('*.sqlite')), backups)

    def test_missing_quantity_or_added_fill_is_refused(self):
        missing_qty = copy.deepcopy(self.evidence)
        missing_qty['fills'][-1].pop('qty')
        self.assert_refused(missing_qty)
        partial = copy.deepcopy(self.evidence)
        partial['fills'][-1]['qty'] = '1'
        self.assert_refused(partial)
        added = copy.deepcopy(self.evidence)
        added['fills'].append(dict(added['fills'][-1], id=1005, orderId=203))
        self.assert_refused(added)
        duplicate = copy.deepcopy(self.evidence)
        duplicate['fills'].append(copy.deepcopy(duplicate['fills'][-1]))
        self.assert_refused(duplicate)

    def test_non_usdt_fee_funding_unknown_algo_or_unconfirmed_exit_refused(self):
        for change in ('fee', 'funding', 'missing-algo', 'new-algo', 'triggered-algo', 'child-algo', 'partial-exit'):
            evidence = copy.deepcopy(self.evidence)
            if change == 'fee':
                evidence['fills'][0]['commissionAsset'] = 'BNB'
            elif change == 'funding':
                evidence['funding'] = [{'income': '.01'}]
            elif change == 'missing-algo':
                evidence['algos'] = []
            elif change == 'new-algo':
                evidence['algos'][0]['algoStatus'] = 'NEW'
            elif change == 'triggered-algo':
                evidence['algos'][0]['algoStatus'] = 'TRIGGERED'
            elif change == 'child-algo':
                evidence['algos'][0]['actualOrderId'] = '909'
            else:
                evidence['exit_order']['status'] = 'PARTIALLY_FILLED'
            with self.subTest(change=change):
                self.assert_refused(evidence)
        self.mutate('UPDATE trades SET funding_fees=.01 WHERE id=1')
        self.assert_refused(copy.deepcopy(self.evidence))

    def test_active_worker_account_position_or_orders_refused_before_plan(self):
        guards = [dict(FLAT, connected=False), dict(FLAT, workers_stopped=False),
                  dict(FLAT, positions=[{'symbol': 'ETHUSDT', 'positionAmt': '1'}]),
                  dict(FLAT, open_orders=1), dict(FLAT, open_algo_orders=1)]
        for guard in guards:
            evidence = copy.deepcopy(self.evidence)
            evidence['guard'] = guard
            with self.subTest(guard=guard):
                self.assert_refused(evidence)

    def test_apply_first_guard_not_flat_leaves_original_book_and_creates_no_backup(self):
        plan = self.plan()
        before = self.book()
        with self.assertRaises(ValueError):
            apply_plan(self.path, plan, lambda: dict(FLAT, workers_stopped=False))
        self.assertEqual(self.book(), before)
        self.assertFalse((self.path.parent / 'backups').exists())

    def test_apply_second_guard_not_flat_leaves_records_unchanged_after_backup(self):
        plan = self.plan()
        before = self.book()
        guards = iter([copy.deepcopy(FLAT), dict(FLAT, open_orders=1)])
        with self.assertRaises(ValueError):
            apply_plan(self.path, plan, lambda: next(guards))
        self.assertEqual(self.book(), before)
        self.assertFalse(self.audit_exists())
        backups = list((self.path.parent / 'backups').glob('*.sqlite'))
        self.assertEqual(len(backups), 1)
        self.assertEqual(read_book(backups[0], 1), before)

    def test_book_change_before_apply_refused_without_backup(self):
        plan = self.plan()
        self.mutate("UPDATE trades SET marker='other-writer' WHERE id=1")
        with self.assertRaises(ValueError):
            apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        self.assertEqual(self.rows('trades')[0]['is_open'], 1)
        self.assertEqual(self.rows('trades')[0]['marker'], 'other-writer')
        self.assertFalse((self.path.parent / 'backups').exists())

    def test_concurrent_book_change_after_backup_rejected_inside_write_transaction(self):
        plan = self.plan()
        calls = []
        def concurrent_guard():
            calls.append(1)
            if len(calls) == 2:
                self.mutate("UPDATE trades SET marker='concurrent-writer' WHERE id=1")
            return copy.deepcopy(FLAT)
        with self.assertRaises(ValueError):
            apply_plan(self.path, plan, concurrent_guard)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.rows('trades')[0]['is_open'], 1)
        self.assertEqual(self.rows('trades')[0]['marker'], 'concurrent-writer')
        self.assertEqual(len(self.rows('orders')), 3)
        self.assertFalse(self.audit_exists())
        backup = next((self.path.parent / 'backups').glob('*.sqlite'))
        self.assertEqual(read_book(backup, 1), plan['before'])

    def test_conflicting_duplicate_algo_evidence_is_refused(self):
        evidence = copy.deepcopy(self.evidence)
        evidence['algos'].insert(0, dict(evidence['algos'][0], algoStatus='NEW'))
        self.assert_refused(evidence)

    def test_sql_insert_failure_rolls_back_audit_trade_and_stop_changes_atomically(self):
        # An unrelated historical order holds this ID under a synthetic global
        # uniqueness rule; target-trade evidence remains otherwise valid.
        self.mutate("UPDATE orders SET order_id='202' WHERE id=13")
        self.mutate('CREATE UNIQUE INDEX test_unique_order_id ON orders(order_id)')
        plan = self.plan()
        before_trade = self.book()
        before_orders = self.rows('orders')
        with self.assertRaises(sqlite3.IntegrityError):
            apply_plan(self.path, plan, lambda: copy.deepcopy(FLAT))
        self.assertEqual(self.book(), before_trade)
        self.assertEqual(self.rows('orders'), before_orders)
        self.assertFalse(self.audit_exists())
        backup = next((self.path.parent / 'backups').glob('*.sqlite'))
        self.assertEqual(read_book(backup, 1), before_trade)


if __name__ == '__main__':
    unittest.main()
