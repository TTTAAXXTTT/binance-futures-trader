"""Editable allocations: fake exchange and disposable SQLite, no live writes."""
import copy
import concurrent.futures
import json
from decimal import Decimal as D
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from execution.engine import Engine, LEDGER_META, ExecutionFault
from execution.store import Store
from execution.equity_adjustments import validate_request, adjustment_summary
from execution.strategy import HOUR_MS
from test_hourly_engine import FakeExchange, trade_row, fill_row


class EditableEquityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'test.sqlite'
        self.store = Store(self.path)
        self.client = FakeExchange()
        self.engine = Engine(self.client, self.store, activation_check=lambda: True)
        self.client.now -= HOUR_MS
        self.engine._initialize_strategy_ledgers(['ETHUSDT'])
        self.engine.set_symbols({'BTCUSDT': False, 'ETHUSDT': True})
        self.client.now += HOUR_MS
        self.engine._persist_intent(True, 'human_start')
        self.engine.state = 'running'
        self.client.calls.clear()

    def save(self, symbol='ETHUSDT', value='50', revision=0, operation='equity-operation-0001'):
        return self.engine.set_strategy_equity(symbol=symbol, target_equity=value,
            expected_revision=revision, operation_id=operation)

    def record(self, symbol='ETHUSDT'):
        return self.store.meta(LEDGER_META)[symbol]

    def journal(self, *, symbol='ETHUSDT', operation='equity-operation-0001', revision=1):
        record = self.record(symbol)
        return dict(version=1, operation_id=operation, symbol=symbol, revision=revision,
            time_ms=self.client.now, activation_boundary_ms=self.client.now // HOUR_MS * HOUR_MS,
            ledger_epoch=record['epoch'], observed_equity='100', observed_wallet='100',
            observed_unrealized='0', target_equity='50', delta='-50', source='human_equity_edit')

    def test_save_current_equity_and_preserve_historical_initial_and_epoch(self):
        before = self.record()
        result = self.save()
        after = self.record()
        self.assertEqual(result['adjustment']['target_equity'], '50')
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']), D(50))
        for key in ('initial_wallet','epoch','initialized_at_ms','activation_boundary_ms'):
            self.assertEqual(before[key], after[key])
        self.assertEqual(self.client.writes(), [])

    def test_disabled_uninitialized_symbol_is_initialized_without_enabling_or_starting(self):
        self.engine.pause()
        intent, selection = self.store.meta('trading_intent_v1'), self.store.meta('symbol_enabled')
        self.save(symbol='BTCUSDT', value='75')
        self.assertEqual(self.record('BTCUSDT')['initial_wallet'], '100')
        self.assertEqual(D(self.engine.strategy_ledgers['BTCUSDT']['equity']), D(75))
        self.assertEqual(self.store.meta('trading_intent_v1'), intent)
        self.assertEqual(self.store.meta('symbol_enabled'), selection)
        self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.engine.state, 'paused')
        self.assertEqual(self.client.writes(), [])

    def test_edit_one_symbol_never_adjusts_other_symbol(self):
        self.engine._initialize_strategy_ledgers(['BTCUSDT'])
        self.save()
        self.assertEqual(self.engine.strategy_ledgers['BTCUSDT']['equity'], '100')
        self.assertEqual(self.store.equity_adjustments('BTCUSDT'), [])

    def test_smallest_supported_amount_saves_as_decimal_without_scientific_notation(self):
        r=self.save(value='0.00000001')
        self.assertEqual(r['adjustment']['target_equity'],'0.00000001')
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']),D('0.00000001'))

    def test_disabled_symbol_can_be_edited_while_other_coin_stays_running(self):
        self.save(symbol='BTCUSDT',value='60')
        self.assertEqual(self.engine.state,'running')
        self.assertTrue(self.engine.desired_running)
        self.assertEqual(self.store.meta('symbol_enabled'),{'BTCUSDT':False,'ETHUSDT':True})

    def test_missing_strategy_activation_blocks_edit(self):
        self.engine.activation_check=lambda:False
        with self.assertRaises(RuntimeError):self.save()
        self.assertEqual(self.client.calls,[])

    def test_adjustment_without_owning_ledger_is_not_ignored(self):
        self.save()
        self.store.set_meta(LEDGER_META,{})
        with self.assertRaises(ExecutionFault):self.engine._rebuild_strategy_ledgers()

    def test_repeated_original_request_is_exactly_once_and_has_no_second_read(self):
        first = self.save()
        self.client.calls.clear()
        second = self.save(value='50.00')
        self.assertEqual(first['adjustment'], second['adjustment'])
        self.assertEqual(len(self.store.equity_adjustments()), 1)
        self.assertEqual(self.client.calls, [])

    def test_reused_operation_with_different_target_is_rejected(self):
        self.save()
        with self.assertRaises(RuntimeError): self.save(value='60')
        self.assertEqual(len(self.store.equity_adjustments()), 1)

    def test_reused_operation_with_different_symbol_is_rejected(self):
        self.save()
        with self.assertRaises(RuntimeError): self.save(symbol='BTCUSDT')

    def test_reused_operation_with_different_revision_is_rejected(self):
        self.save()
        with self.assertRaises(RuntimeError): self.save(revision=1)

    def test_stale_other_page_cannot_overwrite(self):
        self.save()
        self.client.calls.clear()
        with self.assertRaises(RuntimeError): self.save(value='70', operation='equity-operation-0002')
        self.assertEqual(self.client.calls, [])
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']), D(50))

    def test_second_revision_sets_new_target_not_a_repeated_delta(self):
        self.save()
        self.save(value='70', revision=1, operation='equity-operation-0002')
        self.assertEqual([D(r['delta']) for r in self.store.equity_adjustments()], [D(-50),D(20)])
        self.assertEqual(self.engine.strategy_ledgers['ETHUSDT']['allocation_revision'], 2)

    def test_reboot_rebuild_retains_adjustments_without_arming(self):
        self.save()
        reboot = Engine(self.client, Store(self.path))
        reboot._rebuild_strategy_ledgers()
        self.assertEqual(D(reboot.strategy_ledgers['ETHUSDT']['equity']), D(50))
        self.assertEqual(reboot.state, 'stopped')
        self.assertFalse(reboot.desired_running)

    def test_already_initialized_start_does_not_reset_edit(self):
        self.save()
        self.engine._initialize_strategy_ledgers(['ETHUSDT'])
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']), D(50))

    def test_actual_prior_and_later_realized_pnl_and_fees_keep_accumulating(self):
        t=trade_row('closed-history',symbol='ETHUSDT',ledger_epoch=self.record()['epoch'],status='closed')
        self.store.save_trade(t)
        def fill(identifier,pnl,fee):
            self.store.claim_fill('ETHUSDT',dict(fill_row(identifier,100+identifier,'0','100',self.client.now,
                trade=t['id'],purpose='exit',pnl=pnl,fee=fee),symbol='ETHUSDT'))
        fill(1,'20','1')
        self.save()
        self.assertEqual(D(self.store.equity_adjustments()[0]['delta']),D(-69))
        fill(2,'-3','.5')
        self.engine._rebuild_strategy_ledgers()
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']),D('46.5'))
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['realized_pnl']),D(17))
        self.assertEqual(len(self.store.fills()),2)

    def test_prior_and_late_visible_funding_remain_attributed_after_equity_edit(self):
        initialized=self.record()['initialized_at_ms']
        t=trade_row('funded-history',symbol='ETHUSDT',ledger_epoch=self.record()['epoch'],status='closed')
        self.store.save_trade(t)
        for i,purpose,time in [(1,'entry',initialized+100),(2,'exit',initialized+10000)]:
            self.store.claim_fill('ETHUSDT',dict(fill_row(i,100+i,'1','100',time,
                trade=t['id'],purpose=purpose,fee='0'),symbol='ETHUSDT'))
        income=dict(tranId=1,symbol='ETHUSDT',asset='USDT',incomeType='FUNDING_FEE',income='-.1',time=initialized+1000)
        self.store.claim_income(income)
        self.save()
        self.assertEqual(D(self.store.equity_adjustments()[0]['observed_equity']),D('99.9'))
        self.store.claim_income(dict(income,tranId=2,income='-.2',time=initialized+2000))
        self.assertFalse(self.store.claim_income(income))
        self.engine._rebuild_strategy_ledgers()
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']),D('49.8'))
        self.assertEqual(self.engine.strategy_ledgers['ETHUSDT']['funding_count'],2)

    def test_open_position_current_mark_equity_and_frozen_budget_are_independent(self):
        t=trade_row('stable-open',symbol='ETHUSDT',ledger_epoch=self.record()['epoch'],
            status='open',quantity='1',average='100',equity0='100',budget0='50',notional0='1500')
        self.store.save_trade(t)
        self.store.claim_fill('ETHUSDT',dict(fill_row(1,101,'1','100',self.client.now-1000,
            trade=t['id']),symbol='ETHUSDT'))
        self.client.position_rows=[dict(symbol='ETHUSDT',positionSide='BOTH',positionAmt='1',
            entryPrice='100',unRealizedProfit='10',markPrice='110')]
        # Stable confirmed protection is supplied by the fake; no order mutation.
        with patch.object(self.engine,'_effective_stop',return_value=(D(95),{})):
            self.save()
        ledger=self.engine.strategy_ledgers['ETHUSDT']
        self.assertEqual(D(ledger['equity']),D(50))
        self.assertEqual(D(ledger['wallet']),D(40))
        for key in ('equity0','budget0','notional0','entry_atr'):
            self.assertEqual(self.store.trade(t['id'])[key],t[key])
        self.engine.positions['ETHUSDT']['unRealizedProfit']='12'
        self.engine._rebuild_strategy_ledgers()
        self.assertEqual(D(self.engine.strategy_ledgers['ETHUSDT']['equity']),D(52))
        self.assertEqual(self.client.writes(),[])

    def test_unknown_order_blocks_before_reads(self):
        self.store.save_trade(trade_row('unknown',symbol='ETHUSDT',ledger_epoch=self.record()['epoch']))
        self.store.save_order(dict(id='entry-unknown',client_id='entry-unknown',trade_id='unknown',
            symbol='ETHUSDT',purpose='entry',kind='market',state='unknown'))
        with self.assertRaises(RuntimeError): self.save()
        self.assertEqual(self.store.equity_adjustments(),[])
        self.assertEqual(self.client.calls,[])

    def test_partial_fill_blocks_edit_without_protection_changes(self):
        self.store.save_trade(trade_row('partial',symbol='ETHUSDT',ledger_epoch=self.record()['epoch']))
        self.store.save_order(dict(id='partial-order',client_id='partial-order',trade_id='partial',
            symbol='ETHUSDT',purpose='entry',kind='market',state='confirmed',fills_complete=False))
        with self.assertRaises(RuntimeError): self.save()
        self.assertEqual(self.client.writes(),[])

    def test_unregistered_position_blocks_save(self):
        self.client.position_rows=[dict(symbol='ETHUSDT',positionSide='BOTH',positionAmt='1',entryPrice='100')]
        with self.assertRaises(RuntimeError): self.save()
        self.assertEqual(self.store.equity_adjustments(),[])

    def test_network_failure_does_not_append_adjustment_or_retry_write(self):
        with patch.object(self.client,'account',side_effect=OSError('offline')):
            with self.assertRaises(OSError): self.save()
        self.assertEqual(self.store.equity_adjustments(),[])
        self.assertEqual(self.client.writes(),[])

    def test_funding_failure_does_not_append_or_clear_pause(self):
        self.engine.pause()
        with patch.object(self.client,'income',side_effect=OSError('income offline')):
            with self.assertRaises(OSError): self.save()
        self.assertEqual(self.store.equity_adjustments(),[])
        self.assertTrue(self.engine.funding_pending_recovery)
        self.assertFalse(self.engine.desired_running)

    def test_candidate_control_generation_and_same_hour_activation_retired(self):
        b=self.client.now//HOUR_MS*HOUR_MS
        c={'symbol':'ETHUSDT','boundary_ms':b,'signal':dict(boundary_ms=b,direction=1,branch='trend',atr='1'),
            'control':self.store.candidate_control('ETHUSDT'),'created_ms':b+1,'deadline_ms':b+300000}
        before=self.store.register_entry_candidate(c)
        self.assertIsNotNone(before)
        self.save()
        self.assertEqual(self.store.entry_candidate('ETHUSDT',b)['status'],'superseded')
        self.assertNotEqual(self.store.candidate_control('ETHUSDT'),c['control'])
        self.assertEqual(self.store.meta('symbol_enabled_after')['ETHUSDT'],b)
        self.assertEqual(self.store.register_entry_candidate(c)['status'],'superseded')

    def test_no_candidate_other_symbol_is_invalidated(self):
        self.engine._initialize_strategy_ledgers(['BTCUSDT'])
        self.engine.set_symbols({'ETHUSDT':True,'BTCUSDT':True})
        self.client.now+=HOUR_MS
        b=self.client.now//HOUR_MS*HOUR_MS
        c={'symbol':'BTCUSDT','boundary_ms':b,'signal':dict(boundary_ms=b,direction=1,branch='trend',atr='1'),
            'control':self.store.candidate_control('BTCUSDT'),'created_ms':b+1,'deadline_ms':b+300000}
        before=self.store.register_entry_candidate(c)
        self.save()
        self.assertEqual(self.store.entry_candidate('BTCUSDT',b),before)

    def test_store_insert_failure_rolls_back_fence_activation_and_journal(self):
        control=self.store.candidate_control('ETHUSDT')
        with self.store.connection() as db:
            db.execute("CREATE TRIGGER fail_equity BEFORE INSERT ON hb_equity_adjustments BEGIN SELECT RAISE(ABORT,'crash'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.commit_equity_adjustment(self.journal(),expected_ledger=self.record())
        self.assertEqual(self.store.equity_adjustments(),[])
        self.assertEqual(self.store.candidate_control('ETHUSDT'),control)

    def test_concurrent_duplicate_store_commit_is_exactly_once(self):
        record=self.record(); row=self.journal()
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            result=list(pool.map(lambda _:Store(self.path).commit_equity_adjustment(row,expected_ledger=record),range(8)))
        self.assertEqual(len(self.store.equity_adjustments()),1)
        self.assertTrue(all(r==row for r in result))

    def test_concurrent_different_edits_one_revision_wins(self):
        record=self.record()
        def commit(i):
            try:return Store(self.path).commit_equity_adjustment(self.journal(operation='equity-operation-'+str(i).zfill(4)),expected_ledger=record)
            except RuntimeError:return None
        with concurrent.futures.ThreadPoolExecutor(2) as pool: results=list(pool.map(commit,[1,2]))
        self.assertEqual(sum(r is not None for r in results),1)
        self.assertEqual(len(self.store.equity_adjustments()),1)

    def test_corrupt_adjustment_blocks_verified_ledger(self):
        self.save()
        with self.store.connection() as db:
            row=self.store.equity_adjustments()[0];row['delta']='999'
            db.execute('UPDATE hb_equity_adjustments SET value=?',(json.dumps(row),))
        self.assertTrue(self.engine._rebuild_strategy_ledgers())
        self.assertFalse(self.engine.strategy_ledgers['ETHUSDT']['verified'])


class EquityValidationTests(unittest.TestCase):
    def test_rejects_invalid_amounts(self):
        for v in [None,50,True,'0','-1','NaN','Infinity','1e3',' 50','50,0','.5','1.123456789','1234567890123','']:
            with self.subTest(value=v),self.assertRaises(ValueError):validate_request('BTCUSDT',v,0,'operation-123456789')

    def test_accepts_positive_eight_decimal_amount(self):
        self.assertEqual(D(validate_request('BTCUSDT','0.00000001',0,'operation-123456789')),D('.00000001'))

    def test_rejects_invalid_symbol_revision_or_operation(self):
        for s,r,o in [([],0,'operation-123456789'),({},0,'operation-123456789'),('XRPUSDT',0,'operation-123456789'),('BTCUSDT',True,'operation-123456789'),
            ('BTCUSDT',-1,'operation-123456789'),('BTCUSDT',0,'../secret'),('BTCUSDT',0,None)]:
            with self.subTest(symbol=s,revision=r),self.assertRaises(ValueError):validate_request(s,'50',r,o)

    def test_summary_rejects_orphan_epoch_or_missing_revision(self):
        with self.assertRaises(ValueError):adjustment_summary([{'symbol':'BTCUSDT','version':1,'revision':2}], 'BTCUSDT','epoch')


class EquityRpcTests(unittest.TestCase):
    def setUp(self):
        EditableEquityTests.setUp(self)
        from fastapi.testclient import TestClient
        from hourly_worker import build_app
        self.http=TestClient(build_app(self.engine,'local-fixture','local-password'))
        self.http.auth=('local-fixture','local-password')
        self.payload=dict(confirmed=True,symbol='ETHUSDT',target_equity='50',expected_revision=0,
                          operation_id='equity-rpc-operation-0001')

    def test_worker_rpc_applies_and_publishes_verified_amount(self):
        response=self.http.post('/api/strategy-equity',json=self.payload)
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(D(response.json()['strategy_ledgers']['ETHUSDT']['equity']),D(50))
        status=self.http.get('/api/status').json()
        self.assertEqual(status['strategy_ledgers']['ETHUSDT']['allocation_revision'],1)
        self.assertTrue(status['desired_running'])

    def test_rpc_query_is_read_only_and_unknown_id_returns_null(self):
        self.assertIsNone(self.http.get('/api/strategy-equity/equity-rpc-operation-0001').json()['adjustment'])
        self.assertEqual(self.client.calls,[])
        self.http.post('/api/strategy-equity',json=self.payload)
        self.client.calls.clear()
        r=self.http.get('/api/strategy-equity/equity-rpc-operation-0001')
        self.assertEqual(r.json()['adjustment']['target_equity'],'50')
        self.assertEqual(self.client.calls,[])

    def test_rpc_both_routes_require_authentication(self):
        self.assertEqual(self.http.post('/api/strategy-equity',json=self.payload,auth=('wrong','wrong')).status_code,401)
        self.assertEqual(self.http.get('/api/strategy-equity/equity-rpc-operation-0001',auth=('wrong','wrong')).status_code,401)
        self.assertEqual(self.store.equity_adjustments(),[])

    def test_missing_confirmation_cannot_save(self):
        self.payload.pop('confirmed')
        self.assertEqual(self.http.post('/api/strategy-equity',json=self.payload).status_code,422)
        self.assertEqual(self.store.equity_adjustments(),[])

    def test_rpc_stale_page_returns_conflict_without_duplicate_adjustment(self):
        self.http.post('/api/strategy-equity',json=self.payload)
        self.payload.update(operation_id='equity-rpc-operation-0002')
        self.assertEqual(self.http.post('/api/strategy-equity',json=self.payload).status_code,409)
        self.assertEqual(len(self.store.equity_adjustments()),1)

    def test_rpc_invalid_amount_returns_validation_error(self):
        self.payload.update(target_equity='NaN')
        self.assertEqual(self.http.post('/api/strategy-equity',json=self.payload).status_code,422)

    def test_console_forwards_once_without_start_or_fallback_mutation(self):
        from control import hourly_app as console
        result={'ok':True,'adjustment':{'operation_id':self.payload['operation_id']}}
        with patch.object(console,'hourly_rpc',return_value=result) as rpc,patch.object(console,'launch_stopped_worker') as launch:
            self.assertEqual(console.strategy_equity(self.payload),result)
            rpc.assert_called_once_with('strategy-equity','POST',self.payload,timeout=45)
            launch.assert_not_called()

    def test_console_timeout_does_not_replay_or_launch(self):
        from control import hourly_app as console
        from fastapi import HTTPException
        with patch.object(console,'hourly_rpc',side_effect=TimeoutError()) as rpc,patch.object(console,'launch_stopped_worker') as launch:
            with self.assertRaises(HTTPException) as error:console.strategy_equity(self.payload)
            self.assertEqual(error.exception.status_code,503)
            self.assertEqual(rpc.call_count,1)
            launch.assert_not_called()

    def test_console_query_only_reads_original_identity(self):
        from control import hourly_app as console
        with patch.object(console,'hourly_rpc',return_value={'ok':True,'adjustment':None}) as rpc:
            console.strategy_equity_result('equity-rpc-operation-0001')
            rpc.assert_called_once_with('strategy-equity/equity-rpc-operation-0001',timeout=5)

    def test_console_newer_allocation_publishes_without_overwriting_pause_or_other_coin(self):
        from control import hourly_app as c
        state={'worker':{'state':'paused','desired_running':False,'symbol_enabled':{'BTCUSDT':False,'ETHUSDT':False},
            'strategy_ledgers':{'BTCUSDT':{'equity':'70','allocation_revision':3},'ETHUSDT':{'equity':'100','allocation_revision':0}}},'control_revision':7}
        ack={'ok':True,'adjustment':{'operation_id':self.payload['operation_id']},
            'strategy_ledgers':{'ETHUSDT':{'equity':'50','allocation_revision':1}}}
        with patch.dict(c.STATE,state,clear=True),patch.object(c,'hourly_rpc',return_value=ack):
            c.strategy_equity(self.payload)
            self.assertEqual(c.STATE['worker']['strategy_ledgers']['ETHUSDT']['equity'],'50')
            self.assertEqual(c.STATE['worker']['strategy_ledgers']['BTCUSDT']['equity'],'70')
            self.assertEqual(c.STATE['worker']['state'],'paused')
            self.assertFalse(c.STATE['worker']['desired_running'])
            self.assertEqual(c.STATE['control_revision'],8)

    def test_console_late_old_allocation_response_cannot_overwrite_newer_edit(self):
        from control import hourly_app as c
        state={'worker':{'strategy_ledgers':{'ETHUSDT':{'equity':'75','allocation_revision':2}}},'control_revision':9}
        ack={'ok':True,'adjustment':{'operation_id':self.payload['operation_id']},
            'strategy_ledgers':{'ETHUSDT':{'equity':'50','allocation_revision':1}}}
        with patch.dict(c.STATE,state,clear=True),patch.object(c,'hourly_rpc',return_value=ack):
            c.strategy_equity(self.payload)
            self.assertEqual(c.STATE['worker']['strategy_ledgers']['ETHUSDT']['equity'],'75')
            self.assertEqual(c.STATE['control_revision'],9)
