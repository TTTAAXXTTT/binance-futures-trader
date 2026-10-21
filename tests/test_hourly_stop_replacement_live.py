"""Real engine + durable SQLite + strict local exchange; never connects to Binance."""
import copy
import sys
import tempfile
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT/'tests')]
from execution.binance import ApiError, OrderUncertain
from execution.engine import Engine, ExecutionFault, LEDGER_META, STRATEGY, STRATEGY_VERSION, candles_from
from execution.strategy import HOUR_MS, analyze
from execution.store import Store
from test_hourly_engine import FakeExchange, trade_row, fill_row, raw_candle


class StrictExchange(FakeExchange):
    """Enforce the production -4130 rule, with both legal/rejected bridge modes."""
    def __init__(self, store):
        super().__init__()
        self.store = store
        self.allow_bridge = True
        self.uncertain_create = set()
        self.uncertain_cancel = set()
        self.defer_cancel = set()
        self.read_failure = set()
        self.latest = '100'

    def account_config(self):
        self._call('account_config')
        return {'canTrade': True, 'dualSidePosition': False, 'multiAssetsMargin': False}

    def contract_price(self, symbol):
        self._call('contract_price', symbol)
        return {'price': self.latest}

    def role(self, raw):
        return 'final' if raw.get('closePosition') else 'bridge'

    def query_algo(self, algo_id=None, client_id=None):
        raw = super().query_algo(algo_id=algo_id, client_id=client_id)
        if raw.get('clientAlgoId') in self.read_failure:
            raise ApiError(None, 'offline read failed', recovery_kind='network_read_failure')
        return raw

    def new_stop(self, symbol, side, trigger, client_id):
        self._call('new_stop', symbol, side, trigger, client_id)
        if any(r['symbol'] == symbol and r['side'] == side and r['algoStatus'] == 'NEW' and
               r.get('closePosition') for r in self.algo_rows.values()):
            raise ApiError(-4130, 'existing same-direction closePosition protection')
        return self.create(symbol, side, trigger, client_id, 'final')

    def new_stop_quantity(self, symbol, side, trigger, client_id, quantity):
        self._call('new_stop_quantity', symbol, side, trigger, client_id, quantity)
        if not self.allow_bridge:
            raise ApiError(-2022, 'quantity bridge rejected')
        return self.create(symbol, side, trigger, client_id, 'bridge', quantity)

    def create(self, symbol, side, trigger, client_id, role, quantity=None):
        intent = self.store.order(client_id)
        assert intent and intent['state'] == 'prepared', 'wire preceded durable intent'
        state = self.store.trade(intent['trade_id'])['stop_replacement_v1']
        assert state[role+'_id'] == client_id, 'wire preceded saved phase identity'
        identifier = self.next_algo
        self.next_algo += 1
        raw = self.stop_row(identifier, client_id, trigger, symbol, side)
        raw.update(quantity='0.0', reduceOnly=True)  # Actual normalized close-all shape.
        if role == 'bridge':
            raw.update(closePosition=False, quantity=str(quantity), reduceOnly=True)
        self.algo_rows[identifier] = raw
        if role in self.uncertain_create:
            self.uncertain_create.remove(role)
            raise OrderUncertain(None, 'offline accepted then timeout', client_id=client_id)
        return copy.deepcopy(raw)

    def cancel_algo(self, algo_id):
        raw = self.algo_rows[algo_id]
        role = 'old' if raw['clientAlgoId'] == 'old-stop' else self.role(raw)
        self._call('cancel_algo', algo_id=algo_id)
        targets = [o for o in self.store.orders() if str(o.get('order_id')) == str(algo_id)]
        assert targets
        assert any(o['kind']=='cancel' and o.get('target_id')==targets[0]['id'] and
                   o['state']=='prepared' for o in self.store.orders()), 'cancel not durably saved'
        if role not in self.defer_cancel:
            raw['algoStatus'] = 'CANCELED'
        if role in self.uncertain_cancel:
            self.uncertain_cancel.remove(role)
            raise OrderUncertain(None, 'offline cancel reply lost')
        return copy.deepcopy(raw)


class StopReplacementLiveTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.store = Store(Path(self.folder.name)/'durable.sqlite')
        self.client = StrictExchange(self.store)
        self.store.set_meta(LEDGER_META, {s:dict(strategy=STRATEGY,version=STRATEGY_VERSION,
            epoch='fixture-ledger-'+s,initialized_at_ms=0,activation_boundary_ms=0,
            initial_wallet='100',wallet='100') for s in ('BTCUSDT','ETHUSDT')})
        self.engine = self.make_engine()

    def make_engine(self):
        engine = Engine(self.client,self.store,activation_check=lambda:True,restore_saved_intent=True)
        engine.state = 'running'
        engine._persist_intent(True,'human_start')
        engine.refresh_rules()
        engine.analyses = {s:SimpleNamespace(ready=True) for s in ('BTCUSDT','ETHUSDT')}
        return engine

    def position(self, direction=1, quantity='1'):
        self.client.position_rows = [dict(symbol='BTCUSDT',positionSide='BOTH',
            positionAmt=str(D(quantity)*direction),entryPrice='100',markPrice='100',unRealizedProfit='0')]
        trade = trade_row(status='open',direction=direction,quantity=quantity,average='100',
            fill_quantity=quantity,fill_average='100',budget0='50',notional0='1500',first_fill_ms=self.client.now-7200000)
        self.store.save_trade(trade)
        fill = fill_row(1,100,quantity,100,self.client.now-7200000)
        self.store.claim_fill('BTCUSDT',fill)
        raw = dict(symbol='BTCUSDT',orderId=100,clientOrderId='entry',status='FILLED',
            executedQty=quantity,avgPrice='100',origQty=quantity)
        self.client.order_rows[100]=raw
        self.client.fill_rows[100]=[fill]
        self.store.save_order(dict(id='entry',client_id='entry',trade_id=trade['id'],symbol='BTCUSDT',
            kind='market',purpose='entry',state='terminal',order_id=100,raw=raw,fills_complete=True))
        old_price='98' if direction==1 else '102'
        raw=self.client.stop_row(400,'old-stop',old_price,side='SELL' if direction==1 else 'BUY')
        raw.update(quantity='0.0',reduceOnly=True)
        self.client.algo_rows[400]=raw
        self.store.save_order(dict(id='old-stop',client_id='old-stop',kind='stop',purpose='protection',
            trade_id=trade['id'],symbol='BTCUSDT',state='confirmed',order_id=400,raw=raw))
        self.engine.reconcile()
        return self.store.trade(trade['id'])

    def start_replacement(self, direction=1):
        trade=self.position(direction)
        self.engine._ensure_stop(trade,D('99' if direction==1 else '101'))
        return self.store.trade(trade['id'])

    def drive(self, rounds=20, restart=False):
        for _ in range(rounds):
            self.client.now += 2000
            if restart:
                self.engine=self.make_engine()
            self.engine.reconcile()
            self.engine._manage_protection()
            state=self.store.trade('trade-1').get('stop_replacement_v1')
            if state and state['phase']=='complete':
                return state
        return self.store.trade('trade-1').get('stop_replacement_v1')

    def writes(self, name):
        return [r for r in self.client.calls if r[0]==name]

    def bridge(self):
        state=self.store.trade('trade-1')['stop_replacement_v1']
        return self.store.order(state['bridge_id'])

    def test_success_long_confirmed_bridge_before_old_cancel_final_before_bridge_cancel(self):
        self.start_replacement();state=self.drive()
        self.assertEqual(state['phase'],'complete')
        bridge=self.store.order(state['bridge_id']);final=self.store.order(state['final_id'])
        self.assertEqual(bridge['raw']['algoStatus'],'CANCELED')
        self.assertEqual(final['raw']['algoStatus'],'NEW')
        actions=[c for c in self.client.calls if c[0] in {'new_stop','new_stop_quantity','cancel_algo'}]
        self.assertEqual([a[0] for a in actions],['new_stop','new_stop_quantity','cancel_algo','new_stop','cancel_algo'])
        self.assertLess(next(i for i,c in enumerate(self.client.calls) if c[0]=='query_algo' and c[2].get('algo_id')==bridge['order_id']),
                        next(i for i,c in enumerate(self.client.calls) if c[0]=='cancel_algo'))

    def test_success_short_normalized_close_all_response(self):
        self.start_replacement(-1);state=self.drive()
        self.assertEqual(state['phase'],'complete');self.assertEqual(state['target'],'101')

    def test_real_normalized_old_stop_literal_is_valid(self):
        trade=trade_row(symbol='ETHUSDT',direction=-1,quantity='.579',status='open')
        raw=dict(algoId=3000002244327771,clientAlgoId='real-shape',symbol='ETHUSDT',side='BUY',
            algoStatus='NEW',orderType='STOP_MARKET',triggerPrice='2599.46',workingType='CONTRACT_PRICE',
            closePosition=True,priceProtect=False,positionSide='BOTH',quantity='0.0',reduceOnly=True)
        self.assertTrue(self.engine._valid_stop(trade,raw))

    def test_bridge_structural_rejection_keeps_old_no_same_context_retry(self):
        self.client.allow_bridge=False;self.start_replacement();self.client.now+=3600000;self.drive(3)
        self.assertEqual(len(self.writes('new_stop_quantity')),1)
        self.assertEqual(self.client.algo_rows[400]['algoStatus'],'NEW')
        self.assertEqual(self.store.trade('trade-1')['stop_replacement_v1']['phase'],'blocked_bridge')
        self.assertEqual(self.engine._recovery['kind'],'protection_replacement')

    def test_bridge_unknown_creation_queries_original_never_replays(self):
        self.client.uncertain_create.add('bridge');self.start_replacement();state=self.drive()
        self.assertEqual(state['phase'],'complete');self.assertEqual(len(self.writes('new_stop_quantity')),1)

    def test_final_unknown_creation_queries_original_never_replays(self):
        self.client.uncertain_create.add('final');self.start_replacement();state=self.drive()
        self.assertEqual(state['phase'],'complete');self.assertEqual(len(self.writes('new_stop')),2)

    def test_old_unknown_cancel_queries_original_never_replays(self):
        self.client.uncertain_cancel.add('old');self.start_replacement();state=self.drive()
        self.assertEqual(state['phase'],'complete')
        self.assertEqual(sum(c[2].get('algo_id')==400 for c in self.writes('cancel_algo')),1)

    def test_bridge_unknown_cancel_queries_original_never_replays(self):
        self.client.uncertain_cancel.add('bridge');self.start_replacement();state=self.drive()
        self.assertEqual(state['phase'],'complete');self.assertEqual(len(self.writes('cancel_algo')),2)

    def test_restart_every_stage_keeps_request_ids_and_preserves_ledger(self):
        self.start_replacement();original=self.store.meta(LEDGER_META)['BTCUSDT']['epoch'];state=self.drive(restart=True)
        self.assertEqual(state['phase'],'complete');self.assertEqual(len(self.writes('new_stop_quantity')),1)
        self.assertEqual(self.store.meta(LEDGER_META)['BTCUSDT']['epoch'],original)

    def test_cancel_ack_without_terminal_does_not_create_final(self):
        self.client.defer_cancel.add('old');self.start_replacement();self.drive(6)
        self.assertEqual(len(self.writes('new_stop')),1)
        self.assertEqual(self.client.algo_rows[400]['algoStatus'],'NEW')
        self.assertEqual(self.store.trade('trade-1')['stop_replacement_v1']['phase'],'cancel_old')

    def test_counterpart_bridge_trigger_during_old_cancel_blocks_final(self):
        self.client.defer_cancel.add('old');self.start_replacement();self.drive(3)
        bridge=self.bridge();self.client.algo_rows[bridge['order_id']]['algoStatus']='TRIGGERED';self.drive(2)
        self.assertEqual(self.store.trade('trade-1')['stop_replacement_v1']['phase'],'exit')
        self.assertEqual(len(self.writes('new_stop')),1)
        self.engine._risk_and_exit();self.assertFalse(self.writes('market_order'))

    def test_final_disappears_while_bridge_cancel_pending_never_completes(self):
        self.client.defer_cancel.add('bridge');self.start_replacement();state=self.drive(7)
        final=self.store.order(state['final_id']);self.client.algo_rows[final['order_id']]['algoStatus']='CANCELED'
        self.drive(2);self.assertNotEqual(self.store.trade('trade-1')['stop_replacement_v1']['phase'],'complete')

    def test_partial_entry_cancelled_before_quantity_bridge(self):
        trade=self.position();entry=self.store.order('entry');entry.update(state='confirmed',fills_complete=True)
        entry['raw']['status']='PARTIALLY_FILLED';self.client.order_rows[100]['status']='PARTIALLY_FILLED';self.store.save_order(entry)
        self.engine._ensure_stop(trade,D('99'));self.assertFalse(self.writes('new_stop_quantity'))
        self.assertTrue(self.writes('cancel_order'));self.drive()
        self.assertEqual(len(self.writes('new_stop_quantity')),1)

    def test_late_unmatched_fill_quantity_blocks_old_cancellation(self):
        self.start_replacement();self.client.position_rows[0]['positionAmt']='1.1';self.drive(2)
        self.assertEqual(self.client.algo_rows[400]['algoStatus'],'NEW')
        self.assertFalse(self.writes('cancel_algo'))

    def test_late_matched_quantity_rebuilds_bridge_with_old_retained(self):
        self.start_replacement();self.client.position_rows[0]['positionAmt']='1.1'
        fill=fill_row(2,100,'.1',100,self.client.now+1);self.client.fill_rows[100].append(fill)
        self.client.order_rows[100]['executedQty']='1.1';entry=self.store.order('entry');entry['fills_complete']=False;self.store.save_order(entry)
        state=self.drive(30);self.assertEqual(state['phase'],'complete')
        self.assertEqual(D(self.store.order(state['bridge_id'])['quantity']),D('1.1'))
        self.assertEqual(len(self.writes('new_stop_quantity')),2)

    def test_pause_keeps_maintenance_but_never_restores_user_intent(self):
        self.start_replacement();self.engine.pause();state=self.drive()
        self.assertEqual(state['phase'],'complete');self.assertFalse(self.engine.desired_running)
        self.assertEqual(self.store.meta('trading_intent_v1')['source'],'human_pause')

    def test_stopped_engine_never_advances_protection_writes(self):
        self.start_replacement();self.engine.state='stopped';before=len(self.client.calls)
        self.engine._manage_protection();self.assertEqual(len(self.client.calls),before)

    def test_candidate_already_touched_records_exit_and_retains_old(self):
        self.start_replacement();self.client.latest='98.5';self.drive(1)
        self.assertIsNotNone(self.store.trade('trade-1')['exit_intent'])
        self.assertEqual(self.client.algo_rows[400]['algoStatus'],'NEW')

    def test_bad_bridge_parameters_are_not_effective_protection(self):
        self.start_replacement();order=self.bridge();trade=self.store.trade('trade-1');raw=copy.deepcopy(order['raw'])
        for key,value in [('reduceOnly',False),('closePosition',True),('workingType','MARK_PRICE'),('priceProtect',True),('side','BUY'),('quantity','.1'),('clientAlgoId','foreign')]:
            with self.subTest(key=key):
                changed=dict(raw,**{key:value});self.assertFalse(self.engine._valid_stop(trade,changed,order))

    def test_pending_replacement_blocks_entries_and_resume_without_generic_failure(self):
        self.start_replacement();self.assertTrue(self.engine._blocked_orders())
        with patch.object(self.engine,'_verify_resume_conditions') as verify:
            self.engine._maybe_resume(fresh_reconcile=True,generation=self.engine._fault_generation)
        verify.assert_not_called()

    def test_fresh_full_account_config_avoids_two_mode_requests(self):
        self.client.calls.clear();self.assertFalse(self.engine._fresh_account_configuration()['dualSidePosition'])
        self.assertEqual([c[0] for c in self.client.calls],['account_config'])

    def test_account_config_only_missing_mode_uses_one_fallback(self):
        with patch.object(self.client,'account_config',return_value={'canTrade':True,'dualSidePosition':False}):
            self.client.calls.clear();config=self.engine._fresh_account_configuration()
        self.assertFalse(config['multiAssetsMargin']);self.assertEqual([c[0] for c in self.client.calls],['multi_assets_mode'])

    def test_present_invalid_mode_never_uses_legacy_false_to_override(self):
        self.position();self.engine._risk_and_exit();self.engine.reconcile()
        with patch.object(self.client,'account_config',return_value={'canTrade':True,'dualSidePosition':True,'multiAssetsMargin':False}):
            with self.assertRaisesRegex(ExecutionFault,'单向'):
                self.engine._verify_resume_conditions()

    def test_prepared_bridge_crash_before_trade_link_is_adopted_not_replayed(self):
        self.start_replacement();state=self.store.trade('trade-1')['stop_replacement_v1']
        original=state['bridge_id'];state['bridge_id']=None
        trade=self.store.trade('trade-1');trade['stop_replacement_v1']=state;self.store.save_trade(trade)
        self.engine=self.make_engine();self.drive()
        self.assertEqual(len(self.writes('new_stop_quantity')),1)
        self.assertEqual(self.store.trade('trade-1')['stop_replacement_v1']['bridge_id'],original)

    def test_original_bridge_read_unknown_preserves_old_without_new_wire(self):
        self.start_replacement();bridge=self.bridge();self.client.read_failure.add(bridge['client_id'])
        with self.assertRaises(ExecutionFault):self.engine._manage_protection()
        self.assertEqual(len(self.writes('new_stop_quantity')),1)
        self.assertEqual(self.client.algo_rows[400]['algoStatus'],'NEW')
        self.client.read_failure.clear();self.assertEqual(self.drive()['phase'],'complete')

    def test_flat_and_fill_fees_confirmed_cleans_replacement_and_unblocks(self):
        self.start_replacement();self.client.position_rows=[]
        raw=dict(symbol='BTCUSDT',orderId=700,clientOrderId='stop-child',status='FILLED',executedQty='1',avgPrice='99')
        self.client.order_rows[700]=raw
        self.client.fill_rows[700]=[fill_row(2,700,'1','99',self.client.now+1,purpose='stop_exit',pnl='-1')]
        self.client.algo_rows[400].update(algoStatus='FINISHED',actualOrderId='700')
        state=self.drive();self.assertEqual(state['phase'],'complete')
        self.assertEqual(self.store.trade('trade-1')['status'],'closed')
        self.engine.reconcile();self.assertFalse(self.engine._blocked_orders())

    def test_partial_protective_child_continues_reduce_only_exit_of_remainder(self):
        self.start_replacement();self.client.position_rows[0]['positionAmt']='.6'
        self.client.order_rows[700]=dict(symbol='BTCUSDT',orderId=700,clientOrderId='stop-child',status='FILLED',executedQty='.4',avgPrice='99')
        self.client.fill_rows[700]=[fill_row(2,700,'.4','99',self.client.now+1,purpose='stop_exit',pnl='-.4')]
        self.client.algo_rows[400].update(algoStatus='FINISHED',actualOrderId='700')
        self.drive(1);self.engine._risk_and_exit()
        exits=self.writes('market_order');self.assertEqual(len(exits),1)
        self.assertTrue(exits[0][2]['reduce_only']);self.assertEqual(D(exits[0][1][2]),D('.6'))

    def test_reduction_during_bridge_keeps_coverage_of_smaller_position(self):
        self.start_replacement();self.client.position_rows[0]['positionAmt']='.7'
        raw=dict(symbol='BTCUSDT',orderId=700,clientOrderId='reduce',status='FILLED',executedQty='.3',avgPrice='100')
        self.client.order_rows[700]=raw
        fill=fill_row(2,700,'.3','100',self.client.now+1,purpose='reduce');self.client.fill_rows[700]=[fill]
        self.store.save_order(dict(id='reduce',client_id='reduce',kind='market',purpose='reduce',trade_id='trade-1',
            symbol='BTCUSDT',state='terminal',order_id=700,raw=raw,fills_complete=False))
        state=self.drive();self.assertEqual(state['phase'],'complete')
        self.assertEqual(len(self.writes('new_stop_quantity')),1)

    def test_structural_rejection_context_change_allows_distinct_attempt(self):
        self.client.allow_bridge=False;self.start_replacement();self.client.allow_bridge=True
        self.engine._ensure_stop(self.store.trade('trade-1'),D('99.5'))
        state=self.drive(30);self.assertEqual(state['phase'],'complete')
        self.assertEqual(len(self.writes('new_stop_quantity')),2)
        self.assertNotEqual(self.writes('new_stop_quantity')[0][1][3],self.writes('new_stop_quantity')[1][1][3])

    def prepare_catchup(self, *, close='104.9', latest='105', already=False):
        self.position();rows=[raw_candle(i,high='100',low='99.8',close='99.9') for i in range(1001)]
        rows[-1]=raw_candle(1000,high='105',low='99',close=close)
        self.store.save_candles('BTCUSDT',rows);self.client.candle_rows['BTCUSDT']=rows
        self.client.latest=latest;self.engine=self.make_engine();self.engine.reconcile()
        self.engine.analyses['BTCUSDT']=analyze(candles_from(rows),self.client.now)
        if already:
            trade=self.store.trade('trade-1');trade['last_trail_hour']=1000*HOUR_MS;self.store.save_trade(trade)

    def test_restart_mid_hour_catches_up_trailing_without_new_entry_or_retro_trigger(self):
        self.prepare_catchup();self.assertTrue(self.engine._catchup_position_exits())
        trade=self.store.trade('trade-1');self.assertIsNotNone(trade.get('stop_replacement_v1'))
        self.assertGreater(D(trade['stop_replacement_v1']['target']),D('99'))
        self.assertIsNone(trade.get('exit_intent'))  # Last bar low was below the NEW target.
        self.assertFalse(self.writes('market_order'))

    def test_catchup_latest_price_touch_uses_exit_without_creating_bridge(self):
        self.prepare_catchup(latest='100');self.engine._catchup_position_exits()
        self.assertIsNotNone(self.store.trade('trade-1')['exit_intent'])
        self.assertFalse(self.writes('new_stop_quantity'))

    def test_catchup_already_processed_hour_does_not_repeat_stop_update(self):
        self.prepare_catchup(already=True);self.engine._catchup_position_exits()
        self.assertFalse(self.writes('new_stop_quantity'))

    def test_catchup_persists_indicator_exit_without_entry_replay(self):
        self.prepare_catchup(close='99');self.engine._catchup_position_exits()
        self.assertIsNotNone(self.store.trade('trade-1')['exit_intent'])
        self.assertFalse(self.writes('market_order'))

    def test_stale_or_incomplete_catchup_does_not_grant_entry_or_mutate_orders(self):
        self.prepare_catchup();self.engine.last_successful_reconcile_ms-=31000
        self.assertFalse(self.engine._catchup_position_exits());self.assertFalse(self.writes('new_stop_quantity'))


if __name__=='__main__':
    unittest.main()
