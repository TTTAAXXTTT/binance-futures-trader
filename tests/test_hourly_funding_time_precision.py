"""Legacy funding timestamp precision: preserve economics, reject boundary changes."""
import sys,tempfile,unittest,copy
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from execution.store import Store,funding_time_equivalent
T=1791763200000
class FundingPrecisionTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.store=Store(Path(self.temp.name)/'offline.sqlite')
  self.old=dict(tranId=1,incomeType='FUNDING_FEE',symbol='ETHUSDT',asset='USDT',time=T,income='0.02171392')
  self.new=dict(self.old,time=T+169)
  self.ledgers={'ETHUSDT':dict(strategy='HourlyTrendRange',version=1,epoch='offline-ETH',initial_wallet='100',activation_boundary_ms=T-86400000,initialized_at_ms=T-86400000)}
  self.store.set_meta('hourly_trend_range_ledgers_v1',self.ledgers)
  self.store.claim_income(self.old)
 def check(self,old=None,new=None,ledgers=None,fills=None):
  return funding_time_equivalent(self.old if old is None else old,self.new if new is None else new,self.ledgers if ledgers is None else ledgers,[] if fills is None else fills)
 def test_observed_real_offsets_preserve_original_and_record_once(self):
  for delta in (169,213):self.assertTrue(self.check(new=dict(self.old,time=T+delta)))
  for _ in range(3):self.assertFalse(self.store.claim_income(self.new))
  self.assertEqual(self.store.incomes(),[self.old])
  audit=[r for r in self.store.events() if '时间精度' in r['message']]
  self.assertEqual(len(audit),1)
  self.assertEqual(self.store.meta('funding_timestamp_observations_v1')['1:ETHUSDT:USDT'],{'stored_time_ms':T,'observed_time_ms':T+169})
 def test_other_economic_fields_remain_strict(self):
  for change in ({'income':'.03'},{'incomeType':'COMMISSION'},{'asset':'BNB'},{'symbol':'BTCUSDT'}):
   self.assertFalse(self.check(new=dict(self.new,**change)))
  for change in ({'income':'.03'},{'incomeType':'COMMISSION'}):
   with self.assertRaises(ValueError):self.store.claim_income(dict(self.new,**change))
  self.assertEqual(self.store.incomes(),[self.old]);self.assertIsNone(self.store.meta('funding_timestamp_observations_v1'))
 def test_fill_anywhere_in_inclusive_interval_blocks_refinement(self):
  for delta in (0,1,100,169):
   fill={'symbol':'ETHUSDT','time':T+delta}
   self.assertFalse(self.check(fills=[fill]))
  self.assertTrue(self.check(fills=[{'symbol':'ETHUSDT','time':T-1},{'symbol':'ETHUSDT','time':T+170}]))
  self.assertTrue(self.check(fills=[{'symbol':'BTCUSDT','time':T+100}]))
 def test_store_refuses_changed_attribution_and_keeps_every_record(self):
  self.store.claim_fill('ETHUSDT',dict(symbol='ETHUSDT',id=7,time=T+100,qty='1'))
  with self.assertRaises(ValueError):self.store.claim_income(self.new)
  self.assertEqual(self.store.incomes(),[self.old]);self.assertEqual(len(self.store.fills()),1)
  self.assertIsNone(self.store.meta('funding_timestamp_observations_v1'))
 def test_initialization_boundary_and_unknown_ledger_block(self):
  for delta in (0,50,169):
   ledger=copy.deepcopy(self.ledgers);ledger['ETHUSDT']['initialized_at_ms']=T+delta
   self.assertFalse(self.check(ledgers=ledger))
  for value in ({},None,{'ETHUSDT':{}},{'ETHUSDT':dict(strategy='other',version=1,initialized_at_ms=T-1)}):
   self.assertFalse(funding_time_equivalent(self.old,self.new,value,[]))
  ledger=copy.deepcopy(self.ledgers);ledger['ETHUSDT']['initialized_at_ms']=T+170
  self.assertTrue(self.check(ledgers=ledger))
 def test_non_precision_time_changes_are_rejected(self):
  for old,new in ((T,T+1000),(T,T-1),(T+1,T+169),(T,T+86400000),(T,T+.5),(True,T),(T,'bad')):
   self.assertFalse(self.check(old=dict(self.old,time=old),new=dict(self.new,time=new)))
 def test_unknown_fill_times_cannot_prove_unchanged_attribution(self):
  for fills in ([{}],[None],[{'symbol':'ETHUSDT','time':True}],[{'symbol':'ETHUSDT','time':'bad'}],[{'symbol':'ETHUSDT','time':T+.5}]):self.assertFalse(self.check(fills=fills))
 def test_late_fill_invalidates_prior_precision_proof_on_ledger_rebuild(self):
  from test_hourly_funding_replay import FundingOwnershipIntegrationChecks
  fixture=FundingOwnershipIntegrationChecks();fixture.setUp()
  try:
   stamp=fixture.client.now-1000
   fixture.owned_trade(status='open',entry_time=stamp-1000)
   old=dict(tranId=701,incomeType='FUNDING_FEE',symbol='BTCUSDT',asset='USDT',time=stamp,income='.01')
   fixture.store.claim_income(old)
   self.assertFalse(fixture.store.claim_income(dict(old,time=stamp+169)))
   fixture.engine._rebuild_strategy_ledgers()
   self.assertTrue(fixture.engine.strategy_ledgers['BTCUSDT']['verified'])
   from test_hourly_funding_replay import fill_row
   late=fill_row(19,119,'.1','100',stamp+100,trade='owned-closed',purpose='exit',fee='.005',pnl='0')
   late['symbol']='BTCUSDT';fixture.store.claim_fill('BTCUSDT',late)
   problems=fixture.engine._rebuild_strategy_ledgers()
   self.assertFalse(fixture.engine.strategy_ledgers['BTCUSDT']['verified'])
   self.assertTrue(any('时间精度' in p for p in problems))
   self.assertEqual(fixture.store.incomes(),[old])
   self.assertEqual(fixture.client.writes(),[])
  finally:
   fixture.doCleanups()
 def test_malformed_amount_and_ledger_do_not_prove_equivalence(self):
  for amount in ('Infinity','NaN'):
   self.assertFalse(self.check(old=dict(self.old,income=amount),new=dict(self.new,income=amount)))
  for change in ({'version':True},{'epoch':None},{'activation_boundary_ms':True},{'initial_wallet':'Infinity'}):
   ledger=copy.deepcopy(self.ledgers);ledger['ETHUSDT'].update(change)
   self.assertFalse(self.check(ledgers=ledger))
 def test_audit_failure_rolls_back_without_mutating_original(self):
  self.store.set_meta('funding_timestamp_observations_v1',[])
  with self.assertRaisesRegex(ValueError,'元数据无效'):self.store.claim_income(self.new)
  self.assertEqual(self.store.incomes(),[self.old]);self.assertEqual(self.store.events(),[])
if __name__=='__main__':unittest.main()
