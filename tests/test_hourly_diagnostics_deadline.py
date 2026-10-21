"""A slow evidence checkpoint cannot spend an order deadline or age a signature."""
import sys,tempfile,unittest
from pathlib import Path
from urllib.parse import parse_qs
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT),str(ROOT/'tests')]
from execution.binance import BinanceClient,PooledHTTPTransport,RequestExpired,LocalTransportDeferred
from execution.diagnostics import ExecutionDiagnostics
from test_hourly_execution_diagnostics import Clock,FakePools,FakeTransport
class EvidenceDeadlineTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.clock=Clock();self.pools=FakePools()
  self.d=ExecutionDiagnostics(Path(self.temp.name)/'evidence.sqlite',release='offline',wall=lambda:self.clock.wall,monotonic=lambda:self.clock.mono)
 def client(self,timeout=15,delay=0,custom=False):
  self.d._persist=lambda value:self.clock.advance(delay)
  transport=FakeTransport({'ok':True}) if custom else PooledHTTPTransport(pool_factory=self.pools.factory,proxy_getter=lambda:{},bypass=lambda _:True,clock=lambda:self.clock.mono)
  self.addCleanup(lambda:transport.close() if hasattr(transport,'close') else None)
  c=BinanceClient('offline-key','offline-secret',transport=transport,clock=lambda:self.clock.wall,monotonic=lambda:self.clock.mono,auto_sync_time=False,timeout=timeout,diagnostics=self.d)
  return c,transport
 def test_slow_checkpoint_crossing_window_never_sends_pool_or_custom_write(self):
  for custom in (False,True):
   c,t=self.client(delay=2,custom=custom);deadline=c.now_ms()+1000
   with self.assertRaises(RequestExpired):c.request('POST','/fapi/v1/order',{'newClientOrderId':'offline-once'},signed=True,_deadline_ms=deadline)
   self.assertEqual(self.pools.calls,[])
   if custom:self.assertEqual(t.requests,[])
   self.assertTrue(self.d.snapshot()['last']['requests'][-1]['definite_not_submitted'])
   self.assertFalse(self.d.snapshot()['last']['requests'][-1]['submission_may_have_occurred'])
 def test_signature_is_fresh_after_checkpoint_and_get_probe_not_reclaimed(self):
  c,t=self.client(delay=6)
  c._network_retry.failed('offline old failure');self.clock.advance(600)
  result=c.account();self.assertEqual(result,{'ok':True})
  self.assertEqual(len(self.pools.calls),1)
  sent=parse_qs(self.pools.calls[0][1].split('?',1)[1]);self.assertEqual(int(sent['timestamp'][0]),6606000)
  self.assertFalse(c.network_status()['waiting'])
 def test_checkpoint_cannot_exceed_transport_time_allowance(self):
  c,t=self.client(timeout=1,delay=2)
  with self.assertRaises(LocalTransportDeferred):c.account()
  self.assertEqual(self.pools.calls,[]);self.assertEqual(t.snapshot()['wire_attempts'],0)
if __name__=='__main__':unittest.main()
