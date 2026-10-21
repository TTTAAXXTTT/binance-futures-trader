"""Freshness evidence distinguishes event and local receive expiry."""
import sys, unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from execution.streams import MarketStreams
class StreamFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.base=1800000000000;self.elapsed=0;self.ready=True
        self.stream=MarketStreams(monotonic=lambda:self.elapsed/1000,
            exchange_clock_ms=lambda:(self.base+self.elapsed,self.base+self.elapsed) if self.ready else None)
    def mark(self,event):
        return self.stream.ingest({'e':'markPriceUpdate','E':event,'s':'ETHUSDT','p':'2500'})
    def status(self):return self.stream.snapshot()['symbols']['ETHUSDT']['freshness']['mark']
    def test_uncalibrated_distinguished_from_no_message(self):
        self.ready=False
        self.assertEqual(self.status()['reason'],'clock_uncalibrated')
        self.ready=True
        self.assertFalse(self.status()['available'])
        self.assertTrue(self.mark(self.base))
        self.assertTrue(self.status()['available'])
    def test_receipt_expiry_without_event_expiry(self):
        self.assertTrue(self.mark(self.base+1000));self.elapsed=3100
        self.assertEqual(self.status()['reasons'],['receive_ttl_expired'])
    def test_event_expiry_while_receipt_is_recent(self):
        self.assertTrue(self.mark(self.base-1000));self.elapsed=2100
        self.assertEqual(self.status()['reasons'],['event_age_expired'])
    def test_all_expiry_reasons_survive_cache_tombstone(self):
        self.assertTrue(self.mark(self.base));self.elapsed=3100
        first=self.status()
        self.assertEqual(set(first['reasons']),{'receive_ttl_expired','event_age_expired'})
        self.assertEqual(self.status(),first)
        self.assertFalse(self.mark(self.base))
        self.assertFalse(self.status()['available'])
        self.assertTrue(self.mark(self.base+self.elapsed))
        self.assertTrue(self.status()['available'])
    def test_independent_symbols_and_channels(self):
        self.assertTrue(self.mark(self.base))
        snapshot=self.stream.snapshot()['symbols']
        self.assertTrue(snapshot['ETHUSDT']['freshness']['mark']['available'])
        self.assertEqual(snapshot['ETHUSDT']['freshness']['book']['reason'],'no_message')
        self.assertEqual(snapshot['BTCUSDT']['freshness']['mark']['reason'],'no_message')
if __name__=='__main__':unittest.main()
