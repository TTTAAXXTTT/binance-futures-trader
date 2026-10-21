"""Offline regression: unreliable headers, real cooldown and minute rollback."""
import io,json,sqlite3,sys,tempfile,unittest
from contextlib import closing
from pathlib import Path
from email.utils import formatdate
from urllib.error import HTTPError
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT));sys.path.insert(0,str(ROOT/'tests'))
from execution.request_budget import RequestBudget,RateLimited,request_weight
from execution.binance import BinanceClient,ApiError
from test_hourly_binance import Response,Transport
BAD=('/fapi/v1/ticker/bookTicker','/fapi/v2/ticker/price')
class BudgetIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.path=Path(self.folder.name)/'budget.sqlite';self.now=6000.1
        self.budget=RequestBudget(self.path,clock=lambda:self.now)
    def test_inaccurate_heads_cannot_raise_weight_or_reset_window(self):
        self.budget.reserve('GET','/fapi/v1/time')
        for path in BAD:
            self.budget.observe({'Date':formatdate(6060,usegmt=True),'X-MBX-USED-WEIGHT-1M':'2000'},path=path)
            self.assertEqual(self.budget.snapshot()['used_weight'],1)
        self.budget.reserve('GET','/fapi/v1/time')
        self.assertEqual(self.budget.snapshot()['used_weight'],2)
    def test_inaccurate_heads_still_enforce_shared_real_cooldown(self):
        for status in (429,418):
            with self.subTest(status=status):
                b=RequestBudget(Path(self.folder.name)/(str(status)+'.sqlite'),clock=lambda:self.now)
                b.reserve('GET','/fapi/v1/time')
                b.observe({'Retry-After':'180','X-MBX-USED-WEIGHT-1M':'2000'},path=BAD[0],status=status,code=-1003)
                self.assertEqual(b.snapshot()['used_weight'],1)
                other=RequestBudget(b.path,clock=lambda:self.now)
                with self.assertRaises(RateLimited):other.reserve('POST','/fapi/v1/order',critical=True)
    def test_trusted_and_default_path_headers_remain_effective(self):
        for path in (None,'/fapi/v3/account','/fapi/v1/ticker/price'):
            self.budget.observe({'X-MBX-USED-WEIGHT-1M':'1195'},path=path,request_minute=int(self.now//60))
            self.assertEqual(self.budget.snapshot()['used_weight'],1195)
            with self.assertRaises(RateLimited):self.budget.reserve('GET','/fapi/v1/openOrders')
    def test_future_exchange_minute_never_rolls_back_or_displays_zero(self):
        self.now=5999.9
        self.budget.observe({'Date':formatdate(6000,usegmt=True),'X-MBX-USED-WEIGHT-1M':'1195'})
        self.assertEqual(self.budget.snapshot()['used_weight'],1195)
        with self.assertRaises(RateLimited) as caught:self.budget.reserve('GET','/fapi/v1/openOrders')
        self.assertEqual(caught.exception.retry_at,6060.5)
        with closing(sqlite3.connect(self.path)) as db:self.assertEqual(db.execute('SELECT minute,used FROM budget').fetchone(),(100,1195))
    def test_future_window_successful_small_reserve_retains_effective_minute(self):
        self.now=5999.9
        self.budget.observe({'Date':formatdate(6000,usegmt=True),'X-MBX-USED-WEIGHT-1M':'10'})
        self.budget.reserve('GET','/fapi/v1/time')
        with closing(sqlite3.connect(self.path)) as db:self.assertEqual(db.execute('SELECT minute,used FROM budget').fetchone(),(100,11))
        self.now=6060.1;self.budget.reserve('GET','/fapi/v1/time')
        self.assertEqual(self.budget.snapshot()['used_weight'],1)
    def test_late_valid_date_and_repeat_headers_do_not_duplicate_or_replace(self):
        self.budget.reserve('GET','/fapi/v1/time')
        self.budget.observe({'Date':formatdate(5999,usegmt=True),'X-MBX-USED-WEIGHT-1M':'2000'})
        self.assertEqual(self.budget.snapshot()['used_weight'],1)
        clients=[RequestBudget(self.path,clock=lambda:self.now) for _ in range(10)]
        for b in clients:b.reserve('GET','/fapi/v3/account')
        for b in clients:b.observe({'Date':formatdate(6000,usegmt=True),'X-MBX-USED-WEIGHT-1M':'51'})
        self.assertEqual(self.budget.snapshot()['used_weight'],51)
    def test_precise_market_weights(self):
        for path,single,all_symbols in (('/fapi/v1/ticker/price',1,2),('/fapi/v2/ticker/price',1,2),('/fapi/v1/ticker/bookTicker',2,5),('/fapi/v1/ticker/24hr',1,40),('/fapi/v1/premiumIndex',1,10)):
            with self.subTest(path=path):
                self.assertEqual(request_weight('GET',path,{'symbol':'ETHUSDT'}),single)
                self.assertEqual(request_weight('GET',path),all_symbols)
    def test_client_success_supplies_endpoint_to_observer(self):
        for path in BAD:
            response=Response({'price':'100'});response.headers={'X-MBX-USED-WEIGHT-1M':'2000'}
            client=BinanceClient('','',transport=Transport(response),budget=self.budget,auto_sync_time=False)
            self.assertEqual(client.public('GET',path,{'symbol':'ETHUSDT'}),{'price':'100'})
        self.assertEqual(self.budget.snapshot()['used_weight'],3)
    def test_client_json_rate_error_supplies_endpoint_and_preserves_cooldown(self):
        response=Response({'code':-1003,'msg':'limited'},429);response.headers={'X-MBX-USED-WEIGHT-1M':'2000','Retry-After':'180'}
        transport=Transport(response);client=BinanceClient('','',transport=transport,budget=self.budget,auto_sync_time=False,clock=lambda:self.now)
        with self.assertRaises(ApiError):client.public('GET',BAD[1],{'symbol':'ETHUSDT'})
        self.assertEqual(self.budget.snapshot()['used_weight'],1)
        self.assertGreater(self.budget.snapshot()['cooldown_seconds'],0)
        self.assertEqual(len(transport.requests),1)
    def test_client_html_rate_error_still_preserves_cooldown(self):
        error=HTTPError('https://offline.invalid',429,'limited',{'X-MBX-USED-WEIGHT-1M':'2000','Retry-After':'180'},io.BytesIO(b'<html>limited</html>'))
        transport=Transport(error);client=BinanceClient('','',transport=transport,budget=self.budget,auto_sync_time=False,clock=lambda:self.now)
        with self.assertRaises(ApiError):client.public('GET',BAD[0],{'symbol':'ETHUSDT'})
        self.assertEqual(self.budget.snapshot()['used_weight'],2)
        self.assertGreater(self.budget.snapshot()['cooldown_seconds'],0)
        self.assertEqual(len(transport.requests),1)
if __name__=='__main__':unittest.main()
