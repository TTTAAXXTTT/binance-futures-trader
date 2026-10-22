"""Inspection and normal recovery separation, offline only."""
from contextlib import contextmanager
import sys
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]
from execution.connection_check import ConnectionCheck
from execution.engine import Engine
from execution.store import Store
from test_hourly_engine import FakeExchange
from test_hourly_worker import FakeEngine
from hourly_worker import build_app
from fastapi.testclient import TestClient


class FreshStreams:
    def __init__(self): self.reconnects = []
    def snapshot(self):
        return {'connections': {'public': True, 'market': True},
                'symbols': {s: dict(book_live=True, trade_live=True, mark_live=True)
                            for s in ('BTCUSDT','ETHUSDT')}}
    def request_reconnect(self, channels, **kwargs): self.reconnects.append(channels)


class GetClient:
    def __init__(self, wait=None):
        self.calls, self.wait, self.closed = [], wait, False
    @contextmanager
    def manual_connection_check(self, **kwargs):
        evidence = Mock()
        evidence.snapshot.return_value = {'completed': True, 'generation': 0}
        yield evidence
    def sync_time(self): self.calls.append('time'); return 1000
    def account(self):
        self.calls.append('account')
        if self.wait: self.wait.wait(2)
        return {}
    def positions(self): self.calls.append('positions'); return []
    def open_orders(self, symbol): self.calls.append(('open_orders',symbol)); return []
    def open_algos(self, symbol): self.calls.append(('open_algos',symbol)); return []
    def query_order(self, symbol, **kwargs):
        self.calls.append(('order',symbol,kwargs)); return {'status':'NEW','symbol':symbol,'orderId':kwargs.get('order_id'), 'clientOrderId':kwargs.get('client_id')}
    def query_algo(self, **kwargs):
        self.calls.append(('algo',kwargs)); return {'algoStatus':'NEW','symbol':'ETHUSDT','algoId':kwargs.get('algo_id'), 'clientAlgoId':kwargs.get('client_id')}
    def close(self): self.closed=True
    def network_status(self): return {'generation':0, 'recovering':False, 'waiting':False}
    def start(self): raise AssertionError('write')
    def market_order(self, *args, **kwargs): raise AssertionError('write')
    def new_stop(self, *args, **kwargs): raise AssertionError('write')


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.client=GetClient(); self.streams=FreshStreams()
        self.checker=ConnectionCheck(Path(self.tmp.name)/'check.sqlite',lambda:self.client,self.streams)
        self.addCleanup(self.checker.close)
    def finish(self):
        self.checker._thread.join(3)
        self.assertFalse(self.checker._thread.is_alive())
        return self.checker.snapshot()
    def test_complete_uses_fresh_reads_and_keeps_healthy_sockets(self):
        response=self.checker.request(); result=self.finish()
        self.assertEqual(response['connection_check']['id'],result['id'])
        self.assertEqual(result['state'],'complete')
        self.assertEqual(self.client.calls[:3],['time','account','positions'])
        self.assertEqual(self.client.calls[3:],[('open_orders','BTCUSDT'),('open_algos','BTCUSDT'),
                                              ('open_orders','ETHUSDT'),('open_algos','ETHUSDT')])
        self.assertEqual(self.streams.reconnects,[])
        self.assertTrue(self.client.closed)
        self.assertTrue(self.checker.take_notification()['transport']['completed'])
    def test_concurrent_clicks_merge_one_actual_read(self):
        stop=threading.Event(); self.client.wait=stop
        first=self.checker.request()
        replies=[]
        threads=[threading.Thread(target=lambda:replies.append(self.checker.request())) for _ in range(8)]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertTrue(all(r['merged'] for r in replies))
        self.assertTrue(all(r['connection_check']['id']==first['connection_check']['id'] for r in replies))
        stop.set(); self.finish();self.assertEqual(self.client.calls.count('account'),1)
    def test_unknown_regular_and_algo_only_original_ids_are_queried(self):
        self.checker.context_supplier=lambda:{'unknown_orders':[
            {'symbol':'ETHUSDT','kind':'entry','client_id':'original-entry'},
            {'symbol':'ETHUSDT','kind':'stop','client_id':'original-stop','order_id':123}]}
        self.checker.request(); result=self.finish()
        self.assertEqual(result['result']['unknown_orders']['queried'],2)
        self.assertEqual(self.client.calls[7][2]['client_id'],'original-entry')
        self.assertEqual(self.client.calls[8][1]['algo_id'],123)
    def test_timeout_keeps_inflight_coalesced_and_discards_late_success(self):
        # Advance a controlled deadline only after the account request entered.
        # Real sleeps can expire before the worker starts on a busy Windows runner.
        now=[0.0]
        self.checker.monotonic=lambda:now[0]
        self.checker.deadline_seconds=30
        block=threading.Event(); entered=threading.Event()
        def blocked_account():
            self.client.calls.append('account')
            entered.set()
            if not block.wait(30):
                raise TimeoutError('test account request was not released')
            return {}
        self.client.account=blocked_account
        self.addCleanup(block.set)
        response=self.checker.request()
        self.assertTrue(entered.wait(5))
        now[0]=31.0
        self.checker._expire(response['connection_check']['id'])
        self.assertEqual(self.checker.snapshot()['state'],'failed')
        self.assertTrue(self.checker.request()['merged'])
        block.set(); result=self.finish()
        self.assertEqual(result['state'],'failed')
        self.assertIsNone(self.checker.take_notification())
        self.assertEqual(response['connection_check']['id'],result['id'])
    def test_task_result_restores_after_process_replacement(self):
        self.checker.request(); result=self.finish()
        new=ConnectionCheck(self.checker.path,lambda:GetClient(),self.streams)
        new.restore(); self.assertEqual(new.snapshot()['id'],result['id'])
        self.assertEqual(new.snapshot()['state'],'complete')
    def test_pause_and_status_do_not_wait_for_inspection_io(self):
        block=threading.Event();self.client.wait=block
        engine=FakeEngine(); app=build_app(engine,'u','p',connection_checker=self.checker)
        with TestClient(app) as web:
            auth=('u','p')
            first=web.post('/api/check-connection',json={'confirmed':True},auth=auth)
            self.assertEqual(first.status_code,200)
            before=time.monotonic()
            self.assertEqual(web.get('/api/status',auth=auth).status_code,200)
            self.assertEqual(web.post('/api/pause',json={'confirmed':True},auth=auth).status_code,200)
            self.assertLess(time.monotonic()-before,.5)
            self.assertEqual(engine.calls,['pause'])
            block.set(); self.finish()
    def test_auth_and_confirmation_required(self):
        web=TestClient(build_app(FakeEngine(),'u','p',connection_checker=self.checker))
        self.assertEqual(web.post('/api/check-connection',json={'confirmed':True}).status_code,401)
        self.assertEqual(web.post('/api/check-connection',json={},auth=('u','p')).status_code,422)
        self.assertEqual(self.client.calls,[])


class RecoveryNotificationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.client=FakeExchange()
        self.client.network_status=lambda:{'generation':4,'recovering':False,'probing':False}
        self.engine=Engine(self.client,Store(Path(self.tmp.name)/'ledger.sqlite'))
        self.engine.desired_running=True;self.engine.state='waiting'
        self.engine._recovery.update(pending=True,kind='network_read_failure')
        self.engine._recovery_next_probe=self.engine._monotonic()+480
    def notice(self):
        return {'context':{'token':self.engine.connection_check_token()},
                'transport':{'completed':True,'generation':4}}
    def test_only_advance_existing_due_time_without_granting_running(self):
        reason=self.engine.fault_reason; notice=self.notice()
        self.assertTrue(self.engine.accept_connection_check(notice))
        self.assertLessEqual(self.engine._recovery_next_probe,self.engine._monotonic())
        self.assertEqual(self.engine.state,'waiting')
        self.assertEqual(self.engine.fault_reason,reason)
        self.assertEqual(self.client.calls,[])
    def test_manual_pause_cannot_be_restored(self):
        notice=self.notice();self.engine.pause()
        self.assertFalse(self.engine.accept_connection_check(notice))
        self.assertFalse(self.engine.desired_running)
    def test_new_fault_generation_discards_old_success(self):
        notice=self.notice();self.engine._fault_generation+=1
        self.assertFalse(self.engine.accept_connection_check(notice))
    def test_new_network_generation_discards_old_success(self):
        notice=self.notice();self.client.network_status=lambda:{'generation':5}
        self.assertFalse(self.engine.accept_connection_check(notice))
    def test_exchange_cooldown_cannot_be_accelerated(self):
        self.engine._recovery['kind']='exchange_cooldown'
        before=self.engine._recovery_next_probe
        self.assertFalse(self.engine.accept_connection_check(self.notice()))
        self.assertEqual(before,self.engine._recovery_next_probe)

