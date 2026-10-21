"""Observe independent operation generations without altering trading state."""
import sys, tempfile, threading, unittest, sqlite3, json
from pathlib import Path
from contextlib import closing
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from execution.diagnostics import ExecutionDiagnostics

class ExecutionOverlapTests(unittest.TestCase):
    def test_wire_checkpoint_retains_business_stage_path(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'diagnostics.sqlite'
            d=ExecutionDiagnostics(path,release='offline-v6')
            with d.operation('tick'):
                with d.stage('entry'):
                    with d.stage('market_intent'):
                        with d.request('POST','/fapi/v1/order',True):
                            d.wire()
                            with closing(sqlite3.connect(path)) as db:
                                pending=json.loads(db.execute('SELECT value FROM operations ORDER BY seq DESC LIMIT 1').fetchone()[0])
                            self.assertEqual(pending['outcome'],'in_progress')
                            self.assertEqual(pending['active_stages'],['entry','market_intent'])
                            self.assertEqual(pending['active_request']['stage_path'],['entry','market_intent'])
                            self.assertTrue(pending['active_request']['submission_may_have_occurred'])
            self.assertEqual(d.snapshot()['last']['outcome'],'complete')

    def test_one_finished_thread_does_not_erase_other_active_operation(self):
        with tempfile.TemporaryDirectory() as folder:
            d=ExecutionDiagnostics(Path(folder)/'diagnostics.sqlite',release='offline-v6')
            started,finish=threading.Event(),threading.Event()
            errors=[]
            def waiting_operation():
                try:
                    with d.operation('tick'):
                        with d.stage('reconcile'):
                            started.set()
                            if not finish.wait(5): raise RuntimeError('fixture timeout')
                except BaseException as exc: errors.append(exc)
            thread=threading.Thread(target=waiting_operation)
            thread.start()
            try:
                self.assertTrue(started.wait(5))
                with d.operation('human_pause'):
                    with d.stage('human_pause'):
                        active=d.snapshot()['active_operations']
                        self.assertEqual({x['phase'] for x in active},{'reconcile','human_pause'})
                snapshot=d.snapshot()
                self.assertEqual(snapshot['current']['phase'],'reconcile')
                self.assertEqual(len(snapshot['active_operations']),1)
            finally:
                finish.set();thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(errors,[])
            self.assertEqual(d.snapshot()['active_operations'],[])
            self.assertIsNone(d.snapshot()['current'])

if __name__=='__main__':unittest.main()
