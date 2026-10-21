import tempfile,unittest
from pathlib import Path
from unittest.mock import Mock
from server.stop_owned_processes import stop_owned


class OwnedStopChecks(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory();self.addCleanup(self.folder.cleanup)
        self.root=Path(self.folder.name).resolve()
        self.process=Mock();self.process.create_time.return_value=123.0
        self.process.cwd.return_value=str(self.root)
        self.process.cmdline.return_value=['python.exe',str(self.root/'hourly_worker.py')]
        self.inspector=lambda root,script:[{'pid':77,'created':123.0,'verified':True}] if script=='hourly_worker.py' else []

    def test_exact_owned_child_is_stopped_and_waited(self):
        self.assertEqual(stop_owned(self.root,inspector=self.inspector,process_factory=lambda pid:self.process),1)
        self.process.terminate.assert_called_once_with()
        self.process.wait.assert_called_once_with(timeout=10)

    def test_reused_pid_is_never_stopped(self):
        self.process.create_time.return_value=124.0
        self.assertEqual(stop_owned(self.root,inspector=self.inspector,process_factory=lambda pid:self.process),0)
        self.process.terminate.assert_not_called()

    def test_changed_script_or_directory_prevents_stop(self):
        for attribute,value in [('cmdline',['python.exe','C:/other/hourly_worker.py']),('cwd',str(self.root.parent))]:
            self.process.cmdline.return_value=['python.exe',str(self.root/'hourly_worker.py')]
            self.process.cwd.return_value=str(self.root)
            getattr(self.process,attribute).return_value=value
            with self.assertRaises(RuntimeError):
                stop_owned(self.root,inspector=self.inspector,process_factory=lambda pid:self.process)
            self.process.terminate.assert_not_called()

    def test_unverified_record_prevents_process_handle_mutation(self):
        inspector=lambda root,script:[{'pid':77,'created':123.0,'verified':False}]
        factory=Mock()
        with self.assertRaises(RuntimeError): stop_owned(self.root,inspector=inspector,process_factory=factory)
        factory.assert_not_called()
