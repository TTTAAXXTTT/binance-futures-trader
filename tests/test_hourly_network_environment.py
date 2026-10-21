import json
import tempfile
import unittest
from pathlib import Path
from server.network_environment import apply_network_environment


class NetworkOwnerChecks(unittest.TestCase):
    def setUp(self):
        self.folder=tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root=Path(self.folder.name)
        (self.root/'user_data').mkdir()
        self.path=self.root/'user_data/outbound-proxy.json'

    def save(self,value):
        self.path.write_text(json.dumps(value),encoding='utf-8')

    def test_missing_configuration_does_not_replace_user_network(self):
        environment={'https_proxy':'http://existing:80','NO_PROXY':'custom.internal'}
        before=environment.copy()
        self.assertFalse(apply_network_environment(self.root,environment))
        self.assertEqual(environment,before)

    def test_service_and_user_owners_use_same_explicit_http_and_websocket_proxy(self):
        self.save({'version':1,'proxy_url':'http://127.0.0.1:7897'})
        for environment in ({},{'https_proxy':'http://old:80'}):
            self.assertTrue(apply_network_environment(self.root,environment))
            for name in ('http_proxy','https_proxy','ws_proxy','wss_proxy'):
                self.assertEqual(environment[name],'http://127.0.0.1:7897')
                self.assertEqual(environment[name.upper()],environment[name])
            self.assertIn('127.0.0.1',environment['NO_PROXY'])
            self.assertIn('localhost',environment['NO_PROXY'])

    def test_local_rpc_bypass_preserves_existing_exclusions_and_is_idempotent(self):
        self.save({'version':1,'proxy_url':'http://127.0.0.1:7897'})
        environment={'NO_PROXY':'custom.internal,localhost'}
        apply_network_environment(self.root,environment)
        before=environment.copy()
        apply_network_environment(self.root,environment)
        self.assertEqual(environment,before)
        self.assertIn('custom.internal',environment['no_proxy'])

    def test_invalid_configuration_does_not_partly_mutate_environment_or_expose_value(self):
        for proxy in ('http://user:private@127.0.0.1:7897','bad-proxy','http://127.0.0.1:bad','http://127.0.0.1:7897\nprivate','http://127.0.0.1:7897/path'):
            self.save({'version':1,'proxy_url':proxy})
            environment={'UNRELATED':'retained'}
            with self.assertRaises(RuntimeError) as raised:
                apply_network_environment(self.root,environment)
            self.assertEqual(environment,{'UNRELATED':'retained'})
            self.assertNotIn('private',str(raised.exception))

    def test_malformed_json_remains_explicit_error(self):
        self.path.write_text('{',encoding='utf-8')
        with self.assertRaises(RuntimeError):
            apply_network_environment(self.root,{})
