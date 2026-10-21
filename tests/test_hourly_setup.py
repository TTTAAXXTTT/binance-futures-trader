"""First-run isolation and flat-start configuration using temporary files only."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT)]
from control.installation import configured, initialize_credentials, initialize_files
from control.hourly_app import FIXED, app
from fastapi.testclient import TestClient
from execution.store import Store


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_shareable_first_run_creates_its_own_auth_and_empty_book(self):
        self.assertFalse(configured(self.root))
        initialize_credentials(self.root, 'recipient-key', 'recipient-secret')
        initialize_files(self.root, FIXED)
        self.assertTrue(configured(self.root))
        auth = json.loads((self.root / 'user_data/local-auth.json').read_text())
        self.assertGreater(len(auth['password']), 30)
        store = Store(self.root / 'user_data/trades-live.sqlite')
        self.assertEqual(store.trades(), [])
        self.assertEqual(store.orders(), [])
        self.assertEqual(store.meta('symbol_enabled'), None)
        config = json.loads((self.root / 'user_data/hourly-settings.json').read_text())
        self.assertEqual(config['strategy'], 'HourlyTrendRange')
        self.assertEqual(config['startup_policy'], 'flat')
        self.assertEqual(config['initial_wallet_per_symbol'], '100')

    def test_existing_history_and_control_selection_are_preserved(self):
        initialize_files(self.root, FIXED)
        store = Store(self.root / 'user_data/trades-live.sqlite')
        trade = {'id': 'old-example', 'status': 'closed', 'cost': '1.23'}
        store.save_trade(trade)
        controls = self.root / 'user_data/hourly-controls.json'
        controls.write_text(json.dumps({'symbols': {'BTCUSDT': False, 'ETHUSDT': True}}))
        initialize_files(self.root, FIXED)
        self.assertEqual(store.trades(), [trade])
        self.assertTrue(json.loads(controls.read_text())['symbols']['ETHUSDT'])

    def test_invalid_credentials_do_not_create_files_or_replace_existing_credentials(self):
        for key, secret in [('', 'secret'), ('x\ny', 'secret'), ('key', '\x00')]:
            with self.assertRaises(ValueError):
                initialize_credentials(self.root, key, secret)
        self.assertFalse((self.root / '.env').exists())
        initialize_credentials(self.root, 'key-one', 'secret-one')
        original = (self.root / '.env').read_bytes()
        with self.assertRaises(ValueError):
            initialize_credentials(self.root, 'key-two', 'secret-two')
        self.assertEqual((self.root / '.env').read_bytes(), original)

    def test_setup_endpoint_requires_human_action_and_returns_no_secret(self):
        from control import hourly_app as console
        with patch.object(console, 'ROOT', self.root):
            client = TestClient(app)
            fields = {'api_key': 'recipient-key', 'api_secret': 'recipient-secret'}
            self.assertEqual(client.post('/api/setup', json=fields).status_code, 422)
            self.assertFalse((self.root / '.env').exists())
            response = client.post('/api/setup', json=dict(fields, confirmed=True))
            self.assertEqual(response.status_code, 200)
            self.assertNotIn('recipient-key', response.text)
            self.assertNotIn('recipient-secret', response.text)
            self.assertEqual(client.post('/api/setup', json=dict(fields, confirmed=True)).status_code, 409)

    def test_empty_quoted_env_is_not_configured_and_account_client_does_not_latch_it(self):
        from control import compat
        (self.root / '.env').write_text(' BINANCE_API_KEY = ""\nBINANCE_API_SECRET=\'\'\n')
        self.assertFalse(configured(self.root))
        with patch.object(compat, 'ROOT', self.root):
            with self.assertRaises(RuntimeError):
                compat.AccountClient()
            initialize_credentials(self.root, 'recipient-key', 'recipient-secret')
            with patch('execution.binance.BinanceClient') as transport:
                client = compat.AccountClient()
                self.assertEqual(client.key, 'recipient-key')
                transport.assert_called_once_with('recipient-key', 'recipient-secret', timeout=15, get_retries=1)

    def test_shareable_setup_is_decided_by_its_file_not_another_program_environment(self):
        with patch.dict('os.environ', {'BINANCE_API_KEY': 'another-program-key',
                                     'BINANCE_API_SECRET': 'another-program-secret'}):
            self.assertFalse(configured(self.root))
            initialize_credentials(self.root, 'recipient-key', 'recipient-secret')
            self.assertTrue(configured(self.root))


if __name__ == '__main__':
    unittest.main()
