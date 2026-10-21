"""First-run credentials for the shareable distribution; no exchange writes."""
import json
import os
import secrets
from pathlib import Path

SYMBOLS = ('BTCUSDT', 'ETHUSDT')


def read_credentials(root):
    path = Path(root) / '.env'
    try:
        fields = {}
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if '=' not in line or line.lstrip().startswith('#'):
                continue
            key, value = line.split('=', 1)
            key, value = key.strip(), value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            fields[key] = value
        return tuple(fields.get(key, '') for key in ('BINANCE_API_KEY', 'BINANCE_API_SECRET'))
    except (OSError, ValueError):
        return '', ''


def configured(root):
    return all(read_credentials(root))


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.setup.tmp')
    temporary.write_text(text, encoding='utf-8')
    os.replace(temporary, path)


def initialize_credentials(root, key, secret):
    root = Path(root)
    if configured(root):
        raise ValueError('此安装已配置密钥；首次配置不会覆盖已有账户和账本')
    for value in (key, secret):
        if not isinstance(value, str) or not value.strip() or any(c in value for c in '\r\n\x00'):
            raise ValueError('请填写完整API密钥和API Secret，不能包含换行')
    data = root / 'user_data'
    data.mkdir(parents=True, exist_ok=True)
    # Only the first-run, explicitly invoked route creates these local files.
    auth_path = data / 'local-auth.json'
    if not auth_path.exists():
        atomic_write(auth_path, json.dumps({'username': 'hourly-local',
            'password': secrets.token_urlsafe(32)}, ensure_ascii=False))
    atomic_write(root / '.env', 'BINANCE_API_KEY=' + key.strip() + '\nBINANCE_API_SECRET=' + secret.strip() + '\n')


def initialize_files(root, fixed):
    """Configure this strategy without inspecting any previous strategy state."""
    root = Path(root)
    data = root / 'user_data'
    data.mkdir(parents=True, exist_ok=True)
    settings = dict(fixed, migrated=True, version=2, startup_policy='flat',
                    capital_mode='independent-symbol-ledgers', initial_wallet_per_symbol='100')
    atomic_write(data / 'hourly-settings.json', json.dumps(settings, ensure_ascii=False, indent=2))
    controls = data / 'hourly-controls.json'
    if not controls.exists():
        atomic_write(controls, json.dumps({'symbols': {s: False for s in SYMBOLS}, 'version': 2}))
    # Existing books are preserved. A new recipient receives an empty book.
    book = data / 'trades-live.sqlite'
    if not book.exists():
        from execution.store import Store
        Store(book)
    return settings
