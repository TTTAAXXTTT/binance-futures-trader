"""Offline checks of packaged dependencies, credentials and preserved books.

No account requests, service launch, trading mutations or writable source DBs.
Credential values and fingerprints are never printed.
"""
import hashlib
import importlib
import json
import socket
import sqlite3
import struct
import sys
from contextlib import closing
from bootstrap import configure


def main():
    root = configure()
    checks = []
    def check(name, operation):
        try:
            passed = operation() is not False
        except Exception:
            passed = False
        checks.append({'check': name, 'passed': passed})
    check('Python 3.12 / Windows x64', lambda: sys.platform == 'win32'
        and sys.version_info[:2] == (3, 12) and struct.calcsize('P') == 8)
    if (root / 'runtime/python/python.exe').is_file():
        check('解释器已从包内启动', lambda: __import__('pathlib').Path(sys.prefix).resolve()
            == (root / 'runtime/python').resolve())
    for module in ['ssl', 'sqlite3', 'numpy', 'pandas', 'psutil', 'fastapi', 'uvicorn', 'websockets.sync.client']:
        check('依赖 ' + module, lambda module=module: importlib.import_module(module))
    def bundled_dependencies():
        from pathlib import Path
        packages = (root / 'runtime/packages').resolve()
        return all(Path(importlib.import_module(name).__file__).resolve().is_relative_to(packages)
            for name in ['numpy', 'pandas', 'psutil', 'fastapi', 'uvicorn', 'websockets'])
    if (root / 'runtime/packages').is_dir():
        check('第三方依赖来自包内', bundled_dependencies)
    check('已构建中文页面', lambda: (root / 'frequi/dist/index.html').is_file())
    from control.installation import read_credentials
    key, secret = read_credentials(root)
    configured = bool(key and secret)
    def local_auth():
        auth = json.loads((root / 'user_data/local-auth.json').read_text(encoding='utf-8'))
        return bool(auth.get('username') and auth.get('password'))
    if configured:
        check('已有本地RPC凭据', local_auth)
    def verify_db(relative):
        path = (root / relative).resolve()
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('PRAGMA query_only=ON')
            return db.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
    if (root / 'user_data/trades-live.sqlite').exists():
        check('本地账本文件完整性', lambda: verify_db('user_data/trades-live.sqlite'))
    port_state = {}
    for port in [8788, 8791, 8790, 8766]:
        with socket.socket() as connection:
            connection.settimeout(.2)
            port_state[str(port)] = '已有监听，请核对已有程序' if connection.connect_ex(('127.0.0.1', port)) == 0 else '空闲'
    passed = all(item['passed'] for item in checks)
    print(json.dumps({'offline': True, 'passed': passed, 'setup_required': not configured,
                     'checks': checks, 'ports': port_state},
                     ensure_ascii=False, indent=2))
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
