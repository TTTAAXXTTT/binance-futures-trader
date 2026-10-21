"""Installation-scoped process supervision; never calls a trading RPC or API.

The worker restores the existing durable user intent itself. This helper only
rebuilds exited processes and never kills a process, changes a symbol selection,
or writes trading intent. Construction/import does not launch anything.
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from pathlib import Path

VERSION = 'saved-intent-supervision-v1'
INTENT_META = 'trading_intent_v1'
DISABLED_FILE = 'supervision-disabled'


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8-sig'))
    except (OSError, ValueError):
        return default


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.supervisor.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def saved_intent(root):
    """Read the authoritative meta without creating or changing the ledger."""
    ledger = Path(root) / 'user_data/trades-live.sqlite'
    if not ledger.is_file():
        return None
    try:
        with closing(sqlite3.connect(ledger.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            db.execute('PRAGMA query_only=ON')
            row = db.execute('SELECT value FROM hb_meta WHERE key=?', (INTENT_META,)).fetchone()
        value = json.loads(row[0]) if row else None
        return value if (isinstance(value, dict) and value.get('version') == 1
                         and type(value.get('desired_running')) is bool) else None
    except (OSError, ValueError, sqlite3.Error):
        return None


class FileLease:
    """Cross-process byte lock shared by supervised and human worker launches."""
    def __init__(self, path, *, timeout=5):
        self.path, self.timeout, self.file = Path(path), timeout, None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open('a+b')
        self.file.seek(0, os.SEEK_END)
        if self.file.tell() == 0:
            self.file.write(b'0')
            self.file.flush()
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.file.seek(0)
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    self.file.close()
                    self.file = None
                    raise RuntimeError('本安装进程启动锁被占用') from None
                time.sleep(.05)

    def __exit__(self, *_):
        if self.file is not None:
            self.file.seek(0)
            try:
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
            finally:
                self.file.close()
                self.file = None


def launch_lock(root):
    return FileLease(Path(root) / 'user_data/process-launch.lock')


def matching_processes(root, script):
    """Find exact installation/script identities, including unregistered PIDs."""
    import psutil
    root = Path(root).resolve()
    target = str((root / script).resolve()).replace('/', '\\').casefold()
    matches = []
    for process in psutil.process_iter(['pid', 'create_time', 'cmdline', 'cwd']):
        try:
            info = process.info
            args = info.get('cmdline') or []
            exact_script = any(str(arg).replace('/', '\\').casefold() == target for arg in args[1:])
            if exact_script and info.get('cwd') and Path(info['cwd']).resolve() == root:
                matches.append({'pid': info['pid'], 'created': info['create_time'], 'verified': True})
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError, ValueError):
            continue
    record_name = {'hourly_worker.py': 'hourly-process.json',
                   'server/console_service.py': 'console-process.json',
                   'server/process_supervisor.py': 'supervisor-process.json'}.get(script)
    record = read_json(root / 'user_data' / record_name, {}) if record_name else {}
    if isinstance(record, dict) and type(record.get('pid')) is int and not any(
            match['pid'] == record['pid'] for match in matches):
        try:
            process = psutil.Process(record['pid'])
            created = process.create_time()
            if process.is_running() and abs(created - float(record.get('created', -1))) < 1:
                try:
                    args = process.cmdline()
                    exact_script = any(str(arg).replace('/', '\\').casefold() == target for arg in args[1:])
                    if exact_script and Path(process.cwd()).resolve() == root:
                        matches.append({'pid': record['pid'], 'created': created, 'verified': True})
                except (psutil.AccessDenied, OSError):
                    # An existing, unreused registered PID cannot be treated as
                    # absent merely because its command line is inaccessible.
                    matches.append({'pid': record['pid'], 'created': created, 'verified': False})
        except psutil.AccessDenied:
            matches.append({'pid': record['pid'], 'created': record.get('created'), 'verified': False})
        except (psutil.NoSuchProcess, OSError, ValueError, TypeError):
            pass
    return matches


def port_busy(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(.25)
        return probe.connect_ex(('127.0.0.1', int(port))) == 0


def worker_environment(root, parent=None):
    from control.installation import read_credentials
    key, secret = read_credentials(root)
    auth = read_json(Path(root) / 'user_data/local-auth.json', {})
    if not key or not secret or not isinstance(auth, dict) or not auth.get('username') or not auth.get('password'):
        raise RuntimeError('原API或本地RPC配置尚未就绪')
    env = dict(os.environ if parent is None else parent)
    env.pop('PYTHONHOME', None)
    env.pop('PYTHONPATH', None)
    env.update(BINANCE_API_KEY=key, BINANCE_API_SECRET=secret,
               HOURLY_RPC_USER=auth['username'], HOURLY_RPC_PASSWORD=auth['password'])
    return env


def spawn_hidden(root, script, log_name, env=None, *, port=None):
    root = Path(root).resolve()
    target = (root / script).resolve()
    if not target.is_relative_to(root) or not target.is_file():
        raise RuntimeError('监督目标脚本不在本安装目录内')
    logs = root / 'user_data/logs'
    logs.mkdir(parents=True, exist_ok=True)
    with (logs / log_name).open('a', encoding='utf-8') as logfile:
        process = subprocess.Popen([sys.executable, '-E', '-X', 'utf8', str(target)], cwd=root,
            env=env, stdin=subprocess.DEVNULL, stdout=logfile, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    import psutil
    record = {'pid': process.pid, 'created': psutil.Process(process.pid).create_time()}
    if port is not None:
        record['port'] = port
    name = {'hourly_worker.py': 'hourly-process.json',
            'server/console_service.py': 'console-process.json',
            'server/process_supervisor.py': 'supervisor-process.json'}[script]
    atomic_json(root / 'user_data' / name, record)
    return record


class ProcessSupervisor:
    def __init__(self, root, *, inspector=matching_processes, port_probe=port_busy,
                 launcher=spawn_hidden, gate=launch_lock, monotonic=time.monotonic,
                 wall=time.time, parent_environment=None):
        self.root = Path(root).resolve()
        self.inspector, self.port_probe, self.launcher, self.gate = inspector, port_probe, launcher, gate
        self.monotonic, self.wall = monotonic, wall
        self.parent_environment = parent_environment
        self.retry_at, self.statuses = {}, {}

    def check_once(self):
        """One bounded process check, with no account request or control RPC."""
        if (self.root / 'user_data' / DISABLED_FILE).exists():
            return {'enabled': False, 'version': VERSION, 'processes': {}}
        for script, port, log_name in (
                ('server/console_service.py', 8788, 'server-console-launch.log'),
                ('hourly_worker.py', 8791, 'hourly-engine.log')):
            status = {'alive': False, 'launched': False, 'waiting': False}
            try:
                with self.gate(self.root):
                    if (self.root / 'user_data' / DISABLED_FILE).exists():
                        return {'enabled': False, 'version': VERSION, 'processes': {}}
                    found = self.inspector(self.root, script)
                    if found:
                        status.update(alive=True, duplicate=len(found) > 1,
                                      pids=[entry['pid'] for entry in found])
                        if any(entry.get('verified') is False for entry in found):
                            status.update(waiting=True, reason='已有注册进程身份不可读取；未重复启动')
                        self.retry_at.pop(script, None)
                    elif self.port_probe(port):
                        status.update(waiting=True, reason='端口已有未确认服务；未重复启动')
                    elif self.monotonic() < self.retry_at.get(script, 0):
                        status.update(waiting=True, reason='启动失败后等待下次监督检查')
                    else:
                        env = None
                        if script == 'hourly_worker.py':
                            intent = saved_intent(self.root)
                            if intent is None:
                                status.update(waiting=True, reason='没有有效的已保存用户意图；未自动创建交易进程')
                                self.statuses[script] = status
                                continue
                            env = worker_environment(self.root, self.parent_environment)
                        record = self.launcher(self.root, script, log_name, env, port=port)
                        # A PID means launched, not that its RPC is already ready.
                        status.update(launched=True, pid=record['pid'])
                        self.retry_at[script] = self.monotonic() + 30
            except Exception:
                # Error text can contain paths/configuration; do not copy it.
                status.update(waiting=True, reason='进程检查或启动失败；保留原配置等待重试')
                self.retry_at[script] = self.monotonic() + 30
            self.statuses[script] = status
        result = {'enabled': True, 'version': VERSION, 'checked_at_ms': int(self.wall() * 1000),
                  'processes': dict(self.statuses)}
        atomic_json(self.root / 'user_data/supervisor-status.json', result)
        return result


def ensure_supervisor(root):
    """Called by display startup; a shared lock prevents a second supervisor."""
    root = Path(root).resolve()
    if (root / 'user_data' / DISABLED_FILE).exists():
        return False
    with launch_lock(root):
        if (root / 'user_data' / DISABLED_FILE).exists():
            return False
        if matching_processes(root, 'server/process_supervisor.py'):
            return False
        spawn_hidden(root, 'server/process_supervisor.py', 'process-supervisor.log')
    return True


def keep_supervisor_alive(root, stop_event):
    """A display-owned thread can rebuild an exited supervisor, never a worker."""
    while not stop_event.wait(15):
        try:
            ensure_supervisor(root)
        except Exception:
            pass  # Bounded next check; no restart of an unidentified process.


def main():
    from bootstrap import configure
    root = configure()
    stop_event = threading.Event()
    try:
        # Held for this process lifetime, independently of launch locks.
        with FileLease(root / 'user_data/process-supervisor-owner.lock', timeout=0):
            supervisor = ProcessSupervisor(root)
            while not stop_event.is_set() and not (root / 'user_data' / DISABLED_FILE).exists():
                supervisor.check_once()
                stop_event.wait(15)
    except RuntimeError:
        return  # Another exact-installation supervisor already holds the lease.


if __name__ == '__main__':
    main()
