"""Copy the server's latest local files into a newly unpacked private bundle.

No Binance access, no interpretation of previous trading states. Original
installation remains intact. SQLite online backups include committed WAL data.
"""
import argparse
import json
import shutil
import sqlite3
from contextlib import closing
from pathlib import Path
from bootstrap import configure


def backup_db(source, destination):
    temporary = destination.with_name(destination.name + '.upgrade.tmp')
    with closing(sqlite3.connect(source.resolve().as_uri() + '?mode=ro', uri=True, timeout=30)) as old:
        with closing(sqlite3.connect(temporary)) as new:
            old.backup(new)
            new.execute('PRAGMA journal_mode=DELETE')
            if new.execute('PRAGMA quick_check').fetchone()[0] != 'ok':
                raise ValueError('服务器账本副本完整性未通过')
    temporary.replace(destination)


def import_state(source_root, target_root):
    source_root, target_root = Path(source_root).resolve(), Path(target_root).resolve()
    if source_root == target_root or target_root.is_relative_to(source_root) or source_root.is_relative_to(target_root):
        raise ValueError('新旧软件必须解压在互不包含的两个独立目录')
    if not (source_root / '.env').is_file() or not (source_root / 'user_data/trades-live.sqlite').is_file():
        raise ValueError('请选择已有软件根目录，必须包含.env和user_data/trades-live.sqlite')
    source_data, target_data = source_root / 'user_data', target_root / 'user_data'
    if not source_data.resolve().is_relative_to(source_root) or not target_data.resolve().is_relative_to(target_root):
        raise ValueError('user_data不能重定向到安装目录以外')
    if not (source_root / '.env').resolve().is_relative_to(source_root) or not (target_root / '.env').resolve().is_relative_to(target_root):
        raise ValueError('密钥文件不能重定向到安装目录以外')
    count, databases = 0, 0
    for old in sorted((source_root / 'user_data').rglob('*')):
        if not old.is_file() or '__pycache__' in old.parts:
            continue
        if old.name in {'process.json', 'hourly-process.json', 'console-process.json'} or old.name.endswith(('-wal', '-shm', '.pyc', '.tmp')):
            continue
        new = target_root / 'user_data' / old.relative_to(source_root / 'user_data')
        if not old.resolve().is_relative_to(source_data.resolve()) or not new.resolve().is_relative_to(target_data.resolve()):
            raise ValueError('文件链接指向user_data以外，未执行该文件复制')
        new.parent.mkdir(parents=True, exist_ok=True)
        with old.open('rb') as file:
            is_db = file.read(16) == b'SQLite format 3\x00'
        if is_db:
            backup_db(old, new)
            databases += 1
        else:
            shutil.copyfile(old, new)
        count += 1
    shutil.copyfile(source_root / '.env', target_root / '.env')
    from control.installation import initialize_files
    from control.hourly_app import FIXED
    initialize_files(target_root, FIXED)
    return {'files_imported': count, 'sqlite_snapshots': databases,
            'original_files_modified': False, 'exchange_requests': 0,
            'trading_started': False, 'source': str(source_root)}


def main():
    root = configure()
    parser = argparse.ArgumentParser(description='合并服务器最新密钥和本地账本；不启动策略')
    parser.add_argument('--from', dest='source', required=True)
    args = parser.parse_args()
    # This is a process/file replacement check, not a trading-history review.
    import socket
    for port in (8788, 8791):
        with socket.socket() as connection:
            connection.settimeout(.2)
            if connection.connect_ex(('127.0.0.1', port)) == 0:
                raise SystemExit('请先结束旧控制台和小时引擎进程，再合并文件；没有发送任何平仓或撤单')
    print(json.dumps(import_state(args.source, root), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
