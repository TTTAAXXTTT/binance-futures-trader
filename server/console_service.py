"""Long-lived display service; keeps the installation's process supervisor alive."""
from bootstrap import configure


def main():
    root = configure()
    import sys
    import threading
    logs = root / 'user_data/logs'
    logs.mkdir(parents=True, exist_ok=True)
    with (logs / 'server-console.log').open('a', encoding='utf-8', buffering=1) as logfile:
        original_stdout, original_stderr = sys.stdout, sys.stderr
        sys.stdout = sys.stderr = logfile
        supervisor_stop = threading.Event()
        supervisor_keeper = None
        try:
            from server.process_supervisor import ensure_supervisor, keep_supervisor_alive
            try:
                ensure_supervisor(root)
            except Exception:
                print('后台监督启动待重试；控制台继续运行，请查看监督状态。')
            supervisor_keeper = threading.Thread(target=keep_supervisor_alive,
                args=(root, supervisor_stop), daemon=True, name='installation-supervisor-keeper')
            supervisor_keeper.start()
            import uvicorn
            from control.hourly_app import app
            uvicorn.run(app, host='127.0.0.1', port=8788, log_level='warning', access_log=False)
        finally:
            supervisor_stop.set()
            if supervisor_keeper is not None:
                supervisor_keeper.join(timeout=2)
            sys.stdout, sys.stderr = original_stdout, original_stderr


if __name__ == '__main__':
    main()
