"""Run offline hourly tests and persist a reviewable verification report."""
import hashlib
import io
import json
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime' / 'packages'), str(ROOT)]


def cleanup_temporary_directory(directory, original_cleanup, *, sleep=time.sleep,
        temporary_root=None, platform=sys.platform, on_retry=None):
    """Retry transient Windows test-file release only; retain cleanup failures."""
    temporary_root = Path(tempfile.gettempdir() if temporary_root is None else temporary_root).resolve()
    target = Path(directory.name).resolve()
    if target == temporary_root or not target.is_relative_to(temporary_root) or target == ROOT or target.is_relative_to(ROOT):
        raise RuntimeError('离线测试清理目标不在当前临时目录内')
    for attempt in range(4):
        try:
            return original_cleanup(directory)
        except OSError as exc:
            if platform != 'win32' or getattr(exc, 'winerror', None) not in {5, 32, 145} or attempt == 3:
                raise
            if on_retry is not None:
                on_retry()
            sleep(.1 * 2**attempt)


def main():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite([
        loader.discover(str(ROOT / 'tests'), pattern='test_hourly_*.py'),
        loader.discover(str(ROOT / 'tests'), pattern='test_stage2_*.py'),
        loader.discover(str(ROOT / 'tests'), pattern='test_stage3_*.py'),
        loader.discover(str(ROOT / 'tests'), pattern='test_regime_backtest.py'),
        loader.discover(str(ROOT / 'tests'), pattern='test_process_supervisor.py'),
    ])
    output = io.StringIO()
    original_cleanup = tempfile.TemporaryDirectory.cleanup
    temporary_root = Path(tempfile.gettempdir()).resolve()
    cleanup_retries = [0]
    def record_cleanup_retry():
        cleanup_retries[0] += 1
    # Test-only scope. A persistent file lock still fails the suite; production
    # database handling and cleanup rules remain unchanged.
    with patch.object(tempfile.TemporaryDirectory, 'cleanup',
            lambda directory: cleanup_temporary_directory(directory, original_cleanup,
                temporary_root=temporary_root, on_retry=record_cleanup_retry)):
        result = unittest.TextTestRunner(stream=output, verbosity=1).run(suite)
    sources = ['execution/status_view.py', 'execution/strategy.py', 'execution/binance.py', 'execution/store.py',
               'execution/engine.py', 'execution/funding.py', 'execution/diagnostics.py', 'hourly_worker.py', 'control/hourly_app.py',
               'control/legacy_reconcile.py', 'control/compat.py',
               'execution/request_budget.py', 'execution/network_retry.py', 'execution/streams.py',
               'frequi/quant/src/App.vue', 'frequi/quant/src/statusReader.mjs', 'frequi/quant/src/equityEditor.mjs',
               'control/installation.py', 'server/upgrade.py',
               'server/network_diagnostics.py', 'server/Check-Network.ps1',
               '检查币安网络.cmd', '网络连接修复说明.zh-CN.md',
               '时间戳修复说明.zh-CN.md',
               'research/regime_backtest.py', 'tools/backtest_regime.py', '新小时策略说明.zh-CN.md',
               'server/process_supervisor.py', 'server/console_service.py', 'server/persist_stop_intent.py',
               'server/Start-Server.ps1', 'server/Stop-Server.ps1', 'server/Install-Autostart.ps1',
               'tests/test_hourly_automatic_recovery.py', 'tests/test_hourly_recovery_transport.py',
               'tests/test_process_supervisor.py', 'tests/test_hourly_lifecycle_scripts.py',
               'tests/test_hourly_request_usage.py', 'tests/test_hourly_snapshot_timestamp.py',
               'tests/test_hourly_monitor_reuse.py', 'tools/verify_hourly.py',
               'server/bootstrap.py','server/network_environment.py','server/stop_owned_processes.py']
    sources = sorted(set(sources) | {p.relative_to(ROOT).as_posix()
        for pattern in ('test_hourly_*.py', 'test_stage2_*.py', 'test_stage3_*.py')
        for p in (ROOT / 'tests').glob(pattern)} |
        {'execution/editable_equity.py', 'execution/equity_adjustments.py', 'execution/entry_candidates.py', 'execution/connection_check.py', 'frequi/quant/src/connectionCheck.mjs',
         'frequi/dist/index.html'})
    report = {'strategy': 'HourlyTrendRange-v1',
              'verified_at': datetime.now(timezone.utc).isoformat(),
              'tests_run': result.testsRun, 'failures': len(result.failures),
              'errors': len(result.errors), 'skipped': len(result.skipped),
              'passed': result.wasSuccessful(),
              'environment': 'mock clients and temporary SQLite; no real orders',
              'temporary_cleanup_retries': cleanup_retries[0],
              'source_sha256': {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in sources},
              'output': output.getvalue()}
    (ROOT / 'reports').mkdir(exist_ok=True)
    target = ROOT / 'reports' / 'hourly-validation.json'
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(output.getvalue().strip())
    print('报告：' + str(target))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
