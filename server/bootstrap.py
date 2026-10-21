"""Locate packaged code and evidence without opening an account or ledger."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def configure():
    os.chdir(ROOT)
    # Plain path insertion does not execute the old editable-install .pth files.
    sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT / 'freqtrade'), str(ROOT)]
    from server.network_environment import apply_network_environment
    apply_network_environment(ROOT, os.environ)
    os.environ['PYTHONUTF8'] = '1'
    evidence = ROOT / 'server/migration-evidence/quantpilot'
    if evidence.is_dir():
        os.environ.setdefault('TRADER_LEGACY_ROOT', str(evidence))
    return ROOT
