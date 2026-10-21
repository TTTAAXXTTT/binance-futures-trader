"""Native Windows launcher; no virtual environment or paper execution."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path[:0] = [str(ROOT / 'runtime' / 'packages'), str(ROOT / 'freqtrade'), str(ROOT)]
os.chdir(ROOT)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run('control.hourly_app:app', host='127.0.0.1', port=8788, log_level='warning')
