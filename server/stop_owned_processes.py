"""Exact installation process stop fallback for Windows service-owned children."""
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'runtime/packages'),str(ROOT)]
from server.process_supervisor import matching_processes


def stop_owned(root, *, inspector=matching_processes, process_factory=None):
    import psutil
    root=Path(root).resolve()
    scripts=('server/process_supervisor.py','hourly_worker.py','server/console_service.py','run.py','worker.py')
    allowed={str((root/script).resolve()).replace('/','\\').casefold() for script in scripts}
    records={}
    for script in scripts:
        for record in inspector(root,script):
            if record.get('verified') is not True:
                raise RuntimeError('本安装仍有身份不可核实的进程，未按进程名称批量停止')
            records[record['pid']]=record
    factory=psutil.Process if process_factory is None else process_factory
    stopped=0
    for pid,record in records.items():
        try:
            process=factory(pid)
            if abs(process.create_time()-record['created'])>.01:
                continue
            args=process.cmdline()
            if Path(process.cwd()).resolve()!=root or not any(str(arg).replace('/','\\').casefold() in allowed for arg in args[1:]):
                raise RuntimeError('停止前进程身份发生变化，未结束该进程')
            process.terminate()
            process.wait(timeout=10)
            stopped+=1
        except psutil.NoSuchProcess:
            continue
    return stopped


if __name__=='__main__':
    print(json.dumps({'stopped_count':stop_owned(ROOT)}))
