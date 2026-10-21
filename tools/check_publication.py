"""Check tracked/index files without displaying secret values or file contents."""
from pathlib import Path
import json
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
IGNORED_PARTS = {'.git', '.venv', 'venv', 'runtime', 'node_modules', '__pycache__'}
PATTERNS = {
    'private-key': rb'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----',
    'github-token': rb'(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,})',
    'aws-access-key': rb'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b',
    'credential-url': rb'https?://[^\s/\x22\x27]+:[^\s/\x22\x27]+@',
    'literal-secret': rb'(?i)(?:api[_-]?(?:key|secret)|password|access[_-]?token)\s*[=:]\s*[\x22\x27]([A-Za-z0-9_+/=-]{24,})[\x22\x27]',
    'personal-windows-path': rb'(?i)[A-Z]:[/\\]+Users[/\\]+(?!Public[/\\]|Default[/\\]|<)[A-Za-z0-9_.-]+[/\\]',
}
# Exact synthetic fixtures that verify proxy redaction/rejection, never an
# exemption for arbitrary strings or arbitrary files under tests/.
SYNTHETIC_URLS = {
    'tests/test_hourly_binance.py': {b'https://' + b'user:password@'},
    'tests/test_hourly_network_diagnostics.py': {
        b'https://' + b'secret-user:secret-password@', b'https://' + b'proxy-user:private-password@'},
    'tests/test_hourly_network_environment.py': {b'http://' + b'user:private@'},
    'tests/test_hourly_pool_transport.py': {
        b'http://' + b'offline-user:offline-password@', b'http://' + b'user:private-password@'},
    'tests/test_hourly_rest_retry.py': {b'https://' + b'private-user:private-password@'},
}


def git(*args):
    result = subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, check=False)
    return result


def forbidden(name):
    path = Path(name)
    parts = set(path.parts)
    return (
        bool(parts & IGNORED_PARTS)
        or (path.name.startswith('.env') and path.name != '.env.example')
        or path.suffix.lower() in {'.pem', '.key', '.p12', '.pfx', '.log', '.db', '.sqlite', '.zip', '.7z'}
        or '.sqlite' in path.name
        or (path.parts[0] in {'user_data', 'reports'} and path.name != '.gitkeep')
        or bool(parts & {'patch-backups', 'pending-patches', 'migration-evidence'})
    )


def main():
    result = git('ls-files', '-z', '--cached')
    tracked = result.returncode == 0
    names = sorted(set(n.decode('utf-8') for n in result.stdout.split(b'\0') if n)) if tracked else [
        p.relative_to(ROOT).as_posix() for p in ROOT.rglob('*')
        if p.is_file() and not set(p.relative_to(ROOT).parts) & IGNORED_PARTS
    ]
    findings = []
    for name in names:
        if forbidden(name):
            findings.append({'path': name, 'rule': 'private-or-generated-file'})
            continue
        contents = []
        if tracked:
            indexed = git('show', ':' + name)
            if indexed.returncode:
                findings.append({'path': name, 'rule': 'unreadable-index'})
                continue
            contents.append(indexed.stdout)
        if (ROOT / name).is_file():
            contents.append((ROOT / name).read_bytes())
        for rule, pattern in PATTERNS.items():
            matches = [match.group(0) for data in contents for match in re.finditer(pattern, data)]
            if rule == 'credential-url':
                matches = [value for value in matches if value not in SYNTHETIC_URLS.get(name, set())]
            if matches:
                findings.append({'path': name, 'rule': rule})
    print(json.dumps({'passed': not findings, 'files_checked': len(names),
                      'scope': 'Git index and tracked working files' if tracked else 'source files',
                      'findings': findings}, ensure_ascii=False, indent=2))
    return 1 if findings else 0


if __name__ == '__main__':
    raise SystemExit(main())
