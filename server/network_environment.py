"""Explicit installation network settings for user and Windows service owners."""
import json
from pathlib import Path
from urllib.parse import urlsplit


def apply_network_environment(root, environment):
    path = Path(root)/'user_data/outbound-proxy.json'
    if not path.is_file():
        return False
    try:
        configuration = json.loads(path.read_text(encoding='utf-8-sig'))
        proxy = configuration['proxy_url']
        if configuration.get('version') != 1 or not isinstance(proxy,str) or any(c.isspace() for c in proxy):
            raise ValueError
        parsed = urlsplit(proxy)
        if parsed.scheme not in {'http','https'} or not parsed.hostname or parsed.username or parsed.password or parsed.path not in ('','/') or parsed.query or parsed.fragment or not parsed.port:
            raise ValueError
    except (OSError,ValueError,KeyError,TypeError):
        raise RuntimeError('本安装的网络代理配置无效，请检查outbound-proxy.json') from None
    # Explicit installation settings win over inherited per-user settings.
    for name in ('http_proxy','https_proxy','ws_proxy','wss_proxy'):
        environment[name] = proxy
        environment[name.upper()] = proxy
    exclusions = [entry.strip() for entry in (environment.get('no_proxy') or environment.get('NO_PROXY') or '').split(',') if entry.strip()]
    exclusions = list(dict.fromkeys(exclusions+['127.0.0.1','localhost','::1']))
    environment['no_proxy'] = environment['NO_PROXY'] = ','.join(exclusions)
    return True
