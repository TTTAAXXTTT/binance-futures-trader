"""One public HTTPS read with private proxy settings and errors kept out of output.

No credentials, account endpoints, ledgers, services, trading or settings changes.
Imports are inert. Tests inject a transport and write only temporary reports.
"""
from __future__ import annotations

import argparse
import errno
import http.client
import json
import os
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ENDPOINT = 'https://fapi.binance.com/fapi/v1/time'
HOST = 'fapi.binance.com'
REPORT_RELATIVE = Path('user_data/logs/network-diagnostics.json')

FAILURES = {
    'certificate_verification': ('TLS证书校验失败', '检查Windows信任证书、服务器UTC时间和代理的HTTPS检查证书；由管理员修复信任链后再运行诊断。'),
    'tls_handshake': ('TLS连接协商失败', '核对服务器出站HTTPS和代理TLS设置；保留报告供维护者检查。'),
    'dns_resolution': ('请求连接中的DNS解析失败', '检查实际请求使用的主机或代理的DNS设置；若启用代理，单独的直连DNS结果不能代表代理请求结果。'),
    'timeout': ('公开接口连接或读取超时', '检查服务器出站443、代理可达性及网络延迟，检查后重新运行诊断。'),
    'connection_refused': ('连接被拒绝', '检查出站防火墙规则，以及当前代理服务是否正在监听。'),
    'connection_reset': ('连接被重置或提前断开', '检查网络设备、出站防火墙和代理连接日志，检查后重新运行诊断。'),
    'network_unreachable': ('网络或目标地址不可达', '检查服务器默认路由、出站网络及代理可达性，检查后重新运行诊断。'),
    'proxy_authentication': ('代理要求认证', '在服务器或代理管理界面核对代理认证配置；不要将代理口令放进诊断报告。'),
    'proxy_tunnel': ('代理HTTPS隧道建立失败', '检查代理的CONNECT权限、认证及出站连通性；此结果不是币安接口返回的HTTP状态。'),
    'access_denied': ('公开接口访问被拒绝', '核对服务器出口地址、出站访问策略和服务可用范围；让服务器或代理维护者检查报告。'),
    'restricted_access': ('公开接口返回访问范围限制', '核对服务器出口地址是否处于该服务支持范围，并向服务或服务器维护者确认。'),
    'rate_limited': ('公开接口请求被限流或临时封禁', '停止重复诊断，等待服务端冷却时间后再检查；同一出口的其他程序也可能消耗请求额度。'),
    'redirect_response': ('公开接口返回重定向', '核对出口网络或代理是否返回登录、拦截或重定向页面；本诊断没有跟随重定向。'),
    'upstream_unavailable': ('公开接口或中间网络服务暂不可用', '等待服务恢复，并核对代理或出站网络日志后重新运行诊断。'),
    'http_error': ('公开接口返回HTTP错误', '结合报告中的HTTP状态检查服务器出口、代理和服务状态。'),
    'invalid_response': ('公开接口未返回有效服务器时间', '核对出口网络或代理是否替换了响应内容；报告不会保存响应正文。'),
    'transport_error': ('公开接口网络读取失败', '核对报告中的代理来源、Python/SSL版本和服务器出口网络；将报告交给维护者检查。'),
    'unexpected_failure': ('本地诊断未能完成公开接口检查', '保留这份报告，检查包内Python及依赖完整性后重新运行诊断。'),
}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


def proxy_summary(*, getter=None, environment_getter=None):
    """Describe Python HTTPS proxy selection without returning any proxy value."""
    getter = getter or urllib.request.getproxies
    environment_getter = environment_getter or urllib.request.getproxies_environment
    try:
        proxies, environment = getter(), environment_getter()
        configured = bool(proxies.get('https'))
        if not configured:
            source = 'none'
        elif environment.get('https'):
            source = 'environment'
        else:
            source = 'windows_system_settings' if sys.platform == 'win32' else 'system_settings'
        return {'available': True, 'https_proxy_configured': configured,
                'source': source, 'bypass_rules_configured': bool(proxies.get('no')),
                'other_proxy_configured': any(bool(proxies.get(key)) for key in ('http', 'all', 'ftp')),
                'note': 'HTTPS代理配置不等于一定使用代理；实际请求遵循Python与系统的绕过规则。'}
    except Exception:
        return {'available': False, 'https_proxy_configured': None, 'source': 'unknown',
                'bypass_rules_configured': None, 'other_proxy_configured': None,
                'note': '代理配置摘要读取失败；仍独立执行公开HTTPS请求。'}


def _exception_chain(error):
    seen = set()
    while isinstance(error, BaseException) and id(error) not in seen:
        seen.add(id(error))
        yield error
        reason = getattr(error, 'reason', None)
        error = reason if isinstance(reason, BaseException) else error.__cause__


def failure_category(error):
    """Return fixed categories; never include exception text or response bodies."""
    status, code = getattr(error, 'status', None), getattr(error, 'code', None)
    if status == 407:
        return 'proxy_authentication'
    if status in (418, 429) or code == -1003:
        return 'rate_limited'
    if status == 451:
        return 'restricted_access'
    if status == 403:
        return 'access_denied'
    if isinstance(status, int) and 300 <= status < 400:
        return 'redirect_response'
    if isinstance(status, int) and status >= 500:
        return 'upstream_unavailable'
    if status == 408:
        return 'timeout'
    if isinstance(status, int) and status >= 400:
        return 'http_error'
    for cause in _exception_chain(error):
        if isinstance(cause, ssl.SSLCertVerificationError):
            return 'certificate_verification'
        if isinstance(cause, ssl.SSLError):
            return 'tls_handshake'
        if isinstance(cause, socket.gaierror):
            return 'dns_resolution'
        if isinstance(cause, TimeoutError):
            return 'timeout'
        if isinstance(cause, ConnectionRefusedError):
            return 'connection_refused'
        if isinstance(cause, (ConnectionResetError, ConnectionAbortedError,
                              http.client.RemoteDisconnected, http.client.IncompleteRead)):
            return 'connection_reset'
        if isinstance(cause, (json.JSONDecodeError, UnicodeDecodeError,
                              http.client.BadStatusLine)):
            return 'invalid_response'
        if isinstance(cause, OSError):
            numbers = {value for value in (cause.errno, getattr(cause, 'winerror', None))
                       if isinstance(value, int)}
            if numbers & {errno.ETIMEDOUT, 10060}:
                return 'timeout'
            if numbers & {errno.ECONNREFUSED, 10061}:
                return 'connection_refused'
            if numbers & {errno.ECONNRESET, errno.ECONNABORTED, 10053, 10054}:
                return 'connection_reset'
            if numbers & {errno.ENETUNREACH, errno.EHOSTUNREACH, 10050, 10051, 10065}:
                return 'network_unreachable'
        # urllib wraps failed HTTPS CONNECT in an OSError with no status field.
        # Compare fixed prefixes privately; never return the string itself.
        reason = getattr(cause, 'reason', cause)
        if isinstance(reason, (str, OSError)):
            text = str(reason).lower()
            if 'tunnel connection failed:' in text:
                return 'proxy_authentication' if 'tunnel connection failed: 407' in text else 'proxy_tunnel'
    return 'transport_error'


def direct_dns_summary(*, enabled=False, resolver=None, timeout=2):
    """Optional bounded check; a failure does not change HTTPS request success."""
    note = '仅检查本机直接解析币安主机；使用代理的HTTPS请求可能由代理解析该主机。'
    if not enabled:
        return {'checked': False, 'status': 'not_requested',
                'note': '没有主动单独查询DNS；HTTPS请求本身仍按正常规则解析主机或代理。'}
    result = {}
    resolver = resolver or socket.getaddrinfo

    def inspect():
        try:
            values = resolver(HOST, 443, type=socket.SOCK_STREAM)
            result.update(status='resolved' if values else 'no_addresses')
        except Exception:
            result.update(status='failed')

    thread = threading.Thread(target=inspect, daemon=True, name='diagnostic-direct-dns')
    thread.start()
    thread.join(timeout=timeout)
    return {'checked': True, 'status': result.get('status', 'timeout'), 'note': note}


def run_diagnostics(root, *, transport=None, timeout=12, check_dns=False,
                    proxy_getter=None, environment_proxy_getter=None,
                    resolver=None, clock=time.time):
    """Read the public time once; no files are read and no report is written here."""
    if not 1 <= timeout <= 30:
        raise ValueError('公开GET超时必须为1至30秒')
    from execution import binance
    proxies = proxy_summary(getter=proxy_getter, environment_getter=environment_proxy_getter)
    dns = direct_dns_summary(enabled=check_dns, resolver=resolver)
    wire = transport or urllib.request.build_opener(_NoRedirect()).open
    observed = {}

    def public_transport(request, *, timeout):
        if request.get_method() != 'GET' or request.full_url != ENDPOINT or request.data is not None:
            raise RuntimeError('诊断只允许固定公开时间GET')
        response = wire(request, timeout=timeout)
        status = getattr(response, 'status', None)
        if isinstance(status, int):
            observed['http_status'] = status
        return response

    client = binance.BinanceClient('', '', transport=public_transport, timeout=timeout,
                                  get_retries=0, clock=clock)
    started, before = time.monotonic(), clock()
    try:
        value = client.public('GET', '/fapi/v1/time')
        raw_time = value.get('serverTime') if isinstance(value, dict) else None
        if isinstance(raw_time, bool) or not isinstance(raw_time, (int, str)):
            raise ValueError('无效公开时间字段')
        server_ms = int(raw_time)
        server_utc = datetime.fromtimestamp(server_ms / 1000, timezone.utc).isoformat()
        if server_ms <= 0:
            raise ValueError('无效公开时间字段')
        after = clock()
        result = {'ok': True, 'category': 'success', 'message': '币安公开HTTPS时间接口读取成功',
                  'http_status': observed.get('http_status'), 'server_time_utc': server_utc,
                  'clock_offset_ms': round(server_ms - (before + after) * 500),
                  'next_step': '公开连接正常。若页面仍无法连接账户，请核对账户接口错误分类和本地API配置。'}
    except binance.ApiError as error:
        category = failure_category(error)
        message, step = FAILURES[category]
        result = {'ok': False, 'category': category, 'message': message,
                  'http_status': error.status if isinstance(error.status, int) else observed.get('http_status'),
                  'next_step': step}
        helper = getattr(binance, 'transport_failure_reason', None)
        if helper is not None and error.__cause__ is not None:
            result['reason'] = helper(error.__cause__)
    except (ValueError, OverflowError, OSError):
        message, step = FAILURES['invalid_response']
        result = {'ok': False, 'category': 'invalid_response', 'message': message,
                  'http_status': observed.get('http_status'), 'next_step': step}
    except Exception:
        message, step = FAILURES['unexpected_failure']
        result = {'ok': False, 'category': 'unexpected_failure', 'message': message,
                  'http_status': observed.get('http_status'), 'next_step': step}
    result['elapsed_seconds'] = round(time.monotonic() - started, 3)
    return {'diagnostic': 'binance-public-time', 'checked_at_utc': datetime.fromtimestamp(clock(), timezone.utc).isoformat(),
            'runtime': {'python_version': sys.version.split()[0], 'ssl_version': ssl.OPENSSL_VERSION,
                        'platform': sys.platform},
            'proxy': proxies, 'direct_dns': dns,
            'request': {'method': 'GET', 'endpoint': ENDPOINT, 'signed': False,
                        'attempts': 1, 'timeout_seconds': timeout, 'follow_redirects': False},
            'result': result}


def write_report(root, report):
    root = Path(root).resolve()
    path = root / REPORT_RELATIVE
    if not path.resolve().is_relative_to(root):
        raise ValueError('诊断日志目录不能指向安装目录以外')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + str(os.getpid()) + '.tmp')
    if not temporary.resolve().is_relative_to(root):
        raise ValueError('诊断临时文件不能指向安装目录以外')
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(path)
    return path


def main(argv=None):
    parser = argparse.ArgumentParser(description='仅检查币安公开HTTPS时间接口；不读取API密钥或启动交易')
    parser.add_argument('--timeout', type=int, choices=range(1, 31), default=12, metavar='1..30')
    parser.add_argument('--dns', action='store_true', help='附加独立、最多等待2秒的直接DNS检查')
    args = parser.parse_args(argv)
    from bootstrap import configure
    root = configure()
    report = run_diagnostics(root, timeout=args.timeout, check_dns=args.dns)
    print('检查接口：GET ' + ENDPOINT)
    print('UTC时间：' + report['checked_at_utc'])
    print('Python ' + report['runtime']['python_version'] + '；SSL ' + report['runtime']['ssl_version'])
    proxy = report['proxy']
    print('HTTPS代理已配置：' + ('是' if proxy['https_proxy_configured'] else '否' if proxy['available'] else '未知') + '；来源：' + proxy['source'])
    print('结果：' + report['result']['message'] + ' [' + report['result']['category'] + ']')
    if report['result'].get('reason'):
        print('分类原因：' + report['result']['reason'])
    if args.dns:
        print('独立直连DNS：' + report['direct_dns']['status'] + '；' + report['direct_dns']['note'])
    print('下一步：' + report['result']['next_step'])
    try:
        path = write_report(root, report)
    except (OSError, ValueError):
        print('报告保存失败。请检查本安装user_data/logs目录写入权限，再运行检查币安网络.cmd。')
        return 2
    print('诊断报告：' + str(path))
    print('处理后可重新运行本安装的“检查币安网络.cmd”；报告不会保存代理地址、口令或响应正文。')
    return 0 if report['result']['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
