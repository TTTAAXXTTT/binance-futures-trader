"""Local console for the fixed hourly strategy.

Monitoring is GET-only. User intent is separate from technical recovery; process
supervision can rebuild exited services without replacing a saved manual pause.
Legacy positions remain owned by their original engine until flat and stopped.
"""
import asyncio
import base64
import copy
import json
import math
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from control import compat
from execution.request_budget import RequestBudget, NETWORK_RETRY_SECONDS

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'user_data'
SETTINGS = DATA / 'hourly-settings.json'
CONTROLS = DATA / 'hourly-controls.json'
PROCESS = DATA / 'hourly-process.json'
LEGACY_PROCESS = DATA / 'process.json'
LEDGER = DATA / 'trades-live.sqlite'
SYMBOLS = ['BTCUSDT', 'ETHUSDT']
STRATEGY = 'HourlyTrendRange'
WORKER_STATES = {'stopped', 'paused', 'running', 'fault', 'waiting', 'recovering'}
DEFERRED_START_KINDS = {'local_budget_wait', 'exchange_cooldown', 'network_read_failure',
                        'network_read_wait', 'timestamp_rejected'}
LOCK = threading.RLock()
ACTION_LOCK = threading.Lock()
MONITOR_LOCK = threading.RLock()
MONITOR_CACHE = {'configuration': None, 'configuration_at': None,
                 'last_full_attempt_at': None, 'client_id': None}
WORKER_ACCOUNT_MAX_AGE = 45
MONITOR_CONFIGURATION_TTL = 300
MONITOR_FULL_READ_INTERVAL = 60
CONNECTION_CHECK_RPC_TIMEOUT = 4
CONNECTION_CHECK_STATES = {'accepted', 'checking', 'waiting', 'complete', 'failed', 'interrupted'}
FIXED = {
    'strategy': STRATEGY, 'symbols': SYMBOLS, 'timeframe': '1h', 'leverage': 50,
    'margin_mode': 'cross', 'position_mode': 'one-way', 'asset_mode': 'single-asset',
    'ema_period': 50, 'exit_ema_period': 12, 'breakout_lookback': 12, 'atr_period': 14,
    'adx_period': 14, 'adx_threshold': 20, 'rsi_period': 14,
    'boll_period': 30, 'boll_std': 1.5, 'boll_ddof': 0,
    'range_long_rsi': 45, 'range_short_rsi': 55, 'range_min_distance': .002,
    'initial_stop_atr': 5, 'trailing_stop_atr': 6, 'risk_fraction': .50,
    'notional_multiple': 15, 'warmup_candles': 1000, 'entry_window_seconds': 300,
    'execution_timeframe': '5m', 'initial_wallet_per_symbol': 100,
    'capital_mode': 'independent-symbol-ledgers', 'startup_policy': 'flat',
    'taker_fee': .0005, 'slippage': .0002, 'max_positions_per_symbol': 1,
    'fixed_take_profit': None, 'cooldown': None, 'position_adjustment': False,
}
STATE = {'updated_at': None, 'account': {'connected': False, 'error': '正在读取实盘账户'},
         'worker': {'connected': False, 'state': 'stopped', 'trades': [], 'history': []},
         'legacy_worker': {'connected': False, 'state': 'stopped', 'trades': [], 'history': []},
         'old_service': {}, 'markets': {}, 'events': [], 'blockers': []}


def now_text():
    return datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')


def event(message):
    with LOCK:
        STATE['events'].insert(0, {'time': now_text(), 'message': message})
        del STATE['events'][100:]


def applied_settings():
    settings = compat.read_json(SETTINGS, {})
    return bool(isinstance(settings, dict) and settings.get('strategy') == STRATEGY
                and settings.get('migrated') is True)


def parse_symbols(value):
    if not isinstance(value, dict) or set(value) != set(SYMBOLS) or any(type(v) is not bool for v in value.values()):
        raise HTTPException(422, '请分别设置BTC和ETH是否启用，值必须为true或false')
    return dict(value)


def trade_controls():
    saved = compat.read_json(CONTROLS, {})
    try:
        symbols = parse_symbols(saved.get('symbols'))
    except (HTTPException, AttributeError):
        symbols = {symbol: False for symbol in SYMBOLS}
    return {'symbols': symbols, 'desired_running': saved_user_intent().get('desired_running', False)}


def saved_user_intent():
    from server.process_supervisor import saved_intent
    return saved_intent(ROOT) or {'version': 1, 'desired_running': False}


def persist_absent_worker_pause():
    """A human stop remains durable even if the worker has already exited."""
    if not LEDGER.is_file():
        return
    value = {'version': 1, 'desired_running': False,
             'updated_at_ms': int(time.time() * 1000), 'source': 'human_pause'}
    with closing(sqlite3.connect(LEDGER.resolve().as_uri() + '?mode=rw', uri=True, timeout=5)) as db:
        with db:
            db.execute('INSERT INTO hb_meta(key,value) VALUES(?,?) '
                       'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                       ('trading_intent_v1', json.dumps(value, ensure_ascii=False)))
            # A previously queued Start must not survive a confirmed human stop.
            db.execute('DELETE FROM hb_meta WHERE key=?', ('trading_start_request_v1',))


def effective_symbols(worker, symbols=None):
    """Use the human request, then the connected engine's durable selection."""
    if symbols is not None:
        return parse_symbols(symbols)
    if worker.get('connected'):
        try:
            return parse_symbols(worker.get('symbol_enabled'))
        except HTTPException:
            return {symbol: False for symbol in SYMBOLS}
    return trade_controls()['symbols']


def inspect_process(path, script):
    """Verify PID reuse and exact script identity; never kill by a port number."""
    record = compat.read_json(path, {})
    if not isinstance(record, dict) or not record.get('pid'):
        return {'alive': False, 'verified': True, 'process': None}
    try:
        import psutil
        process = psutil.Process(int(record['pid']))
        if not process.is_running():
            return {'alive': False, 'verified': True, 'process': None}
        if abs(process.create_time() - float(record['created'])) >= 1:
            return {'alive': False, 'verified': True, 'process': None}
        target = str((ROOT / script).resolve()).replace('/', '\\').casefold()
        args = process.cmdline()
        matching = any(str(arg).replace('/', '\\').casefold() == target for arg in args[1:])
        if script == 'worker.py':
            configured = any(args[i] == '--config' and i + 1 < len(args)
                and str(args[i + 1]).replace('/', '\\').casefold()
                == str((DATA / 'live-config.json').resolve()).replace('/', '\\').casefold()
                for i in range(len(args)))
            matching = matching and configured
        cwd_matches = Path(process.cwd()).resolve() == ROOT.resolve()
        return {'alive': True, 'verified': matching and cwd_matches, 'process': process}
    except Exception as exc:
        import psutil
        if isinstance(exc, psutil.NoSuchProcess):
            return {'alive': False, 'verified': True, 'process': None}
        return {'alive': True, 'verified': False, 'process': None}


def legacy_ledger_snapshot():
    """Read the existing Freqtrade book without opening a writable connection."""
    if not LEDGER.exists():
        return {'verified': True, 'open_trades': 0}
    try:
        with closing(sqlite3.connect(LEDGER.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            db.execute('PRAGMA query_only=ON')
            present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trades'").fetchone()
            count = db.execute('SELECT COUNT(*) FROM trades WHERE is_open=1').fetchone()[0] if present else 0
            history = []
            if present:
                db.row_factory = sqlite3.Row
                columns = {row[1] for row in db.execute('PRAGMA table_info(trades)')}
                projection = [name for name in ['id', 'pair', 'is_short', 'stake_amount', 'leverage',
                    'close_profit_abs', 'exit_reason', 'is_open'] if name in columns]
                if 'id' in projection:
                    history = [dict(row) for row in db.execute(
                        'SELECT ' + ','.join(projection) + ' FROM trades ORDER BY id DESC LIMIT 100')]
                    for trade in history:
                        trade['trade_id'] = trade.pop('id')
                        trade['profit_abs'] = trade.pop('close_profit_abs', None)
        return {'verified': True, 'open_trades': int(count), 'history': history}
    except (OSError, sqlite3.Error, ValueError):
        return {'verified': False, 'open_trades': None}


def rpc_auth():
    auth = compat.read_json(DATA / 'local-auth.json', {})
    if not isinstance(auth, dict) or not auth.get('username') or not auth.get('password'):
        raise RuntimeError('新引擎本地连接尚未初始化')
    return auth


def hourly_rpc(path, method='GET', payload=None, timeout=8):
    if method not in {'GET', 'POST'}:
        raise ValueError('unsupported local RPC method')
    auth = rpc_auth()
    token = base64.b64encode((auth['username'] + ':' + auth['password']).encode()).decode()
    request = urllib.request.Request('http://127.0.0.1:8791/api/' + path,
        data=json.dumps(payload or {}).encode() if method == 'POST' else None,
        method=method, headers={'Content-Type': 'application/json', 'Authorization': 'Basic ' + token})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        try:
            detail = json.loads(error.read()).get('detail', '新策略引擎拒绝操作')
        except Exception:
            detail = '新策略引擎拒绝操作'
        raise HTTPException(error.code, detail) from None


def worker_snapshot():
    try:
        result = hourly_rpc('status')
        if not isinstance(result, dict) or result.get('strategy') != STRATEGY:
            raise ValueError('unverified strategy')
        if result.get('state') not in WORKER_STATES:
            raise ValueError('unverified state')
        if not isinstance(result.get('trades'), list) or not isinstance(result.get('unknown_orders', []), list):
            raise ValueError('unverified ledger')
        return dict(result, connected=True)
    except Exception:
        process = inspect_process(PROCESS, 'hourly_worker.py')
        return {'connected': False, 'state': 'unknown' if process['alive'] else 'stopped',
                'process_alive': process['alive'], 'trades': [], 'history': [],
                'desired_running': saved_user_intent().get('desired_running', False),
                'unknown_orders': [], 'fault_reason': '新策略进程或账本待核对' if process['alive'] else None}


def legacy_worker_snapshot():
    result = compat.worker_snapshot()
    process = inspect_process(LEGACY_PROCESS, 'worker.py')
    # compat retains its last connected snapshot on an API outage. Once this
    # controller verifies the recorded process has exited, that cache must not
    # be mistaken for a live engine forever.
    if not result.get('connected') and not process['alive'] and process['verified']:
        result = {'connected': False, 'state': 'stopped', 'trades': [], 'history': []}
    return dict(result, process_alive=process['alive'], process_verified=process['verified'])


def quant_reasons(old):
    if not old.get('present'):
        return []
    reasons = []
    if old.get('raw_running'):
        reasons.append('原 QuantPilot 仍有执行活动')
    if old.get('unknown') or old.get('process_verified') is False:
        reasons.append('原 QuantPilot 停机账本尚未核对')
    if old.get('position') or old.get('pending') or old.get('unfinished') or old.get('pending_flow'):
        reasons.append('原 QuantPilot 仍有未完成仓位、委托或记账')
    if old.get('halted') and not old.get('accepted_for_new_strategy'):
        reasons.append('原 QuantPilot 保留的锁定决定尚未核对')
    return reasons


def migration_reasons(account, legacy_worker, old, book):
    reasons = []
    if not account.get('connected'):
        reasons.append('实盘账户尚未连接')
    else:
        if account.get('positions'):
            reasons.append('账户仍有实际持仓，旧仓不能迁入新策略')
        if account.get('open_orders') or account.get('open_algo_orders'):
            reasons.append('账户仍有普通订单或条件单')
    if legacy_worker.get('state') == 'unknown' or not legacy_worker.get('process_verified', True):
        reasons.append('旧交易进程身份或状态待核对')
    if legacy_worker.get('connected'):
        if legacy_worker.get('state') == 'running' and legacy_worker.get('max_open_trades') != 0:
            reasons.append('请先暂停旧策略新开仓')
        if legacy_worker.get('trades'):
            reasons.append('旧引擎仍有登记持仓')
    elif legacy_worker.get('process_alive'):
        reasons.append('旧交易进程存在但 API 未就绪')
    if not book.get('verified'):
        reasons.append('旧实盘账本尚未核对')
    elif book.get('open_trades'):
        reasons.append('旧实盘账本仍有未平交易')
    reasons.extend(quant_reasons(old))
    return list(dict.fromkeys(reasons))


def start_reasons(account, worker, legacy_worker, old, symbols=None):
    reasons = []
    budget = RequestBudget(DATA / 'binance-request-budget.sqlite').snapshot()
    if budget['cooldown_seconds']:
        reasons.append('币安接口正在限流冷却，约' + str(budget['cooldown_seconds']) + '秒后重新核对')
    network = budget.get('network_retry') or {}
    if network.get('probing'):
        reasons.append('正在重新读取币安接口，连接确认前暂停新开仓')
    elif network.get('waiting'):
        reasons.append('币安连接失败，每10分钟自动重试；约' +
            str(max(0, int(network.get('retry_in_seconds', 0)))) + '秒后重新读取')
    selected = effective_symbols(worker, symbols)
    if not account.get('connected'):
        reasons.append('实盘账户尚未连接')
    else:
        if account.get('can_trade') is not True:
            reasons.append('币安账户未允许合约 API 交易')
        if account.get('hedge_mode') is not False:
            reasons.append('账户必须为单向持仓模式')
        if account.get('multi_asset') is not False:
            reasons.append('账户必须为单资产保证金模式')
        if account.get('equity_usdt', 0) <= 0:
            reasons.append('账户权益必须大于零')
        for symbol in [s for s in SYMBOLS if selected.get(s)]:
            if account.get('leverage_limits', {}).get(symbol, 0) < 50:
                reasons.append(symbol + ' 的适用最高杠杆不足50倍')
        if not worker.get('connected') and (account.get('positions') or account.get('open_orders') or account.get('open_algo_orders')):
            reasons.append('账户存在仓位或订单，请先恢复并核对新策略账本')
    unfinished = bool(worker.get('unpriced_fee') or any(
        trade.get('exit_intent') or trade.get('unpriced_fee') for trade in worker.get('trades', [])))
    recoverable_fault = (worker.get('state') == 'fault' and worker.get('reconciled') is True
                         and not worker.get('unknown_orders') and not unfinished)
    if (worker.get('state') == 'unknown'
            or (worker.get('state') == 'fault' and not recoverable_fault)
            or worker.get('unknown_orders')):
        reasons.append(worker.get('fault_reason') or '订单或持仓状态尚未确定')
    if unfinished:
        reasons.append('仍有未完成退出或手续费核对，暂不能恢复新开仓')
    if worker.get('connected'):
        markets = worker.get('markets')
        markets = markets if isinstance(markets, dict) else {}
        for symbol in [s for s in SYMBOLS if selected.get(s)]:
            market = markets.get(symbol)
            if not isinstance(market, dict) or market.get('ready') is not True:
                reasons.append(symbol + ' 指标尚未完成1,000根完整小时线预热')
    if worker.get('connected') and worker.get('reconciled') is False:
        reasons.append(worker.get('reconciliation_reason') or '真实仓位、订单与成交费用尚未完成核对')
    return list(dict.fromkeys(reasons))


def deferred_start_reasons(current):
    """Only accept a human Start request; execution permission stays in Engine."""
    reasons = []
    if not applied_settings():
        reasons.append('尚未完成策略迁移，不能保存待连接启动请求')
    if not LEDGER.is_file():
        reasons.append('原实盘账本不存在，不能保存启动请求')
    worker, legacy = current.get('worker', {}), current.get('legacy_worker', {})
    if worker.get('state') == 'unknown' or (worker.get('process_alive') and not worker.get('connected')):
        reasons.append('交易进程身份或状态未确认，未保存启动请求')
    if worker.get('unknown_orders'):
        reasons.append('仍有未确定订单，先核对原订单状态')
    if worker.get('unpriced_fee') or any(t.get('exit_intent') or t.get('unpriced_fee')
                                       for t in worker.get('trades', [])):
        reasons.append('仍有未完成退出或成交费用核对')
    if (legacy.get('connected') or legacy.get('process_alive') or legacy.get('state') == 'unknown'
            or legacy.get('process_verified') is False or legacy.get('trades')):
        reasons.append('旧交易引擎仍存在或状态未确定，不能保存待连接启动请求')
    book = current.get('legacy_book', {})
    if book.get('verified') is False or book.get('open_trades'):
        reasons.append('旧账本仍有未核对持仓，不能保存待连接启动请求')
    reasons.extend(quant_reasons(current.get('old_service', {})))
    from server.process_supervisor import matching_processes, port_busy
    identity = inspect_process(LEGACY_PROCESS, 'worker.py')
    if identity['alive'] or matching_processes(ROOT, 'worker.py') or port_busy(8790) or port_busy(8766):
        reasons.append('旧执行进程或端口仍存在，先核对后再保存启动请求')
    return list(dict.fromkeys(reasons))


def start_request_reasons(current, symbols):
    account = current.get('account', {})
    if not account.get('connected') and account.get('recovery_kind') in DEFERRED_START_KINDS:
        return deferred_start_reasons(current)
    return start_reasons(account, current.get('worker', {}), current.get('legacy_worker', {}),
                         current.get('old_service', {}), symbols)


def read_private_account(client):
    """Always issue the real account GET sweep; user Start never uses a cache."""
    try:
        account = compat.read_account(client)
        account.update(source='console_direct', observed_at_ms=int(time.time() * 1000),
                       age_seconds=0, configuration_age_seconds=0)
    except Exception as exc:
        from execution.binance import ApiError
        account = copy.deepcopy(STATE.get('account', {}))
        account.update(connected=False, stale=True,
            error=str(exc) if isinstance(exc, RuntimeError) else '账户读取失败，请检查网络',
            recovery_kind=exc.recovery_kind if isinstance(exc, ApiError) else None,
            retry_at_ms=exc.retry_at_ms if isinstance(exc, ApiError) else None,
            retry_in_seconds=exc.retry_in_seconds if isinstance(exc, ApiError) else 0)
    return account


def publish_private_snapshot(account, worker, control_revision):
    legacy_worker = {'connected': False, 'state': 'stopped', 'process_alive': False,
                     'process_verified': True, 'trades': [], 'history': []}
    old = {'present': False}
    book = {'verified': True, 'open_trades': 0, 'history': []}
    with LOCK:
        if control_revision != STATE.get('control_revision', 0):
            worker = copy.deepcopy(STATE['worker'])
        else:
            STATE['worker_observed_monotonic'] = time.monotonic()
        if account.get('source') not in {'console_cached', 'worker_previous_reconcile'}:
            STATE['account_observed_monotonic'] = time.monotonic()
        STATE.update(account=account, worker=worker, legacy_worker=legacy_worker, old_service=old,
                     legacy_book=book, updated_at=now_text(),
                     blockers=start_reasons(account, worker, legacy_worker, old))


def refresh_private(client):
    account = read_private_account(client)
    # A monitor read may overlap an acknowledged human control. Never publish
    # the older worker read over that confirmation; account polling stays free
    # of the action lock so stopping entries does not wait for Binance reads.
    with LOCK:
        control_revision = STATE.get('control_revision', 0)
    worker = worker_snapshot()
    publish_private_snapshot(account, worker, control_revision)


def worker_account_view(worker, configuration, configuration_age, *, rpc_elapsed=0):
    """Use only a complete, fresh reconciliation, never its connected flag alone."""
    if (worker.get('connected') is not True or worker.get('reconciled') is not True
            or (worker.get('snapshot_metadata') or {}).get('publication_error')
            or (worker.get('snapshot_metadata') or {}).get('execution_busy')
            or worker.get('state') not in {'running', 'paused', 'stopped'}
            or worker.get('fault_reason') or worker.get('unknown_orders')
            or worker.get('loop_error') or (worker.get('execution_recovery') or {}).get('pending')
            or (worker.get('funding_read') or {}).get('pending_recovery')
            or (worker.get('network_retry') or {}).get('waiting')
            or (worker.get('network_retry') or {}).get('probing')
            or not isinstance(configuration, dict)
            or any(type(configuration.get(key)) is not bool for key in ('can_trade', 'hedge_mode', 'multi_asset'))
            or type(configuration_age) not in {int, float} or not math.isfinite(configuration_age)
            or not 0 <= configuration_age <= MONITOR_CONFIGURATION_TTL):
        return None
    snapshot = worker.get('account_snapshot')
    if not isinstance(snapshot, dict):
        return None
    age, observed = snapshot.get('age_seconds'), snapshot.get('reconciled_at_ms')
    if (type(age) not in {int, float} or not math.isfinite(age)
            or not 0 <= age <= WORKER_ACCOUNT_MAX_AGE
            or type(rpc_elapsed) not in {int, float} or not math.isfinite(rpc_elapsed)
            or rpc_elapsed < 0 or age + rpc_elapsed > WORKER_ACCOUNT_MAX_AGE
            or type(observed) is not int or observed <= 0
            or type(worker.get('last_successful_reconcile_ms')) is not int
            or observed != worker.get('last_successful_reconcile_ms')):
        return None
    raw, positions = snapshot.get('account'), snapshot.get('positions')
    orders, algos = snapshot.get('open_orders'), snapshot.get('open_algo_orders')
    if (not isinstance(raw, dict) or not isinstance(positions, list)
            or not isinstance(orders, list) or not isinstance(algos, list)):
        return None
    try:
        def number(value):
            result = float(value)
            if isinstance(value, bool) or not math.isfinite(result):
                raise ValueError('invalid account number')
            return result
        active = []
        for row in positions:
            symbol, quantity = row['symbol'], number(row['positionAmt'])
            if not isinstance(symbol, str) or not symbol:
                return None
            if quantity:
                active.append({'symbol': symbol, 'quantity': quantity,
                    'entry_price': number(row['entryPrice']),
                    'unrealized': number(row.get('unRealizedProfit', 0)),
                    'side': row.get('positionSide')})
        order_ids = [{'symbol': row['symbol'], 'id': str(row['orderId'])} for row in orders]
        order_ids.extend({'symbol': row['symbol'], 'id': str(row['algoId'])} for row in algos)
        if any(not isinstance(row['symbol'], str) or not row['symbol']
               or row['id'] in {'None', ''} for row in order_ids):
            return None
        result = copy.deepcopy(configuration)
        result.update(connected=True, stale=False, error='', recovery_kind=None,
            available_usdt=number(raw['availableBalance']), equity_usdt=number(raw['totalMarginBalance']),
            wallet_usdt=number(raw['totalWalletBalance']), unrealized_usdt=number(raw['totalUnrealizedProfit']),
            positions=active, open_orders=len(orders), open_algo_orders=len(algos), order_ids=order_ids,
            source='worker_reconcile', observed_at_ms=observed, age_seconds=float(age + rpc_elapsed),
            configuration_age_seconds=configuration_age)
        return result
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


def refresh_monitor_private(client):
    """Display-only reuse/serialization; fresh() deliberately does not call this."""
    with MONITOR_LOCK:
        now = time.monotonic()
        if MONITOR_CACHE.get('client_id') != id(client):
            MONITOR_CACHE.update(configuration=None, configuration_at=None,
                                 last_full_attempt_at=None, client_id=id(client))
        with LOCK:
            revision = STATE.get('control_revision', 0)
        worker = worker_snapshot()
        fetched_at = time.monotonic()
        metadata = worker.get('snapshot_metadata') or {}
        if metadata.get('execution_busy') or metadata.get('publication_error'):
            # A status read succeeds during slow execution, but it is not a new
            # account reconciliation. Avoid a competing REST sweep from display.
            account = copy.deepcopy(STATE.get('account', {}))
            account.update(stale=True, source='worker_previous_reconcile',
                error='执行核对尚未完成，显示上次已确认账户读取')
            publish_private_snapshot(account, worker, revision)
            return
        cfg_at = MONITOR_CACHE.get('configuration_at')
        cfg_age = fetched_at - cfg_at if cfg_at is not None else float('inf')
        account = worker_account_view(worker, MONITOR_CACHE.get('configuration'), cfg_age,
                                      rpc_elapsed=max(0, fetched_at - now))
        if account is None:
            last_attempt = MONITOR_CACHE.get('last_full_attempt_at')
            if last_attempt is not None and 0 <= fetched_at - last_attempt < MONITOR_FULL_READ_INTERVAL:
                account = copy.deepcopy(STATE.get('account', {}))
                account.update(connected=False, stale=True, source='console_cached',
                    monitor_retry_in_seconds=max(0, MONITOR_FULL_READ_INTERVAL - (fetched_at - last_attempt)))
            else:
                MONITOR_CACHE['last_full_attempt_at'] = fetched_at
                account = read_private_account(client)
                if account.get('connected') is True:
                    names = ('can_trade', 'hedge_mode', 'multi_asset', 'taker_fees',
                             'leverage_limits', 'actual_settings')
                    MONITOR_CACHE['configuration'] = {key: copy.deepcopy(account[key])
                        for key in names if key in account}
                    MONITOR_CACHE['configuration_at'] = time.monotonic()
        publish_private_snapshot(account, worker, revision)


def publish_worker_control(worker, state, symbols=None, *, desired_running=None, execution_recovery=None):
    """Publish an acknowledged local action without waiting for account polling."""
    if state not in WORKER_STATES:
        raise HTTPException(503, '引擎未返回可确认的控制状态，请核对运行事件')
    updated = dict(copy.deepcopy(worker), state=state)
    if desired_running is not None:
        updated['desired_running'] = desired_running is True
    if execution_recovery is not None:
        updated['execution_recovery'] = copy.deepcopy(execution_recovery)
    if symbols is not None:
        updated['symbol_enabled'] = parse_symbols(symbols)
    with LOCK:
        revision = STATE.get('control_revision', 0) + 1
        confirmed_at = now_text()
        STATE.update(worker=updated, control_revision=revision, worker_updated_at=confirmed_at)
        STATE['blockers'] = start_reasons(STATE.get('account', {}), updated,
            STATE.get('legacy_worker', {}), STATE.get('old_service', {}))
        return {'worker': copy.deepcopy(updated), 'state': state,
                'desired_running': updated.get('desired_running', False),
                'execution_recovery': copy.deepcopy(updated.get('execution_recovery')),
                'control_revision': revision, 'worker_updated_at': confirmed_at}


def market_snapshot(symbol):
    from execution.strategy import Candle, analyze
    rows = compat.public_json('https://fapi.binance.com/fapi/v1/klines?symbol=' + symbol + '&interval=1h&limit=1500')
    now_ms = int(time.time() * 1000)
    closed = [row for row in rows if int(row[6]) < now_ms]
    candles = [Candle(int(r[0]), Decimal(str(r[1])), Decimal(str(r[2])),
        Decimal(str(r[3])), Decimal(str(r[4])), int(r[6])) for r in closed]
    result = analyze(candles, now_ms)
    signal = result.signal
    def number(value):
        return float(value) if value is not None else None
    reason = {'warmup': '正在完成1,000根完整小时线预热', 'zero_atr': 'ATR为零，等待有效波动',
        'no_complete_candles': '完整小时线尚未取得', 'insufficient_indicator_history': '指标初始化数据不足'}.get(result.reason, result.reason)
    return {'symbol': symbol, 'strategy': STRATEGY, 'ready': result.ready, 'reason': reason,
        'preview': True, 'preview_note': '初始化行情预览；执行指标以引擎固定锚点为准',
        'candle_count': result.candle_count, 'ema': number(result.ema), 'atr': number(result.atr),
        'ema50': number(result.ema50), 'ema12': number(result.ema12),
        'adx14': number(result.adx14), 'rsi14': number(result.rsi14),
        'boll_mid': number(result.boll_mid), 'boll_upper': number(result.boll_upper),
        'boll_lower': number(result.boll_lower), 'branch': signal.branch if signal else None,
        'price': float(closed[-1][4]) if closed else None,
        'closed_at': datetime.fromtimestamp((int(closed[-1][6]) + 1) / 1000, timezone.utc).isoformat() if closed else None,
        'direction': signal.direction if signal else 0,
        'breakout_high': number(signal.breakout_high) if signal else None,
        'breakout_low': number(signal.breakout_low) if signal else None,
        'boundary_ms': signal.boundary_ms if signal else None,
        'signal_valid_now': bool(signal and signal.direction and signal.boundary_ms <= now_ms < signal.boundary_ms + 300000),
        'candles': [{'time': int(r[0]), 'close': float(r[4])} for r in closed[-96:]]}


async def monitor():
    try:
        client = compat.AccountClient()
    except Exception:
        client = None
    last_market = {symbol: -1 for symbol in SYMBOLS}
    next_retry = {symbol: 0 for symbol in SYMBOLS}
    failures = {symbol: 0 for symbol in SYMBOLS}
    while True:
        if client is None:
            try:
                client = compat.AccountClient()
            except Exception:
                pass
        if client:
            await asyncio.to_thread(refresh_monitor_private, client)
        # Reuse connected worker indicators; otherwise preview once per closed hour.
        if not STATE.get('worker', {}).get('connected'):
            for symbol in SYMBOLS:
                now = time.time()
                hour = int(now // 3600)
                if last_market[symbol] == hour or now < next_retry[symbol]:
                    continue
                try:
                    market = await asyncio.to_thread(market_snapshot, symbol)
                    last_market[symbol], failures[symbol] = hour, 0
                except Exception as exc:
                    if getattr(exc, 'network_backoff', False):
                        delay = max(0, float(exc.retry_in_seconds))
                    else:
                        failures[symbol] += 1
                        delay = NETWORK_RETRY_SECONDS
                    next_retry[symbol] = now + delay
                    market = copy.deepcopy(STATE.get('markets', {}).get(symbol, {'symbol': symbol}))
                    market.update(stale=True, error=str(exc) if isinstance(exc, RuntimeError) else '小时行情读取失败，稍后核对')
                with LOCK:
                    STATE['markets'][symbol] = market
        # Frequent local RPC keeps the display current. Actual fallback account
        # sweeps are separately limited to once per minute in this process.
        await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(_app):
    from control.installation import initialize_files
    initialize_files(ROOT, FIXED)
    event('趋势突破＋震荡回归控制台已启动；沿用已保存的用户启停意图，技术恢复独立核对')
    task = asyncio.create_task(monitor())
    try:
        yield
    finally:
        # Closing display must never close or restart a trading worker.
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title='BTC / ETH 趋势突破＋震荡回归交易台', lifespan=lifespan)


@app.middleware('http')
async def local_mutations(request: Request, call_next):
    if request.method == 'POST' and request.headers.get('origin') not in (None, 'http://127.0.0.1:8788', 'http://localhost:8788'):
        return JSONResponse({'detail': '仅支持本地界面操作'}, status_code=403)
    return await call_next(request)


@app.get('/api/status')
def status():
    with LOCK:
        result = copy.deepcopy(STATE)
    from execution.status_view import age_status, age_connection_check, signal_windows
    observed = result.pop('worker_observed_monotonic', None)
    elapsed = max(0, time.monotonic() - observed) if isinstance(observed, (float, int)) else 0
    if isinstance(result.get('worker'), dict):
        result['worker'] = age_status(result['worker'], elapsed)
    account_observed = result.pop('account_observed_monotonic', None)
    account_age = max(0, time.monotonic() - account_observed) if isinstance(account_observed, (float, int)) else 0
    if type(result.get('account', {}).get('age_seconds')) in (float, int):
        result['account']['age_seconds'] += account_age
    result['account'].pop('account_key_fingerprint', None)
    if not result.get('legacy_worker', {}).get('connected'):
        result.setdefault('legacy_worker', {})['history'] = copy.deepcopy(result.get('legacy_book', {}).get('history', []))
    if result.get('worker', {}).get('connected') and isinstance(result['worker'].get('markets'), dict):
        result['markets'] = copy.deepcopy(result['worker']['markets'])
    else:
        result['markets'] = signal_windows(result.get('markets'), int(time.time() * 1000))
    from control.installation import configured
    result.update(parameters=copy.deepcopy(FIXED), strategy=STRATEGY, strategy_version=STRATEGY + '-v1',
        migration={'required': False, 'applied': True, 'blockers': []},
        setup_required=not configured(ROOT),
        installation_root=str(ROOT.resolve()),
        mode='live', engine='趋势突破＋震荡回归执行引擎', console='中文交易台 3.1 自动恢复',
        ledger_available=LEDGER.exists())
    controls = trade_controls()
    try:
        if result.get('worker', {}).get('connected'):
            controls['symbols'] = parse_symbols(result['worker'].get('symbol_enabled'))
    except HTTPException:
        controls['symbols'] = {symbol: False for symbol in SYMBOLS}
    result['trade_controls'] = controls
    worker = result.get('worker', {})
    result['connection_check'] = age_connection_check(latest_connection_check(
        result.get('connection_check'), worker.get('connection_check')) or {}, worker.get('server_time_ms'))
    desired = (worker.get('desired_running') if type(worker.get('desired_running')) is bool
               else saved_user_intent().get('desired_running', False))
    recovery = copy.deepcopy(worker.get('execution_recovery'))
    if recovery is None:
        recovery = {'version': 'automatic-recovery-v1', 'pending': False, 'kind': None,
                    'reason': None, 'retry_in_seconds': 0, 'next_retry_at_ms': None}
        if desired and not worker.get('connected'):
            recovery.update(pending=True, kind='process', reason='交易进程未就绪，后台监督将按保存意图重建')
    result['desired_running'] = controls['desired_running'] = desired
    result['execution_recovery'] = recovery
    result['supervision'] = compat.read_json(DATA / 'supervisor-status.json', {})
    result['blockers'] = start_reasons(result.get('account', {}), result.get('worker', {}),
        result.get('legacy_worker', {}), result.get('old_service', {}))
    account = result.get('account', {})
    result['deferred_start_available'] = bool(
        not account.get('connected') and account.get('recovery_kind') in DEFERRED_START_KINDS
        and not deferred_start_reasons(result))
    result['request_budget'] = RequestBudget(DATA / 'binance-request-budget.sqlite').snapshot()
    result['network_retry'] = copy.deepcopy(result['request_budget'].get('network_retry'))
    # Only the trading worker owns live sockets. A separate display subscription
    # duplicated every stream and could appear live after the worker was lost.
    result['market_stream'] = (copy.deepcopy(worker.get('market_stream'))
        if worker.get('connected') and isinstance(worker.get('market_stream'), dict)
        else {'connections': {'public': False, 'market': False},
              'symbols': {symbol: {'book_live': False, 'trade_live': False,
                                   'mark_live': False, 'latest_trade': None,
                                   'closed_hours_cached': 0} for symbol in SYMBOLS},
              'retry': {}, 'diagnostics': {},
              'source': 'worker', 'available': False, 'reason': '交易引擎行情状态未就绪'})
    return result


def fresh(client=None):
    refresh_private(client if client is not None else compat.AccountClient())
    return copy.deepcopy(STATE)


def confirmed(payload):
    if payload.get('confirmed') is not True:
        raise HTTPException(422, '请在本地界面确认本次操作')


def latest_connection_check(*checks):
    """Keep a slow monitor response from replacing an acknowledged newer task."""
    values = [check for check in checks if isinstance(check, dict) and check.get('id')]
    def rank(check):
        # Persisted revisions span tasks; wall-clock timestamps can move backwards.
        # Legacy snapshots without a revision retain their timestamp ordering.
        return (check.get('revision') or 0, check.get('requested_at_ms') or 0,
                check.get('updated_at_ms') or 0, check.get('completed_at_ms') or 0,
                check.get('started_at_ms') or 0)
    return copy.deepcopy(max(values, key=rank)) if values else None


@app.post('/api/check-connection')
def check_connection(payload: dict):
    """Submit a bounded asynchronous check; never starts or restores a worker."""
    confirmed(payload)
    try:
        result = hourly_rpc('check-connection', 'POST', {'confirmed': True},
                            timeout=CONNECTION_CHECK_RPC_TIMEOUT)
    except HTTPException:
        raise
    except (TimeoutError, OSError, urllib.error.URLError, ValueError):
        raise HTTPException(503, '连接检查受理结果尚未确认；请查看检查任务状态。未自动重发，也未启动策略。') from None
    check = result.get('connection_check') if isinstance(result, dict) else None
    if (not isinstance(result, dict) or result.get('ok') is not True
            or not isinstance(check, dict) or not isinstance(check.get('id'), str)
            or not check['id'] or check.get('state') not in CONNECTION_CHECK_STATES):
        raise HTTPException(503, '引擎尚未返回可核对的检查任务；请查看任务状态。未自动重发，也未启动策略。')
    with LOCK:
        STATE['connection_check'] = latest_connection_check(STATE.get('connection_check'), check)
        published = copy.deepcopy(STATE['connection_check'])
    return {'ok': True, 'merged': result.get('merged') is True, 'connection_check': published,
            'message': ('已合并到正在进行的连接检查；请查看任务进度，受理不代表连接成功。'
                        if result.get('merged') is True else
                        '连接检查已登记；请查看任务进度，受理不代表连接成功。')}


@app.post('/api/strategy-equity')
def strategy_equity(payload: dict):
    confirmed(payload)
    from execution.equity_adjustments import validate_request
    try:
        validate_request(payload.get('symbol'), payload.get('target_equity'),
            payload.get('expected_revision'), payload.get('operation_id'))
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    try:
        result = hourly_rpc('strategy-equity', 'POST', payload, timeout=45)
    except HTTPException:
        raise
    except (TimeoutError, OSError, urllib.error.URLError, ValueError):
        raise HTTPException(503, '保存结果尚未确认，请按原保存编号查询；未自动重发或启动策略。') from None
    if not isinstance(result, dict) or result.get('ok') is not True or not isinstance(result.get('adjustment'), dict):
        raise HTTPException(503, '保存回包尚未确认，请查询原保存编号。')
    # Publish only this coin's newer allocation revision. Never merge old
    # running/selection fields over a concurrent human Pause or symbol choice.
    symbol = payload['symbol']
    acknowledged = result.get('strategy_ledgers', {}).get(symbol)
    if isinstance(acknowledged, dict) and type(acknowledged.get('allocation_revision')) is int:
        with LOCK:
            current = copy.deepcopy(STATE.get('worker', {}))
            ledgers = current.setdefault('strategy_ledgers', {})
            if acknowledged['allocation_revision'] > ledgers.get(symbol, {}).get('allocation_revision', 0):
                ledgers[symbol] = copy.deepcopy(acknowledged)
                STATE.update(worker=current, control_revision=STATE.get('control_revision', 0) + 1,
                             worker_updated_at=now_text())
    return result


@app.get('/api/strategy-equity/{operation_id}')
def strategy_equity_result(operation_id: str):
    from execution.equity_adjustments import validate_request
    try:
        validate_request('BTCUSDT', '1', 0, operation_id)
        return hourly_rpc('strategy-equity/' + operation_id, timeout=5)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    except HTTPException:
        raise
    except (TimeoutError, OSError, urllib.error.URLError):
        raise HTTPException(503, '暂时无法查询保存结果；保留原保存编号，不要重复新建调整。') from None


@app.post('/api/setup')
def setup_credentials(payload: dict):
    confirmed(payload)
    from control.installation import initialize_credentials, initialize_files
    with ACTION_LOCK:
        try:
            initialize_credentials(ROOT, payload.get('api_key'), payload.get('api_secret'))
            initialize_files(ROOT, FIXED)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from None
        event('首次配置已保存；新开仓保持停止，等待账户连接与人工选择品种')
        return {'message': '密钥已保存在此安装的本地文件，等待账户连接后可选择品种并启动。'}


def terminate_legacy():
    identity = inspect_process(LEGACY_PROCESS, 'worker.py')
    if not identity['verified']:
        raise HTTPException(409, '旧进程身份无法核对，未执行进程终止')
    if identity['alive']:
        identity['process'].terminate()
        try:
            identity['process'].wait(timeout=8)
        except Exception:
            raise HTTPException(503, '旧引擎尚未完成退出，迁移尚未应用') from None


@app.post('/api/migrate')
def migrate(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        current = fresh()
        reasons = migration_reasons(current['account'], current['legacy_worker'], current['old_service'], current['legacy_book'])
        if reasons:
            raise HTTPException(409, '；'.join(reasons))
        if current['worker'].get('connected') or current['worker'].get('process_alive'):
            raise HTTPException(409, '新引擎已经存在，先核对其状态')
        if current['legacy_worker'].get('connected'):
            compat.worker_rpc('stop', 'POST')
            for _ in range(15):
                old = legacy_worker_snapshot()
                if old.get('connected') and old.get('state') == 'stopped' and not old.get('trades'):
                    break
                time.sleep(1)
            else:
                raise HTTPException(503, '旧引擎停止状态未确认，新策略尚未应用')
        # Check the exchange and book again after stopping the old worker.
        current = fresh()
        reasons = migration_reasons(current['account'], current['legacy_worker'], current['old_service'], current['legacy_book'])
        if reasons:
            raise HTTPException(409, '；'.join(reasons))
        terminate_legacy()
        current = fresh()
        if current['legacy_worker'].get('connected') or current['legacy_worker'].get('process_alive'):
            raise HTTPException(409, '旧引擎仍存在，新策略尚未应用')
        reasons = migration_reasons(current['account'], current['legacy_worker'], current['old_service'], current['legacy_book'])
        if reasons:
            raise HTTPException(409, '；'.join(reasons))
        settings = dict(copy.deepcopy(FIXED), version=1, migrated=True, applied_at=now_text())
        compat.atomic_json(SETTINGS, settings)
        event('用户已完成旧引擎空仓过渡并应用固定小时策略；新开仓尚未启动')
        return {'message': '新固定策略已应用，旧引擎已退出。新引擎尚未启动，请核对后点击启动。'}


def launch_stopped_worker():
    """Compatibility name: launch/reuse a worker that restores saved intent."""
    from server.process_supervisor import launch_lock, matching_processes, port_busy
    process = None
    with launch_lock(ROOT):
        identity = inspect_process(PROCESS, 'hourly_worker.py')
        found = matching_processes(ROOT, 'hourly_worker.py')
        if identity['alive'] and not identity['verified']:
            raise HTTPException(409, '现有进程身份未确定，未重复启动')
        if not identity['alive'] and not found:
            if port_busy(8791):
                raise HTTPException(409, '8791已有未确认服务，未重复启动')
            key, secret = compat.load_credentials()
            try:
                auth = rpc_auth()
            except RuntimeError:
                auth = {'username': 'hourly-local', 'password': secrets.token_urlsafe(32)}
                compat.atomic_json(DATA / 'local-auth.json', auth)
            env = os.environ.copy()
            env.pop('PYTHONHOME', None)
            env.pop('PYTHONPATH', None)
            env.update(BINANCE_API_KEY=key, BINANCE_API_SECRET=secret,
                       HOURLY_RPC_USER=auth['username'], HOURLY_RPC_PASSWORD=auth['password'])
            (DATA / 'logs').mkdir(parents=True, exist_ok=True)
            with open(DATA / 'logs' / 'hourly-engine.log', 'a', encoding='utf-8') as logfile:
                process = subprocess.Popen([sys.executable, '-X', 'utf8', str(ROOT / 'hourly_worker.py')],
                    cwd=ROOT, env=env, stdout=logfile, stderr=subprocess.STDOUT,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            import psutil
            compat.atomic_json(PROCESS, {'pid': process.pid, 'created': psutil.Process(process.pid).create_time(), 'port': 8791})
    for _ in range(45):
        if process is not None and process.poll() is not None:
            raise HTTPException(503, '新引擎初始化失败，请查看小时引擎日志')
        result = worker_snapshot()
        if result.get('connected'):
            return result
        time.sleep(1)
    raise HTTPException(503, '新引擎仍在核对账户和账本，请稍后刷新')


@app.post('/api/start')
def start(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        symbols = (parse_symbols(payload['symbols']) if 'symbols' in payload else
                   effective_symbols(worker_snapshot()))
        if not any(symbols.values()):
            raise HTTPException(422, '请先选择启用BTC或ETH，两个品种均关闭时不启动交易')
        # One fresh client per human Start: configuration is freshly checked
        # initially, then reused within this action. Dynamic account, position
        # and order reads are never cached.
        client = compat.AccountClient()
        current = fresh(client)
        reasons = start_request_reasons(current, symbols)
        if reasons:
            raise HTTPException(409, '；'.join(reasons))
        if not current['worker'].get('connected'):
            launch_stopped_worker()
            current = fresh(client)
            reasons = start_request_reasons(current, symbols)
            if reasons:
                raise HTTPException(409, '；'.join(reasons))
        result = hourly_rpc('start', 'POST', {'confirmed': True, 'symbols': symbols}, timeout=60)
        if result.get('state') is not None:
            publish_worker_control(current['worker'], result['state'], symbols,
                desired_running=result.get('desired_running', True),
                execution_recovery=result.get('execution_recovery'))
        compat.atomic_json(CONTROLS, {'symbols': symbols, 'updated_at': now_text(), 'version': 1})
        event('用户已请求启动固定小时策略；实际状态以执行引擎核对为准')
        if not (result.get('desired_running') is True and result.get('state') in {'waiting', 'recovering'}):
            refresh_private(client)
        with LOCK:
            published = {'worker': copy.deepcopy(STATE['worker']),
                'control_revision': STATE.get('control_revision', 0),
                'worker_updated_at': STATE.get('worker_updated_at')}
        return dict(result, **published,
            message=result.get('message', '已提交新策略启动请求，请核对实际运行状态'))


@app.post('/api/symbols')
def symbols_action(payload: dict):
    confirmed(payload)
    selected = parse_symbols(payload.get('symbols'))
    with ACTION_LOCK:
        worker = worker_snapshot()
        if worker.get('state') == 'unknown' or (not worker.get('connected') and worker.get('process_alive')):
            raise HTTPException(409, '新交易进程状态未确定，先核对后再改变品种开关')
        if not worker.get('connected') and (worker.get('desired_running') is True
                or saved_user_intent().get('desired_running') is True):
            raise HTTPException(409, '原启动意图仍有效，后台交易进程正在恢复；恢复后再修改品种开关，或先停止新开仓')
        if worker.get('connected'):
            result = hourly_rpc('symbols', 'POST', {'confirmed': True, 'symbols': selected})
        else:
            from server.process_supervisor import launch_lock, matching_processes, port_busy
            with launch_lock(ROOT):
                latest = worker_snapshot()
                if (latest.get('connected') or latest.get('process_alive') or
                        matching_processes(ROOT, 'hourly_worker.py') or port_busy(8791)):
                    raise HTTPException(409, '交易进程正在恢复，请就绪后重新提交品种选择')
                if saved_user_intent().get('desired_running') is True:
                    raise HTTPException(409, '原启动意图仍有效，先停止新开仓或等待进程恢复')
                if LEDGER.is_file():
                    try:
                        with closing(sqlite3.connect(LEDGER.resolve().as_uri() + '?mode=rw', uri=True, timeout=5)) as db, db:
                            db.execute('INSERT INTO hb_meta(key,value) VALUES(?,?) '
                                'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                                ('symbol_enabled', json.dumps(selected)))
                    except sqlite3.Error as exc:
                        raise HTTPException(409, '原账本无法保存品种选择，请先核对账本') from exc
            result = {'state': 'stopped'}
        state = result.get('state')
        if state is None:
            # Compatibility with a local engine that acknowledges the choice
            # without state. Only a post-action engine read can confirm it.
            latest = worker_snapshot()
            if not latest.get('connected') or latest.get('symbol_enabled') != selected:
                raise HTTPException(503, '品种请求已提交，但引擎状态尚未确认，请核对后再操作')
            worker, state = latest, latest['state']
        if 'symbol_enabled' in result and result['symbol_enabled'] != selected:
            raise HTTPException(503, '引擎返回的品种选择与本次请求不一致，请核对运行事件')
        published = publish_worker_control(worker, state, selected,
            desired_running=result.get('desired_running'), execution_recovery=result.get('execution_recovery'))
        compat.atomic_json(CONTROLS, {'symbols': selected, 'updated_at': now_text(), 'version': 1})
        event('用户更新BTC/ETH新开仓开关；实际启停状态以执行引擎确认结果为准')
        return dict(result, **published, symbols=selected, symbol_enabled=selected,
            message=result.get('message') or '品种开关已保存；已有持仓继续管理，停止状态下仍需点击启动。')


@app.post('/api/restore')
def restore(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        current = fresh()
        if not applied_settings():
            raise HTTPException(409, '尚未完成人工策略过渡')
        if current['legacy_worker'].get('connected') or current['legacy_worker'].get('process_alive'):
            raise HTTPException(409, '旧引擎尚未退出')
        if not current['account'].get('connected') or quant_reasons(current['old_service']):
            raise HTTPException(409, '账户或旧项目状态尚未核对')
        if not current['worker'].get('connected'):
            launch_stopped_worker()
        event('用户已恢复小时引擎读取原账本；沿用保存的启停意图和品种开关')
        return {'message': '已恢复引擎核对持仓和订单；沿用保存的用户启停意图。'}


@app.post('/api/pause')
def pause(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        worker = worker_snapshot()
        if not worker.get('connected'):
            if worker.get('process_alive') or worker.get('state') == 'unknown':
                raise HTTPException(409, '新引擎进程状态未确定，无法确认停止成功')
            from server.process_supervisor import launch_lock, matching_processes, port_busy
            with launch_lock(ROOT):
                latest = worker_snapshot()
                if (latest.get('connected') or latest.get('process_alive')
                        or matching_processes(ROOT, 'hourly_worker.py') or port_busy(8791)):
                    raise HTTPException(409, '交易进程正在恢复，请刷新后再次停止以确认执行结果')
                persist_absent_worker_pause()
            event('用户已持久化停止新开仓意图；进程恢复后仍保持停止')
            return dict(publish_worker_control(worker, 'stopped', desired_running=False),
                message='新策略已经停止，目前没有新交易进程运行。')
        result = hourly_rpc('pause', 'POST', {'confirmed': True})
        # Older engine versions return only a message; a successful pause RPC
        # still acknowledges the state assignment under the engine lock.
        if not isinstance(result, dict) or result.get('state', 'paused') != 'paused':
            raise HTTPException(503, '引擎未确认暂停新开仓，请核对运行事件')
        published = publish_worker_control(worker, 'paused', desired_running=False,
            execution_recovery=result.get('execution_recovery'))
        event('用户已暂停新策略开仓，已有新策略持仓继续退出管理')
        return dict(result, **published, message='已停止全部新开仓；已有持仓继续按止损和退出规则管理。')


@app.post('/api/exit-all')
def exit_all(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        result = hourly_rpc('exit-all', 'POST', {'confirmed': True})
        event('用户已请求平掉新策略登记持仓并暂停新开仓')
        return dict(result, message=result.get('message', '已提交新策略平仓并暂停请求，请核对成交'))


@app.post('/api/legacy-pause')
def legacy_pause(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        if not legacy_worker_snapshot().get('connected'):
            raise HTTPException(409, '旧引擎 API 未就绪，未提交操作')
        compat.worker_rpc('stopentry', 'POST')
        event('用户已暂停旧策略新开仓，旧持仓仍由旧引擎管理退出')
        return {'message': '已请求暂停旧策略新开仓；旧持仓继续由原引擎管理。'}


@app.post('/api/legacy-exit-all')
def legacy_exit_all(payload: dict):
    confirmed(payload)
    with ACTION_LOCK:
        current = legacy_worker_snapshot()
        if not current.get('connected'):
            raise HTTPException(409, '旧引擎 API 未就绪，未提交操作')
        compat.worker_rpc('stopentry', 'POST')
        compat.worker_rpc('forceexit', 'POST', {'tradeid': 'all'})
        event('用户已请求旧引擎平仓并暂停，成交确认前不应用新策略')
        return {'message': '已提交旧引擎平仓请求，请核对实际仓位与残留条件单。'}


@app.post('/api/settings')
@app.post('/api/strategy-profile')
@app.post('/api/legacy-review')
def retired(_payload: dict):
    raise HTTPException(410, '原策略与手动金额入口已退役；当前仅提供固定小时策略')


@app.get('/api/strategy-spec')
def strategy_spec():
    path = ROOT / '新小时策略说明.zh-CN.md'
    if not path.exists():
        raise HTTPException(404, '新策略说明正在生成')
    return FileResponse(path, media_type='text/markdown; charset=utf-8')


dist = ROOT / 'frequi' / 'dist'
if dist.exists():
    app.mount('/', StaticFiles(directory=dist, html=True), name='中文界面')
