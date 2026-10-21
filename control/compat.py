"""Chinese local console and read-only account monitor.

No background task starts trading. Only a human's /api/start UI action can
start the separate Freqtrade engine. This service never submits exchange orders.
"""
import asyncio
import base64
import copy
import hashlib
import hmac
import importlib
import json
import math
import os
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'user_data'
PARAMS = DATA / 'parameters.json'
PROFILE_CATALOG = ROOT / 'reports' / 'strategy-profiles.json'
STRATEGY_CLASSES = {'strict': 'AdaptiveFutures', 'aggressive': 'AggressiveAdaptiveFutures'}
PROFILE_SWITCH = {'active': False, 'status': 'idle', 'reason': None}
PROFILE_APPLY_IN_PROGRESS = False
LEGACY_ROOT = Path(os.environ.get('TRADER_LEGACY_ROOT',
    str(ROOT / 'archive/legacy')))
PAIRS = ['BTC/USDT:USDT', 'ETH/USDT:USDT']
SYMBOLS = ['BTCUSDT', 'ETHUSDT']
LOCK = threading.RLock()
SHUTTING_DOWN = False
STATE = {'updated_at': None, 'account': {'connected': False, 'error': '正在连接实盘账户'},
         'markets': {}, 'worker': {'connected': False, 'state': 'stopped', 'trades': []},
         'old_service': {'present': False}, 'events': []}


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def atomic_json(path, value):
    tmp = Path(path).with_suffix(Path(path).suffix + '.tmp')
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    os.replace(tmp, path)


def selected_strategy(settings):
    name = settings.get('strategy_name', 'AdaptiveFutures')
    if name not in STRATEGY_CLASSES.values():
        raise HTTPException(422, '策略类不在已审核白名单中')
    return name


def profile_parameters(profile):
    """Only audited indicator thresholds may be copied from the local catalog."""
    parameters = profile.get('parameters')
    required = {'adx_min', 'breakout_volume', 'pullback_volume', 'compression_quantile'}
    allowed = required | {'breakout_lookback'}
    if (not isinstance(parameters, dict) or not required <= parameters.keys()
            or not parameters.keys() <= allowed
            or (profile.get('id') == 'aggressive' and 'breakout_lookback' not in parameters)):
        raise HTTPException(409, '策略档位阈值尚未完成审核')
    for key, value in parameters.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise HTTPException(409, '策略档位包含无效阈值')
        if ((key == 'adx_min' and not 0 < value <= 100)
                or (key in {'breakout_volume', 'pullback_volume'} and value <= 0)
                or (key == 'compression_quantile' and not 0 < value < 1)
                or (key == 'breakout_lookback' and (not isinstance(value, int) or not 2 <= value <= 192))):
            raise HTTPException(409, '策略档位阈值超出审核范围')
    return copy.deepcopy(parameters)


def strategy_profiles():
    catalog = read_json(PROFILE_CATALOG, {})
    profiles = catalog.get('profiles', []) if isinstance(catalog, dict) else []
    result, seen = [], set()
    for profile in profiles if isinstance(profiles, list) else []:
        if (not isinstance(profile, dict) or not isinstance(profile.get('id'), str)
                or profile.get('id') not in STRATEGY_CLASSES
                or profile.get('id') in seen
                or profile.get('strategy_name') != STRATEGY_CLASSES[profile['id']]):
            continue
        seen.add(profile['id'])
        item = {key: copy.deepcopy(profile.get(key)) for key in
                ['id', 'label', 'description', 'strategy_name', 'parameters', 'research']}
        try:
            profile_parameters(profile)
            item['ready'] = (profile.get('ready') is True
                             and (DATA / 'strategies' / (profile['strategy_name'] + '.py')).is_file())
        except HTTPException:
            item['ready'] = False
        result.append(item)
    return result


def profile_switch_state():
    path = DATA / 'strategy-profile-switch.json'
    persisted = read_json(path)
    if path.exists():
        if not isinstance(persisted, dict) or not isinstance(persisted.get('active'), bool):
            return {'active': True, 'status': 'pending', 'reason': '策略切换记录无法核对，请重新人工应用档位'}
        result = dict(persisted)
    else:
        result = dict(PROFILE_SWITCH)
    if result.get('active') and result.get('status') == 'applying' and not PROFILE_APPLY_IN_PROGRESS:
        result.update(status='pending', reason='上次策略切换未完成核对，新开仓保持关闭，请重新人工应用档位')
    return result


def set_profile_switch(active, status, reason=None, profile_id=None):
    PROFILE_SWITCH.update(active=active, status=status, reason=reason, profile_id=profile_id)
    atomic_json(DATA / 'strategy-profile-switch.json', PROFILE_SWITCH)


def close_entry_permission():
    atomic_json(DATA / 'permission.json', {'allowed': False, 'valid_until': 0})


def load_credentials():
    from control.installation import read_credentials
    key, secret = read_credentials(ROOT)
    if not key or not secret:
        raise RuntimeError('请先填写此安装的API Key和API Secret')
    return key, secret


def public_json(url, timeout=12):
    parsed = urllib.parse.urlsplit(url)
    if parsed.hostname == 'fapi.binance.com':
        from execution.binance import BinanceClient
        fields = dict(urllib.parse.parse_qsl(parsed.query))
        return BinanceClient('', '', timeout=timeout, get_retries=1).public('GET', parsed.path, fields)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.URLError:
            if attempt == 2:
                raise
            time.sleep(0.4 * 2 ** attempt)


class AccountClient:
    """Signed GET requests only. Nothing in this class can place an order."""
    def __init__(self):
        self.key, self.secret = load_credentials()
        if not self.key or not self.secret:
            raise RuntimeError('此安装的API尚未配置')
        self.offset = 0
        from execution.binance import BinanceClient
        self.transport = BinanceClient(self.key, self.secret, timeout=15, get_retries=1)
        self._cache, self._cache_lock, self._clock_synced = {}, threading.RLock(), 0

    def sync_clock(self):
        if time.time() - self._clock_synced < 300:
            return
        self.transport.sync_time()
        self.offset = self.transport._offset_ms
        self._clock_synced = time.time()

    def get(self, path, params=None):
        # Only configuration is cached. Balances, positions, orders and fills stay fresh.
        ttl = {'/fapi/v1/accountConfig': 300, '/fapi/v1/positionSide/dual': 300,
            '/fapi/v1/multiAssetsMargin': 300, '/fapi/v1/leverageBracket': 3600,
            '/fapi/v1/commissionRate': 3600}.get(path, 0)
        key = (path, json.dumps(params or {}, sort_keys=True))
        with self._cache_lock:
            cached = self._cache.get(key)
            if ttl and cached and time.time() - cached[0] < ttl:
                return copy.deepcopy(cached[1])
            result = self.transport.signed('GET', path, params)
            if ttl:
                self._cache[key] = (time.time(), copy.deepcopy(result))
            return result


def old_status(account=None):
    try:
        status = public_json('http://127.0.0.1:8766/api/status', timeout=2)
        # Do not copy ledger or credentials; preserve all source records.
        session = status.get('session') or {}
        position = status.get('position')
        pending = status.get('pending_count')
        valid = (isinstance(status.get('running'), bool) and isinstance(status.get('ready'), bool)
                 and isinstance(pending, int) and pending >= 0 and 'position' in status)
        unfinished = bool(session.get('entry_id') and session.get('phase') not in ['completed', 'failed'])
        risk = status.get('risk') or {}
        cfg = status.get('config', {})
        valid = (valid and not isinstance(pending, bool) and isinstance(session, dict)
                 and isinstance(risk, dict) and isinstance(risk.get('locked'), bool)
                 and isinstance(cfg, dict) and isinstance(position, (dict, type(None)))
                 and isinstance(status.get('last_poll'), (int, float))
                 and abs(time.time() * 1000 - status['last_poll']) <= 120000)
        return {'present': True, 'state': session.get('phase', session.get('state', 'unknown')),
                'position': position is not None, 'pending': pending if isinstance(pending, int) else 1,
                'raw_running': bool(status.get('running', True)), 'unfinished': unfinished,
                'unknown': not valid or not status.get('ready') or bool(status.get('error')),
                'halted': bool(risk.get('locked') or risk.get('strategy_review') or risk.get('accounting_review')),
                'reason': risk.get('reason'),
                'symbol': cfg.get('symbol')}
    except Exception:
        from control.legacy import inspect_stopped_legacy, accepted_review
        fingerprint = (account or STATE.get('account', {})).get('account_key_fingerprint')
        old = inspect_stopped_legacy(LEGACY_ROOT, fingerprint)
        old['accepted_for_new_strategy'] = accepted_review(
            old, fingerprint, read_json(DATA / 'legacy-review.json'))
        return old


def event(message):
    STATE['events'].insert(0, {'time': time.strftime('%Y-%m-%d %H:%M:%S'), 'message': message})
    del STATE['events'][60:]


def worker_rpc(path, method='GET', payload=None, timeout=8):
    config = read_json(DATA / 'live-config.json', {})
    auth = config.get('api_server', {})
    token = base64.b64encode(f"{auth.get('username', '')}:{auth.get('password', '')}".encode()).decode()
    data = json.dumps(payload or {}).encode() if method == 'POST' else None
    request = urllib.request.Request('http://127.0.0.1:8790/api/v1/' + path, data=data, method=method,
                                     headers={'Authorization': 'Basic ' + token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def worker_snapshot():
    try:
        cfg = worker_rpc('show_config')
        if cfg.get('strategy') not in STRATEGY_CLASSES.values():
            raise RuntimeError('无法核对交易引擎策略类')
        trades = worker_rpc('status')
        profit = worker_rpc('profit')
        history = worker_rpc('trades?limit=30')
        return {'connected': True, 'state': cfg.get('state', 'unknown'), 'trades': trades,
                'profit': profit, 'history': history.get('trades', []),
                'strategy': cfg.get('strategy'),
                'max_open_trades': cfg.get('max_open_trades')}
    except Exception:
        previous = STATE.get('worker', {})
        record = read_json(DATA / 'process.json', {})
        alive = False
        if record.get('pid'):
            try:
                import psutil
                process = psutil.Process(record['pid'])
                alive = process.is_running() and abs(process.create_time() - record['created']) < 1
            except (psutil.NoSuchProcess, KeyError):
                pass
        if alive or previous.get('connected'):
            return dict(previous, connected=False, state='unknown', process_alive=alive)
        return {'connected': False, 'state': 'stopped', 'trades': [], 'history': [], 'process_alive': False}


def candles(symbol, interval, limit=500):
    raw = public_json(f'https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}')
    now = int(time.time() * 1000)
    rows = [r for r in raw if int(r[6]) < now]
    frame = pd.DataFrame([[r[0]] + [float(v) for v in r[1:6]] for r in rows],
                         columns=['date', 'open', 'high', 'low', 'close', 'volume'])
    frame['date'] = pd.to_datetime(frame['date'], unit='ms', utc=True)
    return frame


def market_snapshot(symbol):
    from freqtrade.enums import RunMode, CandleType
    from freqtrade.strategy import merge_informative_pair
    sys.path.insert(0, str(DATA / 'strategies'))
    name = selected_strategy(read_json(PARAMS, {}))
    strategy_class = getattr(importlib.import_module(name), name)
    pair = symbol[:-4] + '/USDT:USDT'
    s = strategy_class({'runmode': RunMode.BACKTEST, 'stake_amount': 100,
                        'quant_parameters_file': str(PARAMS), 'strategy': name,
                        'user_data_dir': DATA, 'timeframe': '15m', 'candle_type_def': CandleType.FUTURES})
    d = candles(symbol, '15m')
    for interval, method in [('1h', s.populate_indicators_1h), ('4h', s.populate_indicators_4h)]:
        d = merge_informative_pair(d, method(candles(symbol, interval), {'pair': pair}),
                                   '15m', interval, ffill=True)
    d = s.populate_indicators(d, {'pair': pair})
    d = s.populate_entry_trend(d, {'pair': pair})
    row = d.iloc[-1]
    regime = '上涨趋势' if row['regime_long'] else '下跌趋势' if row['regime_short'] else '过渡 / 震荡'
    signal = str(row['enter_tag']) if row['enter_long'] or row['enter_short'] else ''
    return {'pair': pair, 'strategy_name': name, 'price': float(row['close']), 'closed_at': row['date'].isoformat(),
            'regime': regime, 'signal': signal, 'atr': float(row['atr']),
            'adx': float(row['adx_1h']), 'cooldown': bool(row['entry_cooldown']),
            'candles': [{'time': r.date.isoformat(), 'close': r.close, 'open': r.open,
                         'high': r.high, 'low': r.low} for r in d.tail(96).itertuples()]}


def read_account(client):
    client.sync_clock()
    account = client.get('/fapi/v3/account')
    configuration = client.get('/fapi/v1/accountConfig')
    if not isinstance(configuration.get('canTrade'), bool):
        raise RuntimeError('币安账户交易状态字段缺失，暂无法核对')
    mode = client.get('/fapi/v1/positionSide/dual')
    assets = client.get('/fapi/v1/multiAssetsMargin')
    positions = client.get('/fapi/v2/positionRisk')
    brackets = client.get('/fapi/v1/leverageBracket')
    orders = client.get('/fapi/v1/openOrders')
    algos = client.get('/fapi/v1/openAlgoOrders')
    if isinstance(algos, dict):
        algos = algos.get('orders', [])
    active = [p for p in positions if float(p.get('positionAmt', 0)) != 0]
    limits = {r['symbol']: max(b['initialLeverage'] for b in r['brackets'])
              for r in brackets if r['symbol'] in SYMBOLS}
    actual = {p['symbol']: {'leverage': int(p.get('leverage', 1)), 'isolated':
              str(p.get('marginType', '')).lower() == 'isolated',
              'position_side': p.get('positionSide')} for p in positions if p['symbol'] in SYMBOLS}
    fees = {symbol: float(client.get('/fapi/v1/commissionRate', {'symbol': symbol})['takerCommissionRate'])
            for symbol in SYMBOLS}
    return {'connected': True, 'error': '', 'available_usdt': float(account.get('availableBalance', 0)),
            'equity_usdt': float(account.get('totalMarginBalance', 0)),
            'wallet_usdt': float(account.get('totalWalletBalance', 0)),
            'unrealized_usdt': float(account.get('totalUnrealizedProfit', 0)),
            'can_trade': configuration['canTrade'], 'taker_fees': fees,
            'hedge_mode': bool(mode.get('dualSidePosition')), 'multi_asset': bool(assets.get('multiAssetsMargin')),
            'positions': [{'symbol': p['symbol'], 'quantity': float(p['positionAmt']),
                           'entry_price': float(p['entryPrice']), 'unrealized': float(p.get('unRealizedProfit', 0)),
                           'side': p.get('positionSide')} for p in active],
            'open_orders': len(orders), 'open_algo_orders': len(algos),
            'order_ids': [{'symbol': o['symbol'], 'id': str(o['orderId'])} for o in orders] +
                         [{'symbol': o['symbol'], 'id': str(o.get('algoId', o.get('orderId')))} for o in algos],
            'leverage_limits': limits, 'actual_settings': actual,
            'account_key_fingerprint': hashlib.sha256(client.key.encode()).hexdigest()}


def permission_reasons(account, settings, worker, old):
    reasons = []
    switching = profile_switch_state()
    if switching.get('active'):
        reasons.append(switching.get('reason') or '策略档位正在切换核对，新开仓保持关闭')
    if (worker.get('connected') and worker.get('strategy') is not None
            and worker['strategy'] != settings.get('strategy_name', 'AdaptiveFutures')):
        reasons.append('交易引擎策略与保存的档位尚未一致，请人工应用档位并核对重载')
    if worker.get('state') == 'unknown':
        reasons.append('交易进程状态无法核对，暂不能启动或修改交易参数')
    if not account.get('connected'):
        reasons.append(account.get('error', '实盘账户未连接'))
    if account.get('connected') and not account.get('can_trade'):
        reasons.append('币安账户当前禁止交易（canTrade=false）')
    if account.get('hedge_mode'):
        reasons.append('当前账户为双向持仓；本策略引擎需要在币安手动设置为单向持仓')
    if account.get('multi_asset'):
        reasons.append('当前账户为多资产模式；本策略引擎需要 USDT 单资产模式')
    enabled = [p for p in settings['pairs'].values() if p['enabled']]
    if not enabled:
        reasons.append('至少启用一个交易品种')
    for pair, p in settings['pairs'].items():
        if p['enabled'] and (p.get('leverage') is None or p.get('margin_usdt') is None):
            reasons.append(pair.split('/')[0] + ' 尚未选择杠杆或每笔保证金')
        elif p['enabled']:
            cap = account.get('leverage_limits', {}).get(pair.replace('/', '').split(':')[0], 125)
            if p['leverage'] > cap:
                reasons.append(pair.split('/')[0] + ' 所选杠杆超出账户档位')
    tracked = {}
    registered_orders = set()
    for trade in worker.get('trades', []):
        symbol = trade.get('pair', '').replace('/', '').split(':')[0]
        amount = float(trade.get('amount') or 0) * (-1 if trade.get('is_short') else 1)
        tracked[symbol] = tracked.get(symbol, 0) + amount
        registered_orders.update((symbol, str(o['order_id'])) for o in trade.get('orders', [])
                                 if o.get('order_id') is not None)
    observed = {p['symbol']: p['quantity'] for p in account.get('positions', [])}
    required = sum(float(p.get('margin_usdt') or 0) *
                   (1 + 2 * float(p.get('leverage') or 0) *
                    account.get('taker_fees', {}).get(pair.replace('/', '').split(':')[0], .0005))
                   for pair, p in settings['pairs'].items() if p['enabled']
                   and pair.replace('/', '').split(':')[0] not in tracked)
    if account.get('connected') and required > account.get('available_usdt', 0):
        reasons.append('可用保证金不足以覆盖待开仓品种的预算和成交费用')
    if any(not math.isclose(observed.get(symbol, 0), tracked.get(symbol, 0), rel_tol=1e-6, abs_tol=1e-8)
           for symbol in set(observed) | set(tracked)):
        reasons.append('账户持仓方向或数量与本引擎账本不一致，需先核对')
    if any((o['symbol'], o['id']) not in registered_orders for o in account.get('order_ids', [])):
        reasons.append('账户存在未登记委托，需在币安或原项目中处理')
    if old.get('present') and (old.get('position') or old.get('pending') or old.get('pending_flow') or old.get('raw_running') or old.get('unfinished') or
                               old.get('state') in ['running', 'executing', 'reconciling']):
        reasons.append('原 QuantPilot 仍有执行活动，请在原界面完成暂停和对账')
    if old.get('halted') and not old.get('accepted_for_new_strategy'):
        reasons.append('原账户交易锁定：' + (old.get('reason') or '原因需在原项目中核对'))
    if old.get('unknown') and not old.get('accepted_for_new_strategy'):
        reasons.append('原项目状态尚未就绪，需核对其执行状态后再启动')
    if old.get('review_available') and not old.get('accepted_for_new_strategy'):
        reasons.append('旧项目已停止且无待处理交易，请点击“核对并切换到新策略”')
    return reasons


def refresh_private(client):
    try:
        account = read_account(client)
    except Exception as exc:
        account = {'connected': False, 'error': str(exc) if isinstance(exc, RuntimeError) else '账户连接失败，请检查网络'}
    worker, old = worker_snapshot(), old_status(account)
    with LOCK:
        settings = read_json(PARAMS)
        switching = profile_switch_state()
        if switching.get('active') and switching.get('status') == 'pending':
            worker = dict(worker, state='unknown')
        reasons = permission_reasons(account, settings, worker, old)
        STATE.update(account=account, worker=worker, old_service=old, blockers=reasons,
                     updated_at=time.strftime('%Y-%m-%d %H:%M:%S'))
        allowed = not reasons and not SHUTTING_DOWN and not switching.get('active')
        atomic_json(DATA / 'permission.json', {'allowed': allowed,
                                              'valid_until': time.time() + 45 if allowed else 0})


async def monitor():
    try:
        client = AccountClient()
    except Exception:
        client = None
    last_market = 0
    while True:
        if client:
            await asyncio.to_thread(refresh_private, client)
        if time.time() - last_market > 60:
            for symbol in SYMBOLS:
                try:
                    market = await asyncio.to_thread(market_snapshot, symbol)
                    with LOCK:
                        name = selected_strategy(read_json(PARAMS, {}))
                        if market.get('strategy_name', name) == name:
                            STATE['markets'][symbol] = market
                except Exception:
                    with LOCK:
                        STATE['markets'][symbol] = {'pair': symbol, 'error': '行情指标计算暂未完成'}
            last_market = time.time()
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app):
    global SHUTTING_DOWN
    SHUTTING_DOWN = False
    event('中文控制台已启动，账户读取使用币安正式接口')
    task = asyncio.create_task(monitor())
    yield
    with LOCK:
        SHUTTING_DOWN = True
        atomic_json(DATA / 'permission.json', {'allowed': False, 'valid_until': 0})
    task.cancel()


app = FastAPI(title='BTC / ETH 合约交易台', lifespan=lifespan)


@app.middleware('http')
async def local_mutations(request: Request, call_next):
    if request.method == 'POST' and request.headers.get('origin') not in (None, 'http://127.0.0.1:8788', 'http://localhost:8788'):
        return __import__('starlette.responses', fromlist=['JSONResponse']).JSONResponse({'detail': '仅支持本地界面操作'}, status_code=403)
    return await call_next(request)


@app.get('/api/status')
def status():
    with LOCK:
        settings = dict(read_json(PARAMS, {}))
        settings.setdefault('profile_id', 'strict')
        settings.setdefault('strategy_name', 'AdaptiveFutures')
        return dict(STATE, parameters=settings, strategy_version=settings['strategy_name'] + '-v1',
                    strategy_profiles=strategy_profiles(), profile_switch=profile_switch_state(),
                    four_cycle_validation=read_json(ROOT / 'reports' / 'aggressive-research' / 'summary.json'),
                    mode='live', engine='Freqtrade 2026.9', console='中文定制控制台 1.0（Vue）',
                    ledger_available=(DATA / 'trades-live.sqlite').exists(),
                    validation=read_json(ROOT / 'reports' / 'validation.json'))


@app.post('/api/settings')
def save_settings(payload: dict):
    with LOCK:
        if profile_switch_state().get('active'):
            raise HTTPException(409, '策略档位切换尚未核对完成，暂不能修改交易参数')
        fresh_worker = worker_snapshot()
        STATE['worker'] = fresh_worker
        if fresh_worker.get('state') == 'unknown':
            raise HTTPException(409, '交易进程状态尚未核对，暂不能修改参数')
        if STATE['worker'].get('state') == 'running' and STATE['worker'].get('max_open_trades', 0) != 0:
            raise HTTPException(409, '请先点击“暂停新开仓”再保存参数')
        current = read_json(PARAMS)
        if set(payload.get('pairs', {})) != set(PAIRS):
            raise HTTPException(422, '必须分别提供 BTC 和 ETH 参数')
        for pair, p in payload['pairs'].items():
            cap = STATE['account'].get('leverage_limits', {}).get(pair.replace('/', '').split(':')[0], 125)
            lev, margin = p.get('leverage'), p.get('margin_usdt')
            if lev is not None and (isinstance(lev, bool) or not isinstance(lev, int) or not 1 <= lev <= cap):
                raise HTTPException(422, '杠杆必须为交易所允许范围内的整数')
            if margin is not None and (isinstance(margin, bool) or not isinstance(margin, (int, float)) or
                                       not __import__('math').isfinite(margin) or margin <= 0):
                raise HTTPException(422, '每笔保证金必须为正数')
            current['pairs'][pair] = {'enabled': bool(p.get('enabled')), 'leverage': lev, 'margin_usdt': margin}
        atomic_json(PARAMS, current)
        atomic_json(DATA / 'permission.json', {'allowed': False, 'valid_until': 0})
        event('已保存手动参数；下一次新开仓使用保存值，现有持仓按原入场参数退出')
        return {'message': '参数已保存', 'parameters': current}


def profile_switch_failure(profile, settings, parameters_saved=True):
    """A partial update stays locked until a human retries and verifies the reload."""
    reason = ('策略档位已保存，但引擎重载尚未核对完成' if parameters_saved else '策略档位切换未完成')
    reason += '；新开仓保持关闭，请稍后人工重试应用'
    with LOCK:
        set_profile_switch(True, 'pending', reason, profile['id'])
        close_entry_permission()
        STATE['worker'] = dict(STATE.get('worker', {}), state='unknown')
        STATE['blockers'] = [reason]
        event(reason)
    return JSONResponse({'status': 'pending', 'message': reason, 'reloaded': False,
                         'trading_started': False, 'profile_id': profile['id'],
                         'strategy_name': profile['strategy_name'], 'parameters': settings,
                         'parameters_saved': parameters_saved}, status_code=202)


@app.post('/api/strategy-profile')
def apply_strategy_profile(payload: dict):
    global PROFILE_APPLY_IN_PROGRESS
    if payload.get('confirmed') is not True:
        raise HTTPException(422, '需要在本地界面明确确认应用策略档位')
    profile_id = payload.get('profile_id')
    profile = next((p for p in strategy_profiles() if p['id'] == profile_id), None)
    if profile is None or profile.get('ready') is not True:
        raise HTTPException(409, '所选策略档位尚未完成研究和部署审核')
    parameters = profile_parameters(profile)
    with LOCK:
        if PROFILE_APPLY_IN_PROGRESS:
            raise HTTPException(409, '另一项策略档位切换正在核对，请稍后再试')
        PROFILE_APPLY_IN_PROGRESS = True
    try:
        return _apply_strategy_profile(profile, parameters)
    finally:
        with LOCK:
            PROFILE_APPLY_IN_PROGRESS = False


def _apply_strategy_profile(profile, parameters):
    profile_id = profile['id']
    with LOCK:
        previous_switch = profile_switch_state()
        set_profile_switch(True, 'applying', '策略档位正在切换核对，新开仓保持关闭', profile_id)
        close_entry_permission()
    parameters_saved = False
    try:
        client = AccountClient()
        refresh_private(client)
        with LOCK:
            account, worker = STATE['account'], STATE['worker']
            state = worker.get('state')
            can_switch = state in {'stopped', 'paused'} or (
                state == 'running' and worker.get('max_open_trades') == 0
                and not isinstance(worker.get('max_open_trades'), bool))
            if (not can_switch or (not worker.get('connected')
                                   and (state != 'stopped' or worker.get('process_alive')))):
                raise HTTPException(409, '请先暂停新开仓并核对引擎状态，再人工应用档位')
            if not account.get('connected'):
                raise HTTPException(409, '实盘账户尚未完成最新核对，暂不能应用档位')
            if (not isinstance(account.get('positions'), list)
                    or not isinstance(worker.get('trades'), list)
                    or any(not isinstance(account.get(key), int) or isinstance(account.get(key), bool)
                           or account[key] < 0 for key in ['open_orders', 'open_algo_orders'])):
                raise HTTPException(409, '账户持仓或委托状态无法核对，暂不能应用档位')
            if (account['positions'] or account['open_orders'] or account['open_algo_orders'] or worker['trades']):
                raise HTTPException(409, '账户或引擎仍有持仓、普通委托或条件委托，请完成对账后再应用档位')
            current = read_json(PARAMS)
            if not isinstance(current, dict) or not isinstance(current.get('pairs'), dict):
                raise HTTPException(409, '现有交易参数无法读取，请先核对')
            settings = copy.deepcopy(current)
            settings.update(strategy_name=profile['strategy_name'], profile_id=profile_id, strategy=parameters)
            atomic_json(PARAMS, settings)
            parameters_saved = True
            STATE['markets'] = {}
            if not worker.get('connected'):
                set_profile_switch(False, 'saved', profile_id=profile_id)
                close_entry_permission()
                event('用户已保存策略档位，等待下一次人工启动；当前未启动交易引擎')
                return {'status': 'saved', 'message': '策略档位已保存，将在下一次人工启动引擎时加载；交易未启动',
                        'reloaded': False, 'trading_started': False, 'profile_id': profile_id,
                        'strategy_name': profile['strategy_name'], 'parameters': settings}
            atomic_json(DATA / 'live-config.json', make_config(settings))
    except HTTPException as error:
        if parameters_saved:
            return profile_switch_failure(profile, read_json(PARAMS))
        with LOCK:
            set_profile_switch(previous_switch.get('active', False), previous_switch.get('status', 'idle'),
                               previous_switch.get('reason'), previous_switch.get('profile_id'))
            close_entry_permission()
            STATE['blockers'] = [error.detail]
        raise
    except Exception:
        return profile_switch_failure(profile, read_json(PARAMS), parameters_saved)

    # Only reload_config is a mutation here. It reloads initial_state=stopped;
    # never send start, forceentry/forceexit, or spawn a new process.
    deadline = time.monotonic() + 15
    try:
        response = worker_rpc('reload_config', 'POST', timeout=min(8, deadline - time.monotonic()))
        if not isinstance(response, dict) or not str(response.get('status', '')).lower().startswith('reloading config'):
            return profile_switch_failure(profile, settings)
        while time.monotonic() < deadline:
            try:
                cfg = worker_rpc('show_config', timeout=min(2, deadline - time.monotonic()))
                if cfg.get('strategy') == profile['strategy_name'] and cfg.get('state') == 'stopped':
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    trades = worker_rpc('status', timeout=min(2, remaining))
                    if not isinstance(trades, list) or trades:
                        break
                    with LOCK:
                        STATE['worker'] = dict(STATE.get('worker', {}), connected=True, state='stopped',
                                               strategy=cfg['strategy'], max_open_trades=cfg.get('max_open_trades'),
                                               trades=trades)
                        set_profile_switch(False, 'applied', profile_id=profile_id)
                        close_entry_permission()
                        STATE['blockers'] = permission_reasons(STATE['account'], settings, STATE['worker'],
                                                              STATE.get('old_service', {}))
                        event('用户已应用策略档位并核对引擎停止状态；开始交易仍需人工点击启动')
                    return {'status': 'applied', 'message': '策略档位已应用，引擎已重新加载并保持停止；开始交易仍需人工点击启动',
                            'reloaded': True, 'trading_started': False, 'profile_id': profile_id,
                            'strategy_name': profile['strategy_name'], 'parameters': settings}
            except Exception:
                pass
            time.sleep(min(.25, max(0, deadline - time.monotonic())))
    except Exception:
        pass
    return profile_switch_failure(profile, settings)


@app.post('/api/legacy-review')
def confirm_legacy_review(payload: dict):
    """Human's local strategy-switch decision. No trading RPC or old-ledger write."""
    if payload.get('confirmed') is not True:
        raise HTTPException(422, '需要在本地界面明确确认策略切换')
    client = AccountClient()
    refresh_private(client)
    with LOCK:
        account, old, worker = STATE['account'], STATE['old_service'], STATE['worker']
        if not account.get('connected') or not old.get('review_available'):
            raise HTTPException(409, old.get('error') or '账户或旧项目核对尚未完成')
        if worker.get('state') not in ['stopped', 'paused']:
            raise HTTPException(409, '请先核对并停止新开仓，再记录策略切换')
        if (not payload.get('expected_ledger_digest') or not payload.get('expected_account_key_fingerprint') or
                payload['expected_ledger_digest'] != old.get('ledger_digest') or
                payload['expected_account_key_fingerprint'] != account.get('account_key_fingerprint')):
            raise HTTPException(409, '账户密钥或旧账本状态已变化，请刷新页面后重新核对')
        if account.get('positions') or account.get('open_orders') or account.get('open_algo_orders'):
            raise HTTPException(409, '账户仍有持仓或挂单，需先完成对账后再确认切换')
        atomic_json(DATA / 'legacy-review.json', {
            'confirmed': True, 'account_key_fingerprint': account['account_key_fingerprint'],
            'ledger_digest': old['ledger_digest'], 'reviewed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'legacy_risk_locked': old.get('halted'), 'legacy_risk_reason': old.get('reason'),
            'api_key_matches': old.get('api_key_matches'), 'decision': 'use_current_api_for_new_strategy',
            'old_ledger_modified': False, 'trading_started': False})
        atomic_json(DATA / 'permission.json', {'allowed': False, 'valid_until': 0})
        event('用户已确认使用当前 API 切换到新策略；旧账本和风险锁原样保留，交易尚未启动')
    refresh_private(client)
    return {'message': '策略切换决定已记录，旧账本和旧锁定未修改；请检查启动条件后手动启动交易'}


def make_config(settings):
    name = selected_strategy(settings)
    auth = read_json(DATA / 'local-auth.json')
    if not auth:
        auth = {'username': 'local', 'password': secrets.token_urlsafe(24), 'jwt_secret_key': secrets.token_hex(32)}
        atomic_json(DATA / 'local-auth.json', auth)
    proxy = urllib.request.getproxies().get('https')
    transport = {'enableRateLimit': True, 'options': {'adjustForTimeDifference': True,
                 'fetchMarkets': {'types': ['linear']}}, **({'httpsProxy': proxy} if proxy else {})}
    return {'max_open_trades': 2, 'stake_currency': 'USDT', 'stake_amount': 'unlimited',
            'tradable_balance_ratio': 0.99, 'dry_run': False, 'trading_mode': 'futures',
            'margin_mode': 'isolated', 'timeframe': '15m', 'strategy': name,
            'quant_parameters_file': str(PARAMS), 'user_data_dir': str(DATA),
            'db_url': 'sqlite:///' + str(DATA / 'trades-live.sqlite'),
            'cancel_open_orders_on_exit': False, 'initial_state': 'stopped', 'force_entry_enable': False,
            'unfilledtimeout': {'entry': 2, 'exit': 10, 'unit': 'minutes', 'exit_timeout_count': 0},
            'entry_pricing': {'price_side': 'other', 'use_order_book': True, 'order_book_top': 1},
            'exit_pricing': {'price_side': 'other', 'use_order_book': True, 'order_book_top': 1},
            'exchange': {'name': 'binance', 'key': '', 'secret': '',
                         'ccxt_config': transport,
                         'ccxt_async_config': transport,
                         'pair_whitelist': PAIRS,
                         'pair_blacklist': []},
            'pairlists': [{'method': 'StaticPairList'}], 'telegram': {'enabled': False, 'token': '', 'chat_id': ''},
            'api_server': dict(auth, enabled=True, listen_ip_address='127.0.0.1', listen_port=8790,
                               verbosity='error', enable_openapi=False, CORS_origins=[]),
            'bot_name': 'BTC-ETH 中文实盘', 'internals': {'process_throttle_secs': 5}}


def launch_stopped_worker():
    """Only user action routes call this; initialization leaves new entries stopped."""
    settings = read_json(PARAMS)
    config = make_config(settings)
    key, secret = load_credentials()
    process_record = read_json(DATA / 'process.json', {})
    if process_record.get('pid'):
        try:
            import psutil
            process = psutil.Process(process_record['pid'])
            if process.is_running() and abs(process.create_time() - process_record['created']) < 1:
                raise HTTPException(409, '交易进程已存在但 API 尚未就绪，请查看运行日志')
        except psutil.NoSuchProcess:
            pass
    atomic_json(DATA / 'live-config.json', config)
    env = os.environ.copy()
    env.update(FREQTRADE__EXCHANGE__KEY=key, FREQTRADE__EXCHANGE__SECRET=secret)
    (DATA / 'logs').mkdir(exist_ok=True)
    with open(DATA / 'logs' / 'engine.log', 'a', encoding='utf-8') as logfile:
        process = subprocess.Popen([sys.executable, '-X', 'utf8', str(ROOT / 'worker.py'), 'trade', '--config',
                                    str(DATA / 'live-config.json'), '--strategy-path', str(DATA / 'strategies')],
                                   cwd=ROOT, env=env, stdout=logfile, stderr=subprocess.STDOUT,
                                   creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    import psutil
    atomic_json(DATA / 'process.json', {'pid': process.pid, 'created': psutil.Process(process.pid).create_time()})
    for _ in range(45):
        if process.poll() is not None:
            raise HTTPException(503, '交易引擎启动失败，请查看 user_data/logs/engine.log')
        try:
            worker_rpc('ping')
            return
        except Exception:
            time.sleep(1)
    raise HTTPException(503, '交易引擎仍在初始化，请稍后查看状态')


@app.post('/api/restore')
def restore():
    # Human recovery action: recover the RPC/ledger first, never POST /start.
    if not (DATA / 'trades-live.sqlite').exists():
        raise HTTPException(409, '尚无本引擎实盘账本可恢复')
    with LOCK:
        atomic_json(DATA / 'permission.json', {'allowed': False, 'valid_until': 0})
        current = worker_snapshot()
        if current.get('process_alive'):
            raise HTTPException(409, '引擎进程仍存在，请先核对其日志和 API 状态')
        if not current.get('connected'):
            launch_stopped_worker()
        STATE['worker'] = worker_snapshot()
        event('用户已恢复停止状态的交易引擎；核对持仓后可点击暂停新开仓以恢复退出管理')
        return {'message': '账本读取已恢复，新开仓保持停止；请核对登记持仓'}


@app.post('/api/start')
def start():
    # This route is invoked only by a user's explicit click, never by deployment or monitoring.
    client = AccountClient()
    refresh_private(client)
    with LOCK:
        if profile_switch_state().get('active'):
            raise HTTPException(409, '策略档位切换尚未核对完成，新开仓保持关闭')
        if STATE.get('blockers'):
            raise HTTPException(409, '；'.join(STATE['blockers']))
        if not STATE['worker']['connected']:
            launch_stopped_worker()
        # Check again after initialization has loaded its ledger and reconciled orders.
        refresh_private(client)
        if STATE.get('blockers'):
            raise HTTPException(409, '；'.join(STATE['blockers']))
        result = worker_rpc('start', 'POST')
        STATE['worker'].update(connected=True, state='running', max_open_trades=2)
        event('用户已启动自动实盘交易，等待策略条件满足')
        return result


@app.post('/api/pause')
def pause():
    result = worker_rpc('stopentry', 'POST')
    with LOCK:
        STATE['worker'].update(connected=True, state='paused')
    event('用户已暂停新开仓，已有持仓继续管理退出')
    return result


@app.post('/api/exit-all')
def exit_all():
    worker_rpc('stopentry', 'POST')
    result = worker_rpc('forceexit', 'POST', {'tradeid': 'all'})
    event('用户已请求平仓并暂停新开仓')
    return result


@app.get('/api/report')
def report():
    path = ROOT / 'reports' / '策略验证报告.md'
    if not path.exists():
        raise HTTPException(404, '策略研究报告尚未生成')
    return FileResponse(path, media_type='text/markdown; charset=utf-8')


@app.get('/api/aggressive-report')
def aggressive_report():
    path = ROOT / 'reports' / 'aggressive-research' / '四周期50倍策略对比.md'
    if not path.exists():
        raise HTTPException(404, '四周期策略对比报告尚未生成')
    return FileResponse(path, media_type='text/markdown; charset=utf-8')


dist = ROOT / 'frequi' / 'dist'
if dist.exists():
    app.mount('/', StaticFiles(directory=dist, html=True), name='中文界面')
