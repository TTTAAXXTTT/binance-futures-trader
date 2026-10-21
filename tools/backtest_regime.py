"""Run the preregistered BTC/ETH study using public data and read-only caches.

Only reports/regime-* is written. No credentials, live ledgers, account
endpoints, RPC requests, processes or trading commands are used.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'runtime/packages'), str(ROOT)]

from execution.strategy import Candle, HOUR_MS, MarginTier, MarketRules
from research.regime_backtest import (
    FIVE_MS, Funding, SCENARIOS, Simulator, acceptance, aggregate,
    assert_candles, prepare_analyses,
)

SH = timezone(timedelta(hours=8))
START = int(datetime(2026, 9, 3, tzinfo=SH).timestamp() * 1000)
END = int(datetime(2026, 10, 1, tzinfo=SH).timestamp() * 1000)
WEEK = 7 * 24 * HOUR_MS
WARM_START = START - 1000 * HOUR_MS
DATA = ROOT / 'reports/regime-data'
OUTPUT = ROOT / 'reports/regime-backtest'
BASE = 'https://fapi.binance.com'
DOCS = 'https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data'


def dump(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2,
                              default=lambda v: str(v) if isinstance(v, D) else str(v)), encoding='utf-8')


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, SH).isoformat()


class PublicHistory:
    allowed = {'/fapi/v1/klines', '/fapi/v1/markPriceKlines', '/fapi/v1/fundingRate', '/fapi/v1/exchangeInfo'}

    def __init__(self):
        self.sources = []
        self.last_request = 0.0

    def get(self, path, params=None):
        if path not in self.allowed or any('key' in k.lower() or 'signature' in k.lower() for k in (params or {})):
            raise ValueError('public_market_endpoint_only')
        # At most one public request per second, independent of live machinery.
        remaining = 1.0 - (time.monotonic() - self.last_request)
        if remaining > 0:
            time.sleep(remaining)
        url = BASE + path + ('?' + urllib.parse.urlencode(params) if params else '')
        self.last_request = time.monotonic()
        request = urllib.request.Request(url, headers={'User-Agent': 'RegimeHistoricalResearch/1.0'})
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
        decoded = json.loads(raw)
        if isinstance(decoded, dict) and decoded.get('code', 0) < 0:
            raise ValueError('public_exchange_error: ' + str(decoded.get('code')))
        item = {'url': url, 'sha256_raw': hashlib.sha256(raw).hexdigest(),
                'retrieved_at_utc': datetime.now(timezone.utc).isoformat()}
        self.sources.append(item)
        return decoded

    def klines(self, symbol, interval, start, end, mark=False):
        duration = HOUR_MS if interval == '1h' else FIVE_MS
        rows = []
        cursor = start
        path = '/fapi/v1/markPriceKlines' if mark else '/fapi/v1/klines'
        while cursor < end:
            page = self.get(path, {'symbol': symbol, 'interval': interval, 'startTime': cursor,
                                   'endTime': end - 1, 'limit': 499})
            if not isinstance(page, list) or not page:
                raise ValueError(f'public_history_missing_{symbol}_{interval}_{cursor}')
            accepted = [row for row in page if cursor <= int(row[0]) < end]
            if not accepted:
                raise ValueError('history_cursor_did_not_advance')
            rows.extend(Candle(int(r[0]), D(r[1]), D(r[2]), D(r[3]), D(r[4]), int(r[6])) for r in accepted)
            cursor = int(accepted[-1][0]) + duration
        assert_candles(rows, start, end, duration, symbol + ('_mark' if mark else '_trade'))
        return rows

    def funding(self, symbol, start, end):
        result = []
        cursor = start
        while cursor < end:
            page = self.get('/fapi/v1/fundingRate', {'symbol': symbol, 'startTime': cursor,
                                                   'endTime': end - 1, 'limit': 1000})
            if not isinstance(page, list) or not page:
                break
            for row in page:
                if not row.get('markPrice'):
                    raise ValueError('published_funding_missing_settlement_mark')
                result.append(Funding(int(row['fundingTime']), D(row['fundingRate']), D(row['markPrice'])))
            cursor = int(page[-1]['fundingTime']) + 1
            if len(page) < 1000:
                break
        verify_funding(result, start, end)
        return result


def verify_funding(events, start, end):
    times = [e.time_ms for e in events]
    if not times or times != sorted(set(times)) or any(t < start or t >= end for t in times):
        raise ValueError('missing_duplicate_or_invalid_funding_events')
    # BTC/ETH cache has actual ~8h events. Reject a missing event; never fill it
    # with an averaged 8h series or a trade price. Small publication ms retained.
    if times[0] - start > 8 * HOUR_MS + 1000 or end - times[-1] > 8 * HOUR_MS + 1000:
        raise ValueError('funding_range_not_covered')
    if any(b - a > 8 * HOUR_MS + 1000 for a, b in zip(times, times[1:])):
        raise ValueError('funding_event_gap')


def frame_candles(frame, start, end, duration):
    import pandas as pd
    dates = pd.to_datetime(frame['date'], utc=True)
    mask = (dates >= pd.Timestamp(start, unit='ms', tz='UTC')) & (dates < pd.Timestamp(end, unit='ms', tz='UTC'))
    selected = frame.loc[mask]
    rows = [Candle(int(pd.Timestamp(row.date).value // 1_000_000), D(str(row.open)), D(str(row.high)),
                   D(str(row.low)), D(str(row.close)), int(pd.Timestamp(row.date).value // 1_000_000) + duration - 1)
            for row in selected.itertuples(index=False)]
    assert_candles(rows, start, end, duration, 'verified_cached_candles')
    return rows


def verified_market(symbol, public, evidence):
    import pandas as pd
    coin = symbol[:3]
    directory = ROOT / 'reports/v2-research/data'
    result = {}
    for kind, tf, source_ms in [('futures', '5m', FIVE_MS), ('mark', '1m', 60_000)]:
        path = directory / f'{coin}_USDT_USDT-{tf}-{kind}.feather'
        provenance = path.with_suffix('.provenance.json')
        if not path.is_file() or not provenance.is_file():
            result[kind] = public.klines(symbol, '5m', WARM_START, END, mark=kind == 'mark')
            continue
        record = json.loads(provenance.read_text(encoding='utf-8'))
        digest = sha(path)
        unverified_in_range = []
        excluded_repairs = []
        for source in record['sources']:
            if source.get('checksum_verified') is True:
                continue
            if source.get('repair_start') and source.get('repair_end_exclusive'):
                repair_start = int(datetime.fromisoformat(source['repair_start']).timestamp() * 1000)
                repair_end = int(datetime.fromisoformat(source['repair_end_exclusive']).timestamp() * 1000)
                if repair_end <= WARM_START or repair_start >= END:
                    excluded_repairs.append(source)
                    continue
            unverified_in_range.append(source)
        if digest != record.get('sha256') or unverified_in_range:
            raise ValueError('public_archive_provenance_hash_or_checksum_failed')
        frame = pd.read_feather(path, columns=['date', 'open', 'high', 'low', 'close'])
        rows = frame_candles(frame, WARM_START, END, source_ms)
        result[kind] = aggregate(rows, source_ms, FIVE_MS) if source_ms != FIVE_MS else rows
        evidence.append({'symbol': symbol, 'kind': kind, 'cache': str(path.relative_to(ROOT)),
                         'sha256': digest, 'provenance_sha256': sha(provenance),
                         'archive_checksums_verified': True, 'original_interval': tf,
                         'outside_study_rest_repairs_excluded': excluded_repairs,
                         'transformation': 'actual 1m OHLC aggregated to 5m' if kind == 'mark' else 'none',
                         'rows_used': len(rows), 'first_ms': rows[0].open_ms,
                         'end_exclusive_ms': rows[-1].open_ms + source_ms,
                         'public_archive_source_urls': [s['url'] for s in record['sources'] if s.get('url')
                            and any(month in s['url'] for month in ('2026-07', '2026-08', '2026-09'))]})
    result['hours'] = aggregate(result['futures'], FIVE_MS, HOUR_MS)
    return result


def verified_funding(symbol, public, evidence, refresh=False):
    import pandas as pd
    path = ROOT / f'user_data/data/binance/futures/{symbol}-funding-detail.feather'
    audit = ROOT / 'reports/v2-research/funding-audit.json'
    if refresh or not path.is_file() or not audit.is_file():
        return public.funding(symbol, WARM_START, END)
    record = json.loads(audit.read_text(encoding='utf-8'))
    item = next((item for item in record['files'] if item['symbol'] == symbol), None)
    digest = sha(path)
    if item is None or item['detail_sha256'] != digest:
        raise ValueError('funding_detail_audit_hash_mismatch')
    frame = pd.read_feather(path)
    dates = pd.to_datetime(frame['date'], utc=True)
    selected = frame[(dates >= pd.Timestamp(WARM_START, unit='ms', tz='UTC'))
                     & (dates < pd.Timestamp(END, unit='ms', tz='UTC'))]
    events = [Funding(int(pd.Timestamp(row.date).value // 1_000_000), D(str(row.rate)), D(str(row.mark)))
              for row in selected.itertuples(index=False)]
    verify_funding(events, WARM_START, END)
    evidence.append({'symbol': symbol, 'kind': 'actual_funding_events', 'cache': str(path.relative_to(ROOT)),
                     'sha256': digest, 'audit_sha256': sha(audit), 'events_used': len(events),
                     'source': 'https://fapi.binance.com/fapi/v1/fundingRate', 'fields': ['fundingTime', 'fundingRate', 'markPrice'],
                     'provenance_limit': record['detail_provenance_limit'],
                     'actual_event_ms_preserved': True, 'settlement_mark_interpolation': False})
    return events


def load_rules(public, offline_filters=False):
    tiers_path = ROOT / 'reports/v2-research/source-snapshot/binance_leverage_tiers.json'
    cached = ROOT / 'reports/v2-research/exchange-filters.json'
    exchange = json.loads(cached.read_text(encoding='utf-8')) if offline_filters else public.get('/fapi/v1/exchangeInfo')
    dump(DATA / 'exchange-filters.json', {'public_current_snapshot': exchange,
                                        'historical_snapshots_available': False,
                                        'used_cached_snapshot': offline_filters})
    tier_data = json.loads(tiers_path.read_text(encoding='utf-8'))
    rules = {}
    for symbol in ('BTCUSDT', 'ETHUSDT'):
        item = next(item for item in exchange['symbols'] if item['symbol'] == symbol)
        filters = {f['filterType']: f for f in item['filters']}
        price, lot = filters['PRICE_FILTER'], filters.get('MARKET_LOT_SIZE', filters['LOT_SIZE'])
        if D(lot['stepSize']) <= 0:
            lot = filters['LOT_SIZE']
        tiers = tier_data[symbol[:3] + '/USDT:USDT']
        converted = tuple(MarginTier(D(str(t['minNotional'])), D(str(t['maxNotional'])),
                                    int(t['maxLeverage']), D(str(t['maintenanceMarginRate'])),
                                    D(str(t['info']['cum']))) for t in tiers)
        rules[symbol] = MarketRules(D(price['tickSize']), D(lot['stepSize']), D(lot['minQty']),
                                   D(filters['MIN_NOTIONAL'].get('notional', filters['MIN_NOTIONAL'].get('minNotional'))),
                                   D(lot['maxQty']), converted)
    return rules, {'current_filters_sha256': sha(DATA / 'exchange-filters.json'),
                   'maintenance_tiers_cache': str(tiers_path.relative_to(ROOT)),
                   'maintenance_tiers_sha256': sha(tiers_path),
                   'historical_filters_and_account_specific_tier_changes_reconstructed': False}


def csv_rows(path, rows):
    if not rows:
        path.write_text('', encoding='utf-8-sig')
        return
    columns = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows({k: json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (dict, list)) else v
                          for k, v in row.items()} for row in rows)


def materialize(markets, funding, rules, evidence, rule_evidence, public_sources):
    """Keep the complete study slice in regime-data for portable reproduction."""
    files = {}
    for symbol, market in markets.items():
        for kind in ('futures', 'mark'):
            path = DATA / f'{symbol}-{kind}-5m.json'
            rows = [[c.open_ms, str(c.open), str(c.high), str(c.low), str(c.close), c.close_ms]
                    for c in market[kind]]
            path.write_text(json.dumps(rows, separators=(',', ':')), encoding='utf-8')
            files[path.name] = sha(path)
        path = DATA / f'{symbol}-funding-events.json'
        dump(path, [{'fundingTime': e.time_ms, 'fundingRate': e.rate, 'markPrice': e.mark} for e in funding[symbol]])
        files[path.name] = sha(path)
    path = DATA / 'study-rules.json'
    dump(path, {symbol: {'tick_size': r.tick_size, 'qty_step': r.qty_step, 'min_qty': r.min_qty,
                        'min_notional': r.min_notional, 'max_qty': r.max_qty,
                        'tiers': [[t.notional_floor, t.notional_cap, t.max_leverage,
                                   t.maintenance_rate, t.maintenance_amount] for t in r.margin_tiers]}
                for symbol, r in rules.items()})
    files[path.name] = sha(path)
    dump(DATA / 'study-inputs.json', {'files_sha256': files, 'range_start_ms': WARM_START,
        'end_exclusive_ms': END, 'source_evidence': evidence, 'rule_evidence': rule_evidence,
        'original_public_get_requests': public_sources,
        'portable_replay': 'These verified materialized slices are sufficient; no user_data or old report caches needed.'})


def load_materialized():
    manifest = json.loads((DATA / 'study-inputs.json').read_text(encoding='utf-8'))
    if manifest['range_start_ms'] != WARM_START or manifest['end_exclusive_ms'] != END:
        raise ValueError('materialized_study_range_changed')
    for name, digest in manifest['files_sha256'].items():
        if Path(name).name != name or sha(DATA / name) != digest:
            raise ValueError('materialized_input_hash_mismatch')
    markets, funding, rules = {}, {}, {}
    rule_data = json.loads((DATA / 'study-rules.json').read_text(encoding='utf-8'))
    for symbol in ('BTCUSDT', 'ETHUSDT'):
        market = {}
        for kind in ('futures', 'mark'):
            raw = json.loads((DATA / f'{symbol}-{kind}-5m.json').read_text(encoding='utf-8'))
            market[kind] = [Candle(int(r[0]), D(r[1]), D(r[2]), D(r[3]), D(r[4]), int(r[5])) for r in raw]
            assert_candles(market[kind], WARM_START, END, FIVE_MS, 'portable_materialized_' + kind)
        market['hours'] = aggregate(market['futures'], FIVE_MS, HOUR_MS)
        markets[symbol] = market
        raw = json.loads((DATA / f'{symbol}-funding-events.json').read_text(encoding='utf-8'))
        funding[symbol] = [Funding(int(r['fundingTime']), D(r['fundingRate']), D(r['markPrice'])) for r in raw]
        verify_funding(funding[symbol], WARM_START, END)
        r = rule_data[symbol]
        rules[symbol] = MarketRules(D(r['tick_size']), D(r['qty_step']), D(r['min_qty']),
            D(r['min_notional']), D(r['max_qty']), tuple(MarginTier(D(t[0]), D(t[1]), int(t[2]),
                                                                       D(t[3]), D(t[4])) for t in r['tiers']))
    return markets, funding, rules, manifest


def render_report(report):
    lines = ['# BTC / ETH 固定分支策略历史回测', '',
             '固定参数未经调优。每资产每个独立窗口以100 USDT空仓开始；连续窗口另以100 USDT开始，不在周界重置。', '',
             '时区为上海UTC+8，四期为2026-09-03→09-10→09-17→09-24→10-01，每期7天。', '',
             '策略：EMA50、EMA12、Wilder ADX14≥20的前12小时突破；否则SMA30±1.5总体标准差、Wilder RSI14<45/>55、距中轨>0.2%。止损5ATR、追踪6ATR，50倍全仓，计划风险预算50%、名义上限15倍权益。所有指标共用实盘纯数学实现。', '',
             '| 场景 | 资产 | 窗口 | 交易数 | 净收益U | 收益率 | 最大回撤(标记价5m收盘) | 手续费U | 资金费U |',
             '|---|---|---|---:|---:|---:|---:|---:|---:|']
    for r in report['metrics']:
        lines.append(f"| {r['scenario']} | {r['symbol']} | {r['window_label']} | {r['trades']} | "
                     f"{float(r['net_profit_usdt']):.4f} | {float(r['return_pct']):.2f}% | "
                     f"{float(r['max_drawdown_mark_close_pct']):.2f}% | {float(r['fees_usdt']):.4f} | {float(r['funding_usdt']):.4f} |")
    lines.extend(['', '验收要求：8个独立资产窗口至少7个净盈利，BTC四期净收益合计与ETH四期合计均为正，BTC与ETH连续28天均为正；触及强平损失界限的场景不能通过。'])
    for a in report['acceptance']:
        lines.append(f"\n- {a['scenario']}：{'通过' if a['passed'] else '未通过'}；独立盈利窗口 {a['positive_independent_windows']}/8；"
                     f"BTC四期合计 {float(a['independent_net_sums_usdt']['BTCUSDT']):.4f}U，"
                     f"ETH四期合计 {float(a['independent_net_sums_usdt']['ETHUSDT']):.4f}U。")
    lines.extend(['', '成本：基线每边手续费0.05%、不利滑点0.02%；另设每边不利滑点0.05%与0.10%两个独立压力场景，各场景手续费仍为每边0.05%。价格中已经包含滑点，账本不重复扣减。', '',
                  '5分钟成交延迟场景假设订单在有效窗口内提交、下一根5分钟线开盘成交；开仓数量在提交时冻结，实际成交超限后只减仓。EMA12/SMA30退出同样延迟，交易所价格止损与追踪更新不延迟。市价单正常成交不代表需要等5分钟，此项仅作压力敏感性。', '',
                  '提交本身延后5分钟则超过[小时边界,边界+5分钟)窗口，全部信号作废；该场景单独报告，不能用于声称策略有收益。', '',
                  '以下限制是结果解释的一部分：', ''])
    lines.extend('- ' + item for item in report['limitations'])
    lines.extend(['', '公开数据依据：[币安合约市场数据文档](' + DOCS + ')；资金费使用每条fundingTime、fundingRate、特定结算markPrice，不用平均费率或成交价替代。', '',
                  '详见 metrics.csv、各场景trades.csv与equity.csv、summary.json和data-evidence.json。所有输入和策略源码均记录SHA-256。'])
    return '\n'.join(lines) + '\n'


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--offline-filters', action='store_true', help='Use the preserved public Oct-7 filter snapshot')
    parser.add_argument('--refresh-funding', action='store_true', help='Download actual public settlement events instead of audited cache')
    args = parser.parse_args(argv)
    DATA.mkdir(parents=True, exist_ok=True)
    OUTPUT.mkdir(parents=True, exist_ok=True)
    public, evidence = PublicHistory(), []
    try:
        if (DATA / 'study-inputs.json').is_file() and not args.refresh_funding:
            markets, funding, rules, replay_manifest = load_materialized()
            evidence = replay_manifest['source_evidence']
            rule_evidence = replay_manifest['rule_evidence']
        else:
            rules, rule_evidence = load_rules(public, args.offline_filters)
            markets = {symbol: verified_market(symbol, public, evidence) for symbol in ('BTCUSDT', 'ETHUSDT')}
            funding = {symbol: verified_funding(symbol, public, evidence, args.refresh_funding) for symbol in markets}
            materialize(markets, funding, rules, evidence, rule_evidence, public.sources)
        all_metrics = []
        for symbol, data in markets.items():
            for index, (start, end, kind) in enumerate(
                [(START + i * WEEK, START + (i + 1) * WEEK, 'independent_7d') for i in range(4)]
                + [(START, END, 'continuous_28d')]):
                hours = [h for h in data['hours'] if start - 1000 * HOUR_MS <= h.open_ms < end]
                analyses = prepare_analyses(hours, end)
                bars = [h for h in data['futures'] if start <= h.open_ms < end]
                marks = [h for h in data['mark'] if start <= h.open_ms < end]
                window = f"week{index + 1}" if kind == 'independent_7d' else 'continuous28d'
                for scenario in SCENARIOS:
                    model = Simulator(symbol, start, end, rules[symbol], scenario)
                    result = model.run(bars, marks, hours, funding[symbol], analyses)
                    metric = result['metrics']
                    metric.update(window_type=kind, window_label=f"{iso(start)[:10]} → {iso(end)[:10]}",
                                  start_shanghai=iso(start), end_exclusive_shanghai=iso(end))
                    all_metrics.append(metric)
                    directory = OUTPUT / f'{symbol}-{window}-{scenario.name}'
                    directory.mkdir(exist_ok=True)
                    dump(directory / 'result.json', result)
                    csv_rows(directory / 'trades.csv', result['trades'])
                    csv_rows(directory / 'equity.csv', result['equity_curve'])
                    print(symbol, window, scenario.name, 'trades=', metric['trades'],
                          'netU=', round(float(metric['net_profit_usdt']), 4), flush=True)
        source_files = ['execution/strategy.py', 'research/regime_backtest.py', 'tools/backtest_regime.py']
        report = {'created_at_utc': datetime.now(timezone.utc).isoformat(), 'private_api_used': False,
                  'real_orders': False, 'initial_equity_per_asset_usdt': 100,
                  'strategy_parameters': {'ema_trend': 50, 'ema_exit': 12, 'adx': 14, 'adx_threshold': 20,
                    'breakout': 12, 'boll_window': 30, 'boll_population_std': 1.5, 'rsi': 14,
                    'rsi_long_lt': 45, 'rsi_short_gt': 55, 'mid_distance_gt': .002,
                    'initial_stop_atr': 5, 'trailing_stop_atr': 6, 'budget_fraction': .5,
                    'notional_multiple': 15, 'leverage': 50, 'warmup_hours': 1000},
                  'metrics': all_metrics, 'acceptance': [acceptance(all_metrics, s.name) for s in SCENARIOS],
                  'source_sha256': {p: sha(ROOT / p) for p in source_files},
                  'limitations': [
                    '合约与标记价格OHLC严格连续校验。1小时指标由实际合约5m聚合；标记5m由实际标记1m聚合。源缓存为公开归档且校验过归档checksum，当前文件hash再次核对。',
                    '历史订单簿、成交排队、部分成交和实际市价单成交延迟不可重建。此研究是5m OHLC完整成交场景；不是历史真实账户订单重放。',
                    '已生效止损跳空按不利的5m开盘价退出，线内触发按止损价退出后加一次不利滑点。线内第一次触达时间未知，模型记在该线末尾；同线资金费/止损先后与标记价强平/合约止损先后单独统计歧义。',
                    '边界市价成交假定提交后100ms；资金费按原始结算毫秒、模型当时的数量及原始settlement mark记账。资金费不参与入场筛选。',
                    '全仓维持保证金按保存的币安档位逐5m标记极值检查；历史与账户专属档位变化和历史交易规则未重建，因此不能声称历史实盘保证金规则精确复现。',
                    '若5m标记极值触及维持保证金，采用该资产钱包归零的保守损失界限并停止该回测窗口；不是伪造强平成交/强平手续费。发生该情况的结果不通过验收。',
                    '最大回撤主值以标记价5m收盘权益计算，额外报告线内不利标记极值回撤界限；OHLC内两个市场的先后次序无法确定。',
                    '独立周末和连续研究末尾均按最后5m收盘价、不利滑点及手续费强制平仓；不把未实现利润当成已实现净收益。',
                    '回测资金按每资产100 USDT独立核算；结果不表示两个品种共用一个100U钱包。参数、窗口和成本场景在运行前固定，未根据结果调整。',
                  ]}
        dump(DATA / 'data-evidence.json', {'inputs': evidence, 'rules': rule_evidence,
                                          'new_public_get_requests': public.sources,
                                          'range_start_ms': WARM_START, 'end_exclusive_ms': END,
                                          'portable_input_manifest_sha256': sha(DATA / 'study-inputs.json'),
                                          'actual_funding_documentation': DOCS})
        report['data_evidence_sha256'] = sha(DATA / 'data-evidence.json')
        dump(OUTPUT / 'summary.json', report)
        csv_rows(OUTPUT / 'metrics.csv', all_metrics)
        (OUTPUT / '回测报告.zh-CN.md').write_text(render_report(report), encoding='utf-8')
        # Preserve earlier generated single-cost attempts, but keep them out of
        # the active per-scenario directories. All paths are verified in OUTPUT.
        old_attempt = OUTPUT / 'superseded-single-stress-attempt'
        for symbol in ('BTCUSDT', 'ETHUSDT'):
            for window in ('week1', 'week2', 'week3', 'week4', 'continuous28d'):
                previous = OUTPUT / f'{symbol}-{window}-slippage_stress'
                if previous.is_dir():
                    target = old_attempt / previous.name
                    if previous.resolve().parent != OUTPUT.resolve() or not target.resolve().is_relative_to(OUTPUT.resolve()):
                        raise ValueError('generated_report_archive_outside_output')
                    old_attempt.mkdir(exist_ok=True)
                    previous.rename(target)
        if (OUTPUT / 'failure.json').is_file():
            failures = OUTPUT / 'resolved-attempts'
            failures.mkdir(exist_ok=True)
            (OUTPUT / 'failure.json').rename(failures / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S') + '.json'))
        print('报告:', str(OUTPUT / '回测报告.zh-CN.md'))
        print('baseline_acceptance:', report['acceptance'][0]['passed'])
        return 0
    except Exception as exc:
        failure = {'failed_at_utc': datetime.now(timezone.utc).isoformat(), 'completed': False,
                   'private_api_used': False, 'real_orders': False, 'error_type': type(exc).__name__,
                   'error': str(exc), 'public_requests_completed': public.sources,
                   'data_substituted_or_synthesized': False}
        dump(OUTPUT / 'failure.json', failure)
        print(json.dumps(failure, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
