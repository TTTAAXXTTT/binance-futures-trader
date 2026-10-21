"""Durable hourly execution. Importing does not connect or place orders.

Durable human intent permits entries after verified automatic recovery. Prices for stops are CONTRACT_PRICE;
equity and margin are read from mark-valued exchange account snapshots.
"""
import hashlib
import copy
import math
import threading
import time
import uuid
from dataclasses import asdict
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

from .binance import ApiError, OrderUncertain, RequestExpired
from .request_budget import NETWORK_RETRY_SECONDS
from .funding import read_funding
from .store import funding_time_equivalent
from .diagnostics import trace_stage
from .stop_replacement import StopReplacementMixin
from .entry_candidates import EntryCandidateMixin
from .strategy import (Candle, MarketRules, MarginTier, analyze, plan_entry, initial_stop,
    tighten_initial_stop, trailing_stop, branch_exit, execution_window_open,
    entry_deviation_allowed, floor_step, risk_at_quantity, reduction_target,
    stop_touched, order_rule_reason, adverse_execution_price, HOUR_MS)

D = lambda value: Decimal(str(value))
SYMBOLS = ('BTCUSDT', 'ETHUSDT')
STRATEGY = 'HourlyTrendRange'
STRATEGY_VERSION = 1
EXECUTION_RECOVERY_VERSION = '2026-10-10-confirmed-fill-protection-v1'
SOFTWARE_RELEASE = '2026-10-15-editable-strategy-equity-v9'
AUTOMATIC_RECOVERY_VERSION = 'automatic-recovery-v1'
INTENT_META = 'trading_intent_v1'
START_REQUEST_META = 'trading_start_request_v1'
RECOVERY_META = 'execution_recovery_v1'
PROTECTION_CONFIRMATION_VERSION = 'original-algo-query-v2'
LEDGER_META = 'hourly_trend_range_ledgers_v1'
STOP_CAPABILITY_META = 'close_all_overlap_capability_v1'
INITIAL_STRATEGY_WALLET = D('100')
FEE, SLIP = D('.0005'), D('.0002')
TERMINAL = {'FILLED', 'CANCELED', 'EXPIRED', 'EXPIRED_IN_MATCH', 'REJECTED'}
ALGOTERMINAL = {'CANCELED', 'EXPIRED', 'REJECTED', 'FINISHED'}


class ExecutionFault(RuntimeError):
    pass


def truth(value):
    return value is True or str(value).lower() == 'true'


def false(value):
    return value is False or str(value).lower() == 'false'


def candles_from(raw):
    return [Candle(int(r[0]), D(r[1]), D(r[2]), D(r[3]), D(r[4]), int(r[6])) for r in raw]


from .editable_equity import EditableEquityMixin
from .equity_adjustments import adjustment_summary


class Engine(EditableEquityMixin, EntryCandidateMixin, StopReplacementMixin):
    def __init__(self, client, store, activation_check=lambda: False, *, monotonic=time.monotonic,
                 restore_saved_intent=False, diagnostics=None):
        self.client, self.store, self.activation_check = client, store, activation_check
        self.diagnostics = diagnostics
        self.lock = threading.RLock()
        self.state = 'stopped'
        intent = store.meta(INTENT_META)
        valid_intent = (isinstance(intent, dict) and intent.get('version') == 1 and
            type(intent.get('desired_running')) is bool and type(intent.get('updated_at_ms')) is int and
            intent.get('source') in {'human_start', 'human_pause', 'human_exit_all'})
        self.desired_running = bool(restore_saved_intent and valid_intent and intent['desired_running'])
        self._recovery = {'version': AUTOMATIC_RECOVERY_VERSION, 'pending': False,
            'kind': None, 'reason': None, 'started_at_ms': None, 'last_failure_at_ms': None,
            'attempts': 0, 'next_retry_at_ms': 0, 'last_recovered_at_ms': None}
        self._recovery_next_probe = 0.0
        self._fault_generation = 0
        self._control_generation = 0
        self.last_successful_reconcile_ms = None
        self._account_snapshot = None
        self._account_snapshot_monotonic = None
        self.fault_reason = store.meta('fault_reason')
        self.rules, self.price_filters, self.analyses, self.markets = {}, {}, {}, {}
        self.started_ms = client.now_ms()
        self.last_boundary = self.started_ms // HOUR_MS * HOUR_MS
        self._startup_clock_pending = bool(restore_saved_intent)
        self._hour_completed = {}
        self._pending_hour_boundary = self.last_boundary
        self._hour_entry_eligible = {}
        self._rules_boundary = None
        self._candle_poll_after = {}
        self._catchup_exits_pending = bool(restore_saved_intent and valid_intent)
        self.symbol_enabled = self._read_symbols()
        saved_after = store.meta('symbol_enabled_after', {})
        self._default_activation_symbols = {symbol for symbol in SYMBOLS if
            not isinstance(saved_after, dict) or type(saved_after.get(symbol)) is not int}
        self.symbol_enabled_after = {
            symbol: saved_after.get(symbol, self.last_boundary)
            if isinstance(saved_after, dict) and type(saved_after.get(symbol)) is int
            else self.last_boundary for symbol in SYMBOLS
        }
        self.account, self.positions = {}, {}
        self.reconciled = False
        self.reconciliation_reason = None
        self.last_income = 0
        self.funding_replay = None
        self._monotonic = monotonic
        self.income_failures = 0
        self.income_retry_at = 0.0
        self.funding_pending_recovery = bool(store.meta('funding_read_pending', False))
        self.last_full_reconcile_ms = None
        self.strategy_ledgers = {}
        self.ledger_problems = []
        if restore_saved_intent and valid_intent:
            self.state = 'recovering' if self.desired_running else 'paused'
            if self.desired_running:
                self._recovery.update(pending=True, kind='restart_reconciliation',
                    reason='进程恢复，按已保存的交易意图核对实盘状态', started_at_ms=self.started_ms)
            self.store.event(self.started_ms, '执行引擎恢复用户交易意图，先核对真实状态',
                {'desired_running': self.desired_running, 'symbol_enabled': self.symbol_enabled})
        else:
            self.store.event(self.started_ms, '新执行引擎停止状态启动，等待人工启用', {})

    def _persist_intent(self, desired, source):
        self._control_generation += 1
        self.desired_running = bool(desired)
        self.store.set_meta(INTENT_META, {'version': 1, 'desired_running': self.desired_running,
            'updated_at_ms': self.client.now_ms(), 'source': source})

    @staticmethod
    def _exception_kind(exc):
        for _ in range(8):
            if getattr(exc, 'recovery_kind', None):
                return exc.recovery_kind, exc
            parent = getattr(exc, '__cause__', None)
            if parent is None or parent is exc:
                break
            exc = parent
        return 'execution_review', exc

    def fault(self, reason, *, error=None, kind=None):
        classified, cause = self._exception_kind(error if error is not None else reason)
        kind = kind or classified
        if self.diagnostics is not None:
            self.diagnostics.record_fault(error, kind)
        now = self.client.now_ms()
        if self.fault_reason != str(reason):
            self.store.event(now, '执行异常暂停新开仓并自动处理', {'reason': str(reason), 'kind': kind})
        self.fault_reason = str(reason)
        self.store.set_meta('fault_reason', self.fault_reason)
        self._fault_generation += 1
        delay = getattr(cause, 'retry_in_seconds', None)
        if delay is None or (float(delay) <= 0 and kind in {'permanent', 'credentials_error', 'exchange_rejected'}):
            delay = (NETWORK_RETRY_SECONDS if kind in {'permanent', 'credentials_error', 'exchange_rejected', 'network_read_failure'}
                     else 30 if kind == 'execution_review' else 2)
        delay = max(0.0, float(delay))
        self._recovery_next_probe = self._monotonic() + delay
        self._recovery.update(pending=True, kind=kind, reason=self.fault_reason,
            started_at_ms=self._recovery['started_at_ms'] if self._recovery['pending'] else now,
            last_failure_at_ms=now, attempts=self._recovery['attempts'] + 1,
            next_retry_at_ms=now + round(delay * 1000))
        self.store.set_meta(RECOVERY_META, self._recovery)
        if self.state not in {'stopped', 'paused'}:
            self.state = ('waiting' if kind in {'local_budget_wait', 'exchange_cooldown',
                'network_read_failure', 'network_read_wait'} else 'recovering') if self.desired_running else 'fault'

    def handle_loop_failure(self, exc):
        with self.lock:
            self.reconciled = False
            self.fault('执行循环异常，保留用户意图并重新核对：' + str(exc), error=exc)

    def _recovery_snapshot(self):
        result = dict(self._recovery)
        result['retry_in_seconds'] = max(0, round(self._recovery_next_probe - self._monotonic(), 1))
        return result

    def connection_check_token(self):
        """Published observation token, never permission to trade."""
        return {'fault_generation': self._fault_generation,
                'control_generation': self._control_generation,
                'symbols': dict(self.symbol_enabled)}

    def accept_connection_check(self, notification):
        """Only schedule the existing recovery pass earlier; keep every gate."""
        with self.lock:
            context = notification.get('context', {})
            if context.get('token') != self.connection_check_token():
                return False
            transport = notification.get('transport') or {}
            network = getattr(self.client, 'network_status', lambda: {})()
            if (transport.get('completed') is not True or
                    transport.get('generation') != network.get('generation') or
                    network.get('recovering') or network.get('probing')):
                return False
            if (not self.desired_running or not self._human_intent_still_allows() or
                    not self._recovery['pending'] or self._recovery['kind'] not in
                    {'network_read_failure', 'network_read_wait'}):
                return False
            acknowledge = getattr(self.client, 'acknowledge_manual_connection_check', None)
            if callable(acknowledge) and not acknowledge(transport):
                return False
            now = self.client.now_ms()
            self._recovery_next_probe = self._monotonic()
            self._recovery['next_retry_at_ms'] = now
            self.store.set_meta(RECOVERY_META, self._recovery)
            # Funding will be read and reconciled again by the normal loop.
            if self.funding_pending_recovery:
                self.income_retry_at = self._monotonic()
            return True

    def _verify_resume_conditions(self):
        """GET-only verification: never reconfigure leverage or invent a new wallet."""
        if not self.activation_check():
            raise ExecutionFault('策略安装设置未通过核对')
        if not self.reconciled or self.last_successful_reconcile_ms is None or self._blocked_orders():
            raise ExecutionFault(self.reconciliation_reason or '真实订单与仓位尚未确定')
        if self.funding_pending_recovery:
            raise ExecutionFault('资金费读取尚未恢复，等待核对账本权益')
        self.symbol_enabled = self._read_symbols()
        enabled = [s for s in SYMBOLS if self.symbol_enabled[s]]
        for symbol in enabled:
            if not self.analyses.get(symbol) or not self.analyses[symbol].ready:
                raise ExecutionFault(symbol + ' 完整小时指标尚未预热')
            ledger = self.strategy_ledgers.get(symbol)
            if not ledger or not ledger.get('verified'):
                raise ExecutionFault(symbol + ' 独立策略账本尚未确认，禁止重置本金')
        if self.ledger_problems:
            raise ExecutionFault('; '.join(self.ledger_problems))
        modes = self._fresh_account_configuration()
        if not truth(modes.get('canTrade')):
            raise ExecutionFault('账户交易权限未恢复')
        if not false(modes.get('dualSidePosition')) or not false(modes.get('multiAssetsMargin')):
            raise ExecutionFault('账户必须为USDT单资产、单向持仓')
        for symbol in enabled:
            cfg = next((r for r in self.client.symbol_config(symbol) if r.get('symbol') == symbol), None)
            if not cfg or str(cfg.get('marginType')).upper() != 'CROSSED' or int(cfg.get('leverage', 0)) != 50:
                raise ExecutionFault(symbol + ' 50倍全仓设置未通过核对')
            # Confirm actual executable prices; a socket connected flag alone is insufficient.
            book = self.client.book_ticker(symbol)
            prices = [D(book['bidPrice']), D(book['askPrice']),
                D(self.client.contract_price(symbol)['price']), D(self.client.mark_price(symbol)['markPrice'])]
            if any(not price.is_finite() or price <= 0 for price in prices):
                raise ExecutionFault(symbol + ' 行情报价尚未恢复')
        for trade in self._trades(True):
            if trade.get('exit_intent') or trade.get('unpriced_fee') or trade.get('protection_pending_position'):
                raise ExecutionFault('已登记交易的退出、保护或费用核对尚未完成')
            if D(trade.get('quantity', 0)) > 0:
                if self._effective_stop(trade)[0] is None or not self._owned_quantity(trade):
                    raise ExecutionFault('已有持仓保护或成交数量尚未确认')
                if ('planned_remaining_risk' not in trade or
                    not D(trade['planned_remaining_risk']).is_finite() or
                    D(trade['planned_remaining_risk']) > D(trade['budget0']) or
                    D(trade['quantity']) * D(trade['average']) > D(trade['notional0']) or
                    D(trade.get('actual_cost', 0)) >= D(trade['budget0'])):
                    raise ExecutionFault('成交后预算调整尚未完成')

    @trace_stage('account_modes')
    def _fresh_account_configuration(self):
        """Fresh endpoint includes modes; older adapters fall back only if absent."""
        config = dict(self.client.account_config())
        if 'dualSidePosition' not in config:
            config['dualSidePosition'] = self.client.position_mode().get('dualSidePosition')
        if 'multiAssetsMargin' not in config:
            config['multiAssetsMargin'] = self.client.multi_assets_mode().get('multiAssetsMargin')
        if self.diagnostics is not None:
            self.diagnostics.observe_account(config)
        return config

    @trace_stage('resume_checks')
    def _maybe_resume(self, *, fresh_reconcile, generation):
        if not self.desired_running or not self._recovery['pending'] or not fresh_reconcile:
            return
        if any(self._replacement_pending(t) for t in self._trades()):
            return  # Targeted protection phases own recovery; avoid full-sweep/error churn.
        if generation != self._fault_generation:
            return  # A fresh fault in this pass needs a new verified pass.
        if not self._human_intent_still_allows():
            return
        request = self.store.meta(START_REQUEST_META)
        if isinstance(request, dict) and request.get('version') == 1 and request.get('pending') is True:
            # Continue the saved human start, not a new automatic permission.
            # Every retry reads real configuration before any setting write;
            # existing ledger epochs and balances are never reinitialized.
            self._start_once(self._read_symbols())
            self.store.event(self.client.now_ms(), '连接恢复，已完成用户先前的启动请求', {})
            return
        self._verify_resume_conditions()
        if not self._catchup_position_exits():
            return
        if any(t.get('exit_intent') or self._replacement_pending(t) for t in self._trades(True)):
            return
        now = self.client.now_ms()
        previous = dict(self._recovery)
        self.fault_reason = None
        self.store.set_meta('fault_reason', None)
        self._recovery.update(pending=False, kind=None, reason=None, next_retry_at_ms=0,
            last_recovered_at_ms=now)
        self._recovery_next_probe = 0.0
        self.store.set_meta(RECOVERY_META, self._recovery)
        self.state = 'running'
        # Only an already qualified, still-unsubmitted original-window candidate
        # may survive recovery. No new or missed signal becomes eligible.
        boundary = self._time_upper_ms() // HOUR_MS * HOUR_MS
        for symbol in SYMBOLS:
            if self._recoverable_entry_candidate(symbol, boundary):
                continue
            self.symbol_enabled_after[symbol] = max(self.symbol_enabled_after[symbol], boundary)
            self.store.claim_signal(symbol, boundary, {'direction': 0, 'time_ms': now,
                'skip_reason': '异常恢复，本小时信号不补用，等待新的小时收盘'})
        self.store.set_meta('symbol_enabled_after', self.symbol_enabled_after)
        self.store.event(now, '执行异常已核对恢复，自动继续原交易意图',
            {'previous_kind': previous['kind'], 'symbol_enabled': self.symbol_enabled})

    def snapshot(self):
        with self.lock:
            self.symbol_enabled = self._read_symbols()
            enabled = [symbol for symbol in SYMBOLS if self.symbol_enabled[symbol]]
            trades = self.store.trades()
            current = [t for t in trades if self._current_trade(t)]
            active = [t for t in current if t['status'] != 'closed']
            return {'connected': True, 'state': self.state, 'strategy': STRATEGY,
                    'strategy_version': STRATEGY_VERSION,
                    'execution_recovery_version': EXECUTION_RECOVERY_VERSION,
                    'software_release': SOFTWARE_RELEASE,
                    'execution_diagnostics': self.diagnostics.snapshot() if self.diagnostics is not None else None,
                    'protection_confirmation_version': PROTECTION_CONFIRMATION_VERSION,
                    'strategy_ledgers': self._ledger_snapshot(),
                    'trades': active, 'history': list(reversed(current[-100:])),
                    'legacy_history': list(reversed([t for t in trades if not self._current_trade(t)][-100:])),
                    'events': self.store.events(),
                    'markets': self._display_markets(),
                    'server_time_ms': self.client.now_ms(),
                    'server_time_upper_ms': self._time_upper_ms(),
                    'unknown_orders': self._blocked_orders(),
                    'connection_check_unknown_orders': self.connection_check_orders(),
                    'fault_reason': self.fault_reason, 'symbol_enabled': dict(self.symbol_enabled),
                    'desired_running': self.desired_running,
                    'connection_check_token': self.connection_check_token(),
                    'entry_candidates': self.store.entry_candidates()[-100:],
                    'execution_recovery': self._recovery_snapshot(),
                    'last_successful_reconcile_ms': self.last_successful_reconcile_ms,
                    'account_snapshot': dict(copy.deepcopy(self._account_snapshot),
                        age_seconds=max(0, self._monotonic() - self._account_snapshot_monotonic))
                        if self._account_snapshot is not None else None,
                    'reconcile_interval_seconds': 30,
                    'exchange_clock': self.client.clock_status() if callable(getattr(self.client, 'clock_status', None)) else None,
                    'network_retry': self.client.network_status() if callable(getattr(self.client, 'network_status', None)) else None,
                    'connection_pool': self.client.connection_status() if callable(getattr(self.client, 'connection_status', None)) else None,
                    'funding_read': {'pending_recovery': self.funding_pending_recovery,
                        'replay': copy.deepcopy(self.funding_replay),
                        'failures': self.income_failures,
                        'interval_seconds': NETWORK_RETRY_SECONDS,
                        'retry_in_seconds': max(0, round(self.income_retry_at - self._monotonic(), 1))},
                    'market_stream': self.client.streams.snapshot() if getattr(self.client, 'streams', None) else None,
                    'warmed_up': bool(enabled) and all(
                    self.analyses.get(symbol) is not None and self.analyses[symbol].ready
                    for symbol in enabled), 'reconciled': self.reconciled,
                    'reconciliation_reason': self.reconciliation_reason}

    def _display_markets(self):
        from .status_view import signal_windows
        return signal_windows(self.markets, self.client.now_ms(), self._time_upper_ms())

    def _hour_data_ready(self, symbol, boundary):
        result = self.analyses.get(symbol)
        if result is None or not result.ready:
            return False
        if result.signal is not None:
            return result.signal.boundary_ms == boundary
        rows = self.store.candles(symbol)
        return bool(rows and int(rows[-1][0]) + HOUR_MS == boundary)

    def _ledger_records(self):
        records = self.store.meta(LEDGER_META, {})
        if not isinstance(records, dict) or any(symbol not in SYMBOLS for symbol in records):
            raise ExecutionFault('独立策略账本初始化记录无效，禁止重置资金')
        for symbol, record in records.items():
            if (not isinstance(record, dict) or record.get('strategy') != STRATEGY or
                record.get('version') != STRATEGY_VERSION or not record.get('epoch') or
                type(record.get('initialized_at_ms')) is not int or
                type(record.get('activation_boundary_ms')) is not int or
                D(record.get('initial_wallet', 0)) != INITIAL_STRATEGY_WALLET):
                raise ExecutionFault(symbol + ' 独立策略账本记录无法核对，禁止重新初始化')
        return records

    def _initialize_strategy_ledgers(self, symbols):
        """Human start or an explicit equity edit establishes the historical allocation once."""
        records = self._ledger_records()
        now = self.client.now_ms()
        for symbol in symbols:
            if symbol not in records:
                records[symbol] = {'strategy': STRATEGY, 'version': STRATEGY_VERSION,
                    'epoch': self._id('ledger', symbol + ':' + str(now)),
                    'initialized_at_ms': now, 'activation_boundary_ms': self._time_upper_ms() // HOUR_MS * HOUR_MS,
                    'initial_wallet': str(INITIAL_STRATEGY_WALLET), 'wallet': str(INITIAL_STRATEGY_WALLET)}
        self.store.set_meta(LEDGER_META, records)
        self._rebuild_strategy_ledgers()

    def _current_trade(self, trade, records=None):
        records = self._ledger_records() if records is None else records
        record = records.get(trade.get('symbol'))
        return bool(record and trade.get('strategy_id') == STRATEGY and
            trade.get('strategy_version') == STRATEGY_VERSION and
            trade.get('ledger_epoch') == record['epoch'])

    def _trades(self, active_only=False):
        records = self._ledger_records()
        return [trade for trade in self.store.trades(active_only) if self._current_trade(trade, records)]

    def _orders(self):
        owned = {trade['id'] for trade in self._trades()}
        return [order for order in self.store.orders() if order.get('trade_id') in owned]

    def connection_check_orders(self):
        """Normalize original query identities in the published snapshot only."""
        rows = []
        for order in self._blocked_orders():
            target = self.store.order(order.get('target_id')) if order.get('kind') == 'cancel' else order
            if (not target or target.get('kind') not in {'market', 'stop'} or
                    target.get('symbol') not in SYMBOLS or
                    not (target.get('client_id') or target.get('order_id'))):
                rows.append({'identity_unavailable': True})
                continue
            rows.append({key: target.get(key) for key in ('kind', 'symbol', 'client_id', 'order_id')})
        return rows

    def _ledger_snapshot(self):
        result = {}
        for symbol in SYMBOLS:
            value = dict(self.strategy_ledgers.get(symbol, {}))
            if not value:
                value = {'strategy': STRATEGY, 'version': STRATEGY_VERSION,
                    'initial_wallet': str(INITIAL_STRATEGY_WALLET), 'initialized': False,
                    'wallet': None, 'unrealized': '0', 'equity': None, 'verified': False,
                    'allocation_revision': 0, 'allocation_adjustment': '0', 'last_equity_adjustment': None}
            result[symbol] = value
        return result

    @staticmethod
    def _money(value):
        value = D(value)
        if not value.is_finite():
            raise ExecutionFault('策略账本金额不是有限数值')
        return value

    @trace_stage('strategy_ledgers')
    def _rebuild_strategy_ledgers(self):
        """Rebuild wallets from immutable fill/income IDs; account balances never overwrite them."""
        records = self._ledger_records()
        trades = self.store.trades()
        owned = {trade['id']: trade for trade in trades if self._current_trade(trade, records)}
        fills_by_trade = {identifier: [] for identifier in owned}
        all_fills = self.store.fills()
        time_observations = self.store.meta('funding_timestamp_observations_v1', {})
        problems = []
        adjustments = self.store.equity_adjustments()
        if any(row.get('symbol') not in records for row in adjustments):
            raise ExecutionFault('发现无对应独立账本的权益调整流水，禁止重置或忽略')
        balances = {symbol: dict(record, initialized=True, wallet=record['initial_wallet'],
            unrealized='0', equity=record['initial_wallet'], verified=True,
            realized_pnl='0', actual_fees='0', funding='0', fill_count=0, funding_count=0)
            for symbol, record in records.items()}
        realized = {symbol: D(0) for symbol in records}
        fees = {symbol: D(0) for symbol in records}
        funding = {symbol: D(0) for symbol in records}
        for row in all_fills:
            trade = owned.get(row.get('_trade_id'))
            if trade is None:
                continue  # Preserve all older strategies without counting their PnL a second time.
            symbol = trade['symbol']
            try:
                if (row.get('_purpose') not in {'entry', 'exit', 'reduce', 'stop_exit'} or
                    int(row['time']) < records[symbol]['initialized_at_ms'] or
                    (row.get('symbol') and row['symbol'] != symbol)):
                    raise ExecutionFault('成交不属于已确认的新策略启用边界')
                fee = self._money(row['commission'])
                if row.get('commissionAsset') != 'USDT' and fee != 0:
                    raise ExecutionFault('实际手续费资产不是USDT，不能猜测独立账本金额')
                realized[symbol] += self._money(row['realizedPnl'])
                fees[symbol] += fee
                fills_by_trade[trade['id']].append(row)
                balances[symbol]['fill_count'] += 1
            except (ExecutionFault, KeyError, ValueError, ArithmeticError) as exc:
                problems.append(symbol + ' 策略成交资金归属待核对：' + str(exc))
                balances[symbol]['verified'] = False
        for rows in fills_by_trade.values():
            rows.sort(key=lambda row: (int(row['time']), int(row['id'])))
        for row in self.store.incomes():
            symbol = row.get('symbol')
            if symbol not in records or row.get('incomeType') != 'FUNDING_FEE':
                continue
            try:
                settled = int(row['time'])
                if not isinstance(time_observations, dict):
                    raise ExecutionFault('资金费时间精度审计元数据无效')
                identity = str(row.get('tranId')) + ':' + row['symbol'] + ':' + row['asset']
                observation = time_observations.get(identity)
                if observation is not None:
                    if (not isinstance(observation, dict) or observation.get('stored_time_ms') != settled or
                            not funding_time_equivalent(row, dict(row, time=observation.get('observed_time_ms')),
                                                        records, all_fills)):
                        raise ExecutionFault('资金费时间精度与新确认成交或初始化边界交叉，保留记录待核对')
                if settled < records[symbol]['initialized_at_ms']:
                    continue
                if row.get('asset') != 'USDT':
                    raise ExecutionFault('资金费资产不是USDT')
                owners = []
                ambiguous = False
                for trade_id, trade in owned.items():
                    if trade['symbol'] != symbol:
                        continue
                    quantity = D(0)
                    for fill in fills_by_trade[trade_id]:
                        fill_time = int(fill['time'])
                        if fill_time == settled:
                            ambiguous = True
                        if fill_time >= settled:
                            break
                        quantity += self._money(fill['qty']) * (1 if fill['_purpose'] == 'entry' else -1)
                    if quantity > 0:
                        owners.append(trade_id)
                if len(owners) != 1 or ambiguous:
                    raise ExecutionFault('结算时无法唯一识别新策略实际持仓')
                funding[symbol] += self._money(row['income'])
                balances[symbol]['funding_count'] += 1
            except (ExecutionFault, KeyError, ValueError, ArithmeticError) as exc:
                problems.append(symbol + ' 策略资金费归属待核对：' + str(exc))
                balances[symbol]['verified'] = False
        for symbol, value in balances.items():
            allocation = D(0)
            try:
                summary = adjustment_summary(adjustments, symbol, value['epoch'])
                value.update(summary)
                allocation = self._money(summary['allocation_adjustment'])
            except (KeyError, ValueError, TypeError, ArithmeticError) as exc:
                problems.append(symbol + ' 权益调整流水待核对：' + str(exc))
                value['verified'] = False
            wallet = self._money(value['initial_wallet']) + allocation + realized[symbol] - fees[symbol] + funding[symbol]
            upnl = D(0)
            position = self.positions.get(symbol)
            if position:
                current = [t for t in owned.values() if t['symbol'] == symbol and t['status'] != 'closed']
                try:
                    if len(current) != 1:
                        raise ExecutionFault('真实仓位不属于唯一的新策略交易')
                    raw_upnl = position.get('unRealizedProfit', position.get('unrealizedProfit'))
                    if raw_upnl is not None:
                        upnl = self._money(raw_upnl)
                    elif position.get('markPrice') is not None:
                        upnl = (self._money(position['markPrice']) - self._money(position['entryPrice'])) * self._money(position['positionAmt'])
                    else:
                        raise ExecutionFault('缺少该品种标记价格未实现盈亏')
                except (ExecutionFault, KeyError, ValueError, ArithmeticError) as exc:
                    problems.append(symbol + ' 独立权益待核对：' + str(exc))
                    value['verified'] = False
            value.update(wallet=str(wallet), unrealized=str(upnl), equity=str(wallet + upnl),
                realized_pnl=str(realized[symbol]), actual_fees=str(fees[symbol]), funding=str(funding[symbol]))
            records[symbol].update(wallet=str(wallet), last_rebuilt_ms=self.client.now_ms())
        self.strategy_ledgers, self.ledger_problems = balances, list(dict.fromkeys(problems))
        if records:
            self.store.set_meta(LEDGER_META, records)
        return self.ledger_problems

    @staticmethod
    def _validate_symbols(symbols):
        if not isinstance(symbols, dict) or set(symbols) != set(SYMBOLS):
            raise ExecutionFault('必须分别明确BTCUSDT和ETHUSDT的启用选择，不支持其他品种')
        if any(type(symbols[symbol]) is not bool for symbol in SYMBOLS):
            raise ExecutionFault('品种启用选择必须为true或false')
        return {symbol: symbols[symbol] for symbol in SYMBOLS}

    def _read_symbols(self):
        try:
            return self._validate_symbols(self.store.meta('symbol_enabled'))
        except ExecutionFault:
            return {symbol: False for symbol in SYMBOLS}

    def _symbol_is_enabled(self, symbol):
        # Re-read the durable human choice immediately before any entry wire.
        # This also protects against a UI/metadata update during persistence.
        self.symbol_enabled = self._read_symbols()
        return self.symbol_enabled.get(symbol, False)

    def _human_intent_still_allows(self):
        intent = self.store.meta(INTENT_META)
        if intent is None:
            return True  # Legacy in-process permission; reboot never infers it.
        if not isinstance(intent, dict) or intent.get('version') != 1 or type(intent.get('desired_running')) is not bool:
            self.fault('持久化用户启停意图无效，等待重新核对')
            return False
        if not intent['desired_running']:
            self.desired_running = False
            self.state = 'paused'
            return False
        return True

    def _time_upper_ms(self):
        upper = getattr(self.client, 'now_upper_ms', None)
        return upper() if callable(upper) else self.client.now_ms()

    def _entry_window_open(self, signal):
        # A slow time-response return path leaves the calibrated clock behind
        # real time. Its upper bound must also precede the five-minute deadline.
        return (execution_window_open(signal, self.client.now_ms()) and
                self._time_upper_ms() < signal.boundary_ms + 300000)

    @trace_stage('human_symbols')
    def set_symbols(self, symbols):
        """Save local entry permissions only; existing positions keep protection/exit management."""
        with self.lock:
            selected = self._validate_symbols(symbols)
            self._control_generation += 1
            previous = self._read_symbols()
            newly_enabled = [symbol for symbol in SYMBOLS if selected[symbol] and not previous[symbol]]
            restart_required = self.state == 'running' and bool(newly_enabled)
            if restart_required and not self.desired_running:
                # Selection alone must not bypass start()'s account and 50x
                # configuration checks for a previously disabled symbol.
                self.state = 'paused'
            elif newly_enabled and self.desired_running:
                self.state = 'recovering'
                self.fault('用户新增启用品种，自动核对设置后继续', kind='symbol_activation')
                restart_required = False
            now = self.client.now_ms()
            boundary = self._time_upper_ms() // HOUR_MS * HOUR_MS
            after = dict(self.symbol_enabled_after)
            for symbol in SYMBOLS:
                if selected[symbol] and not previous[symbol]:
                    after[symbol] = boundary
            # Store the no-backfill boundary before allowing the new selection.
            self.store.set_meta('symbol_enabled_after', after)
            self.store.set_meta('symbol_enabled', selected)
            self.symbol_enabled, self.symbol_enabled_after = selected, after
            if newly_enabled and self.desired_running:
                self.store.set_meta(START_REQUEST_META, {'version': 1, 'pending': True,
                    'requested_at_ms': now, 'source': 'human_symbol_selection'})
            for symbol in SYMBOLS:
                if previous[symbol] != selected[symbol]:
                    reason = ('用户启用品种，等待下一新小时收盘' if selected[symbol] else
                              '用户停止该品种新开仓，本小时信号不补用')
                    self.store.claim_signal(symbol, boundary,
                        {'direction': 0, 'time_ms': now, 'skip_reason': reason})
            self.store.event(now, '用户分别设置品种新开仓开关',
                             {'symbol_enabled': selected, 'restart_required': restart_required})
            message = ('新增品种选择已保存，全部新开仓已暂停；请再次点击启动核对账户和50倍全仓设置，'
                       '已有持仓继续管理保护和退出' if restart_required else
                       '品种开关已保存；停止新开仓不影响已有持仓的保护和退出')
            return {'message': message,
                    'symbol_enabled': dict(selected), 'state': self.state}

    @trace_stage('start')
    def start(self, symbols=None):
        with self.lock:
            selected = self._validate_symbols(symbols) if symbols is not None else self._read_symbols()
            if not any(selected.values()):
                raise ExecutionFault('请先至少启用BTC或ETH中的一个品种')
            if not self.activation_check():
                raise ExecutionFault('尚未完成人工策略迁移，不能启用新开仓')
            try:
                return self._start_once(selected)
            except Exception as exc:
                kind, cause = self._exception_kind(exc)
                if kind not in {'local_budget_wait', 'exchange_cooldown', 'network_read_failure',
                        'network_read_wait', 'timestamp_rejected'}:
                    raise
                self.set_symbols(selected)
                self._persist_intent(True, 'human_start')
                self.store.set_meta(START_REQUEST_META, {'version': 1, 'pending': True,
                    'requested_at_ms': self.client.now_ms()})
                self.state = 'recovering'
                self.reconciled = False
                self.fault(str(exc), error=cause)
                return {'message': '启动意图已保存；按故障类型等待重试，连接核对完成后自动运行',
                    'state': self.state, 'desired_running': True,
                    'execution_recovery': self._recovery_snapshot(), 'symbol_enabled': dict(selected)}

    @trace_stage('start_checks')
    def _start_once(self, symbols=None):
        with self.lock:
            # Validate a human selection without I/O, then establish exchange
            # time before storing any activation boundary or querying account.
            selected = self._validate_symbols(symbols) if symbols is not None else self._read_symbols()
            if not any(selected.values()):
                raise ExecutionFault('请先至少启用BTC或ETH中的一个品种')
            self.client.sync_time()
            if symbols is not None:
                self.set_symbols(symbols)
            self.symbol_enabled = self._read_symbols()
            enabled = [symbol for symbol in SYMBOLS if self.symbol_enabled[symbol]]
            if not enabled:
                raise ExecutionFault('请先至少启用BTC或ETH中的一个品种')
            if self.state == 'running':
                # Adding a symbol must not grant entry permission until this
                # human start request has finished all account/mode checks.
                self.state = 'paused'
            if not self.activation_check():
                raise ExecutionFault('尚未完成人工策略迁移，不能启用新开仓')
            self.reconcile()
            if self._blocked_orders() or not self.reconciled:
                raise ExecutionFault('订单或持仓尚未确定，不能启用新开仓')
            if self.funding_pending_recovery:
                # A human recovery must finish the failed funding read before
                # clearing the fault or sizing from an independent ledger.
                self._funding(force=True)
            if any(self.analyses.get(symbol) is None or not self.analyses[symbol].ready for symbol in enabled):
                raise ExecutionFault('完整小时指标预热尚未完成')
            if any(t.get('unpriced_fee') for t in self._trades(True)):
                raise ExecutionFault('实际费用资产未完成折算核对')
            modes = self._fresh_account_configuration()
            if not truth(modes.get('canTrade')):
                raise ExecutionFault('账户交易开关尚未通过核对')
            if not false(modes.get('dualSidePosition')) or not false(modes.get('multiAssetsMargin')):
                raise ExecutionFault('需在币安设置USDT单资产、单向持仓')
            # Only this human-triggered start route may change cross/leverage.
            for symbol in enabled:
                cfg = next((r for r in self.client.symbol_config(symbol) if r['symbol'] == symbol), None)
                if not cfg:
                    raise ExecutionFault('无法核对品种保证金设置')
                correct = str(cfg.get('marginType')).upper() == 'CROSSED' and int(cfg.get('leverage', 0)) == 50
                if not correct and self.positions.get(symbol):
                    raise ExecutionFault('已有持仓的模式与50倍全仓要求不一致')
                if not correct:
                    self.client.set_cross50(symbol)
            self.refresh_rules()
            self._check_protection()
            if any(t.get('exit_intent') for t in self._trades(True)):
                raise ExecutionFault('退出意图尚未完成，先核对已有交易')
            self._initialize_strategy_ledgers(enabled)
            if self.ledger_problems:
                raise ExecutionFault('; '.join(self.ledger_problems))
            self._verify_resume_conditions()
            self.fault_reason = None
            self.store.set_meta('fault_reason', None)
            self._persist_intent(True, 'human_start')
            self.store.set_meta(START_REQUEST_META, None)
            self._recovery.update(pending=False, kind=None, reason=None, next_retry_at_ms=0)
            self._recovery_next_probe = 0.0
            self.store.set_meta(RECOVERY_META, self._recovery)
            self.state = 'running'
            # Keep the confirmed-hour processing cursor on the lower bound.
            # Only entry activation uses the upper bound, so an existing
            # position still receives the next hour's stop/indicator updates.
            self.last_boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
            self._startup_clock_pending = False
            self._hour_completed = {}
            self._pending_hour_boundary = self.last_boundary
            self._hour_entry_eligible = {}
            activation_boundary = self._time_upper_ms() // HOUR_MS * HOUR_MS
            for symbol in enabled:
                self.symbol_enabled_after[symbol] = activation_boundary
                self.store.claim_signal(symbol, activation_boundary,
                    {'direction': 0, 'time_ms': self.client.now_ms(),
                     'skip_reason': '用户启动品种，等待下一新小时收盘'})
            self.store.set_meta('symbol_enabled_after', self.symbol_enabled_after)
            self.store.event(self.client.now_ms(), '用户启用新策略，等待下一个新小时收盘',
                             {'symbol_enabled': self.symbol_enabled})
            return {'message': '所选品种已启用，等待新的小时收盘信号', 'state': self.state,
                    'symbol_enabled': dict(self.symbol_enabled)}

    @trace_stage('human_pause')
    def pause(self):
        with self.lock:
            self._persist_intent(False, 'human_pause')
            self.store.set_meta(START_REQUEST_META, None)
            self.state = 'paused'
            self.store.event(self.client.now_ms(), '用户暂停新开仓，继续处理保护和退出', {})
            return {'message': '新开仓已暂停，已有登记持仓继续管理退出',
                    'state': self.state, 'symbol_enabled': dict(self.symbol_enabled)}

    @trace_stage('human_exit')
    def exit_all(self):
        with self.lock:
            self._persist_intent(False, 'human_exit_all')
            self.store.set_meta(START_REQUEST_META, None)
            self.state = 'paused'
            self.store.event(self.client.now_ms(), '用户请求全平并停止新开仓', {})
            for trade in self._trades(True):
                self._exit_intent(trade, '用户请求全平')
            return {'message': '已记录全平意图；持续核对真实成交直到仓位归零'}

    @trace_stage('exchange_rules')
    def refresh_rules(self):
        info, brackets = self.client.exchange_info(), self.client.leverage_brackets()
        for symbol in SYMBOLS:
            row = next((r for r in info['symbols'] if r['symbol'] == symbol), None)
            if not row or row.get('status') != 'TRADING' or row.get('contractType') != 'PERPETUAL':
                raise ExecutionFault('品种不是可交易的USDT永续合约')
            if row.get('quoteAsset') != 'USDT' or row.get('marginAsset') != 'USDT':
                raise ExecutionFault('交易品种报价或保证金资产不是USDT')
            filters = {r['filterType']: r for r in row['filters']}
            lot, market = filters['LOT_SIZE'], filters.get('MARKET_LOT_SIZE', filters['LOT_SIZE'])
            steps = [D(v['stepSize']) for v in [lot, market] if D(v['stepSize']) > 0]
            scale = 10 ** max(-s.as_tuple().exponent for s in steps)
            step = D(math.lcm(*[int(s * scale) for s in steps])) / scale
            caps = [D(v['maxQty']) for v in [lot, market] if D(v['maxQty']) > 0]
            mins = [D(v['minQty']) for v in [lot, market]]
            nf = filters.get('NOTIONAL', filters.get('MIN_NOTIONAL', {}))
            band = next((r for r in brackets if r['symbol'] == symbol), None)
            if not band:
                raise ExecutionFault('缺少当前账户保证金档位')
            tiers = tuple(MarginTier(D(r['notionalFloor']), D(r['notionalCap']), int(r['initialLeverage']),
                                    D(r['maintMarginRatio']), D(r.get('cum', 0))) for r in band['brackets'])
            applicable_min = D(nf.get('minNotional', nf.get('notional', 0)))
            if nf.get('applyMinToMarket') is False or nf.get('applyToMarket') is False:
                applicable_min = D(0)
            self.rules[symbol] = MarketRules(tick_size=D(filters['PRICE_FILTER']['tickSize']),
                qty_step=step, min_qty=max(mins), min_notional=applicable_min,
                max_qty=min(caps) if caps else None, margin_tiers=tiers)
            self.price_filters[symbol] = filters['PRICE_FILTER']
            if not hasattr(self, 'notional_filters'):
                self.notional_filters = {}
            self.notional_filters[symbol] = nf

    def _unknown(self):
        return [o for o in self._orders() if o['state'] in {'prepared', 'unknown'}]

    def _position_map(self, rows):
        result = {}
        for row in rows:
            if D(row.get('positionAmt', 0)):
                if row.get('positionSide') != 'BOTH' or row['symbol'] in result:
                    raise ExecutionFault('真实持仓不符合单向单仓要求')
                result[row['symbol']] = row
        return result

    def _id(self, purpose, key=None):
        return 'hb-' + purpose[:2] + '-' + hashlib.sha256(
            str(key or uuid.uuid4()).encode()).hexdigest()[:27]

    def _pending(self, trade, purposes=None):
        return [o for o in self._orders() if o['trade_id'] == trade['id'] and
                o['kind'] == 'market' and (purposes is None or o['purpose'] in purposes) and
                (o['state'] != 'terminal' or not o.get('fills_complete', False))]

    def _blocked_orders(self):
        orders, trades = self._orders(), {t['id']: t for t in self._trades()}
        blocked = []
        for order in orders:
            waiting = order['state'] in {'prepared','unknown'} or (order['kind'] == 'market' and
                      (order['state'] != 'terminal' or not order.get('fills_complete', False)))
            if order['kind'] == 'stop' and self._stop_needs_child(order):
                waiting = True
            if order['kind'] == 'stop' and order['state'] != 'terminal':
                if trades.get(order['trade_id'], {}).get('status') == 'closed':
                    waiting = True  # A residual closePosition stop must never attach to a new trade.
                if order.get('raw', {}).get('algoStatus') in {'TRIGGERING','TRIGGERED'}:
                    child = self.store.order('child-' + order['symbol'] + '-' + str(order.get('order_id')))
                    if not child or child['state'] != 'terminal' or not child.get('fills_complete'):
                        waiting = True
            if waiting:
                blocked.append(order)
        for trade in trades.values():
            if self._replacement_pending(trade):
                blocked.append({'id': trade['id'] + '-protection-replacement',
                    'trade_id': trade['id'], 'symbol': trade['symbol'], 'kind': 'protection_replacement',
                    'state': 'pending', 'reason': trade['stop_replacement_v1'].get('reason')})
        return blocked

    def _save_response(self, intent, raw):
        intent['raw'] = raw
        identity = raw.get('algoId') if intent['kind'] == 'stop' else raw.get('orderId')
        status = raw.get('algoStatus', raw.get('status'))
        if (not identity or not status or raw.get('symbol') != intent['symbol'] or
            (intent.get('order_id') and str(identity) != str(intent['order_id']))):
            intent.update(state='unknown', error='订单回包缺少有效身份或状态')
            self.store.save_order(intent)
            raise ExecutionFault('订单回包缺少有效身份或状态，必须查询原订单')
        if intent['kind'] == 'stop':
            intent['order_id'] = raw.get('algoId', intent.get('order_id'))
            status = raw.get('algoStatus', raw.get('status', ''))
            intent['state'] = 'terminal' if status in ALGOTERMINAL else 'confirmed'
        elif intent['kind'] == 'market':
            intent['order_id'] = raw.get('orderId', intent.get('order_id'))
            intent['state'] = 'terminal' if raw.get('status') in TERMINAL else 'confirmed'
        self.store.save_order(intent)

    @trace_stage('market_intent')
    def _submit_market(self, trade, purpose, quantity):
        """Only one wire attempt per durable intent. A timeout is queried, never replayed."""
        intent = {'id': self._id(purpose), 'trade_id': trade['id'], 'symbol': trade['symbol'],
                  'kind': 'market', 'purpose': purpose, 'state': 'prepared',
                  'quantity': str(quantity), 'time_ms': self.client.now_ms(), 'fills_complete': False}
        intent['client_id'] = intent['id']
        self.store.save_order(intent)
        if purpose == 'entry':
            boundary = int(trade['signal_boundary_ms'])
            now = self.client.now_ms()
            if (self.state != 'running' or not self._human_intent_still_allows() or
                    not self._symbol_is_enabled(trade['symbol'])):
                intent.update(state='terminal', fills_complete=True, locally_not_submitted=True,
                              error='持久化后用户已停止该品种或暂停全局新开仓')
                self.store.save_order(intent)
                self.store.event(now, '品种已停止，市价请求未提交', {'trade': trade['id']})
                return intent
            if not (boundary <= now and self._time_upper_ms() < boundary + 300000):
                intent.update(state='terminal', fills_complete=True, locally_not_submitted=True,
                              error='持久化完成后开仓窗口已结束')
                self.store.save_order(intent)
                self.store.event(now, '信号过期，市价请求未提交', {'trade': trade['id']})
                return intent
        side = ('BUY' if trade['direction'] == 1 else 'SELL') if purpose == 'entry' else (
            'SELL' if trade['direction'] == 1 else 'BUY')
        try:
            timing_args = ({'deadline_ms': boundary + 300000}
                if purpose == 'entry' and callable(getattr(self.client, 'now_upper_ms', None)) else {})
            raw = self.client.market_order(trade['symbol'], side, quantity, intent['id'],
                                           reduce_only=purpose != 'entry', **timing_args)
            self._save_response(intent, raw)
        except RequestExpired as exc:
            intent.update(state='terminal', fills_complete=True, locally_not_submitted=True, error=str(exc))
            self.store.save_order(intent)
            self.store.event(self.client.now_ms(), '信号过期，市价请求未提交', {'trade': trade['id']})
        except OrderUncertain as exc:
            intent.update(state='unknown', error=str(exc))
            self.store.save_order(intent)
            self.fault('市价订单状态未确定，按原客户订单号查询', error=exc)
        except ApiError as exc:
            intent.update(state='terminal', fills_complete=True, rejected=True, error=str(exc))
            self.store.save_order(intent)
            self.fault('市价委托被拒绝：' + str(exc), error=exc)
        return intent

    def _discover_child(self, intent):
        raw = intent.get('raw', {})
        child = raw.get('actualOrderId')
        if child and str(child) != '0':
            child_id = 'child-' + intent['symbol'] + '-' + str(intent['order_id'])
            if not self.store.order(child_id):
                self.store.save_order({'id': child_id, 'client_id': None, 'kind': 'market',
                    'purpose': 'stop_exit', 'trade_id': intent['trade_id'], 'symbol': intent['symbol'],
                    'order_id': int(child), 'state': 'confirmed', 'fills_complete': False,
                    'time_ms': self.client.now_ms()})

    @staticmethod
    def _stop_needs_child(intent):
        # FINISHED confirms the conditional trigger, not the identity or fills
        # of its market child. A late actualOrderId must still be queried.
        raw = intent.get('raw', {})
        return (intent['kind'] == 'stop' and
                raw.get('algoStatus', raw.get('status')) == 'FINISHED' and
                str(raw.get('actualOrderId') or '0') == '0')

    def _confirmed_execution_quantity(self, trade):
        """Known net executions, never a substitute for positionRisk.

        Used only for closePosition protection and reduce-only emergency exits.
        Subtract confirmed exit executions even when individual fees are late.
        """
        orders = [o for o in self._orders() if o['trade_id'] == trade['id']]
        if any(o['kind'] == 'stop' and (self._stop_needs_child(o) or
                o.get('raw', {}).get('algoStatus') in {'TRIGGERING', 'TRIGGERED'}) for o in orders):
            return D(0)  # First discover/query the in-flight protective exit.
        fills = [f for f in self.store.fills(trade['symbol']) if f.get('_trade_id') == trade['id']]
        by_order = {}
        for fill in fills:
            oid = str(fill['orderId'])
            by_order[oid] = by_order.get(oid, D(0)) + D(fill['qty'])
        entry, exit_qty = D(0), D(0)
        for order in orders:
            if order['kind'] != 'market' or not order.get('order_id'):
                continue
            executed = by_order.pop(str(order['order_id']), D(0))
            if order['state'] in {'confirmed', 'terminal'} and not order.get('rejected'):
                executed = max(executed, D(order.get('raw', {}).get('executedQty', 0)))
            if order['purpose'] == 'entry':
                entry += executed
            else:
                exit_qty += executed
        # Fixtures/recovery may already contain immutable fills without the
        # ordinary intent record; preserve their known direction and quantity.
        for oid, quantity in by_order.items():
            purpose = next(f['_purpose'] for f in fills if str(f['orderId']) == oid)
            if purpose == 'entry':
                entry += quantity
            else:
                exit_qty += quantity
        return max(entry - exit_qty, D(0))

    def _protect_entry_confirmation(self, intent, *, strict_fills=False):
        """Protect verified entry execution before account/position/fee reads.

        These fields describe order/fill evidence only. Never publish them as
        a positionRisk snapshot or use them to authorize ordinary reductions.
        """
        trade = self.store.trade(intent['trade_id'])
        if not trade or trade['status'] == 'closed' or intent['purpose'] != 'entry':
            return
        fills_consistent = True
        try:
            self._rebuild_fills(trade)
        except ExecutionFault:
            if strict_fills:
                raise
            # Exit records can arrive before entry fee records. Never let a
            # preliminary reconstruction prevent reading those missing fills.
            fills_consistent = False
        trade = self.store.trade(trade['id'])
        raw = intent.get('raw', {})
        executed, average = D(raw.get('executedQty', 0)), D(raw.get('avgPrice', 0))
        fills = [f for f in self.store.fills(trade['symbol']) if f.get('_trade_id') == trade['id']]
        entry_qty = sum((D(f['qty']) for f in fills if f['_purpose'] == 'entry'), D(0))
        quantity = self._confirmed_execution_quantity(trade)
        if executed > 0 and fills_consistent and entry_qty == executed:
            average = D(trade['fill_average'])
        elif executed > 0 and average > 0:
            pass
        else:
            return  # No actual mean price/quantity has been established yet.
        if quantity <= 0 or average <= 0:
            return
        trade.update(protection_fill_quantity=str(quantity), protection_fill_average=str(average),
                     protection_pending_position=True)
        self.store.save_trade(trade)
        if self.state == 'stopped' or trade['symbol'] not in self.rules:
            return
        try:
            self._protect_trade(trade, quantity, average)
        except (ApiError, ExecutionFault) as exc:
            # Record the durable protective exit intent, but still try to read
            # fees/fills. A failing price read must not discard known execution.
            if self._effective_stop(trade)[0] is None:
                self._exit_intent(trade, '已确认成交但首次止损读取或确认失败')
                self.fault('无法确认首次成交保护：' + str(exc), error=exc)

    def _query_stop_original(self, intent):
        """Read the same protection via bounded identities; never recreate it.

        A creation acknowledgement can precede visibility in one lookup index.
        Only a definite -2013 allows alternate GETs; transport failures and
        exchange backoff keep their existing treatment.
        """
        def checked(raw):
            if (not isinstance(raw, dict) or raw.get('symbol') != intent['symbol'] or
                    raw.get('clientAlgoId') != intent['client_id'] or
                    not raw.get('algoId') or (intent.get('order_id') is not None and
                    str(raw.get('algoId')) != str(intent['order_id']))):
                raise ExecutionFault('原止损查询身份不一致，不能确认保护')
            return raw

        if intent.get('order_id') is not None:
            try:
                return checked(self.client.query_algo(algo_id=int(intent['order_id'])))
            except ApiError as exc:
                if exc.code != -2013:
                    raise
        try:
            return checked(self.client.query_algo(client_id=intent['client_id']))
        except ApiError as exc:
            if exc.code != -2013:
                raise
            missing = exc
        rows = self.client.open_algos()
        if not isinstance(rows, list):
            raise ExecutionFault('开放条件单响应无效，不能确认原保护')
        matches = [row for row in rows if isinstance(row, dict) and
            (row.get('clientAlgoId') == intent['client_id'] or
             (row.get('symbol') == intent['symbol'] and intent.get('order_id') is not None and
              str(row.get('algoId')) == str(intent['order_id'])))]
        if not matches:
            raise missing
        if len(matches) != 1:
            raise ExecutionFault('原止损查询出现多个身份记录，不能确认保护')
        return checked(matches[0])

    @trace_stage('order_reconcile')
    def _query_intent(self, intent):
        if intent['kind'] == 'cancel':
            target = self.store.order(intent['target_id'])
            if target and target['state'] == 'terminal':
                intent['state'] = 'terminal'
                self.store.save_order(intent)
            return
        if intent['kind'] == 'stop':
            self._discover_child(intent)
        if intent['state'] == 'terminal' and (
                (intent['kind'] == 'stop' and not self._stop_needs_child(intent)) or
                (intent['kind'] != 'stop' and intent.get('fills_complete'))):
            return
        try:
            raw = (self._query_stop_original(intent) if intent['kind'] == 'stop'
                   else self.client.query_order(intent['symbol'], client_id=intent['client_id'],
                                                order_id=intent.get('order_id')))
            self._save_response(intent, raw)
            # Only a successful query, never a submission acknowledgement or
            # a cached position snapshot, can authorize recovery of a rejected
            # cancellation. Uncertain cancellation attempts are not replayed.
            intent['last_query_ms'] = self.client.now_ms()
            self.store.save_order(intent)
            if intent['kind'] == 'stop':
                self._discover_child(intent)
            elif intent.get('order_id'):
                if intent['purpose'] == 'entry':
                    self._protect_entry_confirmation(intent)
                fills = self.client.fills_all(intent['symbol'], order_id=int(intent['order_id']))
                for row in fills:
                    if int(row['orderId']) != int(intent['order_id']):
                        raise ExecutionFault('成交记录订单号不匹配')
                    value = dict(row, _trade_id=intent['trade_id'], _purpose=intent['purpose'])
                    if self.store.claim_fill(intent['symbol'], value) and self.diagnostics is not None:
                        self.diagnostics.observe_fee(value)
                persisted = [f for f in self.store.fills(intent['symbol'])
                             if int(f['orderId']) == int(intent['order_id']) and f.get('_trade_id') == intent['trade_id']]
                total = sum((D(f['qty']) for f in persisted), D(0))
                executed = D(raw.get('executedQty', 0))
                if total > executed:
                    raise ExecutionFault('实际成交数量大于订单累计成交，等待交易所快照一致')
                intent['fills_complete'] = total == executed
                self.store.save_order(intent)
                if intent['purpose'] == 'entry':
                    self._protect_entry_confirmation(intent, strict_fills=True)
            return raw
        except (ApiError, OrderUncertain) as exc:
            # A missing order after a timeout does not prove the original request was never accepted.
            if intent['state'] in {'prepared','unknown'}:
                intent.update(state='unknown', error=str(exc))
                self.store.save_order(intent)
            raise ExecutionFault('原订单查询未确认：' + str(exc)) from exc

    def _rebuild_fills(self, trade):
        """Derive accounting from immutable raw fills on every reconciliation, including after crash."""
        fills = sorted([f for f in self.store.fills(trade['symbol']) if f.get('_trade_id') == trade['id']],
                       key=lambda f: (int(f['time']), int(f['id'])))
        quantity, average, cost, realized, fees = D(0), D(0), D(0), D(0), D(0)
        first, last_exit = None, None
        for row in fills:
            q, price, fee, pnl = D(row['qty']), D(row['price']), D(row['commission']), D(row['realizedPnl'])
            if row.get('commissionAsset') != 'USDT' and fee != 0:
                self.fault('成交手续费不是USDT，未折算金额须人工核对')
                trade['unpriced_fee'] = True
            else:
                fees += fee
                cost += max(fee, D(0))
            realized += pnl
            cost += max(-pnl, D(0))
            if row['_purpose'] == 'entry':
                average = (quantity * average + q * price) / (quantity + q)
                quantity += q
                first = int(row['time']) if first is None else min(first, int(row['time']))
            else:
                if q > quantity:
                    raise ExecutionFault('退出成交大于已登记入场成交，等待迟到成交对账')
                quantity -= q
                last_exit = int(row['time'])
        trade.update(fill_quantity=str(quantity), fill_average=str(average), actual_cost=str(cost),
                     actual_fees=str(fees), realized_pnl=str(realized), fill_count=len(fills))
        if first is not None:
            trade['first_fill_ms'] = first
        if last_exit is not None:
            trade['last_exit_fill_ms'] = last_exit
        self.store.save_trade(trade)

    def _effective_stop(self, trade):
        active = []
        for order in self._orders():
            if order['trade_id'] != trade['id'] or order['kind'] != 'stop':
                continue
            raw = order.get('raw', {})
            aid = str(order.get('order_id'))
            actual = self.algo_map.get((trade['symbol'], aid))
            # Query and open-order snapshots must both confirm NEW, with every trigger field correct.
            if actual and raw.get('algoStatus', raw.get('status')) == 'NEW' and self._valid_stop(trade, actual, order):
                active.append((D(actual['triggerPrice']), order))
        if not active:
            return None, []
        merged = (max if trade['direction'] == 1 else min)(r[0] for r in active)
        return merged, [o for _, o in active]

    def _valid_stop(self, trade, raw, intent=None):
        if not isinstance(raw, dict):
            return False
        try:
            trigger = D(raw.get('triggerPrice', 0))
            if not trigger.is_finite() or trigger <= 0:
                return False
        except (ValueError, TypeError, ArithmeticError):
            return False
        base = (raw.get('algoStatus', raw.get('status')) == 'NEW' and
            raw.get('algoType', 'CONDITIONAL') == 'CONDITIONAL' and
            raw.get('symbol') == trade['symbol'] and raw.get('orderType', raw.get('type')) == 'STOP_MARKET' and
            raw.get('side') == ('SELL' if trade['direction'] == 1 else 'BUY') and
            raw.get('positionSide') == 'BOTH' and
            raw.get('workingType') == 'CONTRACT_PRICE' and
            false(raw.get('priceProtect')))
        if not base:
            return False
        if intent and (raw.get('clientAlgoId') != intent.get('client_id') or
                       str(raw.get('algoId')) != str(intent.get('order_id'))):
            return False
        if truth(raw.get('closePosition')):
            try:
                zero_quantity = raw.get('quantity') in {None, ''} or D(raw['quantity']) == 0
            except (ValueError, TypeError, ArithmeticError):
                return False
            # Binance normalizes close-all responses to reduceOnly=true even
            # though the request correctly omitted reduceOnly and quantity.
            return zero_quantity
        return self._bridge_valid(trade, raw, intent)

    def _inflight_protective_exit(self, trade):
        for order in self._orders():
            if order['trade_id'] != trade['id'] or order['kind'] != 'stop':
                continue
            if self._stop_needs_child(order):
                return True
            if order.get('raw', {}).get('algoStatus') in {'TRIGGERING', 'TRIGGERED', 'FINISHED'}:
                child = self.store.order('child-' + order['symbol'] + '-' + str(order.get('order_id')))
                if not child or child['state'] != 'terminal' or not child.get('fills_complete'):
                    return True
        return False

    @trace_stage('reconcile')
    def reconcile(self):
        self.reconciled = False
        try:
            self._reconcile_snapshot()
            if self.reconciled:
                self.last_successful_reconcile_ms = self.client.now_ms()
                self._account_snapshot['reconciled_at_ms'] = self.last_successful_reconcile_ms
                self._account_snapshot_monotonic = self._monotonic()
        finally:
            # Signed reads may have corrected the clock. Record completion in
            # that time base, including on failure, to retain bounded polling.
            self.last_full_reconcile_ms = self.client.now_ms()

    @trace_stage('account_orders_snapshot')
    def _reconcile_snapshot(self):
        self.account = self.client.account()
        margin_equity = D(self.account.get('totalMarginBalance',
            D(self.account.get('totalWalletBalance', 0)) + D(self.account.get('totalUnrealizedProfit', 0))))
        margin_bad = margin_equity <= D(self.account.get('totalMaintMargin', 0))
        ordinary, algos = self.client.open_orders(), self.client.open_algos()
        self.algo_map = {(r['symbol'], str(r['algoId'])): r for r in algos}
        problems = []
        waiting = []
        if margin_bad:
            problems.append('标记价格权益未高于账户维持保证金要求')
        # Includes newly found conditional children, without losing their actual trade timestamps.
        queried = set()
        for _ in range(2):
            for order in self._orders():
                if order['id'] in queried:
                    continue
                queried.add(order['id'])
                try:
                    self._query_intent(order)
                except ExecutionFault as exc:
                    problems.append(str(exc))
        position_rows = self.client.positions()
        self.positions = self._position_map(position_rows)
        active = self._trades(True)
        owned = {t['symbol']: t for t in active}
        if any(s not in owned for s in self.positions):
            problems.append('发现未登记真实持仓，不自动接管或开仓')
        known_market = {(o['symbol'], str(o.get('order_id'))) for o in self._orders() if o['kind'] == 'market'}
        known_algo = {(o['symbol'], str(o.get('order_id'))) for o in self._orders() if o['kind'] == 'stop'}
        if any((o['symbol'], str(o['orderId'])) not in known_market for o in ordinary) or any(
                (o['symbol'], str(o['algoId'])) not in known_algo for o in algos):
            problems.append('发现未登记普通订单或条件单')
        for trade in active:
            try:
                self._rebuild_fills(trade)
            except ExecutionFault as exc:
                problems.append(str(exc))
            trade = self.store.trade(trade['id'])
            trade['protection_pending_position'] = False
            position = self.positions.get(trade['symbol'])
            if position:
                qty = D(position['positionAmt'])
                if (1 if qty > 0 else -1) != trade['direction']:
                    problems.append('真实持仓方向与登记交易不符')
                    continue
                trade.update(quantity=str(abs(qty)), average=str(position['entryPrice']), status='open')
                if D(trade.get('fill_quantity', 0)) != abs(qty):
                    (waiting if self._pending(trade) else problems).append('真实仓位与成交记录数量尚未一致')
                stop, _ = self._effective_stop(trade)
                trade['confirmed_stop'] = str(stop) if stop is not None else None
                if stop is not None:
                    old = trade.get('tightest_stop')
                    trade['tightest_stop'] = str(stop if old is None else
                        (max if trade['direction'] == 1 else min)(D(old), stop))
            else:
                trade['quantity'] = '0'
                # FINISHED algo is not proof of closure. Require real flat and fully accounted fills.
                if not self._pending(trade) and D(trade.get('fill_quantity', 0)) == 0:
                    trade.update(status='closed', closed_ms=trade.get('last_exit_fill_ms', self.client.now_ms()))
                else:
                    waiting.append('真实空仓与已登记成交余量或退出费用尚未一致')
            self.store.save_trade(trade)
        if self._blocked_orders():
            (problems if self._unknown() else waiting).append('订单、保护清理或成交费用尚未完全确认')
        problems.extend(self._rebuild_strategy_ledgers())
        self.reconciled = not problems and not waiting
        self.reconciliation_reason = '; '.join(dict.fromkeys(problems + waiting)) or None
        if self.reconciled:
            self._account_snapshot = copy.deepcopy({'reconciled_at_ms': self.client.now_ms(),
                'account': self.account, 'positions': position_rows,
                'open_orders': ordinary, 'open_algo_orders': algos})
            self._account_snapshot_monotonic = self._monotonic()
        if problems:
            self.fault('; '.join(dict.fromkeys(problems)))

    @trace_stage('cancel_intent')
    def _cancel(self, target):
        target = self.store.order(target['id']) or target
        if target['state'] == 'terminal':
            return
        attempts = [o for o in self._orders() if o['kind'] == 'cancel' and o.get('target_id') == target['id']]
        previous = max(attempts, key=lambda o: (int(o.get('attempt', 1)), int(o.get('time_ms', 0)))) if attempts else None
        if previous:
            # A DELETE may already have reached the exchange. Even a later NEW
            # target snapshot cannot justify replaying an uncertain request.
            if not previous.get('rejected') or previous['state'] != 'terminal':
                return
            if int(target.get('last_query_ms', -1)) <= int(previous['time_ms']):
                return
        attempt = int(previous.get('attempt', 1)) + 1 if previous else 1
        cancel_id = 'cancel-' + target['id'] + (('-' + str(attempt)) if attempt > 1 else '')
        intent = {'id': cancel_id, 'client_id': None, 'kind': 'cancel', 'purpose': 'cancel',
                  'trade_id': target['trade_id'], 'symbol': target['symbol'], 'target_id': target['id'],
                  'state': 'prepared', 'attempt': attempt, 'time_ms': self.client.now_ms()}
        self.store.save_order(intent)
        try:
            raw = (self.client.cancel_algo(algo_id=int(target['order_id'])) if target['kind'] == 'stop'
                   else self.client.cancel_order(target['symbol'], order_id=int(target['order_id'])))
            intent.update(state='confirmed', raw=raw)
        except OrderUncertain as exc:
            intent.update(state='unknown', error=str(exc))
        except ApiError as exc:
            # Includes local budget rejection before the wire and definite
            # exchange errors. Keep the audit, but do not invent an unknown
            # mutation that would permanently prevent recovery.
            intent.update(state='terminal', rejected=True, error=str(exc))
        self.store.save_order(intent)

    def _exit_intent(self, trade, reason):
        if not trade.get('exit_intent'):
            trade['exit_intent'] = {'reason': reason, 'time_ms': self.client.now_ms()}
            self.store.save_trade(trade)
            self.store.event(self.client.now_ms(), '记录持续退出意图', {'trade': trade['id'], 'reason': reason})

    def _stop_capability_account(self):
        key = getattr(self.client, '_key', None)
        if not isinstance(key, str) or not key:
            return None
        return hashlib.sha256(('close-all-overlap-v1:' + key).encode()).hexdigest()

    def _close_all_overlap_rejected(self, symbol):
        account = self._stop_capability_account()
        records = self.store.meta(STOP_CAPABILITY_META)
        if not account or not isinstance(records, dict) or records.get('version') != 1:
            return False
        accounts = records.get('accounts')
        rows = accounts.get(account) if isinstance(accounts, dict) else None
        row = rows.get(symbol) if isinstance(rows, dict) else None
        return bool(isinstance(row, dict) and row.get('code') == -4130 and
            row.get('source') == 'confirmed_exchange_rejection' and
            type(row.get('confirmed_at_ms')) is int)

    def _remember_close_all_overlap_rejection(self, symbol, error):
        # Only a definite exchange rejection learns this account/symbol trait.
        # Unknown writes, local preflight and generic network errors never do.
        if (not isinstance(error, ApiError) or isinstance(error, OrderUncertain) or
            error.code != -4130 or not error.definite_rejected or
            error.definite_not_submitted or symbol not in SYMBOLS):
            return
        account = self._stop_capability_account()
        if not account:
            return
        records = self.store.meta(STOP_CAPABILITY_META)
        if not isinstance(records, dict) or records.get('version') != 1:
            records = {'version': 1, 'accounts': {}}
        if not isinstance(records.get('accounts'), dict):
            records['accounts'] = {}
        rows = records['accounts'].setdefault(account, {})
        if not isinstance(rows, dict):
            rows = records['accounts'][account] = {}
        rows[symbol] = {'source': 'confirmed_exchange_rejection', 'code': -4130,
            'confirmed_at_ms': self.client.now_ms()}
        self.store.set_meta(STOP_CAPABILITY_META, records)

    @trace_stage('protective_order')
    def _ensure_stop(self, trade, candidate):
        current = self.store.trade(trade['id']) or trade
        if self._replacement_pending(current):
            replacement = current['stop_replacement_v1']
            queued = D(replacement.get('queued_target', replacement['target']))
            replacement['queued_target'] = str((max if trade['direction'] == 1 else min)(queued, candidate))
            self._save_replacement(current, replacement)
            return self._advance_stop_replacement(current)
        symbol, d = trade['symbol'], trade['direction']
        stop, active = self._effective_stop(trade)
        floor = trade.get('tightest_stop')
        if floor is not None:
            candidate = (max if d == 1 else min)(D(floor), candidate)
        price_filter = self.price_filters.get(symbol, {})
        lower, upper = D(price_filter.get('minPrice', 0)), D(price_filter.get('maxPrice', 0))
        if (candidate <= 0 or floor_step(candidate, self.rules[symbol].tick_size) != candidate or
            (lower > 0 and candidate < lower) or (upper > 0 and candidate > upper)):
            if stop is None:
                self._exit_intent(trade, '保护价格不符合报价规则且没有有效止损')
                self.fault('止损报价规则不允许建立保护')
            return False
        if stop is not None and ((d == 1 and candidate <= stop) or (d == -1 and candidate >= stop)):
            keep = max(active, key=lambda o: D(o['raw']['triggerPrice'])) if d == 1 else min(
                active, key=lambda o: D(o['raw']['triggerPrice']))
            for redundant in active:
                if redundant['id'] != keep['id']:
                    self._cancel(redundant)
            return True
        latest = D(self.client.contract_price(symbol)['price'])
        if stop_touched(d, latest, candidate):
            self._exit_intent(trade, '最新成交价已触及收紧止损')
            return False
        if any(o['trade_id'] == trade['id'] and o['kind'] == 'stop' and
               o['state'] in {'prepared','unknown'} for o in self._orders()):
            if stop is None:
                self._exit_intent(trade, '止损未确认且没有有效保护')
                self.fault('无法确认任何有效保护，处理未完成入场并退出')
            return False
        if stop is not None and self._close_all_overlap_rejected(symbol):
            if self._unknown() or trade.get('exit_intent') or self._inflight_protective_exit(trade):
                return False  # Existing query/exit maintenance owns uncertain execution.
            # Reuse the existing verified bridge protocol; old protection is
            # retained until a new quantity bridge is actually confirmed.
            return self._start_stop_replacement(trade, candidate, active)
        intent = {'id': self._id('stop'), 'kind': 'stop', 'purpose': 'protection',
                  'trade_id': trade['id'], 'symbol': symbol, 'state': 'prepared',
                  'trigger': str(candidate), 'time_ms': self.client.now_ms()}
        intent['client_id'] = intent['id']
        self.store.save_order(intent)
        try:
            response = self.client.new_stop(symbol, 'SELL' if d == 1 else 'BUY', candidate, intent['id'])
            self._save_response(intent, response)
            actual = self._query_intent(intent)
            if not self._valid_stop(trade, actual) or D(actual['triggerPrice']) != candidate:
                raise ExecutionFault('新保护未确认有效')
            self.algo_map[(symbol, str(intent['order_id']))] = actual
            trade.update(confirmed_stop=str(candidate), tightest_stop=str(candidate))
            self.store.save_trade(trade)
            # Replacement is confirmed before cancellation of any previous protection.
            for old in active:
                self._cancel(old)
            self.store.event(self.client.now_ms(), '交易所止损保护已确认',
                             {'trade': trade['id'], 'trigger': str(candidate)})
            return True
        except (ApiError, OrderUncertain, ExecutionFault) as exc:
            if intent['state'] == 'prepared':
                intent.update(state='unknown' if isinstance(exc, OrderUncertain) else 'terminal',
                              rejected=not isinstance(exc, OrderUncertain), error=str(exc))
                self.store.save_order(intent)
            if isinstance(exc, ApiError) and exc.code == -4130 and stop is not None:
                self._remember_close_all_overlap_rejection(symbol, exc)
                return self._start_stop_replacement(trade, candidate, active)
            if isinstance(exc, ApiError) and exc.code == -2021:
                # Price may cross between our last price read and the exchange's trigger validation.
                if stop_touched(d, D(self.client.contract_price(symbol)['price']), candidate):
                    self._exit_intent(trade, '下单时价格已触及新的止损，立即市价退出')
            if stop is None:
                self._exit_intent(trade, '没有有效止损保护')
                self.fault('没有有效止损保护：' + str(exc), error=exc)
            else:
                self.store.event(self.client.now_ms(), '止损更新失败，保留旧保护', {'reason': str(exc)})
            return False

    def _check_protection(self):
        for trade in self._trades(True):
            if D(trade.get('quantity', 0)) > 0 and self._effective_stop(trade)[0] is None:
                raise ExecutionFault('已有登记持仓尚未确认任何有效止损')

    @trace_stage('protection')
    def _manage_protection(self):
        for trade in self._trades():
            if self._replacement_pending(trade):
                self._advance_stop_replacement(trade)
                continue
            if trade['status'] == 'closed':
                continue
            replacement = self._replacement(trade)
            if replacement and replacement.get('queued_target') is not None:
                target = D(replacement.pop('queued_target'))
                self._save_replacement(trade, replacement)
                self._ensure_stop(self.store.trade(trade['id']), target)
                continue
            quantity, average = D(trade.get('quantity', 0)), D(trade.get('average', 0))
            if trade.get('protection_pending_position'):
                quantity = self._confirmed_execution_quantity(trade)
                average = D(trade.get('protection_fill_average', average))
            self._protect_trade(trade, quantity, average)

    def _protect_trade(self, trade, quantity, average):
        if quantity <= 0 or self.state == 'stopped':
            return
        # The price can come from confirmed fills while positionRisk is down.
        try:
            candidate = initial_stop(average, D(trade['entry_atr']), trade['direction'],
                                     self.rules[trade['symbol']].tick_size,
                                     atr_multiplier=D(trade.get('initial_atr_multiplier', 5)))
        except (ValueError, ArithmeticError) as exc:
            if self._effective_stop(trade)[0] is None:
                self._exit_intent(trade, '实际成交价无法形成有效初始保护')
                self.fault('无法建立有效初始止损：' + str(exc))
            else:
                self.store.event(self.client.now_ms(), '初始保护候选无效，保留已确认旧保护',
                                 {'trade': trade['id'], 'reason': str(exc)})
            return
        self._ensure_stop(trade, candidate)

    def _cancel_entries(self, trade):
        pending = self._pending(trade, {'entry'})
        for order in pending:
            if order['state'] == 'confirmed' and order.get('order_id') and order.get('raw', {}).get('status') not in TERMINAL:
                self._cancel(order)
        return bool(pending)

    @trace_stage('risk_exit')
    def _risk_and_exit(self):
        for trade in self._trades(True):
            q = D(trade.get('quantity', 0))
            if trade.get('exit_intent') and trade.get('protection_pending_position'):
                q = self._confirmed_execution_quantity(trade)
            if not q:
                continue
            if trade.get('unpriced_fee'):
                self._exit_intent(trade, '手续费资产无法按实际USDT核算')
            stop, _ = self._effective_stop(trade)
            if trade.get('exit_intent'):
                self._cancel_entries(trade)
                if self._pending(trade, {'exit', 'reduce', 'stop_exit'}) or self._inflight_protective_exit(trade):
                    continue
                if not self._owned_quantity(trade, emergency=True):
                    continue
                qty = min(q, self.rules[trade['symbol']].max_qty or q)
                self._submit_market(trade, 'exit', qty)
                continue
            if stop is None:
                continue
            book = self.client.book_ticker(trade['symbol'])
            d = trade['direction']
            tick = self.rules[trade['symbol']].tick_size
            x = adverse_execution_price(D(book['bidPrice'] if d == 1 else book['askPrice']),
                d, tick, entry=False, slippage_rate=SLIP)
            z = adverse_execution_price(stop, d, tick, entry=False, slippage_rate=SLIP)
            a, cost = D(trade['average']), D(trade.get('actual_cost', 0))
            risk = risk_at_quantity(q, current_quantity=q, average_price=a, expected_reduce_price=x,
                                   expected_stop_price=z, direction=d, fee_rate=FEE, actual_cost=cost)
            trade['planned_remaining_risk'] = str(risk)
            over = risk > D(trade['budget0']) or q * a > D(trade['notional0']) or cost >= D(trade['budget0'])
            self.store.save_trade(trade)
            if not over:
                continue
            self.fault('成交后超过冻结入场预算或名义价值，保护并取消剩余入场')
            if self._cancel_entries(trade) or self._pending(trade):
                continue
            if not self._owned_quantity(trade):
                continue
            plan = reduction_target(current_quantity=q, average_price=a, expected_reduce_price=x,
                expected_stop_price=z, direction=d, budget=D(trade['budget0']), notional_cap=D(trade['notional0']),
                rules=self.rules[trade['symbol']], fee_rate=FEE, actual_cost=cost)
            self.store.event(self.client.now_ms(), '成交后预算核算及减仓目标',
                {'trade': trade['id'], 'risk': str(risk), 'plan': asdict(plan)})
            if plan.full_close or not plan.accepted:
                self._exit_intent(trade, '无法保留非零合规仓位或计划预算已消耗')
            elif plan.quantity_to_reduce > 0:
                self._submit_market(trade, 'reduce', plan.quantity_to_reduce)

    def _owned_quantity(self, trade, emergency=False):
        if emergency and trade.get('exit_intent') and trade.get('protection_pending_position'):
            # A failed position endpoint must not obstruct closing execution
            # already confirmed by order/fill IDs. The submitted order is
            # reduceOnly; this path never authorizes entries or risk resizing.
            return self._confirmed_execution_quantity(trade) > 0
        position = self.positions.get(trade['symbol'])
        if not position or (1 if D(position['positionAmt']) > 0 else -1) != trade['direction']:
            return False
        if D(trade.get('fill_quantity', 0)) != abs(D(position['positionAmt'])):
            # Fee/fill endpoint latency must not obstruct a reduce-only emergency exit
            # of a real position whose entry order is already exchange-confirmed.
            confirmed_entry = sum((D(o.get('raw', {}).get('executedQty', 0)) for o in self._orders()
                if o['trade_id'] == trade['id'] and o['kind'] == 'market' and o['purpose'] == 'entry'
                and o['state'] in {'confirmed','terminal'} and o.get('order_id')), D(0))
            if not emergency or confirmed_entry < abs(D(position['positionAmt'])):
                return False
        return True

    def _recover_confirmed_execution(self):
        """Only protect or emergency-close known fills after a snapshot failure."""
        if self.state == 'stopped':
            return
        for trade in self._trades(True):
            if not trade.get('protection_pending_position'):
                continue
            quantity = self._confirmed_execution_quantity(trade)
            if quantity <= 0 or trade['symbol'] not in self.rules:
                continue
            if not trade.get('exit_intent'):
                self._protect_trade(trade, quantity, D(trade['protection_fill_average']))
                trade = self.store.trade(trade['id'])
            if not trade.get('exit_intent'):
                continue
            self._cancel_entries(trade)
            if self._pending(trade, {'exit', 'reduce', 'stop_exit'}):
                continue
            quantity = self._confirmed_execution_quantity(trade)
            if quantity > 0:
                self._submit_market(trade, 'exit', min(quantity, self.rules[trade['symbol']].max_qty or quantity))

    @trace_stage('closed_cleanup')
    def _closed_cleanup(self):
        for trade in self._trades():
            if trade['status'] == 'closed' and trade['symbol'] not in self.positions:
                for order in self._orders():
                    if order['trade_id'] == trade['id'] and order['kind'] == 'stop' and order['state'] == 'confirmed':
                        self._cancel(order)

    @trace_stage('hourly_data')
    def refresh_candles(self, symbols=None):
        now = self.client.now_ms()
        for symbol in SYMBOLS if symbols is None else symbols:
            existing = self.store.candles(symbol)
            start = int(existing[-1][0]) + HOUR_MS if existing else None
            boundary = now // HOUR_MS * HOUR_MS
            stream = getattr(self.client, 'streams', None)
            pushed = stream.closed_since(symbol, start, boundary) if stream and start is not None else []
            if pushed:
                self.store.save_candles(symbol, pushed)
                start = int(pushed[-1][0]) + HOUR_MS
            # A closed stream frame may arrive before an earlier missing frame.
            # Backfill the first actual gap, rather than skipping to the tail.
            existing = self.store.candles(symbol)
            for before, after in zip(existing, existing[1:]):
                if int(after[0]) > int(before[0]) + HOUR_MS:
                    start = int(before[0]) + HOUR_MS
                    break
            while True:
                if start is not None and start >= boundary:
                    break
                if self._monotonic() < self._candle_poll_after.get(symbol, 0):
                    break
                limit = min(1500, max(3, (now - start) // HOUR_MS + 2)) if start is not None else 1500
                rows = self.client.klines(symbol, limit=limit, startTime=start)
                closed = [r for r in rows if int(r[0]) + HOUR_MS <= now]
                self.store.save_candles(symbol, closed)
                if len(rows) < limit or not closed or int(closed[-1][0]) + 2 * HOUR_MS > now:
                    break
                start = int(closed[-1][0]) + HOUR_MS
            candles = candles_from(self.store.candles(symbol))
            analysis = analyze(candles, now)
            self.analyses[symbol] = analysis
            signal = analysis.signal
            complete = bool(analysis.ready and signal and signal.boundary_ms == boundary)
            if complete:
                self._candle_poll_after.pop(symbol, None)
            else:
                self._candle_poll_after.setdefault(symbol, self._monotonic() + 10)
                if self._candle_poll_after[symbol] <= self._monotonic():
                    self._candle_poll_after[symbol] = self._monotonic() + 10
            self.markets[symbol] = {'symbol': symbol, 'preview': False,
                'candle_count': analysis.candle_count, 'ready': analysis.ready, 'reason': analysis.reason,
                'ema': str(analysis.ema) if analysis.ema is not None else None,
                'atr': str(analysis.atr) if analysis.atr is not None else None,
                'ema50': str(analysis.ema50) if analysis.ema50 is not None else None,
                'ema12': str(analysis.ema12) if analysis.ema12 is not None else None,
                'adx14': str(analysis.adx14) if analysis.adx14 is not None else None,
                'rsi14': str(analysis.rsi14) if analysis.rsi14 is not None else None,
                'boll_mid': str(analysis.boll_mid) if analysis.boll_mid is not None else None,
                'boll_upper': str(analysis.boll_upper) if analysis.boll_upper is not None else None,
                'boll_lower': str(analysis.boll_lower) if analysis.boll_lower is not None else None,
                'branch': signal.branch if signal else None,
                'signal': asdict(signal) if signal else None, 'price': str(candles[-1].close) if candles else None,
                'anchor_ms': candles[0].open_ms if candles else None,
                'direction': signal.direction if signal else 0,
                'breakout_high': str(signal.breakout_high) if signal else None,
                'breakout_low': str(signal.breakout_low) if signal else None,
                'boundary_ms': signal.boundary_ms if signal else None,
                'hour_data_pending': not complete,
                'hour_data_boundary_ms': boundary,
                'closed_at': datetime.fromtimestamp(
                    (candles[-1].open_ms + HOUR_MS) / 1000, timezone.utc).isoformat() if candles else None,
                'signal_valid_now': bool(signal and signal.direction and self._entry_window_open(signal)),
                'candles': [{'time': c.open_ms, 'close': str(c.close)} for c in candles[-96:]]}

    @trace_stage('indicator_exit')
    def _hourly_exits(self, boundary, symbols=None):
        for trade in self._trades(True):
            if not D(trade.get('quantity', 0)):
                continue
            symbol = trade['symbol']
            if symbols is not None and symbol not in symbols:
                continue
            analysis = self.analyses.get(symbol)
            if not analysis or not analysis.ready:
                continue
            candles = candles_from(self.store.candles(symbol))
            if not candles or candles[-1].open_ms + HOUR_MS != boundary:
                continue
            stop, _ = self._effective_stop(trade)
            if stop is not None and trade.get('first_fill_ms') is not None:
                trail = trailing_stop(stop, trade['direction'], int(trade['first_fill_ms']), candles,
                    self.rules[symbol], now_ms=self.client.now_ms(),
                    latest_trade_price=D(self.client.contract_price(symbol)['price']),
                    last_processed_hour_open=trade.get('last_trail_hour'),
                    atr_multiplier=D(trade.get('trailing_atr_multiplier', 6)))
                if trail.updated:
                    if trail.immediate_exit:
                        self._exit_intent(trade, '当前成交价已触及小时追踪止损候选')
                    else:
                        self._ensure_stop(trade, trail.stop)
                    # Only confirmed candidate or current-price exit consumes the hourly update.
                    updated = self.store.trade(trade['id'])
                    if updated.get('exit_intent') or (updated.get('confirmed_stop') and
                        ((trade['direction'] == 1 and D(updated['confirmed_stop']) >= trail.stop) or
                         (trade['direction'] == -1 and D(updated['confirmed_stop']) <= trail.stop))):
                        updated['last_trail_hour'] = trail.last_hour_open_ms
                        self.store.save_trade(updated)
            trade = self.store.trade(trade['id'])
            branch = trade.get('branch')
            if branch not in {'trend', 'range'}:
                self.fault('新策略登记交易缺少冻结入场分支，等待人工核对')
                continue
            if branch_exit(trade['direction'], candles[-1].close, branch,
                           analysis.ema12, analysis.boll_mid):
                self._exit_intent(trade, '趋势分支小时收盘严格穿越EMA12' if branch == 'trend' else
                                  '震荡分支小时收盘严格越过SMA30')

    def _catchup_position_exits(self):
        """Latest completed hour may manage held positions; it never evaluates entries."""
        if self.state == 'stopped' or not self.reconciled or self._unknown():
            return False
        active = self._trades(True)
        boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
        if self.last_successful_reconcile_ms is None or self.client.now_ms() - self.last_successful_reconcile_ms > 30000:
            return False
        ready_symbols = set()
        complete = True
        for trade in active:
            if D(trade.get('quantity', 0)) <= 0:
                continue
            result = self.analyses.get(trade['symbol'])
            rows = self.store.candles(trade['symbol'])
            if (not result or not result.ready or not rows or int(rows[-1][0]) + HOUR_MS != boundary or
                    not self._owned_quantity(trade)):
                complete = False
            else:
                ready_symbols.add(trade['symbol'])
        if ready_symbols:
            self._hourly_exits(boundary, ready_symbols)
        if complete:
            self._catchup_exits_pending = False
        return complete

    @trace_stage('entry')
    def _entry(self, symbol, signal, eligible):
        return super()._entry(symbol, signal, eligible)

    @trace_stage('funding')
    def _funding(self, *, force=False):
        now = self.client.now_ms()
        if not force and (self._monotonic() < self.income_retry_at or
                (now >= self.last_income and now - self.last_income < 60000)):
            return
        try:
            self._read_funding(now)
        except Exception as exc:
            local_wait = (getattr(exc, 'network_backoff', False) or
                (getattr(exc, 'definite_not_submitted', False) and
                 getattr(exc, 'recovery_kind', None) in {'local_budget_wait', 'exchange_cooldown', 'network_read_wait'}))
            if not local_wait:
                self.income_failures += 1
            if getattr(exc, 'network_backoff', False) or float(getattr(exc, 'retry_in_seconds', 0) or 0) > 0:
                # A shared local rejection made no request. Preserve the
                # original recovery boundary instead of adding ten minutes.
                delay = max(0, float(exc.retry_in_seconds))
            else:
                delay = NETWORK_RETRY_SECONDS
            self.income_retry_at = self._monotonic() + delay
            self.funding_pending_recovery = True
            self.store.set_meta('funding_read_pending', True)
            raise
        self.income_failures = 0
        self.income_retry_at = 0.0
        self.funding_pending_recovery = False
        self.store.set_meta('funding_read_pending', False)

    @trace_stage('funding_replay')
    def _read_funding(self, now):
        completed, self.funding_replay = read_funding(self.client, self.store, now, self.started_ms)
        self.last_income = completed
        if self._rebuild_strategy_ledgers():
            self.fault('; '.join(self.ledger_problems))

    @trace_stage('owned_orders')
    def _poll_owned_orders(self):
        """Small targeted queries keep pending fills/protection responsive.

        Unchanged NEW stops do not require a full account/open-orders scan.
        Any actual order/fill transition forces full reconciliation before action.
        """
        changed = False
        for order in self._orders():
            if order['state'] == 'terminal' and (
                    (order['kind'] == 'stop' and not self._stop_needs_child(order)) or
                    (order['kind'] != 'stop' and order.get('fills_complete'))):
                continue
            before = (order['state'], order.get('raw'), order.get('fills_complete'))
            try:
                self._query_intent(order)
            except ExecutionFault as exc:
                # One uncertain replacement must not starve other order queries
                # or a previously confirmed position's durable exit management.
                self.reconciled = False
                self.fault(str(exc), error=exc)
                continue
            after = self.store.order(order['id'])
            changed |= before != (after['state'], after.get('raw'), after.get('fills_complete'))
        return changed

    @trace_stage('tick')
    def tick(self):
        with self.lock:
            try:
                if not self._startup_clock_pending:
                    self._expire_entry_candidates()
                generation = self._fault_generation
                recovery_probe_due = self._monotonic() >= self._recovery_next_probe
                if (self._recovery['pending'] and not recovery_probe_due and
                    self._recovery['kind'] in {'local_budget_wait', 'exchange_cooldown',
                        'network_read_failure', 'network_read_wait', 'credentials_error'}):
                    self._recover_confirmed_execution()
                    return
                ensure_time_sync = getattr(self.client, 'ensure_time_sync', None)
                if callable(ensure_time_sync):
                    ensure_time_sync()
                elif not self.rules:
                    self.client.sync_time()
                if self._startup_clock_pending:
                    # Only the first successful startup calibration replaces a
                    # wall-clock baseline. Never reset consumed hours on a later
                    # time correction or overwrite persisted human controls.
                    calibrated = self.client.now_ms() // HOUR_MS * HOUR_MS
                    self.last_boundary = self._pending_hour_boundary = calibrated
                    self._hour_completed = {}
                    self._hour_entry_eligible = {}
                    self._rules_boundary = None
                    for symbol in self._default_activation_symbols:
                        self.symbol_enabled_after[symbol] = calibrated
                    self._initialize_entry_candidates()
                    self._startup_clock_pending = False
                self._expire_entry_candidates()
                if not self.rules:
                    self.refresh_rules()
                    self._rules_boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
                now = self.client.now_ms()
                boundary = now // HOUR_MS * HOUR_MS
                fresh = boundary > self.last_boundary
                changed = self._poll_owned_orders() if self.last_full_reconcile_ms is not None else False
                now = self.client.now_ms()
                boundary = now // HOUR_MS * HOUR_MS
                fresh = boundary > self.last_boundary
                full_due = (self.last_full_reconcile_ms is None or
                    (self._recovery['pending'] and recovery_probe_due and
                     self._recovery['kind'] != 'protection_replacement') or
                    (fresh and self.last_full_reconcile_ms < boundary) or changed
                    or now < self.last_full_reconcile_ms
                    or now - self.last_full_reconcile_ms >= 30000)
                if full_due:
                    self.reconcile()
                self._funding()
                # All signed read preflight can recover -1021 by correcting
                # time. Never consume an hour using a boundary captured before
                # that correction, especially when a fast clock moves back.
                boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
                fresh = boundary > self.last_boundary
                if fresh and self._rules_boundary != boundary:
                    self.refresh_rules()
                    self._rules_boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
                boundary = self.client.now_ms() // HOUR_MS * HOUR_MS
                fresh = boundary > self.last_boundary
                pending_symbols = [s for s in SYMBOLS if self._hour_completed.get(s, self.last_boundary) < boundary]
                if pending_symbols or not self.analyses:
                    self.refresh_candles()
                if self.state == 'stopped':
                    self.last_boundary = boundary
                    return  # Reboot/read-only service must never create/cancel trading orders.
                # Unknown orders/late fees close entry permission, not the
                # protection and durable exits of an already confirmed position.
                # _entry checks reconciliation; exit quantities are independently
                # checked against the current exchange position by _owned_quantity.
                # Capture signal eligibility BEFORE any exits at this boundary, including late close fills.
                eligible = {s: s not in self.positions and not self._blocked_orders() and
                    not any(t['symbol'] == s and (t['status'] != 'closed' or
                            int(t.get('closed_ms', 0)) >= boundary) for t in self._trades()) for s in SYMBOLS}
                if boundary > self._pending_hour_boundary or not self._hour_entry_eligible:
                    self._pending_hour_boundary = boundary
                    self._hour_entry_eligible = dict(eligible)
                self._manage_protection()
                self._risk_and_exit()
                self._closed_cleanup()
                if self._catchup_exits_pending:
                    self._catchup_position_exits()
                    self._risk_and_exit()
                self._maybe_resume(fresh_reconcile=full_due, generation=generation)
                if fresh:
                    ready_symbols = [s for s in pending_symbols if self._hour_data_ready(s, boundary)]
                    self._hourly_exits(boundary, ready_symbols)
                    self._risk_and_exit()
                self._resume_entry_candidates()
                if fresh:
                    for symbol in ready_symbols:
                        result = self.analyses.get(symbol)
                        if result and result.ready and result.signal and result.signal.boundary_ms == boundary:
                            self._entry(symbol, result.signal, self._hour_entry_eligible.get(symbol, False))
                        self._hour_completed[symbol] = boundary
                    if all(self._hour_completed.get(s, self.last_boundary) >= boundary for s in SYMBOLS):
                        self.last_boundary = boundary
            except Exception as exc:
                self.reconciled = False
                self.fault(str(exc), error=exc)
                try:
                    self._recover_confirmed_execution()
                except Exception as recovery_error:
                    self.fault('已确认成交的保护或紧急退出待继续核对：' + str(recovery_error), error=recovery_error)
                # Next loop resumes query/known protection. No exception grants entry permission.
