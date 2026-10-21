"""Original-window entry continuation, never an exchange write retry queue.

Only a qualified flat-hour signal may enter this journal.  Before the first
write admission it can repeat reads; after atomic intent admission the ordinary
order reconciliation owns its one identity forever.
"""
from dataclasses import asdict
from decimal import Decimal

from .binance import ApiError, OrderUncertain, RequestExpired
from .diagnostics import trace_stage
from .strategy import Signal, HOUR_MS, plan_entry, entry_deviation_allowed

D = lambda value: Decimal(str(value))
ACTIVATION_META = 'entry_candidate_activation_v1'
RELEASE = '2026-10-15-stage3-entry-candidates-v8'


class EntryCandidateMixin:
    def _initialize_entry_candidates(self):
        """A first v8 calibration starts future hours; existing v8 journals survive."""
        marker = self.store.meta(ACTIVATION_META)
        if marker is None:
            self.store.set_meta(ACTIVATION_META, {'version': 1,
                'activation_boundary_ms': self._time_upper_ms() // HOUR_MS * HOUR_MS,
                'installed_release': RELEASE})
        elif (not isinstance(marker, dict) or marker.get('version') != 1 or
                type(marker.get('activation_boundary_ms')) is not int):
            raise ValueError('入场候选启用边界无效，禁止回补旧信号')

    @staticmethod
    def _candidate_signal(candidate):
        raw = dict(candidate['signal'])
        for key in ('atr', 'close', 'ema', 'breakout_high', 'breakout_low'):
            raw[key] = D(raw[key])
        return Signal(**raw)

    def _candidate_control_current(self, candidate):
        return (candidate['control'] == self.store.candidate_control(candidate['symbol']) and
                self._human_intent_still_allows() and self._symbol_is_enabled(candidate['symbol']))

    def _candidate_terminal(self, candidate, status, reason):
        result = self.store.update_entry_candidate(candidate['symbol'], candidate['boundary_ms'],
            {'status': status, 'reason': reason, 'updated_ms': self.client.now_ms()}, candidate['revision'])
        if result:
            self.store.event(self.client.now_ms(), '入场候选已结束',
                {'symbol': candidate['symbol'], 'boundary_ms': candidate['boundary_ms'],
                 'status': status, 'reason': reason})
        return result

    def _expire_entry_candidates(self):
        # Local-only expiry is allowed during a network cooldown. It never probes.
        for candidate in self.store.entry_candidates(active_only=True):
            if self._time_upper_ms() >= candidate['deadline_ms']:
                self._candidate_terminal(candidate, 'expired', '原五分钟开仓窗口已结束')
            elif not self._candidate_control_current(candidate):
                self._candidate_terminal(candidate, 'superseded', '原人工启停或品种选择已变化')

    def _recoverable_entry_candidate(self, symbol, boundary):
        candidate = self.store.entry_candidate(symbol, boundary)
        return bool(candidate and candidate['status'] == 'precheck' and
            self._entry_window_open(self._candidate_signal(candidate)) and
            self._candidate_control_current(candidate))

    def _entry(self, symbol, signal, eligible):
        existing = self.store.entry_candidate(symbol, signal.boundary_ms)
        if existing is not None:
            if existing['status'] == 'precheck':
                self._continue_entry_candidate(existing)
            return
        now = self.client.now_ms()
        reason = None
        marker = self.store.meta(ACTIVATION_META)
        if not self._symbol_is_enabled(symbol):
            reason = '用户未启用该品种新开仓'
        elif signal.boundary_ms <= self.symbol_enabled_after.get(symbol, self.last_boundary):
            reason = '品种启动前的本小时信号不补用，等待下一新小时收盘'
        elif marker is not None and (not isinstance(marker, dict) or
                marker.get('version') != 1 or type(marker.get('activation_boundary_ms')) is not int or
                signal.boundary_ms <= marker['activation_boundary_ms']):
            reason = '候选机制启用前的信号不迁移或回补'
        elif not eligible or self.state != 'running' or self.fault_reason or not self.reconciled:
            reason = '信号收盘时持仓、订单或执行状态不允许开仓'
        elif not signal.direction:
            reason = '未满足趋势突破或震荡回归严格入场条件'
        elif signal.branch not in {'trend', 'range'}:
            reason = '入场信号没有明确的趋势或震荡分支'
        elif not self._entry_window_open(signal):
            reason = '五分钟开仓窗口已结束'
        if reason:
            if self.store.claim_signal(symbol, signal.boundary_ms, {'direction': signal.direction,
                    'branch': signal.branch, 'time_ms': now, 'skip_reason': reason}):
                self.store.event(now, '本小时不入场', {'symbol': symbol, 'reason': reason})
            return
        candidate = self.store.register_entry_candidate({'symbol': symbol,
            'boundary_ms': signal.boundary_ms, 'signal': asdict(signal),
            'control': self.store.candidate_control(symbol), 'created_ms': now,
            'deadline_ms': signal.boundary_ms + 300000, 'stage': 'qualified', 'attempts': 0})
        if candidate and candidate['status'] == 'precheck':
            self._continue_entry_candidate(candidate)

    def _resume_entry_candidates(self):
        self._expire_entry_candidates()
        if self.state != 'running' or self.fault_reason or not self.reconciled:
            return
        for candidate in self.store.entry_candidates(active_only=True):
            self._continue_entry_candidate(candidate)

    @trace_stage('entry_candidate_precheck')
    def _continue_entry_candidate(self, candidate):
        symbol, boundary = candidate['symbol'], candidate['boundary_ms']
        candidate = self.store.entry_candidate(symbol, boundary)
        if not candidate or candidate['status'] != 'precheck':
            return
        signal = self._candidate_signal(candidate)
        if not self._entry_window_open(signal):
            self._candidate_terminal(candidate, 'expired', '原五分钟开仓窗口已结束')
            return
        if not self._candidate_control_current(candidate):
            self._candidate_terminal(candidate, 'superseded', '原人工启停或品种选择已变化')
            return
        if self.state != 'running' or self.fault_reason or not self.reconciled:
            return
        try:
            candidate = self.store.update_entry_candidate(symbol, boundary,
                {'attempts': candidate.get('attempts', 0) + 1, 'stage': 'precheck',
                 'last_checked_ms': self.client.now_ms()}, candidate['revision'])
            if candidate is None:
                return
            self.reconcile()
            if self.state != 'running' or self.fault_reason or not self.reconciled or self._blocked_orders():
                return
            if symbol in self.positions or any(t['symbol'] == symbol and (
                    t['status'] != 'closed' or int(t.get('closed_ms', 0)) >= boundary) for t in self._trades()):
                self._candidate_terminal(candidate, 'rejected', '候选等待期间出现持仓或本小时已发生交易')
                return
            self.refresh_rules()
            cfg = next((r for r in self.client.symbol_config(symbol) if r['symbol'] == symbol), {})
            modes = self._fresh_account_configuration()
            if (str(cfg.get('marginType')).upper() != 'CROSSED' or int(cfg.get('leverage', 0)) != 50 or
                    str(modes.get('dualSidePosition')).lower() != 'false' or
                    str(modes.get('multiAssetsMargin')).lower() != 'false' or
                    str(modes.get('canTrade')).lower() != 'true'):
                self._candidate_terminal(candidate, 'rejected', '下单前50倍全仓设置发生变化')
                self.fault('下单前50倍全仓设置发生变化')
                return
            ledger = self.strategy_ledgers.get(symbol)
            if not ledger or not ledger.get('verified') or D(ledger['equity']) <= 0:
                self._candidate_terminal(candidate, 'rejected', '品种独立策略权益尚未确认或不足')
                return
            frozen = candidate.get('frozen')
            if frozen is None:
                equity = D(ledger['equity'])
                frozen = {'equity0': str(equity), 'budget0': str(equity * D('.50')),
                    'notional0': str(equity * 15), 'entry_atr': str(signal.atr), 'ledger_epoch': ledger['epoch']}
                candidate = self.store.update_entry_candidate(symbol, boundary, {'frozen': frozen}, candidate['revision'])
                if candidate is None:
                    return
            elif frozen['ledger_epoch'] != ledger['epoch']:
                self._candidate_terminal(candidate, 'superseded', '独立账本代际已变化')
                return
            observe = getattr(self.client, 'entry_book_ticker', None)
            if callable(observe):
                book, quote_is_fresh = observe(symbol)
            else:
                book, quote_is_fresh = self.client.book_ticker(symbol), None
            quote_observed = self._monotonic()
            reference = D(book['askPrice'] if signal.direction == 1 else book['bidPrice'])
            if not entry_deviation_allowed(signal, reference):
                self._candidate_terminal(candidate, 'rejected', '可成交参考价偏离信号收盘价')
                return
            plan = plan_entry(D(frozen['equity0']), reference, signal.atr, signal.direction, self.rules[symbol],
                mark_price=D(self.client.mark_price(symbol)['markPrice']),
                available_margin=D(self.account['availableBalance']), fee_rate=D('.0005'), slippage_rate=D('.0002'))
            nf = getattr(self, 'notional_filters', {}).get(symbol, {})
            max_notional = D(nf.get('maxNotional', 0))
            if max_notional > 0 and nf.get('applyMaxToMarket') is not False and plan.notional > max_notional:
                self._candidate_terminal(candidate, 'rejected', '超过适用市价订单最大金额')
                return
            if not plan.accepted:
                self._candidate_terminal(candidate, 'rejected', plan.reason)
                self.store.event(self.client.now_ms(), '放弃开仓', {'symbol': symbol, 'reason': plan.reason})
                return
            if not self._entry_window_open(signal):
                self._candidate_terminal(candidate, 'expired', '原五分钟开仓窗口已结束')
                return
            trade = {'id': self._id('trade', 'HourlyTrendRange:' + frozen['ledger_epoch'] + ':' + symbol + ':' + str(boundary)),
                'symbol': symbol, 'strategy_id': 'HourlyTrendRange', 'strategy_version': 1,
                'ledger_epoch': frozen['ledger_epoch'], 'branch': signal.branch, 'signal_snapshot': asdict(signal),
                'initial_atr_multiplier': '5', 'trailing_atr_multiplier': '6', 'direction': signal.direction,
                'status': 'pending', 'quantity': '0', 'average': '0', **frozen,
                'signal_boundary_ms': boundary, 'signal_close': str(signal.close), 'plan': asdict(plan),
                'actual_cost': '0', 'first_fill_ms': None, 'confirmed_stop': None, 'exit_intent': None}
            submitted = self._submit_candidate_market(candidate, trade, plan.quantity,
                quote_observed=quote_observed, quote_is_fresh=quote_is_fresh)
            if submitted is None:
                return
            raw = submitted.get('raw', {})
            if (submitted['state'] in {'confirmed', 'terminal'} and submitted.get('order_id') and
                    D(raw.get('executedQty', 0)) > 0 and D(raw.get('avgPrice', 0)) > 0):
                trade.update(quantity=str(D(raw['executedQty'])), average=str(D(raw['avgPrice'])), status='open')
                self.store.save_trade(trade)
                self._manage_protection()
            self.reconcile()
            self._manage_protection()
            self._risk_and_exit()
        except Exception as exc:
            current = self.store.entry_candidate(symbol, boundary)
            if current and current['status'] == 'precheck':
                self.store.update_entry_candidate(symbol, boundary,
                    {'wait_reason': str(exc), 'last_error': str(exc), 'updated_ms': self.client.now_ms()}, current['revision'])
            raise

    @trace_stage('market_intent')
    def _submit_candidate_market(self, candidate, trade, quantity, *, quote_observed=None, quote_is_fresh=None):
        """Admission is atomic. Once admitted, only original-order queries follow."""
        symbol, boundary = candidate['symbol'], candidate['boundary_ms']
        identity = self._id('entry', trade['id'])
        intent = {'id': identity, 'client_id': identity, 'trade_id': trade['id'], 'symbol': symbol,
            'kind': 'market', 'purpose': 'entry', 'state': 'prepared', 'quantity': str(quantity),
            'time_ms': self.client.now_ms(), 'fills_complete': False}
        admitted = False
        quote_observed = self._monotonic() if quote_observed is None else quote_observed

        def guard():
            current = self.store.entry_candidate(symbol, boundary)
            expected = 'write_intent' if admitted else 'precheck'
            if (not current or current['status'] != expected or
                    (admitted and current.get('order_id') != identity) or
                    not self._candidate_control_current(current) or self.state != 'running' or
                    self.fault_reason or not self.reconciled):
                raise ApiError(None, '入场候选或人工控制已变化，原请求未发送', definite_not_submitted=True)
            if not self._entry_window_open(self._candidate_signal(current)):
                raise RequestExpired(None, '原五分钟开仓窗口已结束')
            if self._monotonic() - quote_observed >= 3 or (quote_is_fresh is not None and not quote_is_fresh()):
                raise ApiError(None, '入场报价检查已过期，等待原窗口内重新核对',
                    recovery_kind='local_budget_wait', retry_in_seconds=2, definite_not_submitted=True)
            return True

        def commit():
            nonlocal admitted
            guard()
            value = self.store.commit_entry_candidate(symbol, boundary, trade, intent, candidate['revision'])
            if value is None:
                raise ApiError(None, '原候选登记冲突，不能创建第二个入场意图', definite_not_submitted=True)
            admitted = True
            return True

        try:
            side = 'BUY' if trade['direction'] == 1 else 'SELL'
            args = ({'deadline_ms': boundary + 300000}
                if callable(getattr(self.client, 'now_upper_ms', None)) else {})
            if getattr(self.client, 'supports_entry_before_send', False) is True:
                args.update(before_send=commit, wire_guard=guard)
            else:
                # Explicit local legacy adapters only; never retry a signature on TypeError.
                commit()
                guard()
            raw = self.client.market_order(symbol, side, quantity, identity, reduce_only=False, **args)
            if not admitted:
                raise RuntimeError('交易接口未执行原子入场登记')
            self._save_response(intent, raw)
        except Exception as exc:
            if not admitted:
                current = self.store.entry_candidate(symbol, boundary)
                if current is not None and current['status'] != 'precheck':
                    # An intentional stop or another committed original intent
                    # is a terminal decision, not a new connection fault.
                    return None
                if isinstance(exc, RequestExpired):
                    self._candidate_terminal(candidate, 'expired', str(exc))
                    return None
                raise
            if isinstance(exc, OrderUncertain) or not isinstance(exc, ApiError):
                intent.update(state='unknown', error=str(exc))
                self.store.save_order(intent)
                self.fault('市价订单状态未确定，按原客户订单号查询', error=exc)
            else:
                intent.update(state='terminal', fills_complete=True, error=str(exc),
                    locally_not_submitted=bool(getattr(exc, 'definite_not_submitted', False)),
                    rejected=not getattr(exc, 'definite_not_submitted', False))
                self.store.save_order(intent)
                if (not isinstance(exc, RequestExpired) and
                        candidate['control'] == self.store.candidate_control(symbol)):
                    self.fault('市价委托未完成：' + str(exc), error=exc)
        return intent
