"""Persisted protected close-all stop replacement; no import-time I/O."""
from decimal import Decimal
from .binance import ApiError, OrderUncertain

D = lambda value: Decimal(str(value))
PHASES = {'bridge', 'cancel_old', 'final', 'cancel_bridge', 'refresh_bridge',
          'exit', 'complete', 'blocked_bridge', 'blocked_final'}


def truth(value):
    return value is True or str(value).lower() == 'true'


def false(value):
    return value is False or str(value).lower() == 'false'


class StopReplacementMixin:
    def _replacement(self, trade):
        state = trade.get('stop_replacement_v1')
        if state is None:
            return None
        if (not isinstance(state, dict) or state.get('version') != 1 or
                state.get('trade_id') != trade['id'] or state.get('symbol') != trade['symbol'] or
                state.get('direction') != trade['direction'] or not state.get('id') or
                state.get('phase') not in PHASES or not isinstance(state.get('old_ids'), list) or
                not state['old_ids']):
            raise RuntimeError('止损替换记录无效，禁止猜测保护或重新提交')
        target = D(state['target'])
        if not target.is_finite() or target <= 0:
            raise RuntimeError('止损替换目标无效')
        return state

    def _replacement_pending(self, trade):
        state = self._replacement(trade)
        return bool(state and state['phase'] != 'complete')

    def _save_replacement(self, trade, state):
        current = self.store.trade(trade['id'])
        current['stop_replacement_v1'] = state
        self.store.save_trade(current)
        return current

    def _replacement_context(self, trade, state):
        position = self.positions.get(trade['symbol'], {})
        owned = []
        for identifier in state['old_ids']:
            order = self.store.order(identifier) or {}
            raw = order.get('raw', {})
            owned.append((identifier, raw.get('algoStatus'), raw.get('triggerPrice')))
        # Full snapshots can reveal a changed exchange conflict; a repeated
        # definite rejection with unchanged context is not a networking retry.
        full = sorted((str(raw.get('algoId')), raw.get('algoStatus'))
            for (symbol, _), raw in getattr(self, 'algo_map', {}).items()
            if symbol == trade['symbol'] and truth(raw.get('closePosition')))
        return repr((position.get('positionAmt'), trade.get('fill_quantity'),
                     state.get('queued_target', state['target']), owned, full))

    def _bridge_valid(self, trade, raw, intent):
        state = self._replacement(trade)
        if not state or not intent or intent.get('protection_mode') != 'quantity_bridge':
            return False
        if (intent.get('replacement_id') != state['id'] or intent.get('trade_id') != trade['id'] or
                intent.get('symbol') != trade['symbol'] or raw.get('clientAlgoId') != intent.get('client_id') or
                str(raw.get('algoId')) != str(intent.get('order_id')) or
                not false(raw.get('closePosition')) or not truth(raw.get('reduceOnly'))):
            return False
        try:
            quantity, expected = D(raw.get('quantity', 0)), D(intent.get('quantity', 0))
            needed = D(trade.get('quantity', 0))
            if trade.get('protection_pending_position'):
                needed = max(needed, self._confirmed_execution_quantity(trade))
            position = self.positions.get(trade['symbol'])
            if position:
                signed = D(position['positionAmt'])
                if (1 if signed > 0 else -1) != trade['direction']:
                    return False
                needed = max(needed, abs(signed))
            return (quantity.is_finite() and quantity > 0 and needed > 0 and quantity == expected and
                    quantity >= needed and D(raw['triggerPrice']) == D(intent['trigger']))
        except (KeyError, ValueError, TypeError, ArithmeticError):
            return False

    def _start_stop_replacement(self, trade, target, old):
        current = self.store.trade(trade['id'])
        if self._replacement_pending(current):
            return self._advance_stop_replacement(current)
        state = {'version': 1, 'id': self._id('replace'), 'trade_id': trade['id'],
            'symbol': trade['symbol'], 'direction': trade['direction'], 'target': str(target),
            'old_ids': [o['id'] for o in old], 'phase': 'bridge', 'quantity': None,
            'bridge_id': None, 'final_id': None, 'bridge_attempt': 0, 'final_attempt': 0,
            'created_ms': self.client.now_ms(), 'next_retry_at_ms': 0, 'reason': None}
        current = self._save_replacement(current, state)
        self.store.event(self.client.now_ms(), '全平保护共存受限，开始核对只减仓保护桥接',
                         {'trade': trade['id'], 'target': str(target)})
        self.fault('止损收紧正在通过已确认保护桥接处理', kind='protection_replacement')
        return self._advance_stop_replacement(current)

    def _replacement_order(self, trade, state, role):
        identifier = state.get(role + '_id')
        if identifier:
            order = self.store.order(identifier)
            if not order:
                raise RuntimeError('已保存的止损替换订单记录缺失')
        else:
            # Adopt the original intent after a crash between order save and
            # trade save. A prepared intent never authorizes another wire attempt.
            found = [o for o in self._orders() if o.get('replacement_id') == state['id'] and
                o.get('replacement_role') == role and o.get('replacement_attempt') == state[role + '_attempt']]
            if len(found) > 1:
                raise RuntimeError('止损替换阶段存在多个请求身份')
            order = found[0] if found else None
            if order:
                state[role + '_id'] = order['id']
                self._save_replacement(trade, state)
        if order and (order.get('trade_id') != trade['id'] or order.get('replacement_id') != state['id'] or
                      order.get('replacement_role') != role or order.get('kind') != 'stop'):
            raise RuntimeError('止损替换订单归属不一致')
        return order

    def _refresh_replacement_order(self, trade, state, role):
        order = self._replacement_order(trade, state, role)
        if order:
            self._query_intent(order)
            order = self.store.order(order['id'])
            if order.get('order_id'):
                key = (trade['symbol'], str(order['order_id']))
                if order.get('raw', {}).get('algoStatus') == 'NEW':
                    self.algo_map[key] = order['raw']
                else:
                    self.algo_map.pop(key, None)
        return order

    def _replacement_submit(self, trade, state, role, quantity=None):
        if self._replacement_order(trade, state, role):
            return
        intent = {'id': self._id('bridge' if role == 'bridge' else 'stop'), 'kind': 'stop',
            'purpose': 'protection', 'trade_id': trade['id'], 'symbol': trade['symbol'],
            'state': 'prepared', 'trigger': state['target'], 'time_ms': self.client.now_ms(),
            'protection_mode': 'quantity_bridge' if role == 'bridge' else 'close_all',
            'replacement_id': state['id'], 'replacement_role': role,
            'replacement_attempt': state[role + '_attempt']}
        intent['client_id'] = intent['id']
        if quantity is not None:
            intent['quantity'] = str(quantity)
            state['quantity'] = str(quantity)
        self.store.save_order(intent)
        state[role + '_id'] = intent['id']
        self._save_replacement(trade, state)
        try:
            side = 'SELL' if trade['direction'] == 1 else 'BUY'
            raw = (self.client.new_stop_quantity(trade['symbol'], side, D(state['target']), intent['id'], quantity)
                   if role == 'bridge' else
                   self.client.new_stop(trade['symbol'], side, D(state['target']), intent['id']))
            self._save_response(intent, raw)
        except OrderUncertain as exc:
            intent.update(state='unknown', error=str(exc))
            self.store.save_order(intent)
            self.fault('止损替换请求未确认，查询原订单：' + str(exc), error=exc)
        except ApiError as exc:
            intent.update(state='terminal', rejected=True, error=str(exc),
                          definitely_not_submitted=bool(getattr(exc, 'definite_not_submitted', False)))
            self.store.save_order(intent)
            state.update(phase='blocked_' + role, reason=str(exc),
                next_retry_at_ms=int(getattr(exc, 'retry_at_ms', 0) or self.client.now_ms() + 600000))
            if exc.code in {-2022, -4130}:
                state['structural_rejection_context'] = self._replacement_context(trade, state)
                state['next_retry_at_ms'] = 0
            self._save_replacement(trade, state)
            self.fault('止损替换被拒绝，保留原有已确认保护：' + str(exc), error=exc,
                       kind='protection_replacement' if exc.code in {-2022, -4130} else None)

    def _replacement_position(self, trade):
        # Fresh targeted read: no repeated expensive all-algo sweep per phase.
        self.positions = self._position_map(self.client.positions())
        current = self.store.trade(trade['id'])
        position = self.positions.get(trade['symbol'])
        if not position:
            return current, D(0)
        signed = D(position['positionAmt'])
        if (1 if signed > 0 else -1) != current['direction']:
            raise RuntimeError('保护替换期间真实持仓方向不一致')
        quantity = abs(signed)
        if D(current.get('fill_quantity', 0)) != quantity or self._pending(current):
            return current, None
        current.update(quantity=str(quantity), average=str(position['entryPrice']))
        self.store.save_trade(current)
        return current, quantity

    def _replacement_exit(self, trade, state, reason):
        self._exit_intent(trade, reason)
        state.update(phase='exit', reason=reason)
        self._save_replacement(trade, state)
        self.fault(reason, kind='protection_replacement')
        return False

    def _advance_stop_replacement(self, trade):
        if self.state == 'stopped':
            return False
        trade = self.store.trade(trade['id'])
        state = self._replacement(trade)
        if not state or state['phase'] == 'complete':
            return True
        now = self.client.now_ms()
        deferred = state['phase'].startswith('blocked_') and (
            (state.get('structural_rejection_context') is not None and
             state['structural_rejection_context'] == self._replacement_context(trade, state)) or
            now < state['next_retry_at_ms'])
        candidate_target = (max if trade['direction'] == 1 else min)(
            D(state['target']), D(state.get('queued_target', state['target'])))
        waiting_on_protection = self._inflight_protective_exit(trade) or any(
            order['trade_id'] == trade['id'] and order['kind'] == 'stop' and
            order['state'] in {'prepared', 'unknown'} for order in self._orders())
        if deferred and not waiting_on_protection and not trade.get('exit_intent'):
            # Rejection/backoff prevents another POST, not a candidate-price
            # exit. Uncrossed blocked work needs only the cached contract price;
            # do not add position/order GETs on every maintenance tick.
            if not state.get('observed_candidate_touch'):
                latest = D(self.client.contract_price(trade['symbol'])['price'])
                touched = latest <= candidate_target if trade['direction'] == 1 else latest >= candidate_target
                if not touched:
                    return False
                state['observed_candidate_touch'] = {
                    'target': str(candidate_target), 'price': str(latest), 'time_ms': now}
                self._save_replacement(trade, state)
            # Keep the observed crossing if original-order reads fail or the
            # price rebounds; resolve protection/children before any market exit.
        bridge = self._refresh_replacement_order(trade, state, 'bridge')
        final = self._refresh_replacement_order(trade, state, 'final')
        old = []
        for identifier in state['old_ids']:
            order = self.store.order(identifier)
            if not order or order['trade_id'] != trade['id'] or order['kind'] != 'stop':
                raise RuntimeError('原保护身份缺失，禁止继续替换')
            self._query_intent(order)
            order = self.store.order(identifier)
            old.append(order)
            key = (trade['symbol'], str(order.get('order_id')))
            if order.get('raw', {}).get('algoStatus') == 'NEW':
                self.algo_map[key] = order['raw']
            else:
                self.algo_map.pop(key, None)
        trade, quantity = self._replacement_position(trade)
        if quantity == 0:
            if trade['status'] != 'closed' or self._pending(trade) or self._inflight_protective_exit(trade):
                return False
            remaining = [o for o in old + [bridge, final] if o and o['state'] != 'terminal']
            for order in remaining:
                self._cancel(order)
            if remaining:
                return False
            state.update(phase='complete', reason='真实平仓及保护清理已核对', completed_ms=now)
            self._save_replacement(trade, state)
            return True
        if (self._inflight_protective_exit(trade) or any(o and o.get('raw', {}).get('algoStatus') in
                {'TRIGGERING', 'TRIGGERED', 'FINISHED'} for o in old + [bridge, final])):
            # The ordinary exit manager waits for the protective child first;
            # after a partial terminal child it can close a confirmed remainder.
            self._exit_intent(trade, '保护替换期间止损已触发，持续核对退出')
            state.update(phase='exit', reason='保护已触发，核对子订单及真实退出')
            self._save_replacement(trade, state)
            return False
        if trade.get('exit_intent') or state['phase'] == 'exit':
            return False
        old_valid = any(self._valid_stop(trade, o.get('raw', {}), o) for o in old)
        bridge_valid = bool(bridge and self._valid_stop(trade, bridge.get('raw', {}), bridge))
        touched = bool(state.get('observed_candidate_touch'))
        if not touched:
            latest = D(self.client.contract_price(trade['symbol'])['price'])
            touched = latest <= candidate_target if trade['direction'] == 1 else latest >= candidate_target
        if touched:
            return self._replacement_exit(trade, state, '当前成交价已触及待确认收紧保护，持续市价退出')
        if quantity is None:
            self._cancel_entries(trade)
            if not old_valid and not bridge_valid:
                return self._replacement_exit(trade, state, '数量核对未完成且保护覆盖无法确认')
            return False
        if deferred:
            return False  # No identical structural rejection or early retry POST.
        if state['phase'].startswith('blocked_'):
            role = state['phase'].split('_', 1)[1]
            previous = bridge if role == 'bridge' else final
            if previous and (previous['state'] != 'terminal' or not previous.get('rejected')):
                return False
            state[role + '_attempt'] += 1
            state[role + '_id'] = None
            state.update(phase=role, reason=None, next_retry_at_ms=0)
            state.pop('structural_rejection_context', None)
            self._save_replacement(trade, state)
            if role == 'bridge':
                bridge = None
            else:
                final = None
        if state['phase'] == 'bridge':
            if bridge:
                if bridge['state'] in {'prepared', 'unknown'}:
                    return False
                if not self._valid_stop(trade, bridge.get('raw', {}), bridge):
                    if old_valid:
                        state['phase'] = 'refresh_bridge'
                        self._save_replacement(trade, state)
                    else:
                        return self._replacement_exit(trade, state, '桥接未覆盖仓位且无完整旧保护')
                    return False
                trade.update(confirmed_stop=state['target'], tightest_stop=state['target'])
                self.store.save_trade(trade)
                state['phase'] = 'cancel_old'
                self._save_replacement(trade, state)
                return False
            if self._cancel_entries(trade) or self._pending(trade) or self._unknown():
                return False
            rules = self.rules[trade['symbol']]
            if quantity < rules.min_qty or (rules.max_qty and quantity > rules.max_qty) or quantity % rules.qty_step:
                state.update(phase='blocked_bridge', reason='真实数量不满足单张桥接保护规则', next_retry_at_ms=now+600000)
                self._save_replacement(trade, state)
                self.fault(state['reason'], kind='protection_replacement')
                return False
            self._replacement_submit(trade, state, 'bridge', quantity)
            return False
        if state['phase'] == 'refresh_bridge':
            if not old_valid:
                return self._replacement_exit(trade, state, '桥接更新前完整旧保护已失效')
            if bridge and bridge['state'] != 'terminal':
                self._cancel(bridge)
                return False
            state['bridge_attempt'] += 1
            state.update(phase='bridge', bridge_id=None, quantity=None)
            self._save_replacement(trade, state)
            return False
        if state['phase'] in {'cancel_old', 'final'} and not bridge_valid:
            if old_valid:
                state['phase'] = 'refresh_bridge'
                self._save_replacement(trade, state)
                return False
            return self._replacement_exit(trade, state, '桥接数量或状态失效且无完整旧保护')
        if state['phase'] == 'cancel_old':
            for order in old:
                if order['state'] != 'terminal':
                    self._cancel(order)
            if any(o['state'] != 'terminal' for o in old):
                return False
            state['phase'] = 'final'
            self._save_replacement(trade, state)
            return False
        if state['phase'] == 'final':
            if final:
                if final['state'] in {'prepared', 'unknown'}:
                    return False
                if not self._valid_stop(trade, final.get('raw', {}), final):
                    return False
                trade.update(confirmed_stop=state['target'], tightest_stop=state['target'])
                self.store.save_trade(trade)
                state['phase'] = 'cancel_bridge'
                self._save_replacement(trade, state)
                return False
            self._replacement_submit(trade, state, 'final')
            return False
        if state['phase'] == 'cancel_bridge':
            if not final or not self._valid_stop(trade, final.get('raw', {}), final):
                if not bridge_valid:
                    return self._replacement_exit(trade, state, '最终保护及桥接均无法确认，持续退出')
                canceling = [o for o in self._orders() if o['kind'] == 'cancel' and
                    bridge and o.get('target_id') == bridge['id'] and not o.get('rejected')]
                if canceling:
                    return self._replacement_exit(trade, state, '最终保护失效且桥接撤销可能在途，持续退出')
                if final and final['state'] == 'terminal':
                    state['final_attempt'] += 1
                    state.update(phase='final', final_id=None)
                    self._save_replacement(trade, state)
                return False
            if bridge and bridge['state'] != 'terminal':
                self._cancel(bridge)
                return False
            state.update(phase='complete', reason=None, completed_ms=now)
            self._save_replacement(trade, state)
            self.store.event(now, '只减仓桥接已清理，新全平止损替换完成', {'trade': trade['id'], 'target': state['target']})
            return True
        return False
