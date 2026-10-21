"""Human ledger allocation controls, serialized with the existing trading engine."""
from decimal import Decimal
from .equity_adjustments import validate_request, adjustment_summary, LEDGER_KEY
from .strategy import HOUR_MS


class EditableEquityMixin:
    def _equity_edit_reply(self, adjustment):
        return {'ok': True, 'adjustment': adjustment, 'strategy_ledgers': self._ledger_snapshot(),
                'message': '已按核对时的标记价权益保存；后续盈亏继续累计。已有持仓仍沿用入场预算。'}

    def set_strategy_equity(self, *, symbol, target_equity, expected_revision, operation_id):
        target = validate_request(symbol, target_equity, expected_revision, operation_id)
        with self.lock:
            previous = self.store.equity_adjustment(operation_id)
            if previous:
                if (previous['symbol'] != symbol or Decimal(previous['target_equity']) != Decimal(target) or
                        previous['revision'] != expected_revision + 1):
                    raise RuntimeError('同一保存编号不能用于不同的权益修改')
                return self._equity_edit_reply(previous)
            if not self.activation_check():
                raise RuntimeError('策略安装设置尚未完成，未修改权益')
            records = self._ledger_records()
            summary = adjustment_summary(self.store.equity_adjustments(symbol), symbol,
                                         records.get(symbol, {}).get('epoch'))
            if summary['allocation_revision'] != expected_revision:
                raise RuntimeError('该品种权益已被其他页面修改，请重新读取后保存')
            if self._blocked_orders() or any(t.get('exit_intent') or t.get('unpriced_fee') or
                    t.get('protection_pending_position') for t in self._trades(True)):
                raise RuntimeError('订单、费用或保护正在核对，未修改权益；请等待核对完成')
            # No start/restore, account configuration or exchange mutation is called here.
            self.reconcile()
            if not self.reconciled or self._blocked_orders():
                raise RuntimeError(self.reconciliation_reason or '真实账户状态尚未确认，未修改权益')
            self._funding()
            if self.funding_pending_recovery or self.client.now_ms() - self.last_income > 90000:
                raise RuntimeError('资金费读取尚未完成，未修改权益')
            self._rebuild_strategy_ledgers()
            if self.ledger_problems:
                raise RuntimeError('；'.join(self.ledger_problems))
            for trade in self._trades(True):
                if (trade.get('exit_intent') or trade.get('unpriced_fee') or
                        trade.get('protection_pending_position') or self._pending(trade)):
                    raise RuntimeError('持仓成交或退出仍在核对，未修改权益')
                if Decimal(trade.get('quantity', 0)) > 0 and self._effective_stop(trade)[0] is None:
                    raise RuntimeError('已有持仓保护尚未确认，未修改权益')
            records = self._ledger_records()
            expected = records.get(symbol)
            now = self.client.now_ms()
            boundary = self._time_upper_ms() // HOUR_MS * HOUR_MS
            new_record = None
            if expected is None:
                new_record = {'strategy': 'HourlyTrendRange', 'version': 1,
                    'epoch': self._id('ledger', symbol + ':' + str(now)),
                    'initialized_at_ms': now, 'activation_boundary_ms': boundary,
                    'initial_wallet': '100', 'wallet': '100'}
                wallet, unrealized = Decimal(100), Decimal(0)
            else:
                ledger = self.strategy_ledgers[symbol]
                if not ledger.get('verified'):
                    raise RuntimeError('独立账本尚未核对，未修改权益')
                wallet, unrealized = Decimal(ledger['wallet']), Decimal(ledger['unrealized'])
            equity = wallet + unrealized
            adjustment = {'version': 1, 'operation_id': operation_id, 'symbol': symbol,
                'revision': expected_revision + 1, 'time_ms': now,
                'activation_boundary_ms': boundary, 'ledger_epoch': (expected or new_record)['epoch'],
                'target_equity': target, 'observed_equity': str(equity),
                'observed_wallet': str(wallet), 'observed_unrealized': str(unrealized),
                'delta': str(Decimal(target) - equity), 'source': 'human_equity_edit',
                'reconciled_at_ms': self.last_successful_reconcile_ms}
            adjustment = self.store.commit_equity_adjustment(adjustment,
                expected_ledger=expected, new_ledger=new_record)
            self._control_generation += 1
            self.symbol_enabled_after = self.store.meta('symbol_enabled_after', {})
            self._rebuild_strategy_ledgers()
            return self._equity_edit_reply(adjustment)
