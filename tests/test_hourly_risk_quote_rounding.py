"""Risk review rounds estimated execution only; actual average/cost remain exact."""
import sys
import unittest
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from execution.engine import Engine
from execution.strategy import MarketRules, risk_at_quantity


class RiskQuoteRoundingTests(unittest.TestCase):
    def review(self,direction,stop,budget='10'):
        trade={'id':'offline','symbol':'ETHUSDT','quantity':'1','average':'100.006',
            'direction':direction,'actual_cost':'.050003','budget0':budget,'notional0':'1500',
            'exit_intent':None}
        engine=Engine.__new__(Engine)
        engine._trades=Mock(return_value=[trade])
        engine._effective_stop=Mock(return_value=(D(stop),[]))
        engine.client=SimpleNamespace(book_ticker=Mock(return_value={'bidPrice':'100.01','askPrice':'100.01'}),
            now_ms=Mock(return_value=1))
        engine.rules={'ETHUSDT':MarketRules(D('.01'),D('.001'),D('.001'),D('20'),D('2000'))}
        engine.store=SimpleNamespace(save_trade=Mock(),event=Mock())
        engine.fault=Mock(); engine._cancel_entries=Mock(return_value=False)
        engine._pending=Mock(return_value=[]); engine._owned_quantity=Mock(return_value=True)
        engine._submit_market=Mock(); engine._exit_intent=Mock()
        with patch('execution.engine.risk_at_quantity',wraps=risk_at_quantity) as risk:
            engine._risk_and_exit()
        return engine,trade,risk.call_args.kwargs

    def test_long_estimates_round_down_but_actual_average_stays_exact(self):
        engine,trade,inputs=self.review(1,'98.01')
        self.assertEqual(inputs['expected_reduce_price'],D('99.98'))
        self.assertEqual(inputs['expected_stop_price'],D('97.99'))
        self.assertEqual(inputs['average_price'],D('100.006'))
        self.assertEqual(trade['average'],'100.006')
        self.assertEqual(D(trade['planned_remaining_risk']),D('2.114998'))
        engine._submit_market.assert_not_called()

    def test_short_estimates_round_up_but_actual_average_stays_exact(self):
        engine,trade,inputs=self.review(-1,'102.01')
        self.assertEqual(inputs['expected_reduce_price'],D('100.04'))
        self.assertEqual(inputs['expected_stop_price'],D('102.04'))
        self.assertEqual(inputs['average_price'],D('100.006'))
        self.assertEqual(D(trade['planned_remaining_risk']),D('2.135023'))
        engine._submit_market.assert_not_called()

    def test_rounding_boundary_in_risk_budget_requires_reduction(self):
        # Unrounded old projected loss is below 2.1148; corrected tick loss is
        # 2.114998. The real production risk planner must adjust, not approve it.
        engine,trade,_inputs=self.review(1,'98.01',budget='2.1148')
        engine.fault.assert_called_once()
        engine._submit_market.assert_called_once()
        self.assertEqual(engine._submit_market.call_args.args[1],'reduce')
        self.assertGreater(engine._submit_market.call_args.args[2],D(0))
        self.assertEqual(trade['actual_cost'],'.050003')


if __name__=='__main__':unittest.main()
