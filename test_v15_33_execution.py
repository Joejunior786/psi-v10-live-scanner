import os
import unittest
from types import SimpleNamespace
os.environ.setdefault("REDIS_URL","redis://localhost:6379/0")

import psi_micro_worker as worker
import psi_v15_21_signal_delivery as delivery
import psi_v13_3_execution as outer


class ControlContractTests(unittest.TestCase):
    def test_current_priorities_override_old_leases(self):
        before=dict(delivery._PRIORITY_LEASES)
        try:
            delivery._PRIORITY_LEASES.clear()
            now=1000000
            old=[f"OLD{i}USDT" for i in range(36)]
            new=[f"NEW{i}USDT" for i in range(30)]
            delivery._stable_market_priorities(old,now)
            ordered=delivery._stable_market_priorities(new,now+1000)
            self.assertEqual(ordered[:30],new)
            self.assertEqual(len(set(ordered)),len(ordered))
            self.assertLessEqual(len(ordered),delivery.MICRO_PRIORITY_SLOTS)
        finally:
            delivery._PRIORITY_LEASES.clear()
            delivery._PRIORITY_LEASES.update(before)

    def test_urgent_top30_is_admitted_without_waiting_for_research_dwell(self):
        active={f"OLD{i}USDT" for i in range(80)}
        wanted=[f"NEW{i}USDT" for i in range(30)]+list(sorted(active))[:50]
        before={x:100.0 for x in active}
        result,add,remove,diag=worker._bounded_control_plan(
            active,wanted,before,110.0,max_replacements=4,
            min_hold_seconds=90,max_symbols=80,force_priority=True)
        self.assertEqual(len(result),80)
        self.assertGreaterEqual(len(add),8)
        self.assertTrue(add.issubset(set(wanted[:30])))
        self.assertEqual(len(add),len(remove))

    def test_pinned_subscription_cannot_be_evicted(self):
        active={f"A{i}USDT" for i in range(80)}
        priority=["A0USDT","A1USDT"]+[f"NEW{i}USDT" for i in range(28)]
        wanted=priority+[f"REST{i}USDT" for i in range(50)]
        result,add,remove,diag=worker._bounded_control_plan(
            active,wanted,{sym:0 for sym in active},200,
            max_replacements=4,min_hold_seconds=90,max_symbols=80,force_priority=True)
        self.assertTrue({"A0USDT","A1USDT"}.issubset(result))
        self.assertFalse({"A0USDT","A1USDT"}.intersection(remove))

    def test_outermost_selector_accepts_frozen_controller_priorities(self):
        keys=("CORE","EARLY","V13","_ORIGINAL_MICRO")
        old={k:getattr(outer,k) for k in keys}
        try:
            high=[f"TOP{i}USDT" for i in range(30)]
            base=[f"OLD{i}USDT" for i in range(60)]
            universe=high+base
            outer.CORE=SimpleNamespace(
                q=SimpleNamespace(universe=universe),
                REDIS_MICRO_POOL_SIZE=64,
                _board=lambda:[])
            outer.EARLY=SimpleNamespace(early_candidates=lambda *a,**kw:[])
            outer.V13=SimpleNamespace(five=lambda:[])
            outer._ORIGINAL_MICRO=lambda:base
            selection=outer._micro_symbols(priority_snapshot=tuple(high))
            self.assertEqual(selection[:30],high)
            self.assertEqual(len(set(selection)),len(selection))
        finally:
            for k,v in old.items():
                setattr(outer,k,v)

    def test_current_ml_top30_are_all_in_priority_slots(self):
        before_core, before_ml = delivery.CORE, delivery.ML
        try:
            structural=[{"symbol":f"STR{i}USDT","state":"BUY",
                         "setup_strength":90-i} for i in range(6)]
            ranked=[{"symbol":f"ML{i}USDT","setup_verification":"RESEARCH"}
                    for i in range(30)]
            delivery.CORE=SimpleNamespace(_board=lambda:structural)
            delivery.ML=SimpleNamespace(_board=ranked,_last_board_ms=0)
            approved,inspected,wanted=delivery._candidate_authorities(100000)
            self.assertEqual(wanted[:6],[r["symbol"] for r in structural])
            self.assertEqual(len(wanted),36)
            self.assertTrue({r["symbol"] for r in ranked}.issubset(set(wanted)))
        finally:
            delivery.CORE,delivery.ML=before_core,before_ml

    def test_full_priority_cohort_pinned_during_replacement(self):
        active={f"A{i}USDT" for i in range(80)}
        pinned=["A0USDT"]+[f"PIN{i}USDT" for i in range(35)]
        wanted=pinned+[f"REST{i}USDT" for i in range(44)]
        result,add,remove,diag=worker._bounded_control_plan(
            active,wanted,{sym:0 for sym in active},200,
            max_replacements=4,min_hold_seconds=90,max_symbols=80,
            force_priority=True)
        self.assertIn("A0USDT",result)
        self.assertNotIn("A0USDT",remove)
        self.assertTrue(add.issubset(set(pinned)))

    def test_depth_worker_uses_diff_not_partial_stream(self):
        original=worker.ROLE
        try:
            worker.ROLE="BOOK"
            self.assertEqual(worker.stream_name("TESTUSDT"),"testusdt@depth@100ms")
        finally:
            worker.ROLE=original

if __name__ == "__main__":
    unittest.main()
