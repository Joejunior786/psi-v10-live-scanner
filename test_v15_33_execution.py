import os
import unittest
os.environ.setdefault("REDIS_URL","redis://localhost:6379/0")

import psi_micro_worker as worker
import psi_v15_21_signal_delivery as delivery


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
            min_hold_seconds=90,max_symbols=80)
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
            max_replacements=4,min_hold_seconds=90,max_symbols=80)
        self.assertTrue({"A0USDT","A1USDT"}.issubset(result))
        self.assertFalse({"A0USDT","A1USDT"}.intersection(remove))

    def test_depth_worker_uses_diff_not_partial_stream(self):
        original=worker.ROLE
        try:
            worker.ROLE="BOOK"
            self.assertEqual(worker.stream_name("TESTUSDT"),"testusdt@depth@100ms")
        finally:
            worker.ROLE=original

if __name__ == "__main__":
    unittest.main()
