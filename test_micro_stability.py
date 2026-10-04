import os
import time
import unittest

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

import psi_micro_worker as worker
import psi_v12_3_hardening as hardening


class MicroStabilityTests(unittest.TestCase):
    def test_full_pool_flip_is_bounded(self):
        active={f"OLD{i}USDT" for i in range(80)}
        wanted=[f"NEW{i}USDT" for i in range(80)]
        now=1000.0
        activated={sym:0.0 for sym in active}
        next_active, added, removed, diag=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(len(next_active),80)
        self.assertEqual(len(added),2)
        self.assertEqual(len(removed),2)
        self.assertEqual(diag["raw_add"],80)
        self.assertEqual(diag["raw_remove"],80)

    def test_minimum_hold_prevents_eviction(self):
        active={f"OLD{i}USDT" for i in range(80)}
        wanted=[f"NEW{i}USDT" for i in range(80)]
        now=1000.0
        activated={sym:950.0 for sym in active}
        next_active, added, removed, diag=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(next_active,active)
        self.assertFalse(added)
        self.assertFalse(removed)
        self.assertEqual(diag["held_for_dwell"],80)

    def test_activity_rank_prefers_fresh_active_tape(self):
        active = {
            "ready": True,
            "age_ms": 300.0,
            "book_age_ms": 400.0,
            "trades_5s": 8,
            "notional_5s": 25000.0,
            "spread_bps": 2.0,
        }
        stale = {
            "ready": False,
            "age_ms": 25000.0,
            "book_age_ms": 25000.0,
            "trades_5s": 0,
            "notional_5s": 0.0,
            "spread_bps": 25.0,
        }
        self.assertGreater(
            hardening._activity_sort_key(active, 1_000_000.0),
            hardening._activity_sort_key(stale, 50_000_000.0),
        )

    def test_spare_capacity_fills_without_eviction(self):
        active={f"OLD{i}USDT" for i in range(40)}
        wanted=[f"OLD{i}USDT" for i in range(40)] + [f"NEW{i}USDT" for i in range(40)]
        now=1000.0
        activated={sym:995.0 for sym in active}
        next_active, added, removed, _=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(len(next_active),42)
        self.assertEqual(len(added),2)
        self.assertFalse(removed)


if __name__ == "__main__":
    unittest.main()
