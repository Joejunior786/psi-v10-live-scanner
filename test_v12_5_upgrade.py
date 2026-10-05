import sys
import types
import unittest
from types import SimpleNamespace

if "redis" not in sys.modules:
    redis_mod = types.ModuleType("redis")
    redis_async = types.ModuleType("redis.asyncio")
    redis_mod.asyncio = redis_async
    sys.modules["redis"] = redis_mod
    sys.modules["redis.asyncio"] = redis_async

import psi_v12_5_upgrade as u


def strong_row():
    return {
        "trade_age_ms": 100,
        "book_age_ms": 100,
        "trade_fresh": True,
        "book_fresh": True,
        "sequence_verified": True,
        "book_sequence_verified": True,
        "spread_bps": 2.0,
        "slippage_bps": 3.0,
        "trade_count_60s": 100,
        "last_price": 1.25,
        "aggressive_buy_ratio": 0.88,
        "flow_persistence": 0.86,
        "cvd_quote_60s": 10000.0,
        "cvd_acceleration": 9000.0,
        "relative_volume_10s": 5.0,
        "relative_volume_30s": 4.0,
        "trade_acceleration": 3.2,
        "trade_size_shift": 3.0,
        "ofi": 0.75,
        "ofi_acceleration": 0.55,
        "ofi_persistence": 0.90,
        "obi": 0.55,
        "ask_depletion": 0.18,
        "bid_depletion": -0.05,
        "_sensor_generated_ms": 1,
    }


def quiet_row():
    x = strong_row()
    x.update({
        "aggressive_buy_ratio": 0.50,
        "flow_persistence": 0.50,
        "cvd_quote_60s": -100.0,
        "cvd_acceleration": -100.0,
        "relative_volume_10s": 1.0,
        "relative_volume_30s": 1.0,
        "trade_acceleration": 1.0,
        "trade_size_shift": 1.0,
        "ofi": 0.0,
        "ofi_acceleration": 0.0,
        "ofi_persistence": 0.50,
        "obi": 0.0,
        "ask_depletion": 0.0,
    })
    return x


class V125Tests(unittest.TestCase):
    def setUp(self):
        u._sensor_cache.clear()
        u._score_history.clear()
        u._latest_candidates[:] = []
        u._stats.clear()
        u._last_error = ""
        u.CORE = SimpleNamespace(
            q=SimpleNamespace(universe=["ABCUSDT", "XYZUSDT"], latest={}),
            REDIS_MICRO_POOL_SIZE=4,
            ACTIVE_SYMBOLS_PER_CYCLE=4,
        )
        u._original_micro = lambda: ["XYZUSDT"]
        u._original_priority = lambda universe: ["XYZUSDT"]

    def tearDown(self):
        u.CORE = None

    def test_sensor_safety_is_fail_closed(self):
        row = strong_row()
        row["book_fresh"] = False
        row["book_age_ms"] = 9999
        safety = u._sensor_safety(row)
        self.assertFalse(safety["pass"])
        self.assertIn("STALE_SENSOR_BOOK", safety["blockers"])

    def test_change_point_can_create_early_pinpoint_without_buy_authority(self):
        symbol = "ABCUSDT"
        u._hazard_row(symbol, quiet_row())
        second = u._hazard_row(symbol, strong_row())
        third = u._hazard_row(symbol, strong_row())
        self.assertGreaterEqual(second["hazard_score"], u.EARLY_PINPOINT_SCORE)
        self.assertEqual(third["state"], "EARLY_PINPOINT")
        self.assertFalse(third["entry_authority"])
        self.assertTrue(third["strict_buy_unchanged"])

    def test_stale_high_score_never_becomes_pinpoint(self):
        row = strong_row()
        row["trade_fresh"] = False
        row["trade_age_ms"] = 99999
        u._hazard_row("ABCUSDT", quiet_row())
        u._hazard_row("ABCUSDT", row)
        out = u._hazard_row("ABCUSDT", row)
        self.assertNotEqual(out["state"], "EARLY_PINPOINT")
        self.assertFalse(out["hard_sensor_safety"])

    def test_shard_merge_counts_unique_symbols(self):
        now = int(__import__("time").time() * 1000)
        payloads = [
            {"generated_ms": now, "metrics": {"ABCUSDT": strong_row()}},
            {"generated_ms": now, "metrics": {"XYZUSDT": quiet_row()}},
        ]
        merged, live = u._merge_sensor_payloads(payloads)
        self.assertEqual(live, 2)
        self.assertEqual(set(merged), {"ABCUSDT", "XYZUSDT"})

    def test_early_candidate_is_promoted_into_deep_pool(self):
        u._sensor_cache["ABCUSDT"] = strong_row()
        u._hazard_row("ABCUSDT", quiet_row())
        u._hazard_row("ABCUSDT", strong_row())
        final = u._hazard_row("ABCUSDT", strong_row())
        u._latest_candidates[:] = [final]
        out = u.promoted_micro_symbols()
        self.assertEqual(out[0], "ABCUSDT")
        self.assertIn("XYZUSDT", out)

    def test_hazard_score_is_not_a_probability(self):
        row = u._hazard_row("ABCUSDT", strong_row())
        self.assertIn("hazard_score", row)
        self.assertNotIn("probability", row)


if __name__ == "__main__":
    unittest.main()
