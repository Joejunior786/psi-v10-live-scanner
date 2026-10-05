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

import psi_v12_4_upgrade as u

class FakeApp:
    def __init__(self, prices):
        self.prices = prices
        self.symbol_meta = {}
    def current_symbol_price(self, symbol):
        return self.prices.get(symbol, 0.0)
    def micro_metrics(self, symbol):
        return {}

class FakeLearner:
    def _features(self, symbol, structural, legacy):
        return {"setup": "TEST_SETUP", "ma_priority": False, "early_explosion_score": 80.0}
    def _signal_class(self, structural, legacy, features):
        return "EARLY_EXPLOSION"
    def _cohort(self, features, signal_class):
        return "TEST_COHORT"
    def _calibration(self, key):
        metric = {
            "samples": 400 if not key.startswith("SPLIT::TEST") else 100,
            "probability": 0.82,
            "ci95": [0.74, 0.88],
            "clean_entry": {
                "samples": 400 if not key.startswith("SPLIT::TEST") else 100,
                "probability": 0.82,
                "ci95": [0.74, 0.88],
            },
        }
        return {"targets": {"plus_10_before_stop": metric}}

class UpgradeTests(unittest.TestCase):
    def setUp(self):
        u.CORE = SimpleNamespace(
            _cache={},
            q=SimpleNamespace(universe=[], latest={}),
            app=FakeApp({}),
            REDIS_MICRO_POOL_SIZE=64,
            _distributed_micro_sticky_pool=[],
            legacy=SimpleNamespace(
                INTEGRITY_MICRO_TRADE_MAX_AGE_MS=15000,
                INTEGRITY_MICRO_BOOK_MAX_AGE_MS=5000,
                INTEGRITY_STRUCTURE_MAX_AGE_S=1200,
            ),
        )
        u.HARDENING = None
        u.LEARNER = None
    def tearDown(self):
        u.CORE = None
        u.HARDENING = None
        u.LEARNER = None
    def test_ma_touch_is_promotion_only(self):
        sym = "ABCUSDT"
        u.CORE.q.universe = [sym]
        u.CORE.app.prices[sym] = 100.2
        u.CORE._cache[sym] = {
            "1h": {"snap": {"current": 100.2, "sma200": 100.0, "atr": 2.0}},
            "4h": {"snap": {"current": 100.2, "sma200": 90.0, "sma50": 92.0, "atr": 3.0}},
            "1d": {"snap": {"current": 100.2, "sma200": 80.0, "sma50": 85.0, "atr": 4.0}},
        }
        sig = u._ma_signal(sym)
        self.assertEqual(sig["label"], "1H_SMA200")
        self.assertEqual(sig["proximity"], "TOUCH")
        self.assertFalse(sig["automatic_buy"])
    def test_validated_ml_probability_can_qualify(self):
        sym = "ABCUSDT"
        u.CORE.q.universe = [sym]
        u.CORE.q.latest[sym] = {"symbol": sym}
        u.CORE.app.prices[sym] = 1.0
        u.LEARNER = FakeLearner()
        d = u.ml_probability(sym, {"symbol": sym, "setup": "TEST_SETUP"}, {"symbol": sym})
        self.assertTrue(d["qualified"])
        self.assertGreater(d["probability"], 0.70)
        self.assertGreaterEqual(d["overall_ci_low"], 0.70)
        self.assertGreaterEqual(d["test_ci_low"], 0.70)
    def test_ml_override_never_bypasses_hard_safety(self):
        sym = "ABCUSDT"
        u.CORE.q.universe = [sym]
        u.CORE.app.prices[sym] = 1.0
        structural = {"symbol": sym, "current": 1.0, "entry": 1.0, "stop": 0.97, "max_chase": 1.02, "anti_chase": False}
        legacy = {"symbol": sym}
        micro = {"micro_ready": False, "sequence_verified": True, "book_sequence_verified": True, "spread_bps": 2.0, "slippage_bps": 3.0}
        integrity = {"ages": {"micro_trade_ms": 100, "micro_book_ms": 100, "tape_ms": 100, "bbo_ms": 100, "structure_s": 10}}
        safety = u._hard_execution_safety(structural, legacy, micro, integrity)
        self.assertFalse(safety["pass"])
        self.assertIn("LIVE_MICRO_DATA", safety["blockers"])
    def test_hard_safety_rejects_stale_tape_and_anti_chase(self):
        sym = "ABCUSDT"
        u.CORE.q.universe = [sym]
        u.CORE.app.prices[sym] = 1.0
        structural = {"symbol": sym, "current": 1.0, "entry": 1.0, "stop": 0.97, "max_chase": 1.02, "anti_chase": False}
        legacy = {"symbol": sym, "pinpoint_anti_chase_ok": False}
        micro = {"micro_ready": True, "sequence_verified": True, "book_sequence_verified": True, "spread_bps": 2.0, "slippage_bps": 3.0}
        integrity = {"ages": {"micro_trade_ms": 100, "micro_book_ms": 100, "tape_ms": 6000, "bbo_ms": 6000, "structure_s": 10}}
        safety = u._hard_execution_safety(structural, legacy, micro, integrity)
        self.assertFalse(safety["pass"])
        self.assertIn("STALE_EVENT_TAPE", safety["blockers"])
        self.assertIn("STALE_EVENT_BBO", safety["blockers"])
        self.assertIn("CUMULATIVE_EXTENSION_GUARD", safety["blockers"])

    def test_cold_start_never_wipes_restored_micro_pool(self):
        restored = [f"COIN{i}USDT" for i in range(12)]
        u.CORE._distributed_micro_sticky_pool = list(restored)
        u.CORE.q.universe = []
        u._original_micro = lambda: list(restored)
        out = u.promoted_micro_symbols()
        self.assertEqual(out, restored)
        self.assertEqual(u.CORE._distributed_micro_sticky_pool, restored)

    def test_hard_safety_passes_with_live_data_and_risk(self):
        sym = "ABCUSDT"
        u.CORE.q.universe = [sym]
        u.CORE.app.prices[sym] = 1.0
        structural = {"symbol": sym, "current": 1.0, "entry": 1.0, "stop": 0.97, "max_chase": 1.02, "anti_chase": False}
        legacy = {"symbol": sym}
        micro = {"micro_ready": True, "sequence_verified": True, "book_sequence_verified": True, "spread_bps": 2.0, "slippage_bps": 3.0}
        integrity = {"ages": {"micro_trade_ms": 100, "micro_book_ms": 100, "tape_ms": 100, "bbo_ms": 100, "structure_s": 10}}
        safety = u._hard_execution_safety(structural, legacy, micro, integrity)
        self.assertTrue(safety["pass"])
        self.assertEqual(safety["blockers"], [])

if __name__ == "__main__":
    unittest.main()
