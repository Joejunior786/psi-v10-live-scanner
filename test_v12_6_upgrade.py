import asyncio
import json
import sys
import time
import threading
import types
import unittest

if "redis" not in sys.modules:
    redis_mod = types.ModuleType("redis")
    redis_async = types.ModuleType("redis.asyncio")
    redis_mod.asyncio = redis_async
    sys.modules["redis"] = redis_mod
    sys.modules["redis.asyncio"] = redis_async

import psi_v12_5_upgrade as v125
import psi_v12_6_upgrade as v126
import psi_sensor_worker_v126 as sensor_v126


class FakeClient:
    def __init__(self, raws):
        self.raws = raws
        self.keys = None

    async def mget(self, keys):
        self.keys = list(keys)
        return self.raws


class V126Tests(unittest.TestCase):
    def setUp(self):
        v125._sensor_cache.clear()
        v125._latest_candidates[:] = []
        v125._score_history.clear()
        v125._stats.clear()
        v125.SENSOR_SHARDS = 2
        v126.V125 = v125

    def test_merge_rejects_stale_shard(self):
        now = int(time.time() * 1000)
        fresh = {"generated_ms": now, "metrics": {"ABCUSDT": {"last_price": 1.0}}}
        stale = {
            "generated_ms": now - v126.SENSOR_MAX_SNAPSHOT_AGE_MS - 1,
            "metrics": {"XYZUSDT": {"last_price": 2.0}},
        }
        merged, live = v126._merge_sensor_payloads([fresh, stale])
        self.assertEqual(live, 1)
        self.assertIn("ABCUSDT", merged)
        self.assertNotIn("XYZUSDT", merged)

    def test_zero_fresh_shards_clears_old_state(self):
        now = int(time.time() * 1000)
        stale = json.dumps({
            "generated_ms": now - 60000,
            "metric_symbols": 1,
            "metrics": {"OLDUSDT": {"last_price": 1.0}},
        })
        v125._sensor_cache["OLDUSDT"] = {"last_price": 1.0}
        v125._latest_candidates[:] = [{"symbol": "OLDUSDT"}]
        asyncio.run(v126._refresh_from_redis(FakeClient([stale, stale])))
        self.assertEqual(v125._sensor_cache, {})
        self.assertEqual(v125._latest_candidates, [])
        self.assertEqual(v125._stats.get("sensor_shards_live"), 0)

    def test_training_is_blocked_when_shards_stale(self):
        calls = []
        v126._original_persist_training = lambda: calls.append("trained")
        v125._stats["sensor_shards_live"] = 0
        v126._persist_training_guarded()
        self.assertEqual(calls, [])
        self.assertEqual(v125._stats.get("training_skipped_stale"), 1)

    def test_sensor_ingest_cooperatively_yields(self):
        every = sensor_v126.EVENT_YIELD_EVERY
        self.assertFalse(sensor_v126._should_yield(1))
        self.assertTrue(sensor_v126._should_yield(every))
        self.assertTrue(sensor_v126._should_yield(every * 2))

    def test_refresh_replaces_cache_object_atomically(self):
        now = int(time.time() * 1000)
        old_cache = v125._sensor_cache
        old_cache["OLDUSDT"] = {"last_price": 0.5}
        fresh = json.dumps({
            "generated_ms": now,
            "metric_symbols": 1,
            "metrics": {"ABCUSDT": {"last_price": 1.0}},
        })
        asyncio.run(v126._refresh_from_redis(FakeClient([fresh, None])))
        self.assertIsNot(v125._sensor_cache, old_cache)
        self.assertIn("ABCUSDT", v125._sensor_cache)
        self.assertNotIn("OLDUSDT", v125._sensor_cache)

    def test_consumer_supervisor_runs_on_dedicated_thread(self):
        main_ident = threading.get_ident()
        calls = []
        original = v126._original_supervisor_loop
        old_thread = v126._supervisor_thread
        try:
            async def fake_supervisor():
                calls.append(threading.get_ident())
                v126._supervisor_stop.set()

            v126._original_supervisor_loop = fake_supervisor
            v126._supervisor_thread = None
            v126._supervisor_stop.clear()
            v126._supervisor_started.clear()
            thread = v126._ensure_supervisor_thread()
            thread.join(timeout=2.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(calls), 1)
            self.assertNotEqual(calls[0], main_ident)
        finally:
            v126._supervisor_stop.set()
            v126._original_supervisor_loop = original
            v126._supervisor_thread = old_thread
            v126._supervisor_started.clear()

    def test_refresh_reads_only_isolated_v126_namespace(self):
        client = FakeClient([None, None])
        asyncio.run(v126._refresh_from_redis(client))
        self.assertEqual(
            client.keys,
            ["psi:v12.6:sensor:0", "psi:v12.6:sensor:1"],
        )

    def test_hot_lane_prefers_pinpoint_then_armed_and_requires_hard_safety(self):
        now = int(time.time() * 1000)
        v125._stats["sensor_shards_live"] = 2
        v125._sensor_cache = {
            "PINUSDT": {"_sensor_generated_ms": now},
            "ARMUSDT": {"_sensor_generated_ms": now},
            "BADUSDT": {"_sensor_generated_ms": now},
        }
        v125._latest_candidates = [
            {
                "symbol": "ARMUSDT", "state": "EARLY_ARMED", "hard_sensor_safety": True,
                "generated_ms": now, "hazard_score": 99, "change_point_delta": 20,
                "buy_ratio": 0.9, "spread_bps": 1, "slippage_bps": 2,
                "book_age_ms": 100, "trade_age_ms": 100,
            },
            {
                "symbol": "PINUSDT", "state": "EARLY_PINPOINT", "hard_sensor_safety": True,
                "generated_ms": now, "hazard_score": 88, "change_point_delta": 8,
                "buy_ratio": 0.7, "spread_bps": 2, "slippage_bps": 3,
                "book_age_ms": 120, "trade_age_ms": 120,
            },
            {
                "symbol": "BADUSDT", "state": "EARLY_PINPOINT", "hard_sensor_safety": False,
                "generated_ms": now, "hazard_score": 100, "change_point_delta": 30,
                "buy_ratio": 1.0, "spread_bps": 1, "slippage_bps": 1,
                "book_age_ms": 50, "trade_age_ms": 50,
            },
        ]
        self.assertEqual(v126._hot_lane_symbols()[:2], ["PINUSDT", "ARMUSDT"])
        self.assertNotIn("BADUSDT", v126._hot_lane_symbols())

    def test_hot_lane_fails_closed_on_stale_snapshot(self):
        now = int(time.time() * 1000)
        v125._stats["sensor_shards_live"] = 2
        stale = now - v126.SENSOR_MAX_SNAPSHOT_AGE_MS - 1
        v125._sensor_cache = {"OLDUSDT": {"_sensor_generated_ms": stale}}
        v125._latest_candidates = [{
            "symbol": "OLDUSDT", "state": "EARLY_PINPOINT", "hard_sensor_safety": True,
            "generated_ms": stale, "hazard_score": 100, "change_point_delta": 20,
            "buy_ratio": 1.0, "spread_bps": 1, "slippage_bps": 1,
            "book_age_ms": 10, "trade_age_ms": 10,
        }]
        self.assertEqual(v126._hot_lane_symbols(), [])

    def test_hot_first_promotion_preserves_base_and_has_no_buy_authority(self):
        now = int(time.time() * 1000)
        v125._stats["sensor_shards_live"] = 2
        v125._sensor_cache = {"HOTUSDT": {"_sensor_generated_ms": now}}
        v125._latest_candidates = [{
            "symbol": "HOTUSDT", "state": "EARLY_PINPOINT", "hard_sensor_safety": True,
            "generated_ms": now, "hazard_score": 90, "change_point_delta": 10,
            "buy_ratio": 0.8, "spread_bps": 1, "slippage_bps": 1,
            "book_age_ms": 100, "trade_age_ms": 100,
            "entry_authority": False, "strict_buy_unchanged": True,
        }]
        old = v126._original_promotion_symbols
        try:
            v126._original_promotion_symbols = lambda: ["BASEUSDT", "HOTUSDT"]
            promoted = v126._hot_first_promotion_symbols()
            self.assertEqual(promoted[:2], ["HOTUSDT", "BASEUSDT"])
            self.assertFalse(v125._latest_candidates[0]["entry_authority"])
            self.assertTrue(v125._latest_candidates[0]["strict_buy_unchanged"])
        finally:
            v126._original_promotion_symbols = old


if __name__ == "__main__":
    unittest.main()
