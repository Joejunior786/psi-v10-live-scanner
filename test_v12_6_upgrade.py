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


if __name__ == "__main__":
    unittest.main()
