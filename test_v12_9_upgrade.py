import asyncio
import json
import time
import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import psi_v12_9_upgrade as fast


def candle_rows(n=220):
    return [[1000 + i * 60000, "1", "1.1", "0.9", "1.0", "100"] for i in range(n)]


def payload(symbol="TESTUSDT", timeframe="4h", fetched=None, n=220):
    return {
        "version": "12.4.3-structure-worker",
        "symbol": symbol,
        "interval": timeframe,
        "fetched_ms": int(time.time() * 1000) if fetched is None else fetched,
        "requested_limit": 220,
        "rows": candle_rows(n),
    }


class PayloadValidationTests(unittest.TestCase):
    def test_fresh_binance_worker_history_is_allowed(self):
        self.assertEqual(len(fast._valid_payload(payload(), "TESTUSDT", "4h", deep=True)), 220)

    def test_wrong_symbol_or_timeframe_never_imported(self):
        self.assertIsNone(fast._valid_payload(payload("OTHERUSDT"), "TESTUSDT", "4h"))
        self.assertIsNone(fast._valid_payload(payload(timeframe="1h"), "TESTUSDT", "4h"))

    def test_stale_and_future_dated_records_rejected(self):
        now = int(time.time() * 1000)
        self.assertIsNone(fast._valid_payload(payload(fetched=now - fast.STRUCTURE_AGE_MS - 1), "TESTUSDT", "4h", now_ms=now))
        self.assertIsNone(fast._valid_payload(payload(fetched=now + 1501), "TESTUSDT", "4h", now_ms=now))

    def test_deep_needs_real_history(self):
        self.assertIsNone(fast._valid_payload(payload(n=20), "TESTUSDT", "4h", deep=True))
        self.assertIsNotNone(fast._valid_payload(payload(n=20), "TESTUSDT", "4h", deep=False))

    def test_risk_cache_is_diagnostic_only(self):
        p = payload(timeframe="1m")
        self.assertTrue(fast._risk_fresh(p, "TESTUSDT", "1m"))
        self.assertFalse(fast._risk_fresh(p, "TESTUSDT", "5m"))
        self.assertFalse(fast._risk_fresh(payload(fetched=0), "TESTUSDT", "4h"))


class WorkerFastPathTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.old = (fast.CORE, fast._client, fast._original_fetch, dict(fast._seen))
        self.core = SimpleNamespace(
            REDIS_URL="redis://dummy",
            DEEP_MIN_ROWS=202,
            _cache=defaultdict(dict),
            TF_TTL={"4h": 120.0},
            _commit_authoritative_rows=self.commit,
        )
        fast.CORE = self.core
        fast._seen.clear()
        fast._original_fetch = AsyncMock(return_value=False)
        fast._client = SimpleNamespace(get=Mock(return_value=json.dumps(payload())))

    async def asyncTearDown(self):
        fast.CORE, fast._client, fast._original_fetch, old_seen = self.old
        fast._seen.clear()
        fast._seen.update(old_seen)

    def commit(self, symbol, timeframe, rows, requested_limit, source):
        self.core._cache[symbol][timeframe] = {
            "rows": rows,
            "snap": {"ema50": 1.0},
            "updated": time.time(),
        }
        return True

    async def test_valid_worker_cache_avoids_slow_ws_rpc(self):
        ok = await fast._worker_first_fetch("TESTUSDT", "4h", deep=True)
        self.assertTrue(ok)
        fast._original_fetch.assert_not_awaited()
        self.assertEqual(len(self.core._cache["TESTUSDT"]["4h"]["rows"]), 220)

    async def test_stale_worker_cache_falls_back(self):
        stale = payload(fetched=int(time.time() * 1000) - fast.STRUCTURE_AGE_MS - 5000)
        fast._client.get.return_value = json.dumps(stale)
        ok = await fast._worker_first_fetch("TESTUSDT", "4h", deep=True)
        self.assertFalse(ok)
        fast._original_fetch.assert_awaited_once_with("TESTUSDT", "4h", deep=True)

    async def test_wrong_symbol_never_promoted(self):
        fast._client.get.return_value = json.dumps(payload("OTHERUSDT"))
        await fast._worker_first_fetch("TESTUSDT", "4h")
        self.assertNotIn("TESTUSDT", self.core._cache)
        fast._original_fetch.assert_awaited_once()

    async def test_redis_error_fails_closed_and_uses_existing_fallback(self):
        fast._client.get.side_effect = TimeoutError("redis socket unavailable")
        ok = await fast._worker_first_fetch("TESTUSDT", "4h", deep=True)
        self.assertFalse(ok)
        fast._original_fetch.assert_awaited_once()
        self.assertGreater(fast._stats["redis_errors"], 0)

    async def test_slow_redis_read_does_not_block_scanner_event_loop(self):
        ticks = []
        def slow_sync_get(key):
            time.sleep(0.055)
            return json.dumps(payload())
        fast._client.get = slow_sync_get
        async def ticker():
            await asyncio.sleep(0.01)
            ticks.append(1)
        job = asyncio.create_task(ticker())
        self.assertTrue(await fast._worker_first_fetch("TESTUSDT", "4h", deep=True))
        await job
        self.assertTrue(ticks)

    async def test_prefetch_caps_per_cycle_work_and_yields(self):
        from unittest.mock import patch
        redis_client = SimpleNamespace(
            mget=Mock(side_effect=[["{}"] * fast.PREFETCH_BATCH, []])
        )
        fast._client = redis_client
        with patch.object(fast, "_hot_symbols", return_value=["TESTUSDT"] * 10), \
             patch.object(fast, "_try_import", new_callable=AsyncMock) as importer:
            await fast._prefetch(redis_client)
            self.assertLessEqual(importer.await_count, fast.PREFETCH_BATCH)
            self.assertEqual(redis_client.mget.await_count, 2)


if __name__ == "__main__":
    unittest.main()
