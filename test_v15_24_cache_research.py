"""V15.24 regression: no stale candle freshness and honest research rotation."""
import unittest
from collections import defaultdict
from types import SimpleNamespace
from unittest.mock import patch

import psi_strategy_v12_entry as core
import psi_v15_21_signal_delivery as feed


def row(i, close=None):
    val = 100.0 + i * .01 if close is None else float(close)
    return [i * 3600000, str(val), str(val + 1), str(val - 1),
            str(val), "100", i * 3600000 + 3599999, "10000",
            40, "60", "6000", "0"]


class AuthoritativeCacheTests(unittest.TestCase):
    def test_short_recent_response_recalculates_deep_snapshot(self):
        existing = [row(i) for i in range(1, 261)]
        latest = [row(i) for i in range(201, 261)] + [row(261, 155)]
        cache = defaultdict(dict)
        cache["SAMPLEUSDT"]["1h"] = {
            "rows": existing, "snap": {"current": 102.6},
            "updated": 100, "max_history_rows": 260
        }
        with patch.object(core, "_cache", cache):
            self.assertTrue(core._commit_authoritative_rows(
                "SAMPLEUSDT", "1h", latest, len(latest), "TEST"))
            result = cache["SAMPLEUSDT"]["1h"]
            self.assertEqual(len(result["rows"]), 260)
            self.assertEqual(result["rows"][-1][0], 261 * 3600000)
            self.assertEqual(result["snap"]["current"], 155.0)
            self.assertNotEqual(result["snap"]["current"], 102.6)
            self.assertTrue(result["snap"]["has_ema200"])

    def test_older_short_response_cannot_fake_freshness(self):
        existing = [row(i) for i in range(1, 261)]
        old = [row(i) for i in range(120, 181)]
        cache = defaultdict(dict)
        cache["SAMPLEUSDT"]["4h"] = {
            "rows": existing, "snap": {"current": 102.6},
            "updated": 100, "max_history_rows": 260
        }
        with patch.object(core, "_cache", cache):
            self.assertFalse(core._commit_authoritative_rows(
                "SAMPLEUSDT", "4h", old, len(old), "TEST"))
            self.assertEqual(cache["SAMPLEUSDT"]["4h"]["updated"], 100)
            self.assertEqual(cache["SAMPLEUSDT"]["4h"]["snap"]["current"], 102.6)


class ResearchRotationTests(unittest.TestCase):
    def setUp(self):
        self.before_core, self.before_ema = feed.CORE, feed.EMA
        self.before_state = dict(feed._RESEARCH_PREVIOUS)
        feed._RESEARCH_PREVIOUS.clear()
        self.core = SimpleNamespace(_cache={
            f"COIN{i}USDT": {"1h": {"updated": 900}}
            for i in range(20)
        })
        def rows(c, now):
            items = []
            for i in range(20):
                is_touch = i < 3
                items.append((f"COIN{i}USDT", {
                    "timeframe": "1h", "ema_period": 50,
                    "distance_pct": float(i) / 100,
                    "status": "ARMED" if is_touch else "WATCH",
                    "touch": is_touch, "seller_exhaustion": False,
                    "buyer_reclaim": False
                }))
            return items, 20, 0
        self.ema = SimpleNamespace(
            scan_cached_ema=rows,
            _technical_complete=lambda item: False,
            _ema_rank=lambda item: -item["distance_pct"],
            num=lambda x: float(x or 0),
        )
        feed.CORE, feed.EMA = self.core, self.ema
        feed._SNAPSHOT = {}
        feed._ACTIVE = set()

    def tearDown(self):
        feed.CORE, feed.EMA = self.before_core, self.before_ema
        feed._RESEARCH_PREVIOUS.clear()
        feed._RESEARCH_PREVIOUS.update(self.before_state)

    def test_additional_watch_candidates_rotate_not_fake_primary(self):
        first = feed.publish_once(1000000)
        second = feed.publish_once(1020000)
        self.assertEqual(first["research_top10"], ["COIN0USDT", "COIN1USDT", "COIN2USDT"])
        self.assertEqual(first["research_eligible_count"], 20)
        self.assertEqual(first["research_alternative_count"], 17)
        self.assertEqual(len(first["rotating_research_rows"]), 10)
        self.assertNotEqual(
            [x["symbol"] for x in first["rotating_research_rows"]],
            [x["symbol"] for x in second["rotating_research_rows"]])
        self.assertEqual(second["research_source_changes"], 0)
        self.assertEqual(second["research_rows"][0]["change"], "UNCHANGED")
        self.assertGreater(second["research_rows"][0]["source_age_s"], 0)

    def test_small_pool_is_not_artificially_rotated(self):
        # Use the honest result: no alternative exists if every eligible row touches.
        self.ema.scan_cached_ema=lambda c,n: ([
            ("ONLYUSDT", {"timeframe": "1h", "ema_period": 50,
                "distance_pct": 0, "status": "ARMED", "touch": True,
                "seller_exhaustion": False, "buyer_reclaim": False})], 1, 0)
        self.core._cache["ONLYUSDT"]={"1h": {"updated": 900}}
        out=feed.publish_once(1000000)
        self.assertEqual(out["research_alternative_count"], 0)
        self.assertEqual(out["rotating_research_rows"], [])
        self.assertEqual(out["research_top10"], ["ONLYUSDT"])

if __name__ == "__main__":
    unittest.main()
