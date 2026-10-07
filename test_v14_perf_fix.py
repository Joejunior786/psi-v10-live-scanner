import types
import unittest

import psi_v14_perf_fix as perf


class V14PerfFixTests(unittest.TestCase):
    def setUp(self):
        perf.V14 = None
        perf._ORIGINAL_EARLY_MAP = None
        perf._ORIGINAL_P10 = None
        perf._ORIGINAL_UNIVERSE = None
        perf.clear_cache()
        for key in list(perf.STATS):
            perf.STATS[key] = 0.0 if key.endswith("_ms") else 0

    def test_reuses_one_sensor_map_inside_cache_window(self):
        calls = {"n": 0}
        source = {"AAAUSDT": {"generated_ms": 123}}

        def build():
            calls["n"] += 1
            return source

        fake = types.SimpleNamespace(_early_map=build)
        perf.install(fake)

        first = fake._early_map()
        second = fake._early_map()

        self.assertIs(first, second)
        self.assertEqual(calls["n"], 1)
        self.assertEqual(perf.STATS["early_source_builds"], 1)
        self.assertGreaterEqual(perf.STATS["early_hits"], 1)

    def test_cache_does_not_change_sensor_timestamps(self):
        sensor = {
            "AAAUSDT": {
                "generated_ms": 1000,
                "trade_age_ms": 1100,
                "book_age_ms": 1150,
            }
        }
        fake = types.SimpleNamespace(_early_map=lambda: sensor)
        perf.install(fake)
        out = fake._early_map()

        self.assertEqual(out["AAAUSDT"]["generated_ms"], 1000)
        self.assertEqual(out["AAAUSDT"]["trade_age_ms"], 1100)
        self.assertEqual(out["AAAUSDT"]["book_age_ms"], 1150)

    def test_expired_early_cache_rebuilds(self):
        calls = {"n": 0}

        def build():
            calls["n"] += 1
            return {"AAAUSDT": {"call": calls["n"]}}

        fake = types.SimpleNamespace(_early_map=build)
        perf.install(fake)
        fake._early_map()
        perf._EARLY_CACHE_AT_MS = (
            perf._now_ms() - perf.EARLY_MAP_CACHE_MS - 1
        )
        out = fake._early_map()

        self.assertEqual(calls["n"], 2)
        self.assertEqual(out["AAAUSDT"]["call"], 2)

    def test_p10_cache_is_exact_per_lane_and_score_bucket(self):
        calls = {"n": 0}

        def p10(lane, score):
            calls["n"] += 1
            return {
                "probability": 0.42 + calls["n"] / 100,
                "samples": 100,
                "confidence": "GOOD",
                "source": "TEST",
                "bucket": int(max(0, min(1, score)) * 5),
            }

        fake = types.SimpleNamespace(_p10_calibrated=p10)
        perf.install(fake)

        a = fake._p10_calibrated("BEAST", 0.61)
        b = fake._p10_calibrated("BEAST", 0.69)
        c = fake._p10_calibrated("BREAKOUT", 0.61)

        self.assertEqual(a, b)
        self.assertEqual(calls["n"], 2)
        self.assertNotEqual(a, c)
        self.assertGreaterEqual(perf.STATS["p10_hits"], 1)

    def test_p10_cache_expiry_recomputes(self):
        calls = {"n": 0}

        def p10(lane, score):
            calls["n"] += 1
            return {"probability": calls["n"] / 10, "samples": 10}

        fake = types.SimpleNamespace(_p10_calibrated=p10)
        perf.install(fake)
        first = fake._p10_calibrated("EXHAUSTION", 0.4)
        key = ("EXHAUSTION", perf._score_bucket(0.4))
        at_ms, value = perf._P10_CACHE[key]
        perf._P10_CACHE[key] = (
            at_ms - perf.P10_CACHE_MS - 1,
            value,
        )
        second = fake._p10_calibrated("EXHAUSTION", 0.4)

        self.assertEqual(calls["n"], 2)
        self.assertNotEqual(first["probability"], second["probability"])

    def test_universe_cache_avoids_per_candidate_rebuild(self):
        calls = {"n": 0}

        def universe():
            calls["n"] += 1
            return ["AAAUSDT", "BBBUSDT"]

        fake = types.SimpleNamespace(_universe=universe)
        perf.install(fake)

        self.assertEqual(fake._universe(), ["AAAUSDT", "BBBUSDT"])
        self.assertEqual(fake._universe(), ["AAAUSDT", "BBBUSDT"])
        self.assertEqual(calls["n"], 1)
        self.assertGreaterEqual(perf.STATS["universe_hits"], 1)


if __name__ == "__main__":
    unittest.main()
