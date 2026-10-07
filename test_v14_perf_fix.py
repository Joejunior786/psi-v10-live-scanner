import time
import types
import unittest

import psi_v14_perf_fix as perf


class V14PerfFixTests(unittest.TestCase):
    def setUp(self):
        perf.V14 = None
        perf._ORIGINAL_EARLY_MAP = None
        perf.clear_cache()
        perf.STATS.update({
            "hits": 0, "misses": 0, "source_builds": 0, "last_build_ms": 0.0,
        })

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
        self.assertEqual(perf.STATS["source_builds"], 1)
        self.assertGreaterEqual(perf.STATS["hits"], 1)

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

    def test_expired_cache_rebuilds(self):
        calls = {"n": 0}

        def build():
            calls["n"] += 1
            return {"AAAUSDT": {"call": calls["n"]}}

        fake = types.SimpleNamespace(_early_map=build)
        perf.install(fake)
        perf._CACHE_AT_MS = perf._now_ms() - perf.CACHE_MS - 1
        out = fake._early_map()

        self.assertEqual(calls["n"], 1)
        self.assertEqual(out["AAAUSDT"]["call"], 1)


if __name__ == "__main__":
    unittest.main()
