"""Safety, coverage and learning regression checks for the V13 module."""
import math
import types
import unittest
from unittest.mock import patch

import psi_v13_upgrade as ml


class V13Tests(unittest.TestCase):
    def setUp(self):
        self.model = dict(ml._model)
        self.model["weights"] = list(ml._model["weights"])
        self.core, self.sensor = ml.CORE, ml.SENSOR
        self.ranked = list(ml._ranked)
        self.pending = list(ml._pending)

    def tearDown(self):
        ml._model.clear()
        ml._model.update(self.model)
        ml.CORE, ml.SENSOR = self.core, self.sensor
        ml._ranked[:] = self.ranked
        ml._pending[:] = self.pending

    def sample(self, sym, now=1_000_000, fresh=True):
        return {
            "symbol": sym, "state": "EARLY_WATCH",
            "hazard_score": 78.4, "entry_reference": 10.0,
            "generated_ms": now,
            "hard_sensor_safety": fresh,
            "trade_age_ms": 200, "book_age_ms": 250,
            "sequence_verified": True, "book_sequence_verified": True,
            "spread_bps": 10, "slippage_bps": 15,
            "buy_ratio": .70, "relative_volume_10s": 5.0,
            "relative_volume_30s": 3.1, "trade_acceleration": 3.8,
            "cvd_acceleration": .3, "ofi": .23, "obi": .32,
            "ask_depletion": .1, "change_point_delta": 12.0,
        }

    def test_tape_and_book_freshness_must_both_hold(self):
        r = self.sample("NMRUSDT")
        self.assertTrue(ml.fresh(r, 1_000_000))
        r["trade_age_ms"] = 5000
        self.assertFalse(ml.fresh(r, 1_000_000))
        r["trade_age_ms"] = 200
        r["book_sequence_verified"] = False
        self.assertFalse(ml.fresh(r, 1_000_000))

    def test_untrained_model_makes_no_actionable_claim(self):
        ml._model["trained"] = 0
        r = self.sample("NMRUSDT")
        candidate = ml._present(r, 1, 1_000_000)
        self.assertEqual(candidate["signal"], "ML BUY CANDIDATE")
        self.assertFalse(candidate["execution_ready"])
        self.assertIsNone(candidate["entry"])
        self.assertIn("MODEL_MINIMUM_HISTORY", candidate["reason"])

    def test_model_is_independent_of_technical_buy_state(self):
        ml._model["trained"] = 200
        ml._model["bias"] = 4.0
        r = self.sample("NMRUSDT")
        r["state"] = "NONE"
        candidate = ml._present(r, 1, 1_000_000)
        self.assertTrue(candidate["execution_ready"])
        self.assertEqual(candidate["signal"], "ML BUY NOW")
        self.assertLess(candidate["stop"], candidate["entry"])
        self.assertGreater(candidate["tp1"], candidate["entry"])

    def test_online_training_changes_estimates(self):
        ml._model["trained"] = 0
        ml._model["bias"] = -1.4
        ml._model["weights"] = [0.0] * len(ml.FEATURE_NAMES)
        x = ml.features(self.sample("NMRUSDT"))
        start = ml.predict(x)
        # Locate deterministic chronological TRAIN partition.
        day = next(n for n in range(200) if n % 20 < 14)
        ml.update_model(x, 1, day * 86_400_000)
        self.assertGreater(ml.predict(x), start)
        self.assertEqual(ml._model["trained"], 1)

    def test_held_out_test_does_not_train(self):
        x = ml.features(self.sample("NMRUSDT"))
        before = (ml._model["trained"], ml._model["bias"], list(ml._model["weights"]))
        ml.update_model(x, 1, 17 * 86_400_000)
        after = (ml._model["trained"], ml._model["bias"], list(ml._model["weights"]))
        self.assertEqual(before, after)

    def test_thirty_distinct_coins_across_six_groups(self):
        now = ml.ts()
        universe = [f"COIN{i}USDT" for i in range(45)]
        rows = [self.sample(s, now=now) for s in universe]
        fake_board = [{"symbol": s, "state": "ARMED", "setup_strength": 60}
                      for s in universe]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={
                "_all_candidates": [{"symbol": s, "state": "MONSTER-WATCH",
                                     "layers": 3} for s in universe]
            }),
            _board=lambda: fake_board,
        )
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=rows)
        ml.rank()
        result = ml.thirty()
        self.assertEqual(len(result), 30)
        self.assertEqual(len({r["symbol"] for r in result}), 30)
        from collections import Counter
        self.assertEqual(set(Counter(r["category"] for r in result).values()), {5})

    def test_missing_sensor_data_cannot_make_entry(self):
        ml._model["trained"] = 200
        ml._model["bias"] = 5
        r = self.sample("NMRUSDT")
        r["generated_ms"] = 1
        candidate = ml._present(r, 1, 1_000_000)
        self.assertFalse(candidate["execution_ready"])
        self.assertIn("LIVE_SENSOR_OR_EXECUTION_SAFETY", candidate["reason"])


if __name__ == "__main__":
    unittest.main()
