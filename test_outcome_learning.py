import unittest

import psi_outcome_learning as ml


class OutcomeLearningTests(unittest.TestCase):
    def make_event(self):
        return {
            "id": "x",
            "symbol": "TESTUSDT",
            "setup": "COMPRESSION_BREAKOUT",
            "signal_class": "PINPOINT_TRIGGERED",
            "cohort": "COMPRESSION_BREAKOUT|PINPOINT_TRIGGERED|M1|F1|B1|P2|C0",
            "created_ms": 1_000_000,
            "entry_price": 100.0,
            "stop_price": 98.0,
            "stop_source": "EXPLICIT",
            "features": {},
            "mfe_pct": 0.0,
            "mae_pct": 0.0,
            "peak_price": 100.0,
            "trough_price": 100.0,
            "first_target_ms": {},
            "stop_hit_ms": 0,
            "horizon_returns": {},
            "resolved": False,
            "resolution": "OPEN",
            "execution_authority": False,
        }

    def test_tracks_mfe_mae_and_targets(self):
        event = self.make_event()
        ml._update_event(event, 106.0, 1_300_000)
        ml._update_event(event, 99.0, 1_600_000)
        self.assertAlmostEqual(event["mfe_pct"], 6.0)
        self.assertAlmostEqual(event["mae_pct"], -1.0)
        self.assertTrue(ml._target_before_stop(event, 3.0))
        self.assertTrue(ml._target_before_stop(event, 5.0))
        self.assertIsNone(ml._target_before_stop(event, 10.0))

    def test_stop_first_resolves_losses(self):
        event = self.make_event()
        ml._update_event(event, 97.5, 1_100_000)
        self.assertTrue(event["resolved"])
        self.assertEqual(event["resolution"], "STOP_FIRST")
        self.assertFalse(ml._target_before_stop(event, 3.0))
        self.assertFalse(ml._target_before_stop(event, 20.0))

    def test_target_before_later_stop_remains_win(self):
        event = self.make_event()
        ml._update_event(event, 111.0, 1_100_000)
        ml._update_event(event, 97.0, 1_200_000)
        self.assertTrue(ml._target_before_stop(event, 10.0))
        self.assertFalse(ml._target_before_stop(event, 20.0))

    def test_70pct_claim_requires_large_sample_and_lower_bound(self):
        weak = ml._posterior(7, 3)
        self.assertFalse(weak["qualified_for_70pct_claim"])
        strong = ml._posterior(285, 15)
        self.assertEqual(strong["samples"], 300)
        self.assertTrue(strong["qualified_for_70pct_claim"])

    def test_shadow_learning_has_zero_execution_authority(self):
        self.assertFalse(ml.EXECUTION_AUTHORITY)
        self.assertEqual(ml.ROLE, "SHADOW_CALIBRATION_ONLY")

    def test_cohort_separates_clean_from_chased(self):
        base = {
            "setup": "LIQUIDITY_SWEEP_REVERSAL",
            "micro_ready": True,
            "buy_ratio": 0.70,
            "cvd_accel": 0.20,
            "ofi_accel": 0.10,
            "obi": 0.30,
            "persistence": 2,
            "anti_chase": False,
            "extension_blocked": False,
        }
        clean = ml._cohort(base, "PINPOINT_TRIGGERED")
        chased = ml._cohort(
            dict(base, anti_chase=True, extension_blocked=True),
            "PINPOINT_TRIGGERED",
        )
        self.assertNotEqual(clean, chased)
        self.assertTrue(clean.endswith("C0"))
        self.assertTrue(chased.endswith("C1"))


if __name__ == "__main__":
    unittest.main()
