import unittest
import psi_v13_3_execution as v


def structural(state="BUY", strength=92.0, setup="GENERIC_MA_RECLAIM"):
    return {
        "symbol": "TESTUSDT", "state": state, "setup": setup,
        "setup_strength": strength,
        "current": 100.0, "entry_low": 100.0, "entry": 100.0,
        "max_chase": 102.0, "invalidation": 98.0,
        "tp1": 103.0, "tp2": 106.0, "tp3": 110.0,
        "anti_chase": False,
        "active_setups": [
            {"name": setup, "state": state, "strength": strength}
        ],
    }


def early(now=1_000_000, state="EARLY_ARMED", hazard=86.0):
    return {
        "symbol": "TESTUSDT", "state": state, "hard_sensor_safety": True,
        "hazard_score": hazard, "entry_reference": 100.0,
        "generated_ms": now - 100,
        "trade_age_ms": 100, "book_age_ms": 100,
        "sequence_verified": True, "book_sequence_verified": True,
        "spread_bps": 5.0, "slippage_bps": 8.0,
        "buy_ratio": 0.62, "cvd_acceleration": 1.0,
        "ofi": 0.2, "ofi_acceleration": 0.1,
        "obi": 0.15, "ask_depletion": 0.05,
        "relative_volume_10s": 1.4, "relative_volume_30s": 1.2,
        "trade_acceleration": 1.3,
        "v128_probe_score": 80.0, "change_point_delta": 10.0,
    }


class SetupSpecificAuthorityTests(unittest.TestCase):
    def test_beast_armed_can_pass_without_structural_buy(self):
        s = structural(state="ARMED", strength=78, setup="COILED_ACCUMULATION")
        out = v._evidence_gate(s, {}, {}, {}, early(), now_ms=1_000_000)
        self.assertTrue(out["pass"], out["blockers"])
        self.assertEqual(out["strategy_engine"], "BEAST")
        self.assertTrue(out["global_safety_pass"])

    def test_exhaustion_does_not_require_breakout_volume(self):
        s = structural(state="ARMED", strength=74, setup="DEEP_PULLBACK_EXHAUSTION")
        row = early()
        row.update({
            "relative_volume_10s": 0.72,
            "relative_volume_30s": 0.81,
            "trade_acceleration": 0.82,
            "buy_ratio": 0.55,
            "cvd_acceleration": 0.12,
            "ofi": 0.08,
            "ofi_acceleration": 0.04,
        })
        out = v._evidence_gate(s, {}, {}, {}, row, now_ms=1_000_000)
        self.assertTrue(out["pass"], out["blockers"])
        self.assertEqual(out["strategy_engine"], "EXHAUSTION")

    def test_breakout_uses_anti_fakeout_engine(self):
        s = structural(state="BUY", strength=91, setup="COMPRESSION_BREAKOUT")
        out = v._evidence_gate(s, {}, {}, {}, early(), now_ms=1_000_000)
        self.assertTrue(out["pass"], out["blockers"])
        self.assertEqual(out["strategy_engine"], "BREAKOUT")

    def test_breakout_cannot_bypass_anti_fakeout_via_generic_lane(self):
        s = structural(state="BUY", strength=95, setup="COMPRESSION_BREAKOUT")
        row = early()
        row.update({
            "buy_ratio": 0.50,
            "cvd_acceleration": -0.4,
            "ofi": -0.2,
            "ofi_acceleration": -0.1,
            "obi": -0.1,
            "ask_depletion": -0.02,
            "relative_volume_10s": 0.8,
            "relative_volume_30s": 0.8,
            "trade_acceleration": 0.8,
            "hazard_score": 60,
            "v128_probe_score": 20,
            "change_point_delta": 0,
        })
        out = v._evidence_gate(s, {}, {}, {}, row, now_ms=1_000_000)
        self.assertFalse(out["pass"])
        breakout = next(x for x in out["strategy_engines"] if x["engine"] == "BREAKOUT")
        generic = next(x for x in out["strategy_engines"] if x["engine"] == "STRUCTURAL_CONFIRMATION")
        self.assertFalse(breakout["pass"])
        self.assertFalse(generic["pass"])
        self.assertIn("SPECIALISED_SETUP_OWNS_AUTHORITY", generic["blockers"])

    def test_generic_structural_buy_only_needs_setup_specific_confirmation(self):
        s = structural(state="BUY", strength=88, setup="DAILY_EMA200_REJECTION")
        row = early()
        row.update({
            "buy_ratio": 0.55,
            "cvd_acceleration": 0.1,
            "ofi": -0.1,
            "ofi_acceleration": -0.1,
            "obi": -0.1,
            "ask_depletion": -0.01,
            "relative_volume_10s": 0.8,
            "relative_volume_30s": 0.8,
            "trade_acceleration": 0.8,
            "v128_probe_score": 0,
            "change_point_delta": 0,
        })
        out = v._evidence_gate(s, {}, {}, {}, row, now_ms=1_000_000)
        self.assertTrue(out["pass"], out["blockers"])
        self.assertEqual(out["strategy_engine"], "STRUCTURAL_CONFIRMATION")

    def test_stale_sensor_is_still_a_global_veto(self):
        row = early()
        row["trade_age_ms"] = 5000
        s = structural(state="ARMED", strength=80, setup="COILED_ACCUMULATION")
        out = v._evidence_gate(s, {}, {}, {}, row, now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("STALE_SENSOR_TRADE", out["blockers"])

    def test_anti_chase_is_still_a_global_veto(self):
        s = structural(state="ARMED", strength=80, setup="COILED_ACCUMULATION")
        s["anti_chase"] = True
        out = v._evidence_gate(s, {}, {}, {}, early(), now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("ANTI_CHASE", out["blockers"])

    def test_bad_risk_plan_is_still_a_global_veto(self):
        s = structural(state="ARMED", strength=80, setup="DEEP_PULLBACK_EXHAUSTION")
        s["invalidation"] = 100.5
        out = v._evidence_gate(s, {}, {}, {}, early(), now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("VALID_RISK_REWARD", out["blockers"])

    def test_entry_far_from_structure_is_global_veto(self):
        row = early()
        row["entry_reference"] = 94.0
        s = structural(state="ARMED", strength=80, setup="COILED_ACCUMULATION")
        s["invalidation"] = 90.0
        out = v._evidence_gate(s, {}, {}, {}, row, now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("ENTRY_TOO_FAR_FROM_STRUCTURE", out["blockers"])


if __name__ == "__main__":
    unittest.main()
