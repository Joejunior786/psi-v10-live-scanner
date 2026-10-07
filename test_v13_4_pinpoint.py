import time
import types
import unittest

import psi_v13_4_pinpoint as v


NOW = int(time.time() * 1000)


def structural(setup="COILED_ACCUMULATION", state="ARMED", strength=80.0):
    return {
        "symbol": "TESTUSDT",
        "state": state,
        "setup": setup,
        "setup_strength": strength,
        "current": 100.0,
        "entry": 100.0,
        "entry_low": 99.8,
        "entry_high": 100.2,
        "max_chase": 102.0,
        "invalidation": 98.0,
        "tp1": 103.0,
        "tp2": 106.0,
        "tp3": 110.0,
        "anti_chase": False,
        "active_setups": [
            {"name": setup, "state": state, "strength": strength}
        ],
    }


def early(**updates):
    row = {
        "symbol": "TESTUSDT",
        "hard_sensor_safety": True,
        "generated_ms": NOW - 100,
        "trade_age_ms": 100,
        "book_age_ms": 100,
        "sequence_verified": True,
        "book_sequence_verified": True,
        "hazard_score": 86.0,
        "v128_probe_score": 80.0,
        "sequence_score": 82.0,
        "buy_ratio": 0.64,
        "cvd_acceleration": 0.5,
        "ofi": 0.2,
        "ofi_acceleration": 0.1,
        "obi": 0.16,
        "ask_depletion": 0.08,
        "relative_volume_10s": 2.0,
        "relative_volume_30s": 1.5,
        "trade_acceleration": 2.0,
        "change_point_delta": 8.0,
        "entry_reference": 100.0,
    }
    row.update(updates)
    return row


def micro(**updates):
    row = {
        "micro_ready": True,
        "sequence_verified": True,
        "book_sequence_verified": True,
        "spread_bps": 5.0,
        "slippage_bps": 8.0,
    }
    row.update(updates)
    return row


def integrity(**updates):
    row = {
        "ages": {
            "micro_trade_ms": 100,
            "micro_book_ms": 100,
            "tape_ms": 100,
            "bbo_ms": 100,
        }
    }
    row.update(updates)
    return row


class EarlyStub:
    def __init__(self, row):
        self.row = row

    def early_candidates(self, limit, actionable_only=False):
        return [self.row]


class MLStub:
    @staticmethod
    def ml_probability(symbol, structural_row, legacy_row):
        return {
            "symbol": symbol,
            "qualified": False,
            "probability": 0.66,
            "target_pct": 10.0,
            "horizon": "24h_before_invalidation",
            "calibration_key": "COHORT::TEST",
            "overall_samples": 500,
            "test_samples": 100,
            "overall_ci_low": 0.56,
            "test_ci_low": 0.54,
            "reason": "INDEPENDENT_READY",
        }


class WeakMLStub:
    @staticmethod
    def ml_probability(symbol, structural_row, legacy_row):
        return {
            "symbol": symbol,
            "qualified": False,
            "probability": 0.54,
            "target_pct": 10.0,
            "horizon": "24h_before_invalidation",
            "calibration_key": "COHORT::WEAK",
            "overall_samples": 500,
            "test_samples": 100,
            "overall_ci_low": 0.50,
            "test_ci_low": 0.49,
            "reason": "BELOW_THRESHOLD",
        }


class PinpointV134Tests(unittest.TestCase):
    def setUp(self):
        v.ACTIVE.clear()
        v.STATS.clear()
        v._EARLY_CACHE = {}
        v._EARLY_CACHE_MS = 0
        v._ORIGINAL_GATE = lambda *args, **kwargs: {
            "buy_now": False,
            "execution_state": "COLLECTING DATA",
            "blockers": ["LEGACY_UNIVERSAL_BLOCKERS"],
            "persistence_passes": 0,
            "authority_chain": "OLD",
            "pinpoint_entry_status": "NO_SETUP",
            "pinpoint_state": "WATCH",
        }
        v.EARLY = EarlyStub(early())
        v.V124 = WeakMLStub()
        v.V13 = types.SimpleNamespace(_ranked=[])

    def test_beast_can_approve_armed_without_structural_buy(self):
        out = v._gate_wrapper(structural(state="ARMED"), {}, micro(), integrity())
        self.assertTrue(out["buy_now"], out)
        self.assertEqual(out["execution_route"], "SETUP_BEAST")
        self.assertEqual(out["v13_4_setup_engine"], "BEAST")
        self.assertNotIn("V12_STRUCTURAL_BUY", out["blockers"])

    def test_exhaustion_engine_approves_live_buyer_takeover(self):
        s = structural("DEEP_PULLBACK_EXHAUSTION", "ARMED", 72.0)
        out = v._gate_wrapper(s, {}, micro(), integrity())
        self.assertTrue(out["buy_now"], out)
        self.assertEqual(out["v13_4_setup_engine"], "EXHAUSTION")

    def test_breakout_engine_uses_anti_fakeout_confirmation(self):
        s = structural("COMPRESSION_BREAKOUT", "ARMED", 78.0)
        out = v._gate_wrapper(s, {}, micro(), integrity())
        self.assertTrue(out["buy_now"], out)
        self.assertEqual(out["v13_4_setup_engine"], "BREAKOUT")
        chosen = out["v13_4_setup_decision"]["chosen"]
        self.assertTrue(chosen["anti_fakeout"]["flow_guard"])
        self.assertTrue(chosen["anti_fakeout"]["sequence_guard"])

    def test_breakout_fakeout_is_blocked_when_flow_is_weak(self):
        weak = early(
            buy_ratio=0.50,
            cvd_acceleration=-0.2,
            ofi=-0.1,
            ofi_acceleration=-0.1,
            obi=-0.1,
            ask_depletion=-0.02,
            relative_volume_10s=0.8,
            relative_volume_30s=0.8,
            trade_acceleration=0.9,
            change_point_delta=0.0,
        )
        v.EARLY = EarlyStub(weak)
        s = structural("COMPRESSION_BREAKOUT", "ARMED", 78.0)
        out = v._gate_wrapper(s, {}, micro(), integrity())
        self.assertFalse(out["buy_now"])
        breakout = next(
            row for row in out["v13_4_setup_decision"]["engines"]
            if row["engine"] == "BREAKOUT"
        )
        self.assertIn("BREAKOUT_ANTI_FAKEOUT_FLOW", breakout["blockers"])

    def test_stale_or_missing_execution_data_remains_hard_veto(self):
        out = v._gate_wrapper(
            structural(),
            {},
            micro(micro_ready=False),
            integrity(),
        )
        self.assertFalse(out["buy_now"])
        self.assertIn("LIVE_MICRO_DATA", out["v13_4_hard_safety"]["blockers"])

    def test_ml10_can_approve_independently_of_setup_rules(self):
        v.V124 = MLStub()
        s = structural("DAILY_EMA200_REACTION", "ARMED", 65.0)
        out = v._gate_wrapper(s, {}, micro(), integrity())
        self.assertTrue(out["buy_now"], out)
        self.assertEqual(out["execution_route"], "ML10_INDEPENDENT")
        self.assertTrue(out["v13_4_ml_independent_buy"])
        self.assertFalse(out["v13_4_setup_buy"])
        self.assertAlmostEqual(out["v13_4_plus10_probability"], 0.66)

    def test_ml10_cannot_bypass_hard_safety(self):
        v.V124 = MLStub()
        s = structural("DAILY_EMA200_REACTION", "ARMED", 65.0)
        bad_integrity = integrity()
        bad_integrity["ages"]["tape_ms"] = 9000
        out = v._gate_wrapper(s, {}, micro(), bad_integrity)
        self.assertFalse(out["buy_now"])
        self.assertIn("STALE_EVENT_TAPE", out["v13_4_hard_safety"]["blockers"])

    def test_every_reported_candidate_gets_plus10_field(self):
        v.CORE = types.SimpleNamespace(
            _board=lambda: [
                structural(),
                {**structural("BREAKOUT_RETEST", "BUY", 90.0), "symbol": "TWOUSDT"},
            ],
            q=types.SimpleNamespace(latest={}),
        )
        v.V124 = MLStub()
        v.V13 = types.SimpleNamespace(_ranked=[
            {"symbol": "TESTUSDT", "model_probability": 0.70, "rank": 1},
            {"symbol": "TWOUSDT", "model_probability": 0.61, "rank": 2},
        ])
        rows = v._candidate_probabilities(30)
        self.assertEqual(len(rows), 2)
        self.assertTrue(all("plus10_probability" in row for row in rows))
        self.assertTrue(all(row["plus10_probability"] == 0.66 for row in rows))
        self.assertEqual(rows[0]["symbol"], "TESTUSDT")


if __name__ == "__main__":
    unittest.main()
