import types
import unittest

import psi_v12_7_upgrade as v127


class FakeV125:
    EARLY_PROMOTION_SLOTS = 16
    _latest_candidates = []


class FakeCore:
    def __init__(self, board):
        self._rows = board
        self.legacy = types.SimpleNamespace(rescue=None)
    def _board(self):
        return self._rows


class FakeV124:
    ML_OVERRIDE_THRESHOLD = 0.70
    @staticmethod
    def ml_probability(symbol, structural, legacy):
        return {"qualified": True, "probability": 0.82, "calibration_key": "TEST"}


class V127Tests(unittest.TestCase):
    def setUp(self):
        v127._history.clear()
        v127._stats.clear()
        v127._blocker_learning_cache = {}
        v127._blocker_learning_n = 0
        v127._blocker_learning_mono = 0.0
        v127.V125 = FakeV125
        v127.V124 = FakeV124

    def _row(self, **kw):
        d = {
            "symbol": "AAAUSDT", "hard_sensor_safety": True,
            "trade_age_ms": 100, "book_age_ms": 100,
            "sequence_verified": True, "book_sequence_verified": True,
            "hazard_score": 92, "change_point_delta": 14,
            "buy_ratio": 0.72, "relative_volume_10s": 4.0,
            "trade_acceleration": 4.0, "cvd_acceleration": 1.0,
            "ofi": 0.22, "ofi_acceleration": 0.15,
            "obi": 0.24, "ask_depletion": 0.03,
            "entry_reference": 101.5,
        }
        d.update(kw)
        return d

    def test_sequence_rewards_ordered_activity_flow_book_thrust(self):
        sym = "AAAUSDT"
        seq = [
            self._row(hazard_score=55, change_point_delta=1, buy_ratio=.52, ofi=0, obi=0, relative_volume_10s=2.0),
            self._row(hazard_score=62, change_point_delta=4, buy_ratio=.64, ofi=.12, obi=.02),
            self._row(hazard_score=72, change_point_delta=9, buy_ratio=.68, ofi=.16, obi=.16),
            self._row(),
        ]
        for row in seq:
            v127._history[sym].append(v127._snapshot(row))
        m = v127._sequence_metrics(sym, seq[-1])
        self.assertGreaterEqual(m["sequence_order_score"], 80)
        self.assertGreaterEqual(m["alignment_score"], 75)

    def test_stale_candidate_cannot_be_sequence_actionable(self):
        row = self._row(trade_age_ms=v127.FRESH_MS + 1)
        v127._history[row["symbol"]].append(v127._snapshot(row))
        m = v127._sequence_metrics(row["symbol"], row)
        self.assertFalse(m["fresh_1200ms"])
        self.assertEqual(m["sequence_state"], "SEQUENCE_DATA_WAIT")

    def test_contextual_extension_never_bypasses_hard_blocker(self):
        v127._original_hard_safety = lambda *a, **k: {
            "pass": False, "blockers": ["CUMULATIVE_EXTENSION_GUARD", "SPREAD_FILTER"],
            "risk": {"valid": True, "entry": 100, "stop": 98, "risk_pct": 2},
        }
        out = v127._adaptive_hard_safety({}, {}, {}, {})
        self.assertFalse(out["pass"])
        self.assertIn("SPREAD_FILTER", out["blockers"])

    def test_exceptional_fresh_sequence_can_contextualize_extension(self):
        row = self._row()
        sym = row["symbol"]
        for _ in range(6):
            v127._history[sym].append(v127._snapshot(row))
        row.update(v127._sequence_metrics(sym, row))
        row["sequence_score"] = 95
        row["alignment_score"] = 95
        row["fresh_1200ms"] = True
        FakeV125._latest_candidates = [row]
        v127.CORE = FakeCore([{
            "symbol": sym, "entry": 100.0, "current": 101.6,
            "max_chase": 101.5, "counter_trend": False,
        }])
        integrity = {"ages": {"micro_trade_ms":100,"micro_book_ms":100,"tape_ms":100,"bbo_ms":100}}
        d = v127._adaptive_extension(v127.CORE._board()[0], {"symbol": sym}, integrity)
        self.assertTrue(d["pass"])
        self.assertGreater(d["dynamic_max_chase"], 101.5)

    def test_promotion_prefers_sequence_candidates(self):
        FakeV125._latest_candidates = [
            {"symbol":"SEQUSDT","sequence_state":"SEQUENCE_PINPOINT","sequence_score":95,"alignment_score":90,"hazard_score":80},
            {"symbol":"ARMUSDT","sequence_state":"SEQUENCE_ARMED","sequence_score":82,"alignment_score":80,"hazard_score":85},
        ]
        v127._original_promotion_symbols = lambda: ["BASEUSDT"]
        out = v127._promotion_wrapper()
        self.assertEqual(out[:3], ["SEQUSDT", "ARMUSDT", "BASEUSDT"])


if __name__ == "__main__":
    unittest.main()
