import unittest

import psi_v12_8_upgrade as v128


class FakeV125:
    EARLY_PROMOTION_SLOTS = 16
    _latest_candidates = []


class V128Tests(unittest.TestCase):
    def setUp(self):
        v128._memory.clear()
        v128._sticky_until.clear()
        v128._stats.clear()
        v128.V125 = FakeV125

    def row(self, **kw):
        x = {
            "symbol": "AAAUSDT",
            "hard_sensor_safety": True,
            "trade_age_ms": 100,
            "book_age_ms": 100,
            "sequence_verified": True,
            "book_sequence_verified": True,
            "hazard_score": 82,
            "change_point_delta": 14,
            "buy_ratio": 0.78,
            "relative_volume_10s": 5.0,
            "trade_acceleration": 5.5,
            "ofi": 0.12,
            "obi": 0.18,
            "alignment_score": 72,
            "sequence_order_score": 78,
            "momentum_score": 70,
            "sequence_persistence": 55,
            "sequence_score": 80,
            "entry_reference": 1.0,
        }
        x.update(kw)
        return x

    def test_exceptional_sequence_becomes_early_probe(self):
        row = self.row()
        v128._enrich_probe(row)
        self.assertEqual(row["v128_probe_state"], "EARLY_PROBE")
        self.assertTrue(row["v128_sticky"])

    def test_stale_data_never_becomes_probe(self):
        row = self.row(trade_age_ms=v128.FRESH_MS + 1)
        v128._enrich_probe(row)
        self.assertNotEqual(row["v128_probe_state"], "EARLY_PROBE")

    def test_negative_flow_aborts_existing_probe(self):
        first = self.row()
        v128._enrich_probe(first)
        second = self.row(ofi=-0.25, obi=-0.30, buy_ratio=0.40)
        v128._enrich_probe(second)
        self.assertEqual(second["v128_probe_state"], "PROBE_ABORT")
        self.assertFalse(second["v128_sticky"])

    def test_sticky_symbols_are_promoted_first(self):
        row = self.row()
        v128._enrich_probe(row)
        FakeV125._latest_candidates = [row]
        v128._original_promotion_symbols = lambda: ["BASEUSDT"]
        out = v128._promotion_wrapper()
        self.assertEqual(out[:2], ["AAAUSDT", "BASEUSDT"])

    def test_probe_does_not_modify_strict_authority(self):
        self.assertTrue(v128.STRICT_BUY_AUTHORITY_UNCHANGED)


if __name__ == "__main__":
    unittest.main()
