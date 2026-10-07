import unittest
import psi_v13_3_execution as v

def structural(state="BUY", strength=92.0):
    return {
        "symbol":"TESTUSDT","state":state,"setup_strength":strength,
        "current":100.0,"entry_low":100.0,"entry":100.0,"max_chase":102.0,
        "invalidation":98.0,"tp1":103.0,"tp2":106.0,"tp3":110.0,
        "anti_chase":False,
    }

def early(now=1_000_000, state="EARLY_ARMED"):
    return {
        "symbol":"TESTUSDT","state":state,"hard_sensor_safety":True,
        "hazard_score":86.0,"entry_reference":100.0,"generated_ms":now-100,
        "trade_age_ms":100,"book_age_ms":100,"sequence_verified":True,
        "book_sequence_verified":True,"spread_bps":5.0,"slippage_bps":8.0,
        "buy_ratio":0.62,"cvd_acceleration":1.0,"ofi":0.2,
        "ofi_acceleration":0.1,"obi":0.15,"ask_depletion":0.05,
        "relative_volume_10s":1.4,"relative_volume_30s":1.2,
        "trade_acceleration":1.3,
    }

class EvidenceGateTests(unittest.TestCase):
    def test_strong_fresh_evidence_passes(self):
        out=v._evidence_gate(structural(),{}, {},{},early(),now_ms=1_000_000)
        self.assertTrue(out["pass"],out["blockers"])
        self.assertGreaterEqual(out["flow_group_count"],4)
        self.assertGreater(out["rr_tp1"],1.35)

    def test_stale_sensor_fails_closed(self):
        row=early(); row["trade_age_ms"]=5000
        out=v._evidence_gate(structural(),{}, {},{},row,now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("STALE_SENSOR_TRADE",out["blockers"])

    def test_weak_flow_fails_closed(self):
        row=early()
        row.update({"buy_ratio":0.50,"cvd_acceleration":-1.0,"ofi":-0.2,
                    "ofi_acceleration":-0.1,"obi":-0.1,"ask_depletion":-0.02,
                    "relative_volume_10s":0.7,"relative_volume_30s":0.8,
                    "trade_acceleration":0.8})
        out=v._evidence_gate(structural(),{}, {},{},row,now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertTrue(any(x.startswith("FLOW_GROUPS_") for x in out["blockers"]))
        self.assertIn("POSITIVE_CVD_ACCELERATION",out["blockers"])

    def test_armed_needs_strong_structure(self):
        out=v._evidence_gate(structural(strength=85),{}, {},{},
                             early(state="EARLY_ARMED"),now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("ARMED_REQUIRES_STRONG_STRUCTURE",out["blockers"])

    def test_pinpoint_can_use_base_strength(self):
        row=early(state="EARLY_PINPOINT"); row["hazard_score"]=92
        out=v._evidence_gate(structural(strength=85),{}, {},{},row,now_ms=1_000_000)
        self.assertTrue(out["pass"],out["blockers"])

    def test_bad_risk_plan_fails_closed(self):
        s=structural(); s["invalidation"]=100.5
        out=v._evidence_gate(s,{}, {},{},early(),now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("VALID_RISK_REWARD",out["blockers"])

    def test_entry_far_from_structure_fails(self):
        row=early(); row["entry_reference"]=94.0
        s=structural(); s["invalidation"]=90.0
        out=v._evidence_gate(s,{}, {},{},row,now_ms=1_000_000)
        self.assertFalse(out["pass"])
        self.assertIn("ENTRY_TOO_FAR_FROM_STRUCTURE",out["blockers"])

if __name__=="__main__":
    unittest.main()
