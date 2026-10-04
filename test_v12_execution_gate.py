import unittest

import psi_strategy_v12_entry as v12


def structural_buy(setup="HIGH_CONFLUENCE_BUY"):
    return {
        "symbol": "TESTUSDT",
        "state": "BUY",
        "setup": setup,
        "anti_chase": False,
        "current": 100.0,
        "max_chase": 102.0,
        "entry": 100.0,
        "tp1": 103.0,
        "tp2": 106.0,
        "tp3": 110.0,
    }


def hard_status():
    return {name: True for name in v12._EXECUTION_HARD_KEYS}


def approved_legacy():
    return {
        "pinpoint_hard_status": hard_status(),
        "pinpoint_live_tape_pass": True,
        "pinpoint_anti_chase_ok": True,
        "pinpoint_persistence_ok": True,
        "pinpoint_persistence_passes": 2,
        "pinpoint_trigger": 100.0,
        "pinpoint_stop": 97.0,
        "pinpoint_risk_pct": 3.0,
        "pinpoint_buy": True,
        "strict_buy_gate_passed": True,
        "pinpoint_state": "BUY NOW",
        "pinpoint_entry_status": "PINPOINT_TRIGGERED",
        "state": "BUY NOW",
        "formal_state": "BUY NOW",
        "integrity_blockers": [],
        "pinpoint_blockers": [],
        "combined_blockers": [],
    }


def good_micro():
    return {
        "micro_ready": True,
        "sequence_verified": True,
        "book_sequence_verified": True,
    }


def good_integrity():
    return {
        "verified": True,
        "blockers": [],
        "ages": {
            "micro_trade_ms": 1000.0,
            "micro_book_ms": 500.0,
            "tape_ms": 500.0,
            "bbo_ms": 500.0,
        },
    }


class StrictExecutionGateTests(unittest.TestCase):
    def test_structural_buy_missing_micro_fails_closed(self):
        micro = good_micro()
        micro["micro_ready"] = False
        integrity = good_integrity()
        integrity["verified"] = False
        integrity["blockers"] = ["MICRO_NOT_READY"]
        result = v12._strict_execution_gate(
            structural_buy(), approved_legacy(), micro, integrity
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("LIVE_MICRO_DATA", result["blockers"])

    def test_structural_buy_stale_tape_fails_closed(self):
        integrity = good_integrity()
        integrity["verified"] = False
        integrity["blockers"] = ["STALE_EVENT_TAPE"]
        integrity["ages"]["tape_ms"] = 6001.0
        result = v12._strict_execution_gate(
            structural_buy(), approved_legacy(), good_micro(), integrity
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("LIVE_TAPE", result["blockers"])

    def test_bad_spread_and_slippage_fail_closed(self):
        legacy = approved_legacy()
        legacy["pinpoint_hard_status"]["SPREAD_FILTER"] = False
        legacy["pinpoint_hard_status"]["SLIPPAGE_FILTER"] = False
        result = v12._strict_execution_gate(
            structural_buy(), legacy, good_micro(), good_integrity()
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("SPREAD_FILTER", result["blockers"])
        self.assertIn("SLIPPAGE_FILTER", result["blockers"])

    def test_persistence_one_of_two_fails_closed(self):
        legacy = approved_legacy()
        legacy["pinpoint_persistence_ok"] = False
        legacy["pinpoint_persistence_passes"] = 1
        result = v12._strict_execution_gate(
            structural_buy(), legacy, good_micro(), good_integrity()
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("PINPOINT_PERSISTENCE_1/2", result["blockers"])

    def test_monster_six_of_six_cannot_bypass_execution(self):
        row = structural_buy()
        row["monster_layers"] = 6
        row["monster_state"] = "MONSTER-IGNITION"
        legacy = approved_legacy()
        legacy["pinpoint_buy"] = False
        legacy["strict_buy_gate_passed"] = False
        legacy["pinpoint_state"] = "WATCH"
        legacy["pinpoint_entry_status"] = "NO_SETUP"
        legacy["state"] = "PRE-IGNITION"
        legacy["formal_state"] = "PRE-IGNITION"
        result = v12._strict_execution_gate(
            row, legacy, good_micro(), good_integrity()
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("PINPOINT_BUY_APPROVAL", result["blockers"])

    def test_pullback_structure_cannot_bypass_execution(self):
        row = structural_buy("DEEP_PULLBACK_EXHAUSTION")
        legacy = approved_legacy()
        legacy["pinpoint_buy"] = False
        legacy["strict_buy_gate_passed"] = False
        legacy["pinpoint_state"] = "WATCH"
        legacy["pinpoint_entry_status"] = "NO_SETUP"
        result = v12._strict_execution_gate(
            row, legacy, good_micro(), good_integrity()
        )
        self.assertFalse(result["buy_now"])
        self.assertIn("PINPOINT_BUY_APPROVAL", result["blockers"])

    def test_all_gates_true_allows_buy_now(self):
        result = v12._strict_execution_gate(
            structural_buy(), approved_legacy(), good_micro(), good_integrity()
        )
        self.assertTrue(result["buy_now"], result["blockers"])
        self.assertEqual(result["execution_state"], "BUY NOW")
        self.assertEqual(result["blockers"], [])


if __name__ == "__main__":
    unittest.main()
