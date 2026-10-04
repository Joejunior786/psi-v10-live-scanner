import os
import time
import unittest

os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

import psi_micro_worker as worker
import psi_v12_3_hardening as hardening


class MicroStabilityTests(unittest.TestCase):
    def test_full_pool_flip_is_bounded(self):
        active={f"OLD{i}USDT" for i in range(80)}
        wanted=[f"NEW{i}USDT" for i in range(80)]
        now=1000.0
        activated={sym:0.0 for sym in active}
        next_active, added, removed, diag=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(len(next_active),80)
        self.assertEqual(len(added),2)
        self.assertEqual(len(removed),2)
        self.assertEqual(diag["raw_add"],80)
        self.assertEqual(diag["raw_remove"],80)

    def test_minimum_hold_prevents_eviction(self):
        active={f"OLD{i}USDT" for i in range(80)}
        wanted=[f"NEW{i}USDT" for i in range(80)]
        now=1000.0
        activated={sym:950.0 for sym in active}
        next_active, added, removed, diag=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(next_active,active)
        self.assertFalse(added)
        self.assertFalse(removed)
        self.assertEqual(diag["held_for_dwell"],80)

    def test_activity_rank_prefers_fresh_active_tape(self):
        active = {
            "ready": True,
            "age_ms": 300.0,
            "book_age_ms": 400.0,
            "trades_5s": 8,
            "notional_5s": 25000.0,
            "spread_bps": 2.0,
        }
        stale = {
            "ready": False,
            "age_ms": 25000.0,
            "book_age_ms": 25000.0,
            "trades_5s": 0,
            "notional_5s": 0.0,
            "spread_bps": 25.0,
        }
        self.assertGreater(
            hardening._activity_sort_key(active, 1_000_000.0),
            hardening._activity_sort_key(stale, 50_000_000.0),
        )

    def test_rapid_score_beats_absolute_liquidity_bias(self):
        small_fast_mover = {
            "ready": True,
            "age_ms": 400.0,
            "book_age_ms": 700.0,
            "trades_5s": 6,
            "notional_5s": 2500.0,
            "spread_bps": 6.0,
        }
        huge_liquid_normal = {
            "ready": True,
            "age_ms": 250.0,
            "book_age_ms": 300.0,
            "trades_5s": 60,
            "notional_5s": 5_000_000.0,
            "spread_bps": 1.0,
        }
        self.assertGreater(
            hardening._activity_sort_key(
                small_fast_mover, 800_000.0, hardening.RAPID_MIN_SCORE + 20.0
            ),
            hardening._activity_sort_key(
                huge_liquid_normal, 2_000_000_000.0, 0.0
            ),
        )

    def test_subthreshold_rapid_does_not_override_normal_activity_rank(self):
        normal_active = {
            "ready": True,
            "age_ms": 250.0,
            "book_age_ms": 300.0,
            "trades_5s": 25,
            "notional_5s": 500_000.0,
            "spread_bps": 2.0,
        }
        weak_challenger = {
            "ready": True,
            "age_ms": 300.0,
            "book_age_ms": 350.0,
            "trades_5s": 2,
            "notional_5s": 1000.0,
            "spread_bps": 8.0,
        }
        self.assertGreater(
            hardening._activity_sort_key(normal_active, 50_000_000.0, 0.0),
            hardening._activity_sort_key(
                weak_challenger,
                500_000.0,
                hardening.RAPID_MIN_SCORE - 5.0,
            ),
        )

    def test_spare_capacity_fills_without_eviction(self):
        active={f"OLD{i}USDT" for i in range(40)}
        wanted=[f"OLD{i}USDT" for i in range(40)] + [f"NEW{i}USDT" for i in range(40)]
        now=1000.0
        activated={sym:995.0 for sym in active}
        next_active, added, removed, _=worker._bounded_control_plan(
            active,
            wanted,
            activated,
            now,
            max_replacements=2,
            min_hold_seconds=120.0,
            max_symbols=80,
        )
        self.assertEqual(len(next_active),42)
        self.assertEqual(len(added),2)
        self.assertFalse(removed)


    def test_true_low_market_cap_is_eligible(self):
        profile = hardening._lowcap_cap_profile_values(
            quote_volume_24h=5_000_000.0,
            market_cap_usd=80_000_000.0,
        )
        self.assertTrue(profile["eligible"])
        self.assertEqual(profile["band"], "LOW_CAP")
        self.assertEqual(profile["source"], "MARKET_CAP")

    def test_quote_volume_fallback_is_explicitly_proxy_only(self):
        profile = hardening._lowcap_cap_profile_values(
            quote_volume_24h=8_000_000.0,
            market_cap_usd=0.0,
        )
        self.assertTrue(profile["eligible"])
        self.assertEqual(profile["band"], "LOW_CAP_PROXY")
        self.assertEqual(profile["source"], "QUOTE_VOLUME_PROXY")

    def test_volume_before_price_beats_same_flow_after_vertical_chase(self):
        common = {
            "notional_accel_1s": 3.2,
            "trade_count_accel_1s": 2.8,
            "avg_trade_shift_1s": 1.8,
            "notional_15s": 300_000.0,
            "notional_30s": 390_000.0,
            "buy_ratio_1s": 0.72,
            "cvd_accel": 0.22,
            "bbo_imbalance": 0.18,
        }
        quiet = dict(common, price_velocity_5s_pct=0.8)
        chased = dict(common, price_velocity_5s_pct=5.5)
        quiet_row = hardening._score_lowcap_candidate(
            "QUIETUSDT",
            quote_volume_24h=5_000_000.0,
            rapid_score=100.0,
            tape_metric=quiet,
        )
        chased_row = hardening._score_lowcap_candidate(
            "CHASEUSDT",
            quote_volume_24h=5_000_000.0,
            rapid_score=100.0,
            tape_metric=chased,
        )
        self.assertGreater(quiet_row["score"], chased_row["score"])
        self.assertGreater(
            quiet_row["components"]["volume_before_price"],
            chased_row["components"]["volume_before_price"],
        )

    def test_flow_flip_materially_improves_lowcap_score(self):
        base_tape = {
            "notional_accel_1s": 2.4,
            "trade_count_accel_1s": 2.2,
            "avg_trade_shift_1s": 1.4,
            "notional_15s": 200_000.0,
            "notional_30s": 300_000.0,
            "price_velocity_5s_pct": 0.7,
            "bbo_imbalance": 0.05,
        }
        weak = hardening._score_lowcap_candidate(
            "FLOWUSDT",
            quote_volume_24h=4_000_000.0,
            tape_metric=dict(base_tape, buy_ratio_1s=0.50, cvd_accel=-0.10),
            micro_metric={"ofi_acceleration": -0.05},
        )
        strong = hardening._score_lowcap_candidate(
            "FLOWUSDT",
            quote_volume_24h=4_000_000.0,
            tape_metric=dict(base_tape, buy_ratio_1s=0.79, cvd_accel=0.38),
            micro_metric={"ofi_acceleration": 0.22, "obi": 0.30, "ask_depletion": 0.18},
        )
        self.assertGreater(strong["score"], weak["score"] + 10.0)
        self.assertGreater(strong["components"]["flow_flip"], weak["components"]["flow_flip"])

    def test_lowcap_engine_has_no_execution_authority(self):
        row = hardening._score_lowcap_candidate(
            "HOTUSDT",
            quote_volume_24h=2_000_000.0,
            rapid_score=150.0,
            tape_metric={
                "notional_accel_1s": 4.0,
                "trade_count_accel_1s": 3.5,
                "avg_trade_shift_1s": 2.0,
                "notional_15s": 400_000.0,
                "notional_30s": 500_000.0,
                "price_velocity_5s_pct": 0.5,
                "buy_ratio_1s": 0.85,
                "cvd_accel": 0.5,
                "bbo_imbalance": 0.4,
            },
            micro_metric={"ofi_acceleration": 0.3, "obi": 0.4, "ask_depletion": 0.25},
            latest_row={"resistance_fatigue": 75.0, "state": "PULLBACK_EXHAUSTED"},
        )
        self.assertGreaterEqual(row["score"], hardening.LOWCAP_MIN_SCORE)
        self.assertFalse(row["execution_authority"])
        self.assertEqual(row["role"], "DISCOVERY_PROMOTION_ONLY")


if __name__ == "__main__":
    unittest.main()
