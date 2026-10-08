import math
import types
import unittest

import psi_v15_multihorizon as v15


class V15MultiHorizonTests(unittest.TestCase):
    def setUp(self):
        self.models = dict(v15._models)
        self.pending = list(v15._pending)
        self.recent = list(v15._recent)
        self.last_signal = dict(v15._last_signal)
        self.seen_seed = set(v15._seen_seed)
        self.seed_buckets = set(v15._seed_buckets)
        self.entry_seed_seen = set(v15._entry_seed_seen)
        self.entry_excursion_stats = dict(v15._entry_excursion_stats)
        self.lane_stats = dict(v15._lane_stats)
        self.board = list(v15._board)
        self.signal_journal = list(v15._signal_journal)
        self.signal_seen = set(v15._signal_seen)
        self.stats = dict(v15._stats)
        self.core, self.v13, self.outcome, self.v14 = v15.CORE, v15.V13, v15.OUTCOME, v15.V14
        v15._models.clear()
        v15._pending.clear()
        v15._recent.clear()
        v15._last_signal.clear()
        v15._seen_seed.clear()
        v15._seed_buckets.clear()
        v15._entry_seed_seen.clear()
        v15._entry_excursion_stats.clear()
        v15._lane_stats.clear()
        v15._board.clear()
        v15._signal_journal.clear()
        v15._signal_seen.clear()
        v15._stats.clear()

    def tearDown(self):
        v15._models.clear(); v15._models.update(self.models)
        v15._pending[:] = self.pending
        v15._recent.clear(); v15._recent.extend(self.recent)
        v15._last_signal.clear(); v15._last_signal.update(self.last_signal)
        v15._seen_seed.clear(); v15._seen_seed.update(self.seen_seed)
        v15._seed_buckets.clear(); v15._seed_buckets.update(self.seed_buckets)
        v15._entry_seed_seen.clear(); v15._entry_seed_seen.update(self.entry_seed_seen)
        v15._entry_excursion_stats.clear(); v15._entry_excursion_stats.update(self.entry_excursion_stats)
        v15._lane_stats.clear(); v15._lane_stats.update(self.lane_stats)
        v15._board[:] = self.board
        v15._signal_journal[:] = self.signal_journal
        v15._signal_seen.clear(); v15._signal_seen.update(self.signal_seen)
        v15._stats.clear(); v15._stats.update(self.stats)
        v15.CORE, v15.V13, v15.OUTCOME, v15.V14 = self.core, self.v13, self.outcome, self.v14

    def sensor(self, now=1_000_000):
        return {
            "symbol": "AAAUSDT", "generated_ms": now, "trade_age_ms": 100, "book_age_ms": 100,
            "hard_sensor_safety": True, "sequence_verified": True, "book_sequence_verified": True,
            "spread_bps": 4, "slippage_bps": 8, "entry_reference": 100,
            "hazard_score": 70, "buy_ratio": .62, "relative_volume_10s": 2.2,
            "relative_volume_30s": 1.7, "trade_acceleration": 1.8,
            "cvd_acceleration": .3, "ofi": .2, "ofi_acceleration": .1,
            "obi": .15, "ask_depletion": .12, "sequence_score": 90,
            "v128_probe_score": 80,
        }

    def test_setup_specialists_are_distinct(self):
        self.assertEqual(v15._classify_lane({"setup": "seller exhaustion pullback"}), "EXHAUSTION")
        self.assertEqual(v15._classify_lane({"setup": "compression breakout retest"}), "BREAKOUT")
        self.assertEqual(v15._classify_lane({"setup": "daily 200 EMA reclaim"}), "HTF_SWING")
        self.assertEqual(v15._classify_lane({"setup": "micro ignition"}), "BEAST")

    def test_hard_live_safety_stays_fail_closed(self):
        ok, blockers = v15._safety(self.sensor(), at=1_000_500)
        self.assertTrue(ok)
        stale = self.sensor()
        stale["trade_age_ms"] = 5000
        ok, blockers = v15._safety(stale, at=1_000_500)
        self.assertFalse(ok)
        self.assertIn("STALE_TRADE", blockers)

    def _positive_ev_models(self, validated=False):
        p = 0.45
        bias = math.log(p / (1 - p))
        for h in v15.HORIZON_ORDER:
            for target in v15.TARGETS:
                m = v15._model("BEAST", h, target)
                m["bias"] = bias
                m["weights"] = [0.0] * v15.FEATURE_COUNT
                m["trained"] = 40
                m["wins"] = 18
                m["losses"] = 22
                m["payoff_win_sum"] = 18 * 15.0
                m["payoff_win_n"] = 18
                m["payoff_loss_sum"] = 22 * 3.0
                m["payoff_loss_n"] = 22
                if validated:
                    m["test_n"] = 30
                    m["test_wins"] = 14
                    m["test_losses"] = 16
                    m["test_correct"] = 15
                    m["test_brier_sum"] = 7.0
                    m["test_payoff_win_sum"] = 14 * 15.0
                    m["test_payoff_win_n"] = 14
                    m["test_payoff_loss_sum"] = 16 * 3.0
                    m["test_payoff_loss_n"] = 16

    def test_positive_ev_without_holdout_is_shadow_buy(self):
        x = v15._features_from_sensor(self.sensor(), {})
        self._positive_ev_models(validated=False)
        opp = v15._opportunity("BEAST", x)
        self.assertLess(opp["probability"], 0.5)
        self.assertGreater(opp["expected_value_pct"], 0)
        self.assertFalse(opp["promotion_validation"]["ready"])
        action, ready, blockers = v15._entry_action(
            self.sensor(), {}, opp, True, []
        )
        self.assertEqual(action, "ML SHADOW BUY")
        self.assertFalse(ready)
        self.assertIn("OUT_OF_SAMPLE_VALIDATION_PENDING", blockers)

    def test_positive_ev_below_fifty_percent_can_buy_after_holdout_validation(self):
        x = v15._features_from_sensor(self.sensor(), {})
        self._positive_ev_models(validated=True)
        opp = v15._opportunity("BEAST", x)
        self.assertLess(opp["probability"], 0.5)
        self.assertGreater(opp["expected_value_pct"], 0)
        self.assertTrue(opp["promotion_validation"]["ready"])
        action, ready, blockers = v15._entry_action(
            self.sensor(), {}, opp, True, []
        )
        self.assertEqual(action, "ML BUY NOW")
        self.assertTrue(ready)

    def test_test_split_never_changes_train_or_calibration_state(self):
        m = v15._new_model()
        x = [0.1] * v15.FEATURE_COUNT
        test_stamp = None
        for day in range(40):
            stamp = day * v15.DAY_MS
            if v15._split(stamp) == "TEST":
                test_stamp = stamp
                break
        self.assertIsNotNone(test_stamp)
        before = (
            list(m["weights"]), m["bias"], m["trained"], m["wins"], m["losses"],
            dict(m["cal_bins"]), m["payoff_win_n"], m["payoff_loss_n"],
        )
        v15._update_model(m, x, True, test_stamp, 6.0)
        after = (
            list(m["weights"]), m["bias"], m["trained"], m["wins"], m["losses"],
            dict(m["cal_bins"]), m["payoff_win_n"], m["payoff_loss_n"],
        )
        self.assertEqual(before, after)
        self.assertEqual(m["test_n"], 1)
        self.assertEqual(m["test_wins"], 1)

    def test_empirical_time_to_target_is_reported(self):
        for hours in (2, 3, 4, 5, 6, 7, 8, 9):
            v15._record_lane_stats("EXHAUSTION", 10, hours * 3600_000, 14, -2)
        med, lo, hi, source = v15._target_time("EXHAUSTION", 10, v15.HORIZONS_MS["24h"])
        self.assertEqual(source, "EMPIRICAL")
        self.assertGreaterEqual(med, 4 * 3600_000)
        self.assertIn("hours", v15._duration(med))

    def test_seed_uses_existing_outcome_memory_without_lookahead(self):
        created = 1_000_000
        event = {
            "id": "seed1", "symbol": "AAAUSDT", "created_ms": created,
            "setup": "breakout", "features": {"setup": "breakout", "buy_ratio": .6},
            "first_target_ms": {"3": created + 10 * 60_000, "5": created + 50 * 60_000},
            "stop_hit_ms": 0,
            "horizon_returns": {"15m": 3.2, "1h": 5.4, "4h": 6.0, "24h": 8.0},
            "mfe_pct": 9.0, "mae_pct": -1.0,
        }
        v15.OUTCOME = types.SimpleNamespace(_recent=[event])
        self.assertEqual(v15._seed_from_outcome_memory(), 1)
        m15 = v15._model("BREAKOUT", "15m", 3)
        m1h = v15._model("BREAKOUT", "1h", 5)
        self.assertGreater(m15["wins"], 0)
        self.assertGreater(m1h["wins"], 0)
        self.assertEqual(v15._seed_from_outcome_memory(), 0)

    def test_forced_top_five_keeps_ranked_waits(self):
        now = v15._now_ms()
        rows = []
        for i in range(7):
            row = self.sensor(now)
            row["symbol"] = f"C{i}USDT"
            row["entry_reference"] = 100 + i
            rows.append(row)
        v15.V13 = types.SimpleNamespace(_rows=lambda: rows)
        v15.CORE = types.SimpleNamespace(_board=lambda: [])
        board = v15._build_board(now)
        self.assertEqual(len(board), min(len(rows), v15.BOARD_LIMIT))
        self.assertEqual([r["rank"] for r in board], list(range(1, len(board) + 1)))
        self.assertTrue(all("expected_time_to_target" in r for r in board))


    def test_no_daily_buy_cap_allows_more_than_ten_qualified_signals(self):
        now = v15._now_ms()
        rows = []
        for i in range(12):
            row = self.sensor(now)
            row["symbol"] = f"U{i}USDT"
            row["entry_reference"] = 100 + i
            rows.append(row)
        v15.V13 = types.SimpleNamespace(_rows=lambda: rows)
        v15.CORE = types.SimpleNamespace(_board=lambda: [])
        v15._entry_action_original_for_test = getattr(v15, "_entry_action_original_for_test", None)
        original = v15._entry_action
        try:
            v15._entry_action = lambda sensor, structural, opp, safe, blockers, entry_plan=None: ("ML BUY NOW", True, [])
            board = v15._build_board(now)
            self.assertEqual(v15._qualified_total, 12)
            self.assertEqual(len(v15._qualified_symbols), 12)
            self.assertGreaterEqual(len(board), 10)
            self.assertTrue(all(r["execution_ready"] for r in board[:10]))
        finally:
            v15._entry_action = original


    def test_trade_scorecard_records_and_resolves_target_with_time_accuracy(self):
        now = 2_000_000_000_000
        row = {
            "symbol": "AAAUSDT", "lane": "BEAST", "action": "ML BUY NOW",
            "reference_entry": 100.0, "dynamic_stop": 97.0,
            "selected_target_pct": 3.0, "selected_target_price": 103.0,
            "selected_horizon": "4h", "expected_time_ms": 2 * 3600_000,
            "expected_time_to_target": "2 hours", "expected_time_range": "1–4 hours",
            "probability": .62, "expected_value_pct": 1.8, "model_samples": 100,
            "model_source": "BEAST", "promotion_ready": True,
        }
        self.assertEqual(v15._record_trade_signals([row], now), 1)
        v15.V13 = types.SimpleNamespace(_rows=lambda: [
            dict(self.sensor(now + 3600_000), symbol="AAAUSDT", entry_reference=104.0)
        ])
        v15.CORE = types.SimpleNamespace(q=types.SimpleNamespace(latest={}))
        self.assertEqual(v15._update_trade_scorecard(now + 3600_000), 1)
        score = v15._trade_scorecard()["buy_now"]
        self.assertEqual(score["resolved"], 1)
        self.assertEqual(score["correct_target_hits"], 1)
        self.assertEqual(score["target_hit_within_predicted_time"], 1)
        self.assertEqual(score["win_rate"], 1.0)

    def test_trade_scorecard_timeout_is_incorrect(self):
        now = 2_100_000_000_000
        row = {
            "symbol": "BBBUSDT", "lane": "HTF_SWING", "action": "ML SHADOW BUY",
            "reference_entry": 100.0, "dynamic_stop": 95.0,
            "selected_target_pct": 10.0, "selected_target_price": 110.0,
            "selected_horizon": "1h", "expected_time_ms": 30 * 60_000,
            "expected_time_to_target": "30 min", "expected_time_range": "15 min–1 hours",
            "probability": .55, "expected_value_pct": 2.0, "model_samples": 90,
            "model_source": "HTF_SWING", "promotion_ready": False,
        }
        v15._record_trade_signals([row], now)
        v15.V13 = types.SimpleNamespace(_rows=lambda: [
            dict(self.sensor(now + 3600_000), symbol="BBBUSDT", entry_reference=101.0)
        ])
        v15.CORE = types.SimpleNamespace(q=types.SimpleNamespace(latest={}))
        v15._update_trade_scorecard(now + 3600_000)
        score = v15._trade_scorecard()["shadow_buy"]
        self.assertEqual(score["resolved"], 1)
        self.assertEqual(score["incorrect"], 1)
        self.assertEqual(score["correct_target_hits"], 0)



    def test_feature_vector_contains_htf_regime_and_entry_geometry(self):
        sensor = self.sensor()
        bull = {
            "setup": "DAILY_EMA200_REJECTION", "state": "BUY",
            "setup_strength": 90, "timeframe": "1D",
            "trend_regime": "FULL_BULLISH_ALIGNMENT",
            "counter_trend": False, "risk_pct": 2.5,
            "entry_low": 99, "entry_high": 101,
            "buy_setup_count": 2, "armed_setup_count": 1,
        }
        bear = dict(bull, trend_regime="WEEKLY_BEARISH_OR_MIXED", counter_trend=True)
        xb = v15._features_from_sensor(sensor, bull)
        xr = v15._features_from_sensor(sensor, bear)
        self.assertEqual(len(xb), v15.FEATURE_COUNT)
        self.assertEqual(v15.FEATURE_COUNT, 26)
        self.assertNotEqual(xb, xr)

    def test_historical_seed_deduplicates_correlated_same_lane_window(self):
        created = 3_000_000_000_000
        base = {
            "symbol": "AAAUSDT", "setup": "breakout",
            "features": {"setup": "breakout", "buy_ratio": .6},
            "first_target_ms": {"3": created + 10 * 60_000},
            "stop_hit_ms": 0, "horizon_returns": {"15m": 3.2, "1h": 4.0},
            "entry_price": 100.0, "observed_price_at_signal": 100.0,
            "mfe_pct": 4.0, "mae_pct": -0.5,
        }
        e1 = dict(base, id="d1", created_ms=created)
        e2 = dict(base, id="d2", created_ms=created + 30 * 60_000)
        v15.OUTCOME = types.SimpleNamespace(_recent=[e1, e2])
        self.assertEqual(v15._seed_from_outcome_memory(), 1)
        self.assertEqual(len(v15._seed_buckets), 1)
        self.assertGreaterEqual(v15._stats.get("seed_dedup_skipped", 0), 1)

    def test_entry_location_actions_include_reclaim_and_pullback(self):
        opp = {
            "samples": 80, "model_source": "HTF_SWING",
            "expected_value_pct": 2.0, "probability": .62,
            "expected_loss_pct": 3.0,
            "promotion_validation": {"ready": True},
        }
        structural = {
            "setup": "DAILY_EMA200_RETEST_RECLAIM", "state": "ARMED",
            "entry_low": 100.0, "entry_high": 102.0,
            "entry": 101.0, "invalidation": 94.0, "max_chase": 110.0,
            "setup_strength": 84,
        }
        below = dict(self.sensor(), entry_reference=98.0)
        plan = v15._entry_plan(below, structural, "HTF_SWING", opp)
        action, ready, blockers = v15._entry_action(below, structural, opp, True, [], plan)
        self.assertEqual(plan["entry_location"], "BELOW_ZONE")
        self.assertEqual(action, "BUY RECLAIM")
        self.assertFalse(ready)

        above = dict(self.sensor(), entry_reference=104.0)
        plan = v15._entry_plan(above, structural, "HTF_SWING", opp)
        action, ready, blockers = v15._entry_action(above, structural, opp, True, [], plan)
        self.assertEqual(plan["entry_location"], "ABOVE_ZONE")
        self.assertEqual(action, "BUY PULLBACK")
        self.assertFalse(ready)

    def test_time_to_invalidation_is_empirical_after_eight_stops(self):
        for hours in (1, 2, 2, 3, 3, 4, 5, 6):
            v15._record_stop_time("BEAST", hours * 3600_000)
        med, lo, hi, source = v15._invalidation_time("BEAST", v15.HORIZONS_MS["12h"])
        self.assertEqual(source, "EMPIRICAL")
        self.assertIsNotNone(med)
        self.assertGreater(med, 0)
        self.assertIn("hours", v15._duration(med))


    def test_setup_specific_horizon_grid_blocks_implausible_fast_targets(self):
        self.assertFalse(v15._combo_allowed("BEAST", 20, "30m"))
        self.assertTrue(v15._combo_allowed("BEAST", 20, "4h"))
        self.assertFalse(v15._combo_allowed("HTF_SWING", 10, "12h"))
        self.assertTrue(v15._combo_allowed("HTF_SWING", 10, "24h"))
        self.assertFalse(v15._combo_allowed("EXHAUSTION", 20, "12h"))
        self.assertTrue(v15._combo_allowed("EXHAUSTION", 20, "24h"))

    def test_opportunity_never_selects_disallowed_target_horizon_pair(self):
        x = [0.1] * v15.FEATURE_COUNT
        for h in v15.HORIZON_ORDER:
            for target in v15.TARGETS:
                model = v15._model("BEAST", h, target)
                model["trained"] = 50
                model["wins"] = 25
                model["losses"] = 25
                model["bias"] = 0.0
                model["payoff_win_sum"] = 25 * float(target)
                model["payoff_win_n"] = 25
                model["payoff_loss_sum"] = 25 * 2.0
                model["payoff_loss_n"] = 25
        opp = v15._opportunity("BEAST", x)
        self.assertTrue(v15._combo_allowed("BEAST", opp["target_pct"], opp["horizon"]))
        if opp["target_pct"] == 20:
            self.assertGreaterEqual(
                v15.HORIZONS_MS[opp["horizon"]], v15.HORIZONS_MS["4h"]
            )


    def test_learned_entry_zone_uses_historical_winner_dips(self):
        for _ in range(12):
            v15._record_entry_excursion("BEAST", 10, -2.0)
        offset, samples, source = v15._learned_entry_offset("BEAST", 10)
        self.assertEqual(samples, 12)
        self.assertEqual(source, "EMPIRICAL_WINNER_MAE")
        self.assertAlmostEqual(offset, -1.0, places=6)
        plan = v15._entry_plan(
            dict(self.sensor(), entry_reference=100.0),
            {}, "BEAST", {"target_pct": 10, "expected_loss_pct": 3.0}
        )
        self.assertEqual(plan["entry_zone_source"], "EMPIRICAL_WINNER_MAE")
        self.assertLess(plan["entry_center"], 100.0)
        self.assertEqual(plan["entry_model_samples"], 12)

    def test_entry_excursion_seed_reads_mae_at_target_without_model_training(self):
        base = 4_000_000_000_000
        events = []
        for i in range(12):
            created = base + i * 7 * 60 * 60_000
            events.append({
                "id": f"entry{i}", "symbol": f"E{i}USDT", "created_ms": created,
                "setup": "micro ignition", "features": {"setup": "micro ignition"},
                "first_target_ms": {"10": created + 2 * 60 * 60_000},
                "mae_at_target": {"10": -1.6}, "stop_hit_ms": 0, "mae_pct": -4.0,
            })
        before_models = len(v15._models)
        v15.OUTCOME = types.SimpleNamespace(_recent=events)
        self.assertEqual(v15._seed_entry_excursions(), 12)
        offset, n, source = v15._learned_entry_offset("BEAST", 10)
        self.assertEqual(n, 12)
        self.assertEqual(source, "EMPIRICAL_WINNER_MAE")
        self.assertAlmostEqual(offset, -0.8, places=6)
        self.assertEqual(len(v15._models), before_models)



    def test_initial_seed_creates_chronological_per_lane_test_holdout(self):
        created = 4_000_000_000_000
        events = []
        for i in range(40):
            events.append({
                "id": f"oos{i}", "symbol": f"O{i}USDT",
                "created_ms": created + i * 60_000,
                "setup": "micro ignition",
                "features": {"setup": "micro ignition", "buy_ratio": .61},
                "first_target_ms": {"3": created + i * 60_000 + 5 * 60_000},
                "stop_hit_ms": 0,
                "horizon_returns": {"15m": 3.4, "1h": 4.0},
                "entry_price": 100.0,
                "observed_price_at_signal": 100.0,
                "mfe_pct": 4.2, "mae_pct": -0.4,
            })
        v15.OUTCOME = types.SimpleNamespace(_recent=events)
        self.assertEqual(v15._seed_from_outcome_memory(), 40)
        model = v15._model("BEAST", "15m", 3)
        self.assertGreaterEqual(model["test_n"], 9)
        self.assertGreater(model["wins"] + model["losses"], model["test_n"])
        self.assertGreaterEqual(v15._stats.get("initial_oos_test_events", 0), 9)

    def test_useful_entry_actions_rank_above_chased_or_rejected_rows(self):
        self.assertGreater(v15._action_priority("BUY RECLAIM"), v15._action_priority("DO NOT CHASE"))
        self.assertGreater(v15._action_priority("ML SHADOW BUY"), v15._action_priority("REJECT"))


if __name__ == "__main__":
    unittest.main()
