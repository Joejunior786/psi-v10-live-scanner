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
        self.learner = ml.LEARNER
        self.stats = dict(ml._stats)
        self.held = {k: dict(v) for k, v in ml._held_back_state.items()}
        self.coverage_history = {k: dict(v) for k, v in ml._coverage_history.items()}
        self.coverage_last = [dict(v) for v in ml._coverage_last]
        self.coverage_ms = ml._coverage_last_ms
        self.coverage_cycle = ml._coverage_cycle
        self.held_progress = {k: dict(v) for k, v in ml._held_progress.items()}
        self.held_last = list(ml._held_last_symbols)
        self.held_rotation_seen = dict(ml._held_rotation_seen)
        self.held_rotation_cycle = ml._held_rotation_cycle
        self.held_snapshot = ml._held_last_snapshot
        ml._held_rotation_seen.clear()
        ml._held_rotation_cycle = 0
        ml._held_last_snapshot = None
        ml._coverage_history.clear()
        ml._coverage_last.clear()
        ml._coverage_last_ms = 0
        ml._coverage_cycle = 0
        ml._held_progress.clear()
        ml._held_last_symbols.clear()

    def tearDown(self):
        ml._model.clear()
        ml._model.update(self.model)
        ml.CORE, ml.SENSOR = self.core, self.sensor
        ml._ranked[:] = self.ranked
        ml._pending[:] = self.pending
        ml.LEARNER = self.learner
        ml._stats.clear()
        ml._stats.update(self.stats)
        ml._held_back_state.clear()
        ml._held_back_state.update(self.held)
        ml._coverage_history.clear()
        ml._coverage_history.update(self.coverage_history)
        ml._coverage_last = self.coverage_last
        ml._coverage_last_ms = self.coverage_ms
        ml._coverage_cycle = self.coverage_cycle
        ml._held_progress.clear()
        ml._held_progress.update(self.held_progress)
        ml._held_last_symbols = self.held_last
        ml._held_rotation_seen.clear()
        ml._held_rotation_seen.update(self.held_rotation_seen)
        ml._held_rotation_cycle = self.held_rotation_cycle
        ml._held_last_snapshot = self.held_snapshot

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

    def test_thirty_distinct_coins_in_5_10_15_layout(self):
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
        self.assertEqual(dict(Counter(r["category"] for r in result)),
                         {"ml": 5, "strongest": 10, "fresh_challenger": 15})


    def test_historical_warm_start_uses_resolved_pre_signal_data(self):
        old = ml._model["trained"]
        ml._model["trained"] = 0
        ml._stats.pop("historical_seed_completed", None)
        created = 86_400_000 * 20
        event = {
            "resolved": True, "created_ms": created,
            "entry_price": 100.0, "stop_price": 97.5,
            "first_target_ms": {"3": created + 120000},
            "stop_hit_ms": 0,
            "features": {"buy_ratio": .75, "ofi": .35, "obi": .2,
                         "spread_bps": 7, "early_hazard_score": 80,
                         "relative_volume_30s": 3.1},
        }
        ml.LEARNER = types.SimpleNamespace(_bootstrapped=True, _recent=[event])
        with patch.object(ml, "_save"):
            ml.seed_historical()
        self.assertEqual(ml._stats["historical_seeded"], 1)
        self.assertEqual(ml._model["trained"], 1)
        self.assertEqual(old >= 0, True)

    def test_missing_sensor_data_cannot_make_entry(self):
        ml._model["trained"] = 200
        ml._model["bias"] = 5
        r = self.sample("NMRUSDT")
        r["generated_ms"] = 1
        candidate = ml._present(r, 1, 1_000_000)
        self.assertFalse(candidate["execution_ready"])
        self.assertIn("LIVE_SENSOR_OR_EXECUTION_SAFETY", candidate["reason"])


    def test_audit_provides_all_30_rows_without_fake_entries(self):
        import json
        now = ml.ts()
        universe = [f"COIN{i}USDT" for i in range(45)]
        data = [self.sample(s, now=now) for s in universe]
        board = [{"symbol": s, "state": "WATCH", "entry_low": 10,
                  "invalidation": 9.5, "tp1": 10.3, "tp2": 10.5,
                  "tp3": 11, "execution_state": "COLLECTING DATA"}
                 for s in universe]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={"_all_candidates": [
                {"symbol": s, "state": "MONSTER-WATCH", "layers": 3}
                for s in universe]}),
            _board=lambda: board,
        )
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=data)
        ml._model["trained"] = 0
        ml.rank()
        selection = ml.thirty()
        audit = ml.coverage30_audit(selection, ml._ranked[0]["signal_ms"])
        self.assertEqual(audit["total"], 30)
        self.assertEqual(audit["unique"], 30)
        self.assertEqual(audit["execution_ready"], 0)
        self.assertEqual(len({r["category"] for r in audit["rows"]}), 3)
        self.assertEqual(audit["rotation_layout"],
                         {"ml": 5, "strongest": 10, "fresh_challenger": 15})
        self.assertTrue(all(r["entry"] is None and r["stop"] is None
                            and r["tp1"] is None for r in audit["rows"]))
        self.assertTrue(all("research_only_levels" in r and "blockers" in r
                            for r in audit["rows"]))
        with patch("builtins.print") as printer:
            ml.log_coverage30_audit(selection, ml._ranked[0]["signal_ms"])
        output = printer.call_args.args[0]
        self.assertTrue(output.startswith("PSI-V13 COVERAGE30_JSON "))
        decoded = json.loads(output[len("PSI-V13 COVERAGE30_JSON "):])
        self.assertEqual(len(decoded["rows"]), 30)

    def test_ml_audit_revalidates_freshness_before_logging(self):
        now = ml.ts()
        universe = [f"COIN{i}USDT" for i in range(45)]
        data = [self.sample(s, now=now) for s in universe]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={"_all_candidates": []}),
            _board=lambda: [],
        )
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=data)
        ml._model["trained"] = 200
        ml._model["bias"] = 5.0
        ml.rank()
        selection = ml.thirty()
        stamp = ml._ranked[0]["signal_ms"]
        current = ml.coverage30_audit(selection, stamp)
        current_ml = [r for r in current["rows"] if r["category"] == "ml"]
        self.assertEqual(len(current_ml), 5)
        self.assertTrue(all(r["entry_verified"] for r in current_ml))
        self.assertTrue(all(r["tp1"] > r["entry"] > r["stop"] for r in current_ml))
        stale = ml.coverage30_audit(selection, stamp + 16_000)
        stale_ml = [r for r in stale["rows"] if r["category"] == "ml"]
        self.assertTrue(all(not r["entry_verified"] for r in stale_ml))
        self.assertTrue(all(r["entry"] is None for r in stale_ml))



    def test_held_back_lane_is_read_only_and_rejects_invalidated_stop(self):
        now = 1_000_000
        good = self.sample("GOODUSDT", now=now)
        bad = self.sample("BADUSDT", now=now)
        bad["entry_reference"] = 9.0
        board = [
            {"symbol": "GOODUSDT", "state": "BUY", "setup_strength": 98,
             "execution_state": "COLLECTING DATA",
             "execution_blockers": ["LIVE_MICRO_DATA", "PINPOINT_TRIGGERED"],
             "entry_low": 10.0, "invalidation": 9.5,
             "tp1": 11.0, "tp2": 12.0, "tp3": 13.0},
            {"symbol": "BADUSDT", "state": "BUY", "setup_strength": 97,
             "execution_state": "COLLECTING DATA",
             "execution_blockers": ["LIVE_MICRO_DATA"],
             "entry_low": 10.0, "invalidation": 9.5,
             "tp1": 11.0, "tp2": 12.0, "tp3": 13.0},
            {"symbol": "APPROVEDUSDT", "state": "BUY", "setup_strength": 99,
             "execution_state": "BUY NOW", "buy_now": True,
             "entry_low": 10, "invalidation": 9.5,
             "tp1": 11, "tp2": 12, "tp3": 13},
        ]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=["GOODUSDT", "BADUSDT", "APPROVEDUSDT"]),
            _board=lambda: board,
        )
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=[good, bad])
        result = ml.held_back_lane(now)
        self.assertEqual(result["total_held"], 1)
        self.assertEqual(result["total_rejected"], 1)
        self.assertEqual(result["rows"][0]["symbol"], "GOODUSDT")
        self.assertEqual(result["rows"][0]["label"], "HELD - DATA")
        self.assertEqual(result["rows"][0]["reward_risk"], 2.0)
        self.assertFalse(result["rows"][0]["entry_verified"])
        self.assertEqual(result["rejected"][0]["rejection_reason"],
                         "REFERENCE_STOP_INVALIDATED")
        self.assertEqual(result["execution_ready"], 0)
        self.assertEqual(board[0]["execution_state"], "COLLECTING DATA")

    def test_held_back_outcomes_are_only_observed_touches(self):
        now = 1_000_000
        sample = self.sample("GOODUSDT", now=now)
        board = [{"symbol": "GOODUSDT", "state": "BUY",
                  "execution_state": "COLLECTING DATA",
                  "entry_low": 10.0, "invalidation": 9.5,
                  "tp1": 11.0, "tp2": 12.0, "tp3": 13.0,
                  "execution_blockers": ["LIVE_MICRO_DATA"]}]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=["GOODUSDT"]),
            _board=lambda: board,
        )
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=[sample])
        ml.held_back_lane(now, record=True)
        self.assertEqual(ml._held_back_state["GOODUSDT"]["outcome"], "PENDING")
        sample["generated_ms"] = now + 1000
        sample["entry_reference"] = 11.1
        ml.held_back_lane(now + 1000, record=True)
        self.assertEqual(ml._held_back_state["GOODUSDT"]["outcome"],
                         "OBSERVED_TP1_TOUCH")
        self.assertEqual(board[0]["execution_state"], "COLLECTING DATA")



    def test_challengers_rotate_without_repeating_previous_scan(self):
        now = 1_000_000
        universe = [f"COIN{i:03}USDT" for i in range(95)]
        samples = [self.sample(sym, now=now) for sym in universe]
        board = [{"symbol": sym, "state": "BUY",
                  "setup_strength": 95 if i < 18 else 40,
                  "execution_state": "COLLECTING DATA"}
                 for i, sym in enumerate(universe)]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={"_all_candidates": []}),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        ml.rank()
        first = ml.thirty(now, record=True)
        self.assertEqual(len(first), 30)
        self.assertEqual(len({x["symbol"] for x in first}), 30)
        self.assertEqual(sum(x["category"] == "fresh_challenger" for x in first), 15)
        first_challengers = {x["symbol"] for x in first
                             if x["category"] == "fresh_challenger"}
        read = ml.thirty(now + 5000, record=False)
        self.assertEqual(first, read)
        self.assertEqual(ml._coverage_cycle, 1)
        for sample in samples:
            sample["generated_ms"] = now + 40_000
        second = ml.thirty(now + 40_000, record=True)
        second_challengers = {x["symbol"] for x in second
                              if x["category"] == "fresh_challenger"}
        self.assertEqual(len(second), 30)
        self.assertFalse(first_challengers & second_challengers)
        self.assertEqual(ml._coverage_cycle, 2)
        self.assertTrue(any(x["rotation_progress"] == "UNCHANGED" for x in second))
        self.assertTrue(all(not x["entry_verified"] for x in second))

    def test_missing_price_is_labeled_not_promoted_by_rotation(self):
        now = 1_000_000
        universe = [f"S{i}USDT" for i in range(50)]
        samples = [self.sample(s, now=now) for s in universe[:5]]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={"_all_candidates": []}),
            _board=lambda: [])
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        ml.rank()
        selections = ml.thirty(now, record=True)
        audit = ml.coverage30_audit(selections, now)
        self.assertEqual(len(audit["rows"]), 30)
        self.assertTrue(any(not r["observed_price"] for r in audit["rows"]))
        self.assertEqual(audit["execution_ready"], 0)
        self.assertTrue(all(r["entry"] is None for r in audit["rows"]))

    def test_held_display_prioritizes_observed_and_marks_continuing(self):
        now = 1_000_000
        universe = [f"HB{i}USDT" for i in range(7)]
        board = [{"symbol": sym, "state": "BUY", "setup_strength": 95-i,
                  "execution_state": "COLLECTING DATA",
                  "execution_blockers": ["LIVE_MICRO_DATA"],
                  "entry_low": 10, "invalidation": 9.5,
                  "tp1": 11, "tp2": 12, "tp3": 13}
                 for i, sym in enumerate(universe)]
        samples = [self.sample(sym, now=now) for sym in universe[:4]]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        first = ml.held_back_lane(now, record=True)
        self.assertEqual(first["rows"][0]["symbol"], "HB0USDT")
        self.assertEqual(first["rows"][0]["display_status"], "NEW TO TOP 5")
        self.assertTrue(all(x["observed_price"] is not None
                            for x in first["rows"][:4]))
        for sample in samples:
            sample["generated_ms"] = now + 40_000
        second = ml.held_back_lane(now + 40_000, record=True)
        displayed = {x["symbol"]: x for x in second["rows"]}
        self.assertEqual(displayed["HB0USDT"]["display_status"], "CONTINUING")
        self.assertEqual(displayed["HB0USDT"]["evidence_progress"], "UNCHANGED")
        self.assertEqual(second["execution_ready"], 0)
        self.assertEqual(board[0]["execution_state"], "COLLECTING DATA")

    def test_rotation_progress_detects_lost_fresh_confirmation(self):
        now = 1_000_000
        universe = [f"T{i}USDT" for i in range(45)]
        samples = [self.sample(sym, now=now) for sym in universe]
        board = [{"symbol": sym, "state": "BUY",
                  "setup_strength": 100-i*.1,
                  "execution_state": "COLLECTING DATA"}
                 for i, sym in enumerate(universe[:15])]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=universe),
            base=types.SimpleNamespace(latest={"_all_candidates": []}),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        ml.rank()
        first = ml.thirty(now, record=True)
        strong = next(x["symbol"] for x in first if x["category"] == "strongest")
        for sample in samples:
            sample["generated_ms"] = now + 40_000
            if sample["symbol"] == strong:
                sample["hard_sensor_safety"] = False
        second = ml.thirty(now + 40_000, record=True)
        report = next(x for x in second if x["symbol"] == strong)
        self.assertEqual(report["rotation_progress"], "DETERIORATING")



    def test_held_back_rotates_all_five_when_ten_priced_candidates_exist(self):
        now = 2_000_000
        names = [f"HB{i:02d}USDT" for i in range(12)]
        board = [{"symbol": sym, "state": "BUY", "setup_strength": 100-i,
                  "execution_state": "COLLECTING DATA",
                  "execution_blockers": ["LIVE_MICRO_DATA"],
                  "entry_low": 10, "invalidation": 9.5,
                  "tp1": 11, "tp2": 12, "tp3": 13}
                 for i, sym in enumerate(names)]
        samples = [self.sample(sym, now=now) for sym in names]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=names),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)

        first = ml.held_back_lane(now, record=True)
        self.assertEqual(len(first["rows"]), 5)
        self.assertEqual(first["eligible_priced_pool"], 12)
        self.assertEqual(first["rotated_from_previous"], 5)
        old = {r["symbol"] for r in first["rows"]}
        self.assertTrue(all(r["rotation_status"] == "ROTATED_IN"
                            for r in first["rows"]))
        for sample in samples:
            sample["generated_ms"] = now + 20_000
        second = ml.held_back_lane(now + 20_000, record=True)
        fresh = {r["symbol"] for r in second["rows"]}
        self.assertEqual(len(second["rows"]), 5)
        self.assertFalse(old & fresh)
        self.assertEqual(second["rotated_from_previous"], 5)
        self.assertEqual(second["continuing_limited_pool"], 0)
        self.assertEqual(second["rotation_cycle"], 2)
        self.assertEqual(second["execution_ready"], 0)
        self.assertTrue(all(not r["entry_verified"] for r in second["rows"]))
        self.assertTrue(all(r["label"] == "HELD - DATA" for r in second["rows"]))

    def test_held_back_read_only_endpoint_does_not_consume_rotation(self):
        now = 2_000_000
        names = [f"HB{i:02d}USDT" for i in range(10)]
        board = [{"symbol": sym, "state": "ARMED", "setup_strength": 100-i,
                  "execution_state": "COLLECTING DATA",
                  "execution_blockers": ["LIVE_MICRO_DATA"],
                  "entry_low": 10, "invalidation": 9.5,
                  "tp1": 11, "tp2": 12, "tp3": 13}
                 for i, sym in enumerate(names)]
        samples = [self.sample(sym, now=now) for sym in names]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=names),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        recorded = ml.held_back_lane(now, record=True)
        self.assertEqual(ml._held_rotation_cycle, 1)
        repeated_read = ml.held_back_lane(now + 1000, record=False)
        self.assertEqual(recorded["rows"], repeated_read["rows"])
        self.assertEqual(ml._held_rotation_cycle, 1)
        for sample in samples:
            sample["generated_ms"] = now + 20_000
        next_scan = ml.held_back_lane(now + 20_000, record=True)
        self.assertEqual(ml._held_rotation_cycle, 2)
        self.assertFalse({r["symbol"] for r in recorded["rows"]} &
                         {r["symbol"] for r in next_scan["rows"]})

    def test_held_back_repeats_only_when_priced_alternatives_insufficient(self):
        now = 2_000_000
        names = [f"HB{i:02d}USDT" for i in range(9)]
        board = [{"symbol": sym, "state": "BUY", "setup_strength": 100-i,
                  "execution_state": "COLLECTING DATA",
                  "execution_blockers": ["LIVE_MICRO_DATA"],
                  "entry_low": 10, "invalidation": 9.5,
                  "tp1": 11, "tp2": 12, "tp3": 13}
                 for i, sym in enumerate(names)]
        samples = [self.sample(sym, now=now) for sym in names[:4]]
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=names),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        first = ml.held_back_lane(now, record=True)
        for sample in samples:
            sample["generated_ms"] = now + 20_000
        second = ml.held_back_lane(now + 20_000, record=True)
        self.assertEqual(second["eligible_priced_pool"], 4)
        self.assertEqual(len(second["rows"]), 5)
        self.assertEqual(second["continuing_limited_pool"], 4)
        self.assertTrue(all(r["rotation_status"] == "CONTINUING_LIMITED_POOL"
                            for r in second["rows"][:4]))
        self.assertIsNone(second["rows"][-1]["observed_price"])
        self.assertEqual(second["execution_ready"], 0)

    def test_held_back_filters_pegged_and_invalidated_stop_during_rotation(self):
        now = 2_000_000
        names = [f"HB{i:02d}USDT" for i in range(8)]
        board = [{"symbol": sym, "state": "BUY", "setup_strength": 100-i,
                  "execution_state": "COLLECTING DATA",
                  "entry_low": 10, "invalidation": 9.5,
                  "tp1": 11, "tp2": 12, "tp3": 13}
                 for i, sym in enumerate(names)]
        board.extend([
            {"symbol": "BFUSDUSDT", "state": "BUY", "entry_low": 1,
             "invalidation": .99, "tp1": 1.01, "tp2": 1.02, "tp3": 1.03},
            {"symbol": "BADUSDT", "state": "BUY", "entry_low": 10,
             "invalidation": 10.5, "tp1": 11, "tp2": 12, "tp3": 13},
        ])
        samples = [self.sample(sym, now=now) for sym in names]
        peg = self.sample("BFUSDUSDT", now=now)
        peg["entry_reference"] = 1.0
        samples.append(peg)
        ml.CORE = types.SimpleNamespace(
            q=types.SimpleNamespace(universe=names + ["BFUSDUSDT", "BADUSDT"]),
            _board=lambda: board)
        ml.SENSOR = types.SimpleNamespace(_latest_candidates=samples)
        first = ml.held_back_lane(now, record=True)
        self.assertNotIn("BFUSDUSDT", [r["symbol"] for r in first["rows"]])
        self.assertNotIn("BADUSDT", [r["symbol"] for r in first["rows"]])
        self.assertEqual(first["execution_ready"], 0)


    def test_http_snapshot_cache_reuses_discovery_worker_reporting(self):
        now = 2_000_000
        ranked = [{"symbol": "AAAUSDT", "execution_ready": False}]
        report = [{"symbol": "AAAUSDT", "category": "ml"}]
        audit = {"rows": [{"symbol": "AAAUSDT"}], "total": 1, "unique": 1}
        held = {"rows": [], "execution_ready": 0}
        ml._store_http_snapshot(ranked, report, audit, held, now)
        snap = ml._get_http_snapshot(now + 1000)
        self.assertIsNotNone(snap)
        self.assertEqual(snap["ranked"][0]["symbol"], "AAAUSDT")
        self.assertEqual(snap["audit"]["total"], 1)
        self.assertEqual(snap["held"]["execution_ready"], 0)
        self.assertIsNone(
            ml._get_http_snapshot(now + ml.HTTP_SNAPSHOT_MAX_AGE_MS + 1)
        )


if __name__ == "__main__":
    unittest.main()
