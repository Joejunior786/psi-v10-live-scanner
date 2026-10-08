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
        self.lane_stats = dict(v15._lane_stats)
        self.board = list(v15._board)
        self.daily_buy_day = v15._daily_buy_day
        self.daily_buy_symbols = set(v15._daily_buy_symbols)
        self.core, self.v13, self.outcome, self.v14 = v15.CORE, v15.V13, v15.OUTCOME, v15.V14
        v15._models.clear()
        v15._pending.clear()
        v15._recent.clear()
        v15._last_signal.clear()
        v15._seen_seed.clear()
        v15._lane_stats.clear()
        v15._board.clear()
        v15._daily_buy_day = ""
        v15._daily_buy_symbols.clear()

    def tearDown(self):
        v15._models.clear(); v15._models.update(self.models)
        v15._pending[:] = self.pending
        v15._recent.clear(); v15._recent.extend(self.recent)
        v15._last_signal.clear(); v15._last_signal.update(self.last_signal)
        v15._seen_seed.clear(); v15._seen_seed.update(self.seen_seed)
        v15._lane_stats.clear(); v15._lane_stats.update(self.lane_stats)
        v15._board[:] = self.board
        v15._daily_buy_day = self.daily_buy_day
        v15._daily_buy_symbols.clear(); v15._daily_buy_symbols.update(self.daily_buy_symbols)
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

    def test_positive_ev_below_fifty_percent_can_buy(self):
        x = v15._features_from_sensor(self.sensor(), {})
        p = 0.45
        bias = math.log(p / (1 - p))
        for h in v15.HORIZON_ORDER:
            for t in v15.TARGETS:
                m = v15._model("BEAST", h, t)
                m["bias"] = bias
                m["weights"] = [0.0] * v15.FEATURE_COUNT
                m["trained"] = 40
                m["wins"] = 18
                m["losses"] = 22
                m["payoff_win_sum"] = 18 * 15.0
                m["payoff_win_n"] = 18
                m["payoff_loss_sum"] = 22 * 3.0
                m["payoff_loss_n"] = 22
        opp = v15._opportunity("BEAST", x)
        self.assertLess(opp["probability"], 0.5)
        self.assertGreater(opp["expected_value_pct"], 0)
        action, ready, blockers = v15._entry_action(
            self.sensor(), {}, opp, True, []
        )
        self.assertEqual(action, "ML BUY NOW")
        self.assertTrue(ready)

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
        self.assertEqual(len(board), 5)
        self.assertEqual([r["rank"] for r in board], [1, 2, 3, 4, 5])
        self.assertTrue(all("expected_time_to_target" in r for r in board))


    def test_daily_buy_cap_keeps_only_three_new_signals(self):
        at = 1_800_000_000_000
        rows = [
            {"symbol": f"B{i}USDT", "execution_ready": True, "action": "ML BUY NOW", "blockers": []}
            for i in range(5)
        ]
        v15._apply_daily_buy_cap(rows, at)
        self.assertEqual(sum(bool(r["execution_ready"]) for r in rows), 3)
        self.assertEqual(len(v15._daily_buy_symbols), 3)
        self.assertTrue(all(r["action"] == "WAIT DAILY LIMIT" for r in rows[3:]))
        self.assertTrue(all("MAX_3_ML_TRADES_PER_DAY" in r["blockers"] for r in rows[3:]))


if __name__ == "__main__":
    unittest.main()
