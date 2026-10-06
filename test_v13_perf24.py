"""V13 24-hour ML buy-option outcome tracking: no leakage or fabricated accuracy."""
import asyncio
import copy
import os
import tempfile
import types
import unittest
from unittest.mock import patch, AsyncMock
import psi_v13_perf24 as perf


class Performance24Tests(unittest.TestCase):
    def setUp(self):
        self.pending = copy.deepcopy(perf._pending)
        self.recent = list(perf._recent)
        self.last = dict(perf._last_by_cohort)
        self.model = copy.deepcopy(perf._model24)
        self.stats = dict(perf._stats)
        self.v13 = perf.V13
        self.core = perf.CORE
        self.paths = (perf.STATE_PATH, perf.SNAPSHOT_LOG_PATH, perf.RESULT_LOG_PATH)
        self.tmp = tempfile.TemporaryDirectory()
        perf.STATE_PATH = os.path.join(self.tmp.name, "state.json")
        perf.SNAPSHOT_LOG_PATH = os.path.join(self.tmp.name, "signalled.jsonl")
        perf.RESULT_LOG_PATH = os.path.join(self.tmp.name, "results.jsonl")
        perf._pending.clear()
        perf._recent.clear()
        perf._last_by_cohort.clear()
        perf._stats.update({"signals": 0, "priced": 0, "unpriced": 0, "resolved": 0, "unavailable": 0})
        perf._model24.update({"weights": [0.0] * 13, "bias": 0.0, "trained": 0, "test_n": 0, "test_correct": 0, "test_brier_sum": 0.0})
        self.stamp = 1_000_000_000

    def tearDown(self):
        perf._pending[:] = self.pending
        perf._recent.clear()
        perf._recent.extend(self.recent)
        perf._last_by_cohort.clear()
        perf._last_by_cohort.update(self.last)
        perf._model24.clear()
        perf._model24.update(self.model)
        perf._stats.clear()
        perf._stats.update(self.stats)
        perf.V13, perf.CORE = self.v13, self.core
        perf.STATE_PATH, perf.SNAPSHOT_LOG_PATH, perf.RESULT_LOG_PATH = self.paths
        self.tmp.cleanup()

    def candidate(self, sym="NMRUSDT", rank=1, valid=True, signal="ML BUY CANDIDATE"):
        return {"symbol": sym, "rank": rank, "signal_ms": self.stamp,
                "execution_ready": signal == "ML BUY NOW",
                "model_version": "13.0",
                "model_probability": 0.61, "rank_score": .76,
                "observed_price": 100 if valid else None,
                "evidence": {"trade_age_ms": 250, "book_age_ms": 180,
                             "hazard": 78, "buy_ratio": .8,
                             "rv10": 5, "ofi": .4, "obi": .2,
                             "spread_bps": 4}}

    def test_tracks_every_model_pick_not_only_top_five(self):
        rows = [self.candidate(f"COIN{i}USDT", rank=i + 1) for i in range(40)]
        self.assertEqual(perf.collect(rows, self.stamp), 40)
        self.assertEqual(perf.report(self.stamp)["pending_24h"], 40)
        self.assertEqual(perf.collect(rows, self.stamp + 3_000), 0)
        self.assertEqual(perf._stats["signals"], 40)
        self.assertEqual(sum(x["seen_count"] for x in perf._pending), 80)

    def test_a_missing_price_is_not_a_win_or_loss(self):
        self.assertEqual(perf.collect([self.candidate(valid=False)], self.stamp), 1)
        self.assertEqual(perf._stats["unpriced"], 1)
        self.assertEqual(perf.report(self.stamp)["pending_24h"], 0)
        fake = AsyncMock()
        asyncio.run(perf.resolve_due(fake, self.stamp + perf.HORIZON_MS + 60_000))
        self.assertEqual(perf._stats["resolved"], 0)
        self.assertEqual(perf._stats["unavailable"], 1)
        self.assertIsNone(perf.report(self.stamp + perf.HORIZON_MS)["completed_past_24h"]["win_rate_pct"])

    def test_later_verified_quote_starts_24h_clock_without_lookahead(self):
        self.assertEqual(perf.collect([self.candidate(valid=False)], self.stamp), 1)
        rec = perf._pending[0]
        self.assertFalse(rec["baseline_verified"])
        later = self.candidate(valid=True)
        later["signal_ms"] = self.stamp + 5000
        later["observed_price"] = 102.0
        self.assertEqual(perf.collect([later], self.stamp + 5000), 0)
        self.assertTrue(rec["baseline_verified"])
        self.assertEqual(rec["price"], 102.0)
        self.assertEqual(rec["target_ms"], self.stamp + 5000 + perf.HORIZON_MS)
        self.assertEqual(perf._stats["priced"], 1)
        self.assertEqual(perf._stats["unpriced"], 0)

    def test_24hour_boundary_is_enforced(self):
        perf.collect([self.candidate()], self.stamp)
        fake = AsyncMock()
        fake.return_value = {"price": 104, "close_ms": self.stamp + perf.HORIZON_MS + 30_000, "source": "BINANCE_SPOT_1M_CLOSE"}
        with patch.object(perf, "close_at_24h", fake):
            self.assertEqual(asyncio.run(perf.resolve_due(None, self.stamp + perf.HORIZON_MS - 1)), [])
            self.assertEqual(fake.await_count, 0)
            rows = asyncio.run(perf.resolve_due(None, self.stamp + perf.HORIZON_MS + 30_000))
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0]["gross_return_pct"], 4.0)
        self.assertAlmostEqual(rows[0]["estimated_net_return_pct"], 3.8)
        self.assertEqual(perf._stats["resolved"], 1)
        self.assertEqual(perf.report(self.stamp + perf.HORIZON_MS + 30_000)["completed_past_24h"]["win_rate_pct"], 100)

    def test_negative_24hour_return_counts_loss(self):
        perf.collect([self.candidate()], self.stamp)
        event = perf._pending[0]
        perf._resolve(event, {"price": 98, "close_ms": self.stamp + perf.HORIZON_MS,
                              "source": "BINANCE_SPOT_1M_CLOSE"}, self.stamp + perf.HORIZON_MS)
        self.assertFalse(perf._recent[-1]["direction_correct"])
        self.assertAlmostEqual(perf._recent[-1]["estimated_net_return_pct"], -2.2)

    def test_new_signal_class_is_separately_tracked(self):
        rows = [self.candidate(signal="ML BUY CANDIDATE"),
                self.candidate(signal="ML BUY NOW")]
        self.assertEqual(perf.collect(rows, self.stamp), 2)
        self.assertEqual({r["signal"] for r in perf._pending}, {"ML BUY NOW", "ML BUY CANDIDATE"})

    def test_state_survives_restart(self):
        perf.collect([self.candidate()], self.stamp)
        perf._save(force=True)
        perf._pending.clear()
        perf._last_by_cohort.clear()
        perf._restore()
        self.assertEqual(len(perf._pending), 1)
        self.assertEqual(perf._pending[0]["symbol"], "NMRUSDT")
        self.assertEqual(perf._stats["signals"], 1)

    def test_test_split_never_changes_training_weights(self):
        perf.collect([self.candidate()], self.stamp)
        event = perf._pending[0]
        event["split"] = "TEST"
        before = (perf._model24["trained"], list(perf._model24["weights"]), perf._model24["bias"])
        perf._resolve(event, {"price": 107, "close_ms": self.stamp + perf.HORIZON_MS, "source": "BINANCE_SPOT_1M_CLOSE"}, self.stamp + perf.HORIZON_MS)
        after = (perf._model24["trained"], list(perf._model24["weights"]), perf._model24["bias"])
        self.assertEqual(before, after)

    def test_price_from_24h_candle_not_present_price(self):
        async def fake_json():
            return [[0, "1", "1", "1", "98.75", "1000", self.stamp + perf.HORIZON_MS + 25_000]]
        response = types.SimpleNamespace(status=200, json=fake_json)
        class CM:
            async def __aenter__(self):
                return response
            async def __aexit__(self, *args):
                return False
        session = types.SimpleNamespace(get=lambda *args, **kwargs: CM())
        actual = asyncio.run(perf.close_at_24h(session, "NMRUSDT", self.stamp + perf.HORIZON_MS))
        self.assertEqual(actual["price"], 98.75)
        self.assertEqual(actual["source"], "BINANCE_SPOT_1M_CLOSE")


if __name__ == "__main__":
    unittest.main()
