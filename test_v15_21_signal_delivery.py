import unittest
from types import SimpleNamespace
import psi_v15_21_signal_delivery as feed
import psi_v15_17_ema_lane as ema


def candle(**kw):
    row = {"current": 100, "low": 99.5, "high": 101, "close": 100.5,
           "atr": 1, "ema50": 100, "ema200": 100,
           "has_ema50": True, "has_ema200": True,
           "buy_ratio": .62, "buy_ratio_3": .58,
           "lower_wick": .35, "close_strength": .80,
           "falling_volume": True}
    row.update(kw)
    return row


def working_micro(now_ms):
    return {"micro_ready": True, "sequence_verified": True,
            "book_sequence_verified": True, "cvd_acceleration": 1.0,
            "ofi_acceleration": .12, "last_trade_ms": now_ms,
            "last_book_ms": now_ms, "last_price": 100.2,
            "spread_bps": 5.0, "slippage_bps": 10.0}


class FeedTests(unittest.TestCase):
    def setUp(self):
        self.old_core, self.old_ema = feed.CORE, feed.EMA
        feed._SNAPSHOT = {}
        feed._EVENTS.clear()
        feed._ACTIVE.clear()
        feed._NEXT_ID = 0
        feed._STATUS["cycles"] = 0
        feed._STATUS["errors"] = 0
        self.micro = working_micro(1000000)
        self.core = SimpleNamespace(
            _cache={"TESTUSDT": {"1h": {"snap": candle(), "updated": 1000}}},
            app=SimpleNamespace(micro_metrics=lambda sym: self.micro))
        feed.CORE, feed.EMA = self.core, ema

    def tearDown(self):
        feed.CORE, feed.EMA = self.old_core, self.old_ema

    def test_executable_emits_without_waiting_for_reporter(self):
        row = feed.publish_once(1000000)
        self.assertEqual(row["research_top10"], ["TESTUSDT"])
        self.assertEqual(len(row["buy_signals"]), 2)
        live = feed.read_live(1000000)
        self.assertEqual(live["status"], "BUY_NOW_VERIFIED")
        self.assertEqual(live["buy_count"], 2)
        self.assertEqual(len(feed._EVENTS), 2)
        self.assertFalse(live["order_placement"])

    def test_no_duplicates_on_repeated_fast_ticks(self):
        feed.publish_once(1000000)
        feed.publish_once(1001000)
        self.assertEqual(len(feed._EVENTS), 2)

    def test_expired_signal_never_visible(self):
        feed.publish_once(1000000)
        live = feed.read_live(1005000)
        self.assertEqual(live["status"], "DATA_STALE")
        self.assertEqual(live["buy_count"], 0)
        self.assertEqual(live["research_top10"], [])

    def test_sequence_loss_revokes_buy_before_snapshot_expiry(self):
        feed.publish_once(1000000)
        self.micro["book_sequence_verified"] = False
        live = feed.read_live(1001000)
        self.assertEqual(live["status"], "NO_VERIFIED_BUY")
        self.assertEqual(live["buy_count"], 0)

    def test_technical_research_does_not_invent_buy(self):
        self.micro["spread_bps"] = None
        row = feed.publish_once(1000000)
        self.assertEqual(len(row["buy_signals"]), 0)
        self.assertEqual(len(feed._EVENTS), 0)
        self.assertEqual(feed.read_live(1000000)["status"], "NO_VERIFIED_BUY")

    def test_seller_exhaustion_missing_creates_no_executable_event(self):
        self.core._cache["TESTUSDT"]["1h"]["snap"]["falling_volume"] = False
        row = feed.publish_once(1000000)
        self.assertEqual(len(row["buy_signals"]), 0)
        self.assertEqual(len(feed._EVENTS), 0)


if __name__ == "__main__":
    unittest.main()
