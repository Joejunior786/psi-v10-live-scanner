import unittest
import json
import re
import asyncio
from unittest.mock import patch
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

    def test_dashboard_has_unique_DOM_ids_and_valid_research_table(self):
        html=feed._DASHBOARD
        ids=re.findall(r'id="([^"]+)"',html)
        self.assertGreater(len(ids),10)
        self.assertEqual(len(ids),len(set(ids)),"Duplicate HTML element IDs can break dashboard JS")
        self.assertIn('<tbody id="research">',html)
        self.assertIn('<h2 id="research-section">',html)
        self.assertIn('getElementById("research")',html)
        self.assertIn('href="#research-section"',html)

    def test_network_and_dashboard_render_errors_are_distinguished(self):
        html=feed._DASHBOARD
        self.assertIn('DASHBOARD DISPLAY ERROR: ',html)
        self.assertIn('CONNECTION UNAVAILABLE: ',html)
        self.assertIn('responseOk=true;',html)

    def test_verified_ema_signals_have_an_independent_lane(self):
        feed.publish_once(1000000)
        live=feed.read_live(1000000)
        self.assertEqual(len(live["verified_lanes"]["EMA"]),2)
        self.assertEqual(live["verified_lane_counts"]["EMA"],2)
        self.assertEqual(live["verified_lane_counts"]["ML"],0)
        self.assertEqual(live["verified_lane_counts"]["V12"],0)
        self.assertEqual(live["buy_count"],sum(live["verified_lane_counts"].values()))
        self.assertFalse(live["order_placement"])

    def test_all_authorities_keep_distinct_verified_lanes(self):
        rows=[{"symbol":"AUSDT","authority":"EMA","lane":"EMA"},
              {"symbol":"AUSDT","authority":"V15_ML","lane":"BEAST"},
              {"symbol":"BUSDT","authority":"V12_PINPOINT","lane":"BREAKOUT"}]
        groups=feed._group_verified_signals(rows)
        self.assertEqual([len(groups[k]) for k in ("EMA","ML","V12")],[1,1,1])
        self.assertEqual(groups["ML"][0]["lane"],"BEAST")
        self.assertEqual(groups["V12"][0]["symbol"],"BUSDT")
        self.assertEqual(rows[1]["authority"],"V15_ML")

    def test_revoked_or_expired_signals_leave_all_verified_lanes(self):
        feed.publish_once(1000000)
        self.micro["book_sequence_verified"]=False
        revoked=feed.read_live(1001000)
        self.assertEqual(revoked["buy_count"],0)
        self.assertTrue(all(not v for v in revoked["verified_lanes"].values()))
        expired=feed.read_live(1005000)
        self.assertEqual(expired["buy_count"],0)
        self.assertTrue(all(not v for v in expired["verified_lanes"].values()))

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


    def test_research_rows_have_unambiguous_level_and_state(self):
        row = feed.publish_once(1000000)
        self.assertEqual(len(row["research_rows"]), 1)
        research = row["research_rows"][0]
        self.assertEqual(research["symbol"], "TESTUSDT")
        self.assertEqual(research["timeframe"], "1h")
        self.assertIn(research["ema_period"], (50, 200))
        self.assertTrue(research["seller_exhaustion"])
        self.assertEqual(feed.read_live(1000000)["research_rows"], row["research_rows"])

    def test_timestamped_railway_log_bridge_only_reports_verified_buys(self):
        row = feed.publish_once(1000000)
        live = feed.read_live(1000000)
        msg = feed.signal_tick_line(row, live, 42)
        self.assertTrue(msg.startswith("PSI-V15.30 SIGNAL_TICK "))
        data = json.loads(msg.split("SIGNAL_TICK ", 1)[1])
        self.assertEqual(data["generated_ms"], 1000000)
        self.assertEqual(data["verified_at_ms"], 1000000)
        self.assertEqual(data["status_at_generation"], "BUY_NOW_VERIFIED")
        self.assertEqual(data["buy_count"], 2)
        self.assertEqual(len(data["buys"]), 2)
        self.assertEqual(data["cycle_ms"], 42)
        self.assertTrue(data["read_only"])

    def test_log_bridge_marks_stale_and_revoked_signals(self):
        snapshot = feed.publish_once(1000000)
        self.micro["book_sequence_verified"] = False
        revoked = feed.read_live(1001000)
        data = json.loads(feed.signal_tick_line(snapshot, revoked).split("SIGNAL_TICK ", 1)[1])
        self.assertEqual(data["status_at_generation"], "NO_VERIFIED_BUY")
        self.assertEqual(data["buys"], [])
        expired = feed.read_live(1005000)
        stale = json.loads(feed.signal_tick_line(snapshot, expired).split("SIGNAL_TICK ", 1)[1])
        self.assertEqual(stale["status_at_generation"], "DATA_STALE")
        self.assertEqual(stale["research"], [])
        self.assertEqual(stale["buys"], [])


    def test_on_demand_refreshes_expired_snapshot_without_extending_old_signal(self):
        feed.publish_once(1000000)
        live_stale = feed.read_live(1005000)
        self.assertFalse(live_stale["fresh"])
        self.micro.update(working_micro(1005000))
        self.core._cache["TESTUSDT"]["1h"]["updated"] = 1005
        with patch.object(feed, "_ms", return_value=1005000):
            response = asyncio.run(feed.http_live(None))
        live = json.loads(response.text)
        self.assertTrue(live["fresh"])
        self.assertEqual(live["generated_ms"], 1005000)
        self.assertEqual(live["buy_count"], 2)

    def test_quote_requires_subsecond_live_book_and_trade_not_just_fresh_report(self):
        feed.publish_once(1000000)
        self.micro["last_trade_ms"] = 997000
        self.micro["last_book_ms"] = 999000
        self.assertEqual(feed.read_live(1000000)["buy_count"], 2)
        with patch.object(feed, "_ms", return_value=1000000):
            response = asyncio.run(feed.http_quote(
                SimpleNamespace(query={"symbol": "TESTUSDT"})))
        quote = json.loads(response.text)
        self.assertEqual(quote["buy_count"], 0)
        self.assertEqual(quote["status"], "NO_VERIFIED_BUY")
        self.assertFalse(quote["order_placement"])

    def test_quote_freshness_and_no_exchange_order(self):
        with patch.object(feed, "_ms", return_value=1000000):
            response = asyncio.run(feed.http_quote(
                SimpleNamespace(query={"symbol": "TESTUSDT"})))
        data = json.loads(response.text)
        self.assertEqual(data["status"], "VERIFIED_AT_READ")
        self.assertEqual(data["buy_count"], 2)
        self.assertLessEqual(data["quotes"][0]["quote_expires_ms"], 1001200)
        self.assertFalse(data["order_placement"])


    def test_buy_structure_formal_only_without_executable_promotion(self):
        self.micro["spread_bps"]=None
        self.core._board=lambda: [
            dict(symbol="TESTUSDT",state="BUY",setup="SUPPORT_RECLAIM",
                timeframe="4H",generated_ms=1000000,setup_strength=92,
                current=100,entry_low=99,entry_high=101,invalidation=95,
                tp1=114,execution_state="COLLECTING DATA",buy_now=False,
                execution_blockers=["STALE_BOOK"]),
            dict(symbol="SHADOWUSDT",state="BUY",setup="SENSOR_PROBE",
                setup_source="SENSOR_SHADOW",generated_ms=1000000),
            dict(symbol="STALEUSDT",state="BUY",setup="BREAKOUT",
                generated_ms=950000)]
        feed.publish_once(1000000)
        live=feed.read_live(1000000)
        self.assertEqual(live["buy_structure_summary"]["structural_buy"],1)
        self.assertEqual(len(live["buy_structure_rows"]),1)
        row=live["buy_structure_rows"][0]
        self.assertEqual(row["symbol"],"TESTUSDT")
        self.assertEqual(row["status"],"STRUCTURE BUY")
        self.assertEqual(row["potential_pct"],round(100*(114/101-1),2))
        self.assertFalse(row["verified_buy_now"])
        self.assertIn("STALE_BOOK",row["blockers"])
        self.assertEqual(live["buy_count"],0)
        self.assertFalse(feed._EVENTS)

    def test_buy_structure_preserves_other_verified_lanes(self):
        self.core._board=lambda: [dict(
            symbol="TESTUSDT",state="ARMED",setup="EMA_REJECTION",
            timeframe="1H",generated_ms=1000000,current=100,
            entry_low=99,entry_high=101,invalidation=95,tp1=115,
            execution_state="COLLECTING DATA",buy_now=False)]
        feed.publish_once(1000000)
        live=feed.read_live(1000000)
        self.assertEqual(live["buy_structure_rows"][0]["status"],"STRUCTURE ARMED")
        self.assertEqual(live["verified_lane_counts"]["V12"],0)
        self.assertEqual(live["verified_lane_counts"]["ML"],0)
        self.assertEqual(live["verified_lane_counts"]["EMA"],2)

    def test_buy_structure_expires_independently_of_snapshot(self):
        self.core._board=lambda: [dict(
            symbol="TESTUSDT",state="BUY",setup="RETEST",
            generated_ms=980000,entry_low=99,entry_high=101,
            current=100,invalidation=95,tp1=115)]
        feed.publish_once(1000000)
        self.assertEqual(len(feed.read_live(1000000)["buy_structure_rows"]),1)
        self.assertEqual(feed.read_live(1003300)["buy_structure_rows"],[])
        self.assertEqual(feed.read_live(1005000)["buy_structure_rows"],[])

    def test_dashboard_has_separate_buy_structure_board(self):
        html=feed._DASHBOARD
        self.assertIn('id="buy-structure"',html)
        self.assertIn('<tbody id="structureRows">',html)
        self.assertIn('href="#buy-structure"',html)
        self.assertIn("d.buy_structure_rows||[]",html)

    def test_board_contains_both_new_live_routes_and_no_order_submission(self):
        response = asyncio.run(feed.http_dashboard(None))
        self.assertIn("/signals/live", response.text)
        self.assertIn("/signals/quote", response.text)
        self.assertIn("No order has been placed", response.text)

if __name__ == "__main__":
    unittest.main()
