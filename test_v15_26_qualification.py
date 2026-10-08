"""V15.26: actual event-time evidence, stable subscriptions, measured missed moves."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import psi_v15_multihorizon as ml
import psi_v15_21_signal_delivery as feed
import psi_v15_17_ema_lane as ema

NOW=1000000
def micro(price=100,last_trade=NOW,last_book=NOW,ready=True):
    return dict(last_price=price,last_trade_ms=last_trade,last_book_ms=last_book,
                micro_ready=ready,sequence_verified=True,
                book_sequence_verified=True,spread_bps=5,slippage_bps=10,
                cvd_acceleration=0.15,ofi_acceleration=0.05,
                aggressive_buy_ratio=.6)
def sensor():
    return dict(symbol="TESTUSDT",entry_reference=100,generated_ms=NOW-20000,
                trade_age_ms=15000,book_age_ms=5000,hard_sensor_safety=False,
                sequence_verified=False,book_sequence_verified=False)

class DecisionEvidence(unittest.TestCase):
    def setUp(self):
        self.old=ml.CORE
        self.value=micro()
        ml.CORE=SimpleNamespace(app=SimpleNamespace(
            micro_metrics=lambda sym:self.value))
        ml._LIVE_DECISION_DIAG.clear()
    def tearDown(self):
        ml.CORE=self.old
    def test_true_live_events_revalidate_cached_candidate(self):
        src=sensor()
        out=ml._synchronise_priority_sensor(src,NOW)
        self.assertEqual(out["live_evidence_source"],"DISTRIBUTED_TRADE_BOOK")
        self.assertTrue(out["hard_sensor_safety"])
        self.assertEqual(out["generated_ms"],NOW)
        self.assertEqual(out["entry_reference"],100)
        self.assertEqual(src["generated_ms"],NOW-20000)
        self.assertTrue(ml._safety(out,NOW)[0])
    def test_stale_trade_never_freshened_by_clock(self):
        self.value=micro(last_trade=NOW-1201)
        out=ml._synchronise_priority_sensor(sensor(),NOW)
        self.assertNotIn("live_evidence_source",out)
        self.assertEqual(out["generated_ms"],NOW-20000)
        self.assertFalse(ml._safety(out,NOW)[0])
    def test_stale_book_and_broken_sequence_fail(self):
        self.value=micro(last_book=NOW-1300)
        self.assertFalse(ml._synchronise_priority_sensor(sensor(),NOW)["hard_sensor_safety"])
        self.value=micro()
        self.value["sequence_verified"]=False
        self.assertFalse(ml._synchronise_priority_sensor(sensor(),NOW)["hard_sensor_safety"])
    def test_unconfirmed_price_gap_fails_closed(self):
        self.value=micro(price=105)
        out=ml._synchronise_priority_sensor(sensor(),NOW)
        self.assertEqual(out["entry_reference"],100)
        self.assertFalse(out["hard_sensor_safety"])
    def test_qualification_is_label_not_execution(self):
        self.assertEqual(ml._qualification_state(dict(action="WAIT",
             blockers=["STALE_TRADE"])),"DATA BLOCKED")
        self.assertEqual(ml._qualification_state(dict(action="REJECT",
             blockers=["NEGATIVE_EXPECTED_VALUE"])),"MODEL REJECTED")
        self.assertEqual(ml._qualification_state(dict(
             execution_ready=False,action="WAIT",blockers=[],
             setup_verification="UPSTREAM_STRUCTURAL",
             selected_target_pct=20,expected_value_pct=1.2)),"NEAR BUY")
        self.assertEqual(ml._qualification_state(dict(
             execution_ready=True,action="ML BUY NOW",
             blockers=[])),"MODEL_APPROVED_PENDING_QUOTE")

class ObservationCohort(unittest.TestCase):
    def setUp(self):
        self.old=(feed.CORE,feed.EMA,feed.ML)
        feed.EMA=ema
        feed._PRIORITY_LEASES.clear()
        feed._OBSERVED_QUOTES.clear()
        feed._MISSED_MOVES.clear()
        feed.ML=None
    def tearDown(self):
        feed.CORE,feed.EMA,feed.ML=self.old
        feed._PRIORITY_LEASES.clear()
        feed._OBSERVED_QUOTES.clear()
        feed._MISSED_MOVES.clear()
    def test_held_pool_retains_priority_for_two_minutes(self):
        a=feed._stable_market_priorities(["TESTUSDT","OTHERUSDT"],NOW)
        self.assertEqual(a,["TESTUSDT","OTHERUSDT"])
        b=feed._stable_market_priorities(["NEWUSDT"],NOW+30000)
        self.assertEqual(b[:2],a)
        self.assertIn("NEWUSDT",b)
        c=feed._stable_market_priorities(["FRESHUSDT"],NOW+120001)
        self.assertEqual(c,["FRESHUSDT"])
    def test_missed_move_only_from_observed_fresh_quotes(self):
        measured={"last_price":100.0}
        def check(sym,at):
            return dict(measured),[]
        with patch.object(feed,"_micro_authority_check",side_effect=check):
            feed._observe_missed_rallies(NOW,["TESTUSDT"],set())
            measured["last_price"]=110.2
            feed._observe_missed_rallies(NOW+61000,["TESTUSDT"],set())
        self.assertEqual(len(feed._MISSED_MOVES),1)
        event=feed._MISSED_MOVES[0]
        self.assertFalse(event["had_approved_buy"])
        self.assertEqual(event["scope"],"OBSERVED_QUOTES_ONLY")
        self.assertGreaterEqual(event["observed_gain_pct"],10)
    def test_no_move_claim_without_actual_verified_evidence(self):
        def fake(sym,at):
            return None,["STALE_TRADE"]
        with patch.object(feed,"_micro_authority_check",side_effect=fake):
            feed._observe_missed_rallies(NOW,["TESTUSDT"],set())
            feed._observe_missed_rallies(NOW+61000,["TESTUSDT"],set())
        self.assertEqual(len(feed._MISSED_MOVES),0)
    def test_approved_rally_is_not_misclassified_as_missed(self):
        measured={"last_price":100.0}
        with patch.object(feed,"_micro_authority_check",
                          side_effect=lambda sym,at:(dict(measured),[])):
            feed._observe_missed_rallies(NOW,["TESTUSDT"],{"TESTUSDT"})
            measured["last_price"]=115.0
            feed._observe_missed_rallies(NOW+61000,["TESTUSDT"],set())
        self.assertTrue(feed._MISSED_MOVES[0]["had_approved_buy"])
    def test_qualification_report_surfaces_blockers_not_buy(self):
        feed.ML=SimpleNamespace(_last_board_ms=NOW,_board=[
            {"symbol":"TESTUSDT","lane":"BEAST",
             "qualification_state":"DATA BLOCKED",
             "blockers":["STALE_TRADE"],"selected_target_pct":20}])
        rows,counts=feed._qualification_report(NOW)
        self.assertEqual(rows[0]["blockers"],["STALE_TRADE"])
        self.assertEqual(counts["DATA BLOCKED"],1)
        self.assertNotIn("entry",rows[0])

if __name__=="__main__":
    unittest.main()
