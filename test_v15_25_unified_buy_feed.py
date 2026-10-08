"""V15.25 regression: independent approved strategies, fail-closed telemetry."""
import unittest
from types import SimpleNamespace
import psi_v15_21_signal_delivery as feed
import psi_v15_17_ema_lane as ema

NOW=1000000
def micro():
    return dict(micro_ready=True,sequence_verified=True,book_sequence_verified=True,
                last_trade_ms=NOW,last_book_ms=NOW,last_price=100,
                spread_bps=5,slippage_bps=10)
def approved():
    return dict(symbol="TESTUSDT",lane="EXHAUSTION",action="ML BUY NOW",
                execution_ready=True,promotion_ready=True,anti_chase=False,
                setup_verification="UPSTREAM_STRUCTURAL",selected_target_pct=20,
                timeframe="4H",setup="SUPPORT_RECLAIM",price=100,
                entry_low=99,entry_high=101,max_chase=102,dynamic_stop=95,
                probability=.64,expected_time_to_target="3 days",model_samples=50)
class Unified(unittest.TestCase):
    def setUp(self):
        self.prev=(feed.CORE,feed.EMA,feed.ML,feed._SNAPSHOT)
        feed._SNAPSHOT={};feed._ACTIVE=set();feed._EVENTS.clear();feed._NEXT_ID=0
        feed._PRIORITY_LEASES.clear();feed._OBSERVED_QUOTES.clear();feed._MISSED_MOVES.clear()
        self.m=micro()
        self.core=SimpleNamespace(_cache={},_board=lambda:[],
                  app=SimpleNamespace(micro_metrics=lambda s:self.m))
        self.ml=SimpleNamespace(_board=[approved()],_last_board_ms=NOW)
        feed.CORE,feed.EMA,feed.ML=self.core,ema,self.ml
    def tearDown(self):
        feed.CORE,feed.EMA,feed.ML,feed._SNAPSHOT=self.prev
        feed._ACTIVE=set();feed._EVENTS.clear()
    def test_verified_ml_passes_and_has_live_quote(self):
        s=feed.publish_once(NOW)
        self.assertEqual(s["foreign_approved_count"],1)
        self.assertEqual(self.core._signal_priority_symbols,["TESTUSDT"])
        v=feed.read_live(NOW)
        self.assertEqual(v["buy_count"],1)
        self.assertEqual(v["buy_signals"][0]["authority"],"V15_ML")
        self.assertEqual(v["buy_signals"][0]["target_pct"],20)
        self.assertEqual(feed._EVENTS[-1]["authority"],"V15_ML")
    def test_revoke_stale_trade_and_book_without_refresh(self):
        feed.publish_once(NOW)
        self.m["last_trade_ms"]=NOW-2000
        self.assertEqual(feed.read_live(NOW)["buy_count"],0)
        self.m=micro();self.m["last_book_ms"]=NOW-2000
        self.assertEqual(feed.read_live(NOW)["buy_count"],0)
    def test_revoke_model_when_approval_changes(self):
        feed.publish_once(NOW)
        self.ml._board[0]["action"]="WAIT"
        self.assertEqual(feed.read_live(NOW)["buy_count"],0)
    def test_unapproved_or_shadow_never_become_buy(self):
        for change in (dict(action="WAIT"),dict(execution_ready=False),
                       dict(promotion_ready=False),
                       dict(setup_verification="PROVISIONAL_SENSOR"),
                       dict(selected_target_pct=3),dict(anti_chase=True)):
            self.ml._board=[dict(approved(),**change)]
            s=feed.publish_once(NOW)
            self.assertEqual(s["foreign_approved_count"],0)
            self.assertEqual(feed.read_live(NOW)["buy_count"],0)
    def test_out_of_zone_and_old_model_are_blocked(self):
        row=dict(approved(),authority="V15_ML")
        self.m=dict(micro(),last_price=104)
        self.assertIsNone(feed._foreign_signal_verified(row,NOW))
        self.m=micro();self.ml._last_board_ms=NOW-30000
        self.assertIsNone(feed._foreign_signal_verified(row,NOW))
    def test_v12_execution_not_structural_buy_only(self):
        row=dict(symbol="TESTUSDT",state="BUY",execution_state="BUY NOW",
                 buy_now=True,generated_ms=NOW)
        self.ml._board=[];self.core._board=lambda:[row]
        self.core._attach_execution_gate=lambda s,r:dict(r,
            current=100,invalidation=95,tp1=110,tp2=120,tp3=130,
            timeframe="1H",setup="GOLDEN_CROSS")
        feed.publish_once(NOW)
        self.assertEqual(feed.read_live(NOW)["buy_signals"][0]["authority"],"V12_PINPOINT")
        self.core._attach_execution_gate=lambda s,r:dict(r,buy_now=False,
                                                         execution_state="COLLECTING DATA")
        self.assertEqual(feed.read_live(NOW)["buy_count"],0)
    def test_missing_micro_is_not_execution_ready(self):
        self.m={}
        s=feed.publish_once(NOW)
        self.assertEqual(s["authority_diagnostics"]["ml_approved"],1)
        self.assertEqual(s["foreign_approved_count"],0)
        self.assertEqual(feed.read_live(NOW)["status"],"NO_VERIFIED_BUY")
if __name__=="__main__":
    unittest.main()
