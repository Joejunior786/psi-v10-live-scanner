import unittest
from unittest.mock import patch
import asyncio
from types import SimpleNamespace
import psi_v15_17_ema_lane as lane

def snap(**kw):
    d={"current":100,"low":99.5,"high":101,"close":100.5,"atr":1,
       "ema50":100,"ema200":100,"has_ema50":True,"has_ema200":True,
       "buy_ratio":.62,"buy_ratio_3":.58,"lower_wick":.35,
       "close_strength":.8,"falling_volume":True}
    d.update(kw)
    return d
GOOD={"verified":True,"blockers":[]}
FLOW={"micro_ready":True,"sequence_verified":True,"book_sequence_verified":True,"cvd_acceleration":1.0,"ofi_acceleration":.1}
class EMATests(unittest.TestCase):
    def test_all_six_independent(self):
        for tf in ("1h","4h","1d"):
            x=lane.evaluate(snap(),tf,1000,1000,GOOD,FLOW)
            self.assertEqual([v["status"] for v in x],["BUY NOW — EMA"]*2)
    def test_report_without_core_is_safe(self):
        old = lane.CORE
        try:
            lane.CORE = None
            self.assertIsNone(lane.emit_report())
        finally:
            lane.CORE = old
    def test_report_supervisor_runs_independently(self):
        calls = []
        async def once(_):
            raise asyncio.CancelledError()
        async def run():
            with patch.object(lane,"emit_report",side_effect=lambda:calls.append(1)):
                with patch.object(lane.asyncio,"sleep",side_effect=once):
                    with self.assertRaises(asyncio.CancelledError):
                        await lane.reporting_supervisor()
        asyncio.run(run())
        self.assertEqual(len(calls),1)
    def test_one_primary_entry_per_coin(self):
        records=[("A",{"timeframe":"1h","ema_period":50,"status":"ARMED","touch":True,"distance_pct":.1}),
                 ("A",{"timeframe":"4h","ema_period":200,"status":"PRE-IGNITION","touch":True,"distance_pct":.3}),
                 ("B",{"timeframe":"1d","ema_period":50,"status":"WATCH","touch":False,"distance_pct":.8})]
        rows, history, unique = lane.select_ema_report(records,{},1,limit=10)
        self.assertEqual(unique,2)
        self.assertEqual(len(rows),2)
        self.assertEqual(next(x for x in rows if x[0]=="A")[3],2)
        self.assertEqual(next(x for x in rows if x[0]=="A")[1]["ema_period"],200)
    def test_rotation_changes_unchanged_candidates(self):
        records=[(str(i),{"timeframe":"1h","ema_period":50,"status":"ARMED","touch":True,"distance_pct":.1+i*.01}) for i in range(12)]
        first,h,_=lane.select_ema_report(records,{},1,limit=6)
        second,h,_=lane.select_ema_report(records,h,2,limit=6)
        self.assertNotEqual([r[0] for r in first],[r[0] for r in second])
        self.assertEqual(len({r[0] for r in second}),6)
    def test_buy_never_displaced_by_rotation(self):
        records=[(str(i),{"timeframe":"1h","ema_period":50,"status":"ARMED","touch":True,"distance_pct":.01}) for i in range(9)]
        records.append(("BUY",{"timeframe":"4h","ema_period":200,"status":"BUY NOW — EMA","touch":True,"distance_pct":.3}))
        rows,_,_=lane.select_ema_report(records,{},2,limit=4)
        self.assertEqual(rows[0][0],"BUY")
    def test_history_marks_unchanged_and_improving(self):
        records=[("A",{"timeframe":"1h","ema_period":50,"status":"ARMED","touch":True,"distance_pct":.2})]
        _,h,_=lane.select_ema_report(records,{},1)
        rows,h,_=lane.select_ema_report(records,h,2)
        self.assertEqual(rows[0][2],"UNCHANGED")
        records[0][1]["status"]="PRE-IGNITION"
        rows,h,_=lane.select_ema_report(records,h,3)
        self.assertEqual(rows[0][2],"IMPROVING")
    def test_marginal_buyer_strength_now_qualifies(self):
        case=snap(buy_ratio=.525,buy_ratio_3=.515,lower_wick=.21,close_strength=.56)
        self.assertTrue(all(x["status"]=="BUY NOW — EMA" for x in lane.evaluate(case,"1h",1000,1000,GOOD,FLOW)))
    def test_weak_buying_still_blocked(self):
        case=snap(buy_ratio=.49,buy_ratio_3=.48)
        self.assertTrue(all(x["status"]!="BUY NOW — EMA" for x in lane.evaluate(case,"1h",1000,1000,GOOD,FLOW)))
    def test_invalid_sequence_still_blocks(self):
        bad=dict(FLOW,book_sequence_verified=False)
        self.assertTrue(all(x["status"]!="BUY NOW — EMA" for x in lane.evaluate(snap(),"1h",1000,1000,GOOD,bad)))
    def test_marginal_risk_plan_accepted(self):
        case=snap(low=96.1,atr=1.0)
        outcome=lane.evaluate(case,"1h",1000,1000,GOOD,FLOW)
        self.assertTrue(all(x["status"]=="BUY NOW — EMA" for x in outcome))
    def test_independent_buy_with_no_v12_structural_row(self):
        live = dict(FLOW, last_trade_ms=1000000, last_book_ms=1000000,
                    last_price=100, spread_bps=5, slippage_bps=12)
        core=SimpleNamespace(_cache={"TESTUSDT":{"1h":{"snap":snap(),"updated":1000}}},
                             app=SimpleNamespace(micro_metrics=lambda symbol:live))
        records, frames, tested = lane.scan_cached_ema(core,now=1000)
        self.assertEqual(frames,1)
        self.assertEqual(tested,1)
        self.assertEqual(len(records),2)
        self.assertTrue(all(x["status"]=="BUY NOW — EMA" for _,x in records))
        self.assertTrue(all(x["evidence_status"]=="LIVE_VERIFIED" for _,x in records))

    def test_missing_spread_never_promotes(self):
        micro=dict(FLOW, last_trade_ms=1000000, last_book_ms=1000000,
                   last_price=100, slippage_bps=12)
        core=SimpleNamespace(_cache={"TESTUSDT":{"1h":{"snap":snap(),"updated":1000}}},
                             app=SimpleNamespace(micro_metrics=lambda symbol:micro))
        rows,_,_=lane.scan_cached_ema(core,now=1000)
        self.assertTrue(all(x["status"]!="BUY NOW — EMA" for _,x in rows))
        self.assertTrue(all(x["evidence_status"]=="LIVE_BLOCKED" for _,x in rows))

    def test_stale_micro_and_bad_sequence_never_promote(self):
        micro=dict(FLOW,last_trade_ms=980000,last_book_ms=1000000,last_price=100,
                   spread_bps=5,slippage_bps=10,sequence_verified=False)
        core=SimpleNamespace(_cache={"TESTUSDT":{"1h":{"snap":snap(),"updated":1000}}},
                             app=SimpleNamespace(micro_metrics=lambda symbol:micro))
        rows,_,_=lane.scan_cached_ema(core,now=1000)
        self.assertFalse(any(x["status"]=="BUY NOW — EMA" for _,x in rows))

    def test_research_top_ten_does_not_cap_verified_buys(self):
        micro=dict(FLOW,last_trade_ms=1000000,last_book_ms=1000000,last_price=100,
                   spread_bps=5,slippage_bps=10)
        core=SimpleNamespace(_cache={f"T{i}USDT":{"1h":{"snap":snap(),"updated":1000}}
                                          for i in range(12)},
                             app=SimpleNamespace(micro_metrics=lambda symbol:micro))
        old=lane.CORE
        try:
            lane.CORE=core
            with patch.object(lane.time,"time",return_value=1000):
                board=lane.emit_report()
            self.assertEqual(len(board["top10_research"]),10)
            self.assertEqual(len(board["execution_ready_symbols"]),12)
            self.assertFalse(board["order_placement"])
        finally:
            lane.CORE=old

    def test_missing_live_evidence_is_not_negative_evidence(self):
        rows=lane.evaluate(snap(),"1h",1000,1000,{}, {})
        self.assertTrue(all("MICRO_NOT_EVALUATED" in x["blockers"] for x in rows))
        self.assertTrue(all("INVALID_MICRO_SEQUENCE" not in x["blockers"] for x in rows))
        self.assertFalse(any(x["status"]=="BUY NOW — EMA" for x in rows))
    def test_missing_data_rejected(self):
        self.assertFalse(any(v["status"]=="BUY NOW — EMA" for v in lane.evaluate(snap(),"1h",1000,1000,{},FLOW)))
    def test_seller_not_exhausted(self):
        x=lane.evaluate(snap(falling_volume=False),"4h",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]!="BUY NOW — EMA" for v in x))
    def test_negative_cvd_blocks_execution(self):
        bad = dict(FLOW, cvd_acceleration=-1)
        self.assertTrue(all(x["status"]!="BUY NOW — EMA" for x in lane.evaluate(snap(),"1h",1000,1000,GOOD,bad)))
    def test_stale_candle_rejected(self):
        self.assertEqual(lane.evaluate(snap(),"1h",700,1000,GOOD,FLOW),[])
    def test_no_reclaim_rejected(self):
        x=lane.evaluate(snap(close=99),"1d",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]!="BUY NOW — EMA" for v in x))
    def test_no_unrelated_strategy_gate(self):
        x=lane.evaluate(snap(),"1h",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]=="BUY NOW — EMA" for v in x))
if __name__=="__main__":unittest.main()
