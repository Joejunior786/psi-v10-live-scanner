import unittest
import psi_v15_17_ema_lane as lane

def snap(**kw):
    d={"current":100,"low":99.5,"high":101,"close":100.5,"atr":1,
       "ema50":100,"ema200":100,"has_ema50":True,"has_ema200":True,
       "buy_ratio":.62,"buy_ratio_3":.58,"lower_wick":.35,
       "close_strength":.8,"falling_volume":True}
    d.update(kw)
    return d
GOOD={"verified":True,"blockers":[]}
FLOW={"micro_ready":True,"sequence_verified":True,"book_sequence_verified":True}
class EMATests(unittest.TestCase):
    def test_all_six_independent(self):
        for tf in ("1h","4h","1d"):
            x=lane.evaluate(snap(),tf,1000,1000,GOOD,FLOW)
            self.assertEqual([v["status"] for v in x],["BUY NOW — EMA"]*2)
    def test_missing_data_rejected(self):
        self.assertFalse(any(v["status"]=="BUY NOW — EMA" for v in lane.evaluate(snap(),"1h",1000,1000,{},FLOW)))
    def test_seller_not_exhausted(self):
        x=lane.evaluate(snap(falling_volume=False),"4h",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]!="BUY NOW — EMA" for v in x))
    def test_stale_candle_rejected(self):
        self.assertEqual(lane.evaluate(snap(),"1h",700,1000,GOOD,FLOW),[])
    def test_no_reclaim_rejected(self):
        x=lane.evaluate(snap(close=99),"1d",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]!="BUY NOW — EMA" for v in x))
    def test_no_unrelated_strategy_gate(self):
        x=lane.evaluate(snap(),"1h",1000,1000,GOOD,FLOW)
        self.assertTrue(all(v["status"]=="BUY NOW — EMA" for v in x))
if __name__=="__main__":unittest.main()
