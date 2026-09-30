import asyncio
import math
import time
from collections import defaultdict, deque

import ignition117_entry as b17

scanner=b17.scanner; q=scanner.q; s=scanner.s; app=scanner.app; b16=b17.b16
VERSION="10.18.0-pump-signature-dynamic-resistance"
PUMP_SAMPLE_SECONDS=5.0; PUMP_WINDOW_SECONDS=300.0
PUMP_WATCH_SCORE=62.0; PUMP_ARMED_SCORE=76.0; PUMP_HOT_SCORE=76.0
LOCAL_EXCLUDE_SECONDS=3.0

rapid_hist=defaultdict(lambda:deque(maxlen=180))
metric_hist=defaultdict(lambda:deque(maxlen=180))
local_dist_hist=defaultdict(lambda:deque(maxlen=180))
pump_cache={}
pump_stats={"samples":0,"watch":0,"armed":0,"hot_candidates":0}

_raw_hot=q.hot; _old_diag=b17.diag_pool; _old_opp=b17.opp_score


def f(v,d=0.0):
    try: x=float(v)
    except (TypeError,ValueError): return d
    return x if math.isfinite(x) else d

def clamp(x,lo,hi): return max(lo,min(hi,x))
def latest(sym): return q.latest.get(sym) or {}


def micro_snapshot(sym):
    r=latest(sym); ranks=r.get("relative_ranks") or {}
    return {
        "micro":bool(r.get("micro_ready") or r.get("micro_collection_ready")),
        "seq":bool(r.get("sequence_verified")),"bookseq":bool(r.get("book_sequence_verified")),
        "relflow":bool(r.get("relative_flow")),"relbook":bool(r.get("relative_book")),
        "relactivity":bool(r.get("relative_activity")),"vwap":bool(r.get("vwap_reclaim")),
        "buy":f(r.get("aggressive_buy_ratio"),0.5),"ofip":f(r.get("ofi_persistence")),
        "flowp":f(r.get("flow_persistence")),"layers":b16._layers(r) if r else 0,
        "exec":b16._execution_pass(r) if r else False,
        "ignv":f(r.get("ignition_velocity_per_min")),"igna":f(r.get("ignition_acceleration_per_min2")),
        "rrv":f(ranks.get("rv30"),0.5),"rtrade":f(ranks.get("trade_acc"),0.5),
        "rcvd":f(ranks.get("cvd_acc"),0.5),"rofi":f(ranks.get("ofi"),0.5),
        "robi":f(ranks.get("obi"),0.5),"rask":f(ranks.get("ask_dep"),0.5),
    }


def local_resistance(sym,now_ms=None):
    n=int(now_ms or app.now_ms()); st=app.micro_state.get(sym) or {}; trades=st.get("trades")
    price=f(app.current_symbol_price(sym))
    if not trades:
        return {"price":price or None,"r60":None,"r180":None,"d60":None,"d180":None,"break60":False}
    rows=list(trades); cutoff=n-int(LOCAL_EXCLUDE_SECONDS*1000)
    p60=[f(r[3]) for r in rows if n-60000<=r[0]<=cutoff]
    p180=[f(r[3]) for r in rows if n-180000<=r[0]<=cutoff]
    if not price and rows: price=f(rows[-1][3])
    r60=max(p60) if p60 else 0.0; r180=max(p180) if p180 else 0.0
    def dist(res): return ((res-price)/price*100.0) if price>0 and res>0 else None
    return {"price":price or None,"r60":r60 or None,"r180":r180 or None,"d60":dist(r60),"d180":dist(r180),"break60":bool(price>0 and r60>0 and price>=r60)}


def hist_delta(sym,key,secs):
    dq=metric_hist.get(sym)
    if not dq or len(dq)<2: return 0.0
    now,cur=dq[-1][0],f(dq[-1][1].get(key)); old=dq[0]
    for x in reversed(dq):
        if x[0]<=now-secs: old=x; break
    return cur-f(old[1].get(key))


def local_velocity(sym,secs):
    dq=local_dist_hist.get(sym)
    if not dq or len(dq)<2: return 0.0
    now,cur=dq[-1]; old=dq[0]
    for x in reversed(dq):
        if x[0]<=now-secs: old=x; break
    dt=max(1.0,now-old[0]); return (old[1]-cur)/(dt/60.0)


def rapid_stats(sym,now=None):
    now=now or time.time(); dq=rapid_hist.get(sym)
    if not dq: return {"raw":0.0,"h120":0,"h140":0,"h160":0,"peak":0.0,"persist":0.0}
    while dq and dq[0][0]<now-PUMP_WINDOW_SECONDS: dq.popleft()
    vals=[x[1] for x in dq]; h120=sum(v>=120 for v in vals); h140=sum(v>=140 for v in vals); h160=sum(v>=160 for v in vals)
    return {"raw":vals[-1] if vals else 0.0,"h120":h120,"h140":h140,"h160":h160,"peak":max(vals) if vals else 0.0,
            "persist":min(1.0,(h120+h140*.75+h160*1.25)/12.0)}


def pump_signature(sym):
    r=latest(sym); m=micro_snapshot(sym); rr=rapid_stats(sym); lr=local_resistance(sym)
    discovery=max(rr["raw"],f(q.sscore(sym)))
    c_disc=clamp((discovery-100.0)/4.0,0,20)
    c_repeat=clamp(rr["h120"]*1.0+rr["h140"]*1.4+rr["h160"]*2.0,0,16)
    rank_mean=(m["rrv"]+m["rtrade"]+m["rcvd"]+m["rofi"])/4.0
    c_activity=clamp((rank_mean-.50)*24.0,0,12)
    c_ign=clamp(m["ignv"]/10.0,0,6)+clamp(m["igna"]/30.0,0,6)
    c_flow=(3 if m["micro"] else 0)+(2 if m["seq"] else 0)+(2 if m["bookseq"] else 0)+(2 if m["relflow"] else 0)+(2 if m["relbook"] else 0)+(2 if m["relactivity"] else 0)+(2 if m["vwap"] else 0)
    c_flow+=clamp((m["buy"]-.50)*20.0,0,2)+clamp(max(m["ofip"],m["flowp"])*3.0,0,3)
    c_structure=clamp(m["layers"]/6.0*7.0,0,7)+(3 if m["exec"] else 0)
    ld=lr.get("d60")
    if ld is None: c_local=0.0
    elif ld<-.25: c_local=1.0
    elif ld<=.35: c_local=8.0
    elif ld<=1.0: c_local=6.5
    elif ld<=2.0: c_local=5.0
    elif ld<=4.0: c_local=3.0
    else: c_local=1.0
    lv30=local_velocity(sym,30); drv=hist_delta(sym,"rrv",30); dtr=hist_delta(sym,"rtrade",30); dofi=hist_delta(sym,"rofi",30)
    c_trend=clamp(lv30*4.0,0,4)+clamp((drv+dtr+dofi)*4.0,0,6)
    score=c_disc+c_repeat+c_activity+c_ign+c_flow+c_structure+c_local+c_trend
    if str(r.get("entry_status") or "")=="RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER": score-=12
    blockers=set(r.get("combined_blockers") or [])
    if "ANTI_CHASE_OR_RUNNER" in blockers or "ANTI_CHASE_OR_RUNNER_LAYER" in blockers: score-=8
    score=round(clamp(score,0,100),1)
    state="PUMP-ARMED" if score>=PUMP_ARMED_SCORE and m["micro"] and m["exec"] else ("PUMP-WATCH" if score>=PUMP_WATCH_SCORE else "NONE")
    return {"pump_signature_score":score,"pump_state":state,"rapid_score":round(discovery,1),"rapid_peak_5m":round(rr["peak"],1),
            "rapid_hits_120_5m":rr["h120"],"rapid_hits_140_5m":rr["h140"],"rapid_hits_160_5m":rr["h160"],"rapid_persistence":round(rr["persist"],3),
            "local_resistance_60s":lr.get("r60"),"local_resistance_180s":lr.get("r180"),"local_distance_60s_pct":lr.get("d60"),"local_distance_180s_pct":lr.get("d180"),
            "local_breakout_60s":lr.get("break60"),"local_dv30_per_min":round(lv30,4),"rank_rv_trend_30s":round(drv,4),"rank_trade_trend_30s":round(dtr,4),"rank_ofi_trend_30s":round(dofi,4),
            "micro_ready_118":m["micro"],"exec_pass_118":m["exec"],"layers_118":m["layers"]}


def augmented_hot(limit=None):
    n=max(int(limit or getattr(q,"HOT_COUNT",80)),80); raw=list(_raw_hot(max(n*2,160)))
    scores={sym:f(score) for score,sym in raw if b17.directional(sym)}
    candidates=set(scores)|{x for x in q.latest if b17.directional(x)}|{x for x in app.anomaly_state if b17.directional(x)}
    for sym in candidates:
        ps=pump_cache.get(sym) or pump_signature(sym); pump_cache[sym]=ps; p=f(ps.get("pump_signature_score"))
        if p>=PUMP_HOT_SCORE: scores[sym]=max(scores.get(sym,0.0),80.0+p)
    ranked=sorted(((score,sym) for sym,score in scores.items()),reverse=True)
    return ranked[:(limit or getattr(q,"HOT_COUNT",80))]


def diag_pool118():
    out=[]
    for raw in _old_diag():
        row=dict(raw); sym=str(row.get("symbol") or "")
        if not sym or not b17.directional(sym): continue
        ps=pump_cache.get(sym) or pump_signature(sym); pump_cache[sym]=ps; row.update(ps); out.append(row)
    return out


def opp_score118(row):
    base=f(_old_opp(row)); ps=f(row.get("pump_signature_score")); bonus=0.0
    if ps>=PUMP_ARMED_SCORE: bonus=22.0+min(10.0,(ps-PUMP_ARMED_SCORE)*.5)
    elif ps>=PUMP_WATCH_SCORE: bonus=10.0+min(10.0,(ps-PUMP_WATCH_SCORE)*.7)
    return round(base+bonus,2)


async def pump_sampler():
    while True:
        await asyncio.sleep(PUMP_SAMPLE_SECONDS)
        try:
            now=time.time(); raw=list(_raw_hot(160)); rawmap={sym:f(score) for score,sym in raw if b17.directional(sym)}
            syms=set(rawmap)|{x for x in app.selected_micro_symbols if b17.directional(x)}|{x for x in q.latest if b17.directional(x)}
            for sym in syms:
                score=rawmap.get(sym,f(q.sscore(sym))); rh=rapid_hist[sym]; rh.append((now,score))
                while rh and rh[0][0]<now-PUMP_WINDOW_SECONDS: rh.popleft()
                m=micro_snapshot(sym); mh=metric_hist[sym]; mh.append((now,{"rrv":m["rrv"],"rtrade":m["rtrade"],"rcvd":m["rcvd"],"rofi":m["rofi"],"robi":m["robi"],"rask":m["rask"]}))
                while mh and mh[0][0]<now-PUMP_WINDOW_SECONDS: mh.popleft()
                ld=local_resistance(sym).get("d60")
                if ld is not None and math.isfinite(float(ld)):
                    dh=local_dist_hist[sym]; dh.append((now,float(ld)))
                    while dh and dh[0][0]<now-PUMP_WINDOW_SECONDS: dh.popleft()
                pump_cache[sym]=pump_signature(sym)
            vals=list(pump_cache.values()); pump_stats["samples"]+=1
            pump_stats["watch"]=sum(x.get("pump_state")=="PUMP-WATCH" for x in vals); pump_stats["armed"]=sum(x.get("pump_state")=="PUMP-ARMED" for x in vals); pump_stats["hot_candidates"]=sum(f(x.get("pump_signature_score"))>=PUMP_HOT_SCORE for x in vals)
        except asyncio.CancelledError: raise
        except Exception as e: print(f"Ψ-V10.18 PUMP_SAMPLER_ERROR {type(e).__name__}: {e}",flush=True)


async def pump_print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            rows=[(f(ps.get("pump_signature_score")),sym,ps) for sym,ps in pump_cache.items() if b17.directional(sym) and f(ps.get("pump_signature_score"))>=45]
            rows.sort(reverse=True); top=rows[:10]
            print(f"Ψ-V10.18 PUMP BOARD {len(top)}/10 watch={pump_stats['watch']} armed={pump_stats['armed']} hot={pump_stats['hot_candidates']} samples={pump_stats['samples']}",flush=True)
            for i,(score,sym,ps) in enumerate(top,1):
                r=latest(sym); sd=r.get("breakout_distance_pct"); ld=ps.get("local_distance_60s_pct"); lr=ps.get("local_resistance_60s")
                sdt="-" if sd is None else f"{f(sd):+.3f}%"; ldt="-" if ld is None else f"{f(ld):+.3f}%"; lrt="-" if lr is None else f"{f(lr):.10g}"
                print(f"P{i:02d}. {sym:14s} pump={ps.get('pump_state','NONE'):10s} sig={score:5.1f} rapid={f(ps.get('rapid_score')):5.1f} peak={f(ps.get('rapid_peak_5m')):5.1f} hits120/140/160={int(ps.get('rapid_hits_120_5m',0))}/{int(ps.get('rapid_hits_140_5m',0))}/{int(ps.get('rapid_hits_160_5m',0))} localRes={lrt} localDist={ldt} localDV30={f(ps.get('local_dv30_per_min')):+.3f}/m structDist={sdt} layers={int(ps.get('layers_118',0))}/6 micro={'READY' if ps.get('micro_ready_118') else 'WAIT'} exec={'PASS_ALL' if ps.get('exec_pass_118') else 'BLOCKED'}",flush=True)
        except asyncio.CancelledError: raise
        except Exception as e: print(f"Ψ-V10.18 PUMP_PRINT_ERROR {type(e).__name__}: {e}",flush=True)


# Early pump intelligence changes ranking and hot-path priority only.
# Formal PRE-IGNITION and BUY NOW evaluation is intentionally untouched.
b17.diag_pool=diag_pool118; b17.opp_score=opp_score118; b17.hot=augmented_hot; q.hot=augmented_hot
_old_main=scanner.v7.main
async def main118(): await asyncio.gather(_old_main(),pump_sampler(),pump_print_loop())
scanner.v7.main=main118; scanner.VERSION=VERSION

print("Ψ-V10.18 UPGRADE ACTIVE — Pump Signature Score, RAPID persistence, dynamic 60s/180s local resistance, multi-factor acceleration trends, pump-score hot-path promotion; formal PRE/BUY rules unchanged",flush=True)
if __name__=="__main__":
    try: print("Ψ-V10.18 ACTIVE — early pump fingerprint + strict execution separation",flush=True); asyncio.run(scanner.v7.main())
    except KeyboardInterrupt: print("Ψ-V10.18 stopped",flush=True)
