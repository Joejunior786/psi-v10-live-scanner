import asyncio, math, time
from collections import defaultdict
import ignition1191_entry as base

scanner, b17, q, app = base.scanner, base.b17, base.q, base.app
VERSION = "10.19.3-trend-pullback-monitor-integrity-sync"
TREND_EVERY, TREND_BATCH, TREND_AGE = 45.0, 24, 16 * 60.0
PB_EVERY, PB_MAX, PB_AGE = 15.0, 24, 90.0
ARM_STREAK, MIN_DEPTH, MAX_DEPTH = 2, 0.35, 8.0
LAYERS = ("ACTIVITY_LAYER","FLOW_LAYER","ORDER_BOOK_LAYER","VWAP_LAYER","MA_STRUCTURE_LAYER","ANTI_CHASE_OR_RUNNER_LAYER")
HARD = ("LIVE_MICRO_DATA","TRADE_SEQUENCE_VALID","BOOK_SEQUENCE_VALID","SPREAD_FILTER","SLIPPAGE_FILTER","CUMULATIVE_EXTENSION_GUARD","MARKET_REGIME_SAFETY")
trend_cache, pb_cache = {}, {}
pb_mem = defaultdict(lambda: {"streak": 0, "armed_once": False})
cursor = 0
stats = {"trend":0,"pb":0,"errors":0}
_old_main, _old_diag = scanner.v7.main, b17.diag_pool
_learn = getattr(base, "_learn_mod", None)
_old_feat = getattr(_learn, "feature_snapshot", None) if _learn else None
_old_keys = getattr(_learn, "pattern_keys", None) if _learn else None

def f(v,d=0.0):
    try: x=float(v)
    except (TypeError,ValueError): return d
    return x if math.isfinite(x) else d

def clamp(x,a,b): return max(a,min(b,x))
def pct(a,b): return ((a/b)-1)*100 if a and b else 0.0

def px(v):
    x=f(v)
    if x<=0:return "-"
    if x>=1000:return f"{x:.2f}"
    if x>=1:return f"{x:.6f}".rstrip("0").rstrip(".")
    if x>=.01:return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")

def price(sym):
    try:return f(base.current_price(sym))
    except Exception:return f((q.latest.get(sym) or {}).get("price"))

def candles(rows):
    out=[]
    for r in rows or []:
        if r and len(r)>=7:
            out.append({"o":f(r[1]),"h":f(r[2]),"l":f(r[3]),"c":f(r[4]),"v":f(r[5])})
    return out

def ema(vals,n):
    vals=[f(x) for x in vals if f(x)>0]
    if len(vals)<n:return []
    a=2/(n+1); cur=sum(vals[:n])/n; out=[None]*(n-1)+[cur]
    for x in vals[n:]: cur=a*x+(1-a)*cur; out.append(cur)
    return out

def elast(vals,n,back=0):
    s=ema(vals,n); i=len(s)-1-back
    return f(s[i]) if 0<=i<len(s) else 0.0

def atr(cs):
    if len(cs)<15:return 0.0
    prev=f(cs[-15]["c"]); tr=[]
    for c in cs[-14:]:
        tr.append(max(c["h"]-c["l"],abs(c["h"]-prev),abs(c["l"]-prev))); prev=c["c"]
    return sum(tr)/len(tr) if tr else 0.0

def vwap(cs,n=32):
    num=den=0.0
    for c in cs[-n:]:
        if c["v"]>0:
            num+=((c["h"]+c["l"]+c["c"])/3)*c["v"]; den+=c["v"]
    return num/den if den else 0.0

def directional(s):
    try:return bool(base.b17.directional(s))
    except Exception:return str(s).endswith("USDT")

def universe():
    s=set(q.latest.keys())|set(getattr(app,"selected_micro_symbols",[]) or [])
    return sorted(x for x in s if str(x).endswith("USDT") and directional(x))

def hot(limit=12):
    try:return [s for s in base.base.candidate_symbols(limit) if directional(s)][:limit]
    except Exception:return []

def trend_batch():
    global cursor
    u=universe(); p=hot(12); rest=[s for s in u if s not in set(p)]
    slots=max(0,TREND_BATCH-len(p))
    if not rest:return p[:TREND_BATCH]
    if cursor>=len(rest):cursor=0
    rot=[rest[(cursor+i)%len(rest)] for i in range(slots)]
    cursor=(cursor+slots)%len(rest)
    return (p+rot)[:TREND_BATCH]

def trend_eval(sym,c1,c4):
    a1=c1[:-1] if len(c1)>1 else c1; a4=c4[:-1] if len(c4)>1 else c4
    if len(a1)<205 or len(a4)<205:return {"trend_state":"WARMING","trend_score":0.0}
    z1=[x["c"] for x in a1]; z4=[x["c"] for x in a4]; p=price(sym) or z1[-1]
    e20=elast(z1,20); e50=elast(z1,50); e200=elast(z1,200); e50o=elast(z1,50,5)
    f50=elast(z4,50); f200=elast(z4,200); f50o=elast(z4,50,3)
    s1=pct(e50,e50o) if e50o else 0; s4=pct(f50,f50o) if f50o else 0
    score=(18 if p>e50 else 0)+(15 if e50>e200 else 0)+(8 if p>e200 else 0)+(9 if s1>0 else 4 if s1>-.12 else 0)+(20 if p>f50 else 0)+(18 if f50>f200 else 0)+(6 if p>f200 else 0)+(6 if s4>0 else 3 if s4>-.08 else 0)
    up=score>=82 and p>e50>e200>0 and p>f50>f200>0 and s1>-.12 and s4>-.08
    high=max((x["h"] for x in a1[-24:]),default=p); dd=((high-p)/high*100) if high else 0
    return {"trend_state":"UPTREND" if up else "TREND-WEAK" if score>=68 and p>f200>0 else "NO-TREND","trend_score":round(score,1),"ema20_1h":e20,"ema50_1h":e50,"ema200_1h":e200,"ema50_4h":f50,"ema200_4h":f200,"recent_high":high,"depth":round(dd,4),"price":p}

async def fetch_trend(sym):
    try:
        r1=await app.load_klines(app.session,sym,"1h",230)
        r4=await app.load_klines(app.session,sym,"4h",230)
        x=trend_eval(sym,candles(r1),candles(r4)); x.update(symbol=sym,updated=time.time()); trend_cache[sym]=x
    except asyncio.CancelledError: raise
    except Exception: stats["errors"]+=1

async def trend_loop():
    sem=asyncio.Semaphore(2)
    async def one(s):
        async with sem: await fetch_trend(s)
    while True:
        await asyncio.sleep(TREND_EVERY)
        try: await asyncio.gather(*(one(s) for s in trend_batch())); stats["trend"]+=1
        except asyncio.CancelledError: raise
        except Exception: stats["errors"]+=1

def gate(sym):
    r=q.latest.get(sym) or {}; lr=r.get("layer_results") or {}; hs=r.get("hard_safety_status") or {}
    lc=sum(bool(lr.get(k)) for k in LAYERS)
    hp=all((hs.get(k,"PASS") if k=="MARKET_REGIME_SAFETY" else hs.get(k))=="PASS" for k in HARD)
    return {"layers":lc,"hard_pass":hp,"formal":str(r.get("pre_warmup_state") or r.get("state") or "")}

def mtf_ctx(sym):
    try:
        fr=((base.base.mtf_cache.get(sym) or {}).get("frames") or {}).get("5m") or {}
        age=f(fr.get("break_age"),99999); phase=str(fr.get("phase") or "UNKNOWN")
        first=age<=2700 and phase in {"BREAKOUT_HOLD","RETEST","CONTINUATION","BREAKOUT_IN_PROGRESS"} and not pb_mem[sym]["armed_once"]
        return first,f(fr.get("used_level")),phase
    except Exception:return False,0.0,"UNKNOWN"

def sell_ratio(c5):
    z=c5[:-1] if len(c5)>1 else c5
    if len(z)<10:return 1.0
    rv=lambda c:c["v"] if c["c"]<c["o"] else 0.0
    a=sum(rv(c) for c in z[-3:])/3; b=sum(rv(c) for c in z[-9:-3])/6
    return a/b if b>0 else 1.0

def zone_pick(p,tm,e5,e15,e50,vw,ri,used):
    cand=[]
    for name,lv,bonus in (("EMA20_1H",tm.get("ema20_1h"),.10),("EMA50_1H",tm.get("ema50_1h"),.16),("EMA20_15M",e15,.05),("EMA50_15M",e50,.08),("EMA20_5M",e5,.02),("VWAP15",vw,.08),("SUPPORT1",ri.get("support1"),.18),("SUPPORT2",ri.get("support2"),.12),("BREAKOUT_RETEST",used,.20)):
        lv=f(lv)
        if lv>0 and p>0 and abs(pct(p,lv))<=5: cand.append((abs(pct(p,lv))-bonus,abs(pct(p,lv)),name,lv))
    if not cand:return "NONE",0.0,99.0
    _,d,n,l=min(cand); return n,l,d

def pb_plan(p,c5,a,zone,tm,ri):
    z=c5[:-1] if len(c5)>1 else c5
    if len(z)<8 or zone<=0:return (None,)*6+("WAIT",)
    entry=max(zone*1.0003,z[-1]["h"]+max(p*.0005,a*.06))
    if pct(entry,p)>1.75:return (None,)*6+("WAIT_RECLAIM",)
    lows=[x for x in [min(c["l"] for c in z[-8:]),f(ri.get("support1")),f(ri.get("support2")),f(ri.get("stop_cluster"))] if 0<x<entry and pct(entry,x)<=5]
    if not lows:return (None,)*6+("WAIT_SUPPORT",)
    anchor=min(lows); stop=anchor-max(anchor*.0015,a*.25); risk=entry-stop; rp=risk/entry*100 if entry else 99
    if not (.20<=rp<=4.0):return entry,stop,None,None,None,rp,"RISK_OUT"
    high=f(tm.get("recent_high")); t1=high*.999 if high>entry+.6*risk and high<=entry+2.5*risk else entry+1.2*risk
    res=sorted({f(ri.get(k)) for k in ("resistance1","resistance2","resistance3") if f(ri.get(k))>entry})
    t2=(next((x*.999 for x in res if x>=entry+1.45*risk),None) or entry+2*risk)
    t3=(next((x*.999 for x in res if x>=max(t2+.25*risk,entry+2.35*risk)),None) or entry+3*risk)
    # Enforce strictly increasing take-profit ladders after structural snapping.
    t1=max(t1,entry+.70*risk)
    t2=max(t2,t1+.25*risk,entry+1.45*risk)
    t3=max(t3,t2+.25*risk,entry+2.35*risk)
    return entry,stop,t1,t2,t3,rp,"CONDITIONAL_RECLAIM"

def pb_eval(sym,tm,c5,c15):
    p=price(sym) or f(tm.get("price")); ri=base.risk_intel(sym); g=gate(sym); first,used,phase=mtf_ctx(sym)
    z5=c5[:-1] if len(c5)>1 else c5; z15=c15[:-1] if len(c15)>1 else c15
    if len(z5)<25 or len(z15)<52:return {"state":"WARMING","score":0.0}
    e5=elast([x["c"] for x in z5],20); e15=elast([x["c"] for x in z15],20); e50=elast([x["c"] for x in z15],50); vw=vwap(z15); a=atr(z5); ap=a/p*100 if a and p else .5
    zn,zl,zd=zone_pick(p,tm,e5,e15,e50,vw,ri,used); near=zd<=clamp(.40+ap*.65,.45,1.25); sr=sell_ratio(c5)
    flow=base.base.flow_divergence(sym); fs=f(flow.get("flow_divergence_score"),50); buy=f((q.latest.get(sym) or {}).get("aggressive_buy_ratio"),.5); buyers=fs>=65 or (fs>=58 and buy>=.56)
    reclaim=z5[-1]["c"]>=e5>0 and (z5[-1]["c"]>z5[-2]["c"] or str(ri.get("sweep_state"))=="SWEEP_RECLAIMED")
    sweep=str(ri.get("sweep_state") or "WAIT"); depth=f(tm.get("depth")); healthy=MIN_DEPTH<=depth<=MAX_DEPTH and sr<=1.25 and sweep!="SUPPORT_LOST"
    ent,st,t1,t2,t3,rp,plan=pb_plan(p,c5,a,zl,tm,ri); technical=str(tm.get("trend_state"))=="UPTREND" and healthy and near and reclaim and buyers and plan=="CONDITIONAL_RECLAIM"
    strict=technical and g["layers"]==6 and g["hard_pass"]; mem=pb_mem[sym]; mem["streak"]=mem["streak"]+1 if strict else 0
    if sweep=="SUPPORT_LOST": state="SUPPORT_LOST"
    elif str(tm.get("trend_state"))!="UPTREND": state="NO_TREND"
    elif depth<MIN_DEPTH: state="TREND_STRONG"
    elif depth>MAX_DEPTH: state="PULLBACK_DEEP"
    elif sweep=="LIQUIDITY_SWEEP_RISK" and near: state="LIQUIDITY_SWEEP"
    elif strict and mem["streak"]>=ARM_STREAK: state="PULLBACK_ARMED"
    elif technical: state="RECLAIM_PENDING"
    elif sweep=="SWEEP_RECLAIMED" and near and not buyers: state="RECLAIM_PENDING"
    elif near and healthy: state="PULLBACK_ZONE"
    else: state="PULLBACK_STARTING"
    if state=="PULLBACK_ARMED": mem["armed_once"]=True
    if state=="PULLBACK_ARMED" and g["formal"]=="BUY NOW": state="PULLBACK_BUY"
    score=f(tm.get("trend_score"))*.45+max(0,20-zd*18)+clamp((1.25-sr)*12,-8,8)+clamp((fs-50)*.2,-8,10)+(8 if sweep=="SWEEP_RECLAIMED" else 0)+(8 if first else 0)+(5 if g["layers"]==6 else 0)+(4 if g["hard_pass"] else 0)
    return {"state":state,"score":round(clamp(score,0,100),1),"depth":depth,"zone":zn,"zone_level":zl,"zone_dist":zd,"sell_ratio":round(sr,3),"flow":round(fs,1),"sweep":sweep,"first":first,"phase5":phase,"layers":g["layers"],"hard_pass":g["hard_pass"],"formal":g["formal"],"streak":mem["streak"],"entry":ent,"stop":st,"tp1":t1,"tp2":t2,"tp3":t3,"risk":rp,"plan":plan}

async def fetch_pb(sym):
    tm=trend_cache.get(sym) or {}
    if time.time()-f(tm.get("updated"))>TREND_AGE or str(tm.get("trend_state")) not in {"UPTREND","TREND-WEAK"}:return
    try:
        r5=await app.load_klines(app.session,sym,"5m",84)
        r15=await app.load_klines(app.session,sym,"15m",84)
        x=pb_eval(sym,tm,candles(r5),candles(r15)); x.update(symbol=sym,updated=time.time(),trend_state=tm.get("trend_state"),trend_score=tm.get("trend_score")); pb_cache[sym]=x
    except asyncio.CancelledError: raise
    except Exception: stats["errors"]+=1

def pb_symbols():
    now=time.time(); p=hot(10); scored=[]
    for s,t in trend_cache.items():
        if now-f(t.get("updated"))>TREND_AGE or str(t.get("trend_state")) not in {"UPTREND","TREND-WEAK"}:continue
        sc=f(t.get("trend_score"))+(25 if MIN_DEPTH<=f(t.get("depth"))<=MAX_DEPTH else 0)+(20 if s in p else 0)
        if str((pb_cache.get(s) or {}).get("state")) in {"PULLBACK_ZONE","LIQUIDITY_SWEEP","RECLAIM_PENDING","PULLBACK_ARMED","PULLBACK_BUY"}:sc+=30
        scored.append((sc,s))
    out=[]
    for s in p:
        if s in trend_cache and s not in out:out.append(s)
    for _,s in sorted(scored,reverse=True):
        if s not in out:out.append(s)
        if len(out)>=PB_MAX:break
    return out[:PB_MAX]

async def pb_loop():
    sem=asyncio.Semaphore(2)
    async def one(s):
        async with sem: await fetch_pb(s)
    while True:
        await asyncio.sleep(PB_EVERY)
        try: await asyncio.gather(*(one(s) for s in pb_symbols())); stats["pb"]+=1
        except asyncio.CancelledError: raise
        except Exception: stats["errors"]+=1

def pb_intel(sym):
    x=dict(pb_cache.get(sym) or {})
    return x if x and time.time()-f(x.get("updated"))<=PB_AGE else {"state":"WAIT","score":0.0}

def diag1192():
    out=[]
    for raw in _old_diag():
        r=dict(raw); s=str(r.get("symbol") or ""); pb=pb_intel(s)
        r.update({"pullback_state":pb.get("state"),"pullback_score":pb.get("score"),"pullback_entry":pb.get("entry"),"pullback_stop":pb.get("stop"),"pullback_tp1":pb.get("tp1"),"pullback_tp2":pb.get("tp2"),"pullback_tp3":pb.get("tp3")}); out.append(r)
    return out

def feat1192(sym):
    x=dict(_old_feat(sym)) if _old_feat else {}; p=pb_intel(sym)
    x.update({"pullback_state1192":p.get("state"),"pullback_score1192":f(p.get("score")),"pullback_depth1192":f(p.get("depth"),99),"pullback_first1192":bool(p.get("first")),"pullback_sweep1192":p.get("sweep")}); return x

def keys1192(s):
    k=list(_old_keys(s)) if _old_keys else []; k += ["PULLBACK|"+str(s.get("pullback_state1192") or "WAIT"),"PULLBACK_FIRST|"+("YES" if s.get("pullback_first1192") else "NO"),"PULLBACK_SWEEP|"+str(s.get("pullback_sweep1192") or "WAIT")]; return k

async def print_loop():
    rank={"PULLBACK_BUY":8,"PULLBACK_ARMED":7,"RECLAIM_PENDING":6,"LIQUIDITY_SWEEP":5,"PULLBACK_ZONE":4,"PULLBACK_STARTING":3,"TREND_STRONG":2,"PULLBACK_DEEP":1,"SUPPORT_LOST":-1}
    while True:
        await asyncio.sleep(float(getattr(base.base,"PRINT_SECONDS",30)))
        try:
            fresh=[(s,x) for s,x in pb_cache.items() if time.time()-f(x.get("updated"))<=PB_AGE]; w=sum(str(x.get("state")) in {"PULLBACK_STARTING","PULLBACK_ZONE","LIQUIDITY_SWEEP","RECLAIM_PENDING"} for _,x in fresh); a=sum(str(x.get("state"))=="PULLBACK_ARMED" for _,x in fresh); b=sum(str(x.get("state"))=="PULLBACK_BUY" for _,x in fresh)
            print(f"Ψ-V10.19.3 PULLBACK BOARD tracked={len(fresh)} watch={w} armed={a} buy={b} trendCycles={stats['trend']} pbCycles={stats['pb']} errors={stats['errors']}",flush=True)
            for i,(s,x) in enumerate(sorted(fresh,key=lambda z:(rank.get(str(z[1].get('state')),0),f(z[1].get('score')),bool(z[1].get('first'))),reverse=True)[:10],1):
                print(f"PB{i:02d}. {s:<14} state={x.get('state','WAIT'):<18} score={f(x.get('score')):5.1f} trend={f(x.get('trend_score')):5.1f} depth={f(x.get('depth')):+.3f}% zone={x.get('zone','NONE')}@{px(x.get('zone_level'))} zDist={f(x.get('zone_dist'),99):.3f}% sweep={x.get('sweep','WAIT')} flow={f(x.get('flow')):4.1f} layers={int(x.get('layers') or 0)}/6 hard={'PASS' if x.get('hard_pass') else 'BLOCK'} first={'YES' if x.get('first') else 'NO'} streak={int(x.get('streak') or 0)} entry={px(x.get('entry'))} stop={px(x.get('stop'))} tp1={px(x.get('tp1'))} tp2={px(x.get('tp2'))} tp3={px(x.get('tp3'))}",flush=True)
        except asyncio.CancelledError: raise
        except Exception as e: print(f"Ψ-V10.19.3 PULLBACK_ERROR {type(e).__name__}: {e}",flush=True)

b17.diag_pool=diag1192
if _learn and _old_feat:_learn.feature_snapshot=feat1192
if _learn and _old_keys:_learn.pattern_keys=keys1192
async def main1192(): await asyncio.gather(_old_main(),trend_loop(),pb_loop(),print_loop())
scanner.v7.main=main1192; scanner.VERSION=VERSION
print("Ψ-V10.19.3 UPGRADE ACTIVE — separate trend-following pullback monitor: rotating 1H/4H EMA50/EMA200 trend confirmation, first-pullback priority, EMA/VWAP/support zones, stop-liquidity sweep/reclaim, sell-pressure contraction, CVD/OFI buyer-return, 2-cycle PULLBACK-ARMED persistence, conditional Entry/Stop/TP; formal BUY gates unchanged",flush=True)
if __name__=="__main__":
    try: print("Ψ-V10.19.3 ACTIVE — trend pullback monitor",flush=True); asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        try: base.base.save_v119_state()
        except Exception: pass
