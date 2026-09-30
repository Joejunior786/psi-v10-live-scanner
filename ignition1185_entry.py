import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

import ignition1184_entry as base

scanner=base.scanner
b18=base.b18
b17=base.b17
b16=base.b16
q=scanner.q
app=scanner.app

VERSION="10.18.5-adaptive-pattern-learning"

LEARN_SAMPLE_SECONDS=5.0
LEARN_ALERT_COOLDOWN=600.0
LEARN_HORIZON_1H=3600.0
LEARN_HORIZON_4H=14400.0
LEARN_MAX_PENDING=1000
LEARN_MAX_RESOLVED=5000
LEARN_MIN_PATTERN_SAMPLES=8
LEARN_MIN_GLOBAL_SAMPLES=20
LEARN_PRIOR_STRENGTH=8.0
LEARN_MAX_OPP_ADJ=8.0
LEARN_MAX_PUMP_ADJ=6.0
LEARN_SAVE_SECONDS=60.0
LEARN_STATE_PATH=os.environ.get("PSI_LEARN_STATE_PATH","/app/psi_v10185_learning.json")

learn_pending=[]
learn_resolved=deque(maxlen=LEARN_MAX_RESOLVED)
learn_last=defaultdict(float)
learn_seq=0
learn_stats={"alerts":0,"stage1h":0,"stage4h":0,"loaded":False,"save_errors":0,"sample_errors":0}
pattern_stats={"1h":{},"4h":{}}
last_save=0.0

_old_diag=b17.diag_pool
_old_opp=b17.opp_score
_old_pump=b18.pump_signature
_old_hot=q.hot
_old_main=scanner.v7.main

def f(v,d=0.0):
    try:
        x=float(v)
    except (TypeError,ValueError):
        return d
    return x if math.isfinite(x) else d

def clamp(v,lo,hi):
    return max(lo,min(hi,v))

def current_price(sym):
    return f(app.current_symbol_price(sym),f((q.latest.get(sym) or {}).get("price")))

def rs_bucket(v):
    x=f(v,50.0)
    if x<45: return "RS<45"
    if x<55: return "RS45-54"
    if x<65: return "RS55-64"
    if x<75: return "RS65-74"
    return "RS75+"

def pump_bucket(v):
    x=f(v)
    if x<52: return "P<52"
    if x<70: return "P52-69"
    if x<85: return "P70-84"
    return "P85+"

def rapid_bucket(v):
    x=f(v)
    if x<120: return "R<120"
    if x<140: return "R120-139"
    if x<160: return "R140-159"
    return "R160+"

def life_bucket(ph):
    p=str(ph or "UNKNOWN")
    if p in ("BREAKOUT_IN_PROGRESS","BREAKOUT_HOLD"): return "BREAKOUT"
    if p in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE"): return "FAIL"
    if p in ("APPROACH","BELOW_RESISTANCE"): return "PREBREAK"
    if p=="RETEST": return "RETEST"
    if p=="CONTINUATION": return "CONTINUATION"
    return p

def feature_snapshot(sym):
    ps=dict(_old_pump(sym))
    inf=base.btc_influence(sym)
    lf=base.base.life(sym)
    row=q.latest.get(sym) or {}
    price=current_price(sym)
    return {
        "symbol":sym,
        "price":price,
        "relationship":str(inf.get("btc_relationship") or "BTC-CONTEXT-WAIT"),
        "rs_score":f(inf.get("btc_rs_score"),50.0),
        "btc_state":str(inf.get("btc_market_state") or "UNKNOWN"),
        "btc_excess_3m":f(inf.get("btc_excess_3m_pct")),
        "btc_rel_accel":f(inf.get("btc_relative_acceleration")),
        "btc_lead_lag":str(inf.get("btc_lead_lag") or "UNKNOWN"),
        "pump_score":f(ps.get("pump_signature_score")),
        "pump_state":str(ps.get("pump_state") or "NONE"),
        "rapid_score":f(ps.get("rapid_score")),
        "rapid_peak":f(ps.get("rapid_peak_5m")),
        "life":str(ps.get("breakout_lifecycle") or lf.get("phase") or "UNKNOWN"),
        "layers":int(f(ps.get("layers_118"),b16._layers(row) if row else 0)),
        "micro":bool(ps.get("micro_ready_118")),
        "exec":bool(ps.get("exec_pass_118")),
    }

def eligible_feature(x):
    if f(x.get("price"))<=0:
        return False
    if life_bucket(x.get("life"))=="FAIL":
        return False
    rel=str(x.get("relationship") or "")
    leader=rel in ("BTC-DIVERGENT-LEADER","RELATIVE-STRENGTH-LEADER")
    return (
        f(x.get("pump_score"))>=48.0 or
        f(x.get("rapid_score"))>=120.0 or
        (leader and f(x.get("rs_score"))>=58.0) or
        (f(x.get("rs_score"))>=65.0 and int(x.get("layers") or 0)>=4)
    )

def pattern_keys(x):
    rel=str(x.get("relationship") or "UNKNOWN")
    rb=rs_bucket(x.get("rs_score"))
    pb=pump_bucket(x.get("pump_score"))
    rap=rapid_bucket(x.get("rapid_score"))
    lb=life_bucket(x.get("life"))
    btc=str(x.get("btc_state") or "UNKNOWN")
    return [
        "REL|"+rel,
        "RS|"+rb,
        "PUMP|"+pb,
        "RAPID|"+rap,
        "BTC|"+btc,
        "REL_PUMP|"+rel+"|"+pb,
        "REL_LIFE|"+rel+"|"+lb,
        "REL_RS|"+rel+"|"+rb,
        "COMBO|"+rel+"|"+rb+"|"+pb+"|"+lb,
    ]

def empty_stat():
    return {"n":0,"hit5":0,"hit10":0,"hit20":0,"false":0,"sum_max":0.0,"sum_dd":0.0}

def stat_for(horizon,key):
    d=pattern_stats[horizon]
    if key not in d:
        d[key]=empty_stat()
    return d[key]

def update_stat(horizon,key,maxret,maxdd):
    s=stat_for(horizon,key)
    s["n"]+=1
    s["hit5"]+=int(maxret>=5.0)
    s["hit10"]+=int(maxret>=10.0)
    s["hit20"]+=int(maxret>=20.0)
    s["false"]+=int(maxret<2.0)
    s["sum_max"]+=maxret
    s["sum_dd"]+=maxdd

def commit_stage(e,horizon):
    stage_key="committed_"+horizon
    if e.get(stage_key):
        return
    maxret=f(e.get("max_return_pct"))
    maxdd=f(e.get("max_drawdown_pct"))
    update_stat(horizon,"GLOBAL",maxret,maxdd)
    for key in e.get("pattern_keys") or []:
        update_stat(horizon,key,maxret,maxdd)
    e[stage_key]=True
    if horizon=="1h":
        learn_stats["stage1h"]+=1
    else:
        learn_stats["stage4h"]+=1

def global_rate(horizon,field):
    g=stat_for(horizon,"GLOBAL")
    n=max(0,int(g.get("n",0)))
    if n<=0:
        defaults={"hit5":0.20,"hit10":0.08,"hit20":0.03,"false":0.45}
        return defaults[field]
    return f(g.get(field))/n

def posterior_rate(horizon,s,field):
    n=max(0,int(s.get("n",0)))
    prior=global_rate(horizon,field)
    return (f(s.get(field))+LEARN_PRIOR_STRENGTH*prior)/(n+LEARN_PRIOR_STRENGTH)

def key_quality(horizon,key):
    s=pattern_stats[horizon].get(key)
    if not s:
        return None
    n=int(s.get("n",0))
    if n<LEARN_MIN_PATTERN_SAMPLES:
        return None
    g=stat_for(horizon,"GLOBAL")
    if int(g.get("n",0))<LEARN_MIN_GLOBAL_SAMPLES:
        return None
    p5=posterior_rate(horizon,s,"hit5"); g5=global_rate(horizon,"hit5")
    p10=posterior_rate(horizon,s,"hit10"); g10=global_rate(horizon,"hit10")
    p20=posterior_rate(horizon,s,"hit20"); g20=global_rate(horizon,"hit20")
    pf=posterior_rate(horizon,s,"false"); gf=global_rate(horizon,"false")
    delta=.45*(p5-g5)+.35*(p10-g10)+.20*(p20-g20)-.25*(pf-gf)
    score=clamp(50.0+140.0*delta,0.0,100.0)
    conf=clamp(n/30.0,0.0,1.0)
    return {"score":score,"confidence":conf,"samples":n,"key":key,
            "p5":p5,"p10":p10,"p20":p20,"false":pf}

def learning_overlay_from_feature(x):
    evid=[]
    keys=pattern_keys(x)
    for horizon,hweight in (("1h",1.0),("4h",1.25)):
        for key in keys:
            qv=key_quality(horizon,key)
            if qv:
                weight=hweight*qv["confidence"]*(1.15 if key.startswith("COMBO|") else 1.0)
                evid.append((weight,qv,horizon))
    if not evid:
        return {"learning_status":"WARMING","learning_score":50.0,"learning_confidence":0.0,
                "learning_samples":0,"learning_adjustment":0.0,"learning_best_pattern":None}
    total=sum(w for w,_,__ in evid)
    score=sum(w*qv["score"] for w,qv,_ in evid)/max(total,1e-9)
    conf=clamp(total/max(1.0,len(evid))*0.85,0.0,1.0)
    samples=max(qv["samples"] for _,qv,__ in evid)
    best=max(evid,key=lambda z:(z[1]["score"]-50.0)*z[1]["confidence"])
    adj=clamp((score-50.0)/6.25,-LEARN_MAX_OPP_ADJ,LEARN_MAX_OPP_ADJ)*conf
    return {"learning_status":"ACTIVE","learning_score":round(score,1),
            "learning_confidence":round(conf,3),"learning_samples":samples,
            "learning_adjustment":round(adj,2),
            "learning_best_pattern":best[1]["key"]+"@"+best[2]}

def learning_overlay(sym):
    try:
        return learning_overlay_from_feature(feature_snapshot(sym))
    except Exception:
        return {"learning_status":"ERROR","learning_score":50.0,"learning_confidence":0.0,
                "learning_samples":0,"learning_adjustment":0.0,"learning_best_pattern":None}

def record_alerts():
    global learn_seq
    now=time.time()
    syms=set()
    try:
        syms.update(base.base.candidate_symbols())
    except Exception:
        pass
    syms.update(x for x in app.selected_micro_symbols if b17.directional(x))
    try:
        syms.update(sym for _,sym in _old_hot(40) if b17.directional(sym))
    except Exception:
        pass
    try:
        ranked=sorted(((f(ps.get("pump_signature_score")),sym) for sym,ps in b18.pump_cache.items()
                       if b17.directional(sym)),reverse=True)[:40]
        syms.update(sym for _,sym in ranked)
    except Exception:
        pass

    for sym in syms:
        if now-learn_last[sym]<LEARN_ALERT_COOLDOWN:
            continue
        try:
            x=feature_snapshot(sym)
        except Exception:
            continue
        if not eligible_feature(x):
            continue
        learn_last[sym]=now
        learn_seq+=1
        e={
            "id":learn_seq,"symbol":sym,"alert_ts":now,"alert_price":x["price"],
            "features":x,"pattern_keys":pattern_keys(x),
            "max_return_pct":0.0,"max_drawdown_pct":0.0,
            "returns":{},"committed_1h":False,"committed_4h":False,
        }
        learn_pending.append(e)
        learn_stats["alerts"]+=1
    if len(learn_pending)>LEARN_MAX_PENDING:
        del learn_pending[:-LEARN_MAX_PENDING]

def update_learning_outcomes():
    now=time.time()
    keep=[]
    for e in learn_pending:
        p=current_price(e["symbol"]); basep=f(e.get("alert_price"))
        if p<=0 or basep<=0:
            keep.append(e); continue
        ret=(p/basep-1.0)*100.0
        e["max_return_pct"]=max(f(e.get("max_return_pct")),ret)
        e["max_drawdown_pct"]=min(f(e.get("max_drawdown_pct")),ret)
        age=now-f(e.get("alert_ts"))
        for label,secs in (("5m",300),("15m",900),("1h",3600),("2h",7200),("4h",14400)):
            if age>=secs and label not in e["returns"]:
                e["returns"][label]=round(ret,4)
        if age>=LEARN_HORIZON_1H and not e.get("committed_1h"):
            commit_stage(e,"1h")
        if age>=LEARN_HORIZON_4H:
            if not e.get("committed_4h"):
                commit_stage(e,"4h")
            learn_resolved.append(dict(e))
        else:
            keep.append(e)
    learn_pending[:]=keep[-LEARN_MAX_PENDING:]

def serializable_state():
    return {
        "version":VERSION,
        "saved_at":time.time(),
        "learn_seq":learn_seq,
        "stats":learn_stats,
        "pattern_stats":pattern_stats,
        "resolved_tail":list(learn_resolved)[-200:],
    }

def save_state():
    global last_save
    now=time.time()
    if now-last_save<LEARN_SAVE_SECONDS:
        return
    last_save=now
    tmp=LEARN_STATE_PATH+".tmp"
    try:
        with open(tmp,"w",encoding="utf-8") as fh:
            json.dump(serializable_state(),fh,separators=(",",":"))
        os.replace(tmp,LEARN_STATE_PATH)
    except Exception:
        learn_stats["save_errors"]+=1

def load_state():
    global learn_seq
    try:
        with open(LEARN_STATE_PATH,"r",encoding="utf-8") as fh:
            obj=json.load(fh)
        ps=obj.get("pattern_stats") or {}
        for horizon in ("1h","4h"):
            src=ps.get(horizon) or {}
            for key,val in src.items():
                if isinstance(val,dict):
                    pattern_stats[horizon][key]=dict(empty_stat(),**val)
        learn_seq=max(learn_seq,int(obj.get("learn_seq") or 0))
        for e in obj.get("resolved_tail") or []:
            if isinstance(e,dict):
                learn_resolved.append(e)
        learn_stats["loaded"]=True
    except FileNotFoundError:
        pass
    except Exception:
        learn_stats["save_errors"]+=1

def diag1185():
    out=[]
    for raw in _old_diag():
        row=dict(raw)
        sym=str(row.get("symbol") or "")
        if sym:
            row.update(learning_overlay(sym))
        out.append(row)
    return out

def opp1185(row):
    score=f(_old_opp(row))
    sym=str(row.get("symbol") or "")
    ov={
        "learning_adjustment":row.get("learning_adjustment"),
        "learning_confidence":row.get("learning_confidence"),
    }
    if ov["learning_adjustment"] is None and sym:
        ov=learning_overlay(sym)
    return round(score+f(ov.get("learning_adjustment")),2)

def pump_signature_1185(sym):
    ps=dict(_old_pump(sym))
    ov=learning_overlay_from_feature({
        "symbol":sym,
        "price":current_price(sym),
        "relationship":ps.get("btc_relationship"),
        "rs_score":ps.get("btc_rs_score"),
        "btc_state":ps.get("btc_market_state"),
        "btc_excess_3m":ps.get("btc_excess_3m_pct"),
        "btc_rel_accel":ps.get("btc_relative_acceleration"),
        "btc_lead_lag":ps.get("btc_lead_lag"),
        "pump_score":ps.get("pump_signature_score"),
        "pump_state":ps.get("pump_state"),
        "rapid_score":ps.get("rapid_score"),
        "rapid_peak":ps.get("rapid_peak_5m"),
        "life":ps.get("breakout_lifecycle"),
        "layers":ps.get("layers_118"),
        "micro":ps.get("micro_ready_118"),
        "exec":ps.get("exec_pass_118"),
    })
    ps.update(ov)
    adj=clamp(f(ov.get("learning_adjustment")),-LEARN_MAX_PUMP_ADJ,LEARN_MAX_PUMP_ADJ)
    score=round(clamp(f(ps.get("pump_signature_score"))+adj,0.0,100.0),1)
    ps["pump_signature_score"]=score

    ph=str(ps.get("breakout_lifecycle") or "UNKNOWN")
    micro=bool(ps.get("micro_ready_118"))
    execp=bool(ps.get("exec_pass_118"))
    lifecycle_ok=ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE")
    risk=bool(ps.get("btc_risk_overlay"))
    if score>=b18.PUMP_ARMED_SCORE and micro and execp and lifecycle_ok and not risk:
        ps["pump_state"]="PUMP-ARMED"
    elif score>=b18.PUMP_WATCH_SCORE and lifecycle_ok:
        ps["pump_state"]="PUMP-WATCH"
    else:
        ps["pump_state"]="NONE"
    return ps

def hot1185(limit=None):
    n=max(int(limit or getattr(q,"HOT_COUNT",80)),80)
    raw=list(_old_hot(max(n*2,160)))
    scored=[]
    seen=set()
    for score,sym in raw:
        if sym in seen or not b17.directional(sym):
            continue
        seen.add(sym)
        ov=learning_overlay(sym)
        bonus=clamp(f(ov.get("learning_adjustment"))*1.5,-8.0,12.0)
        scored.append((f(score)+bonus,sym))
    scored.sort(reverse=True)
    return scored[:(limit or getattr(q,"HOT_COUNT",80))]

def pattern_leaderboard():
    rows=[]
    for horizon in ("1h","4h"):
        for key in list(pattern_stats[horizon]):
            if key=="GLOBAL":
                continue
            qv=key_quality(horizon,key)
            if qv:
                rows.append((qv["score"]*qv["confidence"],horizon,qv))
    rows.sort(reverse=True,key=lambda x:x[0])
    return rows

async def learning_sampler():
    while True:
        await asyncio.sleep(LEARN_SAMPLE_SECONDS)
        try:
            record_alerts()
            update_learning_outcomes()
            save_state()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            learn_stats["sample_errors"]+=1
            print(f"Ψ-V10.18.5 LEARN_ERROR {type(e).__name__}: {e}",flush=True)

async def learning_print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            g1=stat_for("1h","GLOBAL"); g4=stat_for("4h","GLOBAL")
            status="ACTIVE" if int(g1.get("n",0))>=LEARN_MIN_GLOBAL_SAMPLES else "WARMING"
            print(
                "Ψ-V10.18.5 LEARNING "
                f"status={status} pending={len(learn_pending)} resolved={len(learn_resolved)} "
                f"alerts={learn_stats['alerts']} global1h={int(g1.get('n',0))} global4h={int(g4.get('n',0))} "
                f"hit5_1h={global_rate('1h','hit5'):.3f} hit10_1h={global_rate('1h','hit10'):.3f} "
                f"hit20_1h={global_rate('1h','hit20'):.3f} false_1h={global_rate('1h','false'):.3f} "
                f"loaded={'YES' if learn_stats['loaded'] else 'NO'} errors={learn_stats['sample_errors']}",
                flush=True
            )
            for i,(_,h,qv) in enumerate(pattern_leaderboard()[:8],1):
                print(
                    f"A{i:02d}. horizon={h} pattern={qv['key']} learn={qv['score']:.1f} "
                    f"conf={qv['confidence']:.2f} n={qv['samples']} "
                    f"p5={qv['p5']:.3f} p10={qv['p10']:.3f} p20={qv['p20']:.3f} false={qv['false']:.3f}",
                    flush=True
                )
            current=[]
            for sym in set(base.base.candidate_symbols()):
                ov=learning_overlay(sym)
                if f(ov.get("learning_confidence"))>0:
                    current.append((f(ov.get("learning_score")),sym,ov))
            current.sort(reverse=True)
            for i,(score,sym,ov) in enumerate(current[:6],1):
                print(
                    f"AC{i:02d}. {sym:14s} learn={score:5.1f} conf={f(ov.get('learning_confidence')):.2f} "
                    f"adj={f(ov.get('learning_adjustment')):+.2f} n={int(ov.get('learning_samples') or 0)} "
                    f"best={ov.get('learning_best_pattern') or '-'}",
                    flush=True
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.18.5 LEARN_PRINT_ERROR {type(e).__name__}: {e}",flush=True)

load_state()

b17.diag_pool=diag1185
b17.opp_score=opp1185
b18.pump_signature=pump_signature_1185
b17.hot=hot1185
q.hot=hot1185

async def main1185():
    await asyncio.gather(_old_main(),learning_sampler(),learning_print_loop())

scanner.v7.main=main1185
scanner.VERSION=VERSION

print(
    "Ψ-V10.18.5 ADAPTIVE LEARNING ACTIVE — forward feature snapshots learn BTC relationship + RS + pump + RAPID + lifecycle "
    "patterns against +5/+10/+20% outcomes and false positives at 1h/4h; Bayesian shrinkage, minimum-sample confidence, "
    "conservative learned ranking/pump adjustments only; formal PRE/BUY gates unchanged",
    flush=True
)

if __name__=="__main__":
    try:
        print("Ψ-V10.18.5 ACTIVE — evidence-driven pump-pattern learning",flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        save_state()
        print("Ψ-V10.18.5 stopped",flush=True)
