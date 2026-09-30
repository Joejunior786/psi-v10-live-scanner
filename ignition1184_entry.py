import asyncio
import math
import statistics
import time
from collections import defaultdict, deque

import ignition1183_entry as base

scanner=base.scanner
b18=base.b18
b17=base.b17
b16=base.b16
q=scanner.q
app=scanner.app

VERSION="10.18.4-btc-influence-relative-strength"

BTC_SAMPLE_SECONDS=10.0
PAIR_HISTORY_MAX=36
PAIR_MIN_SAMPLES=8
BTC_RISK_1M_PCT=-0.30
BTC_RISK_3M_PCT=-0.45
BTC_RISK_5M_PCT=-0.80

btc_context={}
btc_history=deque(maxlen=PAIR_HISTORY_MAX)
pair_history=defaultdict(lambda: deque(maxlen=PAIR_HISTORY_MAX))
pair_last_life_update={}

_old_diag=b17.diag_pool
_old_opp=b17.opp_score
_old_pump=b18.pump_signature
_old_main=scanner.v7.main

def f(v,d=0.0):
    try:
        x=float(v)
    except (TypeError,ValueError):
        return d
    return x if math.isfinite(x) else d

def pct(a,b):
    return ((a/b)-1.0)*100.0 if a and b else 0.0

def clamp(v,lo,hi):
    return max(lo,min(hi,v))

def pearson(xs,ys):
    n=min(len(xs),len(ys))
    if n<PAIR_MIN_SAMPLES:
        return None
    xs=list(xs)[-n:]
    ys=list(ys)[-n:]
    mx=sum(xs)/n
    my=sum(ys)/n
    dx=[x-mx for x in xs]
    dy=[y-my for y in ys]
    vx=sum(x*x for x in dx)
    vy=sum(y*y for y in dy)
    if vx<=1e-12 or vy<=1e-12:
        return None
    return sum(a*b for a,b in zip(dx,dy))/math.sqrt(vx*vy)

def beta_from_pairs(pairs):
    if len(pairs)<PAIR_MIN_SAMPLES:
        return 1.0,None
    alt=[f(x[1]) for x in pairs]
    btc=[f(x[2]) for x in pairs]
    corr=pearson(alt,btc)
    if corr is None:
        return 1.0,None
    try:
        sa=statistics.pstdev(alt)
        sb=statistics.pstdev(btc)
    except statistics.StatisticsError:
        return 1.0,corr
    if sb<=1e-9:
        return 1.0,corr
    beta=clamp(corr*(sa/sb),0.0,3.0)
    return beta,corr

def btc_state_from_returns(r1,r3,r5,accel):
    if (r1<=BTC_RISK_1M_PCT and r3<=BTC_RISK_3M_PCT) or r5<=BTC_RISK_5M_PCT:
        return "RISK_OFF"
    if r3>=0.18 and r5>=0.25 and accel>=-0.05:
        return "BULLISH"
    if r3<0 or accel<=-0.08:
        return "WEAKENING"
    return "NEUTRAL"

def btc_snapshot_from_rows(rows):
    if not rows or len(rows)<17:
        return None
    closes=[f(r[4]) for r in rows if r and len(r)>4]
    if len(closes)<17 or closes[-1]<=0:
        return None
    price=closes[-1]
    r1=pct(price,closes[-2])
    r3=pct(price,closes[-4])
    r5=pct(price,closes[-6])
    r15=pct(price,closes[-16])
    accel=r1-(r3/3.0)
    state=btc_state_from_returns(r1,r3,r5,accel)
    return {
        "updated":time.time(),"price":price,
        "return_1m_pct":round(r1,4),"return_3m_pct":round(r3,4),
        "return_5m_pct":round(r5,4),"return_15m_pct":round(r15,4),
        "acceleration_proxy":round(accel,4),"state":state,
    }

async def refresh_btc():
    if app.session is None:
        return
    rows=await app.load_klines(app.session,"BTCUSDT","1m",25)
    snap=btc_snapshot_from_rows(rows)
    if snap:
        btc_context.clear()
        btc_context.update(snap)
        btc_history.append((snap["updated"],snap["return_1m_pct"],snap["return_3m_pct"],snap["return_5m_pct"]))

def alt_returns(sym):
    lf=base.life(sym)
    r1=f(lf.get("return_1m_pct"))
    r3=f(lf.get("return_3m_pct"))
    an=app.anomaly_state.get(sym) or {}
    r5=f(an.get("return_5m_pct"),r3)
    return r1,r3,r5,lf

def record_pairs():
    if not btc_context:
        return
    br1=f(btc_context.get("return_1m_pct"))
    now=time.time()
    for sym in base.candidate_symbols():
        if sym=="BTCUSDT":
            continue
        r1,r3,r5,lf=alt_returns(sym)
        updated=f(lf.get("updated"))
        if not updated or now-updated>base.LIFECYCLE_STALE_SECONDS:
            continue
        if pair_last_life_update.get(sym)==updated:
            continue
        pair_last_life_update[sym]=updated
        pair_history[sym].append((updated,r1,br1))

def btc_influence(sym):
    if sym=="BTCUSDT":
        return {
            "btc_relationship":"BTC-BENCHMARK","btc_rs_score":50.0,
            "btc_beta":1.0,"btc_correlation":1.0,"btc_lead_lag":"BENCHMARK",
            "btc_expected_3m_pct":f(btc_context.get("return_3m_pct")),
            "btc_excess_3m_pct":0.0,"btc_relative_acceleration":0.0,
            "btc_market_state":str(btc_context.get("state") or "UNKNOWN"),
            "btc_risk_overlay":str(btc_context.get("state") or "")=="RISK_OFF",
        }
    if not btc_context or time.time()-f(btc_context.get("updated"))>30:
        return {
            "btc_relationship":"BTC-CONTEXT-WAIT","btc_rs_score":50.0,
            "btc_beta":1.0,"btc_correlation":None,"btc_lead_lag":"UNKNOWN",
            "btc_expected_3m_pct":0.0,"btc_excess_3m_pct":0.0,
            "btc_relative_acceleration":0.0,"btc_market_state":"UNKNOWN",
            "btc_risk_overlay":False,
        }

    ar1,ar3,ar5,lf=alt_returns(sym)
    br1=f(btc_context.get("return_1m_pct"))
    br3=f(btc_context.get("return_3m_pct"))
    br5=f(btc_context.get("return_5m_pct"))
    bacc=f(btc_context.get("acceleration_proxy"))
    beta,corr=beta_from_pairs(pair_history.get(sym,()))
    expected3=beta*br3
    excess3=ar3-expected3
    excess1=ar1-beta*br1
    aacc=ar1-(ar3/3.0)
    relacc=aacc-beta*bacc

    score=50.0
    score+=clamp(excess3*18.0,-22.0,22.0)
    score+=clamp(excess1*20.0,-14.0,14.0)
    score+=clamp(relacc*22.0,-14.0,14.0)
    if corr is not None and corr<0.35 and ar3>0:
        score+=5.0
    if corr is not None and corr>0.75 and excess3<=0.10:
        score-=7.0
    score=round(clamp(score,0.0,100.0),1)

    state=str(btc_context.get("state") or "UNKNOWN")
    risk=(state=="RISK_OFF")
    strong_divergent=ar3>=0.35 and excess3>=0.45

    if risk and not strong_divergent:
        relation="BTC-RISK"
    elif br3<=0 and strong_divergent:
        relation="BTC-DIVERGENT-LEADER"
    elif excess3>=0.45 and ar3>0:
        relation="RELATIVE-STRENGTH-LEADER"
    elif br3>=0.15 and ar3>=br3+0.12:
        relation="BTC-ASSISTED"
    elif br3>=0.15 and corr is not None and corr>=0.55 and abs(excess3)<=0.25:
        relation="BTC-LED"
    elif corr is not None and corr>=0.65 and excess3<0.15:
        relation="BTC-DEPENDENT"
    elif br3>0.10 and ar3>0:
        relation="BTC-ASSISTED"
    elif strong_divergent:
        relation="RELATIVE-STRENGTH-LEADER"
    else:
        relation="BTC-NEUTRAL"

    if excess1>=0.18:
        leadlag="ALT-LEADING"
    elif excess1<=-0.12 and br1>0:
        leadlag="BTC-LEADING"
    else:
        leadlag="CO-MOVING"

    return {
        "btc_relationship":relation,
        "btc_rs_score":score,
        "btc_beta":round(beta,3),
        "btc_correlation":None if corr is None else round(corr,3),
        "btc_lead_lag":leadlag,
        "btc_expected_3m_pct":round(expected3,4),
        "btc_excess_3m_pct":round(excess3,4),
        "btc_relative_acceleration":round(relacc,4),
        "btc_market_state":state,
        "btc_risk_overlay":risk,
        "alt_return_1m_pct_1184":round(ar1,4),
        "alt_return_3m_pct_1184":round(ar3,4),
        "alt_return_5m_pct_1184":round(ar5,4),
        "btc_return_1m_pct_1184":round(br1,4),
        "btc_return_3m_pct_1184":round(br3,4),
        "btc_return_5m_pct_1184":round(br5,4),
    }

def diag1184():
    out=[]
    for raw in _old_diag():
        row=dict(raw)
        sym=str(row.get("symbol") or "")
        row.update(btc_influence(sym))
        out.append(row)
    return out

def opp1184(row):
    score=f(_old_opp(row))
    rel=str(row.get("btc_relationship") or btc_influence(str(row.get("symbol") or "")).get("btc_relationship"))
    rs=f(row.get("btc_rs_score"),50.0)
    adj={
        "BTC-DIVERGENT-LEADER":10.0,
        "RELATIVE-STRENGTH-LEADER":8.0,
        "BTC-ASSISTED":4.0,
        "BTC-LED":1.0,
        "BTC-DEPENDENT":-4.0,
        "BTC-RISK":-10.0,
        "BTC-NEUTRAL":0.0,
        "BTC-CONTEXT-WAIT":0.0,
        "BTC-BENCHMARK":0.0,
    }.get(rel,0.0)
    adj+=clamp((rs-50.0)/10.0,-4.0,4.0)
    return round(score+adj,2)

def pump_signature_1184(sym):
    ps=dict(_old_pump(sym))
    inf=btc_influence(sym)
    ps.update(inf)
    score=f(ps.get("pump_signature_score"))
    rel=str(inf.get("btc_relationship") or "")
    score+={
        "BTC-DIVERGENT-LEADER":8.0,
        "RELATIVE-STRENGTH-LEADER":6.0,
        "BTC-ASSISTED":3.0,
        "BTC-LED":0.0,
        "BTC-DEPENDENT":-3.0,
        "BTC-RISK":-8.0,
    }.get(rel,0.0)
    score+=clamp((f(inf.get("btc_rs_score"),50.0)-50.0)/12.0,-3.0,3.0)
    score=round(clamp(score,0.0,100.0),1)
    ps["pump_signature_score"]=score

    ph=str(ps.get("breakout_lifecycle") or "UNKNOWN")
    micro=bool(ps.get("micro_ready_118"))
    execp=bool(ps.get("exec_pass_118"))
    lifecycle_ok=ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE")
    risk=bool(inf.get("btc_risk_overlay"))
    if score>=b18.PUMP_ARMED_SCORE and micro and execp and lifecycle_ok and not risk:
        state="PUMP-ARMED"
    elif score>=b18.PUMP_WATCH_SCORE and lifecycle_ok:
        state="PUMP-WATCH"
    else:
        state="NONE"
    ps["pump_state"]=state
    ps["btc_risk_suppressed_pump_armed"]=bool(risk and score>=b18.PUMP_ARMED_SCORE)
    return ps

async def btc_sampler():
    while True:
        try:
            await refresh_btc()
            record_pairs()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.18.4 BTC_CONTEXT_ERROR {type(e).__name__}: {e}",flush=True)
        await asyncio.sleep(BTC_SAMPLE_SECONDS)

async def btc_print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            c=dict(btc_context)
            print(
                "Ψ-V10.18.4 BTC_CONTEXT "
                f"state={str(c.get('state') or 'UNKNOWN')} price={f(c.get('price')):.10g} "
                f"r1={f(c.get('return_1m_pct')):+.3f}% r3={f(c.get('return_3m_pct')):+.3f}% "
                f"r5={f(c.get('return_5m_pct')):+.3f}% accel={f(c.get('acceleration_proxy')):+.3f}",
                flush=True
            )
            rows=[]
            for raw in _old_diag():
                sym=str(raw.get("symbol") or "")
                if not sym or sym=="BTCUSDT":
                    continue
                inf=btc_influence(sym)
                rows.append((f(inf.get("btc_rs_score"),50.0),sym,inf))
            rows.sort(reverse=True)
            for i,(_,sym,inf) in enumerate(rows[:10],1):
                corr=inf.get("btc_correlation")
                corr_text="-" if corr is None else f"{f(corr):+.2f}"
                print(
                    f"R{i:02d}. {sym:14s} btcRel={str(inf.get('btc_relationship') or '-'):26s} "
                    f"RS={f(inf.get('btc_rs_score')):5.1f} alt3={f(inf.get('alt_return_3m_pct_1184')):+.3f}% "
                    f"btc3={f(inf.get('btc_return_3m_pct_1184')):+.3f}% excess3={f(inf.get('btc_excess_3m_pct')):+.3f}% "
                    f"beta={f(inf.get('btc_beta'),1.0):.2f} corr={corr_text} lead={str(inf.get('btc_lead_lag') or '-')}",
                    flush=True
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.18.4 BTC_PRINT_ERROR {type(e).__name__}: {e}",flush=True)

b17.diag_pool=diag1184
b17.opp_score=opp1184
b18.pump_signature=pump_signature_1184

async def main1184():
    await asyncio.gather(_old_main(),btc_sampler(),btc_print_loop())

scanner.v7.main=main1184
scanner.VERSION=VERSION

print(
    "Ψ-V10.18.4 BTC INFLUENCE UPGRADE ACTIVE — BTC 1m/3m/5m context, rolling beta/correlation, "
    "relative-strength acceleration, BTC lead/lag labels, BTC-led/assisted/divergent/dependent/risk classification; "
    "relative-strength feeds ranking + pump hot-path, BTC risk suppresses PUMP-ARMED only; formal PRE/BUY rules unchanged",
    flush=True
)

if __name__=="__main__":
    try:
        print("Ψ-V10.18.4 ACTIVE — BTC influence + relative-strength overlay",flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.18.4 stopped",flush=True)
