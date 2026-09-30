import asyncio
import math
import time
from collections import defaultdict, deque

import ignition1182_entry as base

scanner=base.scanner
b18=base.b18
b17=b18.b17
b16=b17.b16
q=scanner.q
app=scanner.app

VERSION="10.18.3-breakout-lifecycle"

LIFECYCLE_SAMPLE_SECONDS=20.0
LIFECYCLE_MAX_SYMBOLS=28
LIFECYCLE_STALE_SECONDS=50.0
LEVEL_MATCH_PCT=0.45
BREAK_EPS_PCT=0.08
HOLD_PCT=0.18
FAIL_PCT=0.25
RETEST_BAND_PCT=0.40
CONTINUATION_PCT=0.65
NO_CHASE_PCT=2.00
PEAK_REJECTION_PCT=0.45
USED_LEVEL_TTL=30*60

lifecycle={}
lifecycle_history=defaultdict(lambda: deque(maxlen=80))
lifecycle_stats={"samples":0,"symbols":0,"errors":0}

_old_diag=b17.diag_pool
_old_opp=b17.opp_score
_old_breakout_strict=b16._breakout_strict
_old_pre_strict=b16._pre_strict
_old_buy_strict=b16._buy_strict
_old_pump_signature=b18.pump_signature
_old_main=scanner.v7.main


def f(v,d=0.0):
    try:
        x=float(v)
    except (TypeError,ValueError):
        return d
    return x if math.isfinite(x) else d


def pct(a,b):
    return ((a/b)-1.0)*100.0 if a and b else 0.0


def candle(row):
    if not row or len(row)<7:
        return None
    return {
        "open_ms":int(row[0]),"open":f(row[1]),"high":f(row[2]),"low":f(row[3]),
        "close":f(row[4]),"close_ms":int(row[6])
    }


def resistance_before_recent(closed, holdout=2, lookback=6):
    if len(closed)<holdout+2:
        return 0.0
    end=max(1,len(closed)-holdout)
    start=max(0,end-lookback)
    vals=[x["high"] for x in closed[start:end] if x and x["high"]>0]
    return max(vals) if vals else 0.0


def recent_breakout_bootstrap(closed5, live5, level):
    recent=[x for x in closed5[-2:] if x]
    if live5:
        recent.append(live5)
    if not recent or level<=0:
        return False,0.0,0.0
    peak=max(x["high"] for x in recent)
    first_ms=0
    for x in recent:
        if x["high"]>=level*(1+BREAK_EPS_PCT/100.0):
            first_ms=x["open_ms"]
            break
    return bool(first_ms), peak, first_ms/1000.0 if first_ms else 0.0


def same_level(a,b):
    return a>0 and b>0 and abs(pct(a,b))<=LEVEL_MATCH_PCT


def infer_lifecycle(sym, rows1, rows5, now=None):
    now=now or time.time()
    c1=[candle(x) for x in (rows1 or [])]
    c5=[candle(x) for x in (rows5 or [])]
    c1=[x for x in c1 if x]
    c5=[x for x in c5 if x]
    if len(c1)<5 or len(c5)<5:
        return {"symbol":sym,"phase":"UNKNOWN","updated":now,"fresh_breakout_eligible":False}

    closed1,live1=c1[:-1],c1[-1]
    closed5,live5=c5[:-1],c5[-1]
    price=live1["close"] or live5["close"]
    if price<=0:
        return {"symbol":sym,"phase":"UNKNOWN","updated":now,"fresh_breakout_eligible":False}

    r1=resistance_before_recent(closed1,holdout=2,lookback=8)
    r5=resistance_before_recent(closed5,holdout=2,lookback=6)
    if r5<=0:
        r5=resistance_before_recent(closed5,holdout=1,lookback=8)

    ret1=pct(price,closed1[-1]["close"]) if closed1 else 0.0
    ret3=pct(price,closed1[-3]["close"]) if len(closed1)>=3 else ret1
    recent_high=max([x["high"] for x in closed1[-5:]]+[live1["high"]])
    drawdown=pct(price,recent_high)

    mem=dict(lifecycle.get(sym) or {})
    used_level=f(mem.get("used_level_5m"))
    used_until=f(mem.get("used_until"))
    break_ts=f(mem.get("break_ts"))
    peak_since=f(mem.get("peak_since_break"))
    phase=str(mem.get("phase") or "UNKNOWN")

    boot,boot_peak,boot_ts=recent_breakout_bootstrap(closed5,live5,r5)
    live_break=bool(r5>0 and live5["high"]>=r5*(1+BREAK_EPS_PCT/100.0))
    last5=closed5[-1] if closed5 else None
    closed_break=bool(last5 and r5>0 and last5["high"]>=r5*(1+BREAK_EPS_PCT/100.0))

    if (boot or live_break or closed_break) and (used_until<=now or not same_level(used_level,r5)):
        used_level=r5
        used_until=now+USED_LEVEL_TTL
        break_ts=boot_ts or now
        peak_since=max(boot_peak,live5["high"],recent_high)
        phase="BREAKOUT_IN_PROGRESS"

    if used_until>now and used_level>0:
        peak_since=max(peak_since,recent_high,live5["high"])
        above=pct(price,used_level)
        dd=pct(price,peak_since) if peak_since>0 else 0.0
        falling=(ret1<0 and ret3<0) or dd<=-PEAK_REJECTION_PCT

        if above<=-FAIL_PCT:
            phase="REJECT_FALLING" if falling else "FAILED_BREAKOUT"
        elif abs(above)<=RETEST_BAND_PCT and (falling or phase in ("BREAKOUT_IN_PROGRESS","BREAKOUT_HOLD","CONTINUATION")):
            phase="RETEST"
        elif above>=NO_CHASE_PCT:
            phase="NO_CHASE"
        elif above>=CONTINUATION_PCT:
            phase="CONTINUATION"
        elif above>=HOLD_PCT:
            phase="BREAKOUT_HOLD"
        else:
            phase="BREAKOUT_IN_PROGRESS"
    else:
        used_level=0.0
        used_until=0.0
        break_ts=0.0
        peak_since=0.0
        dist5=pct(r5,price) if r5>0 else 999.0
        if r5>0 and 0<=dist5<=1.5:
            phase="APPROACH"
        elif r5>0:
            phase="BELOW_RESISTANCE"
        else:
            phase="UNKNOWN"

    fresh=phase in ("APPROACH","BELOW_RESISTANCE") and used_until<=now
    level=r5 or r1
    dist_to_level=pct(level,price) if level>0 else None
    age=max(0.0,now-break_ts) if break_ts else None
    result={
        "symbol":sym,"phase":phase,"updated":now,"price":price,
        "resistance_1m":r1 or None,"resistance_5m":r5 or None,
        "used_level_5m":used_level or None,"used_until":used_until or None,
        "break_age_seconds":age,"peak_since_break":peak_since or None,
        "drawdown_from_recent_peak_pct":round(drawdown,4),
        "return_1m_pct":round(ret1,4),"return_3m_pct":round(ret3,4),
        "distance_to_local_5m_pct":None if dist_to_level is None else round(dist_to_level,4),
        "fresh_breakout_eligible":bool(fresh),
    }
    lifecycle_history[sym].append((now,phase,price,r5))
    return result


def life(sym):
    x=lifecycle.get(sym) or {}
    if not x or time.time()-f(x.get("updated"))>LIFECYCLE_STALE_SECONDS:
        return {"phase":"UNKNOWN","fresh_breakout_eligible":False}
    return x


def candidate_symbols():
    rows=[]
    seen=set()
    for score,sym in b18.augmented_hot(40):
        if sym in seen or not b17.directional(sym):
            continue
        rows.append(sym); seen.add(sym)
        if len(rows)>=14:
            break

    near=[]
    for sym,r in q.latest.items():
        if sym in seen or not b17.directional(sym):
            continue
        d=r.get("breakout_distance_pct")
        layers=b16._layers(r) if r else 0
        dv=abs(f(r.get("distance_velocity_30s_per_min")))
        if d is not None and -1.5<=f(d)<=8.0 and layers>=4:
            near.append((f(d),-layers,-dv,sym))
    near.sort()
    for _,__,___,sym in near:
        if sym not in seen:
            rows.append(sym); seen.add(sym)
        if len(rows)>=LIFECYCLE_MAX_SYMBOLS:
            break

    recent=sorted(
        ((f(x.get("updated")),sym) for sym,x in lifecycle.items()
         if time.time()-f(x.get("updated"))<USED_LEVEL_TTL and sym not in seen),
        reverse=True
    )
    for _,sym in recent:
        rows.append(sym); seen.add(sym)
        if len(rows)>=LIFECYCLE_MAX_SYMBOLS:
            break
    return rows[:LIFECYCLE_MAX_SYMBOLS]


async def fetch_lifecycle(sym):
    if app.session is None:
        return
    try:
        rows1,rows5=await asyncio.gather(
            app.load_klines(app.session,sym,"1m",14),
            app.load_klines(app.session,sym,"5m",14),
        )
        lifecycle[sym]=infer_lifecycle(sym,rows1,rows5)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        lifecycle_stats["errors"]+=1
        print(f"Ψ-V10.18.3 LIFE_ERROR {sym} {type(e).__name__}: {e}",flush=True)


async def lifecycle_sampler():
    while True:
        await asyncio.sleep(LIFECYCLE_SAMPLE_SECONDS)
        try:
            syms=candidate_symbols()
            await asyncio.gather(*(fetch_lifecycle(sym) for sym in syms))
            lifecycle_stats["samples"]+=1
            lifecycle_stats["symbols"]=len(syms)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            lifecycle_stats["errors"]+=1
            print(f"Ψ-V10.18.3 LIFE_SAMPLER_ERROR {type(e).__name__}: {e}",flush=True)


def diag1183():
    out=[]
    for raw in _old_diag():
        row=dict(raw)
        sym=str(row.get("symbol") or "")
        lf=life(sym)
        row["breakout_lifecycle"]=lf.get("phase","UNKNOWN")
        row["fresh_breakout_eligible_1183"]=bool(lf.get("fresh_breakout_eligible"))
        row["local_5m_resistance_1183"]=lf.get("resistance_5m")
        row["local_5m_distance_1183"]=lf.get("distance_to_local_5m_pct")
        row["local_5m_used_level_1183"]=lf.get("used_level_5m")
        row["local_5m_drawdown_1183"]=lf.get("drawdown_from_recent_peak_pct")
        row["local_1m_return_1183"]=lf.get("return_1m_pct")
        row["local_3m_return_1183"]=lf.get("return_3m_pct")
        bad=lf.get("phase") in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE")
        if bad:
            bb=list(row.get("combined_blockers") or [])
            tag="LOCAL_5M_"+str(lf.get("phase"))
            if tag not in bb:
                bb.append(tag)
            row["combined_blockers"]=bb
        out.append(row)
    return out


def opp1183(row):
    score=f(_old_opp(row))
    ph=str(row.get("breakout_lifecycle") or "UNKNOWN")
    adj={
        "APPROACH":8.0,
        "BREAKOUT_IN_PROGRESS":2.0,
        "BREAKOUT_HOLD":4.0,
        "RETEST":3.0,
        "CONTINUATION":2.0,
        "FAILED_BREAKOUT":-24.0,
        "REJECT_FALLING":-36.0,
        "NO_CHASE":-28.0,
        "UNKNOWN":-8.0,
    }.get(ph,0.0)
    return round(score+adj,2)


def breakout_strict_1183(row):
    if not _old_breakout_strict(row):
        return False
    sym=str(row.get("symbol") or "")
    lf=life(sym)
    return bool(lf.get("fresh_breakout_eligible")) and lf.get("phase")=="APPROACH"


def pre_strict_1183(row):
    if not _old_pre_strict(row):
        return False
    ph=str(life(str(row.get("symbol") or "")).get("phase") or "UNKNOWN")
    return ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE")


def buy_strict_1183(row):
    if not _old_buy_strict(row):
        return False
    ph=str(life(str(row.get("symbol") or "")).get("phase") or "UNKNOWN")
    return ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE")


def pump_signature_1183(sym):
    ps=dict(_old_pump_signature(sym))
    ph=str(life(sym).get("phase") or "UNKNOWN")
    score=f(ps.get("pump_signature_score"))
    if ph=="APPROACH":
        score+=5
    elif ph in ("BREAKOUT_HOLD","CONTINUATION"):
        score+=2
    elif ph=="RETEST":
        score-=4
    elif ph=="FAILED_BREAKOUT":
        score-=20
    elif ph=="REJECT_FALLING":
        score-=30
    elif ph=="NO_CHASE":
        score-=24
    score=max(0.0,min(100.0,score))
    ps["pump_signature_score"]=round(score,1)
    ps["breakout_lifecycle"]=ph
    ps["fresh_breakout_eligible_1183"]=bool(life(sym).get("fresh_breakout_eligible"))
    micro=bool(ps.get("micro_ready_118"))
    execp=bool(ps.get("exec_pass_118"))
    ps["pump_state"]="PUMP-ARMED" if score>=b18.PUMP_ARMED_SCORE and micro and execp and ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE") else ("PUMP-WATCH" if score>=b18.PUMP_WATCH_SCORE and ph not in ("FAILED_BREAKOUT","REJECT_FALLING","NO_CHASE") else "NONE")
    return ps


def lifecycle_priority(x):
    ph=str(x.get("phase") or "UNKNOWN")
    p={"REJECT_FALLING":9,"FAILED_BREAKOUT":8,"RETEST":7,"BREAKOUT_IN_PROGRESS":6,"BREAKOUT_HOLD":5,"CONTINUATION":4,"APPROACH":3,"NO_CHASE":2,"BELOW_RESISTANCE":1}.get(ph,0)
    return (p, -abs(f(x.get("distance_to_local_5m_pct"),99)), f(x.get("updated")))


async def lifecycle_print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            rows=[x for x in lifecycle.values() if time.time()-f(x.get("updated"))<=LIFECYCLE_STALE_SECONDS]
            rows.sort(key=lifecycle_priority,reverse=True)
            counts=defaultdict(int)
            for x in rows:
                counts[str(x.get("phase") or "UNKNOWN")]+=1
            print(
                "Ψ-V10.18.3 LIFECYCLE "
                f"tracked={len(rows)} approach={counts['APPROACH']} break={counts['BREAKOUT_IN_PROGRESS']} "
                f"hold={counts['BREAKOUT_HOLD']} retest={counts['RETEST']} continuation={counts['CONTINUATION']} "
                f"failed={counts['FAILED_BREAKOUT']} reject={counts['REJECT_FALLING']} nochase={counts['NO_CHASE']} "
                f"samples={lifecycle_stats['samples']} errors={lifecycle_stats['errors']}",
                flush=True
            )
            for i,x in enumerate(rows[:12],1):
                level=x.get("resistance_5m") or x.get("used_level_5m")
                lt="-" if level is None else f"{f(level):.10g}"
                dist=x.get("distance_to_local_5m_pct")
                dt="-" if dist is None else f"{f(dist):+.3f}%"
                age=x.get("break_age_seconds")
                at="-" if age is None else f"{f(age):.0f}s"
                print(
                    f"C{i:02d}. {str(x.get('symbol') or '-'):14s} life={str(x.get('phase') or 'UNKNOWN'):20s} "
                    f"local5={lt} dist={dt} ret1={f(x.get('return_1m_pct')):+.3f}% ret3={f(x.get('return_3m_pct')):+.3f}% "
                    f"drawdown={f(x.get('drawdown_from_recent_peak_pct')):+.3f}% used={'YES' if x.get('used_level_5m') else 'NO'} "
                    f"breakAge={at} fresh={'YES' if x.get('fresh_breakout_eligible') else 'NO'}",
                    flush=True
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.18.3 LIFE_PRINT_ERROR {type(e).__name__}: {e}",flush=True)


b17.diag_pool=diag1183
b17.opp_score=opp1183
b16._breakout_strict=breakout_strict_1183
b16._pre_strict=pre_strict_1183
b16._buy_strict=buy_strict_1183
b18.pump_signature=pump_signature_1183

async def main1183():
    await asyncio.gather(_old_main(),lifecycle_sampler(),lifecycle_print_loop())

scanner.v7.main=main1183
scanner.VERSION=VERSION

print(
    "Ψ-V10.18.3 LIFECYCLE UPGRADE ACTIVE — real 1m/5m Binance candle state, used-level memory, "
    "APPROACH→BREAK→HOLD/RETEST→CONTINUATION/FAILURE, failed/no-chase suppression; formal PRE/BUY unchanged",
    flush=True
)

if __name__=="__main__":
    try:
        print("Ψ-V10.18.3 ACTIVE — fresh-breakout lifecycle enforcement",flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.18.3 stopped",flush=True)
