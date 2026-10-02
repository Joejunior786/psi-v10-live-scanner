import asyncio
import json
import time
from collections import defaultdict, deque

import aiohttp
import app
import qualifier_app as q
import ignition10_app as v7

VERSION = "10.7.1-ignition-feed"
SAMPLE_SECONDS = 1.0
HISTORY_SAMPLES = 190
TRIGGER_SCORE = 26.0

radar_hist = defaultdict(lambda: deque(maxlen=HISTORY_SAMPLES))
radar_last_sample = {}
radar_last_full = {}
radar_ticker_connected = False
radar_mini_connected = False
mini_24h = {}
mini_last_message_ts = 0.0
mini_last_count = 0
base_discovery_loop = q.discovery_loop

v7.VERSION = VERSION
v7.RAPID_TRIGGER_SCORE = TRIGGER_SCORE
app.USER_AGENT = "psi-v10-live-scanner/10.7.1-ignition-feed"


def now():
    return time.time()


def before(samples, target):
    for x in reversed(samples):
        if x[0] <= target:
            return x
    return samples[0] if samples else None


def ratio(a, b):
    return a / b if b and b > 0 else 0.0


def push(symbol, p, qv, n=None, bid=None, ask=None, source="ticker"):
    if symbol not in q.universe_set or p <= 0:
        return
    t = now()
    if t - radar_last_sample.get(symbol, 0) < SAMPLE_SECONDS * 0.75:
        return
    prev = radar_hist[symbol][-1] if radar_hist[symbol] else None
    if n is None:
        n = prev[3] if prev else 0.0
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        bid = p
        ask = p
    radar_hist[symbol].append((t, p, qv, n, bid, ask))
    radar_last_sample[symbol] = t
    if source == "ticker":
        radar_last_full[symbol] = t


def metric(symbol):
    samples = radar_hist.get(symbol)
    if not samples or len(samples) < 5:
        fallback = q.disc.get(symbol)
        samples = fallback if fallback and len(fallback) >= 5 else samples
    if not samples or len(samples) < 5:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples or ())}

    t, p, qv, n, bid, ask = samples[-1]
    age = t - samples[0][0]
    if p <= 0 or age < 10:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples), "age_s": round(age,1)}

    s5, s15, s30, s60 = (before(samples, t-x) for x in (5,15,30,60))
    ret = lambda x: (p/x[1]-1)*100 if x and x[1] > 0 else 0.0
    r5, r15, r30, r60 = ret(s5), ret(s15), ret(s30), ret(s60)

    def d(idx, x):
        cur = qv if idx == 2 else n
        return max(0.0, cur - (x[idx] if x else cur))

    v5,v15,v30 = d(2,s5),d(2,s15),d(2,s30)
    n5,n15,n30 = d(3,s5),d(3,s15),d(3,s30)
    v5x = ratio(v5/5, max(0,v15-v5)/10)
    v15x = ratio(v15/15, max(0,v30-v15)/15)
    t5x = ratio(n5/5, max(0,n15-n5)/10)
    t15x = ratio(n15/15, max(0,n30-n15)/15)
    pacc = max(0,r5*3-r15)+max(0,r15*2-r30)
    spread = (ask-bid)/((ask+bid)/2)*10000 if bid>0 and ask>bid else 0.0
    prices=[x[1] for x in samples if x[0]>=t-60 and x[1]>0]
    range60=(max(prices)-min(prices))/p*100 if prices else 999
    c=lambda x,a,b:max(a,min(b,x))
    score=(c(r5,0,1.5)*18+c(r15,0,3)*8+c(r30,0,5)*3+c(pacc,0,3)*7+
           c(v5x-1,0,5)*8+c(v15x-1,0,4)*4+c(t5x-1,0,5)*6+c(t15x-1,0,4)*3+
           (6 if spread<=8 else 2 if spread<=15 else -8 if spread>=30 else 0)+(4 if range60<=1.8 else 0))
    score-=min(max(0,r60-6)*1.5+max(0,r30-4),12)
    acceleration=(r5>=0.10 or r15>=0.20 or v5x>=1.6 or t5x>=1.6 or (v15x>=1.4 and t15x>=1.4))
    trig=v7._directional(symbol) and acceleration and score>=TRIGGER_SCORE and spread<=30
    return {"symbol":symbol,"score":round(score,3),"trigger":trig,"r5":round(r5,4),"r15":round(r15,4),
            "r30":round(r30,4),"r60":round(r60,4),"price_accel":round(pacc,4),"vol_accel_5":round(v5x,3),
            "vol_accel_15":round(v15x,3),"trade_accel_5":round(t5x,3),"trade_accel_15":round(t15x,3),
            "spread_bps":round(spread,3),"range60_pct":round(range60,4),"price":p,"quote_5s":round(v5,2),
            "trades_5s":int(n5),"samples":len(samples),"age_s":round(age,1)}


def rank(limit=None, triggered_only=False):
    rows=[]
    for symbol in q.universe:
        m=metric(symbol); v7.rapid_metrics[symbol]=m
        if triggered_only and not m.get("trigger"): continue
        rows.append(m)
    rows.sort(key=lambda x:(bool(x.get("trigger")),float(x.get("score",0))),reverse=True)
    return rows[:limit] if limit else rows


def hot(limit=q.HOT_COUNT):
    rows=rank(limit=limit,triggered_only=False)
    if not rows or all(float(x.get("score",0))==0 for x in rows[:10]):
        return v7._base_hot(limit)
    return [(float(x.get("score",0)),x["symbol"]) for x in rows]


async def ticker_loop():
    global radar_ticker_connected
    url=f"{app.WS_BASE}/ws/!ticker@arr"
    while True:
        try:
            async with app.session.ws_connect(url,heartbeat=30,receive_timeout=90,max_msg_size=0) as ws:
                radar_ticker_connected=True
                print("Ψ-V10.7.1 RADAR ticker WS connected (!ticker@arr)",flush=True)
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try: payload=json.loads(msg.data)
                        except json.JSONDecodeError: continue
                        if not isinstance(payload,list): continue
                        for x in payload:
                            if not isinstance(x,dict): continue
                            sym=x.get("s","")
                            push(sym,app.safe_float(x.get("c")),app.safe_float(x.get("q")),app.safe_float(x.get("n")),
                                 app.safe_float(x.get("b")),app.safe_float(x.get("a")),"ticker")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except asyncio.CancelledError: raise
        except Exception as e:
            app.last_error=f"RADAR_TICKER: {type(e).__name__}: {e}"; print(app.last_error,flush=True)
        finally: radar_ticker_connected=False
        await asyncio.sleep(2)


async def mini_loop():
    global radar_mini_connected, mini_last_message_ts, mini_last_count
    url=f"{app.WS_BASE}/ws/!miniTicker@arr"
    while True:
        try:
            async with app.session.ws_connect(url,heartbeat=30,receive_timeout=90,max_msg_size=0) as ws:
                radar_mini_connected=True
                print("Ψ-V10.7.1 RADAR mini WS connected (!miniTicker@arr)",flush=True)
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try: payload=json.loads(msg.data)
                        except json.JSONDecodeError: continue
                        if not isinstance(payload,list): continue
                        t=now()
                        mini_last_message_ts=t
                        mini_last_count=len(payload)
                        for x in payload:
                            if not isinstance(x,dict): continue
                            sym=x.get("s","")
                            last=app.safe_float(x.get("c"))
                            open_=app.safe_float(x.get("o"))
                            high=app.safe_float(x.get("h"))
                            low=app.safe_float(x.get("l"))
                            if sym and last>0:
                                mini_24h[sym]={
                                    "change_pct": ((last/open_)-1.0)*100.0 if open_>0 else 0.0,
                                    "open": open_,
                                    "high": high,
                                    "low": low,
                                    "last": last,
                                    "ts": t,
                                }
                            if t-radar_last_full.get(sym,0)<2.5: continue
                            push(sym,last,app.safe_float(x.get("q")),source="mini")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR): break
        except asyncio.CancelledError: raise
        except Exception as e:
            app.last_error=f"RADAR_MINI: {type(e).__name__}: {e}"; print(app.last_error,flush=True)
        finally: radar_mini_connected=False
        await asyncio.sleep(2)


async def combined_discovery_loop():
    await asyncio.gather(base_discovery_loop(),ticker_loop(),mini_loop())


v7.ignition_metric=metric
v7.ignition_rank=rank
v7.ignition_hot=hot
q.hot=hot
q.discovery_loop=combined_discovery_loop


if __name__=="__main__":
    try: asyncio.run(v7.main())
    except KeyboardInterrupt: print("Ψ-V10.7.1 stopped",flush=True)
