import asyncio
import json
import time
from collections import defaultdict, deque

import aiohttp
import app
import qualifier_app as q
import ignition10_app as v7

VERSION = "10.7.2-resilient-market-feed"
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
mini_source = "NONE"
mini_rest_ok = 0
mini_rest_fail = 0
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
    host_cursor=0
    while True:
        bases=[]
        for raw in (
            str(getattr(app,"WS_BASE","") or "").rstrip("/"),
            "wss://data-stream.binance.vision",
            "wss://stream.binance.com:443",
            "wss://stream.binance.com:9443",
        ):
            if raw and raw not in bases:
                bases.append(raw)
        base_url=bases[host_cursor%len(bases)] if bases else "wss://data-stream.binance.vision"
        url=f"{base_url}/ws/!ticker@arr"
        try:
            async with app.session.ws_connect(
                url,heartbeat=25,receive_timeout=60,max_msg_size=0,timeout=12
            ) as ws:
                radar_ticker_connected=True
                print(f"Ψ-V10.7.2 RADAR ticker WS connected host={base_url} (!ticker@arr)",flush=True)
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload,list):
                            continue
                        for x in payload:
                            if not isinstance(x,dict):
                                continue
                            sym=x.get("s","")
                            push(
                                sym,app.safe_float(x.get("c")),app.safe_float(x.get("q")),
                                app.safe_float(x.get("n")),app.safe_float(x.get("b")),
                                app.safe_float(x.get("a")),"ticker"
                            )
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f"ticker_websocket_{msg.type.name.lower()}")
                raise RuntimeError("ticker_websocket_stream_ended")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            app.last_error=f"RADAR_TICKER: {type(e).__name__}: {e}"
            print(f"{app.last_error} host={base_url}",flush=True)
            host_cursor=(host_cursor+1)%max(1,len(bases))
        finally:
            radar_ticker_connected=False
        await asyncio.sleep(1)


def _mini_ingest(payload, source):
    global mini_last_message_ts,mini_last_count,mini_source
    if not isinstance(payload,list):
        return 0
    t=now(); accepted=0
    for x in payload:
        if not isinstance(x,dict):
            continue
        sym=x.get("s") or x.get("symbol") or ""
        last=app.safe_float(x.get("c") if x.get("c") is not None else x.get("lastPrice"))
        open_=app.safe_float(x.get("o") if x.get("o") is not None else x.get("openPrice"))
        high=app.safe_float(x.get("h") if x.get("h") is not None else x.get("highPrice"))
        low=app.safe_float(x.get("l") if x.get("l") is not None else x.get("lowPrice"))
        qv=app.safe_float(x.get("q") if x.get("q") is not None else x.get("quoteVolume"))
        if sym and last>0:
            mini_24h[sym]={
                "change_pct": ((last/open_)-1.0)*100.0 if open_>0 else 0.0,
                "open": open_,
                "high": high,
                "low": low,
                "last": last,
                "ts": t,
            }
            accepted+=1
        if sym and last>0 and t-radar_last_full.get(sym,0)>=2.5:
            push(sym,last,qv,source="mini" if source.startswith("WS") else "mini_rest")
    if accepted:
        mini_last_message_ts=t
        mini_last_count=accepted
        mini_source=source
    return accepted

async def _radar_book_rest_snapshot():
    """Full-universe price/spread fallback using Binance bookTicker.

    This is discovery telemetry only. No synthetic volume or order flow is
    created, so it cannot by itself grant PRE/BUY authority.
    """
    if getattr(app,"session",None) is None:
        return 0
    hosts=[
        "https://data-api.binance.vision",
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]
    last_exc=None
    for host in hosts:
        try:
            async with app.session.get(
                f"{host}/api/v3/ticker/bookTicker",
                timeout=aiohttp.ClientTimeout(total=6,connect=2),
            ) as resp:
                body=await resp.text()
                if resp.status!=200:
                    raise RuntimeError(f"{host} HTTP {resp.status}: {body[:120]}")
                rows=json.loads(body)
                if not isinstance(rows,list):
                    raise RuntimeError("bookTicker payload is not a list")
                accepted=0
                for x in rows:
                    if not isinstance(x,dict):
                        continue
                    sym=str(x.get("symbol") or "")
                    if sym not in q.universe_set:
                        continue
                    bid=app.safe_float(x.get("bidPrice"))
                    ask=app.safe_float(x.get("askPrice"))
                    if bid<=0 or ask<=0:
                        continue
                    mid=(bid+ask)/2.0
                    meta=app.symbol_meta.get(sym,{}) if isinstance(getattr(app,"symbol_meta",None),dict) else {}
                    push(
                        sym,mid,app.safe_float(meta.get("quote_volume_24h")),None,
                        bid,ask,"book_rest"
                    )
                    accepted+=1
                if accepted:
                    print(
                        f"Ψ-V10.7.2 RADAR book REST fallback live "
                        f"symbols={accepted} host={host}",
                        flush=True,
                    )
                return accepted
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exc=exc
            continue
    print(
        f"RADAR_BOOK_REST: {type(last_exc).__name__ if last_exc else 'RuntimeError'}: {last_exc}",
        flush=True,
    )
    return 0


async def _mini_rest_snapshot():
    """Bounded 24h telemetry fallback for active candidates.

    Avoid the heavy all-symbol 24h response that times out on this runtime.
    """
    global radar_mini_connected,mini_rest_ok,mini_rest_fail,mini_source
    if getattr(app,"session",None) is None:
        return 0

    universe_set=set(getattr(q,"universe_set",set()) or set())
    priority=[];seen=set()
    def add(sym):
        sym=str(sym or "")
        if sym and sym in universe_set and sym not in seen:
            seen.add(sym);priority.append(sym)

    for sym in ("BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT"):
        add(sym)
    for sym in list(getattr(app,"selected_micro_symbols",[]) or []):
        add(sym)
    try:
        for _,sym in q.hot(12):
            add(sym)
    except Exception:
        pass
    priority=priority[:16]
    if not priority:
        return 0

    hosts=[
        "https://data-api.binance.vision",
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]
    sem=asyncio.Semaphore(4)

    async def one(sym):
        async with sem:
            offset=sum(ord(ch) for ch in sym)%len(hosts)
            ordered=hosts[offset:]+hosts[:offset]
            for host in ordered:
                try:
                    async with app.session.get(
                        f"{host}/api/v3/ticker/24hr",
                        params={"symbol":sym,"type":"MINI"},
                        timeout=aiohttp.ClientTimeout(total=4,connect=1.5),
                    ) as resp:
                        body=await resp.text()
                        if resp.status!=200:
                            raise RuntimeError(f"{host} HTTP {resp.status}: {body[:100]}")
                        row=json.loads(body)
                        if isinstance(row,dict):
                            return row
                except asyncio.CancelledError:
                    raise
                except Exception:
                    continue
        return None

    results=await asyncio.gather(*(one(s) for s in priority),return_exceptions=True)
    rows=[x for x in results if isinstance(x,dict)]
    n=_mini_ingest(rows,"REST_24HR_PARTIAL") if rows else 0
    if n:
        radar_mini_connected=True
        mini_rest_ok+=1
        print(
            f"Ψ-V10.7.2 RADAR mini REST fallback live "
            f"symbols={n}/{len(priority)} mode=BOUNDED_SINGLE",
            flush=True,
        )
        return n
    mini_rest_fail+=1
    print(
        f"RADAR_MINI_REST: no live 24h rows priority={len(priority)}",
        flush=True,
    )
    return 0


async def mini_rest_loop():
    while True:
        try:
            await asyncio.sleep(3)
            ws_stale=(now()-mini_last_message_ts>8.0) or not radar_mini_connected
            if ws_stale:
                await _radar_book_rest_snapshot()
                await _mini_rest_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"RADAR_MINI_REST_LOOP: {type(e).__name__}: {e}",flush=True)


async def mini_loop():
async def mini_loop():
    global radar_mini_connected,mini_source
    host_cursor=0
    while True:
        bases=[]
        for raw in (
            str(getattr(app,"WS_BASE","") or "").rstrip("/"),
            "wss://data-stream.binance.vision",
            "wss://stream.binance.com:443",
            "wss://stream.binance.com:9443",
        ):
            if raw and raw not in bases:
                bases.append(raw)
        base_url=bases[host_cursor%len(bases)] if bases else "wss://data-stream.binance.vision"
        url=f"{base_url}/ws/!miniTicker@arr"
        try:
            async with app.session.ws_connect(url,heartbeat=25,receive_timeout=60,max_msg_size=0,timeout=12) as ws:
                radar_mini_connected=True
                mini_source=f"WS:{base_url}"
                print(f"Ψ-V10.7.1 RADAR mini WS connected host={base_url} (!miniTicker@arr)",flush=True)
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        _mini_ingest(payload,f"WS:{base_url}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f"mini_websocket_{msg.type.name.lower()}")
                raise RuntimeError("mini_websocket_stream_ended")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            app.last_error=f"RADAR_MINI: {type(e).__name__}: {e}"
            print(f"{app.last_error} host={base_url}",flush=True)
            host_cursor=(host_cursor+1)%max(1,len(bases))
        finally:
            # Do not mark the feed unavailable when a fresh REST fallback is
            # already maintaining mini_24h.
            radar_mini_connected=(mini_source.startswith("REST_") and now()-mini_last_message_ts<=8.0)
        await asyncio.sleep(1)


async def combined_discovery_loop():
    await asyncio.gather(
        base_discovery_loop(),
        q.discovery_rest_loop(),
        ticker_loop(),
        mini_loop(),
        mini_rest_loop(),
    )


v7.ignition_metric=metric
v7.ignition_rank=rank
v7.ignition_hot=hot
q.hot=hot
q.discovery_loop=combined_discovery_loop


if __name__=="__main__":
    try: asyncio.run(v7.main())
    except KeyboardInterrupt: print("Ψ-V10.7.1 stopped",flush=True)
