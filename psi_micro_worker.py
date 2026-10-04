import asyncio
import json
import os
import time
from collections import defaultdict, deque
from typing import List, Tuple

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.3.0-distributed-micro-3"
ROLE = os.getenv("PSI_WORKER_ROLE", "TRADE").strip().upper()
REDIS_URL = os.getenv("REDIS_URL", "").strip()
CONTROL_KEY = os.getenv("PSI_MICRO_CONTROL_KEY", "psi:v12:selected").strip()
CHANNEL = "psi:v12:trade" if ROLE == "TRADE" else "psi:v12:depth"
HEARTBEAT_KEY = f"psi:v12:worker:{ROLE.lower()}"
SNAPSHOT_KEY = f"psi:v12:micro-snapshot:{ROLE.lower()}"
SNAPSHOT_INTERVAL = max(0.25, float(os.getenv("PSI_MICRO_SNAPSHOT_SECONDS", "0.5")))
SNAPSHOT_TTL_SECONDS = max(5, int(os.getenv("PSI_MICRO_SNAPSHOT_TTL_SECONDS", "15")))
PUBLISH_RAW = str(os.getenv("PSI_MICRO_PUBLISH_RAW", "1")).strip().lower() in {"1","true","yes","on"}
SLIPPAGE_TEST_NOTIONAL = max(1.0, float(os.getenv("SLIPPAGE_TEST_NOTIONAL", "1000")))
WS_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_WORKER_WS_HOSTS",
        "wss://stream.binance.com:9443,wss://data-stream.binance.vision"
    ).split(",")
    if x.strip()
)
CONTROL_POLL_SECONDS = max(1.0, float(os.getenv("PSI_WORKER_CONTROL_POLL_SECONDS", "2")))
MAX_SYMBOLS = max(1, min(int(os.getenv("PSI_WORKER_MAX_SYMBOLS", "80")), 120))
RECONNECT_BACKOFF = max(0.5, float(os.getenv("PSI_WORKER_RECONNECT_BACKOFF", "1.5")))

if ROLE not in {"TRADE", "BOOK"}:
    raise RuntimeError(f"Unsupported PSI_WORKER_ROLE={ROLE}; expected TRADE or BOOK")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for distributed micro worker")


def now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _safe_div(a, b, default=0.0):
    try:
        return a / b if b else default
    except Exception:
        return default


def _avg(values):
    values=list(values or [])
    return sum(values)/len(values) if values else 0.0


def _trade_state_factory():
    return {
        "buckets": deque(maxlen=190),
        "last_agg_id": None,
        "sequence_samples": 0,
        "sequence_ok": True,
        "last_trade_ms": 0,
    }


def _book_state_factory():
    return {
        "last_update_id": None,
        "sequence_samples": 0,
        "sequence_ok": True,
        "book_updates": 0,
        "last_book_ms": 0,
        "bids": [],
        "asks": [],
        "ofi": deque(maxlen=1200),
        "obi": deque(maxlen=1200),
        "ask_dep": deque(maxlen=1200),
        "bid_dep": deque(maxlen=1200),
        "spread": deque(maxlen=1200),
        "slip": deque(maxlen=1200),
    }


def _record_trade(state, data):
    try:
        price=_safe_float(data.get("p"))
        qty=_safe_float(data.get("q"))
        if price<=0 or qty<=0:
            return False
        ts=int(data.get("T") or data.get("E") or now_ms())
    except Exception:
        return False

    aid_raw=data.get("a")
    if aid_raw is not None:
        try:
            aid=int(aid_raw)
        except (TypeError, ValueError):
            state["sequence_ok"]=False
            return False
        last=state.get("last_agg_id")
        if last is not None:
            if aid<=int(last):
                return False
            state["sequence_samples"]+=1
        state["last_agg_id"]=aid

    quote=price*qty
    is_buy=not bool(data.get("m",False))
    signed=quote if is_buy else -quote
    sec=ts//1000
    buckets=state["buckets"]
    if buckets and buckets[-1]["sec"]==sec:
        b=buckets[-1]
    else:
        b={
            "sec":sec,"first_ms":ts,"last_ms":ts,
            "signed":0.0,"quote":0.0,"buy_quote":0.0,"qty":0.0,
            "count":0,"buy_count":0,"first_price":price,"last_price":price,
        }
        buckets.append(b)
    b["last_ms"]=max(int(b.get("last_ms",ts)),ts)
    b["signed"]+=signed
    b["quote"]+=quote
    b["buy_quote"]+=quote if is_buy else 0.0
    b["qty"]+=qty
    b["count"]+=1
    b["buy_count"]+=1 if is_buy else 0
    b["last_price"]=price
    state["last_trade_ms"]=max(int(state.get("last_trade_ms",0)),ts)

    cutoff=now_ms()-180_000
    while buckets and int(buckets[0]["last_ms"])<cutoff:
        buckets.popleft()
    return True


def _trade_window(buckets, now, lo_s, hi_s=0):
    lower=now-int(lo_s*1000)
    upper=now-int(hi_s*1000)
    return [b for b in buckets if lower<=int(b["last_ms"])<upper]


def _agg_buckets(rows):
    if not rows:
        return {
            "signed":0.0,"quote":0.0,"buy_quote":0.0,"qty":0.0,
            "count":0,"buy_count":0,"first_price":0.0,"last_price":0.0,
        }
    return {
        "signed":sum(float(b["signed"]) for b in rows),
        "quote":sum(float(b["quote"]) for b in rows),
        "buy_quote":sum(float(b["buy_quote"]) for b in rows),
        "qty":sum(float(b["qty"]) for b in rows),
        "count":sum(int(b["count"]) for b in rows),
        "buy_count":sum(int(b["buy_count"]) for b in rows),
        "first_price":float(rows[0]["first_price"]),
        "last_price":float(rows[-1]["last_price"]),
    }


def _trade_metrics(state, now):
    buckets=list(state["buckets"])
    recent=_agg_buckets(_trade_window(buckets,now,60))
    first30=_agg_buckets(_trade_window(buckets,now,60,30))
    last30=_agg_buckets(_trade_window(buckets,now,30))
    prev60=_agg_buckets(_trade_window(buckets,now,120,60))
    last10=_agg_buckets(_trade_window(buckets,now,10))
    prev10=_agg_buckets(_trade_window(buckets,now,20,10))

    cvd60=recent["signed"]
    cvd_acc=last30["signed"]-first30["signed"]
    buy_ratio=_safe_div(recent["buy_quote"],recent["quote"],0.5)
    trade_acc=_safe_div(last30["count"],max(first30["count"],1),0.0)
    avg_first=_safe_div(first30["quote"],first30["count"],0.0)
    avg_last=_safe_div(last30["quote"],last30["count"],0.0)
    trade_size_shift=_safe_div(avg_last,max(avg_first,1e-9),0.0)
    rv10=_safe_div(last10["quote"],max(prev10["quote"],1e-9),0.0)
    rv30=_safe_div(last30["quote"],max(prev60["quote"]/2.0,1e-9),0.0)
    vwap=_safe_div(recent["quote"],recent["qty"],0.0)
    prev_vwap=_safe_div(first30["quote"],first30["qty"],0.0)
    last_price=recent["last_price"]
    vwap_reclaim=bool(
        vwap and last_price>=vwap
        and (
            first30["count"]==0
            or first30["last_price"]<=prev_vwap
            or cvd_acc>0
        )
    )
    flow_persistence=_safe_div(last30["buy_count"],last30["count"],0.0)
    last_ms=int(state.get("last_trade_ms",0) or 0)
    age_ms=max(0,now-last_ms) if last_ms>0 else 999999999
    return {
        "last_trade_ms":last_ms,
        "trade_age_ms":age_ms,
        "trade_fresh":bool(last_ms>0 and age_ms<=15000),
        "sequence_verified":bool(state.get("sequence_ok",False) and int(state.get("sequence_samples",0))>=3),
        "trade_sequence_samples":int(state.get("sequence_samples",0)),
        "cvd_quote_60s":cvd60,
        "cvd_acceleration":cvd_acc,
        "aggressive_buy_ratio":buy_ratio,
        "trade_count_60s":int(recent["count"]),
        "trade_acceleration":trade_acc,
        "trade_size_shift":trade_size_shift,
        "relative_volume_10s":rv10,
        "relative_volume_30s":rv30,
        "vwap_60s":vwap,
        "vwap_reclaim":vwap_reclaim,
        "flow_persistence":flow_persistence,
        "last_price":last_price,
    }


def _depth_notional(levels):
    return sum(float(p)*float(q) for p,q in levels)


def _estimate_slippage(asks, notional):
    if not asks or notional<=0:
        return None
    remaining=float(notional)
    spent=0.0
    base=0.0
    best=float(asks[0][0])
    for price,qty in asks:
        price=float(price);qty=float(qty)
        level_quote=price*qty
        take=min(remaining,level_quote)
        if take>0:
            spent+=take
            base+=take/price
            remaining-=take
        if remaining<=1e-9:
            break
    if remaining>1e-6 or base<=0 or best<=0:
        return None
    return ((spent/base)/best-1.0)*10000.0


def _record_book(state, data):
    try:
        update_id=int(data.get("lastUpdateId") or data.get("u") or 0)
    except (TypeError, ValueError):
        return False
    raw_bids=data.get("bids") or data.get("b") or []
    raw_asks=data.get("asks") or data.get("a") or []
    if update_id<=0 or not raw_bids or not raw_asks:
        return False

    last=state.get("last_update_id")
    if last is not None and update_id<=int(last):
        return False

    bids=[
        (_safe_float(px),_safe_float(qty))
        for px,qty in raw_bids[:20]
        if _safe_float(px)>0 and _safe_float(qty)>0
    ]
    asks=[
        (_safe_float(px),_safe_float(qty))
        for px,qty in raw_asks[:20]
        if _safe_float(px)>0 and _safe_float(qty)>0
    ]
    if not bids or not asks:
        state["sequence_ok"]=False
        return False

    prev_bids=list(state.get("bids") or [])
    prev_asks=list(state.get("asks") or [])
    ts=now_ms()

    bid_notional=_depth_notional(bids)
    ask_notional=_depth_notional(asks)
    obi=_safe_div(bid_notional-ask_notional,bid_notional+ask_notional,0.0)
    mid=(bids[0][0]+asks[0][0])/2.0
    spread=_safe_div(asks[0][0]-bids[0][0],mid,0.0)*10000.0
    slip=_estimate_slippage(asks,SLIPPAGE_TEST_NOTIONAL)
    state["obi"].append((ts,obi))
    state["spread"].append((ts,spread))
    if slip is not None:
        state["slip"].append((ts,slip))

    if prev_bids and prev_asks:
        pb=dict(prev_bids); pa=dict(prev_asks)
        cb=dict(bids); ca=dict(asks)
        bid_change=sum(p*(cb.get(p,0.0)-pb.get(p,0.0)) for p in set(pb)|set(cb))
        ask_change=sum(p*(ca.get(p,0.0)-pa.get(p,0.0)) for p in set(pa)|set(ca))
        ofi=_safe_div(bid_change-ask_change,abs(bid_change)+abs(ask_change),0.0)
        prev_ask=_depth_notional(prev_asks)
        prev_bid=_depth_notional(prev_bids)
        ask_dep=_safe_div(prev_ask-ask_notional,prev_ask,0.0)
        bid_dep=_safe_div(prev_bid-bid_notional,prev_bid,0.0)
        state["ofi"].append((ts,ofi))
        state["ask_dep"].append((ts,ask_dep))
        state["bid_dep"].append((ts,bid_dep))

    state["bids"]=bids
    state["asks"]=asks
    state["last_update_id"]=update_id
    state["sequence_ok"]=True
    state["sequence_samples"]+=1
    state["book_updates"]+=1
    state["last_book_ms"]=ts

    cutoff=ts-120_000
    for key in ("ofi","obi","ask_dep","bid_dep","spread","slip"):
        dq=state[key]
        while dq and int(dq[0][0])<cutoff:
            dq.popleft()
    return True


def _book_values(state, key, now, seconds=60):
    cutoff=now-int(seconds*1000)
    return [float(v) for ts,v in state[key] if int(ts)>=cutoff]


def _book_metrics(state, now):
    ofis=_book_values(state,"ofi",now)
    obis=_book_values(state,"obi",now)
    asks=_book_values(state,"ask_dep",now)
    bids=_book_values(state,"bid_dep",now)
    spreads=_book_values(state,"spread",now)
    slips=_book_values(state,"slip",now)
    ofi=_avg(ofis[-20:])
    obi=_avg(obis[-20:])
    ask_dep=_avg(asks[-20:])
    bid_dep=_avg(bids[-20:])
    half=max(1,len(ofis)//2)
    ofi_acc=_avg(ofis[half:])-_avg(ofis[:half]) if len(ofis)>=6 else 0.0
    ofi_persistence=_safe_div(sum(1 for x in ofis[-20:] if x>0),len(ofis[-20:]),0.0)
    last_ms=int(state.get("last_book_ms",0) or 0)
    age_ms=max(0,now-last_ms) if last_ms>0 else 999999999
    return {
        "last_book_ms":last_ms,
        "book_age_ms":age_ms,
        "book_fresh":bool(last_ms>0 and age_ms<=5000),
        "book_sequence_verified":bool(state.get("sequence_ok",False) and int(state.get("sequence_samples",0))>=3),
        "book_sequence_samples":int(state.get("sequence_samples",0)),
        "book_updates":int(state.get("book_updates",0)),
        "ofi_samples":len(ofis),
        "ofi":ofi,
        "ofi_acceleration":ofi_acc,
        "ofi_persistence":ofi_persistence,
        "obi":obi,
        "ask_depletion":ask_dep,
        "bid_depletion":bid_dep,
        "spread_bps":spreads[-1] if spreads else None,
        "slippage_bps":slips[-1] if slips else None,
    }


async def publish_snapshot(r, symbols, states, events, host):
    now=now_ms()
    metrics={}
    if ROLE=="TRADE":
        for sym in symbols:
            st=states.get(sym)
            if st:
                metrics[sym]=_trade_metrics(st,now)
    else:
        for sym in symbols:
            st=states.get(sym)
            if st:
                metrics[sym]=_book_metrics(st,now)
    payload={
        "version":WORKER_VERSION,
        "role":ROLE,
        "generated_ms":now,
        "source_symbols":len(symbols),
        "metric_symbols":len(metrics),
        "events":events,
        "host":host,
        "publish_raw":PUBLISH_RAW,
        "metrics":metrics,
    }
    await r.set(SNAPSHOT_KEY,json.dumps(payload,separators=(",",":")),ex=SNAPSHOT_TTL_SECONDS)


async def selected_symbols(r) -> List[str]:
    raw = await r.get(CONTROL_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("symbols") or []
    out = []
    seen = set()
    for item in data if isinstance(data, list) else []:
        sym = str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= MAX_SYMBOLS:
            break
    return out


def stream_name(symbol: str) -> str:
    s = symbol.lower()
    return f"{s}@aggTrade" if ROLE == "TRADE" else f"{s}@depth20@100ms"


def combined_url(host: str, symbols: List[str]) -> str:
    streams = "/".join(stream_name(s) for s in symbols)
    return f"{host}/stream?streams={streams}"


async def publish_heartbeat(r, symbols: List[str], events: int, host: str, error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "role": ROLE,
        "symbols": len(symbols),
        "events": events,
        "host": host,
        "last_event_ms": now_ms(),
        "error": error,
    }
    await r.set(HEARTBEAT_KEY, json.dumps(payload, separators=(",", ":")), ex=15)


async def _subscription_change(ws, method: str, symbols, request_id: int):
    symbols=sorted({str(s).upper() for s in symbols if str(s).upper().endswith("USDT")})
    if not symbols:
        return request_id
    params=[stream_name(s) for s in symbols]
    await ws.send_json({"method":method,"params":params,"id":request_id})
    return request_id+1


async def stream_once(r, session: aiohttp.ClientSession, symbols: List[str], host: str):
    # Use Binance's raw /ws endpoint so control-pool changes can be applied
    # incrementally. Unchanged symbols retain their sequence/history instead of
    # being reset every time V12 re-ranks the 80-symbol micro pool.
    url = f"{host}/ws"
    events = 0
    last_hb = 0.0
    last_snapshot = 0.0
    last_control = 0.0
    states = defaultdict(_trade_state_factory if ROLE=="TRADE" else _book_state_factory)
    active = {str(s).upper() for s in symbols if str(s).upper().endswith("USDT")}
    request_id = 1
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=30)

    async with session.ws_connect(
        url,
        heartbeat=15,
        receive_timeout=30,
        autoping=True,
        timeout=timeout,
        max_msg_size=4 * 1024 * 1024,
    ) as ws:
        # Keep a raw /ws connection so subscriptions can change in place, but
        # request combined envelopes so partial-depth packets retain their
        # stream name/symbol. Trade events already include s; depth20 does not
        # reliably include it without the combined wrapper.
        await ws.send_json({
            "method":"SET_PROPERTY",
            "params":["combined",True],
            "id":request_id,
        })
        request_id+=1
        request_id=await _subscription_change(ws,"SUBSCRIBE",active,request_id)
        print(
            f"PSI-DISTRIBUTED-MICRO connected role={ROLE} symbols={len(active)} "
            f"host={host} mode=INCREMENTAL combined=YES",
            flush=True,
        )
        await publish_heartbeat(r, sorted(active), events, host)
        last_control=time.monotonic()

        async for msg in ws:
            now = time.monotonic()

            # Apply pool membership changes on the existing socket. A ranking
            # reorder alone does nothing; only true additions/removals alter
            # subscriptions. Host rotation is reserved for real socket failure.
            if now-last_control>=CONTROL_POLL_SECONDS:
                wanted_list=await selected_symbols(r)
                if wanted_list:
                    wanted=set(wanted_list)
                    added=wanted-active
                    removed=active-wanted
                    if removed:
                        request_id=await _subscription_change(ws,"UNSUBSCRIBE",removed,request_id)
                    if added:
                        request_id=await _subscription_change(ws,"SUBSCRIBE",added,request_id)
                    if added or removed:
                        for sym in removed:
                            states.pop(sym,None)
                        active=wanted
                        print(
                            f"PSI-DISTRIBUTED-MICRO incremental_update role={ROLE} "
                            f"symbols={len(active)} add={len(added)} remove={len(removed)} "
                            f"preview={','.join(sorted(active)[:8])}",
                            flush=True,
                        )
                else:
                    # A transient control-key gap must never tear down a healthy
                    # Binance stream or erase strict sequence history.
                    await publish_heartbeat(
                        r, sorted(active), events, host, "control_stale_holding_last_pool"
                    )
                last_control=now

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    envelope = json.loads(msg.data)
                    if isinstance(envelope,dict) and envelope.get("id") is not None and "result" in envelope:
                        continue
                    data = envelope.get("data") if isinstance(envelope, dict) and isinstance(envelope.get("data"),dict) else envelope
                    if not isinstance(data, dict):
                        continue
                    stream = str(envelope.get("stream") or "") if isinstance(envelope,dict) else ""
                    symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
                    if not symbol.endswith("USDT") or symbol not in active:
                        continue
                    accepted=False
                    if ROLE == "TRADE":
                        if "p" not in data or "q" not in data:
                            continue
                        accepted=_record_trade(states[symbol],data)
                    else:
                        if not (data.get("bids") or data.get("b")) or not (data.get("asks") or data.get("a")):
                            continue
                        accepted=_record_book(states[symbol],data)
                    if not accepted:
                        continue

                    if PUBLISH_RAW:
                        payload = json.dumps(
                            {"symbol": symbol, "data": data, "worker_ts": now_ms()},
                            separators=(",", ":"),
                        )
                        await r.publish(CHANNEL, payload)
                    events += 1
                    if now - last_snapshot >= SNAPSHOT_INTERVAL:
                        await publish_snapshot(r, sorted(active), states, events, host)
                        last_snapshot = now
                    if now - last_hb >= 3.0:
                        await publish_heartbeat(r, sorted(active), events, host)
                        last_hb = now
                except Exception as exc:
                    print(
                        f"PSI-DISTRIBUTED-MICRO event_error role={ROLE} "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    raise RuntimeError(f"websocket closed role={ROLE} host={host}")


async def main():
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    while True:
        try:
            await r.ping()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(
                f"PSI-DISTRIBUTED-MICRO redis_wait role={ROLE} {type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(RECONNECT_BACKOFF)

    print(f"PSI-DISTRIBUTED-MICRO START version={WORKER_VERSION} role={ROLE}", flush=True)
    host_index = 0
    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while True:
            try:
                wanted=await selected_symbols(r)
                if not wanted:
                    await publish_heartbeat(r, [], 0, "", "waiting_for_control_pool")
                    await asyncio.sleep(CONTROL_POLL_SECONDS)
                    continue

                host=WS_HOSTS[host_index % len(WS_HOSTS)]
                try:
                    await stream_once(r,session,wanted,host)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(
                        f"PSI-DISTRIBUTED-MICRO reconnect role={ROLE} "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )
                    # Rotate hosts only on a real connection failure.
                    host_index=(host_index+1)%len(WS_HOSTS)
                    await asyncio.sleep(RECONNECT_BACKOFF)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"PSI-DISTRIBUTED-MICRO loop_error role={ROLE} "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(RECONNECT_BACKOFF)


if __name__ == "__main__":
    asyncio.run(main())
