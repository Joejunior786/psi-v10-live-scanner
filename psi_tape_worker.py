import asyncio
import json
import os
import time
import math
from typing import List, Tuple
from collections import defaultdict, deque

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.3.0-distributed-tape-2"
REDIS_URL = os.getenv("REDIS_URL", "").strip()
UNIVERSE_KEY = os.getenv("PSI_TAPE_UNIVERSE_KEY", "psi:v12:universe").strip()
SHARD_INDEX = max(0, int(os.getenv("PSI_TAPE_SHARD_INDEX", "0")))
SHARD_COUNT = max(1, int(os.getenv("PSI_TAPE_SHARD_COUNT", "2")))
POLL_SECONDS = max(1.0, float(os.getenv("PSI_TAPE_CONTROL_POLL_SECONDS", "3")))
RECONNECT_BACKOFF = max(0.5, float(os.getenv("PSI_TAPE_RECONNECT_BACKOFF", "1.5")))
TRADE_CHANNEL = "psi:v12:tape-trade"
BOOK_CHANNEL = "psi:v12:tape-book"
HEARTBEAT_KEY = f"psi:v12:tape-worker:{SHARD_INDEX}"
SNAPSHOT_KEY = f"psi:v12:tape-snapshot:{SHARD_INDEX}"
SNAPSHOT_INTERVAL = max(0.5, float(os.getenv("PSI_TAPE_SNAPSHOT_SECONDS", "1.0")))
PUBLISH_RAW = str(os.getenv("PSI_TAPE_PUBLISH_RAW", "0")).strip().lower() in {"1","true","yes","on"}
WINDOW = 35.0
MAX_EVENTS = max(200, int(os.getenv("PSI_TAPE_LOCAL_MAX_EVENTS", "1200")))
WS_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_TAPE_WS_HOSTS",
        "wss://stream.binance.com:9443,wss://data-stream.binance.vision"
    ).split(",")
    if x.strip()
)

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for distributed tape worker")
if SHARD_INDEX >= SHARD_COUNT:
    raise RuntimeError("PSI_TAPE_SHARD_INDEX must be less than PSI_TAPE_SHARD_COUNT")


def now_ms() -> int:
    return int(time.time() * 1000)


def _f(v, default=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _cl(v, a=0.0, b=1.0):
    return max(a, min(b, v))


def _metric_for(dq, book, now_s):
    if not dq:
        return None
    cutoff = now_s - WINDOW
    while dq and dq[0][0] < cutoff:
        dq.popleft()
    if not dq:
        return None
    rows = list(dq)

    n1=b1=s1=c1=0.0
    np=bp=sp=cp=0.0
    n5=b5=s5=c5=0.0
    n15=0.0
    n30=0.0
    w1_first=w1_last=None
    w5_first=w5_last=None

    for stamp, price, notional, is_buy, _event_ms in rows:
        age = now_s - stamp
        if age < 0 or age > 30.0:
            continue
        n30 += notional
        if age <= 15.0:
            n15 += notional
        if age <= 5.0:
            n5 += notional
            c5 += 1
            if is_buy: b5 += notional
            else: s5 += notional
            if w5_first is None: w5_first = price
            w5_last = price
            if age <= 1.0:
                n1 += notional
                c1 += 1
                if is_buy: b1 += notional
                else: s1 += notional
                if w1_first is None: w1_first = price
                w1_last = price
            elif age <= 5.0:
                np += notional
                cp += 1
                if is_buy: bp += notional
                else: sp += notional

    buy1 = b1/n1 if n1>0 else 0.5
    buy5 = b5/n5 if n5>0 else 0.5
    cvd1 = (b1-s1)/n1 if n1>0 else 0.0
    cvd5 = (b5-s5)/n5 if n5>0 else 0.0
    rate_prev = np/4.0
    cnt_prev = cp/4.0
    nacc = (n1/max(rate_prev,1e-9)) if n1>0 and rate_prev>0 else (2.0 if n1>0 else 0.0)
    cacc = (c1/max(cnt_prev,1e-9)) if c1>0 and cnt_prev>0 else (2.0 if c1>0 else 0.0)
    avg1 = n1/max(c1,1.0)
    avgp = np/max(cp,1.0)
    avg_shift = (avg1/max(avgp,1e-9)) if c1>0 and cp>0 else (1.0 if c1 else 0.0)
    pv1 = ((w1_last/w1_first)-1.0)*100.0 if w1_first and w1_last and w1_first>0 and c1>=2 else 0.0
    pv5 = ((w5_last/w5_first)-1.0)*100.0 if w5_first and w5_last and w5_first>0 and c5>=2 else 0.0

    bt = book or {}
    bid=_f(bt.get("bid")); ask=_f(bt.get("ask")); bq=_f(bt.get("bq")); aq=_f(bt.get("aq"))
    spread=((ask-bid)/((ask+bid)/2.0)*10000.0) if bid>0 and ask>=bid else 999.0
    imb=(bq-aq)/(bq+aq) if bq+aq>0 else 0.0
    age_ms=(now_s-rows[-1][0])*1000.0
    book_age_ms=(now_s-_f(bt.get("t"),0.0))*1000.0 if bt else 999999.0
    ready=age_ms<=1500.0 and c5>=3
    score=100.0*(
        .23*_cl((buy1-.50)/.25)+
        .18*_cl((cvd1+.02)/.42)+
        .14*_cl((cvd1-cvd5+.02)/.25)+
        .13*_cl((nacc-.8)/2.2)+
        .11*_cl((cacc-.8)/2.2)+
        .08*_cl((avg_shift-.8)/2.0)+
        .08*_cl((imb+.05)/.55)+
        .05*_cl((3.0-spread)/3.0)
    )
    return {
        "ready": bool(ready),
        "age_ms": round(age_ms, 3),
        "book_age_ms": round(book_age_ms, 3),
        "score": round(score, 2),
        "buy_ratio_1s": buy1,
        "buy_ratio_5s": buy5,
        "cvd_1s": cvd1,
        "cvd_5s": cvd5,
        "cvd_accel": cvd1-cvd5,
        "notional_accel_1s": nacc,
        "trade_count_accel_1s": cacc,
        "avg_trade_shift_1s": avg_shift,
        "price_velocity_1s_pct": pv1,
        "price_velocity_5s_pct": pv5,
        "spread_bps": spread,
        "bbo_imbalance": imb,
        "trades_1s": int(c1),
        "trades_5s": int(c5),
        "notional_1s": n1,
        "notional_5s": n5,
        "notional_15s": n15,
        "notional_30s": n30,
        "last_trade_event_ms": int(rows[-1][4]),
        "last_book_receipt_ms": int(_f(bt.get("t"),0.0)*1000.0) if bt else 0,
    }


async def publish_snapshot(r, symbols, trade_events, bbo, trades, books, host):
    now_s=time.time()
    metrics={}
    last_trade_ms=0
    last_book_ms=0
    for sym in symbols:
        metric=_metric_for(trade_events.get(sym), bbo.get(sym), now_s)
        if metric is None:
            continue
        metrics[sym]=metric
        last_trade_ms=max(last_trade_ms, int(metric.get("last_trade_event_ms") or 0))
        last_book_ms=max(last_book_ms, int(metric.get("last_book_receipt_ms") or 0))
    payload={
        "version": WORKER_VERSION,
        "shard_index": SHARD_INDEX,
        "shard_count": SHARD_COUNT,
        "generated_ms": int(now_s*1000),
        "source_symbols": len(symbols),
        "metric_symbols": len(metrics),
        "trades": trades,
        "books": books,
        "last_trade_event_ms": last_trade_ms,
        "last_book_receipt_ms": last_book_ms,
        "host": host,
        "metrics": metrics,
    }
    await r.set(SNAPSHOT_KEY, json.dumps(payload,separators=(",",":")), ex=15)


async def universe_symbols(r) -> List[str]:
    raw = await r.get(UNIVERSE_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("symbols") or []
    full = []
    seen = set()
    for item in data if isinstance(data, list) else []:
        sym = str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            full.append(sym)
    full.sort()
    return full[SHARD_INDEX::SHARD_COUNT]


def subscription_streams(symbols: List[str]) -> List[str]:
    streams = []
    for sym in symbols:
        low = sym.lower()
        streams.append(f"{low}@aggTrade")
        streams.append(f"{low}@bookTicker")
    return streams


async def publish_heartbeat(r, symbols: List[str], trades: int, books: int, host: str, error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "shard_index": SHARD_INDEX,
        "shard_count": SHARD_COUNT,
        "symbols": len(symbols),
        "trades": trades,
        "books": books,
        "events": trades + books,
        "host": host,
        "last_event_ms": now_ms(),
        "error": error,
    }
    await r.set(HEARTBEAT_KEY, json.dumps(payload, separators=(",", ":")), ex=15)


async def stream_once(r, session: aiohttp.ClientSession, symbols: List[str], host: str):
    streams = subscription_streams(symbols)
    url = f"{host}/ws"
    trades = 0
    books = 0
    last_hb = 0.0
    last_snapshot = 0.0
    trade_events = defaultdict(lambda: deque(maxlen=MAX_EVENTS))
    bbo = {}

    async with session.ws_connect(
        url,
        heartbeat=20,
        receive_timeout=45,
        autoping=True,
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=45),
        max_msg_size=0,
    ) as ws:
        await ws.send_json({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 12000 + SHARD_INDEX,
        })
        print(
            f"PSI-DISTRIBUTED-TAPE connected shard={SHARD_INDEX+1}/{SHARD_COUNT} "
            f"symbols={len(symbols)} streams={len(streams)} host={host}",
            flush=True,
        )
        await publish_heartbeat(r, symbols, trades, books, host)

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    packet = json.loads(msg.data)
                    if isinstance(packet, dict) and packet.get("id") is not None and "result" in packet:
                        continue
                    data = packet.get("data") if isinstance(packet, dict) and isinstance(packet.get("data"), dict) else packet
                    if not isinstance(data, dict):
                        continue
                    event = str(data.get("e") or "")
                    symbol = str(data.get("s") or "").upper()
                    if not symbol.endswith("USDT"):
                        continue

                    if event == "aggTrade":
                        try:
                            price=float(data.get("p") or 0.0)
                            qty=float(data.get("q") or 0.0)
                            event_ms=int(data.get("T") or data.get("E") or now_ms())
                        except (TypeError, ValueError):
                            price=qty=0.0
                            event_ms=0
                        if price>0 and qty>0 and event_ms>0:
                            trade_events[symbol].append(
                                (
                                    event_ms/1000.0,
                                    price,
                                    price*qty,
                                    not bool(data.get("m", False)),
                                    event_ms,
                                )
                            )
                        if PUBLISH_RAW:
                            await r.publish(
                                TRADE_CHANNEL,
                                json.dumps({"symbol": symbol, "data": data, "worker_ts": now_ms()}, separators=(",", ":")),
                            )
                        trades += 1
                    else:
                        is_book = (
                            data.get("u") is not None
                            and data.get("b") is not None
                            and data.get("B") is not None
                            and data.get("a") is not None
                            and data.get("A") is not None
                            and data.get("p") is None
                        )
                        if is_book:
                            try:
                                bid=float(data.get("b") or 0.0)
                                ask=float(data.get("a") or 0.0)
                                bq=float(data.get("B") or 0.0)
                                aq=float(data.get("A") or 0.0)
                            except (TypeError, ValueError):
                                bid=ask=bq=aq=0.0
                            if bid>0 and ask>=bid:
                                bbo[symbol]={"t":time.time(),"bid":bid,"bq":bq,"ask":ask,"aq":aq}
                            if PUBLISH_RAW:
                                await r.publish(
                                    BOOK_CHANNEL,
                                    json.dumps({"symbol": symbol, "data": data, "worker_ts": now_ms()}, separators=(",", ":")),
                                )
                            books += 1

                    now = time.monotonic()
                    if now - last_snapshot >= SNAPSHOT_INTERVAL:
                        await publish_snapshot(r, symbols, trade_events, bbo, trades, books, host)
                        last_snapshot = now
                    if now - last_hb >= 3.0:
                        await publish_heartbeat(r, symbols, trades, books, host)
                        last_hb = now
                except Exception as exc:
                    print(
                        f"PSI-DISTRIBUTED-TAPE event_error shard={SHARD_INDEX} "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    raise RuntimeError(f"websocket closed shard={SHARD_INDEX} host={host}")


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
                f"PSI-DISTRIBUTED-TAPE redis_wait shard={SHARD_INDEX} "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(RECONNECT_BACKOFF)

    print(
        f"PSI-DISTRIBUTED-TAPE START version={WORKER_VERSION} "
        f"shard={SHARD_INDEX+1}/{SHARD_COUNT}",
        flush=True,
    )

    # Start every shard on Binance's data-stream endpoint, which is the
    # most reliable route in this Railway region. Reconnects still rotate
    # through the configured fallback hosts.
    host_index = 0
    active: Tuple[str, ...] = tuple()
    task = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while True:
            try:
                wanted = tuple(await universe_symbols(r))
                if not wanted:
                    if active and task is not None and not task.done():
                        await publish_heartbeat(
                            r, list(active), 0, 0, "", "control_stale_holding_last_universe"
                        )
                        await asyncio.sleep(POLL_SECONDS)
                        continue
                    if task:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass
                        task = None
                    if active:
                        print(
                            f"PSI-DISTRIBUTED-TAPE control_update shard={SHARD_INDEX} symbols=0",
                            flush=True,
                        )
                    active = tuple()
                    await publish_heartbeat(r, [], 0, 0, "", "waiting_for_universe")
                    await asyncio.sleep(POLL_SECONDS)
                    continue

                if wanted != active or task is None or task.done():
                    if wanted != active:
                        print(
                            f"PSI-DISTRIBUTED-TAPE control_update shard={SHARD_INDEX} "
                            f"symbols={len(wanted)} preview={','.join(wanted[:6])}",
                            flush=True,
                        )
                    if task:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass
                    active = wanted
                    host = WS_HOSTS[host_index % len(WS_HOSTS)]
                    host_index += 1
                    task = asyncio.create_task(stream_once(r, session, list(active), host))

                await asyncio.sleep(POLL_SECONDS)
                if task and task.done():
                    try:
                        task.result()
                    except Exception as exc:
                        print(
                            f"PSI-DISTRIBUTED-TAPE reconnect shard={SHARD_INDEX} "
                            f"{type(exc).__name__}: {exc}",
                            flush=True,
                        )
                    task = None
                    await asyncio.sleep(RECONNECT_BACKOFF)
            except asyncio.CancelledError:
                if task:
                    task.cancel()
                raise
            except Exception as exc:
                print(
                    f"PSI-DISTRIBUTED-TAPE loop_error shard={SHARD_INDEX} "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(RECONNECT_BACKOFF)


if __name__ == "__main__":
    asyncio.run(main())
