import asyncio
import json
import os
import time
from collections import defaultdict, deque

import aiohttp
import redis.asyncio as redis

VERSION = "12.5.0-full-universe-combined-sensor"
REDIS_URL = os.getenv("REDIS_URL", "").strip()
SHARD_INDEX = int(os.getenv("PSI_SENSOR_SHARD_INDEX", "0"))
SHARD_COUNT = max(1, int(os.getenv("PSI_SENSOR_SHARD_COUNT", "4")))
MAX_SYMBOLS = max(25, min(int(os.getenv("PSI_SENSOR_MAX_SYMBOLS", "140")), 140))
SNAPSHOT_INTERVAL = max(0.25, float(os.getenv("PSI_SENSOR_SNAPSHOT_SECONDS", "0.5")))
SNAPSHOT_TTL = max(5, int(os.getenv("PSI_SENSOR_SNAPSHOT_TTL_SECONDS", "15")))
UNIVERSE_REFRESH_SECONDS = max(300, int(os.getenv("PSI_SENSOR_UNIVERSE_REFRESH_SECONDS", "1800")))
SLIPPAGE_TEST_NOTIONAL = max(1.0, float(os.getenv("SLIPPAGE_TEST_NOTIONAL", "1000")))
RECONNECT_BACKOFF = max(0.5, float(os.getenv("PSI_SENSOR_RECONNECT_BACKOFF", "1.5")))
WS_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_SENSOR_WS_HOSTS",
        "wss://stream.binance.com:9443,wss://data-stream.binance.vision",
    ).split(",")
    if x.strip()
)
REST_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_SENSOR_REST_HOSTS",
        "https://data-api.binance.vision,https://api.binance.com,https://api1.binance.com,https://api2.binance.com",
    ).split(",")
    if x.strip()
)

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required")
if SHARD_INDEX < 0 or SHARD_INDEX >= SHARD_COUNT:
    raise RuntimeError("PSI_SENSOR_SHARD_INDEX must be in [0, PSI_SENSOR_SHARD_COUNT)")

SNAPSHOT_KEY = f"psi:v12.5:sensor:{SHARD_INDEX}"
HEARTBEAT_KEY = f"psi:v12.5:sensor-heartbeat:{SHARD_INDEX}"


def now_ms():
    return int(time.time() * 1000)


def f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def div(a, b, default=0.0):
    try:
        return a / b if b else default
    except Exception:
        return default


def avg(values):
    values = list(values or [])
    return sum(values) / len(values) if values else 0.0


def trade_state():
    return {
        "buckets": deque(maxlen=190),
        "last_agg_id": None,
        "sequence_samples": 0,
        "sequence_ok": True,
        "last_trade_ms": 0,
    }


def book_state():
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


def symbol_state():
    return {"trade": trade_state(), "book": book_state()}


def record_trade(state, data):
    price = f(data.get("p"))
    qty = f(data.get("q"))
    if price <= 0 or qty <= 0:
        return False
    ts = int(data.get("T") or data.get("E") or now_ms())
    aid_raw = data.get("a")
    if aid_raw is not None:
        try:
            aid = int(aid_raw)
        except (TypeError, ValueError):
            state["sequence_ok"] = False
            return False
        last = state.get("last_agg_id")
        if last is not None:
            if aid <= int(last):
                return False
            state["sequence_samples"] += 1
        state["last_agg_id"] = aid
    quote = price * qty
    is_buy = not bool(data.get("m", False))
    signed = quote if is_buy else -quote
    sec = ts // 1000
    buckets = state["buckets"]
    if buckets and buckets[-1]["sec"] == sec:
        row = buckets[-1]
    else:
        row = {
            "sec": sec,
            "first_ms": ts,
            "last_ms": ts,
            "signed": 0.0,
            "quote": 0.0,
            "buy_quote": 0.0,
            "qty": 0.0,
            "count": 0,
            "buy_count": 0,
            "first_price": price,
            "last_price": price,
        }
        buckets.append(row)
    row["last_ms"] = max(int(row.get("last_ms", ts)), ts)
    row["signed"] += signed
    row["quote"] += quote
    row["buy_quote"] += quote if is_buy else 0.0
    row["qty"] += qty
    row["count"] += 1
    row["buy_count"] += 1 if is_buy else 0
    row["last_price"] = price
    state["last_trade_ms"] = max(int(state.get("last_trade_ms", 0)), ts)
    return True


def trade_window(buckets, now, seconds, offset=0):
    hi = now - int(offset * 1000)
    lo = hi - int(seconds * 1000)
    return [x for x in buckets if lo <= int(x.get("last_ms", 0)) <= hi]


def agg_buckets(rows):
    if not rows:
        return {
            "signed": 0.0,
            "quote": 0.0,
            "buy_quote": 0.0,
            "qty": 0.0,
            "count": 0,
            "buy_count": 0,
            "last_price": 0.0,
        }
    return {
        "signed": sum(f(x.get("signed")) for x in rows),
        "quote": sum(f(x.get("quote")) for x in rows),
        "buy_quote": sum(f(x.get("buy_quote")) for x in rows),
        "qty": sum(f(x.get("qty")) for x in rows),
        "count": sum(int(x.get("count", 0)) for x in rows),
        "buy_count": sum(int(x.get("buy_count", 0)) for x in rows),
        "last_price": f(rows[-1].get("last_price")),
    }


def trade_metrics(state, now):
    buckets = list(state["buckets"])
    recent = agg_buckets(trade_window(buckets, now, 60))
    first30 = agg_buckets(trade_window(buckets, now, 30, 30))
    last30 = agg_buckets(trade_window(buckets, now, 30))
    prev60 = agg_buckets(trade_window(buckets, now, 60, 60))
    last10 = agg_buckets(trade_window(buckets, now, 10))
    prev10 = agg_buckets(trade_window(buckets, now, 10, 10))
    avg_first = div(first30["quote"], first30["count"], 0.0)
    avg_last = div(last30["quote"], last30["count"], 0.0)
    last_ms = int(state.get("last_trade_ms", 0) or 0)
    age_ms = max(0, now - last_ms) if last_ms > 0 else 999999999
    return {
        "last_trade_ms": last_ms,
        "trade_age_ms": age_ms,
        "trade_fresh": bool(last_ms > 0 and age_ms <= 3000),
        "sequence_verified": bool(state.get("sequence_ok", False) and int(state.get("sequence_samples", 0)) >= 3),
        "cvd_quote_60s": recent["signed"],
        "cvd_acceleration": last30["signed"] - first30["signed"],
        "aggressive_buy_ratio": div(recent["buy_quote"], recent["quote"], 0.5),
        "trade_count_60s": int(recent["count"]),
        "trade_acceleration": div(last30["count"], max(first30["count"], 1), 0.0),
        "trade_size_shift": div(avg_last, max(avg_first, 1e-9), 0.0),
        "relative_volume_10s": div(last10["quote"], max(prev10["quote"], 1e-9), 0.0),
        "relative_volume_30s": div(last30["quote"], max(prev60["quote"] / 2.0, 1e-9), 0.0),
        "flow_persistence": div(last30["buy_count"], last30["count"], 0.0),
        "last_price": recent["last_price"],
    }


def depth_notional(levels):
    return sum(float(p) * float(q) for p, q in levels)


def estimate_slippage(asks, notional):
    if not asks or notional <= 0:
        return None
    remaining = float(notional)
    spent = 0.0
    base = 0.0
    best = float(asks[0][0])
    for price, qty in asks:
        price = float(price)
        qty = float(qty)
        level_quote = price * qty
        take = min(remaining, level_quote)
        if take > 0:
            spent += take
            base += take / price
            remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or base <= 0 or best <= 0:
        return None
    return ((spent / base) / best - 1.0) * 10000.0


def record_book(state, data):
    try:
        update_id = int(data.get("lastUpdateId") or data.get("u") or 0)
    except (TypeError, ValueError):
        return False
    raw_bids = data.get("bids") or data.get("b") or []
    raw_asks = data.get("asks") or data.get("a") or []
    if update_id <= 0 or not raw_bids or not raw_asks:
        return False
    last = state.get("last_update_id")
    if last is not None and update_id <= int(last):
        return False
    bids = [(f(px), f(qty)) for px, qty in raw_bids[:20] if f(px) > 0 and f(qty) > 0]
    asks = [(f(px), f(qty)) for px, qty in raw_asks[:20] if f(px) > 0 and f(qty) > 0]
    if not bids or not asks:
        state["sequence_ok"] = False
        return False
    prev_bids = list(state.get("bids") or [])
    prev_asks = list(state.get("asks") or [])
    ts = now_ms()
    bid_notional = depth_notional(bids)
    ask_notional = depth_notional(asks)
    obi = div(bid_notional - ask_notional, bid_notional + ask_notional, 0.0)
    mid = (bids[0][0] + asks[0][0]) / 2.0
    spread = div(asks[0][0] - bids[0][0], mid, 0.0) * 10000.0
    slip = estimate_slippage(asks, SLIPPAGE_TEST_NOTIONAL)
    state["obi"].append((ts, obi))
    state["spread"].append((ts, spread))
    if slip is not None:
        state["slip"].append((ts, slip))
    if prev_bids and prev_asks:
        pb, pa, cb, ca = dict(prev_bids), dict(prev_asks), dict(bids), dict(asks)
        bid_change = sum(p * (cb.get(p, 0.0) - pb.get(p, 0.0)) for p in set(pb) | set(cb))
        ask_change = sum(p * (ca.get(p, 0.0) - pa.get(p, 0.0)) for p in set(pa) | set(ca))
        ofi = div(bid_change - ask_change, abs(bid_change) + abs(ask_change), 0.0)
        prev_ask = depth_notional(prev_asks)
        prev_bid = depth_notional(prev_bids)
        state["ofi"].append((ts, ofi))
        state["ask_dep"].append((ts, div(prev_ask - ask_notional, prev_ask, 0.0)))
        state["bid_dep"].append((ts, div(prev_bid - bid_notional, prev_bid, 0.0)))
    state["bids"] = bids
    state["asks"] = asks
    state["last_update_id"] = update_id
    state["sequence_ok"] = True
    state["sequence_samples"] += 1
    state["book_updates"] += 1
    state["last_book_ms"] = ts
    cutoff = ts - 120000
    for key in ("ofi", "obi", "ask_dep", "bid_dep", "spread", "slip"):
        dq = state[key]
        while dq and int(dq[0][0]) < cutoff:
            dq.popleft()
    return True


def recent_values(state, key, now, seconds=60):
    cutoff = now - int(seconds * 1000)
    return [float(v) for ts, v in state[key] if int(ts) >= cutoff]


def book_metrics(state, now):
    ofis = recent_values(state, "ofi", now)
    obis = recent_values(state, "obi", now)
    asks = recent_values(state, "ask_dep", now)
    bids = recent_values(state, "bid_dep", now)
    spreads = recent_values(state, "spread", now)
    slips = recent_values(state, "slip", now)
    half = max(1, len(ofis) // 2)
    last_ms = int(state.get("last_book_ms", 0) or 0)
    age_ms = max(0, now - last_ms) if last_ms > 0 else 999999999
    return {
        "last_book_ms": last_ms,
        "book_age_ms": age_ms,
        "book_fresh": bool(last_ms > 0 and age_ms <= 1500),
        "book_sequence_verified": bool(state.get("sequence_ok", False) and int(state.get("sequence_samples", 0)) >= 3),
        "book_updates": int(state.get("book_updates", 0)),
        "ofi": avg(ofis[-20:]),
        "ofi_acceleration": avg(ofis[half:]) - avg(ofis[:half]) if len(ofis) >= 6 else 0.0,
        "ofi_persistence": div(sum(1 for x in ofis[-20:] if x > 0), len(ofis[-20:]), 0.0),
        "obi": avg(obis[-20:]),
        "ask_depletion": avg(asks[-20:]),
        "bid_depletion": avg(bids[-20:]),
        "spread_bps": spreads[-1] if spreads else None,
        "slippage_bps": slips[-1] if slips else None,
    }


async def fetch_universe(session):
    last_error = None
    for host in REST_HOSTS:
        try:
            async with session.get(
                f"{host}/api/v3/exchangeInfo",
                timeout=aiohttp.ClientTimeout(total=10, connect=3),
            ) as resp:
                body = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                payload = json.loads(body)
                symbols = []
                for row in payload.get("symbols", []):
                    sym = str(row.get("symbol") or "").upper()
                    if (
                        sym.endswith("USDT")
                        and row.get("status") == "TRADING"
                        and row.get("quoteAsset") == "USDT"
                        and row.get("isSpotTradingAllowed", True)
                    ):
                        symbols.append(sym)
                symbols = sorted(set(symbols))
                shard = symbols[SHARD_INDEX::SHARD_COUNT][:MAX_SYMBOLS]
                if shard:
                    print(
                        f"PSI-SENSOR UNIVERSE total={len(symbols)} shard={SHARD_INDEX}/{SHARD_COUNT} "
                        f"symbols={len(shard)} host={host}",
                        flush=True,
                    )
                    return shard
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{exc}"
    raise RuntimeError(f"unable to fetch universe: {last_error}")


def streams(symbols):
    out = []
    for symbol in symbols:
        s = symbol.lower()
        out.append(f"{s}@aggTrade")
        out.append(f"{s}@depth20@100ms")
    return out


async def subscribe(ws, symbols, request_id):
    params = streams(symbols)
    for i in range(0, len(params), 180):
        await ws.send_json({"method": "SUBSCRIBE", "params": params[i:i+180], "id": request_id})
        request_id += 1
    return request_id


async def publish_snapshot(r, symbols, states, events, host):
    now = now_ms()
    metrics = {}
    for sym in symbols:
        state = states.get(sym)
        if not state:
            continue
        row = {}
        row.update(trade_metrics(state["trade"], now))
        row.update(book_metrics(state["book"], now))
        metrics[sym] = row
    payload = {
        "version": VERSION,
        "shard_index": SHARD_INDEX,
        "shard_count": SHARD_COUNT,
        "generated_ms": now,
        "source_symbols": len(symbols),
        "metric_symbols": len(metrics),
        "events": events,
        "host": host,
        "metrics": metrics,
    }
    await r.set(SNAPSHOT_KEY, json.dumps(payload, separators=(",", ":")), ex=SNAPSHOT_TTL)


async def publish_heartbeat(r, symbols, events, host, error=""):
    await r.set(
        HEARTBEAT_KEY,
        json.dumps(
            {
                "version": VERSION,
                "shard_index": SHARD_INDEX,
                "shard_count": SHARD_COUNT,
                "symbols": len(symbols),
                "events": events,
                "host": host,
                "last_event_ms": now_ms(),
                "error": error,
            },
            separators=(",", ":"),
        ),
        ex=15,
    )


async def stream_once(r, session, symbols, host):
    url = f"{host}/ws"
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=30)
    states = defaultdict(symbol_state)
    symbol_set = set(symbols)
    events = 0
    last_snapshot = 0.0
    last_hb = 0.0
    started = time.monotonic()
    last_diag = 0.0
    async with session.ws_connect(
        url,
        heartbeat=15,
        receive_timeout=30,
        autoping=True,
        timeout=timeout,
        max_msg_size=8 * 1024 * 1024,
    ) as ws:
        req = 1
        await ws.send_json({"method": "SET_PROPERTY", "params": ["combined", True], "id": req})
        req += 1
        req = await subscribe(ws, symbols, req)
        print(
            f"PSI-SENSOR CONNECTED shard={SHARD_INDEX}/{SHARD_COUNT} symbols={len(symbols)} "
            f"streams={len(symbols)*2} host={host}",
            flush=True,
        )
        await publish_heartbeat(r, symbols, events, host)
        async for msg in ws:
            mono = time.monotonic()
            if mono - started >= UNIVERSE_REFRESH_SECONDS:
                raise RuntimeError("scheduled_universe_refresh")
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    envelope = json.loads(msg.data)
                    if isinstance(envelope, dict) and envelope.get("id") is not None and "result" in envelope:
                        continue
                    data = envelope.get("data") if isinstance(envelope, dict) else None
                    if not isinstance(data, dict):
                        continue
                    stream = str(envelope.get("stream") or "")
                    symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
                    if symbol not in symbol_set:
                        continue
                    accepted = False
                    if stream.endswith("@aggTrade") or ("p" in data and "q" in data and "m" in data):
                        accepted = record_trade(states[symbol]["trade"], data)
                    elif "@depth" in stream or data.get("lastUpdateId") or data.get("u"):
                        accepted = record_book(states[symbol]["book"], data)
                    if not accepted:
                        continue
                    events += 1
                    if mono - last_snapshot >= SNAPSHOT_INTERVAL:
                        await publish_snapshot(r, symbols, states, events, host)
                        last_snapshot = mono
                    if mono - last_hb >= 3.0:
                        await publish_heartbeat(r, symbols, events, host)
                        last_hb = mono
                    if mono - last_diag >= 10.0:
                        fresh_trade = 0
                        fresh_book = 0
                        metric_symbols = 0
                        now = now_ms()
                        for sym in symbols:
                            st = states.get(sym)
                            if not st:
                                continue
                            metric_symbols += 1
                            tm = trade_metrics(st["trade"], now)
                            bm = book_metrics(st["book"], now)
                            fresh_trade += int(bool(tm.get("trade_fresh")))
                            fresh_book += int(bool(bm.get("book_fresh")))
                        print(
                            f"PSI-SENSOR LIVE shard={SHARD_INDEX}/{SHARD_COUNT} "
                            f"symbols={len(symbols)} metrics={metric_symbols} events={events} "
                            f"tradeFresh={fresh_trade} bookFresh={fresh_book} host={host}",
                            flush=True,
                        )
                        last_diag = mono
                except Exception as exc:
                    print(
                        f"PSI-SENSOR EVENT_ERROR shard={SHARD_INDEX} {type(exc).__name__}:{exc}",
                        flush=True,
                    )
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
    raise RuntimeError("websocket_closed")


async def main():
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    while True:
        try:
            await r.ping()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"PSI-SENSOR REDIS_WAIT shard={SHARD_INDEX} {type(exc).__name__}:{exc}", flush=True)
            await asyncio.sleep(RECONNECT_BACKOFF)

    print(
        f"PSI-SENSOR START version={VERSION} shard={SHARD_INDEX}/{SHARD_COUNT}",
        flush=True,
    )
    host_index = SHARD_INDEX % max(1, len(WS_HOSTS))
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while True:
            try:
                symbols = await fetch_universe(session)
                host = WS_HOSTS[host_index % len(WS_HOSTS)]
                await stream_once(r, session, symbols, host)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"PSI-SENSOR RECONNECT shard={SHARD_INDEX} {type(exc).__name__}:{exc}",
                    flush=True,
                )
                try:
                    await publish_heartbeat(r, [], 0, "", f"{type(exc).__name__}:{exc}")
                except Exception:
                    pass
                host_index = (host_index + 1) % max(1, len(WS_HOSTS))
                await asyncio.sleep(RECONNECT_BACKOFF)


if __name__ == "__main__":
    asyncio.run(main())
