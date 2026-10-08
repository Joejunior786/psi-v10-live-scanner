"""V15.21: low-latency, read-only, expiring, verified EMA signal delivery.

This module NEVER submits an exchange order. HTTP/SSE output is informational.
The independent EMA evidence engine remains the sole source of EMA BUY truth.
"""
import asyncio
import json
import os
import time
from collections import deque
from threading import Lock
from aiohttp import web

CORE = None
EMA = None
INTERVAL_SECONDS = 2.0
LOG_INTERVAL_SECONDS = max(2.0, float(os.environ.get("PSI_SIGNAL_LOG_INTERVAL_SECONDS", "4")))
MAX_AGE_MS = 3500
SIGNAL_LIFETIME_MS = 3000
MAX_HISTORY = 100
_LOCK = Lock()
_SNAPSHOT = {}
_EVENTS = deque(maxlen=MAX_HISTORY)
_ACTIVE = set()
_NEXT_ID = 0
_STATUS = {"cycles": 0, "errors": 0, "last_error": ""}


def _ms():
    return int(time.time() * 1000)


def _latest_verified(row, now_ms):
    """Recheck all hard live-data conditions at *read* time, not just emission."""
    symbol = row.get("symbol")
    tf = row.get("timeframe")
    if not symbol or tf not in {"1h", "4h", "1d"} or CORE is None:
        return None
    frame = ((getattr(CORE, "_cache", {}) or {}).get(symbol) or {}).get(tf) or {}
    snap = frame.get("snap")
    updated = EMA.num(frame.get("updated"))
    if not isinstance(snap, dict):
        return None
    integrity, micro = EMA.standalone_live_evidence(CORE, symbol, snap, now_ms / 1000)
    if not integrity.get("verified"):
        return None
    trade_price = EMA.num(micro.get("last_price"))
    old_price = EMA.num(snap.get("current"))
    if old_price <= 0 or trade_price <= 0 or abs(trade_price / old_price - 1) > 0.015:
        return None
    for candidate in EMA.evaluate(
        dict(snap, current=trade_price), tf, updated, now_ms / 1000, integrity, micro
    ):
        if (candidate["ema_period"] == row.get("ema_period")
                and candidate["status"] == "BUY NOW — EMA"):
            return dict(symbol=symbol, **candidate)
    return None


def publish_once(now_ms=None):
    """Refresh entire cached EMA opportunity set; no 45s reporting dependency."""
    global _SNAPSHOT, _ACTIVE, _NEXT_ID
    now_ms = _ms() if now_ms is None else int(now_ms)
    records, frames, live_checked = EMA.scan_cached_ema(CORE, now_ms / 1000)
    buys = {}
    technical = set()
    for symbol, item in records:
        if EMA._technical_complete(item) and not symbol.startswith(
            ("XUSD", "BFUSD", "USDC", "USD1", "FDUSD", "TUSD")
        ):
            technical.add(symbol)
        if item.get("status") == "BUY NOW — EMA":
            key = (symbol, item["timeframe"], item["ema_period"])
            buys[key] = dict(symbol=symbol, **item)
    ranked = sorted(records, key=lambda pair: EMA._ema_rank(pair[1]), reverse=True)
    research = []
    research_rows = []
    for symbol, item in ranked:
        if (symbol not in research and not symbol.startswith(
                ("XUSD", "BFUSD", "USDC", "USD1", "FDUSD", "TUSD"))
                and item["touch"] and abs(item["distance_pct"]) <= 1):
            research.append(symbol)
            research_rows.append({
                "symbol": symbol, "timeframe": item["timeframe"],
                "ema_period": item["ema_period"],
                "distance_pct": item["distance_pct"],
                "status": item["status"],
                "seller_exhaustion": item["seller_exhaustion"],
                "buyer_reclaim": item["buyer_reclaim"],
            })
        if len(research) >= 10:
            break
    new_active = set(buys)
    with _LOCK:
        for key in sorted(new_active - _ACTIVE):
            _NEXT_ID += 1
            row = buys[key]
            _EVENTS.append({
                "id": _NEXT_ID, "type": "BUY_NOW_VERIFIED",
                "symbol": row["symbol"], "timeframe": row["timeframe"],
                "ema_period": row["ema_period"],
                "entry": row["entry"], "stop": row["stop"],
                "tp1": row["tp1"], "tp2": row["tp2"], "tp3": row["tp3"],
                "risk_pct": row["risk_pct"],
                "generated_ms": now_ms,
                "expires_ms": now_ms + SIGNAL_LIFETIME_MS,
                "order_placement": False,
            })
        _ACTIVE = new_active
        _SNAPSHOT = {
            "revision": "15.21-live-signal-feed",
            "generated_ms": now_ms,
            "expires_ms": now_ms + SIGNAL_LIFETIME_MS,
            "candle_frames": frames,
            "interactions": len(records),
            "research_top10": research,
            "research_rows": research_rows,
            "technical_ready_symbols": sorted(technical),
            "buy_signals": list(buys.values()),
            "live_evidence_checked": live_checked,
            "last_event_id": _NEXT_ID,
            "order_placement": False,
        }
        _STATUS["cycles"] += 1
    if new_active:
        print(f"Ψ-V15.21 LIVE_BUY_FEED liveBuys={len(new_active)} "
              f"events={_NEXT_ID} generated={now_ms}", flush=True)
    return dict(_SNAPSHOT)


def _fresh_buy_rows(snapshot, now_ms):
    if (not snapshot or now_ms - int(snapshot.get("generated_ms") or 0) > MAX_AGE_MS
            or now_ms >= int(snapshot.get("expires_ms") or 0)):
        return []
    passed = []
    for row in snapshot.get("buy_signals") or []:
        fresh = _latest_verified(row, now_ms)
        if fresh:
            passed.append(fresh)
    return passed


def read_live(now_ms=None):
    now_ms = _ms() if now_ms is None else int(now_ms)
    with _LOCK:
        snap = dict(_SNAPSHOT)
        status = dict(_STATUS)
    current = bool(
        snap and 0 <= now_ms - int(snap.get("generated_ms") or 0) <= MAX_AGE_MS
        and now_ms < int(snap.get("expires_ms") or 0)
    )
    approved = _fresh_buy_rows(snap, now_ms) if current else []
    return {
        "ok": True, "revision": "15.21-live-signal-feed",
        "server_time_ms": now_ms,
        "generated_ms": snap.get("generated_ms"),
        "snapshot_age_ms": now_ms - snap["generated_ms"] if snap.get("generated_ms") else None,
        "expires_ms": snap.get("expires_ms"),
        "fresh": current,
        "status": ("BUY_NOW_VERIFIED" if approved else
                   "NO_VERIFIED_BUY" if current else "DATA_STALE"),
        "buy_count": len(approved),
        "buy_signals": approved,
        "research_top10": snap.get("research_top10", []) if current else [],
        "research_rows": snap.get("research_rows", []) if current else [],
        "technical_ready_symbols": snap.get("technical_ready_symbols", []) if current else [],
        "live_evidence_checked": snap.get("live_evidence_checked", 0) if current else 0,
        "cycles": status["cycles"], "errors": status["errors"],
        "last_error": status["last_error"],
        "order_placement": False,
    }


async def http_live(request):
    return web.json_response(read_live(), headers={"Cache-Control": "no-store, max-age=0"})


async def http_events(request):
    """SSE: immediate new BUY alerts; clients must revalidate /signals/live."""
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream",
                 "Cache-Control": "no-store, no-transform",
                 "Connection": "keep-alive",
                 "X-Accel-Buffering": "no"}
    )
    await response.prepare(request)
    last_id = 0
    try:
        last_id = max(0, int(request.headers.get("Last-Event-ID", "0")))
    except ValueError:
        pass
    try:
        initial = json.dumps(read_live(), separators=(",", ":"), allow_nan=False)
        await response.write(f"event: snapshot\ndata: {initial}\n\n".encode())
        until = time.monotonic() + 110
        while time.monotonic() < until:
            now_ms = _ms()
            with _LOCK:
                new = [dict(e) for e in _EVENTS if e["id"] > last_id and now_ms < e["expires_ms"]]
            if new:
                # Do not stream events that lost their live verification.
                live = read_live()
                live_keys = {(x["symbol"], x["timeframe"], x["ema_period"])
                             for x in live["buy_signals"]}
                for event in new:
                    last_id = max(last_id, event["id"])
                    if (event["symbol"], event["timeframe"], event["ema_period"]) not in live_keys:
                        continue
                    raw = json.dumps(event, separators=(",", ":"), allow_nan=False)
                    await response.write(f"id: {event['id']}\nevent: buy\ndata: {raw}\n\n".encode())
            else:
                await response.write(b": heartbeat\n\n")
            await asyncio.sleep(1.0)
    except (ConnectionError, ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        try:
            await response.write_eof()
        except (ConnectionError, ConnectionResetError, RuntimeError):
            pass
    return response


def signal_tick_line(snapshot, live, cycle_ms=0):
    """Timestamped fast-lane status for environments that cannot GET HTTP URLs.

    Railway log delivery is a transport fallback, not a claim that the signal
    will remain valid when a reader receives the log several seconds later.
    """
    if live.get("buy_count"):
        verified = [{
            "symbol": row.get("symbol"),
            "timeframe": row.get("timeframe"),
            "ema_period": row.get("ema_period"),
            "entry": row.get("entry"), "stop": row.get("stop"),
            "tp1": row.get("tp1"), "tp2": row.get("tp2"), "tp3": row.get("tp3")
        } for row in live.get("buy_signals") or []]
    else:
        verified = []
    report = {
        "generated_ms": live.get("generated_ms"),
        "verified_at_ms": live.get("server_time_ms"),
        "snapshot_age_ms": live.get("snapshot_age_ms"),
        "expires_ms": live.get("expires_ms"),
        "status_at_generation": live.get("status"),
        "buy_count": live.get("buy_count", 0),
        "buys": verified,
        "candle_frames": snapshot.get("candle_frames", 0),
        "interactions": snapshot.get("interactions", 0),
        "live_evidence_checked": live.get("live_evidence_checked", 0),
        "technical_ready": live.get("technical_ready_symbols", []),
        "research": live.get("research_top10", []),
        "research_rows": live.get("research_rows", [])[:10],
        "cycle_ms": cycle_ms,
        "worker_errors": live.get("errors", 0),
        "read_only": True,
    }
    return "Ψ-V15.22 SIGNAL_TICK " + json.dumps(
        report, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


async def supervisor_loop():
    last_log = 0.0
    while True:
        try:
            start = time.monotonic()
            snap = await asyncio.to_thread(publish_once)
            completed = time.monotonic()
            if completed - last_log >= LOG_INTERVAL_SECONDS or snap.get("buy_signals"):
                # Re-validate right before emitting; no BUY may survive lost
                # book/trade evidence merely because a prior tick approved it.
                live = await asyncio.to_thread(read_live)
                print(signal_tick_line(snap, live, int(
                    (time.monotonic() - start) * 1000
                )), flush=True)
                last_log = completed
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with _LOCK:
                _STATUS["errors"] += 1
                _STATUS["last_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
            print(f"Ψ-V15.22 LIVE_FEED_ERROR {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(INTERVAL_SECONDS)


def install(core, ema):
    global CORE, EMA
    CORE, EMA = core, ema
    core.app.fast_signal_handler = http_live
    core.app.fast_events_handler = http_events
    print("Ψ-V15.21 LIVE_SIGNAL_DELIVERY installed read-only HTTP+SSE expiry=3s", flush=True)
