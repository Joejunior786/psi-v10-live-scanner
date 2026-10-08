"""V15.21: low-latency, read-only, expiring, verified EMA signal delivery.

This module NEVER submits an exchange order. HTTP/SSE output is informational.
The independent EMA evidence engine remains the sole source of EMA BUY truth.
"""
import asyncio
import json
import os
import time
import re
from collections import deque
from threading import Lock
from aiohttp import web

CORE = None
EMA = None
INTERVAL_SECONDS = 1.0
LOG_INTERVAL_SECONDS = max(2.0, float(os.environ.get("PSI_SIGNAL_LOG_INTERVAL_SECONDS", "4")))
MAX_AGE_MS = 3500
SIGNAL_LIFETIME_MS = 3000
MAX_HISTORY = 100
_LOCK = Lock()
_PUBLISH_LOCK = Lock()
_SNAPSHOT = {}
_EVENTS = deque(maxlen=MAX_HISTORY)
_ACTIVE = set()
_NEXT_ID = 0
_STATUS = {"cycles": 0, "errors": 0, "last_error": ""}


def _ms():
    return int(time.time() * 1000)


def _latest_verified(row, now_ms, strict_micro_age_ms=None):
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
    if strict_micro_age_ms is not None:
        if not (0 <= integrity["trade_age_ms"] <= strict_micro_age_ms
                and 0 <= integrity["book_age_ms"] <= strict_micro_age_ms):
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
            result = dict(symbol=symbol, **candidate)
            if strict_micro_age_ms is not None:
                result["quote_checked_ms"] = now_ms
                result["trade_age_ms"] = integrity["trade_age_ms"]
                result["book_age_ms"] = integrity["book_age_ms"]
                result["quote_expires_ms"] = min(
                    int(now_ms + SIGNAL_LIFETIME_MS),
                    int(EMA.num(micro.get("last_trade_ms")) + strict_micro_age_ms),
                    int(EMA.num(micro.get("last_book_ms")) + strict_micro_age_ms),
                )
                if result["quote_expires_ms"] <= now_ms:
                    return None
            return result
    return None


def _publish_once_unlocked(now_ms=None):
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
            "revision": "15.23-live-signal-feed",
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


def publish_once(now_ms=None):
    """Serialise refresh requests and supervisor publications."""
    with _PUBLISH_LOCK:
        return _publish_once_unlocked(now_ms)


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
        "ok": True, "revision": "15.23-live-signal-feed",
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



async def _live_checked():
    """Get a truly current snapshot; a stale cache triggers a fresh evaluation."""
    live = await asyncio.to_thread(read_live)
    if not live["fresh"]:
        await asyncio.to_thread(publish_once)
        live = await asyncio.to_thread(read_live)
    return live


async def http_live(request):
    return web.json_response(
        await _live_checked(), headers={"Cache-Control": "no-store, max-age=0"}
    )


async def http_quote(request):
    """Read-only manual quote check: do not extend source-data or market TTLs."""
    symbol = (request.query.get("symbol") or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,24}USDT", symbol):
        return web.json_response(
            {"ok": False, "status": "INVALID_SYMBOL", "buy_count": 0,
             "quotes": [], "order_placement": False},
            status=400, headers={"Cache-Control": "no-store"}
        )
    # An on-demand request cannot depend on the previous reporter tick.
    await asyncio.to_thread(publish_once)
    checked_ms = _ms()
    with _LOCK:
        snap = dict(_SNAPSHOT)
    active = (
        snap and 0 <= checked_ms - int(snap.get("generated_ms") or 0) <= MAX_AGE_MS
        and checked_ms < int(snap.get("expires_ms") or 0)
    )
    quotes = []
    if active:
        for row in snap.get("buy_signals") or []:
            if row.get("symbol") != symbol:
                continue
            verified = _latest_verified(row, checked_ms, strict_micro_age_ms=1200)
            if verified is not None:
                # The source snapshot may expire earlier than the micro lease.
                verified["quote_expires_ms"] = min(
                    verified["quote_expires_ms"], int(snap["expires_ms"])
                )
                if checked_ms < verified["quote_expires_ms"]:
                    quotes.append(verified)
    return web.json_response({
        "ok": True, "revision": "15.23-on-demand-verified-quote",
        "symbol": symbol, "server_time_ms": checked_ms,
        "generated_ms": snap.get("generated_ms"),
        "expires_ms": snap.get("expires_ms"),
        "status": ("VERIFIED_AT_READ" if quotes else
                   "NO_VERIFIED_BUY" if active else "DATA_STALE"),
        "buy_count": len(quotes), "quotes": quotes,
        "order_placement": False,
        "disclaimer": "Read-only observation; venue conditions can change before an order.",
    }, headers={"Cache-Control": "no-store, max-age=0"})


_DASHBOARD = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PSI Live Signal Board</title>
<style>
:root{color-scheme:dark;font-family:system-ui,sans-serif}
body{background:#0e1620;color:#e9f1f5;margin:auto;max-width:1100px;padding:22px}
h1{font-size:1.5rem}h2{font-size:1.05rem;margin-top:26px}
p{color:#b7c6d0}table{width:100%;border-collapse:collapse;font-size:0.9rem}
td,th{border-bottom:1px solid #293746;text-align:left;padding:9px 7px}
button{cursor:pointer;padding:8px 12px;background:#183c54;border:1px solid #6b97b0;
color:#fff;border-radius:6px}button:disabled{opacity:.4;cursor:default}
.good{color:#75e5bb}.warn{color:#ffcd77}.bad{color:#ff9696}
code{word-break:break-word}#status,#quote{padding:12px;background:#182635;border-radius:7px}
</style></head><body>
<h1>Ψ Live Signal Board · V15.23</h1>
<p>Auto-refreshes live market checks. Historical logs are never executable quotes.
Every entry requires a new server-side integrity check; this page never submits orders.</p>
<div id="status" role="status" aria-live="polite">Connecting…</div>
<h2>Verified scanner signals</h2>
<table><thead><tr><th>Pair</th><th>Frame</th><th>Entry</th><th>Stop</th><th>Target 1</th><th>Action</th></tr></thead>
<tbody id="signals"><tr><td colspan="6">Fetching live market checks…</td></tr></tbody></table>
<h2>On-demand quote check</h2><div id="quote" aria-live="polite">Select Verify on an active signal.</div>
<h2>Developing setups (not buy instructions)</h2>
<table><thead><tr><th>Pair</th><th>Stage</th><th>Frame</th><th>EMA</th><th>Distance</th></tr></thead>
<tbody id="research"></tbody></table>
<script>
"use strict";
const status=document.getElementById("status");
const signals=document.getElementById("signals");
const research=document.getElementById("research");
const quote=document.getElementById("quote");
let aliveUntil=0, token=0, requestPending=false;
function cell(row,value){const td=document.createElement("td");
  td.textContent=value==null?"—":String(value);row.appendChild(td);return td;}
function money(v){return typeof v==="number"?Number(v.toPrecision(9)).toString():"—";}
function clear(el){while(el.firstChild)el.removeChild(el.firstChild);}
function invalidate(){aliveUntil=0;clear(signals);
  let r=signals.insertRow();cell(r,"No verified active signal — do not trade from this page");}
async function verify(symbol){
  const started=performance.now();
  quote.textContent="Revalidating "+symbol+" against live trade/book evidence…";
  try{
    const r=await fetch("/signals/quote?symbol="+encodeURIComponent(symbol),
      {cache:"no-store",signal:AbortSignal.timeout(2500)});
    if(!r.ok)throw Error("HTTP "+r.status);
    const d=await r.json();
    const lease=Math.max(0,Math.min(...(d.quotes||[]).map(x=>x.quote_expires_ms))
      - d.server_time_ms - (performance.now()-started) - 150);
    if(d.status!=="VERIFIED_AT_READ"||!d.quotes.length||lease<=0){
      quote.textContent="NO EXECUTABLE QUOTE: "+d.status+". Wait for a new live confirmation.";
      return;
    }
    const q=d.quotes[0];
    quote.textContent=symbol+" live-verified at "+new Date(d.server_time_ms).toLocaleTimeString()
      +" | Entry "+money(q.entry)+" | Stop "+money(q.stop)+" | TP1 "+money(q.tp1)
      +" | TP2 "+money(q.tp2)+" | TP3 "+money(q.tp3)
      +" | Quote lease remaining "+Math.floor(lease)+"ms"
      +" | Recheck the exchange before submitting. No order has been placed.";
    setTimeout(()=>{quote.textContent="QUOTE EXPIRED: verify again before considering any order.";},lease);
  }catch(e){quote.textContent="QUOTE UNAVAILABLE: "+String(e.message)+"; no order permission.";}
}
async function refresh(){
  if(requestPending)return;
  requestPending=true;
  const id=++token, started=performance.now();
  try{
    const response=await fetch("/signals/live?nocache="+Date.now(),
      {cache:"no-store",signal:AbortSignal.timeout(2600)});
    if(!response.ok)throw Error("HTTP "+response.status);
    const d=await response.json();
    if(id!==token)return;
    aliveUntil=performance.now()+Math.max(0,
      (d.expires_ms||0)-(d.server_time_ms||0)-(performance.now()-started)-150);
    const valid=d.fresh&&performance.now()<aliveUntil;
    status.className=valid?"good":"bad";
    status.textContent=(valid?"LIVE: ":"STALE: ")+d.status
      +" | verified buys "+(valid?d.buy_count:0)
      +" | scan age "+d.snapshot_age_ms+"ms | cycles "+d.cycles
      +" | checked "+new Date(d.server_time_ms).toLocaleTimeString();
    clear(signals);
    if(valid&&d.buy_signals&&d.buy_signals.length){
      for(const q of d.buy_signals){
        const row=signals.insertRow();
        cell(row,q.symbol);cell(row,q.timeframe+" EMA"+q.ema_period);
        cell(row,money(q.entry));cell(row,money(q.stop));cell(row,money(q.tp1));
        const c=row.insertCell(), b=document.createElement("button");
        b.textContent="Verify quote";b.onclick=()=>verify(q.symbol);
        c.appendChild(b);
      }
    }else{let row=signals.insertRow();
      cell(row,valid?"No BUY NOW signal qualified":"Snapshot expired — waiting for refresh");
      row.firstChild.colSpan=6;}
    clear(research);
    if(valid)for(const q of d.research_rows||[]){
      const row=research.insertRow();cell(row,q.symbol);cell(row,q.status);
      cell(row,q.timeframe);cell(row,"EMA"+q.ema_period);
      cell(row,q.distance_pct+"%");
    }
  }catch(e){status.className="bad";status.textContent="CONNECTION UNAVAILABLE: "+e.message;
    invalidate();clear(research);}
  finally{requestPending=false;setTimeout(refresh,850);}
}
setInterval(()=>{if(aliveUntil&&performance.now()>=aliveUntil){
  status.className="bad";status.textContent="EXPIRED — refreshing; no active quote";
  invalidate();}},150);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)refresh();});
refresh();
</script></body></html>"""


async def http_dashboard(request):
    return web.Response(
        text=_DASHBOARD, content_type="text/html",
        headers={"Cache-Control": "no-store, max-age=0",
                 "X-Content-Type-Options": "nosniff",
                 "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'"},
    )


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
    return "Ψ-V15.23 SIGNAL_TICK " + json.dumps(
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
            print(f"Ψ-V15.23 LIVE_FEED_ERROR {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(INTERVAL_SECONDS)


def install(core, ema):
    global CORE, EMA
    CORE, EMA = core, ema
    core.app.fast_signal_handler = http_live
    core.app.fast_events_handler = http_events
    core.app.fast_quote_handler = http_quote
    core.app.fast_dashboard_handler = http_dashboard
    print("Ψ-V15.23 LIVE_SIGNAL_DELIVERY installed read-only HTTP+SSE+QUOTE+BOARD strict-quote-micro=1200ms expiry=3s", flush=True)
