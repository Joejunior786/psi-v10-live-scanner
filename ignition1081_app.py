import asyncio
import json
import time
from collections import Counter

import aiohttp
import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8

VERSION = "10.8.2-extension-telemetry-fix"

FRESH_MAX_PCT = 10.0
CONTROLLED_RUNNER_MAX_PCT = 20.0
EXCEPTIONAL_RUNNER_MAX_PCT = 35.0
MAX_LATE_RUNNER_SCORE = 45.0
MAX_CONTROLLED_BLOCK_SCORE = 68.0
MAX_EXCEPTIONAL_BLOCK_SCORE = 58.0

# The all-market ticker stream is a feed-level heartbeat. Individual symbols do
# not have to print every second, so a 5-second per-symbol age must never be
# interpreted as proof that a coin is over-extended.
EXTENSION_FEED_MAX_AGE_SECONDS = 10.0
EXTENSION_SNAPSHOT_MAX_AGE_SECONDS = 120.0

market_24h = {}
extension_ticker_connected = False
extension_ticker_last_message_ts = 0.0
extension_ticker_last_count = 0

v8.VERSION = VERSION
v7.VERSION = VERSION
base.VERSION = VERSION
app.USER_AGENT = "psi-v10-live-scanner/10.8.2-extension-telemetry-fix"

_original_evaluate = v8.evaluate


def _pct_from_high(price, high):
    if not price or not high or high <= 0 or price >= high:
        return 0.0
    return (high - price) / high * 100.0


def _extension_tier(change_pct):
    if change_pct <= FRESH_MAX_PCT:
        return "FRESH"
    if change_pct <= CONTROLLED_RUNNER_MAX_PCT:
        return "CONTROLLED_RUNNER"
    if change_pct <= EXCEPTIONAL_RUNNER_MAX_PCT:
        return "EXCEPTIONAL_RUNNER"
    return "LATE_RUNNER"


def _reset_required(extension_pct):
    return max(5.0, min(12.0, extension_pct * 0.20))


def _telemetry_status(sym):
    now_t = time.time()
    t = market_24h.get(sym) or {}
    ticker_ts = float(t.get("ts") or 0.0)
    feed_ts = float(extension_ticker_last_message_ts or 0.0)
    ticker_age = max(0.0, now_t - ticker_ts) if ticker_ts > 0 else None
    feed_age = max(0.0, now_t - feed_ts) if feed_ts > 0 else None

    if not extension_ticker_connected or feed_age is None or feed_age > EXTENSION_FEED_MAX_AGE_SECONDS:
        status = "STALE_FEED"
    elif not t or ticker_ts <= 0:
        status = "MISSING_SYMBOL_SNAPSHOT"
    elif ticker_age is None or ticker_age > EXTENSION_SNAPSHOT_MAX_AGE_SECONDS:
        status = "STALE_SYMBOL_SNAPSHOT"
    else:
        status = "LIVE"

    return {
        "status": status,
        "ticker_age_seconds": round(ticker_age, 2) if ticker_age is not None else None,
        "feed_age_seconds": round(feed_age, 2) if feed_age is not None else None,
        "feed_connected": bool(extension_ticker_connected),
        "last_feed_symbol_count": int(extension_ticker_last_count or 0),
    }


def _extension_context(sym, row):
    t = market_24h.get(sym) or {}
    telemetry = _telemetry_status(sym)
    price = float(row.get("price") or t.get("last") or 0.0)
    change = float(t.get("change_pct") or 0.0)
    high = float(t.get("high") or 0.0)
    low = float(t.get("low") or 0.0)
    open_ = float(t.get("open") or 0.0)
    pullback = _pct_from_high(price, high)
    from_low = ((price / low) - 1.0) * 100.0 if price > 0 and low > 0 else 0.0
    tier = _extension_tier(change) if t else "UNKNOWN"

    sd = app.structure.get(sym) or {}
    ma1 = sd.get("ma_1h") or {}
    near = ma1.get("near") or {}
    one_hour_near = bool(near.get("ema50") or near.get("sma50") or near.get("ema200") or near.get("sma200"))
    ma_reset = bool(ma1.get("reclaim_path") or (ma1.get("structural_support") and one_hour_near))
    compression20 = float((row.get("fast_anomaly") or {}).get("compression_20m_pct") or 999.0)
    reset_depth = _reset_required(max(change, 0.0))

    layers = row.get("layer_results") or {}
    reset_reentry = bool(
        change > EXCEPTIONAL_RUNNER_MAX_PCT
        and pullback >= reset_depth
        and ma_reset
        and compression20 <= 4.0
        and row.get("vwap_hold")
        and layers.get("ACTIVITY_LAYER")
        and layers.get("FLOW_LAYER")
        and layers.get("ORDER_BOOK_LAYER")
    )

    exceptional_runner = bool(
        row.get("runner_second_ignition")
        and layers.get("ACTIVITY_LAYER")
        and layers.get("FLOW_LAYER")
        and layers.get("ORDER_BOOK_LAYER")
        and layers.get("VWAP_LAYER")
        and float(row.get("ofi") or 0.0) > 0.0
        and float(row.get("obi") or 0.0) >= 0.05
        and float(row.get("aggressive_buy_ratio") or 0.0) >= 0.60
    )

    controlled_reentry = bool(
        row.get("runner_second_ignition")
        or row.get("active_setup") in ("MA_RETEST_RECLAIM", "BREAKOUT_RETEST_CONTINUATION")
    )

    if tier == "FRESH":
        eligible = True
        reason = "FRESH_MOVE"
    elif tier == "CONTROLLED_RUNNER":
        eligible = controlled_reentry
        reason = "CONTROLLED_REENTRY_CONFIRMED" if eligible else "NEEDS_RETEST_OR_SECOND_IGNITION"
    elif tier == "EXCEPTIONAL_RUNNER":
        eligible = exceptional_runner
        reason = "EXCEPTIONAL_RUNNER_CONFIRMED" if eligible else "EXTENSION_20_TO_35_REQUIRES_EXCEPTIONAL_CONFLUENCE"
    elif tier == "LATE_RUNNER":
        eligible = reset_reentry
        reason = "NEW_BASE_RESET_CONFIRMED" if eligible else "LATE_RUNNER_DO_NOT_CHASE"
    else:
        eligible = False
        reason = "NO_24H_EXTENSION_SNAPSHOT"

    if telemetry["status"] != "LIVE":
        # Stale/missing telemetry is UNKNOWN, not evidence of over-extension.
        # This still blocks BUY because execution gates require PASS, but it no
        # longer demotes valid early/PRE candidates as if they were late runners.
        reason = f"EXTENSION_TELEMETRY_{telemetry['status']}"

    return {
        "change_24h_pct": round(change, 3) if t else None,
        "open_24h": open_ if t else None,
        "high_24h": high if t else None,
        "low_24h": low if t else None,
        "extension_from_24h_low_pct": round(from_low, 3) if t else None,
        "pullback_from_24h_high_pct": round(pullback, 3) if t else None,
        "extension_tier": tier,
        "extension_guard_pass": bool(eligible) if telemetry["status"] == "LIVE" else None,
        "extension_guard_reason": reason,
        "reset_required_pct": round(reset_depth, 3),
        "ma_reset": ma_reset,
        "reset_reentry": reset_reentry,
        "exceptional_runner": exceptional_runner,
        "controlled_reentry": controlled_reentry,
        "ticker_age_seconds": telemetry["ticker_age_seconds"],
        "extension_feed_age_seconds": telemetry["feed_age_seconds"],
        "extension_feed_connected": telemetry["feed_connected"],
        "extension_feed_symbol_count": telemetry["last_feed_symbol_count"],
        "extension_telemetry_status": telemetry["status"],
    }


def evaluate(sym):
    row = _original_evaluate(sym)
    if not row:
        return None

    ext = _extension_context(sym, row)
    row["extension_guard"] = ext
    row["change_24h_pct"] = ext["change_24h_pct"]
    row["extension_tier"] = ext["extension_tier"]
    row["late_runner"] = ext["extension_tier"] == "LATE_RUNNER"
    row["new_base_reset"] = ext["reset_reentry"]

    current_state = row.get("state", "REJECT")
    qualifies = current_state in q.QUALIFIER_STATES
    telemetry_live = ext.get("extension_telemetry_status") == "LIVE"
    guard = ext.get("extension_guard_pass") is True

    hard = row.setdefault("hard_safety_status", {})
    failed_hard = row.setdefault("failed_hard", [])
    unknown_hard = row.setdefault("unknown_hard", [])

    if not telemetry_live:
        hard["CUMULATIVE_EXTENSION_GUARD"] = "UNKNOWN"
        if "CUMULATIVE_EXTENSION_GUARD" in failed_hard:
            failed_hard.remove("CUMULATIVE_EXTENSION_GUARD")
        if "CUMULATIVE_EXTENSION_GUARD" not in unknown_hard:
            unknown_hard.append("CUMULATIVE_EXTENSION_GUARD")
        row["hard_safety_all_aligned"] = False
        row["mandatory_all_aligned"] = False
        return row

    if "CUMULATIVE_EXTENSION_GUARD" in unknown_hard:
        unknown_hard.remove("CUMULATIVE_EXTENSION_GUARD")

    hard["CUMULATIVE_EXTENSION_GUARD"] = "PASS" if guard else "FAIL"
    if guard:
        if "CUMULATIVE_EXTENSION_GUARD" in failed_hard:
            failed_hard.remove("CUMULATIVE_EXTENSION_GUARD")
        if ext["reset_reentry"] and qualifies:
            row["active_setup"] = "RESET_REENTRY"
        return row

    if "CUMULATIVE_EXTENSION_GUARD" not in failed_hard:
        failed_hard.append("CUMULATIVE_EXTENSION_GUARD")
    row["hard_safety_all_aligned"] = False
    row["mandatory_all_aligned"] = False

    if qualifies or ext["extension_tier"] == "LATE_RUNNER":
        row["state"] = "LATE RUNNER"
        row["active_setup"] = "LATE_RUNNER_BLOCKED"
        if ext["extension_tier"] == "CONTROLLED_RUNNER":
            row["score"] = round(min(float(row.get("score") or 0.0), MAX_CONTROLLED_BLOCK_SCORE), 2)
        elif ext["extension_tier"] == "EXCEPTIONAL_RUNNER":
            row["score"] = round(min(float(row.get("score") or 0.0), MAX_EXCEPTIONAL_BLOCK_SCORE), 2)
        else:
            row["score"] = round(min(float(row.get("score") or 0.0), MAX_LATE_RUNNER_SCORE), 2)

    return row


async def ticker_loop():
    global extension_ticker_connected, extension_ticker_last_message_ts, extension_ticker_last_count
    url = f"{app.WS_BASE}/ws/!ticker@arr"
    while True:
        try:
            async with app.session.ws_connect(url, heartbeat=30, receive_timeout=90, max_msg_size=0) as ws:
                extension_ticker_connected = True
                base.radar_ticker_connected = True
                print("Ψ-V10.8.2 EXTENSION ticker WS connected (!ticker@arr)", flush=True)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, list):
                            continue
                        ts = time.time()
                        extension_ticker_last_message_ts = ts
                        extension_ticker_last_count = len(payload)
                        for x in payload:
                            if not isinstance(x, dict):
                                continue
                            sym = x.get("s", "")
                            last = app.safe_float(x.get("c"))
                            market_24h[sym] = {
                                "change_pct": app.safe_float(x.get("P")),
                                "open": app.safe_float(x.get("o")),
                                "high": app.safe_float(x.get("h")),
                                "low": app.safe_float(x.get("l")),
                                "last": last,
                                "ts": ts,
                            }
                            base.push(
                                sym,
                                last,
                                app.safe_float(x.get("q")),
                                app.safe_float(x.get("n")),
                                app.safe_float(x.get("b")),
                                app.safe_float(x.get("a")),
                                "ticker",
                            )
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.last_error = f"EXTENSION_TICKER: {type(exc).__name__}: {exc}"
            print(app.last_error, flush=True)
        finally:
            extension_ticker_connected = False
            base.radar_ticker_connected = False
        await asyncio.sleep(2)


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", v7.TARGET)), v7.TARGET))
    except ValueError:
        limit = v7.TARGET
    app.resolve_outcomes()
    rows = s.results(limit)
    c = v7.coverage()
    now_t = time.time()
    feed_age = max(0.0, now_t - extension_ticker_last_message_ts) if extension_ticker_last_message_ts else None
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "ALL_MAJOR_LAYERS_PLUS_EXECUTION_PLUS_CUMULATIVE_EXTENSION_GUARD",
        "fresh_max_24h_pct": FRESH_MAX_PCT,
        "controlled_runner_max_24h_pct": CONTROLLED_RUNNER_MAX_PCT,
        "exceptional_runner_max_24h_pct": EXCEPTIONAL_RUNNER_MAX_PCT,
        "late_runner_rule": "NO_FRESH_BUY_ABOVE_35PCT_UNLESS_NEW_BASE_RESET_CONFIRMED",
        "extension_ticker_connected": extension_ticker_connected,
        "extension_feed_age_seconds": round(feed_age, 2) if feed_age is not None else None,
        "extension_feed_symbol_count": extension_ticker_last_count,
        "extension_feed_max_age_seconds": EXTENSION_FEED_MAX_AGE_SECONDS,
        "extension_snapshot_max_age_seconds": EXTENSION_SNAPSHOT_MAX_AGE_SECONDS,
        "stale_extension_policy": "UNKNOWN_NOT_FAIL",
        "rolling_persistence_seconds": v8.PERSIST_WINDOW,
        "rolling_persistence_required_hits": v8.PERSIST_HITS,
        "rapid_slots": v7.RAPID_MICRO_SLOTS,
        "runner_second_ignition": True,
    }
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.8.2 Extension Guard Telemetry Fix + Layer Voting + Rolling Persistence",
        "version": VERSION,
        "buy_policy": "ALL_MAJOR_LAYERS_AND_LIVE_EXECUTION_AND_CUMULATIVE_EXTENSION_GUARD",
        "returned": len(rows),
        "state_counts": dict(Counter(x["state"] for x in rows)),
        "coverage": c,
        "results": rows,
        "near_miss_diagnostics": s.near_diag(10),
        "ignition_top": base.rank(limit=10),
        "missed_move_events": list(v7.missed_move_events)[-20:],
        "generated_ms": q.ms(),
    })


base.ticker_loop = ticker_loop
app.evaluate_symbol = evaluate
app.scan_endpoint = scan
v7.tick = v8.tick
q.tick = v8.tick
s.tick = v8.tick
app.ranked_results = s.results


if __name__ == "__main__":
    try:
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.8.2 stopped", flush=True)
