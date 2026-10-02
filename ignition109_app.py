import asyncio
import json
import math
import time
from collections import Counter, defaultdict, deque

import aiohttp
import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8

VERSION = "10.9-latent-cluster"
LATENT_PRICE_5S_MAX = 0.55
LATENT_PRICE_15S_MAX = 1.00
LATENT_PRICE_30S_MAX = 2.00
LATENT_RANGE60_MAX = 2.20
LATENT_VOL5_MIN = 3.0
LATENT_VOL15_MIN = 2.0
LATENT_TRADES5_MIN = 2.2
LATENT_TRADES15_MIN = 1.8
BURST_MIN_GAP = 5.0
BURST_WINDOW = 1800
PRESSURE_WINDOW = 180
SUBSCRIPTION_SYNC_SECONDS = 1.0

burst_hist = defaultdict(lambda: deque(maxlen=200))
last_burst = defaultdict(float)
pressure_hist = defaultdict(lambda: deque(maxlen=120))
last_pressure = defaultdict(float)
first_latent = {}
stream_reconnects = 0
stream_subscription_changes = 0
stream_bootstraps = 0

v7.VERSION = VERSION
base.VERSION = VERSION
v8.VERSION = VERSION
base.HISTORY_SAMPLES = 900
v7.RAPID_MICRO_SLOTS = max(v7.RAPID_MICRO_SLOTS, 16)
v7.RAPID_MIN_HOLD = max(v7.RAPID_MIN_HOLD, 300)
v7.RAPID_REPLACE_COOLDOWN = max(v7.RAPID_REPLACE_COOLDOWN, 30)
v7.RAPID_STRUCTURE_COOLDOWN = max(v7.RAPID_STRUCTURE_COOLDOWN, 30)
q.MICRO_HOLD = max(q.MICRO_HOLD, 480)
q.LOCK_GRACE = max(q.LOCK_GRACE, 600)
q.TICK_SECONDS = 10
app.USER_AGENT = "psi-v10-live-scanner/10.9-latent-cluster"


def _before(samples, target):
    for x in reversed(samples):
        if x[0] <= target:
            return x
    return samples[0] if samples else None


def _positive_increment(samples, start_t, end_t, idx):
    pts = [x for x in samples if start_t <= x[0] <= end_t]
    if len(pts) < 2:
        a = _before(samples, start_t)
        b = _before(samples, end_t)
        if a and b:
            return max(0.0, float(b[idx]) - float(a[idx]))
        return 0.0
    total = 0.0
    prev = pts[0]
    for cur in pts[1:]:
        total += max(0.0, float(cur[idx]) - float(prev[idx]))
        prev = cur
    return total


def _accel(cur_amount, cur_seconds, prev_amount, prev_seconds, daily_total):
    cur_rate = cur_amount / max(cur_seconds, 1.0)
    prev_rate = prev_amount / max(prev_seconds, 1.0)
    daily_rate = max(float(daily_total), 0.0) / 86400.0
    denominator = max(prev_rate, daily_rate * 0.20, 1e-12)
    return cur_rate / denominator if cur_rate > 0 else 0.0


def _prune_bursts(symbol, now_t):
    d = burst_hist[symbol]
    while d and d[0] < now_t - BURST_WINDOW:
        d.popleft()
    return d


def _burst_counts(symbol, now_t):
    d = _prune_bursts(symbol, now_t)
    return {
        "5m": sum(1 for x in d if x >= now_t - 300),
        "15m": sum(1 for x in d if x >= now_t - 900),
        "30m": len(d),
    }


def metric(symbol):
    samples = base.radar_hist.get(symbol)
    if not samples or len(samples) < 5:
        fallback = q.disc.get(symbol)
        samples = fallback if fallback and len(fallback) >= 5 else samples
    if not samples or len(samples) < 5:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples or ())}

    t, p, qv, n, bid, ask = samples[-1]
    age = t - samples[0][0]
    if p <= 0 or age < 12:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples), "age_s": round(age, 1)}

    s5, s15, s30, s60 = (_before(samples, t - x) for x in (5, 15, 30, 60))
    ret = lambda x: (p / x[1] - 1.0) * 100.0 if x and x[1] > 0 else 0.0
    r5, r15, r30, r60 = ret(s5), ret(s15), ret(s30), ret(s60)

    v5 = _positive_increment(samples, t - 5, t, 2)
    v_prev10 = _positive_increment(samples, t - 15, t - 5, 2)
    v15 = _positive_increment(samples, t - 15, t, 2)
    v_prev15 = _positive_increment(samples, t - 30, t - 15, 2)
    n5 = _positive_increment(samples, t - 5, t, 3)
    n_prev10 = _positive_increment(samples, t - 15, t - 5, 3)
    n15 = _positive_increment(samples, t - 15, t, 3)
    n_prev15 = _positive_increment(samples, t - 30, t - 15, 3)

    v5x = _accel(v5, 5, v_prev10, 10, qv)
    v15x = _accel(v15, 15, v_prev15, 15, qv)
    t5x = _accel(n5, 5, n_prev10, 10, n)
    t15x = _accel(n15, 15, n_prev15, 15, n)

    pacc = max(0.0, r5 * 3.0 - r15) + max(0.0, r15 * 2.0 - r30)
    spread = (ask - bid) / ((ask + bid) / 2.0) * 10000.0 if bid > 0 and ask > bid else 0.0
    prices = [x[1] for x in samples if x[0] >= t - 60 and x[1] > 0]
    range60 = (max(prices) - min(prices)) / p * 100.0 if prices else 999.0

    volume_burst = v5x >= LATENT_VOL5_MIN or v15x >= LATENT_VOL15_MIN
    trade_burst = t5x >= LATENT_TRADES5_MIN or t15x >= LATENT_TRADES15_MIN
    quiet = (
        abs(r5) <= LATENT_PRICE_5S_MAX
        and abs(r15) <= LATENT_PRICE_15S_MAX
        and abs(r30) <= LATENT_PRICE_30S_MAX
    )
    compressed = range60 <= LATENT_RANGE60_MAX
    latent = bool(
        quiet and compressed and (volume_burst or trade_burst)
        and spread <= 30 and r60 < 6.0
    )

    displacement = max(abs(r15), abs(r5) * 0.5, 0.05)
    activity_price_divergence = min(250.0, max(v5x, v15x, t5x, t15x) / displacement)

    normal_accel = (
        r5 >= 0.10 or r15 >= 0.20 or v5x >= 1.6 or t5x >= 1.6
        or (v15x >= 1.4 and t15x >= 1.4)
    )
    burst_event = bool(latent or ((volume_burst or trade_burst) and (r5 >= 0.05 or r15 >= 0.10)))
    if burst_event and t - last_burst[symbol] >= BURST_MIN_GAP:
        burst_hist[symbol].append(t)
        last_burst[symbol] = t
        first_latent.setdefault(symbol, t)

    counts = _burst_counts(symbol, t)
    clustered = counts["5m"] >= 2 or counts["15m"] >= 3 or counts["30m"] >= 4
    first = burst_hist[symbol][0] if burst_hist[symbol] else 0.0
    velocity_seconds = round(t - first, 1) if first else None

    if r15 >= 1.5 or r30 >= 3.0:
        phase = "EXPANSION"
    elif clustered and (r5 >= 0.15 or r15 >= 0.30) and (volume_burst or trade_burst):
        phase = "IGNITION"
    elif latent:
        phase = "LATENT_IGNITION"
    elif clustered:
        phase = "IGNITION_BUILDING"
    else:
        phase = "RADAR"

    c = lambda x, a, b: max(a, min(b, x))
    score = (
        c(r5, 0, 1.5) * 16
        + c(r15, 0, 3) * 7
        + c(r30, 0, 5) * 2.5
        + c(pacc, 0, 3) * 6
        + c(v5x - 1, 0, 8) * 7
        + c(v15x - 1, 0, 6) * 3
        + c(t5x - 1, 0, 8) * 6
        + c(t15x - 1, 0, 6) * 3
        + (6 if spread <= 8 else 2 if spread <= 15 else -8 if spread >= 30 else 0)
        + (5 if compressed else 0)
        + (12 if latent else 0)
        + min(counts["5m"] * 4 + counts["15m"] * 2, 18)
        + c(activity_price_divergence / 20.0, 0, 8)
    )
    score -= min(max(0.0, r60 - 6.0) * 1.5 + max(0.0, r30 - 4.0), 12.0)

    trigger = bool(
        v7._directional(symbol)
        and spread <= 30
        and (normal_accel or latent or clustered)
        and score >= base.TRIGGER_SCORE
    )
    return {
        "symbol": symbol, "score": round(score, 3), "trigger": trigger,
        "phase": phase, "latent_ignition": latent, "clustered_ignition": clustered,
        "activity_price_divergence": round(activity_price_divergence, 3),
        "burst_count_5m": counts["5m"], "burst_count_15m": counts["15m"], "burst_count_30m": counts["30m"],
        "ignition_velocity_seconds": velocity_seconds,
        "r5": round(r5, 4), "r15": round(r15, 4), "r30": round(r30, 4), "r60": round(r60, 4),
        "price_accel": round(pacc, 4), "vol_accel_5": round(v5x, 3), "vol_accel_15": round(v15x, 3),
        "trade_accel_5": round(t5x, 3), "trade_accel_15": round(t15x, 3),
        "spread_bps": round(spread, 3), "range60_pct": round(range60, 4), "price": p,
        "quote_5s": round(v5, 2), "trades_5s": int(n5), "samples": len(samples), "age_s": round(age, 1),
    }


def _pressure_stats(symbol, now_t):
    d = pressure_hist[symbol]
    while d and d[0][0] < now_t - PRESSURE_WINDOW:
        d.popleft()

    def one(window):
        pts = [x for x in d if x[0] >= now_t - window]
        return {
            "samples": len(pts),
            "buy_bursts": sum(1 for x in pts if x[1] >= 0.65),
            "obi_bursts": sum(1 for x in pts if x[2] >= 0.08),
            "ofi_bursts": sum(1 for x in pts if x[3] >= 0.05),
            "ask_thin_bursts": sum(1 for x in pts if x[4] >= 0 and x[2] >= 0.03),
        }

    return {"30s": one(30), "60s": one(60), "180s": one(180)}


def evaluate(symbol):
    row = v8.evaluate(symbol)
    if not row:
        return None

    now_t = time.time()
    if row.get("micro_ready") and now_t - last_pressure[symbol] >= 2:
        pressure_hist[symbol].append((
            now_t,
            float(row.get("aggressive_buy_ratio") or 0),
            float(row.get("obi") or 0),
            float(row.get("ofi") or 0),
            float(row.get("ask_depletion") or 0),
        ))
        last_pressure[symbol] = now_t

    ps = _pressure_stats(symbol, now_t)
    p30, p60, p180 = ps["30s"], ps["60s"], ps["180s"]
    rolling_flow = bool(
        (p30["buy_bursts"] >= 1 and p30["ofi_bursts"] >= 1 and p30["obi_bursts"] >= 1)
        or (p60["buy_bursts"] >= 2 and (p60["ofi_bursts"] >= 1 or p60["obi_bursts"] >= 2))
        or (p180["buy_bursts"] >= 4 and p180["ofi_bursts"] >= 2)
    )
    rolling_book = bool(
        p30["obi_bursts"] >= 2
        or p60["obi_bursts"] >= 2
        or (p60["obi_bursts"] >= 1 and p60["ask_thin_bursts"] >= 2)
        or p180["obi_bursts"] >= 4
    )

    r = metric(symbol)
    latent_activity = bool(
        r.get("latent_ignition")
        or (r.get("clustered_ignition") and r.get("burst_count_15m", 0) >= 3)
    )

    layers = dict(row.get("layer_results") or {})
    original_activity = bool(layers.get("ACTIVITY_LAYER"))
    original_flow = bool(layers.get("FLOW_LAYER"))
    original_book = bool(layers.get("ORDER_BOOK_LAYER"))
    layers["ACTIVITY_LAYER"] = bool(original_activity or latent_activity)
    layers["FLOW_LAYER"] = bool(original_flow or rolling_flow)
    layers["ORDER_BOOK_LAYER"] = bool(original_book or rolling_book)

    exe_keys = (
        "LIVE_MICRO_DATA", "TRADE_SEQUENCE_VALID", "BOOK_SEQUENCE_VALID",
        "SPREAD_FILTER", "SLIPPAGE_FILTER",
    )
    hard = row.get("hard_safety_status") or {}
    exe_pass = all(hard.get(k) == "PASS" for k in exe_keys)
    major_keys = (
        "ACTIVITY_LAYER", "FLOW_LAYER", "ORDER_BOOK_LAYER", "VWAP_LAYER",
        "MA_STRUCTURE_LAYER", "ANTI_CHASE_OR_RUNNER_LAYER",
    )
    all_layers = all(bool(layers.get(k)) for k in major_keys)
    layer_count = sum(bool(layers.get(k)) for k in major_keys)
    micro_ready = bool(row.get("micro_ready"))
    buy = bool(all_layers and exe_pass)
    pre = bool(
        not buy and micro_ready
        and layers.get("MA_STRUCTURE_LAYER")
        and layers.get("ACTIVITY_LAYER")
        and layers.get("FLOW_LAYER")
        and layer_count >= 5
    )

    if buy:
        state = "BUY NOW"
    elif pre:
        state = "PRE-IGNITION"
    elif micro_ready and layer_count >= 4:
        state = "WATCH"
    else:
        state = row.get("state", "REJECT")

    row["state"] = state
    row["layer_results"] = layers
    row["failed_layers"] = [k for k, v in layers.items() if not v]
    row["failed_setup"] = list(row["failed_layers"])
    row["mandatory_all_aligned"] = buy
    row["micro_confirmation_count"] = layer_count
    row["rolling_buy_pressure"] = ps
    row["rolling_flow_memory"] = rolling_flow
    row["rolling_book_memory"] = rolling_book
    row["latent_activity_memory"] = latent_activity
    row["ignition_phase"] = r.get("phase")
    row["latent_ignition"] = r.get("latent_ignition")
    row["clustered_ignition"] = r.get("clustered_ignition")
    row["activity_price_divergence"] = r.get("activity_price_divergence")
    row["burst_count_5m"] = r.get("burst_count_5m", 0)
    row["burst_count_15m"] = r.get("burst_count_15m", 0)
    row["burst_count_30m"] = r.get("burst_count_30m", 0)
    row["ignition_velocity_seconds"] = r.get("ignition_velocity_seconds")
    row["rapid_ignition"] = r

    exe_count = sum(hard.get(k) == "PASS" for k in exe_keys)
    row["score"] = round(max(
        float(row.get("score") or 0),
        min(105.0, (layer_count / 6.0) * 80.0 + (exe_count / 5.0) * 20.0
            + min(float(r.get("score") or 0), 100.0) * 0.04),
    ), 2)
    return row


async def persistent_rapid_websocket_loop():
    global stream_reconnects, stream_subscription_changes, stream_bootstraps
    while True:
        try:
            symbols=sorted({
                x for x in v7.rapid_symbols
                if x not in set(app.selected_micro_symbols)
            })
            if not symbols:
                v7.rapid_ws_connected=False
                v7.rapid_ws_symbols=[]
                await asyncio.sleep(1)
                continue

            streams=[]
            for symbol in symbols:
                lower=symbol.lower()
                streams.extend([f"{lower}@aggTrade",f"{lower}@depth20@100ms"])
            url=f"{app.WS_BASE}/stream?streams={'/'.join(streams)}"
            assert app.session is not None
            print(f"Ψ-V10.9 RAPID WS connecting symbols={len(symbols)} book=DEPTH20_WS",flush=True)
            async with app.session.ws_connect(url,heartbeat=None,receive_timeout=90,max_msg_size=0) as ws:
                v7.rapid_ws_connected=True
                v7.rapid_ws_symbols=list(symbols)
                for symbol in symbols:
                    st=app.ensure_micro_state(symbol)
                    st["book_buffer"].clear()
                    st["book_snapshot_ready"]=False
                    st["book_sequence_ok"]=True
                    st["book_sequence_samples"]=0
                    st["book_resyncing"]=False
                    st["last_book_update_id"]=None
                print(f"Ψ-V10.9 RAPID WS connected symbols={len(symbols)} book=REST_FREE_DEPTH20",flush=True)

                async for message in ws:
                    desired=sorted({
                        x for x in v7.rapid_symbols
                        if x not in set(app.selected_micro_symbols)
                    })
                    if set(desired)!=set(symbols):
                        stream_subscription_changes+=len(set(desired)^set(symbols))
                        stream_reconnects+=1
                        print("Ψ-V10.9 RAPID membership changed; reconnecting combined stream",flush=True)
                        break

                    if message.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        stream_name=payload.get("stream","")
                        data=payload.get("data",{})
                        if not stream_name or not isinstance(data,dict):
                            continue
                        symbol=stream_name.split("@")[0].upper()
                        if "@aggTrade" in stream_name:
                            app.process_agg_trade(symbol,data)
                        elif "@depth20" in stream_name:
                            app.process_partial_depth_snapshot(symbol,data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        stream_reconnects+=1
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            stream_reconnects+=1
            app.last_error=f"V10.9_RAPID_WS: {type(exc).__name__}: {exc}"
            print(app.last_error,flush=True)
        finally:
            v7.rapid_ws_connected=False
            v7.rapid_ws_symbols=[]
        await asyncio.sleep(1)


async def health(req):
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "ALL_MAJOR_LAYERS_PLUS_LIVE_EXECUTION_GATES",
        "latent_ignition": True,
        "activity_price_divergence": True,
        "burst_cluster_windows_seconds": [300, 900, 1800],
        "rolling_buy_pressure_windows_seconds": [30, 60, 180],
        "dynamic_rapid_subscriptions": True,
        "rapid_stream_reconnects": stream_reconnects,
        "rapid_subscription_changes": stream_subscription_changes,
        "rapid_book_bootstraps": stream_bootstraps,
        "trade_count_acceleration_v2": True,
    }
    return app.web.json_response({
        "ok": True, "service": "psi-v10-live-scanner", "version": VERSION,
        "scanner_ready": app.scanner_ready,
        "websocket_connected": app.websocket_connected,
        "rapid_websocket_connected": v7.rapid_ws_connected,
        "discovery_ws_connected": q.disc_ws,
        "coverage": c,
        "stable_qualifier_symbols": list(q.stable),
        "near_miss_diagnostics": s.near_diag(10),
        "latent_ignition_top": [x for x in _radar_rows(20) if x.get("latent_ignition")][:10],
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", v7.TARGET)), v7.TARGET))
    except ValueError:
        limit = v7.TARGET
    app.resolve_outcomes()
    rows = s.results(limit)
    radar = _radar_rows(20)
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "ALL_MAJOR_LAYERS_PLUS_LIVE_EXECUTION_GATES",
        "rolling_persistence_seconds": v8.PERSIST_WINDOW,
        "rolling_persistence_required_hits": v8.PERSIST_HITS,
        "latent_ignition": True,
        "activity_price_divergence": True,
        "burst_cluster_windows_seconds": [300, 900, 1800],
        "rolling_buy_pressure_windows_seconds": [30, 60, 180],
        "trade_count_acceleration_v2": True,
        "dynamic_rapid_subscriptions": True,
        "rapid_stream_reconnects": stream_reconnects,
    }
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.9 Latent Ignition + Burst Clustering + Rolling Pressure",
        "version": VERSION,
        "buy_policy": "ALL_MAJOR_LAYERS_ALIGNED_AND_ALL_LIVE_EXECUTION_GATES_PASS",
        "returned": len(rows),
        "state_counts": dict(Counter(x["state"] for x in rows)),
        "coverage": c,
        "results": rows,
        "near_miss_diagnostics": s.near_diag(10),
        "latent_ignition_top": [x for x in radar if x.get("latent_ignition")][:10],
        "ignition_building_top": [x for x in radar if x.get("phase") in ("IGNITION_BUILDING", "IGNITION")][:10],
        "ignition_top": radar[:10],
        "missed_move_events": list(v7.missed_move_events)[-20:],
        "generated_ms": q.ms(),
    })


async def print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        rows = s.results()
        c = v7.coverage()
        h = c.get("hunt", {})
        radar = _radar_rows(5)
        print("\n==================================================", flush=True)
        print(f"Ψ-V10.9 LATENT IGNITION + TARGET-10 — {len(rows)}/{v7.TARGET}", flush=True)
        print(
            f"attempted={h.get('structure_attempted',0)}/{c.get('full_universe',0)} "
            f"valid={h.get('structure_valid',0)} micro_verified={h.get('micro_candidates_verified',0)} "
            f"live_micro={c.get('micro_verified_fresh',0)}/{c.get('micro_total',0)} "
            f"rapid={len(v7.rapid_symbols)}/{v7.RAPID_MICRO_SLOTS} "
            f"stream_reconnects={stream_reconnects}",
            flush=True,
        )
        print("==================================================", flush=True)
        for i, row in enumerate(rows, 1):
            print(
                f"{i:02d}. {row['symbol']:12s} {row['state']:14s} score={row['score']:6.2f} "
                f"persist={row.get('persistence_samples',0)} lane={row.get('lane','STABLE'):6s} "
                f"phase={str(row.get('ignition_phase','')):18s} "
                f"OFI={float(row.get('ofi') or 0):+.3f} OBI={float(row.get('obi') or 0):+.3f} "
                f"buy={float(row.get('aggressive_buy_ratio') or 0):.2%}",
                flush=True,
            )
        if not rows:
            print("No persistent PRE-IGNITION / BUY NOW setup currently qualifies.", flush=True)

        if radar:
            print("IGNITION RADAR TOP:", flush=True)
            for i, r in enumerate(radar, 1):
                print(
                    f"R{i:02d}. {r['symbol']:12s} score={float(r.get('score',0)):6.1f} "
                    f"phase={str(r.get('phase','RADAR')):18s} latent={r.get('latent_ignition')} "
                    f"b5={r.get('burst_count_5m',0)} b15={r.get('burst_count_15m',0)} "
                    f"r5={float(r.get('r5',0)):+.2f}% r15={float(r.get('r15',0)):+.2f}% "
                    f"v5x={float(r.get('vol_accel_5',0)):.2f} t5x={float(r.get('trade_accel_5',0)):.2f}",
                    flush=True,
                )

        diagnostics = s.near_diag(10)
        if diagnostics:
            print("TOP NEAR MISSES:", flush=True)
            for i, d in enumerate(diagnostics, 1):
                print(
                    f"N{i:02d}. {str(d.get('symbol')):12s} state={str(d.get('state')):12s} "
                    f"score={float(d.get('score',0) or 0):6.2f} micro={d.get('micro_ready')} "
                    f"hard={d.get('failed_hard',[])} layers={d.get('failed_layers',d.get('failed_setup',[]))}",
                    flush=True,
                )


base.metric = metric
v7.ignition_metric = metric
app.evaluate_symbol = evaluate
v7.rapid_websocket_loop = persistent_rapid_websocket_loop
v7.print_loop = print_loop
q.print_loop = print_loop
s.print_loop = print_loop
app.health = health
app.scan_endpoint = scan
app.ranked_results = s.results

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.9 ACTIVE — latent ignition, activity/price divergence, burst clustering, "
            "rolling buy-pressure memory, trade-acceleration v2, persistent rapid subscriptions",
            flush=True,
        )
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.9 stopped", flush=True)
