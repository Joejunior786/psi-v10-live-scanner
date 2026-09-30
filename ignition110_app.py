import asyncio
import math
import statistics
import time
from collections import Counter, defaultdict, deque

import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8
import ignition1081_app as v81
import ignition109_app as v9
import ignition1091_app as core
import ignition1092_app as v92
import ignition1093_app as v93

VERSION = "10.10-opportunity-sequence"
PRE_MIN_LAYERS = 4
PRE_MIN_EARLY_EVIDENCE = 2
PRE_MIN_BIG_MOVE_SCORE = 55.0
HIGH_BIG_MOVE_SCORE = 75.0
MEDIUM_BIG_MOVE_SCORE = 55.0
MARKET_CACHE_SECONDS = 2.0
MARKET_DATA_MAX_AGE_SECONDS = 30.0
STAGE_SAMPLE_SECONDS = 5.0
STAGE_WINDOW_SECONDS = 1800.0

stage_history = defaultdict(lambda: deque(maxlen=240))
last_stage_sample = defaultdict(float)
market_cache = {"ts": 0.0, "value": {}}

for mod in (core, v93, v92, v9, v81, v8, v7, base):
    mod.VERSION = VERSION

v7.RAPID_MICRO_SLOTS = max(v7.RAPID_MICRO_SLOTS, 20)
v7.RAPID_MIN_HOLD = max(v7.RAPID_MIN_HOLD, 360)
q.MICRO_HOLD = max(q.MICRO_HOLD, 600)
q.LOCK_GRACE = max(q.LOCK_GRACE, 900)
app.USER_AGENT = "psi-v10-live-scanner/10.10-opportunity-sequence"

_base_evaluate = core.evaluate
MAJOR_KEYS = (
    "ACTIVITY_LAYER", "FLOW_LAYER", "ORDER_BOOK_LAYER",
    "VWAP_LAYER", "MA_STRUCTURE_LAYER", "ANTI_CHASE_OR_RUNNER_LAYER",
)
EXECUTION_KEYS = (
    "LIVE_MICRO_DATA", "TRADE_SEQUENCE_VALID", "BOOK_SEQUENCE_VALID",
    "SPREAD_FILTER", "SLIPPAGE_FILTER", "CUMULATIVE_EXTENSION_GUARD",
)


def _median(values, default=0.0):
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return statistics.median(vals) if vals else default


def _hist_return(symbol, seconds):
    samples = base.radar_hist.get(symbol)
    if not samples or len(samples) < 2:
        return 0.0
    t, price = samples[-1][0], float(samples[-1][1] or 0.0)
    if price <= 0:
        return 0.0
    old = None
    target = t - seconds
    for item in reversed(samples):
        if item[0] <= target:
            old = item
            break
    old = old or samples[0]
    old_price = float(old[1] or 0.0)
    return (price / old_price - 1.0) * 100.0 if old_price > 0 else 0.0


def _market_context():
    now_t = time.time()
    if now_t - market_cache["ts"] < MARKET_CACHE_SECONDS:
        return market_cache["value"]

    fresh = {}
    changes = []
    for sym, item in v81.market_24h.items():
        age = now_t - float(item.get("ts") or 0.0)
        if age > MARKET_DATA_MAX_AGE_SECONDS:
            continue
        change = float(item.get("change_pct") or 0.0)
        if math.isfinite(change):
            fresh[sym] = item
            if sym in q.universe_set and -80.0 < change < 300.0:
                changes.append(change)

    median24 = _median(changes, 0.0)
    btc24 = float((fresh.get("BTCUSDT") or {}).get("change_pct") or 0.0)
    eth24 = float((fresh.get("ETHUSDT") or {}).get("change_pct") or 0.0)
    btc_r15 = _hist_return("BTCUSDT", 15)
    btc_r60 = _hist_return("BTCUSDT", 60)

    if btc_r60 <= -0.80 or median24 <= -2.50:
        regime = "SEVERE_RISK_OFF"
    elif btc_r60 <= -0.30 or median24 <= -1.00:
        regime = "DEFENSIVE"
    elif btc_r60 >= 0.15 and median24 >= 0.0:
        regime = "SUPPORTIVE"
    else:
        regime = "NEUTRAL"

    value = {
        "regime": regime,
        "median_binance_24h_pct": round(median24, 3),
        "btc_24h_pct": round(btc24, 3),
        "eth_24h_pct": round(eth24, 3),
        "btc_r15_pct": round(btc_r15, 4),
        "btc_r60_pct": round(btc_r60, 4),
        "fresh_24h_symbols": len(fresh),
    }
    market_cache["ts"] = now_t
    market_cache["value"] = value
    return value


def _relative_strength(symbol, rapid):
    ctx = _market_context()
    ticker = v81.market_24h.get(symbol) or {}
    change24 = float(ticker.get("change_pct") or 0.0)
    rs24 = change24 - float(ctx.get("median_binance_24h_pct") or 0.0)
    rs15 = float(rapid.get("r15") or 0.0) - float(ctx.get("btc_r15_pct") or 0.0)
    rs60 = float(rapid.get("r60") or 0.0) - float(ctx.get("btc_r60_pct") or 0.0)
    return {
        "change_24h_pct": round(change24, 3),
        "vs_binance_median_24h_pct": round(rs24, 3),
        "vs_btc_15s_pct": round(rs15, 4),
        "vs_btc_60s_pct": round(rs60, 4),
    }


def _record_stage(symbol, row, rapid):
    now_t = time.time()
    if now_t - last_stage_sample[symbol] < STAGE_SAMPLE_SECONDS:
        return
    layers = row.get("layer_results") or {}
    stage_history[symbol].append({
        "ts": now_t,
        "phase": str(rapid.get("phase") or row.get("ignition_phase") or "RADAR"),
        "flow": bool(layers.get("FLOW_LAYER")),
        "book": bool(layers.get("ORDER_BOOK_LAYER")),
        "ma": bool(layers.get("MA_STRUCTURE_LAYER")),
        "vwap": bool(layers.get("VWAP_LAYER")),
        "activity": bool(layers.get("ACTIVITY_LAYER")),
        "r15": float(rapid.get("r15") or 0.0),
    })
    last_stage_sample[symbol] = now_t
    d = stage_history[symbol]
    while d and d[0]["ts"] < now_t - STAGE_WINDOW_SECONDS:
        d.popleft()


def _sequence_info(symbol):
    hist = list(stage_history.get(symbol) or ())
    if not hist:
        return {"stage_count": 0, "score": 0.0, "sequence": []}

    sequence = []
    last_ts = -1.0

    def find_after(predicate, label):
        nonlocal last_ts
        for item in hist:
            if item["ts"] > last_ts and predicate(item):
                last_ts = item["ts"]
                sequence.append(label)
                return True
        return False

    find_after(
        lambda x: x["phase"] in ("LATENT_IGNITION", "IGNITION_BUILDING", "IGNITION"),
        "ACTIVITY_WAKEUP",
    )
    if sequence:
        find_after(lambda x: x["flow"] or x["book"], "PRESSURE_CONFIRMATION")
    if len(sequence) >= 2:
        find_after(lambda x: x["ma"] and x["vwap"], "STRUCTURE_ALIGNMENT")
    if len(sequence) >= 3:
        find_after(lambda x: x["r15"] >= 0.30 or x["phase"] in ("IGNITION", "EXPANSION"), "PRICE_EXPANSION")

    count = len(sequence)
    score = min(100.0, count * 25.0)
    return {"stage_count": count, "score": score, "sequence": sequence}


def _resistance_pressure(row, rapid):
    breakout = abs(float(row.get("breakout_distance_pct") or 999.0))
    obi = float(row.get("obi") or 0.0)
    ofi = float(row.get("ofi") or 0.0)
    buy = float(row.get("aggressive_buy_ratio") or 0.0)
    ask_dep = float(row.get("ask_depletion") or 0.0)
    bursts = int(rapid.get("burst_count_5m") or 0)

    score = 0.0
    reasons = []
    if breakout <= 0.75:
        score += 25
        reasons.append("NEAR_RESISTANCE")
    elif breakout <= 1.50:
        score += 15
        reasons.append("APPROACHING_RESISTANCE")
    if bursts >= 4:
        score += 20
        reasons.append("REPEATED_ATTACKS")
    elif bursts >= 2:
        score += 10
    if obi >= 0.08 and ask_dep >= 0:
        score += 20
        reasons.append("ASK_SIDE_WEAKENING")
    if buy >= 0.62:
        score += 20
        reasons.append("AGGRESSIVE_BUYERS")
    if ofi >= 0.05:
        score += 15
        reasons.append("POSITIVE_OFI")
    return {"score": min(100.0, score), "reasons": reasons}


def _big_move_potential(symbol, row, rapid):
    ctx = _market_context()
    rs = _relative_strength(symbol, rapid)
    ext = row.get("extension_guard") or {}
    tier = str(row.get("extension_tier") or ext.get("extension_tier") or "UNKNOWN")
    change24 = float(rs["change_24h_pct"])
    layers = row.get("layer_results") or {}
    seq = _sequence_info(symbol)
    resistance = _resistance_pressure(row, rapid)

    if change24 <= 3.0:
        fresh = 20.0
    elif change24 <= 10.0:
        fresh = 17.0
    elif change24 <= 20.0:
        fresh = 9.0
    elif change24 <= 35.0:
        fresh = 4.0
    else:
        fresh = 0.0

    absorption = 0.0
    if rapid.get("latent_ignition"):
        absorption += 12.0
    if abs(float(rapid.get("r15") or 0.0)) <= 0.50:
        absorption += 4.0
    absorption += min(9.0, float(rapid.get("activity_price_divergence") or 0.0) / 25.0)

    bursts = min(
        13.0,
        int(rapid.get("burst_count_5m") or 0) * 1.5
        + int(rapid.get("burst_count_15m") or 0) * 0.35,
    )
    pressure = 0.0
    if layers.get("FLOW_LAYER"):
        pressure += 6.0
    if layers.get("ORDER_BOOK_LAYER"):
        pressure += 6.0
    if row.get("rolling_flow_memory"):
        pressure += 3.0
    if row.get("rolling_book_memory"):
        pressure += 3.0
    pressure = min(15.0, pressure)

    relative = 0.0
    relative += max(0.0, min(6.0, float(rs["vs_binance_median_24h_pct"]) * 1.2))
    relative += max(0.0, min(4.0, float(rs["vs_btc_15s_pct"]) * 8.0))

    regime_bonus = {
        "SUPPORTIVE": 5.0,
        "NEUTRAL": 3.0,
        "DEFENSIVE": 0.0,
        "SEVERE_RISK_OFF": -8.0,
    }.get(ctx["regime"], 0.0)

    score = (
        fresh
        + min(25.0, absorption)
        + bursts
        + pressure
        + resistance["score"] * 0.12
        + relative
        + seq["score"] * 0.12
        + regime_bonus
    )
    score = max(0.0, min(100.0, score))

    reset_ok = bool(row.get("new_base_reset"))
    guard_pass = (row.get("hard_safety_status") or {}).get("CUMULATIVE_EXTENSION_GUARD") == "PASS"
    if tier == "LATE_RUNNER" and not reset_ok:
        score = min(score, 39.0)
    elif not guard_pass and tier in ("CONTROLLED_RUNNER", "EXCEPTIONAL_RUNNER"):
        score = min(score, 54.0)

    if score >= HIGH_BIG_MOVE_SCORE:
        label = "HIGH"
    elif score >= MEDIUM_BIG_MOVE_SCORE:
        label = "MEDIUM"
    else:
        label = "LOW"

    return {
        "score": round(score, 2),
        "label": label,
        "fresh_base_points": round(fresh, 2),
        "absorption_points": round(min(25.0, absorption), 2),
        "burst_points": round(bursts, 2),
        "pressure_points": round(pressure, 2),
        "resistance_pressure": resistance,
        "relative_strength": rs,
        "sequence": seq,
        "market_regime": ctx["regime"],
    }


def _early_evidence(row, rapid, big):
    evidence = {
        "LATENT_IGNITION": bool(rapid.get("latent_ignition")),
        "BURST_CLUSTER": bool(
            rapid.get("clustered_ignition")
            and int(rapid.get("burst_count_15m") or 0) >= 3
        ),
        "ACTIVITY_PRICE_DIVERGENCE": float(rapid.get("activity_price_divergence") or 0.0) >= 20.0,
        "ROLLING_FLOW_MEMORY": bool(row.get("rolling_flow_memory")),
        "ROLLING_BOOK_MEMORY": bool(row.get("rolling_book_memory")),
        "ORDERED_IGNITION_SEQUENCE": int(big["sequence"]["stage_count"]) >= 2,
        "RESISTANCE_PRESSURE": float(big["resistance_pressure"]["score"]) >= 50.0,
        "RELATIVE_STRENGTH": (
            float(big["relative_strength"]["vs_binance_median_24h_pct"]) > 0.0
            and float(big["relative_strength"]["vs_btc_15s_pct"]) >= 0.0
        ),
    }
    return evidence


def evaluate(symbol):
    row = _base_evaluate(symbol)
    if not row:
        return None

    rapid = row.get("rapid_ignition") or v93.metric(symbol)
    _record_stage(symbol, row, rapid)
    big = _big_move_potential(symbol, row, rapid)
    evidence = _early_evidence(row, rapid, big)

    layers = row.get("layer_results") or {}
    layer_count = sum(bool(layers.get(k)) for k in MAJOR_KEYS)
    hard = row.setdefault("hard_safety_status", {})
    ctx = _market_context()

    market_permission = ctx["regime"] != "SEVERE_RISK_OFF"
    hard["MARKET_REGIME_SAFETY"] = "PASS" if market_permission else "FAIL"

    extension_ok = hard.get("CUMULATIVE_EXTENSION_GUARD") == "PASS"
    execution_ok = all(hard.get(k) == "PASS" for k in EXECUTION_KEYS)
    strict_buy = bool(
        all(bool(layers.get(k)) for k in MAJOR_KEYS)
        and execution_ok
        and market_permission
    )

    evidence_count = sum(bool(v) for v in evidence.values())
    early_pre = bool(
        not strict_buy
        and row.get("micro_ready")
        and extension_ok
        and market_permission
        and layer_count >= PRE_MIN_LAYERS
        and layers.get("ACTIVITY_LAYER")
        and (layers.get("FLOW_LAYER") or layers.get("ORDER_BOOK_LAYER"))
        and (layers.get("MA_STRUCTURE_LAYER") or layers.get("VWAP_LAYER"))
        and evidence_count >= PRE_MIN_EARLY_EVIDENCE
        and float(big["score"]) >= PRE_MIN_BIG_MOVE_SCORE
        and row.get("state") != "LATE RUNNER"
    )

    old_state = row.get("state", "REJECT")
    strict_pre = old_state == "PRE-IGNITION" and extension_ok and market_permission
    early_opportunity = bool(
        not strict_buy
        and not early_pre
        and row.get("state") != "LATE RUNNER"
        and float(big["score"]) >= MEDIUM_BIG_MOVE_SCORE
        and (
            rapid.get("latent_ignition")
            or rapid.get("clustered_ignition")
            or evidence_count >= 2
        )
    )

    if strict_buy:
        state = "BUY NOW"
        pre_mode = None
    elif strict_pre:
        state = "PRE-IGNITION"
        pre_mode = "STRICT_5_OF_6"
    elif early_pre:
        state = "PRE-IGNITION"
        pre_mode = "EARLY_4_OF_6"
    elif old_state == "LATE RUNNER":
        state = old_state
        pre_mode = None
    elif early_opportunity:
        state = "EARLY OPPORTUNITY"
        pre_mode = None
    else:
        state = old_state if old_state != "BUY NOW" else "WATCH"
        pre_mode = None

    row["state"] = state
    row["pre_ignition_mode"] = pre_mode
    row["pre_ignition_layer_count"] = layer_count
    row["early_evidence"] = evidence
    row["early_evidence_count"] = evidence_count
    row["early_opportunity"] = early_opportunity
    row["big_move_potential"] = big
    row["big_move_potential_score"] = big["score"]
    row["big_move_potential_label"] = big["label"]
    row["market_regime"] = ctx
    row["resistance_pressure"] = big["resistance_pressure"]
    row["relative_strength"] = big["relative_strength"]
    row["ignition_sequence"] = big["sequence"]
    row["mandatory_all_aligned"] = strict_buy
    row["hard_safety_all_aligned"] = all(v == "PASS" for v in hard.values())

    if not market_permission:
        failed = row.setdefault("failed_hard", [])
        if "MARKET_REGIME_SAFETY" not in failed:
            failed.append("MARKET_REGIME_SAFETY")
    elif "MARKET_REGIME_SAFETY" in row.get("failed_hard", []):
        row["failed_hard"].remove("MARKET_REGIME_SAFETY")

    return row


def _radar_opportunity(rapid):
    symbol = rapid.get("symbol")
    if not symbol:
        return None
    now_t = time.time()
    ticker = v81.market_24h.get(symbol) or {}
    age = now_t - float(ticker.get("ts") or 0.0) if ticker else 9999.0
    if age > MARKET_DATA_MAX_AGE_SECONDS:
        return None

    change24 = float(ticker.get("change_pct") or 0.0)
    ctx = _market_context()
    rs24 = change24 - float(ctx["median_binance_24h_pct"])
    rs15 = float(rapid.get("r15") or 0.0) - float(ctx["btc_r15_pct"])
    sd = app.structure.get(symbol) or {}
    breakout = abs(float(sd.get("breakout_distance_pct") or 999.0))

    score = 0.0
    if change24 <= 3.0:
        score += 24
    elif change24 <= 10.0:
        score += 20
    elif change24 <= 20.0:
        score += 10
    elif change24 <= 35.0:
        score += 4

    if rapid.get("latent_ignition"):
        score += 20
    if rapid.get("clustered_ignition"):
        score += 10
    score += min(12.0, float(rapid.get("activity_price_divergence") or 0.0) / 20.0)
    score += min(
        12.0,
        int(rapid.get("burst_count_5m") or 0) * 1.2
        + int(rapid.get("burst_count_15m") or 0) * 0.25,
    )
    score += max(0.0, min(8.0, rs24 * 1.2))
    score += max(0.0, min(6.0, rs15 * 8.0))
    if breakout <= 1.0:
        score += 5
    if sd.get("ma_regime"):
        score += 3
    score += {"SUPPORTIVE": 5, "NEUTRAL": 3, "DEFENSIVE": 0, "SEVERE_RISK_OFF": -8}.get(ctx["regime"], 0)
    score = max(0.0, min(100.0, score))
    if change24 > 35.0:
        score = min(score, 35.0)

    label = "HIGH" if score >= HIGH_BIG_MOVE_SCORE else "MEDIUM" if score >= MEDIUM_BIG_MOVE_SCORE else "LOW"
    return {
        **rapid,
        "big_move_potential_score": round(score, 2),
        "big_move_potential_label": label,
        "change_24h_pct": round(change24, 3),
        "relative_strength_24h_pct": round(rs24, 3),
        "relative_strength_vs_btc_15s_pct": round(rs15, 4),
        "market_regime": ctx["regime"],
    }


def _opportunity_rows(limit=10):
    radar = v9._radar_rows(120)
    rows = []
    for r in radar:
        item = _radar_opportunity(r)
        if item is None:
            continue
        if (
            item["big_move_potential_score"] >= MEDIUM_BIG_MOVE_SCORE
            or item.get("latent_ignition")
            or item.get("clustered_ignition")
        ):
            rows.append(item)
    rows.sort(
        key=lambda x: (
            float(x.get("big_move_potential_score") or 0.0),
            bool(x.get("latent_ignition")),
            bool(x.get("clustered_ignition")),
            float(x.get("score") or 0.0),
        ),
        reverse=True,
    )
    return rows[:limit]


async def health(req):
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "EARLIER_PRE_IGNITION_WITH_STRICT_BUY_AND_BIG_MOVE_POTENTIAL",
        "pre_ignition_min_layers": PRE_MIN_LAYERS,
        "pre_ignition_min_early_evidence": PRE_MIN_EARLY_EVIDENCE,
        "pre_ignition_min_big_move_score": PRE_MIN_BIG_MOVE_SCORE,
        "buy_policy": "ALL_6_MAJOR_LAYERS_PLUS_LIVE_EXECUTION_PLUS_EXTENSION_PLUS_MARKET_SAFETY",
        "rapid_slots": v7.RAPID_MICRO_SLOTS,
        "micro_hold_seconds": q.MICRO_HOLD,
        "lock_grace_seconds": q.LOCK_GRACE,
        "ordered_ignition_sequence": True,
        "relative_strength": True,
        "resistance_pressure": True,
        "market_regime": _market_context(),
        "rapid_stream_reconnects": v9.stream_reconnects,
    }
    return app.web.json_response({
        "ok": True,
        "service": "psi-v10-live-scanner",
        "version": VERSION,
        "scanner_ready": app.scanner_ready,
        "websocket_connected": app.websocket_connected,
        "rapid_websocket_connected": v7.rapid_ws_connected,
        "coverage": c,
        "stable_qualifier_symbols": list(q.stable),
        "near_miss_diagnostics": s.near_diag(10),
        "early_opportunity_top": _opportunity_rows(10),
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", v7.TARGET)), v7.TARGET))
    except ValueError:
        limit = v7.TARGET

    app.resolve_outcomes()
    rows = s.results(limit)
    opportunities = _opportunity_rows(10)
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "EARLY_OPPORTUNITY_TO_PRE_TO_STRICT_BUY",
        "pre_ignition_min_layers": PRE_MIN_LAYERS,
        "pre_ignition_min_early_evidence": PRE_MIN_EARLY_EVIDENCE,
        "pre_ignition_min_big_move_score": PRE_MIN_BIG_MOVE_SCORE,
        "buy_policy": "ALL_6_MAJOR_LAYERS_PLUS_EXECUTION_PLUS_EXTENSION_PLUS_MARKET_SAFETY",
        "rapid_slots": v7.RAPID_MICRO_SLOTS,
        "market_regime": _market_context(),
        "rapid_stream_reconnects": v9.stream_reconnects,
    }
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.10 Early Opportunity + Ordered Ignition + Big Move Potential",
        "version": VERSION,
        "buy_policy": "STRICT_ALL_LAYER_BUY; EARLIER_4_OF_6_PRE_REQUIRES_STRONG_IGNITION_EVIDENCE",
        "returned": len(rows),
        "state_counts": dict(Counter(x["state"] for x in rows)),
        "coverage": c,
        "results": rows,
        "early_opportunity_top": opportunities,
        "big_move_potential_top": opportunities,
        "near_miss_diagnostics": s.near_diag(10),
        "missed_move_events": list(v7.missed_move_events)[-20:],
        "generated_ms": q.ms(),
    })


async def print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        rows = s.results()
        opportunities = _opportunity_rows(5)
        c = v7.coverage()
        h = c.get("hunt", {})
        ctx = _market_context()

        print("\n==================================================", flush=True)
        print(f"Ψ-V10.10 OPPORTUNITY + TARGET-10 — {len(rows)}/{v7.TARGET}", flush=True)
        print(
            f"attempted={h.get('structure_attempted',0)}/{c.get('full_universe',0)} "
            f"valid={h.get('structure_valid',0)} micro_verified={h.get('micro_candidates_verified',0)} "
            f"live_micro={c.get('micro_verified_fresh',0)}/{c.get('micro_total',0)} "
            f"rapid={len(v7.rapid_symbols)}/{v7.RAPID_MICRO_SLOTS} "
            f"market={ctx['regime']} stream_reconnects={v9.stream_reconnects}",
            flush=True,
        )
        print("==================================================", flush=True)

        for i, row in enumerate(rows, 1):
            print(
                f"{i:02d}. {row['symbol']:12s} {row['state']:14s} score={float(row.get('score') or 0):6.2f} "
                f"big={float(row.get('big_move_potential_score') or 0):5.1f}/{row.get('big_move_potential_label','LOW'):6s} "
                f"pre={str(row.get('pre_ignition_mode') or '-'):14s} "
                f"OFI={float(row.get('ofi') or 0):+.3f} OBI={float(row.get('obi') or 0):+.3f} "
                f"buy={float(row.get('aggressive_buy_ratio') or 0):.2%}",
                flush=True,
            )

        if not rows:
            print("No persistent PRE-IGNITION / BUY NOW setup currently qualifies.", flush=True)

        if opportunities:
            print("EARLY OPPORTUNITY / BIG MOVE POTENTIAL:", flush=True)
            for i, r in enumerate(opportunities, 1):
                print(
                    f"O{i:02d}. {r['symbol']:12s} big={float(r.get('big_move_potential_score') or 0):5.1f}/"
                    f"{str(r.get('big_move_potential_label','LOW')):6s} "
                    f"phase={str(r.get('phase','RADAR')):18s} "
                    f"24h={float(r.get('change_24h_pct') or 0):+.2f}% "
                    f"rs24={float(r.get('relative_strength_24h_pct') or 0):+.2f}% "
                    f"b5={r.get('burst_count_5m',0)} b15={r.get('burst_count_15m',0)} "
                    f"r5={float(r.get('r5') or 0):+.2f}% "
                    f"v5x={float(r.get('vol_accel_5') or 0):.2f} "
                    f"t5x={float(r.get('trade_accel_5') or 0):.2f}",
                    flush=True,
                )

        diagnostics = s.near_diag(10)
        if diagnostics:
            print("TOP NEAR MISSES:", flush=True)
            for i, d in enumerate(diagnostics, 1):
                print(
                    f"N{i:02d}. {str(d.get('symbol')):12s} state={str(d.get('state')):18s} "
                    f"score={float(d.get('score',0) or 0):6.2f} micro={d.get('micro_ready')} "
                    f"hard={d.get('failed_hard',[])} layers={d.get('failed_layers',d.get('failed_setup',[]))}",
                    flush=True,
                )


app.evaluate_symbol = evaluate
app.health = health
app.scan_endpoint = scan
app.ranked_results = s.results
v7.rapid_websocket_loop = v9.persistent_rapid_websocket_loop
v7.print_loop = print_loop
q.print_loop = print_loop
s.print_loop = print_loop
base.ticker_loop = v92.ticker_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.10 ACTIVE — early opportunity + 4/6 evidence-gated PRE + strict BUY + "
            "ordered ignition sequence + big-move potential + relative strength + market regime",
            flush=True,
        )
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.10 stopped", flush=True)
