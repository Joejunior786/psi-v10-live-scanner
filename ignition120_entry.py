import asyncio
import math
import time
import statistics

import ignition1192_entry as base

scanner, q, app = base.scanner, base.q, base.app

VERSION = "10.20.0-multi-regime-buy-engine"

HARD_GATES = (
    "LIVE_MICRO_DATA",
    "TRADE_SEQUENCE_VALID",
    "BOOK_SEQUENCE_VALID",
    "SPREAD_FILTER",
    "SLIPPAGE_FILTER",
    "CUMULATIVE_EXTENSION_GUARD",
    "MARKET_REGIME_SAFETY",
)

SETUP_ORDER = (
    "LIQUIDITY_SWEEP_REVERSAL",
    "BREAKOUT_RETEST_CONTINUATION",
    "COMPRESSION_BREAKOUT",
    "TREND_MA_PULLBACK",
    "MEAN_REVERSION",
    "RUNNER_SECOND_IGNITION",
)

_old_evaluate = app.evaluate_symbol
_old_main = scanner.v7.main
_old_pb_eval = base.pb_eval
_old_fetch_pb = base.fetch_pb
_old_pb_symbols = base.pb_symbols
_old_diag = getattr(base.b17, "diag_pool", None)


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if a and b else 0.0


def passed(v):
    if v is True:
        return True
    return str(v or "").upper() == "PASS"


def result(gates):
    total = len(gates)
    count = sum(bool(v) for v in gates.values())
    return {
        "gates": gates,
        "pass_count": count,
        "total": total,
        "pass_ratio": round(count / total, 4) if total else 0.0,
        "all_aligned": bool(total and count == total),
        "failed": [k for k, v in gates.items() if not v],
    }


def fresh(d, max_age):
    if not isinstance(d, dict) or not d:
        return {}
    updated = f(d.get("updated"), 0.0)
    if updated and time.time() - updated > max_age:
        return {}
    return d


def risk_intel(sym):
    try:
        return base.base.risk_intel(sym) or {}
    except Exception:
        return {}


def mtf5(sym):
    try:
        frames = ((base.base.base.mtf_cache.get(sym) or {}).get("frames") or {})
        return frames.get("5m") or {}
    except Exception:
        return {}


def mean_reversion_context(sym, c5, c15):
    z5 = c5[:-1] if len(c5) > 1 else c5
    z15 = c15[:-1] if len(c15) > 1 else c15
    if len(z5) < 36 or len(z15) < 24:
        return {
            "ready": False,
            "oversold": False,
            "sell_exhaustion": False,
            "zscore_5m": None,
            "atr_deviation": None,
        }

    closes = [f(x.get("c")) for x in z5[-32:] if f(x.get("c")) > 0]
    if len(closes) < 24:
        return {"ready": False, "oversold": False, "sell_exhaustion": False}

    p = base.price(sym) or closes[-1]
    mean = sum(closes) / len(closes)
    stdev = statistics.pstdev(closes) if len(closes) >= 2 else 0.0
    zscore = (p - mean) / stdev if stdev > 0 else 0.0
    a = base.atr(z5)
    atr_dev = (p - mean) / a if a > 0 else 0.0
    vw15 = base.vwap(z15)

    r_prev = pct(z5[-2]["c"], z5[-3]["c"]) if len(z5) >= 3 else 0.0
    r_last = pct(z5[-1]["c"], z5[-2]["c"]) if len(z5) >= 2 else 0.0
    sell_exhaustion = bool(
        (r_prev < 0 and r_last > r_prev)
        or (r_last >= 0 and min(r_prev, r_last) > -4.0)
    )

    prev_mid = (f(z5[-1].get("o")) + f(z5[-1].get("c"))) / 2.0
    reversal_level = max(f(z5[-1].get("c")), prev_mid)
    oversold = bool(zscore <= -1.65 or atr_dev <= -1.35)

    return {
        "ready": True,
        "updated": time.time(),
        "price": p,
        "mean_5m_32": mean,
        "stdev_5m_32": stdev,
        "zscore_5m": round(zscore, 4),
        "atr_5m": a,
        "atr_deviation": round(atr_dev, 4),
        "vwap_15m": vw15,
        "distance_to_mean_pct": round(pct(p, mean), 4) if mean else None,
        "oversold": oversold,
        "sell_exhaustion": sell_exhaustion,
        "r_prev_5m_pct": round(r_prev, 4),
        "r_last_5m_pct": round(r_last, 4),
        "reversal_level": reversal_level,
    }


def pb_eval_with_mean_reversion(sym, tm, c5, c15):
    out = _old_pb_eval(sym, tm, c5, c15)
    if not isinstance(out, dict):
        out = {}
    try:
        out["mean_reversion"] = mean_reversion_context(sym, c5, c15)
    except Exception:
        out["mean_reversion"] = {"ready": False, "oversold": False, "sell_exhaustion": False}
    return out


base.pb_eval = pb_eval_with_mean_reversion


async def fetch_pb_extended(sym):
    """Reuse the existing pullback candle fetch for trends and also maintain
    mean-reversion context for hot non-trending candidates."""
    tm = base.trend_cache.get(sym) or {}
    trend_state = str(tm.get("trend_state") or "UNKNOWN")
    if trend_state in {"UPTREND", "TREND-WEAK"}:
        return await _old_fetch_pb(sym)
    if app.session is None:
        return
    try:
        r5 = await app.load_klines(app.session, sym, "5m", 84)
        r15 = await app.load_klines(app.session, sym, "15m", 84)
        c5, c15 = base.candles(r5), base.candles(r15)
        mr = mean_reversion_context(sym, c5, c15)
        base.pb_cache[sym] = {
            "symbol": sym,
            "updated": time.time(),
            "state": "RANGE_OR_REVERSAL_MONITOR",
            "score": 0.0,
            "trend_state": trend_state,
            "trend_score": f(tm.get("trend_score")),
            "depth": f(tm.get("depth"), 999.0),
            "zone_dist": 999.0,
            "sell_ratio": 999.0,
            "mean_reversion": mr,
        }
    except asyncio.CancelledError:
        raise
    except Exception:
        base.stats["errors"] = int(base.stats.get("errors", 0)) + 1


def pb_symbols_extended():
    existing = list(_old_pb_symbols() or [])
    priority = []
    try:
        priority = list(base.hot(10) or [])
    except Exception:
        priority = []
    out = []
    for sym in priority + existing:
        if sym not in out:
            out.append(sym)
        if len(out) >= base.PB_MAX:
            break
    return out


base.fetch_pb = fetch_pb_extended
base.pb_symbols = pb_symbols_extended


def setup_context(sym, row):
    layers = row.get("layer_results") or {}
    sd = app.structure.get(sym) or {}
    ri = risk_intel(sym)
    tm = fresh(base.trend_cache.get(sym) or {}, base.TREND_AGE)
    pb = fresh(base.pb_cache.get(sym) or {}, base.PB_AGE)
    mr = pb.get("mean_reversion") or {}
    frame5 = mtf5(sym)

    price = f(row.get("price"), f(sd.get("price")))
    vwap60 = f(row.get("vwap_60s"))
    aggressive = f(row.get("aggressive_buy_ratio"), 0.5)
    ofi = f(row.get("ofi"))
    ofi_acc = f(row.get("ofi_acceleration"))
    cvd = f(row.get("cvd_quote_60s"))
    cvd_acc = f(row.get("cvd_acceleration"))
    obi = f(row.get("obi"))
    ask_dep = f(row.get("ask_depletion"))
    ask_dep30 = f(row.get("ask_depletion_30s_pct"))

    activity = bool(layers.get("ACTIVITY_LAYER"))
    flow = bool(layers.get("FLOW_LAYER"))
    book = bool(layers.get("ORDER_BOOK_LAYER"))
    vwap_layer = bool(layers.get("VWAP_LAYER"))
    ma_layer = bool(layers.get("MA_STRUCTURE_LAYER"))
    anti_layer = bool(layers.get("ANTI_CHASE_OR_RUNNER_LAYER"))

    real_book_pressure = bool(
        book
        or obi >= 0.05
        or ask_dep30 >= 10.0
        or (ask_dep > 0 and obi >= 0.03)
    )
    live_flow_turn = bool(
        flow
        or (ofi_acc > 0 and aggressive >= 0.53)
        or (cvd_acc >= 0 and aggressive >= 0.55)
    )
    flow_reversal = bool(
        ofi_acc > 0
        and aggressive >= 0.52
        and (cvd_acc >= 0 or cvd > 0 or flow)
    )

    sweep_state = str(ri.get("sweep_state") or "WAIT")
    support1 = f(ri.get("support1"))
    stop_cluster = f(ri.get("stop_cluster"))
    support_anchor = support1 or stop_cluster
    support_near = bool(
        support_anchor > 0
        and price > 0
        and -1.5 <= pct(price, support_anchor) <= 4.0
    )

    trend_state = str(tm.get("trend_state") or "UNKNOWN")
    phase5 = str(frame5.get("phase") or "UNKNOWN")
    used_level = f(frame5.get("used_level"))
    break_age = f(frame5.get("break_age"), 999999.0)

    return {
        "layers": layers,
        "sd": sd,
        "ri": ri,
        "tm": tm,
        "pb": pb,
        "mr": mr,
        "frame5": frame5,
        "price": price,
        "vwap60": vwap60,
        "aggressive": aggressive,
        "ofi": ofi,
        "ofi_acc": ofi_acc,
        "cvd": cvd,
        "cvd_acc": cvd_acc,
        "obi": obi,
        "ask_dep": ask_dep,
        "ask_dep30": ask_dep30,
        "activity": activity,
        "flow": flow,
        "book": book,
        "real_book_pressure": real_book_pressure,
        "vwap_layer": vwap_layer,
        "ma_layer": ma_layer,
        "anti_layer": anti_layer,
        "live_flow_turn": live_flow_turn,
        "flow_reversal": flow_reversal,
        "sweep_state": sweep_state,
        "support_anchor": support_anchor,
        "support_near": support_near,
        "trend_state": trend_state,
        "phase5": phase5,
        "used_level": used_level,
        "break_age": break_age,
    }


def build_setups(sym, row):
    c = setup_context(sym, row)
    sd, ri, tm, pb, mr = c["sd"], c["ri"], c["tm"], c["pb"], c["mr"]
    p = c["price"]

    compression = {
        "MA_REGIME": bool(sd.get("ma_regime") or c["ma_layer"]),
        "TRUE_COMPRESSION": bool(sd.get("compression")),
        "NEAR_OR_BREAKING_RESISTANCE": bool(sd.get("breakout_near") or sd.get("breakout")),
        "ACTIVITY_ACCELERATION": c["activity"],
        "FLOW_CONFIRMATION": c["flow"],
        "REAL_ORDER_BOOK_PRESSURE": c["real_book_pressure"],
        "VWAP_HOLD_OR_RECLAIM": c["vwap_layer"] or bool(row.get("vwap_reclaim")),
        "ANTI_CHASE_CLEAR": bool(c["anti_layer"] and not sd.get("anti_chase")),
    }

    pb_state = str(pb.get("state") or "")
    depth = f(pb.get("depth"), 999.0)
    zone_dist = f(pb.get("zone_dist"), 999.0)
    sell_ratio = f(pb.get("sell_ratio"), 999.0)
    pullback = {
        "UPTREND_1H_4H": c["trend_state"] == "UPTREND",
        "HEALTHY_PULLBACK_DEPTH": base.MIN_DEPTH <= depth <= base.MAX_DEPTH,
        "NEAR_MA_OR_SUPPORT_ZONE": zone_dist <= 1.25,
        "SELL_PRESSURE_CONTROLLED": sell_ratio <= 1.25,
        "SUPPORT_NOT_LOST": c["sweep_state"] != "SUPPORT_LOST",
        "RECLAIM_CONFIRMED": bool(
            pb_state in {"RECLAIM_PENDING", "PULLBACK_ARMED", "PULLBACK_BUY"}
            or c["sweep_state"] == "SWEEP_RECLAIMED"
        ),
        "BUYERS_RETURNING": bool(c["live_flow_turn"] and c["aggressive"] >= 0.53),
        "BOOK_OR_FLOW_SUPPORT": bool(c["real_book_pressure"] or c["flow"]),
        "ANTI_CHASE_CLEAR": bool(not sd.get("anti_chase")),
    }

    reversal_level = f(mr.get("reversal_level"))
    local_trigger = bool(
        row.get("vwap_reclaim")
        or (reversal_level > 0 and p >= reversal_level * 1.0002)
    )
    structure_intact = bool(
        c["sweep_state"] != "SUPPORT_LOST"
        and (
            c["support_near"]
            or c["trend_state"] in {"UPTREND", "TREND-WEAK"}
            or bool(sd.get("structural_support"))
        )
    )
    mean_reversion = {
        "STATISTICAL_EXTREME": bool(mr.get("ready") and mr.get("oversold")),
        "STRUCTURE_OR_SUPPORT_INTACT": structure_intact,
        "SELLING_EXHAUSTION": bool(mr.get("sell_exhaustion")),
        "FLOW_REVERSAL": c["flow_reversal"],
        "BOOK_RECOVERY": bool(c["real_book_pressure"] or c["obi"] >= 0.03),
        "LOCAL_RECLAIM_TRIGGER": local_trigger,
        "NOT_ALREADY_CHASING": bool(not sd.get("anti_chase")),
    }

    cluster = f(ri.get("stop_cluster"))
    sweep_distance_ok = bool(cluster > 0 and p > 0 and 0 <= pct(p, cluster) <= 3.0)
    liquidity_sweep = {
        "SWEEP_RECLAIMED": c["sweep_state"] == "SWEEP_RECLAIMED",
        "SUPPORT_RECLAIMED": bool(cluster > 0 and p >= cluster * 1.0002),
        "ENTRY_STILL_NEAR_SWEEP": sweep_distance_ok,
        "FLOW_FLIPPED_POSITIVE": c["flow_reversal"],
        "BOOK_RECOVERY": bool(c["real_book_pressure"] or c["obi"] >= 0.03),
        "BUY_DOMINANCE_RECOVERY": c["aggressive"] >= 0.53,
    }

    retest_zone = bool(
        c["used_level"] > 0
        and p > 0
        and p >= c["used_level"] * 0.997
        and p <= c["used_level"] * 1.020
    )
    breakout_retest = {
        "RECENT_BREAKOUT_EXISTS": bool(c["used_level"] > 0 and c["break_age"] <= 3600),
        "RETEST_OR_HOLD_PHASE": c["phase5"] in {"RETEST", "BREAKOUT_HOLD"},
        "FORMER_RESISTANCE_HOLDING": retest_zone,
        "MA_OR_TREND_SUPPORT": bool(
            sd.get("ma_regime")
            or c["ma_layer"]
            or c["trend_state"] == "UPTREND"
        ),
        "FLOW_CONFIRMATION": c["live_flow_turn"],
        "REAL_ORDER_BOOK_PRESSURE": c["real_book_pressure"],
        "VWAP_OR_LOCAL_HOLD": bool(c["vwap_layer"] or row.get("vwap_reclaim") or retest_zone),
        "ANTI_CHASE_CLEAR": bool(not sd.get("anti_chase")),
    }

    rapid = row.get("rapid_ignition") or {}
    r5 = f(rapid.get("r5"))
    r15 = f(rapid.get("r15"))
    v5 = f(rapid.get("vol_accel_5"))
    runner_structure = bool(
        row.get("runner_second_ignition")
        or (
            c["phase5"] in {"CONTINUATION", "BREAKOUT_HOLD"}
            and bool(sd.get("breakout") or (row.get("structure_setups") or {}).get("RUNNER_SECOND_IGNITION"))
        )
    )
    runner = {
        "SECOND_IGNITION_STRUCTURE": runner_structure,
        "CONTROLLED_5M_ACCELERATION": 0.05 <= r5 <= 2.0,
        "VOLUME_REACCELERATION": bool(v5 >= 1.5 or c["activity"]),
        "FLOW_CONFIRMATION": c["flow"],
        "REAL_ORDER_BOOK_PRESSURE": c["real_book_pressure"],
        "VWAP_HOLD": c["vwap_layer"],
        "MOMENTUM_NOT_ROLLING_OVER": r15 >= -0.25,
    }

    return {
        "COMPRESSION_BREAKOUT": result(compression),
        "TREND_MA_PULLBACK": result(pullback),
        "MEAN_REVERSION": result(mean_reversion),
        "LIQUIDITY_SWEEP_REVERSAL": result(liquidity_sweep),
        "BREAKOUT_RETEST_CONTINUATION": result(breakout_retest),
        "RUNNER_SECOND_IGNITION": result(runner),
    }, c


def evaluate_multi_regime(symbol):
    row = _old_evaluate(symbol)
    if not row:
        return row

    legacy_state = str(row.get("state") or "REJECT")
    legacy_formal = str(
        row.get("formal_state")
        or row.get("pre_warmup_state")
        or legacy_state
    )
    row["legacy_state_before_multi_regime"] = legacy_state
    row["legacy_formal_state_before_multi_regime"] = legacy_formal

    hard = row.get("hard_safety_status") or {}
    hard_checks = {k: passed(hard.get(k)) for k in HARD_GATES}
    collection_ready = bool(row.get("micro_collection_ready", row.get("micro_ready")))
    hard_checks["QUALIFIED_MICRO_WARMUP"] = collection_ready
    hard_ok = all(hard_checks.values())

    setups, ctx = build_setups(symbol, row)
    aligned = [name for name in SETUP_ORDER if setups[name]["all_aligned"]]

    legacy_buy = legacy_formal == "BUY NOW" and hard_ok
    buy = bool(hard_ok and (aligned or legacy_buy))

    ranked = sorted(
        SETUP_ORDER,
        key=lambda name: (
            setups[name]["all_aligned"],
            setups[name]["pass_ratio"],
            setups[name]["pass_count"],
        ),
        reverse=True,
    )
    best_name = aligned[0] if aligned else (ranked[0] if ranked else "NO_SETUP")
    best = setups.get(best_name, {"pass_ratio": 0.0, "gates": {}, "failed": []})

    if buy:
        active = aligned[0] if aligned else (row.get("active_setup") or "LEGACY_FULL_CONFLUENCE")
        row["state"] = "BUY NOW"
        row["pre_warmup_state"] = "BUY NOW"
        row["formal_state"] = "BUY NOW"
    else:
        active = best_name
        ratio = f(best.get("pass_ratio"))
        if hard_ok and ratio >= 0.80:
            if legacy_state not in {"BUY NOW", "PRE-IGNITION"}:
                row["state"] = "PRE-IGNITION"
                row["pre_warmup_state"] = "PRE-IGNITION"
                row["formal_state"] = "PRE-IGNITION"
        elif collection_ready and ratio >= 0.60:
            if legacy_state in {"REJECT", "COLLECTING DATA", "EARLY OPPORTUNITY"}:
                row["state"] = "WATCH"
                row["pre_warmup_state"] = "WATCH"
                row["formal_state"] = "WATCH"

    row["active_setup"] = active
    row["decision_model"] = "ONE_COMPLETE_REGIME_SETUP_PLUS_ALL_UNIVERSAL_HARD_SAFETY"
    row["multi_regime_version"] = VERSION
    row["multi_regime_buy"] = buy
    row["multi_regime_aligned_setups"] = aligned
    row["multi_regime_best_setup"] = best_name
    row["multi_regime_setup_results"] = setups
    row["setup_results"] = setups
    row["multi_regime_hard_checks"] = hard_checks
    row["hard_safety_all_aligned"] = hard_ok

    failed_hard = [k for k, v in hard_checks.items() if not v]
    row["failed_hard"] = failed_hard
    row["failed_setup"] = best.get("failed", [])
    row["mandatory_all_aligned"] = buy

    hard_pass_count = sum(hard_checks.values())
    setup_pass_count = int(best.get("pass_count", 0))
    total = len(hard_checks) + int(best.get("total", 0))
    passed_total = hard_pass_count + setup_pass_count
    row["mandatory_pass_count"] = passed_total
    row["mandatory_total"] = total
    row["mandatory_pass_ratio"] = round(passed_total / total, 4) if total else 0.0

    row["mean_reversion"] = ctx["mr"]
    row["trend_pullback_state"] = (ctx["pb"] or {}).get("state")
    row["trend_state"] = ctx["trend_state"]
    row["sweep_state"] = ctx["sweep_state"]
    row["mtf_5m_phase"] = ctx["phase5"]
    row["mtf_5m_used_level"] = ctx["used_level"]
    row["real_order_book_pressure"] = ctx["real_book_pressure"]
    row["flow_reversal_confirmed"] = ctx["flow_reversal"]
    row["buy_policy"] = "ALL_UNIVERSAL_HARD_GATES_AND_ALL_GATES_OF_ONE_REGIME_SETUP"

    return row


app.evaluate_symbol = evaluate_multi_regime


if _old_diag:
    def diag_pool(*args, **kwargs):
        out = _old_diag(*args, **kwargs)
        if isinstance(out, dict):
            out["multi_regime_version"] = VERSION
            out["multi_regime_buy_policy"] = "ALL_UNIVERSAL_HARD_GATES_AND_ALL_GATES_OF_ONE_REGIME_SETUP"
            out["multi_regime_setups"] = list(SETUP_ORDER)
        return out

    base.b17.diag_pool = diag_pool


base.VERSION = VERSION
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        "[v10.20] multi-regime BUY engine active: "
        "compression | trend-pullback | mean-reversion | liquidity-sweep | "
        "breakout-retest | second-ignition",
        flush=True,
    )
    await _old_main()


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Psi-V10.20 stopped", flush=True)
