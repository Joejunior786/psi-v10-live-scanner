import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque

import psi_v11_3_2_entry as base

app = base.app
q = base.q
scanner = base.scanner
learn = base.learn
riskmap = base.riskmap
pullback = base.pullback

VERSION = "11.0.3.5-pinpoint-execution"
PINPOINT_SAMPLE_SECONDS = 4.0
PINPOINT_HISTORY = 3
PINPOINT_REQUIRED_PASSES = 2
PINPOINT_MAX_OVERSHOOT_PCT = 0.80
PINPOINT_ARM_DISTANCE_PCT = 0.45
PINPOINT_BOARD_SECONDS = 30.0
ABLATION_HORIZON = 3600.0
ABLATION_COOLDOWN = 900.0
ABLATION_PATH = os.environ.get("PSI_PINPOINT_OUTCOMES", "/data/psi_pinpoint_outcomes.jsonl")

_old_evaluate = app.evaluate_symbol

_tape_history = defaultdict(lambda: deque(maxlen=PINPOINT_HISTORY))
_tape_last_sample = defaultdict(float)
_ablation_pending = []
_ablation_last = defaultdict(float)
_ablation_resolved = deque(maxlen=2000)
_stats = defaultdict(int)

SETUP_ORDER = (
    "LIQUIDITY_SWEEP_REVERSAL",
    "BREAKOUT_RETEST_CONTINUATION",
    "TREND_MA_PULLBACK",
    "COMPRESSION_BREAKOUT",
    "MEAN_REVERSION",
    "RUNNER_SECOND_IGNITION",
)

CORE_HARD_GATES = (
    "LIVE_MICRO_DATA",
    "TRADE_SEQUENCE_VALID",
    "BOOK_SEQUENCE_VALID",
    "SPREAD_FILTER",
    "SLIPPAGE_FILTER",
    "CUMULATIVE_EXTENSION_GUARD",
    "MARKET_REGIME_SAFETY",
    "QUALIFIED_MICRO_WARMUP",
    "FRESH_STRUCTURE",
)


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def passed(value):
    if value is True:
        return True
    return str(value or "").upper() == "PASS"


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if a and b else 0.0


def _median_mad(values):
    xs = []
    for value in values or []:
        try:
            x = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(x):
            xs.append(x)
    if len(xs) < 8:
        return None, None
    med = statistics.median(xs)
    mad = statistics.median(abs(x - med) for x in xs)
    return med, mad


def _robust_z(sym, key, current):
    try:
        hist = app.micro_state.get(sym, {}).get("metrics_history", {}).get(key, ())
        med, mad = _median_mad(hist)
        if med is None or mad is None or mad <= 1e-12:
            return 0.0
        return clamp((f(current) - med) / (1.4826 * mad), -4.0, 4.0)
    except Exception:
        return 0.0


def _fresh_mtf(sym):
    try:
        block = learn.mtf_cache.get(sym) or {}
        age = max(0.0, time.time() - f(block.get("updated"), 0.0))
        if not block or age > 95.0:
            return {}, 999999.0
        return block.get("frames") or {}, age
    except Exception:
        return {}, 999999.0


def _pb(sym):
    try:
        x = pullback.pb_intel(sym)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _risk(sym):
    try:
        x = riskmap.risk_intel(sym)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _rapid(row):
    x = (row or {}).get("rapid_ignition") or {}
    return x if isinstance(x, dict) else {}


def _weighted_obi(sym, levels=10, decay=0.35):
    """Distance-weighted L1-L10 book imbalance; near-touch liquidity matters most."""
    try:
        state = app.micro_state.get(sym, {}) or {}
        bids_map = state.get("book_bids") or {}
        asks_map = state.get("book_asks") or {}
        bids = [(px, bids_map[px]) for px in sorted(bids_map, reverse=True)[:levels] if f(px) > 0 and f(bids_map[px]) > 0]
        asks = [(px, asks_map[px]) for px in sorted(asks_map)[:levels] if f(px) > 0 and f(asks_map[px]) > 0]
        if not bids or not asks:
            return None
        bid_n = 0.0
        ask_n = 0.0
        for i, (px, qty) in enumerate(bids):
            bid_n += math.exp(-decay * i) * f(px) * f(qty)
        for i, (px, qty) in enumerate(asks):
            ask_n += math.exp(-decay * i) * f(px) * f(qty)
        total = bid_n + ask_n
        return (bid_n - ask_n) / total if total > 0 else None
    except Exception:
        return None


def _current_tape(sym, row):
    buy_ratio = clamp(f(row.get("aggressive_buy_ratio"), 0.5), 0.0, 1.0)
    delta = 2.0 * buy_ratio - 1.0
    ofi = f(row.get("ofi"))
    ofi_acc = f(row.get("ofi_acceleration"))
    cvd_acc = f(row.get("cvd_acceleration"))
    raw_obi = f(row.get("obi"))
    weighted_obi = _weighted_obi(sym)
    obi = raw_obi if weighted_obi is None else f(weighted_obi)
    ask_dep = f(row.get("ask_depletion"))
    rv30 = f(row.get("relative_volume_30s"))
    trade_acc = f(row.get("trade_acceleration"))

    z_ofi = _robust_z(sym, "ofi", ofi)
    z_obi = _robust_z(sym, "obi", obi)
    z_rv = _robust_z(sym, "rv30", rv30)
    z_trade = _robust_z(sym, "trade_acc", trade_acc)

    delta_score = clamp(50.0 + delta * 220.0, 0.0, 100.0)
    ofi_score = clamp(50.0 + (18.0 * z_ofi if ofi > 0 else min(0.0, 12.0 * z_ofi)) + ofi * 85.0, 0.0, 100.0)
    accel_sign = 1.0 if (ofi_acc > 0 and cvd_acc >= 0) else (0.5 if (ofi_acc > 0 or cvd_acc >= 0) else -1.0)
    accel_score = clamp(50.0 + accel_sign * 22.0 + clamp(ofi_acc * 60.0, -18.0, 18.0), 0.0, 100.0)
    activity_score = clamp(50.0 + max(0.0, z_rv) * 12.0 + max(0.0, z_trade) * 10.0 + (rv30 - 1.0) * 18.0 + (trade_acc - 1.0) * 12.0, 0.0, 100.0)
    book_score = clamp(50.0 + (15.0 * z_obi if obi > 0 else min(0.0, 10.0 * z_obi)) + obi * 80.0 + max(0.0, ask_dep) * 35.0, 0.0, 100.0)

    score = (
        0.35 * delta_score
        + 0.25 * ofi_score
        + 0.15 * accel_score
        + 0.15 * activity_score
        + 0.10 * book_score
    )

    direction_ok = bool(delta > 0.0 and not (ofi < 0.0 and ofi_acc <= 0.0))
    flow_ok = bool(direction_ok and score >= 57.5 and (ofi > 0.0 or ofi_acc > 0.0 or cvd_acc >= 0.0))

    contradictions = []
    if delta <= -0.04 and ofi < 0.0:
        contradictions.append("AGGRESSIVE_SELLERS_PLUS_NEGATIVE_OFI")
    if ofi <= -0.10 and obi <= -0.05:
        contradictions.append("STRONG_NEGATIVE_FLOW_AND_BOOK")
    if delta < 0.0 and ofi_acc <= 0.0 and cvd_acc < 0.0:
        contradictions.append("FLOW_ACCELERATION_ROLLOVER")
    if obi <= -0.18 and ofi < 0.0:
        contradictions.append("HEAVY_ASK_SIDE_DOMINANCE")

    return {
        "buy_ratio": buy_ratio,
        "aggressive_delta": delta,
        "ofi": ofi,
        "ofi_acc": ofi_acc,
        "cvd_acc": cvd_acc,
        "obi": obi,
        "raw_obi": raw_obi,
        "weighted_obi": weighted_obi,
        "ask_dep": ask_dep,
        "rv30": rv30,
        "trade_acc": trade_acc,
        "z_ofi": z_ofi,
        "z_obi": z_obi,
        "z_rv30": z_rv,
        "z_trade_acc": z_trade,
        "delta_score": round(delta_score, 2),
        "ofi_score": round(ofi_score, 2),
        "acceleration_score": round(accel_score, 2),
        "activity_score": round(activity_score, 2),
        "book_score": round(book_score, 2),
        "score": round(score, 2),
        "flow_ok": flow_ok,
        "contradictions": contradictions,
    }


def _record_tape(sym, tape_ok):
    now = time.time()
    dq = _tape_history[sym]
    if now - _tape_last_sample[sym] >= PINPOINT_SAMPLE_SECONDS:
        dq.append((now, bool(tape_ok)))
        _tape_last_sample[sym] = now
    elif dq:
        dq[-1] = (dq[-1][0], bool(tape_ok))
    vals = [bool(x[1]) for x in dq]
    passes = sum(vals)
    return {
        "samples": len(vals),
        "passes": passes,
        "latest": bool(vals[-1]) if vals else False,
        "ok": bool(vals and vals[-1] and passes >= PINPOINT_REQUIRED_PASSES),
    }


def _core_hard(row):
    hard = dict(row.get("multi_regime_hard_checks") or {})
    raw_hard = row.get("hard_safety_status") or {}
    status = {}
    for key in CORE_HARD_GATES:
        if key == "FRESH_STRUCTURE":
            value = hard.get(key, row.get("fresh_structure_121"))
        elif key == "QUALIFIED_MICRO_WARMUP":
            value = hard.get(key, row.get("micro_collection_ready", row.get("micro_ready")))
        else:
            value = hard.get(key, raw_hard.get(key))
        if key == "MARKET_REGIME_SAFETY" and value is None:
            value = True
        status[key] = passed(value)
    return status, [k for k, ok in status.items() if not ok]


def _setup_models(sym, row, tape):
    sd = app.structure.get(sym) or {}
    ri = _risk(sym)
    pb = _pb(sym)
    frames, mtf_age = _fresh_mtf(sym)
    f1 = frames.get("1m") or {}
    f3 = frames.get("3m") or {}
    f5 = frames.get("5m") or {}
    f15 = frames.get("15m") or {}
    rapid = _rapid(row)
    price = f(row.get("price"), f(sd.get("price")))
    vwap = f(row.get("vwap_60s"))
    phase5 = str(f5.get("phase") or row.get("mtf_5m_phase") or "UNKNOWN")
    trend_state = str(row.get("trend_state") or pb.get("trend_state") or "UNKNOWN")
    sweep_state = str(ri.get("sweep_state") or row.get("sweep_state") or "WAIT")
    mr = row.get("mean_reversion") or {}

    activity_now = bool(
        tape["rv30"] >= 1.0
        or tape["trade_acc"] >= 1.0
        or f(sd.get("volume_acceleration_15m")) >= 1.10
    )
    vwap_hold = bool(vwap > 0 and price >= vwap)
    mtf_positive = phase5 in {"APPROACH", "BREAKOUT_IN_PROGRESS", "BREAKOUT_HOLD", "RETEST", "CONTINUATION"}
    ma_or_mtf = bool(sd.get("ma_regime") or (mtf_age <= 95 and mtf_positive and str(f15.get("phase") or "") not in {"FAILED_BREAKOUT", "REJECT_FALLING"}))

    compression_trigger = f(f5.get("resistance")) or f(f3.get("resistance")) or f(ri.get("resistance1"))
    compression = {
        "qualified": bool(sd.get("compression") and compression_trigger > 0 and ma_or_mtf and activity_now and vwap_hold),
        "trigger": compression_trigger,
        "reason": "COMPRESSION+LOCAL_RESISTANCE+ACTIVITY+VWAP",
    }

    used5 = f(f5.get("used_level"))
    retest_zone = bool(used5 > 0 and price >= used5 * 0.997 and price <= used5 * 1.0125)
    retest = {
        "qualified": bool(phase5 in {"RETEST", "BREAKOUT_HOLD"} and used5 > 0 and retest_zone and sweep_state != "SUPPORT_LOST" and (ma_or_mtf or trend_state == "UPTREND")),
        "trigger": used5,
        "reason": "VERIFIED_5M_BREAKOUT_RETEST",
    }

    pb_state = str(pb.get("state") or "")
    pb_entry = f(pb.get("entry"))
    pullback_ok = bool(
        trend_state == "UPTREND"
        and pb_state in {"PULLBACK_ARMED", "PULLBACK_BUY"}
        and f(pb.get("zone_dist"), 99.0) <= 1.25
        and f(pb.get("sell_ratio"), 99.0) <= 1.25
        and sweep_state != "SUPPORT_LOST"
        and pb_entry > 0
    )
    trend_pullback = {
        "qualified": pullback_ok,
        "trigger": pb_entry,
        "reason": "UPTREND+ACTUAL_RECLAIM+CONTROLLED_PULLBACK",
    }

    reversal = f(mr.get("reversal_level"))
    mr_support = bool(
        sweep_state != "SUPPORT_LOST"
        and (
            f(ri.get("support1")) > 0
            or bool(sd.get("structural_support"))
            or trend_state in {"UPTREND", "TREND-WEAK"}
        )
    )
    mean_reversion = {
        "qualified": bool(mr.get("ready") and mr.get("oversold") and mr.get("sell_exhaustion") and mr_support and reversal > 0),
        "trigger": reversal,
        "reason": "STATISTICAL_EXTREME+EXHAUSTION+REVERSAL_LEVEL",
    }

    cluster = f(ri.get("stop_cluster"))
    sweep = {
        "qualified": bool(sweep_state == "SWEEP_RECLAIMED" and cluster > 0 and 0.0 <= pct(price, cluster) <= 3.0),
        "trigger": cluster * 1.0002 if cluster > 0 else 0.0,
        "reason": "LIQUIDITY_SWEEP+RECLAIM",
    }

    runner_trigger = f(f1.get("resistance")) or f(f3.get("resistance")) or f(ri.get("resistance1"))
    r5 = f(rapid.get("r5"))
    v5 = f(rapid.get("vol_accel_5"))
    runner_structure = bool(
        phase5 in {"CONTINUATION", "BREAKOUT_HOLD"}
        and (
            row.get("runner_second_ignition")
            or bool((row.get("structure_setups") or {}).get("RUNNER_SECOND_IGNITION"))
            or f(f5.get("used_level")) > 0
        )
    )
    runner = {
        "qualified": bool(runner_structure and 0.05 <= r5 <= 2.0 and (v5 >= 1.5 or activity_now) and runner_trigger > 0),
        "trigger": runner_trigger,
        "reason": "SECOND_IGNITION+LOCAL_CONTINUATION_HIGH",
    }

    models = {
        "COMPRESSION_BREAKOUT": compression,
        "TREND_MA_PULLBACK": trend_pullback,
        "MEAN_REVERSION": mean_reversion,
        "LIQUIDITY_SWEEP_REVERSAL": sweep,
        "BREAKOUT_RETEST_CONTINUATION": retest,
        "RUNNER_SECOND_IGNITION": runner,
    }
    for name, model in models.items():
        model["name"] = name
    return models, {
        "sd": sd,
        "ri": ri,
        "pb": pb,
        "frames": frames,
        "mtf_age": mtf_age,
        "phase5": phase5,
        "price": price,
        "vwap": vwap,
        "trend_state": trend_state,
        "sweep_state": sweep_state,
        "activity_now": activity_now,
        "ma_or_mtf": ma_or_mtf,
        "rapid": rapid,
    }


def _choose_setup(models, ctx):
    qualified = [models[name] for name in SETUP_ORDER if models.get(name, {}).get("qualified")]
    if not qualified:
        return None
    price = f(ctx.get("price"))
    def key(model):
        trig = f(model.get("trigger"))
        dist = abs(pct(trig, price)) if trig > 0 and price > 0 else 999.0
        return (dist, SETUP_ORDER.index(model["name"]))
    return min(qualified, key=key)


def _entry_trigger(model, ctx, row):
    if not model:
        return {"trigger": None, "distance_pct": None, "status": "NO_SETUP", "crossed": False, "armed": False, "overshoot_pct": None}
    raw = f(model.get("trigger"))
    price = f(ctx.get("price"))
    spread_bps = max(0.0, f(row.get("spread_bps")))
    name = str(model.get("name") or "")

    if name == "TREND_MA_PULLBACK":
        trigger = raw
    elif name == "LIQUIDITY_SWEEP_REVERSAL":
        trigger = raw
    else:
        buffer_pct = clamp(max(0.03, spread_bps * 1.2 / 100.0), 0.03, 0.18)
        trigger = raw * (1.0 + buffer_pct / 100.0) if raw > 0 else 0.0

    if trigger <= 0 or price <= 0:
        return {"trigger": trigger or None, "distance_pct": None, "status": "NO_LOCAL_TRIGGER", "crossed": False, "armed": False, "overshoot_pct": None}

    distance = pct(trigger, price)
    crossed = price >= trigger
    overshoot = pct(price, trigger) if crossed else 0.0
    if crossed and overshoot <= PINPOINT_MAX_OVERSHOOT_PCT:
        status = "PINPOINT_TRIGGERED"
    elif crossed:
        status = "RETEST_REQUIRED_ALREADY_EXTENDED"
    elif 0.0 <= distance <= PINPOINT_ARM_DISTANCE_PCT:
        status = "PINPOINT_ARMED"
    else:
        status = "WAIT_LOCAL_APPROACH"
    return {
        "trigger": trigger,
        "distance_pct": distance,
        "status": status,
        "crossed": bool(crossed and overshoot <= PINPOINT_MAX_OVERSHOOT_PCT),
        "armed": bool((not crossed) and 0.0 <= distance <= PINPOINT_ARM_DISTANCE_PCT),
        "overshoot_pct": overshoot if crossed else 0.0,
    }


def _risk_plan(sym, model, ctx, entry):
    ri = ctx.get("ri") or {}
    pb = ctx.get("pb") or {}
    frames = ctx.get("frames") or {}
    f5 = frames.get("5m") or {}
    entry = f(entry)
    if entry <= 0:
        return {"ok": False, "stop": None, "risk_pct": None, "source": "NO_ENTRY"}

    if model and model.get("name") == "TREND_MA_PULLBACK":
        stop = f(pb.get("stop"))
        if 0 < stop < entry:
            risk_pct = (entry - stop) / entry * 100.0
            if 0.20 <= risk_pct <= 3.50:
                return {"ok": True, "stop": stop, "risk_pct": risk_pct, "source": "PULLBACK_PLAN"}

    stop = f(ri.get("stop_loss"))
    if 0 < stop < entry:
        risk_pct = (entry - stop) / entry * 100.0
        if 0.20 <= risk_pct <= 3.50:
            return {"ok": True, "stop": stop, "risk_pct": risk_pct, "source": "RISKMAP_PLAN"}

    anchors = []
    for value in (ri.get("support1"), ri.get("stop_cluster"), f5.get("support"), f5.get("used_level")):
        x = f(value)
        if 0 < x < entry:
            anchors.append(x)
    if not anchors:
        return {"ok": False, "stop": None, "risk_pct": None, "source": "NO_SUPPORT_ANCHOR"}

    anchor = max(anchors)
    atr5 = f(ri.get("atr5"))
    stop_buffer = max(atr5 * 0.25 if atr5 > 0 else 0.0, anchor * 0.0015)
    stop = anchor - stop_buffer
    if not (0 < stop < entry):
        return {"ok": False, "stop": stop or None, "risk_pct": None, "source": "INVALID_DERIVED_STOP"}
    risk_pct = (entry - stop) / entry * 100.0
    return {
        "ok": bool(0.20 <= risk_pct <= 3.50),
        "stop": stop,
        "risk_pct": risk_pct,
        "source": "DERIVED_LOCAL_SUPPORT" if 0.20 <= risk_pct <= 3.50 else "RISK_OUT_OF_BOUNDS",
    }


def _contradictions(ctx, tape, entry):
    out = list(tape.get("contradictions") or [])
    phase5 = str(ctx.get("phase5") or "UNKNOWN")
    if phase5 in {"FAILED_BREAKOUT", "REJECT_FALLING", "NO_CHASE"}:
        out.append("5M_LIFECYCLE_REJECTION")
    if str(ctx.get("sweep_state") or "") == "SUPPORT_LOST":
        out.append("STRUCTURAL_SUPPORT_LOST")
    rapid = ctx.get("rapid") or {}
    if f(rapid.get("r5")) > 0.5 and tape.get("cvd_acc", 0.0) < 0 and tape.get("aggressive_delta", 0.0) < 0:
        out.append("PRICE_UP_FLOW_DOWN_DIVERGENCE")
    if entry.get("status") == "RETEST_REQUIRED_ALREADY_EXTENDED":
        out.append("LOCAL_TRIGGER_OVEREXTENDED")
    return list(dict.fromkeys(out))


def _maybe_record_ablation(sym, row, setup, entry, risk, tape, hard_status, contradictions):
    if not setup or not setup.get("qualified") or not entry.get("crossed"):
        return
    now = time.time()
    if now - _ablation_last[sym] < ABLATION_COOLDOWN:
        return
    price = f(row.get("price"))
    if price <= 0:
        return
    _ablation_last[sym] = now
    event = {
        "symbol": sym,
        "ts": now,
        "entry_price": price,
        "setup": setup.get("name"),
        "pinpoint_buy": bool(row.get("pinpoint_buy")),
        "legacy_formal": row.get("pinpoint_legacy_formal"),
        "tape_score": tape.get("score"),
        "tape_pass": bool(tape.get("flow_ok")),
        "persistence_pass": bool(row.get("pinpoint_persistence_ok")),
        "risk_pass": bool(risk.get("ok")),
        "contradiction_free": not contradictions,
        "hard": dict(hard_status),
        "stop": risk.get("stop"),
        "returns": {},
        "max_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
    }
    _ablation_pending.append(event)
    del _ablation_pending[:-600]


def evaluate_pinpoint(sym):
    row = _old_evaluate(sym)
    if not isinstance(row, dict) or not row:
        return row

    legacy_formal = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    row["pinpoint_legacy_formal"] = legacy_formal

    tape = _current_tape(sym, row)
    hard_status, hard_missing = _core_hard(row)
    models, ctx = _setup_models(sym, row, tape)
    ctx["symbol"] = sym
    setup = _choose_setup(models, ctx)
    entry = _entry_trigger(setup, ctx, row)
    risk_basis = max(f(entry.get("trigger")), f(ctx.get("price"))) if entry.get("crossed") else f(entry.get("trigger"))
    risk = _risk_plan(sym, setup, ctx, risk_basis)

    sd = ctx.get("sd") or {}
    runner_exception = bool(
        setup
        and setup.get("name") == "RUNNER_SECOND_IGNITION"
        and passed(hard_status.get("CUMULATIVE_EXTENSION_GUARD"))
    )
    anti_ok = bool(not sd.get("anti_chase") or runner_exception)

    tape_now_ok = bool(tape.get("flow_ok") and not tape.get("contradictions"))
    persistence = _record_tape(sym, tape_now_ok)
    contradictions = _contradictions(ctx, tape, entry)

    structure_ok = bool(setup and setup.get("qualified"))
    data_ok = bool(all(hard_status.values()))
    trigger_ok = bool(entry.get("crossed"))
    persistence_ok = bool(persistence.get("ok"))
    risk_ok = bool(risk.get("ok"))
    veto_clear = not contradictions

    buy = bool(
        data_ok
        and structure_ok
        and anti_ok
        and tape_now_ok
        and persistence_ok
        and trigger_ok
        and risk_ok
        and veto_clear
    )

    blockers = []
    blockers.extend(hard_missing)
    if not structure_ok:
        blockers.append("NO_SETUP_SPECIFIC_STRUCTURE")
    if not anti_ok:
        blockers.append("ANTI_CHASE")
    if not tape_now_ok:
        blockers.append("LIVE_TAPE")
    if not persistence_ok:
        blockers.append(f"PINPOINT_PERSISTENCE_{persistence.get('passes', 0)}/{PINPOINT_REQUIRED_PASSES}")
    if not trigger_ok:
        blockers.append(str(entry.get("status") or "LOCAL_TRIGGER"))
    if not risk_ok:
        blockers.append("VALID_RISK_PLAN")
    blockers.extend(contradictions)
    blockers = list(dict.fromkeys(blockers))

    if buy:
        row["state"] = "BUY NOW"
        row["pre_warmup_state"] = "BUY NOW"
        row["formal_state"] = "BUY NOW"
        row["combined_blockers"] = []
        row["mandatory_all_aligned"] = True
        _stats["buy"] += 1
    else:
        if legacy_formal == "BUY NOW":
            _stats["legacy_buy_demoted"] += 1
        near = bool(entry.get("armed") or entry.get("status") == "PINPOINT_TRIGGERED")
        if structure_ok and data_ok and anti_ok and risk_ok and veto_clear and (near or tape_now_ok):
            row["state"] = "PRE-IGNITION"
            row["pre_warmup_state"] = "PRE-IGNITION"
            row["formal_state"] = "PRE-IGNITION"
        elif legacy_formal == "BUY NOW":
            row["state"] = "WATCH"
            row["pre_warmup_state"] = "WATCH"
            row["formal_state"] = "WATCH"
        row["mandatory_all_aligned"] = False
        row["combined_blockers"] = blockers

    row["pinpoint_version"] = VERSION
    row["pinpoint_buy"] = buy
    row["pinpoint_state"] = "BUY NOW" if buy else ("PINPOINT ARMED" if entry.get("armed") and structure_ok else "SETUP READY" if structure_ok else "WATCH")
    row["pinpoint_setup"] = setup.get("name") if setup else None
    row["pinpoint_setup_reason"] = setup.get("reason") if setup else None
    row["pinpoint_models"] = {name: bool(model.get("qualified")) for name, model in models.items()}
    row["pinpoint_live_tape_score"] = tape.get("score")
    row["pinpoint_live_tape_pass"] = tape_now_ok
    row["pinpoint_aggressive_delta"] = round(f(tape.get("aggressive_delta")), 5)
    row["pinpoint_weighted_obi_l1_l10"] = tape.get("weighted_obi")
    row["pinpoint_persistence_samples"] = persistence.get("samples")
    row["pinpoint_persistence_passes"] = persistence.get("passes")
    row["pinpoint_persistence_ok"] = persistence_ok
    row["pinpoint_trigger"] = entry.get("trigger")
    row["pinpoint_trigger_distance_pct"] = entry.get("distance_pct")
    row["pinpoint_entry_status"] = entry.get("status")
    row["pinpoint_entry_actionable_now"] = trigger_ok
    row["pinpoint_stop"] = risk.get("stop")
    row["pinpoint_risk_pct"] = risk.get("risk_pct")
    row["pinpoint_risk_source"] = risk.get("source")
    row["pinpoint_hard_status"] = hard_status
    row["pinpoint_anti_chase_ok"] = anti_ok
    row["pinpoint_contradictions"] = contradictions
    row["pinpoint_blockers"] = blockers
    row["buy_policy"] = "SETUP_SPECIFIC_STRUCTURE+LIVE_TAPE+LOCAL_TRIGGER+2_OF_3_PERSISTENCE+RISK+NO_CONTRADICTION"
    row["strict_buy_gate_passed"] = buy

    _maybe_record_ablation(sym, row, setup, entry, risk, tape, hard_status, contradictions)
    return row


app.evaluate_symbol = evaluate_pinpoint


def _pinpoint_runner_context(sym):
    row = q.latest.get(sym) or {}
    formal = str(row.get("formal_state") or row.get("state") or "")
    setup_ready = bool(row.get("pinpoint_setup"))
    hard = row.get("pinpoint_hard_status") or {}
    hard_ok = bool(hard) and all(bool(v) for v in hard.values())
    risk_ok = f(row.get("pinpoint_stop")) > 0 and f(row.get("pinpoint_risk_pct")) > 0
    candidate = bool(setup_ready and hard_ok and risk_ok and formal in {"PRE-IGNITION", "BUY NOW"})
    return {
        "formal": formal,
        "all_layers": bool(row.get("pinpoint_live_tape_pass") and setup_ready),
        "all_hard": hard_ok,
        "candidate": candidate,
        "active": bool(candidate and formal == "BUY NOW" and row.get("pinpoint_buy")),
        "layers": sum(bool(v) for v in (row.get("layer_results") or {}).values()),
    }


base._runner_context = _pinpoint_runner_context


def _persist_resolved(event):
    try:
        os.makedirs(os.path.dirname(ABLATION_PATH) or ".", exist_ok=True)
        with open(ABLATION_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, separators=(",", ":"), sort_keys=True) + "\n")
    except Exception:
        _stats["ablation_persist_errors"] += 1


def _update_ablation():
    now = time.time()
    keep = []
    for event in _ablation_pending:
        price = f(app.current_symbol_price(event.get("symbol")))
        entry = f(event.get("entry_price"))
        if price <= 0 or entry <= 0:
            keep.append(event)
            continue
        ret = pct(price, entry)
        event["max_return_pct"] = max(f(event.get("max_return_pct")), ret)
        event["max_drawdown_pct"] = min(f(event.get("max_drawdown_pct")), ret)
        age = now - f(event.get("ts"), now)
        for label, secs in (("5m", 300), ("15m", 900), ("1h", 3600)):
            if age >= secs and label not in event["returns"]:
                event["returns"][label] = round(ret, 5)
        if age >= ABLATION_HORIZON:
            final = dict(event)
            _ablation_resolved.append(final)
            _persist_resolved(final)
            _stats["ablation_resolved"] += 1
        else:
            keep.append(event)
    _ablation_pending[:] = keep[-600:]


async def ablation_loop():
    while True:
        await asyncio.sleep(5.0)
        try:
            _update_ablation()
        except asyncio.CancelledError:
            raise
        except Exception:
            _stats["ablation_errors"] += 1


def _candidate_rows():
    rows = []
    for sym, row in list(q.latest.items()):
        if not isinstance(row, dict) or not row.get("pinpoint_version"):
            continue
        state = str(row.get("formal_state") or row.get("state") or "")
        dist = row.get("pinpoint_trigger_distance_pct")
        dist_rank = abs(f(dist, 999.0)) if dist is not None else 999.0
        rows.append((
            1 if state == "BUY NOW" else 0,
            1 if row.get("pinpoint_entry_status") == "PINPOINT_TRIGGERED" else 0,
            f(row.get("pinpoint_live_tape_score")),
            -dist_rank,
            sym,
            row,
        ))
    rows.sort(reverse=True)
    return rows


async def pinpoint_board_loop():
    while True:
        await asyncio.sleep(PINPOINT_BOARD_SECONDS)
        try:
            rows = _candidate_rows()
            buys = sum(1 for _, _, _, _, _, row in rows if str(row.get("formal_state") or row.get("state")) == "BUY NOW")
            armed = sum(1 for _, _, _, _, _, row in rows if row.get("pinpoint_entry_status") == "PINPOINT_ARMED")
            triggered = sum(1 for _, _, _, _, _, row in rows if row.get("pinpoint_entry_status") == "PINPOINT_TRIGGERED")
            print(
                f"Ψ-PINPOINT BOARD tracked={len(rows)} buy={buys} armed={armed} triggered={triggered} "
                f"legacyDemoted={_stats['legacy_buy_demoted']} ablationPending={len(_ablation_pending)} "
                f"ablationResolved={len(_ablation_resolved)}",
                flush=True,
            )
            for i, (_, _, _, _, sym, row) in enumerate(rows[:12], 1):
                state = str(row.get("formal_state") or row.get("state") or "-")
                trig = row.get("pinpoint_trigger")
                stop = row.get("pinpoint_stop")
                dist = row.get("pinpoint_trigger_distance_pct")
                print(
                    f"PP{i:02d}. {sym:<14} state={state:<12} pstate={str(row.get('pinpoint_state') or '-'):<14} "
                    f"setup={str(row.get('pinpoint_setup') or '-'):<29} tape={f(row.get('pinpoint_live_tape_score')):5.1f} "
                    f"persist={int(row.get('pinpoint_persistence_passes') or 0)}/{PINPOINT_REQUIRED_PASSES} "
                    f"entry={('-' if trig is None else f'{f(trig):.10g}')} status={row.get('pinpoint_entry_status')} "
                    f"dist={('-' if dist is None else f'{f(dist):+.3f}%')} stop={('-' if stop is None else f'{f(stop):.10g}')} "
                    f"risk={f(row.get('pinpoint_risk_pct')):.3f}% blockers={row.get('pinpoint_blockers') or []}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-PINPOINT ERROR {type(exc).__name__}: {exc}", flush=True)


for mod in (base, scanner, scanner.v7):
    try:
        mod.VERSION = VERSION
    except Exception:
        pass
app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.3.5] Ψ PINPOINT EXECUTION active — formal BUY NOW now requires setup-specific structure, "
        "zero-memory current tape confirmation, exact local trigger, 2-of-3 persistence, valid <=3.5% risk plan, "
        "anti-chase and contradiction vetoes. Legacy BUY cannot bypass the model. CVD/buy-dominance double counting "
        "is replaced by one signed aggressive-trade delta; relative negative OFI/OBI cannot become bullish merely by rank. "
        "Gate-ablation outcomes are recorded for later expectancy testing.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        pinpoint_board_loop(),
        ablation_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
