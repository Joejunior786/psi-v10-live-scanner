import asyncio
import math
import statistics
import time

import psi_v11_3_1_entry as base
import ignition119_entry as learn
import ignition1191_entry as riskmap
import ignition1192_entry as pullback

app = base.app
q = base.q
scanner = base.scanner

VERSION = "11.0.3.4-expected-move-engine"
RUNNER_PRINT_SECONDS = 30.0
EXPECTED_PRINT_SECONDS = 30.0

# -----------------------------------------------------------------------------
# Position-management profiles.
# These alter target management only. They never create or relax BUY NOW.
# -----------------------------------------------------------------------------
TARGET_ALLOCATIONS = {
    "NORMAL": {"tp1_pct": 25, "tp2_pct": 30, "tp3_pct": 25, "runner_pct": 20},
    "RUNNER": {"tp1_pct": 20, "tp2_pct": 25, "tp3_pct": 25, "runner_pct": 30},
    "MONSTER": {"tp1_pct": 15, "tp2_pct": 20, "tp3_pct": 25, "runner_pct": 40},
}
RUNNER_ALLOCATION = TARGET_ALLOCATIONS["RUNNER"]
RUNNER_TRAIL = "5M_9EMA_OR_15M_STRUCTURE_LOW"
RUNNER_AFTER_TP1 = "MOVE_STOP_TO_ENTRY_PLUS_FEES"

# Expected-move model configuration. The model is deliberately conservative:
# global empirical rates become priors and similar historical outcomes update
# them. MONSTER mode requires both sufficient local evidence and the existing
# walk-forward learner to be ACTIVE.
MOVE_THRESHOLDS = (5.0, 10.0, 15.0, 20.0)
PRIOR_STRENGTH = 10.0
MAX_HISTORY = 1200
MAX_NEIGHBORS = 160
MIN_SIMILARITY = 0.18
ONE_SIDED_Z = 1.2815515655446004  # ~90% one-sided lower bound
FORECAST_CACHE_SECONDS = 8.0
forecast_cache = {}

_old_risk_trade_plan = riskmap.trade_plan
_old_pullback_eval = pullback.pb_eval


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _gate_passed(value):
    if value is True:
        return True
    return str(value or "").upper() == "PASS"


def _runner_context(sym):
    row = q.latest.get(sym) or {}
    layers = row.get("layer_results") or {}
    layer_status = {
        key: bool(layers.get(key))
        for key in base.MANDATORY_LAYER_KEYS
    }
    all_layers = bool(layer_status) and all(layer_status.values())

    # Prefer the strict guard's own audited booleans when they are available.
    if "strict_buy_all_6_layers" in row:
        all_layers = bool(row.get("strict_buy_all_6_layers"))

    hard = row.get("multi_regime_hard_checks") or row.get("hard_safety_status") or {}
    all_hard = bool(hard) and all(_gate_passed(v) for v in hard.values())
    if "strict_buy_all_hard_gates" in row:
        all_hard = bool(row.get("strict_buy_all_hard_gates"))

    formal = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    candidate = bool(all_layers and all_hard and formal in {"PRE-IGNITION", "BUY NOW"})
    active = bool(candidate and formal == "BUY NOW")

    return {
        "formal": formal,
        "all_layers": all_layers,
        "all_hard": all_hard,
        "candidate": candidate,
        "active": active,
        "layers": sum(layer_status.values()),
    }


# -----------------------------------------------------------------------------
# Bayesian expected-move engine
# -----------------------------------------------------------------------------
def _optional_float(value):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _num_similarity(a, b, scale):
    aa, bb = _optional_float(a), _optional_float(b)
    if aa is None or bb is None:
        return 0.45
    return clamp(1.0 - abs(aa - bb) / max(scale, 1e-9), 0.0, 1.0)


def _cat_similarity(a, b):
    aa, bb = str(a or "UNKNOWN"), str(b or "UNKNOWN")
    if aa == "UNKNOWN" or bb == "UNKNOWN":
        return 0.35
    return 1.0 if aa == bb else 0.0


def _current_signal(sym):
    row = q.latest.get(sym) or {}
    formal = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    if formal == "BUY NOW":
        return "BUY"
    if formal == "PRE-IGNITION":
        return "PRE"
    try:
        ps = learn.pump119(sym)
        pst = str(ps.get("pump_state") or "")
        if pst == "PUMP-ARMED":
            return "PUMP-ARMED"
        if pst == "PUMP-WATCH":
            return "PUMP-WATCH"
    except Exception:
        pass
    return formal or "OTHER"


def _current_features(sym):
    try:
        x = dict(learn.feature_snapshot119(sym))
    except Exception:
        x = {}
    try:
        ps = learn.pump119(sym)
    except Exception:
        ps = {}
    try:
        intel = learn.intelligence(sym)
    except Exception:
        intel = {}
    row = q.latest.get(sym) or {}
    x.update({
        "signal": _current_signal(sym),
        "early_quality119": f(x.get("early_quality119"), f(ps.get("early_quality_119"), 50.0)),
        "pump_score": f(x.get("pump_score"), f(ps.get("pump_signature_score"), 50.0)),
        "flow_score119": f(x.get("flow_score119"), f(intel.get("flow_divergence_score"), 50.0)),
        "vacuum_score119": f(x.get("vacuum_score119"), f(intel.get("liquidity_vacuum_score"), 50.0)),
        "sector_rs119": f(x.get("sector_rs119"), f(intel.get("sector_rs_score"), 50.0)),
        "regime119": x.get("regime119") or intel.get("market_regime_119") or "UNKNOWN",
        "mtf119": x.get("mtf119") or intel.get("mtf_state") or "UNKNOWN",
        "spot_futures119": x.get("spot_futures119") or intel.get("spot_futures_state") or "UNKNOWN",
        "aggressive_buy_ratio": f(row.get("aggressive_buy_ratio"), 0.5),
    })
    return x


def _history_rows():
    rows = [x for x in list(getattr(learn, "shadow_resolved", []))[-MAX_HISTORY:] if isinstance(x, dict)]
    return [x for x in rows if f(x.get("entry")) > 0 and x.get("max_return_pct") is not None]


def _similarity(current, trade):
    hist = trade.get("features") or {}
    signal_match = _cat_similarity(current.get("signal"), trade.get("signal"))
    score = (
        0.18 * signal_match
        + 0.12 * _cat_similarity(current.get("regime119"), hist.get("regime119"))
        + 0.10 * _cat_similarity(current.get("mtf119"), hist.get("mtf119"))
        + 0.07 * _cat_similarity(current.get("spot_futures119"), hist.get("spot_futures119"))
        + 0.18 * _num_similarity(current.get("early_quality119"), hist.get("early_quality119"), 25.0)
        + 0.13 * _num_similarity(current.get("flow_score119"), hist.get("flow_score119"), 30.0)
        + 0.10 * _num_similarity(current.get("vacuum_score119"), hist.get("vacuum_score119"), 30.0)
        + 0.07 * _num_similarity(current.get("sector_rs119"), hist.get("sector_rs119"), 30.0)
        + 0.05 * _num_similarity(current.get("pump_score"), hist.get("pump_score"), 30.0)
    )
    return clamp(score, 0.0, 1.0)


def _neighbors(current, rows):
    scored = []
    for trade in rows:
        sim = _similarity(current, trade)
        if sim >= MIN_SIMILARITY:
            # Squaring emphasizes genuinely similar regimes without throwing
            # away broader history completely.
            scored.append((sim, sim * sim, trade))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:MAX_NEIGHBORS]


def _effective_n(weights):
    sw = sum(weights)
    sw2 = sum(w * w for w in weights)
    return (sw * sw / sw2) if sw2 > 0 else 0.0


def _beta_posterior(rows, neighbors, predicate):
    n = len(rows)
    if n <= 0:
        return {"mean": 0.0, "lower": 0.0, "global": 0.0, "weighted_n": 0.0}

    # Laplace-smoothed global rate becomes the empirical prior center.
    global_hits = sum(1 for t in rows if predicate(t))
    global_rate = (global_hits + 1.0) / (n + 2.0)
    alpha = 1.0 + PRIOR_STRENGTH * global_rate
    beta = 1.0 + PRIOR_STRENGTH * (1.0 - global_rate)

    for _, weight, trade in neighbors:
        if predicate(trade):
            alpha += weight
        else:
            beta += weight

    total = alpha + beta
    mean = alpha / total if total > 0 else 0.0
    variance = (alpha * beta) / ((total * total) * (total + 1.0)) if total > 1 else 0.0
    sd = math.sqrt(max(0.0, variance))
    lower = clamp(mean - ONE_SIDED_Z * sd, 0.0, 1.0)
    return {
        "mean": mean,
        "lower": lower,
        "global": global_rate,
        "weighted_n": sum(w for _, w, _ in neighbors),
    }


def _hist_r_multiple(trade):
    entry = f(trade.get("entry"))
    stop = f(trade.get("stop_initial"), f(trade.get("stop")))
    realized = _optional_float(trade.get("realized_return_pct"))
    if entry <= 0 or stop <= 0 or stop >= entry or realized is None:
        return None
    risk_pct = (entry - stop) / entry * 100.0
    if risk_pct <= 0.05:
        return None
    return clamp(realized / risk_pct, -2.5, 20.0)


def _empirical_r(rows, neighbors):
    global_rs = [r for r in (_hist_r_multiple(t) for t in rows) if r is not None]
    global_mean = statistics.mean(global_rs) if global_rs else 0.0

    vals = []
    for _, weight, trade in neighbors:
        r = _hist_r_multiple(trade)
        if r is not None:
            vals.append((weight, r))
    sw = sum(w for w, _ in vals)
    if sw <= 0:
        return {"mean": global_mean, "lower": min(0.0, global_mean), "n": 0.0}

    mean = (PRIOR_STRENGTH * global_mean + sum(w * r for w, r in vals)) / (PRIOR_STRENGTH + sw)
    weights = [w for w, _ in vals]
    neff = _effective_n(weights)
    local_mean = sum(w * r for w, r in vals) / sw
    variance = sum(w * (r - local_mean) ** 2 for w, r in vals) / sw if sw > 0 else 0.0
    se = math.sqrt(max(variance, 0.0) / max(neff, 1.0))
    lower = mean - ONE_SIDED_Z * se
    return {"mean": mean, "lower": lower, "n": neff}


def _live_continuation_score(sym, features):
    row = q.latest.get(sym) or {}
    mtf = str(features.get("mtf119") or "UNKNOWN")
    mtf_score = {
        "MTF-ALIGNED": 100.0,
        "MTF-MIXED-BULL": 72.0,
        "MTF-WAIT": 45.0,
        "WAIT": 45.0,
        "MTF-REJECT": 10.0,
    }.get(mtf, 45.0)

    buy_ratio = f(features.get("aggressive_buy_ratio"), 0.5)
    buy_score = clamp(buy_ratio * 100.0 if buy_ratio <= 1.5 else buy_ratio, 0.0, 100.0)

    score = (
        0.25 * f(features.get("early_quality119"), 50.0)
        + 0.22 * f(features.get("flow_score119"), 50.0)
        + 0.18 * f(features.get("vacuum_score119"), 50.0)
        + 0.15 * f(features.get("pump_score"), 50.0)
        + 0.12 * mtf_score
        + 0.08 * buy_score
    )

    # A current lifecycle rejection is a direct continuation penalty.
    lifecycle = str(row.get("breakout_lifecycle") or row.get("lifecycle_state") or "")
    if lifecycle in {"FAILED_BREAKOUT", "REJECT_FALLING", "NO_CHASE"}:
        score -= 20.0
    return round(clamp(score, 0.0, 100.0), 1)


def expected_move_forecast(sym, entry, stop):
    entry, stop = f(entry), f(stop)
    now = time.time()
    cache = forecast_cache.get(sym)
    if cache and now - f(cache.get("updated")) <= FORECAST_CACHE_SECONDS:
        if abs(f(cache.get("entry")) - entry) <= max(entry * 0.0005, 1e-12) and abs(f(cache.get("stop")) - stop) <= max(stop * 0.0005, 1e-12):
            return dict(cache)

    rows = _history_rows()
    current = _current_features(sym)
    neighbors = _neighbors(current, rows)
    weights = [w for _, w, _ in neighbors]
    neff = _effective_n(weights)
    live_score = _live_continuation_score(sym, current)
    ctx = _runner_context(sym)

    out = {
        "updated": now,
        "entry": entry,
        "stop": stop,
        "history_n": len(rows),
        "neighbor_n": len(neighbors),
        "effective_n": round(neff, 2),
        "live_continuation_score": live_score,
        "probability_method": "BAYESIAN_EMPIRICAL_SHADOW",
        "target_mode": "NORMAL",
        "mode_reason": "DEFAULT_CONSERVATIVE",
    }

    if not rows:
        out.update({
            "data_status": "NO_DATA",
            "p5": 0.0, "p10": 0.0, "p15": 0.0, "p20": 0.0,
            "p5_lower": 0.0, "p10_lower": 0.0, "p15_lower": 0.0, "p20_lower": 0.0,
            "p_stop": 0.0, "expected_excursion_pct": 0.0,
            "expected_r": 0.0, "expected_r_lower": -1.0,
        })
        forecast_cache[sym] = dict(out)
        return out

    posts = {}
    for threshold in MOVE_THRESHOLDS:
        posts[threshold] = _beta_posterior(
            rows, neighbors,
            lambda t, th=threshold: f(t.get("max_return_pct")) >= th,
        )

    stop_post = _beta_posterior(rows, neighbors, lambda t: str(t.get("status") or "") == "STOP")
    empirical_r = _empirical_r(rows, neighbors)

    # Enforce the mathematical nesting P(+5)>=P(+10)>=P(+15)>=P(+20).
    means = [posts[t]["mean"] for t in MOVE_THRESHOLDS]
    lowers = [posts[t]["lower"] for t in MOVE_THRESHOLDS]
    for i in range(1, len(means)):
        means[i] = min(means[i], means[i - 1])
        lowers[i] = min(lowers[i], lowers[i - 1])

    risk_pct = ((entry - stop) / entry * 100.0) if entry > 0 and 0 < stop < entry else 2.0
    risk_pct = clamp(risk_pct, 0.20, 6.0)

    # Integral of threshold-survival probabilities approximates expected upside
    # excursion capped at +20%. It is NOT presented as guaranteed return.
    expected_excursion = 5.0 * sum(means)
    conservative_excursion = 5.0 * sum(lowers)
    move_ev_pct = expected_excursion - stop_post["mean"] * risk_pct
    move_ev_low_pct = conservative_excursion - min(1.0, stop_post["mean"] + 0.10) * risk_pct
    move_r = move_ev_pct / risk_pct
    move_r_low = move_ev_low_pct / risk_pct

    expected_r = 0.55 * empirical_r["mean"] + 0.45 * move_r
    expected_r_lower = 0.60 * empirical_r["lower"] + 0.40 * move_r_low

    calibration_status = str((getattr(learn, "calibration", {}) or {}).get("status") or "WARMING")
    if len(rows) < 20 or neff < 5:
        data_status = "WARMING"
    elif neff < 12:
        data_status = "EMPIRICAL_LOW"
    elif calibration_status == "ACTIVE" and neff >= 25:
        data_status = "EMPIRICAL_WALKFORWARD_ACTIVE"
    else:
        data_status = "EMPIRICAL_MEDIUM"

    out.update({
        "data_status": data_status,
        "calibration_status": calibration_status,
        "p5": means[0], "p10": means[1], "p15": means[2], "p20": means[3],
        "p5_lower": lowers[0], "p10_lower": lowers[1], "p15_lower": lowers[2], "p20_lower": lowers[3],
        "p_stop": stop_post["mean"],
        "expected_excursion_pct": expected_excursion,
        "expected_excursion_lower_pct": conservative_excursion,
        "expected_r_move": move_r,
        "expected_r_empirical": empirical_r["mean"],
        "expected_r": expected_r,
        "expected_r_lower": expected_r_lower,
        "risk_pct": risk_pct,
    })

    # Mode selection is fail-closed. It affects only TP management after the
    # ordinary strict V10 candidate gate has passed.
    if not ctx["candidate"]:
        mode, reason = "NORMAL", "NOT_STRICT_6_OF_6_CANDIDATE"
    else:
        mode, reason = "NORMAL", "INSUFFICIENT_RIGHT_TAIL_EVIDENCE"
        runner_ok = (
            neff >= 8
            and lowers[1] >= 0.16
            and lowers[2] >= 0.07
            and expected_r_lower >= 1.35
            and live_score >= 62.0
        )
        monster_ok = (
            calibration_status == "ACTIVE"
            and len(rows) >= 60
            and neff >= 20
            and lowers[1] >= 0.28
            and lowers[2] >= 0.16
            and lowers[3] >= 0.07
            and expected_r_lower >= 2.50
            and live_score >= 77.0
        )
        if runner_ok:
            mode, reason = "RUNNER", "RIGHT_TAIL_AND_LIVE_CONTINUATION"
        if monster_ok:
            mode, reason = "MONSTER", "VALIDATED_FAT_RIGHT_TAIL"

    out["target_mode"] = mode
    out["mode_reason"] = reason
    forecast_cache[sym] = dict(out)
    return out


# -----------------------------------------------------------------------------
# Dynamic target construction driven by Expected-Move mode
# -----------------------------------------------------------------------------
def _structural_levels(values, entry):
    out = []
    for value in values or []:
        x = f(value)
        if x > entry:
            out.append(x)
    return sorted(set(out))


def _snap_up(entry, floor_pct, cap_pct, structural):
    floor_price = entry * (1.0 + floor_pct / 100.0)
    cap_price = entry * (1.0 + cap_pct / 100.0)
    candidates = [x * 0.999 for x in structural if floor_price <= x <= cap_price]
    return min(candidates) if candidates else floor_price


def _runner_targets(sym, entry, stop, structural=None):
    entry = f(entry)
    stop = f(stop)
    if entry <= 0 or stop <= 0 or stop >= entry:
        return None

    risk_pct = (entry - stop) / entry * 100.0
    forecast = expected_move_forecast(sym, entry, stop)
    mode = str(forecast.get("target_mode") or "NORMAL")

    if mode == "MONSTER":
        tp1_pct = max(5.0, min(7.0, risk_pct * 1.8))
        tp2_pct = max(12.0, min(16.0, risk_pct * 4.0))
        tp3_pct = max(20.0, min(28.0, risk_pct * 7.0))
        runner_pct = max(35.0, min(50.0, risk_pct * 12.0))
        caps = (9.0, 19.0, 32.0, 60.0)
    elif mode == "RUNNER":
        tp1_pct = max(4.0, min(6.0, risk_pct * 1.6))
        tp2_pct = max(9.0, min(12.0, risk_pct * 3.4))
        tp3_pct = max(15.0, min(21.0, risk_pct * 5.8))
        runner_pct = max(25.0, min(38.0, risk_pct * 9.5))
        caps = (7.0, 14.0, 24.0, 45.0)
    else:
        tp1_pct = max(3.0, min(5.0, risk_pct * 1.5))
        tp2_pct = max(7.0, min(10.0, risk_pct * 3.0))
        tp3_pct = max(12.0, min(18.0, risk_pct * 5.0))
        runner_pct = max(20.0, min(30.0, risk_pct * 8.0))
        caps = (6.0, 12.0, 20.0, 35.0)

    levels = _structural_levels(structural, entry)
    tp1 = _snap_up(entry, tp1_pct, caps[0], levels)
    tp2 = _snap_up(entry, tp2_pct, caps[1], levels)
    tp3 = _snap_up(entry, tp3_pct, caps[2], levels)
    runner_ref = _snap_up(entry, runner_pct, caps[3], levels)

    tp2 = max(tp2, tp1 * 1.0025)
    tp3 = max(tp3, tp2 * 1.0025)
    runner_ref = max(runner_ref, tp3 * 1.0025)

    return {
        "runner_tp1": tp1,
        "runner_tp2": tp2,
        "runner_tp3": tp3,
        "runner_reference": runner_ref,
        "runner_tp1_pct": (tp1 / entry - 1.0) * 100.0,
        "runner_tp2_pct": (tp2 / entry - 1.0) * 100.0,
        "runner_tp3_pct": (tp3 / entry - 1.0) * 100.0,
        "runner_reference_pct": (runner_ref / entry - 1.0) * 100.0,
        "runner_risk_pct": risk_pct,
        "expected_target_mode": mode,
        "expected_mode_reason": forecast.get("mode_reason"),
        "expected_data_status": forecast.get("data_status"),
        "expected_history_n": forecast.get("history_n"),
        "expected_effective_n": forecast.get("effective_n"),
        "expected_live_score": forecast.get("live_continuation_score"),
        "expected_p5": forecast.get("p5"),
        "expected_p10": forecast.get("p10"),
        "expected_p15": forecast.get("p15"),
        "expected_p20": forecast.get("p20"),
        "expected_p5_lower": forecast.get("p5_lower"),
        "expected_p10_lower": forecast.get("p10_lower"),
        "expected_p15_lower": forecast.get("p15_lower"),
        "expected_p20_lower": forecast.get("p20_lower"),
        "expected_r": forecast.get("expected_r"),
        "expected_r_lower": forecast.get("expected_r_lower"),
        "expected_excursion_pct": forecast.get("expected_excursion_pct"),
        "probability_method": forecast.get("probability_method"),
    }


def _apply_runner(sym, plan, structural=None, source="RISKMAP"):
    if not isinstance(plan, dict):
        return plan

    ctx = _runner_context(sym)
    plan["runner_candidate"] = ctx["candidate"]
    plan["runner_active"] = ctx["active"]
    plan["runner_mode"] = "ACTIVE" if ctx["active"] else ("CANDIDATE" if ctx["candidate"] else "OFF")
    plan["runner_source"] = source

    if not ctx["candidate"]:
        return plan

    entry = f(plan.get("entry_trigger"), f(plan.get("entry")))
    stop = f(plan.get("stop_loss"), f(plan.get("stop")))
    targets = _runner_targets(sym, entry, stop, structural)
    if not targets:
        return plan

    plan["base_tp1"] = plan.get("tp1")
    plan["base_tp2"] = plan.get("tp2")
    plan["base_tp3"] = plan.get("tp3")
    plan.update(targets)
    plan["tp1"] = targets["runner_tp1"]
    plan["tp2"] = targets["runner_tp2"]
    plan["tp3"] = targets["runner_tp3"]

    mode = str(targets.get("expected_target_mode") or "NORMAL")
    plan["runner_allocation"] = dict(TARGET_ALLOCATIONS.get(mode, TARGET_ALLOCATIONS["NORMAL"]))
    plan["runner_after_tp1"] = RUNNER_AFTER_TP1
    plan["runner_trail"] = RUNNER_TRAIL
    plan["target_basis"] = f"EXPECTED_MOVE_{mode}+STRUCTURE+RISK"
    return plan


def trade_plan_runner(sym, price, atr, supports, resistances, sweep):
    plan = _old_risk_trade_plan(sym, price, atr, supports, resistances, sweep)
    structural = [f(x.get("level")) for x in resistances or []]
    return _apply_runner(sym, plan, structural=structural, source="RISKMAP")


def pullback_eval_runner(sym, tm, c5, c15):
    out = _old_pullback_eval(sym, tm, c5, c15)
    if not isinstance(out, dict):
        return out

    ri = riskmap.risk_intel(sym)
    structural = [ri.get("resistance1"), ri.get("resistance2"), ri.get("resistance3"), tm.get("recent_high")]
    temp = {
        "entry": out.get("entry"),
        "stop": out.get("stop"),
        "tp1": out.get("tp1"),
        "tp2": out.get("tp2"),
        "tp3": out.get("tp3"),
    }
    temp = _apply_runner(sym, temp, structural=structural, source="PULLBACK")

    for key in (
        "tp1", "tp2", "tp3", "base_tp1", "base_tp2", "base_tp3",
        "runner_candidate", "runner_active", "runner_mode", "runner_source",
        "runner_tp1", "runner_tp2", "runner_tp3", "runner_reference",
        "runner_tp1_pct", "runner_tp2_pct", "runner_tp3_pct",
        "runner_reference_pct", "runner_risk_pct", "runner_allocation",
        "runner_after_tp1", "runner_trail", "target_basis",
        "expected_target_mode", "expected_mode_reason", "expected_data_status",
        "expected_history_n", "expected_effective_n", "expected_live_score",
        "expected_p5", "expected_p10", "expected_p15", "expected_p20",
        "expected_p5_lower", "expected_p10_lower", "expected_p15_lower", "expected_p20_lower",
        "expected_r", "expected_r_lower", "expected_excursion_pct", "probability_method",
    ):
        if key in temp:
            out[key] = temp[key]
    return out


riskmap.trade_plan = trade_plan_runner
pullback.pb_eval = pullback_eval_runner


def _best_runner_plan(sym):
    plans = []
    pb = pullback.pb_intel(sym)
    if isinstance(pb, dict) and pb.get("runner_candidate") and f(pb.get("entry")) > 0:
        plans.append(("PULLBACK", pb, f(pb.get("score"))))
    ri = riskmap.risk_intel(sym)
    if isinstance(ri, dict) and ri.get("runner_candidate") and f(ri.get("entry_trigger")) > 0:
        plans.append(("RISKMAP", ri, f(ri.get("sweep_score"))))
    return max(plans, key=lambda x: x[2]) if plans else None


def _fmt(v):
    x = f(v)
    if x <= 0:
        return "-"
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


async def runner_board_loop():
    while True:
        await asyncio.sleep(RUNNER_PRINT_SECONDS)
        try:
            symbols = set(riskmap.risk_cache) | set(pullback.pb_cache)
            rows = []
            for sym in symbols:
                best = _best_runner_plan(sym)
                if not best:
                    continue
                source, plan, score = best
                ctx = _runner_context(sym)
                rows.append((1 if ctx["active"] else 0, score, sym, source, plan, ctx))

            rows.sort(reverse=True)
            active = sum(x[0] for x in rows)
            mode_counts = {m: sum(str(x[4].get("expected_target_mode") or "NORMAL") == m for x in rows) for m in ("NORMAL", "RUNNER", "MONSTER")}
            print(
                f"Ψ-V10 RUNNER BOARD candidates={len(rows)} active={active} "
                f"normal={mode_counts['NORMAL']} runner={mode_counts['RUNNER']} monster={mode_counts['MONSTER']} "
                f"trail={RUNNER_TRAIL}",
                flush=True,
            )
            for i, (_, score, sym, source, plan, ctx) in enumerate(rows[:10], 1):
                entry = f(plan.get("entry_trigger"), f(plan.get("entry")))
                stop = f(plan.get("stop_loss"), f(plan.get("stop")))
                alloc = plan.get("runner_allocation") or TARGET_ALLOCATIONS["NORMAL"]
                print(
                    f"RN{i:02d}. {sym:<14} mode={plan.get('runner_mode','OFF'):<9} "
                    f"target={str(plan.get('expected_target_mode') or 'NORMAL'):<7} formal={ctx['formal']:<16} "
                    f"src={source:<8} score={score:5.1f} entry={_fmt(entry)} stop={_fmt(stop)} "
                    f"tp1={_fmt(plan.get('tp1'))}({f(plan.get('runner_tp1_pct')):+.1f}%) "
                    f"tp2={_fmt(plan.get('tp2'))}({f(plan.get('runner_tp2_pct')):+.1f}%) "
                    f"tp3={_fmt(plan.get('tp3'))}({f(plan.get('runner_tp3_pct')):+.1f}%) "
                    f"runner={_fmt(plan.get('runner_reference'))}({f(plan.get('runner_reference_pct')):+.1f}%) "
                    f"alloc={alloc.get('tp1_pct')}/{alloc.get('tp2_pct')}/{alloc.get('tp3_pct')}/{alloc.get('runner_pct')}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-V10 RUNNER_ERROR {type(exc).__name__}: {exc}", flush=True)


async def expected_move_board_loop():
    while True:
        await asyncio.sleep(EXPECTED_PRINT_SECONDS)
        try:
            symbols = set(riskmap.risk_cache) | set(pullback.pb_cache)
            rows = []
            for sym in symbols:
                best = _best_runner_plan(sym)
                if not best:
                    continue
                source, plan, score = best
                entry = f(plan.get("entry_trigger"), f(plan.get("entry")))
                stop = f(plan.get("stop_loss"), f(plan.get("stop")))
                if entry <= 0 or stop <= 0 or stop >= entry:
                    continue
                fc = expected_move_forecast(sym, entry, stop)
                rank = {"MONSTER": 3, "RUNNER": 2, "NORMAL": 1}.get(str(fc.get("target_mode")), 0)
                rows.append((rank, f(fc.get("expected_r_lower")), f(fc.get("live_continuation_score")), score, sym, source, fc))

            rows.sort(reverse=True)
            history_n = len(_history_rows())
            cal = str((getattr(learn, "calibration", {}) or {}).get("status") or "WARMING")
            print(
                f"Ψ-V10 EXPECTED_MOVE BOARD candidates={len(rows)} history={history_n} calibration={cal} "
                f"method=BAYESIAN_EMPIRICAL_SHADOW thresholds=5/10/15/20",
                flush=True,
            )
            for i, (_, _, _, _, sym, source, fc) in enumerate(rows[:10], 1):
                print(
                    f"EM{i:02d}. {sym:<14} target={str(fc.get('target_mode') or 'NORMAL'):<7} "
                    f"data={str(fc.get('data_status') or 'NO_DATA'):<28} src={source:<8} "
                    f"nEff={f(fc.get('effective_n')):5.1f} live={f(fc.get('live_continuation_score')):5.1f} "
                    f"P5={100*f(fc.get('p5')):5.1f}%[{100*f(fc.get('p5_lower')):4.1f}] "
                    f"P10={100*f(fc.get('p10')):5.1f}%[{100*f(fc.get('p10_lower')):4.1f}] "
                    f"P15={100*f(fc.get('p15')):5.1f}%[{100*f(fc.get('p15_lower')):4.1f}] "
                    f"P20={100*f(fc.get('p20')):5.1f}%[{100*f(fc.get('p20_lower')):4.1f}] "
                    f"expExc={f(fc.get('expected_excursion_pct')):5.2f}% "
                    f"expR={f(fc.get('expected_r')):+5.2f} lowerR={f(fc.get('expected_r_lower')):+5.2f} "
                    f"reason={fc.get('mode_reason')}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-V10 EXPECTED_MOVE_ERROR {type(exc).__name__}: {exc}", flush=True)


for mod in (base, scanner, scanner.v7):
    try:
        mod.VERSION = VERSION
    except Exception:
        pass
app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.3.4] Ψ EXPECTED-MOVE ENGINE active — strict BUY invariant unchanged; "
        "Bayesian similarity-weighted shadow outcomes estimate +5/+10/+15/+20 forward excursion, "
        "expected R and NORMAL/RUNNER/MONSTER target mode. MONSTER requires walk-forward ACTIVE "
        "plus sufficient effective sample size and strong live continuation. Probabilities are empirical "
        "model estimates, not guarantees.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        runner_board_loop(),
        expected_move_board_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
