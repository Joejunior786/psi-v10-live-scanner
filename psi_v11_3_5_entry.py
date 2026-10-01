import asyncio
import math
import statistics
import time

import psi_v11_3_4_entry as base

app = base.app
q = base.q
scanner = base.scanner

VERSION = "11.0.3.7-tail-hunter-scientist"
PRIOR_STRENGTH = 12.0
MIN_SIMILARITY_V2 = 0.35
MAX_NEIGHBORS_V2 = 180
MIN_ACTIVE_NEFF = 12.0
MIN_ACTIVE_EVENTS = 60
RISK_MIN_PCT = 0.20
RISK_MAX_PCT = 3.50
LATE_TRIGGER_PCT = 0.35
BOARD_SECONDS = 30.0

# Preserve the proven recorder and persistence state from v11.0.3.6. This layer
# replaces forecasting/ranking only; Pinpoint remains the sole BUY authority.
_old_current_features = base._current_features


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def optional_float(value):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _cohort_from_event(event):
    explicit = str(event.get("cohort") or "").upper()
    if explicit in {"BUY", "ARMED", "TRIGGER", "LATE_TRIGGER"}:
        return explicit
    if bool(event.get("pinpoint_buy_at_open")):
        return "BUY"
    features = event.get("features") or {}
    state = str(features.get("pinpoint_state") or "").upper()
    overshoot = abs(f(features.get("trigger_overshoot_pct"), 0.0))
    if overshoot > LATE_TRIGGER_PCT:
        return "LATE_TRIGGER"
    if state in {"PINPOINT ARMED", "BUY NOW"}:
        return "ARMED"
    return "TRIGGER"


def _cohort_from_row(row):
    if bool(row.get("pinpoint_buy")):
        return "BUY"
    state = str(row.get("pinpoint_state") or "").upper()
    if state == "PINPOINT ARMED":
        return "ARMED"
    overshoot = max(0.0, -f(row.get("pinpoint_trigger_distance_pct"), 0.0))
    if str(row.get("pinpoint_entry_status") or "") == "PINPOINT_TRIGGERED" and overshoot > LATE_TRIGGER_PCT:
        return "LATE_TRIGGER"
    return "TRIGGER"


def _cohort_weight(current, historical):
    if current == historical:
        return 1.0
    table = {
        "BUY": {"ARMED": 0.65, "TRIGGER": 0.30, "LATE_TRIGGER": 0.12},
        "ARMED": {"BUY": 0.85, "TRIGGER": 0.55, "LATE_TRIGGER": 0.20},
        "TRIGGER": {"BUY": 0.65, "ARMED": 0.80, "LATE_TRIGGER": 0.35},
        "LATE_TRIGGER": {"BUY": 0.30, "ARMED": 0.45, "TRIGGER": 0.70},
    }
    return table.get(current, {}).get(historical, 0.25)


def _current_features_v2(sym, row):
    try:
        feat = dict(_old_current_features(sym, row))
    except Exception:
        feat = {}
    buy_ratio = clamp(f(row.get("aggressive_buy_ratio"), 0.5), 0.0, 1.0)
    price = f(row.get("price"))
    vwap = f(row.get("vwap_60s"))
    feat.update({
        "cohort": _cohort_from_row(row),
        "pinpoint_setup": row.get("pinpoint_setup") or "UNKNOWN",
        "pinpoint_tape": f(row.get("pinpoint_live_tape_score"), 50.0),
        "pinpoint_risk_pct": f(row.get("pinpoint_risk_pct"), 0.0),
        "pinpoint_state": row.get("pinpoint_state") or "UNKNOWN",
        "pinpoint_entry_status": row.get("pinpoint_entry_status") or "UNKNOWN",
        "trigger_distance_pct": f(row.get("pinpoint_trigger_distance_pct"), 0.0),
        "trigger_overshoot_pct": max(0.0, -f(row.get("pinpoint_trigger_distance_pct"), 0.0)),
        "persistence_passes": f(row.get("pinpoint_persistence_passes"), 0.0),
        "persistence_samples": f(row.get("pinpoint_persistence_samples"), 0.0),
        "aggressive_delta": f(row.get("pinpoint_aggressive_delta"), 2.0 * buy_ratio - 1.0),
        "ofi": f(row.get("ofi")),
        "ofi_acceleration": f(row.get("ofi_acceleration")),
        "cvd_acceleration": f(row.get("cvd_acceleration")),
        "weighted_obi": f(row.get("pinpoint_weighted_obi_l1_l10"), f(row.get("obi"))),
        "ask_depletion": f(row.get("ask_depletion")),
        "relative_volume_30s": f(row.get("relative_volume_30s")),
        "trade_acceleration": f(row.get("trade_acceleration")),
        "spread_bps": f(row.get("spread_bps")),
        "vwap_distance_pct": ((price / vwap) - 1.0) * 100.0 if price > 0 and vwap > 0 else None,
        "extension_ok": bool(row.get("pinpoint_anti_chase_ok")),
        "contradiction_count": len(row.get("pinpoint_contradictions") or []),
    })
    return feat


base._current_features = _current_features_v2


SIM_SPECS = (
    ("pinpoint_setup", 0.22, "cat", None),
    ("cohort", 0.13, "cat", None),
    ("regime119", 0.08, "cat", None),
    ("mtf119", 0.06, "cat", None),
    ("spot_futures119", 0.03, "cat", None),
    ("pinpoint_tape", 0.10, "num", 22.0),
    ("pinpoint_risk_pct", 0.05, "num", 1.8),
    ("aggressive_delta", 0.07, "num", 0.35),
    ("ofi", 0.055, "num", 0.25),
    ("ofi_acceleration", 0.035, "num", 0.25),
    ("cvd_acceleration", 0.035, "num", 0.25),
    ("weighted_obi", 0.055, "num", 0.35),
    ("relative_volume_30s", 0.035, "num", 1.5),
    ("trade_acceleration", 0.025, "num", 1.5),
    ("trigger_overshoot_pct", 0.025, "num", 0.60),
    ("flow_score119", 0.03, "num", 30.0),
    ("pump_score", 0.02, "num", 30.0),
)


def _feature_similarity(current, event):
    hist = event.get("features") or {}
    weighted = 0.0
    denom = 0.0
    for key, weight, kind, scale in SIM_SPECS:
        a = current.get(key)
        b = hist.get(key)
        if kind == "cat":
            aa = str(a or "UNKNOWN")
            bb = str(b or "UNKNOWN")
            if aa == "UNKNOWN" or bb == "UNKNOWN":
                continue
            sim = 1.0 if aa == bb else 0.0
        else:
            aa = optional_float(a)
            bb = optional_float(b)
            if aa is None or bb is None:
                continue
            sim = clamp(1.0 - abs(aa - bb) / max(float(scale), 1e-9), 0.0, 1.0)
        weighted += weight * sim
        denom += weight
    if denom <= 0.25:
        return 0.0
    return clamp(weighted / denom, 0.0, 1.0)


def _event_weight(current, event):
    sim = _feature_similarity(current, event)
    if sim < MIN_SIMILARITY_V2:
        return 0.0, sim
    cw = _cohort_weight(str(current.get("cohort") or "TRIGGER"), _cohort_from_event(event))
    return (sim ** 3) * cw, sim


def _effective_n(weights):
    sw = sum(weights)
    sw2 = sum(w * w for w in weights)
    return (sw * sw / sw2) if sw2 > 0 else 0.0


def _all_events():
    return [x for x in list(base.tail_resolved) + list(base.tail_pending) if isinstance(x, dict)]


def _event_record(event, horizon_seconds, target):
    opened = f(event.get("opened"))
    if opened <= 0:
        return None
    now = time.time()
    horizon_end = opened + horizon_seconds
    hit_ts = f((event.get("hit_ts") or {}).get(str(int(target))), 0.0)
    stop_ts = f(event.get("stop_ts"), 0.0)
    closed = f(event.get("closed"), 0.0)
    observed_end = min(horizon_end, closed if closed > 0 else now)
    if observed_end < opened:
        return None

    kind = "CENSOR"
    event_time = observed_end
    if hit_ts > 0 and hit_ts <= horizon_end and (stop_ts <= 0 or hit_ts <= stop_ts):
        kind = "TARGET"
        event_time = hit_ts
    elif stop_ts > 0 and stop_ts <= horizon_end and (hit_ts <= 0 or stop_ts < hit_ts):
        kind = "STOP"
        event_time = stop_ts
    elif observed_end >= horizon_end:
        kind = "HORIZON"
        event_time = horizon_end
    return {
        "event": event,
        "opened": opened,
        "time": max(0.0, event_time - opened),
        "observed": max(0.0, observed_end - opened),
        "kind": kind,
    }


def _aj_cif(current, horizon, target, local=True):
    hsecs = float(base.TAIL_HORIZONS[horizon])
    records = []
    weights = []
    sims = []
    for event in _all_events():
        rec = _event_record(event, hsecs, target)
        if rec is None:
            continue
        if local:
            weight, sim = _event_weight(current, event)
            if weight <= 0:
                continue
        else:
            weight, sim = 1.0, 1.0
        rec["weight"] = weight
        records.append(rec)
        weights.append(weight)
        sims.append(sim)

    if not records:
        return {"target": 0.0, "stop": 0.0, "survival": 1.0, "effective_n": 0.0, "n": 0, "censored": 0}

    event_times = sorted({r["time"] for r in records if r["kind"] in {"TARGET", "STOP"}})
    surv = 1.0
    cif_target = 0.0
    cif_stop = 0.0
    for t in event_times:
        at_risk = sum(r["weight"] for r in records if r["observed"] + 1e-9 >= t)
        if at_risk <= 1e-12:
            continue
        d_target = sum(r["weight"] for r in records if r["kind"] == "TARGET" and abs(r["time"] - t) <= 1e-9)
        d_stop = sum(r["weight"] for r in records if r["kind"] == "STOP" and abs(r["time"] - t) <= 1e-9)
        prev_surv = surv
        cif_target += prev_surv * (d_target / at_risk)
        cif_stop += prev_surv * (d_stop / at_risk)
        surv *= max(0.0, 1.0 - (d_target + d_stop) / at_risk)

    return {
        "target": clamp(cif_target, 0.0, 1.0),
        "stop": clamp(cif_stop, 0.0, 1.0),
        "survival": clamp(surv, 0.0, 1.0),
        "effective_n": _effective_n(weights),
        "n": len(records),
        "censored": sum(1 for r in records if r["kind"] == "CENSOR"),
        "mean_similarity": (sum(sims) / len(sims)) if sims else 0.0,
    }


def _wilson_bounds(p, n_eff, z=1.2815515655446004):
    n = max(float(n_eff), 1.0)
    p = clamp(float(p), 0.0, 1.0)
    z2 = z * z
    denom = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / denom
    margin = z * math.sqrt(max(0.0, p * (1.0 - p) / n + z2 / (4.0 * n * n))) / denom
    return clamp(center - margin, 0.0, 1.0), clamp(center + margin, 0.0, 1.0)


def _competing_posterior(current, horizon, target):
    local = _aj_cif(current, horizon, target, local=True)
    global_ = _aj_cif(current, horizon, target, local=False)
    neff = local["effective_n"]
    if local["n"] < 4 or neff <= 1.0:
        return {
            "mean": None, "lower": None, "upper": None,
            "stop_mean": None, "stop_upper": None,
            "n": local["n"], "effective_n": neff, "censored": local["censored"],
            "status": "WARMING", "method": "AALEN_JOHANSEN_COMPETING_RISK",
        }

    shrink = neff / (neff + PRIOR_STRENGTH)
    mean = shrink * local["target"] + (1.0 - shrink) * global_["target"]
    stop_mean = shrink * local["stop"] + (1.0 - shrink) * global_["stop"]
    interval_n = neff + PRIOR_STRENGTH
    lower, upper = _wilson_bounds(mean, interval_n)
    stop_lower, stop_upper = _wilson_bounds(stop_mean, interval_n)
    total_events = len(_all_events())
    status = "ACTIVE" if total_events >= MIN_ACTIVE_EVENTS and neff >= MIN_ACTIVE_NEFF else "CALIBRATING"
    return {
        "mean": mean, "lower": lower, "upper": upper,
        "stop_mean": stop_mean, "stop_lower": stop_lower, "stop_upper": stop_upper,
        "global": global_["target"], "global_stop": global_["stop"],
        "n": local["n"], "effective_n": neff, "censored": local["censored"],
        "mean_similarity": local.get("mean_similarity", 0.0),
        "status": status, "method": "AALEN_JOHANSEN_COMPETING_RISK",
    }


def _weighted_quantile(pairs, quantile):
    pairs = [(w, v) for w, v in pairs if w > 0 and math.isfinite(v)]
    if not pairs:
        return None
    pairs.sort(key=lambda x: x[1])
    total = sum(w for w, _ in pairs)
    cutoff = total * quantile
    acc = 0.0
    for weight, value in pairs:
        acc += weight
        if acc >= cutoff:
            return value
    return pairs[-1][1]


def _mfe_stats(current):
    pairs = []
    for event in list(base.tail_resolved):
        value = optional_float(event.get("max_return_pct"))
        if value is None:
            continue
        weight, _ = _event_weight(current, event)
        if weight > 0:
            pairs.append((weight, max(0.0, value)))
    neff = _effective_n([w for w, _ in pairs])
    if neff < 5:
        return {"median": None, "p75": None, "p90": None, "effective_n": neff}
    return {
        "median": _weighted_quantile(pairs, 0.50),
        "p75": _weighted_quantile(pairs, 0.75),
        "p90": _weighted_quantile(pairs, 0.90),
        "effective_n": neff,
    }


def _evt_head(current, p10_72):
    threshold = 10.0
    exceed = []
    for event in list(base.tail_resolved):
        mfe = optional_float(event.get("max_return_pct"))
        if mfe is None or mfe <= threshold:
            continue
        weight, _ = _event_weight(current, event)
        if weight > 0:
            exceed.append((weight, mfe))
    neff = _effective_n([w for w, _ in exceed])
    if len(exceed) < 30 or neff < 15:
        return {"status": "WARMING", "n": len(exceed), "effective_n": neff, "alpha": None, "p30": None, "p50": None}
    denom = sum(w * math.log(max(v / threshold, 1.000001)) for w, v in exceed)
    sw = sum(w for w, _ in exceed)
    if denom <= 0 or sw <= 0:
        return {"status": "INVALID", "n": len(exceed), "effective_n": neff, "alpha": None, "p30": None, "p50": None}
    alpha = sw / denom
    base_p = max(0.0, f((p10_72 or {}).get("mean"), 0.0))
    return {
        "status": "ACTIVE",
        "n": len(exceed), "effective_n": neff, "alpha": alpha,
        "p30": clamp(base_p * (30.0 / threshold) ** (-alpha), 0.0, 1.0),
        "p50": clamp(base_p * (50.0 / threshold) ** (-alpha), 0.0, 1.0),
    }


def _execution_score(row):
    tape = clamp(f(row.get("pinpoint_live_tape_score"), 0.0), 0.0, 100.0)
    persistence = clamp(f(row.get("pinpoint_persistence_passes"), 0.0) / 2.0, 0.0, 1.0)
    entry_status = str(row.get("pinpoint_entry_status") or "")
    trigger = 1.0 if entry_status == "PINPOINT_TRIGGERED" else (0.72 if entry_status == "PINPOINT_ARMED" else 0.35)
    risk = f(row.get("pinpoint_risk_pct"), 0.0)
    risk_ok = 1.0 if RISK_MIN_PCT <= risk <= RISK_MAX_PCT else 0.0
    data_ok = 1.0 if all(bool(v) for v in (row.get("pinpoint_hard_status") or {}).values()) else 0.0
    contradiction = 0.0 if row.get("pinpoint_contradictions") else 1.0
    buy_bonus = 1.0 if row.get("pinpoint_buy") else 0.0
    score = 0.30 * (tape / 100.0) + 0.16 * persistence + 0.18 * trigger + 0.12 * risk_ok + 0.12 * data_ok + 0.08 * contradiction + 0.04 * buy_bonus
    return round(100.0 * clamp(score, 0.0, 1.0), 1)


def _tail_score(forecasts, confidence):
    def adj(h, t):
        item = forecasts[h][t]
        if item.get("mean") is None:
            return 0.0
        return 0.65 * f(item.get("mean")) + 0.35 * f(item.get("lower"))
    p20 = adj("24h", 20)
    p30 = adj("72h", 30)
    p40 = adj("72h", 40)
    p50 = adj("72h", 50)
    score = (
        35.0 * clamp(p20 / 0.20, 0.0, 1.0)
        + 30.0 * clamp(p30 / 0.12, 0.0, 1.0)
        + 15.0 * clamp(p40 / 0.08, 0.0, 1.0)
        + 10.0 * clamp(p50 / 0.05, 0.0, 1.0)
        + 10.0 * clamp(confidence, 0.0, 1.0)
    )
    return round(clamp(score, 0.0, 100.0), 2)


def _fixed_target_ev(prob, risk_pct, target_pct, cost_pct):
    if not isinstance(prob, dict) or prob.get("mean") is None:
        return {"ev_pct": None, "ev_r": None, "lower_ev_pct": None, "lower_ev_r": None}
    if not (RISK_MIN_PCT <= risk_pct <= RISK_MAX_PCT):
        return {"ev_pct": None, "ev_r": None, "lower_ev_pct": None, "lower_ev_r": None, "status": "INVALID_RISK_DENOMINATOR"}
    p = f(prob.get("mean"))
    p_low = f(prob.get("lower"))
    s = f(prob.get("stop_mean"))
    s_up = f(prob.get("stop_upper"), s)
    ev = p * target_pct - s * risk_pct - cost_pct
    ev_low = p_low * target_pct - s_up * risk_pct - cost_pct
    return {
        "ev_pct": ev, "ev_r": ev / risk_pct,
        "lower_ev_pct": ev_low, "lower_ev_r": ev_low / risk_pct,
        "status": "VALID",
    }


def scientist_tail_forecast(sym, row):
    current = _current_features_v2(sym, row)
    forecasts = {}
    for horizon in base.TAIL_HORIZONS:
        forecasts[horizon] = {}
        for target in base.TAIL_TARGETS:
            forecasts[horizon][int(target)] = _competing_posterior(current, horizon, target)

    for horizon in forecasts:
        prev_mean = 1.0
        prev_lower = 1.0
        prev_upper = 1.0
        for target in base.TAIL_TARGETS:
            item = forecasts[horizon][int(target)]
            if item.get("mean") is not None:
                item["mean"] = min(prev_mean, item["mean"])
                prev_mean = item["mean"]
            if item.get("lower") is not None:
                item["lower"] = min(prev_lower, item["lower"])
                prev_lower = item["lower"]
            if item.get("upper") is not None:
                item["upper"] = min(prev_upper, item["upper"])
                prev_upper = item["upper"]

    p20 = forecasts["24h"][20]
    p30 = forecasts["72h"][30]
    p10_72 = forecasts["72h"][10]
    neffs = [f(p20.get("effective_n")), f(p30.get("effective_n"))]
    confidence = clamp(min(neffs) / 40.0, 0.0, 1.0) if neffs else 0.0
    tail_potential = _tail_score(forecasts, confidence)
    execution = _execution_score(row)
    opportunity = round(0.70 * tail_potential + 0.30 * execution, 2)

    risk_pct = f(row.get("pinpoint_risk_pct"), 0.0)
    spread_cost = max(0.0, f(row.get("spread_bps"), 0.0)) / 100.0
    cost_pct = spread_cost + 0.05
    ev20 = _fixed_target_ev(p20, risk_pct, 20.0, cost_pct)
    ev30 = _fixed_target_ev(p30, risk_pct, 30.0, cost_pct)

    mfe = _mfe_stats(current)
    evt = _evt_head(current, p10_72)
    statuses = [p20.get("status"), p30.get("status")]
    if statuses and all(x == "ACTIVE" for x in statuses):
        data_status = "ACTIVE"
    elif any(x == "CALIBRATING" for x in statuses):
        data_status = "CALIBRATING"
    else:
        data_status = "WARMING"

    return {
        "version": VERSION,
        "data_status": data_status,
        "ranking_method": "COMPETING_RISK_COHORT_TAIL_POTENTIAL",
        "cohort": current.get("cohort"),
        "p20_24": p20,
        "p30_24": forecasts["24h"][30],
        "p30_72": p30,
        "p40_72": forecasts["72h"][40],
        "p50_72": forecasts["72h"][50],
        "tail_potential_score": tail_potential,
        "execution_quality_score": execution,
        "opportunity_score": opportunity,
        "tail_rank": tail_potential,
        "confidence": confidence,
        "mfe": mfe,
        "evt": evt,
        "ev20": ev20,
        "ev30": ev30,
        "expected_r_24h": ev20.get("ev_r"),
        "expected_r_24h_lower": ev20.get("lower_ev_r"),
        "expected_return_24h": {"mean": ev20.get("ev_pct"), "lower": ev20.get("lower_ev_pct")},
        "eta20_24_minutes": base._eta_target(current, "24h", 20.0),
        "eta30_72_minutes": base._eta_target(current, "72h", 30.0),
    }


base.tail_forecast = scientist_tail_forecast


def _open_tail_event_v2(sym, row):
    now = time.time()
    setup = str(row.get("pinpoint_setup") or "")
    entry_status = str(row.get("pinpoint_entry_status") or "")
    trigger = f(row.get("pinpoint_trigger"))
    stop = f(row.get("pinpoint_stop"))
    price = base._current_price(sym)
    if not setup or entry_status != "PINPOINT_TRIGGERED" or trigger <= 0 or stop <= 0 or price <= 0:
        return
    if stop >= price:
        return
    key = (sym, setup)
    if now - base.tail_last_open[key] < base.TAIL_OPEN_COOLDOWN:
        return
    if any(x.get("symbol") == sym and x.get("setup") == setup for x in base.tail_pending):
        return

    overshoot = max(0.0, (price / trigger - 1.0) * 100.0)
    cohort = _cohort_from_row(row)
    if overshoot > LATE_TRIGGER_PCT and cohort != "BUY":
        cohort = "LATE_TRIGGER"
    spread_bps = max(0.0, f(row.get("spread_bps"), 0.0))
    execution = price * (1.0 + spread_bps / 20000.0 + 0.00025)
    if execution <= stop:
        return
    features = _current_features_v2(sym, row)
    features["cohort"] = cohort
    features["trigger_overshoot_pct"] = overshoot
    event = {
        "id": f"{sym}:{int(now * 1000)}",
        "symbol": sym,
        "setup": setup,
        "cohort": cohort,
        "opened": now,
        "entry": execution,
        "first_cross_price": price,
        "modeled_execution_price": execution,
        "trigger": trigger,
        "trigger_overshoot_pct": overshoot,
        "stop": stop,
        "risk_pct": (execution - stop) / execution * 100.0,
        "pinpoint_buy_at_open": bool(row.get("pinpoint_buy")),
        "formal_at_open": row.get("formal_state") or row.get("state"),
        "tape_at_open": f(row.get("pinpoint_live_tape_score")),
        "features": features,
        "hit_ts": {},
        "horizon_labels": {},
        "horizon_returns": {},
        "max_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "last_price": price,
        "status": "OPEN",
        "recorder_precision": "2S_PATH_WITH_MODELED_EXECUTION",
    }
    base.tail_pending.append(event)
    if len(base.tail_pending) > base.TAIL_MAX_PENDING:
        del base.tail_pending[:-base.TAIL_MAX_PENDING]
    base.tail_last_open[key] = now
    base.tail_stats["opened"] += 1


base._open_tail_event = _open_tail_event_v2


def _fmt_prob(item):
    if not isinstance(item, dict) or item.get("mean") is None:
        return "-"
    return f"{item['mean']*100:.1f}%[{item['lower']*100:.1f}-{item['upper']*100:.1f}]"


async def scientist_board_loop():
    while True:
        await asyncio.sleep(BOARD_SECONDS)
        try:
            rows = base._candidate_rows()
            print(
                f"Ψ-TAIL-HUNTER SCIENTIST BOARD candidates={len(rows)} pending={len(base.tail_pending)} "
                f"resolved={len(base.tail_resolved)} model=AALEN_JOHANSEN cohorts=BUY/ARMED/TRIGGER/LATE "
                f"ranking=TAIL_POTENTIAL execution=SEPARATE sampler={base.TAIL_SAMPLE_SECONDS:.0f}s",
                flush=True,
            )
            for i, (_, _, _, sym, row, fc) in enumerate(rows[:10], 1):
                p20 = fc.get("p20_24") or {}
                p30 = fc.get("p30_72") or {}
                ev20 = fc.get("ev20") or {}
                evr = ev20.get("ev_r")
                evrl = ev20.get("lower_ev_r")
                evtxt = "-" if evr is None else f"{evr:+.2f}R[{evrl:+.2f}]"
                mfe = fc.get("mfe") or {}
                mfe75 = mfe.get("p75")
                evt = fc.get("evt") or {}
                print(
                    f"THS{i:02d}. {sym:<14} cohort={str(fc.get('cohort') or '-'):<12} "
                    f"pstate={str(row.get('pinpoint_state') or '-'):<14} setup={str(row.get('pinpoint_setup') or '-'):<29} "
                    f"TAIL={f(fc.get('tail_potential_score')):5.1f} EXEC={f(fc.get('execution_quality_score')):5.1f} "
                    f"OPP={f(fc.get('opportunity_score')):5.1f} P20_24={_fmt_prob(p20)} P30_72={_fmt_prob(p30)} "
                    f"stop20={f(p20.get('stop_mean'))*100:4.1f}% neff={f(p20.get('effective_n')):4.1f} "
                    f"EV20={evtxt} MFE75={'-' if mfe75 is None else f'{mfe75:.1f}%'} EVT={evt.get('status')} data={fc.get('data_status')}",
                    flush=True,
                )

            executable = [x for x in rows if x[4].get("pinpoint_buy")]
            print(f"Ψ-TAIL-HUNTER SCIENTIST EXECUTABLE {len(executable)}", flush=True)
            for i, (_, _, _, sym, row, fc) in enumerate(executable[:5], 1):
                print(
                    f"THSE{i:02d}. {sym:<14} setup={row.get('pinpoint_setup')} "
                    f"TAIL={f(fc.get('tail_potential_score')):.1f} EXEC={f(fc.get('execution_quality_score')):.1f} "
                    f"P20_24={_fmt_prob(fc.get('p20_24'))} P30_72={_fmt_prob(fc.get('p30_72'))} "
                    f"EV20R={((fc.get('ev20') or {}).get('ev_r'))} entry={f(row.get('pinpoint_trigger')):.10g} "
                    f"stop={f(row.get('pinpoint_stop')):.10g} risk={f(row.get('pinpoint_risk_pct')):.3f}%",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            base.tail_stats["scientist_board_errors"] += 1
            print(f"Ψ-TAIL-HUNTER SCIENTIST BOARD_ERROR {type(exc).__name__}: {exc}", flush=True)


base.tail_board_loop = scientist_board_loop

for mod in (base, getattr(base, "base", None), getattr(base, "expected", None)):
    if mod is None:
        continue
    try:
        mod.VERSION = VERSION
    except Exception:
        pass
try:
    scanner.VERSION = VERSION
except Exception:
    pass


async def main():
    print(
        "[v11.0.3.7] Ψ TAIL-HUNTER SCIENTIST active — Pinpoint remains sole BUY NOW authority. "
        "Upgrade adds censoring-aware Aalen-Johansen competing-risk P20/P30, hierarchical BUY/ARMED/TRIGGER cohorts, "
        "missing-safe similarity, richer live microstructure fingerprints, Wilson uncertainty bands, separate Tail Potential "
        "and Execution scores, risk-bounded fixed-target EV20/EV30, MFE diagnostics, and EVT monster-tail warm-up. "
        "Existing v11.0.3.6 target-before-stop history is preserved.",
        flush=True,
    )
    await base.main()


if __name__ == "__main__":
    asyncio.run(main())
