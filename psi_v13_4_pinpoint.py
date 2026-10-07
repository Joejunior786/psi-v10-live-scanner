"""Psi V13.4 setup-specific pinpoint + independent ML authority.

Purpose
-------
Remove the universal technical conjunction that could suppress otherwise-valid
entries. Genuine execution/data vetoes stay fail-closed. Normal scan authority
is split into BEAST, EXHAUSTION and anti-fakeout BREAKOUT engines. The ML lane
uses its own calibrated +10% probability evidence and can approve independently
of the technical setup engines, but can never bypass hard safety.

No order placement is performed by this module.
"""
import json
import math
import os
import time
from collections import defaultdict

REVISION = "13.4.0-pinpoint-setup-specific-ml-authority"
AUTHORITY_CHAIN = (
    "STRICT_EXISTING_OR_SETUP_SPECIFIC(BEAST|EXHAUSTION|BREAKOUT)"
    "_OR_ML10_INDEPENDENT->HARD_EXECUTION_SAFETY->BUY_NOW"
)

CORE = None
EARLY = None
V124 = None
V13 = None
PREVIOUS = None

_ORIGINAL_GATE = None
_ORIGINAL_ATTACH = None
_ORIGINAL_SCAN = None
_ORIGINAL_HEALTH = None

ACTIVE = {}
STATS = defaultdict(int)
_EARLY_CACHE = {}
_EARLY_CACHE_MS = 0

FRESH_MS = max(250, int(os.getenv("PSI_V134_FRESH_MS", "1200")))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_V134_MAX_SPREAD_BPS", "18")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_V134_MAX_SLIPPAGE_BPS", "30")))
MAX_RISK_PCT = max(0.5, float(os.getenv("PSI_V134_MAX_RISK_PCT", "8")))
MIN_RR_TP1 = max(0.8, float(os.getenv("PSI_V134_MIN_RR_TP1", "1.10")))

BEAST_MIN_SCORE = max(55.0, float(os.getenv("PSI_V134_BEAST_MIN_SCORE", "72")))
EXHAUSTION_MIN_SCORE = max(55.0, float(os.getenv("PSI_V134_EXHAUSTION_MIN_SCORE", "68")))
BREAKOUT_MIN_SCORE = max(55.0, float(os.getenv("PSI_V134_BREAKOUT_MIN_SCORE", "75")))

ML10_MIN_PROB = min(0.95, max(0.50, float(os.getenv("PSI_V134_ML10_MIN_PROB", "0.58"))))
ML10_MIN_TOTAL = max(30, int(os.getenv("PSI_V134_ML10_MIN_TOTAL", "100")))
ML10_MIN_TEST = max(10, int(os.getenv("PSI_V134_ML10_MIN_TEST", "30")))
ML10_MIN_CI = min(0.90, max(0.35, float(os.getenv("PSI_V134_ML10_MIN_CI", "0.50"))))
REPORT_LIMIT = max(10, min(int(os.getenv("PSI_V134_REPORT_LIMIT", "30")), 60))

BEAST_SETUPS = {
    "COILED_ACCUMULATION",
    "COMPRESSION_BREAKOUT",
    "TREND_CONTINUATION",
}
EXHAUSTION_SETUPS = {
    "DEEP_PULLBACK_EXHAUSTION",
    "VOLUME_CLIMAX_REVERSAL",
    "DAILY_RANGE_BOTTOM_REVERSAL",
    "FAILED_BREAKDOWN_RECLAIM",
    "LIQUIDITY_SWEEP_REVERSAL",
    "HIGHER_LOW_REVERSAL",
}
BREAKOUT_SETUPS = {
    "COMPRESSION_BREAKOUT",
    "BREAKOUT_RETEST",
    "COILED_ACCUMULATION",
    "TREND_CONTINUATION",
}


def _f(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _clamp(value, low=0.0, high=100.0):
    return max(low, min(high, value))


def _now_ms():
    return int(time.time() * 1000)


def _early_map():
    global _EARLY_CACHE, _EARLY_CACHE_MS
    now = _now_ms()
    if _EARLY_CACHE and now - _EARLY_CACHE_MS <= 250:
        return _EARLY_CACHE
    rows = []
    if EARLY is not None:
        try:
            rows = list(EARLY.early_candidates(250, actionable_only=False) or [])
        except Exception:
            rows = list(getattr(EARLY, "_latest_candidates", []) or [])
    _EARLY_CACHE = {
        str(row.get("symbol") or "").upper(): row
        for row in rows
        if isinstance(row, dict) and row.get("symbol")
    }
    _EARLY_CACHE_MS = now
    return _EARLY_CACHE


def _active_setups(structural):
    structural = structural if isinstance(structural, dict) else {}
    rows = []
    best_name = str(structural.get("setup") or "")
    if best_name:
        rows.append({
            "name": best_name,
            "state": str(structural.get("state") or ""),
            "strength": _f(structural.get("setup_strength")),
        })
    for row in list(structural.get("active_setups") or []):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if not name:
            continue
        rows.append({
            "name": name,
            "state": str(row.get("state") or ""),
            "strength": _f(row.get("strength")),
        })
    dedup = {}
    for row in rows:
        old = dedup.get(row["name"])
        if old is None or row["strength"] > old["strength"]:
            dedup[row["name"]] = row
    return list(dedup.values())


def _family_matches(structural, family):
    return [row for row in _active_setups(structural) if row["name"] in family]


def _sensor_fresh(early, now_ms=None):
    early = early if isinstance(early, dict) else {}
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    generated = int(_f(early.get("generated_ms"), 0))
    return bool(
        early.get("hard_sensor_safety")
        and generated > 0
        and 0 <= now_ms - generated <= FRESH_MS
        and 0 <= _f(early.get("trade_age_ms"), 1e12) <= FRESH_MS
        and 0 <= _f(early.get("book_age_ms"), 1e12) <= FRESH_MS
        and early.get("sequence_verified")
        and early.get("book_sequence_verified")
    )


def _flow_evidence(early):
    early = early if isinstance(early, dict) else {}
    buy_ratio = _f(early.get("buy_ratio"), 0.5)
    cvd = _f(early.get("cvd_acceleration"))
    ofi = max(_f(early.get("ofi")), _f(early.get("ofi_acceleration")))
    obi = _f(early.get("obi"))
    depletion = _f(early.get("ask_depletion"))
    rv10 = _f(early.get("relative_volume_10s"))
    rv30 = _f(early.get("relative_volume_30s"))
    trades = _f(early.get("trade_acceleration"))
    delta = _f(early.get("change_point_delta"))

    pressure = {
        "buyer_dominance": buy_ratio >= 0.56,
        "cvd_positive": cvd > 0.0,
        "order_flow_positive": ofi > 0.0,
        "book_positive": obi >= 0.02 or depletion > 0.0,
        "activity_accelerating": (
            rv10 >= 1.25 or rv30 >= 1.15 or trades >= 1.35 or delta >= 6.0
        ),
    }
    pressure_count = sum(bool(pressure[k]) for k in (
        "cvd_positive", "order_flow_positive", "book_positive"
    ))
    return {
        **pressure,
        "pressure_count": pressure_count,
        "buy_ratio": buy_ratio,
        "cvd_acceleration": cvd,
        "ofi": ofi,
        "obi": obi,
        "ask_depletion": depletion,
        "rv10": rv10,
        "rv30": rv30,
        "trade_acceleration": trades,
        "change_point_delta": delta,
    }


def _risk_plan(structural, early=None, legacy=None):
    structural = structural if isinstance(structural, dict) else {}
    early = early if isinstance(early, dict) else {}
    legacy = legacy if isinstance(legacy, dict) else {}

    entry = _f(early.get("entry_reference"))
    if entry <= 0:
        entry = _f(structural.get("current"), _f(structural.get("entry")))
    stop = _f(structural.get("invalidation"), _f(structural.get("stop")))
    if stop <= 0 or stop >= entry:
        stop = _f(legacy.get("pinpoint_stop"))

    tp1 = _f(structural.get("tp1"))
    tp2 = _f(structural.get("tp2"))
    tp3 = _f(structural.get("tp3"))
    if not (entry > 0 and 0 < stop < entry):
        return {
            "valid": False, "entry": entry, "stop": stop,
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "risk_pct": 0.0, "rr_tp1": 0.0,
        }

    risk = entry - stop
    risk_pct = risk / entry * 100.0
    if tp1 <= entry:
        tp1 = entry + 1.5 * risk
    if tp2 <= tp1:
        tp2 = entry + 2.5 * risk
    if tp3 <= tp2:
        tp3 = entry + 4.0 * risk
    rr = (tp1 - entry) / risk if risk > 0 else 0.0
    return {
        "valid": bool(
            risk_pct > 0
            and risk_pct <= MAX_RISK_PCT
            and rr >= MIN_RR_TP1
            and tp1 > entry
            and tp2 > tp1
            and tp3 > tp2
        ),
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk_pct": risk_pct,
        "rr_tp1": rr,
    }


def _hard_safety(structural, legacy, micro, integrity, early=None):
    """Only genuine data/execution vetoes live here.

    No structural BUY requirement, no universal OFI/CVD conjunction, no
    Pinpoint persistence requirement and no final technical approval label.
    """
    structural = structural if isinstance(structural, dict) else {}
    legacy = legacy if isinstance(legacy, dict) else {}
    micro = micro if isinstance(micro, dict) else {}
    integrity = integrity if isinstance(integrity, dict) else {}
    blockers = []

    if not bool(micro.get("micro_ready")):
        blockers.append("LIVE_MICRO_DATA")
    if not bool(micro.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE_VALID")
    if not bool(micro.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE_VALID")

    spread = _f(micro.get("spread_bps"), 999999.0)
    slip = _f(micro.get("slippage_bps"), 999999.0)
    if spread < 0 or spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD_FILTER")
    if slip < 0 or slip > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE_FILTER")

    ages = integrity.get("ages") if isinstance(integrity.get("ages"), dict) else {}
    age_limits = (
        ("micro_trade_ms", 15000.0, "STALE_DEPTH_TRADE"),
        ("micro_book_ms", 5000.0, "STALE_DEPTH_BOOK"),
        ("tape_ms", 5000.0, "STALE_EVENT_TAPE"),
        ("bbo_ms", 5000.0, "STALE_EVENT_BBO"),
    )
    # Missing age telemetry stays fail-closed in production. Unit callers can
    # omit integrity entirely only when fresh micro timestamps are explicit.
    if ages:
        for key, limit, blocker in age_limits:
            if _f(ages.get(key), 999999999.0) > limit:
                blockers.append(blocker)
    elif not _sensor_fresh(early):
        blockers.append("LIVE_DATA_INTEGRITY")

    hard = legacy.get("pinpoint_hard_status") or {}
    if hard.get("MARKET_REGIME_SAFETY") is False or hard.get("market_regime_safety") is False:
        blockers.append("MARKET_REGIME_SAFETY")

    if bool(structural.get("anti_chase")):
        blockers.append("CUMULATIVE_EXTENSION_GUARD")
    max_chase = _f(structural.get("max_chase"))
    current = _f(structural.get("current"), _f((early or {}).get("entry_reference")))
    if max_chase > 0 and current > max_chase:
        blockers.append("CUMULATIVE_EXTENSION_GUARD")
    if legacy.get("pinpoint_anti_chase_ok") is False:
        blockers.append("CUMULATIVE_EXTENSION_GUARD")

    risk = _risk_plan(structural, early, legacy)
    if not risk["valid"]:
        blockers.append("VALID_RISK_PLAN")

    blockers = list(dict.fromkeys(str(x) for x in blockers if str(x)))
    return {
        "pass": not blockers,
        "blockers": blockers,
        "risk": risk,
        "spread_bps": spread,
        "slippage_bps": slip,
    }


def _beast_engine(structural, early, now_ms=None):
    matches = _family_matches(structural, BEAST_SETUPS)
    if not matches:
        return {"engine": "BEAST", "pass": False, "score": 0.0, "blockers": ["BEAST_STRUCTURE"]}
    flow = _flow_evidence(early)
    fresh = _sensor_fresh(early, now_ms)
    hazard = _f(early.get("hazard_score"))
    probe = _f(early.get("v128_probe_score"))
    sequence = _f(early.get("sequence_score"))
    strength = max([row["strength"] for row in matches] or [0.0])
    setup_buy = any(row["state"] == "BUY" for row in matches)

    score = _clamp(
        0.28 * hazard
        + 0.22 * probe
        + 0.16 * sequence
        + 0.18 * strength
        + (8.0 if flow["buyer_dominance"] else 0.0)
        + (5.0 if flow["activity_accelerating"] else 0.0)
        + 3.0 * flow["pressure_count"]
        + (4.0 if setup_buy else 0.0)
    )
    blockers = []
    if not fresh:
        blockers.append("BEAST_FRESH_SENSOR")
    if hazard < 62.0 and probe < 68.0:
        blockers.append("BEAST_EARLY_HAZARD")
    if not flow["buyer_dominance"]:
        blockers.append("BEAST_BUYER_DOMINANCE")
    if not flow["activity_accelerating"]:
        blockers.append("BEAST_ACTIVITY_ACCELERATION")
    if flow["pressure_count"] < 1:
        blockers.append("BEAST_FLOW_OR_BOOK_PRESSURE")
    if score < BEAST_MIN_SCORE:
        blockers.append("BEAST_SCORE")
    return {
        "engine": "BEAST",
        "pass": not blockers,
        "score": round(score, 2),
        "blockers": blockers,
        "matched_setups": matches,
        "flow": flow,
        "entry_reason": "pre-breakout acceleration + buyer control without requiring universal BUY gates",
    }


def _exhaustion_engine(structural, early, now_ms=None):
    matches = _family_matches(structural, EXHAUSTION_SETUPS)
    if not matches:
        return {"engine": "EXHAUSTION", "pass": False, "score": 0.0, "blockers": ["EXHAUSTION_STRUCTURE"]}
    flow = _flow_evidence(early)
    fresh = _sensor_fresh(early, now_ms)
    hazard = _f(early.get("hazard_score"))
    strength = max([row["strength"] for row in matches] or [0.0])
    structural_takeover = any(row["state"] == "BUY" for row in matches)
    live_takeover = (
        flow["buy_ratio"] >= 0.54
        and (flow["cvd_positive"] or flow["order_flow_positive"] or flow["book_positive"])
    )
    score = _clamp(
        0.50 * strength
        + 0.20 * hazard
        + (18.0 if structural_takeover else 0.0)
        + (12.0 if live_takeover else 0.0)
        + 2.0 * flow["pressure_count"]
    )
    blockers = []
    if not fresh:
        blockers.append("EXHAUSTION_FRESH_SENSOR")
    if not (structural_takeover or live_takeover):
        blockers.append("EXHAUSTION_BUYER_TAKEOVER")
    if flow["buy_ratio"] < 0.52:
        blockers.append("EXHAUSTION_BUY_RATIO")
    if score < EXHAUSTION_MIN_SCORE:
        blockers.append("EXHAUSTION_SCORE")
    return {
        "engine": "EXHAUSTION",
        "pass": not blockers,
        "score": round(score, 2),
        "blockers": blockers,
        "matched_setups": matches,
        "flow": flow,
        "entry_reason": "seller exhaustion + observed buyer takeover",
    }


def _breakout_engine(structural, early, now_ms=None):
    matches = _family_matches(structural, BREAKOUT_SETUPS)
    if not matches:
        return {"engine": "BREAKOUT", "pass": False, "score": 0.0, "blockers": ["BREAKOUT_STRUCTURE"]}
    flow = _flow_evidence(early)
    fresh = _sensor_fresh(early, now_ms)
    hazard = _f(early.get("hazard_score"))
    probe = _f(early.get("v128_probe_score"))
    sequence = _f(early.get("sequence_score"))
    strength = max([row["strength"] for row in matches] or [0.0])
    confirmed_break = any(
        row["state"] == "BUY"
        and row["name"] in {"COMPRESSION_BREAKOUT", "BREAKOUT_RETEST", "TREND_CONTINUATION"}
        for row in matches
    )
    retest = any(row["name"] == "BREAKOUT_RETEST" for row in matches)
    sequence_guard = sequence >= 55.0 or probe >= 62.0 or retest
    fakeout_flow = (
        flow["buyer_dominance"]
        and flow["activity_accelerating"]
        and flow["pressure_count"] >= (1 if retest and confirmed_break else 2)
    )
    score = _clamp(
        0.28 * strength
        + 0.22 * hazard
        + 0.18 * sequence
        + 0.12 * probe
        + (8.0 if confirmed_break else 0.0)
        + (7.0 if retest else 0.0)
        + (5.0 if flow["buyer_dominance"] else 0.0)
        + 3.0 * flow["pressure_count"]
    )
    blockers = []
    if not fresh:
        blockers.append("BREAKOUT_FRESH_SENSOR")
    if not fakeout_flow:
        blockers.append("BREAKOUT_ANTI_FAKEOUT_FLOW")
    if not sequence_guard:
        blockers.append("BREAKOUT_SEQUENCE_CONFIRMATION")
    if score < BREAKOUT_MIN_SCORE:
        blockers.append("BREAKOUT_SCORE")
    return {
        "engine": "BREAKOUT",
        "pass": not blockers,
        "score": round(score, 2),
        "blockers": blockers,
        "matched_setups": matches,
        "flow": flow,
        "anti_fakeout": {
            "confirmed_break": confirmed_break,
            "retest": retest,
            "sequence_guard": sequence_guard,
            "flow_guard": fakeout_flow,
        },
        "entry_reason": "breakout/retest with activity + buyer/flow confirmation to suppress fakeouts",
    }


def _setup_decision(structural, early, now_ms=None):
    engines = [
        _beast_engine(structural, early, now_ms),
        _exhaustion_engine(structural, early, now_ms),
        _breakout_engine(structural, early, now_ms),
    ]
    passed = [row for row in engines if row.get("pass")]
    chosen = max(passed or engines, key=lambda row: _f(row.get("score")))
    return {
        "pass": bool(passed),
        "chosen": chosen,
        "engines": engines,
    }


def _ml10_probability(symbol, structural=None, legacy=None):
    symbol = str(symbol or "").upper()
    if V124 is None or not hasattr(V124, "ml_probability"):
        return {"symbol": symbol, "qualified": False, "reason": "ML10_UNAVAILABLE"}
    try:
        row = dict(V124.ml_probability(symbol, structural or {}, legacy or {}) or {})
    except Exception as exc:
        return {
            "symbol": symbol,
            "qualified": False,
            "reason": "ML10_ERROR:" + type(exc).__name__,
        }
    row["symbol"] = symbol
    row["target_pct"] = _f(row.get("target_pct"), 10.0)
    p = _f(row.get("probability"), -1.0)
    total = int(_f(row.get("overall_samples")))
    test = int(_f(row.get("test_samples")))
    overall_ci = _f(row.get("overall_ci_low"))
    test_ci = _f(row.get("test_ci_low"))
    independent = bool(
        p >= ML10_MIN_PROB
        and total >= ML10_MIN_TOTAL
        and test >= ML10_MIN_TEST
        and overall_ci >= ML10_MIN_CI
        and test_ci >= ML10_MIN_CI
    )
    row["independent_authority_ready"] = independent
    row["independent_threshold"] = ML10_MIN_PROB
    row["independent_min_total"] = ML10_MIN_TOTAL
    row["independent_min_test"] = ML10_MIN_TEST
    row["independent_min_ci"] = ML10_MIN_CI
    return row


def _ml15_map():
    out = {}
    if V13 is None:
        return out
    for row in list(getattr(V13, "_ranked", []) or []):
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper()
        if sym:
            out[sym] = row
    return out


def _candidate_probabilities(limit=REPORT_LIMIT):
    board = []
    if CORE is not None:
        try:
            board = list(CORE._board() or [])
        except Exception:
            board = []
    latest = getattr(getattr(CORE, "q", None), "latest", {}) or {}
    ml15 = _ml15_map()
    rows = []
    for structural in board[:max(limit * 2, limit)]:
        if not isinstance(structural, dict):
            continue
        sym = str(structural.get("symbol") or "").upper()
        if not sym:
            continue
        p10 = _ml10_probability(sym, structural, latest.get(sym) or {})
        p15 = ml15.get(sym) or {}
        prob10 = p10.get("probability")
        prob15 = p15.get("model_probability")
        p10f = _f(prob10, -1.0)
        p15f = _f(prob15, -1.0)
        priority = (
            (0.72 * p10f if p10f >= 0 else 0.0)
            + (0.28 * p15f if p15f >= 0 else 0.0)
            + (0.03 if p10.get("independent_authority_ready") else 0.0)
        )
        rows.append({
            "symbol": sym,
            "plus10_probability": round(p10f, 4) if p10f >= 0 else None,
            "plus10_target_pct": p10.get("target_pct", 10.0),
            "plus10_horizon": p10.get("horizon"),
            "plus10_calibration_key": p10.get("calibration_key"),
            "plus10_samples": int(_f(p10.get("overall_samples"))),
            "plus10_test_samples": int(_f(p10.get("test_samples"))),
            "plus10_ci_low": (
                round(min(_f(p10.get("overall_ci_low")), _f(p10.get("test_ci_low"))), 4)
                if p10f >= 0 else None
            ),
            "ml10_independent_ready": bool(p10.get("independent_authority_ready")),
            "ml15_probability": round(p15f, 4) if p15f >= 0 else None,
            "ml15_rank": p15.get("rank"),
            "ml_priority_score": round(priority, 5),
            "calibration_status": p10.get("reason"),
        })
    rows.sort(
        key=lambda row: (
            row["ml10_independent_ready"],
            _f(row.get("plus10_probability"), -1.0),
            _f(row.get("ml15_probability"), -1.0),
            _f(row.get("ml_priority_score"), -1.0),
        ),
        reverse=True,
    )
    return rows[:limit]


def _gate_wrapper(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    result = dict(_ORIGINAL_GATE(structural_row, legacy_row, micro_metrics, integrity))
    structural_row = structural_row if isinstance(structural_row, dict) else {}
    legacy_row = legacy_row if isinstance(legacy_row, dict) else {}
    micro_metrics = micro_metrics if isinstance(micro_metrics, dict) else {}
    integrity = integrity if isinstance(integrity, dict) else {}
    symbol = str(structural_row.get("symbol") or legacy_row.get("symbol") or "").upper()

    # Preserve already-approved authorities exactly. V13.4 exists to remove
    # false universal blockers, not to invalidate a genuine earlier approval.
    if result.get("buy_now"):
        result["v13_4_evaluated"] = True
        result["v13_4_preserved_prior_authority"] = True
        ACTIVE.pop(symbol, None)
        STATS["preserved_prior_buy"] += 1
        return result

    early = _early_map().get(symbol) or {}
    setup = _setup_decision(structural_row, early)
    ml10 = _ml10_probability(symbol, structural_row, legacy_row)
    safety = _hard_safety(structural_row, legacy_row, micro_metrics, integrity, early)

    result["v13_4_evaluated"] = True
    result["v13_4_setup_decision"] = setup
    result["v13_4_ml10"] = ml10
    result["v13_4_hard_safety"] = {
        "pass": safety["pass"],
        "blockers": list(safety["blockers"]),
    }
    result["authority_chain"] = AUTHORITY_CHAIN

    setup_ready = bool(setup.get("pass"))
    ml_ready = bool(ml10.get("independent_authority_ready"))

    if not safety["pass"] or not (setup_ready or ml_ready):
        ACTIVE.pop(symbol, None)
        if not safety["pass"]:
            STATS["hard_safety_blocked"] += 1
        else:
            STATS["evidence_not_ready"] += 1
        return result

    chosen = setup.get("chosen") or {}
    if setup_ready and ml_ready:
        route = "SETUP_" + str(chosen.get("engine") or "UNKNOWN") + "+ML10"
    elif setup_ready:
        route = "SETUP_" + str(chosen.get("engine") or "UNKNOWN")
    else:
        route = "ML10_INDEPENDENT"

    risk = safety["risk"]
    active = {
        "symbol": symbol,
        "route": route,
        "setup_engine": chosen.get("engine") if setup_ready else None,
        "setup_score": chosen.get("score") if setup_ready else None,
        "plus10_probability": ml10.get("probability"),
        "plus10_calibration_key": ml10.get("calibration_key"),
        "entry": risk.get("entry"),
        "stop": risk.get("stop"),
        "tp1": risk.get("tp1"),
        "tp2": risk.get("tp2"),
        "tp3": risk.get("tp3"),
        "risk_pct": round(_f(risk.get("risk_pct")), 4),
        "rr_tp1": round(_f(risk.get("rr_tp1")), 3),
        "activated_ms": _now_ms(),
    }
    ACTIVE[symbol] = active
    STATS["buy_now_total"] += 1
    STATS["setup_buy_now"] += int(setup_ready)
    STATS["ml10_buy_now"] += int(ml_ready)
    if setup_ready:
        STATS["engine_" + str(chosen.get("engine") or "UNKNOWN").lower()] += 1

    result.update({
        "buy_now": True,
        "execution_state": "BUY NOW",
        "blockers": [],
        "authority_chain": AUTHORITY_CHAIN,
        "execution_route": route,
        "pinpoint_entry_status": route + "_TRIGGERED",
        "pinpoint_state": "BUY NOW",
        "v13_4_buy": True,
        "v13_4_setup_buy": setup_ready,
        "v13_4_ml_independent_buy": ml_ready,
        "v13_4_setup_engine": chosen.get("engine") if setup_ready else None,
        "v13_4_setup_score": chosen.get("score") if setup_ready else None,
        "v13_4_plus10_probability": ml10.get("probability"),
        "v13_4_entry": risk.get("entry"),
        "v13_4_stop": risk.get("stop"),
        "v13_4_tp1": risk.get("tp1"),
        "v13_4_tp2": risk.get("tp2"),
        "v13_4_tp3": risk.get("tp3"),
        "v13_4_risk_pct": active["risk_pct"],
        "v13_4_rr_tp1": active["rr_tp1"],
    })
    return result


def _attach_wrapper(symbol, structural_row):
    row = dict(_ORIGINAL_ATTACH(symbol, structural_row))
    sym = str(symbol or "").upper()
    active = ACTIVE.get(sym)
    if active and row.get("execution_state") == "BUY NOW":
        row["execution_route"] = active["route"]
        row["execution_entry"] = active["entry"]
        row["execution_stop"] = active["stop"]
        row["execution_tp1"] = active["tp1"]
        row["execution_tp2"] = active["tp2"]
        row["execution_tp3"] = active["tp3"]
        row["execution_risk_pct"] = active["risk_pct"]
        row["execution_rr_tp1"] = active["rr_tp1"]
        row["plus10_probability"] = active["plus10_probability"]
        row["setup_engine"] = active["setup_engine"]
        row["setup_engine_score"] = active["setup_score"]
    return row


def _enrich_list(rows, probability_index):
    out = []
    for row in list(rows or []):
        if not isinstance(row, dict):
            out.append(row)
            continue
        item = dict(row)
        sym = str(item.get("symbol") or "").upper()
        p = probability_index.get(sym)
        if p:
            item["plus10_probability"] = p.get("plus10_probability")
            item["plus10_samples"] = p.get("plus10_samples")
            item["plus10_test_samples"] = p.get("plus10_test_samples")
            item["plus10_ci_low"] = p.get("plus10_ci_low")
            item["ml_top_pick_score"] = p.get("ml_priority_score")
        out.append(item)
    return out


def _augment(response):
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response

    probability_rows = _candidate_probabilities(REPORT_LIMIT)
    probability_index = {row["symbol"]: row for row in probability_rows}
    if isinstance(data.get("results"), list):
        data["results"] = _enrich_list(data["results"], probability_index)
    if isinstance(data.get("buy_now"), list):
        data["buy_now"] = _enrich_list(data["buy_now"], probability_index)

    top_pick = probability_rows[0] if probability_rows else None
    data["version"] = REVISION
    data["execution_authority"] = "SETUP_SPECIFIC_OR_ML10_WITH_HARD_SAFETY"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["v13_4_pinpoint"] = {
        "revision": REVISION,
        "universal_buy_blockers_removed": True,
        "hard_vetoes_retained": [
            "fresh live micro/data integrity",
            "trade/book sequence validity",
            "spread/slippage",
            "market-regime hard safety when explicitly unsafe",
            "anti-chase/max-chase",
            "valid risk plan",
        ],
        "normal_scan_engines": {
            "BEAST": "pre-breakout acceleration + buyer control",
            "EXHAUSTION": "seller exhaustion + buyer takeover",
            "BREAKOUT": "breakout/retest + anti-fakeout flow/sequence checks",
        },
        "ml_authority": {
            "independent": True,
            "target": "+10% before invalidation",
            "minimum_probability": ML10_MIN_PROB,
            "minimum_total_samples": ML10_MIN_TOTAL,
            "minimum_test_samples": ML10_MIN_TEST,
            "minimum_ci_low": ML10_MIN_CI,
            "technical_buy_rules_required": False,
            "hard_execution_safety_required": True,
        },
        "candidate_probabilities": probability_rows,
        "ml_top_pick": top_pick,
        "active_buy_now": list(ACTIVE.values())[:30],
        "stats": dict(STATS),
        "rule": (
            "A normal BUY NOW may be approved by one setup-specific engine without "
            "passing unrelated setup rules. ML may approve independently using "
            "calibrated +10% evidence. Neither path can bypass hard safety."
        ),
    }
    return CORE.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    return _augment(await _ORIGINAL_SCAN(request))


async def _health_wrapper(request):
    return _augment(await _ORIGINAL_HEALTH(request))


def install(core, early, v124=None, v13=None, previous=None):
    global CORE, EARLY, V124, V13, PREVIOUS
    global _ORIGINAL_GATE, _ORIGINAL_ATTACH, _ORIGINAL_SCAN, _ORIGINAL_HEALTH
    if CORE is not None:
        return

    CORE, EARLY, V124, V13, PREVIOUS = core, early, v124, v13, previous
    _ORIGINAL_GATE = core._strict_execution_gate
    _ORIGINAL_ATTACH = core._attach_execution_gate
    _ORIGINAL_SCAN = core.v12_scan
    _ORIGINAL_HEALTH = core.v12_health

    core._strict_execution_gate = _gate_wrapper
    core._attach_execution_gate = _attach_wrapper
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        "PSI-V13.4 INSTALLED revision=" + REVISION
        + " universalBuyConjunction=REMOVED"
        + " setupAuthorities=BEAST,EXHAUSTION,BREAKOUT"
        + " ml10Authority=INDEPENDENT"
        + " hardSafety=FAIL_CLOSED",
        flush=True,
    )
