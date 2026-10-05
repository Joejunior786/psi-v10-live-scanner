import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

REVISION = "12.7.0-sequential-counterfactual-intelligence"
ROLE = "SEQUENCE_RANKING+CONTEXTUAL_RISK+COUNTERFACTUAL_BLOCKER_LEARNING"
STRICT_CONVENTIONAL_BUY_AUTHORITY_UNCHANGED = True

CORE = None
HARDENING = None
LEARNER = None
V124 = None
V125 = None
V126 = None

FRESH_MS = max(250, int(os.getenv("PSI_V127_FRESH_MS", "1200")))
SEQ_HISTORY = max(8, min(int(os.getenv("PSI_V127_SEQ_HISTORY", "24")), 80))
SEQ_WATCH = max(55.0, float(os.getenv("PSI_V127_SEQ_WATCH", "68")))
SEQ_ARMED = max(SEQ_WATCH, float(os.getenv("PSI_V127_SEQ_ARMED", "78")))
SEQ_PINPOINT = max(SEQ_ARMED, float(os.getenv("PSI_V127_SEQ_PINPOINT", "88")))
EXTENSION_HARD_CAP_PCT = max(1.0, min(float(os.getenv("PSI_V127_EXTENSION_HARD_CAP_PCT", "3.25")), 6.0))
EXTENSION_MAX_BUDGET_MULT = max(1.0, min(float(os.getenv("PSI_V127_EXTENSION_MAX_BUDGET_MULT", "1.65")), 2.0))
DIAG_SECONDS = max(5.0, float(os.getenv("PSI_V127_DIAG_SECONDS", "10")))
PROMOTION_SLOTS = max(6, min(int(os.getenv("PSI_V127_PROMOTION_SLOTS", "16")), 32))
LEARN_REFRESH_SECONDS = max(5.0, float(os.getenv("PSI_V127_LEARN_REFRESH_SECONDS", "10")))

_history = defaultdict(lambda: deque(maxlen=SEQ_HISTORY))
_blocker_learning_cache = {}
_blocker_learning_n = 0
_blocker_learning_mono = 0.0
_stats = defaultdict(int)
_last_diag_mono = 0.0

_original_rebuild = None
_original_promotion_symbols = None
_original_hard_safety = None
_original_scan = None
_original_health = None
_original_features = None

HARD_NEVER_BYPASS = {
    "LIVE_PRICE", "BINANCE_SPOT_UNIVERSE", "LIVE_MICRO_DATA",
    "TRADE_SEQUENCE_VALID", "BOOK_SEQUENCE_VALID", "SPREAD_FILTER",
    "SLIPPAGE_FILTER", "STALE_DEPTH_TRADE", "STALE_DEPTH_BOOK",
    "STALE_EVENT_TAPE", "STALE_EVENT_BBO", "STALE_STRUCTURE",
    "VALID_RISK_PLAN", "MARKET_REGIME_SAFETY",
}
CONTEXTUAL_BLOCKERS = {
    "CUMULATIVE_EXTENSION_GUARD", "ANTI_CHASE", "ANTI_CHASE_CLEAR",
    "SECOND_IGNITION_STRUCTURE", "CONTROLLED_5M_ACCELERATION",
    "TRUE_COMPRESSION", "NEAR_OR_BREAKING_RESISTANCE", "FLOW_CONFIRMATION",
    "REAL_ORDER_BOOK_PRESSURE", "VWAP_HOLD_OR_RECLAIM", "VWAP_HOLD",
}


def _f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def _structural_map():
    if CORE is None:
        return {}
    try:
        return {str(r.get("symbol") or "").upper(): r for r in (CORE._board() or []) if isinstance(r, dict)}
    except Exception:
        return {}


def _fresh_candidate(row):
    if not isinstance(row, dict) or not bool(row.get("hard_sensor_safety")):
        return False
    return (
        _f(row.get("trade_age_ms"), 999999999) <= FRESH_MS
        and _f(row.get("book_age_ms"), 999999999) <= FRESH_MS
        and bool(row.get("sequence_verified"))
        and bool(row.get("book_sequence_verified"))
    )


def _alignment_score(row):
    buy = _f(row.get("buy_ratio"), 0.5)
    rv = _f(row.get("relative_volume_10s"))
    ta = _f(row.get("trade_acceleration"))
    cvda = _f(row.get("cvd_acceleration"))
    ofi = _f(row.get("ofi"))
    ofia = _f(row.get("ofi_acceleration"))
    obi = _f(row.get("obi"))
    dep = _f(row.get("ask_depletion"))
    score = 0.0
    score += 18.0 if buy >= 0.65 else 12.0 if buy >= 0.58 else 4.0 if buy >= 0.53 else 0.0
    score += 15.0 if rv >= 4.0 else 11.0 if rv >= 2.0 else 5.0 if rv >= 1.25 else 0.0
    score += 13.0 if ta >= 4.0 else 9.0 if ta >= 2.0 else 4.0 if ta >= 1.25 else 0.0
    score += 10.0 if cvda > 0 else 0.0
    score += 14.0 if ofi >= 0.15 else 10.0 if ofi >= 0.05 else 5.0 if ofi > 0 else 0.0
    score += 6.0 if ofia > 0 else 0.0
    score += 14.0 if obi >= 0.15 else 10.0 if obi >= 0.05 else 5.0 if obi > 0 else 0.0
    score += 10.0 if dep >= 0.02 else 5.0 if dep > 0 else 0.0
    if ofi < -0.10:
        score -= 14.0
    if obi < -0.10:
        score -= 14.0
    return _clamp(score)


def _snapshot(row):
    return {
        "ts": int(time.time() * 1000),
        "safe": _fresh_candidate(row),
        "haz": _f(row.get("hazard_score")),
        "delta": _f(row.get("change_point_delta")),
        "buy": _f(row.get("buy_ratio"), 0.5),
        "rv": _f(row.get("relative_volume_10s")),
        "ta": _f(row.get("trade_acceleration")),
        "cvda": _f(row.get("cvd_acceleration")),
        "ofi": _f(row.get("ofi")),
        "obi": _f(row.get("obi")),
        "dep": _f(row.get("ask_depletion")),
        "align": _alignment_score(row),
    }


def _stage_flags(s):
    return {
        "activity": s["rv"] >= 1.5 or s["ta"] >= 1.75,
        "flow": s["buy"] >= 0.58 and s["ofi"] > 0,
        "book": s["obi"] > 0,
        "thrust": s["haz"] >= SEQ_WATCH or s["delta"] >= 8.0,
    }


def _first_idx(hist, key):
    for i, s in enumerate(hist):
        if _stage_flags(s).get(key):
            return i
    return None


def _sequence_order_score(hist):
    if not hist:
        return 0.0
    h = list(hist)[-12:]
    ia, ifl, ib, it = (_first_idx(h, k) for k in ("activity", "flow", "book", "thrust"))
    score = 0.0
    if ia is not None:
        score += 20.0
    if ifl is not None:
        score += 20.0
    if ib is not None:
        score += 20.0
    if it is not None:
        score += 15.0
    if ia is not None and ifl is not None and ia <= ifl:
        score += 10.0
    if ia is not None and ib is not None and ia <= ib:
        score += 5.0
    if it is not None and ifl is not None and ib is not None and max(ifl, ib) <= it:
        score += 10.0
    if h[-1]["safe"]:
        score += 10.0
    return _clamp(score)


def _momentum_score(hist):
    h = list(hist)[-6:]
    if len(h) < 2:
        return 50.0
    hazards = [x["haz"] for x in h]
    velocity = hazards[-1] - hazards[-2]
    prev_velocity = hazards[-2] - hazards[-3] if len(hazards) >= 3 else 0.0
    acceleration = velocity - prev_velocity
    score = 50.0 + 3.0 * velocity + 1.5 * acceleration + 0.6 * h[-1]["delta"]
    return _clamp(score)


def _persistence_score(hist):
    h = list(hist)[-6:]
    if not h:
        return 0.0
    weights = list(range(1, len(h) + 1))
    num = sum(w for w, s in zip(weights, h) if s["safe"] and s["align"] >= 60.0)
    return 100.0 * num / max(sum(weights), 1)


def _sequence_metrics(symbol, row):
    hist = _history[symbol]
    order = _sequence_order_score(hist)
    align = _alignment_score(row)
    momentum = _momentum_score(hist)
    persistence = _persistence_score(hist)
    hazard = _f(row.get("hazard_score"))
    score = _clamp(0.30 * hazard + 0.25 * align + 0.20 * order + 0.15 * momentum + 0.10 * persistence)
    fresh = _fresh_candidate(row)
    state = "SEQUENCE_DATA_WAIT"
    if fresh and score >= SEQ_PINPOINT and align >= 75:
        state = "SEQUENCE_PINPOINT"
    elif fresh and score >= SEQ_ARMED and align >= 65:
        state = "SEQUENCE_ARMED"
    elif fresh and score >= SEQ_WATCH:
        state = "SEQUENCE_WATCH"
    return {
        "sequence_score": round(score, 2),
        "sequence_state": state,
        "sequence_order_score": round(order, 2),
        "alignment_score": round(align, 2),
        "momentum_score": round(momentum, 2),
        "sequence_persistence": round(persistence, 2),
        "fresh_1200ms": fresh,
    }


def _refresh_blocker_learning(force=False):
    global _blocker_learning_cache, _blocker_learning_n, _blocker_learning_mono
    if not force and time.monotonic() - _blocker_learning_mono < LEARN_REFRESH_SECONDS:
        return _blocker_learning_cache
    _blocker_learning_mono = time.monotonic()
    out = {}
    total = 0
    try:
        rescue = getattr(getattr(CORE, "legacy", None), "rescue", None)
        if rescue is not None and hasattr(rescue, "_blocker_learning"):
            rows, total = rescue._blocker_learning()
            for w20, w10, mfe, n, blocker in rows:
                n = int(n)
                if n < 3:
                    continue
                shrink = n / (n + 20.0)
                raw = 100.0 * (0.45 * _f(w10) + 0.55 * _f(w20))
                out[str(blocker)] = {
                    "n": n,
                    "win10": round(_f(w10), 4),
                    "win20": round(_f(w20), 4),
                    "avg_mfe": round(_f(mfe), 4),
                    "opportunity_cost": round(raw * shrink, 2),
                }
    except Exception:
        pass
    _blocker_learning_cache = out
    _blocker_learning_n = total
    return out


def _blocker_context_bonus():
    learned = _refresh_blocker_learning()
    vals = []
    for name in ("CUMULATIVE_EXTENSION_GUARD", "ANTI_CHASE", "ANTI_CHASE_OR_RUNNER"):
        if learned.get(name):
            vals.append(_f(learned[name].get("opportunity_cost")))
    return min(15.0, max(vals, default=0.0))


def _enrich_rows(rows):
    structural = _structural_map()
    for row in rows:
        sym = str(row.get("symbol") or "").upper()
        if not sym:
            continue
        _history[sym].append(_snapshot(row))
        row.update(_sequence_metrics(sym, row))
        s = structural.get(sym) or {}
        row["structural_state"] = str(s.get("state") or "")
        row["structural_setup"] = str(s.get("setup") or "")
        row["structural_entry"] = _f(s.get("entry"))
        row["structural_max_chase"] = _f(s.get("max_chase"))
        row["structural_current"] = _f(s.get("current"), _f(row.get("entry_reference")))
        row["blocker_context_bonus"] = round(_blocker_context_bonus(), 2)
    return rows


def _rebuild_wrapper():
    rows = list(_original_rebuild() or [])
    _enrich_rows(rows)
    rows.sort(key=lambda r: (
        bool(r.get("fresh_1200ms")),
        str(r.get("sequence_state")) == "SEQUENCE_PINPOINT",
        str(r.get("sequence_state")) == "SEQUENCE_ARMED",
        _f(r.get("sequence_score")),
        _f(r.get("hazard_score")),
    ), reverse=True)
    V125._latest_candidates = rows
    _stats["sequence_pinpoint"] = sum(r.get("sequence_state") == "SEQUENCE_PINPOINT" for r in rows)
    _stats["sequence_armed"] = sum(r.get("sequence_state") == "SEQUENCE_ARMED" for r in rows)
    _stats["sequence_watch"] = sum(r.get("sequence_state") == "SEQUENCE_WATCH" for r in rows)
    return rows


def sequence_candidates(limit=20):
    rows = list(getattr(V125, "_latest_candidates", []) or [])
    rows = [r for r in rows if r.get("sequence_state") in {"SEQUENCE_PINPOINT", "SEQUENCE_ARMED", "SEQUENCE_WATCH"}]
    rows.sort(key=lambda r: (_f(r.get("sequence_score")), _f(r.get("alignment_score")), _f(r.get("hazard_score"))), reverse=True)
    return rows[:limit]


def _promotion_wrapper():
    try:
        base = list(_original_promotion_symbols() or [])
    except Exception:
        base = []
    seq = [str(r.get("symbol") or "").upper() for r in sequence_candidates(PROMOTION_SLOTS)]
    out = []
    for sym in seq + base:
        if sym and sym not in out:
            out.append(sym)
        if len(out) >= max(PROMOTION_SLOTS, int(getattr(V125, "EARLY_PROMOTION_SLOTS", 16))):
            break
    _stats["sequence_promoted"] = sum(1 for s in seq if s in out)
    return out


def _candidate(symbol):
    sym = str(symbol or "").upper()
    for row in list(getattr(V125, "_latest_candidates", []) or []):
        if str(row.get("symbol") or "").upper() == sym:
            return row
    return {}


def _integrity_fresh_1200(integrity):
    ages = (integrity or {}).get("ages") if isinstance((integrity or {}).get("ages"), dict) else {}
    required = (
        _f(ages.get("micro_trade_ms"), 999999999),
        _f(ages.get("micro_book_ms"), 999999999),
        _f(ages.get("tape_ms"), 999999999),
        _f(ages.get("bbo_ms"), 999999999),
    )
    return max(required) <= FRESH_MS


def _adaptive_extension(structural, legacy, integrity):
    structural = structural or {}
    legacy = legacy or {}
    symbol = str(structural.get("symbol") or legacy.get("symbol") or "").upper()
    row = _candidate(symbol)
    if not row or not _fresh_candidate(row) or not _integrity_fresh_1200(integrity):
        return {"pass": False, "reason": "FRESHNESS"}
    seq = _f(row.get("sequence_score"))
    align = _f(row.get("alignment_score"))
    buy = _f(row.get("buy_ratio"), 0.5)
    ofi = _f(row.get("ofi"))
    obi = _f(row.get("obi"))
    rv = _f(row.get("relative_volume_10s"))
    ta = _f(row.get("trade_acceleration"))
    if not (seq >= SEQ_PINPOINT and align >= 75 and buy >= 0.62 and ofi >= 0.08 and obi >= 0.08 and (rv >= 1.5 or ta >= 2.5)):
        return {"pass": False, "reason": "SEQUENCE_NOT_EXCEPTIONAL", "sequence": seq, "alignment": align}
    try:
        ml = V124.ml_probability(symbol, structural, legacy)
    except Exception:
        ml = {"qualified": False}
    if not ml.get("qualified"):
        return {"pass": False, "reason": "ML_NOT_CALIBRATED", "ml": ml}
    entry = _f(structural.get("entry"))
    current = _f(structural.get("current"), _f(row.get("entry_reference")))
    max_chase = _f(structural.get("max_chase"))
    if entry <= 0 or current <= 0 or max_chase <= entry:
        return {"pass": False, "reason": "NO_EXTENSION_GEOMETRY"}
    base_budget = max_chase - entry
    learned_bonus = _blocker_context_bonus() / 100.0
    quality_bonus = max(0.0, min(0.50, (seq - SEQ_PINPOINT) / 40.0 + (align - 75.0) / 100.0))
    budget_mult = min(EXTENSION_MAX_BUDGET_MULT, 1.0 + quality_bonus + learned_bonus)
    dynamic_max = min(entry * (1.0 + EXTENSION_HARD_CAP_PCT / 100.0), entry + base_budget * budget_mult)
    passed = (not bool(structural.get("counter_trend"))) and current <= dynamic_max
    return {
        "pass": bool(passed),
        "reason": "ADAPTIVE_EXTENSION_OK" if passed else "EXTENSION_STILL_TOO_HIGH",
        "sequence": round(seq, 2),
        "alignment": round(align, 2),
        "entry": entry,
        "current": current,
        "base_max_chase": max_chase,
        "dynamic_max_chase": dynamic_max,
        "budget_mult": round(budget_mult, 3),
        "ml_probability": ml.get("probability"),
        "ml_key": ml.get("calibration_key"),
    }


def _adaptive_hard_safety(structural, legacy, micro, integrity):
    result = dict(_original_hard_safety(structural, legacy, micro, integrity))
    blockers = list(dict.fromkeys(str(x) for x in (result.get("blockers") or [])))
    if not blockers:
        return result
    if [b for b in blockers if b != "CUMULATIVE_EXTENSION_GUARD"]:
        result["v127_contextual_override"] = False
        return result
    decision = _adaptive_extension(structural, legacy, integrity)
    result["v127_extension_decision"] = decision
    if decision.get("pass"):
        result["pass"] = True
        result["blockers"] = []
        result["v127_contextual_override"] = True
        _stats["adaptive_extension_pass"] += 1
    else:
        _stats["adaptive_extension_block"] += 1
    return result


def _wrap_learning_features():
    global _original_features
    if LEARNER is None or _original_features is not None:
        return
    _original_features = LEARNER._features
    def features(symbol, structural, legacy):
        out = dict(_original_features(symbol, structural, legacy) or {})
        row = _candidate(symbol)
        out["v127_sequence_score"] = _f(row.get("sequence_score"))
        out["v127_sequence_state"] = str(row.get("sequence_state") or "")
        out["v127_alignment_score"] = _f(row.get("alignment_score"))
        out["v127_order_score"] = _f(row.get("sequence_order_score"))
        out["v127_momentum_score"] = _f(row.get("momentum_score"))
        out["v127_fresh_1200ms"] = bool(row.get("fresh_1200ms"))
        return out
    LEARNER._features = features


def _augment_response(response):
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    data["v12_7_sequence_intelligence"] = {
        "revision": REVISION,
        "role": ROLE,
        "conventional_buy_authority_unchanged": True,
        "freshness_ms": FRESH_MS,
        "sequence_pinpoint": _stats.get("sequence_pinpoint", 0),
        "sequence_armed": _stats.get("sequence_armed", 0),
        "sequence_watch": _stats.get("sequence_watch", 0),
        "sequence_promoted": _stats.get("sequence_promoted", 0),
        "adaptive_extension_pass": _stats.get("adaptive_extension_pass", 0),
        "adaptive_extension_block": _stats.get("adaptive_extension_block", 0),
        "blocker_learning_resolved": _blocker_learning_n,
        "blocker_learning": _refresh_blocker_learning(),
        "candidates": sequence_candidates(20),
        "hard_never_bypass": sorted(HARD_NEVER_BYPASS),
        "contextual_blockers": sorted(CONTEXTUAL_BLOCKERS),
        "rule": "Sequence intelligence may contextualize extension only on the calibrated ML route with <=1200ms live data; it never bypasses stale/invalid data, spread/slippage, market-regime or risk-plan safety.",
    }
    data["upgrade_revision_v12_7"] = REVISION
    return CORE.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    return _augment_response(await _original_scan(request))


async def _health_wrapper(request):
    return _augment_response(await _original_health(request))


async def supervisor_loop():
    global _last_diag_mono
    while True:
        await asyncio.sleep(1.0)
        mono = time.monotonic()
        if mono - _last_diag_mono < DIAG_SECONDS:
            continue
        _last_diag_mono = mono
        top = sequence_candidates(8)
        print(
            f"Ψ-V12.7 SEQUENCE pinpoint={_stats.get('sequence_pinpoint',0)} "
            f"armed={_stats.get('sequence_armed',0)} watch={_stats.get('sequence_watch',0)} "
            f"promoted={_stats.get('sequence_promoted',0)} adaptivePass={_stats.get('adaptive_extension_pass',0)} "
            f"adaptiveBlock={_stats.get('adaptive_extension_block',0)} blockerN={_blocker_learning_n} "
            f"top={[(r.get('symbol'),round(_f(r.get('sequence_score')),1),r.get('sequence_state')) for r in top]}",
            flush=True,
        )
        learned = _refresh_blocker_learning()
        if learned:
            print(f"Ψ-V12.7 BLOCKER-COST {learned}", flush=True)


def install(core, hardening, learner, v124, v125, v126=None):
    global CORE, HARDENING, LEARNER, V124, V125, V126
    global _original_rebuild, _original_promotion_symbols, _original_hard_safety
    global _original_scan, _original_health
    if CORE is not None:
        return
    CORE = core
    HARDENING = hardening
    LEARNER = learner
    V124 = v124
    V125 = v125
    V126 = v126
    _original_rebuild = v125._rebuild_candidates
    _original_promotion_symbols = v125._promotion_symbols
    _original_hard_safety = v124._hard_execution_safety
    _original_scan = core.v12_scan
    _original_health = core.v12_health
    v125._rebuild_candidates = _rebuild_wrapper
    v125._promotion_symbols = _promotion_wrapper
    v124._hard_execution_safety = _adaptive_hard_safety
    _wrap_learning_features()
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper
    print(
        f"Ψ-V12.7 installed revision={REVISION} freshness={FRESH_MS}ms "
        "sequence=ORDERED_PATH blockerLearning=COUNTERFACTUAL extension=ADAPTIVE_CALIBRATED "
        "hardSafety=FAIL_CLOSED conventionalBuyAuthority=UNCHANGED",
        flush=True,
    )
