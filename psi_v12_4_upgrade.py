import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

import redis.asyncio as redis_async

# V12.4 is an additive runtime layer. The conventional execution path remains
# V12.3.4; this layer adds discovery/promotion, worker-cache rescue and a
# separately-labelled calibrated ML override that can bypass technical BUY
# confirmation but never bypass hard execution/data-safety checks.
REVISION = "12.4.3-full-ma-ema-weekly-hydration+cap-truth+scan-completeness+ml70-safety"
AUTHORITY_CHAIN = "V12.3.4_CONVENTIONAL_OR_V12.4_ML70_HARD_SAFETY->BUY_NOW"
ML_AUTHORITY_CHAIN = "V12.4_ML70->HARD_EXECUTION_SAFETY->BUY_NOW"
ROLE = "PROMOTION_AND_CALIBRATED_OVERRIDE"

SCAN_REPORT_VERSION = "1.1"
SCAN_REQUIRED_SECTIONS = (
    "EXECUTION_AUTHORITY",
    "MA_PRIORITY_50_200",
    "WEEKLY_MA_EMA",
    "HYDRATION_200_COVERAGE",
    "ML_OVERRIDE",
    "PINPOINT",
    "RISKMAP_CONDITIONAL",
    "PRE_IGNITION",
    "PULLBACK_EXHAUSTION",
    "LOWCAP_ROTATION",
    "LOWCAP_CAP_SOURCE",
    "RAPID_ROTATION",
    "MONSTER",
    "STRUCTURAL_SETUPS",
    "DATA_HEALTH",
    "MISSED_MOVER_LEARNING",
)
SCAN_NEVER_OMIT_WHEN_NONEMPTY = (
    "MA_PRIORITY_50_200",
    "WEEKLY_MA_EMA",
    "HYDRATION_200_COVERAGE",
    "ML_OVERRIDE",
    "PINPOINT",
    "RISKMAP_CONDITIONAL",
    "PRE_IGNITION",
    "PULLBACK_EXHAUSTION",
    "LOWCAP_ROTATION",
    "LOWCAP_CAP_SOURCE",
    "RAPID_ROTATION",
    "STRUCTURAL_SETUPS",
)

CORE = None
HARDENING = None
LEARNER = None

MA_TOUCH_PCT = max(0.05, float(os.getenv("PSI_MA_PROMOTE_TOUCH_PCT", "0.50")))
MA_NEAR_PCT = max(MA_TOUCH_PCT, float(os.getenv("PSI_MA_PROMOTE_NEAR_PCT", "1.00")))
MA_APPROACH_ATR = max(0.25, float(os.getenv("PSI_MA_PROMOTE_APPROACH_ATR", "0.90")))
MA_APPROACH_MAX_PCT = max(MA_NEAR_PCT, float(os.getenv("PSI_MA_PROMOTE_MAX_PCT", "3.00")))
MA_PROMOTION_SLOTS = max(4, min(int(os.getenv("PSI_MA_PROMOTION_SLOTS", "16")), 24))
MA_PRIORITY_LEVELS = (
    ("WEEKLY_SMA200", "1w", "sma200", 122.0),
    ("WEEKLY_EMA200", "1w", "ema200", 121.0),
    ("DAILY_SMA200", "1d", "sma200", 120.0),
    ("DAILY_EMA200", "1d", "ema200", 119.0),
    ("4H_SMA200", "4h", "sma200", 114.0),
    ("4H_EMA200", "4h", "ema200", 113.0),
    ("1H_SMA200", "1h", "sma200", 108.0),
    ("1H_EMA200", "1h", "ema200", 107.0),
    ("WEEKLY_SMA50", "1w", "sma50", 104.0),
    ("WEEKLY_EMA50", "1w", "ema50", 103.0),
    ("DAILY_SMA50", "1d", "sma50", 100.0),
    ("DAILY_EMA50", "1d", "ema50", 99.0),
    ("4H_SMA50", "4h", "sma50", 94.0),
    ("4H_EMA50", "4h", "ema50", 93.0),
    ("1H_SMA50", "1h", "sma50", 88.0),
    ("1H_EMA50", "1h", "ema50", 87.0),
)
ML_PROMOTION_SLOTS = max(2, min(int(os.getenv("PSI_ML_PROMOTION_SLOTS", "8")), 20))
EARLY_PROMOTION_SLOTS = max(4, min(int(os.getenv("PSI_EARLY_EXPLOSION_PROMOTION_SLOTS", "12")), 24))
CORE_RETAIN_SLOTS = max(4, min(int(os.getenv("PSI_DYNAMIC_MICRO_CORE_RETAIN", "10")), 24))
EARLY_EXPLOSION_MIN = max(45.0, float(os.getenv("PSI_EARLY_EXPLOSION_MIN", "70")))
ML_OVERRIDE_THRESHOLD = min(0.99, max(0.51, float(os.getenv("PSI_ML_OVERRIDE_THRESHOLD", "0.70"))))
ML_OVERRIDE_TARGET_PCT = float(os.getenv("PSI_ML_OVERRIDE_TARGET_PCT", "10"))
ML_OVERRIDE_REQUIRE_CI70 = os.getenv("PSI_ML_OVERRIDE_REQUIRE_CI70", "1").strip() not in {"0", "false", "False"}
ML_OVERRIDE_MIN_TOTAL = max(30, int(os.getenv("PSI_ML_OVERRIDE_MIN_TOTAL", "300")))
ML_OVERRIDE_MIN_TEST = max(10, int(os.getenv("PSI_ML_OVERRIDE_MIN_TEST", "75")))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_ML_OVERRIDE_MAX_SPREAD_BPS", "20")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_ML_OVERRIDE_MAX_SLIPPAGE_BPS", "35")))
RESCUE_MAX_AGE_S = max(30.0, float(os.getenv("PSI_STRUCTURE_WORKER_RESCUE_MAX_AGE_S", "600")))
WEEKLY_RESCUE_MAX_AGE_S = max(RESCUE_MAX_AGE_S, float(os.getenv("PSI_STRUCTURE_WORKER_WEEKLY_RESCUE_MAX_AGE_S", "1800")))
RESCUE_BATCH_SYMBOLS = max(44, min(int(os.getenv("PSI_STRUCTURE_RESCUE_BATCH_SYMBOLS", "72")), 120))
RESCUE_POLL_S = max(3.0, float(os.getenv("PSI_STRUCTURE_WORKER_RESCUE_POLL_S", "10")))
DIAG_SECONDS = max(10.0, float(os.getenv("PSI_V12_4_DIAG_SECONDS", "30")))
HISTORY_SAMPLE_S = max(30.0, float(os.getenv("PSI_MISSED_HISTORY_SAMPLE_S", "60")))
MISSED_MOVE_MIN_PCT = max(3.0, float(os.getenv("PSI_MISSED_MOVE_MIN_PCT", "5")))
MISSED_COOLDOWN_S = max(900.0, float(os.getenv("PSI_MISSED_MOVE_COOLDOWN_S", "21600")))
MISSED_KEY = os.getenv("PSI_MISSED_TRAINING_KEY", "psi:v12:ml:missed:v2").strip()
MISSED_MAX = max(500, min(int(os.getenv("PSI_MISSED_TRAINING_MAX", "5000")), 20000))
MISSED_LEGACY_SEEN_KEY = f"{MISSED_KEY}:legacy-seen"
STRUCTURE_PREFIX = os.getenv("PSI_STRUCTURE_REDIS_PREFIX", "psi:v12:structure").strip()

_original_gate = None
_original_micro = None
_original_priority = None
_original_scan = None
_original_health = None
_original_features = None
_original_cohort = None
_original_candidates = None
_original_signal_class = None
_original_evaluate = None

_last_worker_fetch_ms = {}
_rescue_cursor = 0
_price_feature_history = defaultdict(lambda: deque(maxlen=70))
_last_missed_label_mono = defaultdict(float)
_last_sample_mono = 0.0
_last_diag_mono = 0.0
_active_ml_overrides = {}
_ml_watch_cache = []
_ma_cache = []
_stats = defaultdict(int)
_last_error = ""
_historical_missed_counts = defaultdict(int)


def _f(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _now_ms():
    return int(time.time() * 1000)


def _core():
    if CORE is None:
        raise RuntimeError("V12.4 upgrade is not installed")
    return CORE


def _universe():
    core = _core()
    return [
        str(s).upper()
        for s in list(getattr(core.q, "universe", []) or [])
        if str(s).upper().endswith("USDT")
    ]


def _current_price(symbol):
    core = _core()
    symbol = str(symbol or "").upper()
    try:
        p = _f(core.app.current_symbol_price(symbol))
        if p > 0:
            return p
    except Exception:
        pass
    for tf in ("1h", "4h", "1d", "1w"):
        snap = ((core._cache.get(symbol) or {}).get(tf) or {}).get("snap") or {}
        p = _f(snap.get("current"), _f(snap.get("close")))
        if p > 0:
            return p
    row = (getattr(core.q, "latest", {}) or {}).get(symbol) or {}
    for key in ("price", "last_price", "current", "mark_price"):
        p = _f(row.get(key))
        if p > 0:
            return p
    return 0.0


def _ma_matches_from_cache(symbol, cache=None):
    """Return SMA/EMA 50/200 proximity across 1H, 4H, Daily and Weekly.

    Proximity is discovery/promotion only. It never creates a BUY by itself;
    conventional V12 confirmation and Pinpoint remain authoritative.
    """
    core = _core()
    symbol = str(symbol or "").upper()
    cache = cache if isinstance(cache, dict) else (core._cache.get(symbol) or {})
    current = _current_price(symbol)
    if current <= 0:
        return []

    out = []
    for label, tf, key, base_score in MA_PRIORITY_LEVELS:
        snap = (cache.get(tf) or {}).get("snap") or {}
        level = _f(snap.get(key))
        atr = _f(snap.get("atr"))
        if level <= 0:
            continue
        dist_pct = abs(current - level) / level * 100.0
        atr_pct = (atr / current * 100.0) if atr > 0 and current > 0 else MA_NEAR_PCT
        approach_pct = min(MA_APPROACH_MAX_PCT, max(MA_NEAR_PCT, MA_APPROACH_ATR * atr_pct))
        if dist_pct <= MA_TOUCH_PCT:
            proximity = "TOUCH"
            bonus = 20.0
        elif dist_pct <= MA_NEAR_PCT:
            proximity = "NEAR"
            bonus = 12.0
        elif dist_pct <= approach_pct:
            proximity = "APPROACHING"
            bonus = 5.0
        else:
            continue
        side = "ABOVE" if current >= level else "BELOW"
        out.append({
            "symbol": symbol,
            "label": label,
            "timeframe": tf,
            "ma": key.upper(),
            "family": "EMA" if key.startswith("ema") else "SMA",
            "period": 200 if key.endswith("200") else 50,
            "level": level,
            "price": current,
            "distance_pct": round(dist_pct, 4),
            "approach_pct": round(approach_pct, 4),
            "proximity": proximity,
            "side": side,
            "score": round(base_score + bonus - min(dist_pct * 2.0, 8.0), 3),
            "role": "PRIORITY_PROMOTION_ONLY",
            "automatic_buy": False,
        })
    out.sort(key=lambda r: (r["score"], -r["distance_pct"]), reverse=True)
    return out


def _ma_signal(symbol):
    matches = _ma_matches_from_cache(symbol)
    if not matches:
        return {}
    best = dict(matches[0])
    best["matches"] = matches
    return best


def ma_priority_candidates(limit=None):
    global _ma_cache
    rows = []
    for symbol in _universe():
        sig = _ma_signal(symbol)
        if sig:
            rows.append(sig)
    rows.sort(key=lambda r: (r.get("score", 0.0), -r.get("distance_pct", 999.0)), reverse=True)
    _ma_cache = rows
    return rows[:limit] if limit else rows


def _hydration_summary():
    core = _core()
    universe = _universe()
    deep_min = max(201, int(getattr(core, "DEEP_MIN_ROWS", 202)))
    core_ready = 0
    deep_200_ready = 0
    weekly_ready = 0
    weekly_deep_ready = 0
    for symbol in universe:
        cache = core._cache.get(symbol) or {}
        core_snaps = [(cache.get(tf) or {}).get("snap") or {} for tf in ("1h", "4h", "1d")]
        if all(core_snaps):
            core_ready += 1
        deep_ok = True
        for tf in ("1h", "4h", "1d"):
            node = cache.get(tf) or {}
            rows = node.get("rows") or []
            if len(rows) < deep_min and not bool(node.get("history_capped")):
                deep_ok = False
                break
        if deep_ok:
            deep_200_ready += 1
        weekly = cache.get("1w") or {}
        if weekly.get("snap"):
            weekly_ready += 1
        weekly_rows = weekly.get("rows") or []
        if len(weekly_rows) >= deep_min or bool(weekly.get("history_capped")):
            weekly_deep_ready += 1
    return {
        "universe": len(universe),
        "core_mtf_ready": core_ready,
        "deep_200_ready": deep_200_ready,
        "weekly_ready": weekly_ready,
        "weekly_deep_ready": weekly_deep_ready,
        "deep_min_rows": deep_min,
    }


def _lowcap_truth_summary():
    if HARDENING is None or not hasattr(HARDENING, "_lowcap_summary"):
        return {"available": False, "verified_market_cap": 0, "quote_volume_proxy": 0, "top": []}
    try:
        summary = HARDENING._lowcap_summary(20) or {}
    except Exception:
        return {"available": False, "verified_market_cap": 0, "quote_volume_proxy": 0, "top": []}
    rows = list(summary.get("top") or [])
    return {
        "available": True,
        "verified_market_cap": sum(1 for r in rows if r.get("cap_source") == "MARKET_CAP"),
        "quote_volume_proxy": sum(1 for r in rows if r.get("cap_source") == "QUOTE_VOLUME_PROXY"),
        "top": rows,
        "rule": "Never describe QUOTE_VOLUME_PROXY as verified market cap.",
    }


def scan_report_contract():
    """Machine-readable contract for every user-facing Scan result.

    The contract makes omission detectable: consumers should surface every section,
    explicitly showing NONE/UNAVAILABLE rather than silently dropping a lane.
    """
    ma_rows = ma_priority_candidates(20)
    ml_rows = ml_watch_candidates(15)
    qualified_ml = [r for r in ml_rows if r.get("qualified")]
    by_level = defaultdict(int)
    by_proximity = defaultdict(int)
    by_family = defaultdict(int)
    by_timeframe = defaultdict(int)
    all_matches = []
    for row in _ma_cache:
        matches = list(row.get("matches") or [row])
        all_matches.extend(matches)
        for match in matches:
            by_level[str(match.get("label") or "UNKNOWN")] += 1
            by_proximity[str(match.get("proximity") or "UNKNOWN")] += 1
            by_family[str(match.get("family") or "UNKNOWN")] += 1
            by_timeframe[str(match.get("timeframe") or "UNKNOWN")] += 1
    all_matches.sort(key=lambda r: (r.get("score", 0.0), -r.get("distance_pct", 999.0)), reverse=True)
    hydration = _hydration_summary()
    lowcap_truth = _lowcap_truth_summary()
    return {
        "version": SCAN_REPORT_VERSION,
        "required_sections": list(SCAN_REQUIRED_SECTIONS),
        "never_omit_when_nonempty": list(SCAN_NEVER_OMIT_WHEN_NONEMPTY),
        "section_sources": {
            "EXECUTION_AUTHORITY": ["Ψ-V12 SIGNAL BOARD", "Ψ-PINPOINT BOARD"],
            "MA_PRIORITY_50_200": ["Ψ-V12.4 UPGRADE", "ma_priority_rule"],
            "WEEKLY_MA_EMA": ["Ψ-V12.4 UPGRADE", "ma_priority_rule", "Ψ-V12 SIGNAL BOARD"],
            "HYDRATION_200_COVERAGE": ["Ψ-V12.4 UPGRADE", "Ψ-V12 REFRESH", "structure_rescue_v12_4"],
            "ML_OVERRIDE": ["Ψ-V12.4 UPGRADE", "ml_override"],
            "PINPOINT": ["Ψ-PINPOINT BOARD"],
            "RISKMAP_CONDITIONAL": ["Ψ-V10.19.9 RISKMAP"],
            "PRE_IGNITION": ["PRE", "EARLY STATES"],
            "PULLBACK_EXHAUSTION": ["PULLBACK BOARD", "PX"],
            "LOWCAP_ROTATION": ["LOWCAP PROMOTION"],
            "LOWCAP_CAP_SOURCE": ["LOWCAP PROMOTION", "low_cap_early_explosion"],
            "RAPID_ROTATION": ["RAPID PROMOTION", "RAPID promoted="],
            "MONSTER": ["MONSTER-CANDIDATES", "MR"],
            "STRUCTURAL_SETUPS": ["structural=BUY", "EX"],
            "DATA_HEALTH": ["WATCHDOG", "MICRO-READINESS", "V12 SIGNAL BOARD"],
            "MISSED_MOVER_LEARNING": ["MISSED-MOVER-TRAINING", "MISSED-EXPERIENCE"],
        },
        "ma_priority_summary": {
            "symbol_count": len(_ma_cache),
            "match_count": len(all_matches),
            "by_level": dict(by_level),
            "by_proximity": dict(by_proximity),
            "by_family": dict(by_family),
            "by_timeframe": dict(by_timeframe),
            "top": ma_rows,
            "top_matches": all_matches[:40],
            "automatic_buy": False,
        },
        "weekly_ma_ema_summary": {
            "ready": hydration.get("weekly_ready", 0),
            "deep_ready": hydration.get("weekly_deep_ready", 0),
            "universe": hydration.get("universe", 0),
            "top_matches": [r for r in all_matches if r.get("timeframe") == "1w"][:20],
        },
        "hydration_200_coverage": hydration,
        "lowcap_cap_truth": lowcap_truth,
        "ml_summary": {
            "watch_count": len(_ml_watch_cache),
            "qualified_70_count": len(qualified_ml),
            "threshold": ML_OVERRIDE_THRESHOLD,
        },
        "runtime_summary": {
            "dynamic_micro_pool": _stats.get("dynamic_micro_pool", 0),
            "structure_rescue_imported": _stats.get("structure_rescue_imported", 0),
            "structure_rescue_attempted": _stats.get("structure_rescue_attempted", 0),
            "structure_rescue_imported_1h": _stats.get("structure_rescue_imported_1h", 0),
            "structure_rescue_imported_4h": _stats.get("structure_rescue_imported_4h", 0),
            "structure_rescue_imported_1d": _stats.get("structure_rescue_imported_1d", 0),
            "structure_rescue_imported_1w": _stats.get("structure_rescue_imported_1w", 0),
            "missed_labelled": _stats.get("missed_labelled", 0),
            "hydration": hydration,
        },
        "reporting_rule": "Every Scan must show every required section or explicitly mark it NONE/UNAVAILABLE; MA proximity is never silently omitted.",
    }


def _rapid_score(symbol):
    core = _core()
    row = (getattr(core.q, "latest", {}) or {}).get(symbol) or {}
    rapid = row.get("rapid_ignition") or {}
    return max(_f(rapid.get("score")), _f(row.get("rapid_score")))


def _lowcap_score(symbol):
    if HARDENING is None:
        return 0.0
    try:
        return _f((HARDENING._lowcap_signal(symbol) or {}).get("score"))
    except Exception:
        return 0.0


def _early_explosion_score(symbol):
    core = _core()
    row = (getattr(core.q, "latest", {}) or {}).get(symbol) or {}
    rapid = _rapid_score(symbol)
    lowcap = _lowcap_score(symbol)
    v = max(
        _f(row.get("v1014_score")),
        _f(row.get("ignition15_score")),
        _f(row.get("score")),
    )
    velocity = max(0.0, _f(row.get("ignition_velocity_per_min")))
    accel = max(0.0, _f(row.get("ignition_acceleration_per_min2")))
    return max(rapid, lowcap, v) + min(12.0, velocity * 0.04) + min(8.0, accel * 0.015)


def early_explosion_candidates(limit=None):
    rows = []
    for symbol in _universe():
        score = _early_explosion_score(symbol)
        if score >= EARLY_EXPLOSION_MIN:
            rows.append({"symbol": symbol, "score": round(score, 3)})
    rows.sort(key=lambda r: r["score"], reverse=True)
    return rows[:limit] if limit else rows


def _structural_map():
    core = _core()
    try:
        board = list(core._board() or [])
    except Exception:
        board = []
    return {str(r.get("symbol") or "").upper(): r for r in board if r.get("symbol")}


def _metric_for_key(key, target):
    learner = LEARNER
    if learner is None:
        return None
    name = f"plus_{int(target)}_before_stop"
    overall = learner._calibration(key)
    test = learner._calibration(f"SPLIT::TEST::{key}")
    om = ((overall.get("targets") or {}).get(name) or {}).get("clean_entry") or {}
    tm = ((test.get("targets") or {}).get(name) or {}).get("clean_entry") or {}
    return {
        "key": key,
        "overall": om,
        "test": tm,
        "overall_samples": int(om.get("samples") or 0),
        "test_samples": int(tm.get("samples") or 0),
        "overall_probability": _f(om.get("probability")),
        "test_probability": _f(tm.get("probability")),
        "overall_ci_low": _f((om.get("ci95") or [0.0])[0]),
        "test_ci_low": _f((tm.get("ci95") or [0.0])[0]),
    }


def ml_probability(symbol, structural=None, legacy=None):
    """Conservative calibrated experience probability for the current pattern."""
    if LEARNER is None:
        return {"symbol": str(symbol or "").upper(), "qualified": False, "reason": "LEARNER_UNAVAILABLE"}
    core = _core()
    symbol = str(symbol or "").upper()
    smap = _structural_map() if structural is None else None
    structural = (smap or {}).get(symbol, {}) if structural is None else (structural or {})
    legacy = ((getattr(core.q, "latest", {}) or {}).get(symbol) or {}) if legacy is None else (legacy or {})
    try:
        features = LEARNER._features(symbol, structural, legacy)
        signal_class = LEARNER._signal_class(structural, legacy, features)
        if not signal_class:
            if _ma_signal(symbol):
                signal_class = "MA_PRIORITY"
            elif _early_explosion_score(symbol) >= EARLY_EXPLOSION_MIN:
                signal_class = "EARLY_EXPLOSION"
            else:
                return {"symbol": symbol, "qualified": False, "reason": "NO_LEARNED_SIGNAL_CLASS"}
        cohort = LEARNER._cohort(features, signal_class)
    except Exception as exc:
        return {"symbol": symbol, "qualified": False, "reason": f"FEATURE_ERROR:{type(exc).__name__}"}

    setup = str(features.get("setup") or "UNKNOWN")
    keys = [f"COHORT::{cohort}", f"CLASS::{signal_class}"]
    if setup and setup != "UNKNOWN":
        keys.append(f"SETUP::{setup}")
    candidates = []
    for key in keys:
        metric = _metric_for_key(key, ML_OVERRIDE_TARGET_PCT)
        if metric:
            candidates.append(metric)
    if not candidates:
        return {"symbol": symbol, "qualified": False, "reason": "NO_CALIBRATION"}

    def metric_status(m):
        p = min(m["overall_probability"], m["test_probability"])
        sample_ok = m["overall_samples"] >= ML_OVERRIDE_MIN_TOTAL and m["test_samples"] >= ML_OVERRIDE_MIN_TEST
        prob_ok = p > ML_OVERRIDE_THRESHOLD
        ci_ok = (
            m["overall_ci_low"] >= ML_OVERRIDE_THRESHOLD
            and m["test_ci_low"] >= ML_OVERRIDE_THRESHOLD
        ) if ML_OVERRIDE_REQUIRE_CI70 else True
        return bool(sample_ok and prob_ok and ci_ok), p, sample_ok, prob_ok, ci_ok

    # Prefer a properly validated cohort/class/setup over a superficially higher
    # probability bucket with too few observations.
    candidates.sort(
        key=lambda m: (
            metric_status(m)[0],
            metric_status(m)[1],
            min(m["overall_ci_low"], m["test_ci_low"]),
            m["overall_samples"] + m["test_samples"],
        ),
        reverse=True,
    )
    best = candidates[0]
    qualified, probability, sample_ok, prob_ok, ci_ok = metric_status(best)
    return {
        "symbol": symbol,
        "qualified": qualified,
        "probability": round(probability, 4),
        "target_pct": ML_OVERRIDE_TARGET_PCT,
        "horizon": "24h_before_invalidation",
        "threshold": ML_OVERRIDE_THRESHOLD,
        "calibration_key": best["key"],
        "overall_samples": best["overall_samples"],
        "test_samples": best["test_samples"],
        "overall_probability": round(best["overall_probability"], 4),
        "test_probability": round(best["test_probability"], 4),
        "overall_ci_low": round(best["overall_ci_low"], 4),
        "test_ci_low": round(best["test_ci_low"], 4),
        "signal_class": signal_class,
        "setup": setup,
        "reason": "QUALIFIED" if qualified else (
            "INSUFFICIENT_SAMPLES" if not sample_ok else "PROBABILITY_BELOW_THRESHOLD" if not prob_ok else "CI_NOT_VALIDATED"
        ),
    }


def ml_watch_candidates(limit=None):
    global _ml_watch_cache
    core = _core()
    structural = _structural_map()
    seed = []
    seen = set()

    def add(sym):
        sym = str(sym or "").upper()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            seed.append(sym)

    for row in ma_priority_candidates(MA_PROMOTION_SLOTS * 2):
        add(row.get("symbol"))
    for row in early_explosion_candidates(40):
        add(row.get("symbol"))
    for sym in structural:
        add(sym)
    for sym in list(getattr(core.app, "selected_micro_symbols", []) or []):
        add(sym)

    rows = []
    latest = getattr(core.q, "latest", {}) or {}
    for symbol in seed[:100]:
        d = ml_probability(symbol, structural.get(symbol) or {}, latest.get(symbol) or {})
        if _f(d.get("probability")) >= 0.55 or d.get("qualified"):
            rows.append(d)
    rows.sort(key=lambda d: (_f(d.get("probability")), int(d.get("overall_samples") or 0)), reverse=True)
    _ml_watch_cache = rows
    return rows[:limit] if limit else rows


def _risk_plan(structural, legacy):
    structural = structural or {}
    legacy = legacy or {}
    entry = _f(legacy.get("pinpoint_trigger"))
    if entry <= 0:
        entry = _f(structural.get("entry"), _f(structural.get("current")))
    stop = _f(legacy.get("pinpoint_stop"))
    if stop <= 0 or stop >= entry:
        stop = _f(structural.get("stop"), _f(structural.get("invalidation")))
    if entry <= 0 or stop <= 0 or stop >= entry:
        return {"valid": False, "entry": entry, "stop": stop, "risk_pct": 0.0}
    risk_pct = (entry - stop) / entry * 100.0
    return {"valid": risk_pct > 0, "entry": entry, "stop": stop, "risk_pct": risk_pct}


def _hard_execution_safety(structural, legacy, micro, integrity):
    core = _core()
    structural = structural or {}
    legacy = legacy or {}
    micro = micro or {}
    integrity = integrity or {}
    blockers = []

    symbol = str(structural.get("symbol") or legacy.get("symbol") or "").upper()
    current = _f(structural.get("current"), _current_price(symbol))
    if current <= 0:
        blockers.append("LIVE_PRICE")
    if symbol not in set(_universe()):
        blockers.append("BINANCE_SPOT_UNIVERSE")
    if not bool(micro.get("micro_ready")):
        blockers.append("LIVE_MICRO_DATA")
    if not bool(micro.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE_VALID")
    if not bool(micro.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE_VALID")

    spread = _f(micro.get("spread_bps"), 999999.0)
    slip = _f(micro.get("slippage_bps"), 999999.0)
    if spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD_FILTER")
    if slip > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE_FILTER")

    legacy_blockers = set(str(x) for x in (
        list(legacy.get("combined_blockers") or [])
        + list(legacy.get("pinpoint_blockers") or [])
        + list(legacy.get("integrity_blockers") or [])
    ))
    if (
        bool(structural.get("anti_chase"))
        or legacy.get("pinpoint_anti_chase_ok") is False
        or "ANTI_CHASE" in legacy_blockers
        or "ANTI_CHASE_CLEAR" in legacy_blockers
        or "CUMULATIVE_EXTENSION_GUARD" in legacy_blockers
    ):
        blockers.append("CUMULATIVE_EXTENSION_GUARD")
    max_chase = _f(structural.get("max_chase"))
    if current > 0 and max_chase > 0 and current > max_chase:
        blockers.append("CUMULATIVE_EXTENSION_GUARD")

    hard = legacy.get("pinpoint_hard_status") or {}
    if "MARKET_REGIME_SAFETY" in hard and not bool(hard.get("MARKET_REGIME_SAFETY")):
        blockers.append("MARKET_REGIME_SAFETY")
    if "market_regime_safety" in hard and not bool(hard.get("market_regime_safety")):
        blockers.append("MARKET_REGIME_SAFETY")

    ages = integrity.get("ages") if isinstance(integrity.get("ages"), dict) else {}
    trade_age = _f(ages.get("micro_trade_ms"), 999999999.0)
    book_age = _f(ages.get("micro_book_ms"), 999999999.0)
    tape_age = _f(ages.get("tape_ms"), 999999999.0)
    bbo_age = _f(ages.get("bbo_ms"), 999999999.0)
    structure_age = _f(ages.get("structure_s"), 999999999.0)
    legacy_mod = getattr(core, "legacy", None)
    if trade_age > _f(getattr(legacy_mod, "INTEGRITY_MICRO_TRADE_MAX_AGE_MS", 15000), 15000):
        blockers.append("STALE_DEPTH_TRADE")
    if book_age > _f(getattr(legacy_mod, "INTEGRITY_MICRO_BOOK_MAX_AGE_MS", 5000), 5000):
        blockers.append("STALE_DEPTH_BOOK")
    if tape_age > _f(getattr(legacy_mod, "INTEGRITY_TAPE_MAX_AGE_MS", 5000), 5000):
        blockers.append("STALE_EVENT_TAPE")
    if bbo_age > _f(getattr(legacy_mod, "INTEGRITY_BBO_MAX_AGE_MS", 5000), 5000):
        blockers.append("STALE_EVENT_BBO")
    if structure_age > _f(getattr(legacy_mod, "INTEGRITY_STRUCTURE_MAX_AGE_S", 1200), 1200):
        blockers.append("STALE_STRUCTURE")

    risk = _risk_plan(structural, legacy)
    if not risk["valid"]:
        blockers.append("VALID_RISK_PLAN")

    blockers = list(dict.fromkeys(blockers))
    return {"pass": not blockers, "blockers": blockers, "risk": risk, "spread_bps": spread, "slippage_bps": slip}


def _gate_wrapper(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    global _active_ml_overrides
    result = dict(_original_gate(structural_row, legacy_row, micro_metrics, integrity))
    structural_row = structural_row if isinstance(structural_row, dict) else {}
    legacy_row = legacy_row if isinstance(legacy_row, dict) else {}
    symbol = str(structural_row.get("symbol") or legacy_row.get("symbol") or "").upper()

    if result.get("buy_now"):
        result["authority_chain"] = AUTHORITY_CHAIN
        if symbol:
            _active_ml_overrides.pop(symbol, None)
        return result

    decision = ml_probability(symbol, structural_row, legacy_row) if symbol else {"qualified": False}
    if not decision.get("qualified"):
        if symbol:
            _active_ml_overrides.pop(symbol, None)
        result["authority_chain"] = AUTHORITY_CHAIN
        return result

    safety = _hard_execution_safety(structural_row, legacy_row, micro_metrics, integrity)
    if not safety["pass"]:
        result["authority_chain"] = AUTHORITY_CHAIN
        result["ml_override_candidate"] = True
        result["ml_override_probability"] = decision.get("probability")
        result["ml_override_safety_blockers"] = safety["blockers"]
        if symbol:
            _active_ml_overrides.pop(symbol, None)
        return result

    result.update({
        "buy_now": True,
        "execution_state": "BUY NOW",
        "blockers": [],
        "authority_chain": ML_AUTHORITY_CHAIN,
        "pinpoint_entry_status": "ML_OVERRIDE_TRIGGERED",
        "pinpoint_state": "ML OVERRIDE BUY",
        "ml_override_buy": True,
        "ml_override_probability": decision.get("probability"),
        "ml_override_target_pct": decision.get("target_pct"),
        "ml_override_calibration_key": decision.get("calibration_key"),
    })
    if symbol:
        _active_ml_overrides[symbol] = {
            **decision,
            "risk": safety["risk"],
            "activated_ms": _now_ms(),
        }
    return result


def _combined_promotions():
    ma = ma_priority_candidates(MA_PROMOTION_SLOTS)
    ml = ml_watch_candidates(ML_PROMOTION_SLOTS)
    early = early_explosion_candidates(EARLY_PROMOTION_SLOTS)
    out = []
    seen = set()
    for source, rows in (("ML", ml), ("MA", ma), ("EARLY", early)):
        for row in rows:
            sym = str(row.get("symbol") or "").upper()
            if sym and sym not in seen:
                seen.add(sym)
                out.append((sym, source, row))
    return out


def promoted_micro_symbols():
    core = _core()
    base = list(_original_micro() or [])
    pool_size = int(core.REDIS_MICRO_POOL_SIZE)
    universe = _universe()
    universe_set = set(universe)

    # Cold-start invariant: never erase the restored/sticky execution pool
    # while the Binance universe is still being populated. The legacy
    # hardening layer can restore a valid pool before q.universe reaches 403;
    # filtering that pool through a partial universe would shrink coverage to
    # zero/few symbols and delay all subsequent high-resolution promotion.
    warm_floor = min(40, pool_size)
    if len(universe) < warm_floor:
        fallback = []
        seen = set()
        for sym in (
            base
            + list(getattr(core, "_distributed_micro_sticky_pool", []) or [])
            + list(getattr(HARDENING, "_protected_pool", []) or [])
        ):
            sym = str(sym or "").upper()
            if sym.endswith("USDT") and sym not in seen:
                seen.add(sym)
                fallback.append(sym)
            if len(fallback) >= pool_size:
                break
        if fallback:
            core._distributed_micro_sticky_pool = list(fallback)
            if HARDENING is not None:
                try:
                    HARDENING._protected_pool[:] = list(fallback)
                except Exception:
                    pass
            _stats["dynamic_micro_coldstart_hold"] += 1
            _stats["dynamic_micro_pool"] = len(fallback)
            return fallback

    promoted = _combined_promotions()

    out = []
    seen = set()
    def add(sym):
        sym = str(sym or "").upper()
        if sym in universe_set and sym not in seen and len(out) < pool_size:
            seen.add(sym)
            out.append(sym)

    for sym in base[:min(CORE_RETAIN_SLOTS, pool_size)]:
        add(sym)
    for sym, _, _ in promoted:
        add(sym)
    for sym in base:
        add(sym)
    for sym in universe:
        add(sym)

    if HARDENING is not None:
        try:
            HARDENING._protected_pool[:] = list(out)
        except Exception:
            pass
    core._distributed_micro_sticky_pool = list(out)
    _stats["dynamic_micro_pool"] = len(out)
    _stats["ma_promoted"] = sum(1 for _, src, _ in promoted if src == "MA")
    _stats["ml_promoted"] = sum(1 for _, src, _ in promoted if src == "ML")
    _stats["early_promoted"] = sum(1 for _, src, _ in promoted if src == "EARLY")
    return out


def priority_symbols(universe):
    core = _core()
    base = list(_original_priority(universe) or [])
    universe_set = set(universe)
    promoted = [sym for sym, _, _ in _combined_promotions() if sym in universe_set]
    limit = int(getattr(core, "ACTIVE_SYMBOLS_PER_CYCLE", 8))
    promo_cap = max(1, min(limit // 2 + 1, len(promoted)))
    out = []
    for sym in promoted[:promo_cap] + base + promoted[promo_cap:]:
        if sym in universe_set and sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    _stats["priority_hydration_promoted"] = sum(sym in out for sym in promoted)
    return out


def _wrap_learning():
    global _original_features, _original_cohort, _original_candidates, _original_signal_class
    if LEARNER is None or _original_features is not None:
        return
    _original_features = LEARNER._features
    _original_cohort = LEARNER._cohort
    _original_candidates = LEARNER._candidate_rows
    _original_signal_class = LEARNER._signal_class

    def features(symbol, structural, legacy):
        out = dict(_original_features(symbol, structural, legacy) or {})
        ma = _ma_signal(symbol)
        out["ma_priority"] = bool(ma)
        out["ma_priority_label"] = str(ma.get("label") or "")
        out["ma_priority_proximity"] = str(ma.get("proximity") or "")
        out["ma_priority_distance_pct"] = _f(ma.get("distance_pct"), 999.0)
        out["early_explosion_score"] = _early_explosion_score(symbol)
        out["historical_missed_count"] = int(_historical_missed_counts.get(str(symbol).upper(), 0))
        return out

    def signal_class(structural, legacy, features_row):
        state = _original_signal_class(structural, legacy, features_row)
        if state:
            return state
        if features_row.get("ma_priority"):
            return "MA_PRIORITY"
        if _f(features_row.get("early_explosion_score")) >= EARLY_EXPLOSION_MIN:
            return "EARLY_EXPLOSION"
        return ""

    def cohort(features_row, signal_class):
        base = _original_cohort(features_row, signal_class)
        ma = "MA1" if features_row.get("ma_priority") else "MA0"
        ex = _f(features_row.get("early_explosion_score"))
        band = "EX3" if ex >= 100 else "EX2" if ex >= 85 else "EX1" if ex >= EARLY_EXPLOSION_MIN else "EX0"
        missed = "MISS1" if int(features_row.get("historical_missed_count") or 0) > 0 else "MISS0"
        return f"{base}|{ma}|{band}|{missed}"

    def candidate_rows():
        rows = list(_original_candidates() or [])
        seen = {str(r[0]).upper() for r in rows if r}
        smap = _structural_map()
        latest = getattr(_core().q, "latest", {}) or {}
        extra = []
        for sym, _, _ in _combined_promotions():
            if sym not in seen:
                seen.add(sym)
                extra.append((sym, smap.get(sym) or {}, latest.get(sym) or {}))
            if len(extra) >= 40:
                break
        return rows + extra

    LEARNER._features = features
    LEARNER._signal_class = signal_class
    LEARNER._cohort = cohort
    LEARNER._candidate_rows = candidate_rows


def _ml_synthetic_row(symbol):
    core = _core()
    symbol = str(symbol or "").upper()
    legacy = (getattr(core.q, "latest", {}) or {}).get(symbol) or {}
    decision = ml_probability(symbol, {}, legacy)
    if not decision.get("qualified"):
        return None
    cache = core._cache.get(symbol) or {}
    s1 = (cache.get("1h") or {}).get("snap") or {}
    s4 = (cache.get("4h") or {}).get("snap") or {}
    sd = (cache.get("1d") or {}).get("snap") or {}
    current = _current_price(symbol)
    if current <= 0 or not (s1 and s4 and sd):
        return None

    supports = []
    for snap in (s1, s4, sd):
        for key in ("sma50", "sma200", "ema50", "ema200", "sup20", "sup60"):
            level = _f(snap.get(key))
            if 0 < level < current:
                supports.append(level)
    if not supports:
        return None
    support = max(supports)
    atr = max(_f(s4.get("atr")), _f(s1.get("atr")), current * 0.005)
    stop = support - 0.35 * atr
    if stop <= 0 or stop >= current:
        return None
    risk_pct = (current - stop) / current * 100.0
    ma = _ma_signal(symbol)
    return {
        "symbol": symbol,
        "state": "WATCH",
        "emoji": "🟡",
        "setup": "ML_EXPERIENCE_OVERRIDE_CANDIDATE",
        "setup_strength": round(100.0 * _f(decision.get("probability")), 1),
        "reason": (
            f"Validated learned pattern P(+{int(ML_OVERRIDE_TARGET_PCT)}%/24h before invalidation)="
            f"{100*_f(decision.get('probability')):.1f}%; waiting only for hard execution safety"
        ),
        "timeframe": "ML/MTF",
        "current": current,
        "entry_low": current,
        "entry_high": current,
        "entry": current,
        "max_chase": current * 1.01,
        "invalidation": stop,
        "stop": stop,
        "risk_pct": round(risk_pct, 3),
        "tp1": current * 1.03,
        "tp1_gain_pct": 3.0,
        "tp2": current * 1.05,
        "tp2_gain_pct": 5.0,
        "tp3": current * (1.0 + ML_OVERRIDE_TARGET_PCT / 100.0),
        "tp3_gain_pct": ML_OVERRIDE_TARGET_PCT,
        "extended": current * 1.20,
        "extended_gain_pct": 20.0,
        "target_sources": ["ML_CALIBRATION", "ML_CALIBRATION", "ML_OVERRIDE_TARGET", "RUNNER"],
        "anti_chase": False,
        "trend_regime": "ML_OVERRIDE_CANDIDATE",
        "counter_trend": False,
        "buy_setup_count": 0,
        "armed_setup_count": 0,
        "active_setups": [],
        "micro_required_by_best": True,
        "micro_fresh": False,
        "micro_positive": False,
        "quote_volume_24h": _f((core.app.symbol_meta.get(symbol, {}) or {}).get("quote_volume_24h")),
        "generated_ms": _now_ms(),
        "ml_probability": decision,
        "ma_priority": ma,
    }


def evaluate_symbol(symbol):
    row = _original_evaluate(symbol)
    if row is not None:
        return row
    return _ml_synthetic_row(symbol)


def _augment_response(response):
    core = _core()
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    data["upgrade_revision"] = REVISION
    data["execution_authority"] = "V12.3.4_CONVENTIONAL_PLUS_V12.4_ML70"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["ma_priority_rule"] = {
        "automatic_buy": False,
        "levels": [row[0] for row in MA_PRIORITY_LEVELS],
        "touch_pct": MA_TOUCH_PCT,
        "near_pct": MA_NEAR_PCT,
        "approach_atr": MA_APPROACH_ATR,
        "candidates": ma_priority_candidates(20),
    }
    data["ml_override"] = {
        "threshold": ML_OVERRIDE_THRESHOLD,
        "target_pct": ML_OVERRIDE_TARGET_PCT,
        "hard_safety_required": True,
        "active": list(_active_ml_overrides.values())[:10],
        "watch": ml_watch_candidates(15),
    }
    data["dynamic_promotion"] = {
        "pool_size": _stats.get("dynamic_micro_pool", 0),
        "ma_promoted": _stats.get("ma_promoted", 0),
        "ml_promoted": _stats.get("ml_promoted", 0),
        "early_promoted": _stats.get("early_promoted", 0),
        "priority_hydration_promoted": _stats.get("priority_hydration_promoted", 0),
    }
    data["structure_rescue_v12_4"] = {
        "attempted": _stats.get("structure_rescue_attempted", 0),
        "imported": _stats.get("structure_rescue_imported", 0),
        "imported_by_tf": {
            "1h": _stats.get("structure_rescue_imported_1h", 0),
            "4h": _stats.get("structure_rescue_imported_4h", 0),
            "1d": _stats.get("structure_rescue_imported_1d", 0),
            "1w": _stats.get("structure_rescue_imported_1w", 0),
        },
        "stale": _stats.get("structure_rescue_stale", 0),
        "invalid": _stats.get("structure_rescue_invalid", 0),
        "hydration": _hydration_summary(),
    }
    data["missed_mover_training"] = {
        "labelled": _stats.get("missed_labelled", 0),
        "legacy_imported": _stats.get("legacy_missed_imported", 0),
        "historical_symbols": len(_historical_missed_counts),
        "history_symbols": len(_price_feature_history),
        "redis_key": MISSED_KEY,
        "feeds_learning_features": True,
    }
    data["scan_report_contract"] = scan_report_contract()
    data["upgrade_last_error"] = _last_error or None
    return core.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    response = _augment_response(await _original_scan(request))
    try:
        payload = json.loads(response.body.decode("utf-8"))
        contract = payload.get("scan_report_contract") or {}
        ma_summary = contract.get("ma_priority_summary") or {}
        top_ma = (ma_summary.get("top") or [])[:5]
        print(
            "Ψ-V12.4 SCAN-COMPLETENESS "
            f"v={SCAN_REPORT_VERSION} required={len(SCAN_REQUIRED_SECTIONS)} "
            f"maPriority={ma_summary.get('symbol_count',0)} maMatches={ma_summary.get('match_count',0)} "
            f"topMA={','.join(str(r.get('symbol'))+':'+str(r.get('label'))+'/'+str(r.get('proximity')) for r in top_ma) or '-'} "
            f"ml70={(contract.get('ml_summary') or {}).get('qualified_70_count',0)} "
            f"micro={(contract.get('runtime_summary') or {}).get('dynamic_micro_pool',0)} "
            "rule=SHOW_ALL_SECTIONS_OR_EXPLICIT_NONE",
            flush=True,
        )
    except Exception:
        pass
    return response


async def _health_wrapper(request):
    return _augment_response(await _original_health(request))


def _snapshot_feature(symbol, micro_pool):
    core = _core()
    price = _current_price(symbol)
    if price <= 0:
        return None
    ma = _ma_signal(symbol)
    in_micro = symbol in micro_pool
    mm = {}
    if in_micro:
        try:
            mm = core.app.micro_metrics(symbol) or {}
        except Exception:
            mm = {}
    c = core._cache.get(symbol) or {}
    structure_ready = all(bool((c.get(tf) or {}).get("snap")) for tf in ("1h", "4h", "1d"))
    weekly_ready = bool((c.get("1w") or {}).get("snap"))
    return {
        "ts": _now_ms(),
        "price": price,
        "rapid": round(_rapid_score(symbol), 3),
        "early": round(_early_explosion_score(symbol), 3),
        "ma": str(ma.get("label") or ""),
        "ma_state": str(ma.get("proximity") or ""),
        "in_micro": bool(in_micro),
        "micro_ready": bool(mm.get("micro_ready")),
        "structure_ready": bool(structure_ready),
        "weekly_ready": bool(weekly_ready),
    }


def _nearest_snapshot(history, age_minutes):
    if not history:
        return None
    target = _now_ms() - int(age_minutes * 60 * 1000)
    return min(history, key=lambda x: abs(int(x.get("ts") or 0) - target))


def _miss_root(history):
    if not history:
        return "NOT_DISCOVERED"
    pre = history[-2] if len(history) >= 2 else history[-1]
    discovered = bool(pre.get("ma")) or _f(pre.get("early")) >= EARLY_EXPLOSION_MIN or _f(pre.get("rapid")) >= 85.0
    if not discovered:
        return "MODEL_RANKED_TOO_LOW"
    if not pre.get("in_micro"):
        return "DISCOVERED_NOT_PROMOTED"
    if not pre.get("micro_ready"):
        return "MICRO_DATA_MISSING"
    if not pre.get("structure_ready"):
        return "STRUCTURE_STALE"
    return "PROMOTED_TOO_LATE"


async def _sample_and_label_missed(client):
    global _last_sample_mono
    now_mono = time.monotonic()
    if now_mono - _last_sample_mono < HISTORY_SAMPLE_S:
        return
    _last_sample_mono = now_mono
    try:
        micro_pool = set(promoted_micro_symbols())
    except Exception:
        micro_pool = set(getattr(_core(), "_distributed_micro_sticky_pool", []) or [])

    for symbol in _universe():
        snap = _snapshot_feature(symbol, micro_pool)
        if not snap:
            continue
        history = _price_feature_history[symbol]
        history.append(snap)
        if len(history) < 6:
            continue
        returns = {}
        for mins in (5, 10, 15, 30, 60):
            old = _nearest_snapshot(history, mins)
            old_p = _f((old or {}).get("price"))
            if old_p > 0:
                returns[str(mins)] = (snap["price"] / old_p - 1.0) * 100.0
        best_move = max(returns.values()) if returns else 0.0
        if best_move < MISSED_MOVE_MIN_PCT:
            continue
        if now_mono - _last_missed_label_mono[symbol] < MISSED_COOLDOWN_S:
            continue
        _last_missed_label_mono[symbol] = now_mono
        event = {
            "symbol": symbol,
            "labelled_ms": _now_ms(),
            "move_pct": round(best_move, 4),
            "returns_pct": {k: round(v, 4) for k, v in returns.items()},
            "targets_hit": [t for t in (5, 10, 15, 20, 30, 40) if best_move >= t],
            "root_cause": _miss_root(history),
            "snapshots": {
                str(mins): _nearest_snapshot(history, mins)
                for mins in (60, 30, 15, 10, 5)
            },
            "revision": REVISION,
        }
        await client.lpush(MISSED_KEY, json.dumps(event, separators=(",", ":"), sort_keys=True))
        await client.ltrim(MISSED_KEY, 0, MISSED_MAX - 1)
        _stats["missed_labelled"] += 1
        print(
            f"Ψ-V12.4 MISSED-MOVER-TRAINING symbol={symbol} move={best_move:.1f}% "
            f"root={event['root_cause']} targets={event['targets_hit']}",
            flush=True,
        )


async def _rescue_worker_structure(client):
    global _rescue_cursor
    core = _core()
    symbols = []
    seen = set()
    for sym, _, _ in _combined_promotions():
        if sym not in seen:
            seen.add(sym); symbols.append(sym)
        if len(symbols) >= 30:
            break
    for sym in list(getattr(core, "_distributed_micro_sticky_pool", []) or []):
        sym = str(sym).upper()
        if sym not in seen:
            seen.add(sym); symbols.append(sym)
        if len(symbols) >= 44:
            break

    universe = sorted(_universe())
    if universe and len(symbols) < RESCUE_BATCH_SYMBOLS:
        checked = 0
        while checked < len(universe) and len(symbols) < RESCUE_BATCH_SYMBOLS:
            sym = universe[_rescue_cursor % len(universe)]
            _rescue_cursor = (_rescue_cursor + 1) % len(universe)
            checked += 1
            if sym not in seen:
                seen.add(sym); symbols.append(sym)
                _stats["structure_rescue_background_symbols"] += 1
    if not symbols:
        return

    keys = []
    mapping = []
    for sym in symbols:
        for tf in ("1h", "4h", "1d", "1w"):
            keys.append(f"{STRUCTURE_PREFIX}:{sym}:{tf}")
            mapping.append((sym, tf))
    raws = await client.mget(keys)
    now_ms = _now_ms()
    for (sym, tf), raw in zip(mapping, raws):
        _stats["structure_rescue_attempted"] += 1
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            _stats["structure_rescue_invalid"] += 1
            continue
        fetched = int(_f(payload.get("fetched_ms")))
        max_age_s = WEEKLY_RESCUE_MAX_AGE_S if tf == "1w" else RESCUE_MAX_AGE_S
        if fetched <= 0 or (now_ms - fetched) / 1000.0 > max_age_s:
            _stats["structure_rescue_stale"] += 1
            continue
        key = (sym, tf)
        if fetched <= int(_last_worker_fetch_ms.get(key) or 0):
            continue
        rows = payload.get("rows")
        if not isinstance(rows, list) or len(rows) < 16:
            _stats["structure_rescue_invalid"] += 1
            continue
        requested = int(payload.get("requested_limit") or len(rows))
        if core._commit_authoritative_rows(sym, tf, rows, requested, source="WORKER_RESCUE_V12_4"):
            _last_worker_fetch_ms[key] = fetched
            _stats["structure_rescue_imported"] += 1
            _stats[f"structure_rescue_imported_{tf}"] += 1


def _legacy_modules():
    core = _core()
    root = getattr(core, "legacy", None)
    if root is None:
        return []
    out, seen, queue = [], set(), [root]
    while queue and len(out) < 40:
        mod = queue.pop(0)
        ident = id(mod)
        if ident in seen:
            continue
        seen.add(ident)
        out.append(mod)
        for name in ("base", "v11", "rescue", "tape", "scanner", "core"):
            child = getattr(mod, name, None)
            if child is not None and hasattr(child, "__dict__") and id(child) not in seen:
                queue.append(child)
    return out


async def _import_legacy_missed(client):
    imported = 0
    for mod in _legacy_modules():
        rows = getattr(mod, "missed_moves", None)
        if rows is None:
            continue
        try:
            rows = list(rows)
        except Exception:
            continue
        for row in rows[-500:]:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").upper()
            if not symbol.endswith("USDT"):
                continue
            detected = int(_f(row.get("detected")) * 1000.0) if _f(row.get("detected")) < 10_000_000_000 else int(_f(row.get("detected")))
            raw_id = f"{symbol}|{detected}|{row.get('return_15m_pct')}|{row.get('best_prior_score')}"
            import hashlib
            event_id = hashlib.sha1(raw_id.encode()).hexdigest()[:20]
            if await client.sismember(MISSED_LEGACY_SEEN_KEY, event_id):
                _historical_missed_counts[symbol] += 1
                continue
            event = {
                "id": event_id,
                "symbol": symbol,
                "labelled_ms": detected or _now_ms(),
                "move_pct": _f(row.get("return_15m_pct")),
                "returns_pct": {"15": _f(row.get("return_15m_pct"))},
                "targets_hit": [t for t in (5, 10, 15, 20, 30, 40) if _f(row.get("return_15m_pct")) >= t],
                "root_cause": "PROMOTED_TOO_LATE" if row.get("had_v11_prediction") else "NOT_DISCOVERED",
                "legacy_missed_experience": True,
                "best_prior_score": _f(row.get("best_prior_score")),
                "revision": REVISION,
            }
            await client.lpush(MISSED_KEY, json.dumps(event, separators=(",", ":"), sort_keys=True))
            await client.ltrim(MISSED_KEY, 0, MISSED_MAX - 1)
            await client.sadd(MISSED_LEGACY_SEEN_KEY, event_id)
            _historical_missed_counts[symbol] += 1
            imported += 1
    if imported:
        _stats["legacy_missed_imported"] += imported
        print(f"Ψ-V12.4 MISSED-EXPERIENCE imported={imported} symbols={len(_historical_missed_counts)}", flush=True)


async def bootstrap():
    core = _core()
    if not core.REDIS_URL:
        return
    client = redis_async.from_url(core.REDIS_URL, encoding="utf-8", decode_responses=True)
    try:
        await client.ping()
        await _import_legacy_missed(client)
    finally:
        await client.aclose()


async def supervisor_loop():
    global _last_diag_mono, _last_error
    core = _core()
    if not core.REDIS_URL:
        _stats["supervisor_disabled"] = 1
        return
    while True:
        client = None
        try:
            client = redis_async.from_url(core.REDIS_URL, encoding="utf-8", decode_responses=True)
            await client.ping()
            while True:
                await _rescue_worker_structure(client)
                await _sample_and_label_missed(client)
                now_mono = time.monotonic()
                if now_mono - _last_diag_mono >= DIAG_SECONDS:
                    _last_diag_mono = now_mono
                    ma = ma_priority_candidates(10)
                    ml = ml_watch_candidates(10)
                    qualified = [r for r in ml if r.get("qualified")]
                    all_ma = [m for r in _ma_cache for m in list(r.get("matches") or [r])]
                    sma200 = sum(1 for r in all_ma if r.get("ma") == "SMA200")
                    sma50 = sum(1 for r in all_ma if r.get("ma") == "SMA50")
                    ema200 = sum(1 for r in all_ma if r.get("ma") == "EMA200")
                    ema50 = sum(1 for r in all_ma if r.get("ma") == "EMA50")
                    weekly_ma = sum(1 for r in all_ma if r.get("timeframe") == "1w")
                    hyd = _hydration_summary()
                    print(
                        "Ψ-V12.4 UPGRADE "
                        f"maPriority={len(_ma_cache)} maMatches={len(all_ma)} "
                        f"sma200={sma200} sma50={sma50} ema200={ema200} ema50={ema50} weeklyMA={weekly_ma} "
                        f"topMA={','.join(r['symbol']+':'+r['label']+'/'+r['proximity'] for r in ma[:5]) or '-'} "
                        f"deep200={hyd['deep_200_ready']}/{hyd['universe']} weeklyReady={hyd['weekly_ready']}/{hyd['universe']} "
                        f"weeklyDeep={hyd['weekly_deep_ready']}/{hyd['universe']} "
                        f"mlWatch={len(_ml_watch_cache)} ml70={len(qualified)} "
                        f"micro={_stats.get('dynamic_micro_pool',0)} "
                        f"rescue={_stats.get('structure_rescue_imported',0)}/{_stats.get('structure_rescue_attempted',0)} "
                        f"missedLabelled={_stats.get('missed_labelled',0)} authority={AUTHORITY_CHAIN}",
                        flush=True,
                    )
                    print(
                        "Ψ-V12.4 REPORT-CONTRACT "
                        f"v={SCAN_REPORT_VERSION} required={','.join(SCAN_REQUIRED_SECTIONS)} "
                        "rule=SHOW_ALL_SECTIONS_OR_EXPLICIT_NONE",
                        flush=True,
                    )
                await asyncio.sleep(RESCUE_POLL_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _last_error = f"{type(exc).__name__}:{exc}"
            print(f"Ψ-V12.4 UPGRADE_ERROR {_last_error}", flush=True)
            await asyncio.sleep(max(3.0, RESCUE_POLL_S))
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


def install(core, hardening, learner):
    global CORE, HARDENING, LEARNER
    global _original_gate, _original_micro, _original_priority, _original_scan, _original_health, _original_evaluate
    if CORE is not None:
        return
    CORE = core
    HARDENING = hardening
    LEARNER = learner

    _original_gate = core._strict_execution_gate
    _original_micro = core._distributed_micro_symbols
    _original_priority = core._priority_symbols
    _original_scan = core.v12_scan
    _original_health = core.v12_health
    _original_evaluate = core.evaluate_symbol

    _wrap_learning()
    core._strict_execution_gate = _gate_wrapper
    core._distributed_micro_symbols = promoted_micro_symbols
    core._priority_symbols = priority_symbols
    core.evaluate_symbol = evaluate_symbol
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        f"Ψ-V12.4 UPGRADE installed — {REVISION}; MA proximity=promotion-only; "
        f"ML override>{100*ML_OVERRIDE_THRESHOLD:.0f}% requires validated TEST calibration + hard execution safety",
        flush=True,
    )
