import json
import math
import os
import time
from collections import defaultdict

REVISION = "14.0.0-setup-specific-authority"
AUTHORITY_CHAIN = "V12.3.4_STRICT_OR_V14_SETUP_SPECIFIC->BUY_NOW"

CORE = None
EARLY = None
V124 = None
V13 = None
_ORIGINAL_GATE = None
_ORIGINAL_ATTACH = None
_ORIGINAL_MICRO = None
_ORIGINAL_SCAN = None
_ORIGINAL_HEALTH = None
ACTIVE = {}
STATS = defaultdict(int)
_EARLY_CACHE = {}
_EARLY_CACHE_MS = 0

MIN_STRUCTURAL_STRENGTH = max(70.0, float(os.getenv("PSI_V133_MIN_STRUCTURAL_STRENGTH", "84")))
MIN_ARMED_STRENGTH = max(MIN_STRUCTURAL_STRENGTH, float(os.getenv("PSI_V133_MIN_ARMED_STRENGTH", "90")))
MAX_SENSOR_AGE_MS = max(250, int(os.getenv("PSI_V133_MAX_SENSOR_AGE_MS", "1200")))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_V133_MAX_SPREAD_BPS", "18")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_V133_MAX_SLIPPAGE_BPS", "30")))
MAX_ENTRY_DISTANCE_PCT = max(0.25, float(os.getenv("PSI_V133_MAX_ENTRY_DISTANCE_PCT", "3.0")))
MAX_RISK_PCT = max(0.5, float(os.getenv("PSI_V133_MAX_RISK_PCT", "8.0")))
MIN_RR_TP1 = max(1.0, float(os.getenv("PSI_V133_MIN_RR_TP1", "1.35")))
MIN_FLOW_GROUPS = max(3, min(int(os.getenv("PSI_V133_MIN_FLOW_GROUPS", "4")), 5))


def _f(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _now_ms():
    return int(time.time() * 1000)


def _early_map():
    global _EARLY_CACHE, _EARLY_CACHE_MS
    now = _now_ms()
    if _EARLY_CACHE and now - _EARLY_CACHE_MS <= 250:
        return _EARLY_CACHE
    if EARLY is None:
        return {}
    try:
        rows = EARLY.early_candidates(200, actionable_only=False) or []
    except Exception:
        rows = []
    _EARLY_CACHE = {
        str(row.get("symbol") or "").upper(): row
        for row in rows
        if isinstance(row, dict) and row.get("symbol")
    }
    _EARLY_CACHE_MS = now
    return _EARLY_CACHE


def _risk_plan(structural, entry):
    structural = structural or {}
    stop = _f(structural.get("invalidation"), _f(structural.get("stop")))
    tp1 = _f(structural.get("tp1"))
    tp2 = _f(structural.get("tp2"))
    tp3 = _f(structural.get("tp3"))
    if not (entry > 0 and 0 < stop < entry < tp1):
        return {
            "valid": False, "entry": entry, "stop": stop,
            "tp1": tp1, "tp2": tp2, "tp3": tp3,
            "risk_pct": 0.0, "rr_tp1": 0.0,
        }
    risk = entry - stop
    risk_pct = risk / entry * 100.0
    rr = (tp1 - entry) / risk if risk > 0 else 0.0
    return {
        "valid": (
            risk_pct <= MAX_RISK_PCT
            and rr >= MIN_RR_TP1
            and (tp2 <= 0 or tp2 > tp1)
            and (tp3 <= 0 or tp3 > max(tp2, tp1))
        ),
        "entry": entry, "stop": stop,
        "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "risk_pct": risk_pct, "rr_tp1": rr,
    }


def _flow_groups(early):
    early = early or {}
    values = {
        "buyer_dominance": _f(early.get("buy_ratio"), 0.5) >= 0.54,
        "cvd_acceleration": _f(early.get("cvd_acceleration")) > 0.0,
        "ofi_acceleration": (
            _f(early.get("ofi")) > 0.0
            or _f(early.get("ofi_acceleration")) > 0.0
        ),
        "book_pressure": (
            _f(early.get("obi")) >= 0.02
            or _f(early.get("ask_depletion")) > 0.0
        ),
        "activity_acceleration": (
            _f(early.get("relative_volume_10s")) >= 1.05
            or _f(early.get("relative_volume_30s")) >= 1.05
            or _f(early.get("trade_acceleration")) >= 1.05
        ),
    }
    return values, sum(bool(v) for v in values.values())


def _ml_support(symbol):
    if V13 is None:
        return False
    try:
        return symbol in {
            str(row.get("symbol") or "").upper()
            for row in (V13.five() or [])
            if isinstance(row, dict)
        }
    except Exception:
        return False



EXHAUSTION_SETUPS = {
    "DEEP_PULLBACK_EXHAUSTION", "DAILY_RANGE_BOTTOM_REVERSAL",
    "FAILED_BREAKDOWN_RECLAIM", "LIQUIDITY_SWEEP_REVERSAL",
    "VOLUME_CLIMAX_REVERSAL", "HIGHER_LOW_REVERSAL",
}
BREAKOUT_SETUPS = {
    "COMPRESSION_BREAKOUT", "BREAKOUT_RETEST", "TREND_CONTINUATION",
    "COILED_ACCUMULATION",
}
BEAST_SETUPS = {
    "COILED_ACCUMULATION", "COMPRESSION_BREAKOUT", "TREND_CONTINUATION",
    "HIGHER_LOW_REVERSAL", "HIGH_CONFLUENCE_BUY",
}


def _active_setups(structural):
    structural = structural if isinstance(structural, dict) else {}
    out = []
    primary = str(structural.get("setup") or "")
    if primary:
        out.append({
            "name": primary,
            "state": str(structural.get("state") or ""),
            "strength": _f(structural.get("setup_strength")),
        })
    for row in list(structural.get("active_setups") or []):
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if name and name not in {x["name"] for x in out}:
            out.append({
                "name": name,
                "state": str(row.get("state") or ""),
                "strength": _f(row.get("strength")),
            })
    return out


def _setup_match(structural, names):
    rows = [x for x in _active_setups(structural) if x["name"] in names]
    rows.sort(key=lambda x: (x["state"] == "BUY", x["state"] == "ARMED", x["strength"]), reverse=True)
    return rows[0] if rows else {}


def _global_safety(structural, legacy, micro, integrity, early, now_ms):
    """Only genuine data/execution/risk vetoes are global.

    Strategy confirmation is deliberately excluded here. BEAST, EXHAUSTION,
    BREAKOUT and STRUCTURAL_CONFIRMATION each own their strategy evidence.
    """
    blockers = []
    structural = structural if isinstance(structural, dict) else {}
    early = early if isinstance(early, dict) else {}
    micro = micro if isinstance(micro, dict) else {}
    integrity = integrity if isinstance(integrity, dict) else {}

    if not bool(early.get("hard_sensor_safety")):
        blockers.append("HARD_SENSOR_SAFETY")

    generated_ms = int(_f(early.get("generated_ms"), 0))
    sensor_age = max(0, now_ms - generated_ms) if generated_ms > 0 else 999999999
    trade_age = int(_f(early.get("trade_age_ms"), 999999999))
    book_age = int(_f(early.get("book_age_ms"), 999999999))
    if sensor_age > MAX_SENSOR_AGE_MS:
        blockers.append("STALE_SENSOR_SNAPSHOT")
    if trade_age > MAX_SENSOR_AGE_MS:
        blockers.append("STALE_SENSOR_TRADE")
    if book_age > MAX_SENSOR_AGE_MS:
        blockers.append("STALE_SENSOR_BOOK")
    if not bool(early.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE_VALID")
    if not bool(early.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE_VALID")

    # When the native live-micro row explicitly says it is invalid, fail closed.
    if "micro_ready" in micro and not bool(micro.get("micro_ready")):
        blockers.append("LIVE_MICRO_DATA")
    if "sequence_verified" in micro and not bool(micro.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE_VALID")
    if "book_sequence_verified" in micro and not bool(micro.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE_VALID")

    spread = _f(early.get("spread_bps"), 999999.0)
    slip = _f(early.get("slippage_bps"), 999999.0)
    if spread < 0 or spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD_FILTER")
    if slip < 0 or slip > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE_FILTER")

    if bool(structural.get("anti_chase")):
        blockers.append("ANTI_CHASE")

    entry = _f(early.get("entry_reference"), _f(structural.get("current")))
    max_chase = _f(structural.get("max_chase"))
    if max_chase > 0 and entry > max_chase:
        blockers.append("CUMULATIVE_EXTENSION_GUARD")

    structural_entry = _f(structural.get("entry_low"), _f(structural.get("entry")))
    if structural_entry > 0 and entry > 0:
        distance = abs(entry / structural_entry - 1.0) * 100.0
        if distance > MAX_ENTRY_DISTANCE_PCT:
            blockers.append("ENTRY_TOO_FAR_FROM_STRUCTURE")
    else:
        distance = 999.0
        blockers.append("STRUCTURAL_ENTRY_REFERENCE")

    risk = _risk_plan(structural, entry)
    if not risk["valid"]:
        blockers.append("VALID_RISK_REWARD")

    hard = (legacy or {}).get("pinpoint_hard_status") or {}
    for key in ("LIVE_MICRO_DATA", "TRADE_SEQUENCE_VALID", "BOOK_SEQUENCE_VALID",
                "SPREAD_FILTER", "SLIPPAGE_FILTER", "CUMULATIVE_EXTENSION_GUARD",
                "QUALIFIED_MICRO_WARMUP", "FRESH_STRUCTURE"):
        if key in hard and hard.get(key) is False:
            blockers.append(key)
        low = key.lower()
        if low in hard and hard.get(low) is False:
            blockers.append(key)

    # Integrity is advisory except for explicit live-data failures. Strategy
    # blockers inside the legacy integrity chain do not become universal vetoes.
    for item in list(integrity.get("blockers") or []):
        name = str(item)
        if (
            name.startswith("STALE_")
            or name in {
                "LIVE_MICRO_DATA", "LIVE_TAPE", "TRADE_SEQUENCE_VALID",
                "BOOK_SEQUENCE_VALID", "SPREAD_FILTER", "SLIPPAGE_FILTER",
                "CUMULATIVE_EXTENSION_GUARD",
            }
        ):
            blockers.append(name)

    return {
        "pass": not blockers,
        "blockers": list(dict.fromkeys(str(x) for x in blockers if str(x))),
        "entry": entry,
        "risk": risk,
        "distance": distance,
        "sensor_age_ms": sensor_age,
        "trade_age_ms": trade_age,
        "book_age_ms": book_age,
        "spread_bps": spread,
        "slippage_bps": slip,
    }


def _engine_beast(structural, early):
    setup = _setup_match(structural, BEAST_SETUPS)
    early_state = str((early or {}).get("state") or "")
    hazard = _f((early or {}).get("hazard_score"))
    groups, _ = _flow_groups(early)
    evidence = {
        "buyer": groups["buyer_dominance"],
        "cvd": groups["cvd_acceleration"],
        "pressure": groups["ofi_acceleration"] or groups["book_pressure"],
        "activity": groups["activity_acceleration"],
    }
    evidence_count = sum(bool(x) for x in evidence.values())
    setup_hint = bool(setup) or _f((early or {}).get("v128_probe_score")) >= 70 or _f((early or {}).get("change_point_delta")) >= 8
    early_ok = early_state in {"EARLY_PINPOINT", "EARLY_ARMED"} or hazard >= 82
    passed = bool(early_ok and setup_hint and hazard >= 74 and evidence_count >= 3)
    score = min(100.0, 0.45 * hazard + 12.0 * evidence_count + (12.0 if setup else 0.0))
    return {
        "engine": "BEAST", "pass": passed, "score": round(score, 2),
        "setup": setup.get("name") if setup else "EARLY_ANOMALY",
        "setup_state": setup.get("state") if setup else early_state,
        "evidence": evidence,
        "blockers": [] if passed else [
            x for x, ok in (
                ("BEAST_EARLY_STATE", early_ok),
                ("BEAST_SETUP_OR_PROBE", setup_hint),
                ("BEAST_HAZARD", hazard >= 74),
                ("BEAST_FLOW_3_OF_4", evidence_count >= 3),
            ) if not ok
        ],
    }


def _engine_exhaustion(structural, early):
    setup = _setup_match(structural, EXHAUSTION_SETUPS)
    groups, _ = _flow_groups(early)
    reversal = {
        "buyer": _f((early or {}).get("buy_ratio"), 0.5) >= 0.52,
        "cvd_nonnegative": _f((early or {}).get("cvd_acceleration")) >= 0.0,
        "order_pressure": groups["ofi_acceleration"] or groups["book_pressure"],
    }
    confirmations = sum(bool(x) for x in reversal.values())
    state = str(setup.get("state") or "")
    strength = _f(setup.get("strength"))
    setup_ok = bool(setup and state in {"BUY", "ARMED"} and strength >= 62)
    required = 1 if state == "BUY" else 2
    passed = bool(setup_ok and confirmations >= required)
    score = min(100.0, strength * 0.65 + confirmations * 11.0)
    return {
        "engine": "EXHAUSTION", "pass": passed, "score": round(score, 2),
        "setup": setup.get("name") if setup else None, "setup_state": state,
        "evidence": reversal,
        "blockers": [] if passed else [
            x for x, ok in (
                ("EXHAUSTION_SETUP", setup_ok),
                (f"EXHAUSTION_CONFIRMATION_{confirmations}/{required}", confirmations >= required),
            ) if not ok
        ],
    }


def _engine_breakout(structural, early):
    setup = _setup_match(structural, BREAKOUT_SETUPS)
    groups, _ = _flow_groups(early)
    checks = {
        "buyer_dominance": groups["buyer_dominance"],
        "cvd_acceleration": groups["cvd_acceleration"],
        "order_pressure": groups["ofi_acceleration"],
        "book_pressure": groups["book_pressure"],
        "activity_acceleration": groups["activity_acceleration"],
    }
    count = sum(bool(x) for x in checks.values())
    state = str(setup.get("state") or "")
    strength = _f(setup.get("strength"))
    confirmed_break = bool(setup and state == "BUY" and strength >= 78)
    # Anti-fakeout is intentionally strict, but ONLY for the breakout family.
    passed = bool(confirmed_break and count >= 4 and checks["activity_acceleration"] and checks["cvd_acceleration"])
    score = min(100.0, strength * 0.55 + count * 9.0)
    return {
        "engine": "BREAKOUT", "pass": passed, "score": round(score, 2),
        "setup": setup.get("name") if setup else None, "setup_state": state,
        "evidence": checks,
        "blockers": [] if passed else [
            x for x, ok in (
                ("BREAKOUT_SETUP_CONFIRMED", confirmed_break),
                (f"ANTI_FAKEOUT_FLOW_{count}/4", count >= 4),
                ("BREAKOUT_ACTIVITY", checks["activity_acceleration"]),
                ("BREAKOUT_CVD", checks["cvd_acceleration"]),
            ) if not ok
        ],
    }


def _engine_structural(structural, early):
    groups, count = _flow_groups(early)
    state = str((structural or {}).get("state") or "")
    strength = _f((structural or {}).get("setup_strength"))
    passed = bool(state == "BUY" and strength >= 82 and count >= 2)
    score = min(100.0, strength * 0.75 + count * 5.0)
    return {
        "engine": "STRUCTURAL_CONFIRMATION", "pass": passed,
        "score": round(score, 2), "setup": str((structural or {}).get("setup") or ""),
        "setup_state": state, "evidence": groups,
        "blockers": [] if passed else [
            x for x, ok in (
                ("STRUCTURAL_BUY", state == "BUY"),
                ("STRUCTURAL_STRENGTH_82", strength >= 82),
                (f"STRUCTURAL_FLOW_{count}/2", count >= 2),
            ) if not ok
        ],
    }


def _evidence_gate(structural, legacy, micro, integrity, early, now_ms=None):
    structural = structural if isinstance(structural, dict) else {}
    legacy = legacy if isinstance(legacy, dict) else {}
    early = early if isinstance(early, dict) else {}
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    symbol = str(
        structural.get("symbol") or legacy.get("symbol") or early.get("symbol") or ""
    ).upper()

    safety = _global_safety(structural, legacy, micro, integrity, early, now_ms)
    engines = [
        _engine_beast(structural, early),
        _engine_exhaustion(structural, early),
        _engine_breakout(structural, early),
        _engine_structural(structural, early),
    ]
    passing = [x for x in engines if x["pass"]]
    passing.sort(key=lambda x: x["score"], reverse=True)
    winner = passing[0] if passing else None

    risk = safety["risk"]
    blockers = list(safety["blockers"])
    if not winner:
        engine_reasons = []
        for engine in engines:
            engine_reasons.extend(engine["blockers"])
        blockers.extend(engine_reasons[:8])

    confidence = winner["score"] if winner else max((x["score"] for x in engines), default=0.0)
    return {
        "pass": bool(safety["pass"] and winner),
        "blockers": list(dict.fromkeys(str(x) for x in blockers if str(x))),
        "symbol": symbol,
        "entry": safety["entry"],
        "stop": risk.get("stop"),
        "tp1": risk.get("tp1"),
        "tp2": risk.get("tp2"),
        "tp3": risk.get("tp3"),
        "risk_pct": round(_f(risk.get("risk_pct")), 4),
        "rr_tp1": round(_f(risk.get("rr_tp1")), 3),
        "entry_distance_pct": round(safety["distance"], 3) if safety["distance"] < 900 else None,
        "flow_groups": _flow_groups(early)[0],
        "flow_group_count": _flow_groups(early)[1],
        "early_state": str(early.get("state") or ""),
        "hazard_score": _f(early.get("hazard_score")),
        "structural_strength": _f(structural.get("setup_strength")),
        "ml_support": _ml_support(symbol),
        "confidence_score": round(confidence + (3.0 if _ml_support(symbol) else 0.0), 2),
        "sensor_age_ms": safety["sensor_age_ms"],
        "trade_age_ms": safety["trade_age_ms"],
        "book_age_ms": safety["book_age_ms"],
        "spread_bps": safety["spread_bps"],
        "slippage_bps": safety["slippage_bps"],
        "strategy_engine": winner["engine"] if winner else None,
        "strategy_setup": winner["setup"] if winner else None,
        "strategy_engines": engines,
        "global_safety_pass": safety["pass"],
    }

def _gate_wrapper(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    result = dict(_ORIGINAL_GATE(structural_row, legacy_row, micro_metrics, integrity))
    structural_row = structural_row if isinstance(structural_row, dict) else {}
    legacy_row = legacy_row if isinstance(legacy_row, dict) else {}
    symbol = str(structural_row.get("symbol") or legacy_row.get("symbol") or "").upper()

    if result.get("buy_now"):
        result["authority_chain"] = AUTHORITY_CHAIN
        result["execution_route"] = "STRICT_PINPOINT"
        ACTIVE.pop(symbol, None)
        STATS["strict_buy_now"] += 1
        return result

    evidence = _evidence_gate(
        structural_row, legacy_row, micro_metrics or {}, integrity or {},
        _early_map().get(symbol) or {},
    )
    result["evidence_ready_candidate"] = True
    result["evidence_ready_blockers"] = list(evidence["blockers"])
    result["evidence_ready_confidence"] = evidence["confidence_score"]

    if not evidence["pass"]:
        ACTIVE.pop(symbol, None)
        STATS["evidence_blocked"] += 1
        result["authority_chain"] = AUTHORITY_CHAIN
        return result

    ACTIVE[symbol] = {**evidence, "activated_ms": _now_ms(), "route": evidence.get("strategy_engine") or "SETUP_SPECIFIC"}
    STATS["evidence_buy_now"] += 1
    result.update({
        "buy_now": True,
        "execution_state": "BUY NOW",
        "blockers": [],
        "authority_chain": AUTHORITY_CHAIN,
        "execution_route": evidence.get("strategy_engine") or "SETUP_SPECIFIC",
        "pinpoint_entry_status": "EVIDENCE_TRIGGERED",
        "pinpoint_state": "BUY NOW",
        "evidence_ready_buy": True,
        "setup_specific_buy": True,
        "strategy_engine": evidence.get("strategy_engine"),
        "strategy_setup": evidence.get("strategy_setup"),
        "evidence_ready_confidence": evidence["confidence_score"],
        "evidence_entry": evidence["entry"],
        "evidence_stop": evidence["stop"],
        "evidence_tp1": evidence["tp1"],
        "evidence_tp2": evidence["tp2"],
        "evidence_tp3": evidence["tp3"],
        "evidence_risk_pct": evidence["risk_pct"],
        "evidence_rr_tp1": evidence["rr_tp1"],
        "evidence_flow_groups": evidence["flow_groups"],
        "evidence_ml_support": evidence["ml_support"],
    })
    return result


def _attach_wrapper(symbol, structural_row):
    row = dict(_ORIGINAL_ATTACH(symbol, structural_row))
    active = ACTIVE.get(str(symbol or "").upper())
    if active and row.get("execution_state") == "BUY NOW":
        row["execution_route"] = active.get("route") or "SETUP_SPECIFIC"
        row["execution_entry"] = active["entry"]
        row["execution_stop"] = active["stop"]
        row["execution_tp1"] = active["tp1"]
        row["execution_tp2"] = active["tp2"]
        row["execution_tp3"] = active["tp3"]
        row["execution_risk_pct"] = active["risk_pct"]
        row["execution_rr_tp1"] = active["rr_tp1"]
        row["execution_confidence_score"] = active["confidence_score"]
        row["execution_flow_groups"] = active["flow_groups"]
        row["execution_ml_support"] = active["ml_support"]
    return row


def _micro_symbols():
    base = list(_ORIGINAL_MICRO() or [])
    universe = set(str(s).upper() for s in list(getattr(CORE.q, "universe", []) or []))
    pool_size = int(getattr(CORE, "REDIS_MICRO_POOL_SIZE", max(40, len(base) or 40)))

    try:
        structural = sorted(
            [r for r in (CORE._board() or []) if isinstance(r, dict) and r.get("state") == "BUY"],
            key=lambda r: (_f(r.get("setup_strength")), _f(r.get("extended_gain_pct"))),
            reverse=True,
        )
    except Exception:
        structural = []

    try:
        early = EARLY.early_candidates(40, actionable_only=True) or []
    except Exception:
        early = []
    try:
        ml = V13.five() or []
    except Exception:
        ml = []

    out, seen = [], set()
    def add(sym):
        sym = str(sym or "").upper()
        if sym in universe and sym not in seen and len(out) < pool_size:
            seen.add(sym)
            out.append(sym)

    for row in structural:
        add(row.get("symbol"))
    for row in early:
        add(row.get("symbol"))
    for row in ml:
        add(row.get("symbol"))
    for sym in base:
        add(sym)

    STATS["micro_pool"] = len(out)
    STATS["micro_structural_priority"] = sum(
        1 for r in structural if str(r.get("symbol") or "").upper() in seen
    )
    return out or base


def _augment(response):
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    data["version"] = REVISION
    data["execution_authority"] = "PINPOINT_STRICT_OR_SETUP_SPECIFIC"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["v13_3_execution"] = {
        "revision": REVISION,
        "strict_lane_unchanged": True,
        "evidence_lane": "SETUP_SPECIFIC",
        "strategy_engines": ["BEAST", "EXHAUSTION", "BREAKOUT", "STRUCTURAL_CONFIRMATION"],
        "universal_strategy_gate_removed": True,
        "global_veto_scope": "DATA_EXECUTION_RISK_ONLY",
        "freshness_max_ms": MAX_SENSOR_AGE_MS,
        "min_structural_strength": MIN_STRUCTURAL_STRENGTH,
        "min_armed_strength": MIN_ARMED_STRENGTH,
        "min_flow_groups": MIN_FLOW_GROUPS,
        "max_spread_bps": MAX_SPREAD_BPS,
        "max_slippage_bps": MAX_SLIPPAGE_BPS,
        "max_entry_distance_pct": MAX_ENTRY_DISTANCE_PCT,
        "max_risk_pct": MAX_RISK_PCT,
        "min_rr_tp1": MIN_RR_TP1,
        "active": list(ACTIVE.values())[:20],
        "stats": dict(STATS),
    }
    return CORE.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    return _augment(await _ORIGINAL_SCAN(request))


async def _health_wrapper(request):
    return _augment(await _ORIGINAL_HEALTH(request))


def install(core, early, v124=None, v13=None):
    global CORE, EARLY, V124, V13
    global _ORIGINAL_GATE, _ORIGINAL_ATTACH, _ORIGINAL_MICRO, _ORIGINAL_SCAN, _ORIGINAL_HEALTH
    if CORE is not None:
        return
    CORE, EARLY, V124, V13 = core, early, v124, v13
    _ORIGINAL_GATE = core._strict_execution_gate
    _ORIGINAL_ATTACH = core._attach_execution_gate
    _ORIGINAL_MICRO = core._distributed_micro_symbols
    _ORIGINAL_SCAN = core.v12_scan
    _ORIGINAL_HEALTH = core.v12_health

    core._strict_execution_gate = _gate_wrapper
    core._attach_execution_gate = _attach_wrapper
    core._distributed_micro_symbols = _micro_symbols
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        "PSI-V14 INSTALLED revision=" + REVISION
        + " authority=STRICT_OR_SETUP_SPECIFIC strictLane=UNCHANGED"
        + f" freshness<={MAX_SENSOR_AGE_MS}ms stale/missing=FAIL_CLOSED",
        flush=True,
    )
