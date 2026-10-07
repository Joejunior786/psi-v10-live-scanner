import json
import math
import os
import time
from collections import defaultdict

REVISION = "13.3.0-evidence-ready-dual-authority"
AUTHORITY_CHAIN = "V12.3.4_STRICT_OR_V13.3_EVIDENCE_READY->BUY_NOW"

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


def _evidence_gate(structural, legacy, micro, integrity, early, now_ms=None):
    structural = structural if isinstance(structural, dict) else {}
    legacy = legacy if isinstance(legacy, dict) else {}
    early = early if isinstance(early, dict) else {}
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    blockers = []

    symbol = str(
        structural.get("symbol") or legacy.get("symbol") or early.get("symbol") or ""
    ).upper()
    strength = _f(structural.get("setup_strength"))
    state = str(structural.get("state") or "")
    if state != "BUY":
        blockers.append("V12_STRUCTURAL_BUY")
    if strength < MIN_STRUCTURAL_STRENGTH:
        blockers.append("STRUCTURAL_STRENGTH")
    if bool(structural.get("anti_chase")):
        blockers.append("ANTI_CHASE")

    early_state = str(early.get("state") or "")
    if early_state not in {"EARLY_PINPOINT", "EARLY_ARMED"}:
        blockers.append("EARLY_EXECUTION_STATE")
    if early_state == "EARLY_ARMED" and strength < MIN_ARMED_STRENGTH:
        blockers.append("ARMED_REQUIRES_STRONG_STRUCTURE")
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

    spread = _f(early.get("spread_bps"), 999999.0)
    slip = _f(early.get("slippage_bps"), 999999.0)
    if spread < 0 or spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD_FILTER")
    if slip < 0 or slip > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE_FILTER")

    hard = legacy.get("pinpoint_hard_status") or {}
    if hard.get("MARKET_REGIME_SAFETY") is False or hard.get("market_regime_safety") is False:
        blockers.append("MARKET_REGIME_SAFETY")

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

    groups, group_count = _flow_groups(early)
    if group_count < MIN_FLOW_GROUPS:
        blockers.append(f"FLOW_GROUPS_{group_count}/{MIN_FLOW_GROUPS}")
    if not groups["cvd_acceleration"]:
        blockers.append("POSITIVE_CVD_ACCELERATION")
    if not (groups["ofi_acceleration"] or groups["book_pressure"]):
        blockers.append("POSITIVE_ORDER_FLOW_OR_BOOK")

    ml_support = _ml_support(symbol)
    confidence = min(
        100.0,
        0.35 * min(100.0, _f(early.get("hazard_score")))
        + 0.30 * min(100.0, strength)
        + 0.25 * (group_count / 5.0 * 100.0)
        + 0.10 * min(100.0, max(0.0, risk.get("rr_tp1", 0.0)) / 3.0 * 100.0)
        + (3.0 if ml_support else 0.0),
    )
    blockers = list(dict.fromkeys(str(x) for x in blockers if str(x)))
    return {
        "pass": not blockers,
        "blockers": blockers,
        "symbol": symbol,
        "entry": entry,
        "stop": risk.get("stop"),
        "tp1": risk.get("tp1"),
        "tp2": risk.get("tp2"),
        "tp3": risk.get("tp3"),
        "risk_pct": round(_f(risk.get("risk_pct")), 4),
        "rr_tp1": round(_f(risk.get("rr_tp1")), 3),
        "entry_distance_pct": round(distance, 3) if distance < 900 else None,
        "flow_groups": groups,
        "flow_group_count": group_count,
        "early_state": early_state,
        "hazard_score": _f(early.get("hazard_score")),
        "structural_strength": strength,
        "ml_support": ml_support,
        "confidence_score": round(confidence, 2),
        "sensor_age_ms": sensor_age,
        "trade_age_ms": trade_age,
        "book_age_ms": book_age,
        "spread_bps": spread,
        "slippage_bps": slip,
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

    ACTIVE[symbol] = {**evidence, "activated_ms": _now_ms(), "route": "EVIDENCE_READY"}
    STATS["evidence_buy_now"] += 1
    result.update({
        "buy_now": True,
        "execution_state": "BUY NOW",
        "blockers": [],
        "authority_chain": AUTHORITY_CHAIN,
        "execution_route": "EVIDENCE_READY",
        "pinpoint_entry_status": "EVIDENCE_TRIGGERED",
        "pinpoint_state": "BUY NOW",
        "evidence_ready_buy": True,
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
        row["execution_route"] = "EVIDENCE_READY"
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
    data["execution_authority"] = "PINPOINT_STRICT_OR_EVIDENCE_READY"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["v13_3_execution"] = {
        "revision": REVISION,
        "strict_lane_unchanged": True,
        "evidence_lane": "ENABLED",
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
        "PSI-V13.3 INSTALLED revision=" + REVISION
        + " authority=STRICT_OR_EVIDENCE_READY strictLane=UNCHANGED"
        + f" freshness<={MAX_SENSOR_AGE_MS}ms stale/missing=FAIL_CLOSED",
        flush=True,
    )
