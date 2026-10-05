import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

REVISION = "12.8.0-early-probe-sticky-sequence-memory"
ROLE = "EARLY_PROBE+STICKY_MICRO+FAST_ABORT+MISSED_MOVE_LEARNING"
STRICT_BUY_AUTHORITY_UNCHANGED = True

CORE = None
V125 = None
V127 = None

FRESH_MS = max(250, int(os.getenv("PSI_V128_FRESH_MS", "1200")))
PROBE_SCORE = max(60.0, float(os.getenv("PSI_V128_PROBE_SCORE", "75")))
PROBE_ALIGNMENT = max(45.0, float(os.getenv("PSI_V128_PROBE_ALIGNMENT", "58")))
PROBE_HAZARD = max(55.0, float(os.getenv("PSI_V128_PROBE_HAZARD", "68")))
STICKY_SECONDS = max(60.0, min(float(os.getenv("PSI_V128_STICKY_SECONDS", "420")), 900.0))
STICKY_SLOTS = max(2, min(int(os.getenv("PSI_V128_STICKY_SLOTS", "10")), 20))
DIAG_SECONDS = max(5.0, float(os.getenv("PSI_V128_DIAG_SECONDS", "10")))
OUTCOME_PATH = os.getenv("PSI_V128_OUTCOME_PATH", "/data/psi_v12_8_probe_outcomes.jsonl").strip()
OUTCOME_MAX_BYTES = max(5_000_000, int(os.getenv("PSI_V128_OUTCOME_MAX_BYTES", "250000000")))
PROBE_POSITION_FRACTION = max(0.05, min(float(os.getenv("PSI_V128_PROBE_POSITION_FRACTION", "0.20")), 0.35))
SEQUENCE_POSITION_FRACTION = max(PROBE_POSITION_FRACTION, min(float(os.getenv("PSI_V128_SEQUENCE_POSITION_FRACTION", "0.35")), 0.50))

ABORT_OFI = float(os.getenv("PSI_V128_ABORT_OFI", "-0.12"))
ABORT_OBI = float(os.getenv("PSI_V128_ABORT_OBI", "-0.15"))
ABORT_BUY_RATIO = max(0.35, min(float(os.getenv("PSI_V128_ABORT_BUY_RATIO", "0.47")), 0.55))
ABORT_HAZARD_DROP = max(8.0, float(os.getenv("PSI_V128_ABORT_HAZARD_DROP", "18")))

OUTCOME_WINDOWS_MS = (60_000, 180_000, 300_000, 900_000, 3_600_000)

_memory = defaultdict(lambda: {
    "first_seen_ms": 0,
    "activity_ms": 0,
    "flow_ms": 0,
    "book_ms": 0,
    "thrust_ms": 0,
    "probe_ms": 0,
    "probe_price": 0.0,
    "peak_price": 0.0,
    "trough_price": 0.0,
    "peak_hazard": 0.0,
    "peak_probe_score": 0.0,
    "snapshots": deque(maxlen=64),
    "resolved_windows": set(),
})
_sticky_until = {}
_stats = defaultdict(int)
_last_diag_mono = 0.0

_original_rebuild = None
_original_promotion_symbols = None
_original_scan = None
_original_health = None


def _f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def _fresh(row):
    if not isinstance(row, dict):
        return False
    return (
        bool(row.get("hard_sensor_safety"))
        and _f(row.get("trade_age_ms"), 999999999) <= FRESH_MS
        and _f(row.get("book_age_ms"), 999999999) <= FRESH_MS
        and bool(row.get("sequence_verified"))
        and bool(row.get("book_sequence_verified"))
    )


def _probe_score(row):
    hazard = _f(row.get("hazard_score"))
    align = _f(row.get("alignment_score"), _f(row.get("v127_alignment_score")))
    order = _f(row.get("sequence_order_score"))
    momentum = _f(row.get("momentum_score"), 50.0)
    persistence = _f(row.get("sequence_persistence"))
    delta = max(0.0, _f(row.get("change_point_delta")))
    rv = max(0.0, _f(row.get("relative_volume_10s")))
    ta = max(0.0, _f(row.get("trade_acceleration")))
    accel = min(100.0, delta * 2.0 + min(rv, 10.0) * 4.0 + min(ta, 10.0) * 3.0)
    return _clamp(
        0.28 * hazard
        + 0.24 * align
        + 0.18 * order
        + 0.12 * momentum
        + 0.08 * persistence
        + 0.10 * accel
    )


def _probe_gate(row):
    if not _fresh(row):
        return False, "FRESHNESS_OR_SEQUENCE"
    score = _probe_score(row)
    hazard = _f(row.get("hazard_score"))
    align = _f(row.get("alignment_score"))
    buy = _f(row.get("buy_ratio"), 0.5)
    rv = _f(row.get("relative_volume_10s"))
    ta = _f(row.get("trade_acceleration"))
    ofi = _f(row.get("ofi"))
    obi = _f(row.get("obi"))
    delta = _f(row.get("change_point_delta"))

    if ofi <= ABORT_OFI or obi <= ABORT_OBI or buy < ABORT_BUY_RATIO:
        return False, "NEGATIVE_FLOW_OR_BOOK"

    normal = (
        score >= PROBE_SCORE
        and hazard >= PROBE_HAZARD
        and align >= PROBE_ALIGNMENT
        and buy >= 0.60
        and (rv >= 1.8 or ta >= 2.2)
        and ofi >= -0.02
        and obi >= -0.05
        and (ofi > 0.0 or obi > 0.0)
    )
    exceptional = (
        hazard >= 72.0
        and delta >= 10.0
        and buy >= 0.75
        and (rv >= 4.0 or ta >= 5.0)
        and ofi > -0.05
        and obi > -0.08
    )
    if normal or exceptional:
        return True, "EXCEPTIONAL_ACCELERATION" if exceptional and not normal else "ORDERED_EARLY_SEQUENCE"
    return False, "PROBE_THRESHOLD"


def _abort_gate(row, mem):
    if not _fresh(row):
        return False, "DATA_WAIT"
    ofi = _f(row.get("ofi"))
    obi = _f(row.get("obi"))
    buy = _f(row.get("buy_ratio"), 0.5)
    hazard = _f(row.get("hazard_score"))
    peak_hazard = _f(mem.get("peak_hazard"))
    hard_reversal = ofi <= ABORT_OFI and obi <= ABORT_OBI
    broad_reversal = buy < ABORT_BUY_RATIO and (ofi < 0 or obi < 0)
    hazard_collapse = peak_hazard > 0 and (peak_hazard - hazard) >= ABORT_HAZARD_DROP and ofi <= 0 and obi <= 0
    if hard_reversal:
        return True, "OFI_OBI_REVERSAL"
    if broad_reversal:
        return True, "BUY_FLOW_COLLAPSE"
    if hazard_collapse:
        return True, "HAZARD_COLLAPSE"
    return False, ""


def _mark_stage_times(mem, row, now):
    if not mem["first_seen_ms"]:
        mem["first_seen_ms"] = now
    if not mem["activity_ms"] and (_f(row.get("relative_volume_10s")) >= 1.5 or _f(row.get("trade_acceleration")) >= 1.75):
        mem["activity_ms"] = now
    if not mem["flow_ms"] and _f(row.get("buy_ratio"), 0.5) >= 0.58 and _f(row.get("ofi")) > 0:
        mem["flow_ms"] = now
    if not mem["book_ms"] and _f(row.get("obi")) > 0:
        mem["book_ms"] = now
    if not mem["thrust_ms"] and (_f(row.get("hazard_score")) >= 68 or _f(row.get("change_point_delta")) >= 8):
        mem["thrust_ms"] = now


def _persist_outcome(record):
    if not OUTCOME_PATH:
        return
    try:
        directory = os.path.dirname(OUTCOME_PATH)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if os.path.exists(OUTCOME_PATH) and os.path.getsize(OUTCOME_PATH) >= OUTCOME_MAX_BYTES:
            try:
                os.replace(OUTCOME_PATH, OUTCOME_PATH + ".1")
            except Exception:
                pass
        with open(OUTCOME_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        _stats["outcomes_written"] += 1
    except Exception:
        _stats["outcome_write_errors"] += 1


def _update_outcome(symbol, row, mem, now):
    price = _f(row.get("entry_reference"), _f(row.get("price")))
    if price <= 0:
        return
    if mem["probe_price"] > 0:
        mem["peak_price"] = max(_f(mem["peak_price"], price), price)
        trough = _f(mem["trough_price"], price)
        mem["trough_price"] = min(trough if trough > 0 else price, price)
        age = now - int(mem["probe_ms"] or now)
        for window in OUTCOME_WINDOWS_MS:
            if age < window or window in mem["resolved_windows"]:
                continue
            entry = _f(mem["probe_price"])
            mfe = (mem["peak_price"] / entry - 1.0) * 100.0 if entry > 0 else 0.0
            mae = (mem["trough_price"] / entry - 1.0) * 100.0 if entry > 0 else 0.0
            _persist_outcome({
                "revision": REVISION,
                "symbol": symbol,
                "probe_ms": mem["probe_ms"],
                "window_ms": window,
                "entry": entry,
                "last": price,
                "mfe_pct": round(mfe, 4),
                "mae_pct": round(mae, 4),
                "peak_probe_score": round(_f(mem["peak_probe_score"]), 2),
                "peak_hazard": round(_f(mem["peak_hazard"]), 2),
                "stage_times": {
                    "activity": mem["activity_ms"],
                    "flow": mem["flow_ms"],
                    "book": mem["book_ms"],
                    "thrust": mem["thrust_ms"],
                },
            })
            mem["resolved_windows"].add(window)


def _enrich_probe(row):
    sym = str(row.get("symbol") or "").upper()
    if not sym:
        return row
    now = int(time.time() * 1000)
    mem = _memory[sym]
    _mark_stage_times(mem, row, now)
    score = _probe_score(row)
    mem["peak_hazard"] = max(_f(mem["peak_hazard"]), _f(row.get("hazard_score")))
    mem["peak_probe_score"] = max(_f(mem["peak_probe_score"]), score)
    price = _f(row.get("entry_reference"), _f(row.get("price")))
    mem["snapshots"].append({
        "ts": now,
        "score": round(score, 2),
        "hazard": round(_f(row.get("hazard_score")), 2),
        "buy": round(_f(row.get("buy_ratio"), 0.5), 4),
        "ofi": round(_f(row.get("ofi")), 4),
        "obi": round(_f(row.get("obi")), 4),
        "rv": round(_f(row.get("relative_volume_10s")), 3),
        "ta": round(_f(row.get("trade_acceleration")), 3),
        "price": price,
    })

    abort, abort_reason = _abort_gate(row, mem)
    qualifies, probe_reason = _probe_gate(row)
    state = "PROBE_WATCH"
    if abort and mem["probe_ms"]:
        state = "PROBE_ABORT"
        _sticky_until.pop(sym, None)
        _stats["probe_aborts"] += 1
    elif qualifies:
        state = "EARLY_PROBE"
        _sticky_until[sym] = max(_sticky_until.get(sym, 0.0), time.monotonic() + STICKY_SECONDS)
        if not mem["probe_ms"]:
            mem["probe_ms"] = now
            mem["probe_price"] = price
            mem["peak_price"] = price
            mem["trough_price"] = price
            _stats["new_probes"] += 1

    _update_outcome(sym, row, mem, now)
    entry = _f(mem.get("probe_price"))
    mfe = ((_f(mem.get("peak_price")) / entry - 1.0) * 100.0) if entry > 0 else 0.0
    mae = ((_f(mem.get("trough_price")) / entry - 1.0) * 100.0) if entry > 0 else 0.0

    row["v128_probe_score"] = round(score, 2)
    row["v128_probe_state"] = state
    row["v128_probe_reason"] = abort_reason if state == "PROBE_ABORT" else probe_reason
    row["v128_sticky"] = _sticky_until.get(sym, 0.0) > time.monotonic()
    row["v128_probe_position_fraction"] = PROBE_POSITION_FRACTION if state == "EARLY_PROBE" else 0.0
    row["v128_sequence_add_fraction"] = SEQUENCE_POSITION_FRACTION
    row["v128_probe_age_s"] = round(max(0, now - int(mem["probe_ms"])) / 1000.0, 1) if mem["probe_ms"] else 0.0
    row["v128_probe_mfe_pct"] = round(mfe, 3)
    row["v128_probe_mae_pct"] = round(mae, 3)
    row["v128_stage_times"] = {
        "activity_ms": mem["activity_ms"],
        "flow_ms": mem["flow_ms"],
        "book_ms": mem["book_ms"],
        "thrust_ms": mem["thrust_ms"],
        "probe_ms": mem["probe_ms"],
    }
    return row


def _rebuild_wrapper():
    rows = list(_original_rebuild() or [])
    for row in rows:
        _enrich_probe(row)
    rows.sort(key=lambda r: (
        str(r.get("v128_probe_state")) == "EARLY_PROBE",
        bool(r.get("v128_sticky")),
        _f(r.get("v128_probe_score")),
        _f(r.get("sequence_score")),
        _f(r.get("hazard_score")),
    ), reverse=True)
    V125._latest_candidates = rows
    _stats["early_probe"] = sum(r.get("v128_probe_state") == "EARLY_PROBE" for r in rows)
    _stats["probe_watch"] = sum(r.get("v128_probe_state") == "PROBE_WATCH" for r in rows)
    return rows


def _sticky_symbols():
    now = time.monotonic()
    expired = [s for s, until in _sticky_until.items() if until <= now]
    for s in expired:
        _sticky_until.pop(s, None)
    rows = {str(r.get("symbol") or "").upper(): r for r in list(getattr(V125, "_latest_candidates", []) or [])}
    active = [s for s in _sticky_until if s in rows]
    active.sort(key=lambda s: (
        _f(rows[s].get("v128_probe_score")),
        _f(rows[s].get("sequence_score")),
        _f(rows[s].get("hazard_score")),
    ), reverse=True)
    return active[:STICKY_SLOTS]


def _promotion_wrapper():
    try:
        base = list(_original_promotion_symbols() or [])
    except Exception:
        base = []
    sticky = _sticky_symbols()
    out = []
    limit = max(STICKY_SLOTS, int(getattr(V125, "EARLY_PROMOTION_SLOTS", 16)))
    for sym in sticky + base:
        sym = str(sym or "").upper()
        if sym and sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    _stats["sticky_promoted"] = sum(1 for s in sticky if s in out)
    return out


def probe_candidates(limit=20):
    rows = list(getattr(V125, "_latest_candidates", []) or [])
    rows = [r for r in rows if r.get("v128_probe_state") in {"EARLY_PROBE", "PROBE_WATCH"}]
    rows.sort(key=lambda r: (
        r.get("v128_probe_state") == "EARLY_PROBE",
        bool(r.get("v128_sticky")),
        _f(r.get("v128_probe_score")),
    ), reverse=True)
    return rows[:limit]


def _augment_response(response):
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    data["v12_8_early_entry"] = {
        "revision": REVISION,
        "role": ROLE,
        "strict_buy_authority_unchanged": True,
        "freshness_ms": FRESH_MS,
        "probe_threshold": PROBE_SCORE,
        "sticky_seconds": STICKY_SECONDS,
        "sticky_symbols": _sticky_symbols(),
        "early_probe_count": _stats.get("early_probe", 0),
        "probe_aborts": _stats.get("probe_aborts", 0),
        "outcomes_written": _stats.get("outcomes_written", 0),
        "position_model": {
            "early_probe_fraction": PROBE_POSITION_FRACTION,
            "sequence_add_fraction": SEQUENCE_POSITION_FRACTION,
            "strict_confirmation_remainder": round(max(0.0, 1.0 - PROBE_POSITION_FRACTION - SEQUENCE_POSITION_FRACTION), 3),
        },
        "candidates": probe_candidates(20),
        "rule": "EARLY_PROBE is an advisory staged-entry state only. It never changes or bypasses strict BUY_NOW, freshness, sequence validity, spread/slippage, regime, or risk-plan safety.",
    }
    data["upgrade_revision_v12_8"] = REVISION
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
        top = probe_candidates(8)
        print(
            f"PSI-V12.8 EARLY-PROBE probes={_stats.get('early_probe',0)} "
            f"sticky={_sticky_symbols()} aborts={_stats.get('probe_aborts',0)} "
            f"outcomes={_stats.get('outcomes_written',0)} "
            f"top={[(r.get('symbol'),r.get('v128_probe_state'),round(_f(r.get('v128_probe_score')),1)) for r in top]} "
            "strictBuyAuthority=UNCHANGED",
            flush=True,
        )


def install(core, v125, v127):
    global CORE, V125, V127
    global _original_rebuild, _original_promotion_symbols, _original_scan, _original_health
    if CORE is not None:
        return
    CORE = core
    V125 = v125
    V127 = v127
    _original_rebuild = v125._rebuild_candidates
    _original_promotion_symbols = v125._promotion_symbols
    _original_scan = core.v12_scan
    _original_health = core.v12_health

    v125._rebuild_candidates = _rebuild_wrapper
    v125._promotion_symbols = _promotion_wrapper
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        f"PSI-V12.8 installed revision={REVISION} earlyProbe=ADVISORY stickyMicro={int(STICKY_SECONDS)}s "
        f"fastAbort=ON missedMoveLearning=ON strictBuyAuthority=UNCHANGED",
        flush=True,
    )
