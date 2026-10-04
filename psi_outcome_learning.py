import asyncio
import hashlib
import json
import math
import os
import time
from collections import defaultdict, deque

import redis.asyncio as redis_async

REVISION = "12.3.4-outcome-memory-probability-shadow-v2"
ROLE = "SHADOW_CALIBRATION_ONLY"
EXECUTION_AUTHORITY = False

CORE = None
HARDENING = None

OPEN_KEY = os.getenv("PSI_ML_OPEN_KEY", "psi:v12:ml:open:v1").strip()
RESOLVED_KEY = os.getenv("PSI_ML_RESOLVED_KEY", "psi:v12:ml:resolved:v1").strip()
STATS_KEY = os.getenv("PSI_ML_STATS_KEY", "psi:v12:ml:stats:v1").strip()
META_KEY = os.getenv("PSI_ML_META_KEY", "psi:v12:ml:meta:v1").strip()

# Slightly larger learning budget than the legacy 1,000-outcome in-process deque.
MAX_OPEN = max(250, min(int(os.getenv("PSI_ML_MAX_OPEN_EVENTS", "2000")), 5000))
MAX_RESOLVED = max(2000, min(int(os.getenv("PSI_ML_MAX_RESOLVED", "20000")), 100000))
MAX_RECENT = max(500, min(int(os.getenv("PSI_ML_RECENT_RESOLVED", "5000")), 20000))
MAX_NEW_PER_CYCLE = max(4, min(int(os.getenv("PSI_ML_MAX_NEW_PER_CYCLE", "30")), 100))
POLL_SECONDS = max(2.0, float(os.getenv("PSI_ML_POLL_SECONDS", "5.0")))
SIGNAL_BUCKET_SECONDS = max(60, int(os.getenv("PSI_ML_SIGNAL_BUCKET_SECONDS", "900")))
ANALYSIS_STOP_PCT = max(0.5, min(float(os.getenv("PSI_ML_ANALYSIS_STOP_PCT", "2.0")), 10.0))
MIN_CALIBRATION_SAMPLES = max(10, int(os.getenv("PSI_ML_MIN_CALIBRATION_SAMPLES", "30")))
CLEAN_MAE_PCT = max(0.25, min(float(os.getenv("PSI_ML_CLEAN_MAE_PCT", "1.5")), 5.0))
MIN_70_TOTAL_SAMPLES = max(100, int(os.getenv("PSI_ML_MIN_70_TOTAL_SAMPLES", "300")))
MIN_70_TEST_SAMPLES = max(30, int(os.getenv("PSI_ML_MIN_70_TEST_SAMPLES", "75")))
DAY_MS = 24 * 60 * 60 * 1000

TARGETS = (3.0, 5.0, 10.0, 20.0, 40.0)
HORIZONS_MS = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "24h": 24 * 60 * 60 * 1000,
}
MAX_AGE_MS = HORIZONS_MS["24h"]

_open = {}
_recent = deque(maxlen=MAX_RECENT)
_stats = defaultdict(dict)
_seen = set()
_last_error = ""
_last_diag_mono = 0.0
_bootstrapped = False


def _f(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _now_ms():
    return int(time.time() * 1000)


def install(core, hardening=None):
    global CORE, HARDENING
    CORE = core
    HARDENING = hardening


def _core():
    if CORE is None:
        raise RuntimeError("outcome learning not installed")
    return CORE


def _current_price(symbol):
    core = _core()
    symbol = str(symbol or "").upper()
    try:
        p = _f(core.app.current_symbol_price(symbol))
        if p > 0:
            return p
    except Exception:
        pass
    try:
        mm = core.app.micro_metrics(symbol) or {}
        p = _f(mm.get("last_price"), _f(mm.get("price")))
        if p > 0:
            return p
    except Exception:
        pass
    try:
        row = (getattr(core.q, "latest", {}) or {}).get(symbol) or {}
        for key in ("price", "last_price", "current", "mark_price"):
            p = _f(row.get(key))
            if p > 0:
                return p
    except Exception:
        pass
    try:
        row = next(
            (r for r in core._board() if str(r.get("symbol") or "").upper() == symbol),
            {},
        )
        for key in ("current", "price", "entry"):
            p = _f(row.get(key))
            if p > 0:
                return p
    except Exception:
        pass
    return 0.0


def _rapid_score(row):
    row = row or {}
    rapid = row.get("rapid_ignition") or {}
    return max(_f(rapid.get("score")), _f(row.get("rapid_score")))


def _lowcap(symbol):
    if HARDENING is None:
        return {}
    try:
        row = HARDENING._lowcap_signal(symbol) or {}
    except Exception:
        return {}
    return {
        "lowcap_score": _f(row.get("score")),
        "lowcap_state": str(row.get("state") or ""),
        "lowcap_cap_source": str(row.get("cap_source") or ""),
        "lowcap_cap_band": str(row.get("cap_band") or ""),
        "lowcap_components": dict(row.get("components") or {}),
    }


def _features(symbol, structural, legacy):
    core = _core()
    structural = structural or {}
    legacy = legacy or {}
    try:
        mm = core.app.micro_metrics(symbol) or {}
    except Exception:
        mm = {}
    try:
        tape = core.tape.tape_metric(symbol) or {}
    except Exception:
        tape = {}

    blockers = list(structural.get("execution_blockers") or [])
    blockers += list(legacy.get("combined_blockers") or [])
    blockers += list(legacy.get("pinpoint_blockers") or [])
    blockers = list(dict.fromkeys(str(x) for x in blockers if str(x)))

    out = {
        "setup": str(
            structural.get("setup")
            or legacy.get("pinpoint_setup")
            or legacy.get("active_setup")
            or "UNKNOWN"
        ),
        "structural_state": str(structural.get("state") or ""),
        "execution_state": str(structural.get("execution_state") or ""),
        "pinpoint_state": str(
            legacy.get("pinpoint_state") or structural.get("pinpoint_state") or ""
        ),
        "pinpoint_entry_status": str(
            legacy.get("pinpoint_entry_status")
            or structural.get("pinpoint_entry_status")
            or ""
        ),
        "persistence": int(
            _f(
                legacy.get("pinpoint_persistence_passes"),
                _f(structural.get("execution_persistence_passes")),
            )
        ),
        "setup_strength": _f(
            structural.get("setup_strength"), _f(structural.get("strength"))
        ),
        "trend_regime": str(
            structural.get("trend_regime") or legacy.get("trend_regime") or ""
        ),
        "anti_chase": bool(structural.get("anti_chase")) or ("ANTI_CHASE" in blockers),
        "extension_blocked": "CUMULATIVE_EXTENSION_GUARD" in blockers,
        "blockers": blockers[:32],
        "rapid_score": _rapid_score(legacy),
        "micro_ready": bool(mm.get("micro_ready")),
        "sequence_verified": bool(mm.get("sequence_verified")),
        "book_sequence_verified": bool(mm.get("book_sequence_verified")),
        "buy_ratio": _f(
            tape.get("buy_ratio_1s"), _f(mm.get("aggressive_buy_ratio"), 0.5)
        ),
        "cvd": _f(tape.get("cvd_1s"), _f(mm.get("cvd_quote_60s"))),
        "cvd_accel": _f(
            tape.get("cvd_accel"), _f(mm.get("cvd_acceleration"))
        ),
        "ofi": _f(mm.get("ofi")),
        "ofi_accel": _f(mm.get("ofi_acceleration")),
        "obi": _f(mm.get("obi"), _f(tape.get("bbo_imbalance"))),
        "ask_depletion": _f(mm.get("ask_depletion")),
        "spread_bps": _f(mm.get("spread_bps"), 9999.0),
        "slippage_bps": _f(mm.get("slippage_bps"), 9999.0),
        "relative_volume_30s": _f(mm.get("relative_volume_30s")),
        "trade_acceleration": _f(mm.get("trade_acceleration")),
        "trade_size_shift": _f(mm.get("trade_size_shift")),
        "vwap_reclaim": bool(mm.get("vwap_reclaim")),
        "price_velocity_5s_pct": _f(tape.get("price_velocity_5s_pct")),
    }
    out.update(_lowcap(symbol))
    return out


def _signal_class(structural, legacy, features):
    structural = structural or {}
    legacy = legacy or {}
    if structural.get("execution_state") == "BUY NOW" or structural.get("buy_now") is True:
        return "BUY_NOW"
    status = str(
        legacy.get("pinpoint_entry_status")
        or structural.get("pinpoint_entry_status")
        or ""
    )
    if status == "PINPOINT_TRIGGERED":
        return "PINPOINT_TRIGGERED"
    if status == "PINPOINT_ARMED":
        return "PINPOINT_ARMED"
    if str(legacy.get("pinpoint_state") or "") in {"PRE-IGNITION", "SETUP READY"}:
        return "PINPOINT_PRE"
    if structural.get("state") == "BUY":
        return "STRUCTURAL_BUY"
    if structural.get("state") == "ARMED":
        return "STRUCTURAL_ARMED"
    if _f(features.get("lowcap_score")) >= 45.0:
        return "LOWCAP_EARLY"
    if _f(features.get("rapid_score")) >= 85.0:
        return "RAPID_EARLY"
    return ""


def _entry_stop(symbol, structural, legacy):
    structural = structural or {}
    legacy = legacy or {}
    current = _current_price(symbol)
    trigger = _f(legacy.get("pinpoint_trigger"))
    entry = trigger if trigger > 0 else _f(structural.get("entry"))
    if entry <= 0:
        entry = current
    stop = _f(legacy.get("pinpoint_stop"), _f(structural.get("stop")))
    stop_source = "EXPLICIT"
    if entry > 0 and (stop <= 0 or stop >= entry):
        stop = entry * (1.0 - ANALYSIS_STOP_PCT / 100.0)
        stop_source = "ANALYSIS_FALLBACK"
    return entry, stop, stop_source, current


def _cohort(features, signal_class):
    setup = str(features.get("setup") or "UNKNOWN")
    micro = "M1" if features.get("micro_ready") else "M0"
    flow = (
        "F1"
        if (
            _f(features.get("buy_ratio")) >= 0.58
            and _f(features.get("cvd_accel")) > 0
            and _f(features.get("ofi_accel")) >= 0
        )
        else "F0"
    )
    book = "B1" if _f(features.get("obi")) >= 0.15 else "B0"
    persist = "P2" if int(features.get("persistence") or 0) >= 2 else "P0"
    chase = (
        "C0"
        if not features.get("anti_chase") and not features.get("extension_blocked")
        else "C1"
    )
    return "|".join((setup, signal_class, micro, flow, book, persist, chase))


def _dataset_split(created_ms):
    # Time-blocked 20-day cycle: 14 train, 3 validation, 3 untouched test.
    # This avoids random leakage from adjacent market regimes while keeping
    # enough fresh shadow samples flowing into every split.
    day_bucket = int(created_ms // DAY_MS) % 20
    if day_bucket < 14:
        return "TRAIN"
    if day_bucket < 17:
        return "VALIDATION"
    return "TEST"


def _event_id(symbol, setup, signal_class, now_ms):
    bucket = int(now_ms // (SIGNAL_BUCKET_SECONDS * 1000))
    raw = f"{symbol}|{setup}|{signal_class}|{bucket}".encode()
    return hashlib.sha1(raw).hexdigest()[:20]


def _new_event(symbol, structural, legacy, now_ms=None):
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    features = _features(symbol, structural, legacy)
    signal_class = _signal_class(structural, legacy, features)
    if not signal_class:
        return None
    entry, stop, stop_source, current = _entry_stop(symbol, structural, legacy)
    if entry <= 0:
        return None
    setup = str(features.get("setup") or "UNKNOWN")
    return {
        "id": _event_id(symbol, setup, signal_class, now_ms),
        "symbol": symbol,
        "setup": setup,
        "signal_class": signal_class,
        "cohort": _cohort(features, signal_class),
        "created_ms": now_ms,
        "entry_price": entry,
        "observed_price_at_signal": current or entry,
        "stop_price": stop,
        "stop_source": stop_source,
        "features": features,
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "peak_price": entry,
        "trough_price": entry,
        "first_target_ms": {},
        "mae_at_target": {},
        "stop_hit_ms": 0,
        "horizon_returns": {},
        "resolved": False,
        "resolution": "OPEN",
        "dataset_split": _dataset_split(now_ms),
        "role": ROLE,
        "execution_authority": False,
    }


def _update_event(event, price, now_ms=None):
    now_ms = _now_ms() if now_ms is None else int(now_ms)
    entry = _f(event.get("entry_price"))
    if entry <= 0 or price <= 0:
        return event

    ret = (price / entry - 1.0) * 100.0
    event["mfe_pct"] = max(_f(event.get("mfe_pct")), ret)
    event["mae_pct"] = min(_f(event.get("mae_pct")), ret)
    event["peak_price"] = max(_f(event.get("peak_price"), entry), price)
    event["trough_price"] = min(_f(event.get("trough_price"), entry), price)

    hits = event.setdefault("first_target_ms", {})
    for target in TARGETS:
        key = str(int(target))
        if key not in hits and ret >= target:
            hits[key] = now_ms
            event.setdefault("mae_at_target", {})[key] = _f(event.get("mae_pct"))

    stop = _f(event.get("stop_price"))
    if not int(event.get("stop_hit_ms") or 0) and stop > 0 and price <= stop:
        event["stop_hit_ms"] = now_ms

    age = now_ms - int(event.get("created_ms") or now_ms)
    horizons = event.setdefault("horizon_returns", {})
    for label, ms in HORIZONS_MS.items():
        if age >= ms and label not in horizons:
            horizons[label] = ret

    if int(event.get("stop_hit_ms") or 0):
        event["resolved"] = True
        event["resolution"] = "STOP_FIRST"
    elif int(hits.get("40") or 0):
        event["resolved"] = True
        event["resolution"] = "TARGET_40"
    elif age >= MAX_AGE_MS:
        event["resolved"] = True
        event["resolution"] = "TIME_24H"
    return event


def _target_before_stop(event, target):
    hit = int(
        (event.get("first_target_ms") or {}).get(str(int(target))) or 0
    )
    stop = int(event.get("stop_hit_ms") or 0)
    if hit:
        return (not stop) or hit <= stop
    if stop or event.get("resolved"):
        return False
    return None


def _clean_target_before_stop(event, target):
    result = _target_before_stop(event, target)
    if result is not True:
        return result
    key = str(int(target))
    mae_at_hit = _f((event.get("mae_at_target") or {}).get(key), _f(event.get("mae_pct")))
    return mae_at_hit >= -CLEAN_MAE_PCT


def _blank_stat():
    return {
        "n": 0,
        "stop_first": 0,
        "targets": {
            str(int(t)): {"win": 0, "loss": 0, "clean_win": 0, "clean_loss": 0}
            for t in TARGETS
        },
    }


def _stat(key):
    row = _stats.get(key)
    if not isinstance(row, dict) or "targets" not in row:
        row = _blank_stat()
        _stats[key] = row
    return row


def _apply_resolution(event):
    split = str(event.get("dataset_split") or "TRAIN")
    base_keys = [
        "GLOBAL",
        f"SETUP::{event.get('setup')}",
        f"CLASS::{event.get('signal_class')}",
        f"COHORT::{event.get('cohort')}",
    ]
    keys = list(base_keys) + [f"SPLIT::{split}::{key}" for key in base_keys]
    for blocker in list((event.get("features") or {}).get("blockers") or [])[:16]:
        keys.append(f"BLOCKER::{blocker}")

    for key in keys:
        row = _stat(key)
        row["n"] = int(row.get("n") or 0) + 1
        if event.get("resolution") == "STOP_FIRST":
            row["stop_first"] = int(row.get("stop_first") or 0) + 1
        for target in TARGETS:
            tkey = str(int(target))
            result = _target_before_stop(event, target)
            bucket = row["targets"].setdefault(tkey, {"win": 0, "loss": 0, "clean_win": 0, "clean_loss": 0})
            if result is True:
                bucket["win"] = int(bucket.get("win") or 0) + 1
            elif result is False:
                bucket["loss"] = int(bucket.get("loss") or 0) + 1
            clean = _clean_target_before_stop(event, target)
            if clean is True:
                bucket["clean_win"] = int(bucket.get("clean_win") or 0) + 1
            elif clean is False:
                bucket["clean_loss"] = int(bucket.get("clean_loss") or 0) + 1


def _posterior(win, loss):
    win = int(win or 0)
    loss = int(loss or 0)
    n = win + loss
    p = (win + 2.0) / (n + 4.0)  # weak Beta(2,2) prior
    se = math.sqrt(max(p * (1.0 - p), 1e-9) / max(n + 4.0, 1.0))
    lo = max(0.0, p - 1.96 * se)
    hi = min(1.0, p + 1.96 * se)
    if n >= 500:
        confidence = "STRONG"
    elif n >= 300:
        confidence = "GOOD"
    elif n >= 100:
        confidence = "USEFUL"
    elif n >= MIN_CALIBRATION_SAMPLES:
        confidence = "PRELIMINARY"
    else:
        confidence = "INSUFFICIENT"
    return {
        "samples": n,
        "probability": round(p, 4),
        "ci95": [round(lo, 4), round(hi, 4)],
        "confidence": confidence,
        "qualified_for_70pct_claim": bool(n >= MIN_70_TOTAL_SAMPLES and lo >= 0.70),
    }


def _calibration(key):
    row = _stats.get(key) or {}
    targets = row.get("targets") or {}
    return {
        "key": key,
        "samples": int(row.get("n") or 0),
        "stop_first": int(row.get("stop_first") or 0),
        "targets": {
            f"plus_{int(target)}_before_stop": {
                **_posterior(
                    (targets.get(str(int(target))) or {}).get("win"),
                    (targets.get(str(int(target))) or {}).get("loss"),
                ),
                "clean_entry": _posterior(
                    (targets.get(str(int(target))) or {}).get("clean_win"),
                    (targets.get(str(int(target))) or {}).get("clean_loss"),
                ),
            }
            for target in TARGETS
        },
    }


def _top(prefix, limit=8):
    keys = [k for k in _stats if k.startswith(prefix)]
    keys.sort(
        key=lambda k: int((_stats.get(k) or {}).get("n") or 0),
        reverse=True,
    )
    return [_calibration(k) for k in keys[:limit]]


def _validated_70_claim(key="GLOBAL", target=5.0, clean=True):
    overall = _calibration(key)
    test = _calibration(f"SPLIT::TEST::{key}")
    name = f"plus_{int(target)}_before_stop"
    overall_metric = (overall.get("targets") or {}).get(name) or {}
    test_metric = (test.get("targets") or {}).get(name) or {}
    if clean:
        overall_metric = overall_metric.get("clean_entry") or {}
        test_metric = test_metric.get("clean_entry") or {}
    test_samples = int(test_metric.get("samples") or 0)
    overall_samples = int(overall_metric.get("samples") or 0)
    test_lo = _f((test_metric.get("ci95") or [0.0])[0])
    overall_lo = _f((overall_metric.get("ci95") or [0.0])[0])
    qualified = bool(
        overall_samples >= MIN_70_TOTAL_SAMPLES
        and test_samples >= MIN_70_TEST_SAMPLES
        and overall_lo >= 0.70
        and test_lo >= 0.70
    )
    return {
        "qualified": qualified,
        "target_pct": target,
        "clean_entry_required": clean,
        "overall": overall_metric,
        "test": test_metric,
    }


def summary():
    return {
        "revision": REVISION,
        "role": ROLE,
        "execution_authority": False,
        "open_events": len(_open),
        "recent_resolved_in_memory": len(_recent),
        "max_open_budget": MAX_OPEN,
        "max_resolved_redis_budget": MAX_RESOLVED,
        "targets_pct": list(TARGETS),
        "horizons": list(HORIZONS_MS),
        "clean_entry_mae_limit_pct": CLEAN_MAE_PCT,
        "dataset_split_policy": "20-day time blocks: 14 TRAIN / 3 VALIDATION / 3 TEST",
        "global": _calibration("GLOBAL"),
        "test_global": _calibration("SPLIT::TEST::GLOBAL"),
        "setup_calibration": _top("SETUP::"),
        "signal_class_calibration": _top("CLASS::"),
        "test_setup_calibration": _top("SPLIT::TEST::SETUP::"),
        "blocker_outcomes": _top("BLOCKER::"),
        "validated_70pct_clean_plus5": _validated_70_claim("GLOBAL", 5.0, True),
        "claim_policy": (
            "70pct claim requires both overall and untouched TEST lower 95% "
            "confidence bounds >=70%, with >= configured minimum samples"
        ),
        "last_error": _last_error,
        "generated_ms": _now_ms(),
    }


def _candidate_rows():
    core = _core()
    try:
        board = list(core._board() or [])
    except Exception:
        board = []
    structural = {
        str(r.get("symbol") or "").upper(): r
        for r in board
        if r.get("symbol")
    }
    latest = getattr(core.q, "latest", {}) or {}
    symbols = set(structural)
    symbols.update(str(s).upper() for s in list(getattr(core.q, "universe", []) or []))

    out = []
    for symbol in symbols:
        sr = structural.get(symbol) or {}
        lr = latest.get(symbol) or {}
        interesting = (
            sr.get("state") in {"BUY", "ARMED"}
            or sr.get("execution_state") in {
                "BUY NOW", "EXECUTION_ARMED", "COLLECTING DATA"
            }
            or str(lr.get("pinpoint_entry_status") or "") in {
                "PINPOINT_TRIGGERED", "PINPOINT_ARMED"
            }
            or str(lr.get("pinpoint_state") or "") in {
                "PRE-IGNITION", "SETUP READY", "BUY NOW"
            }
            or _rapid_score(lr) >= 85.0
        )
        if not interesting and HARDENING is not None:
            try:
                interesting = _f(HARDENING._lowcap_signal(symbol).get("score")) >= 45.0
            except Exception:
                pass
        if interesting:
            out.append((symbol, sr, lr))
    return out


async def _save_open(client, event):
    await client.hset(
        OPEN_KEY,
        event["id"],
        json.dumps(event, separators=(",", ":"), sort_keys=True),
    )


async def _save_stats(client):
    await client.set(
        STATS_KEY,
        json.dumps(dict(_stats), separators=(",", ":"), sort_keys=True),
    )


async def _resolve(client, event):
    _apply_resolution(event)
    _recent.append(event)
    await client.hdel(OPEN_KEY, event["id"])
    await client.lpush(
        RESOLVED_KEY,
        json.dumps(event, separators=(",", ":"), sort_keys=True),
    )
    await client.ltrim(RESOLVED_KEY, 0, MAX_RESOLVED - 1)


async def bootstrap():
    global _bootstrapped, _last_error
    core = _core()
    if not core.REDIS_URL:
        _bootstrapped = True
        return

    client = redis_async.from_url(
        core.REDIS_URL, encoding="utf-8", decode_responses=True
    )
    try:
        await client.ping()
        raw = await client.get(STATS_KEY)
        if raw:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                for key, value in loaded.items():
                    if isinstance(value, dict):
                        _stats[key] = value

        raw_open = await client.hgetall(OPEN_KEY)
        for event_id, payload in list(raw_open.items())[:MAX_OPEN]:
            try:
                event = json.loads(payload)
            except Exception:
                continue
            if isinstance(event, dict) and not event.get("resolved"):
                _open[event_id] = event
                _seen.add(event_id)

        recent = await client.lrange(
            RESOLVED_KEY, 0, min(MAX_RECENT, 1000) - 1
        )
        for payload in reversed(recent):
            try:
                event = json.loads(payload)
            except Exception:
                continue
            if isinstance(event, dict):
                _recent.append(event)

        await client.set(
            META_KEY,
            json.dumps(
                {
                    "revision": REVISION,
                    "role": ROLE,
                    "execution_authority": False,
                    "boot_ms": _now_ms(),
                },
                separators=(",", ":"),
            ),
        )
        print(
            f"Ψ-ML OUTCOME_MEMORY restored open={len(_open)} "
            f"recentResolved={len(_recent)} revision={REVISION}",
            flush=True,
        )
    except Exception as exc:
        _last_error = f"bootstrap:{type(exc).__name__}:{exc}"
        print(f"Ψ-ML OUTCOME_MEMORY bootstrap_error={_last_error}", flush=True)
    finally:
        await client.aclose()
        _bootstrapped = True


async def supervisor_loop():
    global _last_error, _last_diag_mono
    core = _core()
    if not _bootstrapped:
        await bootstrap()

    while True:
        client = None
        try:
            if not core.REDIS_URL:
                await asyncio.sleep(POLL_SECONDS)
                continue
            client = redis_async.from_url(
                core.REDIS_URL, encoding="utf-8", decode_responses=True
            )
            await client.ping()

            created = 0
            for symbol, structural, legacy in _candidate_rows():
                if created >= MAX_NEW_PER_CYCLE or len(_open) >= MAX_OPEN:
                    break
                event = _new_event(symbol, structural, legacy)
                if (
                    not event
                    or event["id"] in _open
                    or event["id"] in _seen
                ):
                    continue
                _open[event["id"]] = event
                _seen.add(event["id"])
                await _save_open(client, event)
                created += 1

            resolved_ids = []
            for event_id, event in list(_open.items()):
                price = _current_price(event.get("symbol"))
                if price <= 0:
                    continue
                _update_event(event, price)
                if event.get("resolved"):
                    await _resolve(client, event)
                    resolved_ids.append(event_id)
                else:
                    await _save_open(client, event)

            for event_id in resolved_ids:
                _open.pop(event_id, None)
            if resolved_ids:
                await _save_stats(client)

            now_mono = time.monotonic()
            if now_mono - _last_diag_mono >= 30.0:
                _last_diag_mono = now_mono
                s = summary()
                p5 = s["global"]["targets"]["plus_5_before_stop"]
                p10 = s["global"]["targets"]["plus_10_before_stop"]
                clean5 = p5.get("clean_entry") or {}
                test5 = (s.get("test_global", {}).get("targets", {}).get("plus_5_before_stop", {}).get("clean_entry") or {})
                print(
                    "Ψ-ML CALIBRATION "
                    f"open={len(_open)} resolvedRecent={len(_recent)} "
                    f"n5={p5['samples']} p5={100*_f(p5['probability']):.1f}% "
                    f"n10={p10['samples']} p10={100*_f(p10['probability']):.1f}% "
                    f"clean5={100*_f(clean5.get('probability')):.1f}% "
                    f"testN5={int(test5.get('samples') or 0)} testClean5={100*_f(test5.get('probability')):.1f}% "
                    f"new={created} resolvedNow={len(resolved_ids)} "
                    f"role={ROLE}",
                    flush=True,
                )

            await asyncio.sleep(POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _last_error = f"loop:{type(exc).__name__}:{exc}"
            print(f"Ψ-ML OUTCOME_MEMORY error={_last_error}", flush=True)
            await asyncio.sleep(max(2.0, POLL_SECONDS))
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass
