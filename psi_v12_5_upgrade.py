import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

import redis.asyncio as redis_async

REVISION = "12.5.0-full-universe-sensor+early-hazard+pinpoint-promotion"
ROLE = "EARLY_PREDICTION_ADVISORY"
STRICT_BUY_AUTHORITY_UNCHANGED = True

CORE = None
HARDENING = None
LEARNER = None
V124 = None

SENSOR_SHARDS = max(1, min(int(os.getenv("PSI_V125_SENSOR_SHARDS", "4")), 8))
POLL_SECONDS = max(0.25, float(os.getenv("PSI_V125_SENSOR_POLL_SECONDS", "0.5")))
DIAG_SECONDS = max(5.0, float(os.getenv("PSI_V125_DIAG_SECONDS", "10")))
TRAIN_SAMPLE_SECONDS = max(15.0, float(os.getenv("PSI_V125_TRAIN_SAMPLE_SECONDS", "60")))
TRAIN_PATH = os.getenv("PSI_V125_TRAIN_PATH", "/data/psi_v12_5_training.jsonl").strip()
TRAIN_MAX_BYTES = max(10_000_000, int(os.getenv("PSI_V125_TRAIN_MAX_BYTES", "750000000")))
EARLY_WATCH_SCORE = max(50.0, float(os.getenv("PSI_V125_EARLY_WATCH_SCORE", "65")))
EARLY_ARMED_SCORE = max(EARLY_WATCH_SCORE, float(os.getenv("PSI_V125_EARLY_ARMED_SCORE", "78")))
EARLY_PINPOINT_SCORE = max(EARLY_ARMED_SCORE, float(os.getenv("PSI_V125_EARLY_PINPOINT_SCORE", "88")))
EARLY_PROMOTION_SLOTS = max(4, min(int(os.getenv("PSI_V125_PROMOTION_SLOTS", "16")), 32))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_V125_MAX_SPREAD_BPS", "20")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_V125_MAX_SLIPPAGE_BPS", "35")))
MAX_TRADE_AGE_MS = max(500, int(os.getenv("PSI_V125_MAX_TRADE_AGE_MS", "3000")))
MAX_BOOK_AGE_MS = max(250, int(os.getenv("PSI_V125_MAX_BOOK_AGE_MS", "1500")))

_sensor_cache = {}
_score_history = defaultdict(lambda: deque(maxlen=40))
_latest_candidates = []
_stats = defaultdict(int)
_last_error = ""
_last_diag_mono = 0.0
_last_train_mono = 0.0
_bootstrapped = False

_original_scan = None
_original_health = None
_original_micro = None
_original_priority = None
_original_features = None
_original_cohort = None
_original_candidates = None
_original_signal_class = None


def _f(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def _above(value, base, span):
    return _clamp((_f(value) - base) / max(span, 1e-9))


def _positive(value, scale):
    return _clamp(_f(value) / max(scale, 1e-9))


def _core():
    if CORE is None:
        raise RuntimeError("V12.5 is not installed")
    return CORE


def _sensor_safety(row):
    blockers = []
    trade_age = int(_f(row.get("trade_age_ms"), 999999999))
    book_age = int(_f(row.get("book_age_ms"), 999999999))
    spread = _f(row.get("spread_bps"), 999999.0)
    slip = _f(row.get("slippage_bps"), 999999.0)
    if not row.get("trade_fresh") or trade_age > MAX_TRADE_AGE_MS:
        blockers.append("STALE_SENSOR_TRADE")
    if not row.get("book_fresh") or book_age > MAX_BOOK_AGE_MS:
        blockers.append("STALE_SENSOR_BOOK")
    if not row.get("sequence_verified"):
        blockers.append("TRADE_SEQUENCE_INVALID")
    if not row.get("book_sequence_verified"):
        blockers.append("BOOK_SEQUENCE_INVALID")
    if spread < 0 or spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD_FILTER")
    if slip < 0 or slip > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE_FILTER")
    if int(_f(row.get("trade_count_60s"))) < 3:
        blockers.append("INSUFFICIENT_TRADE_FLOW")
    if _f(row.get("last_price")) <= 0:
        blockers.append("INVALID_PRICE")
    return {"pass": not blockers, "blockers": blockers}


def _base_hazard(row):
    buy_ratio = _f(row.get("aggressive_buy_ratio"), 0.5)
    flow_persistence = _f(row.get("flow_persistence"), 0.5)
    cvd = _f(row.get("cvd_quote_60s"))
    cvd_acc = _f(row.get("cvd_acceleration"))
    rv10 = _f(row.get("relative_volume_10s"))
    rv30 = _f(row.get("relative_volume_30s"))
    trade_acc = _f(row.get("trade_acceleration"))
    size_shift = _f(row.get("trade_size_shift"))
    ofi = _f(row.get("ofi"))
    ofi_acc = _f(row.get("ofi_acceleration"))
    ofi_persist = _f(row.get("ofi_persistence"))
    obi = _f(row.get("obi"))
    ask_dep = _f(row.get("ask_depletion"))

    flow = (
        8.0 * _above(buy_ratio, 0.50, 0.25)
        + 5.0 * _above(flow_persistence, 0.50, 0.30)
        + (3.0 if cvd > 0 else 0.0)
        + (4.0 if cvd_acc > 0 else 0.0)
        + 5.0 * _positive(cvd_acc / max(abs(cvd), 1.0), 1.0)
    )
    activity = (
        10.0 * _above(rv10, 1.0, 3.0)
        + 8.0 * _above(rv30, 1.0, 2.5)
        + 7.0 * _above(trade_acc, 1.0, 2.0)
        + 5.0 * _above(size_shift, 1.0, 2.0)
    )
    book = (
        7.0 * _positive(ofi, 0.50)
        + 6.0 * _positive(ofi_acc, 0.40)
        + 4.0 * _above(ofi_persist, 0.50, 0.35)
        + 6.0 * _positive(obi, 0.40)
        + 7.0 * _positive(ask_dep, 0.10)
    )
    return _clamp(flow + activity + book, 0.0, 85.0)


def _hazard_row(symbol, row):
    base = _base_hazard(row)
    history = _score_history[symbol]
    prior = list(history)[-6:]
    prior_avg = sum(prior) / len(prior) if prior else base
    delta = base - prior_avg
    change = 15.0 * _positive(delta, 12.0)
    score = _clamp(base + change, 0.0, 100.0)
    history.append(base)

    safety = _sensor_safety(row)
    recent = list(history)[-4:]
    armed_persistence = sum(1 for x in reversed(recent) if x >= EARLY_ARMED_SCORE - 6.0)
    pinpoint_persistence = sum(1 for x in reversed(recent) if x >= EARLY_PINPOINT_SCORE - 8.0)

    state = "NONE"
    eta = "-"
    if safety["pass"] and score >= EARLY_PINPOINT_SCORE and pinpoint_persistence >= 2:
        state = "EARLY_PINPOINT"
        eta = "0-5m"
    elif safety["pass"] and score >= EARLY_ARMED_SCORE and armed_persistence >= 2:
        state = "EARLY_ARMED"
        eta = "5-15m"
    elif safety["pass"] and score >= EARLY_WATCH_SCORE:
        state = "EARLY_WATCH"
        eta = "15-60m"
    elif score >= EARLY_WATCH_SCORE:
        state = "DATA_WAIT"

    return {
        "symbol": symbol,
        "state": state,
        "hazard_score": round(score, 2),
        "base_score": round(base, 2),
        "change_point_delta": round(delta, 2),
        "eta_band": eta,
        "entry_reference": _f(row.get("last_price")),
        "entry_authority": False,
        "strict_buy_unchanged": True,
        "hard_sensor_safety": safety["pass"],
        "safety_blockers": safety["blockers"],
        "buy_ratio": round(_f(row.get("aggressive_buy_ratio"), 0.5), 4),
        "cvd_acceleration": round(_f(row.get("cvd_acceleration")), 4),
        "relative_volume_10s": round(_f(row.get("relative_volume_10s")), 3),
        "relative_volume_30s": round(_f(row.get("relative_volume_30s")), 3),
        "trade_acceleration": round(_f(row.get("trade_acceleration")), 3),
        "trade_size_shift": round(_f(row.get("trade_size_shift")), 3),
        "ofi": round(_f(row.get("ofi")), 4),
        "ofi_acceleration": round(_f(row.get("ofi_acceleration")), 4),
        "obi": round(_f(row.get("obi")), 4),
        "ask_depletion": round(_f(row.get("ask_depletion")), 4),
        "spread_bps": round(_f(row.get("spread_bps"), 999999.0), 3),
        "slippage_bps": round(_f(row.get("slippage_bps"), 999999.0), 3),
        "trade_age_ms": int(_f(row.get("trade_age_ms"), 999999999)),
        "book_age_ms": int(_f(row.get("book_age_ms"), 999999999)),
        "sequence_verified": bool(row.get("sequence_verified")),
        "book_sequence_verified": bool(row.get("book_sequence_verified")),
        "generated_ms": int(_f(row.get("_sensor_generated_ms"), 0)),
    }


def _rebuild_candidates():
    global _latest_candidates
    rows = []
    for symbol, raw in _sensor_cache.items():
        rows.append(_hazard_row(symbol, raw))
    rows.sort(
        key=lambda r: (
            r["hard_sensor_safety"],
            r["state"] == "EARLY_PINPOINT",
            r["state"] == "EARLY_ARMED",
            r["hazard_score"],
        ),
        reverse=True,
    )
    _latest_candidates = rows
    _stats["sensor_symbols"] = len(rows)
    _stats["sensor_safe"] = sum(1 for r in rows if r["hard_sensor_safety"])
    _stats["early_pinpoint"] = sum(1 for r in rows if r["state"] == "EARLY_PINPOINT")
    _stats["early_armed"] = sum(1 for r in rows if r["state"] == "EARLY_ARMED")
    _stats["early_watch"] = sum(1 for r in rows if r["state"] == "EARLY_WATCH")
    return rows


def early_candidates(limit=20, actionable_only=False):
    rows = _latest_candidates or _rebuild_candidates()
    if actionable_only:
        rows = [r for r in rows if r["state"] in {"EARLY_PINPOINT", "EARLY_ARMED", "EARLY_WATCH"}]
    return rows[:limit]


def _merge_sensor_payloads(payloads):
    merged = {}
    shards_live = 0
    now = int(time.time() * 1000)
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        generated = int(_f(payload.get("generated_ms"), 0))
        if generated <= 0 or now - generated > 15000:
            continue
        shards_live += 1
        for symbol, row in (payload.get("metrics") or {}).items():
            if not isinstance(row, dict):
                continue
            sym = str(symbol or "").upper()
            if not sym.endswith("USDT"):
                continue
            x = dict(row)
            x["_sensor_generated_ms"] = generated
            merged[sym] = x
    return merged, shards_live


async def _refresh_from_redis(client):
    global _sensor_cache
    keys = [f"psi:v12.5:sensor:{i}" for i in range(SENSOR_SHARDS)]
    raws = await client.mget(keys)
    payloads = []
    for raw in raws:
        if not raw:
            continue
        try:
            payloads.append(json.loads(raw))
        except Exception:
            continue
    merged, shards_live = _merge_sensor_payloads(payloads)
    if merged:
        _sensor_cache = merged
        _stats["sensor_shards_live"] = shards_live
        _stats["sensor_shards_expected"] = SENSOR_SHARDS
        _rebuild_candidates()
    return len(merged)


def _promotion_symbols():
    return [
        r["symbol"]
        for r in early_candidates(EARLY_PROMOTION_SLOTS, actionable_only=True)
        if r.get("hard_sensor_safety")
    ]


def promoted_micro_symbols():
    base = list(_original_micro() or [])
    universe = set(str(s).upper() for s in list(getattr(_core().q, "universe", []) or []))
    pool_size = int(getattr(_core(), "REDIS_MICRO_POOL_SIZE", max(40, len(base) or 40)))
    out = []
    for sym in _promotion_symbols() + base:
        sym = str(sym or "").upper()
        if sym in universe and sym not in out:
            out.append(sym)
        if len(out) >= pool_size:
            break
    _stats["deep_promoted"] = sum(1 for s in _promotion_symbols() if s in out)
    return out or base


def priority_symbols(universe):
    base = list(_original_priority(universe) or [])
    uset = set(universe or [])
    limit = int(getattr(_core(), "ACTIVE_SYMBOLS_PER_CYCLE", 8))
    promoted = [s for s in _promotion_symbols() if s in uset]
    out = []
    for sym in promoted[: max(1, limit // 2)] + base + promoted:
        if sym in uset and sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    _stats["structure_promoted"] = sum(1 for s in promoted if s in out)
    return out or base


def _candidate_map():
    return {r["symbol"]: r for r in _latest_candidates}


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
        h = _candidate_map().get(str(symbol).upper()) or {}
        out["early_hazard_score"] = _f(h.get("hazard_score"))
        out["early_hazard_state"] = str(h.get("state") or "")
        out["early_hazard_change"] = _f(h.get("change_point_delta"))
        out["sensor_hard_safe"] = bool(h.get("hard_sensor_safety"))
        return out

    def signal_class(structural, legacy, features_row):
        state = _original_signal_class(structural, legacy, features_row)
        if state:
            return state
        if _f(features_row.get("early_hazard_score")) >= EARLY_WATCH_SCORE:
            return "EARLY_HAZARD"
        return ""

    def cohort(features_row, signal_class_name):
        base = _original_cohort(features_row, signal_class_name)
        score = _f(features_row.get("early_hazard_score"))
        band = "HZ3" if score >= EARLY_PINPOINT_SCORE else "HZ2" if score >= EARLY_ARMED_SCORE else "HZ1" if score >= EARLY_WATCH_SCORE else "HZ0"
        safe = "SAFE1" if features_row.get("sensor_hard_safe") else "SAFE0"
        return f"{base}|{band}|{safe}"

    def candidate_rows():
        rows = list(_original_candidates() or [])
        seen = {str(r[0]).upper() for r in rows if r}
        smap = {}
        try:
            smap = {str(r.get("symbol") or "").upper(): r for r in (_core()._board() or [])}
        except Exception:
            pass
        latest = getattr(_core().q, "latest", {}) or {}
        for h in early_candidates(40, actionable_only=True):
            sym = h["symbol"]
            if sym not in seen:
                rows.append((sym, smap.get(sym) or {}, latest.get(sym) or {}))
                seen.add(sym)
        return rows

    LEARNER._features = features
    LEARNER._signal_class = signal_class
    LEARNER._cohort = cohort
    LEARNER._candidate_rows = candidate_rows


def _training_record():
    rows = []
    for r in _latest_candidates:
        raw = _sensor_cache.get(r["symbol"]) or {}
        rows.append({
            "s": r["symbol"],
            "hz": r["hazard_score"],
            "st": r["state"],
            "p": r["entry_reference"],
            "br": r["buy_ratio"],
            "ca": r["cvd_acceleration"],
            "rv10": r["relative_volume_10s"],
            "rv30": r["relative_volume_30s"],
            "ta": r["trade_acceleration"],
            "ts": r["trade_size_shift"],
            "ofi": r["ofi"],
            "oa": r["ofi_acceleration"],
            "obi": r["obi"],
            "ad": r["ask_depletion"],
            "sp": r["spread_bps"],
            "sl": r["slippage_bps"],
            "safe": r["hard_sensor_safety"],
            "tc": int(_f(raw.get("trade_count_60s"))),
        })
    return {"ts": int(time.time() * 1000), "revision": REVISION, "rows": rows}


def _persist_training():
    if not TRAIN_PATH or not _latest_candidates:
        return
    try:
        directory = os.path.dirname(TRAIN_PATH)
        if directory:
            os.makedirs(directory, exist_ok=True)
        if os.path.exists(TRAIN_PATH) and os.path.getsize(TRAIN_PATH) >= TRAIN_MAX_BYTES:
            rotated = TRAIN_PATH + ".1"
            try:
                os.replace(TRAIN_PATH, rotated)
            except Exception:
                pass
        with open(TRAIN_PATH, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(_training_record(), separators=(",", ":")) + "\n")
        _stats["training_snapshots"] += 1
    except Exception as exc:
        _stats["training_errors"] += 1
        global _last_error
        _last_error = f"training:{type(exc).__name__}:{exc}"


def _augment_response(response):
    core = _core()
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    candidates = early_candidates(20, actionable_only=True)
    data["v12_5_early_prediction"] = {
        "revision": REVISION,
        "role": ROLE,
        "execution_authority": False,
        "strict_buy_authority_unchanged": True,
        "sensor_shards_live": _stats.get("sensor_shards_live", 0),
        "sensor_shards_expected": SENSOR_SHARDS,
        "sensor_symbols": _stats.get("sensor_symbols", 0),
        "sensor_safe": _stats.get("sensor_safe", 0),
        "early_pinpoint": _stats.get("early_pinpoint", 0),
        "early_armed": _stats.get("early_armed", 0),
        "early_watch": _stats.get("early_watch", 0),
        "deep_promoted": _stats.get("deep_promoted", 0),
        "candidates": candidates,
        "score_is_probability": False,
        "rule": "EARLY_PINPOINT is predictive/advisory only; strict BUY NOW remains controlled by V12.3.4/V12.4 hard execution authority.",
    }
    data["upgrade_revision_v12_5"] = REVISION
    data["upgrade_last_error_v12_5"] = _last_error or None
    return core.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    response = _augment_response(await _original_scan(request))
    try:
        top = early_candidates(5, actionable_only=True)
        print(
            "Ψ-V12.5 EARLY-SCAN "
            f"sensor={_stats.get('sensor_symbols',0)} safe={_stats.get('sensor_safe',0)} "
            f"shards={_stats.get('sensor_shards_live',0)}/{SENSOR_SHARDS} "
            f"pinpoint={_stats.get('early_pinpoint',0)} armed={_stats.get('early_armed',0)} "
            f"watch={_stats.get('early_watch',0)} "
            f"top={','.join(r['symbol']+':'+r['state']+'/'+str(r['hazard_score']) for r in top) or '-'} "
            "authority=ADVISORY_ONLY strictBuy=UNCHANGED",
            flush=True,
        )
    except Exception:
        pass
    return response


async def _health_wrapper(request):
    return _augment_response(await _original_health(request))


async def bootstrap():
    global _bootstrapped
    if _bootstrapped:
        return
    core = _core()
    if not getattr(core, "REDIS_URL", ""):
        _bootstrapped = True
        return
    client = redis_async.from_url(core.REDIS_URL, encoding="utf-8", decode_responses=True)
    try:
        await client.ping()
        await _refresh_from_redis(client)
    except Exception as exc:
        global _last_error
        _last_error = f"bootstrap:{type(exc).__name__}:{exc}"
    finally:
        await client.aclose()
        _bootstrapped = True


async def supervisor_loop():
    global _last_diag_mono, _last_train_mono, _last_error
    core = _core()
    if not _bootstrapped:
        await bootstrap()
    while True:
        client = None
        try:
            if not getattr(core, "REDIS_URL", ""):
                await asyncio.sleep(POLL_SECONDS)
                continue
            client = redis_async.from_url(core.REDIS_URL, encoding="utf-8", decode_responses=True)
            await client.ping()
            while True:
                await _refresh_from_redis(client)
                mono = time.monotonic()
                if mono - _last_train_mono >= TRAIN_SAMPLE_SECONDS:
                    _last_train_mono = mono
                    _persist_training()
                if mono - _last_diag_mono >= DIAG_SECONDS:
                    _last_diag_mono = mono
                    top = early_candidates(8, actionable_only=True)
                    print(
                        "Ψ-V12.5 SENSOR "
                        f"coverage={_stats.get('sensor_symbols',0)} "
                        f"safe={_stats.get('sensor_safe',0)} "
                        f"shards={_stats.get('sensor_shards_live',0)}/{SENSOR_SHARDS} "
                        f"pinpoint={_stats.get('early_pinpoint',0)} armed={_stats.get('early_armed',0)} "
                        f"watch={_stats.get('early_watch',0)} deepPromoted={_stats.get('deep_promoted',0)} "
                        f"training={_stats.get('training_snapshots',0)}",
                        flush=True,
                    )
                    for i, r in enumerate(top, 1):
                        print(
                            f"EP{i:02d}. {r['symbol']:12s} state={r['state']:14s} "
                            f"hazard={r['hazard_score']:5.1f} delta={r['change_point_delta']:+5.1f} "
                            f"eta={r['eta_band']:6s} buy={100*r['buy_ratio']:4.0f}% "
                            f"rv10={r['relative_volume_10s']:4.1f} ta={r['trade_acceleration']:4.1f} "
                            f"ofi={r['ofi']:+.2f} obi={r['obi']:+.2f} askDep={r['ask_depletion']:+.2f} "
                            f"entryRef={r['entry_reference']:.10g} authority=ADVISORY",
                            flush=True,
                        )
                await asyncio.sleep(POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _last_error = f"loop:{type(exc).__name__}:{exc}"
            print(f"Ψ-V12.5 ERROR {_last_error}", flush=True)
            await asyncio.sleep(max(1.0, POLL_SECONDS))
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


def install(core, hardening, learner, v124=None):
    global CORE, HARDENING, LEARNER, V124
    global _original_scan, _original_health, _original_micro, _original_priority
    if CORE is not None:
        return
    CORE = core
    HARDENING = hardening
    LEARNER = learner
    V124 = v124

    _original_scan = core.v12_scan
    _original_health = core.v12_health
    _original_micro = core._distributed_micro_symbols
    _original_priority = core._priority_symbols

    _wrap_learning()
    core._distributed_micro_symbols = promoted_micro_symbols
    core._priority_symbols = priority_symbols
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        f"Ψ-V12.5 installed — {REVISION}; full-universe sensor promotion enabled; "
        "EARLY_PINPOINT is advisory only; strict BUY authority unchanged",
        flush=True,
    )
