import asyncio
import json
import os
import threading
import time

REVISION = "12.6.3-freshness+consumer-isolation+training-quality+pinpoint-hotlane"
SENSOR_MAX_SNAPSHOT_AGE_MS = max(500, int(os.getenv("PSI_V126_SENSOR_MAX_SNAPSHOT_AGE_MS", "1500")))
HOT_LANE_SLOTS = max(2, min(int(os.getenv("PSI_V126_PINPOINT_HOT_LANE_SLOTS", "8")), 16))

V125 = None
_original_persist_training = None
_original_supervisor_loop = None
_original_bootstrap = None
_original_promotion_symbols = None

_supervisor_thread = None
_supervisor_thread_lock = threading.Lock()
_supervisor_started = threading.Event()
_supervisor_stop = threading.Event()
_supervisor_thread_ident = None
_supervisor_thread_error = ""


def _f(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _merge_sensor_payloads(payloads):
    merged = {}
    shards_live = 0
    now = int(time.time() * 1000)
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        generated = int(_f(payload.get("generated_ms"), 0))
        age = now - generated if generated > 0 else 999999999
        if generated <= 0 or age > SENSOR_MAX_SNAPSHOT_AGE_MS:
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
            x["_sensor_snapshot_age_ms"] = max(0, age)
            merged[sym] = x
    return merged, shards_live


async def _refresh_from_redis(client):
    keys = [f"psi:v12.6:sensor:{i}" for i in range(V125.SENSOR_SHARDS)]
    raws = await client.mget(keys)
    payloads = []
    now = int(time.time() * 1000)
    shard_diag = []

    for idx, raw in enumerate(raws):
        if not raw:
            shard_diag.append({"i": idx, "present": False, "age_ms": None, "decode": False, "fresh": False})
            continue
        try:
            payload = json.loads(raw)
            generated = int(_f(payload.get("generated_ms"), 0))
            age = (now - generated) if generated > 0 else None
            fresh = bool(age is not None and 0 <= age <= SENSOR_MAX_SNAPSHOT_AGE_MS)
            shard_diag.append({
                "i": idx,
                "present": True,
                "age_ms": age,
                "decode": True,
                "fresh": fresh,
                "payload_shard": int(_f(payload.get("shard_index"), -1)),
                "symbols": int(_f(payload.get("metric_symbols"), 0)),
            })
            payloads.append(payload)
        except Exception:
            shard_diag.append({"i": idx, "present": True, "age_ms": None, "decode": False, "fresh": False})

    V125._stats["sensor_shard_diag"] = shard_diag
    V125._stats["sensor_keys_present"] = sum(1 for row in shard_diag if row.get("present"))
    V125._stats["sensor_keys_decoded"] = sum(1 for row in shard_diag if row.get("decode"))

    merged, shards_live = _merge_sensor_payloads(payloads)
    V125._stats["sensor_shards_live"] = shards_live
    V125._stats["sensor_shards_expected"] = V125.SENSOR_SHARDS
    V125._stats["sensor_snapshot_max_age_ms"] = SENSOR_MAX_SNAPSHOT_AGE_MS

    # Replace complete snapshots atomically. The dedicated consumer thread is
    # the only writer; scanner threads only observe whole dict/list objects.
    if merged:
        V125._sensor_cache = merged
        V125._rebuild_candidates()
    else:
        V125._sensor_cache = {}
        V125._latest_candidates = []
        V125._score_history.clear()
        for key in (
            "sensor_symbols",
            "sensor_safe",
            "early_pinpoint",
            "early_armed",
            "early_watch",
            "deep_promoted",
            "structure_promoted",
        ):
            V125._stats[key] = 0
        V125._stats["stale_invalidations"] = int(V125._stats.get("stale_invalidations", 0)) + 1
    return len(merged)


def _fresh_cache_quality_ok():
    if int(V125._stats.get("sensor_shards_live", 0)) != int(V125.SENSOR_SHARDS):
        return False
    cache = V125._sensor_cache
    if not cache:
        return False
    now = int(time.time() * 1000)
    for row in cache.values():
        generated = int(_f(row.get("_sensor_generated_ms"), 0))
        if generated <= 0 or now - generated > SENSOR_MAX_SNAPSHOT_AGE_MS:
            return False
    return True


def _persist_training_guarded():
    if not _fresh_cache_quality_ok():
        V125._stats["training_skipped_stale"] = int(V125._stats.get("training_skipped_stale", 0)) + 1
        return
    _original_persist_training()


def _hot_lane_symbols():
    """Return the freshest hard-safe early candidates for deep/Pinpoint promotion.

    This is ordering/promotion only. It never creates BUY authority and never
    relaxes any V12.3/V12.4 execution, integrity, persistence, anti-chase or
    risk gate.
    """
    if V125 is None or not _fresh_cache_quality_ok():
        if V125 is not None:
            V125._stats["pinpoint_hot_lane"] = 0
            V125._stats["pinpoint_hot_lane_symbols"] = []
        return []

    now = int(time.time() * 1000)
    rank = {"EARLY_PINPOINT": 3, "EARLY_ARMED": 2, "EARLY_WATCH": 1}
    rows = []
    for row in list(getattr(V125, "_latest_candidates", []) or []):
        state = str(row.get("state") or "")
        generated = int(_f(row.get("generated_ms"), 0))
        if state not in rank or not bool(row.get("hard_sensor_safety")):
            continue
        if generated <= 0 or now - generated > SENSOR_MAX_SNAPSHOT_AGE_MS:
            continue
        rows.append(row)

    rows.sort(
        key=lambda r: (
            rank.get(str(r.get("state") or ""), 0),
            _f(r.get("hazard_score")),
            _f(r.get("change_point_delta")),
            _f(r.get("buy_ratio")),
            -_f(r.get("spread_bps"), 999999.0),
            -_f(r.get("slippage_bps"), 999999.0),
            -_f(r.get("book_age_ms"), 999999999.0),
            -_f(r.get("trade_age_ms"), 999999999.0),
        ),
        reverse=True,
    )
    out = [str(r.get("symbol") or "").upper() for r in rows[:HOT_LANE_SLOTS] if str(r.get("symbol") or "")]
    V125._stats["pinpoint_hot_lane"] = len(out)
    V125._stats["pinpoint_hot_lane_symbols"] = list(out)
    return out


def _hot_first_promotion_symbols():
    hot = _hot_lane_symbols()
    try:
        base = list(_original_promotion_symbols() or []) if _original_promotion_symbols else []
    except Exception:
        base = []
    limit = max(HOT_LANE_SLOTS, int(getattr(V125, "EARLY_PROMOTION_SLOTS", 16)))
    out = []
    for sym in hot + base:
        sym = str(sym or "").upper()
        if sym and sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    return out


def _run_original_supervisor_once():
    asyncio.run(_original_supervisor_loop())


def _supervisor_thread_main():
    global _supervisor_thread_ident, _supervisor_thread_error
    _supervisor_thread_ident = threading.get_ident()
    V125._stats["consumer_thread_ident"] = _supervisor_thread_ident
    V125._stats["consumer_thread_started"] = 1
    _supervisor_started.set()
    print(
        f"PSI-V12.6 CONSUMER_THREAD started ident={_supervisor_thread_ident} "
        f"poll={getattr(V125, 'POLL_SECONDS', '?')}s",
        flush=True,
    )
    while not _supervisor_stop.is_set():
        try:
            _run_original_supervisor_once()
            _supervisor_thread_error = "supervisor_returned"
        except Exception as exc:
            _supervisor_thread_error = f"{type(exc).__name__}:{exc}"
        V125._stats["consumer_thread_restarts"] = int(V125._stats.get("consumer_thread_restarts", 0)) + 1
        V125._stats["consumer_thread_last_error"] = _supervisor_thread_error
        if not _supervisor_stop.is_set():
            print(
                f"PSI-V12.6 CONSUMER_THREAD restart error={_supervisor_thread_error}",
                flush=True,
            )
        _supervisor_stop.wait(1.0)


def _ensure_supervisor_thread():
    global _supervisor_thread
    with _supervisor_thread_lock:
        if _supervisor_thread is not None and _supervisor_thread.is_alive():
            return _supervisor_thread
        _supervisor_stop.clear()
        _supervisor_started.clear()
        _supervisor_thread = threading.Thread(
            target=_supervisor_thread_main,
            name="psi-v126-sensor-consumer",
            daemon=True,
        )
        _supervisor_thread.start()
        return _supervisor_thread


async def _bootstrap_and_start_consumer():
    await _original_bootstrap()
    thread = _ensure_supervisor_thread()
    started = await asyncio.to_thread(_supervisor_started.wait, 5.0)
    if not started or not thread.is_alive():
        raise RuntimeError("V12.6 sensor consumer thread failed to start")


async def _threaded_supervisor_loop():
    # Runner compatibility: the actual Redis polling loop lives in a dedicated
    # event-loop thread so heavy scanner work cannot delay MGET response handling.
    _ensure_supervisor_thread()
    while True:
        if _supervisor_thread is None or not _supervisor_thread.is_alive():
            _ensure_supervisor_thread()
        await asyncio.sleep(60.0)


def install(v125):
    global V125, _original_persist_training, _original_supervisor_loop, _original_bootstrap, _original_promotion_symbols
    if V125 is not None:
        return
    V125 = v125
    _original_persist_training = v125._persist_training
    _original_supervisor_loop = v125.supervisor_loop
    _original_bootstrap = v125.bootstrap
    _original_promotion_symbols = v125._promotion_symbols
    v125._merge_sensor_payloads = _merge_sensor_payloads
    v125._refresh_from_redis = _refresh_from_redis
    v125._persist_training = _persist_training_guarded
    v125._promotion_symbols = _hot_first_promotion_symbols
    v125.bootstrap = _bootstrap_and_start_consumer
    v125.supervisor_loop = _threaded_supervisor_loop
    print(
        f"PSI-V12.6 installed revision={REVISION} maxSnapshotAgeMs={SENSOR_MAX_SNAPSHOT_AGE_MS} "
        f"consumerLoop=DEDICATED_THREAD staleState=INVALIDATE staleTraining=BLOCK hotLane={HOT_LANE_SLOTS} authority=PROMOTION_ONLY",
        flush=True,
    )
