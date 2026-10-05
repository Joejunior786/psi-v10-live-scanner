import json
import os
import time

REVISION = "12.6.0-freshness-gate+stale-invalidation+training-quality"
SENSOR_MAX_SNAPSHOT_AGE_MS = max(500, int(os.getenv("PSI_V126_SENSOR_MAX_SNAPSHOT_AGE_MS", "1500")))

V125 = None
_original_persist_training = None


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
    keys = [f"psi:v12.5:sensor:{i}" for i in range(V125.SENSOR_SHARDS)]
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

    V125._sensor_cache.clear()
    if merged:
        V125._sensor_cache.update(merged)
        V125._rebuild_candidates()
    else:
        V125._latest_candidates[:] = []
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
    if not V125._sensor_cache:
        return False
    now = int(time.time() * 1000)
    for row in V125._sensor_cache.values():
        generated = int(_f(row.get("_sensor_generated_ms"), 0))
        if generated <= 0 or now - generated > SENSOR_MAX_SNAPSHOT_AGE_MS:
            return False
    return True


def _persist_training_guarded():
    if not _fresh_cache_quality_ok():
        V125._stats["training_skipped_stale"] = int(V125._stats.get("training_skipped_stale", 0)) + 1
        return
    _original_persist_training()


def install(v125):
    global V125, _original_persist_training
    if V125 is not None:
        return
    V125 = v125
    _original_persist_training = v125._persist_training
    v125._merge_sensor_payloads = _merge_sensor_payloads
    v125._refresh_from_redis = _refresh_from_redis
    v125._persist_training = _persist_training_guarded
    print(
        f"PSI-V12.6 installed revision={REVISION} maxSnapshotAgeMs={SENSOR_MAX_SNAPSHOT_AGE_MS} "
        "staleState=INVALIDATE staleTraining=BLOCK",
        flush=True,
    )
