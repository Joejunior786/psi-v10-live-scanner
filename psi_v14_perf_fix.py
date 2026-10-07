"""V14 performance hot-path fix.

V14's strict-gate wrapper consults the early-sensor map once per structural row.
Without a tiny read-through cache that rebuilds the full sensor map hundreds of
times inside one board evaluation. This module caches only the map construction;
the underlying rows keep their original timestamps and V14 still re-checks
sensor/trade/book freshness against the current clock on every candidate.
"""
import os
import time

REVISION = "14.0.1-early-map-hotpath"
CACHE_MS = max(50, min(1000, int(os.getenv("PSI_V14_EARLY_MAP_CACHE_MS", "500"))))

V14 = None
_ORIGINAL_EARLY_MAP = None
_CACHE = None
_CACHE_AT_MS = 0
STATS = {
    "hits": 0,
    "misses": 0,
    "source_builds": 0,
    "last_build_ms": 0.0,
}


def _now_ms():
    return int(time.monotonic() * 1000)


def _cached_early_map():
    global _CACHE, _CACHE_AT_MS
    now = _now_ms()
    if isinstance(_CACHE, dict) and 0 <= now - _CACHE_AT_MS <= CACHE_MS:
        STATS["hits"] += 1
        return _CACHE

    STATS["misses"] += 1
    started = time.perf_counter()
    rows = _ORIGINAL_EARLY_MAP() if _ORIGINAL_EARLY_MAP is not None else {}
    _CACHE = rows if isinstance(rows, dict) else {}
    _CACHE_AT_MS = now
    STATS["source_builds"] += 1
    STATS["last_build_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
    return _CACHE


def clear_cache():
    global _CACHE, _CACHE_AT_MS
    _CACHE = None
    _CACHE_AT_MS = 0


def install(v14):
    global V14, _ORIGINAL_EARLY_MAP
    if V14 is not None:
        return
    V14 = v14
    _ORIGINAL_EARLY_MAP = v14._early_map
    v14._early_map = _cached_early_map
    clear_cache()
    print(
        "PSI-V14.0.1 PERF_FIX installed earlyMapCacheMs="
        + str(CACHE_MS)
        + " scope=MAP_BUILD_ONLY freshnessChecks=UNCHANGED",
        flush=True,
    )
