"""V14 performance hot-path fixes.

The V14 gate is evaluated for every structural symbol. Two helpers originally
rebuilt broad shared state per candidate:
1) the full early-sensor map;
2) the leakage-controlled +10% calibration history.

Both are invariant across many candidates inside the same short scan window.
This module adds bounded read-through caches for those shared calculations.
It does NOT cache candidate decisions, risk plans, timestamps, or hard vetoes.
V14 still evaluates live sensor/trade/book freshness for every candidate.
"""
import os
import time

REVISION = "14.0.2-shared-hotpath-cache"
EARLY_MAP_CACHE_MS = max(
    50, min(1000, int(os.getenv("PSI_V14_EARLY_MAP_CACHE_MS", "500")))
)
P10_CACHE_MS = max(
    500, min(10_000, int(os.getenv("PSI_V14_P10_CACHE_MS", "5000")))
)
UNIVERSE_CACHE_MS = max(
    500, min(10_000, int(os.getenv("PSI_V14_UNIVERSE_CACHE_MS", "5000")))
)

V14 = None
_ORIGINAL_EARLY_MAP = None
_ORIGINAL_P10 = None
_ORIGINAL_UNIVERSE = None

_EARLY_CACHE = None
_EARLY_CACHE_AT_MS = 0
_P10_CACHE = {}
_UNIVERSE_CACHE = None
_UNIVERSE_CACHE_AT_MS = 0

STATS = {
    "early_hits": 0,
    "early_misses": 0,
    "early_source_builds": 0,
    "early_last_build_ms": 0.0,
    "p10_hits": 0,
    "p10_misses": 0,
    "p10_source_builds": 0,
    "p10_last_build_ms": 0.0,
    "universe_hits": 0,
    "universe_misses": 0,
}


def _now_ms():
    return int(time.monotonic() * 1000)


def _score_bucket(score):
    try:
        score = float(score)
    except (TypeError, ValueError, OverflowError):
        score = 0.0
    score = max(0.0, min(1.0, score))
    return int(score * 5.0)


def _cached_early_map():
    global _EARLY_CACHE, _EARLY_CACHE_AT_MS
    now = _now_ms()
    if (
        isinstance(_EARLY_CACHE, dict)
        and 0 <= now - _EARLY_CACHE_AT_MS <= EARLY_MAP_CACHE_MS
    ):
        STATS["early_hits"] += 1
        return _EARLY_CACHE

    STATS["early_misses"] += 1
    started = time.perf_counter()
    rows = _ORIGINAL_EARLY_MAP() if callable(_ORIGINAL_EARLY_MAP) else {}
    _EARLY_CACHE = rows if isinstance(rows, dict) else {}
    _EARLY_CACHE_AT_MS = now
    STATS["early_source_builds"] += 1
    STATS["early_last_build_ms"] = round(
        (time.perf_counter() - started) * 1000.0, 3
    )
    return _EARLY_CACHE


def _cached_p10(lane, score):
    """Cache exact V14 calibration by the only inputs that affect it.

    V14's original calibration reduces score to a six-level bucket before
    reading historical outcomes, so (lane, bucket) is an exact cache key.
    """
    now = _now_ms()
    key = (str(lane or ""), _score_bucket(score))
    cached = _P10_CACHE.get(key)
    if cached is not None:
        at_ms, value = cached
        if 0 <= now - at_ms <= P10_CACHE_MS:
            STATS["p10_hits"] += 1
            return dict(value)

    STATS["p10_misses"] += 1
    started = time.perf_counter()
    value = (
        _ORIGINAL_P10(lane, score)
        if callable(_ORIGINAL_P10)
        else {
            "probability": 0.5,
            "samples": 0,
            "confidence": "INSUFFICIENT",
            "source": "NO_ORIGINAL_P10",
            "ci95": [0.0, 1.0],
        }
    )
    value = dict(value or {})
    _P10_CACHE[key] = (now, value)
    STATS["p10_source_builds"] += 1
    STATS["p10_last_build_ms"] = round(
        (time.perf_counter() - started) * 1000.0, 3
    )
    if len(_P10_CACHE) > 32:
        oldest = sorted(_P10_CACHE, key=lambda k: _P10_CACHE[k][0])
        for old in oldest[:-24]:
            _P10_CACHE.pop(old, None)
    return dict(value)


def _cached_universe():
    global _UNIVERSE_CACHE, _UNIVERSE_CACHE_AT_MS
    now = _now_ms()
    if (
        isinstance(_UNIVERSE_CACHE, list)
        and 0 <= now - _UNIVERSE_CACHE_AT_MS <= UNIVERSE_CACHE_MS
    ):
        STATS["universe_hits"] += 1
        return _UNIVERSE_CACHE

    STATS["universe_misses"] += 1
    rows = _ORIGINAL_UNIVERSE() if callable(_ORIGINAL_UNIVERSE) else []
    _UNIVERSE_CACHE = list(rows or [])
    _UNIVERSE_CACHE_AT_MS = now
    return _UNIVERSE_CACHE


def clear_cache():
    global _EARLY_CACHE, _EARLY_CACHE_AT_MS
    global _P10_CACHE, _UNIVERSE_CACHE, _UNIVERSE_CACHE_AT_MS
    _EARLY_CACHE = None
    _EARLY_CACHE_AT_MS = 0
    _P10_CACHE = {}
    _UNIVERSE_CACHE = None
    _UNIVERSE_CACHE_AT_MS = 0


def install(v14):
    global V14, _ORIGINAL_EARLY_MAP, _ORIGINAL_P10, _ORIGINAL_UNIVERSE
    if V14 is not None:
        return
    V14 = v14

    _ORIGINAL_EARLY_MAP = getattr(v14, "_early_map", None)
    _ORIGINAL_P10 = getattr(v14, "_p10_calibrated", None)
    _ORIGINAL_UNIVERSE = getattr(v14, "_universe", None)

    if callable(_ORIGINAL_EARLY_MAP):
        v14._early_map = _cached_early_map
    if callable(_ORIGINAL_P10):
        v14._p10_calibrated = _cached_p10
    if callable(_ORIGINAL_UNIVERSE):
        v14._universe = _cached_universe

    clear_cache()
    print(
        "PSI-V14.0.2 PERF_FIX installed"
        + f" earlyMapCacheMs={EARLY_MAP_CACHE_MS}"
        + f" p10CacheMs={P10_CACHE_MS}"
        + f" universeCacheMs={UNIVERSE_CACHE_MS}"
        + " scope=SHARED_COMPUTATION_ONLY"
        + " freshnessChecks=UNCHANGED candidateDecisions=UNCACHED",
        flush=True,
    )
