"""V12.9 worker-first structure hydration and hot-candidate prefetch.

This is a data-delivery upgrade, not a new trade authority.
It never changes microstructure integrity, risk-plan validation, or BUY rules.
"""
import asyncio
import json
import os
import time
from collections import defaultdict

import redis.asyncio as redis_async

REVISION = "12.9.0-worker-first-fast-confirmation"
ROLE = "WORKER_FIRST_STRUCTURE+HOT_PREFETCH+HANDOFF_DIAGNOSTICS"
STRICT_BUY_AUTHORITY_UNCHANGED = True

CORE = None
V128 = None
_original_fetch = None
_client = None
_stats = defaultdict(int)
_seen = {}
_last_report = 0.0

STRUCTURE_PREFIX = os.getenv("PSI_STRUCTURE_REDIS_PREFIX", "psi:v12:structure").strip()
RISK_PREFIX = os.getenv("PSI_RISK_REDIS_PREFIX", "psi:v12:risk-candle").strip()
HOT_COUNT = max(8, min(int(os.getenv("PSI_V129_HOT_COUNT", "32")), 60))
PREFETCH_SECONDS = max(2.0, float(os.getenv("PSI_V129_PREFETCH_SECONDS", "5")))
DIAG_SECONDS = max(10.0, float(os.getenv("PSI_V129_DIAG_SECONDS", "20")))
READ_TIMEOUT = max(0.1, min(float(os.getenv("PSI_V129_READ_TIMEOUT", "0.65")), 1.5))
STRUCTURE_AGE_MS = max(30000, int(os.getenv("PSI_V129_STRUCTURE_AGE_MS", "120000")))
WEEKLY_AGE_MS = max(120000, int(os.getenv("PSI_V129_WEEKLY_AGE_MS", "1200000")))
RISK_AGE_MS = max(15000, int(os.getenv("PSI_V129_RISK_AGE_MS", "90000")))


def _now_ms():
    return int(time.time() * 1000)


def _valid_payload(payload, symbol, timeframe, deep=False, now_ms=None):
    """Validate a worker candle cache entry without interpreting it as live tape."""
    if not isinstance(payload, dict):
        return None
    if str(payload.get("symbol") or "").upper() != str(symbol).upper():
        return None
    if str(payload.get("interval") or "") != str(timeframe):
        return None
    if not str(symbol).upper().endswith("USDT"):
        return None

    fetched = payload.get("fetched_ms")
    try:
        fetched = int(fetched)
    except (TypeError, ValueError, OverflowError):
        return None
    now = _now_ms() if now_ms is None else int(now_ms)
    age = now - fetched
    max_age = WEEKLY_AGE_MS if timeframe == "1w" else STRUCTURE_AGE_MS
    if age < -1500 or age > max_age:
        return None

    rows = payload.get("rows")
    required = max(202, int(getattr(CORE, "DEEP_MIN_ROWS", 202))) if deep else 16
    if not isinstance(rows, list) or len(rows) < required or len(rows) > 1500:
        return None
    for row in (rows[0], rows[-2], rows[-1]):
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            return None
        try:
            if float(row[4]) <= 0:
                return None
        except (TypeError, ValueError, OverflowError):
            return None
    try:
        if int(rows[-1][0]) <= int(rows[-2][0]):
            return None
    except (TypeError, ValueError, OverflowError):
        return None
    return rows


def _risk_fresh(payload, symbol, timeframe, now_ms=None):
    if not isinstance(payload, dict) or str(payload.get("symbol") or "").upper() != symbol:
        return False
    if str(payload.get("interval") or "") != timeframe:
        return False
    try:
        age = (_now_ms() if now_ms is None else now_ms) - int(payload["fetched_ms"])
    except (KeyError, TypeError, ValueError):
        return False
    return 0 <= age <= RISK_AGE_MS and isinstance(payload.get("rows"), list) and len(payload["rows"]) >= 16


def _hot_symbols():
    if CORE is None:
        return []
    universe = set(str(s).upper() for s in list(getattr(CORE.q, "universe", []) or []))
    out = []
    def add(symbol):
        symbol = str(symbol or "").upper()
        if symbol in universe and symbol not in out and len(out) < HOT_COUNT:
            out.append(symbol)
    if V128 is not None:
        try:
            for symbol in V128._sticky_symbols():
                add(symbol)
        except Exception:
            _stats["sticky_read_error"] += 1
    try:
        for row in CORE._board():
            if row.get("state") == "BUY":
                add(row.get("symbol"))
    except Exception:
        _stats["board_read_error"] += 1
    for symbol in list(getattr(CORE, "_distributed_micro_sticky_pool", []) or []):
        add(symbol)
    for symbol in list(getattr(CORE.app, "selected_micro_symbols", []) or []):
        add(symbol)
    return out


def _get_client():
    global _client
    if _client is None:
        if CORE is None or not getattr(CORE, "REDIS_URL", ""):
            return None
        _client = redis_async.from_url(
            CORE.REDIS_URL, encoding="utf-8", decode_responses=True,
            socket_connect_timeout=READ_TIMEOUT, socket_timeout=READ_TIMEOUT,
            health_check_interval=15,
        )
    return _client


async def _try_import(symbol, timeframe, deep=False, raw=None):
    if CORE is None:
        return False
    symbol = str(symbol or "").upper()
    timeframe = str(timeframe or "")
    if timeframe not in ("1h", "4h", "1d", "1w"):
        return False
    client = _get_client()
    if raw is None:
        if client is None:
            return False
        try:
            raw = await asyncio.wait_for(
                client.get(f"{STRUCTURE_PREFIX}:{symbol}:{timeframe}"),
                timeout=READ_TIMEOUT,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _stats["redis_errors"] += 1
            return False
    if not raw:
        _stats["cache_miss"] += 1
        return False
    try:
        payload = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except (TypeError, ValueError):
        _stats["invalid_payload"] += 1
        return False
    rows = _valid_payload(payload, symbol, timeframe, deep)
    if rows is None:
        _stats["invalid_or_stale"] += 1
        return False
    stamp = int(payload["fetched_ms"])
    key = (symbol, timeframe)
    if stamp <= _seen.get(key, 0):
        item = (getattr(CORE, "_cache", {}) or {}).get(symbol, {}).get(timeframe, {})
        if not item.get("snap") or (deep and len(item.get("rows") or []) < getattr(CORE, "DEEP_MIN_ROWS", 202)):
            return False
        age_s = time.time() - float(item.get("updated") or 0)
        ttl = float((getattr(CORE, "TF_TTL", {}) or {}).get(timeframe, 120.0))
        if age_s < 0 or age_s > max(1.0, min(ttl, (WEEKLY_AGE_MS if timeframe == "1w" else STRUCTURE_AGE_MS) / 1000.0)):
            _stats["already_imported_stale"] += 1
            return False
        # Already imported and still valid. Never extend its source timestamp.
        _stats["already_imported"] += 1
        return True
    if CORE._commit_authoritative_rows(
        symbol, timeframe, rows, len(rows), source="WORKER_FAST_V129"
    ):
        _seen[key] = stamp
        _stats["worker_commits"] += 1
        return True
    _stats["commit_rejected"] += 1
    return False


async def _worker_first_fetch(symbol, timeframe, deep=False):
    if await _try_import(symbol, timeframe, deep=deep):
        _stats["fast_path_hits"] += 1
        return True
    _stats["legacy_fallbacks"] += 1
    return await _original_fetch(symbol, timeframe, deep=deep)


async def _prefetch(client):
    symbols = _hot_symbols()
    _stats["hot_count"] = len(symbols)
    if not symbols:
        return
    mapping = [(sym, tf) for sym in symbols for tf in ("1h", "4h", "1d", "1w")]
    keys = [f"{STRUCTURE_PREFIX}:{sym}:{tf}" for sym, tf in mapping]
    raws = await asyncio.wait_for(client.mget(keys), timeout=max(1.0, READ_TIMEOUT * 2.0))
    for (sym, tf), raw in zip(mapping, raws):
        if raw:
            await _try_import(sym, tf, deep=False, raw=raw)
    # Diagnose worker->RiskMap handoff. Do not manufacture risk plans or permit BUY.
    risk_map = [(sym, tf) for sym in symbols for tf in ("1m", "5m", "15m")]
    risk_keys = [f"{RISK_PREFIX}:{sym}:{tf}" for sym, tf in risk_map]
    raw_risk = await asyncio.wait_for(client.mget(risk_keys), timeout=max(1.0, READ_TIMEOUT * 2.0))
    fresh = 0
    for (sym, tf), raw in zip(risk_map, raw_risk):
        if raw:
            try:
                fresh += bool(_risk_fresh(json.loads(raw), sym, tf))
            except (TypeError, ValueError):
                pass
    _stats["risk_candle_fresh"] = fresh
    _stats["risk_candle_total"] = len(risk_map)


async def supervisor_loop():
    global _last_report
    while True:
        try:
            client = _get_client()
            if client is not None:
                started = time.monotonic()
                await _prefetch(client)
                _stats["last_prefetch_ms"] = round((time.monotonic() - started) * 1000, 1)
                _stats["cycles"] += 1
            now = time.monotonic()
            if now - _last_report >= DIAG_SECONDS:
                _last_report = now
                print(
                    "PSI-V12.9 HANDOFF "
                    f"hot={_stats['hot_count']} "
                    f"workerCommits={_stats['worker_commits']} "
                    f"fastHits={_stats['fast_path_hits']} "
                    f"fallbacks={_stats['legacy_fallbacks']} "
                    f"cacheMiss={_stats['cache_miss']} "
                    f"invalidOrStale={_stats['invalid_or_stale']} "
                    f"riskCandleFresh={_stats['risk_candle_fresh']}/{_stats['risk_candle_total']} "
                    f"redisErrors={_stats['redis_errors']} "
                    f"cycleMs={_stats['last_prefetch_ms']} "
                    "strictBuyAuthority=UNCHANGED",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["supervisor_errors"] += 1
            _stats["last_error"] = f"{type(exc).__name__}:{str(exc)[:120]}"
            print(f"PSI-V12.9 HANDOFF_ERROR {_stats['last_error']}", flush=True)
        await asyncio.sleep(PREFETCH_SECONDS)


def install(core, v128=None):
    global CORE, V128, _original_fetch
    if CORE is not None:
        raise RuntimeError("V12.9 already installed")
    CORE, V128 = core, v128
    _original_fetch = core._fetch_tf
    core._fetch_tf = _worker_first_fetch
    print(f"PSI-V12.9 INSTALLED revision={REVISION} strictBuyAuthority=UNCHANGED", flush=True)
