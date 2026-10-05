import asyncio
import json
import os
import time
from typing import Dict, List, Tuple

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.4.3-structure-worker-deep-weekly"
REDIS_URL = os.getenv("REDIS_URL", "").strip()
UNIVERSE_KEY = os.getenv("PSI_TAPE_UNIVERSE_KEY", "psi:v12:universe").strip()
CONTROL_KEY = os.getenv("PSI_MICRO_CONTROL_KEY", "psi:v12:selected").strip()
KEY_PREFIX = os.getenv("PSI_STRUCTURE_REDIS_PREFIX", "psi:v12:structure").strip()
SHARD_INDEX = max(0, int(os.getenv("PSI_STRUCTURE_SHARD_INDEX", "0")))
SHARD_COUNT = max(1, int(os.getenv("PSI_STRUCTURE_SHARD_COUNT", "2")))
PRIORITY_REFRESH_S = max(10.0, float(os.getenv("PSI_STRUCTURE_PRIORITY_REFRESH_S", "30")))
BACKGROUND_REFRESH_S = max(30.0, float(os.getenv("PSI_STRUCTURE_BACKGROUND_REFRESH_S", "75")))
CONTROL_POLL_S = max(1.0, float(os.getenv("PSI_STRUCTURE_CONTROL_POLL_S", "2")))
BATCH_SIZE = max(1, min(int(os.getenv("PSI_STRUCTURE_BATCH_SIZE", "4")), 10))
HTTP_CONCURRENCY = max(2, min(int(os.getenv("PSI_STRUCTURE_HTTP_CONCURRENCY", "8")), 20))
CACHE_TTL_S = max(1800, int(os.getenv("PSI_STRUCTURE_CACHE_TTL_S", "2400")))
WEEKLY_PRIORITY_REFRESH_S = max(120.0, float(os.getenv("PSI_STRUCTURE_WEEKLY_PRIORITY_REFRESH_S", "300")))
WEEKLY_BACKGROUND_REFRESH_S = max(WEEKLY_PRIORITY_REFRESH_S, float(os.getenv("PSI_STRUCTURE_WEEKLY_BACKGROUND_REFRESH_S", "900")))
HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_STRUCTURE_REST_HOSTS",
        "https://data-api.binance.vision,https://api-gcp.binance.com,https://api.binance.com,https://api1.binance.com"
    ).split(",")
    if x.strip()
)
CORE_TF_LIMITS = (("15m", 80), ("1h", 220), ("4h", 220), ("1d", 220))
WEEKLY_TF_LIMIT = ("1w", 220)
HEARTBEAT_KEY = f"psi:v12:structure-worker:{SHARD_INDEX}"

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for structure worker")
if SHARD_INDEX >= SHARD_COUNT:
    raise RuntimeError("PSI_STRUCTURE_SHARD_INDEX must be less than PSI_STRUCTURE_SHARD_COUNT")


def now_ms() -> int:
    return int(time.time() * 1000)


async def read_symbol_list(r, key: str) -> List[str]:
    raw = await r.get(key)
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except Exception:
        return []
    if isinstance(payload, dict):
        payload = payload.get("symbols") or []
    out, seen = [], set()
    for item in payload if isinstance(payload, list) else []:
        sym = str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            out.append(sym)
    return out


async def fetch_klines(session, sem, symbol: str, interval: str, limit: int):
    endpoint = "/api/v3/klines"
    errors = []
    async with sem:
        for offset in range(min(3, len(HOSTS))):
            host = HOSTS[(SHARD_INDEX + offset) % len(HOSTS)]
            try:
                timeout = aiohttp.ClientTimeout(total=4.2, connect=1.2, sock_read=3.2)
                async with session.get(
                    host + endpoint,
                    params={"symbol": symbol, "interval": interval, "limit": int(limit)},
                    timeout=timeout,
                ) as resp:
                    if resp.status != 200:
                        errors.append(f"{host}:{resp.status}")
                        continue
                    rows = await resp.json()
                    if isinstance(rows, list) and rows:
                        return rows, host, ""
                    errors.append(f"{host}:empty")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(f"{host}:{type(exc).__name__}")
    return None, "", ";".join(errors[-3:])


async def publish_symbol(r, session, sem, symbol: str, stats: dict, include_weekly: bool = False):
    started = time.monotonic()
    tf_limits = CORE_TF_LIMITS + ((WEEKLY_TF_LIMIT,) if include_weekly else ())
    tasks = [
        asyncio.create_task(fetch_klines(session, sem, symbol, tf, limit))
        for tf, limit in tf_limits
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    fetched = now_ms()
    success = 0
    failed = 0
    weekly_ok = False
    for (tf, limit), result in zip(tf_limits, results):
        if isinstance(result, BaseException):
            rows, host, err = None, "", type(result).__name__
        else:
            rows, host, err = result
        if isinstance(rows, list) and rows:
            payload = {
                "version": WORKER_VERSION,
                "symbol": symbol,
                "interval": tf,
                "requested_limit": limit,
                "rows": rows,
                "fetched_ms": fetched,
                "host": host,
                "shard_index": SHARD_INDEX,
            }
            await r.set(
                f"{KEY_PREFIX}:{symbol}:{tf}",
                json.dumps(payload, separators=(",", ":")),
                ex=CACHE_TTL_S,
            )
            success += 1
            if tf == "1w":
                weekly_ok = True
            stats[f"ok_{tf}"] = stats.get(f"ok_{tf}", 0) + 1
        else:
            failed += 1
            stats[f"fail_{tf}"] = stats.get(f"fail_{tf}", 0) + 1
            stats["last_error"] = f"{symbol}:{tf}:{err}"
    stats["symbols"] = stats.get("symbols", 0) + 1
    stats["last_symbol"] = symbol
    stats["last_duration_ms"] = int((time.monotonic() - started) * 1000)
    stats["last_fetch_ms"] = fetched
    return success, failed, weekly_ok


async def heartbeat(r, assigned: int, priority: int, stats: dict, error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "shard_index": SHARD_INDEX,
        "shard_count": SHARD_COUNT,
        "assigned": assigned,
        "priority": priority,
        "ok_15m": stats.get("ok_15m", 0),
        "ok_1h": stats.get("ok_1h", 0),
        "ok_4h": stats.get("ok_4h", 0),
        "ok_1d": stats.get("ok_1d", 0),
        "ok_1w": stats.get("ok_1w", 0),
        "fail_15m": stats.get("fail_15m", 0),
        "fail_1h": stats.get("fail_1h", 0),
        "fail_4h": stats.get("fail_4h", 0),
        "fail_1d": stats.get("fail_1d", 0),
        "fail_1w": stats.get("fail_1w", 0),
        "symbols": stats.get("symbols", 0),
        "last_symbol": stats.get("last_symbol"),
        "last_fetch_ms": stats.get("last_fetch_ms", 0),
        "last_error": stats.get("last_error", ""),
        "error": error,
        "heartbeat_ms": now_ms(),
    }
    await r.set(HEARTBEAT_KEY, json.dumps(payload, separators=(",", ":")), ex=15)


async def main():
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    while True:
        try:
            await r.ping()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(
                f"PSI-STRUCTURE-WORKER redis_wait shard={SHARD_INDEX} {type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(1.5)

    connector = aiohttp.TCPConnector(
        limit=max(HTTP_CONCURRENCY * 2, 16),
        limit_per_host=HTTP_CONCURRENCY,
        ttl_dns_cache=300,
        keepalive_timeout=45,
        family=2,
    )
    session = aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": f"psi-structure-worker/{WORKER_VERSION}/{SHARD_INDEX}"},
    )
    sem = asyncio.Semaphore(HTTP_CONCURRENCY)
    last_fetch: Dict[str, float] = {}
    last_weekly_fetch: Dict[str, float] = {}
    stats: Dict[str, int] = {}
    cursor = 0
    last_hb = 0.0

    print(
        f"PSI-STRUCTURE-WORKER START version={WORKER_VERSION} shard={SHARD_INDEX+1}/{SHARD_COUNT}",
        flush=True,
    )

    try:
        while True:
            universe = await read_symbol_list(r, UNIVERSE_KEY)
            priority_all = await read_symbol_list(r, CONTROL_KEY)
            if not universe:
                await heartbeat(r, 0, 0, stats, "waiting_for_universe")
                await asyncio.sleep(CONTROL_POLL_S)
                continue

            ordered = sorted(universe)
            assigned = ordered[SHARD_INDEX::SHARD_COUNT]
            assigned_set = set(assigned)
            priority = [s for s in priority_all if s in assigned_set]
            now = time.monotonic()

            due_priority = [
                s for s in priority
                if now - last_fetch.get(s, 0.0) >= PRIORITY_REFRESH_S
            ]
            due_background = []
            if assigned:
                checked = 0
                while checked < len(assigned) and len(due_background) < BATCH_SIZE * 3:
                    sym = assigned[cursor % len(assigned)]
                    cursor = (cursor + 1) % len(assigned)
                    checked += 1
                    if sym in due_priority:
                        continue
                    if now - last_fetch.get(sym, 0.0) >= BACKGROUND_REFRESH_S:
                        due_background.append(sym)

            queue = []
            for sym in due_priority + due_background:
                if sym not in queue:
                    queue.append(sym)
                if len(queue) >= BATCH_SIZE:
                    break

            if not queue:
                if now - last_hb >= 3.0:
                    await heartbeat(r, len(assigned), len(priority), stats)
                    last_hb = now
                await asyncio.sleep(CONTROL_POLL_S)
                continue

            weekly_requested = {}
            for sym in queue:
                weekly_refresh = WEEKLY_PRIORITY_REFRESH_S if sym in priority else WEEKLY_BACKGROUND_REFRESH_S
                weekly_requested[sym] = now - last_weekly_fetch.get(sym, 0.0) >= weekly_refresh

            results = await asyncio.gather(
                *(publish_symbol(r, session, sem, sym, stats, include_weekly=weekly_requested[sym]) for sym in queue),
                return_exceptions=True,
            )
            completed = time.monotonic()
            for sym, result in zip(queue, results):
                if not isinstance(result, BaseException):
                    last_fetch[sym] = completed
                    if weekly_requested.get(sym) and len(result) >= 3 and bool(result[2]):
                        last_weekly_fetch[sym] = completed
                else:
                    stats["last_error"] = f"{sym}:{type(result).__name__}:{result}"

            if completed - last_hb >= 3.0:
                await heartbeat(r, len(assigned), len(priority), stats)
                last_hb = completed

            if stats.get("symbols", 0) % 40 < len(queue):
                print(
                    f"PSI-STRUCTURE-WORKER progress shard={SHARD_INDEX+1}/{SHARD_COUNT} "
                    f"assigned={len(assigned)} priority={len(priority)} symbols={stats.get('symbols',0)} "
                    f"ok15={stats.get('ok_15m',0)} ok1h={stats.get('ok_1h',0)} ok4h={stats.get('ok_4h',0)} ok1d={stats.get('ok_1d',0)} ok1w={stats.get('ok_1w',0)} "
                    f"fail15={stats.get('fail_15m',0)} fail1h={stats.get('fail_1h',0)} fail4h={stats.get('fail_4h',0)} fail1d={stats.get('fail_1d',0)} fail1w={stats.get('fail_1w',0)}",
                    flush=True,
                )
            await asyncio.sleep(0.05)
    finally:
        await session.close()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
