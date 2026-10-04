import asyncio
import json
import os
import time
from typing import Dict, List

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.3.0-risk-data-worker-1"
REDIS_URL = os.getenv("REDIS_URL", "").strip()
CONTROL_KEY = os.getenv("PSI_RISK_CONTROL_KEY", "psi:v12:risk-priority").strip()
FALLBACK_CONTROL_KEY = os.getenv("PSI_MICRO_CONTROL_KEY", "psi:v12:selected").strip()
KEY_PREFIX = os.getenv("PSI_RISK_REDIS_PREFIX", "psi:v12:risk-candle").strip()
HEARTBEAT_KEY = os.getenv("PSI_RISK_HEARTBEAT_KEY", "psi:v12:risk-worker").strip()
MAX_SYMBOLS = max(4, min(int(os.getenv("PSI_RISK_MAX_SYMBOLS", "32")), 80))
BATCH_SIZE = max(1, min(int(os.getenv("PSI_RISK_BATCH_SIZE", "6")), 12))
HTTP_CONCURRENCY = max(3, min(int(os.getenv("PSI_RISK_HTTP_CONCURRENCY", "12")), 24))
PRIORITY_REFRESH_S = max(8.0, float(os.getenv("PSI_RISK_PRIORITY_REFRESH_S", "24")))
CACHE_TTL_S = max(120, int(os.getenv("PSI_RISK_CACHE_TTL_S", "240")))
CONTROL_POLL_S = max(1.0, float(os.getenv("PSI_RISK_CONTROL_POLL_S", "2")))
HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_RISK_REST_HOSTS",
        "https://data-api.binance.vision,https://api-gcp.binance.com,https://api.binance.com,https://api1.binance.com"
    ).split(",")
    if x.strip()
)
TF_LIMITS = (("1m",64),("5m",72),("15m",52))

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for risk-data worker")


def now_ms() -> int:
    return int(time.time() * 1000)


async def _symbols_from_key(r, key: str) -> List[str]:
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
        if len(out) >= MAX_SYMBOLS:
            break
    return out


async def selected_symbols(r) -> List[str]:
    primary = await _symbols_from_key(r, CONTROL_KEY)
    if primary:
        return primary
    return await _symbols_from_key(r, FALLBACK_CONTROL_KEY)


async def fetch_klines(session, sem, symbol: str, interval: str, limit: int):
    errors = []
    async with sem:
        for idx, host in enumerate(HOSTS[:3]):
            try:
                timeout = aiohttp.ClientTimeout(total=3.5, connect=1.0, sock_read=2.8)
                async with session.get(
                    host + "/api/v3/klines",
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
            if idx == 0:
                await asyncio.sleep(0)
    return None, "", ";".join(errors[-3:])


async def hydrate_symbol(r, session, sem, symbol: str, stats: Dict[str, object]):
    started = time.monotonic()
    jobs = [
        asyncio.create_task(fetch_klines(session, sem, symbol, tf, lim))
        for tf, lim in TF_LIMITS
    ]
    results = await asyncio.gather(*jobs, return_exceptions=True)
    fetched = now_ms()
    ok = 0
    for (tf, lim), result in zip(TF_LIMITS, results):
        if isinstance(result, BaseException):
            rows, host, err = None, "", type(result).__name__
        else:
            rows, host, err = result
        if isinstance(rows, list) and rows:
            payload = {
                "version": WORKER_VERSION,
                "symbol": symbol,
                "interval": tf,
                "requested_limit": lim,
                "rows": rows,
                "fetched_ms": fetched,
                "host": host,
            }
            await r.set(
                f"{KEY_PREFIX}:{symbol}:{tf}",
                json.dumps(payload, separators=(",", ":")),
                ex=CACHE_TTL_S,
            )
            stats[f"ok_{tf}"] = int(stats.get(f"ok_{tf}",0)) + 1
            ok += 1
        else:
            stats[f"fail_{tf}"] = int(stats.get(f"fail_{tf}",0)) + 1
            stats["last_error"] = f"{symbol}:{tf}:{err}"
    stats["symbols"] = int(stats.get("symbols",0)) + 1
    stats["last_symbol"] = symbol
    stats["last_fetch_ms"] = fetched
    stats["last_duration_ms"] = int((time.monotonic() - started) * 1000)
    return ok


async def publish_heartbeat(r, active: List[str], stats: Dict[str, object], error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "symbols": len(active),
        "ok_1m": int(stats.get("ok_1m",0)),
        "ok_5m": int(stats.get("ok_5m",0)),
        "ok_15m": int(stats.get("ok_15m",0)),
        "fail_1m": int(stats.get("fail_1m",0)),
        "fail_5m": int(stats.get("fail_5m",0)),
        "fail_15m": int(stats.get("fail_15m",0)),
        "samples": int(stats.get("symbols",0)),
        "last_symbol": stats.get("last_symbol"),
        "last_fetch_ms": int(stats.get("last_fetch_ms",0) or 0),
        "last_duration_ms": int(stats.get("last_duration_ms",0) or 0),
        "last_error": str(stats.get("last_error","") or ""),
        "heartbeat_ms": now_ms(),
        "error": error,
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
            print(f"PSI-RISK-WORKER redis_wait {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(1.5)

    connector = aiohttp.TCPConnector(
        limit=max(HTTP_CONCURRENCY * 2, 20),
        limit_per_host=HTTP_CONCURRENCY,
        ttl_dns_cache=300,
        keepalive_timeout=45,
        family=2,
    )
    session = aiohttp.ClientSession(
        connector=connector,
        headers={"User-Agent": f"psi-risk-worker/{WORKER_VERSION}"},
    )
    sem = asyncio.Semaphore(HTTP_CONCURRENCY)
    last_fetch: Dict[str, float] = {}
    stats: Dict[str, object] = {}
    cursor = 0
    last_hb = 0.0

    print(f"PSI-RISK-WORKER START version={WORKER_VERSION} maxSymbols={MAX_SYMBOLS}", flush=True)

    try:
        while True:
            symbols = await selected_symbols(r)
            if not symbols:
                await publish_heartbeat(r, [], stats, "waiting_for_priority")
                await asyncio.sleep(CONTROL_POLL_S)
                continue

            now = time.monotonic()
            due = [s for s in symbols if now - last_fetch.get(s, 0.0) >= PRIORITY_REFRESH_S]
            if not due:
                if now - last_hb >= 3.0:
                    await publish_heartbeat(r, symbols, stats)
                    last_hb = now
                await asyncio.sleep(CONTROL_POLL_S)
                continue

            ordered = []
            if due:
                start = cursor % len(due)
                ordered = due[start:] + due[:start]
                cursor = (cursor + BATCH_SIZE) % max(1, len(due))
            batch = ordered[:BATCH_SIZE]

            results = await asyncio.gather(
                *(hydrate_symbol(r, session, sem, sym, stats) for sym in batch),
                return_exceptions=True,
            )
            completed = time.monotonic()
            for sym, result in zip(batch, results):
                if not isinstance(result, BaseException):
                    last_fetch[sym] = completed
                else:
                    stats["last_error"] = f"{sym}:{type(result).__name__}:{result}"

            if completed - last_hb >= 3.0:
                await publish_heartbeat(r, symbols, stats)
                last_hb = completed

            if int(stats.get("symbols",0)) % 24 < len(batch):
                print(
                    f"PSI-RISK-WORKER progress active={len(symbols)} samples={stats.get('symbols',0)} "
                    f"ok1m={stats.get('ok_1m',0)} ok5m={stats.get('ok_5m',0)} ok15m={stats.get('ok_15m',0)} "
                    f"fail1m={stats.get('fail_1m',0)} fail5m={stats.get('fail_5m',0)} fail15m={stats.get('fail_15m',0)} "
                    f"last={stats.get('last_symbol')}:{stats.get('last_duration_ms',0)}ms",
                    flush=True,
                )
            await asyncio.sleep(0.05)
    finally:
        await session.close()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
