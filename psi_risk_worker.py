import asyncio
import json
import os
import time
from typing import Dict, List

import aiohttp
import redis.asyncio as redis

WORKER_VERSION="12.3.0-risk-candle-worker-1"
REDIS_URL=os.getenv("REDIS_URL","").strip()
CONTROL_KEY=os.getenv("PSI_MICRO_CONTROL_KEY","psi:v12:selected").strip()
KEY_PREFIX=os.getenv("PSI_RISK_REDIS_PREFIX","psi:v12:risk").strip()
SHARD_INDEX=max(0,int(os.getenv("PSI_RISK_SHARD_INDEX","0")))
SHARD_COUNT=max(1,int(os.getenv("PSI_RISK_SHARD_COUNT","2")))
REFRESH_S=max(5.0,float(os.getenv("PSI_RISK_REFRESH_SECONDS","15")))
POLL_S=max(1.0,float(os.getenv("PSI_RISK_CONTROL_POLL_SECONDS","2")))
BATCH_SIZE=max(1,min(int(os.getenv("PSI_RISK_BATCH_SIZE","4")),10))
HTTP_CONCURRENCY=max(2,min(int(os.getenv("PSI_RISK_HTTP_CONCURRENCY","8")),20))
CACHE_TTL_S=max(60,int(os.getenv("PSI_RISK_CACHE_TTL_S","180")))
HOSTS=tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_RISK_REST_HOSTS",
        "https://data-api.binance.vision,https://api-gcp.binance.com,https://api.binance.com,https://api1.binance.com"
    ).split(",")
    if x.strip()
)
TF_LIMITS=(("1m",64),("5m",72),("15m",52))
HEARTBEAT_KEY=f"psi:v12:risk-worker:{SHARD_INDEX}"

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for risk candle worker")
if SHARD_INDEX>=SHARD_COUNT:
    raise RuntimeError("PSI_RISK_SHARD_INDEX must be less than PSI_RISK_SHARD_COUNT")


def now_ms():
    return int(time.time()*1000)


async def selected_symbols(r)->List[str]:
    raw=await r.get(CONTROL_KEY)
    if not raw:
        return []
    try:
        payload=json.loads(raw)
    except Exception:
        return []
    if isinstance(payload,dict):
        payload=payload.get("symbols") or []
    out=[];seen=set()
    for item in payload if isinstance(payload,list) else []:
        sym=str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym);out.append(sym)
    return sorted(out)[SHARD_INDEX::SHARD_COUNT]


async def fetch_klines(session,sem,symbol,interval,limit):
    errors=[]
    async with sem:
        for offset in range(min(3,len(HOSTS))):
            host=HOSTS[(SHARD_INDEX+offset)%len(HOSTS)]
            try:
                timeout=aiohttp.ClientTimeout(total=3.5,connect=1.0,sock_read=2.7)
                async with session.get(
                    host+"/api/v3/klines",
                    params={"symbol":symbol,"interval":interval,"limit":int(limit)},
                    timeout=timeout,
                ) as resp:
                    if resp.status!=200:
                        errors.append(f"{host}:{resp.status}")
                        continue
                    rows=await resp.json()
                    if isinstance(rows,list) and rows:
                        return rows,host,""
                    errors.append(f"{host}:empty")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(f"{host}:{type(exc).__name__}")
    return None,"",";".join(errors[-3:])


async def publish_symbol(r,session,sem,symbol,stats):
    started=time.monotonic()
    tasks=[
        asyncio.create_task(fetch_klines(session,sem,symbol,tf,limit))
        for tf,limit in TF_LIMITS
    ]
    results=await asyncio.gather(*tasks,return_exceptions=True)
    fetched=now_ms()
    ok=0;fail=0
    for (tf,limit),result in zip(TF_LIMITS,results):
        if isinstance(result,BaseException):
            rows,host,err=None,"",type(result).__name__
        else:
            rows,host,err=result
        if isinstance(rows,list) and rows:
            payload={
                "version":WORKER_VERSION,
                "symbol":symbol,
                "interval":tf,
                "requested_limit":limit,
                "rows":rows,
                "fetched_ms":fetched,
                "host":host,
                "shard_index":SHARD_INDEX,
            }
            await r.set(
                f"{KEY_PREFIX}:{symbol}:{tf}",
                json.dumps(payload,separators=(",",":")),
                ex=CACHE_TTL_S,
            )
            ok+=1
            stats[f"ok_{tf}"]=stats.get(f"ok_{tf}",0)+1
        else:
            fail+=1
            stats[f"fail_{tf}"]=stats.get(f"fail_{tf}",0)+1
            stats["last_error"]=f"{symbol}:{tf}:{err}"
    stats["symbols"]=stats.get("symbols",0)+1
    stats["last_symbol"]=symbol
    stats["last_fetch_ms"]=fetched
    stats["last_duration_ms"]=int((time.monotonic()-started)*1000)
    return ok,fail


async def heartbeat(r,assigned,stats,error=""):
    payload={
        "version":WORKER_VERSION,
        "shard_index":SHARD_INDEX,
        "shard_count":SHARD_COUNT,
        "assigned":assigned,
        "symbols":stats.get("symbols",0),
        "ok_1m":stats.get("ok_1m",0),
        "ok_5m":stats.get("ok_5m",0),
        "ok_15m":stats.get("ok_15m",0),
        "fail_1m":stats.get("fail_1m",0),
        "fail_5m":stats.get("fail_5m",0),
        "fail_15m":stats.get("fail_15m",0),
        "last_symbol":stats.get("last_symbol"),
        "last_fetch_ms":stats.get("last_fetch_ms",0),
        "last_error":stats.get("last_error",""),
        "heartbeat_ms":now_ms(),
        "error":error,
    }
    await r.set(HEARTBEAT_KEY,json.dumps(payload,separators=(",",":")),ex=15)


async def main():
    r=redis.from_url(REDIS_URL,encoding="utf-8",decode_responses=True)
    while True:
        try:
            await r.ping()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"PSI-RISK-WORKER redis_wait shard={SHARD_INDEX} {type(exc).__name__}: {exc}",flush=True)
            await asyncio.sleep(1.5)

    connector=aiohttp.TCPConnector(
        limit=max(HTTP_CONCURRENCY*2,16),
        limit_per_host=HTTP_CONCURRENCY,
        ttl_dns_cache=300,
        keepalive_timeout=45,
        family=2,
    )
    sem=asyncio.Semaphore(HTTP_CONCURRENCY)
    last_fetch:Dict[str,float]={}
    stats={}
    print(f"PSI-RISK-WORKER START version={WORKER_VERSION} shard={SHARD_INDEX+1}/{SHARD_COUNT}",flush=True)

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            symbols=await selected_symbols(r)
            if not symbols:
                await heartbeat(r,0,stats,"waiting_for_control_pool")
                await asyncio.sleep(POLL_S)
                continue

            now=time.monotonic()
            due=[s for s in symbols if now-last_fetch.get(s,0.0)>=REFRESH_S]
            queue=due[:BATCH_SIZE]
            if not queue:
                await heartbeat(r,len(symbols),stats)
                await asyncio.sleep(POLL_S)
                continue

            results=await asyncio.gather(
                *(publish_symbol(r,session,sem,sym,stats) for sym in queue),
                return_exceptions=True,
            )
            completed=time.monotonic()
            for sym,result in zip(queue,results):
                if not isinstance(result,BaseException):
                    last_fetch[sym]=completed
                else:
                    stats["last_error"]=f"{sym}:{type(result).__name__}:{result}"

            await heartbeat(r,len(symbols),stats)
            if stats.get("symbols",0)%40<len(queue):
                print(
                    f"PSI-RISK-WORKER progress shard={SHARD_INDEX+1}/{SHARD_COUNT} "
                    f"assigned={len(symbols)} symbols={stats.get('symbols',0)} "
                    f"ok1={stats.get('ok_1m',0)} ok5={stats.get('ok_5m',0)} ok15={stats.get('ok_15m',0)} "
                    f"fail1={stats.get('fail_1m',0)} fail5={stats.get('fail_5m',0)} fail15={stats.get('fail_15m',0)}",
                    flush=True,
                )
            await asyncio.sleep(0.05)


if __name__=="__main__":
    asyncio.run(main())
