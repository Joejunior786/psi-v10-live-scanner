import asyncio
import json
import os
import time

import aiohttp
import redis.asyncio as redis

WORKER_VERSION="12.3.0-extension-worker-2"
REDIS_URL=os.getenv("REDIS_URL","").strip()
SNAPSHOT_KEY=os.getenv("PSI_EXTENSION_SNAPSHOT_KEY","psi:v12:extension-snapshot").strip()
REFRESH_SECONDS=max(1.0,float(os.getenv("PSI_EXTENSION_REFRESH_SECONDS","3")))
TTL_SECONDS=max(15,int(os.getenv("PSI_EXTENSION_SNAPSHOT_TTL_SECONDS","45")))
FULL_REFRESH_SECONDS=max(15.0,float(os.getenv("PSI_EXTENSION_FULL_REFRESH_SECONDS","25")))
WS_HOSTS=tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_EXTENSION_WS_HOSTS",
        "wss://data-stream.binance.vision,wss://stream.binance.com:443,wss://stream.binance.com:9443"
    ).split(",")
    if x.strip()
)
REST_HOSTS=tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_EXTENSION_REST_HOSTS",
        "https://data-api.binance.vision,https://api-gcp.binance.com,https://api.binance.com"
    ).split(",")
    if x.strip()
)
HEARTBEAT_KEY="psi:v12:extension-worker"

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required")


def now_ms():
    return int(time.time()*1000)


def merge_rows(state, rows):
    accepted=0
    for x in rows if isinstance(rows,list) else []:
        if not isinstance(x,dict):
            continue
        sym=str(x.get("s") or x.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        last=x.get("c") if x.get("c") is not None else x.get("lastPrice")
        try:
            lastf=float(last or 0)
        except (TypeError,ValueError):
            lastf=0.0
        if lastf<=0:
            continue
        state[sym]=dict(x)
        accepted+=1
    return accepted


async def publish_snapshot(r, state, source, events):
    rows=list(state.values())
    ts=now_ms()
    payload={
        "version":WORKER_VERSION,
        "generated_ms":ts,
        "source":source,
        "symbols":len(rows),
        "events":events,
        "rows":rows,
    }
    await r.set(SNAPSHOT_KEY,json.dumps(payload,separators=(",",":")),ex=TTL_SECONDS)
    await r.set(
        HEARTBEAT_KEY,
        json.dumps({
            "version":WORKER_VERSION,
            "generated_ms":ts,
            "source":source,
            "symbols":len(rows),
            "events":events,
        },separators=(",",":")),
        ex=15,
    )
    return len(rows)


async def fetch_rest_rows(session, host):
    url=host+"/api/v3/ticker/24hr"
    timeout=aiohttp.ClientTimeout(total=6.0,connect=1.5,sock_read=4.5)
    async with session.get(
        url,
        params={"type":"MINI","symbolStatus":"TRADING"},
        timeout=timeout,
    ) as resp:
        if resp.status!=200:
            body=await resp.text()
            raise RuntimeError(f"{host} HTTP {resp.status}: {body[:160]}")
        rows=await resp.json()
    if not isinstance(rows,list) or not rows:
        raise RuntimeError("empty extension REST snapshot")
    return rows


async def periodic_rest_loop(session, r, state):
    host_cursor=0
    while True:
        ok=False
        for offset in range(len(REST_HOSTS)):
            host=REST_HOSTS[(host_cursor+offset)%len(REST_HOSTS)]
            try:
                rows=await fetch_rest_rows(session,host)
                merged=merge_rows(state,rows)
                host_cursor=(host_cursor+offset)%len(REST_HOSTS)
                n=await publish_snapshot(r,state,f"REST_FULL:{host}",0)
                print(
                    f"PSI-EXTENSION-WORKER full_snapshot symbols={n} merged={merged} host={host}",
                    flush=True,
                )
                ok=True
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(
                    f"PSI-EXTENSION-WORKER rest_error {type(exc).__name__}: {exc}",
                    flush=True,
                )
        await asyncio.sleep(FULL_REFRESH_SECONDS if ok else 3.0)


async def ws_loop(session, r, host, state):
    url=host+"/ws/!miniTicker@arr"
    events=0
    last_publish=0.0
    timeout=aiohttp.ClientTimeout(total=None,sock_connect=10,sock_read=35)
    async with session.ws_connect(
        url,
        heartbeat=15,
        receive_timeout=35,
        autoping=True,
        timeout=timeout,
        max_msg_size=8*1024*1024,
    ) as ws:
        print(f"PSI-EXTENSION-WORKER connected host={host}",flush=True)
        async for msg in ws:
            if msg.type==aiohttp.WSMsgType.TEXT:
                try:
                    packet=json.loads(msg.data)
                    rows=packet.get("data") if isinstance(packet,dict) and isinstance(packet.get("data"),list) else packet
                    if not isinstance(rows,list):
                        continue
                    merged=merge_rows(state,rows)
                    events+=1
                    now=time.monotonic()
                    if now-last_publish>=REFRESH_SECONDS:
                        n=await publish_snapshot(r,state,f"WS:{host}",events)
                        print(
                            f"PSI-EXTENSION-WORKER snapshot symbols={n} merged={merged} events={events} source=WS",
                            flush=True,
                        )
                        last_publish=now
                except Exception as exc:
                    print(f"PSI-EXTENSION-WORKER event_error {type(exc).__name__}: {exc}",flush=True)
            elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                break
    raise RuntimeError(f"extension websocket closed host={host}")


async def main():
    r=redis.from_url(REDIS_URL,encoding="utf-8",decode_responses=True)
    while True:
        try:
            await r.ping()
            break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"PSI-EXTENSION-WORKER redis_wait {type(exc).__name__}: {exc}",flush=True)
            await asyncio.sleep(1.5)

    print(f"PSI-EXTENSION-WORKER START version={WORKER_VERSION}",flush=True)
    connector=aiohttp.TCPConnector(limit=8,limit_per_host=4,ttl_dns_cache=300,keepalive_timeout=45,family=2)
    state={}
    async with aiohttp.ClientSession(connector=connector) as session:
        rest_task=asyncio.create_task(periodic_rest_loop(session,r,state))
        host_index=0
        try:
            while True:
                host=WS_HOSTS[host_index%len(WS_HOSTS)]
                try:
                    await ws_loop(session,r,host,state)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"PSI-EXTENSION-WORKER ws_error {type(exc).__name__}: {exc}",flush=True)
                    host_index=(host_index+1)%len(WS_HOSTS)
                    await asyncio.sleep(1.0)
        finally:
            rest_task.cancel()
            try:
                await rest_task
            except BaseException:
                pass
            await r.aclose()


if __name__=="__main__":
    asyncio.run(main())
