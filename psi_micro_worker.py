import asyncio
import json
import os
import time
from typing import List, Tuple

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.3.0-distributed-micro-1"
ROLE = os.getenv("PSI_WORKER_ROLE", "TRADE").strip().upper()
REDIS_URL = os.getenv("REDIS_URL", "").strip()
CONTROL_KEY = os.getenv("PSI_MICRO_CONTROL_KEY", "psi:v12:selected").strip()
CHANNEL = "psi:v12:trade" if ROLE == "TRADE" else "psi:v12:depth"
HEARTBEAT_KEY = f"psi:v12:worker:{ROLE.lower()}"
WS_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_WORKER_WS_HOSTS",
        "wss://stream.binance.com:9443,wss://data-stream.binance.vision"
    ).split(",")
    if x.strip()
)
CONTROL_POLL_SECONDS = max(1.0, float(os.getenv("PSI_WORKER_CONTROL_POLL_SECONDS", "2")))
MAX_SYMBOLS = max(1, min(int(os.getenv("PSI_WORKER_MAX_SYMBOLS", "80")), 120))
RECONNECT_BACKOFF = max(0.5, float(os.getenv("PSI_WORKER_RECONNECT_BACKOFF", "1.5")))

if ROLE not in {"TRADE", "BOOK"}:
    raise RuntimeError(f"Unsupported PSI_WORKER_ROLE={ROLE}; expected TRADE or BOOK")
if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for distributed micro worker")


def now_ms() -> int:
    return int(time.time() * 1000)


async def selected_symbols(r) -> List[str]:
    raw = await r.get(CONTROL_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("symbols") or []
    out = []
    seen = set()
    for item in data if isinstance(data, list) else []:
        sym = str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            out.append(sym)
        if len(out) >= MAX_SYMBOLS:
            break
    return out


def stream_name(symbol: str) -> str:
    s = symbol.lower()
    return f"{s}@aggTrade" if ROLE == "TRADE" else f"{s}@depth20@100ms"


def combined_url(host: str, symbols: List[str]) -> str:
    streams = "/".join(stream_name(s) for s in symbols)
    return f"{host}/stream?streams={streams}"


async def publish_heartbeat(r, symbols: List[str], events: int, host: str, error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "role": ROLE,
        "symbols": len(symbols),
        "events": events,
        "host": host,
        "last_event_ms": now_ms(),
        "error": error,
    }
    await r.set(HEARTBEAT_KEY, json.dumps(payload, separators=(",", ":")), ex=15)


async def stream_once(r, session: aiohttp.ClientSession, symbols: List[str], host: str):
    url = combined_url(host, symbols)
    events = 0
    last_hb = 0.0
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=25)

    async with session.ws_connect(
        url,
        heartbeat=15,
        receive_timeout=25,
        autoping=True,
        timeout=timeout,
        max_msg_size=4 * 1024 * 1024,
    ) as ws:
        print(
            f"PSI-DISTRIBUTED-MICRO connected role={ROLE} symbols={len(symbols)} host={host}",
            flush=True,
        )
        await publish_heartbeat(r, symbols, events, host)
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    envelope = json.loads(msg.data)
                    data = envelope.get("data") if isinstance(envelope, dict) else None
                    if not isinstance(data, dict):
                        continue
                    stream = str(envelope.get("stream") or "")
                    symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
                    if not symbol.endswith("USDT"):
                        continue
                    if ROLE == "TRADE":
                        if "p" not in data or "q" not in data:
                            continue
                    else:
                        if not (data.get("bids") or data.get("b")) or not (data.get("asks") or data.get("a")):
                            continue
                    payload = json.dumps(
                        {"symbol": symbol, "data": data, "worker_ts": now_ms()},
                        separators=(",", ":"),
                    )
                    await r.publish(CHANNEL, payload)
                    events += 1
                    now = time.monotonic()
                    if now - last_hb >= 3.0:
                        await publish_heartbeat(r, symbols, events, host)
                        last_hb = now
                except Exception as exc:
                    print(f"PSI-DISTRIBUTED-MICRO event_error role={ROLE} {type(exc).__name__}: {exc}", flush=True)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break
    raise RuntimeError(f"websocket closed role={ROLE} host={host}")


async def main():
    r = redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
    await r.ping()
    print(f"PSI-DISTRIBUTED-MICRO START version={WORKER_VERSION} role={ROLE}", flush=True)
    host_index = 0

    timeout = aiohttp.ClientTimeout(total=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        active: Tuple[str, ...] = tuple()
        task = None
        while True:
            try:
                wanted = tuple(await selected_symbols(r))
                if not wanted:
                    if task:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass
                        task = None
                    active = tuple()
                    await publish_heartbeat(r, [], 0, "", "waiting_for_control_pool")
                    await asyncio.sleep(CONTROL_POLL_SECONDS)
                    continue

                if wanted != active or task is None or task.done():
                    if task:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass
                    active = wanted
                    host = WS_HOSTS[host_index % len(WS_HOSTS)]
                    host_index += 1
                    task = asyncio.create_task(stream_once(r, session, list(active), host))

                await asyncio.sleep(CONTROL_POLL_SECONDS)
                if task and task.done():
                    try:
                        task.result()
                    except Exception as exc:
                        print(f"PSI-DISTRIBUTED-MICRO reconnect role={ROLE} {type(exc).__name__}: {exc}", flush=True)
                    task = None
                    await asyncio.sleep(RECONNECT_BACKOFF)
            except asyncio.CancelledError:
                if task:
                    task.cancel()
                raise
            except Exception as exc:
                print(f"PSI-DISTRIBUTED-MICRO loop_error role={ROLE} {type(exc).__name__}: {exc}", flush=True)
                await asyncio.sleep(RECONNECT_BACKOFF)


if __name__ == "__main__":
    asyncio.run(main())
