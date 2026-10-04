import asyncio
import json
import os
import time
from typing import List, Tuple

import aiohttp
import redis.asyncio as redis

WORKER_VERSION = "12.3.0-distributed-tape-1"
REDIS_URL = os.getenv("REDIS_URL", "").strip()
UNIVERSE_KEY = os.getenv("PSI_TAPE_UNIVERSE_KEY", "psi:v12:universe").strip()
SHARD_INDEX = max(0, int(os.getenv("PSI_TAPE_SHARD_INDEX", "0")))
SHARD_COUNT = max(1, int(os.getenv("PSI_TAPE_SHARD_COUNT", "2")))
POLL_SECONDS = max(1.0, float(os.getenv("PSI_TAPE_CONTROL_POLL_SECONDS", "3")))
RECONNECT_BACKOFF = max(0.5, float(os.getenv("PSI_TAPE_RECONNECT_BACKOFF", "1.5")))
TRADE_CHANNEL = "psi:v12:tape-trade"
BOOK_CHANNEL = "psi:v12:tape-book"
HEARTBEAT_KEY = f"psi:v12:tape-worker:{SHARD_INDEX}"
WS_HOSTS = tuple(
    x.strip().rstrip("/")
    for x in os.getenv(
        "PSI_TAPE_WS_HOSTS",
        "wss://stream.binance.com:9443,wss://data-stream.binance.vision"
    ).split(",")
    if x.strip()
)

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is required for distributed tape worker")
if SHARD_INDEX >= SHARD_COUNT:
    raise RuntimeError("PSI_TAPE_SHARD_INDEX must be less than PSI_TAPE_SHARD_COUNT")


def now_ms() -> int:
    return int(time.time() * 1000)


async def universe_symbols(r) -> List[str]:
    raw = await r.get(UNIVERSE_KEY)
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except Exception:
        return []
    if isinstance(data, dict):
        data = data.get("symbols") or []
    full = []
    seen = set()
    for item in data if isinstance(data, list) else []:
        sym = str(item or "").upper().strip()
        if sym.endswith("USDT") and sym not in seen:
            seen.add(sym)
            full.append(sym)
    full.sort()
    return full[SHARD_INDEX::SHARD_COUNT]


def subscription_streams(symbols: List[str]) -> List[str]:
    streams = []
    for sym in symbols:
        low = sym.lower()
        streams.append(f"{low}@aggTrade")
        streams.append(f"{low}@bookTicker")
    return streams


async def publish_heartbeat(r, symbols: List[str], trades: int, books: int, host: str, error: str = ""):
    payload = {
        "version": WORKER_VERSION,
        "shard_index": SHARD_INDEX,
        "shard_count": SHARD_COUNT,
        "symbols": len(symbols),
        "trades": trades,
        "books": books,
        "events": trades + books,
        "host": host,
        "last_event_ms": now_ms(),
        "error": error,
    }
    await r.set(HEARTBEAT_KEY, json.dumps(payload, separators=(",", ":")), ex=15)


async def stream_once(r, session: aiohttp.ClientSession, symbols: List[str], host: str):
    streams = subscription_streams(symbols)
    url = f"{host}/ws"
    trades = 0
    books = 0
    last_hb = 0.0

    async with session.ws_connect(
        url,
        heartbeat=20,
        receive_timeout=45,
        autoping=True,
        timeout=aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=45),
        max_msg_size=0,
    ) as ws:
        await ws.send_json({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 12000 + SHARD_INDEX,
        })
        print(
            f"PSI-DISTRIBUTED-TAPE connected shard={SHARD_INDEX+1}/{SHARD_COUNT} "
            f"symbols={len(symbols)} streams={len(streams)} host={host}",
            flush=True,
        )
        await publish_heartbeat(r, symbols, trades, books, host)

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    packet = json.loads(msg.data)
                    if isinstance(packet, dict) and packet.get("id") is not None and "result" in packet:
                        continue
                    data = packet.get("data") if isinstance(packet, dict) and isinstance(packet.get("data"), dict) else packet
                    if not isinstance(data, dict):
                        continue
                    event = str(data.get("e") or "")
                    symbol = str(data.get("s") or "").upper()
                    if not symbol.endswith("USDT"):
                        continue

                    if event == "aggTrade":
                        await r.publish(
                            TRADE_CHANNEL,
                            json.dumps({"symbol": symbol, "data": data, "worker_ts": now_ms()}, separators=(",", ":")),
                        )
                        trades += 1
                    else:
                        is_book = (
                            data.get("u") is not None
                            and data.get("b") is not None
                            and data.get("B") is not None
                            and data.get("a") is not None
                            and data.get("A") is not None
                            and data.get("p") is None
                        )
                        if is_book:
                            await r.publish(
                                BOOK_CHANNEL,
                                json.dumps({"symbol": symbol, "data": data, "worker_ts": now_ms()}, separators=(",", ":")),
                            )
                            books += 1

                    now = time.monotonic()
                    if now - last_hb >= 3.0:
                        await publish_heartbeat(r, symbols, trades, books, host)
                        last_hb = now
                except Exception as exc:
                    print(
                        f"PSI-DISTRIBUTED-TAPE event_error shard={SHARD_INDEX} "
                        f"{type(exc).__name__}: {exc}",
                        flush=True,
                    )
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    raise RuntimeError(f"websocket closed shard={SHARD_INDEX} host={host}")


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
                f"PSI-DISTRIBUTED-TAPE redis_wait shard={SHARD_INDEX} "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(RECONNECT_BACKOFF)

    print(
        f"PSI-DISTRIBUTED-TAPE START version={WORKER_VERSION} "
        f"shard={SHARD_INDEX+1}/{SHARD_COUNT}",
        flush=True,
    )

    host_index = SHARD_INDEX
    active: Tuple[str, ...] = tuple()
    task = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        while True:
            try:
                wanted = tuple(await universe_symbols(r))
                if not wanted:
                    if active and task is not None and not task.done():
                        await publish_heartbeat(
                            r, list(active), 0, 0, "", "control_stale_holding_last_universe"
                        )
                        await asyncio.sleep(POLL_SECONDS)
                        continue
                    if task:
                        task.cancel()
                        try:
                            await task
                        except BaseException:
                            pass
                        task = None
                    if active:
                        print(
                            f"PSI-DISTRIBUTED-TAPE control_update shard={SHARD_INDEX} symbols=0",
                            flush=True,
                        )
                    active = tuple()
                    await publish_heartbeat(r, [], 0, 0, "", "waiting_for_universe")
                    await asyncio.sleep(POLL_SECONDS)
                    continue

                if wanted != active or task is None or task.done():
                    if wanted != active:
                        print(
                            f"PSI-DISTRIBUTED-TAPE control_update shard={SHARD_INDEX} "
                            f"symbols={len(wanted)} preview={','.join(wanted[:6])}",
                            flush=True,
                        )
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

                await asyncio.sleep(POLL_SECONDS)
                if task and task.done():
                    try:
                        task.result()
                    except Exception as exc:
                        print(
                            f"PSI-DISTRIBUTED-TAPE reconnect shard={SHARD_INDEX} "
                            f"{type(exc).__name__}: {exc}",
                            flush=True,
                        )
                    task = None
                    await asyncio.sleep(RECONNECT_BACKOFF)
            except asyncio.CancelledError:
                if task:
                    task.cancel()
                raise
            except Exception as exc:
                print(
                    f"PSI-DISTRIBUTED-TAPE loop_error shard={SHARD_INDEX} "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                await asyncio.sleep(RECONNECT_BACKOFF)


if __name__ == "__main__":
    asyncio.run(main())
