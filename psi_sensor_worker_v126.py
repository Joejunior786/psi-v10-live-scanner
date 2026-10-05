import asyncio
import json
import os
import time
from collections import defaultdict
from contextlib import suppress

import aiohttp
import psi_sensor_worker as base

VERSION = "12.6.1-decoupled-publisher+event-loop-fairness"
PUBLISH_SECONDS = max(0.5, float(os.getenv("PSI_SENSOR_SNAPSHOT_SECONDS", "1.0")))
HEARTBEAT_SECONDS = max(1.0, float(os.getenv("PSI_SENSOR_HEARTBEAT_SECONDS", "3.0")))
DIAG_SECONDS = max(5.0, float(os.getenv("PSI_SENSOR_DIAG_SECONDS", "10.0")))
RV10_MIN_BASELINE_QUOTE = max(1.0, float(os.getenv("PSI_SENSOR_RV10_MIN_BASELINE_QUOTE", "25")))
RV30_MIN_BASELINE_QUOTE = max(1.0, float(os.getenv("PSI_SENSOR_RV30_MIN_BASELINE_QUOTE", "75")))
RV_RATIO_CAP = max(2.0, float(os.getenv("PSI_SENSOR_RV_RATIO_CAP", "25")))
EVENT_YIELD_EVERY = max(8, min(256, int(os.getenv("PSI_SENSOR_EVENT_YIELD_EVERY", "32"))))

_original_trade_metrics = base.trade_metrics


def safe_trade_metrics(state, now):
    out = dict(_original_trade_metrics(state, now))
    buckets = list(state.get("buckets") or [])
    prev10 = base.agg_buckets(base.trade_window(buckets, now, 10, 10))
    prev60 = base.agg_buckets(base.trade_window(buckets, now, 60, 60))
    rv10_ok = float(prev10.get("quote") or 0.0) >= RV10_MIN_BASELINE_QUOTE
    rv30_ok = float(prev60.get("quote") or 0.0) >= RV30_MIN_BASELINE_QUOTE
    out["relative_volume_10s_baseline_ok"] = rv10_ok
    out["relative_volume_30s_baseline_ok"] = rv30_ok
    out["relative_volume_10s"] = min(RV_RATIO_CAP, float(out.get("relative_volume_10s") or 0.0)) if rv10_ok else 1.0
    out["relative_volume_30s"] = min(RV_RATIO_CAP, float(out.get("relative_volume_30s") or 0.0)) if rv30_ok else 1.0
    return out


base.trade_metrics = safe_trade_metrics
base.VERSION = VERSION
base.SNAPSHOT_KEY = f"psi:v12.6:sensor:{base.SHARD_INDEX}"
base.HEARTBEAT_KEY = f"psi:v12.6:sensor-heartbeat:{base.SHARD_INDEX}"


def _should_yield(events):
    return int(events or 0) > 0 and int(events) % EVENT_YIELD_EVERY == 0


async def _publisher_loop(r, symbols, states, counters, host, stop_event):
    next_publish = time.monotonic()
    last_hb = 0.0
    last_diag = 0.0
    while not stop_event.is_set():
        now_mono = time.monotonic()
        if now_mono < next_publish:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=next_publish - now_mono)
                break
            except asyncio.TimeoutError:
                pass
        cycle_started = time.monotonic()
        try:
            await base.publish_snapshot(r, symbols, states, counters["events"], host)
            counters["snapshot_ms"] = base.now_ms()
            counters["publish_count"] += 1
        except Exception as exc:
            counters["publish_errors"] += 1
            print(
                f"PSI-SENSOR-V126 SNAPSHOT_ERROR shard={base.SHARD_INDEX} {type(exc).__name__}:{exc}",
                flush=True,
            )

        mono = time.monotonic()
        if mono - last_hb >= HEARTBEAT_SECONDS:
            try:
                await base.publish_heartbeat(r, symbols, counters["events"], host)
            except Exception as exc:
                print(
                    f"PSI-SENSOR-V126 HEARTBEAT_ERROR shard={base.SHARD_INDEX} {type(exc).__name__}:{exc}",
                    flush=True,
                )
            last_hb = mono

        if mono - last_diag >= DIAG_SECONDS:
            now = base.now_ms()
            trade_fresh = book_fresh = metric_symbols = 0
            for sym in symbols:
                st = states.get(sym)
                if not st:
                    continue
                metric_symbols += 1
                tm = safe_trade_metrics(st["trade"], now)
                bm = base.book_metrics(st["book"], now)
                trade_fresh += int(bool(tm.get("trade_fresh")))
                book_fresh += int(bool(bm.get("book_fresh")))
            lag_ms = max(0.0, (time.monotonic() - cycle_started) * 1000.0)
            print(
                f"PSI-SENSOR-V126 LIVE shard={base.SHARD_INDEX}/{base.SHARD_COUNT} "
                f"symbols={len(symbols)} metrics={metric_symbols} events={counters['events']} "
                f"tradeFresh={trade_fresh} bookFresh={book_fresh} "
                f"publishes={counters['publish_count']} publishErrors={counters['publish_errors']} "
                f"publishWorkMs={lag_ms:.1f} cadence={PUBLISH_SECONDS:.2f}s host={host}",
                flush=True,
            )
            last_diag = mono

        next_publish += PUBLISH_SECONDS
        if next_publish < time.monotonic() - PUBLISH_SECONDS:
            next_publish = time.monotonic() + PUBLISH_SECONDS


async def stream_once(r, session, symbols, host):
    url = f"{host}/ws"
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=12, sock_read=30)
    states = defaultdict(base.symbol_state)
    symbol_set = set(symbols)
    counters = {"events": 0, "publish_count": 0, "publish_errors": 0, "snapshot_ms": 0}
    started = time.monotonic()
    stop_event = asyncio.Event()

    async with session.ws_connect(
        url,
        heartbeat=15,
        receive_timeout=30,
        autoping=True,
        timeout=timeout,
        max_msg_size=8 * 1024 * 1024,
    ) as ws:
        req = 1
        await ws.send_json({"method": "SET_PROPERTY", "params": ["combined", True], "id": req})
        req += 1
        req = await base.subscribe(ws, symbols, req)
        print(
            f"PSI-SENSOR-V126 CONNECTED shard={base.SHARD_INDEX}/{base.SHARD_COUNT} "
            f"symbols={len(symbols)} streams={len(symbols)*2} cadence={PUBLISH_SECONDS:.2f}s "
            f"yieldEvery={EVENT_YIELD_EVERY} host={host}",
            flush=True,
        )

        publisher = asyncio.create_task(
            _publisher_loop(r, symbols, states, counters, host, stop_event),
            name=f"psi-v126-publisher-{base.SHARD_INDEX}",
        )
        try:
            async for msg in ws:
                if time.monotonic() - started >= base.UNIVERSE_REFRESH_SECONDS:
                    raise RuntimeError("scheduled_universe_refresh")
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        envelope = json.loads(msg.data)
                        if isinstance(envelope, dict) and envelope.get("id") is not None and "result" in envelope:
                            continue
                        data = envelope.get("data") if isinstance(envelope, dict) else None
                        if not isinstance(data, dict):
                            continue
                        stream = str(envelope.get("stream") or "")
                        symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
                        if symbol not in symbol_set:
                            continue
                        accepted = False
                        if stream.endswith("@aggTrade") or ("p" in data and "q" in data and "m" in data):
                            accepted = base.record_trade(states[symbol]["trade"], data)
                        elif "@depth" in stream or data.get("lastUpdateId") or data.get("u"):
                            accepted = base.record_book(states[symbol]["book"], data)
                        if accepted:
                            counters["events"] += 1
                            if _should_yield(counters["events"]):
                                await asyncio.sleep(0)
                    except Exception as exc:
                        print(
                            f"PSI-SENSOR-V126 EVENT_ERROR shard={base.SHARD_INDEX} {type(exc).__name__}:{exc}",
                            flush=True,
                        )
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            stop_event.set()
            publisher.cancel()
            with suppress(asyncio.CancelledError):
                await publisher
    raise RuntimeError("websocket_closed")


base.stream_once = stream_once


if __name__ == "__main__":
    asyncio.run(base.main())
