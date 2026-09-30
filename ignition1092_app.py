import asyncio
import json
import time

import aiohttp
import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8
import ignition1081_app as v81
import ignition109_app as v9
import ignition1091_app as core

VERSION = "10.9-latent-cluster-extension-guard"
_original_metric = v9.metric

core.VERSION = VERSION
v9.VERSION = VERSION
v81.VERSION = VERSION
v8.VERSION = VERSION
v7.VERSION = VERSION
base.VERSION = VERSION
app.USER_AGENT = "psi-v10-live-scanner/10.9-latent-cluster-extension-guard"


def _trade_accel(cur_amount, cur_seconds, prev_amount, prev_seconds):
    cur_rate = cur_amount / max(cur_seconds, 1.0)
    prev_rate = prev_amount / max(prev_seconds, 1.0)
    if cur_rate <= 0:
        return 0.0
    if prev_rate > 0:
        return min(50.0, cur_rate / prev_rate)
    return min(12.0, 1.0 + cur_amount)


def metric(symbol):
    r = _original_metric(symbol)
    samples = base.radar_hist.get(symbol)
    if not samples or len(samples) < 5:
        return r

    t = samples[-1][0]
    n5 = v9._positive_increment(samples, t - 5, t, 3)
    n_prev10 = v9._positive_increment(samples, t - 15, t - 5, 3)
    n15 = v9._positive_increment(samples, t - 15, t, 3)
    n_prev15 = v9._positive_increment(samples, t - 30, t - 15, 3)
    t5x = _trade_accel(n5, 5, n_prev10, 10)
    t15x = _trade_accel(n15, 15, n_prev15, 15)

    r["trade_accel_5"] = round(t5x, 3)
    r["trade_accel_15"] = round(t15x, 3)
    r["trades_5s"] = int(n5)

    r5 = float(r.get("r5") or 0)
    r15 = float(r.get("r15") or 0)
    r30 = float(r.get("r30") or 0)
    r60 = float(r.get("r60") or 0)
    spread = float(r.get("spread_bps") or 0)
    range60 = float(r.get("range60_pct") or 999)
    v5x = float(r.get("vol_accel_5") or 0)
    v15x = float(r.get("vol_accel_15") or 0)

    quiet = (
        abs(r5) <= v9.LATENT_PRICE_5S_MAX
        and abs(r15) <= v9.LATENT_PRICE_15S_MAX
        and abs(r30) <= v9.LATENT_PRICE_30S_MAX
    )
    compressed = range60 <= v9.LATENT_RANGE60_MAX
    trade_burst = t5x >= v9.LATENT_TRADES5_MIN or t15x >= v9.LATENT_TRADES15_MIN
    latent = bool(
        r.get("latent_ignition")
        or (quiet and compressed and trade_burst and spread <= 30 and r60 < 6.0)
    )

    if latent and t - v9.last_burst[symbol] >= v9.BURST_MIN_GAP:
        v9.burst_hist[symbol].append(t)
        v9.last_burst[symbol] = t
        v9.first_latent.setdefault(symbol, t)

    counts = v9._burst_counts(symbol, t)
    clustered = counts["5m"] >= 2 or counts["15m"] >= 3 or counts["30m"] >= 4

    displacement = max(abs(r15), abs(r5) * 0.5, 0.05)
    divergence = min(250.0, max(v5x, v15x, t5x, t15x) / displacement)

    score = float(r.get("score") or 0)
    score += max(0.0, min(t5x - 1.0, 8.0)) * 6.0
    score += max(0.0, min(t15x - 1.0, 6.0)) * 3.0
    if latent and not r.get("latent_ignition"):
        score += 12.0

    if r15 >= 1.5 or r30 >= 3.0:
        phase = "EXPANSION"
    elif clustered and (r5 >= 0.15 or r15 >= 0.30) and (v5x >= 3.0 or t5x >= 2.2):
        phase = "IGNITION"
    elif latent:
        phase = "LATENT_IGNITION"
    elif clustered:
        phase = "IGNITION_BUILDING"
    else:
        phase = r.get("phase", "RADAR")

    normal_accel = (
        r5 >= 0.10 or r15 >= 0.20 or v5x >= 1.6 or t5x >= 1.6
        or (v15x >= 1.4 and t15x >= 1.4)
    )
    trigger = bool(
        v7._directional(symbol)
        and spread <= 30
        and (normal_accel or latent or clustered)
        and score >= base.TRIGGER_SCORE
    )

    first = v9.burst_hist[symbol][0] if v9.burst_hist[symbol] else 0.0
    r.update({
        "score": round(score, 3),
        "trigger": trigger,
        "phase": phase,
        "latent_ignition": latent,
        "clustered_ignition": clustered,
        "activity_price_divergence": round(divergence, 3),
        "burst_count_5m": counts["5m"],
        "burst_count_15m": counts["15m"],
        "burst_count_30m": counts["30m"],
        "ignition_velocity_seconds": round(t - first, 1) if first else None,
        "trade_counter_source": "LAST_TRADE_ID_DELTA",
    })
    return r


async def ticker_loop():
    url = f"{app.WS_BASE}/ws/!ticker@arr"
    while True:
        try:
            async with app.session.ws_connect(url, heartbeat=30, receive_timeout=90, max_msg_size=0) as ws:
                v81.extension_ticker_connected = True
                base.radar_ticker_connected = True
                print("Ψ-V10.9 EXTENSION/RADAR ticker WS connected (!ticker@arr, trade_id=L)", flush=True)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(payload, list):
                            continue
                        ts = time.time()
                        # V10.10 runs this ticker loop, not v81.ticker_loop. Keep
                        # the extension guard's feed heartbeat in sync here so
                        # extension telemetry is judged by the feed actually in use.
                        v81.extension_ticker_last_message_ts = ts
                        v81.extension_ticker_last_count = len(payload)
                        for x in payload:
                            if not isinstance(x, dict):
                                continue
                            sym = x.get("s", "")
                            last = app.safe_float(x.get("c"))
                            v81.market_24h[sym] = {
                                "change_pct": app.safe_float(x.get("P")),
                                "open": app.safe_float(x.get("o")),
                                "high": app.safe_float(x.get("h")),
                                "low": app.safe_float(x.get("l")),
                                "last": last,
                                "ts": ts,
                            }
                            base.push(
                                sym,
                                last,
                                app.safe_float(x.get("q")),
                                app.safe_float(x.get("L")),
                                app.safe_float(x.get("b")),
                                app.safe_float(x.get("a")),
                                "ticker",
                            )
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.last_error = f"V10.9_TICKER: {type(exc).__name__}: {exc}"
            print(app.last_error, flush=True)
        finally:
            v81.extension_ticker_connected = False
            base.radar_ticker_connected = False
        await asyncio.sleep(2)


v9.metric = metric
base.metric = metric
v7.ignition_metric = metric
base.ticker_loop = ticker_loop

app.evaluate_symbol = core.evaluate
app.health = core.health
app.scan_endpoint = core.scan
app.ranked_results = s.results
v7.rapid_websocket_loop = v9.persistent_rapid_websocket_loop
v7.print_loop = v9.print_loop
q.print_loop = v9.print_loop
s.print_loop = v9.print_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.9 ACTIVE — latent ignition + burst clustering + rolling pressure + "
            "trade-ID acceleration + persistent rapid subscriptions + extension guard",
            flush=True,
        )
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.9 stopped", flush=True)
