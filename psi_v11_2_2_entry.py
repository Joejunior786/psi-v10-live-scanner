import asyncio
import math
import time

import psi_v11_2_entry as base
import ignition120_entry as v120
import ignition1071_app as v71

VERSION = "11.0.2.4-adaptive-extension-miniticker"

app = base.app

WS_SYNC_SECONDS = 1.0
REST_MAX_AGE_SECONDS = 45.0

ext_cache = v71.mini_24h
ext_last_refresh = 0.0
ext_last_error = None
ext_refresh_ok = 0
ext_refresh_errors = 0
ext_failure_streak = 0


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _rest_status(symbol):
    now = time.time()
    row = ext_cache.get(symbol) or {}
    ts = f(row.get("ts"))
    feed_ts = f(getattr(v71, "mini_last_message_ts", 0.0))
    connected = bool(getattr(v71, "radar_mini_connected", False))
    feed_age = max(0.0, now - feed_ts) if feed_ts > 0 else None
    symbol_age = max(0.0, now - ts) if ts > 0 else None

    if not connected or feed_ts <= 0 or feed_age is None or feed_age > REST_MAX_AGE_SECONDS:
        status = "STALE_FEED"
    elif not row or ts <= 0:
        status = "MISSING_SYMBOL_SNAPSHOT"
    elif symbol_age is None or symbol_age > REST_MAX_AGE_SECONDS:
        status = "STALE_SYMBOL_SNAPSHOT"
    else:
        status = "LIVE"

    return {
        "status": status,
        "feed_age_seconds": round(feed_age, 2) if feed_age is not None else None,
        "ticker_age_seconds": round(symbol_age, 2) if symbol_age is not None else None,
        "feed_connected": status == "LIVE",
        "last_feed_symbol_count": int(getattr(v71, "mini_last_count", 0) or len(ext_cache)),
    }

def sync_extension_from_ws():
    global ext_last_refresh, ext_last_error, ext_refresh_ok, ext_refresh_errors, ext_failure_streak
    feed_ts=f(getattr(v71,"mini_last_message_ts",0.0))
    connected=bool(getattr(v71,"radar_mini_connected",False))
    if connected and feed_ts>0:
        if feed_ts>ext_last_refresh:
            ext_last_refresh=feed_ts
            ext_refresh_ok+=1
        ext_last_error=None
        ext_failure_streak=0
        return len(ext_cache)
    ext_failure_streak+=1
    ext_last_error="BINANCE_MINITICKER_WS_NOT_LIVE"
    return 0


def _inject_rest_extension(symbol, row):
    if not isinstance(row, dict) or not row:
        return row

    status = _rest_status(symbol)
    ticker = ext_cache.get(symbol) or {}
    ext = dict(row.get("extension_guard") or {})

    if status["status"] == "LIVE" and ticker:
        price = f(row.get("price"), f(ticker.get("last")))
        change = f(ticker.get("change_pct"))
        high = f(ticker.get("high"))
        low = f(ticker.get("low"))
        open_ = f(ticker.get("open"))

        pullback = (
            max(0.0, (high - price) / high * 100.0)
            if high > 0 and price > 0 and price < high
            else 0.0
        )
        from_low = (
            max(0.0, (price / low - 1.0) * 100.0)
            if price > 0 and low > 0
            else 0.0
        )

        ext.update({
            "change_24h_pct": round(change, 4),
            "open_24h": open_,
            "high_24h": high,
            "low_24h": low,
            "extension_from_24h_low_pct": round(from_low, 4),
            "pullback_from_24h_high_pct": round(pullback, 4),
            "extension_telemetry_status": "LIVE",
            "ticker_age_seconds": status["ticker_age_seconds"],
            "extension_feed_age_seconds": status["feed_age_seconds"],
            "extension_feed_connected": True,
            "extension_feed_symbol_count": status["last_feed_symbol_count"],
            "adaptive_telemetry_source": "BINANCE_PUBLIC_WS_MINITICKER_ALL",
        })
        row["change_24h_pct"] = round(change, 4)
    else:
        ext.update({
            "extension_telemetry_status": status["status"],
            "ticker_age_seconds": status["ticker_age_seconds"],
            "extension_feed_age_seconds": status["feed_age_seconds"],
            "extension_feed_connected": False,
            "extension_feed_symbol_count": status["last_feed_symbol_count"],
            "adaptive_telemetry_source": "BINANCE_PUBLIC_WS_MINITICKER_ALL",
        })

    row["extension_guard"] = ext
    return row


def adaptive_pre_multi_evaluate_11022(symbol):
    # Call the exact pre-V10.20 evaluator, hydrate extension telemetry from the
    # dedicated Binance public REST snapshot, then apply V11.0.2 GREEN/AMBER/RED
    # before V10.20 reads universal hard gates.
    row = base._legacy_pre_multi_evaluate(symbol)
    if not row:
        return row
    try:
        row = _inject_rest_extension(symbol, row)
        return base._adaptive_extension(symbol, row)
    except Exception as exc:
        base.stats["ERROR"] += 1
        row.setdefault("extension_guard", {})["adaptive_extension_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
        return row


v120._old_evaluate = adaptive_pre_multi_evaluate_11022

base.VERSION = VERSION
base.base.VERSION = VERSION
base.base.v11.VERSION = VERSION
base.base.v11.base.VERSION = VERSION
base.base.v11.scanner.VERSION = VERSION
base.base.v11.scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v11/{VERSION}"


async def extension_ws_sync_loop():
    global ext_last_refresh, ext_last_error, ext_refresh_ok, ext_refresh_errors, ext_failure_streak
    last_logged=0.0
    while True:
        try:
            n=sync_extension_from_ws()
            now=time.time()
            if n>0 and now-last_logged>=30.0:
                refs=[]
                for symbol in ("BTCUSDT","SOLUSDT","XRPUSDT"):
                    d=ext_cache.get(symbol) or {}
                    refs.append(f"{symbol}:P={d.get('change_pct')}")
                print(
                    f"Ψ-V11.0.2.4 EXTENSION_MINI symbols={n} ok={ext_refresh_ok} "
                    f"errors={ext_refresh_errors} " + " ".join(refs),
                    flush=True,
                )
                last_logged=now
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ext_refresh_errors+=1
            ext_last_error=f"{type(exc).__name__}: {exc}"
            print(f"Ψ-V11.0.2.4 EXTENSION_MINI_ERROR {ext_last_error}",flush=True)
        await asyncio.sleep(WS_SYNC_SECONDS)


async def telemetry_health_loop():
    while True:
        await asyncio.sleep(30)
        btc = _rest_status("BTCUSDT")
        print(
            f"Ψ-V11.0.2.4 EXTENSION_SOURCE status={btc['status']} "
            f"cache={len(ext_cache)} feedAge={btc['feed_age_seconds']}s "
            f"btcAge={btc['ticker_age_seconds']}s ok={ext_refresh_ok} "
            f"errors={ext_refresh_errors}",
            flush=True,
        )


async def main():
    print(
        "[v11.0.2.4] Adaptive extension guard production source: "
        "Binance public !miniTicker@arr WebSocket all-market feed; "
        "miniTicker heartbeat >45s becomes UNKNOWN and blocks execution.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        extension_ws_sync_loop(),
        telemetry_health_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        try:
            base.base.v11.persist_outcomes(force=True)
        except Exception:
            pass
        print("Psi-V11.0.2.4 stopped", flush=True)
