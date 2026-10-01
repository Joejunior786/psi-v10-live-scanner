import asyncio
import math
import time

import psi_v11_2_entry as base
import ignition120_entry as v120

VERSION = "11.0.2.2-adaptive-extension-rest-snapshot"

app = base.app

REST_REFRESH_SECONDS = 30.0
REST_MAX_AGE_SECONDS = 45.0

ext_cache = {}
ext_last_refresh = 0.0
ext_last_error = None
ext_refresh_ok = 0
ext_refresh_errors = 0


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
    feed_age = max(0.0, now - ext_last_refresh) if ext_last_refresh > 0 else None
    symbol_age = max(0.0, now - ts) if ts > 0 else None

    if ext_last_refresh <= 0 or feed_age is None or feed_age > REST_MAX_AGE_SECONDS:
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
        "last_feed_symbol_count": len(ext_cache),
    }


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
            "adaptive_telemetry_source": "BINANCE_PUBLIC_REST_24H_ALL",
        })
        row["change_24h_pct"] = round(change, 4)
    else:
        ext.update({
            "extension_telemetry_status": status["status"],
            "ticker_age_seconds": status["ticker_age_seconds"],
            "extension_feed_age_seconds": status["feed_age_seconds"],
            "extension_feed_connected": False,
            "extension_feed_symbol_count": status["last_feed_symbol_count"],
            "adaptive_telemetry_source": "BINANCE_PUBLIC_REST_24H_ALL",
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


async def extension_rest_loop():
    global ext_last_refresh, ext_last_error, ext_refresh_ok, ext_refresh_errors

    # Wait for the scanner to create its shared aiohttp session.
    while app.session is None:
        await asyncio.sleep(0.25)

    while True:
        cycle_start = time.time()
        try:
            payload = await app.api_get(app.session, "/api/v3/ticker/24hr")
            if not isinstance(payload, list):
                raise RuntimeError(f"unexpected ticker/24hr payload type={type(payload).__name__}")

            ts = time.time()
            new_cache = {}
            for item in payload:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "")
                if not symbol:
                    continue
                new_cache[symbol] = {
                    "change_pct": f(item.get("priceChangePercent")),
                    "open": f(item.get("openPrice")),
                    "high": f(item.get("highPrice")),
                    "low": f(item.get("lowPrice")),
                    "last": f(item.get("lastPrice")),
                    "ts": ts,
                }

            if not new_cache:
                raise RuntimeError("ticker/24hr returned no usable symbols")

            ext_cache.clear()
            ext_cache.update(new_cache)
            ext_last_refresh = ts
            ext_last_error = None
            ext_refresh_ok += 1

            refs = []
            for symbol in ("BTCUSDT", "SOLUSDT", "XRPUSDT"):
                d = ext_cache.get(symbol) or {}
                refs.append(f"{symbol}:P={d.get('change_pct')}")
            print(
                f"Ψ-V11.0.2.2 EXTENSION_REST symbols={len(ext_cache)} ok={ext_refresh_ok} "
                f"errors={ext_refresh_errors} " + " ".join(refs),
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            ext_refresh_errors += 1
            ext_last_error = f"{type(exc).__name__}: {exc}"
            print(
                f"Ψ-V11.0.2.2 EXTENSION_REST_ERROR {ext_last_error}",
                flush=True,
            )

        elapsed = time.time() - cycle_start
        await asyncio.sleep(max(1.0, REST_REFRESH_SECONDS - elapsed))


async def telemetry_health_loop():
    while True:
        await asyncio.sleep(30)
        btc = _rest_status("BTCUSDT")
        print(
            f"Ψ-V11.0.2.2 EXTENSION_SOURCE status={btc['status']} "
            f"cache={len(ext_cache)} feedAge={btc['feed_age_seconds']}s "
            f"btcAge={btc['ticker_age_seconds']}s ok={ext_refresh_ok} "
            f"errors={ext_refresh_errors}",
            flush=True,
        )


async def main():
    print(
        "[v11.0.2.2] Adaptive extension guard production source: "
        "Binance public /api/v3/ticker/24hr all-market snapshot every 30s; "
        "snapshots >45s become UNKNOWN and block execution.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        extension_rest_loop(),
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
        print("Psi-V11.0.2.2 stopped", flush=True)
