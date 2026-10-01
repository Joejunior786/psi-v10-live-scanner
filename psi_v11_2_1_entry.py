import asyncio
import math

import psi_v11_2_entry as base
import ignition1081_app as ext_mod
import ignition120_entry as v120

VERSION = "11.0.2.1-adaptive-extension-live-cache"

app = base.app


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _inject_live_extension_telemetry(symbol, row):
    """Hydrate the adaptive guard from the live Binance 24h cache.

    V10.9.2 owns the active !ticker@arr websocket and continuously updates
    ignition1081_app.market_24h plus the feed heartbeat. Some later evaluator
    wrappers do not always preserve the full extension_guard dictionary even
    though the legacy hard-gate result survives. Reading this shared live cache
    directly removes that wrapper-boundary information loss without opening a
    second websocket or relaxing stale-data safety.
    """
    if not isinstance(row, dict) or not row:
        return row

    try:
        telemetry = ext_mod._telemetry_status(symbol) or {}
    except Exception:
        telemetry = {}

    status = str(telemetry.get("status") or "UNKNOWN")
    ticker = ext_mod.market_24h.get(symbol) or {}
    ext = dict(row.get("extension_guard") or {})

    if status == "LIVE" and ticker:
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
            "ticker_age_seconds": telemetry.get("ticker_age_seconds"),
            "extension_feed_age_seconds": telemetry.get("feed_age_seconds"),
            "extension_feed_connected": telemetry.get("feed_connected"),
            "extension_feed_symbol_count": telemetry.get("last_feed_symbol_count"),
            "adaptive_telemetry_source": "V10.9_SHARED_BINANCE_24H_CACHE",
        })
        row["change_24h_pct"] = round(change, 4)
    else:
        # Fail closed and truthful when the actual ticker heartbeat is stale.
        ext["extension_telemetry_status"] = status
        ext["ticker_age_seconds"] = telemetry.get("ticker_age_seconds")
        ext["extension_feed_age_seconds"] = telemetry.get("feed_age_seconds")
        ext["extension_feed_connected"] = telemetry.get("feed_connected")
        ext["extension_feed_symbol_count"] = telemetry.get("last_feed_symbol_count")
        ext["adaptive_telemetry_source"] = "V10.9_SHARED_BINANCE_24H_CACHE"

    row["extension_guard"] = ext
    return row


def adaptive_pre_multi_evaluate_11021(symbol):
    # Call the evaluator that existed before V11.0.2 patched V10.20, hydrate
    # its row from the actual shared ticker cache, then apply V11.0.2's
    # GREEN/AMBER/RED mathematics. V10.20 reads the resulting hard gate next.
    row = base._legacy_pre_multi_evaluate(symbol)
    if not row:
        return row
    try:
        row = _inject_live_extension_telemetry(symbol, row)
        return base._adaptive_extension(symbol, row)
    except Exception as exc:
        base.stats["ERROR"] += 1
        row.setdefault("extension_guard", {})["adaptive_extension_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
        # Fail closed by returning the lower evaluator's legacy hard-gate row.
        return row


# V10.20 evaluate_multi_regime resolves this global each call, so this is the
# exact pre-hard-gate integration point.
v120._old_evaluate = adaptive_pre_multi_evaluate_11021

base.VERSION = VERSION
base.base.VERSION = VERSION
base.base.v11.VERSION = VERSION
base.base.v11.base.VERSION = VERSION
base.base.v11.scanner.VERSION = VERSION
base.base.v11.scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.2.1] Adaptive extension telemetry bridge active: "
        "GREEN/AMBER/RED reads V10.9 shared live Binance 24h cache directly; "
        "stale/missing feed remains UNKNOWN and blocks execution.",
        flush=True,
    )
    await base.main()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        try:
            base.base.v11.persist_outcomes(force=True)
        except Exception:
            pass
        print("Psi-V11.0.2.1 stopped", flush=True)
