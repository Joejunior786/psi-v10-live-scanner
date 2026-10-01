import asyncio
import math
import os
import time
from collections import deque

import psi_v11_entry as v11

VERSION = "11.0.1-cross-venue-resilience"
KUCOIN_TICKERS_URL = os.getenv(
    "PSI_KUCOIN_TICKERS_URL",
    "https://api.kucoin.com/api/v1/market/allTickers",
)
# Bybit officially rejects US-located IP addresses with HTTP 403. Railway is
# currently hosted in a US region, so do not repeatedly hammer a known-blocked
# endpoint. It can be explicitly re-enabled if the service moves regions.
BYBIT_ENABLED = os.getenv("PSI_BYBIT_ENABLED", "0") == "1"

v11.VERSION = VERSION
v11.base.VERSION = VERSION
v11.scanner.VERSION = VERSION
v11.scanner.v7.VERSION = VERSION
v11.app.USER_AGENT = f"psi-v11/{VERSION}"

v11.venue_prices.setdefault("kucoin", {})
v11.venue_stats.setdefault("kucoin_ok", 0)
v11.venue_stats.setdefault("kucoin_err", 0)
v11.venue_stats.setdefault("last_kucoin", 0.0)
v11.venue_stats.setdefault("bybit_geo_disabled", 0 if BYBIT_ENABLED else 1)


def _safe_float(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _ensure_hist(symbol, venue):
    bucket = v11.venue_history[symbol]
    if venue not in bucket:
        bucket[venue] = deque(maxlen=120)
    return bucket[venue]


async def poll_cross_venues():
    """Fetch independent public spot snapshots without private API keys.

    OKX + KuCoin are the normal two external legs in the current Railway
    region. Bybit remains optional and automatically excluded by default
    because its official API blocks US-origin IPs.
    """
    if v11.app.session is None:
        return

    requests = [
        v11._fetch_json(v11.app.session, v11.OKX_TICKERS_URL, {"instType": "SPOT"}),
        v11._fetch_json(v11.app.session, KUCOIN_TICKERS_URL),
    ]
    if BYBIT_ENABLED:
        requests.append(
            v11._fetch_json(v11.app.session, v11.BYBIT_TICKERS_URL, {"category": "spot"})
        )

    results = await asyncio.gather(*requests)
    okx = results[0]
    kucoin = results[1]
    bybit = results[2] if BYBIT_ENABLED and len(results) > 2 else None
    t = time.time()

    if isinstance(okx, dict) and str(okx.get("code")) == "0":
        parsed = {}
        for row in okx.get("data") or []:
            inst = str(row.get("instId") or "")
            if not inst.endswith("-USDT"):
                continue
            symbol = inst.replace("-", "")
            price = _safe_float(row.get("last"))
            if price > 0:
                parsed[symbol] = price
        v11.venue_prices["okx"] = parsed
        v11.venue_stats["okx_ok"] += 1
        v11.venue_stats["last_okx"] = t
    else:
        v11.venue_stats["okx_err"] += 1

    kucoin_ok = isinstance(kucoin, dict) and str(kucoin.get("code")) == "200000"
    if kucoin_ok:
        parsed = {}
        data = kucoin.get("data") or {}
        for row in data.get("ticker") or []:
            inst = str(row.get("symbol") or "")
            if not inst.endswith("-USDT"):
                continue
            symbol = inst.replace("-", "")
            price = _safe_float(row.get("last"))
            if price > 0:
                parsed[symbol] = price
        v11.venue_prices["kucoin"] = parsed
        v11.venue_stats["kucoin_ok"] += 1
        v11.venue_stats["last_kucoin"] = t
    else:
        v11.venue_stats["kucoin_err"] += 1

    if BYBIT_ENABLED:
        try:
            bybit_ok = isinstance(bybit, dict) and int(bybit.get("retCode", -1)) == 0
        except (TypeError, ValueError):
            bybit_ok = False
        if bybit_ok:
            parsed = {}
            result = bybit.get("result") or {}
            for row in result.get("list") or []:
                symbol = str(row.get("symbol") or "")
                if not symbol.endswith("USDT"):
                    continue
                price = _safe_float(row.get("lastPrice"))
                if price > 0:
                    parsed[symbol] = price
            v11.venue_prices["bybit"] = parsed
            v11.venue_stats["bybit_ok"] += 1
            v11.venue_stats["last_bybit"] = t
        else:
            v11.venue_stats["bybit_err"] += 1


# Replace the original helper because the original implementation assumes only
# OKX and Bybit. Returns are compared rather than absolute prices, so venue
# tick-size / minor basis differences do not contaminate the lead-lag signal.
def cross_venue_leadlag(symbol):
    bh = v11.radar_feed.radar_hist.get(symbol)
    if not bh or len(bh) < 5:
        return {"symbol": symbol, "ready": False, "score": 0.0, "venues": 0}

    bt, bp = _safe_float(bh[-1][0]), _safe_float(bh[-1][1])
    b5 = v11.before(bh, bt - 5)
    b15 = v11.before(bh, bt - 15)
    br5 = v11.pct(bp, _safe_float(b5[1])) if b5 and _safe_float(b5[1]) > 0 else 0.0
    br15 = v11.pct(bp, _safe_float(b15[1])) if b15 and _safe_float(b15[1]) > 0 else 0.0

    ext5, ext15, names = [], [], []
    venues = ["okx", "kucoin"] + (["bybit"] if BYBIT_ENABLED else [])
    for venue in venues:
        r5 = _venue_return(symbol, venue, 5)
        r15 = _venue_return(symbol, venue, 15)
        if r5 is not None and r15 is not None:
            ext5.append(r5)
            ext15.append(r15)
            names.append(venue)

    if not ext5:
        return {
            "symbol": symbol,
            "ready": False,
            "score": 0.0,
            "venues": 0,
            "binance_r5": round(br5, 4),
            "binance_r15": round(br15, 4),
        }

    lead5 = v11.mean(ext5) - br5
    lead15 = v11.mean(ext15) - br15
    agreement = sum(1 for value in ext5 if value > br5 + 0.02) / len(ext5)

    # Penalise disagreement between independent external venues. A single
    # exchange print should not dominate V11 just because it temporarily gaps.
    dispersion5 = max(ext5) - min(ext5) if len(ext5) >= 2 else 0.0
    dispersion15 = max(ext15) - min(ext15) if len(ext15) >= 2 else 0.0
    agreement_quality = v11.clamp(
        1.0 - (dispersion5 / 0.35 + dispersion15 / 0.70) * 0.5,
        0.35,
        1.0,
    )
    multi_venue_bonus = 0.10 if len(ext5) >= 2 else 0.0
    raw = (
        0.55 * v11.clamp((lead5 - 0.01) / 0.18)
        + 0.28 * v11.clamp((lead15 - 0.02) / 0.35)
        + 0.07 * agreement
        + multi_venue_bonus
    )
    score = 100.0 * v11.clamp(raw * agreement_quality, 0.0, 1.0)

    return {
        "symbol": symbol,
        "ready": True,
        "score": round(score, 2),
        "venues": len(ext5),
        "venue_names": names,
        "binance_r5": round(br5, 4),
        "binance_r15": round(br15, 4),
        "external_r5_mean": round(v11.mean(ext5), 4),
        "external_r15_mean": round(v11.mean(ext15), 4),
        "lead5_pct": round(lead5, 4),
        "lead15_pct": round(lead15, 4),
        "bullish_agreement": round(agreement, 3),
        "dispersion5_pct": round(dispersion5, 4),
        "dispersion15_pct": round(dispersion15, 4),
        "agreement_quality": round(agreement_quality, 4),
    }


def _venue_return(symbol, venue, seconds):
    hist = _ensure_hist(symbol, venue)
    if len(hist) < 2:
        return None
    current = hist[-1]
    previous = v11.before(hist, current[0] - seconds)
    if not previous or _safe_float(previous[1]) <= 0:
        return None
    return v11.pct(current[1], previous[1])


async def cross_venue_loop():
    while True:
        await asyncio.sleep(v11.V11_XVENUE_SECONDS)
        try:
            await poll_cross_venues()
            t = time.time()
            venues = ["okx", "kucoin"] + (["bybit"] if BYBIT_ENABLED else [])
            for symbol in list(v11.q.universe):
                for venue in venues:
                    price = _safe_float(v11.venue_prices.get(venue, {}).get(symbol))
                    if price > 0:
                        _ensure_hist(symbol, venue).append((t, price))
                v11.xvenue_state[symbol] = cross_venue_leadlag(symbol)
            v11.engine_stats["xvenue_cycles"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            v11.engine_stats["errors"] += 1
            print(f"Ψ-V11.0.1 XVENUE_ERROR {type(exc).__name__}: {exc}", flush=True)


async def venue_health_loop():
    while True:
        await asyncio.sleep(v11.V11_PRINT_SECONDS)
        ready1 = 0
        ready2 = 0
        for symbol in v11.q.universe:
            row = v11.xvenue_state.get(symbol) or {}
            count = int(_safe_float(row.get("venues")))
            ready1 += count >= 1
            ready2 += count >= 2
        bybit_state = (
            f"ON:{v11.venue_stats.get('bybit_ok', 0)}/{v11.venue_stats.get('bybit_err', 0)}"
            if BYBIT_ENABLED else "OFF_US_GEO"
        )
        print(
            f"Ψ-V11.0.1 VENUES onePlus={ready1}/{len(v11.q.universe)} "
            f"twoPlus={ready2}/{len(v11.q.universe)} "
            f"okx={v11.venue_stats.get('okx_ok', 0)}/{v11.venue_stats.get('okx_err', 0)} "
            f"kucoin={v11.venue_stats.get('kucoin_ok', 0)}/{v11.venue_stats.get('kucoin_err', 0)} "
            f"bybit={bybit_state}",
            flush=True,
        )


# Patch the functions looked up by the existing V11 scoring and promotion
# logic before the base loops start.
v11.poll_cross_venues = poll_cross_venues
v11.cross_venue_leadlag = cross_venue_leadlag
v11.cross_venue_loop = cross_venue_loop


async def main():
    print(
        "[v11.0.1] Cross-venue resilience active: Binance + OKX + KuCoin; "
        "Bybit disabled on US-hosted Railway because the provider geo-blocks "
        "US API origin. Formal V10 PRE/BUY/PUMP gates remain unchanged.",
        flush=True,
    )
    print(
        "Ψ-V11.0.1 SAFETY predictive probabilities remain SHADOW/UNCALIBRATED "
        "until forward outcomes establish calibration.",
        flush=True,
    )
    await asyncio.gather(
        v11.base.main(),
        v11.event_engine_loop(),
        cross_venue_loop(),
        v11.outcome_loop(),
        v11.prediction_loop(),
        v11.v11_print_loop(),
        venue_health_loop(),
    )


v11.scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        v11.persist_outcomes(force=True)
        print("Psi-V11.0.1 stopped", flush=True)
