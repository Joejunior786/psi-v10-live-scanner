import os
import asyncio
import statistics

import httpx
from fastapi import FastAPI, Query
from fastmcp import FastMCP


app = FastAPI(
    title="Psi V10 Live Scanner",
    version="2.3.0",
    description="Psi-V10 Binance EMA + Spot Microstructure Scanner",
)


# ============================================================
# CONFIG
# ============================================================

BASES = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "BINANCE_BASE_URLS",
        (
            "https://data-api.binance.vision,"
            "https://api1.binance.com,"
            "https://api2.binance.com,"
            "https://api3.binance.com,"
            "https://api4.binance.com,"
            "https://api.binance.com"
        ),
    ).split(",")
    if url.strip()
]

CONCURRENCY = int(os.getenv("CONCURRENCY", "12"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "1000000"))

# Only strongest preliminary candidates get expensive microstructure calls
MICRO_SHORTLIST = int(os.getenv("MICRO_SHORTLIST", "20"))

SEM = asyncio.Semaphore(CONCURRENCY)


STABLE_BASES = {
    "U", "USDC", "FDUSD", "TUSD", "USDP", "DAI",
    "USDE", "USD1", "RLUSD", "PYUSD", "EUR", "EURC",
    "AEUR", "EURI", "BUSD", "USDS", "XUSD", "BFUSD",
}


# ============================================================
# STABLECOIN DETECTOR
# ============================================================

def looks_like_stablecoin(ticker):

    try:
        last_price = float(ticker.get("lastPrice", 0) or 0)
        high_price = float(ticker.get("highPrice", 0) or 0)
        low_price = float(ticker.get("lowPrice", 0) or 0)

        if last_price <= 0 or high_price <= 0 or low_price <= 0:
            return False

        near_one = 0.985 <= last_price <= 1.015

        range_pct = (
            (high_price - low_price)
            / last_price
            * 100
        )

        return near_one and range_pct <= 0.75

    except (TypeError, ValueError, ZeroDivisionError):
        return False


# ============================================================
# BINANCE API
# ============================================================

async def api_get(client, path, params=None):

    last_error = None

    async with SEM:

        for base in BASES:

            try:
                response = await client.get(
                    base + path,
                    params=params,
                    timeout=20,
                )

                response.raise_for_status()
                return response.json()

            except (
                httpx.HTTPStatusError,
                httpx.RequestError,
            ) as exc:
                last_error = exc

    if last_error:
        raise last_error

    raise RuntimeError("No Binance market-data host available")


# ============================================================
# EMA
# ============================================================

def ema(values, period):

    if len(values) < period:
        return None

    current = sum(values[:period]) / period
    multiplier = 2.0 / (period + 1.0)

    for price in values[period:]:
        current = (
            (price - current)
            * multiplier
            + current
        )

    return current


# ============================================================
# ATR
# ============================================================

def atr(rows, period=14):

    if len(rows) < period + 1:
        return None

    values = []

    for i in range(1, len(rows)):

        high = float(rows[i][2])
        low = float(rows[i][3])
        prev_close = float(rows[i - 1][4])

        values.append(
            max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close),
            )
        )

    return sum(values[-period:]) / period


# ============================================================
# HELPERS
# ============================================================

def pct_distance(price, level):

    if not level:
        return None

    return ((price - level) / level) * 100


def proximity(price, level, atr_value):

    if not level:
        return {
            "pct": None,
            "atr": None,
            "near": False,
        }

    distance = abs(price - level)

    pct = abs(
        pct_distance(price, level)
    )

    atr_distance = (
        distance / atr_value
        if atr_value
        else None
    )

    near = (
        pct <= 0.50
        or (
            atr_distance is not None
            and atr_distance <= 1.0
        )
    )

    return {
        "pct": pct,
        "atr": atr_distance,
        "near": near,
    }


# ============================================================
# CANDLE METRICS
# ============================================================

def candle_metrics(rows):

    closed = rows[:-1]

    if len(closed) < 200:
        return None

    closes = [
        float(row[4])
        for row in closed
    ]

    volumes = [
        float(row[5])
        for row in closed
    ]

    last = closed[-1]

    atr14 = atr(closed, 14)

    ema7_value = ema(closes, 7)
    ema50_value = ema(closes, 50)
    ema200_value = ema(closes, 200)

    avg20vol = sum(volumes[-20:]) / 20

    volume_acceleration = (
        volumes[-1] / avg20vol
        if avg20vol
        else 0
    )

    total_volume = float(last[5])
    taker_buy = float(last[9])

    candle_taker_buy_ratio = (
        taker_buy / total_volume
        if total_volume
        else 0
    )

    ranges = [
        float(row[2]) - float(row[3])
        for row in closed[-20:]
    ]

    recent_range = statistics.mean(ranges[-5:])
    baseline_range = statistics.mean(ranges)

    compression = (
        recent_range / baseline_range
        if baseline_range
        else 1
    )

    high20 = max(
        float(row[2])
        for row in closed[-20:]
    )

    breakout_distance = (
        ((high20 - closes[-1]) / closes[-1]) * 100
        if closes[-1]
        else 999
    )

    return {
        "atr": atr14,
        "ema7": ema7_value,
        "ema50": ema50_value,
        "ema200": ema200_value,

        "volume_acceleration": volume_acceleration,
        "candle_taker_buy_ratio": candle_taker_buy_ratio,
        "trade_count": int(last[8]),

        "compression_ratio": compression,
        "breakout_distance_pct": breakout_distance,

        "last_close": closes[-1],
        "last_low": float(last[3]),
        "last_high": float(last[2]),
    }


def reclaimed_ema(metrics, level, prox):

    if not level:
        return False

    return (
        prox["near"]
        and metrics["last_low"] <= level
        and metrics["last_close"] >= level
    )


# ============================================================
# PRELIMINARY EMA ANALYSIS
# ============================================================

async def analyse_preliminary(
    client,
    symbol,
    quote_volume,
):

    rows4, rows1 = await asyncio.gather(

        api_get(
            client,
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "4h",
                "limit": 1000,
            },
        ),

        api_get(
            client,
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "1h",
                "limit": 1000,
            },
        ),
    )

    m4 = candle_metrics(rows4)
    m1 = candle_metrics(rows1)

    if not m4 or not m1:
        return None

    price = float(rows4[-1][4])

    p4_50 = proximity(price, m4["ema50"], m4["atr"])
    p4_200 = proximity(price, m4["ema200"], m4["atr"])

    p1_50 = proximity(price, m1["ema50"], m1["atr"])
    p1_200 = proximity(price, m1["ema200"], m1["atr"])

    reclaim50 = reclaimed_ema(
        m4,
        m4["ema50"],
        p4_50,
    )

    reclaim200 = reclaimed_ema(
        m4,
        m4["ema200"],
        p4_200,
    )

    reclaim = reclaim50 or reclaim200

    confirmations = {

        "4h_ema50_near":
            p4_50["near"],

        "4h_ema200_near":
            p4_200["near"],

        "4h_ema50_reclaim":
            reclaim50,

        "4h_ema200_reclaim":
            reclaim200,

        "1h_ema50_near":
            p1_50["near"],

        "1h_ema200_near":
            p1_200["near"],

        "volume_acceleration":
            m4["volume_acceleration"] >= 1.20,

        "candle_aggressive_buy_proxy":
            m4["candle_taker_buy_ratio"] >= 0.55,

        "range_compression":
            m4["compression_ratio"] <= 0.80,

        "near_20bar_breakout":
            0 <= m4["breakout_distance_pct"] <= 2.0,

        "above_4h_ema50":
            price >= m4["ema50"],

        "above_4h_ema200":
            price >= m4["ema200"],

        "ema7_above_ema50":
            m4["ema7"] >= m4["ema50"],
    }

    count = sum(
        bool(v)
        for v in confirmations.values()
    )

    extension_atr = (
        (price - m4["ema50"])
        / m4["atr"]
        if m4["atr"]
        else 0
    )

    anti_chase = extension_atr <= 2.0

    four_hour_near = (
        p4_50["near"]
        or p4_200["near"]
    )

    one_hour_near = (
        p1_50["near"]
        or p1_200["near"]
    )

    momentum = (
        confirmations["volume_acceleration"]
        or confirmations["candle_aggressive_buy_proxy"]
    )

    structure = (
        confirmations["range_compression"]
        or confirmations["near_20bar_breakout"]
    )

    # IMPORTANT:
    # Preliminary layer cannot issue final BUY anymore.

    if not anti_chase:
        preliminary_state = "EXTENDED"

    elif (
        four_hour_near
        and reclaim
        and count >= 5
        and momentum
        and structure
    ):
        preliminary_state = "PRE_IGNITION"

    elif (
        four_hour_near
        and count >= 4
        and (momentum or structure)
    ):
        preliminary_state = "PRE_IGNITION"

    elif one_hour_near or four_hour_near:
        preliminary_state = "WATCH"

    else:
        preliminary_state = "OBSERVE"

    score = count * 8

    if reclaim50:
        score += 10

    if reclaim200:
        score += 14

    if confirmations["volume_acceleration"]:
        score += min(
            m4["volume_acceleration"],
            2.5,
        ) * 5

    if confirmations["range_compression"]:
        score += 7

    if confirmations["near_20bar_breakout"]:
        score += 8

    if not anti_chase:
        score -= 20

    return {
        "symbol": symbol,
        "price": price,
        "quote_volume_24h": quote_volume,

        "state": preliminary_state,
        "preliminary_score": round(score, 2),

        "confirmations": count,
        "confirmation_map": confirmations,

        "ema_reclaim": reclaim,

        "momentum_confirmation": momentum,
        "structure_confirmation": structure,

        "anti_chase_ok": anti_chase,

        "4h": {
            "ema7": m4["ema7"],
            "ema50": m4["ema50"],
            "ema200": m4["ema200"],

            "ema50_distance_pct":
                pct_distance(price, m4["ema50"]),

            "ema200_distance_pct":
                pct_distance(price, m4["ema200"]),

            "ema50_near":
                p4_50["near"],

            "ema200_near":
                p4_200["near"],

            "ema50_reclaim":
                reclaim50,

            "ema200_reclaim":
                reclaim200,

            "atr14":
                m4["atr"],

            "volume_vs_20avg":
                m4["volume_acceleration"],

            "candle_taker_buy_ratio":
                m4["candle_taker_buy_ratio"],

            "compression_ratio":
                m4["compression_ratio"],

            "breakout_distance_pct":
                m4["breakout_distance_pct"],
        },

        "1h": {
            "ema7": m1["ema7"],
            "ema50": m1["ema50"],
            "ema200": m1["ema200"],

            "ema50_near":
                p1_50["near"],

            "ema200_near":
                p1_200["near"],
        },
    }


# ============================================================
# REAL ORDER BOOK MICROSTRUCTURE
# ============================================================

def analyse_order_book(depth):

    bids = depth.get("bids", [])[:10]
    asks = depth.get("asks", [])[:10]

    if not bids or not asks:
        return None

    bid_notional = sum(
        float(price) * float(qty)
        for price, qty in bids
    )

    ask_notional = sum(
        float(price) * float(qty)
        for price, qty in asks
    )

    total = bid_notional + ask_notional

    obi = (
        bid_notional / total
        if total
        else 0.5
    )

    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])

    midpoint = (
        best_bid + best_ask
    ) / 2

    spread_pct = (
        ((best_ask - best_bid) / midpoint) * 100
        if midpoint
        else 999
    )

    depth_ratio = (
        bid_notional / ask_notional
        if ask_notional
        else 999
    )

    return {
        "last_update_id":
            depth.get("lastUpdateId"),

        "levels":
            10,

        "bid_notional":
            bid_notional,

        "ask_notional":
            ask_notional,

        "obi":
            obi,

        "obi_pct":
            obi * 100,

        "bid_ask_depth_ratio":
            depth_ratio,

        "best_bid":
            best_bid,

        "best_ask":
            best_ask,

        "spread_pct":
            spread_pct,
    }


# ============================================================
# REAL AGGREGATE TRADE FLOW / CVD
# ============================================================

def trade_flow_metrics(trades):

    if not trades:
        return None

    parsed = []

    for trade in trades:

        price = float(trade["p"])
        qty = float(trade["q"])

        notional = price * qty

        # Binance:
        # m = Was the buyer the maker?
        #
        # m=False -> buyer was taker -> aggressive buy
        # m=True  -> buyer was maker -> seller was taker
        #            -> aggressive sell

        aggressive_buy = not trade["m"]

        parsed.append({
            "notional": notional,
            "buy": aggressive_buy,
        })

    if not parsed:
        return None

    buy_notional = sum(
        x["notional"]
        for x in parsed
        if x["buy"]
    )

    sell_notional = sum(
        x["notional"]
        for x in parsed
        if not x["buy"]
    )

    total_notional = (
        buy_notional
        + sell_notional
    )

    cvd = (
        buy_notional
        - sell_notional
    )

    buy_ratio = (
        buy_notional / total_notional
        if total_notional
        else 0.5
    )

    buy_trades = [
        x["notional"]
        for x in parsed
        if x["buy"]
    ]

    sell_trades = [
        x["notional"]
        for x in parsed
        if not x["buy"]
    ]

    avg_buy_size = (
        sum(buy_trades) / len(buy_trades)
        if buy_trades
        else 0
    )

    avg_sell_size = (
        sum(sell_trades) / len(sell_trades)
        if sell_trades
        else 0
    )

    # Split aggregate trades into older/recent halves
    midpoint = max(1, len(parsed) // 2)

    older = parsed[:midpoint]
    recent = parsed[midpoint:]

    def window_cvd(window):

        return sum(
            x["notional"]
            if x["buy"]
            else -x["notional"]
            for x in window
        )

    old_cvd = window_cvd(older)
    recent_cvd = window_cvd(recent)

    cvd_accelerating = (
        recent_cvd > old_cvd
        and recent_cvd > 0
    )

    recent_buy_notional = sum(
        x["notional"]
        for x in recent
        if x["buy"]
    )

    recent_sell_notional = sum(
        x["notional"]
        for x in recent
        if not x["buy"]
    )

    recent_total = (
        recent_buy_notional
        + recent_sell_notional
    )

    recent_buy_ratio = (
        recent_buy_notional / recent_total
        if recent_total
        else 0.5
    )

    return {
        "aggregate_trade_count":
            len(parsed),

        "aggressive_buy_notional":
            buy_notional,

        "aggressive_sell_notional":
            sell_notional,

        "aggressive_buy_ratio":
            buy_ratio,

        "spot_cvd_quote":
            cvd,

        "older_half_cvd_quote":
            old_cvd,

        "recent_half_cvd_quote":
            recent_cvd,

        "cvd_accelerating":
            cvd_accelerating,

        "recent_aggressive_buy_ratio":
            recent_buy_ratio,

        "average_aggressive_buy_size":
            avg_buy_size,

        "average_aggressive_sell_size":
            avg_sell_size,

        "buy_trade_size_dominance":
            avg_buy_size > avg_sell_size,
    }


# ============================================================
# MICROSTRUCTURE ENRICHMENT
# ============================================================

async def enrich_microstructure(client, candidate):

    symbol = candidate["symbol"]

    try:

        depth, trades = await asyncio.gather(

            api_get(
                client,
                "/api/v3/depth",
                {
                    "symbol": symbol,
                    "limit": 20,
                },
            ),

            api_get(
                client,
                "/api/v3/aggTrades",
                {
                    "symbol": symbol,
                    "limit": 500,
                },
            ),
        )

        book = analyse_order_book(depth)
        flow = trade_flow_metrics(trades)

        if not book or not flow:
            candidate["microstructure_verified"] = False
            candidate["state"] = "PRE_IGNITION"
            return candidate

        # ----------------------------------------------------
        # MICRO CONFIRMATIONS
        # ----------------------------------------------------

        micro = {

            # 55%+ of L1-L10 notional on bid side
            "bid_depth_dominant":
                book["obi"] >= 0.55,

            # Stronger institutional-style imbalance
            "obi_60_plus":
                book["obi"] >= 0.60,

            # Spread protection
            "spread_tight":
                book["spread_pct"] <= 0.15,

            # Real aggressive trade flow
            "aggressive_buy_dominant":
                flow["aggressive_buy_ratio"] >= 0.55,

            "recent_aggressive_buy_dominant":
                flow["recent_aggressive_buy_ratio"] >= 0.55,

            # Positive actual spot CVD
            "positive_spot_cvd":
                flow["spot_cvd_quote"] > 0,

            # Recent CVD improving
            "cvd_accelerating":
                flow["cvd_accelerating"],

            # Average aggressive buys larger than sells
            "buy_size_dominance":
                flow["buy_trade_size_dominance"],
        }

        micro_count = sum(
            bool(v)
            for v in micro.values()
        )

        # ----------------------------------------------------
        # CORE ORDER-FLOW REQUIREMENT
        # ----------------------------------------------------

        real_flow_confirmation = (
            micro["spread_tight"]
            and micro["aggressive_buy_dominant"]
            and micro["positive_spot_cvd"]
            and (
                micro["bid_depth_dominant"]
                or micro["cvd_accelerating"]
            )
        )

        # ----------------------------------------------------
        # FINAL BUY
        # ----------------------------------------------------

        final_buy = (
            candidate["state"] == "PRE_IGNITION"
            and candidate["ema_reclaim"]
            and candidate["anti_chase_ok"]
            and candidate["momentum_confirmation"]
            and candidate["structure_confirmation"]
            and real_flow_confirmation
            and micro_count >= 5
        )

        if final_buy:
            candidate["state"] = "BUY"

        elif candidate["state"] == "PRE_IGNITION":
            candidate["state"] = "PRE_IGNITION"

        # ----------------------------------------------------
        # FINAL SCORE
        # ----------------------------------------------------

        micro_score = 0

        if micro["bid_depth_dominant"]:
            micro_score += 8

        if micro["obi_60_plus"]:
            micro_score += 5

        if micro["spread_tight"]:
            micro_score += 5

        if micro["aggressive_buy_dominant"]:
            micro_score += 10

        if micro["recent_aggressive_buy_dominant"]:
            micro_score += 8

        if micro["positive_spot_cvd"]:
            micro_score += 10

        if micro["cvd_accelerating"]:
            micro_score += 12

        if micro["buy_size_dominance"]:
            micro_score += 7

        candidate["microstructure_verified"] = True

        candidate["micro_confirmations"] = micro_count

        candidate["micro_confirmation_map"] = micro

        candidate["real_flow_confirmation"] = (
            real_flow_confirmation
        )

        candidate["order_book"] = book
        candidate["trade_flow"] = flow

        candidate["score"] = round(
            candidate["preliminary_score"]
            + micro_score,
            2,
        )

        return candidate

    except Exception as exc:

        candidate["microstructure_verified"] = False

        candidate["microstructure_error"] = (
            type(exc).__name__
        )

        candidate["score"] = (
            candidate["preliminary_score"]
        )

        # Never promote without verified telemetry
        if candidate["state"] == "PRE_IGNITION":
            candidate["state"] = "PRE_IGNITION"

        return candidate


# ============================================================
# FULL SCAN
# ============================================================

async def run_scan(limit=10):

    async with httpx.AsyncClient(
        headers={
            "User-Agent":
                "psi-v10-live-scanner/2.3"
        }
    ) as client:

        exchange_info, tickers = await asyncio.gather(

            api_get(
                client,
                "/api/v3/exchangeInfo",
            ),

            api_get(
                client,
                "/api/v3/ticker/24hr",
            ),
        )

        ticker_map = {
            row["symbol"]: row
            for row in tickers
        }

        universe = []

        excluded_known = 0
        excluded_dynamic = 0

        for market in exchange_info["symbols"]:

            symbol = market["symbol"]
            base = market.get("baseAsset", "")

            if market.get("status") != "TRADING":
                continue

            if market.get("quoteAsset") != "USDT":
                continue

            if not market.get(
                "isSpotTradingAllowed",
                False,
            ):
                continue

            if base in STABLE_BASES:
                excluded_known += 1
                continue

            ticker = ticker_map.get(symbol, {})

            if looks_like_stablecoin(ticker):
                excluded_dynamic += 1
                continue

            quote_volume = float(
                ticker.get("quoteVolume", 0)
                or 0
            )

            if quote_volume >= MIN_QUOTE_VOLUME:
                universe.append(
                    (symbol, quote_volume)
                )

        universe.sort(
            key=lambda x: x[1],
            reverse=True,
        )

        universe = universe[:160]

        preliminary_raw = await asyncio.gather(

            *(
                analyse_preliminary(
                    client,
                    symbol,
                    quote_volume,
                )
                for symbol, quote_volume
                in universe
            ),

            return_exceptions=True,
        )

        preliminary = [
            x
            for x in preliminary_raw
            if isinstance(x, dict)
        ]

        # ----------------------------------------------------
        # RANK PRELIMINARY CANDIDATES
        # ----------------------------------------------------

        pre_priority = {
            "PRE_IGNITION": 0,
            "WATCH": 1,
            "OBSERVE": 2,
            "EXTENDED": 3,
        }

        preliminary.sort(
            key=lambda x: (
                pre_priority.get(
                    x["state"],
                    9,
                ),
                -x["preliminary_score"],
            )
        )

        shortlist = preliminary[
            :min(
                MICRO_SHORTLIST,
                len(preliminary),
            )
        ]

        # ----------------------------------------------------
        # REAL MICROSTRUCTURE STAGE
        # ----------------------------------------------------

        enriched = await asyncio.gather(

            *(
                enrich_microstructure(
                    client,
                    candidate,
                )
                for candidate in shortlist
            ),

            return_exceptions=True,
        )

        enriched = [
            x
            for x in enriched
            if isinstance(x, dict)
        ]

    # --------------------------------------------------------
    # FINAL RANKING
    # --------------------------------------------------------

    final_priority = {
        "BUY": 0,
        "PRE_IGNITION": 1,
        "WATCH": 2,
        "OBSERVE": 3,
        "EXTENDED": 4,
    }

    enriched.sort(
        key=lambda x: (
            final_priority.get(
                x["state"],
                9,
            ),
            -x.get(
                "score",
                x["preliminary_score"],
            ),
        )
    )

    return {

        "source":
            "Binance public Spot market-data API",

        "engine":
            "Psi-V10 EMA + Spot Microstructure v2.3",

        "version":
            "2.3.0",

        "ema_configuration": {
            "periods": [7, 50, 200],
            "source": "close",
            "timeframes": ["1h", "4h"],
        },

        "microstructure": {
            "enabled": True,

            "shortlist_size":
                MICRO_SHORTLIST,

            "order_book":
                "L1-L10 Binance Spot depth snapshot",

            "aggregate_trades":
                500,

            "obi":
                "real current depth imbalance",

            "cvd":
                "real aggregate-trade-derived Spot CVD",

            "true_ofi":
                False,

            "true_ofi_note":
                (
                    "Requires sequential order-book "
                    "updates/WebSocket state."
                ),
        },

        "stablecoin_filter": {
            "known": True,
            "dynamic": True,
            "excluded_known":
                excluded_known,
            "excluded_dynamic":
                excluded_dynamic,
        },

        "markets_preliminary_scanned":
            len(preliminary),

        "markets_microstructure_checked":
            len(enriched),

        "returned":
            min(limit, len(enriched)),

        "buy_rule":
            (
                "4H EMA50/EMA200 reclaim + "
                "EMA/volume/structure qualification + "
                "verified real Spot order-flow confirmation + "
                "at least 5 microstructure confirmations + "
                "anti-chase."
            ),

        "telemetry_note":
            (
                "OBI and Spot CVD are derived from real "
                "Binance public market data. "
                "True sequential OFI, futures OI/funding "
                "and iceberg persistence are not fabricated."
            ),

        "results":
            enriched[:limit],
    }


# ============================================================
# FASTAPI
# ============================================================

@app.get("/health")
async def health():

    return {
        "ok": True,
        "service":
            "psi-v10-live-scanner",

        "version":
            "2.3.0",

        "moving_average_type":
            "EMA",

        "ema_periods":
            [7, 50, 200],

        "spot_microstructure":
            True,

        "obi":
            True,

        "spot_cvd":
            True,

        "true_sequential_ofi":
            False,

        "mcp":
            "/mcp",
    }


@app.get("/scan")
async def scan(
    limit: int = Query(
        10,
        ge=1,
        le=20,
    )
):
    return await run_scan(limit)


@app.get("/")
async def root():

    return {
        "service":
            "Psi V10 Live Scanner",

        "version":
            "2.3.0",

        "engine":
            "EMA + Spot Microstructure",

        "scan":
            "/scan",

        "health":
            "/health",

        "mcp":
            "/mcp",
    }


# ============================================================
# MCP
# ============================================================

mcp = FastMCP(
    "Psi V10 Live Scanner"
)


@mcp.tool()
async def scanner_health() -> dict:

    return await health()


@mcp.tool()
async def scan_top(
    limit: int = 10,
) -> dict:

    return await run_scan(
        max(
            1,
            min(
                int(limit),
                20,
            ),
        )
    )


@mcp.tool()
async def symbol_detail(
    symbol: str,
) -> dict:

    symbol = symbol.upper().strip()

    if not symbol.endswith("USDT"):
        symbol += "USDT"

    async with httpx.AsyncClient(
        headers={
            "User-Agent":
                "psi-v10-live-scanner/2.3"
        }
    ) as client:

        ticker = await api_get(
            client,
            "/api/v3/ticker/24hr",
            {
                "symbol": symbol,
            },
        )

        quote_volume = float(
            ticker.get(
                "quoteVolume",
                0,
            )
            or 0
        )

        preliminary = await analyse_preliminary(
            client,
            symbol,
            quote_volume,
        )

        if not preliminary:

            return {
                "ok": False,
                "symbol": symbol,
                "error":
                    "Insufficient data or unsupported symbol",
            }

        return await enrich_microstructure(
            client,
            preliminary,
        )


# ============================================================
# MCP HTTP
# ============================================================

mcp_app = mcp.http_app(
    path="/"
)

app.mount(
    "/mcp",
    mcp_app,
)
