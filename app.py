import os
import asyncio
import statistics

import httpx
from fastapi import FastAPI, Query
from fastmcp import FastMCP


app = FastAPI(
    title="Psi V10 Live Scanner",
    version="2.1.0",
    description="Verified Binance Spot Psi-V10 EMA pre-breakout scanner",
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
SEM = asyncio.Semaphore(CONCURRENCY)

STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USDE",
    "USD1", "RLUSD", "PYUSD", "EUR", "EURC", "AEUR",
    "EURI", "BUSD", "USDS", "XUSD", "BFUSD"
}


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

            except (httpx.HTTPStatusError, httpx.RequestError) as exc:
                last_error = exc

    if last_error:
        raise last_error

    raise RuntimeError("No Binance market-data hosts configured")


# ============================================================
# EMA
# ============================================================

def ema(values, period):
    """
    Standard EMA:
    1. Seed with SMA of first 'period' closes.
    2. Apply multiplier 2 / (period + 1).
    """

    if len(values) < period:
        return None

    seed = sum(values[:period]) / period
    multiplier = 2.0 / (period + 1.0)

    current_ema = seed

    for price in values[period:]:
        current_ema = (
            (price - current_ema) * multiplier
            + current_ema
        )

    return current_ema


def atr(rows, period=14):
    if len(rows) < period + 1:
        return None

    values = []

    for i in range(1, len(rows)):
        high = float(rows[i][2])
        low = float(rows[i][3])
        previous_close = float(rows[i - 1][4])

        values.append(
            max(
                high - low,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        )

    return sum(values[-period:]) / period


def percentage_distance(price, level):
    if not level:
        return None

    return ((price - level) / level) * 100.0


def proximity(price, level, atr_value):
    distance = abs(price - level)
    pct = abs(percentage_distance(price, level))

    atr_distance = (
        distance / atr_value
        if atr_value
        else None
    )

    # V10 dynamic proximity:
    # within 0.5% OR within 1 ATR
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

    # Closed candles only for confirmation logic
    closed = rows[:-1]

    closes = [float(row[4]) for row in closed]
    volumes = [float(row[5]) for row in closed]

    last = closed[-1]

    atr14 = atr(closed, 14)

    ema7 = ema(closes, 7)
    ema50 = ema(closes, 50)
    ema200 = ema(closes, 200)

    average_20_volume = sum(volumes[-20:]) / 20

    volume_acceleration = (
        volumes[-1] / average_20_volume
        if average_20_volume
        else 0
    )

    total_volume = float(last[5])
    taker_buy_volume = float(last[9])

    taker_buy_ratio = (
        taker_buy_volume / total_volume
        if total_volume
        else 0
    )

    ranges = [
        float(row[2]) - float(row[3])
        for row in closed[-20:]
    ]

    recent_range = (
        statistics.mean(ranges[-5:])
        if ranges[-5:]
        else 0
    )

    baseline_range = (
        statistics.mean(ranges)
        if ranges
        else 0
    )

    compression_ratio = (
        recent_range / baseline_range
        if baseline_range
        else 1
    )

    high_20 = max(
        float(row[2])
        for row in closed[-20:]
    )

    breakout_distance = (
        ((high_20 - closes[-1]) / closes[-1]) * 100
        if closes[-1]
        else 999
    )

    return {
        "closed": closed,
        "closes": closes,
        "atr": atr14,

        "ema7": ema7,
        "ema50": ema50,
        "ema200": ema200,

        "volume_acceleration": volume_acceleration,
        "taker_buy_ratio": taker_buy_ratio,
        "trade_count": int(last[8]),
        "compression_ratio": compression_ratio,
        "breakout_distance_pct": breakout_distance,

        "last_close": closes[-1],
        "last_low": float(last[3]),
        "last_high": float(last[2]),
    }


# ============================================================
# EMA RECLAIM
# ============================================================

def reclaimed_ema(metrics, ema_level, proximity_data):

    if not ema_level:
        return False

    return (
        proximity_data["near"]
        and metrics["last_low"] <= ema_level
        and metrics["last_close"] >= ema_level
    )


# ============================================================
# SYMBOL ANALYSIS
# ============================================================

async def analyse_symbol(client, symbol, quote_volume):

    rows_4h, rows_1h = await asyncio.gather(

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

    m4 = candle_metrics(rows_4h)
    m1 = candle_metrics(rows_1h)

    # Current Binance market price from current candle
    price = float(rows_4h[-1][4])

    if (
        not m4["ema50"]
        or not m4["ema200"]
        or not m1["ema50"]
        or not m1["ema200"]
    ):
        return None


    # ========================================================
    # EMA PROXIMITY
    # ========================================================

    p4_50 = proximity(
        price,
        m4["ema50"],
        m4["atr"],
    )

    p4_200 = proximity(
        price,
        m4["ema200"],
        m4["atr"],
    )

    p1_50 = proximity(
        price,
        m1["ema50"],
        m1["atr"],
    )

    p1_200 = proximity(
        price,
        m1["ema200"],
        m1["atr"],
    )


    # ========================================================
    # EXACT EMA RECLAIM IDENTIFICATION
    # ========================================================

    reclaim_4h_50 = reclaimed_ema(
        m4,
        m4["ema50"],
        p4_50,
    )

    reclaim_4h_200 = reclaimed_ema(
        m4,
        m4["ema200"],
        p4_200,
    )

    reclaim_4h = (
        reclaim_4h_50
        or reclaim_4h_200
    )


    # ========================================================
    # V10 CONFIRMATIONS
    # ========================================================

    confirmations = {

        "4h_ema50_near":
            p4_50["near"],

        "4h_ema200_near":
            p4_200["near"],

        "4h_ema50_reclaim":
            reclaim_4h_50,

        "4h_ema200_reclaim":
            reclaim_4h_200,

        "1h_ema50_near":
            p1_50["near"],

        "1h_ema200_near":
            p1_200["near"],

        "volume_acceleration":
            m4["volume_acceleration"] >= 1.20,

        "aggressive_buy_proxy":
            m4["taker_buy_ratio"] >= 0.55,

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

    confirmation_count = sum(
        bool(value)
        for value in confirmations.values()
    )


    # ========================================================
    # ANTI-CHASE
    # ========================================================

    extension_atr = (
        (price - m4["ema50"]) / m4["atr"]
        if m4["atr"]
        else 0
    )

    anti_chase_ok = extension_atr <= 2.0


    # ========================================================
    # STRICT PRE-BREAKOUT REQUIREMENTS
    # ========================================================

    four_hour_ema_near = (
        p4_50["near"]
        or p4_200["near"]
    )

    one_hour_ema_near = (
        p1_50["near"]
        or p1_200["near"]
    )

    momentum_confirmation = (
        confirmations["volume_acceleration"]
        or confirmations["aggressive_buy_proxy"]
    )

    structure_confirmation = (
        confirmations["range_compression"]
        or confirmations["near_20bar_breakout"]
    )


    # ========================================================
    # STATE ENGINE
    # ========================================================

    if not anti_chase_ok:

        state = "EXTENDED"

    elif (
        four_hour_ema_near
        and reclaim_4h
        and confirmation_count >= 5
        and momentum_confirmation
        and structure_confirmation
    ):

        state = "BUY"

    elif (
        four_hour_ema_near
        and confirmation_count >= 4
        and (
            momentum_confirmation
            or structure_confirmation
        )
    ):

        state = "PRE_IGNITION"

    elif (
        one_hour_ema_near
        or four_hour_ema_near
    ):

        state = "WATCH"

    else:

        state = "OBSERVE"


    # ========================================================
    # SCORE
    # ========================================================

    score = confirmation_count * 8

    if reclaim_4h_50:
        score += 10

    if reclaim_4h_200:
        score += 14

    if confirmations["volume_acceleration"]:
        score += min(
            m4["volume_acceleration"],
            2.5,
        ) * 5

    if confirmations["aggressive_buy_proxy"]:
        score += (
            m4["taker_buy_ratio"] - 0.50
        ) * 40

    if confirmations["range_compression"]:
        score += 7

    if confirmations["near_20bar_breakout"]:
        score += 8

    if not anti_chase_ok:
        score -= 20


    # ========================================================
    # IDENTIFY ACTIVE EMA
    # ========================================================

    active_4h_ema = []

    if p4_50["near"]:
        active_4h_ema.append("EMA50")

    if p4_200["near"]:
        active_4h_ema.append("EMA200")

    active_1h_ema = []

    if p1_50["near"]:
        active_1h_ema.append("EMA50")

    if p1_200["near"]:
        active_1h_ema.append("EMA200")


    return {

        "symbol": symbol,
        "price": price,
        "quote_volume_24h": quote_volume,

        "state": state,
        "score": round(score, 2),

        "confirmations": confirmation_count,
        "confirmation_map": confirmations,

        "active_4h_ema":
            active_4h_ema,

        "active_1h_ema":
            active_1h_ema,

        "4h": {

            "ema7":
                m4["ema7"],

            "ema50":
                m4["ema50"],

            "ema200":
                m4["ema200"],

            "ema50_distance_pct":
                percentage_distance(
                    price,
                    m4["ema50"],
                ),

            "ema200_distance_pct":
                percentage_distance(
                    price,
                    m4["ema200"],
                ),

            "ema50_distance_atr":
                p4_50["atr"],

            "ema200_distance_atr":
                p4_200["atr"],

            "ema50_near":
                p4_50["near"],

            "ema200_near":
                p4_200["near"],

            "ema50_reclaim":
                reclaim_4h_50,

            "ema200_reclaim":
                reclaim_4h_200,

            "atr14":
                m4["atr"],

            "volume_vs_20avg":
                m4["volume_acceleration"],

            "taker_buy_ratio":
                m4["taker_buy_ratio"],

            "trade_count":
                m4["trade_count"],

            "compression_ratio":
                m4["compression_ratio"],

            "breakout_distance_pct":
                m4["breakout_distance_pct"],
        },

        "1h": {

            "ema7":
                m1["ema7"],

            "ema50":
                m1["ema50"],

            "ema200":
                m1["ema200"],

            "ema50_distance_pct":
                percentage_distance(
                    price,
                    m1["ema50"],
                ),

            "ema200_distance_pct":
                percentage_distance(
                    price,
                    m1["ema200"],
                ),

            "ema50_distance_atr":
                p1_50["atr"],

            "ema200_distance_atr":
                p1_200["atr"],

            "ema50_near":
                p1_50["near"],

            "ema200_near":
                p1_200["near"],
        },

        "anti_chase_ok":
            anti_chase_ok,

        "unavailable_in_this_endpoint": [
            "persistent websocket L1-L10 sequence/OFI",
            "true multi-window CVD",
            "futures OI change",
            "funding-rate shift",
            "iceberg/whale persistence",
        ],
    }


# ============================================================
# MARKET SCAN
# ============================================================

async def run_scan(limit=10):

    async with httpx.AsyncClient(
        headers={
            "User-Agent":
                "psi-v10-live-scanner/2.1"
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

        for market in exchange_info["symbols"]:

            symbol = market["symbol"]
            base_asset = market.get("baseAsset", "")

            if market.get("status") != "TRADING":
                continue

            if market.get("quoteAsset") != "USDT":
                continue

            if not market.get(
                "isSpotTradingAllowed",
                False,
            ):
                continue

            if base_asset in STABLE_BASES:
                continue

            quote_volume = float(
                ticker_map
                .get(symbol, {})
                .get("quoteVolume", 0)
                or 0
            )

            if quote_volume >= MIN_QUOTE_VOLUME:
                universe.append(
                    (symbol, quote_volume)
                )


        # Highest-liquidity markets first
        universe.sort(
            key=lambda item: item[1],
            reverse=True,
        )

        universe = universe[:160]


        raw_results = await asyncio.gather(

            *(
                analyse_symbol(
                    client,
                    symbol,
                    quote_volume,
                )

                for symbol, quote_volume
                in universe
            ),

            return_exceptions=True,
        )


    results = [
        result
        for result in raw_results
        if isinstance(result, dict)
    ]


    priority = {
        "BUY": 0,
        "PRE_IGNITION": 1,
        "WATCH": 2,
        "OBSERVE": 3,
        "EXTENDED": 4,
    }


    results.sort(
        key=lambda row: (
            priority.get(
                row["state"],
                9,
            ),
            -row["score"],
        )
    )


    return {

        "source":
            "Binance public Spot market-data API",

        "engine":
            "Psi-V10 Binance EMA pre-breakout v2.1",

        "ema_configuration": {
            "fast": 7,
            "medium": 50,
            "long": 200,
            "source": "close",
            "timeframes": [
                "1h",
                "4h",
            ],
        },

        "universe":
            (
                "Liquid Binance USDT spot markets; "
                "stable/pegged bases excluded"
            ),

        "minimum_24h_quote_volume_usdt":
            MIN_QUOTE_VOLUME,

        "scanned":
            len(results),

        "returned":
            min(limit, len(results)),

        "signal_rules": {

            "WATCH":
                (
                    "Price near 1H EMA50/EMA200 "
                    "or 4H EMA50/EMA200."
                ),

            "PRE_IGNITION":
                (
                    "4H EMA proximity plus >=4 "
                    "confirmations and momentum/structure."
                ),

            "BUY":
                (
                    "4H EMA50/EMA200 proximity + reclaim + "
                    ">=5 confirmations + momentum + "
                    "pre-breakout structure + anti-chase."
                ),
        },

        "telemetry_note":
            (
                "Unavailable microstructure and derivatives "
                "fields are never fabricated."
            ),

        "results":
            results[:limit],
    }


# ============================================================
# FASTAPI
# ============================================================

@app.get("/health")
async def health():

    return {
        "ok": True,
        "service": "psi-v10-live-scanner",
        "version": "2.1.0",
        "moving_average_type": "EMA",
        "ema_periods": [7, 50, 200],
        "mcp": "/mcp",
        "market_data_hosts": BASES,
    }


@app.get("/scan")
async def scan(
    limit: int = Query(
        10,
        ge=1,
        le=50,
    )
):
    return await run_scan(limit)


@app.get("/")
async def root():

    return {
        "service":
            "Psi V10 Live Scanner",

        "version":
            "2.1.0",

        "moving_average_type":
            "EMA",

        "ema_periods":
            [7, 50, 200],

        "health":
            "/health",

        "scan":
            "/scan",

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

    return {
        "ok": True,
        "version": "2.1.0",
        "moving_average_type": "EMA",
        "ema_periods": [7, 50, 200],
        "source":
            "Binance public Spot market-data API",
    }


@mcp.tool()
async def scan_top(
    limit: int = 10,
) -> dict:

    limit = max(
        1,
        min(int(limit), 50),
    )

    return await run_scan(limit)


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
                "psi-v10-live-scanner/2.1"
        }
    ) as client:

        ticker = await api_get(
            client,
            "/api/v3/ticker/24hr",
            {"symbol": symbol},
        )

        quote_volume = float(
            ticker.get(
                "quoteVolume",
                0,
            )
            or 0
        )

        result = await analyse_symbol(
            client,
            symbol,
            quote_volume,
        )


    if result is None:

        return {
            "ok": False,
            "symbol": symbol,
            "error":
                "Insufficient data or unsupported symbol",
        }


    return result


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
