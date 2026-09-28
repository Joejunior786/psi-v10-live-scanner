import os
import asyncio
import statistics

import httpx
from fastapi import FastAPI, Query
from fastmcp import FastMCP


app = FastAPI(
    title="Psi V10 Live Scanner",
    version="2.0.0",
    description=(
        "Verified Binance Spot pre-breakout scanner "
        "for the Psi-V10 workflow."
    ),
)


# ============================================================
# CONFIGURATION
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

MIN_QUOTE_VOLUME = float(
    os.getenv("MIN_QUOTE_VOLUME", "1000000")
)

SEM = asyncio.Semaphore(CONCURRENCY)


# Stablecoins / pegged assets excluded from breakout ranking
STABLE_BASES = {
    "USDC",
    "FDUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USDE",
    "USD1",
    "RLUSD",
    "PYUSD",
    "EUR",
    "EURC",
    "AEUR",
    "EURI",
    "BUSD",
    "USTC",
    "USDS",
    "XUSD",
    "BFUSD",
}


# ============================================================
# BINANCE API
# ============================================================

async def api_get(
    client: httpx.AsyncClient,
    path: str,
    params=None,
):
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

    raise RuntimeError(
        "No Binance market-data hosts configured"
    )


# ============================================================
# INDICATORS
# ============================================================

def sma(values, period):
    if len(values) < period:
        return None

    return sum(values[-period:]) / period


def atr(rows, period=14):

    if len(rows) < period + 1:
        return None

    true_ranges = []

    for i in range(1, len(rows)):

        high = float(rows[i][2])
        low = float(rows[i][3])
        previous_close = float(rows[i - 1][4])

        true_range = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

        true_ranges.append(true_range)

    return sum(true_ranges[-period:]) / period


def percentage_distance(price, level):

    if not level:
        return None

    return (
        (price - level)
        / level
        * 100.0
    )


def proximity(
    price,
    level,
    atr_value,
):

    distance = abs(
        price - level
    )

    pct = abs(
        percentage_distance(
            price,
            level,
        )
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

    # Ignore currently forming candle
    closed = rows[:-1]

    closes = [
        float(row[4])
        for row in closed
    ]

    volumes = [
        float(row[5])
        for row in closed
    ]

    last = closed[-1]

    atr14 = atr(
        closed,
        14,
    )

    sma50 = sma(
        closes,
        50,
    )

    sma200 = sma(
        closes,
        200,
    )

    average_20_volume = (
        sum(volumes[-20:])
        / 20
    )

    volume_acceleration = (
        volumes[-1]
        / average_20_volume
        if average_20_volume
        else 0
    )

    total_volume = float(
        last[5]
    )

    taker_buy_volume = float(
        last[9]
    )

    taker_buy_ratio = (
        taker_buy_volume
        / total_volume
        if total_volume
        else 0
    )

    ranges = [
        float(row[2])
        - float(row[3])
        for row in closed[-20:]
    ]

    recent_range = (
        statistics.mean(
            ranges[-5:]
        )
        if ranges[-5:]
        else 0
    )

    baseline_range = (
        statistics.mean(
            ranges
        )
        if ranges
        else 0
    )

    compression_ratio = (
        recent_range
        / baseline_range
        if baseline_range
        else 1
    )

    high_20 = max(
        float(row[2])
        for row in closed[-20:]
    )

    breakout_distance = (
        (
            high_20
            - closes[-1]
        )
        / closes[-1]
        * 100
        if closes[-1]
        else 999
    )

    return {

        "closed": closed,

        "closes": closes,

        "atr": atr14,

        "sma50": sma50,

        "sma200": sma200,

        "volume_acceleration":
            volume_acceleration,

        "taker_buy_ratio":
            taker_buy_ratio,

        "trade_count":
            int(last[8]),

        "compression_ratio":
            compression_ratio,

        "breakout_distance_pct":
            breakout_distance,

        "last_close":
            closes[-1],

        "last_low":
            float(last[3]),

        "last_high":
            float(last[2]),
    }


# ============================================================
# SYMBOL ANALYSIS
# ============================================================

async def analyse_symbol(
    client,
    symbol,
    quote_volume,
):

    rows_4h, rows_1h = (
        await asyncio.gather(

            api_get(
                client,
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": "4h",
                    "limit": 220,
                },
            ),

            api_get(
                client,
                "/api/v3/klines",
                {
                    "symbol": symbol,
                    "interval": "1h",
                    "limit": 220,
                },
            ),
        )
    )

    metrics_4h = candle_metrics(
        rows_4h
    )

    metrics_1h = candle_metrics(
        rows_1h
    )

    price = float(
        rows_4h[-1][4]
    )

    if (
        not metrics_4h["sma200"]
        or not metrics_1h["sma200"]
    ):
        return None


    # ========================================================
    # MA PROXIMITY
    # ========================================================

    p4_200 = proximity(
        price,
        metrics_4h["sma200"],
        metrics_4h["atr"],
    )

    p4_50 = proximity(
        price,
        metrics_4h["sma50"],
        metrics_4h["atr"],
    )

    p1_200 = proximity(
        price,
        metrics_1h["sma200"],
        metrics_1h["atr"],
    )

    p1_50 = proximity(
        price,
        metrics_1h["sma50"],
        metrics_1h["atr"],
    )


    # ========================================================
    # 4H MA RECLAIM
    # ========================================================

    reclaim_4h = any(
        [

            (
                p4_200["near"]
                and metrics_4h["last_low"]
                <= metrics_4h["sma200"]
                and metrics_4h["last_close"]
                >= metrics_4h["sma200"]
            ),

            (
                p4_50["near"]
                and metrics_4h["last_low"]
                <= metrics_4h["sma50"]
                and metrics_4h["last_close"]
                >= metrics_4h["sma50"]
            ),
        ]
    )


    # ========================================================
    # V10 CONFIRMATIONS
    # ========================================================

    confirmations = {

        "4h_ma_near":
            (
                p4_200["near"]
                or p4_50["near"]
            ),

        "4h_ma_reclaim":
            reclaim_4h,

        "1h_ma_near":
            (
                p1_200["near"]
                or p1_50["near"]
            ),

        "volume_acceleration":
            (
                metrics_4h[
                    "volume_acceleration"
                ]
                >= 1.20
            ),

        "aggressive_buy_proxy":
            (
                metrics_4h[
                    "taker_buy_ratio"
                ]
                >= 0.55
            ),

        "range_compression":
            (
                metrics_4h[
                    "compression_ratio"
                ]
                <= 0.80
            ),

        "near_20bar_breakout":
            (
                0
                <= metrics_4h[
                    "breakout_distance_pct"
                ]
                <= 2.0
            ),

        "above_4h_sma50":
            (
                price
                >= metrics_4h["sma50"]
            ),

        "above_4h_sma200":
            (
                price
                >= metrics_4h["sma200"]
            ),
    }


    confirmation_count = sum(
        bool(value)
        for value
        in confirmations.values()
    )


    # ========================================================
    # ANTI-CHASE
    # ========================================================

    extension_atr = (

        (
            price
            - metrics_4h["sma50"]
        )
        / metrics_4h["atr"]

        if metrics_4h["atr"]
        else 0
    )

    anti_chase_ok = (
        extension_atr
        <= 2.0
    )


    # ========================================================
    # STATE ENGINE
    # ========================================================

    if not anti_chase_ok:

        state = "EXTENDED"


    elif (

        confirmations[
            "4h_ma_near"
        ]

        and reclaim_4h

        and confirmation_count
        >= 5

        and (

            confirmations[
                "volume_acceleration"
            ]

            or confirmations[
                "aggressive_buy_proxy"
            ]
        )
    ):

        state = "BUY"


    elif (

        confirmations[
            "4h_ma_near"
        ]

        and confirmation_count
        >= 4
    ):

        state = "PRE_IGNITION"


    elif (

        confirmations[
            "1h_ma_near"
        ]

        or confirmations[
            "4h_ma_near"
        ]
    ):

        state = "WATCH"


    else:

        state = "OBSERVE"


    # ========================================================
    # SCORE
    # ========================================================

    score = (

        confirmation_count
        * 10

        + (
            12
            if reclaim_4h
            else 0
        )

        + min(
            metrics_4h[
                "volume_acceleration"
            ],
            2.5,
        )
        * 6

        + max(
            0,
            (
                metrics_4h[
                    "taker_buy_ratio"
                ]
                - 0.50
            ),
        )
        * 40

        - max(
            0,
            (
                metrics_4h[
                    "breakout_distance_pct"
                ]
                - 2
            ),
        )
    )


    return {

        "symbol":
            symbol,

        "price":
            price,

        "quote_volume_24h":
            quote_volume,

        "state":
            state,

        "score":
            round(
                score,
                2,
            ),

        "confirmations":
            confirmation_count,

        "confirmation_map":
            confirmations,


        "4h": {

            "sma50":
                metrics_4h["sma50"],

            "sma200":
                metrics_4h["sma200"],

            "sma50_distance_pct":
                percentage_distance(
                    price,
                    metrics_4h["sma50"],
                ),

            "sma200_distance_pct":
                percentage_distance(
                    price,
                    metrics_4h["sma200"],
                ),

            "sma50_distance_atr":
                p4_50["atr"],

            "sma200_distance_atr":
                p4_200["atr"],

            "atr14":
                metrics_4h["atr"],

            "volume_vs_20avg":
                metrics_4h[
                    "volume_acceleration"
                ],

            "taker_buy_ratio":
                metrics_4h[
                    "taker_buy_ratio"
                ],

            "trade_count":
                metrics_4h[
                    "trade_count"
                ],

            "compression_ratio":
                metrics_4h[
                    "compression_ratio"
                ],

            "breakout_distance_pct":
                metrics_4h[
                    "breakout_distance_pct"
                ],

            "ma_reclaim":
                reclaim_4h,
        },


        "1h": {

            "sma50":
                metrics_1h["sma50"],

            "sma200":
                metrics_1h["sma200"],

            "sma50_distance_pct":
                percentage_distance(
                    price,
                    metrics_1h["sma50"],
                ),

            "sma200_distance_pct":
                percentage_distance(
                    price,
                    metrics_1h["sma200"],
                ),

            "sma50_distance_atr":
                p1_50["atr"],

            "sma200_distance_atr":
                p1_200["atr"],
        },


        "anti_chase_ok":
            anti_chase_ok,


        # IMPORTANT:
        # These are deliberately NOT fabricated.
        "unavailable_in_this_endpoint": [

            "persistent websocket L1-L10 sequence/OFI",

            "true multi-window CVD",

            "futures OI change",

            "funding-rate shift",

            "iceberg/whale persistence",
        ],
    }


# ============================================================
# FULL MARKET SCAN
# ============================================================

async def run_scan(
    limit=10,
):

    async with httpx.AsyncClient(
        headers={
            "User-Agent":
                "psi-v10-live-scanner/2.0"
        }
    ) as client:


        exchange_info, tickers = (
            await asyncio.gather(

                api_get(
                    client,
                    "/api/v3/exchangeInfo",
                ),

                api_get(
                    client,
                    "/api/v3/ticker/24hr",
                ),
            )
        )


        ticker_map = {

            row["symbol"]: row

            for row
            in tickers
        }


        universe = []


        for market in exchange_info[
            "symbols"
        ]:

            symbol = market[
                "symbol"
            ]

            base_asset = market.get(
                "baseAsset",
                "",
            )


            if (
                market.get("status")
                != "TRADING"
            ):
                continue


            if (
                market.get(
                    "quoteAsset"
                )
                != "USDT"
            ):
                continue


            if not market.get(
                "isSpotTradingAllowed",
                False,
            ):
                continue


            if (
                base_asset
                in STABLE_BASES
            ):
                continue


            quote_volume = float(

                ticker_map
                .get(
                    symbol,
                    {},
                )
                .get(
                    "quoteVolume",
                    0,
                )

                or 0
            )


            if (
                quote_volume
                >= MIN_QUOTE_VOLUME
            ):

                universe.append(
                    (
                        symbol,
                        quote_volume,
                    )
                )


        # Highest liquidity first
        universe.sort(
            key=lambda item:
                item[1],
            reverse=True,
        )


        # Control Binance request load
        universe = universe[:160]


        raw_results = (
            await asyncio.gather(

                *(
                    analyse_symbol(
                        client,
                        symbol,
                        quote_volume,
                    )

                    for (
                        symbol,
                        quote_volume,
                    )

                    in universe
                ),

                return_exceptions=True,
            )
        )


    results = [

        result

        for result
        in raw_results

        if isinstance(
            result,
            dict,
        )
    ]


    # ========================================================
    # PRIORITY RANKING
    # ========================================================

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
            "Psi-V10 verified pre-breakout layer v2.0",

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
            min(
                limit,
                len(results),
            ),

        "buy_rule":
            (
                "4H MA proximity/reclaim + "
                "at least 5 verified confirmations + "
                "volume acceleration or aggressive-buy proxy + "
                "anti-chase filter"
            ),

        "telemetry_note":
            (
                "Unavailable microstructure and derivatives "
                "fields are never fabricated."
            ),

        "results":
            results[:limit],
    }


# ============================================================
# FASTAPI ENDPOINTS
# ============================================================

@app.get("/health")
async def health():

    return {

        "ok": True,

        "service":
            "psi-v10-live-scanner",

        "version":
            "2.0.0",

        "mcp":
            "/mcp",

        "market_data_hosts":
            BASES,
    }


@app.get("/scan")
async def scan(

    limit: int = Query(
        10,
        ge=1,
        le=50,
    )
):

    return await run_scan(
        limit
    )


@app.get("/")
async def root():

    return {

        "service":
            "Psi V10 Live Scanner",

        "version":
            "2.0.0",

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

        "ok":
            True,

        "service":
            "psi-v10-live-scanner",

        "version":
            "2.0.0",

        "source":
            "Binance public Spot market-data API",
    }


@mcp.tool()
async def scan_top(
    limit: int = 10,
) -> dict:

    """
    Rank verified Binance Spot pre-breakout
    candidates using the available V10 layers.
    """

    limit = max(
        1,
        min(
            int(limit),
            50,
        ),
    )

    return await run_scan(
        limit
    )


@mcp.tool()
async def symbol_detail(
    symbol: str,
) -> dict:

    symbol = (
        symbol
        .upper()
        .strip()
    )


    if not symbol.endswith(
        "USDT"
    ):

        symbol += "USDT"


    async with httpx.AsyncClient(

        headers={

            "User-Agent":
                "psi-v10-live-scanner/2.0"
        }

    ) as client:


        ticker = await api_get(

            client,

            "/api/v3/ticker/24hr",

            {
                "symbol":
                    symbol
            },
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

            "ok":
                False,

            "symbol":
                symbol,

            "error":
                (
                    "Insufficient data "
                    "or unsupported symbol"
                ),
        }


    return result


# ============================================================
# MCP HTTP TRANSPORT
# ============================================================

mcp_app = mcp.http_app(
    path="/"
)

app.mount(
    "/mcp",
    mcp_app,
)
