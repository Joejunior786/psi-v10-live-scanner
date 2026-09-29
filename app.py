# =============================================================================
# Ψ-V10 BINANCE LIVE SCANNER
# =============================================================================
# Read-only market scanner. Does not place trades.
# Railway endpoints: GET /, GET /health, GET /scan
# =============================================================================

import asyncio
import json
import os
import time
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web


# =============================================================================
# CONFIG
# =============================================================================

REST_BASE = os.getenv(
    "BINANCE_REST",
    "https://data-api.binance.vision"
).rstrip("/")

WS_BASE = os.getenv(
    "BINANCE_WS",
    "wss://data-stream.binance.vision"
).rstrip("/")

PORT = int(os.getenv("PORT", "8080"))

TOP_STRUCTURE_UNIVERSE = int(
    os.getenv("TOP_STRUCTURE_UNIVERSE", "120")
)

MICRO_UNIVERSE_SIZE = int(
    os.getenv("MICRO_UNIVERSE_SIZE", "30")
)

RETURN_LIMIT = int(
    os.getenv("RETURN_LIMIT", "10")
)

MIN_QUOTE_VOLUME = float(
    os.getenv("MIN_QUOTE_VOLUME", "500000")
)

STRUCTURE_REFRESH_SECONDS = int(
    os.getenv("STRUCTURE_REFRESH_SECONDS", "300")
)

PRINT_SECONDS = int(
    os.getenv("PRINT_SECONDS", "30")
)

EMA_TOUCH_ATR = float(
    os.getenv("EMA_TOUCH_ATR", "0.75")
)

EMA_NEAR_ATR = float(
    os.getenv("EMA_NEAR_ATR", "1.00")
)

ANTI_CHASE_ATR = float(
    os.getenv("ANTI_CHASE_ATR", "2.50")
)

BREAKOUT_NEAR_PCT = float(
    os.getenv("BREAKOUT_NEAR_PCT", "2.0")
)

BUY_MIN_CONFIRMATIONS = int(
    os.getenv("BUY_MIN_CONFIRMATIONS", "5")
)

BUY_MIN_MICRO_CONFIRMATIONS = int(
    os.getenv("BUY_MIN_MICRO_CONFIRMATIONS", "4")
)

PRE_MIN_CONFIRMATIONS = int(
    os.getenv("PRE_MIN_CONFIRMATIONS", "4")
)

PRE_MIN_MICRO_CONFIRMATIONS = int(
    os.getenv("PRE_MIN_MICRO_CONFIRMATIONS", "3")
)

TOP_BOOK_LEVELS = 10

TRADE_WINDOW_SECONDS = 60

OFI_WINDOW_SECONDS = 60

USER_AGENT = "psi-v10-live-scanner/4.0"


# =============================================================================
# OPTIONAL UK SYMBOL ALLOW-LIST
# =============================================================================
#
# Binance's public market-data API does not itself identify which symbols
# are available to a particular UK account.
#
# For strict UK/account-specific filtering, set a Railway variable such as:
#
# UK_SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT,...
#
# If UK_SYMBOLS is empty, the scanner uses active Binance Spot USDT markets.
# =============================================================================

UK_SYMBOLS_RAW = os.getenv(
    "UK_SYMBOLS",
    ""
).strip()

UK_SYMBOLS = {
    x.strip().upper()
    for x in UK_SYMBOLS_RAW.split(",")
    if x.strip()
}


# =============================================================================
# GLOBAL STATE
# =============================================================================

session: Optional[aiohttp.ClientSession] = None

symbol_meta: Dict[str, dict] = {}

structure: Dict[str, dict] = {}

micro_state: Dict[str, dict] = defaultdict(dict)

selected_micro_symbols: List[str] = []

scanner_started_at = time.time()

last_structure_refresh = 0.0

last_error: Optional[str] = None

scanner_ready = False

rest_connected = False

websocket_connected = False

websocket_symbols: List[str] = []


# =============================================================================
# HELPERS
# =============================================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(
    value,
    default=0.0,
) -> float:

    try:
        return float(value)

    except (TypeError, ValueError):
        return default


def safe_div(
    a: float,
    b: float,
    default=0.0,
) -> float:

    if b == 0:
        return default

    return a / b


def average(
    values: List[float],
) -> float:

    if not values:
        return 0.0

    return sum(values) / len(values)


def prune_deque(
    dq: deque,
    cutoff_ms: int,
) -> None:

    while dq and dq[0][0] < cutoff_ms:
        dq.popleft()


# =============================================================================
# BINANCE REST
# =============================================================================

async def api_get(
    client: aiohttp.ClientSession,
    path: str,
    params: Optional[dict] = None,
):

    global rest_connected
    global last_error

    url = f"{REST_BASE}{path}"

    try:

        async with client.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(
                total=20
            ),
        ) as response:

            text = await response.text()

            if response.status != 200:

                raise RuntimeError(
                    f"Binance REST "
                    f"{response.status}: "
                    f"{text[:500]}"
                )

            data = json.loads(
                text
            )

            rest_connected = True

            return data

    except Exception as exc:

        rest_connected = False

        last_error = (
            f"REST: "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        raise


# =============================================================================
# INDICATORS
# =============================================================================

def ema(
    values: List[float],
    period: int,
) -> Optional[float]:

    if len(values) < period:
        return None

    result = (
        sum(values[:period])
        / period
    )

    multiplier = (
        2.0
        / (period + 1.0)
    )

    for value in values[period:]:

        result = (
            value * multiplier
            + result * (
                1.0 - multiplier
            )
        )

    return result


def true_ranges(
    rows: List[list],
) -> List[float]:

    output = []

    for i in range(
        1,
        len(rows),
    ):

        high = safe_float(
            rows[i][2]
        )

        low = safe_float(
            rows[i][3]
        )

        previous_close = safe_float(
            rows[i - 1][4]
        )

        output.append(
            max(
                high - low,
                abs(
                    high
                    - previous_close
                ),
                abs(
                    low
                    - previous_close
                ),
            )
        )

    return output


def atr(
    rows: List[list],
    period: int = 14,
) -> Optional[float]:

    trs = true_ranges(
        rows
    )

    if len(trs) < period:
        return None

    return average(
        trs[-period:]
    )


# =============================================================================
# BINANCE UNIVERSE
# =============================================================================

async def get_exchange_symbols(
    client: aiohttp.ClientSession,
) -> List[Tuple[str, float]]:

    info, tickers = await asyncio.gather(

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

        row.get("symbol"): row

        for row in tickers

        if isinstance(
            row,
            dict,
        )
    }

    symbol_meta.clear()

    universe = []

    for market in info.get(
        "symbols",
        [],
    ):

        symbol = market.get(
            "symbol",
            "",
        )

        if not symbol:
            continue

        if (
            market.get("status")
            != "TRADING"
        ):
            continue

        if (
            market.get("quoteAsset")
            != "USDT"
        ):
            continue

        if not market.get(
            "isSpotTradingAllowed",
            False,
        ):
            continue

        if (
            UK_SYMBOLS
            and symbol not in UK_SYMBOLS
        ):
            continue

        quote_volume = safe_float(
            ticker_map
            .get(symbol, {})
            .get("quoteVolume")
        )

        if (
            quote_volume
            < MIN_QUOTE_VOLUME
        ):
            continue

        symbol_meta[symbol] = {

            "quote_volume_24h":
                quote_volume,

            "base_asset":
                market.get(
                    "baseAsset"
                ),

            "quote_asset":
                market.get(
                    "quoteAsset"
                ),
        }

        universe.append(
            (
                symbol,
                quote_volume,
            )
        )

    universe.sort(
        key=lambda item: item[1],
        reverse=True,
    )

    return universe


# =============================================================================
# 4H STRUCTURE
# =============================================================================

async def load_4h_structure(
    client: aiohttp.ClientSession,
    symbol: str,
) -> Optional[dict]:

    try:

        rows = await api_get(
            client,
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "4h",
                "limit": 1000,
            },
        )

    except Exception:

        return None

    if (
        not isinstance(
            rows,
            list,
        )
        or len(rows) < 500
    ):
        return None

    # Closed candles drive EMA / ATR /
    # reclaim logic.
    #
    # Current live candle close is used
    # as the current market price.

    closed = rows[:-1]

    if len(closed) < 500:
        return None

    closes = [
        safe_float(row[4])
        for row in closed
    ]

    highs = [
        safe_float(row[2])
        for row in closed
    ]

    lows = [
        safe_float(row[3])
        for row in closed
    ]

    volumes = [
        safe_float(row[5])
        for row in closed
    ]

    current_price = safe_float(
        rows[-1][4]
    )

    ema50 = ema(
        closes,
        50,
    )

    ema200 = ema(
        closes,
        200,
    )

    atr14 = atr(
        closed,
        14,
    )

    if (
        not ema50
        or not ema200
        or not atr14
        or current_price <= 0
    ):
        return None

    last_close = closes[-1]

    previous_close = closes[-2]

    last_low = lows[-1]


    # =========================================================================
    # VOLUME ACCELERATION
    # =========================================================================

    avg20_volume = average(
        volumes[-20:]
    )

    volume_acceleration = safe_div(
        volumes[-1],
        avg20_volume,
    )


    # =========================================================================
    # RANGE COMPRESSION
    # =========================================================================

    recent_ranges = [

        highs[i] - lows[i]

        for i in range(
            max(
                0,
                len(highs) - 6,
            ),
            len(highs),
        )
    ]

    baseline_ranges = [

        highs[i] - lows[i]

        for i in range(
            max(
                0,
                len(highs) - 26,
            ),
            max(
                0,
                len(highs) - 6,
            ),
        )
    ]

    compression_ratio = safe_div(

        average(
            recent_ranges
        ),

        average(
            baseline_ranges
        ),

        1.0,
    )

    compression = (
        compression_ratio
        <= 0.80
    )


    # =========================================================================
    # BREAKOUT STRUCTURE
    # =========================================================================

    resistance_window = (
        highs[-21:-1]
    )

    resistance = (

        max(
            resistance_window
        )

        if resistance_window

        else highs[-1]
    )

    breakout_distance_pct = (

        (
            resistance
            - current_price
        )
        / current_price
        * 100.0
    )

    breakout_near = (
        -0.5
        <= breakout_distance_pct
        <= BREAKOUT_NEAR_PCT
    )

    breakout = (
        current_price
        > resistance
    )


    # =========================================================================
    # EMA PROXIMITY
    # =========================================================================

    distance_ema50_atr = safe_div(
        current_price - ema50,
        atr14,
    )

    distance_ema200_atr = safe_div(
        current_price - ema200,
        atr14,
    )

    ema50_touch = (
        abs(
            distance_ema50_atr
        )
        <= EMA_TOUCH_ATR
    )

    ema200_touch = (
        abs(
            distance_ema200_atr
        )
        <= EMA_TOUCH_ATR
    )

    ema50_near = (
        abs(
            distance_ema50_atr
        )
        <= EMA_NEAR_ATR
    )

    ema200_near = (
        abs(
            distance_ema200_atr
        )
        <= EMA_NEAR_ATR
    )


    # =========================================================================
    # EMA RECLAIM / REJECTION
    # =========================================================================

    ema50_reclaim = (
        previous_close <= ema50
        and last_close > ema50
    )

    ema200_reclaim = (
        previous_close <= ema200
        and last_close > ema200
    )

    ema50_rejection = (
        last_low <= ema50
        and last_close > ema50
    )

    ema200_rejection = (
        last_low <= ema200
        and last_close > ema200
    )

    bullish_ema_stack = (
        current_price > ema50
        and ema50 > ema200
    )

    structural_support = any(
        (
            ema50_touch,
            ema200_touch,
            ema50_reclaim,
            ema200_reclaim,
            ema50_rejection,
            ema200_rejection,
        )
    )


    # =========================================================================
    # ANTI-CHASE
    # =========================================================================

    anti_chase = (
        distance_ema50_atr
        > ANTI_CHASE_ATR
        and
        distance_ema200_atr
        > ANTI_CHASE_ATR
    )


    # =========================================================================
    # STRUCTURAL CONFIRMATIONS
    # =========================================================================

    confirmations = []

    tests = [

        (
            ema50_touch,
            "4H_EMA50_TOUCH",
        ),

        (
            ema200_touch,
            "4H_EMA200_TOUCH",
        ),

        (
            ema50_reclaim,
            "4H_EMA50_RECLAIM",
        ),

        (
            ema200_reclaim,
            "4H_EMA200_RECLAIM",
        ),

        (
            ema50_rejection,
            "4H_EMA50_REJECTION",
        ),

        (
            ema200_rejection,
            "4H_EMA200_REJECTION",
        ),

        (
            bullish_ema_stack,
            "BULLISH_EMA_STACK",
        ),

        (
            volume_acceleration
            >= 1.20,
            "VOLUME_ACCELERATION",
        ),

        (
            compression,
            "RANGE_COMPRESSION",
        ),

        (
            breakout_near,
            "NEAR_RESISTANCE",
        ),

        (
            breakout,
            "BREAKOUT",
        ),
    ]

    confirmations.extend(

        name

        for passed, name
        in tests

        if passed
    )

    return {

        "symbol":
            symbol,

        "price":
            current_price,

        "ema50_4h":
            ema50,

        "ema200_4h":
            ema200,

        "atr14_4h":
            atr14,

        "distance_ema50_atr":
            distance_ema50_atr,

        "distance_ema200_atr":
            distance_ema200_atr,

        "ema50_touch":
            ema50_touch,

        "ema200_touch":
            ema200_touch,

        "ema50_near":
            ema50_near,

        "ema200_near":
            ema200_near,

        "ema50_reclaim":
            ema50_reclaim,

        "ema200_reclaim":
            ema200_reclaim,

        "ema50_rejection":
            ema50_rejection,

        "ema200_rejection":
            ema200_rejection,

        "bullish_ema_stack":
            bullish_ema_stack,

        "structural_support":
            structural_support,

        "volume_acceleration":
            volume_acceleration,

        "compression_ratio":
            compression_ratio,

        "compression":
            compression,

        "resistance":
            resistance,

        "breakout_distance_pct":
            breakout_distance_pct,

        "breakout_near":
            breakout_near,

        "breakout":
            breakout,

        "anti_chase":
            anti_chase,

        "structure_confirmations":
            confirmations,

        "quote_volume_24h":
            symbol_meta
            .get(
                symbol,
                {},
            )
            .get(
                "quote_volume_24h",
                0.0,
            ),

        "updated_ms":
            now_ms(),
    }


# =============================================================================
# MICROSTRUCTURE STATE
# =============================================================================

def ensure_micro_state(
    symbol: str,
) -> dict:

    state = micro_state[
        symbol
    ]

    if "trades" not in state:

        # (timestamp, signed quote value, quote value)

        state["trades"] = deque(
            maxlen=20000
        )

        state["ofi"] = deque(
            maxlen=5000
        )

        state["obi"] = deque(
            maxlen=5000
        )

        state["ask_depletion"] = deque(
            maxlen=5000
        )

        state["bid_depletion"] = deque(
            maxlen=5000
        )

        state["previous_bids"] = None

        state["previous_asks"] = None

        state["book_updates"] = 0

        state["last_trade_ms"] = 0

        state["last_book_ms"] = 0

    return state


# =============================================================================
# ORDER BOOK MATH
# =============================================================================

def depth_notional(
    levels: List[
        Tuple[
            float,
            float,
        ]
    ],
) -> float:

    return sum(

        price * quantity

        for price, quantity
        in levels[
            :TOP_BOOK_LEVELS
        ]
    )


def level_map(
    levels: List[
        Tuple[
            float,
            float,
        ]
    ],
) -> Dict[
    float,
    float,
]:

    return {

        price: quantity

        for price, quantity
        in levels[
            :TOP_BOOK_LEVELS
        ]
    }


def calculate_obi(
    bids,
    asks,
) -> float:

    bid_value = depth_notional(
        bids
    )

    ask_value = depth_notional(
        asks
    )

    return safe_div(

        bid_value
        - ask_value,

        bid_value
        + ask_value,
    )


def calculate_ofi(
    previous_bids,
    previous_asks,
    bids,
    asks,
) -> float:

    pb = level_map(
        previous_bids
    )

    pa = level_map(
        previous_asks
    )

    cb = level_map(
        bids
    )

    ca = level_map(
        asks
    )

    bid_prices = (
        set(pb)
        | set(cb)
    )

    ask_prices = (
        set(pa)
        | set(ca)
    )

    bid_change = sum(

        price
        * (
            cb.get(
                price,
                0.0,
            )
            -
            pb.get(
                price,
                0.0,
            )
        )

        for price
        in bid_prices
    )

    ask_change = sum(

        price
        * (
            ca.get(
                price,
                0.0,
            )
            -
            pa.get(
                price,
                0.0,
            )
        )

        for price
        in ask_prices
    )

    return safe_div(

        bid_change
        - ask_change,

        abs(
            bid_change
        )
        +
        abs(
            ask_change
        ),
    )


def depletion(
    previous,
    current,
) -> float:

    previous_value = depth_notional(
        previous
    )

    current_value = depth_notional(
        current
    )

    return safe_div(

        previous_value
        - current_value,

        previous_value,
    )


# =============================================================================
# AGGREGATE TRADE PROCESSING
# =============================================================================

def process_agg_trade(
    symbol: str,
    data: dict,
) -> None:

    state = ensure_micro_state(
        symbol
    )

    timestamp = int(
        data.get("T")
        or data.get("E")
        or now_ms()
    )

    price = safe_float(
        data.get("p")
    )

    quantity = safe_float(
        data.get("q")
    )

    quote_value = (
        price
        * quantity
    )

    # Binance aggTrade:
    #
    # m=True means buyer is maker.
    # Therefore the aggressive side was SELL.
    #
    # m=False means buyer was taker/aggressor.
    # Therefore aggressive BUY.

    buyer_is_maker = bool(
        data.get(
            "m",
            False,
        )
    )

    signed_quote = (

        -quote_value

        if buyer_is_maker

        else quote_value
    )

    state[
        "trades"
    ].append(
        (
            timestamp,
            signed_quote,
            quote_value,
        )
    )

    state[
        "last_trade_ms"
    ] = timestamp

    prune_deque(

        state["trades"],

        now_ms()
        - 120_000,
    )


# =============================================================================
# PARTIAL ORDER BOOK PROCESSING
# =============================================================================

def parse_levels(
    raw,
) -> List[
    Tuple[
        float,
        float,
    ]
]:

    levels = [

        (
            safe_float(
                row[0]
            ),
            safe_float(
                row[1]
            ),
        )

        for row in raw

        if len(row) >= 2
    ]

    return [

        (
            price,
            quantity,
        )

        for price, quantity
        in levels

        if (
            price > 0
            and quantity > 0
        )

    ][:20]


def process_partial_depth(
    symbol: str,
    data: dict,
) -> None:

    state = ensure_micro_state(
        symbol
    )

    bids = parse_levels(
        data.get(
            "bids",
            [],
        )
    )

    asks = parse_levels(
        data.get(
            "asks",
            [],
        )
    )

    if (
        not bids
        or not asks
    ):
        return

    timestamp = now_ms()

    obi = calculate_obi(
        bids,
        asks,
    )

    state[
        "obi"
    ].append(
        (
            timestamp,
            obi,
        )
    )

    previous_bids = state.get(
        "previous_bids"
    )

    previous_asks = state.get(
        "previous_asks"
    )

    if (
        previous_bids
        and previous_asks
    ):

        ofi = calculate_ofi(
            previous_bids,
            previous_asks,
            bids,
            asks,
        )

        ask_depletion = depletion(
            previous_asks,
            asks,
        )

        bid_depletion = depletion(
            previous_bids,
            bids,
        )

        state[
            "ofi"
        ].append(
            (
                timestamp,
                ofi,
            )
        )

        state[
            "ask_depletion"
        ].append(
            (
                timestamp,
                ask_depletion,
            )
        )

        state[
            "bid_depletion"
        ].append(
            (
                timestamp,
                bid_depletion,
            )
        )

    state[
        "previous_bids"
    ] = bids

    state[
        "previous_asks"
    ] = asks

    state[
        "book_updates"
    ] += 1

    state[
        "last_book_ms"
    ] = timestamp

    cutoff = (
        timestamp
        - OFI_WINDOW_SECONDS
        * 1000
    )

    for key in (
        "ofi",
        "obi",
        "ask_depletion",
        "bid_depletion",
    ):

        prune_deque(
            state[key],
            cutoff,
        )


# =============================================================================
# MICROSTRUCTURE METRICS
# =============================================================================

def micro_metrics(
    symbol: str,
) -> dict:

    state = ensure_micro_state(
        symbol
    )

    current_time = now_ms()

    trades = state[
        "trades"
    ]

    prune_deque(

        trades,

        current_time
        - 120_000,
    )

    recent = [

        row

        for row in trades

        if row[0]
        >= current_time
        - 60_000
    ]

    first_30 = [

        row

        for row in recent

        if row[0]
        < current_time
        - 30_000
    ]

    last_30 = [

        row

        for row in recent

        if row[0]
        >= current_time
        - 30_000
    ]


    # =========================================================================
    # CVD
    # =========================================================================

    cvd_quote_60s = sum(
        row[1]
        for row in recent
    )

    total_quote_60s = sum(
        row[2]
        for row in recent
    )

    aggressive_buy_quote = sum(

        row[2]

        for row in recent

        if row[1] > 0
    )

    aggressive_buy_ratio = safe_div(

        aggressive_buy_quote,

        total_quote_60s,

        0.5,
    )


    # =========================================================================
    # TRADE ACCELERATION
    # =========================================================================

    trade_count_60s = len(
        recent
    )

    first_count = len(
        first_30
    )

    last_count = len(
        last_30
    )

    trade_acceleration = safe_div(

        last_count,

        max(
            first_count,
            1,
        ),

        0.0,
    )

    average_trade_size = safe_div(

        total_quote_60s,

        trade_count_60s,
    )


    # =========================================================================
    # ORDER FLOW
    # =========================================================================

    ofi_values = [

        row[1]

        for row
        in state["ofi"]

        if row[0]
        >= current_time
        - 60_000
    ]

    obi_values = [

        row[1]

        for row
        in state["obi"]

        if row[0]
        >= current_time
        - 60_000
    ]

    ask_depletion_values = [

        row[1]

        for row
        in state[
            "ask_depletion"
        ]

        if row[0]
        >= current_time
        - 60_000
    ]

    bid_depletion_values = [

        row[1]

        for row
        in state[
            "bid_depletion"
        ]

        if row[0]
        >= current_time
        - 60_000
    ]

    ofi = average(
        ofi_values[-20:]
    )

    obi = average(
        obi_values[-20:]
    )

    ask_depletion = average(
        ask_depletion_values[-20:]
    )

    bid_depletion = average(
        bid_depletion_values[-20:]
    )

    recent_ofi = (
        ofi_values[-20:]
    )

    ofi_persistence = safe_div(

        sum(
            1
            for value
            in recent_ofi
            if value > 0
        ),

        len(
            recent_ofi
        ),
    )


    # =========================================================================
    # LIVE DATA VALIDATION
    # =========================================================================
    #
    # BUY and PRE-IGNITION cannot use stale/missing WebSocket data.
    # =========================================================================

    trade_fresh = (
        state[
            "last_trade_ms"
        ]
        >= current_time
        - 15_000
    )

    book_fresh = (
        state[
            "last_book_ms"
        ]
        >= current_time
        - 5_000
    )

    micro_ready = (

        trade_fresh

        and book_fresh

        and len(recent) >= 3

        and len(
            ofi_values
        ) >= 3

        and state[
            "book_updates"
        ] >= 4
    )

    return {

        "micro_ready":
            micro_ready,

        "cvd_quote_60s":
            cvd_quote_60s,

        "aggressive_buy_ratio":
            aggressive_buy_ratio,

        "trade_count_60s":
            trade_count_60s,

        "trade_acceleration":
            trade_acceleration,

        "avg_trade_size_quote":
            average_trade_size,

        "ofi":
            ofi,

        "ofi_persistence":
            ofi_persistence,

        "obi":
            obi,

        "ask_depletion":
            ask_depletion,

        "bid_depletion":
            bid_depletion,

        "last_trade_ms":
            state[
                "last_trade_ms"
            ],

        "last_book_ms":
            state[
                "last_book_ms"
            ],
    }


# =============================================================================
# SIGNAL ENGINE
# =============================================================================

def evaluate_symbol(
    symbol: str,
) -> Optional[dict]:

    structure_data = structure.get(
        symbol
    )

    if not structure_data:
        return None

    micro = micro_metrics(
        symbol
    )

    micro_confirmations = []

    micro_tests = [

        (
            micro[
                "cvd_quote_60s"
            ] > 0,
            "POSITIVE_CVD",
        ),

        (
            micro[
                "aggressive_buy_ratio"
            ] >= 0.55,
            "AGGRESSIVE_BUY_DOMINANCE",
        ),

        (
            micro[
                "ofi"
            ] > 0.05,
            "POSITIVE_OFI",
        ),

        (
            micro[
                "ofi_persistence"
            ] >= 0.60,
            "OFI_PERSISTENCE",
        ),

        (
            micro[
                "obi"
            ] >= 0.10,
            "BID_DEPTH_IMBALANCE",
        ),

        (
            micro[
                "ask_depletion"
            ] > 0.03,
            "ASK_LIQUIDITY_DEPLETION",
        ),

        (
            micro[
                "trade_acceleration"
            ] >= 1.20,
            "TRADE_COUNT_ACCELERATION",
        ),
    ]

    micro_confirmations.extend(

        name

        for passed, name
        in micro_tests

        if passed
    )

    confirmations = (

        list(
            structure_data[
                "structure_confirmations"
            ]
        )

        + micro_confirmations
    )

    micro_count = len(
        micro_confirmations
    )

    total_count = len(
        confirmations
    )

    structure_ok = (

        structure_data[
            "structural_support"
        ]

        or structure_data[
            "breakout_near"
        ]

        or structure_data[
            "breakout"
        ]
    )

    volume_ok = (
        structure_data[
            "volume_acceleration"
        ]
        >= 1.05
    )

    core_flow_ok = (

        micro[
            "cvd_quote_60s"
        ] > 0

        and micro[
            "aggressive_buy_ratio"
        ] >= 0.55

        and micro[
            "ofi"
        ] > 0.05

        and micro[
            "ofi_persistence"
        ] >= 0.60
    )


    # =========================================================================
    # BUY GATE
    # =========================================================================

    # STRICT Ψ-V10 BUY MATRIX: every mandatory live layer must align.
    # Confirmation counts are diagnostic/ranking only and can never override a failed gate.
    mandatory_conditions = {
        "LIVE_MICRO_DATA": micro["micro_ready"],
        "EMA_BULLISH_STACK": structure_data["bullish_ema_stack"],
        "EMA_STRUCTURAL_SUPPORT": structure_data["structural_support"],
        "VOLUME_ACCELERATION": structure_data["volume_acceleration"] >= 1.20,
        "RANGE_COMPRESSION": structure_data["compression"],
        "BREAKOUT_POSITIONING": structure_data["breakout_near"] or structure_data["breakout"],
        "POSITIVE_CVD": micro["cvd_quote_60s"] > 0,
        "AGGRESSIVE_BUY_DOMINANCE": micro["aggressive_buy_ratio"] >= 0.55,
        "POSITIVE_OFI": micro["ofi"] > 0.05,
        "OFI_PERSISTENCE": micro["ofi_persistence"] >= 0.60,
        "BID_DEPTH_IMBALANCE": micro["obi"] >= 0.10,
        "ASK_LIQUIDITY_DEPLETION": micro["ask_depletion"] > 0.03,
        "TRADE_COUNT_ACCELERATION": micro["trade_acceleration"] >= 1.20,
        "ANTI_CHASE_CLEAR": not structure_data["anti_chase"],
    }

    mandatory_status = {
        name: ("PASS" if passed else "FAIL")
        for name, passed in mandatory_conditions.items()
    }

    failed_mandatory = [
        name for name, passed in mandatory_conditions.items() if not passed
    ]

    buy_gate = all(mandatory_conditions.values())


    # =========================================================================
    # PRE-IGNITION GATE
    # =========================================================================

    pre_gate = (

        not buy_gate

        and not structure_data[
            "anti_chase"
        ]

        and micro[
            "micro_ready"
        ]

        and structure_ok

        and micro_count
        >= PRE_MIN_MICRO_CONFIRMATIONS

        and total_count
        >= PRE_MIN_CONFIRMATIONS
    )


    # =========================================================================
    # CLASSIFICATION
    # =========================================================================

    if buy_gate:

        signal_state = "BUY NOW"

    elif pre_gate:

        signal_state = (
            "PRE-IGNITION"
        )

    elif (

        structure_data[
            "ema50_near"
        ]

        or structure_data[
            "ema200_near"
        ]

        or structure_data[
            "breakout_near"
        ]
    ):

        signal_state = "WATCH"

    else:

        signal_state = (
            "MONITOR"
        )


    # =========================================================================
    # RANKING SCORE
    # =========================================================================

    score = 0.0

    score += (
        len(
            structure_data[
                "structure_confirmations"
            ]
        )
        * 7.0
    )

    score += (
        micro_count
        * 9.0
    )

    proximity = min(

        abs(
            structure_data[
                "distance_ema50_atr"
            ]
        ),

        abs(
            structure_data[
                "distance_ema200_atr"
            ]
        ),
    )

    score += max(
        0.0,
        12.0
        - proximity
        * 4.0,
    )

    score += (

        min(
            max(
                structure_data[
                    "volume_acceleration"
                ],
                0.0,
            ),
            3.0,
        )

        * 4.0
    )

    score += (

        max(
            0.0,
            micro["ofi"],
        )

        * 12.0
    )

    score += (

        max(
            0.0,
            micro["obi"],
        )

        * 8.0
    )

    score += (

        max(
            0.0,
            micro[
                "ofi_persistence"
            ],
        )

        * 6.0
    )

    if structure_data[
        "anti_chase"
    ]:

        score -= 30.0

    if not micro[
        "micro_ready"
    ]:

        score -= 5.0


    return {

        "symbol":
            symbol,

        "state":
            signal_state,

        "score":
            round(
                score,
                2,
            ),

        "price":
            structure_data[
                "price"
            ],

        "ema50_4h":
            structure_data[
                "ema50_4h"
            ],

        "ema200_4h":
            structure_data[
                "ema200_4h"
            ],

        "atr14_4h":
            structure_data[
                "atr14_4h"
            ],

        "distance_ema50_atr":
            structure_data[
                "distance_ema50_atr"
            ],

        "distance_ema200_atr":
            structure_data[
                "distance_ema200_atr"
            ],

        "volume_acceleration":
            structure_data[
                "volume_acceleration"
            ],

        "compression_ratio":
            structure_data[
                "compression_ratio"
            ],

        "resistance":
            structure_data[
                "resistance"
            ],

        "breakout_distance_pct":
            structure_data[
                "breakout_distance_pct"
            ],

        "anti_chase":
            structure_data[
                "anti_chase"
            ],

        "micro_ready":
            micro[
                "micro_ready"
            ],

        "cvd_quote_60s":
            micro[
                "cvd_quote_60s"
            ],

        "aggressive_buy_ratio":
            micro[
                "aggressive_buy_ratio"
            ],

        "ofi":
            micro[
                "ofi"
            ],

        "ofi_persistence":
            micro[
                "ofi_persistence"
            ],

        "obi":
            micro[
                "obi"
            ],

        "ask_depletion":
            micro[
                "ask_depletion"
            ],

        "bid_depletion":
            micro[
                "bid_depletion"
            ],

        "trade_count_60s":
            micro[
                "trade_count_60s"
            ],

        "trade_acceleration":
            micro[
                "trade_acceleration"
            ],

        "avg_trade_size_quote":
            micro[
                "avg_trade_size_quote"
            ],

        "structure_confirmations":
            structure_data[
                "structure_confirmations"
            ],

        "micro_confirmations":
            micro_confirmations,

        "confirmations":
            confirmations,

        "confirmation_count":
            total_count,

        "micro_confirmation_count":
            micro_count,

        "mandatory_status":
            mandatory_status,

        "mandatory_all_aligned":
            buy_gate,

        "failed_mandatory":
            failed_mandatory,

        "quote_volume_24h":
            structure_data[
                "quote_volume_24h"
            ],

        "updated_ms":
            structure_data[
                "updated_ms"
            ],
    }


# =============================================================================
# RANKING
# =============================================================================

STATE_PRIORITY = {

    "BUY NOW": 4,

    "PRE-IGNITION": 3,

    "WATCH": 2,

    "MONITOR": 1,
}


def ranked_results(
    limit: int = RETURN_LIMIT,
) -> List[dict]:

    rows = []

    for symbol in structure:

        row = evaluate_symbol(
            symbol
        )

        if row:
            rows.append(
                row
            )

    rows.sort(

        key=lambda row: (

            STATE_PRIORITY.get(
                row["state"],
                0,
            ),

            row["score"],

            row[
                "quote_volume_24h"
            ],
        ),

        reverse=True,
    )

    return rows[:limit]


# =============================================================================
# STRUCTURE REFRESH
# =============================================================================

async def structure_batch(
    symbols: List[str],
) -> None:

    if session is None:
        return

    semaphore = asyncio.Semaphore(
        8
    )

    async def worker(
        symbol: str,
    ):

        async with semaphore:

            result = (
                await load_4h_structure(
                    session,
                    symbol,
                )
            )

            if result:

                structure[
                    symbol
                ] = result

    await asyncio.gather(

        *(
            worker(symbol)
            for symbol
            in symbols
        ),

        return_exceptions=True,
    )


async def refresh_structure() -> None:

    global selected_micro_symbols
    global last_structure_refresh
    global scanner_ready

    if session is None:
        return

    print(
        "Ψ-V10: loading Binance universe...",
        flush=True,
    )

    universe = (
        await get_exchange_symbols(
            session
        )
    )

    symbols = [

        symbol

        for symbol, _
        in universe[
            :TOP_STRUCTURE_UNIVERSE
        ]
    ]

    print(
        f"Ψ-V10: analysing "
        f"{len(symbols)} "
        f"liquid Spot USDT markets...",
        flush=True,
    )


    # Remove markets that are no longer
    # inside the active structural universe.

    active = set(
        symbols
    )

    for old_symbol in list(
        structure
    ):

        if old_symbol not in active:

            structure.pop(
                old_symbol,
                None,
            )


    await structure_batch(
        symbols
    )


    # =========================================================================
    # CHOOSE LIVE MICROSTRUCTURE UNIVERSE
    # =========================================================================

    candidates = []

    for symbol in symbols:

        row = structure.get(
            symbol
        )

        if not row:
            continue

        proximity = min(

            abs(
                row[
                    "distance_ema50_atr"
                ]
            ),

            abs(
                row[
                    "distance_ema200_atr"
                ]
            ),
        )

        structural_score = (

            len(
                row[
                    "structure_confirmations"
                ]
            )

            * 10.0
        )

        structural_score += max(

            0.0,

            20.0
            - proximity
            * 5.0,
        )

        structural_score += min(

            row[
                "volume_acceleration"
            ]

            * 5.0,

            15.0,
        )

        if row[
            "compression"
        ]:

            structural_score += 10.0

        if row[
            "breakout_near"
        ]:

            structural_score += 10.0

        if row[
            "anti_chase"
        ]:

            structural_score -= 25.0

        candidates.append(
            (
                structural_score,
                row[
                    "quote_volume_24h"
                ],
                symbol,
            )
        )

    candidates.sort(
        reverse=True
    )

    new_symbols = [

        symbol

        for _, _, symbol
        in candidates[
            :MICRO_UNIVERSE_SIZE
        ]
    ]

    selected_micro_symbols = (
        new_symbols
    )

    for symbol in new_symbols:

        ensure_micro_state(
            symbol
        )

    last_structure_refresh = (
        time.time()
    )

    scanner_ready = True

    print(
        "Ψ-V10 STRUCTURE READY",
        flush=True,
    )

    print(
        "Micro universe:",
        ", ".join(
            new_symbols
        ),
        flush=True,
    )


async def structure_refresh_loop() -> None:

    global last_error

    while True:

        try:

            await asyncio.sleep(
                STRUCTURE_REFRESH_SECONDS
            )

            await refresh_structure()

        except asyncio.CancelledError:

            raise

        except Exception as exc:

            last_error = (
                f"STRUCTURE: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error,
                flush=True,
            )


# =============================================================================
# BINANCE MARKET-DATA WEBSOCKET
# =============================================================================

async def websocket_loop() -> None:

    global websocket_connected
    global websocket_symbols
    global last_error

    while True:

        try:

            symbols = list(
                selected_micro_symbols
            )

            if not symbols:

                await asyncio.sleep(
                    1
                )

                continue


            # =================================================================
            # STREAMS
            # =================================================================
            #
            # Two streams per symbol:
            #
            # 1. aggTrade
            # 2. depth20@100ms
            #
            # With 30 symbols this is 60 streams.
            # =================================================================

            streams = []

            for symbol in symbols:

                lower = (
                    symbol.lower()
                )

                streams.append(
                    f"{lower}@aggTrade"
                )

                streams.append(
                    f"{lower}@depth20@100ms"
                )


            stream_string = (
                "/".join(
                    streams
                )
            )

            url = (
                f"{WS_BASE}/stream"
                f"?streams="
                f"{stream_string}"
            )

            print(
                f"Ψ-V10 WebSocket connecting "
                f"for {len(symbols)} symbols...",
                flush=True,
            )

            assert session is not None

            async with session.ws_connect(

                url,

                # aiohttp automatically responds
                # to WebSocket PING frames.
                heartbeat=None,

                receive_timeout=90,

                max_msg_size=0,

            ) as ws:

                websocket_connected = True

                websocket_symbols = (
                    symbols
                )

                last_error = None

                print(
                    "Ψ-V10 WebSocket connected.",
                    flush=True,
                )


                async for message in ws:


                    # =========================================================
                    # RECONNECT IF MICRO UNIVERSE CHANGES
                    # =========================================================

                    if (
                        set(
                            selected_micro_symbols
                        )
                        !=
                        set(
                            symbols
                        )
                    ):

                        print(
                            "Ψ-V10 micro universe changed; "
                            "reconnecting WebSocket.",
                            flush=True,
                        )

                        break


                    # =========================================================
                    # TEXT MESSAGE
                    # =========================================================

                    if (
                        message.type
                        == aiohttp.WSMsgType.TEXT
                    ):

                        try:

                            payload = (
                                json.loads(
                                    message.data
                                )
                            )

                        except json.JSONDecodeError:

                            continue


                        stream_name = (
                            payload.get(
                                "stream",
                                "",
                            )
                        )

                        data = (
                            payload.get(
                                "data",
                                {},
                            )
                        )

                        if (
                            not stream_name
                            or not isinstance(
                                data,
                                dict,
                            )
                        ):

                            continue


                        symbol = (

                            stream_name
                            .split("@")[0]
                            .upper()
                        )


                        # =====================================================
                        # AGGREGATE TRADES
                        # =====================================================

                        if (
                            "@aggTrade"
                            in stream_name
                        ):

                            process_agg_trade(
                                symbol,
                                data,
                            )


                        # =====================================================
                        # PARTIAL DEPTH 20
                        # =====================================================

                        elif (
                            "@depth20"
                            in stream_name
                        ):

                            process_partial_depth(
                                symbol,
                                data,
                            )


                    # =========================================================
                    # CLOSED / ERROR
                    # =========================================================

                    elif message.type in (

                        aiohttp.WSMsgType.CLOSED,

                        aiohttp.WSMsgType.ERROR,

                    ):

                        break


        except asyncio.CancelledError:

            raise


        except Exception as exc:

            last_error = (
                f"WEBSOCKET: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error,
                flush=True,
            )


        finally:

            websocket_connected = False

            websocket_symbols = []


        await asyncio.sleep(
            5
        )


# =============================================================================
# CONSOLE OUTPUT
# =============================================================================

async def print_loop() -> None:

    while True:

        await asyncio.sleep(
            PRINT_SECONDS
        )

        try:

            results = ranked_results(
                10
            )

            print(
                "\n"
                "==================================================",
                flush=True,
            )

            print(
                "Ψ-V10 LIVE TOP 10",
                flush=True,
            )

            print(
                "==================================================",
                flush=True,
            )

            for index, row in enumerate(
                results,
                start=1,
            ):

                print(

                    f"{index:02d}. "

                    f"{row['symbol']:12s} "

                    f"{row['state']:14s} "

                    f"score="
                    f"{row['score']:6.2f} "

                    f"conf="
                    f"{row['confirmation_count']} "

                    f"micro="
                    f"{row['micro_confirmation_count']} "

                    f"OFI="
                    f"{row['ofi']:+.3f} "

                    f"OBI="
                    f"{row['obi']:+.3f} "

                    f"buy="
                    f"{row['aggressive_buy_ratio']:.2%} "

                    f"ready="
                    f"{row['micro_ready']}",

                    flush=True,
                )

        except Exception as exc:

            print(
                f"[PRINT ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}",
                flush=True,
            )


# =============================================================================
# HTTP HEALTH ENDPOINT
# =============================================================================

async def health(
    request: web.Request,
):

    return web.json_response(
        {

            "ok":
                True,

            "service":
                "psi-v10-live-scanner",

            "version":
                "4.0",

            "scanner_ready":
                scanner_ready,

            "rest_connected":
                rest_connected,

            "websocket_connected":
                websocket_connected,

            "rest_base":
                REST_BASE,

            "ws_base":
                WS_BASE,

            "structure_symbols":
                len(
                    structure
                ),

            "micro_symbols":
                len(
                    selected_micro_symbols
                ),

            "websocket_symbols":
                len(
                    websocket_symbols
                ),

            "strict_uk_allowlist_enabled":
                bool(
                    UK_SYMBOLS
                ),

            "uptime_seconds":
                int(
                    time.time()
                    - scanner_started_at
                ),

            "last_structure_refresh_age_seconds":
                (
                    int(
                        time.time()
                        - last_structure_refresh
                    )

                    if last_structure_refresh

                    else None
                ),

            "last_error":
                last_error,

            "endpoints":
                {
                    "health":
                        "/health",

                    "scan":
                        "/scan",
                },
        }
    )


# =============================================================================
# HTTP SCAN ENDPOINT
# =============================================================================

async def scan_endpoint(
    request: web.Request,
):

    try:

        limit = int(
            request.query.get(
                "limit",
                RETURN_LIMIT,
            )
        )

    except ValueError:

        limit = (
            RETURN_LIMIT
        )

    limit = max(
        1,
        min(
            limit,
            50,
        ),
    )

    results = ranked_results(
        limit
    )

    states = defaultdict(
        int
    )

    for row in results:

        states[
            row["state"]
        ] += 1

    return web.json_response(
        {

            "ok":
                True,

            "scanner":
                "Ψ-V10",

            "version":
                "4.0",

            "source":
                "Binance public Spot market data",

            "rest_base":
                REST_BASE,

            "ws_base":
                WS_BASE,

            "timeframe":
                "4h",

            "buy_policy":
                "STRICT_ALL_MANDATORY_LIVE_CONDITIONS",

            "ema_history_candles":
                999,

            "moving_averages":
                [
                    "EMA50",
                    "EMA200",
                ],

            "microstructure":
                [
                    "aggTrade CVD",
                    "L1-L10 OBI",
                    "L1-L10 OFI",
                    "ask depletion",
                    "bid depletion",
                    "trade acceleration",
                    "OFI persistence",
                ],

            "scanner_ready":
                scanner_ready,

            "websocket_connected":
                websocket_connected,

            "strict_uk_allowlist_enabled":
                bool(
                    UK_SYMBOLS
                ),

            "universe_size":
                len(
                    structure
                ),

            "micro_universe_size":
                len(
                    selected_micro_symbols
                ),

            "state_counts":
                dict(
                    states
                ),

            "returned":
                len(
                    results
                ),

            "results":
                results,

            "generated_ms":
                now_ms(),
        }
    )


# =============================================================================
# HTTP SERVER
# =============================================================================

async def start_http_server():

    application = web.Application()

    application.router.add_get(
        "/",
        health,
    )

    application.router.add_get(
        "/health",
        health,
    )

    application.router.add_get(
        "/scan",
        scan_endpoint,
    )

    runner = web.AppRunner(
        application
    )

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT,
    )

    await site.start()

    print(
        f"Ψ-V10 HTTP listening "
        f"on port {PORT}",
        flush=True,
    )

    return runner


# =============================================================================
# INITIALISATION
# =============================================================================

async def initialise() -> None:

    print(
        "Ψ-V10 INITIALISING...",
        flush=True,
    )

    print(
        f"REST: {REST_BASE}",
        flush=True,
    )

    print(
        f"WS:   {WS_BASE}",
        flush=True,
    )

    await refresh_structure()

    print(
        "Ψ-V10 INITIALISED.",
        flush=True,
    )


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:

    global session
    global last_error

    timeout = (
        aiohttp.ClientTimeout(
            total=30
        )
    )

    connector = (
        aiohttp.TCPConnector(
            limit=100,
            ttl_dns_cache=300,
        )
    )

    session = (
        aiohttp.ClientSession(

            timeout=timeout,

            connector=connector,

            headers={
                "User-Agent":
                    USER_AGENT,
            },
        )
    )


    # =========================================================================
    # START HTTP FIRST
    # =========================================================================
    #
    # Railway can pass its health check
    # while Binance data is warming up.
    # =========================================================================

    runner = (
        await start_http_server()
    )

    tasks = []

    try:

        try:

            await initialise()

        except Exception as exc:

            last_error = (
                f"INITIALISE: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error,
                flush=True,
            )

            print(
                "Ψ-V10 will retry structure "
                "loading in background.",
                flush=True,
            )


        tasks = [

            asyncio.create_task(
                structure_refresh_loop()
            ),

            asyncio.create_task(
                websocket_loop()
            ),

            asyncio.create_task(
                print_loop()
            ),
        ]

        await asyncio.gather(
            *tasks
        )


    finally:

        for task in tasks:

            task.cancel()

        if tasks:

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

        await runner.cleanup()

        if session:

            await session.close()


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "Ψ-V10 stopped.",
            flush=True,
        )
