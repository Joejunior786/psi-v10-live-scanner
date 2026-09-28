# =============================================================================
# Ψ-V10 BINANCE LIVE SCANNER
# =============================================================================
#
# READ-ONLY MARKET SCANNER
#
# Core layers:
#   - Binance Spot USDT universe
#   - 4H EMA50 / EMA200
#   - ATR14
#   - EMA proximity / reclaim
#   - Volume acceleration
#   - Range compression
#   - Breakout proximity
#   - Anti-chase filter
#   - Live aggTrade CVD
#   - L1-L10 order-book imbalance
#   - Order-flow imbalance (OFI)
#   - Bid / ask liquidity depletion
#   - Persistence
#   - WATCH / PRE-IGNITION / BUY classification
#
# Railway:
#   GET /
#   GET /health
#   GET /scan
#
# This program DOES NOT place trades.
# =============================================================================

import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque
from statistics import mean
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web


# =============================================================================
# CONFIGURATION
# =============================================================================

# Public Binance market-data-only REST endpoint.
REST_BASE = os.getenv(
    "BINANCE_REST",
    "https://data-api.binance.vision",
).rstrip("/")

# Keep WebSocket independently configurable.
WS_BASE = os.getenv(
    "BINANCE_WS",
    "wss://stream.binance.com:9443",
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

MICRO_REFRESH_SECONDS = int(
    os.getenv("MICRO_REFRESH_SECONDS", "20")
)

DEPTH_LIMIT = 100

TOP_BOOK_LEVELS = 10

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

# Optional explicit allow-list.
# Example Railway variable:
#
# UK_SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT
#
# If empty, scanner uses Binance active Spot USDT markets.
UK_SYMBOLS_RAW = os.getenv("UK_SYMBOLS", "").strip()

UK_SYMBOLS = {
    x.strip().upper()
    for x in UK_SYMBOLS_RAW.split(",")
    if x.strip()
}

USER_AGENT = "psi-v10-live-scanner/2.0"


# =============================================================================
# GLOBAL STATE
# =============================================================================

session: Optional[aiohttp.ClientSession] = None

symbol_meta: Dict[str, dict] = {}

structure: Dict[str, dict] = {}

micro_state: Dict[str, dict] = defaultdict(dict)

books: Dict[str, dict] = {}

book_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

depth_buffers: Dict[str, deque] = defaultdict(
    lambda: deque(maxlen=5000)
)

depth_queues: Dict[str, asyncio.Queue] = {}

depth_workers: Dict[str, asyncio.Task] = {}

selected_micro_symbols: List[str] = []

scanner_started_at = time.time()

last_structure_refresh = 0.0

last_micro_refresh = 0.0

last_error: Optional[str] = None

scanner_ready = False

websocket_connected = False

rest_connected = False


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_div(a: float, b: float, default=0.0) -> float:
    if b == 0:
        return default
    return a / b


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def pct_distance(price: float, reference: float) -> Optional[float]:
    if not reference:
        return None

    return ((price - reference) / reference) * 100.0


def abs_pct_distance(price: float, reference: float) -> Optional[float]:
    d = pct_distance(price, reference)

    if d is None:
        return None

    return abs(d)


def average(values: List[float]) -> float:
    if not values:
        return 0.0

    return sum(values) / len(values)


# =============================================================================
# REST
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
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:

            text = await response.text()

            if response.status != 200:
                raise RuntimeError(
                    f"Binance REST {response.status}: {text[:500]}"
                )

            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                raise RuntimeError(
                    f"Invalid Binance JSON: {text[:300]}"
                )

            rest_connected = True

            return data

    except Exception as exc:
        rest_connected = False
        last_error = f"REST: {type(exc).__name__}: {exc}"
        raise


# =============================================================================
# INDICATORS
# =============================================================================

def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None

    seed = sum(values[:period]) / period

    multiplier = 2.0 / (period + 1.0)

    result = seed

    for value in values[period:]:
        result = (
            value * multiplier
            + result * (1.0 - multiplier)
        )

    return result


def true_ranges(rows: List[list]) -> List[float]:
    if len(rows) < 2:
        return []

    result = []

    for i in range(1, len(rows)):
        high = safe_float(rows[i][2])
        low = safe_float(rows[i][3])
        previous_close = safe_float(rows[i - 1][4])

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )

        result.append(tr)

    return result


def atr(rows: List[list], period: int = 14) -> Optional[float]:
    trs = true_ranges(rows)

    if len(trs) < period:
        return None

    return sum(trs[-period:]) / period


# =============================================================================
# EXCHANGE UNIVERSE
# =============================================================================

async def get_exchange_symbols(
    client: aiohttp.ClientSession,
) -> List[Tuple[str, float]]:

    info, tickers = await asyncio.gather(
        api_get(client, "/api/v3/exchangeInfo"),
        api_get(client, "/api/v3/ticker/24hr"),
    )

    ticker_map = {
        row.get("symbol"): row
        for row in tickers
        if isinstance(row, dict)
    }

    universe = []

    symbol_meta.clear()

    for market in info.get("symbols", []):

        symbol = market.get("symbol", "")

        if not symbol:
            continue

        if market.get("status") != "TRADING":
            continue

        if market.get("quoteAsset") != "USDT":
            continue

        if not market.get(
            "isSpotTradingAllowed",
            False,
        ):
            continue

        if UK_SYMBOLS and symbol not in UK_SYMBOLS:
            continue

        ticker = ticker_map.get(symbol, {})

        quote_volume = safe_float(
            ticker.get("quoteVolume")
        )

        if quote_volume < MIN_QUOTE_VOLUME:
            continue

        symbol_meta[symbol] = {
            "quote_volume_24h": quote_volume,
            "base_asset": market.get("baseAsset"),
            "quote_asset": market.get("quoteAsset"),
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
                "limit": 250,
            },
        )

    except Exception:
        return None

    if len(rows) < 210:
        return None

    # Binance last candle can still be open.
    closed = rows[:-1]

    if len(closed) < 205:
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

    if not ema50 or not ema200 or not atr14:
        return None

    last_close = closes[-1]

    previous_close = closes[-2]

    last_low = lows[-1]

    previous_low = lows[-2]

    last_high = highs[-1]

    avg20_volume = average(
        volumes[-20:]
    )

    recent_volume = volumes[-1]

    volume_acceleration = safe_div(
        recent_volume,
        avg20_volume,
    )

    # -------------------------------------------------------------------------
    # Compression
    # -------------------------------------------------------------------------

    recent_ranges = [
        highs[i] - lows[i]
        for i in range(
            max(0, len(highs) - 6),
            len(highs),
        )
    ]

    baseline_ranges = [
        highs[i] - lows[i]
        for i in range(
            max(0, len(highs) - 26),
            max(0, len(highs) - 6),
        )
    ]

    recent_range_avg = average(
        recent_ranges
    )

    baseline_range_avg = average(
        baseline_ranges
    )

    compression_ratio = safe_div(
        recent_range_avg,
        baseline_range_avg,
        1.0,
    )

    compression = (
        compression_ratio <= 0.80
    )

    # -------------------------------------------------------------------------
    # Breakout level
    # -------------------------------------------------------------------------

    resistance_window = highs[-21:-1]

    resistance = (
        max(resistance_window)
        if resistance_window
        else last_high
    )

    breakout_distance_pct = (
        ((resistance - current_price) / current_price) * 100.0
        if current_price > 0
        else 999.0
    )

    breakout_near = (
        -0.5
        <= breakout_distance_pct
        <= BREAKOUT_NEAR_PCT
    )

    breakout = (
        current_price > resistance
    )

    # -------------------------------------------------------------------------
    # EMA proximity
    # -------------------------------------------------------------------------

    distance_ema50_atr = safe_div(
        current_price - ema50,
        atr14,
    )

    distance_ema200_atr = safe_div(
        current_price - ema200,
        atr14,
    )

    abs_ema50_atr = abs(
        distance_ema50_atr
    )

    abs_ema200_atr = abs(
        distance_ema200_atr
    )

    ema50_touch = (
        abs_ema50_atr <= EMA_TOUCH_ATR
    )

    ema200_touch = (
        abs_ema200_atr <= EMA_TOUCH_ATR
    )

    ema50_near = (
        abs_ema50_atr <= EMA_NEAR_ATR
    )

    ema200_near = (
        abs_ema200_atr <= EMA_NEAR_ATR
    )

    # -------------------------------------------------------------------------
    # EMA reclaim
    # -------------------------------------------------------------------------

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

    structural_support = (
        ema50_touch
        or ema200_touch
        or ema50_reclaim
        or ema200_reclaim
        or ema50_rejection
        or ema200_rejection
    )

    # -------------------------------------------------------------------------
    # Anti-chase
    # -------------------------------------------------------------------------

    extension_ema50_atr = safe_div(
        current_price - ema50,
        atr14,
    )

    extension_ema200_atr = safe_div(
        current_price - ema200,
        atr14,
    )

    anti_chase = (
        extension_ema50_atr > ANTI_CHASE_ATR
        and extension_ema200_atr > ANTI_CHASE_ATR
    )

    # -------------------------------------------------------------------------
    # Structure confirmations
    # -------------------------------------------------------------------------

    confirmations = []

    if ema50_touch:
        confirmations.append(
            "4H_EMA50_TOUCH"
        )

    if ema200_touch:
        confirmations.append(
            "4H_EMA200_TOUCH"
        )

    if ema50_reclaim:
        confirmations.append(
            "4H_EMA50_RECLAIM"
        )

    if ema200_reclaim:
        confirmations.append(
            "4H_EMA200_RECLAIM"
        )

    if ema50_rejection:
        confirmations.append(
            "4H_EMA50_REJECTION"
        )

    if ema200_rejection:
        confirmations.append(
            "4H_EMA200_REJECTION"
        )

    if bullish_ema_stack:
        confirmations.append(
            "BULLISH_EMA_STACK"
        )

    if volume_acceleration >= 1.20:
        confirmations.append(
            "VOLUME_ACCELERATION"
        )

    if compression:
        confirmations.append(
            "RANGE_COMPRESSION"
        )

    if breakout_near:
        confirmations.append(
            "NEAR_RESISTANCE"
        )

    if breakout:
        confirmations.append(
            "BREAKOUT"
        )

    return {
        "symbol": symbol,

        "price": current_price,

        "ema50_4h": ema50,
        "ema200_4h": ema200,

        "atr14_4h": atr14,

        "distance_ema50_atr": distance_ema50_atr,
        "distance_ema200_atr": distance_ema200_atr,

        "ema50_touch": ema50_touch,
        "ema200_touch": ema200_touch,

        "ema50_near": ema50_near,
        "ema200_near": ema200_near,

        "ema50_reclaim": ema50_reclaim,
        "ema200_reclaim": ema200_reclaim,

        "ema50_rejection": ema50_rejection,
        "ema200_rejection": ema200_rejection,

        "bullish_ema_stack": bullish_ema_stack,

        "structural_support": structural_support,

        "volume_acceleration": volume_acceleration,

        "compression_ratio": compression_ratio,
        "compression": compression,

        "resistance": resistance,

        "breakout_distance_pct": breakout_distance_pct,
        "breakout_near": breakout_near,
        "breakout": breakout,

        "anti_chase": anti_chase,

        "structure_confirmations": confirmations,

        "quote_volume_24h": symbol_meta.get(
            symbol,
            {},
        ).get(
            "quote_volume_24h",
            0.0,
        ),

        "updated_ms": now_ms(),
    }


# =============================================================================
# ORDER BOOK HELPERS
# =============================================================================

def sorted_top10(
    side: Dict[float, float],
    reverse: bool,
) -> List[Tuple[float, float]]:

    levels = [
        (price, qty)
        for price, qty in side.items()
        if qty > 0
    ]

    levels.sort(
        key=lambda x: x[0],
        reverse=reverse,
    )

    return levels[:TOP_BOOK_LEVELS]


def depth_notional(
    levels: List[Tuple[float, float]],
) -> float:

    return sum(
        price * qty
        for price, qty in levels
    )


def calculate_obi(
    bids: List[Tuple[float, float]],
    asks: List[Tuple[float, float]],
) -> float:

    bid_value = depth_notional(
        bids
    )

    ask_value = depth_notional(
        asks
    )

    total = (
        bid_value
        + ask_value
    )

    if total <= 0:
        return 0.0

    return (
        bid_value - ask_value
    ) / total


def level_map(
    levels: List[Tuple[float, float]],
) -> Dict[float, float]:

    return {
        price: qty
        for price, qty in levels
    }


def calculate_ofi(
    previous_bids: List[Tuple[float, float]],
    previous_asks: List[Tuple[float, float]],
    current_bids: List[Tuple[float, float]],
    current_asks: List[Tuple[float, float]],
) -> float:

    pb = level_map(
        previous_bids
    )

    pa = level_map(
        previous_asks
    )

    cb = level_map(
        current_bids
    )

    ca = level_map(
        current_asks
    )

    bid_prices = set(pb) | set(cb)

    ask_prices = set(pa) | set(ca)

    bid_change = sum(
        price * (
            cb.get(price, 0.0)
            - pb.get(price, 0.0)
        )
        for price in bid_prices
    )

    ask_change = sum(
        price * (
            ca.get(price, 0.0)
            - pa.get(price, 0.0)
        )
        for price in ask_prices
    )

    denominator = (
        abs(bid_change)
        + abs(ask_change)
    )

    if denominator <= 0:
        return 0.0

    return (
        bid_change - ask_change
    ) / denominator


def depletion(
    previous: List[Tuple[float, float]],
    current: List[Tuple[float, float]],
) -> float:

    previous_value = depth_notional(
        previous
    )

    current_value = depth_notional(
        current
    )

    if previous_value <= 0:
        return 0.0

    return clamp(
        (
            previous_value
            - current_value
        ) / previous_value,
        -1.0,
        1.0,
    )


# =============================================================================
# MICROSTRUCTURE STATE
# =============================================================================

def ensure_micro_state(symbol: str):

    state = micro_state[symbol]

    state.setdefault(
        "buy_quote",
        deque(maxlen=2000),
    )

    state.setdefault(
        "sell_quote",
        deque(maxlen=2000),
    )

    state.setdefault(
        "trade_times",
        deque(maxlen=5000),
    )

    state.setdefault(
        "trade_sizes",
        deque(maxlen=5000),
    )

    state.setdefault(
        "ofi_history",
        deque(maxlen=100),
    )

    state.setdefault(
        "obi_history",
        deque(maxlen=100),
    )

    state.setdefault(
        "ask_depletion_history",
        deque(maxlen=100),
    )

    state.setdefault(
        "bid_depletion_history",
        deque(maxlen=100),
    )

    state.setdefault(
        "book_samples",
        deque(maxlen=100),
    )

    state.setdefault(
        "last_trade_price",
        0.0,
    )

    state.setdefault(
        "last_trade_ms",
        0,
    )


def trim_trade_window(
    dq: deque,
    cutoff_ms: int,
):

    while dq and dq[0][0] < cutoff_ms:
        dq.popleft()


def record_book_sample(
    symbol: str,
):

    ensure_micro_state(
        symbol
    )

    book = books.get(
        symbol
    )

    if not book:
        return

    bids = sorted_top10(
        book["bids"],
        True,
    )

    asks = sorted_top10(
        book["asks"],
        False,
    )

    if not bids or not asks:
        return

    state = micro_state[symbol]

    previous = (
        state["book_samples"][-1]
        if state["book_samples"]
        else None
    )

    obi = calculate_obi(
        bids,
        asks,
    )

    state["obi_history"].append(
        (
            now_ms(),
            obi,
        )
    )

    if previous:

        previous_bids = previous["bids"]
        previous_asks = previous["asks"]

        ofi = calculate_ofi(
            previous_bids,
            previous_asks,
            bids,
            asks,
        )

        ask_dep = depletion(
            previous_asks,
            asks,
        )

        bid_dep = depletion(
            previous_bids,
            bids,
        )

        state["ofi_history"].append(
            (
                now_ms(),
                ofi,
            )
        )

        state[
            "ask_depletion_history"
        ].append(
            (
                now_ms(),
                ask_dep,
            )
        )

        state[
            "bid_depletion_history"
        ].append(
            (
                now_ms(),
                bid_dep,
            )
        )

    state["book_samples"].append(
        {
            "time": now_ms(),
            "bids": bids,
            "asks": asks,
        }
    )


# =============================================================================
# AGGTRADE
# =============================================================================

def process_agg_trade(
    symbol: str,
    data: dict,
):

    ensure_micro_state(
        symbol
    )

    state = micro_state[symbol]

    price = safe_float(
        data.get("p")
    )

    qty = safe_float(
        data.get("q")
    )

    event_time = int(
        data.get(
            "T",
            data.get(
                "E",
                now_ms(),
            ),
        )
    )

    quote_value = (
        price * qty
    )

    buyer_is_maker = bool(
        data.get("m")
    )

    # m == False:
    # aggressive buyer crossed the ask.
    if buyer_is_maker:
        state[
            "sell_quote"
        ].append(
            (
                event_time,
                quote_value,
            )
        )
    else:
        state[
            "buy_quote"
        ].append(
            (
                event_time,
                quote_value,
            )
        )

    state[
        "trade_times"
    ].append(
        event_time
    )

    state[
        "trade_sizes"
    ].append(
        (
            event_time,
            quote_value,
        )
    )

    state[
        "last_trade_price"
    ] = price

    state[
        "last_trade_ms"
    ] = event_time


# =============================================================================
# DEPTH SNAPSHOT
# =============================================================================

async def fetch_depth_snapshot(
    symbol: str,
) -> Optional[dict]:

    if session is None:
        return None

    try:
        snapshot = await api_get(
            session,
            "/api/v3/depth",
            {
                "symbol": symbol,
                "limit": DEPTH_LIMIT,
            },
        )

        bids = {
            safe_float(price): safe_float(qty)
            for price, qty in snapshot.get(
                "bids",
                [],
            )
            if safe_float(qty) > 0
        }

        asks = {
            safe_float(price): safe_float(qty)
            for price, qty in snapshot.get(
                "asks",
                [],
            )
            if safe_float(qty) > 0
        }

        return {
            "last_update_id": int(
                snapshot[
                    "lastUpdateId"
                ]
            ),
            "bids": bids,
            "asks": asks,
        }

    except Exception as exc:

        print(
            f"[DEPTH SNAPSHOT ERROR] "
            f"{symbol}: {exc}"
        )

        return None


# =============================================================================
# DEPTH SYNCHRONISATION
# =============================================================================

async def resync_book(
    symbol: str,
):

    async with book_locks[
        symbol
    ]:

        snapshot = await fetch_depth_snapshot(
            symbol
        )

        if not snapshot:
            books.pop(
                symbol,
                None,
            )
            return

        last_id = snapshot[
            "last_update_id"
        ]

        books[symbol] = {
            "bids": snapshot[
                "bids"
            ],
            "asks": snapshot[
                "asks"
            ],
            "last_update_id": last_id,
            "ready": False,
        }

        buffered = list(
            depth_buffers[symbol]
        )

        # Drop events already covered by snapshot.
        buffered = [
            event
            for event in buffered
            if int(
                event.get(
                    "u",
                    0,
                )
            ) > last_id
        ]

        first_index = None

        for i, event in enumerate(
            buffered
        ):

            U = int(
                event.get(
                    "U",
                    0,
                )
            )

            u = int(
                event.get(
                    "u",
                    0,
                )
            )

            if (
                U
                <= last_id + 1
                <= u
            ):
                first_index = i
                break

        if first_index is None:

            # We may simply be waiting for the first event
            # after the REST snapshot.
            return

        relevant = buffered[
            first_index:
        ]

        book = books[
            symbol
        ]

        current_id = last_id

        for event in relevant:

            U = int(
                event.get(
                    "U",
                    0,
                )
            )

            u = int(
                event.get(
                    "u",
                    0,
                )
            )

            if u <= current_id:
                continue

            if not (
                U
                <= current_id + 1
                <= u
            ):
                books.pop(
                    symbol,
                    None,
                )
                return

            apply_depth_update(
                book,
                event,
            )

            current_id = u

            book[
                "last_update_id"
            ] = u

        book[
            "ready"
        ] = True

        record_book_sample(
            symbol
        )


def apply_depth_update(
    book: dict,
    data: dict,
):

    for price_raw, qty_raw in data.get(
        "b",
        [],
    ):

        price = safe_float(
            price_raw
        )

        qty = safe_float(
            qty_raw
        )

        if qty == 0:
            book[
                "bids"
            ].pop(
                price,
                None,
            )
        else:
            book[
                "bids"
            ][price] = qty

    for price_raw, qty_raw in data.get(
        "a",
        [],
    ):

        price = safe_float(
            price_raw
        )

        qty = safe_float(
            qty_raw
        )

        if qty == 0:
            book[
                "asks"
            ].pop(
                price,
                None,
            )
        else:
            book[
                "asks"
            ][price] = qty


async def process_depth(
    symbol: str,
    data: dict,
):

    depth_buffers[
        symbol
    ].append(
        data
    )

    book = books.get(
        symbol
    )

    if not book:
        await resync_book(
            symbol
        )
        return

    if not book.get(
        "ready"
    ):
        await resync_book(
            symbol
        )
        return

    async with book_locks[
        symbol
    ]:

        book = books.get(
            symbol
        )

        if not book:
            return

        last_id = int(
            book.get(
                "last_update_id",
                0,
            )
        )

        U = int(
            data.get(
                "U",
                0,
            )
        )

        u = int(
            data.get(
                "u",
                0,
            )
        )

        if u <= last_id:
            return

        if not (
            U
            <= last_id + 1
            <= u
        ):
            books.pop(
                symbol,
                None,
            )

            asyncio.create_task(
                resync_book(
                    symbol
                )
            )

            return

        apply_depth_update(
            book,
            data,
        )

        book[
            "last_update_id"
        ] = u

        record_book_sample(
            symbol
        )


# =============================================================================
# DEPTH QUEUE
# =============================================================================

async def depth_worker(
    symbol: str,
):

    queue = depth_queues[
        symbol
    ]

    while True:

        data = await queue.get()

        try:
            await process_depth(
                symbol,
                data,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(
                f"[DEPTH WORKER ERROR] "
                f"{symbol}: {exc}"
            )

        finally:
            queue.task_done()


def ensure_depth_worker(
    symbol: str,
):

    if symbol in depth_workers:
        return

    depth_queues[
        symbol
    ] = asyncio.Queue(
        maxsize=5000
    )

    depth_workers[
        symbol
    ] = asyncio.create_task(
        depth_worker(
            symbol
        )
    )


# =============================================================================
# MICRO METRICS
# =============================================================================

def window_sum(
    dq: deque,
    cutoff_ms: int,
) -> float:

    return sum(
        value
        for timestamp, value in dq
        if timestamp >= cutoff_ms
    )


def window_values(
    dq: deque,
    cutoff_ms: int,
) -> List[float]:

    return [
        value
        for timestamp, value in dq
        if timestamp >= cutoff_ms
    ]


def micro_metrics(
    symbol: str,
) -> dict:

    ensure_micro_state(
        symbol
    )

    state = micro_state[
        symbol
    ]

    now = now_ms()

    cutoff_60 = (
        now - 60_000
    )

    cutoff_30 = (
        now - 30_000
    )

    cutoff_15 = (
        now - 15_000
    )

    buy_60 = window_sum(
        state[
            "buy_quote"
        ],
        cutoff_60,
    )

    sell_60 = window_sum(
        state[
            "sell_quote"
        ],
        cutoff_60,
    )

    buy_30 = window_sum(
        state[
            "buy_quote"
        ],
        cutoff_30,
    )

    sell_30 = window_sum(
        state[
            "sell_quote"
        ],
        cutoff_30,
    )

    total_60 = (
        buy_60
        + sell_60
    )

    total_30 = (
        buy_30
        + sell_30
    )

    cvd_60 = (
        buy_60
        - sell_60
    )

    cvd_30 = (
        buy_30
        - sell_30
    )

    aggressive_buy_ratio = safe_div(
        buy_60,
        total_60,
        0.5,
    )

    recent_trade_times = [
        t
        for t in state[
            "trade_times"
        ]
        if t >= cutoff_60
    ]

    recent_trade_times_30 = [
        t
        for t in state[
            "trade_times"
        ]
        if t >= cutoff_30
    ]

    trade_count_60 = len(
        recent_trade_times
    )

    trade_count_30 = len(
        recent_trade_times_30
    )

    trade_acceleration = safe_div(
        trade_count_30 * 2.0,
        max(
            trade_count_60,
            1,
        ),
    )

    trade_sizes_60 = window_values(
        state[
            "trade_sizes"
        ],
        cutoff_60,
    )

    average_trade_size = average(
        trade_sizes_60
    )

    ofi_values = window_values(
        state[
            "ofi_history"
        ],
        cutoff_60,
    )

    ofi_recent = window_values(
        state[
            "ofi_history"
        ],
        cutoff_15,
    )

    obi_values = window_values(
        state[
            "obi_history"
        ],
        cutoff_60,
    )

    ask_depletion_values = window_values(
        state[
            "ask_depletion_history"
        ],
        cutoff_60,
    )

    bid_depletion_values = window_values(
        state[
            "bid_depletion_history"
        ],
        cutoff_60,
    )

    ofi = (
        average(
            ofi_recent
        )
        if ofi_recent
        else average(
            ofi_values
        )
    )

    obi = average(
        obi_values
    )

    ask_depletion = average(
        ask_depletion_values
    )

    bid_depletion = average(
        bid_depletion_values
    )

    positive_ofi_samples = sum(
        1
        for value in ofi_values
        if value > 0
    )

    ofi_persistence = safe_div(
        positive_ofi_samples,
        len(
            ofi_values
        ),
    )

    book = books.get(
        symbol,
        {},
    )

    micro_ready = bool(
        book.get(
            "ready"
        )
        and len(
            ofi_values
        ) >= 3
        and total_60 > 0
    )

    return {
        "micro_ready": micro_ready,

        "buy_quote_60s": buy_60,
        "sell_quote_60s": sell_60,

        "cvd_quote_60s": cvd_60,
        "cvd_quote_30s": cvd_30,

        "aggressive_buy_ratio": aggressive_buy_ratio,

        "trade_count_60s": trade_count_60,
        "trade_count_30s": trade_count_30,

        "trade_acceleration": trade_acceleration,

        "average_trade_size_quote": average_trade_size,

        "ofi": ofi,

        "ofi_persistence": ofi_persistence,

        "obi": obi,

        "ask_depletion": ask_depletion,
        "bid_depletion": bid_depletion,

        "last_trade_price": state.get(
            "last_trade_price",
            0.0,
        ),

        "last_trade_ms": state.get(
            "last_trade_ms",
            0,
        ),
    }


# =============================================================================
# V10 EVALUATION
# =============================================================================

def evaluate_symbol(
    symbol: str,
) -> Optional[dict]:

    s = structure.get(
        symbol
    )

    if not s:
        return None

    m = micro_metrics(
        symbol
    )

    confirmations = list(
        s.get(
            "structure_confirmations",
            [],
        )
    )

    micro_confirmations = []

    # -------------------------------------------------------------------------
    # CVD
    # -------------------------------------------------------------------------

    cvd_positive = (
        m[
            "cvd_quote_60s"
        ] > 0
    )

    cvd_buy_dominance = (
        m[
            "aggressive_buy_ratio"
        ] >= 0.55
    )

    if cvd_positive:
        micro_confirmations.append(
            "POSITIVE_CVD"
        )

    if cvd_buy_dominance:
        micro_confirmations.append(
            "AGGRESSIVE_BUY_DOMINANCE"
        )

    # -------------------------------------------------------------------------
    # OFI
    # -------------------------------------------------------------------------

    ofi_positive = (
        m["ofi"] > 0.05
    )

    ofi_persistent = (
        m[
            "ofi_persistence"
        ] >= 0.60
    )

    if ofi_positive:
        micro_confirmations.append(
            "POSITIVE_OFI"
        )

    if ofi_persistent:
        micro_confirmations.append(
            "OFI_PERSISTENCE"
        )

    # -------------------------------------------------------------------------
    # OBI
    # -------------------------------------------------------------------------

    obi_bullish = (
        m["obi"] >= 0.10
    )

    if obi_bullish:
        micro_confirmations.append(
            "BID_DEPTH_IMBALANCE"
        )

    # -------------------------------------------------------------------------
    # Ask depletion
    # -------------------------------------------------------------------------

    ask_depletion = (
        m[
            "ask_depletion"
        ] > 0.03
    )

    if ask_depletion:
        micro_confirmations.append(
            "ASK_LIQUIDITY_DEPLETION"
        )

    # -------------------------------------------------------------------------
    # Trade acceleration
    # -------------------------------------------------------------------------

    trade_acceleration = (
        m[
            "trade_acceleration"
        ] >= 1.20
    )

    if trade_acceleration:
        micro_confirmations.append(
            "TRADE_COUNT_ACCELERATION"
        )

    confirmations.extend(
        micro_confirmations
    )

    structure_ok = bool(
        s[
            "structural_support"
        ]
        or s[
            "breakout_near"
        ]
        or s[
            "breakout"
        ]
    )

    volume_ok = (
        s[
            "volume_acceleration"
        ] >= 1.05
    )

    micro_ready = bool(
        m[
            "micro_ready"
        ]
    )

    anti_chase = bool(
        s[
            "anti_chase"
        ]
    )

    micro_count = len(
        micro_confirmations
    )

    total_count = len(
        confirmations
    )

    # -------------------------------------------------------------------------
    # Hard BUY gates
    # -------------------------------------------------------------------------

    buy_gate = all(
        [
            not anti_chase,

            micro_ready,

            structure_ok,

            volume_ok,

            cvd_buy_dominance,

            ofi_positive,

            ofi_persistent,

            micro_count
            >= BUY_MIN_MICRO_CONFIRMATIONS,

            total_count
            >= BUY_MIN_CONFIRMATIONS,
        ]
    )

    pre_gate = all(
        [
            not anti_chase,

            structure_ok,

            micro_ready,

            micro_count
            >= PRE_MIN_MICRO_CONFIRMATIONS,

            total_count
            >= PRE_MIN_CONFIRMATIONS,
        ]
    )

    if buy_gate:
        state = "BUY"

    elif pre_gate:
        state = "PRE-IGNITION"

    elif (
        s[
            "ema50_near"
        ]
        or s[
            "ema200_near"
        ]
        or s[
            "breakout_near"
        ]
    ):
        state = "WATCH"

    else:
        state = "MONITOR"

    # -------------------------------------------------------------------------
    # Ranking
    # -------------------------------------------------------------------------

    score = 0.0

    score += min(
        total_count * 7.0,
        42.0,
    )

    score += clamp(
        m[
            "aggressive_buy_ratio"
        ] * 20.0,
        0.0,
        20.0,
    )

    score += clamp(
        max(
            m["ofi"],
            0.0,
        ) * 15.0,
        0.0,
        15.0,
    )

    score += clamp(
        max(
            m["obi"],
            0.0,
        ) * 10.0,
        0.0,
        10.0,
    )

    score += clamp(
        s[
            "volume_acceleration"
        ] * 5.0,
        0.0,
        10.0,
    )

    if s[
        "compression"
    ]:
        score += 5.0

    if s[
        "breakout_near"
    ]:
        score += 5.0

    if anti_chase:
        score -= 30.0

    score = round(
        max(
            score,
            0.0,
        ),
        2,
    )

    return {
        "symbol": symbol,

        "state": state,

        "score": score,

        "price": s[
            "price"
        ],

        "ema50_4h": s[
            "ema50_4h"
        ],

        "ema200_4h": s[
            "ema200_4h"
        ],

        "atr14_4h": s[
            "atr14_4h"
        ],

        "distance_ema50_atr": s[
            "distance_ema50_atr"
        ],

        "distance_ema200_atr": s[
            "distance_ema200_atr"
        ],

        "ema50_touch": s[
            "ema50_touch"
        ],

        "ema200_touch": s[
            "ema200_touch"
        ],

        "ema50_reclaim": s[
            "ema50_reclaim"
        ],

        "ema200_reclaim": s[
            "ema200_reclaim"
        ],

        "volume_acceleration": s[
            "volume_acceleration"
        ],

        "compression": s[
            "compression"
        ],

        "compression_ratio": s[
            "compression_ratio"
        ],

        "resistance": s[
            "resistance"
        ],

        "breakout_distance_pct": s[
            "breakout_distance_pct"
        ],

        "breakout_near": s[
            "breakout_near"
        ],

        "breakout": s[
            "breakout"
        ],

        "anti_chase": anti_chase,

        "micro_ready": micro_ready,

        "cvd_quote_60s": m[
            "cvd_quote_60s"
        ],

        "aggressive_buy_ratio": m[
            "aggressive_buy_ratio"
        ],

        "ofi": m[
            "ofi"
        ],

        "ofi_persistence": m[
            "ofi_persistence"
        ],

        "obi": m[
            "obi"
        ],

        "ask_depletion": m[
            "ask_depletion"
        ],

        "bid_depletion": m[
            "bid_depletion"
        ],

        "trade_count_60s": m[
            "trade_count_60s"
        ],

        "trade_acceleration": m[
            "trade_acceleration"
        ],

        "structure_confirmations": s[
            "structure_confirmations"
        ],

        "micro_confirmations": micro_confirmations,

        "confirmations": confirmations,

        "confirmation_count": total_count,

        "micro_confirmation_count": micro_count,

        "quote_volume_24h": s[
            "quote_volume_24h"
        ],

        "updated_ms": s[
            "updated_ms"
        ],
    }


# =============================================================================
# RANKING
# =============================================================================

STATE_PRIORITY = {
    "BUY": 4,
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
                row[
                    "state"
                ],
                0,
            ),
            row[
                "score"
            ],
            row[
                "quote_volume_24h"
            ],
        ),
        reverse=True,
    )

    return rows[
        :limit
    ]


# =============================================================================
# STRUCTURE REFRESH
# =============================================================================

async def structure_batch(
    symbols: List[str],
):

    if session is None:
        return

    semaphore = asyncio.Semaphore(
        8
    )

    async def worker(
        symbol: str,
    ):

        async with semaphore:

            result = await load_4h_structure(
                session,
                symbol,
            )

            if result:
                structure[
                    symbol
                ] = result

    await asyncio.gather(
        *[
            worker(
                symbol
            )
            for symbol in symbols
        ],
        return_exceptions=True,
    )


async def refresh_structure():

    global selected_micro_symbols
    global last_structure_refresh
    global scanner_ready

    if session is None:
        return

    print(
        "Ψ-V10: loading Binance universe..."
    )

    universe = await get_exchange_symbols(
        session
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
        f"{len(symbols)} liquid Spot USDT markets..."
    )

    await structure_batch(
        symbols
    )

    # Rank structural candidates before allocating
    # expensive live microstructure streams.
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

        structural_score = 0.0

        structural_score += (
            len(
                row[
                    "structure_confirmations"
                ]
            )
            * 10.0
        )

        structural_score += max(
            0.0,
            20.0 - proximity * 5.0,
        )

        structural_score += min(
            row[
                "volume_acceleration"
            ] * 5.0,
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

    selected_micro_symbols = [
        symbol
        for _, _, symbol
        in candidates[
            :MICRO_UNIVERSE_SIZE
        ]
    ]

    for symbol in selected_micro_symbols:
        ensure_micro_state(
            symbol
        )
        ensure_depth_worker(
            symbol
        )

    last_structure_refresh = time.time()

    scanner_ready = True

    print(
        "Ψ-V10 STRUCTURE READY"
    )

    print(
        "Micro universe:",
        ", ".join(
            selected_micro_symbols
        ),
    )


async def structure_refresh_loop():

    while True:

        try:
            await refresh_structure()

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            global last_error

            last_error = (
                f"STRUCTURE: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error
            )

        await asyncio.sleep(
            STRUCTURE_REFRESH_SECONDS
        )


# =============================================================================
# WEBSOCKET
# =============================================================================

async def websocket_loop():

    global websocket_connected
    global last_error

    while True:

        try:

            symbols = list(
                selected_micro_symbols
            )

            if not symbols:

                await asyncio.sleep(
                    2
                )

                continue

            streams = []

            for symbol in symbols:

                lower = symbol.lower()

                streams.append(
                    f"{lower}@aggTrade"
                )

                streams.append(
                    f"{lower}@depth@100ms"
                )

            stream_string = "/".join(
                streams
            )

            url = (
                f"{WS_BASE}/stream"
                f"?streams={stream_string}"
            )

            print(
                f"Ψ-V10 WebSocket connecting "
                f"for {len(symbols)} symbols..."
            )

            async with session.ws_connect(
                url,
                heartbeat=30,
                receive_timeout=90,
                max_msg_size=0,
            ) as ws:

                websocket_connected = True

                print(
                    "Ψ-V10 WebSocket connected."
                )

                # Buffer events first, then snapshots can
                # reconcile against the stream.
                bootstrap_tasks = []

                for symbol in symbols:
                    bootstrap_tasks.append(
                        asyncio.create_task(
                            resync_book(
                                symbol
                            )
                        )
                    )

                if bootstrap_tasks:
                    await asyncio.gather(
                        *bootstrap_tasks,
                        return_exceptions=True,
                    )

                async for message in ws:

                    if message.type == aiohttp.WSMsgType.TEXT:

                        payload = json.loads(
                            message.data
                        )

                        stream_name = payload.get(
                            "stream",
                            "",
                        )

                        data = payload.get(
                            "data",
                            {},
                        )

                        if not stream_name:
                            continue

                        symbol = (
                            stream_name
                            .split("@")[0]
                            .upper()
                        )

                        if (
                            "@aggTrade"
                            in stream_name
                        ):
                            process_agg_trade(
                                symbol,
                                data,
                            )

                        elif (
                            "@depth"
                            in stream_name
                        ):

                            ensure_depth_worker(
                                symbol
                            )

                            queue = depth_queues[
                                symbol
                            ]

                            try:
                                queue.put_nowait(
                                    data
                                )

                            except asyncio.QueueFull:

                                # Dropping a depth event means
                                # local book continuity can no
                                # longer be trusted.
                                books.pop(
                                    symbol,
                                    None,
                                )

                                while not queue.empty():

                                    try:
                                        queue.get_nowait()
                                        queue.task_done()

                                    except asyncio.QueueEmpty:
                                        break

                    elif message.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            websocket_connected = False

            last_error = (
                f"WEBSOCKET: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error
            )

        websocket_connected = False

        await asyncio.sleep(
            5
        )


# =============================================================================
# MICRO UNIVERSE WATCHER
# =============================================================================

async def micro_universe_watcher():

    previous = set()

    while True:

        current = set(
            selected_micro_symbols
        )

        if current != previous:

            print(
                "Ψ-V10 micro universe changed."
            )

            # Force WS reconnect naturally by cancelling
            # and recreating is cleaner, but the primary
            # websocket supervisor below handles this via
            # periodic refresh.
            previous = current

        await asyncio.sleep(
            MICRO_REFRESH_SECONDS
        )


# =============================================================================
# CONSOLE OUTPUT
# =============================================================================

async def print_loop():

    while True:

        await asyncio.sleep(
            30
        )

        try:

            results = ranked_results(
                10
            )

            print(
                "\n"
                "=================================================="
            )

            print(
                "Ψ-V10 LIVE TOP 10"
            )

            print(
                "=================================================="
            )

            for index, row in enumerate(
                results,
                start=1,
            ):

                print(
                    f"{index:02d}. "
                    f"{row['symbol']:12s} "
                    f"{row['state']:14s} "
                    f"score={row['score']:6.2f} "
                    f"conf={row['confirmation_count']} "
                    f"micro={row['micro_confirmation_count']} "
                    f"OFI={row['ofi']:+.3f} "
                    f"OBI={row['obi']:+.3f} "
                    f"buy={row['aggressive_buy_ratio']:.2%}"
                )

        except Exception as exc:

            print(
                f"[PRINT ERROR] {exc}"
            )


# =============================================================================
# HTTP
# =============================================================================

async def health(
    request: web.Request,
):

    uptime = int(
        time.time()
        - scanner_started_at
    )

    return web.json_response(
        {
            "ok": True,

            "service": "psi-v10-live-scanner",

            "version": "2.0",

            "scanner_ready": scanner_ready,

            "rest_connected": rest_connected,

            "websocket_connected": websocket_connected,

            "rest_base": REST_BASE,

            "structure_symbols": len(
                structure
            ),

            "micro_symbols": len(
                selected_micro_symbols
            ),

            "uptime_seconds": uptime,

            "last_error": last_error,

            "endpoints": {
                "health": "/health",
                "scan": "/scan",
            },
        }
    )


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
        limit = RETURN_LIMIT

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
            row[
                "state"
            ]
        ] += 1

    return web.json_response(
        {
            "ok": True,

            "scanner": "Ψ-V10",

            "source": (
                "Binance public Spot market data"
            ),

            "rest_base": REST_BASE,

            "timeframe": "4h",

            "moving_averages": [
                "EMA50",
                "EMA200",
            ],

            "microstructure": [
                "aggTrade CVD",
                "L1-L10 OBI",
                "L1-L10 OFI",
                "ask depletion",
                "bid depletion",
                "trade acceleration",
                "OFI persistence",
            ],

            "scanner_ready": scanner_ready,

            "websocket_connected": websocket_connected,

            "universe_size": len(
                structure
            ),

            "micro_universe_size": len(
                selected_micro_symbols
            ),

            "state_counts": dict(
                states
            ),

            "returned": len(
                results
            ),

            "results": results,

            "generated_ms": now_ms(),
        }
    )


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
        f"on port {PORT}"
    )

    return runner


# =============================================================================
# INITIALISATION
# =============================================================================

async def initialise():

    print(
        "Ψ-V10 INITIALISING..."
    )

    print(
        f"REST: {REST_BASE}"
    )

    print(
        f"WS:   {WS_BASE}"
    )

    # Initial structure scan.
    await refresh_structure()

    print(
        "Ψ-V10 INITIALISED."
    )


# =============================================================================
# MAIN
# =============================================================================

async def main():

    global session

    timeout = aiohttp.ClientTimeout(
        total=30
    )

    connector = aiohttp.TCPConnector(
        limit=100,
        ttl_dns_cache=300,
    )

    session = aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers={
            "User-Agent": USER_AGENT,
        },
    )

    # IMPORTANT:
    #
    # Start HTTP BEFORE Binance initialisation.
    #
    # This means Railway's health check can reach the
    # service even while the scanner is warming up.
    runner = await start_http_server()

    tasks = []

    try:

        try:
            await initialise()

        except Exception as exc:

            global last_error

            last_error = (
                f"INITIALISE: "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            print(
                last_error
            )

            # Do NOT kill Railway.
            # Background refresh loop will retry.
            print(
                "Ψ-V10 will retry in background."
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

            asyncio.create_task(
                micro_universe_watcher()
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
            "Ψ-V10 stopped."
        )
