# ================================================================
# Ψ-V10 BINANCE LIVE SCANNER
# ================================================================
#
# LIVE DATA:
#   Binance Spot REST + WebSocket
#
# CORE:
#   - Binance USDT universe
#   - 4H EMA50 / EMA200
#   - ATR
#   - Relative volume acceleration
#   - Range compression
#   - Breakout distance
#   - Anti-chase filter
#
# LIVE MICROSTRUCTURE:
#   - Sequential local order book
#   - L1-L10 depth
#   - Order Flow Imbalance (OFI)
#   - Order Book Imbalance (OBI)
#   - Rolling aggressive-buy CVD
#   - Ask liquidity depletion
#   - Bid liquidity depletion
#   - Persistence
#
# OUTPUT:
#   BUY
#   PRE-IGNITION
#   WATCH
#   REJECT
#
# IMPORTANT:
#   Scanner only. No automatic order execution.
#
# ================================================================

import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque
from statistics import mean

import aiohttp


# ================================================================
# CONFIG
# ================================================================

REST_BASE = os.getenv(
    "BINANCE_REST",
    "https://api.binance.com"
)

WS_BASE = os.getenv(
    "BINANCE_WS",
    "wss://stream.binance.com:9443/ws"
)

QUOTE_ASSET = "USDT"

# How many high-liquidity pairs receive full live microstructure
MAX_LIVE_SYMBOLS = int(
    os.getenv("MAX_LIVE_SYMBOLS", "30")
)

# Scanner returns this many
TOP_RESULTS = int(
    os.getenv("TOP_RESULTS", "10")
)

# Minimum 24h USDT quote volume
MIN_QUOTE_VOLUME = float(
    os.getenv("MIN_QUOTE_VOLUME", "5000000")
)

# 4H historical candles
KLINE_LIMIT = 260

# Book
BOOK_LEVELS = 10
SNAPSHOT_LIMIT = 1000

# Rolling microstructure window
MICRO_WINDOW_SEC = 60

# Need some data before trusting microstructure
MIN_BOOK_SAMPLES = 10
MIN_TRADE_SAMPLES = 10

# Refresh structural analysis
STRUCTURE_REFRESH_SEC = 60

# Full universe refresh
UNIVERSE_REFRESH_SEC = 1800

# Scanner output refresh
PRINT_INTERVAL_SEC = 15

# EMA proximity
EMA_NEAR_PCT = 2.0

# Anti-chase
MAX_ATR_EXTENSION = 2.25
MAX_4H_PUMP_PCT = 8.0

# Breakout proximity
MAX_BREAKOUT_DISTANCE_PCT = 2.5

# Micro thresholds
MIN_CVD_RATIO = 0.10
MIN_OFI = 0.03
MIN_OBI = 0.08
MIN_ASK_DEPLETION = 0.05

# V10
BUY_MIN_CONFIRMATIONS = 5
PRE_MIN_CONFIRMATIONS = 4


# ================================================================
# OPTIONAL SYMBOL FILTER
# ================================================================
#
# If you have a confirmed Binance-UK symbol allowlist, place it in:
#
# UK_SYMBOLS=BTCUSDT,ETHUSDT,XRPUSDT,...
#
# If absent, scanner uses active Binance Spot USDT pairs.
#
# ================================================================

UK_SYMBOLS_ENV = os.getenv(
    "UK_SYMBOLS",
    ""
).strip()

UK_SYMBOLS = {
    x.strip().upper()
    for x in UK_SYMBOLS_ENV.split(",")
    if x.strip()
}


# ================================================================
# GLOBAL STATE
# ================================================================

session = None

symbol_meta = {}
structure = {}

books = {}
book_locks = defaultdict(asyncio.Lock)

depth_buffers = defaultdict(deque)

micro = defaultdict(
    lambda: {
        "trades": deque(),
        "book_samples": deque(),

        "buy_quote": 0.0,
        "sell_quote": 0.0,

        "trade_count": 0,

        "ofi": 0.0,
        "obi": 0.0,

        "ask_depletion": 0.0,
        "bid_depletion": 0.0,

        "book_samples_count": 0,

        "last_trade_ts": 0,
        "last_book_ts": 0,
    }
)

live_symbols = []

last_universe_refresh = 0


# ================================================================
# HTTP
# ================================================================

async def api_get(path, params=None, timeout=15):

    url = REST_BASE + path

    async with session.get(
        url,
        params=params,
        timeout=aiohttp.ClientTimeout(total=timeout)
    ) as r:

        if r.status == 429:
            retry = float(
                r.headers.get("Retry-After", "2")
            )

            await asyncio.sleep(
                max(retry, 1)
            )

            raise RuntimeError(
                "Binance rate limit 429"
            )

        if r.status != 200:

            text = await r.text()

            raise RuntimeError(
                f"Binance HTTP {r.status}: {text}"
            )

        return await r.json()


# ================================================================
# BASIC MATH
# ================================================================

def safe_div(a, b):

    if not b:
        return 0.0

    return a / b


def pct_distance(price, reference):

    if reference <= 0:
        return 999.0

    return (
        (price - reference)
        / reference
    ) * 100.0


def ema(values, period):

    if len(values) < period:
        return None

    alpha = 2.0 / (
        period + 1.0
    )

    result = mean(
        values[:period]
    )

    for value in values[period:]:

        result = (
            value * alpha
            +
            result * (1.0 - alpha)
        )

    return result


def true_ranges(highs, lows, closes):

    output = []

    for i in range(1, len(closes)):

        output.append(
            max(
                highs[i] - lows[i],
                abs(
                    highs[i]
                    - closes[i - 1]
                ),
                abs(
                    lows[i]
                    - closes[i - 1]
                )
            )
        )

    return output


def atr(
    highs,
    lows,
    closes,
    period=14
):

    tr = true_ranges(
        highs,
        lows,
        closes
    )

    if len(tr) < period:
        return None

    return mean(
        tr[-period:]
    )


# ================================================================
# EXCHANGE UNIVERSE
# ================================================================

async def get_exchange_symbols():

    info, tickers = await asyncio.gather(
        api_get(
            "/api/v3/exchangeInfo"
        ),
        api_get(
            "/api/v3/ticker/24hr"
        )
    )

    ticker_map = {
        x["symbol"]: x
        for x in tickers
    }

    candidates = []

    for item in info["symbols"]:

        symbol = item["symbol"]

        if (
            item.get("status") != "TRADING"
        ):
            continue

        if (
            item.get("quoteAsset")
            != QUOTE_ASSET
        ):
            continue

        if not item.get(
            "isSpotTradingAllowed",
            True
        ):
            continue

        # Optional externally verified
        # Binance UK allowlist
        if (
            UK_SYMBOLS
            and symbol not in UK_SYMBOLS
        ):
            continue

        ticker = ticker_map.get(
            symbol
        )

        if not ticker:
            continue

        try:

            quote_volume = float(
                ticker.get(
                    "quoteVolume",
                    0
                )
            )

        except Exception:
            continue

        if (
            quote_volume
            < MIN_QUOTE_VOLUME
        ):
            continue

        candidates.append(
            (
                symbol,
                quote_volume
            )
        )

        symbol_meta[symbol] = {
            "quote_volume":
                quote_volume,

            "base_asset":
                item.get(
                    "baseAsset"
                )
        }

    candidates.sort(
        key=lambda x: x[1],
        reverse=True
    )

    return [
        x[0]
        for x in candidates
    ]


# ================================================================
# 4H STRUCTURAL ENGINE
# ================================================================

async def load_4h_structure(symbol):

    try:

        data = await api_get(
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": "4h",
                "limit": KLINE_LIMIT
            }
        )

        if len(data) < 210:
            return None

        opens = [
            float(x[1])
            for x in data
        ]

        highs = [
            float(x[2])
            for x in data
        ]

        lows = [
            float(x[3])
            for x in data
        ]

        closes = [
            float(x[4])
            for x in data
        ]

        volumes = [
            float(x[5])
            for x in data
        ]

        quote_volumes = [
            float(x[7])
            for x in data
        ]

        price = closes[-1]

        ema50 = ema(
            closes,
            50
        )

        ema200 = ema(
            closes,
            200
        )

        current_atr = atr(
            highs,
            lows,
            closes,
            14
        )

        if (
            ema50 is None
            or ema200 is None
            or current_atr is None
        ):
            return None

        # ----------------------------
        # EMA distance
        # ----------------------------

        ema50_dist = abs(
            pct_distance(
                price,
                ema50
            )
        )

        ema200_dist = abs(
            pct_distance(
                price,
                ema200
            )
        )

        nearest_ema_dist = min(
            ema50_dist,
            ema200_dist
        )

        nearest_ema = (
            "EMA50"
            if ema50_dist <= ema200_dist
            else "EMA200"
        )

        # ----------------------------
        # ATR-adjusted EMA proximity
        # ----------------------------

        ema50_atr_dist = (
            abs(price - ema50)
            / current_atr
        )

        ema200_atr_dist = (
            abs(price - ema200)
            / current_atr
        )

        nearest_ema_atr = min(
            ema50_atr_dist,
            ema200_atr_dist
        )

        ema_near = (
            nearest_ema_dist
            <= EMA_NEAR_PCT
            or nearest_ema_atr
            <= 1.0
        )

        # ----------------------------
        # Trend
        # ----------------------------

        bullish_ema_structure = (
            price > ema50
            and ema50 > ema200
        )

        ema_reclaim = (
            closes[-2] <= ema50
            and closes[-1] > ema50
        ) or (
            closes[-2] <= ema200
            and closes[-1] > ema200
        )

        # ----------------------------
        # Relative volume
        # ----------------------------

        recent_qv = quote_volumes[-1]

        baseline_qv = mean(
            quote_volumes[-21:-1]
        )

        relative_volume = safe_div(
            recent_qv,
            baseline_qv
        )

        # Faster acceleration
        qv_3 = mean(
            quote_volumes[-3:]
        )

        qv_prev_10 = mean(
            quote_volumes[-13:-3]
        )

        volume_acceleration = safe_div(
            qv_3,
            qv_prev_10
        )

        # ----------------------------
        # Compression
        # ----------------------------

        ranges = [
            highs[i] - lows[i]
            for i in range(
                len(highs)
            )
        ]

        recent_range = mean(
            ranges[-4:]
        )

        baseline_range = mean(
            ranges[-24:-4]
        )

        compression_ratio = safe_div(
            recent_range,
            baseline_range
        )

        compression = (
            compression_ratio
            < 0.75
        )

        # ----------------------------
        # Local breakout
        # ----------------------------

        resistance = max(
            highs[-21:-1]
        )

        breakout_distance = (
            (
                resistance - price
            )
            / price
            * 100.0
        )

        breakout = (
            price > resistance
        )

        near_breakout = (
            -1.0
            <= breakout_distance
            <= MAX_BREAKOUT_DISTANCE_PCT
        )

        # ----------------------------
        # 4H candle movement
        # ----------------------------

        candle_change = (
            (
                closes[-1]
                - opens[-1]
            )
            / opens[-1]
            * 100.0
        )

        # ----------------------------
        # Anti chase
        # ----------------------------

        atr_extension = (
            abs(
                price - ema50
            )
            / current_atr
        )

        anti_chase = (
            atr_extension
            > MAX_ATR_EXTENSION
            or candle_change
            > MAX_4H_PUMP_PCT
        )

        result = {

            "symbol":
                symbol,

            "price":
                price,

            "ema50":
                ema50,

            "ema200":
                ema200,

            "ema50_distance_pct":
                ema50_dist,

            "ema200_distance_pct":
                ema200_dist,

            "nearest_ema":
                nearest_ema,

            "nearest_ema_distance_pct":
                nearest_ema_dist,

            "nearest_ema_atr":
                nearest_ema_atr,

            "ema_near":
                ema_near,

            "ema_reclaim":
                ema_reclaim,

            "bullish_ema_structure":
                bullish_ema_structure,

            "atr":
                current_atr,

            "relative_volume":
                relative_volume,

            "volume_acceleration":
                volume_acceleration,

            "compression_ratio":
                compression_ratio,

            "compression":
                compression,

            "resistance":
                resistance,

            "breakout_distance_pct":
                breakout_distance,

            "breakout":
                breakout,

            "near_breakout":
                near_breakout,

            "4h_change_pct":
                candle_change,

            "atr_extension":
                atr_extension,

            "anti_chase":
                anti_chase,

            "updated":
                time.time()
        }

        structure[symbol] = result

        return result

    except Exception as e:

        print(
            f"[STRUCTURE ERROR] "
            f"{symbol}: {e}"
        )

        return None


# ================================================================
# ORDER BOOK HELPERS
# ================================================================

def sorted_top10(book):

    bids = sorted(
        book["bids"].items(),
        key=lambda x: x[0],
        reverse=True
    )[:BOOK_LEVELS]

    asks = sorted(
        book["asks"].items(),
        key=lambda x: x[0]
    )[:BOOK_LEVELS]

    return bids, asks


def depth_notional(levels):

    return sum(
        price * qty
        for price, qty in levels
    )


def calculate_obi(
    bids,
    asks
):

    bid_depth = depth_notional(
        bids
    )

    ask_depth = depth_notional(
        asks
    )

    total = (
        bid_depth
        +
        ask_depth
    )

    return safe_div(
        bid_depth - ask_depth,
        total
    )


def calculate_ofi(
    previous_bids,
    previous_asks,
    current_bids,
    current_asks
):

    prev_bid = dict(
        previous_bids
    )

    prev_ask = dict(
        previous_asks
    )

    curr_bid = dict(
        current_bids
    )

    curr_ask = dict(
        current_asks
    )

    bid_flow = 0.0
    ask_flow = 0.0

    for price in (
        set(prev_bid)
        | set(curr_bid)
    ):

        old = prev_bid.get(
            price,
            0.0
        )

        new = curr_bid.get(
            price,
            0.0
        )

        bid_flow += (
            new - old
        ) * price

    for price in (
        set(prev_ask)
        | set(curr_ask)
    ):

        old = prev_ask.get(
            price,
            0.0
        )

        new = curr_ask.get(
            price,
            0.0
        )

        # Removing asks =
        # positive pressure
        ask_flow += (
            old - new
        ) * price

    total_depth = (
        depth_notional(
            current_bids
        )
        +
        depth_notional(
            current_asks
        )
    )

    return safe_div(
        bid_flow + ask_flow,
        total_depth
    )


def depletion(
    previous,
    current
):

    if previous <= 0:
        return 0.0

    return max(
        0.0,
        (
            previous
            - current
        )
        / previous
    )


# ================================================================
# MICRO SAMPLE
# ================================================================

def record_book_sample(
    symbol,
    previous_bids,
    previous_asks,
    current_bids,
    current_asks,
    timestamp
):

    m = micro[symbol]

    ofi = calculate_ofi(
        previous_bids,
        previous_asks,
        current_bids,
        current_asks
    )

    obi = calculate_obi(
        current_bids,
        current_asks
    )

    prev_bid_depth = (
        depth_notional(
            previous_bids
        )
    )

    prev_ask_depth = (
        depth_notional(
            previous_asks
        )
    )

    curr_bid_depth = (
        depth_notional(
            current_bids
        )
    )

    curr_ask_depth = (
        depth_notional(
            current_asks
        )
    )

    ask_dep = depletion(
        prev_ask_depth,
        curr_ask_depth
    )

    bid_dep = depletion(
        prev_bid_depth,
        curr_bid_depth
    )

    m["ofi"] = ofi
    m["obi"] = obi

    m["ask_depletion"] = (
        ask_dep
    )

    m["bid_depletion"] = (
        bid_dep
    )

    m["last_book_ts"] = (
        timestamp
    )

    m["book_samples"].append(
        {
            "ts":
                timestamp,

            "ofi":
                ofi,

            "obi":
                obi,

            "ask_depletion":
                ask_dep,

            "bid_depletion":
                bid_dep
        }
    )

    cutoff = (
        timestamp
        - MICRO_WINDOW_SEC
    )

    while (
        m["book_samples"]
        and
        m["book_samples"][0]["ts"]
        < cutoff
    ):
        m["book_samples"].popleft()

    m["book_samples_count"] += 1


# ================================================================
# AGGTRADE -> CVD
# ================================================================

def process_agg_trade(
    symbol,
    data
):

    m = micro[symbol]

    timestamp = (
        data.get(
            "T",
            data.get(
                "E",
                int(
                    time.time() * 1000
                )
            )
        )
        / 1000.0
    )

    price = float(
        data["p"]
    )

    qty = float(
        data["q"]
    )

    quote_notional = (
        price * qty
    )

    # Binance aggTrade:
    #
    # m = True:
    # buyer is maker
    # -> aggressive seller
    #
    # m = False:
    # seller is maker
    # -> aggressive buyer

    aggressive_buy = (
        not bool(
            data["m"]
        )
    )

    signed_quote = (
        quote_notional
        if aggressive_buy
        else -quote_notional
    )

    m["trades"].append(
        (
            timestamp,
            signed_quote,
            quote_notional
        )
    )

    cutoff = (
        timestamp
        - MICRO_WINDOW_SEC
    )

    while (
        m["trades"]
        and
        m["trades"][0][0]
        < cutoff
    ):
        m["trades"].popleft()

    buy_quote = 0.0
    sell_quote = 0.0

    for (
        _,
        signed,
        notional
    ) in m["trades"]:

        if signed > 0:
            buy_quote += notional
        else:
            sell_quote += notional

    m["buy_quote"] = (
        buy_quote
    )

    m["sell_quote"] = (
        sell_quote
    )

    m["trade_count"] = len(
        m["trades"]
    )

    m["last_trade_ts"] = (
        timestamp
    )


# ================================================================
# LOCAL ORDER BOOK SNAPSHOT
# ================================================================

async def get_depth_snapshot(
    symbol
):

    data = await api_get(
        "/api/v3/depth",
        {
            "symbol":
                symbol,

            "limit":
                SNAPSHOT_LIMIT
        }
    )

    return {

        "bids": {
            float(p): float(q)
            for p, q
            in data["bids"]
            if float(q) > 0
        },

        "asks": {
            float(p): float(q)
            for p, q
            in data["asks"]
            if float(q) > 0
        },

        "last_update_id":
            int(
                data["lastUpdateId"]
            ),

        "ready":
            False
    }


# ================================================================
# APPLY DEPTH EVENT
# ================================================================

def apply_levels(
    side,
    updates
):

    for price_raw, qty_raw in updates:

        price = float(
            price_raw
        )

        qty = float(
            qty_raw
        )

        if qty == 0.0:

            side.pop(
                price,
                None
            )

        else:

            side[price] = qty


def apply_depth_event(
    symbol,
    event
):

    book = books.get(
        symbol
    )

    if not book:
        return False

    U = int(
        event["U"]
    )

    u = int(
        event["u"]
    )

    last_id = int(
        book["last_update_id"]
    )

    # Old event
    if u <= last_id:
        return True

    # Gap:
    # expected local update ID + 1
    # must fall inside U..u
    if U > (
        last_id + 1
    ):

        return False

    previous_bids, previous_asks = (
        sorted_top10(
            book
        )
    )

    apply_levels(
        book["bids"],
        event.get(
            "b",
            []
        )
    )

    apply_levels(
        book["asks"],
        event.get(
            "a",
            []
        )
    )

    book["last_update_id"] = u
    book["ready"] = True

    current_bids, current_asks = (
        sorted_top10(
            book
        )
    )

    if (
        previous_bids
        and previous_asks
        and current_bids
        and current_asks
    ):

        record_book_sample(
            symbol,
            previous_bids,
            previous_asks,
            current_bids,
            current_asks,
            event.get(
                "E",
                int(
                    time.time()
                    * 1000
                )
            )
            / 1000.0
        )

    return True


# ================================================================
# RESYNC LOCAL BOOK
# ================================================================

async def resync_book(
    symbol
):

    async with book_locks[
        symbol
    ]:

        try:

            snapshot = (
                await get_depth_snapshot(
                    symbol
                )
            )

            books[symbol] = snapshot

            buffered = list(
                depth_buffers[
                    symbol
                ]
            )

            depth_buffers[
                symbol
            ].clear()

            last_id = snapshot[
                "last_update_id"
            ]

            # Remove events already covered
            buffered = [
                e
                for e in buffered
                if int(e["u"])
                > last_id
            ]

            started = False

            for event in buffered:

                U = int(
                    event["U"]
                )

                u = int(
                    event["u"]
                )

                current_id = books[
                    symbol
                ][
                    "last_update_id"
                ]

                if not started:

                    if not (
                        U
                        <= current_id + 1
                        <= u
                    ):
                        continue

                    started = True

                ok = apply_depth_event(
                    symbol,
                    event
                )

                if not ok:

                    books.pop(
                        symbol,
                        None
                    )

                    return False

            return True

        except Exception as e:

            print(
                f"[BOOK RESYNC ERROR] "
                f"{symbol}: {e}"
            )

            return False


# ================================================================
# DEPTH HANDLER
# ================================================================

async def process_depth(
    symbol,
    event
):

    depth_buffers[
        symbol
    ].append(
        event
    )

    # Don't allow unbounded buffer
    while (
        len(
            depth_buffers[
                symbol
            ]
        )
        > 5000
    ):

        depth_buffers[
            symbol
        ].popleft()

    book = books.get(
        symbol
    )

    if book is None:

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

        if book is None:
            return

        U = int(
            event["U"]
        )

        u = int(
            event["u"]
        )

        last_id = int(
            book[
                "last_update_id"
            ]
        )

        if u <= last_id:
            return

        if (
            U > last_id + 1
        ):

            print(
                f"[BOOK GAP] "
                f"{symbol} "
                f"local={last_id} "
                f"U={U} "
                f"u={u}"
            )

            books.pop(
                symbol,
                None
            )

            need_resync = True

        else:

            apply_depth_event(
                symbol,
                event
            )

            need_resync = False

    if need_resync:

        await resync_book(
            symbol
        )


# ================================================================
# MICROSTRUCTURE ANALYSIS
# ================================================================

def micro_metrics(
    symbol
):

    m = micro[symbol]

    trades = list(
        m["trades"]
    )

    samples = list(
        m["book_samples"]
    )

    if (
        len(trades)
        < MIN_TRADE_SAMPLES
        or
        len(samples)
        < MIN_BOOK_SAMPLES
    ):

        return {
            "ready":
                False,

            "reason":
                "WARMING_UP"
        }

    buy_quote = (
        m["buy_quote"]
    )

    sell_quote = (
        m["sell_quote"]
    )

    total_aggressive = (
        buy_quote
        +
        sell_quote
    )

    cvd = (
        buy_quote
        -
        sell_quote
    )

    cvd_ratio = safe_div(
        cvd,
        total_aggressive
    )

    ofis = [
        x["ofi"]
        for x in samples
    ]

    obis = [
        x["obi"]
        for x in samples
    ]

    ask_deps = [
        x["ask_depletion"]
        for x in samples
    ]

    bid_deps = [
        x["bid_depletion"]
        for x in samples
    ]

    # Recent weighting
    recent_samples = (
        samples[-10:]
    )

    recent_ofi = mean(
        [
            x["ofi"]
            for x
            in recent_samples
        ]
    )

    recent_obi = mean(
        [
            x["obi"]
            for x
            in recent_samples
        ]
    )

    avg_ofi = mean(
        ofis
    )

    avg_obi = mean(
        obis
    )

    avg_ask_dep = mean(
        ask_deps
    )

    avg_bid_dep = mean(
        bid_deps
    )

    # Persistence
    positive_ofi_count = sum(
        1
        for x in recent_samples
        if x["ofi"] > 0
    )

    ofi_persistence = (
        safe_div(
            positive_ofi_count,
            len(
                recent_samples
            )
        )
    )

    positive_obi_count = sum(
        1
        for x in recent_samples
        if x["obi"] > 0
    )

    obi_persistence = (
        safe_div(
            positive_obi_count,
            len(
                recent_samples
            )
        )
    )

    # Trade velocity comparison
    now = time.time()

    recent_15 = [
        x
        for x in trades
        if x[0] >= now - 15
    ]

    prior_45 = [
        x
        for x in trades
        if (
            now - 60
            <= x[0]
            < now - 15
        )
    ]

    recent_rate = (
        len(recent_15)
        / 15.0
    )

    prior_rate = (
        len(prior_45)
        / 45.0
    )

    trade_acceleration = (
        safe_div(
            recent_rate,
            prior_rate
        )
        if prior_rate > 0
        else 0.0
    )

    confirmations = []

    # --------------------------------
    # 1 CVD
    # --------------------------------

    if (
        cvd > 0
        and
        cvd_ratio >= MIN_CVD_RATIO
    ):

        confirmations.append(
            "CVD_BUY_DOMINANCE"
        )

    # --------------------------------
    # 2 OFI
    # --------------------------------

    if (
        recent_ofi
        >= MIN_OFI
    ):

        confirmations.append(
            "POSITIVE_OFI"
        )

    # --------------------------------
    # 3 OFI persistence
    # --------------------------------

    if (
        ofi_persistence
        >= 0.60
    ):

        confirmations.append(
            "OFI_PERSISTENCE"
        )

    # --------------------------------
    # 4 L1-L10 bid imbalance
    # --------------------------------

    if (
        recent_obi
        >= MIN_OBI
    ):

        confirmations.append(
            "L1_L10_BID_IMBALANCE"
        )

    # --------------------------------
    # 5 OBI persistence
    # --------------------------------

    if (
        obi_persistence
        >= 0.60
    ):

        confirmations.append(
            "BOOK_SUPPORT_PERSISTENCE"
        )

    # --------------------------------
    # 6 Ask depletion
    # --------------------------------

    if (
        avg_ask_dep
        >= MIN_ASK_DEPLETION
    ):

        confirmations.append(
            "ASK_LIQUIDITY_DEPLETION"
        )

    # --------------------------------
    # 7 Bid support
    # --------------------------------

    if (
        avg_bid_dep
        < avg_ask_dep
    ):

        confirmations.append(
            "BID_SUPPORT_STABLE"
        )

    # --------------------------------
    # 8 Trade acceleration
    # --------------------------------

    if (
        trade_acceleration
        >= 1.25
    ):

        confirmations.append(
            "TRADE_ACCELERATION"
        )

    return {

        "ready":
            True,

        "cvd_quote":
            cvd,

        "cvd_ratio":
            cvd_ratio,

        "buy_quote":
            buy_quote,

        "sell_quote":
            sell_quote,

        "avg_ofi":
            avg_ofi,

        "recent_ofi":
            recent_ofi,

        "ofi_persistence":
            ofi_persistence,

        "avg_obi":
            avg_obi,

        "recent_obi":
            recent_obi,

        "obi_persistence":
            obi_persistence,

        "ask_depletion":
            avg_ask_dep,

        "bid_depletion":
            avg_bid_dep,

        "trade_acceleration":
            trade_acceleration,

        "trade_count":
            len(trades),

        "book_sample_count":
            len(samples),

        "confirmations":
            confirmations,

        "micro_score":
            len(
                confirmations
            )
    }


# ================================================================
# V10 DECISION ENGINE
# ================================================================

def evaluate_symbol(
    symbol
):

    s = structure.get(
        symbol
    )

    if not s:

        return None

    m = micro_metrics(
        symbol
    )

    confirmations = []

    # ============================================================
    # STRUCTURAL CONFIRMATIONS
    # ============================================================

    if s[
        "ema_near"
    ]:

        confirmations.append(
            "4H_EMA_PROXIMITY"
        )

    if s[
        "ema_reclaim"
    ]:

        confirmations.append(
            "4H_EMA_RECLAIM"
        )

    if s[
        "bullish_ema_structure"
    ]:

        confirmations.append(
            "BULLISH_EMA_STRUCTURE"
        )

    if (
        s["relative_volume"]
        >= 1.20
    ):

        confirmations.append(
            "RELATIVE_VOLUME"
        )

    if (
        s["volume_acceleration"]
        >= 1.20
    ):

        confirmations.append(
            "VOLUME_ACCELERATION"
        )

    if s[
        "compression"
    ]:

        confirmations.append(
            "COMPRESSION"
        )

    if s[
        "near_breakout"
    ]:

        confirmations.append(
            "NEAR_BREAKOUT"
        )

    if s[
        "breakout"
    ]:

        confirmations.append(
            "BREAKOUT"
        )

    # ============================================================
    # MICRO
    # ============================================================

    if m.get(
        "ready"
    ):

        confirmations.extend(
            m[
                "confirmations"
            ]
        )

    # ============================================================
    # HARD V10 CONDITIONS
    # ============================================================

    anti_chase = s[
        "anti_chase"
    ]

    micro_ready = m.get(
        "ready",
        False
    )

    micro_confirms = (
        m.get(
            "micro_score",
            0
        )
    )

    cvd_ok = (
        "CVD_BUY_DOMINANCE"
        in confirmations
    )

    ofi_ok = (
        "POSITIVE_OFI"
        in confirmations
    )

    ofi_persistent = (
        "OFI_PERSISTENCE"
        in confirmations
    )

    volume_ok = (
        "VOLUME_ACCELERATION"
        in confirmations
        or
        "RELATIVE_VOLUME"
        in confirmations
    )

    structure_ok = (
        s["ema_near"]
        or
        s["ema_reclaim"]
        or
        s["near_breakout"]
        or
        s["breakout"]
    )

    # ============================================================
    # STATE
    # ============================================================

    if anti_chase:

        state = "REJECT"

        reason = (
            "ANTI_CHASE"
        )

    elif not micro_ready:

        state = "WATCH"

        reason = (
            "MICRO_WARMING_UP"
        )

    elif (
        cvd_ok
        and
        ofi_ok
        and
        ofi_persistent
        and
        volume_ok
        and
        structure_ok
        and
        micro_confirms >= 4
        and
        len(confirmations)
        >= BUY_MIN_CONFIRMATIONS
    ):

        state = "BUY"

        reason = (
            "V10_FULL_CONFIRMATION"
        )

    elif (
        structure_ok
        and
        micro_confirms >= 3
        and
        len(confirmations)
        >= PRE_MIN_CONFIRMATIONS
    ):

        state = "PRE-IGNITION"

        reason = (
            "BUILDING_CONFLUENCE"
        )

    else:

        state = "WATCH"

        reason = (
            "INSUFFICIENT_CONFLUENCE"
        )

    # ============================================================
    # SCORE
    # ============================================================

    score = 0.0

    # High weight:
    # OFI/CVD/volume
    if cvd_ok:
        score += 18

    if ofi_ok:
        score += 18

    if ofi_persistent:
        score += 12

    if (
        "L1_L10_BID_IMBALANCE"
        in confirmations
    ):
        score += 10

    if (
        "ASK_LIQUIDITY_DEPLETION"
        in confirmations
    ):
        score += 10

    if (
        "VOLUME_ACCELERATION"
        in confirmations
    ):
        score += 10

    if (
        "RELATIVE_VOLUME"
        in confirmations
    ):
        score += 5

    if s["ema_near"]:
        score += 5

    if s["ema_reclaim"]:
        score += 8

    if s["compression"]:
        score += 5

    if s["near_breakout"]:
        score += 5

    if s["breakout"]:
        score += 8

    if (
        "TRADE_ACCELERATION"
        in confirmations
    ):
        score += 6

    if anti_chase:
        score -= 40

    score = max(
        0.0,
        min(
            100.0,
            score
        )
    )

    return {

        "symbol":
            symbol,

        "state":
            state,

        "reason":
            reason,

        "score":
            round(
                score,
                1
            ),

        "price":
            s["price"],

        "nearest_ema":
            s[
                "nearest_ema"
            ],

        "ema_distance_pct":
            round(
                s[
                    "nearest_ema_distance_pct"
                ],
                3
            ),

        "ema_atr_distance":
            round(
                s[
                    "nearest_ema_atr"
                ],
                3
            ),

        "relative_volume":
            round(
                s[
                    "relative_volume"
                ],
                2
            ),

        "volume_acceleration":
            round(
                s[
                    "volume_acceleration"
                ],
                2
            ),

        "compression_ratio":
            round(
                s[
                    "compression_ratio"
                ],
                3
            ),

        "breakout_distance_pct":
            round(
                s[
                    "breakout_distance_pct"
                ],
                3
            ),

        "anti_chase":
            anti_chase,

        "cvd_quote":
            round(
                m.get(
                    "cvd_quote",
                    0
                ),
                2
            ),

        "cvd_ratio":
            round(
                m.get(
                    "cvd_ratio",
                    0
                ),
                4
            ),

        "ofi":
            round(
                m.get(
                    "recent_ofi",
                    0
                ),
                4
            ),

        "ofi_persistence":
            round(
                m.get(
                    "ofi_persistence",
                    0
                ),
                3
            ),

        "obi":
            round(
                m.get(
                    "recent_obi",
                    0
                ),
                4
            ),

        "ask_depletion_pct":
            round(
                m.get(
                    "ask_depletion",
                    0
                )
                * 100,
                2
            ),

        "bid_depletion_pct":
            round(
                m.get(
                    "bid_depletion",
                    0
                )
                * 100,
                2
            ),

        "trade_acceleration":
            round(
                m.get(
                    "trade_acceleration",
                    0
                ),
                2
            ),

        "confirmations":
            confirmations
    }


# ================================================================
# RANK
# ================================================================

STATE_PRIORITY = {
    "BUY": 4,
    "PRE-IGNITION": 3,
    "WATCH": 2,
    "REJECT": 1
}


def get_ranked_results():

    output = []

    for symbol in live_symbols:

        result = evaluate_symbol(
            symbol
        )

        if result:
            output.append(
                result
            )

    output.sort(
        key=lambda x: (
            STATE_PRIORITY.get(
                x["state"],
                0
            ),
            x["score"],
            -abs(
                x[
                    "ema_distance_pct"
                ]
            )
        ),
        reverse=True
    )

    return output[
        :TOP_RESULTS
    ]


# ================================================================
# INITIAL STRUCTURAL PREFILTER
# ================================================================

async def structure_batch(
    symbols
):

    semaphore = asyncio.Semaphore(
        8
    )

    async def worker(
        symbol
    ):

        async with semaphore:

            result = (
                await load_4h_structure(
                    symbol
                )
            )

            await asyncio.sleep(
                0.03
            )

            return result

    results = await asyncio.gather(
        *[
            worker(s)
            for s in symbols
        ],
        return_exceptions=True
    )

    valid = [
        x
        for x in results
        if isinstance(
            x,
            dict
        )
    ]

    # --------------------------------
    # Candidate pre-ranking
    # --------------------------------

    def candidate_score(x):

        score = 0

        # Near EMA
        score += max(
            0,
            20
            - (
                x[
                    "nearest_ema_distance_pct"
                ]
                * 5
            )
        )

        # Volume
        score += min(
            x[
                "volume_acceleration"
            ]
            * 10,
            20
        )

        # Compression
        if x[
            "compression"
        ]:
            score += 15

        # Near breakout
        if x[
            "near_breakout"
        ]:
            score += 15

        # EMA reclaim
        if x[
            "ema_reclaim"
        ]:
            score += 15

        # Penalise chase
        if x[
            "anti_chase"
        ]:
            score -= 50

        return score

    valid.sort(
        key=candidate_score,
        reverse=True
    )

    return [
        x["symbol"]
        for x in valid[
            :MAX_LIVE_SYMBOLS
        ]
    ]


# ================================================================
# WEBSOCKET
# ================================================================

async def websocket_loop():

    global live_symbols

    backoff = 1

    while True:

        if not live_symbols:

            await asyncio.sleep(
                1
            )

            continue

        try:

            print(
                f"[WS] connecting "
                f"{len(live_symbols)} symbols"
            )

            async with session.ws_connect(
                WS_BASE,
                heartbeat=30,
                autoping=True,
                receive_timeout=90
            ) as ws:

                streams = []

                for symbol in live_symbols:

                    lower = (
                        symbol.lower()
                    )

                    streams.append(
                        f"{lower}@aggTrade"
                    )

                    streams.append(
                        f"{lower}@depth@100ms"
                    )

                await ws.send_json(
                    {
                        "method":
                            "SUBSCRIBE",

                        "params":
                            streams,

                        "id":
                            1
                    }
                )

                print(
                    f"[WS] subscribed to "
                    f"{len(streams)} streams"
                )

                # Bootstrap books after
                # socket begins receiving.
                bootstrap_tasks = [
                    asyncio.create_task(
                        resync_book(
                            symbol
                        )
                    )
                    for symbol
                    in live_symbols
                ]

                # Don't block websocket
                # processing waiting for
                # all snapshots.
                asyncio.gather(
                    *bootstrap_tasks,
                    return_exceptions=True
                )

                backoff = 1

                async for msg in ws:

                    if (
                        msg.type
                        == aiohttp.WSMsgType.TEXT
                    ):

                        try:

                            payload = (
                                json.loads(
                                    msg.data
                                )
                            )

                        except Exception:
                            continue

                        # Subscription ACK
                        if (
                            "result"
                            in payload
                            and "id"
                            in payload
                        ):
                            continue

                        event_type = (
                            payload.get(
                                "e"
                            )
                        )

                        symbol = (
                            payload.get(
                                "s"
                            )
                        )

                        if (
                            not symbol
                            or symbol
                            not in live_symbols
                        ):
                            continue

                        if (
                            event_type
                            == "aggTrade"
                        ):

                            process_agg_trade(
                                symbol,
                                payload
                            )

                        elif (
                            event_type
                            == "depthUpdate"
                        ):

                            asyncio.create_task(
                                process_depth(
                                    symbol,
                                    payload
                                )
                            )

                    elif (
                        msg.type
                        in (
                            aiohttp.WSMsgType.ERROR,
                            aiohttp.WSMsgType.CLOSED
                        )
                    ):

                        break

        except asyncio.CancelledError:
            raise

        except Exception as e:

            print(
                f"[WS ERROR] {e}"
            )

        print(
            f"[WS] reconnecting "
            f"in {backoff}s"
        )

        await asyncio.sleep(
            backoff
        )

        backoff = min(
            backoff * 2,
            30
        )


# ================================================================
# STRUCTURE REFRESH LOOP
# ================================================================

async def structure_refresh_loop():

    while True:

        try:

            symbols = list(
                live_symbols
            )

            if symbols:

                semaphore = (
                    asyncio.Semaphore(
                        5
                    )
                )

                async def worker(
                    symbol
                ):

                    async with semaphore:

                        await load_4h_structure(
                            symbol
                        )

                        await asyncio.sleep(
                            0.05
                        )

                await asyncio.gather(
                    *[
                        worker(s)
                        for s
                        in symbols
                    ],
                    return_exceptions=True
                )

        except Exception as e:

            print(
                "[STRUCTURE LOOP]",
                e
            )

        await asyncio.sleep(
            STRUCTURE_REFRESH_SEC
        )


# ================================================================
# DISPLAY
# ================================================================

def fmt_num(
    value
):

    if abs(value) >= 1_000_000:

        return (
            f"{value / 1_000_000:.2f}M"
        )

    if abs(value) >= 1_000:

        return (
            f"{value / 1_000:.2f}K"
        )

    return f"{value:.2f}"


async def print_loop():

    while True:

        await asyncio.sleep(
            PRINT_INTERVAL_SEC
        )

        results = (
            get_ranked_results()
        )

        print("\n")
        print(
            "=" * 100
        )

        print(
            "Ψ-V10 LIVE BINANCE SCAN"
        )

        print(
            time.strftime(
                "%Y-%m-%d %H:%M:%S UTC",
                time.gmtime()
            )
        )

        print(
            "=" * 100
        )

        for i, r in enumerate(
            results,
            1
        ):

            print(
                f"\n#{i} "
                f"{r['symbol']} "
                f"| {r['state']} "
                f"| SCORE {r['score']}"
            )

            print(
                f"Price: "
                f"{r['price']}"
            )

            print(
                f"EMA: "
                f"{r['nearest_ema']} "
                f"| distance "
                f"{r['ema_distance_pct']}%"
            )

            print(
                f"RVOL: "
                f"{r['relative_volume']} "
                f"| VolAccel: "
                f"{r['volume_acceleration']}"
            )

            print(
                f"Breakout distance: "
                f"{r['breakout_distance_pct']}%"
            )

            print(
                f"CVD: "
                f"{fmt_num(r['cvd_quote'])} "
                f"| CVD ratio: "
                f"{r['cvd_ratio']}"
            )

            print(
                f"OFI: "
                f"{r['ofi']} "
                f"| persistence: "
                f"{r['ofi_persistence']}"
            )

            print(
                f"OBI L1-L10: "
                f"{r['obi']}"
            )

            print(
                f"Ask depletion: "
                f"{r['ask_depletion_pct']}% "
                f"| Bid depletion: "
                f"{r['bid_depletion_pct']}%"
            )

            print(
                f"Trade acceleration: "
                f"{r['trade_acceleration']}x"
            )

            print(
                "Confirmations: "
                + ", ".join(
                    r[
                        "confirmations"
                    ]
                )
            )

        print("\n")


# ================================================================
# SIMPLE HTTP SERVER
# ================================================================
#
# Railway can ping:
#
# /
# /scan
#
# ================================================================

async def health(
    request
):

    return aiohttp.web.json_response(
        {
            "status":
                "ok",

            "scanner":
                "Psi-V10",

            "symbols":
                len(
                    live_symbols
                ),

            "timestamp":
                time.time()
        }
    )


async def scan_endpoint(
    request
):

    return aiohttp.web.json_response(
        {
            "scanner":
                "Psi-V10",

            "timestamp":
                time.time(),

            "results":
                get_ranked_results()
        }
    )


async def start_http_server():

    from aiohttp import web

    app = web.Application()

    app.router.add_get(
        "/",
        health
    )

    app.router.add_get(
        "/scan",
        scan_endpoint
    )

    runner = web.AppRunner(
        app
    )

    await runner.setup()

    port = int(
        os.getenv(
            "PORT",
            "8080"
        )
    )

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

    print(
        f"[HTTP] listening "
        f"on port {port}"
    )


# ================================================================
# INITIALISE
# ================================================================

async def initialise():

    global live_symbols

    print(
        "Ψ-V10 INITIALISING..."
    )

    universe = (
        await get_exchange_symbols()
    )

    print(
        f"[UNIVERSE] "
        f"{len(universe)} "
        f"eligible USDT pairs"
    )

    # Limit initial historical requests
    # to most liquid symbols.
    #
    # We still rank by actual V10
    # structure afterwards.

    initial_pool = universe[
        :120
    ]

    print(
        f"[STRUCTURE] scanning "
        f"{len(initial_pool)} pairs"
    )

    selected = (
        await structure_batch(
            initial_pool
        )
    )

    live_symbols = selected

    print(
        "[LIVE CANDIDATES]"
    )

    for symbol in live_symbols:

        print(
            "  ",
            symbol
        )


# ================================================================
# MAIN
# ================================================================

async def main():

    global session

    connector = (
        aiohttp.TCPConnector(
            limit=100,
            ttl_dns_cache=300
        )
    )

    session = aiohttp.ClientSession(
        connector=connector,
        headers={
            "User-Agent":
                "Psi-V10-Binance-Scanner"
        }
    )

    try:

        await initialise()

        await start_http_server()

        tasks = [

            asyncio.create_task(
                websocket_loop()
            ),

            asyncio.create_task(
                structure_refresh_loop()
            ),

            asyncio.create_task(
                print_loop()
            )
        ]

        await asyncio.gather(
            *tasks
        )

    finally:

        if session:

            await session.close()


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        print(
            "\nΨ-V10 stopped."
        )
