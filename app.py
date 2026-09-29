# =============================================================================
# Î¨-V10 BINANCE LIVE SCANNER
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

PRE_MIN_CONFIRMATIONS = int(
    os.getenv("PRE_MIN_CONFIRMATIONS", "4")
)

PRE_MIN_MICRO_CONFIRMATIONS = int(
    os.getenv("PRE_MIN_MICRO_CONFIRMATIONS", "3")
)

ANOMALY_REFRESH_SECONDS = int(os.getenv("ANOMALY_REFRESH_SECONDS", "30"))
ANOMALY_CANDLE_LIMIT = int(os.getenv("ANOMALY_CANDLE_LIMIT", "32"))
ANOMALY_PROMOTION_SLOTS = int(os.getenv("ANOMALY_PROMOTION_SLOTS", "40"))
MISSED_MOVE_15M_PCT = float(os.getenv("MISSED_MOVE_15M_PCT", "10.0"))

TOP_BOOK_LEVELS = 10

TRADE_WINDOW_SECONDS = 120

OFI_WINDOW_SECONDS = 120

MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "20"))
MAX_SLIPPAGE_BPS = float(os.getenv("MAX_SLIPPAGE_BPS", "35"))
SLIPPAGE_TEST_NOTIONAL = float(os.getenv("SLIPPAGE_TEST_NOTIONAL", "1000"))

USER_AGENT = "psi-v10-live-scanner/6.0"


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

anomaly_state: Dict[str, dict] = {}
missed_moves: deque = deque(maxlen=500)
last_anomaly_refresh = 0.0


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



def sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    return average(values[-period:])


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



async def load_klines(client: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> Optional[List[list]]:
    try:
        rows = await api_get(client, "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
    except Exception:
        return None
    return rows if isinstance(rows, list) else None


def ma_snapshot(rows: List[list]) -> Optional[dict]:
    if not rows or len(rows) < 205:
        return None
    closed = rows[:-1]
    closes = [safe_float(r[4]) for r in closed]
    lows = [safe_float(r[3]) for r in closed]
    current = safe_float(rows[-1][4])
    a = atr(closed, 14)
    if current <= 0 or not a:
        return None
    e50, e200 = ema(closes, 50), ema(closes, 200)
    s50, s200 = sma(closes, 50), sma(closes, 200)
    if not all((e50, e200, s50, s200)):
        return None
    previous, last = closes[-2], closes[-1]
    values = {"ema50": e50, "ema200": e200, "sma50": s50, "sma200": s200}
    near = {k: abs(current-v)/a <= EMA_NEAR_ATR for k,v in values.items()}
    touch = {k: abs(current-v)/a <= EMA_TOUCH_ATR for k,v in values.items()}
    reclaim = {k: previous <= v and last > v for k,v in values.items()}
    rejection = {k: lows[-1] <= v and last > v for k,v in values.items()}
    bullish = current > e50 > e200 and current > s50 > s200
    support = any(touch.values()) or any(reclaim.values()) or any(rejection.values())
    return {
        "price": current, "atr14": a, **values, "near": near, "touch": touch,
        "reclaim": reclaim, "rejection": rejection, "bullish_stack": bullish,
        "structural_support": support,
    }


async def load_fast_anomaly(client: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    rows = await load_klines(client, symbol, "1m", ANOMALY_CANDLE_LIMIT)
    if not rows or len(rows) < 22:
        return None
    closed = rows[:-1]
    closes = [safe_float(r[4]) for r in closed]
    qvol = [safe_float(r[7]) for r in closed]
    trades = [safe_float(r[8]) for r in closed]
    p = closes[-1]
    if p <= 0:
        return None
    def ret(n):
        return (p / closes[-1-n] - 1.0) * 100.0 if len(closes) > n and closes[-1-n] > 0 else 0.0
    r1, r3, r5, r15 = ret(1), ret(3), ret(5), ret(15)
    rv = safe_div(qvol[-1], average(qvol[-21:-1]), 0.0)
    ta = safe_div(trades[-1], average(trades[-21:-1]), 0.0)
    highs = [safe_float(r[2]) for r in closed[-20:]]
    lows = [safe_float(r[3]) for r in closed[-20:]]
    compression_pct = safe_div(max(highs)-min(lows), p) * 100.0
    score = max(0,r1)*3.0 + max(0,r3)*1.7 + max(0,r5) + max(0,r15)*0.35
    score += min(rv,8.0)*4.0 + min(ta,8.0)*3.0 + (6.0 if compression_pct < 3.0 else 0.0)
    return {
        "symbol": symbol, "price": p, "return_1m_pct": r1, "return_3m_pct": r3,
        "return_5m_pct": r5, "return_15m_pct": r15, "relative_volume_1m": rv,
        "trade_count_acceleration_1m": ta, "compression_20m_pct": compression_pct,
        "anomaly_score": score, "updated_ms": now_ms(),
    }


# =============================================================================
# 4H STRUCTURE
# =============================================================================

async def load_4h_structure(client: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    rows1, rows4 = await asyncio.gather(
        load_klines(client, symbol, "1h", 260),
        load_klines(client, symbol, "4h", 260),
    )
    m1, m4 = ma_snapshot(rows1 or []), ma_snapshot(rows4 or [])
    if not m1 or not m4 or not rows4:
        return None
    closed = rows4[:-1]
    highs = [safe_float(r[2]) for r in closed]
    lows = [safe_float(r[3]) for r in closed]
    volumes = [safe_float(r[5]) for r in closed]
    current_price = safe_float(rows4[-1][4])
    avg20_volume = average(volumes[-20:])
    volume_acceleration = safe_div(volumes[-1], avg20_volume)
    recent_ranges = [highs[i]-lows[i] for i in range(max(0,len(highs)-6),len(highs))]
    baseline_ranges = [highs[i]-lows[i] for i in range(max(0,len(highs)-26),max(0,len(highs)-6))]
    compression_ratio = safe_div(average(recent_ranges), average(baseline_ranges), 1.0)
    compression = compression_ratio <= 0.80
    resistance_window = highs[-21:-1]
    resistance = max(resistance_window) if resistance_window else highs[-1]
    breakout_distance_pct = safe_div(resistance-current_price,current_price)*100.0
    breakout_near = -0.5 <= breakout_distance_pct <= BREAKOUT_NEAR_PCT
    breakout = current_price > resistance

    # Full required MA harmony: 1H + 4H, EMA50/200 + SMA50/200.
    ma_harmony = m1["bullish_stack"] and m4["bullish_stack"]
    ma_support = m1["structural_support"] or m4["structural_support"]
    near_any = any(m1["near"].values()) or any(m4["near"].values())
    dist50 = safe_div(current_price-m4["ema50"],m4["atr14"])
    dist200 = safe_div(current_price-m4["ema200"],m4["atr14"])
    anti_chase = (dist50 > ANTI_CHASE_ATR and dist200 > ANTI_CHASE_ATR)

    confirmations = []
    for tf, snap in (("1H",m1),("4H",m4)):
        for k,v in snap["touch"].items():
            if v: confirmations.append(f"{tf}_{k.upper()}_TOUCH")
        for k,v in snap["reclaim"].items():
            if v: confirmations.append(f"{tf}_{k.upper()}_RECLAIM")
        for k,v in snap["rejection"].items():
            if v: confirmations.append(f"{tf}_{k.upper()}_REJECTION")
        if snap["bullish_stack"]: confirmations.append(f"{tf}_EMA_SMA_STACK")
    if volume_acceleration >= 1.20: confirmations.append("4H_VOLUME_ACCELERATION")
    if compression: confirmations.append("RANGE_COMPRESSION")
    if breakout_near: confirmations.append("NEAR_RESISTANCE")
    if breakout: confirmations.append("BREAKOUT")

    return {
        "symbol": symbol, "price": current_price,
        "ema50_1h": m1["ema50"], "ema200_1h": m1["ema200"], "sma50_1h": m1["sma50"], "sma200_1h": m1["sma200"],
        "ema50_4h": m4["ema50"], "ema200_4h": m4["ema200"], "sma50_4h": m4["sma50"], "sma200_4h": m4["sma200"],
        "atr14_1h": m1["atr14"], "atr14_4h": m4["atr14"],
        "distance_ema50_atr": dist50, "distance_ema200_atr": dist200,
        "ema50_near": m4["near"]["ema50"], "ema200_near": m4["near"]["ema200"],
        "ma_1h": m1, "ma_4h": m4, "ma_harmony": ma_harmony,
        "structural_support": ma_support, "volume_acceleration": volume_acceleration,
        "compression_ratio": compression_ratio, "compression": compression,
        "resistance": resistance, "breakout_distance_pct": breakout_distance_pct,
        "breakout_near": breakout_near, "breakout": breakout, "anti_chase": anti_chase,
        "structure_confirmations": confirmations,
        "quote_volume_24h": symbol_meta.get(symbol,{}).get("quote_volume_24h",0.0),
        "updated_ms": now_ms(),
    }


# =============================================================================
# MICROSTRUCTURE STATE / ORDER BOOK / LIVE FLOW
# =============================================================================

def ensure_micro_state(symbol: str) -> dict:
    state = micro_state[symbol]
    if "trades" not in state:
        # trade tuple: (timestamp_ms, signed_quote, quote_value, price, quantity)
        state["trades"] = deque(maxlen=30000)
        for key in ("ofi", "obi", "ask_depletion", "bid_depletion", "spread_bps", "slippage_bps"):
            state[key] = deque(maxlen=10000)
        state["previous_bids"] = None
        state["previous_asks"] = None
        state["book_updates"] = 0
        state["last_trade_ms"] = 0
        state["last_book_ms"] = 0
        state["last_agg_id"] = None
        state["trade_sequence_ok"] = True
        state["trade_sequence_samples"] = 0
        state["last_book_update_id"] = None
        state["book_sequence_ok"] = True
        state["book_sequence_samples"] = 0
    return state


def depth_notional(levels) -> float:
    return sum(p * q for p, q in levels[:TOP_BOOK_LEVELS])


def level_map(levels) -> Dict[float, float]:
    return {p: q for p, q in levels[:TOP_BOOK_LEVELS]}


def calculate_obi(bids, asks) -> float:
    bv, av = depth_notional(bids), depth_notional(asks)
    return safe_div(bv - av, bv + av)


def calculate_ofi(previous_bids, previous_asks, bids, asks) -> float:
    pb, pa, cb, ca = level_map(previous_bids), level_map(previous_asks), level_map(bids), level_map(asks)
    bid_change = sum(p * (cb.get(p, 0.0) - pb.get(p, 0.0)) for p in set(pb) | set(cb))
    ask_change = sum(p * (ca.get(p, 0.0) - pa.get(p, 0.0)) for p in set(pa) | set(ca))
    return safe_div(bid_change - ask_change, abs(bid_change) + abs(ask_change))


def depletion(previous, current) -> float:
    pv = depth_notional(previous)
    return safe_div(pv - depth_notional(current), pv)


def estimate_buy_slippage_bps(asks, notional: float) -> Optional[float]:
    if not asks or notional <= 0:
        return None
    remaining, base_bought, spent = notional, 0.0, 0.0
    best = asks[0][0]
    for price, qty in asks[:TOP_BOOK_LEVELS]:
        level_quote = price * qty
        take = min(remaining, level_quote)
        if take > 0:
            spent += take
            base_bought += take / price
            remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or base_bought <= 0 or best <= 0:
        return None
    avg_price = spent / base_bought
    return (avg_price / best - 1.0) * 10000.0


def process_agg_trade(symbol: str, data: dict) -> None:
    state = ensure_micro_state(symbol)
    timestamp = int(data.get("T") or data.get("E") or now_ms())
    price, quantity = safe_float(data.get("p")), safe_float(data.get("q"))
    if price <= 0 or quantity <= 0:
        return
    quote_value = price * quantity
    signed_quote = -quote_value if bool(data.get("m", False)) else quote_value
    agg_id = data.get("a")
    if agg_id is not None:
        try:
            agg_id = int(agg_id)
            last_id = state.get("last_agg_id")
            if last_id is not None:
                state["trade_sequence_samples"] += 1
                if agg_id <= last_id:
                    state["trade_sequence_ok"] = False
            state["last_agg_id"] = agg_id
        except (TypeError, ValueError):
            state["trade_sequence_ok"] = False
    state["trades"].append((timestamp, signed_quote, quote_value, price, quantity))
    state["last_trade_ms"] = timestamp
    prune_deque(state["trades"], now_ms() - 180_000)


def parse_levels(raw):
    return [(p, q) for p, q in ((safe_float(r[0]), safe_float(r[1])) for r in raw if len(r) >= 2) if p > 0 and q > 0][:20]


def process_partial_depth(symbol: str, data: dict) -> None:
    state = ensure_micro_state(symbol)
    bids, asks = parse_levels(data.get("bids", [])), parse_levels(data.get("asks", []))
    if not bids or not asks:
        return
    timestamp = now_ms()
    update_id = data.get("lastUpdateId")
    if update_id is not None:
        try:
            update_id = int(update_id)
            last_update_id = state.get("last_book_update_id")
            if last_update_id is not None:
                state["book_sequence_samples"] += 1
                if update_id <= last_update_id:
                    state["book_sequence_ok"] = False
            state["last_book_update_id"] = update_id
        except (TypeError, ValueError):
            state["book_sequence_ok"] = False
    obi = calculate_obi(bids, asks)
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2.0
    spread_bps = safe_div(best_ask - best_bid, mid) * 10000.0
    slippage = estimate_buy_slippage_bps(asks, SLIPPAGE_TEST_NOTIONAL)
    state["obi"].append((timestamp, obi))
    state["spread_bps"].append((timestamp, spread_bps))
    if slippage is not None:
        state["slippage_bps"].append((timestamp, slippage))
    pb, pa = state.get("previous_bids"), state.get("previous_asks")
    if pb and pa:
        state["ofi"].append((timestamp, calculate_ofi(pb, pa, bids, asks)))
        state["ask_depletion"].append((timestamp, depletion(pa, asks)))
        state["bid_depletion"].append((timestamp, depletion(pb, bids)))
    state["previous_bids"], state["previous_asks"] = bids, asks
    state["book_updates"] += 1
    state["last_book_ms"] = timestamp
    cutoff = timestamp - OFI_WINDOW_SECONDS * 1000
    for key in ("ofi", "obi", "ask_depletion", "bid_depletion", "spread_bps", "slippage_bps"):
        prune_deque(state[key], cutoff)


def _window(rows, now, lo, hi=0):
    lower, upper = now - lo * 1000, now - hi * 1000
    return [r for r in rows if lower <= r[0] < upper]


def micro_metrics(symbol: str) -> dict:
    state, current_time = ensure_micro_state(symbol), now_ms()
    trades = state["trades"]
    prune_deque(trades, current_time - 180_000)
    recent = _window(trades, current_time, 60)
    first30, last30 = _window(trades, current_time, 60, 30), _window(trades, current_time, 30)
    prev60 = _window(trades, current_time, 120, 60)
    last10, prev10 = _window(trades, current_time, 10), _window(trades, current_time, 20, 10)

    cvd60 = sum(r[1] for r in recent)
    cvd_first, cvd_last = sum(r[1] for r in first30), sum(r[1] for r in last30)
    cvd_acceleration = cvd_last - cvd_first
    total60 = sum(r[2] for r in recent)
    buy_ratio = safe_div(sum(r[2] for r in recent if r[1] > 0), total60, 0.5)
    trade_acceleration = safe_div(len(last30), max(len(first30), 1))
    avg_trade = safe_div(total60, len(recent))
    avg_first = safe_div(sum(r[2] for r in first30), len(first30))
    avg_last = safe_div(sum(r[2] for r in last30), len(last30))
    trade_size_shift = safe_div(avg_last, max(avg_first, 1e-9))
    qv10, qvprev10 = sum(r[2] for r in last10), sum(r[2] for r in prev10)
    qv30, qvprev60 = sum(r[2] for r in last30), sum(r[2] for r in prev60)
    rel_volume_10s = safe_div(qv10, max(qvprev10, 1e-9))
    rel_volume_30s = safe_div(qv30, max(qvprev60 / 2.0, 1e-9))

    vwap = safe_div(sum(r[3] * r[4] for r in recent), sum(r[4] for r in recent))
    last_price = recent[-1][3] if recent else 0.0
    prev_vwap = safe_div(sum(r[3] * r[4] for r in first30), sum(r[4] for r in first30))
    vwap_reclaim = bool(vwap and last_price >= vwap and (not prev_vwap or (first30 and first30[-1][3] <= prev_vwap) or cvd_acceleration > 0))
    vwap_deviation_bps = safe_div(last_price - vwap, vwap) * 10000.0 if vwap else 0.0

    def vals(key, seconds=60):
        return [r[1] for r in state[key] if r[0] >= current_time - seconds * 1000]
    ofis, obis = vals("ofi"), vals("obi")
    asks, bids = vals("ask_depletion"), vals("bid_depletion")
    spreads, slips = vals("spread_bps"), vals("slippage_bps")
    ofi = average(ofis[-20:]); obi = average(obis[-20:])
    ask_dep = average(asks[-20:]); bid_dep = average(bids[-20:])
    half = max(1, len(ofis) // 2)
    ofi_acceleration = average(ofis[half:]) - average(ofis[:half]) if len(ofis) >= 6 else 0.0
    ofi_persistence = safe_div(sum(1 for x in ofis[-20:] if x > 0), len(ofis[-20:]))
    flow_persistence = safe_div(sum(1 for r in last30 if r[1] > 0), len(last30))
    spread_bps = spreads[-1] if spreads else None
    slippage_bps = slips[-1] if slips else None

    trade_fresh = state["last_trade_ms"] >= current_time - 15_000
    book_fresh = state["last_book_ms"] >= current_time - 5_000
    sequence_verified = state["trade_sequence_ok"] and state["trade_sequence_samples"] >= 3
    book_sequence_verified = state["book_sequence_ok"] and state["book_sequence_samples"] >= 3
    micro_ready = trade_fresh and book_fresh and len(recent) >= 10 and len(ofis) >= 6 and state["book_updates"] >= 8

    return {
        "micro_ready": micro_ready, "trade_fresh": trade_fresh, "book_fresh": book_fresh,
        "sequence_verified": sequence_verified, "book_sequence_verified": book_sequence_verified, "cvd_quote_60s": cvd60,
        "cvd_acceleration": cvd_acceleration, "aggressive_buy_ratio": buy_ratio,
        "trade_count_60s": len(recent), "trade_acceleration": trade_acceleration,
        "avg_trade_size_quote": avg_trade, "trade_size_shift": trade_size_shift,
        "relative_volume_10s": rel_volume_10s, "relative_volume_30s": rel_volume_30s,
        "vwap_60s": vwap, "vwap_reclaim": vwap_reclaim, "vwap_deviation_bps": vwap_deviation_bps,
        "ofi": ofi, "ofi_acceleration": ofi_acceleration, "ofi_persistence": ofi_persistence,
        "flow_persistence": flow_persistence, "obi": obi, "ask_depletion": ask_dep,
        "bid_depletion": bid_dep, "spread_bps": spread_bps, "slippage_bps": slippage_bps,
        "last_trade_ms": state["last_trade_ms"], "last_book_ms": state["last_book_ms"],
    }


# =============================================================================
# SIGNAL ENGINE â€” STRICT ALL-MANDATORY BUY
# =============================================================================

def evaluate_symbol(symbol: str) -> Optional[dict]:
    sd = structure.get(symbol)
    if not sd:
        return None
    m = micro_metrics(symbol)

    # A trigger is valid either while compressed immediately under resistance,
    # or after the breakout has actually fired. This avoids requiring mutually
    # exclusive pre-breakout and post-breakout states at the same instant.
    trigger_state = (sd["compression"] and sd["breakout_near"]) or sd["breakout"]
    ema_layer = sd["ma_harmony"] and (sd["structural_support"] or sd["ema50_near"] or sd["ema200_near"])

    mandatory = {
        "LIVE_MICRO_DATA": m["micro_ready"],
        "TRADE_SEQUENCE_VALID": m["sequence_verified"],
        "BOOK_UPDATE_SEQUENCE_VALID": m["book_sequence_verified"],
        "EMA_SMA_1H_4H_HARMONY": ema_layer,
        "FAST_ANOMALY_DISCOVERY": anomaly_state.get(symbol, {}).get("fast_trigger", False),
        "STRUCTURE_TRIGGER": trigger_state,
        "4H_VOLUME_ACCELERATION": sd["volume_acceleration"] >= 1.20,
        "MULTI_WINDOW_RELATIVE_VOLUME": m["relative_volume_10s"] >= 1.05 and m["relative_volume_30s"] >= 1.05,
        "POSITIVE_CVD": m["cvd_quote_60s"] > 0,
        "CVD_ACCELERATION": m["cvd_acceleration"] > 0,
        "AGGRESSIVE_BUY_DOMINANCE": m["aggressive_buy_ratio"] >= 0.55,
        "POSITIVE_OFI": m["ofi"] > 0.05,
        "OFI_ACCELERATION": m["ofi_acceleration"] > 0,
        "OFI_PERSISTENCE": m["ofi_persistence"] >= 0.60,
        "BID_DEPTH_IMBALANCE": m["obi"] >= 0.10,
        "ASK_LIQUIDITY_DEPLETION": m["ask_depletion"] > 0.03,
        "TRADE_COUNT_ACCELERATION": m["trade_acceleration"] >= 1.20,
        "TRADE_SIZE_SHIFT": m["trade_size_shift"] >= 1.05,
        "FLOW_PERSISTENCE": m["flow_persistence"] >= 0.55,
        "VWAP_RECLAIM": m["vwap_reclaim"],
        "SPREAD_FILTER": m["spread_bps"] is not None and m["spread_bps"] <= MAX_SPREAD_BPS,
        "SLIPPAGE_FILTER": m["slippage_bps"] is not None and m["slippage_bps"] <= MAX_SLIPPAGE_BPS,
        "ANTI_CHASE_CLEAR": not sd["anti_chase"],
    }
    status = {k: ("PASS" if v else "FAIL") for k, v in mandatory.items()}
    failed = [k for k, v in mandatory.items() if not v]
    buy = all(mandatory.values())
    pass_count = sum(mandatory.values())
    pass_ratio = safe_div(pass_count, len(mandatory))

    if buy:
        state = "BUY NOW"
    elif m["micro_ready"] and not sd["anti_chase"] and pass_ratio >= 0.70:
        state = "PRE-IGNITION"
    elif sd["ema50_near"] or sd["ema200_near"] or sd["breakout_near"]:
        state = "WATCH"
    else:
        state = "REJECT"

    confirmations = list(sd["structure_confirmations"]) + [k for k, v in mandatory.items() if v]
    score = pass_ratio * 100.0
    score += min(max(sd["volume_acceleration"] - 1.0, 0.0) * 5.0, 10.0)
    score -= 20.0 if sd["anti_chase"] else 0.0

    return {
        "symbol": symbol, "state": state, "score": round(score, 2), "price": sd["price"],
        "ema50_1h": sd["ema50_1h"], "ema200_1h": sd["ema200_1h"], "sma50_1h": sd["sma50_1h"], "sma200_1h": sd["sma200_1h"],
        "ema50_4h": sd["ema50_4h"], "ema200_4h": sd["ema200_4h"], "sma50_4h": sd["sma50_4h"], "sma200_4h": sd["sma200_4h"],
        "atr14_1h": sd["atr14_1h"], "atr14_4h": sd["atr14_4h"], "ma_harmony": sd["ma_harmony"],
        "fast_anomaly": anomaly_state.get(symbol, {}),
        "distance_ema50_atr": sd["distance_ema50_atr"], "distance_ema200_atr": sd["distance_ema200_atr"],
        "volume_acceleration": sd["volume_acceleration"], "compression_ratio": sd["compression_ratio"],
        "resistance": sd["resistance"], "breakout_distance_pct": sd["breakout_distance_pct"],
        "anti_chase": sd["anti_chase"], "micro_ready": m["micro_ready"],
        "cvd_quote_60s": m["cvd_quote_60s"], "cvd_acceleration": m["cvd_acceleration"],
        "aggressive_buy_ratio": m["aggressive_buy_ratio"], "ofi": m["ofi"],
        "ofi_acceleration": m["ofi_acceleration"], "ofi_persistence": m["ofi_persistence"],
        "obi": m["obi"], "ask_depletion": m["ask_depletion"], "bid_depletion": m["bid_depletion"],
        "trade_count_60s": m["trade_count_60s"], "trade_acceleration": m["trade_acceleration"],
        "avg_trade_size_quote": m["avg_trade_size_quote"], "trade_size_shift": m["trade_size_shift"],
        "relative_volume_10s": m["relative_volume_10s"], "relative_volume_30s": m["relative_volume_30s"],
        "vwap_60s": m["vwap_60s"], "vwap_reclaim": m["vwap_reclaim"],
        "vwap_deviation_bps": m["vwap_deviation_bps"], "spread_bps": m["spread_bps"],
        "slippage_bps": m["slippage_bps"], "sequence_verified": m["sequence_verified"],
        "book_sequence_verified": m["book_sequence_verified"],
        "flow_persistence": m["flow_persistence"], "confirmations": confirmations,
        "confirmation_count": len(confirmations), "micro_confirmation_count": pass_count,
        "mandatory_status": status, "mandatory_pass_count": pass_count,
        "mandatory_total": len(mandatory), "mandatory_pass_ratio": round(pass_ratio, 4),
        "mandatory_all_aligned": buy, "failed_mandatory": failed,
        "quote_volume_24h": sd["quote_volume_24h"], "updated_ms": sd["updated_ms"],
    }


# =============================================================================
# RANKING
# =============================================================================

STATE_PRIORITY = {

    "BUY NOW": 4,

    "PRE-IGNITION": 3,

    "WATCH": 2,

    "REJECT": 1,
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
        "Î¨-V10: loading Binance universe...",
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
        f"Î¨-V10: analysing "
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
    # Blend slower MA/structure candidates with market-wide fast anomalies.
    # =========================================================================
    candidates = []
    for symbol in symbols:
        row = structure.get(symbol)
        if not row:
            continue
        proximity = min(abs(row["distance_ema50_atr"]), abs(row["distance_ema200_atr"]))
        structural_score = len(row["structure_confirmations"]) * 10.0
        structural_score += max(0.0, 20.0-proximity*5.0)
        structural_score += min(row["volume_acceleration"]*5.0,15.0)
        if row["compression"]: structural_score += 10.0
        if row["breakout_near"]: structural_score += 10.0
        if row["ma_harmony"]: structural_score += 12.0
        if row["anti_chase"]: structural_score -= 25.0
        candidates.append((structural_score,row["quote_volume_24h"],symbol))
    candidates.sort(reverse=True)

    fast_ranked = sorted(
        (x for x in anomaly_state.values() if x.get("symbol") in active),
        key=lambda x: x.get("anomaly_score",0.0),
        reverse=True,
    )
    fast_slots = min(ANOMALY_PROMOTION_SLOTS, MICRO_UNIVERSE_SIZE)
    structural_slots = max(0, MICRO_UNIVERSE_SIZE-fast_slots)
    new_symbols = [x["symbol"] for x in fast_ranked[:fast_slots]]
    for _,_,symbol in candidates:
        if symbol not in new_symbols:
            new_symbols.append(symbol)
        if len(new_symbols) >= MICRO_UNIVERSE_SIZE:
            break
    selected_micro_symbols = new_symbols
    for symbol in new_symbols:
        ensure_micro_state(symbol)

    last_structure_refresh = (
        time.time()
    )

    scanner_ready = True

    print(
        "Î¨-V10 STRUCTURE READY",
        flush=True,
    )

    print(
        "Micro universe:",
        ", ".join(
            new_symbols
        ),
        flush=True,
    )


async def refresh_anomalies() -> None:
    global last_anomaly_refresh, selected_micro_symbols
    if session is None:
        return
    universe = await get_exchange_symbols(session)
    symbols = [symbol for symbol,_ in universe[:TOP_STRUCTURE_UNIVERSE]]
    sem = asyncio.Semaphore(24)
    async def worker(symbol):
        async with sem:
            return await load_fast_anomaly(session,symbol)
    rows = await asyncio.gather(*(worker(s) for s in symbols), return_exceptions=True)
    for row in rows:
        if not isinstance(row,dict):
            continue
        row["fast_trigger"] = (
            row["relative_volume_1m"] >= 2.0
            or row["trade_count_acceleration_1m"] >= 2.0
            or row["return_3m_pct"] >= 1.2
            or row["return_5m_pct"] >= 2.0
        )
        anomaly_state[row["symbol"]] = row
        if row["return_15m_pct"] >= MISSED_MOVE_15M_PCT and row["symbol"] not in selected_micro_symbols:
            missed_moves.append({
                "timestamp_ms": now_ms(), "symbol": row["symbol"],
                "return_15m_pct": row["return_15m_pct"],
                "reason": "NOT_IN_MICRO_POOL_BEFORE_MOVE",
                "anomaly_score": row["anomaly_score"],
            })
    ranked = sorted(anomaly_state.values(),key=lambda x:x.get("anomaly_score",0.0),reverse=True)
    fast = [x["symbol"] for x in ranked[:min(ANOMALY_PROMOTION_SLOTS,MICRO_UNIVERSE_SIZE)]]
    # Immediate promotion: preserve the rest of the current pool.
    merged = fast + [x for x in selected_micro_symbols if x not in fast]
    selected_micro_symbols = merged[:MICRO_UNIVERSE_SIZE]
    for symbol in selected_micro_symbols:
        ensure_micro_state(symbol)
    last_anomaly_refresh = time.time()


async def anomaly_refresh_loop() -> None:
    global last_error
    while True:
        try:
            await refresh_anomalies()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error = f"ANOMALY: {type(exc).__name__}: {exc}"
            print(last_error,flush=True)
        await asyncio.sleep(ANOMALY_REFRESH_SECONDS)


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
                f"Î¨-V10 WebSocket connecting "
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
                    "Î¨-V10 WebSocket connected.",
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
                            "Î¨-V10 micro universe changed; "
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
                "Î¨-V10 LIVE TOP 10",
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
                "6.0",

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

            "anomaly_symbols":
                len(anomaly_state),

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
                "Î¨-V10",

            "version":
                "6.0",

            "source":
                "Binance public Spot market data",

            "rest_base":
                REST_BASE,

            "ws_base":
                WS_BASE,

            "timeframe":
                "1m anomaly + 1h/4h structure",

            "buy_policy":
                "STRICT_ALL_MANDATORY_LIVE_CONDITIONS",

            "ema_history_candles":
                999,

            "moving_averages":
                [
                    "1H EMA50", "1H EMA200", "1H SMA50", "1H SMA200",
                    "4H EMA50", "4H EMA200", "4H SMA50", "4H SMA200",
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
                    "multi-window relative volume",
                    "CVD acceleration",
                    "OFI acceleration",
                    "trade-size shift",
                    "VWAP reclaim",
                    "spread filter",
                    "top-10-book slippage estimate",
                    "trade/update sequence validation",
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

            "anomaly_universe_size": len(anomaly_state),
            "last_anomaly_refresh_age_seconds": (
                int(time.time()-last_anomaly_refresh) if last_anomaly_refresh else None
            ),
            "missed_moves": list(missed_moves)[-50:],
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
        f"Î¨-V10 HTTP listening "
        f"on port {PORT}",
        flush=True,
    )

    return runner


# =============================================================================
# INITIALISATION
# =============================================================================

async def initialise() -> None:

    print(
        "Î¨-V10 INITIALISING...",
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

    await refresh_anomalies()
    await refresh_structure()

    print(
        "Î¨-V10 INITIALISED.",
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
                "Î¨-V10 will retry structure "
                "loading in background.",
                flush=True,
            )


        tasks = [

            asyncio.create_task(
                structure_refresh_loop()
            ),

            asyncio.create_task(
                anomaly_refresh_loop()
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
            "Î¨-V10 stopped.",
            flush=True,)
