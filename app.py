import asyncio
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import web

# =============================================================================
# Î¨-V10.1 â€” setup-aware, time-synchronised Binance Spot scanner
# Read-only: it never places trades.
# =============================================================================

REST_BASE = os.getenv("BINANCE_REST", "https://data-api.binance.vision").rstrip("/")
WS_BASE = os.getenv("BINANCE_WS", "wss://data-stream.binance.vision").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))
TOP_STRUCTURE_UNIVERSE = int(os.getenv("TOP_STRUCTURE_UNIVERSE", "120"))
MICRO_UNIVERSE_SIZE = int(os.getenv("MICRO_UNIVERSE_SIZE", "30"))
RETURN_LIMIT = int(os.getenv("RETURN_LIMIT", "10"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME", "500000"))
STRUCTURE_REFRESH_SECONDS = int(os.getenv("STRUCTURE_REFRESH_SECONDS", "300"))
ANOMALY_REFRESH_SECONDS = int(os.getenv("ANOMALY_REFRESH_SECONDS", "15"))
ANOMALY_CANDLE_LIMIT = int(os.getenv("ANOMALY_CANDLE_LIMIT", "40"))
ANOMALY_PROMOTION_SLOTS = int(os.getenv("ANOMALY_PROMOTION_SLOTS", "80"))
PRINT_SECONDS = int(os.getenv("PRINT_SECONDS", "30"))
MICRO_POOL_MIN_HOLD_SECONDS = int(os.getenv("MICRO_POOL_MIN_HOLD_SECONDS", "60"))
EMA_TOUCH_ATR = float(os.getenv("EMA_TOUCH_ATR", "0.75"))
EMA_NEAR_ATR = float(os.getenv("EMA_NEAR_ATR", "1.00"))
ANTI_CHASE_ATR = float(os.getenv("ANTI_CHASE_ATR", "2.50"))
BREAKOUT_NEAR_PCT = float(os.getenv("BREAKOUT_NEAR_PCT", "2.0"))
MAX_SPREAD_BPS = float(os.getenv("MAX_SPREAD_BPS", "20"))
MAX_SLIPPAGE_BPS = float(os.getenv("MAX_SLIPPAGE_BPS", "35"))
SLIPPAGE_TEST_NOTIONAL = float(os.getenv("SLIPPAGE_TEST_NOTIONAL", "1000"))
MISSED_MOVE_15M_PCT = float(os.getenv("MISSED_MOVE_15M_PCT", "10.0"))
TOP_BOOK_LEVELS = int(os.getenv("TOP_BOOK_LEVELS", "20"))
DEPTH_SNAPSHOT_LIMIT = int(os.getenv("DEPTH_SNAPSHOT_LIMIT", "100"))
USER_AGENT = "psi-v10-live-scanner/10.1"

# Signal TTLs: a higher-timeframe event remains valid while live microstructure
# catches up, instead of requiring unrelated events on the exact same tick.
TTL_STRUCTURE_MS = int(os.getenv("TTL_STRUCTURE_SECONDS", "1800")) * 1000
TTL_MA_MS = int(os.getenv("TTL_MA_SECONDS", "1800")) * 1000
TTL_BREAKOUT_MS = int(os.getenv("TTL_BREAKOUT_SECONDS", "600")) * 1000
TTL_ANOMALY_MS = int(os.getenv("TTL_ANOMALY_SECONDS", "300")) * 1000
TTL_MICRO_MS = int(os.getenv("TTL_MICRO_SECONDS", "45")) * 1000

UK_SYMBOLS_RAW = os.getenv("UK_SYMBOLS", "").strip()
UK_SYMBOLS = {x.strip().upper() for x in UK_SYMBOLS_RAW.split(",") if x.strip()}

session: Optional[aiohttp.ClientSession] = None
symbol_meta: Dict[str, dict] = {}
structure: Dict[str, dict] = {}
anomaly_state: Dict[str, dict] = {}
micro_state: Dict[str, dict] = defaultdict(dict)
signal_memory: Dict[str, Dict[str, int]] = defaultdict(dict)
selected_micro_symbols: List[str] = []
missed_moves = deque(maxlen=500)
diagnostic_events = deque(maxlen=1500)
resolved_outcomes = deque(maxlen=1000)
pending_outcomes: List[dict] = []
diagnostic_failures = Counter()
diagnostic_setup_evals = Counter()
diagnostic_setup_buys = Counter()
last_candidate_record_ms: Dict[str, int] = defaultdict(int)
scanner_started_at = time.time()
last_structure_refresh = 0.0
last_anomaly_refresh = 0.0
last_micro_pool_change = 0.0
last_error: Optional[str] = None
scanner_ready = False
rest_connected = False
websocket_connected = False
websocket_symbols: List[str] = []


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def safe_div(a: float, b: float, default=0.0) -> float:
    return a / b if b else default


def average(values) -> float:
    values = list(values)
    return sum(values) / len(values) if values else 0.0


def percentile_rank(history, value: float) -> float:
    vals = list(history)
    if len(vals) < 12:
        return 0.5
    return sum(1 for x in vals if x <= value) / len(vals)


def prune_time_deque(dq: deque, cutoff_ms: int) -> None:
    while dq and dq[0][0] < cutoff_ms:
        dq.popleft()


def remember(symbol: str, key: str, condition: bool, ttl_ms: int) -> bool:
    n = now_ms()
    if condition:
        signal_memory[symbol][key] = n + ttl_ms
    return signal_memory[symbol].get(key, 0) >= n


def remembered(symbol: str, key: str) -> bool:
    return signal_memory[symbol].get(key, 0) >= now_ms()


async def api_get(client: aiohttp.ClientSession, path: str, params: Optional[dict] = None):
    global rest_connected, last_error
    try:
        async with client.get(
            f"{REST_BASE}{path}", params=params,
            timeout=aiohttp.ClientTimeout(total=20)
        ) as response:
            text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"Binance REST {response.status}: {text[:300]}")
            rest_connected = True
            return json.loads(text)
    except Exception as exc:
        rest_connected = False
        last_error = f"REST: {type(exc).__name__}: {exc}"
        raise


def ema(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    result = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    for value in values[period:]:
        result = value * k + result * (1.0 - k)
    return result


def sma(values: List[float], period: int) -> Optional[float]:
    return average(values[-period:]) if len(values) >= period else None


def atr(rows: List[list], period: int = 14) -> Optional[float]:
    if len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h, l, pc = safe_float(rows[i][2]), safe_float(rows[i][3]), safe_float(rows[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return average(trs[-period:]) if len(trs) >= period else None


async def get_exchange_symbols(client: aiohttp.ClientSession) -> List[Tuple[str, float]]:
    info, tickers = await asyncio.gather(
        api_get(client, "/api/v3/exchangeInfo"),
        api_get(client, "/api/v3/ticker/24hr"),
    )
    ticker_map = {x.get("symbol"): x for x in tickers if isinstance(x, dict)}
    symbol_meta.clear()
    universe = []
    for market in info.get("symbols", []):
        symbol = market.get("symbol", "")
        if not symbol or market.get("status") != "TRADING":
            continue
        if market.get("quoteAsset") != "USDT" or not market.get("isSpotTradingAllowed", False):
            continue
        if UK_SYMBOLS and symbol not in UK_SYMBOLS:
            continue
        quote_volume = safe_float(ticker_map.get(symbol, {}).get("quoteVolume"))
        if quote_volume < MIN_QUOTE_VOLUME:
            continue
        symbol_meta[symbol] = {
            "quote_volume_24h": quote_volume,
            "base_asset": market.get("baseAsset"),
            "quote_asset": market.get("quoteAsset"),
        }
        universe.append((symbol, quote_volume))
    universe.sort(key=lambda x: x[1], reverse=True)
    return universe


async def load_klines(client, symbol: str, interval: str, limit: int) -> Optional[List[list]]:
    try:
        rows = await api_get(client, "/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        return rows if isinstance(rows, list) else None
    except Exception:
        return None


def ma_snapshot(rows: List[list]) -> Optional[dict]:
    if not rows or len(rows) < 205:
        return None
    closed = rows[:-1]
    closes = [safe_float(r[4]) for r in closed]
    lows = [safe_float(r[3]) for r in closed]
    current = safe_float(rows[-1][4])
    a = atr(closed, 14)
    e50, e200, s50, s200 = ema(closes, 50), ema(closes, 200), sma(closes, 50), sma(closes, 200)
    if current <= 0 or not a or not all((e50, e200, s50, s200)):
        return None
    prev_e50 = ema(closes[:-3], 50) or e50
    prev_e200 = ema(closes[:-3], 200) or e200
    prev_s50 = sma(closes[:-3], 50) or s50
    previous, last = closes[-2], closes[-1]
    values = {"ema50": e50, "ema200": e200, "sma50": s50, "sma200": s200}
    near = {k: abs(current-v)/a <= EMA_NEAR_ATR for k,v in values.items()}
    touch = {k: abs(current-v)/a <= EMA_TOUCH_ATR for k,v in values.items()}
    reclaim = {k: previous <= v and last > v for k,v in values.items()}
    rejection = {k: lows[-1] <= v and last > v for k,v in values.items()}
    bullish_stack = current > e50 > e200 and current > s50 > s200
    improving = e50 >= prev_e50 and e200 >= prev_e200 and s50 >= prev_s50
    reclaim_path = (
        (reclaim["ema50"] or rejection["ema50"] or reclaim["sma50"] or rejection["sma50"])
        and current > e200 and current > s200 and improving
    )
    support = any(touch.values()) or any(reclaim.values()) or any(rejection.values())
    return {
        "price": current, "atr14": a, **values, "near": near, "touch": touch,
        "reclaim": reclaim, "rejection": rejection, "bullish_stack": bullish_stack,
        "improving_slope": improving, "reclaim_path": reclaim_path,
        "structural_support": support,
    }


async def load_fast_anomaly(client, symbol: str) -> Optional[dict]:
    rows = await load_klines(client, symbol, "1m", ANOMALY_CANDLE_LIMIT)
    if not rows or len(rows) < 25:
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
    score = max(0,r1)*3 + max(0,r3)*1.7 + max(0,r5) + max(0,r15)*0.35
    score += min(rv,8)*4 + min(ta,8)*3 + (6 if compression_pct < 3 else 0)
    fast_trigger = rv >= 1.6 or ta >= 1.6 or r3 >= 0.8 or r5 >= 1.4
    volume_impulse = rv >= 1.35 or ta >= 1.35
    return {
        "symbol": symbol, "price": p, "return_1m_pct": r1, "return_3m_pct": r3,
        "return_5m_pct": r5, "return_15m_pct": r15, "relative_volume_1m": rv,
        "trade_count_acceleration_1m": ta, "compression_20m_pct": compression_pct,
        "anomaly_score": score, "fast_trigger": fast_trigger, "volume_impulse": volume_impulse,
        "updated_ms": now_ms(),
    }


async def load_structure(client, symbol: str) -> Optional[dict]:
    rows1, rows4, rows15 = await asyncio.gather(
        load_klines(client, symbol, "1h", 260),
        load_klines(client, symbol, "4h", 260),
        load_klines(client, symbol, "15m", 80),
    )
    m1, m4 = ma_snapshot(rows1 or []), ma_snapshot(rows4 or [])
    if not m1 or not m4 or not rows4 or not rows15:
        return None
    closed4 = rows4[:-1]
    highs4 = [safe_float(r[2]) for r in closed4]
    lows4 = [safe_float(r[3]) for r in closed4]
    vols4 = [safe_float(r[5]) for r in closed4]
    current = safe_float(rows4[-1][4])
    volume4 = safe_div(vols4[-1], average(vols4[-21:-1]), 0.0)
    closed15 = rows15[:-1]
    vols15 = [safe_float(r[7]) for r in closed15]
    volume15 = safe_div(vols15[-1], average(vols15[-21:-1]), 0.0)
    recent_ranges = [highs4[i]-lows4[i] for i in range(max(0,len(highs4)-6),len(highs4))]
    baseline_ranges = [highs4[i]-lows4[i] for i in range(max(0,len(highs4)-26),max(0,len(highs4)-6))]
    compression_ratio = safe_div(average(recent_ranges), average(baseline_ranges), 1.0)
    compression = compression_ratio <= 0.82
    resistance_window = highs4[-21:-1]
    resistance = max(resistance_window) if resistance_window else highs4[-1]
    breakout_distance_pct = safe_div(resistance-current, current) * 100.0
    breakout_near = -0.6 <= breakout_distance_pct <= BREAKOUT_NEAR_PCT
    breakout = current > resistance
    full_harmony = m1["bullish_stack"] and m4["bullish_stack"]
    reclaim_regime = (m1["reclaim_path"] or m4["reclaim_path"]) and m1["improving_slope"] and m4["improving_slope"]
    ma_regime = full_harmony or reclaim_regime
    ma_support = m1["structural_support"] or m4["structural_support"]
    dist50 = safe_div(current-m4["ema50"], m4["atr14"])
    dist200 = safe_div(current-m4["ema200"], m4["atr14"])
    anti_chase = dist50 > ANTI_CHASE_ATR and dist200 > ANTI_CHASE_ATR
    confirmations = []
    if full_harmony: confirmations.append("FULL_1H_4H_MA_HARMONY")
    if reclaim_regime: confirmations.append("MA_RECLAIM_REGIME")
    if ma_support: confirmations.append("MA_SUPPORT")
    if compression: confirmations.append("4H_COMPRESSION")
    if breakout_near: confirmations.append("NEAR_RESISTANCE")
    if breakout: confirmations.append("BREAKOUT")
    if volume15 >= 1.20: confirmations.append("15M_VOLUME_ACCELERATION")
    if volume4 >= 1.20: confirmations.append("4H_VOLUME_ACCELERATION")
    return {
        "symbol": symbol, "price": current, "ma_1h": m1, "ma_4h": m4,
        "ma_harmony": full_harmony, "ma_reclaim_regime": reclaim_regime,
        "ma_regime": ma_regime, "structural_support": ma_support,
        "ema50_1h": m1["ema50"], "ema200_1h": m1["ema200"], "sma50_1h": m1["sma50"], "sma200_1h": m1["sma200"],
        "ema50_4h": m4["ema50"], "ema200_4h": m4["ema200"], "sma50_4h": m4["sma50"], "sma200_4h": m4["sma200"],
        "atr14_1h": m1["atr14"], "atr14_4h": m4["atr14"],
        "distance_ema50_atr": dist50, "distance_ema200_atr": dist200,
        "ema50_near": m4["near"]["ema50"], "ema200_near": m4["near"]["ema200"],
        "volume_acceleration": volume4, "volume_acceleration_15m": volume15,
        "compression_ratio": compression_ratio, "compression": compression,
        "resistance": resistance, "breakout_distance_pct": breakout_distance_pct,
        "breakout_near": breakout_near, "breakout": breakout, "anti_chase": anti_chase,
        "structure_confirmations": confirmations,
        "quote_volume_24h": symbol_meta.get(symbol,{}).get("quote_volume_24h",0.0),
        "updated_ms": now_ms(),
    }


def ensure_micro_state(symbol: str) -> dict:
    s = micro_state[symbol]
    if "trades" not in s:
        s["trades"] = deque(maxlen=30000)
        for key in ("ofi","obi","ask_depletion","bid_depletion","spread_bps","slippage_bps"):
            s[key] = deque(maxlen=20000)
        s["metrics_history"] = defaultdict(lambda: deque(maxlen=240))
        s["last_metric_snapshot_ms"] = 0
        s["book_bids"] = {}
        s["book_asks"] = {}
        s["book_buffer"] = deque(maxlen=5000)
        s["book_snapshot_ready"] = False
        s["book_resyncing"] = False
        s["last_book_update_id"] = None
        s["book_sequence_ok"] = True
        s["book_sequence_samples"] = 0
        s["last_agg_id"] = None
        s["trade_sequence_ok"] = True
        s["trade_sequence_samples"] = 0
        s["last_trade_ms"] = 0
        s["last_book_ms"] = 0
        s["book_updates"] = 0
    return s


def process_agg_trade(symbol: str, data: dict) -> None:
    s = ensure_micro_state(symbol)
    ts = int(data.get("T") or data.get("E") or now_ms())
    price, qty = safe_float(data.get("p")), safe_float(data.get("q"))
    if price <= 0 or qty <= 0:
        return
    quote = price * qty
    signed = -quote if bool(data.get("m", False)) else quote
    agg_id = data.get("a")
    try:
        if agg_id is not None:
            agg_id = int(agg_id)
            if s["last_agg_id"] is not None:
                s["trade_sequence_samples"] += 1
                if agg_id <= s["last_agg_id"]:
                    s["trade_sequence_ok"] = False
            s["last_agg_id"] = agg_id
    except (TypeError, ValueError):
        s["trade_sequence_ok"] = False
    s["trades"].append((ts, signed, quote, price, qty))
    s["last_trade_ms"] = ts
    prune_time_deque(s["trades"], now_ms()-180_000)


def sorted_levels(book: dict, reverse: bool) -> List[Tuple[float,float]]:
    return [(p, book[p]) for p in sorted(book.keys(), reverse=reverse)[:TOP_BOOK_LEVELS] if book[p] > 0]


def depth_notional(levels) -> float:
    return sum(p*q for p,q in levels)


def calculate_obi(bids, asks) -> float:
    bv, av = depth_notional(bids), depth_notional(asks)
    return safe_div(bv-av, bv+av)


def calculate_ofi(previous_bids, previous_asks, bids, asks) -> float:
    pb, pa = dict(previous_bids), dict(previous_asks)
    cb, ca = dict(bids), dict(asks)
    bid_change = sum(p*(cb.get(p,0)-pb.get(p,0)) for p in set(pb)|set(cb))
    ask_change = sum(p*(ca.get(p,0)-pa.get(p,0)) for p in set(pa)|set(ca))
    return safe_div(bid_change-ask_change, abs(bid_change)+abs(ask_change))


def estimate_buy_slippage_bps(asks, notional: float) -> Optional[float]:
    if not asks or notional <= 0:
        return None
    remaining, base, spent = notional, 0.0, 0.0
    best = asks[0][0]
    for price, qty in asks:
        level_quote = price*qty
        take = min(remaining, level_quote)
        if take > 0:
            spent += take
            base += take/price
            remaining -= take
        if remaining <= 1e-9:
            break
    if remaining > 1e-6 or base <= 0 or best <= 0:
        return None
    return ((spent/base)/best-1.0)*10000.0


def apply_book_changes(book: dict, changes) -> None:
    for raw in changes:
        if len(raw) < 2:
            continue
        p, q = safe_float(raw[0]), safe_float(raw[1])
        if p <= 0:
            continue
        if q == 0:
            book.pop(p, None)
        elif q > 0:
            book[p] = q


def calculate_book_metrics(symbol: str, previous_bids, previous_asks) -> None:
    s = ensure_micro_state(symbol)
    bids = sorted_levels(s["book_bids"], True)
    asks = sorted_levels(s["book_asks"], False)
    if not bids or not asks:
        return
    ts = now_ms()
    obi = calculate_obi(bids, asks)
    mid = (bids[0][0]+asks[0][0])/2
    spread = safe_div(asks[0][0]-bids[0][0], mid)*10000
    slip = estimate_buy_slippage_bps(asks, SLIPPAGE_TEST_NOTIONAL)
    s["obi"].append((ts,obi)); s["spread_bps"].append((ts,spread))
    if slip is not None: s["slippage_bps"].append((ts,slip))
    if previous_bids and previous_asks:
        ofi = calculate_ofi(previous_bids,previous_asks,bids,asks)
        prev_ask_notional, prev_bid_notional = depth_notional(previous_asks), depth_notional(previous_bids)
        ask_dep = safe_div(prev_ask_notional-depth_notional(asks), prev_ask_notional)
        bid_dep = safe_div(prev_bid_notional-depth_notional(bids), prev_bid_notional)
        s["ofi"].append((ts,ofi)); s["ask_depletion"].append((ts,ask_dep)); s["bid_depletion"].append((ts,bid_dep))
    s["last_book_ms"] = ts; s["book_updates"] += 1
    for key in ("ofi","obi","ask_depletion","bid_depletion","spread_bps","slippage_bps"):
        prune_time_deque(s[key], ts-120_000)


def process_partial_depth_snapshot(symbol: str, data: dict) -> None:
    """Process a complete Binance depth20 WebSocket snapshot.

    Full top-20 snapshots remove the REST bootstrap dependency. Three
    monotonically newer snapshots are required before book sequence is verified.
    """
    s = ensure_micro_state(symbol)
    try:
        update_id = int(data.get("lastUpdateId") or data.get("u") or 0)
    except (TypeError, ValueError):
        update_id = 0
    raw_bids = data.get("bids") or data.get("b") or []
    raw_asks = data.get("asks") or data.get("a") or []
    if update_id <= 0 or not raw_bids or not raw_asks:
        s["book_sequence_ok"] = False
        return
    last = s.get("last_book_update_id")
    if last is not None and update_id <= int(last):
        return
    prev_bids = sorted_levels(s["book_bids"], True) if s.get("book_snapshot_ready") else []
    prev_asks = sorted_levels(s["book_asks"], False) if s.get("book_snapshot_ready") else []
    bids = {
        safe_float(px): safe_float(qty)
        for px, qty in raw_bids[:20]
        if safe_float(px) > 0 and safe_float(qty) > 0
    }
    asks = {
        safe_float(px): safe_float(qty)
        for px, qty in raw_asks[:20]
        if safe_float(px) > 0 and safe_float(qty) > 0
    }
    if not bids or not asks:
        s["book_sequence_ok"] = False
        return
    s["book_bids"], s["book_asks"] = bids, asks
    s["last_book_update_id"] = update_id
    s["book_snapshot_ready"] = True
    s["book_resyncing"] = False
    s["book_sequence_ok"] = True
    s["book_sequence_samples"] += 1
    s["book_buffer"].clear()
    calculate_book_metrics(symbol, prev_bids, prev_asks)


def schedule_book_resync(symbol: str) -> None:
    s = ensure_micro_state(symbol)
    if s["book_resyncing"]:
        return
    s["book_resyncing"] = True
    s["book_snapshot_ready"] = False
    asyncio.create_task(bootstrap_book(symbol))


def process_diff_depth(symbol: str, data: dict) -> None:
    s = ensure_micro_state(symbol)
    try:
        U, u = int(data.get("U")), int(data.get("u"))
    except (TypeError, ValueError):
        s["book_sequence_ok"] = False
        return
    event = {"U":U,"u":u,"b":data.get("b",[]),"a":data.get("a",[])}
    if not s["book_snapshot_ready"]:
        s["book_buffer"].append(event)
        return
    last = s["last_book_update_id"]
    if last is None or U != last + 1:
        s["book_sequence_ok"] = False
        s["book_buffer"].append(event)
        schedule_book_resync(symbol)
        return
    prev_bids = sorted_levels(s["book_bids"], True)
    prev_asks = sorted_levels(s["book_asks"], False)
    apply_book_changes(s["book_bids"], event["b"])
    apply_book_changes(s["book_asks"], event["a"])
    s["last_book_update_id"] = u
    s["book_sequence_samples"] += 1
    s["book_sequence_ok"] = True
    calculate_book_metrics(symbol, prev_bids, prev_asks)


async def bootstrap_book(symbol: str) -> None:
    s = ensure_micro_state(symbol)
    if session is None:
        s["book_resyncing"] = False
        return
    try:
        snap = await api_get(session, "/api/v3/depth", {"symbol":symbol,"limit":DEPTH_SNAPSHOT_LIMIT})
        last_id = int(snap.get("lastUpdateId",0))
        bids = {safe_float(p):safe_float(q) for p,q in snap.get("bids",[]) if safe_float(p)>0 and safe_float(q)>0}
        asks = {safe_float(p):safe_float(q) for p,q in snap.get("asks",[]) if safe_float(p)>0 and safe_float(q)>0}
        buffered = list(s["book_buffer"])
        s["book_buffer"].clear()
        buffered = [e for e in buffered if e["u"] > last_id]
        first_index = None
        for i,e in enumerate(buffered):
            if e["U"] <= last_id+1 <= e["u"]:
                first_index = i
                break
        if first_index is not None:
            previous_u = last_id
            for e in buffered[first_index:]:
                if previous_u != last_id and e["U"] != previous_u+1:
                    raise RuntimeError("depth sequence gap during bootstrap")
                apply_book_changes(bids,e["b"]); apply_book_changes(asks,e["a"])
                previous_u = e["u"]
            last_id = previous_u
        s["book_bids"],s["book_asks"] = bids,asks
        s["last_book_update_id"] = last_id
        s["book_snapshot_ready"] = True
        s["book_sequence_ok"] = True
        s["book_sequence_samples"] = max(s["book_sequence_samples"],3)
        calculate_book_metrics(symbol, [], [])
    except Exception:
        s["book_snapshot_ready"] = False
        s["book_sequence_ok"] = False
    finally:
        s["book_resyncing"] = False


def _window(rows, now, lo, hi=0):
    lower, upper = now-lo*1000, now-hi*1000
    return [r for r in rows if lower <= r[0] < upper]


def micro_metrics(symbol: str) -> dict:
    s, n = ensure_micro_state(symbol), now_ms()
    trades = s["trades"]
    prune_time_deque(trades,n-180_000)
    recent = _window(trades,n,60); first30 = _window(trades,n,60,30); last30 = _window(trades,n,30)
    prev60 = _window(trades,n,120,60); last10 = _window(trades,n,10); prev10 = _window(trades,n,20,10)
    cvd60 = sum(r[1] for r in recent)
    cvd_acc = sum(r[1] for r in last30)-sum(r[1] for r in first30)
    total60 = sum(r[2] for r in recent)
    buy_ratio = safe_div(sum(r[2] for r in recent if r[1]>0),total60,0.5)
    trade_acc = safe_div(len(last30),max(len(first30),1))
    avg_first = safe_div(sum(r[2] for r in first30),len(first30)); avg_last = safe_div(sum(r[2] for r in last30),len(last30))
    trade_size_shift = safe_div(avg_last,max(avg_first,1e-9))
    rv10 = safe_div(sum(r[2] for r in last10),max(sum(r[2] for r in prev10),1e-9))
    rv30 = safe_div(sum(r[2] for r in last30),max(sum(r[2] for r in prev60)/2,1e-9))
    vwap = safe_div(sum(r[3]*r[4] for r in recent),sum(r[4] for r in recent))
    last_price = recent[-1][3] if recent else 0.0
    prev_vwap = safe_div(sum(r[3]*r[4] for r in first30),sum(r[4] for r in first30))
    vwap_reclaim = bool(vwap and last_price>=vwap and (not first30 or first30[-1][3]<=prev_vwap or cvd_acc>0))
    def vals(key,seconds=60): return [r[1] for r in s[key] if r[0]>=n-seconds*1000]
    ofis,obis,asks,bids = vals("ofi"),vals("obi"),vals("ask_depletion"),vals("bid_depletion")
    spreads,slips = vals("spread_bps"),vals("slippage_bps")
    ofi,obi = average(ofis[-20:]),average(obis[-20:])
    ask_dep,bid_dep = average(asks[-20:]),average(bids[-20:])
    half=max(1,len(ofis)//2); ofi_acc=average(ofis[half:])-average(ofis[:half]) if len(ofis)>=6 else 0.0
    ofi_persistence=safe_div(sum(1 for x in ofis[-20:] if x>0),len(ofis[-20:]))
    flow_persistence=safe_div(sum(1 for r in last30 if r[1]>0),len(last30))
    spread=spreads[-1] if spreads else None; slip=slips[-1] if slips else None
    trade_fresh=s["last_trade_ms"]>=n-15_000; book_fresh=s["last_book_ms"]>=n-5_000
    seq=s["trade_sequence_ok"] and s["trade_sequence_samples"]>=3
    book_seq=s["book_snapshot_ready"] and s["book_sequence_ok"] and s["book_sequence_samples"]>=3
    micro_ready=trade_fresh and book_fresh and len(recent)>=10 and len(ofis)>=6 and s["book_updates"]>=8
    raw = {
        "ofi":ofi,"obi":obi,"cvd_acc":cvd_acc,"rv30":rv30,"trade_acc":trade_acc,"ask_dep":ask_dep,"trade_size_shift":trade_size_shift
    }
    hist=s["metrics_history"]
    ranks={k:percentile_rank(hist[k],v) for k,v in raw.items()}
    if n-s["last_metric_snapshot_ms"]>=5000 and micro_ready:
        for k,v in raw.items(): hist[k].append(v)
        s["last_metric_snapshot_ms"]=n
    relative_flow = ((len(hist["ofi"])<12 and ofi>0.03) or ranks["ofi"]>=0.70) and ofi_acc>=0
    relative_book = ((len(hist["obi"])<12 and obi>0.03) or ranks["obi"]>=0.65) and (((len(hist["ask_dep"])<12 and ask_dep>0.0) or ranks["ask_dep"]>=0.60))
    relative_activity = ((len(hist["rv30"])<12 and rv30>=1.0) or ranks["rv30"]>=0.65) and ((len(hist["trade_acc"])<12 and trade_acc>=1.0) or ranks["trade_acc"]>=0.60)
    return {
        "micro_ready":micro_ready,"sequence_verified":seq,"book_sequence_verified":book_seq,
        "cvd_quote_60s":cvd60,"cvd_acceleration":cvd_acc,"aggressive_buy_ratio":buy_ratio,
        "trade_count_60s":len(recent),"trade_acceleration":trade_acc,"trade_size_shift":trade_size_shift,
        "relative_volume_10s":rv10,"relative_volume_30s":rv30,"vwap_60s":vwap,"vwap_reclaim":vwap_reclaim,
        "ofi":ofi,"ofi_acceleration":ofi_acc,"ofi_persistence":ofi_persistence,"flow_persistence":flow_persistence,
        "obi":obi,"ask_depletion":ask_dep,"bid_depletion":bid_dep,"spread_bps":spread,"slippage_bps":slip,
        "last_price":last_price,"relative_ranks":ranks,"relative_flow":relative_flow,"relative_book":relative_book,
        "relative_activity":relative_activity,
    }


def update_signal_memory(symbol: str, sd: dict, m: dict, anomaly: dict) -> None:
    remember(symbol,"MA_REGIME",sd["ma_regime"],TTL_MA_MS)
    remember(symbol,"MA_RETEST",sd["structural_support"] and (sd["ma_1h"]["reclaim_path"] or sd["ma_4h"]["reclaim_path"]),TTL_MA_MS)
    remember(symbol,"MA_SLOPE_UP",sd["ma_1h"]["improving_slope"] and sd["ma_4h"]["improving_slope"],TTL_MA_MS)
    remember(symbol,"COMPRESSION_NEAR",sd["compression"] and sd["breakout_near"],TTL_STRUCTURE_MS)
    remember(symbol,"BREAKOUT",sd["breakout"],TTL_BREAKOUT_MS)
    remember(symbol,"STRUCTURE_SUPPORT",sd["structural_support"],TTL_STRUCTURE_MS)
    remember(symbol,"ANOMALY",bool(anomaly.get("fast_trigger")),TTL_ANOMALY_MS)
    remember(symbol,"VOLUME_IMPULSE",bool(anomaly.get("volume_impulse")) or sd["volume_acceleration_15m"]>=1.20,TTL_ANOMALY_MS)
    remember(symbol,"CVD_POSITIVE",m["cvd_quote_60s"]>0 and m["cvd_acceleration"]>=0,TTL_MICRO_MS)
    remember(symbol,"RELATIVE_FLOW",m["relative_flow"],TTL_MICRO_MS)
    remember(symbol,"RELATIVE_BOOK",m["relative_book"],TTL_MICRO_MS)
    remember(symbol,"RELATIVE_ACTIVITY",m["relative_activity"],TTL_MICRO_MS)
    remember(symbol,"BUY_DOMINANCE",m["aggressive_buy_ratio"]>=0.55,TTL_MICRO_MS)
    remember(symbol,"VWAP_RECLAIM",m["vwap_reclaim"],TTL_MICRO_MS)
    remember(symbol,"PERSISTENCE",m["ofi_persistence"]>=0.55 and m["flow_persistence"]>=0.55,TTL_MICRO_MS)


def setup_templates(symbol: str, sd: dict, m: dict) -> Dict[str,dict]:
    last_price=m["last_price"] or sd["price"]
    resistance=sd["resistance"]
    retest_hold = remembered(symbol,"BREAKOUT") and resistance>0 and last_price>=resistance*0.997 and last_price<=resistance*1.015 and m["vwap_reclaim"]
    common = {
        "ANOMALY_RECENT":remembered(symbol,"ANOMALY"),
        "VOLUME_IMPULSE":remembered(symbol,"VOLUME_IMPULSE"),
        "CVD_POSITIVE":remembered(symbol,"CVD_POSITIVE"),
        "RELATIVE_FLOW":remembered(symbol,"RELATIVE_FLOW"),
        "RELATIVE_BOOK":remembered(symbol,"RELATIVE_BOOK"),
        "RELATIVE_ACTIVITY":remembered(symbol,"RELATIVE_ACTIVITY"),
        "BUY_DOMINANCE":remembered(symbol,"BUY_DOMINANCE"),
        "VWAP_RECLAIM":remembered(symbol,"VWAP_RECLAIM"),
        "PERSISTENCE":remembered(symbol,"PERSISTENCE"),
    }
    return {
        "COMPRESSION_BREAKOUT": {
            **common,
            "MA_REGIME":remembered(symbol,"MA_REGIME"),
            "COMPRESSION_OR_BREAKOUT":remembered(symbol,"COMPRESSION_NEAR") or remembered(symbol,"BREAKOUT"),
        },
        "MA_RETEST_RECLAIM": {
            **common,
            "MA_RETEST":remembered(symbol,"MA_RETEST"),
            "MA_SLOPE_UP":remembered(symbol,"MA_SLOPE_UP"),
            "STRUCTURE_SUPPORT":remembered(symbol,"STRUCTURE_SUPPORT"),
        },
        "BREAKOUT_RETEST_CONTINUATION": {
            **common,
            "MA_REGIME":remembered(symbol,"MA_REGIME"),
            "BREAKOUT_RECENT":remembered(symbol,"BREAKOUT"),
            "RETEST_HOLD":retest_hold,
        },
    }


def record_diagnostics(symbol: str, setup_name: str, setup: dict, state: str, price: float) -> None:
    diagnostic_setup_evals[setup_name]+=1
    failed=[k for k,v in setup.items() if not v]
    for key in failed: diagnostic_failures[f"{setup_name}:{key}"]+=1
    if state=="BUY NOW": diagnostic_setup_buys[setup_name]+=1
    diagnostic_events.append({"timestamp_ms":now_ms(),"symbol":symbol,"setup":setup_name,"state":state,"failed":failed,"price":price})


def maybe_record_outcome_candidate(row: dict) -> None:
    if row["state"] not in ("PRE-IGNITION","BUY NOW"):
        return
    n=now_ms(); symbol=row["symbol"]
    if n-last_candidate_record_ms[symbol]<60_000:
        return
    last_candidate_record_ms[symbol]=n
    pending_outcomes.append({"symbol":symbol,"setup":row["active_setup"],"state":row["state"],"entry_ms":n,"entry_price":row["price"],"returns":{}})


def current_symbol_price(symbol: str) -> float:
    s=micro_state.get(symbol,{})
    trades=s.get("trades")
    if trades: return trades[-1][3]
    if symbol in anomaly_state: return safe_float(anomaly_state[symbol].get("price"))
    return safe_float(structure.get(symbol,{}).get("price"))


def resolve_outcomes() -> None:
    n=now_ms(); keep=[]
    for event in pending_outcomes:
        p=current_symbol_price(event["symbol"])
        if p<=0 or event["entry_price"]<=0:
            keep.append(event); continue
        age=n-event["entry_ms"]
        for label,ms in (("1m",60_000),("5m",300_000),("15m",900_000),("1h",3_600_000)):
            if age>=ms and label not in event["returns"]:
                event["returns"][label]=(p/event["entry_price"]-1)*100
        if age>=3_600_000:
            resolved_outcomes.append(event)
        else:
            keep.append(event)
    pending_outcomes[:] = keep[-1000:]


def evaluate_symbol(symbol: str) -> Optional[dict]:
    sd=structure.get(symbol)
    if not sd: return None
    m=micro_metrics(symbol); anomaly=anomaly_state.get(symbol,{})
    update_signal_memory(symbol,sd,m,anomaly)
    hard = {
        "LIVE_MICRO_DATA":m["micro_ready"],
        "TRADE_SEQUENCE_VALID":m["sequence_verified"],
        "BOOK_SEQUENCE_VALID":m["book_sequence_verified"],
        "SPREAD_FILTER":m["spread_bps"] is not None and m["spread_bps"]<=MAX_SPREAD_BPS,
        "SLIPPAGE_FILTER":m["slippage_bps"] is not None and m["slippage_bps"]<=MAX_SLIPPAGE_BPS,
        "ANTI_CHASE_CLEAR":not sd["anti_chase"],
    }
    templates=setup_templates(symbol,sd,m)
    setup_results={}
    for name,gates in templates.items():
        passed=sum(bool(v) for v in gates.values()); total=len(gates)
        setup_results[name]={"gates":gates,"pass_count":passed,"total":total,"pass_ratio":safe_div(passed,total),"all_aligned":all(gates.values())}
    active_setup=max(setup_results,key=lambda k:setup_results[k]["pass_ratio"])
    best=setup_results[active_setup]
    hard_ok=all(hard.values())
    buy=hard_ok and any(x["all_aligned"] for x in setup_results.values())
    if buy:
        active_setup=next(name for name,x in setup_results.items() if x["all_aligned"])
        best=setup_results[active_setup]
        state="BUY NOW"
    elif hard_ok and best["pass_ratio"]>=0.78:
        state="PRE-IGNITION"
    elif m["micro_ready"] and (best["pass_ratio"]>=0.55 or sd["ema50_near"] or sd["ema200_near"] or sd["breakout_near"]):
        state="WATCH"
    else:
        state="REJECT"
    hard_pass=sum(hard.values()); hard_ratio=safe_div(hard_pass,len(hard))
    score=best["pass_ratio"]*85+hard_ratio*15
    score += min(max(anomaly.get("anomaly_score",0.0),0.0),20.0)*0.20
    score -= 20 if sd["anti_chase"] else 0
    failed_hard=[k for k,v in hard.items() if not v]
    failed_setup=[k for k,v in best["gates"].items() if not v]
    row={
        "symbol":symbol,"state":state,"score":round(score,2),"price":m["last_price"] or sd["price"],
        "active_setup":active_setup,"hard_safety_status":{k:("PASS" if v else "FAIL") for k,v in hard.items()},
        "hard_safety_all_aligned":hard_ok,"setup_results":setup_results,
        "failed_hard":failed_hard,"failed_setup":failed_setup,"mandatory_all_aligned":buy,
        "mandatory_pass_count":hard_pass+best["pass_count"],"mandatory_total":len(hard)+best["total"],
        "mandatory_pass_ratio":round(safe_div(hard_pass+best["pass_count"],len(hard)+best["total"]),4),
        "micro_confirmation_count":best["pass_count"],"confirmation_count":len(sd["structure_confirmations"])+hard_pass+best["pass_count"],
        "ma_harmony":sd["ma_harmony"],"ma_reclaim_regime":sd["ma_reclaim_regime"],"anti_chase":sd["anti_chase"],
        "volume_acceleration":sd["volume_acceleration"],"volume_acceleration_15m":sd["volume_acceleration_15m"],
        "breakout_distance_pct":sd["breakout_distance_pct"],"fast_anomaly":anomaly,
        "micro_ready":m["micro_ready"],"sequence_verified":m["sequence_verified"],"book_sequence_verified":m["book_sequence_verified"],
        "cvd_quote_60s":m["cvd_quote_60s"],"cvd_acceleration":m["cvd_acceleration"],"aggressive_buy_ratio":m["aggressive_buy_ratio"],
        "ofi":m["ofi"],"ofi_acceleration":m["ofi_acceleration"],"ofi_persistence":m["ofi_persistence"],"obi":m["obi"],
        "ask_depletion":m["ask_depletion"],"bid_depletion":m["bid_depletion"],"trade_count_60s":m["trade_count_60s"],
        "trade_acceleration":m["trade_acceleration"],"trade_size_shift":m["trade_size_shift"],"relative_volume_10s":m["relative_volume_10s"],
        "relative_volume_30s":m["relative_volume_30s"],"relative_ranks":m["relative_ranks"],"vwap_60s":m["vwap_60s"],
        "vwap_reclaim":m["vwap_reclaim"],"spread_bps":m["spread_bps"],"slippage_bps":m["slippage_bps"],
        "flow_persistence":m["flow_persistence"],"quote_volume_24h":sd["quote_volume_24h"],"updated_ms":now_ms(),
    }
    record_diagnostics(symbol,active_setup,best["gates"],state,row["price"])
    maybe_record_outcome_candidate(row)
    return row


STATE_PRIORITY={"BUY NOW":4,"PRE-IGNITION":3,"WATCH":2,"REJECT":1}

def ranked_results(limit: int=RETURN_LIMIT) -> List[dict]:
    rows=[]
    for symbol in structure:
        row=evaluate_symbol(symbol)
        if row: rows.append(row)
    rows.sort(key=lambda r:(STATE_PRIORITY.get(r["state"],0),r["score"],r["quote_volume_24h"]),reverse=True)
    return rows[:limit]


async def structure_batch(symbols: List[str]) -> None:
    if session is None: return
    sem=asyncio.Semaphore(8)
    async def worker(symbol):
        async with sem:
            result=await load_structure(session,symbol)
            if result: structure[symbol]=result
    await asyncio.gather(*(worker(s) for s in symbols),return_exceptions=True)


async def refresh_structure() -> None:
    global selected_micro_symbols,last_structure_refresh,last_micro_pool_change,scanner_ready
    if session is None: return
    print("Î¨-V10.1: loading Binance universe...",flush=True)
    universe=await get_exchange_symbols(session)
    symbols=[s for s,_ in universe[:TOP_STRUCTURE_UNIVERSE]]
    print(f"Î¨-V10.1: analysing {len(symbols)} liquid Spot USDT markets...",flush=True)
    active=set(symbols)
    for old in list(structure):
        if old not in active: structure.pop(old,None)
    await structure_batch(symbols)
    candidates=[]
    for symbol in symbols:
        row=structure.get(symbol)
        if not row: continue
        proximity=min(abs(row["distance_ema50_atr"]),abs(row["distance_ema200_atr"]))
        score=len(row["structure_confirmations"])*8+max(0,20-proximity*5)+min(row["volume_acceleration_15m"]*8,16)
        if row["compression"]: score+=10
        if row["breakout_near"] or row["breakout"] : score+=12
        if row["ma_regime"]: score+=15
        if row["anti_chase"]: score-=25
        candidates.append((score,row["quote_volume_24h"],symbol))
    candidates.sort(reverse=True)
    fast_ranked=sorted((x for x in anomaly_state.values() if x.get("symbol") in active),key=lambda x:x.get("anomaly_score",0),reverse=True)
    fast_slots=min(ANOMALY_PROMOTION_SLOTS,MICRO_UNIVERSE_SIZE)
    new=[x["symbol"] for x in fast_ranked[:fast_slots]]
    for _,_,symbol in candidates:
        if symbol not in new: new.append(symbol)
        if len(new)>=MICRO_UNIVERSE_SIZE: break
    selected_micro_symbols=new[:MICRO_UNIVERSE_SIZE]
    last_micro_pool_change=time.time()
    for symbol in selected_micro_symbols: ensure_micro_state(symbol)
    last_structure_refresh=time.time(); scanner_ready=True
    print("Î¨-V10.1 STRUCTURE READY",flush=True)
    print("Micro universe:",", ".join(selected_micro_symbols),flush=True)


async def refresh_anomalies() -> None:
    global last_anomaly_refresh,last_micro_pool_change,selected_micro_symbols
    if session is None: return
    universe=await get_exchange_symbols(session)
    symbols=[s for s,_ in universe[:TOP_STRUCTURE_UNIVERSE]]
    sem=asyncio.Semaphore(24)
    async def worker(symbol):
        async with sem: return await load_fast_anomaly(session,symbol)
    rows=await asyncio.gather(*(worker(s) for s in symbols),return_exceptions=True)
    for row in rows:
        if not isinstance(row,dict): continue
        anomaly_state[row["symbol"]]=row
        if row["return_15m_pct"]>=MISSED_MOVE_15M_PCT and row["symbol"] not in selected_micro_symbols:
            missed_moves.append({"timestamp_ms":now_ms(),"symbol":row["symbol"],"return_15m_pct":row["return_15m_pct"],"reason":"NOT_IN_MICRO_POOL_BEFORE_MOVE","anomaly_score":row["anomaly_score"]})
    ranked=sorted(anomaly_state.values(),key=lambda x:x.get("anomaly_score",0),reverse=True)
    fast=[x["symbol"] for x in ranked[:min(ANOMALY_PROMOTION_SLOTS,MICRO_UNIVERSE_SIZE)]]
    now_s=time.time()
    # Diff-depth books require a fresh REST snapshot whenever the combined
    # stream reconnects. Hold the pool briefly so a 15s anomaly refresh does
    # not force 80 snapshot bootstraps every cycle.
    if not selected_micro_symbols or now_s-last_micro_pool_change>=MICRO_POOL_MIN_HOLD_SECONDS:
        merged=fast+[x for x in selected_micro_symbols if x not in fast]
        proposed=merged[:MICRO_UNIVERSE_SIZE]
        if set(proposed)!=set(selected_micro_symbols):
            selected_micro_symbols=proposed
            last_micro_pool_change=now_s
            for symbol in selected_micro_symbols: ensure_micro_state(symbol)
    last_anomaly_refresh=now_s


async def anomaly_refresh_loop():
    global last_error
    while True:
        try: await refresh_anomalies()
        except asyncio.CancelledError: raise
        except Exception as exc:
            last_error=f"ANOMALY: {type(exc).__name__}: {exc}"; print(last_error,flush=True)
        await asyncio.sleep(ANOMALY_REFRESH_SECONDS)


async def structure_refresh_loop():
    global last_error
    while True:
        try:
            await asyncio.sleep(STRUCTURE_REFRESH_SECONDS); await refresh_structure()
        except asyncio.CancelledError: raise
        except Exception as exc:
            last_error=f"STRUCTURE: {type(exc).__name__}: {exc}"; print(last_error,flush=True)


async def websocket_loop():
    global websocket_connected,websocket_symbols,last_error
    while True:
        try:
            symbols=list(selected_micro_symbols)
            if not symbols:
                await asyncio.sleep(1)
                continue
            streams=[]
            for symbol in symbols:
                lower=symbol.lower()
                streams.extend([f"{lower}@aggTrade",f"{lower}@depth20@100ms"])
            url=f"{WS_BASE}/stream?streams={'/'.join(streams)}"
            print(f"Î¨-V10.1 WebSocket connecting for {len(symbols)} symbols (aggTrade + depth20)...",flush=True)
            assert session is not None
            async with session.ws_connect(url,heartbeat=None,receive_timeout=90,max_msg_size=0) as ws:
                websocket_connected=True
                websocket_symbols=symbols
                last_error=None
                for symbol in symbols:
                    s=ensure_micro_state(symbol)
                    s["book_buffer"].clear()
                    s["book_snapshot_ready"]=False
                    s["book_sequence_ok"]=True
                    s["book_sequence_samples"]=0
                    s["book_resyncing"]=False
                    s["last_book_update_id"]=None
                print("Î¨-V10.1 WebSocket connected (REST-free depth20).",flush=True)
                async for message in ws:
                    if set(selected_micro_symbols)!=set(symbols):
                        print("Î¨-V10.1 micro universe changed; reconnecting WebSocket.",flush=True)
                        break
                    if message.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        stream_name=payload.get("stream","")
                        data=payload.get("data",{})
                        if not stream_name or not isinstance(data,dict):
                            continue
                        symbol=stream_name.split("@")[0].upper()
                        if "@aggTrade" in stream_name:
                            process_agg_trade(symbol,data)
                        elif "@depth20" in stream_name:
                            process_partial_depth_snapshot(symbol,data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_error=f"WEBSOCKET: {type(exc).__name__}: {exc}"
            print(last_error,flush=True)
        finally:
            websocket_connected=False
            websocket_symbols=[]
        await asyncio.sleep(3)


async def print_loop():
    while True:
        await asyncio.sleep(PRINT_SECONDS)
        try:
            resolve_outcomes(); results=ranked_results(10)
            print("\n==================================================",flush=True)
            print("Î¨-V10.1 LIVE TOP 10",flush=True)
            print("==================================================",flush=True)
            for i,row in enumerate(results,1):
                print(f"{i:02d}. {row['symbol']:12s} {row['state']:14s} score={row['score']:6.2f} setup={row['active_setup'][:10]:10s} conf={row['confirmation_count']} micro={row['micro_confirmation_count']} OFI={row['ofi']:+.3f} OBI={row['obi']:+.3f} buy={row['aggressive_buy_ratio']:.2%} ready={row['micro_ready']}",flush=True)
        except Exception as exc:
            print(f"[PRINT ERROR] {type(exc).__name__}: {exc}",flush=True)


async def liveness(request):
    """Lightweight process-liveness endpoint for platform deployment probes.

    This deliberately does not evaluate scanner readiness or trading gates.
    /health remains the full fail-closed scanner diagnostic endpoint.
    """
    return web.json_response({
        "ok": True,
        "service": "psi-v10-live-scanner",
        "liveness": "UP",
        "uptime_seconds": int(time.time() - scanner_started_at),
    })


async def health(request):
    return web.json_response({
        "ok":True,"service":"psi-v10-live-scanner","version":"10.1","scanner_ready":scanner_ready,
        "rest_connected":rest_connected,"websocket_connected":websocket_connected,"rest_base":REST_BASE,"ws_base":WS_BASE,
        "structure_symbols":len(structure),"micro_symbols":len(selected_micro_symbols),"anomaly_symbols":len(anomaly_state),
        "websocket_symbols":len(websocket_symbols),"strict_uk_allowlist_enabled":bool(UK_SYMBOLS),
        "uptime_seconds":int(time.time()-scanner_started_at),
        "last_structure_refresh_age_seconds":int(time.time()-last_structure_refresh) if last_structure_refresh else None,
        "last_error":last_error,"endpoints":{"health":"/health","scan":"/scan","diagnostics":"/diagnostics"}
    })


async def scan_endpoint(request):
    try: limit=max(1,min(int(request.query.get("limit",RETURN_LIMIT)),50))
    except ValueError: limit=RETURN_LIMIT
    resolve_outcomes(); results=ranked_results(limit); states=Counter(r["state"] for r in results)
    return web.json_response({
        "ok":True,"scanner":"Î¨-V10.1","version":"10.1","source":"Binance public Spot market data",
        "timeframe":"1m anomaly + 15m impulse + 1h/4h structure + live microstructure",
        "buy_policy":"ALL_HARD_SAFETY_GATES_PLUS_ALL_GATES_OF_ONE_VERIFIED_SETUP",
        "setups":["COMPRESSION_BREAKOUT","MA_RETEST_RECLAIM","BREAKOUT_RETEST_CONTINUATION"],
        "signal_memory_seconds":{"structure":TTL_STRUCTURE_MS//1000,"ma":TTL_MA_MS//1000,"breakout":TTL_BREAKOUT_MS//1000,"anomaly":TTL_ANOMALY_MS//1000,"micro":TTL_MICRO_MS//1000},
        "order_book":"Binance diff-depth with REST snapshot sequence reconciliation",
        "relative_microstructure":"rolling per-symbol percentile baselines with warm-up fallbacks",
        "scanner_ready":scanner_ready,"websocket_connected":websocket_connected,"strict_uk_allowlist_enabled":bool(UK_SYMBOLS),
        "universe_size":len(structure),"micro_universe_size":len(selected_micro_symbols),"state_counts":dict(states),"returned":len(results),
        "anomaly_universe_size":len(anomaly_state),"last_anomaly_refresh_age_seconds":int(time.time()-last_anomaly_refresh) if last_anomaly_refresh else None,
        "missed_moves":list(missed_moves)[-50:],"results":results,"generated_ms":now_ms()
    })


async def diagnostics_endpoint(request):
    top_failures=diagnostic_failures.most_common(40)
    return web.json_response({
        "ok":True,"version":"10.1","setup_evaluations":dict(diagnostic_setup_evals),"setup_buys":dict(diagnostic_setup_buys),
        "top_failed_gates":[{"gate":k,"count":v} for k,v in top_failures],
        "recent_events":list(diagnostic_events)[-100:],"pending_outcomes":pending_outcomes[-100:],"resolved_outcomes":list(resolved_outcomes)[-100:]
    })


async def start_http_server():
    app=web.Application()
    # The public homepage should show the live scanner app, not the JSON status
    # endpoint. All API and health paths keep their existing handlers.
    dashboard_handler = globals().get("fast_dashboard_handler")
    app.router.add_get("/", dashboard_handler if callable(dashboard_handler) else health)
    app.router.add_get("/live",liveness); app.router.add_get("/health",health); app.router.add_get("/scan",scan_endpoint); app.router.add_get("/diagnostics",diagnostics_endpoint)
    # Only registered when the latest installed read-only signal worker supplies handlers.
    # Never allow a stale legacy /scan response to masquerade as execution-ready.
    if callable(globals().get("fast_signal_handler")):
        app.router.add_get("/signals/live",globals()["fast_signal_handler"])
    if callable(globals().get("fast_events_handler")):
        app.router.add_get("/signals/events",globals()["fast_events_handler"])
    if callable(globals().get("fast_quote_handler")):
        app.router.add_get("/signals/quote",globals()["fast_quote_handler"])
    if callable(globals().get("fast_dashboard_handler")):
        app.router.add_get("/signals/board",globals()["fast_dashboard_handler"])
    runner=web.AppRunner(app); await runner.setup(); site=web.TCPSite(runner,"0.0.0.0",PORT); await site.start()
    print(f"Î¨-V10.1 HTTP listening on port {PORT}",flush=True); return runner


async def initialise():
    print("Î¨-V10.1 INITIALISING...",flush=True); print(f"REST: {REST_BASE}",flush=True); print(f"WS:   {WS_BASE}",flush=True)
    await refresh_anomalies(); await refresh_structure(); print("Î¨-V10.1 INITIALISED.",flush=True)


async def main():
    global session,last_error
    session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),connector=aiohttp.TCPConnector(limit=120,ttl_dns_cache=300),headers={"User-Agent":USER_AGENT})
    runner=await start_http_server(); tasks=[]
    try:
        try: await initialise()
        except Exception as exc:
            last_error=f"INITIALISE: {type(exc).__name__}: {exc}"; print(last_error,flush=True)
        tasks=[asyncio.create_task(structure_refresh_loop()),asyncio.create_task(anomaly_refresh_loop()),asyncio.create_task(websocket_loop()),asyncio.create_task(print_loop())]
        await asyncio.gather(*tasks)
    finally:
        for task in tasks: task.cancel()
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)
        await runner.cleanup()
        if session: await session.close()


if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Î¨-V10.1 stopped.",flush=True)
