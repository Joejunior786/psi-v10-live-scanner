import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict

import psi_v11_5_entry as legacy

app = legacy.app
q = legacy.q
base = legacy.base

VERSION = "12.0.1-hydration-fix"

# ---------------------------------------------------------------------------
# V12 mandate
# ---------------------------------------------------------------------------
# The inherited V10/V11 stack remains the market-data/discovery/diagnostic
# layer. It no longer owns BUY authority. V12 recognises independent setup
# families; each family has its own mandatory confirmations.
#
# Signal colours/states:
#   🟢 BUY   = setup-specific confirmation complete
#   🟠 ARMED = legitimate setup is close, confirmation still required
#   🟡 WATCH = interesting structure/area, not ready
#
# Candle/volume setups only require fresh candle data for their own timeframes.
# Microstructure setups still fail closed when live micro data is unavailable.

# Legacy modules continue producing discovery/microstructure telemetry, but
# their BUY/PRE labels are non-authoritative. Only this V12 layer is exposed
# through the public /scan endpoint as signal authority.
EMA_TOUCH_ATR = float(os.getenv("PSI_V12_EMA_TOUCH_ATR", "0.55"))
EMA_NEAR_ATR = float(os.getenv("PSI_V12_EMA_NEAR_ATR", "0.90"))
WEEKLY_TOUCH_ATR = float(os.getenv("PSI_V12_WEEKLY_TOUCH_ATR", "0.80"))
BUY_RATIO_MIN = float(os.getenv("PSI_V12_BUY_RATIO_MIN", "0.54"))
BUY_VOLUME_RATIO_MIN = float(os.getenv("PSI_V12_BUY_VOLUME_RATIO_MIN", "1.05"))
BREAKOUT_VOLUME_RATIO = float(os.getenv("PSI_V12_BREAKOUT_VOLUME_RATIO", "1.35"))
LOW_LIQUIDITY_QV_MAX = float(os.getenv("PSI_V12_LOW_LIQ_QV_MAX", "50000000"))
ANTI_CHASE_ATR = float(os.getenv("PSI_V12_ANTI_CHASE_ATR", "0.65"))
ANTI_CHASE_PCT = float(os.getenv("PSI_V12_ANTI_CHASE_PCT", "1.5"))
ROTATION_SLOTS = max(4, int(os.getenv("PSI_V12_ROTATION_SLOTS", "4")))
PRIORITY_SLOTS = max(4, int(os.getenv("PSI_V12_PRIORITY_SLOTS", "4")))
LOOP_SECONDS = max(8.0, float(os.getenv("PSI_V12_LOOP_SECONDS", "15")))
FETCH_CONCURRENCY = max(6, min(int(os.getenv("PSI_V12_FETCH_CONCURRENCY", "12")), 16))
MAX_INFLIGHT_SYMBOLS = max(24, min(int(os.getenv("PSI_V12_MAX_INFLIGHT_SYMBOLS", "48")), 80))
BOOTSTRAP_SYMBOLS_PER_CYCLE = max(8, min(int(os.getenv("PSI_V12_BOOTSTRAP_SYMBOLS_PER_CYCLE", "24")), 48))
ACTIVE_SYMBOLS_PER_CYCLE = max(4, min(int(os.getenv("PSI_V12_ACTIVE_SYMBOLS_PER_CYCLE", "8")), 16))
MAX_BOARD_PER_STATE = max(5, int(os.getenv("PSI_V12_MAX_BOARD_PER_STATE", "20")))

# 210 candles are enough for EMA/SMA200 + previous-value calculation while
# reducing payload size versus the old 260-candle hydration.
TF_LIMIT = {"1h": 210, "4h": 210, "1d": 210, "1w": 210}

# Structural cache accumulates across the entire 403-symbol universe. Active
# candidates refresh much faster, but broad coverage is never destroyed just
# because an hourly candle cache is older than 75 seconds.
TF_TTL = {"1h": 1800.0, "4h": 7200.0, "1d": 21600.0, "1w": 86400.0}
ACTIVE_TF_TTL = {"1h": 90.0, "4h": 300.0, "1d": 900.0, "1w": 3600.0}
STATE_RANK = {"BUY": 3, "ARMED": 2, "WATCH": 1}
STATE_EMOJI = {"BUY": "🟢", "ARMED": "🟠", "WATCH": "🟡"}

_cache = defaultdict(dict)
_results = {}
_cycle = 0
_cursor = 0
_last_board_print = 0.0
_stats = defaultdict(int)

# Dedicated V12 Binance Spot WS-API connection for historical candles. This
# prevents legacy recovery/structure traffic from starving the new strategy
# engines and avoids dependence on Railway REST routing.
_v12_ws_conn = None
_v12_ws_ready = None
_v12_ws_lock = None
_v12_ws_gate = None
_v12_ws_pending = {}
_v12_ws_id = 0
V12_WS_API_URL = os.getenv("PSI_V12_WS_API_URL", "wss://ws-api.binance.com:443/ws-api/v3")


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d


def cl(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if b else 0.0


def avg(xs):
    xs = list(xs)
    return sum(xs) / len(xs) if xs else 0.0


def ema(values, period):
    if len(values) < period:
        return None
    out = sum(values[:period]) / period
    k = 2.0 / (period + 1.0)
    for v in values[period:]:
        out = v * k + out * (1.0 - k)
    return out


def sma(values, period):
    if len(values) < period:
        return None
    return avg(values[-period:])


def atr_rows(rows, period=14):
    if len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        h = f(rows[i][2]); l = f(rows[i][3]); pc = f(rows[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return avg(trs[-period:]) if len(trs) >= period else None


def rolling_vwap(rows, n=20):
    if len(rows) < n:
        return None
    num = den = 0.0
    for r in rows[-n:]:
        h, l, c, v = f(r[2]), f(r[3]), f(r[4]), f(r[5])
        typical = (h + l + c) / 3.0
        num += typical * v
        den += v
    return num / den if den else None


def _ema_prev(values, period):
    return ema(values[:-1], period) if len(values) > period else None


def snap(rows):
    if not isinstance(rows, list) or len(rows) < 55:
        return None
    # Binance includes the currently-forming candle as the last row.
    closed = rows[:-1] if len(rows) > 1 else rows
    if len(closed) < 50:
        return None

    closes = [f(r[4]) for r in closed]
    opens = [f(r[1]) for r in closed]
    highs = [f(r[2]) for r in closed]
    lows = [f(r[3]) for r in closed]
    vols = [f(r[5]) for r in closed]
    current = f(rows[-1][4], closes[-1])
    last = closed[-1]
    prev = closed[-2] if len(closed) >= 2 else last

    e50 = ema(closes, 50)
    e200 = ema(closes, 200)
    s50 = sma(closes, 50)
    s200 = sma(closes, 200)
    pe50 = _ema_prev(closes, 50)
    pe200 = _ema_prev(closes, 200)
    ps50 = sma(closes[:-1], 50) if len(closes) > 50 else s50
    ps200 = sma(closes[:-1], 200) if len(closes) > 200 else s200
    a = atr_rows(closed, 14)

    lv = vols[-1] if vols else 0.0
    vol_base = avg(vols[-21:-1]) if len(vols) >= 21 else avg(vols[:-1])
    vol_ratio = lv / vol_base if vol_base > 0 else 0.0

    def br(r):
        v = f(r[5]); tb = f(r[9]) if len(r) > 9 else 0.0
        return tb / v if v > 0 else 0.5

    buy_ratio = br(last)
    buy3 = avg(br(r) for r in closed[-3:])
    o, h, l, c = f(last[1]), f(last[2]), f(last[3]), f(last[4])
    rng = max(h - l, 1e-12)
    lower_wick = max(0.0, min(o, c) - l) / rng
    upper_wick = max(0.0, h - max(o, c)) / rng
    close_strength = (c - l) / rng

    std20 = statistics.pstdev(closes[-20:]) if len(closes) >= 20 else 0.0
    mean20 = avg(closes[-20:])
    bb_width = (4.0 * std20 / mean20) if mean20 > 0 else 0.0
    std20_prev = statistics.pstdev(closes[-21:-1]) if len(closes) >= 21 else std20
    mean20_prev = avg(closes[-21:-1]) if len(closes) >= 21 else mean20
    bb_prev = (4.0 * std20_prev / mean20_prev) if mean20_prev > 0 else bb_width

    res20 = max(highs[-21:-1]) if len(highs) >= 21 else max(highs)
    sup20 = min(lows[-21:-1]) if len(lows) >= 21 else min(lows)
    res60 = max(highs[-61:-1]) if len(highs) >= 61 else max(highs)
    sup60 = min(lows[-61:-1]) if len(lows) >= 61 else min(lows)
    range_high = max(highs[-90:]) if len(highs) >= 20 else max(highs)
    range_low = min(lows[-90:]) if len(lows) >= 20 else min(lows)
    rising_lows = len(lows) >= 4 and lows[-1] > lows[-2] > lows[-3]
    falling_vol = len(vols) >= 8 and avg(vols[-3:]) < avg(vols[-8:-3])

    return {
        "current": current,
        "close": c,
        "prev_close": f(prev[4]),
        "open": o,
        "high": h,
        "low": l,
        "ema50": e50,
        "ema200": e200,
        "sma50": s50,
        "sma200": s200,
        "prev_ema50": pe50,
        "prev_ema200": pe200,
        "prev_sma50": ps50,
        "prev_sma200": ps200,
        "atr": a,
        "volume_ratio": vol_ratio,
        "buy_ratio": buy_ratio,
        "buy_ratio_3": buy3,
        "lower_wick": lower_wick,
        "upper_wick": upper_wick,
        "close_strength": close_strength,
        "bullish": c > o,
        "bb_width": bb_width,
        "bb_width_prev": bb_prev,
        "vwap20": rolling_vwap(closed, 20),
        "res20": res20,
        "sup20": sup20,
        "res60": res60,
        "sup60": sup60,
        "range_high": range_high,
        "range_low": range_low,
        "rising_lows": rising_lows,
        "falling_volume": falling_vol,
        "rows": closed,
    }


def buying(s, strong=False):
    if not s:
        return False
    br = f(s.get("buy_ratio"), 0.5)
    b3 = f(s.get("buy_ratio_3"), 0.5)
    vr = f(s.get("volume_ratio"))
    bull = bool(s.get("bullish"))
    if strong:
        return (br >= 0.57 and vr >= 1.15) or (b3 >= 0.56 and vr >= 1.25 and bull)
    return (br >= BUY_RATIO_MIN and vr >= BUY_VOLUME_RATIO_MIN) or (b3 >= 0.53 and vr >= 1.20 and bull)


def zone(s, mult=EMA_TOUCH_ATR, pct_floor=0.0025):
    if not s:
        return 0.0
    p = f(s.get("current"))
    a = f(s.get("atr"))
    return max(a * mult, p * pct_floor)


def near_level(s, level, mult=EMA_NEAR_ATR):
    if not s or not level:
        return False
    z = zone(s, mult)
    return min(abs(f(s.get("current")) - level), abs(f(s.get("low")) - level)) <= z


def rejection(s, level, mult=EMA_TOUCH_ATR, require_buy=True):
    if not s or not level:
        return False
    z = zone(s, mult)
    touched = f(s.get("low")) <= level + z and f(s.get("high")) >= level - z
    reclaimed = f(s.get("close")) >= level - z * 0.15
    candle = f(s.get("lower_wick")) >= 0.24 and f(s.get("close_strength")) >= 0.58
    buy_ok = buying(s) if require_buy else True
    return touched and reclaimed and candle and buy_ok


def cross_up(s, fast_key="ema50", slow_key="ema200"):
    if not s:
        return False
    pf = f(s.get("prev_" + fast_key))
    ps = f(s.get("prev_" + slow_key))
    cf = f(s.get(fast_key))
    cs = f(s.get(slow_key))
    return pf > 0 and ps > 0 and cf > 0 and cs > 0 and pf <= ps and cf > cs


def bullish_regime(s):
    if not s:
        return False
    e50 = f(s.get("ema50")); e200 = f(s.get("ema200"))
    return e50 > 0 and e200 > 0 and e50 > e200 and f(s.get("close")) >= e50


def emit(setups, name, state, strength, tf, reason, entry=0.0, invalidation=0.0,
         support=0.0, target_hints=None, requires_micro=False, confirmed=None):
    state = state if state in STATE_RANK else "WATCH"
    setups.append({
        "name": name,
        "state": state,
        "strength": cl(float(strength), 0.0, 100.0),
        "timeframe": tf,
        "reason": reason,
        "entry": f(entry),
        "invalidation": f(invalidation),
        "support": f(support),
        "target_hints": [f(x) for x in (target_hints or []) if f(x) > 0],
        "requires_micro": bool(requires_micro),
        "confirmed": list(confirmed or []),
    })


def _closest_level(current, levels):
    good = [(abs(current - level), label, level) for label, level in levels if f(level) > 0]
    return min(good, default=(999999.0, "", 0.0))


def _micro_confirm(sym):
    row = q.latest.get(sym) or {}
    now = int(time.time() * 1000)
    trade_ms = int(f(row.get("last_trade_ms"), f((app.micro_state.get(sym) or {}).get("last_trade_ms"))))
    book_ms = int(f(row.get("last_book_ms"), f((app.micro_state.get(sym) or {}).get("last_book_ms"))))
    fresh = trade_ms > 0 and book_ms > 0 and now - trade_ms <= 15000 and now - book_ms <= 7000
    buy = f(row.get("aggressive_buy_ratio"), f(row.get("buy_ratio"), 0.5))
    cvd = f(row.get("cvd_acceleration"), f(row.get("cvd_1s"), 0.0))
    ofi = f(row.get("ofi"), f(row.get("order_flow_imbalance"), 0.0))
    return {
        "fresh": fresh,
        "buy_ratio": buy,
        "cvd": cvd,
        "ofi": ofi,
        "positive": fresh and (buy >= 0.56 or cvd >= 0.08 or ofi > 0),
    }


def evaluate_symbol(sym):
    c = _cache.get(sym) or {}
    s1 = (c.get("1h") or {}).get("snap")
    s4 = (c.get("4h") or {}).get("snap")
    sd = (c.get("1d") or {}).get("snap")
    sw = (c.get("1w") or {}).get("snap")
    if not s1 or not s4 or not sd:
        return None

    current = f(s1.get("current"), f(s4.get("current"), f(sd.get("current"))))
    if current <= 0:
        return None
    a1 = f(s1.get("atr")); a4 = f(s4.get("atr"), a1); ad = f(sd.get("atr"), a4)
    setups = []
    micro = _micro_confirm(sym)
    quote_vol = f((app.symbol_meta.get(sym) or {}).get("quote_volume_24h"),
                  f((base.meta.get(sym) or {}).get("quote_volume_24h")) if hasattr(base, "meta") else 0.0)

    # 1-3. Golden cross family. EMA is primary; SMA is secondary confluence.
    for tf_name, s, tf_label in (("DAILY", sd, "1D"), ("4H", s4, "4H")):
        if cross_up(s):
            conf = ["EMA50_CROSS_EMA200"]
            if f(s.get("sma50")) > f(s.get("sma200")) > 0:
                conf.append("SMA50_ABOVE_SMA200")
            if buying(s):
                emit(setups, f"{tf_name}_GOLDEN_CROSS", "BUY", 82 + 4 * len(conf), tf_label,
                     "50 EMA crossed above 200 EMA with bullish price/volume confirmation",
                     entry=f(s.get("close")), invalidation=min(f(s.get("ema50")), f(s.get("ema200"))) - 0.45 * f(s.get("atr")),
                     support=f(s.get("ema200")), target_hints=[f(s.get("res20")), f(s.get("res60"))],
                     confirmed=conf + ["BUY_VOLUME"])
            else:
                emit(setups, f"{tf_name}_GOLDEN_CROSS", "ARMED", 70, tf_label,
                     "50 EMA crossed above 200 EMA; waiting for bullish hold/retest and buy volume",
                     entry=max(f(s.get("ema50")), f(s.get("ema200"))), support=f(s.get("ema200")),
                     target_hints=[f(s.get("res20")), f(s.get("res60"))], confirmed=conf)

    if cross_up(s1):
        state = "BUY" if (rejection(s1, f(s1.get("ema50"))) or rejection(s1, f(s1.get("ema200")))) and buying(s1) else "ARMED"
        emit(setups, "1H_GOLDEN_CROSS", state, 78 if state == "BUY" else 66, "1H",
             "1H 50/200 EMA bullish cross; BUY requires successful retest/reclaim and buying",
             entry=f(s1.get("close")) if state == "BUY" else max(f(s1.get("ema50")), f(s1.get("ema200"))),
             invalidation=min(x for x in [f(s1.get("ema50")), f(s1.get("ema200"))] if x > 0) - 0.5 * a1,
             support=f(s1.get("ema200")), target_hints=[f(s4.get("res20")), f(sd.get("res20"))])

    # 4-6. 200 EMA reactions.
    for label, s, tf_label, buy_direct in (("DAILY", sd, "1D", True), ("4H", s4, "4H", True), ("1H", s1, "1H", False)):
        level = f(s.get("ema200"))
        if level <= 0:
            continue
        if rejection(s, level):
            emit(setups, f"{label}_EMA200_REJECTION", "BUY", 86 if buy_direct else 79, tf_label,
                 f"{label} rejected/reclaimed the 200 EMA with positive buying",
                 entry=f(s.get("close")), invalidation=level - 0.65 * f(s.get("atr")),
                 support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))],
                 confirmed=["EMA200_TOUCH", "REJECTION", "BUY_VOLUME"])
        elif near_level(s, level):
            emit(setups, f"{label}_EMA200_REACTION", "ARMED", 62 if label == "1H" else 68, tf_label,
                 f"{label} is touching/near the 200 EMA; waiting for confirmed rejection and buying",
                 entry=level, support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))])

    # 7-10. Weekly MA interactions + Weekly/Daily crossover.
    if sw:
        weekly_levels = [("WEEKLY_EMA50", f(sw.get("ema50"))), ("WEEKLY_EMA200", f(sw.get("ema200")))]
        weekly_levels = [(n, v) for n, v in weekly_levels if v > 0]
        for label, s, tf_label, armed_only in (("1H", s1, "1H", True), ("4H", s4, "4H", False), ("DAILY", sd, "1D", False)):
            _, lname, level = _closest_level(current, weekly_levels)
            if level <= 0:
                continue
            if rejection(s, level, WEEKLY_TOUCH_ATR):
                emit(setups, f"{label}_WEEKLY_MA_REJECTION", "BUY", 84 if label == "1H" else 89 if label == "4H" else 93, tf_label,
                     f"{label} touched {lname} and confirmed rejection with buying",
                     entry=f(s.get("close")), invalidation=level - 0.70 * max(f(s.get("atr")), a4),
                     support=level, target_hints=[f(sd.get("res20")), f(sd.get("res60")), f(sw.get("res20"))],
                     confirmed=[lname, "REJECTION", "BUY_VOLUME"])
            elif near_level(s, level, WEEKLY_TOUCH_ATR):
                emit(setups, f"{label}_WEEKLY_MA_TOUCH", "ARMED", 72 if armed_only else 76, tf_label,
                     f"{label} is touching/near {lname}; BUY only after rejection/buying confirmation",
                     entry=level, support=level, target_hints=[f(sd.get("res20")), f(sw.get("res20"))],
                     confirmed=[lname])

        # Compare equivalent Weekly and Daily EMA values. This is intentionally
        # a user-requested regime crossover, not a textbook single-timeframe cross.
        wd_cross = []
        for p in (50, 200):
            w = f(sw.get(f"ema{p}")); d = f(sd.get(f"ema{p}"))
            pw = f(sw.get(f"prev_ema{p}")); pd = f(sd.get(f"prev_ema{p}"))
            if w > 0 and d > 0 and pw > 0 and pd > 0 and pw <= pd and w > d:
                wd_cross.append(p)
        if wd_cross:
            if buying(sd) and f(sd.get("close")) >= max(f(sw.get(f"ema{p}")) for p in wd_cross):
                emit(setups, "WEEKLY_DAILY_TREND_CROSS", "BUY", 91, "1D",
                     f"Weekly EMA crossed above Daily EMA ({wd_cross}) with Daily bullish confirmation",
                     entry=f(sd.get("close")), invalidation=f(sd.get("sup20")) - 0.4 * ad,
                     support=f(sd.get("sup20")), target_hints=[f(sd.get("res20")), f(sd.get("res60")), f(sw.get("res20"))],
                     confirmed=["WEEKLY_DAILY_CROSS", "DAILY_BUY_CONFIRMATION"])
            else:
                emit(setups, "WEEKLY_DAILY_TREND_CROSS", "ARMED", 74, "1D",
                     f"Weekly/Daily EMA crossover detected ({wd_cross}); waiting for Daily bullish confirmation",
                     entry=f(sd.get("close")), support=f(sd.get("sup20")),
                     target_hints=[f(sd.get("res20")), f(sw.get("res20"))])

    # 11. Multi-timeframe MA confluence.
    levels = [
        ("1H_E50", f(s1.get("ema50"))), ("1H_E200", f(s1.get("ema200"))),
        ("4H_E50", f(s4.get("ema50"))), ("4H_E200", f(s4.get("ema200"))),
        ("D_E50", f(sd.get("ema50"))), ("D_E200", f(sd.get("ema200"))),
    ]
    if sw:
        levels += [("W_E50", f(sw.get("ema50"))), ("W_E200", f(sw.get("ema200")))]
    near_levels = [(n, v) for n, v in levels if v > 0 and abs(current - v) <= max(a4 * 0.85, current * 0.01)]
    if len(near_levels) >= 3:
        centre = avg(v for _, v in near_levels)
        rej = rejection(s4, centre, 0.9) or rejection(sd, centre, 0.9)
        state = "BUY" if rej else "ARMED"
        emit(setups, "MULTI_TIMEFRAME_MA_CONFLUENCE", state, min(98, 72 + 5 * len(near_levels)), "MTF",
             f"{len(near_levels)} MA/support levels cluster in one zone" + (" with bullish rejection" if rej else ""),
             entry=f(s4.get("close")) if rej else centre, invalidation=centre - max(0.7 * a4, current * 0.015),
             support=centre, target_hints=[f(s4.get("res20")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=[n for n, _ in near_levels])

    # 12. 50/200 retest/reclaim in established bullish regimes.
    for label, s, tf_label in (("4H", s4, "4H"), ("DAILY", sd, "1D")):
        if not bullish_regime(s):
            continue
        ma_levels = [("EMA50", f(s.get("ema50"))), ("EMA200", f(s.get("ema200")))]
        _, lname, level = _closest_level(current, [(n, v) for n, v in ma_levels if v > 0])
        if level > 0 and rejection(s, level):
            emit(setups, f"{label}_{lname}_RETEST_RECLAIM", "BUY", 84, tf_label,
                 f"Bullish {label} trend retested and reclaimed {lname}",
                 entry=f(s.get("close")), invalidation=level - 0.55 * f(s.get("atr")),
                 support=level, target_hints=[f(s.get("res20")), f(s.get("res60"))],
                 confirmed=["BULLISH_REGIME", "MA_RETEST", "RECLAIM", "BUY_VOLUME"])
        elif level > 0 and near_level(s, level):
            emit(setups, f"{label}_{lname}_RETEST", "ARMED", 65, tf_label,
                 f"Bullish {label} trend is retesting {lname}", entry=level, support=level,
                 target_hints=[f(s.get("res20")), f(s.get("res60"))])

    # 13. Deep pullback exhaustion, 5%-90%.
    high90 = f(sd.get("range_high"))
    depth = max(0.0, (high90 - current) / high90 * 100.0) if high90 > 0 else 0.0
    if 5.0 <= depth <= 90.0:
        recent_lows = [f(r[3]) for r in sd.get("rows", [])[-8:]]
        failed_new_low = len(recent_lows) >= 4 and recent_lows[-1] >= min(recent_lows[:-1])
        exhausting = bool(sd.get("falling_volume")) or f(sd.get("lower_wick")) >= 0.30 or failed_new_low
        takeover = exhausting and buying(sd) and f(sd.get("close_strength")) >= 0.62
        if takeover:
            emit(setups, "DEEP_PULLBACK_EXHAUSTION", "BUY", min(96, 76 + depth * 0.20), "1D",
                 f"{depth:.1f}% pullback shows seller exhaustion and buyer takeover",
                 entry=f(sd.get("close")), invalidation=f(sd.get("low")) - 0.55 * ad,
                 support=f(sd.get("range_low")), target_hints=[f(sd.get("res20")), (f(sd.get("range_high")) + f(sd.get("range_low"))) / 2.0, f(sd.get("range_high"))],
                 confirmed=["PULLBACK_5_90", "SELLER_EXHAUSTION", "BUYER_TAKEOVER"])
        elif exhausting:
            emit(setups, "DEEP_PULLBACK_EXHAUSTION", "ARMED", min(82, 58 + depth * 0.18), "1D",
                 f"{depth:.1f}% pullback; selling pressure is weakening, waiting for buyer takeover",
                 entry=f(sd.get("close")), support=f(sd.get("range_low")),
                 target_hints=[f(sd.get("res20")), (f(sd.get("range_high")) + f(sd.get("range_low"))) / 2.0])
        else:
            emit(setups, "DEEP_PULLBACK_MONITOR", "WATCH", 48, "1D",
                 f"{depth:.1f}% pullback is inside monitored 5%-90% range; no exhaustion yet",
                 entry=f(sd.get("close")), support=f(sd.get("range_low")),
                 target_hints=[f(sd.get("res20"))])

    # 14. Coiled accumulation. Quote volume is a liquidity proxy, not market cap.
    low_liq_proxy = quote_vol > 0 and quote_vol <= LOW_LIQUIDITY_QV_MAX
    coil = f(s1.get("bb_width")) > 0 and f(s1.get("bb_width")) <= max(0.018, f(s1.get("bb_width_prev")) * 0.90)
    close_to_res = f(s1.get("res20")) > 0 and (f(s1.get("res20")) - current) / current <= 0.018
    unusual_buy = (f(s1.get("buy_ratio_3")) >= 0.57 and f(s1.get("volume_ratio")) >= 1.25) or micro.get("positive")
    if low_liq_proxy and coil and close_to_res:
        if unusual_buy and micro.get("fresh"):
            broke = current >= f(s1.get("res20")) * 0.998
            state = "BUY" if broke else "ARMED"
            emit(setups, "COILED_ACCUMULATION", state, 90 if broke else 78, "1H",
                 "Low-liquidity proxy is extremely coiled with unusual live buying" + (" and breakout pressure" if broke else ""),
                 entry=max(current, f(s1.get("res20"))) if broke else f(s1.get("res20")),
                 invalidation=f(s1.get("sup20")) - 0.35 * a1, support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20")), f(sd.get("res20"))], requires_micro=True,
                 confirmed=["COMPRESSION", "UNUSUAL_BUY_ACTIVITY", "LIVE_MICRO"])
        elif unusual_buy:
            emit(setups, "COILED_ACCUMULATION", "ARMED", 70, "1H",
                 "Coiled low-liquidity proxy shows unusual buying; live micro confirmation is incomplete",
                 entry=f(s1.get("res20")), support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20")), f(sd.get("res20"))], requires_micro=True)
        else:
            emit(setups, "COILED_ACCUMULATION", "WATCH", 56, "1H",
                 "Low-liquidity proxy is extremely coiled near resistance; waiting for unusual buying",
                 entry=f(s1.get("res20")), support=f(s1.get("sup20")),
                 target_hints=[f(s4.get("res20"))], requires_micro=True)

    # 15. Daily range-bottom mean reversion.
    rlo, rhi = f(sd.get("range_low")), f(sd.get("range_high"))
    if rhi > rlo > 0:
        rh = rhi - rlo
        pos = (current - rlo) / rh if rh > 0 else 1.0
        if pos <= 0.12:
            rej = rejection(sd, rlo, 0.9) or (f(sd.get("low")) <= rlo + 0.35 * ad and buying(sd) and f(sd.get("close")) > rlo)
            state = "BUY" if rej else "ARMED"
            emit(setups, "DAILY_RANGE_BOTTOM_REVERSAL", state, 88 if rej else 70, "1D",
                 "Price is defending the bottom of an established Daily range" if rej else "Price is at the bottom of the Daily range; waiting for rejection/buying",
                 entry=f(sd.get("close")) if rej else rlo, invalidation=rlo - 0.55 * ad,
                 support=rlo, target_hints=[rlo + rh * 0.35, rlo + rh * 0.50, rhi],
                 confirmed=["DAILY_RANGE_LOW"] + (["REJECTION", "BUY_VOLUME"] if rej else []))

    # 16. Failed breakdown.
    support = f(s4.get("sup20"))
    if support > 0 and f(s4.get("low")) < support and f(s4.get("close")) > support:
        state = "BUY" if buying(s4) else "ARMED"
        emit(setups, "FAILED_BREAKDOWN_RECLAIM", state, 86 if state == "BUY" else 70, "4H",
             "4H broke below support and reclaimed it" + (" with buying" if state == "BUY" else ""),
             entry=f(s4.get("close")), invalidation=f(s4.get("low")) - 0.35 * a4, support=support,
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["FAILED_BREAKDOWN", "RECLAIM"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 17. Liquidity sweep reversal.
    sweep_level = f(s4.get("sup60"))
    if sweep_level > 0 and f(s4.get("low")) < sweep_level and f(s4.get("close")) > sweep_level and f(s4.get("lower_wick")) >= 0.32:
        state = "BUY" if buying(s4) else "ARMED"
        emit(setups, "LIQUIDITY_SWEEP_REVERSAL", state, 88 if state == "BUY" else 72, "4H",
             "4H swept a prior low and reclaimed the liquidity level",
             entry=f(s4.get("close")), invalidation=f(s4.get("low")) - 0.30 * a4, support=sweep_level,
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["LOW_SWEEP", "RECLAIM"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 18. Compression breakout.
    compression = f(s4.get("bb_width")) > 0 and f(s4.get("bb_width")) < 0.035 and f(s4.get("bb_width")) <= f(s4.get("bb_width_prev")) * 0.95
    near_res = f(s4.get("res20")) > 0 and (f(s4.get("res20")) - current) / current <= 0.02
    breakout = current > f(s4.get("res20")) and f(s4.get("volume_ratio")) >= BREAKOUT_VOLUME_RATIO and buying(s4)
    if breakout:
        emit(setups, "COMPRESSION_BREAKOUT", "BUY", 91, "4H",
             "Compressed 4H structure broke resistance with volume/buying",
             entry=current, invalidation=f(s4.get("res20")) - 0.55 * a4, support=f(s4.get("res20")),
             target_hints=[f(s4.get("res60")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=["COMPRESSION", "BREAKOUT", "BUY_VOLUME"])
    elif compression and near_res:
        emit(setups, "COMPRESSION_BREAKOUT", "ARMED", 74, "4H",
             "4H is tightly compressed beneath resistance; waiting for volume-backed break",
             entry=f(s4.get("res20")), support=f(s4.get("sup20")),
             target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
             confirmed=["COMPRESSION", "NEAR_RESISTANCE"])

    # 19. Breakout retest.
    rows4 = s4.get("rows", [])
    if len(rows4) >= 24:
        prev_res = max(f(r[2]) for r in rows4[-24:-3])
        broke_recently = max(f(r[4]) for r in rows4[-3:-1]) > prev_res
        retest = f(s4.get("low")) <= prev_res + 0.35 * a4 and f(s4.get("close")) >= prev_res
        if broke_recently and retest:
            state = "BUY" if buying(s4) else "ARMED"
            emit(setups, "BREAKOUT_RETEST", state, 90 if state == "BUY" else 75, "4H",
                 "Previous resistance was broken and is being retested as support",
                 entry=f(s4.get("close")), invalidation=prev_res - 0.55 * a4, support=prev_res,
                 target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                 confirmed=["BREAKOUT", "RETEST"] + (["BUY_VOLUME"] if state == "BUY" else []))

    # 20. VWAP reclaim.
    vw = f(s1.get("vwap20"))
    if vw > 0:
        reclaimed = f(s1.get("prev_close")) <= vw and f(s1.get("close")) > vw
        held = f(s1.get("low")) <= vw + 0.30 * a1 and f(s1.get("close")) > vw
        if reclaimed and buying(s1):
            state = "BUY" if held else "ARMED"
            emit(setups, "VWAP_RECLAIM", state, 80 if state == "BUY" else 67, "1H",
                 "1H reclaimed VWAP with buying" + (" and held the retest" if held else ""),
                 entry=f(s1.get("close")), invalidation=vw - 0.55 * a1, support=vw,
                 target_hints=[f(s1.get("res20")), f(s4.get("res20"))],
                 confirmed=["VWAP_RECLAIM", "BUY_VOLUME"])

    # 21. Volume-climax reversal.
    rowsd = sd.get("rows", [])
    if len(rowsd) >= 25:
        prior = rowsd[-2]
        prior_vol = f(prior[5])
        base_vol = avg(f(r[5]) for r in rowsd[-22:-2])
        po, ph, pl, pc = f(prior[1]), f(prior[2]), f(prior[3]), f(prior[4])
        pr = max(ph - pl, 1e-12)
        prior_lower_wick = max(0.0, min(po, pc) - pl) / pr
        climax = base_vol > 0 and prior_vol >= 2.5 * base_vol and prior_lower_wick >= 0.30
        follow = f(sd.get("close")) > (ph + pl) / 2.0 and buying(sd)
        if climax:
            emit(setups, "VOLUME_CLIMAX_REVERSAL", "BUY" if follow else "ARMED", 89 if follow else 72, "1D",
                 "Capitulation-style volume climax with lower wick" + (" and bullish follow-through" if follow else ""),
                 entry=f(sd.get("close")), invalidation=pl - 0.35 * ad, support=pl,
                 target_hints=[f(sd.get("res20")), f(sd.get("res60"))],
                 confirmed=["VOLUME_CLIMAX", "LOWER_WICK"] + (["BUYER_FOLLOW_THROUGH"] if follow else []))

    # 22. Higher-low reversal.
    lows4 = [f(r[3]) for r in rows4[-12:]] if len(rows4) >= 12 else []
    highs4 = [f(r[2]) for r in rows4[-12:]] if len(rows4) >= 12 else []
    if len(lows4) >= 8:
        first_low = min(lows4[:-3])
        recent_low = min(lows4[-3:])
        higher_low = recent_low > first_low * 1.002
        swing_trigger = max(highs4[-6:-1])
        if higher_low:
            if current > swing_trigger and buying(s4):
                emit(setups, "HIGHER_LOW_REVERSAL", "BUY", 87, "4H",
                     "Higher low confirmed by break above intervening swing high with buying",
                     entry=current, invalidation=recent_low - 0.35 * a4, support=recent_low,
                     target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                     confirmed=["HIGHER_LOW", "SWING_BREAK", "BUY_VOLUME"])
            else:
                emit(setups, "HIGHER_LOW_REVERSAL", "ARMED", 67, "4H",
                     "Higher low formed; waiting for break above intervening swing high",
                     entry=swing_trigger, invalidation=recent_low - 0.35 * a4, support=recent_low,
                     target_hints=[f(s4.get("res60")), f(sd.get("res20"))],
                     confirmed=["HIGHER_LOW"])

    # 23. Trend continuation.
    trend_ok = bullish_regime(sd) and bullish_regime(s4)
    one_hour_coil = f(s1.get("bb_width")) > 0 and f(s1.get("bb_width")) < 0.03
    one_hour_break = current > f(s1.get("res20")) and buying(s1) and f(s1.get("volume_ratio")) >= 1.25
    if trend_ok and one_hour_break:
        emit(setups, "TREND_CONTINUATION", "BUY", 90, "MTF",
             "Daily + 4H bullish regime, 1H consolidation broke with buying",
             entry=current, invalidation=f(s1.get("sup20")) - 0.45 * a1, support=f(s1.get("sup20")),
             target_hints=[f(s4.get("res20")), f(sd.get("res20")), f(sd.get("res60"))],
             confirmed=["DAILY_BULL", "4H_BULL", "1H_BREAKOUT", "BUY_VOLUME"])
    elif trend_ok and one_hour_coil:
        emit(setups, "TREND_CONTINUATION", "ARMED", 72, "MTF",
             "Daily + 4H bullish regime with 1H consolidation; waiting for breakout",
             entry=f(s1.get("res20")), support=f(s1.get("sup20")),
             target_hints=[f(s4.get("res20")), f(sd.get("res20"))],
             confirmed=["DAILY_BULL", "4H_BULL", "1H_COMPRESSION"])

    if not setups:
        return None

    setups.sort(key=lambda x: (STATE_RANK[x["state"]], x["strength"]), reverse=True)
    best = dict(setups[0])
    buy_setups = [x for x in setups if x["state"] == "BUY"]
    armed_setups = [x for x in setups if x["state"] == "ARMED"]

    if len(buy_setups) >= 2:
        best["name"] = "HIGH_CONFLUENCE_BUY"
        best["state"] = "BUY"
        best["strength"] = min(100.0, max(x["strength"] for x in buy_setups) + min(10.0, 2.5 * (len(buy_setups) - 1)))
        best["reason"] = f"{len(buy_setups)} independent BUY setup families are confirmed"
        best["confirmed"] = [x["name"] for x in buy_setups]
    elif best["state"] != "BUY" and len(armed_setups) >= 2:
        best["strength"] = min(95.0, best["strength"] + min(8.0, 2.0 * (len(armed_setups) - 1)))

    plan = build_plan(sym, current, best, setups, s1, s4, sd, sw)

    # Higher-timeframe regime is informational, not a universal BUY blocker.
    weekly_bull = bullish_regime(sw) if sw else None
    daily_bull = bullish_regime(sd)
    four_bull = bullish_regime(s4)
    if weekly_bull is True and daily_bull and four_bull:
        regime = "FULL_BULLISH_ALIGNMENT"
    elif weekly_bull is False:
        regime = "WEEKLY_BEARISH_OR_MIXED"
    else:
        regime = "MIXED"
    counter_trend = best["state"] == "BUY" and weekly_bull is False

    # Anti-chase remains mandatory for entries, but does not erase the setup.
    if best["state"] == "BUY" and plan["max_chase"] > 0 and current > plan["max_chase"]:
        best["state"] = "WATCH"
        best["name"] = "MISSED_WAIT_RETEST"
        best["reason"] = f"Setup confirmed but price is {pct(current, plan['entry']):.1f}% above ideal entry; do not chase"
        plan["anti_chase"] = True

    return {
        "symbol": sym,
        "state": best["state"],
        "emoji": STATE_EMOJI[best["state"]],
        "setup": best["name"],
        "setup_strength": round(best["strength"], 1),
        "reason": best["reason"],
        "timeframe": best["timeframe"],
        "current": current,
        "entry_low": plan["entry_low"],
        "entry_high": plan["entry_high"],
        "entry": plan["entry"],
        "max_chase": plan["max_chase"],
        "invalidation": plan["invalidation"],
        "risk_pct": plan["risk_pct"],
        "tp1": plan["tp1"],
        "tp1_gain_pct": plan["tp1_gain_pct"],
        "tp2": plan["tp2"],
        "tp2_gain_pct": plan["tp2_gain_pct"],
        "tp3": plan["tp3"],
        "tp3_gain_pct": plan["tp3_gain_pct"],
        "extended": plan["extended"],
        "extended_gain_pct": plan["extended_gain_pct"],
        "target_sources": plan["target_sources"],
        "anti_chase": plan["anti_chase"],
        "trend_regime": regime,
        "counter_trend": counter_trend,
        "buy_setup_count": len(buy_setups),
        "armed_setup_count": len(armed_setups),
        "active_setups": [
            {
                "name": x["name"], "state": x["state"], "strength": round(x["strength"], 1),
                "timeframe": x["timeframe"], "reason": x["reason"],
            }
            for x in setups[:10]
        ],
        "micro_required_by_best": bool(best.get("requires_micro")),
        "micro_fresh": bool(micro.get("fresh")),
        "micro_positive": bool(micro.get("positive")),
        "quote_volume_24h": quote_vol,
        "generated_ms": int(time.time() * 1000),
    }


def build_plan(sym, current, best, setups, s1, s4, sd, sw):
    a4 = max(f(s4.get("atr")), current * 0.006)
    entry = f(best.get("entry"), current)
    if entry <= 0:
        entry = current

    invalidation = f(best.get("invalidation"))
    support = f(best.get("support"))
    if invalidation <= 0:
        if support > 0:
            invalidation = support - 0.55 * a4
        else:
            invalidation = entry - max(0.9 * a4, entry * 0.025)
    if invalidation >= entry:
        invalidation = entry - max(0.9 * a4, entry * 0.025)

    risk = max(entry - invalidation, entry * 0.0075)
    entry_pad = min(0.18 * a4, entry * 0.006)
    entry_low = max(invalidation + 0.15 * risk, entry - entry_pad)
    entry_high = entry + entry_pad
    max_chase = entry + max(ANTI_CHASE_ATR * a4, entry * (ANTI_CHASE_PCT / 100.0))

    candidates = []
    for x in setups:
        for h in x.get("target_hints") or []:
            if h > entry * 1.003:
                candidates.append((h, x["name"]))
    for label, s in (("1H", s1), ("4H", s4), ("1D", sd), ("1W", sw)):
        if not s:
            continue
        for key, desc in (("res20", "near_resistance"), ("res60", "major_resistance"), ("range_high", "range_high")):
            val = f(s.get(key))
            if val > entry * 1.003:
                candidates.append((val, f"{label}_{desc}"))
        rlo, rhi = f(s.get("range_low")), f(s.get("range_high"))
        if rhi > rlo > 0:
            mid = (rlo + rhi) / 2.0
            if mid > entry * 1.003:
                candidates.append((mid, f"{label}_range_mid"))

    # Deduplicate nearby structural levels.
    candidates.sort(key=lambda z: z[0])
    dedup = []
    for val, source in candidates:
        if not dedup or abs(val - dedup[-1][0]) / max(val, 1e-12) > 0.004:
            dedup.append((val, source))

    # Structural levels are preferred. ATR/R expansion only fills missing slots.
    fillers = [
        (entry + risk * 1.0, "1R_expansion"),
        (entry + risk * 2.0, "2R_expansion"),
        (entry + risk * 3.0, "3R_expansion"),
        (entry + risk * 4.0, "4R_expansion"),
        (entry + a4 * 2.5, "ATR_expansion"),
    ]
    for val, source in fillers:
        if val > entry * 1.003 and all(abs(val - x[0]) / val > 0.004 for x in dedup):
            dedup.append((val, source))
    dedup.sort(key=lambda z: z[0])

    # Require increasing targets with useful separation.
    selected = []
    last = entry
    for val, source in dedup:
        if val > last * 1.006:
            selected.append((val, source))
            last = val
        if len(selected) >= 4:
            break
    while len(selected) < 4:
        mult = len(selected) + 1
        val = entry + risk * (mult + 0.5)
        selected.append((val, f"{mult + 0.5:.1f}R_fallback"))

    t1, t2, t3, ext = selected[:4]
    risk_pct = (entry - invalidation) / entry * 100.0 if entry > 0 else 0.0

    return {
        "entry": entry,
        "entry_low": entry_low,
        "entry_high": entry_high,
        "max_chase": max_chase,
        "invalidation": invalidation,
        "risk_pct": risk_pct,
        "tp1": t1[0], "tp1_gain_pct": pct(t1[0], entry),
        "tp2": t2[0], "tp2_gain_pct": pct(t2[0], entry),
        "tp3": t3[0], "tp3_gain_pct": pct(t3[0], entry),
        "extended": ext[0], "extended_gain_pct": pct(ext[0], entry),
        "target_sources": [t1[1], t2[1], t3[1], ext[1]],
        "anti_chase": False,
    }



def _v12_ws_primitives():
    global _v12_ws_ready, _v12_ws_lock, _v12_ws_gate
    if _v12_ws_ready is None:
        _v12_ws_ready = asyncio.Event()
    if _v12_ws_lock is None:
        _v12_ws_lock = asyncio.Lock()
    if _v12_ws_gate is None:
        _v12_ws_gate = asyncio.Semaphore(12)
    return _v12_ws_ready, _v12_ws_lock, _v12_ws_gate


def _v12_ws_fail_pending(reason):
    for rid, fut in list(_v12_ws_pending.items()):
        if fut is not None and not fut.done():
            try:
                fut.set_exception(RuntimeError(reason))
            except Exception:
                pass
    _v12_ws_pending.clear()


async def v12_ws_rpc_loop():
    global _v12_ws_conn
    ready, _, _ = _v12_ws_primitives()
    while app.session is None:
        await asyncio.sleep(0.25)
    while True:
        ws = None
        try:
            async with app.session.ws_connect(
                V12_WS_API_URL,
                heartbeat=15,
                autoping=True,
                receive_timeout=40,
            ) as ws:
                _v12_ws_conn = ws
                ready.set()
                _stats["ws_connects"] += 1
                print(f"Ψ-V12 WS-RPC connected url={V12_WS_API_URL}", flush=True)
                async for msg in ws:
                    if msg.type == legacy.aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except Exception:
                            continue
                        rid = str(payload.get("id") or "")
                        fut = _v12_ws_pending.pop(rid, None)
                        if fut is not None and not fut.done():
                            fut.set_result(payload)
                    elif msg.type in {
                        legacy.aiohttp.WSMsgType.CLOSED,
                        legacy.aiohttp.WSMsgType.CLOSE,
                        legacy.aiohttp.WSMsgType.ERROR,
                    }:
                        raise RuntimeError(f"V12 WS RPC closed type={msg.type}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["ws_errors"] += 1
            _stats["ws_last_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            ready.clear()
            if _v12_ws_conn is ws:
                _v12_ws_conn = None
            _v12_ws_fail_pending("V12 WS RPC reset")
        await asyncio.sleep(0.75)


async def v12_ws_klines(symbol, interval, limit):
    global _v12_ws_id
    ready, lock, gate = _v12_ws_primitives()
    try:
        await asyncio.wait_for(ready.wait(), timeout=2.0)
    except asyncio.TimeoutError:
        _stats["ws_unavailable"] += 1
        return None

    acquired = False
    rid = None
    try:
        await asyncio.wait_for(gate.acquire(), timeout=1.5)
        acquired = True
        loop = asyncio.get_running_loop()
        async with lock:
            ws = _v12_ws_conn
            if ws is None or ws.closed:
                return None
            _v12_ws_id += 1
            rid = str(_v12_ws_id)
            fut = loop.create_future()
            _v12_ws_pending[rid] = fut
            await ws.send_json({
                "id": rid,
                "method": "klines",
                "params": {
                    "symbol": str(symbol).upper(),
                    "interval": str(interval),
                    "limit": int(limit),
                },
            })
            _stats["ws_requests"] += 1

        payload = await asyncio.wait_for(fut, timeout=5.0)
        status = int(payload.get("status") or 0) if isinstance(payload, dict) else 0
        rows = payload.get("result") if isinstance(payload, dict) else None
        if status == 200 and isinstance(rows, list) and rows:
            _stats["ws_ok"] += 1
            return rows
        _stats["ws_fail"] += 1
        _stats["ws_last_error"] = f"status={status} payload={str(payload)[:180]}"
        return None
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _stats["ws_fail"] += 1
        _stats["ws_last_error"] = f"{type(exc).__name__}: {exc}"
        return None
    finally:
        if rid is not None:
            _v12_ws_pending.pop(rid, None)
        if acquired:
            gate.release()


async def _fetch_tf(sym, tf):
    if app.session is None:
        return False

    rows = None
    # Dedicated V12 WS-RPC is the authoritative hydration transport. Retry once
    # on the same socket; do not fall into the slow Railway REST timeout path,
    # which previously held a hydration permit for ~7 seconds per miss.
    for attempt in range(2):
        rows = await v12_ws_klines(sym, tf, TF_LIMIT[tf])
        if isinstance(rows, list) and len(rows) >= 55:
            _stats["fetch_ws_ok"] += 1
            break
        rows = None
        if attempt == 0:
            await asyncio.sleep(0.12)

    if isinstance(rows, list) and len(rows) >= 55:
        snapshot = snap(rows)
        if snapshot is not None:
            _cache[sym][tf] = {"rows": rows, "snap": snapshot, "updated": time.time()}
            _stats["fetch_ok"] += 1
            return True
        _stats["fetch_short_history"] += 1
        return False

    _stats["fetch_fail"] += 1
    return False


async def refresh_symbol(sym, sem, active=False):
    now = time.time()
    ttl = ACTIVE_TF_TTL if active else TF_TTL
    core_tfs = ("1h", "4h", "1d")
    core_stale = []

    for tf in core_tfs:
        item = _cache.get(sym, {}).get(tf) or {}
        if now - f(item.get("updated")) > ttl[tf] or not item.get("snap"):
            core_stale.append(tf)

    async def one(tf):
        async with sem:
            try:
                return await asyncio.wait_for(_fetch_tf(sym, tf), timeout=11.0)
            except asyncio.TimeoutError:
                _stats["fetch_timeout"] += 1
                return False

    # Fetch the three core timeframes together. Once present they remain valid
    # for structural qualification long enough for coverage to accumulate.
    if core_stale:
        await asyncio.gather(*(one(tf) for tf in core_stale), return_exceptions=True)
        return

    # Weekly enrichment is lower priority and never blocks initial 1H/4H/1D
    # coverage. Active symbols receive fresher Weekly refreshes.
    weekly = _cache.get(sym, {}).get("1w") or {}
    if now - f(weekly.get("updated")) > ttl["1w"] or not weekly.get("snap"):
        await one("1w")


def _priority_symbols(universe):
    out = []
    seen = set()
    universe_set = set(universe)

    def add(s):
        s = str(s or "")
        if s and s in universe_set and s not in seen:
            seen.add(s)
            out.append(s)

    legacy_rows = list(base.latest.get("_all_candidates") or [])
    legacy_rows.sort(key=lambda r: (
        int(f(r.get("layers"))),
        f(r.get("bsi")),
        f(r.get("early")),
        f(r.get("eventTape")),
    ), reverse=True)

    for r in legacy_rows:
        add(r.get("symbol"))
        if len(out) >= ACTIVE_SYMBOLS_PER_CYCLE:
            break

    for s in list(getattr(app, "selected_micro_symbols", []) or []):
        add(s)
        if len(out) >= ACTIVE_SYMBOLS_PER_CYCLE:
            break

    return out[:ACTIVE_SYMBOLS_PER_CYCLE]


def _bootstrap_symbols(universe, refresh_tasks):
    """Fairly select never-ready/core-stale symbols across the whole universe."""
    global _cursor
    out = []
    n = len(universe)
    if not n:
        return out

    attempts = 0
    while len(out) < BOOTSTRAP_SYMBOLS_PER_CYCLE and attempts < n * 2:
        sym = universe[_cursor % n]
        _cursor = (_cursor + 1) % n
        attempts += 1
        if sym in refresh_tasks:
            continue

        c = _cache.get(sym) or {}
        core_ready = all((c.get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d"))
        if not core_ready:
            out.append(sym)

    # Once core coverage is complete, the same fair cursor enriches Weekly.
    if not out:
        attempts = 0
        while len(out) < BOOTSTRAP_SYMBOLS_PER_CYCLE and attempts < n * 2:
            sym = universe[_cursor % n]
            _cursor = (_cursor + 1) % n
            attempts += 1
            if sym in refresh_tasks:
                continue
            c = _cache.get(sym) or {}
            core_ready = all((c.get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d"))
            weekly_ready = bool((c.get("1w") or {}).get("snap"))
            if core_ready and not weekly_ready:
                out.append(sym)

    return out


def _board():
    rows = list(_results.values())
    rows.sort(key=lambda r: (STATE_RANK.get(r.get("state"), 0), f(r.get("setup_strength")), f(r.get("extended_gain_pct"))), reverse=True)
    return rows


def _fmt(v):
    x = f(v)
    if x <= 0:
        return "-"
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


def print_board(force=False):
    global _last_board_print
    now = time.time()
    if not force and now - _last_board_print < LOOP_SECONDS - 1:
        return
    _last_board_print = now

    universe = list(getattr(q, "universe", []) or [])
    ready = sum(all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")) for s in universe)
    weekly_ready = sum(bool((_cache.get(s, {}).get("1w") or {}).get("snap")) for s in universe)
    rows = _board()
    counts = {st: sum(r.get("state") == st for r in rows) for st in ("BUY", "ARMED", "WATCH")}

    print(
        f"Ψ-V12 SIGNAL BOARD universe={len(universe)} mtfReady={ready}/{len(universe)} "
        f"weeklyReady={weekly_ready}/{len(universe)} BUY={counts['BUY']} ARMED={counts['ARMED']} "
        f"WATCH={counts['WATCH']} legacyBuyAuthority=DISABLED setupAuthority=V12",
        flush=True,
    )

    for state in ("BUY", "ARMED", "WATCH"):
        chosen = [r for r in rows if r.get("state") == state][:MAX_BOARD_PER_STATE]
        print(f"{STATE_EMOJI[state]} {state} count={len(chosen)}", flush=True)
        for i, r in enumerate(chosen, 1):
            print(
                f"{STATE_EMOJI[state]} {state[0]}{i:02d}. {r['symbol']:<14} "
                f"setup={r['setup']} strength={r['setup_strength']:.1f} "
                f"entry={_fmt(r['entry_low'])}-{_fmt(r['entry_high'])} "
                f"maxChase={_fmt(r['max_chase'])} stop={_fmt(r['invalidation'])} "
                f"TP1={_fmt(r['tp1'])}(+{r['tp1_gain_pct']:.1f}%) "
                f"TP2={_fmt(r['tp2'])}(+{r['tp2_gain_pct']:.1f}%) "
                f"TP3={_fmt(r['tp3'])}(+{r['tp3_gain_pct']:.1f}%) "
                f"EXT={_fmt(r['extended'])}(+{r['extended_gain_pct']:.1f}%) "
                f"regime={r['trend_regime']} confluence={r['buy_setup_count']}/{r['armed_setup_count']} "
                f"why={r['reason']}",
                flush=True,
            )


async def strategy_loop():
    global _cycle, _results
    print("Ψ-V12 STRATEGY_LOOP starting; waiting for Binance session/universe", flush=True)
    while app.session is None:
        await asyncio.sleep(0.5)
    await asyncio.sleep(2.0)

    sem = asyncio.Semaphore(FETCH_CONCURRENCY)
    refresh_tasks = {}

    while True:
        try:
            universe = list(getattr(q, "universe", []) or [])
            if not universe:
                await asyncio.sleep(2.0)
                continue

            # Retire completed work first so permits and symbol slots are
            # immediately reusable.
            for sym, task in list(refresh_tasks.items()):
                if task.done():
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        _stats["refresh_task_fail"] += 1
                    refresh_tasks.pop(sym, None)

            active = _priority_symbols(universe)

            # Active candidates refresh quickly, but never consume every slot.
            active_budget = min(
                len(active),
                max(0, min(ACTIVE_SYMBOLS_PER_CYCLE, MAX_INFLIGHT_SYMBOLS // 4))
            )
            for sym in active[:active_budget]:
                if sym not in refresh_tasks and len(refresh_tasks) < MAX_INFLIGHT_SYMBOLS:
                    refresh_tasks[sym] = asyncio.create_task(refresh_symbol(sym, sem, active=True))

            # The majority of slots are reserved for fair whole-universe
            # hydration so 403/403 coverage actually converges.
            bootstrap = _bootstrap_symbols(universe, refresh_tasks)
            for sym in bootstrap:
                if len(refresh_tasks) >= MAX_INFLIGHT_SYMBOLS:
                    break
                if sym not in refresh_tasks:
                    refresh_tasks[sym] = asyncio.create_task(refresh_symbol(sym, sem, active=False))

            await asyncio.sleep(0.25)

            ready_now = sum(
                all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d"))
                for s in universe
            )
            weekly_ready = sum(
                bool((_cache.get(s, {}).get("1w") or {}).get("snap"))
                for s in universe
            )

            print(
                f"Ψ-V12 REFRESH cycle={_cycle + 1} active={len(active)} bootstrap={len(bootstrap)} "
                f"mtfReady={ready_now}/{len(universe)} weeklyReady={weekly_ready}/{len(universe)} "
                f"inFlight={len(refresh_tasks)}/{MAX_INFLIGHT_SYMBOLS} permits={FETCH_CONCURRENCY} "
                f"fetchOK={_stats.get('fetch_ok', 0)} fetchFail={_stats.get('fetch_fail', 0)} "
                f"fetchTO={_stats.get('fetch_timeout', 0)} wsOK={_stats.get('ws_ok', 0)} "
                f"wsFail={_stats.get('ws_fail', 0)}",
                flush=True,
            )

            new_results = {}
            now = time.time()
            for sym in universe:
                c = _cache.get(sym) or {}
                # Qualification requires structurally usable core snapshots.
                # Active-candidate freshness is handled by the fast tier above.
                if not all((c.get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")):
                    continue
                if now - f((c.get("1h") or {}).get("updated")) > TF_TTL["1h"]:
                    continue
                try:
                    row = evaluate_symbol(sym)
                except Exception:
                    _stats["eval_fail"] += 1
                    continue
                if row:
                    new_results[sym] = row

            _results = new_results
            _cycle += 1
            _stats["cycles"] = _cycle
            _stats["refresh_inflight"] = len(refresh_tasks)
            _stats["mtf_ready"] = ready_now
            _stats["weekly_ready"] = weekly_ready
            print_board(force=True)

        except asyncio.CancelledError:
            for task in refresh_tasks.values():
                task.cancel()
            raise
        except Exception as exc:
            _stats["loop_fail"] += 1
            print(f"Ψ-V12 LOOP_ERROR {type(exc).__name__}: {exc}", flush=True)

        await asyncio.sleep(5.0)


async def v12_scan(req):
    rows = _board()
    try:
        limit = max(1, min(int(req.query.get("limit", "60")), 100))
    except Exception:
        limit = 60
    state = str(req.query.get("state", "")).upper().strip()
    if state in STATE_RANK:
        rows = [r for r in rows if r.get("state") == state]
    universe = list(getattr(q, "universe", []) or [])
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V12 Multi-Setup Authority",
        "version": VERSION,
        "legacy_buy_authority": False,
        "signal_authority": "V12_SETUP_FAMILIES",
        "colour_map": {"BUY": "green", "ARMED": "orange", "WATCH": "yellow"},
        "universe": len(universe),
        "returned": min(limit, len(rows)),
        "state_counts": {st: sum(r.get("state") == st for r in rows) for st in ("BUY", "ARMED", "WATCH")},
        "results": rows[:limit],
        "stats": dict(_stats),
        "generated_ms": int(time.time() * 1000),
    })


async def v12_health(req):
    universe = list(getattr(q, "universe", []) or [])
    ready = sum(all((_cache.get(s, {}).get(tf) or {}).get("snap") for tf in ("1h", "4h", "1d")) for s in universe)
    weekly_ready = sum(bool((_cache.get(s, {}).get("1w") or {}).get("snap")) for s in universe)
    return app.web.json_response({
        "ok": True,
        "version": VERSION,
        "legacy_buy_authority": False,
        "universe": len(universe),
        "mtf_ready": ready,
        "weekly_ready": weekly_ready,
        "stats": dict(_stats),
    })


# app.main() constructs the aiohttp application later and resolves these
# module globals at runtime. Replace the public handlers now so /scan and
# /health expose V12 authority, while inherited modules remain telemetry only.
app.scan_endpoint = v12_scan
app.health = v12_health


async def main():
    for mod in (app, q, base, legacy):
        try:
            mod.VERSION = VERSION
        except Exception:
            pass
    app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"
    print(
        "[v12.0.1] MULTI-SETUP AUTHORITY + FULL-UNIVERSE HYDRATION active — legacy BUY/PRE authority disabled; "
        "independent Golden Cross, EMA rejection/reclaim, Weekly MA interaction, "
        "Weekly/Daily cross, MTF confluence, deep pullback exhaustion, coiled accumulation, "
        "Daily range-bottom, failed breakdown, liquidity sweep, compression breakout, "
        "breakout-retest, VWAP reclaim, volume-climax, higher-low and trend-continuation "
        "engines now own 🟢 BUY / 🟠 ARMED / 🟡 WATCH. Entry, max-chase, invalidation and "
        "structure-derived targets with gain percentages are mandatory.",
        flush=True,
    )
    await asyncio.gather(legacy.main(), v12_ws_rpc_loop(), strategy_loop())


if __name__ == "__main__":
    asyncio.run(main())
