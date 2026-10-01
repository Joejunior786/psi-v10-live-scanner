import asyncio
import json
import math
import os
import time
from collections import defaultdict, deque

import aiohttp

import ignition1214_entry as base
import ignition1071_app as radar_feed
import ignition116_entry as v16
import ignition1183_entry as life_mod

scanner, q, app = base.scanner, base.q, base.app

VERSION = "11.0.0-predictive-market-physics"

# V11 starts as an intelligence/ranking layer. It can promote candidates into
# the micro pool, but it cannot relax or manufacture V10 PRE/BUY/PUMP states.
V11_SAMPLE_SECONDS = max(1.0, float(os.getenv("PSI_V11_SAMPLE_SECONDS", "2")))
V11_PRINT_SECONDS = max(10.0, float(os.getenv("PSI_V11_PRINT_SECONDS", "30")))
V11_XVENUE_SECONDS = max(2.0, float(os.getenv("PSI_V11_XVENUE_SECONDS", "4")))
V11_PREDICT_SECONDS = max(20.0, float(os.getenv("PSI_V11_PREDICT_SECONDS", "30")))
V11_MAX_BOARD = max(10, min(40, int(os.getenv("PSI_V11_BOARD_SIZE", "25"))))
V11_HISTORY_SECONDS = max(1800, int(os.getenv("PSI_V11_HISTORY_SECONDS", "4200")))
V11_HISTORY_SAMPLES = int(V11_HISTORY_SECONDS / 5) + 16
V11_OUTCOME_FILE = os.getenv("PSI_V11_OUTCOME_FILE", "/data/psi_v11_outcomes.json")
V11_SHADOW_ONLY = os.getenv("PSI_V11_SHADOW_ONLY", "1") != "0"

# Public market-data endpoints only. No trading keys or private data are used.
OKX_TICKERS_URL = os.getenv("PSI_OKX_TICKERS_URL", "https://www.okx.com/api/v5/market/tickers")
BYBIT_TICKERS_URL = os.getenv("PSI_BYBIT_TICKERS_URL", "https://api.bybit.com/v5/market/tickers")

event_state = {}
fatigue_state = {}
xvenue_state = {}
hazard_state = {}
v11_board = []
price_history = defaultdict(lambda: deque(maxlen=V11_HISTORY_SAMPLES))
venue_history = defaultdict(lambda: {
    "okx": deque(maxlen=120),
    "bybit": deque(maxlen=120),
})
venue_prices = {"okx": {}, "bybit": {}}
venue_stats = {
    "okx_ok": 0, "okx_err": 0, "bybit_ok": 0, "bybit_err": 0,
    "last_okx": 0.0, "last_bybit": 0.0,
}
pending_predictions = []
resolved_predictions = deque(maxlen=5000)
missed_moves = deque(maxlen=500)
last_prediction_at = defaultdict(float)
last_missed_at = defaultdict(float)
engine_stats = {
    "event_cycles": 0,
    "xvenue_cycles": 0,
    "prediction_cycles": 0,
    "errors": 0,
    "last_persist": 0.0,
}


def now():
    return time.time()


def ms():
    return int(time.time() * 1000)


def f(v, default=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def sigmoid(x):
    if x >= 0:
        e = math.exp(-min(x, 60.0))
        return 1.0 / (1.0 + e)
    e = math.exp(max(x, -60.0))
    return e / (1.0 + e)


def mean(xs):
    vals = [f(x) for x in xs]
    return sum(vals) / len(vals) if vals else 0.0


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if a and b else 0.0


def before(samples, target):
    for x in reversed(samples):
        if x[0] <= target:
            return x
    return samples[0] if samples else None


def slope(values):
    vals = [f(x) for x in values]
    n = len(vals)
    if n < 2:
        return 0.0
    xm = (n - 1) / 2.0
    ym = sum(vals) / n
    den = sum((i - xm) ** 2 for i in range(n))
    if den <= 0:
        return 0.0
    return sum((i - xm) * (v - ym) for i, v in enumerate(vals)) / den


def raw_binance_price(sym):
    hist = radar_feed.radar_hist.get(sym)
    if hist:
        return f(hist[-1][1])
    try:
        return f(app.current_symbol_price(sym))
    except Exception:
        row = q.latest.get(sym) or {}
        return f(row.get("price") or row.get("current_price"))


def _trade_windows(sym, seconds=30):
    state = app.micro_state.get(sym) or {}
    rows = list(state.get("trades") or ())
    cutoff = ms() - seconds * 1000
    return [r for r in rows if r and r[0] >= cutoff]


def _bucket_signed(rows, start_ago, end_ago):
    n = ms()
    lo = n - start_ago * 1000
    hi = n - end_ago * 1000
    vals = [r for r in rows if lo <= r[0] < hi]
    return sum(f(r[1]) for r in vals), sum(f(r[2]) for r in vals), len(vals)


def _series_velocity(series, seconds=20):
    if not series:
        return 0.0
    cutoff = ms() - seconds * 1000
    rows = [x for x in series if x[0] >= cutoff]
    if len(rows) < 3:
        return 0.0
    dt = max((rows[-1][0] - rows[0][0]) / 1000.0, 1e-6)
    return (f(rows[-1][1]) - f(rows[0][1])) / dt


def event_microstructure(sym):
    state = app.micro_state.get(sym) or {}
    trades = list(state.get("trades") or ())
    if not trades:
        return {"symbol": sym, "ready": False, "score": 0.0, "reason": "NO_EVENT_TRADES"}

    recent = [r for r in trades if r[0] >= ms() - 30_000]
    if len(recent) < 6:
        return {"symbol": sym, "ready": False, "score": 0.0, "reason": "INSUFFICIENT_EVENT_TRADES"}

    f0, q0, n0 = _bucket_signed(recent, 5, 0)
    f1, q1, n1 = _bucket_signed(recent, 10, 5)
    f2, q2, n2 = _bucket_signed(recent, 15, 10)

    flow_v0, flow_v1, flow_v2 = f0 / 5.0, f1 / 5.0, f2 / 5.0
    flow_acc = flow_v0 - flow_v1
    flow_jerk = flow_acc - (flow_v1 - flow_v2)
    tr_v0, tr_v1, tr_v2 = n0 / 5.0, n1 / 5.0, n2 / 5.0
    trade_acc = tr_v0 - tr_v1
    trade_jerk = (tr_v0 - tr_v1) - (tr_v1 - tr_v2)

    quote_rate = q0 / 5.0
    buy_quote = sum(f(r[2]) for r in recent if f(r[1]) > 0)
    total_quote = sum(f(r[2]) for r in recent)
    buy_ratio = buy_quote / total_quote if total_quote > 0 else 0.5

    ofi_series = list(state.get("ofi") or ())
    obi_series = list(state.get("obi") or ())
    ask_series = list(state.get("ask_depletion") or ())
    bid_series = list(state.get("bid_depletion") or ())
    ofi_v = _series_velocity(ofi_series, 15)
    obi_v = _series_velocity(obi_series, 15)
    ask_v = _series_velocity(ask_series, 15)
    bid_v = _series_velocity(bid_series, 15)

    bids = app.sorted_levels(state.get("book_bids", {}), True) if state.get("book_bids") else []
    asks = app.sorted_levels(state.get("book_asks", {}), False) if state.get("book_asks") else []

    def notion(levels, k):
        return sum(f(p) * f(qty) for p, qty in levels[:k])

    b3, a3 = notion(bids, 3), notion(asks, 3)
    b10, a10 = notion(bids, 10), notion(asks, 10)
    obi3 = (b3 - a3) / (b3 + a3) if b3 + a3 > 0 else 0.0
    obi10 = (b10 - a10) / (b10 + a10) if b10 + a10 > 0 else 0.0
    depth_curvature = obi3 - obi10
    ask_thin_ratio = a3 / max(a10, 1e-9)
    bid_support_ratio = b3 / max(b10, 1e-9)

    spread_rows = list(state.get("spread_bps") or ())
    slip_rows = list(state.get("slippage_bps") or ())
    latest_spread = f(spread_rows[-1][1], 999) if spread_rows else 999.0
    latest_slip = f(slip_rows[-1][1], 999) if slip_rows else 999.0

    trade_fresh = f(state.get("last_trade_ms")) >= ms() - 15_000
    book_fresh = f(state.get("last_book_ms")) >= ms() - 5_000
    seq_ok = bool(state.get("trade_sequence_ok")) and bool(state.get("book_sequence_ok"))
    ready = trade_fresh and book_fresh and seq_ok and len(recent) >= 10

    # High-order event derivatives are bounded so a single outlier cannot
    # dominate the score.
    flow_scale = max(total_quote / max(len(recent), 1), 1.0)
    z_flow = math.tanh(flow_v0 / (flow_scale * 3.0))
    z_acc = math.tanh(flow_acc / (flow_scale * 2.0))
    z_jerk = math.tanh(flow_jerk / (flow_scale * 2.5))
    z_trade = math.tanh(trade_acc / 3.0)
    z_tjerk = math.tanh(trade_jerk / 4.0)
    z_ofi = math.tanh(ofi_v * 18.0)
    z_obi = math.tanh(obi_v * 18.0)
    z_ask = math.tanh(ask_v * 20.0)
    z_curve = math.tanh(depth_curvature * 4.0)

    raw = (
        0.17 * z_flow + 0.16 * z_acc + 0.10 * z_jerk
        + 0.10 * z_trade + 0.06 * z_tjerk
        + 0.12 * z_ofi + 0.08 * z_obi + 0.08 * z_ask
        + 0.07 * z_curve + 0.06 * ((buy_ratio - 0.5) * 2.0)
    )
    quality = 1.0
    if latest_spread > 20:
        quality *= 0.75
    if latest_slip > 35:
        quality *= 0.75
    if not ready:
        quality *= 0.55
    score = clamp(50.0 + 50.0 * raw, 0.0, 100.0) * quality

    # Observable book instability diagnostic; this is not labelled spoofing.
    depth_instability = clamp(
        abs(ofi_v) * 4.0 + abs(obi_v) * 3.0 + abs(ask_v - bid_v) * 2.0,
        0.0, 1.0,
    )

    return {
        "symbol": sym,
        "ready": bool(ready),
        "score": round(score, 2),
        "flow_velocity": round(flow_v0, 4),
        "flow_acceleration": round(flow_acc, 4),
        "flow_jerk": round(flow_jerk, 4),
        "trade_rate_5s": round(tr_v0, 3),
        "trade_acceleration": round(trade_acc, 3),
        "trade_jerk": round(trade_jerk, 3),
        "quote_rate_5s": round(quote_rate, 2),
        "aggressive_buy_ratio_30s": round(buy_ratio, 4),
        "ofi_velocity": round(ofi_v, 6),
        "obi_velocity": round(obi_v, 6),
        "ask_depletion_velocity": round(ask_v, 6),
        "bid_depletion_velocity": round(bid_v, 6),
        "obi_l1_l3": round(obi3, 4),
        "obi_l1_l10": round(obi10, 4),
        "depth_curvature": round(depth_curvature, 4),
        "ask_thin_ratio": round(ask_thin_ratio, 4),
        "bid_support_ratio": round(bid_support_ratio, 4),
        "spread_bps": round(latest_spread, 3),
        "slippage_bps": round(latest_slip, 3),
        "depth_instability": round(depth_instability, 4),
    }


def _local_resistance(sym):
    lf = life_mod.life(sym)
    r5 = f(lf.get("resistance_5m"))
    if r5 > 0:
        return r5, "LIFECYCLE_5M"

    trades = _trade_windows(sym, 180)
    if len(trades) >= 20:
        cutoff = ms() - 15_000
        older = [f(r[3]) for r in trades if r[0] < cutoff and f(r[3]) > 0]
        if older:
            return max(older), "EVENT_180S"

    row = q.latest.get(sym) or {}
    r = f(row.get("resistance") or row.get("breakout_resistance"))
    return (r, "STRUCTURE") if r > 0 else (0.0, "NONE")


def resistance_fatigue(sym):
    level, source = _local_resistance(sym)
    trades = _trade_windows(sym, 180)
    px = raw_binance_price(sym)
    if level <= 0 or px <= 0 or len(trades) < 10:
        return {
            "symbol": sym, "ready": False, "score": 0.0,
            "resistance": level or None, "source": source,
            "reason": "NO_LOCAL_RESISTANCE_OR_TRADES",
        }

    band_pct = 0.20
    episodes = []
    in_band = False
    start_idx = None
    for i, row in enumerate(trades):
        price = f(row[3])
        dist = pct(level, price)
        near = -0.12 <= dist <= band_pct
        if near and not in_band:
            in_band = True
            start_idx = i
        elif not near and in_band:
            if start_idx is not None:
                episodes.append((start_idx, i - 1))
            in_band = False
            start_idx = None
    if in_band and start_idx is not None:
        episodes.append((start_idx, len(trades) - 1))

    merged = []
    for ep in episodes:
        if not merged:
            merged.append(ep)
            continue
        prev = merged[-1]
        gap_ms = trades[ep[0]][0] - trades[prev[1]][0]
        if gap_ms <= 2500:
            merged[-1] = (prev[0], ep[1])
        else:
            merged.append(ep)
    merged = merged[-8:]

    rejections = []
    attack_buy_ratios = []
    for a, b in merged:
        end_ts = trades[b][0] + 12_000
        post = [r for r in trades[b:] if r[0] <= end_ts]
        if not post:
            continue
        peak = max(f(r[3]) for r in trades[a:b + 1])
        low_after = min(f(r[3]) for r in post)
        rejection_bps = max(0.0, (peak - low_after) / max(level, 1e-9) * 10000.0)
        rejections.append(rejection_bps)
        attack_rows = trades[a:b + 1]
        tq = sum(f(r[2]) for r in attack_rows)
        bq = sum(f(r[2]) for r in attack_rows if f(r[1]) > 0)
        attack_buy_ratios.append(bq / tq if tq > 0 else 0.5)

    reject_slope = slope(rejections[-5:])
    rejection_decay = clamp(-reject_slope / 8.0, 0.0, 1.0)
    attacks = len(rejections)

    ev = event_state.get(sym) or event_microstructure(sym)
    ask_thinning = clamp(
        0.5 + f(ev.get("ask_depletion_velocity")) * 30.0
        + max(0.0, f(ev.get("depth_curvature"))) * 0.8,
        0.0, 1.0,
    )
    buy_pressure = clamp((mean(attack_buy_ratios) - 0.45) / 0.30, 0.0, 1.0)
    attack_density = clamp(attacks / 4.0, 0.0, 1.0)
    distance = pct(level, px)
    proximity = clamp(1.0 - max(distance, 0.0) / 2.0, 0.0, 1.0)

    fatigue = 100.0 * (
        0.28 * attack_density
        + 0.25 * rejection_decay
        + 0.20 * ask_thinning
        + 0.17 * buy_pressure
        + 0.10 * proximity
    )
    ready = attacks >= 2 and abs(distance) <= 3.0

    return {
        "symbol": sym,
        "ready": bool(ready),
        "score": round(fatigue, 2),
        "resistance": level,
        "source": source,
        "distance_pct": round(distance, 4),
        "attacks": attacks,
        "rejections_bps": [round(x, 2) for x in rejections[-5:]],
        "rejection_slope_bps_per_attack": round(reject_slope, 3),
        "rejection_decay": round(rejection_decay, 4),
        "ask_thinning": round(ask_thinning, 4),
        "attack_buy_pressure": round(buy_pressure, 4),
    }


async def _fetch_json(session, url, params=None):
    try:
        async with session.get(
            url,
            params=params,
            timeout=aiohttp.ClientTimeout(total=8),
            headers={"User-Agent": f"psi-scanner/{VERSION}"},
        ) as response:
            if response.status != 200:
                return None
            return await response.json(content_type=None)
    except Exception:
        return None


async def poll_cross_venues():
    if app.session is None:
        return
    okx, bybit = await asyncio.gather(
        _fetch_json(app.session, OKX_TICKERS_URL, {"instType": "SPOT"}),
        _fetch_json(app.session, BYBIT_TICKERS_URL, {"category": "spot"}),
    )
    t = now()

    if isinstance(okx, dict) and str(okx.get("code")) == "0":
        parsed = {}
        for row in okx.get("data") or []:
            inst = str(row.get("instId") or "")
            if not inst.endswith("-USDT"):
                continue
            sym = inst.replace("-", "")
            price = f(row.get("last"))
            if price > 0:
                parsed[sym] = price
        venue_prices["okx"] = parsed
        venue_stats["okx_ok"] += 1
        venue_stats["last_okx"] = t
    else:
        venue_stats["okx_err"] += 1

    try:
        bybit_ok = isinstance(bybit, dict) and int(bybit.get("retCode", -1)) == 0
    except (TypeError, ValueError):
        bybit_ok = False
    if bybit_ok:
        parsed = {}
        result = bybit.get("result") or {}
        for row in result.get("list") or []:
            sym = str(row.get("symbol") or "")
            if not sym.endswith("USDT"):
                continue
            price = f(row.get("lastPrice"))
            if price > 0:
                parsed[sym] = price
        venue_prices["bybit"] = parsed
        venue_stats["bybit_ok"] += 1
        venue_stats["last_bybit"] = t
    else:
        venue_stats["bybit_err"] += 1



def _venue_return(sym, venue, seconds):
    hist = venue_history[sym][venue]
    if len(hist) < 2:
        return None
    cur = hist[-1]
    prev = before(hist, cur[0] - seconds)
    if not prev or f(prev[1]) <= 0:
        return None
    return pct(cur[1], prev[1])



def cross_venue_leadlag(sym):
    bh = radar_feed.radar_hist.get(sym)
    if not bh or len(bh) < 5:
        return {"symbol": sym, "ready": False, "score": 0.0, "venues": 0}

    bt, bp = f(bh[-1][0]), f(bh[-1][1])
    b5 = before(bh, bt - 5)
    b15 = before(bh, bt - 15)
    br5 = pct(bp, f(b5[1])) if b5 and f(b5[1]) > 0 else 0.0
    br15 = pct(bp, f(b15[1])) if b15 and f(b15[1]) > 0 else 0.0

    ext5, ext15, names = [], [], []
    for venue in ("okx", "bybit"):
        r5 = _venue_return(sym, venue, 5)
        r15 = _venue_return(sym, venue, 15)
        if r5 is not None and r15 is not None:
            ext5.append(r5)
            ext15.append(r15)
            names.append(venue)

    if not ext5:
        return {
            "symbol": sym, "ready": False, "score": 0.0,
            "venues": 0, "binance_r5": round(br5, 4), "binance_r15": round(br15, 4),
        }

    lead5 = mean(ext5) - br5
    lead15 = mean(ext15) - br15
    agreement = sum(1 for x in ext5 if x > br5 + 0.02) / len(ext5)
    lead_strength = clamp(
        0.58 * clamp((lead5 - 0.01) / 0.18)
        + 0.32 * clamp((lead15 - 0.02) / 0.35)
        + 0.10 * agreement,
        0.0, 1.0,
    )
    score = 100.0 * lead_strength
    return {
        "symbol": sym,
        "ready": len(ext5) >= 1,
        "score": round(score, 2),
        "venues": len(ext5),
        "venue_names": names,
        "binance_r5": round(br5, 4),
        "binance_r15": round(br15, 4),
        "external_r5_mean": round(mean(ext5), 4),
        "external_r15_mean": round(mean(ext15), 4),
        "lead5_pct": round(lead5, 4),
        "lead15_pct": round(lead15, 4),
        "bullish_agreement": round(agreement, 3),
    }



def _layer_count(row):
    try:
        return int(v16._layers(dict(row or {})))
    except Exception:
        source = row or {}
        return sum(bool(v) for v in (source.get("layer_results") or {}).values())



def breakout_hazard(sym):
    row = q.latest.get(sym) or {}
    ev = event_state.get(sym) or {}
    ft = fatigue_state.get(sym) or {}
    xv = xvenue_state.get(sym) or {}
    rm = radar_feed.metric(sym)
    px = raw_binance_price(sym)
    resistance = f(ft.get("resistance"))
    dist = f(ft.get("distance_pct"), 99.0) if resistance > 0 else f(row.get("breakout_distance_pct"), 99.0)
    layers = _layer_count(row)

    event_n = f(ev.get("score")) / 100.0
    fatigue_n = f(ft.get("score")) / 100.0
    xlead_n = f(xv.get("score")) / 100.0
    radar_n = clamp(f(rm.get("score")) / 100.0)
    prox = clamp(1.0 - max(dist, 0.0) / 4.0)
    layer_n = clamp(layers / 6.0)
    micro = 1.0 if ev.get("ready") else 0.0
    life = life_mod.life(sym)
    life_bonus = 1.0 if life.get("phase") == "APPROACH" else 0.4 if life.get("phase") == "BELOW_RESISTANCE" else 0.0

    linear = (
        -4.60
        + 1.45 * event_n
        + 1.15 * fatigue_n
        + 0.70 * xlead_n
        + 0.95 * radar_n
        + 1.05 * prox
        + 0.70 * layer_n
        + 0.45 * micro
        + 0.35 * life_bonus
    )
    # Continuous-time exponential hazard. This remains explicitly uncalibrated
    # until V11 has resolved enough forward observations.
    lam = math.exp(clamp(linear, -6.0, 2.5)) * 0.02
    p5 = 1.0 - math.exp(-lam * 5.0)
    p15 = 1.0 - math.exp(-lam * 15.0)
    p60 = 1.0 - math.exp(-lam * 60.0)
    expected_minutes = min(999.0, 1.0 / max(lam, 1e-6))

    impulse = (
        0.30 * event_n + 0.22 * fatigue_n + 0.12 * xlead_n
        + 0.20 * radar_n + 0.16 * layer_n
    )
    p_up5 = sigmoid(-2.8 + 4.2 * impulse)
    p_up10 = sigmoid(-4.0 + 4.6 * impulse)
    p_up20 = sigmoid(-5.1 + 4.9 * impulse)

    evidence = sum([
        bool(ev.get("ready")), bool(ft.get("ready")), bool(xv.get("ready")),
        len(radar_feed.radar_hist.get(sym) or ()) >= 15, bool(row),
    ])
    uncertainty = clamp(1.0 - evidence / 5.0, 0.0, 1.0)

    return {
        "symbol": sym,
        "status": "SHADOW_UNCALIBRATED",
        "lambda_per_min": round(lam, 6),
        "raw_p_breakout_5m": round(p5, 4),
        "raw_p_breakout_15m": round(p15, 4),
        "raw_p_breakout_60m": round(p60, 4),
        "expected_breakout_minutes": round(expected_minutes, 2),
        "raw_p_up5_60m": round(p_up5, 4),
        "raw_p_up10_60m": round(p_up10, 4),
        "raw_p_up20_60m": round(p_up20, 4),
        "evidence_layers": evidence,
        "uncertainty": round(uncertainty, 4),
        "distance_pct": round(dist, 4) if abs(dist) < 900 else None,
        "resistance": resistance or None,
        "price": px or None,
    }



def v11_candidate(sym):
    row = q.latest.get(sym) or {}
    ev = event_state.get(sym) or event_microstructure(sym)
    ft = fatigue_state.get(sym) or resistance_fatigue(sym)
    xv = xvenue_state.get(sym) or cross_venue_leadlag(sym)
    hz = breakout_hazard(sym)
    rm = radar_feed.metric(sym)

    event_n = f(ev.get("score")) / 100.0
    fatigue_n = f(ft.get("score")) / 100.0
    xv_n = f(xv.get("score")) / 100.0
    p15 = f(hz.get("raw_p_breakout_15m"))
    radar_n = clamp(f(rm.get("score")) / 100.0)
    uncertainty = f(hz.get("uncertainty"), 1.0)

    ensemble = 100.0 * (
        0.28 * event_n
        + 0.22 * fatigue_n
        + 0.13 * xv_n
        + 0.23 * p15
        + 0.14 * radar_n
    )
    ensemble *= (1.0 - 0.35 * uncertainty)

    formal_state = str(row.get("state") or "UNKNOWN")
    if ensemble >= 72 and hz.get("evidence_layers", 0) >= 4:
        predictive_state = "V11-PRIME"
    elif ensemble >= 56:
        predictive_state = "V11-EARLY"
    else:
        predictive_state = "V11-WATCH"

    return {
        "symbol": sym,
        "predictive_state": predictive_state,
        "score": round(ensemble, 2),
        "formal_state": formal_state,
        "event_score": ev.get("score", 0.0),
        "fatigue_score": ft.get("score", 0.0),
        "xvenue_score": xv.get("score", 0.0),
        "hazard": hz,
        "event": ev,
        "fatigue": ft,
        "cross_venue": xv,
        "radar_score": round(f(rm.get("score")), 2),
        "layers": _layer_count(row),
        "micro_ready": bool(ev.get("ready")),
    }


# Candidate-promotion bridge: broad public radar + cross-venue lead can move a
# symbol into the V10 micro pool earlier. No execution threshold is modified.
_old_hot = q.hot


def v11_hot(limit=None):
    requested = int(limit or getattr(q, "HOT_COUNT", 80))
    base_rows = list(_old_hot(max(requested, 80)))
    base_map = {sym: f(score) for score, sym in base_rows}
    rows = []
    for sym in list(q.universe):
        rm = radar_feed.metric(sym)
        xv = xvenue_state.get(sym) or {}
        base_score = base_map.get(sym, 0.0)
        score = base_score + 0.18 * max(0.0, f(rm.get("score"))) + 0.22 * f(xv.get("score"))
        rows.append((round(score, 3), sym))
    rows.sort(reverse=True)
    return rows[:requested]


q.hot = v11_hot



def candidate_symbols(limit=80):
    seen = set()
    out = []

    def add(sym):
        if sym and sym in q.universe_set and sym not in seen:
            seen.add(sym)
            out.append(sym)

    for _, sym in v11_hot(max(limit, 80)):
        add(sym)
    for metric in radar_feed.rank(limit=60, triggered_only=False):
        add(metric.get("symbol"))
    rows = []
    for sym, row in q.latest.items():
        rows.append((max(f(row.get("score")), f(row.get("ignition15_score"))), sym))
    for _, sym in sorted(rows, reverse=True)[:40]:
        add(sym)
    for sym in list(app.selected_micro_symbols):
        add(sym)
    return out[:limit]



def build_v11_board():
    global v11_board
    rows = []
    for sym in candidate_symbols(max(V11_MAX_BOARD * 3, 80)):
        try:
            rows.append(v11_candidate(sym))
        except Exception:
            continue
    rows.sort(
        key=lambda row: (
            row.get("predictive_state") == "V11-PRIME",
            row.get("predictive_state") == "V11-EARLY",
            f(row.get("score")),
        ),
        reverse=True,
    )
    v11_board = rows[:V11_MAX_BOARD]
    return v11_board



def _prediction_snapshot(row):
    hz = row.get("hazard") or {}
    return {
        "symbol": row["symbol"],
        "created": now(),
        "entry_price": raw_binance_price(row["symbol"]),
        "resistance": f(hz.get("resistance")),
        "p_break_15": f(hz.get("raw_p_breakout_15m")),
        "p_up5": f(hz.get("raw_p_up5_60m")),
        "p_up10": f(hz.get("raw_p_up10_60m")),
        "p_up20": f(hz.get("raw_p_up20_60m")),
        "score": f(row.get("score")),
        "state": row.get("predictive_state"),
        "event_score": f(row.get("event_score")),
        "fatigue_score": f(row.get("fatigue_score")),
        "xvenue_score": f(row.get("xvenue_score")),
        "max_return_pct": 0.0,
        "min_return_pct": 0.0,
        "breakout_crossed": False,
        "breakout_crossed_at": None,
    }



def update_outcomes():
    t = now()
    keep = []
    for pred in pending_predictions:
        px = raw_binance_price(pred["symbol"])
        entry = f(pred.get("entry_price"))
        if px <= 0 or entry <= 0:
            keep.append(pred)
            continue
        ret = pct(px, entry)
        pred["max_return_pct"] = max(f(pred.get("max_return_pct")), ret)
        pred["min_return_pct"] = min(f(pred.get("min_return_pct")), ret)
        resistance = f(pred.get("resistance"))
        if resistance > 0 and px >= resistance and not pred.get("breakout_crossed"):
            pred["breakout_crossed"] = True
            pred["breakout_crossed_at"] = t

        age = t - f(pred.get("created"))
        if age >= 3600:
            pred["resolved_at"] = t
            pred["hit5"] = f(pred.get("max_return_pct")) >= 5.0
            pred["hit10"] = f(pred.get("max_return_pct")) >= 10.0
            pred["hit20"] = f(pred.get("max_return_pct")) >= 20.0
            pred["break15"] = bool(
                pred.get("breakout_crossed_at")
                and f(pred.get("breakout_crossed_at")) - f(pred.get("created")) <= 900
            )
            resolved_predictions.append(pred)
        else:
            keep.append(pred)
    pending_predictions[:] = keep



def calibration_summary():
    rows = list(resolved_predictions)
    if not rows:
        return {
            "status": "WARMING", "n": 0, "brier_break15": None,
            "hit5": None, "hit10": None, "hit20": None,
        }
    n = len(rows)
    brier = mean([
        (f(row.get("p_break_15")) - (1.0 if row.get("break15") else 0.0)) ** 2
        for row in rows
    ])
    return {
        "status": "VALIDATING" if n < 200 else "CALIBRATED_RESEARCH",
        "n": n,
        "brier_break15": round(brier, 5),
        "hit5": round(sum(bool(row.get("hit5")) for row in rows) / n, 4),
        "hit10": round(sum(bool(row.get("hit10")) for row in rows) / n, 4),
        "hit20": round(sum(bool(row.get("hit20")) for row in rows) / n, 4),
    }



def detect_missed_moves():
    t = now()
    for sym, hist in list(price_history.items()):
        if len(hist) < 2 or t - last_missed_at[sym] < 900:
            continue
        prev = before(hist, t - 900)
        if not prev or f(prev[1]) <= 0:
            continue
        cur = f(hist[-1][1])
        r15 = pct(cur, f(prev[1]))
        if r15 < 5.0:
            continue
        prior = [
            pred for pred in list(pending_predictions) + list(resolved_predictions)
            if pred.get("symbol") == sym and t - 1200 <= f(pred.get("created")) <= t
        ]
        best = max((f(pred.get("score")) for pred in prior), default=0.0)
        missed_moves.append({
            "symbol": sym,
            "detected": t,
            "return_15m_pct": round(r15, 3),
            "had_v11_prediction": bool(prior),
            "best_prior_score": round(best, 2),
        })
        last_missed_at[sym] = t
        print(
            f"Ψ-V11 MISSED_MOVE symbol={sym} r15={r15:+.2f}% "
            f"hadPrediction={'YES' if prior else 'NO'} bestScore={best:.1f}",
            flush=True,
        )



def load_outcomes():
    try:
        if not os.path.exists(V11_OUTCOME_FILE):
            return
        with open(V11_OUTCOME_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        for row in (data.get("pending") or [])[-1200:]:
            if isinstance(row, dict):
                pending_predictions.append(row)
        for row in (data.get("resolved") or [])[-5000:]:
            if isinstance(row, dict):
                resolved_predictions.append(row)
        for row in (data.get("missed_moves") or [])[-500:]:
            if isinstance(row, dict):
                missed_moves.append(row)
        print(
            f"Ψ-V11 OUTCOME_LOAD pending={len(pending_predictions)} "
            f"resolved={len(resolved_predictions)} missed={len(missed_moves)}",
            flush=True,
        )
    except Exception as exc:
        print(f"Ψ-V11 OUTCOME_LOAD_ERROR {type(exc).__name__}: {exc}", flush=True)



def persist_outcomes(force=False):
    t = now()
    if not force and t - engine_stats["last_persist"] < 120:
        return
    engine_stats["last_persist"] = t
    try:
        os.makedirs(os.path.dirname(V11_OUTCOME_FILE) or ".", exist_ok=True)
        tmp = V11_OUTCOME_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump({
                "version": VERSION,
                "saved": t,
                "pending": pending_predictions[-1200:],
                "resolved": list(resolved_predictions)[-5000:],
                "missed_moves": list(missed_moves)[-500:],
            }, handle, separators=(",", ":"))
        os.replace(tmp, V11_OUTCOME_FILE)
    except Exception as exc:
        print(f"Ψ-V11 OUTCOME_SAVE_ERROR {type(exc).__name__}: {exc}", flush=True)


async def event_engine_loop():
    while True:
        await asyncio.sleep(V11_SAMPLE_SECONDS)
        try:
            for sym in list(app.selected_micro_symbols):
                event_state[sym] = event_microstructure(sym)
                fatigue_state[sym] = resistance_fatigue(sym)
            engine_stats["event_cycles"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine_stats["errors"] += 1
            print(f"Ψ-V11 EVENT_ERROR {type(exc).__name__}: {exc}", flush=True)


async def cross_venue_loop():
    while True:
        await asyncio.sleep(V11_XVENUE_SECONDS)
        try:
            await poll_cross_venues()
            t = now()
            # Bulk endpoints make it cheap enough to maintain lead/lag history
            # for the whole Binance crypto universe, avoiding candidate bias.
            for sym in list(q.universe):
                for venue in ("okx", "bybit"):
                    price = f(venue_prices.get(venue, {}).get(sym))
                    if price > 0:
                        venue_history[sym][venue].append((t, price))
                xvenue_state[sym] = cross_venue_leadlag(sym)
            engine_stats["xvenue_cycles"] += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine_stats["errors"] += 1
            print(f"Ψ-V11 XVENUE_ERROR {type(exc).__name__}: {exc}", flush=True)


async def outcome_loop():
    while True:
        await asyncio.sleep(5.0)
        try:
            t = now()
            for sym in list(q.universe):
                price = raw_binance_price(sym)
                if price > 0:
                    hist = price_history[sym]
                    if not hist or t - hist[-1][0] >= 4.5:
                        hist.append((t, price))
            update_outcomes()
            detect_missed_moves()
            persist_outcomes()
        except asyncio.CancelledError:
            persist_outcomes(force=True)
            raise
        except Exception as exc:
            engine_stats["errors"] += 1
            print(f"Ψ-V11 OUTCOME_ERROR {type(exc).__name__}: {exc}", flush=True)


async def prediction_loop():
    while True:
        await asyncio.sleep(V11_PREDICT_SECONDS)
        try:
            rows = build_v11_board()
            t = now()
            created = 0
            for row in rows[:25]:
                sym = row["symbol"]
                if row.get("predictive_state") not in ("V11-PRIME", "V11-EARLY"):
                    continue
                if t - last_prediction_at[sym] < 120:
                    continue
                snap = _prediction_snapshot(row)
                if f(snap.get("entry_price")) <= 0:
                    continue
                pending_predictions.append(snap)
                last_prediction_at[sym] = t
                created += 1
            engine_stats["prediction_cycles"] += 1
            if created:
                print(
                    f"Ψ-V11 SHADOW_PREDICTIONS created={created} "
                    f"pending={len(pending_predictions)} resolved={len(resolved_predictions)}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine_stats["errors"] += 1
            print(f"Ψ-V11 PREDICT_ERROR {type(exc).__name__}: {exc}", flush=True)



def _fmt_prob(value):
    return f"{100.0 * f(value):.1f}%"


async def v11_print_loop():
    while True:
        await asyncio.sleep(V11_PRINT_SECONDS)
        try:
            rows = build_v11_board()
            cal = calibration_summary()
            ready_ev = sum(bool((event_state.get(sym) or {}).get("ready")) for sym in app.selected_micro_symbols)
            xcov = sum(bool((xvenue_state.get(sym) or {}).get("ready")) for sym in q.universe)
            prime = sum(row.get("predictive_state") == "V11-PRIME" for row in rows)
            early = sum(row.get("predictive_state") == "V11-EARLY" for row in rows)
            formal_buy = sum(
                str((q.latest.get(row["symbol"]) or {}).get("state")) == "BUY NOW"
                for row in rows
            )
            print(
                f"Ψ-V11 PREDICTIVE BOARD {len(rows)}/{V11_MAX_BOARD} "
                f"prime={prime} early={early} formal_buy={formal_buy} "
                f"mode={'SHADOW' if V11_SHADOW_ONLY else 'ACTIVE_RESEARCH'}",
                flush=True,
            )
            for i, row in enumerate(rows[:15], 1):
                hz = row.get("hazard") or {}
                ft = row.get("fatigue") or {}
                xv = row.get("cross_venue") or {}
                print(
                    f"V{i:02d}. {row['symbol']:<14} state={row['predictive_state']:<10} "
                    f"score={f(row['score']):5.1f} event={f(row['event_score']):5.1f} "
                    f"fatigue={f(row['fatigue_score']):5.1f} xlead={f(row['xvenue_score']):5.1f} "
                    f"pB15={_fmt_prob(hz.get('raw_p_breakout_15m'))} "
                    f"eta={f(hz.get('expected_breakout_minutes')):5.1f}m "
                    f"p5/10/20={_fmt_prob(hz.get('raw_p_up5_60m'))}/"
                    f"{_fmt_prob(hz.get('raw_p_up10_60m'))}/"
                    f"{_fmt_prob(hz.get('raw_p_up20_60m'))} "
                    f"attacks={int(f(ft.get('attacks')))} xv={int(f(xv.get('venues')))} "
                    f"layers={int(f(row.get('layers')))} formal={row.get('formal_state')}",
                    flush=True,
                )
            print(
                f"Ψ-V11 HEALTH eventReady={ready_ev}/{len(app.selected_micro_symbols)} "
                f"xvenueReady={xcov}/{len(q.universe)} "
                f"okx={venue_stats['okx_ok']}/{venue_stats['okx_err']} "
                f"bybit={venue_stats['bybit_ok']}/{venue_stats['bybit_err']} "
                f"outcomes={cal['status']} n={cal['n']} brier={cal['brier_break15']} "
                f"pending={len(pending_predictions)} missed={len(missed_moves)} "
                f"errors={engine_stats['errors']}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            engine_stats["errors"] += 1
            print(f"Ψ-V11 PRINT_ERROR {type(exc).__name__}: {exc}", flush=True)


load_outcomes()

base.VERSION = VERSION
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.0] Predictive Market Physics active: event-time derivatives, "
        "resistance fatigue, OKX/Bybit lead-lag, survival hazard, candidate "
        "promotion, persistent walk-forward outcomes. V10 formal PRE/BUY/PUMP "
        "gates unchanged.",
        flush=True,
    )
    print(
        "Ψ-V11 SAFETY predictive layers start SHADOW/RESEARCH only; they rank, "
        "promote telemetry coverage and learn but cannot manufacture BUY NOW.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        event_engine_loop(),
        cross_venue_loop(),
        outcome_loop(),
        prediction_loop(),
        v11_print_loop(),
    )


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        persist_outcomes(force=True)
        print("Psi-V11 stopped", flush=True)
