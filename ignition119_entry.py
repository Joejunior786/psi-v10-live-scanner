import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque

import aiohttp
import ignition1185_entry as base

scanner = base.scanner
b184 = base.base
b18 = base.b18
b17 = base.b17
b16 = base.b16
q = scanner.q
app = scanner.app

VERSION = "10.19.0-validation-execution-intelligence"

# -----------------------------------------------------------------------------
# Persistent state
# -----------------------------------------------------------------------------
PERSIST_DIR = "/data" if os.path.isdir("/data") else "/app"
V119_STATE_PATH = os.environ.get("PSI_V119_STATE_PATH", os.path.join(PERSIST_DIR, "psi_v1019_state.json"))
BASE_LEARN_PATH = os.environ.get("PSI_LEARN_STATE_PATH", os.path.join(PERSIST_DIR, "psi_v10185_learning.json"))
base.LEARN_STATE_PATH = BASE_LEARN_PATH
try:
    base.load_state()
except Exception:
    pass

# -----------------------------------------------------------------------------
# Sampling / guardrails
# -----------------------------------------------------------------------------
MTF_SAMPLE_SECONDS = 40.0
MTF_MAX_SYMBOLS = 4
DEPTH_SAMPLE_SECONDS = 20.0
DEPTH_MAX_SYMBOLS = 4
FLOW_SAMPLE_SECONDS = 5.0
FUTURES_SAMPLE_SECONDS = 30.0
FUTURES_MAX_SYMBOLS = 10
MARKET_SAMPLE_SECONDS = 15.0
STATE_SAVE_SECONDS = 60.0
PRINT_SECONDS = float(getattr(app, "PRINT_SECONDS", 30))

STALE_PENALTY_SECONDS = 120.0
STALE_DEMOTE_SECONDS = 240.0
STALE_EVICT_SECONDS = 300.0
STALE_DV_EPS = 0.03

SHADOW_COOLDOWN = 1800.0
SHADOW_MAX_PENDING = 500
SHADOW_MAX_RESOLVED = 5000
SHADOW_HORIZON = 4 * 3600.0

FUTURES_REST = os.environ.get("BINANCE_FUTURES_REST", "https://fapi.binance.com").rstrip("/")

# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------
mtf_cache = {}
mtf_memory = defaultdict(dict)
depth_cache = {}
depth_hist = defaultdict(lambda: deque(maxlen=16))
flow_hist = defaultdict(lambda: deque(maxlen=120))
flow_cache = {}
futures_cache = {}
market_context = {}
shadow_pending = []
shadow_resolved = deque(maxlen=SHADOW_MAX_RESOLVED)
shadow_last = defaultdict(float)
shadow_seq = 0
calibration = {
    "status": "WARMING",
    "quality_threshold": 60.0,
    "train_n": 0,
    "valid_n": 0,
    "train_utility": 0.0,
    "valid_utility": 0.0,
    "baseline_valid_utility": 0.0,
    "updated": 0.0,
}
stats = {
    "mtf_samples": 0, "mtf_errors": 0,
    "depth_samples": 0, "depth_errors": 0,
    "flow_samples": 0,
    "futures_samples": 0, "futures_errors": 0,
    "market_samples": 0, "market_errors": 0,
    "shadow_opened": 0, "shadow_closed": 0,
    "state_loaded": False, "save_errors": 0,
}

_old_diag = b17.diag_pool
_old_opp = b17.opp_score
_old_pump = b18.pump_signature
_old_hot = q.hot
_old_breakout_strict = b16._breakout_strict
_old_feature_snapshot = base.feature_snapshot
_old_pattern_keys = base.pattern_keys
_old_main = scanner.v7.main

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if a and b else 0.0

def median(vals, default=0.0):
    xs = [f(x) for x in vals if x is not None]
    return statistics.median(xs) if xs else default

def base_asset(sym):
    s = str(sym or "").upper()
    return s[:-4] if s.endswith("USDT") else s

def candidate_symbols(limit=30):
    out, seen = [], set()
    try:
        for score, sym in _old_hot(max(limit * 2, 40)):
            if sym not in seen and b17.directional(sym):
                out.append(sym); seen.add(sym)
            if len(out) >= limit:
                return out
    except Exception:
        pass
    for sym in list(app.selected_micro_symbols):
        if sym not in seen and b17.directional(sym):
            out.append(sym); seen.add(sym)
        if len(out) >= limit:
            return out
    ranked = []
    for sym, row in q.latest.items():
        if sym in seen or not b17.directional(sym):
            continue
        ranked.append((f(row.get("ignition15_score")) + f(row.get("anomaly_score")), sym))
    ranked.sort(reverse=True)
    for _, sym in ranked:
        out.append(sym); seen.add(sym)
        if len(out) >= limit:
            break
    return out

def current_price(sym):
    try:
        return f(app.current_symbol_price(sym), f((q.latest.get(sym) or {}).get("price")))
    except Exception:
        return f((q.latest.get(sym) or {}).get("price"))

# -----------------------------------------------------------------------------
# 1m / 3m / 5m / 15m breakout lifecycle
# -----------------------------------------------------------------------------
TF_CFG = {
    "1m": {"lookback": 10, "ttl": 12 * 60},
    "3m": {"lookback": 9, "ttl": 20 * 60},
    "5m": {"lookback": 8, "ttl": 30 * 60},
    "15m": {"lookback": 7, "ttl": 60 * 60},
}

def candle_rows(rows):
    out = []
    for r in rows or []:
        if not r or len(r) < 7:
            continue
        out.append({
            "open_ms": int(r[0]), "open": f(r[1]), "high": f(r[2]),
            "low": f(r[3]), "close": f(r[4]), "close_ms": int(r[6]),
        })
    return out

def infer_tf_state(sym, tf, rows, now=None):
    now = now or time.time()
    cs = candle_rows(rows)
    if len(cs) < 8:
        return {"tf": tf, "phase": "UNKNOWN", "score": 0.0}
    closed, live = cs[:-1], cs[-1]
    cfg = TF_CFG[tf]
    look = int(cfg["lookback"])
    src = closed[max(0, len(closed) - look - 2): max(1, len(closed) - 2)]
    resistance = max((x["high"] for x in src), default=0.0)
    support = min((x["low"] for x in closed[-look:]), default=0.0)
    price = live["close"] or closed[-1]["close"]
    mem = dict(mtf_memory[sym].get(tf) or {})
    used = f(mem.get("used_level"))
    used_until = f(mem.get("used_until"))
    peak = f(mem.get("peak"))
    break_ts = f(mem.get("break_ts"))

    recent = closed[-2:] + [live]
    broke = resistance > 0 and any(x["high"] >= resistance * 1.0008 for x in recent)
    same = used > 0 and resistance > 0 and abs(pct(resistance, used)) <= 0.5
    if broke and (used_until <= now or not same):
        used = resistance
        used_until = now + cfg["ttl"]
        peak = max(x["high"] for x in recent)
        break_ts = now

    level = used if used_until > now and used > 0 else resistance
    above = pct(price, level) if level > 0 else -999
    recent_peak = max([x["high"] for x in recent] + ([peak] if peak > 0 else []))
    drawdown = pct(price, recent_peak) if recent_peak > 0 else 0.0
    ret1 = pct(price, closed[-1]["close"]) if closed[-1]["close"] else 0.0
    falling = ret1 < 0 and drawdown <= -0.25

    if used_until > now and used > 0:
        peak = max(peak, recent_peak)
        if above <= -0.30:
            phase = "REJECT_FALLING" if falling else "FAILED_BREAKOUT"
        elif abs(above) <= 0.35:
            phase = "RETEST"
        elif above >= 2.0:
            phase = "NO_CHASE"
        elif above >= 0.65:
            phase = "CONTINUATION"
        elif above >= 0.18:
            phase = "BREAKOUT_HOLD"
        else:
            phase = "BREAKOUT_IN_PROGRESS"
    else:
        used = 0.0; used_until = 0.0; peak = 0.0; break_ts = 0.0
        dist = pct(resistance, price) if resistance > 0 else 999.0
        phase = "APPROACH" if 0 <= dist <= 1.5 else ("BELOW_RESISTANCE" if resistance > 0 else "UNKNOWN")

    score_map = {
        "APPROACH": 8, "BREAKOUT_IN_PROGRESS": 7, "BREAKOUT_HOLD": 8,
        "RETEST": 6, "CONTINUATION": 7, "BELOW_RESISTANCE": 2,
        "FAILED_BREAKOUT": -8, "REJECT_FALLING": -10, "NO_CHASE": -7,
        "UNKNOWN": 0,
    }
    result = {
        "tf": tf, "phase": phase, "price": price,
        "resistance": resistance or None, "support": support or None,
        "used_level": used or None, "used_until": used_until or None,
        "distance_pct": None if level <= 0 else round(pct(level, price), 4),
        "drawdown_pct": round(drawdown, 4), "score": float(score_map.get(phase, 0)),
        "updated": now, "break_age": None if not break_ts else round(now - break_ts, 1),
    }
    mtf_memory[sym][tf] = {
        "used_level": used, "used_until": used_until, "peak": peak,
        "break_ts": break_ts, "phase": phase,
    }
    return result

def mtf_summary(sym):
    x = mtf_cache.get(sym) or {}
    frames = x.get("frames") or {}
    if not frames:
        return {
            "mtf_state": "WAIT", "mtf_score": 0.0, "mtf_alignment": 0,
            "mtf_1m": "UNKNOWN", "mtf_3m": "UNKNOWN", "mtf_5m": "UNKNOWN", "mtf_15m": "UNKNOWN",
        }
    phases = {tf: str((frames.get(tf) or {}).get("phase") or "UNKNOWN") for tf in TF_CFG}
    positives = {"APPROACH", "BREAKOUT_IN_PROGRESS", "BREAKOUT_HOLD", "RETEST", "CONTINUATION"}
    failures = {"FAILED_BREAKOUT", "REJECT_FALLING", "NO_CHASE"}
    alignment = sum(phases[tf] in positives for tf in TF_CFG)
    fail_count = sum(phases[tf] in failures for tf in TF_CFG)
    weighted = sum(f((frames.get(tf) or {}).get("score")) * w for tf, w in (("1m", .7), ("3m", 1.0), ("5m", 1.3), ("15m", 1.1)))
    if fail_count >= 2 or phases["5m"] in ("FAILED_BREAKOUT", "REJECT_FALLING"):
        state = "MTF-REJECT"
    elif alignment >= 3 and phases["5m"] in positives:
        state = "MTF-ALIGNED"
    elif alignment >= 2:
        state = "MTF-MIXED-BULL"
    else:
        state = "MTF-WAIT"
    return {
        "mtf_state": state, "mtf_score": round(clamp(50 + weighted * 1.4, 0, 100), 1),
        "mtf_alignment": alignment,
        "mtf_1m": phases["1m"], "mtf_3m": phases["3m"],
        "mtf_5m": phases["5m"], "mtf_15m": phases["15m"],
        "mtf_support_5m": (frames.get("5m") or {}).get("support"),
        "mtf_resistance_5m": (frames.get("5m") or {}).get("resistance"),
    }

async def fetch_mtf(sym):
    if app.session is None:
        return
    try:
        rows = await asyncio.gather(*(app.load_klines(app.session, sym, tf, 26) for tf in TF_CFG))
        now = time.time()
        frames = {tf: infer_tf_state(sym, tf, r, now) for tf, r in zip(TF_CFG, rows)}
        mtf_cache[sym] = {"updated": now, "frames": frames}
    except asyncio.CancelledError:
        raise
    except Exception:
        stats["mtf_errors"] += 1

async def mtf_loop():
    while True:
        await asyncio.sleep(MTF_SAMPLE_SECONDS)
        try:
            # MTF REST is execution-tier enrichment only. Full-universe early
            # discovery is already supplied by the WebSocket radar/tape stack.
            selected=set(getattr(app,"selected_micro_symbols",[]) or [])
            if not selected:
                continue
            syms=[
                s for s in candidate_symbols(max(20,MTF_MAX_SYMBOLS*5))
                if s in selected
            ][:MTF_MAX_SYMBOLS]
            if not syms:
                continue
            await asyncio.gather(*(fetch_mtf(sym) for sym in syms))
            stats["mtf_samples"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["mtf_errors"] += 1

# -----------------------------------------------------------------------------
# Liquidity-vacuum / ask-wall depletion
# -----------------------------------------------------------------------------
def depth_band(snapshot, side, bps):
    mid = f(snapshot.get("mid"))
    rows = snapshot.get(side) or []
    if mid <= 0:
        return 0.0
    total = 0.0
    for price, qty in rows:
        p, qv = f(price), f(qty)
        if p <= 0 or qv <= 0:
            continue
        dist = abs(pct(p, mid)) * 100.0
        if dist <= bps:
            total += p * qv
    return total

def historical_depth(sym, seconds):
    dq = depth_hist.get(sym)
    if not dq:
        return None
    now = dq[-1][0]
    old = None
    for item in reversed(dq):
        if item[0] <= now - seconds:
            old = item[1]
            break
    return old or (dq[0][1] if len(dq) >= 2 else None)

def vacuum_metrics(sym):
    cur = depth_cache.get(sym)
    if not cur:
        return {"liquidity_vacuum_score": 50.0, "liquidity_vacuum_state": "WAIT", "ask_depletion_30s_pct": 0.0}
    bid25 = depth_band(cur, "bids", 25)
    ask25 = depth_band(cur, "asks", 25)
    bid50 = depth_band(cur, "bids", 50)
    ask50 = depth_band(cur, "asks", 50)
    old = historical_depth(sym, 30)
    old_ask25 = depth_band(old, "asks", 25) if old else 0.0
    old_bid25 = depth_band(old, "bids", 25) if old else 0.0
    ask_dep = ((old_ask25 - ask25) / old_ask25 * 100.0) if old_ask25 > 0 else 0.0
    bid_change = ((bid25 - old_bid25) / old_bid25 * 100.0) if old_bid25 > 0 else 0.0
    imbalance25 = (bid25 - ask25) / (bid25 + ask25) if bid25 + ask25 > 0 else 0.0
    imbalance50 = (bid50 - ask50) / (bid50 + ask50) if bid50 + ask50 > 0 else 0.0
    score = 50.0
    score += clamp(imbalance25 * 30.0, -20, 20)
    score += clamp(imbalance50 * 18.0, -12, 12)
    score += clamp(ask_dep * 0.35, -15, 20)
    score += clamp(bid_change * 0.15, -8, 10)
    score = round(clamp(score, 0, 100), 1)
    if score >= 78 and ask_dep >= 12:
        state = "VACUUM-HIGH"
    elif score >= 65:
        state = "VACUUM-BUILDING"
    elif score <= 35:
        state = "ASK-HEAVY"
    else:
        state = "BALANCED"
    return {
        "liquidity_vacuum_score": score, "liquidity_vacuum_state": state,
        "ask_depletion_30s_pct": round(ask_dep, 2), "bid_change_30s_pct": round(bid_change, 2),
        "depth_imbalance_25bps": round(imbalance25, 4), "depth_imbalance_50bps": round(imbalance50, 4),
        "ask_notional_25bps": round(ask25, 2), "bid_notional_25bps": round(bid25, 2),
    }

async def fetch_depth(sym):
    """Mirror the canonical live depth20 book into the vacuum cache."""
    try:
        s=app.ensure_micro_state(sym)
        if not s.get("book_snapshot_ready"):
            return
        bids=app.sorted_levels(s.get("book_bids") or {},True)[:20]
        asks=app.sorted_levels(s.get("book_asks") or {},False)[:20]
        if not bids or not asks:
            return
        best_bid,best_ask=f(bids[0][0]),f(asks[0][0])
        mid=(best_bid+best_ask)/2.0 if best_bid and best_ask else 0.0
        snap={
            "updated":time.time(),
            "mid":mid,
            "bids":[[px,qty] for px,qty in bids],
            "asks":[[px,qty] for px,qty in asks],
        }
        depth_cache[sym]=snap
        depth_hist[sym].append((snap["updated"],snap))
    except asyncio.CancelledError:
        raise
    except Exception:
        stats["depth_errors"]+=1

async def depth_loop():
    while True:
        await asyncio.sleep(DEPTH_SAMPLE_SECONDS)
        try:
            # Full-universe order-flow discovery already comes from WebSockets.
            # REST depth is reserved for symbols in the continuity execution pool.
            selected=set(getattr(app,"selected_micro_symbols",[]) or [])
            if not selected:
                continue
            ranked=[
                s for s in candidate_symbols(max(20,DEPTH_MAX_SYMBOLS*5))
                if s in selected
            ]
            syms=ranked[:DEPTH_MAX_SYMBOLS]
            if not syms:
                continue
            await asyncio.gather(*(fetch_depth(sym) for sym in syms))
            stats["depth_samples"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["depth_errors"] += 1

# -----------------------------------------------------------------------------
# CVD / OFI acceleration divergence
# -----------------------------------------------------------------------------
def flow_point(sym):
    row = q.latest.get(sym) or {}
    ranks = row.get("relative_ranks") or {}
    return {
        "price": current_price(sym),
        "cvd": f(ranks.get("cvd_acc"), 0.5),
        "ofi": f(ranks.get("ofi"), 0.5),
        "buy": f(row.get("aggressive_buy_ratio"), 0.5),
    }

def flow_delta(sym, seconds):
    dq = flow_hist.get(sym)
    if not dq or len(dq) < 2:
        return None
    now, cur = dq[-1]
    old = dq[0]
    for x in reversed(dq):
        if x[0] <= now - seconds:
            old = x
            break
    return cur, old[1]

def flow_divergence(sym):
    pair = flow_delta(sym, 30)
    if not pair:
        return {"flow_divergence_score": 50.0, "flow_divergence_state": "WAIT", "cvd_delta_30s": 0.0, "ofi_delta_30s": 0.0}
    cur, old = pair
    cdelta = f(cur.get("cvd")) - f(old.get("cvd"))
    odelta = f(cur.get("ofi")) - f(old.get("ofi"))
    pchange = pct(f(cur.get("price")), f(old.get("price"))) if f(old.get("price")) > 0 else 0.0
    buy = f(cur.get("buy"), .5)
    score = 50.0 + clamp(cdelta * 65, -20, 24) + clamp(odelta * 65, -20, 24)
    score += clamp((buy - .5) * 45, -10, 12)
    if abs(pchange) <= 0.35 and cdelta >= .10 and odelta >= .10:
        score += 10
    score = round(clamp(score, 0, 100), 1)
    if score >= 78 and abs(pchange) <= 0.45:
        state = "FLOW-LEADS-PRICE"
    elif score >= 65:
        state = "FLOW-ACCELERATING"
    elif score <= 35:
        state = "FLOW-WEAK"
    else:
        state = "FLOW-NEUTRAL"
    return {
        "flow_divergence_score": score, "flow_divergence_state": state,
        "cvd_delta_30s": round(cdelta, 4), "ofi_delta_30s": round(odelta, 4),
        "flow_price_change_30s_pct": round(pchange, 4),
    }

async def flow_loop():
    while True:
        await asyncio.sleep(FLOW_SAMPLE_SECONDS)
        now = time.time()
        syms = candidate_symbols(40)
        for sym in syms:
            p = flow_point(sym)
            flow_hist[sym].append((now, p))
            while flow_hist[sym] and flow_hist[sym][0][0] < now - 600:
                flow_hist[sym].popleft()
            flow_cache[sym] = flow_divergence(sym)
        stats["flow_samples"] += 1

# -----------------------------------------------------------------------------
# Spot-vs-futures lead / lag telemetry
# -----------------------------------------------------------------------------
async def futures_klines(sym, limit=8):
    if app.session is None:
        return None
    try:
        async with app.session.get(
            FUTURES_REST + "/fapi/v1/klines",
            params={"symbol": sym, "interval": "1m", "limit": limit},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            return data if isinstance(data, list) else None
    except Exception:
        return None

def rows_return(rows, n):
    if not rows or len(rows) < n + 2:
        return 0.0
    closes = [f(x[4]) for x in rows]
    if closes[-1] <= 0 or closes[-1-n] <= 0:
        return 0.0
    return pct(closes[-1], closes[-1-n])

async def fetch_spot_futures(sym):
    try:
        fut = await futures_klines(sym, 8)
        an = app.anomaly_state.get(sym) or {}
        spot1 = f(an.get("return_1m_pct"))
        spot3 = f(an.get("return_3m_pct"))
        if not fut:
            futures_cache[sym] = {"updated": time.time(), "spot_futures_state": "NO-FUTURES", "spot_futures_score": 50.0}
            return
        fut1, fut3 = rows_return(fut, 1), rows_return(fut, 3)
        excess1, excess3 = spot1 - fut1, spot3 - fut3
        if spot3 > 0 and excess3 >= 0.18:
            state = "SPOT-LEADING"
        elif fut3 > 0 and excess3 <= -0.22:
            state = "FUTURES-LEADING"
        else:
            state = "CO-MOVING"
        score = round(clamp(50 + excess3 * 35 + excess1 * 20, 0, 100), 1)
        futures_cache[sym] = {
            "updated": time.time(), "spot_futures_state": state, "spot_futures_score": score,
            "spot_1m_pct": round(spot1, 4), "spot_3m_pct": round(spot3, 4),
            "futures_1m_pct": round(fut1, 4), "futures_3m_pct": round(fut3, 4),
            "spot_minus_futures_3m_pct": round(excess3, 4),
        }
    except asyncio.CancelledError:
        raise
    except Exception:
        stats["futures_errors"] += 1

async def futures_loop():
    while True:
        await asyncio.sleep(FUTURES_SAMPLE_SECONDS)
        syms = candidate_symbols(FUTURES_MAX_SYMBOLS)
        try:
            await asyncio.gather(*(fetch_spot_futures(sym) for sym in syms))
            stats["futures_samples"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["futures_errors"] += 1

# -----------------------------------------------------------------------------
# Market + sector relative strength and regime
# -----------------------------------------------------------------------------
SECTORS = {}
def _sector(name, symbols):
    for x in symbols.split():
        SECTORS[x] = name

_sector("L1", "SOL AVAX SUI APT SEI NEAR ADA ATOM INJ TON TRX HBAR ALGO ICP DOT EGLD KAS")
_sector("L2", "ARB OP STRK ZK MANTA METIS POL IMX")
_sector("DEFI", "AAVE UNI CRV MKR SNX PENDLE LDO JUP RAY CAKE COMP DYDX ENA")
_sector("AI", "RENDER FET TAO WLD GRT ARKM AI PHB NMR")
_sector("MEME", "DOGE SHIB PEPE WIF BONK FLOKI PUMP TURBO NEIRO")
_sector("GAMING", "GALA SAND MANA AXS ENJ ILV PIXEL PORTAL")
_sector("ORACLE", "LINK API3 BAND TRB PYTH DIA")
_sector("STORAGE", "FIL AR STORJ SC")
_sector("PAYMENTS", "XRP XLM LTC BCH DASH ZEC")
_sector("RWA", "ONDO OM POLYX CFG")
_sector("DEX", "JUP RAY CAKE UNI SUSHI 1INCH")
_sector("INFRA", "TIA ICP AR DOT ATOM NEAR AVAX HBAR")
_sector("PRIVACY", "ZEC XMR DASH")

async def market_ref(sym):
    rows = await app.load_klines(app.session, sym, "1m", 18)
    if not rows or len(rows) < 7:
        return None
    return {"r1": rows_return(rows, 1), "r3": rows_return(rows, 3), "r5": rows_return(rows, 5)}

def market_regime():
    btc_state = str(b184.btc_context.get("state") or "UNKNOWN")
    refs = market_context.get("refs") or {}
    r3s = [f(v.get("r3")) for v in refs.values() if isinstance(v, dict)]
    r5s = [f(v.get("r5")) for v in refs.values() if isinstance(v, dict)]
    bench3 = median(r3s)
    bench5 = median(r5s)
    breadth = sum(x > 0 for x in r3s) / len(r3s) if r3s else 0.5
    vol = median([abs(x) for x in r5s])
    if btc_state == "RISK_OFF" or bench5 <= -0.8:
        regime = "RISK_OFF"
    elif vol >= 0.9:
        regime = "HIGH_VOL"
    elif bench3 >= 0.20 and breadth >= 0.66:
        regime = "TREND_UP"
    elif vol <= 0.18 and abs(bench3) <= 0.12:
        regime = "QUIET"
    elif btc_state == "WEAKENING" or bench3 < 0:
        regime = "WEAKENING"
    else:
        regime = "NORMAL"
    return {
        "market_regime_119": regime, "market_benchmark_3m_pct": round(bench3, 4),
        "market_benchmark_5m_pct": round(bench5, 4), "market_breadth_3m": round(breadth, 3),
        "market_volatility_proxy": round(vol, 4),
    }

def sector_relative(sym):
    an = app.anomaly_state.get(sym) or {}
    alt3 = f(an.get("return_3m_pct"))
    market = market_regime()
    market3 = f(market.get("market_benchmark_3m_pct"))
    sector = SECTORS.get(base_asset(sym), "OTHER")
    peers = []
    for psym, row in app.anomaly_state.items():
        if psym == sym or SECTORS.get(base_asset(psym), "OTHER") != sector:
            continue
        if isinstance(row, dict) and row.get("return_3m_pct") is not None:
            peers.append(f(row.get("return_3m_pct")))
    sector3 = median(peers, market3) if len(peers) >= 2 else market3
    sector_excess = alt3 - sector3
    market_excess = alt3 - market3
    score = round(clamp(50 + sector_excess * 22 + market_excess * 12, 0, 100), 1)
    state = "SECTOR-LEADER" if score >= 70 else ("SECTOR-LAGGER" if score <= 35 else "SECTOR-NEUTRAL")
    return {
        "sector_119": sector, "sector_rs_score": score, "sector_rs_state": state,
        "sector_excess_3m_pct": round(sector_excess, 4),
        "market_excess_3m_pct": round(market_excess, 4),
        "sector_peer_count": len(peers),
        **market,
    }

async def market_loop():
    while True:
        await asyncio.sleep(MARKET_SAMPLE_SECONDS)
        if app.session is None:
            continue
        try:
            syms = ("BTCUSDT", "ETHUSDT", "SOLUSDT")
            vals = await asyncio.gather(*(market_ref(x) for x in syms))
            market_context["refs"] = {s: v for s, v in zip(syms, vals) if v}
            market_context["updated"] = time.time()
            stats["market_samples"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            stats["market_errors"] += 1

# -----------------------------------------------------------------------------
# Regime-adaptive early thresholds
# -----------------------------------------------------------------------------
def adaptive_thresholds():
    regime = market_regime()["market_regime_119"]
    mapping = {
        "QUIET": (50.0, 68.0),
        "TREND_UP": (52.0, 69.0),
        "NORMAL": (52.0, 70.0),
        "WEAKENING": (55.0, 73.0),
        "HIGH_VOL": (58.0, 76.0),
        "RISK_OFF": (64.0, 82.0),
    }
    watch, armed = mapping.get(regime, (52.0, 70.0))
    if calibration.get("status") == "ACTIVE":
        qth = f(calibration.get("quality_threshold"), 60.0)
        shift = clamp((qth - 60.0) * 0.15, -2.0, 2.0)
        watch += shift
        armed += shift
    return regime, round(watch, 1), round(armed, 1)

# -----------------------------------------------------------------------------
# Stagnation killer
# -----------------------------------------------------------------------------
def stagnation_metrics(row, pump=None):
    stale = f(row.get("no_progress_seconds"))
    dv30 = f(row.get("distance_velocity_30s_per_min"))
    rapid = f((pump or {}).get("rapid_score"), f(row.get("rapid_score")))
    pscore = f((pump or {}).get("pump_signature_score"), f(row.get("pump_signature_score")))
    inf = b184.btc_influence(str(row.get("symbol") or ""))
    leader = str(inf.get("btc_relationship") or "") in ("BTC-DIVERGENT-LEADER", "RELATIVE-STRENGTH-LEADER")
    exempt = rapid >= 150 or pscore >= 78 or leader
    if exempt:
        state, penalty = "ACTIVE-EXEMPT", 0.0
    elif stale >= STALE_EVICT_SECONDS and dv30 <= STALE_DV_EPS:
        state, penalty = "STALE-EVICT", 42.0
    elif stale >= STALE_DEMOTE_SECONDS and dv30 <= STALE_DV_EPS:
        state, penalty = "STALE-DEMOTE", 28.0
    elif stale >= STALE_PENALTY_SECONDS and dv30 <= STALE_DV_EPS:
        state, penalty = "STALE-PENALTY", 12.0
    else:
        state, penalty = "ACTIVE", 0.0
    return {"stagnation_state_119": state, "stagnation_penalty_119": penalty}

# -----------------------------------------------------------------------------
# Combined early intelligence
# -----------------------------------------------------------------------------
def intelligence(sym):
    vac = vacuum_metrics(sym)
    flow = flow_cache.get(sym) or flow_divergence(sym)
    fut = futures_cache.get(sym) or {"spot_futures_state": "WAIT", "spot_futures_score": 50.0}
    sec = sector_relative(sym)
    mtf = mtf_summary(sym)
    return {**vac, **flow, **fut, **sec, **mtf}

def early_quality(sym, ps=None):
    ps = ps or _old_pump(sym)
    intel = intelligence(sym)
    return round(clamp(
        .40 * f(ps.get("pump_signature_score"))
        + .20 * f(ps.get("btc_rs_score"), 50)
        + .18 * f(intel.get("liquidity_vacuum_score"), 50)
        + .12 * f(intel.get("flow_divergence_score"), 50)
        + .10 * f(intel.get("sector_rs_score"), 50),
        0, 100
    ), 1)

# -----------------------------------------------------------------------------
# V10.19 diagnostics / ranking
# -----------------------------------------------------------------------------
def diag119():
    out = []
    for raw in _old_diag():
        row = dict(raw)
        sym = str(row.get("symbol") or "")
        if not sym:
            out.append(row); continue
        ps = _old_pump(sym)
        intel = intelligence(sym)
        row.update(intel)
        row.update(stagnation_metrics(row, ps))
        row["early_quality_119"] = early_quality(sym, ps)
        regime, watch, armed = adaptive_thresholds()
        row["pump_watch_threshold_119"] = watch
        row["pump_armed_threshold_119"] = armed
        row["regime_119"] = regime
        out.append(row)
    return out

def opp119(row):
    score = f(_old_opp(row))
    sym = str(row.get("symbol") or "")
    intel = row if row.get("liquidity_vacuum_score") is not None else intelligence(sym)
    score += clamp((f(intel.get("liquidity_vacuum_score"), 50) - 50) / 5.0, -8, 10)
    score += clamp((f(intel.get("flow_divergence_score"), 50) - 50) / 6.0, -7, 8)
    score += clamp((f(intel.get("sector_rs_score"), 50) - 50) / 8.0, -5, 6)
    sf = str(intel.get("spot_futures_state") or "")
    score += 4 if sf == "SPOT-LEADING" else (-3 if sf == "FUTURES-LEADING" else 0)
    mtf = str(intel.get("mtf_state") or "")
    score += 5 if mtf == "MTF-ALIGNED" else (2 if mtf == "MTF-MIXED-BULL" else (-10 if mtf == "MTF-REJECT" else 0))
    sm = stagnation_metrics(row)
    score -= f(sm.get("stagnation_penalty_119"))
    if calibration.get("status") == "ACTIVE" and f(row.get("early_quality_119"), early_quality(sym)) >= f(calibration.get("quality_threshold")):
        score += 3.0
    return round(score, 2)

def pump119(sym):
    ps = dict(_old_pump(sym))
    intel = intelligence(sym)
    row = q.latest.get(sym) or {}
    stale = stagnation_metrics(row, ps)
    score = f(ps.get("pump_signature_score"))
    score += clamp((f(intel.get("liquidity_vacuum_score"), 50) - 50) / 6.0, -6, 8)
    score += clamp((f(intel.get("flow_divergence_score"), 50) - 50) / 8.0, -5, 6)
    score += clamp((f(intel.get("sector_rs_score"), 50) - 50) / 10.0, -4, 5)
    sf = str(intel.get("spot_futures_state") or "")
    score += 4 if sf == "SPOT-LEADING" else (-3 if sf == "FUTURES-LEADING" else 0)
    mtf = str(intel.get("mtf_state") or "")
    score += 4 if mtf == "MTF-ALIGNED" else (1 if mtf == "MTF-MIXED-BULL" else (-12 if mtf == "MTF-REJECT" else 0))
    score -= min(30.0, f(stale.get("stagnation_penalty_119")) * .65)
    score = round(clamp(score, 0, 100), 1)
    ps.update(intel)
    ps.update(stale)
    ps["pump_signature_score"] = score
    ps["early_quality_119"] = early_quality(sym, ps)
    regime, watch_th, armed_th = adaptive_thresholds()
    ps["regime_119"] = regime
    ps["pump_watch_threshold_119"] = watch_th
    ps["pump_armed_threshold_119"] = armed_th

    ph = str(ps.get("breakout_lifecycle") or "UNKNOWN")
    lifecycle_ok = ph not in ("FAILED_BREAKOUT", "REJECT_FALLING", "NO_CHASE") and mtf != "MTF-REJECT"
    micro = bool(ps.get("micro_ready_118"))
    execp = bool(ps.get("exec_pass_118"))
    risk = bool(ps.get("btc_risk_overlay")) or regime == "RISK_OFF"
    if score >= armed_th and micro and execp and lifecycle_ok and not risk and stale["stagnation_state_119"] != "STALE-EVICT":
        ps["pump_state"] = "PUMP-ARMED"
    elif score >= watch_th and lifecycle_ok and stale["stagnation_state_119"] != "STALE-EVICT":
        ps["pump_state"] = "PUMP-WATCH"
    else:
        ps["pump_state"] = "NONE"
    return ps

def hot119(limit=None):
    n = max(int(limit or getattr(q, "HOT_COUNT", 80)), 80)
    raw = list(_old_hot(max(n * 2, 160)))
    scores = {}
    for score, sym in raw:
        if b17.directional(sym):
            scores[sym] = max(scores.get(sym, -999), f(score))
    for sym in candidate_symbols(50):
        try:
            ps = pump119(sym)
            bonus = clamp((f(ps.get("early_quality_119"), 50) - 50) / 4.0, -8, 14)
            if ps.get("pump_state") == "PUMP-ARMED":
                bonus += 12
            elif ps.get("pump_state") == "PUMP-WATCH":
                bonus += 6
            scores[sym] = scores.get(sym, f(q.sscore(sym))) + bonus
        except Exception:
            continue
    ranked = sorted(((v, s) for s, v in scores.items()), reverse=True)
    return ranked[:(limit or getattr(q, "HOT_COUNT", 80))]

def breakout_strict119(row):
    if not _old_breakout_strict(row):
        return False
    if stagnation_metrics(row).get("stagnation_state_119") == "STALE-EVICT":
        return False
    sym = str(row.get("symbol") or "")
    mtf = mtf_summary(sym)
    return mtf.get("mtf_state") != "MTF-REJECT"

# -----------------------------------------------------------------------------
# Extend adaptive learner with V10.19 features
# -----------------------------------------------------------------------------
def vac_bucket(x):
    v = f(x, 50)
    return "VAC75+" if v >= 75 else ("VAC60-74" if v >= 60 else ("VAC<40" if v < 40 else "VAC40-59"))

def flow_bucket(x):
    v = f(x, 50)
    return "FLOW75+" if v >= 75 else ("FLOW60-74" if v >= 60 else ("FLOW<40" if v < 40 else "FLOW40-59"))

def feature_snapshot119(sym):
    x = dict(_old_feature_snapshot(sym))
    ps = _old_pump(sym)
    intel = intelligence(sym)
    x["pump_score"] = f(ps.get("pump_signature_score"), f(x.get("pump_score")))
    x.update({
        "regime119": intel.get("market_regime_119"),
        "vacuum_score119": intel.get("liquidity_vacuum_score"),
        "flow_score119": intel.get("flow_divergence_score"),
        "spot_futures119": intel.get("spot_futures_state"),
        "sector119": intel.get("sector_119"),
        "sector_rs119": intel.get("sector_rs_score"),
        "mtf119": intel.get("mtf_state"),
        "early_quality119": early_quality(sym, ps),
    })
    return x

def pattern_keys119(x):
    keys = list(_old_pattern_keys(x))
    keys.extend([
        "REGIME|" + str(x.get("regime119") or "UNKNOWN"),
        "VAC|" + vac_bucket(x.get("vacuum_score119")),
        "FLOW119|" + flow_bucket(x.get("flow_score119")),
        "SPOTFUT|" + str(x.get("spot_futures119") or "WAIT"),
        "SECTOR|" + str(x.get("sector119") or "OTHER"),
        "MTF|" + str(x.get("mtf119") or "WAIT"),
        "V19COMBO|" + str(x.get("regime119") or "UNKNOWN") + "|" + vac_bucket(x.get("vacuum_score119")) + "|" + flow_bucket(x.get("flow_score119")) + "|" + str(x.get("spot_futures119") or "WAIT"),
    ])
    return keys

base.feature_snapshot = feature_snapshot119
base.pattern_keys = pattern_keys119

# -----------------------------------------------------------------------------
# Shadow execution evaluator
# -----------------------------------------------------------------------------
def signal_type(sym, ps):
    row = q.latest.get(sym) or {}
    formal = str(row.get("formal_state") or row.get("state") or "")
    if formal == "BUY NOW":
        return "BUY"
    if formal == "PRE-IGNITION":
        return "PRE"
    if ps.get("pump_state") == "PUMP-ARMED":
        return "PUMP-ARMED"
    if ps.get("pump_state") == "PUMP-WATCH":
        return "PUMP-WATCH"
    return None

def shadow_entry_cost_bps(sym):
    row = q.latest.get(sym) or {}
    spread = f(row.get("spread_bps"))
    if spread <= 0:
        try:
            st = app.micro_state.get(sym) or {}
            vals = st.get("spread_bps") or []
            if vals:
                spread = f(vals[-1][1])
        except Exception:
            pass
    return clamp(max(4.0, spread * .5 + 3.0), 4.0, 20.0)

def shadow_stop(sym, entry):
    mtf = mtf_summary(sym)
    support = f(mtf.get("mtf_support_5m"))
    if support > 0 and support < entry:
        dist = pct(entry, support)
        if 0.5 <= dist <= 4.0:
            return support * 0.998
    return entry * 0.98

def open_shadow(sym, ps):
    global shadow_seq
    now = time.time()
    stype = signal_type(sym, ps)
    if not stype or now - shadow_last[(sym, stype)] < SHADOW_COOLDOWN:
        return
    px = current_price(sym)
    if px <= 0:
        return
    cost = shadow_entry_cost_bps(sym)
    entry = px * (1 + cost / 10000.0)
    stop = shadow_stop(sym, entry)
    shadow_seq += 1
    features = feature_snapshot119(sym)
    shadow_pending.append({
        "id": shadow_seq, "symbol": sym, "signal": stype, "opened": now,
        "entry": entry, "raw_price": px, "cost_bps": cost, "stop": stop,
        "stop_initial": stop, "stop_moved_be": False,
        "tp1": entry * 1.02, "tp2": entry * 1.05, "tp3": entry * 1.10, "tp4": entry * 1.20,
        "hit_tp1": False, "hit_tp2": False, "hit_tp3": False, "hit_tp4": False,
        "max_return_pct": 0.0, "max_drawdown_pct": 0.0,
        "realized_return_pct": None, "status": "OPEN", "features": features,
        "returns": {},
    })
    shadow_last[(sym, stype)] = now
    stats["shadow_opened"] += 1
    if len(shadow_pending) > SHADOW_MAX_PENDING:
        del shadow_pending[:-SHADOW_MAX_PENDING]

def update_shadows():
    now = time.time()
    keep = []
    for t in shadow_pending:
        px = current_price(t["symbol"])
        if px <= 0:
            keep.append(t); continue
        entry = f(t.get("entry"))
        ret = pct(px, entry)
        t["max_return_pct"] = max(f(t.get("max_return_pct")), ret)
        t["max_drawdown_pct"] = min(f(t.get("max_drawdown_pct")), ret)
        age = now - f(t.get("opened"))
        if not t["hit_tp1"] and px >= f(t["tp1"]):
            t["hit_tp1"] = True
            t["stop_moved_be"] = True
            t["stop"] = entry * (1 + f(t.get("cost_bps")) / 10000.0)
        if not t["hit_tp2"] and px >= f(t["tp2"]):
            t["hit_tp2"] = True
        if not t["hit_tp3"] and px >= f(t["tp3"]):
            t["hit_tp3"] = True
        if not t["hit_tp4"] and px >= f(t["tp4"]):
            t["hit_tp4"] = True
        for label, secs in (("15m", 900), ("1h", 3600), ("2h", 7200), ("4h", 14400)):
            if age >= secs and label not in t["returns"]:
                t["returns"][label] = round(ret, 4)
        closed = False
        if px <= f(t.get("stop")):
            t["status"] = "STOP"
            t["closed"] = now
            t["realized_return_pct"] = round(pct(f(t.get("stop")), entry), 4)
            closed = True
        elif t["hit_tp4"]:
            t["status"] = "TP20"
            t["closed"] = now
            t["realized_return_pct"] = 20.0
            closed = True
        elif age >= SHADOW_HORIZON:
            t["status"] = "EXPIRE4H"
            t["closed"] = now
            t["realized_return_pct"] = round(ret, 4)
            closed = True
        if closed:
            shadow_resolved.append(dict(t))
            stats["shadow_closed"] += 1
        else:
            keep.append(t)
    shadow_pending[:] = keep

async def shadow_loop():
    while True:
        await asyncio.sleep(5)
        try:
            for sym in candidate_symbols(30):
                ps = pump119(sym)
                open_shadow(sym, ps)
            update_shadows()
            walk_forward_calibrate()
            save_v119_state()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.19 SHADOW_ERROR {type(e).__name__}: {e}", flush=True)

# -----------------------------------------------------------------------------
# Walk-forward calibration
# -----------------------------------------------------------------------------
def trade_quality(t):
    x = t.get("features") or {}
    return f(x.get("early_quality119"), .40 * f(x.get("pump_score")) + .20 * f(x.get("rs_score"), 50) + 20.0)

def trade_utility(t):
    m = f(t.get("max_return_pct"))
    r = f(t.get("realized_return_pct"))
    status = str(t.get("status") or "")
    u = 0.0
    if m >= 5: u += 1.0
    if m >= 10: u += 1.5
    if m >= 20: u += 3.0
    if m < 2: u -= 1.0
    if status == "STOP": u -= 1.0
    u += clamp(r / 10.0, -0.5, 1.0)
    return u

def avg_utility(rows):
    return sum(trade_utility(x) for x in rows) / len(rows) if rows else -999.0

def walk_forward_calibrate():
    now = time.time()
    rows = sorted(list(shadow_resolved), key=lambda x: f(x.get("opened")))
    n = len(rows)
    if n < 80:
        calibration.update({"status": "WARMING", "train_n": max(0, int(n * .75)), "valid_n": n - max(0, int(n * .75)), "folds": 0})
        return

    fold_specs = []
    for frac in (0.55, 0.65, 0.75, 0.85):
        train_end = int(n * frac)
        valid_end = min(n, train_end + max(10, int(n * .10)))
        if train_end >= 40 and valid_end - train_end >= 8:
            fold_specs.append((rows[:train_end], rows[train_end:valid_end]))
    if len(fold_specs) < 2:
        return

    best = None
    for th in range(50, 81, 3):
        fold_scores = []
        train_scores = []
        used_train = 0
        used_valid = 0
        valid_ok = True
        for train, valid in fold_specs:
            tr = [x for x in train if trade_quality(x) >= th]
            va = [x for x in valid if trade_quality(x) >= th]
            if len(tr) < 20 or len(va) < 5:
                valid_ok = False
                break
            train_scores.append(avg_utility(tr))
            fold_scores.append(avg_utility(va))
            used_train += len(tr)
            used_valid += len(va)
        if not valid_ok:
            continue
        mean_train = sum(train_scores) / len(train_scores)
        mean_valid = sum(fold_scores) / len(fold_scores)
        stability = statistics.pstdev(fold_scores) if len(fold_scores) > 1 else 0.0
        objective = mean_valid + .12 * mean_train - .35 * stability + min(.35, used_valid / 150.0)
        if best is None or objective > best[0]:
            best = (objective, th, mean_train, mean_valid, stability, used_train, used_valid)

    if best is None:
        calibration.update({"status": "INSUFFICIENT-FOLD-COVERAGE", "folds": len(fold_specs)})
        return

    _, th, tu, vu, stability, tn, vn = best
    baseline_folds = []
    for _, valid in fold_specs:
        va = [x for x in valid if trade_quality(x) >= 60]
        if va:
            baseline_folds.append(avg_utility(va))
    baseline_u = sum(baseline_folds) / len(baseline_folds) if baseline_folds else -1.0

    status = "ACTIVE" if vu > baseline_u + 0.05 and stability <= 1.5 else "VALIDATION-FAILED"
    calibration.update({
        "status": status, "quality_threshold": float(th), "train_n": tn, "valid_n": vn,
        "train_utility": round(tu, 4), "valid_utility": round(vu, 4),
        "baseline_valid_utility": round(baseline_u, 4), "fold_stability": round(stability, 4),
        "folds": len(fold_specs), "updated": now,
    })

# -----------------------------------------------------------------------------
# Persistent V10.19 state
# -----------------------------------------------------------------------------
last_state_save = 0.0

def save_v119_state():
    global last_state_save
    now = time.time()
    if now - last_state_save < STATE_SAVE_SECONDS:
        return
    last_state_save = now
    obj = {
        "version": VERSION, "saved_at": now, "shadow_seq": shadow_seq,
        "shadow_pending": shadow_pending[-300:],
        "shadow_resolved": list(shadow_resolved)[-1000:],
        "calibration": calibration, "stats": stats,
    }
    tmp = V119_STATE_PATH + ".tmp"
    try:
        os.makedirs(os.path.dirname(V119_STATE_PATH), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, separators=(",", ":"))
        os.replace(tmp, V119_STATE_PATH)
    except Exception:
        stats["save_errors"] += 1

def load_v119_state():
    global shadow_seq
    try:
        with open(V119_STATE_PATH, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        shadow_seq = int(obj.get("shadow_seq") or 0)
        for t in obj.get("shadow_pending") or []:
            if isinstance(t, dict):
                shadow_pending.append(t)
        for t in obj.get("shadow_resolved") or []:
            if isinstance(t, dict):
                shadow_resolved.append(t)
        if isinstance(obj.get("calibration"), dict):
            calibration.update(obj["calibration"])
        stats["state_loaded"] = True
    except FileNotFoundError:
        pass
    except Exception:
        stats["save_errors"] += 1

load_v119_state()

# -----------------------------------------------------------------------------
# Wiring
# -----------------------------------------------------------------------------
b17.diag_pool = diag119
b17.opp_score = opp119
b18.pump_signature = pump119
b17.hot = hot119
q.hot = hot119
b16._breakout_strict = breakout_strict119

# -----------------------------------------------------------------------------
# Telemetry
# -----------------------------------------------------------------------------
def shadow_summary():
    rows = list(shadow_resolved)
    if not rows:
        return {"n": 0, "hit5": 0.0, "hit10": 0.0, "hit20": 0.0, "stops": 0.0, "avg_realized": 0.0}
    n = len(rows)
    return {
        "n": n,
        "hit5": sum(f(x.get("max_return_pct")) >= 5 for x in rows) / n,
        "hit10": sum(f(x.get("max_return_pct")) >= 10 for x in rows) / n,
        "hit20": sum(f(x.get("max_return_pct")) >= 20 for x in rows) / n,
        "stops": sum(str(x.get("status")) == "STOP" for x in rows) / n,
        "avg_realized": sum(f(x.get("realized_return_pct")) for x in rows) / n,
    }

async def print_loop119():
    while True:
        await asyncio.sleep(PRINT_SECONDS)
        try:
            regime, wt, at = adaptive_thresholds()
            ss = shadow_summary()
            print(
                f"Ψ-V10.19 INTEL regime={regime} watchTh={wt:.1f} armedTh={at:.1f} "
                f"mtfSamples={stats['mtf_samples']} depthSamples={stats['depth_samples']} "
                f"flowSamples={stats['flow_samples']} futuresSamples={stats['futures_samples']} "
                f"persist={'YES' if PERSIST_DIR == '/data' else 'LOCAL'} stateLoaded={'YES' if stats['state_loaded'] else 'NO'}",
                flush=True
            )
            rows = []
            for sym in candidate_symbols(24):
                try:
                    ps = pump119(sym)
                    rows.append((f(ps.get("early_quality_119")), sym, ps))
                except Exception:
                    continue
            rows.sort(reverse=True)
            for i, (quality, sym, ps) in enumerate(rows[:10], 1):
                print(
                    f"I{i:02d}. {sym:14s} quality={quality:5.1f} pump={f(ps.get('pump_signature_score')):5.1f} "
                    f"state={str(ps.get('pump_state') or 'NONE'):10s} vac={f(ps.get('liquidity_vacuum_score'),50):5.1f} "
                    f"flow={f(ps.get('flow_divergence_score'),50):5.1f} sf={str(ps.get('spot_futures_state') or 'WAIT'):15s} "
                    f"sector={str(ps.get('sector_rs_state') or '-'):14s}/{f(ps.get('sector_rs_score'),50):4.0f} "
                    f"mtf={str(ps.get('mtf_state') or '-'):14s} stale={str(ps.get('stagnation_state_119') or '-')}",
                    flush=True
                )
            print(
                f"Ψ-V10.19 SHADOW open={len(shadow_pending)} resolved={ss['n']} "
                f"hit5={ss['hit5']:.3f} hit10={ss['hit10']:.3f} hit20={ss['hit20']:.3f} "
                f"stops={ss['stops']:.3f} avgRealized={ss['avg_realized']:+.3f}%",
                flush=True
            )
            print(
                f"Ψ-V10.19 WALK_FORWARD status={calibration['status']} qualityTh={f(calibration.get('quality_threshold')):.1f} "
                f"train={int(calibration.get('train_n') or 0)} valid={int(calibration.get('valid_n') or 0)} "
                f"trainU={f(calibration.get('train_utility')):+.3f} validU={f(calibration.get('valid_utility')):+.3f} "
                f"baselineValidU={f(calibration.get('baseline_valid_utility')):+.3f}",
                flush=True
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.19 PRINT_ERROR {type(e).__name__}: {e}", flush=True)

async def main119():
    await asyncio.gather(
        _old_main(),
        mtf_loop(),
        depth_loop(),
        flow_loop(),
        futures_loop(),
        market_loop(),
        shadow_loop(),
        print_loop119(),
    )

scanner.v7.main = main119
scanner.VERSION = VERSION

print(
    "Ψ-V10.19 UPGRADE ACTIVE — persistent learning path, walk-forward calibration, shadow execution, "
    "hard stagnation demotion, 1m/3m/5m/15m lifecycle, liquidity-vacuum/ask depletion, CVD+OFI divergence, "
    "spot-vs-futures lead/lag, market+sector relative strength, regime-adaptive early thresholds; "
    "formal PRE/BUY safety gates unchanged",
    flush=True
)

if __name__ == "__main__":
    try:
        print("Ψ-V10.19 ACTIVE — validation-first breakout intelligence", flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        save_v119_state()
        print("Ψ-V10.19 stopped", flush=True)
