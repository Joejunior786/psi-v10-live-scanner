"""Independent EMA50/EMA200 seller-exhaustion reporting and execution authority."""
import time
import math

CORE = None
PREVIOUS_ATTACH = None
PREVIOUS_PRINT = None
FRAMES = (("1h", "1H", 180), ("4h", "4H", 420), ("1d", "DAILY", 1200))

def num(x, default=0):
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (ValueError, TypeError):
        return default

def evaluate(snap, tf, updated, now, integrity, micro):
    if not isinstance(snap, dict) or updated <= 0 or now - updated > dict((x, z) for x, _, z in FRAMES)[tf] or updated > now+5:
        return []
    price, low, high, close = (num(snap.get(k)) for k in ("current","low","high","close"))
    atr = num(snap.get("atr"))
    if min(price,low,high,close,atr) <= 0 or low > high:
        return []
    sell_fade = snap.get("falling_volume") is True and num(snap.get("buy_ratio_3")) >= .5
    absorption = num(snap.get("buy_ratio")) >= .55 and num(snap.get("buy_ratio_3")) >= .53
    rejection = num(snap.get("lower_wick")) >= .24 and num(snap.get("close_strength")) >= .58
    live_ok = bool(integrity.get("verified")) and not integrity.get("blockers")
    flow_ok = all(micro.get(k) is True for k in ("micro_ready", "sequence_verified", "book_sequence_verified"))
    out = []
    for period in (50,200):
        ema = num(snap.get(f"ema{period}"))
        if ema <= 0 or snap.get(f"has_ema{period}") is not True: continue
        distance = 100*(price-ema)/ema
        touch = low <= ema <= high or abs(distance) <= .25
        if not touch and abs(distance) > 1: continue
        reclaim = close >= ema and price >= .9975*ema
        exhausted = sell_fade and absorption and rejection
        risk = max(.65*atr,.004*ema)
        stop = min(ema,low)-risk
        per_unit = price-stop
        risk_pct = 100*per_unit/price
        blockers = []
        if not touch: blockers.append("NO_EMA_TOUCH")
        if not exhausted: blockers.append("SELLER_EXHAUSTION_UNCONFIRMED")
        if not reclaim: blockers.append("EMA_RECLAIM_MISSING")
        if not flow_ok: blockers.append("INVALID_MICRO_SEQUENCE")
        if not live_ok: blockers.append("LIVE_INTEGRITY_INVALID")
        if not 0 < risk_pct <= 5: blockers.append("INVALID_EMA_RISK")
        state = ("BUY NOW — EMA" if not blockers else
                 "PRE-IGNITION" if touch and (sell_fade or absorption) else
                 "ARMED" if touch else "WATCH")
        out.append({"timeframe":tf, "ema_period":period,"ema_value":ema,"price":price,
                    "distance_pct":round(distance,4), "touch":touch,
                    "seller_exhaustion":exhausted,"buyer_reclaim":reclaim,
                    "status":state,"blockers":blockers,"entry":price,
                    "stop":stop if 0<risk_pct<=5 else None,
                    "tp1":price+per_unit*1.5 if 0<risk_pct<=5 else None,
                    "tp2":price+per_unit*2 if 0<risk_pct<=5 else None,
                    "tp3":price+per_unit*3 if 0<risk_pct<=5 else None,
                    "risk_pct":round(risk_pct,3) if 0<risk_pct<=5 else None})
    return out

def attach(symbol, structural_row):
    row = dict(PREVIOUS_ATTACH(symbol, structural_row))
    try:
        original = CORE.q.latest.get(symbol) or {}
        integrity = CORE.legacy._integrity_status(symbol, original, require_risk=True, require_event_tape=True)
        micro = CORE.app.micro_metrics(symbol)
    except Exception:
        integrity, micro = {}, {}
    data = CORE._cache.get(symbol) or {}
    now = time.time()
    candidates = []
    for tf, _, _ in FRAMES:
        item = data.get(tf) or {}
        candidates.extend(evaluate(item.get("snap"),tf,num(item.get("updated")),now,integrity,micro))
    row["ema_signal_lane"] = candidates
    eligible = [r for r in candidates if r["status"] == "BUY NOW — EMA"]
    if eligible:
        best = sorted(eligible,key=lambda r:({"1h":1,"4h":2,"1d":3}[r["timeframe"]],-r["risk_pct"]),reverse=True)[0]
        row.update({"buy_now":True,"execution_state":"BUY NOW",
                    "execution_route":"INDEPENDENT_EMA_EXHAUSTION",
                    "execution_entry":best["entry"],"execution_stop":best["stop"],
                    "execution_tp1":best["tp1"],"execution_tp2":best["tp2"],
                    "execution_tp3":best["tp3"],"execution_risk_pct":best["risk_pct"],
                    "execution_blockers":[],"ema_buy_signal":best})
    return row

def print_board(*args, **kwargs):
    original = PREVIOUS_PRINT(*args, **kwargs)
    if CORE is None:
        return original
    records = []
    for symbol, row in list(CORE._results.items()):
        for item in row.get("ema_signal_lane") or []:
            records.append((symbol, item))
    touches = [r for r in records if r[1]["touch"]]
    buys = [r for r in touches if r[1]["status"] == "BUY NOW — EMA"]
    print(
        f"Ψ-V15.17 EMA_SIGNAL_LANE evaluated={len(records)} "
        f"touches={len(touches)} buys={len(buys)} "
        f"approaching={len(records)-len(touches)}",
        flush=True,
    )
    for symbol, item in sorted(records, key=lambda x: (
            x[1]["status"] == "BUY NOW — EMA",
            x[1]["touch"], -abs(x[1]["distance_pct"])), reverse=True)[:30]:
        print(
            f"EMA {symbol} tf={item['timeframe']} ema={item['ema_period']} "
            f"dist={item['distance_pct']:+.3f}% exhausted={item['seller_exhaustion']} "
            f"reclaim={item['buyer_reclaim']} status={item['status']} "
            f"blockers={','.join(item['blockers']) or '-'}",
            flush=True,
        )
    return original


def install(core):
    global CORE, PREVIOUS_ATTACH, PREVIOUS_PRINT
    if CORE is not None: return
    CORE = core
    PREVIOUS_ATTACH = core._attach_execution_gate
    core._attach_execution_gate = attach
    PREVIOUS_PRINT = core.print_board
    core.print_board = print_board
    print("PSI-V15.17 EMA_EXHAUSTION 1H/4H/DAILY 50/200 independent route installed",flush=True)
