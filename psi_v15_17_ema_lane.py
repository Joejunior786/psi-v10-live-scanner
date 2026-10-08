"""Independent EMA50/EMA200 seller-exhaustion reporting and execution authority."""
import time
import math
import asyncio

CORE = None
PREVIOUS_ATTACH = None
PREVIOUS_PRINT = None
EMA_REPORT_HISTORY = {}
EMA_REPORT_TICK = 0
EMA_DISPLAY_LIMIT = 30
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
    absorption = num(snap.get("buy_ratio")) >= .52 and num(snap.get("buy_ratio_3")) >= .51
    rejection = num(snap.get("lower_wick")) >= .20 and num(snap.get("close_strength")) >= .55
    live_ok = bool(integrity.get("verified")) and not integrity.get("blockers")
    flow_ok = all(micro.get(k) is True for k in ("micro_ready", "sequence_verified", "book_sequence_verified"))
    positive_flow = num(micro.get("cvd_acceleration")) > 0 and num(micro.get("ofi_acceleration")) >= 0
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
        if not positive_flow: blockers.append("CVD_OFI_NOT_POSITIVE")
        if not live_ok: blockers.append("LIVE_INTEGRITY_INVALID")
        if not 0 < risk_pct <= 5.5: blockers.append("INVALID_EMA_RISK")
        state = ("BUY NOW — EMA" if not blockers else
                 "PRE-IGNITION" if touch and (sell_fade or absorption) else
                 "ARMED" if touch else "WATCH")
        out.append({"timeframe":tf, "ema_period":period,"ema_value":ema,"price":price,
                    "distance_pct":round(distance,4), "touch":touch,
                    "seller_exhaustion":exhausted,"buyer_reclaim":reclaim,
                    "status":state,"blockers":blockers,"entry":price,
                    "stop":stop if 0<risk_pct<=5.5 else None,
                    "tp1":price+per_unit*1.5 if 0<risk_pct<=5.5 else None,
                    "tp2":price+per_unit*2 if 0<risk_pct<=5.5 else None,
                    "tp3":price+per_unit*3 if 0<risk_pct<=5.5 else None,
                    "risk_pct":round(risk_pct,3) if 0<risk_pct<=5.5 else None})
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

def _ema_rank(item):
    states = {"BUY NOW — EMA": 5, "PRE-IGNITION": 4, "ARMED": 3, "WATCH": 1}
    return (states.get(item.get("status"), 0),
            int(bool(item.get("seller_exhaustion"))),
            int(bool(item.get("buyer_reclaim"))),
            int(bool(item.get("touch"))),
            -abs(num(item.get("distance_pct"), 999)))


def select_ema_report(records, history, tick, limit=EMA_DISPLAY_LIMIT):
    """Pure unique-symbol selection with stable priority and fair candidate rotation."""
    grouped = {}
    for symbol, item in records:
        grouped.setdefault(symbol, []).append(item)
    primary = {}
    for symbol, items in grouped.items():
        primary[symbol] = max(items, key=_ema_rank)
    current = {}
    for symbol, item in primary.items():
        signature = (item.get("timeframe"), item.get("ema_period"),
                     item.get("status"), bool(item.get("touch")),
                     bool(item.get("seller_exhaustion")),
                     bool(item.get("buyer_reclaim")),
                     tuple(sorted(item.get("blockers") or ())))
        prior = history.get(symbol)
        if prior is None:
            change = "NEW"
        elif prior["signature"] == signature:
            change = "UNCHANGED"
        elif _ema_rank(item)[:4] > prior["rank"][:4]:
            change = "IMPROVING"
        elif _ema_rank(item)[:4] < prior["rank"][:4]:
            change = "WEAKENING"
        else:
            change = "CHANGED"
        current[symbol] = {"signature": signature, "rank": _ema_rank(item),
                           "change": change, "last_shown": prior.get("last_shown", -1) if prior else -1}
    ranked = sorted(primary, key=lambda sym: (_ema_rank(primary[sym]), sym), reverse=True)
    # All BUY NOW signals are shown before any rotation; never hide a qualifying BUY.
    approved = [sym for sym in ranked if primary[sym].get("status") == "BUY NOW — EMA"]
    slots = max(0, limit - len(approved))
    # Retain highest-quality signals; other slots favor new/improving and long-unseen coins.
    fixed = [sym for sym in ranked if sym not in approved][:min(slots, max(1, limit // 3))]
    slots -= len(fixed)
    remainder = [sym for sym in ranked if sym not in approved and sym not in fixed]
    rotated = sorted(remainder, key=lambda sym: (
        int(current[sym]["change"] in ("NEW", "IMPROVING")),
        -current[sym]["last_shown"], _ema_rank(primary[sym]), sym), reverse=True)[:slots]
    chosen = approved + fixed + rotated
    next_history = {sym: {**data, "last_shown": tick if sym in chosen else data["last_shown"]}
                    for sym, data in current.items()}
    rows = [(sym, primary[sym], current[sym]["change"], len(grouped[sym]))
            for sym in chosen]
    return rows, next_history, len(grouped)


def emit_report():
    global EMA_REPORT_HISTORY, EMA_REPORT_TICK
    if CORE is None:
        return None
    records = []
    covered = 0
    now = time.time()
    # Discovery is independent: use every fresh cached Binance candle snapshot,
    # including symbols excluded from the normal structural top list.
    for symbol, frames in list(CORE._cache.items()):
        if not isinstance(frames, dict):
            continue
        for tf, _, _ in FRAMES:
            frame = frames.get(tf) or {}
            snap = frame.get("snap")
            if isinstance(snap, dict) and num(frame.get("updated")) > 0:
                covered += 1
                # Discovery-only telemetry does not grant execution authority.
                for item in evaluate(snap, tf, num(frame.get("updated")), now, {}, {}):
                    records.append((symbol, item))
    # Overlay exact execution-grade decisions when their safety evidence exists.
    evidence = {(symbol, item["timeframe"], item["ema_period"]): item
                for symbol, row in list(CORE._results.items())
                for item in row.get("ema_signal_lane") or []}
    records = [(symbol, evidence.get((symbol, item["timeframe"], item["ema_period"]), item))
               for symbol, item in records]
    touches = [r for r in records if r[1]["touch"]]
    buys = [r for r in touches if r[1]["status"] == "BUY NOW — EMA"]
    print(
        f"Ψ-V15.17 EMA_SIGNAL_LANE candleFrames={covered} evaluated={len(records)} "
        f"touches={len(touches)} buys={len(buys)} "
        f"approaching={len(records)-len(touches)}",
        flush=True,
    )
    EMA_REPORT_TICK += 1
    shown, EMA_REPORT_HISTORY, unique_symbols = select_ema_report(
        records, EMA_REPORT_HISTORY, EMA_REPORT_TICK)
    print(f"Ψ-V15.17 EMA_ROTATION unique={unique_symbols} displayed={len(shown)} "
          f"new={sum(row[2] == 'NEW' for row in shown)} "
          f"improving={sum(row[2] == 'IMPROVING' for row in shown)} "
          f"buy={sum(row[1]['status'] == 'BUY NOW — EMA' for row in shown)}", flush=True)
    for symbol, item, change, signals in shown:
        print(
            f"EMA {symbol} tf={item['timeframe']} ema={item['ema_period']} "
            f"dist={item['distance_pct']:+.3f}% exhausted={item['seller_exhaustion']} "
            f"reclaim={item['buyer_reclaim']} status={item['status']} "
            f"change={change} signals={signals} "
            f"blockers={','.join(item['blockers']) or '-'}",
            flush=True,
        )
    return None


def print_board(*args, **kwargs):
    result = PREVIOUS_PRINT(*args, **kwargs)
    emit_report()
    return result


async def reporting_supervisor():
    """Independent EMA reporter; never wait for long structural hydration loops."""
    while True:
        try:
            emit_report()
        except Exception as exc:
            print(f"PSI-V15.17 EMA_REPORT_ERROR {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(45)


def install(core):
    global CORE, PREVIOUS_ATTACH, PREVIOUS_PRINT
    if CORE is not None: return
    CORE = core
    PREVIOUS_ATTACH = core._attach_execution_gate
    core._attach_execution_gate = attach
    PREVIOUS_PRINT = core.print_board
    core.print_board = print_board
    print("PSI-V15.17 EMA_EXHAUSTION 1H/4H/DAILY 50/200 independent route installed",flush=True)
