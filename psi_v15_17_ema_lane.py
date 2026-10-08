"""Independent EMA50/EMA200 seller-exhaustion reporting and execution authority."""
import time
import math
import asyncio
import threading

CORE = None
PREVIOUS_ATTACH = None
PREVIOUS_PRINT = None
EMA_REPORT_HISTORY = {}
EMA_REPORT_TICK = 0
EMA_DISPLAY_LIMIT = 30
EMA_REPORT_LOCK = threading.Lock()
EMA_REVISION = "15.20-independent-evidence"
EMA_PRIORITY_LIMIT = 12
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
        if not micro: blockers.append("MICRO_NOT_EVALUATED")
        elif not flow_ok: blockers.append("INVALID_MICRO_SEQUENCE")
        if not micro: pass
        elif not positive_flow: blockers.append("CVD_OFI_NOT_POSITIVE")
        if not integrity: blockers.append("LIVE_INTEGRITY_NOT_EVALUATED")
        elif not live_ok: blockers.append("LIVE_INTEGRITY_INVALID")
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

def standalone_live_evidence(core, symbol, snap, now=None):
    """EMA-specific execution integrity, without an unrelated Pinpoint risk plan.

    Never infer readiness from discovery. Require real trade/book timestamps,
    verified sequencing, spread, slippage and price agreement. The EMA risk
    plan is checked independently by evaluate().
    """
    now = time.time() if now is None else float(now)
    blockers = []
    try:
        micro = core.app.micro_metrics(symbol) or {}
    except Exception:
        micro = {}
        blockers.append("MICRO_READ_ERROR")
    if not isinstance(micro, dict):
        micro = {}
        blockers.append("MICRO_INVALID")

    trade_ms = num(micro.get("last_trade_ms"))
    book_ms = num(micro.get("last_book_ms"))
    trade_age = now * 1000 - trade_ms if trade_ms > 0 else float("inf")
    book_age = now * 1000 - book_ms if book_ms > 0 else float("inf")
    if not -2000 <= trade_age <= 15000:
        blockers.append("STALE_OR_MISSING_TRADES")
    if not -2000 <= book_age <= 5000:
        blockers.append("STALE_OR_MISSING_BOOK")
    if micro.get("micro_ready") is not True:
        blockers.append("MICRO_NOT_READY")
    if micro.get("sequence_verified") is not True:
        blockers.append("TRADE_SEQUENCE_INVALID")
    if micro.get("book_sequence_verified") is not True:
        blockers.append("BOOK_SEQUENCE_INVALID")

    spread, slippage = micro.get("spread_bps"), micro.get("slippage_bps")
    if spread is None or not 0 <= num(spread, -1) <= 40:
        blockers.append("SPREAD_UNVERIFIED_OR_EXCESSIVE")
    if slippage is None or not 0 <= num(slippage, -1) <= 80:
        blockers.append("SLIPPAGE_UNVERIFIED_OR_EXCESSIVE")

    live_price = num(micro.get("last_price"))
    candle_price = num(snap.get("current"))
    if (live_price <= 0 or candle_price <= 0
            or abs(live_price / candle_price - 1) > 0.015):
        blockers.append("LIVE_PRICE_NOT_CONFIRMED")

    return {"verified": not blockers, "blockers": blockers,
            "trade_age_ms": trade_age, "book_age_ms": book_age}, micro


def _technical_complete(item):
    """Signal evidence only; NEVER an executable trade by itself."""
    return (bool(item.get("touch")) and bool(item.get("seller_exhaustion"))
            and bool(item.get("buyer_reclaim")) and item.get("stop") is not None)


def scan_cached_ema(core, now=None):
    """Scan each fresh frame independently of V12's structural result map."""
    now = time.time() if now is None else float(now)
    records = []
    frames_covered = 0
    full_evidence_symbols = set()
    for symbol, frames in list(core._cache.items()):
        if not isinstance(frames, dict):
            continue
        snapshots = []
        for tf, _, _ in FRAMES:
            frame = frames.get(tf) or {}
            snap = frame.get("snap")
            updated = num(frame.get("updated"))
            if isinstance(snap, dict) and updated > 0:
                frames_covered += 1
                previews = evaluate(snap, tf, updated, now, {}, {})
                if previews:
                    snapshots.append((tf, snap, updated, previews))
        if not snapshots:
            continue
        qualifying = [(tf, snap, updated) for tf, snap, updated, items in snapshots
                      if any(_technical_complete(item) for item in items)]
        live_by_symbol = None
        if qualifying:
            # Technical screening FIRST. Only then spend live-data budget.
            live_by_symbol = standalone_live_evidence(core, symbol, qualifying[0][1], now)
            full_evidence_symbols.add(symbol)
        for tf, snap, updated, previews in snapshots:
            if live_by_symbol:
                integrity, micro = live_by_symbol
                # Each timeframe's candle must agree independently with trade price.
                live_price = num(micro.get("last_price"))
                cand_price = num(snap.get("current"))
                if not cand_price or not live_price or abs(live_price / cand_price - 1) > 0.015:
                    integrity = dict(integrity, verified=False, blockers=list(integrity["blockers"])+["LIVE_PRICE_NOT_CONFIRMED"])
                items = evaluate(snap, tf, updated, now, integrity, micro)
            else:
                items = previews
            for item in items:
                item["evidence_status"] = ("LIVE_VERIFIED" if live_by_symbol and
                                           live_by_symbol[0]["verified"] and
                                           "LIVE_PRICE_NOT_CONFIRMED" not in item["blockers"]
                                           else "LIVE_BLOCKED" if live_by_symbol else "DISCOVERY_ONLY")
                records.append((symbol, item))
    return records, frames_covered, len(full_evidence_symbols)


def attach(symbol, structural_row):
    row = dict(PREVIOUS_ATTACH(symbol, structural_row))
    data = CORE._cache.get(symbol) or {}
    now = time.time()
    snapshots = [(tf, (data.get(tf) or {}).get("snap"), num((data.get(tf) or {}).get("updated")))
                 for tf, _, _ in FRAMES]
    previews = [(tf, snap, updated, evaluate(snap, tf, updated, now, {}, {}))
                for tf, snap, updated in snapshots]
    qualified = next((snap for tf, snap, _, items in previews
                      if any(_technical_complete(i) for i in items)), None)
    integrity, micro = (standalone_live_evidence(CORE, symbol, qualified, now)
                        if qualified else ({}, {}))
    candidates = []
    for tf, snap, updated, _ in previews:
        candidates.extend(evaluate(snap, tf, updated, now, integrity, micro))
    row["ema_signal_lane"] = candidates
    eligible = [r for r in candidates if r["status"] == "BUY NOW — EMA"]
    if eligible:
        best = sorted(eligible, key=lambda r: ({"1h":1,"4h":2,"1d":3}[r["timeframe"]],
                                                  -r["risk_pct"]), reverse=True)[0]
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
    if CORE is None or not EMA_REPORT_LOCK.acquire(blocking=False):
        return None
    try:
        records, covered, live_checked = scan_cached_ema(CORE)
        touches = [r for r in records if r[1]["touch"]]
        buys = [r for r in records if r[1]["status"] == "BUY NOW — EMA"]
        technical = [(sym, item) for sym, item in records if _technical_complete(item)]
        technical_symbols = list(dict.fromkeys(sym for sym, _ in technical))
        ranked = sorted(records, key=lambda r: _ema_rank(r[1]), reverse=True)
        research = list(dict.fromkeys(sym for sym, item in ranked
                                     if item["touch"] and abs(item["distance_pct"]) <= 1.0
                                     and sym not in {"XUSDUSDT", "BFUSDUSDT", "USDCUSDT", "USD1USDT", "FDUSDUSDT", "TUSDUSDT"}))[:10]
        priority = list(dict.fromkeys(technical_symbols + research))[:EMA_PRIORITY_LIMIT]
        snapshot = {
            "revision": EMA_REVISION, "generated_ms": int(time.time() * 1000),
            "candle_frames": covered, "interactions": len(records),
            "unique_symbols": len(set(sym for sym, _ in records)),
            "live_evidence_symbols": live_checked,
            "technical_qualified_symbols": technical_symbols,
            "execution_ready_symbols": list(dict.fromkeys(sym for sym, _ in buys)),
            "execution_ready_signals": [dict(symbol=sym, **item) for sym, item in buys],
            "top10_research": research,
            "order_placement": False,
        }
        # Atomic snapshot for API/report consumers; no mutation of legacy authority.
        CORE._independent_ema_board = snapshot
        CORE._ema_priority_symbols = priority
        print(f"Ψ-V15.20 EMA_INDEPENDENT candleFrames={covered} "
              f"interactions={len(records)} unique={snapshot['unique_symbols']} "
              f"touches={len(touches)} technical={len(technical_symbols)} "
              f"liveChecked={live_checked} verifiedBuys={len(buys)} "
              f"research={len(research)}", flush=True)
        print("Ψ-V15.20 EMA_RESEARCH_TOP10 " + ",".join(research or ["NONE"]), flush=True)
        for sym, item in buys:
            print(f"Ψ-V15.20 EMA_VERIFIED_BUY {sym} tf={item['timeframe']} "
                  f"ema={item['ema_period']} entry={item['entry']:.10g} "
                  f"stop={item['stop']:.10g} tp1={item['tp1']:.10g} "
                  f"risk={item['risk_pct']:.2f}% evidence=LIVE_VERIFIED", flush=True)
        EMA_REPORT_TICK += 1
        shown, EMA_REPORT_HISTORY, unique_symbols = select_ema_report(
            records, EMA_REPORT_HISTORY, EMA_REPORT_TICK)
        print(f"Ψ-V15.20 EMA_ROTATION unique={unique_symbols} displayed={len(shown)} "
              f"new={sum(row[2] == 'NEW' for row in shown)} "
              f"improving={sum(row[2] == 'IMPROVING' for row in shown)} "
              f"buy={sum(row[1]['status'] == 'BUY NOW — EMA' for row in shown)}", flush=True)
        for symbol, item, change, signals in shown:
            print(f"EMA {symbol} tf={item['timeframe']} ema={item['ema_period']} "
                  f"dist={item['distance_pct']:+.3f}% exhausted={item['seller_exhaustion']} "
                  f"reclaim={item['buyer_reclaim']} status={item['status']} "
                  f"change={change} signals={signals} evidence={item.get('evidence_status','LEGACY')} "
                  f"blockers={','.join(item['blockers']) or '-'}", flush=True)
        return snapshot
    finally:
        EMA_REPORT_LOCK.release()


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
