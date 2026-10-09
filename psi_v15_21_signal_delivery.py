"""V15.21: low-latency, read-only, expiring, verified EMA signal delivery.

This module NEVER submits an exchange order. HTTP/SSE output is informational.
The independent EMA evidence engine remains the sole source of EMA BUY truth.
"""
import asyncio
import json
import os
import time
import re
from collections import deque
from threading import Lock
from aiohttp import web

CORE = None
EMA = None
ML = None
INTERVAL_SECONDS = 1.0
LOG_INTERVAL_SECONDS = max(2.0, float(os.environ.get("PSI_SIGNAL_LOG_INTERVAL_SECONDS", "4")))
MAX_AGE_MS = 3500
SIGNAL_LIFETIME_MS = 3000
MAX_HISTORY = 100
RESEARCH_ROTATE_MS = 20000
RESEARCH_ROTATE_COUNT = 10
SOURCE_DECISION_MAX_AGE_MS = 20000
MICRO_QUOTE_MAX_AGE_MS = 1200
ML_PRIORITY_COUNT = 12
MICRO_PRIORITY_SLOTS = 36
PRIORITY_HOLD_MS = 180000
MOVER_WINDOW_MS = 24 * 60 * 60 * 1000
MOVER_THRESHOLD_PCT = 10.0
_PRIORITY_LEASES = {}
_OBSERVED_QUOTES = {}
_MISSED_MOVES = deque(maxlen=80)
_RESEARCH_PREVIOUS = {}
_LOCK = Lock()
_PUBLISH_LOCK = Lock()
_SNAPSHOT = {}
_EVENTS = deque(maxlen=MAX_HISTORY)
_ACTIVE = set()
_NEXT_ID = 0
_STATUS = {"cycles": 0, "errors": 0, "last_error": ""}


def _ms():
    return int(time.time() * 1000)


def _latest_verified(row, now_ms, strict_micro_age_ms=None):
    """Recheck all hard live-data conditions at *read* time, not just emission."""
    symbol = row.get("symbol")
    tf = row.get("timeframe")
    if not symbol or tf not in {"1h", "4h", "1d"} or CORE is None:
        return None
    frame = ((getattr(CORE, "_cache", {}) or {}).get(symbol) or {}).get(tf) or {}
    snap = frame.get("snap")
    updated = EMA.num(frame.get("updated"))
    if not isinstance(snap, dict):
        return None
    integrity, micro = EMA.standalone_live_evidence(CORE, symbol, snap, now_ms / 1000)
    if not integrity.get("verified"):
        return None
    if strict_micro_age_ms is not None:
        if not (0 <= integrity["trade_age_ms"] <= strict_micro_age_ms
                and 0 <= integrity["book_age_ms"] <= strict_micro_age_ms):
            return None
    trade_price = EMA.num(micro.get("last_price"))
    old_price = EMA.num(snap.get("current"))
    if old_price <= 0 or trade_price <= 0 or abs(trade_price / old_price - 1) > 0.015:
        return None
    for candidate in EMA.evaluate(
        dict(snap, current=trade_price), tf, updated, now_ms / 1000, integrity, micro
    ):
        if (candidate["ema_period"] == row.get("ema_period")
                and candidate["status"] == "BUY NOW — EMA"):
            result = dict(symbol=symbol, **candidate)
            if strict_micro_age_ms is not None:
                result["quote_checked_ms"] = now_ms
                result["trade_age_ms"] = integrity["trade_age_ms"]
                result["book_age_ms"] = integrity["book_age_ms"]
                result["quote_expires_ms"] = min(
                    int(now_ms + SIGNAL_LIFETIME_MS),
                    int(EMA.num(micro.get("last_trade_ms")) + strict_micro_age_ms),
                    int(EMA.num(micro.get("last_book_ms")) + strict_micro_age_ms),
                )
                if result["quote_expires_ms"] <= now_ms:
                    return None
            return result
    return None


def _micro_authority_check(symbol, now_ms, strict_age_ms=MICRO_QUOTE_MAX_AGE_MS):
    """Fail closed against actual Binance trade/book events, not a log timestamp.

    This is common execution integrity only. Each setup engine remains the
    exclusive authority on whether its own structural/ML rules passed.
    """
    if CORE is None or not re.fullmatch(r"[A-Z0-9]{2,24}USDT", symbol):
        return None, ["INVALID_SYMBOL"]
    try:
        micro = CORE.app.micro_metrics(symbol) or {}
    except Exception:
        return None, ["MICRO_READ_ERROR"]
    if not isinstance(micro, dict):
        return None, ["MICRO_INVALID"]
    err = []
    trade_ms = EMA.num(micro.get("last_trade_ms"))
    book_ms = EMA.num(micro.get("last_book_ms"))
    trade_age = now_ms - trade_ms
    book_age = now_ms - book_ms
    if not (0 <= trade_age <= strict_age_ms):
        err.append("STALE_TRADE")
    if not (0 <= book_age <= strict_age_ms):
        err.append("STALE_BOOK")
    for key, blocker in (("micro_ready","MICRO_NOT_READY"),
                         ("sequence_verified","TRADE_SEQUENCE"),
                         ("book_sequence_verified","BOOK_SEQUENCE")):
        if micro.get(key) is not True:
            err.append(blocker)
    spread = micro.get("spread_bps")
    slippage = micro.get("slippage_bps")
    if spread is None or not 0 <= EMA.num(spread,-1) <= 20:
        err.append("SPREAD")
    if slippage is None or not 0 <= EMA.num(slippage,-1) <= 35:
        err.append("SLIPPAGE")
    if EMA.num(micro.get("last_price")) <= 0:
        err.append("LIVE_PRICE_MISSING")
    if err:
        return None, err
    return micro, []


def _foreign_signal_verified(row, now_ms, strict_age_ms=MICRO_QUOTE_MAX_AGE_MS):
    """Read-time gateway for already APPROVED independent engines.

    Neither ML research WATCHes nor V12 structural BUYs lacking the execution
    authority are promoted. Fresh common market evidence is mandatory.
    """
    symbol = str(row.get("symbol") or "").upper()
    authority = row.get("authority")
    micro, errors = _micro_authority_check(symbol, now_ms, strict_age_ms)
    if errors:
        return None
    price = EMA.num(micro.get("last_price"))
    if authority == "V15_ML":
        if ML is None or not (row.get("execution_ready") is True
                and row.get("action") == "ML BUY NOW"
                and row.get("promotion_ready") is True
                and EMA.num(row.get("selected_target_pct")) >= 10
                and row.get("setup_verification") == "UPSTREAM_STRUCTURAL"
                and row.get("anti_chase") is not True):
            return None
        board_ms = EMA.num(getattr(ML, "_last_board_ms", 0))
        if not 0 <= now_ms - board_ms <= SOURCE_DECISION_MAX_AGE_MS:
            return None
        # Latest model decision must still be in the authority's live board.
        live = next((x for x in list(getattr(ML, "_board", []) or [])
                     if x.get("symbol") == symbol and
                     x.get("lane") == row.get("lane") and
                     x.get("action") == "ML BUY NOW" and
                     x.get("execution_ready") is True and
                     x.get("promotion_ready") is True), None)
        if live is None:
            return None
        expected = EMA.num(row.get("price"))
        stop = EMA.num(row.get("dynamic_stop"))
        low = EMA.num(row.get("entry_low"))
        high = EMA.num(row.get("entry_high"))
        maximum = EMA.num(row.get("max_chase"))
        # Model authority is NOT transferable to a materially different price.
        if not (expected > 0 and abs(price/expected-1) <= .015
                and stop > 0 and stop < price
                and low > 0 and high >= low
                and low <= price <= high
                and (maximum <= 0 or price <= maximum)):
            return None
        target_pct = EMA.num(row.get("selected_target_pct"))
        target = price * (1 + target_pct/100.0)
        result = {
            "symbol":symbol, "authority":"V15_ML", "lane":row.get("lane"),
            "timeframe":row.get("timeframe") or "?",
            "setup":row.get("setup"), "entry":price, "stop":stop,
            "tp1":target, "tp2":None, "tp3":None,
            "target_pct":target_pct, "probability":row.get("probability"),
            "expected_time_to_target":row.get("expected_time_to_target"),
            "model_samples":row.get("model_samples"),
            "risk_pct":round(100 * (price-stop)/price,3),
        }
    elif authority == "V12_PINPOINT":
        if CORE is None or row.get("execution_state") != "BUY NOW":
            return None
        original = next((x for x in list(CORE._board() or [])
                         if x.get("symbol") == symbol and
                         x.get("execution_state") == "BUY NOW"), None)
        if original is None or not 0 <= now_ms - EMA.num(original.get("generated_ms")) <= SOURCE_DECISION_MAX_AGE_MS:
            return None
        verify = getattr(CORE, "_attach_execution_gate", None)
        if not callable(verify):
            return None
        try:
            checked = verify(symbol, original)
        except Exception:
            return None
        if checked.get("execution_state") != "BUY NOW" or checked.get("buy_now") is not True:
            return None
        expected = EMA.num(checked.get("current"))
        stop = EMA.num(checked.get("invalidation"))
        tps = [EMA.num(checked.get(k)) for k in ("tp1","tp2","tp3")]
        if not (expected > 0 and abs(price/expected-1) <= .015
                and stop > 0 and stop < price and
                all(x > price for x in tps if x > 0)
                and tps[0] > price):
            return None
        result = {
            "symbol":symbol, "authority":"V12_PINPOINT", "lane":checked.get("setup"),
            "timeframe":checked.get("timeframe") or "?",
            "setup":checked.get("setup"), "entry":price, "stop":stop,
            "tp1":tps[0], "tp2":tps[1] or None, "tp3":tps[2] or None,
            "target_pct":round(100*(tps[0]/price-1),2),
            "risk_pct":round(100*(price-stop)/price,3),
        }
    else:
        return None
    result["quote_checked_ms"] = now_ms
    result["trade_age_ms"] = now_ms - EMA.num(micro.get("last_trade_ms"))
    result["book_age_ms"] = now_ms - EMA.num(micro.get("last_book_ms"))
    result["quote_expires_ms"] = min(
        now_ms + SIGNAL_LIFETIME_MS,
        int(EMA.num(micro.get("last_trade_ms")) + strict_age_ms),
        int(EMA.num(micro.get("last_book_ms")) + strict_age_ms),
    )
    if result["quote_expires_ms"] <= now_ms:
        return None
    return result


def _candidate_authorities(now_ms):
    """Collect independent approved decisions, not merely candidate rankings."""
    approved = []
    inspected = {"ml_ranked": 0,"ml_approved":0,"v12_approved":0}
    ml_rows = []
    if ML is not None:
        ml_rows = list(getattr(ML, "_board", []) or [])
        inspected["ml_ranked"] = len(ml_rows)
        if 0 <= now_ms - EMA.num(getattr(ML, "_last_board_ms",0)) <= SOURCE_DECISION_MAX_AGE_MS:
            for row in ml_rows:
                if row.get("execution_ready") is True and row.get("action") == "ML BUY NOW":
                    inspected["ml_approved"] += 1
                    approved.append(dict(row, authority="V15_ML"))
    try:
        structural_rows = list(CORE._board() or [])
        for row in structural_rows:
            if row.get("execution_state") == "BUY NOW" and row.get("buy_now") is True:
                inspected["v12_approved"] += 1
                approved.append(dict(row, authority="V12_PINPOINT"))
    except (AttributeError, TypeError):
        pass

    # Subscriptions follow verified structure before unverified model ranks.
    # This is telemetry scheduling, NEVER a BUY authorisation.
    wanted = []
    def add(sym):
        sym = str(sym or "").upper()
        if re.fullmatch(r"[A-Z0-9]{2,24}USDT",sym) and sym not in wanted:
            wanted.append(sym)
    structural = sorted(locals().get("structural_rows", []),
        key=lambda r: (r.get("execution_state") == "BUY NOW",
            r.get("state") == "BUY",r.get("state") == "ARMED",
            EMA.num(r.get("setup_strength"))),reverse=True)
    for row in structural[:6]:
        add(row.get("symbol"))
    for row in sorted(ml_rows,key=lambda r: (
        r.get("setup_verification") == "UPSTREAM_STRUCTURAL",
        r.get("qualification_state") == "NEAR BUY",
        EMA.num(r.get("setup_strength")),
        EMA.num(r.get("probability"))),reverse=True):
        if row.get("setup_verification") == "UPSTREAM_STRUCTURAL":
            add(row.get("symbol"))
        if len(wanted) >= 12:
            break
    for row in ml_rows:
        add(row.get("symbol"))
        if len(wanted) >= MICRO_PRIORITY_SLOTS:
            break
    return approved, inspected, wanted



def _stable_market_priorities(candidates, at):
    """Hold real worker subscriptions for 120s instead of churning each tick."""
    for symbol, expires in list(_PRIORITY_LEASES.items()):
        if expires <= at:
            del _PRIORITY_LEASES[symbol]
    for symbol in candidates or []:
        symbol = str(symbol or "").upper()
        if len(_PRIORITY_LEASES) >= MICRO_PRIORITY_SLOTS:
            break
        if (re.fullmatch(r"[A-Z0-9]{2,24}USDT",symbol)
                and symbol not in _PRIORITY_LEASES):
            _PRIORITY_LEASES[symbol] = at + PRIORITY_HOLD_MS
    return list(_PRIORITY_LEASES)


def _subscription_coverage(at):
    """Full shortlisted ML universe: control, Binance ACK, real event ages."""
    samples=[]
    status_counts={}
    rank=list(getattr(ML,"_board",[]) or [])[:30] if ML is not None else []
    getter=getattr(CORE,"_subscription_telemetry",None)
    for item in rank:
        sym=str(item.get("symbol") or "").upper()
        try:
            data=getter(sym, at) if callable(getter) else {}
        except Exception:
            data={}
        status=data.get("status") or "NO_WORKER_SUBSCRIPTION_TELEMETRY"
        status_counts[status]=status_counts.get(status,0)+1
        samples.append({
            "symbol":sym,"lane":item.get("lane"),
            "status":status,
            "requested":bool(data.get("control_requested")),
            "trade_ack":bool(data.get("trade_acknowledged")),
            "book_ack":bool(data.get("book_acknowledged")),
            "trade_age_ms":data.get("trade_age_ms"),
            "book_age_ms":data.get("book_age_ms"),
            "trade_sequence_verified":bool(data.get("trade_sequence_verified")),
            "book_sequence_verified":bool(data.get("book_sequence_verified")),
            "setup_confirmation":item.get("setup_verification"),
            "ml_action":item.get("action")
        })
    return {
        "shortlisted":len(samples),
        "control_requested":sum(x["requested"] for x in samples),
        "both_acknowledged":sum(x["trade_ack"] and x["book_ack"] for x in samples),
        "event_aligned":status_counts.get("TRADE_BOOK_ALIGNED",0),
        "status_counts":status_counts
    },samples


def _qualification_report(at):
    if ML is None or not 0 <= at-EMA.num(getattr(ML,"_last_board_ms",0)) <= SOURCE_DECISION_MAX_AGE_MS:
        return [],{}
    rows,counts=[],{}
    for item in list(getattr(ML,"_board",[]) or [])[:30]:
        state=item.get("qualification_state") or "WATCH"
        counts[state]=counts.get(state,0)+1
        if len(rows)>=15:
            continue
        rows.append({
            "symbol":item.get("symbol"),"lane":item.get("lane"),
            "state":state,"action":item.get("action"),
            "setup_confirmation":item.get("setup_verification") or "NONE",
            "structural_state":item.get("structural_state") or "",
            "setup_not_confirmed":item.get("setup_verification") != "UPSTREAM_STRUCTURAL",
            "probability":item.get("probability"),
            "target_pct":item.get("selected_target_pct"),
            "live_source":item.get("live_evidence_source") or "SENSOR_CACHE",
            "blockers":list(item.get("blockers") or [])[:4],
            "decision_ms":int(EMA.num(getattr(ML,"_last_board_ms",0)))
        })
    return rows,counts



BUY_STRUCTURE_LIMIT = 20

def _buy_structure_report(now_ms):
    """Read-only, independent formal V12 structure board, NEVER a BUY signal."""
    try:
        formal = list(CORE._board() or []) if CORE is not None else []
    except (AttributeError, TypeError, ValueError):
        formal = []
    # ML's UPSTREAM_STRUCTURAL rows are backed by the *same real V12
    # evaluator*, including its on-demand refresh from 1H/4H/Daily candles.
    # They are a separate display source, not model-invented structure and
    # never imply a V12 execution approval. No sensor shadow is admitted.
    if ML is not None:
        ml_time = int(EMA.num(getattr(ML, "_last_board_ms", 0)))
        if 0 <= now_ms - ml_time <= SOURCE_DECISION_MAX_AGE_MS:
            for candidate in list(getattr(ML, "_board", []) or []):
                if not isinstance(candidate, dict) or (
                    candidate.get("setup_verification") != "UPSTREAM_STRUCTURAL"
                ):
                    continue
                formal.append({
                    "symbol":candidate.get("symbol"),
                    "state":candidate.get("structural_state"),
                    "setup":candidate.get("setup"),
                    "timeframe":candidate.get("timeframe"),
                    "generated_ms":ml_time,
                    "setup_strength":candidate.get("setup_strength"),
                    "current":candidate.get("price"),
                    "entry_low":candidate.get("entry_low"),
                    "entry_high":candidate.get("entry_high"),
                    "invalidation":candidate.get("invalidation"),
                    "tp1":candidate.get("selected_target_price"),
                    "execution_state":candidate.get("structural_execution_state"),
                    "execution_blockers":candidate.get("blockers"),
                    "setup_source":"V12_CONFIRMED_VIA_ML",
                    "target_source":"ML_MODEL_TARGET_NOT_V12_TP",
                })
    rows, seen = [], set()
    counts = {"BUY": 0, "ARMED": 0, "WATCH": 0}
    for item in formal:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        state = str(item.get("state") or "").upper()
        setup = str(item.get("setup") or "").strip()
        source_ms = int(EMA.num(item.get("generated_ms")))
        age_ms = now_ms - source_ms
        if (not re.fullmatch(r"[A-Z0-9]{2,24}USDT", symbol)
                or symbol in seen or not setup
                or item.get("setup_source") == "SENSOR_SHADOW"
                or state not in counts
                or not 0 <= age_ms <= SOURCE_DECISION_MAX_AGE_MS):
            continue
        seen.add(symbol)
        counts[state] += 1
        low,high,stop,tp1,reference = (
            EMA.num(item.get(k)) for k in
            ("entry_low","entry_high","invalidation","tp1","current")
        )
        valid_levels = (low > 0 and high >= low and 0 < stop < high
                        and tp1 > high and reference > 0)
        blockers = list(item.get("execution_blockers") or [])[:3]
        if not valid_levels:
            blockers = ["RISK_LEVELS_UNAVAILABLE"] + blockers
        rows.append({
            "symbol":symbol, "setup":setup,
            "source":str(item.get("setup_source") or "V12_DIRECT"),
            "target_source":str(item.get("target_source") or "V12_TP1"),
            "timeframe":str(item.get("timeframe") or "?"),
            "state":state, "status":"STRUCTURE "+state,
            "execution_state":str(item.get("execution_state") or "NOT_APPROVED"),
            "source_ms":source_ms, "source_age_ms":age_ms,
            "reference_price":reference,
            "entry_low":low if valid_levels else None,
            "entry_high":high if valid_levels else None,
            "stop":stop if valid_levels else None,
            "tp1":tp1 if valid_levels else None,
            "potential_pct":round(100*(tp1/high-1),2) if valid_levels else None,
            "setup_strength":EMA.num(item.get("setup_strength")),
            "blockers":blockers,
            "verified_buy_now":False,
        })
    rows.sort(key=lambda x: (
        {"BUY":3,"ARMED":2,"WATCH":1}[x["state"]],
        x["setup_strength"]), reverse=True)
    return rows[:BUY_STRUCTURE_LIMIT], {
        "formal_fresh":len(rows), "shown":min(len(rows),BUY_STRUCTURE_LIMIT),
        "structural_buy":counts["BUY"], "armed":counts["ARMED"],
        "watch":counts["WATCH"],
        "execution_authority":"V12_PINPOINT_READ_TIME_ONLY",
    }


def _observe_missed_rallies(now_ms, chosen, accepted):
    """Observed window only: verified trade/book quotes, never cached closes."""
    active=set(accepted or [])
    for sym in chosen[:MICRO_PRIORITY_SLOTS]:
        mm, errors=_micro_authority_check(sym,now_ms)
        if errors:
            continue
        price=EMA.num(mm.get("last_price"))
        if price<=0:
            continue
        previous=_OBSERVED_QUOTES.get(sym)
        if previous is None or now_ms-previous["first_ms"]>MOVER_WINDOW_MS:
            reason="UNCLASSIFIED"
            if ML is not None:
                candidate=next((r for r in list(getattr(ML,"_board",[]) or [])
                                if r.get("symbol")==sym),None)
                if candidate:
                    reason=(candidate.get("qualification_state") or
                            candidate.get("action") or "WATCH")
            _OBSERVED_QUOTES[sym]={
                "first_ms":now_ms,"low_ms":now_ms,"low":price,
                "first_price":price,"previous_state":reason,
                "had_approved_buy":sym in active,"reported":False
            }
            continue
        previous["had_approved_buy"] |= sym in active
        baseline=previous["low"]
        change=100*(price/baseline-1) if baseline else 0
        if (change>=MOVER_THRESHOLD_PCT and not previous["reported"]
                and now_ms-previous["low_ms"]>=60000):
            _MISSED_MOVES.append({
                "symbol":sym,"observed_gain_pct":round(change,2),
                "from_price":baseline,"to_price":price,
                "start_ms":previous["low_ms"],"end_ms":now_ms,
                "prior_status":previous["previous_state"],
                "had_approved_buy":previous["had_approved_buy"],
                "scope":"OBSERVED_QUOTES_ONLY"
            })
            previous["reported"]=True
        if price<previous["low"]:
            previous["low"]=price
            previous["low_ms"]=now_ms
            previous["reported"]=False
            previous["previous_state"]="WATCH"
    if len(_OBSERVED_QUOTES)>250:
        for sym,item in list(_OBSERVED_QUOTES.items()):
            if now_ms-item["first_ms"]>MOVER_WINDOW_MS:
                del _OBSERVED_QUOTES[sym]


def _publish_once_unlocked(now_ms=None):
    """Refresh entire cached EMA opportunity set; no 45s reporting dependency."""
    global _SNAPSHOT, _ACTIVE, _NEXT_ID
    now_ms = _ms() if now_ms is None else int(now_ms)
    records, frames, live_checked = EMA.scan_cached_ema(CORE, now_ms / 1000)
    buys = {}
    technical = set()
    foreign, inspected, wanted_ml = _candidate_authorities(now_ms)
    for symbol, item in records:
        if EMA._technical_complete(item) and not symbol.startswith(
            ("XUSD", "BFUSD", "USDC", "USD1", "FDUSD", "TUSD")
        ):
            technical.add(symbol)
        if item.get("status") == "BUY NOW — EMA":
            key = (symbol, item["timeframe"], item["ema_period"])
            buys[key] = dict(symbol=symbol, **item)
    ranked = sorted(records, key=lambda pair: EMA._ema_rank(pair[1]), reverse=True)
    # Top-ranked entries can legitimately persist across many 1H/4H candles.
    # Keep them separate from a rotation of additional evidence-backed WATCHes.
    eligible = []
    seen = set()
    for symbol, item in ranked:
        if (symbol in seen or symbol.startswith(
                ("XUSD", "BFUSD", "USDC", "USD1", "FDUSD", "TUSD"))
                or abs(item.get("distance_pct", 999)) > 1):
            continue
        seen.add(symbol)
        tf = item["timeframe"]
        frame = ((getattr(CORE, "_cache", {}) or {}).get(symbol) or {}).get(tf) or {}
        updated_ms = int(EMA.num(frame.get("updated")) * 1000)
        key = (symbol, tf, item["ema_period"])
        signature = (
            item.get("status"), item.get("distance_pct"),
            item.get("seller_exhaustion"), item.get("buyer_reclaim"),
            item.get("touch"),
        )
        previous = _RESEARCH_PREVIOUS.get(key)
        if previous is None:
            change = "NEW"
            changed_ms = now_ms
        elif previous["signature"] != signature:
            change = "CHANGED"
            changed_ms = now_ms
        else:
            change = "UNCHANGED"
            changed_ms = previous["changed_ms"]
        _RESEARCH_PREVIOUS[key] = {
            "signature": signature, "changed_ms": changed_ms, "last_seen_ms": now_ms
        }
        eligible.append({
            "symbol": symbol, "timeframe": tf,
            "ema_period": item["ema_period"],
            "distance_pct": item["distance_pct"],
            "status": item["status"],
            "touch": bool(item["touch"]),
            "seller_exhaustion": item["seller_exhaustion"],
            "buyer_reclaim": item["buyer_reclaim"],
            "source": "CACHED_CANDLE",
            "source_updated_ms": updated_ms,
            "source_age_s": round(max(0, now_ms - updated_ms) / 1000, 1)
                if updated_ms > 0 else None,
            "change": change, "last_change_ms": changed_ms,
        })
    # Limit history for symbols which no longer meet the criteria.
    if len(_RESEARCH_PREVIOUS) > 2000:
        cutoff = now_ms - 3600000
        for key, record in list(_RESEARCH_PREVIOUS.items()):
            if record["last_seen_ms"] < cutoff:
                del _RESEARCH_PREVIOUS[key]
    primary = [r for r in eligible if r["touch"]][:10]
    research = [r["symbol"] for r in primary]
    research_rows = primary
    primary_symbols = set(research)
    alternatives = [r for r in eligible if r["symbol"] not in primary_symbols]
    rotation_tick = now_ms // RESEARCH_ROTATE_MS
    rotating = []
    if alternatives:
        start = (rotation_tick * RESEARCH_ROTATE_COUNT) % len(alternatives)
        rotating = [
            alternatives[(start + i) % len(alternatives)]
            for i in range(min(RESEARCH_ROTATE_COUNT, len(alternatives)))
        ]
    recent_changes = sum(r["change"] != "UNCHANGED" for r in eligible)
    # The V15/V12 routes are additive, not gated on the EMA-specific rule.
    # All existing explicit market-safety checks stay fail-closed.
    foreign_approved = []
    foreign_rejections = 0
    for candidate in foreign:
        verified = _foreign_signal_verified(candidate, now_ms)
        if verified is None:
            foreign_rejections += 1
        else:
            # Retain the underlying strategy approval for read-time rechecks.
            foreign_approved.append(candidate)
    priorities=_stable_market_priorities(wanted_ml,now_ms)
    try:
        CORE._signal_priority_symbols=list(priorities)
    except (AttributeError, TypeError):
        pass
    approved_symbols={key[0] for key in buys} | {
        row["symbol"] for row in foreign_approved
    }
    _observe_missed_rallies(now_ms,priorities,approved_symbols)
    qualification_rows,qualification_counts=_qualification_report(now_ms)
    buy_structure_rows,buy_structure_summary=_buy_structure_report(now_ms)
    structure_summary={
        "available":int(getattr(ML,"_stats",{}).get("structure_candidates_available",0) or 0) if ML else 0,
        "shown":sum(x.get("setup_verification")=="UPSTREAM_STRUCTURAL" for x in
                    list(getattr(ML,"_board",[]) or [])[:30]) if ML else 0,
        "formal_buy_shown":sum(
            x.get("setup_verification")=="UPSTREAM_STRUCTURAL"
            and str(x.get("structural_state") or "").upper()=="BUY"
            for x in list(getattr(ML,"_board",[]) or [])[:30]) if ML else 0,
    }
    subscription_summary,subscription_rows=_subscription_coverage(now_ms)
    new_active = set(buys) | {
        (v["symbol"],v["authority"],v.get("lane")) for v in foreign_approved
    }
    with _LOCK:
        for key in sorted(new_active - _ACTIVE):
            if key in buys:
                normalized = dict(buys[key], authority="EMA", lane="EMA")
            else:
                candidate = next((v for v in foreign_approved if
                    (v["symbol"], v["authority"],v.get("lane")) == key),None)
                normalized = _foreign_signal_verified(candidate,now_ms) if candidate else None
            if not normalized:
                continue
            _NEXT_ID += 1
            _EVENTS.append({
                "id":_NEXT_ID,"type":"BUY_NOW_VERIFIED",
                "symbol":normalized["symbol"],
                "authority":normalized.get("authority","EMA"),
                "lane":normalized.get("lane","EMA"),
                "timeframe":normalized.get("timeframe"),
                "ema_period":normalized.get("ema_period",0),
                "entry":normalized["entry"],"stop":normalized["stop"],
                "tp1":normalized["tp1"],"tp2":normalized.get("tp2"),
                "tp3":normalized.get("tp3"),"risk_pct":normalized["risk_pct"],
                "generated_ms":now_ms,
                "expires_ms":min(now_ms+SIGNAL_LIFETIME_MS,
                                 normalized.get("quote_expires_ms",now_ms+SIGNAL_LIFETIME_MS)),
                "order_placement":False,
            })
        _ACTIVE = new_active
        _SNAPSHOT = {
            "revision": "15.31-independent-buy-structure-lane",
            "generated_ms": now_ms,
            "expires_ms": now_ms + SIGNAL_LIFETIME_MS,
            "candle_frames": frames,
            "interactions": len(records),
            "research_top10": research,
            "research_rows": research_rows,
            "rotating_research_rows": rotating,
            "research_eligible_count": len(eligible),
            "research_alternative_count": len(alternatives),
            "research_source_changes": recent_changes,
            "research_rotation_tick": rotation_tick,
            "technical_ready_symbols": sorted(technical),
            "buy_signals": list(buys.values()),
            "foreign_buy_signals": foreign_approved,
            "foreign_approved_count": len(foreign_approved),
            "foreign_screened_count": len(foreign),
            "foreign_rejected_at_gate": foreign_rejections,
            "authority_diagnostics": inspected,
            "ml_priority_symbols": list(priorities),
            "qualification_rows": qualification_rows,
            "qualification_counts": qualification_counts,
            "structure_summary": structure_summary,
            "buy_structure_rows": buy_structure_rows,
            "buy_structure_summary": buy_structure_summary,
            "subscription_summary": subscription_summary,
            "subscription_rows": subscription_rows,
            "missed_rallies": list(_MISSED_MOVES)[-10:][::-1],
            "missed_rally_count": sum(not x["had_approved_buy"] for x in _MISSED_MOVES),
            "live_evidence_checked": live_checked,
            "last_event_id": _NEXT_ID,
            "order_placement": False,
        }
        _STATUS["cycles"] += 1
    if new_active:
        print(f"PSI-V15.25 LIVE_BUY_FEED liveBuys={len(new_active)} "
              f"events={_NEXT_ID} generated={now_ms}", flush=True)
    return dict(_SNAPSHOT)


def publish_once(now_ms=None):
    """Serialise refresh requests and supervisor publications."""
    with _PUBLISH_LOCK:
        return _publish_once_unlocked(now_ms)


def _fresh_buy_rows(snapshot, now_ms):
    if (not snapshot or now_ms - int(snapshot.get("generated_ms") or 0) > MAX_AGE_MS
            or now_ms >= int(snapshot.get("expires_ms") or 0)):
        return []
    passed = []
    for row in snapshot.get("buy_signals") or []:
        fresh = _latest_verified(row, now_ms)
        if fresh:
            fresh["authority"] = "EMA"
            fresh["lane"] = "EMA"
            passed.append(fresh)
    for row in snapshot.get("foreign_buy_signals") or []:
        # Re-run the *same* independent authority check at response time.
        fresh = _foreign_signal_verified(row, now_ms)
        if fresh:
            passed.append(fresh)
    return passed


def _group_verified_signals(approved):
    """Group only the already revalidated, execution-approved rows."""
    result={"EMA":[],"ML":[],"V12":[]}
    keys={"EMA":"EMA","V15_ML":"ML","V12_PINPOINT":"V12"}
    for row in approved or []:
        if not isinstance(row,dict):
            continue
        group=keys.get(row.get("authority"))
        if group:
            result[group].append(dict(row))
    return result


def read_live(now_ms=None):
    now_ms = _ms() if now_ms is None else int(now_ms)
    with _LOCK:
        snap = dict(_SNAPSHOT)
        status = dict(_STATUS)
    current = bool(
        snap and 0 <= now_ms - int(snap.get("generated_ms") or 0) <= MAX_AGE_MS
        and now_ms < int(snap.get("expires_ms") or 0)
    )
    approved = _fresh_buy_rows(snap, now_ms) if current else []
    v12_approved = {(str(r.get("symbol")), str(r.get("setup")))
                    for r in approved if r.get("authority") == "V12_PINPOINT"}
    structure = []
    if current:
        for item in snap.get("buy_structure_rows") or []:
            age = now_ms - int(item.get("source_ms") or 0)
            if not 0 <= age <= SOURCE_DECISION_MAX_AGE_MS:
                continue
            verified = (item.get("symbol"),item.get("setup")) in v12_approved
            structure.append(dict(item,source_age_ms=age,
                verified_buy_now=verified,
                status=("VERIFIED BUY NOW" if verified else item["status"])))
    return {
        "ok": True, "revision": "15.31-independent-buy-structure-lane",
        "server_time_ms": now_ms,
        "generated_ms": snap.get("generated_ms"),
        "snapshot_age_ms": now_ms - snap["generated_ms"] if snap.get("generated_ms") else None,
        "expires_ms": snap.get("expires_ms"),
        "fresh": current,
        "status": ("BUY_NOW_VERIFIED" if approved else
                   "NO_VERIFIED_BUY" if current else "DATA_STALE"),
        "buy_count": len(approved),
        "buy_signals": approved,
        "verified_lanes": _group_verified_signals(approved),
        "verified_lane_counts": {k:len(v) for k,v in _group_verified_signals(approved).items()},
        "authority_diagnostics": snap.get("authority_diagnostics", {}) if current else {},
        "foreign_screened_count": snap.get("foreign_screened_count", 0) if current else 0,
        "foreign_rejected_at_gate": snap.get("foreign_rejected_at_gate", 0) if current else 0,
        "ml_priority_symbols": snap.get("ml_priority_symbols", []) if current else [],
        "qualification_rows": snap.get("qualification_rows", []) if current else [],
        "qualification_counts": snap.get("qualification_counts", {}) if current else {},
        "structure_summary": snap.get("structure_summary", {}) if current else {},
        "buy_structure_rows": structure,
        "buy_structure_summary": snap.get("buy_structure_summary", {}) if current else {},
        "subscription_summary": snap.get("subscription_summary", {}) if current else {},
        "subscription_rows": snap.get("subscription_rows", []) if current else [],
        "missed_rallies": snap.get("missed_rallies", []) if current else [],
        "missed_rally_count": snap.get("missed_rally_count", 0) if current else 0,
        "research_top10": snap.get("research_top10", []) if current else [],
        "research_rows": snap.get("research_rows", []) if current else [],
        "rotating_research_rows": snap.get("rotating_research_rows", []) if current else [],
        "research_eligible_count": snap.get("research_eligible_count", 0) if current else 0,
        "research_alternative_count": snap.get("research_alternative_count", 0) if current else 0,
        "research_source_changes": snap.get("research_source_changes", 0) if current else 0,
        "research_rotation_tick": snap.get("research_rotation_tick") if current else None,
        "technical_ready_symbols": snap.get("technical_ready_symbols", []) if current else [],
        "live_evidence_checked": snap.get("live_evidence_checked", 0) if current else 0,
        "cycles": status["cycles"], "errors": status["errors"],
        "last_error": status["last_error"],
        "order_placement": False,
    }



async def _live_checked():
    """Get a truly current snapshot; a stale cache triggers a fresh evaluation."""
    live = await asyncio.to_thread(read_live)
    if not live["fresh"]:
        await asyncio.to_thread(publish_once)
        live = await asyncio.to_thread(read_live)
    return live


async def http_live(request):
    return web.json_response(
        await _live_checked(), headers={"Cache-Control": "no-store, max-age=0"}
    )


async def http_quote(request):
    """Read-only manual quote check: do not extend source-data or market TTLs."""
    symbol = (request.query.get("symbol") or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{2,24}USDT", symbol):
        return web.json_response(
            {"ok": False, "status": "INVALID_SYMBOL", "buy_count": 0,
             "quotes": [], "order_placement": False},
            status=400, headers={"Cache-Control": "no-store"}
        )
    # An on-demand request cannot depend on the previous reporter tick.
    await asyncio.to_thread(publish_once)
    checked_ms = _ms()
    with _LOCK:
        snap = dict(_SNAPSHOT)
    active = (
        snap and 0 <= checked_ms - int(snap.get("generated_ms") or 0) <= MAX_AGE_MS
        and checked_ms < int(snap.get("expires_ms") or 0)
    )
    quotes = []
    if active:
        for row in (snap.get("buy_signals") or []) + (snap.get("foreign_buy_signals") or []):
            if row.get("symbol") != symbol:
                continue
            verified = (_foreign_signal_verified(row, checked_ms, strict_age_ms=1200)
                        if row.get("authority") in {"V15_ML","V12_PINPOINT"}
                        else _latest_verified(row, checked_ms, strict_micro_age_ms=1200))
            if verified is not None:
                # The source snapshot may expire earlier than the micro lease.
                verified["quote_expires_ms"] = min(
                    verified["quote_expires_ms"], int(snap["expires_ms"])
                )
                if checked_ms < verified["quote_expires_ms"]:
                    quotes.append(verified)
    return web.json_response({
        "ok": True, "revision": "15.25-on-demand-unified-verified-quote",
        "symbol": symbol, "server_time_ms": checked_ms,
        "generated_ms": snap.get("generated_ms"),
        "expires_ms": snap.get("expires_ms"),
        "status": ("VERIFIED_AT_READ" if quotes else
                   "NO_VERIFIED_BUY" if active else "DATA_STALE"),
        "buy_count": len(quotes), "quotes": quotes,
        "order_placement": False,
        "disclaimer": "Read-only observation; venue conditions can change before an order.",
    }, headers={"Cache-Control": "no-store, max-age=0"})


_DASHBOARD = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="theme-color" content="#0a111d">
<meta name="description" content="Live Binance Spot market intelligence, independently verified signal lanes and structural research. Read-only scanner.">
<title>Live Scanner · Market Intelligence</title>
<style>
:root{color-scheme:dark;--bg:#0a111d;--surface:#121d2b;--surface-2:#172536;--edge:#26374a;--line:#213144;--text:#e9f1f7;--muted:#9aafc0;--mint:#6be4b6;--mint-bg:rgba(73,195,153,.11);--amber:#efc77b;--red:#ff9d9d;--blue:#99c8ef;font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;font-synthesis:none}
*{box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:96px}
body{margin:0;min-width:280px;color:var(--text);background:radial-gradient(ellipse at 85% -120px,rgba(55,100,120,.16),transparent 500px),var(--bg);font-size:14px;line-height:1.55;-webkit-text-size-adjust:100%}
.app-shell{max-width:1360px;margin:0 auto;padding:0 27px 72px}
.topbar{min-height:94px;display:flex;align-items:center;justify-content:space-between;gap:20px;border-bottom:1px solid var(--line)}
.brand{display:flex;align-items:center;gap:14px;min-width:0}
.brand-mark{width:43px;height:43px;flex-shrink:0;display:grid;place-items:center;border:1px solid #436c72;border-radius:13px;background:linear-gradient(135deg,#17343c,#10222c);box-shadow:0 0 25px rgba(53,170,143,.08)}
.brand-mark:after{content:"";width:16px;height:16px;border:3px solid var(--mint);border-top-color:transparent;transform:rotate(-45deg);border-radius:4px}
.kicker{font-size:10px;font-weight:800;color:#8fb0bd;letter-spacing:.15em;text-transform:uppercase}
h1{font-size:20px;letter-spacing:-.045em;line-height:1.2;margin:2px 0 0;font-weight:750}
.version{display:inline-block;margin-left:7px;padding:3px 6px;border-radius:5px;background:#253343;border:1px solid #344456;color:#9fb8cc;font-size:10px;vertical-align:3px;letter-spacing:0;font-weight:750}
.header-meta{display:flex;align-items:center;gap:16px;color:var(--muted);font-size:12px;white-space:nowrap}
.header-market{border:1px solid var(--edge);border-radius:999px;padding:7px 11px;letter-spacing:.06em;font-weight:700;font-size:10px;color:#c5d4df}
.last-check{font-variant-numeric:tabular-nums}
.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:20px;padding:34px 0 25px}
.hero h2{font-size:clamp(25px,3.4vw,38px);line-height:1.13;letter-spacing:-.045em;margin:7px 0 12px;font-weight:760}
.hero p{max-width:700px;margin:0;color:var(--muted);font-size:13px}
.hero-stamp{flex-shrink:0;align-self:flex-start;border:1px solid var(--edge);border-radius:9px;color:var(--muted);padding:10px 13px;background:rgba(21,38,53,.5);font-size:10px;font-weight:750;letter-spacing:.11em}
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:13px}
.metric{min-width:0;padding:18px 19px;background:linear-gradient(155deg,#152335,#101b29);border:1px solid var(--edge);border-radius:13px}
.metric:first-child{background:linear-gradient(145deg,rgba(35,98,82,.29),#101c27);border-color:#325c58}
.metric-label{display:flex;align-items:center;gap:8px;color:#a9bdcb;font-size:11px;font-weight:700;letter-spacing:.055em;text-transform:uppercase}
.metric-label:before{content:"";height:7px;width:7px;background:#789ab1;border-radius:50%;flex-shrink:0}
.metric:first-child .metric-label:before{background:var(--mint);box-shadow:0 0 12px rgba(101,224,179,.55)}
.metric-value{font-size:29px;line-height:1.1;font-weight:750;margin-top:11px;font-variant-numeric:tabular-nums;letter-spacing:-.04em}
.metric:first-child .metric-value{color:var(--mint)}
.metric-detail{font-size:11px;color:#8fa5b6;margin-top:8px}
.connection-panel{display:grid;grid-template-columns:minmax(0,1fr);gap:7px;padding:13px 16px;border-radius:11px;border:1px solid var(--edge);background:rgba(18,29,43,.78);font-size:12px;overflow-wrap:anywhere}
#status{font-variant-numeric:tabular-nums;font-weight:650;padding-left:18px;position:relative;color:var(--muted)}
#status:before{content:"";position:absolute;top:6px;left:0;width:8px;height:8px;border-radius:50%;background:#7895a6}
#status.good:before{background:var(--mint);box-shadow:0 0 9px rgba(100,228,180,.7)}
#status.bad:before{background:var(--red)}
#authority{padding-left:18px;color:#a1b2c1}
.quick{display:flex;gap:7px;overflow-x:auto;overscroll-behavior-x:contain;scrollbar-width:none;margin:22px 0 20px;padding:0 0 2px}
.quick::-webkit-scrollbar{display:none}
.quick a{flex-shrink:0;text-decoration:none;color:#b7c9d6;font-size:12px;font-weight:700;border:1px solid var(--edge);background:#142233;border-radius:8px;padding:10px 16px;transition:background .15s,border-color .15s}
.quick a:hover,.quick a:focus-visible{background:#1b3145;border-color:#4b697d;color:#fff}
main>section.panel{scroll-margin-top:86px}
.panel{background:var(--surface);border:1px solid var(--edge);border-radius:15px;padding:21px 22px;margin:0 0 15px;box-shadow:0 4px 24px rgba(0,0,0,.08)}
.approved{background:linear-gradient(165deg,rgba(20,53,49,.45),#121e2c 42%);border-color:#335a55}
.section-header{display:flex;align-items:center;justify-content:space-between;gap:14px;flex-wrap:wrap;margin-bottom:7px}
.section-tag{padding:5px 9px;background:#203548;border:1px solid #305064;color:#abc9db;border-radius:7px;font-size:10px;letter-spacing:.09em;text-transform:uppercase;font-weight:800}
.tag-mint{background:var(--mint-bg);color:var(--mint);border-color:#356c59}
h2{font-size:17px;letter-spacing:-.025em;line-height:1.35;font-weight:730;margin:0;color:#f0f5f9}
h3{font-size:13px;margin:22px 0 11px;font-weight:750;letter-spacing:.005em;color:#d6e7ed}
h3 span{color:var(--mint);font-variant-numeric:tabular-nums}
p{color:var(--muted);margin:7px 0 15px;font-size:12px;line-height:1.7}
.inline-number{color:var(--mint);font-variant-numeric:tabular-nums}
.detail-note{font-size:11px;color:#8099ab}
.note{padding:11px 13px;background:#162637;border:1px solid #263b4e;border-radius:9px;margin:12px 0;font-size:12px;color:#c1cfda;overflow-wrap:anywhere}
#verifiedSummary{background:var(--mint-bg);border-color:#315b4f;color:#a6edd0}
#quote{font-variant-numeric:tabular-nums;line-height:1.8}
.table-scroll{overflow-x:auto;max-width:100%;-webkit-overflow-scrolling:touch}
table{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:13px 11px;border-bottom:1px solid #263648;vertical-align:middle}
th{color:#8ca3b6;white-space:nowrap;font-size:10px;font-weight:800;letter-spacing:.065em;text-transform:uppercase}
td{color:#d4e0e9;overflow-wrap:anywhere}
td:first-child{font-weight:750;color:#f2f6fa;white-space:nowrap}
tbody tr:last-child td{border-bottom:0}
tbody tr:hover td{background:rgba(141,182,211,.045)}
button{font:inherit;font-size:11px;font-weight:750;cursor:pointer;padding:9px 12px;background:rgba(55,128,113,.19);border:1px solid #3a8573;border-radius:7px;color:#abf3d4;white-space:nowrap;min-height:38px}
button:hover,button:focus-visible{background:#255c50;color:#fff}
button:disabled{opacity:.4;cursor:default}
.good,.text-mint,.state-buy{color:var(--mint)}
.warn,.state-armed{color:var(--amber)}
.bad{color:var(--red)}
.state-watch{color:#f2df90}
.state-buy,.state-armed,.state-watch{font-weight:800}
.panel .table-scroll{margin-top:9px}
code{word-break:break-word}
.footer{margin:26px 0 0;color:#7f97a8;font-size:11px;text-align:center}
:focus-visible{outline:2px solid #8bdac2;outline-offset:3px}
@media(max-width:900px){
 .app-shell{padding:0 18px 85px}
 .metrics{grid-template-columns:repeat(2,minmax(0,1fr))}
 .header-meta .last-check{display:none}
 .hero-stamp{display:none}
}
@media(max-width:700px){
 html{scroll-padding-top:80px}
 .app-shell{padding:0 13px calc(104px + env(safe-area-inset-bottom))}
 .topbar{min-height:72px;gap:10px}
 .brand{gap:10px}
 .brand-mark{width:37px;height:37px;border-radius:10px}
 h1{font-size:17px}
 .header-meta{gap:7px}
 .header-market{font-size:9px;padding:6px 8px}
 .hero{padding:24px 3px 20px}
 .hero h2{font-size:27px;margin:5px 0 10px}
 .hero p{font-size:12px}
 .metrics{gap:9px}
 .metric{padding:13px;border-radius:11px}
 .metric-label{font-size:9px;line-height:1.4}
 .metric-value{font-size:27px;margin-top:8px}
 .metric-detail{font-size:10px;margin-top:6px}
 .connection-panel{padding:12px}
 .quick{position:fixed;z-index:40;left:0;bottom:0;right:0;padding:8px 10px calc(9px + env(safe-area-inset-bottom));margin:0;display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:5px;background:rgba(10,17,29,.97);border-top:1px solid #2c3c4e;box-shadow:0 -8px 28px rgba(0,0,0,.23)}
 .quick a{font-size:10px;text-align:center;line-height:1.25;padding:9px 2px;border-radius:8px;white-space:normal;display:grid;place-items:center;min-height:42px}
 .panel{padding:16px 13px;border-radius:12px;margin-bottom:11px}
 .section-header{align-items:flex-start}
 h2{font-size:16px}
 h3{margin:20px 0 9px;font-size:12px}
 .note{padding:10px 11px;margin:11px 0}
 table,thead,tbody,tr,td{box-sizing:border-box}
 table,tbody{display:block;width:100%;min-width:0!important}
 thead{display:none}
 .table-scroll{overflow:visible}
 tbody{display:grid;gap:9px;padding-top:8px}
 tbody tr{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px 14px;background:#172637;border:1px solid #2a3d50;border-radius:10px;padding:13px;min-width:0}
 .approved tbody tr{background:#162e32;border-color:#31534d}
 tbody tr td{min-width:0;display:flex;flex-direction:column;align-items:flex-start;justify-content:center;gap:3px;border:0!important;padding:0!important;white-space:normal!important;line-height:1.45;font-size:12px}
 tbody tr td:before{content:attr(data-label);color:#86a1b5;display:block;font-size:9px;font-weight:750;text-transform:uppercase;letter-spacing:.07em}
 tbody tr td:first-child{grid-column:1/-1;font-size:16px;color:#fff;font-weight:800;padding-bottom:8px!important;border-bottom:1px solid #2e4655!important}
 tbody tr td:first-child:before{content:"Pair";font-size:9px}
 tbody tr td[data-label="Verify"]{grid-column:1/-1}
 tbody tr td[data-label="Verify"] button{width:100%;margin-top:3px;font-size:12px}
 tbody tr td[colspan]{grid-column:1/-1;font-size:12px!important;border:0!important;padding:3px 0!important;font-weight:500!important;color:#b3c6d5}
 tbody tr td[colspan]:before{display:none}
 tbody tr td[data-label="Execution blockers"],tbody tr td[data-label="Primary blocker"]{grid-column:1/-1}
 .footer{margin-top:17px}
}
@media(max-width:370px){
 .brand-mark{width:32px;height:32px}
 h1{font-size:15px}
 .version{font-size:9px}
 .header-market{font-size:8px}
 .quick a{font-size:9px}
 tbody tr{gap:9px}
}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}*{transition:none!important}}

/* Presentation-only improvements: onboarding, setup lanes and mobile cards. */
:root{--bg:#090f19;--surface:#121e2b;--surface-2:#1a2a3b;--edge:#2a3e50;--mint:#69e7bd;--amber:#ffd18a}
body{background:radial-gradient(ellipse at 14% 0,rgba(61,135,146,.12),transparent 420px),#090f19}
.topbar{min-height:82px}.brand-mark{background:linear-gradient(135deg,#174447,#12323a)}
.hero h2{max-width:760px}.hero p{font-size:14px}
.panel{border-radius:17px;box-shadow:0 8px 30px rgba(0,0,0,.10)}
.start-here{margin:20px 0 6px;padding:23px;border-radius:18px;background:linear-gradient(135deg,#18333a,#11202f 65%);border:1px solid #346064}
.start-here h2{font-size:22px;margin:4px 0 10px}.start-here>p{font-size:12px;margin:10px 0 0}
.guide-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;margin-top:15px}
.guide-card{padding:14px;border-radius:12px;background:rgba(11,23,33,.65);border:1px solid rgba(145,196,207,.15)}
.guide-card strong{font-size:13px;display:block;margin-bottom:4px}.guide-card p{font-size:12px;margin:0}
.guide-dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:6px;background:var(--mint)}
.guide-card.arm .guide-dot{background:var(--amber)}
.guide-card.watch .guide-dot{background:#9db7d2}
.quick{margin:16px 0 9px}.quick a{border-radius:999px;padding:10px 15px}
.lane-links{display:flex;gap:8px;overflow-x:auto;padding:7px 0 19px;scrollbar-width:none;-webkit-overflow-scrolling:touch}
.lane-links::-webkit-scrollbar{display:none}
.lane-links a{display:inline-flex;flex:none;align-items:center;white-space:nowrap;text-decoration:none;background:#1a2b3b;color:#b9d0de;border:1px solid #314a5f;padding:8px 13px;border-radius:999px;font-size:12px;font-weight:700}
.lane-links a:hover,.lane-links a:focus-visible{color:#fff;background:#234356}
.lane-card{border-left:3px solid #689bc6}.lane-card.beast{border-left-color:#dcb37c}.lane-card.ema{border-left-color:#78bcdc}
.lane-card.exhaustion{border-left-color:#c4a7e5}.lane-card.breakout{border-left-color:#72ceac}.lane-card.pullback{border-left-color:#d8c482}
.lane-card .lane-intro{font-size:13px;max-width:900px}
.lane-count{color:#c9e6ea;font-variant-numeric:tabular-nums}
.simple-stage{font-weight:750;color:#edc886}
.simple-stage.verified-stage{color:var(--mint)}
.advanced-panel{padding:15px 20px}.advanced-panel summary{cursor:pointer;list-style:none;display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:14px;font-weight:750;color:#ddeaf2;min-height:36px}
.advanced-panel summary::-webkit-details-marker{display:none}
.advanced-panel summary:after{content:"+";font-weight:500;font-size:24px;color:#9bc2d0}
.advanced-panel[open] summary:after{content:"−"}
.advanced-panel summary small{margin-left:auto;color:#91aebf;font-size:11px;font-weight:600}
.advanced-panel .section-header{margin-top:13px}
.section-heading-caption{font-size:11px;color:#96b2c4;font-weight:500}
.empty-reason{font-size:12px;color:#9fbacc}
#verified{scroll-margin-top:85px}
@media(max-width:700px){
 .topbar{min-height:70px}.header-meta .header-market{font-size:9px}
 .hero{padding:22px 2px 18px}.hero h2{font-size:28px}
 .start-here{padding:16px 14px;margin:13px 0 5px;border-radius:14px}
 .start-here h2{font-size:19px}
 .guide-grid{grid-template-columns:1fr;gap:7px;margin-top:12px}
 .guide-card{padding:10px 12px}
 .guide-card strong{font-size:12px;margin:0 0 3px}
 .guide-card p{font-size:11px}
 .quick{grid-template-columns:repeat(5,minmax(0,1fr));margin:0}
 .lane-links{padding:11px 0 16px;gap:6px}
 .lane-links a{font-size:11px;padding:9px 11px}
 .lane-card .lane-intro{font-size:12px}
 .advanced-panel{padding:13px}
 .lane-card tbody tr td[data-label="What's missing"]{grid-column:1/-1}
 .lane-card tbody tr td[data-label="Pattern"]{grid-column:1/-1}
}
</style></head><body><div class="app-shell">
<header class="topbar">
 <div class="brand"><div class="brand-mark" aria-hidden="true"></div>
  <div><div class="kicker">Market intelligence</div><h1>Live Scanner <span class="version">V15.31</span></h1></div></div>
 <div class="header-meta"><span class="header-market">BINANCE SPOT · USDT</span><span class="last-check" id="lastUpdated">Checking feed…</span></div>
</header>
<main>
<section class="hero" aria-label="Scanner overview"><div>
 <div class="kicker">REAL-TIME MARKET OVERVIEW</div>
 <h2>Find promising coins. Understand every signal.</h2>
 <p>See coins grouped by trading setup, spot early opportunities and check which signals are actually verified. Made for beginners. This app never places trades.</p>
</div><div class="hero-stamp">LIVE MARKET MONITOR · READ ONLY</div></section>
<section class="metrics" aria-label="Live scan metrics">
 <article class="metric"><div class="metric-label">Verified BUY NOW</div><div class="metric-value" id="metricVerified">0</div><div class="metric-detail">Read-time approved entries</div></article>
 <article class="metric"><div class="metric-label">Developing setups</div><div class="metric-value" id="metricStructure">0</div><div class="metric-detail">Research setups only</div></article>
 <article class="metric"><div class="metric-label">AI verified buys</div><div class="metric-value" id="metricMl">0</div><div class="metric-detail">Independent ML approvals</div></article>
 <article class="metric"><div class="metric-label">Live data</div><div class="metric-value" id="metricFeed">Checking</div><div class="metric-detail">Freshness-aware live check</div></article>
</section>
<div class="connection-panel">
 <div id="status" role="status" aria-live="polite">Connecting to market feed…</div>
 <div id="authority" role="status">Checking independent strategy authorities…</div>
</div>
<section class="start-here" id="guide" aria-labelledby="start-title">
 <div class="kicker">YOUR QUICK GUIDE</div><h2 id="start-title">What does each signal mean?</h2>
 <div class="guide-grid">
  <div class="guide-card"><strong><i class="guide-dot" aria-hidden="true"></i>Verified BUY NOW</strong><p>A trade has passed the scanner’s latest checks. Tap <b>Verify quote</b> before any decision; prices and approvals expire.</p></div>
  <div class="guide-card arm"><strong><i class="guide-dot" aria-hidden="true"></i>Almost ready (ARMED)</strong><p>A promising pattern is forming, but still needs more confirmation. Not permission to buy.</p></div>
  <div class="guide-card watch"><strong><i class="guide-dot" aria-hidden="true"></i>Watch / Research</strong><p>Worth monitoring. Price zones and possible gains are estimates, not verified trade entries or guarantees.</p></div>
 </div><p><strong>New here?</strong> Start with <b>Verified buys</b>, then browse the setup categories below. Technical diagnostics are tucked away at the bottom.</p>
</section>
<nav class="quick" aria-label="Main dashboard sections">
 <a href="#guide">Start</a><a href="#verified">Buy now</a><a href="#beast-lane">BEAST</a><a href="#ema-lane">EMA</a><a href="#more-lanes">More</a>
</nav>
<nav class="lane-links" aria-label="More trading setup categories"><a href="#exhaustion-lane">Seller exhaustion</a><a href="#breakout-lane">Breakouts</a><a href="#pullback-lane">Pullbacks</a><a href="#other-lane">Other setups</a><a href="#ema-watch">EMA watchlist</a><a href="#research-section">Early EMA details</a><a href="#buy-structure">Full structure details</a><a href="#qualification">AI review</a><a href="#feed">Data health</a></nav>
<section class="approved panel" id="verified">
 <div class="section-header"><h2>Verified BUY NOW · <span class="inline-number" id="verifiedCount">0</span> active</h2><span class="section-tag tag-mint">Execution-grade check</span></div>
 <p>These are the <b>only</b> entries that have passed the scanner’s live checks. Other categories below are <b>watchlists, not buy instructions</b>. Always press Verify quote before considering a trade.</p>
 <div id="verifiedSummary" class="note" aria-live="polite">Checking live approvals…</div>
 <h3>EMA verified lane · <span id="emaCount">0</span></h3>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Frame</th><th>Entry</th><th>Stop</th><th>TP1</th><th>TP2</th><th>TP3</th><th>Verify</th></tr></thead><tbody id="signals"></tbody></table></div>
 <h3>Machine Learning verified lane · <span id="mlCount">0</span></h3>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Setup / duration</th><th>Entry</th><th>Stop</th><th>TP1</th><th>TP2</th><th>TP3</th><th>Verify</th></tr></thead><tbody id="mlSignals"></tbody></table></div>
 <h3>V12 Pinpoint verified lane · <span id="v12Count">0</span></h3>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Setup / frame</th><th>Entry</th><th>Stop</th><th>TP1</th><th>TP2</th><th>TP3</th><th>Verify</th></tr></thead><tbody id="v12Signals"></tbody></table></div>
</section>
<section class="panel" id="quote-check">
 <div class="section-header"><h2>On-demand quote verification</h2><span class="section-tag">Fresh validation</span></div>
 <p>Select Verify on an approved signal. Quotes expire quickly and are never orders.</p>
 <div id="quote" class="note" aria-live="polite">Select Verify on an active signal.</div>
</section>
<div id="more-lanes" aria-label="Browse different setup types">
<section class="panel lane-card beast" id="beast-lane">
 <div class="section-header"><h2>BEAST · Powerful setups <span class="lane-count" id="beastCount">0</span></h2><span class="section-tag">High-conviction patterns</span></div>
 <p class="lane-intro">Coins the scanner identifies in its BEAST strategy lane. These may have strong technical patterns but are <b>not</b> approved trades unless they appear in Verified BUY NOW.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="beastRows"></tbody></table></div>
</section>
<section class="panel lane-card ema" id="ema-lane">
 <div class="section-header"><h2>EMA · Moving-average setups <span class="lane-count" id="emaSetupCount">0</span></h2><span class="section-tag">Trend &amp; support</span></div>
 <p class="lane-intro"><b>EMA</b> is a trend-following line. These coins are near, reclaiming or reacting to an important EMA. For earlier watch candidates, open the <a href="#ema-watch">EMA watchlist</a> below.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="emaSetupRows"></tbody></table></div>
</section>
<section class="panel lane-card exhaustion" id="exhaustion-lane">
 <div class="section-header"><h2>Seller exhaustion <span class="lane-count" id="exhaustionCount">0</span></h2><span class="section-tag">Possible reversal</span></div>
 <p class="lane-intro">Selling pressure may be fading and buyers may be returning. A slowdown in selling alone does not confirm a reversal.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="exhaustionRows"></tbody></table></div>
</section>
<section class="panel lane-card breakout" id="breakout-lane">
 <div class="section-header"><h2>Breakout candidates <span class="lane-count" id="breakoutCount">0</span></h2><span class="section-tag">Resistance &amp; momentum</span></div>
 <p class="lane-intro">Coins testing or recovering important price levels. Breakouts can fail, so the scanner still checks confirmation and fresh market data.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="breakoutRows"></tbody></table></div>
</section>
<section class="panel lane-card pullback" id="pullback-lane">
 <div class="section-header"><h2>Pullback opportunities <span class="lane-count" id="pullbackCount">0</span></h2><span class="section-tag">Price returning to support</span></div>
 <p class="lane-intro">Coins that may be retracing towards a better-priced area rather than being chased after a rally. Entry zones are research references only.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="pullbackRows"></tbody></table></div>
</section>
<section class="panel lane-card" id="other-lane">
 <div class="section-header"><h2>Other chart patterns <span class="lane-count" id="otherCount">0</span></h2><span class="section-tag">Additional setups</span></div>
 <p class="lane-intro">Other structural patterns that do not clearly belong to the categories above. Nothing is silently discarded.</p>
 <div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Pattern</th><th>Entry area*</th><th>Possible upside*</th><th>Stop*</th><th>What's missing</th></tr></thead><tbody id="otherRows"></tbody></table></div>
</section>
<p class="detail-note">*Entry, stop and upside values come from the existing scanner research. They may be stale or unconfirmed and are not trade instructions.</p>
</div>
<details class="panel advanced-panel" id="feed"><summary class="advanced-toggle">Live data health <small>Advanced</small></summary>
 <div class="section-header"><h2>Live trade &amp; book delivery</h2><span class="section-tag">Data integrity</span></div>
 <p>All shortlisted symbols. A requested subscription is not an acknowledged subscription; live event ages and sequence validation determine feed readiness.</p>
 <div id="subNote" class="note">Checking worker subscriptions…</div>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Trade ACK</th><th>Book ACK</th><th>Trade age</th><th>Book age</th><th>Delivery status</th></tr></thead><tbody id="subRows"></tbody></table></div>
</details>
<details class="panel advanced-panel" id="buy-structure"><summary class="advanced-toggle">All technical setups and full detail <small>Advanced</small></summary>
 <div class="section-header"><h2>BUY STRUCTURE · <span class="inline-number" id="structureCount">0</span> setups</h2><span class="section-tag">Independent V12 lane</span></div>
 <p>BUY, ARMED and WATCH structures remain separate from executable BUY NOW approvals. Entry zones are research references; formal V12 TP1 and ML estimated targets have different sources.</p>
 <div id="structureNote" class="note" role="status">Checking formal structure…</div>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>State</th><th>Setup / frame</th><th>Entry zone</th><th>Stop</th><th>Target (source)</th><th>Potential</th><th>Evidence age</th><th>Execution blockers</th></tr></thead><tbody id="structureRows"></tbody></table></div>
</details>
<details class="panel advanced-panel" id="qualification"><summary class="advanced-toggle">AI decisions and reasons <small>Advanced</small></summary>
 <div class="section-header"><h2>Machine Learning qualification</h2><span class="section-tag">Decision audit</span></div>
 <p>NEAR BUY, DATA BLOCKED and MODEL REJECTED are diagnostics, not trade approvals.</p>
 <div id="qualNote" class="note">Checking qualification evidence…</div>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Engine</th><th>Status</th><th>Target</th><th>Primary blocker</th></tr></thead><tbody id="qualRows"></tbody></table></div>
</details>
<details class="panel advanced-panel" id="rallies"><summary class="advanced-toggle">Previous 10%+ moves <small>Advanced</small></summary>
 <div class="section-header"><h2>Observed 10%+ rallies</h2><span class="section-tag">Learning audit</span></div>
 <p>Verified monitored moves without a prior approval, not a full exchange gainer list. Tracking resets on scanner restart.</p>
 <div id="moverNote" class="note">Waiting for verified tracking observations…</div>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Observed rise</th><th>Tracking start</th><th>Earlier state</th></tr></thead><tbody id="moverRows"></tbody></table></div>
</details>
<section class="panel" id="ema-watch">
 <div class="section-header"><h2 id="research-section">EMA early watchlist</h2><span class="section-tag">Ranked research</span></div>
 <p>Highest-ranked near-EMA setups based on cached 1H/4H/daily candles, not live execution quotes. Unchanged snapshots can repeat.</p>
 <div id="researchNote" class="note" role="status">Checking source updates…</div>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Stage</th><th>Frame</th><th>EMA</th><th>Distance</th><th>Candle age</th><th>Changed</th></tr></thead><tbody id="research"></tbody></table></div>
</section>
<section class="panel" id="rotating-section">
 <div class="section-header"><h2>More EMA coins to watch (rotating)</h2><span class="section-tag">Opportunity monitor</span></div>
 <p>Up to 10 other eligible symbols rotate every 20 seconds. WATCH and near-touch states are not verified BUY signals.</p>
 <div class="table-scroll"><table><thead><tr><th>Pair</th><th>Stage</th><th>Frame</th><th>EMA</th><th>Distance</th><th>Candle age</th><th>Changed</th></tr></thead><tbody id="rotating"></tbody></table></div>
</section>
<p class="footer">LIVE SCANNER · BINANCE SPOT MONITORING · READ-ONLY SIGNAL RESEARCH</p>
</main></div>
<script>
"use strict";
const status=document.getElementById("status");
const authority=document.getElementById("authority");
const signals=document.getElementById("signals");
const mlSignals=document.getElementById("mlSignals");
const v12Signals=document.getElementById("v12Signals");
const verifiedCount=document.getElementById("verifiedCount");
const verifiedSummary=document.getElementById("verifiedSummary");
const laneGroups={EMA:{body:signals,count:document.getElementById("emaCount")},
  ML:{body:mlSignals,count:document.getElementById("mlCount")},
  V12:{body:v12Signals,count:document.getElementById("v12Count")}};
const structureRows=document.getElementById("structureRows");
const structureNote=document.getElementById("structureNote");
const structureCount=document.getElementById("structureCount");
const research=document.getElementById("research");
const rotating=document.getElementById("rotating");
const researchNote=document.getElementById("researchNote");
const subRows=document.getElementById("subRows");
const subNote=document.getElementById("subNote");
const qualRows=document.getElementById("qualRows");
const qualNote=document.getElementById("qualNote");
const moverRows=document.getElementById("moverRows");
const moverNote=document.getElementById("moverNote");
const quote=document.getElementById("quote");
const metricVerified=document.getElementById("metricVerified");
const metricStructure=document.getElementById("metricStructure");
const metricMl=document.getElementById("metricMl");
const metricFeed=document.getElementById("metricFeed");
const lastUpdated=document.getElementById("lastUpdated");

/* Display-only categories; do not modify any backend engine decision. */
const setupLanes={
 beast:{body:document.getElementById("beastRows"),count:document.getElementById("beastCount")},
 ema:{body:document.getElementById("emaSetupRows"),count:document.getElementById("emaSetupCount")},
 exhaustion:{body:document.getElementById("exhaustionRows"),count:document.getElementById("exhaustionCount")},
 breakout:{body:document.getElementById("breakoutRows"),count:document.getElementById("breakoutCount")},
 pullback:{body:document.getElementById("pullbackRows"),count:document.getElementById("pullbackCount")},
 other:{body:document.getElementById("otherRows"),count:document.getElementById("otherCount")}
};
function classifyForDisplay(q,officialLane){
 const lane=String(q.lane||officialLane||"").toUpperCase(),pattern=String(q.setup||"").toUpperCase();
 if(lane.includes("BEAST"))return "beast";
 if(/EMA|GOLDEN_CROSS|MOVING_AVERAGE|\bMA200\b|\bMA50\b/.test(pattern)||lane.includes("EMA"))return "ema";
 if(lane.includes("EXHAUST")||/EXHAUST|FAILED_BREAKDOWN|REVERSAL|SELLER/.test(pattern))return "exhaustion";
 if(lane.includes("BREAKOUT")||/BREAKOUT|SQUEEZE|COIL/.test(pattern))return "breakout";
 if(lane.includes("PULLBACK")||/PULLBACK|SUPPORT|RETEST/.test(pattern))return "pullback";
 return "other";
}
function friendlyPattern(code){return String(code||"Chart setup").replace(/_/g," ").toLowerCase().replace(/\b[a-z]/g,c=>c.toUpperCase());}
function friendlyBlocker(code){
 const labels={"ANTI_CHASE":"Price too far from entry","STALE_TRADE":"Trade updates delayed",
 "STALE_BOOK":"Order-book updates delayed","HARD_SENSOR_SAFETY":"Live-data safety check pending",
 "NEGATIVE_EXPECTED_VALUE":"Risk vs reward not good enough",
 "RISK_LEVELS_UNAVAILABLE":"Entry or stop levels missing",
 "ENTRY_STRUCTURE_INVALIDATED":"Chart setup no longer valid",
 "PROBABILITY_BELOW_DYNAMIC_FLOOR":"Model confidence is too low"};
 return labels[code]||friendlyPattern(code);
}
function resetDisplayLanes(note){
 for(const lane of Object.values(setupLanes)){clear(lane.body);lane.count.textContent="0";
  const row=lane.body.insertRow();cell(row,note||"No matching setups in this live scan").colSpan=7;}
}
function showDisplayLanes(data){
 const laneMap=new Map();
 for(const q of (data.subscription_rows||[]))if(q.symbol&&q.lane)laneMap.set(q.symbol,q.lane);
 for(const q of (data.qualification_rows||[]))if(q.symbol&&q.lane)laneMap.set(q.symbol,q.lane);
 const buckets={beast:[],ema:[],exhaustion:[],breakout:[],pullback:[],other:[]};
 for(const q of (data.buy_structure_rows||[])){
  buckets[classifyForDisplay(q,laneMap.get(q.symbol))].push(q);
 }
 for(const [key,lane] of Object.entries(setupLanes)){
  clear(lane.body);lane.count.textContent=String(buckets[key].length);
  if(!buckets[key].length){const row=lane.body.insertRow();cell(row,"No "+key+" setups currently identified").colSpan=7;continue;}
  for(const q of buckets[key]){
   const row=lane.body.insertRow();
   cell(row,q.symbol);
   const stage=cell(row,q.verified_buy_now?"Verified in Buy now":q.status==="ARMED"?"Almost ready":q.status==="WATCH"?"Watch":"Research only");
   stage.className="simple-stage"+(q.verified_buy_now?" verified-stage":"");
   cell(row,friendlyPattern(q.setup)+" · "+(q.timeframe||"—"));
   cell(row,q.entry_low==null?"Not available":money(q.entry_low)+" – "+money(q.entry_high));
   cell(row,q.potential_pct==null?"Not available":q.potential_pct+"% est.");
   cell(row,money(q.stop));
   cell(row,q.verified_buy_now?"Check verified quote above":((q.blockers||[]).slice(0,2).map(friendlyBlocker).join(" · ")||"Waiting for live confirmation"));
  }
 }
}

let aliveUntil=0, token=0, requestPending=false;
function cell(row,value){const td=document.createElement("td");
  td.textContent=value==null?"—":String(value);
  const header=row.closest("table")?.querySelectorAll("thead th")[row.cells.length];
  if(header)td.setAttribute("data-label",header.textContent.trim());
  row.appendChild(td);return td;}
function money(v){return typeof v==="number"?Number(v.toPrecision(9)).toString():"—";}
function clear(el){while(el.firstChild)el.removeChild(el.firstChild);}
function invalidate(){
  aliveUntil=0;verifiedCount.textContent="0";
  metricVerified.textContent="0";metricStructure.textContent="0";metricMl.textContent="0";metricFeed.textContent="Offline";
  clear(structureRows);structureCount.textContent="0";
  resetDisplayLanes("Live data not confirmed yet");
  structureNote.textContent="Structure evidence unavailable or expired";
  verifiedSummary.textContent="No currently verified signal; previous approvals are expired.";
  for(const group of Object.values(laneGroups)){
    clear(group.body);group.count.textContent="0";
    const row=group.body.insertRow();cell(row,"No active verified BUY NOW signal").colSpan=8;
  }
}
async function verify(symbol){
  const started=performance.now();
  quote.textContent="Revalidating "+symbol+" against live trade/book evidence…";
  try{
    const r=await fetch("/signals/quote?symbol="+encodeURIComponent(symbol),
      {cache:"no-store",signal:AbortSignal.timeout(2500)});
    if(!r.ok)throw Error("HTTP "+r.status);
    const d=await r.json();
    const lease=Math.max(0,Math.min(...(d.quotes||[]).map(x=>x.quote_expires_ms))
      - d.server_time_ms - (performance.now()-started) - 150);
    if(d.status!=="VERIFIED_AT_READ"||!d.quotes.length||lease<=0){
      quote.textContent="NO EXECUTABLE QUOTE: "+d.status+". Wait for a new live confirmation.";
      return;
    }
    const q=d.quotes[0];
    quote.textContent=symbol+" live-verified at "+new Date(d.server_time_ms).toLocaleTimeString()
      +" | "+(q.authority||"EMA")+" | Entry "+money(q.entry)+" | Stop "+money(q.stop)+" | TP1 "+money(q.tp1)
      +" | TP2 "+money(q.tp2)+" | TP3 "+money(q.tp3)
      +" | Quote lease remaining "+Math.floor(lease)+"ms"
      +" | Recheck the exchange before submitting. No order has been placed.";
    setTimeout(()=>{quote.textContent="QUOTE EXPIRED: verify again before considering any order.";},lease);
  }catch(e){quote.textContent="QUOTE UNAVAILABLE: "+String(e.message)+"; no order permission.";}
}
async function refresh(){
  if(requestPending)return;
  requestPending=true;
  const id=++token, started=performance.now();
  let responseOk=false;
  try{
    const response=await fetch("/signals/live?nocache="+Date.now(),
      {cache:"no-store",signal:AbortSignal.timeout(2600)});
    if(!response.ok)throw Error("HTTP "+response.status);
    const d=await response.json();
    responseOk=true;
    if(id!==token)return;
    aliveUntil=performance.now()+Math.max(0,
      (d.expires_ms||0)-(d.server_time_ms||0)-(performance.now()-started)-150);
    const valid=d.fresh&&performance.now()<aliveUntil;
    status.className=valid?"good":"bad";
    metricFeed.textContent=valid?"Live":"Stale";
    lastUpdated.textContent=valid?"Checked "+new Date(d.server_time_ms).toLocaleTimeString():"Data stale";
    status.textContent=(valid?"LIVE: ":"STALE: ")+d.status
      +" | verified buys "+(valid?d.buy_count:0)
      +" | scan age "+d.snapshot_age_ms+"ms | cycles "+d.cycles
      +" | checked "+new Date(d.server_time_ms).toLocaleTimeString();
    const ad=d.authority_diagnostics||{};
    authority.textContent=valid
      ?"Independent engines: "+(ad.ml_ranked||0)+" ML ranked, "
        +(ad.ml_approved||0)+" ML approved, "+(ad.v12_approved||0)
        +" V12 approved; "+(d.foreign_rejected_at_gate||0)
        +" rejected at fresh trade/book check"
      :"Authority status unavailable — market verification is stale";
    let visible=0;
    for(const [engine,group] of Object.entries(laneGroups)){
      clear(group.body);
      const lane=(d.verified_lanes||{})[engine];
      const rows=valid&&Array.isArray(lane)?lane:[];
      group.count.textContent=String(rows.length);
      visible+=rows.length;
      if(!rows.length){
        const empty=group.body.insertRow();
        cell(empty,valid?"No verified BUY NOW":"Verification expired").colSpan=8;
      }
      for(const q of rows){
        const r=group.body.insertRow();
        cell(r,q.symbol);
        cell(r,(q.lane||engine)+" / "+(q.timeframe||"-")
          +(q.ema_period?" EMA"+q.ema_period:"")
          +(q.expected_time_to_target?" / "+q.expected_time_to_target:""));
        cell(r,money(q.entry));cell(r,money(q.stop));
        cell(r,money(q.tp1));cell(r,money(q.tp2));cell(r,money(q.tp3));
        const td=r.insertCell(),b=document.createElement("button");
        td.setAttribute("data-label","Verify");
        b.textContent="Verify quote";b.onclick=()=>verify(q.symbol);td.appendChild(b);
      }
    }
    verifiedCount.textContent=String(visible);
    metricVerified.textContent=String(visible);
    metricMl.textContent=String(valid?Number(laneGroups.ML.count.textContent):0);
    verifiedSummary.textContent=valid?
      visible+" read-time verified signals across EMA, ML and V12 lanes":
      "No current live quote; verification has expired";
    clear(subRows);clear(qualRows);clear(moverRows);clear(structureRows);
    structureCount.textContent="0";
    structureNote.textContent="Structure evidence unavailable";
    resetDisplayLanes(valid?"No matching setups in this scan":"Live data is stale");
    if(valid){
      showDisplayLanes(d);
      const bs=d.buy_structure_summary||{};
      const structural=d.buy_structure_rows||[];
      structureCount.textContent=String(structural.length);
      metricStructure.textContent=String(structural.length);
      structureNote.textContent=(bs.structural_buy||0)+" structural BUY | "
        +(bs.armed||0)+" ARMED | "+(bs.watch||0)+" WATCH | "
        +"Only separately verified V12 signals qualify for BUY NOW.";
      if(!structural.length){
        const row=structureRows.insertRow();
        cell(row,"No fresh V12 structural setups").colSpan=9;
      }
      for(const q of structural){
        const row=structureRows.insertRow();
        cell(row,q.symbol);
        const stateCell=cell(row,q.status+(q.verified_buy_now?" ✓":" — NOT EXECUTION APPROVED"));
        stateCell.className=q.status==="BUY"?"state-buy":q.status==="ARMED"?"state-armed":"state-watch";
        cell(row,q.setup+" / "+q.timeframe);
        cell(row,q.entry_low==null?"—":money(q.entry_low)+"–"+money(q.entry_high));
        cell(row,money(q.stop));
        cell(row,money(q.tp1)+" / "+(q.target_source==="V12_TP1"?"V12 TP1":"ML estimate"));
        cell(row,q.potential_pct==null?"—":q.potential_pct+"%");
        cell(row,q.source_age_ms==null?"—":q.source_age_ms+"ms");
        cell(row,q.verified_buy_now?"NONE":
          ((q.blockers||[]).slice(0,2).join(", ")||"EXECUTION NOT APPROVED"));
      }
      const summary=d.subscription_summary||{};
      subNote.textContent=(summary.control_requested||0)+"/"+(summary.shortlisted||0)
        +" control requested | "+(summary.both_acknowledged||0)
        +" both streams acknowledged | "+(summary.event_aligned||0)
        +" within 1.2s + valid sequences";
      for(const q of (d.subscription_rows||[])){
        const row=subRows.insertRow();
        cell(row,q.symbol);
        const tradeCell=cell(row,q.trade_ack?"YES":"NO");tradeCell.className=q.trade_ack?"good":"bad";
        const bookCell=cell(row,q.book_ack?"YES":"NO");bookCell.className=q.book_ack?"good":"bad";
        cell(row,q.trade_age_ms==null?"—":q.trade_age_ms+"ms");
        cell(row,q.book_age_ms==null?"—":q.book_age_ms+"ms");
        cell(row,q.status);
      }
      const counts=d.qualification_counts||{};
      const structure=d.structure_summary||{};
      qualNote.textContent="Formal V12 setups shown: "+(structure.shown||0)
        +" (BUY structure "+(structure.formal_buy_shown||0)+") | "
        +(Object.entries(counts).map(([k,v])=>k+": "+v).join(" | ")
        ||"No current ML decisions");
      for(const q of (d.qualification_rows||[])){
        const r=qualRows.insertRow();
        cell(r,q.symbol);cell(r,q.lane);const decisionCell=cell(r,q.state);
        decisionCell.className=q.state==="BUY"?"state-buy":q.state==="ARMED"?"state-armed":"";
        cell(r,(q.target_pct==null?"—":q.target_pct+"%"));
        const b=(q.blockers||[]).slice(0,2).join(", ");
        cell(r,(q.setup_not_confirmed?"SETUP NOT CONFIRMED; ":"FORMAL "+(q.structural_state||"STRUCTURE")+"; ")+(b||"—"));
      }
      const events=(d.missed_rallies||[]).filter(e=>!e.had_approved_buy);
      moverNote.textContent=events.length
        +" tracked 10%+ observed moves without an approved BUY";
      for(const m of events){
        const r=moverRows.insertRow();
        cell(r,m.symbol);cell(r,"+"+m.observed_gain_pct+"%");
        cell(r,new Date(m.start_ms).toLocaleTimeString());
        cell(r,m.prior_status);
      }
    }else{
      qualNote.textContent="Evidence unavailable";
      moverNote.textContent="Tracking unavailable";
    }
    clear(research);clear(rotating);
    if(valid){
      const count=d.research_eligible_count||0;
      researchNote.textContent=count+" eligible pairs within 1% of an EMA"
        +" | "+(d.research_source_changes||0)+" readings changed since last tick"
        +" | "+(d.research_alternative_count||0)+" additional candidates";
      const render=(body,items)=>{
        if(!items||!items.length){
          const row=body.insertRow();
          cell(row,"No additional candidates currently qualify").colSpan=7;
          return;
        }
        for(const q of items){
          const row=body.insertRow();
          cell(row,q.symbol);cell(row,q.status);
          cell(row,q.timeframe);cell(row,"EMA"+q.ema_period);
          cell(row,q.distance_pct+"%");
          cell(row,q.source_age_s==null?"unverified":q.source_age_s+"s");
          cell(row,q.change);
        }
      };
      render(research,d.research_rows||[]);
      render(rotating,d.rotating_research_rows||[]);
    }else{
      researchNote.textContent="Candle evidence unavailable — no current research results";
    }
  }catch(e){status.className="bad";metricFeed.textContent="Offline";lastUpdated.textContent="Disconnected";status.textContent=(responseOk?"DASHBOARD DISPLAY ERROR: ":"CONNECTION UNAVAILABLE: ")+e.message;
    invalidate();clear(research);clear(rotating);clear(subRows);clear(qualRows);clear(moverRows);
    subNote.textContent="Disconnected";qualNote.textContent="Disconnected";moverNote.textContent="Disconnected";
    researchNote.textContent="Connection lost — no fresh research";}
  finally{requestPending=false;setTimeout(refresh,850);}
}
setInterval(()=>{if(aliveUntil&&performance.now()>=aliveUntil){
  status.className="bad";metricFeed.textContent="Stale";lastUpdated.textContent="Expired";status.textContent="EXPIRED — refreshing; no active quote";
  invalidate();}},150);
document.addEventListener("visibilitychange",()=>{if(!document.hidden)refresh();});
refresh();
</script></body></html>"""


async def http_dashboard(request):
    return web.Response(
        text=_DASHBOARD, content_type="text/html",
        headers={"Cache-Control": "no-store, max-age=0",
                 "X-Content-Type-Options": "nosniff",
                 "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'"},
    )


async def http_events(request):
    """SSE: immediate new BUY alerts; clients must revalidate /signals/live."""
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream",
                 "Cache-Control": "no-store, no-transform",
                 "Connection": "keep-alive",
                 "X-Accel-Buffering": "no"}
    )
    await response.prepare(request)
    last_id = 0
    try:
        last_id = max(0, int(request.headers.get("Last-Event-ID", "0")))
    except ValueError:
        pass
    try:
        initial = json.dumps(read_live(), separators=(",", ":"), allow_nan=False)
        await response.write(f"event: snapshot\ndata: {initial}\n\n".encode())
        until = time.monotonic() + 110
        while time.monotonic() < until:
            now_ms = _ms()
            with _LOCK:
                new = [dict(e) for e in _EVENTS if e["id"] > last_id and now_ms < e["expires_ms"]]
            if new:
                # Do not stream events that lost their live verification.
                live = read_live()
                live_keys = {(x["symbol"], x.get("authority","EMA"),
                              x.get("lane","EMA"),x.get("ema_period",0))
                             for x in live["buy_signals"]}
                for event in new:
                    last_id = max(last_id, event["id"])
                    if (event["symbol"],event.get("authority","EMA"),
                            event.get("lane","EMA"),event.get("ema_period",0)) not in live_keys:
                        continue
                    raw = json.dumps(event, separators=(",", ":"), allow_nan=False)
                    await response.write(f"id: {event['id']}\nevent: buy\ndata: {raw}\n\n".encode())
            else:
                await response.write(b": heartbeat\n\n")
            await asyncio.sleep(1.0)
    except (ConnectionError, ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        try:
            await response.write_eof()
        except (ConnectionError, ConnectionResetError, RuntimeError):
            pass
    return response


def signal_tick_line(snapshot, live, cycle_ms=0):
    """Timestamped fast-lane status for environments that cannot GET HTTP URLs.

    Railway log delivery is a transport fallback, not a claim that the signal
    will remain valid when a reader receives the log several seconds later.
    """
    if live.get("buy_count"):
        verified = [{
            "symbol": row.get("symbol"),"authority":row.get("authority","EMA"),
            "lane":row.get("lane","EMA"),
            "timeframe": row.get("timeframe"),
            "ema_period": row.get("ema_period"),
            "entry": row.get("entry"), "stop": row.get("stop"),
            "tp1": row.get("tp1"), "tp2": row.get("tp2"), "tp3": row.get("tp3")
        } for row in live.get("buy_signals") or []]
    else:
        verified = []
    report = {
        "generated_ms": live.get("generated_ms"),
        "verified_at_ms": live.get("server_time_ms"),
        "snapshot_age_ms": live.get("snapshot_age_ms"),
        "expires_ms": live.get("expires_ms"),
        "status_at_generation": live.get("status"),
        "buy_count": live.get("buy_count", 0),
        "buys": verified,
        "candle_frames": snapshot.get("candle_frames", 0),
        "interactions": snapshot.get("interactions", 0),
        "live_evidence_checked": live.get("live_evidence_checked", 0),
        "authority_diagnostics": live.get("authority_diagnostics", {}),
        "foreign_rejected_at_gate": live.get("foreign_rejected_at_gate", 0),
        "ml_priority_symbols": live.get("ml_priority_symbols", []),
        "qualification_counts": live.get("qualification_counts", {}),
        "structure_summary": live.get("structure_summary", {}),
        "buy_structure_summary": live.get("buy_structure_summary", {}),
        "buy_structure_rows": live.get("buy_structure_rows", [])[:20],
        "subscription_summary": live.get("subscription_summary", {}),
        "subscription_rows": live.get("subscription_rows", [])[:30],
        "missed_rally_count": live.get("missed_rally_count", 0),
        "technical_ready": live.get("technical_ready_symbols", []),
        "research": live.get("research_top10", []),
        "research_rows": live.get("research_rows", [])[:10],
        "rotating_research": [r["symbol"] for r in live.get("rotating_research_rows", [])],
        "research_eligible_count": live.get("research_eligible_count", 0),
        "research_source_changes": live.get("research_source_changes", 0),
        "cycle_ms": cycle_ms,
        "worker_errors": live.get("errors", 0),
        "read_only": True,
    }
    return "PSI-V15.30 SIGNAL_TICK " + json.dumps(
        report, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


async def supervisor_loop():
    last_log = 0.0
    while True:
        try:
            start = time.monotonic()
            snap = await asyncio.to_thread(publish_once)
            completed = time.monotonic()
            if completed - last_log >= LOG_INTERVAL_SECONDS or snap.get("buy_signals") or snap.get("foreign_buy_signals"):
                # Re-validate right before emitting; no BUY may survive lost
                # book/trade evidence merely because a prior tick approved it.
                live = await asyncio.to_thread(read_live)
                print(signal_tick_line(snap, live, int(
                    (time.monotonic() - start) * 1000
                )), flush=True)
                last_log = completed
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            with _LOCK:
                _STATUS["errors"] += 1
                _STATUS["last_error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
            print(f"PSI-V15.29 LIVE_FEED_ERROR {type(exc).__name__}: {exc}", flush=True)
        await asyncio.sleep(INTERVAL_SECONDS)


def install(core, ema, ml=None):
    global CORE, EMA, ML
    CORE, EMA, ML = core, ema, ml
    core.app.fast_signal_handler = http_live
    core.app.fast_events_handler = http_events
    core.app.fast_quote_handler = http_quote
    core.app.fast_dashboard_handler = http_dashboard
    print("PSI-V15.29 LIVE_SIGNAL_DELIVERY timed decision evidence, watch dwell and rally audit", flush=True)
