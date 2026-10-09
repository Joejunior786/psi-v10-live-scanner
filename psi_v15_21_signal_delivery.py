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
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="theme-color" content="#0e1620">
<title>Live Scanner</title>
<style>
:root{color-scheme:dark;font-family:system-ui,sans-serif}
body{background:#0e1620;color:#e9f1f5;margin:auto;max-width:1100px;padding:22px}
h1{font-size:1.5rem}h2{font-size:1.05rem;margin-top:26px}
p{color:#b7c6d0}table{width:100%;border-collapse:collapse;font-size:0.9rem}
td,th{border-bottom:1px solid #293746;text-align:left;padding:9px 7px}
button{cursor:pointer;padding:8px 12px;background:#183c54;border:1px solid #6b97b0;
color:#fff;border-radius:6px}button:disabled{opacity:.4;cursor:default}
.good{color:#75e5bb}.warn{color:#ffcd77}.bad{color:#ff9696}
code{word-break:break-word}#status,#quote{padding:12px;background:#182635;border-radius:7px}
.quick{display:flex;flex-wrap:wrap;gap:9px;margin:18px 0}
.quick a{padding:8px 11px;background:#193346;border:1px solid #34566d;border-radius:7px;color:#cfe9f5;text-decoration:none}
.approved{border:1px solid #3a8672;padding:16px;border-radius:12px;margin-top:18px;background:#12252a}
.approved h2{margin:0 0 8px}.approved h3{margin:22px 0 9px;font-size:1rem}
.approved h3 span{font-variant-numeric:tabular-nums;color:#9ee2bc}
.table-scroll{overflow-x:auto}
.approved table{min-width:710px}.approved tbody tr{background:#193039}
#verifiedSummary{margin:10px 0;padding:8px 0}
@media(max-width:650px){body{padding:14px}.approved{padding:12px}}


/* Historical pre-mobile-redesign layout retained. Readability helpers only. */
*{box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:18px}
body{min-width:280px;line-height:1.5;-webkit-text-size-adjust:100%}
h1{margin:0 0 8px}h2{font-weight:700}
h3{margin-top:20px}
p{font-size:0.9rem;line-height:1.55}
#authority{padding:10px 12px;color:#b3cad8}
#status{margin-top:16px}
.summary-line{padding:10px 0;display:flex;gap:7px 22px;flex-wrap:wrap;color:#bbcbd5;font-size:0.85rem;border-bottom:1px solid #293746}
.summary-line strong{color:#e7f4fa;font-variant-numeric:tabular-nums}
.legend{border-left:3px solid #4b8794;padding:9px 12px;background:#152430;color:#c8d8e2;margin:14px 0;font-size:0.87rem}
.legend span{white-space:nowrap}
.legend .ready{color:#75e5bb;font-weight:700}.legend .pending{color:#ffcd77;font-weight:700}.legend .watch{color:#e8d585;font-weight:700}
.quick{margin:18px 0 20px}
.quick a{font-size:0.87rem;font-weight:650}
.quick a:hover,.quick a:focus-visible{background:#23465d;border-color:#729ab0}
section.strategy-lane{margin:0 0 22px;padding:0 0 12px;border-bottom:1px solid #293746}
.strategy-lane h2{margin-top:22px}
.strategy-lane table{min-width:700px}
.strategy-lane td:first-child{font-weight:700;color:#eaf7f9}
.strategy-lane td{font-size:.82rem}
.lane-context{color:#91bac9}
.note{background:#182635;padding:10px 12px;border-radius:7px;color:#c4d4de;font-size:.85rem;margin:10px 0}
.text-mint,.state-buy{color:#75e5bb}.state-armed{color:#ffcd77}.state-watch{color:#e8d585}
.state-buy,.state-armed,.state-watch{font-weight:700}
#structureRows td,#qualRows td{font-size:.82rem}
#subNote,#qualNote,#moverNote,#structureNote,#researchNote{margin:10px 0;color:#b7cbd8}
#verifiedSummary{font-weight:650}
table{font-variant-numeric:tabular-nums} th{color:#b9cbd5;white-space:nowrap}
.table-scroll{overflow-x:auto;-webkit-overflow-scrolling:touch;max-width:100%}
.table-scroll table{min-width:650px}
button{min-height:36px}
:focus-visible{outline:2px solid #9cd4e7;outline-offset:2px}
.footer{font-size:.75rem;color:#91aabd;padding:22px 0 14px}
@media(max-width:650px){
 body{padding:14px}
 h1{font-size:1.4rem}
 p{font-size:.85rem}
 .summary-line{gap:6px 15px}
 .quick{gap:6px}
 .quick a{padding:7px 9px;font-size:.82rem}
 .approved{padding:12px}
 .table-scroll table{min-width:675px}
 .strategy-lane table{min-width:730px}
}

/* iPhone display fixes only. Original desktop dashboard and scanner remain unchanged. */
@media(max-width:760px){
 html{scroll-padding-top:79px}
 body{
  width:100%;max-width:100%;margin:0;
  padding:14px max(12px,env(safe-area-inset-right)) calc(24px + env(safe-area-inset-bottom)) max(12px,env(safe-area-inset-left));
  font-size:15px;overflow-x:hidden
 }
 h1{font-size:1.45rem;line-height:1.25}
 h2{font-size:1.08rem;line-height:1.4;margin:24px 0 10px}
 h3{font-size:.98rem;line-height:1.4}
 p{font-size:.9rem;line-height:1.65;overflow-wrap:anywhere}
 #status,#authority,#quote,.note,#subNote,#structureNote,#qualNote,#researchNote,#moverNote{
  overflow-wrap:anywhere;word-break:normal;font-size:.86rem;line-height:1.6
 }
 .summary-line{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px 10px}
 .summary-line span{min-width:0}
 .summary-line #lastUpdated{grid-column:1/-1;color:#a7c3d0;font-size:.82rem}
 .legend{font-size:.85rem;line-height:1.8}
 .legend span{white-space:normal}
 .quick{
  position:sticky;z-index:10;top:0;
  display:flex;flex-wrap:nowrap;gap:8px;
  margin:16px -12px 18px;padding:10px 12px 11px;
  overflow-x:auto;overflow-y:hidden;
  background:#0e1620;border-top:1px solid #273a4b;border-bottom:1px solid #273a4b;
  -webkit-overflow-scrolling:touch;overscroll-behavior-x:contain;scrollbar-width:thin
 }
 .quick a{
  display:inline-flex;flex:none;align-items:center;justify-content:center;
  padding:10px 14px;min-height:44px;
  border-radius:7px;font-size:.89rem;line-height:1.25
 }
 .approved{padding:13px}
 .approved table,.strategy-lane table,.table-scroll table{min-width:0!important}
 .table-scroll{overflow:visible;max-width:100%}
 .table-scroll table,table{display:block;width:100%;max-width:100%;min-width:0!important}
 thead{display:none}
 tbody{display:block;width:100%;max-width:100%}
 tbody tr{
  display:grid;grid-template-columns:repeat(2,minmax(0,1fr));
  gap:8px 12px;width:100%;min-width:0;
  margin:0 0 9px;padding:12px;
  background:#152330;border:1px solid #2c4051;border-radius:7px
 }
 .approved tbody tr{background:#163035;border-color:#346057}
 tbody td{
  display:flex;flex-direction:column;justify-content:center;align-items:flex-start;
  min-width:0;width:auto;max-width:100%;padding:2px 0;
  margin:0;border:0!important;line-height:1.4;
  font-size:.9rem!important;overflow-wrap:anywhere;white-space:normal!important
 }
 tbody td::before{
  display:block;content:attr(data-label);text-transform:uppercase;
  color:#a4bccb;font-weight:700;font-size:.65rem;letter-spacing:.045em;
  line-height:1.3;overflow-wrap:anywhere
 }
 tbody td:first-child{
  grid-column:1/-1;display:flex;padding:0 0 8px!important;
  border-bottom:1px solid #2c4152!important;
  color:#eaf7f9;font-weight:750;font-size:1.05rem!important
 }
 tbody td:first-child::before{content:"Coin";font-size:.65rem}
 tbody td[colspan]{grid-column:1/-1;font-size:.85rem!important;border:0!important;padding:2px!important}
 tbody td[colspan]::before{display:none}
 tbody td[data-label="Verify"],tbody td[data-label="Check"],tbody td[data-label="What is missing"],
 tbody td[data-label="What's missing"],tbody td[data-label="Why it cannot be bought yet"],
 tbody td[data-label="Primary blocker"],tbody td[data-label="Current status"]{
  grid-column:1/-1
 }
 tbody td button{width:100%;min-height:44px;text-align:center;font-size:.9rem;white-space:normal}
 .strategy-lane{margin-bottom:20px}
 .footer{text-align:center}
}
@media(max-width:350px){
 body{padding-left:10px;padding-right:10px}
 tbody tr{gap:8px}
 tbody td{font-size:.84rem!important}
 .summary-line{gap:7px}
 .quick{margin-left:-10px;margin-right:-10px;padding-left:10px}
}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}
</style>
</head><body>
<h1>Live Scanner · V15.31</h1>
<p>Simple crypto market scanner for Binance Spot. Browse coins by setup, see possible entry areas and understand what is still missing. <b>This page does not place trades.</b></p>
<div id="status" role="status" aria-live="polite">Connecting to current market data…</div>
<div id="authority" role="status">Checking signal approvals…</div>
<div class="summary-line" aria-label="Market overview">
<span>Verified buys: <strong id="metricVerified">0</strong></span>
<span>Chart setups: <strong id="metricStructure">0</strong></span>
<span>AI-approved buys: <strong id="metricMl">0</strong></span>
<span>Data: <strong id="metricFeed">Checking</strong></span>
<span id="lastUpdated">Connecting…</span>
</div>
<div class="legend"><b>Quick guide:</b>
<span class="ready">Green = verified BUY NOW</span> ·
<span class="pending">Orange = ARMED (almost ready)</span> ·
<span class="watch">Yellow = WATCH (not ready)</span>.
A chart pattern labelled BUY is <b>not</b> a verified entry. You must check the live quote first.
</div>
<nav class="quick" aria-label="Scanner sections">
<a href="#verified">Verified BUY NOW</a>
<a href="#beast-lane">BEAST</a>
<a href="#ema-lane">EMA</a>
<a href="#exhaustion-lane">Seller exhaustion</a>
<a href="#breakout-lane">Breakouts</a>
<a href="#pullback-lane">Pullbacks</a>
<a href="#other-lane">Other setups</a>
<a href="#buy-structure">All technical setups</a>
<a href="#qualification">AI review</a>
<a href="#research-section">EMA watchlist</a>
<a href="#feed">Data connection</a>
</nav>
<section class="approved" id="verified">
<h2>VERIFIED BUY NOW · <span id="verifiedCount">0</span> active</h2>
<p>Only signals approved by the scanner's live checks appear here. <b>Never confuse research setups with verified BUY NOW.</b> Tap <b>Verify quote</b> for a fresh check before considering a trade.</p>
<div id="verifiedSummary" class="good" aria-live="polite">Checking verified entries…</div>
<h3>EMA verified buys · <span id="emaCount">0</span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Timeframe</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check</th></tr></thead><tbody id="signals"></tbody></table></div>
<h3>AI (Machine Learning) verified buys · <span id="mlCount">0</span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Setup / hold time</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check</th></tr></thead><tbody id="mlSignals"></tbody></table></div>
<h3>Technical analysis verified buys · <span id="v12Count">0</span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Pattern / timeframe</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check</th></tr></thead><tbody id="v12Signals"></tbody></table></div>
</section>
<h2>Verify a current entry</h2>
<p>The market moves fast. Tap <b>Verify quote</b> above to check whether an approved entry is still current. It never places an order.</p>
<div id="quote" aria-live="polite">Select Verify on an approved buy signal.</div>
<h2>Trading opportunities by setup (research)</h2>
<p>Every lane below shows patterns the scanner has already found. A suggested entry or target is an <b>estimate, not a guaranteed result or a live BUY NOW approval.</b> A setup can appear here while still waiting for evidence.</p>
<section class="strategy-lane" id="beast-lane">
<h2>BEAST candidates · <span id="beastCount">0</span></h2>
<p>Strong patterns found by the scanner's BEAST strategy. These are not live BUY orders. <span class="lane-context">(Potential fast moves)</span></p>
<div class="table-scroll"><table aria-label="BEAST candidates"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="beastRows"></tbody></table></div></section>
<section class="strategy-lane" id="ema-lane">
<h2>EMA candidates · <span id="emaSetupCount">0</span></h2>
<p>Coins testing or recovering a moving-average line. EMA is a price trend line that can act as support. <span class="lane-context">(Trend / EMA support)</span></p>
<div class="table-scroll"><table aria-label="EMA candidates"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="emaSetupRows"></tbody></table></div></section>
<section class="strategy-lane" id="exhaustion-lane">
<h2>Seller exhaustion · <span id="exhaustionCount">0</span></h2>
<p>Coins showing signs that heavy selling may be slowing. Buyers still need to confirm a recovery. <span class="lane-context">(Selling may be fading)</span></p>
<div class="table-scroll"><table aria-label="Seller exhaustion"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="exhaustionRows"></tbody></table></div></section>
<section class="strategy-lane" id="breakout-lane">
<h2>Breakout candidates · <span id="breakoutCount">0</span></h2>
<p>Coins approaching or retesting levels where price might start a stronger rise. False breakouts remain possible. <span class="lane-context">(Potential breakout)</span></p>
<div class="table-scroll"><table aria-label="Breakout candidates"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="breakoutRows"></tbody></table></div></section>
<section class="strategy-lane" id="pullback-lane">
<h2>Pullback candidates · <span id="pullbackCount">0</span></h2>
<p>Coins pulling back towards a potentially better entry area, rather than chasing a rapid rise. <span class="lane-context">(Potential support entry)</span></p>
<div class="table-scroll"><table aria-label="Pullback candidates"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="pullbackRows"></tbody></table></div></section>
<section class="strategy-lane" id="other-lane">
<h2>Other setups · <span id="otherCount">0</span></h2>
<p>Other useful patterns already identified by the scanner that do not fit the categories above. <span class="lane-context">(Additional research)</span></p>
<div class="table-scroll"><table aria-label="Other setups"><thead><tr>
<th>Coin</th><th>Stage</th><th>Pattern / timeframe</th><th>Entry zone*</th><th>Estimated upside*</th><th>Stop*</th><th>What is missing</th>
</tr></thead><tbody id="otherRows"></tbody></table></div></section>
<p class="note">*Entry zone = possible buying price range. Stop = possible loss limit. Estimated upside = potential price rise towards the scanner's target; it is not a prediction or guaranteed profit.</p>
<h2 id="buy-structure">All chart setups · <span id="structureCount">0</span></h2>
<p>Full technical list in one place. A green-looking chart setup is not a verified BUY NOW unless it appears in the approved section at the top.</p>
<div id="structureNote" class="warn" role="status">Checking chart patterns…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Chart status</th><th>Pattern / timeframe</th><th>Entry zone</th><th>Stop loss</th><th>Target (source)</th><th>Potential gain</th><th>Data age</th><th>Why it cannot be bought yet</th></tr></thead><tbody id="structureRows"></tbody></table></div>
<h2 id="qualification">AI decisions · why coins are being held back</h2>
<p>These are evaluation results, not trade approvals. <b>Data blocked</b> means market evidence is missing or late. <b>Model rejected</b> means the machine-learning model did not approve the setup.</p>
<div id="qualNote" class="warn">Checking AI decisions…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Strategy</th><th>AI status</th><th>Estimated target</th><th>What's missing</th></tr></thead><tbody id="qualRows"></tbody></table></div>
<h2 id="research-section">Early EMA candidates · watchlist</h2>
<p>Coins close to an EMA line, including coins that are not yet ready. These prices use recent chart candles and are not instant execution quotes. Repeated names can be normal if the market has not changed.</p>
<div id="researchNote" class="warn" role="status">Checking watchlist…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Timeframe</th><th>EMA line</th><th>Distance</th><th>Candle age</th><th>Updated?</th></tr></thead><tbody id="research"></tbody></table></div>
<h2>More EMA watchlist candidates (rotating)</h2>
<p>Additional coins close to important EMA lines. WATCH means a possible setup to monitor, not an approval to buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Stage</th><th>Timeframe</th><th>EMA line</th><th>Distance</th><th>Candle age</th><th>Updated?</th></tr></thead><tbody id="rotating"></tbody></table></div>
<h2 id="feed">Live market connection · detailed information</h2>
<p><b>Trades connected</b> and <b>Order book connected</b> show whether live Binance information is being received. Old event ages can stop an entry from being verified.</p>
<div id="subNote" class="warn">Checking subscriptions…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Trades connected?</th><th>Order book connected?</th><th>Trade update age</th><th>Book update age</th><th>Current status</th></tr></thead><tbody id="subRows"></tbody></table></div>
<h2>Previous 10%+ rises (tracked by scanner)</h2>
<p>Previously observed rising coins during monitoring, not a complete list of top gainers. This is historical research, not a current buy signal.</p>
<div id="moverNote" class="warn">Checking recorded moves…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Observed rise</th><th>Tracking started</th><th>Earlier state</th></tr></thead><tbody id="moverRows"></tbody></table></div>
<p class="footer">Live Scanner · Binance Spot / USDT · Market research only · No automatic orders</p>
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
