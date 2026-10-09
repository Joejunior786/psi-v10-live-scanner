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
from psi_v15_32_continuity import OpportunityJournal

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
_OPPORTUNITIES = OpportunityJournal(os.environ.get(
    "PSI_OPPORTUNITY_STATE_PATH", "/data/psi_v15_32_opportunities.json"))


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
    try:
        structural_rows = list(CORE._board() or []) if CORE is not None else []
    except (AttributeError, TypeError, ValueError):
        structural_rows = []
    structural = sorted(structural_rows,
        key=lambda r: (r.get("execution_state") == "BUY NOW",
            r.get("state") == "BUY",r.get("state") == "ARMED",
            EMA.num(r.get("setup_strength"))),reverse=True)
    for row in structural[:6]:
        add(row.get("symbol"))
    # Guarantee the entire current ML top 30 a place after six
    # execution-structure priorities. Do not allow older ranked research
    # rows to evict today's shortlist.
    for row in ml_rows[:30]:
        add(row.get("symbol"))
        if len(wanted) >= MICRO_PRIORITY_SLOTS:
            break
    if len(wanted) < MICRO_PRIORITY_SLOTS:
        for row in ml_rows[30:]:
            add(row.get("symbol"))
            if len(wanted) >= MICRO_PRIORITY_SLOTS:
                break
    return approved, inspected, wanted



def _stable_market_priorities(candidates, at):
    """Reserve all current execution candidates BEFORE old research leases.

    Historical leases may consume only the remaining slots. A 3-minute
    research hold must never evict a newly ranked top-30 coin from worker
    subscriptions. Executable status is NOT conferred by this scheduling.
    """
    for symbol, expires in list(_PRIORITY_LEASES.items()):
        if expires <= at:
            del _PRIORITY_LEASES[symbol]
    current=[]
    for value in candidates or []:
        sym=str(value or "").upper()
        if (re.fullmatch(r"[A-Z0-9]{2,24}USDT",sym) and sym not in current):
            current.append(sym)
        if len(current)>=MICRO_PRIORITY_SLOTS:
            break
    if len(current) < 30:
        # Short watchlists keep the established 3-minute retention behavior;
        # only a FULL execution shortlist can preempt research leases.
        for sym in current:
            if sym not in _PRIORITY_LEASES:
                _PRIORITY_LEASES[sym]=at+PRIORITY_HOLD_MS
        return list(_PRIORITY_LEASES)[:MICRO_PRIORITY_SLOTS]
    for sym in current:
        _PRIORITY_LEASES[sym]=at+PRIORITY_HOLD_MS
    overflow=[sym for sym in _PRIORITY_LEASES if sym not in current]
    out=(current+overflow)[:MICRO_PRIORITY_SLOTS]
    # Drop old leases that cannot fit; avoid starving later top-rank arrivals.
    _PRIORITY_LEASES.clear()
    for sym in out:
        _PRIORITY_LEASES[sym]=at+PRIORITY_HOLD_MS
    return out


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
        "quote_ready":(status_counts.get("TRADE_BOOK_ALIGNED",0)+
                       status_counts.get("QUIET_TRADE_LIVE_BOOK",0)),
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
        # Journal only proven source observations; it cannot grant BUY approval.
        _OPPORTUNITIES.update(now_ms, buy_structure_rows, eligible)
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
            "opportunity_tracking_stats": _OPPORTUNITIES.stats(now_ms),
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
    continuity = _OPPORTUNITIES.view(now_ms, current)
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
        "opportunity_tracking": continuity,
        "opportunity_tracking_stats": _OPPORTUNITIES.stats(now_ms),
        "uk_spot_account_status": "ACCOUNT_NOT_CONNECTED_UNVERIFIED",
        "uk_spot_account_tradability_verified": False,
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
    # Use the current independent live snapshot whenever it is fresh.
    # Only rebuild a stale snapshot, avoiding unnecessary quote latency.
    live = await asyncio.to_thread(read_live)
    if not live["fresh"]:
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
        "uk_spot_account_status": "ACCOUNT_NOT_CONNECTED_UNVERIFIED",
        "uk_spot_account_tradability_verified": False,
        "order_placement": False,
        "disclaimer": "Read-only observation; venue conditions can change before an order.",
    }, headers={"Cache-Control": "no-store, max-age=0"})


_DASHBOARD = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#f7f8fa">
<title>Live Crypto Scanner</title>
<style>
:root{color-scheme:light;--base:#f7f8fa;--paper:#fff;--text:#24292f;--muted:#65727b;--line:#e3e7eb;--ink:#26343b;--accent:#146b5c;--green:#127154;--amber:#ab6900;--yellow:#857300;--red:#ad4040;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Arial,system-ui,sans-serif}
*{box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:78px}
body{margin:0 auto;padding:26px 25px 55px;max-width:1340px;background:var(--base);color:var(--text);font-size:14px;line-height:1.48;-webkit-text-size-adjust:100%}
header{margin:0 0 14px}
h1{font-size:26px;letter-spacing:-.035em;margin:0 0 4px;line-height:1.3;color:var(--ink)}
h2{font-size:18px;color:var(--ink);letter-spacing:-.015em;margin:29px 0 8px}
h3{font-size:14px;color:var(--ink);font-weight:700;margin:19px 0 9px}
p{margin:5px 0 12px;line-height:1.55;color:var(--muted);font-size:13px}
p b{color:var(--ink)}
.muted,.footer{color:var(--muted)}
.app-kicker{font-size:12px;color:var(--muted);margin:0}
.app-top{display:flex;align-items:start;justify-content:space-between;gap:18px}
.header-pill{font-size:11px;font-weight:700;color:var(--accent);border:1px solid #d3e2df;border-radius:6px;padding:7px 9px;white-space:nowrap;background:var(--paper)}
#status,#authority{display:block;border:1px solid var(--line);padding:9px 12px;border-radius:6px;background:var(--paper);color:var(--muted);margin:7px 0;overflow-wrap:anywhere;font-size:12px}
#status.good{color:var(--green)}#status.bad{color:var(--red)}
.good,.state-buy{color:var(--green)}.warn,.state-armed{color:var(--amber)}.bad{color:var(--red)}.state-watch{color:var(--yellow)}
.summary-line{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin:16px 0}
.summary-line span{background:var(--paper);border:1px solid var(--line);padding:11px 14px;border-radius:7px;font-size:12px;color:var(--muted);min-width:0}
.summary-line strong{font-size:19px;color:var(--ink);display:block;margin-top:5px;font-variant-numeric:tabular-nums}
.summary-line #lastUpdated{grid-column:1/-1;border:0;background:transparent;padding:1px;font-size:11px}
.legend{background:var(--paper);border:1px solid var(--line);border-radius:7px;color:var(--muted);padding:10px 12px;font-size:12px;margin:16px 0}
.legend b{color:var(--ink)}.legend .ready{color:var(--green);font-weight:700}.legend .pending{color:var(--amber);font-weight:700}.legend .watch{color:var(--yellow);font-weight:700}
.quick{display:flex;gap:8px;flex-wrap:wrap;padding:13px 0;margin-bottom:9px;border-bottom:1px solid var(--line)}
.quick a{color:#48565f;text-decoration:none;font-weight:600;font-size:12px;border:1px solid #d9dfe4;padding:8px 11px;border-radius:6px;background:var(--paper);white-space:nowrap;transition:border-color .15s}
.quick a:hover,.quick a:focus-visible{border-color:#9ebdb4;color:var(--accent)}
.quick a:first-child{border-color:#b5d6c8;color:var(--green)}
section{margin:0}
section.approved{padding:14px 17px 7px;border:1px solid #dce4e2;background:var(--paper);border-radius:8px}
section.approved h2{margin-top:4px}
.strategy-lane{margin:0 0 19px;padding:9px 0 3px}
.strategy-lane h2{margin-top:10px}
.strategy-lane p{margin:0 0 11px}
.lane-context{display:none}
.table-scroll{position:relative;max-width:100%;overflow-x:auto;overflow-y:hidden;-webkit-overflow-scrolling:touch;border:1px solid var(--line);border-radius:7px;background:var(--paper)}
table{border-collapse:separate;border-spacing:0;width:100%;min-width:650px;font-size:12px;background:var(--paper);font-variant-numeric:tabular-nums}
th{background:#f4f6f7;color:#5c6871;font-weight:700;text-align:left;white-space:nowrap;padding:11px 12px;border-bottom:1px solid var(--line);font-size:11px}
td{padding:11px 12px;text-align:left;vertical-align:middle;color:var(--text);border-bottom:1px solid #edf0f2;white-space:nowrap;font-size:12px}
td:nth-child(3),td:nth-child(7),td:nth-child(9){max-width:270px;white-space:normal;min-width:110px;overflow-wrap:anywhere}
td:first-child{font-weight:700;color:var(--ink);min-width:96px}
tr:last-child td{border-bottom:0}
tbody tr:nth-child(even) td{background:#fbfcfd}
tbody tr:hover td{background:#f3f8f5}
th:first-child,td:first-child{position:sticky;left:0;z-index:1;background:var(--paper);box-shadow:1px 0 0 var(--line)}
th:first-child{z-index:2;background:#f4f6f7}
tbody tr:nth-child(even) td:first-child{background:#fbfcfd}
tbody tr:hover td:first-child{background:#f3f8f5}
button{font:inherit;font-weight:650;color:var(--accent);background:var(--paper);border:1px solid #bad3ca;border-radius:6px;padding:8px 10px;cursor:pointer;min-height:36px}
button:disabled{opacity:.5;cursor:default}
button:hover:not(:disabled){background:#edf6f1}
.note{color:var(--muted);font-size:12px;padding:10px 0;margin-bottom:7px}
#quote{padding:12px;background:var(--paper);border:1px solid var(--line);border-radius:6px;font-size:12px;overflow-wrap:anywhere}
#structureNote,#qualNote,#subNote,#moverNote,#researchNote{color:var(--muted);font-size:12px;margin:0 0 9px}
#verifiedSummary{font-size:12px;color:var(--muted);margin:4px 0 7px}
#verifiedSummary.good{color:var(--green)}
details.advanced{margin-top:24px;background:var(--paper);border:1px solid var(--line);border-radius:7px;padding:0 16px 16px}
details.advanced>summary{cursor:pointer;padding:13px 0;font-size:14px;font-weight:700;color:var(--ink)}
details.advanced>summary::marker{color:var(--accent)}
details.advanced h2{margin-top:19px}
.footer{font-size:11px;margin:18px 0 0;text-align:center}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
@media(max-width:740px){
 body{padding:16px max(12px,env(safe-area-inset-right)) calc(24px + env(safe-area-inset-bottom)) max(12px,env(safe-area-inset-left));max-width:100%;overflow-x:hidden}
 h1{font-size:22px}
 .app-top{gap:6px}
 .header-pill{font-size:10px;padding:6px}
 .summary-line{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
 .summary-line span{padding:9px 10px;font-size:11px}
 .summary-line strong{font-size:17px}
 .quick{flex-wrap:nowrap;overflow-x:auto;overscroll-behavior-x:contain;-webkit-overflow-scrolling:touch;position:sticky;top:0;z-index:10;background:var(--base);padding:10px 0 12px;margin:10px -2px 12px}
 .quick a{min-height:43px;display:inline-flex;align-items:center;flex-shrink:0;font-size:12px}
 section.approved{padding:11px 10px 8px}
 h2{font-size:16px;margin-top:25px}
 h3{font-size:13px}
 p{font-size:12px}
 .table-scroll{overflow-x:auto;touch-action:pan-x pan-y}
 .table-scroll::before{content:"Swipe table →";position:sticky;left:8px;display:block;width:max-content;padding:5px 0 4px;font-size:10px;color:#78858c}
 table{display:table!important;table-layout:auto;min-width:720px!important;width:max-content;max-width:none;font-size:12px}
 thead{display:table-header-group!important}
 tbody{display:table-row-group!important}
 tbody tr{display:table-row!important;background:transparent!important;border:0!important}
 th,td{display:table-cell!important;min-width:unset;grid-column:auto!important;border-radius:0!important}
 th{padding:10px 10px;font-size:11px}
 td{padding:10px 10px;font-size:12px!important}
 td::before{display:none!important;content:none!important}
 tbody td:first-child{font-size:12px!important;padding:10px!important;border-bottom:1px solid #edf0f2!important}
 tbody td[colspan]{display:table-cell!important;font-size:12px!important}
 tbody td button{min-width:96px;min-height:43px}
 .legend{line-height:1.8}
 .legend span{white-space:normal}
}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}}

/* Lime and grey presentation palette only. No layout or behaviour changes. */
:root{
 --base:#b9eb8b;
 --paper:#f3f4f6;
 --text:#283036;
 --muted:#616d74;
 --line:#b7c0c5;
 --ink:#30393d;
 --accent:#4d760d;
 --green:#47750d;
}
.header-pill{background-color:#e8f5ca;border-color:#a3e635;color:#41650a}
#status,#authority,#quote{background-color:var(--paper);border-color:#afb9bf}
.summary-line span{background-color:#eceff1;border-color:#bdc5c9}
.legend{background-color:#edf2e6;border-color:#a3e635}
.quick a{background-color:#eceff1;border-color:#b8c2c6}
.quick a:first-child{background-color:#e8f5ca;border-color:#9bd62b;color:#41650a}
.quick a:hover,.quick a:focus-visible{border-color:#91c927;background-color:#eaf6d2}
section.approved{border-color:#9bd62b;background-color:var(--paper)}
.strategy-lane h2{color:#456c0b}
.table-scroll{border-color:#b3c9a0;background-color:var(--paper)}
table{background-color:var(--paper)}
th,th:first-child{background-color:#dee3e5;color:#4a565b}
td{border-bottom-color:#cbd2d6}
th:first-child,td:first-child{box-shadow:1px 0 0 #b9c4b4}
tbody tr:nth-child(even) td,tbody tr:nth-child(even) td:first-child{background-color:#e9edef}
tbody tr:hover td,tbody tr:hover td:first-child{background-color:#eaf5d3}
tbody td:first-child{background-color:var(--paper)}
button{color:#263d06;background-color:#a3e635;border-color:#83b925}
button:hover:not(:disabled){background-color:#b8ee68}
details.advanced{background-color:var(--paper);border-color:#b8c2c6}
:focus-visible{outline-color:#81b521}
</style></head><body>
<header class="app-top"><div><h1>Live Crypto Scanner</h1><p class="app-kicker">Binance Spot • Simple coin tables • Auto-refreshing</p></div><span class="header-pill">Market watch</span></header>
<div id="status" role="status" aria-live="polite">Connecting to market prices…</div>
<div id="authority" role="status">Checking live signal approvals…</div>
<div class="summary-line" aria-label="At a glance">
<span>Ready to buy<strong id="metricVerified">0</strong></span>
<span>Coin setups<strong id="metricStructure">0</strong></span>
<span>AI approved<strong id="metricMl">0</strong></span>
<span>Market connection<strong id="metricFeed">Checking</strong></span>
<span id="lastUpdated">Connecting…</span>
</div>
<div class="legend"><b>Signal guide:</b>
<span class="ready">Green = verified BUY NOW</span> · <span class="pending">Orange = ARMED</span> · <span class="watch">Yellow = WATCH</span>.
Research entries are <b>not</b> approved trades.</div>
<nav class="quick" aria-label="Trading sections">
<a href="#verified">Ready to buy</a><a href="#beast-lane">BEAST</a><a href="#ema-lane">EMA</a>
<a href="#exhaustion-lane">Seller exhaustion</a><a href="#breakout-lane">Breakouts</a><a href="#pullback-lane">Pullbacks</a>
<a href="#other-lane">Other</a><a href="#buy-structure">All coins</a><a href="#research-section">EMA watch</a>
</nav>
<section class="approved" id="verified">
<h2>Verified BUY NOW <span class="muted">· <span id="verifiedCount">0</span> coins</span></h2>
<p>Only coins confirmed by the scanner's current live checks. These are separate from the developing ideas below. Recheck the quote before placing any trade.</p>
<div id="verifiedSummary" class="good" aria-live="polite">Checking approved entries…</div>
<h3>EMA approved <span class="muted">· <span id="emaCount">0</span></span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Timeframe</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check price</th></tr></thead><tbody id="signals"></tbody></table></div>
<h3>Machine Learning approved <span class="muted">· <span id="mlCount">0</span></span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Setup / hold time</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check price</th></tr></thead><tbody id="mlSignals"></tbody></table></div>
<h3>Chart-pattern approved <span class="muted">· <span id="v12Count">0</span></span></h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Pattern / timeframe</th><th>Entry</th><th>Stop loss</th><th>Target 1</th><th>Target 2</th><th>Target 3</th><th>Check price</th></tr></thead><tbody id="v12Signals"></tbody></table></div>
<h3>Fresh price check</h3><div id="quote" aria-live="polite">Tap Verify on an approved coin to check its latest price.</div>
</section>
<h2>Coin opportunities <span class="muted">· developing</span></h2>
<p>Each coin appears in its relevant table. Entry prices and potential gains are estimates, not guaranteed results.</p>
<section class="strategy-lane" id="beast-lane">
<h2>BEAST <span class="muted">· <span id="beastCount">0</span> coins</span></h2>
<p>Possible bigger movers. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="beastRows"></tbody></table></div>
</section>
<section class="strategy-lane" id="ema-lane">
<h2>EMA <span class="muted">· <span id="emaSetupCount">0</span> coins</span></h2>
<p>Near important moving averages. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="emaSetupRows"></tbody></table></div>
</section>
<section class="strategy-lane" id="exhaustion-lane">
<h2>Seller exhaustion <span class="muted">· <span id="exhaustionCount">0</span> coins</span></h2>
<p>Selling pressure may be slowing. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="exhaustionRows"></tbody></table></div>
</section>
<section class="strategy-lane" id="breakout-lane">
<h2>Breakouts <span class="muted">· <span id="breakoutCount">0</span> coins</span></h2>
<p>Prices approaching breakout levels. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="breakoutRows"></tbody></table></div>
</section>
<section class="strategy-lane" id="pullback-lane">
<h2>Pullbacks <span class="muted">· <span id="pullbackCount">0</span> coins</span></h2>
<p>Coins pulling back near support. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="pullbackRows"></tbody></table></div>
</section>
<section class="strategy-lane" id="other-lane">
<h2>Other opportunities <span class="muted">· <span id="otherCount">0</span> coins</span></h2>
<p>More developing setups. <b>Research only</b> — not a verified buy.</p>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Setup / timeframe</th><th>Entry zone*</th><th>Possible upside*</th><th>Stop-loss*</th><th>Still needed</th></tr></thead><tbody id="otherRows"></tbody></table></div>
</section>
<p class="note">*Entry zone = possible buying range; upside = a potential move towards the listed target; stop-loss = an estimated exit point. These figures do not guarantee a result.</p>
<h2 id="buy-structure">All chart setups <span class="muted">· <span id="structureCount">0</span> coins</span></h2>
<p>One full table of chart patterns. A chart marked BUY here is <b>not yet a verified BUY NOW</b> unless it also appears in the approved section.</p>
<div id="structureNote" role="status">Checking coin setups…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Pattern / timeframe</th><th>Entry zone</th><th>Stop-loss</th><th>Target</th><th>Upside</th><th>Data age</th><th>Why not approved?</th></tr></thead><tbody id="structureRows"></tbody></table></div>
<h2 id="tracking-section">Continuously tracked opportunities <span class="muted">· <span id="trackingCount">0</span></span></h2>
<p>Chart and EMA setups remain visible after a brief quote expiry. <b>Revalidation required</b> means no current BUY authorisation. After restart, stored setups must receive fresh evidence.</p>
<div id="trackingNote" class="note">Loading tracked opportunities…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Current monitoring</th><th>Setup</th><th>Observed entry</th><th>Target estimate</th><th>Last seen</th></tr></thead><tbody id="trackingRows"></tbody></table></div>
<p class="note">UK Spot account permissions are not verified without an authorised account connection. A structural BUY or fresh quote is not a trade instruction.</p>
<h2 id="research-section">EMA watchlist</h2>
<p>Coins approaching important moving averages. WATCH means monitor, not buy.</p>
<div id="researchNote" role="status">Checking EMA watchlist…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Timeframe</th><th>EMA</th><th>Distance</th><th>Data age</th><th>Updated?</th></tr></thead><tbody id="research"></tbody></table></div>
<h3>More EMA candidates (rotating)</h3>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Status</th><th>Timeframe</th><th>EMA</th><th>Distance</th><th>Data age</th><th>Updated?</th></tr></thead><tbody id="rotating"></tbody></table></div>
<details class="advanced"><summary>More information: AI decisions and connection status</summary>
<h2 id="qualification">Why some coins are held back</h2>
<p>Data blocked means market information is missing or late. Model rejected means the AI did not approve that coin.</p>
<div id="qualNote" role="status">Checking AI decisions…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Strategy</th><th>AI status</th><th>Target</th><th>Reason</th></tr></thead><tbody id="qualRows"></tbody></table></div>
<h2 id="feed">Live price and order-book connection</h2>
<p>Technical connection details. Missing or old information can prevent a signal from being verified.</p>
<div id="subNote" role="status">Checking live data…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Trade stream</th><th>Order book</th><th>Trade age</th><th>Book age</th><th>Connection status</th></tr></thead><tbody id="subRows"></tbody></table></div>
<h2>Tracked 10%+ price moves</h2>
<p>Previously observed rises during scanner monitoring, not a list of coins to buy now.</p>
<div id="moverNote">Checking records…</div>
<div class="table-scroll"><table><thead><tr><th>Coin</th><th>Price rise</th><th>Tracking started</th><th>Earlier status</th></tr></thead><tbody id="moverRows"></tbody></table></div>
</details>
<p class="footer">Live scanner • Research only • No automatic orders • Binance Spot USDT pairs</p>
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
const trackingRows=document.getElementById("trackingRows");
const trackingNote=document.getElementById("trackingNote");
const trackingCount=document.getElementById("trackingCount");
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
function showTracking(d){
 clear(trackingRows);
 const list=d.opportunity_tracking||[];
 trackingCount.textContent=String(list.length);
 trackingNote.textContent=list.length ? "Continuously refreshed when the scanner has valid source evidence. Expired quotes never become automatic orders." : "No source-confirmed opportunities have been tracked yet.";
 if(!list.length){const r=trackingRows.insertRow();cell(r,"No tracked setups yet").colSpan=6;return;}
 for(const q of list){
  const r=trackingRows.insertRow();
  cell(r,q.symbol);
  const state=cell(r,q.live_monitoring?q.display_state:"REVALIDATION REQUIRED");
  state.className=q.live_monitoring?"state-armed":"bad";
  cell(r,friendlyPattern(q.setup)+" / "+q.timeframe);
  cell(r,q.entry_low==null?"Research only":money(q.entry_low)+"–"+money(q.entry_high));
  cell(r,money(q.tp1));
  cell(r,Math.floor((q.last_observed_age_ms||0)/1000)+"s ago");
 }
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
      +" | UK Spot account permission unverified; recheck on Binance. No order has been placed.";
    setTimeout(()=>{quote.textContent="LIVE QUOTE EXPIRED: underlying opportunity stays tracked; verify a new quote before trading.";},lease);
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
    showTracking(d);
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
  finally{requestPending=false;setTimeout(refresh,1250);}
}
// Server-pushed BUY changes accelerate the existing polling fallback.
try { const alerts=new EventSource("/signals/events");
 alerts.addEventListener("buy",()=>{if(!document.hidden)refresh();});
 alerts.addEventListener("snapshot",()=>{if(!document.hidden)refresh();});
} catch(e) { /* Regular no-cache polling remains active. */ }
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
        "opportunity_tracking_stats": live.get("opportunity_tracking_stats", {}),
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
