"""Independent, auditable 24-hour outcome tracker for every ranked V13 ML buy option.

Tracks ALL 40 considered ML options, not just the top five. The 24h close is
the first Binance Spot 1m candle close at/after the recorded signal +24h.
Only fresh, priced signals enter accuracy statistics. Tracking never issues BUY.
"""
import asyncio
import hashlib
import json
import math
import os
import statistics
import time
from collections import deque
from datetime import datetime, timezone

import aiohttp

REVISION = "13.1.0-ml24h-all-candidates"
STATE_PATH = os.getenv("PSI_V13_PERF24_STATE", "/data/psi_v13_perf24_state.json")
SNAPSHOT_LOG_PATH = os.getenv("PSI_V13_PERF24_SIGNALS", "/data/psi_v13_perf24_signals.jsonl")
RESULT_LOG_PATH = os.getenv("PSI_V13_PERF24_RESULTS", "/data/psi_v13_perf24_results.jsonl")
HORIZON_MS = 86_400_000
COOLDOWN_MS = 86_400_000
MAX_RECENT = 3500
MAX_DUE_BATCH = 8
FEE_ROUND_TRIP_PCT = max(0.0, min(2.0, float(os.getenv("PSI_V13_PERF24_FEES_PCT", "0.2"))))
REST_HOSTS = ("https://data-api.binance.vision", "https://api.binance.com")
V13 = None
CORE = None
_ORIGINAL_SCAN = None
_ORIGINAL_HEALTH = None
_pending = []
_recent = deque(maxlen=MAX_RECENT)
_last_by_cohort = {}
_model24 = {"weights": [0.0] * 13, "bias": 0.0, "trained": 0, "test_n": 0,
            "test_correct": 0, "test_brier_sum": 0.0}
_stats = {"signals": 0, "priced": 0, "unpriced": 0, "resolved": 0, "unavailable": 0,
          "capture_errors": 0, "price_errors": 0, "write_errors": 0,
          "model_training_events": 0, "last_error": ""}
_last_snapshot_save = 0
_last_heartbeat_ms = 0
_last_daily_day = ""
_rest_cursor = 0


def number(value, default=0.0):
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (ValueError, TypeError, OverflowError):
        return default


def now_ms():
    return int(time.time() * 1000)


def day_utc(value):
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _append(path, event):
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, separators=(",", ":"), allow_nan=False) + "\n")
        return True
    except (OSError, ValueError, TypeError) as exc:
        _stats["write_errors"] += 1
        _stats["last_error"] = "journal:" + type(exc).__name__
        return False


def _save(force=False):
    global _last_snapshot_save
    at = now_ms()
    if not force and at - _last_snapshot_save < 45_000:
        return
    parent = os.path.dirname(STATE_PATH)
    try:
        if parent:
            os.makedirs(parent, exist_ok=True)
        temp = STATE_PATH + ".tmp"
        with open(temp, "w", encoding="utf-8") as fh:
            json.dump({"pending": _pending, "recent": list(_recent),
                       "last_by_cohort": _last_by_cohort,
                       "model24": _model24, "stats": _stats,
                       "last_daily_day": _last_daily_day}, fh,
                      separators=(",", ":"), allow_nan=False)
        os.replace(temp, STATE_PATH)
        _last_snapshot_save = at
    except (OSError, ValueError, TypeError) as exc:
        _stats["write_errors"] += 1
        _stats["last_error"] = "state:" + type(exc).__name__


def _restore():
    global _last_daily_day
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return
        _pending[:] = list(data.get("pending") or [])[-2000:]
        _recent.clear()
        _recent.extend(list(data.get("recent") or [])[-MAX_RECENT:])
        _last_by_cohort.clear()
        _last_by_cohort.update(data.get("last_by_cohort") or {})
        if len((data.get("model24") or {}).get("weights") or []) == 13:
            _model24.update(data["model24"])
        _stats.update(data.get("stats") or {})
        _last_daily_day = str(data.get("last_daily_day") or "")
    except (OSError, ValueError, TypeError):
        pass


def _feature_vector(ml):
    evidence = ml.get("evidence") or {}
    # Recorded at selection time, never reconstructed after knowing the result.
    return [
        min(1.0, number(evidence.get("hazard")) / 100),
        max(-1, min(1, (number(evidence.get("buy_ratio"), 0.5) - 0.5) * 2)),
        min(2, number(evidence.get("rv10")) / 10),
        min(2, number(evidence.get("rv30")) / 10),
        min(2, number(evidence.get("trade_acceleration")) / 10),
        max(-1, min(1, number(evidence.get("cvd_acceleration")))),
        max(-1, min(1, number(evidence.get("ofi")))),
        max(-1, min(1, number(evidence.get("ofi_acceleration")))),
        max(-1, min(1, number(evidence.get("obi")))),
        max(-1, min(1, number(evidence.get("ask_depletion")))),
        min(1, number(evidence.get("sequence_score")) / 100),
        min(1, number(evidence.get("v128_probe_score")) / 100),
        min(5, number(evidence.get("spread_bps"), 100) / 20),
    ]


def _prob24(x):
    z = max(-20.0, min(20.0, number(_model24["bias"]) +
                   sum(w * v for w, v in zip(_model24["weights"], x))))
    return 1 / (1 + math.exp(-z))


def _learn24(event, win):
    """Independent 24h target model. Does not modify V13's 15m model."""
    x = event["features"]
    frozen = event.get("prob24_at_signal")
    # Test statistics are prospective; untrained picks have no claim to accuracy.
    if event.get("split") == "TEST":
        if frozen is not None:
            _model24["test_n"] += 1
            _model24["test_correct"] += int((frozen >= 0.5) == bool(win))
            _model24["test_brier_sum"] += (frozen - win) ** 2
        return
    if event.get("split") == "VALIDATION":
        return
    p = _prob24(x)
    eta = 0.055 / math.sqrt(1 + _model24["trained"] / 250)
    _model24["weights"] = [
        max(-4.5, min(4.5, w * (1 - eta * 0.001) + eta * (win - p) * v))
        for w, v in zip(_model24["weights"], x)
    ]
    _model24["bias"] = max(-5, min(5, _model24["bias"] + eta * (win - p)))
    _model24["trained"] += 1
    _stats["model_training_events"] += 1


def collect(ml_rows=None, at=None):
    """Once per unique symbol/class/24h; repeated scans update seen_count only."""
    at = now_ms() if at is None else int(at)
    if ml_rows is None:
        ml_rows = list(getattr(V13, "_ranked", []) or [])
    added = 0
    by_key = {e["cohort"]: e for e in _pending}
    for row in ml_rows:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol.endswith("USDT"):
            continue
        signal = "ML BUY NOW" if row.get("execution_ready") else "ML BUY CANDIDATE"
        stamp = int(number(row.get("signal_ms"), at))
        if stamp <= 0 or stamp > at + 2000 or at - stamp > 90_000:
            continue
        key = symbol + "|" + signal
        old = int(number(_last_by_cohort.get(key)))
        if old > 0 and at - old < COOLDOWN_MS:
            if key in by_key:
                event = by_key[key]
                event["seen_count"] += 1
                event["best_rank"] = min(event["best_rank"], int(row.get("rank") or 40))
                # If the first sighting lacked a trustworthy trade price, start
                # the 24-hour clock at the FIRST subsequently verified quote.
                # Never backfill the first entry using later price action.
                if not event.get("baseline_verified"):
                    execution_price = number(row.get("observed_price"))
                    quote = execution_price or number(row.get("research_price"))
                    trade_age = number((row.get("evidence") or {}).get("trade_age_ms"), -1)
                    has_quote = bool(quote > 0 and (execution_price > 0 or
                                      row.get("research_price_source") == "RECENT_BINANCE_SPOT_SENSOR_TRADE"
                                      and 0 <= trade_age <= 15000))
                    if has_quote:
                        event.update({
                            "id": hashlib.sha256((key + "|" + str(stamp)).encode()).hexdigest()[:18],
                            "signal_ms": stamp, "target_ms": stamp + HORIZON_MS,
                            "signal_day_utc": day_utc(stamp),
                            "price": quote, "baseline_verified": True,
                            "entry_quality": "EXECUTION_VERIFIED" if execution_price > 0 else "RESEARCH_QUOTE",
                            "features": _feature_vector(row),
                            "model_probability_15m": row.get("model_probability"),
                            "prob24_at_signal": (round(_prob24(_feature_vector(row)), 5)
                                                 if _model24["trained"] >= 100 else None),
                            "status": "PENDING", "observed_at_ms": at,
                        })
                        _last_by_cohort[key] = stamp
                        _stats["priced"] += 1
                        _stats["unpriced"] = max(0, _stats["unpriced"] - 1)
                        _append(SNAPSHOT_LOG_PATH, {"event": "BASELINE_VERIFIED", **event})
                        _save(force=True)
            continue
        if len(_pending) >= 2000:
            _stats["capture_errors"] += 1
            _stats["last_error"] = "PENDING_CAPACITY"
            break
        execution_price = number(row.get("observed_price"))
        price = execution_price or number(row.get("research_price"))
        # Research returns are tracked even when book/sequence checks prevent
        # executable BUYs. Such prices require a recent real Spot trade.
        baseline_valid = bool(price > 0 and
                              row.get("research_price_source") == "RECENT_BINANCE_SPOT_SENSOR_TRADE"
                              and 0 <= number((row.get("evidence") or {}).get("trade_age_ms"), -1) <= 15000)
        if not baseline_valid and execution_price > 0:
            price = execution_price
            baseline_valid = True
        x = _feature_vector(row)
        model_ready = _model24["trained"] >= 100
        record = {
            "id": hashlib.sha256((key + "|" + str(stamp)).encode()).hexdigest()[:18],
            "cohort": key, "symbol": symbol, "signal": signal,
            "signal_ms": stamp, "target_ms": stamp + HORIZON_MS,
            "signal_day_utc": day_utc(stamp),
            "price": price if baseline_valid else None,
            "baseline_verified": baseline_valid,
            "entry_quality": ("EXECUTION_VERIFIED" if baseline_valid and execution_price > 0
                              else "RESEARCH_QUOTE" if baseline_valid else "NO_PRICE"),
            "rank": int(row.get("rank") or 40),
            "best_rank": int(row.get("rank") or 40),
            "seen_count": 1,
            "model": row.get("model_version"),
            "model_probability_15m": row.get("model_probability"),
            "score": row.get("rank_score"),
            "features": x,
            "prob24_at_signal": round(_prob24(x), 5) if model_ready else None,
            "split": ("TRAIN" if (stamp // 86_400_000) % 20 < 14
                      else "VALIDATION" if (stamp // 86_400_000) % 20 < 17
                      else "TEST"),
            "observed_at_ms": at,
            "status": "PENDING" if baseline_valid else "UNPRICED",
            "attempts": 0,
        }
        _pending.append(record)
        by_key[key] = record
        _last_by_cohort[key] = stamp
        _stats["signals"] += 1
        _stats["priced" if baseline_valid else "unpriced"] += 1
        _append(SNAPSHOT_LOG_PATH, record)
        added += 1
    if added:
        _save()
    return added


async def close_at_24h(session, symbol, target_ms):
    """Use a 1m candle close (within 60s of the exact 24h target)."""
    start = (int(target_ms) // 60_000) * 60_000
    params = {"symbol": symbol, "interval": "1m", "startTime": start,
              "endTime": start + 120_000, "limit": 3}
    error = None
    for host in REST_HOSTS:
        try:
            async with session.get(host + "/api/v3/klines", params=params,
                                   timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    error = "HTTP_" + str(resp.status)
                    continue
                candles = await resp.json()
                if not isinstance(candles, list):
                    error = "BAD_CANDLES"
                    continue
                for row in candles:
                    if not isinstance(row, list) or len(row) < 7:
                        continue
                    closed_ms = int(number(row[6]))
                    px = number(row[4])
                    if target_ms <= closed_ms <= target_ms + 60_000 and px > 0:
                        return {"price": px, "close_ms": closed_ms,
                                "source": "BINANCE_SPOT_1M_CLOSE"}
                return None
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
            error = type(exc).__name__
    _stats["price_errors"] += 1
    _stats["last_error"] = "price:" + str(error or "UNAVAILABLE")
    return None


def _resolve(event, candle, at):
    entry = number(event.get("price"))
    close = number(candle.get("price"))
    if entry <= 0 or close <= 0:
        raise ValueError("unverified entry or target price")
    gross = 100 * (close / entry - 1)
    net = gross - FEE_ROUND_TRIP_PCT
    won = net > 0
    result = {
        "id": event["id"], "symbol": event["symbol"], "signal": event["signal"],
        "rank": event["rank"], "best_rank": event["best_rank"],
        "seen_count": event["seen_count"], "signal_day_utc": event["signal_day_utc"],
        "entry_quality": event.get("entry_quality", "UNKNOWN"),
        "signal_ms": event["signal_ms"], "target_ms": event["target_ms"],
        "resolved_ms": at, "entry_price": entry, "close_price": close,
        "close_ms": candle["close_ms"], "close_source": candle["source"],
        "gross_return_pct": round(gross, 5), "estimated_net_return_pct": round(net, 5),
        "assumed_round_trip_fee_pct": FEE_ROUND_TRIP_PCT,
        "direction_correct": won, "close_above_entry": gross > 0,
        "close_at_least_3pct": gross >= 3.0,
        "close_at_least_5pct": gross >= 5.0,
        "close_at_least_10pct": gross >= 10.0,
        "model_probability_15m": event.get("model_probability_15m"),
        "predicted_24h_direction_probability": event.get("prob24_at_signal"),
        "features_at_signal": event["features"],
        "split": event["split"],
        "note": "24h close return, NOT peak gain, fills, or realized trading PnL",
    }
    _learn24(event, int(won))
    _stats["resolved"] += 1
    _recent.append(result)
    _append(RESULT_LOG_PATH, result)
    return result


async def resolve_due(session, at=None, batch=MAX_DUE_BATCH):
    at = now_ms() if at is None else int(at)
    resolved = []
    remaining = []
    processed = 0
    for event in _pending:
        if at < event["target_ms"]:
            remaining.append(event)
            continue
        if not event.get("baseline_verified"):
            _stats["unavailable"] += 1
            # Excluded from win-rate; never call an unpriced pick a loss.
            continue
        if processed >= batch:
            remaining.append(event)
            continue
        processed += 1
        candle = await close_at_24h(session, event["symbol"], event["target_ms"])
        if candle is not None:
            resolved.append(_resolve(event, candle, at))
            continue
        event["attempts"] += 1
        if at - event["target_ms"] <= 48 * 3_600_000:
            remaining.append(event)
        else:
            _stats["unavailable"] += 1
    _pending[:] = remaining
    if processed or resolved:
        _save(force=True)
    return resolved


def _summary(rows):
    priced = [r for r in rows if r.get("estimated_net_return_pct") is not None]
    if not priced:
        return {"resolved": 0, "win_rate_pct": None, "avg_net_return_pct": None,
                "median_net_return_pct": None, "max_net_return_pct": None,
                "min_net_return_pct": None, "wins": 0, "losses": 0}
    returns = [number(r["estimated_net_return_pct"]) for r in priced]
    wins = sum(v > 0 for v in returns)
    return {
        "resolved": len(priced),
        "wins": wins,
        "losses": len(priced) - wins,
        "win_rate_pct": round(100 * wins / len(priced), 2),
        "avg_net_return_pct": round(statistics.mean(returns), 4),
        "median_net_return_pct": round(statistics.median(returns), 4),
        "max_net_return_pct": round(max(returns), 4),
        "min_net_return_pct": round(min(returns), 4),
        "close_at_least_3pct_count": sum(r.get("close_at_least_3pct") for r in priced),
        "close_at_least_5pct_count": sum(r.get("close_at_least_5pct") for r in priced),
    }


def report(at=None):
    at = now_ms() if at is None else int(at)
    all_rows = list(_recent)
    last24 = [r for r in all_rows if 0 <= at - int(r["resolved_ms"]) < HORIZON_MS]
    last7d = [r for r in all_rows if 0 <= at - int(r["resolved_ms"]) < 7 * HORIZON_MS]
    top5 = [r for r in last7d if r["rank"] <= 5]
    rest = [r for r in last7d if r["rank"] > 5]
    buys = [r for r in last7d if r["signal"] == "ML BUY NOW"]
    return {
        "revision": REVISION,
        "evaluation": "Binance Spot 1-minute candle close at signal+24h; no earlier data",
        "fee_assumption_pct_round_trip": FEE_ROUND_TRIP_PCT,
        "distinct_opportunities_not_repeated_scan_counts": True,
        "observation_universe": "ALL top-40 ML-ranked buy options per ranking cycle",
        "total_recorded_signals": _stats["signals"],
        "measurement_requires_verified_signal_time_price": True,
        "priced_signals": _stats["priced"],
        "unpriced_signals": _stats["unpriced"],
        "pending_24h": sum(bool(e.get("baseline_verified")) for e in _pending),
        "unpriced_pending": sum(not e.get("baseline_verified") for e in _pending),
        "unavailable": _stats["unavailable"],
        "completed_today_utc": _summary([r for r in all_rows if day_utc(r["resolved_ms"]) == day_utc(at)]),
        "completed_past_24h": _summary(last24),
        "completed_past_7d": _summary(last7d),
        "historical_recent": _summary(all_rows),
        "top5_past_7d": _summary(top5),
        "rank6_to40_past_7d": _summary(rest),
        "executable_ml_buys_past_7d": _summary(buys),
        "research_candidates_past_7d": _summary([r for r in last7d if r["signal"] != "ML BUY NOW"]),
        "model24_training_count": _model24["trained"],
        "model24_test_count": _model24["test_n"],
        "model24_test_accuracy_pct": (round(100 * _model24["test_correct"] / _model24["test_n"], 2)
                                     if _model24["test_n"] else None),
        "model24_test_brier": (round(_model24["test_brier_sum"] / _model24["test_n"], 5)
                              if _model24["test_n"] else None),
        "most_recent_resolved": all_rows[-10:][::-1],
        "last_error": _stats.get("last_error") or None,
        "note": "Directional results are observational and may differ from actual executable returns.",
    }


def _augment(response):
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    perf = report()
    payload["v13_ml_performance_24h"] = perf
    if isinstance(payload.get("v13_ml"), dict):
        payload["v13_ml"]["performance_24h"] = {
            "completed_past_24h": perf["completed_past_24h"],
            "pending_24h": perf["pending_24h"],
            "unpriced_signals": perf["unpriced_signals"],
            "report_revision": REVISION,
        }
    return CORE.app.web.json_response(payload, status=response.status)


async def _scan(req):
    return _augment(await _ORIGINAL_SCAN(req))


async def _health(req):
    return _augment(await _ORIGINAL_HEALTH(req))


async def supervisor_loop():
    global _last_heartbeat_ms, _last_daily_day
    async with aiohttp.ClientSession(headers={"User-Agent": "PsiV13-24h-outcomes/1.0"}) as session:
        while True:
            try:
                added = collect()
                resolved = await resolve_due(session)
                at = now_ms()
                today = day_utc(at)
                if _last_daily_day != today:
                    _last_daily_day = today
                    rep = report(at)
                    m = rep["completed_past_24h"]
                    print("PSI-V13 PERF24_DAILY day=" + today +
                          " tracked=" + str(rep["total_recorded_signals"]) +
                          " resolved24=" + str(m["resolved"]) +
                          " winRate24=" + str(m["win_rate_pct"]) +
                          " avgNet24=" + str(m["avg_net_return_pct"]) +
                          " pending=" + str(rep["pending_24h"]) +
                          " unpriced=" + str(rep["unpriced_signals"]), flush=True)
                    _save(force=True)
                if at - _last_heartbeat_ms >= 120_000:
                    _last_heartbeat_ms = at
                    print("PSI-V13 PERF24_BOARD tracked=" + str(_stats["signals"]) +
                          " priced=" + str(_stats["priced"]) +
                          " unpriced=" + str(_stats["unpriced"]) +
                          " resolved=" + str(_stats["resolved"]) +
                          " pending=" + str(report(at)["pending_24h"]) +
                          " newlyTracked=" + str(added) +
                          " newlyResolved=" + str(len(resolved)) +
                          " learn24=" + str(_model24["trained"]) +
                          " api=BINANCE_1M", flush=True)
                _save()
            except asyncio.CancelledError:
                _save(force=True)
                raise
            except Exception as exc:
                _stats["capture_errors"] += 1
                _stats["last_error"] = "loop:" + type(exc).__name__ + ":" + str(exc)[:120]
                print("PSI-V13 PERF24_ERROR " + _stats["last_error"], flush=True)
            await asyncio.sleep(15)


def install(core, v13):
    global V13, CORE, _ORIGINAL_SCAN, _ORIGINAL_HEALTH
    if CORE is not None:
        return
    CORE, V13 = core, v13
    _ORIGINAL_SCAN = core.v12_scan
    _ORIGINAL_HEALTH = core.v12_health
    core.v12_scan = _scan
    core.v12_health = _health
    core.app.scan_endpoint = _scan
    core.app.health = _health
    _restore()
    print("PSI-V13 PERF24_INSTALLED version=" + REVISION +
          " watchScope=ALL40 strictBuy=UNCHANGED autoOrder=NO", flush=True)
