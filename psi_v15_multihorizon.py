"""Psi V15 multi-horizon, setup-specific ML decision layer.

Independent ML authority for research signals only. It never places orders and
never weakens hard live-data execution vetoes. The layer learns separate
specialists for BEAST, EXHAUSTION, BREAKOUT and HTF_SWING across multiple
holding horizons and gain targets, estimates empirical expected value,
MFE/MAE and time-to-target, and always publishes a forced top-five ranking.
"""
import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque

REVISION = "15.0.1-multihorizon-daily-cap-audit"
AUTHORITY = "V15_ML_EXPECTED_VALUE_PLUS_HARD_SAFETY"
STATE_PATH = os.getenv("PSI_V15_STATE_PATH", "/data/psi_v15_multihorizon.json")
TARGETS = (3.0, 5.0, 10.0, 20.0)
HORIZONS_MS = {
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "24h": 24 * 60 * 60_000,
    "2d": 2 * 24 * 60 * 60_000,
    "3d": 3 * 24 * 60 * 60_000,
    "7d": 7 * 24 * 60 * 60_000,
}
HORIZON_ORDER = tuple(HORIZONS_MS)
LANES = ("BEAST", "EXHAUSTION", "BREAKOUT", "HTF_SWING")
FEATURE_COUNT = 15
MIN_MODEL_SAMPLES = max(15, int(os.getenv("PSI_V15_MIN_MODEL_SAMPLES", "30")))
MIN_SPECIALIST_SAMPLES = max(10, int(os.getenv("PSI_V15_MIN_SPECIALIST_SAMPLES", "20")))
MIN_BUY_PROBABILITY = max(0.20, min(0.75, float(os.getenv("PSI_V15_MIN_BUY_PROBABILITY", "0.34"))))
MIN_EXPECTED_VALUE_PCT = max(0.0, float(os.getenv("PSI_V15_MIN_EV_PCT", "0.30")))
ROUND_TRIP_FEE_PCT = max(0.0, min(1.0, float(os.getenv("PSI_V15_FEE_PCT", "0.20"))))
SIGNAL_COOLDOWN_MS = max(30 * 60_000, int(os.getenv("PSI_V15_SIGNAL_COOLDOWN_MS", str(6 * 60 * 60_000))))
MAX_PENDING = max(200, int(os.getenv("PSI_V15_MAX_PENDING", "1800")))
MAX_RECENT = max(250, int(os.getenv("PSI_V15_MAX_RECENT", "3500")))
BOARD_LIMIT = 5
MAX_ML_BUYS_PER_DAY = max(1, min(3, int(os.getenv("PSI_V15_MAX_BUYS_PER_DAY", "3"))))
POLL_SECONDS = max(5.0, float(os.getenv("PSI_V15_POLL_SECONDS", "15")))
MAX_SENSOR_AGE_MS = max(250, int(os.getenv("PSI_V15_MAX_SENSOR_AGE_MS", "1200")))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_V15_MAX_SPREAD_BPS", "20")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_V15_MAX_SLIPPAGE_BPS", "35")))
DAY_MS = 24 * 60 * 60_000

CORE = None
V13 = None
OUTCOME = None
V14 = None
_ORIGINAL_SCAN = None
_ORIGINAL_HEALTH = None

_models = {}
_pending = []
_recent = deque(maxlen=MAX_RECENT)
_last_signal = {}
_seen_seed = set()
_lane_stats = {}
_board = []
_last_board_ms = 0
_last_save_ms = 0
_stats = defaultdict(int)
_daily_buy_day = ""
_daily_buy_symbols = set()
_last_error = ""


def _f(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _now_ms():
    return int(time.time() * 1000)


def _split(stamp):
    day = int(stamp // DAY_MS) % 20
    return "TRAIN" if day < 14 else "VALIDATION" if day < 17 else "TEST"


def _model_key(lane, horizon, target):
    return f"{lane}|{horizon}|{int(target)}"


def _new_model():
    return {
        "weights": [0.0] * FEATURE_COUNT,
        "bias": -1.15,
        "trained": 0,
        "wins": 0,
        "losses": 0,
        "test_n": 0,
        "test_correct": 0,
        "brier_sum": 0.0,
        "payoff_win_sum": 0.0,
        "payoff_win_n": 0,
        "payoff_loss_sum": 0.0,
        "payoff_loss_n": 0,
        "cal_bins": {},
    }


def _model(lane, horizon, target):
    key = _model_key(lane, horizon, target)
    row = _models.get(key)
    if not isinstance(row, dict) or len(row.get("weights") or []) != FEATURE_COUNT:
        row = _new_model()
        _models[key] = row
    return row


def _sigmoid(z):
    z = _clamp(z, -20.0, 20.0)
    return 1.0 / (1.0 + math.exp(-z))


def _raw_probability(model, x):
    return _sigmoid(_f(model.get("bias")) + sum(_f(w) * _f(v) for w, v in zip(model["weights"], x)))


def _probability(model, x):
    raw = _raw_probability(model, x)
    bucket = str(min(9, max(0, int(raw * 10))))
    b = (model.get("cal_bins") or {}).get(bucket) or {}
    n = int(b.get("n") or 0)
    if n >= 20:
        empirical = (float(b.get("wins") or 0) + 1.0) / (n + 2.0)
        calibrated = 0.35 * raw + 0.65 * empirical
        source = "RELIABILITY_BIN"
    else:
        wins = int(model.get("wins") or 0)
        losses = int(model.get("losses") or 0)
        total = wins + losses
        if total >= 20:
            empirical = (wins + 2.0) / (total + 4.0)
            calibrated = 0.70 * raw + 0.30 * empirical
            source = "LOGISTIC_BETA_BLEND"
        else:
            calibrated = raw
            source = "LOGISTIC_UNCALIBRATED"
    return _clamp(calibrated, 0.001, 0.999), raw, source


def _update_model(model, x, outcome, stamp, realised_return=None):
    p = _raw_probability(model, x)
    bucket = str(min(9, max(0, int(p * 10))))
    cb = model.setdefault("cal_bins", {}).setdefault(bucket, {"n": 0, "wins": 0})
    cb["n"] += 1
    cb["wins"] += int(bool(outcome))
    model["brier_sum"] = _f(model.get("brier_sum")) + (p - int(bool(outcome))) ** 2

    if outcome:
        model["wins"] = int(model.get("wins") or 0) + 1
        payoff = max(0.0, _f(realised_return))
        if payoff > 0:
            model["payoff_win_sum"] = _f(model.get("payoff_win_sum")) + min(100.0, payoff)
            model["payoff_win_n"] = int(model.get("payoff_win_n") or 0) + 1
    else:
        model["losses"] = int(model.get("losses") or 0) + 1
        payoff = abs(min(0.0, _f(realised_return)))
        if payoff > 0:
            model["payoff_loss_sum"] = _f(model.get("payoff_loss_sum")) + min(50.0, payoff)
            model["payoff_loss_n"] = int(model.get("payoff_loss_n") or 0) + 1

    split = _split(stamp)
    if split == "TEST":
        model["test_n"] = int(model.get("test_n") or 0) + 1
        model["test_correct"] = int(model.get("test_correct") or 0) + int((p >= 0.5) == bool(outcome))
        return
    if split == "VALIDATION":
        return

    eta = 0.055 / math.sqrt(1.0 + int(model.get("trained") or 0) / 250.0)
    delta = int(bool(outcome)) - p
    model["weights"] = [
        _clamp(_f(w) * (1.0 - eta * 0.001) + eta * delta * _f(v), -5.0, 5.0)
        for w, v in zip(model["weights"], x)
    ]
    model["bias"] = _clamp(_f(model.get("bias")) + eta * delta, -7.0, 5.0)
    model["trained"] = int(model.get("trained") or 0) + 1


def _lane_stat(lane, target):
    key = f"{lane}|{int(target)}"
    row = _lane_stats.get(key)
    if not isinstance(row, dict):
        row = {"times_ms": [], "mfe_sum": 0.0, "mae_sum": 0.0, "n": 0}
        _lane_stats[key] = row
    return row


def _record_lane_stats(lane, target, time_ms, mfe, mae):
    row = _lane_stat(lane, target)
    times = row.setdefault("times_ms", [])
    if time_ms > 0:
        times.append(int(time_ms))
        if len(times) > 600:
            del times[:-600]
    row["mfe_sum"] = _f(row.get("mfe_sum")) + _f(mfe)
    row["mae_sum"] = _f(row.get("mae_sum")) + _f(mae)
    row["n"] = int(row.get("n") or 0) + 1


def _target_time(lane, target, horizon_ms):
    row = _lane_stat(lane, target)
    times = sorted(int(x) for x in row.get("times_ms") or [] if int(x) > 0)
    if len(times) >= 8:
        med = int(statistics.median(times))
        q1 = times[max(0, int(0.25 * (len(times) - 1)))]
        q3 = times[min(len(times) - 1, int(0.75 * (len(times) - 1)))]
        return min(med, horizon_ms), min(q1, horizon_ms), min(q3, horizon_ms), "EMPIRICAL"
    med = int(horizon_ms * (0.55 if lane == "BEAST" else 0.65 if lane == "BREAKOUT" else 0.72))
    return med, int(med * 0.55), horizon_ms, "HORIZON_PRIOR"


def _duration(ms):
    seconds = max(0, int(ms // 1000))
    if seconds < 3600:
        minutes = max(1, round(seconds / 60))
        return f"{minutes} min"
    hours = seconds / 3600.0
    if hours < 24:
        value = round(hours, 1)
        return f"{int(value) if value.is_integer() else value} hours"
    days = hours / 24.0
    value = round(days, 1)
    return f"{int(value) if value.is_integer() else value} days"


def _features_from_sensor(row, structural=None):
    row = row or {}
    structural = structural or {}
    return [
        _clamp(_f(row.get("hazard_score")) / 100.0, 0.0, 1.5),
        _clamp((_f(row.get("buy_ratio"), 0.5) - 0.5) * 2.0, -1.0, 1.0),
        _clamp(_f(row.get("relative_volume_10s")) / 10.0, 0.0, 2.0),
        _clamp(_f(row.get("relative_volume_30s")) / 10.0, 0.0, 2.0),
        _clamp(_f(row.get("trade_acceleration")) / 10.0, 0.0, 2.0),
        _clamp(_f(row.get("cvd_acceleration")), -1.0, 1.0),
        _clamp(_f(row.get("ofi")), -1.0, 1.0),
        _clamp(_f(row.get("ofi_acceleration")), -1.0, 1.0),
        _clamp(_f(row.get("obi")), -1.0, 1.0),
        _clamp(_f(row.get("ask_depletion")), -1.0, 1.0),
        _clamp(_f(row.get("sequence_score")) / 100.0, 0.0, 1.0),
        _clamp(_f(row.get("v128_probe_score")) / 100.0, 0.0, 1.0),
        _clamp(_f(row.get("spread_bps"), 100.0) / 20.0, 0.0, 5.0),
        _clamp(_f(structural.get("setup_strength")) / 100.0, 0.0, 1.0),
        1.0 if (
            structural.get("structural_support")
            or structural.get("breakout")
            or structural.get("breakout_near")
            or structural.get("ma_reclaim_regime")
        ) else 0.0,
    ]


def _features_from_outcome(event):
    feat = event.get("features") or {}
    structural = {
        "setup_strength": feat.get("setup_strength"),
        "structural_support": feat.get("structural_support"),
        "breakout": feat.get("breakout"),
        "breakout_near": feat.get("breakout_near"),
        "ma_reclaim_regime": feat.get("ma_reclaim_regime"),
    }
    return _features_from_sensor(
        {
            "hazard_score": feat.get("early_hazard_score", feat.get("hazard_score")),
            "buy_ratio": feat.get("buy_ratio"),
            "relative_volume_10s": feat.get("relative_volume_10s"),
            "relative_volume_30s": feat.get("relative_volume_30s"),
            "trade_acceleration": feat.get("trade_acceleration"),
            "cvd_acceleration": feat.get("cvd_accel", feat.get("cvd_acceleration")),
            "ofi": feat.get("ofi"),
            "ofi_acceleration": feat.get("ofi_accel", feat.get("ofi_acceleration")),
            "obi": feat.get("obi"),
            "ask_depletion": feat.get("ask_depletion"),
            "sequence_score": feat.get("sequence_score"),
            "v128_probe_score": feat.get("v128_probe_score"),
            "spread_bps": feat.get("spread_bps"),
        },
        structural,
    )


def _classify_lane(structural=None, sensor=None, setup=None, features=None):
    structural = structural or {}
    sensor = sensor or {}
    features = features or {}
    name = " ".join([
        str(setup or ""),
        str(structural.get("setup") or ""),
        str(structural.get("setup_type") or ""),
        str(structural.get("signal_class") or ""),
        str(features.get("setup") or ""),
        str(features.get("signal_class") or ""),
    ]).upper()

    htf_words = ("WEEKLY", "DAILY", "200", "EMA", "MA_RECLAIM", "HTF")
    exhaustion_words = ("EXHAUST", "PULLBACK", "SELLER", "RANGE_BOTTOM", "SUPPORT")
    breakout_words = ("BREAKOUT", "RETEST", "COMPRESSION", "COIL", "MONSTER")

    if any(w in name for w in htf_words) or structural.get("weekly_touch") or structural.get("daily_touch"):
        return "HTF_SWING"
    if any(w in name for w in exhaustion_words) or structural.get("structural_support"):
        return "EXHAUSTION"
    if (
        any(w in name for w in breakout_words)
        or structural.get("breakout")
        or structural.get("breakout_near")
        or structural.get("compression")
    ):
        return "BREAKOUT"
    return "BEAST"


def _safety(sensor, at=None):
    at = _now_ms() if at is None else int(at)
    if not sensor:
        return False, ["NO_LIVE_SENSOR"]
    blockers = []
    generated = int(_f(sensor.get("generated_ms"), 0))
    age = at - generated if generated > 0 else 10**12
    trade_age = int(_f(sensor.get("trade_age_ms"), 10**12))
    book_age = int(_f(sensor.get("book_age_ms"), 10**12))
    if not bool(sensor.get("hard_sensor_safety")):
        blockers.append("HARD_SENSOR_SAFETY")
    if age < 0 or age > 15_000:
        blockers.append("STALE_SENSOR_SNAPSHOT")
    if trade_age < 0 or trade_age > MAX_SENSOR_AGE_MS:
        blockers.append("STALE_TRADE")
    if book_age < 0 or book_age > MAX_SENSOR_AGE_MS:
        blockers.append("STALE_BOOK")
    if not bool(sensor.get("sequence_verified")):
        blockers.append("TRADE_SEQUENCE")
    if not bool(sensor.get("book_sequence_verified")):
        blockers.append("BOOK_SEQUENCE")
    spread = _f(sensor.get("spread_bps"), 10**9)
    slippage = _f(sensor.get("slippage_bps"), 10**9)
    if spread <= 0 or spread > MAX_SPREAD_BPS:
        blockers.append("SPREAD")
    if slippage < 0 or slippage > MAX_SLIPPAGE_BPS:
        blockers.append("SLIPPAGE")
    if _f(sensor.get("entry_reference")) <= 0:
        blockers.append("PRICE")
    return not blockers, blockers


def _structural_map():
    try:
        return {
            str(r.get("symbol") or "").upper(): r
            for r in (CORE._board() or [])
            if isinstance(r, dict) and r.get("symbol")
        }
    except Exception:
        return {}


def _sensor_rows():
    try:
        rows = V13._rows() or []
    except Exception:
        rows = []
    return [r for r in rows if isinstance(r, dict) and r.get("symbol")]


def _prices():
    prices = {}
    for row in _sensor_rows():
        sym = str(row.get("symbol") or "").upper()
        px = _f(row.get("entry_reference"))
        if sym and px > 0:
            prices[sym] = px
    try:
        latest = getattr(CORE.q, "latest", {}) or {}
        for sym, row in latest.items():
            if sym in prices:
                continue
            if isinstance(row, dict):
                px = _f(row.get("price"), _f(row.get("last"), _f(row.get("close"))))
            else:
                px = _f(row)
            if px > 0:
                prices[str(sym).upper()] = px
    except Exception:
        pass
    return prices


def _seed_from_outcome_memory():
    if OUTCOME is None:
        return 0
    added = 0
    events = list(getattr(OUTCOME, "_recent", []) or [])
    for event in events:
        if not isinstance(event, dict):
            continue
        eid = str(event.get("id") or f"{event.get('symbol')}|{event.get('created_ms')}")
        if eid in _seen_seed:
            continue
        created = int(_f(event.get("created_ms"), 0))
        if created <= 0:
            continue
        x = _features_from_outcome(event)
        lane = _classify_lane(setup=event.get("setup"), features=event.get("features") or {})
        first = event.get("first_target_ms") or {}
        stop_at = int(_f(event.get("stop_hit_ms"), 0))
        hret = event.get("horizon_returns") or {}
        entry_price = _f(event.get("entry_price"))
        stop_price = _f(event.get("stop_price"))
        stop_return = (
            (stop_price / entry_price - 1.0) * 100.0
            if entry_price > 0 and stop_price > 0 else -3.0
        )
        recorded_targets = set()
        for horizon in HORIZON_ORDER:
            hms = HORIZONS_MS[horizon]
            has_close = horizon in hret
            stop_observed_inside_horizon = bool(
                stop_at and 0 <= stop_at - created <= hms
            )
            if not has_close and not stop_observed_inside_horizon:
                continue
            realised = _f(hret.get(horizon), stop_return)
            for target in TARGETS:
                hit = int(_f(first.get(str(int(target))), 0))
                win = bool(hit and hit - created <= hms and (not stop_at or hit <= stop_at))
                for specialist in (lane, "ALL"):
                    _update_model(_model(specialist, horizon, target), x, win, created, realised)
                target_key = str(int(target))
                if win and target_key not in recorded_targets:
                    _record_lane_stats(
                        lane, target, hit - created,
                        _f(event.get("mfe_pct")), _f(event.get("mae_pct"))
                    )
                    recorded_targets.add(target_key)
        _seen_seed.add(eid)
        added += 1
    if added:
        _stats["seeded_events"] += added
    return added


def _collect_candidates(at=None):
    at = _now_ms() if at is None else int(at)
    structural = _structural_map()
    rows = _sensor_rows()
    rows.sort(
        key=lambda r: (
            _f(r.get("hazard_score"))
            + 8.0 * _f(r.get("relative_volume_10s"))
            + 5.0 * _f(r.get("trade_acceleration"))
            + _f(structural.get(str(r.get("symbol") or "").upper(), {}).get("setup_strength"))
        ),
        reverse=True,
    )
    added = 0
    for sensor in rows[:60]:
        if len(_pending) >= MAX_PENDING:
            break
        sym = str(sensor.get("symbol") or "").upper()
        if not sym:
            continue
        srow = structural.get(sym) or {}
        lane = _classify_lane(srow, sensor)
        cohort = f"{sym}|{lane}"
        if at - int(_last_signal.get(cohort) or 0) < SIGNAL_COOLDOWN_MS:
            continue
        entry = _f(sensor.get("entry_reference"))
        if entry <= 0:
            continue
        _pending.append({
            "id": f"{cohort}|{at}",
            "symbol": sym,
            "lane": lane,
            "created_ms": at,
            "entry_price": entry,
            "x": _features_from_sensor(sensor, srow),
            "first_target_ms": {},
            "stop_at_ms": 0,
            "mfe_pct": 0.0,
            "mae_pct": 0.0,
            "resolved_horizons": [],
            "stats_recorded_targets": [],
        })
        _last_signal[cohort] = at
        added += 1
    _stats["captured"] += added
    return added


def _resolve_pending(at=None):
    at = _now_ms() if at is None else int(at)
    prices = _prices()
    remaining = []
    newly_resolved = 0
    for event in _pending:
        px = prices.get(event["symbol"])
        if px and px > 0:
            ret = (px / _f(event.get("entry_price")) - 1.0) * 100.0
            event["mfe_pct"] = max(_f(event.get("mfe_pct")), ret)
            event["mae_pct"] = min(_f(event.get("mae_pct")), ret)
            for target in TARGETS:
                key = str(int(target))
                if key not in event["first_target_ms"] and ret >= target:
                    event["first_target_ms"][key] = at
            if not event.get("stop_at_ms") and ret <= -3.0:
                event["stop_at_ms"] = at

        age = at - int(event["created_ms"])
        done = set(event.get("resolved_horizons") or [])
        for horizon in HORIZON_ORDER:
            if horizon in done or age < HORIZONS_MS[horizon]:
                continue
            realised = ((px / _f(event.get("entry_price")) - 1.0) * 100.0) if px else None
            if realised is None:
                continue
            for target in TARGETS:
                hit = int(_f(event["first_target_ms"].get(str(int(target))), 0))
                stop = int(_f(event.get("stop_at_ms"), 0))
                win = bool(hit and hit - event["created_ms"] <= HORIZONS_MS[horizon]
                           and (not stop or hit <= stop))
                for specialist in (event["lane"], "ALL"):
                    _update_model(
                        _model(specialist, horizon, target),
                        event["x"], win, event["created_ms"], realised
                    )
                if win and str(int(target)) not in event["stats_recorded_targets"]:
                    _record_lane_stats(
                        event["lane"], target, hit - event["created_ms"],
                        event["mfe_pct"], event["mae_pct"]
                    )
                    event["stats_recorded_targets"].append(str(int(target)))
            event["resolved_horizons"].append(horizon)
            newly_resolved += 1

        if age >= HORIZONS_MS["7d"]:
            _recent.append(dict(event))
        else:
            remaining.append(event)
    _pending[:] = remaining[-MAX_PENDING:]
    _stats["resolved_horizons"] += newly_resolved
    return newly_resolved


def _model_for(lane, horizon, target):
    specialist = _model(lane, horizon, target)
    sn = int(specialist.get("wins") or 0) + int(specialist.get("losses") or 0)
    if sn >= MIN_SPECIALIST_SAMPLES:
        return specialist, lane, sn
    global_model = _model("ALL", horizon, target)
    gn = int(global_model.get("wins") or 0) + int(global_model.get("losses") or 0)
    if gn >= MIN_MODEL_SAMPLES:
        return global_model, "ALL_FALLBACK", gn
    return specialist if sn >= gn else global_model, "LEARNING", max(sn, gn)


def _payoffs(model, target, lane):
    wn = int(model.get("payoff_win_n") or 0)
    ln = int(model.get("payoff_loss_n") or 0)
    avg_win = (_f(model.get("payoff_win_sum")) / wn) if wn else target
    default_loss = 2.5 if lane in {"BEAST", "BREAKOUT"} else 3.5
    avg_loss = (_f(model.get("payoff_loss_sum")) / ln) if ln else default_loss
    avg_win = max(target * 0.75, min(max(target, 35.0), avg_win))
    avg_loss = max(0.75, min(15.0, avg_loss))
    return avg_win, avg_loss


def _opportunity(lane, x):
    choices = []
    probabilities = {}
    for horizon in HORIZON_ORDER:
        probabilities[horizon] = {}
        for target in TARGETS:
            model, source, samples = _model_for(lane, horizon, target)
            p, raw, calibration = _probability(model, x)
            avg_win, avg_loss = _payoffs(model, target, lane)
            ev = p * avg_win - (1.0 - p) * avg_loss - ROUND_TRIP_FEE_PCT
            probabilities[horizon][str(int(target))] = {
                "probability": round(p, 4),
                "raw_probability": round(raw, 4),
                "samples": samples,
                "model_source": source,
                "calibration": calibration,
                "expected_value_pct": round(ev, 3),
            }
            horizon_minutes = HORIZONS_MS[horizon] / 60_000
            plausibility = 1.0
            if target >= 20 and horizon_minutes < 240 and samples < 100:
                plausibility = 0.55
            elif target >= 10 and horizon_minutes < 60 and samples < 100:
                plausibility = 0.70
            score = ev * plausibility + 0.25 * p + 0.03 * math.log1p(samples)
            choices.append((score, ev, p, target, horizon, model, source, samples, calibration))
    choices.sort(key=lambda z: z[0], reverse=True)
    best = choices[0]
    _, ev, p, target, horizon, model, source, samples, calibration = best
    avg_win, avg_loss = _payoffs(model, target, lane)
    med, lo, hi, time_source = _target_time(lane, target, HORIZONS_MS[horizon])
    lane_stat = _lane_stat(lane, target)
    nstat = int(lane_stat.get("n") or 0)
    mfe = _f(lane_stat.get("mfe_sum")) / nstat if nstat else avg_win
    mae = _f(lane_stat.get("mae_sum")) / nstat if nstat else -avg_loss
    return {
        "target_pct": target,
        "horizon": horizon,
        "probability": p,
        "expected_value_pct": ev,
        "expected_win_pct": avg_win,
        "expected_loss_pct": avg_loss,
        "samples": samples,
        "model_source": source,
        "calibration": calibration,
        "expected_time_ms": med,
        "time_low_ms": lo,
        "time_high_ms": hi,
        "time_source": time_source,
        "expected_mfe_pct": mfe,
        "expected_mae_pct": mae,
        "probabilities": probabilities,
    }


def _entry_action(sensor, structural, opp, safe, safety_blockers):
    anti_chase = bool(structural.get("anti_chase"))
    ready_model = opp["samples"] >= MIN_MODEL_SAMPLES or (
        opp["model_source"] not in {"LEARNING"} and opp["samples"] >= MIN_SPECIALIST_SAMPLES
    )
    positive = opp["expected_value_pct"] >= MIN_EXPECTED_VALUE_PCT
    probability_ok = opp["probability"] >= MIN_BUY_PROBABILITY

    if anti_chase:
        return "DO NOT CHASE", False, ["ANTI_CHASE"]
    if not ready_model:
        return "ML LEARNING", False, ["INSUFFICIENT_MULTI_HORIZON_HISTORY"]
    if not positive:
        return "REJECT", False, ["NEGATIVE_EXPECTED_VALUE"]
    if not probability_ok:
        return "WAIT", False, ["PROBABILITY_BELOW_DYNAMIC_FLOOR"]
    if not safe:
        return "WAIT DATA", False, list(safety_blockers)

    lane = _classify_lane(structural, sensor)
    if lane == "BREAKOUT" and structural.get("breakout_near") and not structural.get("breakout"):
        return "BUY BREAKOUT/RETEST", False, ["AWAIT_BREAKOUT_ACCEPTANCE"]
    if lane in {"EXHAUSTION", "HTF_SWING"}:
        support = bool(
            structural.get("structural_support")
            or structural.get("ma_reclaim_regime")
            or structural.get("daily_touch")
            or structural.get("weekly_touch")
        )
        if not support and _f(structural.get("setup_strength")) < 65:
            return "BUY PULLBACK", False, ["PREFER_BETTER_LOCATION"]

    return "ML BUY NOW", True, []



def _utc_day(at_ms):
    return time.strftime("%Y-%m-%d", time.gmtime(int(at_ms) / 1000.0))


def _apply_daily_buy_cap(rows, at_ms):
    """Limit independent ML BUY NOW signals to the user's max trades/day.

    Already-counted symbols may remain BUY NOW; additional otherwise-qualified
    candidates stay visible as WAIT DAILY LIMIT so the ranking is not hidden.
    """
    global _daily_buy_day
    day = _utc_day(at_ms)
    if day != _daily_buy_day:
        _daily_buy_day = day
        _daily_buy_symbols.clear()

    for row in rows:
        if not bool(row.get("execution_ready")):
            continue
        sym = str(row.get("symbol") or "").upper()
        row["qualified_before_daily_limit"] = True
        if sym in _daily_buy_symbols:
            continue
        if len(_daily_buy_symbols) < MAX_ML_BUYS_PER_DAY:
            _daily_buy_symbols.add(sym)
            continue
        row["execution_ready"] = False
        row["action"] = "WAIT DAILY LIMIT"
        row["blockers"] = list(dict.fromkeys(
            list(row.get("blockers") or []) + ["MAX_3_ML_TRADES_PER_DAY"]
        ))
    return rows


def _build_board(at=None):
    global _board, _last_board_ms
    at = _now_ms() if at is None else int(at)
    structural = _structural_map()
    rows = _sensor_rows()
    out = []
    for sensor in rows:
        sym = str(sensor.get("symbol") or "").upper()
        if not sym:
            continue
        srow = structural.get(sym) or {}
        lane = _classify_lane(srow, sensor)
        x = _features_from_sensor(sensor, srow)
        opp = _opportunity(lane, x)
        safe, safety_blockers = _safety(sensor, at)
        action, executable, action_blockers = _entry_action(
            sensor, srow, opp, safe, safety_blockers
        )
        price = _f(sensor.get("entry_reference"))
        loss_pct = max(0.75, opp["expected_loss_pct"])
        stop = price * (1.0 - loss_pct / 100.0) if price > 0 else None
        target_price = price * (1.0 + opp["target_pct"] / 100.0) if price > 0 else None
        expected_time = _duration(opp["expected_time_ms"])
        time_range = f"{_duration(opp['time_low_ms'])}–{_duration(opp['time_high_ms'])}"
        rank_score = (
            opp["expected_value_pct"]
            + 2.0 * opp["probability"]
            + 0.02 * min(100.0, _f(sensor.get("hazard_score")))
            + (0.35 if safe else 0.0)
            - (0.75 if bool(srow.get("anti_chase")) else 0.0)
        )
        out.append({
            "symbol": sym,
            "lane": lane,
            "action": action,
            "execution_ready": executable,
            "authority": AUTHORITY,
            "price": price or None,
            "reference_entry": price or None,
            "dynamic_stop": round(stop, 12) if stop else None,
            "selected_target_pct": opp["target_pct"],
            "selected_target_price": round(target_price, 12) if target_price else None,
            "selected_horizon": opp["horizon"],
            "expected_time_to_target": expected_time,
            "expected_time_range": time_range,
            "time_model_source": opp["time_source"],
            "probability": round(opp["probability"], 4),
            "expected_value_pct": round(opp["expected_value_pct"], 3),
            "expected_mfe_pct": round(opp["expected_mfe_pct"], 3),
            "expected_mae_pct": round(opp["expected_mae_pct"], 3),
            "expected_win_pct": round(opp["expected_win_pct"], 3),
            "expected_loss_pct": round(opp["expected_loss_pct"], 3),
            "model_samples": opp["samples"],
            "model_source": opp["model_source"],
            "probability_calibration": opp["calibration"],
            "multi_horizon_probabilities": opp["probabilities"],
            "hard_safety_verified": safe,
            "blockers": list(dict.fromkeys(action_blockers + ([] if safe else safety_blockers)))[:10],
            "rank_score": round(rank_score, 5),
            "setup_strength": _f(srow.get("setup_strength")),
            "anti_chase": bool(srow.get("anti_chase")),
        })
    out.sort(
        key=lambda r: (
            bool(r["execution_ready"]),
            r["expected_value_pct"],
            r["probability"],
            r["rank_score"],
        ),
        reverse=True,
    )
    _apply_daily_buy_cap(out, at)
    for idx, row in enumerate(out, 1):
        row["rank"] = idx
    _board = out[:BOARD_LIMIT]
    _last_board_ms = at
    return list(_board)


def board():
    if not _board or _now_ms() - _last_board_ms > 30_000:
        return _build_board()
    return list(_board)


def report():
    rows = board()
    return {
        "revision": REVISION,
        "authority": AUTHORITY,
        "order_placement": False,
        "specialists": list(LANES),
        "targets_pct": list(TARGETS),
        "horizons": list(HORIZON_ORDER),
        "decision_rule": "positive empirical EV + setup-specific model + hard live-data safety",
        "fixed_50pct_gate_removed": True,
        "forced_top_five": True,
        "max_ml_trades_per_day": MAX_ML_BUYS_PER_DAY,
        "daily_buy_day_utc": _daily_buy_day or _utc_day(_now_ms()),
        "daily_buy_symbols": sorted(_daily_buy_symbols),
        "execution_ready": sum(bool(r.get("execution_ready")) for r in rows),
        "rows": rows,
        "training": {
            "pending_events": len(_pending),
            "recent_completed": len(_recent),
            "model_count": len(_models),
            "seeded_events": int(_stats.get("seeded_events") or 0),
            "captured": int(_stats.get("captured") or 0),
            "resolved_horizons": int(_stats.get("resolved_horizons") or 0),
        },
        "last_error": _last_error or None,
        "generated_ms": _now_ms(),
    }


def _save(force=False):
    global _last_save_ms, _last_error
    at = _now_ms()
    if not force and at - _last_save_ms < 45_000:
        return
    _last_save_ms = at
    try:
        folder = os.path.dirname(STATE_PATH)
        if folder:
            os.makedirs(folder, exist_ok=True)
        temp = STATE_PATH + ".tmp"
        payload = {
            "models": _models,
            "pending": _pending[-MAX_PENDING:],
            "recent": list(_recent)[-MAX_RECENT:],
            "last_signal": _last_signal,
            "seen_seed": list(_seen_seed)[-5000:],
            "lane_stats": _lane_stats,
            "stats": dict(_stats),
            "daily_buy_day": _daily_buy_day,
            "daily_buy_symbols": sorted(_daily_buy_symbols),
        }
        with open(temp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"), allow_nan=False)
        os.replace(temp, STATE_PATH)
    except (OSError, ValueError, TypeError) as exc:
        _stats["write_errors"] += 1
        _last_error = "save:" + type(exc).__name__


def _restore():
    global _daily_buy_day
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        if not isinstance(payload, dict):
            return
        for key, value in (payload.get("models") or {}).items():
            if isinstance(value, dict) and len(value.get("weights") or []) == FEATURE_COUNT:
                _models[key] = value
        _pending[:] = list(payload.get("pending") or [])[-MAX_PENDING:]
        _recent.clear()
        _recent.extend(list(payload.get("recent") or [])[-MAX_RECENT:])
        _last_signal.clear()
        _last_signal.update(payload.get("last_signal") or {})
        _seen_seed.clear()
        _seen_seed.update(payload.get("seen_seed") or [])
        _lane_stats.clear()
        _lane_stats.update(payload.get("lane_stats") or {})
        _stats.update(payload.get("stats") or {})
        _daily_buy_day = str(payload.get("daily_buy_day") or "")
        _daily_buy_symbols.clear()
        _daily_buy_symbols.update(
            str(x).upper() for x in (payload.get("daily_buy_symbols") or []) if str(x)
        )
        if _daily_buy_day and _daily_buy_day != _utc_day(_now_ms()):
            _daily_buy_day = _utc_day(_now_ms())
            _daily_buy_symbols.clear()
    except (OSError, ValueError, TypeError):
        pass


def _augment(response):
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    payload["v15_ml_multihorizon"] = report()
    return CORE.app.web.json_response(payload, status=response.status)


async def _scan(request):
    return _augment(await _ORIGINAL_SCAN(request))


async def _health(request):
    return _augment(await _ORIGINAL_HEALTH(request))


async def supervisor_loop():
    global _last_error
    cycle = 0
    while True:
        try:
            seeded = _seed_from_outcome_memory()
            added = _collect_candidates()
            resolved = _resolve_pending()
            rows = _build_board()
            cycle += 1
            if cycle % 4 == 1:
                top = ",".join(
                    f"{r['symbol']}:{r['action']}:{r['selected_target_pct']:.0f}%/{r['expected_time_to_target']}"
                    for r in rows
                )
                print(
                    "PSI-V15 ML_MULTI BOARD"
                    f" specialists={len(LANES)}"
                    f" models={len(_models)}"
                    f" seeded={int(_stats.get('seeded_events') or 0)}"
                    f" pending={len(_pending)}"
                    f" exec={sum(bool(r.get('execution_ready')) for r in rows)}"
                    f" newlyTracked={added} newlyResolved={resolved} newlySeeded={seeded}"
                    f" top={top or 'NONE'}",
                    flush=True,
                )
                audit = {
                    "revision": REVISION,
                    "generated_ms": _now_ms(),
                    "daily_buy_day_utc": _daily_buy_day,
                    "daily_buy_symbols": sorted(_daily_buy_symbols),
                    "rows": [
                        {
                            k: r.get(k) for k in (
                                "rank", "symbol", "lane", "action", "execution_ready",
                                "price", "dynamic_stop", "selected_target_pct",
                                "selected_target_price", "selected_horizon",
                                "expected_time_to_target", "expected_time_range",
                                "time_model_source", "probability", "expected_value_pct",
                                "expected_mfe_pct", "expected_mae_pct", "model_samples",
                                "model_source", "probability_calibration",
                                "hard_safety_verified", "blockers",
                            )
                        } for r in rows
                    ],
                }
                print(
                    "PSI-V15 ML_MULTI_JSON "
                    + json.dumps(audit, separators=(",", ":"), allow_nan=False),
                    flush=True,
                )
            _save()
        except asyncio.CancelledError:
            _save(force=True)
            raise
        except Exception as exc:
            _stats["worker_errors"] += 1
            _last_error = f"loop:{type(exc).__name__}:{str(exc)[:120]}"
            print("PSI-V15 ERROR " + _last_error, flush=True)
        await asyncio.sleep(POLL_SECONDS)


def install(core, v13, outcome=None, v14=None):
    global CORE, V13, OUTCOME, V14, _ORIGINAL_SCAN, _ORIGINAL_HEALTH
    if CORE is not None:
        return
    CORE, V13, OUTCOME, V14 = core, v13, outcome, v14
    _restore()
    _seed_from_outcome_memory()
    _ORIGINAL_SCAN = core.v12_scan
    _ORIGINAL_HEALTH = core.v12_health
    core.v12_scan = _scan
    core.v12_health = _health
    core.app.scan_endpoint = _scan
    core.app.health = _health
    print(
        "PSI-V15 INSTALLED revision=" + REVISION
        + " specialists=BEAST/EXHAUSTION/BREAKOUT/HTF_SWING"
        + " horizons=15m,30m,1h,4h,12h,24h,2d,3d,7d"
        + " targets=3,5,10,20"
        + " fixed50Gate=REMOVED EV=DYNAMIC hardSafety=FAIL_CLOSED"
        + f" maxBuysPerDay={MAX_ML_BUYS_PER_DAY}"
        + " orders=DISABLED",
        flush=True,
    )
