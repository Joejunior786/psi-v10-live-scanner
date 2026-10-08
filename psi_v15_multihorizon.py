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

REVISION = "15.2.0-trade-scorecard-unlimited"
AUTHORITY = "V15_ML_EXPECTED_VALUE_PLUS_HARD_SAFETY"
STATE_PATH = os.getenv("PSI_V15_STATE_PATH", "/data/psi_v15_1_multihorizon.json")
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
MIN_PROMOTION_TEST_SAMPLES = max(10, int(os.getenv("PSI_V15_MIN_PROMOTION_TEST_SAMPLES", "30")))
MIN_PROMOTION_TEST_WINS = max(2, int(os.getenv("PSI_V15_MIN_PROMOTION_TEST_WINS", "5")))
MIN_PROMOTION_TEST_EV_PCT = max(0.0, float(os.getenv("PSI_V15_MIN_PROMOTION_TEST_EV_PCT", "0.10")))
ROUND_TRIP_FEE_PCT = max(0.0, min(1.0, float(os.getenv("PSI_V15_FEE_PCT", "0.20"))))
SIGNAL_COOLDOWN_MS = max(30 * 60_000, int(os.getenv("PSI_V15_SIGNAL_COOLDOWN_MS", str(6 * 60 * 60_000))))
MAX_PENDING = max(200, int(os.getenv("PSI_V15_MAX_PENDING", "1800")))
MAX_RECENT = max(250, int(os.getenv("PSI_V15_MAX_RECENT", "3500")))
MAX_SIGNAL_JOURNAL = max(500, int(os.getenv("PSI_V15_MAX_SIGNAL_JOURNAL", "5000")))
SIGNAL_REARM_MS = max(15 * 60_000, int(os.getenv("PSI_V15_SIGNAL_REARM_MS", str(60 * 60_000))))
BOARD_LIMIT = max(10, min(40, int(os.getenv("PSI_V15_BOARD_LIMIT", "20"))))
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
_qualified_total = 0
_qualified_symbols = []
_signal_journal = []
_signal_seen = set()
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
        "test_wins": 0,
        "test_losses": 0,
        "test_correct": 0,
        "test_brier_sum": 0.0,
        "test_payoff_win_sum": 0.0,
        "test_payoff_win_n": 0,
        "test_payoff_loss_sum": 0.0,
        "test_payoff_loss_n": 0,
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
    """Train without leaking untouched TEST outcomes into prediction/calibration."""
    p = _raw_probability(model, x)
    split = _split(stamp)
    realised = _f(realised_return)

    if split == "TEST":
        model["test_n"] = int(model.get("test_n") or 0) + 1
        model["test_wins"] = int(model.get("test_wins") or 0) + int(bool(outcome))
        model["test_losses"] = int(model.get("test_losses") or 0) + int(not bool(outcome))
        model["test_correct"] = int(model.get("test_correct") or 0) + int((p >= 0.5) == bool(outcome))
        model["test_brier_sum"] = _f(model.get("test_brier_sum")) + (p - int(bool(outcome))) ** 2
        if outcome:
            payoff = max(0.0, realised)
            if payoff > 0:
                model["test_payoff_win_sum"] = _f(model.get("test_payoff_win_sum")) + min(100.0, payoff)
                model["test_payoff_win_n"] = int(model.get("test_payoff_win_n") or 0) + 1
        else:
            payoff = abs(min(0.0, realised))
            if payoff > 0:
                model["test_payoff_loss_sum"] = _f(model.get("test_payoff_loss_sum")) + min(50.0, payoff)
                model["test_payoff_loss_n"] = int(model.get("test_payoff_loss_n") or 0) + 1
        return

    # TRAIN + VALIDATION may contribute to calibration/payoff estimates.
    bucket = str(min(9, max(0, int(p * 10))))
    cb = model.setdefault("cal_bins", {}).setdefault(bucket, {"n": 0, "wins": 0})
    cb["n"] += 1
    cb["wins"] += int(bool(outcome))
    model["brier_sum"] = _f(model.get("brier_sum")) + (p - int(bool(outcome))) ** 2

    if outcome:
        model["wins"] = int(model.get("wins") or 0) + 1
        payoff = max(0.0, realised)
        if payoff > 0:
            model["payoff_win_sum"] = _f(model.get("payoff_win_sum")) + min(100.0, payoff)
            model["payoff_win_n"] = int(model.get("payoff_win_n") or 0) + 1
    else:
        model["losses"] = int(model.get("losses") or 0) + 1
        payoff = abs(min(0.0, realised))
        if payoff > 0:
            model["payoff_loss_sum"] = _f(model.get("payoff_loss_sum")) + min(50.0, payoff)
            model["payoff_loss_n"] = int(model.get("payoff_loss_n") or 0) + 1

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


def _promotion_validation(model, lane, target):
    """Prospective holdout gate; TEST outcomes never train or calibrate the model."""
    n = int(model.get("test_n") or 0)
    wins = int(model.get("test_wins") or 0)
    losses = int(model.get("test_losses") or 0)
    if wins + losses < n:
        losses = max(losses, n - wins)

    default_loss = 2.5 if lane in {"BEAST", "BREAKOUT"} else 3.5
    wn = int(model.get("test_payoff_win_n") or 0)
    ln = int(model.get("test_payoff_loss_n") or 0)
    avg_win = (_f(model.get("test_payoff_win_sum")) / wn) if wn else target
    avg_loss = (_f(model.get("test_payoff_loss_sum")) / ln) if ln else default_loss
    avg_win = max(target * 0.75, min(max(target, 35.0), avg_win))
    avg_loss = max(0.75, min(15.0, avg_loss))
    win_rate = (wins / n) if n else None
    test_ev = (
        win_rate * avg_win - (1.0 - win_rate) * avg_loss - ROUND_TRIP_FEE_PCT
        if win_rate is not None else None
    )
    brier = (_f(model.get("test_brier_sum")) / n) if n else None
    ready = bool(
        n >= MIN_PROMOTION_TEST_SAMPLES
        and wins >= MIN_PROMOTION_TEST_WINS
        and test_ev is not None
        and test_ev >= MIN_PROMOTION_TEST_EV_PCT
    )
    return {
        "ready": ready,
        "samples": n,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4) if win_rate is not None else None,
        "expected_value_pct": round(test_ev, 3) if test_ev is not None else None,
        "brier": round(brier, 5) if brier is not None else None,
        "minimum_samples": MIN_PROMOTION_TEST_SAMPLES,
        "minimum_wins": MIN_PROMOTION_TEST_WINS,
        "minimum_ev_pct": MIN_PROMOTION_TEST_EV_PCT,
    }


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
            promotion = _promotion_validation(model, lane, target)
            probabilities[horizon][str(int(target))] = {
                "probability": round(p, 4),
                "raw_probability": round(raw, 4),
                "samples": samples,
                "model_source": source,
                "calibration": calibration,
                "expected_value_pct": round(ev, 3),
                "promotion_validation": promotion,
            }
            horizon_minutes = HORIZONS_MS[horizon] / 60_000
            plausibility = 1.0
            if target >= 20 and horizon_minutes < 240 and samples < 100:
                plausibility = 0.55
            elif target >= 10 and horizon_minutes < 60 and samples < 100:
                plausibility = 0.70
            score = ev * plausibility + 0.25 * p + 0.03 * math.log1p(samples)
            score += 0.15 if promotion["ready"] else 0.0
            choices.append((score, ev, p, target, horizon, model, source, samples, calibration, promotion))
    choices.sort(key=lambda z: z[0], reverse=True)
    best = choices[0]
    _, ev, p, target, horizon, model, source, samples, calibration, promotion = best
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
        "promotion_validation": promotion,
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

    if not bool((opp.get("promotion_validation") or {}).get("ready")):
        return "ML SHADOW BUY", False, ["OUT_OF_SAMPLE_VALIDATION_PENDING"]

    return "ML BUY NOW", True, []




def _signal_id(row, at_ms):
    bucket = int(at_ms // SIGNAL_REARM_MS)
    return "|".join((
        str(row.get("symbol") or "").upper(),
        str(row.get("lane") or ""),
        str(row.get("action") or ""),
        str(int(_f(row.get("selected_target_pct")))),
        str(row.get("selected_horizon") or ""),
        str(bucket),
    ))


def _record_trade_signals(rows, at_ms):
    added = 0
    for row in rows:
        if str(row.get("action") or "") not in {"ML BUY NOW", "ML SHADOW BUY"}:
            continue
        entry = _f(row.get("reference_entry"))
        target_price = _f(row.get("selected_target_price"))
        stop = _f(row.get("dynamic_stop"))
        horizon = str(row.get("selected_horizon") or "")
        horizon_ms = int(HORIZONS_MS.get(horizon) or 0)
        if entry <= 0 or target_price <= 0 or horizon_ms <= 0:
            continue
        sid = _signal_id(row, at_ms)
        if sid in _signal_seen:
            continue
        signal = {
            "id": sid,
            "symbol": str(row.get("symbol") or "").upper(),
            "lane": str(row.get("lane") or ""),
            "action": str(row.get("action") or ""),
            "created_ms": int(at_ms),
            "entry_price": entry,
            "target_pct": _f(row.get("selected_target_pct")),
            "target_price": target_price,
            "stop_price": stop if stop > 0 else None,
            "horizon": horizon,
            "horizon_ms": horizon_ms,
            "predicted_time_ms": int(_f(row.get("expected_time_ms"), horizon_ms)),
            "predicted_time_text": str(row.get("expected_time_to_target") or ""),
            "predicted_time_range": str(row.get("expected_time_range") or ""),
            "probability": _f(row.get("probability")),
            "expected_value_pct": _f(row.get("expected_value_pct")),
            "model_samples": int(_f(row.get("model_samples"))),
            "model_source": str(row.get("model_source") or ""),
            "promotion_ready": bool(row.get("promotion_ready")),
            "status": "OPEN",
            "resolved_ms": 0,
            "actual_time_ms": None,
            "time_error_ms": None,
            "hit_within_predicted_time": None,
            "final_return_pct": None,
            "mfe_pct": 0.0,
            "mae_pct": 0.0,
        }
        _signal_journal.append(signal)
        _signal_seen.add(sid)
        added += 1
    if len(_signal_journal) > MAX_SIGNAL_JOURNAL:
        drop = len(_signal_journal) - MAX_SIGNAL_JOURNAL
        removed = _signal_journal[:drop]
        del _signal_journal[:drop]
        for signal in removed:
            _signal_seen.discard(str(signal.get("id") or ""))
    if added:
        _stats["trade_signals_recorded"] += added
    return added


def _update_trade_scorecard(at_ms=None):
    at_ms = _now_ms() if at_ms is None else int(at_ms)
    prices = _prices()
    resolved_now = 0
    for signal in _signal_journal:
        if signal.get("status") != "OPEN":
            continue
        symbol = str(signal.get("symbol") or "").upper()
        price = _f(prices.get(symbol))
        entry = _f(signal.get("entry_price"))
        if price <= 0 or entry <= 0:
            continue
        ret = (price / entry - 1.0) * 100.0
        signal["mfe_pct"] = max(_f(signal.get("mfe_pct")), ret)
        signal["mae_pct"] = min(_f(signal.get("mae_pct")), ret)
        age = max(0, at_ms - int(signal.get("created_ms") or at_ms))
        target = _f(signal.get("target_price"))
        stop = _f(signal.get("stop_price"))
        status = None
        if target > 0 and price >= target:
            status = "TARGET_HIT"
        elif stop > 0 and price <= stop:
            status = "STOP_FIRST"
        elif age >= int(signal.get("horizon_ms") or 0) > 0:
            status = "HORIZON_TIMEOUT"

        if status:
            signal["status"] = status
            signal["resolved_ms"] = at_ms
            signal["actual_time_ms"] = age
            predicted = int(signal.get("predicted_time_ms") or 0)
            signal["time_error_ms"] = (age - predicted) if predicted > 0 else None
            signal["hit_within_predicted_time"] = bool(
                status == "TARGET_HIT" and predicted > 0 and age <= predicted
            )
            signal["final_return_pct"] = round(ret, 4)
            resolved_now += 1
    if resolved_now:
        _stats["trade_signals_resolved"] += resolved_now
    return resolved_now


def _scorecard_for(action=None):
    rows = [
        s for s in _signal_journal
        if action is None or str(s.get("action") or "") == action
    ]
    open_rows = [s for s in rows if s.get("status") == "OPEN"]
    resolved = [s for s in rows if s.get("status") != "OPEN"]
    wins = [s for s in resolved if s.get("status") == "TARGET_HIT"]
    losses = [s for s in resolved if s.get("status") in {"STOP_FIRST", "HORIZON_TIMEOUT"}]
    timed = [s for s in wins if s.get("hit_within_predicted_time") is not None]
    within = [s for s in timed if s.get("hit_within_predicted_time")]
    time_errors = [
        abs(int(s.get("time_error_ms") or 0))
        for s in wins if s.get("time_error_ms") is not None
    ]
    avg_abs_time_error_ms = (
        int(sum(time_errors) / len(time_errors)) if time_errors else None
    )
    return {
        "signals": len(rows),
        "open": len(open_rows),
        "resolved": len(resolved),
        "correct_target_hits": len(wins),
        "incorrect": len(losses),
        "win_rate": round(len(wins) / len(resolved), 4) if resolved else None,
        "target_hit_within_predicted_time": len(within),
        "time_accuracy_rate": round(len(within) / len(timed), 4) if timed else None,
        "avg_abs_time_error_ms": avg_abs_time_error_ms,
        "avg_abs_time_error": _duration(avg_abs_time_error_ms) if avg_abs_time_error_ms is not None else None,
    }


def _trade_scorecard():
    resolved = [s for s in _signal_journal if s.get("status") != "OPEN"]
    recent = sorted(
        resolved, key=lambda s: int(s.get("resolved_ms") or 0), reverse=True
    )[:20]
    return {
        "all": _scorecard_for(),
        "buy_now": _scorecard_for("ML BUY NOW"),
        "shadow_buy": _scorecard_for("ML SHADOW BUY"),
        "recent_resolved": [
            {
                k: s.get(k) for k in (
                    "symbol", "lane", "action", "created_ms", "status",
                    "entry_price", "target_pct", "target_price", "stop_price",
                    "horizon", "predicted_time_text", "actual_time_ms",
                    "hit_within_predicted_time", "final_return_pct",
                    "mfe_pct", "mae_pct", "probability", "expected_value_pct",
                )
            } for s in recent
        ],
    }


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
            "expected_time_ms": int(opp["expected_time_ms"]),
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
            "promotion_ready": bool((opp.get("promotion_validation") or {}).get("ready")),
            "promotion_test_samples": int((opp.get("promotion_validation") or {}).get("samples") or 0),
            "promotion_test_wins": int((opp.get("promotion_validation") or {}).get("wins") or 0),
            "promotion_test_ev_pct": (opp.get("promotion_validation") or {}).get("expected_value_pct"),
            "promotion_test_brier": (opp.get("promotion_validation") or {}).get("brier"),
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
    global _qualified_total, _qualified_symbols
    _qualified_symbols = [
        str(r.get("symbol") or "").upper()
        for r in out if bool(r.get("execution_ready"))
    ]
    _qualified_total = len(_qualified_symbols)
    _record_trade_signals(out, at)
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
        "buy_signal_cap": None,
        "qualified_buy_count_all": _qualified_total,
        "qualified_buy_symbols_all": list(_qualified_symbols),
        "promotion_policy": {
            "out_of_sample_required": True,
            "minimum_test_samples": MIN_PROMOTION_TEST_SAMPLES,
            "minimum_test_wins": MIN_PROMOTION_TEST_WINS,
            "minimum_test_ev_pct": MIN_PROMOTION_TEST_EV_PCT,
            "unvalidated_signal": "ML SHADOW BUY",
        },
        "execution_ready_shown": sum(bool(r.get("execution_ready")) for r in rows),
        "trade_scorecard": _trade_scorecard(),
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
            "signal_journal": _signal_journal[-MAX_SIGNAL_JOURNAL:],
            "signal_seen": list(_signal_seen)[-MAX_SIGNAL_JOURNAL:],
        }
        with open(temp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"), allow_nan=False)
        os.replace(temp, STATE_PATH)
    except (OSError, ValueError, TypeError) as exc:
        _stats["write_errors"] += 1
        _last_error = "save:" + type(exc).__name__


def _restore():
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
        _signal_journal[:] = list(payload.get("signal_journal") or [])[-MAX_SIGNAL_JOURNAL:]
        _signal_seen.clear()
        _signal_seen.update(str(x) for x in (payload.get("signal_seen") or []) if str(x))
        if not _signal_seen:
            _signal_seen.update(str(s.get("id") or "") for s in _signal_journal if s.get("id"))
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
            scored = _update_trade_scorecard()
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
                    f" scoredTrades={scored}"
                    f" top={top or 'NONE'}",
                    flush=True,
                )
                audit = {
                    "revision": REVISION,
                    "generated_ms": _now_ms(),
                    "buy_signal_cap": None,
                    "qualified_buy_count_all": _qualified_total,
                    "qualified_buy_symbols_all": list(_qualified_symbols),
                    "trade_scorecard": _trade_scorecard(),
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
                                "promotion_ready", "promotion_test_samples",
                                "promotion_test_wins", "promotion_test_ev_pct",
                                "promotion_test_brier",
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
        + " oosPromotion=REQUIRED"
        + f" minTest={MIN_PROMOTION_TEST_SAMPLES}/{MIN_PROMOTION_TEST_WINS}"
        + f" boardLimit={BOARD_LIMIT} buySignalCap=NONE"
        + " scorecard=TARGET_STOP_TIMEOUT_AND_TIME_ACCURACY"
        + " orders=DISABLED",
        flush=True,
    )
