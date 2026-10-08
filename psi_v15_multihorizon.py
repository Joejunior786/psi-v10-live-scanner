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

REVISION = "15.11.0-target10-validated"
AUTHORITY = "V15_ML_EXPECTED_VALUE_PLUS_HARD_SAFETY"
STATE_PATH = os.getenv("PSI_V15_STATE_PATH", "/data/psi_v15_10_multihorizon.json")
TARGETS = (3.0, 5.0, 10.0, 20.0)
# Preserve smaller targets for training, but never select them as ML trade targets.
MIN_TRADE_TARGET_PCT = 10.0
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
FEATURE_COUNT = 26
MIN_MODEL_SAMPLES = max(15, int(os.getenv("PSI_V15_MIN_MODEL_SAMPLES", "30")))
MIN_SPECIALIST_SAMPLES = max(10, int(os.getenv("PSI_V15_MIN_SPECIALIST_SAMPLES", "20")))
MIN_BUY_PROBABILITY = max(0.20, min(0.75, float(os.getenv("PSI_V15_MIN_BUY_PROBABILITY", "0.34"))))
MIN_EXPECTED_VALUE_PCT = max(0.0, float(os.getenv("PSI_V15_MIN_EV_PCT", "0.30")))
MIN_PROMOTION_TEST_SAMPLES = max(10, int(os.getenv("PSI_V15_MIN_PROMOTION_TEST_SAMPLES", "30")))
MIN_PROMOTION_TEST_WINS = max(2, int(os.getenv("PSI_V15_MIN_PROMOTION_TEST_WINS", "5")))
MIN_PROMOTION_TEST_EV_PCT = max(0.0, float(os.getenv("PSI_V15_MIN_PROMOTION_TEST_EV_PCT", "0.10")))
ROUND_TRIP_FEE_PCT = max(0.0, min(1.0, float(os.getenv("PSI_V15_FEE_PCT", "0.20"))))
SIGNAL_COOLDOWN_MS = max(30 * 60_000, int(os.getenv("PSI_V15_SIGNAL_COOLDOWN_MS", str(6 * 60 * 60_000))))
SEED_DEDUP_MS = max(60 * 60_000, int(os.getenv("PSI_V15_SEED_DEDUP_MS", str(6 * 60 * 60_000))))
MAX_PENDING = max(200, int(os.getenv("PSI_V15_MAX_PENDING", "1800")))
MAX_RECENT = max(250, int(os.getenv("PSI_V15_MAX_RECENT", "3500")))
MAX_SIGNAL_JOURNAL = max(500, int(os.getenv("PSI_V15_MAX_SIGNAL_JOURNAL", "5000")))
SIGNAL_REARM_MS = max(15 * 60_000, int(os.getenv("PSI_V15_SIGNAL_REARM_MS", str(60 * 60_000))))
BOARD_LIMIT = max(10, min(40, int(os.getenv("PSI_V15_BOARD_LIMIT", "30"))))
POLL_SECONDS = max(5.0, float(os.getenv("PSI_V15_POLL_SECONDS", "15")))
MAX_SENSOR_AGE_MS = max(250, int(os.getenv("PSI_V15_MAX_SENSOR_AGE_MS", "1200")))
MAX_SPREAD_BPS = max(1.0, float(os.getenv("PSI_V15_MAX_SPREAD_BPS", "20")))
MAX_SLIPPAGE_BPS = max(1.0, float(os.getenv("PSI_V15_MAX_SLIPPAGE_BPS", "35")))
DAY_MS = 24 * 60 * 60_000
HISTORICAL_WARM_START_MAX_HORIZON_MS = HORIZONS_MS["24h"]

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
_seed_buckets = set()
_entry_seed_seen = set()
_entry_excursion_stats = {}
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


def _update_model(model, x, outcome, stamp, realised_return=None, split_override=None):
    """Train without leaking untouched TEST outcomes into prediction/calibration."""
    p = _raw_probability(model, x)
    split = str(split_override or _split(stamp)).upper()
    if split not in {"TRAIN", "VALIDATION", "TEST"}:
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


def _stop_stat(lane):
    key = f"STOP|{lane}"
    row = _lane_stats.get(key)
    if not isinstance(row, dict):
        row = {"times_ms": [], "n": 0}
        _lane_stats[key] = row
    return row


def _record_stop_time(lane, time_ms):
    if time_ms <= 0:
        return
    row = _stop_stat(lane)
    times = row.setdefault("times_ms", [])
    times.append(int(time_ms))
    if len(times) > 600:
        del times[:-600]
    row["n"] = int(row.get("n") or 0) + 1


def _invalidation_time(lane, horizon_ms):
    row = _stop_stat(lane)
    times = sorted(int(x) for x in row.get("times_ms") or [] if int(x) > 0)
    if len(times) < 8:
        return None, None, None, "INSUFFICIENT"
    med = int(statistics.median(times))
    q1 = times[max(0, int(0.25 * (len(times) - 1)))]
    q3 = times[min(len(times) - 1, int(0.75 * (len(times) - 1)))]
    cap = max(int(horizon_ms), med)
    return min(med, cap), min(q1, cap), min(q3, cap), "EMPIRICAL"


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


def _duration_class(ms):
    ms = max(0, int(ms or 0))
    if ms <= 60 * 60_000:
        return "SCALP"
    if ms <= 12 * 60 * 60_000:
        return "INTRADAY"
    if ms <= 3 * DAY_MS:
        return "SWING"
    return "EXTENDED_SWING"


def _regime_score(value):
    text_value = str(value or "").upper()
    if "FULL_BULLISH_ALIGNMENT" in text_value:
        return 1.0
    if "WEEKLY_BEARISH_OR_MIXED" in text_value:
        return -1.0
    if "BULL" in text_value:
        return 0.6
    if "BEAR" in text_value:
        return -0.6
    return 0.0


def _timeframe_score(value):
    text_value = str(value or "").upper()
    if "W" in text_value and "1H" not in text_value:
        return 1.0
    if "1D" in text_value or "DAILY" in text_value:
        return 0.85
    if "4H" in text_value:
        return 0.60
    if "1H" in text_value:
        return 0.40
    if "15M" in text_value:
        return 0.20
    return 0.0


def _state_score(value):
    value = str(value or "").upper()
    return 1.0 if value == "BUY" else 0.65 if value == "ARMED" else 0.25 if value == "WATCH" else 0.0


def _entry_geometry(structural, current):
    structural = structural or {}
    current = _f(current)
    low = _f(structural.get("entry_low"), _f(structural.get("entry")))
    high = _f(structural.get("entry_high"), low)
    if low > 0 and high <= 0:
        high = low
    if high > 0 and low <= 0:
        low = high
    if low > high > 0:
        low, high = high, low
    center = (low + high) / 2.0 if low > 0 and high > 0 else 0.0
    distance = ((current / center - 1.0) * 100.0) if current > 0 and center > 0 else 0.0
    risk_pct = _f(structural.get("risk_pct"))
    invalidation = _f(structural.get("invalidation"))
    if risk_pct <= 0 and center > 0 and 0 < invalidation < center:
        risk_pct = (center - invalidation) / center * 100.0
    return {
        "entry_low": low,
        "entry_high": high,
        "entry_center": center,
        "entry_distance_pct": distance,
        "risk_pct": risk_pct,
    }


def _setup_context_flag(structural):
    text_value = " ".join([
        str(structural.get("setup") or ""),
        " ".join(str(x.get("name") or "") for x in (structural.get("active_setups") or []) if isinstance(x, dict)),
    ]).upper()
    tokens = ("EMA", "WEEKLY", "DAILY", "RECLAIM", "SUPPORT", "PULLBACK", "BREAKOUT", "RETEST", "COMPRESSION", "RANGE_BOTTOM", "SWEEP")
    return 1.0 if any(token in text_value for token in tokens) else 0.0


def _features_from_sensor(row, structural=None):
    row = row or {}
    structural = structural or {}
    current = _f(row.get("entry_reference"), _f(structural.get("current")))
    geometry = _entry_geometry(structural, current)
    size_shift = _f(row.get("trade_size_shift"), 1.0)
    if size_shift <= 0:
        size_shift = 1.0
    flow_persistence = _f(row.get("flow_persistence"), _f(row.get("buy_ratio"), 0.5))
    return [
        _clamp(_f(row.get("hazard_score")) / 100.0, 0.0, 1.5),
        _clamp((_f(row.get("buy_ratio"), 0.5) - 0.5) * 2.0, -1.0, 1.0),
        _clamp(_f(row.get("relative_volume_10s")) / 10.0, 0.0, 2.0),
        _clamp(_f(row.get("relative_volume_30s")) / 10.0, 0.0, 2.0),
        _clamp(_f(row.get("trade_acceleration")) / 10.0, 0.0, 2.0),
        _clamp((size_shift - 1.0) / 2.0, -1.0, 1.0),
        _clamp((flow_persistence - 0.5) * 2.0, -1.0, 1.0),
        _clamp(_f(row.get("cvd_acceleration")), -1.0, 1.0),
        _clamp(_f(row.get("ofi")), -1.0, 1.0),
        _clamp(_f(row.get("ofi_acceleration")), -1.0, 1.0),
        _clamp(_f(row.get("obi")), -1.0, 1.0),
        _clamp(_f(row.get("ask_depletion")), -1.0, 1.0),
        _clamp(_f(row.get("spread_bps"), 100.0) / 20.0, 0.0, 5.0),
        _clamp(_f(row.get("slippage_bps"), 100.0) / 35.0, 0.0, 5.0),
        _clamp(_f(structural.get("setup_strength")) / 100.0, 0.0, 1.0),
        _regime_score(structural.get("trend_regime")),
        -1.0 if bool(structural.get("counter_trend")) else 0.0,
        _clamp(geometry["risk_pct"] / 10.0, 0.0, 2.0),
        _clamp(geometry["entry_distance_pct"] / 10.0, -2.0, 2.0),
        _clamp(_f(structural.get("buy_setup_count")) / 5.0, 0.0, 2.0),
        _clamp(_f(structural.get("armed_setup_count")) / 5.0, 0.0, 2.0),
        _timeframe_score(structural.get("timeframe")),
        1.0 if bool(structural.get("extension_blocked")) else 0.0,
        1.0 if bool(structural.get("anti_chase")) else 0.0,
        _state_score(structural.get("state")),
        _setup_context_flag(structural),
    ]


def _features_from_outcome(event):
    feat = event.get("features") or {}
    entry = _f(event.get("entry_price"))
    current = _f(event.get("observed_price_at_signal"), entry)
    structural = {
        "setup": feat.get("setup") or event.get("setup"),
        "setup_strength": feat.get("setup_strength"),
        "timeframe": feat.get("timeframe"),
        "trend_regime": feat.get("trend_regime"),
        "counter_trend": feat.get("counter_trend"),
        "risk_pct": feat.get("risk_pct"),
        "entry_low": feat.get("entry_low", entry),
        "entry_high": feat.get("entry_high", entry),
        "entry": entry,
        "current": current,
        "buy_setup_count": feat.get("buy_setup_count"),
        "armed_setup_count": feat.get("armed_setup_count"),
        "extension_blocked": feat.get("extension_blocked"),
        "anti_chase": feat.get("anti_chase"),
        "state": feat.get("structural_state"),
        "active_setups": feat.get("active_setups") or [],
    }
    return _features_from_sensor(
        {
            "entry_reference": current,
            "hazard_score": feat.get("early_hazard_score", feat.get("hazard_score")),
            "buy_ratio": feat.get("buy_ratio"),
            "relative_volume_10s": feat.get("relative_volume_10s"),
            "relative_volume_30s": feat.get("relative_volume_30s"),
            "trade_acceleration": feat.get("trade_acceleration"),
            "trade_size_shift": feat.get("trade_size_shift", 1.0),
            "flow_persistence": feat.get("flow_persistence", feat.get("buy_ratio", 0.5)),
            "cvd_acceleration": feat.get("cvd_accel", feat.get("cvd_acceleration")),
            "ofi": feat.get("ofi"),
            "ofi_acceleration": feat.get("ofi_accel", feat.get("ofi_acceleration")),
            "obi": feat.get("obi"),
            "ask_depletion": feat.get("ask_depletion"),
            "spread_bps": feat.get("spread_bps"),
            "slippage_bps": feat.get("slippage_bps"),
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
    try:
        sensor_module = getattr(V13, "SENSOR", None)
        raw_cache = getattr(sensor_module, "_sensor_cache", {}) or {}
    except Exception:
        raw_cache = {}
    enriched = []
    for row in rows:
        if not isinstance(row, dict) or not row.get("symbol"):
            continue
        x = dict(row)
        raw = raw_cache.get(str(x.get("symbol") or "").upper()) or {}
        x["flow_persistence"] = _f(
            raw.get("flow_persistence"),
            _f(x.get("buy_ratio"), 0.5),
        )
        enriched.append(x)
    return enriched


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
    events = sorted(
        [e for e in list(getattr(OUTCOME, "_recent", []) or []) if isinstance(e, dict)],
        key=lambda e: int(_f(e.get("created_ms"), 0)),
    )
    initial_seed = not bool(_stats.get("initial_seed_complete"))
    prepared = []
    duplicate_ids = []
    batch_buckets = set(_seed_buckets)

    for event in events:
        eid = str(event.get("id") or f"{event.get('symbol')}|{event.get('created_ms')}")
        if eid in _seen_seed:
            continue
        created = int(_f(event.get("created_ms"), 0))
        if created <= 0:
            _seen_seed.add(eid)
            continue
        lane = _classify_lane(setup=event.get("setup"), features=event.get("features") or {})
        symbol = str(event.get("symbol") or "").upper()
        dedup_key = f"{symbol}|{lane}|{created // SEED_DEDUP_MS}"
        if dedup_key in batch_buckets:
            duplicate_ids.append(eid)
            continue
        batch_buckets.add(dedup_key)
        prepared.append((event, eid, lane, dedup_key))

    split_by_id = {}
    if initial_seed and prepared:
        groups = defaultdict(list)
        for item in prepared:
            groups[item[2]].append(item)
        for lane_items in groups.values():
            lane_items.sort(key=lambda z: int(_f(z[0].get("created_ms"), 0)))
            n = len(lane_items)
            train_cut = max(1, int(math.floor(n * 0.65)))
            val_cut = max(train_cut, int(math.floor(n * 0.75)))
            if n >= 4:
                val_cut = max(train_cut + 1, val_cut)
            val_cut = min(n, val_cut)
            for idx, item in enumerate(lane_items):
                split_by_id[item[1]] = (
                    "TRAIN" if idx < train_cut
                    else "VALIDATION" if idx < val_cut
                    else "TEST"
                )

    for eid in duplicate_ids:
        _seen_seed.add(eid)
        _stats["seed_dedup_skipped"] += 1

    added = 0
    initial_test_events = 0
    for event, eid, lane, dedup_key in prepared:
        created = int(_f(event.get("created_ms"), 0))
        split_override = split_by_id.get(eid)
        if split_override == "TEST":
            initial_test_events += 1
        x = _features_from_outcome(event)
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
            # The inherited outcome-memory source has complete follow-up only
            # through 24h. Do not warm-start 2d/3d/7d models from selectively
            # observed early hits/stops because that would censor neutral/loss
            # cases and inflate long-horizon probabilities. Those horizons
            # learn prospectively from V15's own pending events.
            if hms > HISTORICAL_WARM_START_MAX_HORIZON_MS:
                continue
            has_close = horizon in hret
            stop_observed_inside_horizon = bool(
                stop_at and 0 <= stop_at - created <= hms
            )
            for target in TARGETS:
                hit = int(_f(first.get(str(int(target))), 0))
                win = bool(
                    hit
                    and 0 <= hit - created <= hms
                    and (not stop_at or hit <= stop_at)
                )
                # A target hit before the horizon is already a known positive
                # outcome even when the source event resolved early and never
                # recorded that horizon's closing return. Likewise, a stop
                # before the horizon is a known loss. Unknown censored cases
                # remain excluded instead of being mislabeled.
                if not has_close and not stop_observed_inside_horizon and not win:
                    continue
                realised = _f(
                    hret.get(horizon),
                    float(target) if win else stop_return,
                )
                for specialist in (lane, "ALL"):
                    _update_model(
                        _model(specialist, horizon, target),
                        x, win, created, realised,
                        split_override=split_override,
                    )
                target_key = str(int(target))
                if win and target_key not in recorded_targets:
                    _record_lane_stats(
                        lane, target, hit - created,
                        _f(event.get("mfe_pct")), _f(event.get("mae_pct"))
                    )
                    recorded_targets.add(target_key)
        if stop_at > created:
            _record_stop_time(lane, stop_at - created)

        _seed_buckets.add(dedup_key)
        _seen_seed.add(eid)
        added += 1

    # install() runs before outcome-memory bootstrap. An empty pre-bootstrap
    # call must not consume the one-time chronological holdout; only mark the
    # initial seed complete after real historical events were available.
    if initial_seed and prepared:
        _stats["initial_seed_complete"] = 1
        _stats["initial_oos_test_events"] = initial_test_events
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
            "stop_time_recorded": False,
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
            if event.get("stop_at_ms") and not event.get("stop_time_recorded"):
                _record_stop_time(event["lane"], int(event["stop_at_ms"]) - int(event["created_ms"]))
                event["stop_time_recorded"] = True

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


HORIZON_MIN_BY_LANE_TARGET = {
    "BEAST": {3: "15m", 5: "30m", 10: "1h", 20: "4h"},
    "BREAKOUT": {3: "30m", 5: "1h", 10: "4h", 20: "12h"},
    "EXHAUSTION": {3: "1h", 5: "4h", 10: "12h", 20: "24h"},
    "HTF_SWING": {3: "4h", 5: "12h", 10: "24h", 20: "2d"},
}


def _combo_allowed(lane, target, horizon):
    required = (HORIZON_MIN_BY_LANE_TARGET.get(str(lane)) or {}).get(int(target))
    if not required:
        return True
    return int(HORIZONS_MS.get(str(horizon)) or 0) >= int(HORIZONS_MS.get(required) or 0)


def _selection_model_ready(source, samples):
    samples = int(samples or 0)
    return bool(
        samples >= MIN_MODEL_SAMPLES
        or (str(source or "") != "LEARNING" and samples >= MIN_SPECIALIST_SAMPLES)
    )


def _opportunity(lane, x):
    choices = []
    ready_choices = []
    probabilities = {}
    for horizon in HORIZON_ORDER:
        probabilities[horizon] = {}
        for target in TARGETS:
            if not _combo_allowed(lane, target, horizon):
                continue
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
            score = ev + 0.25 * p + 0.03 * math.log1p(samples)
            score += 0.15 if promotion["ready"] else 0.0
            choice = (score, ev, p, target, horizon, model, source, samples, calibration, promotion)
            if target < MIN_TRADE_TARGET_PCT:
                continue
            choices.append(choice)
            if _selection_model_ready(source, samples):
                ready_choices.append(choice)
    # Untrained long-horizon priors remain visible in the probability grid,
    # but they cannot displace a genuinely trained model. If no trained
    # combination exists yet, fall back to the learning pool so the lane still
    # publishes a research candidate.
    selection_pool = ready_choices or choices
    selection_pool.sort(key=lambda z: z[0], reverse=True)
    best = selection_pool[0]
    _, ev, p, target, horizon, model, source, samples, calibration, promotion = best
    avg_win, avg_loss = _payoffs(model, target, lane)
    med, lo, hi, time_source = _target_time(lane, target, HORIZONS_MS[horizon])
    stop_med, stop_lo, stop_hi, stop_time_source = _invalidation_time(lane, HORIZONS_MS[horizon])
    lane_stat = _lane_stat(lane, target)
    nstat = int(lane_stat.get("n") or 0)
    raw_mfe = _f(lane_stat.get("mfe_sum")) / nstat if nstat else avg_win
    raw_mae = _f(lane_stat.get("mae_sum")) / nstat if nstat else -avg_loss
    # Lane aggregates can contain extreme movers. Keep the display estimate
    # anchored to the selected target/payoff until target+horizon cohorts mature.
    mfe = min(raw_mfe, max(target * 2.0, avg_win * 1.5))
    mae = max(raw_mae, -max(avg_loss * 2.0, 15.0))
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
        "expected_invalidation_time_ms": stop_med,
        "invalidation_time_low_ms": stop_lo,
        "invalidation_time_high_ms": stop_hi,
        "invalidation_time_source": stop_time_source,
        "expected_mfe_pct": mfe,
        "expected_mae_pct": mae,
        "probabilities": probabilities,
    }


def _entry_excursion_stat(lane, target):
    key = f"{lane}|{int(target)}"
    row = _entry_excursion_stats.get(key)
    if not isinstance(row, dict):
        row = {"winner_mae_pct": [], "n": 0}
        _entry_excursion_stats[key] = row
    return row


def _record_entry_excursion(lane, target, mae_pct):
    mae = _clamp(_f(mae_pct), -15.0, 0.0)
    row = _entry_excursion_stat(lane, target)
    values = row.setdefault("winner_mae_pct", [])
    values.append(mae)
    if len(values) > 800:
        del values[:-800]
    row["n"] = int(row.get("n") or 0) + 1


def _seed_entry_excursions():
    if OUTCOME is None:
        return 0
    added = 0
    for event in list(getattr(OUTCOME, "_recent", []) or []):
        if not isinstance(event, dict):
            continue
        eid = str(event.get("id") or f"{event.get('symbol')}|{event.get('created_ms')}")
        if eid in _entry_seed_seen:
            continue
        created = int(_f(event.get("created_ms"), 0))
        first = event.get("first_target_ms") or {}
        mae_at = event.get("mae_at_target") or {}
        stop_at = int(_f(event.get("stop_hit_ms"), 0))
        lane = _classify_lane(setup=event.get("setup"), features=event.get("features") or {})
        for target in TARGETS:
            key = str(int(target))
            hit = int(_f(first.get(key), 0))
            if not hit or hit < created or (stop_at and stop_at < hit):
                continue
            mae = _f(mae_at.get(key), _f(event.get("mae_pct"), 0.0))
            _record_entry_excursion(lane, target, mae)
        _entry_seed_seen.add(eid)
        added += 1
    if added:
        _stats["entry_excursion_seeded_events"] += added
    return added


def _learned_entry_offset(lane, target):
    row = _entry_excursion_stat(lane, target)
    values = sorted(_f(x) for x in (row.get("winner_mae_pct") or []))
    n = len(values)
    if n < 12:
        return 0.0, n, "INSUFFICIENT"
    median_mae = float(statistics.median(values))
    offset = _clamp(0.5 * median_mae, -2.5, 0.0)
    return offset, n, "EMPIRICAL_WINNER_MAE"


def _entry_plan(sensor, structural, lane, opp):
    sensor = sensor or {}
    structural = structural or {}
    current = _f(sensor.get("entry_reference"), _f(structural.get("current")))
    geometry = _entry_geometry(structural, current)
    low = geometry["entry_low"]
    high = geometry["entry_high"]
    center = geometry["entry_center"]
    source = "STRUCTURAL_ZONE" if center > 0 else "LIVE_ML_REFERENCE"
    learned_offset = 0.0
    entry_samples = 0
    if center <= 0 and current > 0:
        learned_offset, entry_samples, learned_source = _learned_entry_offset(
            lane, opp.get("target_pct")
        )
        center = current * (1.0 + learned_offset / 100.0)
        half_width_pct = max(0.20, min(0.75, abs(learned_offset) * 0.35 + 0.20))
        low = center * (1.0 - half_width_pct / 100.0)
        high = center * (1.0 + half_width_pct / 100.0)
        source = learned_source if learned_source != "INSUFFICIENT" else "LIVE_ML_REFERENCE"

    invalidation = _f(structural.get("invalidation"))
    expected_loss = max(0.75, _f(opp.get("expected_loss_pct"), 3.0))
    if invalidation <= 0 or (low > 0 and invalidation >= low):
        invalidation = center * (1.0 - expected_loss / 100.0) if center > 0 else 0.0
        invalidation_source = "LEARNED_EXPECTED_LOSS"
    else:
        invalidation_source = "STRUCTURAL"

    max_chase = _f(structural.get("max_chase"))
    if max_chase <= 0 and high > 0:
        max_chase = high * 1.015

    if current > 0 and invalidation > 0 and current <= invalidation:
        location = "INVALIDATED"
    elif current > 0 and max_chase > 0 and current > max_chase:
        location = "CHASING"
    elif current > 0 and low > 0 and current < low * 0.9975:
        location = "BELOW_ZONE"
    elif current > 0 and high > 0 and current > high * 1.0025:
        location = "ABOVE_ZONE"
    else:
        location = "IN_ZONE"

    return {
        "current": current,
        "entry_low": low or None,
        "entry_high": high or None,
        "entry_center": center or None,
        "entry_zone_source": source,
        "learned_entry_offset_pct": round(learned_offset, 3),
        "entry_model_samples": entry_samples,
        "entry_location": location,
        "entry_distance_pct": (
            round((current / center - 1.0) * 100.0, 3)
            if current > 0 and center > 0 else None
        ),
        "invalidation": invalidation or None,
        "invalidation_source": invalidation_source,
        "max_chase": max_chase or None,
        "risk_pct": geometry["risk_pct"],
    }


def _entry_action(sensor, structural, opp, safe, safety_blockers, entry_plan=None):
    structural = structural or {}
    entry_plan = entry_plan or _entry_plan(
        sensor, structural, _classify_lane(structural, sensor), opp
    )
    location = str(entry_plan.get("entry_location") or "")
    if location == "INVALIDATED":
        return "REJECT", False, ["ENTRY_STRUCTURE_INVALIDATED"]
    if bool(structural.get("anti_chase")) or location == "CHASING":
        return "DO NOT CHASE", False, ["ANTI_CHASE"]

    if _f(opp.get("target_pct")) < MIN_TRADE_TARGET_PCT:
        return "REJECT", False, ["TARGET_BELOW_10_PERCENT"]
    ready_model = opp["samples"] >= MIN_MODEL_SAMPLES or (
        opp["model_source"] not in {"LEARNING"} and opp["samples"] >= MIN_SPECIALIST_SAMPLES
    )
    if not ready_model:
        return "ML LEARNING", False, ["INSUFFICIENT_MULTI_HORIZON_HISTORY"]
    if opp["expected_value_pct"] < MIN_EXPECTED_VALUE_PCT:
        return "REJECT", False, ["NEGATIVE_EXPECTED_VALUE"]
    if opp["probability"] < MIN_BUY_PROBABILITY:
        return "WAIT", False, ["PROBABILITY_BELOW_DYNAMIC_FLOOR"]
    if not safe:
        return "WAIT DATA", False, list(safety_blockers)

    lane = _classify_lane(structural, sensor)
    setup = str(structural.get("setup") or "").upper()
    state = str(structural.get("state") or "").upper()

    if lane == "BREAKOUT" and (
        state in {"ARMED", "WATCH"}
        or ("BREAKOUT" in setup and "RETEST" not in setup and state != "BUY")
    ):
        return "BUY BREAKOUT/RETEST", False, ["AWAIT_BREAKOUT_ACCEPTANCE"]

    if lane in {"EXHAUSTION", "HTF_SWING"} and location == "BELOW_ZONE":
        return "BUY RECLAIM", False, ["AWAIT_RECLAIM_OF_ENTRY_ZONE"]

    if location == "ABOVE_ZONE":
        return "BUY PULLBACK", False, ["PREFER_PULLBACK_TO_ENTRY_ZONE"]

    if location == "BELOW_ZONE":
        return "WAIT ENTRY", False, ["AWAIT_ENTRY_ZONE"]

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


def _action_priority(action):
    return {
        "ML BUY NOW": 10,
        "ML SHADOW BUY": 9,
        "BUY RECLAIM": 8,
        "BUY PULLBACK": 8,
        "BUY BREAKOUT/RETEST": 8,
        "WAIT ENTRY": 7,
        "ML LEARNING": 6,
        "WAIT DATA": 5,
        "WAIT": 4,
        "DO NOT CHASE": 1,
        "REJECT": 0,
    }.get(str(action or ""), 2)


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
        entry_plan = _entry_plan(sensor, srow, lane, opp)
        action, executable, action_blockers = _entry_action(
            sensor, srow, opp, safe, safety_blockers, entry_plan
        )
        price = _f(sensor.get("entry_reference"))
        loss_pct = max(0.75, opp["expected_loss_pct"])
        learned_stop = price * (1.0 - loss_pct / 100.0) if price > 0 else None
        planned_stop = _f(entry_plan.get("invalidation"))
        stop = planned_stop if planned_stop > 0 and (price <= 0 or planned_stop < price) else learned_stop
        target_basis = (
            price if action in {"ML BUY NOW", "ML SHADOW BUY"}
            else _f(entry_plan.get("entry_center"), price)
        )
        target_price = target_basis * (1.0 + opp["target_pct"] / 100.0) if target_basis > 0 else None
        expected_time = _duration(opp["expected_time_ms"])
        time_range = f"{_duration(opp['time_low_ms'])}–{_duration(opp['time_high_ms'])}"
        invalidation_time = (
            _duration(opp["expected_invalidation_time_ms"])
            if opp.get("expected_invalidation_time_ms") is not None else None
        )
        invalidation_time_range = (
            f"{_duration(opp['invalidation_time_low_ms'])}–{_duration(opp['invalidation_time_high_ms'])}"
            if opp.get("invalidation_time_low_ms") is not None and opp.get("invalidation_time_high_ms") is not None
            else None
        )
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
            "entry_low": entry_plan.get("entry_low"),
            "entry_high": entry_plan.get("entry_high"),
            "entry_center": entry_plan.get("entry_center"),
            "entry_zone_source": entry_plan.get("entry_zone_source"),
            "learned_entry_offset_pct": entry_plan.get("learned_entry_offset_pct"),
            "entry_model_samples": entry_plan.get("entry_model_samples"),
            "entry_location": entry_plan.get("entry_location"),
            "entry_distance_pct": entry_plan.get("entry_distance_pct"),
            "max_chase": entry_plan.get("max_chase"),
            "invalidation": entry_plan.get("invalidation"),
            "invalidation_source": entry_plan.get("invalidation_source"),
            "dynamic_stop": round(stop, 12) if stop else None,
            "selected_target_pct": opp["target_pct"],
            "selected_target_basis_price": round(target_basis, 12) if target_basis else None,
            "selected_target_price": round(target_price, 12) if target_price else None,
            "selected_horizon": opp["horizon"],
            "expected_time_to_target": expected_time,
            "expected_time_range": time_range,
            "expected_time_ms": int(opp["expected_time_ms"]),
            "trade_duration_class": _duration_class(opp["expected_time_ms"]),
            "time_model_source": opp["time_source"],
            "expected_time_to_invalidation": invalidation_time,
            "expected_invalidation_time_range": invalidation_time_range,
            "invalidation_time_model_source": opp.get("invalidation_time_source"),
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
            "setup": str(srow.get("setup") or ""),
            "timeframe": str(srow.get("timeframe") or ""),
            "trend_regime": str(srow.get("trend_regime") or ""),
            "setup_strength": _f(srow.get("setup_strength")),
            "anti_chase": bool(srow.get("anti_chase")),
        })
    out.sort(
        key=lambda r: (
            bool(r["execution_ready"]),
            _action_priority(r.get("action")),
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
        "minimum_ml_trade_target_pct": MIN_TRADE_TARGET_PCT,
        "selection_rule": "10%+ target only; smaller targets remain training data; positive EV and out-of-sample validation still required",
        "horizons": list(HORIZON_ORDER),
        "decision_rule": "10%+ selected target + positive empirical EV + out-of-sample validation + hard live-data safety",
        "fixed_50pct_gate_removed": True,
        "forced_top_five": True,
        "feature_schema_version": 4,
        "feature_count": FEATURE_COUNT,
        "setup_specific_horizon_grid": HORIZON_MIN_BY_LANE_TARGET,
        "historical_seed_dedup_window": _duration(SEED_DEDUP_MS),
        "initial_holdout_policy": "CHRONOLOGICAL_PER_LANE_65_TRAIN_10_VALIDATION_25_TEST",
        "historical_warm_start_max_horizon": "24h",
        "long_horizons_2d_3d_7d": "PROSPECTIVE_V15_ONLY",
        "entry_actions": ["ML BUY NOW", "ML SHADOW BUY", "BUY PULLBACK", "BUY RECLAIM", "BUY BREAKOUT/RETEST", "WAIT", "REJECT", "DO NOT CHASE"],
        "time_to_invalidation_enabled": True,
        "learned_entry_zone_enabled": True,
        "learned_entry_zone_method": "50% of median winner MAE capped at -2.5%; structural zone preferred",
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
            "seed_dedup_skipped": int(_stats.get("seed_dedup_skipped") or 0),
            "initial_oos_test_events": int(_stats.get("initial_oos_test_events") or 0),
            "entry_excursion_seeded_events": int(_stats.get("entry_excursion_seeded_events") or 0),
            "entry_excursion_cohorts": len(_entry_excursion_stats),
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
            "seed_buckets": list(_seed_buckets)[-5000:],
            "entry_seed_seen": list(_entry_seed_seen)[-5000:],
            "entry_excursion_stats": _entry_excursion_stats,
            "feature_schema_version": 4,
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
        _seed_buckets.clear()
        _seed_buckets.update(payload.get("seed_buckets") or [])
        _entry_seed_seen.clear()
        _entry_seed_seen.update(payload.get("entry_seed_seen") or [])
        _entry_excursion_stats.clear()
        _entry_excursion_stats.update(payload.get("entry_excursion_stats") or {})
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
            _seed_entry_excursions()
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
                                "price", "entry_low", "entry_high", "entry_center",
                                "entry_zone_source", "learned_entry_offset_pct", "entry_model_samples",
                                "entry_location", "entry_distance_pct",
                                "max_chase", "invalidation", "invalidation_source", "dynamic_stop",
                                "selected_target_pct", "selected_target_basis_price",
                                "selected_target_price", "selected_horizon",
                                "expected_time_to_target", "expected_time_range",
                                "trade_duration_class", "time_model_source",
                                "expected_time_to_invalidation", "expected_invalidation_time_range",
                                "invalidation_time_model_source",
                                "probability", "expected_value_pct",
                                "expected_mfe_pct", "expected_mae_pct", "model_samples",
                                "model_source", "probability_calibration",
                                "promotion_ready", "promotion_test_samples",
                                "promotion_test_wins", "promotion_test_ev_pct",
                                "promotion_test_brier",
                                "hard_safety_verified", "setup", "timeframe", "trend_regime", "blockers",
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
    _seed_entry_excursions()
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
        + " trainedTargets=3,5,10,20 selectionTargets=10,20"
        + " fixed50Gate=REMOVED EV=DYNAMIC hardSafety=FAIL_CLOSED"
        + " oosPromotion=REQUIRED chronologicalHoldout=65/10/25 bootstrapSafeHoldout=ENABLED knownTargetLabels=ENABLED warmStartMax=24h longHorizons=PROSPECTIVE_ONLY trainedModelPriority=ENABLED contextFeatures=HTF/REGIME/ENTRY_GEOMETRY seedDedup=6H horizonGrid=SETUP_SPECIFIC entryZone=LEARNED_WINNER_MAE"
        + f" minTest={MIN_PROMOTION_TEST_SAMPLES}/{MIN_PROMOTION_TEST_WINS}"
        + f" boardLimit={BOARD_LIMIT} buySignalCap=NONE"
        + " scorecard=TARGET_STOP_TIMEOUT_AND_TIME_ACCURACY"
        + " orders=DISABLED",
        flush=True,
    )
