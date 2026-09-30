import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque

import ignition110_entry as base

scanner = base.scanner

VERSION = "10.14.0-learning-velocity-resistance-book"

# -----------------------------------------------------------------------------
# V10.14 intelligence layer
# 1) outcome/post-mortem learning
# 2) ignition velocity + per-symbol anomaly percentiles
# 3) resistance weakness + persistent-book / spoof-like-instability heuristic
# Formal PRE-IGNITION and BUY NOW gates are inherited unchanged from V10.13.
# -----------------------------------------------------------------------------

HISTORY_MAXLEN = 720
HISTORY_SAMPLE_SECONDS = 5.0
VELOCITY_LOOKBACK_SECONDS = 120.0
RESISTANCE_LOOKBACK_SECONDS = 1800.0
RESISTANCE_ATTACK_ZONE_PCT = 0.60
RESISTANCE_RESET_ZONE_PCT = 1.00
OUTCOME_HORIZONS = (
    ("5m", 300),
    ("15m", 900),
    ("30m", 1800),
    ("1h", 3600),
    ("2h", 7200),
    ("4h", 14400),
)
OUTCOME_RECORD_COOLDOWN = 300.0
OUTCOME_MAX_PENDING = 1200
OUTCOME_MAX_RESOLVED = 2000

metric_histories = defaultdict(lambda: {
    "rapid": deque(maxlen=HISTORY_MAXLEN),
    "vol": deque(maxlen=HISTORY_MAXLEN),
    "trade": deque(maxlen=HISTORY_MAXLEN),
    "ofi": deque(maxlen=HISTORY_MAXLEN),
    "obi": deque(maxlen=HISTORY_MAXLEN),
})
last_history_sample = defaultdict(float)
resistance_price_history = defaultdict(lambda: deque(maxlen=HISTORY_MAXLEN))

outcome_pending = defaultdict(list)
outcome_resolved = deque(maxlen=OUTCOME_MAX_RESOLVED)
outcome_last_record = defaultdict(float)
outcome_sequence = 0


def _f(value, default=None):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _clip(value, lo, hi):
    return max(lo, min(hi, value))


def _percentile(values, value):
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    if value is None or len(vals) < 24:
        return 0.50
    return sum(1 for x in vals if x <= value) / len(vals)


def _prune_time_series(dq, cutoff):
    while dq and dq[0][0] < cutoff:
        dq.popleft()


def _rapid_metric(symbol, row):
    try:
        return base._rapid_metric(symbol, row) or {}
    except Exception:
        return row.get("rapid_ignition") or {}


def _record_metric_sample(symbol, row, rapid, now_t):
    if now_t - last_history_sample[symbol] < HISTORY_SAMPLE_SECONDS:
        return
    last_history_sample[symbol] = now_t

    h = metric_histories[symbol]
    values = {
        "rapid": _f(rapid.get("score"), 0.0) or 0.0,
        "vol": max(
            _f(rapid.get("vol_accel_5"), 0.0) or 0.0,
            _f(rapid.get("vol_accel_15"), 0.0) or 0.0,
            _f(row.get("relative_volume_30s"), 0.0) or 0.0,
        ),
        "trade": max(
            _f(rapid.get("trade_accel_5"), 0.0) or 0.0,
            _f(rapid.get("trade_accel_15"), 0.0) or 0.0,
            _f(row.get("trade_acceleration"), 0.0) or 0.0,
        ),
        "ofi": _f(row.get("ofi"), 0.0) or 0.0,
        "obi": _f(row.get("obi"), 0.0) or 0.0,
    }
    for key, value in values.items():
        h[key].append((now_t, value))
        _prune_time_series(h[key], now_t - 6 * 3600)

    entry = base._entry_telemetry(scanner.app, scanner.q, symbol, row)
    price = _f(entry.get("price"), None)
    resistance = _f(entry.get("resistance"), None)
    if price and resistance and price > 0 and resistance > 0:
        rh = resistance_price_history[symbol]
        rh.append((now_t, price, resistance))
        _prune_time_series(rh, now_t - RESISTANCE_LOOKBACK_SECONDS)


def _ignition_velocity(symbol):
    series = list(metric_histories[symbol]["rapid"])
    if len(series) < 3:
        return {"velocity_per_min": 0.0, "acceleration_per_min2": 0.0, "samples": len(series)}

    now_t = series[-1][0]
    points = [(t, v) for t, v in series if t >= now_t - VELOCITY_LOOKBACK_SECONDS]
    if len(points) < 3:
        points = series[-3:]

    def slope(chunk):
        if len(chunk) < 2:
            return 0.0
        dt = max(chunk[-1][0] - chunk[0][0], 1.0)
        return (chunk[-1][1] - chunk[0][1]) / dt * 60.0

    velocity = slope(points)
    mid = max(1, len(points) // 2)
    early = slope(points[: mid + 1])
    late = slope(points[mid:])
    acceleration = late - early
    return {
        "velocity_per_min": round(velocity, 3),
        "acceleration_per_min2": round(acceleration, 3),
        "samples": len(points),
    }


def _symbol_percentiles(symbol, row, rapid):
    h = metric_histories[symbol]
    current = {
        "rapid": _f(rapid.get("score"), 0.0) or 0.0,
        "vol": max(
            _f(rapid.get("vol_accel_5"), 0.0) or 0.0,
            _f(rapid.get("vol_accel_15"), 0.0) or 0.0,
            _f(row.get("relative_volume_30s"), 0.0) or 0.0,
        ),
        "trade": max(
            _f(rapid.get("trade_accel_5"), 0.0) or 0.0,
            _f(rapid.get("trade_accel_15"), 0.0) or 0.0,
            _f(row.get("trade_acceleration"), 0.0) or 0.0,
        ),
        "ofi": _f(row.get("ofi"), 0.0) or 0.0,
        "obi": _f(row.get("obi"), 0.0) or 0.0,
    }

    out = {}
    for key, value in current.items():
        vals = [v for _, v in h[key]]
        out[f"{key}_percentile"] = round(_percentile(vals, value), 4)
        out[f"{key}_samples"] = len(vals)
    return out


def _book_persistence(symbol):
    app = scanner.app
    state = app.micro_state.get(symbol) or {}
    now_ms = app.now_ms()
    cutoff = now_ms - 60_000

    def values(key):
        return [float(v) for ts, v in list(state.get(key) or []) if ts >= cutoff and math.isfinite(float(v))]

    obis = values("obi")
    ofis = values("ofi")
    asks = values("ask_depletion")
    if not obis or not ofis:
        return {
            "score": 0.0,
            "obi_positive_ratio": 0.0,
            "ofi_positive_ratio": 0.0,
            "ask_depletion_positive_ratio": 0.0,
            "obi_sign_flip_ratio": 0.0,
            "obi_std": 0.0,
            "instability_risk": False,
            "sample_count": 0,
            "label": "INSUFFICIENT_BOOK_HISTORY",
        }

    obi_pos = sum(1 for x in obis if x > 0.03) / len(obis)
    ofi_pos = sum(1 for x in ofis if x > 0.0) / len(ofis)
    ask_pos = (sum(1 for x in asks if x > 0.0) / len(asks)) if asks else 0.0
    flips = 0
    comparable = 0
    for a, b in zip(obis, obis[1:]):
        if abs(a) < 0.03 and abs(b) < 0.03:
            continue
        comparable += 1
        if (a > 0) != (b > 0):
            flips += 1
    flip_ratio = flips / comparable if comparable else 0.0
    obi_std = statistics.pstdev(obis) if len(obis) >= 2 else 0.0

    score = 100.0 * (
        0.35 * obi_pos
        + 0.35 * ofi_pos
        + 0.20 * ask_pos
        + 0.10 * (1.0 - min(1.0, flip_ratio))
    )
    score = _clip(score, 0.0, 100.0)

    # This is deliberately called an instability heuristic, not proof of spoofing.
    instability = bool(
        len(obis) >= 12
        and flip_ratio >= 0.45
        and obi_std >= 0.18
        and obi_pos < 0.60
    )
    if instability:
        score = max(0.0, score - 25.0)

    if instability:
        label = "SPOOF_LIKE_BOOK_INSTABILITY"
    elif score >= 70:
        label = "PERSISTENT_BULLISH_BOOK"
    elif score >= 50:
        label = "MIXED_BOOK"
    else:
        label = "WEAK_BOOK_PERSISTENCE"

    return {
        "score": round(score, 2),
        "obi_positive_ratio": round(obi_pos, 4),
        "ofi_positive_ratio": round(ofi_pos, 4),
        "ask_depletion_positive_ratio": round(ask_pos, 4),
        "obi_sign_flip_ratio": round(flip_ratio, 4),
        "obi_std": round(obi_std, 4),
        "instability_risk": instability,
        "sample_count": min(len(obis), len(ofis)),
        "label": label,
    }


def _resistance_attacks(symbol):
    rows = list(resistance_price_history[symbol])
    if not rows:
        return 0
    attacks = 0
    armed = True
    for _, price, resistance in rows:
        if price <= 0 or resistance <= 0:
            continue
        dist = (resistance - price) / price * 100.0
        if dist >= RESISTANCE_RESET_ZONE_PCT:
            armed = True
        elif armed and -0.30 <= dist <= RESISTANCE_ATTACK_ZONE_PCT:
            attacks += 1
            armed = False
    return attacks


def _resistance_quality(symbol, row, book):
    sd = scanner.app.structure.get(symbol) or {}
    entry = base._entry_telemetry(scanner.app, scanner.q, symbol, row)
    distance = _f(entry.get("breakout_distance_pct"), None)
    attacks = _resistance_attacks(symbol)
    compression_ratio = _f(sd.get("compression_ratio"), 1.0) or 1.0
    volume15 = _f(sd.get("volume_acceleration_15m"), 0.0) or 0.0

    proximity_points = 0.0
    if distance is not None and -0.30 <= distance <= 10.0:
        proximity_points = 20.0 * (1.0 - _clip(max(distance, 0.0) / 10.0, 0.0, 1.0))

    attack_points = min(28.0, attacks * 7.0)
    compression_points = 14.0 if compression_ratio <= 0.82 else 8.0 if compression_ratio <= 0.95 else 0.0
    volume_points = min(12.0, max(0.0, volume15 - 1.0) * 20.0)
    book_points = min(16.0, float(book.get("score") or 0.0) * 0.16)
    ask_points = min(10.0, float(book.get("ask_depletion_positive_ratio") or 0.0) * 10.0)

    score = proximity_points + attack_points + compression_points + volume_points + book_points + ask_points
    if book.get("instability_risk"):
        score -= 15.0
    score = _clip(score, 0.0, 100.0)

    return {
        "score": round(score, 2),
        "attack_count_30m": int(attacks),
        "distance_pct": distance,
        "compression_ratio": round(compression_ratio, 4),
        "volume_acceleration_15m": round(volume15, 4),
        "label": (
            "WEAKENING_RESISTANCE" if score >= 70
            else "PRESSURED_RESISTANCE" if score >= 50
            else "UNPROVEN_RESISTANCE_WEAKNESS"
        ),
    }


def _v1014_intelligence(symbol, row):
    now_t = time.time()
    rapid = _rapid_metric(symbol, row)
    _record_metric_sample(symbol, row, rapid, now_t)

    velocity = _ignition_velocity(symbol)
    percentiles = _symbol_percentiles(symbol, row, rapid)
    book = _book_persistence(symbol)
    resistance = _resistance_quality(symbol, row, book)

    base_score = _f(row.get("ignition15_score"), 0.0) or 0.0
    velocity_bonus = _clip(max(0.0, velocity["velocity_per_min"]) / 10.0, 0.0, 8.0)
    accel_bonus = _clip(max(0.0, velocity["acceleration_per_min2"]) / 15.0, 0.0, 5.0)

    anomaly_mean = (
        percentiles["rapid_percentile"]
        + percentiles["vol_percentile"]
        + percentiles["trade_percentile"]
        + percentiles["ofi_percentile"]
        + percentiles["obi_percentile"]
    ) / 5.0
    percentile_bonus = _clip((anomaly_mean - 0.50) * 20.0, 0.0, 10.0)
    resistance_bonus = float(resistance["score"]) * 0.08
    book_bonus = float(book["score"]) * 0.06
    instability_penalty = 15.0 if book.get("instability_risk") else 0.0

    enhanced_score = _clip(
        base_score + velocity_bonus + accel_bonus + percentile_bonus + resistance_bonus + book_bonus - instability_penalty,
        0.0,
        100.0,
    )

    if (
        enhanced_score >= 82
        and velocity["velocity_per_min"] > 0
        and resistance["score"] >= 55
        and book["score"] >= 50
        and not book.get("instability_risk")
    ):
        grade = "PRIME"
    elif enhanced_score >= 72 and not book.get("instability_risk"):
        grade = "STRONG"
    elif enhanced_score >= 60:
        grade = "DEVELOPING"
    else:
        grade = "LOW"

    return {
        "score": round(enhanced_score, 2),
        "grade": grade,
        "velocity": velocity,
        "percentiles": percentiles,
        "book": book,
        "resistance": resistance,
        "base_ignition15_score": round(base_score, 2),
    }


def _current_price(symbol, row=None):
    if row:
        p = _f(row.get("price"), None)
        if p and p > 0:
            return p
    try:
        return _f(scanner.app.current_symbol_price(symbol), 0.0) or 0.0
    except Exception:
        source = scanner.q.latest.get(symbol) or {}
        return _f(source.get("price"), 0.0) or 0.0


def _snapshot_for_outcome(symbol, row, intel):
    entry = base._entry_telemetry(scanner.app, scanner.q, symbol, row)
    return {
        "formal_state": row.get("formal_state", row.get("state")),
        "display_state": row.get("state"),
        "score": _f(row.get("score"), 0.0) or 0.0,
        "ignition15_score": _f(row.get("ignition15_score"), 0.0) or 0.0,
        "v1014_score": intel["score"],
        "v1014_grade": intel["grade"],
        "ignition_velocity_per_min": intel["velocity"]["velocity_per_min"],
        "ignition_acceleration_per_min2": intel["velocity"]["acceleration_per_min2"],
        "percentiles": intel["percentiles"],
        "book_persistence": intel["book"],
        "resistance_quality": intel["resistance"],
        "breakout_distance_pct": entry.get("breakout_distance_pct"),
        "entry_status": entry.get("entry_status"),
        "layers": row.get("layer_results") or {},
        "failed_hard": list(row.get("failed_hard", []) or []),
        "market_regime": row.get("market_regime"),
    }


def _should_record_outcome(row, intel):
    formal = str(row.get("formal_state") or row.get("state") or "")
    display = str(row.get("state") or "")
    if display == "15% IGNITION WATCH":
        return True
    if formal in ("PRE-IGNITION", "BUY NOW"):
        return True
    if formal == "EARLY OPPORTUNITY" and intel["score"] >= 65:
        return True
    return False


def _update_outcomes_for_symbol(symbol, price, now_t):
    if price <= 0:
        return
    events = outcome_pending.get(symbol)
    if not events:
        return

    keep = []
    for event in events:
        entry = event["entry_price"]
        if entry <= 0:
            continue
        ret = (price / entry - 1.0) * 100.0
        event["mfe_pct"] = max(event.get("mfe_pct", ret), ret)
        event["mae_pct"] = min(event.get("mae_pct", ret), ret)
        event["last_return_pct"] = ret
        age = now_t - event["entry_time"]

        for label, seconds in OUTCOME_HORIZONS:
            if age >= seconds and label not in event["horizons"]:
                event["horizons"][label] = {
                    "return_pct": round(ret, 4),
                    "mfe_pct": round(event["mfe_pct"], 4),
                    "mae_pct": round(event["mae_pct"], 4),
                }

        if age >= OUTCOME_HORIZONS[-1][1]:
            event["resolved_time"] = now_t
            outcome_resolved.append(event)
            print("Ψ-V10.14 OUTCOME_RESOLVED " + json.dumps(event, separators=(",", ":"), default=str), flush=True)
        else:
            keep.append(event)
    outcome_pending[symbol] = keep


def _record_outcome_candidate(symbol, row, intel):
    global outcome_sequence
    now_t = time.time()
    price = _current_price(symbol, row)
    _update_outcomes_for_symbol(symbol, price, now_t)
    if price <= 0 or not _should_record_outcome(row, intel):
        return

    key = (symbol, str(row.get("state")), intel["grade"])
    if now_t - outcome_last_record[key] < OUTCOME_RECORD_COOLDOWN:
        return
    outcome_last_record[key] = now_t
    outcome_sequence += 1

    event = {
        "id": outcome_sequence,
        "symbol": symbol,
        "entry_time": now_t,
        "entry_price": price,
        "snapshot": _snapshot_for_outcome(symbol, row, intel),
        "horizons": {},
        "mfe_pct": 0.0,
        "mae_pct": 0.0,
        "last_return_pct": 0.0,
    }
    outcome_pending[symbol].append(event)

    # Hard cap pending memory, oldest first across each symbol bucket.
    total = sum(len(v) for v in outcome_pending.values())
    if total > OUTCOME_MAX_PENDING:
        oldest_symbol = None
        oldest_time = float("inf")
        for s, events in outcome_pending.items():
            if events and events[0]["entry_time"] < oldest_time:
                oldest_time = events[0]["entry_time"]
                oldest_symbol = s
        if oldest_symbol is not None:
            outcome_pending[oldest_symbol].pop(0)


def _learning_summary():
    rows = list(outcome_resolved)
    by_grade = {}
    for grade in ("PRIME", "STRONG", "DEVELOPING", "LOW"):
        group = [e for e in rows if e.get("snapshot", {}).get("v1014_grade") == grade]
        if not group:
            continue
        by_grade[grade] = {
            "n": len(group),
            "mfe_ge_5_pct": round(sum(1 for e in group if e.get("mfe_pct", 0) >= 5) / len(group), 4),
            "mfe_ge_10_pct": round(sum(1 for e in group if e.get("mfe_pct", 0) >= 10) / len(group), 4),
            "mfe_ge_15_pct": round(sum(1 for e in group if e.get("mfe_pct", 0) >= 15) / len(group), 4),
            "avg_mfe_pct": round(sum(float(e.get("mfe_pct", 0)) for e in group) / len(group), 4),
            "avg_mae_pct": round(sum(float(e.get("mae_pct", 0)) for e in group) / len(group), 4),
        }
    return by_grade


def _install_v1014():
    app = scanner.app
    s = scanner.s
    q = scanner.q

    original_evaluate = app.evaluate_symbol
    original_near_diag = s.near_diag

    def evaluate_v1014(symbol):
        row = original_evaluate(symbol)
        if not row:
            return row
        intel = _v1014_intelligence(symbol, row)
        row["v1014_score"] = intel["score"]
        row["v1014_grade"] = intel["grade"]
        row["ignition_velocity_per_min"] = intel["velocity"]["velocity_per_min"]
        row["ignition_acceleration_per_min2"] = intel["velocity"]["acceleration_per_min2"]
        row["anomaly_percentiles"] = intel["percentiles"]
        row["book_persistence"] = intel["book"]
        row["book_instability_risk"] = bool(intel["book"].get("instability_risk"))
        row["resistance_quality"] = intel["resistance"]
        row["resistance_weakness_score"] = intel["resistance"]["score"]

        # Never alter formal PRE/BUY. For the early 15% watch only, flag severe
        # book instability so ranking can demote it instead of trusting one OBI spike.
        row["ignition15_quality_pass"] = not row["book_instability_risk"]
        _record_outcome_candidate(symbol, row, intel)
        return row

    def near_diag_v1014(limit=10):
        rows = original_near_diag(max(limit, 40))
        out = []
        for d0 in rows:
            d = dict(d0)
            symbol = d.get("symbol")
            source = q.latest.get(symbol) or {}
            d["v1014_score"] = _f(source.get("v1014_score"), 0.0) or 0.0
            d["v1014_grade"] = source.get("v1014_grade", "LOW")
            d["ignition_velocity_per_min"] = _f(source.get("ignition_velocity_per_min"), 0.0) or 0.0
            d["ignition_acceleration_per_min2"] = _f(source.get("ignition_acceleration_per_min2"), 0.0) or 0.0
            d["book_persistence"] = source.get("book_persistence") or {}
            d["book_instability_risk"] = bool(source.get("book_instability_risk"))
            d["resistance_quality"] = source.get("resistance_quality") or {}
            d["resistance_weakness_score"] = _f(source.get("resistance_weakness_score"), 0.0) or 0.0
            out.append(d)

        grade_rank = {"PRIME": 4, "STRONG": 3, "DEVELOPING": 2, "LOW": 1}
        out.sort(
            key=lambda d: (
                base._state_rank(d.get("state") or d.get("pre_warmup_state")),
                not bool(d.get("book_instability_risk")),
                grade_rank.get(str(d.get("v1014_grade")), 0),
                float(d.get("v1014_score") or 0.0),
                float(d.get("resistance_weakness_score") or 0.0),
                float(d.get("ignition_velocity_per_min") or 0.0),
                base._breakout_rank(d),
                float(d.get("score") or 0.0),
            ),
            reverse=True,
        )
        return out[:limit]

    app.evaluate_symbol = evaluate_v1014
    s.near_diag = near_diag_v1014
    scanner.VERSION = VERSION

    print(
        "Ψ-V10.14 INTELLIGENCE UPGRADE ACTIVE — outcome learning at 5m/15m/30m/1h/2h/4h, "
        "ignition velocity+acceleration, per-symbol anomaly percentiles, resistance attack/weakness scoring, "
        "persistent-book + spoof-like instability heuristic; formal PRE/BUY gates unchanged",
        flush=True,
    )


_install_v1014()


async def _v1014_telemetry_loop():
    while True:
        await asyncio.sleep(scanner.app.PRINT_SECONDS)
        try:
            rows = scanner.s.near_diag(10)
            pending_count = sum(len(v) for v in outcome_pending.values())
            print(
                f"Ψ-V10.14 LEARNING pending={pending_count} resolved={len(outcome_resolved)} "
                f"summary={_learning_summary()}",
                flush=True,
            )
            for i, row in enumerate(rows, 1):
                book = row.get("book_persistence") or {}
                res = row.get("resistance_quality") or {}
                print(
                    f"L{i:02d}. {str(row.get('symbol')):12s} "
                    f"grade={str(row.get('v1014_grade','LOW')):10s} "
                    f"v14={float(row.get('v1014_score') or 0):5.1f} "
                    f"vel={float(row.get('ignition_velocity_per_min') or 0):+7.2f}/m "
                    f"acc={float(row.get('ignition_acceleration_per_min2') or 0):+7.2f} "
                    f"resWeak={float(row.get('resistance_weakness_score') or 0):5.1f} "
                    f"attacks={int(res.get('attack_count_30m') or 0)} "
                    f"bookPersist={float(book.get('score') or 0):5.1f} "
                    f"book={book.get('label','-')}",
                    flush=True,
                )
        except Exception as exc:
            print(f"Ψ-V10.14 INTELLIGENCE_ERROR {type(exc).__name__}: {exc}", flush=True)


async def _combined_v1014_print_loop():
    await asyncio.gather(
        base._combined_print_loop(),
        _v1014_telemetry_loop(),
    )


scanner.v7.print_loop = _combined_v1014_print_loop
scanner.q.print_loop = _combined_v1014_print_loop
scanner.s.print_loop = _combined_v1014_print_loop


if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.14 ACTIVE — learning + ignition dynamics + resistance/book quality; strict PRE/BUY unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.14 stopped", flush=True)
