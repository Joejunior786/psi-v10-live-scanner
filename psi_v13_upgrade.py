"""Psi V13 independent online ML lane and 30-symbol reporting.

ML predictions never write to or weaken the conventional V12 execution authority.
No order placement. "ML BUY NOW" requires learned evidence AND fresh trade safety.
"""
import asyncio
import json
import math
import os
import time
from collections import deque

REVISION = "13.2.0-independent-ml30-rotation"
STATE_PATH = os.getenv("PSI_V13_STATE_PATH", "/data/psi_v13_model.json")
OUTCOME_PATH = os.getenv("PSI_V13_OUTCOME_PATH", "/data/psi_v13_outcomes.jsonl")
TRAIN_HORIZON_MS = 15 * 60_000
SNAPSHOT_EVERY_S = 60
MIN_TRAIN = max(30, int(os.getenv("PSI_V13_MIN_TRAIN", "100")))
MAX_PENDING = 1100
FEATURE_NAMES = (
    "hazard", "buy_delta", "rv10", "rv30", "trade_accel",
    "cvd_accel", "ofi", "ofi_accel", "obi", "ask_depletion",
    "sequence", "probe", "spread",
)

CORE = None
SENSOR = None
EARLY = None
LEARNER = None
_ORIGINAL_SCAN = None
_ORIGINAL_HEALTH = None
_ORIGINAL_PROMOTION = None
_ORIGINAL_PRIORITY = None
_model = {"weights": [0.0] * len(FEATURE_NAMES), "bias": -1.4,
          "trained": 0, "wins": 0, "test_n": 0, "test_wins": 0}
_pending = []
_recent = deque(maxlen=250)
_ranked = []
_last_collection_ms = 0
_last_save_ms = 0
_last_diag_ms = 0
_rotation = 0
# Distinct from the ML outcome-training rotation.
_coverage_history = {}
_coverage_last = []
_coverage_last_ms = 0
_coverage_cycle = 0
_held_progress = {}
_held_last_symbols = []
_stats = {"captured": 0, "resolved": 0, "invalid": 0, "model_errors": 0,
          "worker_errors": 0, "last_error": "", "promotion_requests": 0}


def num(v, default=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (ValueError, TypeError, OverflowError):
        return default


def clamp(x, low, high):
    return max(low, min(high, x))


def ts():
    return int(time.time() * 1000)


def features(row):
    """Current-time features only; no future outcome or V12 BUY status."""
    return [
        clamp(num(row.get("hazard_score")) / 100, 0, 1),
        clamp((num(row.get("buy_ratio"), 0.5) - 0.5) * 2, -1, 1),
        clamp(num(row.get("relative_volume_10s")) / 10, 0, 2),
        clamp(num(row.get("relative_volume_30s")) / 10, 0, 2),
        clamp(num(row.get("trade_acceleration")) / 10, 0, 2),
        clamp(num(row.get("cvd_acceleration")), -1, 1),
        clamp(num(row.get("ofi")), -1, 1),
        clamp(num(row.get("ofi_acceleration")), -1, 1),
        clamp(num(row.get("obi")), -1, 1),
        clamp(num(row.get("ask_depletion")), -1, 1),
        clamp(num(row.get("sequence_score")) / 100, 0, 1),
        clamp(num(row.get("v128_probe_score")) / 100, 0, 1),
        clamp(num(row.get("spread_bps"), 100) / 20, 0, 5),
    ]


def predict(x):
    z = _model["bias"] + sum(w * v for w, v in zip(_model["weights"], x))
    z = clamp(z, -20, 20)
    return 1 / (1 + math.exp(-z))


def update_model(x, outcome, created_ms):
    """Online logistic regression; validation/test events are never trained."""
    _model["trained"] += int(_split(created_ms) == "TRAIN")
    if _split(created_ms) == "TEST":
        _model["test_n"] += 1
        _model["test_wins"] += int(outcome)
        return
    if _split(created_ms) == "VALIDATION":
        return
    p = predict(x)
    delta = clamp(outcome - p, -1, 1)
    eta = 0.06 / math.sqrt(1 + _model["trained"] / 250)
    _model["weights"] = [
        clamp(w * (1 - eta * 0.001) + eta * delta * xi, -5, 5)
        for w, xi in zip(_model["weights"], x)
    ]
    _model["bias"] = clamp(_model["bias"] + eta * delta, -7, 5)
    _model["wins"] += int(outcome)


def _split(stamp):
    day = int(stamp // 86_400_000) % 20
    return "TRAIN" if day < 14 else "VALIDATION" if day < 17 else "TEST"


def fresh(row, at=None):
    at = ts() if at is None else at
    generated = num(row.get("generated_ms"))
    return bool(
        row.get("hard_sensor_safety")
        and generated > 0 and 0 <= at - generated <= 15_000
        and 0 <= num(row.get("trade_age_ms"), 1e9) <= 1200
        and 0 <= num(row.get("book_age_ms"), 1e9) <= 1200
        and row.get("sequence_verified")
        and row.get("book_sequence_verified")
        and 0 < num(row.get("spread_bps"), 1e9) <= 20
        and 0 <= num(row.get("slippage_bps"), 1e9) <= 35
        and num(row.get("entry_reference")) > 0
    )


def _rows():
    if SENSOR is None:
        return []
    universe = set(str(s).upper() for s in list(getattr(CORE.q, "universe", []) or []))
    rows = list(getattr(SENSOR, "_latest_candidates", []) or [])
    return [r for r in rows if isinstance(r, dict) and r.get("symbol") in universe]


def _priority_score(row, p):
    # This score ranks candidates, and is NOT an estimated success probability.
    if _model["trained"] >= MIN_TRAIN:
        return p
    return (
        num(row.get("hazard_score")) / 150
        + clamp(num(row.get("relative_volume_10s")) / 40, 0, 0.15)
        + clamp(num(row.get("v128_probe_score")) / 400, 0, 0.2)
    )


def _present(row, rank, at):
    sym = row["symbol"]
    x = features(row)
    p = predict(x)
    valid = fresh(row, at)
    entry = num(row.get("entry_reference"))
    trained = _model["trained"] >= MIN_TRAIN
    # Model has its own expected-value gate; technical V12 conditions are absent.
    ev = p * 0.03 - (1 - p) * 0.025
    actionable = bool(trained and valid and p >= 0.5 and ev > 0)
    blockers = []
    if not trained:
        blockers.append("MODEL_MINIMUM_HISTORY")
    if not valid:
        blockers.append("LIVE_SENSOR_OR_EXECUTION_SAFETY")
    if trained and (p < 0.5 or ev <= 0):
        blockers.append("NEGATIVE_MODEL_EXPECTED_VALUE")
    return {
        "symbol": sym,
        "rank": rank,
        "signal_ms": at,
        "signal": "ML BUY NOW" if actionable else "ML BUY CANDIDATE",
        "execution_ready": actionable,
        "reason": list(blockers),
        "model_probability": round(p, 4) if trained else None,
        "learning_samples": _model["trained"],
        "model_type": "online_logistic_sgd",
        "model_version": REVISION,
        "rank_score": round(_priority_score(row, p), 4),
        "observed_price": entry if valid else None,
        "research_price": (entry if entry > 0 and
                           0 <= at - num(row.get("generated_ms"), 0) <= 15000 and
                           0 <= num(row.get("trade_age_ms"), 1e9) <= 15000 else None),
        "research_price_source": "RECENT_BINANCE_SPOT_SENSOR_TRADE",
        "entry": entry if actionable else None,
        "stop": round(entry * 0.975, 10) if actionable else None,
        "tp1": round(entry * 1.03, 10) if actionable else None,
        "tp2": round(entry * 1.05, 10) if actionable else None,
        "tp3": round(entry * 1.10, 10) if actionable else None,
        "timeframe": "15m model / 5m-1h monitoring",
        "target_definition": "+3% before -2.5% stop within 15m",
        "model_ev_pct": round(ev * 100, 3) if trained else None,
        "evidence": {
            "hazard": row.get("hazard_score"),
            "buy_ratio": row.get("buy_ratio"),
            "rv10": row.get("relative_volume_10s"),
            "rv30": row.get("relative_volume_30s"),
            "trade_acceleration": row.get("trade_acceleration"),
            "cvd_acceleration": row.get("cvd_acceleration"),
            "ofi": row.get("ofi"),
            "ofi_acceleration": row.get("ofi_acceleration"),
            "ask_depletion": row.get("ask_depletion"),
            "sequence_score": row.get("sequence_score"),
            "v128_probe_score": row.get("v128_probe_score"),
            "spread_bps": row.get("spread_bps"),
            "obi": row.get("obi"),
            "trade_age_ms": row.get("trade_age_ms"),
            "book_age_ms": row.get("book_age_ms"),
        },
        "legacy_buy_authority": False,
    }


def rank():
    global _ranked
    at = ts()
    rows = _rows()
    # Always return five separate model selections after universe discovery,
    # even when sensor telemetry is missing. Unverified picks stay blocked.
    known = {r["symbol"] for r in rows}
    for sym in list(getattr(CORE.q, "universe", []) or []):
        if len(rows) >= 5:
            break
        if sym not in known:
            rows.append({"symbol": sym, "state": "NO_DATA", "entry_reference": 0.0,
                         "generated_ms": 0, "hard_sensor_safety": False})
            known.add(sym)
    ranked = sorted(rows, key=lambda r: _priority_score(r, predict(features(r))), reverse=True)
    # Preserve observations of incomplete candidates, but score fresh data first.
    ranked.sort(key=lambda r: (fresh(r, at), _priority_score(r, predict(features(r)))), reverse=True)
    _ranked = [_present(row, i + 1, at) for i, row in enumerate(ranked[:40])]
    return _ranked


def five():
    return list(_ranked[:5])


def _snapshot_prices():
    at = ts()
    return {
        r["symbol"]: num(r.get("entry_reference"))
        for r in _rows()
        if num(r.get("entry_reference")) > 0
        and 0 <= at - num(r.get("generated_ms")) <= 15_000
    }



def seed_historical():
    """Warm-start only from recorded, resolved pre-signal outcomes.

    Restrict legacy risk bands to near the new fixed stop. No reconstructed
    price histories and no look-ahead information are used as input features.
    """
    if LEARNER is None or _stats.get("historical_seed_completed"):
        return
    if not getattr(LEARNER, "_bootstrapped", False):
        return
    _stats["historical_seed_completed"] = True
    count = 0
    skipped = 0
    now = ts()
    for event in list(getattr(LEARNER, "_recent", []) or []):
        if not isinstance(event, dict) or not event.get("resolved"):
            continue
        feat = event.get("features") or {}
        created = int(num(event.get("created_ms")))
        entry = num(event.get("entry_price"))
        stop = num(event.get("stop_price"))
        if not isinstance(feat, dict) or not created or not entry or not stop:
            skipped += 1
            continue
        risk = (entry - stop) / entry
        if not (0.015 <= risk <= 0.035 and created < now - TRAIN_HORIZON_MS):
            skipped += 1
            continue
        target_at = int(num((event.get("first_target_ms") or {}).get("3")))
        stop_at = int(num(event.get("stop_hit_ms")))
        if not (
            target_at
            or stop_at
            or "15m" in (event.get("horizon_returns") or {})
        ):
            skipped += 1
            continue
        won = int(bool(
            target_at and target_at - created <= TRAIN_HORIZON_MS
            and (not stop_at or target_at < stop_at)
        ))
        pre_signal = {
            "hazard_score": feat.get("early_hazard_score"),
            "buy_ratio": feat.get("buy_ratio", 0.5),
            "relative_volume_10s": feat.get("relative_volume_10s", feat.get("relative_volume_30s")),
            "relative_volume_30s": feat.get("relative_volume_30s"),
            "trade_acceleration": feat.get("trade_acceleration"),
            "cvd_acceleration": feat.get("cvd_accel"),
            "ofi": feat.get("ofi"),
            "ofi_acceleration": feat.get("ofi_accel"),
            "obi": feat.get("obi"),
            "ask_depletion": feat.get("ask_depletion"),
            "sequence_score": feat.get("sequence_score"),
            "spread_bps": feat.get("spread_bps"),
        }
        update_model(features(pre_signal), won, created)
        count += 1
    _stats["historical_seeded"] = count
    _stats["historical_seed_skipped"] = skipped
    print("PSI-V13 HISTORICAL_SEED valid=" + str(count) +
          " excluded=" + str(skipped) +
          " train=" + str(_model["trained"]) +
          " source=RESOLVED_PRE_SIGNAL_OUTCOMES", flush=True)
    _save()


def collect():
    global _last_collection_ms, _rotation
    at = ts()
    if at - _last_collection_ms < SNAPSHOT_EVERY_S * 1000:
        return
    _last_collection_ms = at
    pool = _rows()
    if not pool:
        return
    top = sorted(pool, key=lambda r: _priority_score(r, predict(features(r))), reverse=True)[:28]
    ordered = sorted(pool, key=lambda r: r["symbol"])
    rotation = [ordered[(_rotation + i) % len(ordered)] for i in range(min(12, len(ordered)))]
    _rotation = (_rotation + 12) % len(ordered)
    symbols = set()
    for r in top + rotation:
        sym = r["symbol"]
        if sym in symbols or len(_pending) >= MAX_PENDING:
            continue
        symbols.add(sym)
        entry = num(r.get("entry_reference"))
        if entry <= 0 or at - num(r.get("generated_ms")) > 15_000:
            continue
        _pending.append({
            "s": sym, "t": at, "p": entry, "x": features(r),
            "pre": round(predict(features(r)), 5),
            "target_at": 0, "stop_at": 0, "max": 0.0, "min": 0.0,
        })
        _stats["captured"] += 1


def _save():
    global _last_save_ms
    at = ts()
    if at - _last_save_ms < 45_000:
        return
    _last_save_ms = at
    folder = os.path.dirname(STATE_PATH)
    if folder:
        os.makedirs(folder, exist_ok=True)
    name = STATE_PATH + ".tmp"
    with open(name, "w", encoding="utf-8") as f:
        json.dump({"model": _model, "pending": _pending[-MAX_PENDING:],
                   "stats": _stats, "rotation": _rotation,
                   "held_back": _held_back_state,
                   "rotation_snapshot": {"history": _coverage_history,
                                         "last": _coverage_last,
                                         "last_ms": _coverage_last_ms,
                                         "cycle": _coverage_cycle,
                                         "held_progress": _held_progress,
                                         "held_last_symbols": _held_last_symbols}}, f,
                  separators=(",", ":"))
    os.replace(name, STATE_PATH)


def _restore():
    global _rotation, _coverage_last_ms, _coverage_cycle, _coverage_last, _held_last_symbols
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        model = data.get("model") or {}
        if len(model.get("weights") or []) == len(FEATURE_NAMES):
            _model.update(model)
        _pending.extend(list(data.get("pending") or [])[-MAX_PENDING:])
        _stats.update(data.get("stats") or {})
        _rotation = int(data.get("rotation") or 0)
        _held_back_state.clear()
        _held_back_state.update(data.get("held_back") or {})
        rotation_state = data.get("rotation_snapshot") or {}
        _coverage_history.clear()
        _coverage_history.update(rotation_state.get("history") or {})
        _coverage_last = list(rotation_state.get("last") or [])[:30]
        _coverage_last_ms = int(rotation_state.get("last_ms") or 0)
        _coverage_cycle = int(rotation_state.get("cycle") or 0)
        _held_progress.clear()
        _held_progress.update(rotation_state.get("held_progress") or {})
        _held_last_symbols = list(rotation_state.get("held_last_symbols") or [])[:5]
    except (OSError, ValueError, TypeError):
        pass


def learn():
    at = ts()
    prices = _snapshot_prices()
    remaining = []
    for event in _pending:
        age = at - int(event["t"])
        price = prices.get(event["s"])
        if price is not None and price > 0:
            ret = price / event["p"] - 1
            event["max"] = max(event["max"], ret)
            event["min"] = min(event["min"], ret)
            if ret >= 0.03 and not event["target_at"]:
                event["target_at"] = at
            if ret <= -0.025 and not event["stop_at"]:
                event["stop_at"] = at
        if age < TRAIN_HORIZON_MS:
            remaining.append(event)
            continue
        if price is None and age < TRAIN_HORIZON_MS + 120_000:
            remaining.append(event)
            continue
        if price is None:
            _stats["invalid"] += 1
            continue
        won = bool(event["target_at"] and
                   (not event["stop_at"] or event["target_at"] < event["stop_at"]))
        outcome = int(won)
        predicted = event["pre"]
        if _split(event["t"]) == "TEST":
            _stats["test_brier_sum"] = num(_stats.get("test_brier_sum")) + (predicted - outcome) ** 2
        update_model(event["x"], outcome, event["t"])
        result = {"symbol": event["s"], "at_ms": event["t"], "resolved_ms": at,
                  "label": outcome, "pre_prediction": predicted,
                  "mfe_pct": round(event["max"] * 100, 3),
                  "mae_pct": round(event["min"] * 100, 3),
                  "split": _split(event["t"]), "model_revision": REVISION}
        _recent.append(result)
        _stats["resolved"] += 1
        try:
            with open(OUTCOME_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(result, separators=(",", ":")) + "\n")
        except OSError:
            _stats["outcome_write_errors"] = _stats.get("outcome_write_errors", 0) + 1
    _pending[:] = remaining
    _save()


def _pool_candidates():
    syms = [r["symbol"] for r in _ranked[:8] if r["rank_score"] > 0]
    # Preserve sticky eligibility for fast NMR-style promotion.
    return syms


def _promoted_symbols():
    base = list(_ORIGINAL_PROMOTION() or [])
    out = []
    for sym in _pool_candidates() + base:
        if sym not in out:
            out.append(sym)
    _stats["promotion_requests"] += len(_pool_candidates())
    return out[:max(16, len(base))]


def _priorities(universe):
    base = list(_ORIGINAL_PRIORITY(universe) or [])
    allowed = set(universe or [])
    limit = int(getattr(CORE, "ACTIVE_SYMBOLS_PER_CYCLE", 8))
    out = []
    for sym in _pool_candidates()[:3] + base:
        if sym in allowed and sym not in out:
            out.append(sym)
        if len(out) >= limit:
            break
    return out


def _symbol(row):
    return str(row.get("symbol") or "").upper()



ROTATION_COOLDOWN_CYCLES = 3
ROTATION_MIN_INTERVAL_MS = 35_000
PEGGED_SYMBOLS = {"XUSDUSDT", "USDCUSDT", "FDUSDUSDT", "DAIUSDT",
                  "TUSDUSDT", "USDDUSDT", "USDEUSDT", "USD1USDT",
                  "BFUSDUSDT"}


def _rotation_price(sensor, at):
    if not isinstance(sensor, dict):
        return None
    age = at - num(sensor.get("generated_ms"), -1)
    trade_age = num(sensor.get("trade_age_ms"), 1e12)
    px = num(sensor.get("entry_reference"))
    return px if px > 0 and 0 <= age <= 15_000 and 0 <= trade_age <= 15_000 else None


def _rotation_evidence(sensor, structural, at):
    px = _rotation_price(sensor, at)
    verified = bool(sensor and fresh(sensor, at))
    strength = num(structural.get("setup_strength")) if structural else 0
    hazard = num(sensor.get("hazard_score")) if sensor else 0
    delta = num(sensor.get("change_point_delta")) if sensor else 0
    score = ((30 if verified else 0) + (10 if px is not None else 0)
             + (20 if structural and structural.get("state") == "BUY" else
                10 if structural and structural.get("state") == "ARMED" else 0)
             + min(25, strength / 4) + min(15, hazard / 8)
             + min(8, max(0, delta) / 4))
    return {"priced": px is not None, "verified": verified,
            "quality": round(score, 3)}


def _rotation_trend(current, previous):
    if not previous:
        return "NEW"
    if previous.get("verified") and not current["verified"]:
        return "DETERIORATING"
    if previous.get("priced") and not current["priced"]:
        return "DETERIORATING"
    if (current["verified"] and not previous.get("verified") or
            current["priced"] and not previous.get("priced")):
        return "IMPROVING"
    d = current["quality"] - num(previous.get("quality"))
    return "IMPROVING" if d >= 8 else "DETERIORATING" if d <= -8 else "UNCHANGED"


def thirty(at=None, record=False):
    """Display-only: 5 independent ML, 10 strongest, 15 rotating challengers.

    Only discovery_worker advances rotation history; endpoint reads do not.
    Missing market data never becomes a BUY or a manufactured price.
    """
    global _coverage_last_ms, _coverage_cycle, _coverage_last
    at = ts() if at is None else at
    if _coverage_last and (not record or
            0 <= at - _coverage_last_ms < ROTATION_MIN_INTERVAL_MS):
        return [dict(x) for x in _coverage_last]
    if CORE is None:
        return []
    universe = list(dict.fromkeys(str(s).upper()
                     for s in (getattr(CORE.q, "universe", []) or [])))
    allowed = set(universe)
    sensors = {_symbol(r): r for r in _rows()}
    stricts = {_symbol(r): r for r in (CORE._board() or [])
               if isinstance(r, dict)}
    monsters = {_symbol(r): r for r in
                ((getattr(CORE.base, "latest", {}) or {}).get("_all_candidates") or [])
                if isinstance(r, dict)}
    selected, used = [], set()

    def setup(sym):
        s, m, q = stricts.get(sym) or {}, monsters.get(sym) or {}, sensors.get(sym) or {}
        if s.get("state") == "BUY":
            return "armed"
        if m.get("monsterPullbackState"):
            return "pullback"
        if m.get("state") in {"MONSTER-HOT", "MONSTER-IGNITION",
                               "MONSTER-RESCUE", "MONSTER-WATCH"}:
            return "monster"
        if q.get("state") not in (None, "", "NONE", "DATA_WAIT", "NO_DATA"):
            return "pre_ignition"
        if s.get("state") in {"ARMED", "WATCH"}:
            return "armed"
        return "dark_horse" if q else "unqualified"

    def add(sym, category, state, score, reason):
        sym = str(sym or "").upper()
        if sym not in allowed or sym in used or len(selected) >= 30:
            return False
        used.add(sym)
        ev = _rotation_evidence(sensors.get(sym), stricts.get(sym), at)
        prev = _coverage_history.get(sym)
        gap = _coverage_cycle - int(prev.get("cycle") or 0) if prev else None
        selected.append({
            "symbol": sym, "category": category, "setup_category": setup(sym),
            "state": str(state or "UNQUALIFIED_WATCH"), "score": score,
            "selection_reason": reason, "entry_verified": False,
            "rotation_progress": _rotation_trend(ev, prev),
            "rotation_previous_cycle_gap": gap,
            "rotation_cooldown_satisfied": bool(
                not prev or gap >= ROTATION_COOLDOWN_CYCLES),
            "recent_trade_observed": ev["priced"],
            "hard_sensor_verified": ev["verified"],
            "rotation_quality": ev["quality"],
        })
        return True

    for ml in five():
        add(ml["symbol"], "ml", ml["signal"], ml["rank_score"],
            "INDEPENDENT_ML_RANKING")
    for sym in universe:
        if sum(x["category"] == "ml" for x in selected) >= min(5, len(universe)):
            break
        add(sym, "ml", "ML NO DATA", None, "UNQUALIFIED_ML_COVERAGE_ONLY")

    def quality(sym):
        s, m, q = stricts.get(sym) or {}, monsters.get(sym) or {}, sensors.get(sym) or {}
        ev = _rotation_evidence(q, s, at)
        return (int(ev["priced"]), int(ev["verified"]),
                int(s.get("state") == "BUY"), int(s.get("state") == "ARMED"),
                int(m.get("state") in {"MONSTER-HOT", "MONSTER-IGNITION", "MONSTER-RESCUE"}),
                num(s.get("setup_strength")), num(q.get("hazard_score")),
                num(m.get("layers")), num(q.get("change_point_delta")))

    strong = [s for s in universe if s not in used and s not in PEGGED_SYMBOLS
              and (stricts.get(s) or {}).get("state") in {"BUY", "ARMED"}]
    strong.sort(key=lambda s: (quality(s), s), reverse=True)
    for sym in strong:
        if sum(x["category"] == "strongest" for x in selected) >= 10:
            break
        st = stricts[sym]
        add(sym, "strongest", "STRUCTURAL_" + str(st.get("state")),
            num(st.get("setup_strength")), "CONTINUING_STRUCTURAL_SETUP")

    if sum(x["category"] == "strongest" for x in selected) < 10:
        extras = [s for s in universe if s not in used and s not in PEGGED_SYMBOLS
                  and (s in sensors or s in monsters)]
        extras.sort(key=lambda s: (quality(s), s), reverse=True)
        for sym in extras:
            if sum(x["category"] == "strongest" for x in selected) >= 10:
                break
            m, q = monsters.get(sym) or {}, sensors.get(sym) or {}
            add(sym, "strongest", m.get("state") or q.get("state") or "DISCOVERY_WATCH",
                num(q.get("hazard_score")) or num(m.get("layers")),
                "BEST_AVAILABLE_WATCH_NOT_CONFIRMED")

    def challenge_rank(sym):
        ev = _rotation_evidence(sensors.get(sym), stricts.get(sym), at)
        prev = _coverage_history.get(sym)
        gap = _coverage_cycle - int(prev.get("cycle") or 0) if prev else 100000
        m, q, st = monsters.get(sym) or {}, sensors.get(sym) or {}, stricts.get(sym) or {}
        evidence = bool(
            st.get("state") in {"BUY", "ARMED"} or
            m.get("state") in {"MONSTER-WATCH", "MONSTER-HOT",
                               "MONSTER-IGNITION", "MONSTER-RESCUE"} or
            m.get("monsterPullbackState") or
            q.get("state") not in (None, "", "NONE", "DATA_WAIT", "NO_DATA") or
            num(q.get("change_point_delta")) >= 10)
        return (int(ev["priced"]), int(gap >= ROTATION_COOLDOWN_CYCLES),
                int(evidence), int(ev["verified"]), min(gap, 10000),
                ev["quality"], sym)

    pool = [s for s in universe if s not in used and s not in PEGGED_SYMBOLS]
    pool.sort(key=challenge_rank, reverse=True)
    for sym in pool:
        if sum(x["category"] == "fresh_challenger" for x in selected) >= 15:
            break
        q, m = sensors.get(sym) or {}, monsters.get(sym) or {}
        ev = _rotation_evidence(sensors.get(sym), stricts.get(sym), at)
        old = _coverage_history.get(sym)
        gap = _coverage_cycle - int(old.get("cycle") or 0) if old else 100000
        reason = ("NEW_RECENT_OBSERVATION" if ev["priced"] and not old else
                  "COOLDOWN_RECENT_OBSERVATION" if ev["priced"] and
                  gap >= ROTATION_COOLDOWN_CYCLES else
                  "REPEATED_LIMITED_FRESH_COVERAGE" if ev["priced"] else
                  "NO_RECENT_PRICE_COVERAGE_ONLY")
        add(sym, "fresh_challenger", m.get("state") or q.get("state") or "UNQUALIFIED_WATCH",
            num(q.get("hazard_score")) or num(q.get("change_point_delta")) or
            num(m.get("layers")) or None, reason)

    for sym in universe:
        if len(selected) >= min(30, len(universe)):
            break
        if sym not in used:
            category = ("strongest" if sum(x["category"] == "strongest"
                                          for x in selected) < 10 else "fresh_challenger")
            add(sym, category, "UNQUALIFIED_WATCH", None,
                "NO_RECENT_PRICE_COVERAGE_ONLY")

    if record and selected:
        _coverage_cycle += 1
        _coverage_last_ms = at
        _coverage_last = [dict(x) for x in selected]
        for x in selected:
            _coverage_history[x["symbol"]] = {
                "cycle": _coverage_cycle, "last_seen_ms": at,
                "quality": x["rotation_quality"],
                "priced": x["recent_trade_observed"],
                "verified": x["hard_sensor_verified"],
            }
        if len(_coverage_history) > 1200:
            oldest = sorted(_coverage_history,
                            key=lambda s: int(_coverage_history[s].get("last_seen_ms") or 0))
            for sym in oldest[:-1200]:
                _coverage_history.pop(sym, None)
    return selected[:30]



def coverage30_audit(report=None, at=None):
    """Capture the exact selected board; never turn research into a BUY."""
    at = ts() if at is None else at
    report = thirty() if report is None else list(report)
    ml_map = {r["symbol"]: r for r in five()}
    sensor_map = {r["symbol"]: r for r in _rows()}
    strict_map = {
        _symbol(r): r for r in (CORE._board() or []) if isinstance(r, dict)
    }
    audited = []
    for selected in report:
        sym = _symbol(selected)
        category = selected["category"]
        strict = strict_map.get(sym) or {}
        sensor = sensor_map.get(sym) or {}
        ml = (ml_map.get(sym) or {}) if category == "ml" else {}
        sample_fresh = bool(sensor and fresh(sensor, at))
        age = at - num(sensor.get("generated_ms"))
        reference = num(sensor.get("entry_reference"))
        observed = reference if (
            reference > 0 and 0 <= age <= 15_000
            and 0 <= num(sensor.get("trade_age_ms"), 1e9) <= 15_000
        ) else None

        if category == "ml":
            approved = bool(ml.get("execution_ready") and sample_fresh
                            and 0 <= at - num(ml.get("signal_ms")) <= 15_000)
            entry = num(ml.get("entry"))
            stop = num(ml.get("stop"))
            targets = [num(ml.get(k)) for k in ("tp1", "tp2", "tp3")]
            blockers = list(ml.get("reason") or [])
            status = "ML BUY NOW" if approved else "ML BUY CANDIDATE"
            authority = "V13_ML_SAFETY_GATES"
            if not approved and not blockers:
                blockers = ["ML_SAFETY_NOT_VERIFIED_AT_REPORT_TIME"]
        else:
            entry = (num(strict.get("pinpoint_trigger")) or
                     num(strict.get("entry_low")) or num(strict.get("entry")))
            stop = num(strict.get("pinpoint_stop")) or num(strict.get("invalidation"))
            targets = [num(strict.get(k)) for k in ("tp1", "tp2", "tp3")]
            approved = bool(
                strict.get("execution_state") == "BUY NOW"
                and 0 < stop < entry < targets[0] < targets[1] < targets[2]
            )
            blockers = list(strict.get("execution_blockers") or [])
            status = str(strict.get("execution_state") or "NOT_APPROVED")
            authority = "V12.3.4_FAIL_CLOSED"
            if not approved and not blockers:
                blockers = ["UNQUALIFIED_COVERAGE_ONLY" if
                            selected.get("state") == "UNQUALIFIED_WATCH" else
                            "V12_EXECUTION_NOT_APPROVED"]

        audited.append({
            "symbol": sym, "category": category,
            "setup_category": selected.get("setup_category"),
            "selection_reason": selected.get("selection_reason"),
            "rotation_progress": selected.get("rotation_progress"),
            "rotation_cooldown_satisfied": selected.get("rotation_cooldown_satisfied"),
            "rotation_previous_cycle_gap": selected.get("rotation_previous_cycle_gap"),
            "state": str(selected.get("state") or ""),
            "score": selected.get("score"),
            "execution_status": status, "execution_authority": authority,
            "entry_verified": approved,
            "observed_price": observed, "sensor_fully_verified": sample_fresh,
            "entry": entry if approved else None,
            "stop": stop if approved else None,
            "tp1": targets[0] if approved else None,
            "tp2": targets[1] if approved else None,
            "tp3": targets[2] if approved else None,
            "research_only_levels": (
                {"entry": entry or None, "stop": stop or None,
                 "tp1": targets[0] or None, "tp2": targets[1] or None,
                 "tp3": targets[2] or None}
                if not approved and category != "ml" and strict else None
            ),
            "ml_probability": ml.get("model_probability") if ml else None,
            "blockers": [str(x) for x in blockers[:5]],
        })
    return {
        "version": REVISION, "generated_ms": at,
        "total": len(audited), "unique": len({r["symbol"] for r in audited}),
        "rotation_cycle": _coverage_cycle,
        "rotation_layout": {"ml": 5, "strongest": 10, "fresh_challenger": 15},
        "rotation_progress_counts": {p: sum(x.get("rotation_progress") == p for x in report)
                                     for p in ("NEW", "IMPROVING", "DETERIORATING", "UNCHANGED")},
        "execution_ready": sum(bool(r["entry_verified"]) for r in audited),
        "rows": audited, "unqualified_are_not_buys": True,
    }



# Read-only seventh lane; never changes V12/ML execution authority.
HELD_BACK_LIMIT = 5
HELD_BACK_MIN_RR = 1.5
HELD_BACK_HORIZON_MS = 24 * 60 * 60_000
_held_back_state = {}


def _held_price(sensor, at):
    """Recent Binance-backed sensor observation; no stale-price substitution."""
    if not isinstance(sensor, dict):
        return None
    age = at - num(sensor.get("generated_ms"))
    trade_age = num(sensor.get("trade_age_ms"), 1e9)
    price = num(sensor.get("entry_reference"))
    if price > 0 and 0 <= age <= 15_000 and 0 <= trade_age <= 15_000:
        return price
    return None


def _held_tracking(snapshot, sensors, at):
    """24h observed target/stop touches, not guaranteed fills or exact P&L."""
    for sym, event in list(_held_back_state.items()):
        if event.get("outcome") != "PENDING":
            continue
        age = at - int(event.get("started_ms") or 0)
        price = _held_price(sensors.get(sym), at)
        if 0 < age <= HELD_BACK_HORIZON_MS and price is not None:
            if price >= num(event.get("tp1")):
                event["outcome"], event["resolved_ms"] = "OBSERVED_TP1_TOUCH", at
            elif price <= num(event.get("stop")):
                event["outcome"], event["resolved_ms"] = "OBSERVED_STOP_TOUCH", at
        if age >= HELD_BACK_HORIZON_MS and event["outcome"] == "PENDING":
            event["outcome"], event["resolved_ms"] = "EXPIRED_UNRESOLVED", at
    for item in snapshot["rows"]:
        if item["observed_price"] is None or item["reward_risk"] is None:
            continue
        sym = item["symbol"]
        existing = _held_back_state.get(sym) or {}
        if existing and at - int(existing.get("started_ms") or at) < HELD_BACK_HORIZON_MS:
            continue
        levels = item["research_only_levels"]
        _held_back_state[sym] = {
            "symbol": sym, "started_ms": at,
            "observed_entry": item["observed_price"],
            "stop": levels["stop"], "tp1": levels["tp1"],
            "outcome": "PENDING", "resolved_ms": None,
            "source": "OBSERVED_SPOT_SENSOR_ONLY",
        }
    if len(_held_back_state) > 300:
        ordered = sorted(_held_back_state,
                         key=lambda sym: int(_held_back_state[sym].get("started_ms") or 0))
        for sym in ordered[:-300]:
            del _held_back_state[sym]



def _held_evidence_trend(current, previous):
    """Compare observed evidence; do not infer missing confirmations."""
    if not previous:
        return "NEW"
    if previous.get("verified") and not current["verified"]:
        return "DETERIORATING"
    if previous.get("priced") and not current["priced"]:
        return "DETERIORATING"
    if (current["verified"] and not previous.get("verified") or
            current["priced"] and not previous.get("priced")):
        return "IMPROVING"
    delta = int(previous.get("blockers_count") or 0) - current["blockers_count"]
    return "IMPROVING" if delta >= 2 else "DETERIORATING" if delta <= -2 else "UNCHANGED"


def held_back_lane(at=None, record=False):
    """Five clearly labelled withheld setups plus separate risk rejections.

    The original 30-coin/ML boards and strict execution checks are untouched.
    """
    at = ts() if at is None else at
    if CORE is None:
        return {"status": "UNAVAILABLE", "rows": [], "rejected": [],
                "total_held": 0, "execution_ready": 0}
    global _held_last_symbols
    sensors = {_symbol(r): r for r in _rows()}
    held, rejected = [], []
    observations = {}
    for structural in (CORE._board() or []):
        if not isinstance(structural, dict):
            continue
        sym = _symbol(structural)
        if not sym or structural.get("state") not in {"BUY", "ARMED"}:
            continue
        if structural.get("execution_state") == "BUY NOW" or structural.get("buy_now"):
            continue
        entry = (num(structural.get("pinpoint_trigger")) or
                 num(structural.get("entry_low")) or num(structural.get("entry")))
        stop = (num(structural.get("pinpoint_stop")) or
                num(structural.get("invalidation")))
        tp1, tp2, tp3 = (num(structural.get(k)) for k in ("tp1", "tp2", "tp3"))
        valid_plan = bool(0 < stop < entry < tp1 < tp2 < tp3)
        sensor = sensors.get(sym)
        observed = _held_price(sensor, at)
        verified = bool(sensor and fresh(sensor, at))
        blockers = list(dict.fromkeys(str(b) for b in
                      (structural.get("execution_blockers") or []) if str(b)))
        distance_pct = ((entry / observed - 1) * 100
                        if observed is not None and entry > 0 else None)
        rr = ((tp1 - observed) / (observed - stop)
              if valid_plan and observed is not None and stop < observed < tp1
              else None)
        risk_reason = None
        if sym in {"XUSDUSDT", "USDCUSDT", "FDUSDUSDT", "DAIUSDT",
                   "TUSDUSDT", "USDDUSDT", "USDEUSDT", "USD1USDT"}:
            risk_reason = "PEGGED_ASSET"
        elif not valid_plan:
            risk_reason = "INVALID_OR_MISSING_REFERENCE_PLAN"
        elif structural.get("anti_chase"):
            risk_reason = "ANTI_CHASE"
        elif observed is not None and observed <= stop:
            risk_reason = "REFERENCE_STOP_INVALIDATED"
        elif observed is not None and observed >= tp1:
            risk_reason = "TP1_ALREADY_PASSED"
        elif distance_pct is not None and abs(distance_pct) > 3:
            risk_reason = "OUTSIDE_3_PERCENT_ENTRY_WINDOW"
        elif rr is not None and rr < HELD_BACK_MIN_RR:
            risk_reason = "REWARD_RISK_BELOW_1_5"
        if risk_reason:
            label = "REJECTED - RISK"
        elif observed is None or not verified or any(
                b.startswith(("LIVE_", "STALE_", "TRADE_SEQUENCE",
                              "BOOK_SEQUENCE", "QUALIFIED_MICRO"))
                for b in blockers):
            label = "HELD - DATA"
        else:
            label = "HELD - CONFIRMATION"
        evidence = {"priced": observed is not None, "verified": verified,
                    "blockers_count": len(blockers)}
        previous = _held_progress.get(sym)
        trend = _held_evidence_trend(evidence, previous)
        new_display = sym not in _held_last_symbols
        observations[sym] = evidence
        item = {
            "symbol": sym, "label": label, "status": "NOT_EXECUTABLE",
            "display_status": "NEW TO TOP 5" if new_display else "CONTINUING",
            "evidence_progress": trend,
            "repeat_reason": (None if new_display else
                              "RETAINS_STRUCTURAL_SETUP_AWAITING_LIVE_CONFIRMATION"),
            "structural_state": str(structural.get("state")),
            "execution_state": str(structural.get("execution_state") or "NOT_APPROVED"),
            "score": num(structural.get("setup_strength")),
            "observed_price": observed, "observed_at_ms": at if observed is not None else None,
            "sensor_fully_verified": verified,
            "research_only_levels": {"entry": entry or None, "stop": stop or None,
                                     "tp1": tp1 or None, "tp2": tp2 or None,
                                     "tp3": tp3 or None},
            "reference_distance_pct": round(distance_pct, 3) if distance_pct is not None else None,
            "reward_risk": round(rr, 3) if rr is not None else None,
            "tp1_distance_pct": (round((tp1 / observed - 1) * 100, 3)
                                  if observed is not None and tp1 > 0 else None),
            "rejection_reason": risk_reason, "missing_confirmations": blockers[:15],
            "entry_verified": False, "trade_instruction": False,
        }
        rank = (int(rr is not None), int(new_display),
                int(trend == "IMPROVING"), int(verified),
                int(structural.get("state") == "BUY"),
                num(structural.get("setup_strength")), num(rr))
        (rejected if risk_reason else held).append((rank, sym, item))
    held.sort(key=lambda x: (x[0], x[1]), reverse=True)
    rejected.sort(key=lambda x: (x[0], x[1]), reverse=True)
    snapshot = {
        "version": REVISION, "generated_ms": at,
        "label": "HELD-BACK BUYS - NOT APPROVED",
        "authority": "REPORTING_ONLY_NO_EXECUTION_AUTHORITY",
        "total_held": len(held), "total_rejected": len(rejected),
        "rows": [r[2] for r in held[:HELD_BACK_LIMIT]],
        "rejected": [r[2] for r in rejected[:HELD_BACK_LIMIT]],
        "new_top_five": sum(r[2]["display_status"] == "NEW TO TOP 5"
                            for r in held[:HELD_BACK_LIMIT]),
        "continuing_top_five": sum(r[2]["display_status"] == "CONTINUING"
                                   for r in held[:HELD_BACK_LIMIT]),
        "execution_ready": 0, "entry_orders_allowed": False,
        "held_min_reward_risk": HELD_BACK_MIN_RR,
        "tracking_method": "OBSERVED_TP1_OR_STOP_TOUCH_24H_NOT_FILL_PROOF",
    }
    if record:
        _held_tracking(snapshot, sensors, at)
        _held_last_symbols = [r["symbol"] for r in snapshot["rows"]]
        for symbol, ev in observations.items():
            _held_progress[symbol] = dict(ev, last_seen_ms=at)
        if len(_held_progress) > 1200:
            oldest = sorted(_held_progress,
                            key=lambda k: int(_held_progress[k].get("last_seen_ms") or 0))
            for symbol in oldest[:-1200]:
                _held_progress.pop(symbol, None)
    snapshot["tracking"] = {
        "total": len(_held_back_state),
        "pending": sum(x.get("outcome") == "PENDING" for x in _held_back_state.values()),
        "observed_tp1": sum(x.get("outcome") == "OBSERVED_TP1_TOUCH"
                            for x in _held_back_state.values()),
        "observed_stop": sum(x.get("outcome") == "OBSERVED_STOP_TOUCH"
                             for x in _held_back_state.values()),
        "expired_unresolved": sum(x.get("outcome") == "EXPIRED_UNRESOLVED"
                                  for x in _held_back_state.values()),
        "not_verified_pnl": True,
    }
    return snapshot


def log_held_back_lane(at=None):
    snapshot = held_back_lane(at=at, record=True)
    print("PSI-V13 HELD_BACK_JSON " +
          json.dumps(snapshot, separators=(",", ":"), allow_nan=False), flush=True)
    print("PSI-V13 HELD_BACK_BOARD held=" + str(snapshot["total_held"]) +
          " rejected=" + str(snapshot["total_rejected"]) +
          " shown=" + str(len(snapshot["rows"])) +
          " newTop=" + str(snapshot["new_top_five"]) +
          " continuing=" + str(snapshot["continuing_top_five"]) +
          " buyNow=0 reportingOnly=YES", flush=True)
    return snapshot


def log_coverage30_audit(report=None, at=None):
    """Single structured Railway line with ML, strongest and challengers."""
    snapshot = coverage30_audit(report, at)
    print("PSI-V13 COVERAGE30_JSON " +
          json.dumps(snapshot, separators=(",", ":"), allow_nan=False),
          flush=True)
    return snapshot

def _inject(response):
    try:
        payload = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response
    ranked = five()
    report = thirty()
    payload["scanner"] = "Psi-V13 Dual Intelligence"
    payload["version"] = REVISION
    payload["conventional_authority_version"] = str(getattr(CORE, "VERSION", "V12"))
    payload["v13_ml"] = {
        "independent_of_conventional_buy_rules": True,
        "model": "online_logistic_sgd",
        "training_horizon": "15m +3% before -2.5% stop",
        "sample_count": _model["trained"],
        "historical_seeded": _stats.get("historical_seeded", 0),
        "model_ready": _model["trained"] >= MIN_TRAIN,
        "validated_test_samples": _model["test_n"],
        "test_win_rate": round(_model["test_wins"] / _model["test_n"], 4) if _model["test_n"] else None,
        "top_5": ranked,
        "actionable_count": sum(r["execution_ready"] for r in ranked),
        "selection_count": len(ranked),
        "outcome_stats": dict(_stats),
        "pending_outcomes": len(_pending),
        "no_order_placement": True,
        "non_guarantee": "ML rankings are not evidence of certain gains.",
    }
    payload["v13_coverage_30"] = {
        "total": len(report),
        "unique": len({r["symbol"] for r in report}),
        "categories": report,
        "audit": coverage30_audit(report),
        "unqualified_are_not_buys": True,
    }
    payload["v13_held_back"] = held_back_lane()
    payload["rotation_layout"] = {"ml": 5, "strongest": 10, "fresh_challenger": 15}
    payload["generated_ms"] = ts()
    return CORE.app.web.json_response(payload, status=response.status)


async def _scan(req):
    return _inject(await _ORIGINAL_SCAN(req))


async def _health(req):
    return _inject(await _ORIGINAL_HEALTH(req))


async def discovery_worker():
    global _last_diag_ms
    while True:
        try:
            seed_historical()
            ranked = rank()
            collect()
            at = ts()
            if at - _last_diag_ms >= 15_000:
                _last_diag_ms = at
                top = ",".join(r["symbol"] + ":" + r["signal"] for r in ranked[:5])
                print("PSI-V13 ML_BOARD selections=" + str(min(5, len(ranked))) +
                      " eligible=" + str(sum(r["execution_ready"] for r in ranked[:5])) +
                      " trained=" + str(_model["trained"]) +
                      " pending=" + str(len(_pending)) +
                      " top=" + top +
                      " independent=YES", flush=True)
                report = thirty(at=at, record=True)
                print("PSI-V13 COVERAGE30 unique=" + str(len({r["symbol"] for r in report})) +
                      " rows=" + str(len(report)) +
                      " categories=5ML+10strongest+15challengers", flush=True)
                log_coverage30_audit(report, at)
                log_held_back_lane(at)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["worker_errors"] += 1
            _stats["last_error"] = "discover:" + type(exc).__name__ + ":" + str(exc)[:130]
            print("PSI-V13 DISCOVERY_ERROR " + _stats["last_error"], flush=True)
        await asyncio.sleep(5)


async def learning_worker():
    while True:
        try:
            learn()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _stats["worker_errors"] += 1
            _stats["last_error"] = "learn:" + type(exc).__name__ + ":" + str(exc)[:130]
            print("PSI-V13 LEARN_ERROR " + _stats["last_error"], flush=True)
        await asyncio.sleep(10)


def install(core, sensor, early, learner=None):
    global CORE, SENSOR, EARLY, LEARNER, _ORIGINAL_SCAN, _ORIGINAL_HEALTH
    global _ORIGINAL_PROMOTION, _ORIGINAL_PRIORITY
    if CORE is not None:
        return
    CORE, SENSOR, EARLY, LEARNER = core, sensor, early, learner
    _ORIGINAL_SCAN, _ORIGINAL_HEALTH = core.v12_scan, core.v12_health
    _ORIGINAL_PROMOTION = sensor._promotion_symbols
    _ORIGINAL_PRIORITY = core._priority_symbols
    sensor._promotion_symbols = _promoted_symbols
    core._priority_symbols = _priorities
    core.v12_scan = _scan
    core.v12_health = _health
    core.app.scan_endpoint = _scan
    core.app.health = _health
    _restore()
    print("PSI-V13 INSTALLED revision=" + REVISION +
          " ml=INDEPENDENT coverage=30 heldBack=REPORTING_ONLY workers=DISCOVERY+LEARNING orders=DISABLED", flush=True)
