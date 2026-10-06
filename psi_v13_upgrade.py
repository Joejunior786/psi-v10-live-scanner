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

REVISION = "13.0.0-independent-ml30"
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
                   "stats": _stats, "rotation": _rotation}, f, separators=(",", ":"))
    os.replace(name, STATE_PATH)


def _restore():
    global _rotation
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        model = data.get("model") or {}
        if len(model.get("weights") or []) == len(FEATURE_NAMES):
            _model.update(model)
        _pending.extend(list(data.get("pending") or [])[-MAX_PENDING:])
        _stats.update(data.get("stats") or {})
        _rotation = int(data.get("rotation") or 0)
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


def thirty():
    picked, seen = [], set()
    rows = _rows()
    by_sym = {r["symbol"]: r for r in rows}
    board = list(CORE._board() or [])
    monsters = list((getattr(CORE.base, "latest", {}) or {}).get("_all_candidates") or [])
    universe = list(getattr(CORE.q, "universe", []) or [])
    counter = {"ml": 0, "monster": 0, "pullback": 0, "pre_ignition": 0,
               "armed": 0, "dark_horse": 0}

    def add(category, sym, state, score=None):
        sym = str(sym or "").upper()
        if sym not in universe or sym in seen or counter[category] >= 5:
            return
        seen.add(sym)
        counter[category] += 1
        picked.append({"symbol": sym, "category": category, "state": state,
                       "score": score, "entry_verified": False})

    for ml in five():
        add("ml", ml["symbol"], ml["signal"], ml["rank_score"])

    for r in sorted(monsters, key=lambda x: (str(x.get("state")) in
         {"MONSTER-IGNITION", "MONSTER-HOT", "MONSTER-RESCUE"}, num(x.get("layers"))),
         reverse=True):
        add("monster", _symbol(r), str(r.get("state") or "MONSTER-WATCH"),
            num(r.get("layers")))

    pulls = [r for r in monsters if r.get("monsterPullbackState")]
    pulls.sort(key=lambda r: num(r.get("monsterExhaustionScore")), reverse=True)
    for r in pulls:
        add("pullback", _symbol(r), str(r.get("monsterPullbackState")),
            num(r.get("monsterExhaustionScore")))

    for r in sorted(rows, key=lambda r: num(r.get("hazard_score")), reverse=True):
        if r.get("state") not in {"NONE", "DATA_WAIT"}:
            add("pre_ignition", _symbol(r), str(r["state"]),
                num(r.get("hazard_score")))

    for r in board:
        if r.get("state") in {"BUY", "ARMED", "WATCH"}:
            add("armed", _symbol(r), "STRUCTURAL_" + str(r.get("state")),
                num(r.get("setup_strength")))

    for r in sorted(rows, key=lambda r: num(r.get("change_point_delta")), reverse=True):
        add("dark_horse", _symbol(r), "DISCOVERY_WATCH",
            num(r.get("change_point_delta")))

    # Guarantee 30 unique symbols without inventing qualifiers.
    for category in list(counter):
        for r in sorted(rows, key=lambda r: num(r.get("hazard_score")), reverse=True):
            if counter[category] >= 5:
                break
            add(category, _symbol(r), "UNQUALIFIED_WATCH", None)
        for sym in universe:
            if counter[category] >= 5:
                break
            add(category, sym, "UNQUALIFIED_WATCH", None)
    return picked[:30]


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
        "unqualified_are_not_buys": True,
    }
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
                report = thirty()
                print("PSI-V13 COVERAGE30 unique=" + str(len({r["symbol"] for r in report})) +
                      " rows=" + str(len(report)) +
                      " categories=6x5", flush=True)
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
          " ml=INDEPENDENT coverage=30 workers=DISCOVERY+LEARNING orders=DISABLED", flush=True)
