import asyncio
import json
import os
import time
from collections import Counter

import redis.asyncio as redis_async

CORE = None
AUTHORITY_CHAIN = "V12.3.4_LANES->V12.3.4_FAIL_CLOSED_AUTHORITY->BUY_NOW"
HARDENING_REVISION = "12.3.4-lowcap-early-explosion+rapid-guaranteed-promotion"
STICKY_KEY = os.getenv("PSI_MICRO_STICKY_KEY", "psi:v12:sticky-micro-pool").strip()
STRUCTURE_WORKERS = max(1, min(int(os.getenv("PSI_STRUCTURE_WORKERS", "2")), 8))
RISK_WORKERS = max(1, min(int(os.getenv("PSI_RISK_WORKERS", "2")), 8))
PRIORITY_SLOTS = 16
ROTATION_SLOTS = 2
ROTATION_PERIOD_S = 120.0
MAX_CHURN = 2
MIN_REBALANCE_S = 15.0
ACTIVITY_HUNTER_SLOTS = 48
ACTIVITY_GRACE_S = 120.0
ACTIVITY_RANK_REFRESH_S = 15.0
RAPID_PROMOTION_SLOTS = 10
RAPID_MIN_SCORE = 85.0
RAPID_CHURN_PER_CYCLE = 4
RAPID_DIAG_SECONDS = 15.0
LOWCAP_PROMOTION_SLOTS = 12
LOWCAP_MIN_SCORE = 65.0
LOWCAP_CHURN_PER_CYCLE = 4
LOWCAP_DIAG_SECONDS = 15.0
LOWCAP_PROXY_MIN_QUOTE_VOLUME_24H = 250_000.0
LOWCAP_PROXY_MAX_QUOTE_VOLUME_24H = 75_000_000.0
LOWCAP_MICRO_CAP_MAX_USD = 25_000_000.0
LOWCAP_LOW_CAP_MAX_USD = 300_000_000.0

_last_rebalance = 0.0
_rotation_epoch = 0
_protected_pool = []
_last_micro_diag = {}
_last_diag_mono = 0.0
_activity_rank_cache = []
_activity_rank_mono = 0.0
_last_rapid_diag_mono = 0.0
_last_lowcap_diag_mono = 0.0
_lowcap_rank_cache = []
_lowcap_rank_mono = 0.0
_inactive_since = {}
_original_gate = None
_original_scan = None
_original_health = None


def _core():
    if CORE is None:
        raise RuntimeError("V12.3 hardening is not installed")
    return CORE


def _strict_micro_ready(symbol):
    core = _core()
    try:
        mm = core.app.micro_metrics(symbol) or {}
    except Exception:
        return False
    return bool(
        mm.get("micro_ready")
        and mm.get("sequence_verified")
        and mm.get("book_sequence_verified")
    )


def _rapid_score(symbol):
    """Return the latest full-universe RAPID score without granting execution authority."""
    core = _core()
    try:
        row = ((getattr(core.q, "latest", {}) or {}).get(str(symbol).upper()) or {})
    except Exception:
        row = {}
    rapid = row.get("rapid_ignition") or {}
    values = [rapid.get("score"), row.get("rapid_score")]
    best = 0.0
    for value in values:
        try:
            best = max(best, float(value or 0.0))
        except (TypeError, ValueError):
            pass
    return best


def _rapid_ranked_symbols(universe, min_score=None):
    threshold = RAPID_MIN_SCORE if min_score is None else float(min_score)
    ranked = []
    for symbol in universe:
        score = _rapid_score(symbol)
        if score >= threshold:
            ranked.append((score, symbol))
    ranked.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [symbol for _, symbol in ranked]




def _f(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clamp01(value):
    return max(0.0, min(1.0, _f(value)))


def _scale(value, low, high):
    value = _f(value)
    if high <= low:
        return 0.0
    return _clamp01((value - low) / (high - low))


def _first_numeric(mapping, keys, default=0.0):
    mapping = mapping or {}
    for key in keys:
        if key not in mapping:
            continue
        try:
            value = float(mapping.get(key))
        except (TypeError, ValueError):
            continue
        return value
    return default


def _lowcap_cap_profile_values(quote_volume_24h=0.0, market_cap_usd=0.0):
    """Classify true market cap when available, otherwise use a labelled liquidity proxy.

    The proxy is discovery-only. It is never represented as a real market-cap value and
    it has no execution authority.
    """
    quote_volume_24h = max(0.0, _f(quote_volume_24h))
    market_cap_usd = max(0.0, _f(market_cap_usd))
    if market_cap_usd > 0.0:
        if market_cap_usd < LOWCAP_MICRO_CAP_MAX_USD:
            band = "MICRO_CAP"
        elif market_cap_usd < LOWCAP_LOW_CAP_MAX_USD:
            band = "LOW_CAP"
        elif market_cap_usd < 3_000_000_000.0:
            band = "MID_CAP"
        else:
            band = "LARGE_CAP"
        return {
            "eligible": band in {"MICRO_CAP", "LOW_CAP"},
            "band": band,
            "source": "MARKET_CAP",
            "market_cap_usd": market_cap_usd,
            "quote_volume_24h": quote_volume_24h,
        }

    proxy_min = max(
        0.0,
        _f(os.getenv("PSI_LOW_CAP_PROXY_MIN_QUOTE_VOLUME_24H", LOWCAP_PROXY_MIN_QUOTE_VOLUME_24H)),
    )
    proxy_max = max(
        proxy_min,
        _f(os.getenv("PSI_LOW_CAP_PROXY_MAX_QUOTE_VOLUME_24H", LOWCAP_PROXY_MAX_QUOTE_VOLUME_24H)),
    )
    eligible = proxy_min <= quote_volume_24h <= proxy_max
    return {
        "eligible": eligible,
        "band": "LOW_CAP_PROXY" if eligible else "OUTSIDE_LOW_CAP_PROXY",
        "source": "QUOTE_VOLUME_PROXY",
        "market_cap_usd": 0.0,
        "quote_volume_24h": quote_volume_24h,
    }


def _score_lowcap_candidate(
    symbol,
    quote_volume_24h=0.0,
    market_cap_usd=0.0,
    rapid_score=0.0,
    tape_metric=None,
    micro_metric=None,
    latest_row=None,
):
    """Pure low-cap early-explosion score.

    This score can only promote a symbol into deeper telemetry. It cannot create
    Pinpoint state, structural BUY, EXEC ARMED or BUY NOW.
    """
    tape_metric = tape_metric or {}
    micro_metric = micro_metric or {}
    latest_row = latest_row or {}
    cap = _lowcap_cap_profile_values(quote_volume_24h, market_cap_usd)
    if not cap["eligible"]:
        return {
            "symbol": str(symbol or "").upper(),
            "eligible": False,
            "score": 0.0,
            "state": "OUTSIDE_LOW_CAP_LANE",
            "cap_band": cap["band"],
            "cap_source": cap["source"],
            "market_cap_usd": cap["market_cap_usd"],
            "quote_volume_24h": cap["quote_volume_24h"],
            "role": "DISCOVERY_PROMOTION_ONLY",
            "execution_authority": False,
            "components": {},
        }

    n1_acc = _first_numeric(tape_metric, ("notional_accel_1s",), 0.0)
    c1_acc = _first_numeric(tape_metric, ("trade_count_accel_1s",), 0.0)
    trade_shift = _first_numeric(
        tape_metric,
        ("avg_trade_shift_1s",),
        _first_numeric(micro_metric, ("trade_size_shift",), 0.0),
    )
    n15 = max(0.0, _first_numeric(tape_metric, ("notional_15s",), 0.0))
    n30 = max(n15, _first_numeric(tape_metric, ("notional_30s",), n15))
    prior15 = max(0.0, n30 - n15)
    n15_ratio = (
        n15 / max(prior15, 1e-9)
        if prior15 > 0.0
        else (2.0 if n15 > 0.0 else 0.0)
    )

    rv30 = _first_numeric(micro_metric, ("relative_volume_30s",), 0.0)
    trade_acc = _first_numeric(micro_metric, ("trade_acceleration",), 0.0)
    pv5 = abs(_first_numeric(tape_metric, ("price_velocity_5s_pct",), 0.0))

    buy_ratio = _first_numeric(
        tape_metric,
        ("buy_ratio_1s", "buy_ratio_5s"),
        _first_numeric(micro_metric, ("aggressive_buy_ratio",), 0.5),
    )
    cvd_acc = _first_numeric(
        tape_metric,
        ("cvd_accel",),
        _first_numeric(micro_metric, ("cvd_acceleration",), 0.0),
    )
    ofi_acc = _first_numeric(micro_metric, ("ofi_acceleration",), 0.0)
    obi = _first_numeric(
        micro_metric,
        ("obi",),
        _first_numeric(tape_metric, ("bbo_imbalance",), 0.0),
    )
    ask_depletion = _first_numeric(micro_metric, ("ask_depletion",), 0.0)

    volume_strength = max(
        _scale(n15_ratio, 1.25, 4.0),
        _scale(n1_acc, 1.20, 3.5),
        _scale(rv30, 1.0, 3.0),
    )
    activity_strength = max(
        _scale(c1_acc, 1.20, 3.0),
        _scale(trade_acc, 1.0, 2.5),
    )
    # We deliberately reward participation expanding before price gets far away.
    quiet_price = 1.0 - _scale(pv5, 0.75, 3.0)
    volume_before_price = _clamp01(
        (0.65 * volume_strength + 0.35 * activity_strength) * quiet_price
    )

    buy_strength = _scale(buy_ratio, 0.55, 0.82)
    cvd_strength = _scale(cvd_acc, 0.04, 0.45)
    ofi_strength = _scale(ofi_acc, 0.01, 0.30)
    book_strength = max(_scale(obi, 0.02, 0.45), _scale(ask_depletion, 0.0, 0.30))
    flow_flip = _clamp01(
        0.38 * buy_strength
        + 0.32 * cvd_strength
        + 0.18 * ofi_strength
        + 0.12 * book_strength
    )

    liquidity_shift = _clamp01(
        0.40 * max(_scale(n1_acc, 1.2, 3.5), _scale(n15_ratio, 1.25, 4.0))
        + 0.35 * max(_scale(c1_acc, 1.2, 3.0), _scale(trade_acc, 1.0, 2.5))
        + 0.25 * _scale(trade_shift, 1.15, 2.5)
    )

    resistance_weakness = _first_numeric(
        latest_row,
        (
            "resistance_fatigue",
            "resistance_fatigue_score",
            "resistance_weakness",
            "resWeak",
            "res_weak",
        ),
        0.0,
    )
    resistance_attacks = _first_numeric(
        latest_row,
        ("resistance_attacks", "attacks", "tests"),
        0.0,
    )
    resistance_fatigue = max(
        _scale(resistance_weakness, 15.0, 70.0),
        _scale(resistance_attacks, 1.0, 4.0),
    )

    state_blob = " ".join(
        str(latest_row.get(key) or "").upper()
        for key in (
            "state",
            "pullback_state",
            "pullback",
            "formal",
            "monster_state",
            "status",
        )
    )
    pullback_exhaustion = 0.0
    if "PULLBACK_EXHAUSTED" in state_blob:
        pullback_exhaustion = 1.0
    elif "SELL_PRESSURE_EXHAUSTING" in state_blob or "SELLERS_EXHAUSTING" in state_blob:
        pullback_exhaustion = 0.65

    rapid_component = _scale(rapid_score, RAPID_MIN_SCORE, 150.0)

    score = (
        30.0 * volume_before_price
        + 25.0 * flow_flip
        + 20.0 * liquidity_shift
        + 15.0 * resistance_fatigue
        + 5.0 * pullback_exhaustion
        + 5.0 * rapid_component
    )
    # Anti-chase applies to promotion too: the engine is designed to catch the
    # participation regime shift before the obvious vertical candle.
    if pv5 >= 3.0:
        score -= min(25.0, 8.0 + (pv5 - 3.0) * 4.0)
    score = max(0.0, min(100.0, score))

    if score >= 90.0:
        state = "RAPID_PROMOTION"
    elif score >= 75.0:
        state = "HOT"
    elif score >= 60.0:
        state = "PRE-IGNITION"
    elif score >= 45.0:
        state = "WAKING"
    else:
        state = "WATCH"

    return {
        "symbol": str(symbol or "").upper(),
        "eligible": True,
        "score": round(score, 2),
        "state": state,
        "cap_band": cap["band"],
        "cap_source": cap["source"],
        "market_cap_usd": cap["market_cap_usd"],
        "quote_volume_24h": cap["quote_volume_24h"],
        "rapid_score": round(_f(rapid_score), 2),
        "price_velocity_5s_pct": round(pv5, 4),
        "buy_ratio_1s": round(buy_ratio, 4),
        "cvd_accel": round(cvd_acc, 4),
        "role": "DISCOVERY_PROMOTION_ONLY",
        "execution_authority": False,
        "components": {
            "volume_before_price": round(volume_before_price * 100.0, 1),
            "flow_flip": round(flow_flip * 100.0, 1),
            "liquidity_shift": round(liquidity_shift * 100.0, 1),
            "resistance_fatigue": round(resistance_fatigue * 100.0, 1),
            "pullback_exhaustion": round(pullback_exhaustion * 100.0, 1),
            "rapid": round(rapid_component * 100.0, 1),
        },
    }


def _lowcap_signal(symbol):
    core = _core()
    symbol = str(symbol or "").upper()
    row = ((getattr(core.q, "latest", {}) or {}).get(symbol) or {})
    meta = (getattr(core.app, "symbol_meta", {}) or {}).get(symbol, {}) or {}
    quote_volume = _first_numeric(
        meta,
        ("quote_volume_24h",),
        _first_numeric(row, ("quote_volume_24h", "quoteVolume"), 0.0),
    )
    market_cap = _first_numeric(
        meta,
        ("market_cap_usd", "market_cap"),
        _first_numeric(row, ("market_cap_usd", "market_cap", "marketCap"), 0.0),
    )
    try:
        tape_metric = core.tape.tape_metric(symbol) or {}
    except Exception:
        tape_metric = {}
    try:
        micro_metric = core.app.micro_metrics(symbol) or {}
    except Exception:
        micro_metric = {}
    return _score_lowcap_candidate(
        symbol,
        quote_volume_24h=quote_volume,
        market_cap_usd=market_cap,
        rapid_score=_rapid_score(symbol),
        tape_metric=tape_metric,
        micro_metric=micro_metric,
        latest_row=row,
    )


def _lowcap_ranked_details(universe, refresh_seconds=5.0):
    global _lowcap_rank_cache, _lowcap_rank_mono
    now_mono = time.monotonic()
    universe_set = set(universe)
    if _lowcap_rank_cache and now_mono - _lowcap_rank_mono < max(2.0, float(refresh_seconds)):
        return [row for row in _lowcap_rank_cache if row.get("symbol") in universe_set]

    try:
        _core()._refresh_tape_snapshots_sync(force=True)
    except Exception:
        pass

    rows = []
    for symbol in universe:
        row = _lowcap_signal(symbol)
        if row.get("eligible"):
            rows.append(row)
    rows.sort(
        key=lambda row: (
            _f(row.get("score")),
            _f((row.get("components") or {}).get("volume_before_price")),
            _f((row.get("components") or {}).get("flow_flip")),
            -_f(row.get("quote_volume_24h")),
            row.get("symbol", ""),
        ),
        reverse=True,
    )
    _lowcap_rank_cache = rows
    _lowcap_rank_mono = now_mono
    return list(rows)


def _lowcap_ranked_symbols(universe, min_score=None):
    threshold = LOWCAP_MIN_SCORE if min_score is None else _f(min_score, LOWCAP_MIN_SCORE)
    return [
        row["symbol"]
        for row in _lowcap_ranked_details(universe)
        if _f(row.get("score")) >= threshold
    ]


def _lowcap_summary(limit=12):
    core = _core()
    universe = list(getattr(core.q, "universe", []) or [])
    rows = _lowcap_ranked_details(universe)[:max(1, int(limit))]
    return {
        "revision": HARDENING_REVISION,
        "role": "DISCOVERY_PROMOTION_ONLY",
        "execution_authority": False,
        "true_market_cap_when_available": True,
        "fallback_cap_source": "QUOTE_VOLUME_PROXY",
        "promotion_threshold": _f(
            os.getenv("PSI_LOW_CAP_MIN_SCORE", LOWCAP_MIN_SCORE),
            LOWCAP_MIN_SCORE,
        ),
        "top": rows,
    }


def _activity_sort_key(metric, quote_volume=0.0, rapid_score=0.0):
    metric = metric or {}
    try:
        age = float(metric.get("age_ms", 999999.0) or 999999.0)
    except (TypeError, ValueError):
        age = 999999.0
    try:
        book_age = float(metric.get("book_age_ms", 999999.0) or 999999.0)
    except (TypeError, ValueError):
        book_age = 999999.0
    try:
        trades5 = int(metric.get("trades_5s") or 0)
    except (TypeError, ValueError):
        trades5 = 0
    try:
        notional5 = float(metric.get("notional_5s") or 0.0)
    except (TypeError, ValueError):
        notional5 = 0.0
    try:
        spread = float(metric.get("spread_bps", 999.0) or 999.0)
    except (TypeError, ValueError):
        spread = 999.0
    try:
        quote_volume = float(quote_volume or 0.0)
    except (TypeError, ValueError):
        quote_volume = 0.0
    try:
        rapid_score = float(rapid_score or 0.0)
    except (TypeError, ValueError):
        rapid_score = 0.0

    ready = bool(metric.get("ready")) and age <= 1500.0
    rapid_qualified = rapid_score >= RAPID_MIN_SCORE
    return (
        1 if rapid_qualified else 0,
        min(rapid_score, 250.0) if rapid_qualified else 0.0,
        1 if ready else 0,
        1 if age <= 1500.0 else 0,
        min(trades5, 100),
        min(notional5, 10_000_000.0),
        1 if book_age <= 5000.0 else 0,
        -min(spread, 999.0),
        quote_volume,
    )


def _activity_ranked_symbols(universe, refresh_seconds=ACTIVITY_RANK_REFRESH_S):
    global _activity_rank_cache, _activity_rank_mono
    core = _core()
    now_mono = time.monotonic()
    if (
        _activity_rank_cache
        and now_mono - _activity_rank_mono < max(5.0, float(refresh_seconds))
    ):
        universe_set = set(universe)
        return [sym for sym in _activity_rank_cache if sym in universe_set]

    try:
        core._refresh_tape_snapshots_sync(force=True)
    except Exception:
        pass

    meta = getattr(core.app, "symbol_meta", {}) or {}
    ranked = []
    for sym in universe:
        try:
            tm = core.tape.tape_metric(sym) or {}
        except Exception:
            tm = {}
        qv = float((meta.get(sym, {}) or {}).get("quote_volume_24h", 0.0) or 0.0)
        rapid_score = _rapid_score(sym)
        ranked.append((_activity_sort_key(tm, qv, rapid_score), sym))
    ranked.sort(reverse=True)
    _activity_rank_cache = [sym for _, sym in ranked]
    _activity_rank_mono = now_mono
    return list(_activity_rank_cache)


def _strict_trade_activity_ok(symbol):
    core = _core()
    try:
        core._refresh_micro_snapshots_sync()
    except Exception:
        pass
    trade = core._distributed_micro_trade.get(str(symbol).upper())
    if not isinstance(trade, dict):
        return False
    now_ms = int(time.time() * 1000)
    snapshot_ms = int(trade.get("_snapshot_ms") or 0)
    transport = max(0, now_ms - snapshot_ms) if snapshot_ms > 0 else 999999999
    try:
        age = float(trade.get("trade_age_ms", 999999999.0)) + transport
    except (TypeError, ValueError):
        age = 999999999.0
    try:
        count = int(trade.get("trade_count_60s") or 0)
    except (TypeError, ValueError):
        count = 0
    return bool(age <= 15000.0 and count >= 10)


def stable_micro_symbols():
    """Keep execution micro coverage sticky while preserving full discovery.

    The protected pool is private to this hardening layer. Core/legacy modules
    may still mutate their own candidate lists, but they cannot replace the
    execution subscription set wholesale. RAPID challengers receive bounded,
    guaranteed access to micro hydration; they still have zero execution
    authority until the unchanged V12.3.4 fail-closed gate approves them.
    """
    global _last_rebalance, _rotation_epoch, _protected_pool, _inactive_since
    global _last_rapid_diag_mono, _last_lowcap_diag_mono

    core = _core()
    pool_size = int(core.REDIS_MICRO_POOL_SIZE)
    priority_slots = max(
        8, min(int(os.getenv("PSI_MICRO_PRIORITY_SLOTS", str(PRIORITY_SLOTS))), pool_size)
    )
    rotation_slots = max(
        1, min(int(os.getenv("PSI_MICRO_ROTATION_SLOTS", str(ROTATION_SLOTS))), pool_size)
    )
    rotation_period = max(
        60.0, float(os.getenv("PSI_MICRO_ROTATION_PERIOD_S", str(ROTATION_PERIOD_S)))
    )
    max_churn = max(
        1, min(int(os.getenv("PSI_MICRO_MAX_CHURN_PER_CYCLE", str(MAX_CHURN))), 12)
    )
    min_rebalance = max(
        5.0, float(os.getenv("PSI_MICRO_MIN_REBALANCE_S", str(MIN_REBALANCE_S)))
    )
    activity_hunter_slots = max(
        8,
        min(
            int(os.getenv("PSI_MICRO_ACTIVITY_HUNTER_SLOTS", str(ACTIVITY_HUNTER_SLOTS))),
            max(8, pool_size - priority_slots),
        ),
    )
    activity_grace = max(
        60.0, float(os.getenv("PSI_MICRO_ACTIVITY_GRACE_S", str(ACTIVITY_GRACE_S)))
    )
    rapid_slots = max(
        2,
        min(int(os.getenv("PSI_MICRO_RAPID_SLOTS", str(RAPID_PROMOTION_SLOTS))), max(2, pool_size // 3)),
    )
    rapid_min_score = max(
        50.0, float(os.getenv("PSI_MICRO_RAPID_MIN_SCORE", str(RAPID_MIN_SCORE)))
    )
    rapid_churn = max(
        1,
        min(int(os.getenv("PSI_MICRO_RAPID_CHURN_PER_CYCLE", str(RAPID_CHURN_PER_CYCLE))), 8),
    )
    lowcap_slots = max(
        4,
        min(
            int(os.getenv("PSI_MICRO_LOWCAP_SLOTS", str(LOWCAP_PROMOTION_SLOTS))),
            max(4, pool_size // 3),
        ),
    )
    lowcap_min_score = max(
        45.0, _f(os.getenv("PSI_LOW_CAP_MIN_SCORE", LOWCAP_MIN_SCORE), LOWCAP_MIN_SCORE)
    )
    lowcap_churn = max(
        1,
        min(
            int(os.getenv("PSI_MICRO_LOWCAP_CHURN_PER_CYCLE", str(LOWCAP_CHURN_PER_CYCLE))),
            8,
        ),
    )

    desired = []
    seen = set()

    def add_desired(symbol):
        symbol = str(symbol or "").upper()
        if symbol.endswith("USDT") and symbol not in seen:
            seen.add(symbol)
            desired.append(symbol)

    # Execution priority is V12 board first, then the already activity-aware
    # legacy micro ranking. This does not change any BUY condition.
    try:
        for row in core._board():
            add_desired(row.get("symbol"))
            if len(desired) >= priority_slots:
                break
    except Exception:
        pass

    for symbol in list(getattr(core.app, "selected_micro_symbols", []) or []):
        add_desired(symbol)

    universe = list(getattr(core.q, "universe", []) or [])
    universe_set = set(universe)

    activity_ranked = _activity_ranked_symbols(universe)
    rapid_ranked = _rapid_ranked_symbols(universe, rapid_min_score)
    lowcap_details = _lowcap_ranked_details(universe)
    lowcap_ranked = [
        row["symbol"] for row in lowcap_details
        if _f(row.get("score")) >= lowcap_min_score
    ]
    priority = [symbol for symbol in desired[:priority_slots] if symbol in universe_set]
    priority_set = set(priority)
    lowcap_priority = [
        symbol for symbol in lowcap_ranked[:lowcap_slots]
        if symbol in universe_set and symbol not in priority_set
    ]
    lowcap_priority_set = set(lowcap_priority)
    rapid_priority = [
        symbol for symbol in rapid_ranked[:rapid_slots]
        if (
            symbol in universe_set
            and symbol not in priority_set
            and symbol not in lowcap_priority_set
        )
    ]
    protected_priority_set = priority_set | lowcap_priority_set | set(rapid_priority)

    # Reserve guaranteed low-cap early-explosion and RAPID challenger tiers
    # before normal activity hunters. Both are discovery/promotion only; final
    # execution remains entirely under the unchanged V12.3.4 fail-closed gate.
    activity_hunters = []
    for symbol in activity_ranked:
        if symbol in universe_set and symbol not in protected_priority_set:
            activity_hunters.append(symbol)
        if len(activity_hunters) >= activity_hunter_slots:
            break

    ordered = []
    ordered_seen = set()
    for symbol in priority + lowcap_priority + rapid_priority + activity_hunters + desired + universe:
        if symbol in universe_set and symbol not in ordered_seen:
            ordered_seen.add(symbol)
            ordered.append(symbol)
    desired = ordered

    if len(desired) < pool_size:
        try:
            meta = getattr(core.app, "symbol_meta", {}) or {}
            liquid = sorted(
                universe,
                key=lambda sym: float(
                    (meta.get(sym, {}) or {}).get("quote_volume_24h", 0.0) or 0.0
                ),
                reverse=True,
            )
            for symbol in liquid:
                add_desired(symbol)
                if len(desired) >= pool_size:
                    break
        except Exception:
            pass

    current = [
        str(symbol).upper()
        for symbol in list(_protected_pool or [])
        if str(symbol).upper().endswith("USDT")
    ]
    if not current:
        current = [
            str(symbol).upper()
            for symbol in list(core._distributed_micro_sticky_pool or [])
            if str(symbol).upper().endswith("USDT")
        ]

    if not universe:
        if current:
            _protected_pool[:] = current[:pool_size]
            core._distributed_micro_sticky_pool = list(_protected_pool)
            return list(_protected_pool)
        _protected_pool[:] = desired[:pool_size]
        core._distributed_micro_sticky_pool = list(_protected_pool)
        return list(_protected_pool)

    current = [symbol for symbol in current if symbol in universe_set]
    if not current:
        _protected_pool[:] = [
            symbol for symbol in desired if symbol in universe_set
        ][:pool_size]
        core._distributed_micro_sticky_pool = list(_protected_pool)
        _last_rebalance = time.monotonic()
        core._redis_bridge_stats["protected_pool_seeded"] = len(_protected_pool)
        return list(_protected_pool)

    now_mono = time.monotonic()
    can_rebalance = now_mono - _last_rebalance >= min_rebalance
    lowcap_missing = [symbol for symbol in lowcap_priority if symbol not in current]
    rapid_missing = [symbol for symbol in rapid_priority if symbol not in current]
    budget = max_churn if can_rebalance else 0
    if can_rebalance and (lowcap_missing or rapid_missing):
        # Early low-cap and RAPID challengers can accelerate faster than the
        # normal sticky-pool churn rate. Permit only a bounded special-access
        # burst; this changes hydration priority, never BUY authority.
        special_needed = (
            min(lowcap_churn, len(lowcap_missing))
            + min(rapid_churn, len(rapid_missing))
        )
        budget = min(12, max(budget, special_needed))

    warmed = [symbol for symbol in current if _strict_micro_ready(symbol)]
    warmed_set = set(warmed)

    # Hunter slots get a sustained-activity grace period. Priority V12
    # candidates are never demoted solely for quiet tape, and verified symbols
    # remain first-class retained history.
    inactive = []
    active_warming = []
    for symbol in current:
        if symbol in protected_priority_set or symbol in warmed_set:
            _inactive_since.pop(symbol, None)
            active_warming.append(symbol)
            continue
        if _strict_trade_activity_ok(symbol):
            _inactive_since.pop(symbol, None)
            active_warming.append(symbol)
            continue
        since = _inactive_since.setdefault(symbol, now_mono)
        if now_mono - since >= activity_grace:
            inactive.append(symbol)
        else:
            active_warming.append(symbol)

    # Drop timers for symbols no longer in the live protected pool.
    for symbol in list(_inactive_since):
        if symbol not in current:
            _inactive_since.pop(symbol, None)

    inject = []
    if budget:
        # Low-cap early-explosion candidates get bounded guaranteed hydration.
        lowcap_target = min(budget, lowcap_churn)
        for symbol in lowcap_priority:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= lowcap_target:
                break

        # Then reserve bounded access for full-universe RAPID challengers.
        rapid_target = min(budget, len(inject) + rapid_churn)
        for symbol in rapid_priority:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= rapid_target:
                break

        # Then preserve normal V12 execution-priority admission.
        for symbol in priority:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= budget:
                break

    # If hunter slots have been persistently unable to satisfy the unchanged
    # strict Trade gate, use the remaining churn budget on the highest-current
    # full-universe tape challengers.
    if inactive and len(inject) < budget:
        for symbol in activity_hunters:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= budget:
                break

    epoch = int(time.time() / rotation_period)
    if can_rebalance and epoch != _rotation_epoch:
        _rotation_epoch = epoch
        rotation_budget = min(rotation_slots, max(0, budget - len(inject)))
        if rotation_budget:
            start = (epoch * rotation_budget) % len(universe)
            checked = 0
            while rotation_budget > 0 and checked < len(universe):
                symbol = universe[(start + checked) % len(universe)]
                checked += 1
                if symbol not in current and symbol not in inject:
                    inject.append(symbol)
                    rotation_budget -= 1

    out = []
    out_seen = set()

    def add_out(symbol):
        symbol = str(symbol or "").upper()
        if (
            symbol in universe_set
            and symbol.endswith("USDT")
            and symbol not in out_seen
            and len(out) < pool_size
        ):
            out_seen.add(symbol)
            out.append(symbol)

    for symbol in inject:
        add_out(symbol)
    for symbol in priority:
        add_out(symbol)
    for symbol in warmed:
        add_out(symbol)
    for symbol in active_warming:
        add_out(symbol)
    # Sustained inactive hunters are retained only if no better candidate can
    # use the bounded replacement budget.
    for symbol in inactive:
        add_out(symbol)
    for symbol in desired:
        add_out(symbol)
    for symbol in universe:
        add_out(symbol)

    previous = set(current)
    next_pool = out[:pool_size]
    actual_added = len(set(next_pool) - previous)
    actual_removed = len(previous - set(next_pool))

    if inject:
        _last_rebalance = now_mono
        core._redis_bridge_stats["micro_last_churn"] = len(inject)
        core._redis_bridge_stats["micro_last_churn_ms"] = int(time.time() * 1000)

    core._redis_bridge_stats["micro_actual_added"] = actual_added
    core._redis_bridge_stats["micro_actual_removed"] = actual_removed
    core._redis_bridge_stats["micro_sustained_inactive"] = len(inactive)
    core._redis_bridge_stats["micro_activity_hunters"] = len(activity_hunters)
    core._redis_bridge_stats["micro_lowcap_candidates"] = len(lowcap_details)
    core._redis_bridge_stats["micro_lowcap_qualified"] = len(lowcap_ranked)
    core._redis_bridge_stats["micro_lowcap_reserved"] = len(lowcap_priority)
    core._redis_bridge_stats["micro_lowcap_in_pool"] = sum(
        symbol in set(next_pool) for symbol in lowcap_priority
    )
    core._redis_bridge_stats["micro_lowcap_missing"] = sum(
        symbol not in set(next_pool) for symbol in lowcap_priority
    )
    core._redis_bridge_stats["micro_rapid_eligible"] = len(rapid_ranked)
    core._redis_bridge_stats["micro_rapid_reserved"] = len(rapid_priority)
    core._redis_bridge_stats["micro_rapid_in_pool"] = sum(
        symbol in set(next_pool) for symbol in rapid_priority
    )
    core._redis_bridge_stats["micro_rapid_missing"] = sum(
        symbol not in set(next_pool) for symbol in rapid_priority
    )
    core._redis_bridge_stats["micro_warmed_retained"] = sum(
        symbol in set(next_pool) for symbol in warmed
    )

    if now_mono - _last_lowcap_diag_mono >= LOWCAP_DIAG_SECONDS:
        _last_lowcap_diag_mono = now_mono
        top_lowcap = ",".join(
            f"{row.get('symbol')}:{_f(row.get('score')):.1f}/{row.get('state')}"
            f"/VBP{_f((row.get('components') or {}).get('volume_before_price')):.0f}"
            f"/FLOW{_f((row.get('components') or {}).get('flow_flip')):.0f}"
            for row in lowcap_details[:min(8, len(lowcap_details))]
        ) or "-"
        missing_lowcap = ",".join(
            symbol for symbol in lowcap_priority if symbol not in set(next_pool)
        ) or "-"
        print(
            f"Ψ-V12.3 LOWCAP PROMOTION candidates={len(lowcap_details)} "
            f"qualified={len(lowcap_ranked)} reserved={len(lowcap_priority)} "
            f"inPool={core._redis_bridge_stats['micro_lowcap_in_pool']}/{len(lowcap_priority)} "
            f"missing={missing_lowcap} injected={','.join([s for s in inject if s in lowcap_priority]) or '-'} "
            f"top={top_lowcap}",
            flush=True,
        )

    if now_mono - _last_rapid_diag_mono >= RAPID_DIAG_SECONDS:
        _last_rapid_diag_mono = now_mono
        top_rapid = ",".join(
            f"{symbol}:{_rapid_score(symbol):.1f}"
            for symbol in rapid_ranked[:min(8, len(rapid_ranked))]
        ) or "-"
        missing_rapid = ",".join(
            symbol for symbol in rapid_priority if symbol not in set(next_pool)
        ) or "-"
        print(
            f"Ψ-V12.3 RAPID PROMOTION eligible={len(rapid_ranked)} "
            f"reserved={len(rapid_priority)} "
            f"inPool={core._redis_bridge_stats['micro_rapid_in_pool']}/{len(rapid_priority)} "
            f"missing={missing_rapid} injected={','.join(inject) or '-'} "
            f"top={top_rapid}",
            flush=True,
        )

    _protected_pool[:] = next_pool
    core._distributed_micro_sticky_pool = list(_protected_pool)
    return list(_protected_pool)


def micro_gate_diagnostics():
    """Aggregate the strict distributed micro gate without relaxing any gate."""
    core = _core()
    try:
        core._refresh_micro_snapshots_sync(force=True)
    except Exception:
        pass

    symbols = list(_protected_pool or core._distributed_micro_sticky_pool or [])
    symbols = [str(s).upper() for s in symbols if str(s).upper().endswith("USDT")]
    counts = Counter()
    combos = Counter()
    low_activity = []

    now_ms = int(time.time() * 1000)
    for sym in symbols:
        trade = core._distributed_micro_trade.get(sym)
        book = core._distributed_micro_book.get(sym)
        both = isinstance(trade, dict) and isinstance(book, dict)
        if both:
            counts["present_both"] += 1
        else:
            missing = []
            if not isinstance(trade, dict):
                missing.append("NO_TRADE_SNAPSHOT")
            if not isinstance(book, dict):
                missing.append("NO_BOOK_SNAPSHOT")
            combos["+".join(missing) or "MISSING_SNAPSHOT"] += 1
            continue

        trade_snapshot_ms = int(trade.get("_snapshot_ms") or 0)
        book_snapshot_ms = int(book.get("_snapshot_ms") or 0)
        trade_transport = max(0, now_ms - trade_snapshot_ms) if trade_snapshot_ms > 0 else 999999999
        book_transport = max(0, now_ms - book_snapshot_ms) if book_snapshot_ms > 0 else 999999999

        trade_age = float(trade.get("trade_age_ms", 999999999.0)) + trade_transport
        book_age = float(book.get("book_age_ms", 999999999.0)) + book_transport
        trade_fresh = trade_age <= 15000.0
        book_fresh = book_age <= 5000.0
        trade_seq = bool(trade.get("sequence_verified"))
        book_seq = bool(book.get("book_sequence_verified"))
        trade_count = int(trade.get("trade_count_60s") or 0)
        ofi_samples = int(book.get("ofi_samples") or 0)
        book_updates = int(book.get("book_updates") or 0)

        conds = {
            "TRADE_STALE": not trade_fresh,
            "BOOK_STALE": not book_fresh,
            "TRADE_SEQ": not trade_seq,
            "BOOK_SEQ": not book_seq,
            "TRADE_COUNT": trade_count < 10,
            "OFI_SAMPLES": ofi_samples < 6,
            "BOOK_UPDATES": book_updates < 8,
        }

        if trade_fresh:
            counts["trade_fresh"] += 1
        if book_fresh:
            counts["book_fresh"] += 1
        if trade_seq:
            counts["trade_sequence_verified"] += 1
        if book_seq:
            counts["book_sequence_verified"] += 1
        if trade_count >= 10:
            counts["trade_count_ge_10"] += 1
        if ofi_samples >= 6:
            counts["ofi_samples_ge_6"] += 1
        if book_updates >= 8:
            counts["book_updates_ge_8"] += 1

        micro_ready = (
            trade_fresh and book_fresh
            and trade_count >= 10
            and ofi_samples >= 6
            and book_updates >= 8
        )
        if micro_ready:
            counts["micro_ready"] += 1
        if micro_ready and trade_seq and book_seq:
            counts["micro_verified"] += 1

        failed = [name for name, is_failed in conds.items() if is_failed]
        combos["+".join(failed) if failed else "PASS_ALL"] += 1

        if failed == ["TRADE_COUNT"] and len(low_activity) < 12:
            low_activity.append({
                "symbol": sym,
                "trade_count_60s": trade_count,
                "trade_age_ms": round(trade_age, 1),
                "book_age_ms": round(book_age, 1),
                "ofi_samples": ofi_samples,
                "book_updates": book_updates,
            })

    return {
        "revision": HARDENING_REVISION,
        "pool": len(symbols),
        "counts": dict(counts),
        "failure_combinations": [
            {"failure": name, "count": count}
            for name, count in combos.most_common(15)
        ],
        "low_activity_examples": low_activity,
        "generated_ms": now_ms,
    }


def _heartbeat_ready(name, timestamp_key, count_key="symbols", max_age_ms=15000):
    core = _core()
    hb = core._redis_worker_health.get(name) or {}
    if not isinstance(hb, dict):
        return False
    try:
        timestamp = int(hb.get(timestamp_key) or 0)
        count = int(hb.get(count_key) or 0)
    except (TypeError, ValueError):
        return False
    age = int(time.time() * 1000) - timestamp if timestamp > 0 else 999999999
    return bool(
        count > 0
        and not str(hb.get("error") or "")
        and 0 <= age <= max_age_ms
    )


def authority_lane_health():
    core = _core()
    universe_count = len(list(getattr(core.q, "universe", []) or []))
    structure_ready = sum(
        _heartbeat_ready(f"structure{idx}", "heartbeat_ms", "assigned")
        for idx in range(STRUCTURE_WORKERS)
    )
    tape_ready = sum(
        _heartbeat_ready(f"tape{idx}", "last_event_ms")
        for idx in range(core.REDIS_TAPE_WORKERS)
    )
    risk_ready = sum(
        _heartbeat_ready(f"risk{idx}", "heartbeat_ms")
        for idx in range(RISK_WORKERS)
    )

    lanes = {
        "DISCOVERY": {"ready": universe_count > 0, "symbols": universe_count},
        "STRUCTURE": {
            "ready": structure_ready == STRUCTURE_WORKERS,
            "workers_ready": structure_ready,
            "workers_required": STRUCTURE_WORKERS,
        },
        "TAPE": {
            "ready": tape_ready == core.REDIS_TAPE_WORKERS,
            "workers_ready": tape_ready,
            "workers_required": core.REDIS_TAPE_WORKERS,
        },
        "TRADE": {"ready": _heartbeat_ready("trade", "last_event_ms")},
        "BOOK": {"ready": _heartbeat_ready("book", "last_event_ms")},
        "RISK": {
            "ready": risk_ready == RISK_WORKERS,
            "workers_ready": risk_ready,
            "workers_required": RISK_WORKERS,
        },
    }
    blockers = [name for name, row in lanes.items() if not bool(row.get("ready"))]
    return {
        "authority_version": core.VERSION,
        "all_ready": not blockers,
        "blockers": blockers,
        "lanes": lanes,
        "generated_ms": int(time.time() * 1000),
    }


def micro_coverage():
    core = _core()
    symbols = list(core._distributed_micro_sticky_pool[: core.REDIS_MICRO_POOL_SIZE])
    ready = 0
    verified = 0
    for symbol in symbols:
        try:
            mm = core.app.micro_metrics(symbol) or {}
        except Exception:
            continue
        is_ready = bool(mm.get("micro_ready"))
        is_verified = bool(
            is_ready
            and mm.get("sequence_verified")
            and mm.get("book_sequence_verified")
        )
        ready += int(is_ready)
        verified += int(is_verified)
    return {
        "selected": len(symbols),
        "target": core.REDIS_MICRO_POOL_SIZE,
        "micro_ready": ready,
        "micro_verified": verified,
    }


def _gate_wrapper(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    result = _original_gate(
        structural_row, legacy_row, micro_metrics, integrity
    )
    lane_health = authority_lane_health()
    blockers = list(result.get("blockers") or [])
    for lane in list(lane_health.get("blockers") or []):
        marker = f"AUTHORITY_LANE_{str(lane).upper()}_UNAVAILABLE"
        if marker not in blockers:
            blockers.append(marker)

    result = dict(result)
    result["blockers"] = blockers
    result["authority_chain"] = AUTHORITY_CHAIN
    result["authority_ready"] = bool(lane_health.get("all_ready"))
    if blockers:
        result["buy_now"] = False
        if isinstance(structural_row, dict) and structural_row.get("state") == "BUY":
            result["execution_state"] = "COLLECTING DATA"
        else:
            result["execution_state"] = "NOT_ELIGIBLE"
    return result


def _augment_response(response):
    core = _core()
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response

    lane_health = authority_lane_health()
    data["execution_authority"] = "V12.3_FAIL_CLOSED"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["hardening_revision"] = HARDENING_REVISION
    data["authority_lane_health"] = lane_health
    data["authority_ready"] = bool(lane_health.get("all_ready"))
    data["legacy_pinpoint_role"] = "INPUT_TELEMETRY_ONLY"
    data["low_cap_early_explosion"] = _lowcap_summary()
    distributed = data.setdefault("distributed_micro", {})
    distributed["coverage"] = micro_coverage()
    if _last_micro_diag:
        distributed["gate_diagnostics"] = dict(_last_micro_diag)
    return core.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    return _augment_response(await _original_scan(request))


async def _health_wrapper(request):
    return _augment_response(await _original_health(request))


async def bootstrap():
    global _protected_pool
    core = _core()
    if not core.REDIS_URL:
        return
    client = redis_async.from_url(
        core.REDIS_URL, encoding="utf-8", decode_responses=True
    )
    try:
        await client.ping()
        raw = await client.get(STICKY_KEY)
        if raw:
            try:
                payload = json.loads(raw)
            except Exception:
                payload = {}
            symbols = payload.get("symbols") if isinstance(payload, dict) else payload
            restored = []
            for symbol in symbols or []:
                symbol = str(symbol or "").upper()
                if symbol.endswith("USDT") and symbol not in restored:
                    restored.append(symbol)
                if len(restored) >= core.REDIS_MICRO_POOL_SIZE:
                    break
            if restored:
                _protected_pool[:] = restored
                core._distributed_micro_sticky_pool = list(_protected_pool)
                core._redis_bridge_stats["sticky_restored"] = len(restored)
                print(
                    f"Ψ-V12.3.2 HARDENING restoredProtected={len(restored)}",
                    flush=True,
                )
    except Exception as exc:
        core._redis_bridge_stats["hardening_bootstrap_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
    finally:
        await client.aclose()


async def supervisor_loop():
    global _last_micro_diag, _last_diag_mono
    core = _core()
    if not core.REDIS_URL:
        core._redis_bridge_stats["hardening_supervisor_disabled"] = 1
        return

    while True:
        client = None
        try:
            client = redis_async.from_url(
                core.REDIS_URL, encoding="utf-8", decode_responses=True
            )
            await client.ping()
            print(
                f"Ψ-V12.3 HARDENING supervisor structure={STRUCTURE_WORKERS} risk={RISK_WORKERS}",
                flush=True,
            )
            while True:
                now_ms = int(time.time() * 1000)
                await client.set(
                    STICKY_KEY,
                    json.dumps(
                        {
                            "version": core.VERSION,
                            "authority": "V12_ONLY",
                            "symbols": list(_protected_pool),
                            "generated_ms": now_ms,
                        },
                        separators=(",", ":"),
                    ),
                    ex=86400,
                )

                for idx in range(STRUCTURE_WORKERS):
                    raw = await client.get(f"psi:v12:structure-worker:{idx}")
                    if raw:
                        try:
                            core._redis_worker_health[f"structure{idx}"] = json.loads(raw)
                        except Exception:
                            core._redis_worker_health[f"structure{idx}"] = {"raw": raw}

                for idx in range(RISK_WORKERS):
                    raw = await client.get(f"psi:v12:risk-worker:{idx}")
                    if raw:
                        try:
                            core._redis_worker_health[f"risk{idx}"] = json.loads(raw)
                        except Exception:
                            core._redis_worker_health[f"risk{idx}"] = {"raw": raw}

                core._redis_bridge_stats["hardening_last_ms"] = now_ms

                now_mono = time.monotonic()
                if now_mono - _last_diag_mono >= 30.0:
                    try:
                        _last_micro_diag = await asyncio.to_thread(micro_gate_diagnostics)
                        _last_diag_mono = now_mono
                        dc = _last_micro_diag.get("counts", {})
                        top_fail = (_last_micro_diag.get("failure_combinations") or [{}])[0]
                        print(
                            "Ψ-V12.3.3 MICRO_DIAG "
                            f"pool={_last_micro_diag.get('pool', 0)} "
                            f"both={dc.get('present_both', 0)} "
                            f"tradeFresh={dc.get('trade_fresh', 0)} "
                            f"bookFresh={dc.get('book_fresh', 0)} "
                            f"tradeSeq={dc.get('trade_sequence_verified', 0)} "
                            f"bookSeq={dc.get('book_sequence_verified', 0)} "
                            f"trade10={dc.get('trade_count_ge_10', 0)} "
                            f"ofi6={dc.get('ofi_samples_ge_6', 0)} "
                            f"book8={dc.get('book_updates_ge_8', 0)} "
                            f"ready={dc.get('micro_ready', 0)} "
                            f"verified={dc.get('micro_verified', 0)} "
                            f"topFail={top_fail.get('failure', 'NONE')}:{top_fail.get('count', 0)}",
                            flush=True,
                        )
                    except Exception as exc:
                        core._redis_bridge_stats["micro_diag_error"] = (
                            f"{type(exc).__name__}: {exc}"
                        )

                core._redis_bridge_stats["structure_workers_up"] = sum(
                    _heartbeat_ready(f"structure{idx}", "heartbeat_ms", "assigned")
                    for idx in range(STRUCTURE_WORKERS)
                )
                core._redis_bridge_stats["risk_workers_up"] = sum(
                    _heartbeat_ready(f"risk{idx}", "heartbeat_ms")
                    for idx in range(RISK_WORKERS)
                )
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            core._redis_bridge_stats["hardening_supervisor_errors"] += 1
            core._redis_bridge_stats["hardening_supervisor_last_error"] = (
                f"{type(exc).__name__}: {exc}"
            )
            await asyncio.sleep(1.5)
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


def install(core):
    global CORE, _original_gate, _original_scan, _original_health
    if CORE is not None:
        return
    CORE = core
    _original_gate = core._strict_execution_gate
    _original_scan = core.v12_scan
    _original_health = core.v12_health

    core._distributed_micro_symbols = stable_micro_symbols
    core._strict_execution_gate = _gate_wrapper
    core.EXECUTION_AUTHORITY_CHAIN = AUTHORITY_CHAIN
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        f"Ψ-V12.3.4 HARDENING installed — {HARDENING_REVISION} + low-cap early explosion promotion + guaranteed RAPID challenger access + guarded workers + live gate diagnostics + fail-closed authority",
        flush=True,
    )
