import asyncio
import math
import time
from collections import defaultdict, deque

import orderbook_patch

# Patch the Binance depth reconciler before importing the active scanner.
orderbook_patch.install_depth_sequence_patch()

import ignition110_app as scanner

# Add explicit diagnostics so 'book data missing' is never confused with
# 'live book present but bullish pressure not confirmed'.
orderbook_patch.install_diagnostics(scanner)


VERSION = "10.13.0-ignition15-fingerprint"
WARMUP_SECONDS = 90
READY_STREAK_REQUIRED = 3
CANDIDATE_LOCK_SECONDS = 1200
MIN_POOL_HOLD_SECONDS = 900
MICRO_POOL_SLOTS = 80
PRIORITY_LOCK_SLOTS = 24
ROTATE_PER_CYCLE = 12
PROTECT_SCORE = 68.0
PROTECT_BIG_MOVE_SCORE = 55.0
PROTECT_STATES = {
    "WATCH",
    "EARLY OPPORTUNITY",
    "15% IGNITION WATCH",
    "PRE-IGNITION",
    "BUY NOW",
}
ENTRY_MIN_BUFFER_BPS = 5.0
ENTRY_MAX_BUFFER_BPS = 20.0
ENTRY_NEAR_BREAKOUT_PCT = 3.0

# V10.13 early 15% mover fingerprint.
IGNITION_WINDOW_SECONDS = 300
IGNITION_DISTANCE_MAX_PCT = 10.0
IGNITION_RAPID_HIGH = 150.0
IGNITION_RAPID_REPEAT = 130.0
IGNITION_RAPID_HIGH_COUNT = 2
IGNITION_RAPID_REPEAT_COUNT = 3
IGNITION_MIN_LAYERS = 4
IGNITION_MIN_STREAK = 2
IGNITION_SCORE_THRESHOLD = 68.0

warm_started = {}
ready_streak = defaultdict(int)
telemetry_meta = {}
candidate_persistence = defaultdict(int)
candidate_last_seen = {}
rapid_score_history = defaultdict(lambda: deque(maxlen=64))
ignition_streak = defaultdict(int)
ignition_meta = {}


def _append_once(items, value):
    if value not in items:
        items.append(value)


def _as_float(value, default=None):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _layer_count(row):
    layers = row.get("layer_results") or {}
    return sum(bool(v) for v in layers.values())


def _promising(row):
    if not row:
        return False
    state = str(row.get("pre_warmup_state") or row.get("state") or "REJECT")
    score = float(row.get("score") or 0.0)
    big = float(row.get("big_move_potential_score") or 0.0)
    failed = set(row.get("failed_hard", []) or [])

    # Do not waste a protected slot on an already-chasing/late runner unless it
    # has independently reached a genuine promoted state.
    if "ANTI_CHASE_OR_RUNNER" in failed and state not in PROTECT_STATES:
        return False

    return (
        bool(row.get("ignition15_watch"))
        or state in PROTECT_STATES
        or score >= PROTECT_SCORE
        or big >= PROTECT_BIG_MOVE_SCORE
        or _layer_count(row) >= 4
    )


def _state_rank(state):
    return {
        "BUY NOW": 6,
        "PRE-IGNITION": 5,
        "15% IGNITION WATCH": 4.5,
        "WATCH": 4,
        "EARLY OPPORTUNITY": 3,
        "COLLECTING DATA": 2,
        "REJECT": 1,
    }.get(str(state or "REJECT"), 0)


def _entry_telemetry(app, q, symbol, row=None):
    source = row or q.latest.get(symbol) or {}
    sd = app.structure.get(symbol) or {}

    price = _as_float(source.get("price"), _as_float(sd.get("price"), 0.0)) or 0.0
    resistance = _as_float(sd.get("resistance"), 0.0) or 0.0
    distance_pct = _as_float(
        source.get("breakout_distance_pct"),
        _as_float(sd.get("breakout_distance_pct"), None),
    )
    spread_bps = _as_float(source.get("spread_bps"), None)

    buffer_bps = None
    reference_trigger = None
    entry_trigger = None
    entry_status = "NO_RESISTANCE"
    entry_armed = False

    if resistance > 0:
        entry_status = "WAIT_SPREAD"
        if spread_bps is not None and spread_bps >= 0:
            buffer_bps = max(
                ENTRY_MIN_BUFFER_BPS,
                min(ENTRY_MAX_BUFFER_BPS, spread_bps * 1.5),
            )
            reference_trigger = resistance * (1.0 + buffer_bps / 10000.0)

            if distance_pct is None:
                entry_status = "WAIT_BREAKOUT_DISTANCE"
            elif price > 0 and price >= reference_trigger:
                # Never print a buy-stop behind the current market. Once the
                # buffered breakout has already traded, require retest/reclaim
                # or a new base instead of encouraging a chase.
                entry_status = "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
            elif distance_pct > ENTRY_NEAR_BREAKOUT_PCT:
                entry_status = "WAIT_APPROACH"
            elif distance_pct >= 0:
                entry_trigger = reference_trigger
                entry_status = "BREAKOUT_TRIGGER_ARMED"
                entry_armed = True
            else:
                # Resistance has been marginally crossed but the buffered
                # confirmation trigger has not. Keep the future confirmation
                # price valid, while clearly labelling the state.
                entry_trigger = reference_trigger
                entry_status = "CONFIRM_BREAKOUT_BUFFER"
                entry_armed = True

    strict_now = str(source.get("pre_warmup_state") or source.get("state") or "") == "BUY NOW"
    exec_all = all(
        (source.get("hard_safety_status") or {}).get(k) == "PASS"
        for k in (
            "LIVE_MICRO_DATA",
            "TRADE_SEQUENCE_VALID",
            "BOOK_SEQUENCE_VALID",
            "SPREAD_FILTER",
            "SLIPPAGE_FILTER",
            "CUMULATIVE_EXTENSION_GUARD",
        )
    )

    return {
        "price": price if price > 0 else None,
        "resistance": resistance if resistance > 0 else None,
        "breakout_distance_pct": distance_pct,
        "distance_to_resistance_bps": None if distance_pct is None else round(distance_pct * 100.0, 2),
        "entry_buffer_bps": None if buffer_bps is None else round(buffer_bps, 2),
        "breakout_trigger_reference": reference_trigger,
        "breakout_entry_trigger": entry_trigger,
        "entry_trigger_verified": reference_trigger is not None,
        "entry_trigger_armed": entry_armed,
        "entry_status": entry_status,
        "entry_actionable_now": bool(strict_now and exec_all and entry_armed),
    }


def _breakout_rank(row):
    status = str(row.get("entry_status") or "")
    distance = _as_float(row.get("breakout_distance_pct"), None)
    if status in ("BREAKOUT_TRIGGER_ARMED", "CONFIRM_BREAKOUT_BUFFER"):
        return 5
    if status == "WAIT_APPROACH" and distance is not None:
        if distance <= 5.0:
            return 4
        if distance <= 8.0:
            return 3
        return 2
    if status == "WAIT_SPREAD":
        return 1
    if status == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER":
        return 0
    return 1


def _rapid_metric(symbol, row):
    rapid = row.get("rapid_ignition")
    if isinstance(rapid, dict) and rapid:
        return rapid
    try:
        return scanner.v93.metric(symbol) or {}
    except Exception:
        return {}


def _record_rapid_sample(symbol, rapid, now_t):
    score = _as_float(rapid.get("score"), 0.0) or 0.0
    hist = rapid_score_history[symbol]
    hist.append((now_t, score))
    cutoff = now_t - IGNITION_WINDOW_SECONDS
    while hist and hist[0][0] < cutoff:
        hist.popleft()
    return hist


def _ignition15_fingerprint(symbol, row, qualified_ready):
    """Detect the pre-expansion fingerprint seen in today's >15% Binance movers.

    This is intentionally an early WATCH state, not PRE-IGNITION or BUY NOW.
    It can tolerate a lagging MA_STRUCTURE layer, but never bypasses live micro,
    sequence, liquidity, extension or anti-chase safety.
    """
    now_t = time.time()
    rapid = _rapid_metric(symbol, row)
    hist = _record_rapid_sample(symbol, rapid, now_t)

    high_count = sum(score >= IGNITION_RAPID_HIGH for _, score in hist)
    repeat_count = sum(score >= IGNITION_RAPID_REPEAT for _, score in hist)
    rapid_score = _as_float(rapid.get("score"), 0.0) or 0.0
    burst5 = int(rapid.get("burst_count_5m") or 0)
    burst15 = int(rapid.get("burst_count_15m") or 0)
    clustered = bool(rapid.get("clustered_ignition"))
    latent = bool(rapid.get("latent_ignition"))

    repeated_rapid = bool(
        high_count >= IGNITION_RAPID_HIGH_COUNT
        or repeat_count >= IGNITION_RAPID_REPEAT_COUNT
        or (rapid_score >= IGNITION_RAPID_REPEAT and burst5 >= 2)
        or (clustered and rapid_score >= 110.0)
        or burst5 >= 4
    )

    layers = row.get("layer_results") or {}
    layer_count = sum(bool(v) for v in layers.values())
    activity = bool(layers.get("ACTIVITY_LAYER"))
    flow = bool(layers.get("FLOW_LAYER"))
    book = bool(layers.get("ORDER_BOOK_LAYER"))
    vwap = bool(layers.get("VWAP_LAYER"))
    ma = bool(layers.get("MA_STRUCTURE_LAYER"))
    anti_chase_layer = bool(layers.get("ANTI_CHASE_OR_RUNNER_LAYER"))

    hard = row.get("hard_safety_status") or {}
    exec_keys = (
        "TRADE_SEQUENCE_VALID",
        "BOOK_SEQUENCE_VALID",
        "SPREAD_FILTER",
        "SLIPPAGE_FILTER",
        "CUMULATIVE_EXTENSION_GUARD",
    )
    execution_safe = qualified_ready and all(hard.get(k) == "PASS" for k in exec_keys)
    market_safe = hard.get("MARKET_REGIME_SAFETY", "PASS") == "PASS"
    failed_hard = set(row.get("failed_hard", []) or [])
    chase_safe = (
        "ANTI_CHASE_OR_RUNNER" not in failed_hard
        and row.get("state") != "LATE RUNNER"
        and anti_chase_layer
    )

    entry = _entry_telemetry(scanner.app, scanner.q, symbol, row)
    distance = _as_float(entry.get("breakout_distance_pct"), None)
    distance_ok = distance is not None and 0.0 <= distance <= IGNITION_DISTANCE_MAX_PCT
    has_resistance = entry.get("resistance") is not None

    vol5 = _as_float(rapid.get("vol_accel_5"), 0.0) or 0.0
    vol15 = _as_float(rapid.get("vol_accel_15"), 0.0) or 0.0
    trade5 = _as_float(rapid.get("trade_accel_5"), 0.0) or 0.0
    activity_accel = bool(
        activity
        and (
            vol5 >= 1.4
            or vol15 >= 1.3
            or trade5 >= 1.4
            or latent
            or clustered
            or repeated_rapid
        )
    )

    pressure_ok = bool(flow or book)
    structure_support = bool(vwap or ma)
    base_conditions = bool(
        repeated_rapid
        and execution_safe
        and market_safe
        and chase_safe
        and has_resistance
        and distance_ok
        and layer_count >= IGNITION_MIN_LAYERS
        and activity_accel
        and pressure_ok
        and structure_support
    )

    if base_conditions:
        ignition_streak[symbol] += 1
    else:
        ignition_streak[symbol] = 0

    reasons = []
    if repeated_rapid:
        reasons.append("REPEATED_RAPID")
    if high_count >= IGNITION_RAPID_HIGH_COUNT:
        reasons.append("RAPID_150_X2")
    elif repeat_count >= IGNITION_RAPID_REPEAT_COUNT:
        reasons.append("RAPID_130_X3")
    if burst5 >= 4:
        reasons.append("BURST_CLUSTER_5M")
    if activity_accel:
        reasons.append("ACTIVITY_ACCEL")
    if flow:
        reasons.append("FLOW")
    if book:
        reasons.append("BOOK")
    if vwap:
        reasons.append("VWAP")
    if ma:
        reasons.append("MA")
    elif base_conditions:
        reasons.append("MA_ALLOWED_TO_LAG")
    if distance_ok:
        reasons.append("PRE_BREAKOUT_0_10PCT")
    if execution_safe:
        reasons.append("EXECUTION_SAFE")

    score = 0.0
    score += min(25.0, high_count * 12.5 + repeat_count * 3.0)
    score += min(15.0, burst5 * 2.5 + burst15 * 0.35)
    score += 12.0 if activity_accel else 0.0
    score += 10.0 if flow else 0.0
    score += 10.0 if book else 0.0
    score += 7.0 if vwap else 0.0
    score += 5.0 if ma else 0.0
    score += 8.0 if distance_ok and distance <= 5.0 else 4.0 if distance_ok else 0.0
    score += 8.0 if execution_safe else 0.0
    score = min(100.0, score)

    watch = bool(
        base_conditions
        and ignition_streak[symbol] >= IGNITION_MIN_STREAK
        and score >= IGNITION_SCORE_THRESHOLD
    )

    meta = {
        "watch": watch,
        "score": round(score, 2),
        "streak": int(ignition_streak[symbol]),
        "rapid_score": round(rapid_score, 2),
        "rapid_high_count_5m": int(high_count),
        "rapid_repeat_count_5m": int(repeat_count),
        "burst_count_5m": burst5,
        "burst_count_15m": burst15,
        "layer_count": int(layer_count),
        "distance_pct": distance,
        "reasons": reasons,
        "ma_allowed_to_lag": bool(base_conditions and not ma),
        "execution_safe": execution_safe,
    }
    ignition_meta[symbol] = meta
    return meta


def _install_upgrade():
    q = scanner.q
    s = scanner.s
    app = scanner.app

    # Capacity upgrade: 24 priority/protected names + 56 rotating hunter names.
    # BUY/PRE rules are unchanged; this only gives more candidates enough live data
    # to qualify under those same strict rules.
    q.MICRO_SLOTS = max(int(q.MICRO_SLOTS), MICRO_POOL_SLOTS)
    q.LOCK_SLOTS = max(int(q.LOCK_SLOTS), PRIORITY_LOCK_SLOTS)
    q.MICRO_HOLD = max(int(q.MICRO_HOLD), MIN_POOL_HOLD_SECONDS)
    q.LOCK_GRACE = max(int(q.LOCK_GRACE), CANDIDATE_LOCK_SECONDS)
    app.MICRO_UNIVERSE_SIZE = q.MICRO_SLOTS
    app.ANOMALY_PROMOTION_SLOTS = q.MICRO_SLOTS
    app.MICRO_POOL_MIN_HOLD_SECONDS = q.MICRO_HOLD
    if hasattr(s, "ROTATE_PER_CYCLE"):
        s.ROTATE_PER_CYCLE = max(int(s.ROTATE_PER_CYCLE), ROTATE_PER_CYCLE)

    original_evaluate = app.evaluate_symbol
    original_tick = s.tick
    original_near_diag = s.near_diag

    def evaluate_with_warmup(symbol):
        row = original_evaluate(symbol)
        if not row:
            return row

        now_t = time.time()
        start = warm_started.get(symbol)
        if start is None:
            start = float(q.entered.get(symbol, now_t) or now_t)
            warm_started[symbol] = start

        age = max(0.0, now_t - start)
        raw_micro_ready = bool(row.get("micro_ready"))
        if raw_micro_ready:
            ready_streak[symbol] += 1
        else:
            ready_streak[symbol] = 0

        warmup_complete = age >= WARMUP_SECONDS
        persistence_ready = ready_streak[symbol] >= READY_STREAK_REQUIRED
        qualified_ready = raw_micro_ready and warmup_complete and persistence_ready
        original_state = str(row.get("state") or "REJECT")

        row["pre_warmup_state"] = original_state
        row["raw_micro_ready"] = raw_micro_ready
        row["micro_warmup_age_seconds"] = round(age, 1)
        row["micro_warmup_seconds"] = WARMUP_SECONDS
        row["micro_ready_streak"] = int(ready_streak[symbol])
        row["micro_ready_streak_required"] = READY_STREAK_REQUIRED
        row["micro_warmup_complete"] = warmup_complete
        row["micro_collection_ready"] = qualified_ready

        if not qualified_ready:
            row["micro_ready"] = False
            hard = row.setdefault("hard_safety_status", {})
            hard["LIVE_MICRO_DATA"] = "FAIL"
            failed_hard = row.setdefault("failed_hard", [])
            _append_once(failed_hard, "LIVE_MICRO_DATA")
            if original_state in ("BUY NOW", "PRE-IGNITION"):
                row["state"] = "WATCH"
            row["telemetry_status"] = "COLLECTING DATA"
        else:
            row["telemetry_status"] = "READY"

        row.update(_entry_telemetry(app, q, symbol, row))

        # New V10.13 post-mortem-derived early mover detector. It NEVER upgrades
        # a coin to formal PRE-IGNITION or BUY NOW; it only creates an earlier,
        # separately named watch state while the strict original state remains
        # available in formal_state.
        fingerprint = _ignition15_fingerprint(symbol, row, qualified_ready)
        row["formal_state"] = original_state
        row["ignition15_watch"] = bool(fingerprint["watch"])
        row["ignition15_score"] = fingerprint["score"]
        row["ignition15_streak"] = fingerprint["streak"]
        row["ignition15_reasons"] = fingerprint["reasons"]
        row["ignition15_rapid_high_count_5m"] = fingerprint["rapid_high_count_5m"]
        row["ignition15_rapid_repeat_count_5m"] = fingerprint["rapid_repeat_count_5m"]

        if (
            fingerprint["watch"]
            and original_state not in ("BUY NOW", "PRE-IGNITION", "LATE RUNNER")
            and qualified_ready
        ):
            row["state"] = "15% IGNITION WATCH"
            row["pre_warmup_state"] = "15% IGNITION WATCH"

        telemetry_meta[symbol] = {
            "raw_ready": raw_micro_ready,
            "streak": int(ready_streak[symbol]),
            "qualified_ready": qualified_ready,
        }
        return row

    def tick_with_candidate_lock():
        # IMPORTANT: install this on s.tick, q.tick and v7.tick. The live hunt loop
        # resolves v7.tick dynamically, which is why the old V10.11 wrapper never
        # affected the active runtime and locked stayed at zero.
        original_tick()
        now_t = time.time()
        selected = set(app.selected_micro_symbols)

        for symbol in selected:
            row = q.latest.get(symbol)
            if not row or symbol not in q.universe_set:
                continue

            if _promising(row):
                candidate_persistence[symbol] += 1
                candidate_last_seen[symbol] = now_t
                q.locked_until[symbol] = max(
                    float(q.locked_until.get(symbol, 0.0) or 0.0),
                    now_t + CANDIDATE_LOCK_SECONDS,
                )
            else:
                candidate_persistence[symbol] = 0

        for symbol in list(candidate_last_seen):
            if now_t - candidate_last_seen[symbol] > CANDIDATE_LOCK_SECONDS:
                candidate_last_seen.pop(symbol, None)
                candidate_persistence.pop(symbol, None)

        for symbol in list(warm_started):
            active_lock = float(q.locked_until.get(symbol, 0.0) or 0.0) >= now_t
            if symbol not in selected and not active_lock:
                if now_t - warm_started[symbol] > CANDIDATE_LOCK_SECONDS:
                    warm_started.pop(symbol, None)
                    ready_streak.pop(symbol, None)
                    telemetry_meta.pop(symbol, None)
                    rapid_score_history.pop(symbol, None)
                    ignition_streak.pop(symbol, None)
                    ignition_meta.pop(symbol, None)

    def near_diag_with_collection_state(limit=10):
        # Pull a wider source set, then rank by promoted state, verified micro,
        # layer completion, breakout proximity, persistence and score. This makes
        # 'closest to BUY/breakout' different from merely 'highest raw score'.
        rows = original_near_diag(max(limit, 40))
        now_t = time.time()
        out = []
        for item in rows:
            d = dict(item)
            symbol = d.get("symbol")
            source = q.latest.get(symbol) or {}
            meta = telemetry_meta.get(symbol) or {}
            fp = ignition_meta.get(symbol) or {}

            if symbol in app.selected_micro_symbols and not meta.get("qualified_ready", False):
                start = warm_started.get(symbol, float(q.entered.get(symbol, now_t) or now_t))
                age = max(0.0, now_t - start)
                remaining = max(0, int(round(WARMUP_SECONDS - age)))
                d["state"] = "COLLECTING DATA"
                d["telemetry_status"] = "COLLECTING DATA"
                d["warmup_seconds_remaining"] = remaining
                d["raw_micro_ready"] = bool(meta.get("raw_ready", False))
                d["micro_ready_streak"] = int(meta.get("streak", 0))
                d["micro_ready_streak_required"] = READY_STREAK_REQUIRED
                blockers = list(d.get("blockers", []) or [])
                _append_once(blockers, "MICRO_WARMUP")
                if remaining:
                    _append_once(blockers, f"WARMUP_{remaining}s")
                elif not meta.get("raw_ready", False):
                    _append_once(blockers, "WAITING_FRESH_MICRO")
                elif int(meta.get("streak", 0)) < READY_STREAK_REQUIRED:
                    _append_once(blockers, "WAITING_READY_STREAK")
                d["blockers"] = blockers
            elif source.get("ignition15_watch"):
                d["state"] = "15% IGNITION WATCH"

            hard_failures = list(d.get("failed_hard", []) or [])
            layer_failures = list(d.get("failed_layers", d.get("failed_setup", [])) or [])
            extra_blockers = list(d.get("blockers", []) or [])

            d["failed_execution_gates"] = hard_failures
            d["execution_gate_status"] = "PASS_ALL" if not hard_failures else "BLOCKED"
            d["missing_signal_layers"] = layer_failures
            d["candidate_persistence_samples"] = int(candidate_persistence.get(symbol, 0))
            d["pre_warmup_state"] = source.get("pre_warmup_state", source.get("state", d.get("state")))
            d["formal_state"] = source.get("formal_state", source.get("state", d.get("state")))
            d["passed_major_layers"] = _layer_count(source)
            d["ignition15_watch"] = bool(source.get("ignition15_watch"))
            d["ignition15_score"] = float(source.get("ignition15_score") or fp.get("score") or 0.0)
            d["ignition15_streak"] = int(source.get("ignition15_streak") or fp.get("streak") or 0)
            d["ignition15_reasons"] = source.get("ignition15_reasons") or fp.get("reasons") or []
            d.update(_entry_telemetry(app, q, symbol, source))

            combined = []
            for blocker in hard_failures + layer_failures + extra_blockers:
                _append_once(combined, blocker)
            d["combined_blockers"] = combined

            if not hard_failures:
                d["failed_hard"] = "PASS_ALL"

            out.append(d)

        out.sort(
            key=lambda d: (
                _state_rank(d.get("state") or d.get("pre_warmup_state")),
                bool((q.latest.get(d.get("symbol")) or {}).get("micro_ready")),
                bool(d.get("ignition15_watch")),
                float(d.get("ignition15_score") or 0.0),
                int(d.get("passed_major_layers") or 0),
                _breakout_rank(d),
                min(int(d.get("candidate_persistence_samples") or 0), 6),
                float(d.get("score") or 0.0),
            ),
            reverse=True,
        )
        return out[:limit]

    app.evaluate_symbol = evaluate_with_warmup
    s.tick = tick_with_candidate_lock
    q.tick = tick_with_candidate_lock
    scanner.v7.tick = tick_with_candidate_lock
    s.near_diag = near_diag_with_collection_state
    scanner.VERSION = VERSION

    print(
        "Ψ-V10.13 IGNITION15 UPGRADE ACTIVE — 80 live-micro slots (24 priority + 56 hunter), "
        "90s warm-up + 3 ready samples, repeated-RAPID 15% mover fingerprint, "
        "0-10% pre-breakout watch zone, MA may lag ONLY for IGNITION WATCH, "
        "formal PRE/BUY gates unchanged, chase-safe entry telemetry",
        flush=True,
    )


_install_upgrade()


def _ignition_watch_rows(limit=10):
    rows = []
    for symbol, row in list(scanner.q.latest.items()):
        if not row or not row.get("ignition15_watch"):
            continue
        fp = ignition_meta.get(symbol) or {}
        entry = _entry_telemetry(scanner.app, scanner.q, symbol, row)
        rows.append(
            {
                "symbol": symbol,
                "state": row.get("state"),
                "formal_state": row.get("formal_state"),
                "score": float(row.get("ignition15_score") or fp.get("score") or 0.0),
                "streak": int(row.get("ignition15_streak") or fp.get("streak") or 0),
                "rapid_score": float(fp.get("rapid_score") or 0.0),
                "rapid_high_count_5m": int(fp.get("rapid_high_count_5m") or 0),
                "rapid_repeat_count_5m": int(fp.get("rapid_repeat_count_5m") or 0),
                "reasons": fp.get("reasons") or [],
                **entry,
            }
        )
    rows.sort(
        key=lambda x: (
            float(x.get("score") or 0.0),
            int(x.get("streak") or 0),
            -abs(float(x.get("breakout_distance_pct") or 999.0)),
        ),
        reverse=True,
    )
    return rows[:limit]


async def _candidate_telemetry_loop():
    while True:
        await asyncio.sleep(scanner.app.PRINT_SECONDS)
        q = scanner.q
        s = scanner.s
        app = scanner.app
        try:
            locks = q.locks()
            print(
                f"Ψ-V10.13 CAPACITY micro={len(app.selected_micro_symbols)}/{q.MICRO_SLOTS} "
                f"priority_locked={len(locks)}/{q.LOCK_SLOTS} "
                f"hold={q.MICRO_HOLD}s lock_grace={q.LOCK_GRACE}s",
                flush=True,
            )

            watches = _ignition_watch_rows(10)
            if watches:
                print(f"Ψ-V10.13 IGNITION15 WATCHES — {len(watches)}", flush=True)
                for i, d in enumerate(watches, 1):
                    distance = d.get("breakout_distance_pct")
                    dtxt = "-" if distance is None else f"{float(distance):+.3f}%"
                    print(
                        f"I{i:02d}. {str(d.get('symbol')):12s} "
                        f"fp={float(d.get('score') or 0):5.1f} "
                        f"streak={int(d.get('streak') or 0)} "
                        f"rapid={float(d.get('rapid_score') or 0):6.1f} "
                        f"r150x={int(d.get('rapid_high_count_5m') or 0)} "
                        f"r130x={int(d.get('rapid_repeat_count_5m') or 0)} "
                        f"dist={dtxt} formal={d.get('formal_state')} "
                        f"reasons={d.get('reasons', [])}",
                        flush=True,
                    )
            else:
                print("Ψ-V10.13 IGNITION15 WATCHES — 0", flush=True)

            for i, d in enumerate(s.near_diag(10), 1):
                resistance = d.get("resistance")
                trigger = d.get("breakout_entry_trigger")
                distance = d.get("breakout_distance_pct")
                status = str(d.get("entry_status") or "-")
                rtxt = "-" if resistance is None else f"{float(resistance):.10g}"
                ttxt = status if trigger is None else f"{float(trigger):.10g}"
                dtxt = "-" if distance is None else f"{float(distance):+.3f}%"
                print(
                    f"E{i:02d}. {str(d.get('symbol')):12s} "
                    f"state={str(d.get('state')):18s} score={float(d.get('score') or 0):6.2f} "
                    f"ign15={float(d.get('ignition15_score') or 0):5.1f} "
                    f"persist={int(d.get('candidate_persistence_samples') or 0)} "
                    f"exec={d.get('execution_gate_status','BLOCKED')} "
                    f"layers_missing={d.get('missing_signal_layers',[])} "
                    f"res={rtxt} dist={dtxt} entry={ttxt} entry_status={status}",
                    flush=True,
                )
        except Exception as exc:
            print(f"Ψ-V10.13 ENTRY_DIAG_ERROR {type(exc).__name__}: {exc}", flush=True)


async def _combined_print_loop():
    await asyncio.gather(
        scanner.print_loop(),
        orderbook_patch.book_diagnostic_loop(scanner),
        _candidate_telemetry_loop(),
    )


scanner.v7.print_loop = _combined_print_loop
scanner.q.print_loop = _combined_print_loop
scanner.s.print_loop = _combined_print_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.13 ACTIVE — post-mortem-derived 15% mover ignition fingerprint + "
            "expanded verified coverage + working priority locks + chase-safe breakout telemetry; "
            "strict PRE/BUY gates unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.13 stopped", flush=True)
