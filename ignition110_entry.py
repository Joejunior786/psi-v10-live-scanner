import asyncio
import math
import time
from collections import defaultdict

import orderbook_patch

# Patch the Binance depth reconciler before importing the active scanner.
orderbook_patch.install_depth_sequence_patch()

import ignition110_app as scanner

# Add explicit diagnostics so 'book data missing' is never confused with
# 'live book present but bullish pressure not confirmed'.
orderbook_patch.install_diagnostics(scanner)


VERSION = "10.12-capacity-lock-entry"
WARMUP_SECONDS = 90
READY_STREAK_REQUIRED = 3
CANDIDATE_LOCK_SECONDS = 1200
MIN_POOL_HOLD_SECONDS = 900
MICRO_POOL_SLOTS = 80
PRIORITY_LOCK_SLOTS = 24
ROTATE_PER_CYCLE = 12
PROTECT_SCORE = 68.0
PROTECT_BIG_MOVE_SCORE = 55.0
PROTECT_STATES = {"WATCH", "EARLY OPPORTUNITY", "PRE-IGNITION", "BUY NOW"}
ENTRY_MIN_BUFFER_BPS = 5.0
ENTRY_MAX_BUFFER_BPS = 20.0

warm_started = {}
ready_streak = defaultdict(int)
telemetry_meta = {}
candidate_persistence = defaultdict(int)
candidate_last_seen = {}


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
    return (
        state in PROTECT_STATES
        or score >= PROTECT_SCORE
        or big >= PROTECT_BIG_MOVE_SCORE
        or _layer_count(row) >= 4
    )


def _state_rank(state):
    return {
        "BUY NOW": 6,
        "PRE-IGNITION": 5,
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
    trigger = None
    if resistance > 0 and spread_bps is not None and spread_bps >= 0:
        buffer_bps = max(
            ENTRY_MIN_BUFFER_BPS,
            min(ENTRY_MAX_BUFFER_BPS, spread_bps * 1.5),
        )
        trigger = resistance * (1.0 + buffer_bps / 10000.0)

    return {
        "price": price if price > 0 else None,
        "resistance": resistance if resistance > 0 else None,
        "breakout_distance_pct": distance_pct,
        "distance_to_resistance_bps": None if distance_pct is None else round(distance_pct * 100.0, 2),
        "entry_buffer_bps": None if buffer_bps is None else round(buffer_bps, 2),
        "breakout_entry_trigger": trigger,
        "entry_trigger_verified": trigger is not None,
    }


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
        pre_warmup_state = str(row.get("state") or "REJECT")

        row["pre_warmup_state"] = pre_warmup_state
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
            if pre_warmup_state in ("BUY NOW", "PRE-IGNITION"):
                row["state"] = "WATCH"
            row["telemetry_status"] = "COLLECTING DATA"
        else:
            row["telemetry_status"] = "READY"

        entry = _entry_telemetry(app, q, symbol, row)
        row.update(entry)

        telemetry_meta[symbol] = {
            "raw_ready": raw_micro_ready,
            "streak": int(ready_streak[symbol]),
            "qualified_ready": qualified_ready,
        }
        return row

    def tick_with_candidate_lock():
        # IMPORTANT: install this on s.tick, q.tick and v7.tick. The live hunt loop
        # resolves v7.tick dynamically, which is why the previous wrapper never
        # affected the active runtime and locked stayed at zero.
        original_tick()
        now_t = time.time()
        selected = set(app.selected_micro_symbols)
        candidates = []

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
                candidates.append((
                    _state_rank(row.get("pre_warmup_state") or row.get("state")),
                    min(candidate_persistence[symbol], 99),
                    float(row.get("big_move_potential_score") or 0.0),
                    float(row.get("score") or 0.0),
                    symbol,
                ))
            else:
                candidate_persistence[symbol] = 0

        candidates.sort(reverse=True)

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

    def near_diag_with_collection_state(limit=10):
        # Pull a wider source set, then persistence-rank it down to the requested
        # limit so a one-cycle score spike cannot automatically outrank a setup
        # that has remained strong across consecutive evaluations.
        rows = original_near_diag(max(limit, 30))
        now_t = time.time()
        out = []
        for item in rows:
            d = dict(item)
            symbol = d.get("symbol")
            source = q.latest.get(symbol) or {}
            meta = telemetry_meta.get(symbol) or {}

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

            hard_failures = list(d.get("failed_hard", []) or [])
            layer_failures = list(d.get("failed_layers", d.get("failed_setup", [])) or [])
            extra_blockers = list(d.get("blockers", []) or [])

            d["failed_execution_gates"] = hard_failures
            d["execution_gate_status"] = "PASS_ALL" if not hard_failures else "BLOCKED"
            d["missing_signal_layers"] = layer_failures
            d["candidate_persistence_samples"] = int(candidate_persistence.get(symbol, 0))
            d["pre_warmup_state"] = source.get("pre_warmup_state", source.get("state", d.get("state")))
            d["passed_major_layers"] = _layer_count(source)
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
                _state_rank(d.get("pre_warmup_state") or d.get("state")),
                bool((q.latest.get(d.get("symbol")) or {}).get("micro_ready")),
                min(int(d.get("candidate_persistence_samples") or 0), 6),
                int(d.get("passed_major_layers") or 0),
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
        "Ψ-V10.12 CAPACITY UPGRADE ACTIVE — 80 live-micro slots (24 priority + 56 hunter), "
        "90s warm-up + 3 ready samples, 20m candidate locks, 15m pool hold, "
        "persistence ranking, strict BUY unchanged, breakout-entry telemetry enabled",
        flush=True,
    )


_install_upgrade()


async def _candidate_telemetry_loop():
    while True:
        await asyncio.sleep(scanner.app.PRINT_SECONDS)
        q = scanner.q
        s = scanner.s
        app = scanner.app
        try:
            locks = q.locks()
            print(
                f"Ψ-V10.12 CAPACITY micro={len(app.selected_micro_symbols)}/{q.MICRO_SLOTS} "
                f"priority_locked={len(locks)}/{q.LOCK_SLOTS} "
                f"hold={q.MICRO_HOLD}s lock_grace={q.LOCK_GRACE}s",
                flush=True,
            )
            for i, d in enumerate(s.near_diag(10), 1):
                resistance = d.get("resistance")
                trigger = d.get("breakout_entry_trigger")
                distance = d.get("breakout_distance_pct")
                rtxt = "-" if resistance is None else f"{float(resistance):.10g}"
                ttxt = "WAIT_SPREAD" if trigger is None else f"{float(trigger):.10g}"
                dtxt = "-" if distance is None else f"{float(distance):+.3f}%"
                print(
                    f"E{i:02d}. {str(d.get('symbol')):12s} "
                    f"state={str(d.get('state')):18s} score={float(d.get('score') or 0):6.2f} "
                    f"persist={int(d.get('candidate_persistence_samples') or 0)} "
                    f"exec={d.get('execution_gate_status','BLOCKED')} "
                    f"layers_missing={d.get('missing_signal_layers',[])} "
                    f"res={rtxt} dist={dtxt} entry={ttxt}",
                    flush=True,
                )
        except Exception as exc:
            print(f"Ψ-V10.12 ENTRY_DIAG_ERROR {type(exc).__name__}: {exc}", flush=True)


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
            "Ψ-V10.12 ACTIVE — expanded verified coverage + working priority locks + "
            "persistence-ranked breakout telemetry; strict BUY gates unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.12 stopped", flush=True)
