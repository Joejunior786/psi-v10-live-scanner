import asyncio
import math
import time
from collections import defaultdict

import ignition114_entry as base14

scanner = base14.scanner
q = scanner.q
s = scanner.s
app = scanner.app

VERSION = "10.15.0-fair-rotation-full-discovery"

# V10.15 fixes candidate recycling without weakening signal gates.
MICRO_POOL_SLOTS = 80
ADAPTIVE_PRIORITY_SLOTS = 16
POOL_MIN_HOLD_SECONDS = 300
POOL_REBALANCE_SECONDS = 30
STRUCTURE_BATCH_SIZE = 60
STRUCTURE_REFRESH_SECONDS = 60
STRUCTURE_COVERAGE_QUOTA = 50
STRUCTURE_PRIORITY_QUOTA = 10
NOVELTY_QUOTA = 24
EARLY_NO_IMPROVE_EVICT_SECONDS = 300
EARLY_MAX_RESIDENCE_SECONDS = 900
IMPROVEMENT_LOCK_SECONDS = 600
PROMOTED_LOCK_SECONDS = 1200
RECYCLE_COOLDOWN_SECONDS = 180
FRESH_BONUS_SECONDS = 300

candidate_first_seen = {}
candidate_last_improve = {}
candidate_signature = {}
evicted_until = {}
rotation_stats = {
    "stale_evictions": 0,
    "newcomers_last": 0,
    "dropped_last": 0,
    "rebalances": 0,
    "full_structure_rounds": 0,
}
structure_round_attempted = set()
structure_attempted_ever = set()


def _f(value, default=0.0):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _state_rank(state):
    return {
        "BUY NOW": 7.0,
        "PRE-IGNITION": 6.0,
        "15% IGNITION WATCH": 5.0,
        "EARLY OPPORTUNITY": 4.0,
        "WATCH": 3.0,
        "COLLECTING DATA": 2.0,
        "REJECT": 1.0,
    }.get(str(state or "REJECT"), 0.0)


def _layer_count(row):
    return sum(bool(v) for v in (row.get("layer_results") or {}).values())


def _entry_rank(row):
    status = str(row.get("entry_status") or "")
    if status in ("BREAKOUT_TRIGGER_ARMED", "CONFIRM_BREAKOUT_BUFFER"):
        return 3
    if status == "WAIT_APPROACH":
        return 2
    if status == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER":
        return 0
    return 1


def _signature(row):
    state = row.get("formal_state") or row.get("pre_warmup_state") or row.get("state")
    distance = _f(row.get("breakout_distance_pct"), 999.0)
    if distance < 0:
        distance = 999.0
    return {
        "state_rank": _state_rank(state),
        "layers": _layer_count(row),
        "v14": _f(row.get("v1014_score"), 0.0),
        "ign15": _f(row.get("ignition15_score"), 0.0),
        "distance": distance,
        "entry_rank": _entry_rank(row),
        "score": _f(row.get("score"), 0.0),
    }


def _is_improvement(previous, current):
    if not previous:
        return True
    return bool(
        current["state_rank"] > previous["state_rank"]
        or current["layers"] > previous["layers"]
        or current["v14"] >= previous["v14"] + 3.0
        or current["ign15"] >= previous["ign15"] + 5.0
        or current["entry_rank"] > previous["entry_rank"]
        or (
            current["distance"] < 999.0
            and previous["distance"] < 999.0
            and current["distance"] <= previous["distance"] - 0.50
        )
        or current["score"] >= previous["score"] + 4.0
    )


def _track_candidate(symbol, row, now_t):
    first = candidate_first_seen.setdefault(symbol, now_t)
    sig = _signature(row)
    prev = candidate_signature.get(symbol)
    if _is_improvement(prev, sig):
        candidate_last_improve[symbol] = now_t
    candidate_signature[symbol] = sig
    last_imp = candidate_last_improve.setdefault(symbol, now_t)
    return first, last_imp, sig


def _promoted(row):
    state = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    return state in ("BUY NOW", "PRE-IGNITION") or bool(row.get("ignition15_watch"))


def _stale_candidate(symbol, row, now_t):
    if _promoted(row):
        return False
    first, last_imp, sig = _track_candidate(symbol, row, now_t)
    state = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    if state not in ("EARLY OPPORTUNITY", "WATCH", "COLLECTING DATA", "REJECT"):
        return False
    no_improve = now_t - last_imp
    residence = now_t - first
    return bool(
        no_improve >= EARLY_NO_IMPROVE_EVICT_SECONDS
        or residence >= EARLY_MAX_RESIDENCE_SECONDS
    )


def _install_v1015():
    # Faster, fairer rotation: 16 adaptive priority slots + 64 hunter slots.
    q.MICRO_SLOTS = MICRO_POOL_SLOTS
    q.LOCK_SLOTS = ADAPTIVE_PRIORITY_SLOTS
    q.MICRO_HOLD = POOL_MIN_HOLD_SECONDS
    q.POOL_SECONDS = POOL_REBALANCE_SECONDS
    q.STRUCTURE_BATCH = STRUCTURE_BATCH_SIZE
    q.STRUCTURE_SECONDS = STRUCTURE_REFRESH_SECONDS

    app.MICRO_UNIVERSE_SIZE = MICRO_POOL_SLOTS
    app.ANOMALY_PROMOTION_SLOTS = MICRO_POOL_SLOTS
    app.MICRO_POOL_MIN_HOLD_SECONDS = POOL_MIN_HOLD_SECONDS
    app.TOP_STRUCTURE_UNIVERSE = STRUCTURE_BATCH_SIZE
    app.STRUCTURE_REFRESH_SECONDS = STRUCTURE_REFRESH_SECONDS

    original_tick = s.tick
    original_near_diag = s.near_diag

    def adaptive_locks():
        now_t = time.time()
        rows = []
        for symbol, until in list(q.locked_until.items()):
            if symbol not in q.universe_set or float(until or 0.0) < now_t:
                continue
            row = q.latest.get(symbol) or {}
            first, last_imp, sig = _track_candidate(symbol, row, now_t)
            if _stale_candidate(symbol, row, now_t):
                continue
            rows.append(
                (
                    1 if _promoted(row) else 0,
                    sig["state_rank"],
                    sig["layers"],
                    sig["v14"],
                    -(now_t - last_imp),
                    sig["score"],
                    symbol,
                )
            )
        rows.sort(reverse=True)
        return [x[-1] for x in rows[:ADAPTIVE_PRIORITY_SLOTS]]

    async def rebalance_pool_v1015(force=False):
        current = list(app.selected_micro_symbols)
        cset = set(current)
        now_t = time.time()
        out = []
        seen = set()

        def add(symbol):
            if (
                symbol in q.universe_set
                and symbol not in seen
                and len(out) < MICRO_POOL_SLOTS
                and float(evicted_until.get(symbol, 0.0) or 0.0) <= now_t
            ):
                out.append(symbol)
                seen.add(symbol)
                return True
            return False

        locked = adaptive_locks()
        for symbol in locked:
            add(symbol)

        # Retain only a limited number of young non-locked names. This preserves
        # warmup continuity without letting the old pool occupy all hunter slots.
        retained = 0
        if not force:
            for symbol in current:
                if symbol in locked:
                    continue
                age = now_t - float(q.entered.get(symbol, now_t) or now_t)
                row = q.latest.get(symbol) or {}
                if age < POOL_MIN_HOLD_SECONDS and not _stale_candidate(symbol, row, now_t):
                    if add(symbol):
                        retained += 1
                if retained >= 20:
                    break

        # Full-universe cheap discovery is already fed by !ticker@arr. Use it
        # explicitly every rebalance and reserve newcomer slots so fresh names
        # cannot be starved by persistent incumbents.
        hot_rows = q.hot(limit=max(160, min(len(q.universe), 240)))
        newcomers = 0
        for discovery_score, symbol in hot_rows:
            if symbol in cset or discovery_score <= 0:
                continue
            if add(symbol):
                newcomers += 1
            if newcomers >= NOVELTY_QUOTA:
                break

        # Structure-qualified ranking next. Apply a soft stale penalty so a name
        # that has stopped improving naturally loses hunter priority.
        ranked = []
        for symbol in list(app.structure):
            if symbol not in q.universe_set or not q.sfresh(symbol):
                continue
            base_score = _f(q.sscore(symbol), -1e9)
            row = q.latest.get(symbol) or {}
            last_imp = candidate_last_improve.get(symbol, now_t)
            no_imp = max(0.0, now_t - last_imp)
            penalty = min(30.0, max(0.0, no_imp - 180.0) / 30.0)
            ranked.append((base_score - penalty, symbol))
        ranked.sort(reverse=True)
        for _, symbol in ranked:
            add(symbol)

        # Fill remaining capacity from the all-universe discovery table.
        for _, symbol in hot_rows:
            add(symbol)

        # Last-resort fair universe fill prevents an underfilled micro pool.
        if len(out) < MICRO_POOL_SLOTS:
            for symbol in q.universe:
                add(symbol)
                if len(out) >= MICRO_POOL_SLOTS:
                    break

        if not out:
            return

        removed = [symbol for symbol in current if symbol not in seen]
        added = [symbol for symbol in out if symbol not in cset]

        for symbol in added:
            q.entered[symbol] = now_t
            candidate_first_seen[symbol] = now_t
            candidate_last_improve[symbol] = now_t
            app.ensure_micro_state(symbol)

        for symbol in removed:
            q.entered.pop(symbol, None)
            if symbol not in locked:
                evicted_until[symbol] = now_t + RECYCLE_COOLDOWN_SECONDS

        if set(out) != cset:
            app.selected_micro_symbols = out
            app.last_micro_pool_change = now_t
            q.pool_cycles += 1

        rotation_stats["newcomers_last"] = len(added)
        rotation_stats["dropped_last"] = len(removed)
        rotation_stats["rebalances"] += 1

        print(
            f"Ψ-V10.15 MICRO priority={len(locked)} retained={retained} "
            f"newcomers={len(added)} hunter={len(out)-len(locked)} total={len(out)}",
            flush=True,
        )

    def structure_batch_symbols_v1015():
        if not q.universe:
            return []

        # Complete a genuine universe attempt round. Unattempted symbols get
        # first claim on 50/60 structure slots; after a full round, the cycle resets.
        global structure_round_attempted
        if len(structure_round_attempted) >= len(q.universe):
            structure_round_attempted = set()
            rotation_stats["full_structure_rounds"] += 1

        out = []
        seen = set()

        def add(symbol):
            if symbol in q.universe_set and symbol not in seen and len(out) < STRUCTURE_BATCH_SIZE:
                out.append(symbol)
                seen.add(symbol)
                structure_round_attempted.add(symbol)
                structure_attempted_ever.add(symbol)
                return True
            return False

        # Fair coverage first: never-seen-in-this-round, oldest structure first.
        coverage = [symbol for symbol in q.universe if symbol not in structure_round_attempted]
        coverage.sort(key=lambda symbol: (q.structure_ms.get(symbol, 0),))
        for symbol in coverage[:STRUCTURE_COVERAGE_QUOTA]:
            add(symbol)

        # Remaining 10 slots service improving locked/hot candidates.
        locked_now = adaptive_locks()
        priority = list(locked_now)
        for _, symbol in q.hot(limit=80):
            if symbol not in priority:
                priority.append(symbol)
        locked_set = set(locked_now)
        priority.sort(
            key=lambda symbol: (
                1 if symbol in locked_set else 0,
                -q.structure_ms.get(symbol, 0),
                _f(q.dmetric(symbol).get("score"), 0.0),
            ),
            reverse=True,
        )
        for symbol in priority:
            if len(out) >= STRUCTURE_BATCH_SIZE:
                break
            add(symbol)

        # If priority had duplicates or invalids, top up with remaining fair coverage.
        if len(out) < STRUCTURE_BATCH_SIZE:
            for symbol in q.universe:
                if symbol not in seen:
                    add(symbol)
                if len(out) >= STRUCTURE_BATCH_SIZE:
                    break
        return out

    def tick_v1015():
        original_tick()
        now_t = time.time()
        selected = set(app.selected_micro_symbols)

        for symbol in list(selected):
            row = q.latest.get(symbol)
            if not row:
                continue
            first, last_imp, _ = _track_candidate(symbol, row, now_t)

            if _promoted(row):
                q.locked_until[symbol] = max(
                    float(q.locked_until.get(symbol, 0.0) or 0.0),
                    now_t + PROMOTED_LOCK_SECONDS,
                )
                continue

            if _stale_candidate(symbol, row, now_t):
                if float(q.locked_until.get(symbol, 0.0) or 0.0) >= now_t:
                    rotation_stats["stale_evictions"] += 1
                q.locked_until[symbol] = now_t - 1.0
                evicted_until[symbol] = max(
                    float(evicted_until.get(symbol, 0.0) or 0.0),
                    now_t + RECYCLE_COOLDOWN_SECONDS,
                )
            else:
                # Counteract V10.13's unconditional 20-minute promising renewal:
                # an EARLY/WATCH lock may only live relative to last improvement.
                cap = last_imp + IMPROVEMENT_LOCK_SECONDS
                current_until = float(q.locked_until.get(symbol, 0.0) or 0.0)
                if current_until > cap:
                    q.locked_until[symbol] = cap

        # Garbage-collect old tracking state.
        for symbol in list(candidate_first_seen):
            if symbol in selected:
                continue
            until = float(evicted_until.get(symbol, 0.0) or 0.0)
            if until < now_t and now_t - candidate_first_seen[symbol] > PROMOTED_LOCK_SECONDS:
                candidate_first_seen.pop(symbol, None)
                candidate_last_improve.pop(symbol, None)
                candidate_signature.pop(symbol, None)
                evicted_until.pop(symbol, None)

    def near_diag_v1015(limit=10):
        rows = original_near_diag(max(limit, 60))
        now_t = time.time()
        out = []
        for item in rows:
            row = dict(item)
            symbol = row.get("symbol")
            source = q.latest.get(symbol) or {}
            first = candidate_first_seen.get(symbol, now_t)
            last_imp = candidate_last_improve.get(symbol, now_t)
            pool_age = max(0.0, now_t - float(q.entered.get(symbol, first) or first))
            no_improve = max(0.0, now_t - last_imp)

            formal = str(row.get("formal_state") or source.get("formal_state") or row.get("state") or "")
            penalty = 0.0
            if formal not in ("BUY NOW", "PRE-IGNITION"):
                penalty = min(25.0, max(0.0, no_improve - 180.0) / 30.0 * 2.0)
            fresh_bonus = 0.0
            if pool_age <= FRESH_BONUS_SECONDS and _f(row.get("v1014_score"), 0.0) >= 70:
                fresh_bonus = 6.0 if pool_age <= 180 else 3.0

            row["rotation_pool_age_seconds"] = round(pool_age, 1)
            row["rotation_no_improve_seconds"] = round(no_improve, 1)
            row["rotation_staleness_penalty"] = round(penalty, 2)
            row["rotation_fresh_bonus"] = round(fresh_bonus, 2)
            row["rotation_adjusted_v14"] = round(
                _f(row.get("v1014_score"), 0.0) - penalty + fresh_bonus, 2
            )
            row["rotation_fresh_candidate"] = pool_age <= FRESH_BONUS_SECONDS
            out.append(row)

        out.sort(
            key=lambda row: (
                _state_rank(row.get("state") or row.get("formal_state")),
                int(row.get("passed_major_layers") or 0),
                row.get("execution_gate_status") == "PASS_ALL",
                bool(row.get("ignition15_watch")),
                bool(row.get("rotation_fresh_candidate")),
                _f(row.get("rotation_adjusted_v14"), 0.0),
                _entry_rank(row),
                _f(row.get("score"), 0.0),
            ),
            reverse=True,
        )
        return out[:limit]

    q.locks = adaptive_locks
    q.rebalance_pool = rebalance_pool_v1015
    q.structure_batch_symbols = structure_batch_symbols_v1015

    s.tick = tick_v1015
    q.tick = tick_v1015
    scanner.v7.tick = tick_v1015
    s.near_diag = near_diag_v1015

    scanner.VERSION = VERSION

    print(
        "Ψ-V10.15 FAIR-ROTATION UPGRADE ACTIVE — full-universe websocket discovery every rebalance, "
        "60-symbol structure batches with 50-slot fair coverage quota, 16 adaptive priority + 64 hunter slots, "
        "24-newcomer target, 5m pool hold, stale/no-improvement lock eviction, 3m recycle cooldown; "
        "formal PRE/BUY gates unchanged",
        flush=True,
    )


_install_v1015()


async def _rotation_telemetry_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            total = len(q.universe)
            discovery_ready = sum(len(q.disc.get(symbol, ())) >= 4 for symbol in q.universe)
            locked = q.locks()
            print(
                f"Ψ-V10.15 ROTATION discovery={discovery_ready}/{total} "
                f"structure_attempted={len(structure_attempted_ever)}/{total} "
                f"round_progress={len(structure_round_attempted)}/{total} "
                f"full_rounds={rotation_stats['full_structure_rounds']} "
                f"micro={len(app.selected_micro_symbols)}/{q.MICRO_SLOTS} "
                f"priority={len(locked)}/{q.LOCK_SLOTS} "
                f"newcomers_last={rotation_stats['newcomers_last']} "
                f"dropped_last={rotation_stats['dropped_last']} "
                f"stale_evicted={rotation_stats['stale_evictions']}",
                flush=True,
            )
            fresh = []
            for row in s.near_diag(20):
                if row.get("rotation_fresh_candidate"):
                    fresh.append(
                        (
                            str(row.get("symbol")),
                            _f(row.get("rotation_adjusted_v14"), 0.0),
                            round(_f(row.get("rotation_pool_age_seconds"), 0.0), 0),
                        )
                    )
                if len(fresh) >= 5:
                    break
            if fresh:
                print(f"Ψ-V10.15 FRESH_TOP {fresh}", flush=True)
        except Exception as exc:
            print(f"Ψ-V10.15 ROTATION_ERROR {type(exc).__name__}: {exc}", flush=True)


_previous_print_loop = scanner.v7.print_loop


async def _combined_v1015_print_loop():
    await asyncio.gather(
        _previous_print_loop(),
        _rotation_telemetry_loop(),
    )


scanner.v7.print_loop = _combined_v1015_print_loop
scanner.q.print_loop = _combined_v1015_print_loop
scanner.s.print_loop = _combined_v1015_print_loop


if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.15 ACTIVE — fair full-universe discovery/structure rotation + stale candidate eviction; "
            "strict V10.14 intelligence and PRE/BUY gates unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.15 stopped", flush=True)
