import asyncio
import time
from collections import Counter

import aiohttp
import app
import qualifier_app as q

VERSION = "10.6-stable-target10"
TARGET = q.QUALIFIER_TARGET

# Stability-first target-10 settings. Qualification gates remain unchanged.
q.STRUCTURE_BATCH = 80
q.STRUCTURE_SECONDS = 25
q.MICRO_SLOTS = 40
q.MICRO_HOLD = 180
q.POOL_SECONDS = 120
q.TICK_SECONDS = 10
q.HOT_COUNT = 120
app.TOP_STRUCTURE_UNIVERSE = q.STRUCTURE_BATCH
app.MICRO_UNIVERSE_SIZE = q.MICRO_SLOTS
app.ANOMALY_PROMOTION_SLOTS = q.MICRO_SLOTS
app.STRUCTURE_REFRESH_SECONDS = q.STRUCTURE_SECONDS
app.MICRO_POOL_MIN_HOLD_SECONDS = q.MICRO_HOLD
app.USER_AGENT = "psi-v10-live-scanner/10.6-stable-target10"

ROTATE_PER_CYCLE = 8
QUALIFIER_DATA_GRACE = 45
NEAR_MEMORY_SECONDS = 300
HUNT_RESTART_SECONDS = 60

hunt_id = 1
hunt_started = time.time()
hunt_attempted = set()
hunt_structurally_valid = set()
hunt_micro_seen = set()
hunt_full_structural_pass = False
hunt_target_reached = False
hunt_exhausted = False
hunt_completed_at = 0.0

# Persistence state based only on verified samples. Missing micro data pauses rather than resets.
verified_state = {}
stable_until = {}
near_memory = {}
_base_coverage = q.coverage


def now():
    return time.time()


def _locks():
    return q.locks()


def _state_rank(state):
    return app.STATE_PRIORITY.get(state or "REJECT", 0)


def _near_row(row, blockers=None):
    return {
        "symbol": row.get("symbol"),
        "state": row.get("state"),
        "score": row.get("score"),
        "active_setup": row.get("active_setup"),
        "failed_hard": list(row.get("failed_hard", []) or []),
        "failed_setup": list(row.get("failed_setup", []) or []),
        "micro_ready": bool(row.get("micro_ready")),
        "blockers": list(blockers or []),
        "seen_at": now(),
    }


def near_diag(limit=10):
    cutoff = now() - NEAR_MEMORY_SECONDS
    for symbol in list(near_memory):
        if near_memory[symbol].get("seen_at", 0) < cutoff:
            near_memory.pop(symbol, None)
    rows = list(near_memory.values())
    rows.sort(
        key=lambda r: (
            _state_rank(r.get("state")),
            float(r.get("score", 0) or 0),
            r.get("seen_at", 0),
        ),
        reverse=True,
    )
    return rows[:limit]


async def refresh_structure():
    """Truthfully count every attempted structure symbol, whether it produced a valid row or not."""
    global hunt_full_structural_pass, hunt_completed_at
    if app.session is None or now() < q.backoff_until:
        return
    await q.refresh_universe()
    batch = q.structure_batch_symbols()
    if not batch:
        return

    hunt_attempted.update(batch)
    await app.structure_batch(batch)
    stamp = q.ms()
    valid = 0
    for symbol in batch:
        if symbol in app.structure:
            q.structure_ms[symbol] = stamp
            q.structure_seen.add(symbol)
            hunt_structurally_valid.add(symbol)
            valid += 1

    app.last_structure_refresh = now()
    app.scanner_ready = bool(app.structure)
    q.structure_cycles += 1
    await rebalance_pool(force=not app.selected_micro_symbols)

    total = len(q.universe)
    attempted = len(hunt_attempted.intersection(q.universe_set))
    if total and attempted >= total:
        hunt_full_structural_pass = True
        hunt_completed_at = hunt_completed_at or now()

    print(
        f"Ψ-V10.6 STRUCTURE valid={valid}/{len(batch)} attempted={attempted}/{total} "
        f"valid_ever={len(hunt_structurally_valid.intersection(q.universe_set))}",
        flush=True,
    )


def _candidate_priority():
    locked = set(_locks())
    ranked = [
        (q.sscore(symbol), symbol)
        for symbol in app.structure
        if symbol in q.universe_set and q.sfresh(symbol) and symbol not in locked
    ]
    ranked.sort(reverse=True)

    unseen = [(score, symbol) for score, symbol in ranked if symbol not in hunt_micro_seen]
    seen = [(score, symbol) for score, symbol in ranked if symbol in hunt_micro_seen]

    # Hot unseen names jump to the front but do not trigger unlimited pool churn.
    hot_scores = {symbol: score for score, symbol in q.hot()}
    unseen.sort(key=lambda x: (x[1] in hot_scores, hot_scores.get(x[1], -1e9), x[0]), reverse=True)
    return unseen + seen


async def rebalance_pool(force=False):
    """Keep the micro pool stable and replace at most ROTATE_PER_CYCLE mature hunter slots."""
    current = list(app.selected_micro_symbols)
    current_set = set(current)
    t = now()
    locked = _locks()
    locked_set = set(locked)

    # First start fills the full pool immediately.
    if force or not current:
        out = []
        seen = set()

        def add(symbol):
            if symbol in q.universe_set and symbol not in seen and len(out) < q.MICRO_SLOTS:
                out.append(symbol)
                seen.add(symbol)

        for symbol in locked:
            add(symbol)
        for _, symbol in _candidate_priority():
            add(symbol)
        for _, symbol in q.hot():
            add(symbol)
        if not out:
            return
        for symbol in out:
            q.entered[symbol] = t
            app.ensure_micro_state(symbol)
        app.selected_micro_symbols = out
        app.last_micro_pool_change = t
        q.pool_cycles += 1
        print(
            f"Ψ-V10.6 MICRO initial locked={len(locked)} hunter={len(out)-len(locked)} total={len(out)}",
            flush=True,
        )
        return

    out = list(current)

    # Ensure all currently locked candidates remain present.
    for symbol in locked:
        if symbol not in out:
            replaceable = [
                s for s in out
                if s not in locked_set and t - q.entered.get(s, t) >= q.MICRO_HOLD
            ]
            if replaceable:
                victim = min(replaceable, key=lambda s: q.entered.get(s, t))
                out[out.index(victim)] = symbol
                q.entered.pop(victim, None)
                q.entered[symbol] = t
                app.ensure_micro_state(symbol)

    # Mature slots can rotate, but only a small slice per cycle.
    eligible_victims = [
        s for s in out
        if s not in locked_set and t - q.entered.get(s, t) >= q.MICRO_HOLD
    ]
    eligible_victims.sort(key=lambda s: q.entered.get(s, t))

    candidates = [
        symbol for _, symbol in _candidate_priority()
        if symbol not in set(out)
    ]
    replacements = min(ROTATE_PER_CYCLE, len(eligible_victims), len(candidates))
    for i in range(replacements):
        victim = eligible_victims[i]
        newcomer = candidates[i]
        idx = out.index(victim)
        out[idx] = newcomer
        q.entered.pop(victim, None)
        q.entered[newcomer] = t
        app.ensure_micro_state(newcomer)

    if set(out) == current_set:
        return

    app.selected_micro_symbols = out
    app.last_micro_pool_change = t
    q.pool_cycles += 1
    print(
        f"Ψ-V10.6 MICRO locked={len(locked)} rotated={replacements} total={len(out)} "
        f"micro_verified={len(hunt_micro_seen)}",
        flush=True,
    )


def tick():
    """Use consecutive verified samples; temporary micro unavailability pauses persistence."""
    global hunt_target_reached, hunt_exhausted, hunt_completed_at

    rows = []
    selected = list(app.selected_micro_symbols)
    for symbol in selected:
        try:
            row = app.evaluate_symbol(symbol)
        except Exception:
            continue
        if row:
            rows.append(row)

    t = now()
    current_symbols = set()

    for row in rows:
        symbol = row["symbol"]
        current_symbols.add(symbol)
        q.latest[symbol] = row
        raw = row.get("state", "REJECT")
        micro_ready = bool(row.get("micro_ready"))

        if micro_ready:
            hunt_micro_seen.add(symbol)
            previous_verified = verified_state.get(symbol)

            if raw in q.QUALIFIER_STATES:
                if previous_verified in q.QUALIFIER_STATES:
                    q.streak[symbol] += 1
                else:
                    q.streak[symbol] = 1
                verified_state[symbol] = raw
                q.last_raw[symbol] = raw
                q.locked_until[symbol] = max(q.locked_until.get(symbol, 0), t + q.LOCK_GRACE)

                if q.streak[symbol] >= q.PERSIST:
                    published = dict(row)
                    published["persistence_samples"] = q.streak[symbol]
                    published["hunter_locked"] = True
                    q.stable[symbol] = published
                    stable_until[symbol] = t + QUALIFIER_DATA_GRACE
                    near_memory.pop(symbol, None)
                else:
                    near_memory[symbol] = _near_row(row, ["WAIT_PERSISTENCE"])
            else:
                # A real verified gate failure invalidates persistence immediately.
                q.streak[symbol] = 0
                verified_state[symbol] = raw
                q.last_raw[symbol] = raw
                q.stable.pop(symbol, None)
                stable_until.pop(symbol, None)
                near_memory[symbol] = _near_row(row)
        else:
            # Do not turn missing/stale micro telemetry into a false market failure.
            # Previously confirmed qualifiers survive briefly while the feed heals.
            if symbol in q.stable and stable_until.get(symbol, 0) < t:
                q.stable.pop(symbol, None)
                stable_until.pop(symbol, None)
            blockers = ["MICRO_NOT_READY"]
            if q.streak.get(symbol, 0) > 0:
                blockers.append("PERSISTENCE_PAUSED")
            near_memory[symbol] = _near_row(row, blockers)

    # Expire grace-retained qualifiers if they stopped receiving usable data.
    for symbol in list(q.stable):
        if symbol not in current_symbols and stable_until.get(symbol, 0) < t:
            q.stable.pop(symbol, None)
            stable_until.pop(symbol, None)

    for symbol in list(q.locked_until):
        if q.locked_until[symbol] < t:
            q.locked_until.pop(symbol, None)

    q.near = near_diag(10)
    q.qualifier_cycles += 1

    if len(q.stable) >= TARGET:
        hunt_target_reached = True
        hunt_completed_at = hunt_completed_at or t

    fresh_valid = {
        s for s in hunt_structurally_valid
        if s in q.universe_set and q.sfresh(s)
    }
    if (
        hunt_full_structural_pass
        and fresh_valid
        and fresh_valid.issubset(hunt_micro_seen)
        and len(q.stable) < TARGET
    ):
        hunt_exhausted = True
        hunt_completed_at = hunt_completed_at or t


def results(limit=TARGET):
    rows = list(q.stable.values())
    rows.sort(
        key=lambda row: (
            2 if row.get("state") == "BUY NOW" else 1,
            row.get("persistence_samples", 0),
            float(row.get("score", 0) or 0),
        ),
        reverse=True,
    )
    return rows[:max(1, min(int(limit or TARGET), TARGET))]


def maybe_restart_hunt():
    global hunt_id, hunt_started, hunt_attempted, hunt_structurally_valid, hunt_micro_seen
    global hunt_full_structural_pass, hunt_target_reached, hunt_exhausted, hunt_completed_at
    if not hunt_completed_at or now() - hunt_completed_at < HUNT_RESTART_SECONDS:
        return
    hunt_id += 1
    hunt_started = now()
    hunt_attempted = set()
    hunt_structurally_valid = set()
    hunt_micro_seen = set(_locks())
    hunt_full_structural_pass = False
    hunt_target_reached = False
    hunt_exhausted = False
    hunt_completed_at = 0.0
    print(f"Ψ-V10.6 TARGET-10 HUNT RESET id={hunt_id}", flush=True)


def coverage():
    base = _base_coverage()
    total = len(q.universe)
    attempted = len(hunt_attempted.intersection(q.universe_set))
    valid = len(hunt_structurally_valid.intersection(q.universe_set))
    base["target"] = TARGET
    base["hunt"] = {
        "id": hunt_id,
        "active": not (hunt_target_reached or hunt_exhausted),
        "target_reached": hunt_target_reached,
        "exhausted_after_full_pass": hunt_exhausted,
        "full_structural_pass_complete": hunt_full_structural_pass,
        "structure_attempted": attempted,
        "structure_attempted_pct": round(attempted / total * 100, 2) if total else 0,
        "structure_valid": valid,
        "structure_valid_pct": round(valid / total * 100, 2) if total else 0,
        "micro_candidates_verified": len(hunt_micro_seen),
        "seconds_running": int(now() - hunt_started),
        "pool_hold_seconds": q.MICRO_HOLD,
        "pool_rotation_seconds": q.POOL_SECONDS,
        "max_replacements_per_rotation": ROTATE_PER_CYCLE,
    }
    return base


async def health(req):
    return app.web.json_response({
        "ok": True,
        "service": "psi-v10-live-scanner",
        "version": VERSION,
        "policy": q.QUALIFIER_POLICY,
        "qualifier_target": TARGET,
        "persistence_samples": q.PERSIST,
        "scanner_ready": app.scanner_ready,
        "websocket_connected": app.websocket_connected,
        "discovery_ws_connected": q.disc_ws,
        "strict_uk_allowlist_enabled": bool(app.UK_SYMBOLS),
        "coverage": coverage(),
        "rest_governor": {
            "requests": q.rest_requests,
            "backoff_active": now() < q.backoff_until,
            "backoff_remaining_seconds": max(0, int(q.backoff_until - now())),
            "http_418_count": q.count418,
            "http_429_count": q.count429,
        },
        "stable_qualifier_symbols": list(q.stable),
        "near_miss_diagnostics": near_diag(10),
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", TARGET)), TARGET))
    except ValueError:
        limit = TARGET
    app.resolve_outcomes()
    rows = results(limit)
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.6 Stable Target-10 Hunter",
        "version": VERSION,
        "policy": q.QUALIFIER_POLICY,
        "buy_policy": "ALL_HARD_SAFETY_GATES_PLUS_ALL_GATES_OF_ONE_VERIFIED_SETUP",
        "qualifier_target": TARGET,
        "returned": len(rows),
        "state_counts": dict(Counter(row["state"] for row in rows)),
        "coverage": coverage(),
        "results": rows,
        "near_miss_diagnostics": near_diag(10),
        "generated_ms": q.ms(),
    })


async def hunt_tick_loop():
    while True:
        await asyncio.sleep(q.TICK_SECONDS)
        tick()
        maybe_restart_hunt()


async def print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        rows = results()
        c = coverage()
        h = c["hunt"]
        print("\n==================================================", flush=True)
        print(f"Ψ-V10.6 STABLE TARGET-10 HUNT — {len(rows)}/{TARGET}", flush=True)
        print(
            f"attempted={h['structure_attempted']}/{c['full_universe']} ({h['structure_attempted_pct']:.1f}%) "
            f"valid={h['structure_valid']} micro_verified={h['micro_candidates_verified']} "
            f"live_micro={c['micro_verified_fresh']}/{c['micro_total']} locked={c['locked_slots']} "
            f"full_pass={h['full_structural_pass_complete']}",
            flush=True,
        )
        print("==================================================", flush=True)
        for i, row in enumerate(rows, 1):
            print(
                f"{i:02d}. {row['symbol']:12s} {row['state']:14s} score={row['score']:6.2f} "
                f"persist={row.get('persistence_samples',0)} setup={row['active_setup'][:10]:10s} "
                f"OFI={row['ofi']:+.3f} OBI={row['obi']:+.3f} "
                f"buy={row['aggressive_buy_ratio']:.2%} ready={row['micro_ready']}",
                flush=True,
            )
        if not rows:
            print("No persistent PRE-IGNITION / BUY NOW setup currently qualifies.", flush=True)

        diagnostics = near_diag(10)
        if diagnostics:
            print("TOP NEAR MISSES:", flush=True)
            for i, d in enumerate(diagnostics, 1):
                blockers = d.get("blockers", [])
                hard = d.get("failed_hard", [])
                setup = d.get("failed_setup", [])
                print(
                    f"N{i:02d}. {str(d.get('symbol')):12s} state={str(d.get('state')):12s} "
                    f"score={float(d.get('score',0) or 0):6.2f} micro={d.get('micro_ready')} "
                    f"blockers={blockers} hard={hard} setup={setup}",
                    flush=True,
                )

        if h["exhausted_after_full_pass"]:
            print(
                f"FULL PASS + MICRO VERIFICATION COMPLETE: fewer than {TARGET} persistent qualifiers exist under strict gates.",
                flush=True,
            )


# Patch orchestration only. app.evaluate_symbol and all strict PRE/BUY gates are unchanged.
q.refresh_structure = refresh_structure
q.rebalance_pool = rebalance_pool
q.tick = tick
q.results = results
q.near_diag = near_diag
q.print_loop = print_loop
app.health = health
app.scan_endpoint = scan
app.ranked_results = results


async def main():
    app.session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=aiohttp.TCPConnector(limit=100, ttl_dns_cache=300),
        headers={"User-Agent": app.USER_AGENT},
    )
    runner = await app.start_http_server()
    tasks = []
    try:
        print(
            "Ψ-V10.6 STABLE TARGET-10 HUNTER ACTIVE — 80 structure batch, 40 stable micro slots, "
            "180s minimum hold, max 8 replacements/120s, verified-sample persistence, "
            "qualifier data grace, near-miss gate diagnostics, TARGET=10, NO PADDING",
            flush=True,
        )
        await q.refresh_universe(True)
        tasks.append(asyncio.create_task(q.discovery_loop()))
        await refresh_structure()
        await q.refresh_anomaly()
        tick()
        tasks += [
            asyncio.create_task(q.loop(q.UNIVERSE_SECONDS, lambda: q.refresh_universe(True), "UNIVERSE")),
            asyncio.create_task(q.loop(q.STRUCTURE_SECONDS, refresh_structure, "STRUCTURE")),
            asyncio.create_task(q.loop(q.ANOMALY_SECONDS, q.refresh_anomaly, "ANOMALY")),
            asyncio.create_task(q.loop(q.POOL_SECONDS, rebalance_pool, "POOL")),
            asyncio.create_task(app.websocket_loop()),
            asyncio.create_task(hunt_tick_loop()),
            asyncio.create_task(print_loop()),
        ]
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        await app.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Ψ-V10.6 stopped", flush=True)
