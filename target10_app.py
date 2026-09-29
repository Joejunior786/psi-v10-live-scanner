import asyncio
import time
from collections import Counter

import aiohttp
import app
import qualifier_app as q

VERSION = "10.5-target10-hunter"
TARGET = q.QUALIFIER_TARGET

# Aggressive but governed target-10 settings. Signal gates are unchanged.
q.STRUCTURE_BATCH = 80
q.STRUCTURE_SECONDS = 20
q.MICRO_SLOTS = 40
q.MICRO_HOLD = 60
q.POOL_SECONDS = 30
q.TICK_SECONDS = 15
q.HOT_COUNT = 120
app.TOP_STRUCTURE_UNIVERSE = q.STRUCTURE_BATCH
app.MICRO_UNIVERSE_SIZE = q.MICRO_SLOTS
app.ANOMALY_PROMOTION_SLOTS = q.MICRO_SLOTS
app.STRUCTURE_REFRESH_SECONDS = q.STRUCTURE_SECONDS
app.MICRO_POOL_MIN_HOLD_SECONDS = q.MICRO_HOLD
app.USER_AGENT = "psi-v10-live-scanner/10.5-target10-hunter"

hunt_id = 1
hunt_started = time.time()
hunt_structure_seen = set()
hunt_micro_seen = set()
hunt_full_structural_pass = False
hunt_target_reached = False
hunt_exhausted = False
hunt_completed_at = 0.0
HUNT_RESTART_SECONDS = 60

_orig_refresh_structure = q.refresh_structure
_orig_tick = q.tick
_orig_coverage = q.coverage


def now():
    return time.time()


async def refresh_structure():
    """Run V10.4 structure work, then record this hunt's newly checked names."""
    global hunt_full_structural_pass, hunt_completed_at
    before = dict(q.structure_ms)
    await _orig_refresh_structure()
    for symbol, stamp in q.structure_ms.items():
        if stamp != before.get(symbol):
            hunt_structure_seen.add(symbol)
    total = len(q.universe)
    if total and len(hunt_structure_seen.intersection(q.universe_set)) >= total:
        hunt_full_structural_pass = True
        hunt_completed_at = hunt_completed_at or now()


def _locks():
    return q.locks()


def _hunter_candidates():
    rows = [
        (q.sscore(s), s)
        for s in app.structure
        if s in q.universe_set and q.sfresh(s) and s not in _locks()
    ]
    rows.sort(reverse=True)
    # Exhaustive behaviour: candidates not yet live-micro-verified in this hunt
    # are always ahead of already checked names. Score still orders each group.
    return (
        [(score, s) for score, s in rows if s not in hunt_micro_seen]
        + [(score, s) for score, s in rows if s in hunt_micro_seen]
    )


async def rebalance_pool(force=False):
    """Lock qualifiers and rotate remaining slots through unseen candidates."""
    current = list(app.selected_micro_symbols)
    cset = set(current)
    t = now()
    out, seen = [], set()
    locked = _locks()

    def add(symbol):
        if symbol in q.universe_set and symbol not in seen and len(out) < q.MICRO_SLOTS:
            out.append(symbol)
            seen.add(symbol)

    for symbol in locked:
        add(symbol)

    # Keep new hunters long enough for depth warmup + 2 persistence samples.
    if not force:
        for symbol in current:
            if symbol not in locked and t - q.entered.get(symbol, t) < q.MICRO_HOLD:
                add(symbol)

    # Reserve up to one third of open hunter slots for hot anomaly candidates.
    open_slots = max(0, q.MICRO_SLOTS - len(out))
    hot_cap = max(4, open_slots // 3) if open_slots else 0
    hot_added = 0
    for _, symbol in q.hot():
        if symbol in app.structure and q.sfresh(symbol):
            before_len = len(out)
            add(symbol)
            if len(out) > before_len:
                hot_added += 1
            if hot_added >= hot_cap:
                break

    for _, symbol in _hunter_candidates():
        add(symbol)
        if len(out) >= q.MICRO_SLOTS:
            break

    if not out or (set(out) == cset and not force):
        return

    for symbol in out:
        if symbol not in cset:
            q.entered[symbol] = t
        app.ensure_micro_state(symbol)
    for symbol in list(q.entered):
        if symbol not in out:
            q.entered.pop(symbol, None)

    app.selected_micro_symbols = out
    app.last_micro_pool_change = t
    q.pool_cycles += 1
    print(
        f"Ψ-V10.5 MICRO locked={len(locked)} hunter={len(out)-len(locked)} "
        f"total={len(out)} hunt_micro_verified={len(hunt_micro_seen)}",
        flush=True,
    )


def tick():
    global hunt_target_reached, hunt_exhausted, hunt_completed_at
    _orig_tick()
    for symbol, row in q.latest.items():
        if symbol in app.selected_micro_symbols and row.get("micro_ready"):
            hunt_micro_seen.add(symbol)

    if len(q.stable) >= TARGET:
        hunt_target_reached = True
        hunt_completed_at = hunt_completed_at or now()

    fresh_structured = {
        s for s in app.structure if s in q.universe_set and q.sfresh(s)
    }
    if (
        hunt_full_structural_pass
        and fresh_structured
        and fresh_structured.issubset(hunt_micro_seen)
        and len(q.stable) < TARGET
    ):
        hunt_exhausted = True
        hunt_completed_at = hunt_completed_at or now()


def maybe_restart_hunt():
    global hunt_id, hunt_started, hunt_structure_seen, hunt_micro_seen
    global hunt_full_structural_pass, hunt_target_reached, hunt_exhausted, hunt_completed_at
    if not hunt_completed_at or now() - hunt_completed_at < HUNT_RESTART_SECONDS:
        return
    hunt_id += 1
    hunt_started = now()
    hunt_structure_seen = set()
    hunt_micro_seen = set(_locks())
    hunt_full_structural_pass = False
    hunt_target_reached = False
    hunt_exhausted = False
    hunt_completed_at = 0.0
    print(f"Ψ-V10.5 TARGET-10 HUNT RESET id={hunt_id}", flush=True)


def coverage():
    base = _orig_coverage()
    total = len(q.universe)
    checked = len(hunt_structure_seen.intersection(q.universe_set))
    base["target"] = TARGET
    base["hunt"] = {
        "id": hunt_id,
        "active": not (hunt_target_reached or hunt_exhausted),
        "target_reached": hunt_target_reached,
        "exhausted_after_full_pass": hunt_exhausted,
        "full_structural_pass_complete": hunt_full_structural_pass,
        "structure_checked": checked,
        "structure_checked_pct": round(checked / total * 100, 2) if total else 0,
        "micro_candidates_verified": len(hunt_micro_seen),
        "seconds_running": int(now() - hunt_started),
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
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", TARGET)), TARGET))
    except ValueError:
        limit = TARGET
    app.resolve_outcomes()
    rows = q.results(limit)
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.5 Target-10 Qualifier Hunter",
        "version": VERSION,
        "policy": q.QUALIFIER_POLICY,
        "buy_policy": "ALL_HARD_SAFETY_GATES_PLUS_ALL_GATES_OF_ONE_VERIFIED_SETUP",
        "qualifier_target": TARGET,
        "returned": len(rows),
        "state_counts": dict(Counter(r["state"] for r in rows)),
        "coverage": coverage(),
        "results": rows,
        "near_miss_diagnostics": q.near_diag(),
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
        rows = q.results()
        c = coverage()
        h = c["hunt"]
        print("\n==================================================", flush=True)
        print(f"Ψ-V10.5 TARGET-10 HUNT — {len(rows)}/{TARGET}", flush=True)
        print(
            f"hunt={h['structure_checked']}/{c['full_universe']} ({h['structure_checked_pct']:.1f}%) "
            f"micro_verified={h['micro_candidates_verified']} live_micro={c['micro_verified_fresh']}/{c['micro_total']} "
            f"locked={c['locked_slots']} full_pass={h['full_structural_pass_complete']}",
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
        if h["exhausted_after_full_pass"]:
            print(
                f"FULL PASS COMPLETE: fewer than {TARGET} persistent qualifiers currently exist under strict gates.",
                flush=True,
            )


# Patch V10.4 orchestration only. Underlying evaluate_symbol gates remain untouched.
q.refresh_structure = refresh_structure
q.rebalance_pool = rebalance_pool
q.tick = tick
q.coverage = coverage
q.print_loop = print_loop
app.health = health
app.scan_endpoint = scan
app.ranked_results = q.results


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
            "Ψ-V10.5 TARGET-10 QUALIFIER HUNTER ACTIVE — 80-market rotating structure, "
            "40 live micro slots, exhaustive unseen-first rotation, locked qualifiers, "
            "TARGET=10, NO PADDING",
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
        print("Ψ-V10.5 stopped", flush=True)
