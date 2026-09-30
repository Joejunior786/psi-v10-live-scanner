import asyncio
import time
from collections import defaultdict

import orderbook_patch

# Patch the Binance depth reconciler before importing the active scanner.
orderbook_patch.install_depth_sequence_patch()

import ignition110_app as scanner

# Add explicit diagnostics so 'book data missing' is never confused with
# 'live book present but bullish pressure not confirmed'.
orderbook_patch.install_diagnostics(scanner)


WARMUP_SECONDS = 90
READY_STREAK_REQUIRED = 3
CANDIDATE_LOCK_SECONDS = 1200
MIN_POOL_HOLD_SECONDS = 900
PROTECT_SCORE = 68.0
PROTECT_BIG_MOVE_SCORE = 55.0
PROTECT_STATES = {"WATCH", "EARLY OPPORTUNITY", "PRE-IGNITION", "BUY NOW"}

warm_started = {}
ready_streak = defaultdict(int)
telemetry_meta = {}


def _append_once(items, value):
    if value not in items:
        items.append(value)


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


def _install_warmup():
    q = scanner.q
    s = scanner.s
    app = scanner.app

    q.MICRO_HOLD = max(int(q.MICRO_HOLD), MIN_POOL_HOLD_SECONDS)
    q.LOCK_GRACE = max(int(q.LOCK_GRACE), CANDIDATE_LOCK_SECONDS)
    app.MICRO_POOL_MIN_HOLD_SECONDS = q.MICRO_HOLD

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

        telemetry_meta[symbol] = {
            "raw_ready": raw_micro_ready,
            "streak": int(ready_streak[symbol]),
            "qualified_ready": qualified_ready,
        }
        return row

    def tick_with_candidate_lock():
        original_tick()
        now_t = time.time()
        candidates = []
        for symbol, row in list(q.latest.items()):
            if symbol not in q.universe_set or not _promising(row):
                continue
            candidates.append((
                3 if row.get("pre_warmup_state") == "BUY NOW" else
                2 if row.get("pre_warmup_state") == "PRE-IGNITION" else
                1 if row.get("pre_warmup_state") in ("WATCH", "EARLY OPPORTUNITY") else 0,
                float(row.get("big_move_potential_score") or 0.0),
                float(row.get("score") or 0.0),
                symbol,
            ))
        candidates.sort(reverse=True)
        for _, _, _, symbol in candidates[: q.LOCK_SLOTS]:
            q.locked_until[symbol] = max(
                float(q.locked_until.get(symbol, 0.0) or 0.0),
                now_t + CANDIDATE_LOCK_SECONDS,
            )

        selected = set(app.selected_micro_symbols)
        for symbol in list(warm_started):
            if symbol not in selected and symbol not in q.locked_until:
                if now_t - warm_started[symbol] > CANDIDATE_LOCK_SECONDS:
                    warm_started.pop(symbol, None)
                    ready_streak.pop(symbol, None)
                    telemetry_meta.pop(symbol, None)

    def near_diag_with_collection_state(limit=10):
        rows = original_near_diag(limit)
        now_t = time.time()
        out = []
        for item in rows:
            d = dict(item)
            symbol = d.get("symbol")
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

            # Diagnostics must distinguish execution/safety gates from signal layers.
            # An empty failed_hard list means every execution gate passed; it does
            # NOT mean Flow/Book/VWAP/MA signal layers passed.
            hard_failures = list(d.get("failed_hard", []) or [])
            layer_failures = list(d.get("failed_layers", d.get("failed_setup", [])) or [])
            extra_blockers = list(d.get("blockers", []) or [])

            d["failed_execution_gates"] = hard_failures
            d["execution_gate_status"] = "PASS_ALL" if not hard_failures else "BLOCKED"
            d["missing_signal_layers"] = layer_failures

            combined = []
            for blocker in hard_failures + layer_failures + extra_blockers:
                _append_once(combined, blocker)
            d["combined_blockers"] = combined

            # Human-readable legacy field used by the current print loop.
            # Keep the real machine-readable failures above while avoiding hard=[]
            # being mistaken for 'everything passed'.
            if not hard_failures:
                d["failed_hard"] = "PASS_ALL"

            out.append(d)
        return out

    app.evaluate_symbol = evaluate_with_warmup
    s.tick = tick_with_candidate_lock
    s.near_diag = near_diag_with_collection_state
    scanner.VERSION = "10.11-micro-warmup-lock"

    print(
        "Ψ-V10.11 MICRO WARMUP ACTIVE — 90s warm-up + 3 ready samples + "
        "WATCH/early candidate locking + 900s minimum pool hold + separated execution/layer diagnostics",
        flush=True,
    )


_install_warmup()


async def _combined_print_loop():
    await asyncio.gather(
        scanner.print_loop(),
        orderbook_patch.book_diagnostic_loop(scanner),
    )


scanner.v7.print_loop = _combined_print_loop
scanner.q.print_loop = _combined_print_loop
scanner.s.print_loop = _combined_print_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.11 ACTIVE — hardened book sequencing + micro warmup + candidate locking + clear blocker diagnostics",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.11 stopped", flush=True)
