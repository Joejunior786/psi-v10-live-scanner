import asyncio
import json
import os
import time
from collections import Counter

import redis.asyncio as redis_async

CORE = None
AUTHORITY_CHAIN = "V12.3.4_LANES->V12.3.4_FAIL_CLOSED_AUTHORITY->BUY_NOW"
HARDENING_REVISION = "12.3.4-rapid-guaranteed-promotion"
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

_last_rebalance = 0.0
_rotation_epoch = 0
_protected_pool = []
_last_micro_diag = {}
_last_diag_mono = 0.0
_activity_rank_cache = []
_activity_rank_mono = 0.0
_last_rapid_diag_mono = 0.0
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
    return (
        1 if rapid_score >= RAPID_MIN_SCORE else 0,
        min(rapid_score, 250.0),
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
    global _last_rapid_diag_mono

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
    priority = [symbol for symbol in desired[:priority_slots] if symbol in universe_set]
    rapid_priority = [
        symbol for symbol in rapid_ranked[:rapid_slots]
        if symbol in universe_set and symbol not in set(priority)
    ]
    priority_set = set(priority)
    protected_priority_set = priority_set | set(rapid_priority)

    # Reserve a guaranteed RAPID challenger tier before the normal activity
    # hunters. This prevents absolute notional/24h-volume bias from hiding a
    # smaller coin whose relative acceleration is already extreme.
    # A final fair/discovery tier remains after these slots.
    activity_hunters = []
    for symbol in activity_ranked:
        if symbol in universe_set and symbol not in protected_priority_set:
            activity_hunters.append(symbol)
        if len(activity_hunters) >= activity_hunter_slots:
            break

    ordered = []
    ordered_seen = set()
    for symbol in priority + rapid_priority + activity_hunters + desired + universe:
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
    rapid_missing = [symbol for symbol in rapid_priority if symbol not in current]
    budget = max_churn if can_rebalance else 0
    if can_rebalance and rapid_missing:
        # RAPID challengers can accelerate faster than the normal sticky-pool
        # churn rate. Allow a small bounded burst so they hydrate before the
        # move is over, without permitting a wholesale pool flip.
        budget = min(12, max(budget, min(rapid_churn, len(rapid_missing))))

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
        # First reserve bounded access for live RAPID challengers such as GTC.
        for symbol in rapid_priority:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= min(budget, rapid_churn):
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
        "Ψ-V12.3.4 HARDENING installed — activity-qualified protected pool + guarded workers + live gate diagnostics + fail-closed authority",
        flush=True,
    )
