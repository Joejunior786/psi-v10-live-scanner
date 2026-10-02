import asyncio
import time
from collections import defaultdict

import psi_v11_3_entry as base

v11 = base.v11
v16 = base.v16
v17 = base.v17
app = base.app
q = base.q
scanner = base.scanner

VERSION = "11.0.3.2-strict-buy-invariant"

managed_micro_pool = []
managed_entered = {}
guard_stats = defaultdict(int)
guard_bootstrapped = False
guard_reconnect_baseline = None

_old_assign_shards = v16._assign_shards
_old_rebalance_continuity = base.rebalance_continuity
_old_strict_evaluate = app.evaluate_symbol

MANDATORY_LAYER_KEYS = (
    "ACTIVITY_LAYER",
    "FLOW_LAYER",
    "ORDER_BOOK_LAYER",
    "VWAP_LAYER",
    "MA_STRUCTURE_LAYER",
    "ANTI_CHASE_OR_RUNNER_LAYER",
)


def _gate_passed(value):
    if value is True:
        return True
    return str(value or "").upper() == "PASS"


def evaluate_strict_buy_invariant(symbol):
    """Final fail-closed BUY guard.

    No upstream setup, overlay, cached state, or transient classifier may emit
    formal BUY NOW unless all six global V10 layers and every current hard gate
    are simultaneously aligned. PRE/WATCH logic remains unchanged except when
    an invalid upstream BUY must be demoted.
    """
    row = _old_strict_evaluate(symbol)
    if not isinstance(row, dict) or not row:
        return row

    layers = row.get("layer_results") or {}
    layer_status = {key: bool(layers.get(key)) for key in MANDATORY_LAYER_KEYS}
    missing_layers = [key for key, ok in layer_status.items() if not ok]
    all_layers = bool(layer_status) and all(layer_status.values())

    hard_checks = row.get("multi_regime_hard_checks") or {}
    hard_status = {str(key): _gate_passed(value) for key, value in hard_checks.items()}
    missing_hard = [key for key, ok in hard_status.items() if not ok]
    all_hard = bool(hard_status) and all(hard_status.values())

    formal = str(row.get("formal_state") or row.get("state") or "")
    strict_ok = bool(all_layers and all_hard)

    row["strict_buy_guard_version"] = VERSION
    row["strict_buy_layer_status"] = layer_status
    row["strict_buy_all_6_layers"] = all_layers
    row["strict_buy_hard_status"] = hard_status
    row["strict_buy_all_hard_gates"] = all_hard
    row["strict_buy_gate_passed"] = strict_ok
    row["strict_buy_missing_layers"] = missing_layers
    row["strict_buy_missing_hard_gates"] = missing_hard

    if formal == "BUY NOW" and not strict_ok:
        fallback = "PRE-IGNITION" if all_hard and sum(layer_status.values()) >= 5 else "WATCH"
        row["strict_buy_demoted_from"] = "BUY NOW"
        row["state"] = fallback
        row["pre_warmup_state"] = fallback
        row["formal_state"] = fallback
        row["multi_regime_buy"] = False
        row["mandatory_all_aligned"] = False
        row["hard_safety_all_aligned"] = all_hard

        blockers = list(row.get("combined_blockers") or [])
        for blocker in missing_layers + missing_hard:
            if blocker not in blockers:
                blockers.append(blocker)
        row["combined_blockers"] = blockers
        row["strict_buy_blockers"] = list(dict.fromkeys(missing_layers + missing_hard))

        guard_stats["invalid_buy_demotions"] += 1
        print(
            "Ψ-V10 STRICT_BUY_GUARD "
            f"symbol={symbol} demoted=BUY_NOW->{fallback} "
            f"layers={sum(layer_status.values())}/6 "
            f"missingLayers={missing_layers} missingHard={missing_hard}",
            flush=True,
        )

    return row


app.evaluate_symbol = evaluate_strict_buy_invariant


def _dedup_valid(symbols):
    out = []
    seen = set()
    universe = set(getattr(q, "universe_set", set()) or set())
    for sym in symbols or []:
        sym = str(sym or "")
        if not sym or sym in seen:
            continue
        if universe and sym not in universe:
            continue
        try:
            if not v17.directional(sym):
                continue
        except Exception:
            pass
        seen.add(sym)
        out.append(sym)
        if len(out) >= base.POOL_SIZE:
            break
    return out


def _accept_managed_pool(symbols, reason="CONTINUITY"):
    global guard_bootstrapped
    now = time.time()
    pool = _dedup_valid(symbols)
    if not pool:
        return False

    old = list(managed_micro_pool)
    old_set = set(old)
    new_set = set(pool)

    for sym in list(managed_entered):
        if sym not in new_set:
            managed_entered.pop(sym, None)

    for sym in pool:
        if sym in managed_entered:
            continue
        entered = base.f(q.entered.get(sym), 0.0)
        managed_entered[sym] = entered if entered > 0 else now

    managed_micro_pool[:] = pool
    app.selected_micro_symbols = list(pool)
    for sym in pool:
        q.entered[sym] = managed_entered[sym]
        try:
            app.ensure_micro_state(sym)
        except Exception:
            pass

    if old and old_set != new_set:
        guard_stats["accepted_pool_changes"] += 1
    guard_bootstrapped = True
    return True


def _restore_managed_pool():
    if not managed_micro_pool:
        return False

    actual = _dedup_valid(app.selected_micro_symbols)
    if actual != managed_micro_pool:
        guard_stats["legacy_overrides_blocked"] += 1
        guard_stats["legacy_symbols_blocked"] += len(set(actual) ^ set(managed_micro_pool))

    app.selected_micro_symbols = list(managed_micro_pool)
    for sym in managed_micro_pool:
        if sym in managed_entered:
            q.entered[sym] = managed_entered[sym]
    return True


def _changed_shards(before, after):
    before_set = set(before)
    after_set = set(after)
    removed = before_set - after_set
    additions = after_set - before_set
    affected = {
        v16.shard_assignments.get(sym)
        for sym in removed
        if v16.shard_assignments.get(sym) is not None
    }
    affected.discard(None)
    return affected, removed, additions


async def rebalance_continuity_guarded(force=False):
    before = list(managed_micro_pool)

    if before:
        _restore_managed_pool()

    await _old_rebalance_continuity(force=force)

    after = _dedup_valid(app.selected_micro_symbols)
    if not after:
        if before:
            _restore_managed_pool()
        return

    if not before:
        _accept_managed_pool(after, reason="INITIAL")
        guard_stats["initial_accept"] += 1
        return

    if after == before:
        _restore_managed_pool()
        return

    affected, removed, additions = _changed_shards(before, after)

    pure_growth = (
        not removed
        and len(after) > len(before)
        and len(additions) <= getattr(base, "POOL_GROWTH_STEP", base.MAX_MIGRATIONS)
        and len(after) <= base.POOL_SIZE
    )
    if pure_growth:
        now = time.time()
        for sym in additions:
            entered = base.f(q.entered.get(sym), 0.0)
            managed_entered[sym] = entered if entered > 0 else now
        _accept_managed_pool(after, reason="VACANCY_GROWTH")
        guard_stats["approved_growth"] += len(additions)
        return

    valid_single_shard = (
        len(affected) <= 1
        and len(removed) <= base.MAX_MIGRATIONS
        and len(additions) <= base.MAX_MIGRATIONS
        and len(after) == len(before)
    )

    if not valid_single_shard:
        guard_stats["multi_shard_rebalances_rejected"] += 1
        guard_stats["rejected_removed"] += len(removed)
        guard_stats["rejected_added"] += len(additions)
        app.selected_micro_symbols = list(before)
        for sym in before:
            if sym in managed_entered:
                q.entered[sym] = managed_entered[sym]
        print(
            "Ψ-V11.0.3.1 SHARD_GUARD_REJECT "
            f"affected={sorted(affected)} removed={sorted(removed)} added={sorted(additions)}",
            flush=True,
        )
        return

    now = time.time()
    for sym in additions:
        entered = base.f(q.entered.get(sym), 0.0)
        managed_entered[sym] = entered if entered > 0 else now
    for sym in removed:
        managed_entered.pop(sym, None)

    _accept_managed_pool(after, reason="V11_REBALANCE")
    guard_stats["approved_rebalances"] += 1


def assign_shards_guarded():
    selected = _dedup_valid(app.selected_micro_symbols)

    if not managed_micro_pool:
        if not selected:
            return set()
        _accept_managed_pool(selected, reason="BOOTSTRAP")
        guard_stats["bootstrap_pool"] += 1
        changed = _old_assign_shards()
        guard_stats["bootstrap_changed_shards"] += len(changed)
        return changed

    _restore_managed_pool()
    changed = _old_assign_shards()

    if guard_bootstrapped and len(changed) > 1:
        guard_stats["unexpected_multi_shard_changes"] += 1
        print(
            "Ψ-V11.0.3.1 SHARD_GUARD_VIOLATION "
            f"changed={sorted(i + 1 for i in changed)}; canonical pool retained",
            flush=True,
        )
    return changed


async def pool_guard_loop():
    while True:
        await asyncio.sleep(0.2)
        try:
            if managed_micro_pool:
                _restore_managed_pool()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            guard_stats["pool_guard_errors"] += 1
            print(
                f"Ψ-V11.0.3.1 POOL_GUARD_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


async def guard_health_loop():
    global guard_reconnect_baseline
    while True:
        await asyncio.sleep(base.HEALTH_SECONDS)
        try:
            _restore_managed_pool()

            reconnects = list(v16.shard_reconnects)
            if guard_reconnect_baseline is None and managed_micro_pool:
                guard_reconnect_baseline = list(reconnects)

            delta = []
            if guard_reconnect_baseline is not None:
                delta = [
                    max(0, reconnects[i] - guard_reconnect_baseline[i])
                    for i in range(len(reconnects))
                ]

            current_set = set(managed_micro_pool)
            assigned_set = {
                sym for sym in v16.shard_assignments
                if sym in current_set
            }
            mismatch = len(current_set ^ assigned_set)

            print(
                "Ψ-V11.0.3.1 SHARD_GUARD "
                f"managed={len(managed_micro_pool)}/{base.POOL_SIZE} "
                f"assigned={len(assigned_set)} mismatch={mismatch} "
                f"blockedOverrides={guard_stats['legacy_overrides_blocked']} "
                f"approved={guard_stats['approved_rebalances']} "
                f"rejectedMulti={guard_stats['multi_shard_rebalances_rejected']} "
                f"unexpectedMulti={guard_stats['unexpected_multi_shard_changes']} "
                f"reconnects={reconnects} reconnectDelta={delta} "
                f"invalidBuyDemotions={guard_stats['invalid_buy_demotions']}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            guard_stats["health_errors"] += 1
            print(
                f"Ψ-V11.0.3.1 SHARD_GUARD_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


v16._assign_shards = assign_shards_guarded
base.rebalance_continuity = rebalance_continuity_guarded
q.rebalance_pool = rebalance_continuity_guarded

for mod in (base, v11, v16, v17):
    try:
        mod.VERSION = VERSION
    except Exception:
        pass

try:
    scanner.VERSION = VERSION
    scanner.v7.VERSION = VERSION
except Exception:
    pass

app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.3.2] strict BUY invariant active: formal BUY NOW requires all six "
        "global V10 layers plus all current hard gates; shard continuity guard remains active.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        pool_guard_loop(),
        guard_health_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
