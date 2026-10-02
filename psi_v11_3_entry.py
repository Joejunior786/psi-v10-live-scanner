import asyncio
import math
import os
import time
from collections import defaultdict

import psi_v11_2_2_entry as base
import psi_v11_entry as v11
import ignition116_entry as v16
import ignition117_entry as v17

VERSION = "11.0.3-continuity-core"

app = v11.app
q = v11.q
scanner = v11.scanner

POOL_SIZE = max(80, int(os.getenv("PSI_V11_CONTINUITY_POOL", "80")))
CORE_SLOTS = max(16, min(40, int(os.getenv("PSI_V11_CORE_SLOTS", "32"))))
STABLE_SLOTS = max(16, min(40, int(os.getenv("PSI_V11_STABLE_SLOTS", "24"))))
EXPLORER_SLOTS = max(8, POOL_SIZE - CORE_SLOTS - STABLE_SLOTS)

CORE_HOLD_SECONDS = max(600.0, float(os.getenv("PSI_V11_CORE_HOLD_SECONDS", "1200")))
PRE_HOLD_SECONDS = max(CORE_HOLD_SECONDS, float(os.getenv("PSI_V11_PRE_HOLD_SECONDS", "1800")))
STABLE_HOLD_SECONDS = max(300.0, float(os.getenv("PSI_V11_STABLE_HOLD_SECONDS", "900")))
EXPLORER_HOLD_SECONDS = max(90.0, float(os.getenv("PSI_V11_EXPLORER_HOLD_SECONDS", "180")))
REBALANCE_MIN_SECONDS = max(15.0, float(os.getenv("PSI_V11_REBALANCE_MIN_SECONDS", "30")))
SHARD_COOLDOWN_SECONDS = max(30.0, float(os.getenv("PSI_V11_SHARD_COOLDOWN_SECONDS", "120")))
MAX_MIGRATIONS = max(1, min(6, int(os.getenv("PSI_V11_MAX_MIGRATIONS", "4"))))
POOL_GROWTH_STEP = max(1, min(8, int(os.getenv("PSI_V11_POOL_GROWTH_STEP", "4"))))
POOL_STRUCTURE_MAX_AGE = max(90.0, float(os.getenv("PSI_V11_POOL_STRUCTURE_MAX_AGE", "300")))
HOT_PROMOTION_TTL = max(180.0, float(os.getenv("PSI_V11_HOT_PROMOTION_TTL", "600")))
HEALTH_SECONDS = max(15.0, float(os.getenv("PSI_V11_CONTINUITY_HEALTH_SECONDS", "30")))

DATA_BLOCKERS = {
    "LIVE_MICRO_DATA",
    "TRADE_SEQUENCE_VALID",
    "BOOK_SEQUENCE_VALID",
    "QUALIFIED_MICRO_WARMUP",
    "MICRO_NOT_READY",
    "MICRO_WARMUP",
    "WAITING_FRESH_MICRO",
}
MARKET_EXECUTION_BLOCKERS = {
    "SPREAD_FILTER",
    "SLIPPAGE_FILTER",
    "CUMULATIVE_EXTENSION_GUARD",
    "MARKET_REGIME_SAFETY",
}

continuity_until = {}
continuity_reason = {}
forced_promotions = {}
last_rebalance = 0.0
continuity_stats = defaultdict(int)

epoch_started_ms = 0
epoch_completed = 0
epoch_seen = set()

_old_evaluate = app.evaluate_symbol
_old_opp_score = v17.opp_score


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def _state(row):
    return str((row or {}).get("formal_state") or (row or {}).get("state") or "")


def _layers(row):
    lr = (row or {}).get("layer_results") or {}
    keys = (
        "ACTIVITY_LAYER",
        "FLOW_LAYER",
        "ORDER_BOOK_LAYER",
        "VWAP_LAYER",
        "MA_STRUCTURE_LAYER",
        "ANTI_CHASE_OR_RUNNER_LAYER",
    )
    return sum(bool(lr.get(k)) for k in keys)


def _distance(row):
    value = (row or {}).get("breakout_distance_pct")
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _all_blockers(row):
    row = row or {}
    out = set()
    for key in (
        "combined_blockers",
        "failed_execution_gates",
        "failed_hard",
        "pre_ignition_exec_blockers_121",
    ):
        vals = row.get(key) or []
        if isinstance(vals, str):
            vals = [vals]
        for item in vals:
            if item:
                out.add(str(item))

    hard = row.get("multi_regime_hard_checks") or row.get("hard_safety_status") or {}
    for key, value in hard.items():
        if str(value).upper() not in {"PASS", "TRUE"} and value is not True:
            if key in DATA_BLOCKERS or key in MARKET_EXECUTION_BLOCKERS:
                out.add(str(key))
    return out


def evaluate_with_blocker_class(symbol):
    row = _old_evaluate(symbol)
    if not isinstance(row, dict) or not row:
        return row

    blockers = _all_blockers(row)
    data = sorted(blockers & DATA_BLOCKERS)
    market = sorted(blockers - DATA_BLOCKERS)

    if not blockers:
        cls = "EXECUTION_READY"
    elif data and not market:
        cls = "DATA_NOT_READY"
    elif data and market:
        cls = "MIXED_DATA_AND_MARKET"
    else:
        cls = "MARKET_REJECT"

    row["blocker_class_1103"] = cls
    row["data_blockers_1103"] = data
    row["market_blockers_1103"] = market
    row["infrastructure_blocked_1103"] = cls in {"DATA_NOT_READY", "MIXED_DATA_AND_MARKET"}
    return row


app.evaluate_symbol = evaluate_with_blocker_class


def opp_score_1103(row):
    raw = f(_old_opp_score(row))
    age = max(0.0, f((row or {}).get("no_progress_seconds")))
    if age <= 300.0:
        penalty = 0.0
    else:
        penalty = min(22.0, (age - 300.0) / 60.0 * 0.85)

    state = _state(row)
    if state in {"BUY NOW", "PRE-IGNITION"}:
        penalty *= 0.35
    elif str((row or {}).get("entry_status") or "") == "BREAKOUT_TRIGGER_ARMED":
        penalty *= 0.60

    if isinstance(row, dict):
        row["freshness_rank_penalty_1103"] = round(penalty, 2)
        row["freshness_rank_score_1103"] = round(raw - penalty, 2)
    return round(raw - penalty, 2)


v17.opp_score = opp_score_1103
v16._opportunity_score = opp_score_1103


def _v11_map():
    out = {}
    for row in list(getattr(v11, "v11_board", []) or []):
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "")
        if sym:
            out[sym] = row
    return out


def _priority_signal(sym, row, pred=None):
    now = time.time()
    state = _state(row)
    pstate = str((pred or {}).get("predictive_state") or "")
    dist = _distance(row)
    entry = str((row or {}).get("entry_status") or "")
    layers = _layers(row)
    blockers = _all_blockers(row)

    if (
        entry == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
        and state not in {"BUY NOW", "PRE-IGNITION"}
    ):
        return 0.0, None, 0.0

    score = 0.0
    reason = None
    hold = CORE_HOLD_SECONDS

    if state == "BUY NOW":
        score, reason, hold = 130.0, "FORMAL_BUY", PRE_HOLD_SECONDS
    elif state == "PRE-IGNITION":
        score, reason, hold = 120.0, "FORMAL_PRE", PRE_HOLD_SECONDS
    elif pstate == "V11-PRIME":
        score, reason, hold = 116.0, "V11_PRIME", PRE_HOLD_SECONDS
    elif pstate == "V11-EARLY":
        score, reason, hold = 112.0, "V11_EARLY", CORE_HOLD_SECONDS
    elif entry == "BREAKOUT_TRIGGER_ARMED" and dist is not None and 0.0 <= dist <= 3.0 and layers >= 4:
        score, reason, hold = 108.0, "NEAR_TRIGGER", CORE_HOLD_SECONDS
    elif bool((row or {}).get("ignition15_watch")):
        score, reason, hold = 104.0, "IGNITION_WATCH", CORE_HOLD_SECONDS
    elif layers == 6 and f((row or {}).get("ignition15_score")) >= 60.0:
        score, reason, hold = 98.0, "SIX_LAYER_BUILD", CORE_HOLD_SECONDS

    if sym in forced_promotions and forced_promotions[sym] > now:
        score = max(score, 110.0)
        reason = reason or "HOT_DISCOVERY"
        hold = max(hold, HOT_PROMOTION_TTL)

    if state not in {"BUY NOW", "PRE-IGNITION"}:
        if "CUMULATIVE_EXTENSION_GUARD" in blockers and entry == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER":
            return 0.0, None, 0.0

    return score, reason, hold


def _candidate_score(sym):
    score = -999.0
    try:
        if sym in app.structure:
            score = f(q.sscore(sym), -999.0)
    except Exception:
        pass

    row = q.latest.get(sym) or {}
    score += _layers(row) * 3.0
    state = _state(row)
    if state == "BUY NOW":
        score += 80.0
    elif state == "PRE-IGNITION":
        score += 55.0
    elif state == "EARLY OPPORTUNITY":
        score += 15.0

    dist = _distance(row)
    if dist is not None and 0.0 <= dist <= 3.0:
        score += 22.0
    elif dist is not None and 0.0 <= dist <= 8.0:
        score += 10.0

    pred = next(
        (x for x in list(getattr(v11, "v11_board", []) or []) if x.get("symbol") == sym),
        None,
    )
    if pred:
        score += f(pred.get("score")) * 0.35
        if str(pred.get("predictive_state") or "") == "V11-EARLY":
            score += 25.0
        elif str(pred.get("predictive_state") or "") == "V11-PRIME":
            score += 40.0

    ts = int(q.structure_ms.get(sym, 0) or 0)
    if ts > 0:
        age = max(0.0, (q.ms() - ts) / 1000.0)
        if age > 300:
            score -= min(30.0, (age - 300.0) / 60.0 * 1.5)
    return score


def _refresh_continuity_locks():
    now = time.time()
    pred = _v11_map()
    symbols = set(q.latest) | set(app.selected_micro_symbols) | set(pred)

    try:
        symbols |= set(q.locks())
    except Exception:
        pass

    for sym in symbols:
        if sym not in q.universe_set:
            continue
        row = q.latest.get(sym) or {}
        score, reason, hold = _priority_signal(sym, row, pred.get(sym))
        if score > 0:
            continuity_until[sym] = max(continuity_until.get(sym, 0.0), now + hold)
            continuity_reason[sym] = (score, reason or "PRIORITY")

    for sym in list(continuity_until):
        if sym not in q.universe_set or continuity_until[sym] < now:
            continuity_until.pop(sym, None)
            continuity_reason.pop(sym, None)

    for sym in list(forced_promotions):
        if forced_promotions[sym] < now:
            forced_promotions.pop(sym, None)


def _structure_fresh_for_pool(sym):
    ts = int(q.structure_ms.get(sym, 0) or 0)
    if ts <= 0 or not isinstance(app.structure.get(sym), dict):
        return False
    age = max(0.0, (q.ms() - ts) / 1000.0)
    return age <= POOL_STRUCTURE_MAX_AGE


def _core_symbols():
    _refresh_continuity_locks()
    now = time.time()
    rows = []
    for sym, until in continuity_until.items():
        if until < now or sym not in q.universe_set or not _structure_fresh_for_pool(sym):
            continue
        pri, reason = continuity_reason.get(sym, (0.0, "PRIORITY"))
        row = q.latest.get(sym) or {}
        rows.append((
            f(pri),
            1 if _state(row) == "BUY NOW" else 0,
            1 if _state(row) == "PRE-IGNITION" else 0,
            _layers(row),
            f(row.get("ignition15_score")),
            _candidate_score(sym),
            sym,
            reason,
        ))
    rows.sort(reverse=True)
    return [x[-2] for x in rows[:CORE_SLOTS]]


def _candidate_universe():
    seen = set()
    out = []

    def add(sym):
        sym = str(sym or "")
        if not sym or sym in seen or sym not in q.universe_set:
            return
        if not _structure_fresh_for_pool(sym):
            return
        try:
            if not v17.directional(sym):
                return
        except Exception:
            pass
        seen.add(sym)
        out.append(sym)

    for sym in _core_symbols():
        add(sym)
    for sym in app.selected_micro_symbols:
        add(sym)

    try:
        for _, sym in q.hot(max(160, POOL_SIZE * 2)):
            add(sym)
    except Exception:
        pass

    try:
        ranked = sorted(
            (s for s in app.structure if s in q.universe_set),
            key=_candidate_score,
            reverse=True,
        )
        for sym in ranked[: max(160, POOL_SIZE * 2)]:
            add(sym)
    except Exception:
        pass

    for row in list(getattr(v11, "v11_board", []) or []):
        add((row or {}).get("symbol"))

    return out


def _replaceable(sym, urgent=False):
    if sym in set(_core_symbols()):
        return False

    age = max(0.0, time.time() - f(q.entered.get(sym), time.time()))
    if age < EXPLORER_HOLD_SECONDS:
        return False

    row = q.latest.get(sym) or {}
    state = _state(row)
    if state in {"BUY NOW", "PRE-IGNITION"} or row.get("ignition15_watch"):
        return False

    if not urgent and age < STABLE_HOLD_SECONDS and _candidate_score(sym) >= 45.0:
        return False
    return True


def _eligible_victim_shard(urgent=False):
    now = time.time()
    choices = []
    for sh in range(v16.MICRO_SHARDS):
        since = now - f(v16.shard_last_change[sh], 0.0)
        if since < SHARD_COOLDOWN_SECONDS:
            continue
        victims = [
            sym for sym in app.selected_micro_symbols
            if v16.shard_assignments.get(sym) == sh and _replaceable(sym, urgent=urgent)
        ]
        if not victims:
            continue
        victims.sort(key=_candidate_score)
        choices.append((_candidate_score(victims[0]), -len(victims), sh, victims))
    if not choices:
        return None, []
    choices.sort()
    _, _, sh, victims = choices[0]
    return sh, victims


async def rebalance_continuity(force=False):
    global last_rebalance

    if not q.universe:
        return

    now = time.time()
    _refresh_continuity_locks()
    current = [
        s for s in dict.fromkeys(app.selected_micro_symbols)
        if s in q.universe_set and v17.directional(s)
    ]
    current_set = set(current)

    candidates = _candidate_universe()
    cores = _core_symbols()
    core_set = set(cores)

    if not current:
        desired = []
        initial_target = min(POOL_SIZE, POOL_GROWTH_STEP)
        for sym in cores + sorted(candidates, key=_candidate_score, reverse=True):
            if sym not in desired:
                desired.append(sym)
            if len(desired) >= initial_target:
                break
        app.selected_micro_symbols = desired[:initial_target]
        for sym in app.selected_micro_symbols:
            q.entered[sym] = now
            app.ensure_micro_state(sym)
        app.last_micro_pool_change = now
        last_rebalance = now
        continuity_stats["initial_fills"] += len(app.selected_micro_symbols)
        print(
            f"Ψ-V11.0.3 CONTINUITY_INIT pool={len(app.selected_micro_symbols)}/{POOL_SIZE} "
            f"core={len(core_set)}",
            flush=True,
        )
        return

    if now - last_rebalance < REBALANCE_MIN_SECONDS and not force:
        continuity_stats["rate_deferred"] += 1
        return

    if len(current) < POOL_SIZE:
        added = 0
        for sym in cores + sorted(candidates, key=_candidate_score, reverse=True):
            if sym in current_set:
                continue
            current.append(sym)
            current_set.add(sym)
            q.entered[sym] = now
            app.ensure_micro_state(sym)
            continuity_stats["vacancy_fills"] += 1
            added += 1
            if len(current) >= POOL_SIZE or added >= POOL_GROWTH_STEP:
                break
        app.selected_micro_symbols = current[:POOL_SIZE]
        app.last_micro_pool_change = now
        last_rebalance = now
        return

    core_add = [s for s in cores if s not in current_set]
    general_add = [
        s for s in sorted(candidates, key=_candidate_score, reverse=True)
        if s not in current_set and s not in core_set
    ]

    urgent = bool(core_add)
    additions = (core_add + general_add)[:MAX_MIGRATIONS]
    if not additions:
        continuity_stats["no_change"] += 1
        last_rebalance = now
        return

    shard, victims = _eligible_victim_shard(urgent=urgent)
    if shard is None or not victims:
        continuity_stats["shard_deferred"] += len(additions)
        last_rebalance = now
        return

    n = min(MAX_MIGRATIONS, len(additions), len(victims))
    additions = additions[:n]
    victims = victims[:n]
    victim_set = set(victims)

    final = [s for s in current if s not in victim_set]
    final.extend(additions)
    final = final[:POOL_SIZE]

    for sym in additions:
        q.entered[sym] = now
        app.ensure_micro_state(sym)
    for sym in victims:
        q.entered.pop(sym, None)

    app.selected_micro_symbols = final
    app.last_micro_pool_change = now
    last_rebalance = now
    continuity_stats["migrations"] += n
    continuity_stats["core_promotions"] += sum(s in core_set for s in additions)

    print(
        f"Ψ-V11.0.3 CONTINUITY_REBALANCE shard={shard + 1} "
        f"added={additions} removed={victims} core={len(core_set)} "
        f"migrations={continuity_stats['migrations']}",
        flush=True,
    )


q.MICRO_SLOTS = max(int(getattr(q, "MICRO_SLOTS", 0)), POOL_SIZE)
q.LOCK_SLOTS = max(int(getattr(q, "LOCK_SLOTS", 0)), CORE_SLOTS)
q.MICRO_HOLD = max(int(getattr(q, "MICRO_HOLD", 0)), int(STABLE_HOLD_SECONDS))
q.LOCK_GRACE = max(int(getattr(q, "LOCK_GRACE", 0)), int(PRE_HOLD_SECONDS))
app.MICRO_UNIVERSE_SIZE = q.MICRO_SLOTS
app.ANOMALY_PROMOTION_SLOTS = q.MICRO_SLOTS
app.MICRO_POOL_MIN_HOLD_SECONDS = q.MICRO_HOLD

q.rebalance_pool = rebalance_continuity


async def hot_loop_continuity():
    while True:
        await asyncio.sleep(5.0)
        try:
            if not q.universe or app.session is None:
                continue

            promoted = None
            for score, sym in q.hot(24):
                if score < f(getattr(v17, "HOT_SCORE", 150.0), 150.0):
                    break
                if sym in app.selected_micro_symbols:
                    continue

                age = (
                    (q.ms() - int(q.structure_ms.get(sym, 0) or 0)) / 1000.0
                    if q.structure_ms.get(sym)
                    else 999999.0
                )
                if age > f(getattr(v17, "HOT_STRUCTURE_AGE", 45.0), 45.0):
                    if not await v17.refresh_hot(sym):
                        continue

                forced_promotions[sym] = time.time() + HOT_PROMOTION_TTL
                continuity_until[sym] = max(
                    continuity_until.get(sym, 0.0),
                    time.time() + HOT_PROMOTION_TTL,
                )
                continuity_reason[sym] = (110.0, "HOT_DISCOVERY")
                promoted = sym
                break

            if promoted:
                continuity_stats["hot_requests"] += 1
                await rebalance_continuity(force=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            continuity_stats["hot_errors"] += 1
            print(
                f"Ψ-V11.0.3 HOT_CONTINUITY_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


v17.hot_loop = hot_loop_continuity


async def continuity_health_loop():
    while True:
        await asyncio.sleep(HEALTH_SECONDS)
        try:
            _refresh_continuity_locks()
            current = list(dict.fromkeys(app.selected_micro_symbols))
            core = set(_core_symbols())
            now = time.time()

            ages = [max(0.0, now - f(q.entered.get(s), now)) for s in current]
            mature = sum(a >= 90.0 for a in ages)
            stable = sum(a >= STABLE_HOLD_SECONDS for a in ages)
            ready_ev = sum(bool((v11.event_state.get(s) or {}).get("ready")) for s in current)

            data_only = mixed = market = ready = 0
            for sym in current:
                cls = str((q.latest.get(sym) or {}).get("blocker_class_1103") or "")
                if cls == "DATA_NOT_READY":
                    data_only += 1
                elif cls == "MIXED_DATA_AND_MARKET":
                    mixed += 1
                elif cls == "MARKET_REJECT":
                    market += 1
                elif cls == "EXECUTION_READY":
                    ready += 1

            connected = sum(bool(x) for x in v16.shard_connected)
            print(
                f"Ψ-V11.0.3 CONTINUITY pool={len(current)}/{POOL_SIZE} "
                f"core={len(core)}/{CORE_SLOTS} mature90={mature} stable={stable} "
                f"eventReady={ready_ev}/{len(current)} shards={connected}/{v16.MICRO_SHARDS} "
                f"dataOnly={data_only} mixed={mixed} marketReject={market} execReady={ready} "
                f"migrations={continuity_stats['migrations']} "
                f"deferred={continuity_stats['shard_deferred'] + continuity_stats['rate_deferred']}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            continuity_stats["health_errors"] += 1
            print(
                f"Ψ-V11.0.3 CONTINUITY_HEALTH_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


async def structure_epoch_loop():
    global epoch_started_ms, epoch_completed, epoch_seen
    last_print = 0.0

    while True:
        await asyncio.sleep(2.0)
        try:
            if not q.universe:
                continue

            if epoch_started_ms <= 0:
                existing = [
                    int(q.structure_ms.get(sym, 0) or 0)
                    for sym in q.universe
                    if int(q.structure_ms.get(sym, 0) or 0) > 0
                ]
                epoch_started_ms = min(existing) if existing else q.ms()
                epoch_seen = set()

            for sym in q.universe:
                if int(q.structure_ms.get(sym, 0) or 0) >= epoch_started_ms:
                    epoch_seen.add(sym)

            total = len(q.universe)
            if total > 0 and len(epoch_seen) >= total:
                epoch_completed += 1
                print(
                    f"Ψ-V11.0.3 FULL_ROUND complete={epoch_completed} symbols={total}/{total}",
                    flush=True,
                )
                epoch_started_ms = q.ms() + 1
                epoch_seen = set()

            if time.time() - last_print >= HEALTH_SECONDS:
                print(
                    f"Ψ-V11.0.3 STRUCTURE_EPOCH complete={epoch_completed} "
                    f"progress={len(epoch_seen)}/{total}",
                    flush=True,
                )
                last_print = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            continuity_stats["epoch_errors"] += 1
            print(
                f"Ψ-V11.0.3 STRUCTURE_EPOCH_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


for mod in (base, v11, v17, v16):
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
        "[v11.0.3] continuity core active: persistent PRE/V11/near-trigger micro locks, "
        "single-shard migration with cooldown, data-vs-market blocker classification, "
        "freshness-decayed ranking, and explicit 403-symbol structural epochs; "
        "formal PRE/BUY gates unchanged.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        continuity_health_loop(),
        structure_epoch_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        try:
            v11.persist_outcomes(force=True)
        except Exception:
            pass
        print("Psi-V11.0.3 stopped", flush=True)
