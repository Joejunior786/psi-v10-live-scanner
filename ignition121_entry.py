import asyncio
import math
import os
import time

import ignition120_entry as core

scanner, q, app = core.scanner, core.q, core.app
b1192 = core.base
b17 = b1192.b17
pump_mod = b1192.base.b18

VERSION = "10.21.0-early-warning-freshness"

STRUCTURE_MAX_AGE = float(os.environ.get("PSI_STRUCTURE_MAX_AGE", "150"))
STRUCTURE_REFRESH_AGE = float(os.environ.get("PSI_STRUCTURE_REFRESH_AGE", "75"))
STRUCTURE_REFRESH_EVERY = float(os.environ.get("PSI_STRUCTURE_REFRESH_EVERY", "8"))
STRUCTURE_REFRESH_MAX = int(os.environ.get("PSI_STRUCTURE_REFRESH_MAX", "12"))
STRUCTURE_REFRESH_CONCURRENCY = int(os.environ.get("PSI_STRUCTURE_REFRESH_CONCURRENCY", "4"))

PRE_MIN_LAYERS = int(os.environ.get("PSI_PRE_MIN_LAYERS", "5"))
PRE_MIN_IGNITION = float(os.environ.get("PSI_PRE_MIN_IGNITION", "65"))
PRE_MAX_RESISTANCE_DISTANCE = float(os.environ.get("PSI_PRE_MAX_DISTANCE", "8.0"))
PRE_MIN_SETUP_RATIO = float(os.environ.get("PSI_PRE_MIN_SETUP_RATIO", "0.70"))

PUMP_PREARM_MIN_SCORE = float(os.environ.get("PSI_PUMP_PREARM_MIN_SCORE", "55"))
PUMP_PREARM_MIN_LAYERS = int(os.environ.get("PSI_PUMP_PREARM_MIN_LAYERS", "5"))

_old_evaluate = app.evaluate_symbol
_old_main = scanner.v7.main
_old_pump_signature = pump_mod.pump_signature

refresh_stats = {"cycles": 0, "ok": 0, "errors": 0, "evaluated": 0}


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d


def passed(v):
    if v is True:
        return True
    return str(v or "").upper() == "PASS"


def layer_count(row):
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


def structure_age(sym):
    try:
        ts = int((q.structure_ms or {}).get(sym, 0) or 0)
        if ts > 0:
            return max(0.0, (int(q.ms()) - ts) / 1000.0)
    except Exception:
        pass
    return 999999.0


def structure_fresh(sym):
    return structure_age(sym) <= STRUCTURE_MAX_AGE


def best_setup_ratio(row):
    setups = (row or {}).get("multi_regime_setup_results") or (row or {}).get("setup_results") or {}
    vals = []
    for x in setups.values():
        if isinstance(x, dict):
            vals.append(f(x.get("pass_ratio")))
    return max(vals) if vals else 0.0


def decision_distance(row):
    d = (row or {}).get("breakout_distance_pct")
    if d is None:
        return None
    try:
        x = float(d)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def pre_ignition_overlay(symbol, row):
    if not isinstance(row, dict) or not row:
        return row

    age = structure_age(symbol)
    fresh = age <= STRUCTURE_MAX_AGE
    row["structure_age_seconds_121"] = round(age, 2)
    row["fresh_structure_121"] = fresh

    # Fresh structure is a new universal execution requirement. Never allow
    # a BUY or formal PRE to survive on a stale MA/structure snapshot.
    formal = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    if not fresh and formal in {"BUY NOW", "PRE-IGNITION"}:
        row["state"] = "WATCH"
        row["pre_warmup_state"] = "WATCH"
        row["formal_state"] = "WATCH"
        row["multi_regime_buy"] = False
        row["mandatory_all_aligned"] = False
        row["freshness_demotion_121"] = formal
        formal = "WATCH"

    hard = row.get("hard_safety_status") or {}
    hard_checks = dict(row.get("multi_regime_hard_checks") or {})
    hard_checks["FRESH_STRUCTURE"] = fresh
    row["multi_regime_hard_checks"] = hard_checks

    layers = layer_count(row)
    ign = f(row.get("ignition15_score"))
    ratio = best_setup_ratio(row)
    dist = decision_distance(row)
    armed = str(row.get("entry_status") or "") == "BREAKOUT_TRIGGER_ARMED"
    near = bool(armed or (dist is not None and -0.50 <= dist <= PRE_MAX_RESISTANCE_DISTANCE))
    base_state = str(row.get("legacy_state_before_multi_regime") or row.get("state") or "")
    regime_safe = passed(hard.get("MARKET_REGIME_SAFETY")) or hard.get("MARKET_REGIME_SAFETY") is None

    alignment = bool(
        (layers >= PRE_MIN_LAYERS and ign >= PRE_MIN_IGNITION)
        or (layers >= PRE_MIN_LAYERS and ratio >= PRE_MIN_SETUP_RATIO)
        or (armed and layers >= PRE_MIN_LAYERS and ign >= PRE_MIN_IGNITION - 5)
    )
    pre_ready = bool(
        fresh
        and regime_safe
        and near
        and alignment
        and formal != "BUY NOW"
        and base_state != "REJECT"
    )

    exec_blockers = [
        k for k in (
            "LIVE_MICRO_DATA",
            "TRADE_SEQUENCE_VALID",
            "BOOK_SEQUENCE_VALID",
            "SPREAD_FILTER",
            "SLIPPAGE_FILTER",
            "CUMULATIVE_EXTENSION_GUARD",
            "QUALIFIED_MICRO_WARMUP",
        )
        if not passed(hard_checks.get(k, hard.get(k)))
    ]

    row["pre_ignition_121"] = pre_ready
    row["pre_ignition_exec_ready_121"] = len(exec_blockers) == 0
    row["pre_ignition_exec_blockers_121"] = exec_blockers
    row["pre_ignition_layers_121"] = layers
    row["pre_ignition_setup_ratio_121"] = round(ratio, 4)
    row["pre_ignition_distance_121"] = dist

    # PRE-IGNITION is now an early-warning state. It intentionally does not
    # require every execution gate. BUY NOW remains owned by V10.20 and still
    # needs every hard gate + one complete regime setup, plus fresh structure.
    if pre_ready:
        row["state"] = "PRE-IGNITION"
        row["pre_warmup_state"] = "PRE-IGNITION"
        row["formal_state"] = "PRE-IGNITION"

    return row


def evaluate_121(symbol):
    row = _old_evaluate(symbol)
    return pre_ignition_overlay(symbol, row)


app.evaluate_symbol = evaluate_121


def pump_signature_121(sym):
    ps = dict(_old_pump_signature(sym) or {})
    row = q.latest.get(sym) or {}
    score = f(ps.get("pump_signature_score"))
    layers = int(ps.get("layers_118") or layer_count(row) or 0)
    rapid = f(ps.get("rapid_score"))
    peak = f(ps.get("rapid_peak_5m"))
    h120 = int(ps.get("rapid_hits_120_5m") or 0)
    dv30 = f(ps.get("local_dv30_per_min"))
    trend = f(ps.get("rank_rv_trend_30s")) + f(ps.get("rank_trade_trend_30s")) + f(ps.get("rank_ofi_trend_30s"))
    accelerating = bool(dv30 > 0.02 or trend > 0.05 or (peak >= 120 and h120 >= 3) or rapid >= 140)
    fresh = structure_fresh(sym)

    state = str(ps.get("pump_state") or "NONE")
    prearmed = bool(
        state != "PUMP-ARMED"
        and score >= PUMP_PREARM_MIN_SCORE
        and layers >= PUMP_PREARM_MIN_LAYERS
        and accelerating
        and fresh
    )
    if prearmed:
        ps["pump_state"] = "PUMP-PRE-ARMED"

    ps["pump_prearmed_121"] = prearmed
    ps["pump_prearmed_fresh_structure_121"] = fresh
    ps["pump_prearmed_acceleration_121"] = accelerating
    ps["structure_age_seconds_121"] = round(structure_age(sym), 2)
    return ps


pump_mod.pump_signature = pump_signature_121


def priority_symbols(limit=48):
    out = []

    def add(sym):
        sym = str(sym or "")
        if not sym or not sym.endswith("USDT") or sym in out:
            return
        try:
            if hasattr(b1192, "directional") and not b1192.directional(sym):
                return
        except Exception:
            pass
        out.append(sym)

    try:
        for _, sym in q.hot(32):
            add(sym)
    except Exception:
        pass

    try:
        ranked = sorted(
            q.latest.items(),
            key=lambda kv: (
                layer_count(kv[1]),
                f(kv[1].get("ignition15_score")),
                f(kv[1].get("opportunity_score")),
            ),
            reverse=True,
        )
        for sym, _ in ranked[:32]:
            add(sym)
    except Exception:
        pass

    try:
        for sym in list(app.selected_micro_symbols or []):
            add(sym)
    except Exception:
        pass

    return out[:limit]


async def refresh_structure(sym):
    if app.session is None:
        return False
    try:
        sd, an = await asyncio.gather(
            app.load_structure(app.session, sym),
            app.load_fast_anomaly(app.session, sym),
        )
        if isinstance(sd, dict):
            app.structure[sym] = sd
            q.structure_ms[sym] = q.ms()
        if isinstance(an, dict):
            app.anomaly_state[sym] = an
        if isinstance(sd, dict):
            row = app.evaluate_symbol(sym)
            if isinstance(row, dict) and row:
                q.latest[sym] = row
                refresh_stats["evaluated"] += 1
            refresh_stats["ok"] += 1
            return True
    except asyncio.CancelledError:
        raise
    except Exception:
        refresh_stats["errors"] += 1
    return False


async def freshness_loop():
    sem = asyncio.Semaphore(max(1, STRUCTURE_REFRESH_CONCURRENCY))

    async def one(sym):
        async with sem:
            await refresh_structure(sym)

    while True:
        await asyncio.sleep(STRUCTURE_REFRESH_EVERY)
        try:
            stale = [s for s in priority_symbols() if structure_age(s) > STRUCTURE_REFRESH_AGE]
            targets = stale[: max(1, STRUCTURE_REFRESH_MAX)]
            if targets:
                await asyncio.gather(*(one(s) for s in targets))
            refresh_stats["cycles"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            refresh_stats["errors"] += 1


def pullback_prearmed_rows():
    now = time.time()
    rows = []
    try:
        cache = b1192.pb_cache
        max_age = f(getattr(b1192, "PB_AGE", 90.0), 90.0)
    except Exception:
        return []

    for sym, x in list(cache.items()):
        if not isinstance(x, dict):
            continue
        if now - f(x.get("updated"), 0.0) > max_age:
            continue
        state = str(x.get("state") or "")
        score = f(x.get("score"))
        trend = f(x.get("trend_score"))
        zone = f(x.get("zone_dist"), 99.0)
        flow = f(x.get("flow"))
        layers = int(x.get("layers") or 0)
        plan_ok = f(x.get("entry")) > 0 and f(x.get("stop")) > 0
        pre = bool(
            state in {"PULLBACK_ZONE", "RECLAIM_PENDING", "LIQUIDITY_SWEEP"}
            and score >= 68
            and trend >= 80
            and zone <= 0.75
            and flow >= 50
            and layers >= 4
            and plan_ok
        )
        if pre:
            rows.append((score, sym, x))
    rows.sort(reverse=True)
    return rows


async def early_state_print_loop():
    while True:
        await asyncio.sleep(float(getattr(app, "PRINT_SECONDS", 30)))
        try:
            latest_rows = list(q.latest.items())
            formal_pre = [
                (sym, r) for sym, r in latest_rows
                if str((r or {}).get("formal_state") or (r or {}).get("state") or "") == "PRE-IGNITION"
                and structure_fresh(sym)
            ]
            pre_pumps = []
            for sym, ps in list(getattr(pump_mod, "pump_cache", {}).items()):
                if str((ps or {}).get("pump_state") or "") == "PUMP-PRE-ARMED":
                    pre_pumps.append((f(ps.get("pump_signature_score")), sym, ps))
            pre_pumps.sort(reverse=True)
            pb_pre = pullback_prearmed_rows()
            candidates = priority_symbols(32)
            fresh_count = sum(structure_fresh(s) for s in candidates)

            print(
                f"Ψ-V10.21 EARLY STATES formal_pre={len(formal_pre)} "
                f"pump_prearmed={len(pre_pumps)} pullback_prearmed={len(pb_pre)} "
                f"fresh_priority={fresh_count}/{len(candidates)} "
                f"refreshCycles={refresh_stats['cycles']} refreshOK={refresh_stats['ok']} "
                f"refreshErrors={refresh_stats['errors']}",
                flush=True,
            )

            for i, (sym, r) in enumerate(sorted(
                formal_pre,
                key=lambda z: (f(z[1].get("ignition15_score")), layer_count(z[1]), -f(z[1].get("structure_age_seconds_121"), 999999)),
                reverse=True,
            )[:10], 1):
                print(
                    f"PRE{i:02d}. {sym:<14} ign15={f(r.get('ignition15_score')):5.1f} "
                    f"layers={layer_count(r)}/6 setup={f(r.get('pre_ignition_setup_ratio_121')):.2f} "
                    f"dist={f(r.get('pre_ignition_distance_121'), 99):+.3f}% "
                    f"fresh={f(r.get('structure_age_seconds_121')):.0f}s "
                    f"execReady={'YES' if r.get('pre_ignition_exec_ready_121') else 'NO'} "
                    f"blockers={r.get('pre_ignition_exec_blockers_121') or []}",
                    flush=True,
                )

            for i, (score, sym, ps) in enumerate(pre_pumps[:10], 1):
                print(
                    f"PPA{i:02d}. {sym:<14} sig={score:5.1f} layers={int(ps.get('layers_118') or 0)}/6 "
                    f"rapid={f(ps.get('rapid_score')):5.1f} peak={f(ps.get('rapid_peak_5m')):5.1f} "
                    f"dv30={f(ps.get('local_dv30_per_min')):+.3f}/m fresh={f(ps.get('structure_age_seconds_121')):.0f}s",
                    flush=True,
                )

            for i, (score, sym, x) in enumerate(pb_pre[:10], 1):
                print(
                    f"PBPA{i:02d}. {sym:<14} score={score:5.1f} trend={f(x.get('trend_score')):5.1f} "
                    f"zoneDist={f(x.get('zone_dist')):.3f}% flow={f(x.get('flow')):4.1f} "
                    f"layers={int(x.get('layers') or 0)}/6 entry={x.get('entry')} stop={x.get('stop')} "
                    f"tp1={x.get('tp1')} tp2={x.get('tp2')} tp3={x.get('tp3')}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.21 EARLY_STATE_ERROR {type(e).__name__}: {e}", flush=True)


core.VERSION = VERSION
b1192.VERSION = VERSION
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        "[v10.21] freshness + early-warning layer active: "
        "fresh structure required for execution; PRE-IGNITION decoupled from execution warmup; "
        "PUMP-PRE-ARMED and PULLBACK-PRE-ARMED added; BUY NOW rules unchanged/stricter",
        flush=True,
    )
    await asyncio.gather(_old_main(), freshness_loop(), early_state_print_loop())


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Psi-V10.21 stopped", flush=True)
