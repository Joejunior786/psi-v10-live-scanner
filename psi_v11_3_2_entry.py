import asyncio
import time

import psi_v11_3_1_entry as base
import ignition1191_entry as riskmap
import ignition1192_entry as pullback

app = base.app
q = base.q
scanner = base.scanner

VERSION = "11.0.3.3-dynamic-runner-mode"
RUNNER_PRINT_SECONDS = 30.0

# Runner allocation is intentionally partial. The final tranche is not a fixed
# hard exit; it is managed by a trailing rule so exceptional moves are not cut
# short by the ordinary 1R/2R/3R risk-map ladder.
RUNNER_ALLOCATION = {
    "tp1_pct": 20,
    "tp2_pct": 25,
    "tp3_pct": 25,
    "runner_pct": 30,
}
RUNNER_TRAIL = "5M_9EMA_OR_15M_STRUCTURE_LOW"
RUNNER_AFTER_TP1 = "MOVE_STOP_TO_ENTRY_PLUS_FEES"

_old_risk_trade_plan = riskmap.trade_plan
_old_pullback_eval = pullback.pb_eval


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if x == x and abs(x) != float("inf") else default


def _gate_passed(value):
    if value is True:
        return True
    return str(value or "").upper() == "PASS"


def _runner_context(sym):
    row = q.latest.get(sym) or {}
    layers = row.get("layer_results") or {}
    layer_status = {
        key: bool(layers.get(key))
        for key in base.MANDATORY_LAYER_KEYS
    }
    all_layers = bool(layer_status) and all(layer_status.values())

    # Prefer the strict guard's own audited booleans when they are available.
    if "strict_buy_all_6_layers" in row:
        all_layers = bool(row.get("strict_buy_all_6_layers"))

    hard = row.get("multi_regime_hard_checks") or row.get("hard_safety_status") or {}
    all_hard = bool(hard) and all(_gate_passed(v) for v in hard.values())
    if "strict_buy_all_hard_gates" in row:
        all_hard = bool(row.get("strict_buy_all_hard_gates"))

    formal = str(row.get("formal_state") or row.get("pre_warmup_state") or row.get("state") or "")
    candidate = bool(all_layers and all_hard and formal in {"PRE-IGNITION", "BUY NOW"})
    active = bool(candidate and formal == "BUY NOW")

    return {
        "formal": formal,
        "all_layers": all_layers,
        "all_hard": all_hard,
        "candidate": candidate,
        "active": active,
        "layers": sum(layer_status.values()),
    }


def _structural_levels(values, entry):
    out = []
    for value in values or []:
        x = f(value)
        if x > entry:
            out.append(x)
    return sorted(set(out))


def _snap_up(entry, floor_pct, cap_pct, structural):
    floor_price = entry * (1.0 + floor_pct / 100.0)
    cap_price = entry * (1.0 + cap_pct / 100.0)
    candidates = [x * 0.999 for x in structural if floor_price <= x <= cap_price]
    return min(candidates) if candidates else floor_price


def _runner_targets(sym, entry, stop, structural=None):
    entry = f(entry)
    stop = f(stop)
    if entry <= 0 or stop <= 0 or stop >= entry:
        return None

    risk_pct = (entry - stop) / entry * 100.0

    # Dynamic floors: tight-stop trades no longer dump the whole position at
    # 1R/2R/3R. Wider legitimate risk naturally widens the target ladder.
    tp1_pct = max(3.0, min(5.0, risk_pct * 1.5))
    tp2_pct = max(7.0, min(10.0, risk_pct * 3.0))
    tp3_pct = max(12.0, min(18.0, risk_pct * 5.0))
    runner_pct = max(20.0, min(30.0, risk_pct * 8.0))

    levels = _structural_levels(structural, entry)
    tp1 = _snap_up(entry, tp1_pct, 6.0, levels)
    tp2 = _snap_up(entry, tp2_pct, 12.0, levels)
    tp3 = _snap_up(entry, tp3_pct, 20.0, levels)
    runner_ref = _snap_up(entry, runner_pct, 35.0, levels)

    # Keep the ladder strictly increasing even when several structural levels
    # collapse into the same region.
    tp2 = max(tp2, tp1 * 1.0025)
    tp3 = max(tp3, tp2 * 1.0025)
    runner_ref = max(runner_ref, tp3 * 1.0025)

    return {
        "runner_tp1": tp1,
        "runner_tp2": tp2,
        "runner_tp3": tp3,
        "runner_reference": runner_ref,
        "runner_tp1_pct": (tp1 / entry - 1.0) * 100.0,
        "runner_tp2_pct": (tp2 / entry - 1.0) * 100.0,
        "runner_tp3_pct": (tp3 / entry - 1.0) * 100.0,
        "runner_reference_pct": (runner_ref / entry - 1.0) * 100.0,
        "runner_risk_pct": risk_pct,
    }


def _apply_runner(sym, plan, structural=None, source="RISKMAP"):
    if not isinstance(plan, dict):
        return plan

    ctx = _runner_context(sym)
    plan["runner_candidate"] = ctx["candidate"]
    plan["runner_active"] = ctx["active"]
    plan["runner_mode"] = "ACTIVE" if ctx["active"] else ("CANDIDATE" if ctx["candidate"] else "OFF")
    plan["runner_source"] = source

    # No target widening unless the setup is currently 6/6 and all hard gates
    # pass. This does not create a BUY signal; ACTIVE still requires formal BUY.
    if not ctx["candidate"]:
        return plan

    entry = f(plan.get("entry_trigger"), f(plan.get("entry")))
    stop = f(plan.get("stop_loss"), f(plan.get("stop")))
    targets = _runner_targets(sym, entry, stop, structural)
    if not targets:
        return plan

    plan["base_tp1"] = plan.get("tp1")
    plan["base_tp2"] = plan.get("tp2")
    plan["base_tp3"] = plan.get("tp3")
    plan.update(targets)

    # The scanner now exposes the larger runner ladder as the primary target
    # path for eligible setups while retaining the old values for diagnostics.
    plan["tp1"] = targets["runner_tp1"]
    plan["tp2"] = targets["runner_tp2"]
    plan["tp3"] = targets["runner_tp3"]
    plan["runner_allocation"] = dict(RUNNER_ALLOCATION)
    plan["runner_after_tp1"] = RUNNER_AFTER_TP1
    plan["runner_trail"] = RUNNER_TRAIL
    plan["target_basis"] = "DYNAMIC_RUNNER+STRUCTURE+RISK"
    return plan


def trade_plan_runner(sym, price, atr, supports, resistances, sweep):
    plan = _old_risk_trade_plan(sym, price, atr, supports, resistances, sweep)
    structural = [f(x.get("level")) for x in resistances or []]
    return _apply_runner(sym, plan, structural=structural, source="RISKMAP")


def pullback_eval_runner(sym, tm, c5, c15):
    out = _old_pullback_eval(sym, tm, c5, c15)
    if not isinstance(out, dict):
        return out

    ri = riskmap.risk_intel(sym)
    structural = [ri.get("resistance1"), ri.get("resistance2"), ri.get("resistance3"), tm.get("recent_high")]

    # pb_eval uses entry/stop names, so normalize them for the shared overlay.
    temp = {
        "entry": out.get("entry"),
        "stop": out.get("stop"),
        "tp1": out.get("tp1"),
        "tp2": out.get("tp2"),
        "tp3": out.get("tp3"),
    }
    temp = _apply_runner(sym, temp, structural=structural, source="PULLBACK")

    for key in (
        "tp1", "tp2", "tp3", "base_tp1", "base_tp2", "base_tp3",
        "runner_candidate", "runner_active", "runner_mode", "runner_source",
        "runner_tp1", "runner_tp2", "runner_tp3", "runner_reference",
        "runner_tp1_pct", "runner_tp2_pct", "runner_tp3_pct",
        "runner_reference_pct", "runner_risk_pct", "runner_allocation",
        "runner_after_tp1", "runner_trail", "target_basis",
    ):
        if key in temp:
            out[key] = temp[key]
    return out


riskmap.trade_plan = trade_plan_runner
pullback.pb_eval = pullback_eval_runner


def _best_runner_plan(sym):
    plans = []
    pb = pullback.pb_intel(sym)
    if isinstance(pb, dict) and pb.get("runner_candidate") and f(pb.get("entry")) > 0:
        plans.append(("PULLBACK", pb, f(pb.get("score"))))
    ri = riskmap.risk_intel(sym)
    if isinstance(ri, dict) and ri.get("runner_candidate") and f(ri.get("entry_trigger")) > 0:
        plans.append(("RISKMAP", ri, f(ri.get("sweep_score"))))
    return max(plans, key=lambda x: x[2]) if plans else None


def _fmt(v):
    x = f(v)
    if x <= 0:
        return "-"
    if x >= 1000:
        return f"{x:.2f}"
    if x >= 1:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


async def runner_board_loop():
    while True:
        await asyncio.sleep(RUNNER_PRINT_SECONDS)
        try:
            symbols = set(riskmap.risk_cache) | set(pullback.pb_cache)
            rows = []
            for sym in symbols:
                best = _best_runner_plan(sym)
                if not best:
                    continue
                source, plan, score = best
                ctx = _runner_context(sym)
                rows.append((1 if ctx["active"] else 0, score, sym, source, plan, ctx))

            rows.sort(reverse=True)
            active = sum(x[0] for x in rows)
            print(
                f"Ψ-V10 RUNNER BOARD candidates={len(rows)} active={active} "
                f"allocation=20/25/25/30 trail={RUNNER_TRAIL}",
                flush=True,
            )
            for i, (_, score, sym, source, plan, ctx) in enumerate(rows[:10], 1):
                entry = f(plan.get("entry_trigger"), f(plan.get("entry")))
                stop = f(plan.get("stop_loss"), f(plan.get("stop")))
                print(
                    f"RN{i:02d}. {sym:<14} mode={plan.get('runner_mode','OFF'):<9} "
                    f"formal={ctx['formal']:<16} src={source:<8} score={score:5.1f} "
                    f"entry={_fmt(entry)} stop={_fmt(stop)} "
                    f"tp1={_fmt(plan.get('tp1'))}({f(plan.get('runner_tp1_pct')):+.1f}%) "
                    f"tp2={_fmt(plan.get('tp2'))}({f(plan.get('runner_tp2_pct')):+.1f}%) "
                    f"tp3={_fmt(plan.get('tp3'))}({f(plan.get('runner_tp3_pct')):+.1f}%) "
                    f"runner={_fmt(plan.get('runner_reference'))}({f(plan.get('runner_reference_pct')):+.1f}%)",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-V10 RUNNER_ERROR {type(exc).__name__}: {exc}", flush=True)


for mod in (base, scanner, scanner.v7):
    try:
        mod.VERSION = VERSION
    except Exception:
        pass
app.USER_AGENT = f"psi-v11/{VERSION}"


async def main():
    print(
        "[v11.0.3.3] DYNAMIC RUNNER MODE active — strict BUY invariant unchanged; "
        "eligible 6/6 + hard-pass setups use wider structure-aware TP ladders, "
        "20/25/25/30 partial allocation, breakeven-plus-fees after TP1, and a "
        "5M 9-EMA / 15M structure trailing runner.",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        runner_board_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
