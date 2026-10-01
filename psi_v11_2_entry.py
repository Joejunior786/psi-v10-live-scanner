import asyncio
import math
import os
import time
from collections import Counter

import psi_v11_1_entry as base
import ignition120_entry as v120

VERSION = "11.0.2-adaptive-extension-guard"

app = base.v11.app

# Preserve the pre-V10.20 evaluator. V10.20 calls this global dynamically,
# so replacing it upgrades the extension decision *before* the formal
# multi-regime BUY hard-gate evaluation runs.
_legacy_pre_multi_evaluate = v120._old_evaluate

GREEN_BASE_PCT = float(os.getenv("PSI_EXT_GREEN_BASE_PCT", "8.0"))
GREEN_ATR_MULT = float(os.getenv("PSI_EXT_GREEN_ATR_MULT", "2.0"))
GREEN_MIN_PCT = float(os.getenv("PSI_EXT_GREEN_MIN_PCT", "10.0"))
GREEN_MAX_PCT = float(os.getenv("PSI_EXT_GREEN_MAX_PCT", "18.0"))

RED_BASE_PCT = float(os.getenv("PSI_EXT_RED_BASE_PCT", "18.0"))
RED_ATR_MULT = float(os.getenv("PSI_EXT_RED_ATR_MULT", "4.0"))
RED_MIN_PCT = float(os.getenv("PSI_EXT_RED_MIN_PCT", "22.0"))
RED_MAX_PCT = float(os.getenv("PSI_EXT_RED_MAX_PCT", "35.0"))

GREEN_MA50_ATR = float(os.getenv("PSI_EXT_GREEN_MA50_ATR", "2.25"))
RED_MA50_ATR = float(os.getenv("PSI_EXT_RED_MA50_ATR", "2.75"))
AMBER_PULLBACK_ATR = float(os.getenv("PSI_EXT_AMBER_PULLBACK_ATR", "0.75"))

stats = Counter()
last_rows = {}


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _state_bool(value):
    if value is True:
        return True
    return str(value or "").upper() == "PASS"


def _adaptive_extension(symbol, row):
    ext = row.get("extension_guard") or {}
    telemetry = str(ext.get("extension_telemetry_status") or "UNKNOWN")
    hard = row.setdefault("hard_safety_status", {})
    failed = row.setdefault("failed_hard", [])
    unknown = row.setdefault("unknown_hard", [])

    # Keep stale/missing extension telemetry conservative. This is not evidence
    # of over-extension and is never fabricated as PASS.
    if telemetry != "LIVE":
        hard["CUMULATIVE_EXTENSION_GUARD"] = "UNKNOWN"
        if "CUMULATIVE_EXTENSION_GUARD" in failed:
            failed.remove("CUMULATIVE_EXTENSION_GUARD")
        if "CUMULATIVE_EXTENSION_GUARD" not in unknown:
            unknown.append("CUMULATIVE_EXTENSION_GUARD")
        ext["adaptive_extension_state"] = "UNKNOWN"
        ext["adaptive_extension_pass"] = None
        ext["adaptive_extension_reason"] = f"TELEMETRY_{telemetry}"
        row["extension_guard"] = ext
        row["extension_risk_state"] = "UNKNOWN"
        stats["UNKNOWN"] += 1
        last_rows[symbol] = {
            "state": "UNKNOWN",
            "pass": None,
            "run_pct": None,
            "green_limit": None,
            "red_limit": None,
        }
        return row

    sd = app.structure.get(symbol) or {}
    price = f(row.get("price"), f(sd.get("price")))
    atr1 = f(sd.get("atr14_1h"))
    atr4 = f(sd.get("atr14_4h"))
    atr_ref = atr1 if atr1 > 0 else atr4
    atr_pct = (atr_ref / price * 100.0) if price > 0 and atr_ref > 0 else 0.0

    # If structure is not ready yet, retain the legacy decision rather than
    # manufacturing volatility information.
    if atr_pct <= 0:
        legacy_pass = ext.get("extension_guard_pass") is True
        hard["CUMULATIVE_EXTENSION_GUARD"] = "PASS" if legacy_pass else "FAIL"
        ext["adaptive_extension_state"] = "LEGACY_FALLBACK"
        ext["adaptive_extension_pass"] = legacy_pass
        ext["adaptive_extension_reason"] = "ATR_STRUCTURE_NOT_READY_USE_LEGACY"
        row["extension_guard"] = ext
        row["extension_risk_state"] = "LEGACY_FALLBACK"
        stats["LEGACY_FALLBACK"] += 1
        return row

    change24 = max(0.0, f(ext.get("change_24h_pct")))
    from_low = max(0.0, f(ext.get("extension_from_24h_low_pct")))
    pullback = max(0.0, f(ext.get("pullback_from_24h_high_pct")))

    # A single low wick can exaggerate from-low extension, so only 65% of that
    # measure competes with the 24h rolling change.
    effective_run = max(change24, from_low * 0.65)

    green_limit = clamp(
        GREEN_BASE_PCT + GREEN_ATR_MULT * atr_pct,
        GREEN_MIN_PCT,
        GREEN_MAX_PCT,
    )
    red_limit = clamp(
        RED_BASE_PCT + RED_ATR_MULT * atr_pct,
        RED_MIN_PCT,
        RED_MAX_PCT,
    )

    ma50_atr = max(0.0, f(sd.get("distance_ema50_atr")))
    pullback_atr = pullback / max(atr_pct, 0.10)
    anti_chase = bool(sd.get("anti_chase"))

    layers = row.get("layer_results") or {}
    micro_ready = bool(row.get("micro_collection_ready", row.get("micro_ready")))
    flow_book = bool(
        layers.get("FLOW_LAYER")
        and layers.get("ORDER_BOOK_LAYER")
        and micro_ready
    )
    structure_layer = bool(
        layers.get("MA_STRUCTURE_LAYER")
        or layers.get("VWAP_LAYER")
        or sd.get("ma_reclaim_regime")
        or sd.get("structural_support")
    )

    active_setup = str(row.get("active_setup") or "")
    phase5 = str(row.get("mtf_5m_phase") or "")
    sweep_state = str(row.get("sweep_state") or "")
    entry_status = str(row.get("entry_status") or row.get("entry_trigger_status") or "")

    structural_reset = bool(
        row.get("new_base_reset")
        or ext.get("reset_reentry")
        or sd.get("ma_reclaim_regime")
        or phase5 in {"RETEST", "BREAKOUT_HOLD", "CONTINUATION"}
        or sweep_state == "SWEEP_RECLAIMED"
        or active_setup in {
            "BREAKOUT_RETEST_CONTINUATION",
            "TREND_MA_PULLBACK",
            "LIQUIDITY_SWEEP_REVERSAL",
            "RESET_REENTRY",
        }
    )

    already_above_trigger = "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER" in entry_status

    # GREEN = normal extension for the coin's own volatility and not stretched
    # from EMA50. This should behave like the old PASS path.
    green = bool(
        effective_run <= green_limit
        and ma50_atr <= GREEN_MA50_ATR
        and not anti_chase
        and not already_above_trigger
    )

    # RED = genuinely severe chase: a large volatility-adjusted run, far above
    # EMA50, without enough pullback/reset evidence. RED stays a hard block.
    red = bool(
        effective_run >= red_limit
        and ma50_atr > RED_MA50_ATR
        and pullback_atr < AMBER_PULLBACK_ATR
        and not structural_reset
    )

    # AMBER may pass only when extension has been "paid for" by a structural
    # reset/retest plus live flow+book confirmation. This increases BUY
    # frequency by removing redundant binary blocking, not by removing safety.
    amber_compensated = bool(
        structural_reset
        and flow_book
        and structure_layer
        and not already_above_trigger
        and (ma50_atr <= RED_MA50_ATR or pullback_atr >= AMBER_PULLBACK_ATR)
    )

    if green:
        state = "GREEN"
        guard_pass = True
        reason = "VOLATILITY_ADJUSTED_EXTENSION_CLEAR"
    elif red:
        state = "RED"
        guard_pass = False
        reason = "SEVERE_EXTENSION_NO_RESET"
    else:
        state = "AMBER"
        guard_pass = amber_compensated
        reason = (
            "AMBER_COMPENSATED_BY_RETEST_FLOW_BOOK"
            if guard_pass
            else "AMBER_REQUIRES_RETEST_PLUS_FLOW_BOOK"
        )

    legacy_pass = ext.get("extension_guard_pass")
    legacy_reason = ext.get("extension_guard_reason")
    ext.update({
        "legacy_extension_guard_pass": legacy_pass,
        "legacy_extension_guard_reason": legacy_reason,
        "adaptive_extension_version": VERSION,
        "adaptive_extension_state": state,
        "adaptive_extension_pass": guard_pass,
        "adaptive_extension_reason": reason,
        "adaptive_effective_run_pct": round(effective_run, 3),
        "adaptive_atr_pct": round(atr_pct, 4),
        "adaptive_green_limit_pct": round(green_limit, 3),
        "adaptive_red_limit_pct": round(red_limit, 3),
        "adaptive_ma50_distance_atr": round(ma50_atr, 3),
        "adaptive_pullback_atr": round(pullback_atr, 3),
        "adaptive_structural_reset": structural_reset,
        "adaptive_flow_book_confirmed": flow_book,
        "adaptive_structure_confirmed": structure_layer,
        "adaptive_already_above_trigger": already_above_trigger,
        "extension_guard_pass": guard_pass,
        "extension_guard_reason": reason,
    })

    row["extension_guard"] = ext
    row["extension_risk_state"] = state
    row["extension_guard_version"] = VERSION

    hard["CUMULATIVE_EXTENSION_GUARD"] = "PASS" if guard_pass else "FAIL"
    if "CUMULATIVE_EXTENSION_GUARD" in unknown:
        unknown.remove("CUMULATIVE_EXTENSION_GUARD")
    if guard_pass:
        while "CUMULATIVE_EXTENSION_GUARD" in failed:
            failed.remove("CUMULATIVE_EXTENSION_GUARD")
    elif "CUMULATIVE_EXTENSION_GUARD" not in failed:
        failed.append("CUMULATIVE_EXTENSION_GUARD")

    stats[state] += 1
    stats[f"{state}_{'PASS' if guard_pass else 'BLOCK'}"] += 1
    if legacy_pass is False and guard_pass:
        stats["UNLOCKED_FROM_LEGACY_FAIL"] += 1

    last_rows[symbol] = {
        "state": state,
        "pass": guard_pass,
        "run_pct": round(effective_run, 3),
        "atr_pct": round(atr_pct, 4),
        "green_limit": round(green_limit, 3),
        "red_limit": round(red_limit, 3),
        "ma50_atr": round(ma50_atr, 3),
        "pullback_atr": round(pullback_atr, 3),
        "reset": structural_reset,
        "flow_book": flow_book,
        "reason": reason,
    }
    return row


def adaptive_pre_multi_evaluate(symbol):
    row = _legacy_pre_multi_evaluate(symbol)
    if not row:
        return row
    try:
        return _adaptive_extension(symbol, row)
    except Exception as exc:
        stats["ERROR"] += 1
        row.setdefault("extension_guard", {})["adaptive_extension_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
        # Fail closed: if the new evaluator errors, preserve the legacy guard.
        return row


# V10.20's evaluate_multi_regime reads this variable at runtime. Patching here
# means every subsequent V10.21/V11 wrapper sees the upgraded hard-gate result.
v120._old_evaluate = adaptive_pre_multi_evaluate

base.VERSION = VERSION
base.v11.VERSION = VERSION
base.v11.base.VERSION = VERSION
base.v11.scanner.VERSION = VERSION
base.v11.scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v11/{VERSION}"


async def extension_health_loop():
    while True:
        await asyncio.sleep(base.v11.V11_PRINT_SECONDS)
        counts = Counter()
        unlocked = []
        for symbol, item in list(last_rows.items()):
            counts[item.get("state", "UNKNOWN")] += 1
            if item.get("pass") and item.get("state") == "AMBER":
                unlocked.append((symbol, item))
        unlocked.sort(
            key=lambda x: (
                f(x[1].get("run_pct")),
                -f(x[1].get("ma50_atr")),
            ),
            reverse=True,
        )
        print(
            f"Ψ-V11.0.2 EXTENSION green={counts['GREEN']} "
            f"amber={counts['AMBER']} red={counts['RED']} "
            f"unknown={counts['UNKNOWN']} fallback={counts['LEGACY_FALLBACK']} "
            f"amberPass={sum(1 for _, x in last_rows.items() if x.get('state') == 'AMBER' and x.get('pass'))} "
            f"legacyUnlocked={stats.get('UNLOCKED_FROM_LEGACY_FAIL', 0)} "
            f"errors={stats.get('ERROR', 0)}",
            flush=True,
        )
        for idx, (symbol, item) in enumerate(unlocked[:5], 1):
            print(
                f"EX{idx:02d}. {symbol:<14} state=AMBER pass=YES "
                f"run={f(item.get('run_pct')):.2f}% "
                f"atr={f(item.get('atr_pct')):.2f}% "
                f"limits={f(item.get('green_limit')):.2f}/{f(item.get('red_limit')):.2f}% "
                f"ma50={f(item.get('ma50_atr')):.2f}ATR "
                f"pullback={f(item.get('pullback_atr')):.2f}ATR "
                f"reset={'YES' if item.get('reset') else 'NO'} "
                f"flowBook={'YES' if item.get('flow_book') else 'NO'}",
                flush=True,
            )


async def main():
    print(
        "[v11.0.2] Adaptive CUMULATIVE_EXTENSION_GUARD active: "
        "GREEN=volatility-adjusted clear; AMBER requires structural reset/retest "
        "+ live flow/book confirmation; RED remains a hard no-chase block.",
        flush=True,
    )
    print(
        f"Ψ-V11.0.2 EXTENSION_CONFIG green={GREEN_BASE_PCT}+"
        f"{GREEN_ATR_MULT}*ATR% clipped[{GREEN_MIN_PCT},{GREEN_MAX_PCT}] "
        f"red={RED_BASE_PCT}+{RED_ATR_MULT}*ATR% "
        f"clipped[{RED_MIN_PCT},{RED_MAX_PCT}] "
        f"greenMA50<={GREEN_MA50_ATR}ATR redMA50>{RED_MA50_ATR}ATR "
        f"amberPullback>={AMBER_PULLBACK_ATR}ATR",
        flush=True,
    )
    await asyncio.gather(
        base.main(),
        extension_health_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        base.v11.persist_outcomes(force=True)
        print("Psi-V11.0.2 stopped", flush=True)
