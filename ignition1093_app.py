import asyncio
import time

import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8
import ignition1081_app as v81
import ignition109_app as v9
import ignition1091_app as core
import ignition1092_app as v92

VERSION = "10.9-latent-cluster-extension-guard"
_original_metric = v92.metric
# Preserve the repaired V10.8.2 feed-aware extension evaluator before this
# module rebinds v81.evaluate. The previous V10.9.3 wrapper reimplemented the
# old age-only logic and silently turned stale/unknown telemetry back into FAIL.
_extension_evaluate_fixed = v81.evaluate

core.VERSION = VERSION
v92.VERSION = VERSION
v9.VERSION = VERSION
v81.VERSION = VERSION
v8.VERSION = VERSION
v7.VERSION = VERSION
base.VERSION = VERSION
app.USER_AGENT = "psi-v10-live-scanner/10.9-latent-cluster-extension-guard"


def _count_window(trades, now_ms, lo_s, hi_s=0):
    lower = now_ms - lo_s * 1000
    upper = now_ms - hi_s * 1000
    return sum(1 for x in trades if lower <= x[0] < upper)


def _ratio(cur_count, cur_seconds, prev_count, prev_seconds):
    cur_rate = cur_count / max(cur_seconds, 1.0)
    prev_rate = prev_count / max(prev_seconds, 1.0)
    if cur_rate <= 0:
        return 0.0
    if prev_rate <= 0:
        return min(12.0, 1.0 + cur_count)
    return min(50.0, cur_rate / prev_rate)


def _aggtrade_accel(symbol):
    st = app.micro_state.get(symbol) or {}
    trades = st.get("trades")
    if not trades:
        return None
    now_ms = app.now_ms()
    live = [x for x in trades if x[0] >= now_ms - 35_000]
    if len(live) < 8:
        return None
    oldest_ms = min(x[0] for x in live)
    history_s = (now_ms - oldest_ms) / 1000.0
    if history_s < 15.0:
        return None

    n5 = _count_window(live, now_ms, 5)
    n_prev10 = _count_window(live, now_ms, 15, 5)
    n15 = _count_window(live, now_ms, 15)
    n_prev15 = _count_window(live, now_ms, 30, 15) if history_s >= 30.0 else 0

    t5x = _ratio(n5, 5, n_prev10, 10)
    t15x = _ratio(n15, 15, n_prev15, 15) if history_s >= 30.0 else None
    return {
        "trade_accel_5": t5x,
        "trade_accel_15": t15x,
        "trades_5s": n5,
        "trades_15s": n15,
        "history_seconds": history_s,
    }


def metric(symbol):
    r = _original_metric(symbol)
    actual = _aggtrade_accel(symbol)
    if actual is None:
        r["trade_accel_5"] = 0.0
        r["trade_accel_15"] = 0.0
        r["trades_5s"] = 0
        r["trade_accel_available"] = False
        r["trade_counter_source"] = "AGGTRADE_AFTER_PROMOTION"
        return r

    old_t5 = float(r.get("trade_accel_5") or 0.0)
    old_t15 = float(r.get("trade_accel_15") or 0.0)
    t5x = float(actual["trade_accel_5"] or 0.0)
    t15_raw = actual["trade_accel_15"]
    t15x = float(t15_raw or 0.0)

    # Remove any contribution that may have come from the unreliable market-wide
    # ticker trade counter, then add only verified aggTrade acceleration.
    score = float(r.get("score") or 0.0)
    score -= max(0.0, min(old_t5 - 1.0, 8.0)) * 6.0
    score -= max(0.0, min(old_t15 - 1.0, 6.0)) * 3.0
    score += max(0.0, min(t5x - 1.0, 8.0)) * 6.0
    if t15_raw is not None:
        score += max(0.0, min(t15x - 1.0, 6.0)) * 3.0

    r["score"] = round(max(0.0, score), 3)
    r["trade_accel_5"] = round(t5x, 3)
    r["trade_accel_15"] = round(t15x, 3) if t15_raw is not None else 0.0
    r["trades_5s"] = int(actual["trades_5s"])
    r["trades_15s"] = int(actual["trades_15s"])
    r["trade_accel_available"] = True
    r["trade_counter_source"] = "LIVE_AGGTRADE_EVENTS"
    r["trade_history_seconds"] = round(float(actual["history_seconds"]), 1)

    r5 = float(r.get("r5") or 0.0)
    r15 = float(r.get("r15") or 0.0)
    v5x = float(r.get("vol_accel_5") or 0.0)
    v15x = float(r.get("vol_accel_15") or 0.0)
    spread = float(r.get("spread_bps") or 0.0)
    normal_accel = (
        r5 >= 0.10 or r15 >= 0.20 or v5x >= 1.6 or t5x >= 1.6
        or (v15x >= 1.4 and t15_raw is not None and t15x >= 1.4)
    )
    r["trigger"] = bool(
        v7._directional(symbol)
        and spread <= 30
        and (normal_accel or r.get("latent_ignition") or r.get("clustered_ignition"))
        and float(r.get("score") or 0.0) >= base.TRIGGER_SCORE
    )
    return r


def extension_evaluate(symbol):
    """Use the repaired feed-aware extension guard without reimplementing it.

    This deliberately delegates to V10.8.2 so future fixes to extension telemetry
    have one source of truth. UNKNOWN telemetry remains UNKNOWN (never fabricated
    as PASS and never mislabeled as an over-extension FAIL).
    """
    row = _extension_evaluate_fixed(symbol)
    if row:
        ext = row.get("extension_guard") or {}
        ext["extension_evaluator"] = "V10.8.2_FEED_AWARE_SINGLE_SOURCE"
        row["extension_guard"] = ext
    return row


v9.metric = metric
base.metric = metric
v7.ignition_metric = metric
v81.evaluate = extension_evaluate

app.evaluate_symbol = core.evaluate
app.health = core.health
app.scan_endpoint = core.scan
app.ranked_results = s.results
v7.rapid_websocket_loop = v9.persistent_rapid_websocket_loop
v7.print_loop = v9.print_loop
q.print_loop = v9.print_loop
s.print_loop = v9.print_loop
base.ticker_loop = v92.ticker_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.9 ACTIVE — latent ignition + burst clustering + rolling pressure + "
            "verified aggTrade acceleration + persistent rapid subscriptions + feed-aware extension guard",
            flush=True,
        )
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.9 stopped", flush=True)
