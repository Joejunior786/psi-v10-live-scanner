import asyncio
import os
import time

import ignition1212_entry as base

scanner, q, app = base.scanner, base.q, base.app
core1211 = base.core
core121 = core1211.core
pump_mod = core121.pump_mod

VERSION = "10.21.3-pump-pre-latch"
PUMP_PRE_LATCH_SECONDS = float(os.environ.get("PSI_PUMP_PRE_LATCH_SECONDS", "90"))
PUMP_PRE_LATCH_MAX = int(os.environ.get("PSI_PUMP_PRE_LATCH_MAX", "25"))

_old_pump_signature = pump_mod.pump_signature
pump_pre_history = {}


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if x == x and abs(x) != float("inf") else d


def _drop_reason(ps):
    state = str((ps or {}).get("pump_state") or "NONE")
    if state == "PUMP-ARMED":
        return "PROMOTED_TO_PUMP_ARMED"

    reasons = []
    if f((ps or {}).get("pump_signature_score")) < f(getattr(core121, "PUMP_PREARM_MIN_SCORE", 55.0), 55.0):
        reasons.append("SCORE_BELOW_PRE")
    if int((ps or {}).get("layers_118") or 0) < int(getattr(core121, "PUMP_PREARM_MIN_LAYERS", 5)):
        reasons.append("LAYERS_BELOW_PRE")
    if not bool((ps or {}).get("pump_prearmed_acceleration_121")):
        reasons.append("ACCELERATION_LOST")
    if not bool((ps or {}).get("pump_prearmed_fresh_structure_121")):
        reasons.append("STRUCTURE_STALE")
    return "+".join(reasons) if reasons else "CONDITIONS_CHANGED"


def _snapshot(sym, ps, now):
    row = q.latest.get(sym) or {}
    return {
        "symbol": sym,
        "current_state": str((ps or {}).get("pump_state") or "NONE"),
        "current_sig": f((ps or {}).get("pump_signature_score")),
        "current_rapid": f((ps or {}).get("rapid_score")),
        "current_peak5m": f((ps or {}).get("rapid_peak_5m")),
        "current_layers": int((ps or {}).get("layers_118") or 0),
        "current_dv30": f((ps or {}).get("local_dv30_per_min")),
        "current_local_dist": (ps or {}).get("local_distance_60s_pct"),
        "current_struct_dist": row.get("breakout_distance_pct"),
        "micro_ready": bool((ps or {}).get("micro_ready_118")),
        "exec_pass": bool((ps or {}).get("exec_pass_118")),
        "structure_age": f((ps or {}).get("structure_age_seconds_121"), 999999.0),
        "sampled_at": now,
    }


def pump_signature_1213(sym):
    ps = dict(_old_pump_signature(sym) or {})
    now = time.time()
    is_pre = str(ps.get("pump_state") or "") == "PUMP-PRE-ARMED"
    hist = pump_pre_history.get(sym)

    if hist is not None and now - f(hist.get("last_seen")) > PUMP_PRE_LATCH_SECONDS:
        pump_pre_history.pop(sym, None)
        hist = None

    snap = _snapshot(sym, ps, now)

    if is_pre:
        if hist is None:
            hist = {
                "symbol": sym,
                "first_seen": now,
                "last_seen": now,
                "cooled_at": None,
                "live": True,
                "trigger_count": 1,
                "peak_sig": snap["current_sig"],
                "peak_rapid": snap["current_rapid"],
                "peak_rapid5m": snap["current_peak5m"],
                "peak_layers": snap["current_layers"],
                "peak_dv30": snap["current_dv30"],
                "drop_reason": "-",
            }
            pump_pre_history[sym] = hist
        else:
            if not hist.get("live"):
                hist["trigger_count"] = int(hist.get("trigger_count") or 0) + 1
            hist["last_seen"] = now
            hist["cooled_at"] = None
            hist["live"] = True
            hist["drop_reason"] = "-"

        hist["peak_sig"] = max(f(hist.get("peak_sig")), snap["current_sig"])
        hist["peak_rapid"] = max(f(hist.get("peak_rapid")), snap["current_rapid"])
        hist["peak_rapid5m"] = max(f(hist.get("peak_rapid5m")), snap["current_peak5m"])
        hist["peak_layers"] = max(int(hist.get("peak_layers") or 0), snap["current_layers"])
        hist["peak_dv30"] = max(f(hist.get("peak_dv30")), snap["current_dv30"])
        hist.update(snap)
        hist["live"] = True
        hist["last_seen"] = now
    elif hist is not None:
        if hist.get("live"):
            hist["cooled_at"] = now
        hist["live"] = False
        hist["drop_reason"] = _drop_reason(ps)
        hist.update(snap)
        hist["live"] = False

    ps["pump_pre_latched_1213"] = bool(hist is not None and now - f(hist.get("last_seen")) <= PUMP_PRE_LATCH_SECONDS)
    if hist is not None:
        ps["pump_pre_latch_live_1213"] = bool(hist.get("live"))
        ps["pump_pre_latch_age_1213"] = round(max(0.0, now - f(hist.get("first_seen"))), 2)
        ps["pump_pre_latch_since_last_1213"] = round(max(0.0, now - f(hist.get("last_seen"))), 2)
        ps["pump_pre_latch_peak_sig_1213"] = round(f(hist.get("peak_sig")), 2)
        ps["pump_pre_latch_peak_rapid_1213"] = round(f(hist.get("peak_rapid")), 2)
        ps["pump_pre_latch_peak_layers_1213"] = int(hist.get("peak_layers") or 0)
        ps["pump_pre_latch_drop_reason_1213"] = str(hist.get("drop_reason") or "-")
    return ps


pump_mod.pump_signature = pump_signature_1213
core121.pump_pre_history_1213 = pump_pre_history


def _prune(now=None):
    now = now or time.time()
    expired = [
        sym for sym, x in list(pump_pre_history.items())
        if now - f((x or {}).get("last_seen")) > PUMP_PRE_LATCH_SECONDS
    ]
    for sym in expired:
        pump_pre_history.pop(sym, None)


def recent_pump_pre_rows():
    now = time.time()
    _prune(now)
    rows = []
    for sym, x in list(pump_pre_history.items()):
        y = dict(x)
        y["since_last"] = max(0.0, now - f(y.get("last_seen")))
        y["age"] = max(0.0, now - f(y.get("first_seen")))
        y["ttl_left"] = max(0.0, PUMP_PRE_LATCH_SECONDS - y["since_last"])
        rows.append((sym, y))
    rows.sort(
        key=lambda z: (
            1 if z[1].get("live") else 0,
            f(z[1].get("peak_sig")),
            f(z[1].get("peak_rapid")),
            -f(z[1].get("since_last")),
        ),
        reverse=True,
    )
    return rows


async def pump_pre_latch_print_loop():
    while True:
        await asyncio.sleep(float(getattr(app, "PRINT_SECONDS", 30)))
        try:
            rows = recent_pump_pre_rows()
            live = sum(bool(x.get("live")) for _, x in rows)
            recent = len(rows) - live
            print(
                f"Ψ-V10.21.3 PUMP-PRE LATCH live={live} recent={recent} "
                f"total={len(rows)} ttl={PUMP_PRE_LATCH_SECONDS:.0f}s",
                flush=True,
            )
            for i, (sym, x) in enumerate(rows[: max(1, PUMP_PRE_LATCH_MAX)], 1):
                state = str(x.get("current_state") or "NONE")
                if state == "PUMP-ARMED":
                    status = "PROMOTED"
                else:
                    status = "LIVE" if x.get("live") else "RECENT"
                struct_dist = x.get("current_struct_dist")
                local_dist = x.get("current_local_dist")
                sdist = "-" if struct_dist is None else f"{f(struct_dist):+.3f}%"
                ldist = "-" if local_dist is None else f"{f(local_dist):+.3f}%"
                print(
                    f"PPL{i:02d}. {sym:14s} status={status:8s} "
                    f"peakSig={f(x.get('peak_sig')):5.1f} currentSig={f(x.get('current_sig')):5.1f} "
                    f"peakRapid={f(x.get('peak_rapid')):5.1f} peak5m={f(x.get('peak_rapid5m')):5.1f} "
                    f"layers={int(x.get('current_layers') or 0)}/6 peakLayers={int(x.get('peak_layers') or 0)}/6 "
                    f"micro={'READY' if x.get('micro_ready') else 'WAIT'} "
                    f"exec={'PASS_ALL' if x.get('exec_pass') else 'BLOCKED'} "
                    f"localDist={ldist} structDist={sdist} dv30={f(x.get('current_dv30')):+.3f}/m "
                    f"age={f(x.get('age')):.0f}s sinceLast={f(x.get('since_last')):.0f}s "
                    f"ttlLeft={f(x.get('ttl_left')):.0f}s triggers={int(x.get('trigger_count') or 0)} "
                    f"drop={x.get('drop_reason') or '-'}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-V10.21.3 PUMP_PRE_LATCH_ERROR {type(exc).__name__}: {exc}", flush=True)


base.VERSION = VERSION
base.b121.VERSION = VERSION
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        f"[v10.21.3] 90-second Pump-Pre history/latch active: "
        f"sampler={getattr(pump_mod, 'PUMP_SAMPLE_SECONDS', 5.0):.0f}s "
        f"ttl={PUMP_PRE_LATCH_SECONDS:.0f}s maxRows={PUMP_PRE_LATCH_MAX}; "
        "PUMP-ARMED and BUY NOW thresholds unchanged",
        flush=True,
    )
    await asyncio.gather(base.main(), pump_pre_latch_print_loop())


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Psi-V10.21.3 stopped", flush=True)
