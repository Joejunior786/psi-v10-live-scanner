import asyncio
import json
import math
import os
import statistics
import time
from collections import defaultdict, deque

import psi_v11_3_3_entry as base

app = base.app
q = base.q
scanner = base.scanner
expected = base.base

VERSION = "11.0.3.6-tail-hunter"
TAIL_SAMPLE_SECONDS = 2.0
TAIL_BOARD_SECONDS = 30.0
TAIL_SAVE_SECONDS = 30.0
TAIL_OPEN_COOLDOWN = 900.0
TAIL_MAX_PENDING = 1200
TAIL_MAX_RESOLVED = 6000
TAIL_PATH = os.environ.get("PSI_TAIL_HUNTER_STATE", "/data/psi_tail_hunter_state.json")
TAIL_TARGETS = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0)
TAIL_HORIZONS = {"4h": 4 * 3600.0, "24h": 24 * 3600.0, "72h": 72 * 3600.0}
PRIOR_STRENGTH = 12.0
ONE_SIDED_Z = 1.2815515655446004
MAX_NEIGHBORS = 180
MIN_SIMILARITY = 0.20
MIN_NUMERIC_LABELS = 8
ACTIVE_LABELS = 60
ACTIVE_EFFECTIVE_N = 12.0

tail_pending = []
tail_resolved = deque(maxlen=TAIL_MAX_RESOLVED)
tail_last_open = defaultdict(float)
tail_stats = defaultdict(int)
tail_last_save = 0.0


def f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if math.isfinite(x) else default


def clamp(value, lo, hi):
    return max(lo, min(hi, value))


def _optional_float(value):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _current_price(sym):
    try:
        return f(app.current_symbol_price(sym), f((q.latest.get(sym) or {}).get("price")))
    except Exception:
        return f((q.latest.get(sym) or {}).get("price"))


def _current_features(sym, row):
    try:
        feat = dict(expected._current_features(sym))
    except Exception:
        feat = {}
    feat.update({
        "pinpoint_setup": row.get("pinpoint_setup") or "UNKNOWN",
        "pinpoint_tape": f(row.get("pinpoint_live_tape_score"), 50.0),
        "pinpoint_risk_pct": f(row.get("pinpoint_risk_pct"), 0.0),
        "pinpoint_buy": bool(row.get("pinpoint_buy")),
        "pinpoint_state": row.get("pinpoint_state") or "UNKNOWN",
        "pinpoint_entry_status": row.get("pinpoint_entry_status") or "UNKNOWN",
    })
    return feat


def _load_state():
    if not os.path.exists(TAIL_PATH):
        return
    try:
        with open(TAIL_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        pending = data.get("pending") or []
        resolved = data.get("resolved") or []
        tail_pending[:] = [x for x in pending if isinstance(x, dict)][-TAIL_MAX_PENDING:]
        tail_resolved.clear()
        tail_resolved.extend([x for x in resolved if isinstance(x, dict)][-TAIL_MAX_RESOLVED:])
        for item in list(tail_pending) + list(tail_resolved):
            key = (str(item.get("symbol") or ""), str(item.get("setup") or "UNKNOWN"))
            tail_last_open[key] = max(tail_last_open[key], f(item.get("opened")))
        tail_stats["state_loaded"] = 1
    except Exception as exc:
        tail_stats["load_errors"] += 1
        print(f"Ψ-TAIL-HUNTER LOAD_ERROR {type(exc).__name__}: {exc}", flush=True)


def _save_state(force=False):
    global tail_last_save
    now = time.time()
    if not force and now - tail_last_save < TAIL_SAVE_SECONDS:
        return
    try:
        folder = os.path.dirname(TAIL_PATH) or "."
        os.makedirs(folder, exist_ok=True)
        tmp = TAIL_PATH + ".tmp"
        payload = {
            "version": VERSION,
            "saved": now,
            "pending": tail_pending[-TAIL_MAX_PENDING:],
            "resolved": list(tail_resolved)[-TAIL_MAX_RESOLVED:],
        }
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, separators=(",", ":"), ensure_ascii=False)
        os.replace(tmp, TAIL_PATH)
        tail_last_save = now
    except Exception as exc:
        tail_stats["save_errors"] += 1
        print(f"Ψ-TAIL-HUNTER SAVE_ERROR {type(exc).__name__}: {exc}", flush=True)


def _open_tail_event(sym, row):
    now = time.time()
    setup = str(row.get("pinpoint_setup") or "")
    entry_status = str(row.get("pinpoint_entry_status") or "")
    trigger = f(row.get("pinpoint_trigger"))
    stop = f(row.get("pinpoint_stop"))
    price = _current_price(sym)
    if not setup or entry_status != "PINPOINT_TRIGGERED" or trigger <= 0 or stop <= 0 or price <= 0:
        return
    if stop >= price:
        return
    key = (sym, setup)
    if now - tail_last_open[key] < TAIL_OPEN_COOLDOWN:
        return
    if any(x.get("symbol") == sym and x.get("setup") == setup for x in tail_pending):
        return

    features = _current_features(sym, row)
    event = {
        "id": f"{sym}:{int(now * 1000)}",
        "symbol": sym,
        "setup": setup,
        "opened": now,
        "entry": price,
        "trigger": trigger,
        "stop": stop,
        "risk_pct": (price - stop) / price * 100.0,
        "pinpoint_buy_at_open": bool(row.get("pinpoint_buy")),
        "formal_at_open": row.get("formal_state") or row.get("state"),
        "tape_at_open": f(row.get("pinpoint_live_tape_score")),
        "features": features,
        "hit_ts": {},
        "horizon_labels": {},
        "horizon_returns": {},
        "max_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "last_price": price,
        "status": "OPEN",
    }
    tail_pending.append(event)
    if len(tail_pending) > TAIL_MAX_PENDING:
        del tail_pending[:-TAIL_MAX_PENDING]
    tail_last_open[key] = now
    tail_stats["opened"] += 1


def _finalize_horizon_labels(event, now, current_return):
    opened = f(event.get("opened"))
    age = max(0.0, now - opened)
    hit_ts = event.get("hit_ts") or {}
    stop_ts = f(event.get("stop_ts"), 0.0)
    for label, seconds in TAIL_HORIZONS.items():
        if label in (event.get("horizon_labels") or {}):
            continue
        can_finalize = age >= seconds or stop_ts > 0 or str(event.get("status")) == "TP50"
        if not can_finalize:
            continue
        labels = {}
        for target in TAIL_TARGETS:
            ts = f(hit_ts.get(str(int(target))), 0.0)
            labels[str(int(target))] = bool(ts > 0 and ts - opened <= seconds and (stop_ts <= 0 or ts <= stop_ts))
        event.setdefault("horizon_labels", {})[label] = labels
        if stop_ts > 0 and stop_ts - opened <= seconds:
            event.setdefault("horizon_returns", {})[label] = round((f(event.get("stop")) / f(event.get("entry")) - 1.0) * 100.0, 4)
        elif age >= seconds:
            event.setdefault("horizon_returns", {})[label] = round(current_return, 4)
        elif str(event.get("status")) == "TP50":
            event.setdefault("horizon_returns", {})[label] = round(current_return, 4)


def _update_tail_events():
    now = time.time()
    keep = []
    for event in tail_pending:
        sym = str(event.get("symbol") or "")
        price = _current_price(sym)
        entry = f(event.get("entry"))
        stop = f(event.get("stop"))
        if price <= 0 or entry <= 0 or stop <= 0:
            keep.append(event)
            continue
        ret = (price / entry - 1.0) * 100.0
        event["last_price"] = price
        event["max_return_pct"] = max(f(event.get("max_return_pct")), ret)
        event["max_drawdown_pct"] = min(f(event.get("max_drawdown_pct")), ret)

        for target in TAIL_TARGETS:
            key = str(int(target))
            if key not in event.setdefault("hit_ts", {}) and ret >= target:
                event["hit_ts"][key] = now

        closed = False
        if price <= stop:
            event["status"] = "STOP"
            event["stop_ts"] = now
            event["closed"] = now
            closed = True
        elif ret >= 50.0:
            event["status"] = "TP50"
            event["closed"] = now
            closed = True
        elif now - f(event.get("opened")) >= TAIL_HORIZONS["72h"]:
            event["status"] = "HORIZON_72H"
            event["closed"] = now
            closed = True

        _finalize_horizon_labels(event, now, ret)
        if closed:
            _finalize_horizon_labels(event, now, ret)
            tail_resolved.append(dict(event))
            tail_stats["resolved"] += 1
        else:
            keep.append(event)
    tail_pending[:] = keep[-TAIL_MAX_PENDING:]


def _cat_sim(a, b):
    aa, bb = str(a or "UNKNOWN"), str(b or "UNKNOWN")
    if aa == "UNKNOWN" or bb == "UNKNOWN":
        return 0.35
    return 1.0 if aa == bb else 0.0


def _num_sim(a, b, scale):
    aa, bb = _optional_float(a), _optional_float(b)
    if aa is None or bb is None:
        return 0.40
    return clamp(1.0 - abs(aa - bb) / max(scale, 1e-9), 0.0, 1.0)


def _similarity(current, event):
    hist = event.get("features") or {}
    return clamp(
        0.28 * _cat_sim(current.get("pinpoint_setup"), hist.get("pinpoint_setup"))
        + 0.22 * _num_sim(current.get("pinpoint_tape"), hist.get("pinpoint_tape"), 25.0)
        + 0.10 * _num_sim(current.get("pinpoint_risk_pct"), hist.get("pinpoint_risk_pct"), 2.0)
        + 0.12 * _cat_sim(current.get("regime119"), hist.get("regime119"))
        + 0.10 * _cat_sim(current.get("mtf119"), hist.get("mtf119"))
        + 0.10 * _num_sim(current.get("flow_score119"), hist.get("flow_score119"), 30.0)
        + 0.05 * _num_sim(current.get("pump_score"), hist.get("pump_score"), 30.0)
        + 0.03 * _cat_sim(current.get("spot_futures119"), hist.get("spot_futures119")),
        0.0, 1.0,
    )


def _label_rows(horizon):
    rows = []
    for event in list(tail_resolved) + list(tail_pending):
        labels = (event.get("horizon_labels") or {}).get(horizon)
        if isinstance(labels, dict):
            rows.append(event)
    return rows


def _neighbors(current, rows):
    scored = []
    for event in rows:
        sim = _similarity(current, event)
        if sim >= MIN_SIMILARITY:
            scored.append((sim, sim * sim, event))
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[:MAX_NEIGHBORS]


def _effective_n(weights):
    sw = sum(weights)
    sw2 = sum(w * w for w in weights)
    return (sw * sw / sw2) if sw2 > 0 else 0.0


def _posterior(current, horizon, target):
    rows = _label_rows(horizon)
    if len(rows) < MIN_NUMERIC_LABELS:
        return {"mean": None, "lower": None, "n": len(rows), "effective_n": 0.0, "status": "WARMING"}

    key = str(int(target))
    hits = sum(bool((x.get("horizon_labels") or {}).get(horizon, {}).get(key)) for x in rows)
    global_rate = (hits + 1.0) / (len(rows) + 2.0)
    alpha = 1.0 + PRIOR_STRENGTH * global_rate
    beta = 1.0 + PRIOR_STRENGTH * (1.0 - global_rate)
    neighbors = _neighbors(current, rows)
    weights = []
    for _, weight, event in neighbors:
        weights.append(weight)
        if bool((event.get("horizon_labels") or {}).get(horizon, {}).get(key)):
            alpha += weight
        else:
            beta += weight
    total = alpha + beta
    mean = alpha / total
    variance = (alpha * beta) / ((total * total) * (total + 1.0))
    lower = clamp(mean - ONE_SIDED_Z * math.sqrt(max(variance, 0.0)), 0.0, 1.0)
    neff = _effective_n(weights)
    status = "ACTIVE" if len(rows) >= ACTIVE_LABELS and neff >= ACTIVE_EFFECTIVE_N else "CALIBRATING"
    return {"mean": mean, "lower": lower, "n": len(rows), "effective_n": neff, "global": global_rate, "status": status}


def _expected_return(current, horizon):
    rows = [x for x in _label_rows(horizon) if _optional_float((x.get("horizon_returns") or {}).get(horizon)) is not None]
    if len(rows) < MIN_NUMERIC_LABELS:
        return {"mean": None, "lower": None, "n": len(rows), "effective_n": 0.0}
    vals = [f((x.get("horizon_returns") or {}).get(horizon)) for x in rows]
    global_mean = statistics.mean(vals) if vals else 0.0
    neighbors = _neighbors(current, rows)
    local = []
    for _, weight, event in neighbors:
        value = _optional_float((event.get("horizon_returns") or {}).get(horizon))
        if value is not None:
            local.append((weight, value))
    sw = sum(w for w, _ in local)
    if sw <= 0:
        return {"mean": global_mean, "lower": min(global_mean, 0.0), "n": len(rows), "effective_n": 0.0}
    mean = (PRIOR_STRENGTH * global_mean + sum(w * value for w, value in local)) / (PRIOR_STRENGTH + sw)
    neff = _effective_n([w for w, _ in local])
    local_mean = sum(w * value for w, value in local) / sw
    var = sum(w * (value - local_mean) ** 2 for w, value in local) / sw
    lower = mean - ONE_SIDED_Z * math.sqrt(max(var, 0.0) / max(neff, 1.0))
    return {"mean": mean, "lower": lower, "n": len(rows), "effective_n": neff}


def _eta_target(current, horizon, target):
    rows = _label_rows(horizon)
    neighbors = _neighbors(current, rows)
    key = str(int(target))
    times = []
    for _, weight, event in neighbors:
        ts = f((event.get("hit_ts") or {}).get(key), 0.0)
        opened = f(event.get("opened"))
        if ts > opened and bool((event.get("horizon_labels") or {}).get(horizon, {}).get(key)):
            times.append((weight, (ts - opened) / 60.0))
    if len(times) < 5:
        return None
    times.sort(key=lambda x: x[1])
    total = sum(w for w, _ in times)
    acc = 0.0
    for weight, value in times:
        acc += weight
        if acc >= total / 2.0:
            return round(value, 1)
    return round(times[-1][1], 1)


def _legacy_forecast(sym, row):
    trigger = f(row.get("pinpoint_trigger"))
    stop = f(row.get("pinpoint_stop"))
    if trigger <= 0 or stop <= 0 or stop >= trigger:
        return {}
    try:
        return expected.expected_move_forecast(sym, trigger, stop) or {}
    except Exception:
        return {}


def tail_forecast(sym, row):
    current = _current_features(sym, row)
    forecasts = {}
    for horizon in TAIL_HORIZONS:
        forecasts[horizon] = {}
        for target in TAIL_TARGETS:
            forecasts[horizon][int(target)] = _posterior(current, horizon, target)

    for horizon in TAIL_HORIZONS:
        prev_mean = 1.0
        prev_lower = 1.0
        for target in TAIL_TARGETS:
            item = forecasts[horizon][int(target)]
            if item.get("mean") is not None:
                item["mean"] = min(prev_mean, item["mean"])
                prev_mean = item["mean"]
            if item.get("lower") is not None:
                item["lower"] = min(prev_lower, item["lower"])
                prev_lower = item["lower"]

    er24 = _expected_return(current, "24h")
    risk_pct = max(f(row.get("pinpoint_risk_pct")), 0.05)
    expected_r = None if er24.get("mean") is None else er24["mean"] / risk_pct
    expected_r_lower = None if er24.get("lower") is None else er24["lower"] / risk_pct

    p20_24 = forecasts["24h"][20]
    p30_24 = forecasts["24h"][30]
    p30_72 = forecasts["72h"][30]
    statuses = [p20_24.get("status"), p30_24.get("status"), p30_72.get("status")]
    if statuses and all(x == "ACTIVE" for x in statuses):
        data_status = "ACTIVE"
    elif any(x == "CALIBRATING" for x in statuses):
        data_status = "CALIBRATING"
    else:
        data_status = "WARMING"

    legacy = _legacy_forecast(sym, row)
    legacy_p20 = _optional_float(legacy.get("p20"))
    legacy_p20_lower = _optional_float(legacy.get("p20_lower"))
    legacy_r = _optional_float(legacy.get("expected_r"))
    legacy_r_lower = _optional_float(legacy.get("expected_r_lower"))

    if p20_24.get("lower") is not None:
        p20l = p20_24["lower"]
        p30l = p30_72.get("lower") or 0.0
        rl = expected_r_lower if expected_r_lower is not None else 0.0
        ranking_method = "CLEAN_TARGET_BEFORE_STOP"
    else:
        p20l = legacy_p20_lower or 0.0
        p30l = 0.0
        rl = legacy_r_lower or 0.0
        ranking_method = "LEGACY_WEAK_PRIOR"

    tape = f(row.get("pinpoint_live_tape_score"), 0.0) / 100.0
    executable = 1.0 if row.get("pinpoint_buy") else (0.65 if row.get("pinpoint_entry_status") == "PINPOINT_TRIGGERED" else 0.35)
    tail_rank = 38.0 * p20l + 30.0 * p30l + 8.0 * clamp(rl / 5.0, -1.0, 1.0) + 14.0 * tape + 10.0 * executable

    return {
        "version": VERSION,
        "data_status": data_status,
        "ranking_method": ranking_method,
        "p20_24": p20_24,
        "p30_24": p30_24,
        "p30_72": p30_72,
        "p40_72": forecasts["72h"][40],
        "p50_72": forecasts["72h"][50],
        "expected_return_24h": er24,
        "expected_r_24h": expected_r,
        "expected_r_24h_lower": expected_r_lower,
        "eta20_24_minutes": _eta_target(current, "24h", 20.0),
        "eta30_72_minutes": _eta_target(current, "72h", 30.0),
        "tail_rank": round(tail_rank, 3),
        "legacy_p20": legacy_p20,
        "legacy_p20_lower": legacy_p20_lower,
        "legacy_expected_r": legacy_r,
        "legacy_expected_r_lower": legacy_r_lower,
    }


def _candidate_rows():
    rows = []
    for sym, row in list(q.latest.items()):
        if not isinstance(row, dict):
            continue
        setup = row.get("pinpoint_setup")
        pstate = str(row.get("pinpoint_state") or "")
        estatus = str(row.get("pinpoint_entry_status") or "")
        formal = str(row.get("formal_state") or row.get("state") or "")
        if not setup:
            continue
        if pstate not in {"SETUP READY", "PINPOINT ARMED", "BUY NOW"} and estatus not in {"PINPOINT_ARMED", "PINPOINT_TRIGGERED"}:
            continue
        if formal in {"REJECT", "UNKNOWN"} and estatus not in {"PINPOINT_ARMED", "PINPOINT_TRIGGERED"}:
            continue
        fc = tail_forecast(sym, row)
        row["tail_hunter"] = fc
        rows.append((f(fc.get("tail_rank")), 1 if row.get("pinpoint_buy") else 0, f(row.get("pinpoint_live_tape_score")), sym, row, fc))
    rows.sort(reverse=True)
    return rows


async def tail_sampler_loop():
    while True:
        await asyncio.sleep(TAIL_SAMPLE_SECONDS)
        try:
            for sym, row in list(q.latest.items()):
                if isinstance(row, dict):
                    _open_tail_event(sym, row)
            _update_tail_events()
            _save_state()
        except asyncio.CancelledError:
            _save_state(force=True)
            raise
        except Exception as exc:
            tail_stats["sampler_errors"] += 1
            print(f"Ψ-TAIL-HUNTER SAMPLER_ERROR {type(exc).__name__}: {exc}", flush=True)


def _fmt_prob(item):
    if not isinstance(item, dict) or item.get("mean") is None:
        return "-"
    return f"{item['mean']*100:.1f}%[{item['lower']*100:.1f}]"


async def tail_board_loop():
    while True:
        await asyncio.sleep(TAIL_BOARD_SECONDS)
        try:
            rows = _candidate_rows()
            n24 = len(_label_rows("24h"))
            n72 = len(_label_rows("72h"))
            active = sum(1 for _, _, _, _, _, fc in rows if fc.get("data_status") == "ACTIVE")
            print(
                f"Ψ-TAIL-HUNTER BOARD candidates={len(rows)} active={active} pending={len(tail_pending)} "
                f"resolved={len(tail_resolved)} labels24={n24} labels72={n72} definition=TARGET_BEFORE_STOP sampler={TAIL_SAMPLE_SECONDS:.0f}s",
                flush=True,
            )
            for i, (_, _, _, sym, row, fc) in enumerate(rows[:10], 1):
                p20 = fc.get("p20_24") or {}
                p30 = fc.get("p30_72") or {}
                er = fc.get("expected_return_24h") or {}
                er_txt = "-" if er.get("mean") is None else f"{er['mean']:+.2f}%[{er['lower']:+.2f}]"
                r = fc.get("expected_r_24h")
                rl = fc.get("expected_r_24h_lower")
                r_txt = "-" if r is None else f"{r:+.2f}R[{rl:+.2f}]"
                print(
                    f"TH{i:02d}. {sym:<14} state={str(row.get('formal_state') or row.get('state') or '-'):<12} "
                    f"pstate={str(row.get('pinpoint_state') or '-'):<14} setup={str(row.get('pinpoint_setup') or '-'):<29} "
                    f"tape={f(row.get('pinpoint_live_tape_score')):5.1f} rank={f(fc.get('tail_rank')):5.1f} "
                    f"P20_24={_fmt_prob(p20)} P30_72={_fmt_prob(p30)} expRet24={er_txt} expR={r_txt} "
                    f"eta20={fc.get('eta20_24_minutes') or '-'}m eta30={fc.get('eta30_72_minutes') or '-'}m "
                    f"data={fc.get('data_status')} method={fc.get('ranking_method')}",
                    flush=True,
                )

            executable = [x for x in rows if x[4].get("pinpoint_buy")]
            print(f"Ψ-TAIL-HUNTER EXECUTABLE {len(executable)}", flush=True)
            for i, (_, _, _, sym, row, fc) in enumerate(executable[:5], 1):
                print(
                    f"THE{i:02d}. {sym:<14} setup={row.get('pinpoint_setup')} "
                    f"P20_24={_fmt_prob(fc.get('p20_24'))} P30_72={_fmt_prob(fc.get('p30_72'))} "
                    f"rank={f(fc.get('tail_rank')):.1f} entry={f(row.get('pinpoint_trigger')):.10g} "
                    f"stop={f(row.get('pinpoint_stop')):.10g} risk={f(row.get('pinpoint_risk_pct')):.3f}%",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            tail_stats["board_errors"] += 1
            print(f"Ψ-TAIL-HUNTER BOARD_ERROR {type(exc).__name__}: {exc}", flush=True)


for mod in (base, expected):
    try:
        mod.VERSION = VERSION
    except Exception:
        pass
try:
    scanner.VERSION = VERSION
except Exception:
    pass

_load_state()


async def main():
    print(
        "[v11.0.3.6] Ψ TAIL-HUNTER active — Pinpoint remains the sole BUY NOW authority. "
        "Tail-Hunter records 2s trigger-accurate target-before-stop paths, learns P20/P30/P40/P50 "
        "at 4h/24h/72h with Bayesian similarity weighting and conservative lower bounds, ranks "
        "expected 24h return/Expected-R, and never creates or relaxes a BUY signal.",
        flush=True,
    )
    await asyncio.gather(base.main(), tail_sampler_loop(), tail_board_loop())


if __name__ == "__main__":
    asyncio.run(main())
