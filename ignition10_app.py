import asyncio
import json
import time
from collections import Counter, defaultdict, deque

import aiohttp
import app
import qualifier_app as q
import stable10_app as s

VERSION = "10.7-ignition-radar"
TARGET = q.QUALIFIER_TARGET

STABLE_MICRO_SLOTS = 40
RAPID_MICRO_SLOTS = 8
RAPID_SCAN_SECONDS = 2
RAPID_MIN_HOLD = 45
RAPID_REPLACE_COOLDOWN = 10
RAPID_STRUCTURE_COOLDOWN = 45
RAPID_TRIGGER_SCORE = 34.0
RAPID_EMERGENCY_SCORE_GAP = 24.0
MOVE_SAMPLE_SECONDS = 5
MOVE_HISTORY_SECONDS = 3700
MISSED_COOLDOWN = 3600

q.SAMPLE_SECONDS = 1.0
q.DISCOVERY_HISTORY = 180
q.DISCOVERY_SAMPLES = 190
q.HOT_COUNT = 160
q.MICRO_SLOTS = STABLE_MICRO_SLOTS
app.MICRO_UNIVERSE_SIZE = STABLE_MICRO_SLOTS
app.ANOMALY_PROMOTION_SLOTS = STABLE_MICRO_SLOTS
app.USER_AGENT = "psi-v10-live-scanner/10.7-ignition-radar"

NON_DIRECTIONAL_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USD1", "RLUSD", "EUR", "GBP", "EURI",
}

rapid_symbols = []
rapid_entered = {}
rapid_last_replace = 0.0
rapid_last_structure = defaultdict(float)
rapid_last_score = {}
rapid_metrics = {}
rapid_ws_connected = False
rapid_ws_symbols = []
rapid_promotions = 0
rapid_replacements = 0
rapid_structure_checks = 0

move_history = defaultdict(lambda: deque(maxlen=max(100, int(MOVE_HISTORY_SECONDS / MOVE_SAMPLE_SECONDS) + 20)))
last_move_sample = 0.0
missed_move_events = deque(maxlen=500)
missed_event_cooldown = {}
promotion_lock = asyncio.Lock()

_base_hot = q.hot
_base_coverage = s.coverage


def now():
    return time.time()


def _base_symbol(symbol):
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def _directional(symbol):
    return _base_symbol(symbol) not in NON_DIRECTIONAL_BASES


def _before(samples, target):
    for item in reversed(samples):
        if item[0] <= target:
            return item
    return samples[0] if samples else None


def _safe_ratio(a, b, fallback=0.0):
    return a / b if b and b > 0 else fallback


def ignition_metric(symbol):
    """Sub-minute market-wide ignition telemetry. Detection deliberately has no anti-chase block."""
    samples = q.disc.get(symbol)
    if not samples or len(samples) < 5:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples or ())}

    t, price, qvol, trades, bid, ask = samples[-1]
    if price <= 0:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples)}
    age = t - samples[0][0]
    if age < 12:
        return {"symbol": symbol, "score": 0.0, "trigger": False, "samples": len(samples), "age_s": age}

    s5 = _before(samples, t - 5)
    s15 = _before(samples, t - 15)
    s30 = _before(samples, t - 30)
    s60 = _before(samples, t - 60)

    def ret(x):
        return (price / x[1] - 1.0) * 100.0 if x and x[1] > 0 else 0.0

    r5, r15, r30, r60 = ret(s5), ret(s15), ret(s30), ret(s60)

    def delta(idx, x):
        return max(0.0, (qvol if idx == 2 else trades) - (x[idx] if x else (qvol if idx == 2 else trades)))

    v5, v15, v30, v60 = delta(2, s5), delta(2, s15), delta(2, s30), delta(2, s60)
    n5, n15, n30, n60 = delta(3, s5), delta(3, s15), delta(3, s30), delta(3, s60)

    prev_v10 = max(0.0, v15 - v5)
    prev_v15 = max(0.0, v30 - v15)
    prev_n10 = max(0.0, n15 - n5)
    prev_n15 = max(0.0, n30 - n15)
    vol_accel_5 = _safe_ratio(v5 / 5.0, prev_v10 / 10.0, 0.0)
    vol_accel_15 = _safe_ratio(v15 / 15.0, prev_v15 / 15.0, 0.0)
    trade_accel_5 = _safe_ratio(n5 / 5.0, prev_n10 / 10.0, 0.0)
    trade_accel_15 = _safe_ratio(n15 / 15.0, prev_n15 / 15.0, 0.0)

    price_accel = max(0.0, r5 * 3.0 - r15) + max(0.0, r15 * 2.0 - r30)
    spread = (ask - bid) / ((ask + bid) / 2.0) * 10000.0 if bid > 0 and ask > bid else 999.0
    recent_prices = [x[1] for x in samples if x[0] >= t - 60 and x[1] > 0]
    range60 = ((max(recent_prices) - min(recent_prices)) / price * 100.0) if recent_prices else 999.0

    clamp = lambda x, lo, hi: max(lo, min(hi, x))
    score = 0.0
    score += clamp(r5, 0.0, 1.5) * 18.0
    score += clamp(r15, 0.0, 3.0) * 8.0
    score += clamp(r30, 0.0, 5.0) * 3.0
    score += clamp(price_accel, 0.0, 3.0) * 7.0
    score += clamp(vol_accel_5 - 1.0, 0.0, 5.0) * 8.0
    score += clamp(vol_accel_15 - 1.0, 0.0, 4.0) * 4.0
    score += clamp(trade_accel_5 - 1.0, 0.0, 5.0) * 6.0
    score += clamp(trade_accel_15 - 1.0, 0.0, 4.0) * 3.0
    score += 6.0 if spread <= 8 else 2.0 if spread <= 15 else -8.0 if spread >= 30 else 0.0
    score += 4.0 if range60 <= 1.8 else 0.0

    extension_penalty = max(0.0, r60 - 6.0) * 1.5 + max(0.0, r30 - 4.0) * 1.0
    score -= min(extension_penalty, 12.0)

    acceleration = (
        r5 >= 0.12
        or r15 >= 0.25
        or vol_accel_5 >= 1.8
        or trade_accel_5 >= 1.8
        or (vol_accel_15 >= 1.5 and trade_accel_15 >= 1.5)
    )
    trigger = _directional(symbol) and acceleration and score >= RAPID_TRIGGER_SCORE and spread <= 25

    return {
        "symbol": symbol,
        "score": round(score, 3),
        "trigger": trigger,
        "r5": round(r5, 4),
        "r15": round(r15, 4),
        "r30": round(r30, 4),
        "r60": round(r60, 4),
        "price_accel": round(price_accel, 4),
        "vol_accel_5": round(vol_accel_5, 3),
        "vol_accel_15": round(vol_accel_15, 3),
        "trade_accel_5": round(trade_accel_5, 3),
        "trade_accel_15": round(trade_accel_15, 3),
        "spread_bps": round(spread, 3),
        "range60_pct": round(range60, 4),
        "price": price,
        "quote_5s": round(v5, 2),
        "trades_5s": int(n5),
        "samples": len(samples),
        "age_s": round(age, 1),
    }


def ignition_rank(limit=None, triggered_only=False):
    rows = []
    for symbol in q.universe:
        metric = ignition_metric(symbol)
        rapid_metrics[symbol] = metric
        if triggered_only and not metric.get("trigger"):
            continue
        rows.append(metric)
    rows.sort(key=lambda x: (bool(x.get("trigger")), float(x.get("score", 0))), reverse=True)
    return rows[:limit] if limit else rows


def ignition_hot(limit=q.HOT_COUNT):
    rows = ignition_rank(triggered_only=False)
    if not rows or all(float(r.get("score", 0)) == 0 for r in rows[:10]):
        return _base_hot(limit)
    return [(float(r.get("score", 0)), r["symbol"]) for r in rows[:limit]]


async def _prepare_rapid(symbols):
    global rapid_structure_checks
    if not symbols or app.session is None:
        return
    async with promotion_lock:
        t = now()
        structure_needed = [
            symbol for symbol in symbols
            if (not q.sfresh(symbol)) and t - rapid_last_structure[symbol] >= RAPID_STRUCTURE_COOLDOWN
        ]
        if structure_needed:
            for symbol in structure_needed:
                rapid_last_structure[symbol] = t
            await app.structure_batch(structure_needed)
            stamp = q.ms()
            for symbol in structure_needed:
                if symbol in app.structure:
                    q.structure_ms[symbol] = stamp
                    q.structure_seen.add(symbol)
                    s.hunt_structurally_valid.add(symbol)
            rapid_structure_checks += len(structure_needed)

        sem = asyncio.Semaphore(4)

        async def one(symbol):
            async with sem:
                try:
                    return await app.load_fast_anomaly(app.session, symbol)
                except Exception:
                    return None

        anomaly_rows = await asyncio.gather(*(one(x) for x in symbols), return_exceptions=True)
        for row in anomaly_rows:
            if isinstance(row, dict):
                app.anomaly_state[row["symbol"]] = row


def _set_rapid(new_symbols, scores):
    global rapid_symbols, rapid_last_replace, rapid_promotions, rapid_replacements
    previous = list(rapid_symbols)
    prev_set = set(previous)
    new_symbols = list(dict.fromkeys(new_symbols))[:RAPID_MICRO_SLOTS]
    rapid_symbols = new_symbols
    t = now()
    for symbol in new_symbols:
        if symbol not in prev_set:
            rapid_entered[symbol] = t
            rapid_promotions += 1
            app.ensure_micro_state(symbol)
    for symbol in list(rapid_entered):
        if symbol not in set(new_symbols):
            rapid_entered.pop(symbol, None)
    rapid_replacements += len(prev_set - set(new_symbols))
    rapid_last_replace = t
    for symbol, score in scores.items():
        rapid_last_score[symbol] = score


async def update_rapid_pool():
    if not q.universe:
        return
    ranked = ignition_rank(limit=80, triggered_only=True)
    if not ranked:
        return
    score_map = {r["symbol"]: float(r.get("score", 0)) for r in ranked}
    t = now()

    locked_now = set(q.locks())
    keep = [
        symbol for symbol in rapid_symbols
        if symbol in locked_now
        or t - rapid_entered.get(symbol, t) < RAPID_MIN_HOLD
        or score_map.get(symbol, 0) >= RAPID_TRIGGER_SCORE * 0.75
    ]
    proposal = list(keep)
    for row in ranked:
        symbol = row["symbol"]
        if symbol not in proposal:
            proposal.append(symbol)
        if len(proposal) >= RAPID_MICRO_SLOTS:
            break

    if len(rapid_symbols) >= RAPID_MICRO_SLOTS and t - rapid_last_replace < RAPID_REPLACE_COOLDOWN:
        current_scores = [(rapid_last_score.get(x, score_map.get(x, 0)), x) for x in rapid_symbols]
        weakest_score, weakest = min(current_scores) if current_scores else (0, None)
        strongest = ranked[0] if ranked else None
        if strongest and strongest["symbol"] not in rapid_symbols and float(strongest["score"]) >= weakest_score + RAPID_EMERGENCY_SCORE_GAP:
            proposal = [x for x in rapid_symbols if x != weakest] + [strongest["symbol"]]
        else:
            proposal = list(rapid_symbols)

    proposal = proposal[:RAPID_MICRO_SLOTS]
    changed = set(proposal) != set(rapid_symbols)
    if changed:
        old = set(rapid_symbols)
        _set_rapid(proposal, score_map)
        newcomers = [x for x in proposal if x not in old]
        asyncio.create_task(_prepare_rapid(newcomers))
        print(
            f"Ψ-V10.7 RAPID promoted={len(newcomers)} active={len(rapid_symbols)}/{RAPID_MICRO_SLOTS} "
            + " ".join(f"{x}:{score_map.get(x,0):.1f}" for x in rapid_symbols),
            flush=True,
        )
    else:
        for symbol in rapid_symbols:
            rapid_last_score[symbol] = score_map.get(symbol, rapid_last_score.get(symbol, 0))


def tick():
    rows = []
    selected = list(dict.fromkeys(list(app.selected_micro_symbols) + list(rapid_symbols)))
    for symbol in selected:
        try:
            row = app.evaluate_symbol(symbol)
        except Exception:
            continue
        if row:
            rows.append(row)

    t = now()
    current_symbols = set()
    for row in rows:
        symbol = row["symbol"]
        current_symbols.add(symbol)
        q.latest[symbol] = row
        raw = row.get("state", "REJECT")
        micro_ready = bool(row.get("micro_ready"))

        if micro_ready:
            s.hunt_micro_seen.add(symbol)
            previous_verified = s.verified_state.get(symbol)
            if raw in q.QUALIFIER_STATES:
                q.streak[symbol] = q.streak[symbol] + 1 if previous_verified in q.QUALIFIER_STATES else 1
                s.verified_state[symbol] = raw
                q.last_raw[symbol] = raw
                q.locked_until[symbol] = max(q.locked_until.get(symbol, 0), t + q.LOCK_GRACE)
                if q.streak[symbol] >= q.PERSIST:
                    published = dict(row)
                    published["persistence_samples"] = q.streak[symbol]
                    published["hunter_locked"] = True
                    published["lane"] = "RAPID" if symbol in rapid_symbols else "STABLE"
                    q.stable[symbol] = published
                    s.stable_until[symbol] = t + s.QUALIFIER_DATA_GRACE
                    s.near_memory.pop(symbol, None)
                else:
                    s.near_memory[symbol] = s._near_row(row, ["WAIT_PERSISTENCE"])
            else:
                q.streak[symbol] = 0
                s.verified_state[symbol] = raw
                q.last_raw[symbol] = raw
                q.stable.pop(symbol, None)
                s.stable_until.pop(symbol, None)
                s.near_memory[symbol] = s._near_row(row)
        else:
            if symbol in q.stable and s.stable_until.get(symbol, 0) < t:
                q.stable.pop(symbol, None)
                s.stable_until.pop(symbol, None)
            blockers = ["MICRO_NOT_READY"]
            if q.streak.get(symbol, 0) > 0:
                blockers.append("PERSISTENCE_PAUSED")
            s.near_memory[symbol] = s._near_row(row, blockers)

    for symbol in list(q.stable):
        if symbol not in current_symbols and s.stable_until.get(symbol, 0) < t:
            q.stable.pop(symbol, None)
            s.stable_until.pop(symbol, None)
    for symbol in list(q.locked_until):
        if q.locked_until[symbol] < t:
            q.locked_until.pop(symbol, None)

    q.near = s.near_diag(10)
    q.qualifier_cycles += 1
    if len(q.stable) >= TARGET:
        s.hunt_target_reached = True
        s.hunt_completed_at = s.hunt_completed_at or t

    fresh_valid = {x for x in s.hunt_structurally_valid if x in q.universe_set and q.sfresh(x)}
    if s.hunt_full_structural_pass and fresh_valid and fresh_valid.issubset(s.hunt_micro_seen) and len(q.stable) < TARGET:
        s.hunt_exhausted = True
        s.hunt_completed_at = s.hunt_completed_at or t


def _price_at(history, target):
    for item in reversed(history):
        if item[0] <= target:
            return item
    return history[0] if history else None


def record_missed_moves():
    global last_move_sample
    t = now()
    if t - last_move_sample < MOVE_SAMPLE_SECONDS:
        return
    last_move_sample = t
    rapid_set, stable_set = set(rapid_symbols), set(app.selected_micro_symbols)

    for symbol in q.universe:
        samples = q.disc.get(symbol)
        if not samples:
            continue
        price = samples[-1][1]
        if price <= 0:
            continue
        metric = rapid_metrics.get(symbol) or ignition_metric(symbol)
        qrow = q.latest.get(symbol, {})
        move_history[symbol].append((
            t, price, float(metric.get("score", 0)), bool(metric.get("trigger")),
            symbol in rapid_set, symbol in stable_set, symbol in s.hunt_micro_seen,
            qrow.get("state"), float(qrow.get("score", 0) or 0),
        ))

        history = move_history[symbol]
        for label, seconds, threshold in (("5m", 300, 5.0), ("15m", 900, 10.0), ("1h", 3600, 20.0)):
            base = _price_at(history, t - seconds)
            if not base or t - base[0] < seconds * 0.8 or base[1] <= 0:
                continue
            gain = (price / base[1] - 1.0) * 100.0
            key = (symbol, label)
            if gain < threshold or t - missed_event_cooldown.get(key, 0) < MISSED_COOLDOWN:
                continue
            missed_event_cooldown[key] = t
            reason = "QUALIFIER_NOT_ALIGNED"
            if not base[3]: reason = "IGNITION_NOT_TRIGGERED_AT_BASELINE"
            elif not base[4] and not base[5]: reason = "DETECTED_BUT_NOT_IN_MICRO_LANE_AT_BASELINE"
            elif not base[6]: reason = "MICRO_NOT_VERIFIED_AT_BASELINE"
            elif base[7] not in q.QUALIFIER_STATES: reason = "MICRO_VERIFIED_BUT_SETUP_GATES_FAILED"
            event = {
                "time": int(t), "symbol": symbol, "window": label, "gain_pct": round(gain, 2),
                "baseline_ignition_score": round(base[2], 2), "baseline_trigger": base[3],
                "baseline_rapid": base[4], "baseline_stable": base[5], "baseline_micro_verified": base[6],
                "baseline_state": base[7], "baseline_qualifier_score": base[8], "reason": reason,
            }
            missed_move_events.append(event)
            print(
                f"Ψ-V10.7 MISSED_MOVE {symbol} +{gain:.2f}%/{label} reason={reason} "
                f"baseline_ignition={base[2]:.1f} rapid={base[4]} stable={base[5]} micro={base[6]} state={base[7]}",
                flush=True,
            )


async def rapid_radar_loop():
    while True:
        try:
            await asyncio.sleep(RAPID_SCAN_SECONDS)
            await update_rapid_pool()
            record_missed_moves()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.last_error = f"IGNITION_RADAR: {type(exc).__name__}: {exc}"
            print(app.last_error, flush=True)


async def rapid_websocket_loop():
    global rapid_ws_connected, rapid_ws_symbols
    while True:
        try:
            symbols = [x for x in rapid_symbols if x not in set(app.selected_micro_symbols)]
            if not symbols:
                rapid_ws_connected = False
                rapid_ws_symbols = []
                await asyncio.sleep(1)
                continue
            streams = []
            for symbol in symbols:
                lower = symbol.lower()
                streams.extend([f"{lower}@aggTrade", f"{lower}@depth20@100ms"])
            url = f"{app.WS_BASE}/stream?streams={'/'.join(streams)}"
            assert app.session is not None
            print(f"Ψ-V10.7 RAPID WS connecting for {len(symbols)} symbols book=DEPTH20_WS...", flush=True)
            async with app.session.ws_connect(url, heartbeat=None, receive_timeout=90, max_msg_size=0) as ws:
                rapid_ws_connected = True
                rapid_ws_symbols = list(symbols)
                for symbol in symbols:
                    st = app.ensure_micro_state(symbol)
                    st["book_buffer"].clear()
                    st["book_snapshot_ready"] = False
                    st["book_sequence_ok"] = True
                    st["book_sequence_samples"] = 0
                    st["book_resyncing"] = False
                    st["last_book_update_id"] = None
                print("Ψ-V10.7 RAPID WS connected book=REST_FREE_DEPTH20.", flush=True)
                async for message in ws:
                    current = [x for x in rapid_symbols if x not in set(app.selected_micro_symbols)]
                    if set(current) != set(symbols):
                        print("Ψ-V10.7 RAPID universe changed; reconnecting.", flush=True)
                        break
                    if message.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        stream_name, data = payload.get("stream", ""), payload.get("data", {})
                        if not stream_name or not isinstance(data, dict):
                            continue
                        symbol = stream_name.split("@")[0].upper()
                        if "@aggTrade" in stream_name:
                            app.process_agg_trade(symbol, data)
                        elif "@depth20" in stream_name:
                            app.process_partial_depth_snapshot(symbol, data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            app.last_error = f"RAPID_WS: {type(exc).__name__}: {exc}"
            print(app.last_error, flush=True)
        finally:
            rapid_ws_connected = False
            rapid_ws_symbols = []
        await asyncio.sleep(1)


def coverage():
    c = _base_coverage()
    union = set(app.selected_micro_symbols) | set(rapid_symbols)
    fresh = 0
    now_ms = q.ms()
    for symbol in union:
        st = app.micro_state.get(symbol, {})
        if now_ms - int(st.get("last_trade_ms", 0) or 0) <= 15000 and now_ms - int(st.get("last_book_ms", 0) or 0) <= 5000:
            fresh += 1
    c["micro_total"] = len(union)
    c["micro_verified_fresh"] = fresh
    c["ignition_radar"] = {
        "version": VERSION,
        "market_wide_symbols": len(q.universe),
        "sample_seconds": q.SAMPLE_SECONDS,
        "rapid_slots": RAPID_MICRO_SLOTS,
        "rapid_active": list(rapid_symbols),
        "rapid_ws_connected": rapid_ws_connected,
        "rapid_promotions": rapid_promotions,
        "rapid_replacements": rapid_replacements,
        "rapid_structure_checks": rapid_structure_checks,
        "triggered_now": sum(1 for x in rapid_metrics.values() if x.get("trigger")),
        "top": ignition_rank(limit=10, triggered_only=False),
        "missed_move_events": list(missed_move_events)[-20:],
    }
    return c


async def health(req):
    return app.web.json_response({
        "ok": True,
        "service": "psi-v10-live-scanner",
        "version": VERSION,
        "policy": q.QUALIFIER_POLICY,
        "qualifier_target": TARGET,
        "persistence_samples": q.PERSIST,
        "scanner_ready": app.scanner_ready,
        "websocket_connected": app.websocket_connected,
        "rapid_websocket_connected": rapid_ws_connected,
        "discovery_ws_connected": q.disc_ws,
        "strict_uk_allowlist_enabled": bool(app.UK_SYMBOLS),
        "coverage": coverage(),
        "rest_governor": {
            "requests": q.rest_requests,
            "backoff_active": now() < q.backoff_until,
            "backoff_remaining_seconds": max(0, int(q.backoff_until - now())),
            "http_418_count": q.count418,
            "http_429_count": q.count429,
        },
        "stable_qualifier_symbols": list(q.stable),
        "near_miss_diagnostics": s.near_diag(10),
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", TARGET)), TARGET))
    except ValueError:
        limit = TARGET
    app.resolve_outcomes()
    rows = s.results(limit)
    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.7 Ignition Radar + Stable Target-10",
        "version": VERSION,
        "policy": q.QUALIFIER_POLICY,
        "buy_policy": "ALL_HARD_SAFETY_GATES_PLUS_ALL_GATES_OF_ONE_VERIFIED_SETUP",
        "qualifier_target": TARGET,
        "returned": len(rows),
        "state_counts": dict(Counter(row["state"] for row in rows)),
        "coverage": coverage(),
        "results": rows,
        "near_miss_diagnostics": s.near_diag(10),
        "ignition_top": ignition_rank(limit=10, triggered_only=False),
        "missed_move_events": list(missed_move_events)[-20:],
        "generated_ms": q.ms(),
    })


async def hunt_tick_loop():
    while True:
        await asyncio.sleep(q.TICK_SECONDS)
        tick()
        s.maybe_restart_hunt()


async def print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        rows = s.results()
        c = coverage(); h = c["hunt"]; radar = c["ignition_radar"]
        print("\n==================================================", flush=True)
        print(f"Ψ-V10.7 IGNITION + TARGET-10 HUNT — {len(rows)}/{TARGET}", flush=True)
        print(
            f"attempted={h['structure_attempted']}/{c['full_universe']} ({h['structure_attempted_pct']:.1f}%) "
            f"valid={h['structure_valid']} micro_verified={h['micro_candidates_verified']} "
            f"live_micro={c['micro_verified_fresh']}/{c['micro_total']} locked={c['locked_slots']} "
            f"rapid={len(rapid_symbols)}/{RAPID_MICRO_SLOTS} triggers={radar['triggered_now']} full_pass={h['full_structural_pass_complete']}",
            flush=True,
        )
        print("==================================================", flush=True)
        for i, row in enumerate(rows, 1):
            print(
                f"{i:02d}. {row['symbol']:12s} {row['state']:14s} score={row['score']:6.2f} "
                f"persist={row.get('persistence_samples',0)} lane={row.get('lane','STABLE'):6s} setup={row['active_setup'][:10]:10s} "
                f"OFI={row['ofi']:+.3f} OBI={row['obi']:+.3f} buy={row['aggressive_buy_ratio']:.2%} ready={row['micro_ready']}",
                flush=True,
            )
        if not rows:
            print("No persistent PRE-IGNITION / BUY NOW setup currently qualifies.", flush=True)

        tops = ignition_rank(limit=5, triggered_only=False)
        if tops:
            print("IGNITION RADAR TOP:", flush=True)
            for i, r in enumerate(tops, 1):
                print(
                    f"R{i:02d}. {r['symbol']:12s} score={float(r.get('score',0)):6.1f} trig={r.get('trigger')} "
                    f"r5={float(r.get('r5',0)):+.2f}% r15={float(r.get('r15',0)):+.2f}% "
                    f"v5x={float(r.get('vol_accel_5',0)):.2f} t5x={float(r.get('trade_accel_5',0)):.2f} "
                    f"rapid={r['symbol'] in rapid_symbols}", flush=True,
                )

        diagnostics = s.near_diag(10)
        if diagnostics:
            print("TOP NEAR MISSES:", flush=True)
            for i, d in enumerate(diagnostics, 1):
                print(
                    f"N{i:02d}. {str(d.get('symbol')):12s} state={str(d.get('state')):12s} "
                    f"score={float(d.get('score',0) or 0):6.2f} micro={d.get('micro_ready')} "
                    f"blockers={d.get('blockers',[])} hard={d.get('failed_hard',[])} setup={d.get('failed_setup',[])}",
                    flush=True,
                )


q.hot = ignition_hot
s.tick = tick
q.tick = tick
s.print_loop = print_loop
q.print_loop = print_loop
app.health = health
app.scan_endpoint = scan
app.ranked_results = s.results


async def main():
    app.session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30),
        connector=aiohttp.TCPConnector(limit=120, ttl_dns_cache=300),
        headers={"User-Agent": app.USER_AGENT},
    )
    runner = await app.start_http_server()
    tasks = []
    try:
        print(
            "Ψ-V10.7 IGNITION RADAR ACTIVE — all-market 1s discovery, 5s/15s/30s/60s acceleration, "
            "40 stable micro + 8 isolated rapid micro slots, emergency structure promotion, "
            "anti-chase separated from detection, missed-move recorder, STRICT PRE/BUY GATES UNCHANGED",
            flush=True,
        )
        await q.refresh_universe(True)
        tasks.append(asyncio.create_task(q.discovery_loop()))
        await s.refresh_structure()
        await q.refresh_anomaly()
        tick()
        tasks += [
            asyncio.create_task(q.loop(q.UNIVERSE_SECONDS, lambda: q.refresh_universe(True), "UNIVERSE")),
            asyncio.create_task(q.loop(q.STRUCTURE_SECONDS, s.refresh_structure, "STRUCTURE")),
            asyncio.create_task(q.loop(q.ANOMALY_SECONDS, q.refresh_anomaly, "ANOMALY")),
            asyncio.create_task(q.loop(q.POOL_SECONDS, s.rebalance_pool, "STABLE_POOL")),
            asyncio.create_task(app.websocket_loop()),
            asyncio.create_task(rapid_radar_loop()),
            asyncio.create_task(rapid_websocket_loop()),
            asyncio.create_task(hunt_tick_loop()),
            asyncio.create_task(print_loop()),
        ]
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        await app.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Ψ-V10.7 stopped", flush=True)
