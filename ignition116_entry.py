import asyncio
import json
import math
import time
from collections import defaultdict

import aiohttp

import ignition1151_entry as base15

v15 = base15.base15
v14 = v15.base14
v13 = v14.base

scanner = base15.scanner
q = scanner.q
s = scanner.s
app = scanner.app

VERSION = "10.16.3-depth-only-execution-shards"

BOARD_SIZE = 10
PRE_LANE_SLOTS = 3
BREAKOUT_LANE_SLOTS = 3
BUY_LANE_SLOTS = 4

MICRO_SHARDS = 4
MICRO_SHARD_SIZE = 20
SHARD_POLL_SECONDS = 0.5
EXEC_WS_HEARTBEAT = 20.0
EXEC_WS_RECEIVE_TIMEOUT = 90.0
EXEC_WS_CONNECT_TIMEOUT = 20.0
EXEC_WS_RECONNECT_MAX_DELAY = 8.0

EXCLUDED_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "USD1", "RLUSD", "EURI",
    "EUR", "GBP", "PAXG", "XAUT",
}
TOKENISED_BASES = {
    "AAPL", "AAPLB", "AMZN", "AMZNB", "GOOGL", "GOOGLB", "META", "METAB",
    "MSFT", "MSFTB", "MSTR", "MSTRB", "IBM", "IBMB", "TSLA", "TSLAB",
    "SPY", "SPYB", "QQQ", "QQQB", "TQQQ", "TQQQB", "SOXL", "SOXLB",
    "SPCX", "SPCXB", "HOOD", "HOODB", "COINB", "CRCLB", "DELLB", "SNDKB",
    "NBISB", "CRWVB", "AGPUB", "USARB",
}

shard_assignments = {}
shard_generation = [0 for _ in range(MICRO_SHARDS)]
shard_connected = [False for _ in range(MICRO_SHARDS)]
shard_reconnects = [0 for _ in range(MICRO_SHARDS)]
shard_last_change = [0.0 for _ in range(MICRO_SHARDS)]
shard_current_symbols = [[] for _ in range(MICRO_SHARDS)]
shard_host_cursor = [i for i in range(MICRO_SHARDS)]
shard_last_host = ["" for _ in range(MICRO_SHARDS)]
shard_last_msg_ms = [0 for _ in range(MICRO_SHARDS)]

board_stats = {
    "prints": 0,
    "strict_pre_count": 0,
    "strict_breakout_count": 0,
    "strict_buy_count": 0,
    "backfill_count": 0,
}


def _f(value, default=0.0):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _base_symbol(symbol):
    return symbol[:-4] if str(symbol).endswith("USDT") else str(symbol)


def _directional_board_symbol(symbol):
    base = _base_symbol(symbol).upper()
    if base in EXCLUDED_BASES or base in TOKENISED_BASES:
        return False
    return True


def _state_rank(state):
    return {
        "BUY NOW": 7.0,
        "PRE-IGNITION": 6.0,
        "15% IGNITION WATCH": 5.0,
        "EARLY OPPORTUNITY": 4.0,
        "WATCH": 3.0,
        "COLLECTING DATA": 2.0,
        "REJECT": 1.0,
    }.get(str(state or "REJECT"), 0.0)


def _source_row(symbol):
    return q.latest.get(symbol) or {}


def _diag_pool():
    rows = {}
    try:
        for row in s.near_diag(120):
            symbol = str(row.get("symbol") or "")
            if symbol:
                rows[symbol] = dict(row)
    except Exception:
        pass

    for symbol, source in list(q.latest.items()):
        if not source or not _directional_board_symbol(symbol):
            continue
        row = rows.get(symbol, {"symbol": symbol})
        row.setdefault("state", source.get("state"))
        row.setdefault("formal_state", source.get("formal_state", source.get("state")))
        row.setdefault("score", source.get("score"))
        row.setdefault("micro_ready", source.get("micro_ready"))
        row.setdefault("passed_major_layers", sum(bool(v) for v in (source.get("layer_results") or {}).values()))
        row.setdefault("missing_signal_layers", [k for k, v in (source.get("layer_results") or {}).items() if not v])
        row.setdefault("ignition15_watch", bool(source.get("ignition15_watch")))
        row.setdefault("ignition15_score", source.get("ignition15_score"))
        row.setdefault("v1014_score", source.get("v1014_score"))
        row.setdefault("v1014_grade", source.get("v1014_grade"))
        row.setdefault("ignition_velocity_per_min", source.get("ignition_velocity_per_min"))
        row.setdefault("ignition_acceleration_per_min2", source.get("ignition_acceleration_per_min2"))
        row.setdefault("resistance_weakness_score", source.get("resistance_weakness_score"))
        row.setdefault("book_persistence", source.get("book_persistence") or {})
        row.setdefault("candidate_persistence_samples", v13.candidate_persistence.get(symbol, 0))
        try:
            entry = v13._entry_telemetry(app, q, symbol, source)
            row.update({k: v for k, v in entry.items() if row.get(k) is None})
        except Exception:
            pass
        rows[symbol] = row

    for discovery_score, symbol in q.hot(limit=80):
        if symbol in rows or not _directional_board_symbol(symbol):
            continue
        sd = app.structure.get(symbol) or {}
        if not sd:
            continue
        distance = _f(sd.get("breakout_distance_pct"), 999.0)
        resistance = _f(sd.get("resistance"), 0.0)
        price = _f(sd.get("price"), 0.0)
        if resistance <= 0 or price <= 0:
            continue
        rows[symbol] = {
            "symbol": symbol,
            "state": "DEVELOPING",
            "formal_state": "DEVELOPING",
            "score": _f(discovery_score, 0.0),
            "micro_ready": False,
            "passed_major_layers": 0,
            "missing_signal_layers": ["LIVE_MICRO_QUALIFICATION"],
            "ignition15_watch": False,
            "ignition15_score": max(0.0, min(100.0, _f(discovery_score, 0.0) * 0.6)),
            "v1014_score": max(0.0, min(100.0, _f(discovery_score, 0.0) * 0.55)),
            "v1014_grade": "DEVELOPING",
            "ignition_velocity_per_min": 0.0,
            "ignition_acceleration_per_min2": 0.0,
            "resistance_weakness_score": 0.0,
            "book_persistence": {},
            "candidate_persistence_samples": 0,
            "resistance": resistance,
            "breakout_distance_pct": distance,
            "entry_status": "WAIT_MICRO_QUALIFICATION",
            "breakout_entry_trigger": None,
            "execution_gate_status": "BLOCKED",
            "failed_execution_gates": ["LIVE_MICRO_DATA"],
            "combined_blockers": ["LIVE_MICRO_DATA"],
            "discovery_only": True,
        }

    return [r for r in rows.values() if _directional_board_symbol(r.get("symbol", ""))]


def _execution_pass(row):
    status = str(row.get("execution_gate_status") or "")
    if status:
        return status == "PASS_ALL"
    hard = row.get("failed_execution_gates")
    if hard is None:
        hard = row.get("failed_hard")
    return hard in (None, [], (), "PASS_ALL")


def _micro_pass(row):
    source = _source_row(row.get("symbol"))
    return bool(source.get("micro_ready") or source.get("micro_collection_ready") or row.get("micro_ready"))


def _distance(row):
    value = row.get("breakout_distance_pct")
    if value is None:
        return 999.0
    return _f(value, 999.0)


def _layers(row):
    value = row.get("passed_major_layers")
    if value is not None:
        try:
            return int(value)
        except Exception:
            pass
    source = _source_row(row.get("symbol"))
    return sum(bool(v) for v in (source.get("layer_results") or {}).values())


def _book_score(row):
    book = row.get("book_persistence") or {}
    return _f(book.get("score"), 0.0)


def _entry_rank(row):
    status = str(row.get("entry_status") or "")
    if status == "BREAKOUT_TRIGGER_ARMED":
        return 4
    if status in ("CONFIRM_BREAKOUT_BUFFER",):
        return 3
    if status == "WAIT_APPROACH":
        return 2
    if status == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER":
        return 0
    return 1


def _opportunity_score(row):
    state = str(row.get("formal_state") or row.get("state") or "")
    distance = _distance(row)
    if distance < 0:
        proximity = -15.0
    elif distance <= 3:
        proximity = 18.0 - distance * 2.5
    elif distance <= 10:
        proximity = 10.0 - (distance - 3.0) * 0.8
    else:
        proximity = max(-10.0, 4.0 - (distance - 10.0) * 0.5)

    score = 0.0
    score += _state_rank(state) * 11.0
    score += _layers(row) * 7.0
    score += 12.0 if _execution_pass(row) else -10.0
    score += 10.0 if _micro_pass(row) else -7.0
    score += min(15.0, _f(row.get("ignition15_score"), 0.0) * 0.15)
    score += min(12.0, _f(row.get("v1014_score"), 0.0) * 0.12)
    score += min(6.0, max(-3.0, _f(row.get("ignition_velocity_per_min"), 0.0) / 25.0))
    score += min(5.0, _f(row.get("resistance_weakness_score"), 0.0) * 0.05)
    score += min(5.0, _book_score(row) * 0.05)
    score += min(6.0, _f(row.get("candidate_persistence_samples"), 0.0) * 0.25)
    score += proximity

    if row.get("ignition15_watch"):
        score += 8.0
    if _entry_rank(row) >= 4:
        score += 8.0
    if str(row.get("entry_status") or "") == "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER":
        score -= 20.0
    blockers = set(row.get("combined_blockers") or [])
    if "ANTI_CHASE_OR_RUNNER" in blockers or "ANTI_CHASE_OR_RUNNER_LAYER" in blockers:
        score -= 25.0
    if row.get("book_instability_risk"):
        score -= 10.0
    if row.get("discovery_only"):
        score -= 18.0
    return round(score, 2)


def _pre_strict(row):
    d = _distance(row)
    state = str(row.get("state") or row.get("formal_state") or "")
    return bool(
        0.0 <= d <= 10.0
        and _layers(row) >= 4
        and (_micro_pass(row) or row.get("ignition15_watch"))
        and (
            state in ("PRE-IGNITION", "15% IGNITION WATCH")
            or bool(row.get("ignition15_watch"))
            or _f(row.get("ignition15_score"), 0.0) >= 68.0
        )
        and str(row.get("entry_status") or "") != "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
    )


def _breakout_strict(row):
    d = _distance(row)
    return bool(
        0.0 <= d <= 3.0
        and _entry_rank(row) >= 3
        and _layers(row) >= 4
        and str(row.get("entry_status") or "") != "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
    )


def _buy_strict(row):
    state = str(row.get("formal_state") or row.get("state") or "")
    return bool(
        state in ("BUY NOW", "PRE-IGNITION")
        or (
            _layers(row) == 6
            and _execution_pass(row)
            and _micro_pass(row)
            and str(row.get("entry_status") or "") != "RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
        )
    )


def _pre_sort(row):
    d = _distance(row)
    return (
        1 if _pre_strict(row) else 0,
        1 if row.get("ignition15_watch") else 0,
        _f(row.get("ignition15_score"), 0.0),
        _f(row.get("ignition_acceleration_per_min2"), 0.0),
        _f(row.get("ignition_velocity_per_min"), 0.0),
        _layers(row),
        -abs(d - 5.0) if d < 999 else -999,
        _opportunity_score(row),
    )


def _breakout_sort(row):
    d = _distance(row)
    return (
        1 if _breakout_strict(row) else 0,
        _entry_rank(row),
        1 if _execution_pass(row) else 0,
        1 if _micro_pass(row) else 0,
        _layers(row),
        -d if d >= 0 else -999,
        _f(row.get("resistance_weakness_score"), 0.0),
        _book_score(row),
        _opportunity_score(row),
    )


def _buy_sort(row):
    state = str(row.get("formal_state") or row.get("state") or "")
    return (
        1 if _buy_strict(row) else 0,
        _state_rank(state),
        _layers(row),
        1 if _execution_pass(row) else 0,
        1 if _micro_pass(row) else 0,
        min(12, int(_f(row.get("candidate_persistence_samples"), 0.0))),
        _entry_rank(row),
        _opportunity_score(row),
    )


def _lane_pick(candidates, used, slots, strict_fn, sorter, lane):
    available = [r for r in candidates if r.get("symbol") not in used]
    strict = [r for r in available if strict_fn(r)]
    strict.sort(key=sorter, reverse=True)
    picked = []

    for row in strict[:slots]:
        x = dict(row)
        x["board_lane"] = lane
        x["board_quality"] = "QUALIFIED"
        x["opportunity_score"] = _opportunity_score(x)
        picked.append(x)
        used.add(x["symbol"])

    if len(picked) < slots:
        remaining = [r for r in candidates if r.get("symbol") not in used]
        remaining.sort(key=sorter, reverse=True)
        for row in remaining[: slots - len(picked)]:
            x = dict(row)
            x["board_lane"] = lane
            x["board_quality"] = "DEVELOPING"
            x["opportunity_score"] = _opportunity_score(x)
            picked.append(x)
            used.add(x["symbol"])

    return picked


def opportunity_board():
    candidates = _diag_pool()
    candidates.sort(key=_opportunity_score, reverse=True)
    used = set()

    pre = _lane_pick(candidates, used, PRE_LANE_SLOTS, _pre_strict, _pre_sort, "PRE-IGNITION")
    breakout = _lane_pick(
        candidates, used, BREAKOUT_LANE_SLOTS, _breakout_strict, _breakout_sort, "READY-BREAKOUT"
    )
    buy = _lane_pick(candidates, used, BUY_LANE_SLOTS, _buy_strict, _buy_sort, "CLOSEST-BUY")

    board = pre + breakout + buy
    if len(board) < BOARD_SIZE:
        for row in candidates:
            if row.get("symbol") in used:
                continue
            x = dict(row)
            x["board_lane"] = "DEVELOPING"
            x["board_quality"] = "DEVELOPING"
            x["opportunity_score"] = _opportunity_score(x)
            board.append(x)
            used.add(x["symbol"])
            if len(board) >= BOARD_SIZE:
                break

    for i, row in enumerate(board[:BOARD_SIZE], 1):
        row["board_rank"] = i
    return board[:BOARD_SIZE]


def _assign_shards():
    selected = list(dict.fromkeys(app.selected_micro_symbols))[: MICRO_SHARDS * MICRO_SHARD_SIZE]
    selected_set = set(selected)
    changed_shards = set()

    for symbol, shard_id in list(shard_assignments.items()):
        if symbol not in selected_set:
            shard_assignments.pop(symbol, None)
            changed_shards.add(shard_id)

    counts = [0] * MICRO_SHARDS
    for symbol, shard_id in shard_assignments.items():
        if symbol in selected_set and 0 <= shard_id < MICRO_SHARDS:
            counts[shard_id] += 1

    if not shard_assignments:
        for idx, symbol in enumerate(selected):
            shard_id = min(MICRO_SHARDS - 1, idx // MICRO_SHARD_SIZE)
            shard_assignments[symbol] = shard_id
            counts[shard_id] += 1
            changed_shards.add(shard_id)
    else:
        for symbol in selected:
            if symbol in shard_assignments:
                continue
            eligible = [i for i in range(MICRO_SHARDS) if counts[i] < MICRO_SHARD_SIZE]
            if not eligible:
                break
            shard_id = min(eligible, key=lambda i: (counts[i], -i))
            shard_assignments[symbol] = shard_id
            counts[shard_id] += 1
            changed_shards.add(shard_id)

    new_lists = []
    for shard_id in range(MICRO_SHARDS):
        members = [s for s in selected if shard_assignments.get(s) == shard_id]
        members = members[:MICRO_SHARD_SIZE]
        new_lists.append(members)
        if set(members) != set(shard_current_symbols[shard_id]):
            shard_current_symbols[shard_id] = list(members)
            shard_generation[shard_id] += 1
            shard_last_change[shard_id] = time.time()
            changed_shards.add(shard_id)
    return changed_shards


def _sync_ws_status():
    active = [i for i in range(MICRO_SHARDS) if shard_connected[i]]
    app.websocket_connected = bool(active)
    union = []
    for shard_id in active:
        union.extend(shard_current_symbols[shard_id])
    app.websocket_symbols = list(dict.fromkeys(union))


async def _shard_loop(shard_id):
    reconnects = 0
    while True:
        generation = shard_generation[shard_id]
        symbols = list(shard_current_symbols[shard_id])
        try:
            if not symbols:
                shard_connected[shard_id] = False
                _sync_ws_status()
                await asyncio.sleep(0.5)
                continue

            # AggTrade is bridged from the already-stable full-universe
            # Monster tape. Qualified execution shards only carry depth20,
            # cutting subscription count and websocket load roughly in half.
            streams = []
            for symbol in symbols:
                lower = symbol.lower()
                streams.append(f"{lower}@depth20@100ms")

            bases = []
            for raw in (
                str(getattr(app, "WS_BASE", "") or "").rstrip("/"),
                "wss://data-stream.binance.vision",
                "wss://stream.binance.com:9443",
                "wss://stream.binance.com:443",
            ):
                if raw and raw not in bases:
                    bases.append(raw)
            base_url = bases[shard_host_cursor[shard_id] % len(bases)]
            url = f"{base_url}/stream?streams={'/'.join(streams)}"

            assert app.session is not None
            print(
                f"Ψ-V10.16.3 SHARD{shard_id+1} connecting symbols={len(symbols)} "
                f"gen={generation} host={base_url} mode=DEPTH_ONLY book=DEPTH20_WS",
                flush=True,
            )
            async with app.session.ws_connect(
                url,
                heartbeat=EXEC_WS_HEARTBEAT,
                receive_timeout=EXEC_WS_RECEIVE_TIMEOUT,
                max_msg_size=0,
                timeout=EXEC_WS_CONNECT_TIMEOUT,
            ) as ws:
                shard_connected[shard_id] = True
                shard_last_host[shard_id] = base_url
                shard_last_msg_ms[shard_id] = int(time.time() * 1000)
                reconnects = 0
                _sync_ws_status()

                for symbol in symbols:
                    st = app.ensure_micro_state(symbol)
                    st["book_buffer"].clear()
                    st["book_snapshot_ready"] = False
                    st["book_sequence_ok"] = True
                    st["book_sequence_samples"] = 0
                    st["book_resyncing"] = False
                    st["last_book_update_id"] = None
                    # Trade sequencing is now supplied continuously by the
                    # full-universe Monster aggTrade bridge, so a depth-shard
                    # reconnect must not reset or interrupt trade continuity.

                print(
                    f"Ψ-V10.16.3 SHARD{shard_id+1} connected symbols={len(symbols)} "
                    f"host={base_url} book=REST_FREE_DEPTH20",
                    flush=True,
                )

                async for message in ws:
                    if generation != shard_generation[shard_id]:
                        shard_reconnects[shard_id] += 1
                        print(
                            f"Ψ-V10.16 SHARD{shard_id+1} membership changed; reconnecting only this shard",
                            flush=True,
                        )
                        break

                    if message.type == aiohttp.WSMsgType.TEXT:
                        shard_last_msg_ms[shard_id] = int(time.time() * 1000)
                        try:
                            payload = json.loads(message.data)
                        except json.JSONDecodeError:
                            continue
                        stream_name = payload.get("stream", "")
                        data = payload.get("data", {})
                        if not stream_name or not isinstance(data, dict):
                            continue
                        symbol = stream_name.split("@")[0].upper()
                        if "@aggTrade" in stream_name:
                            app.process_agg_trade(symbol, data)
                        elif "@depth20" in stream_name:
                            app.process_partial_depth_snapshot(symbol, data)
                    elif message.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        shard_reconnects[shard_id] += 1
                        break

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            shard_reconnects[shard_id] += 1
            reconnects += 1
            host = shard_last_host[shard_id] or (
                bases[shard_host_cursor[shard_id] % len(bases)] if "bases" in locals() and bases else "-"
            )
            app.last_error = (
                f"SHARD_WS_{shard_id+1}: {type(exc).__name__}: {exc} host={host}"
            )
            print(app.last_error, flush=True)
            shard_host_cursor[shard_id] += 1
        finally:
            shard_connected[shard_id] = False
            _sync_ws_status()

        await asyncio.sleep(min(EXEC_WS_RECONNECT_MAX_DELAY, 1.0 + min(reconnects, 7)))


async def sharded_websocket_loop():
    _assign_shards()
    tasks = [asyncio.create_task(_shard_loop(i)) for i in range(MICRO_SHARDS)]
    try:
        while True:
            changed = _assign_shards()
            if changed:
                labels = ",".join(str(i + 1) for i in sorted(changed))
                print(
                    f"Ψ-V10.16 SHARD_ASSIGN changed={labels} "
                    f"sizes={[len(x) for x in shard_current_symbols]}",
                    flush=True,
                )
            _sync_ws_status()
            await asyncio.sleep(SHARD_POLL_SECONDS)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


app.websocket_loop = sharded_websocket_loop
scanner.VERSION = VERSION


async def _v1016_board_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            board = opportunity_board()
            pre_q = sum(1 for x in board if x.get("board_lane") == "PRE-IGNITION" and x.get("board_quality") == "QUALIFIED")
            brk_q = sum(1 for x in board if x.get("board_lane") == "READY-BREAKOUT" and x.get("board_quality") == "QUALIFIED")
            buy_q = sum(1 for x in board if x.get("board_lane") == "CLOSEST-BUY" and x.get("board_quality") == "QUALIFIED")
            backfills = sum(1 for x in board if x.get("board_quality") != "QUALIFIED")
            board_stats["prints"] += 1
            board_stats["strict_pre_count"] = pre_q
            board_stats["strict_breakout_count"] = brk_q
            board_stats["strict_buy_count"] = buy_q
            board_stats["backfill_count"] = backfills

            print(
                f"Ψ-V10.16 TOP10 BOARD {len(board)}/10 "
                f"pre={pre_q}/{PRE_LANE_SLOTS} breakout={brk_q}/{BREAKOUT_LANE_SLOTS} "
                f"closest_buy={buy_q}/{BUY_LANE_SLOTS} developing_backfills={backfills}",
                flush=True,
            )

            for i, row in enumerate(board, 1):
                symbol = str(row.get("symbol") or "-")
                lane = str(row.get("board_lane") or "-")
                quality = str(row.get("board_quality") or "-")
                state = str(row.get("formal_state") or row.get("state") or "-")
                layers = _layers(row)
                exec_txt = "PASS_ALL" if _execution_pass(row) else "BLOCKED"
                micro_txt = "READY" if _micro_pass(row) else "WAIT"
                resistance = row.get("resistance")
                entry = row.get("breakout_entry_trigger")
                distance = row.get("breakout_distance_pct")
                status = str(row.get("entry_status") or "-")
                rtxt = "-" if resistance is None else f"{float(resistance):.10g}"
                etxt = status if entry is None else f"{float(entry):.10g}"
                dtxt = "-" if distance is None else f"{float(distance):+.3f}%"
                blockers = row.get("combined_blockers") or row.get("missing_signal_layers") or []
                print(
                    f"B{i:02d}. {symbol:14s} lane={lane:14s} quality={quality:10s} "
                    f"state={state:18s} opp={_opportunity_score(row):6.1f} "
                    f"layers={layers}/6 exec={exec_txt:8s} micro={micro_txt:5s} "
                    f"ign15={_f(row.get('ignition15_score'),0):5.1f} "
                    f"res={rtxt} dist={dtxt} entry={etxt} status={status} "
                    f"blockers={blockers}",
                    flush=True,
                )

            now_ms = int(time.time() * 1000)
            ages = [
                (now_ms - ts) if ts > 0 else 999999999
                for ts in shard_last_msg_ms
            ]
            print(
                f"Ψ-V10.16.3 SHARDS sizes={[len(x) for x in shard_current_symbols]} "
                f"connected={sum(1 for x in shard_connected if x)}/{MICRO_SHARDS} "
                f"reconnects={shard_reconnects} generations={shard_generation} "
                f"hosts={shard_last_host} msgAgeMs={ages}",
                flush=True,
            )
        except Exception as exc:
            print(f"Ψ-V10.16 BOARD_ERROR {type(exc).__name__}: {exc}", flush=True)


_previous_print_loop = scanner.v7.print_loop


async def _combined_v1016_print_loop():
    await asyncio.gather(
        _previous_print_loop(),
        _v1016_board_loop(),
    )


scanner.v7.print_loop = _combined_v1016_print_loop
scanner.q.print_loop = _combined_v1016_print_loop
scanner.s.print_loop = _combined_v1016_print_loop

print(
    "Ψ-V10.16 UPGRADE ACTIVE — guaranteed 10-candidate opportunity board "
    "(3 pre-ignition + 3 breakout-ready + 4 closest-to-buy, truthful developing backfill), "
    "4x20 sharded live-micro websocket preserving untouched shard continuity; "
    "formal PRE/BUY gates unchanged",
    flush=True,
)


if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.16 ACTIVE — Top-10 Opportunity Board + sharded micro rotation; strict PRE/BUY unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.16 stopped", flush=True)
