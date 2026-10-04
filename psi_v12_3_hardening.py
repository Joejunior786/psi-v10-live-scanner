import asyncio
import json
import os
import time

import redis.asyncio as redis_async

CORE = None
AUTHORITY_CHAIN = "V12.3_LANES->V12.3_FAIL_CLOSED_AUTHORITY->BUY_NOW"
STICKY_KEY = os.getenv("PSI_MICRO_STICKY_KEY", "psi:v12:sticky-micro-pool").strip()
STRUCTURE_WORKERS = max(1, min(int(os.getenv("PSI_STRUCTURE_WORKERS", "2")), 8))
RISK_WORKERS = max(1, min(int(os.getenv("PSI_RISK_WORKERS", "2")), 8))
PRIORITY_SLOTS = 16
ROTATION_SLOTS = 2
ROTATION_PERIOD_S = 120.0
MAX_CHURN = 2
MIN_REBALANCE_S = 15.0

_last_rebalance = 0.0
_rotation_epoch = 0
_original_gate = None
_original_scan = None
_original_health = None


def _core():
    if CORE is None:
        raise RuntimeError("V12.3 hardening is not installed")
    return CORE


def _strict_micro_ready(symbol):
    core = _core()
    try:
        mm = core.app.micro_metrics(symbol) or {}
    except Exception:
        return False
    return bool(
        mm.get("micro_ready")
        and mm.get("sequence_verified")
        and mm.get("book_sequence_verified")
    )


def stable_micro_symbols():
    """Keep execution micro coverage sticky while preserving full discovery."""
    global _last_rebalance, _rotation_epoch

    core = _core()
    pool_size = int(core.REDIS_MICRO_POOL_SIZE)
    priority_slots = max(
        8, min(int(os.getenv("PSI_MICRO_PRIORITY_SLOTS", str(PRIORITY_SLOTS))), pool_size)
    )
    rotation_slots = max(
        1, min(int(os.getenv("PSI_MICRO_ROTATION_SLOTS", str(ROTATION_SLOTS))), pool_size)
    )
    rotation_period = max(
        60.0, float(os.getenv("PSI_MICRO_ROTATION_PERIOD_S", str(ROTATION_PERIOD_S)))
    )
    max_churn = max(
        1, min(int(os.getenv("PSI_MICRO_MAX_CHURN_PER_CYCLE", str(MAX_CHURN))), 12)
    )
    min_rebalance = max(
        5.0, float(os.getenv("PSI_MICRO_MIN_REBALANCE_S", str(MIN_REBALANCE_S)))
    )

    desired = []
    seen = set()

    def add_desired(symbol):
        symbol = str(symbol or "").upper()
        if symbol.endswith("USDT") and symbol not in seen:
            seen.add(symbol)
            desired.append(symbol)

    for symbol in list(getattr(core.app, "selected_micro_symbols", []) or []):
        add_desired(symbol)

    try:
        for row in core._board():
            add_desired(row.get("symbol"))
            if len(desired) >= max(pool_size * 2, 120):
                break
    except Exception:
        pass

    universe = list(getattr(core.q, "universe", []) or [])
    universe_set = set(universe)

    if len(desired) < pool_size:
        try:
            meta = getattr(core.app, "symbol_meta", {}) or {}
            liquid = sorted(
                universe,
                key=lambda sym: float(
                    (meta.get(sym, {}) or {}).get("quote_volume_24h", 0.0) or 0.0
                ),
                reverse=True,
            )
            for symbol in liquid:
                add_desired(symbol)
                if len(desired) >= pool_size:
                    break
        except Exception:
            pass

    current = [
        str(symbol).upper()
        for symbol in list(core._distributed_micro_sticky_pool or [])
        if str(symbol).upper().endswith("USDT")
    ]

    if not universe:
        if current:
            return current[:pool_size]
        core._distributed_micro_sticky_pool = desired[:pool_size]
        return list(core._distributed_micro_sticky_pool)

    current = [symbol for symbol in current if symbol in universe_set]
    if not current:
        core._distributed_micro_sticky_pool = [
            symbol for symbol in desired if symbol in universe_set
        ][:pool_size]
        _last_rebalance = time.monotonic()
        return list(core._distributed_micro_sticky_pool)

    now_mono = time.monotonic()
    can_rebalance = now_mono - _last_rebalance >= min_rebalance
    budget = max_churn if can_rebalance else 0

    warmed = [symbol for symbol in current if _strict_micro_ready(symbol)]
    warmed_set = set(warmed)
    warming = [symbol for symbol in current if symbol not in warmed_set]
    priority = [symbol for symbol in desired[:priority_slots] if symbol in universe_set]

    inject = []
    if budget:
        for symbol in priority:
            if symbol not in current and symbol not in inject:
                inject.append(symbol)
            if len(inject) >= budget:
                break

    epoch = int(time.time() / rotation_period)
    if can_rebalance and epoch != _rotation_epoch:
        _rotation_epoch = epoch
        rotation_budget = min(rotation_slots, max(0, budget - len(inject)))
        if rotation_budget:
            start = (epoch * rotation_budget) % len(universe)
            checked = 0
            while rotation_budget > 0 and checked < len(universe):
                symbol = universe[(start + checked) % len(universe)]
                checked += 1
                if symbol not in current and symbol not in inject:
                    inject.append(symbol)
                    rotation_budget -= 1

    out = []
    out_seen = set()

    def add_out(symbol):
        symbol = str(symbol or "").upper()
        if (
            symbol in universe_set
            and symbol.endswith("USDT")
            and symbol not in out_seen
            and len(out) < pool_size
        ):
            out_seen.add(symbol)
            out.append(symbol)

    for symbol in inject:
        add_out(symbol)
    for symbol in warmed:
        add_out(symbol)
    for symbol in warming:
        add_out(symbol)
    for symbol in desired:
        add_out(symbol)
    for symbol in universe:
        add_out(symbol)

    if inject:
        _last_rebalance = now_mono
        core._redis_bridge_stats["micro_last_churn"] = len(inject)
        core._redis_bridge_stats["micro_last_churn_ms"] = int(time.time() * 1000)

    core._redis_bridge_stats["micro_warmed_retained"] = sum(
        symbol in out_seen for symbol in warmed
    )
    core._distributed_micro_sticky_pool = out[:pool_size]
    return list(core._distributed_micro_sticky_pool)


def _heartbeat_ready(name, timestamp_key, count_key="symbols", max_age_ms=15000):
    core = _core()
    hb = core._redis_worker_health.get(name) or {}
    if not isinstance(hb, dict):
        return False
    try:
        timestamp = int(hb.get(timestamp_key) or 0)
        count = int(hb.get(count_key) or 0)
    except (TypeError, ValueError):
        return False
    age = int(time.time() * 1000) - timestamp if timestamp > 0 else 999999999
    return bool(
        count > 0
        and not str(hb.get("error") or "")
        and 0 <= age <= max_age_ms
    )


def authority_lane_health():
    core = _core()
    universe_count = len(list(getattr(core.q, "universe", []) or []))
    structure_ready = sum(
        _heartbeat_ready(f"structure{idx}", "heartbeat_ms", "assigned")
        for idx in range(STRUCTURE_WORKERS)
    )
    tape_ready = sum(
        _heartbeat_ready(f"tape{idx}", "last_event_ms")
        for idx in range(core.REDIS_TAPE_WORKERS)
    )
    risk_ready = sum(
        _heartbeat_ready(f"risk{idx}", "heartbeat_ms")
        for idx in range(RISK_WORKERS)
    )

    lanes = {
        "DISCOVERY": {"ready": universe_count > 0, "symbols": universe_count},
        "STRUCTURE": {
            "ready": structure_ready == STRUCTURE_WORKERS,
            "workers_ready": structure_ready,
            "workers_required": STRUCTURE_WORKERS,
        },
        "TAPE": {
            "ready": tape_ready == core.REDIS_TAPE_WORKERS,
            "workers_ready": tape_ready,
            "workers_required": core.REDIS_TAPE_WORKERS,
        },
        "TRADE": {"ready": _heartbeat_ready("trade", "last_event_ms")},
        "BOOK": {"ready": _heartbeat_ready("book", "last_event_ms")},
        "RISK": {
            "ready": risk_ready == RISK_WORKERS,
            "workers_ready": risk_ready,
            "workers_required": RISK_WORKERS,
        },
    }
    blockers = [name for name, row in lanes.items() if not bool(row.get("ready"))]
    return {
        "authority_version": core.VERSION,
        "all_ready": not blockers,
        "blockers": blockers,
        "lanes": lanes,
        "generated_ms": int(time.time() * 1000),
    }


def micro_coverage():
    core = _core()
    symbols = list(core._distributed_micro_sticky_pool[: core.REDIS_MICRO_POOL_SIZE])
    ready = 0
    verified = 0
    for symbol in symbols:
        try:
            mm = core.app.micro_metrics(symbol) or {}
        except Exception:
            continue
        is_ready = bool(mm.get("micro_ready"))
        is_verified = bool(
            is_ready
            and mm.get("sequence_verified")
            and mm.get("book_sequence_verified")
        )
        ready += int(is_ready)
        verified += int(is_verified)
    return {
        "selected": len(symbols),
        "target": core.REDIS_MICRO_POOL_SIZE,
        "micro_ready": ready,
        "micro_verified": verified,
    }


def _gate_wrapper(structural_row, legacy_row=None, micro_metrics=None, integrity=None):
    result = _original_gate(
        structural_row, legacy_row, micro_metrics, integrity
    )
    lane_health = authority_lane_health()
    blockers = list(result.get("blockers") or [])
    for lane in list(lane_health.get("blockers") or []):
        marker = f"AUTHORITY_LANE_{str(lane).upper()}_UNAVAILABLE"
        if marker not in blockers:
            blockers.append(marker)

    result = dict(result)
    result["blockers"] = blockers
    result["authority_chain"] = AUTHORITY_CHAIN
    result["authority_ready"] = bool(lane_health.get("all_ready"))
    if blockers:
        result["buy_now"] = False
        if isinstance(structural_row, dict) and structural_row.get("state") == "BUY":
            result["execution_state"] = "COLLECTING DATA"
        else:
            result["execution_state"] = "NOT_ELIGIBLE"
    return result


def _augment_response(response):
    core = _core()
    try:
        data = json.loads(response.body.decode("utf-8"))
    except Exception:
        return response

    lane_health = authority_lane_health()
    data["execution_authority"] = "V12.3_FAIL_CLOSED"
    data["authority_chain"] = AUTHORITY_CHAIN
    data["authority_lane_health"] = lane_health
    data["authority_ready"] = bool(lane_health.get("all_ready"))
    data["legacy_pinpoint_role"] = "INPUT_TELEMETRY_ONLY"
    distributed = data.setdefault("distributed_micro", {})
    distributed["coverage"] = micro_coverage()
    return core.app.web.json_response(data, status=response.status)


async def _scan_wrapper(request):
    return _augment_response(await _original_scan(request))


async def _health_wrapper(request):
    return _augment_response(await _original_health(request))


async def bootstrap():
    core = _core()
    if not core.REDIS_URL:
        return
    client = redis_async.from_url(
        core.REDIS_URL, encoding="utf-8", decode_responses=True
    )
    try:
        await client.ping()
        raw = await client.get(STICKY_KEY)
        if raw:
            try:
                payload = json.loads(raw)
            except Exception:
                payload = {}
            symbols = payload.get("symbols") if isinstance(payload, dict) else payload
            restored = []
            for symbol in symbols or []:
                symbol = str(symbol or "").upper()
                if symbol.endswith("USDT") and symbol not in restored:
                    restored.append(symbol)
                if len(restored) >= core.REDIS_MICRO_POOL_SIZE:
                    break
            if restored:
                core._distributed_micro_sticky_pool[:] = restored
                core._redis_bridge_stats["sticky_restored"] = len(restored)
                print(f"Ψ-V12.3 HARDENING restoredSticky={len(restored)}", flush=True)
    except Exception as exc:
        core._redis_bridge_stats["hardening_bootstrap_error"] = (
            f"{type(exc).__name__}: {exc}"
        )
    finally:
        await client.aclose()


async def supervisor_loop():
    core = _core()
    if not core.REDIS_URL:
        core._redis_bridge_stats["hardening_supervisor_disabled"] = 1
        return

    while True:
        client = None
        try:
            client = redis_async.from_url(
                core.REDIS_URL, encoding="utf-8", decode_responses=True
            )
            await client.ping()
            print(
                f"Ψ-V12.3 HARDENING supervisor structure={STRUCTURE_WORKERS} risk={RISK_WORKERS}",
                flush=True,
            )
            while True:
                now_ms = int(time.time() * 1000)
                await client.set(
                    STICKY_KEY,
                    json.dumps(
                        {
                            "version": core.VERSION,
                            "authority": "V12_ONLY",
                            "symbols": list(core._distributed_micro_sticky_pool),
                            "generated_ms": now_ms,
                        },
                        separators=(",", ":"),
                    ),
                    ex=86400,
                )

                for idx in range(STRUCTURE_WORKERS):
                    raw = await client.get(f"psi:v12:structure-worker:{idx}")
                    if raw:
                        try:
                            core._redis_worker_health[f"structure{idx}"] = json.loads(raw)
                        except Exception:
                            core._redis_worker_health[f"structure{idx}"] = {"raw": raw}

                for idx in range(RISK_WORKERS):
                    raw = await client.get(f"psi:v12:risk-worker:{idx}")
                    if raw:
                        try:
                            core._redis_worker_health[f"risk{idx}"] = json.loads(raw)
                        except Exception:
                            core._redis_worker_health[f"risk{idx}"] = {"raw": raw}

                core._redis_bridge_stats["hardening_last_ms"] = now_ms
                core._redis_bridge_stats["structure_workers_up"] = sum(
                    _heartbeat_ready(f"structure{idx}", "heartbeat_ms", "assigned")
                    for idx in range(STRUCTURE_WORKERS)
                )
                core._redis_bridge_stats["risk_workers_up"] = sum(
                    _heartbeat_ready(f"risk{idx}", "heartbeat_ms")
                    for idx in range(RISK_WORKERS)
                )
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            core._redis_bridge_stats["hardening_supervisor_errors"] += 1
            core._redis_bridge_stats["hardening_supervisor_last_error"] = (
                f"{type(exc).__name__}: {exc}"
            )
            await asyncio.sleep(1.5)
        finally:
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass


def install(core):
    global CORE, _original_gate, _original_scan, _original_health
    if CORE is not None:
        return
    CORE = core
    _original_gate = core._strict_execution_gate
    _original_scan = core.v12_scan
    _original_health = core.v12_health

    core._distributed_micro_symbols = stable_micro_symbols
    core._strict_execution_gate = _gate_wrapper
    core.EXECUTION_AUTHORITY_CHAIN = AUTHORITY_CHAIN
    core.v12_scan = _scan_wrapper
    core.v12_health = _health_wrapper
    core.app.scan_endpoint = _scan_wrapper
    core.app.health = _health_wrapper

    print(
        "Ψ-V12.3 HARDENING installed — sticky micro + lane health + fail-closed authority",
        flush=True,
    )
