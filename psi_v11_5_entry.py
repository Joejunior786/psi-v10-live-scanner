import asyncio, json, math, statistics, time, os, contextvars
from collections import defaultdict, Counter
import aiohttp
import psi_v11_4_entry as rescue
import psi_v11_2_2_entry as extrest
import psi_v11_3_1_entry as continuity_guard
import psi_v11_3_2_entry as move_engine
import stable10_app as stable_core
import target10_app as target_core
import qualifier_app as qualifier_core

base=rescue.base
tape=rescue.tape
app,q,scanner=base.app,base.q,base.scanner
VERSION="11.0.5.58-independent-feed-fallbacks"

# Discovery-breadth controls. These change research coverage/visibility only;
# Pinpoint and every mandatory BUY/risk gate remain fail-closed.
DISCOVERY_NEW_SLOTS = int(os.environ.get("PSI_DISCOVERY_NEW_SLOTS", "48"))
DISCOVERY_ROTATE_SLOTS = int(os.environ.get("PSI_DISCOVERY_ROTATE_SLOTS", "96"))
DISCOVERY_DISPLAY_SLOTS = int(os.environ.get("PSI_DISCOVERY_DISPLAY_SLOTS", "12"))
DISCOVERY_RECENT_CYCLES = int(os.environ.get("PSI_DISCOVERY_RECENT_CYCLES", "10"))
DISCOVERY_WATCH_COOLDOWN_CYCLES = int(os.environ.get("PSI_DISCOVERY_WATCH_COOLDOWN_CYCLES", "8"))
DISCOVERY_WATCH_PENALTY = float(os.environ.get("PSI_DISCOVERY_WATCH_PENALTY", "2.5"))
RECOVERY_ROTATION_SLOTS = int(os.environ.get("PSI_RECOVERY_ROTATION_SLOTS", "64"))
RECOVERY_ROTATION_PERIOD_S = float(os.environ.get("PSI_RECOVERY_ROTATION_PERIOD_S", "10"))

# Diagnostic-only early-momentum lane. This NEVER feeds formal state, the
# Monster deep pool, Pinpoint, RiskMap, BUY/PRE authority, or execution gates.
# It exists only to surface very-early 0-2/6 names before full confluence.
DARK_HORSE_SLOTS = int(os.environ.get("PSI_DARK_HORSE_SLOTS", "5"))
DARK_HORSE_MAX_LAYERS = int(os.environ.get("PSI_DARK_HORSE_MAX_LAYERS", "2"))
DARK_HORSE_MAX_TRADE_AGE_MS = float(os.environ.get("PSI_DARK_HORSE_MAX_TRADE_AGE_MS", "15000"))
DARK_HORSE_MIN_SCORE = float(os.environ.get("PSI_DARK_HORSE_MIN_SCORE", "18"))

# Hard live-integrity thresholds. Elevated states are fail-closed if any
# mandatory feed is outside these bounds.
INTEGRITY_STRUCTURE_MAX_AGE_S = float(os.environ.get("PSI_INTEGRITY_STRUCTURE_MAX_AGE_S", "120"))
# Integrity freshness is aligned with the native micro engine: aggTrade data
# remains live for 15s and depth/BBO for 5s. The event-tape lane is a separate
# fast signal and is only mandatory for states that explicitly depend on it.
INTEGRITY_TAPE_MAX_AGE_MS = float(os.environ.get("PSI_INTEGRITY_TAPE_MAX_AGE_MS", "5000"))
INTEGRITY_BBO_MAX_AGE_MS = float(os.environ.get("PSI_INTEGRITY_BBO_MAX_AGE_MS", "5000"))
INTEGRITY_MICRO_TRADE_MAX_AGE_MS = float(os.environ.get("PSI_INTEGRITY_MICRO_TRADE_MAX_AGE_MS", "15000"))
INTEGRITY_MICRO_BOOK_MAX_AGE_MS = float(os.environ.get("PSI_INTEGRITY_MICRO_BOOK_MAX_AGE_MS", "5000"))
INTEGRITY_RISK_MAX_AGE_S = float(os.environ.get("PSI_INTEGRITY_RISK_MAX_AGE_S", "90"))
INTEGRITY_CACHE_TTL_S = float(os.environ.get("PSI_INTEGRITY_CACHE_TTL_S", "0.25"))
_integrity_cache = {}

_discovery_cycle = 0
_discovery_cursor = 0
_discovery_last_seen = {}
_discovery_watch_streak = defaultdict(int)
_discovery_recent_promotions = {}

REST_BASES = [
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://data-api.binance.vision",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]
_rest_route_printed = False
_rest_global_gate = None
_rest_kline_gate = None
_rest_structure_gate = None
_rest_risk_kline_gate = None
_rest_bg_kline_gate = None
_rest_depth_gate = None
_structure_request_ctx = contextvars.ContextVar("psi_structure_request", default=False)
_risk_plan_request_ctx = contextvars.ContextVar("psi_risk_plan_request", default=False)
_structure_owner_ctx = contextvars.ContextVar("psi_v11_structure_owner", default=False)
_structure_watchdog_rest_ctx = contextvars.ContextVar("psi_v11_structure_watchdog_rest", default=False)
_structure_active = 0
_rest_good_host = {}
_rest_host_bad_until = {}
_rest_stats = {"ok":0,"fail":0,"attempt_fail":0,"failover":0,"host_ok":{},"host_fail":{},"gate_timeout":0,"risk_ok":0,"risk_fail":0,"risk_defer":0,"bg_ok":0,"bg_fail":0,"bg_defer":0}
_rest_last_fail = 0.0
_risk_last_ok = 0.0
_risk_last_fail = 0.0

WS_API_URL = "wss://ws-api.binance.com:443/ws-api/v3"
_ws_api_conn = None
_ws_api_ready = None
_ws_api_send_lock = None
_ws_api_gate = None
_ws_api_pending = {}
_ws_api_id = 0
_ws_api_stats = {
    "connects":0,"reconnects":0,"sent":0,"ok":0,"fail":0,"timeouts":0,
    "unavailable":0,"defer":0,"structure_ok":0,"risk_ok":0,"last_error":"",
}

def _rest_gates(path):
    global _rest_global_gate, _rest_kline_gate, _rest_structure_gate, _rest_risk_kline_gate, _rest_bg_kline_gate, _rest_depth_gate
    if _rest_global_gate is None:
        _rest_global_gate = asyncio.Semaphore(14)
    if _rest_kline_gate is None:
        _rest_kline_gate = asyncio.Semaphore(12)
    if _rest_structure_gate is None:
        _rest_structure_gate = asyncio.Semaphore(6)
    if _rest_risk_kline_gate is None:
        _rest_risk_kline_gate = asyncio.Semaphore(6)
    if _rest_bg_kline_gate is None:
        _rest_bg_kline_gate = asyncio.Semaphore(1)
    if _rest_depth_gate is None:
        _rest_depth_gate = asyncio.Semaphore(1)
    p=str(path)
    if "/klines" in p:
        return _rest_global_gate, _rest_kline_gate
    if "/depth" in p:
        return _rest_global_gate, _rest_depth_gate
    return _rest_global_gate, None

def _rest_lane(path):
    p=str(path)
    if "/klines" in p: return "klines"
    if "/depth" in p: return "depth"
    if "/ticker/24hr" in p: return "ticker24"
    if "/exchangeInfo" in p: return "exchange"
    return "other"

async def resilient_api_get(client, path, params=None):
    global _rest_route_printed
    p=str(path)
    lane=_rest_lane(p)
    is_structure = lane=="klines" and _structure_request_ctx.get()
    is_risk = lane=="klines" and (not is_structure) and _risk_plan_request_ctx.get()
    structure_symbol = str((params or {}).get("symbol") or "") if isinstance(params,dict) else ""
    route_key = (f"structure_klines:{structure_symbol}" if is_structure else ("risk_klines" if is_risk else ("background_klines" if lane=="klines" else lane)))

    if lane=="klines":
        timeout_s,max_hosts=(5.5,1) if is_structure else ((6.5,3) if is_risk else (5.5,3))
    elif lane=="depth":
        timeout_s,max_hosts=3.5,2
    elif lane=="ticker24":
        timeout_s,max_hosts=8.0,3
    else:
        timeout_s,max_hosts=5.0,3

    global_gate,lane_gate=_rest_gates(p)

    # Background risk-map/pullback candles have one reserved lane. They may
    # run alongside the three structure requests, but cannot fan out enough to
    # starve structural hydration.

    preferred=_rest_good_host.get(route_key)
    if is_structure:
        base_hosts=[
            "https://api.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://api4.binance.com",
            "https://data-api.binance.vision",
        ]
        if structure_symbol and not preferred:
            offset=sum(ord(ch) for ch in structure_symbol)%len(base_hosts)
            base_hosts=base_hosts[offset:]+base_hosts[:offset]
    elif lane in {"klines","depth","ticker24"}:
        base_hosts=[
            "https://data-api.binance.vision",
            "https://api.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
            "https://api3.binance.com",
            "https://api4.binance.com",
            "https://api-gcp.binance.com",
        ]
    else:
        base_hosts=list(REST_BASES)

    ordered=([preferred] if preferred else [])+[h for h in base_hosts if h!=preferred]
    now=time.time()
    healthy=[h for h in ordered if _rest_host_bad_until.get((route_key,h),0)<=now]
    hosts=(healthy or ordered)[:max_hosts]
    last_exc=None

    async def _request_once(host):
        acquired=False
        try:
            await asyncio.wait_for(global_gate.acquire(),timeout=2.0)
            acquired=True
            async with client.get(
                f"{host}{p}",
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_s,connect=min(1.6,timeout_s)),
            ) as response:
                body=await response.text()
                if response.status!=200:
                    raise RuntimeError(f"{host} HTTP {response.status}: {body[:180]}")
                return json.loads(body)
        finally:
            if acquired:
                global_gate.release()

    for idx,host in enumerate(hosts):
        lane_acquired=False
        risk_acquired=False
        bg_acquired=False
        try:
            if lane_gate is not None:
                if lane=="klines" and is_risk:
                    try:
                        await asyncio.wait_for(_rest_risk_kline_gate.acquire(),timeout=6.0)
                    except asyncio.TimeoutError:
                        _rest_stats["risk_defer"]+=1
                        raise
                    risk_acquired=True
                    try:
                        await asyncio.wait_for(lane_gate.acquire(),timeout=6.0)
                    except asyncio.TimeoutError:
                        _rest_stats["risk_defer"]+=1
                        raise
                    lane_acquired=True
                elif lane=="klines" and not is_structure:
                    try:
                        await asyncio.wait_for(_rest_bg_kline_gate.acquire(),timeout=8.0)
                    except asyncio.TimeoutError:
                        _rest_stats["bg_defer"]+=1
                        return []
                    bg_acquired=True
                    try:
                        await asyncio.wait_for(lane_gate.acquire(),timeout=8.0)
                    except asyncio.TimeoutError:
                        _rest_stats["bg_defer"]+=1
                        return []
                    lane_acquired=True
                else:
                    await asyncio.wait_for(lane_gate.acquire(),timeout=3.0)
                    lane_acquired=True

            payload=await _request_once(host)
            _rest_good_host[route_key]=host
            _rest_host_bad_until.pop((route_key,host),None)
            _rest_stats["ok"]+=1
            if lane=="klines" and is_risk:
                global _risk_last_ok
                _risk_last_ok=time.time()
                _rest_stats["risk_ok"]+=1
            elif lane=="klines" and not is_structure:
                _rest_stats["bg_ok"]+=1
            _rest_stats["host_ok"][host]=_rest_stats["host_ok"].get(host,0)+1
            if idx>0:
                _rest_stats["failover"]+=1
            app.rest_connected=True
            app.last_error=None
            if not _rest_route_printed:
                print(f"Ψ-REST ROUTE active={host} hosts={len(REST_BASES)} global=14 klines=12(structure<=6+risk<=6+background=1) depth=1 keepalive=ON",flush=True)
                _rest_route_printed=True
            return payload

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError as exc:
            last_exc=exc
            _rest_stats["attempt_fail"]+=1
            _rest_stats["gate_timeout"]+=1
            _rest_stats["host_fail"][host]=_rest_stats["host_fail"].get(host,0)+1
            _rest_host_bad_until[(route_key,host)]=time.time()+15.0
            await asyncio.sleep(.08)
        except Exception as exc:
            last_exc=exc
            _rest_stats["attempt_fail"]+=1
            _rest_stats["host_fail"][host]=_rest_stats["host_fail"].get(host,0)+1
            _rest_host_bad_until[(route_key,host)]=time.time()+15.0
            await asyncio.sleep(.08)
        finally:
            if lane_acquired:
                lane_gate.release()
            if risk_acquired:
                _rest_risk_kline_gate.release()
            if bg_acquired:
                _rest_bg_kline_gate.release()

    global _rest_last_fail
    _rest_stats["fail"]+=1
    if lane=="klines" and is_risk:
        global _risk_last_fail
        _risk_last_fail=time.time()
        _rest_stats["risk_fail"]+=1
    elif lane=="klines" and not is_structure:
        _rest_stats["bg_fail"]+=1
    _rest_last_fail=time.time()
    app.rest_connected=False
    app.last_error=f"REST_FAILOVER_FAIL {p}: {type(last_exc).__name__}: {last_exc}"
    try:
        safe_params={k:params.get(k) for k in ("symbol","interval","limit") if isinstance(params,dict) and k in params}
        print(f"Ψ-REST FAIL lane={route_key} path={p} params={safe_params} hosts={hosts} err={type(last_exc).__name__}:{last_exc}",flush=True)
    except Exception:
        pass
    raise RuntimeError(app.last_error)

# Replace the shared module-level REST function before any scanner loop starts.
app.api_get = resilient_api_get

def _ws_api_primitives():
    global _ws_api_ready, _ws_api_send_lock, _ws_api_gate
    if _ws_api_ready is None:
        _ws_api_ready = asyncio.Event()
    if _ws_api_send_lock is None:
        _ws_api_send_lock = asyncio.Lock()
    if _ws_api_gate is None:
        _ws_api_gate = asyncio.Semaphore(6)
    return _ws_api_ready, _ws_api_send_lock, _ws_api_gate

def _ws_api_fail_pending(reason):
    for rid,fut in list(_ws_api_pending.items()):
        if fut is not None and not fut.done():
            try:
                fut.set_exception(RuntimeError(reason))
            except Exception:
                pass
    _ws_api_pending.clear()

async def binance_ws_api_loop():
    global _ws_api_conn
    ready,_,_ = _ws_api_primitives()

    while app.session is None or getattr(app.session,"closed",True):
        await asyncio.sleep(.25)

    first=True
    while True:
        try:
            async with app.session.ws_connect(
                WS_API_URL,
                heartbeat=25,
                receive_timeout=90,
                max_msg_size=0,
            ) as ws:
                _ws_api_conn=ws
                ready.set()
                _ws_api_stats["connects"]+=1
                if not first:
                    _ws_api_stats["reconnects"]+=1
                first=False
                print(f"Ψ-WS-API connected url={WS_API_URL}",flush=True)

                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(msg.data)
                        except Exception:
                            continue
                        rid=str(payload.get("id") or "")
                        if not rid:
                            continue
                        fut=_ws_api_pending.pop(rid,None)
                        if fut is not None and not fut.done():
                            fut.set_result(payload)
                    elif msg.type in {aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.ERROR}:
                        raise RuntimeError(f"Binance WS API closed type={msg.type}")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _ws_api_stats["fail"]+=1
            _ws_api_stats["last_error"]=f"{type(exc).__name__}: {exc}"
            print(f"Ψ-WS-API ERROR {type(exc).__name__}: {exc}",flush=True)
        finally:
            ready.clear()
            _ws_api_conn=None
            _ws_api_fail_pending("Binance WS API connection reset")
        await asyncio.sleep(1.5)

async def binance_ws_api_klines(symbol, interval, limit, wait_ready=3.0, response_timeout=8.0, gate_timeout=2.5):
    global _ws_api_id
    ready,send_lock,gate=_ws_api_primitives()
    symbol=str(symbol).upper(); interval=str(interval); limit=int(limit)

    try:
        await asyncio.wait_for(ready.wait(),timeout=wait_ready)
    except asyncio.TimeoutError:
        _ws_api_stats["unavailable"]+=1
        return None

    acquired=False
    rid=None
    fut=None
    try:
        try:
            await asyncio.wait_for(gate.acquire(),timeout=gate_timeout)
            acquired=True
        except asyncio.TimeoutError:
            _ws_api_stats["defer"]+=1
            return None

        loop=asyncio.get_running_loop()
        async with send_lock:
            ws=_ws_api_conn
            if ws is None or ws.closed:
                _ws_api_stats["unavailable"]+=1
                return None
            _ws_api_id+=1
            rid=str(_ws_api_id)
            fut=loop.create_future()
            _ws_api_pending[rid]=fut
            await ws.send_json({
                "id":rid,
                "method":"klines",
                "params":{"symbol":symbol,"interval":interval,"limit":limit},
            })
            _ws_api_stats["sent"]+=1

        payload=await asyncio.wait_for(fut,timeout=response_timeout)
        status=int(payload.get("status") or 0) if isinstance(payload,dict) else 0
        rows=payload.get("result") if isinstance(payload,dict) else None
        if status==200 and isinstance(rows,list) and rows:
            _ws_api_stats["ok"]+=1
            return rows

        _ws_api_stats["fail"]+=1
        _ws_api_stats["last_error"]=f"status={status} payload={str(payload)[:220]}"
        return None

    except asyncio.TimeoutError:
        _ws_api_stats["timeouts"]+=1
        _ws_api_stats["fail"]+=1
        _ws_api_stats["last_error"]=f"timeout {symbol} {interval} {limit}"
        return None
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _ws_api_stats["fail"]+=1
        _ws_api_stats["last_error"]=f"{type(exc).__name__}: {exc}"
        return None
    finally:
        if rid is not None:
            _ws_api_pending.pop(rid,None)
        if acquired:
            gate.release()

# Structure-timeframe resilience. A structure build needs 1h + 4h + 15m.
# Cache only successful short-lived payloads, so if one sibling timeframe fails
# the next retry reuses the verified siblings and refetches only the missing one.
_original_load_klines = app.load_klines
_structure_tf_cache = {}
_structure_symbol_gates = {}
_structure_raw_cache = {}
_structure_raw_dirty = False
_structure_ws_latest = {}
_structure_ws_stats = {"connects":0,"events":0,"errors":0,"subscribed":0,"refresh_ok":0,"stale":0}
STRUCTURE_WS_STALE_S = 8.0
STRUCTURE_WS_SYNC_S = 12.0
STRUCTURE_RAW_CACHE_PATH = os.environ.get(
    "PSI_STRUCTURE_RAW_CACHE_PATH",
    "/data/psi_v11_structure_raw.json" if os.path.isdir("/data") else "/app/psi_v11_structure_raw.json",
)
STRUCTURE_TF_CACHE_S = 180.0
STRUCTURE_TF_RETRY_DELAY_S = 0.12
STRUCTURE_TF_ATTEMPTS = 1
STRUCTURE_RAW_MAX_INCREMENTAL_BARS = 48
_structure_tf_stats = {
    "cache_hit":0,"fetch_ok":0,"retry_ok":0,"fail":0,
    "raw_load":0,"raw_save":0,"raw_hit":0,"incremental_ok":0,"full_seed":0,"ws_refresh":0,"bar_reuse":0,
    "watchdog_rest_ok":0,"watchdog_rest_fail":0,
}

def _raw_key(symbol, interval, limit):
    return f"{symbol}|{interval}|{int(limit)}"

def _interval_ms(interval):
    return {"15m":900000,"1h":3600000,"4h":14400000}.get(str(interval),0)

def _minimum_structure_rows(interval):
    return 205 if str(interval) in {"1h","4h"} else 22

def _current_interval_open_ms(interval, now_ms=None):
    step=_interval_ms(interval)
    if step<=0:
        return 0
    now_ms=int(now_ms or time.time()*1000)
    return (now_ms//step)*step

def _live_binance_price(symbol):
    try:
        row=extrest.ext_cache.get(str(symbol)) or {}
        px=float(row.get("last") or 0.0)
        if math.isfinite(px) and px>0:
            return px
    except Exception:
        pass
    try:
        row=q.latest.get(str(symbol)) or {}
        px=float(row.get("price") or 0.0)
        if math.isfinite(px) and px>0:
            return px
    except Exception:
        pass
    return 0.0

def _reuse_current_candle(rows, symbol, interval):
    if not isinstance(rows,list) or len(rows)<_minimum_structure_rows(interval):
        return None
    try:
        last_open=int(rows[-1][0])
    except Exception:
        return None
    if last_open!=_current_interval_open_ms(interval):
        return None
    out=list(rows)
    last=list(out[-1])
    px=_live_binance_price(symbol)
    if px>0 and len(last)>=7:
        try:
            last[2]=str(max(float(last[2]),px))
            last[3]=str(min(float(last[3]),px))
            last[4]=str(px)
        except Exception:
            last[4]=str(px)
        out[-1]=last
    return out

def _bootstrap_structure_limit(interval, requested):
    if str(interval) in {"1h","4h"} and int(requested)>=205:
        return min(int(requested),220)
    if str(interval)=="15m" and int(requested)>=22:
        return min(int(requested),40)
    return int(requested)

def _load_structure_raw_cache():
    global _structure_raw_cache
    try:
        if not os.path.exists(STRUCTURE_RAW_CACHE_PATH):
            return 0
        with open(STRUCTURE_RAW_CACHE_PATH,"r",encoding="utf-8") as fh:
            payload=json.load(fh)
        rows=payload.get("rows") or {}
        clean={}
        for key,entry in rows.items():
            if not isinstance(entry,dict):
                continue
            data=entry.get("rows")
            if not isinstance(data,list) or not data:
                continue
            clean[str(key)]={"rows":data,"saved":float(entry.get("saved") or 0.0)}
        _structure_raw_cache=clean
        _structure_tf_stats["raw_load"]+=len(clean)
        print(f"Ψ-RECOVERY RAW_CACHE_LOAD entries={len(clean)} path={STRUCTURE_RAW_CACHE_PATH}",flush=True)
        return len(clean)
    except Exception as exc:
        print(f"Ψ-RECOVERY RAW_CACHE_LOAD_ERROR {type(exc).__name__}: {exc}",flush=True)
        return 0

def _save_structure_raw_cache():
    global _structure_raw_dirty
    if not _structure_raw_dirty:
        return 0
    try:
        tmp=STRUCTURE_RAW_CACHE_PATH+".tmp"
        payload={"version":VERSION,"saved_at":time.time(),"rows":_structure_raw_cache}
        with open(tmp,"w",encoding="utf-8") as fh:
            json.dump(payload,fh,separators=(",",":"))
        os.replace(tmp,STRUCTURE_RAW_CACHE_PATH)
        _structure_tf_stats["raw_save"]+=1
        _structure_raw_dirty=False
        return len(_structure_raw_cache)
    except Exception as exc:
        print(f"Ψ-RECOVERY RAW_CACHE_SAVE_ERROR {type(exc).__name__}: {exc}",flush=True)
        return 0

def _merge_kline_rows(seed, fresh, keep):
    merged={}
    for row in list(seed or [])+list(fresh or []):
        try:
            if row and len(row)>=7:
                merged[int(row[0])]=row
        except Exception:
            continue
    return [merged[k] for k in sorted(merged.keys())][-int(keep):]

def _raw_seed_count(symbol):
    symbol=str(symbol)
    return sum(
        1 for interval,limit in (("1h",260),("4h",260),("15m",80))
        if isinstance((_structure_raw_cache.get(_raw_key(symbol,interval,limit)) or {}).get("rows"),list)
    )

def _structure_symbol_gate(symbol, interval=None):
    # Lock only identical symbol/timeframe refreshes. 1h/4h/15m for the same
    # symbol must run concurrently because app.load_structure() requires all
    # three mandatory timeframes in one bounded build.
    key=(str(symbol),str(interval or "*"))
    gate=_structure_symbol_gates.get(key)
    if gate is None:
        gate=asyncio.Semaphore(1)
        _structure_symbol_gates[key]=gate
    return gate

STRUCTURE_RACE_HOSTS = [
    "https://data-api.binance.vision",
    "https://api-gcp.binance.com",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]

async def _structure_fetch_race(client, symbol, interval, limit):
    symbol=str(symbol); interval=str(interval); limit=int(limit)
    route_key=f"structure_klines:{symbol}"
    hosts=list(STRUCTURE_RACE_HOSTS)
    preferred=_rest_good_host.get(route_key)
    small_15m = interval=="15m" and limit<=10
    if small_15m:
        # Small 15m rollovers are the most time-sensitive structure request.
        # Keep a deterministic market-data-first order rather than rotating by symbol.
        priority=[
            "https://data-api.binance.vision",
            "https://api-gcp.binance.com",
            "https://api.binance.com",
            "https://api1.binance.com",
            "https://api2.binance.com",
        ]
        hosts=priority+[h for h in hosts if h not in priority]
        if preferred in hosts:
            hosts=[preferred]+[h for h in hosts if h!=preferred]
    elif preferred in hosts:
        hosts=[preferred]+[h for h in hosts if h!=preferred]
    elif symbol:
        offset=sum(ord(ch) for ch in symbol)%len(hosts)
        hosts=hosts[offset:]+hosts[:offset]

    now=time.time()
    healthy=[h for h in hosts if _rest_host_bad_until.get((route_key,h),0)<=now]
    ordered=(healthy+[h for h in hosts if h not in healthy])[:4]
    timeout_s=3.0 if limit<=10 else 4.0
    global_gate,lane_gate=_rest_gates("/api/v3/klines")

    async def one(host):
        g=l=s=False
        try:
            await asyncio.wait_for(_rest_structure_gate.acquire(),timeout=2.5);s=True
            await asyncio.wait_for(global_gate.acquire(),timeout=2.5);g=True
            await asyncio.wait_for(lane_gate.acquire(),timeout=2.5);l=True
            async with client.get(
                f"{host}/api/v3/klines",
                params={"symbol":symbol,"interval":interval,"limit":limit},
                timeout=aiohttp.ClientTimeout(total=timeout_s,connect=min(1.8,timeout_s)),
            ) as response:
                body=await response.text()
                if response.status!=200:
                    raise RuntimeError(f"{host} HTTP {response.status}: {body[:160]}")
                payload=json.loads(body)
                if not isinstance(payload,list) or not payload:
                    raise RuntimeError(f"{host} empty kline payload")
                return host,payload
        finally:
            if l: lane_gate.release()
            if g: global_gate.release()
            if s: _rest_structure_gate.release()

    last_exc=None
    for round_idx in range(0,len(ordered),2):
        pair=ordered[round_idx:round_idx+2]
        tasks=[asyncio.create_task(one(host)) for host in pair]
        try:
            for fut in asyncio.as_completed(tasks):
                try:
                    host,payload=await fut
                    for t in tasks:
                        if not t.done(): t.cancel()
                    await asyncio.gather(*tasks,return_exceptions=True)
                    _rest_good_host[route_key]=host
                    _rest_host_bad_until.pop((route_key,host),None)
                    _rest_stats["ok"]+=1
                    _rest_stats["host_ok"][host]=_rest_stats["host_ok"].get(host,0)+1
                    if round_idx>0: _rest_stats["failover"]+=1
                    app.rest_connected=True
                    app.last_error=None
                    return payload
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_exc=exc
            # Both hosts in the pair failed.
            for host in pair:
                _rest_stats["attempt_fail"]+=1
                _rest_stats["host_fail"][host]=_rest_stats["host_fail"].get(host,0)+1
                _rest_host_bad_until[(route_key,host)]=time.time()+15.0
        finally:
            for t in tasks:
                if not t.done(): t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)

    _rest_stats["fail"]+=1
    app.rest_connected=False
    app.last_error=f"STRUCTURE_RACE_FAIL {symbol} {interval} {limit}: {type(last_exc).__name__}: {last_exc}"
    print(
        f"Ψ-REST FAIL lane=structure_race:{symbol} path=/api/v3/klines "
        f"params={{'symbol':'{symbol}','interval':'{interval}','limit':{limit}}} err={type(last_exc).__name__}:{last_exc}",
        flush=True,
    )
    return None

def _structure_ws_seeded_symbols():
    try:
        scope=_recovery_scope() if "_recovery_scope" in globals() else list(getattr(q,"universe",[]) or [])
    except Exception:
        scope=list(getattr(q,"universe",[]) or [])
    return [s for s in scope if _raw_seed_count(s)>=3][:RECOVERY_PRIORITY if "RECOVERY_PRIORITY" in globals() else 80]

def _structure_ws_row(sym, interval):
    item=_structure_ws_latest.get((str(sym),str(interval)))
    if not item:
        return None
    ts,row=item
    age=max(0.0,time.time()-float(ts))
    if age>STRUCTURE_WS_STALE_S:
        _structure_ws_stats["stale"]+=1
        return None
    return row

async def structure_kline_ws_loop():
    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)

    msg_id=1000
    while True:
        try:
            url=f"{app.WS_BASE}/ws"
            async with app.session.ws_connect(url,heartbeat=None,receive_timeout=90,max_msg_size=0) as ws:
                _structure_ws_stats["connects"]+=1
                subscribed=set()
                last_sync=0.0
                print(f"Ψ-STRUCTURE-WS connected url={app.WS_BASE}",flush=True)

                while True:
                    now=time.time()
                    if now-last_sync>=STRUCTURE_WS_SYNC_S:
                        syms=_structure_ws_seeded_symbols()
                        desired={
                            f"{str(sym).lower()}@kline_{tf}"
                            for sym in syms
                            for tf in ("15m","1h","4h")
                        }
                        add=sorted(desired-subscribed)
                        rem=sorted(subscribed-desired)

                        for chunk_start in range(0,len(add),100):
                            chunk=add[chunk_start:chunk_start+100]
                            if chunk:
                                msg_id+=1
                                await ws.send_json({"method":"SUBSCRIBE","params":chunk,"id":msg_id})
                        for chunk_start in range(0,len(rem),100):
                            chunk=rem[chunk_start:chunk_start+100]
                            if chunk:
                                msg_id+=1
                                await ws.send_json({"method":"UNSUBSCRIBE","params":chunk,"id":msg_id})
                        subscribed=desired
                        _structure_ws_stats["subscribed"]=len(subscribed)
                        last_sync=now

                    try:
                        msg=await asyncio.wait_for(ws.receive(),timeout=2.0)
                    except asyncio.TimeoutError:
                        continue

                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(msg.data)
                        except Exception:
                            continue
                        data=payload.get("data") if isinstance(payload,dict) and isinstance(payload.get("data"),dict) else payload
                        if not isinstance(data,dict) or str(data.get("e") or "")!="kline":
                            continue
                        k=data.get("k") or {}
                        sym=str(data.get("s") or "").upper()
                        interval=str(k.get("i") or "")
                        if not sym or interval not in {"15m","1h","4h"}:
                            continue
                        try:
                            row=[
                                int(k.get("t")),str(k.get("o")),str(k.get("h")),str(k.get("l")),
                                str(k.get("c")),str(k.get("v")),int(k.get("T")),str(k.get("q")),
                                int(k.get("n") or 0),str(k.get("V")),str(k.get("Q")),"0"
                            ]
                        except Exception:
                            continue
                        _structure_ws_latest[(sym,interval)]=(time.time(),row)
                        _structure_ws_stats["events"]+=1

                    elif msg.type in {aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.ERROR}:
                        raise RuntimeError(f"structure kline websocket closed type={msg.type}")

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _structure_ws_stats["errors"]+=1
            print(f"Ψ-STRUCTURE-WS ERROR {type(exc).__name__}: {exc}",flush=True)
            await asyncio.sleep(2.0)

async def _structure_historical_klines(client, symbol, interval, limit):
    # Watchdog rescue is an independent route. If the normal WS-API structure
    # lane is stalled, rescue goes directly to the bounded multi-host REST race
    # instead of repeating the same failed request path.
    if _structure_watchdog_rest_ctx.get():
        rows=await _structure_fetch_race(client,symbol,interval,limit)
        if isinstance(rows,list) and rows:
            _structure_tf_stats["watchdog_rest_ok"]+=1
        else:
            _structure_tf_stats["watchdog_rest_fail"]+=1
        return rows

    # Normal hydration remains WS-API first with REST host-racing fallback.
    rows=await binance_ws_api_klines(symbol,interval,limit,wait_ready=2.5,response_timeout=7.0)
    if isinstance(rows,list) and rows:
        _ws_api_stats["structure_ok"]+=1
        return rows
    return await _structure_fetch_race(client,symbol,interval,limit)

async def _structure_resilient_load_klines(client, symbol, interval, limit):
    global _structure_raw_dirty
    if not _structure_request_ctx.get():
        return await _original_load_klines(client, symbol, interval, limit)

    symbol=str(symbol); interval=str(interval); limit=int(limit)
    key=(symbol,interval,limit)
    now=time.time()
    cached=_structure_tf_cache.get(key)
    if cached and now-float(cached[0])<=STRUCTURE_TF_CACHE_S:
        reusable=_reuse_current_candle(cached[1],symbol,interval)
        if reusable is not None:
            _structure_tf_stats["cache_hit"]+=1
            _structure_tf_stats["bar_reuse"]+=1
            return reusable

    raw_key=_raw_key(symbol,interval,limit)
    raw_entry=_structure_raw_cache.get(raw_key) or {}
    seed=raw_entry.get("rows") if isinstance(raw_entry,dict) else None
    if isinstance(seed,list) and seed:
        _structure_tf_stats["raw_hit"]+=1

        # Preferred live path: historical seed + Binance kline WebSocket current
        # candle. This keeps the mandatory structure state current without REST.
        wsrow=_structure_ws_row(symbol,interval)
        if len(seed)>=_minimum_structure_rows(interval) and wsrow is not None:
            merged=_merge_kline_rows(seed,[wsrow],limit)
            if len(merged)>=_minimum_structure_rows(interval):
                _structure_tf_cache[key]=(time.time(),merged)
                _structure_raw_cache[raw_key]={"rows":merged,"saved":time.time()}
                _structure_raw_dirty=True
                _structure_tf_stats["ws_refresh"]+=1
                _structure_ws_stats["refresh_ok"]+=1
                return merged

        reusable=_reuse_current_candle(seed,symbol,interval)
        if reusable is not None:
            _structure_tf_cache[key]=(time.time(),reusable)
            _structure_tf_stats["bar_reuse"]+=1
            return reusable

    async with _structure_symbol_gate(symbol,interval):
        cached=_structure_tf_cache.get(key)
        if cached and time.time()-float(cached[0])<=STRUCTURE_TF_CACHE_S:
            reusable=_reuse_current_candle(cached[1],symbol,interval)
            if reusable is not None:
                _structure_tf_stats["cache_hit"]+=1
                _structure_tf_stats["bar_reuse"]+=1
                return reusable

        raw_entry=_structure_raw_cache.get(raw_key) or {}
        seed=raw_entry.get("rows") if isinstance(raw_entry,dict) else None

        wsrow=_structure_ws_row(symbol,interval)
        if isinstance(seed,list) and len(seed)>=_minimum_structure_rows(interval) and wsrow is not None:
            merged=_merge_kline_rows(seed,[wsrow],limit)
            if len(merged)>=_minimum_structure_rows(interval):
                _structure_tf_cache[key]=(time.time(),merged)
                _structure_raw_cache[raw_key]={"rows":merged,"saved":time.time()}
                _structure_raw_dirty=True
                _structure_tf_stats["ws_refresh"]+=1
                _structure_ws_stats["refresh_ok"]+=1
                return merged

        request_limit=_bootstrap_structure_limit(interval,limit)
        incremental=False

        if isinstance(seed,list) and len(seed)>=_minimum_structure_rows(interval):
            try:
                last_open=int(seed[-1][0])
            except Exception:
                last_open=0
            step=_interval_ms(interval)
            bars_behind=max(0,int(math.ceil(max(0.0,(time.time()*1000-last_open))/step))) if step>0 and last_open>0 else STRUCTURE_RAW_MAX_INCREMENTAL_BARS+1
            if bars_behind<=STRUCTURE_RAW_MAX_INCREMENTAL_BARS:
                request_limit=max(3,min(limit,bars_behind+2))
                incremental=True

        rows=None
        for attempt in range(STRUCTURE_TF_ATTEMPTS):
            rows=await _structure_historical_klines(client, symbol, interval, request_limit)
            if isinstance(rows,list) and rows:
                if attempt==0:
                    _structure_tf_stats["fetch_ok"]+=1
                else:
                    _structure_tf_stats["retry_ok"]+=1
                break
            if attempt+1<STRUCTURE_TF_ATTEMPTS:
                await asyncio.sleep(STRUCTURE_TF_RETRY_DELAY_S)

        if not isinstance(rows,list) or not rows:
            _structure_tf_stats["fail"]+=1
            print(
                f"Ψ-STRUCTURE-TF FAIL {symbol} tf={interval} req={request_limit}/{limit} "
                f"incremental={int(incremental)} cacheHits={_structure_tf_stats['cache_hit']} "
                f"rawHits={_structure_tf_stats['raw_hit']} retryOK={_structure_tf_stats['retry_ok']} "
                f"fail={_structure_tf_stats['fail']}",
                flush=True,
            )
            return None

        merged=_merge_kline_rows(seed if incremental else [],rows,limit)
        if len(merged)<_minimum_structure_rows(interval):
            # An old/incomplete persisted series is never promoted as live structure.
            if incremental:
                full_limit=_bootstrap_structure_limit(interval,limit)
                full=await _structure_historical_klines(client,symbol,interval,full_limit)
                if isinstance(full,list) and full:
                    merged=_merge_kline_rows([],full,limit)
                    incremental=False
            if len(merged)<_minimum_structure_rows(interval):
                _structure_tf_stats["fail"]+=1
                return None

        _structure_tf_cache[key]=(time.time(),merged)
        _structure_raw_cache[raw_key]={"rows":merged,"saved":time.time()}
        _structure_raw_dirty=True
        if incremental:
            _structure_tf_stats["incremental_ok"]+=1
        else:
            _structure_tf_stats["full_seed"]+=1
        return merged

app.load_klines = _structure_resilient_load_klines

RISK_RACE_HOSTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]
_risk_tf_cache = {}
RISK_TF_CACHE_S = 120.0
RISK_WS_ATTEMPTS = 2
RISK_WS_RETRY_DELAY_S = 0.18
_risk_tf_stats = {"cache_hit":0,"ws_ok":0,"ws_retry_ok":0,"rest_ok":0,"fail":0}

async def _risk_fetch_race(client, symbol, interval, limit):
    """Secondary REST fallback for RiskMap with a hard wall-clock budget.

    Only the two best hosts are raced. RiskMap is execution support, so it must
    never monopolise REST/klines capacity or block the main scanner while a
    remote host is slow.
    """
    global _risk_last_ok, _risk_last_fail
    symbol=str(symbol); interval=str(interval); limit=int(limit)
    key=(symbol,interval,limit)
    now=time.time()
    cached=_risk_tf_cache.get(key)
    if cached and now-float(cached[0])<=RISK_TF_CACHE_S:
        _risk_tf_stats["cache_hit"]+=1
        return cached[1]

    route_key=f"risk_klines:{symbol}:{interval}"
    hosts=list(RISK_RACE_HOSTS)
    preferred=_rest_good_host.get(route_key)
    if preferred in hosts:
        hosts=[preferred]+[h for h in hosts if h!=preferred]
    elif symbol and hosts:
        offset=(sum(ord(ch) for ch in symbol)+sum(ord(ch) for ch in interval))%len(hosts)
        hosts=hosts[offset:]+hosts[:offset]

    healthy=[h for h in hosts if _rest_host_bad_until.get((route_key,h),0)<=now]
    ordered=(healthy+[h for h in hosts if h not in healthy])[:2]
    global_gate,lane_gate=_rest_gates("/api/v3/klines")
    last_exc=None

    async def one(host):
        g=l=r=False
        try:
            await asyncio.wait_for(_rest_risk_kline_gate.acquire(),timeout=.55);r=True
            await asyncio.wait_for(global_gate.acquire(),timeout=.55);g=True
            await asyncio.wait_for(lane_gate.acquire(),timeout=.55);l=True
            async with client.get(
                f"{host}/api/v3/klines",
                params={"symbol":symbol,"interval":interval,"limit":limit},
                timeout=aiohttp.ClientTimeout(total=2.2,connect=.75),
            ) as response:
                body=await response.text()
                if response.status!=200:
                    raise RuntimeError(f"{host} HTTP {response.status}: {body[:160]}")
                payload=json.loads(body)
                if not isinstance(payload,list) or not payload:
                    raise RuntimeError(f"{host} empty risk kline payload")
                return host,payload
        finally:
            if l: lane_gate.release()
            if g: global_gate.release()
            if r: _rest_risk_kline_gate.release()

    def _consume_child_result(task):
        # Retrieve terminal exceptions from raced/cancelled aiohttp children so
        # asyncio does not emit "Task exception was never retrieved" noise.
        if task.cancelled():
            return
        try:
            task.exception()
        except BaseException:
            pass

    tasks=[]
    for host in ordered:
        task=asyncio.create_task(one(host))
        task.add_done_callback(_consume_child_result)
        tasks.append(task)
    try:
        try:
            for fut in asyncio.as_completed(tasks,timeout=3.0):
                try:
                    host,payload=await fut
                    _rest_good_host[route_key]=host
                    _rest_host_bad_until.pop((route_key,host),None)
                    _rest_stats["ok"]+=1
                    _rest_stats["risk_ok"]+=1
                    _rest_stats["host_ok"][host]=_rest_stats["host_ok"].get(host,0)+1
                    _risk_last_ok=time.time()
                    app.rest_connected=True
                    app.last_error=None
                    _risk_tf_cache[key]=(time.time(),payload)
                    _risk_tf_stats["rest_ok"]+=1
                    return payload
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    last_exc=exc
        except asyncio.TimeoutError as exc:
            last_exc=exc
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
        if tasks:
            try:
                await asyncio.wait(tasks,timeout=.20)
            except Exception:
                pass

    for host in ordered:
        _rest_stats["attempt_fail"]+=1
        _rest_stats["host_fail"][host]=_rest_stats["host_fail"].get(host,0)+1
        _rest_host_bad_until[(route_key,host)]=time.time()+10.0
    _rest_stats["fail"]+=1
    _rest_stats["risk_fail"]+=1
    _risk_last_fail=time.time()
    app.rest_connected=False
    app.last_error=f"RISK_RACE_FAIL {symbol} {interval} {limit}: {type(last_exc).__name__}: {last_exc}"
    _risk_tf_stats["fail"]+=1
    return []


async def _risk_load_klines(client, symbol, interval, limit):
    """Reliable bounded WS-first RiskMap candle loader.

    Production telemetry showed the Binance WS-API path can return valid
    RiskMap candles when allowed a realistic response window. RiskMap now
    gives one WS request enough time to succeed, while using a short gate
    acquisition so it cannot queue behind structure work indefinitely.
    A two-host REST race remains the bounded fallback.
    """
    global _risk_last_ok
    symbol=str(symbol); interval=str(interval); limit=int(limit)
    key=(symbol,interval,limit)
    cached=_risk_tf_cache.get(key)
    if cached and time.time()-float(cached[0])<=RISK_TF_CACHE_S:
        _risk_tf_stats["cache_hit"]+=1
        return cached[1]

    if _ws_api_ready is not None and _ws_api_ready.is_set():
        rows=await binance_ws_api_klines(
            symbol,interval,limit,
            wait_ready=.75,
            response_timeout=3.5,
            gate_timeout=.75,
        )
        if isinstance(rows,list) and rows:
            _risk_tf_cache[key]=(time.time(),rows)
            _risk_last_ok=time.time()
            _ws_api_stats["risk_ok"]+=1
            _risk_tf_stats["ws_ok"]+=1
            return rows

    # Bounded public-REST fallback if WS is unavailable, deferred, or fails.
    try:
        rows=await _risk_fetch_race(client, symbol, interval, limit)
    except asyncio.CancelledError:
        raise
    except Exception:
        rows=None
    if isinstance(rows,list) and rows:
        return rows

    _risk_tf_stats["fail"]+=1
    return []

# V10.19.1 resolves this attribute at call time. Risk-plan candles use their
# own bounded host races and cannot be crowded out by structure/background work.
app.load_risk_klines = _risk_load_klines
_original_load_structure = app.load_structure

async def _priority_load_structure(client, symbol):
    global _structure_active
    # Single-owner rule: only the V11 recovery scheduler may hit Binance REST
    # for historical structure. Legacy loops receive fresh cached structure or
    # None and therefore cannot create duplicate REST bursts.
    if not _structure_owner_ctx.get():
        sd=getattr(app,"structure",{}).get(symbol)
        age=_structure_age_recovery(symbol) if "_structure_age_recovery" in globals() else 999999.0
        if isinstance(sd,dict) and age<=RECOVERY_STALE_S if "RECOVERY_STALE_S" in globals() else False:
            return sd
        return None
    token = _structure_request_ctx.set(True)
    _structure_active += 1
    try:
        return await _original_load_structure(client, symbol)
    finally:
        _structure_active = max(0, _structure_active - 1)
        _structure_request_ctx.reset(token)

app.load_structure = _priority_load_structure

# The execution book now comes from Binance depth20 WebSocket snapshots.
# The legacy REST depth bootstrap remains dormant.
app.DEPTH_SNAPSHOT_LIMIT = 20

BOARD_ROWS=30

_old_deep=base.deep
_old_candidate=base.candidate

def f(v,d=0.0):
    try:x=float(v)
    except (TypeError,ValueError):return d
    return x if math.isfinite(x) else d

def cl(v,a=0.0,b=1.0):return max(a,min(b,v))

def _gate_state(row,key):
    aliases=(key,key.lower(),key.upper())
    for bucket in ("pinpoint_hard_status","hard_safety_status"):
        d=row.get(bucket) or {}
        if isinstance(d,dict):
            for k in aliases:
                if k in d:
                    v=d[k]
                    if v is True or str(v).upper()=="PASS":return 1.0
                    if v is False or str(v).upper() in {"FAIL","BLOCK","BLOCKED"}:return 0.0
    blockers=rescue._row_blockers(row)
    if key in blockers:return 0.0
    return 0.5

def _hist_prices(sym,window=180.0):
    now=time.time();h=list(base.price_hist.get(sym) or [])
    return [(t,p) for t,p in h if p>0 and t>=now-window]

def _range_pct(xs):
    if len(xs)<2:return None
    vals=[p for _,p in xs if p>0]
    if len(vals)<2:return None
    lo=min(vals);hi=max(vals);mid=(hi+lo)/2
    return ((hi-lo)/mid*100) if mid>0 else None

def _micro_structure(sym):
    xs=_hist_prices(sym,180.0)
    if len(xs)<16:return {"score":0.5,"hh":0,"hl":0,"range15":None,"range60":None,"compression":0.5}
    now=time.time();x15=[x for x in xs if x[0]>=now-15];x60=[x for x in xs if x[0]>=now-60]
    r15=_range_pct(x15);r60=_range_pct(x60);comp=.5
    if r15 is not None and r60 is not None and r60>1e-9:
        ratio=r15/r60;tight=cl((0.55-ratio)/0.45);abs_tight=cl((0.45-r15)/0.40);comp=.65*tight+.35*abs_tight
    chunks=[];n=len(xs)
    for i in range(4):
        a=int(i*n/4);b=max(a+1,int((i+1)*n/4));vals=[p for _,p in xs[a:b]]
        if vals:chunks.append((max(vals),min(vals)))
    hh=hl=0
    for i in range(1,len(chunks)):
        hh+=int(chunks[i][0]>=chunks[i-1][0]);hl+=int(chunks[i][1]>=chunks[i-1][1])
    denom=max(1,2*(len(chunks)-1));score=(hh+hl)/denom
    return {"score":score,"hh":hh,"hl":hl,"range15":r15,"range60":r60,"compression":comp}

def _bsi(sym,row,d):
    ms=_micro_structure(sym);dist=d.get("dist");mtxt=str(d.get("mtf") or "")
    mtf=1.0 if "ALIGNED" in mtxt else .65 if "MIXED-BULL" in mtxt else .5
    trend=.55*mtf+.45*ms["score"]
    attacks=cl(f(d.get("attacks"))/5);fatigue=cl(f(d.get("fatigue"))/100);tests=.58*fatigue+.42*attacks
    setup=str(row.get("pinpoint_setup") or "");compression=ms["compression"]
    if "COMPRESSION" in setup:compression=max(compression,.90)
    vac=cl(f(d.get("vac"),50)/100);ask=cl(max(f(d.get("askdep")),0)/20);liquidity=.68*vac+.32*ask
    if dist is None:proximity=.35
    elif -0.35<=dist<=0.75:proximity=1.0-cl(abs(dist)/1.1)
    elif 0.75<dist<=2.0:proximity=cl(1-(dist-.75)/1.25)*.65
    elif -1.0<=dist<-.35:proximity=.45
    else:proximity=.10
    retest=.25
    if "RETEST" in setup:retest=.85
    if dist is not None and -.25<=dist<=.25:retest=max(retest,.75)
    if str(row.get("pinpoint_entry_status") or "")=="PINPOINT_TRIGGERED":retest=max(retest,.65)
    buy=f(row.get("aggressive_buy_ratio"),.5);cvd=f(row.get("cvd_acceleration"),f(row.get("pinpoint_aggressive_delta"),2*buy-1))
    live_tape=cl(f(row.get("pinpoint_live_tape_score"),50)/100);event=cl(f(d.get("event"))/100);flow=cl(f(d.get("flow"),50)/100)
    confirm=.24*live_tape+.22*event+.18*flow+.18*cl((buy-.5)/.25)+.18*cl((cvd+.02)/.30)
    fresh=_gate_state(row,"FRESH_STRUCTURE");ma=_gate_state(row,"MA_STRUCTURE_LAYER")
    structure_quality=.30*trend+.18*tests+.14*compression+.12*liquidity+.10*proximity+.08*fresh+.08*ma
    extension_ok=row.get("pinpoint_anti_chase_ok");room=.70 if extension_ok is True else .45 if extension_ok is None else .15
    r60=abs(f(row.get("return_60s_pct"),0))
    if r60>2.5:room*=.55
    false_break=0.0
    if dist is not None and dist<0:
        false_break+=.24*(1-cl((buy-.45)/.25));false_break+=.24*(1-cl((cvd+.05)/.30));false_break+=.18*(1-event);false_break+=.14*(1-flow)
    spread=f(row.get("spread_bps"),f(row.get("event_spread_bps"),0));false_break+=.10*cl((spread-3)/12);false_break+=.10*(1-fresh);false_break=cl(false_break)
    score=100*cl(.34*structure_quality+.16*compression+.12*liquidity+.12*proximity+.12*retest+.10*confirm+.04*room-.16*false_break)
    if score>=78 and false_break<=.35 and proximity>=.55:state="BSI-BREAKOUT-READY"
    elif score>=68 and compression>=.60 and proximity>=.40:state="BSI-COILED"
    elif score>=66 and retest>=.70:state="BSI-RETEST"
    elif score>=55:state="BSI-DEVELOPING"
    else:state="BSI-WEAK"
    return {"bsi":score,"bsiState":state,"bsiTrend":100*trend,"bsiTests":100*tests,"bsiCompression":100*compression,"bsiLiquidity":100*liquidity,"bsiProximity":100*proximity,"bsiRetest":100*retest,"bsiConfirm":100*confirm,"bsiRoom":100*room,"falseBreakRisk":100*false_break,"freshStructure":fresh,"maStructure":ma,"microHH":ms["hh"],"microHL":ms["hl"],"range15":ms["range15"],"range60":ms["range60"]}

def deep_v5(sym):
    d=dict(_old_deep(sym));row=q.latest.get(sym) or {};d.update(_bsi(sym,row,d));return d
base.deep=deep_v5

def candidate_v5(sym,row,c,d):
    out=_old_candidate(sym,row,c,d)
    for k in ("bsi","bsiState","bsiTrend","bsiTests","bsiCompression","bsiLiquidity","bsiProximity","bsiRetest","bsiConfirm","bsiRoom","falseBreakRisk","freshStructure","maStructure","microHH","microHL","range15","range60"):out[k]=d.get(k)
    feats=dict(out.get("features") or {});feats.update({"bsi_n":cl(f(d.get("bsi"))/100),"bsi_trend_n":cl(f(d.get("bsiTrend"))/100),"bsi_tests_n":cl(f(d.get("bsiTests"))/100),"bsi_compression_n":cl(f(d.get("bsiCompression"))/100),"bsi_retest_n":cl(f(d.get("bsiRetest"))/100),"bsi_false_break_n":cl(f(d.get("falseBreakRisk"))/100)});out["features"]=feats
    reasons=list(out.get("reasons") or []);st=str(d.get("bsiState") or "")
    if st=="BSI-BREAKOUT-READY":reasons.append("BSI_BREAKOUT_READY")
    elif st=="BSI-COILED":reasons.append("BSI_COILED")
    elif st=="BSI-RETEST":reasons.append("BSI_RETEST")
    if f(d.get("bsiTests"))>=65:reasons.append("STRUCTURE_MULTI_TEST")
    if f(d.get("bsiCompression"))>=70:reasons.append("STRUCTURE_COMPRESSION")
    if f(d.get("falseBreakRisk"))>=55:reasons.append("FALSE_BREAK_RISK")
    out["reasons"]=list(dict.fromkeys(reasons))
    bsi_bonus=max(0.0,(f(d.get("bsi"))-60.0)*.18)
    if f(d.get("falseBreakRisk"))>=55:bsi_bonus*=.35
    out["retentionScore"]=cl(f(out.get("retentionScore"))+min(6.0,bsi_bonus),0,100)

    # Final integrity overlay is deliberately applied after every inherited
    # scorer. It may only downgrade elevated labels; it can never promote.
    try:
        require_event_tape=str(out.get("state") or "") in {
            "MONSTER-HOT","MONSTER-IGNITION","MONSTER-RESCUE","MONSTER-SEED"
        }
        integ=_integrity_status(sym,row,require_event_tape=require_event_tape)
    except Exception:
        integ={"verified":False,"blockers":["INTEGRITY_CHECK_ERROR"],"ages":{}}
    out["integrityVerified"]=bool(integ.get("verified"))
    out["integrityBlockers"]=list(integ.get("blockers") or [])
    out["integrityAges"]=dict(integ.get("ages") or {})
    out["integrityRawFormal"]=out.get("formal")
    out["integrityRawState"]=out.get("state")
    if not out["integrityVerified"]:
        if str(out.get("formal") or "") in {"PRE-IGNITION","BUY NOW"}:
            out["formal"]="COLLECTING DATA"
        if str(out.get("state") or "") in {"MONSTER-HOT","MONSTER-IGNITION"}:
            out["state"]="MONSTER-WATCH"
        rr=list(out.get("reasons") or [])
        rr.append("DATA_INTEGRITY_WAIT")
        out["reasons"]=list(dict.fromkeys(rr))
    return out
base.candidate=candidate_v5

def _live_pullback_exhaustion(sym,ca):
    # Pullback exhaustion needs native live micro plus a recent event-tape/BBO
    # observation, but does not require the ultra-fast 1.5s Monster tape-ready
    # condition or a pre-existing Monster execution label.
    integ=_integrity_status(sym,q.latest.get(sym) or {},require_event_tape=False)
    tm=tape.tape_metric(sym)
    event_live=(
        f(tm.get("age_ms"),999999)<=INTEGRITY_TAPE_MAX_AGE_MS
        and f(tm.get("book_age_ms"),999999)<=INTEGRITY_BBO_MAX_AGE_MS
        and int(f(tm.get("trades_5s")))>=1
    )
    if not bool(integ.get("verified")) or not event_live:
        return {"state":"DATA_WAIT","depth":0.0,"rebound":0.0,"score":0.0}
    h=base.price_hist.get(sym)
    if not h or len(h)<8:
        return {"state":"NONE","depth":0.0,"rebound":0.0,"score":0.0}
    now=time.time()
    pts=[(t,p) for t,p in h if now-t<=180 and p>0]
    if len(pts)<6:
        pts=list(h)[-40:]
    vals=[p for _,p in pts if p>0]
    if len(vals)<4:
        return {"state":"NONE","depth":0.0,"rebound":0.0,"score":0.0}
    cur=vals[-1]; hi=max(vals); lo=min(vals)
    depth=((hi-cur)/hi*100) if hi>0 else 0.0
    rebound=((cur-lo)/lo*100) if lo>0 else 0.0
    buy=f(ca.get("buy1s"),.5); cvd=f(ca.get("cvd1s")); tape_score=f(ca.get("eventTape"))
    layers=int(f(ca.get("layers"))); bsi=f(ca.get("bsi")); reasons=set(ca.get("reasons") or [])
    structure_ok=layers>=3 or bsi>=52
    pulled=.12<=depth<=5.0
    buyer_return=(buy>=.58 and cvd>=.10) or ("OFI_POS" in reasons and buy>=.54) or (tape_score>=65 and buy>=.55)
    reclaim=rebound>=.05
    exhausting=pulled and structure_ok and ((buy>=.52 and cvd>=-.05) or "OFI_POS" in reasons)
    exhausted=pulled and structure_ok and buyer_return and reclaim
    score=0.0
    if pulled:
        score+=min(28.0,8.0+depth*8.0)
    score+=min(18.0,max(0.0,(buy-.50)*90.0))
    score+=min(16.0,max(0.0,cvd*16.0))
    score+=min(15.0,tape_score*.15)
    score+=min(12.0,layers*2.0)
    score+=min(8.0,rebound*18.0)
    if "OFI_POS" in reasons: score+=5.0
    state="PULLBACK_EXHAUSTED" if exhausted else "SELL_PRESSURE_EXHAUSTING" if exhausting else "PULLBACK_ONLY" if pulled else "NONE"
    return {
        "state":state,
        "depth":round(depth,3),
        "rebound":round(rebound,3),
        "score":round(cl(score,0,100),1),
        "buy":buy,"cvd":cvd,"tape":tape_score,
    }

def _balanced_monster_board(candidates):
    """Select up to BOARD_ROWS meaningful candidates across distinct lanes.

    The full candidate universe remains in _all_candidates. This function only
    controls display visibility; it never changes formal state, layers, risk
    plans, Pinpoint authority, or execution gates.
    """
    rows=list(candidates or [])
    state_rank={
        "MONSTER-HOT":7,
        "MONSTER-IGNITION":6,
        "MONSTER-MEMORY":5,
        "MONSTER-RESCUE":4,
        "MONSTER-SEED":3,
        "MONSTER-EXTENDED":2,
        "MONSTER-WATCH":1,
    }

    def rank(r):
        formal=str(r.get("formal") or "")
        state=str(r.get("state") or "")
        pb=str(r.get("monsterPullbackState") or "")
        layers=int(f(r.get("layers")))
        tape=f(r.get("eventTape"))
        cvd=f(r.get("cvd1s"))
        buy=f(r.get("buy1s"),.5)
        return (
            1 if formal=="PRE-IGNITION" else 0,
            state_rank.get(state,0),
            2 if pb=="PULLBACK_EXHAUSTED" else 1 if pb=="SELL_PRESSURE_EXHAUSTING" else 0,
            layers,
            f(r.get("bsi")),
            f(r.get("retentionScore")),
            f(r.get("early")),
            tape,
            cvd,
            buy,
            f(r.get("dna")),
            f(r.get("peak")),
        )

    selected=[]
    seen=set()
    lane_counts=defaultdict(int)

    def add_lane(name,pred,quota):
        pool=sorted((r for r in rows if pred(r)),key=rank,reverse=True)
        for r in pool:
            if len(selected)>=BOARD_ROWS or lane_counts[name]>=quota:
                break
            sym=str(r.get("symbol") or "")
            if not sym or sym in seen:
                continue
            r["displayLane"]=name
            selected.append(r);seen.add(sym);lane_counts[name]+=1

    # Reserve capacity so one crowded lane cannot hide another. Discovery is
    # deliberately first: it guarantees fresh full-universe names can be seen
    # without changing their formal state or execution authority.
    add_lane("DISCOVERY",lambda r:(
        bool(r.get("discoveryFresh"))
        and (
            int(f(r.get("layers")))>=2
            or f(r.get("eventTape"))>=40
            or f(r.get("early"))>=28
            or f(r.get("bsi"))>=48
        )
    ),DISCOVERY_DISPLAY_SLOTS)
    add_lane("PRE",lambda r:str(r.get("formal") or "")=="PRE-IGNITION" and int(f(r.get("layers")))>=4,8)
    add_lane("MONSTER",lambda r:str(r.get("state") or "") in {
        "MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED"
    } and int(f(r.get("layers")))>=3,7)
    add_lane("PULLBACK",lambda r:str(r.get("monsterPullbackState") or "") in {
        "PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING"
    } and int(f(r.get("layers")))>=3,5)
    add_lane("NEAR5",lambda r:int(f(r.get("layers")))>=5,6)
    add_lane("EARLY",lambda r:str(r.get("formal") or "")=="EARLY OPPORTUNITY" and int(f(r.get("layers")))>=3,5)

    # Early anomaly lane: permit lower-layer names only when live tape itself
    # is exceptional. This avoids generic 0/6 activity noise.
    add_lane("ANOMALY",lambda r:(
        int(f(r.get("layers")))>=2
        and f(r.get("eventTape"))>=72
        and f(r.get("buy1s"),.5)>=.62
        and f(r.get("cvd1s"))>=.20
    ),4)

    # Fill any remaining slots with the strongest meaningful candidates.
    def meaningful(r):
        layers=int(f(r.get("layers")))
        formal=str(r.get("formal") or "")
        state=str(r.get("state") or "")
        pb=str(r.get("monsterPullbackState") or "")
        return (
            layers>=3
            or formal in {"PRE-IGNITION","EARLY OPPORTUNITY"}
            or (state in {"MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED"} and layers>=2)
            or pb in {"PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING"}
            or f(r.get("bsi"))>=58
        )

    for r in sorted((r for r in rows if meaningful(r)),key=rank,reverse=True):
        if len(selected)>=BOARD_ROWS:
            break
        sym=str(r.get("symbol") or "")
        if not sym or sym in seen:
            continue
        r["displayLane"]=r.get("displayLane") or "BEST"
        selected.append(r);seen.add(sym)

    # Never pad the visible board with generic 0/6 fallback rows.
    return selected[:BOARD_ROWS]

def scan_v5():
    global _discovery_cycle,_discovery_cursor
    _discovery_cycle += 1
    cycle=_discovery_cycle
    now=time.time();u=list(getattr(q,"universe",[]) or []);rows=[]
    for sym in u:
        row=q.latest.get(sym) or {};p=base.px(sym,row)
        if p>0 and (not base.price_hist[sym] or now-base.price_hist[sym][-1][0]>=.45):base.price_hist[sym].append((now,p))
        c=base.cheap(sym,row,now);em,rs=rescue._emergency_promote(c,row);c["rescue_score"]=rs;c["rescue_promote"]=em
        rows.append((f(c.get("cheap")),rs,sym,row,c))

    # Repeated WATCH names gradually lose discovery priority unless they have
    # materially stronger live evidence. This prevents stale pool lock-in.
    def discovery_rank(x):
        a,rs,s,row,c=x
        streak=int(_discovery_watch_streak.get(s,0))
        penalty=DISCOVERY_WATCH_PENALTY*max(0,streak-DISCOVERY_WATCH_COOLDOWN_CYCLES)
        live_bonus=min(12.0,max(0.0,f(c.get("peak"))-100.0)*.08)+min(8.0,max(0.0,f(c.get("r60")))*2.0)
        return (a+live_bonus-penalty,rs)

    rows.sort(reverse=True,key=discovery_rank)
    pool=[(a,s,r,c) for a,_,s,r,c in rows[:base.DEEP_LIMIT]]
    seen={s for _,s,_,_ in pool}
    row_by_sym={x[2]:x for x in rows}

    # Fair-universe reservation: high-ranked/rescue names are not allowed to
    # consume the entire deep pool before novel + round-robin symbols enter.
    # This changes research coverage only; formal BUY/PRE gates remain intact.
    fair_reserve=min(
        max(0,rescue.MAX_DEEP_POOL-int(base.DEEP_LIMIT)),
        max(0,DISCOVERY_NEW_SLOTS)+max(0,DISCOVERY_ROTATE_SLOTS),
    )
    priority_cap=max(int(base.DEEP_LIMIT),rescue.MAX_DEEP_POOL-fair_reserve)

    # Preserve genuine high-velocity exceptions.
    for a,rs,s,r,c in rows:
        if s not in seen and (f(c.get("peak"))>=100 or f(c.get("radar_n"))>=.45 or f(c.get("r60"))>=.75):
            if len(pool)>=priority_cap: break
            pool.append((a,s,r,c));seen.add(s)

    emergency=sorted(
        [x for x in rows if x[2] not in seen and x[4].get("rescue_promote")],
        key=lambda x:(x[1],x[0]),reverse=True
    )[:rescue.EXTRA_RESCUE_SLOTS]
    for a,rs,s,r,c in emergency:
        if len(pool)>=priority_cap:break
        pool.append((a,s,r,c));seen.add(s);rescue.rescue_stats["emergency_promotions"]+=1

    promoted={}

    # Novelty quota: every cycle reserve deep-analysis capacity for symbols
    # that have not been in the recent deep pool. They are still ranked by
    # current live anomaly strength, never promoted to a formal signal.
    novel=[
        x for x in rows
        if x[2] not in seen
        and cycle-int(_discovery_last_seen.get(x[2],-10_000))>DISCOVERY_RECENT_CYCLES
    ]
    novel.sort(key=discovery_rank,reverse=True)
    for a,rs,s,r,c in novel[:DISCOVERY_NEW_SLOTS]:
        if len(pool)>=rescue.MAX_DEEP_POOL:break
        pool.append((a,s,r,c));seen.add(s);promoted[s]="NOVEL"

    # Forced circular exploration guarantees eventual coverage of the whole
    # Binance universe even when the same high-liquidity names dominate score.
    if u and len(pool)<rescue.MAX_DEEP_POOL:
        start_idx=_discovery_cursor%len(u)
        inspected=0;added=0
        while inspected<len(u) and added<DISCOVERY_ROTATE_SLOTS and len(pool)<rescue.MAX_DEEP_POOL:
            s=u[(start_idx+inspected)%len(u)];inspected+=1
            if s in seen:continue
            x=row_by_sym.get(s)
            if not x:continue
            a,rs,_,r,c=x
            pool.append((a,s,r,c));seen.add(s);promoted[s]="ROTATE";added+=1
        _discovery_cursor=(start_idx+max(inspected,DISCOVERY_ROTATE_SLOTS))%len(u)

    for _,s,_,_ in pool:
        _discovery_last_seen[s]=cycle
    for s,lane in promoted.items():
        _discovery_recent_promotions[s]=(cycle,lane)
    for s,(cy,lane) in list(_discovery_recent_promotions.items()):
        if cycle-cy>DISCOVERY_RECENT_CYCLES:
            _discovery_recent_promotions.pop(s,None)

    out=[]
    for _,s,row,c in pool:
        ca=base.candidate(s,row,c,base.deep(s))
        ex=_live_pullback_exhaustion(s,ca)
        ca.update({
            "monsterPullbackState":ex["state"],
            "monsterPullbackDepth":ex["depth"],
            "monsterPullbackRebound":ex["rebound"],
            "monsterExhaustionScore":ex["score"],
            "discoveryFresh":s in promoted,
            "discoveryLane":promoted.get(s),
            "discoveryCycle":cycle if s in promoted else None,
        })
        if ex["state"]=="PULLBACK_EXHAUSTED":
            rs=list(ca.get("reasons") or [])
            if "PULLBACK_SELL_EXHAUSTED" not in rs: rs.append("PULLBACK_SELL_EXHAUSTED")
            ca["reasons"]=rs
        elif ex["state"]=="SELL_PRESSURE_EXHAUSTING":
            rs=list(ca.get("reasons") or [])
            if "SELL_PRESSURE_EXHAUSTING" not in rs: rs.append("SELL_PRESSURE_EXHAUSTING")
            ca["reasons"]=rs

        formal=str(ca.get("formal") or "")
        state=str(ca.get("state") or "")
        if formal=="WATCH" and state=="MONSTER-WATCH":
            _discovery_watch_streak[s]+=1
        elif formal in {"PRE-IGNITION","EARLY OPPORTUNITY"} or state in {"MONSTER-HOT","MONSTER-IGNITION","MONSTER-RESCUE","MONSTER-MEMORY","MONSTER-SEED"}:
            _discovery_watch_streak[s]=0
        else:
            _discovery_watch_streak[s]=max(0,int(_discovery_watch_streak.get(s,0))-1)
        ca["watchStreak"]=int(_discovery_watch_streak.get(s,0))
        ca["watchCooldown"]=bool(
            ca["watchStreak"]>=DISCOVERY_WATCH_COOLDOWN_CYCLES
            and int(f(ca.get("layers")))<5
            and f(ca.get("eventTape"))<72
            and f(ca.get("bsi"))<70
        )

        base.latest[s]=ca
        discovery_visible=bool(ca.get("discoveryFresh")) and (
            int(f(ca.get("layers")))>=2
            or f(ca.get("eventTape"))>=40
            or f(ca.get("early"))>=28
            or f(ca.get("bsi"))>=48
        )
        visible=(
            f(ca.get("early"))>=38
            or f(ca.get("dna"))>=50
            or f(ca.get("peak"))>=120
            or ca.get("state") in {"MONSTER-RESCUE","MONSTER-MEMORY"}
            or f(ca.get("retentionScore"))>=58
            or f(ca.get("bsi"))>=62
            or ex["state"] in {"PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING"}
            or discovery_visible
        )
        if visible:
            out.append(ca);base.open_obs(ca)

    priority={"MONSTER-HOT":6,"MONSTER-IGNITION":5,"MONSTER-MEMORY":4,"MONSTER-RESCUE":3,"MONSTER-SEED":2,"MONSTER-EXTENDED":1,"MONSTER-WATCH":0}
    out.sort(
        key=lambda x:(
            priority.get(str(x.get("state")),0),
            0 if x.get("watchCooldown") else 1,
            1 if x.get("discoveryFresh") else 0,
            f(x.get("bsi")),f(x.get("retentionScore")),f(x.get("early")),f(x.get("dna")),f(x.get("peak"))
        ),
        reverse=True
    )
    base.stats["cycles"]+=1;base.stats["universe"]=len(u);base.stats["deep"]=len(pool);base.stats["cand"]=len(out);rescue.rescue_stats["last_pool"]=len(pool);rescue.rescue_stats["last_emergency"]=len(emergency)
    base.latest["_all_candidates"]=list(out)
    fair_window=max(2,math.ceil(len(u)/max(1,DISCOVERY_ROTATE_SLOTS))+1) if u else 0
    fair_recent=sum(1 for s in u if cycle-int(_discovery_last_seen.get(s,-10_000))<=fair_window) if u else 0
    base.latest["_discovery_stats"]={
        "cycle":cycle,"novel":sum(1 for v in promoted.values() if v=="NOVEL"),
        "rotated":sum(1 for v in promoted.values() if v=="ROTATE"),
        "recent":len(_discovery_recent_promotions),"cursor":_discovery_cursor,
        "fairReserve":fair_reserve,"fairRecent":fair_recent,"fairUniverse":len(u),"fairWindow":fair_window,
    }
    board=_balanced_monster_board(out)
    base.latest["_display_mix"]=dict(Counter(str(r.get("displayLane") or "BEST") for r in board))
    return board
base.scan=scan_v5

def _fmt_px(v):
    x=f(v)
    if x<=0:return "-"
    if x>=1000:return f"{x:.2f}"
    if x>=1:return f"{x:.6f}".rstrip("0").rstrip(".")
    if x>=.01:return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")

def _candidate_move_plan(r):
    sym=str(r.get("symbol") or "")
    row=q.latest.get(sym) or {}
    current=base.px(sym,row)
    conditional=f(row.get("breakout_entry_trigger"),f(row.get("entry_trigger")))
    ref_entry=conditional if conditional>0 else current
    shadow=move_engine.expected_move_shadow(sym,ref_entry)

    plans=[]
    # A live Pinpoint trigger/stop is already an execution risk plan. Convert
    # it into deterministic R-multiple targets so RiskMap outages cannot erase
    # an otherwise valid execution plan.
    pin_entry=f(row.get("pinpoint_trigger"))
    pin_stop=f(row.get("pinpoint_stop"))
    pin_risk=f(row.get("pinpoint_risk_pct"))
    pin_status=str(row.get("pinpoint_entry_status") or "")
    if pin_entry>0 and pin_stop>0 and pin_stop<pin_entry and pin_risk>0 and pin_status in {"PINPOINT_ARMED","PINPOINT_TRIGGERED"}:
        rr=pin_entry-pin_stop
        pin_updated=f(row.get("updated_ms"))/1000.0 if f(row.get("updated_ms"))>0 else time.time()
        pin_plan={
            "updated":pin_updated,
            "entry_trigger":pin_entry,
            "stop_loss":pin_stop,
            "tp1":pin_entry+rr,
            "tp2":pin_entry+2.0*rr,
            "tp3":pin_entry+3.0*rr,
            "plan_state":"PINPOINT_R_PLAN",
        }
        plans.append((pin_updated,"PINPOINT_R",pin_plan))
    try:
        ri=move_engine.riskmap.risk_intel(sym)
        if isinstance(ri,dict):
            en=f(ri.get("entry_trigger"));st=f(ri.get("stop_loss"))
            if en>0 and st>0 and st<en:
                plans.append((f(ri.get("updated")), "RISKMAP", ri))
    except Exception: pass
    try:
        pb=move_engine.pullback.pb_intel(sym)
        if isinstance(pb,dict):
            en=f(pb.get("entry"));st=f(pb.get("stop"))
            if en>0 and st>0 and st<en:
                plans.append((f(pb.get("updated")), "PULLBACK", pb))
    except Exception: pass

    source="SHADOW_ONLY";plan={}
    risk_state="NOT_TRACKED"
    try:
        ri_state=move_engine.riskmap.risk_intel(sym)
        if isinstance(ri_state,dict):
            risk_state=str(ri_state.get("plan_state") or "WAIT")
    except Exception:
        pass
    if plans:
        _,source,plan=max(plans,key=lambda z:z[0])
        risk_state=str(plan.get("plan_state") or plan.get("state") or risk_state)
    entry=f(plan.get("entry_trigger"),f(plan.get("entry"),conditional))
    stop=f(plan.get("stop_loss"),f(plan.get("stop")))
    tp1=f(plan.get("tp1"));tp2=f(plan.get("tp2"));tp3=f(plan.get("tp3"))
    runner=f(plan.get("runner_reference"))
    plan_updated=f(plan.get("updated"))
    plan_age=(time.time()-plan_updated) if plan_updated>0 else 999999.0
    integ=_integrity_status(sym,row)
    valid=(
        entry>0 and stop>0 and stop<entry and tp1>entry and tp2>tp1 and tp3>tp2
        and bool(integ.get("verified"))
        and plan_age<=INTEGRITY_RISK_MAX_AGE_S
    )
    return {
        "source":source,
        "risk_state":risk_state,
        "valid":valid,
        "integrity_verified":bool(integ.get("verified")),
        "integrity_blockers":list(integ.get("blockers") or []),
        "plan_age_s":plan_age,
        "entry":entry if entry>0 else conditional,
        "stop":stop if valid else 0.0,
        "tp1":tp1 if valid else 0.0,
        "tp2":tp2 if valid else 0.0,
        "tp3":tp3 if valid else 0.0,
        "runner":runner if valid and runner>tp3 else 0.0,
        "shadow":shadow,
    }

def _monster_risk_priority():
    rows=list(base.latest.get("_all_candidates") or [])
    by_sym={str(r.get("symbol") or ""):r for r in rows if str(r.get("symbol") or "")}

    state_rank={
        "MONSTER-HOT":6,
        "MONSTER-IGNITION":5,
        "MONSTER-RESCUE":4,
        "MONSTER-MEMORY":3,
        "MONSTER-SEED":2,
        "MONSTER-EXTENDED":1,
        "MONSTER-WATCH":0,
    }

    def row_score(r):
        formal=str(r.get("formal") or "")
        state=str(r.get("state") or "")
        layers=int(f(r.get("layers")))
        fresh=1 if _structure_age_recovery(str(r.get("symbol") or ""))<=RECOVERY_STALE_S else 0
        serious=1 if (
            formal=="PRE-IGNITION"
            or state in {"MONSTER-HOT","MONSTER-IGNITION","MONSTER-RESCUE","MONSTER-MEMORY"}
            or layers>=4
        ) else 0
        return (
            fresh,
            serious,
            1 if formal=="PRE-IGNITION" else 0,
            state_rank.get(state,0),
            layers,
            f(r.get("bsi")),
            f(r.get("eventTape")),
            f(r.get("early")),
        )

    ordered=[];seen=set()

    # First claim goes to symbols that the formal/Pinpoint engine is actively
    # preparing for execution. This prevents cold-start discovery symbols from
    # consuming every RiskMap slot before real setups receive a plan.
    live_rows=[]
    for sym,row in list(q.latest.items()):
        if not isinstance(row,dict) or not str(sym).endswith("USDT"):
            continue
        formal=str(row.get("formal_state") or row.get("state") or "")
        pstate=str(row.get("pinpoint_state") or "")
        estatus=str(row.get("pinpoint_entry_status") or "")
        active=(
            formal in {"BUY NOW","PRE-IGNITION","EARLY OPPORTUNITY"}
            or pstate in {"BUY NOW","PINPOINT ARMED","SETUP READY"}
            or estatus in {"PINPOINT_ARMED","PINPOINT_TRIGGERED"}
            or bool(row.get("pinpoint_setup"))
        )
        if active:
            live_rows.append((
                1 if formal=="BUY NOW" else 0,
                1 if estatus=="PINPOINT_TRIGGERED" else 0,
                1 if formal=="PRE-IGNITION" else 0,
                f(row.get("pinpoint_live_tape_score")),
                f(row.get("score")),
                str(sym),
            ))
    for *_,sym in sorted(live_rows,reverse=True):
        if sym not in seen:
            ordered.append(sym);seen.add(sym)

    # Then serious Monster/structure candidates, strongest first.
    for r in sorted(rows,key=row_score,reverse=True):
        sym=str(r.get("symbol") or "")
        if not sym or sym in seen:
            continue
        layers=int(f(r.get("layers")))
        state=str(r.get("state") or "")
        formal=str(r.get("formal") or "")
        if (
            layers>=3
            or formal=="PRE-IGNITION"
            or state in {"MONSTER-HOT","MONSTER-IGNITION","MONSTER-RESCUE","MONSTER-MEMORY"}
        ):
            ordered.append(sym);seen.add(sym)

    # Execution-pool symbols come next. ASCII fallbacks are preferred only as
    # a scheduler efficiency measure; any non-ASCII symbol that becomes a real
    # formal/Monster setup is already admitted by the two lanes above.
    for sym in list(getattr(app,"selected_micro_symbols",[]) or []):
        sym=str(sym)
        if sym and sym not in seen and sym.isascii():
            ordered.append(sym);seen.add(sym)

    # Only fill spare slots from weaker discovery names.
    for r in sorted(rows,key=row_score,reverse=True):
        sym=str(r.get("symbol") or "")
        if sym and sym not in seen and sym.isascii():
            ordered.append(sym);seen.add(sym)
    return ordered

move_engine.riskmap.priority_symbols_provider = _monster_risk_priority

def _bsi_learning():
    src=[x for x in base.resolved if isinstance(x,dict) and isinstance(x.get("features"),dict) and "bsi_n" in x["features"]]
    if not src:return {"status":"WARMING","n":0}
    hi=[x for x in src if f(x["features"].get("bsi_n"))>=.70]
    return {"status":"ACTIVE" if len(src)>=30 else "WARMING","n":len(src),"high":len(hi),"p10":(sum(f(x.get("max_return_pct"))>=10 for x in hi)/len(hi)) if hi else None,"p20":(sum(f(x.get("max_return_pct"))>=20 for x in hi)/len(hi)) if hi else None,"mfe":statistics.mean(f(x.get("max_return_pct")) for x in hi) if hi else None}

def _monster_tape_health():
    strict=0
    trade_fresh=0
    book_fresh=0
    syms=list(getattr(q,'universe',[]) or [])
    for sym in syms:
        tm=tape.tape_metric(sym)
        if tm.get('ready'):
            strict+=1
        if f(tm.get('age_ms'),999999)<=5000 and int(f(tm.get('trades_5s')))>=1:
            trade_fresh+=1
        if f(tm.get('book_age_ms'),999999)<=5000:
            book_fresh+=1
    return strict,trade_fresh,book_fresh

def _dark_horse_board(exclude_symbols=None):
    """Read-only full-universe early-momentum ranking.

    This lane is intentionally isolated from every execution/state machine.
    It consumes already-existing market telemetry and returns diagnostics only.
    """
    picks=[]
    exclude_symbols=set(exclude_symbols or [])
    now=time.time()
    for sym in list(getattr(q,"universe",[]) or []):
        try:
            if sym in exclude_symbols:
                continue
            row=q.latest.get(sym) or {}

            # Fast prefilter: do not run the heavier tape metric when this
            # symbol has not had any recent aggregate trades. For this
            # diagnostic lane only, live aggTrade is also an allowed price
            # source so a symbol can surface before the main q.latest cache
            # has hydrated it.
            dq=getattr(tape,"trade_events",{}).get(sym)
            if not dq:
                continue
            price=base.px(sym,row)
            if price<=0:
                price=f(dq[-1][1],0)
            if price<=0:
                continue
            trade_age_ms=(now-f(dq[-1][0],0))*1000.0
            if trade_age_ms>DARK_HORSE_MAX_TRADE_AGE_MS:
                continue

            layers=int(base.layers(row))
            formal=str(row.get("formal_state") or row.get("state") or "")
            if layers>DARK_HORSE_MAX_LAYERS or formal in {"BUY NOW","PRE-IGNITION"}:
                continue

            tm=tape.tape_metric(sym) or {}
            age=f(tm.get("age_ms"),999999)
            book_age=f(tm.get("book_age_ms"),999999)
            if age>DARK_HORSE_MAX_TRADE_AGE_MS:
                continue

            buy=f(tm.get("buy_ratio_1s"),.5)
            cvd=f(tm.get("cvd_1s"))
            nacc=f(tm.get("notional_accel_1s"))
            cacc=f(tm.get("trade_count_accel_1s"))
            avg=f(tm.get("avg_trade_shift_1s"))
            pv5=f(tm.get("price_velocity_5s_pct"))
            imb=f(tm.get("bbo_imbalance"))
            spread=f(tm.get("spread_bps"),999)
            r60=base.ret(sym,60)

            rr=row.get("rapid_ignition") or {}
            rapid=f(rr.get("score"),f(row.get("rapid_score")))

            # Admission is deliberately broad enough to catch a QI/SUPER/SAND
            # style early burst, but this lane has zero execution authority.
            strong_anomaly=(
                rapid>=85
                or pv5>=0.10
                or r60>=.25
                or (buy>=.58 and cvd>=.08)
                or (imb>=.15 and (nacc>=1.20 or cacc>=1.20))
                or (avg>=1.45 and buy>=.55)
            )
            # If fewer than five strong names exist, keep a positive-side
            # fallback pool so the diagnostic board can still return five
            # clearly-labelled WATCH names without relaxing any trade gate.
            positive_fallback=(
                buy>=.52
                or cvd>=0.02
                or imb>=.08
                or pv5>=0.02
                or r60>=0.08
            )
            if not (strong_anomaly or positive_fallback):
                continue

            score=100*(
                .18*cl(rapid/150.0)
                +.16*cl((nacc-.8)/2.2)
                +.13*cl((cacc-.8)/2.2)
                +.08*cl((avg-.8)/2.0)
                +.13*cl((buy-.50)/.20)
                +.12*cl((cvd+.02)/.42)
                +.08*cl((imb+.05)/.55)
                +.06*cl((pv5+.01)/.30)
                +.04*cl((r60+.03)/.60)
                +.02*cl((3.0-spread)/3.0)
            )
            # Early-momentum means early: demote already-extended one-minute
            # moves rather than chasing them into this diagnostic bucket.
            if r60>4.0:
                score-=min(20.0,(r60-4.0)*4.0)
            score=cl(score,0,100)
            if strong_anomaly and score<DARK_HORSE_MIN_SCORE:
                strong_anomaly=False
            if (not strong_anomaly) and score<8.0:
                continue

            picks.append({
                "tier":"STRONG" if strong_anomaly else "WATCH",
                "symbol":sym,
                "score":score,
                "layers":layers,
                "formal":formal or "NONE",
                "live":bool(age<=5000 and book_age<=5000),
                "age_ms":age,
                "book_age_ms":book_age,
                "rapid":rapid,
                "buy1":buy,
                "cvd1":cvd,
                "notionalA":nacc,
                "countA":cacc,
                "avgShift":avg,
                "pv5":pv5,
                "imb":imb,
                "spread":spread,
                "r60":r60,
            })
        except Exception:
            continue

    picks.sort(
        key=lambda r:(
            1 if r.get("tier")=="STRONG" else 0,
            f(r.get("score")),
            1 if r.get("live") else 0,
            f(r.get("rapid")),
            f(r.get("pv5")),
        ),
        reverse=True,
    )
    return picks[:max(0,DARK_HORSE_SLOTS)]


async def board_loop_v5():
    while True:
        await asyncio.sleep(base.BOARD_S)
        try:
            base.refresh_adapt();rows=list(base.latest.get("_board") or []);all_rows=list(base.latest.get("_all_candidates") or rows);states=("MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED","MONSTER-EXTENDED");counts={k:sum(r.get("state")==k for r in all_rows) for k in states};ups=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS));ready,trade_fresh,book_fresh=_monster_tape_health()
            integrity_live=sum(bool(r.get("integrityVerified")) for r in all_rows)
            print(f"Ψ-MONSTER-RADAR BOARD scanned={base.stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={base.stats['deep']} candidates={base.stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} memory={counts['MONSTER-MEMORY']} rescue={counts['MONSTER-RESCUE']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} rows={len(rows)}/{BOARD_ROWS} allRows={len(all_rows)} integrityLive={integrity_live}/{len(all_rows)} scan={int(base.SCAN_S*1000)}ms tape={trade_fresh}/{len(getattr(q,'universe',[]) or [])} tapeStrict={ready}/{len(getattr(q,'universe',[]) or [])} bookFresh={book_fresh}/{len(getattr(q,'universe',[]) or [])} shards={ups}/{tape.SHARDS} trades={tape.tape_stats['trades']} books={tape.tape_stats['books']} learning={base.adapt['status']} obsPending={len(base.pending)} obsResolved={len(base.resolved)} PinpointAuthority=YES BSI=ON HARD_LIVE_INTEGRITY=ON",flush=True)
            dark_exclude={
                str(r.get("symbol") or "")
                for r in all_rows
                if (
                    int(f(r.get("layers")))>=3
                    or str(r.get("formal") or "") in {"PRE-IGNITION","EARLY OPPORTUNITY"}
                    or str(r.get("state") or "") in {"MONSTER-HOT","MONSTER-IGNITION","MONSTER-RESCUE","MONSTER-MEMORY","MONSTER-SEED"}
                )
            }
            dark_horses=_dark_horse_board(dark_exclude)
            print(f"Ψ-EARLY-MOMENTUM-DARK-HORSES BOARD count={len(dark_horses)} slots={DARK_HORSE_SLOTS} maxLayers={DARK_HORSE_MAX_LAYERS}/6 source=FULL_UNIVERSE mode=DIAGNOSTIC_ONLY executionAuthority=NONE",flush=True)
            for j,r in enumerate(dark_horses,1):
                print(
                    f"DH{j:02d}. {r.get('symbol'):<14} tier={str(r.get('tier')):<6} score={f(r.get('score')):5.1f} "
                    f"layers={int(f(r.get('layers')))}/6 live={'LIVE' if r.get('live') else 'WARM'} "
                    f"age={f(r.get('age_ms'),999999):6.0f}ms bookAge={f(r.get('book_age_ms'),999999):6.0f}ms "
                    f"rapid={f(r.get('rapid')):6.1f} buy1={100*f(r.get('buy1'),.5):4.0f}% "
                    f"cvd1={f(r.get('cvd1')):+.2f} nA={f(r.get('notionalA')):.2f}x "
                    f"cA={f(r.get('countA')):.2f}x avg={f(r.get('avgShift')):.2f}x "
                    f"pv5={f(r.get('pv5')):+.3f}% imb={f(r.get('imb')):+.2f} "
                    f"spr={f(r.get('spread'),999):.2f}bp r60={f(r.get('r60')):+.2f}% formal={r.get('formal')}",
                    flush=True,
                )
            move_rows=[]
            move_syms=set()
            for r in all_rows:
                mp=_candidate_move_plan(r)
                sh=mp["shadow"]
                r["movePlanV11"]=mp
                r["moveOrigin"]="MONSTER"
                move_rows.append(r)
                sym=str(r.get("symbol") or "")
                if sym: move_syms.add(sym)

            # A valid V10.19.1 risk plan must remain visible even when its
            # symbol temporarily rotates out of the Monster candidate slice.
            # This augments the move-plan display only; it does not alter the
            # Monster board, formal state, layers, or Pinpoint BUY authority.
            try:
                now_risk=time.time()
                max_age=f(getattr(move_engine.riskmap,"SUPPORT_MAX_AGE",95.0),95.0)
                for sym,ri in list(getattr(move_engine.riskmap,"risk_cache",{}).items()):
                    sym=str(sym or "")
                    if not sym or sym in move_syms or not isinstance(ri,dict):
                        continue
                    updated=f(ri.get("updated"))
                    if updated<=0 or now_risk-updated>max_age:
                        continue
                    en=f(ri.get("entry_trigger"));st=f(ri.get("stop_loss"))
                    t1=f(ri.get("tp1"));t2=f(ri.get("tp2"));t3=f(ri.get("tp3"))
                    if not (en>0 and st>0 and st<en and t1>en and t2>t1 and t3>t2):
                        continue
                    qrow=q.latest.get(sym) or {}
                    rr={
                        "symbol":sym,
                        "state":"RISK-PLAN",
                        "formal":qrow.get("formal_state") or qrow.get("formal") or "NONE",
                        "pp":qrow.get("pinpoint_entry_status") or "WATCH",
                        "layers":f(qrow.get("layers"),f(qrow.get("gate_count"))),
                        "bsi":f(qrow.get("bsi")),
                        "eventTape":f(qrow.get("event_tape_score")),
                        "moveOrigin":"RISKMAP",
                    }
                    rr["movePlanV11"]=_candidate_move_plan(rr)
                    move_rows.append(rr)
                    move_syms.add(sym)
            except Exception as exc:
                print(f"Ψ-MONSTER-MOVE-PLAN MERGE_ERROR {type(exc).__name__}: {exc}",flush=True)

            print("Ψ-MONSTER-CANDIDATES ALL count="+str(len(all_rows))+" rows="+",".join(f"{r.get('symbol')}:{r.get('state')}:{int(f(r.get('layers')))}/6:UP{f((r.get('movePlanV11') or {}).get('shadow',{}).get('expected_excursion_pct')):.1f}%:MS{f((r.get('movePlanV11') or {}).get('shadow',{}).get('move_score')):.0f}" for r in all_rows),flush=True)
            move_rows.sort(key=lambda r:(bool((r.get("movePlanV11") or {}).get("valid")),int(f(r.get("layers"))),f((r.get("movePlanV11") or {}).get("shadow",{}).get("move_score")),f(r.get("bsi"))),reverse=True)
            print(f"Ψ-MONSTER-MOVE-PLAN BOARD candidates={len(move_rows)} validPlans={sum(bool((r.get('movePlanV11') or {}).get('valid')) for r in move_rows)} model=EMPIRICAL_SHADOW",flush=True)
            for j,r in enumerate(move_rows[:20],1):
                mp=r.get("movePlanV11") or {};sh=mp.get("shadow") or {}
                print(
                    f"MP{j:02d}. {r.get('symbol'):<14} data={str(sh.get('data_status') or 'NO_DATA'):<14} "
                    f"moveScore={f(sh.get('move_score')):5.1f}/100 expUp={f(sh.get('expected_excursion_pct')):5.2f}% "
                    f"P5={100*f(sh.get('p5')):4.1f}% P10={100*f(sh.get('p10')):4.1f}% "
                    f"P15={100*f(sh.get('p15')):4.1f}% P20={100*f(sh.get('p20')):4.1f}% "
                    f"entry={_fmt_px(mp.get('entry'))} stop={_fmt_px(mp.get('stop'))} "
                    f"tp1={_fmt_px(mp.get('tp1'))} tp2={_fmt_px(mp.get('tp2'))} tp3={_fmt_px(mp.get('tp3'))} "
                    f"runner={_fmt_px(mp.get('runner'))} proj5={_fmt_px(sh.get('projection5'))} "
                    f"proj10={_fmt_px(sh.get('projection10'))} proj15={_fmt_px(sh.get('projection15'))} "
                    f"proj20={_fmt_px(sh.get('projection20'))} plan={'VALID' if mp.get('valid') else 'SHADOW_ONLY'} "
                    f"riskState={mp.get('risk_state')} src={mp.get('source')} origin={r.get('moveOrigin','MONSTER')}",
                    flush=True,
                )
            exrows=[r for r in all_rows if str(r.get("monsterPullbackState")) in {"PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING","PULLBACK_ONLY"}]
            exrows.sort(key=lambda r:(2 if r.get("monsterPullbackState")=="PULLBACK_EXHAUSTED" else 1 if r.get("monsterPullbackState")=="SELL_PRESSURE_EXHAUSTING" else 0,f(r.get("monsterExhaustionScore")),f(r.get("bsi"))),reverse=True)
            print(f"Ψ-MONSTER-PULLBACK-EXHAUSTION BOARD candidates={len(exrows)} exhausted={sum(r.get('monsterPullbackState')=='PULLBACK_EXHAUSTED' for r in exrows)} exhausting={sum(r.get('monsterPullbackState')=='SELL_PRESSURE_EXHAUSTING' for r in exrows)}",flush=True)
            for j,r in enumerate(exrows,1):
                print(f"PX{j:02d}. {r.get('symbol'):<14} state={r.get('monsterPullbackState'):<24} score={f(r.get('monsterExhaustionScore')):5.1f} depth={f(r.get('monsterPullbackDepth')):5.2f}% rebound={f(r.get('monsterPullbackRebound')):5.2f}% BSI={f(r.get('bsi')):5.1f} layers={int(f(r.get('layers')))}/6 tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} formal={r.get('formal')} pp={r.get('pp')}",flush=True)
            for i,r in enumerate(rows,1):
                ds="-" if r.get("dist") is None else f"{f(r.get('dist')):+.2f}%";age=f(r.get("peak20Age"),999999);mem="-" if age>rescue.MEMORY_WINDOW_S else f"{100*f(r.get('peakShP20_120')):.1f}%/{age:.0f}s"
                ia=r.get("integrityAges") or {};ilive="LIVE" if r.get("integrityVerified") else "WAIT"
                print(f"MR{i:02d}. {r['symbol']:<14} integrity={ilive:<4} sAge={f(ia.get('structure_s'),999999):5.1f}s tAge={f(ia.get('tape_ms'),999999):6.0f}ms bAge={f(ia.get('micro_book_ms'),999999):6.0f}ms state={str(r.get('state')):<17} BSI={f(r.get('bsi')):5.1f} {str(r.get('bsiState')):<19} FB={f(r.get('falseBreakRisk')):4.0f} comp={f(r.get('bsiCompression')):4.0f} tests={f(r.get('bsiTests')):4.0f} retest={f(r.get('bsiRetest')):4.0f} trend={f(r.get('bsiTrend')):4.0f} EARLY={f(r.get('early')):5.1f} DNA={f(r.get('dna')):5.1f} retain={f(r.get('retentionScore')):5.1f} rapid={f(r.get('rapid')):6.1f}/{f(r.get('peak')):6.1f} tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} event={f(r.get('event')):4.0f} vac={f(r.get('vac')):4.0f} pB15={100*f(r.get('pb15')):4.1f}% shP20={100*f(r.get('sp20')):4.1f}% mem20={mem} layers={int(f(r.get('layers')))}/6 dist={ds} formal={r.get('formal')} pp={r.get('pp')} moveScore={f(((r.get('movePlanV11') or {}).get('shadow') or {}).get('move_score')):4.0f}/100 expUp={f(((r.get('movePlanV11') or {}).get('shadow') or {}).get('expected_excursion_pct')):4.1f}% entry={_fmt_px((r.get('movePlanV11') or {}).get('entry'))} tp3={_fmt_px((r.get('movePlanV11') or {}).get('tp3'))} why={(r.get('reasons') or [])[:10]}",flush=True)
            top=sorted(rows,key=lambda r:f(r.get("bsi")),reverse=True)[:8];print(f"Ψ-BSI BOARD top={[(r.get('symbol'),round(f(r.get('bsi')),1),r.get('bsiState'),round(f(r.get('falseBreakRisk')),1),round(f(r.get('bsiCompression')),1),round(f(r.get('bsiTests')),1),r.get('pp')) for r in top]}",flush=True);print(f"Ψ-BSI LEARNING {_bsi_learning()}",flush=True)
            br,bn=rescue._blocker_learning();print("Ψ-MONSTER-BLOCKER-LEARN "+(f"resolved={bn} top={[(b,n,round(w10*100,1),round(w20*100,1),round(mfe,2)) for w20,w10,mfe,n,b in br]}" if bn else "status=WARMING resolved=0"),flush=True);paths=rescue._path_learning();print(f"Ψ-MONSTER-PATH-LEARN {paths}" if paths else "Ψ-MONSTER-PATH-LEARN status=WARMING",flush=True);print(f"Ψ-MONSTER-RESCUE HEALTH emergencyPromotions={rescue.rescue_stats['emergency_promotions']} lastEmergency={rescue.rescue_stats['last_emergency']} deepPool={rescue.rescue_stats['last_pool']} ignitionEpisodes={rescue.rescue_stats['ignition_episodes']} memoryWindow={int(rescue.MEMORY_WINDOW_S)}s buyAuthority=PINPOINT_ONLY",flush=True);rescue._save_rescue()
        except asyncio.CancelledError:rescue._save_rescue(True);raise
        except Exception as e:base.stats["board_errors"]+=1;print(f"Ψ-BSI BOARD_ERROR {type(e).__name__}: {e}",flush=True)
base.board_loop=board_loop_v5

for mod in (rescue,tape,base,getattr(base,"scientist",None),scanner):
    try:mod.VERSION=VERSION
    except Exception:pass


RECOVERY_BATCH = int(os.environ.get("PSI_RECOVERY_BATCH", "4"))
WATCHDOG_STRUCTURE_RESCUE_BATCH = int(os.environ.get("PSI_WATCHDOG_STRUCTURE_RESCUE_BATCH", "1"))
WATCHDOG_STRUCTURE_STALE_RESCUE_BATCH = int(os.environ.get("PSI_WATCHDOG_STRUCTURE_STALE_RESCUE_BATCH", "2"))
WATCHDOG_STRUCTURE_CRITICAL_BATCH = int(os.environ.get("PSI_WATCHDOG_STRUCTURE_CRITICAL_BATCH", "1"))
WATCHDOG_STRUCTURE_RESCUE_SEED_SLOTS = int(os.environ.get("PSI_WATCHDOG_STRUCTURE_RESCUE_SEED_SLOTS", "2"))
WATCHDOG_STRUCTURE_RESCUE_COOLDOWN_S = float(os.environ.get("PSI_WATCHDOG_STRUCTURE_RESCUE_COOLDOWN_S", "15"))
RECOVERY_PRIORITY = 80
# Structure may be cached for research, but execution-tier freshness is much
# tighter than before. This prevents 3-5 minute-old structure from supporting
# a live PRE/HOT/IGNITION label.
RECOVERY_STALE_S = INTEGRITY_STRUCTURE_MAX_AGE_S
recovery_stats = {"passes":0,"ok":0,"fail":0,"fast_ok":0,"fast_fail":0,"seed_ok":0,"seed_fail":0,"seed_cycles":0,"rescue_ok":0,"rescue_fail":0,"rescue_cycles":0,"rescue_stale_ok":0,"rescue_stale_fail":0,"rescue_seed_ok":0,"rescue_seed_fail":0,"route_resets":0,"pool_kicks":0,"ext_ok":0,"ext_err":0,"cache_load":0,"cache_save":0}
_recovery_retry_after = {}
_recovery_inflight = set()
RECOVERY_FAIL_COOLDOWN_S = 20.0
COLD_SEED_SLEEP_S = 2.0
COLD_SEED_BACKOFF_S = 35.0
COLD_SEED_REST_QUIET_S = 12.0
COLD_SEED_RISK_QUIET_S = 30.0
RECOVERY_CYCLE_SLEEP_S = 1.0
_fast_recovery_active = 0
STRUCTURE_CACHE_MAX_AGE_S = 300.0
STRUCTURE_CACHE_PATH = os.environ.get("PSI_STRUCTURE_CACHE_PATH", "/data/psi_v11_structure_cache.json" if os.path.isdir("/data") else "/app/psi_v11_structure_cache.json")
_structure_cache_dirty = False

def _recovery_scope():
    # Protect the real execution pool, then reserve a large rotating slice for
    # discovery. This prevents the same historical 20 names from monopolising
    # the 80-symbol structure window.
    universe=list(getattr(q,"universe",[]) or [])
    universe_set=set(universe)
    out=[];seen=set()
    def add(s):
        s=str(s or "")
        if s and s in universe_set and s not in seen and len(out)<RECOVERY_PRIORITY:
            seen.add(s);out.append(s)

    for s in list(getattr(app,"selected_micro_symbols",[]) or []): add(s)

    recent=sorted(
        ((cy,s,lane) for s,(cy,lane) in _discovery_recent_promotions.items()),
        reverse=True
    )
    for _,s,_ in recent: add(s)

    try:
        for r in list(base.latest.get("_board") or []): add(r.get("symbol"))
    except Exception:
        pass

    try:
        for _,s in q.hot(min(32,RECOVERY_PRIORITY)): add(s)
    except Exception:
        pass

    # Keep only two permanent market anchors; everything else earns/rotates in.
    for s in ("BTCUSDT","ETHUSDT"): add(s)

    if universe and len(out)<RECOVERY_PRIORITY:
        slots=min(RECOVERY_ROTATION_SLOTS,RECOVERY_PRIORITY-len(out))
        bucket=int(time.time()/max(5.0,RECOVERY_ROTATION_PERIOD_S))
        start=(bucket*max(1,slots))%len(universe)
        checked=0
        while checked<len(universe) and slots>0 and len(out)<RECOVERY_PRIORITY:
            s=universe[(start+checked)%len(universe)];checked+=1
            if s in seen:continue
            add(s);slots-=1

    # Fill any remaining capacity from the generic priority list.
    for s in _recovery_symbols():
        add(s)
        if len(out)>=RECOVERY_PRIORITY:break
    return out

def _execution_structure_batch_symbols():
    scope=_recovery_scope()
    now=time.time()
    stale=[
        s for s in scope
        if _raw_seed_count(s)>=3
        and _structure_age_recovery(s)>RECOVERY_STALE_S
        and _recovery_retry_after.get(s,0)<=now
    ]
    return stale[:RECOVERY_BATCH]

# Neutralize the legacy 60-symbol historical-structure sweeps. The WebSocket
# discovery stack still scans all 403 markets; REST historical structure is
# execution-tier only and owned by the v11 recovery scheduler.
q.STRUCTURE_BATCH=RECOVERY_BATCH
q.structure_batch_symbols=_execution_structure_batch_symbols

async def _legacy_structure_noop():
    # V11 recovery is the sole owner of historical structure REST.
    # Discovery and live microstructure continue through their WebSocket loops.
    return None

stable_core.refresh_structure=_legacy_structure_noop
target_core.refresh_structure=_legacy_structure_noop
try:
    target_core._orig_refresh_structure=_legacy_structure_noop
except Exception:
    pass
qualifier_core.refresh_structure=_legacy_structure_noop
q.refresh_structure=_legacy_structure_noop
app.refresh_structure=_legacy_structure_noop

async def _execution_anomaly_refresh():
    # Full-universe discovery is WebSocket-native. Avoid the old 20-symbol
    # REST anomaly burst until the continuity execution pool exists.
    if app.session is None or not getattr(app,"selected_micro_symbols",None):
        return
    selected=list(dict.fromkeys(app.selected_micro_symbols))[:4]
    sem=asyncio.Semaphore(2)
    async def one(sym):
        async with sem:
            try:
                row=await asyncio.wait_for(app.load_fast_anomaly(app.session,sym),timeout=8.0)
                if isinstance(row,dict):
                    app.anomaly_state[sym]=row
            except Exception:
                return
    await asyncio.gather(*(one(sym) for sym in selected))

q.refresh_anomaly=_execution_anomaly_refresh

def _load_structure_cache():
    global _structure_cache_dirty
    try:
        if not os.path.exists(STRUCTURE_CACHE_PATH):
            return 0
        with open(STRUCTURE_CACHE_PATH,"r",encoding="utf-8") as fh:
            payload=json.load(fh)
        rows=payload.get("rows") or {}
        now_ms=int(time.time()*1000)
        universe=set(getattr(q,"universe_set",set()) or set())
        loaded=0
        for sym,sd in rows.items():
            if sym not in universe or not isinstance(sd,dict):
                continue
            updated=int(sd.get("updated_ms") or 0)
            age=(now_ms-updated)/1000.0 if updated>0 else 999999.0
            if age<0 or age>STRUCTURE_CACHE_MAX_AGE_S:
                continue
            app.structure[sym]=sd
            q.structure_ms[sym]=updated
            loaded+=1
        recovery_stats["cache_load"]+=loaded
        _structure_cache_dirty=False
        print(f"Ψ-RECOVERY CACHE_LOAD loaded={loaded} path={STRUCTURE_CACHE_PATH}",flush=True)
        return loaded
    except Exception as exc:
        print(f"Ψ-RECOVERY CACHE_LOAD_ERROR {type(exc).__name__}: {exc}",flush=True)
        return 0

def _save_structure_cache():
    global _structure_cache_dirty
    if not _structure_cache_dirty:
        return 0
    try:
        now_ms=int(time.time()*1000)
        rows={}
        for sym,sd in list(getattr(app,"structure",{}).items()):
            if not isinstance(sd,dict): continue
            updated=int(sd.get("updated_ms") or q.structure_ms.get(sym,0) or 0)
            age=(now_ms-updated)/1000.0 if updated>0 else 999999.0
            if 0<=age<=STRUCTURE_CACHE_MAX_AGE_S:
                rows[sym]=sd
        payload={"version":VERSION,"saved_at":time.time(),"rows":rows}
        tmp=STRUCTURE_CACHE_PATH+".tmp"
        with open(tmp,"w",encoding="utf-8") as fh:
            json.dump(payload,fh,separators=(",",":"))
        os.replace(tmp,STRUCTURE_CACHE_PATH)
        recovery_stats["cache_save"]+=1
        _structure_cache_dirty=False
        return len(rows)
    except Exception as exc:
        print(f"Ψ-RECOVERY CACHE_SAVE_ERROR {type(exc).__name__}: {exc}",flush=True)
        return 0

async def structure_cache_loop():
    while True:
        await asyncio.sleep(20.0)
        _save_structure_cache()
        _save_structure_raw_cache()

# Production watchdog: detects data-pipeline starvation and performs bounded,
# fail-closed recovery actions. It never changes signal thresholds or BUY authority.
WATCHDOG_INTERVAL_S = 15.0
WATCHDOG_STARTUP_GRACE_S = 45.0
WATCHDOG_STRUCTURE_STALL_S = 75.0
WATCHDOG_STRUCTURE_CRITICAL_FRESH = 8
WATCHDOG_POOL_STALL_S = 75.0
WATCHDOG_SHARD_STALL_S = 45.0
WATCHDOG_EXT_STALE_S = 50.0
watchdog_stats = {
    "cycles":0,"healthy":0,"degraded":0,"actions":0,"errors":0,
    "ext_refresh":0,"structure_kicks":0,"structure_rescues":0,"structure_rescue_ok":0,
    "structure_stale_rescues":0,"structure_seed_rescues":0,"route_resets":0,
    "pool_kicks":0,"shard_kicks":0,
}
_watchdog_started = time.time()
_watchdog_last_ever_cov = 0
_watchdog_last_cov_progress = time.time()
_watchdog_last_pool = 0
_watchdog_last_pool_progress = time.time()
_watchdog_last_shards = 0
_watchdog_last_shard_progress = time.time()
_watchdog_last_structure_rescue = 0.0
_watchdog_structure_rescue_task = None
_watchdog_last_rescue_result = None

def _structure_age_recovery(sym):
    ts = int(q.structure_ms.get(sym,0) or 0)
    return 999999.0 if ts <= 0 else max(0.0,(q.ms()-ts)/1000.0)

def _integrity_status(sym,row=None,require_risk=False,require_event_tape=False):
    """One authoritative hard-live integrity check.

    Native micro readiness is taken from app.micro_metrics(), the same source
    used by the formal evaluator. Event tape is only mandatory for Monster
    states that explicitly depend on that fast lane. BUY accepts the Pinpoint
    trigger/stop risk plan first, with RiskMap as a structural fallback.
    """
    sym=str(sym or "")
    row=row if isinstance(row,dict) else (q.latest.get(sym) or {})
    cache_key=(sym,bool(require_risk),bool(require_event_tape))
    cached=_integrity_cache.get(cache_key)
    now=time.time()
    if cached and now-f(cached.get("t"))<=INTEGRITY_CACHE_TTL_S:
        return dict(cached.get("v") or {})

    blockers=[]
    ages={}

    structure_age=_structure_age_recovery(sym)
    ages["structure_s"]=round(structure_age,3)
    if structure_age>INTEGRITY_STRUCTURE_MAX_AGE_S:
        blockers.append("STALE_STRUCTURE")

    micro=(getattr(app,"micro_state",{}) or {}).get(sym) or {}
    now_ms=q.ms()
    last_trade=int(micro.get("last_trade_ms",0) or 0)
    last_book=int(micro.get("last_book_ms",0) or 0)
    micro_trade_age=(now_ms-last_trade) if last_trade>0 else 999999999.0
    micro_book_age=(now_ms-last_book) if last_book>0 else 999999999.0
    ages["micro_trade_ms"]=round(micro_trade_age,1)
    ages["micro_book_ms"]=round(micro_book_age,1)

    try:
        mm=app.micro_metrics(sym)
    except Exception:
        mm={}
    native_ready=bool(mm.get("micro_ready"))
    trade_seq=bool(mm.get("sequence_verified"))
    book_seq=bool(mm.get("book_sequence_verified"))
    ages["native_micro_ready"]=native_ready
    ages["trade_seq"]=trade_seq
    ages["book_seq"]=book_seq

    if micro_trade_age>INTEGRITY_MICRO_TRADE_MAX_AGE_MS:
        blockers.append("STALE_DEPTH_TRADE")
    if micro_book_age>INTEGRITY_MICRO_BOOK_MAX_AGE_MS:
        blockers.append("STALE_DEPTH_BOOK")
    if not native_ready:
        blockers.append("MICRO_NOT_READY")
    if not trade_seq:
        blockers.append("TRADE_SEQUENCE_INVALID")
    if not book_seq:
        blockers.append("BOOK_SEQUENCE_INVALID")

    tm=tape.tape_metric(sym)
    tape_age=f(tm.get("age_ms"),999999.0)
    bbo_age=f(tm.get("book_age_ms"),999999.0)
    ages["tape_ms"]=round(tape_age,1)
    ages["bbo_ms"]=round(bbo_age,1)
    if require_event_tape:
        if tape_age>INTEGRITY_TAPE_MAX_AGE_MS or int(f(tm.get("trades_5s")))<1:
            blockers.append("STALE_EVENT_TAPE")
        if bbo_age>INTEGRITY_BBO_MAX_AGE_MS:
            blockers.append("STALE_EVENT_BBO")

    if require_risk:
        pin_entry=f(row.get("pinpoint_trigger"))
        pin_stop=f(row.get("pinpoint_stop"))
        pin_risk=f(row.get("pinpoint_risk_pct"))
        pin_status=str(row.get("pinpoint_entry_status") or "")
        pin_ok=(
            pin_entry>0 and pin_stop>0 and pin_stop<pin_entry and pin_risk>0
            and pin_status in {"PINPOINT_TRIGGERED","PINPOINT_ARMED"}
        )
        if pin_ok:
            ages["risk_s"]=0.0
            ages["risk_source"]="PINPOINT"
        else:
            try:
                ri=move_engine.riskmap.risk_intel(sym)
            except Exception:
                ri=None
            if not isinstance(ri,dict):
                blockers.append("NO_FRESH_RISK_PLAN")
                ages["risk_s"]=999999.0
                ages["risk_source"]="NONE"
            else:
                updated=f(ri.get("updated"))
                risk_age=(time.time()-updated) if updated>0 else 999999.0
                ages["risk_s"]=round(risk_age,3)
                ages["risk_source"]="RISKMAP"
                en=f(ri.get("entry_trigger"));st=f(ri.get("stop_loss"))
                t1=f(ri.get("tp1"));t2=f(ri.get("tp2"));t3=f(ri.get("tp3"))
                if risk_age>INTEGRITY_RISK_MAX_AGE_S:
                    blockers.append("STALE_RISK_PLAN")
                if not (en>0 and st>0 and st<en and t1>en and t2>t1 and t3>t2):
                    blockers.append("INVALID_RISK_PLAN")

    result={"verified":not blockers,"blockers":list(dict.fromkeys(blockers)),"ages":ages}
    _integrity_cache[cache_key]={"t":now,"v":dict(result)}
    return result

# Wrap the sole formal evaluator. Elevated states are preserved only if their
# mandatory live feeds pass the independent integrity check at evaluation time.
_original_evaluate_symbol_integrity = app.evaluate_symbol

def evaluate_symbol_integrity(sym):
    row=_original_evaluate_symbol_integrity(sym)
    if not isinstance(row,dict):
        return row
    row=dict(row)
    raw=str(row.get("state") or "REJECT")
    integ=_integrity_status(sym,row,require_risk=(raw=="BUY NOW"))
    raw_formal=str(row.get("formal_state") or raw)
    row["integrity_raw_state"]=raw
    row["integrity_raw_formal"]=raw_formal
    row["integrity_verified"]=bool(integ.get("verified"))
    row["integrity_blockers"]=list(integ.get("blockers") or [])
    row["integrity_ages"]=dict(integ.get("ages") or {})
    elevated=raw in {"EARLY OPPORTUNITY","PRE-IGNITION","BUY NOW"} or raw_formal in {"EARLY OPPORTUNITY","PRE-IGNITION","BUY NOW"}
    if elevated and not row["integrity_verified"]:
        # Keep every downstream/reporting alias in sync. Previously only
        # row["state"] was downgraded, allowing the Pinpoint board to continue
        # printing stale PRE-IGNITION from formal_state.
        row["state"]="COLLECTING DATA"
        row["formal_state"]="COLLECTING DATA"
        row["pre_warmup_state"]="COLLECTING DATA"
        row["pinpoint_state"]="WATCH"
        row["pinpoint_buy"]=False
        row["strict_buy_gate_passed"]=False
        fh=list(row.get("failed_hard") or [])
        fh.extend(row["integrity_blockers"])
        fh.append("LIVE_DATA_INTEGRITY")
        row["failed_hard"]=list(dict.fromkeys(fh))
        pb=list(row.get("pinpoint_blockers") or [])
        pb.extend(row["integrity_blockers"])
        pb.append("LIVE_DATA_INTEGRITY")
        row["pinpoint_blockers"]=list(dict.fromkeys(pb))
        row["combined_blockers"]=list(dict.fromkeys(list(row.get("combined_blockers") or [])+pb))
    return row

app.evaluate_symbol=evaluate_symbol_integrity

def _recovery_symbols():
    out=[];seen=set()
    def add(s):
        s=str(s or "")
        if s and s not in seen and s in set(getattr(q,"universe_set",set()) or set()):
            seen.add(s);out.append(s)
    # Once continuity is populated, protect the actual execution pool first.
    for s in list(getattr(app,"selected_micro_symbols",[]) or []): add(s)
    # Permanent anchors are intentionally minimal. Other symbols must earn
    # priority through live discovery, board strength, hot ranking, or rotation.
    for s in ("BTCUSDT","ETHUSDT"):
        add(s)
    for s,(cy,lane) in sorted(_discovery_recent_promotions.items(),key=lambda kv:kv[1][0],reverse=True):
        add(s)
    try:
        for r in list(base.latest.get("_board") or []): add(r.get("symbol"))
    except Exception:
        pass
    try:
        for _,s in q.hot(RECOVERY_PRIORITY): add(s)
    except Exception:
        pass
    for s in list(getattr(q,"universe",[]) or []): add(s)
    return out

async def _hydrate_one(sym, lane="FAST"):
    global _structure_cache_dirty, _fast_recovery_active
    sym=str(sym)
    if sym in _recovery_inflight:
        return None
    _recovery_inflight.add(sym)
    high_priority = lane in {"FAST","WATCHDOG","WATCHDOG_REST"}
    if high_priority:
        _fast_recovery_active += 1
    rest_token=None
    try:
        if app.session is None or app.session.closed:
            raise RuntimeError("shared REST session unavailable")
        client=app.session
        owner_token=_structure_owner_ctx.set(True)
        rest_token=None
        try:
            # Normal FAST recovery remains WS-API first. Watchdog rescue has an
            # independent direct-REST lane so a stalled WS kline route cannot
            # consume the entire 120s structure-freshness budget.
            if lane=="WATCHDOG_REST":
                rest_token=_structure_watchdog_rest_ctx.set(True)
                timeout_s=14.0
            elif lane=="WATCHDOG":
                timeout_s=28.0
            else:
                timeout_s=18.0 if lane=="FAST" else 22.0
            sd=await asyncio.wait_for(app.load_structure(client,sym),timeout=timeout_s)
        finally:
            if rest_token is not None:
                _structure_watchdog_rest_ctx.reset(rest_token)
            _structure_owner_ctx.reset(owner_token)
        if not isinstance(sd,dict):
            raise RuntimeError("structure payload incomplete")
        app.structure[sym]=sd
        q.structure_ms[sym]=q.ms()
        if not isinstance(app.anomaly_state.get(sym),dict):
            try:
                an=await asyncio.wait_for(app.load_fast_anomaly(client,sym),timeout=3.0)
                if isinstance(an,dict):
                    app.anomaly_state[sym]=an
            except Exception:
                pass
        row=app.evaluate_symbol(sym)
        if isinstance(row,dict) and row:
            q.latest[sym]=row
        _recovery_retry_after.pop(sym,None)
        _structure_cache_dirty=True
        recovery_stats["ok"]+=1
        if lane=="FAST":
            recovery_stats["fast_ok"]+=1
        elif lane in {"WATCHDOG","WATCHDOG_REST"}:
            recovery_stats["rescue_ok"]+=1
            if lane=="WATCHDOG_REST": recovery_stats["rescue_stale_ok"]+=1
            else: recovery_stats["rescue_seed_ok"]+=1
        else:
            recovery_stats["seed_ok"]+=1
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _recovery_retry_after[sym]=time.time()+(6.0 if lane in {"WATCHDOG","WATCHDOG_REST"} else RECOVERY_FAIL_COOLDOWN_S)
        recovery_stats["fail"]+=1
        if lane=="FAST":
            recovery_stats["fast_fail"]+=1
        elif lane in {"WATCHDOG","WATCHDOG_REST"}:
            recovery_stats["rescue_fail"]+=1
            if lane=="WATCHDOG_REST": recovery_stats["rescue_stale_fail"]+=1
            else: recovery_stats["rescue_seed_fail"]+=1
        else:
            recovery_stats["seed_fail"]+=1
        if recovery_stats["fail"]<=60:
            print(f"Ψ-RECOVERY {lane}_ERROR {sym} {type(exc).__name__}: {exc}",flush=True)
        return False
    finally:
        if high_priority:
            _fast_recovery_active=max(0,_fast_recovery_active-1)
        _recovery_inflight.discard(sym)


async def structure_recovery_loop():
    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)
    _load_structure_cache()
    _load_structure_raw_cache()

    while True:
        scope=_recovery_scope()
        total=len(scope)
        if total<=0:
            await asyncio.sleep(RECOVERY_CYCLE_SLEEP_S)
            continue

        now=time.time()
        fresh=sum(1 for s in scope if _structure_age_recovery(s)<=RECOVERY_STALE_S)
        ever=sum(1 for s in scope if _structure_age_recovery(s)<999000)
        targets=[
            s for s in scope
            if _raw_seed_count(s)>=3
            and _structure_age_recovery(s)>RECOVERY_STALE_S
            and _recovery_retry_after.get(s,0)<=now
            and s not in _recovery_inflight
        ]
        scope_pos={s:i for i,s in enumerate(scope)}
        execution_set=set(getattr(app,"selected_micro_symbols",[]) or [])
        targets.sort(
            key=lambda s:(
                0 if s in execution_set else 1,
                0 if _structure_age_recovery(s)<999000 else 1,
                scope_pos.get(s,9999),
            )
        )

        if targets:
            batch=targets[:RECOVERY_BATCH]
            batch_started=time.time()
            results=await asyncio.gather(*[_hydrate_one(s,"FAST") for s in batch])
            batch_s=time.time()-batch_started
            fresh=sum(1 for s in scope if _structure_age_recovery(s)<=RECOVERY_STALE_S)
            ever=sum(1 for s in scope if _structure_age_recovery(s)<999000)
            print(
                f"Ψ-RECOVERY FAST_BATCH fresh={fresh}/{total} ever={ever}/{total} "
                f"batch={len(batch)} batchOK={sum(bool(x) for x in results)} batchFail={sum(not bool(x) for x in results)} "
                f"batchSec={batch_s:.2f} ok={recovery_stats['ok']} fail={recovery_stats['fail']} "
                f"structHosts={sorted({str(_rest_good_host.get('structure_klines:'+s,'-')).replace('https://','') for s in batch})} "
                f"tfCache={_structure_tf_stats['cache_hit']} rawHit={_structure_tf_stats['raw_hit']} "
                f"incOK={_structure_tf_stats['incremental_ok']} seed={_structure_tf_stats['full_seed']} reuse={_structure_tf_stats['bar_reuse']} "
                f"tfRetryOK={_structure_tf_stats['retry_ok']} tfFail={_structure_tf_stats['fail']}",
                flush=True,
            )
            _save_structure_raw_cache()

        # Continuity is fresh-structure-only and grows in bounded steps. It can
        # start as soon as verified structure exists; every execution symbol
        # must retain its own fresh structure and all Pinpoint hard gates.
        if fresh>0:
            try:
                await continuity_guard.rebalance_continuity_guarded(force=True)
                recovery_stats["pool_kicks"]+=1
            except Exception as exc:
                print(f"Ψ-RECOVERY POOL_ERROR {type(exc).__name__}: {exc}",flush=True)

        recovery_stats["passes"]+=1
        if recovery_stats["passes"]%5==0 or not targets:
            print(
                f"Ψ-RECOVERY STRUCTURE scope={total} fresh={fresh}/{total} ever={ever}/{total} "
                f"pass={recovery_stats['passes']} ok={recovery_stats['ok']} fail={recovery_stats['fail']} "
                f"fast={recovery_stats['fast_ok']}/{recovery_stats['fast_fail']} seed={recovery_stats['seed_ok']}/{recovery_stats['seed_fail']} "
                f"pool={len(app.selected_micro_symbols or [])} kicks={recovery_stats['pool_kicks']} "
                f"restOK={_rest_stats['ok']} restFail={_rest_stats['fail']} restRetry={_rest_stats['attempt_fail']} failover={_rest_stats['failover']} tfCache={_structure_tf_stats['cache_hit']} tfRetryOK={_structure_tf_stats['retry_ok']} tfFail={_structure_tf_stats['fail']} riskOK={_rest_stats['risk_ok']} riskFail={_rest_stats['risk_fail']} riskDefer={_rest_stats['risk_defer']} bgOK={_rest_stats['bg_ok']} bgFail={_rest_stats['bg_fail']} bgDefer={_rest_stats['bg_defer']} "
                f"cacheLoad={recovery_stats['cache_load']} cacheSave={recovery_stats['cache_save']} "
                f"rawLoad={_structure_tf_stats['raw_load']} rawSave={_structure_tf_stats['raw_save']} "
                f"incOK={_structure_tf_stats['incremental_ok']} seed={_structure_tf_stats['full_seed']} wsRefresh={_structure_tf_stats['ws_refresh']} wsSubs={_structure_ws_stats['subscribed']} wsApiStruct={_ws_api_stats['structure_ok']} "
                f"structRoutes={sum(1 for k in _rest_good_host if str(k).startswith('structure_klines:'))}",
                flush=True,
            )
        await asyncio.sleep(RECOVERY_CYCLE_SLEEP_S)


async def cold_seed_loop():
    # Cold/partial historical seeding is deliberately separated from the
    # execution freshness lane. One symbol at a time may consume two race
    # hosts, leaving capacity reserved for FAST refresh + risk maps.
    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)

    while True:
        await asyncio.sleep(COLD_SEED_SLEEP_S)
        try:
            now=time.time()

            # Cold seeding is strictly opportunistic. It yields whenever FAST
            # structure work is active or recent REST/risk failures show that
            # Binance HTTP capacity is degraded.
            if _fast_recovery_active>0:
                continue
            rest_recent = _rest_last_fail>0 and (now-_rest_last_fail)<COLD_SEED_REST_QUIET_S
            risk_recent = _risk_last_fail>0 and (now-_risk_last_fail)<COLD_SEED_RISK_QUIET_S
            risk_unrecovered = risk_recent and (_risk_last_ok<=_risk_last_fail)
            if rest_recent or risk_unrecovered:
                print(
                    f"Ψ-RECOVERY SEED_BACKOFF restRecent={int(rest_recent)} "
                    f"riskUnrecovered={int(risk_unrecovered)} restFail={_rest_stats['fail']} "
                    f"riskOK={_rest_stats['risk_ok']} riskFail={_rest_stats['risk_fail']} "
                    f"sleep={int(COLD_SEED_BACKOFF_S)}s",
                    flush=True,
                )
                await asyncio.sleep(COLD_SEED_BACKOFF_S)
                continue

            scope=_recovery_scope()

            fast_pending=[
                s for s in scope
                if _raw_seed_count(s)>=3
                and _structure_age_recovery(s)>RECOVERY_STALE_S
                and _recovery_retry_after.get(s,0)<=now
                and s not in _recovery_inflight
            ]
            if fast_pending:
                continue

            candidates=[
                s for s in scope
                if _raw_seed_count(s)<3
                and _recovery_retry_after.get(s,0)<=now
                and s not in _recovery_inflight
            ]
            if not candidates:
                continue

            scope_pos={s:i for i,s in enumerate(scope)}
            candidates.sort(
                key=lambda s:(
                    -_raw_seed_count(s),
                    0 if _structure_age_recovery(s)<999000 else 1,
                    scope_pos.get(s,9999),
                )
            )
            sym=candidates[0]
            before=_raw_seed_count(sym)
            started=time.time()
            result=await _hydrate_one(sym,"SEED")
            after=_raw_seed_count(sym)
            recovery_stats["seed_cycles"]+=1
            _save_structure_raw_cache()
            print(
                f"Ψ-RECOVERY SEED symbol={sym} seedBefore={before}/3 seedAfter={after}/3 "
                f"ok={int(result is True)} sec={time.time()-started:.2f} "
                f"seedOK={recovery_stats['seed_ok']} seedFail={recovery_stats['seed_fail']} "
                f"rawEntries={len(_structure_raw_cache)}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-RECOVERY SEED_LOOP_ERROR {type(exc).__name__}: {exc}",flush=True)


async def _watchdog_refresh_extension():
    n=extrest.sync_extension_from_ws()
    if n<=0:
        raise RuntimeError("Binance miniTicker WebSocket is not live")
    watchdog_stats["ext_refresh"]+=1
    return n


def _watchdog_execution_scope(fallback_scope=None):
    """Stable Watchdog health population: actual execution symbols first.

    Recovery/discovery may rotate across 80 symbols, but health should reflect
    the symbols that can currently reach Pinpoint execution. Per-symbol
    structure freshness remains mandatory regardless of Watchdog status.
    """
    out=[];seen=set()
    universe_set=set(getattr(q,"universe_set",set()) or set())
    def add(sym):
        sym=str(sym or "")
        if sym and sym in universe_set and sym not in seen:
            seen.add(sym);out.append(sym)

    for sym in list(getattr(app,"selected_micro_symbols",[]) or []):
        add(sym)

    # Include formal execution-near candidates even if continuity has not yet
    # migrated them into selected_micro_symbols.
    ranked=[]
    for sym,row in list(getattr(q,"latest",{}).items()):
        if sym not in universe_set or not isinstance(row,dict):
            continue
        state=str(row.get("formal_state") or row.get("state") or "")
        pstate=str(row.get("pinpoint_state") or "")
        rank=(
            4 if state=="BUY NOW" else
            3 if state=="PRE-IGNITION" else
            2 if state=="EARLY OPPORTUNITY" else
            1 if pstate in {"BUY NOW","PINPOINT ARMED","SETUP READY"} else 0
        )
        if rank:
            ranked.append((rank,f(row.get("score")),str(sym)))
    for _,_,sym in sorted(ranked,reverse=True):
        add(sym)

    # During cold start only, give Watchdog a small stable fallback rather than
    # the entire rotating 80-symbol discovery scope.
    if not out and fallback_scope:
        for sym in list(fallback_scope)[:16]:
            add(sym)
    return out


def _reset_structure_route(sym):
    """Clear only this symbol's learned structure route/cooldowns before rescue."""
    route_key=f"structure_klines:{str(sym)}"
    reset=0
    if route_key in _rest_good_host:
        _rest_good_host.pop(route_key,None); reset+=1
    for key in list(_rest_host_bad_until.keys()):
        try:
            rkey,_host=key
        except Exception:
            continue
        if str(rkey)==route_key:
            _rest_host_bad_until.pop(key,None); reset+=1
    recovery_stats["route_resets"]+=reset
    watchdog_stats["route_resets"]+=reset
    return reset


def _watchdog_structure_priority(scope):
    """Execution-first stale/missing structure rescue order."""
    out=[];seen=set()
    universe_set=set(getattr(q,"universe_set",set()) or set())
    def add(sym):
        sym=str(sym or "")
        if sym and sym in universe_set and sym not in seen:
            seen.add(sym);out.append(sym)

    for sym in list(getattr(app,"selected_micro_symbols",[]) or []):
        add(sym)

    ranked=[]
    for sym,row in list(getattr(q,"latest",{}).items()):
        if sym not in universe_set or not isinstance(row,dict):
            continue
        state=str(row.get("formal_state") or row.get("state") or "")
        pstate=str(row.get("pinpoint_state") or "")
        rank=(
            3 if state=="BUY NOW" else
            2 if state=="PRE-IGNITION" else
            1 if state in {"EARLY OPPORTUNITY","WATCH"} or pstate in {"BUY NOW","PINPOINT ARMED","SETUP READY"} else 0
        )
        if rank:
            ranked.append((rank,f(row.get("score")),str(sym)))
    for _,_,sym in sorted(ranked,reverse=True):
        add(sym)

    for sym in scope:
        add(sym)
    return out


async def _watchdog_structure_rescue(scope, fresh_cov):
    """Non-blocking execution-structure rescue.

    Priority 1: stale-but-seeded execution symbols use a direct bounded REST
    lane, bypassing a stalled WS-API kline route.
    Priority 2: if no stale seeded symbols remain, bootstrap one missing seed
    set through the normal adaptive loader so FAST recovery can take over.
    """
    global _watchdog_last_structure_rescue
    now=time.time()
    if now-_watchdog_last_structure_rescue<WATCHDOG_STRUCTURE_RESCUE_COOLDOWN_S:
        return {"attempted":0,"ok":0,"symbols":[],"cooldown":True}

    priority=_watchdog_structure_priority(scope)
    stale_seeded=[
        sym for sym in priority
        if sym not in _recovery_inflight
        and _raw_seed_count(sym)>=3
        and _structure_age_recovery(sym)>RECOVERY_STALE_S
    ]
    missing=[
        sym for sym in priority
        if sym not in _recovery_inflight
        and _raw_seed_count(sym)<3
        and _structure_age_recovery(sym)>RECOVERY_STALE_S
    ]

    critical=fresh_cov<WATCHDOG_STRUCTURE_CRITICAL_FRESH
    if stale_seeded:
        count=min(
            WATCHDOG_STRUCTURE_STALE_RESCUE_BATCH if critical else WATCHDOG_STRUCTURE_RESCUE_BATCH,
            len(stale_seeded),
        )
        chosen=stale_seeded[:count]
        lane="WATCHDOG_REST"
        kind="STALE"
    elif missing:
        chosen=missing[:1]
        lane="WATCHDOG"
        kind="SEED"
    else:
        _watchdog_last_structure_rescue=now
        return {"attempted":0,"ok":0,"symbols":[],"cooldown":False}

    for sym in chosen:
        _recovery_retry_after.pop(sym,None)
        if lane=="WATCHDOG_REST":
            _reset_structure_route(sym)

    _watchdog_last_structure_rescue=now
    recovery_stats["rescue_cycles"]+=1
    started=time.time()
    before={sym:_raw_seed_count(sym) for sym in chosen}
    results=await asyncio.gather(*[_hydrate_one(sym,lane) for sym in chosen])
    after={sym:_raw_seed_count(sym) for sym in chosen}
    ok=sum(x is True for x in results)
    fail=len(chosen)-ok
    watchdog_stats["structure_rescues"]+=len(chosen)
    watchdog_stats["structure_rescue_ok"]+=ok
    if kind=="STALE":
        watchdog_stats["structure_stale_rescues"]+=len(chosen)
    else:
        watchdog_stats["structure_seed_rescues"]+=len(chosen)
    _save_structure_raw_cache()
    payload={
        "attempted":len(chosen),"ok":ok,"fail":fail,"symbols":chosen,
        "kind":kind,"cooldown":False,"seeds_before":before,"seeds_after":after,
        "seconds":time.time()-started,
    }
    print(
        f"Ψ-WATCHDOG STRUCTURE_{kind}_RESCUE attempted={len(chosen)} ok={ok} fail={fail} "
        f"symbols={chosen} seedsBefore={before} seedsAfter={after} "
        f"sec={payload['seconds']:.2f} rescueOK={recovery_stats['rescue_ok']} "
        f"rescueFail={recovery_stats['rescue_fail']} staleOK={recovery_stats['rescue_stale_ok']} "
        f"staleFail={recovery_stats['rescue_stale_fail']} routeResets={recovery_stats['route_resets']}",
        flush=True,
    )
    return payload


def _watchdog_pinpoint_count():
    n=0
    for sym,row in list(getattr(q,"latest",{}).items()):
        if sym not in getattr(q,"universe_set",set()) or not isinstance(row,dict):
            continue
        if any(k.startswith("pinpoint_") for k in row.keys()):
            n+=1
    return n

async def watchdog_loop():
    global _watchdog_last_ever_cov,_watchdog_last_cov_progress
    global _watchdog_last_pool,_watchdog_last_pool_progress
    global _watchdog_last_shards,_watchdog_last_shard_progress
    global _watchdog_last_structure_rescue
    global _watchdog_structure_rescue_task,_watchdog_last_rescue_result

    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)

    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_S)
        watchdog_stats["cycles"]+=1
        actions=[]
        try:
            now=time.time()

            # Harvest the previous background seed bootstrap without ever
            # blocking the Watchdog cadence.
            if _watchdog_structure_rescue_task is not None and _watchdog_structure_rescue_task.done():
                try:
                    _watchdog_last_rescue_result=_watchdog_structure_rescue_task.result()
                    result=_watchdog_last_rescue_result or {}
                    if int(result.get("attempted",0))>0:
                        actions.append(f"STRUCTURE_SEED_DONE:{result.get('ok',0)}/{result.get('attempted',0)}")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    watchdog_stats["errors"]+=1
                    actions.append("STRUCTURE_SEED_TASK_FAIL")
                    print(f"Ψ-WATCHDOG ERROR STRUCTURE_SEED_TASK {type(exc).__name__}: {exc}",flush=True)
                finally:
                    _watchdog_structure_rescue_task=None

            scope=_recovery_scope()
            total=len(scope)
            ever_cov=sum(1 for s in scope if _structure_age_recovery(s)<999000)
            fresh_cov=sum(1 for s in scope if _structure_age_recovery(s)<=RECOVERY_STALE_S)

            exec_scope=_watchdog_execution_scope(scope)
            exec_total=len(exec_scope)
            exec_ever=sum(1 for s in exec_scope if _structure_age_recovery(s)<999000)
            exec_fresh=sum(1 for s in exec_scope if _structure_age_recovery(s)<=RECOVERY_STALE_S)

            pool=len(getattr(app,"selected_micro_symbols",[]) or [])
            pin=_watchdog_pinpoint_count()
            shards=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS))
            ext_age=(now-f(extrest.ext_last_refresh,0.0)) if f(extrest.ext_last_refresh,0.0)>0 else 999999.0
            startup_age=now-_watchdog_started

            if exec_ever>_watchdog_last_ever_cov:
                _watchdog_last_ever_cov=exec_ever
                _watchdog_last_cov_progress=now
            if pool>_watchdog_last_pool:
                _watchdog_last_pool=pool
                _watchdog_last_pool_progress=now
            if shards>_watchdog_last_shards:
                _watchdog_last_shards=shards
                _watchdog_last_shard_progress=now

            # Emergency extension refresh only when the canonical loop has gone stale.
            if startup_age>WATCHDOG_STARTUP_GRACE_S and ext_age>WATCHDOG_EXT_STALE_S:
                try:
                    n=await asyncio.wait_for(_watchdog_refresh_extension(),timeout=12.0)
                    watchdog_stats["actions"]+=1
                    actions.append(f"EXT_REFRESH:{n}")
                    print(f"Ψ-WATCHDOG ACTION EXT_REFRESH symbols={n} priorAge={ext_age:.1f}s",flush=True)
                    ext_age=0.0
                except Exception as exc:
                    watchdog_stats["errors"]+=1
                    actions.append("EXT_REFRESH_FAIL")
                    print(f"Ψ-WATCHDOG ERROR EXT_REFRESH {type(exc).__name__}: {exc}",flush=True)

            # Seed expansion is non-blocking. FAST recovery owns all stale
            # already-seeded structure; Watchdog only schedules one missing
            # execution seed-set at a time in the background.
            required_exec_fresh=min(exec_total,16) if exec_total>0 else min(total,16)
            structure_critical=exec_fresh<min(required_exec_fresh,WATCHDOG_STRUCTURE_CRITICAL_FRESH)
            structure_stalled=now-_watchdog_last_cov_progress>WATCHDOG_STRUCTURE_STALL_S

            rescue_priority=_watchdog_structure_priority(exec_scope)
            rescue_candidates=[
                sym for sym in rescue_priority
                if sym not in _recovery_inflight
                and _structure_age_recovery(sym)>RECOVERY_STALE_S
            ]
            if (
                startup_age>WATCHDOG_STARTUP_GRACE_S
                and required_exec_fresh>0
                and exec_fresh<required_exec_fresh
                and rescue_candidates
                and (structure_critical or structure_stalled)
                and _watchdog_structure_rescue_task is None
                and now-_watchdog_last_structure_rescue>=WATCHDOG_STRUCTURE_RESCUE_COOLDOWN_S
            ):
                seeded=sum(_raw_seed_count(s)>=3 for s in rescue_candidates)
                _watchdog_structure_rescue_task=asyncio.create_task(
                    _watchdog_structure_rescue(exec_scope,exec_fresh)
                )
                watchdog_stats["structure_kicks"]+=1
                watchdog_stats["actions"]+=1
                actions.append(f"STRUCTURE_RESCUE_SCHEDULED:{len(rescue_candidates)}")
                print(
                    f"Ψ-WATCHDOG ACTION STRUCTURE_RESCUE_SCHEDULED candidates={len(rescue_candidates)} "
                    f"seeded={seeded} missing={len(rescue_candidates)-seeded} "
                    f"execFresh={exec_fresh}/{exec_total} execEver={exec_ever}/{exec_total}",
                    flush=True,
                )

            # Continuity is no longer allowed to freeze at a small sticky pool.
            # Whenever verified fresh structure materially exceeds the current
            # pool, ask the guarded continuity allocator to expand/rotate it.
            desired_pool=min(int(getattr(base,"POOL_SIZE",80)),total,max(16,fresh_cov))
            if (
                startup_age>WATCHDOG_STARTUP_GRACE_S
                and fresh_cov>=min(16,total)
                and pool<desired_pool
                and fresh_cov>=pool+3
                and now-_watchdog_last_pool_progress>min(WATCHDOG_POOL_STALL_S,35.0)
            ):
                try:
                    before_pool=pool
                    await continuity_guard.rebalance_continuity_guarded(force=True)
                    new_pool=len(getattr(app,"selected_micro_symbols",[]) or [])
                    watchdog_stats["pool_kicks"]+=1
                    watchdog_stats["actions"]+=1
                    actions.append(f"POOL_REBALANCE:{before_pool}->{new_pool}")
                    print(f"Ψ-WATCHDOG ACTION POOL_REBALANCE before={before_pool} after={new_pool} desired={desired_pool} structure={fresh_cov}/{total}",flush=True)
                    _watchdog_last_pool=max(_watchdog_last_pool,new_pool)
                    _watchdog_last_pool_progress=now
                except Exception as exc:
                    watchdog_stats["errors"]+=1
                    actions.append("POOL_REBALANCE_FAIL")
                    print(f"Ψ-WATCHDOG ERROR POOL_REBALANCE {type(exc).__name__}: {exc}",flush=True)

            # If the continuity pool exists but shard assignments remain absent,
            # ask the existing guarded shard allocator to repair the mapping.
            if (
                startup_age>WATCHDOG_STARTUP_GRACE_S
                and pool>0 and shards<tape.SHARDS
                and now-_watchdog_last_shard_progress>WATCHDOG_SHARD_STALL_S
            ):
                try:
                    changed=continuity_guard.assign_shards_guarded()
                    watchdog_stats["shard_kicks"]+=1
                    watchdog_stats["actions"]+=1
                    actions.append(f"SHARD_ASSIGN:{len(changed or [])}")
                    print(f"Ψ-WATCHDOG ACTION SHARD_ASSIGN pool={pool} shards={shards}/{tape.SHARDS} changed={sorted(list(changed or []))}",flush=True)
                    _watchdog_last_shard_progress=now
                except Exception as exc:
                    watchdog_stats["errors"]+=1
                    actions.append("SHARD_ASSIGN_FAIL")
                    print(f"Ψ-WATCHDOG ERROR SHARD_ASSIGN {type(exc).__name__}: {exc}",flush=True)

            ext_live=ext_age<=WATCHDOG_EXT_STALE_S
            min_fresh=required_exec_fresh
            structure_ready=(
                startup_age<=WATCHDOG_STARTUP_GRACE_S
                or required_exec_fresh==0
                or exec_fresh>=required_exec_fresh
            )
            rest_recent_ok=(_rest_last_fail<=0 or now-_rest_last_fail>60.0)
            continuity_ok=(startup_age<=WATCHDOG_STARTUP_GRACE_S or pool>0)
            shard_ok=(pool==0 or shards==tape.SHARDS or now-_watchdog_last_shard_progress<=WATCHDOG_SHARD_STALL_S)

            live_micro=0
            for sym in list(getattr(app,"selected_micro_symbols",[]) or []):
                try:
                    if bool(app.micro_metrics(sym).get("micro_ready")):
                        live_micro+=1
                except Exception:
                    pass
            execution_micro_ok=(
                startup_age<=WATCHDOG_STARTUP_GRACE_S
                or pool==0
                or live_micro>0
            )

            health_reasons=[]
            if not structure_ready: health_reasons.append("STRUCTURE_COVERAGE")
            if not execution_micro_ok: health_reasons.append("EXECUTION_MICRO")
            if not shard_ok: health_reasons.append("MONSTER_SHARDS")
            if not ext_live: health_reasons.append("EXTENSION")
            if not rest_recent_ok: health_reasons.append("REST_HEALTH")
            if not continuity_ok: health_reasons.append("CONTINUITY")
            healthy=not health_reasons

            # RiskMap diagnostics: report actual live map/cache state. The old
            # "riskCache" field was only a cache-hit counter and could show 0
            # while valid RiskMap plans already existed.
            _risk_map_cache=getattr(move_engine.riskmap,"risk_cache",{}) or {}
            _risk_map_max_age=float(getattr(move_engine.riskmap,"SUPPORT_MAX_AGE",120.0) or 120.0)
            _risk_map_fresh=[
                ri for ri in _risk_map_cache.values()
                if isinstance(ri,dict) and now-f(ri.get("updated"))<=_risk_map_max_age
            ]
            _risk_map_tracked=len(_risk_map_fresh)
            _risk_map_plans=sum(
                f(ri.get("entry_trigger"))>0 and f(ri.get("stop_loss"))>0
                for ri in _risk_map_fresh
            )
            if healthy: watchdog_stats["healthy"]+=1
            else: watchdog_stats["degraded"]+=1
            status="HEALTHY" if healthy else "RECOVERING"
            reason="OK" if healthy else ",".join(health_reasons)
            print(
                f"Ψ-WATCHDOG status={status} reason={reason} cycle={watchdog_stats['cycles']} "
                f"structureFreshExec={exec_fresh}/{exec_total} structureEverExec={exec_ever}/{exec_total} "
                f"structureFreshScope={fresh_cov}/{total} structureEverScope={ever_cov}/{total} "
                f"pinpoint={pin} pool={pool}/{getattr(base,'POOL_SIZE',80)} liveMicro={live_micro}/{pool} "
                f"monsterShards={shards}/{tape.SHARDS} extAge={ext_age:.1f}s "
                f"restOK={_rest_stats['ok']} restFail={_rest_stats['fail']} restRetry={_rest_stats['attempt_fail']} wsApi={'UP' if (_ws_api_ready is not None and _ws_api_ready.is_set()) else 'DOWN'} wsApiKlineOK={_ws_api_stats['ok']} wsApiKlineFail={_ws_api_stats['fail']} wsStruct={_ws_api_stats['structure_ok']} wdRescueOK={recovery_stats['rescue_ok']} wdRescueFail={recovery_stats['rescue_fail']} wdStaleOK={recovery_stats['rescue_stale_ok']} wdStaleFail={recovery_stats['rescue_stale_fail']} wdRouteReset={recovery_stats['route_resets']} wdRestOK={_structure_tf_stats['watchdog_rest_ok']} wdRestFail={_structure_tf_stats['watchdog_rest_fail']} wsRisk={_ws_api_stats['risk_ok']} riskTfCacheSize={len(_risk_tf_cache)} riskTfCacheHits={_risk_tf_stats['cache_hit']} riskMapTracked={_risk_map_tracked} riskPlans={_risk_map_plans} riskWsOK={_risk_tf_stats['ws_ok']} riskWsRetry={_risk_tf_stats['ws_retry_ok']} riskRestOK={_risk_tf_stats['rest_ok']} riskTfFail={_risk_tf_stats['fail']} riskRestRouteOK={_rest_stats['risk_ok']} riskFail={_rest_stats['risk_fail']} riskDefer={_rest_stats['risk_defer']} bgOK={_rest_stats['bg_ok']} bgFail={_rest_stats['bg_fail']} bgDefer={_rest_stats['bg_defer']} "
                f"actions={actions or ['NONE']} totals={watchdog_stats}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            watchdog_stats["errors"]+=1
            print(f"Ψ-WATCHDOG ERROR LOOP {type(exc).__name__}: {exc}",flush=True)

async def main():
    # Expose one integrated runtime version even though historical feature
    # modules keep their own lineage versions.
    for mod in (scanner,base,rescue,move_engine,stable_core,target_core,qualifier_core):
        try: mod.VERSION=VERSION
        except Exception: pass
    print("[v11.0.5.58] Ψ INDEPENDENT FEED-FALLBACKS + NON-BLOCKING EXECUTION-SCOPE WATCHDOG active — native micro readiness now drives formal integrity, event tape is only mandatory for event-dependent Monster states, pullback uses the corrected live gate, Pinpoint/formal aliases are synchronised, and BUY accepts a valid Pinpoint trigger/stop risk plan with RiskMap as fallback. RiskMap remains reliable and fully diagnosed. Qualified aggTrade reuses the stable full-universe Monster Binance feed, while the four execution shards carry depth20 only. An assigned shard is now immutable until its current websocket generation has processed a real valid depth20 frame; the 12-second rebalance dwell begins from that first verified depth frame. Watchdog separates execution structure health from rotating discovery coverage. Missing execution raw seeds are bootstrapped one symbol at a time in a background task, while FAST recovery exclusively owns already-seeded stale structure, keeping the Watchdog cadence non-blocking. Watchdog now adds an independent bounded direct-REST rescue lane for stale execution structure while normal FAST recovery remains WS-first. Health thresholds, signal thresholds and Pinpoint BUY authority are unchanged.",flush=True)
    await asyncio.gather(rescue.main(), binance_ws_api_loop(), structure_kline_ws_loop(), structure_recovery_loop(), cold_seed_loop(), structure_cache_loop(), watchdog_loop())

if __name__=="__main__":asyncio.run(main())
