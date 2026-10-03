import asyncio, json, math, statistics, time, os, contextvars
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
VERSION="11.0.5.21-breakout-structural-intelligence"

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
_structure_active = 0
_rest_good_host = {}
_rest_host_bad_until = {}
_rest_stats = {"ok":0,"fail":0,"attempt_fail":0,"failover":0,"host_ok":{},"host_fail":{},"gate_timeout":0,"risk_ok":0,"risk_fail":0,"risk_defer":0,"bg_ok":0,"bg_fail":0,"bg_defer":0}
_rest_last_fail = 0.0
_risk_last_ok = 0.0
_risk_last_fail = 0.0

def _rest_gates(path):
    global _rest_global_gate, _rest_kline_gate, _rest_structure_gate, _rest_risk_kline_gate, _rest_bg_kline_gate, _rest_depth_gate
    if _rest_global_gate is None:
        _rest_global_gate = asyncio.Semaphore(14)
    if _rest_kline_gate is None:
        _rest_kline_gate = asyncio.Semaphore(12)
    if _rest_structure_gate is None:
        _rest_structure_gate = asyncio.Semaphore(6)
    if _rest_risk_kline_gate is None:
        _rest_risk_kline_gate = asyncio.Semaphore(3)
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
                print(f"Ψ-REST ROUTE active={host} hosts={len(REST_BASES)} global=14 klines=12(structure<=6+risk=3+background=1) depth=1 keepalive=ON",flush=True)
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

# Structure-timeframe resilience. A structure build needs 1h + 4h + 15m.
# Cache only successful short-lived payloads, so if one sibling timeframe fails
# the next retry reuses the verified siblings and refetches only the missing one.
_original_load_klines = app.load_klines
_structure_tf_cache = {}
_structure_symbol_gates = {}
_structure_raw_cache = {}
_structure_raw_dirty = False
STRUCTURE_RAW_CACHE_PATH = os.environ.get(
    "PSI_STRUCTURE_RAW_CACHE_PATH",
    "/data/psi_v11_structure_raw.json" if os.path.isdir("/data") else "/app/psi_v11_structure_raw.json",
)
STRUCTURE_TF_CACHE_S = 180.0
STRUCTURE_TF_RETRY_DELAY_S = 0.12
STRUCTURE_TF_ATTEMPTS = 3
STRUCTURE_RAW_MAX_INCREMENTAL_BARS = 48
_structure_tf_stats = {
    "cache_hit":0,"fetch_ok":0,"retry_ok":0,"fail":0,
    "raw_load":0,"raw_save":0,"raw_hit":0,"incremental_ok":0,"full_seed":0,
}

def _raw_key(symbol, interval, limit):
    return f"{symbol}|{interval}|{int(limit)}"

def _interval_ms(interval):
    return {"15m":900000,"1h":3600000,"4h":14400000}.get(str(interval),0)

def _minimum_structure_rows(interval):
    return 205 if str(interval) in {"1h","4h"} else 22

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

def _structure_symbol_gate(symbol):
    key=str(symbol)
    gate=_structure_symbol_gates.get(key)
    if gate is None:
        gate=asyncio.Semaphore(1)
        _structure_symbol_gates[key]=gate
    return gate

STRUCTURE_RACE_HOSTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
    "https://data-api.binance.vision",
]

async def _structure_fetch_race(client, symbol, interval, limit):
    symbol=str(symbol); interval=str(interval); limit=int(limit)
    route_key=f"structure_klines:{symbol}"
    hosts=list(STRUCTURE_RACE_HOSTS)
    preferred=_rest_good_host.get(route_key)
    if preferred in hosts:
        hosts=[preferred]+[h for h in hosts if h!=preferred]
    elif symbol:
        offset=sum(ord(ch) for ch in symbol)%len(hosts)
        hosts=hosts[offset:]+hosts[:offset]

    now=time.time()
    healthy=[h for h in hosts if _rest_host_bad_until.get((route_key,h),0)<=now]
    ordered=healthy+[h for h in hosts if h not in healthy]
    timeout_s=3.8 if limit<=10 else 6.2
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

async def _structure_resilient_load_klines(client, symbol, interval, limit):
    global _structure_raw_dirty
    if not _structure_request_ctx.get():
        return await _original_load_klines(client, symbol, interval, limit)

    symbol=str(symbol); interval=str(interval); limit=int(limit)
    key=(symbol,interval,limit)
    now=time.time()
    cached=_structure_tf_cache.get(key)
    if cached and now-float(cached[0])<=STRUCTURE_TF_CACHE_S:
        _structure_tf_stats["cache_hit"]+=1
        return cached[1]

    raw_key=_raw_key(symbol,interval,limit)
    raw_entry=_structure_raw_cache.get(raw_key) or {}
    seed=raw_entry.get("rows") if isinstance(raw_entry,dict) else None
    if isinstance(seed,list) and seed:
        _structure_tf_stats["raw_hit"]+=1

    async with _structure_symbol_gate(symbol):
        cached=_structure_tf_cache.get(key)
        if cached and time.time()-float(cached[0])<=STRUCTURE_TF_CACHE_S:
            _structure_tf_stats["cache_hit"]+=1
            return cached[1]

        raw_entry=_structure_raw_cache.get(raw_key) or {}
        seed=raw_entry.get("rows") if isinstance(raw_entry,dict) else None
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
            rows=await _structure_fetch_race(client, symbol, interval, request_limit)
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
                full=await _structure_fetch_race(client,symbol,interval,full_limit)
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

async def _risk_load_klines(client, symbol, interval, limit):
    token = _risk_plan_request_ctx.set(True)
    try:
        return await app.load_klines(client, symbol, interval, limit)
    finally:
        _risk_plan_request_ctx.reset(token)

# V10.19.1 resolves this attribute at call time. Risk-plan candles therefore
# use two dedicated slots and cannot be crowded out by lifecycle/MTF enrichers.
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
    return out
base.candidate=candidate_v5

def _live_pullback_exhaustion(sym,ca):
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
    buy=f(ca.get("buy1s"),.5); cvd=f(ca.get("cvd1s")); tape=f(ca.get("eventTape"))
    layers=int(f(ca.get("layers"))); bsi=f(ca.get("bsi")); reasons=set(ca.get("reasons") or [])
    structure_ok=layers>=3 or bsi>=52
    pulled=.12<=depth<=5.0
    buyer_return=(buy>=.58 and cvd>=.10) or ("OFI_POS" in reasons and buy>=.54) or (tape>=65 and buy>=.55)
    reclaim=rebound>=.05
    exhausting=pulled and structure_ok and ((buy>=.52 and cvd>=-.05) or "OFI_POS" in reasons)
    exhausted=pulled and structure_ok and buyer_return and reclaim
    score=0.0
    if pulled:
        score+=min(28.0,8.0+depth*8.0)
    score+=min(18.0,max(0.0,(buy-.50)*90.0))
    score+=min(16.0,max(0.0,cvd*16.0))
    score+=min(15.0,tape*.15)
    score+=min(12.0,layers*2.0)
    score+=min(8.0,rebound*18.0)
    if "OFI_POS" in reasons: score+=5.0
    state="PULLBACK_EXHAUSTED" if exhausted else "SELL_PRESSURE_EXHAUSTING" if exhausting else "PULLBACK_ONLY" if pulled else "NONE"
    return {
        "state":state,
        "depth":round(depth,3),
        "rebound":round(rebound,3),
        "score":round(cl(score,0,100),1),
        "buy":buy,"cvd":cvd,"tape":tape,
    }

def scan_v5():
    now=time.time();u=list(getattr(q,"universe",[]) or []);rows=[]
    for sym in u:
        row=q.latest.get(sym) or {};p=base.px(sym,row)
        if p>0 and (not base.price_hist[sym] or now-base.price_hist[sym][-1][0]>=.45):base.price_hist[sym].append((now,p))
        c=base.cheap(sym,row,now);em,rs=rescue._emergency_promote(c,row);c["rescue_score"]=rs;c["rescue_promote"]=em;rows.append((f(c.get("cheap")),rs,sym,row,c))
    rows.sort(reverse=True,key=lambda x:x[0]);pool=[(a,s,r,c) for a,_,s,r,c in rows[:base.DEEP_LIMIT]];seen={s for _,s,_,_ in pool}
    for a,rs,s,r,c in rows:
        if s not in seen and (f(c.get("peak"))>=100 or f(c.get("radar_n"))>=.45 or f(c.get("r60"))>=.75):pool.append((a,s,r,c));seen.add(s)
    emergency=sorted([x for x in rows if x[2] not in seen and x[4].get("rescue_promote")],key=lambda x:(x[1],x[0]),reverse=True)[:rescue.EXTRA_RESCUE_SLOTS]
    for a,rs,s,r,c in emergency:
        if len(pool)>=rescue.MAX_DEEP_POOL:break
        pool.append((a,s,r,c));seen.add(s);rescue.rescue_stats["emergency_promotions"]+=1
    out=[]
    for _,s,row,c in pool:
        ca=base.candidate(s,row,c,base.deep(s))
        ex=_live_pullback_exhaustion(s,ca)
        ca.update({
            "monsterPullbackState":ex["state"],
            "monsterPullbackDepth":ex["depth"],
            "monsterPullbackRebound":ex["rebound"],
            "monsterExhaustionScore":ex["score"],
        })
        if ex["state"]=="PULLBACK_EXHAUSTED":
            rs=list(ca.get("reasons") or [])
            if "PULLBACK_SELL_EXHAUSTED" not in rs: rs.append("PULLBACK_SELL_EXHAUSTED")
            ca["reasons"]=rs
        elif ex["state"]=="SELL_PRESSURE_EXHAUSTING":
            rs=list(ca.get("reasons") or [])
            if "SELL_PRESSURE_EXHAUSTING" not in rs: rs.append("SELL_PRESSURE_EXHAUSTING")
            ca["reasons"]=rs
        base.latest[s]=ca
        visible=(f(ca.get("early"))>=38 or f(ca.get("dna"))>=50 or f(ca.get("peak"))>=120 or ca.get("state") in {"MONSTER-RESCUE","MONSTER-MEMORY"} or f(ca.get("retentionScore"))>=58 or f(ca.get("bsi"))>=62 or ex["state"] in {"PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING"})
        if visible:out.append(ca);base.open_obs(ca)
    priority={"MONSTER-HOT":6,"MONSTER-IGNITION":5,"MONSTER-MEMORY":4,"MONSTER-RESCUE":3,"MONSTER-SEED":2,"MONSTER-EXTENDED":1,"MONSTER-WATCH":0}
    out.sort(key=lambda x:(priority.get(str(x.get("state")),0),f(x.get("bsi")),f(x.get("retentionScore")),f(x.get("early")),f(x.get("dna")),f(x.get("peak"))),reverse=True)
    base.stats["cycles"]+=1;base.stats["universe"]=len(u);base.stats["deep"]=len(pool);base.stats["cand"]=len(out);rescue.rescue_stats["last_pool"]=len(pool);rescue.rescue_stats["last_emergency"]=len(emergency)
    base.latest["_all_candidates"]=list(out)
    return out[:BOARD_ROWS]
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
    valid=entry>0 and stop>0 and stop<entry and tp1>entry and tp2>tp1 and tp3>tp2
    return {
        "source":source,
        "risk_state":risk_state,
        "valid":valid,
        "entry":entry if entry>0 else conditional,
        "stop":stop if valid else 0.0,
        "tp1":tp1 if valid else 0.0,
        "tp2":tp2 if valid else 0.0,
        "tp3":tp3 if valid else 0.0,
        "runner":runner if valid and runner>tp3 else 0.0,
        "shadow":shadow,
    }

def _monster_risk_priority():
    out=[];seen=set()
    for r in list(base.latest.get("_all_candidates") or []):
        sym=str(r.get("symbol") or "")
        if sym and sym not in seen:
            out.append(sym);seen.add(sym)
    return out

move_engine.riskmap.priority_symbols_provider = _monster_risk_priority

def _bsi_learning():
    src=[x for x in base.resolved if isinstance(x,dict) and isinstance(x.get("features"),dict) and "bsi_n" in x["features"]]
    if not src:return {"status":"WARMING","n":0}
    hi=[x for x in src if f(x["features"].get("bsi_n"))>=.70]
    return {"status":"ACTIVE" if len(src)>=30 else "WARMING","n":len(src),"high":len(hi),"p10":(sum(f(x.get("max_return_pct"))>=10 for x in hi)/len(hi)) if hi else None,"p20":(sum(f(x.get("max_return_pct"))>=20 for x in hi)/len(hi)) if hi else None,"mfe":statistics.mean(f(x.get("max_return_pct")) for x in hi) if hi else None}

async def board_loop_v5():
    while True:
        await asyncio.sleep(base.BOARD_S)
        try:
            base.refresh_adapt();rows=list(base.latest.get("_board") or []);all_rows=list(base.latest.get("_all_candidates") or rows);states=("MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED","MONSTER-EXTENDED");counts={k:sum(r.get("state")==k for r in all_rows) for k in states};ups=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS));ready=sum(1 for s in list(getattr(q,"universe",[]) or []) if tape.tape_metric(s).get("ready"))
            print(f"Ψ-MONSTER-RADAR BOARD scanned={base.stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={base.stats['deep']} candidates={base.stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} memory={counts['MONSTER-MEMORY']} rescue={counts['MONSTER-RESCUE']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} rows={len(rows)}/{BOARD_ROWS} allRows={len(all_rows)} scan={int(base.SCAN_S*1000)}ms tape={ready}/{len(getattr(q,'universe',[]) or [])} shards={ups}/{tape.SHARDS} trades={tape.tape_stats['trades']} books={tape.tape_stats['books']} learning={base.adapt['status']} obsPending={len(base.pending)} obsResolved={len(base.resolved)} PinpointAuthority=YES BSI=ON",flush=True)
            move_rows=[]
            for r in all_rows:
                mp=_candidate_move_plan(r)
                sh=mp["shadow"]
                r["movePlanV11"]=mp
                move_rows.append(r)
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
                    f"riskState={mp.get('risk_state')} src={mp.get('source')}",
                    flush=True,
                )
            exrows=[r for r in all_rows if str(r.get("monsterPullbackState")) in {"PULLBACK_EXHAUSTED","SELL_PRESSURE_EXHAUSTING","PULLBACK_ONLY"}]
            exrows.sort(key=lambda r:(2 if r.get("monsterPullbackState")=="PULLBACK_EXHAUSTED" else 1 if r.get("monsterPullbackState")=="SELL_PRESSURE_EXHAUSTING" else 0,f(r.get("monsterExhaustionScore")),f(r.get("bsi"))),reverse=True)
            print(f"Ψ-MONSTER-PULLBACK-EXHAUSTION BOARD candidates={len(exrows)} exhausted={sum(r.get('monsterPullbackState')=='PULLBACK_EXHAUSTED' for r in exrows)} exhausting={sum(r.get('monsterPullbackState')=='SELL_PRESSURE_EXHAUSTING' for r in exrows)}",flush=True)
            for j,r in enumerate(exrows,1):
                print(f"PX{j:02d}. {r.get('symbol'):<14} state={r.get('monsterPullbackState'):<24} score={f(r.get('monsterExhaustionScore')):5.1f} depth={f(r.get('monsterPullbackDepth')):5.2f}% rebound={f(r.get('monsterPullbackRebound')):5.2f}% BSI={f(r.get('bsi')):5.1f} layers={int(f(r.get('layers')))}/6 tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} formal={r.get('formal')} pp={r.get('pp')}",flush=True)
            for i,r in enumerate(rows,1):
                ds="-" if r.get("dist") is None else f"{f(r.get('dist')):+.2f}%";age=f(r.get("peak20Age"),999999);mem="-" if age>rescue.MEMORY_WINDOW_S else f"{100*f(r.get('peakShP20_120')):.1f}%/{age:.0f}s"
                print(f"MR{i:02d}. {r['symbol']:<14} state={str(r.get('state')):<17} BSI={f(r.get('bsi')):5.1f} {str(r.get('bsiState')):<19} FB={f(r.get('falseBreakRisk')):4.0f} comp={f(r.get('bsiCompression')):4.0f} tests={f(r.get('bsiTests')):4.0f} retest={f(r.get('bsiRetest')):4.0f} trend={f(r.get('bsiTrend')):4.0f} EARLY={f(r.get('early')):5.1f} DNA={f(r.get('dna')):5.1f} retain={f(r.get('retentionScore')):5.1f} rapid={f(r.get('rapid')):6.1f}/{f(r.get('peak')):6.1f} tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} event={f(r.get('event')):4.0f} vac={f(r.get('vac')):4.0f} pB15={100*f(r.get('pb15')):4.1f}% shP20={100*f(r.get('sp20')):4.1f}% mem20={mem} layers={int(f(r.get('layers')))}/6 dist={ds} formal={r.get('formal')} pp={r.get('pp')} moveScore={f(((r.get('movePlanV11') or {}).get('shadow') or {}).get('move_score')):4.0f}/100 expUp={f(((r.get('movePlanV11') or {}).get('shadow') or {}).get('expected_excursion_pct')):4.1f}% entry={_fmt_px((r.get('movePlanV11') or {}).get('entry'))} tp3={_fmt_px((r.get('movePlanV11') or {}).get('tp3'))} why={(r.get('reasons') or [])[:10]}",flush=True)
            top=sorted(rows,key=lambda r:f(r.get("bsi")),reverse=True)[:8];print(f"Ψ-BSI BOARD top={[(r.get('symbol'),round(f(r.get('bsi')),1),r.get('bsiState'),round(f(r.get('falseBreakRisk')),1),round(f(r.get('bsiCompression')),1),round(f(r.get('bsiTests')),1),r.get('pp')) for r in top]}",flush=True);print(f"Ψ-BSI LEARNING {_bsi_learning()}",flush=True)
            br,bn=rescue._blocker_learning();print("Ψ-MONSTER-BLOCKER-LEARN "+(f"resolved={bn} top={[(b,n,round(w10*100,1),round(w20*100,1),round(mfe,2)) for w20,w10,mfe,n,b in br]}" if bn else "status=WARMING resolved=0"),flush=True);paths=rescue._path_learning();print(f"Ψ-MONSTER-PATH-LEARN {paths}" if paths else "Ψ-MONSTER-PATH-LEARN status=WARMING",flush=True);print(f"Ψ-MONSTER-RESCUE HEALTH emergencyPromotions={rescue.rescue_stats['emergency_promotions']} lastEmergency={rescue.rescue_stats['last_emergency']} deepPool={rescue.rescue_stats['last_pool']} ignitionEpisodes={rescue.rescue_stats['ignition_episodes']} memoryWindow={int(rescue.MEMORY_WINDOW_S)}s buyAuthority=PINPOINT_ONLY",flush=True);rescue._save_rescue()
        except asyncio.CancelledError:rescue._save_rescue(True);raise
        except Exception as e:base.stats["board_errors"]+=1;print(f"Ψ-BSI BOARD_ERROR {type(e).__name__}: {e}",flush=True)
base.board_loop=board_loop_v5

for mod in (rescue,tape,base,getattr(base,"scientist",None),scanner):
    try:mod.VERSION=VERSION
    except Exception:pass


RECOVERY_BATCH = 3
RECOVERY_PRIORITY = 80
RECOVERY_STALE_S = 285.0
recovery_stats = {"passes":0,"ok":0,"fail":0,"fast_ok":0,"fast_fail":0,"seed_ok":0,"seed_fail":0,"seed_cycles":0,"pool_kicks":0,"ext_ok":0,"ext_err":0,"cache_load":0,"cache_save":0}
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
    return _recovery_symbols()[:RECOVERY_PRIORITY]

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
WATCHDOG_POOL_STALL_S = 75.0
WATCHDOG_SHARD_STALL_S = 45.0
WATCHDOG_EXT_STALE_S = 50.0
watchdog_stats = {
    "cycles":0,"healthy":0,"degraded":0,"actions":0,"errors":0,
    "ext_refresh":0,"structure_kicks":0,"pool_kicks":0,"shard_kicks":0,
}
_watchdog_started = time.time()
_watchdog_last_ever_cov = 0
_watchdog_last_cov_progress = time.time()
_watchdog_last_pool = 0
_watchdog_last_pool_progress = time.time()
_watchdog_last_shards = 0
_watchdog_last_shard_progress = time.time()

def _structure_age_recovery(sym):
    ts = int(q.structure_ms.get(sym,0) or 0)
    return 999999.0 if ts <= 0 else max(0.0,(q.ms()-ts)/1000.0)

def _recovery_symbols():
    out=[];seen=set()
    def add(s):
        s=str(s or "")
        if s and s not in seen and s in set(getattr(q,"universe_set",set()) or set()):
            seen.add(s);out.append(s)
    # Once continuity is populated, protect the actual execution pool first.
    for s in list(getattr(app,"selected_micro_symbols",[]) or []): add(s)
    for s in ("BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","DOGEUSDT","ADAUSDT","LINKUSDT","SUIUSDT","LTCUSDT","AVAXUSDT","DOTUSDT","AAVEUSDT","TAOUSDT","FETUSDT","NEARUSDT","ICPUSDT","ONDOUSDT","PEPEUSDT","SHIBUSDT"):
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
    if lane=="FAST":
        _fast_recovery_active += 1
    try:
        if app.session is None or app.session.closed:
            raise RuntimeError("shared REST session unavailable")
        client=app.session
        owner_token=_structure_owner_ctx.set(True)
        try:
            timeout_s=20.0 if lane=="FAST" else 30.0
            sd=await asyncio.wait_for(app.load_structure(client,sym),timeout=timeout_s)
        finally:
            _structure_owner_ctx.reset(owner_token)
        if not isinstance(sd,dict):
            raise RuntimeError("structure payload incomplete")
        app.structure[sym]=sd
        q.structure_ms[sym]=q.ms()
        if not isinstance(app.anomaly_state.get(sym),dict):
            try:
                an=await asyncio.wait_for(app.load_fast_anomaly(client,sym),timeout=3.0)
                if isinstance(an,dict): app.anomaly_state[sym]=an
            except Exception:
                pass
        row=app.evaluate_symbol(sym)
        if isinstance(row,dict) and row: q.latest[sym]=row
        _recovery_retry_after.pop(sym,None)
        _structure_cache_dirty=True
        recovery_stats["ok"]+=1
        recovery_stats["fast_ok" if lane=="FAST" else "seed_ok"]+=1
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        _recovery_retry_after[sym]=time.time()+RECOVERY_FAIL_COOLDOWN_S
        recovery_stats["fail"]+=1
        recovery_stats["fast_fail" if lane=="FAST" else "seed_fail"]+=1
        if recovery_stats["fail"]<=40:
            print(f"Ψ-RECOVERY {lane}_ERROR {sym} {type(exc).__name__}: {exc}",flush=True)
        return False
    finally:
        if lane=="FAST":
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
        targets.sort(
            key=lambda s:(
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
                f"incOK={_structure_tf_stats['incremental_ok']} seed={_structure_tf_stats['full_seed']} "
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
                f"incOK={_structure_tf_stats['incremental_ok']} seed={_structure_tf_stats['full_seed']} "
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

    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)

    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_S)
        watchdog_stats["cycles"]+=1
        actions=[]
        try:
            now=time.time()
            scope=_recovery_scope()
            total=len(scope)
            ever_cov=sum(1 for s in scope if _structure_age_recovery(s)<999000)
            fresh_cov=sum(1 for s in scope if _structure_age_recovery(s)<=RECOVERY_STALE_S)
            pool=len(getattr(app,"selected_micro_symbols",[]) or [])
            pin=_watchdog_pinpoint_count()
            shards=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS))
            ext_age=(now-f(extrest.ext_last_refresh,0.0)) if f(extrest.ext_last_refresh,0.0)>0 else 999999.0
            startup_age=now-_watchdog_started

            if ever_cov>_watchdog_last_ever_cov:
                _watchdog_last_ever_cov=ever_cov
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

            # If structure coverage stops advancing, rotate the preferred kline route
            # and release only expired recovery cooldowns. The normal recovery loop
            # remains the sole hydrator, preventing duplicate request storms.
            if (
                startup_age>WATCHDOG_STARTUP_GRACE_S
                and fresh_cov<min(total,(16 if pool==0 else 32))
                and now-_watchdog_last_cov_progress>WATCHDOG_STRUCTURE_STALL_S
            ):
                rotated=0
                for key in list(_rest_good_host.keys()):
                    if str(key).startswith("structure_klines:"):
                        _rest_good_host.pop(key,None);rotated+=1
                released=0
                for sym,until in list(_recovery_retry_after.items()):
                    if until<=now:
                        _recovery_retry_after.pop(sym,None);released+=1
                _watchdog_last_cov_progress=now
                watchdog_stats["structure_kicks"]+=1
                watchdog_stats["actions"]+=1
                actions.append(f"STRUCTURE_ROUTE_ROTATE:{rotated}")
                print(
                    f"Ψ-WATCHDOG ACTION STRUCTURE_ROUTE_ROTATE coverage={ever_cov}/{total} "
                    f"routesRotated={rotated} expiredReleased={released} activeCooldowns={len(_recovery_retry_after)}",
                    flush=True,
                )

            # Continuity should populate as soon as enough verified structure exists.
            if (
                startup_age>WATCHDOG_STARTUP_GRACE_S
                and pool==0 and fresh_cov>=min(16,total)
                and now-_watchdog_last_pool_progress>WATCHDOG_POOL_STALL_S
            ):
                try:
                    await continuity_guard.rebalance_continuity_guarded(force=True)
                    new_pool=len(getattr(app,"selected_micro_symbols",[]) or [])
                    watchdog_stats["pool_kicks"]+=1
                    watchdog_stats["actions"]+=1
                    actions.append(f"POOL_REBALANCE:{new_pool}")
                    print(f"Ψ-WATCHDOG ACTION POOL_REBALANCE before=0 after={new_pool} structure={ever_cov}/{total}",flush=True)
                    if new_pool>0:
                        _watchdog_last_pool=new_pool
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
            min_fresh=min(total,16)
            structure_ready=(startup_age<=WATCHDOG_STARTUP_GRACE_S or fresh_cov>=min_fresh)
            rest_recent_ok=(_rest_last_fail<=0 or now-_rest_last_fail>60.0)
            continuity_ok=(startup_age<=WATCHDOG_STARTUP_GRACE_S or pool>0)
            shard_ok=(pool==0 or shards==tape.SHARDS or now-_watchdog_last_shard_progress<=WATCHDOG_SHARD_STALL_S)
            healthy=ext_live and structure_ready and rest_recent_ok and continuity_ok and shard_ok
            if healthy: watchdog_stats["healthy"]+=1
            else: watchdog_stats["degraded"]+=1
            status="HEALTHY" if healthy else "RECOVERING"
            print(
                f"Ψ-WATCHDOG status={status} cycle={watchdog_stats['cycles']} "
                f"structureFresh={fresh_cov}/{total} structureEver={ever_cov}/{total} "
                f"pinpoint={pin} pool={pool}/{getattr(base,'POOL_SIZE',80)} "
                f"monsterShards={shards}/{tape.SHARDS} extAge={ext_age:.1f}s "
                f"restOK={_rest_stats['ok']} restFail={_rest_stats['fail']} restRetry={_rest_stats['attempt_fail']} riskOK={_rest_stats['risk_ok']} riskFail={_rest_stats['risk_fail']} riskDefer={_rest_stats['risk_defer']} bgOK={_rest_stats['bg_ok']} bgFail={_rest_stats['bg_fail']} bgDefer={_rest_stats['bg_defer']} "
                f"actions={actions or ['NONE']} totals={watchdog_stats}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            watchdog_stats["errors"]+=1
            print(f"Ψ-WATCHDOG ERROR LOOP {type(exc).__name__}: {exc}",flush=True)

async def main():
    print("[v11.0.5.1] Ψ BREAKOUT STRUCTURAL INTELLIGENCE active — BSI fuses micro HH/HL structure, MTF alignment, resistance fatigue/attack count, compression, liquidity vacuum/ask depletion, resistance proximity, breakout/retest context, live confirmation, fresh-structure and MA-structure gate state, anti-chase room and false-break risk. BSI changes research ranking/visibility only; Pinpoint remains sole BUY NOW authority and every hard execution gate remains fail-closed. Monster board now emits 30 ranked rows. Production watchdog monitors extension freshness, structure progress, continuity initialization, Pinpoint visibility and Monster shard health with bounded fail-closed self-healing.",flush=True)
    await asyncio.gather(rescue.main(), structure_recovery_loop(), cold_seed_loop(), structure_cache_loop(), watchdog_loop())

if __name__=="__main__":asyncio.run(main())
