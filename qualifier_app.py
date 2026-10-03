import asyncio, json, os, time
from collections import Counter, defaultdict, deque
import aiohttp
import app

VERSION="10.4-rolling-hunter"
QUALIFIER_STATES=("BUY NOW","PRE-IGNITION")
QUALIFIER_TARGET=max(1,min(int(os.getenv("QUALIFIER_TARGET","10")),10))
QUALIFIER_POLICY="BUY_PRE_ONLY_NO_PADDING_NO_THRESHOLD_RELAXATION"
STRUCTURE_BATCH=max(12,min(int(os.getenv("HUNTER_STRUCTURE_BATCH","40")),60))
STRUCTURE_SECONDS=max(60,int(os.getenv("HUNTER_STRUCTURE_SECONDS","90")))
UNIVERSE_SECONDS=max(600,int(os.getenv("HUNTER_UNIVERSE_SECONDS","900")))
STRUCTURE_MAX_AGE=max(900,int(os.getenv("HUNTER_STRUCTURE_MAX_AGE","1800")))
ANOMALY_SECONDS=max(45,int(os.getenv("HUNTER_ANOMALY_SECONDS","60")))
MICRO_SLOTS=max(12,min(int(os.getenv("HUNTER_MICRO_SLOTS","20")),30))
LOCK_SLOTS=max(4,min(int(os.getenv("HUNTER_LOCK_SLOTS","10")),QUALIFIER_TARGET))
MICRO_HOLD=max(180,int(os.getenv("HUNTER_MICRO_HOLD","300")))
POOL_SECONDS=max(30,int(os.getenv("HUNTER_POOL_SECONDS","60")))
LOCK_GRACE=max(90,int(os.getenv("HUNTER_LOCK_GRACE","240")))
PERSIST=max(2,min(int(os.getenv("HUNTER_PERSIST_SAMPLES","2")),4))
TICK_SECONDS=max(15,int(os.getenv("HUNTER_TICK_SECONDS","30")))
SAMPLE_SECONDS=max(1.0,float(os.getenv("HUNTER_DISCOVERY_SAMPLE_SECONDS","2")))
DISCOVERY_HISTORY=max(300,int(os.getenv("HUNTER_DISCOVERY_HISTORY_SECONDS","420")))
DISCOVERY_SAMPLES=int(DISCOVERY_HISTORY/SAMPLE_SECONDS)+8
HOT_COUNT=max(20,int(os.getenv("HUNTER_HOT_COUNT","80")))
REST_INTERVAL=max(0.08,float(os.getenv("HUNTER_REST_MIN_INTERVAL","0.12")))
REST_BACKOFF=max(300,int(os.getenv("HUNTER_REST_BACKOFF_SECONDS","900")))

app.TOP_STRUCTURE_UNIVERSE=STRUCTURE_BATCH
app.MICRO_UNIVERSE_SIZE=MICRO_SLOTS
app.ANOMALY_PROMOTION_SLOTS=MICRO_SLOTS
app.ANOMALY_REFRESH_SECONDS=ANOMALY_SECONDS
app.STRUCTURE_REFRESH_SECONDS=STRUCTURE_SECONDS
app.MICRO_POOL_MIN_HOLD_SECONDS=MICRO_HOLD
app.USER_AGENT="psi-v10-live-scanner/10.4-rolling-hunter"

universe=[]; universe_set=set(); universe_ts=0.0; cursor=0; coverage_round=0
structure_seen=set(); structure_ms={}; structure_cycles=0; anomaly_cycles=0; pool_cycles=0; qualifier_cycles=0
disc=defaultdict(lambda:deque(maxlen=DISCOVERY_SAMPLES)); disc_sample_ts={}; disc_ws=False; disc_event_ms=0; disc_source="NONE"; disc_ws_failures=0; disc_rest_ok=0; disc_rest_fail=0
entered={}; locked_until={}; streak=defaultdict(int); last_raw={}; stable={}; latest={}; near=[]
orig_api_get=app.api_get; rest_lock=asyncio.Lock(); next_rest=0.0; backoff_until=0.0; count418=0; count429=0; rest_requests=0

def ts(): return time.time()
def ms(): return int(time.time()*1000)
def clamp(v,a,b): return max(a,min(b,v))

async def governed_api_get(client,path,params=None):
    global next_rest,backoff_until,count418,count429,rest_requests
    if ts()<backoff_until: raise RuntimeError(f"REST cooldown {int(backoff_until-ts())}s")
    async with rest_lock:
        wait=max(0,next_rest-ts())
        if wait: await asyncio.sleep(wait)
        next_rest=max(ts(),next_rest)+REST_INTERVAL
    try:
        out=await orig_api_get(client,path,params); rest_requests+=1; return out
    except Exception as e:
        text=str(e)
        if "REST 418" in text: count418+=1; backoff_until=max(backoff_until,ts()+REST_BACKOFF)
        if "REST 429" in text: count429+=1; backoff_until=max(backoff_until,ts()+REST_BACKOFF)
        raise
app.api_get=governed_api_get

def before(samples,target):
    for x in reversed(samples):
        if x[0]<=target: return x
    return samples[0] if samples else None

def dmetric(symbol):
    q=disc.get(symbol)
    if not q or len(q)<4: return {"score":0.0,"samples":len(q or ())}
    t,p,v,n,bid,ask=q[-1]
    if p<=0: return {"score":0.0,"samples":len(q)}
    s1,s3,s5=before(q,t-60),before(q,t-180),before(q,t-300)
    ret=lambda x:(p/x[1]-1)*100 if x and x[1]>0 else 0.0
    r1,r3=ret(s1),ret(s3)
    v1=max(0,v-(s1[2] if s1 else v)); v5=max(0,v-(s5[2] if s5 else v)); va=v1/(v5/5) if v5>0 else 0
    n1=max(0,n-(s1[3] if s1 else n)); n5=max(0,n-(s5[3] if s5 else n)); ta=n1/(n5/5) if n5>0 else 0
    spr=(ask-bid)/((ask+bid)/2)*10000 if bid>0 and ask>bid else 999
    prices=[x[1] for x in q if x[0]>=t-180 and x[1]>0]; comp=(max(prices)-min(prices))/p*100 if prices else 999
    score=clamp(r1,-1,2.5)*8+clamp(r3,-1.5,5)*3.5+clamp(va-1,0,5)*9+clamp(ta-1,0,5)*6
    score+=(10 if comp<=1.5 else 5 if comp<=2.5 else 0)+(4 if spr<=8 else -8 if spr>=25 else 0)
    if r1>=4 or r3>=8: score-=30
    return {"score":round(score,3),"r1":round(r1,4),"r3":round(r3,4),"vol_accel":round(va,3),"trade_accel":round(ta,3),"spread_bps":round(spr,3),"compression_3m_pct":round(comp,4),"samples":len(q)}

def hot(limit=HOT_COUNT):
    rows=[(dmetric(s).get("score",0),s) for s in universe]; rows.sort(reverse=True); return rows[:limit]

def sfresh(s): return ms()-structure_ms.get(s,0)<=STRUCTURE_MAX_AGE*1000 and structure_ms.get(s,0)>0

def sscore(s):
    r=app.structure.get(s)
    if not r: return -1e9
    try: prox=min(abs(r["distance_ema50_atr"]),abs(r["distance_ema200_atr"]))
    except Exception: prox=99
    score=len(r.get("structure_confirmations",[]))*8+max(0,20-prox*5)+min(float(r.get("volume_acceleration_15m",0) or 0)*8,16)
    score+=(10 if r.get("compression") else 0)+(12 if r.get("breakout_near") or r.get("breakout") else 0)+(15 if r.get("ma_regime") else 0)
    if r.get("anti_chase"): score-=35
    score+=dmetric(s).get("score",0)*.35+min(max(float(app.anomaly_state.get(s,{}).get("anomaly_score",0) or 0),0),30)*.25
    return score

async def refresh_universe(force=False):
    global universe,universe_set,universe_ts,cursor
    if app.session is None or (not force and ts()-universe_ts<UNIVERSE_SECONDS): return
    rows=await app.get_exchange_symbols(app.session); universe=[s for s,_ in rows]; universe_set=set(universe); universe_ts=ts()
    if cursor>=len(universe): cursor=0
    print(f"Ψ-V10.4 UNIVERSE {len(universe)} eligible Spot USDT",flush=True)

def structure_batch_symbols():
    global cursor,coverage_round
    if not universe: return []
    out=[]; seen=set()
    def add(s):
        if s in universe_set and s not in seen and len(out)<STRUCTURE_BATCH: out.append(s); seen.add(s)
    for s in sorted(locked_until,key=lambda x:locked_until.get(x,0),reverse=True):
        if locked_until.get(s,0)>=ts(): add(s)
    hot_added=0
    for _,s in hot():
        if ms()-structure_ms.get(s,0)<300000: continue
        n=len(out); add(s); hot_added+=len(out)>n
        if hot_added>=max(6,STRUCTURE_BATCH//3): break
    attempts=0
    while len(out)<STRUCTURE_BATCH and attempts<len(universe)*2:
        add(universe[cursor]); cursor+=1; attempts+=1
        if cursor>=len(universe): cursor=0; coverage_round+=1
    return out

async def refresh_structure():
    global structure_cycles
    if app.session is None or ts()<backoff_until: return
    await refresh_universe(); batch=structure_batch_symbols()
    if not batch: return
    await app.structure_batch(batch); stamp=ms(); done=0
    for s in batch:
        if s in app.structure: structure_ms[s]=stamp; structure_seen.add(s); done+=1
    app.last_structure_refresh=ts(); app.scanner_ready=bool(app.structure); structure_cycles+=1
    await rebalance_pool(force=not app.selected_micro_symbols)
    print(f"Ψ-V10.4 STRUCTURE {done}/{len(batch)} | ever={len(structure_seen)}/{len(universe)} round={coverage_round}",flush=True)

async def refresh_anomaly():
    global anomaly_cycles
    if app.session is None or ts()<backoff_until: return
    symbols=[]
    for s in list(app.selected_micro_symbols)+[s for _,s in hot()]:
        if s in universe_set and s not in symbols: symbols.append(s)
        if len(symbols)>=MICRO_SLOTS: break
    sem=asyncio.Semaphore(6)
    async def one(s):
        async with sem: return await app.load_fast_anomaly(app.session,s)
    rows=await asyncio.gather(*(one(s) for s in symbols),return_exceptions=True)
    for r in rows:
        if isinstance(r,dict): app.anomaly_state[r["symbol"]]=r
    app.last_anomaly_refresh=ts(); anomaly_cycles+=1

def locks():
    rows=[s for s,x in locked_until.items() if x>=ts() and s in universe_set]
    rows.sort(key=lambda s:(2 if last_raw.get(s)=="BUY NOW" else 1,streak.get(s,0),latest.get(s,{}).get("score",0)),reverse=True)
    return rows[:LOCK_SLOTS]

async def rebalance_pool(force=False):
    global pool_cycles
    current=list(app.selected_micro_symbols); cset=set(current); now=ts(); out=[]; seen=set(); locked=locks()
    def add(s):
        if s in universe_set and s not in seen and len(out)<MICRO_SLOTS: out.append(s); seen.add(s)
    for s in locked: add(s)
    if not force:
        for s in current:
            if s not in locked and now-entered.get(s,now)<MICRO_HOLD: add(s)
    ranked=sorted(((sscore(s),s) for s in app.structure if s in universe_set and sfresh(s)),reverse=True)
    for _,s in ranked: add(s)
    for _,s in hot(): add(s)
    if not out or set(out)==cset: return
    for s in out:
        if s not in cset: entered[s]=now
        app.ensure_micro_state(s)
    for s in list(entered):
        if s not in out: entered.pop(s,None)
    app.selected_micro_symbols=out; app.last_micro_pool_change=now; pool_cycles+=1
    print(f"Ψ-V10.4 MICRO locked={len(locked)} hunter={len(out)-len(locked)} total={len(out)}",flush=True)

def tick():
    global stable,near,qualifier_cycles
    rows=[]
    for s in list(app.selected_micro_symbols):
        try: r=app.evaluate_symbol(s)
        except Exception: continue
        if r: rows.append(r)
    now=ts(); good={}; misses=[]
    for r in rows:
        s=r["symbol"]; latest[s]=r; raw=r.get("state","REJECT"); prev=last_raw.get(s)
        if raw in QUALIFIER_STATES and r.get("micro_ready"):
            streak[s]=streak[s]+1 if prev in QUALIFIER_STATES else 1; locked_until[s]=max(locked_until.get(s,0),now+LOCK_GRACE)
            if streak[s]>=PERSIST:
                x=dict(r); x["persistence_samples"]=streak[s]; x["hunter_locked"]=True; good[s]=x
        else: streak[s]=0; misses.append(r)
        last_raw[s]=raw
    for s in list(locked_until):
        if locked_until[s]<now: locked_until.pop(s,None)
    misses.sort(key=lambda r:(app.STATE_PRIORITY.get(r.get("state","REJECT"),0),float(r.get("score",0) or 0)),reverse=True)
    stable=good; near=misses[:10]; qualifier_cycles+=1

def results(limit=QUALIFIER_TARGET):
    rows=list(stable.values()); rows.sort(key=lambda r:(2 if r.get("state")=="BUY NOW" else 1,r.get("persistence_samples",0),float(r.get("score",0) or 0)),reverse=True)
    return rows[:max(1,min(int(limit or QUALIFIER_TARGET),QUALIFIER_TARGET))]

def coverage():
    total=len(universe); cutoff=ms()-STRUCTURE_MAX_AGE*1000; recent=sum(structure_ms.get(s,0)>=cutoff for s in universe); fresh=0
    for s in app.selected_micro_symbols:
        x=app.micro_state.get(s,{}); fresh+=ms()-int(x.get("last_trade_ms",0) or 0)<=15000 and ms()-int(x.get("last_book_ms",0) or 0)<=5000
    return {"full_universe":total,"discovery_ready":sum(len(disc.get(s,()))>=4 for s in universe),"discovery_source":disc_source,"discovery_ws_failures":disc_ws_failures,"discovery_rest_ok":disc_rest_ok,"discovery_rest_fail":disc_rest_fail,"structure_seen_ever":len(structure_seen),"structure_recent":recent,"structure_recent_pct":round(recent/total*100,2) if total else 0,"coverage_round":coverage_round,"structure_cursor":cursor,"micro_total":len(app.selected_micro_symbols),"micro_verified_fresh":fresh,"locked_slots":len(locks()),"hunter_slots":max(0,len(app.selected_micro_symbols)-len(locks())),"stable_qualifiers":len(stable),"cycles":{"structure":structure_cycles,"anomaly":anomaly_cycles,"pool":pool_cycles,"qualifier":qualifier_cycles}}

def near_diag(): return [{"symbol":r.get("symbol"),"state":r.get("state"),"score":r.get("score"),"active_setup":r.get("active_setup"),"failed_hard":r.get("failed_hard",[]),"failed_setup":r.get("failed_setup",[]),"micro_ready":r.get("micro_ready")} for r in near]

async def health(req):
    return app.web.json_response({"ok":True,"service":"psi-v10-live-scanner","version":VERSION,"policy":QUALIFIER_POLICY,"qualifier_target":QUALIFIER_TARGET,"persistence_samples":PERSIST,"scanner_ready":app.scanner_ready,"websocket_connected":app.websocket_connected,"discovery_ws_connected":disc_ws,"strict_uk_allowlist_enabled":bool(app.UK_SYMBOLS),"coverage":coverage(),"rest_governor":{"requests":rest_requests,"backoff_active":ts()<backoff_until,"backoff_remaining_seconds":max(0,int(backoff_until-ts())),"http_418_count":count418,"http_429_count":count429},"stable_qualifier_symbols":list(stable),"last_error":app.last_error})

async def scan(req):
    try: limit=max(1,min(int(req.query.get("limit",QUALIFIER_TARGET)),QUALIFIER_TARGET))
    except ValueError: limit=QUALIFIER_TARGET
    app.resolve_outcomes(); rows=results(limit)
    return app.web.json_response({"ok":True,"scanner":"Ψ-V10.4 Rolling Qualifier Hunter","version":VERSION,"policy":QUALIFIER_POLICY,"buy_policy":"ALL_HARD_SAFETY_GATES_PLUS_ALL_GATES_OF_ONE_VERIFIED_SETUP","qualifier_target":QUALIFIER_TARGET,"returned":len(rows),"state_counts":dict(Counter(r["state"] for r in rows)),"coverage":coverage(),"results":rows,"near_miss_diagnostics":near_diag(),"generated_ms":ms()})

def _ingest_discovery_payload(payload, source):
    global disc_event_ms,disc_source
    if not isinstance(payload,list):
        return 0
    now=ts(); accepted=0
    for x in payload:
        if not isinstance(x,dict):
            continue
        s=x.get("s","")
        if s not in universe_set or now-disc_sample_ts.get(s,0)<SAMPLE_SECONDS:
            continue
        bid=app.safe_float(x.get("b") if x.get("b") is not None else x.get("bidPrice"))
        ask=app.safe_float(x.get("a") if x.get("a") is not None else x.get("askPrice"))
        p=app.safe_float(x.get("c") if x.get("c") is not None else x.get("lastPrice"))
        if p<=0 and bid>0 and ask>0:
            p=(bid+ask)/2.0
        if p<=0:
            continue
        qv=app.safe_float(x.get("q") if x.get("q") is not None else x.get("quoteVolume"))
        cnt=app.safe_float(x.get("n") if x.get("n") is not None else x.get("count"))
        disc[s].append((now,p,qv,cnt,bid,ask)); disc_sample_ts[s]=now; accepted+=1
    if accepted:
        disc_event_ms=int(now*1000); disc_source=source
    return accepted

async def _discovery_rest_snapshot():
    global disc_rest_ok,disc_rest_fail,disc_source
    if app.session is None:
        return 0
    hosts=[
        "https://api.binance.com",
        "https://data-api.binance.vision",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]
    last_exc=None
    for host in hosts:
        try:
            async with app.session.get(
                f"{host}/api/v3/ticker/bookTicker",
                timeout=aiohttp.ClientTimeout(total=6,connect=2),
            ) as resp:
                body=await resp.text()
                if resp.status!=200:
                    raise RuntimeError(f"{host} HTTP {resp.status}: {body[:120]}")
                payload=json.loads(body)
                accepted=_ingest_discovery_payload(payload,"REST_BOOK_TICKER")
                disc_rest_ok+=1
                if accepted:
                    print(f"Ψ-DISCOVERY FALLBACK source=REST_BOOK_TICKER accepted={accepted}/{len(universe)} host={host}",flush=True)
                return accepted
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_exc=e
            continue
    disc_rest_fail+=1
    print(f"Ψ-DISCOVERY FALLBACK_ERROR {type(last_exc).__name__ if last_exc else 'RuntimeError'}: {last_exc}",flush=True)
    return 0

async def discovery_rest_loop():
    while True:
        try:
            await asyncio.sleep(5)
            stale=(not disc_ws) or (ms()-disc_event_ms>8000)
            if stale:
                await _discovery_rest_snapshot()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"DISCOVERY_REST_LOOP: {type(e).__name__}: {e}",flush=True)

async def discovery_loop():
    global disc_ws,disc_event_ms,disc_source,disc_ws_failures
    host_cursor=0
    while True:
        bases=[]
        for raw in (
            str(getattr(app,"WS_BASE","") or "").rstrip("/"),
            "wss://data-stream.binance.vision",
            "wss://stream.binance.com:443",
            "wss://stream.binance.com:9443",
        ):
            if raw and raw not in bases:
                bases.append(raw)
        base_url=bases[host_cursor%len(bases)] if bases else "wss://data-stream.binance.vision"
        url=f"{base_url}/ws/!ticker@arr"
        try:
            async with app.session.ws_connect(url,heartbeat=25,receive_timeout=60,max_msg_size=0,timeout=12) as ws:
                disc_ws=True; disc_source=f"WS:{base_url}"
                print(f"Ψ-V10.4 discovery WS connected host={base_url} (!ticker@arr)",flush=True)
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        try:
                            payload=json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue
                        _ingest_discovery_payload(payload,f"WS:{base_url}")
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f"discovery_websocket_{msg.type.name.lower()}")
                raise RuntimeError("discovery_websocket_stream_ended")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            disc_ws_failures+=1
            app.last_error=f"DISCOVERY_WS: {type(e).__name__}: {e}"
            print(f"{app.last_error} host={base_url}",flush=True)
            host_cursor=(host_cursor+1)%max(1,len(bases))
        finally:
            disc_ws=False
        await asyncio.sleep(1)

async def loop(delay,fn,label):
    while True:
        try:
            await asyncio.sleep(delay); out=fn()
            if asyncio.iscoroutine(out): await out
        except asyncio.CancelledError: raise
        except Exception as e: app.last_error=f"{label}: {type(e).__name__}: {e}"; print(app.last_error,flush=True)

async def print_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS); rows=results(); c=coverage()
        print("\n==================================================",flush=True); print(f"Ψ-V10.4 QUALIFIER HUNT — {len(rows)}/{QUALIFIER_TARGET}",flush=True); print(f"coverage={c['structure_recent']}/{c['full_universe']} ({c['structure_recent_pct']:.1f}%) micro={c['micro_verified_fresh']}/{c['micro_total']} locked={c['locked_slots']}",flush=True); print("==================================================",flush=True)
        for i,r in enumerate(rows,1): print(f"{i:02d}. {r['symbol']:12s} {r['state']:14s} score={r['score']:6.2f} persist={r.get('persistence_samples',0)} setup={r['active_setup'][:10]:10s} OFI={r['ofi']:+.3f} OBI={r['obi']:+.3f} buy={r['aggressive_buy_ratio']:.2%} ready={r['micro_ready']}",flush=True)
        if not rows: print("No persistent PRE-IGNITION / BUY NOW setup currently qualifies.",flush=True)

app.ranked_results=results; app.health=health; app.scan_endpoint=scan; app.QUALIFIER_TARGET=QUALIFIER_TARGET; app.QUALIFIER_STATES=QUALIFIER_STATES; app.QUALIFIER_POLICY=QUALIFIER_POLICY

async def main():
    app.session=aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),connector=aiohttp.TCPConnector(limit=80,ttl_dns_cache=300),headers={"User-Agent":app.USER_AGENT})
    runner=await app.start_http_server(); tasks=[]
    try:
        print("Ψ-V10.4 ROLLING QUALIFIER HUNTER ACTIVE — full-universe discovery, rotating structure, locked qualifiers, TARGET=10, NO PADDING",flush=True)
        await refresh_universe(True); tasks.append(asyncio.create_task(discovery_loop())); tasks.append(asyncio.create_task(discovery_rest_loop())); await refresh_structure(); await refresh_anomaly(); tick()
        tasks += [asyncio.create_task(loop(UNIVERSE_SECONDS,lambda:refresh_universe(True),"UNIVERSE")),asyncio.create_task(loop(STRUCTURE_SECONDS,refresh_structure,"STRUCTURE")),asyncio.create_task(loop(ANOMALY_SECONDS,refresh_anomaly,"ANOMALY")),asyncio.create_task(loop(POOL_SECONDS,rebalance_pool,"POOL")),asyncio.create_task(app.websocket_loop()),asyncio.create_task(loop(TICK_SECONDS,tick,"QUALIFIER")),asyncio.create_task(print_loop())]
        await asyncio.gather(*tasks)
    finally:
        for t in tasks: t.cancel()
        if tasks: await asyncio.gather(*tasks,return_exceptions=True)
        await runner.cleanup(); await app.session.close()

if __name__=="__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: print("Ψ-V10.4 stopped",flush=True)
