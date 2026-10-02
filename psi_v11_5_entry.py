import asyncio, json, math, statistics, time
import aiohttp
import psi_v11_4_entry as rescue
import psi_v11_2_2_entry as extrest
import psi_v11_3_1_entry as continuity_guard

base=rescue.base
tape=rescue.tape
app,q,scanner=base.app,base.q,base.scanner
VERSION="11.0.5.0-breakout-structural-intelligence"

REST_BASES = (
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api-gcp.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
)
_rest_preferred = 0
_rest_failover_printed = None
_rest_gate = None

def _rest_semaphore():
    global _rest_gate
    if _rest_gate is None:
        _rest_gate = asyncio.Semaphore(8)
    return _rest_gate

async def resilient_api_get(client, path, params=None):
    global _rest_preferred, _rest_failover_printed
    order=[_rest_preferred]+[i for i in range(len(REST_BASES)) if i!=_rest_preferred]
    errs=[]
    for idx in order:
        host=REST_BASES[idx]
        try:
            async with client.get(
                f"{host}{path}",
                params=params,
                timeout=aiohttp.ClientTimeout(total=3.5),
                headers={"Connection":"close"},
            ) as response:
                body=await response.text()
                if response.status!=200:
                    raise RuntimeError(f"HTTP {response.status}: {body[:160]}")
                payload=json.loads(body)
                app.rest_connected=True
                app.last_error=None
                if idx!=_rest_preferred or _rest_failover_printed is None:
                    _rest_preferred=idx
                    if _rest_failover_printed!=host:
                        print(f"Ψ-REST FAILOVER active={host}",flush=True)
                        _rest_failover_printed=host
                return payload
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errs.append(f"{host}:{type(exc).__name__}:{exc}")
            continue
    app.rest_connected=False
    app.last_error="REST_FAILOVER_ALL: "+" | ".join(errs[-3:])
    raise RuntimeError(app.last_error)

# Replace the shared module-level REST function before any scanner loop starts.
app.api_get = resilient_api_get

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
        ca=base.candidate(s,row,c,base.deep(s));base.latest[s]=ca
        visible=(f(ca.get("early"))>=38 or f(ca.get("dna"))>=50 or f(ca.get("peak"))>=120 or ca.get("state") in {"MONSTER-RESCUE","MONSTER-MEMORY"} or f(ca.get("retentionScore"))>=58 or f(ca.get("bsi"))>=62)
        if visible:out.append(ca);base.open_obs(ca)
    priority={"MONSTER-HOT":6,"MONSTER-IGNITION":5,"MONSTER-MEMORY":4,"MONSTER-RESCUE":3,"MONSTER-SEED":2,"MONSTER-EXTENDED":1,"MONSTER-WATCH":0}
    out.sort(key=lambda x:(priority.get(str(x.get("state")),0),f(x.get("bsi")),f(x.get("retentionScore")),f(x.get("early")),f(x.get("dna")),f(x.get("peak"))),reverse=True)
    base.stats["cycles"]+=1;base.stats["universe"]=len(u);base.stats["deep"]=len(pool);base.stats["cand"]=len(out);rescue.rescue_stats["last_pool"]=len(pool);rescue.rescue_stats["last_emergency"]=len(emergency)
    return out[:BOARD_ROWS]
base.scan=scan_v5

def _bsi_learning():
    src=[x for x in base.resolved if isinstance(x,dict) and isinstance(x.get("features"),dict) and "bsi_n" in x["features"]]
    if not src:return {"status":"WARMING","n":0}
    hi=[x for x in src if f(x["features"].get("bsi_n"))>=.70]
    return {"status":"ACTIVE" if len(src)>=30 else "WARMING","n":len(src),"high":len(hi),"p10":(sum(f(x.get("max_return_pct"))>=10 for x in hi)/len(hi)) if hi else None,"p20":(sum(f(x.get("max_return_pct"))>=20 for x in hi)/len(hi)) if hi else None,"mfe":statistics.mean(f(x.get("max_return_pct")) for x in hi) if hi else None}

async def board_loop_v5():
    while True:
        await asyncio.sleep(base.BOARD_S)
        try:
            base.refresh_adapt();rows=list(base.latest.get("_board") or []);states=("MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED","MONSTER-EXTENDED");counts={k:sum(r.get("state")==k for r in rows) for k in states};ups=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS));ready=sum(1 for s in list(getattr(q,"universe",[]) or []) if tape.tape_metric(s).get("ready"))
            print(f"Ψ-MONSTER-RADAR BOARD scanned={base.stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={base.stats['deep']} candidates={base.stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} memory={counts['MONSTER-MEMORY']} rescue={counts['MONSTER-RESCUE']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} rows={len(rows)}/{BOARD_ROWS} scan={int(base.SCAN_S*1000)}ms tape={ready}/{len(getattr(q,'universe',[]) or [])} shards={ups}/{tape.SHARDS} trades={tape.tape_stats['trades']} books={tape.tape_stats['books']} learning={base.adapt['status']} obsPending={len(base.pending)} obsResolved={len(base.resolved)} PinpointAuthority=YES BSI=ON",flush=True)
            for i,r in enumerate(rows,1):
                ds="-" if r.get("dist") is None else f"{f(r.get('dist')):+.2f}%";age=f(r.get("peak20Age"),999999);mem="-" if age>rescue.MEMORY_WINDOW_S else f"{100*f(r.get('peakShP20_120')):.1f}%/{age:.0f}s"
                print(f"MR{i:02d}. {r['symbol']:<14} state={str(r.get('state')):<17} BSI={f(r.get('bsi')):5.1f} {str(r.get('bsiState')):<19} FB={f(r.get('falseBreakRisk')):4.0f} comp={f(r.get('bsiCompression')):4.0f} tests={f(r.get('bsiTests')):4.0f} retest={f(r.get('bsiRetest')):4.0f} trend={f(r.get('bsiTrend')):4.0f} EARLY={f(r.get('early')):5.1f} DNA={f(r.get('dna')):5.1f} retain={f(r.get('retentionScore')):5.1f} rapid={f(r.get('rapid')):6.1f}/{f(r.get('peak')):6.1f} tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} event={f(r.get('event')):4.0f} vac={f(r.get('vac')):4.0f} pB15={100*f(r.get('pb15')):4.1f}% shP20={100*f(r.get('sp20')):4.1f}% mem20={mem} layers={int(f(r.get('layers')))}/6 dist={ds} formal={r.get('formal')} pp={r.get('pp')} why={(r.get('reasons') or [])[:10]}",flush=True)
            top=sorted(rows,key=lambda r:f(r.get("bsi")),reverse=True)[:8];print(f"Ψ-BSI BOARD top={[(r.get('symbol'),round(f(r.get('bsi')),1),r.get('bsiState'),round(f(r.get('falseBreakRisk')),1),round(f(r.get('bsiCompression')),1),round(f(r.get('bsiTests')),1),r.get('pp')) for r in top]}",flush=True);print(f"Ψ-BSI LEARNING {_bsi_learning()}",flush=True)
            br,bn=rescue._blocker_learning();print("Ψ-MONSTER-BLOCKER-LEARN "+(f"resolved={bn} top={[(b,n,round(w10*100,1),round(w20*100,1),round(mfe,2)) for w20,w10,mfe,n,b in br]}" if bn else "status=WARMING resolved=0"),flush=True);paths=rescue._path_learning();print(f"Ψ-MONSTER-PATH-LEARN {paths}" if paths else "Ψ-MONSTER-PATH-LEARN status=WARMING",flush=True);print(f"Ψ-MONSTER-RESCUE HEALTH emergencyPromotions={rescue.rescue_stats['emergency_promotions']} lastEmergency={rescue.rescue_stats['last_emergency']} deepPool={rescue.rescue_stats['last_pool']} ignitionEpisodes={rescue.rescue_stats['ignition_episodes']} memoryWindow={int(rescue.MEMORY_WINDOW_S)}s buyAuthority=PINPOINT_ONLY",flush=True);rescue._save_rescue()
        except asyncio.CancelledError:rescue._save_rescue(True);raise
        except Exception as e:base.stats["board_errors"]+=1;print(f"Ψ-BSI BOARD_ERROR {type(e).__name__}: {e}",flush=True)
base.board_loop=board_loop_v5

for mod in (rescue,tape,base,getattr(base,"scientist",None),scanner):
    try:mod.VERSION=VERSION
    except Exception:pass


RECOVERY_BATCH = 3
RECOVERY_PRIORITY = 96
RECOVERY_STALE_S = 240.0
recovery_stats = {"passes":0,"ok":0,"fail":0,"pool_kicks":0,"ext_ok":0,"ext_err":0}

def _structure_age_recovery(sym):
    ts = int(q.structure_ms.get(sym,0) or 0)
    return 999999.0 if ts <= 0 else max(0.0,(q.ms()-ts)/1000.0)

def _recovery_symbols():
    out=[];seen=set()
    def add(s):
        s=str(s or "")
        if s and s not in seen and s in set(getattr(q,"universe_set",set()) or set()):
            seen.add(s);out.append(s)
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
    for s in list(getattr(app,"selected_micro_symbols",[]) or []): add(s)
    for s in list(getattr(q,"universe",[]) or []): add(s)
    return out

async def _hydrate_one(sym):
    last_exc=None
    for attempt in range(2):
        try:
            if attempt==0 and app.session is not None and not app.session.closed:
                client=app.session
                sd=await asyncio.wait_for(app.load_structure(client,sym),timeout=7.0)
                if not isinstance(sd,dict):
                    raise RuntimeError("structure payload incomplete")
                app.structure[sym]=sd
                q.structure_ms[sym]=q.ms()
                if not isinstance(app.anomaly_state.get(sym),dict):
                    try:
                        an=await asyncio.wait_for(app.load_fast_anomaly(client,sym),timeout=8.0)
                        if isinstance(an,dict): app.anomaly_state[sym]=an
                    except Exception:
                        pass
            else:
                timeout=aiohttp.ClientTimeout(total=16)
                connector=aiohttp.TCPConnector(limit=4,ttl_dns_cache=60,force_close=True)
                async with aiohttp.ClientSession(timeout=timeout,connector=connector,headers={"User-Agent":getattr(app,"USER_AGENT","psi-v11-recovery")}) as client:
                    sd=await asyncio.wait_for(app.load_structure(client,sym),timeout=14.0)
                    if not isinstance(sd,dict):
                        raise RuntimeError("structure payload incomplete")
                    app.structure[sym]=sd
                    q.structure_ms[sym]=q.ms()
                    if not isinstance(app.anomaly_state.get(sym),dict):
                        try:
                            an=await asyncio.wait_for(app.load_fast_anomaly(client,sym),timeout=10.0)
                            if isinstance(an,dict): app.anomaly_state[sym]=an
                        except Exception:
                            pass
            row=app.evaluate_symbol(sym)
            if isinstance(row,dict) and row: q.latest[sym]=row
            recovery_stats["ok"]+=1
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_exc=exc
            if attempt==0:
                await asyncio.sleep(.25)
                continue
    recovery_stats["fail"]+=1
    if recovery_stats["fail"]<=20:
        print(f"Ψ-RECOVERY STRUCTURE_ERROR {sym} {type(last_exc).__name__}: {last_exc}",flush=True)
    return False

async def structure_recovery_loop():
    while app.session is None or not getattr(q,"universe",None):
        await asyncio.sleep(.5)
    while True:
        syms=_recovery_symbols()
        total=len(syms)
        coverage=sum(1 for s in syms if _structure_age_recovery(s)<999000)
        cold=coverage < max(1,int(total*.92))
        if cold:
            targets=[s for s in syms if _structure_age_recovery(s)>=999000]
        else:
            targets=[s for s in syms[:RECOVERY_PRIORITY] if _structure_age_recovery(s)>RECOVERY_STALE_S]
        if targets:
            for i in range(0,len(targets),RECOVERY_BATCH):
                batch=targets[i:i+RECOVERY_BATCH]
                await asyncio.gather(*[_hydrate_one(s) for s in batch])
                cov_now=sum(1 for s in syms if _structure_age_recovery(s)<999000)
                print(f"Ψ-RECOVERY BATCH coverage={cov_now}/{total} batch={i//RECOVERY_BATCH+1} ok={recovery_stats['ok']} fail={recovery_stats['fail']}",flush=True)
                if len(app.selected_micro_symbols or [])==0 and recovery_stats["ok"]>=16:
                    try:
                        await continuity_guard.rebalance_continuity_guarded(force=True)
                        recovery_stats["pool_kicks"]+=1
                    except Exception as exc:
                        print(f"Ψ-RECOVERY POOL_ERROR {type(exc).__name__}: {exc}",flush=True)
                await asyncio.sleep(.15)
        recovery_stats["passes"]+=1
        coverage=sum(1 for s in syms if _structure_age_recovery(s)<999000)
        print(f"Ψ-RECOVERY STRUCTURE coverage={coverage}/{total} pass={recovery_stats['passes']} ok={recovery_stats['ok']} fail={recovery_stats['fail']} pool={len(app.selected_micro_symbols or [])} kicks={recovery_stats['pool_kicks']}",flush=True)
        try:
            if len(app.selected_micro_symbols or [])==0 and coverage>=16:
                await continuity_guard.rebalance_continuity_guarded(force=True)
                recovery_stats["pool_kicks"]+=1
        except Exception as exc:
            print(f"Ψ-RECOVERY POOL_ERROR {type(exc).__name__}: {exc}",flush=True)
        await asyncio.sleep(20.0 if coverage < max(1,int(total*.92)) else 45.0)

async def extension_recovery_loop():
    while app.session is None:
        await asyncio.sleep(.5)
    while True:
        try:
            timeout=aiohttp.ClientTimeout(total=10)
            connector=aiohttp.TCPConnector(limit=2,ttl_dns_cache=30,force_close=True)
            async with aiohttp.ClientSession(timeout=timeout,connector=connector,headers={"User-Agent":getattr(app,"USER_AGENT","psi-v11-recovery"),"Connection":"close"}) as client:
                payload=await asyncio.wait_for(app.api_get(client,"/api/v3/ticker/24hr"),timeout=10.0)
            if not isinstance(payload,list): raise RuntimeError("ticker snapshot not list")
            ts=time.time();new={}
            for item in payload:
                if not isinstance(item,dict): continue
                sym=str(item.get("symbol") or "")
                if not sym: continue
                new[sym]={"change_pct":f(item.get("priceChangePercent")),"open":f(item.get("openPrice")),"high":f(item.get("highPrice")),"low":f(item.get("lowPrice")),"last":f(item.get("lastPrice")),"ts":ts}
            if not new: raise RuntimeError("empty ticker snapshot")
            extrest.ext_cache.clear();extrest.ext_cache.update(new)
            extrest.ext_last_refresh=ts
            extrest.ext_last_error=None
            extrest.ext_refresh_ok+=1
            recovery_stats["ext_ok"]+=1
            print(f"Ψ-RECOVERY EXTENSION status=LIVE symbols={len(new)} ok={recovery_stats['ext_ok']} err={recovery_stats['ext_err']}",flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            recovery_stats["ext_err"]+=1
            extrest.ext_refresh_errors+=1
            extrest.ext_last_error=f"{type(exc).__name__}: {exc}"
            print(f"Ψ-RECOVERY EXTENSION_ERROR {extrest.ext_last_error}",flush=True)
        await asyncio.sleep(20.0)

async def main():
    print("[v11.0.5.0] Ψ BREAKOUT STRUCTURAL INTELLIGENCE active — BSI fuses micro HH/HL structure, MTF alignment, resistance fatigue/attack count, compression, liquidity vacuum/ask depletion, resistance proximity, breakout/retest context, live confirmation, fresh-structure and MA-structure gate state, anti-chase room and false-break risk. BSI changes research ranking/visibility only; Pinpoint remains sole BUY NOW authority and every hard execution gate remains fail-closed. Monster board now emits 30 ranked rows.",flush=True)
    await asyncio.gather(rescue.main(), structure_recovery_loop(), extension_recovery_loop())

if __name__=="__main__":asyncio.run(main())
