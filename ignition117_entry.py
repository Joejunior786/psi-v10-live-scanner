import asyncio
import math
import time
from collections import defaultdict, deque

import ignition116_entry as b16

scanner=b16.scanner; q=scanner.q; s=scanner.s; app=scanner.app
v15=b16.v15; v13=b16.v13
VERSION="10.17.0-atomic-hotpath-stable-shards-outcomes"

EARLY_SLOTS=3; BREAKOUT_SLOTS=3; BUY_SLOTS=4
MIGRATION_BUDGET=4; HOT_SCORE=150.0; HOT_COOLDOWN=15.0; HOT_STRUCTURE_AGE=45.0
SAMPLE_SECONDS=5.0; STALE_START=90.0; STALE_FULL=240.0
OUTCOME_COOLDOWN=180.0

EXCLUDED=set(b16.EXCLUDED_BASES)|{"U","AEUR","USDE","USDS","USDX","USUAL","EUR","GBP","PAXG","XAUT"}
TOKENISED=set(b16.TOKENISED_BASES)|{
    "NVDA","NVDAB","EWY","EWYB","RKLB","AMCB","PYPLB","RDDTB","NOKB","CBRSB","LITEB","INTCB","MRNAB","AVGOB",
    "ADBE","ADBEB","CRCL","CRCLB","TQQQ","TQQQB","SOXL","SOXLB","MSTR","MSTRB","MSFT","MSFTB","AAPL","AAPLB",
    "TSLA","TSLAB","GOOGL","GOOGLB","IBM","IBMB","HOOD","HOODB","SPY","SPYB","SPCX","SPCXB"
}
b16.EXCLUDED_BASES.update(EXCLUDED); b16.TOKENISED_BASES.update(TOKENISED)

dist_hist=defaultdict(lambda:deque(maxlen=80)); last_improve={}; best_dist={}
out_pending=[]; out_resolved=deque(maxlen=1000); out_last=defaultdict(float); out_seq=0
hot_last=0.0
stats={"hot_injected":0,"hot_structures":0,"pool_applied":0,"pool_deferred":0,"last_shard":None}


def f(v,d=0.0):
    try: x=float(v)
    except (TypeError,ValueError): return d
    return x if math.isfinite(x) else d

def base(sym):
    x=str(sym or "").upper(); return x[:-4] if x.endswith("USDT") else x

def directional(sym):
    x=base(sym); return bool(x and x not in EXCLUDED and x not in TOKENISED)

# Filter before the qualifier universe is populated.
_old_exchange=app.get_exchange_symbols
async def filtered_exchange(client):
    rows=await _old_exchange(client); out=[x for x in rows if directional(x[0])]
    if len(out)!=len(rows): print(f"Ψ-V10.17 UNIVERSE_FILTER removed={len(rows)-len(out)} kept={len(out)}",flush=True)
    return out
app.get_exchange_symbols=filtered_exchange

_old_hot=q.hot
def hot(limit=None):
    n=max(int(limit or getattr(q,"HOT_COUNT",80)),80); out=[]
    for score,sym in _old_hot(n*2):
        if directional(sym): out.append((score,sym))
        if len(out)>=n: break
    return out[:(limit or getattr(q,"HOT_COUNT",80))]
q.hot=hot

_old_batch=q.structure_batch_symbols
def structure_batch():
    n=int(getattr(q,"STRUCTURE_BATCH",60)); out=[]; seen=set()
    for sym in _old_batch():
        if directional(sym) and sym not in seen: out.append(sym); seen.add(sym)
    for _,sym in hot(max(120,n*2)):
        if len(out)>=n: break
        if sym in q.universe_set and sym not in seen: out.append(sym); seen.add(sym)
    for sym in q.universe:
        if len(out)>=n: break
        if directional(sym) and sym not in seen: out.append(sym); seen.add(sym)
    return out[:n]
q.structure_batch_symbols=structure_batch

# Atomic price/resistance/distance/entry calculation. Never mix a stale distance with a new entry status.
def atomic_entry(appm,qm,sym,row=None):
    src=dict(row or qm.latest.get(sym) or {}); sd=dict(appm.structure.get(sym) or {}); snap=appm.now_ms()
    price=f(appm.current_symbol_price(sym),0.0) or f(src.get("price"),f(sd.get("price"),0.0)); res=f(sd.get("resistance"),0.0)
    dist=((res-price)/price*100.0) if price>0 and res>0 else None
    spread=None; st=appm.micro_state.get(sym) or {}; spreads=st.get("spread_bps")
    if spreads:
        try: spread=f(spreads[-1][1],None)
        except Exception: spread=None
    if spread is None: spread=f(src.get("spread_bps"),None)
    buf=ref=entry=None; status="NO_RESISTANCE"; armed=False
    if res>0:
        status="WAIT_SPREAD"
        if spread is not None and spread>=0:
            buf=max(float(v13.ENTRY_MIN_BUFFER_BPS),min(float(v13.ENTRY_MAX_BUFFER_BPS),spread*1.5)); ref=res*(1+buf/10000)
            if dist is None: status="WAIT_BREAKOUT_DISTANCE"
            elif price>=ref: status="RETEST_REQUIRED_ALREADY_ABOVE_TRIGGER"
            elif dist>float(v13.ENTRY_NEAR_BREAKOUT_PCT): status="WAIT_APPROACH"
            elif dist>=0: entry=ref; status="BREAKOUT_TRIGGER_ARMED"; armed=True
            else: entry=ref; status="CONFIRM_BREAKOUT_BUFFER"; armed=True
    hs=src.get("hard_safety_status") or {}; strict=str(src.get("pre_warmup_state") or src.get("state") or "")=="BUY NOW"
    exec_all=all(hs.get(k)=="PASS" for k in ("LIVE_MICRO_DATA","TRADE_SEQUENCE_VALID","BOOK_SEQUENCE_VALID","SPREAD_FILTER","SLIPPAGE_FILTER","CUMULATIVE_EXTENSION_GUARD"))
    return {"snapshot_ms":snap,"price":price or None,"resistance":res or None,"breakout_distance_pct":dist,
            "distance_to_resistance_bps":None if dist is None else round(dist*100,2),"entry_buffer_bps":None if buf is None else round(buf,2),
            "breakout_trigger_reference":ref,"breakout_entry_trigger":entry,"entry_trigger_verified":ref is not None,
            "entry_trigger_armed":armed,"entry_status":status,"entry_actionable_now":bool(strict and exec_all and armed)}
v13._entry_telemetry=atomic_entry

# Distance velocity: positive = closing on resistance.
def record_dist(sym,d,now=None):
    if d is None or not math.isfinite(float(d)): return
    now=now or time.time(); dq=dist_hist[sym]
    if dq and now-dq[-1][0]<4: return
    d=float(d); dq.append((now,d))
    while dq and dq[0][0]<now-300: dq.popleft()
    if sym not in best_dist or d<=best_dist[sym]-0.10: best_dist[sym]=d; last_improve[sym]=now
    elif sym not in last_improve: last_improve[sym]=now

def velocity(sym,secs):
    dq=dist_hist.get(sym)
    if not dq or len(dq)<2: return 0.0
    now,cur=dq[-1]; old=dq[0]
    for x in reversed(dq):
        if x[0]<=now-secs: old=x; break
    dt=max(1,now-old[0]); return (old[1]-cur)/(dt/60)

def momentum(sym):
    v30=velocity(sym,30); v60=velocity(sym,60); v180=velocity(sym,180); stale=max(0,time.time()-last_improve.get(sym,time.time()))
    pen=0 if stale<=STALE_START else 25*min(1,(stale-STALE_START)/max(1,STALE_FULL-STALE_START))
    return {"distance_velocity_30s_per_min":round(v30,4),"distance_velocity_60s_per_min":round(v60,4),
            "distance_velocity_180s_per_min":round(v180,4),"distance_acceleration":round(v30-v180,4),
            "no_progress_seconds":round(stale,1),"no_progress_penalty":round(pen,2)}

_old_diag=b16._diag_pool
def diag_pool():
    now=time.time(); out=[]
    for raw in _old_diag():
        sym=str(raw.get("symbol") or "")
        if not directional(sym): continue
        row=dict(raw); row.update(atomic_entry(app,q,sym,q.latest.get(sym) or row)); record_dist(sym,row.get("breakout_distance_pct"),now); row.update(momentum(sym)); out.append(row)
    return out
b16._diag_pool=diag_pool

_old_score=b16._opportunity_score
def opp_score(row):
    score=float(_old_score(row)); v30=f(row.get("distance_velocity_30s_per_min")); v60=f(row.get("distance_velocity_60s_per_min")); acc=f(row.get("distance_acceleration")); pen=f(row.get("no_progress_penalty"))
    bonus=max(-8,min(12,v30*4))+max(-5,min(8,v60*2))+max(-4,min(6,acc*1.5))
    return round(score+bonus-pen,2)
b16._opportunity_score=opp_score

# Cap each ordinary rebalance to one existing shard and <=4 replacements.
_old_rebalance=q.rebalance_pool
async def rebalance(force=False):
    before=list(dict.fromkeys(app.selected_micro_symbols)); before_set=set(before)
    await _old_rebalance(force); desired=[x for x in dict.fromkeys(app.selected_micro_symbols) if directional(x)]
    if not before or not b16.shard_assignments:
        app.selected_micro_symbols=desired[:int(getattr(q,"MICRO_SLOTS",80))]; return
    desired_set=set(desired); removals=[x for x in before if x not in desired_set or not directional(x)]; additions=[x for x in desired if x not in before_set]
    if not removals or not additions:
        app.selected_micro_symbols=[x for x in before if directional(x)]
        for x in desired:
            if len(app.selected_micro_symbols)>=int(getattr(q,"MICRO_SLOTS",80)): break
            if x not in app.selected_micro_symbols: app.selected_micro_symbols.append(x)
        return
    groups=defaultdict(list)
    for x in removals: groups[b16.shard_assignments.get(x,-1)].append(x)
    shard=max(groups,key=lambda k:(sum(not directional(x) for x in groups[k]),len(groups[k]))); rm=groups[shard][:MIGRATION_BUDGET]; n=min(len(rm),len(additions)); rm=rm[:n]; add=additions[:n]
    final=[x for x in before if x not in set(rm)]+add
    app.selected_micro_symbols=final[:int(getattr(q,"MICRO_SLOTS",80))]; app.last_micro_pool_change=time.time()
    for x in add: q.entered[x]=time.time(); app.ensure_micro_state(x)
    for x in rm: q.entered.pop(x,None); v15.evicted_until[x]=time.time()+float(v15.RECYCLE_COOLDOWN_SECONDS)
    stats["pool_applied"]+=len(add); stats["pool_deferred"]+=max(0,min(len(removals),len(additions))-len(add)); stats["last_shard"]=None if shard<0 else shard+1
    print(f"Ψ-V10.17 MICRO_STABLE shard={stats['last_shard']} added={len(add)} removed={len(rm)} deferred={stats['pool_deferred']}",flush=True)
q.rebalance_pool=rebalance

# Preserve shard membership. New names fill the exact vacancies left by removed names.
def stable_assign():
    selected=[x for x in dict.fromkeys(app.selected_micro_symbols) if directional(x)][:b16.MICRO_SHARDS*b16.MICRO_SHARD_SIZE]; sset=set(selected); changed=set()
    if not b16.shard_assignments:
        for i,x in enumerate(selected): b16.shard_assignments[x]=min(b16.MICRO_SHARDS-1,i//b16.MICRO_SHARD_SIZE)
    for x,sh in list(b16.shard_assignments.items()):
        if x not in sset: b16.shard_assignments.pop(x,None); changed.add(sh)
    counts=[0]*b16.MICRO_SHARDS
    for x,sh in b16.shard_assignments.items():
        if x in sset and 0<=sh<b16.MICRO_SHARDS: counts[sh]+=1
    for x in selected:
        if x in b16.shard_assignments: continue
        elig=[i for i in range(b16.MICRO_SHARDS) if counts[i]<b16.MICRO_SHARD_SIZE]
        if not elig: break
        sh=min(elig,key=lambda i:(counts[i],i)); b16.shard_assignments[x]=sh; counts[sh]+=1; changed.add(sh)
    for sh in range(b16.MICRO_SHARDS):
        members=[x for x in selected if b16.shard_assignments.get(x)==sh][:b16.MICRO_SHARD_SIZE]
        if set(members)!=set(b16.shard_current_symbols[sh]): b16.shard_current_symbols[sh]=members; b16.shard_generation[sh]+=1; b16.shard_last_change[sh]=time.time(); changed.add(sh)
    return changed
b16._assign_shards=stable_assign

async def refresh_hot(sym):
    if app.session is None: return False
    try: sd,an=await asyncio.gather(app.load_structure(app.session,sym),app.load_fast_anomaly(app.session,sym))
    except Exception: return False
    if isinstance(sd,dict): app.structure[sym]=sd; q.structure_ms[sym]=q.ms(); stats["hot_structures"]+=1
    if isinstance(an,dict): app.anomaly_state[sym]=an
    return isinstance(sd,dict)

def weak_victim():
    locks=set(q.locks()); now=time.time(); rows=[]
    for x in app.selected_micro_symbols:
        if x in locks or not directional(x): continue
        sh=b16.shard_assignments.get(x)
        if sh is None or now-f(b16.shard_last_change[sh])<HOT_COOLDOWN: continue
        r=q.latest.get(x) or {}; state=str(r.get("formal_state") or r.get("state") or "")
        if state in ("BUY NOW","PRE-IGNITION") or r.get("ignition15_watch"): continue
        rows.append((f(q.sscore(x),-999) if x in app.structure else -999,x,sh))
    return min(rows) if rows else None

async def hot_loop():
    global hot_last
    while True:
        await asyncio.sleep(2)
        try:
            if not q.universe or app.session is None: continue
            for score,sym in hot(16):
                if score<HOT_SCORE: break
                if sym in app.selected_micro_symbols: continue
                age=(q.ms()-int(q.structure_ms.get(sym,0) or 0))/1000 if q.structure_ms.get(sym) else 999999
                if age>HOT_STRUCTURE_AGE and not await refresh_hot(sym): continue
                if time.time()-hot_last<HOT_COOLDOWN: continue
                victim=weak_victim()
                if not victim: continue
                _,old,sh=victim; final=[x for x in app.selected_micro_symbols if x!=old]; final.append(sym); app.selected_micro_symbols=final[:int(getattr(q,"MICRO_SLOTS",80))]
                q.entered.pop(old,None); q.entered[sym]=time.time(); v15.evicted_until[old]=time.time()+float(v15.RECYCLE_COOLDOWN_SECONDS); app.ensure_micro_state(sym); app.last_micro_pool_change=time.time(); hot_last=time.time(); stats["hot_injected"]+=1
                print(f"Ψ-V10.17 HOTPATH promoted={sym} discovery={score:.1f} replaced={old} shard={sh+1}",flush=True); break
        except asyncio.CancelledError: raise
        except Exception as e: print(f"Ψ-V10.17 HOTPATH_ERROR {type(e).__name__}: {e}",flush=True)

# Truthful board: heuristic lane is EARLY-IGNITION, never labelled formal PRE.
def board117():
    cand=diag_pool(); cand.sort(key=opp_score,reverse=True); used=set()
    early=b16._lane_pick(cand,used,EARLY_SLOTS,b16._pre_strict,b16._pre_sort,"EARLY-IGNITION")
    brk=b16._lane_pick(cand,used,BREAKOUT_SLOTS,b16._breakout_strict,b16._breakout_sort,"READY-BREAKOUT")
    buy=b16._lane_pick(cand,used,BUY_SLOTS,b16._buy_strict,b16._buy_sort,"CLOSEST-BUY")
    out=early+brk+buy
    for i,r in enumerate(out[:10],1): r["board_rank"]=i; r["opportunity_score"]=opp_score(r)
    return out[:10]
b16.opportunity_board=board117

def record_outcomes(board):
    global out_seq
    now=time.time()
    for r in board:
        sym=str(r.get("symbol") or "")
        if not directional(sym) or now-out_last[sym]<OUTCOME_COOLDOWN: continue
        price=f(r.get("price")) or f(app.current_symbol_price(sym))
        if price<=0: continue
        out_last[sym]=now; out_seq+=1
        out_pending.append({"id":out_seq,"symbol":sym,"lane":r.get("board_lane"),"quality":r.get("board_quality"),"alert_ts":now,"alert_price":price,
                            "trigger":r.get("breakout_trigger_reference") or r.get("breakout_entry_trigger"),"returns":{},"max_return_pct":0.0,"max_drawdown_pct":0.0,"trigger_cross_seconds":None})
    del out_pending[:-500]

def update_outcomes():
    now=time.time(); keep=[]
    for e in out_pending:
        p=f(app.current_symbol_price(e["symbol"])); basep=f(e.get("alert_price"))
        if p<=0 or basep<=0: keep.append(e); continue
        ret=(p/basep-1)*100; e["max_return_pct"]=max(f(e.get("max_return_pct")),ret); e["max_drawdown_pct"]=min(f(e.get("max_drawdown_pct")),ret)
        tr=f(e.get("trigger")); age=now-e["alert_ts"]
        if tr>0 and e.get("trigger_cross_seconds") is None and p>=tr: e["trigger_cross_seconds"]=round(age,2)
        for label,secs in (("1m",60),("5m",300),("15m",900),("1h",3600)):
            if age>=secs and label not in e["returns"]: e["returns"][label]=round(ret,4)
        if age>=3600: out_resolved.append(dict(e))
        else: keep.append(e)
    out_pending[:]=keep[-500:]

async def sample_loop():
    while True:
        await asyncio.sleep(SAMPLE_SECONDS)
        try:
            now=time.time(); syms=list(dict.fromkeys(list(app.selected_micro_symbols)+[x for _,x in hot(20)]))
            for sym in syms:
                if directional(sym): record_dist(sym,atomic_entry(app,q,sym,q.latest.get(sym) or {}).get("breakout_distance_pct"),now)
            update_outcomes()
        except asyncio.CancelledError: raise
        except Exception as e: print(f"Ψ-V10.17 SAMPLE_ERROR {type(e).__name__}: {e}",flush=True)

async def board_loop():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            board=board117(); record_outcomes(board)
            fp=sum(str(x.get("formal_state") or x.get("state"))=="PRE-IGNITION" for x in board); fb=sum(str(x.get("formal_state") or x.get("state"))=="BUY NOW" for x in board)
            eq=sum(x.get("board_lane")=="EARLY-IGNITION" and x.get("board_quality")=="QUALIFIED" for x in board); bq=sum(x.get("board_lane")=="READY-BREAKOUT" and x.get("board_quality")=="QUALIFIED" for x in board); cq=sum(x.get("board_lane")=="CLOSEST-BUY" and x.get("board_quality")=="QUALIFIED" for x in board)
            print(f"Ψ-V10.17 TOP10 BOARD {len(board)}/10 early={eq}/3 breakout={bq}/3 closest_buy={cq}/4 formal_pre={fp} formal_buy={fb}",flush=True)
            for i,r in enumerate(board,1):
                sym=str(r.get("symbol") or "-"); lane=str(r.get("board_lane") or "-"); state=str(r.get("formal_state") or r.get("state") or "-"); layers=b16._layers(r); ex="PASS_ALL" if b16._execution_pass(r) else "BLOCKED"; mi="READY" if b16._micro_pass(r) else "WAIT"; res=r.get("resistance"); ent=r.get("breakout_entry_trigger"); d=r.get("breakout_distance_pct"); st=str(r.get("entry_status") or "-"); rt="-" if res is None else f"{float(res):.10g}"; et=st if ent is None else f"{float(ent):.10g}"; dt="-" if d is None else f"{float(d):+.3f}%"; blockers=r.get("combined_blockers") or r.get("missing_signal_layers") or []
                print(f"B{i:02d}. {sym:14s} lane={lane:14s} state={state:18s} opp={opp_score(r):6.1f} layers={layers}/6 exec={ex:8s} micro={mi:5s} ign15={f(r.get('ignition15_score')):5.1f} res={rt} dist={dt} entry={et} status={st} dV30={f(r.get('distance_velocity_30s_per_min')):+.3f}/m dV60={f(r.get('distance_velocity_60s_per_min')):+.3f}/m stale={f(r.get('no_progress_seconds')):.0f}s blockers={blockers} snapshot={r.get('snapshot_ms')}",flush=True)
            crossed=sum(e.get("trigger_cross_seconds") is not None for e in out_pending)
            print(f"Ψ-V10.17 PIPELINE hot_injected={stats['hot_injected']} hot_structures={stats['hot_structures']} pool_shard={stats['last_shard']} pool_applied={stats['pool_applied']} pool_deferred={stats['pool_deferred']} shards={sum(b16.shard_connected)}/4 reconnects={b16.shard_reconnects}",flush=True)
            print(f"Ψ-V10.17 OUTCOMES pending={len(out_pending)} resolved={len(out_resolved)} trigger_crossed_pending={crossed}",flush=True)
        except Exception as e: print(f"Ψ-V10.17 BOARD_ERROR {type(e).__name__}: {e}",flush=True)

b16._v1016_board_loop=board_loop
_old_main=scanner.v7.main
async def main117(): await asyncio.gather(_old_main(),hot_loop(),sample_loop())
scanner.v7.main=main117; scanner.VERSION=VERSION

print("Ψ-V10.17 UPGRADE ACTIVE — atomic snapshots, EARLY-IGNITION truthful lanes, resistance momentum/stagnation decay, hot-path promotion, one-shard migration budget, noncrypto universe filter, forward outcomes; formal PRE/BUY unchanged",flush=True)
if __name__=="__main__":
    try: print("Ψ-V10.17 ACTIVE — pump timing + stable micro continuity",flush=True); asyncio.run(scanner.v7.main())
    except KeyboardInterrupt: print("Ψ-V10.17 stopped",flush=True)
