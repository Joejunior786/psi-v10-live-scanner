import asyncio, json, math, os, statistics, time
from collections import defaultdict, deque
import psi_v11_3_7_entry as tape

base=tape.base
app,q,scanner=base.app,base.q,base.scanner
VERSION="11.0.4.0-monster-rescue-learning"
MEMORY_WINDOW_S=120.0
IGN_SHORT=1800.0
IGN_LONG=7200.0
RESCUE_STATE=os.environ.get("PSI_MONSTER_RESCUE_STATE","/data/psi_monster_rescue_state.json")
RESCUE_SAVE_S=30.0
MAX_DEEP_POOL=320
EXTRA_RESCUE_SLOTS=72

memory_hist=defaultdict(lambda:deque(maxlen=320))
ignition_events=defaultdict(lambda:deque(maxlen=128))
active_seen=defaultdict(lambda:{"active":False,"last":0.0})
rescue_stats=defaultdict(int)
rescue_last_save=0.0

_old_candidate=base.candidate
_old_save=base.save

def f(v,d=0.0):
    try:x=float(v)
    except (TypeError,ValueError):return d
    return x if math.isfinite(x) else d

def cl(v,a=0.0,b=1.0):return max(a,min(b,v))

def _row_blockers(row):
    out=[]
    for key in ("pinpoint_blockers","blockers","hard_blockers","pinpoint_hard_blockers"):
        vals=row.get(key)
        if isinstance(vals,(list,tuple,set)):out.extend(str(x) for x in vals if x)
    for key in ("pinpoint_hard_status","hard_safety_status"):
        d=row.get(key) or {}
        if isinstance(d,dict):
            for k,v in d.items():
                if not (v is True or str(v).upper()=="PASS"):out.append(str(k))
    if not row.get("pinpoint_setup"):out.append("NO_SETUP")
    return list(dict.fromkeys(out))

def _rescue_score(c,row):
    event=cl(f(c.get("event_tape_score"))/100)
    cheap=cl(f(c.get("cheap"))/100)
    peak=cl(f(c.get("peak"))/180)
    buy=cl((f(c.get("event_buy_ratio"),.5)-.5)/.25)
    cvd=cl((f(c.get("event_cvd"))+.02)/.42)
    nacc=cl((f(c.get("event_notional_accel"))-.8)/4.2)
    cacc=cl((f(c.get("event_count_accel"))-.8)/4.2)
    bbo=cl((f(c.get("event_bbo_imb"))+.05)/.55)
    layer=cl(f(c.get("layers"))/6)
    formal=str(row.get("formal_state") or row.get("state") or "")
    fb=1.0 if formal in {"EARLY OPPORTUNITY","PRE-IGNITION"} else 0.0
    return cl(100*(.24*cheap+.16*event+.13*peak+.09*buy+.09*cvd+.08*nacc+.06*cacc+.05*bbo+.06*layer+.04*fb),0,100)

def _emergency_promote(c,row):
    rs=_rescue_score(c,row)
    event=f(c.get("event_tape_score"));buy=f(c.get("event_buy_ratio"),.5);cvd=f(c.get("event_cvd"))
    nacc=f(c.get("event_notional_accel"));cacc=f(c.get("event_count_accel"));peak=f(c.get("peak"))
    formal=str(row.get("formal_state") or row.get("state") or "")
    ok=(rs>=55 or (event>=60 and buy>=.62 and cvd>=.18) or (nacc>=4 and cacc>=3 and event>=45)
        or peak>=115 or (formal in {"EARLY OPPORTUNITY","PRE-IGNITION"} and rs>=48))
    return ok,rs

def _prune(sym,now):
    h=memory_hist[sym]
    while h and h[0]["t"]<now-MEMORY_WINDOW_S:h.popleft()
    ev=ignition_events[sym]
    while ev and ev[0]<now-IGN_LONG:ev.popleft()

def _memory(sym,now):
    _prune(sym,now);h=memory_hist[sym]
    if not h:return {"dna":0.0,"sp20":0.0,"pb15":0.0,"event":0.0,"cvd":0.0,"buy":.5,"age":999999.0}
    pd=max(h,key=lambda x:x["dna"]);ps=max(h,key=lambda x:x["sp20"]);pp=max(h,key=lambda x:x["pb15"])
    pe=max(h,key=lambda x:x["event"]);pc=max(h,key=lambda x:x["cvd"]);pb=max(h,key=lambda x:x["buy"])
    return {"dna":pd["dna"],"sp20":ps["sp20"],"pb15":pp["pb15"],"event":pe["event"],"cvd":pc["cvd"],"buy":pb["buy"],"age":max(0,now-ps["t"])}

def _mark_ignition(sym,state,now):
    st=active_seen[sym];active=state in {"MONSTER-HOT","MONSTER-IGNITION"}
    if active and (not st["active"] or now-f(st["last"])>15):
        ignition_events[sym].append(now);rescue_stats["ignition_episodes"]+=1
    st["active"]=active;st["last"]=now

def _repeat(sym,now):
    _prune(sym,now);ev=ignition_events[sym]
    return sum(t>=now-IGN_SHORT for t in ev),sum(t>=now-IGN_LONG for t in ev)

def candidate_v4(sym,row,c,d):
    now=time.time();out=_old_candidate(sym,row,c,d);raw=str(out.get("state") or "MONSTER-WATCH")
    rescue=f(c.get("rescue_score")) or _rescue_score(c,row)
    mtf_bonus=0.0
    if f(d.get("mtn"))>=.95:
        mtf_bonus+=1.4
        if f(d.get("sf"))>=60 or f(d.get("event"))>=55:mtf_bonus+=1.4
    sf_bonus=1.8*cl((f(d.get("sf"),50)-55)/30)
    rs_pen=2.5 if (f(c.get("ex60"))>=.15 and f(d.get("event"))<40 and f(d.get("flow"),50)<55 and f(c.get("event_tape_score"))<45) else 0.0
    out["dna"]=cl(f(out.get("dna"))+mtf_bonus+sf_bonus-rs_pen,0,100)
    out["early"]=cl(f(out.get("early"))+.7*mtf_bonus+.6*sf_bonus-rs_pen,0,100)
    out["rescueScore"]=rescue;out["blockers"]=_row_blockers(row);out["rawState"]=raw
    memory_hist[sym].append({"t":now,"dna":f(out.get("dna")),"sp20":f(out.get("sp20")),"pb15":f(out.get("pb15")),
                             "event":f(out.get("event")),"cvd":f(out.get("cvd1s")),"buy":f(out.get("buy1s"),.5)})
    _mark_ignition(sym,raw,now);mem=_memory(sym,now);i30,i2=_repeat(sym,now)
    out.update({"peakDna120":mem["dna"],"peakShP20_120":mem["sp20"],"peakPB15_120":mem["pb15"],
                "peakEvent120":mem["event"],"peakCvd120":mem["cvd"],"peakBuy120":mem["buy"],"peak20Age":mem["age"],
                "ignitions30m":i30,"ignitions2h":i2})
    repeat_bonus=min(5,max(0,i30-1)*1.5+max(0,i2-i30)*.35)
    decay=cl(1-mem["age"]/MEMORY_WINDOW_S)
    memory_bonus=5*decay if mem["sp20"]>=.18 and mem["dna"]>=66 else 0
    if memory_bonus and "20PCT_PEAK_MEMORY" not in out["reasons"]:out["reasons"].append("20PCT_PEAK_MEMORY")
    if i30>=2 and "REPEAT_IGNITION" not in out["reasons"]:out["reasons"].append("REPEAT_IGNITION")
    out["retentionScore"]=cl(f(out.get("early"))+memory_bonus+repeat_bonus+max(0,rescue-55)*.18,0,100)
    if raw!="MONSTER-EXTENDED":
        if raw in {"MONSTER-WATCH","MONSTER-SEED"} and mem["sp20"]>=.18 and mem["dna"]>=66 and decay>0:
            out["state"]="MONSTER-MEMORY"
        elif raw in {"MONSTER-WATCH","MONSTER-SEED"} and rescue>=62 and len(out.get("reasons") or [])>=4:
            out["state"]="MONSTER-RESCUE"
            if "EMERGENCY_RESCUE_PROMOTION" not in out["reasons"]:out["reasons"].append("EMERGENCY_RESCUE_PROMOTION")
    return out

base.candidate=candidate_v4

def open_obs_v4(c):
    sym=c["symbol"];now=time.time()
    qualifies=(f(c.get("early"))>=42 or f(c.get("rescueScore"))>=60 or c.get("state") in {"MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-IGNITION","MONSTER-HOT"})
    if (not qualifies or f(c.get("price"))<=0 or now-base.last_open[sym]<base.OBS_COOLDOWN or any(x.get("symbol")==sym for x in base.pending)):return
    base.pending.append({"id":f"{sym}:{int(now*1000)}","symbol":sym,"opened":now,"entry":f(c.get("price")),
        "features":dict(c.get("features") or {}),"dna":f(c.get("dna")),"early":f(c.get("early")),"state":c.get("state"),
        "raw_state":c.get("rawState"),"reasons":list(c.get("reasons") or []),"blockers":list(c.get("blockers") or []),
        "rescue_score":f(c.get("rescueScore")),"peak_shp20_120":f(c.get("peakShP20_120")),"peak_dna_120":f(c.get("peakDna120")),
        "ignitions_30m":int(f(c.get("ignitions30m"))),"ignitions_2h":int(f(c.get("ignitions2h"))),
        "hit_ts":{},"max_return_pct":0.0,"max_drawdown_pct":0.0})
    base.last_open[sym]=now;base.stats["opened"]+=1;rescue_stats["observations_opened"]+=1
    if len(base.pending)>base.MAX_PENDING:del base.pending[:-base.MAX_PENDING]

base.open_obs=open_obs_v4

def _load_rescue():
    if not os.path.exists(RESCUE_STATE):return
    try:
        with open(RESCUE_STATE,encoding="utf-8") as fh:d=json.load(fh)
        now=time.time()
        for sym,vals in (d.get("ignition_events") or {}).items():
            for t in vals:
                t=f(t)
                if t>=now-IGN_LONG:ignition_events[str(sym)].append(t)
        rescue_stats["state_loaded"]=1
    except Exception as e:print(f"Ψ-MONSTER-RESCUE LOAD_ERROR {type(e).__name__}: {e}",flush=True)

def _save_rescue(force=False):
    global rescue_last_save
    now=time.time()
    if not force and now-rescue_last_save<RESCUE_SAVE_S:return
    try:
        os.makedirs(os.path.dirname(RESCUE_STATE) or ".",exist_ok=True);tmp=RESCUE_STATE+".tmp"
        data={"version":VERSION,"saved":now,"ignition_events":{s:[t for t in dq if t>=now-IGN_LONG] for s,dq in ignition_events.items() if dq}}
        with open(tmp,"w",encoding="utf-8") as fh:json.dump(data,fh,separators=(",",":"),ensure_ascii=False)
        os.replace(tmp,RESCUE_STATE);rescue_last_save=now
    except Exception as e:rescue_stats["save_errors"]+=1;print(f"Ψ-MONSTER-RESCUE SAVE_ERROR {type(e).__name__}: {e}",flush=True)

def save_v4(force=False):
    _old_save(force);_save_rescue(force)
base.save=save_v4

def scan_v4():
    now=time.time();u=list(getattr(q,"universe",[]) or []);rows=[]
    for sym in u:
        row=q.latest.get(sym) or {};p=base.px(sym,row)
        if p>0 and (not base.price_hist[sym] or now-base.price_hist[sym][-1][0]>=.45):base.price_hist[sym].append((now,p))
        c=base.cheap(sym,row,now);em,rs=_emergency_promote(c,row);c["rescue_score"]=rs;c["rescue_promote"]=em
        rows.append((f(c.get("cheap")),rs,sym,row,c))
    rows.sort(reverse=True,key=lambda x:x[0]);pool=[(a,s,r,c) for a,_,s,r,c in rows[:base.DEEP_LIMIT]];seen={s for _,s,_,_ in pool}
    for a,rs,s,r,c in rows:
        if s not in seen and (f(c.get("peak"))>=100 or f(c.get("radar_n"))>=.45 or f(c.get("r60"))>=.75):
            pool.append((a,s,r,c));seen.add(s)
    emergency=sorted([x for x in rows if x[2] not in seen and x[4].get("rescue_promote")],key=lambda x:(x[1],x[0]),reverse=True)[:EXTRA_RESCUE_SLOTS]
    for a,rs,s,r,c in emergency:
        if len(pool)>=MAX_DEEP_POOL:break
        pool.append((a,s,r,c));seen.add(s);rescue_stats["emergency_promotions"]+=1
    out=[]
    for _,s,row,c in pool:
        ca=base.candidate(s,row,c,base.deep(s));base.latest[s]=ca
        visible=(f(ca.get("early"))>=38 or f(ca.get("dna"))>=50 or f(ca.get("peak"))>=120 or ca.get("state") in {"MONSTER-RESCUE","MONSTER-MEMORY"} or f(ca.get("retentionScore"))>=58)
        if visible:out.append(ca);base.open_obs(ca)
    priority={"MONSTER-HOT":6,"MONSTER-IGNITION":5,"MONSTER-MEMORY":4,"MONSTER-RESCUE":3,"MONSTER-SEED":2,"MONSTER-EXTENDED":1,"MONSTER-WATCH":0}
    out.sort(key=lambda x:(priority.get(str(x.get("state")),0),f(x.get("retentionScore")),f(x.get("early")),f(x.get("dna")),f(x.get("peak"))),reverse=True)
    base.stats["cycles"]+=1;base.stats["universe"]=len(u);base.stats["deep"]=len(pool);base.stats["cand"]=len(out)
    rescue_stats["last_pool"]=len(pool);rescue_stats["last_emergency"]=len(emergency)
    return out[:16]
base.scan=scan_v4

def _blocker_learning():
    src=[x for x in base.resolved if isinstance(x,dict) and x.get("blockers")]
    tab=defaultdict(lambda:{"n":0,"w10":0,"w20":0,"sum":0.0})
    for e in src:
        mfe=f(e.get("max_return_pct"))
        for b in e.get("blockers") or []:
            z=tab[str(b)];z["n"]+=1;z["w10"]+=int(mfe>=10);z["w20"]+=int(mfe>=20);z["sum"]+=mfe
    out=[]
    for b,z in tab.items():
        if z["n"]>=3:out.append((z["w20"]/z["n"],z["w10"]/z["n"],z["sum"]/z["n"],z["n"],b))
    out.sort(reverse=True)
    return out[:8],len(src)

def _path_learning():
    src=[x for x in base.resolved if isinstance(x,dict) and x.get("hit_ts") and f(x.get("opened"))>0];out={}
    for target in (5,10,15,20):
        vals=sorted((f((e.get("hit_ts") or {}).get(str(target)))-f(e.get("opened")))/60 for e in src if f((e.get("hit_ts") or {}).get(str(target)))>f(e.get("opened")))
        if vals:out[target]={"n":len(vals),"median_min":round(statistics.median(vals),1),"p75_min":round(vals[min(len(vals)-1,int(.75*(len(vals)-1)))],1)}
    return out

def _pt(p):return "-" if not isinstance(p,dict) or p.get("mean") is None else f"{100*f(p.get('mean')):.1f}%[{100*f(p.get('lower')):.1f}]"

async def board_loop_v4():
    while True:
        await asyncio.sleep(base.BOARD_S)
        try:
            base.refresh_adapt();rows=list(base.latest.get("_board") or [])
            states=("MONSTER-HOT","MONSTER-IGNITION","MONSTER-MEMORY","MONSTER-RESCUE","MONSTER-SEED","MONSTER-EXTENDED")
            counts={k:sum(r.get("state")==k for r in rows) for k in states}
            ups=sum(int(tape.tape_stats.get(f"shard_{i}_up",0)) for i in range(tape.SHARDS))
            ready=sum(1 for s in list(getattr(q,"universe",[]) or []) if tape.tape_metric(s).get("ready"))
            print(f"Ψ-MONSTER-RADAR BOARD scanned={base.stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={base.stats['deep']} candidates={base.stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} memory={counts['MONSTER-MEMORY']} rescue={counts['MONSTER-RESCUE']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} scan={int(base.SCAN_S*1000)}ms tape={ready}/{len(getattr(q,'universe',[]) or [])} shards={ups}/{tape.SHARDS} trades={tape.tape_stats['trades']} books={tape.tape_stats['books']} learning={base.adapt['status']} obsPending={len(base.pending)} obsResolved={len(base.resolved)} PinpointAuthority=YES",flush=True)
            for i,r in enumerate(rows,1):
                ds="-" if r.get("dist") is None else f"{f(r.get('dist')):+.2f}%";age=f(r.get("peak20Age"),999999)
                mem="-" if age>MEMORY_WINDOW_S else f"{100*f(r.get('peakShP20_120')):.1f}%/{age:.0f}s"
                print(f"MR{i:02d}. {r['symbol']:<14} state={str(r.get('state')):<17} EARLY={f(r.get('early')):5.1f} DNA={f(r.get('dna')):5.1f} retain={f(r.get('retentionScore')):5.1f} rescue={f(r.get('rescueScore')):5.1f} rapid={f(r.get('rapid')):6.1f}/{f(r.get('peak')):6.1f} tape={f(r.get('eventTape')):4.0f} buy1={100*f(r.get('buy1s'),.5):4.0f}% cvd1={f(r.get('cvd1s')):+.2f} nA={f(r.get('notionalA')):.1f}x cA={f(r.get('countA')):.1f}x spr={f(r.get('spreadBps'),999):.2f}bp r15={f(r.get('r15')):+.2f}% r60={f(r.get('r60')):+.2f}% event={f(r.get('event')):4.0f} xv={f(r.get('xlead')):4.0f} vac={f(r.get('vac')):4.0f} pB15={100*f(r.get('pb15')):4.1f}% shP20={100*f(r.get('sp20')):4.1f}% mem20={mem} ign30/2h={int(f(r.get('ignitions30m')))}/{int(f(r.get('ignitions2h')))} layers={int(f(r.get('layers')))}/6 dist={ds} P20e4h={_pt(r.get('p20'))} formal={r.get('formal')} pp={r.get('pp')} why={(r.get('reasons') or [])[:9]}",flush=True)
            br,bn=_blocker_learning()
            print("Ψ-MONSTER-BLOCKER-LEARN "+(f"resolved={bn} top={[(b,n,round(w10*100,1),round(w20*100,1),round(mfe,2)) for w20,w10,mfe,n,b in br]}" if bn else "status=WARMING resolved=0"),flush=True)
            paths=_path_learning();print(f"Ψ-MONSTER-PATH-LEARN {paths}" if paths else "Ψ-MONSTER-PATH-LEARN status=WARMING",flush=True)
            print(f"Ψ-MONSTER-RESCUE HEALTH emergencyPromotions={rescue_stats['emergency_promotions']} lastEmergency={rescue_stats['last_emergency']} deepPool={rescue_stats['last_pool']} ignitionEpisodes={rescue_stats['ignition_episodes']} memoryWindow={int(MEMORY_WINDOW_S)}s buyAuthority=PINPOINT_ONLY",flush=True)
            if base.adapt["status"]=="ACTIVE":
                lifts=sorted(base.adapt["lift"].items(),key=lambda x:x[1],reverse=True)[:8]
                print(f"Ψ-MONSTER-DNA LEARNED n={base.adapt['n']} winners20={base.adapt['winners20']} topLift={[(k,round(v,3)) for k,v in lifts]}",flush=True)
            else:print(f"Ψ-MONSTER-DNA LEARNING status=WARMING n={base.adapt['n']} winners20={base.adapt['winners20']} target=+20%/4h",flush=True)
            _save_rescue()
        except asyncio.CancelledError:_save_rescue(True);raise
        except Exception as e:base.stats["board_errors"]+=1;print(f"Ψ-MONSTER-RADAR BOARD_ERROR {type(e).__name__}: {e}",flush=True)

base.board_loop=board_loop_v4
for mod in (tape,base,getattr(base,"scientist",None),scanner):
    try:mod.VERSION=VERSION
    except Exception:pass

_load_rescue()

async def main():
    print("[v11.0.4.0] Ψ MONSTER RESCUE & LEARNING active — emergency full-universe deep promotion, 120s peak-memory retention, repeat-ignition tracking, blocker-outcome recording, target-path learning, MTF/spot-futures tail bonuses and isolated-relative-strength downweighting. Detection/retention are more sensitive; execution is unchanged. Pinpoint remains sole BUY NOW authority and no hard safety gate is bypassed.",flush=True)
    await tape.main()

if __name__=="__main__":asyncio.run(main())
