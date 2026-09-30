import asyncio, time
from collections import Counter, defaultdict, deque
import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base

VERSION="10.8-layer-runner"
PERSIST_WINDOW=90
PERSIST_HITS=2
hits=defaultdict(lambda:deque(maxlen=20))

v7.VERSION=VERSION
base.VERSION=VERSION
base.HISTORY_SAMPLES=600
v7.RAPID_MICRO_SLOTS=16
v7.RAPID_MIN_HOLD=300
v7.RAPID_REPLACE_COOLDOWN=30
v7.RAPID_STRUCTURE_COOLDOWN=30
v7.RAPID_EMERGENCY_SCORE_GAP=30.0
q.MICRO_HOLD=max(q.MICRO_HOLD,480)
q.LOCK_GRACE=max(q.LOCK_GRACE,600)
q.TICK_SECONDS=10
app.USER_AGENT="psi-v10-live-scanner/10.8-layer-runner"


def mem(sym,key): return bool(app.remembered(sym,key))
def ntrue(d): return sum(bool(x) for x in d.values())
def rapid(sym):
    try: return base.metric(sym)
    except Exception: return {"symbol":sym,"score":0.0,"trigger":False}


def evaluate(sym):
    sd=app.structure.get(sym)
    if not sd: return None
    m=app.micro_metrics(sym); an=app.anomaly_state.get(sym,{})
    app.update_signal_memory(sym,sd,m,an); r=rapid(sym)
    price=m.get("last_price") or sd.get("price") or 0.0
    vw=m.get("vwap_60s") or 0.0
    vwap_hold=bool(vw and price>=vw*0.998)
    vwap=bool(m.get("vwap_reclaim") or vwap_hold)

    av={"ANOMALY":mem(sym,"ANOMALY"),"VOLUME":mem(sym,"VOLUME_IMPULSE"),"ACTIVITY":mem(sym,"RELATIVE_ACTIVITY"),"RAPID":bool(r.get("trigger"))}
    fv={"CVD":mem(sym,"CVD_POSITIVE"),"OFI":mem(sym,"RELATIVE_FLOW"),"BUY_DOM":mem(sym,"BUY_DOMINANCE"),"PERSIST":mem(sym,"PERSISTENCE")}
    activity=ntrue(av)>=2; flow=ntrue(fv)>=2
    obi=float(m.get("obi") or 0); ask=float(m.get("ask_depletion") or 0)
    bv={"REL_BOOK":mem(sym,"RELATIVE_BOOK"),"OBI":obi>=0.05,"ASK_THIN":ask>=0 and obi>=0.03}
    book=bool(bv["REL_BOOK"] or (bv["OBI"] and bv["ASK_THIN"]))

    ma=bool(mem(sym,"MA_REGIME") or sd.get("ma_regime"))
    ma_retest=bool(mem(sym,"MA_RETEST") and mem(sym,"MA_SLOPE_UP") and mem(sym,"STRUCTURE_SUPPORT"))
    comp=bool(ma and (mem(sym,"COMPRESSION_NEAR") or mem(sym,"BREAKOUT")))
    cont=bool(ma and mem(sym,"BREAKOUT") and vwap)

    r5=float(r.get("r5") or 0); r15=float(r.get("r15") or 0); rng=float(r.get("range60_pct") or 999); v5=float(r.get("vol_accel_5") or 0)
    runner_base=bool(sd.get("anti_chase") and ma and mem(sym,"BREAKOUT") and vwap and rng<=3.0 and r15>=-0.25)
    runner_reaccel=bool(0.05<=r5<=1.8 and (v5>=1.5 or mem(sym,"RELATIVE_ACTIVITY")) and flow and book)
    runner=bool(runner_base and runner_reaccel)
    setups={"COMPRESSION_BREAKOUT":comp,"MA_RETEST_RECLAIM":ma_retest,"BREAKOUT_RETEST_CONTINUATION":cont,"RUNNER_SECOND_IGNITION":runner}
    structure=any(setups.values()); chase=bool(not sd.get("anti_chase") or runner)

    layers={"ACTIVITY_LAYER":activity,"FLOW_LAYER":flow,"ORDER_BOOK_LAYER":book,"VWAP_LAYER":vwap,"MA_STRUCTURE_LAYER":structure,"ANTI_CHASE_OR_RUNNER_LAYER":chase}
    lc=ntrue(layers)
    exe={"LIVE_MICRO_DATA":bool(m.get("micro_ready")),"TRADE_SEQUENCE_VALID":bool(m.get("sequence_verified")),"BOOK_SEQUENCE_VALID":bool(m.get("book_sequence_verified")),"SPREAD_FILTER":m.get("spread_bps") is not None and m.get("spread_bps")<=app.MAX_SPREAD_BPS,"SLIPPAGE_FILTER":m.get("slippage_bps") is not None and m.get("slippage_bps")<=app.MAX_SLIPPAGE_BPS}
    ec=ntrue(exe); buy=all(layers.values()) and all(exe.values())
    pre=bool(not buy and m.get("micro_ready") and structure and activity and flow and lc>=5)
    state="BUY NOW" if buy else "PRE-IGNITION" if pre else "WATCH" if m.get("micro_ready") and lc>=4 else "REJECT"
    active="RUNNER_SECOND_IGNITION" if runner else "COMPRESSION_BREAKOUT" if comp else "MA_RETEST_RECLAIM" if ma_retest else "BREAKOUT_RETEST_CONTINUATION" if cont else "NO_COMPLETE_STRUCTURE"
    hard=dict(exe); hard["ANTI_CHASE_OR_RUNNER"]=chase
    score=max(0,min(100,(lc/6)*80+(ec/5)*20+min(max(float(r.get("score",0)),0),100)*.04-(8 if sd.get("anti_chase") and not runner else 0)))
    failed_layers=[k for k,v in layers.items() if not v]; failed_hard=[k for k,v in hard.items() if not v]
    row={
        "symbol":sym,"state":state,"score":round(score,2),"price":price,"active_setup":active,
        "decision_model":"ALL_MAJOR_LAYERS_WITH_INTERNAL_VOTING","layer_results":layers,"failed_layers":failed_layers,
        "activity_votes":av,"flow_votes":fv,"book_votes":bv,"structure_setups":setups,
        "runner_second_ignition":runner,"runner_base":runner_base,"runner_reaccel":runner_reaccel,
        "hard_safety_status":{k:("PASS" if v else "FAIL") for k,v in hard.items()},"hard_safety_all_aligned":all(hard.values()),
        "setup_results":{k:{"gates":{"SETUP_STRUCTURE":v},"pass_count":int(v),"total":1,"pass_ratio":1.0 if v else 0.0,"all_aligned":v} for k,v in setups.items()},
        "failed_hard":failed_hard,"failed_setup":failed_layers,"mandatory_all_aligned":buy,
        "mandatory_pass_count":lc+ntrue(hard),"mandatory_total":6+len(hard),"mandatory_pass_ratio":round((lc+ntrue(hard))/(6+len(hard)),4),
        "micro_confirmation_count":lc,"confirmation_count":len(sd.get("structure_confirmations",[]))+lc+ntrue(hard),
        "ma_harmony":sd.get("ma_harmony"),"ma_reclaim_regime":sd.get("ma_reclaim_regime"),"anti_chase":sd.get("anti_chase"),
        "volume_acceleration":sd.get("volume_acceleration"),"volume_acceleration_15m":sd.get("volume_acceleration_15m"),"breakout_distance_pct":sd.get("breakout_distance_pct"),
        "fast_anomaly":an,"rapid_ignition":r,"micro_ready":m.get("micro_ready"),"sequence_verified":m.get("sequence_verified"),"book_sequence_verified":m.get("book_sequence_verified"),
        "cvd_quote_60s":m.get("cvd_quote_60s"),"cvd_acceleration":m.get("cvd_acceleration"),"aggressive_buy_ratio":m.get("aggressive_buy_ratio"),
        "ofi":m.get("ofi"),"ofi_acceleration":m.get("ofi_acceleration"),"ofi_persistence":m.get("ofi_persistence"),"obi":m.get("obi"),"ask_depletion":m.get("ask_depletion"),"bid_depletion":m.get("bid_depletion"),
        "trade_count_60s":m.get("trade_count_60s"),"trade_acceleration":m.get("trade_acceleration"),"trade_size_shift":m.get("trade_size_shift"),"relative_volume_10s":m.get("relative_volume_10s"),"relative_volume_30s":m.get("relative_volume_30s"),
        "relative_ranks":m.get("relative_ranks"),"vwap_60s":m.get("vwap_60s"),"vwap_reclaim":m.get("vwap_reclaim"),"vwap_hold":vwap_hold,"spread_bps":m.get("spread_bps"),"slippage_bps":m.get("slippage_bps"),
        "flow_persistence":m.get("flow_persistence"),"quote_volume_24h":sd.get("quote_volume_24h",0),"updated_ms":app.now_ms(),
    }
    try:
        app.record_diagnostics(sym,active,layers,state,price); app.maybe_record_outcome_candidate(row)
    except Exception: pass
    return row


def prune(sym,t):
    d=hits[sym]
    while d and d[0]<t-PERSIST_WINDOW: d.popleft()
    return d


def tick():
    rows=[]; selected=list(dict.fromkeys(list(app.selected_micro_symbols)+list(v7.rapid_symbols)))
    for sym in selected:
        try: row=app.evaluate_symbol(sym)
        except Exception: continue
        if row: rows.append(row)
    t=time.time(); current=set()
    for row in rows:
        sym=row["symbol"]; current.add(sym); q.latest[sym]=row; raw=row.get("state","REJECT"); micro=bool(row.get("micro_ready")); d=prune(sym,t)
        if micro:
            s.hunt_micro_seen.add(sym)
            if raw in q.QUALIFIER_STATES:
                if not d or t-d[-1]>=5: d.append(t)
                q.locked_until[sym]=max(q.locked_until.get(sym,0),t+q.LOCK_GRACE); s.verified_state[sym]=raw; q.last_raw[sym]=raw; q.streak[sym]=len(d)
                if len(d)>=PERSIST_HITS:
                    pub=dict(row); pub["persistence_samples"]=len(d); pub["persistence_window_seconds"]=PERSIST_WINDOW; pub["hunter_locked"]=True; pub["lane"]="RUNNER" if row.get("runner_second_ignition") else "RAPID" if sym in v7.rapid_symbols else "STABLE"
                    q.stable[sym]=pub; s.stable_until[sym]=t+s.QUALIFIER_DATA_GRACE; s.near_memory.pop(sym,None)
                else: s.near_memory[sym]=s._near_row(row,[f"WAIT_ROLLING_PERSISTENCE_{len(d)}/{PERSIST_HITS}"])
            else:
                q.streak[sym]=len(d); s.verified_state[sym]=raw; q.last_raw[sym]=raw; q.stable.pop(sym,None); s.stable_until.pop(sym,None); s.near_memory[sym]=s._near_row(row)
        else:
            q.streak[sym]=len(d)
            if sym in q.stable and s.stable_until.get(sym,0)<t: q.stable.pop(sym,None); s.stable_until.pop(sym,None)
            s.near_memory[sym]=s._near_row(row,["MICRO_NOT_READY"]+(["ROLLING_PERSISTENCE_PAUSED"] if d else []))
    for sym in list(q.stable):
        if sym not in current and s.stable_until.get(sym,0)<t: q.stable.pop(sym,None); s.stable_until.pop(sym,None)
    for sym in list(q.locked_until):
        if q.locked_until[sym]<t: q.locked_until.pop(sym,None)
    q.near=s.near_diag(10); q.qualifier_cycles+=1
    if len(q.stable)>=v7.TARGET: s.hunt_target_reached=True; s.hunt_completed_at=s.hunt_completed_at or t


async def scan(req):
    try: limit=max(1,min(int(req.query.get("limit",v7.TARGET)),v7.TARGET))
    except ValueError: limit=v7.TARGET
    app.resolve_outcomes(); rows=s.results(limit); c=v7.coverage(); c["decision_engine"]={"version":VERSION,"policy":"ALL_MAJOR_LAYERS_PLUS_EXECUTION_GATES","rolling_persistence_seconds":PERSIST_WINDOW,"rolling_persistence_required_hits":PERSIST_HITS,"rapid_slots":v7.RAPID_MICRO_SLOTS,"rapid_min_hold_seconds":v7.RAPID_MIN_HOLD,"runner_second_ignition":True}
    return app.web.json_response({"ok":True,"scanner":"Ψ-V10.8 Layer Voting + Rolling Persistence + Second Ignition","version":VERSION,"buy_policy":"ALL_MAJOR_LAYERS_ALIGNED_AND_ALL_LIVE_EXECUTION_GATES_PASS","returned":len(rows),"state_counts":dict(Counter(x["state"] for x in rows)),"coverage":c,"results":rows,"near_miss_diagnostics":s.near_diag(10),"ignition_top":base.rank(limit=10),"missed_move_events":list(v7.missed_move_events)[-20:],"generated_ms":q.ms()})


app.evaluate_symbol=evaluate
v7.tick=tick; q.tick=tick; s.tick=tick
app.scan_endpoint=scan; app.ranked_results=s.results

if __name__=="__main__":
    try: asyncio.run(v7.main())
    except KeyboardInterrupt: print("Ψ-V10.8 stopped",flush=True)
