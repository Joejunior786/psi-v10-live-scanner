import asyncio, json, math, os, statistics, time
from collections import defaultdict, deque
import psi_v11_3_5_entry as scientist
try: import psi_v11_entry as research
except Exception: research=None
try: import ignition119_entry as intel119
except Exception: intel119=None
try: import ignition110_entry as ignition13
except Exception: ignition13=None
app,q,scanner=scientist.app,scientist.q,scientist.scanner
VERSION='11.0.3.8-full-universe-monster-radar'
SCAN_S=0.5; BOARD_S=5.0; SAVE_S=30.0; OBS_COOLDOWN=600.0; HORIZON=4*3600.0
STATE=os.environ.get('PSI_MONSTER_RADAR_STATE','/data/psi_monster_radar_state.json')
DEEP_LIMIT=96; MAX_PENDING=2500; MAX_RESOLVED=10000; TARGETS=(5.,10.,15.,20.,30.)
price_hist=defaultdict(lambda:deque(maxlen=900)); rapid_hist=defaultdict(lambda:deque(maxlen=240))
latest={}; pending=[]; resolved=deque(maxlen=MAX_RESOLVED); last_open=defaultdict(float); stats=defaultdict(int)
adapt={'updated':0.0,'status':'WARMING','n':0,'winners20':0,'lift':{}}

def f(v,d=0.0):
    try: x=float(v)
    except (TypeError,ValueError): return d
    return x if math.isfinite(x) else d

def opt(v):
    try: x=float(v)
    except (TypeError,ValueError): return None
    return x if math.isfinite(x) else None

def cl(v,a=0.,b=1.): return max(a,min(b,v))

def px(sym,row=None):
    row=row or (q.latest.get(sym) or {})
    try:
        p=f(app.current_symbol_price(sym))
        if p>0:return p
    except Exception: pass
    return f(row.get('price'))

def layers(row):
    x=row.get('layer_results') or {}
    return sum(bool(v) for v in x.values()) if x else int(f(row.get('layers'),f(row.get('layer_count'))))

def hard(row,key):
    for k in ('hard_safety_status','pinpoint_hard_status'):
        d=row.get(k) or {}
        if key in d: return d[key] is True or str(d[key]).upper()=='PASS'
    return None

def rapid(sym,row):
    r=row.get('rapid_ignition')
    if isinstance(r,dict) and r:return r
    if ignition13:
        try:
            r=ignition13._rapid_metric(sym,row)
            if isinstance(r,dict):return r
        except Exception:pass
    return {}

def radar(sym):
    if not research:return {}
    try:return research.radar_feed.metric(sym) or {}
    except Exception:return {}

def v11(sym):
    if not research:return {}
    try:return research.v11_candidate(sym) or {}
    except Exception:return {}

def intel(sym):
    if not intel119:return {}
    try:return intel119.intelligence(sym) or {}
    except Exception:return {}

def ret(sym,sec):
    h=price_hist.get(sym)
    if not h or len(h)<2:return 0.0
    t,p=h[-1]; old=None
    for ts,op in reversed(h):
        if ts<=t-sec: old=op; break
    if old is None: old=h[0][1]
    return (p/old-1)*100 if p>0 and old>0 else 0.0

def persist(sym,now):
    h=rapid_hist[sym]
    while h and h[0][0]<now-60:h.popleft()
    s=[x for _,x in h]
    return {'peak':max(s) if s else 0.,'g100':sum(x>=100 for x in s),'g130':sum(x>=130 for x in s),'g150':sum(x>=150 for x in s)}

def cheap(sym,row,now):
    rr=rapid(sym,row); rs=f(rr.get('score'),f(row.get('rapid_score'))); rapid_hist[sym].append((now,rs)); ps=persist(sym,now)
    r15,r30,r60,r180=ret(sym,15),ret(sym,30),ret(sym,60),ret(sym,180); btc60,btc180=ret('BTCUSDT',60),ret('BTCUSDT',180)
    buy=f(row.get('aggressive_buy_ratio'),.5); ofi=f(row.get('ofi')); obi=f(row.get('obi'),f(row.get('pinpoint_weighted_obi_l1_l10')))
    vol5=f(rr.get('vol_accel_5')); vol15=f(rr.get('vol_accel_15')); trade5=f(rr.get('trade_accel_5')); burst5=int(f(rr.get('burst_count_5m')))
    act=cl(max(vol5/2.5,vol15/2.2,trade5/2.5,burst5/4, .85 if rr.get('clustered_ignition') else .65 if rr.get('latent_ignition') else 0))
    rp=cl(.35*ps['g100']/4+.30*ps['g130']/3+.35*ps['g150']/2)
    mom=cl(.4*cl((r15+.03)/.45)+.3*cl((r15-.5*r30+.02)/.30)+.3*cl((r60-btc60+.03)/.50))
    vals={'rapid_n':cl(max(rs,ps['peak'])/180),'rp_n':rp,'act_n':act,'radar_n':cl(f(radar(sym).get('score'))/100),'layer_n':cl(layers(row)/6),'tape_n':cl(f(row.get('pinpoint_live_tape_score'))/100),'pump_n':cl(f(row.get('pump_signature_score'))/80),'ign_n':cl(f(row.get('ignition15_score'))/85),'buy_n':cl((buy-.5)/.25),'ofi_n':cl((ofi+.03)/.30),'obi_n':cl((obi+.03)/.38),'mom_n':mom}
    score=100*(.19*vals['rapid_n']+.09*rp+.11*act+.09*vals['radar_n']+.09*vals['layer_n']+.08*vals['tape_n']+.07*vals['pump_n']+.06*vals['ign_n']+.06*vals['buy_n']+.05*vals['ofi_n']+.05*vals['obi_n']+.06*mom)
    return {'cheap':score,'rapid':rs,'peak':ps['peak'],'g100':ps['g100'],'g130':ps['g130'],'g150':ps['g150'],'vol5':vol5,'vol15':vol15,'trade5':trade5,'burst5':burst5,'buy':buy,'ofi':ofi,'obi':obi,'layers':layers(row),'tape':f(row.get('pinpoint_live_tape_score')),'r15':r15,'r60':r60,'r180':r180,'ex60':r60-btc60,'ex180':r180-btc180,**vals}

def deep(sym):
    vv=v11(sym); hz=vv.get('hazard') or {}; ft=vv.get('fatigue') or {}; ii=intel(sym)
    dist=opt(hz.get('distance_pct')); prox=cl(1-max(dist or 99,0)/4) if dist is not None and -1<=dist<=4 else 0
    mt=str(ii.get('mtf_state') or ii.get('mtf_alignment') or ''); mtn=1 if 'ALIGNED' in mt else .65 if 'MIXED-BULL' in mt else .5
    d={'event':f(vv.get('event_score')),'fatigue':f(vv.get('fatigue_score')),'xlead':f(vv.get('xvenue_score')),'pb15':f(hz.get('raw_p_breakout_15m')),'sp20':f(hz.get('raw_p_up20_60m')),'attacks':int(f(ft.get('attacks'))),'dist':dist,'vac':f(ii.get('liquidity_vacuum_score'),50),'askdep':f(ii.get('ask_depletion_30s_pct')),'flow':f(ii.get('flow_divergence_score'),50),'sector':f(ii.get('sector_rs_score'),50),'sf':f(ii.get('spot_futures_score'),50),'mtf':mt or 'UNKNOWN','mtn':mtn,'prox':prox}
    d['deep']=100*(.13*cl(d['event']/100)+.10*cl(d['fatigue']/100)+.11*cl(d['xlead']/100)+.12*cl(d['pb15']/.30)+.09*cl(d['sp20']/.15)+.10*cl(d['vac']/100)+.08*cl(d['flow']/100)+.05*cl(d['sector']/100)+.04*cl(d['sf']/100)+.04*mtn+.04*prox+.05*cl(d['attacks']/5)+.05*cl(max(d['askdep'],0)/20))
    return d

KEYS=('rapid_n','rp_n','act_n','buy_n','ofi_n','obi_n','event_n','fatigue_n','xlead_n','pb15_n','sp20_n','vac_n','flow_n','sector_n','mtn','prox_n','attacks_n','rs_n')
def vec(c,d): return {'rapid_n':c['rapid_n'],'rp_n':c['rp_n'],'act_n':c['act_n'],'buy_n':c['buy_n'],'ofi_n':c['ofi_n'],'obi_n':c['obi_n'],'event_n':cl(d['event']/100),'fatigue_n':cl(d['fatigue']/100),'xlead_n':cl(d['xlead']/100),'pb15_n':cl(d['pb15']/.30),'sp20_n':cl(d['sp20']/.15),'vac_n':cl(d['vac']/100),'flow_n':cl(d['flow']/100),'sector_n':cl(d['sector']/100),'mtn':d['mtn'],'prox_n':d['prox'],'attacks_n':cl(d['attacks']/5),'rs_n':cl((c['ex60']+.03)/.55)}

def refresh_adapt():
    now=time.time()
    if now-adapt['updated']<60:return
    rows=[x for x in resolved if isinstance(x.get('features'),dict)]; wins=[x for x in rows if f(x.get('max_return_pct'))>=20]; lift={}; status='WARMING'
    if len(rows)>=50 and len(wins)>=5:
        status='ACTIVE'
        for k in KEYS: lift[k]=cl(statistics.mean(f(x['features'].get(k),.5) for x in wins)-statistics.mean(f(x['features'].get(k),.5) for x in rows),-.35,.35)
    adapt.update(updated=now,status=status,n=len(rows),winners20=len(wins),lift=lift)

def bonus(v):
    refresh_adapt()
    if adapt['status']!='ACTIVE':return 0
    num=den=0
    for k,l in adapt['lift'].items():
        if l<=0:continue
        w=min(l/.20,1); num+=w*(f(v.get(k),.5)-.5); den+=w
    return cl((num/den*16) if den else 0,-8,8)

def reasons(c,d):
    z=[]
    if c['rapid']>=130 or c['peak']>=150:z.append('RAPID_BURST')
    if c['g130']>=2 or c['g100']>=4:z.append('RAPID_PERSIST')
    if max(c['vol5'],c['vol15'],c['trade5'],c['burst5']/3)>=1.4:z.append('ACTIVITY_ACCEL')
    if c['buy']>=.62:z.append('AGGRESSIVE_BUYS')
    if c['ofi']>=.10:z.append('OFI_POS')
    if c['obi']>=.12:z.append('BOOK_BID_HEAVY')
    if d['vac']>=65 or d['askdep']>=12:z.append('LIQUIDITY_VACUUM')
    if d['flow']>=65:z.append('FLOW_LEAD')
    if d['xlead']>=55:z.append('XVENUE_LEAD')
    if d['event']>=55:z.append('EVENT_CLUSTER')
    if d['fatigue']>=55 or d['attacks']>=3:z.append('RESISTANCE_FATIGUE')
    if d['pb15']>=.18:z.append('BREAKOUT_HAZARD')
    if d['sp20']>=.08:z.append('RIGHT_TAIL_SHADOW')
    if 'ALIGNED' in d['mtf']:z.append('MTF_ALIGNED')
    if c['layers']>=5:z.append('LAYER_CONFLUENCE')
    if c['ex60']>=.15:z.append('BTC_RELATIVE_STRENGTH')
    return z

def empirical(v,target):
    rows=[x for x in resolved if isinstance(x.get('features'),dict)]
    if len(rows)<30:return {'mean':None,'lower':None,'n':len(rows),'neff':0,'status':'WARMING'}
    ss=[]
    for e in rows:
        h=e['features']; sims=[1-abs(f(v.get(k),.5)-f(h.get(k),.5)) for k in KEYS if k in h]
        if len(sims)>=8 and statistics.mean(sims)>=.55:ss.append((statistics.mean(sims)**3,e))
    ss=sorted(ss,key=lambda x:x[0],reverse=True)[:220]; sw=sum(w for w,_ in ss); sw2=sum(w*w for w,_ in ss); ne=(sw*sw/sw2) if sw2 else 0
    if sw<=0 or ne<8:return {'mean':None,'lower':None,'n':len(ss),'neff':ne,'status':'CALIBRATING'}
    broad=(sum(f(e.get('max_return_pct'))>=target for e in rows)+.5)/(len(rows)+1); mean=(sum(w for w,e in ss if f(e.get('max_return_pct'))>=target)+10*broad)/(sw+10); se=math.sqrt(max(mean*(1-mean)/(ne+10),0)); return {'mean':mean,'lower':cl(mean-1.28155*se),'n':len(ss),'neff':ne,'status':'ACTIVE' if ne>=15 else 'CALIBRATING'}

def candidate(sym,row,c,d):
    v=vec(c,d); rs=reasons(c,d); dna=cl(.44*c['cheap']+.56*d['deep']+bonus(v)+min(8,max(0,len(rs)-4)*1.6)-(8 if c['ofi']<-.10 and c['obi']<-.10 else 0),0,100)
    extended=c['r60']>=5 or c['r180']>=9 or (hard(row,'CUMULATIVE_EXTENSION_GUARD') is False and c['r180']>=3)
    state='MONSTER-EXTENDED' if extended else 'MONSTER-HOT' if dna>=78 and len(rs)>=6 else 'MONSTER-IGNITION' if dna>=66 and len(rs)>=4 else 'MONSTER-SEED' if dna>=55 else 'MONSTER-WATCH'
    early=cl(dna-min(30,max(0,c['r60']-2.5)*2.2+max(0,c['r180']-5)*1.3),0,100)
    return {'symbol':sym,'state':state,'dna':dna,'early':early,'price':px(sym,row),'reasons':rs,'rapid':c['rapid'],'peak':c['peak'],'g130':c['g130'],'r15':c['r15'],'r60':c['r60'],'r180':c['r180'],'event':d['event'],'xlead':d['xlead'],'vac':d['vac'],'flow':d['flow'],'pb15':d['pb15'],'sp20':d['sp20'],'layers':c['layers'],'dist':d['dist'],'formal':row.get('formal_state') or row.get('state'),'pp':row.get('pinpoint_state') or 'WATCH','features':v,'p10':empirical(v,10),'p20':empirical(v,20),'p30':empirical(v,30)}

def open_obs(c):
    sym=c['symbol']; now=time.time()
    if c['early']<42 or c['price']<=0 or now-last_open[sym]<OBS_COOLDOWN or any(x.get('symbol')==sym for x in pending):return
    pending.append({'id':f'{sym}:{int(now*1000)}','symbol':sym,'opened':now,'entry':c['price'],'features':dict(c['features']),'dna':c['dna'],'early':c['early'],'state':c['state'],'reasons':list(c['reasons']),'hit_ts':{},'max_return_pct':0.,'max_drawdown_pct':0.}); last_open[sym]=now; stats['opened']+=1
    if len(pending)>MAX_PENDING:del pending[:-MAX_PENDING]

def update_obs():
    now=time.time(); keep=[]
    for e in pending:
        p=px(str(e.get('symbol') or '')); ent=f(e.get('entry'))
        if p<=0 or ent<=0:keep.append(e);continue
        r=(p/ent-1)*100;e['max_return_pct']=max(f(e.get('max_return_pct')),r);e['max_drawdown_pct']=min(f(e.get('max_drawdown_pct')),r)
        for t in TARGETS:
            k=str(int(t))
            if k not in e['hit_ts'] and r>=t:e['hit_ts'][k]=now
        if now-f(e.get('opened'),now)>=HORIZON:e['closed']=now;e['final_return_pct']=r;resolved.append(dict(e));stats['resolved']+=1
        else:keep.append(e)
    pending[:]=keep[-MAX_PENDING:]

def load():
    if not os.path.exists(STATE):return
    try:
        with open(STATE,encoding='utf-8') as fh:d=json.load(fh)
        pending[:]=[x for x in d.get('pending',[]) if isinstance(x,dict)][-MAX_PENDING:];resolved.clear();resolved.extend([x for x in d.get('resolved',[]) if isinstance(x,dict)][-MAX_RESOLVED:])
        for s,t in (d.get('last_open') or {}).items():last_open[str(s)]=f(t)
        stats['state_loaded']=1
    except Exception as e:print(f'Ψ-MONSTER-RADAR LOAD_ERROR {type(e).__name__}: {e}',flush=True)

def save(force=False):
    now=time.time()
    if not force and now-stats['last_save']<SAVE_S:return
    try:
        os.makedirs(os.path.dirname(STATE) or '.',exist_ok=True);tmp=STATE+'.tmp'
        with open(tmp,'w',encoding='utf-8') as fh:json.dump({'version':VERSION,'saved':now,'pending':pending[-MAX_PENDING:],'resolved':list(resolved)[-MAX_RESOLVED:],'last_open':dict(last_open)},fh,separators=(',',':'),ensure_ascii=False)
        os.replace(tmp,STATE);stats['last_save']=now
    except Exception as e:stats['save_errors']+=1;print(f'Ψ-MONSTER-RADAR SAVE_ERROR {type(e).__name__}: {e}',flush=True)

def scan():
    now=time.time(); u=list(getattr(q,'universe',[]) or []); cr=[]
    for s in u:
        row=q.latest.get(s) or {}; p=px(s,row)
        if p>0 and (not price_hist[s] or now-price_hist[s][-1][0]>=.45):price_hist[s].append((now,p))
        c=cheap(s,row,now);cr.append((c['cheap'],s,row,c))
    cr.sort(reverse=True,key=lambda x:x[0]); pool=cr[:DEEP_LIMIT];seen={s for _,s,_,_ in pool}
    for it in cr:
        _,s,_,c=it
        if s not in seen and (c['peak']>=100 or c['radar_n']>=.45 or c['r60']>=.75):pool.append(it);seen.add(s)
    out=[]
    for _,s,row,c in pool:
        ca=candidate(s,row,c,deep(s));latest[s]=ca
        if ca['early']>=38 or ca['dna']>=50 or ca['peak']>=120:out.append(ca);open_obs(ca)
    out.sort(key=lambda x:(x['early'],x['dna'],x['peak']),reverse=True);stats['cycles']+=1;stats['universe']=len(u);stats['deep']=len(pool);stats['cand']=len(out);return out[:12]

async def scan_loop():
    while True:
        await asyncio.sleep(SCAN_S)
        try:latest['_board']=scan();update_obs();save()
        except asyncio.CancelledError:save(True);raise
        except Exception as e:stats['scan_errors']+=1;print(f'Ψ-MONSTER-RADAR SCAN_ERROR {type(e).__name__}: {e}',flush=True)

def pt(p):return '-' if not isinstance(p,dict) or p.get('mean') is None else f"{100*f(p.get('mean')):.1f}%[{100*f(p.get('lower')):.1f}]"
async def board_loop():
    while True:
        await asyncio.sleep(BOARD_S)
        try:
            refresh_adapt();rows=list(latest.get('_board') or []);counts={k:sum(r['state']==k for r in rows) for k in ('MONSTER-HOT','MONSTER-IGNITION','MONSTER-SEED','MONSTER-EXTENDED')}
            print(f"Ψ-MONSTER-RADAR BOARD scanned={stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={stats['deep']} candidates={stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} scan={int(SCAN_S*1000)}ms learning={adapt['status']} obsPending={len(pending)} obsResolved={len(resolved)} PinpointAuthority=YES",flush=True)
            for i,r in enumerate(rows,1):
                ds='-' if r['dist'] is None else f"{r['dist']:+.2f}%";print(f"MR{i:02d}. {r['symbol']:<14} state={r['state']:<17} EARLY={r['early']:5.1f} DNA={r['dna']:5.1f} rapid={r['rapid']:6.1f}/{r['peak']:6.1f} r15={r['r15']:+.2f}% r60={r['r60']:+.2f}% r3m={r['r180']:+.2f}% event={r['event']:4.0f} xv={r['xlead']:4.0f} vac={r['vac']:4.0f} flow={r['flow']:4.0f} pB15={100*r['pb15']:4.1f}% shP20={100*r['sp20']:4.1f}% layers={r['layers']}/6 dist={ds} P20e4h={pt(r['p20'])} formal={r['formal']} pp={r['pp']} why={r['reasons'][:6]}",flush=True)
            if adapt['status']=='ACTIVE':
                lifts=sorted(adapt['lift'].items(),key=lambda x:x[1],reverse=True)[:6];print(f"Ψ-MONSTER-DNA LEARNED n={adapt['n']} winners20={adapt['winners20']} topLift={[(k,round(v,3)) for k,v in lifts]}",flush=True)
            else:print(f"Ψ-MONSTER-DNA LEARNING status=WARMING n={adapt['n']} winners20={adapt['winners20']} target=+20%/4h",flush=True)
        except asyncio.CancelledError:raise
        except Exception as e:stats['board_errors']+=1;print(f'Ψ-MONSTER-RADAR BOARD_ERROR {type(e).__name__}: {e}',flush=True)

for m in (scientist,getattr(scientist,'base',None),scanner):
    try:m.VERSION=VERSION
    except Exception:pass
load()
async def main():
    print('[v11.0.3.8] Ψ FULL-UNIVERSE MONSTER RADAR active — 500ms sweep of every live Binance scanner symbol before Pinpoint qualification; fuses RAPID persistence, volume/trade acceleration, aggressive buys, OFI/OBI, liquidity vacuum/ask depletion, flow divergence, cross-venue lead, resistance fatigue/attacks, breakout hazard, MTF/sector/BTC relative strength, structure confluence and anti-chase timing. Forward +10/+20/+30% learner persists on /data. RESEARCH/RADAR ONLY: Pinpoint remains sole BUY NOW authority.',flush=True)
    await asyncio.gather(scientist.main(),scan_loop(),board_loop())
if __name__=='__main__':asyncio.run(main())
