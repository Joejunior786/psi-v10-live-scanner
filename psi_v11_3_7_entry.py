import asyncio, json, math, time
from collections import defaultdict, deque
import aiohttp
import psi_v11_3_6_entry as base

app,q,scanner=base.app,base.q,base.scanner
VERSION='11.0.3.11-resilient-rest-tape'
SHARDS=8
WINDOW=35.0
WS_HEARTBEAT=20.0
WS_RECEIVE_TIMEOUT=45.0
WS_RECONNECT_MAX_DELAY=8.0
MAX_EVENTS=3000
trade_events=defaultdict(lambda:deque(maxlen=MAX_EVENTS))
bbo={}
tape_stats=defaultdict(int)
REST_FALLBACK_INTERVAL=float(__import__("os").environ.get("PSI_TAPE_REST_FALLBACK_INTERVAL","1.0"))
REST_FALLBACK_AGG_PER_CYCLE=int(__import__("os").environ.get("PSI_TAPE_REST_AGG_PER_CYCLE","4"))
REST_FALLBACK_DEPTH_PER_CYCLE=int(__import__("os").environ.get("PSI_TAPE_REST_DEPTH_PER_CYCLE","4"))
REST_FALLBACK_ROTATE_AGG_PER_CYCLE=int(__import__("os").environ.get("PSI_TAPE_REST_ROTATE_AGG_PER_CYCLE","4"))
_rest_agg_last_id=defaultdict(lambda:-1)
_rest_symbol_cursor=0
_rest_universe_cursor=0
_rest_depth_cursor=0
_rest_last_book_snapshot=0.0
_rest_direct_gate=None
_rest_direct_host_idx=0
_rest_fallback_announced=False

def f(v,d=0.0):
    try:x=float(v)
    except (TypeError,ValueError):return d
    return x if math.isfinite(x) else d

def cl(v,a=0.,b=1.):return max(a,min(b,v))

def _window(rows,now,lo,hi=0.0):
    return [x for x in rows if now-lo <= x[0] < now-hi]

def tape_metric(sym):
    now=time.time(); dq=trade_events.get(sym)
    if not dq:return {'ready':False,'age_ms':999999.0}
    while dq and dq[0][0] < now-WINDOW:dq.popleft()
    rows=list(dq)
    if not rows:return {'ready':False,'age_ms':999999.0}
    w1=_window(rows,now,1.0); p4=_window(rows,now,5.0,1.0); w5=_window(rows,now,5.0); w15=_window(rows,now,15.0); w30=_window(rows,now,30.0)
    def sums(xs):
        total=sum(x[2] for x in xs); buy=sum(x[2] for x in xs if x[3]); sell=max(0.0,total-buy)
        return total,buy,sell,len(xs)
    n1,b1,s1,c1=sums(w1); np,bp,sp,cp=sums(p4); n5,b5,s5,c5=sums(w5); n15,b15,s15,c15=sums(w15); n30,b30,s30,c30=sums(w30)
    buy1=b1/n1 if n1>0 else .5; buy5=b5/n5 if n5>0 else .5
    cvd1=(b1-s1)/n1 if n1>0 else 0.; cvd5=(b5-s5)/n5 if n5>0 else 0.
    rate_prev=np/4.0; cnt_prev=cp/4.0
    notional_accel=(n1/max(rate_prev,1e-9)) if n1>0 and rate_prev>0 else (2.0 if n1>0 else 0.0)
    count_accel=(c1/max(cnt_prev,1e-9)) if c1>0 and cnt_prev>0 else (2.0 if c1>0 else 0.0)
    avg1=n1/max(c1,1); avgp=np/max(cp,1)
    avg_shift=(avg1/max(avgp,1e-9)) if c1>0 and cp>0 else (1.0 if c1 else 0.0)
    pv1=(w1[-1][1]/w1[0][1]-1)*100 if len(w1)>=2 and w1[0][1]>0 else 0.
    pv5=(w5[-1][1]/w5[0][1]-1)*100 if len(w5)>=2 and w5[0][1]>0 else 0.
    bt=bbo.get(sym) or {}; age=(now-rows[-1][0])*1000.0; bage=(now-f(bt.get('t'),0))*1000.0 if bt else 999999.
    bid=f(bt.get('bid')); ask=f(bt.get('ask')); bq=f(bt.get('bq')); aq=f(bt.get('aq'))
    spread=((ask-bid)/((ask+bid)/2)*10000) if bid>0 and ask>=bid else 999.
    imb=(bq-aq)/(bq+aq) if bq+aq>0 else 0.
    ready=age<=1500 and len(w5)>=3
    score=100*(.23*cl((buy1-.50)/.25)+.18*cl((cvd1+.02)/.42)+.14*cl((cvd1-cvd5+.02)/.25)+.13*cl((notional_accel-.8)/2.2)+.11*cl((count_accel-.8)/2.2)+.08*cl((avg_shift-.8)/2.0)+.08*cl((imb+.05)/.55)+.05*cl((3.0-spread)/3.0))
    return {'ready':ready,'age_ms':age,'book_age_ms':bage,'score':round(score,2),'buy_ratio_1s':buy1,'buy_ratio_5s':buy5,'cvd_1s':cvd1,'cvd_5s':cvd5,'cvd_accel':cvd1-cvd5,'notional_accel_1s':notional_accel,'trade_count_accel_1s':count_accel,'avg_trade_shift_1s':avg_shift,'price_velocity_1s_pct':pv1,'price_velocity_5s_pct':pv5,'spread_bps':spread,'bbo_imbalance':imb,'trades_1s':c1,'trades_5s':c5,'notional_1s':n1,'notional_5s':n5,'notional_15s':n15,'notional_30s':n30}

async def _shard_loop(idx):
    host_cursor=idx
    reconnects=0
    while True:
        key=f'shard_{idx}_up'
        tape_stats[key]=0
        try:
            while getattr(app,'session',None) is None or not list(getattr(q,'universe',[]) or []):
                await asyncio.sleep(.25)

            syms=sorted(list(q.universe))[idx::SHARDS]
            streams=[]
            for s in syms:
                l=s.lower()
                streams.extend([f'{l}@aggTrade',f'{l}@bookTicker'])
            if not streams:
                await asyncio.sleep(1.0)
                continue

            bases=[]
            for raw in (
                str(getattr(app,'WS_BASE','') or '').rstrip('/'),
                'wss://data-stream.binance.vision',
                'wss://stream.binance.com:443',
                'wss://stream.binance.com:9443',
            ):
                if raw and raw not in bases:
                    bases.append(raw)

            base_url=bases[host_cursor % len(bases)]
            # Use a short /ws handshake then SUBSCRIBE in-frame. This avoids
            # long combined-stream query strings being rejected or timing out
            # at egress/proxy layers.
            url=f"{base_url}/ws"
            tape_stats[f'shard_{idx}_host_idx']=host_cursor % len(bases)
            async with app.session.ws_connect(
                url,
                heartbeat=WS_HEARTBEAT,
                receive_timeout=WS_RECEIVE_TIMEOUT,
                max_msg_size=0,
                timeout=12,
            ) as ws:
                await ws.send_json({"method":"SUBSCRIBE","params":streams,"id":1000+idx})
                tape_stats[key]=1
                tape_stats['connects']+=1
                tape_stats[f'shard_{idx}_last_connect_ms']=int(time.time()*1000)
                tape_stats[f'shard_{idx}_mode']='RAW_SUBSCRIBE'
                print(
                    f'Ψ-MONSTER-TAPE shard={idx+1}/{SHARDS} connected '
                    f'symbols={len(syms)} streams={len(streams)} host={base_url} mode=RAW_SUBSCRIBE',
                    flush=True,
                )

                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        tape_stats[f'shard_{idx}_last_msg_ms']=int(time.time()*1000)
                        try:
                            p=json.loads(msg.data)
                        except Exception:
                            continue
                        if isinstance(p,dict) and 'result' in p and p.get('id') is not None:
                            continue
                        d=p.get('data') if isinstance(p,dict) and isinstance(p.get('data'),dict) else p
                        if not isinstance(d,dict):
                            continue
                        event=str(d.get('e') or '')
                        stream=str(p.get('stream') or '') if isinstance(p,dict) else ''
                        now=time.time()
                        sym=str(d.get('s') or (stream.split('@')[0] if stream else '')).upper()
                        if not sym:
                            continue
                        is_trade=(event=='aggTrade' or stream.endswith('@aggTrade'))
                        is_book=(event=='bookTicker' or stream.endswith('@bookTicker'))
                        if is_trade:
                            price=f(d.get('p'));qty=f(d.get('q'))
                            if price>0 and qty>0:
                                event_ts=f(d.get('T') or d.get('E'))/1000.0
                                stamp=event_ts if event_ts>0 else now
                                if stamp>=now-WINDOW-5:
                                    trade_events[sym].append((stamp,price,price*qty,not bool(d.get('m')),int(f(d.get('E') or d.get('T')))))
                                    tape_stats['trades']+=1
                                    if sym in set(getattr(app,'selected_micro_symbols',[]) or []):
                                        try:
                                            app.process_agg_trade(sym,d)
                                            tape_stats['micro_trade_bridge']+=1
                                        except Exception:
                                            tape_stats['micro_trade_bridge_fail']+=1
                        elif is_book:
                            bbo[sym]={'t':now,'bid':f(d.get('b')),'bq':f(d.get('B')),'ask':f(d.get('a')),'aq':f(d.get('A'))}
                            tape_stats['books']+=1
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        raise RuntimeError(f'websocket_{msg.type.name.lower()}')
                raise RuntimeError('websocket_stream_ended')

        except asyncio.CancelledError:
            tape_stats[key]=0
            raise
        except Exception as e:
            tape_stats['errors']+=1
            tape_stats[key]=0
            reconnects+=1
            tape_stats[f'shard_{idx}_reconnects']=reconnects
            print(
                f'Ψ-MONSTER-TAPE shard={idx+1}/{SHARDS} error={type(e).__name__}:{e}',
                flush=True,
            )
            host_cursor+=1
        finally:
            tape_stats[key]=0
        await asyncio.sleep(min(WS_RECONNECT_MAX_DELAY,1.0+min(reconnects,7)))


async def _direct_rest_json(path, params=None):
    global _rest_direct_gate,_rest_direct_host_idx
    if _rest_direct_gate is None:
        _rest_direct_gate=asyncio.Semaphore(4)
    hosts=[
        "https://api.binance.com",
        "https://data-api.binance.vision",
        "https://api1.binance.com",
        "https://api2.binance.com",
    ]
    last_exc=None
    async with _rest_direct_gate:
        start=_rest_direct_host_idx%len(hosts)
        for off in range(len(hosts)):
            host=hosts[(start+off)%len(hosts)]
            try:
                async with app.session.get(
                    f"{host}{path}",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=5,connect=2),
                ) as resp:
                    body=await resp.text()
                    if resp.status!=200:
                        raise RuntimeError(f"{host} HTTP {resp.status}: {body[:120]}")
                    _rest_direct_host_idx=(start+off)%len(hosts)
                    return json.loads(body)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_exc=exc
                continue
    raise last_exc or RuntimeError("REST fallback exhausted hosts")


def _rest_priority_symbols():
    universe=list(getattr(q,'universe',[]) or [])
    u=set(universe)
    out=[];seen=set()
    def add(s):
        s=str(s or '')
        if s and s in u and s not in seen:
            seen.add(s);out.append(s)
    for s in list(getattr(app,'selected_micro_symbols',[]) or []):
        add(s)
    try:
        for r in list(base.latest.get('_board') or []):
            add(r.get('symbol'))
    except Exception:
        pass
    # Always include BTC/ETH anchors if listed.
    add('BTCUSDT');add('ETHUSDT')
    return out

async def _rest_fetch_agg(sym):
    try:
        rows=await _direct_rest_json('/api/v3/aggTrades',{'symbol':sym,'limit':20})
        if not isinstance(rows,list):
            return 0
        now=time.time();added=0
        rows=sorted((x for x in rows if isinstance(x,dict)),key=lambda x:int(x.get('a',-1)))
        for d in rows:
            try:
                aid=int(d.get('a',-1))
            except Exception:
                aid=-1
            if aid<0 or aid<=_rest_agg_last_id[sym]:
                continue
            _rest_agg_last_id[sym]=aid
            event_ms=int(f(d.get('T')))
            stamp=event_ms/1000.0 if event_ms>0 else 0.0
            if stamp<=0 or stamp<now-WINDOW-5:
                continue
            price=f(d.get('p'));qty=f(d.get('q'))
            if price<=0 or qty<=0:
                continue
            trade_events[sym].append((stamp,price,price*qty,not bool(d.get('m')),event_ms))
            payload=dict(d);payload['E']=event_ms
            if sym in set(getattr(app,'selected_micro_symbols',[]) or []):
                try:
                    app.process_agg_trade(sym,payload)
                    tape_stats['rest_micro_trade_bridge']+=1
                except Exception:
                    tape_stats['rest_micro_trade_bridge_fail']+=1
            added+=1
        tape_stats['rest_trades']+=added
        if added:
            tape_stats['rest_last_trade_ms']=int(time.time()*1000)
        return added
    except asyncio.CancelledError:
        raise
    except Exception as e:
        tape_stats['rest_agg_errors']+=1
        return 0

async def _rest_fetch_depth(sym):
    try:
        d=await _direct_rest_json('/api/v3/depth',{'symbol':sym,'limit':20})
        if not isinstance(d,dict):
            return 0
        try:
            app.process_partial_depth_snapshot(sym,d)
            tape_stats['rest_depth_ok']+=1
            tape_stats['rest_last_depth_ms']=int(time.time()*1000)
            return 1
        except Exception:
            tape_stats['rest_depth_process_fail']+=1
            return 0
    except asyncio.CancelledError:
        raise
    except Exception:
        tape_stats['rest_depth_errors']+=1
        return 0

async def _rest_book_snapshot():
    global _rest_last_book_snapshot
    now=time.time()
    if now-_rest_last_book_snapshot<1.5:
        return 0
    try:
        rows=await _direct_rest_json('/api/v3/ticker/bookTicker')
        if not isinstance(rows,list):
            return 0
        universe=set(getattr(q,'universe',[]) or [])
        n=0
        for d in rows:
            if not isinstance(d,dict):
                continue
            sym=str(d.get('symbol') or '')
            if sym not in universe:
                continue
            bid=f(d.get('bidPrice'));ask=f(d.get('askPrice'))
            if bid<=0 or ask<=0:
                continue
            bbo[sym]={'t':now,'bid':bid,'bq':f(d.get('bidQty')),'ask':ask,'aq':f(d.get('askQty'))}
            mid=(bid+ask)/2.0
            try:
                hist=base.price_hist[sym]
                if not hist or now-hist[-1][0]>=0.75:
                    hist.append((now,mid))
            except Exception:
                pass
            n+=1
        _rest_last_book_snapshot=now
        if n:
            tape_stats['rest_last_book_ms']=int(now*1000)
        tape_stats['rest_books']+=n
        tape_stats['rest_book_snapshots']+=1
        return n
    except asyncio.CancelledError:
        raise
    except Exception:
        tape_stats['rest_book_errors']+=1
        return 0

async def _rest_fallback_loop():
    global _rest_symbol_cursor,_rest_universe_cursor,_rest_depth_cursor,_rest_fallback_announced
    while True:
        try:
            await asyncio.sleep(max(.5,REST_FALLBACK_INTERVAL))
            if getattr(app,'session',None) is None or not list(getattr(q,'universe',[]) or []):
                continue
            ups=sum(int(tape_stats.get(f'shard_{i}_up',0)) for i in range(SHARDS))
            if ups>=SHARDS:
                tape_stats['rest_fallback_active']=0
                continue
            tape_stats['rest_fallback_active']=1
            books_now=await _rest_book_snapshot()
            if books_now and not _rest_fallback_announced:
                _rest_fallback_announced=True
                print(
                    f"Ψ-TAPE FALLBACK LIVE source=BINANCE_REST shards={ups}/{SHARDS} "
                    f"bookSymbols={books_now} executionGates=UNCHANGED",
                    flush=True,
                )

            priority=_rest_priority_symbols()
            universe=list(getattr(q,'universe',[]) or [])
            if not priority and not universe:
                continue

            # Priority symbols receive frequent real aggregate-trade polling.
            stale=[s for s in priority if f(tape_metric(s).get('age_ms'),999999)>1200]
            agg_batch=[]
            if stale:
                start=_rest_symbol_cursor%len(stale)
                take=min(REST_FALLBACK_AGG_PER_CYCLE,len(stale))
                agg_batch.extend(stale[(start+i)%len(stale)] for i in range(take))
                _rest_symbol_cursor=(start+take)%len(stale)

            # Reserve a round-robin quota for the whole universe so lower-ranked
            # symbols still receive periodic real trade telemetry.
            if universe and REST_FALLBACK_ROTATE_AGG_PER_CYCLE>0:
                start=_rest_universe_cursor%len(universe)
                checked=0;added=0
                while checked<len(universe) and added<REST_FALLBACK_ROTATE_AGG_PER_CYCLE:
                    s=universe[(start+checked)%len(universe)]
                    checked+=1
                    if s in agg_batch:
                        continue
                    if f(tape_metric(s).get('age_ms'),999999)<=1200:
                        continue
                    agg_batch.append(s);added+=1
                _rest_universe_cursor=(start+max(checked,1))%len(universe)

            if agg_batch:
                await asyncio.gather(*(_rest_fetch_agg(s) for s in agg_batch),return_exceptions=True)

            selected=[s for s in priority if s in set(getattr(app,'selected_micro_symbols',[]) or [])]
            depth_stale=[]
            nowms=int(time.time()*1000)
            for s in selected:
                try:
                    state=app.ensure_micro_state(s)
                    if nowms-int(state.get('last_book_ms',0) or 0)>3500:
                        depth_stale.append(s)
                except Exception:
                    depth_stale.append(s)
            if depth_stale:
                start=_rest_depth_cursor%len(depth_stale)
                batch=[depth_stale[(start+i)%len(depth_stale)] for i in range(min(REST_FALLBACK_DEPTH_PER_CYCLE,len(depth_stale)))]
                _rest_depth_cursor=(start+len(batch))%len(depth_stale)
                await asyncio.gather(*(_rest_fetch_depth(s) for s in batch),return_exceptions=True)

            if int(time.time())%15==0 or (tape_stats['rest_trades']>0 and tape_stats['rest_trades']<=20):
                print(
                    f"Ψ-TAPE FALLBACK active=YES shards={ups}/{SHARDS} restTrades={tape_stats['rest_trades']} "
                    f"restBooks={tape_stats['rest_books']} restDepth={tape_stats['rest_depth_ok']} "
                    f"aggErr={tape_stats['rest_agg_errors']} bookErr={tape_stats['rest_book_errors']} depthErr={tape_stats['rest_depth_errors']}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            tape_stats['rest_fallback_errors']+=1
            print(f"Ψ-TAPE FALLBACK_ERROR {type(e).__name__}: {e}",flush=True)



_old_px=base.px
def px_v3(sym,row=None):
    bt=bbo.get(sym) or {}
    age=(time.time()-f(bt.get('t'),0))*1000.0 if bt else 999999.0
    bid=f(bt.get('bid'));ask=f(bt.get('ask'))
    if age<=5000 and bid>0 and ask>=bid:
        return (bid+ask)/2.0
    return _old_px(sym,row)
base.px=px_v3

_old_cheap=base.cheap
def cheap_v2(sym,row,now):
    c=_old_cheap(sym,row,now);tm=tape_metric(sym)
    c.update({'tape_event_ready':bool(tm.get('ready')),'event_tape_score':f(tm.get('score')),'event_buy_ratio':f(tm.get('buy_ratio_1s'),.5),'event_cvd':f(tm.get('cvd_1s')),'event_cvd_accel':f(tm.get('cvd_accel')),'event_notional_accel':f(tm.get('notional_accel_1s')),'event_count_accel':f(tm.get('trade_count_accel_1s')),'event_avg_shift':f(tm.get('avg_trade_shift_1s')),'event_spread_bps':f(tm.get('spread_bps'),999),'event_bbo_imb':f(tm.get('bbo_imbalance')),'event_age_ms':f(tm.get('age_ms'),999999)})
    if tm.get('ready'):
        micro=f(tm.get('score'))/100.0
        boost=14.0*max(0.0,micro-.45)
        veto=7.0 if f(tm.get('buy_ratio_1s'),.5)<.37 and f(tm.get('cvd_1s'))<-.20 else 0.0
        c['cheap']=cl(f(c.get('cheap'))+boost-veto,0,100)
    return c
base.cheap=cheap_v2

_old_vec=base.vec
def vec_v2(c,d):
    v=_old_vec(c,d)
    v.update({'event_tape_n':cl(f(c.get('event_tape_score'))/100),'event_buy_n':cl((f(c.get('event_buy_ratio'),.5)-.5)/.25),'event_cvd_n':cl((f(c.get('event_cvd'))+.02)/.42),'event_cvd_accel_n':cl((f(c.get('event_cvd_accel'))+.02)/.25),'event_notional_accel_n':cl((f(c.get('event_notional_accel'))-.8)/2.2),'event_count_accel_n':cl((f(c.get('event_count_accel'))-.8)/2.2),'event_avg_shift_n':cl((f(c.get('event_avg_shift'))-.8)/2.0),'event_bbo_n':cl((f(c.get('event_bbo_imb'))+.05)/.55),'spread_quality_n':cl((3-f(c.get('event_spread_bps'),999))/3)})
    return v
base.vec=vec_v2
base.KEYS=tuple(list(base.KEYS)+['event_tape_n','event_buy_n','event_cvd_n','event_cvd_accel_n','event_notional_accel_n','event_count_accel_n','event_avg_shift_n','event_bbo_n','spread_quality_n'])

_old_reasons=base.reasons
def reasons_v2(c,d):
    z=list(_old_reasons(c,d))
    if c.get('tape_event_ready'):
        if f(c.get('event_tape_score'))>=68:z.append('EVENT_TAPE_STRONG')
        if f(c.get('event_buy_ratio'))>=.64:z.append('AGGTRADE_BUY_BURST')
        if f(c.get('event_cvd'))>=.20 and f(c.get('event_cvd_accel'))>=.06:z.append('CVD_IMPULSE')
        if f(c.get('event_notional_accel'))>=1.8:z.append('NOTIONAL_ACCEL')
        if f(c.get('event_count_accel'))>=1.8:z.append('TRADE_COUNT_ACCEL')
        if f(c.get('event_avg_shift'))>=1.6:z.append('AVG_TRADE_SIZE_EXPANSION')
        if f(c.get('event_bbo_imb'))>=.18:z.append('BBO_BID_PRESSURE')
        if f(c.get('event_spread_bps'),999)<=2.0:z.append('TIGHT_SPREAD')
    return list(dict.fromkeys(z))
base.reasons=reasons_v2

_old_candidate=base.candidate
def candidate_v2(sym,row,c,d):
    out=_old_candidate(sym,row,c,d)
    out.update({'eventTape':f(c.get('event_tape_score')),'buy1s':f(c.get('event_buy_ratio'),.5),'cvd1s':f(c.get('event_cvd')),'cvdA':f(c.get('event_cvd_accel')),'notionalA':f(c.get('event_notional_accel')),'countA':f(c.get('event_count_accel')),'avgSizeA':f(c.get('event_avg_shift')),'spreadBps':f(c.get('event_spread_bps'),999),'bbo':f(c.get('event_bbo_imb')),'tapeReady':bool(c.get('tape_event_ready'))})
    return out
base.candidate=candidate_v2

def _pt(p):return '-' if not isinstance(p,dict) or p.get('mean') is None else f"{100*f(p.get('mean')):.1f}%[{100*f(p.get('lower')):.1f}]"

async def board_loop_v2():
    while True:
        await asyncio.sleep(base.BOARD_S)
        try:
            base.refresh_adapt();rows=list(base.latest.get('_board') or []);counts={k:sum(r['state']==k for r in rows) for k in ('MONSTER-HOT','MONSTER-IGNITION','MONSTER-SEED','MONSTER-EXTENDED')}
            ups=sum(int(tape_stats.get(f'shard_{i}_up',0)) for i in range(SHARDS))
            ready=sum(1 for s in list(getattr(q,'universe',[]) or []) if tape_metric(s).get('ready'))
            print(f"Ψ-MONSTER-RADAR BOARD scanned={base.stats['universe']}/{len(getattr(q,'universe',[]) or [])} deep={base.stats['deep']} candidates={base.stats['cand']} hot={counts['MONSTER-HOT']} ignition={counts['MONSTER-IGNITION']} seed={counts['MONSTER-SEED']} extended={counts['MONSTER-EXTENDED']} scan={int(base.SCAN_S*1000)}ms tape={ready}/{len(getattr(q,'universe',[]) or [])} shards={ups}/{SHARDS} trades={tape_stats['trades']} books={tape_stats['books']} learning={base.adapt['status']} obsPending={len(base.pending)} obsResolved={len(base.resolved)} PinpointAuthority=YES",flush=True)
            for i,r in enumerate(rows,1):
                ds='-' if r['dist'] is None else f"{r['dist']:+.2f}%"
                print(f"MR{i:02d}. {r['symbol']:<14} state={r['state']:<17} EARLY={r['early']:5.1f} DNA={r['dna']:5.1f} rapid={r['rapid']:6.1f}/{r['peak']:6.1f} tape={r.get('eventTape',0):4.0f} buy1={100*r.get('buy1s',.5):4.0f}% cvd1={r.get('cvd1s',0):+.2f} nA={r.get('notionalA',0):.1f}x cA={r.get('countA',0):.1f}x sizeA={r.get('avgSizeA',0):.1f}x spr={r.get('spreadBps',999):.2f}bp bbo={r.get('bbo',0):+.2f} r15={r['r15']:+.2f}% r60={r['r60']:+.2f}% event={r['event']:4.0f} xv={r['xlead']:4.0f} vac={r['vac']:4.0f} pB15={100*r['pb15']:4.1f}% shP20={100*r['sp20']:4.1f}% layers={r['layers']}/6 dist={ds} P20e4h={_pt(r['p20'])} formal={r['formal']} pp={r['pp']} why={r['reasons'][:8]}",flush=True)
            if base.adapt['status']=='ACTIVE':
                lifts=sorted(base.adapt['lift'].items(),key=lambda x:x[1],reverse=True)[:8];print(f"Ψ-MONSTER-DNA LEARNED n={base.adapt['n']} winners20={base.adapt['winners20']} topLift={[(k,round(v,3)) for k,v in lifts]}",flush=True)
            else:print(f"Ψ-MONSTER-DNA LEARNING status=WARMING n={base.adapt['n']} winners20={base.adapt['winners20']} target=+20%/4h",flush=True)
        except asyncio.CancelledError:raise
        except Exception as e:
            base.stats['board_errors']+=1;print(f'Ψ-MONSTER-RADAR BOARD_ERROR {type(e).__name__}: {e}',flush=True)
base.board_loop=board_loop_v2

for m in (base,getattr(base,'scientist',None),scanner):
    try:m.VERSION=VERSION
    except Exception:pass

async def main():
    print('[v11.0.3.9] Ψ EVENT-TAPE MONSTER RADAR active — full-universe Binance aggTrade + bookTicker WebSocket shards add real-time aggressive-buy ratio, CVD impulse/acceleration, notional acceleration, trade-count acceleration, average-trade-size expansion, BBO imbalance and spread to the 500ms Monster Radar. Top candidates still receive deep L1-L20/vacuum, cross-venue, flow, MTF and structure enrichment. Research/radar only; Pinpoint remains sole BUY NOW authority.',flush=True)
    await asyncio.gather(base.main(),_rest_fallback_loop(),*[_shard_loop(i) for i in range(SHARDS)])

if __name__=='__main__':asyncio.run(main())
