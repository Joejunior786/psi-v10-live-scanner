#!/usr/bin/env python3
"""Scan real Binance Spot order books; not a buy instruction."""
import concurrent.futures,datetime as dt,json,random,time,urllib.parse,urllib.request
from pathlib import Path
URLS=('https://data-api.binance.vision','https://api1.binance.com','https://api-gcp.binance.com')
FOCUS='FARM FIS DEGO MBOX PHA DEXE NFP RSR DCR RAD FIRO UTK VIB DENT CFX IOTX STORJ RAY BNT FLM SUSHI SUPER CELO BLZ CHZ CKB XLM RUNE KAVA SYS RLC MBL ALPINE LAZIO PSG OG CITY PORTO BAR JUV SANTOS ATM ASR ACM LIT LOKA YGG AUCTION GTC FORTH LPT UMA PHB SFP CTSI SLP ALGO AERGO OSMO PIVX NMR FIDA AKT STPT BEL REEF DOCK DATA DF VIDT HIVE STEEM ARDR FTT REI ALPHA CLV ATA TRU TRB TWT MDT VITE PERP NKN DODO WIN KMD AUDIO KDA BAL COMP XVS NEO GAS ONG ONT COTI DUSK IDEX HFT ID EDU TLM OGN C98 ALCX DYM MAV THETA TFUEL GLMR INJ SOLV AXS PYR AVA PUNDIX NULS ARK HBAR API3 CYBER MASK XAI SSV PENDLE ENJ ILV ELF ACE HOOK PORTAL OXT OOKI BICO ALICE CVC RARE CRV LQTY DAR JASMY FIO FET STMX ONE QI QUICK CAKE ALPACA REZ RIF SXT DGB SCRT TIA AEVO TNSR STG BNBX ALLO CHR PYTH IOST NTRN GHST KNC FLOKI ANKR ORCA JTO JUP SCR WIF'.split()
def get(path,timeout=8):
    errs=[]
    for root in URLS:
        try:
            q=urllib.request.Request(root+path,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
            with urllib.request.urlopen(q,timeout=timeout) as f:return json.load(f)
        except Exception as ex:errs.append(type(ex).__name__+':'+str(ex)[:80])
    raise RuntimeError('Binance endpoints failed: '+' | '.join(errs))
def analyze(pair,book,quote_volume):
    asks=sorted((float(p),float(q)) for p,q in book.get('asks',[]) if float(p)>0 and float(q)>0)
    bids=sorted(float(p) for p,q in book.get('bids',[]) if float(p)>0 and float(q)>0)
    if len(asks)<2 or not bids or bids[-1]>asks[0][0]:return None
    best=asks[0][0];spread=(best/bids[-1]-1)*100;cum=0.; gaps=[]
    for (p,q),(hi,_) in zip(asks,asks[1:]):
        cum+=p*q
        if (hi/best-1)*100>100:break
        g=(hi/p-1)*100
        if g>=.05:gaps.append({'start':p,'end':hi,'empty_pct':round(g,6),'end_upside_pct':round((hi/best-1)*100,5),'ask_usdt_before':round(cum,2)})
    if not gaps:return None
    gaps.sort(key=lambda z:z['empty_pct']/(1+z['end_upside_pct']/12),reverse=True)
    ask_depth={str(p):round(sum(price*qty for price,qty in asks if price<=best*(1+p/100)),2) for p in (2,5,10,20)}
    bid_depth={str(p):round(sum(price*qty for price,qty in ((float(a),float(b)) for a,b in book.get('bids',[])) if price>=best*(1-p/100)),2) for p in (2,5,10)}
    return {'ask_depth_usdt':ask_depth,'bid_depth_usdt':bid_depth,'pair':pair,'ask':best,'spread_pct':round(spread,4),'book_levels':len(asks),'visible_coverage_pct':round((asks[-1][0]/best-1)*100,4),'quote_volume_24h_usdt':round(quote_volume,2),'update_id':book.get('lastUpdateId'),**gaps[0],'other_gaps':gaps[1:12]}
def main():
    results={'started_utc':dt.datetime.now(dt.timezone.utc).isoformat(),'exchange':'Binance Spot','books':0,'pairs_checked':0,'candidates':[],'errors':[],'note':'Displayed asks only. Gaps do not guarantee future price movement. Reverify before trading.'}
    try:
        inf=get('/api/v3/exchangeInfo',12)
        tick=get('/api/v3/ticker/24hr',12)
        valid={s['symbol'] for s in inf.get('symbols',[]) if s.get('status')=='TRADING' and s.get('quoteAsset')=='USDT' and s.get('isSpotTradingAllowed',True) and ('SPOT' in s.get('permissions',['SPOT']) or not s.get('permissions'))}
        volume={s.get('symbol'):float(s.get('quoteVolume') or 0) for s in tick if isinstance(s,dict)}
        ranked=sorted(valid,key=lambda s:volume.get(s,0),reverse=True)
        focus=[x+'USDT' for x in FOCUS if x+'USDT' in valid]
        mids=[s for s in ranked[380:940] if s not in focus and not s.endswith('BUSDT') and not s.endswith('BUSD')]
        random.Random(int(time.time()//3600)).shuffle(mids)
        pairs=list(dict.fromkeys(focus+mids[:18]))[:135]
        print('Verified exchangeInfo, Spot USDT pairs selected:',len(pairs),flush=True)
        def run(pair):
            try:
                book=get('/api/v3/depth?'+urllib.parse.urlencode({'symbol':pair,'limit':1000}),10)
                return pair,analyze(pair,book,volume.get(pair,0)),None
            except Exception as e:return pair,None,str(e)[:260]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            for pair,x,e in pool.map(run,pairs):
                results['pairs_checked']+=1
                if e:results['errors'].append({'pair':pair,'message':e})
                else:
                    results['books']+=1
                    if x:results['candidates'].append(x)
    except Exception as e:results['errors'].append({'phase':'exchangeInfo/ticker','message':str(e)[:600]})
    results['candidates'].sort(key=lambda z:z['empty_pct']/(1+z['end_upside_pct']/12),reverse=True)
    results['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
    Path('binance_spot_gap_results.json').write_text(json.dumps(results,indent=2))
    print('RESULTS UTC',results['finished_utc'],'Spot books',results['books'],'pairs checked',results['pairs_checked'],'candidates',len(results['candidates']))
    for i,x in enumerate(results['candidates'][:25],1):
        print(f"{i:02d} {x['pair']:<12} ask={x['ask']:.9g} GAP={x['start']:.9g} -> {x['end']:.9g} EMPTY={x['empty_pct']:.4f}% END=+{x['end_upside_pct']:.2f}% ASK-BEFORE-USD={x['ask_usdt_before']:.0f} coverage=+{x['visible_coverage_pct']:.2f}% spread={x['spread_pct']:.3f}%")
    if results['errors']:print('ERRORS',str(results['errors'][:2])[:1000])
    if not results['books']:print('NO VERIFIED BOOKS; DO NOT USE THIS AS A LIVE BUY LIST')
if __name__=='__main__':main()
