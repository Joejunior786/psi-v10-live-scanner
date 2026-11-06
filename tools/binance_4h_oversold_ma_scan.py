#!/usr/bin/env python3
"""Binance Spot USDT 4h oversold/MA-touch scanner. Observational, not trading advice."""
import concurrent.futures,datetime as dt,json,math,random,time,urllib.parse,urllib.request
from pathlib import Path
URLS=('https://data-api.binance.vision','https://api1.binance.com','https://api-gcp.binance.com')
FOCUS="""BTC ETH BNB SOL XRP DOGE ADA AVAX LINK DOT UNI SUI TRX LTC BCH HBAR NEAR RENDER FET TAO FIL ICP INJ APT SEI ARB OP ONDO PENDLE ENA WLD TIA STX RUNE JUP AAVE CRV LDO IMX GRT SAND MANA AXS GALA POL FTM SONIC TRB EGLD MOVR BEAMX KAIA IQ BNT ME WAXP JOE CVC NOM IOST BERA KNC QKC ALICE SAGA ALGO VET XTZ ZRO ARKM MANTA CYBER RLC ENS YGG APE WOO CHZ ZIL SKL SSV MASK GLMR XAI ALT API3 DYM STG KAVA XVS DUSK FIDA REZ RIF COTI LRC AR ANKR LPT NMR TLM HIVE ONE ZRX MBL GNO MINA PHA MAGIC RDNT BLUR PYTH BONK PEPE FLOKI TRUMP WIF PENGU PNUT TURBO BABY BIGTIME TNSR JASMY SPELL BAND QNT ID TWT ROSE CKB IMX XEC JTO OGN OSMO EGLD RSR AGLD LISTA PUNDIX MATIC HFT HBAR NEIRO AVA ORCA ASTR SUSHI CELO NEO IOTX ARK SUPER AEVO CTSI ARPA AUCTION ILV RARE DEGO BICO CELR ALCX FLOW COMP BAT ZEC DASH ACH SXP FIO MEME OXT QUICK C98 NKN TRU MTL CHR ALGO ALGO ADA BCH LTC THETA LUNC USTC""".split()
STABLE={"USDC","FDUSD","TUSD","USDP","USDE","USD1","RLUSD","DAI","EUR","EURI","U","PAXG","XAUT","BFUSD","EURI","WBTC","WBETH"}
def get(path,timeout=12):
    errors=[]
    for root in URLS:
        try:
            q=urllib.request.Request(root+path,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
            with urllib.request.urlopen(q,timeout=timeout) as f:return json.load(f)
        except Exception as e:errors.append(str(e)[:70])
    raise RuntimeError(" / ".join(errors))
def sma(values,n):
    return sum(values[-n:])/n if len(values)>=n else None
def ema(values,n):
    if len(values)<n:return None
    a=2/(n+1); e=sum(values[:n])/n
    for v in values[n:]:e+=a*(v-e)
    return e
def rsi_wilder(values,n=14):
    if len(values)<n+2:return None
    ch=[values[i]-values[i-1] for i in range(1,len(values))]
    up=sum(max(v,0) for v in ch[:n])/n
    dn=sum(max(-v,0) for v in ch[:n])/n
    for v in ch[n:]:
        up=(up*(n-1)+max(v,0))/n
        dn=(dn*(n-1)+max(-v,0))/n
    if dn==0:return 100.0 if up>0 else 50.0
    return 100-100/(1+up/dn)
def runpair(pair,prices,volume,nowms):
    try:
        q=urllib.parse.urlencode({'symbol':pair,'interval':'4h','limit':260})
        raw=get('/api/v3/klines?'+q,9)
        closed=[k for k in raw if int(k[6])<nowms]
        if len(closed)<205:return {'pair':pair,'error':'insufficient closed 4h bars'}
        closes=[float(k[4]) for k in closed]
        last=closes[-1];current=float(prices.get(pair,last))
        if current<=0:return {'pair':pair,'error':'invalid price'}
        rsi=rsi_wilder(closes,14)
        vals={'4H SMA59':sma(closes,59),'4H SMA200':sma(closes,200),'4H EMA50':ema(closes,50),'4H EMA200':ema(closes,200)}
        touches={}
        recent=closed[-3:]
        # Include partially forming latest 4h candle for wick-touch observation, without using it in RSI.
        if int(raw[-1][6])>=nowms:recent=recent+[raw[-1]]
        for label,avg in vals.items():
            if not avg:continue
            dist=100*(current/avg-1)
            recent_touch=any(float(k[3])<=avg<=float(k[2]) for k in recent)
            touches[label]={'value':avg,'distance_pct':round(dist,3),'wick_touch':recent_touch,'side':'above/support-side' if current>=avg else 'below/resistance-side'}
        daily={}
        if rsi<=43 or any(abs(z['distance_pct'])<=5 or z['wick_touch'] for z in touches.values()):
            try:
                dq=urllib.parse.urlencode({'symbol':pair,'interval':'1d','limit':230})
                dayraw=get('/api/v3/klines?'+dq,9)
                dcls=[float(k[4]) for k in dayraw if int(k[6])<nowms]
                for n in (59,200):
                    ma=sma(dcls,n)
                    if ma:daily['1D SMA'+str(n)]={'value':ma,'distance_pct':round(100*(current/ma-1),3),'side':'above/support-side' if current>=ma else 'below/resistance-side'}
            except Exception as e:daily={'error':str(e)[:120]}
        active=[{'ma':label,**a} for label,a in touches.items() if abs(a['distance_pct'])<=3 or a['wick_touch']]
        active+= [{'ma':label,**a,'wick_touch':False} for label,a in daily.items() if isinstance(a,dict) and 'distance_pct' in a and abs(a['distance_pct'])<=3]
        active.sort(key=lambda z:(not z.get('wick_touch',False),abs(z['distance_pct'])))
        status='OVERSOLD TOUCH' if rsi<=30 and any((abs(a['distance_pct'])<=2 or a.get('wick_touch')) for a in active) else ('NEAR OVERSOLD TOUCH' if rsi<=38 and active else ('OVERSOLD NO TOUCH' if rsi<=30 else ('NEAR MA, RSI NEUTRAL' if active else 'NONE')))
        atr=[];bb=None
        if len(closes)>=21:
            w=closes[-20:];m=sum(w)/len(w);st=(sum((v-m)**2 for v in w)/len(w))**0.5
            bb=round((4*st/m)*100,3) if m else None
        recent_quote_volume=sum(float(k[7]) for k in closed[-6:])/6
        prior_quote_volume=sum(float(k[7]) for k in closed[-26:-6])/20
        return {'pair':pair,'price':current,'rsi4h':round(rsi,2),'last_closed4h_utc':dt.datetime.fromtimestamp(int(closed[-1][6])/1000,dt.timezone.utc).isoformat(),'quote_volume_24h':volume.get(pair,0),'status':status,'ma_matches':active,'ma_4h':touches,'ma_daily':daily,'bb_width_pct':bb,'recent_4h_volume_vs_prior20':round(recent_quote_volume/prior_quote_volume,2) if prior_quote_volume else None}
    except Exception as e:return {'pair':pair,'error':str(e)[:200]}
def main():
    now=dt.datetime.now(dt.timezone.utc)
    out={'started_utc':now.isoformat(),'kind':'Binance Spot USDT 4h RSI14 Wilder, SMA59 SMA200, EMA50 EMA200, daily SMA59 SMA200','checked':0,'successful':0,'errors':[],'universe_size':0,'strict':[],'near':[],'oversold_without_touch':[],'other_ma':[],'notes':'RSI from last CLOSED 4h candle. MA from closed candles. Current price from ticker. Real calendar-day MAs from 1D bars. Live 4h candle wick used only for recent touch. Not a verified BUY signal.'}
    try:
        info=get('/api/v3/exchangeInfo',12)
        tk=get('/api/v3/ticker/24hr',12)
        pairs={s['symbol'] for s in info['symbols'] if s.get('status')=='TRADING' and s.get('quoteAsset')=='USDT' and s.get('isSpotTradingAllowed',True) and 'SPOT' in s.get('permissions',['SPOT'])}
        vol={s['symbol']:float(s.get('quoteVolume') or 0) for s in tk if isinstance(s,dict) and s.get('symbol')}
        price={s['symbol']:float(s.get('lastPrice') or 0) for s in tk if isinstance(s,dict) and s.get('symbol')}
        focus=[x+'USDT' for x in dict.fromkeys(FOCUS) if x not in STABLE and x+'USDT' in pairs]
        ranked=[p for p in sorted(pairs,key=lambda p:vol.get(p,0),reverse=True) if vol.get(p,0)>=75000 and not p.endswith('BUSDT') and p[:-4] not in STABLE]
        # Higher liquidity and rotating mid caps; include tracked coins first.
        random.Random(int(time.time()//600)).shuffle(ranked[120:]) if False else None
        mids=ranked[100:750];random.Random(int(time.time()//600)).shuffle(mids)
        candidates=list(dict.fromkeys(focus+mids[:45]+ranked[10:60]))[:165]
        out['universe_size']=len(candidates)
        print('Spot USDT symbols selected:',len(candidates),flush=True)
        nowms=int(time.time()*1000)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            for row in pool.map(lambda p:runpair(p,price,vol,nowms),candidates):
                out['checked']+=1
                if 'error' in row:out['errors'].append(row);continue
                out['successful']+=1
                if row['status']=='OVERSOLD TOUCH':out['strict'].append(row)
                elif row['status']=='NEAR OVERSOLD TOUCH':out['near'].append(row)
                elif row['status']=='OVERSOLD NO TOUCH':out['oversold_without_touch'].append(row)
                elif row['status']=='NEAR MA, RSI NEUTRAL':out['other_ma'].append(row)
    except Exception as e:out['errors'].append({'phase':'discovery','error':str(e)[:300]})
    out['strict'].sort(key=lambda z:(z['rsi4h'],min((abs(a['distance_pct']) for a in z['ma_matches']),default=99)))
    out['near'].sort(key=lambda z:(z['rsi4h'],min((abs(a['distance_pct']) for a in z['ma_matches']),default=99)))
    out['oversold_without_touch'].sort(key=lambda z:z['rsi4h'])
    out['other_ma'].sort(key=lambda z:(z['rsi4h'],min((abs(a['distance_pct']) for a in z['ma_matches']),default=99)))
    out['finished_utc']=dt.datetime.now(dt.timezone.utc).isoformat()
    Path('binance_4h_oversold_ma_results.json').write_text(json.dumps(out,indent=2))
    print('FINISHED',out['finished_utc'],'checked',out['checked'],'success',out['successful'],'strict',len(out['strict']),'near',len(out['near']),'oversold_notouch',len(out['oversold_without_touch']),'errors',len(out['errors']),flush=True)
    for k in ('strict','near','oversold_without_touch','other_ma'):
        print('===',k.upper(),'===',flush=True)
        for x in out[k][:12]:
            ma=', '.join(m['ma']+':'+str(m['distance_pct'])+'%' for m in x['ma_matches'][:3])
            print(x['pair'],'price',x['price'],'RSI',x['rsi4h'],ma,'volume',int(x['quote_volume_24h']),flush=True)
if __name__=='__main__':main()
