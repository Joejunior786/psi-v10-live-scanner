#!/usr/bin/env python3
"""Research-only Binance SPOT USDT early-breakout scan. No execution or buy guarantees."""
import concurrent.futures,datetime as dt,json,math,random,time,urllib.parse,urllib.request
from pathlib import Path
ROOTS=["https://data-api.binance.vision","https://api1.binance.com"]
FOCUS="""FLOCK DYDX ZRO SUI SOL ETH BNB BTC XRP AVAX LINK INJ SEI RENDER FET ONDO NEAR APT TRB MOVR TAO RUNE FIL ARB OP ENA PENDLE WLD TIA SAGA STG ALICE KAIA IQ BNT ME WAXP JOE CVC NOM BEAMX IOST BERA KNC QKC EGLD ASTR XVS COOKIE BAND RESOLV NEWT BREV OSMO MANTA RLC YGG APE TNSR BIGTIME TWT PNUT PENGU FIDA REZ RIF PHA JASMY LPT NMR ZEC DASH XTZ HBAR VET ADA DOT DOGE PEPE BONK WIF FLOKI TURBO GALA SAND AXS CHZ COMP UNI AAVE CRV LDO IMX KAVA TRX PYTH JUP ORCA JTO ARKM CYBER MASK BLUR DYM WOO ANKR API3 SPELL RARE C98 PUNDIX SKL GRT ALGO CKB POL PORTAL ALT AEVO HIVE FLUX XAI TLM ACH LQTY OGN RSR ZIL ROSE THETA KERNEL COTI ARPA SSV MTL QNT CTK STRAX HIGH FIO HFT AUCTION SUPER NEO EGLD GNO CELR TRUMP MAGIC GNS ZRX MANA METIS ILV EGLD RAY BANANA DOGE1000 AGLD HOOK LISTA SXT BICO NTRN RED ALLO MIRA VANA PROVE SAHARA INIT OPG HAEDAL SUNDOG ZKJ BARD FARTCOIN MEME PNUT BOME SIGN AIXBT TOWNS""".split()
EXCLUDED={"USDC","USDE","FDUSD","TUSD","DAI","USDP","USD1","RLUSD","EURI","EUR","PAXG","XAUT","BFUSD","U","WBETH","WBTC","USDT"}
def get(path,timeout=10):
    errors=[]
    for root in ROOTS:
        try:
            req=urllib.request.Request(root+path,headers={"User-Agent":"Mozilla/5.0 BinanceSpotResearch","Accept":"application/json"})
            with urllib.request.urlopen(req,timeout=timeout) as r:return json.load(r)
        except Exception as e:errors.append(str(e)[:90])
    raise RuntimeError(" / ".join(errors))
def ema(vals,n):
    if len(vals)<n:return None
    v=sum(vals[:n])/n; k=2/(n+1)
    for x in vals[n:]:v=(1-k)*v+k*x
    return v
def rsi(vals,n=14):
    if len(vals)<n+3:return None
    dif=[vals[i]-vals[i-1] for i in range(1,len(vals))]
    g=sum(max(0,x) for x in dif[:n])/n;l=sum(max(0,-x) for x in dif[:n])/n
    for x in dif[n:]:g=(g*(n-1)+max(0,x))/n;l=(l*(n-1)+max(0,-x))/n
    return 100-100/(1+g/l) if l else (100 if g else 50)
def bb_width(vals):
    v=vals[-20:];avg=sum(v)/20
    return (4*math.sqrt(sum((x-avg)**2 for x in v)/20)/avg)*100 if avg else None
def select(pair,price,vol,change,nowms):
    try:
        raw=get("/api/v3/klines?"+urllib.parse.urlencode({"symbol":pair,"interval":"1h","limit":125}),10)
        bars=[x for x in raw if int(x[6])<nowms]
        if len(bars)<105:return {"pair":pair,"error":"insufficient 1h klines"}
        cs=[float(x[4]) for x in bars]; hs=[float(x[2]) for x in bars];ls=[float(x[3]) for x in bars];vs=[float(x[7]) for x in bars]
        p=price.get(pair,cs[-1])
        if p<=0:return {"pair":pair,"error":"price unavailable"}
        resistance=max(hs[-25:]);support=min(ls[-12:])
        dist=100*(resistance/p-1)
        vbase=sum(vs[-24:-4])/20
        rv=(sum(vs[-3:])/3)/vbase if vbase>0 else 0
        recent=cs[-1]/cs[-4]-1
        atr=sum(max(hs[i]-ls[i],abs(hs[i]-cs[i-1]),abs(ls[i]-cs[i-1])) for i in range(len(cs)-14,len(cs)))/14
        rs=rsi(cs)
        ew=ema(cs,20);e50=ema(cs,50);e200=ema(cs,100)
        recent_bb=bb_width(cs);old_bb=bb_width(cs[-40:-20])
        compression=(recent_bb/old_bb if old_bb and recent_bb else 1.0)
        hlow=min(ls[-6:])>min(ls[-12:-6])
        base_score=(max(0,3.5-abs(dist))*10 +min(5,rv)*8+ (8 if hlow else 0)+(10 if p>=ew else 0) +(8 if compression<0.8 else 0) +(5 if recent>0 else 0))
        if change.get(pair,0)>11:base_score-=22
        if dist>6:base_score-=20
        if rs and (rs>75 or rs<30):base_score-=12
        return {"pair":pair,"price":p,"volume24_usdt":round(vol.get(pair,0),2),"change24_pct":round(change.get(pair,0),2),
                "rsi1h":round(rs,2),"ema20_1h":ew,"ema50_1h":e50,"ema100_1h":e200,
                "resistance24h":resistance,"distance_below_resistance_pct":round(dist,2),
                "recent_support_12h":support,"atr1h":atr,"vol1h_3bar_ratio":round(rv,2),
                "bb_compression_ratio":round(compression,3),"higher_low_6h":hlow,
                "price_change_3h_pct":round(100*recent,2),"initial_score":round(base_score,1)}
    except Exception as e:return {"pair":pair,"error":str(e)[:220]}
def refine(row,nowms):
    pair=row["pair"];p=row["price"]
    try:
        raw=get("/api/v3/klines?"+urllib.parse.urlencode({"symbol":pair,"interval":"15m","limit":110}),10)
        bars=[k for k in raw if int(k[6])<nowms]
        if len(bars)<55:return dict(row,error="insufficient 15m")
        cs=[float(k[4]) for k in bars];vs=[float(k[7]) for k in bars];hs=[float(k[2]) for k in bars];ls=[float(k[3]) for k in bars]
        base=sum(vs[-27:-7])/20
        rv15=(sum(vs[-3:])/3)/base if base else 0
        rsi15=rsi(cs)
        # recent high excludes last completed bar, so newly breaking levels don't erase the signal.
        res15=max(hs[-33:-3]);res15dist=(res15/p-1)*100
        hl15=min(ls[-6:])>min(ls[-12:-6])
        row.update({"rsi15m":round(rsi15,2),"vol15m_3bar_ratio":round(rv15,2),"resistance15m":res15,"distance_to_resistance15_pct":round(res15dist,2),"higher_low15":hl15})
        depth=get("/api/v3/depth?"+urllib.parse.urlencode({"symbol":pair,"limit":500}),10)
        asks=[(float(a),float(b)) for a,b in depth.get("asks",[]) if float(a)>0 and float(b)>0]
        bids=[(float(a),float(b)) for a,b in depth.get("bids",[]) if float(a)>0 and float(b)>0]
        if not asks or not bids:return dict(row,error="empty depth")
        bestask=min(a for a,b in asks);bestbid=max(a for a,b in bids)
        ask2=sum(a*b for a,b in asks if a<=bestask*1.02)
        bid2=sum(a*b for a,b in bids if a>=bestask*0.98)
        nearby=sorted(asks)
        gap=[(a,c,(c/a-1)*100,sum(pp*qq for pp,qq in asks if pp<=a)) for (a,b),(c,d) in zip(nearby,nearby[1:]) if a<=bestask*1.08 and .15<(c/a-1)*100<10]
        gap.sort(key=lambda z:z[2],reverse=True)
        row.update({"bestask":bestask,"bestbid":bestbid,"spread_pct":round((bestask/bestbid-1)*100,3),
                    "asks2_usdt":round(ask2,2),"bids2_usdt":round(bid2,2),"bid_ask_depth2_ratio":round(bid2/ask2,3) if ask2 else None,
                    "top_upward_gap":{"from":gap[0][0],"to":gap[0][1],"gap_pct":round(gap[0][2],3),"asks_before_usdt":round(gap[0][3],2)} if gap else None})
        trades=get("/api/v3/aggTrades?"+urllib.parse.urlencode({"symbol":pair,"limit":250}),10)
        buy=sum(float(t["p"])*float(t["q"]) for t in trades if t.get("m") is False)
        sell=sum(float(t["p"])*float(t["q"]) for t in trades if t.get("m") is True)
        row["recent_aggressive_buy_share"]=round(buy/(buy+sell),3) if buy+sell else None
        row["recent_trades_usdt"]=round(buy+sell,2)
        # The aggTrades sample is the latest 250 aggregated trades, not a fixed-duration flow measurement.
        row["aggressor_sample_count"]=len(trades)
        row["market_data_complete"]=True
        score=row["initial_score"]
        score+=min(rv15,5)*10
        score+=(10 if hl15 else 0)
        score+=12 if -0.8<=res15dist<=1.5 else (5 if -2<=res15dist<=3 else 0)
        score+=min(12,max(-12,(row["bid_ask_depth2_ratio"]-1)*8)) if row.get("bid_ask_depth2_ratio") else 0
        score+=12*(row["recent_aggressive_buy_share"]-.5)*2
        if row["spread_pct"]>1:score-=15
        if row["change24_pct"]>12:score-=22
        row["score"]=round(score,1)
        early=((row["vol15m_3bar_ratio"]>=1.35 or row["vol1h_3bar_ratio"]>=1.4) and row["recent_aggressive_buy_share"]>=.54 and
              row["rsi15m"]>=42 and row["rsi15m"]<=73 and row["rsi1h"]>=40 and row["rsi1h"]<=72 and
              -0.8<=row["distance_to_resistance15_pct"]<=2.5 and -1.2<=row["distance_below_resistance_pct"]<=3.0 and row["change24_pct"]<14)
        row["status"]="EARLY ARMED" if early else ("WATCH" if row["score"]>=50 else "WEAK")
        row["confirm_above"]=round(max(res15,row["resistance24h"]),10)
        row["invalidation_reference"]=round(min(ls[-8:]),10)
        return row
    except Exception as e:return dict(row,error=str(e)[:220],market_data_complete=False)
def main():
    utc=dt.datetime.now(dt.timezone.utc)
    out={"started_utc":utc.isoformat(),"market":"Binance Spot USDT (not futures)","method":"1h resistance/EMA/RSI, 15m volume/higher low, 500-level ask/bid book, last 250 aggregate trades","pairs_checked":0,"stage1_success":0,"stage2_checked":0,"stage2_success":0,"errors":[],"early_armed":[],"watch":[],"other":[],"notes":"Observe only. No BUY NOW. 15m/1h candles use last completed bars. Latest aggregated trades not a duration-defined CVD. Snapshots become stale quickly. UK Spot availability not checked."}
    try:
        inf=get("/api/v3/exchangeInfo",12)
        tks=get("/api/v3/ticker/24hr",12)
        tradable={s["symbol"] for s in inf["symbols"] if s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING" and s.get("isSpotTradingAllowed",True) and ("SPOT" in s.get("permissions",["SPOT"]) or not s.get("permissions"))}
        tks={t["symbol"]:t for t in tks if isinstance(t,dict) and "symbol" in t}
        volume={s:float(tks.get(s,{}).get("quoteVolume") or 0) for s in tradable}
        price={s:float(tks.get(s,{}).get("lastPrice") or 0) for s in tradable}
        change={s:float(tks.get(s,{}).get("priceChangePercent") or 0) for s in tradable}
        allpairs=sorted([s for s in tradable if s[:-4] not in EXCLUDED and not s.endswith("BUSDT") and volume.get(s,0)>110000],key=lambda s:volume[s],reverse=True)
        focus=[s+"USDT" for s in dict.fromkeys(FOCUS) if s+"USDT" in tradable and volume.get(s+"USDT",0)>100000]
        mids=allpairs[100:650];random.Random(int(time.time()//1200)).shuffle(mids)
        pairs=list(dict.fromkeys(focus+allpairs[:65]+mids[:40]))[:190]
        print("BINANCE SPOT PAIRS",len(pairs),flush=True)
        nowms=int(time.time()*1000)
        good=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for row in pool.map(lambda p:select(p,price,volume,change,nowms),pairs):
                out["pairs_checked"]+=1
                if "error" in row:out["errors"].append(row)
                else:
                    out["stage1_success"]+=1;good.append(row)
        good.sort(key=lambda r:r["initial_score"],reverse=True)
        # Reserve multiple candidates from earlier-stage near-resistance pool, not just obvious winners.
        chosen=good[:65]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            for row in pool.map(lambda r:refine(r,int(time.time()*1000)),chosen):
                out["stage2_checked"]+=1
                if "error" in row:out["errors"].append({"pair":row["pair"],"error":row["error"]})
                else:
                    out["stage2_success"]+=1
                    if row["status"]=="EARLY ARMED":out["early_armed"].append(row)
                    elif row["status"]=="WATCH":out["watch"].append(row)
                    else:out["other"].append(row)
    except Exception as e:out["errors"].append({"phase":"discovery","error":str(e)[:260]})
    for name in ("early_armed","watch","other"):out[name].sort(key=lambda r:r.get("score",0),reverse=True)
    out["finished_utc"]=dt.datetime.now(dt.timezone.utc).isoformat()
    Path("binance_spot_early_breakout_results.json").write_text(json.dumps(out,indent=2))
    print("RESULT",out["finished_utc"],"stage1",out["stage1_success"],"/",out["pairs_checked"],"stage2",out["stage2_success"],"/",out["stage2_checked"],"ARMED",len(out["early_armed"]),"WATCH",len(out["watch"]),"ERR",len(out["errors"]),flush=True)
    for name in ("early_armed","watch"):
        print("LANE",name)
        for x in out[name][:15]:
            print(x["pair"],"score",x["score"],"RSI1H",x["rsi1h"],"RV15",x["vol15m_3bar_ratio"],"BUYSHARE",x["recent_aggressive_buy_share"],"distRes",x["distance_below_resistance_pct"],"change24",x["change24_pct"],"volume",int(x["volume24_usdt"]),flush=True)
if __name__=="__main__":main()
