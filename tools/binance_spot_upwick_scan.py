#!/usr/bin/env python3
"""Binance Spot early upside-wick anomaly research, no prediction or execution."""
import concurrent.futures, datetime as dt, json, math, random, time, urllib.request, urllib.parse
from pathlib import Path
ROOTS=("https://data-api.binance.vision","https://api1.binance.com","https://api-gcp.binance.com")
PRIOR=set()
FOCUS="""GAS KDA STORJ REQ CTK IDEX ALPACA CVX UMA BICO POL SXP HFT NKN KAVA ONG ONT QI OSMO LUNA HIVE NEO ARK ALICE CTSI SUPER NTRN ELF LRC STG FET DUSK ARPA RAD DATA HIGH CVC TRU API3 ARDR ACE BANANA PHA FIRO LIT FIDA KSM BNT BAND MAV DYM QKC AUCTION FIO SUN VET QNT ONE JOE MAGIC OGN MANTA CELR FLUX HBAR AR XAI AEVO LPT TLM WOO IOST ALGO SEI SUI LINK AVAX JASMY MOVE ALLO ENS STX PUNDIX LSK ASTER MBOX CYBER YGG APE RARE FLOW RIF GALA ARKM ORCA WAXP SPELL DEGO BEL MINA XVS COMP BCH RLC AAVE OXT ZEN ZEC TNSR TRB BEAMX KAIA IQ ME BERA ASTR SSV NEAR REZ INJ FIL ONDO HYPE PENGU WLD MEME AGLD ETHFI SUSHI PENDLE PYTH JUP JTO CAKE LDO ILV DODO INJ COTI METIS STMX DENT STPT PIVX DGB RUNE WIF BONK PEPE TURBO BABY TWT DEXE MASK RSR RED SANTOS PORTAL PHA KERNEL SAHARA INIT BREV NOM WLD BOME ACT NEIRO FARTCOIN VANA PROVE ROBO AIXBT FLM GNS RVN LUNC GNO XEC RAY GMT NFP NULS NMR ICP EGLD BANANAS31 XTZ ROSE ENA LPT NTRN SOLV ORCA TIA TAO ENJ CHZ OCEAN ZIL THETA SAND GRT AXS APT ADA DOT SOL BNB BTC ETH DOGE XRP TRX LTC 1000CHEEMS MOVR SNX RENDER CHR TUT IOTX CKB ASTR GALA SFP QKC ALT TLM SXT TWT IMX SUPER DYM MAV LRC OSMO GLMR""".split()
EXCLUDE={"USDC","USDE","FDUSD","TUSD","DAI","USDP","USD1","RLUSD","EURI","EUR","PAXG","XAUT","BFUSD","U","WBETH","WBTC","USDT","RWUSD"}
def get(path,timeout=10):
    errors=[]
    for root in ROOTS:
        try:
            q=urllib.request.Request(root+path,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
            with urllib.request.urlopen(q,timeout=timeout) as f:return json.load(f)
        except Exception as e:errors.append(str(e)[:90])
    raise RuntimeError(" | ".join(errors))
def av(v):return sum(v)/len(v) if v else 0
def ema(v,n):
    if len(v)<n:return None
    e=av(v[:n]);a=2/(n+1)
    for x in v[n:]:e+=a*(x-e)
    return e
def rsi(v,n=14):
    if len(v)<n+2:return None
    changes=[v[i]-v[i-1] for i in range(1,len(v))]
    u=av([max(x,0) for x in changes[:n]]);d=av([max(-x,0) for x in changes[:n]])
    for x in changes[n:]:
        u=(u*(n-1)+max(x,0))/n;d=(d*(n-1)+max(-x,0))/n
    return 100-100/(1+u/d) if d else (100 if u else 50)
def bbwidth(v,n=20):
    w=v[-n:];m=av(w)
    return 4*(av([(x-m)**2 for x in w])**.5)/m*100 if m else 0
def closed_bars(symbol,interval,n,now):
    raw=get("/api/v3/klines?"+urllib.parse.urlencode({"symbol":symbol,"interval":interval,"limit":n}),10)
    return [k for k in raw if int(k[6])<now]
def first(pair,prices,vol,change,now):
    try:
        bars=closed_bars(pair,"5m",125,now)
        if len(bars)<100:return {"pair":pair,"error":"incomplete 5m"}
        c=[float(k[4]) for k in bars];h=[float(k[2]) for k in bars];l=[float(k[3]) for k in bars];v=[float(k[7]) for k in bars]
        p=prices.get(pair,0)
        if not p:return {"pair":pair,"error":"no ticker"}
        r=rsi(c);last3=av(v[-3:]);base=av(v[-33:-3]);rv=last3/base if base else 0
        previoushigh=max(h[-39:-3]);distance=(previoushigh/p-1)*100
        comp=bbwidth(c)/bbwidth(c[-60:-20]) if bbwidth(c[-60:-20]) else 1
        higher=min(l[-6:])>min(l[-12:-6])
        strong=48<=r<=72
        score=min(rv,5)*11+(12 if strong else 0)+(10 if higher else 0)+(13 if comp<.7 else 0)
        score+=max(0,3-abs(distance))*8
        if p>=ema(c,20):score+=8
        if change.get(pair,0)>12:score-=30
        if distance>6 or distance<-3:score-=14
        return {"pair":pair,"price":p,"change24_pct":round(change.get(pair,0),2),"volume24_usdt":round(vol.get(pair,0),2),
                "rsi5m":round(r,2),"rvol5m":round(rv,2),"5m_quote_usdt_last3":round(sum(v[-3:]),1),
                "previous_5m_resistance":previoushigh,"distance_to_5m_resistance_pct":round(distance,2),
                "higher_low5m":higher,"bb_compression_ratio":round(comp,3),"ema20_5m":ema(c,20),
                "initial_score":round(score,1),"last_closed5m_utc":dt.datetime.fromtimestamp(int(bars[-1][6])/1000,dt.timezone.utc).isoformat()}
    except Exception as e:return {"pair":pair,"error":str(e)[:170]}
def second(row,now):
    pair=row["pair"];p=row["price"]
    try:
        m1=closed_bars(pair,"1m",92,now)
        m15=closed_bars(pair,"15m",85,now)
        if len(m1)<65 or len(m15)<45:return dict(row,error="not enough 1m/15m candles")
        c1=[float(k[4]) for k in m1];l1=[float(k[3]) for k in m1];h1=[float(k[2]) for k in m1];v1=[float(k[7]) for k in m1]
        c15=[float(k[4]) for k in m15];v15=[float(k[7]) for k in m15]
        vol1=sum(v1[-3:]); rvol1=av(v1[-3:])/av(v1[-33:-3]) if av(v1[-33:-3]) else 0
        rvol15=av(v15[-3:])/av(v15[-23:-3]) if av(v15[-23:-3]) else 0
        resistance1=max(h1[-34:-4]);dist1=(resistance1/p-1)*100
        r15=rsi(c15);highlow1=min(l1[-8:])>min(l1[-16:-8])
        q=urllib.parse.urlencode({"symbol":pair,"limit":500})
        depth=get("/api/v3/depth?"+q,10)
        asks=sorted((float(a),float(b)) for a,b in depth.get("asks",[]) if float(a)>0 and float(b)>0)
        bids=sorted((float(a),float(b)) for a,b in depth.get("bids",[]) if float(a)>0 and float(b)>0)
        if not asks or not bids:return dict(row,error="empty spot depth")
        ask=asks[0][0];bid=bids[-1][0]
        def asksTo(pct):return sum(a*b for a,b in asks if a<=ask*(1+pct/100))
        def bidsTo(pct):return sum(a*b for a,b in bids if a>=bid*(1-pct/100))
        a1=asksTo(1);a2=asksTo(2);a5=asksTo(5);b1=bidsTo(1)
        gaps=[];cum=0.
        for (a,q),(upper,q2) in zip(asks,asks[1:]):
            cum+=a*q
            if a>ask*1.065:break
            g=(upper/a-1)*100
            if g>=.12:
                gaps.append({"from":a,"to":upper,"empty_pct":round(g,3),
                            "start_above_ask_pct":round((a/ask-1)*100,2),"asks_before_usdt":round(cum,2)})
        gaps.sort(key=lambda x:x["empty_pct"]/(1+x["start_above_ask_pct"]/8),reverse=True)
        trades=get("/api/v3/aggTrades?"+urllib.parse.urlencode({"symbol":pair,"limit":500}),10)
        buy=sum(float(t["p"])*float(t["q"]) for t in trades if t.get("m")==False)
        sell=sum(float(t["p"])*float(t["q"]) for t in trades if t.get("m")==True)
        sample_buy_share=buy/(buy+sell) if buy+sell else None
        recent=[t for t in trades if int(t.get("T",0))>=now-300000 and int(t.get("T",0))<=now+10000]
        freshbuy=sum(float(t["p"])*float(t["q"]) for t in recent if t.get("m")==False)
        freshsell=sum(float(t["p"])*float(t["q"]) for t in recent if t.get("m")==True)
        recent_usdt=freshbuy+freshsell
        age=(now-int(trades[-1]["T"]))/1000 if trades else None
        # Fixed 5-minute buy share is usable only with minimum trades, value and recency.
        buyshare=freshbuy/recent_usdt if len(recent)>=8 and recent_usdt>=500 and age is not None and age<120 else None
        trade_span=(int(trades[-1]["T"])-int(trades[0]["T"]))/1000 if len(trades)>1 else 0
        ratio=b1/a1 if a1 else 0
        r1=rsi(c1)
        row.update({"rsi1m":round(r1,2),"rsi15m":round(r15,2),"rvol1m":round(rvol1,2),"rvol15m":round(rvol15,2),
                    "last3_1m_quote_usdt":round(vol1,2),"distance_to_1m_resistance_pct":round(dist1,2),
                    "resistance1m":resistance1,"higher_low1m":highlow1,
                    "best_ask":ask,"best_bid":bid,"spread_pct":round((ask/bid-1)*100,3),
                    "ask1_usdt":round(a1,2),"ask2_usdt":round(a2,2),"ask5_usdt":round(a5,2),
                    "bid1_usdt":round(b1,2),"bid_ask1_ratio":round(ratio,2),
                    "ask2_over_last3_1m_traded_value":round(a2/vol1,2) if vol1 else None,
                    "top_nearby_gap":gaps[0] if gaps else None,"other_nearby_gaps":gaps[1:3],
                    "aggressive_buy_share":round(buyshare,3) if buyshare is not None else None,
                    "historical_500_trade_buy_share":round(sample_buy_share,3) if sample_buy_share is not None else None,
                    "recent_5m_aggregated_trade_count":len(recent),
                    "recent_5m_aggregated_trades_usdt":round(recent_usdt,2),
                    "latest_trade_age_seconds":round(age,2) if age is not None else None,
                    "aggressive_trades_usdt":round(buy+sell,2),
                    "aggressive_trade_sample_span_seconds":round(trade_span,1),
                    "aggressive_trade_sample_count":len(trades)})
        # Read 4h from completed candles (deliberately optional, not required for buy classification)
        try:
            bars4=closed_bars(pair,"4h",125,now);c4=[float(k[4]) for k in bars4]
            row["rsi4h"]=round(rsi(c4),2) if len(c4)>=35 else None
            row["above_4h_ema50"]=p>=ema(c4,50) if len(c4)>=55 else None
        except Exception as e:row["4h_error"]=str(e)[:80]
        score=row["initial_score"]+min(rvol1,5)*9+min(rvol15,4)*5
        score+=12 if highlow1 else 0
        score+=15 if buyshare is not None and buyshare>=.6 else (5 if buyshare is not None and buyshare>=.54 else -12)
        score+=12 if ratio>=1.4 else (6 if ratio>=1.05 else -5)
        if gaps and gaps[0]["start_above_ask_pct"]<=3 and gaps[0]["empty_pct"]>=.35 and gaps[0]["asks_before_usdt"]<=35000:score+=13
        if a2<20000 and row["volume24_usdt"]>250000:score+=8
        if 40<=r15<=68:score+=7
        if -0.8<=dist1<=1.8:score+=12
        if row["change24_pct"]>11 or r15>74 or row["rsi5m"]>76:score-=22
        if row["spread_pct"]>0.75:score-=20
        row["wick_score"]=round(score,1)
        ignition=(rvol1>=1.75 and rvol15>=1.0 and buyshare is not None and buyshare>=.58
                  and (ratio>=1 or (gaps and gaps[0]["empty_pct"]>=.4)) and
                  -1<=dist1<=2.8 and -1<=row["distance_to_5m_resistance_pct"]<=3
                  and 43<=r15<=73 and row["change24_pct"]<12 and row["spread_pct"]<.75
                  and vol1>=1250)
        setup=((rvol1>=1.4 or row["rvol5m"]>=1.7) and buyshare is not None and buyshare>=.54
               and -2<=dist1<=3 and ratio>=.75 and row["change24_pct"]<13)
        row["status"]="IGNITION" if ignition else ("EARLY WATCH" if setup else "MONITOR")
        row["breakout_reference"]=max(resistance1,row["previous_5m_resistance"])
        row["invalidation_reference"]=min(l1[-9:])
        # A separate stricter quality assessment: avoid promoting 1-minute spikes alone.
        enough_buying=(buyshare is not None and len(recent)>=20 and recent_usdt>=2500
                       and age is not None and -6<=age<=40 and buyshare>=.60)
        volume_aligned=((rvol1>=1.25 and row["rvol5m"]>=1.45)
                        or (row["rvol5m"]>=2.0 and rvol15>=1.15)
                        or (rvol1>=2.0 and rvol15>=1.35))
        near_break=(-1<=dist1<=2.0 and -1<=row["distance_to_5m_resistance_pct"]<=2.5)
        no_chase=(row["change24_pct"]<10 and row["rsi5m"]<74 and r15<72)
        structure=(highlow1 or row["higher_low5m"])
        demand=(ratio>=1.1)
        healthy_spread=(row["spread_pct"]<=.35)
        sample_quality=(len(recent)>=20 and recent_usdt>=2500 and age is not None and -6<=age<=40)
        row["quality_checks"]={"recent_sample":sample_quality,"buyers_60pct":enough_buying,
                "aligned_multitimeframe_volume":volume_aligned,"near_breakout":near_break,
                "not_overextended":no_chase,"higher_low":structure,
                "bids_gt_asks":demand,"spread_ok":healthy_spread}
        row["quality_reasons_missing"]=[k for k,v in row["quality_checks"].items() if not v]
        row["quality_score"]=round(
            (23 if enough_buying else (7 if sample_quality else -10))
            +(24 if volume_aligned else 0)+(15 if near_break else 0)
            +(10 if no_chase else -10)+(8 if structure else 0)
            +(8 if demand else 0)+(7 if healthy_spread else -10)
            +(5 if row["bb_compression_ratio"]<=.85 else 0)
            +(5 if row.get("above_4h_ema50") else 0),1)
        row["qualified_preignition"]=all(row["quality_checks"].values())
        row["quality_tier"]="STRICT CANDIDATE" if row["qualified_preignition"] else (
            "PARTIAL WATCH" if enough_buying and near_break and healthy_spread and
            (volume_aligned or demand) and no_chase else "UNCONFIRMED")
        return row
    except Exception as e:return dict(row,error=str(e)[:220])
def second_snapshot(row,now):
    """Independent Binance depth/trade sample to check consistency; cancellations are not fills."""
    try:
        pair=row["pair"];old=row["best_ask"]
        book=get("/api/v3/depth?"+urllib.parse.urlencode({"symbol":pair,"limit":500}),10)
        asks=sorted((float(p),float(q)) for p,q in book.get("asks",[]) if float(p)>0 and float(q)>0)
        bids=sorted((float(p),float(q)) for p,q in book.get("bids",[]) if float(p)>0 and float(q)>0)
        if not asks or not bids:return dict(row,verification_error="missing second book")
        nextask=asks[0][0];nextbid=bids[-1][0]
        anchoredask1=sum(p*q for p,q in asks if p<=old*1.01)
        anchoredbid1=sum(p*q for p,q in bids if p>=old*.99)
        oldasks=row["ask1_usdt"]
        delta=round(100*(anchoredask1/oldasks-1),2) if oldasks>0 else None
        tt=get("/api/v3/aggTrades?"+urllib.parse.urlencode({"symbol":pair,"limit":500}),10)
        recent=[t for t in tt if now-300000<=int(t.get("T",0))<=now+10000]
        buy=sum(float(t["p"])*float(t["q"]) for t in recent if t.get("m")==False)
        sell=sum(float(t["p"])*float(t["q"]) for t in recent if t.get("m")==True)
        share=buy/(buy+sell) if buy+sell>0 else None
        age=(now-int(tt[-1]["T"]))/1000 if tt else None
        good=(share is not None and share>=.58 and len(recent)>=20 and buy+sell>=2500
              and age is not None and -6<=age<=45
              and nextask>=old*.995 and nextask<=old*1.035)
        row.update({"second_book_best_ask":nextask,"second_book_ask1_anchored_usdt":round(anchoredask1,2),
          "second_book_bid1_anchored_usdt":round(anchoredbid1,2),
          "ask1_depth_change_pct":delta,"second_5m_buy_share":round(share,3) if share is not None else None,
          "second_5m_trade_count":len(recent),"second_5m_trade_usdt":round(buy+sell,2),
          "second_sample_latest_age_seconds":round(age,1) if age is not None else None,
          "second_sample_buyers_valid":good,
          "potential_ask_depletion":bool(delta is not None and delta<=-12 and nextask>=old*.995)})
        row["repeat_check"]="PERSISTENT" if good else "NOT_CONFIRMED"
        # Depth depletion is descriptive, NOT proof of market buys.
        return row
    except Exception as e:return dict(row,verification_error=str(e)[:180],repeat_check="NOT_CONFIRMED")
def main():
    out={"started_utc":dt.datetime.now(dt.timezone.utc).isoformat(),"exchange":"Binance Spot USDT",
         "purpose":"Detect early upside-wick candidates with 5m 1m and 15m volume, 4h RSI, bid/ask depth and sampled aggressor trades",
         "pairs_checked":0,"stage1":0,"deep_checked":0,"deep_success":0,"repeat_checked":0,"errors":[],"ignition":[],"early_watch":[],"monitor":[],
         "notes":"Research-only, no BUY NOW. Two depth samples are not proof of fills. Trade sample is capped at latest 500 aggregate trades and five minutes; high-volume pairs can be truncated. UK Spot access is not verified."}
    try:
        info=get("/api/v3/exchangeInfo",12)
        tk=get("/api/v3/ticker/24hr",12)
        valid={s["symbol"] for s in info["symbols"] if s.get("status")=="TRADING" and s.get("quoteAsset")=="USDT" and s.get("isSpotTradingAllowed",True) and ("SPOT" in s.get("permissions",["SPOT"]) or not s.get("permissions"))}
        tick={x["symbol"]:x for x in tk if isinstance(x,dict) and "symbol" in x}
        vol={s:float(tick.get(s,{}).get("quoteVolume") or 0) for s in valid}
        price={s:float(tick.get(s,{}).get("lastPrice") or 0) for s in valid}
        change={s:float(tick.get(s,{}).get("priceChangePercent") or 0) for s in valid}
        ranked=sorted((s for s in valid if s[:-4] not in EXCLUDE and not s.endswith("BUSDT") and vol.get(s,0)>90000),
                      key=lambda s:vol[s],reverse=True)
        focus=[s+"USDT" for s in dict.fromkeys(FOCUS) if s not in PRIOR and s+"USDT" in valid and vol.get(s+"USDT",0)>90000]
        core=["FIDAUSDT","LAZIOUSDT","QNTUSDT","ACMUSDT","HEIUSDT","DOGEUSDT","STXUSDT","NOMUSDT","APTUSDT","OSMOUSDT","BEAMXUSDT","IQUSDT","BNTUSDT","MEUSDT","THETAUSDT","RENDERUSDT","TUTUSDT","GLMRUSDT","CKBUSDT","ACTUSDT","RAYUSDT","SSVUSDT","HUMAUSDT","ASTRUSDT","NTRNUSDT","KAVAUSDT"]
        rotation=random.Random(int(time.time()//300))
        rest=[s for s in focus if s not in core]
        rotation.shuffle(rest)
        mids=[s for s in ranked[80:750] if s not in core and s not in rest]
        rotation.shuffle(mids)
        universe=list(dict.fromkeys([s for s in core if s in valid]+rest[:110]+mids[:75]))[:205]
        print("Binance Spot 5m screening",len(universe),flush=True)
        now=int(time.time()*1000);screen=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            for x in ex.map(lambda p:first(p,price,vol,change,now),universe):
                out["pairs_checked"]+=1
                if "error" in x:out["errors"].append(x)
                else:out["stage1"]+=1;screen.append(x)
        # Give fresh volume anomalies and near-resistance structures a chance to be checked.
        screen.sort(key=lambda x:x["initial_score"],reverse=True)
        chosen=screen[:82]
        deep=[]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            for x in ex.map(lambda p:second(p,int(time.time()*1000)),chosen):
                out["deep_checked"]+=1
                if "error" in x:out["errors"].append({"pair":x["pair"],"error":x["error"]})
                else:
                    out["deep_success"]+=1
                    deep.append(x)
        # Check the strongest independent quality candidates at two different times.
        deep.sort(key=lambda x:(x["quality_score"],x["wick_score"]),reverse=True)
        repeat_candidates=deep[:20]
        out["repeat_checked"]=len(repeat_candidates)
        if repeat_candidates:time.sleep(7)
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            repeated=list(ex.map(lambda r:second_snapshot(r,int(time.time()*1000)),repeat_candidates))
        repeats={x["pair"]:x for x in repeated}
        for x in deep:
            x=repeats.get(x["pair"],x)
            if x["qualified_preignition"] and x.get("second_sample_buyers_valid"):
                x["status"]="PERSISTENT PRE-IGNITION"
                out["ignition"].append(x)
            elif x["quality_tier"] in ("STRICT CANDIDATE","PARTIAL WATCH"):
                x["status"]="EARLY WATCH - UNVERIFIED"
                out["early_watch"].append(x)
            else:
                x["status"]="MONITOR"
                out["monitor"].append(x)
    except Exception as e:out["errors"].append({"phase":"discovery","error":str(e)[:250]})
    for key in ("ignition","early_watch","monitor"):out[key].sort(key=lambda x:(x.get("quality_score",0),x.get("wick_score",0)),reverse=True)
    out["finished_utc"]=dt.datetime.now(dt.timezone.utc).isoformat()
    Path("binance_spot_upwick_results.json").write_text(json.dumps(out,indent=2))
    print("UPWICK",out["finished_utc"],"pairs",out["pairs_checked"],"stage1",out["stage1"],"deep",out["deep_success"],
          "IGNITION",len(out["ignition"]),"EARLY",len(out["early_watch"]),"ERRORS",len(out["errors"]),flush=True)
    for k in ("ignition","early_watch"):
        for x in out[k][:14]:print(k,x["pair"],x["wick_score"],"RV1",x["rvol1m"],"RV5",x["rvol5m"],"BUY",x["aggressive_buy_share"],flush=True)
if __name__=="__main__":main()
