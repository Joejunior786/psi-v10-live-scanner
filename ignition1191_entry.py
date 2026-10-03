import asyncio
import math
import time

import ignition119_entry as base

scanner = base.scanner
b18 = base.b18
b17 = base.b17
q = base.q
app = base.app

VERSION = "10.19.6-hard-budget-no-outer-cancel"
SUPPORT_SAMPLE_SECONDS = 10.0
SUPPORT_MAX_SYMBOLS = 8
SUPPORT_MAX_AGE = 120.0
SUPPORT_BATCH_PER_CYCLE = 2
SUPPORT_BUILD_TIMEOUT = 6.0
RISK_FETCH_BUDGET = 4.5
ENTRY_MAX_DISTANCE_PCT = 3.0
MAX_PLAN_RISK_PCT = 3.5
MIN_PLAN_RISK_PCT = 0.20

risk_cache = {}
risk_stats = {"samples": 0, "errors": 0, "timeouts": 0, "slow": 0, "empty": 0, "partial": 0, "sweep_reclaimed": 0, "sweep_risk": 0, "support_lost": 0, "plans": 0, "last_error": "", "last_symbol": ""}
priority_symbols_provider = None

_old_main = scanner.v7.main
_old_diag = b17.diag_pool
_old_opp = b17.opp_score
_old_pump = b18.pump_signature
_learn_mod = getattr(base, "base", None)
_old_feature_snapshot = getattr(_learn_mod, "feature_snapshot", None)
_old_pattern_keys = getattr(_learn_mod, "pattern_keys", None)


def f(v, d=0.0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return d
    return x if math.isfinite(x) else d


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def pct(a, b):
    return ((a / b) - 1.0) * 100.0 if a and b else 0.0


def current_price(sym):
    try:
        return f(base.current_price(sym))
    except Exception:
        try:
            return f(app.current_symbol_price(sym), f((q.latest.get(sym) or {}).get("price")))
        except Exception:
            return f((q.latest.get(sym) or {}).get("price"))


def candle_rows(rows):
    try:
        return base.candle_rows(rows)
    except Exception:
        out = []
        for r in rows or []:
            if not r or len(r) < 7:
                continue
            out.append({"open_ms": int(r[0]), "open": f(r[1]), "high": f(r[2]), "low": f(r[3]), "close": f(r[4]), "close_ms": int(r[6])})
        return out


def atr14(candles):
    cs = list(candles or [])
    if len(cs) < 15:
        return 0.0
    trs = []
    prev = f(cs[-15].get("close"))
    for c in cs[-14:]:
        hi, lo, close = f(c.get("high")), f(c.get("low")), f(c.get("close"))
        if hi <= 0 or lo <= 0:
            continue
        trs.append(max(hi - lo, abs(hi - prev), abs(lo - prev)))
        if close > 0:
            prev = close
    return sum(trs) / len(trs) if trs else 0.0


def pivots(candles, side="low", span=2, tf="5m"):
    cs = list(candles or [])
    out = []
    if len(cs) < span * 2 + 3:
        return out
    key = "low" if side == "low" else "high"
    for i in range(span, len(cs) - span):
        p = f(cs[i].get(key))
        if p <= 0:
            continue
        peers = [f(cs[j].get(key)) for j in range(i - span, i + span + 1) if j != i]
        if not peers:
            continue
        ok = p <= min(peers) if side == "low" else p >= max(peers)
        if ok:
            out.append({"price": p, "ts": f(cs[i].get("close_ms")) / 1000.0, "tf": tf})
    return out


def cluster_levels(points, price, atr_pct, side="support"):
    pts = []
    for p in points or []:
        lv = f(p.get("price"))
        if lv <= 0 or price <= 0:
            continue
        if side == "support" and lv >= price * 1.003:
            continue
        if side == "resistance" and lv <= price * 0.997:
            continue
        pts.append(dict(p))
    if not pts:
        return []
    tol_pct = clamp(max(0.10, atr_pct * 0.22), 0.10, 0.40)
    tf_w = {"1m": 1.0, "3m": 1.3, "5m": 1.8, "15m": 2.6, "mtf": 2.2}
    clusters = []
    for pt in sorted(pts, key=lambda x: f(x.get("price"))):
        lv = f(pt.get("price"))
        match = None
        for c in clusters:
            if abs(pct(lv, c["level"])) <= tol_pct:
                match = c
                break
        w = tf_w.get(str(pt.get("tf")), 1.0)
        if match is None:
            clusters.append({"level": lv, "weight_sum": w, "weighted": lv * w, "touches": 1, "tfs": {str(pt.get("tf"))}, "latest": f(pt.get("ts"))})
        else:
            match["weighted"] += lv * w
            match["weight_sum"] += w
            match["touches"] += 1
            match["tfs"].add(str(pt.get("tf")))
            match["latest"] = max(match["latest"], f(pt.get("ts")))
            match["level"] = match["weighted"] / max(match["weight_sum"], 1e-9)
    now = time.time()
    out = []
    for c in clusters:
        lv = c["level"]
        distance = abs(pct(price, lv))
        recency_min = max(0.0, (now - c["latest"]) / 60.0) if c["latest"] else 999.0
        strength = clamp(18.0 + min(35.0, c["touches"] * 8.0) + min(22.0, c["weight_sum"] * 3.0) + max(0.0, 15.0 - recency_min * 0.35) + max(0.0, 10.0 - distance * 4.0), 0.0, 100.0)
        out.append({"level": lv, "touches": c["touches"], "tf_count": len(c["tfs"]), "strength": round(strength, 1), "distance_pct": round(pct(price, lv) if side == "support" else pct(lv, price), 4)})
    out.sort(key=lambda x: x["level"], reverse=(side == "support"))
    dedup = []
    for x in out:
        if not dedup or abs(pct(x["level"], dedup[-1]["level"])) > tol_pct * 0.85:
            dedup.append(x)
    return dedup


def nearest_overhead_resistance(price, resistances):
    vals = [x for x in resistances or [] if f(x.get("level")) > price]
    return min(vals, key=lambda x: f(x.get("level"))) if vals else None


def structural_targets(entry, resistances):
    return sorted({round(f(x.get("level")), 12) for x in resistances or [] if f(x.get("level")) > entry * 1.0025})


def detect_sweep(price, support_clusters, c1, c5, atr):
    if price <= 0:
        return {"sweep_state": "WAIT", "sweep_score": 0.0, "stop_cluster": None, "stop_cluster_strength": 0.0}
    candidates = [x for x in support_clusters if f(x.get("level")) < price and 0 <= pct(price, f(x.get("level"))) <= 2.5]
    clustered = [x for x in candidates if int(x.get("touches") or 0) >= 2]
    pool = clustered[0] if clustered else (candidates[0] if candidates else None)
    if not pool:
        return {"sweep_state": "NO_CLUSTER", "sweep_score": 0.0, "stop_cluster": None, "stop_cluster_strength": 0.0}
    level = f(pool.get("level"))
    strength = f(pool.get("strength"))
    dist = pct(price, level)
    reclaimed = False
    deepest = 0.0
    wick_score = 0.0
    for c in list(c1[-4:]) + list(c5[-2:]):
        lo, close, hi, op = f(c.get("low")), f(c.get("close")), f(c.get("high")), f(c.get("open"))
        if lo <= 0 or close <= 0:
            continue
        pierce = max(0.0, pct(level, lo)) if lo < level else 0.0
        deepest = max(deepest, pierce)
        if lo < level * 0.9994 and close > level * 1.0002:
            reclaimed = True
            wick = max(0.0, hi - max(close, op)) + max(0.0, min(close, op) - lo)
            wick_score = max(wick_score, clamp(wick / max(hi - lo, 1e-12) * 100.0, 0, 100))
    support_lost = price < level - max(atr * 0.25, level * 0.0015)
    if support_lost:
        state, score = "SUPPORT_LOST", 95.0
    elif reclaimed:
        state = "SWEEP_RECLAIMED"
        score = clamp(55 + strength * 0.25 + min(20.0, deepest * 40.0) + wick_score * 0.10, 0, 100)
    elif dist <= 0.90 and strength >= 50:
        state = "LIQUIDITY_SWEEP_RISK"
        score = clamp(35 + strength * 0.45 + max(0.0, 0.9 - dist) * 20.0, 0, 100)
    else:
        state, score = "SUPPORT_CLUSTER", clamp(strength * 0.5, 0, 70)
    return {"sweep_state": state, "sweep_score": round(score, 1), "stop_cluster": level, "stop_cluster_strength": round(strength, 1), "stop_cluster_distance_pct": round(dist, 4), "deepest_pierce_pct": round(deepest, 4)}


def trade_plan(sym, price, atr, supports, resistances, sweep):
    plan = {"entry_trigger": None, "stop_loss": None, "tp1": None, "tp2": None, "tp3": None, "risk_pct": None, "plan_state": "WAIT", "exit_rule": "WAIT_FOR_VALID_ENTRY", "target_basis": None}
    if price <= 0:
        return plan
    mtf = base.mtf_cache.get(sym) or {}
    frames = mtf.get("frames") or {}
    f5 = frames.get("5m") or {}
    phase5 = str(f5.get("phase") or "UNKNOWN")
    used = f(f5.get("used_level"))
    nearest = nearest_overhead_resistance(price, resistances)
    r1 = f(nearest.get("level")) if nearest else 0.0
    entry = 0.0
    entry_buffer_pct = clamp(max(0.05, (atr / price * 100.0) * 0.10 if atr > 0 else 0.05), 0.05, 0.20)
    if phase5 in {"FAILED_BREAKOUT", "REJECT_FALLING", "NO_CHASE"}:
        plan["plan_state"] = "NO_ENTRY_LIFECYCLE"
        return plan
    if phase5 in {"RETEST", "BREAKOUT_HOLD"} and used > 0:
        entry = used * (1.0 + entry_buffer_pct / 100.0)
        plan["plan_state"] = "CONDITIONAL_RECLAIM"
    elif r1 > price and pct(r1, price) <= ENTRY_MAX_DISTANCE_PCT:
        entry = r1 * (1.0 + entry_buffer_pct / 100.0)
        plan["plan_state"] = "CONDITIONAL_BREAKOUT"
    elif phase5 == "CONTINUATION":
        plan["plan_state"] = "WAIT_RETEST_NO_CHASE"
        return plan
    else:
        plan["plan_state"] = "WAIT_APPROACH"
        return plan
    s1 = f(supports[0].get("level")) if supports else 0.0
    pool = f(sweep.get("stop_cluster"))
    anchors = [x for x in (s1, pool) if x > 0 and x < entry and pct(entry, x) <= 4.0]
    if not anchors:
        plan["plan_state"] = "WAIT_SUPPORT_MAP"
        return plan
    anchor = min(anchors)
    stop_buffer = max(atr * 0.25 if atr > 0 else 0.0, anchor * 0.0015)
    stop = anchor - stop_buffer
    if stop <= 0 or stop >= entry:
        plan["plan_state"] = "WAIT_INVALID_RISK"
        return plan
    risk = entry - stop
    risk_pct = risk / entry * 100.0
    if risk_pct > MAX_PLAN_RISK_PCT or risk_pct < MIN_PLAN_RISK_PCT:
        plan.update({"entry_trigger": entry, "stop_loss": stop, "risk_pct": round(risk_pct, 3), "plan_state": "RISK_OUT_OF_BOUNDS", "exit_rule": "NO_EXECUTION"})
        return plan
    t1, t2, t3 = entry + risk, entry + 2 * risk, entry + 3 * risk
    structural = structural_targets(entry, resistances)
    def snap(target, lo_mult, hi_mult):
        lo = entry + lo_mult * risk
        hi = entry + hi_mult * risk
        candidates = [x for x in structural if lo <= x <= hi]
        return min(candidates) * 0.999 if candidates else target
    t1 = snap(t1, 0.70, 1.35)
    t2 = snap(t2, 1.55, 2.50)
    t3 = snap(t3, 2.50, 3.75)
    plan.update({"entry_trigger": entry, "stop_loss": stop, "tp1": t1, "tp2": t2, "tp3": t3, "risk_pct": round(risk_pct, 3), "exit_rule": "STOP_OR_5M_CLOSE_BELOW_SUPPORT", "target_basis": "STRUCTURE+R_MULTIPLE"})
    return plan


async def build_risk_map(sym):
    if app.session is None:
        return
    try:
        fetch = getattr(app, "load_risk_klines", app.load_klines)
        jobs = {
            "1m": asyncio.create_task(fetch(app.session, sym, "1m", 64)),
            "5m": asyncio.create_task(fetch(app.session, sym, "5m", 72)),
            "15m": asyncio.create_task(fetch(app.session, sym, "15m", 52)),
        }
        done, pending = await asyncio.wait(
            set(jobs.values()),
            timeout=RISK_FETCH_BUDGET,
            return_when=asyncio.ALL_COMPLETED,
        )
        if pending:
            risk_stats["partial"] += 1
            for task in pending:
                task.cancel()
                # Do not await cancellation here. Some aiohttp/WS cancellation
                # paths can outlive their requested timeout; the RiskMap
                # scheduler must remain hard-bounded.
                task.add_done_callback(lambda t: t.exception() if (not t.cancelled() and t.exception() is not None) else None)

        results = {}
        for name, task in jobs.items():
            if task not in done:
                results[name] = []
                continue
            try:
                results[name] = task.result() or []
            except asyncio.CancelledError:
                raise
            except Exception:
                results[name] = []

        rows1, rows5, rows15 = results["1m"], results["5m"], results["15m"]
        c1, c5, c15 = candle_rows(rows1), candle_rows(rows5), candle_rows(rows15)
        if len(c5) < 20:
            risk_stats["empty"] += 1
            risk_stats["last_symbol"] = sym
            risk_stats["last_error"] = f"EMPTY_CANDLES 1m={len(c1)} 5m={len(c5)} 15m={len(c15)}"
            return
        price = current_price(sym) or f(c1[-1].get("close") if c1 else c5[-1].get("close"))
        atr = atr14(c5[:-1] if len(c5) > 1 else c5)
        atr_pct = atr / price * 100.0 if atr > 0 and price > 0 else 0.5
        lows = pivots(c1[:-1], "low", 2, "1m") + pivots(c5[:-1], "low", 2, "5m") + pivots(c15[:-1], "low", 1, "15m")
        highs = pivots(c1[:-1], "high", 2, "1m") + pivots(c5[:-1], "high", 2, "5m") + pivots(c15[:-1], "high", 1, "15m")
        try:
            ms = base.mtf_summary(sym)
            s5, r5 = f(ms.get("mtf_support_5m")), f(ms.get("mtf_resistance_5m"))
            if s5 > 0: lows.append({"price": s5, "ts": time.time(), "tf": "mtf"})
            if r5 > 0: highs.append({"price": r5, "ts": time.time(), "tf": "mtf"})
        except Exception:
            pass
        supports = cluster_levels(lows, price, atr_pct, "support")
        resistances = cluster_levels(highs, price, atr_pct, "resistance")
        sweep = detect_sweep(price, supports, c1, c5, atr)
        plan = trade_plan(sym, price, atr, supports, resistances, sweep)
        risk_cache[sym] = {"symbol": sym, "updated": time.time(), "price": price, "atr5": atr, "atr5_pct": round(atr_pct, 4), "support1": f(supports[0].get("level")) if len(supports) > 0 else None, "support2": f(supports[1].get("level")) if len(supports) > 1 else None, "support3": f(supports[2].get("level")) if len(supports) > 2 else None, "support1_strength": f(supports[0].get("strength")) if supports else 0.0, "support1_distance_pct": pct(price, f(supports[0].get("level"))) if supports and f(supports[0].get("level")) > 0 else None, "resistance1": f(resistances[0].get("level")) if len(resistances) > 0 else None, "resistance2": f(resistances[1].get("level")) if len(resistances) > 1 else None, "resistance3": f(resistances[2].get("level")) if len(resistances) > 2 else None, **sweep, **plan}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        risk_stats["errors"] += 1
        risk_stats["last_symbol"] = sym
        risk_stats["last_error"] = f"{type(exc).__name__}: {exc}"
        print(f"Ψ-V10.19.6 RISKMAP_BUILD_ERROR {sym} {type(exc).__name__}: {exc}", flush=True)


async def support_loop():
    cursor = 0

    async def one(sym):
        started=time.time()
        try:
            # build_risk_map() owns the only hard deadline via RISK_FETCH_BUDGET.
            # Do not wrap it in wait_for(): on a busy loop the outer timer can
            # start before the task itself gets CPU and create false timeouts.
            await build_risk_map(sym)
            elapsed=time.time()-started
            if elapsed>SUPPORT_BUILD_TIMEOUT:
                risk_stats["slow"] += 1
                print(f"Ψ-V10.19.6 RISKMAP_SLOW {sym} elapsed={elapsed:.2f}s budget={RISK_FETCH_BUDGET:.1f}s", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            risk_stats["errors"] += 1
            risk_stats["last_symbol"] = sym
            risk_stats["last_error"] = f"{type(exc).__name__}: {exc}"
            print(f"Ψ-V10.19.6 RISKMAP_BUILD_ERROR {sym} {type(exc).__name__}: {exc}", flush=True)

    while True:
        await asyncio.sleep(SUPPORT_SAMPLE_SECONDS)
        try:
            syms=[];seen=set()
            try:
                if callable(priority_symbols_provider):
                    for sym in priority_symbols_provider() or []:
                        if sym and sym not in seen:
                            syms.append(sym);seen.add(sym)
                        if len(syms)>=SUPPORT_MAX_SYMBOLS: break
            except Exception:
                pass
            if len(syms)<SUPPORT_MAX_SYMBOLS:
                for sym in base.candidate_symbols(SUPPORT_MAX_SYMBOLS*3):
                    sym=str(sym or "")
                    # Cold-start fallback is scheduler-only, not a universe
                    # filter. Non-ASCII markets remain eligible through the
                    # formal priority provider once they become real setups.
                    if not sym.isascii():
                        continue
                    if sym not in seen:
                        syms.append(sym);seen.add(sym)
                    if len(syms)>=SUPPORT_MAX_SYMBOLS: break

            if not syms:
                continue

            n=min(SUPPORT_BATCH_PER_CYCLE,len(syms))
            batch=[syms[(cursor+i)%len(syms)] for i in range(n)]
            cursor=(cursor+n)%max(1,len(syms))

            # Exactly two complete risk-map builds at once. V11.0.5.36 uses
            # a hard internal timeframe budget; no outer cancellation timer is
            # allowed to generate false timeouts under event-loop pressure.
            await asyncio.gather(*(one(sym) for sym in batch))
            risk_stats["samples"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            risk_stats["errors"] += 1


def risk_intel(sym):
    x = dict(risk_cache.get(sym) or {})
    if not x or time.time() - f(x.get("updated")) > SUPPORT_MAX_AGE:
        return {"sweep_state": "WAIT", "sweep_score": 0.0, "support1": None, "support2": None, "support3": None, "entry_trigger": None, "stop_loss": None, "tp1": None, "tp2": None, "tp3": None, "plan_state": "WAIT"}
    return x


def diag1191():
    out = []
    for raw in _old_diag():
        row = dict(raw)
        sym = str(row.get("symbol") or "")
        if sym: row.update(risk_intel(sym))
        out.append(row)
    return out


def opp1191(row):
    score = f(_old_opp(row))
    sym = str((row or {}).get("symbol") or "")
    ri = risk_intel(sym)
    state = str(ri.get("sweep_state") or "WAIT")
    if state == "SWEEP_RECLAIMED": score += 4.0
    elif state == "SUPPORT_LOST": score -= 10.0
    elif state == "LIQUIDITY_SWEEP_RISK": score -= 1.5
    sdist, sstr = f(ri.get("support1_distance_pct"), 99.0), f(ri.get("support1_strength"))
    if 0 <= sdist <= 1.0 and sstr >= 65 and state != "SUPPORT_LOST": score += 1.5
    if str(ri.get("plan_state")) == "RISK_OUT_OF_BOUNDS": score -= 2.0
    return round(score, 2)


def pump1191(sym):
    ps = dict(_old_pump(sym))
    ps.update(risk_intel(sym))
    return ps


def feature_snapshot1191(sym):
    x = dict(_old_feature_snapshot(sym)) if _old_feature_snapshot else {}
    ri = risk_intel(sym)
    x.update({"sweep_state1191": ri.get("sweep_state"), "sweep_score1191": f(ri.get("sweep_score")), "support_dist1191": f(ri.get("support1_distance_pct"), 99.0), "support_strength1191": f(ri.get("support1_strength")), "risk_pct1191": f(ri.get("risk_pct"), 99.0), "plan_state1191": ri.get("plan_state")})
    return x


def support_bucket(x):
    v = f(x, 99.0)
    return "S<0.5" if 0 <= v < 0.5 else ("S0.5-1.5" if v < 1.5 else "S>1.5")


def risk_bucket(x):
    v = f(x, 99.0)
    return "R<1" if 0 < v < 1 else ("R1-2" if v < 2 else ("R2-3.5" if v <= 3.5 else "R>3.5"))


def pattern_keys1191(snapshot):
    keys = list(_old_pattern_keys(snapshot)) if _old_pattern_keys else []
    keys.extend(["SWEEP|" + str(snapshot.get("sweep_state1191") or "WAIT"), "SUPPORT|" + support_bucket(snapshot.get("support_dist1191")), "RISK|" + risk_bucket(snapshot.get("risk_pct1191")), "V191COMBO|" + str(snapshot.get("sweep_state1191") or "WAIT") + "|" + support_bucket(snapshot.get("support_dist1191")) + "|" + risk_bucket(snapshot.get("risk_pct1191"))])
    return keys


def fmt_px(v):
    x = f(v)
    if x <= 0: return "-"
    if x >= 1000: return f"{x:.2f}"
    if x >= 1: return f"{x:.6f}".rstrip("0").rstrip(".")
    if x >= 0.01: return f"{x:.7f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")


async def print_risk_loop():
    while True:
        await asyncio.sleep(float(getattr(base, "PRINT_SECONDS", 30)))
        try:
            fresh = [(sym, x) for sym, x in risk_cache.items() if time.time() - f(x.get("updated")) <= SUPPORT_MAX_AGE]
            sweeps = sum(str(x.get("sweep_state")) == "SWEEP_RECLAIMED" for _, x in fresh)
            risks = sum(str(x.get("sweep_state")) == "LIQUIDITY_SWEEP_RISK" for _, x in fresh)
            lost = sum(str(x.get("sweep_state")) == "SUPPORT_LOST" for _, x in fresh)
            plans = sum(f(x.get("entry_trigger")) > 0 and f(x.get("stop_loss")) > 0 for _, x in fresh)
            risk_stats.update({"sweep_reclaimed": sweeps, "sweep_risk": risks, "support_lost": lost, "plans": plans})
            states={}
            for _,x in fresh:
                st=str(x.get("plan_state") or "WAIT");states[st]=states.get(st,0)+1
            print(f"Ψ-V10.19.6 RISKMAP tracked={len(fresh)} sweeps={sweeps} sweepRisk={risks} supportLost={lost} plans={plans} samples={risk_stats['samples']} states={states} errors={risk_stats['errors']} timeouts={risk_stats['timeouts']} slow={risk_stats['slow']} partial={risk_stats['partial']} empty={risk_stats['empty']} last={risk_stats['last_symbol']}:{risk_stats['last_error']}", flush=True)
            ranked = sorted(fresh, key=lambda item: (1 if str(item[1].get("sweep_state")) == "SWEEP_RECLAIMED" else 0, f(item[1].get("sweep_score")), f(item[1].get("support1_strength")), -f(item[1].get("support1_distance_pct"), 99)), reverse=True)[:10]
            for i, (sym, x) in enumerate(ranked, 1):
                risk_text = "-" if x.get("risk_pct") is None else f"{f(x.get('risk_pct')):.3f}%"
                print(f"X{i:02d}. {sym:<14} support={fmt_px(x.get('support1'))}/{fmt_px(x.get('support2'))}/{fmt_px(x.get('support3'))} pool={fmt_px(x.get('stop_cluster'))} sweep={x.get('sweep_state','WAIT')}:{f(x.get('sweep_score')):.1f} entry={fmt_px(x.get('entry_trigger'))} stop={fmt_px(x.get('stop_loss'))} tp1={fmt_px(x.get('tp1'))} tp2={fmt_px(x.get('tp2'))} tp3={fmt_px(x.get('tp3'))} risk={risk_text} plan={x.get('plan_state','WAIT')}", flush=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Ψ-V10.19.6 RISKMAP_ERROR {type(e).__name__}: {e}", flush=True)


b17.diag_pool = diag1191
b17.opp_score = opp1191
b18.pump_signature = pump1191
if _learn_mod is not None and _old_feature_snapshot is not None: _learn_mod.feature_snapshot = feature_snapshot1191
if _learn_mod is not None and _old_pattern_keys is not None: _learn_mod.pattern_keys = pattern_keys1191


async def main1191():
    await asyncio.gather(_old_main(), support_loop(), print_risk_loop())


scanner.v7.main = main1191
scanner.VERSION = VERSION

print("Ψ-V10.19.6 UPGRADE ACTIVE — hard-budget multi-TF support map, non-blocking WS/REST acquisition, partial-timeframe recovery, sweep/reclaim + support-loss detection, ATR-buffered stop placement, verified conditional entry, TP1/TP2/TP3 risk map; formal PRE/BUY gates unchanged", flush=True)

if __name__ == "__main__":
    try:
        print("Ψ-V10.19.6 ACTIVE — hard-budget support + liquidity-sweep risk intelligence", flush=True)
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        try: base.save_v119_state()
        except Exception: pass
