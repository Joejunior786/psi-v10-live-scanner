import asyncio
import time
from collections import defaultdict

import ignition118_entry as b18

scanner=b18.scanner; q=scanner.q; app=scanner.app; b17=b18.b17; b16=b17.b16; v15=b17.v15
VERSION="10.18.1-pump-signature-stable-rotation"
MIGRATION_BUDGET=4

_bootstrap_rebalance=q.rebalance_pool


def f(v,d=0.0):
    try: return float(v)
    except (TypeError,ValueError): return d


def protected(sym):
    if sym in set(q.locks()): return True
    r=q.latest.get(sym) or {}
    state=str(r.get("formal_state") or r.get("pre_warmup_state") or r.get("state") or "")
    ps=b18.pump_cache.get(sym) or {}
    return state in ("BUY NOW","PRE-IGNITION") or bool(r.get("ignition15_watch")) or ps.get("pump_state")=="PUMP-ARMED"


def quality(sym):
    raw=f(q.sscore(sym),-999.0); ps=b18.pump_cache.get(sym) or b18.pump_signature(sym); pump=f(ps.get("pump_signature_score"),0.0)
    r=q.latest.get(sym) or {}; layers=b16._layers(r) if r else 0
    return max(raw,80.0+pump)+layers*2.0


async def stable_rebalance(force=False):
    current=[x for x in dict.fromkeys(app.selected_micro_symbols) if b17.directional(x)]
    # Initial population may use the inherited planner before shards exist.
    if not current or not b16.shard_assignments:
        await _bootstrap_rebalance(force)
        return

    now=time.time(); cset=set(current)
    incoming=[]
    for score,sym in b18.augmented_hot(200):
        if sym in cset or not b17.directional(sym) or sym not in q.universe_set: continue
        if f(v15.evicted_until.get(sym,0.0))>now: continue
        if sym not in app.structure or not q.sfresh(sym): continue
        incoming.append((max(f(score),quality(sym)),sym))
    incoming.sort(reverse=True)
    if not incoming: return

    groups=defaultdict(list)
    for sym in current:
        sh=b16.shard_assignments.get(sym)
        if sh is None or protected(sym): continue
        groups[sh].append((quality(sym),sym))
    if not groups: return

    # Pick the single shard containing the weakest replaceable names.
    for sh in groups: groups[sh].sort()
    shard=min(groups,key=lambda sh:(sum(x[0] for x in groups[sh][:MIGRATION_BUDGET])/max(1,min(MIGRATION_BUDGET,len(groups[sh]))),sh))
    victims=groups[sh][:MIGRATION_BUDGET]

    replacements=[]; used=set()
    for vscore,old in victims:
        choice=None
        for iscore,sym in incoming:
            if sym in used: continue
            if iscore>=vscore+5.0:
                choice=(iscore,sym); break
        if not choice: continue
        used.add(choice[1]); replacements.append((old,choice[1],vscore,choice[0]))
    if not replacements: return

    rm={x[0] for x in replacements}; add=[x[1] for x in replacements]
    final=[x for x in current if x not in rm]+add
    app.selected_micro_symbols=final[:int(getattr(q,"MICRO_SLOTS",80))]
    app.last_micro_pool_change=now
    q.pool_cycles+=1
    for old,new,_,_ in replacements:
        q.entered.pop(old,None); v15.evicted_until[old]=now+float(v15.RECYCLE_COOLDOWN_SECONDS)
        q.entered[new]=now; app.ensure_micro_state(new)
    b17.stats["pool_applied"]+=len(replacements); b17.stats["last_shard"]=shard+1
    print(f"Ψ-V10.18 MICRO_STABLE shard={shard+1} added={len(replacements)} removed={len(replacements)} budget={MIGRATION_BUDGET}",flush=True)


q.rebalance_pool=stable_rebalance
scanner.VERSION=VERSION
_old_main=scanner.v7.main
async def main1181(): await _old_main()
scanner.v7.main=main1181

print("Ψ-V10.18.1 ROTATION PATCH ACTIVE — ordinary pool rebalance limited to one shard / four replacements; PUMP-ARMED and formal signals protected",flush=True)
if __name__=="__main__":
    try: print("Ψ-V10.18.1 ACTIVE — pump signature + stable shard continuity",flush=True); asyncio.run(scanner.v7.main())
    except KeyboardInterrupt: print("Ψ-V10.18.1 stopped",flush=True)
