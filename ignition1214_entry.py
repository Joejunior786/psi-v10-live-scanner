import asyncio
import json
import os
import time

import aiohttp

import ignition1213_entry as base
import ignition117_entry as v17
import ignition119_entry as v19

scanner, q, app = base.scanner, base.q, base.app

VERSION = "10.21.4-integrity-repair"

# Binance bStocks / tokenized securities known on Spot through 2026-09-30.
# These are not ordinary crypto assets and must never enter the crypto scanner.
BSTOCK_BASES = {
    "AAOIB", "AAPLB", "ADBEB", "AGPUB", "ALABB", "AMATB", "AMCB", "AMDB",
    "AMZNB", "ARMB", "ASMLB", "ASTSB", "AVGOB", "AXTIB", "BABAB", "BEB",
    "BMNRB", "BNCB", "CBRSB", "COHRB", "COINB", "CRCLB", "CRDOB", "CRMB",
    "CRWDB", "CRWVB", "CYPHB", "DELLB", "DJTB", "DRAMB", "EWYB", "FLNCB",
    "FWDIB", "GLWB", "GMEB", "GOOGLB", "GPROB", "GSB", "HIMSB", "HOODB",
    "HPEB", "IBMB", "INTCB", "INTWB", "IRENB", "KORUB", "LITEB", "METAB",
    "MRNAB", "MRVLB", "MSFTB", "MSTRB", "MUB", "MUUB", "MVLLB", "NBISB",
    "NFLXB", "NOKB", "NVDAB", "ORCLB", "PDDB", "PLTRB", "PYPLB", "QCOMB",
    "QNTB", "QQQB", "RDDTB", "RKLBB", "SHAZB", "SKHYB", "SMCIB", "SMHB",
    "SNDKB", "SNXXB", "SOXLB", "SOXSB", "SPCXB", "SPYB", "SQQQB", "STXB",
    "TQQQB", "TSLAB", "TSMB", "USARB", "WDCB", "WENB", "ZMB",
}

_extra = {
    x.strip().upper()
    for x in os.environ.get("PSI_EXTRA_NONCRYPTO_BASES", "").split(",")
    if x.strip()
}
NONCRYPTO_BASES = BSTOCK_BASES | _extra

try:
    v17.TOKENISED.update(NONCRYPTO_BASES)
except Exception:
    pass
try:
    v17.b16.TOKENISED_BASES.update(NONCRYPTO_BASES)
except Exception:
    pass

_old_exchange = app.get_exchange_symbols
_filter_stats = {"last_removed": [], "last_kept": 0, "runs": 0}


def _base_asset(symbol):
    s = str(symbol or "").upper()
    return s[:-4] if s.endswith("USDT") else s


async def exchange_crypto_only_1214(client):
    rows = await _old_exchange(client)
    removed = [sym for sym, _ in rows if _base_asset(sym) in NONCRYPTO_BASES]
    out = [x for x in rows if _base_asset(x[0]) not in NONCRYPTO_BASES]
    _filter_stats["last_removed"] = removed
    _filter_stats["last_kept"] = len(out)
    _filter_stats["runs"] += 1
    if removed:
        print(
            f"Ψ-V10.21.4 NONCRYPTO_FILTER removed={len(removed)} "
            f"kept={len(out)} symbols={removed[:20]}",
            flush=True,
        )
    return out


app.get_exchange_symbols = exchange_crypto_only_1214

# ---------------------------------------------------------------------------
# Qualifier discovery repair.
#
# V10.4's disc deque was staying at 0/456 even though its websocket reported
# connected. This independent tolerant sampler consumes the all-market ticker
# stream and fills the SAME q.disc deques expected by q.dmetric / V10.15.1.
# It accepts both raw-list and combined-stream payload shapes.
# ---------------------------------------------------------------------------
async def qualifier_discovery_repair_loop():
    while app.session is None:
        await asyncio.sleep(0.25)

    url = f"{str(app.WS_BASE).rstrip('/')}/ws/!ticker@arr"
    while True:
        try:
            q.disc_ws = False
            async with app.session.ws_connect(
                url,
                heartbeat=30,
                receive_timeout=90,
                max_msg_size=0,
            ) as ws:
                q.disc_ws = True
                print("Ψ-V10.21.4 QUALIFIER_REPAIR connected !ticker@arr", flush=True)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except json.JSONDecodeError:
                            continue

                        if isinstance(payload, dict) and "data" in payload:
                            payload = payload.get("data")
                        if isinstance(payload, dict):
                            payload = [payload]
                        if not isinstance(payload, list):
                            continue

                        now = time.time()
                        added = 0
                        for item in payload:
                            if not isinstance(item, dict):
                                continue
                            sym = str(item.get("s") or "").upper()
                            if not sym or sym not in q.universe_set:
                                continue
                            if _base_asset(sym) in NONCRYPTO_BASES:
                                continue

                            last = float(q.disc_sample_ts.get(sym, 0.0) or 0.0)
                            if now - last < float(getattr(q, "SAMPLE_SECONDS", 2.0)):
                                continue

                            try:
                                price = float(item.get("c") or 0.0)
                                quote_volume = float(item.get("q") or 0.0)
                                trades = int(item.get("n") or 0)
                                bid = float(item.get("b") or 0.0)
                                ask = float(item.get("a") or 0.0)
                            except (TypeError, ValueError):
                                continue

                            if price <= 0:
                                continue
                            q.disc[sym].append(
                                (now, price, quote_volume, trades, bid, ask)
                            )
                            q.disc_sample_ts[sym] = now
                            added += 1

                        if added:
                            q.disc_event_ms = int(now * 1000)

                    elif msg.type in (
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.ERROR,
                    ):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            q.disc_ws = False
            print(
                f"Ψ-V10.21.4 QUALIFIER_REPAIR_ERROR "
                f"{type(exc).__name__}: {exc}",
                flush=True,
            )
            await asyncio.sleep(3.0)
        finally:
            q.disc_ws = False

# ---------------------------------------------------------------------------
# Shadow-learning repair.
#
# V10.19 was market-entering PRE/PUMP-WATCH shadow trades immediately even
# when their real strategy entry was a future breakout trigger. That made the
# walk-forward dataset structurally pessimistic and produced ~99% stops.
# Formal BUY NOW behaviour is untouched. Early states are now shadow-opened
# only after a genuine below->above breakout-reference crossing.
# ---------------------------------------------------------------------------
_orig_open_shadow = v19.open_shadow
_shadow_trigger_memory = {}


def _f(value, default=0.0):
    try:
        x = float(value)
    except (TypeError, ValueError):
        return default
    return x if x == x and abs(x) != float("inf") else default


def open_shadow_1214(sym, ps):
    stype = v19.signal_type(sym, ps)
    if not stype:
        return

    # Formal BUY NOW remains simulated exactly as before.
    if stype == "BUY":
        return _orig_open_shadow(sym, ps)

    now = time.time()
    if now - float(v19.shadow_last[(sym, stype)] or 0.0) < float(v19.SHADOW_COOLDOWN):
        return

    row = q.latest.get(sym) or {}
    try:
        entry = v17.atomic_entry(app, q, sym, row)
    except Exception:
        return

    ref = _f(entry.get("breakout_trigger_reference"), 0.0)
    px = _f(v19.current_price(sym), 0.0)
    if ref <= 0 or px <= 0:
        return

    mem = _shadow_trigger_memory.get((sym, stype))
    if (
        mem is None
        or abs((_f(mem.get("ref"), ref) / ref) - 1.0) > 0.0075
        or now - _f(mem.get("updated"), now) > 120.0
    ):
        mem = {
            "ref": ref,
            "seen_below": px < ref,
            "last_below": now if px < ref else 0.0,
            "updated": now,
        }
        _shadow_trigger_memory[(sym, stype)] = mem

    mem["ref"] = ref
    mem["updated"] = now

    if px < ref:
        mem["seen_below"] = True
        mem["last_below"] = now
        return

    # No chase: the candidate must have been observed below this same trigger
    # recently before the crossing can become a simulated entry.
    if not mem.get("seen_below"):
        return
    if now - _f(mem.get("last_below"), 0.0) > 90.0:
        return
    if px > ref * 1.0125:
        return

    real_current_price = v19.current_price
    try:
        # _orig_open_shadow is synchronous. Feeding the verified trigger price
        # makes its existing fee/stop/feature accounting reusable unchanged.
        v19.current_price = (
            lambda s: ref if str(s) == str(sym) else real_current_price(s)
        )
        _orig_open_shadow(sym, ps)
    finally:
        v19.current_price = real_current_price

    mem["seen_below"] = False


v19.open_shadow = open_shadow_1214

# Existing V10.19 history was produced with the invalid immediate-market-entry
# semantics above. Start a clean walk-forward sample unless explicitly disabled.
if os.environ.get("PSI_RESET_INVALID_SHADOW_HISTORY", "1") == "1":
    try:
        old_resolved = len(v19.shadow_resolved)
        old_pending = len(v19.shadow_pending)
        v19.shadow_resolved.clear()
        v19.shadow_pending.clear()
        v19.shadow_last.clear()
        v19.shadow_seq = 0
        v19.calibration.update(
            {
                "status": "WARMING",
                "quality_threshold": 60.0,
                "train_n": 0,
                "valid_n": 0,
                "train_utility": 0.0,
                "valid_utility": 0.0,
                "baseline_valid_utility": 0.0,
                "folds": 0,
                "updated": time.time(),
            }
        )
        print(
            f"Ψ-V10.21.4 SHADOW_RESET invalid_history "
            f"resolved={old_resolved} pending={old_pending}",
            flush=True,
        )
    except Exception as exc:
        print(
            f"Ψ-V10.21.4 SHADOW_RESET_ERROR {type(exc).__name__}: {exc}",
            flush=True,
        )


async def integrity_audit_loop():
    while True:
        await asyncio.sleep(float(getattr(app, "PRINT_SECONDS", 30)))
        try:
            total = len(q.universe)
            qualifier_ready = sum(
                len(q.disc.get(sym, ())) >= 4 for sym in q.universe
            )
            leaks = [
                sym for sym in q.universe
                if _base_asset(sym) in NONCRYPTO_BASES
            ]
            strict_uk = bool(getattr(app, "UK_SYMBOLS", set()))
            uk_scope = (
                "EXACT_ALLOWLIST"
                if strict_uk
                else "CRYPTO_ONLY_PUBLIC_SCOPE_NOT_ACCOUNT_VERIFIED"
            )
            cal = getattr(v19, "calibration", {}) or {}
            print(
                f"Ψ-V10.21.4 INTEGRITY universe={total} "
                f"qualifier_ready={qualifier_ready}/{total} "
                f"discWS={'UP' if getattr(q, 'disc_ws', False) else 'DOWN'} "
                f"noncrypto_leaks={len(leaks)} ukScope={uk_scope} "
                f"learning={cal.get('status', 'UNKNOWN')} "
                f"shadowOpen={len(v19.shadow_pending)} "
                f"shadowResolved={len(v19.shadow_resolved)}",
                flush=True,
            )
            if leaks:
                print(
                    f"Ψ-V10.21.4 LEAK_ALERT symbols={leaks[:25]}",
                    flush=True,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(
                f"Ψ-V10.21.4 INTEGRITY_ERROR {type(exc).__name__}: {exc}",
                flush=True,
            )


base.VERSION = VERSION
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        "[v10.21.4] integrity repair active: crypto-only bStocks denylist, "
        "qualifier discovery repair, trigger-accurate shadow learning; "
        "formal PRE/BUY/PUMP thresholds unchanged",
        flush=True,
    )
    if not getattr(app, "UK_SYMBOLS", set()):
        print(
            "Ψ-V10.21.4 UK_SCOPE exact account/jurisdiction allowlist is not "
            "available from Binance public exchangeInfo; running crypto-only "
            "public Spot scope and refusing to label it exact-UK.",
            flush=True,
        )
    await asyncio.gather(
        base.main(),
        qualifier_discovery_repair_loop(),
        integrity_audit_loop(),
    )


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Psi-V10.21.4 stopped", flush=True)
