import asyncio
import math

import ignition115_entry as base15

scanner = base15.scanner
q = scanner.q
s = scanner.s
app = scanner.app

VERSION = "10.15.1-radar-discovery-fallback"


def _f(value, default=0.0):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _discovery_metric(symbol):
    local = q.dmetric(symbol)
    if int(local.get("samples") or 0) >= 4:
        out = dict(local)
        out["source"] = "QUALIFIER_DISCOVERY"
        return out

    try:
        rapid = scanner.v93.metric(symbol) or {}
    except Exception:
        rapid = {}

    score = _f(rapid.get("score"), 0.0)
    r15 = _f(rapid.get("r15"), 0.0)
    r60 = _f(rapid.get("r60"), 0.0)
    spread = _f(rapid.get("spread_bps"), 999.0)

    # Discovery must stay early. Penalise already-expanded/chasing candidates.
    if r15 >= 3.0 or r60 >= 6.0:
        score -= 30.0
    if spread > 30.0:
        score -= 12.0

    try:
        samples = len(scanner.base.radar_hist.get(symbol) or ())
    except Exception:
        samples = 0

    return {
        "score": round(score, 3),
        "r15": r15,
        "r60": r60,
        "spread_bps": spread,
        "samples": samples,
        "source": "V10_ALL_MARKET_RADAR",
    }


def _full_hot(limit=None):
    if limit is None:
        limit = getattr(q, "HOT_COUNT", 80)
    rows = [(_discovery_metric(symbol).get("score", 0.0), symbol) for symbol in q.universe]
    rows.sort(reverse=True)
    return rows[:max(1, int(limit))]


q.hot = _full_hot
scanner.VERSION = VERSION

print(
    "Ψ-V10.15.1 ALL-MARKET DISCOVERY FALLBACK ACTIVE — qualifier discovery remains preferred when ready; "
    "otherwise V10 all-market RAPID/radar history ranks newcomers across the full Binance universe; "
    "formal PRE/BUY gates unchanged",
    flush=True,
)


_previous_print_loop = scanner.v7.print_loop


async def _discovery_fallback_telemetry():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        total = len(q.universe)
        qualifier_ready = sum(len(q.disc.get(symbol, ())) >= 4 for symbol in q.universe)
        radar_ready = 0
        for symbol in q.universe:
            try:
                radar_ready += int(len(scanner.base.radar_hist.get(symbol) or ()) >= 5)
            except Exception:
                pass

        leaders = []
        for score, symbol in _full_hot(5):
            metric = _discovery_metric(symbol)
            leaders.append((symbol, round(float(score), 1), metric.get("source")))

        print(
            f"Ψ-V10.15.1 DISCOVERY qualifier_ready={qualifier_ready}/{total} "
            f"radar_ready={radar_ready}/{total} effective_ready={max(qualifier_ready, radar_ready)}/{total} "
            f"leaders={leaders}",
            flush=True,
        )


async def _combined_print_loop():
    await asyncio.gather(
        _previous_print_loop(),
        _discovery_fallback_telemetry(),
    )


scanner.v7.print_loop = _combined_print_loop
scanner.q.print_loop = _combined_print_loop
scanner.s.print_loop = _combined_print_loop


if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.15.1 ACTIVE — fair rotation + all-market radar discovery fallback; strict PRE/BUY unchanged",
            flush=True,
        )
        asyncio.run(scanner.v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.15.1 stopped", flush=True)
