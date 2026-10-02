import asyncio
import math
import os

import ignition121_entry as core

scanner, q, app = core.scanner, core.q, core.app

VERSION = "10.21.1-priority-freshness-recovery"

# V10.21.1 keeps the V10.21 early-warning states, but prevents the auxiliary
# freshness worker from starving the main discovery/structure pass.
# COVERAGE_MIN remains an absolute ceiling for backwards compatibility, while
# COVERAGE_RATIO makes the release threshold universe-aware. This matters when
# non-crypto markets are removed: a 403-symbol crypto universe must not wait
# forever for the old 420/456 threshold.
COVERAGE_MIN = int(os.environ.get("PSI_1211_COVERAGE_MIN", "420"))
COVERAGE_RATIO = float(os.environ.get("PSI_1211_COVERAGE_RATIO", "0.92"))
REFRESH_EVERY = float(os.environ.get("PSI_1211_REFRESH_EVERY", "20"))
REFRESH_MAX = int(os.environ.get("PSI_1211_REFRESH_MAX", "4"))
REFRESH_CONCURRENCY = int(os.environ.get("PSI_1211_REFRESH_CONCURRENCY", "2"))
PRIORITY_LIMIT = int(os.environ.get("PSI_1211_PRIORITY_LIMIT", "16"))

# Five-minute structural freshness is strict enough for execution while allowing
# the main scanner to finish a broad universe rotation. Live microstructure
# gates remain mandatory for BUY NOW.
core.STRUCTURE_REFRESH_AGE = float(os.environ.get("PSI_STRUCTURE_REFRESH_AGE", "180"))
core.STRUCTURE_MAX_AGE = float(os.environ.get("PSI_STRUCTURE_MAX_AGE", "300"))

_old_priority_symbols = core.priority_symbols


def priority_symbols_1211(limit=PRIORITY_LIMIT):
    """Keep freshness work aligned with the 16 locked priority micro slots."""
    return _old_priority_symbols(min(int(limit or PRIORITY_LIMIT), PRIORITY_LIMIT))


core.priority_symbols = priority_symbols_1211


async def refresh_structure_light(sym):
    """Refresh only structure; anomaly/micro feeds already have their own loops.

    V10.21 refreshed both structure and fast anomaly for up to 12 symbols every
    8 seconds. That duplicated REST work and competed with the main universe
    rotation. This light refresh intentionally leaves anomaly collection to the
    existing live scanner pipeline.
    """
    if app.session is None:
        return False
    try:
        sd = await app.load_structure(app.session, sym)
        if not isinstance(sd, dict):
            return False
        app.structure[sym] = sd
        q.structure_ms[sym] = q.ms()
        row = app.evaluate_symbol(sym)
        if isinstance(row, dict) and row:
            q.latest[sym] = row
            core.refresh_stats["evaluated"] += 1
        core.refresh_stats["ok"] += 1
        return True
    except asyncio.CancelledError:
        raise
    except Exception:
        core.refresh_stats["errors"] += 1
        return False


core.refresh_structure = refresh_structure_light


def structure_coverage():
    try:
        return sum(
            1
            for sym, ts in (q.structure_ms or {}).items()
            if str(sym).endswith("USDT") and int(ts or 0) > 0
        )
    except Exception:
        return 0


def coverage_target():
    """Execution freshness target; full-universe discovery remains WebSocket-based."""
    try:
        total=len(q.universe or [])
    except Exception:
        total=0
    return min(max(1,PRIORITY_LIMIT),max(1,total))

async def freshness_loop_1211():
    sem = asyncio.Semaphore(max(1, REFRESH_CONCURRENCY))

    async def one(sym):
        async with sem:
            await refresh_structure_light(sym)

    last_target = None
    while True:
        await asyncio.sleep(max(5.0, REFRESH_EVERY))
        try:
            coverage = structure_coverage()
            target = coverage_target()
            core.refresh_stats["coverage"] = coverage
            core.refresh_stats["coverage_target"] = target

            if target != last_target:
                print(
                    f"Ψ-V10.21.1 PRIORITY_FRESHNESS target={target} "
                    f"universe={len(q.universe or [])} broadCoverage={coverage} "
                    f"priority={PRIORITY_LIMIT}",
                    flush=True,
                )
                last_target = target

            # Priority symbols refresh immediately. Broad 403-symbol historical
            # structure coverage is diagnostic only and never blocks this worker.
            stale = [
                s
                for s in priority_symbols_1211(PRIORITY_LIMIT)
                if core.structure_age(s) > core.STRUCTURE_REFRESH_AGE
            ]
            targets = stale[: max(1, REFRESH_MAX)]
            if targets:
                await asyncio.gather(*(one(s) for s in targets))
            core.refresh_stats["cycles"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            core.refresh_stats["errors"] += 1


core.freshness_loop = freshness_loop_1211
core.VERSION = VERSION
try:
    core.b1192.VERSION = VERSION
except Exception:
    pass
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        "[v10.21.1] priority freshness recovery active: full-universe discovery stays WebSocket-based; "
        f"priority structure refresh begins immediately for {PRIORITY_LIMIT} symbols; "
        f"refresh max={REFRESH_MAX}/{REFRESH_EVERY:.0f}s concurrency={REFRESH_CONCURRENCY}; "
        f"structure fresh<= {core.STRUCTURE_MAX_AGE:.0f}s",
        flush=True,
    )
    await core.main()


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Psi-V10.21.1 stopped", flush=True)
