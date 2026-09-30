import asyncio
import os

import ignition1211_entry as core

scanner, q, app = core.scanner, core.q, core.app
b121 = core.core
b17 = b121.b17
b16 = b17.b16

VERSION = "10.21.2-top25-board"

BOARD_SIZE = int(os.environ.get("PSI_BOARD_SIZE", "25"))
EARLY_SLOTS = int(os.environ.get("PSI_BOARD_EARLY_SLOTS", "8"))
BREAKOUT_SLOTS = int(os.environ.get("PSI_BOARD_BREAKOUT_SLOTS", "7"))
BUY_SLOTS = int(os.environ.get("PSI_BOARD_BUY_SLOTS", "10"))

# Keep the default board exactly 25 rows. If custom lane values are supplied,
# the remaining capacity is filled from the best unselected candidates.


def board1212():
    cand = b17.diag_pool()
    cand.sort(key=b17.opp_score, reverse=True)
    used = set()

    early = b16._lane_pick(
        cand, used, EARLY_SLOTS, b16._pre_strict, b16._pre_sort, "EARLY-IGNITION"
    )
    breakout = b16._lane_pick(
        cand, used, BREAKOUT_SLOTS, b16._breakout_strict, b16._breakout_sort, "READY-BREAKOUT"
    )
    buy = b16._lane_pick(
        cand, used, BUY_SLOTS, b16._buy_strict, b16._buy_sort, "CLOSEST-BUY"
    )

    out = early + breakout + buy

    # Guarantee the requested board size without weakening formal BUY/PRE rules.
    # Any extra rows are explicitly labelled DEVELOPING, not promoted to a signal.
    if len(out) < BOARD_SIZE:
        for row in cand:
            sym = row.get("symbol")
            if not sym or sym in used:
                continue
            x = dict(row)
            x["board_lane"] = "DEVELOPING"
            x["board_quality"] = "DEVELOPING"
            x["opportunity_score"] = b17.opp_score(x)
            out.append(x)
            used.add(sym)
            if len(out) >= BOARD_SIZE:
                break

    out = out[:BOARD_SIZE]
    for i, row in enumerate(out, 1):
        row["board_rank"] = i
        row["opportunity_score"] = b17.opp_score(row)
    return out


# All downstream modules that ask for the opportunity board now receive Top 25.
b17.board117 = board1212
b16.opportunity_board = board1212


async def board_loop1212():
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            board = board1212()
            b17.record_outcomes(board)

            formal_pre = sum(
                str(x.get("formal_state") or x.get("state")) == "PRE-IGNITION" for x in board
            )
            formal_buy = sum(
                str(x.get("formal_state") or x.get("state")) == "BUY NOW" for x in board
            )
            early_q = sum(
                x.get("board_lane") == "EARLY-IGNITION" and x.get("board_quality") == "QUALIFIED"
                for x in board
            )
            breakout_q = sum(
                x.get("board_lane") == "READY-BREAKOUT" and x.get("board_quality") == "QUALIFIED"
                for x in board
            )
            buy_q = sum(
                x.get("board_lane") == "CLOSEST-BUY" and x.get("board_quality") == "QUALIFIED"
                for x in board
            )

            print(
                f"Ψ-V10.21.2 TOP25 BOARD {len(board)}/{BOARD_SIZE} "
                f"early={early_q}/{EARLY_SLOTS} breakout={breakout_q}/{BREAKOUT_SLOTS} "
                f"closest_buy={buy_q}/{BUY_SLOTS} formal_pre={formal_pre} formal_buy={formal_buy}",
                flush=True,
            )

            for i, r in enumerate(board, 1):
                sym = str(r.get("symbol") or "-")
                lane = str(r.get("board_lane") or "-")
                quality = str(r.get("board_quality") or "-")
                state = str(r.get("formal_state") or r.get("state") or "-")
                layers = b16._layers(r)
                ex = "PASS_ALL" if b16._execution_pass(r) else "BLOCKED"
                mi = "READY" if b16._micro_pass(r) else "WAIT"
                res = r.get("resistance")
                ent = r.get("breakout_entry_trigger")
                dist = r.get("breakout_distance_pct")
                status = str(r.get("entry_status") or "-")
                rtxt = "-" if res is None else f"{float(res):.10g}"
                etxt = status if ent is None else f"{float(ent):.10g}"
                dtxt = "-" if dist is None else f"{float(dist):+.3f}%"
                blockers = r.get("combined_blockers") or r.get("missing_signal_layers") or []
                stale = b17.f(r.get("no_progress_seconds"))
                dv30 = b17.f(r.get("distance_velocity_30s_per_min"))
                dv60 = b17.f(r.get("distance_velocity_60s_per_min"))

                print(
                    f"B{i:02d}. {sym:14s} lane={lane:14s} quality={quality:10s} "
                    f"state={state:18s} opp={b17.opp_score(r):6.1f} layers={layers}/6 "
                    f"exec={ex:8s} micro={mi:5s} ign15={b17.f(r.get('ignition15_score')):5.1f} "
                    f"res={rtxt} dist={dtxt} entry={etxt} status={status} "
                    f"dV30={dv30:+.3f}/m dV60={dv60:+.3f}/m stale={stale:.0f}s "
                    f"blockers={blockers} snapshot={r.get('snapshot_ms')}",
                    flush=True,
                )

            crossed = sum(e.get("trigger_cross_seconds") is not None for e in b17.out_pending)
            print(
                f"Ψ-V10.21.2 PIPELINE hot_injected={b17.stats['hot_injected']} "
                f"hot_structures={b17.stats['hot_structures']} pool_shard={b17.stats['last_shard']} "
                f"pool_applied={b17.stats['pool_applied']} pool_deferred={b17.stats['pool_deferred']} "
                f"shards={sum(b16.shard_connected)}/4 reconnects={b16.shard_reconnects}",
                flush=True,
            )
            print(
                f"Ψ-V10.21.2 OUTCOMES pending={len(b17.out_pending)} resolved={len(b17.out_resolved)} "
                f"trigger_crossed_pending={crossed}",
                flush=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"Ψ-V10.21.2 BOARD_ERROR {type(exc).__name__}: {exc}", flush=True)


# V10.16's combined print loop resolves this module global at runtime, so this
# safely replaces only the opportunity-board printer while leaving every other
# scanner loop, execution gate, micro feed, and early-state engine unchanged.
b16._v1016_board_loop = board_loop1212

b121.VERSION = VERSION
try:
    b121.b1192.VERSION = VERSION
except Exception:
    pass
scanner.VERSION = VERSION
scanner.v7.VERSION = VERSION
app.USER_AGENT = f"psi-v10-live-scanner/{VERSION}"


async def main():
    print(
        f"[v10.21.2] Top-{BOARD_SIZE} scan board active: "
        f"{EARLY_SLOTS} early + {BREAKOUT_SLOTS} breakout + {BUY_SLOTS} closest-buy; "
        "formal BUY NOW/PRE-IGNITION gates unchanged",
        flush=True,
    )
    await core.main()


scanner.v7.main = main


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Psi-V10.21.2 stopped", flush=True)
