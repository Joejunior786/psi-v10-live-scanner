import asyncio

import app


def _book_reason(row):
    votes = dict(row.get("book_votes") or {})
    micro_ready = bool(row.get("micro_ready"))
    seq_ok = bool(row.get("book_sequence_verified"))
    obi = float(row.get("obi") or 0.0)
    ask_dep = float(row.get("ask_depletion") or 0.0)

    if not micro_ready:
        reason = "BOOK_MICRO_NOT_READY"
    elif not seq_ok:
        reason = "BOOK_SEQUENCE_NOT_VERIFIED"
    elif votes and not any(bool(v) for v in votes.values()):
        reason = "LIVE_BOOK_BUT_NO_BULLISH_PRESSURE"
    elif votes and not bool(votes.get("REL_BOOK")) and not (bool(votes.get("OBI")) and bool(votes.get("ASK_THIN"))):
        reason = "LIVE_BOOK_PRESSURE_BELOW_LAYER_THRESHOLD"
    else:
        reason = "BOOK_LAYER_CONFIRMED"

    return {
        "data_live": micro_ready,
        "sequence_verified": seq_ok,
        "obi": round(obi, 6),
        "ask_depletion": round(ask_dep, 6),
        "votes": votes,
        "layer_confirmed": "ORDER_BOOK_LAYER" not in (row.get("failed_layers") or row.get("failed_setup") or []),
        "reason": reason,
    }


def install_depth_sequence_patch():
    """Use Binance's documented inclusive update-range continuity rule.

    A valid diff-depth event only needs to cover local_update_id + 1:
    U <= local_id + 1 <= u. Requiring U == local_id + 1 rejects valid
    overlapping packets and can cause unnecessary book resync loops.
    """

    def process_diff_depth(symbol: str, data: dict) -> None:
        s = app.ensure_micro_state(symbol)
        try:
            first_id = int(data.get("U"))
            final_id = int(data.get("u"))
        except (TypeError, ValueError):
            s["book_sequence_ok"] = False
            return

        event = {
            "U": first_id,
            "u": final_id,
            "b": data.get("b", []),
            "a": data.get("a", []),
        }

        if not s["book_snapshot_ready"]:
            s["book_buffer"].append(event)
            return

        last = s.get("last_book_update_id")
        if last is None:
            s["book_sequence_ok"] = False
            s["book_buffer"].append(event)
            app.schedule_book_resync(symbol)
            return

        # Old/duplicate event: safe to ignore.
        if final_id <= last:
            return

        next_id = last + 1
        # True gap: the event starts after the next update we require.
        if first_id > next_id:
            s["book_sequence_ok"] = False
            s["book_buffer"].append(event)
            app.schedule_book_resync(symbol)
            return

        # Valid overlap/continuation: U <= last+1 <= u.
        if not (first_id <= next_id <= final_id):
            s["book_sequence_ok"] = False
            s["book_buffer"].append(event)
            app.schedule_book_resync(symbol)
            return

        prev_bids = app.sorted_levels(s["book_bids"], True)
        prev_asks = app.sorted_levels(s["book_asks"], False)
        app.apply_book_changes(s["book_bids"], event["b"])
        app.apply_book_changes(s["book_asks"], event["a"])
        s["last_book_update_id"] = final_id
        s["book_sequence_samples"] += 1
        s["book_sequence_ok"] = True
        app.calculate_book_metrics(symbol, prev_bids, prev_asks)

    async def bootstrap_book(symbol: str) -> None:
        s = app.ensure_micro_state(symbol)
        if app.session is None:
            s["book_resyncing"] = False
            return
        try:
            snap = await app.api_get(
                app.session,
                "/api/v3/depth",
                {"symbol": symbol, "limit": app.DEPTH_SNAPSHOT_LIMIT},
            )
            last_id = int(snap.get("lastUpdateId", 0))
            bids = {
                app.safe_float(p): app.safe_float(q)
                for p, q in snap.get("bids", [])
                if app.safe_float(p) > 0 and app.safe_float(q) > 0
            }
            asks = {
                app.safe_float(p): app.safe_float(q)
                for p, q in snap.get("asks", [])
                if app.safe_float(p) > 0 and app.safe_float(q) > 0
            }

            buffered = [e for e in list(s["book_buffer"]) if e["u"] > last_id]
            s["book_buffer"].clear()

            current_id = last_id
            started = False
            for e in buffered:
                if e["u"] <= current_id:
                    continue
                next_id = current_id + 1
                if not started:
                    if e["U"] <= next_id <= e["u"]:
                        started = True
                    else:
                        continue
                elif e["U"] > next_id:
                    raise RuntimeError("depth sequence gap during bootstrap")
                elif not (e["U"] <= next_id <= e["u"]):
                    continue

                app.apply_book_changes(bids, e["b"])
                app.apply_book_changes(asks, e["a"])
                current_id = e["u"]

            s["book_bids"], s["book_asks"] = bids, asks
            s["last_book_update_id"] = current_id
            s["book_snapshot_ready"] = True
            s["book_sequence_ok"] = True
            s["book_sequence_samples"] = max(s["book_sequence_samples"], 3)
            app.calculate_book_metrics(symbol, [], [])
        except Exception:
            s["book_snapshot_ready"] = False
            s["book_sequence_ok"] = False
        finally:
            s["book_resyncing"] = False

    app.process_diff_depth = process_diff_depth
    app.bootstrap_book = bootstrap_book


def install_diagnostics(scanner_module):
    original_evaluate = scanner_module.evaluate

    def evaluate(symbol):
        row = original_evaluate(symbol)
        if row:
            row["order_book_diagnostics"] = _book_reason(row)
        return row

    scanner_module.evaluate = evaluate
    app.evaluate_symbol = evaluate

    try:
        stable = scanner_module.s
        original_near = stable._near_row

        def near_row(row, blockers=None):
            out = original_near(row, blockers)
            out["order_book_diagnostics"] = row.get("order_book_diagnostics") or _book_reason(row)
            return out

        stable._near_row = near_row
    except Exception:
        pass


async def book_diagnostic_loop(scanner_module):
    while True:
        await asyncio.sleep(app.PRINT_SECONDS)
        try:
            rows = scanner_module.s.near_diag(10)
            parts = []
            for row in rows:
                d = row.get("order_book_diagnostics") or {}
                if d:
                    parts.append(
                        f"{row.get('symbol')}:live={d.get('data_live')} seq={d.get('sequence_verified')} "
                        f"obi={float(d.get('obi') or 0):+.3f} askDep={float(d.get('ask_depletion') or 0):+.3f} "
                        f"book={d.get('reason')}"
                    )
            if parts:
                print("Ψ-V10.10 BOOK_DIAG " + " | ".join(parts[:10]), flush=True)
        except Exception:
            pass
