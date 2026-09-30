import asyncio
from collections import Counter

import app
import qualifier_app as q
import stable10_app as s
import ignition10_app as v7
import ignition1071_app as base
import ignition108_app as v8
import ignition1081_app as v81
import ignition109_app as v9

VERSION = "10.9-latent-cluster-extension-guard"

v9.VERSION = VERSION
v81.VERSION = VERSION
v8.VERSION = VERSION
v7.VERSION = VERSION
base.VERSION = VERSION
app.USER_AGENT = "psi-v10-live-scanner/10.9-latent-cluster-extension-guard"

v81._original_evaluate = v9.evaluate


def evaluate(symbol):
    return v81.evaluate(symbol)


async def health(req):
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "ALL_MAJOR_LAYERS_PLUS_LIVE_EXECUTION_PLUS_CUMULATIVE_EXTENSION_GUARD",
        "latent_ignition": True,
        "activity_price_divergence": True,
        "burst_cluster_windows_seconds": [300, 900, 1800],
        "rolling_buy_pressure_windows_seconds": [30, 60, 180],
        "dynamic_rapid_subscriptions": True,
        "rapid_stream_reconnects": v9.stream_reconnects,
        "rapid_subscription_changes": v9.stream_subscription_changes,
        "rapid_book_bootstraps": v9.stream_bootstraps,
        "trade_count_acceleration_v2": True,
        "fresh_max_24h_pct": v81.FRESH_MAX_PCT,
        "controlled_runner_max_24h_pct": v81.CONTROLLED_RUNNER_MAX_PCT,
        "exceptional_runner_max_24h_pct": v81.EXCEPTIONAL_RUNNER_MAX_PCT,
        "late_runner_rule": "NO_FRESH_BUY_ABOVE_35PCT_UNLESS_NEW_BASE_RESET_CONFIRMED",
        "extension_ticker_connected": v81.extension_ticker_connected,
    }
    return app.web.json_response({
        "ok": True,
        "service": "psi-v10-live-scanner",
        "version": VERSION,
        "scanner_ready": app.scanner_ready,
        "websocket_connected": app.websocket_connected,
        "rapid_websocket_connected": v7.rapid_ws_connected,
        "discovery_ws_connected": q.disc_ws,
        "coverage": c,
        "stable_qualifier_symbols": list(q.stable),
        "near_miss_diagnostics": s.near_diag(10),
        "latent_ignition_top": [x for x in v9._radar_rows(20) if x.get("latent_ignition")][:10],
        "last_error": app.last_error,
    })


async def scan(req):
    try:
        limit = max(1, min(int(req.query.get("limit", v7.TARGET)), v7.TARGET))
    except ValueError:
        limit = v7.TARGET

    app.resolve_outcomes()
    rows = s.results(limit)
    radar = v9._radar_rows(20)
    c = v7.coverage()
    c["decision_engine"] = {
        "version": VERSION,
        "policy": "ALL_MAJOR_LAYERS_PLUS_LIVE_EXECUTION_PLUS_CUMULATIVE_EXTENSION_GUARD",
        "rolling_persistence_seconds": v8.PERSIST_WINDOW,
        "rolling_persistence_required_hits": v8.PERSIST_HITS,
        "latent_ignition": True,
        "activity_price_divergence": True,
        "burst_cluster_windows_seconds": [300, 900, 1800],
        "rolling_buy_pressure_windows_seconds": [30, 60, 180],
        "trade_count_acceleration_v2": True,
        "dynamic_rapid_subscriptions": True,
        "rapid_stream_reconnects": v9.stream_reconnects,
        "fresh_max_24h_pct": v81.FRESH_MAX_PCT,
        "controlled_runner_max_24h_pct": v81.CONTROLLED_RUNNER_MAX_PCT,
        "exceptional_runner_max_24h_pct": v81.EXCEPTIONAL_RUNNER_MAX_PCT,
        "late_runner_rule": "NO_FRESH_BUY_ABOVE_35PCT_UNLESS_NEW_BASE_RESET_CONFIRMED",
        "extension_ticker_connected": v81.extension_ticker_connected,
    }

    return app.web.json_response({
        "ok": True,
        "scanner": "Ψ-V10.9 Latent Ignition + Burst Clustering + Rolling Pressure + Extension Guard",
        "version": VERSION,
        "buy_policy": "ALL_MAJOR_LAYERS_AND_LIVE_EXECUTION_AND_CUMULATIVE_EXTENSION_GUARD",
        "returned": len(rows),
        "state_counts": dict(Counter(x["state"] for x in rows)),
        "coverage": c,
        "results": rows,
        "near_miss_diagnostics": s.near_diag(10),
        "latent_ignition_top": [x for x in radar if x.get("latent_ignition")][:10],
        "ignition_building_top": [x for x in radar if x.get("phase") in ("IGNITION_BUILDING", "IGNITION")][:10],
        "ignition_top": radar[:10],
        "missed_move_events": list(v7.missed_move_events)[-20:],
        "generated_ms": q.ms(),
    })


app.evaluate_symbol = evaluate
app.health = health
app.scan_endpoint = scan
app.ranked_results = s.results

v7.rapid_websocket_loop = v9.persistent_rapid_websocket_loop
v7.print_loop = v9.print_loop
q.print_loop = v9.print_loop
s.print_loop = v9.print_loop
base.ticker_loop = v81.ticker_loop

if __name__ == "__main__":
    try:
        print(
            "Ψ-V10.9 ACTIVE — latent ignition + burst clustering + rolling buy pressure + "
            "trade-acceleration v2 + persistent rapid subscriptions + cumulative extension guard",
            flush=True,
        )
        asyncio.run(v7.main())
    except KeyboardInterrupt:
        print("Ψ-V10.9 stopped", flush=True)
