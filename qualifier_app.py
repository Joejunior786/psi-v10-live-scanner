import asyncio

import app

# -----------------------------------------------------------------------------
# Qualifier-only runtime policy
# -----------------------------------------------------------------------------
# Keep the core signal engine unchanged. This wrapper controls universe breadth,
# result filtering, and the runtime entrypoint so Railway and Docker always use
# the same source-controlled policy.

# Conservative Binance request/runtime limits to reduce 418/429 risk.
app.TOP_STRUCTURE_UNIVERSE = min(app.TOP_STRUCTURE_UNIVERSE, 60)
app.MICRO_UNIVERSE_SIZE = min(app.MICRO_UNIVERSE_SIZE, 20)
app.ANOMALY_PROMOTION_SLOTS = min(app.ANOMALY_PROMOTION_SLOTS, 20)
app.ANOMALY_REFRESH_SECONDS = max(app.ANOMALY_REFRESH_SECONDS, 60)
app.STRUCTURE_REFRESH_SECONDS = max(app.STRUCTURE_REFRESH_SECONDS, 900)
app.MICRO_POOL_MIN_HOLD_SECONDS = max(app.MICRO_POOL_MIN_HOLD_SECONDS, 300)

QUALIFIER_STATES = ("BUY NOW", "PRE-IGNITION")
QUALIFIER_TARGET = 10
QUALIFIER_POLICY = "BUY_PRE_ONLY_NO_PADDING_NO_THRESHOLD_RELAXATION"


def qualifier_results(limit: int = QUALIFIER_TARGET):
    """Return only genuine BUY NOW / PRE-IGNITION candidates.

    This function never fills empty slots with WATCH/REJECT candidates and never
    relaxes the underlying signal thresholds to reach the requested target.
    """
    rows = []
    for symbol in list(app.structure):
        try:
            row = app.evaluate_symbol(symbol)
        except Exception:
            continue
        if row and row.get("state") in QUALIFIER_STATES:
            rows.append(row)

    rows.sort(
        key=lambda row: (
            2 if row.get("state") == "BUY NOW" else 1,
            float(row.get("score", 0) or 0),
            float(row.get("quote_volume_24h", 0) or 0),
        ),
        reverse=True,
    )
    requested = max(1, min(int(limit or QUALIFIER_TARGET), QUALIFIER_TARGET))
    return rows[:requested]


app.ranked_results = qualifier_results
app.QUALIFIER_TARGET = QUALIFIER_TARGET
app.QUALIFIER_STATES = QUALIFIER_STATES
app.QUALIFIER_POLICY = QUALIFIER_POLICY


if __name__ == "__main__":
    print(
        "QUALIFIER POLICY ACTIVE: BUY NOW/PRE-IGNITION ONLY; TARGET=10; NO PADDING",
        flush=True,
    )
    asyncio.run(app.main())
