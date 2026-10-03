# Ψ-V11 Live Scanner

Current integrated production build: **Ψ-V11.0.5.41 — Execution Micro Live-Subscribe + Reliable RiskMap**

Read-only Binance Spot market-data scanner with full-universe discovery, deep microstructure analysis, Pinpoint execution authority, Monster breakout detection, pullback monitoring, and fail-closed live-data integrity.

## Active production entrypoint

- `psi_v11_5_entry.py`
- Docker environment: `PSI_SCANNER_VERSION=11.0.5.41`
- Pinpoint remains the sole BUY NOW authority.
- Historical modules keep their own lineage version strings, but the runtime exposes the integrated V11.0.5.41 build.

## Live integrity rules

Elevated execution states use the same native micro readiness source as the formal evaluator.

- Structure freshness: <= 120s
- Native aggTrade/depth trade freshness: <= 15s
- Native order-book freshness: <= 5s
- Trade and book sequence validation required
- Fast Monster event tape is only mandatory for Monster states that depend on it
- PRE-IGNITION/HOT/IGNITION/BUY fail closed if mandatory live inputs are missing
- Pinpoint/formal/reporting state aliases are synchronised so stale raw PRE states cannot leak into the live board

## BUY NOW risk authority

A BUY NOW candidate must still satisfy the full Pinpoint execution gate.

A valid live Pinpoint trigger/stop/risk plan is accepted as the primary execution-risk plan. The separate RiskMap is retained as an additional structural/fallback plan source and no longer makes BUY mathematically impossible when its cache is temporarily unavailable.

RiskMap refresh is hard-bounded and WS-first: one realistically timed Binance WS-API request is attempted per timeframe with a short queue deadline, followed by a two-host public REST fallback. Only one live candidate is built per scheduler cycle, while 1m/5m/15m still fetch concurrently. The outer scheduler has no wait_for cancellation and never spends capacity on arbitrary cold-start symbols. Raced REST child exceptions are explicitly drained, and Watchdog reports actual RiskMap tracked-plan counts plus raw timeframe-cache size/hits. A valid 5m result can still produce a structural plan when 1m/15m are delayed.

## Scanner lanes

- BUY NOW
- PRE-IGNITION
- MONSTER HOT / IGNITION / RESCUE / WATCH
- Pullback exhaustion and trend pullback
- Early Opportunity / WATCH
- Full-universe discovery and rotating deep analysis
- MISSED MOVE / empirical shadow learning diagnostics

## Core telemetry

- Binance Spot USDT universe
- EMA/SMA structure and retests
- 1H/4H moving-average context
- ATR and compression
- OFI / CVD / aggressive-buy flow
- L1-L10 / BBO order-book pressure
- VWAP and breakout positioning
- trade-count, notional and average trade-size acceleration
- anti-chase and false-break controls
- execution persistence
- risk/stop/target plans

## Railway

The included `Dockerfile` and `railway.toml` deploy the current V11 entrypoint.

No Binance API key is required for the public market-data feeds used by this service.


## V11.0.5.41 execution-micro transport repair

The four qualified execution-micro shards now rotate across Binance websocket hosts instead of being pinned to a single endpoint. Each shard uses explicit connect/heartbeat/receive deadlines and reports its active host plus last-message age. Trade and book sequence validators are reset cleanly on a shard reconnect so a replayed first frame cannot permanently poison sequence validity. This changes transport reliability only; PRE/BUY thresholds and Pinpoint authority are unchanged.


## V11.0.5.41 execution-micro subscription repair

Qualified execution shards now open a short `/stream` websocket first and then issue Binance's official `SUBSCRIBE` request for each shard's `aggTrade` and `depth20@100ms` streams. Subscription acknowledgements and server-side subscription errors are logged explicitly. Multi-host failover and reconnect sequence resets from V11.0.5.40 remain active. Signal thresholds are unchanged.
