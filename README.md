# Ψ-V11 Live Scanner

Current integrated production build: **Ψ-V11.0.5.49 — Watchdog Seed Expansion + Depth-Frame Execution**

Read-only Binance Spot market-data scanner with full-universe discovery, deep microstructure analysis, Pinpoint execution authority, Monster breakout detection, pullback monitoring, and fail-closed live-data integrity.

## Active production entrypoint

- `psi_v11_5_entry.py`
- Docker environment: `PSI_SCANNER_VERSION=11.0.5.49`
- Pinpoint remains the sole BUY NOW authority.
- Historical modules keep their own lineage version strings, but the runtime exposes the integrated V11.0.5.49 build.

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


## V11.0.5.49 execution-micro transport repair

The four qualified execution-micro shards now rotate across Binance websocket hosts instead of being pinned to a single endpoint. Each shard uses explicit connect/heartbeat/receive deadlines and reports its active host plus last-message age. Trade and book sequence validators are reset cleanly on a shard reconnect so a replayed first frame cannot permanently poison sequence validity. This changes transport reliability only; PRE/BUY thresholds and Pinpoint authority are unchanged.


## V11.0.5.49 execution-micro subscription repair

Qualified execution shards now open a short `/stream` websocket first and then issue Binance's official `SUBSCRIBE` request for each shard's `aggTrade` and `depth20@100ms` streams. Subscription acknowledgements and server-side subscription errors are logged explicitly. Multi-host failover and reconnect sequence resets from V11.0.5.40 remain active. Signal thresholds are unchanged.


## V11.0.5.49 split execution-micro transport

Qualified aggressive-trade telemetry now reuses the already-stable full-universe Monster aggTrade websocket and forwards the original Binance aggTrade payload into the formal micro engine only for selected micro symbols. The four execution shards are depth20-only. This removes duplicate aggTrade subscriptions, reduces execution-shard stream load, and lets depth reconnect independently without resetting trade sequence continuity. No synthetic order flow is introduced and no PRE/BUY threshold is relaxed.


## V11.0.5.49 depth continuity

Depth20 execution shards preserve already-valid book state across membership/rebalance reconnects. Because each incoming depth20 frame is a complete top-20 snapshot and the formal engine already requires very fresh books, there is no need to zero healthy book state during our own reconnect. If the replacement stream fails, freshness expires naturally and execution still fails closed. This prevents pool growth from repeatedly collapsing live-micro readiness.


## V11.0.5.49 stable execution-depth scheduling

Execution depth shards now enforce a 12-second minimum dwell between membership rebalances. This prevents startup pool growth from reconnecting a shard again before it can accumulate the depth samples required by the formal micro gate. The execution depth host order now prefers `stream.binance.com:9443` and `:443`, with the less reliable data-stream endpoint retained as fallback. Existing freshness, book-sequence, spread, slippage and Pinpoint execution gates remain unchanged.


## V11.0.5.49 connection-aware depth dwell

Execution-depth rebalancing now uses connection time, not only assignment time. After any successful depth websocket connection, the current shard membership is protected for at least 12 seconds so the formal micro engine can accumulate fresh depth samples. If an assigned shard is handshaking or temporarily disconnected, pool membership is frozen for a 30-second settle window rather than being rewritten underneath the reconnect. Freshness and sequence gates remain fail-closed; this changes scheduling continuity only.


## V11.0.5.49 handshake-safe depth membership

An execution depth shard with assigned symbols is now immutable while its websocket is disconnected or still handshaking. The previous 30-second settle window was shorter than some real Binance handshake delays, allowing a shard generation to change before the socket completed and forcing an immediate reconnect on arrival. Pool growth now waits for assigned shards to establish a connection, after which the existing 12-second post-connect dwell applies. All execution gates remain unchanged.


## V11.0.5.49 first-depth readiness gate

Execution-depth scheduler readiness is now based on receipt and successful processing of a real Binance depth20 frame, not websocket-open state. Each shard carries an explicit stream-ready flag tied to its current generation. While an assigned shard has not yet produced a valid depth frame, membership is immutable. Once the first valid frame arrives, a 12-second post-data dwell begins before any rebalance is permitted. This removes the final socket-open / first-frame generation race without changing any trading threshold.


## V11.0.5.49 Watchdog structure recovery

Watchdog now reports a concrete recovery reason instead of only `RECOVERING`: `STRUCTURE_COVERAGE`, `EXECUTION_MICRO`, `MONSTER_SHARDS`, `EXTENSION`, `REST_HEALTH`, or `CONTINUITY`.

When execution-tier structure is critically low or has stopped advancing, Watchdog launches a small execution-priority rescue batch through an independent multi-host Binance REST race rather than repeating the failing WS-API path. The rescue lane prioritises the selected execution pool, then formal PRE/BUY/early candidates; can bootstrap up to two missing raw seeds per cycle; uses 4 normal slots or 6 when fresh structure is below 8; and clears route affinity/cooldowns only for rescued symbols.

The health threshold is unchanged: after startup, Watchdog still requires at least 16 fresh structure symbols plus live execution micro, extension health, continuity, and Monster shard health. All Pinpoint, structure freshness, liquidity, micro and BUY gates remain fail-closed.


## V11.0.5.49 Watchdog seed expansion

The V11.0.5.48 direct-REST rescue was too aggressive: a six-symbol rescue could occupy the structure gates long enough to hit the Watchdog's outer timeout. V11.0.5.49 changes the emergency lane to the proven adaptive structure loader (WS-API first, bounded REST fallback) and removes the outer cancellation timer.

The rescue lane now exists primarily to grow `structureEver`: it bootstraps up to two execution-priority symbols with fewer than three raw timeframe seeds. Two symbols correspond to at most six simultaneous 15m/1h/4h bootstrap requests, matching the structure request capacity. Once seeded, the ordinary FAST recovery loop owns freshness. This fixes the observed condition where FAST recovery repeatedly refreshed the same 11 seeded symbols while the remaining execution pool could never enter the recovery set.
