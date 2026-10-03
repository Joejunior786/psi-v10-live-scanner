# Ψ-V11 Live Scanner

Current integrated production build: **Ψ-V11.0.5.37 — REST-First RiskMap + Integrity Sync**

Read-only Binance Spot market-data scanner with full-universe discovery, deep microstructure analysis, Pinpoint execution authority, Monster breakout detection, pullback monitoring, and fail-closed live-data integrity.

## Active production entrypoint

- `psi_v11_5_entry.py`
- Docker environment: `PSI_SCANNER_VERSION=11.0.5.37`
- Pinpoint remains the sole BUY NOW authority.
- Historical modules keep their own lineage version strings, but the runtime exposes the integrated V11.0.5.37 build.

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

RiskMap refresh is hard-bounded and REST-first: a two-host Binance public REST race is attempted first, followed by one short WS-API fallback. The 1m/5m/15m build returns at a fixed internal deadline without waiting for slow cancellation cleanup. The outer scheduler has no wait_for cancellation, and it only schedules genuine live Pinpoint/Monster/live-micro candidates rather than arbitrary cold-start symbols. A valid 5m result can still produce a structural plan when 1m/15m are delayed.

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
