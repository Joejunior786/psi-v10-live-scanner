# Ψ-V10 Live Scanner

Read-only Binance Spot market-data service for the Ψ-V10 workflow.

## Current integrated layer

- Binance Spot USDT universe
- 24h liquidity filter
- 4-hour candles
- 4H SMA200 calculated from **200 completed 4H candles**
- 4H SMA50
- percentage distance from SMA200
- ATR(14) distance
- volume acceleration versus the previous 20 completed 4H candles
- taker-buy ratio
- trade count
- ranking by absolute distance from 4H SMA200

### MA states

- `TOUCH`: <= 0.5%
- `ALMOST`: > 0.5% and <= 1.0%
- `APPROACHING`: > 1.0% and <= requested maximum distance

MA proximity alone is deliberately returned as `MA_CANDIDATE`; full Ψ-V10 BUY confirmation
requires the later microstructure layers (OFI/CVD/order-book persistence/anti-chase etc.).

## Endpoints

- `/health`
- `/scan?limit=10&max_distance=2`
- `/docs`

## Railway

The included `Dockerfile` and `railway.toml` are ready for Railway deployment.

No Binance API key is required. This service only reads Binance public market data.
