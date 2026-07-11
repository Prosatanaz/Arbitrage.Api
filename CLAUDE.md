# Arbitrage.Api

Real-money crypto carry-trade / arbitrage system: scans cross-exchange perpetual-futures
spreads across 10 exchanges in real time, validates candidates through a signal-quality
pipeline, and (for Bybit + HTX, with 8 more exchanges wired but unverified) can open and
close actual two-leg positions on live exchange accounts.

**This trades real money on live exchanges when execution is armed and enabled.** Treat
any change to execution/risk logic or exchange connectors as high-stakes — see
[lessons.md](lessons.md).

## Stack
- Backend: ASP.NET Core (.NET 8), `Program.cs` as the composition root (no separate
  `Startup.cs`). Dapper + Npgsql against Postgres (raw SQL, no EF Core).
- Frontend: React + TypeScript + Vite, in `frontend/`.
- Deploy target: Docker (see `Dockerfile`, and [lessons.md](lessons.md) for the working
  local run commands — `docker-compose.yml` only stands up Postgres, not the API).

## Architecture

### Market data pipeline
- `Infrastructure/MarketData/Streams/<Exchange>/` — one WebSocket client per exchange
  (Binance, BingX, BitMart, Bitget, Bybit, GateIo, Htx, KuCoin, Mexc, OKX), each producing
  BBO (best bid/offer) and order-book depth updates.
- All exchange streams are built on shared base classes: `WsStreamBase.cs`,
  `BboStreamBase.cs`, `DepthStreamBase.cs` (`Infrastructure/MarketData/Streams/`) — this is
  the result of a refactor (see git history `06b0469`..`bc7f520`); new exchanges or stream
  fixes should extend these bases rather than reimplementing connection/reconnect logic.
- `Application/MarketData/Depth`, `Application/MarketData/Opportunities`,
  `Application/MarketData/Universe` — depth tracking, opportunity/spread detection, and the
  tradeable-instrument universe.
- `Application/SignalQuality/SignalQualityService.cs` — quality-gates raw spread candidates
  before they become "validated opportunities."
- `LatestValidatedOpportunityStore` (`Application/MarketData/Opportunities/`) — the
  in-memory hand-off point: validated candidates land here and the execution side watches it.

### Execution / carry-trade pipeline
- `Infrastructure/HostedServices/CarryTradeEntryWorker` — watches
  `LatestValidatedOpportunityStore`, opens a position via `ExecutionGateService`
  (arm/kill-switch) when a valid candidate clears `MinEntryNetEdgePct`.
- `Application/Execution/CarryTrades/CarryTradeLegExecutor` — shared open/close protocol:
  fires both legs in parallel, reconciles fills (trims the more-filled leg to match the
  less-filled one — never chases), fully unwinds on total leg failure.
- `CarryTradePositionMonitorWorker` — always runs regardless of kill switch (closing risk
  must never be blockable). Two exit triggers only: take-profit (debounced) and
  `MaxHoldMinutes` timeout (hard force-close). **No percentage stop-loss** — see
  [lessons.md](lessons.md) for why.
- `Application/Execution/CarryTrades/CarryTradePnlCalculator.cs` — realized PnL:
  `(exitLong-entryLong)*qty - (exitShort-entryShort)*qty - fees`.
- `Infrastructure/Execution/Trading/<Exchange>/` — one `PlaceOrderAsync`-style trading
  client per exchange. All 10 now implement real order placement, but only **Bybit and HTX
  have been treated as production-trustworthy**; the other 8 (Binance, OKX, Bitget, Gate.io,
  KuCoin, MEXC, BitMart, BingX) were written from general API knowledge and are unverified
  against live docs/sandbox — see [lessons.md](lessons.md) for per-exchange risk notes
  before touching them.
- `carry_trades` Postgres table / `ICarryTradeRepository` /
  `Infrastructure/Persistence/Postgres/PostgresCarryTradeRepository` — trade history +
  realized PnL persistence.
- `Application/Execution/Credentials/` + `ExchangeCredentialsController` — encrypted
  storage of per-exchange API keys (ASP.NET Data Protection), decoupled from
  `ExecutionOptions.EnabledConnectors` so keys can be pre-staged before a connector's
  trading client is production-ready.

### Controllers (`Controllers/`)
`AnalyticsController`, `DepthController`, `ExchangeCredentialsController`,
`ExecutionController`, `InstrumentsController`, `MarketDataStreamingController`,
`OpportunitiesController`, `SignalQualityController`, `SignalsController`,
`SpreadController`, `SystemHealthController`. Note: realized-PnL summary lives at
`GET /api/analytics/opportunities/realized/summary` (nested under `/opportunities`, not
`/api/analytics/realized/summary` — this exact mismatch has caused a live bug before).

### Frontend (`frontend/src/`)
- `MonitoringPage.tsx` / `TradingPage.tsx` — dashboard split into monitoring (spreads,
  signal quality) vs. trading (open positions, realized PnL) tabs.
- `ConnectorsPage.tsx` — exchange API credential management (all 10 exchanges shown, even
  though only some can currently place real orders).
- `ExecutionControls.tsx` — arm/disarm and kill-switch UI for the execution engine.

## Tests
`Arbitrage.Api.Tests/` — xUnit project (added `c0fb43e`). `InternalsVisibleTo` is set on
the main project so tests can reach internals directly.

## Running locally
See [lessons.md](lessons.md) for the exact, previously-verified Docker run commands and
gotchas (HTTPS port crash, Data Protection key persistence, Windows Git Bash path
mangling, Postgres connection-string key name).

## Working conventions
- No EF Core — Dapper + raw SQL against Postgres throughout; keep new persistence code
  consistent with `Infrastructure/Persistence/Postgres/`.
- New exchange connectors (both market-data stream and trading client) should follow the
  existing per-exchange folder pattern and extend `WsStreamBase`/`BboStreamBase`/
  `DepthStreamBase` rather than duplicating reconnect/parsing logic.
- Any change touching real order placement, risk/exit logic, or credential handling is a
  scope question first — read [lessons.md](lessons.md) before proceeding.
