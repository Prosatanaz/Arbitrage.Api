# Lessons learned

Accumulated gotchas and hard-won rules for working on this repo. Read before touching
execution/risk logic, exchange connectors, or the local Docker setup.

## Real-money scope discipline

This project places real orders with real capital on live exchanges once execution is
armed and a connector is enabled. Confirm scope explicitly (ask, don't assume) before:
- expanding real order-execution to a new exchange,
- changing risk-control logic (stop-loss, averaging, position sizing, exit triggers),
- or otherwise touching anything that moves real money.

**Why this rule exists:** mid-build, two unilateral assumptions had to be reversed after
the fact:
1. An exit/stop-loss model was assumed (percentage stop-loss) that was backwards from the
   actual thesis: a *wider* cross-exchange divergence is mean-reverting and therefore a
   stronger signal, not a loss to cut. The only backstop wanted is a max-hold-time timeout
   (`MaxHoldMinutes`, hard force-close, no debounce) — there is deliberately no percentage
   stop-loss.
2. Real execution was built for Bybit+HTX only per an earlier answer; a later "exchange
   choice doesn't matter" comment turned out to imply all 10, but scope was actually:
   credential-storage UI for all 10, real execution expansion as separate, later work — not
   "build everything now." This was caught by asking before implementing, not by guessing.

**Rule of thumb:** treat "just make it trade on X" or "add exchange Y" as a scope question
first, especially when it implies writing a new `PlaceOrderAsync`-equivalent (auth signing,
symbol rules, precision handling) — each exchange is comparable effort to the last one
done, not a small add. When the user describes a trading rule in their own words that
contradicts something already built, stop and re-derive the model numerically with them
before writing code.

Other scope decisions already made explicitly (don't relitigate without asking):
- No leverage → no per-exchange liquidation/margin monitoring.
- No averaging/top-up in v1 — only open, hold, close-on-convergence-or-timeout.
- One open position at a time, globally.

## Unverified exchange connectors — flag before trusting

Bybit and HTX are the only trading clients treated as production-trustworthy so far (and
even HTX's order-placement field names — `trade_volume`, `trade_avg_price`, `fee`,
`client_order_id`, `order_price_type="optimal_20_ioc"` — were written from general API
knowledge, not confirmed live). The other 8 (Binance, OKX, Bitget, Gate.io, KuCoin, MEXC,
BitMart, BingX) are equally unverified. Do a real smoke test with tiny notional before
trusting fills on any of them. Known highest-risk spots:

- **Gate.io** (`Infrastructure/Execution/Trading/GateIo/GateIoTradingClient.cs`): orders
  sized in *contracts*, converted from base-asset qty via `quanto_multiplier` — a wrong
  multiplier reading places a wildly wrong-sized order. Per-trade fee not wired up (always
  reports 0).
- **KuCoin** (`.../KuCoin/KuCoinTradingClient.cs`): same contract-multiplier risk
  (`multiplier` field), plus KuCoin renames BTC to "XBT" (`XBTUSDTM`) — only BTC is
  special-cased; verify no other asset needs the same treatment.
- **MEXC** (`.../Mexc/MexcTradingClient.cs`): `side` is a single fused code (1=open long,
  2=close short, 3=open short, 4=close long) instead of separate side/reduceOnly — highest
  risk spot in the whole batch. Also contract-multiplier sizing via `contractSize`.
- **BitMart** (`.../BitMart/BitMartTradingClient.cs`): same fused open/close side-code
  pattern as MEXC, plus requires a "memo" (stored in the connector's `Passphrase` field)
  signed into every request. Contract-multiplier sizing via `contract_size`.
- **BingX** (`.../BingX/BingXTradingClient.cs`): assumes hedge mode is on (default for new
  API keys) — every order needs both `side` (BUY/SELL) and `positionSide` (LONG/SHORT),
  reduceOnly implied by the combination. Quantity is base-asset units directly (no contract
  multiplier), unlike Gate.io/KuCoin/MEXC/BitMart.
- **Binance, OKX, Bitget**: closest to Bybit/HTX's style (well-documented, string-encoded
  decimals, direct base-asset quantities) — comparatively lower risk, still unverified.
- `Infrastructure/Execution/Trading/FlexibleNumericStringConverter.cs` exists because some
  APIs (KuCoin, MEXC) are suspected to return raw JSON numbers instead of the
  decimal-as-string convention Bybit/OKX/HTX use — it accepts either JSON *shape*, but does
  not fix a wrong field *name*.
- Funding-rate PnL is not included in realized PnL (known v1 limitation).

## First live-order incident (2026-07-11) — confirmed findings

First real armed attempts (Mode=ArmedOneShot, $10 notional, Bybit/HTX/Bitget/BingX enabled)
placed real orders and exposed concrete client bugs. Safety machinery (gate lock, kill
switch, one-shot arm) behaved correctly throughout — the failures were all in the exchange
clients. What was actually learned:

- **Bybit** is the only client proven to place + unwind real orders cleanly.
- **HTX `swap_order_info` (fill-check) is a POST endpoint** — it was being called with GET,
  which returns HTTP 405. A successfully-placed HTX order then looked "unfilled", so the
  leg executor unwound only the opposite (Bybit) leg and **left a real naked HTX short open**
  that had to be closed manually. Fixed: POST with `{contract_code, order_id}` in the body.
- **HTX positions can be isolated OR cross** — `swap_cross_position_info` alone missed an
  isolated position (so the naked leg was invisible on the Accounts page). Now querying both
  `swap_cross_position_info` and `swap_position_info` and merging.
- **Bitget rejects orders with code 40774 when the account is in hedge (two-way) mode** but
  the client sends one-way params. Hedge mode needs `side` + `tradeSide` (open/close); a
  close uses the position's OWN side (close long = side buy + tradeSide close), not the
  opposite side. Added `BitgetTrading.PositionMode` (default "hedge"). Still unverified live.
- **BingX never even executes** if its market-data BBO stream produces 0 snapshots — it's
  never selected as a candidate leg, so it can't be smoke-tested until its stream works.

**Systemic lesson — never treat an EXCEPTION during order placement as "unfilled".** An
exception means UNKNOWN state (the order may have placed and filled). Blindly unwinding the
opposite leg on such an error is what strands a naked leg. `CarryTradeLegExecutor` now fires
a **reduce-only safety close** on any leg that threw: it's a harmless no-op if the account is
flat and closes the position if one opened — guaranteeing flatness without relying on the
(per-exchange, often-unverified) position-read code. This is the right pattern for any new
connector too.

## Bybit opened at the account's default leverage — "no leverage" was not enforced (2026-07-11)

`BybitTradingClient.PlaceOrderAsync` placed a Market IOC order but never set leverage, so real
positions opened at whatever the Bybit account defaults to (commonly 10x) - while HTX orders
correctly sent `lever_rate=1`. The carry-trade design is explicitly no-leverage, and for a good
reason: there is no stop-loss and the thesis treats a WIDER basis as a stronger signal, so a
leveraged leg can be **liquidated during divergence**, turning the hedge into a naked position and
a large realized loss. Fix: `EnsureIsolatedOneXLeverageAsync` runs before every opening order
(cached per symbol, skipped for reduce-only closes): best-effort `switch-isolated` (tradeMode=1,
account-level on Unified accounts so failure is logged not fatal) then a **mandatory**
`set-leverage` buy=1/sell=1 - if 1x cannot be set the order is NOT placed (fail closed). Both
tolerate Bybit's "not modified" ret-codes (110026 margin mode, 110043 leverage) as success.
**Lesson: leverage/margin mode is a per-exchange setting that must be explicitly forced, not
assumed - check it for every connector that can open real positions.**

Live result (2026-07-11): the user's Bybit account is a **Unified Trading Account (UTA)**, where
`v5/position/switch-isolated` returns `RetCode=100028 "unified account is forbidden"` (margin mode
is account-level, not per-symbol). The best-effort catch handled it and `set-leverage` buy=1/sell=1
then SUCCEEDED - so positions now open at **1x in cross margin**. At 1x, cross vs isolated barely
changes liquidation risk (position is fully collateralized). If true isolation is ever wanted on a
UTA account, it needs the account-level `v5/account/set-margin-mode` (setMarginMode=ISOLATED_MARGIN,
requires the whole account flat), not switch-isolated - deliberately NOT done since 1x already
removes the risk.

## HTX order volume is in CONTRACTS, not base units — the "trusted" client had a 100x bug (2026-07-11)

The contract-multiplier trap flagged for Gate.io/KuCoin/MEXC/BitMart **also applied to HTX**,
which had been treated as production-trustworthy. `HtxTradingClient.PlaceOrderAsync` did
`volume = (long)request.Quantity`, passing the base-asset quantity straight through as HTX's
order volume — but HTX volume is denominated in **contracts**, where 1 contract = `contract_size`
base-asset units. It only worked for TIA/DYDX because their `contract_size = 1`. On **H-USDT**
(`contract_size = 100`, price ~$0.0686) a ~$10 order (≈145 base units) was placed as 145
*contracts* = 14,500 H ≈ **$960** → HTX rejected it `ErrCode=1047 Insufficient margin available`.
Two-way bug: HTX also **reports fills in contracts** (`trade_volume`), so with contract_size≠1 the
leg executor would have compared HTX contracts against Bybit base units and mis-reconciled the
legs. Fix: the HTX client now converts both directions (`HtxContractConversion.BaseToContracts` /
`ContractsToBase`, contract_size cached per contract_code) so it presents a base-asset-unit
interface like Bybit. Rounds down; a request below one whole contract throws rather than distort
the hedge. **Lesson: verify order-volume units (base vs contract) for EVERY connector, including
the two "trusted" ones — `contract_size = 1` hides the bug until a pair with a real multiplier
comes along.** Granularity note: with a large contract_size, small notionals quantize hard
(H-USDT: 1 contract ≈ $6.86, so a $10 cap = 1 contract).

## Close status must reconcile against real positions (2026-07-11)

A reduce-only close order fired at an account that is **already flat** is REJECTED by the
exchange (it throws), which the leg executor can only surface as "failed to fill". Treating
that as a genuine close failure made the monitor retry 5× and then mark the trade `Failed` —
a **status that lies**, because the position was actually gone (the DYDX stuck-`Closing` and
TIA `Failed` incidents were both this). "Failed to fill" on a *close* is ambiguous: it means
either a real position we couldn't close (dangerous) OR nothing to close (benign) — you cannot
tell without reading positions.

Fix pattern (`CarryTradeCloseReconciler`): on any close failure, read live positions on both
legs; if **both** connectors positively confirm no position for the pair, mark the trade
`Closed` (reconciled, PnL from captured fills or null) instead of Failed. Never infer "flat"
from a connector that can't really read positions — `IExchangeTradingClient.SupportsPositionReads`
is default-false and only Bybit/HTX set it true, because the other 8 clients' `GetPositionsAsync`
is an empty stub and an empty list would otherwise be misread as "flat" and hide a naked leg.
A position-read exception is NOT evidence of flatness → decline and keep the retry/Failed path.

## Dapper + records: reads go through a mutable `*Row` DTO, never straight into the record

`timestamptz` comes back as `System.DateTime`; a positional record with a `DateTimeOffset`
member makes Dapper's constructor-matching fail ("A parameterless default constructor or one
matching signature is required"). The property-setter path is lenient and converts fine, so
every Postgres read in this repo materializes a mutable `CarryTradeRow`-style class and calls
`.ToModel()`. `ListLegsAsync`/`ListEventsAsync` were reading straight into `CarryTradeLeg`/
`CarryTradeEvent` records and 500'd the `/detail` endpoint until given `*Row` DTOs too.

**Two-leg open failures were a confirmation-timeout bug, not real "won't fill" (2026-07-11).**
Every failed open logged "Long leg failed to fill; short leg unwound" with a
`TaskCanceledException` out of `CarryTradeLegExecutor.TryPlaceAsync`. Root cause: `TryPlaceAsync`
wraps the WHOLE `PlaceOrderAsync` (placement **and** fill confirmation) in one `MaxLegDelayMs`
timeout. HTX's `PlaceOrderAsync` = 1 place POST + up to 5 `swap_order_info` polls with
`Task.Delay(200)` between them ≈ 1.8–2.8s worst case, so it could not fit in the old 1500ms
budget: a filled IOC order got its confirmation HTTP call canceled mid-flight and was reported
as unfilled → the opposite (Bybit) leg was unwound and the HTX leg safety-closed. The order had
almost certainly filled on the exchange; we just couldn't read it in time. Key mental model:
**`MaxLegDelayMs` is a fill-CONFIRMATION-read budget, not a market-exposure bound** — the
`optimal_20_ioc` legs are terminal on the exchange near-instantly. Fix: raised `MaxLegDelayMs`
1500→3500 and cut HTX's poll sleep 200→50ms (6 attempts). If you add/verify another connector,
check its worst-case place+poll chain fits inside `MaxLegDelayMs`.

**Viability metric:** the real question is still `count(EntryFilled)/count(EntrySignal)` over
`carry_trade_events` — but measure it only AFTER the confirmation budget is adequate, or you are
measuring your own timeout, not the market.

## Local Docker run

`docker-compose.yml` only defines the `arbitrage-postgres` service — it does **not** run
the API. Don't assume `docker compose up` starts the backend.

Working local backend setup:
```bash
docker build -t arbitrageapi:local -f Dockerfile .
MSYS_NO_PATHCONV=1 docker run -d --name arbitrage-api-local \
  -p 8080:8080 \
  -e ASPNETCORE_URLS=http://+:8080 \
  -e ASPNETCORE_ENVIRONMENT=Development \
  -e Postgres__ConnectionString="Host=host.docker.internal;Port=5432;Database=arbitrage;Username=arbitrage;Password=arbitrage;GSS Encryption Mode=Disable" \
  -e DataProtection__KeysPath=/app/dataprotection-keys \
  -v arbitrage-dataprotection-keys:/app/dataprotection-keys \
  --add-host=host.docker.internal:host-gateway \
  arbitrageapi:local
```

- **HTTP-only is intentional.** The published image has no dev HTTPS cert — adding
  `ASPNETCORE_HTTPS_PORTS=8081` without a mounted real cert crashes the whole host
  (`Unable to configure HTTPS endpoint`).
- The app reads Postgres config from `Postgres:ConnectionString` (`appsettings.json`), NOT
  the standard `ConnectionStrings:ArbitrageDb` key. `Properties/launchSettings.json`'s
  Docker profile sets the wrong key *and* has a stale Postgres port (5433 vs. the real
  5432) — don't trust that launch profile, use the env var above.
- **`-v arbitrage-dataprotection-keys:/app/dataprotection-keys` is mandatory, not
  optional.** Without it, every container recreation regenerates the ASP.NET Data
  Protection key ring from scratch, permanently bricking every previously-saved encrypted
  exchange API secret in Postgres (`SecretDecryptFailed`). This happened twice in real use
  (2026-07-04) before the fix landed (commit `b4c93e4`) and required re-entering
  Bybit/HTX keys both times. The Dockerfile creates `/app/dataprotection-keys` owned by the
  non-root `app` user specifically so this mount has somewhere writable to land.
- **`MSYS_NO_PATHCONV=1` is required on Windows Git Bash** whenever a `docker run`/`docker
  exec` command has an absolute-looking Unix path in `-e VAR=/path` or `-v name:/path` —
  otherwise Git Bash's MSYS layer mangles `/app/dataprotection-keys` into a Windows path
  and the container crashes on startup.

Frontend: `cd frontend && npm run dev`. Vite proxies `/api` to `VITE_API_TARGET` in
`.env.local` (currently `http://localhost:8080`, matching the container above).

Sanity check:
```bash
curl http://localhost:8080/api/system/health
curl http://localhost:8080/api/execution/credentials
curl http://localhost:8080/api/execution/trades
curl http://localhost:8080/api/analytics/opportunities/realized/summary
```

Stray containers `Arbitrage.Api` / `Arbitrage.Api_1` are Visual Studio Docker-debug
artifacts (volume-mount local build output, break if not launched via VS's debugger) — not
`arbitrage-api-local`. Leave them alone unless asked to clean up.

## UI / API gotchas

- The Connectors page "Enabled" checkbox previously defaulted to unchecked for new
  credentials — a first-time key save can silently land with `isEnabled:false`. There is no
  separate enable/disable-only endpoint; `UpsertAsync` always requires resending
  ApiKey+ApiSecret, so a mis-saved credential must be fully re-entered (fixed in the UI
  since, but re-check if this class of bug resurfaces).
- Realized-PnL summary endpoint is nested at
  `/api/analytics/opportunities/realized/summary`, not `/api/analytics/realized/summary` —
  this exact mismatch has caused a live bug once already.
