# Session history

Log of working sessions on this repo. Reconstructed from git history and persisted
Claude memory (no raw chat transcripts were available for the earlier sessions), then
maintained going forward — **append a new entry at the top for each future session**
rather than editing past ones.

---

## 2026-07-11 — Close status-correctness + trade-detail crash fix (uncommitted)
Two bugs found while the user was staging API keys for the newly-added exchanges (BingX,
Bitget, Gate.io, MEXC now credentialed+enabled, but `EnabledConnectors` still Bybit+HTX
only, so none have actually traded yet).

1. **`GET /api/execution/trades/{id}/detail` returned 500.** `ListLegsAsync`/`ListEventsAsync`
   read straight into the positional records `CarryTradeLeg`/`CarryTradeEvent`, forcing
   Dapper's constructor-match path, which throws on the `timestamptz`(DateTime)→
   `DateTimeOffset` mismatch. The working `CarryTrade` reads go through a mutable
   `CarryTradeRow` + `ToModel()`. Fixed by adding `CarryTradeLegRow`/`CarryTradeEventRow`
   the same way. Any click-to-expand on the Trading page hit this.

2. **Trade status lied: `Closing`/`Failed` while exchanges were actually flat.** A reduce-only
   close fired at an already-flat account is rejected by the exchange → leg executor can only
   report "failed to fill" → monitor retried 5× then marked `Failed`, even though the position
   was genuinely gone (the DYDX/TIA incidents). Added `CarryTradeCloseReconciler`: on a close
   failure it reads real positions on both legs and, if both **positively** confirm no position
   for the pair, marks the trade `Closed` (reconciled) instead of Failed. PnL computed only from
   captured exit fills, else null with close_reason "Reconciled: exchanges confirmed flat".
   Guarded by a new `IExchangeTradingClient.SupportsPositionReads` (default false; true only on
   Bybit/HTX) so an unverified connector's empty position stub is never mistaken for "flat".
   New `CarryTradeEventTypes.CloseReconciled`, `ICarryTradeRepository.MarkReconciledClosedAsync`.
   Wired into `CarryTradePositionMonitorWorker` close-failure path; registered in `Program.cs`.

Build + 36 tests green (6 new reconciler tests). Image rebuilt, container healthy, `/detail`
now 200, monitor starts clean. All exchanges confirmed flat; no stranded positions. Changes
uncommitted.

3. **`LockedByError` investigation → HTX leg confirmation-timeout fix.** User armed and hit
   `LockedByError` (SLX-USDT, long HTX / short Bybit: "Long leg failed to fill; short leg
   unwound" — the expected arm-once lock after a failed attempt, no naked leg). Logs showed the
   real cause was a `TaskCanceledException` in `CarryTradeLegExecutor.TryPlaceAsync`: HTX's
   `PlaceOrderAsync` (1 place POST + up to 5 `swap_order_info` polls × 200ms sleep ≈ 1.8–2.8s)
   could not fit the 1500ms `MaxLegDelayMs`, so a filled IOC order was canceled mid-confirmation
   and misreported as unfilled. `MaxLegDelayMs` is really a fill-confirmation-read budget, not a
   market-exposure bound. Fix: `MaxLegDelayMs` 1500→3500 (`ExecutionOptions` + appsettings), HTX
   poll sleep 200→50ms / 5→6 attempts. Rebuilt+restarted; config shows maxLegDelayMs 3500, and
   the restart's startup reset cleared LockedByError→KillSwitch. Uncommitted.

4. **HTX order-volume contract-multiplier bug (100x oversize) fixed.** After the timing fix a
   re-arm hit `LockedByError` again — but now with a REAL HTX error `ErrCode=1047 Insufficient
   margin` on H-USDT (long Bybit / short HTX). Root cause: `HtxTradingClient.PlaceOrderAsync` sent
   the base-asset quantity as HTX order volume, but HTX volume is in CONTRACTS (1 contract =
   contract_size base units). H-USDT `contract_size=100`, so a ~$10 (≈145 base) order was placed
   as 145 contracts ≈ $960. Worked for TIA/DYDX only because their contract_size=1. Two-way bug
   (HTX also reports fills in contracts). Fix: HTX client now converts base↔contracts both
   directions (`HtxContractConversion` helper, contract_size cached), presenting a base-unit
   interface like Bybit; rounds down, throws if below one contract. No naked leg (safety close
   fired, got 1048 "nothing to close"). Build + 44 tests green (8 new conversion tests). Rebuilt,
   healthy, config maxLegDelayMs=3500, state KillSwitch. Uncommitted.

5. **First successful live open+close, then Bybit-leverage fix.** After the HTX contract-size
   fix, a full cycle finally worked: SLX-USDT (long HTX / short Bybit, qty 60) opened 09:02:03,
   manually closed 09:02:21, realized PnL −0.15 USDT — validating the timeout + contract-size
   fixes live. User then flagged Bybit "opens with leverage": confirmed `BybitTradingClient`
   never set leverage, so positions opened at the account default (commonly 10x) while HTX sent
   `lever_rate=1`. Risk: no stop-loss + wider-basis thesis → a leveraged leg can be liquidated
   during divergence. Fix: `EnsureIsolatedOneXLeverageAsync` before opening orders (cached per
   symbol, skipped for closes) — best-effort switch-isolated, mandatory set-leverage 1x
   (fail-closed if it can't be set); tolerates Bybit "not modified" ret-codes. Build + 44 tests
   green, rebuilt healthy. Uncommitted.

Net this session: 5 stacked fixes (detail-crash, close reconciliation, HTX leg
confirmation-timeout, HTX contract-multiplier, Bybit no-leverage) - committed + pushed
(a27ccca, e132fce, 649cb46, c42cb7e). First real open+close cycle succeeded.

6. **Bitget brought up to production trust + live-validated.** Started expanding to the other
   8 connectors, one at a time (user picked Bitget first). Audit found the same two systematic
   gaps as Bybit/HTX: (a) leverage never forced to 1x, (b) `GetPositionsAsync` was an empty
   stub. Fixed in `BitgetTradingClient`: `EnsureOneXLeverageAsync` before opening orders
   (hedge-aware - sets both long+short via set-leverage, cached per symbol, fail-closed);
   real `GetPositionsAsync` via mix/position/all-position + `SupportsPositionReads=true`; fill
   poll 200->50ms. Contract sizing was already correct (Bitget uses base coin, not contracts).
   Live smoke test (EnabledConnectors overridden to [bitget, bybit] via env so trades force
   through Bitget): BTW-USDT long Bybit / short Bitget, qty 160, clean open+close, PnL -0.069.
   Logs confirmed set-leverage fired for long+short with no error, hedge order placed+filled,
   close worked, all flat. Bitget now trusted like Bybit/HTX. Uncommitted.

## 2026-07-05 — Documentation pass (this session)
Analyzed the whole repo (backend architecture, frontend, exchange connectors, execution
pipeline) and created this file plus [CLAUDE.md](CLAUDE.md) and [lessons.md](lessons.md),
consolidating what had previously only lived in Claude's cross-session memory so it's
readable directly from the repo.

## 2026-07-05 00:56 — `b4c93e4` fix: Data Protection key persistence
Fixed a real incident: every `docker stop && rm && run` cycle regenerated the ASP.NET Data
Protection key ring inside the container's ephemeral filesystem, permanently bricking every
previously-saved encrypted exchange API secret in Postgres. Had already bitten the user
twice in practice, requiring Bybit/HTX keys to be re-entered both times. Fix: mount a named
Docker volume at `/app/dataprotection-keys` and point `DataProtection:KeysPath` at it;
Dockerfile creates that directory owned by the non-root `app` user. Verified by saving a
credential, recreating the container, and confirming it still decrypted. Full commands in
[lessons.md](lessons.md).

## 2026-07-05 00:41 — `d7b9bcc` refactor: dashboard tab split + Connectors redesign
Split the single dashboard into `MonitoringPage.tsx` (spreads/signal quality) and
`TradingPage.tsx` (positions/realized PnL) tabs; redesigned the Connectors credential
cards.

## 2026-07-04 23:37 — `8e16a44` feat: real carry-trade execution pipeline
The largest single session so far, done in (at least) two passes on the same day:

**Pass 1 — real execution for Bybit + HTX.** Replaced the dashboard's old fake
"possible income" framing (a backtest-style `notional_usd * net_edge_pct / 100` projection
computed from observed spreads, with no order ever actually placed) with a real
open/close/monitor pipeline that places actual two-leg positions and tracks realized PnL.
Also built the Connectors settings page (previously the `ExchangeCredentialsController`
API existed with zero frontend) and the `ExecutionControls.tsx` arm/kill-switch panel
(previously only reachable via raw curl). Key design decisions made explicitly with the
user, not assumed:
  - No percentage stop-loss — wider divergence is treated as a *stronger* mean-reversion
    signal, not a loss to cut. Only backstop: `MaxHoldMinutes` hard timeout.
  - No leverage, no averaging/top-up in v1, one open position at a time globally.
  (See [lessons.md](lessons.md) for why these were flagged as reversals of an earlier
  unilateral assumption.)

**Pass 2 — remaining 8 exchanges.** User asked to build out Binance, OKX, Bitget, Gate.io,
KuCoin, MEXC, BitMart, BingX to full `PlaceOrderAsync` parity with Bybit/HTX (not just
balance checks), confirmed via explicit follow-up question rather than assumed from a
throwaway "exchange choice doesn't matter" comment. All 10 connectors can now place real
orders, but only Bybit/HTX are considered production-trustworthy — the other 8 are
unverified against live docs (see [lessons.md](lessons.md) for per-exchange risk notes:
fused side-codes on MEXC/BitMart, contract-multiplier sizing on Gate.io/KuCoin/MEXC/
BitMart, hedge-mode assumptions on BingX).

Both passes landed together in this one commit. A UI bug was also caught here: the
Connectors page's "Enabled" checkbox defaulted to unchecked, so the user's first Bybit key
save landed disabled and had to be re-entered.

## 2026-07-02 02:32–05:23 — WsStreamBase refactor (`06b0469`, `b74a68e`, `bc7f520`)
Introduced shared `WsStreamBase`/`BboStreamBase`/`DepthStreamBase` classes and migrated all
10 exchange market-data streams onto them in three steps: pilot on Binance, then
Bybit/OKX/Bitget/GateIo/BitMart, then KuCoin/MEXC/HTX/BingX. Consolidated reconnect and
parsing logic that had previously been duplicated per exchange.

## 2026-07-01 14:28–16:31 — Cleanup, security, tests
Same-day sequence: rotated the Data Protection key ring outside the repo and disarmed
execution as a security fix (`5d7a5d5`), added the `Arbitrage.Api.Tests` xUnit project plus
an order-size rounding utility with tests (`c0fb43e`), removed dead code and consolidated
the instrument filter / DI setup (`a7ca8b4`), and fixed Binance BBO price parsing to use
`InvariantCulture` (`64fc7ed`) — a locale-dependent decimal-parsing bug.

## 2026-05-25 — Baseline
`272eb3d` — initial baseline: live arbitrage scanner across 8 connectors.
`137f362` — stabilized universe filtering and depth parsing on top of that baseline.

---

## Standing context carried across sessions
These are the durable facts every session so far has had to respect — see
[lessons.md](lessons.md) for the full detail:
- This is a real-money trading system; scope changes to execution/risk/credentials get
  confirmed with the user before implementation, not assumed.
- Only Bybit and HTX order placement has any real-world confidence; the other 8 exchanges
  need a live smoke test before being trusted with real capital.
- The local Docker setup has two footguns already hit in practice: the HTTPS port crash
  and the Data Protection key-ring loss (now fixed via a persistent volume).
