using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Arbitrage.Api.Tests.Application.Execution.CarryTrades;

public class CarryTradeCloseReconcilerTests
{
    private static readonly ExchangeApiCredentialSecret Credentials =
        new("test", "key", "secret", null, true);

    [Fact]
    public async Task BothLegsFlat_MarksClosed_WithNullPnlWhenNoFillsCaptured()
    {
        var longClient = FlatClient("bybit_perpetual");
        var shortClient = FlatClient("htx_perpetual");
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "Manual",
            exitNetEdgePct: null,
            legResult: FailedClose(longFill: null, shortFill: null),
            CancellationToken.None);

        Assert.True(reconciled);
        Assert.True(repository.ReconciledClosed);
        Assert.Null(repository.ReconciledPnl);
        Assert.Null(repository.ReconciledFees);
        Assert.Contains(CarryTradeEventTypes.CloseReconciled, repository.RecordedEventTypes);
    }

    [Fact]
    public async Task BothLegsFlat_WithCapturedFills_RecordsRealizedPnl()
    {
        var longClient = FlatClient("bybit_perpetual");
        var shortClient = FlatClient("htx_perpetual");
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        // Both exit legs actually filled (partial-close race) - PnL should be computed, not null.
        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "TakeProfit",
            exitNetEdgePct: 0.1m,
            legResult: FailedClose(
                longFill: Fill("bybit_perpetual", filledQty: 24m, avgPrice: 0.41m, fee: 0.01m),
                shortFill: Fill("htx_perpetual", filledQty: 24m, avgPrice: 0.40m, fee: 0.01m)),
            CancellationToken.None);

        Assert.True(reconciled);
        Assert.True(repository.ReconciledClosed);
        Assert.NotNull(repository.ReconciledPnl);
        Assert.NotNull(repository.ReconciledFees);
    }

    [Fact]
    public async Task PositionStillOpen_DoesNotReconcile()
    {
        var longClient = ClientWithPosition("bybit_perpetual", "TIAUSDT", size: 24m);
        var shortClient = FlatClient("htx_perpetual");
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "Manual",
            exitNetEdgePct: null,
            legResult: FailedClose(null, null),
            CancellationToken.None);

        Assert.False(reconciled);
        Assert.False(repository.ReconciledClosed);
    }

    [Fact]
    public async Task PositionSymbolInExchangeNativeForm_StillMatchesCanonicalPair()
    {
        // Bybit reports "TIAUSDT"; the trade pair is "TIA-USDT". Normalization must still see the
        // position, so reconciliation must decline (a real position remains).
        var longClient = ClientWithPosition("bybit_perpetual", "TIAUSDT", size: 24m);
        var shortClient = ClientWithPosition("htx_perpetual", "TIA-USDT", size: 24m);
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "Manual",
            exitNetEdgePct: null,
            legResult: FailedClose(null, null),
            CancellationToken.None);

        Assert.False(reconciled);
        Assert.False(repository.ReconciledClosed);
    }

    [Fact]
    public async Task ConnectorWithoutPositionReads_DeclinesEvenWhenEmpty()
    {
        // An unverified connector's GetPositionsAsync stub returns empty - which must NOT be trusted
        // as "flat". Reconciliation must decline so the safe retry/Failed path is kept.
        var longClient = FlatClient("bybit_perpetual", supportsPositionReads: false);
        var shortClient = FlatClient("htx_perpetual");
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "Manual",
            exitNetEdgePct: null,
            legResult: FailedClose(null, null),
            CancellationToken.None);

        Assert.False(reconciled);
        Assert.False(repository.ReconciledClosed);
    }

    [Fact]
    public async Task PositionReadThrows_DoesNotReconcile()
    {
        var longClient = ThrowingClient("bybit_perpetual");
        var shortClient = FlatClient("htx_perpetual");
        var repository = new FakeRepository();

        var reconciler = CreateReconciler(repository, longClient, shortClient);

        var reconciled = await reconciler.TryReconcileClosedAsync(
            Trade(),
            Credentials,
            Credentials,
            reason: "Manual",
            exitNetEdgePct: null,
            legResult: FailedClose(null, null),
            CancellationToken.None);

        Assert.False(reconciled);
        Assert.False(repository.ReconciledClosed);
    }

    private static CarryTradeCloseReconciler CreateReconciler(
        ICarryTradeRepository repository,
        params IExchangeTradingClient[] clients)
    {
        var registry = new ExchangeTradingClientRegistry(clients);

        return new CarryTradeCloseReconciler(
            registry,
            repository,
            NullLogger<CarryTradeCloseReconciler>.Instance);
    }

    private static CarryTrade Trade()
    {
        return new CarryTrade(
            Id: Guid.NewGuid(),
            AttemptId: Guid.NewGuid(),
            TradingPair: "TIA-USDT",
            LongConnector: "bybit_perpetual",
            ShortConnector: "htx_perpetual",
            NotionalUsd: 10m,
            BaseQuantity: 24m,
            EntryLongPrice: 0.40m,
            EntryShortPrice: 0.41m,
            EntryFeesUsd: 0.01m,
            EntryNetEdgePct: 0.5m,
            EntryGrossSpreadPct: null,
            EntryEstimatedFeesPct: null,
            EntryReferencePrice: null,
            OpenedAt: DateTimeOffset.UtcNow.AddMinutes(-5),
            Status: CarryTradeStatus.Closing,
            ExitLongPrice: null,
            ExitShortPrice: null,
            ExitFeesUsd: null,
            ExitNetEdgePct: null,
            CloseReason: null,
            ClosedAt: null,
            RealizedPnlUsd: null,
            Error: null);
    }

    private static LegExecutionResult FailedClose(OrderFillResult? longFill, OrderFillResult? shortFill)
        => new(false, 0m, longFill, shortFill, "Both legs failed to fill.");

    private static OrderFillResult Fill(string connector, decimal filledQty, decimal avgPrice, decimal fee)
        => new(connector, "coid", "oid", true, filledQty, avgPrice, fee, "Filled", DateTimeOffset.UtcNow);

    private static FakePositionClient FlatClient(string connector, bool supportsPositionReads = true)
        => new(connector, supportsPositionReads, () => []);

    private static FakePositionClient ClientWithPosition(string connector, string symbol, decimal size)
        => new(connector, true, () =>
        [
            new ExchangePositionSnapshot(connector, symbol, size, 0.4m, 0.4m, 0m, "Buy", DateTimeOffset.UtcNow)
        ]);

    private static FakePositionClient ThrowingClient(string connector)
        => new(connector, true, () => throw new InvalidOperationException("read failed"));

    private sealed class FakePositionClient : IExchangeTradingClient
    {
        private readonly Func<IReadOnlyList<ExchangePositionSnapshot>> _positions;

        public FakePositionClient(
            string connectorName,
            bool supportsPositionReads,
            Func<IReadOnlyList<ExchangePositionSnapshot>> positions)
        {
            ConnectorName = connectorName;
            SupportsPositionReads = supportsPositionReads;
            _positions = positions;
        }

        public string ConnectorName { get; }

        public bool SupportsPositionReads { get; }

        public Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
            ExchangeApiCredentialSecret credentials,
            CancellationToken ct)
            => Task.FromResult(_positions());

        public Task<ExchangeConnectionCheckResult> CheckConnectionAsync(ExchangeApiCredentialSecret credentials, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<IReadOnlyList<ExchangeBalanceSnapshot>> GetBalancesAsync(ExchangeApiCredentialSecret credentials, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<ExchangeSymbolRules?> GetSymbolRulesAsync(string tradingPair, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<OrderFillResult> PlaceOrderAsync(ExchangeApiCredentialSecret credentials, PlaceOrderRequest request, CancellationToken ct)
            => throw new NotImplementedException();
    }

    private sealed class FakeRepository : ICarryTradeRepository
    {
        public bool ReconciledClosed { get; private set; }
        public decimal? ReconciledPnl { get; private set; }
        public decimal? ReconciledFees { get; private set; }
        public List<string> RecordedEventTypes { get; } = new();

        public Task<bool> MarkReconciledClosedAsync(
            Guid id,
            string closeReason,
            decimal? realizedPnlUsd,
            decimal? exitFeesUsd,
            decimal? exitNetEdgePct,
            CancellationToken ct)
        {
            ReconciledClosed = true;
            ReconciledPnl = realizedPnlUsd;
            ReconciledFees = exitFeesUsd;
            return Task.FromResult(true);
        }

        public Task RecordEventAsync(RecordCarryTradeEventRequest request, CancellationToken ct)
        {
            RecordedEventTypes.Add(request.EventType);
            return Task.CompletedTask;
        }

        public Task<CarryTrade> InsertOpenAsync(OpenCarryTradeRequest request, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<CarryTrade?> GetOpenAsync(CancellationToken ct)
            => throw new NotImplementedException();

        public Task<CarryTrade?> GetByIdAsync(Guid id, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<bool> MarkClosingAsync(Guid id, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<CarryTrade> MarkClosedAsync(CloseCarryTradeRequest request, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<CarryTrade> MarkFailedAsync(Guid id, string error, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<IReadOnlyList<CarryTrade>> ListRecentAsync(int hours, int limit, CancellationToken ct)
            => throw new NotImplementedException();

        public Task RecordLegAsync(RecordCarryTradeLegRequest request, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<IReadOnlyList<CarryTradeLeg>> ListLegsAsync(Guid tradeId, CancellationToken ct)
            => throw new NotImplementedException();

        public Task<IReadOnlyList<CarryTradeEvent>> ListEventsAsync(Guid tradeId, CancellationToken ct)
            => throw new NotImplementedException();
    }
}
