using Arbitrage.Api.Application.Execution.CarryTrades;
using Arbitrage.Api.Application.Execution.Credentials;
using Arbitrage.Api.Application.Execution.Trading;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Arbitrage.Api.Tests.Application.Execution.CarryTrades;

public class CarryTradeLegExecutorTests
{
    private static readonly ExchangeApiCredentialSecret Credentials =
        new("test", "key", "secret", null, true);

    [Fact]
    public async Task ExecuteAsync_BothLegsFillFully_ReturnsSuccessWithFullQuantity()
    {
        var longClient = new FakeExchangeTradingClient("long", request => Filled(request, request.Quantity));
        var shortClient = new FakeExchangeTradingClient("short", request => Filled(request, request.Quantity));

        var executor = CreateExecutor(longClient, shortClient);

        var result = await executor.ExecuteAsync(
            CreateRequest(longClient.ConnectorName, shortClient.ConnectorName, quantity: 10m),
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.Equal(10m, result.MatchedQuantity);
        Assert.Single(longClient.PlacedOrders);
        Assert.Single(shortClient.PlacedOrders);
    }

    [Fact]
    public async Task ExecuteAsync_OneLegFails_UnwindsTheOtherLegAndReturnsFailure()
    {
        var longClient = new FakeExchangeTradingClient("long", request => Filled(request, request.Quantity));
        var shortClient = new FakeExchangeTradingClient("short", _ => throw new InvalidOperationException("exchange rejected order"));

        var executor = CreateExecutor(longClient, shortClient);

        var result = await executor.ExecuteAsync(
            CreateRequest(longClient.ConnectorName, shortClient.ConnectorName, quantity: 10m),
            CancellationToken.None);

        Assert.False(result.Success);
        Assert.Equal(0m, result.MatchedQuantity);

        // The filled long leg must be unwound (reduce-only, opposite side) since the short leg
        // never filled - never leave a single naked leg open.
        Assert.Equal(2, longClient.PlacedOrders.Count);
        var unwindOrder = longClient.PlacedOrders[1];
        Assert.Equal("Sell", unwindOrder.Side);
        Assert.True(unwindOrder.ReduceOnly);
        Assert.Equal(10m, unwindOrder.Quantity);

        Assert.Single(shortClient.PlacedOrders);
    }

    [Fact]
    public async Task ExecuteAsync_UnequalFills_TrimsLargerLegDownToMatchSmaller()
    {
        var longClient = new FakeExchangeTradingClient("long", request => Filled(request, request.Quantity));
        var shortClient = new FakeExchangeTradingClient("short", request => Filled(request, 7m));

        var executor = CreateExecutor(longClient, shortClient);

        var result = await executor.ExecuteAsync(
            CreateRequest(longClient.ConnectorName, shortClient.ConnectorName, quantity: 10m),
            CancellationToken.None);

        Assert.True(result.Success);
        Assert.Equal(7m, result.MatchedQuantity);

        // Long leg over-filled relative to short (10 vs 7) - trim the excess 3, never chase the
        // short leg up to 10 (that would add more slippage risk for a marginal size gain).
        Assert.Equal(2, longClient.PlacedOrders.Count);
        var trimOrder = longClient.PlacedOrders[1];
        Assert.Equal("Sell", trimOrder.Side);
        Assert.True(trimOrder.ReduceOnly);
        Assert.Equal(3m, trimOrder.Quantity);

        Assert.Single(shortClient.PlacedOrders);
    }

    [Fact]
    public async Task ExecuteAsync_BothLegsFail_ReturnsFailureWithoutUnwinding()
    {
        var longClient = new FakeExchangeTradingClient("long", _ => throw new InvalidOperationException("boom"));
        var shortClient = new FakeExchangeTradingClient("short", _ => throw new InvalidOperationException("boom"));

        var executor = CreateExecutor(longClient, shortClient);

        var result = await executor.ExecuteAsync(
            CreateRequest(longClient.ConnectorName, shortClient.ConnectorName, quantity: 10m),
            CancellationToken.None);

        Assert.False(result.Success);
        Assert.Single(longClient.PlacedOrders);
        Assert.Single(shortClient.PlacedOrders);
    }

    private static CarryTradeLegExecutor CreateExecutor(
        params IExchangeTradingClient[] clients)
    {
        var registry = new ExchangeTradingClientRegistry(clients);

        return new CarryTradeLegExecutor(
            registry,
            NullLogger<CarryTradeLegExecutor>.Instance);
    }

    private static LegExecutionRequest CreateRequest(
        string longConnector,
        string shortConnector,
        decimal quantity)
    {
        return new LegExecutionRequest(
            TradingPair: "BTC-USDT",
            LongConnector: longConnector,
            ShortConnector: shortConnector,
            LongCredentials: Credentials,
            ShortCredentials: Credentials,
            LongSide: "Buy",
            ShortSide: "Sell",
            Quantity: quantity,
            ReduceOnly: false,
            ClientOrderId: "attempt-1",
            MaxLegDelayMs: 5000);
    }

    private static OrderFillResult Filled(
        PlaceOrderRequest request,
        decimal filledQuantity)
    {
        return new OrderFillResult(
            ConnectorName: "test",
            ClientOrderId: request.ClientOrderId,
            ExchangeOrderId: "order-1",
            IsFilled: filledQuantity > 0,
            FilledQuantity: filledQuantity,
            AverageFillPrice: 100m,
            FeePaidUsd: 0.1m,
            Status: filledQuantity >= request.Quantity ? "Filled" : "PartiallyFilled",
            FilledAt: DateTimeOffset.UtcNow);
    }

    private sealed class FakeExchangeTradingClient : IExchangeTradingClient
    {
        private readonly Func<PlaceOrderRequest, OrderFillResult> _placeOrderHandler;

        public FakeExchangeTradingClient(
            string connectorName,
            Func<PlaceOrderRequest, OrderFillResult> placeOrderHandler)
        {
            ConnectorName = connectorName;
            _placeOrderHandler = placeOrderHandler;
        }

        public string ConnectorName { get; }

        public List<PlaceOrderRequest> PlacedOrders { get; } = new();

        public Task<ExchangeConnectionCheckResult> CheckConnectionAsync(
            ExchangeApiCredentialSecret credentials,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyList<ExchangeBalanceSnapshot>> GetBalancesAsync(
            ExchangeApiCredentialSecret credentials,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task<IReadOnlyList<ExchangePositionSnapshot>> GetPositionsAsync(
            ExchangeApiCredentialSecret credentials,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task<ExchangeSymbolRules?> GetSymbolRulesAsync(
            string tradingPair,
            CancellationToken ct)
        {
            throw new NotImplementedException();
        }

        public Task<OrderFillResult> PlaceOrderAsync(
            ExchangeApiCredentialSecret credentials,
            PlaceOrderRequest request,
            CancellationToken ct)
        {
            PlacedOrders.Add(request);

            var result = _placeOrderHandler(request);

            return Task.FromResult(result);
        }
    }
}
