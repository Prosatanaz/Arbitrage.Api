using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Domain.MarketData;
using Xunit;

namespace Arbitrage.Api.Tests.Application.MarketData.Depth;

public class OrderBookWalkerTests
{
    [Fact]
    public void Walk_FullyFillableAcrossMultipleLevels_ReturnsWeightedAveragePrice()
    {
        var levels = new List<OrderBookDepthLevel>
        {
            new(Price: 100m, Amount: 1m),
            new(Price: 101m, Amount: 1m),
        };

        var quote = OrderBookWalker.Walk(levels, requestedBaseAmount: 1.5m, isBuy: true);

        Assert.True(quote.IsFullyFillable);
        Assert.Equal(1.5m, quote.FilledBaseAmount);
        Assert.Equal(2, quote.LevelsUsed);
        Assert.Equal(100m, quote.BestPrice);
        Assert.Equal(101m, quote.WorstPrice);
        Assert.Equal(150.5m / 1.5m, quote.AveragePrice);
    }

    [Fact]
    public void Walk_RequestExceedsAvailableDepth_ReturnsNotFullyFillable()
    {
        var levels = new List<OrderBookDepthLevel>
        {
            new(Price: 100m, Amount: 1m),
        };

        var quote = OrderBookWalker.Walk(levels, requestedBaseAmount: 5m, isBuy: true);

        Assert.False(quote.IsFullyFillable);
        Assert.Equal(1m, quote.FilledBaseAmount);
        Assert.Equal(1, quote.LevelsUsed);
    }

    [Fact]
    public void Walk_EmptyOrderBook_ReturnsEmptyNotFillableQuote()
    {
        var quote = OrderBookWalker.Walk(
            Array.Empty<OrderBookDepthLevel>(),
            requestedBaseAmount: 1m,
            isBuy: true);

        Assert.False(quote.IsFullyFillable);
        Assert.Equal(0, quote.LevelsUsed);
        Assert.Equal(0m, quote.FilledBaseAmount);
    }

    [Fact]
    public void Walk_RequestedAmountIsZeroOrNegative_ReturnsEmptyNotFillableQuote()
    {
        var levels = new List<OrderBookDepthLevel>
        {
            new(Price: 100m, Amount: 1m),
        };

        var quote = OrderBookWalker.Walk(levels, requestedBaseAmount: 0m, isBuy: true);

        Assert.False(quote.IsFullyFillable);
        Assert.Equal(0, quote.LevelsUsed);
    }

    [Fact]
    public void Walk_SingleLevelCoversEntireRequest_UsesOneLevel()
    {
        var levels = new List<OrderBookDepthLevel>
        {
            new(Price: 100m, Amount: 10m),
            new(Price: 101m, Amount: 10m),
        };

        var quote = OrderBookWalker.Walk(levels, requestedBaseAmount: 3m, isBuy: true);

        Assert.True(quote.IsFullyFillable);
        Assert.Equal(1, quote.LevelsUsed);
        Assert.Equal(100m, quote.AveragePrice);
        Assert.Equal(100m, quote.WorstPrice);
    }

    [Fact]
    public void Walk_SellSide_ComputesSlippageAgainstBestBid()
    {
        var bidsDescending = new List<OrderBookDepthLevel>
        {
            new(Price: 100m, Amount: 1m),
            new(Price: 99m, Amount: 1m),
        };

        var quote = OrderBookWalker.Walk(bidsDescending, requestedBaseAmount: 2m, isBuy: false);

        Assert.True(quote.IsFullyFillable);
        Assert.Equal(100m, quote.BestPrice);
        Assert.True(quote.SlippagePct > 0);
    }
}
