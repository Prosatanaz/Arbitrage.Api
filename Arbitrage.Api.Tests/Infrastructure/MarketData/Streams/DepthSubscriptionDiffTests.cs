using Arbitrage.Api.Infrastructure.MarketData.Streams;
using Xunit;

namespace Arbitrage.Api.Tests.Infrastructure.MarketData.Streams;

public class DepthSubscriptionDiffTests
{
    [Fact]
    public void Compute_NewSymbolsDesired_ReturnsThemAsToAdd()
    {
        var (toAdd, toRemove) = DepthSubscriptionDiff.Compute(
            desired: new[] { "BTCUSDT", "ETHUSDT" },
            subscribed: Array.Empty<string>());

        Assert.Equal(new[] { "BTCUSDT", "ETHUSDT" }, toAdd.OrderBy(x => x));
        Assert.Empty(toRemove);
    }

    [Fact]
    public void Compute_SymbolNoLongerDesired_ReturnsItAsToRemove()
    {
        var (toAdd, toRemove) = DepthSubscriptionDiff.Compute(
            desired: new[] { "BTCUSDT" },
            subscribed: new[] { "BTCUSDT", "ETHUSDT" });

        Assert.Empty(toAdd);
        Assert.Equal(new[] { "ETHUSDT" }, toRemove);
    }

    [Fact]
    public void Compute_SameSetDifferentCase_ProducesNoChanges()
    {
        var (toAdd, toRemove) = DepthSubscriptionDiff.Compute(
            desired: new[] { "btcusdt" },
            subscribed: new[] { "BTCUSDT" });

        Assert.Empty(toAdd);
        Assert.Empty(toRemove);
    }

    [Fact]
    public void Compute_EmptyDesired_UnsubscribesEverything()
    {
        var (toAdd, toRemove) = DepthSubscriptionDiff.Compute(
            desired: Array.Empty<string>(),
            subscribed: new[] { "BTCUSDT", "ETHUSDT" });

        Assert.Empty(toAdd);
        Assert.Equal(2, toRemove.Count);
    }
}
