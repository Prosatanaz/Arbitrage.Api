namespace Arbitrage.Api.Infrastructure.MarketData.Streams;

public static class DepthSubscriptionDiff
{
    public static (IReadOnlyList<string> ToAdd, IReadOnlyList<string> ToRemove) Compute(
        IReadOnlyCollection<string> desired,
        IReadOnlyCollection<string> subscribed)
    {
        var toAdd = desired
            .Except(subscribed, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var toRemove = subscribed
            .Except(desired, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return (toAdd, toRemove);
    }
}
