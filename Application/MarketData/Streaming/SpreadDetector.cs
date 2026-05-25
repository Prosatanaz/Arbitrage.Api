using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed class SpreadDetector
{
    private readonly BestBidAskCache _cache;

    public SpreadDetector(BestBidAskCache cache)
    {
        _cache = cache;
    }

    public IReadOnlyList<SpreadCandidate> Detect(
        IReadOnlyList<string> tradingPairs,
        TimeSpan maxSnapshotAge,
        decimal minGrossSpreadPct,
        decimal maxGrossSpreadPct)
    {
        var now = DateTimeOffset.UtcNow;
        var result = new List<SpreadCandidate>();

        foreach (var tradingPair in tradingPairs)
        {
            var snapshots = _cache
                .GetByTradingPair(tradingPair)
                .Where(x => now - x.ReceivedAt <= maxSnapshotAge)
                .Where(x => x.BestBidPrice > 0)
                .Where(x => x.BestAskPrice > 0)
                .ToList();

            if (snapshots.Count < 2)
                continue;

            var cheapestAsk = snapshots.MinBy(x => x.BestAskPrice);
            var highestBid = snapshots.MaxBy(x => x.BestBidPrice);

            if (cheapestAsk is null || highestBid is null)
                continue;

            if (cheapestAsk.ConnectorName.Equals(
                    highestBid.ConnectorName,
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var grossSpread = highestBid.BestBidPrice - cheapestAsk.BestAskPrice;

            if (grossSpread <= 0)
                continue;

            var grossSpreadPct = cheapestAsk.BestAskPrice > 0
                ? grossSpread / cheapestAsk.BestAskPrice * 100m
                : 0m;

            if (grossSpreadPct < minGrossSpreadPct)
                continue;

            if (maxGrossSpreadPct > 0 &&
                grossSpreadPct > maxGrossSpreadPct)
            {
                continue;
            }

            result.Add(new SpreadCandidate(
                TradingPair: tradingPair,
                LongConnector: cheapestAsk.ConnectorName,
                ShortConnector: highestBid.ConnectorName,
                BuyPrice: cheapestAsk.BestAskPrice,
                SellPrice: highestBid.BestBidPrice,
                GrossSpread: grossSpread,
                GrossSpreadPct: grossSpreadPct,
                DetectedAt: now));
        }

        return result
            .OrderByDescending(x => x.GrossSpreadPct)
            .ToList();
    }
}