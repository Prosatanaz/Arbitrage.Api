using Arbitrage.Api.Application.MarketData.Streaming;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/market-data/streaming")]
public sealed class MarketDataStreamingController : ControllerBase
{
    private readonly BestBidAskCache _cache;

    public MarketDataStreamingController(BestBidAskCache cache)
    {
        _cache = cache;
    }

    [HttpGet("bbo/summary")]
    public object Summary()
    {
        var now = DateTimeOffset.UtcNow;

        return _cache.GetAll()
            .Select(x => new
            {
                x.Key.ConnectorName,
                x.Key.TradingPair,
                x.Value.BestBidPrice,
                x.Value.BestBidAmount,
                x.Value.BestAskPrice,
                x.Value.BestAskAmount,
                AgeMs = (now - x.Value.ReceivedAt).TotalMilliseconds,
                x.Value.ExchangeTimestamp,
                x.Value.ReceivedAt
            })
            .OrderBy(x => x.ConnectorName)
            .ThenBy(x => x.TradingPair)
            .ToList();
    }
    [HttpGet("bbo/stats")]
    public object Stats()
    {
        var now = DateTimeOffset.UtcNow;

        return _cache.GetAll()
            .GroupBy(x => x.Key.ConnectorName)
            .Select(group =>
            {
                var items = group
                    .Select(x => x.Value)
                    .ToList();

                return new
                {
                    ConnectorName = group.Key,
                    Count = items.Count,
                    FreshCount1s = items.Count(x => now - x.ReceivedAt <= TimeSpan.FromSeconds(1)),
                    FreshCount3s = items.Count(x => now - x.ReceivedAt <= TimeSpan.FromSeconds(3)),
                    FreshCount10s = items.Count(x => now - x.ReceivedAt <= TimeSpan.FromSeconds(10)),
                    MaxAgeMs = items.Count == 0
                        ? 0
                        : items.Max(x => (now - x.ReceivedAt).TotalMilliseconds),
                    AvgAgeMs = items.Count == 0
                        ? 0
                        : items.Average(x => (now - x.ReceivedAt).TotalMilliseconds),
                    SamplePairs = items
                        .OrderBy(x => x.TradingPair)
                        .Take(10)
                        .Select(x => x.TradingPair)
                        .ToList()
                };
            })
            .OrderBy(x => x.ConnectorName)
            .ToList();
    }

    [HttpGet("bbo/pair/{tradingPair}")]
    public object ByPair(string tradingPair)
    {
        var now = DateTimeOffset.UtcNow;

        return _cache
            .GetByTradingPair(tradingPair)
            .Select(x => new
            {
                x.ConnectorName,
                x.TradingPair,
                x.BestBidPrice,
                x.BestBidAmount,
                x.BestAskPrice,
                x.BestAskAmount,
                AgeMs = (now - x.ReceivedAt).TotalMilliseconds,
                x.ExchangeTimestamp,
                x.ReceivedAt
            })
            .OrderBy(x => x.ConnectorName);
    }
}
