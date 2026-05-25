using Arbitrage.Api.Application.MarketData.Depth;
using Microsoft.AspNetCore.Mvc;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/market-data/depth")]
public sealed class DepthController : ControllerBase
{
    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly DepthCandidateEvaluator _depthCandidateEvaluator;

    public DepthController(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        DepthCandidateEvaluator depthCandidateEvaluator)
    {
        _targetStore = targetStore;
        _depthCache = depthCache;
        _depthCandidateEvaluator = depthCandidateEvaluator;
    }

    [HttpGet("targets")]
    public object Targets()
    {
        var targets = _targetStore.GetAll();

        return new
        {
            UpdatedAt = _targetStore.GetUpdatedAt(),
            Count = targets.Count,
            Items = targets
                .OrderBy(x => x.ConnectorName)
                .ThenBy(x => x.TradingPair)
                .ToList()
        };
    }
    [HttpGet("stats")]
    public object Stats()
    {
        var now = DateTimeOffset.UtcNow;

        return _depthCache.GetAll()
            .GroupBy(x => x.Key.ConnectorName)
            .Select(group =>
            {
                var items = group.Select(x => x.Value).ToList();

                return new
                {
                    ConnectorName = group.Key,
                    Count = items.Count,
                    FreshCount1s = items.Count(x => now - x.ReceivedAt <= TimeSpan.FromSeconds(1)),
                    FreshCount3s = items.Count(x => now - x.ReceivedAt <= TimeSpan.FromSeconds(3)),
                    MaxAgeMs = items.Count == 0
                        ? 0
                        : items.Max(x => (now - x.ReceivedAt).TotalMilliseconds),
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
    [HttpGet("pair/{tradingPair}")]
    public object ByPair(string tradingPair)
    {
        var now = DateTimeOffset.UtcNow;

        return _depthCache.GetByTradingPair(tradingPair)
            .Select(x => new
            {
                x.ConnectorName,
                x.TradingPair,
                BestBidPrice = x.BestBidPrice,
                BestAskPrice = x.BestAskPrice,
                BidLevels = x.Bids.Count,
                AskLevels = x.Asks.Count,
                AgeMs = (now - x.ReceivedAt).TotalMilliseconds,
                Bids = x.Bids.Take(5),
                Asks = x.Asks.Take(5)
            })
            .OrderBy(x => x.ConnectorName)
            .ToList();
    }
    [HttpPost("evaluate-current")]
    public EvaluateDepthCandidatesResponse EvaluateCurrent(
    [FromBody] EvaluateDepthCandidatesRequest request)
    {
        return _depthCandidateEvaluator.EvaluateCurrent(request);
    }
}