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
    private readonly IReadOnlyList<IOrderBookDepthStream> _depthStreams;

    public DepthController(
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache,
        DepthCandidateEvaluator depthCandidateEvaluator,
        IEnumerable<IOrderBookDepthStream> depthStreams)
    {
        _targetStore = targetStore;
        _depthCache = depthCache;
        _depthCandidateEvaluator = depthCandidateEvaluator;
        _depthStreams = depthStreams.ToList();
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
                var items = group
                    .Select(x => x.Value)
                    .ToList();

                return new
                {
                    ConnectorName = group.Key,
                    Count = items.Count,
                    FreshCount1s = items.Count(x =>
                        now - x.ReceivedAt <= TimeSpan.FromSeconds(1)),
                    FreshCount3s = items.Count(x =>
                        now - x.ReceivedAt <= TimeSpan.FromSeconds(3)),
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

    [HttpGet("diagnostics")]
    public object Diagnostics(
        [FromQuery] int freshAgeMs = 3000,
        [FromQuery] int staleAgeMs = 15000,
        [FromQuery] int targetChangeGraceMs = 3000)
    {
        var now = DateTimeOffset.UtcNow;

        var targetsUpdatedAt = _targetStore.GetUpdatedAt();

        var targetsAgeMs = targetsUpdatedAt is null
            ? (double?)null
            : (now - targetsUpdatedAt.Value).TotalMilliseconds;

        var targetChangeGrace = TimeSpan.FromMilliseconds(targetChangeGraceMs);

        var targetsRecentlyChanged =
            targetsUpdatedAt is not null &&
            now - targetsUpdatedAt.Value <= targetChangeGrace;

        var freshAge = TimeSpan.FromMilliseconds(freshAgeMs);
        var staleAge = TimeSpan.FromMilliseconds(staleAgeMs);

        var targets = _targetStore
            .GetAll()
            .ToList();

        var cached = _depthCache
            .GetAll()
            .Select(x => new
            {
                x.Key.ConnectorName,
                x.Key.TradingPair,
                Snapshot = x.Value
            })
            .ToList();

        var registeredConnectors = _depthStreams
            .Select(x => x.ConnectorName)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var targetConnectors = targets
            .Select(x => x.ConnectorName)
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var cachedConnectors = cached
            .Select(x => x.ConnectorName)
            .Distinct(StringComparer.OrdinalIgnoreCase);

        var connectors = registeredConnectors
            .Concat(targetConnectors)
            .Concat(cachedConnectors)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var items = connectors
            .Select(connectorName =>
            {
                var targetPairs = targets
                    .Where(x => x.ConnectorName.Equals(
                        connectorName,
                        StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.TradingPair)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(x => x)
                    .ToList();

                var targetPairSet = targetPairs
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var cachedItems = cached
                    .Where(x => x.ConnectorName.Equals(
                        connectorName,
                        StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.Snapshot)
                    .ToList();

                var cachedPairs = cachedItems
                    .Select(x => x.TradingPair)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(x => x)
                    .ToList();

                var cachedPairSet = cachedPairs
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var targetedCachedItems = cachedItems
                    .Where(x => targetPairSet.Contains(x.TradingPair))
                    .ToList();

                var missingTargetPairs = targetPairs
                    .Where(x => !cachedPairSet.Contains(x))
                    .OrderBy(x => x)
                    .ToList();

                var staleTargetPairs = targetedCachedItems
                    .Where(x => now - x.ReceivedAt > staleAge)
                    .OrderByDescending(x => now - x.ReceivedAt)
                    .Select(x => new
                    {
                        x.TradingPair,
                        AgeMs = (now - x.ReceivedAt).TotalMilliseconds,
                        x.ReceivedAt,
                        x.ExchangeTimestamp
                    })
                    .ToList();

                var orphanCachedPairs = cachedPairs
                    .Where(x => !targetPairSet.Contains(x))
                    .OrderBy(x => x)
                    .ToList();

                var freshTargetedCachedCount = targetedCachedItems
                    .Count(x => now - x.ReceivedAt <= freshAge);

                var registered = registeredConnectors.Contains(
                    connectorName,
                    StringComparer.OrdinalIgnoreCase);

                var healthy =
                    targetPairs.Count == 0 ||
                    staleTargetPairs.Count == 0 &&
                    (missingTargetPairs.Count == 0 || targetsRecentlyChanged);

                return new
                {
                    ConnectorName = connectorName,
                    Registered = registered,

                    TargetCount = targetPairs.Count,
                    CachedCount = cachedPairs.Count,
                    TargetedCachedCount = targetedCachedItems.Count,
                    FreshTargetedCachedCount = freshTargetedCachedCount,

                    MissingTargetCount = missingTargetPairs.Count,
                    StaleTargetCount = staleTargetPairs.Count,
                    OrphanCachedCount = orphanCachedPairs.Count,

                    Healthy = healthy,
                    TargetsRecentlyChanged = targetsRecentlyChanged,
                    TargetChangeGraceMs = targetChangeGraceMs,

                    SampleTargets = targetPairs
                        .Take(10)
                        .ToList(),

                    SampleCachedPairs = cachedPairs
                        .Take(10)
                        .ToList(),

                    MissingTargetPairs = missingTargetPairs
                        .Take(20)
                        .ToList(),

                    StaleTargetPairs = staleTargetPairs
                        .Take(20)
                        .ToList(),

                    OrphanCachedPairs = orphanCachedPairs
                        .Take(20)
                        .ToList()
                };
            })
            .ToList();

        return new
        {
            Now = now,
            TargetsUpdatedAt = targetsUpdatedAt,
            TargetsAgeMs = targetsAgeMs,

            FreshAgeMs = freshAgeMs,
            StaleAgeMs = staleAgeMs,
            TargetChangeGraceMs = targetChangeGraceMs,
            TargetsRecentlyChanged = targetsRecentlyChanged,

            ConnectorCount = items.Count,
            RegisteredConnectorCount = items.Count(x => x.Registered),
            TargetConnectorCount = items.Count(x => x.TargetCount > 0),
            HealthyConnectorCount = items.Count(x => x.Healthy),

            Items = items
        };
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