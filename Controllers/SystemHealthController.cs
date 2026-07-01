using Arbitrage.Api.Application.MarketData.Depth;
using Arbitrage.Api.Application.MarketData.Opportunities;
using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Application.MarketData.Universe;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Controllers;

[ApiController]
[Route("api/system")]
public sealed class SystemHealthController : ControllerBase
{
    private readonly BestBidAskCache _bboCache;
    private readonly DepthSubscriptionTargetStore _depthTargetStore;
    private readonly OrderBookDepthCache _depthCache;
    private readonly LatestSpreadCandidateStore _spreadStore;
    private readonly LatestValidatedOpportunityStore _opportunityStore;
    private readonly ITradingPairUniverseProvider _universeProvider;
    private readonly IReadOnlyList<IBestBidAskStream> _bboStreams;
    private readonly IReadOnlyList<IOrderBookDepthStream> _depthStreams;
    private readonly MarketDataStreamOptions _marketDataOptions;
    private readonly DepthTrackingOptions _depthTrackingOptions;
    private readonly SpreadDetectorOptions _spreadDetectorOptions;
    private readonly ValidatedOpportunityOptions _validatedOpportunityOptions;
    private readonly TradingPairUniverseOptions _universeOptions;

    public SystemHealthController(
        BestBidAskCache bboCache,
        DepthSubscriptionTargetStore depthTargetStore,
        OrderBookDepthCache depthCache,
        LatestSpreadCandidateStore spreadStore,
        LatestValidatedOpportunityStore opportunityStore,
        ITradingPairUniverseProvider universeProvider,
        IEnumerable<IBestBidAskStream> bboStreams,
        IEnumerable<IOrderBookDepthStream> depthStreams,
        IOptions<MarketDataStreamOptions> marketDataOptions,
        IOptions<DepthTrackingOptions> depthTrackingOptions,
        IOptions<SpreadDetectorOptions> spreadDetectorOptions,
        IOptions<ValidatedOpportunityOptions> validatedOpportunityOptions,
        IOptions<TradingPairUniverseOptions> universeOptions)
    {
        _bboCache = bboCache;
        _depthTargetStore = depthTargetStore;
        _depthCache = depthCache;
        _spreadStore = spreadStore;
        _opportunityStore = opportunityStore;
        _universeProvider = universeProvider;
        _bboStreams = bboStreams.ToList();
        _depthStreams = depthStreams.ToList();
        _marketDataOptions = marketDataOptions.Value;
        _depthTrackingOptions = depthTrackingOptions.Value;
        _spreadDetectorOptions = spreadDetectorOptions.Value;
        _validatedOpportunityOptions = validatedOpportunityOptions.Value;
        _universeOptions = universeOptions.Value;
    }

    [HttpGet("health")]
    public async Task<object> Health(
        [FromQuery] int bboFreshAgeMs = 10000,
        [FromQuery] int depthFreshAgeMs = 3000,
        [FromQuery] int depthStaleAgeMs = 15000,
        [FromQuery] int targetChangeGraceMs = 3000,
        CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow;

        var universe = await LoadUniverseSummaryAsync(ct);

        var bboFreshAge = TimeSpan.FromMilliseconds(bboFreshAgeMs);
        var depthFreshAge = TimeSpan.FromMilliseconds(depthFreshAgeMs);
        var depthStaleAge = TimeSpan.FromMilliseconds(depthStaleAgeMs);
        var targetChangeGrace = TimeSpan.FromMilliseconds(targetChangeGraceMs);

        var targetsUpdatedAt = _depthTargetStore.GetUpdatedAt();

        var targetsAgeMs = targetsUpdatedAt is null
            ? (double?)null
            : (now - targetsUpdatedAt.Value).TotalMilliseconds;

        var targetsRecentlyChanged =
            targetsUpdatedAt is not null &&
            now - targetsUpdatedAt.Value <= targetChangeGrace;

        var enabledMarketConnectors = _marketDataOptions.EnabledConnectors
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var enabledUniverseConnectors = _universeOptions.EnabledConnectors
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var registeredBboConnectors = _bboStreams
            .Select(x => x.ConnectorName)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var registeredDepthConnectors = _depthStreams
            .Select(x => x.ConnectorName)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var bboSnapshots = _bboCache
            .GetAll()
            .Select(x => x.Value)
            .ToList();

        var depthTargets = _depthTargetStore
            .GetAll()
            .ToList();

        var depthSnapshots = _depthCache
            .GetAll()
            .Select(x => x.Value)
            .ToList();

        var connectors = enabledMarketConnectors
            .Concat(enabledUniverseConnectors)
            .Concat(registeredBboConnectors)
            .Concat(registeredDepthConnectors)
            .Concat(bboSnapshots.Select(x => x.ConnectorName))
            .Concat(depthTargets.Select(x => x.ConnectorName))
            .Concat(depthSnapshots.Select(x => x.ConnectorName))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var connectorItems = connectors
            .Select(connectorName => BuildConnectorHealth(
                connectorName,
                now,
                bboFreshAge,
                depthFreshAge,
                depthStaleAge,
                targetsRecentlyChanged,
                enabledMarketConnectors,
                enabledUniverseConnectors,
                registeredBboConnectors,
                registeredDepthConnectors,
                bboSnapshots,
                depthTargets,
                depthSnapshots))
            .ToList();

        var spreadSnapshot = _spreadStore.GetTracked();
        var opportunitySnapshot = _opportunityStore.Get();

        var degradedConnectors = connectorItems
            .Where(x => x.EnabledForMarketData && (!x.BboHealthy || !x.DepthHealthy))
            .Select(x => x.ConnectorName)
            .ToList();

        var missingBboRegistrations = enabledMarketConnectors
            .Except(registeredBboConnectors, StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var missingDepthRegistrations = enabledMarketConnectors
            .Except(registeredDepthConnectors, StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var status =
            !universe.Loaded
                ? "Degraded"
                : degradedConnectors.Count > 0
                    ? "Degraded"
                    : missingBboRegistrations.Count > 0
                        ? "Degraded"
                        : "Healthy";

        return new
        {
            Status = status,
            Now = now,

            Thresholds = new
            {
                BboFreshAgeMs = bboFreshAgeMs,
                DepthFreshAgeMs = depthFreshAgeMs,
                DepthStaleAgeMs = depthStaleAgeMs,
                TargetChangeGraceMs = targetChangeGraceMs
            },

            Universe = universe,

            Options = new
            {
                MarketDataStreamsEnabled = _marketDataOptions.Enabled,
                SpreadDetectorEnabled = _spreadDetectorOptions.Enabled,
                DepthTrackingEnabled = _depthTrackingOptions.Enabled,
                ValidatedOpportunitiesEnabled = _validatedOpportunityOptions.Enabled,
                TradingPairUniverseEnabled = _universeOptions.Enabled,
                TradingPairUniverseRefreshOnStartup = _universeOptions.RefreshOnStartup,
                TradingPairUniverseRefreshIntervalMinutes = _universeOptions.RefreshIntervalMinutes,
                TradingPairUniverseRequireAllEnabledConnectors = _universeOptions.RequireAllEnabledConnectors,
                TradingPairUniverseTreatZeroPairsAsFailure = _universeOptions.TreatZeroPairsAsFailure
            },

            Registrations = new
            {
                EnabledMarketConnectors = enabledMarketConnectors,
                EnabledUniverseConnectors = enabledUniverseConnectors,
                RegisteredBboConnectors = registeredBboConnectors,
                RegisteredDepthConnectors = registeredDepthConnectors,
                MissingBboRegistrations = missingBboRegistrations,
                MissingDepthRegistrations = missingDepthRegistrations
            },

            Bbo = new
            {
                TotalSnapshots = bboSnapshots.Count,
                ConnectorCount = bboSnapshots
                    .Select(x => x.ConnectorName)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count(),
                FreshCount = bboSnapshots.Count(x => now - x.ReceivedAt <= bboFreshAge)
            },

            Depth = new
            {
                TargetsUpdatedAt = targetsUpdatedAt,
                TargetsAgeMs = targetsAgeMs,
                TargetsRecentlyChanged = targetsRecentlyChanged,
                TotalTargets = depthTargets.Count,
                TotalCachedSnapshots = depthSnapshots.Count,
                TargetConnectorCount = depthTargets
                    .Select(x => x.ConnectorName)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count(),
                CachedConnectorCount = depthSnapshots
                    .Select(x => x.ConnectorName)
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .Count()
            },

            Spreads = new
            {
                spreadSnapshot.UpdatedAt,
                HasData = spreadSnapshot.UpdatedAt is not null,
                Count = spreadSnapshot.Items.Count,
                Best = spreadSnapshot.Items.Count == 0
                    ? null
                    : new
                    {
                        spreadSnapshot.Items[0].Candidate.TradingPair,
                        spreadSnapshot.Items[0].Candidate.LongConnector,
                        spreadSnapshot.Items[0].Candidate.ShortConnector,
                        spreadSnapshot.Items[0].Candidate.GrossSpreadPct,
                        spreadSnapshot.Items[0].FirstSeenAt,
                        spreadSnapshot.Items[0].LastSeenAt
                    }
            },

            Opportunities = new
            {
                opportunitySnapshot.UpdatedAt,
                HasData = opportunitySnapshot.UpdatedAt is not null,
                Count = opportunitySnapshot.Items.Count,
                Evaluation = opportunitySnapshot.Evaluation is null
                    ? null
                    : new
                    {
                        opportunitySnapshot.Evaluation.EvaluatedAt,
                        opportunitySnapshot.Evaluation.CandidatesAvailable,
                        opportunitySnapshot.Evaluation.CandidatesMatured,
                        opportunitySnapshot.Evaluation.CandidatesTargeted,
                        opportunitySnapshot.Evaluation.CandidatesSkippedAsNotTargeted,
                        opportunitySnapshot.Evaluation.CandidatesChecked,
                        opportunitySnapshot.Evaluation.ValidCount,
                        opportunitySnapshot.Evaluation.NetEdgeBelowMinimumCount,
                        opportunitySnapshot.Evaluation.MissingDepthCount,
                        opportunitySnapshot.Evaluation.NotFillableCount
                    },
                ValidCount = opportunitySnapshot.Items.Count(x =>
                    x.Status == DepthCandidateValidationStatus.Valid),
                PositiveValidCount = opportunitySnapshot.Items.Count(x =>
                    x.Status == DepthCandidateValidationStatus.Valid &&
                    x.NetEdgePct > 0),
                Best = opportunitySnapshot.Items
                    .Where(x =>
                        x.Status == DepthCandidateValidationStatus.Valid &&
                        x.NetEdgePct > 0)
                    .OrderByDescending(x => x.NetEdgePct)
                    .Select(x => new
                    {
                        x.Candidate.TradingPair,
                        x.Candidate.LongConnector,
                        x.Candidate.ShortConnector,
                        x.GrossSpreadPct,
                        x.EstimatedFeesPct,
                        x.NetEdgePct,
                        x.NotionalUsd
                    })
                    .FirstOrDefault()
            },

            Connectors = connectorItems,

            DegradedConnectors = degradedConnectors
        };
    }

    private async Task<UniverseHealth> LoadUniverseSummaryAsync(
        CancellationToken ct)
    {
        try
        {
            var pairs = await _universeProvider.GetTradingPairsAsync(ct);

            return new UniverseHealth(
                Loaded: true,
                PairCount: pairs.Count,
                OutputPath: _universeOptions.OutputPath,
                Error: null);
        }
        catch (Exception ex)
        {
            return new UniverseHealth(
                Loaded: false,
                PairCount: 0,
                OutputPath: _universeOptions.OutputPath,
                Error: ex.Message);
        }
    }

    private static ConnectorHealthItem BuildConnectorHealth(
        string connectorName,
        DateTimeOffset now,
        TimeSpan bboFreshAge,
        TimeSpan depthFreshAge,
        TimeSpan depthStaleAge,
        bool targetsRecentlyChanged,
        IReadOnlyList<string> enabledMarketConnectors,
        IReadOnlyList<string> enabledUniverseConnectors,
        IReadOnlyList<string> registeredBboConnectors,
        IReadOnlyList<string> registeredDepthConnectors,
        IReadOnlyList<Arbitrage.Api.Domain.MarketData.BestBidAskSnapshot> bboSnapshots,
        IReadOnlyList<DepthSubscriptionKey> depthTargets,
        IReadOnlyList<Arbitrage.Api.Domain.MarketData.OrderBookDepthSnapshot> depthSnapshots)
    {
        var enabledForMarketData = enabledMarketConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);

        var enabledForUniverse = enabledUniverseConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);

        var bboRegistered = registeredBboConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);

        var depthRegistered = registeredDepthConnectors.Contains(
            connectorName,
            StringComparer.OrdinalIgnoreCase);

        var connectorBboSnapshots = bboSnapshots
            .Where(x => x.ConnectorName.Equals(
                connectorName,
                StringComparison.OrdinalIgnoreCase))
            .ToList();

        var bboCount = connectorBboSnapshots.Count;

        var bboFreshCount = connectorBboSnapshots.Count(x =>
            now - x.ReceivedAt <= bboFreshAge);

        var bboMaxAgeMs = bboCount == 0
            ? (double?)null
            : connectorBboSnapshots.Max(x => (now - x.ReceivedAt).TotalMilliseconds);

        var bboAvgAgeMs = bboCount == 0
            ? (double?)null
            : connectorBboSnapshots.Average(x => (now - x.ReceivedAt).TotalMilliseconds);

        var connectorTargetPairs = depthTargets
            .Where(x => x.ConnectorName.Equals(
                connectorName,
                StringComparison.OrdinalIgnoreCase))
            .Select(x => x.TradingPair)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var targetPairSet = connectorTargetPairs
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var connectorDepthSnapshots = depthSnapshots
            .Where(x => x.ConnectorName.Equals(
                connectorName,
                StringComparison.OrdinalIgnoreCase))
            .ToList();

        var cachedPairs = connectorDepthSnapshots
            .Select(x => x.TradingPair)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(x => x)
            .ToList();

        var cachedPairSet = cachedPairs
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var targetedCachedSnapshots = connectorDepthSnapshots
            .Where(x => targetPairSet.Contains(x.TradingPair))
            .ToList();

        var missingTargetPairs = connectorTargetPairs
            .Where(x => !cachedPairSet.Contains(x))
            .OrderBy(x => x)
            .ToList();

        var staleTargetPairs = targetedCachedSnapshots
            .Where(x => now - x.ReceivedAt > depthStaleAge)
            .OrderByDescending(x => now - x.ReceivedAt)
            .Select(x => new StaleDepthPair(
                TradingPair: x.TradingPair,
                AgeMs: (now - x.ReceivedAt).TotalMilliseconds,
                ReceivedAt: x.ReceivedAt,
                ExchangeTimestamp: x.ExchangeTimestamp))
            .ToList();

        var orphanCachedPairs = cachedPairs
            .Where(x => !targetPairSet.Contains(x))
            .OrderBy(x => x)
            .ToList();

        var depthFreshTargetedCachedCount = targetedCachedSnapshots.Count(x =>
            now - x.ReceivedAt <= depthFreshAge);

        var bboHealthy =
            !enabledForMarketData ||
            bboRegistered &&
            bboCount > 0 &&
            bboFreshCount > 0;

        var depthHealthy =
            !enabledForMarketData ||
            !depthRegistered ||
            connectorTargetPairs.Count == 0 ||
            staleTargetPairs.Count == 0 &&
            (missingTargetPairs.Count == 0 || targetsRecentlyChanged);

        return new ConnectorHealthItem(
            ConnectorName: connectorName,
            EnabledForMarketData: enabledForMarketData,
            EnabledForUniverse: enabledForUniverse,
            BboRegistered: bboRegistered,
            DepthRegistered: depthRegistered,

            BboCount: bboCount,
            BboFreshCount: bboFreshCount,
            BboMaxAgeMs: bboMaxAgeMs,
            BboAvgAgeMs: bboAvgAgeMs,
            BboHealthy: bboHealthy,

            DepthTargetCount: connectorTargetPairs.Count,
            DepthCachedCount: cachedPairs.Count,
            DepthTargetedCachedCount: targetedCachedSnapshots.Count,
            DepthFreshTargetedCachedCount: depthFreshTargetedCachedCount,
            DepthMissingTargetCount: missingTargetPairs.Count,
            DepthStaleTargetCount: staleTargetPairs.Count,
            DepthOrphanCachedCount: orphanCachedPairs.Count,
            DepthHealthy: depthHealthy,

            SampleBboPairs: connectorBboSnapshots
                .OrderBy(x => x.TradingPair)
                .Take(10)
                .Select(x => x.TradingPair)
                .ToList(),

            SampleDepthTargets: connectorTargetPairs
                .Take(10)
                .ToList(),

            SampleDepthCachedPairs: cachedPairs
                .Take(10)
                .ToList(),

            MissingDepthTargets: missingTargetPairs
                .Take(20)
                .ToList(),

            StaleDepthTargets: staleTargetPairs
                .Take(20)
                .ToList(),

            OrphanDepthCachedPairs: orphanCachedPairs
                .Take(20)
                .ToList());
    }

    private sealed record UniverseHealth(
        bool Loaded,
        int PairCount,
        string OutputPath,
        string? Error);

    private sealed record ConnectorHealthItem(
        string ConnectorName,
        bool EnabledForMarketData,
        bool EnabledForUniverse,
        bool BboRegistered,
        bool DepthRegistered,

        int BboCount,
        int BboFreshCount,
        double? BboMaxAgeMs,
        double? BboAvgAgeMs,
        bool BboHealthy,

        int DepthTargetCount,
        int DepthCachedCount,
        int DepthTargetedCachedCount,
        int DepthFreshTargetedCachedCount,
        int DepthMissingTargetCount,
        int DepthStaleTargetCount,
        int DepthOrphanCachedCount,
        bool DepthHealthy,

        IReadOnlyList<string> SampleBboPairs,
        IReadOnlyList<string> SampleDepthTargets,
        IReadOnlyList<string> SampleDepthCachedPairs,
        IReadOnlyList<string> MissingDepthTargets,
        IReadOnlyList<StaleDepthPair> StaleDepthTargets,
        IReadOnlyList<string> OrphanDepthCachedPairs);

    private sealed record StaleDepthPair(
        string TradingPair,
        double AgeMs,
        DateTimeOffset ReceivedAt,
        DateTimeOffset ExchangeTimestamp);
}