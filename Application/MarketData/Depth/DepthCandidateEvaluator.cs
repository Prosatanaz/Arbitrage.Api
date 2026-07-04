using Arbitrage.Api.Application.MarketData.Streaming;
using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Depth;

public sealed class DepthCandidateEvaluator
{
    private readonly LatestSpreadCandidateStore _candidateStore;
    private readonly DepthSubscriptionTargetStore _targetStore;
    private readonly OrderBookDepthCache _depthCache;

    public DepthCandidateEvaluator(
        LatestSpreadCandidateStore candidateStore,
        DepthSubscriptionTargetStore targetStore,
        OrderBookDepthCache depthCache)
    {
        _candidateStore = candidateStore;
        _targetStore = targetStore;
        _depthCache = depthCache;
    }

    public EvaluateDepthCandidatesResponse EvaluateCurrent(
        EvaluateDepthCandidatesRequest request)
    {
        var evaluatedAt = DateTimeOffset.UtcNow;
        var maxDepthAge = TimeSpan.FromMilliseconds(request.MaxDepthAgeMs);
        var minCandidateAge = TimeSpan.FromMilliseconds(request.MinCandidateAgeMs);

        var (_, trackedCandidates) = _candidateStore.GetTracked();

        var activeTargets = _targetStore.GetAll();

        var maturedCandidates = trackedCandidates
            .Where(x => evaluatedAt - x.FirstSeenAt >= minCandidateAge)
            .OrderByDescending(x => x.Candidate.GrossSpreadPct)
            .ToList();

        var targetedCandidates = maturedCandidates
            .Where(x => HasBothDepthTargets(x.Candidate, activeTargets))
            .OrderByDescending(x => x.Candidate.GrossSpreadPct)
            .ToList();

        var selectedCandidates = targetedCandidates
            .Take(Math.Clamp(request.MaxCandidates, 1, 200))
            .ToList();

        var items = selectedCandidates
            .Select(trackedCandidate => EvaluateSingle(
                trackedCandidate.Candidate,
                request,
                evaluatedAt,
                maxDepthAge))
            .OrderByDescending(x => x.NetEdgePct ?? decimal.MinValue)
            .ToList();

        return new EvaluateDepthCandidatesResponse(
            EvaluatedAt: evaluatedAt,
            CandidatesAvailable: trackedCandidates.Count,
            CandidatesMatured: maturedCandidates.Count,
            CandidatesTargeted: targetedCandidates.Count,
            CandidatesSkippedAsNotTargeted: maturedCandidates.Count - targetedCandidates.Count,
            CandidatesChecked: items.Count,
            ValidCount: items.Count(x => x.Status == DepthCandidateValidationStatus.Valid),
            NetEdgeBelowMinimumCount: items.Count(x =>
                x.Status == DepthCandidateValidationStatus.NetEdgeBelowMinimum),
            MissingDepthCount: items.Count(x =>
                x.Status is DepthCandidateValidationStatus.MissingBuyDepth
                    or DepthCandidateValidationStatus.MissingSellDepth
                    or DepthCandidateValidationStatus.StaleBuyDepth
                    or DepthCandidateValidationStatus.StaleSellDepth),
            NotFillableCount: items.Count(x =>
                x.Status == DepthCandidateValidationStatus.NotFullyFillable),
            Items: items);
    }

    private static bool HasBothDepthTargets(
        SpreadCandidate candidate,
        IReadOnlySet<DepthSubscriptionKey> activeTargets)
    {
        return HasDepthTarget(
                   activeTargets,
                   candidate.LongConnector,
                   candidate.TradingPair)
               &&
               HasDepthTarget(
                   activeTargets,
                   candidate.ShortConnector,
                   candidate.TradingPair);
    }

    private static bool HasDepthTarget(
        IReadOnlySet<DepthSubscriptionKey> activeTargets,
        string connectorName,
        string tradingPair)
    {
        return activeTargets.Any(x =>
            x.ConnectorName.Equals(
                connectorName,
                StringComparison.OrdinalIgnoreCase)
            &&
            x.TradingPair.Equals(
                tradingPair,
                StringComparison.OrdinalIgnoreCase));
    }

    private ValidatedDepthCandidate EvaluateSingle(
        SpreadCandidate candidate,
        EvaluateDepthCandidatesRequest request,
        DateTimeOffset now,
        TimeSpan maxDepthAge)
    {
        if (!_depthCache.TryGet(
                candidate.LongConnector,
                candidate.TradingPair,
                out var buyDepth) ||
            buyDepth is null)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingBuyDepth,
                "Buy-side depth snapshot is missing.");
        }

        var bestAsk = buyDepth.BestAskPrice;

        if (bestAsk <= 0)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingBuyDepth,
                "Buy-side best ask is invalid.");
        }

        var requestedBaseAmount = request.NotionalUsd / bestAsk;

        return EvaluateWithBaseAmount(
            candidate,
            requestedBaseAmount,
            request,
            now,
            maxDepthAge);
    }

    /// <summary>
    /// Evaluates the current live net edge for a specific already-open position (known pair,
    /// connectors and base quantity) rather than deriving the size from a tracked candidate's
    /// notional - used by the carry-trade position monitor to decide when to close.
    /// </summary>
    public ValidatedDepthCandidate EvaluateForOpenPosition(
        string tradingPair,
        string longConnector,
        string shortConnector,
        decimal baseQuantity,
        EvaluateDepthCandidatesRequest request)
    {
        var now = DateTimeOffset.UtcNow;
        var maxDepthAge = TimeSpan.FromMilliseconds(request.MaxDepthAgeMs);

        var syntheticCandidate = new SpreadCandidate(
            TradingPair: tradingPair,
            LongConnector: longConnector,
            ShortConnector: shortConnector,
            BuyPrice: 0m,
            SellPrice: 0m,
            GrossSpread: 0m,
            GrossSpreadPct: 0m,
            DetectedAt: now);

        return EvaluateWithBaseAmount(
            syntheticCandidate,
            baseQuantity,
            request,
            now,
            maxDepthAge);
    }

    private ValidatedDepthCandidate EvaluateWithBaseAmount(
        SpreadCandidate candidate,
        decimal requestedBaseAmount,
        EvaluateDepthCandidatesRequest request,
        DateTimeOffset now,
        TimeSpan maxDepthAge)
    {
        if (!_depthCache.TryGet(
                candidate.LongConnector,
                candidate.TradingPair,
                out var buyDepth) ||
            buyDepth is null)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingBuyDepth,
                "Buy-side depth snapshot is missing.");
        }

        if (!_depthCache.TryGet(
                candidate.ShortConnector,
                candidate.TradingPair,
                out var sellDepth) ||
            sellDepth is null)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingSellDepth,
                "Sell-side depth snapshot is missing.");
        }

        if (now - buyDepth.ReceivedAt > maxDepthAge)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.StaleBuyDepth,
                "Buy-side depth snapshot is stale.");
        }

        if (now - sellDepth.ReceivedAt > maxDepthAge)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.StaleSellDepth,
                "Sell-side depth snapshot is stale.");
        }

        if (buyDepth.Asks.Count == 0)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingBuyDepth,
                "Buy-side asks are empty.");
        }

        if (sellDepth.Bids.Count == 0)
        {
            return Empty(
                candidate,
                request,
                DepthCandidateValidationStatus.MissingSellDepth,
                "Sell-side bids are empty.");
        }

        var buyQuote = CalculateBuyQuote(
            buyDepth.Asks,
            requestedBaseAmount);

        var sellQuote = CalculateSellQuote(
            sellDepth.Bids,
            requestedBaseAmount);

        var estimatedFeesPct =
            GetTakerFeePct(candidate.LongConnector, request)
            + GetTakerFeePct(candidate.ShortConnector, request)
            + request.CloseFeeBufferPct
            + request.SafetyBufferPct;

        if (!buyQuote.IsFullyFillable || !sellQuote.IsFullyFillable)
        {
            var grossSpreadPctPartial = CalculateGrossSpreadPct(
                buyQuote,
                sellQuote);

            return new ValidatedDepthCandidate(
                Candidate: candidate,
                Status: DepthCandidateValidationStatus.NotFullyFillable,
                NotionalUsd: request.NotionalUsd,
                RequestedBaseAmount: requestedBaseAmount,
                BuyQuote: buyQuote,
                SellQuote: sellQuote,
                GrossSpreadPct: grossSpreadPctPartial,
                EstimatedFeesPct: estimatedFeesPct,
                NetEdgePct: grossSpreadPctPartial - estimatedFeesPct,
                Reason: "Depth is not enough to fully fill requested notional.");
        }

        var grossSpreadPct = CalculateGrossSpreadPct(
            buyQuote,
            sellQuote);

        var netEdgePct = grossSpreadPct - estimatedFeesPct;

        if (netEdgePct < request.MinNetEdgePct)
        {
            return new ValidatedDepthCandidate(
                Candidate: candidate,
                Status: DepthCandidateValidationStatus.NetEdgeBelowMinimum,
                NotionalUsd: request.NotionalUsd,
                RequestedBaseAmount: requestedBaseAmount,
                BuyQuote: buyQuote,
                SellQuote: sellQuote,
                GrossSpreadPct: grossSpreadPct,
                EstimatedFeesPct: estimatedFeesPct,
                NetEdgePct: netEdgePct,
                Reason: "Net edge is below minimum.");
        }

        return new ValidatedDepthCandidate(
            Candidate: candidate,
            Status: DepthCandidateValidationStatus.Valid,
            NotionalUsd: request.NotionalUsd,
            RequestedBaseAmount: requestedBaseAmount,
            BuyQuote: buyQuote,
            SellQuote: sellQuote,
            GrossSpreadPct: grossSpreadPct,
            EstimatedFeesPct: estimatedFeesPct,
            NetEdgePct: netEdgePct,
            Reason: null);
    }

    private static DepthExecutionQuote CalculateBuyQuote(
        IReadOnlyList<OrderBookDepthLevel> asks,
        decimal requestedBaseAmount)
    {
        var orderedAsks = asks
            .OrderBy(x => x.Price)
            .ToList();

        return OrderBookWalker.Walk(
            levels: orderedAsks,
            requestedBaseAmount: requestedBaseAmount,
            isBuy: true);
    }

    private static DepthExecutionQuote CalculateSellQuote(
        IReadOnlyList<OrderBookDepthLevel> bids,
        decimal requestedBaseAmount)
    {
        var orderedBids = bids
            .OrderByDescending(x => x.Price)
            .ToList();

        return OrderBookWalker.Walk(
            levels: orderedBids,
            requestedBaseAmount: requestedBaseAmount,
            isBuy: false);
    }

    private static decimal CalculateGrossSpreadPct(
        DepthExecutionQuote buyQuote,
        DepthExecutionQuote sellQuote)
    {
        if (buyQuote.AveragePrice <= 0 || sellQuote.AveragePrice <= 0)
            return 0m;

        return (sellQuote.AveragePrice - buyQuote.AveragePrice)
            / buyQuote.AveragePrice
            * 100m;
    }

    private static decimal GetTakerFeePct(
        string connectorName,
        EvaluateDepthCandidatesRequest request)
    {
        return request.TakerFeesPct.TryGetValue(connectorName, out var fee)
            ? fee
            : request.DefaultTakerFeePct;
    }

    private static ValidatedDepthCandidate Empty(
        SpreadCandidate candidate,
        EvaluateDepthCandidatesRequest request,
        DepthCandidateValidationStatus status,
        string reason)
    {
        return new ValidatedDepthCandidate(
            Candidate: candidate,
            Status: status,
            NotionalUsd: request.NotionalUsd,
            RequestedBaseAmount: 0,
            BuyQuote: null,
            SellQuote: null,
            GrossSpreadPct: candidate.GrossSpreadPct,
            EstimatedFeesPct: null,
            NetEdgePct: null,
            Reason: reason);
    }
}