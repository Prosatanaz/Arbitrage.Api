using Arbitrage.Api.Domain.MarketData;

namespace Arbitrage.Api.Application.MarketData.Depth;

public enum DepthCandidateValidationStatus
{
    Unknown = 0,
    Valid = 1,
    MissingBuyDepth = 2,
    MissingSellDepth = 3,
    StaleBuyDepth = 4,
    StaleSellDepth = 5,
    NotFullyFillable = 6,
    NetEdgeBelowMinimum = 7
}

public sealed class EvaluateDepthCandidatesRequest
{
    public int MaxCandidates { get; init; } = 20;

    public decimal NotionalUsd { get; init; } = 100m;

    public decimal MinNetEdgePct { get; init; } = 0m;

    public int MaxDepthAgeMs { get; init; } = 3000;

    public int MinCandidateAgeMs { get; init; } = 1500;

    public decimal DefaultTakerFeePct { get; init; } = 0.055m;

    public decimal CloseFeeBufferPct { get; init; } = 0.11m;

    public decimal SafetyBufferPct { get; init; } = 0.05m;

    public Dictionary<string, decimal> TakerFeesPct { get; init; } = new();
}

public sealed record DepthExecutionQuote(
    decimal RequestedBaseAmount,
    decimal FilledBaseAmount,
    decimal AveragePrice,
    decimal BestPrice,
    decimal WorstPrice,
    decimal QuoteAmount,
    decimal SlippagePct,
    int LevelsUsed,
    bool IsFullyFillable);

public sealed record ValidatedDepthCandidate(
    SpreadCandidate Candidate,
    DepthCandidateValidationStatus Status,
    decimal NotionalUsd,
    decimal RequestedBaseAmount,
    DepthExecutionQuote? BuyQuote,
    DepthExecutionQuote? SellQuote,
    decimal? GrossSpreadPct,
    decimal? EstimatedFeesPct,
    decimal? NetEdgePct,
    string? Reason);

public sealed record EvaluateDepthCandidatesResponse(
    DateTimeOffset EvaluatedAt,
    int CandidatesAvailable,
    int CandidatesMatured,
    int CandidatesTargeted,
    int CandidatesSkippedAsNotTargeted,
    int CandidatesChecked,
    int ValidCount,
    int NetEdgeBelowMinimumCount,
    int MissingDepthCount,
    int NotFillableCount,
    IReadOnlyList<ValidatedDepthCandidate> Items);