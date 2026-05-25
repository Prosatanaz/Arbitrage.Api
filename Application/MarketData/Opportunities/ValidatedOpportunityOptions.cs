namespace Arbitrage.Api.Application.MarketData.Opportunities;

public sealed class ValidatedOpportunityOptions
{
    public const string SectionName = "ValidatedOpportunities";

    public bool Enabled { get; init; } = true;

    public int IntervalMs { get; init; } = 1000;

    public int MaxCandidates { get; init; } = 20;

    public decimal NotionalUsd { get; init; } = 100m;

    public decimal MinNetEdgePct { get; init; } = 0m;

    public int MaxDepthAgeMs { get; init; } = 10000;

    public int MinCandidateAgeMs { get; init; } = 1500;

    public decimal DefaultTakerFeePct { get; init; } = 0.055m;

    public decimal CloseFeeBufferPct { get; init; } = 0.11m;

    public decimal SafetyBufferPct { get; init; } = 0.05m;

    public Dictionary<string, decimal> TakerFeesPct { get; init; } = new();

    public bool IncludeNetEdgeBelowMinimum { get; init; } = true;

    public bool IncludeNotFullyFillable { get; init; } = true;

    public bool IncludeMissingDepth { get; init; } = false;
}