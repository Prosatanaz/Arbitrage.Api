namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed class SpreadDetectorOptions
{
    public const string SectionName = "SpreadDetector";

    public bool Enabled { get; init; } = true;

    public int IntervalMs { get; init; } = 1000;

    public decimal MinGrossSpreadPct { get; init; } = 0.03m;

    public decimal MaxGrossSpreadPct { get; init; } = 5m;

    public int MaxSnapshotAgeMs { get; init; } = 10000;

    public int MaxCandidatesToKeep { get; init; } = 100;

    public int CandidateTtlMs { get; init; } = 15000;
}