namespace Arbitrage.Api.Application.MarketData.Depth;

public sealed class DepthTrackingOptions
{
    public const string SectionName = "DepthTracking";

    public bool Enabled { get; init; } = true;

    public int IntervalMs { get; init; } = 500;

    public int CandidateMaxAgeMs { get; init; } = 15000;

    public int MaxCandidatesToTrack { get; init; } = 20;

    public int MaxPairsPerConnector { get; init; } = 40;

    public string[] EnabledConnectors { get; init; } = [];
}