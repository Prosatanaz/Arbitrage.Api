namespace Arbitrage.Api.Application.SignalQuality;

public sealed class SignalQualityOptions
{
    public const string SectionName = "SignalQuality";

    public bool Enabled { get; init; } = true;

    public decimal MinCandidateNetEdgePct { get; init; } = 0.20m;

    public decimal MinAlertNetEdgePct { get; init; } = 0.30m;

    public int MinEpisodeSamples { get; init; } = 3;

    public int MinEpisodeDurationSeconds { get; init; } = 20;

    public int EpisodeMaxGapSeconds { get; init; } = 25;

    public decimal MaxBuySlippagePct { get; init; } = 0.15m;

    public decimal MaxSellSlippagePct { get; init; } = 0.15m;

    public string[] BlockedConnectors { get; init; } = [];

    public string[] AllowedTradingPairs { get; init; } = [];

    public string[] BlockedTradingPairs { get; init; } = [];
}