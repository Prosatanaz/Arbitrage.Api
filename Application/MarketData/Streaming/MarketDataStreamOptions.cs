namespace Arbitrage.Api.Application.MarketData.Streaming;

public sealed class MarketDataStreamOptions
{
    public const string SectionName = "MarketDataStreams";

    public bool Enabled { get; init; } = true;

    public string DiscoveredPairsPath { get; init; } =
        "Data/universe/usdt-perp-pairs.json";

    public IReadOnlyList<string> EnabledConnectors { get; init; } = new[]
    {
        "binance_perpetual"
    };

    public int MaxSnapshotAgeMs { get; init; } = 3000;
}