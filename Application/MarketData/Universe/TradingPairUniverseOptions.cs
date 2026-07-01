namespace Arbitrage.Api.Application.MarketData.Universe;

public sealed class TradingPairUniverseOptions
{
    public const string SectionName = "TradingPairUniverse";

    public bool Enabled { get; init; } = true;

    public bool RefreshOnStartup { get; init; } = true;

    public int RefreshIntervalMinutes { get; init; } = 360;

    public string OutputPath { get; init; } = "Data/universe/usdt-perp-pairs.json";

    public string QuoteAsset { get; init; } = "USDT";

    public bool RequireAllEnabledConnectors { get; init; } = true;

    public bool TreatZeroPairsAsFailure { get; init; } = true;

    public string[] EnabledConnectors { get; init; } =
    [
        "binance_perpetual",
        "bybit_perpetual",
        "okx_perpetual"
    ];
}