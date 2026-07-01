namespace Arbitrage.Api.Application.Instruments;

public sealed class InstrumentFilterOptions
{
    public const string SectionName = "InstrumentFilter";

    public bool Enabled { get; init; } = true;

    public string RequiredQuoteAsset { get; init; } = "USDT";

    public bool RequireAlphanumericBaseAsset { get; init; } = true;

    public string[] BlockedBaseAssets { get; init; } = [];

    public string[] BlockedBaseAssetSuffixes { get; init; } =
    [
        "STOCK"
    ];

    public string[] BlockedTradingPairs { get; init; } = [];

    public string[] AllowedBaseAssets { get; init; } = [];

    public string[] AllowedTradingPairs { get; init; } = [];
}