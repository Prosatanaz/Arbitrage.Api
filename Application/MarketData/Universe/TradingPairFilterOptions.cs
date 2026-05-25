namespace Arbitrage.Api.Application.MarketData.Universe;

public sealed class TradingPairFilterOptions
{
    public const string SectionName = "TradingPairFilters";

    public bool Enabled { get; init; } = true;

    public string RequiredQuoteAsset { get; init; } = "USDT";

    public bool RequireAlphanumericBaseAsset { get; init; } = true;

    public string[] ExcludedBaseAssets { get; init; } = [];

    public string[] ExcludedBaseAssetSuffixes { get; init; } = [];

    public string[] ExcludedTradingPairs { get; init; } = [];
}