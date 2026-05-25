using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.MarketData.Universe;

public sealed record TradingPairFilterEvaluation(
    bool IsAllowed,
    string? Reason);

public sealed class TradingPairFilter
{
    private readonly TradingPairFilterOptions _options;
    private readonly HashSet<string> _excludedBaseAssets;
    private readonly HashSet<string> _excludedBaseAssetSuffixes;
    private readonly HashSet<string> _excludedTradingPairs;

    public TradingPairFilter(
        IOptions<TradingPairFilterOptions> options)
    {
        _options = options.Value;

        _excludedBaseAssets = _options.ExcludedBaseAssets
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(Normalize)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        _excludedBaseAssetSuffixes = _options.ExcludedBaseAssetSuffixes
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(Normalize)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        _excludedTradingPairs = _options.ExcludedTradingPairs
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(Normalize)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public TradingPairFilterEvaluation Evaluate(string tradingPair)
    {
        if (!_options.Enabled)
        {
            return Allowed();
        }

        if (string.IsNullOrWhiteSpace(tradingPair))
        {
            return Rejected("empty_trading_pair");
        }

        var normalized = Normalize(tradingPair);

        if (_excludedTradingPairs.Contains(normalized))
        {
            return Rejected("trading_pair_excluded");
        }

        var parts = normalized.Split(
            '-',
            2,
            StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length != 2)
        {
            return Rejected("invalid_trading_pair_format");
        }

        var baseAsset = parts[0];
        var quoteAsset = parts[1];

        if (string.IsNullOrWhiteSpace(baseAsset))
        {
            return Rejected("empty_base_asset");
        }

        if (!string.IsNullOrWhiteSpace(_options.RequiredQuoteAsset) &&
            !quoteAsset.Equals(
                _options.RequiredQuoteAsset,
                StringComparison.OrdinalIgnoreCase))
        {
            return Rejected("quote_asset_not_allowed");
        }

        if (_options.RequireAlphanumericBaseAsset &&
            !baseAsset.All(IsAsciiLetterOrDigit))
        {
            return Rejected("base_asset_contains_non_alphanumeric_chars");
        }

        if (_excludedBaseAssets.Contains(baseAsset))
        {
            return Rejected("base_asset_excluded");
        }

        if (_excludedBaseAssetSuffixes.Any(suffix =>
                baseAsset.EndsWith(suffix, StringComparison.OrdinalIgnoreCase)))
        {
            return Rejected("base_asset_suffix_excluded");
        }

        return Allowed();
    }

    public bool IsAllowed(string tradingPair)
    {
        return Evaluate(tradingPair).IsAllowed;
    }

    private static TradingPairFilterEvaluation Allowed()
    {
        return new TradingPairFilterEvaluation(
            IsAllowed: true,
            Reason: null);
    }

    private static TradingPairFilterEvaluation Rejected(string reason)
    {
        return new TradingPairFilterEvaluation(
            IsAllowed: false,
            Reason: reason);
    }

    private static string Normalize(string value)
    {
        return value.Trim().ToUpperInvariant();
    }

    private static bool IsAsciiLetterOrDigit(char value)
    {
        return value is >= 'A' and <= 'Z'
            or >= 'a' and <= 'z'
            or >= '0' and <= '9';
    }
}