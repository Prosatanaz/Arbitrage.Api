using Microsoft.Extensions.Options;

namespace Arbitrage.Api.Application.Instruments;

public sealed class InstrumentFilterService
{
    private readonly InstrumentFilterOptions _options;

    public InstrumentFilterService(
        IOptions<InstrumentFilterOptions> options)
    {
        _options = options.Value;
    }

    public InstrumentFilterResult EvaluateTradingPair(string tradingPair)
    {
        var normalizedTradingPair = NormalizeTradingPair(tradingPair);

        var (baseAsset, quoteAsset) = SplitTradingPair(normalizedTradingPair);

        if (!_options.Enabled)
        {
            return Allowed(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Instrument filter is disabled.");
        }

        if (string.IsNullOrWhiteSpace(normalizedTradingPair))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Trading pair is empty.");
        }

        if (string.IsNullOrWhiteSpace(baseAsset) ||
            string.IsNullOrWhiteSpace(quoteAsset))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Trading pair format is invalid.");
        }

        if (!quoteAsset.Equals(
                _options.RequiredQuoteAsset,
                StringComparison.OrdinalIgnoreCase))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                $"Quote asset is not {_options.RequiredQuoteAsset}.");
        }

        if (_options.RequireAlphanumericBaseAsset &&
            !IsAlphanumeric(baseAsset))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Base asset is not alphanumeric.");
        }

        if (_options.AllowedTradingPairs.Length > 0 &&
            !_options.AllowedTradingPairs.Contains(
                normalizedTradingPair,
                StringComparer.OrdinalIgnoreCase))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Trading pair is not in allowlist.");
        }

        if (_options.AllowedBaseAssets.Length > 0 &&
            !_options.AllowedBaseAssets.Contains(
                baseAsset,
                StringComparer.OrdinalIgnoreCase))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Base asset is not in allowlist.");
        }

        if (_options.BlockedTradingPairs.Contains(
                normalizedTradingPair,
                StringComparer.OrdinalIgnoreCase))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Trading pair is explicitly blocked.");
        }

        if (_options.BlockedBaseAssets.Contains(
                baseAsset,
                StringComparer.OrdinalIgnoreCase))
        {
            return Blocked(
                normalizedTradingPair,
                baseAsset,
                quoteAsset,
                "Base asset is explicitly blocked.");
        }

        foreach (var suffix in _options.BlockedBaseAssetSuffixes)
        {
            if (string.IsNullOrWhiteSpace(suffix))
                continue;

            if (baseAsset.EndsWith(
                    suffix.Trim(),
                    StringComparison.OrdinalIgnoreCase))
            {
                return Blocked(
                    normalizedTradingPair,
                    baseAsset,
                    quoteAsset,
                    $"Base asset has blocked suffix '{suffix}'.");
            }
        }

        return Allowed(
            normalizedTradingPair,
            baseAsset,
            quoteAsset,
            "Instrument passed filter.");
    }

    public bool IsAllowed(string tradingPair)
    {
        return EvaluateTradingPair(tradingPair).Decision == InstrumentFilterDecision.Allowed;
    }

    public static string ExtractBaseAsset(string tradingPair)
    {
        var normalized = NormalizeTradingPair(tradingPair);

        var dashIndex = normalized.IndexOf('-');

        if (dashIndex <= 0)
            return normalized;

        return normalized[..dashIndex];
    }

    public static string NormalizeTradingPair(string tradingPair)
    {
        return string.IsNullOrWhiteSpace(tradingPair)
            ? ""
            : tradingPair.Trim().ToUpperInvariant();
    }

    private static (string BaseAsset, string QuoteAsset) SplitTradingPair(string tradingPair)
    {
        if (string.IsNullOrWhiteSpace(tradingPair))
            return ("", "");

        var parts = tradingPair.Split(
            '-',
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length != 2)
            return (tradingPair.ToUpperInvariant(), "");

        return (
            parts[0].ToUpperInvariant(),
            parts[1].ToUpperInvariant());
    }

    private static bool IsAlphanumeric(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        foreach (var ch in value)
        {
            if (!char.IsLetterOrDigit(ch))
                return false;
        }

        return true;
    }

    private static InstrumentFilterResult Allowed(
        string tradingPair,
        string baseAsset,
        string quoteAsset,
        string reason)
    {
        return new InstrumentFilterResult(
            InstrumentFilterDecision.Allowed,
            reason,
            tradingPair,
            baseAsset,
            quoteAsset);
    }

    private static InstrumentFilterResult Blocked(
        string tradingPair,
        string baseAsset,
        string quoteAsset,
        string reason)
    {
        return new InstrumentFilterResult(
            InstrumentFilterDecision.Blocked,
            reason,
            tradingPair,
            baseAsset,
            quoteAsset);
    }
}