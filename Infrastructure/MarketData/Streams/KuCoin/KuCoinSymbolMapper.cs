namespace Arbitrage.Api.Infrastructure.MarketData.Streams.KuCoin;

public static class KuCoinSymbolMapper
{
    public static string ToKuCoinSymbol(string tradingPair)
    {
        // BTC-USDT -> XBTUSDTM
        // ETH-USDT -> ETHUSDTM
        var parts = tradingPair.Split('-', StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length != 2)
            return tradingPair.Replace("-", "", StringComparison.OrdinalIgnoreCase).ToUpperInvariant();

        var baseAsset = parts[0].ToUpperInvariant();
        var quoteAsset = parts[1].ToUpperInvariant();

        if (baseAsset == "BTC")
            baseAsset = "XBT";

        return $"{baseAsset}{quoteAsset}M";
    }

    public static string FromKuCoinSymbol(string symbol)
    {
        // XBTUSDTM -> BTC-USDT
        // ETHUSDTM -> ETH-USDT
        var normalized = symbol.ToUpperInvariant();

        if (!normalized.EndsWith("USDTM", StringComparison.OrdinalIgnoreCase))
            return normalized;

        var baseAsset = normalized[..^5];

        if (baseAsset == "XBT")
            baseAsset = "BTC";

        return $"{baseAsset}-USDT";
    }

    public static string FromKuCoinAssets(
        string baseCurrency,
        string quoteCurrency)
    {
        var baseAsset = baseCurrency.ToUpperInvariant();

        if (baseAsset == "XBT")
            baseAsset = "BTC";

        return $"{baseAsset}-{quoteCurrency.ToUpperInvariant()}";
    }
}