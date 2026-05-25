namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BitMart;

public static class BitMartSymbolMapper
{
    public static string ToBitMartSymbol(string tradingPair)
    {
        // BTC-USDT -> BTCUSDT
        return tradingPair
            .Replace("-", "", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromBitMartSymbol(string symbol)
    {
        // BTCUSDT -> BTC-USDT
        var normalized = symbol.ToUpperInvariant();

        return normalized.EndsWith("USDT", StringComparison.OrdinalIgnoreCase)
            ? $"{normalized[..^4]}-USDT"
            : normalized;
    }

    public static string FromAssets(
        string baseCurrency,
        string quoteCurrency)
    {
        return $"{baseCurrency.ToUpperInvariant()}-{quoteCurrency.ToUpperInvariant()}";
    }
}