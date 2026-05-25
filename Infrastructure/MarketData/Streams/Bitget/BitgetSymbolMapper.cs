namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bitget;

public static class BitgetSymbolMapper
{
    public static string ToBitgetSymbol(string tradingPair)
    {
        // BTC-USDT -> BTCUSDT
        return tradingPair
            .Replace("-", "", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromBitgetSymbol(string symbol)
    {
        // BTCUSDT -> BTC-USDT
        var normalized = symbol.ToUpperInvariant();

        return normalized.EndsWith("USDT", StringComparison.OrdinalIgnoreCase)
            ? $"{normalized[..^4]}-USDT"
            : normalized;
    }
}