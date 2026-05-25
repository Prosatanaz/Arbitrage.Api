namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Mexc;

public static class MexcSymbolMapper
{
    public static string ToMexcSymbol(string tradingPair)
    {
        // BTC-USDT -> BTC_USDT
        return tradingPair
            .Replace("-", "_", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromMexcSymbol(string symbol)
    {
        // BTC_USDT -> BTC-USDT
        return symbol
            .Replace("_", "-", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }
}