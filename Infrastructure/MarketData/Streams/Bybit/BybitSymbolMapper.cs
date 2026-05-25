namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Bybit;

public static class BybitSymbolMapper
{
    public static string ToBybitSymbol(string tradingPair)
    {
        return tradingPair
            .Replace("-", "", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromBybitSymbol(string symbol)
    {
        symbol = symbol.ToUpperInvariant();

        if (symbol.EndsWith("USDT", StringComparison.OrdinalIgnoreCase))
            return symbol[..^4] + "-USDT";

        return symbol;
    }
}