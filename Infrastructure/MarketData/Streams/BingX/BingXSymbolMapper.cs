namespace Arbitrage.Api.Infrastructure.MarketData.Streams.BingX;

public static class BingXSymbolMapper
{
    public static string ToBingXSymbol(string tradingPair)
    {
        // BTC-USDT -> BTC-USDT
        return tradingPair
            .Trim()
            .ToUpperInvariant();
    }

    public static string FromBingXSymbol(string symbol)
    {
        // BTC-USDT -> BTC-USDT
        return symbol
            .Trim()
            .ToUpperInvariant();
    }
}