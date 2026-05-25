namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Binance;

public static class BinanceSymbolMapper
{
    public static string ToBinanceSymbol(string tradingPair)
    {
        return tradingPair.Replace("-", "", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromBinanceSymbol(string symbol)
    {
        symbol = symbol.ToUpperInvariant();

        if (symbol.EndsWith("USDT", StringComparison.OrdinalIgnoreCase))
            return symbol[..^4] + "-USDT";

        return symbol;
    }
}