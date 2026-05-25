namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Okx;

public static class OkxSymbolMapper
{
    public static string ToOkxInstrumentId(string tradingPair)
    {
        // BTC-USDT -> BTC-USDT-SWAP
        return tradingPair.EndsWith("-USDT", StringComparison.OrdinalIgnoreCase)
            ? $"{tradingPair.ToUpperInvariant()}-SWAP"
            : tradingPair.ToUpperInvariant();
    }

    public static string FromOkxInstrumentId(string instrumentId)
    {
        // BTC-USDT-SWAP -> BTC-USDT
        return instrumentId.EndsWith("-SWAP", StringComparison.OrdinalIgnoreCase)
            ? instrumentId[..^5].ToUpperInvariant()
            : instrumentId.ToUpperInvariant();
    }
}