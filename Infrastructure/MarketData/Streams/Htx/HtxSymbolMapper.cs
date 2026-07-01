namespace Arbitrage.Api.Infrastructure.MarketData.Streams.Htx;

public static class HtxSymbolMapper
{
    public static string ToHtxContractCode(string tradingPair)
    {
        // BTC-USDT -> BTC-USDT
        return tradingPair
            .Trim()
            .ToUpperInvariant();
    }

    public static string FromHtxContractCode(string contractCode)
    {
        // BTC-USDT -> BTC-USDT
        return contractCode
            .Trim()
            .ToUpperInvariant();
    }
}