namespace Arbitrage.Api.Infrastructure.MarketData.Streams.GateIo;

public static class GateIoSymbolMapper
{
    public static string ToGateIoContract(string tradingPair)
    {
        // BTC-USDT -> BTC_USDT
        return tradingPair
            .Replace("-", "_", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }

    public static string FromGateIoContract(string contract)
    {
        // BTC_USDT -> BTC-USDT
        return contract
            .Replace("_", "-", StringComparison.OrdinalIgnoreCase)
            .ToUpperInvariant();
    }
}