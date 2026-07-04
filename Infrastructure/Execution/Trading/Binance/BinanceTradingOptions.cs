namespace Arbitrage.Api.Infrastructure.Execution.Trading.Binance;

public sealed class BinanceTradingOptions
{
    public const string SectionName = "BinanceTrading";

    public string BaseUrl { get; init; } = "https://fapi.binance.com";

    public int RecvWindowMs { get; init; } = 5000;
}
