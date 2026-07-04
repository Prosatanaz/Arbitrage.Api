namespace Arbitrage.Api.Infrastructure.Execution.Trading.KuCoin;

public sealed class KuCoinTradingOptions
{
    public const string SectionName = "KuCoinTrading";

    public string BaseUrl { get; init; } = "https://api-futures.kucoin.com";
}
