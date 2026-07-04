namespace Arbitrage.Api.Infrastructure.Execution.Trading.BitMart;

public sealed class BitMartTradingOptions
{
    public const string SectionName = "BitMartTrading";

    public string BaseUrl { get; init; } = "https://api-cloud-v2.bitmart.com";
}
