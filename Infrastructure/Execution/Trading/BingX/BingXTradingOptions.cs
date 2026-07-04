namespace Arbitrage.Api.Infrastructure.Execution.Trading.BingX;

public sealed class BingXTradingOptions
{
    public const string SectionName = "BingXTrading";

    public string BaseUrl { get; init; } = "https://open-api.bingx.com";
}
