namespace Arbitrage.Api.Infrastructure.Execution.Trading.Okx;

public sealed class OkxTradingOptions
{
    public const string SectionName = "OkxTrading";

    public string BaseUrl { get; init; } = "https://www.okx.com";

    public string TradeMode { get; init; } = "cross";
}
